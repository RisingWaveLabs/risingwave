// Copyright 2022 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::LazyLock;
use std::task::Poll;

use anyhow::Context;
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use parking_lot::Mutex;
use prost_013::Message as _;
use pulsar::consumer::{InitialPosition, Message};
use pulsar::message::proto::MessageIdData;
use pulsar::{Consumer, ConsumerBuilder, ConsumerOptions, Pulsar, SubType, TokioExecutor};
use risingwave_common::{bail, ensure};
use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;
use thiserror_ext::AsReport;

use crate::error::ConnectorResult;
use crate::parser::ParserConfig;
use crate::source::monitor::ConnectorAckFailureType;
use crate::source::pulsar::split::PulsarSplit;
use crate::source::pulsar::{PulsarEnumeratorOffset, PulsarProperties};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId, SplitMetaData,
    SplitReader, build_pulsar_ack_channel_id, into_chunk_stream,
};
type PulsarAckSender = tokio::sync::mpsc::UnboundedSender<Vec<u8>>;

pub(crate) static PULSAR_ACK_CHANNEL: LazyLock<Mutex<HashMap<String, PulsarAckSender>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Owns one ACK channel registry entry and removes it when its reader is dropped.
///
/// The sender identity check prevents an old reader from removing the replacement channel when a
/// stream is rebuilt with the same channel ID.
struct PulsarAckChannelRegistration {
    channel_id: String,
    sender: PulsarAckSender,
}

impl PulsarAckChannelRegistration {
    fn new(channel_id: String, sender: PulsarAckSender) -> Self {
        PULSAR_ACK_CHANNEL
            .lock()
            .insert(channel_id.clone(), sender.clone());
        Self { channel_id, sender }
    }
}

impl Drop for PulsarAckChannelRegistration {
    fn drop(&mut self) {
        let mut channels = PULSAR_ACK_CHANNEL.lock();
        if channels
            .get(&self.channel_id)
            .is_some_and(|sender| sender.same_channel(&self.sender))
        {
            channels.remove(&self.channel_id);
        }
    }
}

const PULSAR_DEFAULT_SUBSCRIPTION_PREFIX: &str = "rw-consumer";

pub enum PulsarSplitReader {
    Broker(PulsarBrokerReader),
}

#[async_trait]
impl SplitReader for PulsarSplitReader {
    type Properties = PulsarProperties;
    type Split = PulsarSplit;

    async fn new(
        props: PulsarProperties,
        splits: Vec<PulsarSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        ensure!(splits.len() == 1, "only support single split");
        let split = splits.into_iter().next().unwrap();
        let topic = split.topic.to_string();

        tracing::debug!("creating consumer for pulsar split topic {}", topic,);

        if props.iceberg_loader_enabled.unwrap_or(false) {
            bail!("PulsarIcebergReader has already been deprecated");
        } else {
            Ok(Self::Broker(
                PulsarBrokerReader::new(props, vec![split], parser_config, source_ctx, None)
                    .await?,
            ))
        }
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        match self {
            Self::Broker(reader) => {
                let (parser_config, source_context) =
                    (reader.parser_config.clone(), reader.source_ctx.clone());
                Box::pin(into_chunk_stream(
                    reader.into_data_stream(),
                    parser_config,
                    source_context,
                ))
            }
        }
    }
}

/// Filter offset for pulsar messages
#[derive(Debug, Clone)]
struct PulsarFilterOffset {
    /// Ledger ID of the message
    pub ledger_id: u64,
    /// Entry ID of the message
    pub entry_id: u64,
    /// Batch index of the message, if any
    pub batch_index: Option<i32>,
}

/// This reader reads from pulsar broker
pub struct PulsarBrokerReader {
    #[expect(dead_code)]
    pulsar: Pulsar<TokioExecutor>,
    consumer: Consumer<Vec<u8>, TokioExecutor>,
    split: PulsarSplit,
    split_id: SplitId,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,

    // for filter out already read messages
    already_read_offset: Option<PulsarFilterOffset>,
}

// {ledger_id}:{entry_id}:{partition}:{batch_index}
fn parse_message_id(id: &str) -> ConnectorResult<MessageIdData> {
    let splits = id.split(':').collect_vec();

    if splits.len() < 2 || splits.len() > 4 {
        bail!("illegal message id string {}", id);
    }

    let ledger_id = splits[0].parse::<u64>().context("illegal ledger id")?;
    let entry_id = splits[1].parse::<u64>().context("illegal entry id")?;

    let mut message_id = MessageIdData {
        ledger_id,
        entry_id,
        partition: None,
        batch_index: None,
        ack_set: vec![],
        batch_size: None,
        first_chunk_message_id: None,
    };

    if splits.len() > 2 {
        let partition = splits[2].parse::<i32>().context("illegal partition")?;
        message_id.partition = Some(partition);
    }

    if splits.len() == 4 {
        let batch_index = splits[3].parse::<i32>().context("illegal batch index")?;
        message_id.batch_index = Some(batch_index);
    }

    Ok(message_id)
}

#[async_trait]
impl SplitReader for PulsarBrokerReader {
    type Properties = PulsarProperties;
    type Split = PulsarSplit;

    async fn new(
        props: PulsarProperties,
        splits: Vec<PulsarSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        ensure!(splits.len() == 1, "only support single split");
        let split = splits.into_iter().next().unwrap();
        let pulsar = props
            .common
            .build_client(
                &props.oauth,
                &props.aws_auth_props,
                props.operation_retry.to_pulsar_options(),
            )
            .await?;
        let topic = split.topic.to_string();

        tracing::debug!("creating consumer for pulsar split topic {}", topic,);

        let mut builder: ConsumerBuilder<TokioExecutor> = pulsar
            .consumer()
            .with_topic(&topic)
            .with_subscription_type(SubType::Exclusive)
            .with_subscription(format!(
                "{}-{}-{}",
                props
                    .subscription_name_prefix
                    .unwrap_or(PULSAR_DEFAULT_SUBSCRIPTION_PREFIX.to_owned()),
                source_ctx.fragment_id,
                source_ctx.actor_id
            ));

        let mut already_read_offset = None;

        let mut consumer_options = match split.start_offset.clone() {
            PulsarEnumeratorOffset::Earliest => {
                let initial_position = if topic.starts_with("non-persistent://") {
                    tracing::warn!(
                        "Earliest offset is not supported for non-persistent topic, use Latest instead"
                    );
                    InitialPosition::Latest
                } else {
                    InitialPosition::Earliest
                };
                ConsumerOptions::default()
                    .with_initial_position(initial_position)
                    .durable(false)
            }
            PulsarEnumeratorOffset::Latest => ConsumerOptions::default()
                .with_initial_position(InitialPosition::Latest)
                .durable(false),
            PulsarEnumeratorOffset::MessageId(m) => {
                if topic.starts_with("non-persistent://") {
                    tracing::warn!(
                        "MessageId offset is not supported for non-persistent topic, use Latest instead"
                    );
                    ConsumerOptions::default().with_initial_position(InitialPosition::Latest)
                } else {
                    let start_message_id = parse_message_id(m.as_str())?;
                    already_read_offset = Some(PulsarFilterOffset {
                        ledger_id: start_message_id.ledger_id,
                        entry_id: start_message_id.entry_id,
                        batch_index: start_message_id.batch_index,
                    });
                    ConsumerOptions {
                        durable: Some(false),
                        start_message_id: Some(start_message_id),
                        ..Default::default()
                    }
                }
            }
            PulsarEnumeratorOffset::Timestamp(_) => ConsumerOptions::default(),
        };

        // Apply read_compacted option to all consumer options
        consumer_options.read_compacted = props.consumer_options.read_compacted;

        builder = builder.with_options(consumer_options);

        if let Some(delay) = props.subscription_unacked_resend_delay {
            builder = builder.with_unacked_message_resend_delay(Some(delay));
        }

        let consumer: Consumer<Vec<u8>, _> = builder.build().await?;
        if let PulsarEnumeratorOffset::Timestamp(_ts) = split.start_offset {
            // FIXME: Here we need pulsar-rs to support the send + sync consumer
            // consumer
            //     .seek(None, None, Some(ts as u64), pulsar.clone())
            //     .await?;
        }

        tracing::info!("consumer created with split {:?}", split);

        Ok(Self {
            pulsar,
            consumer,
            split_id: split.id(),
            split,
            parser_config,
            source_ctx,
            already_read_offset,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }
}

impl PulsarBrokerReader {
    #[try_stream(ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    async fn into_data_stream(mut self) {
        let max_chunk_size = self.source_ctx.source_ctrl_opts.chunk_size;
        let mut already_read_offset = self.already_read_offset.take();
        let require_message_header = self.parser_config.common.rw_columns.iter().any(|col_desc| {
            matches!(
                col_desc.additional_column.column_type,
                Some(AdditionalColumnType::Headers(_) | AdditionalColumnType::HeaderInner(_))
            )
        });
        #[for_await]
        for msgs in self.into_stream().ready_chunks(max_chunk_size) {
            let msgs = msgs
                .into_iter()
                .collect::<Result<Vec<Message<Vec<u8>>>, _>>()?;
            let mut res = Vec::with_capacity(msgs.len());
            for msg in msgs {
                if let Some(PulsarFilterOffset {
                    ledger_id,
                    entry_id,
                    batch_index,
                }) = already_read_offset
                {
                    let message_id = msg.message_id();

                    // If we have a previously stored offset, check that the messages we're consuming
                    // appear after. Note that entry IDs and batch indexes are only monotonically increasing
                    // within a BookKeeper ledger, so we must check that the ledger IDs match. If they don't match,
                    // messages are coming from a new ledger and we can safely ignore the stored offset.
                    if message_id.ledger_id == ledger_id
                        && message_id.entry_id <= entry_id
                        && message_id.batch_index <= batch_index
                    {
                        tracing::info!(
                            "skipping message with entry_id: {}, batch_index: {:?} as expected offset after entry_id {} batch_index {:?}",
                            message_id.entry_id,
                            message_id.batch_index,
                            entry_id,
                            batch_index
                        );
                        continue;
                    } else {
                        already_read_offset = None;
                    }
                }

                let msg = SourceMessage::from_pulsar_message(msg, require_message_header);
                res.push(msg);
            }
            yield res;
        }
    }

    fn into_stream(self) -> PulsarConsumeStream {
        let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel();
        let channel_entry = build_pulsar_ack_channel_id(
            self.source_ctx.source_id,
            &self.split_id,
            self.source_ctx.actor_id,
        );
        let ack_channel_registration = PulsarAckChannelRegistration::new(channel_entry, ack_tx);

        PulsarConsumeStream {
            source_ctx: self.source_ctx,
            split_id: self.split_id,
            pulsar_reader: self.consumer,
            ack_rx,
            _ack_channel_registration: ack_channel_registration,
            topic: self.split.topic.to_string(),
            termination_reported: false,
        }
    }
}

struct PulsarConsumeStream {
    source_ctx: SourceContextRef,
    split_id: SplitId,
    pulsar_reader: Consumer<Vec<u8>, TokioExecutor>,
    ack_rx: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    _ack_channel_registration: PulsarAckChannelRegistration,
    topic: String,
    termination_reported: bool,
}

impl PulsarConsumeStream {
    fn unexpected_termination_error(&self) -> pulsar::error::Error {
        // Current RisingWave Pulsar sources are unbounded, so SDK consumer
        // termination is unexpected and should trigger source retry. Future
        // bounded Pulsar source mode must revisit this assumption.
        pulsar::error::Error::Custom(format!(
            "unexpected Pulsar consumer termination: source_id={}, source_name={}, actor_id={}, fragment_id={}, split_id={}, topic={}",
            self.source_ctx.source_id,
            self.source_ctx.source_name,
            self.source_ctx.actor_id,
            self.source_ctx.fragment_id,
            self.split_id,
            self.topic,
        ))
    }

    fn report_unexpected_termination(
        &mut self,
    ) -> Poll<Option<Result<Message<Vec<u8>>, pulsar::error::Error>>> {
        self.termination_reported = true;
        Poll::Ready(Some(Err(self.unexpected_termination_error())))
    }

    fn inc_ack_failure_count(&self, failure_type: ConnectorAckFailureType) {
        self.source_ctx.metrics.inc_connector_ack_failure_count(
            self.source_ctx.source_name.as_str(),
            "pulsar",
            failure_type,
        );
    }

    fn inc_ack_success_count(&self) {
        self.source_ctx
            .metrics
            .inc_connector_ack_success_count(self.source_ctx.source_name.as_str(), "pulsar");
    }

    fn do_ack(&mut self, message_id_bytes: Vec<u8>) {
        let message_id = match MessageIdData::decode(message_id_bytes.as_slice()) {
            Ok(message_id) => message_id,
            Err(e) => {
                self.inc_ack_failure_count(ConnectorAckFailureType::DecodeError);
                tracing::warn!(
                    source_id = %self.source_ctx.source_id,
                    source_name = self.source_ctx.source_name.as_str(),
                    actor_id = %self.source_ctx.actor_id,
                    fragment_id = %self.source_ctx.fragment_id,
                    split_id = %self.split_id,
                    error=%e.as_report(), "meet error when decode message id, skip ack"
                );
                return;
            }
        };
        let topic = self.topic.clone();
        tracing::debug!(
            "ack message id: {:?} from channel {}",
            message_id,
            build_pulsar_ack_channel_id(
                self.source_ctx.source_id,
                &self.split_id,
                self.source_ctx.actor_id,
            )
        );
        #[cfg(not(madsim))]
        {
            // FIXME: madsim does not support block_on and block_in_place
            let ack_result = {
                let pulsar_reader = &mut self.pulsar_reader;
                tokio::task::block_in_place(move || {
                    tokio::runtime::Handle::current().block_on(async move {
                        pulsar_reader
                            .cumulative_ack_with_id(&topic, message_id)
                            .await
                    })
                })
            };
            match ack_result {
                Ok(()) => {
                    self.inc_ack_success_count();
                }
                Err(e) => {
                    self.inc_ack_failure_count(ConnectorAckFailureType::BrokerError);
                    tracing::warn!(
                        source_id = %self.source_ctx.source_id,
                        source_name = self.source_ctx.source_name.as_str(),
                        actor_id = %self.source_ctx.actor_id,
                        fragment_id = %self.source_ctx.fragment_id,
                        split_id = %self.split_id,
                        error=%e.as_report(), "meet error when ack message"
                    );
                }
            }
        }
    }
}

impl futures::Stream for PulsarConsumeStream {
    type Item = Result<Message<Vec<u8>>, pulsar::error::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.termination_reported {
            return Poll::Ready(None);
        }

        match (
            self.ack_rx.poll_recv(cx),
            self.pulsar_reader.poll_next_unpin(cx),
        ) {
            (Poll::Pending, Poll::Pending) => {}
            (Poll::Ready(some_ack), Poll::Pending) => {
                if let Some(ack_message_id) = some_ack {
                    self.do_ack(ack_message_id);
                }
            }
            (Poll::Pending, Poll::Ready(maybe_message)) => match maybe_message {
                Some(Ok(msg)) => {
                    return Poll::Ready(Some(Ok(msg)));
                }
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                None => {
                    return self.report_unexpected_termination();
                }
            },
            (Poll::Ready(some_ack), Poll::Ready(maybe_message)) => {
                if let Some(ack_message_id) = some_ack {
                    self.do_ack(ack_message_id);
                }
                match maybe_message {
                    Some(Ok(msg)) => {
                        return Poll::Ready(Some(Ok(msg)));
                    }
                    Some(Err(e)) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                    None => {
                        return self.report_unexpected_termination();
                    }
                }
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::{PULSAR_ACK_CHANNEL, PulsarAckChannelRegistration};

    #[test]
    fn test_ack_channel_registration_cleanup_does_not_remove_replacement() {
        let channel_id = "pulsar-ack-channel-registration-test".to_owned();
        let (first_tx, _first_rx) = tokio::sync::mpsc::unbounded_channel();
        let first = PulsarAckChannelRegistration::new(channel_id.clone(), first_tx);

        let (second_tx, _second_rx) = tokio::sync::mpsc::unbounded_channel();
        let second = PulsarAckChannelRegistration::new(channel_id.clone(), second_tx.clone());

        drop(first);
        assert!(
            PULSAR_ACK_CHANNEL
                .lock()
                .get(&channel_id)
                .is_some_and(|sender| sender.same_channel(&second_tx))
        );

        drop(second);
        assert!(!PULSAR_ACK_CHANNEL.lock().contains_key(&channel_id));
    }
}
