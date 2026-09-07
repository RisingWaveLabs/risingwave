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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::mem::swap;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use risingwave_common::metrics::{LabelGuardedIntCounter, LabelGuardedIntGauge};
use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;

use crate::connector_common::read_kafka_log_level;
use crate::error::ConnectorResult as Result;
use crate::parser::ParserConfig;
use crate::source::base::SourceMessage;
use crate::source::kafka::{
    KAFKA_ISOLATION_LEVEL, KafkaContextCommon, KafkaProperties, KafkaSplit, RwConsumerContext,
};
use crate::source::{
    BackfillInfo, BoxSourceChunkStream, BoxSourceReaderEventStream, Column, SourceContextRef,
    SourceMessageEvent, SplitId, SplitImpl, SplitMetaData, SplitReader, into_chunk_event_stream,
    into_chunk_stream,
};

pub struct KafkaSplitReader {
    consumer: StreamConsumer<RwConsumerContext>,
    offsets: HashMap<SplitId, (Option<i64>, Option<i64>)>,
    backfill_info: HashMap<SplitId, BackfillInfo>,
    /// The furthest inclusive offset that is known to exist when the reader observes EOF for a
    /// split, even if that offset does not correspond to a visible data record.
    known_eof_offsets: HashMap<SplitId, i64>,
    splits: Vec<KafkaSplit>,
    sync_call_timeout: Duration,
    bytes_per_second: usize,
    max_num_messages: usize,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

#[async_trait]
impl SplitReader for KafkaSplitReader {
    type Properties = KafkaProperties;
    type Split = KafkaSplit;

    async fn new(
        properties: KafkaProperties,
        splits: Vec<KafkaSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        let mut config = ClientConfig::new();

        let bootstrap_servers = &properties.connection.brokers;
        let broker_rewrite_map = properties.privatelink_common.broker_rewrite_map.clone();

        // Enable partition EOF to emit split progress updates when the fetched data ends with
        // non-deliverable records, e.g. transactional control records in read-committed mode.
        config.set("enable.partition.eof", "true");
        config.set("auto.offset.reset", "smallest");
        config.set("isolation.level", KAFKA_ISOLATION_LEVEL);
        config.set("bootstrap.servers", bootstrap_servers);

        properties.connection.set_security_properties(&mut config);
        properties.set_client(&mut config);

        config.set("group.id", properties.group_id(source_ctx.fragment_id));

        let ctx_common = KafkaContextCommon::new(
            broker_rewrite_map,
            Some(format!(
                "fragment-{}-source-{}-actor-{}",
                source_ctx.fragment_id, source_ctx.source_id, source_ctx.actor_id
            )),
            // thread consumer will keep polling in the background, we don't need to call `poll`
            // explicitly
            Some(source_ctx.metrics.rdkafka_native_metric.clone()),
            properties.aws_auth_props,
            properties.connection.is_aws_msk_iam(),
        )
        .await?;

        let client_ctx = RwConsumerContext::new(ctx_common);

        if let Some(log_level) = read_kafka_log_level() {
            config.set_log_level(log_level);
        }
        let consumer: StreamConsumer<RwConsumerContext> = config
            .create_with_context(client_ctx)
            .await
            .context("failed to create kafka consumer")?;

        let mut tpl = TopicPartitionList::with_capacity(splits.len());

        let mut offsets = HashMap::new();
        let mut backfill_info = HashMap::new();
        let mut known_eof_offsets = HashMap::new();
        for split in splits.clone() {
            offsets.insert(split.id(), (split.start_offset, split.stop_offset));

            if let Some(offset) = split.start_offset {
                tpl.add_partition_offset(
                    split.topic.as_str(),
                    split.partition,
                    Offset::Offset(offset + 1),
                )?;
            } else {
                tpl.add_partition(split.topic.as_str(), split.partition);
            }

            let (low, high) = consumer
                .fetch_watermarks(
                    split.topic.as_str(),
                    split.partition,
                    properties.common.sync_call_timeout,
                )
                .await?;
            tracing::info!("fetch kafka watermarks: low: {low}, high: {high}, split: {split:?}");
            // note: low is inclusive, high is exclusive, start_offset is exclusive
            let has_data_to_backfill =
                low != high && split.start_offset.is_none_or(|offset| offset + 1 < high);
            let watermark_offset = if has_data_to_backfill {
                debug_assert!(high > 0);
                Some(high - 1)
            } else {
                None
            };
            if !has_data_to_backfill {
                backfill_info.insert(split.id(), BackfillInfo::NoDataToBackfill);
            } else {
                backfill_info.insert(
                    split.id(),
                    BackfillInfo::HasDataToBackfill {
                        latest_offset: (high - 1).to_string(),
                    },
                );
            }

            let stop_offset = split.stop_offset.and_then(Self::offset_before);
            if let Some(known_eof_offset) = Self::max_known_offset(watermark_offset, stop_offset) {
                known_eof_offsets.insert(split.id(), known_eof_offset);
            }
        }
        tracing::info!(
            topic = properties.common.topic,
            source_name = source_ctx.source_name,
            fragment_id = %source_ctx.fragment_id,
            source_id = %source_ctx.source_id,
            actor_id = %source_ctx.actor_id,
            "backfill_info: {:?}",
            backfill_info
        );

        consumer.assign(&tpl)?;

        // The two parameters below are only used by developers for performance testing purposes,
        // so we panic here on purpose if the input is not correctly recognized.
        let bytes_per_second = match properties.bytes_per_second {
            None => usize::MAX,
            Some(number) => number
                .parse::<usize>()
                .expect("bytes.per.second expect usize"),
        };
        let max_num_messages = match properties.max_num_messages {
            None => usize::MAX,
            Some(number) => number
                .parse::<usize>()
                .expect("max.num.messages expect usize"),
        };

        Ok(Self {
            consumer,
            offsets,
            splits,
            backfill_info,
            known_eof_offsets,
            bytes_per_second,
            sync_call_timeout: properties.common.sync_call_timeout,
            max_num_messages,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        let data_stream = self
            .into_data_event_stream()
            .try_filter_map(|event| async move {
                Ok(match event {
                    SourceMessageEvent::Data(batch) => Some(batch),
                    SourceMessageEvent::SplitProgress(_) => None,
                })
            });
        into_chunk_stream(data_stream, parser_config, source_context)
    }

    fn into_event_stream(self) -> BoxSourceReaderEventStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_event_stream(self.into_data_event_stream(), parser_config, source_context)
    }

    fn backfill_info(&self) -> HashMap<SplitId, BackfillInfo> {
        self.backfill_info.clone()
    }

    async fn seek_to_latest(&mut self) -> Result<Vec<SplitImpl>> {
        let mut latest_splits: Vec<SplitImpl> = Vec::new();
        let mut tpl = TopicPartitionList::with_capacity(self.splits.len());
        for mut split in self.splits.clone() {
            // we can't get latest offset if we use Offset::End, so we just fetch watermark here.
            let (_low, high) = self
                .consumer
                .fetch_watermarks(
                    split.topic.as_str(),
                    split.partition,
                    self.sync_call_timeout,
                )
                .await?;
            tpl.add_partition_offset(split.topic.as_str(), split.partition, Offset::Offset(high))?;
            split.start_offset = Some(high - 1);
            latest_splits.push(split.into());
        }
        // replace the previous assignment
        self.consumer.assign(&tpl)?;
        Ok(latest_splits)
    }
}

impl KafkaSplitReader {
    fn offset_before(next_offset: i64) -> Option<i64> {
        next_offset.checked_sub(1).filter(|offset| *offset >= 0)
    }

    fn max_known_offset(current_offset: Option<i64>, candidate_offset: Option<i64>) -> Option<i64> {
        match (current_offset, candidate_offset) {
            (Some(current_offset), Some(candidate_offset)) => {
                Some(current_offset.max(candidate_offset))
            }
            (Some(current_offset), None) => Some(current_offset),
            (None, Some(candidate_offset)) => Some(candidate_offset),
            (None, None) => None,
        }
    }

    fn record_split_progress(
        progress: &mut HashMap<SplitId, String>,
        latest_progress_offsets: &mut HashMap<SplitId, i64>,
        split_id: &SplitId,
        inclusive_offset: i64,
    ) {
        let should_emit = latest_progress_offsets
            .get(split_id)
            .is_none_or(|offset| *offset < inclusive_offset);
        if should_emit {
            latest_progress_offsets.insert(split_id.clone(), inclusive_offset);
            progress.insert(split_id.clone(), inclusive_offset.to_string());
        }
    }

    fn drain_stop_offsets_with_progress(
        stop_offsets: &mut HashMap<SplitId, i64>,
        progress: &HashMap<SplitId, String>,
    ) {
        let mut finished_splits = Vec::new();

        for (split_id, progress_offset) in progress {
            let Some(stop_offset) = stop_offsets.get(split_id).copied() else {
                continue;
            };
            let Ok(progress_offset) = progress_offset.parse::<i64>() else {
                tracing::warn!(
                    split_id = split_id.as_ref(),
                    progress_offset = progress_offset.as_str(),
                    "invalid split progress offset from kafka reader"
                );
                continue;
            };

            // `stop_offset` is exclusive while progress is inclusive.
            if progress_offset >= stop_offset - 1 {
                tracing::debug!(
                    split_id = split_id.as_ref(),
                    stop_offset,
                    progress_offset,
                    "stop offset reached by split progress"
                );
                finished_splits.push(split_id.clone());
            }
        }

        for split_id in finished_splits {
            stop_offsets.remove(&split_id);
        }
    }

    fn snapshot_split_progress(
        eof_offsets: &HashMap<SplitId, i64>,
        latest_progress_offsets: &mut HashMap<SplitId, i64>,
    ) -> Option<HashMap<SplitId, String>> {
        let mut progress = HashMap::new();

        for (split_id, inclusive_offset) in eof_offsets {
            Self::record_split_progress(
                &mut progress,
                latest_progress_offsets,
                split_id,
                *inclusive_offset,
            );
        }

        (!progress.is_empty()).then_some(progress)
    }

    fn resolve_eof_offsets(&self, eof_partitions: &HashSet<i32>) -> Result<HashMap<SplitId, i64>> {
        let positions = self.consumer.position()?;
        let mut eof_offsets = HashMap::new();

        for split in &self.splits {
            if !eof_partitions.contains(&split.partition) {
                continue;
            }

            let split_id = split.id();
            let position_offset = positions
                .find_partition(split.topic.as_str(), split.partition)
                .and_then(|position| match position.offset() {
                    Offset::Offset(next_offset) => Self::offset_before(next_offset),
                    _ => None,
                });
            let Some(inclusive_offset) = Self::max_known_offset(
                position_offset,
                self.known_eof_offsets.get(&split_id).copied(),
            ) else {
                continue;
            };
            eof_offsets.insert(split_id, inclusive_offset);
        }

        Ok(eof_offsets)
    }

    fn apply_split_progress(
        stop_offsets: &mut HashMap<SplitId, i64>,
        split_progress: Option<HashMap<SplitId, String>>,
        is_bounded: bool,
    ) -> Option<HashMap<SplitId, String>> {
        let progress = split_progress?;
        if is_bounded {
            Self::drain_stop_offsets_with_progress(stop_offsets, &progress);
        }
        Some(progress)
    }

    #[try_stream(ok = SourceMessageEvent, error = crate::error::ConnectorError)]
    async fn into_data_event_stream(self) {
        if self.offsets.values().all(|(start_offset, stop_offset)| {
            match (start_offset, stop_offset) {
                (Some(start), Some(stop)) if (*start + 1) >= *stop => true,
                (_, Some(stop)) if *stop == 0 => true,
                _ => false,
            }
        }) {
            yield SourceMessageEvent::Data(Vec::new());
            return Ok(());
        };

        let mut stop_offsets: HashMap<_, _> = self
            .offsets
            .iter()
            .flat_map(|(split_id, (_, stop_offset))| {
                stop_offset
                    .filter(|offset| *offset > 0)
                    .map(|offset| (split_id.clone() as SplitId, offset))
            })
            .collect();
        let is_bounded = !stop_offsets.is_empty();

        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.tick().await;
        let mut bytes_current_second = 0;
        let mut num_messages = 0;
        let max_chunk_size = self.source_ctx.source_ctrl_opts.chunk_size;
        let mut res = Vec::with_capacity(max_chunk_size);
        // ingest kafka message header can be expensive, do it only when required
        let require_message_header = self.parser_config.common.rw_columns.iter().any(|col_desc| {
            matches!(
                col_desc.additional_column.column_type,
                Some(AdditionalColumnType::Headers(_) | AdditionalColumnType::HeaderInner(_))
            )
        });

        let mut latest_message_id_metrics: HashMap<String, LabelGuardedIntGauge> = HashMap::new();
        let mut partition_eof_count_metrics: HashMap<String, LabelGuardedIntCounter> =
            HashMap::new();
        let mut partition_eof_offset_metrics: HashMap<String, LabelGuardedIntGauge> =
            HashMap::new();
        let mut latest_progress_offsets = self
            .splits
            .iter()
            .filter_map(|split| split.start_offset.map(|offset| (split.id(), offset)))
            .collect::<HashMap<_, _>>();

        #[for_await]
        'for_outer_loop: for raw_msgs in self.consumer.stream().ready_chunks(max_chunk_size) {
            let mut eof_partitions = HashSet::new();
            let mut msgs = Vec::with_capacity(max_chunk_size);
            for msg in raw_msgs {
                match msg {
                    Ok(msg) => msgs.push(msg),
                    Err(KafkaError::PartitionEOF(partition)) => {
                        let split_id = partition.to_string();
                        partition_eof_count_metrics
                            .entry(split_id.clone())
                            .or_insert_with(|| {
                                self.source_ctx
                                    .metrics
                                    .partition_eof_count
                                    .with_guarded_label_values(&[
                                        &self.source_ctx.source_id.to_string(),
                                        &split_id,
                                        &self.source_ctx.source_name,
                                        &self.source_ctx.fragment_id.to_string(),
                                    ])
                            })
                            .inc();
                        eof_partitions.insert(partition);
                    }
                    Err(err) => return Err(err.into()),
                }
            }

            let eof_offsets = if eof_partitions.is_empty() {
                HashMap::new()
            } else {
                self.resolve_eof_offsets(&eof_partitions)?
            };
            for (split_id, eof_offset) in &eof_offsets {
                let split_id_label = split_id.as_ref().to_owned();
                partition_eof_offset_metrics
                    .entry(split_id_label.clone())
                    .or_insert_with(|| {
                        self.source_ctx
                            .metrics
                            .partition_eof_offset
                            .with_guarded_label_values(&[
                                &self.source_ctx.source_id.to_string(),
                                &split_id_label,
                                &self.source_ctx.source_name,
                                &self.source_ctx.fragment_id.to_string(),
                            ])
                    })
                    .set(*eof_offset);
                tracing::info!(
                    actor_id = %self.source_ctx.actor_id,
                    source_id = %self.source_ctx.source_id,
                    source_name = self.source_ctx.source_name,
                    fragment_id = %self.source_ctx.fragment_id,
                    split_id = %split_id,
                    eof_offset,
                    "received kafka partition EOF"
                );
            }
            let split_progress =
                Self::snapshot_split_progress(&eof_offsets, &mut latest_progress_offsets);
            let split_progress =
                Self::apply_split_progress(&mut stop_offsets, split_progress, is_bounded);

            if msgs.is_empty() {
                if let Some(progress) = split_progress {
                    yield SourceMessageEvent::SplitProgress(progress);
                }
                if is_bounded && stop_offsets.is_empty() {
                    if !res.is_empty() {
                        yield SourceMessageEvent::Data(res);
                    }
                    break 'for_outer_loop;
                }
                continue;
            }

            let mut split_msg_offsets = HashMap::new();

            for msg in &msgs {
                split_msg_offsets.insert(msg.partition(), msg.offset());
            }

            for (partition, offset) in split_msg_offsets {
                let split_id = partition.to_string();
                latest_message_id_metrics
                    .entry(split_id.clone())
                    .or_insert_with(|| {
                        self.source_ctx
                            .metrics
                            .latest_message_id
                            .with_guarded_label_values(&[
                                // source name is not available here
                                &self.source_ctx.actor_id.to_string(),
                                &self.source_ctx.source_id.to_string(),
                                &split_id,
                            ])
                    })
                    .set(offset);
            }

            for msg in msgs {
                let cur_offset = msg.offset();
                bytes_current_second += match &msg.payload() {
                    None => 0,
                    Some(payload) => payload.len(),
                };
                num_messages += 1;
                let source_message =
                    SourceMessage::from_kafka_message(&msg, require_message_header);
                let split_id = source_message.split_id.clone();
                res.push(source_message);

                if let Entry::Occupied(o) = stop_offsets.entry(split_id) {
                    let stop_offset = *o.get();

                    if cur_offset == stop_offset - 1 {
                        tracing::debug!(
                            "stop offset reached for split {}, stop reading, offset: {}, stop offset: {}",
                            o.key(),
                            cur_offset,
                            stop_offset
                        );

                        o.remove();

                        if stop_offsets.is_empty() {
                            yield SourceMessageEvent::Data(res);
                            break 'for_outer_loop;
                        }

                        continue;
                    }
                }

                if bytes_current_second > self.bytes_per_second {
                    // swap to make compiler happy
                    let mut cur = Vec::with_capacity(res.capacity());
                    swap(&mut cur, &mut res);
                    yield SourceMessageEvent::Data(cur);
                    interval.tick().await;
                    bytes_current_second = 0;
                    res.clear();
                }
                if num_messages >= self.max_num_messages {
                    yield SourceMessageEvent::Data(res);
                    break 'for_outer_loop;
                }
            }
            let mut cur = Vec::with_capacity(res.capacity());
            swap(&mut cur, &mut res);
            yield SourceMessageEvent::Data(cur);
            if let Some(progress) = split_progress {
                yield SourceMessageEvent::SplitProgress(progress);
            }
            if is_bounded && stop_offsets.is_empty() {
                break 'for_outer_loop;
            }
            // don't clear `bytes_current_second` here as it is only related to `.tick()`.
            // yield in the outer loop so that we can always guarantee that some messages are read
            // every `MAX_CHUNK_SIZE`.
        }
        tracing::info!("kafka reader finished");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::KafkaSplitReader;
    use crate::source::SplitId;

    #[test]
    fn test_drain_stop_offsets_with_progress() {
        let mut stop_offsets = HashMap::from([
            (SplitId::from("0"), 10_i64),
            (SplitId::from("1"), 15_i64),
            (SplitId::from("2"), 20_i64),
        ]);
        let progress = HashMap::from([
            (SplitId::from("0"), "9".to_owned()),
            (SplitId::from("1"), "13".to_owned()),
            (SplitId::from("2"), "invalid".to_owned()),
        ]);

        KafkaSplitReader::drain_stop_offsets_with_progress(&mut stop_offsets, &progress);

        assert!(!stop_offsets.contains_key("0"));
        assert_eq!(stop_offsets.get("1"), Some(&15));
        assert_eq!(stop_offsets.get("2"), Some(&20));
    }

    #[test]
    fn test_drain_stop_offsets_with_progress_for_zero_stop_offset() {
        let mut stop_offsets = HashMap::from([(SplitId::from("0"), 0_i64)]);
        let progress = HashMap::from([(SplitId::from("0"), "0".to_owned())]);

        KafkaSplitReader::drain_stop_offsets_with_progress(&mut stop_offsets, &progress);

        assert!(stop_offsets.is_empty());
    }

    #[test]
    fn test_max_known_offset_prefers_larger_offset() {
        assert_eq!(
            KafkaSplitReader::max_known_offset(Some(3), Some(4)),
            Some(4)
        );
        assert_eq!(
            KafkaSplitReader::max_known_offset(Some(7), Some(4)),
            Some(7)
        );
        assert_eq!(KafkaSplitReader::max_known_offset(None, Some(4)), Some(4));
        assert_eq!(KafkaSplitReader::max_known_offset(Some(4), None), Some(4));
    }

    #[test]
    fn test_apply_split_progress_for_empty_message_batch() {
        let mut stop_offsets = HashMap::from([(SplitId::from("0"), 5_i64)]);
        let split_progress = Some(HashMap::from([(SplitId::from("0"), "4".to_owned())]));

        let applied =
            KafkaSplitReader::apply_split_progress(&mut stop_offsets, split_progress, true);

        assert_eq!(
            applied,
            Some(HashMap::from([(SplitId::from("0"), "4".to_owned())]))
        );
        assert!(stop_offsets.is_empty());
    }
}
