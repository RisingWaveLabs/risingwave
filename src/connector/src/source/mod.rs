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

pub mod prelude {
    // import all split enumerators
    pub use crate::source::datagen::DatagenSplitEnumerator;
    pub use crate::source::filesystem::LegacyS3SplitEnumerator;
    pub use crate::source::filesystem::opendal_source::OpendalEnumerator;
    pub use crate::source::google_pubsub::PubsubSplitEnumerator as GooglePubsubSplitEnumerator;
    pub use crate::source::iceberg::IcebergSplitEnumerator;
    pub use crate::source::kafka::KafkaSplitEnumerator;
    pub use crate::source::kinesis::KinesisSplitEnumerator;
    pub use crate::source::mqtt::MqttSplitEnumerator;
    pub use crate::source::nats::NatsSplitEnumerator;
    pub use crate::source::nexmark::NexmarkSplitEnumerator;
    pub use crate::source::pulsar::PulsarSplitEnumerator;
    pub use crate::source::test_source::TestSourceSplitEnumerator as TestSplitEnumerator;
    pub type AzblobSplitEnumerator =
        OpendalEnumerator<crate::source::filesystem::opendal_source::OpendalAzblob>;
    pub type GcsSplitEnumerator =
        OpendalEnumerator<crate::source::filesystem::opendal_source::OpendalGcs>;
    pub type OpendalS3SplitEnumerator =
        OpendalEnumerator<crate::source::filesystem::opendal_source::OpendalS3>;
    pub type PosixFsSplitEnumerator =
        OpendalEnumerator<crate::source::filesystem::opendal_source::OpendalPosixFs>;
    pub use crate::source::cdc::enumerator::DebeziumSplitEnumerator;
    pub use crate::source::filesystem::opendal_source::BatchPosixFsEnumerator as BatchPosixFsSplitEnumerator;
    pub type CitusCdcSplitEnumerator = DebeziumSplitEnumerator<crate::source::cdc::Citus>;
    pub type MongodbCdcSplitEnumerator = DebeziumSplitEnumerator<crate::source::cdc::Mongodb>;
    pub type PostgresCdcSplitEnumerator = DebeziumSplitEnumerator<crate::source::cdc::Postgres>;
    pub type MysqlCdcSplitEnumerator = DebeziumSplitEnumerator<crate::source::cdc::Mysql>;
    pub type SqlServerCdcSplitEnumerator = DebeziumSplitEnumerator<crate::source::cdc::SqlServer>;
    pub type OracleCdcSplitEnumerator = DebeziumSplitEnumerator<crate::source::cdc::Oracle>;
}

pub mod base;
pub mod batch;
pub mod cdc;
pub mod data_gen_util;
pub mod datagen;
pub mod filesystem;
pub mod google_pubsub;
pub mod kafka;
pub mod kinesis;
pub mod monitor;
pub mod mqtt;
pub mod nats;
pub mod nexmark;
pub mod pulsar;
pub mod utils;

mod util;
use std::collections::HashMap;
use std::future::IntoFuture;
use std::time::Duration;

pub use base::{UPSTREAM_SOURCE_KEY, WEBHOOK_CONNECTOR, *};
pub use batch::BatchSourceSplitImpl;
pub(crate) use common::*;
use google_cloud_pubsub::subscription::Subscription;
pub use google_pubsub::GOOGLE_PUBSUB_CONNECTOR;
pub use kafka::KAFKA_CONNECTOR;
pub use kinesis::KINESIS_CONNECTOR;
use monitor::{ConnectorAckFailureType, GLOBAL_SOURCE_METRICS};
pub use mqtt::MQTT_CONNECTOR;
pub use nats::NATS_CONNECTOR;
use utils::feature_gated_source_mod;

pub use self::adbc_snowflake::ADBC_SNOWFLAKE_CONNECTOR;
mod common;
pub mod iceberg;
mod manager;
pub mod reader;
pub mod test_source;
feature_gated_source_mod!(adbc_snowflake, "adbc_snowflake");

use async_nats::jetstream::consumer::AckPolicy as JetStreamAckPolicy;
use async_nats::jetstream::context::Context as JetStreamContext;
pub use manager::{SourceColumnDesc, SourceColumnType};
use risingwave_common::array::{Array, ArrayRef};
use risingwave_common::row::OwnedRow;
use risingwave_pb::id::{ActorId, SourceId};
use thiserror_ext::AsReport;
pub use util::fill_adaptive_split;

pub use crate::source::filesystem::LEGACY_S3_CONNECTOR;
pub use crate::source::filesystem::opendal_source::{
    AZBLOB_CONNECTOR, BATCH_POSIX_FS_CONNECTOR, GCS_CONNECTOR, OPENDAL_S3_CONNECTOR,
    POSIX_FS_CONNECTOR,
};
pub use crate::source::nexmark::NEXMARK_CONNECTOR;
pub use crate::source::pulsar::PULSAR_CONNECTOR;
use crate::source::pulsar::source::reader::PULSAR_ACK_CHANNEL;

pub fn should_copy_to_format_encode_options(key: &str, connector: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "schema.registry",
        "schema.location",
        "message",
        "key.message",
        "without_header",
        "delimiter",
        // AwsAuthProps
        "region",
        "endpoint_url",
        "access_key",
        "secret_key",
        "session_token",
        "arn",
        "external_id",
        "profile",
    ];
    PREFIXES.iter().any(|prefix| key.starts_with(prefix))
        || (key == "endpoint" && !connector.eq_ignore_ascii_case(KINESIS_CONNECTOR))
}

/// Tasks executed by `WaitCheckpointWorker`
pub enum WaitCheckpointTask {
    CommitCdcOffset(Option<(SplitId, String)>),
    AckPubsubMessage(Subscription, Vec<ArrayRef>),
    AckNatsJetStream(JetStreamContext, Vec<ArrayRef>, JetStreamAckPolicy),
    AckPulsarMessage(Vec<(String, ArrayRef)>),
}

impl WaitCheckpointTask {
    /// Create a fresh task for the next epoch, reusing expensive-to-create clients
    /// (e.g. `PubSub` `Subscription`, NATS `JetStreamContext`) from the current task.
    /// This avoids re-establishing gRPC/network connections on every checkpoint.
    pub fn reset_for_next_epoch(&self) -> Self {
        match self {
            WaitCheckpointTask::CommitCdcOffset(_) => WaitCheckpointTask::CommitCdcOffset(None),
            WaitCheckpointTask::AckPubsubMessage(subscription, _) => {
                WaitCheckpointTask::AckPubsubMessage(subscription.clone(), vec![])
            }
            WaitCheckpointTask::AckNatsJetStream(context, _, ack_policy) => {
                WaitCheckpointTask::AckNatsJetStream(context.clone(), vec![], *ack_policy)
            }
            WaitCheckpointTask::AckPulsarMessage(_) => WaitCheckpointTask::AckPulsarMessage(vec![]),
        }
    }

    pub async fn run(self, source_id: SourceId, source_name: &str) {
        self.run_with_on_commit_success(source_id, source_name, |_source_id, _offset| {
            // Default implementation: no action on commit success
        })
        .await;
    }

    pub async fn run_with_on_commit_success<F>(
        self,
        source_id: SourceId,
        source_name: &str,
        mut on_commit_success: F,
    ) where
        F: FnMut(u64, &str),
    {
        use std::str::FromStr;
        let source_id_label = source_id.to_string();
        match self {
            WaitCheckpointTask::CommitCdcOffset(updated_offset) => {
                if let Some((split_id, offset)) = updated_offset {
                    let committed_source_id: u64 = u64::from_str(split_id.as_ref()).unwrap();
                    // notify cdc connector to commit offset
                    match cdc::jni_source::commit_cdc_offset(committed_source_id, offset.clone()) {
                        Ok(()) => {
                            // Execute callback after successful commit
                            on_commit_success(committed_source_id, &offset);
                        }
                        Err(e) => {
                            tracing::error!(
                                source_id = committed_source_id,
                                source_name,
                                error = %e.as_report(),
                                "source#{committed_source_id}: failed to commit cdc offset: {offset}.",
                            )
                        }
                    }
                }
            }
            WaitCheckpointTask::AckPulsarMessage(ack_array) => {
                let mut latest_ack_by_channel = HashMap::new();
                for (ack_channel_id, to_cumulative_ack) in ack_array {
                    let encode_message_id_data = to_cumulative_ack
                        .as_bytea()
                        .iter()
                        .flatten()
                        .last()
                        .map(|message_id| message_id.to_owned());

                    if let Some(encode_message_id_data) = encode_message_id_data {
                        latest_ack_by_channel.insert(ack_channel_id, Some(encode_message_id_data));
                    } else {
                        latest_ack_by_channel.entry(ack_channel_id).or_insert(None);
                    }
                }

                for (ack_channel_id, encode_message_id_data) in latest_ack_by_channel {
                    let Some(encode_message_id_data) = encode_message_id_data else {
                        GLOBAL_SOURCE_METRICS.inc_connector_ack_failure_count(
                            source_name,
                            "pulsar",
                            ConnectorAckFailureType::EmptyMessageId,
                        );
                        tracing::warn!(
                            source_id = source_id_label,
                            source_name,
                            ack_channel_id,
                            "skip Pulsar ack because the checkpoint ack batches have no message id",
                        );
                        continue;
                    };

                    let Some(ack_tx) = PULSAR_ACK_CHANNEL.lock().get(&ack_channel_id).cloned()
                    else {
                        GLOBAL_SOURCE_METRICS.inc_connector_ack_failure_count(
                            source_name,
                            "pulsar",
                            ConnectorAckFailureType::ChannelMissing,
                        );
                        tracing::warn!(
                            source_id = source_id_label,
                            source_name,
                            ack_channel_id,
                            "skip Pulsar ack because the ack channel is missing",
                        );
                        continue;
                    };

                    if let Err(e) = ack_tx.send(encode_message_id_data) {
                        GLOBAL_SOURCE_METRICS.inc_connector_ack_failure_count(
                            source_name,
                            "pulsar",
                            ConnectorAckFailureType::ChannelSendError,
                        );
                        tracing::warn!(
                            source_id = source_id_label,
                            source_name,
                            ack_channel_id,
                            error = %e.as_report(),
                            "failed to send Pulsar ack message id to the reader ack channel",
                        );
                    }
                }
            }
            WaitCheckpointTask::AckPubsubMessage(subscription, ack_id_arrs) => {
                const ACK_RPC_TIMEOUT: Duration = Duration::from_secs(30);
                async fn ack(
                    subscription: &Subscription,
                    ack_ids: Vec<String>,
                    source_id_label: &str,
                    source_name: &str,
                ) {
                    if ack_ids.is_empty() {
                        return;
                    }
                    tracing::trace!("acking pubsub messages {:?}", ack_ids);
                    match tokio::time::timeout(ACK_RPC_TIMEOUT, subscription.ack(ack_ids)).await {
                        Ok(Ok(())) => {
                            GLOBAL_SOURCE_METRICS
                                .inc_connector_ack_success_count(source_name, "pubsub");
                        }
                        Ok(Err(e)) => {
                            GLOBAL_SOURCE_METRICS.inc_connector_ack_failure_count(
                                source_name,
                                "pubsub",
                                ConnectorAckFailureType::Error,
                            );
                            tracing::error!(
                                source_id = source_id_label,
                                source_name,
                                error = %e.as_report(),
                                "failed to ack pubsub messages",
                            )
                        }
                        Err(_) => {
                            GLOBAL_SOURCE_METRICS.inc_connector_ack_failure_count(
                                source_name,
                                "pubsub",
                                ConnectorAckFailureType::Timeout,
                            );
                            tracing::error!(
                                source_id = source_id_label,
                                source_name,
                                "pubsub ack timed out after {ACK_RPC_TIMEOUT:?}",
                            )
                        }
                    }
                }
                const MAX_ACK_BATCH_SIZE: usize = 1000;
                let mut ack_ids: Vec<String> = vec![];
                for arr in ack_id_arrs {
                    for ack_id in arr.as_utf8().iter().flatten() {
                        ack_ids.push(ack_id.to_owned());
                        if ack_ids.len() >= MAX_ACK_BATCH_SIZE {
                            ack(
                                &subscription,
                                std::mem::take(&mut ack_ids),
                                &source_id_label,
                                source_name,
                            )
                            .await;
                        }
                    }
                }
                ack(&subscription, ack_ids, &source_id_label, source_name).await;
            }
            WaitCheckpointTask::AckNatsJetStream(
                ref context,
                reply_subjects_arrs,
                ref ack_policy,
            ) => {
                const ACK_RPC_TIMEOUT: Duration = Duration::from_secs(30);
                async fn ack(
                    context: &JetStreamContext,
                    reply_subject: String,
                    source_id_label: &str,
                    source_name: &str,
                ) {
                    let fut = async {
                        let ack_future = context
                            .publish(reply_subject.clone(), "+ACK".into())
                            .await
                            .map_err(|e| e.to_report_string())?;
                        ack_future
                            .into_future()
                            .await
                            .map_err(|e| e.to_report_string())?;
                        Ok::<(), String>(())
                    };
                    match tokio::time::timeout(ACK_RPC_TIMEOUT, fut).await {
                        Ok(Ok(())) => {
                            GLOBAL_SOURCE_METRICS
                                .inc_connector_ack_success_count(source_name, "nats_jetstream");
                        }
                        Ok(Err(e)) => {
                            GLOBAL_SOURCE_METRICS.inc_connector_ack_failure_count(
                                source_name,
                                "nats_jetstream",
                                ConnectorAckFailureType::Error,
                            );
                            tracing::error!(
                                source_id = source_id_label,
                                source_name,
                                error = %e,
                                subject = ?reply_subject,
                                "failed to ack NATS JetStream message",
                            );
                        }
                        Err(_) => {
                            GLOBAL_SOURCE_METRICS.inc_connector_ack_failure_count(
                                source_name,
                                "nats_jetstream",
                                ConnectorAckFailureType::Timeout,
                            );
                            tracing::error!(
                                source_id = source_id_label,
                                source_name,
                                subject = ?reply_subject,
                                "NATS JetStream ack timed out after {ACK_RPC_TIMEOUT:?}",
                            );
                        }
                    }
                }

                let reply_subjects = reply_subjects_arrs
                    .iter()
                    .flat_map(|arr| {
                        arr.as_utf8()
                            .iter()
                            .flatten()
                            .map(|s| s.to_owned())
                            .collect::<Vec<String>>()
                    })
                    .collect::<Vec<String>>();

                match ack_policy {
                    JetStreamAckPolicy::None | JetStreamAckPolicy::FlowControl => (),
                    JetStreamAckPolicy::Explicit => {
                        for reply_subject in reply_subjects {
                            if reply_subject.is_empty() {
                                continue;
                            }
                            ack(context, reply_subject, &source_id_label, source_name).await;
                        }
                    }
                    JetStreamAckPolicy::All => {
                        if let Some(reply_subject) = reply_subjects.last() {
                            ack(
                                context,
                                reply_subject.clone(),
                                &source_id_label,
                                source_name,
                            )
                            .await;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CdcTableSnapshotSplitCommon<T: Clone> {
    pub split_id: i64,
    pub left_bound_inclusive: T,
    pub right_bound_exclusive: T,
}

pub type CdcTableSnapshotSplit = CdcTableSnapshotSplitCommon<OwnedRow>;
pub type CdcTableSnapshotSplitRaw = CdcTableSnapshotSplitCommon<Vec<u8>>;

/// Build the identifier of the ACK channel for a Pulsar reader.
///
/// Multiple actors can consume the same source split in one process, so the actor ID is required
/// to prevent one reader from replacing another reader's channel.
#[inline]
pub fn build_pulsar_ack_channel_id(
    source_id: SourceId,
    split_id: &SplitId,
    actor_id: ActorId,
) -> String {
    format!("{}-{}-{}", source_id, split_id, actor_id)
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Array, BytesArray};
    use tokio::sync::mpsc::error::TryRecvError;

    use super::*;

    #[test]
    fn test_pulsar_ack_channel_id_is_actor_scoped() {
        let source_id = SourceId::new(7);
        let split_id: SplitId = "persistent://public/default/topic".into();

        let first = build_pulsar_ack_channel_id(source_id, &split_id, ActorId::new(11));
        let second = build_pulsar_ack_channel_id(source_id, &split_id, ActorId::new(12));

        assert_ne!(first, second);
    }

    fn message_ids<const N: usize>(values: [Option<&[u8]>; N]) -> ArrayRef {
        BytesArray::from_iter(values).into_ref()
    }

    #[tokio::test]
    async fn test_ack_pulsar_message_for_each_split() {
        let split_0_channel = "test-pulsar-ack-multiple-splits-0".to_owned();
        let split_1_channel = "test-pulsar-ack-multiple-splits-1".to_owned();
        let empty_split_channel = "test-pulsar-ack-multiple-splits-empty".to_owned();
        let (split_0_tx, mut split_0_rx) = tokio::sync::mpsc::unbounded_channel();
        let (split_1_tx, mut split_1_rx) = tokio::sync::mpsc::unbounded_channel();
        let (empty_split_tx, mut empty_split_rx) = tokio::sync::mpsc::unbounded_channel();

        PULSAR_ACK_CHANNEL
            .lock()
            .insert(split_0_channel.clone(), split_0_tx);
        PULSAR_ACK_CHANNEL
            .lock()
            .insert(split_1_channel.clone(), split_1_tx);
        PULSAR_ACK_CHANNEL
            .lock()
            .insert(empty_split_channel.clone(), empty_split_tx);

        WaitCheckpointTask::AckPulsarMessage(vec![
            (split_0_channel.clone(), message_ids([Some(b"split-0-old")])),
            (
                split_1_channel.clone(),
                message_ids([Some(b"split-1-latest"), None]),
            ),
            (
                split_0_channel.clone(),
                message_ids([Some(b"split-0-latest")]),
            ),
            (split_0_channel.clone(), message_ids([None])),
            (empty_split_channel.clone(), message_ids([None, None])),
        ])
        .run(SourceId::new(26891), "test_pulsar_source")
        .await;

        assert_eq!(split_0_rx.try_recv().unwrap(), b"split-0-latest");
        assert_eq!(split_0_rx.try_recv(), Err(TryRecvError::Empty));
        assert_eq!(split_1_rx.try_recv().unwrap(), b"split-1-latest");
        assert_eq!(split_1_rx.try_recv(), Err(TryRecvError::Empty));
        assert_eq!(empty_split_rx.try_recv(), Err(TryRecvError::Empty));

        let mut ack_channels = PULSAR_ACK_CHANNEL.lock();
        ack_channels.remove(&split_0_channel);
        ack_channels.remove(&split_1_channel);
        ack_channels.remove(&empty_split_channel);
    }
}
