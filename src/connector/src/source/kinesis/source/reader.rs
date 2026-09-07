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

use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use aws_sdk_kinesis::Client as KinesisClient;
use aws_sdk_kinesis::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_kinesis::operation::get_records::{GetRecordsError, GetRecordsOutput};
use aws_sdk_kinesis::primitives::DateTime;
use aws_sdk_kinesis::types::ShardIteratorType;
use futures_async_stream::try_stream;
use risingwave_common::bail;
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntCounter};
use risingwave_common::util::retry::exponential_backoff;
use thiserror_ext::AsReport;

use crate::error::ConnectorResult as Result;
use crate::parser::ParserConfig;
use crate::source::kinesis::KinesisProperties;
use crate::source::kinesis::source::message::from_kinesis_record;
use crate::source::kinesis::split::{KinesisOffset, KinesisSplit};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId, SplitMetaData,
    SplitReader, into_chunk_stream,
};

#[derive(Debug, Clone)]
pub struct KinesisSplitReader {
    client: KinesisClient,
    stream_name: String,
    shard_id: SplitId,
    latest_offset: Option<String>,
    shard_iter: Option<String>,
    next_offset: KinesisOffset,
    #[expect(dead_code)]
    end_offset: KinesisOffset,

    split_id: SplitId,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,

    eof_retry_interval: Duration,
    error_retry_interval: Duration,

    kinesis_throughput_exceeded_count: LabelGuardedIntCounter,
    kinesis_timeout_count: LabelGuardedIntCounter,
    kinesis_rebuild_shard_iter_count: LabelGuardedIntCounter,
    kinesis_early_terminate_shard_count: LabelGuardedIntCounter,
    kinesis_lag_latency_ms: LabelGuardedHistogram,
}

#[async_trait]
impl SplitReader for KinesisSplitReader {
    type Properties = KinesisProperties;
    type Split = KinesisSplit;

    async fn new(
        properties: KinesisProperties,
        splits: Vec<KinesisSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        assert!(splits.len() == 1);

        let split = splits.into_iter().next().unwrap();

        let next_offset = match &split.next_offset {
            KinesisOffset::None => match &properties.scan_startup_mode {
                None => KinesisOffset::Earliest,
                Some(mode) => match mode.as_str() {
                    "earliest" => KinesisOffset::Earliest,
                    "latest" => KinesisOffset::Latest,
                    "timestamp" => {
                        if let Some(ts) = &properties.start_timestamp_millis {
                            KinesisOffset::Timestamp(*ts)
                        } else {
                            bail!("scan.startup.timestamp.millis is required");
                        }
                    }
                    _ => {
                        bail!("invalid scan.startup.mode, accept earliest/latest/timestamp")
                    }
                },
            },
            next_offset => next_offset.to_owned(),
        };

        if !matches!(next_offset, KinesisOffset::Timestamp(_))
            && properties.start_timestamp_millis.is_some()
        {
            // cannot bail! here because all new split readers will fail to start if user set 'scan.startup.mode' to 'timestamp'
            tracing::warn!(
                "scan.startup.mode needs to be set to 'timestamp' if you want to start with a specific timestamp, starting shard {} from the beginning",
                split.id()
            );
        }

        let stream_name = properties.common.stream_name.clone();
        let client = properties.common.build_client().await?;

        let metrics_labels = [
            source_ctx.source_id.to_string(),
            source_ctx.source_name.clone(),
            source_ctx.fragment_id.to_string(),
            split.shard_id.to_string(),
        ];
        let kinesis_throughput_exceeded_count = source_ctx
            .metrics
            .kinesis_throughput_exceeded_count
            .with_guarded_label_values(&metrics_labels);
        let kinesis_timeout_count = source_ctx
            .metrics
            .kinesis_timeout_count
            .with_guarded_label_values(&metrics_labels);
        let kinesis_rebuild_shard_iter_count = source_ctx
            .metrics
            .kinesis_rebuild_shard_iter_count
            .with_guarded_label_values(&metrics_labels);
        let kinesis_early_terminate_shard_count = source_ctx
            .metrics
            .kinesis_early_terminate_shard_count
            .with_guarded_label_values(&metrics_labels);
        let kinesis_lag_latency_ms = source_ctx
            .metrics
            .kinesis_lag_latency_ms
            .with_guarded_label_values(&metrics_labels);

        let split_id = split.id();
        Ok(Self {
            client,
            stream_name,
            shard_id: split.shard_id,
            shard_iter: None,
            latest_offset: None,
            next_offset,
            end_offset: split.end_offset,
            split_id,
            parser_config,
            source_ctx,
            eof_retry_interval: Duration::from_millis(
                properties.reader_config.eof_retry_interval_ms,
            ),
            error_retry_interval: Duration::from_millis(
                properties.reader_config.error_retry_interval_ms,
            ),
            kinesis_throughput_exceeded_count,
            kinesis_timeout_count,
            kinesis_rebuild_shard_iter_count,
            kinesis_early_terminate_shard_count,
            kinesis_lag_latency_ms,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }
}

impl KinesisSplitReader {
    #[try_stream(ok = Vec < SourceMessage >, error = crate::error::ConnectorError)]
    async fn into_data_stream(mut self) {
        self.new_shard_iter().await?;
        let mut provisioned_throughput_exceeded_start_time: Option<Instant> = None;

        loop {
            if self.shard_iter.is_none() {
                tracing::warn!(
                    "shard iterator is none unexpectedly, may reach the end of shard {}, latest seq {}, retrying in one second",
                    self.shard_id,
                    self.latest_offset.as_ref().unwrap_or(&"None".to_owned())
                );
                tokio::time::sleep(self.eof_retry_interval).await;
                self.new_shard_iter().await?;
            }
            match self.get_records().await {
                Ok(resp) => {
                    self.kinesis_lag_latency_ms
                        .observe(resp.millis_behind_latest().unwrap_or(0) as f64);

                    self.shard_iter = resp.next_shard_iterator().map(String::from);
                    let chunk = (resp.records().iter())
                        .map(|r| from_kinesis_record(r, self.split_id.clone()))
                        .collect::<Vec<SourceMessage>>();
                    if let Some(shard) = &resp.child_shards
                        && !shard.is_empty()
                        && self.shard_iter.is_none()
                    {
                        // according to the doc https://docs.rs/aws-sdk-kinesis/latest/aws_sdk_kinesis/operation/get_records/struct.GetRecordsOutput.html
                        //
                        // > The list of the current shard's child shards, returned in the GetRecords API's response only when the end of the current shard is reached.

                        // The response will be like:
                        // {
                        //     "Records": [], // empty
                        //     "MillisBehindLatest": 2665000, // non-zero
                        //     "ChildShards": [...] // non-empty
                        //     // no NextShardIterator
                        // }

                        // Other executors are going to read the child shards.

                        if !chunk.is_empty() {
                            // This should not happen. But be extra safe here.
                            yield chunk;
                        }

                        tracing::info!(
                            "shard {:?} reaches the end and is inactive, stop reading",
                            self.shard_id
                        );
                        self.kinesis_early_terminate_shard_count.inc();

                        break;
                    }
                    if chunk.is_empty() {
                        tokio::time::sleep(self.error_retry_interval).await;
                        continue;
                    }
                    self.latest_offset = Some(chunk.last().unwrap().offset.clone());
                    tracing::debug!(
                        "shard {:?} latest offset: {:?}",
                        self.shard_id,
                        self.latest_offset
                    );

                    // reset the provisioned throughput exceeded time
                    provisioned_throughput_exceeded_start_time = None;
                    yield chunk;
                }
                Err(SdkError::ServiceError(e)) if e.err().is_resource_not_found_exception() => {
                    tracing::warn!("shard {:?} is closed, stop reading", self.shard_id);
                    break;
                }
                Err(SdkError::ServiceError(e)) if e.err().is_expired_iterator_exception() => {
                    tracing::warn!(
                        "stream {:?} shard {:?} iterator expired, renew it",
                        self.stream_name,
                        self.shard_id
                    );
                    self.new_shard_iter().await?;
                    tokio::time::sleep(self.error_retry_interval).await;
                    continue;
                }
                Err(SdkError::ServiceError(e))
                    if e.err().is_provisioned_throughput_exceeded_exception() =>
                {
                    self.kinesis_throughput_exceeded_count.inc();

                    if let Some(start_time) = provisioned_throughput_exceeded_start_time
                        && start_time.elapsed() > Duration::from_secs(5)
                    {
                        tracing::warn!(
                            "stream {:?} shard {:?} has been throttled for {} seconds, retry",
                            self.stream_name,
                            self.shard_id,
                            start_time.elapsed().as_secs()
                        );
                    } else if provisioned_throughput_exceeded_start_time.is_none() {
                        provisioned_throughput_exceeded_start_time = Some(Instant::now());
                    }

                    self.new_shard_iter().await?;
                    tokio::time::sleep(self.error_retry_interval).await;
                    continue;
                }
                Err(SdkError::DispatchFailure(e)) => {
                    tracing::warn!(
                        "stream {:?} shard {:?} dispatch failure: {:?}",
                        self.stream_name,
                        self.shard_id,
                        e
                    );
                    self.new_shard_iter().await?;
                    continue;
                }
                Err(e) if e.code() == Some("InternalFailure") => {
                    tracing::warn!(
                        "stream {:?} shard {:?} met internal failure, retrying",
                        self.stream_name,
                        self.shard_id
                    );
                    self.new_shard_iter().await?;
                    tokio::time::sleep(self.error_retry_interval).await;
                    continue;
                }
                Err(SdkError::TimeoutError(_)) => {
                    self.kinesis_timeout_count.inc();

                    // according to sdk doc:
                    // The request failed due to a timeout. The request MAY have been sent and received.
                    tracing::warn!(
                        "shard {:?} request timeout, rolling back to previous offset",
                        self.shard_id
                    );
                    self.new_shard_iter().await?;
                    tokio::time::sleep(self.error_retry_interval).await;
                    continue;
                }
                Err(e) => {
                    let error = anyhow!(e).context(format!(
                        "Kinesis got an unhandled error on stream {:?}, shard {:?}",
                        self.stream_name, self.shard_id
                    ));
                    tracing::warn!(error = %error.as_report()); // change to warn as user has no action to take
                    return Err(error.into());
                }
            }
        }
    }
}
impl KinesisSplitReader {
    async fn new_shard_iter(&mut self) -> Result<()> {
        let (starting_seq_num, start_timestamp, iter_type) = if self.latest_offset.is_some() {
            (
                self.latest_offset.clone(),
                None,
                ShardIteratorType::AfterSequenceNumber,
            )
        } else {
            match &self.next_offset {
                KinesisOffset::Earliest => (None, None, ShardIteratorType::TrimHorizon),
                KinesisOffset::AfterSequenceNumber(seq) => (
                    Some(seq.clone()),
                    None,
                    ShardIteratorType::AfterSequenceNumber,
                ),
                KinesisOffset::Latest => (None, None, ShardIteratorType::Latest),
                KinesisOffset::Timestamp(ts) => (
                    None,
                    Some(DateTime::from_millis(*ts)),
                    ShardIteratorType::AtTimestamp,
                ),
                _ => unreachable!(),
            }
        };

        // `starting_seq_num` and `starting_timestamp` will not be both set
        async fn get_shard_iter_inner(
            client: &KinesisClient,
            stream_name: &str,
            shard_id: &str,
            starting_seq_num: Option<String>,
            starting_timestamp: Option<DateTime>,
            iter_type: ShardIteratorType,
        ) -> Result<String> {
            let resp = client
                .get_shard_iterator()
                .stream_name(stream_name)
                .shard_id(shard_id)
                .shard_iterator_type(iter_type)
                .set_starting_sequence_number(starting_seq_num)
                .set_timestamp(starting_timestamp)
                .send()
                .await
                .context("failed to get kinesis shard iterator")?;

            if let Some(iter) = resp.shard_iterator() {
                Ok(iter.to_owned())
            } else {
                bail!("shard iterator is none")
            }
        }

        self.shard_iter = Some(
            tokio_retry::Retry::spawn(
                exponential_backoff(Duration::from_millis(100), 2, Duration::MAX).take(3),
                || {
                    get_shard_iter_inner(
                        &self.client,
                        &self.stream_name,
                        &self.shard_id,
                        starting_seq_num.clone(),
                        start_timestamp,
                        iter_type.clone(),
                    )
                },
            )
            .await?,
        );

        self.kinesis_rebuild_shard_iter_count.inc();

        tracing::info!(
            "resetting kinesis to: stream {:?} shard {:?} starting from {:?}",
            self.stream_name,
            self.shard_id,
            starting_seq_num
        );
        Ok(())
    }

    async fn get_records(
        &mut self,
    ) -> core::result::Result<GetRecordsOutput, SdkError<GetRecordsError>> {
        self.client
            .get_records()
            .limit(self.source_ctx.source_ctrl_opts.chunk_size as i32)
            .set_shard_iterator(self.shard_iter.take())
            .send()
            .await
    }
}

#[cfg(test)]
mod tests {
    use futures::{StreamExt, pin_mut};

    use super::*;
    use crate::connector_common::KinesisCommon;
    use crate::source::SourceContext;
    use crate::source::kinesis::KinesisReaderConfig;

    #[tokio::test]
    #[ignore]
    async fn test_single_thread_kinesis_reader() -> Result<()> {
        let properties = KinesisProperties {
            common: KinesisCommon {
                assume_role_arn: None,
                credentials_access_key: None,
                credentials_secret_access_key: None,
                stream_name: "kinesis_debug".to_owned(),
                stream_region: "cn-northwest-1".to_owned(),
                endpoint: None,
                session_token: None,
                assume_role_external_id: None,
                sdk_connect_timeout_ms: 1000,
                sdk_read_timeout_ms: 1000,
                sdk_operation_timeout_ms: 1000,
                sdk_operation_attempt_timeout_ms: 1000,
                sdk_max_retry_limit: 3,
                sdk_init_backoff_ms: 100,
                sdk_max_backoff_ms: 1000,
            },
            reader_config: KinesisReaderConfig::default(),
            scan_startup_mode: None,
            start_timestamp_millis: None,

            unknown_fields: Default::default(),
        };

        let trim_horizen_reader = KinesisSplitReader::new(
            properties.clone(),
            vec![KinesisSplit {
                shard_id: "shardId-000000000001".to_owned().into(),
                next_offset: KinesisOffset::Earliest,
                end_offset: KinesisOffset::None,
            }],
            Default::default(),
            SourceContext::dummy().into(),
            None,
        )
        .await?
        .into_data_stream();
        pin_mut!(trim_horizen_reader);
        println!("{:?}", trim_horizen_reader.next().await.unwrap()?);

        let offset_reader = KinesisSplitReader::new(
            properties.clone(),
            vec![KinesisSplit {
                shard_id: "shardId-000000000001".to_owned().into(),
                next_offset: KinesisOffset::AfterSequenceNumber(
                    "49629139817504901062972448413535783695568426186596941842".to_owned(),
                ),
                end_offset: KinesisOffset::None,
            }],
            Default::default(),
            SourceContext::dummy().into(),
            None,
        )
        .await?
        .into_data_stream();
        pin_mut!(offset_reader);
        println!("{:?}", offset_reader.next().await.unwrap()?);

        Ok(())
    }
}
