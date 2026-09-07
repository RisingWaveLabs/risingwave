// Copyright 2023 RisingWave Labs
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

use std::collections::BTreeMap;

use anyhow::{Context, anyhow};
use aws_sdk_kinesis::Client as KinesisClient;
use aws_sdk_kinesis::operation::put_records::PutRecordsOutput;
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::{PutRecordsRequestEntry, PutRecordsResultEntry};
use futures::{FutureExt, TryFuture};
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use serde::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use super::SinkParam;
use super::catalog::SinkFormatDesc;
use crate::connector_common::KinesisCommon;
use crate::dispatch_sink_formatter_str_key_impl;
use crate::enforce_secret::EnforceSecret;
use crate::sink::formatter::SinkFormatterImpl;
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt, FormattedSink,
};
use crate::sink::{Result, Sink, SinkError, SinkWriterParam};
pub const KINESIS_SINK: &str = "kinesis";

#[derive(Clone, Debug)]
pub struct KinesisSink {
    pub config: KinesisSinkConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    format_desc: SinkFormatDesc,
    db_name: String,
    sink_from_name: String,
}

impl EnforceSecret for KinesisSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            KinesisSinkConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for KinesisSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = KinesisSinkConfig::from_btreemap(param.properties)?;
        Ok(Self {
            config,
            schema,
            pk_indices,
            format_desc: param
                .format_desc
                .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?,
            db_name: param.db_name,
            sink_from_name: param.sink_from_name,
        })
    }
}

const KINESIS_SINK_MAX_PENDING_CHUNK_NUM: usize = 64;

impl Sink for KinesisSink {
    type LogSinker = AsyncTruncateLogSinkerOf<KinesisSinkWriter>;

    const SINK_NAME: &'static str = KINESIS_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn validate(&self) -> Result<()> {
        // Kinesis requires partition key. There is no builtin support for round-robin as in kafka/pulsar.
        // https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html#Streams-PutRecord-request-PartitionKey
        if self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "kinesis sink requires partition key (please define in `primary_key` field)",
            )));
        }
        // Check for formatter constructor error, before it is too late for error reporting.
        SinkFormatterImpl::new(
            &self.format_desc,
            self.schema.clone(),
            self.pk_indices.clone(),
            self.db_name.clone(),
            self.sink_from_name.clone(),
            &self.config.common.stream_name,
        )
        .await?;

        // check reachability
        let client = self.config.common.build_client().await?;
        client
            .list_shards()
            .stream_name(&self.config.common.stream_name)
            .send()
            .await
            .context("failed to list shards")
            .map_err(SinkError::Kinesis)?;
        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(KinesisSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            &self.format_desc,
            self.db_name.clone(),
            self.sink_from_name.clone(),
        )
        .await?
        .into_log_sinker(KINESIS_SINK_MAX_PENDING_CHUNK_NUM))
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct KinesisSinkConfig {
    #[serde(flatten)]
    pub common: KinesisCommon,

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(KinesisSinkConfig);

impl EnforceSecret for KinesisSinkConfig {
    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        KinesisCommon::enforce_one(prop)?;
        Ok(())
    }
}

impl KinesisSinkConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<KinesisSinkConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }
}

pub struct KinesisSinkWriter {
    pub config: KinesisSinkConfig,
    formatter: SinkFormatterImpl,
    client: KinesisClient,
}

struct KinesisSinkPayloadWriter {
    client: KinesisClient,
    entries: Vec<(PutRecordsRequestEntry, usize)>,
    stream_name: String,
}

impl KinesisSinkWriter {
    pub async fn new(
        config: KinesisSinkConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        format_desc: &SinkFormatDesc,
        db_name: String,
        sink_from_name: String,
    ) -> Result<Self> {
        let formatter = SinkFormatterImpl::new(
            format_desc,
            schema,
            pk_indices,
            db_name,
            sink_from_name,
            &config.common.stream_name,
        )
        .await?;
        let client = config
            .common
            .build_client()
            .await
            .map_err(|err| SinkError::Kinesis(anyhow!(err)))?;
        Ok(Self {
            config: config.clone(),
            formatter,
            client,
        })
    }

    fn new_payload_writer(&self) -> KinesisSinkPayloadWriter {
        KinesisSinkPayloadWriter {
            client: self.client.clone(),
            entries: vec![],
            stream_name: self.config.common.stream_name.clone(),
        }
    }
}

mod opaque_type {
    use std::cmp::min;
    use std::time::Duration;

    use risingwave_common::util::retry::exponential_backoff;
    use thiserror_ext::AsReport;
    use tokio::time::sleep;
    use tokio_retry::strategy::jitter;
    use tracing::warn;

    use super::*;
    pub type KinesisSinkPayloadWriterDeliveryFuture =
        impl TryFuture<Ok = (), Error = SinkError> + Unpin + Send + 'static;

    impl KinesisSinkPayloadWriter {
        #[define_opaque(KinesisSinkPayloadWriterDeliveryFuture)]
        pub(super) fn finish(self) -> KinesisSinkPayloadWriterDeliveryFuture {
            // For reference to the behavior of `put_records`
            // https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis/client/put_records.html

            async move {
                // From the doc of `put_records`:
                // Each PutRecords request can support up to 500 records. Each record in the request can be as large as 1 MiB,
                // up to a limit of 5 MiB for the entire request, including partition keys. Each shard can support writes up to
                // 1,000 records per second, up to a maximum data write total of 1 MiB per second.

                const MAX_RECORD_COUNT: usize = 500;
                const MAX_SINGLE_RECORD_PAYLOAD_SIZE: usize = 1 << 20;
                const MAX_TOTAL_RECORD_PAYLOAD_SIZE: usize = 5 * (1 << 20);
                // Allow at most 3 times of retry when not making any progress to avoid endless retry
                const MAX_NO_PROGRESS_RETRY_COUNT: usize = 3;

                let mut remaining_no_progress_retry_count = MAX_NO_PROGRESS_RETRY_COUNT;
                let total_count = self.entries.len();
                let mut start_idx = 0;

                let mut throttle_delay = None;

                while start_idx < total_count {
                    // 1. Prepare the records to be sent

                    // The maximum possible number of records that can be sent in this iteration.
                    // Can be smaller than this number when the total payload size exceeds `MAX_TOTAL_RECORD_PAYLOAD_SIZE`
                    let max_record_count = min(MAX_RECORD_COUNT, total_count - start_idx);
                    let mut records = Vec::with_capacity(max_record_count);
                    let mut total_payload_size = 0;
                    for i in start_idx..(start_idx + max_record_count) {
                        let (record, size) = &self.entries[i];
                        if *size >= MAX_SINGLE_RECORD_PAYLOAD_SIZE {
                            warn!(
                                size,
                                partition = record.partition_key,
                                "encounter a large single record"
                            );
                        }
                        if total_payload_size + *size < MAX_TOTAL_RECORD_PAYLOAD_SIZE {
                            total_payload_size += *size;
                            records.push(record.clone());
                        } else {
                            break;
                        }
                    }
                    if records.is_empty() {
                        // at least include one record even if its size exceed `MAX_TOTAL_RECORD_PAYLOAD_SIZE`
                        records.push(self.entries[start_idx].0.clone());
                    }

                    // 2. send the records and handle the result
                    let record_count = records.len();
                    match self
                        .client
                        .put_records()
                        .stream_name(&self.stream_name)
                        .set_records(Some(records))
                        .send()
                        .await
                    {
                        Ok(output) => {
                            if record_count != output.records.len() {
                                return Err(SinkError::Kinesis(anyhow!("request record count {} not match the response record count {}", record_count, output.records.len())));
                            }
                            // From the doc of `put_records`:
                            // A single record failure does not stop the processing of subsequent records. As a result,
                            // PutRecords doesn’t guarantee the ordering of records. If you need to read records in the same
                            // order they are written to the stream, use PutRecord instead of PutRecords, and write to the same shard.

                            // Therefore, to ensure at least once and eventual consistency, we figure out the first failed entry, and retry
                            // all the following entries even if the following entries may have been successfully processed.
                            if let Some((first_failed_idx, result_entry)) = Self::first_failed_entry(output) {
                                // first_failed_idx is also the number of successful entries
                                let partially_sent_count = first_failed_idx;
                                if partially_sent_count > 0 {
                                    warn!(
                                        partially_sent_count,
                                        record_count,
                                        "records are partially sent. code: [{}], message: [{}]",
                                        result_entry.error_code.unwrap_or_default(),
                                        result_entry.error_message.unwrap_or_default()
                                    );
                                    start_idx += partially_sent_count;
                                    // reset retry count when having progress
                                    remaining_no_progress_retry_count = MAX_NO_PROGRESS_RETRY_COUNT;
                                } else if let Some(err_code) = &result_entry.error_code && err_code == "ProvisionedThroughputExceededException" {
                                    // From the doc of `put_records`:
                                    // The ErrorCode parameter reflects the type of error and can be one of the following values:
                                    // ProvisionedThroughputExceededException or InternalFailure. ErrorMessage provides more detailed
                                    // information about the ProvisionedThroughputExceededException exception including the account ID,
                                    // stream name, and shard ID of the record that was throttled.
                                    let throttle_delay = throttle_delay.get_or_insert_with(|| exponential_backoff(Duration::from_millis(100), 2, Duration::from_secs(2)).map(jitter)).next().expect("should not be none");
                                    warn!(err_string = ?result_entry.error_message, ?throttle_delay, "throttle");
                                    sleep(throttle_delay).await;
                                } else  {
                                    // no progress due to some internal error
                                    assert_eq!(first_failed_idx, 0);
                                    remaining_no_progress_retry_count -= 1;
                                    if remaining_no_progress_retry_count == 0 {
                                        return Err(SinkError::Kinesis(anyhow!(
                                            "failed to send records. sent {} out of {}, last err: code: [{}], message: [{}]",
                                            start_idx,
                                            total_count,
                                            result_entry.error_code.unwrap_or_default(),
                                            result_entry.error_message.unwrap_or_default()
                                        )));
                                    } else {
                                        warn!(
                                            remaining_no_progress_retry_count,
                                            sent = start_idx,
                                            total_count,
                                            "failed to send records. code: [{}], message: [{}]",
                                            result_entry.error_code.unwrap_or_default(),
                                            result_entry.error_message.unwrap_or_default()
                                        )
                                    }
                                }
                            } else {
                                start_idx += record_count;
                                // reset retry count when having progress
                                remaining_no_progress_retry_count = MAX_NO_PROGRESS_RETRY_COUNT;
                                // reset throttle delay when records can be fully sent.
                                throttle_delay = None;
                            }
                        }
                        Err(e) => {
                            remaining_no_progress_retry_count -= 1;
                            if remaining_no_progress_retry_count == 0 {
                                return Err(SinkError::Kinesis(anyhow!(e).context(format!(
                                    "failed to send records. sent {} out of {}",
                                    start_idx, total_count,
                                ))));
                            } else {
                                warn!(
                                    remaining_no_progress_retry_count,
                                    sent = start_idx,
                                    total_count,
                                    "failed to send records. err: [{:?}]",
                                    e.as_report(),
                                )
                            }
                        }
                    }
                }
                Ok(())
            }
            .boxed()
        }
    }
}
pub use opaque_type::KinesisSinkPayloadWriterDeliveryFuture;

impl KinesisSinkPayloadWriter {
    fn first_failed_entry(output: PutRecordsOutput) -> Option<(usize, PutRecordsResultEntry)> {
        // From the doc of `put_records`:
        // A successfully processed record includes ShardId and SequenceNumber values. The ShardId parameter
        // identifies the shard in the stream where the record is stored. The SequenceNumber parameter is an
        // identifier assigned to the put record, unique to all records in the stream.
        //
        // An unsuccessfully processed record includes ErrorCode and ErrorMessage values. ErrorCode reflects
        // the type of error and can be one of the following values: ProvisionedThroughputExceededException or
        // InternalFailure. ErrorMessage provides more detailed information about the ProvisionedThroughputExceededException
        // exception including the account ID, stream name, and shard ID of the record that was throttled.
        output
            .records
            .into_iter()
            .find_position(|entry| entry.shard_id.is_none())
    }

    fn put_record(&mut self, key: String, payload: Vec<u8>) {
        let size = key.len() + payload.len();
        self.entries.push((
            PutRecordsRequestEntry::builder()
                .partition_key(key)
                .data(Blob::new(payload))
                .build()
                .expect("should not fail because we have set `data` and `partition_key`"),
            size,
        ))
    }
}

impl FormattedSink for KinesisSinkPayloadWriter {
    type K = String;
    type V = Vec<u8>;

    async fn write_one(&mut self, k: Option<Self::K>, v: Option<Self::V>) -> Result<()> {
        self.put_record(
            k.ok_or_else(|| SinkError::Kinesis(anyhow!("no key provided")))?,
            v.unwrap_or_default(),
        );
        Ok(())
    }
}

impl AsyncTruncateSinkWriter for KinesisSinkWriter {
    type DeliveryFuture = KinesisSinkPayloadWriterDeliveryFuture;

    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        mut add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        let mut payload_writer = self.new_payload_writer();
        dispatch_sink_formatter_str_key_impl!(
            &self.formatter,
            formatter,
            payload_writer.write_chunk(chunk, formatter).await
        )?;

        add_future
            .add_future_may_await(payload_writer.finish())
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_kinesis::types::PutRecordsRequestEntry;
    use aws_smithy_types::Blob;

    #[test]
    fn test_kinesis_entry_builder_save_unwrap() {
        PutRecordsRequestEntry::builder()
            .data(Blob::new(b"data"))
            .partition_key("partition-key")
            .build()
            .unwrap();
    }
}
