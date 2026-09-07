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

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use anyhow::anyhow;
use futures::{Future, FutureExt, TryFuture};
use rdkafka::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::message::ToBytes;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::log::LogSuppressor;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use strum_macros::{Display, EnumString};
use thiserror_ext::AsReport;
use with_options::WithOptions;

use super::catalog::{SinkFormat, SinkFormatDesc};
use super::{Sink, SinkError, SinkParam};
use crate::connector_common::{
    AwsAuthProps, KafkaCommon, KafkaConnectionProps, KafkaPrivateLinkCommon,
    RdKafkaPropertiesCommon, read_kafka_log_level,
};
use crate::enforce_secret::EnforceSecret;
use crate::sink::formatter::SinkFormatterImpl;
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt, FormattedSink,
};
use crate::sink::{Result, SinkWriterParam};
use crate::source::kafka::{
    KafkaContextCommon, KafkaProperties, KafkaSplitEnumerator, RwProducerContext,
};
use crate::source::{SourceEnumeratorContext, SplitEnumerator};
use crate::{
    deserialize_duration_from_string, deserialize_u32_from_string, dispatch_sink_formatter_impl,
};

pub const KAFKA_SINK: &str = "kafka";

const fn _default_max_retries() -> u32 {
    3
}

const fn _default_retry_backoff() -> Duration {
    Duration::from_millis(100)
}

const fn _default_max_in_flight_requests_per_connection() -> usize {
    5
}

#[derive(Debug, Clone, PartialEq, Display, Deserialize, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum CompressionCodec {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

/// See <https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md>
/// for the detailed meaning of these librdkafka producer properties
#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions)]
pub struct RdKafkaPropertiesProducer {
    /// Allow automatic topic creation on the broker when subscribing to or assigning non-existent topics.
    #[serde(rename = "properties.allow.auto.create.topics")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub allow_auto_create_topics: Option<bool>,

    /// Maximum number of messages allowed on the producer queue. This queue is shared by all
    /// topics and partitions. A value of 0 disables this limit.
    #[serde(rename = "properties.queue.buffering.max.messages")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub queue_buffering_max_messages: Option<usize>,

    /// Maximum total message size sum allowed on the producer queue. This queue is shared by all
    /// topics and partitions. This property has higher priority than queue.buffering.max.messages.
    #[serde(rename = "properties.queue.buffering.max.kbytes")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    queue_buffering_max_kbytes: Option<usize>,

    /// Delay in milliseconds to wait for messages in the producer queue to accumulate before
    /// constructing message batches (`MessageSets`) to transmit to brokers. A higher value allows
    /// larger and more effective (less overhead, improved compression) batches of messages to
    /// accumulate at the expense of increased message delivery latency.
    #[serde(rename = "properties.queue.buffering.max.ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    queue_buffering_max_ms: Option<f64>,

    /// When set to true, the producer will ensure that messages are successfully produced exactly
    /// once and in the original produce order. The following configuration properties are adjusted
    /// automatically (if not modified by the user) when idempotence is enabled:
    /// max.in.flight.requests.per.connection=5 (must be less than or equal to 5),
    /// `retries=INT32_MAX` (must be greater than 0), acks=all, queuing.strategy=fifo. Producer
    /// will fail if user-supplied configuration is incompatible.
    #[serde(rename = "properties.enable.idempotence")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    enable_idempotence: Option<bool>,

    /// How many times to retry sending a failing Message.
    #[serde(rename = "properties.message.send.max.retries")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    message_send_max_retries: Option<usize>,

    /// Maximum number of messages batched in one `MessageSet`
    #[serde(rename = "properties.batch.num.messages")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    batch_num_messages: Option<usize>,

    /// Maximum size (in bytes) of all messages batched in one `MessageSet`, including protocol
    /// framing overhead. This limit is applied after the first message has been added to the
    /// batch, regardless of the first message's size, this is to ensure that messages that exceed
    /// batch.size are produced.
    #[serde(rename = "properties.batch.size")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    batch_size: Option<usize>,

    /// Compression codec to use for compressing message sets.
    #[serde(rename = "properties.compression.codec")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    compression_codec: Option<CompressionCodec>,

    /// Produce message timeout.
    /// This value is used to limits the time a produced message waits for
    /// successful delivery (including retries).
    #[serde(rename = "properties.message.timeout.ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    message_timeout_ms: Option<usize>,

    /// The maximum number of unacknowledged requests the client will send on a single connection before blocking.
    #[serde(
        rename = "properties.max.in.flight.requests.per.connection",
        default = "_default_max_in_flight_requests_per_connection"
    )]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    max_in_flight_requests_per_connection: usize,

    #[serde(rename = "properties.request.required.acks")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    request_required_acks: Option<i32>,
}

impl RdKafkaPropertiesProducer {
    pub(crate) fn set_client(&self, c: &mut rdkafka::ClientConfig) {
        if let Some(v) = self.allow_auto_create_topics {
            c.set("allow.auto.create.topics", v.to_string());
        }
        if let Some(v) = self.queue_buffering_max_messages {
            c.set("queue.buffering.max.messages", v.to_string());
        }
        if let Some(v) = self.queue_buffering_max_kbytes {
            c.set("queue.buffering.max.kbytes", v.to_string());
        }
        if let Some(v) = self.queue_buffering_max_ms {
            c.set("queue.buffering.max.ms", v.to_string());
        }
        if let Some(v) = self.enable_idempotence {
            c.set("enable.idempotence", v.to_string());
        }
        if let Some(v) = self.message_send_max_retries {
            c.set("message.send.max.retries", v.to_string());
        }
        if let Some(v) = self.batch_num_messages {
            c.set("batch.num.messages", v.to_string());
        }
        if let Some(v) = self.batch_size {
            c.set("batch.size", v.to_string());
        }
        if let Some(v) = &self.compression_codec {
            c.set("compression.codec", v.to_string());
        }
        if let Some(v) = self.request_required_acks {
            c.set("request.required.acks", v.to_string());
        }
        if let Some(v) = self.message_timeout_ms {
            c.set("message.timeout.ms", v.to_string());
        }
        c.set(
            "max.in.flight.requests.per.connection",
            self.max_in_flight_requests_per_connection.to_string(),
        );
    }
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions)]
pub struct KafkaConfig {
    #[serde(flatten)]
    pub common: KafkaCommon,

    #[serde(flatten)]
    pub connection: KafkaConnectionProps,

    #[serde(
        rename = "properties.retry.max",
        default = "_default_max_retries",
        deserialize_with = "deserialize_u32_from_string"
    )]
    pub max_retry_num: u32,

    #[serde(
        rename = "properties.retry.interval",
        default = "_default_retry_backoff",
        deserialize_with = "deserialize_duration_from_string"
    )]
    pub retry_interval: Duration,

    /// We have parsed the primary key for an upsert kafka sink into a `usize` vector representing
    /// the indices of the pk columns in the frontend, so we simply store the primary key here
    /// as a string.
    pub primary_key: Option<String>,

    #[serde(flatten)]
    pub rdkafka_properties_common: RdKafkaPropertiesCommon,

    #[serde(flatten)]
    pub rdkafka_properties_producer: RdKafkaPropertiesProducer,

    #[serde(flatten)]
    pub privatelink_common: KafkaPrivateLinkCommon,

    #[serde(flatten)]
    pub aws_auth_props: AwsAuthProps,

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(KafkaConfig);

impl EnforceSecret for KafkaConfig {
    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        KafkaConnectionProps::enforce_one(prop)?;
        AwsAuthProps::enforce_one(prop)?;
        Ok(())
    }
}

impl KafkaConfig {
    pub fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<KafkaConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;

        Ok(config)
    }

    pub(crate) fn set_client(&self, c: &mut rdkafka::ClientConfig) {
        self.rdkafka_properties_common.set_client(c);
        self.rdkafka_properties_producer.set_client(c);
    }
}

impl From<KafkaConfig> for KafkaProperties {
    fn from(val: KafkaConfig) -> Self {
        KafkaProperties {
            bytes_per_second: None,
            max_num_messages: None,
            scan_startup_mode: None,
            time_offset: None,
            upsert: None,
            common: val.common,
            connection: val.connection,
            rdkafka_properties_common: val.rdkafka_properties_common,
            rdkafka_properties_consumer: Default::default(),
            privatelink_common: val.privatelink_common,
            aws_auth_props: val.aws_auth_props,
            group_id_prefix: None,
            unknown_fields: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct KafkaSink {
    pub config: KafkaConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    format_desc: SinkFormatDesc,
    db_name: String,
    sink_from_name: String,
}

impl EnforceSecret for KafkaSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            KafkaConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for KafkaSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = KafkaConfig::from_btreemap(param.properties)?;
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

impl Sink for KafkaSink {
    type LogSinker = AsyncTruncateLogSinkerOf<KafkaSinkWriter>;

    const SINK_NAME: &'static str = KAFKA_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let formatter = SinkFormatterImpl::new(
            &self.format_desc,
            self.schema.clone(),
            self.pk_indices.clone(),
            self.db_name.clone(),
            self.sink_from_name.clone(),
            &self.config.common.topic,
        )
        .await?;
        let max_delivery_buffer_size = (self
            .config
            .rdkafka_properties_producer
            .queue_buffering_max_messages
            .as_ref()
            .cloned()
            .unwrap_or(KAFKA_WRITER_MAX_QUEUE_SIZE) as f32
            * KAFKA_WRITER_MAX_QUEUE_SIZE_RATIO) as usize;

        Ok(KafkaSinkWriter::new(self.config.clone(), formatter)
            .await?
            .into_log_sinker(max_delivery_buffer_size))
    }

    async fn validate(&self) -> Result<()> {
        // For non-append-only Kafka sink, the primary key must be defined.
        if self.format_desc.format != SinkFormat::AppendOnly && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "primary key not defined for {:?} kafka sink (please define in `primary_key` field)",
                self.format_desc.format
            )));
        }
        // Check for formatter constructor error, before it is too late for error reporting.
        SinkFormatterImpl::new(
            &self.format_desc,
            self.schema.clone(),
            self.pk_indices.clone(),
            self.db_name.clone(),
            self.sink_from_name.clone(),
            &self.config.common.topic,
        )
        .await?;

        // Try Kafka connection.
        // There is no such interface for kafka producer to validate a connection
        // use enumerator to validate broker reachability and existence of topic
        let check = KafkaSplitEnumerator::new(
            KafkaProperties::from(self.config.clone()),
            Arc::new(SourceEnumeratorContext::dummy()),
        )
        .await?;
        if let Err(e) = check.check_reachability().await {
            return Err(SinkError::Config(
                anyhow!(
                    "cannot connect to kafka broker ({})",
                    self.config.connection.brokers,
                )
                .context(e),
            ));
        }
        Ok(())
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        KafkaConfig::from_btreemap(config.clone())?;
        Ok(())
    }
}

/// When the `DeliveryFuture` the current `future_delivery_buffer`
/// is buffering is greater than `queue_buffering_max_messages` * `KAFKA_WRITER_MAX_QUEUE_SIZE_RATIO`,
/// then enforcing commit once
const KAFKA_WRITER_MAX_QUEUE_SIZE_RATIO: f32 = 1.2;
/// The default queue size used to enforce a commit in kafka producer if `queue.buffering.max.messages` is not specified.
/// This default value is determined based on the librdkafka default. See the following doc for more details:
/// <https://github.com/confluentinc/librdkafka/blob/1cb80090dfc75f5a36eae3f4f8844b14885c045e/CONFIGURATION.md>
const KAFKA_WRITER_MAX_QUEUE_SIZE: usize = 100000;

struct KafkaPayloadWriter<'a> {
    inner: &'a FutureProducer<RwProducerContext>,
    add_future: DeliveryFutureManagerAddFuture<'a, KafkaSinkDeliveryFuture>,
    config: &'a KafkaConfig,
}

mod opaque_type {
    use super::*;
    pub type KafkaSinkDeliveryFuture = impl TryFuture<Ok = (), Error = SinkError> + Unpin + 'static;

    #[define_opaque(KafkaSinkDeliveryFuture)]
    pub(super) fn map_delivery_future(future: DeliveryFuture) -> KafkaSinkDeliveryFuture {
        future.map(KafkaPayloadWriter::<'static>::map_future_result)
    }
}
pub use opaque_type::KafkaSinkDeliveryFuture;
use opaque_type::map_delivery_future;

pub struct KafkaSinkWriter {
    formatter: SinkFormatterImpl,
    inner: FutureProducer<RwProducerContext>,
    config: KafkaConfig,
}

impl KafkaSinkWriter {
    async fn new(config: KafkaConfig, formatter: SinkFormatterImpl) -> Result<Self> {
        let inner: FutureProducer<RwProducerContext> = {
            let mut c = ClientConfig::new();

            // KafkaConfig configuration
            config.connection.set_security_properties(&mut c);
            config.set_client(&mut c);

            // ClientConfig configuration
            c.set("bootstrap.servers", &config.connection.brokers);

            // Create the producer context, will be used to create the producer
            let broker_rewrite_map = config.privatelink_common.broker_rewrite_map.clone();
            let ctx_common = KafkaContextCommon::new(
                broker_rewrite_map,
                None,
                None,
                config.aws_auth_props.clone(),
                config.connection.is_aws_msk_iam(),
            )
            .await?;
            let producer_ctx = RwProducerContext::new(ctx_common);
            // Generate the producer

            if let Some(log_level) = read_kafka_log_level() {
                c.set_log_level(log_level);
            }
            c.create_with_context(producer_ctx).await?
        };

        Ok(KafkaSinkWriter {
            formatter,
            inner,
            config: config.clone(),
        })
    }
}

impl AsyncTruncateSinkWriter for KafkaSinkWriter {
    type DeliveryFuture = KafkaSinkDeliveryFuture;

    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        let mut payload_writer = KafkaPayloadWriter {
            inner: &mut self.inner,
            add_future,
            config: &self.config,
        };
        dispatch_sink_formatter_impl!(&self.formatter, formatter, {
            payload_writer.write_chunk(chunk, formatter).await
        })
    }
}

impl KafkaPayloadWriter<'_> {
    /// The actual `send_result` function, will be called when the `KafkaSinkWriter` needs to sink
    /// messages
    async fn send_result<'a, K, P>(&'a mut self, mut record: FutureRecord<'a, K, P>) -> Result<()>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let mut success_flag = false;

        let mut ret = Ok(());

        for i in 0..self.config.max_retry_num {
            match self.inner.send_result(record) {
                Ok(delivery_future) => {
                    // The buffer holds delivery futures that we have not polled yet, including
                    // those of records already acked by the broker, so reaching the cap is
                    // ordinary backpressure rather than an anomaly.
                    let awaited = self
                        .add_future
                        .add_future_may_await(map_delivery_future(delivery_future))
                        .await?;
                    if awaited {
                        static LOG_SUPPRESSOR: LazyLock<LogSuppressor> =
                            LazyLock::new(LogSuppressor::default);
                        if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                            tracing::warn!(
                                suppressed_count,
                                future_count = self.add_future.future_count(),
                                max_future_count = self.add_future.max_future_count(),
                                "delivery future buffer is full, awaited an outstanding delivery before enqueuing"
                            );
                        }
                    }
                    success_flag = true;
                    break;
                }
                Err((e, rec)) => match e {
                    KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull) => {
                        record = rec;
                        static LOG_SUPPRESSOR: LazyLock<LogSuppressor> =
                            LazyLock::new(LogSuppressor::default);
                        if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                            tracing::warn!(
                                suppressed_count,
                                future_count = self.add_future.future_count(),
                                retry = i,
                                "producer queue is full, awaiting an outstanding delivery before retry"
                            );
                        }
                        self.add_future.await_one_delivery().await?;
                        continue;
                    }
                    _ => {
                        tracing::warn!(
                            error = %e.as_report(),
                            "producing message (key {:?}) to topic {} failed",
                            rec.key.map(|k| k.to_bytes()),
                            rec.topic,
                        );
                        return Err(e.into());
                    }
                },
            }
        }

        if !success_flag {
            // In this case, after trying `max_retry_num`
            // The enqueue buffer is still full
            ret = Err(KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull).into());
        }

        ret
    }

    async fn write_inner(
        &mut self,
        event_key_object: Option<Vec<u8>>,
        event_object: Option<Vec<u8>>,
    ) -> Result<()> {
        let topic = self.config.common.topic.clone();
        let mut record = FutureRecord::<[u8], [u8]>::to(topic.as_str());
        if let Some(key_str) = &event_key_object {
            record = record.key(key_str);
        }
        if let Some(payload) = &event_object {
            record = record.payload(payload);
        }
        // Send the data but not wait it to finish sinking
        // Will join all `DeliveryFuture` during commit
        self.send_result(record).await?;
        Ok(())
    }

    fn map_future_result(delivery_future_result: <DeliveryFuture as Future>::Output) -> Result<()> {
        match delivery_future_result {
            // Successfully sent the record
            // Will return the partition and offset of the message (i32, i64)
            // Note that `Vec<()>` won't cause memory allocation
            Ok(Ok(_)) => Ok(()),
            // If the message failed to be delivered. (i.e., flush)
            // The error & the copy of the original message will be returned
            // i.e., (KafkaError, OwnedMessage)
            // We will just stop the loop, and return the error
            // The sink executor will back to the latest checkpoint
            Ok(Err((k_err, _msg))) => Err(k_err.into()),
            // This represents the producer is dropped
            // before the delivery status is received
            // Return `KafkaError::Canceled`
            Err(_) => Err(KafkaError::Canceled.into()),
        }
    }
}

impl FormattedSink for KafkaPayloadWriter<'_> {
    type K = Vec<u8>;
    type V = Vec<u8>;

    async fn write_one(&mut self, k: Option<Self::K>, v: Option<Self::V>) -> Result<()> {
        self.write_inner(k, v).await
    }
}

#[cfg(test)]
mod test {
    use maplit::btreemap;
    use risingwave_common::catalog::Field;
    use risingwave_common::types::DataType;

    use super::*;
    use crate::sink::encoder::{
        DateHandlingMode, JsonEncoder, JsonbHandlingMode, TimeHandlingMode, TimestampHandlingMode,
        TimestamptzHandlingMode,
    };
    use crate::sink::formatter::AppendOnlyFormatter;

    #[test]
    fn parse_rdkafka_props() {
        let props: BTreeMap<String, String> = btreemap! {
            // basic
            // "connector".to_string() => "kafka".to_string(),
            "properties.bootstrap.server".to_owned() => "localhost:9092".to_owned(),
            "topic".to_owned() => "test".to_owned(),
            // "type".to_string() => "append-only".to_string(),
            // RdKafkaPropertiesCommon
            "properties.message.max.bytes".to_owned() => "12345".to_owned(),
            "properties.receive.message.max.bytes".to_owned() => "54321".to_owned(),
            "properties.reconnect.backoff.ms".to_owned() => "1000".to_owned(),
            "properties.reconnect.backoff.max.ms".to_owned() => "30000".to_owned(),
            "properties.socket.connection.setup.timeout.ms".to_owned() => "60000".to_owned(),
            "properties.retry.backoff.ms".to_owned() => "200".to_owned(),
            "properties.retry.backoff.max.ms".to_owned() => "2000".to_owned(),
            // RdKafkaPropertiesProducer
            "properties.queue.buffering.max.messages".to_owned() => "114514".to_owned(),
            "properties.queue.buffering.max.kbytes".to_owned() => "114514".to_owned(),
            "properties.queue.buffering.max.ms".to_owned() => "114.514".to_owned(),
            "properties.enable.idempotence".to_owned() => "false".to_owned(),
            "properties.message.send.max.retries".to_owned() => "114514".to_owned(),
            "properties.batch.num.messages".to_owned() => "114514".to_owned(),
            "properties.batch.size".to_owned() => "114514".to_owned(),
            "properties.compression.codec".to_owned() => "zstd".to_owned(),
            "properties.message.timeout.ms".to_owned() => "114514".to_owned(),
            "properties.max.in.flight.requests.per.connection".to_owned() => "114514".to_owned(),
            "properties.request.required.acks".to_owned() => "-1".to_owned(),
        };
        let c = KafkaConfig::from_btreemap(props).unwrap();
        assert_eq!(c.rdkafka_properties_common.message_max_bytes, Some(12345));
        assert_eq!(
            c.rdkafka_properties_common.receive_message_max_bytes,
            Some(54321)
        );
        assert_eq!(c.rdkafka_properties_common.reconnect_backoff_ms, Some(1000));
        assert_eq!(
            c.rdkafka_properties_common.reconnect_backoff_max_ms,
            Some(30000)
        );
        assert_eq!(
            c.rdkafka_properties_common
                .socket_connection_setup_timeout_ms,
            Some(60000)
        );
        assert_eq!(c.rdkafka_properties_common.retry_backoff_ms, Some(200));
        assert_eq!(c.rdkafka_properties_common.retry_backoff_max_ms, Some(2000));
        let mut client_config = rdkafka::ClientConfig::new();
        c.set_client(&mut client_config);
        assert_eq!(client_config.get("reconnect.backoff.ms"), Some("1000"));
        assert_eq!(client_config.get("reconnect.backoff.max.ms"), Some("30000"));
        assert_eq!(
            client_config.get("socket.connection.setup.timeout.ms"),
            Some("60000")
        );
        assert_eq!(client_config.get("retry.backoff.ms"), Some("200"));
        assert_eq!(client_config.get("retry.backoff.max.ms"), Some("2000"));
        assert_eq!(
            c.rdkafka_properties_producer.queue_buffering_max_ms,
            Some(114.514f64)
        );
        assert_eq!(
            c.rdkafka_properties_producer.compression_codec,
            Some(CompressionCodec::Zstd)
        );
        assert_eq!(
            c.rdkafka_properties_producer.message_timeout_ms,
            Some(114514)
        );
        assert_eq!(
            c.rdkafka_properties_producer
                .max_in_flight_requests_per_connection,
            114514
        );
        assert_eq!(
            c.rdkafka_properties_producer.request_required_acks,
            Some(-1)
        );

        let props: BTreeMap<String, String> = btreemap! {
            // basic
            "connector".to_owned() => "kafka".to_owned(),
            "properties.bootstrap.server".to_owned() => "localhost:9092".to_owned(),
            "topic".to_owned() => "test".to_owned(),
            "type".to_owned() => "append-only".to_owned(),

            "properties.enable.idempotence".to_owned() => "True".to_owned(), // can only be 'true' or 'false'
        };
        assert!(KafkaConfig::from_btreemap(props).is_err());

        let props: BTreeMap<String, String> = btreemap! {
            // basic
            "connector".to_owned() => "kafka".to_owned(),
            "properties.bootstrap.server".to_owned() => "localhost:9092".to_owned(),
            "topic".to_owned() => "test".to_owned(),
            "type".to_owned() => "append-only".to_owned(),
            "properties.queue.buffering.max.kbytes".to_owned() => "-114514".to_owned(), // usize cannot be negative
        };
        assert!(KafkaConfig::from_btreemap(props).is_err());

        let props: BTreeMap<String, String> = btreemap! {
            // basic
            "connector".to_owned() => "kafka".to_owned(),
            "properties.bootstrap.server".to_owned() => "localhost:9092".to_owned(),
            "topic".to_owned() => "test".to_owned(),
            "type".to_owned() => "append-only".to_owned(),
            "properties.compression.codec".to_owned() => "notvalid".to_owned(), // has to be a valid CompressionCodec
        };
        assert!(KafkaConfig::from_btreemap(props).is_err());
    }

    #[test]
    fn parse_kafka_config() {
        let properties: BTreeMap<String, String> = btreemap! {
            // "connector".to_string() => "kafka".to_string(),
            "properties.bootstrap.server".to_owned() => "localhost:9092".to_owned(),
            "topic".to_owned() => "test".to_owned(),
            // "type".to_string() => "append-only".to_string(),
            // "force_append_only".to_string() => "true".to_string(),
            "properties.security.protocol".to_owned() => "SASL".to_owned(),
            "properties.sasl.mechanism".to_owned() => "SASL".to_owned(),
            "properties.sasl.username".to_owned() => "test".to_owned(),
            "properties.sasl.password".to_owned() => "test".to_owned(),
            "properties.retry.max".to_owned() => "20".to_owned(),
            "properties.retry.interval".to_owned() => "500ms".to_owned(),
            // PrivateLink
            "broker.rewrite.endpoints".to_owned() => "{\"broker1\": \"10.0.0.1:8001\"}".to_owned(),
        };
        let config = KafkaConfig::from_btreemap(properties).unwrap();
        assert_eq!(config.connection.brokers, "localhost:9092");
        assert_eq!(config.common.topic, "test");
        assert_eq!(config.max_retry_num, 20);
        assert_eq!(config.retry_interval, Duration::from_millis(500));

        // PrivateLink fields
        let btreemap: BTreeMap<String, String> = btreemap! {
            "broker1".to_owned() => "10.0.0.1:8001".to_owned()
        };
        assert_eq!(config.privatelink_common.broker_rewrite_map, Some(btreemap));

        // Optional fields eliminated.
        let properties: BTreeMap<String, String> = btreemap! {
            // "connector".to_string() => "kafka".to_string(),
            "properties.bootstrap.server".to_owned() => "localhost:9092".to_owned(),
            "topic".to_owned() => "test".to_owned(),
            // "type".to_string() => "upsert".to_string(),
        };
        let config = KafkaConfig::from_btreemap(properties).unwrap();
        assert_eq!(config.max_retry_num, 3);
        assert_eq!(config.retry_interval, Duration::from_millis(100));

        // Invalid u32 input.
        let properties: BTreeMap<String, String> = btreemap! {
            "connector".to_owned() => "kafka".to_owned(),
            "properties.bootstrap.server".to_owned() => "localhost:9092".to_owned(),
            "topic".to_owned() => "test".to_owned(),
            "type".to_owned() => "upsert".to_owned(),
            "properties.retry.max".to_owned() => "-20".to_owned(),  // error!
        };
        assert!(KafkaConfig::from_btreemap(properties).is_err());

        // Invalid duration input.
        let properties: BTreeMap<String, String> = btreemap! {
            "connector".to_owned() => "kafka".to_owned(),
            "properties.bootstrap.server".to_owned() => "localhost:9092".to_owned(),
            "topic".to_owned() => "test".to_owned(),
            "type".to_owned() => "upsert".to_owned(),
            "properties.retry.interval".to_owned() => "500miiinutes".to_owned(),  // invalid duration
        };
        assert!(KafkaConfig::from_btreemap(properties).is_err());
    }

    /// Note: Please enable the kafka by running `./risedev configure` before commenting #[ignore]
    /// to run the test, also remember to modify `risedev.yml`
    #[ignore]
    #[tokio::test]
    async fn test_kafka_producer() -> Result<()> {
        // Create a dummy kafka properties
        let properties = btreemap! {
            "connector".to_owned() => "kafka".to_owned(),
            "properties.bootstrap.server".to_owned() => "localhost:29092".to_owned(),
            "type".to_owned() => "append-only".to_owned(),
            "topic".to_owned() => "test_topic".to_owned(),
            "properties.compression.codec".to_owned() => "zstd".to_owned(),
        };

        // Create a table with two columns (| id : INT32 | v2 : VARCHAR |) here
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "id".into(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "v2".into(),
            },
        ]);

        let kafka_config = KafkaConfig::from_btreemap(properties)?;

        // Create the actual sink writer to Kafka
        let sink = KafkaSinkWriter::new(
            kafka_config.clone(),
            SinkFormatterImpl::AppendOnlyJson(AppendOnlyFormatter::new(
                // We do not specify primary key for this schema
                None,
                JsonEncoder::new(
                    schema,
                    None,
                    DateHandlingMode::FromCe,
                    TimestampHandlingMode::Milli,
                    TimestamptzHandlingMode::UtcString,
                    TimeHandlingMode::Milli,
                    JsonbHandlingMode::String,
                ),
            )),
        )
        .await
        .unwrap();

        use crate::sink::log_store::DeliveryFutureManager;

        let mut future_manager = DeliveryFutureManager::new(usize::MAX);

        for i in 0..10 {
            println!("epoch: {}", i);
            for j in 0..100 {
                let mut writer = KafkaPayloadWriter {
                    inner: &sink.inner,
                    add_future: future_manager.start_write_chunk(i, j),
                    config: &sink.config,
                };
                match writer
                    .send_result(
                        FutureRecord::to(kafka_config.common.topic.as_str())
                            .payload(format!("value-{}", j).as_bytes())
                            .key(format!("dummy_key_for_epoch-{}", i).as_bytes()),
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        println!("{:?}", e);
                        break;
                    }
                };
            }
        }

        Ok(())
    }
}
