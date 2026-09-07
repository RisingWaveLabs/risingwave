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
use std::fmt::Debug;
use std::time::Duration;

use anyhow::anyhow;
use futures::{FutureExt, TryFuture, TryFutureExt};
use pulsar::producer::{Message, SendFuture};
use pulsar::routing_policy::RoutingPolicy;
use pulsar::{Producer, ProducerOptions, Pulsar, TokioExecutor};
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use strum_macros::{Display, EnumString};
use with_options::WithOptions;

use super::catalog::{SinkFormat, SinkFormatDesc};
use super::{Sink, SinkError, SinkParam, SinkWriterParam};
use crate::connector_common::{AwsAuthProps, PulsarCommon, PulsarOauthCommon};
use crate::enforce_secret::EnforceSecret;
use crate::sink::Result;
use crate::sink::encoder::SerTo;
use crate::sink::formatter::{SinkFormatter, SinkFormatterImpl};
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt, FormattedSink,
};
use crate::{deserialize_duration_from_string, dispatch_sink_formatter_str_key_impl};

pub const PULSAR_SINK: &str = "pulsar";

/// The delivery buffer queue size
/// When the `SendFuture` the current `send_future_buffer`
/// is buffering is greater than this size, then enforcing commit once
const PULSAR_SEND_FUTURE_BUFFER_MAX_SIZE: usize = 65536;

const fn _default_max_retries() -> u32 {
    3
}

const fn _default_retry_backoff() -> Duration {
    Duration::from_millis(100)
}

const fn _default_batch_size() -> u32 {
    10000
}

const fn _default_batch_byte_size() -> usize {
    1 << 20
}

fn pulsar_to_sink_err(e: pulsar::Error) -> SinkError {
    SinkError::Pulsar(anyhow!(e))
}

async fn build_pulsar_producer(
    pulsar: &Pulsar<TokioExecutor>,
    config: &PulsarConfig,
) -> Result<Producer<TokioExecutor>> {
    // Reduce async state machine size (see `clippy::large_futures`).
    Box::pin(
        pulsar
            .producer()
            .with_options(ProducerOptions {
                batch_size: Some(config.producer_properties.batch_size),
                batch_byte_size: Some(config.producer_properties.batch_byte_size),
                routing_policy: pulsar_producer_routing_policy(
                    config.producer_properties.routing_mode,
                ),
                ..Default::default()
            })
            .with_topic(&config.common.topic)
            .build()
            .map_err(pulsar_to_sink_err),
    )
    .await
}

fn pulsar_producer_routing_policy(
    routing_mode: Option<PulsarRoutingMode>,
) -> Option<RoutingPolicy> {
    routing_mode.map(Into::into)
}

#[derive(Debug, Copy, Clone, Display, Deserialize, EnumString)]
#[strum(serialize_all = "snake_case", ascii_case_insensitive)]
pub enum PulsarRoutingMode {
    #[strum(
        serialize = "round_robin",
        serialize = "roundrobin",
        serialize = "roundrobinpartition",
        serialize = "round_robin_partition",
        serialize = "round-robin",
        serialize = "round-robin-partition",
        serialize = "RoundRobinPartition"
    )]
    RoundRobin,
    #[strum(
        serialize = "single",
        serialize = "singlepartition",
        serialize = "single_partition",
        serialize = "single-partition",
        serialize = "SinglePartition"
    )]
    Single,
}

impl From<PulsarRoutingMode> for RoutingPolicy {
    fn from(mode: PulsarRoutingMode) -> Self {
        match mode {
            PulsarRoutingMode::RoundRobin => RoutingPolicy::RoundRobin,
            PulsarRoutingMode::Single => RoutingPolicy::Single,
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions)]
pub struct PulsarPropertiesProducer {
    #[serde(rename = "properties.batch.size", default = "_default_batch_size")]
    #[serde_as(as = "DisplayFromStr")]
    batch_size: u32,

    #[serde(
        rename = "properties.batch.byte.size",
        default = "_default_batch_byte_size"
    )]
    #[serde_as(as = "DisplayFromStr")]
    batch_byte_size: usize,

    #[serde(
        rename = "properties.routing.mode",
        alias = "properties.routing_mode",
        alias = "routing_mode",
        alias = "pulsar.routing_mode",
        alias = "pulsar.routing.mode",
        alias = "pulsar.properties.routing.mode",
        alias = "pulsar.properties.routing_mode"
    )]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    routing_mode: Option<PulsarRoutingMode>,
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions)]
pub struct PulsarConfig {
    #[serde(rename = "properties.retry.max", default = "_default_max_retries")]
    #[serde_as(as = "DisplayFromStr")]
    pub max_retry_num: u32,

    #[serde(
        rename = "properties.retry.interval",
        default = "_default_retry_backoff",
        deserialize_with = "deserialize_duration_from_string"
    )]
    pub retry_interval: Duration,

    #[serde(flatten)]
    pub common: PulsarCommon,

    #[serde(flatten)]
    pub oauth: Option<PulsarOauthCommon>,

    #[serde(flatten)]
    pub aws_auth_props: AwsAuthProps,

    #[serde(flatten)]
    pub producer_properties: PulsarPropertiesProducer,

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(PulsarConfig);

impl EnforceSecret for PulsarConfig {
    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        PulsarCommon::enforce_one(prop)?;
        AwsAuthProps::enforce_one(prop)?;
        Ok(())
    }
}
impl PulsarConfig {
    pub fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<PulsarConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;

        Ok(config)
    }
}

#[derive(Debug)]
pub struct PulsarSink {
    pub config: PulsarConfig,
    schema: Schema,
    downstream_pk: Vec<usize>,
    format_desc: SinkFormatDesc,
    db_name: String,
    sink_from_name: String,
}

impl EnforceSecret for PulsarSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            PulsarConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for PulsarSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let downstream_pk = param.downstream_pk_or_empty();
        let config = PulsarConfig::from_btreemap(param.properties)?;
        Ok(Self {
            config,
            schema,
            downstream_pk,
            format_desc: param
                .format_desc
                .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?,
            db_name: param.db_name,
            sink_from_name: param.sink_from_name,
        })
    }
}

impl Sink for PulsarSink {
    type LogSinker = AsyncTruncateLogSinkerOf<PulsarSinkWriter>;

    const SINK_NAME: &'static str = PULSAR_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        // Reduce async state machine size (see `clippy::large_futures`).
        let writer = Box::pin(PulsarSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.downstream_pk.clone(),
            &self.format_desc,
            self.db_name.clone(),
            self.sink_from_name.clone(),
        ))
        .await?;
        Ok(writer.into_log_sinker(PULSAR_SEND_FUTURE_BUFFER_MAX_SIZE))
    }

    async fn validate(&self) -> Result<()> {
        // For non-append-only Pulsar sink, the primary key must be defined.
        if self.format_desc.format != SinkFormat::AppendOnly && self.downstream_pk.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "primary key not defined for {:?} pulsar sink (please define in `primary_key` field)",
                self.format_desc.format
            )));
        }
        // Check for formatter constructor error, before it is too late for error reporting.
        SinkFormatterImpl::new(
            &self.format_desc,
            self.schema.clone(),
            self.downstream_pk.clone(),
            self.db_name.clone(),
            self.sink_from_name.clone(),
            &self.config.common.topic,
        )
        .await?;

        // Validate pulsar connection.
        let pulsar = self
            .config
            .common
            // Source-side Pulsar client retry overrides are intentionally not applied to sinks.
            .build_client(&self.config.oauth, &self.config.aws_auth_props, None)
            .await?;
        build_pulsar_producer(&pulsar, &self.config).await?;

        Ok(())
    }
}

pub struct PulsarSinkWriter {
    formatter: SinkFormatterImpl,
    #[expect(dead_code)]
    pulsar: Pulsar<TokioExecutor>,
    producer: Producer<TokioExecutor>,
    config: PulsarConfig,
}

struct PulsarPayloadWriter<'w> {
    producer: &'w mut Producer<TokioExecutor>,
    config: &'w PulsarConfig,
    add_future: DeliveryFutureManagerAddFuture<'w, PulsarDeliveryFuture>,
}

mod opaque_type {
    use super::*;
    pub type PulsarDeliveryFuture = impl TryFuture<Ok = (), Error = SinkError> + Unpin + 'static;

    #[define_opaque(PulsarDeliveryFuture)]
    pub(super) fn may_delivery_future(future: SendFuture) -> PulsarDeliveryFuture {
        future.map(|result| {
            result
                .map(|_| ())
                .map_err(|e: pulsar::Error| SinkError::Pulsar(anyhow!(e)))
        })
    }
}
pub use opaque_type::PulsarDeliveryFuture;
use opaque_type::may_delivery_future;

impl PulsarSinkWriter {
    pub async fn new(
        config: PulsarConfig,
        schema: Schema,
        downstream_pk: Vec<usize>,
        format_desc: &SinkFormatDesc,
        db_name: String,
        sink_from_name: String,
    ) -> Result<Self> {
        let formatter = SinkFormatterImpl::new(
            format_desc,
            schema,
            downstream_pk,
            db_name,
            sink_from_name,
            &config.common.topic,
        )
        .await?;
        let pulsar = config
            .common
            // Source-side Pulsar client retry overrides are intentionally not applied to sinks.
            .build_client(&config.oauth, &config.aws_auth_props, None)
            .await?;
        let producer = build_pulsar_producer(&pulsar, &config).await?;
        Ok(Self {
            formatter,
            pulsar,
            producer,
            config,
        })
    }
}

impl PulsarPayloadWriter<'_> {
    async fn send_message(&mut self, message: Message) -> Result<()> {
        let mut success_flag = false;
        let mut connection_err = None;

        for retry_num in 0..self.config.max_retry_num {
            if retry_num > 0 {
                tracing::warn!("failed to send the message; retry {retry_num}");
            }
            match Box::pin(self.producer.send_non_blocking(message.clone())).await {
                // If the message is sent successfully,
                // a SendFuture holding the message receipt
                // or error after sending is returned
                Ok(send_future) => {
                    self.add_future
                        .add_future_may_await(may_delivery_future(send_future))
                        .await?;
                    success_flag = true;
                    break;
                }
                // error upon sending
                Err(e) => match e {
                    pulsar::Error::Connection(_)
                    | pulsar::Error::Producer(_)
                    | pulsar::Error::Consumer(_) => {
                        connection_err = Some(e);
                        tokio::time::sleep(self.config.retry_interval).await;
                        continue;
                    }
                    _ => return Err(SinkError::Pulsar(anyhow!(e))),
                },
            }
        }

        if !success_flag {
            Err(SinkError::Pulsar(anyhow!(connection_err.unwrap())))
        } else {
            Ok(())
        }
    }

    async fn write_inner(
        &mut self,
        event_key_object: Option<String>,
        event_object: Option<Vec<u8>>,
    ) -> Result<()> {
        let message = Message {
            partition_key: event_key_object,
            payload: event_object.unwrap_or_default(),
            ..Default::default()
        };

        self.send_message(message).await?;
        Ok(())
    }
}

impl FormattedSink for PulsarPayloadWriter<'_> {
    type K = String;
    type V = Vec<u8>;

    async fn write_one(&mut self, k: Option<Self::K>, v: Option<Self::V>) -> Result<()> {
        self.write_inner(k, v).await
    }
}

impl AsyncTruncateSinkWriter for PulsarSinkWriter {
    type DeliveryFuture = PulsarDeliveryFuture;

    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        // Structured to avoid `clippy::large_stack_frames` and `large_futures`
        let iter = {
            dispatch_sink_formatter_str_key_impl!(
                &self.formatter,
                formatter,
                {
                    // Convert items to owned, concrete types before any `.await`,
                    // so the future doesn't capture formatter/iterator generics.
                    formatter.format_chunk(&chunk).map(|r| {
                        let (key, value) = r?;
                        let key: Option<String> = key.map(SerTo::ser_to).transpose()?;
                        let value: Option<Vec<u8>> = value.map(SerTo::ser_to).transpose()?;
                        Ok((key, value)) as Result<_>
                    })
                },
                // Produce a single iterator type for all formatter variants.
                auto_enums::auto_enum(Iterator)
            )
        };

        // Only concrete state is held across `.await`, keeping the future small.
        let mut payload_writer = PulsarPayloadWriter {
            producer: &mut self.producer,
            add_future,
            config: &self.config,
        };

        for r in iter {
            let (key, value): (Option<String>, Option<Vec<u8>>) = r?;
            payload_writer.write_inner(key, value).await?;
        }

        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<()> {
        if is_checkpoint {
            self.producer
                .send_batch()
                .map_err(pulsar_to_sink_err)
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn base_properties() -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                "service.url".to_owned(),
                "pulsar://localhost:6650".to_owned(),
            ),
            ("topic".to_owned(), "test-topic".to_owned()),
        ])
    }

    fn parse_config_with(
        extra: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> PulsarConfig {
        let mut props = base_properties();
        props.extend(
            extra
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned())),
        );
        PulsarConfig::from_btreemap(props).unwrap()
    }

    #[test]
    fn test_pulsar_routing_mode_default_is_unset() {
        let config = parse_config_with([]);

        assert!(config.producer_properties.routing_mode.is_none());
    }

    #[test]
    fn test_pulsar_producer_routing_policy_preserves_unset_default() {
        let config = parse_config_with([]);

        assert!(pulsar_producer_routing_policy(config.producer_properties.routing_mode).is_none());
    }

    #[test]
    fn test_pulsar_producer_routing_policy_preserves_configured_mode() {
        let config = parse_config_with([("properties.routing.mode", "SinglePartition")]);

        assert!(matches!(
            pulsar_producer_routing_policy(config.producer_properties.routing_mode),
            Some(RoutingPolicy::Single)
        ));
    }

    #[test]
    fn test_parse_pulsar_routing_mode() {
        let config = parse_config_with([("properties.routing.mode", "round_robin")]);
        assert!(matches!(
            config.producer_properties.routing_mode,
            Some(PulsarRoutingMode::RoundRobin)
        ));

        let config = parse_config_with([("properties.routing.mode", "SinglePartition")]);
        assert!(matches!(
            config.producer_properties.routing_mode,
            Some(PulsarRoutingMode::Single)
        ));
    }

    #[test]
    fn test_parse_pulsar_routing_mode_alias() {
        let config = parse_config_with([("routing_mode", "single")]);

        assert!(matches!(
            config.producer_properties.routing_mode,
            Some(PulsarRoutingMode::Single)
        ));
    }
}
