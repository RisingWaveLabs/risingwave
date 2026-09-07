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

use core::fmt::Debug;
use core::future::IntoFuture;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, anyhow};
use async_nats::jetstream::context::Context;
use futures::FutureExt;
use futures::prelude::TryFuture;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::util::retry::exponential_backoff;
use serde::Deserialize;
use serde_with::serde_as;
use tokio_retry::Retry;
use tokio_retry::strategy::jitter;
use with_options::WithOptions;

use super::SinkWriterParam;
use super::encoder::{
    DateHandlingMode, JsonbHandlingMode, TimeHandlingMode, TimestamptzHandlingMode,
};
use super::utils::chunk_to_json;
use crate::connector_common::NatsCommon;
use crate::enforce_secret::EnforceSecret;
use crate::sink::encoder::{JsonEncoder, TimestampHandlingMode};
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use crate::sink::{Result, SINK_TYPE_APPEND_ONLY, Sink, SinkError, SinkParam};

pub const NATS_SINK: &str = "nats";
const NATS_SEND_FUTURE_BUFFER_MAX_SIZE: usize = 65536;

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct NatsConfig {
    #[serde(flatten)]
    pub common: NatsCommon,
    // accept "append-only"
    pub r#type: String,

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(NatsConfig);

#[derive(Clone, Debug)]
pub struct NatsSink {
    pub config: NatsConfig,
    schema: Schema,
    is_append_only: bool,
}

impl EnforceSecret for NatsSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            NatsCommon::enforce_one(prop)?;
        }
        Ok(())
    }
}

// sink write
pub struct NatsSinkWriter {
    pub config: NatsConfig,
    context: Context,
    /// Hold the client Arc to keep it alive. This allows the shared client cache to reuse
    /// the connection while we're still using it.
    #[expect(dead_code)]
    client: Arc<async_nats::Client>,
    #[expect(dead_code)]
    schema: Schema,
    json_encoder: JsonEncoder,
}

pub type NatsSinkDeliveryFuture = impl TryFuture<Ok = (), Error = SinkError> + Unpin + 'static;

/// Basic data types for use with the nats interface
impl NatsConfig {
    pub fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<NatsConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY {
            Err(SinkError::Config(anyhow!(
                "NATS sink only supports append-only mode"
            )))
        } else {
            Ok(config)
        }
    }
}

impl TryFrom<SinkParam> for NatsSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = NatsConfig::from_btreemap(param.properties)?;
        Ok(Self {
            config,
            schema,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

impl Sink for NatsSink {
    type LogSinker = AsyncTruncateLogSinkerOf<NatsSinkWriter>;

    const SINK_NAME: &'static str = NATS_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            return Err(SinkError::Nats(anyhow!(
                "NATS sink only supports append-only mode"
            )));
        }
        let _client = (self.config.common.build_client().await)
            .context("validate nats sink error")
            .map_err(SinkError::Nats)?;
        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(
            NatsSinkWriter::new(self.config.clone(), self.schema.clone())
                .await?
                .into_log_sinker(NATS_SEND_FUTURE_BUFFER_MAX_SIZE),
        )
    }
}

impl NatsSinkWriter {
    pub async fn new(config: NatsConfig, schema: Schema) -> Result<Self> {
        let client = config
            .common
            .build_client()
            .await
            .map_err(|e| SinkError::Nats(anyhow!(e)))?;
        let context = NatsCommon::build_context_from_client(&client);
        Ok::<_, SinkError>(Self {
            config: config.clone(),
            context,
            client,
            schema: schema.clone(),
            json_encoder: JsonEncoder::new(
                schema,
                None,
                DateHandlingMode::FromCe,
                TimestampHandlingMode::Milli,
                TimestamptzHandlingMode::UtcWithoutSuffix,
                TimeHandlingMode::Milli,
                JsonbHandlingMode::String,
            ),
        })
    }
}

impl AsyncTruncateSinkWriter for NatsSinkWriter {
    type DeliveryFuture = NatsSinkDeliveryFuture;

    #[define_opaque(NatsSinkDeliveryFuture)]
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        mut add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        let mut data = chunk_to_json(chunk, &self.json_encoder)?;
        for item in &mut data {
            let publish_ack_future = Retry::spawn(
                exponential_backoff(Duration::from_millis(100), 2, Duration::MAX)
                    .map(jitter)
                    .take(3),
                || async {
                    self.context
                        .publish(self.config.common.subject.clone(), item.clone().into())
                        .await
                        .context("nats sink error")
                        .map_err(SinkError::Nats)
                },
            )
            .await
            .context("nats sink error")
            .map_err(SinkError::Nats)?;
            let future = publish_ack_future.into_future().map(|result| {
                result
                    .context("Nats sink error")
                    .map_err(SinkError::Nats)
                    .map(|_| ())
            });
            add_future.add_future_may_await(future).await?;
        }
        Ok(())
    }
}
