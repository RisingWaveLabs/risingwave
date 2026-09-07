// Copyright 2024 RisingWave Labs
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

use anyhow::anyhow;
use google_cloud_gax::conn::Environment;
use google_cloud_gax::grpc::Status;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::apiv1;
use google_cloud_pubsub::client::google_cloud_auth::credentials::CredentialsFile;
use google_cloud_pubsub::client::google_cloud_auth::project;
use google_cloud_pubsub::client::google_cloud_auth::token::DefaultTokenSourceProvider;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::publisher::Publisher;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use serde::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use super::catalog::SinkFormatDesc;
use super::formatter::SinkFormatterImpl;
use super::log_store::DeliveryFutureManagerAddFuture;
use super::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt, FormattedSink,
};
use super::{Result, Sink, SinkError, SinkParam, SinkWriterParam};
use crate::dispatch_sink_formatter_str_key_impl;
use crate::enforce_secret::EnforceSecret;

pub const PUBSUB_SINK: &str = "google_pubsub";
const PUBSUB_SEND_FUTURE_BUFFER_MAX_SIZE: usize = 65536;

mod delivery_future {
    use anyhow::Context;
    use futures::future::try_join_all;
    use futures::{FutureExt, TryFuture, TryFutureExt};
    use google_cloud_pubsub::publisher::Awaiter;

    use crate::sink::SinkError;

    pub type GooglePubSubSinkDeliveryFuture =
        impl TryFuture<Ok = (), Error = SinkError> + Unpin + 'static;

    #[define_opaque(GooglePubSubSinkDeliveryFuture)]
    pub(super) fn may_delivery_future(awaiter: Vec<Awaiter>) -> GooglePubSubSinkDeliveryFuture {
        try_join_all(awaiter.into_iter().map(|awaiter| {
            awaiter.get().map(|result| {
                result
                    .context("Google Pub/Sub sink error")
                    .map_err(SinkError::GooglePubSub)
                    .map(|_| ())
            })
        }))
        .map_ok(|_: Vec<()>| ())
        .boxed()
    }
}

use delivery_future::*;

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct GooglePubSubConfig {
    /// The Google Pub/Sub Project ID
    #[serde(rename = "pubsub.project_id")]
    pub project_id: String,

    /// Specifies the Pub/Sub topic to publish messages
    #[serde(rename = "pubsub.topic")]
    pub topic: String,

    /// The Google Pub/Sub endpoint URL
    #[serde(rename = "pubsub.endpoint")]
    pub endpoint: String,

    /// use the connector with a pubsub emulator
    /// <https://cloud.google.com/pubsub/docs/emulator>
    #[serde(rename = "pubsub.emulator_host")]
    pub emulator_host: Option<String>,

    /// A JSON string containing the service account credentials for authorization,
    /// see the [service-account](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account) credentials guide.
    /// The provided account credential must have the
    /// `pubsub.publisher` [role](https://cloud.google.com/pubsub/docs/access-control#roles)
    #[serde(rename = "pubsub.credentials")]
    pub credentials: Option<String>,

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(GooglePubSubConfig);

impl EnforceSecret for GooglePubSubConfig {
    const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf::phf_set! {
        "pubsub.credentials",
    };
}

impl GooglePubSubConfig {
    fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        serde_json::from_value::<GooglePubSubConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))
    }
}

#[derive(Clone, Debug)]
pub struct GooglePubSubSink {
    pub config: GooglePubSubConfig,
    is_append_only: bool,

    schema: Schema,
    pk_indices: Vec<usize>,
    format_desc: SinkFormatDesc,
    db_name: String,
    sink_from_name: String,
}

impl EnforceSecret for GooglePubSubSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            GooglePubSubConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}
impl Sink for GooglePubSubSink {
    type LogSinker = AsyncTruncateLogSinkerOf<GooglePubSubSinkWriter>;

    const SINK_NAME: &'static str = PUBSUB_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            return Err(SinkError::GooglePubSub(anyhow!(
                "Google Pub/Sub sink only support append-only mode"
            )));
        }

        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(GooglePubSubSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            &self.format_desc,
            self.db_name.clone(),
            self.sink_from_name.clone(),
        )
        .await?
        .into_log_sinker(PUBSUB_SEND_FUTURE_BUFFER_MAX_SIZE))
    }
}

impl TryFrom<SinkParam> for GooglePubSubSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = GooglePubSubConfig::from_btreemap(param.properties)?;
        let format_desc = param
            .format_desc
            .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?;
        Ok(Self {
            config,
            is_append_only: param.sink_type.is_append_only(),
            schema,
            pk_indices,
            format_desc,
            db_name: param.db_name,
            sink_from_name: param.sink_from_name,
        })
    }
}

struct GooglePubSubPayloadWriter<'w> {
    publisher: &'w mut Publisher,
    message_vec: Vec<PubsubMessage>,
    add_future: DeliveryFutureManagerAddFuture<'w, GooglePubSubSinkDeliveryFuture>,
}

impl GooglePubSubSinkWriter {
    pub async fn new(
        config: GooglePubSubConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        format_desc: &SinkFormatDesc,
        db_name: String,
        sink_from_name: String,
    ) -> Result<Self> {
        let environment = if let Some(ref cred) = config.credentials {
            let mut auth_config = project::Config::default();
            auth_config = auth_config.with_audience(apiv1::conn_pool::AUDIENCE);
            auth_config = auth_config.with_scopes(&apiv1::conn_pool::SCOPES);
            let cred_file = CredentialsFile::new_from_str(cred).await.map_err(|e| {
                SinkError::GooglePubSub(
                    anyhow!(e).context("Failed to create Google Cloud Pub/Sub credentials file"),
                )
            })?;
            let provider =
                DefaultTokenSourceProvider::new_with_credentials(auth_config, Box::new(cred_file))
                    .await
                    .map_err(|e| {
                        SinkError::GooglePubSub(
                            anyhow!(e).context(
                                "Failed to create Google Cloud Pub/Sub token source provider",
                            ),
                        )
                    })?;
            Environment::GoogleCloud(Box::new(provider))
        } else if let Some(emu_host) = config.emulator_host {
            Environment::Emulator(emu_host)
        } else {
            let mut auth_config = project::Config::default();
            auth_config = auth_config.with_audience(apiv1::conn_pool::AUDIENCE);
            auth_config = auth_config.with_scopes(&apiv1::conn_pool::SCOPES);

            let provider = DefaultTokenSourceProvider::new(auth_config)
                .await
                .map_err(|e| {
                    SinkError::GooglePubSub(
                        anyhow!(e).context(
                            "No explicit credentials or emulator_host provided. \
                             Attempted to authenticate via Google Application Default Credentials (ADC) but not successful. \
                             Please configure credentials via one of the following methods: \
                             1) Set `pubsub.credentials` parameter in your sink configuration, \
                             2) Set `GOOGLE_APPLICATION_CREDENTIALS_JSON` environment variable (JSON content), \
                             3) Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable (file path), \
                             4) Run `gcloud auth application-default login` for local development, \
                             5) Ensure workload identity is configured on GCE/GKE"
                        )
                    )
                })?;
            Environment::GoogleCloud(Box::new(provider))
        };

        let client_config = ClientConfig {
            endpoint: config.endpoint,
            project_id: Some(config.project_id),
            environment,
            ..Default::default()
        };
        let client = Client::new(client_config)
            .await
            .map_err(|e| SinkError::GooglePubSub(anyhow!(e)))?;

        let topic = async {
            let topic = client.topic(&config.topic);
            if !topic.exists(None).await? {
                topic.create(None, None).await?;
            }
            Ok(topic)
        }
        .await
        .map_err(|e: Status| SinkError::GooglePubSub(anyhow!(e)))?;

        let formatter = SinkFormatterImpl::new(
            format_desc,
            schema,
            pk_indices,
            db_name,
            sink_from_name,
            topic.fully_qualified_name(),
        )
        .await?;

        let publisher = topic.new_publisher(None);

        Ok(Self {
            formatter,
            publisher,
        })
    }
}

pub struct GooglePubSubSinkWriter {
    formatter: SinkFormatterImpl,
    publisher: Publisher,
}

impl AsyncTruncateSinkWriter for GooglePubSubSinkWriter {
    type DeliveryFuture = GooglePubSubSinkDeliveryFuture;

    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        let mut payload_writer = GooglePubSubPayloadWriter {
            publisher: &mut self.publisher,
            message_vec: Vec::with_capacity(chunk.cardinality()),
            add_future,
        };
        dispatch_sink_formatter_str_key_impl!(&self.formatter, formatter, {
            payload_writer.write_chunk(chunk, formatter).await
        })?;
        payload_writer.finish().await
    }
}

impl GooglePubSubPayloadWriter<'_> {
    pub async fn finish(&mut self) -> Result<()> {
        let message_vec = std::mem::take(&mut self.message_vec);
        let awaiters = self.publisher.publish_bulk(message_vec).await;
        self.add_future
            .add_future_may_await(may_delivery_future(awaiters))
            .await?;
        Ok(())
    }
}

impl FormattedSink for GooglePubSubPayloadWriter<'_> {
    type K = String;
    type V = Vec<u8>;

    async fn write_one(&mut self, k: Option<Self::K>, v: Option<Self::V>) -> Result<()> {
        let ordering_key = k.unwrap_or_default();
        match v {
            Some(data) => {
                let msg = PubsubMessage {
                    data,
                    ordering_key,
                    ..Default::default()
                };
                self.message_vec.push(msg);
                Ok(())
            }
            None => Err(SinkError::GooglePubSub(anyhow!(
                "Google Pub/Sub sink error: missing value to publish"
            ))),
        }
    }
}
