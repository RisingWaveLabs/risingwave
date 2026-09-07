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

use std::sync::Arc;

use risingwave_common::array::ArrayError;
use risingwave_common::error::def_anyhow_newtype;
use risingwave_pb::PbFieldNotFound;
use risingwave_rpc_client::error::RpcError;

use crate::enforce_secret::EnforceSecretError;
use crate::parser::AccessError;
use crate::schema::InvalidOptionError;
use crate::schema::schema_registry::{
    ConcurrentRequestError, SchemaRegistryClientError, WireFormatError,
};
use crate::sink::SinkError;
use crate::source::mqtt::MqttError;
use crate::source::nats::NatsJetStreamError;

def_anyhow_newtype! {
    pub ConnectorError,

    // Common errors
    std::io::Error => transparent,
    Arc<ConnectorError> => transparent,

    // Fine-grained connector errors
    AccessError => transparent,
    WireFormatError => transparent,
    ConcurrentRequestError => transparent,
    InvalidOptionError => transparent,
    SinkError => transparent,
    PbFieldNotFound => transparent,

    // TODO(error-handling): Remove implicit contexts below and specify ad-hoc context for each conversion.

    // Parsing errors
    url::ParseError => "failed to parse URL",
    serde_json::Error => "failed to parse JSON",
    csv::Error => "failed to parse CSV",

    uuid::Error => transparent, // believed to be self-explanatory

    // Connector errors
    opendal::Error => transparent, // believed to be self-explanatory
    parquet::errors::ParquetError => transparent,
    ArrayError => "Array error",
    sqlx::Error => transparent, // believed to be self-explanatory
    mysql_async::Error => "MySQL error",
    tokio_postgres::Error => "Postgres error",
    tiberius::error::Error => "Sql Server error",
    apache_avro::Error => "Avro error",
    rdkafka::error::KafkaError => "Kafka error",
    pulsar::Error => "Pulsar error",

    async_nats::jetstream::consumer::StreamError => "Nats error",
    async_nats::jetstream::consumer::pull::MessagesError => "Nats error",
    async_nats::jetstream::context::CreateStreamError => "Nats error",
    async_nats::jetstream::stream::ConsumerError => "Nats error",
    async_nats::error::Error<async_nats::jetstream::context::RequestErrorKind> => "Nats error",
    NatsJetStreamError => "Nats error",

    risingwave_common::error::IcebergError => "IcebergV2 error",
    redis::RedisError => "Redis error",
    risingwave_common::array::arrow::arrow_schema_iceberg::ArrowError => "Arrow error",
    google_cloud_pubsub::client::google_cloud_auth::error::Error => "Google Cloud error",
    rumqttc::tokio_rustls::rustls::Error => "TLS error",
    rumqttc::v5::ClientError => "MQTT SDK error",
    rumqttc::v5::OptionError => "MQTT Option error",
    rumqttc::v5::ConnectionError => "MQTT Connection error",
    MqttError => "MQTT Source error",
    mongodb::error::Error => "Mongodb error",

    openssl::error::ErrorStack => "OpenSSL error",
    risingwave_common::secret::SecretError => "Secret error",
    EnforceSecretError => transparent,

    // ADBC errors
    #[cfg(feature = "source-adbc_snowflake")]
    adbc_core::error::Error => "ADBC error",
    SchemaRegistryClientError => "Schema registry client error",
}

pub type ConnectorResult<T, E = ConnectorError> = std::result::Result<T, E>;

impl From<ConnectorError> for RpcError {
    fn from(value: ConnectorError) -> Self {
        RpcError::Internal(value.0)
    }
}

#[expect(clippy::disallowed_types)]
impl From<iceberg::Error> for ConnectorError {
    fn from(value: iceberg::Error) -> Self {
        risingwave_common::error::IcebergError::from(value).into()
    }
}
