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

//! Common parameters and utilities for both source and sink.

mod mqtt_common;
pub use mqtt_common::{MqttCommon, QualityOfService as MqttQualityOfService};

mod common;
pub use common::{
    AwsAuthProps, AwsPrivateLinkItem, DISABLE_DEFAULT_CREDENTIAL, KafkaCommon,
    KafkaConnectionProps, KafkaPrivateLinkCommon, KinesisCommon, MongodbCommon, NatsCommon,
    NatsConnectionProps, PRIVATE_LINK_BROKER_REWRITE_MAP_KEY, PRIVATE_LINK_TARGETS_KEY,
    PulsarCommon, PulsarOauthCommon, RdKafkaPropertiesCommon, SHARED_NATS_CLIENT,
    TcpKeepaliveConfig,
};
mod connection;
pub use connection::{
    ConfluentSchemaRegistryConnection, Connection, ElasticsearchConnection, IcebergConnection,
    KafkaConnection, SCHEMA_REGISTRY_CONNECTION_TYPE, read_kafka_log_level, validate_connection,
};
pub use iceberg::compaction::{IcebergCommittedSnapshot, IcebergSinkCompactionUpdate};

mod iceberg;
#[cfg(not(madsim))]
mod maybe_tls_connector;
pub mod postgres;
pub mod sql_server;

pub use iceberg::{
    IcebergCatalogKind, IcebergCatalogRuntime, IcebergCommon, IcebergTableIdentifier,
    ResolvedIcebergCatalogConfig, iceberg_java_catalog_props_from_options,
};
pub use postgres::{
    PgConnectionConfig, PostgresExternalTable, SslMode, create_pg_client,
    create_pg_client_from_properties, discover_pgvector_dimensions,
    pg_connection_config_from_properties,
};

#[cfg(test)]
mod common_test;
