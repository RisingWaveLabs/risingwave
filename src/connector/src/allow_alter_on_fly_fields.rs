// Copyright 2025 RisingWave Labs
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

// THIS FILE IS AUTO_GENERATED. DO NOT EDIT
// UPDATE WITH: ./risedev generate-with-options
// This file is rewritten by `tests::test_allow_alter_on_fly_fields_rust_up_to_date` with
// `UPDATE_EXPECT=1`.
// To update content, change source/sink/connection WITH options definitions (for example,
// `#[with_option(allow_alter_on_fly)]` on struct fields), then run `./risedev generate-with-options`.
// `./risedev generate-with-options` runs three UPDATE_EXPECT tests:
// 1) refresh `with_options_{source,sink,connection}.yaml`;
// 2) regenerate this file from those YAML files;
// 3) regenerate the Iceberg Engine option classifier.

#![rustfmt::skip]

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use crate::error::ConnectorError;
use crate::sink::remote::JdbcSink;
use crate::sink::Sink;

macro_rules! use_source_properties {
    ({ $({ $variant_name:ident, $prop_name:ty, $split:ty }),* }) => {
        $(
            #[allow(unused_imports)]
            pub(super) use $prop_name;
        )*
    };
}

mod source_properties {
    use crate::for_all_sources;
    use crate::source::base::SourceProperties;

    for_all_sources!(use_source_properties);

    /// Implements a function that maps a source name string to the Rust type name of the corresponding property type.
    /// Usage: `impl_source_name_to_prop_type_name!();` will generate:
    /// ```ignore
    /// pub fn source_name_to_prop_type_name(source_name: &str) -> Option<&'static str>
    /// ```
    macro_rules! impl_source_name_to_prop_type_name_inner {
        ({ $({$variant:ident, $prop_name:ty, $split:ty}),* }) => {
            pub fn source_name_to_prop_type_name(source_name: &str) -> Option<&'static str> {
                match source_name {
                    $(
                        <$prop_name>::SOURCE_NAME => Some(std::any::type_name::<$prop_name>()),
                    )*
                    _ => None,
                }
            }
        };
    }

    macro_rules! impl_source_name_to_prop_type_name {
        () => {
            $crate::for_all_sources! { impl_source_name_to_prop_type_name_inner }
        };
    }

    impl_source_name_to_prop_type_name!();
}

mod sink_properties {
    use crate::use_all_sink_configs;
    use crate::sink::Sink;
    use crate::sink::file_sink::fs::FsSink;

    use_all_sink_configs!();

    macro_rules! impl_sink_name_to_config_type_name_inner {
        ({ $({ $variant_name:ident, $sink_type:ty, $config_type:ty }),* }) => {
            pub fn sink_name_to_config_type_name(sink_name: &str) -> Option<&'static str> {
                match sink_name {
                $(
                    <$sink_type>::SINK_NAME => Some(std::any::type_name::<$config_type>()),
                )*
                    _ => None,
                }
            }
        };
    }

    macro_rules! impl_sink_name_to_config_type_name {
        () => {
            $crate::for_all_sinks! { impl_sink_name_to_config_type_name_inner }
        };
    }

    impl_sink_name_to_config_type_name!();
}

/// Map of source connector names to their `allow_alter_on_fly` field names
pub static SOURCE_ALLOW_ALTER_ON_FLY_FIELDS: LazyLock<HashMap<String, HashSet<String>>> = LazyLock::new(|| {
    use source_properties::*;
    let mut map = HashMap::new();
    // CDC Properties - added for schema.change.failure.policy
    map.try_insert(
        std::any::type_name::<MysqlCdcProperties>().to_owned(),
        [
            "cdc.source.wait.streaming.start.timeout".to_owned(),
            "debezium.max.queue.size".to_owned(),
            "debezium.queue.memory.ratio".to_owned(),
            "hostname".to_owned(),
            "port".to_owned(),
            "password".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    map.try_insert(
        std::any::type_name::<PostgresCdcProperties>().to_owned(),
        [
            "cdc.source.wait.streaming.start.timeout".to_owned(),
            "debezium.max.queue.size".to_owned(),
            "debezium.queue.memory.ratio".to_owned(),
            "debezium.heartbeat.interval.ms".to_owned(),
            "hostname".to_owned(),
            "port".to_owned(),
            "password".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    map.try_insert(
        std::any::type_name::<SqlServerCdcProperties>().to_owned(),
        [
            "cdc.source.wait.streaming.start.timeout".to_owned(),
            "debezium.max.queue.size".to_owned(),
            "debezium.queue.memory.ratio".to_owned(),
            "hostname".to_owned(),
            "port".to_owned(),
            "password".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    map.try_insert(
        std::any::type_name::<OracleCdcProperties>().to_owned(),
        [
            "cdc.source.wait.streaming.start.timeout".to_owned(),
            "debezium.max.queue.size".to_owned(),
            "debezium.queue.memory.ratio".to_owned(),
            "debezium.heartbeat.interval.ms".to_owned(),
            "heartbeat.table.name".to_owned(),
            "hostname".to_owned(),
            "port".to_owned(),
            "password".to_owned(),
        ].into_iter().collect(),
    ).unwrap();

    map.try_insert(
        std::any::type_name::<MongodbCdcProperties>().to_owned(),
        [
            "cdc.source.wait.streaming.start.timeout".to_owned(),
            "debezium.max.queue.size".to_owned(),
            "debezium.queue.memory.ratio".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    // KafkaProperties
    map.try_insert(
        std::any::type_name::<KafkaProperties>().to_owned(),
        [
            "group.id.prefix".to_owned(),
            "properties.sync.call.timeout".to_owned(),
            "properties.security.protocol".to_owned(),
            "properties.ssl.endpoint.identification.algorithm".to_owned(),
            "properties.ssl.ca.location".to_owned(),
            "properties.ssl.ca.pem".to_owned(),
            "properties.ssl.certificate.location".to_owned(),
            "properties.ssl.certificate.pem".to_owned(),
            "properties.ssl.key.location".to_owned(),
            "properties.ssl.key.pem".to_owned(),
            "properties.ssl.key.password".to_owned(),
            "properties.sasl.mechanism".to_owned(),
            "properties.sasl.username".to_owned(),
            "properties.sasl.password".to_owned(),
            "properties.message.max.bytes".to_owned(),
            "properties.receive.message.max.bytes".to_owned(),
            "properties.statistics.interval.ms".to_owned(),
            "properties.client.id".to_owned(),
            "properties.enable.ssl.certificate.verification".to_owned(),
            "properties.reconnect.backoff.ms".to_owned(),
            "properties.reconnect.backoff.max.ms".to_owned(),
            "properties.socket.connection.setup.timeout.ms".to_owned(),
            "properties.retry.backoff.ms".to_owned(),
            "properties.retry.backoff.max.ms".to_owned(),
            "properties.queued.min.messages".to_owned(),
            "properties.queued.max.messages.kbytes".to_owned(),
            "properties.fetch.wait.max.ms".to_owned(),
            "properties.fetch.queue.backoff.ms".to_owned(),
            "properties.fetch.max.bytes".to_owned(),
            "properties.enable.auto.commit".to_owned(),
            "properties.auto.commit.interval.ms".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    // PubsubProperties
    map.try_insert(
        std::any::type_name::<PubsubProperties>().to_owned(),
        [
            "pubsub.ack_deadline_seconds".to_owned(),
            "pubsub.max_outstanding_messages".to_owned(),
            "pubsub.max_outstanding_bytes".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    // PulsarProperties
    map.try_insert(
        std::any::type_name::<PulsarProperties>().to_owned(),
        [
            "pulsar.operation.retry.max.retries".to_owned(),
            "pulsar.operation.retry.delay".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    map
});

/// Map of sink connector names to their `allow_alter_on_fly` field names
pub static SINK_ALLOW_ALTER_ON_FLY_FIELDS: LazyLock<HashMap<String, HashSet<String>>> = LazyLock::new(|| {
    use sink_properties::*;
    let mut map = HashMap::new();
    // ClickHouseConfig
    map.try_insert(
        std::any::type_name::<ClickHouseConfig>().to_owned(),
        [
            "commit_checkpoint_interval".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    // DeltaLakeConfig
    map.try_insert(
        std::any::type_name::<DeltaLakeConfig>().to_owned(),
        [
            "commit_checkpoint_interval".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    // DorisConfig
    map.try_insert(
        std::any::type_name::<DorisConfig>().to_owned(),
        [
            "doris.stream_load.http.timeout.ms".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    // ElasticSearchConfig
    map.try_insert(
        std::any::type_name::<ElasticSearchConfig>().to_owned(),
        [
            "batch_num_messages".to_owned(),
            "batch_size_kb".to_owned(),
            "concurrent_requests".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    // IcebergConfig
    map.try_insert(
        std::any::type_name::<IcebergConfig>().to_owned(),
        [
            "commit_checkpoint_interval".to_owned(),
            "enable_compaction".to_owned(),
            "compaction_interval_sec".to_owned(),
            "enable_snapshot_expiration".to_owned(),
            "snapshot_expiration_max_age_millis".to_owned(),
            "snapshot_expiration_retain_last".to_owned(),
            "snapshot_expiration_clear_expired_files".to_owned(),
            "snapshot_expiration_clear_expired_meta_data".to_owned(),
            "enable_manifest_rewrite".to_owned(),
            "manifest_rewrite_target_size_bytes".to_owned(),
            "manifest_rewrite_min_count_to_merge".to_owned(),
            "compaction.max_snapshots_num".to_owned(),
            "compaction.small_files_threshold_mb".to_owned(),
            "compaction.delete_files_count_threshold".to_owned(),
            "compaction.trigger_snapshot_count".to_owned(),
            "compaction.target_file_size_mb".to_owned(),
            "compaction.type".to_owned(),
            "compaction.write_parquet_compression".to_owned(),
            "compaction.write_parquet_max_row_group_rows".to_owned(),
            "compaction.write_parquet_max_row_group_bytes".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    // KafkaConfig
    map.try_insert(
        std::any::type_name::<KafkaConfig>().to_owned(),
        [
            "properties.sync.call.timeout".to_owned(),
            "properties.security.protocol".to_owned(),
            "properties.ssl.endpoint.identification.algorithm".to_owned(),
            "properties.ssl.ca.location".to_owned(),
            "properties.ssl.ca.pem".to_owned(),
            "properties.ssl.certificate.location".to_owned(),
            "properties.ssl.certificate.pem".to_owned(),
            "properties.ssl.key.location".to_owned(),
            "properties.ssl.key.pem".to_owned(),
            "properties.ssl.key.password".to_owned(),
            "properties.sasl.mechanism".to_owned(),
            "properties.sasl.username".to_owned(),
            "properties.sasl.password".to_owned(),
            "properties.message.max.bytes".to_owned(),
            "properties.receive.message.max.bytes".to_owned(),
            "properties.statistics.interval.ms".to_owned(),
            "properties.client.id".to_owned(),
            "properties.enable.ssl.certificate.verification".to_owned(),
            "properties.reconnect.backoff.ms".to_owned(),
            "properties.reconnect.backoff.max.ms".to_owned(),
            "properties.socket.connection.setup.timeout.ms".to_owned(),
            "properties.retry.backoff.ms".to_owned(),
            "properties.retry.backoff.max.ms".to_owned(),
            "properties.allow.auto.create.topics".to_owned(),
            "properties.queue.buffering.max.messages".to_owned(),
            "properties.queue.buffering.max.kbytes".to_owned(),
            "properties.queue.buffering.max.ms".to_owned(),
            "properties.enable.idempotence".to_owned(),
            "properties.message.send.max.retries".to_owned(),
            "properties.batch.num.messages".to_owned(),
            "properties.batch.size".to_owned(),
            "properties.message.timeout.ms".to_owned(),
            "properties.max.in.flight.requests.per.connection".to_owned(),
            "properties.request.required.acks".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    // OpenSearchConfig
    map.try_insert(
        std::any::type_name::<OpenSearchConfig>().to_owned(),
        [
            "batch_num_messages".to_owned(),
            "batch_size_kb".to_owned(),
            "concurrent_requests".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    // PulsarConfig
    map.try_insert(
        std::any::type_name::<PulsarConfig>().to_owned(),
        [
            "properties.routing.mode".to_owned(),
            "properties.routing_mode".to_owned(),
            "routing_mode".to_owned(),
            "pulsar.routing_mode".to_owned(),
            "pulsar.routing.mode".to_owned(),
            "pulsar.properties.routing.mode".to_owned(),
            "pulsar.properties.routing_mode".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    // SnowflakeV2Config
    map.try_insert(
        std::any::type_name::<SnowflakeV2Config>().to_owned(),
        [
            "commit_checkpoint_interval".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    // StarrocksConfig
    map.try_insert(
        std::any::type_name::<StarrocksConfig>().to_owned(),
        [
            "starrocks.stream_load.http.timeout.ms".to_owned(),
            "commit_checkpoint_interval".to_owned(),
            "starrocks.max_batch_size_bytes".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    // TurbopufferConfig
    map.try_insert(
        std::any::type_name::<TurbopufferConfig>().to_owned(),
        [
            "write_batch_size".to_owned(),
            "max_linger_second".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    // Jdbc
    map.try_insert(
        JdbcSink::SINK_NAME.to_owned(),
        [
            "jdbc.url".to_owned(),
            "user".to_owned(),
            "password".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    map
});

/// Map of connection names to their `allow_alter_on_fly` field names
pub static CONNECTION_ALLOW_ALTER_ON_FLY_FIELDS: LazyLock<HashMap<String, HashSet<String>>> = LazyLock::new(|| {
    use crate::connector_common::*;
    let mut map = HashMap::new();
    // KafkaConnection
    map.try_insert(
        std::any::type_name::<KafkaConnection>().to_owned(),
        [
            "properties.security.protocol".to_owned(),
            "properties.ssl.endpoint.identification.algorithm".to_owned(),
            "properties.ssl.ca.location".to_owned(),
            "properties.ssl.ca.pem".to_owned(),
            "properties.ssl.certificate.location".to_owned(),
            "properties.ssl.certificate.pem".to_owned(),
            "properties.ssl.key.location".to_owned(),
            "properties.ssl.key.pem".to_owned(),
            "properties.ssl.key.password".to_owned(),
            "properties.sasl.mechanism".to_owned(),
            "properties.sasl.username".to_owned(),
            "properties.sasl.password".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    // Jdbc
    map.try_insert(
        JdbcSink::SINK_NAME.to_owned(),
        [
            "jdbc.url".to_owned(),
            "user".to_owned(),
            "password".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    map
});

/// Get all source connector names that have `allow_alter_on_fly` fields
pub fn get_source_connectors_with_allow_alter_on_fly_fields() -> Vec<&'static str> {
    SOURCE_ALLOW_ALTER_ON_FLY_FIELDS.keys().map(|s| s.as_str()).collect()
}

/// Get all sink connector names that have `allow_alter_on_fly` fields
pub fn get_sink_connectors_with_allow_alter_on_fly_fields() -> Vec<&'static str> {
    SINK_ALLOW_ALTER_ON_FLY_FIELDS.keys().map(|s| s.as_str()).collect()
}

/// Get all connection names that have `allow_alter_on_fly` fields
pub fn get_connection_names_with_allow_alter_on_fly_fields() -> Vec<&'static str> {
    CONNECTION_ALLOW_ALTER_ON_FLY_FIELDS.keys().map(|s| s.as_str()).collect()
}

/// Checks if all given fields are allowed to be altered on the fly for the specified source connector.
/// Returns Ok(()) if all fields are allowed, otherwise returns a `ConnectorError`.
pub fn check_source_allow_alter_on_fly_fields(
    connector_name: &str,
    fields: &[String],
) -> crate::error::ConnectorResult<()> {
    // Convert connector name to the type name key
    let Some(type_name) = source_properties::source_name_to_prop_type_name(connector_name) else {
        return Err(ConnectorError::from(anyhow::anyhow!(
            "Unknown source connector: {connector_name}"
        )));
    };
    let Some(allowed_fields) = SOURCE_ALLOW_ALTER_ON_FLY_FIELDS.get(type_name) else {
    return Err(ConnectorError::from(anyhow::anyhow!(
        "No allow_alter_on_fly fields registered for connector: {connector_name}"
    )));
    };
    for field in fields {
        if !allowed_fields.contains(field) {
            return Err(ConnectorError::from(anyhow::anyhow!(
                "Field '{field}' is not allowed to be altered on the fly for connector: {connector_name}"
            )));
        }
    }
    Ok(())
}

pub fn check_connection_allow_alter_on_fly_fields(
    connection_name: &str,
    fields: &[String],
) -> crate::error::ConnectorResult<()> {
    use crate::source::connection_name_to_prop_type_name;

    // Convert connection name to the type name key
    let Some(type_name) = connection_name_to_prop_type_name(connection_name) else {
        return Err(ConnectorError::from(anyhow::anyhow!(
            "Unknown connection: {connection_name}"
        )));
    };
    let Some(allowed_fields) = CONNECTION_ALLOW_ALTER_ON_FLY_FIELDS.get(type_name) else {
        return Err(ConnectorError::from(anyhow::anyhow!(
            "No allow_alter_on_fly fields registered for connection: {connection_name}"
        )));
    };
    for field in fields {
        if !allowed_fields.contains(field) {
            return Err(ConnectorError::from(anyhow::anyhow!(
                "Field '{field}' is not allowed to be altered on the fly for connection: {connection_name}"
            )));
        }
    }
    Ok(())
}

/// Checks if all given fields are allowed to be altered on the fly for the specified sink connector.
/// Returns Ok(()) if all fields are allowed, otherwise returns a `ConnectorError`.
pub fn check_sink_allow_alter_on_fly_fields(
    sink_name: &str,
    fields: &[String],
) -> crate::error::ConnectorResult<()> {
    // TODO(#24846): JDBC sink currently uses `()` as sink config type in `for_all_sinks!`,
    // so it cannot have an isolated key in `SINK_ALLOW_ALTER_ON_FLY_FIELDS`.
    // Reuse the JDBC entry in `CONNECTION_ALLOW_ALTER_ON_FLY_FIELDS` for now.
    // TODO(#24846): remove this special case after JDBC sink has a dedicated config type
    // and allow-alter fields are generated directly into `SINK_ALLOW_ALTER_ON_FLY_FIELDS`.
    let allowed_fields = if sink_name == JdbcSink::SINK_NAME {
        CONNECTION_ALLOW_ALTER_ON_FLY_FIELDS.get(JdbcSink::SINK_NAME)
    } else {
        // Convert sink name to the type name key
        let Some(type_name) = sink_properties::sink_name_to_config_type_name(sink_name) else {
            return Err(ConnectorError::from(anyhow::anyhow!(
                "Unknown sink connector: {sink_name}"
            )));
        };
        SINK_ALLOW_ALTER_ON_FLY_FIELDS.get(type_name)
    };
    let Some(allowed_fields) = allowed_fields else {
        return Err(ConnectorError::from(anyhow::anyhow!(
            "No allow_alter_on_fly fields registered for sink: {sink_name}"
        )));
    };
    for field in fields {
        if !allowed_fields.contains(field) {
            return Err(ConnectorError::from(anyhow::anyhow!(
                "Field '{field}' is not allowed to be altered on the fly for sink: {sink_name}"
            )));
        }
    }
    Ok(())
}
