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

use risingwave_connector::source::{ADBC_SNOWFLAKE_CONNECTOR, BATCH_POSIX_FS_CONNECTOR};

use super::*;

pub static SOURCE_ALLOWED_CONNECTION_CONNECTOR: LazyLock<HashSet<PbConnectionType>> =
    LazyLock::new(|| {
        hashset! {
            PbConnectionType::Unspecified,
            PbConnectionType::Kafka,
            PbConnectionType::Iceberg,
        }
    });

pub static SOURCE_ALLOWED_CONNECTION_SCHEMA_REGISTRY: LazyLock<HashSet<PbConnectionType>> =
    LazyLock::new(|| {
        hashset! {
            PbConnectionType::Unspecified,
            PbConnectionType::SchemaRegistry,
        }
    });

// TODO: Better design if we want to support ENCODE KEY where we will have 4 dimensional array
static CONNECTORS_COMPATIBLE_FORMATS: LazyLock<HashMap<String, HashMap<Format, Vec<Encode>>>> =
    LazyLock::new(|| {
        convert_args!(hashmap!(
                KAFKA_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Avro, Encode::Bytes, Encode::Csv],
                    Format::Upsert => vec![Encode::Json, Encode::Avro, Encode::Protobuf],
                    Format::Debezium => vec![Encode::Json, Encode::Avro],
                    Format::DebeziumMongo => vec![Encode::Json],
                ),
                PULSAR_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Avro, Encode::Bytes],
                    Format::Upsert => vec![Encode::Json, Encode::Avro],
                    Format::Debezium => vec![Encode::Json],
                ),
                KINESIS_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Avro, Encode::Bytes, Encode::Csv],
                    Format::Upsert => vec![Encode::Json, Encode::Avro],
                    Format::Debezium => vec![Encode::Json],
                ),
                GOOGLE_PUBSUB_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Avro, Encode::Bytes],
                    Format::Debezium => vec![Encode::Json],
                ),
                NEXMARK_CONNECTOR => hashmap!(
                    Format::Native => vec![Encode::Native],
                    Format::Plain => vec![Encode::Bytes],
                ),
                DATAGEN_CONNECTOR => hashmap!(
                    Format::Native => vec![Encode::Native],
                    Format::Plain => vec![Encode::Bytes, Encode::Json],
                ),
                OPENDAL_S3_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Csv, Encode::Json, Encode::Parquet],
                ),
                GCS_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Csv, Encode::Json, Encode::Parquet],
                ),
                AZBLOB_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Csv, Encode::Json, Encode::Parquet],
                ),
                POSIX_FS_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Csv, Encode::Json, Encode::Parquet],
                ),
                BATCH_POSIX_FS_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Csv],
                ),
                MYSQL_CDC_CONNECTOR => hashmap!(
                    Format::Debezium => vec![Encode::Json],
                    // support source stream job
                    Format::Plain => vec![Encode::Json],
                ),
                POSTGRES_CDC_CONNECTOR => hashmap!(
                    Format::Debezium => vec![Encode::Json],
                    // support source stream job
                    Format::Plain => vec![Encode::Json],
                ),
                ORACLE_CDC_CONNECTOR => hashmap!(
                    Format::Debezium => vec![Encode::Json],
                    // support source stream job
                    Format::Plain => vec![Encode::Json],
                ),
                MONGODB_CDC_CONNECTOR => hashmap!(
                    Format::DebeziumMongo => vec![Encode::Json],
                ),
                NATS_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Bytes],
                ),
                MQTT_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Bytes],
                ),
                TEST_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json],
                ),
                ICEBERG_CONNECTOR => hashmap!(
                    Format::None => vec![Encode::None],
                ),
                ADBC_SNOWFLAKE_CONNECTOR => hashmap!(
                    Format::None => vec![Encode::None],
                ),
                SQL_SERVER_CDC_CONNECTOR => hashmap!(
                    Format::Debezium => vec![Encode::Json],
                    // support source stream job
                    Format::Plain => vec![Encode::Json],
                ),
        ))
    });

fn validate_license(connector: &str) -> Result<()> {
    if connector == SQL_SERVER_CDC_CONNECTOR {
        Feature::SqlServerCdcSource.check_available()?;
    }
    Ok(())
}

pub fn validate_compatibility(
    format_encode: &FormatEncodeOptions,
    props: &mut BTreeMap<String, String>,
) -> Result<()> {
    let mut connector = props
        .get_connector()
        .ok_or_else(|| RwError::from(ProtocolError("missing field 'connector'".to_owned())))?;

    if connector == OPENDAL_S3_CONNECTOR {
        // reject s3_v2 creation
        return Err(RwError::from(Deprecated(
            OPENDAL_S3_CONNECTOR.to_owned(),
            LEGACY_S3_CONNECTOR.to_owned(),
        )));
    }
    if connector == LEGACY_S3_CONNECTOR {
        // S3 connector is deprecated, use OPENDAL_S3_CONNECTOR instead
        // do s3 -> s3_v2 migration
        let entry = props.get_mut(UPSTREAM_SOURCE_KEY).unwrap();
        *entry = OPENDAL_S3_CONNECTOR.to_owned();
        connector = OPENDAL_S3_CONNECTOR.to_owned();
    }

    let compatible_formats = CONNECTORS_COMPATIBLE_FORMATS
        .get(&connector)
        .ok_or_else(|| {
            RwError::from(ProtocolError(format!(
                "connector {:?} is not supported, accept {:?}",
                connector,
                CONNECTORS_COMPATIBLE_FORMATS.keys()
            )))
        })?;

    validate_license(&connector)?;
    if connector != KAFKA_CONNECTOR {
        let res = match (&format_encode.format, &format_encode.row_encode) {
            (Format::Plain, Encode::Protobuf) | (Format::Plain, Encode::Avro) => {
                let mut options = WithOptions::try_from(format_encode.row_options())?;
                let (_, use_schema_registry) = get_schema_location(options.inner_mut())?;
                use_schema_registry
            }
            (Format::Debezium, Encode::Avro) => true,
            (_, _) => false,
        };
        if res {
            return Err(RwError::from(ProtocolError(format!(
                "The {} must be kafka when schema registry is used",
                UPSTREAM_SOURCE_KEY
            ))));
        }
    }

    let compatible_encodes = compatible_formats
        .get(&format_encode.format)
        .ok_or_else(|| {
            RwError::from(ProtocolError(format!(
                "connector {} does not support format {:?}",
                connector, format_encode.format
            )))
        })?;
    if !compatible_encodes.contains(&format_encode.row_encode) {
        return Err(RwError::from(ProtocolError(format!(
            "connector {} does not support format {:?} with encode {:?}",
            connector, format_encode.format, format_encode.row_encode
        ))));
    }

    if connector == POSTGRES_CDC_CONNECTOR || connector == CITUS_CDC_CONNECTOR {
        match props.get("slot.name") {
            None => {
                // Build a random slot name with UUID
                // e.g. "rw_cdc_f9a3567e6dd54bf5900444c8b1c03815"
                let uuid = uuid::Uuid::new_v4();
                props.insert("slot.name".into(), format!("rw_cdc_{}", uuid.simple()));
            }
            Some(slot_name) => {
                // please refer to
                // - https://github.com/debezium/debezium/blob/97956ce25b7612e3413d363658661896b7d2e0a2/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresConnectorConfig.java#L1179
                // - https://doxygen.postgresql.org/slot_8c.html#afac399f07320b9adfd2c599cf822aaa3
                if !slot_name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                    || slot_name.len() > 63
                {
                    return Err(RwError::from(ProtocolError(format!(
                        "Invalid replication slot name: {:?}. Valid replication slot name must contain only digits, lowercase characters and underscores with length <= 63",
                        slot_name
                    ))));
                }
            }
        }

        if !props.contains_key("schema.name") {
            // Default schema name is "public"
            props.insert("schema.name".into(), "public".into());
        }
        if !props.contains_key("publication.name") {
            // Build a random publication name with UUID to avoid conflicts between sources
            // e.g. "rw_publication_f9a3567e6dd54bf5900444c8b1c03815"
            let uuid = uuid::Uuid::new_v4();
            props.insert(
                "publication.name".into(),
                format!("rw_publication_{}", uuid.simple()),
            );
        }
        if !props.contains_key("publication.create.enable") {
            // Default auto create publication if doesn't exist
            props.insert("publication.create.enable".into(), "true".into());
        }
    }

    if connector == SQL_SERVER_CDC_CONNECTOR && !props.contains_key("schema.name") {
        // Default schema name is "dbo"
        props.insert("schema.name".into(), "dbo".into());
    }

    // Validate cdc.source.wait.streaming.start.timeout for all CDC connectors
    if (connector == MYSQL_CDC_CONNECTOR
        || connector == POSTGRES_CDC_CONNECTOR
        || connector == CITUS_CDC_CONNECTOR
        || connector == MONGODB_CDC_CONNECTOR
        || connector == SQL_SERVER_CDC_CONNECTOR
        || connector == ORACLE_CDC_CONNECTOR)
        && let Some(timeout_value) = props.get("cdc.source.wait.streaming.start.timeout")
        && timeout_value.parse::<u32>().is_err()
    {
        return Err(ErrorCode::InvalidConfigValue {
            config_entry: "cdc.source.wait.streaming.start.timeout".to_owned(),
            config_value: timeout_value.to_owned(),
        }
        .into());
    }

    // Validate debezium.max.queue.size for all CDC connectors
    if (connector == MYSQL_CDC_CONNECTOR
        || connector == POSTGRES_CDC_CONNECTOR
        || connector == CITUS_CDC_CONNECTOR
        || connector == MONGODB_CDC_CONNECTOR
        || connector == SQL_SERVER_CDC_CONNECTOR
        || connector == ORACLE_CDC_CONNECTOR)
        && let Some(queue_size_value) = props.get("debezium.max.queue.size")
        && queue_size_value.parse::<u32>().is_err()
    {
        return Err(ErrorCode::InvalidConfigValue {
            config_entry: "debezium.max.queue.size".to_owned(),
            config_value: queue_size_value.to_owned(),
        }
        .into());
    }

    // Validate debezium.heartbeat.interval.ms for Postgres and Oracle CDC: if configured, it must
    // be a positive integer.
    if (connector == POSTGRES_CDC_CONNECTOR || connector == ORACLE_CDC_CONNECTOR)
        && let Some(interval_value) = props.get("debezium.heartbeat.interval.ms")
        && !is_valid_heartbeat_interval(interval_value)
    {
        return Err(ErrorCode::InvalidConfigValue {
            config_entry: "debezium.heartbeat.interval.ms".to_owned(),
            config_value: interval_value.to_owned(),
        }
        .into());
    }

    Ok(())
}
