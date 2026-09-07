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

use std::collections::{BTreeMap, HashSet};

use risingwave_common::bail;
use risingwave_common::secret::LocalSecretManager;
use risingwave_connector_codec::decoder::avro::MapHandling;
use risingwave_pb::catalog::{PbSchemaRegistryNameStrategy, StreamSourceInfo};

use super::unified::json::BigintUnsignedHandlingMode;
use super::utils::get_kafka_topic;
use super::{DebeziumProps, TimeHandling, TimestampHandling, TimestamptzHandling};
use crate::connector_common::AwsAuthProps;
use crate::error::ConnectorResult;
use crate::parser::PROTOBUF_MESSAGES_AS_JSONB;
use crate::schema::AWS_GLUE_SCHEMA_ARN_KEY;
use crate::schema::schema_registry::SchemaRegistryConfig;
use crate::source::cdc::{CDC_MONGODB_STRONG_SCHEMA_KEY, ORACLE_CDC_CONNECTOR};
use crate::source::{SourceColumnDesc, SourceEncode, SourceFormat, extract_source_struct};
use crate::{WithOptionsSecResolved, WithPropertiesExt};

pub const PARQUET_CASE_INSENSITIVE_KEY: &str = "parquet.case_insensitive";

/// Note: this is created in `SourceReader::build_stream`
#[derive(Debug, Clone, Default)]
pub struct ParserConfig {
    pub common: CommonParserConfig,
    pub specific: SpecificParserConfig,
}

fn parse_parquet_case_insensitive_option(
    options_with_secret: &BTreeMap<String, String>,
) -> ConnectorResult<bool> {
    let value = options_with_secret.get(PARQUET_CASE_INSENSITIVE_KEY);
    let Some(value) = value else {
        return Ok(false);
    };
    let parsed = match value.trim().to_ascii_lowercase().parse::<bool>() {
        Ok(parsed) => parsed,
        Err(_) => bail!(
            "invalid value for {}, expect true or false",
            PARQUET_CASE_INSENSITIVE_KEY
        ),
    };
    Ok(parsed)
}

impl ParserConfig {
    pub fn get_config(self) -> (Vec<SourceColumnDesc>, SpecificParserConfig) {
        (self.common.rw_columns, self.specific)
    }
}

#[derive(Debug, Clone, Default)]
pub struct CommonParserConfig {
    /// Note: this is created by `SourceDescBuilder::builder`
    pub rw_columns: Vec<SourceColumnDesc>,
}

#[derive(Debug, Clone, Default)]
pub struct SpecificParserConfig {
    pub encoding_config: EncodingProperties,
    pub protocol_config: ProtocolProperties,
}

#[derive(Debug, Default, Clone)]
pub enum EncodingProperties {
    Avro(AvroProperties),
    Protobuf(ProtobufProperties),
    Csv(CsvProperties),
    Json(JsonProperties),
    MongoJson(MongoProperties),
    Bytes(BytesProperties),
    Parquet(ParquetProperties),
    Native,
    /// Encoding can't be specified because the source will determines it. Now only used in Iceberg.
    None,
    #[default]
    Unspecified,
}

#[derive(Debug, Default, Clone)]
pub enum ProtocolProperties {
    Debezium(DebeziumProps),
    DebeziumMongo,
    Maxwell,
    Canal,
    Plain,
    Upsert,
    Native,
    /// Protocol can't be specified because the source will determines it. Now only used in Iceberg.
    None,
    #[default]
    Unspecified,
}

impl SpecificParserConfig {
    // for test only
    pub const DEFAULT_PLAIN_JSON: SpecificParserConfig = SpecificParserConfig {
        encoding_config: EncodingProperties::Json(JsonProperties {
            use_schema_registry: false,
            timestamp_handling: None,
            timestamptz_handling: None,
            time_handling: None,
            bigint_unsigned_handling: None,
            handle_toast_columns: false,
            numeric_string_parsing: false,
        }),
        protocol_config: ProtocolProperties::Plain,
    };

    // The validity of (format, encode) is ensured by `extract_format_encode`
    pub fn new(
        info: &StreamSourceInfo,
        with_properties: &WithOptionsSecResolved,
    ) -> ConnectorResult<Self> {
        let info = info.clone();
        let source_struct = extract_source_struct(&info)?;
        let format_encode_options_with_secret = LocalSecretManager::global()
            .fill_secrets(info.format_encode_options, info.format_encode_secret_refs)?;
        let (options, secret_refs) = with_properties.clone().into_parts();
        // Make sure `with_properties` is no longer used by accident.
        // All reads shall go to `options_with_secret` instead.
        #[expect(unused_variables)]
        let with_properties = ();
        let options_with_secret =
            LocalSecretManager::global().fill_secrets(options, secret_refs)?;
        let format = source_struct.format;
        let encode = source_struct.encode;
        // this transformation is needed since there may be config for the protocol
        // in the future
        let protocol_config = match format {
            SourceFormat::Native => ProtocolProperties::Native,
            SourceFormat::None => ProtocolProperties::None,
            SourceFormat::Debezium => {
                let debezium_props = DebeziumProps::from(&format_encode_options_with_secret);
                ProtocolProperties::Debezium(debezium_props)
            }
            SourceFormat::DebeziumMongo => ProtocolProperties::DebeziumMongo,
            SourceFormat::Maxwell => ProtocolProperties::Maxwell,
            SourceFormat::Canal => ProtocolProperties::Canal,
            SourceFormat::Upsert => ProtocolProperties::Upsert,
            SourceFormat::Plain => ProtocolProperties::Plain,
            _ => unreachable!(),
        };

        let encoding_config = match (format, encode) {
            (SourceFormat::Plain, SourceEncode::Csv) => EncodingProperties::Csv(CsvProperties {
                delimiter: info.csv_delimiter as u8,
                has_header: info.csv_has_header,
            }),
            (SourceFormat::Plain, SourceEncode::Parquet) => {
                let case_insensitive = parse_parquet_case_insensitive_option(&options_with_secret)?;
                EncodingProperties::Parquet(ParquetProperties { case_insensitive })
            }
            (SourceFormat::Plain, SourceEncode::Avro)
            | (SourceFormat::Upsert, SourceEncode::Avro) => {
                let mut config = AvroProperties {
                    record_name: if info.proto_message_name.is_empty() {
                        None
                    } else {
                        Some(info.proto_message_name.clone())
                    },
                    key_record_name: info.key_message_name.clone(),
                    map_handling: MapHandling::from_options(&format_encode_options_with_secret)?,
                    ..Default::default()
                };
                config.schema_location = if let Some(schema_arn) =
                    format_encode_options_with_secret.get(AWS_GLUE_SCHEMA_ARN_KEY)
                {
                    risingwave_common::license::Feature::GlueSchemaRegistry
                        .check_available()
                        .map_err(anyhow::Error::from)?;
                    SchemaLocation::Glue {
                        schema_arn: schema_arn.clone(),
                        aws_auth_props: serde_json::from_value::<AwsAuthProps>(
                            serde_json::to_value(format_encode_options_with_secret.clone())
                                .unwrap(),
                        )
                        .map_err(|e| anyhow::anyhow!(e))?,
                        // The option `mock_config` is not public and we can break compatibility.
                        mock_config: format_encode_options_with_secret
                            .get("aws.glue.mock_config")
                            .cloned(),
                    }
                } else if info.use_schema_registry {
                    SchemaLocation::Confluent {
                        urls: info.row_schema_location.clone(),
                        client_config: SchemaRegistryConfig::from(
                            &format_encode_options_with_secret,
                        ),
                        name_strategy: PbSchemaRegistryNameStrategy::try_from(info.name_strategy)
                            .unwrap(),
                        topic: get_kafka_topic(&options_with_secret)?.clone(),
                    }
                } else {
                    SchemaLocation::File {
                        url: info.row_schema_location,
                        aws_auth_props: Some(
                            serde_json::from_value::<AwsAuthProps>(
                                serde_json::to_value(format_encode_options_with_secret).unwrap(),
                            )
                            .map_err(|e| anyhow::anyhow!(e))?,
                        ),
                    }
                };
                EncodingProperties::Avro(config)
            }
            (SourceFormat::Plain, SourceEncode::Protobuf)
            | (SourceFormat::Upsert, SourceEncode::Protobuf) => {
                if info.row_schema_location.is_empty() {
                    bail!("protobuf file location not provided");
                }
                let mut messages_as_jsonb = if let Some(messages_as_jsonb) =
                    format_encode_options_with_secret.get(PROTOBUF_MESSAGES_AS_JSONB)
                {
                    messages_as_jsonb.split(',').map(|s| s.to_owned()).collect()
                } else {
                    HashSet::new()
                };
                messages_as_jsonb.insert("google.protobuf.Any".to_owned());

                let mut config = ProtobufProperties {
                    message_name: info.proto_message_name.clone(),
                    key_message_name: info.key_message_name.clone(),
                    messages_as_jsonb,
                    ..Default::default()
                };
                config.schema_location = if info.use_schema_registry {
                    SchemaLocation::Confluent {
                        urls: info.row_schema_location.clone(),
                        client_config: SchemaRegistryConfig::from(
                            &format_encode_options_with_secret,
                        ),
                        name_strategy: PbSchemaRegistryNameStrategy::try_from(info.name_strategy)
                            .unwrap(),
                        topic: get_kafka_topic(&options_with_secret)?.clone(),
                    }
                } else {
                    SchemaLocation::File {
                        url: info.row_schema_location,
                        aws_auth_props: Some(
                            serde_json::from_value::<AwsAuthProps>(
                                serde_json::to_value(format_encode_options_with_secret).unwrap(),
                            )
                            .map_err(|e| anyhow::anyhow!(e))?,
                        ),
                    }
                };
                EncodingProperties::Protobuf(config)
            }
            (SourceFormat::Debezium, SourceEncode::Avro) => {
                EncodingProperties::Avro(AvroProperties {
                    record_name: if info.proto_message_name.is_empty() {
                        None
                    } else {
                        Some(info.proto_message_name.clone())
                    },
                    key_record_name: info.key_message_name.clone(),
                    schema_location: SchemaLocation::Confluent {
                        urls: info.row_schema_location.clone(),
                        client_config: SchemaRegistryConfig::from(
                            &format_encode_options_with_secret,
                        ),
                        name_strategy: PbSchemaRegistryNameStrategy::try_from(info.name_strategy)
                            .unwrap(),
                        topic: get_kafka_topic(&options_with_secret).unwrap().clone(),
                    },
                    ..Default::default()
                })
            }
            (
                SourceFormat::Plain
                | SourceFormat::Debezium
                | SourceFormat::Maxwell
                | SourceFormat::Canal
                | SourceFormat::Upsert,
                SourceEncode::Json,
            ) => EncodingProperties::Json(JsonProperties {
                use_schema_registry: info.use_schema_registry,
                timestamp_handling: None,
                timestamptz_handling: format_encode_options_with_secret
                    .get(TimestamptzHandling::OPTION_KEY)
                    .map(|value| TimestamptzHandling::from_options(value))
                    .transpose()?,
                time_handling: None,
                bigint_unsigned_handling: None,
                handle_toast_columns: false,
                numeric_string_parsing: options_with_secret
                    .get_connector()
                    .is_some_and(|connector| connector == ORACLE_CDC_CONNECTOR),
            }),
            (SourceFormat::DebeziumMongo, SourceEncode::Json) => {
                let props = MongoProperties::from(&format_encode_options_with_secret);
                EncodingProperties::MongoJson(props)
            }
            (SourceFormat::Plain, SourceEncode::Bytes) => {
                EncodingProperties::Bytes(BytesProperties { column_name: None })
            }
            (SourceFormat::Native, SourceEncode::Native) => EncodingProperties::Native,
            (SourceFormat::None, SourceEncode::None) => EncodingProperties::None,
            (format, encode) => {
                bail!("Unsupported format {:?} encode {:?}", format, encode);
            }
        };
        Ok(Self {
            encoding_config,
            protocol_config,
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct AvroProperties {
    pub schema_location: SchemaLocation,
    pub record_name: Option<String>,
    pub key_record_name: Option<String>,
    pub map_handling: Option<MapHandling>,
}

/// WIP: may cover protobuf and json schema later.
#[derive(Debug, Clone)]
pub enum SchemaLocation {
    /// Avsc from `https://`, `s3://` or `file://`.
    File {
        url: String,
        aws_auth_props: Option<AwsAuthProps>, // for s3
    },
    /// <https://docs.confluent.io/platform/current/schema-registry/index.html>
    Confluent {
        urls: String,
        client_config: SchemaRegistryConfig,
        name_strategy: PbSchemaRegistryNameStrategy,
        topic: String,
    },
    /// <https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html>
    Glue {
        schema_arn: String,
        aws_auth_props: AwsAuthProps,
        // When `Some(_)`, ignore AWS and load schemas from provided config
        mock_config: Option<String>,
    },
}

// TODO: `SpecificParserConfig` shall not `impl`/`derive` a `Default`
impl Default for SchemaLocation {
    fn default() -> Self {
        // backward compatible but undesired
        Self::File {
            url: Default::default(),
            aws_auth_props: None,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ProtobufProperties {
    pub schema_location: SchemaLocation,
    pub message_name: String,
    pub key_message_name: Option<String>,
    pub messages_as_jsonb: HashSet<String>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CsvProperties {
    pub delimiter: u8,
    pub has_header: bool,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ParquetProperties {
    pub case_insensitive: bool,
}

#[derive(Debug, Default, Clone)]
pub struct JsonProperties {
    pub use_schema_registry: bool,
    pub timestamp_handling: Option<TimestampHandling>,
    pub timestamptz_handling: Option<TimestamptzHandling>,
    pub time_handling: Option<TimeHandling>,
    pub bigint_unsigned_handling: Option<BigintUnsignedHandlingMode>,
    pub handle_toast_columns: bool,
    /// Whether the Debezium JSON decoder may parse JSON strings as numeric values.
    ///
    /// This is currently enabled for Oracle CDC because Debezium serializes all Oracle `NUMBER`
    /// values as strings when `decimal.handling.mode` is set to `string`.
    pub numeric_string_parsing: bool,
}

#[derive(Debug, Default, Clone)]
pub struct BytesProperties {
    pub column_name: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct MongoProperties {
    pub strong_schema: bool,
}

impl MongoProperties {
    pub fn new(strong_schema: bool) -> Self {
        Self { strong_schema }
    }
}
impl From<&BTreeMap<String, String>> for MongoProperties {
    fn from(config: &BTreeMap<String, String>) -> Self {
        let strong_schema = config
            .get(CDC_MONGODB_STRONG_SCHEMA_KEY)
            .is_some_and(|k| k.eq_ignore_ascii_case("true"));
        Self { strong_schema }
    }
}
