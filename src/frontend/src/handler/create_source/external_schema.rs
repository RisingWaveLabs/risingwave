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

//! bind columns from external schema

use risingwave_connector::source::ADBC_SNOWFLAKE_CONNECTOR;

use super::*;

mod json;
use json::*;
mod avro;
use avro::extract_avro_table_schema;
pub mod debezium;
pub mod iceberg;
use iceberg::extract_iceberg_columns;
mod protobuf;
use protobuf::extract_protobuf_table_schema;
pub mod nexmark;

/// Define a feature-gated function from a module.
///
/// This macro conditionally includes a module and re-exports a function from it.
/// When the feature is disabled, it generates a stub function that returns an error.
macro_rules! feature_gated_function {
    (
        mod $mod_name:ident,
        $feature_name:literal,
        async fn $func_name:ident ( $( $param_name:ident : $param_type:ty ),* $(,)? ) -> $ret_type:ty
    ) => {
        #[cfg(feature = $feature_name)]
        pub mod $mod_name;
        #[cfg(feature = $feature_name)]
        use $mod_name::$func_name;

        #[cfg(not(feature = $feature_name))]
        #[allow(unused_variables)]
        pub async fn $func_name( $( $param_name : $param_type ),* ) -> $ret_type {
            Err(anyhow::anyhow!(
                "Feature `{}` is not enabled at compile time. \
                Please enable it in `Cargo.toml` and rebuild.",
                $feature_name
            ))
        }
    };
}

feature_gated_function!(
    mod adbc_snowflake,
    "source-adbc_snowflake",
    async fn extract_adbc_snowflake_columns(
        with_properties: &WithOptionsSecResolved,
    ) -> anyhow::Result<Vec<ColumnCatalog>>
);

/// Resolves the schema of the source from external schema file.
/// See <https://docs.risingwave.com/sql/commands/sql-create-source> for more information.
///
/// Note: the returned schema strictly corresponds to the schema.
/// Other special columns like additional columns (`INCLUDE`), and `row_id` column are not included.
pub async fn bind_columns_from_source(
    session: &SessionImpl,
    format_encode: &FormatEncodeOptions,
    with_properties: Either<&WithOptions, &WithOptionsSecResolved>,
    create_source_type: CreateSourceType,
) -> Result<(Option<Vec<ColumnCatalog>>, StreamSourceInfo)> {
    let (columns_from_resolve_source, mut source_info) =
        if create_source_type == CreateSourceType::SharedCdc {
            bind_columns_from_source_for_cdc(session, format_encode)?
        } else {
            bind_columns_from_source_for_non_cdc(session, format_encode, with_properties).await?
        };
    if create_source_type.is_shared() {
        // Note: this field should be called is_shared. Check field doc for more details.
        source_info.cdc_source_job = true;
        source_info.is_distributed = create_source_type == CreateSourceType::SharedNonCdc;
    }
    Ok((columns_from_resolve_source, source_info))
}

async fn bind_columns_from_source_for_non_cdc(
    session: &SessionImpl,
    format_encode: &FormatEncodeOptions,
    with_properties: Either<&WithOptions, &WithOptionsSecResolved>,
) -> Result<(Option<Vec<ColumnCatalog>>, StreamSourceInfo)> {
    const MESSAGE_NAME_KEY: &str = "message";
    const KEY_MESSAGE_NAME_KEY: &str = "key.message";
    const NAME_STRATEGY_KEY: &str = "schema.registry.name.strategy";

    let options_with_secret = match with_properties {
        Either::Left(options) => {
            let (sec_resolve_props, connection_type, _) = resolve_connection_ref_and_secret_ref(
                options.clone(),
                session,
                Some(TelemetryDatabaseObject::Source),
            )?;
            if !SOURCE_ALLOWED_CONNECTION_CONNECTOR.contains(&connection_type) {
                return Err(RwError::from(ProtocolError(format!(
                    "connection type {:?} is not allowed, allowed types: {:?}",
                    connection_type, SOURCE_ALLOWED_CONNECTION_CONNECTOR
                ))));
            }

            sec_resolve_props
        }
        Either::Right(options_with_secret) => options_with_secret.clone(),
    };

    let is_kafka: bool = options_with_secret.is_kafka_connector();

    // todo: need to resolve connection ref for schema registry
    let (sec_resolve_props, connection_type, schema_registry_conn_ref) =
        resolve_connection_ref_and_secret_ref(
            WithOptions::try_from(format_encode.row_options())?,
            session,
            Some(TelemetryDatabaseObject::Source),
        )?;
    ensure_connection_type_allowed(connection_type, &SOURCE_ALLOWED_CONNECTION_SCHEMA_REGISTRY)?;

    let (format_encode_options, format_encode_secret_refs) = sec_resolve_props.into_parts();
    // Need real secret to access the schema registry
    let mut format_encode_options_to_consume = LocalSecretManager::global().fill_secrets(
        format_encode_options.clone(),
        format_encode_secret_refs.clone(),
    )?;

    fn get_key_message_name(options: &mut BTreeMap<String, String>) -> Option<String> {
        consume_string_from_options(options, KEY_MESSAGE_NAME_KEY)
            .map(|ele| Some(ele.0))
            .unwrap_or(None)
    }
    fn get_sr_name_strategy_check(
        options: &mut BTreeMap<String, String>,
        use_sr: bool,
    ) -> Result<Option<i32>> {
        let name_strategy = get_name_strategy_or_default(try_consume_string_from_options(
            options,
            NAME_STRATEGY_KEY,
        ))?;
        if !use_sr && name_strategy.is_some() {
            return Err(RwError::from(ProtocolError(
                "schema registry name strategy only works with schema registry enabled".to_owned(),
            )));
        }
        Ok(name_strategy)
    }

    let mut stream_source_info = StreamSourceInfo {
        format: format_to_prost(&format_encode.format) as i32,
        row_encode: row_encode_to_prost(&format_encode.row_encode) as i32,
        format_encode_options,
        format_encode_secret_refs,
        connection_id: schema_registry_conn_ref,
        ..Default::default()
    };

    if format_encode.format == Format::Debezium {
        try_consume_string_from_options(&mut format_encode_options_to_consume, DEBEZIUM_IGNORE_KEY);
    }

    let columns = match (&format_encode.format, &format_encode.row_encode) {
        (Format::Native, Encode::Native)
        | (Format::Plain, Encode::Bytes)
        | (Format::DebeziumMongo, Encode::Json) => None,
        (Format::Plain, Encode::Protobuf) | (Format::Upsert, Encode::Protobuf) => {
            let (row_schema_location, use_schema_registry) =
                get_schema_location(&mut format_encode_options_to_consume)?;
            let message_name = consume_string_from_options(
                &mut format_encode_options_to_consume,
                MESSAGE_NAME_KEY,
            )?;
            let name_strategy = get_sr_name_strategy_check(
                &mut format_encode_options_to_consume,
                use_schema_registry,
            )?;

            stream_source_info.use_schema_registry = use_schema_registry;
            stream_source_info
                .row_schema_location
                .clone_from(&row_schema_location.0);
            stream_source_info
                .proto_message_name
                .clone_from(&message_name.0);
            stream_source_info.key_message_name =
                get_key_message_name(&mut format_encode_options_to_consume);
            stream_source_info.name_strategy =
                name_strategy.unwrap_or(PbSchemaRegistryNameStrategy::Unspecified as i32);

            Some(
                extract_protobuf_table_schema(
                    &stream_source_info,
                    &options_with_secret,
                    &mut format_encode_options_to_consume,
                )
                .await?,
            )
        }
        (format @ (Format::Plain | Format::Upsert | Format::Debezium), Encode::Avro) => {
            if format_encode_options_to_consume.contains_key(PULSAR_SCHEMA_URL_KEY) {
                // Pulsar schema options are parsed by `SpecificParserConfig` and remain in the
                // catalog format options. They deliberately do not populate the legacy
                // Confluent-oriented `row_schema_location` fields.
            } else if format_encode_options_to_consume
                .remove(AWS_GLUE_SCHEMA_ARN_KEY)
                .is_none()
            {
                // Legacy logic that assumes either `schema.location` or confluent `schema.registry`.
                // The handling of newly added aws glue is centralized in `connector::parser`.
                // TODO(xiangjinwu): move these option parsing to `connector::parser` as well.

                let (row_schema_location, use_schema_registry) =
                    get_schema_location(&mut format_encode_options_to_consume)?;

                if matches!(format, Format::Debezium) && !use_schema_registry {
                    return Err(RwError::from(ProtocolError(
                        "schema location for DEBEZIUM_AVRO row format is not supported".to_owned(),
                    )));
                }

                let message_name = try_consume_string_from_options(
                    &mut format_encode_options_to_consume,
                    MESSAGE_NAME_KEY,
                );
                let name_strategy = get_sr_name_strategy_check(
                    &mut format_encode_options_to_consume,
                    use_schema_registry,
                )?;

                stream_source_info.use_schema_registry = use_schema_registry;
                stream_source_info
                    .row_schema_location
                    .clone_from(&row_schema_location.0);
                stream_source_info.proto_message_name =
                    message_name.unwrap_or(AstString("".into())).0;
                stream_source_info.key_message_name =
                    get_key_message_name(&mut format_encode_options_to_consume);
                stream_source_info.name_strategy =
                    name_strategy.unwrap_or(PbSchemaRegistryNameStrategy::Unspecified as i32);
            }

            Some(
                extract_avro_table_schema(
                    &stream_source_info,
                    &options_with_secret,
                    &mut format_encode_options_to_consume,
                    matches!(format, Format::Debezium),
                )
                .await?,
            )
        }
        (Format::Plain, Encode::Csv) => {
            let chars =
                consume_string_from_options(&mut format_encode_options_to_consume, "delimiter")?.0;
            let delimiter = get_delimiter(chars.as_str()).context("failed to parse delimiter")?;
            let has_header = try_consume_string_from_options(
                &mut format_encode_options_to_consume,
                "without_header",
            )
            .map(|s| s.0 == "false")
            .unwrap_or(true);

            if is_kafka && has_header {
                return Err(RwError::from(ProtocolError(
                    "CSV HEADER is not supported when creating table with Kafka connector"
                        .to_owned(),
                )));
            }

            stream_source_info.csv_delimiter = delimiter as i32;
            stream_source_info.csv_has_header = has_header;

            None
        }
        // For parquet format, this step is implemented in parquet parser.
        (Format::Plain, Encode::Parquet) => None,
        (
            Format::Plain | Format::Upsert | Format::Maxwell | Format::Canal | Format::Debezium,
            Encode::Json,
        ) => {
            if matches!(
                format_encode.format,
                Format::Plain | Format::Upsert | Format::Debezium
            ) {
                // Parse the value but throw it away.
                // It would be too late to report error in `SpecificParserConfig::new`,
                // which leads to recovery loop.
                // TODO: rely on SpecificParserConfig::new to validate, like Avro
                if let Some(value) =
                    format_encode_options_to_consume.get(TimestamptzHandling::OPTION_KEY)
                {
                    TimestamptzHandling::from_options(value)
                        .map_err(|err| InvalidInputSyntax(err.message))?;
                }
                try_consume_string_from_options(
                    &mut format_encode_options_to_consume,
                    TimestamptzHandling::OPTION_KEY,
                );
            }

            let schema_config = get_json_schema_location(&mut format_encode_options_to_consume)?;
            stream_source_info.use_schema_registry =
                json_schema_infer_use_schema_registry(&schema_config);

            extract_json_table_schema(
                &schema_config,
                &options_with_secret,
                &mut format_encode_options_to_consume,
            )
            .await?
        }
        (Format::None, Encode::None) => {
            if options_with_secret.is_iceberg_connector() {
                Some(
                    extract_iceberg_columns(&options_with_secret)
                        .await
                        .map_err(|err| ProtocolError(err.to_report_string()))?,
                )
            } else if options_with_secret
                .get(UPSTREAM_SOURCE_KEY)
                .is_some_and(|s| s.eq_ignore_ascii_case(ADBC_SNOWFLAKE_CONNECTOR))
            {
                Some(
                    extract_adbc_snowflake_columns(&options_with_secret)
                        .await
                        .map_err(|err| ProtocolError(err.to_report_string()))?,
                )
            } else {
                None
            }
        }
        (format, encoding) => {
            return Err(RwError::from(ProtocolError(format!(
                "Unknown combination {:?} {:?}",
                format, encoding
            ))));
        }
    };

    if !format_encode_options_to_consume.is_empty() {
        let err_string = format!(
            "Get unknown format_encode_options for {:?} {:?}: {}",
            format_encode.format,
            format_encode.row_encode,
            format_encode_options_to_consume
                .keys()
                .cloned()
                .collect::<Vec<String>>()
                .join(","),
        );
        session.notice_to_user(err_string);
    }
    Ok((columns, stream_source_info))
}

fn bind_columns_from_source_for_cdc(
    session: &SessionImpl,
    format_encode: &FormatEncodeOptions,
) -> Result<(Option<Vec<ColumnCatalog>>, StreamSourceInfo)> {
    let with_options = WithOptions::try_from(format_encode.row_options())?;
    if !with_options.connection_ref().is_empty() {
        return Err(RwError::from(NotSupported(
            "CDC connector does not support connection ref yet".to_owned(),
            "Explicitly specify the connection in WITH clause".to_owned(),
        )));
    }
    let (format_encode_options, format_encode_secret_refs) =
        resolve_secret_ref_in_with_options(with_options, session)?.into_parts();

    // Need real secret to access the schema registry
    let mut format_encode_options_to_consume = LocalSecretManager::global().fill_secrets(
        format_encode_options.clone(),
        format_encode_secret_refs.clone(),
    )?;

    match (&format_encode.format, &format_encode.row_encode) {
        (Format::Plain, Encode::Json) => (),
        (format, encoding) => {
            // Note: parser will also check this. Just be extra safe here
            return Err(RwError::from(ProtocolError(format!(
                "Row format for CDC connectors should be either omitted or set to `FORMAT PLAIN ENCODE JSON`, got: {:?} {:?}",
                format, encoding
            ))));
        }
    };

    let columns = ColumnCatalog::debezium_cdc_source_cols().to_vec();
    let schema_config = get_json_schema_location(&mut format_encode_options_to_consume)?;

    let stream_source_info = StreamSourceInfo {
        format: format_to_prost(&format_encode.format) as i32,
        row_encode: row_encode_to_prost(&format_encode.row_encode) as i32,
        format_encode_options,
        use_schema_registry: json_schema_infer_use_schema_registry(&schema_config),
        cdc_source_job: true,
        is_distributed: false,
        format_encode_secret_refs,
        ..Default::default()
    };
    if !format_encode_options_to_consume.is_empty() {
        let err_string = format!(
            "Get unknown format_encode_options for {:?} {:?}: {}",
            format_encode.format,
            format_encode.row_encode,
            format_encode_options_to_consume
                .keys()
                .cloned()
                .collect::<Vec<String>>()
                .join(","),
        );
        session.notice_to_user(err_string);
    }
    Ok((Some(columns), stream_source_info))
}

fn format_to_prost(format: &Format) -> FormatType {
    match format {
        Format::Native => FormatType::Native,
        Format::Plain => FormatType::Plain,
        Format::Upsert => FormatType::Upsert,
        Format::Debezium => FormatType::Debezium,
        Format::DebeziumMongo => FormatType::DebeziumMongo,
        Format::Maxwell => FormatType::Maxwell,
        Format::Canal => FormatType::Canal,
        Format::None => FormatType::None,
    }
}
fn row_encode_to_prost(row_encode: &Encode) -> EncodeType {
    match row_encode {
        Encode::Native => EncodeType::Native,
        Encode::Json => EncodeType::Json,
        Encode::Avro => EncodeType::Avro,
        Encode::Protobuf => EncodeType::Protobuf,
        Encode::Csv => EncodeType::Csv,
        Encode::Bytes => EncodeType::Bytes,
        Encode::Template => EncodeType::Template,
        Encode::Parquet => EncodeType::Parquet,
        Encode::None => EncodeType::None,
        Encode::Text => EncodeType::Text,
    }
}

pub fn get_schema_location(
    format_encode_options: &mut BTreeMap<String, String>,
) -> Result<(AstString, bool)> {
    let schema_location = try_consume_string_from_options(format_encode_options, "schema.location");
    let schema_registry = try_consume_string_from_options(format_encode_options, "schema.registry");
    match (schema_location, schema_registry) {
        (None, None) => Err(RwError::from(ProtocolError(
            "missing either a schema location or a schema registry".to_owned(),
        ))),
        (None, Some(schema_registry)) => Ok((schema_registry, true)),
        (Some(schema_location), None) => Ok((schema_location, false)),
        (Some(_), Some(_)) => Err(RwError::from(ProtocolError(
            "only need either the schema location or the schema registry".to_owned(),
        ))),
    }
}

pub fn schema_has_schema_registry(schema: &FormatEncodeOptions) -> bool {
    match schema.row_encode {
        Encode::Avro | Encode::Protobuf => true,
        Encode::Json => {
            let mut options = WithOptions::try_from(schema.row_options()).unwrap();
            matches!(get_json_schema_location(options.inner_mut()), Ok(Some(_)))
        }
        _ => false,
    }
}

#[inline]
fn get_name_strategy_or_default(name_strategy: Option<AstString>) -> Result<Option<i32>> {
    match name_strategy {
        None => Ok(None),
        Some(name) => Ok(Some(name_strategy_from_str(name.0.as_str())
            .ok_or_else(|| RwError::from(ProtocolError(format!("\
            expect strategy name in topic_name_strategy, record_name_strategy and topic_record_name_strategy, but got {}", name))))? as i32)),
    }
}
