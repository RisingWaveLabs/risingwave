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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;
use std::sync::LazyLock;

use anyhow::{Context, anyhow};
use either::Either;
use external_schema::debezium::extract_debezium_avro_table_pk_columns;
use external_schema::nexmark::check_nexmark_schema;
use itertools::Itertools;
use maplit::{convert_args, hashmap, hashset};
use pgwire::pg_response::{PgResponse, StatementType};
use rand::Rng;
use risingwave_common::array::arrow::{IcebergArrowConvert, arrow_schema_iceberg};
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{
    ColumnCatalog, ColumnDesc, ColumnId, INITIAL_SOURCE_VERSION_ID, KAFKA_TIMESTAMP_COLUMN_NAME,
    ROW_ID_COLUMN_NAME, TableId, debug_assert_column_ids_distinct,
};
use risingwave_common::license::Feature;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::parser::additional_columns::{
    build_additional_column_desc, get_supported_additional_columns,
    source_add_partition_offset_cols,
};
use risingwave_connector::parser::{
    AvroParserConfig, DEBEZIUM_IGNORE_KEY, DebeziumAvroParserConfig, ProtobufParserConfig,
    SchemaLocation, SpecificParserConfig, TimestamptzHandling,
    fetch_json_schema_and_map_to_columns,
};
use risingwave_connector::schema::AWS_GLUE_SCHEMA_ARN_KEY;
use risingwave_connector::schema::schema_registry::{
    SCHEMA_REGISTRY_BACKOFF_DURATION_KEY, SCHEMA_REGISTRY_BACKOFF_FACTOR_KEY,
    SCHEMA_REGISTRY_CA_PEM_PATH, SCHEMA_REGISTRY_MAX_DELAY_KEY, SCHEMA_REGISTRY_PASSWORD,
    SCHEMA_REGISTRY_RETRIES_MAX_KEY, SCHEMA_REGISTRY_USERNAME, SchemaRegistryConfig,
    name_strategy_from_str,
};
use risingwave_connector::source::cdc::{
    CDC_MONGODB_STRONG_SCHEMA_KEY, CDC_SHARING_MODE_KEY, CDC_SNAPSHOT_BACKFILL,
    CDC_SNAPSHOT_MODE_KEY, CDC_TRANSACTIONAL_KEY, CDC_WAIT_FOR_STREAMING_START_TIMEOUT,
    CITUS_CDC_CONNECTOR, MONGODB_CDC_CONNECTOR, MYSQL_CDC_CONNECTOR, ORACLE_CDC_CONNECTOR,
    POSTGRES_CDC_CONNECTOR, SQL_SERVER_CDC_CONNECTOR,
};
use risingwave_connector::source::datagen::DATAGEN_CONNECTOR;
use risingwave_connector::source::iceberg::ICEBERG_CONNECTOR;
use risingwave_connector::source::nexmark::source::{EventType, get_event_data_types_with_names};
use risingwave_connector::source::test_source::TEST_CONNECTOR;
pub use risingwave_connector::source::{
    ADBC_SNOWFLAKE_CONNECTOR, UPSTREAM_SOURCE_KEY, WEBHOOK_CONNECTOR,
};
use risingwave_connector::source::{
    AZBLOB_CONNECTOR, ConnectorProperties, GCS_CONNECTOR, GOOGLE_PUBSUB_CONNECTOR, KAFKA_CONNECTOR,
    KINESIS_CONNECTOR, LEGACY_S3_CONNECTOR, MQTT_CONNECTOR, NATS_CONNECTOR, NEXMARK_CONNECTOR,
    OPENDAL_S3_CONNECTOR, POSIX_FS_CONNECTOR, PULSAR_CONNECTOR,
};
use risingwave_connector::{AUTO_SCHEMA_CHANGE_KEY, WithPropertiesExt};
use risingwave_pb::catalog::connection_params::PbConnectionType;
use risingwave_pb::catalog::{PbSchemaRegistryNameStrategy, StreamSourceInfo, WatermarkDesc};
use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;
use risingwave_pb::plan_common::source_refresh_mode::{RefreshMode, SourceRefreshModeStreaming};
use risingwave_pb::plan_common::{EncodeType, FormatType, SourceRefreshMode};
use risingwave_pb::stream_plan::PbStreamFragmentGraph;
use risingwave_pb::telemetry::TelemetryDatabaseObject;
use risingwave_sqlparser::ast::{
    AstString, ColumnDef, ColumnOption, CreateSourceStatement, Encode, Format, FormatEncodeOptions,
    ObjectName, SourceWatermark, SqlOptionValue, TableConstraint, Value, get_delimiter,
};
use risingwave_sqlparser::parser::{IncludeOption, IncludeOptionItem};
use thiserror_ext::AsReport;

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::CatalogError;
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::ErrorCode::{self, Deprecated, InvalidInputSyntax, NotSupported, ProtocolError};
use crate::error::{Result, RwError};
use crate::expr::{Expr, ExprRewriter, SessionTimezone};
use crate::handler::HandlerArgs;
use crate::handler::create_table::{
    ColumnIdGenerator, bind_pk_and_row_id_on_relation, bind_sql_column_constraints,
    bind_sql_columns, bind_sql_pk_names, bind_table_constraints,
};
use crate::handler::util::{
    SourceSchemaCompatExt, check_connector_match_connection_type, ensure_connection_type_allowed,
    ensure_local_fs_connector_allowed, ensure_oracle_cdc_connector_allowed,
};
use crate::optimizer::plan_node::generic::SourceNodeKind;
use crate::optimizer::plan_node::{BackfillType, LogicalSource, ToStream, ToStreamContext};
use crate::session::SessionImpl;
use crate::session::current::notice_to_user;
use crate::utils::{
    OverwriteOptions, resolve_connection_ref_and_secret_ref, resolve_privatelink_in_with_option,
    resolve_secret_ref_in_with_options, resolve_source_refresh_mode_in_with_option,
};
use crate::{OptimizerContext, WithOptions, WithOptionsSecResolved, bind_data_type, build_graph};

mod external_schema;
pub use external_schema::{
    bind_columns_from_source, get_schema_location, schema_has_schema_registry,
};
mod validate;
pub use validate::validate_compatibility;
use validate::{SOURCE_ALLOWED_CONNECTION_CONNECTOR, SOURCE_ALLOWED_CONNECTION_SCHEMA_REGISTRY};
mod additional_column;
use additional_column::check_and_add_timestamp_column;
pub use additional_column::handle_addition_columns;
use risingwave_common::catalog::ICEBERG_SOURCE_PREFIX;
use risingwave_common::id::SourceId;

use crate::stream_fragmenter::GraphJobType;

pub(crate) fn is_valid_heartbeat_interval(interval: &str) -> bool {
    interval.parse::<i64>().is_ok_and(|interval| interval > 0)
}

fn non_generated_sql_columns(columns: &[ColumnDef]) -> Vec<ColumnDef> {
    columns
        .iter()
        .filter(|c| !c.is_generated())
        .cloned()
        .collect()
}

fn try_consume_string_from_options(
    format_encode_options: &mut BTreeMap<String, String>,
    key: &str,
) -> Option<AstString> {
    format_encode_options.remove(key).map(AstString)
}

fn try_consume_schema_registry_config_from_options(
    format_encode_options: &mut BTreeMap<String, String>,
) {
    [
        SCHEMA_REGISTRY_USERNAME,
        SCHEMA_REGISTRY_PASSWORD,
        SCHEMA_REGISTRY_CA_PEM_PATH,
        SCHEMA_REGISTRY_MAX_DELAY_KEY,
        SCHEMA_REGISTRY_BACKOFF_DURATION_KEY,
        SCHEMA_REGISTRY_BACKOFF_FACTOR_KEY,
        SCHEMA_REGISTRY_RETRIES_MAX_KEY,
    ]
    .iter()
    .for_each(|key| {
        try_consume_string_from_options(format_encode_options, key);
    });
}

fn consume_string_from_options(
    format_encode_options: &mut BTreeMap<String, String>,
    key: &str,
) -> Result<AstString> {
    try_consume_string_from_options(format_encode_options, key).ok_or(RwError::from(ProtocolError(
        format!("missing field {} in options", key),
    )))
}

fn consume_aws_config_from_options(format_encode_options: &mut BTreeMap<String, String>) {
    format_encode_options.retain(|key, _| !key.starts_with("aws."))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateSourceType {
    SharedCdc,
    /// e.g., shared Kafka source
    SharedNonCdc,
    NonShared,
    /// create table with connector
    Table,
}

impl CreateSourceType {
    /// Note: shouldn't be used for `ALTER SOURCE`, since session variables should not affect existing source. We should respect the original type instead.
    pub fn for_newly_created(
        session: &SessionImpl,
        with_properties: &impl WithPropertiesExt,
    ) -> Self {
        if with_properties.is_shareable_cdc_connector() {
            CreateSourceType::SharedCdc
        } else if with_properties.is_shareable_non_cdc_connector()
            && session
                .env()
                .streaming_config()
                .developer
                .enable_shared_source
            && session.config().streaming_use_shared_source()
        {
            CreateSourceType::SharedNonCdc
        } else {
            CreateSourceType::NonShared
        }
    }

    pub fn for_replace(catalog: &SourceCatalog) -> Self {
        if !catalog.info.is_shared() {
            CreateSourceType::NonShared
        } else if catalog.with_properties.is_shareable_cdc_connector() {
            CreateSourceType::SharedCdc
        } else {
            CreateSourceType::SharedNonCdc
        }
    }

    pub fn is_shared(&self) -> bool {
        matches!(
            self,
            CreateSourceType::SharedCdc | CreateSourceType::SharedNonCdc
        )
    }
}

/// Bind columns from both source and sql defined.
pub(crate) fn bind_all_columns(
    format_encode: &FormatEncodeOptions,
    cols_from_source: Option<Vec<ColumnCatalog>>,
    cols_from_sql: Vec<ColumnCatalog>,
    col_defs_from_sql: &[ColumnDef],
    wildcard_idx: Option<usize>,
    sql_column_strategy: SqlColumnStrategy,
) -> Result<Vec<ColumnCatalog>> {
    if let Some(cols_from_source) = cols_from_source {
        // Need to check `col_defs` to see if a column is generated, as we haven't bind the
        // `GeneratedColumnDesc` in `ColumnCatalog` yet.
        let generated_cols_from_sql = cols_from_sql
            .iter()
            .filter(|c| {
                col_defs_from_sql
                    .iter()
                    .find(|d| d.name.real_value() == c.name())
                    .unwrap()
                    .is_generated()
            })
            .cloned()
            .collect_vec();

        match sql_column_strategy {
            // Ignore `cols_from_source`, follow `cols_from_sql` without checking.
            SqlColumnStrategy::FollowUnchecked => {
                assert!(
                    wildcard_idx.is_none(),
                    "wildcard still exists while strategy is Follows, not correctly purified?"
                );
                return Ok(cols_from_sql);
            }

            // Will merge `generated_cols_from_sql` into `cols_from_source`.
            SqlColumnStrategy::Ignore => {}

            SqlColumnStrategy::FollowChecked => {
                let has_regular_cols_from_sql =
                    generated_cols_from_sql.len() != cols_from_sql.len();

                if has_regular_cols_from_sql {
                    if wildcard_idx.is_some() {
                        // (*, normal_column INT)
                        return Err(RwError::from(NotSupported(
                            "When there's a wildcard (\"*\"), \
                             only generated columns are allowed in user-defined schema from SQL"
                                .to_owned(),
                            "Remove the non-generated columns".to_owned(),
                        )));
                    } else {
                        // (normal_column INT)
                        // Follow `cols_from_sql` with name & type checking.
                        for col in &cols_from_sql {
                            if generated_cols_from_sql.contains(col) {
                                continue;
                            }
                            let Some(col_from_source) =
                                cols_from_source.iter().find(|c| c.name() == col.name())
                            else {
                                return Err(RwError::from(ProtocolError(format!(
                                    "Column \"{}\" is defined in SQL but not found in the source",
                                    col.name()
                                ))));
                            };

                            if col_from_source.data_type() != col.data_type() {
                                return Err(RwError::from(ProtocolError(format!(
                                    "Data type mismatch for column \"{}\". \
                                     Defined in SQL as \"{}\", but found in the source as \"{}\"",
                                    col.name(),
                                    col.data_type(),
                                    col_from_source.data_type()
                                ))));
                            }
                        }
                        return Ok(cols_from_sql);
                    }
                } else {
                    if wildcard_idx.is_some() {
                        // (*)
                        // (*, generated_column INT)
                        // Good, expand the wildcard later.
                    } else {
                        // ()
                        // (generated_column INT)
                        // Interpreted as if there's a wildcard for backward compatibility.
                        // TODO: the behavior is not that consistent, making it impossible to ingest no
                        //       columns from the source (though not useful in practice). Currently we
                        //       just notice the user but we may want to interpret it as empty columns
                        //       in the future.
                        notice_to_user("\
                            Neither wildcard (\"*\") nor regular (non-generated) columns appear in the user-defined schema from SQL. \
                            For backward compatibility, all columns from the source will be included at the beginning. \
                            For clarity, consider adding a wildcard (\"*\") to indicate where the columns from the source should be included, \
                            or specifying the columns you want to include from the source.
                        ");
                    }
                }
            }
        }

        // In some cases the wildcard may be absent:
        // - plan based on a purified SQL
        // - interpret `()` as `(*)` for backward compatibility (see notice above)
        // Default to 0 to expand the wildcard at the beginning.
        let wildcard_idx = wildcard_idx.unwrap_or(0).min(generated_cols_from_sql.len());

        // Merge `generated_cols_from_sql` with `cols_from_source`.
        let mut merged_cols = generated_cols_from_sql;
        let merged_cols_r = merged_cols.split_off(wildcard_idx);
        merged_cols.extend(cols_from_source);
        merged_cols.extend(merged_cols_r);

        Ok(merged_cols)
    } else {
        if wildcard_idx.is_some() {
            return Err(RwError::from(NotSupported(
                "Wildcard in user-defined schema is only allowed when there exists columns from external schema".to_owned(),
                "Remove the wildcard or use a source with external schema".to_owned(),
            )));
        }
        let non_generated_sql_defined_columns = non_generated_sql_columns(col_defs_from_sql);

        match (&format_encode.format, &format_encode.row_encode) {
            (Format::DebeziumMongo, Encode::Json) => {
                let strong_schema = format_encode
                    .row_options
                    .iter()
                    .find(|k| k.name.real_value().to_lowercase() == CDC_MONGODB_STRONG_SCHEMA_KEY)
                    .map(|k| matches!(k.value, SqlOptionValue::Value(Value::Boolean(true))))
                    .unwrap_or(false);

                // strong schema requires a '_id' column at the first position with a specific type
                if strong_schema {
                    let (_, id_column) = non_generated_sql_defined_columns
                        .iter()
                        .enumerate()
                        .find(|(idx, col)| *idx == 0 && col.name.real_value() == "_id")
                        .ok_or_else(|| {
                            RwError::from(ProtocolError(
                                "The `_id` column of the source with row format DebeziumMongoJson must be defined as the first column in SQL".to_owned(),
                            ))
                        })?;

                    let id_data_type = bind_data_type(id_column.data_type.as_ref().unwrap())?;
                    if !matches!(
                        id_data_type,
                        DataType::Varchar | DataType::Int32 | DataType::Int64 | DataType::Jsonb
                    ) {
                        return Err(RwError::from(ProtocolError(
                            "the `_id` column of the source with row format DebeziumMongoJson must be [Jsonb | Varchar | Int32 | Int64]".to_owned(),
                        )));
                    }

                    let mut columns = Vec::with_capacity(non_generated_sql_defined_columns.len());
                    columns.push(
                        // id column
                        ColumnCatalog {
                            column_desc: ColumnDesc::named("_id", 0.into(), id_data_type),
                            is_hidden: false,
                        },
                    );

                    // bind rest of the columns
                    for (idx, col) in non_generated_sql_defined_columns
                        .into_iter()
                        // skip the first column
                        .skip(1)
                        .enumerate()
                    {
                        columns.push(ColumnCatalog {
                            column_desc: ColumnDesc::named(
                                col.name.real_value(),
                                (idx as i32).into(),
                                bind_data_type(col.data_type.as_ref().unwrap())?,
                            ),
                            is_hidden: false,
                        });
                    }

                    return Ok(columns);
                }

                let mut columns = vec![
                    ColumnCatalog {
                        column_desc: ColumnDesc::named("_id", 0.into(), DataType::Varchar),
                        is_hidden: false,
                    },
                    ColumnCatalog {
                        column_desc: ColumnDesc::named("payload", 0.into(), DataType::Jsonb),
                        is_hidden: false,
                    },
                ];

                if non_generated_sql_defined_columns.len() != 2
                    || non_generated_sql_defined_columns[0].name.real_value() != columns[0].name()
                    || non_generated_sql_defined_columns[1].name.real_value() != columns[1].name()
                {
                    return Err(RwError::from(ProtocolError(
                        "the not generated columns of the source with row format DebeziumMongoJson
        must be (_id [Jsonb | Varchar | Int32 | Int64], payload jsonb)."
                            .to_owned(),
                    )));
                }
                // ok to unwrap since it was checked at `bind_sql_columns`
                let key_data_type = bind_data_type(
                    non_generated_sql_defined_columns[0]
                        .data_type
                        .as_ref()
                        .unwrap(),
                )?;
                match key_data_type {
                    DataType::Jsonb | DataType::Varchar | DataType::Int32 | DataType::Int64 => {
                        columns[0].column_desc.data_type = key_data_type;
                    }
                    _ => {
                        return Err(RwError::from(ProtocolError(
                            "the `_id` column of the source with row format DebeziumMongoJson
        must be [Jsonb | Varchar | Int32 | Int64]"
                                .to_owned(),
                        )));
                    }
                }

                // ok to unwrap since it was checked at `bind_sql_columns`
                let value_data_type = bind_data_type(
                    non_generated_sql_defined_columns[1]
                        .data_type
                        .as_ref()
                        .unwrap(),
                )?;
                if !matches!(value_data_type, DataType::Jsonb) {
                    return Err(RwError::from(ProtocolError(
                        "the `payload` column of the source with row format DebeziumMongoJson
        must be Jsonb datatype"
                            .to_owned(),
                    )));
                }
                Ok(columns)
            }
            (Format::Plain, Encode::Bytes) => {
                let err = Err(RwError::from(ProtocolError(
                    "ENCODE BYTES only accepts one BYTEA type column".to_owned(),
                )));
                if non_generated_sql_defined_columns.len() == 1 {
                    // ok to unwrap `data_type`` since it was checked at `bind_sql_columns`
                    let col_data_type = bind_data_type(
                        non_generated_sql_defined_columns[0]
                            .data_type
                            .as_ref()
                            .unwrap(),
                    )?;
                    if col_data_type == DataType::Bytea {
                        Ok(cols_from_sql)
                    } else {
                        err
                    }
                } else {
                    err
                }
            }
            (_, _) => Ok(cols_from_sql),
        }
    }
}

/// TODO: perhaps put the hint in notice is better. The error message format might be not that reliable.
fn hint_format_encode(format_encode: &FormatEncodeOptions) -> String {
    format!(
        r#"Hint: For FORMAT {0} ENCODE {1}, INCLUDE KEY must be specified and the key column must be used as primary key.
example:
    CREATE TABLE <table_name> ( PRIMARY KEY ([rw_key | <key_name>]) )
    INCLUDE KEY [AS <key_name>]
    WITH (...)
    FORMAT {0} ENCODE {1}{2}
"#,
        format_encode.format,
        format_encode.row_encode,
        if format_encode.row_encode == Encode::Json || format_encode.row_encode == Encode::Bytes {
            "".to_owned()
        } else {
            " (...)".to_owned()
        }
    )
}

/// Bind column from source. Add key column to table columns if necessary.
/// Return `pk_names`.
pub(crate) async fn bind_source_pk(
    format_encode: &FormatEncodeOptions,
    source_info: &StreamSourceInfo,
    columns: &mut [ColumnCatalog],
    sql_defined_pk_names: Vec<String>,
    with_properties: &WithOptionsSecResolved,
) -> Result<Vec<String>> {
    let sql_defined_pk = !sql_defined_pk_names.is_empty();
    let include_key_column_name: Option<String> = {
        // iter columns to check if contains additional columns from key part
        // return the key column names if exists
        columns.iter().find_map(|catalog| {
            if matches!(
                catalog.column_desc.additional_column.column_type,
                Some(AdditionalColumnType::Key(_))
            ) {
                Some(catalog.name().to_owned())
            } else {
                None
            }
        })
    };
    let additional_column_names = columns
        .iter()
        .filter_map(|col| {
            if col.column_desc.additional_column.column_type.is_some() {
                Some(col.name().to_owned())
            } else {
                None
            }
        })
        .collect_vec();

    let res = match (&format_encode.format, &format_encode.row_encode) {
        (Format::Native, Encode::Native) | (Format::None, Encode::None) | (Format::Plain, _) => {
            sql_defined_pk_names
        }

        // For all Upsert formats, we only accept one and only key column as primary key.
        // Additional KEY columns must be set in this case and must be primary key.
        (Format::Upsert, Encode::Json | Encode::Avro | Encode::Protobuf) => {
            if let Some(ref key_column_name) = include_key_column_name
                && sql_defined_pk
            {
                // pk is set. check if it's valid

                // the column name have been converted to real value in `handle_addition_columns`
                // so we don't ignore ascii case here
                if sql_defined_pk_names.len() != 1
                    || !key_column_name.eq(sql_defined_pk_names[0].as_str())
                {
                    return Err(RwError::from(ProtocolError(format!(
                        "Only \"{}\" can be used as primary key\n\n{}",
                        key_column_name,
                        hint_format_encode(format_encode)
                    ))));
                }
                sql_defined_pk_names
            } else {
                // pk not set, or even key not included
                return if let Some(include_key_column_name) = include_key_column_name {
                    Err(RwError::from(ProtocolError(format!(
                        "Primary key must be specified to {}\n\n{}",
                        include_key_column_name,
                        hint_format_encode(format_encode)
                    ))))
                } else {
                    Err(RwError::from(ProtocolError(format!(
                        "INCLUDE KEY clause not set\n\n{}",
                        hint_format_encode(format_encode)
                    ))))
                };
            }
        }

        (Format::Debezium, Encode::Json) => {
            if !additional_column_names.is_empty() {
                return Err(RwError::from(ProtocolError(format!(
                    "FORMAT DEBEZIUM forbids additional columns, but got {:?}",
                    additional_column_names
                ))));
            }
            if !sql_defined_pk {
                return Err(RwError::from(ProtocolError(
                    "Primary key must be specified when creating source with FORMAT DEBEZIUM."
                        .to_owned(),
                )));
            }
            sql_defined_pk_names
        }
        (Format::Debezium, Encode::Avro) => {
            if !additional_column_names.is_empty() {
                return Err(RwError::from(ProtocolError(format!(
                    "FORMAT DEBEZIUM forbids additional columns, but got {:?}",
                    additional_column_names
                ))));
            }
            if sql_defined_pk {
                sql_defined_pk_names
            } else {
                let pk_names =
                    extract_debezium_avro_table_pk_columns(source_info, with_properties).await?;
                // extract pk(s) from schema registry
                for pk_name in &pk_names {
                    columns
                        .iter()
                        .find(|c: &&ColumnCatalog| c.name().eq(pk_name))
                        .ok_or_else(|| {
                            RwError::from(ProtocolError(format!(
                                "avro's key column {} not exists in avro's row schema",
                                pk_name
                            )))
                        })?;
                }
                pk_names
            }
        }
        (Format::DebeziumMongo, Encode::Json) => {
            if sql_defined_pk {
                sql_defined_pk_names
            } else {
                vec!["_id".to_owned()]
            }
        }

        (Format::Maxwell, Encode::Json) => {
            if !additional_column_names.is_empty() {
                return Err(RwError::from(ProtocolError(format!(
                    "FORMAT MAXWELL forbids additional columns, but got {:?}",
                    additional_column_names
                ))));
            }
            if !sql_defined_pk {
                return Err(RwError::from(ProtocolError(
    "Primary key must be specified when creating source with FORMAT MAXWELL ENCODE JSON.".to_owned(),
    )));
            }
            sql_defined_pk_names
        }

        (Format::Canal, Encode::Json) => {
            if !additional_column_names.is_empty() {
                return Err(RwError::from(ProtocolError(format!(
                    "FORMAT CANAL forbids additional columns, but got {:?}",
                    additional_column_names
                ))));
            }
            if !sql_defined_pk {
                return Err(RwError::from(ProtocolError(
    "Primary key must be specified when creating source with FORMAT CANAL ENCODE JSON.".to_owned(),
    )));
            }
            sql_defined_pk_names
        }
        (format, encoding) => {
            return Err(RwError::from(ProtocolError(format!(
                "Unknown combination {:?} {:?}",
                format, encoding
            ))));
        }
    };
    Ok(res)
}

pub(super) fn bind_source_watermark(
    session: &SessionImpl,
    name: String,
    source_watermarks: Vec<SourceWatermark>,
    column_catalogs: &[ColumnCatalog],
) -> Result<Vec<WatermarkDesc>> {
    let mut binder = Binder::new_for_ddl(session);
    binder.bind_columns_to_context(name.clone(), column_catalogs)?;

    let mut session_tz = SessionTimezone::new(session.config().timezone());

    let watermark_descs = source_watermarks
        .into_iter()
        .map(|source_watermark| {
            let col_name = source_watermark.column.real_value();
            let watermark_idx = binder.get_column_binding_index(name.clone(), &col_name)?;

            let expr = binder.bind_expr(&source_watermark.expr)?;
            // Apply session timezone rewrite so that timestamptz +/- interval operations
            // are transformed into the timezone-aware variant (e.g. `subtract_with_time_zone`).
            // Without this, intervals containing days/months would fail at runtime.
            let expr = session_tz.rewrite_expr(expr);
            let watermark_col_type = column_catalogs[watermark_idx].data_type();
            let watermark_expr_type = &expr.return_type();
            if watermark_col_type != watermark_expr_type {
                Err(RwError::from(ErrorCode::BindError(
                    format!("The return value type of the watermark expression must be identical to the watermark column data type. Current data type of watermark return value: `{}`, column `{}`",watermark_expr_type, watermark_col_type),
                )))
            } else {
                let expr_proto = expr.to_expr_proto();
                Ok::<_, RwError>(WatermarkDesc {
                    watermark_idx: watermark_idx as u32,
                    expr: Some(expr_proto),
                    with_ttl: source_watermark.with_ttl,
                })
            }
        })
        .try_collect()?;
    Ok(watermark_descs)
}

/// Performs early stage checking in frontend to see if the schema of the given `columns` is
/// compatible with the connector extracted from the properties.
///
/// One should only call this function after all properties of all columns are resolved, like
/// generated column descriptors.
pub(super) fn check_format_encode(
    props: &WithOptionsSecResolved,
    row_id_index: Option<usize>,
    columns: &[ColumnCatalog],
) -> Result<()> {
    let Some(connector) = props.get_connector() else {
        return Ok(());
    };

    if connector == NEXMARK_CONNECTOR {
        check_nexmark_schema(props, row_id_index, columns)
    } else {
        Ok(())
    }
}

pub fn bind_connector_props(
    handler_args: &HandlerArgs,
    format_encode: &FormatEncodeOptions,
    is_create_source: bool,
) -> Result<(WithOptions, SourceRefreshMode)> {
    let mut with_properties = handler_args.with_options.clone().into_connector_props();
    if let Some(connector) = with_properties.get_connector() {
        ensure_oracle_cdc_connector_allowed(&handler_args.session, &connector)?;
    }
    validate_compatibility(format_encode, &mut with_properties)?;
    let refresh_mode = {
        let refresh_mode = resolve_source_refresh_mode_in_with_option(&mut with_properties)?;
        if is_create_source && refresh_mode.is_some() {
            return Err(RwError::from(ProtocolError(
                "`refresh_mode` only supported for CREATE TABLE".to_owned(),
            )));
        }

        refresh_mode.unwrap_or(SourceRefreshMode {
            refresh_mode: Some(RefreshMode::Streaming(SourceRefreshModeStreaming {})),
        })
    };

    let create_cdc_source_job = with_properties.is_shareable_cdc_connector();

    if !is_create_source && with_properties.is_shareable_cdc_connector() {
        return Err(RwError::from(ProtocolError(format!(
            "directly creating a CDC table for connector {} is no longer supported; \
             please `CREATE SOURCE` to create a shared CDC source first, \
             then `CREATE TABLE ... FROM <source> TABLE '<database>.<table>'`",
            with_properties.get_connector().unwrap(),
        ))));
    }
    if is_create_source && create_cdc_source_job {
        if let Some(value) = with_properties.get(AUTO_SCHEMA_CHANGE_KEY)
            && value.parse::<bool>().map_err(|_| {
                ErrorCode::InvalidInputSyntax(format!(
                    "invalid value of '{}' option",
                    AUTO_SCHEMA_CHANGE_KEY
                ))
            })?
        {
            Feature::CdcAutoSchemaChange.check_available()?;
        }

        // set connector to backfill mode
        with_properties.insert(CDC_SNAPSHOT_MODE_KEY.into(), CDC_SNAPSHOT_BACKFILL.into());
        // enable cdc sharing mode, which will capture all tables in the given `database.name`
        with_properties.insert(CDC_SHARING_MODE_KEY.into(), "true".into());
        // enable transactional cdc
        if with_properties.enable_transaction_metadata() {
            with_properties.insert(CDC_TRANSACTIONAL_KEY.into(), "true".into());
        }
        // Only set CDC_WAIT_FOR_STREAMING_START_TIMEOUT if not already specified by user.
        if !with_properties.contains_key(CDC_WAIT_FOR_STREAMING_START_TIMEOUT) {
            with_properties.insert(
                CDC_WAIT_FOR_STREAMING_START_TIMEOUT.into(),
                handler_args
                    .session
                    .config()
                    .cdc_source_wait_streaming_start_timeout()
                    .to_string(),
            );
        }
    }
    if with_properties.is_mysql_cdc_connector() {
        // Generate a random server id for mysql cdc source if needed
        // `server.id` (in the range from 1 to 2^32 - 1). This value MUST be unique across whole replication
        // group (that is, different from any other server id being used by any master or slave)
        with_properties
            .entry("server.id".to_owned())
            .or_insert(rand::rng().random_range(1..u32::MAX).to_string());
    }
    Ok((with_properties, refresh_mode))
}

fn must_wait_cdc_offset_before_report(with_properties: &WithOptions) -> bool {
    matches!(
        with_properties.get_connector().as_deref(),
        Some(MYSQL_CDC_CONNECTOR) | Some(SQL_SERVER_CDC_CONNECTOR)
    )
}

/// When the schema can be inferred from external system (like schema registry),
/// how to handle the regular columns (i.e., non-generated) defined in SQL?
pub enum SqlColumnStrategy {
    /// Follow all columns defined in SQL, ignore the columns from external system.
    /// This ensures that no accidental side effect will change the schema.
    ///
    /// This is the behavior when re-planning the target table of `SINK INTO`.
    FollowUnchecked,

    /// Follow all column defined in SQL, check the columns from external system with name & type.
    /// This ensures that no accidental side effect will change the schema.
    ///
    /// This is the behavior when creating a new table or source with a resolvable schema;
    /// adding, or dropping columns on that table.
    // TODO(purify): `ALTER SOURCE` currently has its own code path and does not check.
    FollowChecked,

    /// Merge the generated columns defined in SQL and columns from external system. If there
    /// are also regular columns defined in SQL, ignore silently.
    ///
    /// This is the behavior when `REFRESH SCHEMA` atop the purified SQL.
    Ignore,
}

/// Reject user-declared `VARIANT` columns (including nested) on binding paths whose ingest
/// pipeline cannot produce variant values. `context` names the path in the error message.
pub(crate) fn reject_variant_columns(columns: &[ColumnCatalog], context: &str) -> Result<()> {
    if let Some(col) = columns.iter().find(|c| c.data_type().contains_variant()) {
        return Err(RwError::from(NotSupported(
            format!(
                "VARIANT column \"{}\" is not supported {context} yet",
                col.name()
            ),
            format!("VARIANT columns are not supported {context} yet"),
        )));
    }
    Ok(())
}

/// Reject user-declared `VARIANT` columns when the `FORMAT ... ENCODE ...` is handled by a
/// row-encode parser or native generator, none of which can produce variant values.
///
/// Not gated: connector-native schemas (`ENCODE NONE`, e.g. iceberg) infer their own schema, and
/// `ENCODE PARQUET` reads variant columns via the Parquet Variant extension type.
fn reject_variant_columns_for_unsupported_encoding(
    format_encode: &FormatEncodeOptions,
    columns_from_sql: &[ColumnCatalog],
) -> Result<()> {
    if matches!(
        (&format_encode.format, &format_encode.row_encode),
        (Format::None, Encode::None) | (_, Encode::Parquet)
    ) {
        return Ok(());
    }
    reject_variant_columns(columns_from_sql, "for this source encoding")
}

/// Entrypoint for binding source connector.
/// Common logic shared by `CREATE SOURCE` and `CREATE TABLE`.
#[expect(clippy::too_many_arguments)]
pub async fn bind_create_source_or_table_with_connector(
    handler_args: HandlerArgs,
    full_name: ObjectName,
    format_encode: FormatEncodeOptions,
    with_properties: WithOptions,
    sql_columns_defs: &[ColumnDef],
    constraints: Vec<TableConstraint>,
    wildcard_idx: Option<usize>,
    source_watermarks: Vec<SourceWatermark>,
    columns_from_resolve_source: Option<Vec<ColumnCatalog>>,
    source_info: StreamSourceInfo,
    include_column_options: IncludeOption,
    col_id_gen: &mut ColumnIdGenerator,
    create_source_type: CreateSourceType,
    source_rate_limit: Option<u32>,
    sql_column_strategy: SqlColumnStrategy,
    refresh_mode: SourceRefreshMode,
) -> Result<SourceCatalog> {
    let session = &handler_args.session;
    let db_name: &str = &session.database();
    let (schema_name, source_name) = Binder::resolve_schema_qualified_name(db_name, &full_name)?;
    let (database_id, schema_id) =
        session.get_database_and_schema_id_for_create(schema_name.clone())?;

    let is_create_source = create_source_type != CreateSourceType::Table;

    if is_create_source {
        // reject refreshable batch source
        if with_properties.is_batch_connector() {
            return Err(ErrorCode::BindError(
            "can't CREATE SOURCE with refreshable batch connector\n\nHint: use CREATE TABLE instead"
                .to_owned(),
        )
        .into());
        }

        match format_encode.format {
            // reject unsupported formats for CREATE SOURCE
            Format::Debezium | Format::DebeziumMongo | Format::Maxwell | Format::Canal => {
                return Err(ErrorCode::BindError(format!(
                    "can't CREATE SOURCE with FORMAT {}.\n\nHint: use CREATE TABLE instead\n\n{}",
                    format_encode.format,
                    hint_format_encode(&format_encode)
                ))
                .into());
            }
            // hint limitations for some other formats
            Format::Upsert => {
                notice_to_user(format!(
                    "Streaming queries on sources with `FORMAT {}` may have limitations. If your query isn't supported, consider using `CREATE TABLE` instead.",
                    format_encode.format
                ));
            }
            _ => {}
        }
    }

    let sql_pk_names = bind_sql_pk_names(sql_columns_defs, bind_table_constraints(&constraints)?)?;

    if with_properties.is_iceberg_connector() {
        if is_create_source && !sql_pk_names.is_empty() {
            return Err(ErrorCode::NotSupported(
                "PRIMARY KEY is not supported for Iceberg CREATE SOURCE in continuous ingestion mode."
                    .to_owned(),
                "Iceberg streaming ingestion only supports append-only sources. Remove the PRIMARY KEY clause."
                    .to_owned(),
            )
            .into());
        }

        // FIXME: ideally we can support it, but current way of handling iceberg additional columns
        // are problematic. They are treated as normal user columns, so they will be lost if we
        // allow user to specify columns. See `extract_iceberg_columns`.
        if !sql_columns_defs.is_empty() {
            return Err(RwError::from(InvalidInputSyntax(
                r#"Schema is automatically inferred for iceberg source and should not be specified

HINT: use `CREATE SOURCE <name> WITH (...)` instead of `CREATE SOURCE <name> (<columns>) WITH (...)`."#.to_owned(),
            )));
        }
    }

    // Same for ADBC Snowflake connector - schema is automatically inferred
    if with_properties.is_batch_connector()
        && with_properties
            .get(UPSTREAM_SOURCE_KEY)
            .is_some_and(|s| s.eq_ignore_ascii_case(ADBC_SNOWFLAKE_CONNECTOR))
        && !sql_columns_defs.is_empty()
    {
        return Err(RwError::from(InvalidInputSyntax(
            r#"Schema is automatically inferred for ADBC Snowflake source and should not be specified

HINT: use `CREATE TABLE <name> WITH (...)` instead of `CREATE TABLE <name> (<columns>) WITH (...)`."#.to_owned(),
        )));
    }
    let columns_from_sql = bind_sql_columns(sql_columns_defs, false)?;

    reject_variant_columns_for_unsupported_encoding(&format_encode, &columns_from_sql)?;

    let mut columns = bind_all_columns(
        &format_encode,
        columns_from_resolve_source,
        columns_from_sql,
        sql_columns_defs,
        wildcard_idx,
        sql_column_strategy,
    )?;

    // add additional columns before bind pk, because `format upsert` requires the key column
    handle_addition_columns(
        Some(&format_encode),
        &with_properties,
        include_column_options,
        &mut columns,
        false,
    )?;

    if columns.is_empty() {
        return Err(RwError::from(ProtocolError(
            "Schema definition is required, either from SQL or schema registry.".to_owned(),
        )));
    }

    // compatible with the behavior that add a hidden column `_rw_kafka_timestamp` to each message from Kafka source
    if is_create_source {
        // must behind `handle_addition_columns`
        check_and_add_timestamp_column(&with_properties, &mut columns);

        // For shared sources, we will include partition and offset cols in the SourceExecutor's *output*, to be used by the SourceBackfillExecutor.
        // For shared CDC source, the schema is different. See ColumnCatalog::debezium_cdc_source_cols(), CDC_BACKFILL_TABLE_ADDITIONAL_COLUMNS
        if create_source_type == CreateSourceType::SharedNonCdc {
            let (columns_exist, additional_columns) = source_add_partition_offset_cols(
                &columns,
                &with_properties.get_connector().unwrap(),
                true, // col_id filled below at col_id_gen.generate
            );
            for (existed, c) in columns_exist.into_iter().zip_eq_fast(additional_columns) {
                if !existed {
                    columns.push(ColumnCatalog::hidden(c));
                }
            }
        }
    }

    // resolve privatelink connection for Kafka
    let mut with_properties = with_properties;
    resolve_privatelink_in_with_option(&mut with_properties)?;

    // check the system parameter `enforce_secret`
    if session
        .env()
        .system_params_manager()
        .get_params()
        .load()
        .enforce_secret()
        && Feature::SecretManagement.check_available().is_ok()
    {
        // check enforce using secret for some props on cloud
        ConnectorProperties::enforce_secret_source(&with_properties)?;
    }

    let (with_properties, connection_type, connector_conn_ref) =
        resolve_connection_ref_and_secret_ref(
            with_properties,
            session,
            Some(TelemetryDatabaseObject::Source),
        )?;
    ensure_connection_type_allowed(connection_type, &SOURCE_ALLOWED_CONNECTION_CONNECTOR)?;

    // if not using connection, we don't need to check connector match connection type
    if !matches!(connection_type, PbConnectionType::Unspecified) {
        let Some(connector) = with_properties.get_connector() else {
            return Err(RwError::from(ProtocolError(format!(
                "missing field '{}' in WITH clause",
                UPSTREAM_SOURCE_KEY
            ))));
        };
        check_connector_match_connection_type(connector.as_str(), &connection_type)?;
    }

    let pk_names = bind_source_pk(
        &format_encode,
        &source_info,
        &mut columns,
        sql_pk_names,
        &with_properties,
    )
    .await?;

    // User may specify a generated or additional column with the same name as one from the external schema.
    // Ensure duplicated column names are handled here.
    if let Some(duplicated_name) = columns.iter().map(|c| c.name()).duplicates().next() {
        return Err(ErrorCode::InvalidInputSyntax(format!(
            "column \"{}\" specified more than once",
            duplicated_name
        ))
        .into());
    }

    // XXX: why do we use col_id_gen here? It doesn't seem to be very necessary.
    for c in &mut columns {
        let original_data_type = c.data_type().clone();
        col_id_gen.generate(c)?;
        // TODO: Now we restore the data type for `CREATE SOURCE`, so that keep the nested field id unset.
        //       This behavior is inconsistent with `CREATE TABLE`, and should be fixed once we refactor
        //       `ALTER SOURCE` to also use `ColumnIdGenerator` in the future.
        if is_create_source {
            c.column_desc.data_type = original_data_type;
        }
    }
    debug_assert_column_ids_distinct(&columns);

    let (mut columns, pk_col_ids, row_id_index) =
        bind_pk_and_row_id_on_relation(columns, pk_names, true)?;

    let watermark_descs =
        bind_source_watermark(session, source_name.clone(), source_watermarks, &columns)?;
    // TODO(yuhao): allow multiple watermark on source.
    assert!(watermark_descs.len() <= 1);
    if is_create_source && watermark_descs.iter().any(|d| d.with_ttl) {
        return Err(ErrorCode::NotSupported(
            "WITH TTL is not supported in WATERMARK clause for CREATE SOURCE.".to_owned(),
            "Use `CREATE TABLE ... WATERMARK ... WITH TTL` instead.".to_owned(),
        )
        .into());
    }

    let append_only = row_id_index.is_some();
    if is_create_source && !append_only && !watermark_descs.is_empty() {
        return Err(ErrorCode::NotSupported(
            "Defining watermarks on source requires the source connector to be append only."
                .to_owned(),
            "Use the key words `FORMAT PLAIN`".to_owned(),
        )
        .into());
    }

    bind_sql_column_constraints(
        session,
        source_name.clone(),
        &mut columns,
        // TODO(st1page): pass the ref
        sql_columns_defs,
        &pk_col_ids,
    )?;
    check_format_encode(&with_properties, row_id_index, &columns)?;

    let definition = handler_args.normalized_sql.clone();

    let associated_table_id = if is_create_source {
        None
    } else {
        Some(TableId::placeholder())
    };
    let source = SourceCatalog {
        id: SourceId::placeholder(),
        name: source_name,
        schema_id,
        database_id,
        columns,
        pk_col_ids,
        append_only,
        owner: session.user_id(),
        info: source_info,
        row_id_index,
        with_properties,
        watermark_descs,
        associated_table_id,
        definition,
        connection_id: connector_conn_ref,
        created_at_epoch: None,
        initialized_at_epoch: None,
        version: INITIAL_SOURCE_VERSION_ID,
        created_at_cluster_version: None,
        initialized_at_cluster_version: None,
        rate_limit: source_rate_limit,
        refresh_mode: Some(refresh_mode),
    };
    Ok(source)
}

pub async fn handle_create_source(
    mut handler_args: HandlerArgs,
    stmt: CreateSourceStatement,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let overwrite_options = OverwriteOptions::new(&mut handler_args);

    if let Either::Right(resp) = session.check_relation_name_duplicated(
        stmt.source_name.clone(),
        StatementType::CREATE_SOURCE,
        stmt.if_not_exists,
    )? {
        return Ok(resp);
    }

    if stmt
        .source_name
        .base_name()
        .starts_with(ICEBERG_SOURCE_PREFIX)
    {
        return Err(RwError::from(InvalidInputSyntax(format!(
            "Source name cannot start with reserved prefix '{}'",
            ICEBERG_SOURCE_PREFIX
        ))));
    }

    if handler_args.with_options.is_empty() {
        return Err(RwError::from(InvalidInputSyntax(
            "missing WITH clause".to_owned(),
        )));
    }

    if overwrite_options.source_rate_limit == Some(0)
        && must_wait_cdc_offset_before_report(&handler_args.with_options)
    {
        let connector = handler_args.with_options.get_connector().unwrap();
        return Err(RwError::from(ErrorCode::InvalidParameterValue(format!(
            "`source_rate_limit` cannot be 0 when creating a `{connector}` source because source creation must wait for the initial CDC offset."
        ))));
    }

    let format_encode = stmt.format_encode.into_v2_with_warning();
    let (with_properties, refresh_mode) =
        bind_connector_props(&handler_args, &format_encode, true)?;
    if let Some(connector) = with_properties.get_connector() {
        ensure_local_fs_connector_allowed(&session, &connector)?;
    }

    let create_source_type = CreateSourceType::for_newly_created(&session, &*with_properties);
    let (columns_from_resolve_source, source_info) = bind_columns_from_source(
        &session,
        &format_encode,
        Either::Left(&with_properties),
        create_source_type,
    )
    .await?;
    let mut col_id_gen = ColumnIdGenerator::new_initial();

    if stmt.columns.iter().any(|col| {
        col.options
            .iter()
            .any(|def| matches!(def.option, ColumnOption::NotNull))
    }) {
        return Err(RwError::from(InvalidInputSyntax(
            "NOT NULL constraint is not supported in source schema".to_owned(),
        )));
    }

    let source_catalog = bind_create_source_or_table_with_connector(
        handler_args.clone(),
        stmt.source_name,
        format_encode,
        with_properties,
        &stmt.columns,
        stmt.constraints,
        stmt.wildcard_idx,
        stmt.source_watermarks,
        columns_from_resolve_source,
        source_info,
        stmt.include_column_options,
        &mut col_id_gen,
        create_source_type,
        overwrite_options.source_rate_limit,
        SqlColumnStrategy::FollowChecked,
        refresh_mode,
    )
    .await?;

    // If it is a temporary source, put it into SessionImpl.
    if stmt.temporary {
        if session.get_temporary_source(&source_catalog.name).is_some() {
            return Err(CatalogError::duplicated("source", source_catalog.name.clone()).into());
        }
        session.create_temporary_source(source_catalog);
        return Ok(PgResponse::empty_result(StatementType::CREATE_SOURCE));
    }

    let source = source_catalog.to_prost();

    let catalog_writer = session.catalog_writer()?;

    if create_source_type.is_shared() {
        let graph = generate_stream_graph_for_source(handler_args, source_catalog)?;
        catalog_writer
            .create_source(source, Some(graph), stmt.if_not_exists)
            .await?;
    } else {
        // For other sources we don't create a streaming job
        catalog_writer
            .create_source(source, None, stmt.if_not_exists)
            .await?;
    }

    Ok(PgResponse::empty_result(StatementType::CREATE_SOURCE))
}

pub(super) fn generate_stream_graph_for_source(
    handler_args: HandlerArgs,
    source_catalog: SourceCatalog,
) -> Result<PbStreamFragmentGraph> {
    let context = OptimizerContext::from_handler_args(handler_args);
    let source_node = LogicalSource::with_catalog(
        Rc::new(source_catalog),
        SourceNodeKind::CreateSharedSource,
        context.into(),
        None,
    )?;

    let stream_plan = source_node.to_stream(&mut ToStreamContext::new_with_backfill_type(
        false,
        // Shared source planning does not create stream table scans, so this required context
        // value is only a placeholder and is not used for backfill selection.
        BackfillType::ArrangementBackfill,
    ))?;
    let graph = build_graph(stream_plan, Some(GraphJobType::Source))?;
    Ok(graph)
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use risingwave_common::catalog::{
        DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, ROW_ID_COLUMN_NAME,
    };
    use risingwave_common::config::FrontendConfig;
    use risingwave_common::types::{DataType, StructType};
    use risingwave_pb::plan_common::EncodeType;

    use crate::catalog::root_catalog::SchemaPath;
    use crate::catalog::source_catalog::SourceCatalog;
    use crate::test_utils::{LocalFrontend, PROTO_FILE_DATA, create_proto_file};

    const GET_COLUMN_FROM_CATALOG: fn(&Arc<SourceCatalog>) -> HashMap<&str, DataType> =
        |catalog: &Arc<SourceCatalog>| -> HashMap<&str, DataType> {
            catalog
                .columns
                .iter()
                .map(|col| (col.name(), col.data_type().clone()))
                .collect::<HashMap<&str, DataType>>()
        };

    #[test]
    fn test_valid_heartbeat_interval() {
        assert!(super::is_valid_heartbeat_interval("1"));
        assert!(super::is_valid_heartbeat_interval("300000"));
        assert!(!super::is_valid_heartbeat_interval("0"));
        assert!(!super::is_valid_heartbeat_interval("-1"));
        assert!(!super::is_valid_heartbeat_interval("invalid"));
    }

    #[tokio::test]
    async fn test_create_source_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t
    WITH (connector = 'kinesis')
    FORMAT PLAIN ENCODE PROTOBUF (message = '.test.TestRecord', schema.location = 'file://{}')"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
            .unwrap();
        assert_eq!(source.name, "t");

        let columns = GET_COLUMN_FROM_CATALOG(source);

        let city_type = StructType::new(vec![
            ("address", DataType::Varchar),
            ("zipcode", DataType::Varchar),
        ])
        // .with_ids([5, 6].map(ColumnId::new))
        .into();
        let expected_columns = maplit::hashmap! {
            ROW_ID_COLUMN_NAME => DataType::Serial,
            "id" => DataType::Int32,
            "zipcode" => DataType::Int64,
            "rate" => DataType::Float32,
            "country" => StructType::new(
                vec![("address", DataType::Varchar),("city", city_type),("zipcode", DataType::Varchar)],
            )
            // .with_ids([3, 4, 7].map(ColumnId::new))
            .into(),
        };
        assert_eq!(columns, expected_columns, "{columns:#?}");
    }

    #[tokio::test]
    async fn test_create_mqtt_source_with_protobuf() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t_mqtt
    WITH (
        connector = 'mqtt',
        url = 'mqtt://localhost:1883',
        topic = 'test_topic'
    )
    FORMAT PLAIN ENCODE PROTOBUF (
        message = '.test.TestRecord',
        schema.location = 'file://{}'
    )"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t_mqtt")
            .unwrap();

        assert_eq!(source.name, "t_mqtt");
        assert_eq!(source.info.row_encode, EncodeType::Protobuf as i32);
    }

    #[tokio::test]
    async fn test_create_posix_fs_source_requires_frontend_config() {
        let frontend = LocalFrontend::with_frontend_config(
            Default::default(),
            FrontendConfig {
                unsafe_enable_local_fs_connector: false,
                ..Default::default()
            },
        )
        .await;
        let err = frontend
            .run_sql(
                r#"CREATE SOURCE local_files (
                    line VARCHAR
                ) WITH (
                    connector = 'posix_fs',
                    posix_fs.root = '/tmp',
                    match_pattern = '*.csv'
                ) FORMAT PLAIN ENCODE CSV (without_header = 'true')"#
                    .to_owned(),
            )
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("frontend.unsafe_enable_local_fs_connector = true"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn test_create_oracle_cdc_source_requires_frontend_config() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let err = frontend
            .run_sql(
                r#"CREATE SOURCE oracle_source
                WITH (connector = 'oracle-cdc')
                FORMAT PLAIN ENCODE JSON"#
                    .to_owned(),
            )
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("frontend.unsafe_enable_oracle_cdc = true"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn test_duplicate_props_options() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t
    WITH (
        connector = 'kinesis',
        aws.region='user_test_topic',
        endpoint='172.10.1.1:9090,172.10.1.2:9090',
        aws.credentials.access_key_id = 'your_access_key_1',
        aws.credentials.secret_access_key = 'your_secret_key_1'
    )
    FORMAT PLAIN ENCODE PROTOBUF (
        message = '.test.TestRecord',
        aws.credentials.access_key_id = 'your_access_key_2',
        aws.credentials.secret_access_key = 'your_secret_key_2',
        schema.location = 'file://{}',
    )"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
            .unwrap();
        assert_eq!(source.name, "t");

        // AwsAuth params exist in options.
        assert_eq!(
            source
                .info
                .format_encode_options
                .get("aws.credentials.access_key_id")
                .unwrap(),
            "your_access_key_2"
        );
        assert_eq!(
            source
                .info
                .format_encode_options
                .get("aws.credentials.secret_access_key")
                .unwrap(),
            "your_secret_key_2"
        );

        // AwsAuth params exist in props.
        assert_eq!(
            source
                .with_properties
                .get("aws.credentials.access_key_id")
                .unwrap(),
            "your_access_key_1"
        );
        assert_eq!(
            source
                .with_properties
                .get("aws.credentials.secret_access_key")
                .unwrap(),
            "your_secret_key_1"
        );

        // Options are not merged into props.
        assert!(!source.with_properties.contains_key("schema.location"));
    }

    #[tokio::test]
    async fn test_multi_table_cdc_create_source_handler() {
        let sql =
            "CREATE SOURCE t2 WITH (connector = 'mysql-cdc') FORMAT PLAIN ENCODE JSON".to_owned();
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();

        frontend
            .run_sql_with_session(session.clone(), sql)
            .await
            .unwrap();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t2")
            .unwrap();
        assert_eq!(source.name, "t2");

        let columns = source
            .columns
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<Vec<(&str, DataType)>>();

        expect_test::expect![[r#"
            [
                (
                    "payload",
                    Jsonb,
                ),
                (
                    "_rw_offset",
                    Varchar,
                ),
                (
                    "_rw_table_name",
                    Varchar,
                ),
                (
                    "_row_id",
                    Serial,
                ),
            ]
        "#]]
        .assert_debug_eq(&columns);
    }

    #[tokio::test]
    async fn test_reject_zero_source_rate_limit_when_cdc_must_wait_for_offset() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();

        for (source_name, connector) in [
            ("mysql_explicit_zero", "mysql-cdc"),
            ("sqlserver_explicit_zero", "sqlserver-cdc"),
        ] {
            let err = frontend
                .run_sql_with_session(
                    session.clone(),
                    format!(
                        "CREATE SOURCE {source_name} WITH (connector = '{connector}', source_rate_limit = '0') FORMAT PLAIN ENCODE JSON"
                    ),
                )
                .await
                .unwrap_err();
            let message = err.to_string();
            assert!(
                message.contains("source creation must wait for the initial CDC offset"),
                "{message}"
            );
        }

        frontend
            .run_sql_with_session(session.clone(), "SET source_rate_limit TO 0;")
            .await
            .unwrap();

        for (source_name, connector) in [
            ("mysql_session_zero", "mysql-cdc"),
            ("sqlserver_session_zero", "sqlserver-cdc"),
        ] {
            let err = frontend
                .run_sql_with_session(
                    session.clone(),
                    format!(
                        "CREATE SOURCE {source_name} WITH (connector = '{connector}') FORMAT PLAIN ENCODE JSON"
                    ),
                )
                .await
                .unwrap_err();
            assert!(
                err.to_string()
                    .contains("source creation must wait for the initial CDC offset"),
                "{err}"
            );
        }

        frontend
            .run_sql_with_session(
                session.clone(),
                "CREATE SOURCE mysql_positive_override WITH (connector = 'mysql-cdc', source_rate_limit = '1') FORMAT PLAIN ENCODE JSON",
            )
            .await
            .unwrap();

        frontend
            .run_sql_with_session(
                session,
                "CREATE SOURCE postgres_zero WITH (connector = 'postgres-cdc') FORMAT PLAIN ENCODE JSON",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_source_addition_columns() {
        // test derive include column for format plain
        let sql =
            "CREATE SOURCE s (v1 int) include key as _rw_kafka_key with (connector = 'kafka') format plain encode json".to_owned();
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();
        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let (source, _) = catalog_reader
            .get_source_by_name(
                DEFAULT_DATABASE_NAME,
                SchemaPath::Name(DEFAULT_SCHEMA_NAME),
                "s",
            )
            .unwrap();
        assert_eq!(source.name, "s");

        let columns = source
            .columns
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<Vec<(&str, DataType)>>();

        expect_test::expect![[r#"
            [
                (
                    "v1",
                    Int32,
                ),
                (
                    "_rw_kafka_key",
                    Bytea,
                ),
                (
                    "_rw_kafka_timestamp",
                    Timestamptz,
                ),
                (
                    "_rw_kafka_partition",
                    Varchar,
                ),
                (
                    "_rw_kafka_offset",
                    Varchar,
                ),
                (
                    "_row_id",
                    Serial,
                ),
            ]
        "#]]
        .assert_debug_eq(&columns);
        drop(catalog_reader);

        let sql =
            "CREATE SOURCE s_pulsar (v1 int) include header 'tenant' as pulsar_header with (connector = 'pulsar') format plain encode json".to_owned();
        frontend.run_sql(sql).await.unwrap();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let (source, _) = catalog_reader
            .get_source_by_name(
                DEFAULT_DATABASE_NAME,
                SchemaPath::Name(DEFAULT_SCHEMA_NAME),
                "s_pulsar",
            )
            .unwrap();
        assert_eq!(source.name, "s_pulsar");

        let columns = source
            .columns
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<Vec<(&str, DataType)>>();

        expect_test::expect![[r#"
            [
                (
                    "v1",
                    Int32,
                ),
                (
                    "pulsar_header",
                    Bytea,
                ),
                (
                    "_row_id",
                    Serial,
                ),
            ]
        "#]]
        .assert_debug_eq(&columns);
        drop(catalog_reader);

        let sql =
            "CREATE SOURCE s3 (v1 int) include timestamp 'header1' as header_col with (connector = 'kafka') format plain encode json".to_owned();
        match frontend.run_sql(sql).await {
            Err(e) => {
                assert_eq!(
                    e.to_string(),
                    "Protocol error: Only header column can have inner field, but got \"timestamp\""
                )
            }
            _ => unreachable!(),
        }
    }
}
