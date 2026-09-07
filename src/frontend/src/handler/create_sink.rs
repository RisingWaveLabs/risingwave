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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use anyhow::Context;
use either::Either;
use iceberg::arrow::type_to_arrow_type;
use iceberg::spec::Transform;
use itertools::Itertools;
use maplit::{convert_args, hashmap, hashset};
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::arrow::arrow_schema_iceberg::DataType as ArrowDataType;
use risingwave_common::bail;
use risingwave_common::catalog::{
    ColumnCatalog, ICEBERG_SINK_PREFIX, ObjectId, RISINGWAVE_ICEBERG_ROW_ID, ROW_ID_COLUMN_NAME,
    Schema,
};
use risingwave_common::license::Feature;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::types::{DataType, Timestamptz};
use risingwave_common::util::epoch::Epoch;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkFormatDesc};
use risingwave_connector::sink::file_sink::s3::SnowflakeSink;
use risingwave_connector::sink::iceberg::{ICEBERG_SINK, IcebergConfig};
use risingwave_connector::sink::kafka::KAFKA_SINK;
use risingwave_connector::sink::snowflake_redshift::redshift::RedshiftSink;
use risingwave_connector::sink::snowflake_redshift::snowflake::SnowflakeV2Sink;
use risingwave_connector::sink::{
    CONNECTOR_TYPE_KEY, SINK_SNAPSHOT_OPTION, SINK_TYPE_OPTION, SINK_USER_FORCE_APPEND_ONLY_OPTION,
    SINK_USER_IGNORE_DELETE_OPTION, Sink, enforce_secret_sink, sink_is_exactly_once,
};
use risingwave_connector::{
    AUTO_SCHEMA_CHANGE_KEY, SINK_CREATE_TABLE_IF_NOT_EXISTS_KEY, SINK_INTERMEDIATE_TABLE_NAME,
    SINK_TARGET_TABLE_NAME, WithPropertiesExt,
};
use risingwave_pb::catalog::connection_params::PbConnectionType;
use risingwave_pb::telemetry::TelemetryDatabaseObject;
use risingwave_sqlparser::ast::{
    CreateSink, CreateSinkStatement, EmitMode, Encode, ExplainOptions, Format, FormatEncodeOptions,
    ObjectName, Query, Statement,
};
use risingwave_sqlparser::parser::Parser;

use super::RwPgResponse;
use super::create_mv::get_column_names;
use super::create_source::UPSTREAM_SOURCE_KEY;
use super::util::gen_query_from_table_name;
use crate::binder::{Binder, Relation};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::TableType;
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{ExprImpl, InputRef, rewrite_now_to_proctime};
use crate::handler::HandlerArgs;
use crate::handler::alter_table_column::fetch_table_catalog_for_alter;
use crate::handler::create_mv::{
    extract_streaming_job_resource_options, parse_column_names, resolve_streaming_job_resource_type,
};
use crate::handler::util::{
    LongRunningNotificationAction, check_connector_match_connection_type,
    ensure_connection_type_allowed, ensure_local_fs_connector_allowed,
    execute_with_long_running_notification, get_table_catalog_by_table_name,
    reject_internal_table_dependencies,
};
use crate::optimizer::backfill_order_strategy::plan_backfill_order;
use crate::optimizer::plan_node::{
    IcebergPartitionInfo, LogicalSource, PartitionComputeInfo, StreamPlanRef as PlanRef,
    StreamProject, ensure_sync_log_store_fragment_root, generic,
};
use crate::optimizer::{OptimizerContext, RelationCollectorVisitor};
use crate::scheduler::streaming_manager::CreatingStreamingJobInfo;
use crate::session::SessionImpl;
use crate::session::current::notice_to_user;
use crate::stream_fragmenter::{GraphJobType, build_graph_with_strategy};
use crate::utils::{resolve_connection_ref_and_secret_ref, resolve_privatelink_in_with_option};
use crate::{Explain, Planner, TableCatalog, WithOptions, WithOptionsSecResolved};

static SINK_ALLOWED_CONNECTION_CONNECTOR: LazyLock<HashSet<PbConnectionType>> =
    LazyLock::new(|| {
        hashset! {
            PbConnectionType::Unspecified,
            PbConnectionType::Kafka,
            PbConnectionType::Iceberg,
            PbConnectionType::Elasticsearch,
        }
    });

static SINK_ALLOWED_CONNECTION_SCHEMA_REGISTRY: LazyLock<HashSet<PbConnectionType>> =
    LazyLock::new(|| {
        hashset! {
            PbConnectionType::Unspecified,
            PbConnectionType::SchemaRegistry,
        }
    });

const SINK_SINCE_TIMESTAMP_OPTION: &str = "since_timestamp";

// used to store result of `gen_sink_plan`
pub struct SinkPlanContext {
    pub query: Box<Query>,
    pub sink_plan: PlanRef,
    pub sink_catalog: SinkCatalog,
    pub target_table_catalog: Option<Arc<TableCatalog>>,
    pub dependencies: HashSet<ObjectId>,
    pub since_timestamp_epoch: Option<u64>,
}

pub async fn gen_sink_plan(
    handler_args: HandlerArgs,
    stmt: CreateSinkStatement,
    explain_options: Option<ExplainOptions>,
    is_iceberg_engine_internal: bool,
) -> Result<SinkPlanContext> {
    let session = handler_args.session.clone();
    let session = session.as_ref();
    let user_specified_columns = !stmt.columns.is_empty();
    let db_name = &session.database();
    let (sink_schema_name, sink_table_name) =
        Binder::resolve_schema_qualified_name(db_name, &stmt.sink_name)?;

    let mut with_options = handler_args.with_options.clone();
    // These are frontend-level streaming job options. They must not be passed to connector
    // property validation.
    extract_streaming_job_resource_options(&mut with_options);

    if session
        .env()
        .system_params_manager()
        .get_params()
        .load()
        .enforce_secret()
        && Feature::SecretManagement.check_available().is_ok()
    {
        enforce_secret_sink(&with_options)?;
    }

    resolve_privatelink_in_with_option(&mut with_options)?;
    let (mut resolved_with_options, connection_type, connector_conn_ref) =
        resolve_connection_ref_and_secret_ref(
            with_options,
            session,
            Some(TelemetryDatabaseObject::Sink),
        )?;

    let since_timestamp_epoch = resolved_with_options
        .remove(SINK_SINCE_TIMESTAMP_OPTION)
        .map(|value| {
            let timestamp = value.parse::<Timestamptz>().map_err(|err| {
                ErrorCode::InvalidInputSyntax(format!(
                    "invalid value {value:?} of '{SINK_SINCE_TIMESTAMP_OPTION}' option: {err}; \
                     expected a timestamptz string with an explicit time zone, \
                     for example '2024-01-01 00:00:00Z'"
                ))
            })?;
            let timestamp_millis = u64::try_from(timestamp.timestamp_millis()).unwrap_or(0);
            Ok::<_, RwError>(Epoch::from_unix_millis_or_earliest(timestamp_millis).0)
        })
        .transpose()?;
    if since_timestamp_epoch.is_some() {
        Feature::SinkSinceTimestamp.check_available()?;
    }

    ensure_connection_type_allowed(connection_type, &SINK_ALLOWED_CONNECTION_CONNECTOR)?;

    // if not using connection, we don't need to check connector match connection type
    if !matches!(connection_type, PbConnectionType::Unspecified) {
        let Some(connector) = resolved_with_options.get_connector() else {
            return Err(RwError::from(ErrorCode::ProtocolError(format!(
                "missing field '{}' in WITH clause",
                CONNECTOR_TYPE_KEY
            ))));
        };
        check_connector_match_connection_type(connector.as_str(), &connection_type)?;
    }

    let partition_info = get_partition_compute_info(&resolved_with_options).await?;

    let context = if let Some(explain_options) = explain_options {
        OptimizerContext::new(handler_args.clone(), explain_options)
    } else {
        OptimizerContext::from_handler_args(handler_args.clone())
    };
    let is_auto_schema_change = resolved_with_options
        .get(AUTO_SCHEMA_CHANGE_KEY)
        .map(|value| {
            value.parse::<bool>().map_err(|_| {
                ErrorCode::InvalidInputSyntax(format!(
                    "invalid value {} of '{}' option, expect",
                    value, AUTO_SCHEMA_CHANGE_KEY
                ))
            })
        })
        .transpose()?
        .unwrap_or(false);

    if is_auto_schema_change && !is_iceberg_engine_internal {
        Feature::SinkAutoSchemaChange.check_available()?;
    }

    let sink_into_table_name = stmt.into_table_name.as_ref().map(|name| name.real_value());
    if sink_into_table_name.is_some() {
        let prev = resolved_with_options.insert(CONNECTOR_TYPE_KEY.to_owned(), "table".to_owned());

        if prev.is_some() {
            return Err(RwError::from(ErrorCode::BindError(
                "In the case of sinking into table, the 'connector' parameter should not be provided.".to_owned(),
            )));
        }
    }
    let connector = resolved_with_options
        .get(CONNECTOR_TYPE_KEY)
        .cloned()
        .ok_or_else(|| ErrorCode::BindError(format!("missing field '{CONNECTOR_TYPE_KEY}'")))?;
    ensure_local_fs_connector_allowed(session, &connector)?;

    // Used for debezium's table name
    let sink_from_table_name;
    // `true` means that sink statement has the form: `CREATE SINK s1 FROM ...`
    // `false` means that sink statement has the form: `CREATE SINK s1 AS <query>`
    let direct_sink_from_name: Option<(ObjectName, bool)>;
    let mut query = match stmt.sink_from {
        CreateSink::From(from_name) => {
            sink_from_table_name = from_name.0.last().unwrap().real_value();
            direct_sink_from_name = Some((from_name.clone(), is_auto_schema_change));
            if is_auto_schema_change && sink_into_table_name.is_some() {
                return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                    "auto schema change not supported for sink-into-table".to_owned(),
                )));
            }
            if resolved_with_options
                .value_eq_ignore_case(SINK_CREATE_TABLE_IF_NOT_EXISTS_KEY, "true")
                && connector == RedshiftSink::SINK_NAME
                || connector == SnowflakeV2Sink::SINK_NAME
            {
                if let Some(table_name) = resolved_with_options.get(SINK_TARGET_TABLE_NAME) {
                    // auto fill intermediate table name if target table name is specified
                    if resolved_with_options
                        .get(SINK_INTERMEDIATE_TABLE_NAME)
                        .is_none()
                    {
                        // generate the intermediate table name with random value appended to the target table name
                        let intermediate_table_name = format!(
                            "rw_{}_{}_{}",
                            sink_table_name,
                            table_name,
                            uuid::Uuid::new_v4()
                        );
                        resolved_with_options.insert(
                            SINK_INTERMEDIATE_TABLE_NAME.to_owned(),
                            intermediate_table_name,
                        );
                    }
                } else {
                    return Err(RwError::from(ErrorCode::BindError(
                        "'table.name' option must be specified.".to_owned(),
                    )));
                }
            }
            Box::new(gen_query_from_table_name(from_name))
        }
        CreateSink::AsQuery(query) => {
            if is_auto_schema_change {
                return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                    "auto schema change not supported for CREATE SINK AS QUERY".to_owned(),
                )));
            }
            sink_from_table_name = sink_table_name.clone();
            direct_sink_from_name = None;
            query
        }
    };

    if is_iceberg_engine_internal && let Some((from_name, _)) = &direct_sink_from_name {
        let (table, _) = get_table_catalog_by_table_name(session, from_name)?;
        let pk_names = table.pk_column_names();
        if pk_names.len() == 1 && pk_names[0].eq(ROW_ID_COLUMN_NAME) {
            let [stmt]: [_; 1] = Parser::parse_sql(&format!(
                "select {} as {}, * from {}",
                ROW_ID_COLUMN_NAME, RISINGWAVE_ICEBERG_ROW_ID, from_name
            ))
            .context("unable to parse query")?
            .try_into()
            .unwrap();
            let Statement::Query(parsed_query) = stmt else {
                panic!("unexpected statement: {:?}", stmt);
            };
            query = parsed_query;
        }
    }

    let (sink_database_id, sink_schema_id) =
        session.get_database_and_schema_id_for_create(sink_schema_name.clone())?;

    if since_timestamp_epoch.is_some() {
        if sink_into_table_name.is_some() {
            return Err(ErrorCode::BindError(format!(
                "`{SINK_SINCE_TIMESTAMP_OPTION}` does not support `CREATE SINK INTO TABLE`"
            ))
            .into());
        }
        if is_iceberg_engine_internal {
            return Err(ErrorCode::BindError(format!(
                "`{SINK_SINCE_TIMESTAMP_OPTION}` does not support iceberg engine internal sinks"
            ))
            .into());
        }
        if let Some((from_name, _)) = &direct_sink_from_name {
            let (table, _) = get_table_catalog_by_table_name(session, from_name)?;
            if table.database_id != sink_database_id {
                return Err(ErrorCode::NotSupported(
                    format!(
                        "`{SINK_SINCE_TIMESTAMP_OPTION}` does not support cross-database sinks"
                    ),
                    "Please create the sink in the same database as the upstream table.".to_owned(),
                )
                .into());
            }
        }
    }

    let (
        dependent_relations,
        dependent_udfs,
        dependent_secrets,
        bound,
        auto_refresh_schema_from_table,
    ) = {
        let mut binder = Binder::new_for_stream(session);
        let auto_refresh_schema_from_table = if let Some((from_name, true)) = &direct_sink_from_name
        {
            let from_relation = binder.bind_relation_by_name(from_name, None, None, true)?;
            if let Relation::BaseTable(table) = from_relation {
                if table.table_catalog.table_type != TableType::Table {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "auto schema change is supported only on TABLE, but got {:?}",
                        table.table_catalog.table_type
                    ))
                    .into());
                }
                if table.table_catalog.database_id != sink_database_id {
                    return Err(ErrorCode::InvalidInputSyntax(
                        "auto schema change sinks do not support cross-database tables".to_owned(),
                    )
                    .into());
                }
                for col in &table.table_catalog.columns {
                    if !col.is_hidden() && (col.is_generated() || col.is_rw_sys_column()) {
                        return Err(ErrorCode::InvalidInputSyntax(format!(
                            "auto schema change is not supported for tables with visible generated columns or visible system columns, but found column {}",
                            col.name()
                        ))
                        .into());
                    }
                }
                Some(table.table_catalog)
            } else {
                return Err(RwError::from(ErrorCode::NotSupported(
                    "auto schema change only supported for TABLE".to_owned(),
                    "try recreating the sink from table".to_owned(),
                )));
            }
        } else {
            None
        };

        let bound = binder.bind_query(&query)?;

        (
            binder.included_relations().clone(),
            binder.included_udfs().clone(),
            binder.included_secrets().clone(),
            bound,
            auto_refresh_schema_from_table,
        )
    };

    reject_internal_table_dependencies(session, &dependent_relations, "CREATE SINK")?;

    let col_names = if sink_into_table_name.is_some() {
        parse_column_names(&stmt.columns)
    } else {
        // If column names not specified, use the name in the bound query, which is equal with the plan root's original field name.
        get_column_names(&bound, stmt.columns)?
    };

    let emit_on_window_close = stmt.emit_mode == Some(EmitMode::OnWindowClose);
    if emit_on_window_close {
        context.warn_to_user("EMIT ON WINDOW CLOSE is currently an experimental feature. Please use it with caution.");
    }

    let format_desc = match stmt.sink_schema {
        // Case A: new syntax `format ... encode ...`
        Some(f) => {
            validate_compatibility(&connector, &f)?;
            Some(bind_sink_format_desc(session,f)?)
        }
        None => match resolved_with_options.get(SINK_TYPE_OPTION) {
            // Case B: old syntax `type = '...'`
            Some(t) => SinkFormatDesc::from_legacy_type(&connector, t)?.map(|mut f| {
                session.notice_to_user("Consider using the newer syntax `FORMAT ... ENCODE ...` instead of `type = '...'`.");
                if let Some(v) = resolved_with_options.get(SINK_USER_FORCE_APPEND_ONLY_OPTION) {
                    f.options.insert(SINK_USER_FORCE_APPEND_ONLY_OPTION.into(), v.into());
                }
                if let Some(v) = resolved_with_options.get(SINK_USER_IGNORE_DELETE_OPTION) {
                    f.options.insert(SINK_USER_IGNORE_DELETE_OPTION.into(), v.into());
                }
                f
            }),
            // Case C: no format + encode required
            None => None,
        },
    };

    let definition = context.normalized_sql().to_owned();
    let mut plan_root = if is_iceberg_engine_internal {
        Planner::new_for_iceberg_table_engine_sink(context.into()).plan_query(bound)?
    } else {
        Planner::new_for_stream(context.into()).plan_query(bound)?
    };
    if let Some(col_names) = &col_names {
        plan_root.set_out_names(col_names.clone())?;
    };

    let without_snapshot = matches!(
        resolved_with_options.remove(SINK_SNAPSHOT_OPTION),
        Some(flag) if flag.eq_ignore_ascii_case("false")
    );

    if since_timestamp_epoch.is_some() && !without_snapshot {
        return Err(ErrorCode::BindError(format!(
            "`{SINK_SINCE_TIMESTAMP_OPTION}` requires `snapshot = false`"
        ))
        .into());
    }

    let target_table_catalog = stmt
        .into_table_name
        .as_ref()
        .map(|table_name| fetch_table_catalog_for_alter(session, table_name).map(|t| t.0))
        .transpose()?;

    if let Some(target_table_catalog) = &target_table_catalog {
        if let Some(col_names) = col_names {
            let target_table_columns = target_table_catalog
                .columns()
                .iter()
                .map(|c| c.name())
                .collect::<BTreeSet<_>>();
            for c in col_names {
                if !target_table_columns.contains(c.as_str()) {
                    return Err(RwError::from(ErrorCode::BindError(format!(
                        "Column {} not found in table {}",
                        c,
                        target_table_catalog.name()
                    ))));
                }
            }
        }
        if target_table_catalog
            .columns()
            .iter()
            .any(|col| !col.nullable())
        {
            notice_to_user(format!(
                "The target table `{}` contains NOT NULL columns. Rows written by the sink that violate those constraints will be ignored silently.",
                target_table_catalog.name(),
            ));
        }
    }

    let sink_plan = plan_root.gen_sink_plan(
        sink_table_name,
        definition,
        resolved_with_options,
        emit_on_window_close,
        db_name.to_owned(),
        sink_from_table_name,
        format_desc,
        without_snapshot,
        since_timestamp_epoch.is_some(),
        is_iceberg_engine_internal,
        target_table_catalog.clone(),
        partition_info,
        user_specified_columns,
        auto_refresh_schema_from_table,
    )?;

    let sink_desc = sink_plan.sink_desc().clone();

    let mut sink_plan: PlanRef = sink_plan.into_stream_plan()?;
    sink_plan = ensure_sync_log_store_fragment_root(sink_plan);

    let ctx = sink_plan.ctx();
    let explain_trace = ctx.is_explain_trace();
    if explain_trace {
        ctx.trace("Create Sink:");
        ctx.trace(sink_plan.explain_to_string());
    }
    tracing::trace!("sink_plan: {:?}", sink_plan.explain_to_string());

    // TODO(rc): To be consistent with UDF dependency check, we should collect relation dependencies
    // during binding instead of visiting the optimized plan.
    let dependencies =
        RelationCollectorVisitor::collect_with(dependent_relations, sink_plan.clone())
            .into_iter()
            .chain(dependent_udfs.iter().copied().map_into())
            .chain(
                dependent_secrets
                    .iter()
                    .copied()
                    .map(|id| id.as_object_id()),
            )
            .collect();

    let sink_catalog = sink_desc.into_catalog(
        sink_schema_id,
        sink_database_id,
        session.user_id(),
        connector_conn_ref,
    );

    if let Some(table_catalog) = &target_table_catalog {
        for column in sink_catalog.full_columns() {
            if !column.can_dml() {
                unreachable!(
                    "cannot derive generated columns or the `_rw_timestamp` system column in a sink catalog, but found one"
                );
            }
        }

        let table_columns_without_rw_timestamp = table_catalog.columns_without_rw_timestamp();
        let exprs = derive_default_column_project_for_sink(
            &sink_catalog,
            sink_plan.schema(),
            &table_columns_without_rw_timestamp,
            user_specified_columns,
        )?;

        let logical_project = generic::Project::new(exprs, sink_plan);

        sink_plan = StreamProject::new(logical_project).into();

        let exprs = LogicalSource::derive_output_exprs_from_generated_columns(
            &table_columns_without_rw_timestamp,
        )?;

        if let Some(exprs) = exprs {
            let logical_project = generic::Project::new(exprs, sink_plan);
            sink_plan = StreamProject::new(logical_project).into();
        }
    };

    Ok(SinkPlanContext {
        query,
        sink_plan,
        sink_catalog,
        target_table_catalog,
        dependencies,
        since_timestamp_epoch,
    })
}

// This function is used to return partition compute info for a sink. More details refer in `PartitionComputeInfo`.
// Return:
// `Some(PartitionComputeInfo)` if the sink need to compute partition.
// `None` if the sink does not need to compute partition.
pub async fn get_partition_compute_info(
    with_options: &WithOptionsSecResolved,
) -> Result<Option<PartitionComputeInfo>> {
    let (options, secret_refs) = with_options.clone().into_parts();
    let Some(connector) = options.get(UPSTREAM_SOURCE_KEY).cloned() else {
        return Ok(None);
    };
    let properties = LocalSecretManager::global().fill_secrets(options, secret_refs)?;
    match connector.as_str() {
        ICEBERG_SINK => {
            let iceberg_config = IcebergConfig::from_btreemap(properties)?;
            get_partition_compute_info_for_iceberg(&iceberg_config).await
        }
        _ => Ok(None),
    }
}

async fn get_partition_compute_info_for_iceberg(
    _iceberg_config: &IcebergConfig,
) -> Result<Option<PartitionComputeInfo>> {
    // TODO: check table if exists
    if _iceberg_config.create_table_if_not_exists {
        return Ok(None);
    }
    let table = _iceberg_config.load_table().await?;
    let partition_spec = table.metadata().default_partition_spec();
    if partition_spec.is_unpartitioned() {
        return Ok(None);
    }

    // Separate the partition spec into two parts: sparse partition and range partition.
    // Sparse partition means that the data distribution is more sparse at a given time.
    // Range partition means that the data distribution is likely same at a given time.
    // Only compute the partition and shuffle by them for the sparse partition.
    let has_sparse_partition = partition_spec.fields().iter().any(|f| match f.transform {
        // Sparse partition
        Transform::Identity | Transform::Truncate(_) | Transform::Bucket(_) => true,
        // Range partition
        Transform::Year
        | Transform::Month
        | Transform::Day
        | Transform::Hour
        | Transform::Void
        | Transform::Unknown => false,
    });
    if !has_sparse_partition {
        return Ok(None);
    }

    let arrow_type = type_to_arrow_type(&iceberg::spec::Type::Struct(
        table.metadata().default_partition_type().clone(),
    ))
    .map_err(|_| {
        RwError::from(ErrorCode::SinkError(
            "Failed to convert the Iceberg partition type to an Arrow type".into(),
        ))
    })?;
    let ArrowDataType::Struct(struct_fields) = arrow_type else {
        return Err(RwError::from(ErrorCode::SinkError(
            "The Iceberg partition type must be a struct type".into(),
        )));
    };

    let schema = table.metadata().current_schema();
    let partition_fields = partition_spec
        .fields()
        .iter()
        .map(|f| {
            let source_f =
                schema
                    .field_by_id(f.source_id)
                    .ok_or(RwError::from(ErrorCode::SinkError(
                        "Failed to look up the Iceberg partition field".into(),
                    )))?;
            Ok((source_f.name.clone(), f.transform))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(PartitionComputeInfo::Iceberg(IcebergPartitionInfo {
        partition_type: IcebergArrowConvert.struct_from_fields(&struct_fields)?,
        partition_fields,
    })))
}

pub async fn handle_create_sink(
    mut handle_args: HandlerArgs,
    stmt: CreateSinkStatement,
    is_iceberg_engine_internal: bool,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();

    session.check_cluster_limits().await?;

    let mode = if stmt.or_replace {
        prepare_replace_sink(&mut handle_args, &stmt)?
    } else {
        let if_not_exists = stmt.if_not_exists;
        if let Either::Right(resp) = session.check_relation_name_duplicated(
            stmt.sink_name.clone(),
            StatementType::CREATE_SINK,
            if_not_exists,
        )? {
            return Ok(resp);
        }

        if stmt.sink_name.base_name().starts_with(ICEBERG_SINK_PREFIX) {
            return Err(RwError::from(ErrorCode::InvalidInputSyntax(format!(
                "Sink name cannot start with reserved prefix '{}'",
                ICEBERG_SINK_PREFIX
            ))));
        }

        SinkCreateMode::Create { if_not_exists }
    };

    create_sink_or_replace(handle_args, stmt, is_iceberg_engine_internal, mode).await
}

enum SinkCreateMode {
    Create { if_not_exists: bool },
    Replace { original_sink: Arc<SinkCatalog> },
}

impl SinkCreateMode {
    fn statement_name(&self) -> &'static str {
        match self {
            SinkCreateMode::Create { .. } => "CREATE SINK",
            SinkCreateMode::Replace { .. } => "REPLACE SINK",
        }
    }
}

async fn create_sink_or_replace(
    mut handle_args: HandlerArgs,
    stmt: CreateSinkStatement,
    is_iceberg_engine_internal: bool,
    mode: SinkCreateMode,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();

    let resource_type =
        resolve_streaming_job_resource_type(session.as_ref(), &mut handle_args.with_options)?;

    let (sink, graph, dependencies, since_timestamp_epoch) = {
        let backfill_order_strategy = handle_args.with_options.backfill_order_strategy();
        let SinkPlanContext {
            query,
            sink_plan: plan,
            sink_catalog: mut sink,
            target_table_catalog,
            dependencies,
            since_timestamp_epoch,
        } = gen_sink_plan(handle_args, stmt, None, is_iceberg_engine_internal).await?;

        let has_order_by = !query.order_by.is_empty();
        if has_order_by {
            plan.ctx().warn_to_user(
                r#"The ORDER BY clause in the CREATE SINK statement has no effect at all."#
                    .to_owned(),
            );
        }

        match &mode {
            SinkCreateMode::Create { .. } => {
                if let Some(table_catalog) = &target_table_catalog {
                    sink.original_target_columns = table_catalog.columns_without_rw_timestamp();
                }
            }
            SinkCreateMode::Replace { original_sink } => {
                if target_table_catalog.is_some() {
                    return Err(ErrorCode::NotSupported(
                        "REPLACE SINK INTO TABLE is not supported yet".to_owned(),
                        "replace ordinary sinks first".to_owned(),
                    )
                    .into());
                }

                sink.schema_id = original_sink.schema_id;
                sink.database_id = original_sink.database_id;
                sink.name = original_sink.name.clone();
                sink.owner = original_sink.owner;
            }
        }

        let backfill_order =
            plan_backfill_order(session.as_ref(), backfill_order_strategy, plan.clone())?;
        let graph =
            build_graph_with_strategy(plan, Some(GraphJobType::Sink), Some(backfill_order))?;

        (sink, graph, dependencies, since_timestamp_epoch)
    };

    let statement_name = mode.statement_name();
    let catalog_writer = session.catalog_writer()?;
    match mode {
        SinkCreateMode::Create { if_not_exists } => {
            let _job_guard = session.env().creating_streaming_job_tracker().guard(
                CreatingStreamingJobInfo::new(
                    session.session_id(),
                    sink.database_id,
                    sink.schema_id,
                    sink.name.clone(),
                ),
            );

            execute_with_long_running_notification(
                catalog_writer.create_sink(
                    sink.to_proto(),
                    graph,
                    dependencies,
                    resource_type,
                    if_not_exists,
                    since_timestamp_epoch,
                ),
                &session,
                statement_name,
                LongRunningNotificationAction::MonitorBackfillJob,
            )
            .await?;
        }
        SinkCreateMode::Replace { original_sink } => {
            let original_sink_id = original_sink.id;
            execute_with_long_running_notification(
                catalog_writer.replace_sink(
                    original_sink_id,
                    sink.to_proto(),
                    graph,
                    dependencies,
                    resource_type,
                ),
                &session,
                statement_name,
                LongRunningNotificationAction::DiagnoseBarrierLatency,
            )
            .await?;

            tracing::info!(
                old_sink_id = %original_sink_id,
                sink_name = %sink.name,
                "replace sink plan submitted"
            );
        }
    }

    Ok(PgResponse::empty_result(StatementType::CREATE_SINK))
}

fn prepare_replace_sink(
    handle_args: &mut HandlerArgs,
    stmt: &CreateSinkStatement,
) -> Result<SinkCreateMode> {
    let session = handle_args.session.clone();
    if stmt.if_not_exists {
        return Err(ErrorCode::InvalidInputSyntax(
            "REPLACE SINK does not support IF NOT EXISTS".to_owned(),
        )
        .into());
    }
    if !matches!(&stmt.sink_from, CreateSink::From(_)) {
        return Err(ErrorCode::NotSupported(
            "REPLACE SINK currently only supports REPLACE SINK ... FROM table_or_mv".to_owned(),
            "use REPLACE SINK name FROM existing_relation ...".to_owned(),
        )
        .into());
    }
    if stmt.into_table_name.is_some() {
        return Err(ErrorCode::NotSupported(
            "REPLACE SINK INTO TABLE is not supported yet".to_owned(),
            "replace ordinary sinks first".to_owned(),
        )
        .into());
    }
    if handle_args
        .with_options
        .get(AUTO_SCHEMA_CHANGE_KEY)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
    {
        return Err(ErrorCode::NotSupported(
            "REPLACE SINK with auto schema change is not supported yet".to_owned(),
            "disable auto schema change for this replacement".to_owned(),
        )
        .into());
    }
    if handle_args
        .with_options
        .contains_key(SINK_SINCE_TIMESTAMP_OPTION)
    {
        return Err(ErrorCode::NotSupported(
            "REPLACE SINK with since_timestamp is not supported yet".to_owned(),
            "create a new sink with since_timestamp instead".to_owned(),
        )
        .into());
    }
    match handle_args.with_options.get(SINK_SNAPSHOT_OPTION) {
        Some(value) if !value.eq_ignore_ascii_case("false") => {
            return Err(ErrorCode::InvalidInputSyntax(
                "REPLACE SINK must not enable snapshot backfill".to_owned(),
            )
            .into());
        }
        Some(_) => {}
        None => {
            handle_args
                .with_options
                .insert(SINK_SNAPSHOT_OPTION.to_owned(), "false".to_owned());
        }
    }

    let db_name = session.database();
    let (sink_schema_name, sink_table_name) =
        Binder::resolve_schema_qualified_name(&db_name, &stmt.sink_name)?;
    let original_sink = {
        let search_path = session.config().search_path();
        let user_name = session.user_name();
        let schema_path = SchemaPath::new(sink_schema_name.as_deref(), &search_path, &user_name);
        let reader = session.env().catalog_reader().read_guard();
        let (sink, schema_name) =
            reader.get_created_sink_by_name(&db_name, schema_path, &sink_table_name)?;
        session.check_privilege_for_drop_alter(schema_name, &**sink)?;
        if sink.target_table.is_some() {
            return Err(ErrorCode::NotSupported(
                "REPLACE SINK INTO TABLE is not supported yet".to_owned(),
                "replace ordinary sinks first".to_owned(),
            )
            .into());
        }
        if sink.auto_refresh_schema_from_table.is_some() {
            return Err(ErrorCode::NotSupported(
                "REPLACE SINK with auto schema change is not supported yet".to_owned(),
                "drop and recreate this auto schema change sink".to_owned(),
            )
            .into());
        }
        if sink_is_exactly_once(&sink.properties)? {
            return Err(ErrorCode::NotSupported(
                "REPLACE SINK does not support exactly-once sinks yet".to_owned(),
                "set is_exactly_once=false or recreate the sink manually".to_owned(),
            )
            .into());
        }
        sink.clone()
    };

    Ok(SinkCreateMode::Replace { original_sink })
}

pub fn fetch_incoming_sinks(
    session: &Arc<SessionImpl>,
    table: &TableCatalog,
) -> Result<Vec<Arc<SinkCatalog>>> {
    let reader = session.env().catalog_reader().read_guard();
    let schema = reader.get_schema_by_id(table.database_id, table.schema_id)?;
    let Some(incoming_sinks) = schema.table_incoming_sinks(table.id) else {
        return Ok(vec![]);
    };
    let mut sinks = vec![];
    for sink_id in incoming_sinks {
        sinks.push(
            schema
                .get_sink_by_id(*sink_id)
                .expect("should exist")
                .clone(),
        );
    }
    Ok(sinks)
}

fn derive_sink_to_table_expr(
    sink_schema: &Schema,
    idx: usize,
    target_type: &DataType,
) -> Result<ExprImpl> {
    let input_type = &sink_schema.fields()[idx].data_type;

    if !target_type.equals_datatype(input_type) {
        bail!(
            "column type mismatch: {:?} vs {:?}, column name: {:?}",
            target_type,
            input_type,
            sink_schema.fields()[idx].name
        );
    } else {
        Ok(ExprImpl::InputRef(Box::new(InputRef::new(
            idx,
            input_type.clone(),
        ))))
    }
}

pub(crate) fn derive_default_column_project_for_sink(
    sink: &SinkCatalog,
    sink_schema: &Schema,
    columns: &[ColumnCatalog],
    user_specified_columns: bool,
) -> Result<Vec<ExprImpl>> {
    assert_eq!(sink.full_schema().len(), sink_schema.len());

    let default_column_exprs = TableCatalog::default_column_exprs(columns);

    let mut exprs = vec![];

    let sink_visible_col_idxes = sink
        .full_columns()
        .iter()
        .positions(|c| !c.is_hidden())
        .collect_vec();
    let sink_visible_col_idxes_by_name = sink
        .full_columns()
        .iter()
        .enumerate()
        .filter(|(_, c)| !c.is_hidden())
        .map(|(i, c)| (c.name(), i))
        .collect::<BTreeMap<_, _>>();

    for (idx, column) in columns.iter().enumerate() {
        if !column.can_dml() {
            continue;
        }

        let default_col_expr =
            || -> ExprImpl { rewrite_now_to_proctime(default_column_exprs[idx].clone()) };

        let sink_col_expr = |sink_col_idx: usize| -> Result<ExprImpl> {
            derive_sink_to_table_expr(sink_schema, sink_col_idx, column.data_type())
        };

        // If users specified the columns to be inserted e.g. `CREATE SINK s INTO t(a, b)`, the expressions of `Project` will be generated accordingly.
        // The missing columns will be filled with default value (`null` if not explicitly defined).
        // Otherwise, e.g. `CREATE SINK s INTO t`, the columns will be matched by their order in `select` query and the target table.
        if user_specified_columns {
            if let Some(idx) = sink_visible_col_idxes_by_name.get(column.name()) {
                exprs.push(sink_col_expr(*idx)?);
            } else {
                exprs.push(default_col_expr());
            }
        } else {
            if idx < sink_visible_col_idxes.len() {
                exprs.push(sink_col_expr(sink_visible_col_idxes[idx])?);
            } else {
                exprs.push(default_col_expr());
            };
        }
    }
    Ok(exprs)
}

/// Transforms the (format, encode, options) from sqlparser AST into an internal struct `SinkFormatDesc`.
/// This is an analogy to (part of) [`crate::handler::create_source::bind_columns_from_source`]
/// which transforms sqlparser AST `SourceSchemaV2` into `StreamSourceInfo`.
fn bind_sink_format_desc(
    session: &SessionImpl,
    value: FormatEncodeOptions,
) -> Result<SinkFormatDesc> {
    use risingwave_connector::sink::catalog::{SinkEncode, SinkFormat};
    use risingwave_connector::sink::encoder::TimestamptzHandlingMode;
    use risingwave_sqlparser::ast::{Encode as E, Format as F};

    let format = match value.format {
        F::Plain => SinkFormat::AppendOnly,
        F::Upsert => SinkFormat::Upsert,
        F::Debezium => SinkFormat::Debezium,
        f @ (F::Native | F::DebeziumMongo | F::Maxwell | F::Canal | F::None) => {
            return Err(ErrorCode::BindError(format!("sink format unsupported: {f}")).into());
        }
    };
    let encode = match value.row_encode {
        E::Json => SinkEncode::Json,
        E::Protobuf => SinkEncode::Protobuf,
        E::Avro => SinkEncode::Avro,
        E::Template => SinkEncode::Template,
        E::Parquet => SinkEncode::Parquet,
        E::Bytes => SinkEncode::Bytes,
        e @ (E::Native | E::Csv | E::None | E::Text) => {
            return Err(ErrorCode::BindError(format!("sink encode unsupported: {e}")).into());
        }
    };

    let mut key_encode = None;
    if let Some(encode) = value.key_encode {
        match encode {
            E::Text => key_encode = Some(SinkEncode::Text),
            E::Bytes => key_encode = Some(SinkEncode::Bytes),
            _ => {
                return Err(ErrorCode::BindError(format!(
                    "sink key encode unsupported: {encode}, only TEXT and BYTES supported"
                ))
                .into());
            }
        }
    }

    let (props, connection_type_flag, schema_registry_conn_ref) =
        resolve_connection_ref_and_secret_ref(
            WithOptions::try_from(value.row_options.as_slice())?,
            session,
            Some(TelemetryDatabaseObject::Sink),
        )?;
    ensure_connection_type_allowed(
        connection_type_flag,
        &SINK_ALLOWED_CONNECTION_SCHEMA_REGISTRY,
    )?;
    let (mut options, secret_refs) = props.into_parts();

    options
        .entry(TimestamptzHandlingMode::OPTION_KEY.to_owned())
        .or_insert(TimestamptzHandlingMode::FRONTEND_DEFAULT.to_owned());

    Ok(SinkFormatDesc {
        format,
        encode,
        options,
        secret_refs,
        key_encode,
        connection_id: schema_registry_conn_ref,
    })
}

static CONNECTORS_COMPATIBLE_FORMATS: LazyLock<HashMap<String, HashMap<Format, Vec<Encode>>>> =
    LazyLock::new(|| {
        use risingwave_connector::sink::Sink as _;
        use risingwave_connector::sink::file_sink::azblob::AzblobSink;
        use risingwave_connector::sink::file_sink::fs::FsSink;
        use risingwave_connector::sink::file_sink::gcs::GcsSink;
        use risingwave_connector::sink::file_sink::opendal_sink::FileSink;
        use risingwave_connector::sink::file_sink::s3::S3Sink;
        use risingwave_connector::sink::file_sink::webhdfs::WebhdfsSink;
        use risingwave_connector::sink::google_pubsub::GooglePubSubSink;
        use risingwave_connector::sink::kafka::KafkaSink;
        use risingwave_connector::sink::kinesis::KinesisSink;
        use risingwave_connector::sink::mqtt::MqttSink;
        use risingwave_connector::sink::pulsar::PulsarSink;
        use risingwave_connector::sink::redis::RedisSink;

        convert_args!(hashmap!(
                GooglePubSubSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json],
                ),
                KafkaSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Avro, Encode::Protobuf, Encode::Bytes],
                    Format::Upsert => vec![Encode::Json, Encode::Avro, Encode::Protobuf],
                    Format::Debezium => vec![Encode::Json],
                ),
                FileSink::<S3Sink>::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Parquet, Encode::Json],
                ),
                FileSink::<SnowflakeSink>::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Parquet, Encode::Json],
                ),
                FileSink::<GcsSink>::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Parquet, Encode::Json],
                ),
                FileSink::<AzblobSink>::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Parquet, Encode::Json],
                ),
                FileSink::<WebhdfsSink>::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Parquet, Encode::Json],
                ),
                FileSink::<FsSink>::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Parquet, Encode::Json],
                ),
                KinesisSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json],
                    Format::Upsert => vec![Encode::Json],
                    Format::Debezium => vec![Encode::Json],
                ),
                MqttSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf],
                ),
                PulsarSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json],
                    Format::Upsert => vec![Encode::Json],
                    Format::Debezium => vec![Encode::Json],
                ),
                RedisSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Template],
                    Format::Upsert => vec![Encode::Json, Encode::Template],
                ),
        ))
    });

pub fn validate_compatibility(connector: &str, format_desc: &FormatEncodeOptions) -> Result<()> {
    let compatible_formats = CONNECTORS_COMPATIBLE_FORMATS
        .get(connector)
        .ok_or_else(|| {
            ErrorCode::BindError(format!(
                "connector {} is not supported by FORMAT ... ENCODE ... syntax",
                connector
            ))
        })?;
    let compatible_encodes = compatible_formats.get(&format_desc.format).ok_or_else(|| {
        ErrorCode::BindError(format!(
            "connector {} does not support format {:?}",
            connector, format_desc.format
        ))
    })?;
    if !compatible_encodes.contains(&format_desc.row_encode) {
        return Err(ErrorCode::BindError(format!(
            "connector {} does not support format {:?} with encode {:?}",
            connector, format_desc.format, format_desc.row_encode
        ))
        .into());
    }

    // only allow Kafka connector work with `bytes` as key encode
    if let Some(encode) = &format_desc.key_encode
        && connector != KAFKA_SINK
        && matches!(encode, Encode::Bytes)
    {
        return Err(ErrorCode::BindError(format!(
            "key encode bytes only works with kafka connector, but found {}",
            connector
        ))
        .into());
    }

    Ok(())
}

#[cfg(test)]
pub mod tests {
    use std::collections::BTreeMap;

    use risingwave_common::catalog::{CreateType, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use risingwave_common::config::FrontendConfig;

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::{LocalFrontend, PROTO_FILE_DATA, create_proto_file};

    #[test]
    fn test_sink_replace_requires_exactly_once_state_defaults() {
        let properties = BTreeMap::from([("connector".to_owned(), "iceberg".to_owned())]);
        assert!(super::sink_is_exactly_once(&properties).unwrap());

        let properties = BTreeMap::from([
            ("connector".to_owned(), "iceberg".to_owned()),
            ("is_exactly_once".to_owned(), "false".to_owned()),
        ]);
        assert!(!super::sink_is_exactly_once(&properties).unwrap());

        let properties = BTreeMap::from([
            ("connector".to_owned(), "jdbc".to_owned()),
            ("is_exactly_once".to_owned(), "true".to_owned()),
        ]);
        assert!(!super::sink_is_exactly_once(&properties).unwrap());
    }

    #[tokio::test]
    async fn test_create_sink_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t1
    WITH (connector = 'kafka', kafka.topic = 'abc', kafka.brokers = 'localhost:1001')
    FORMAT PLAIN ENCODE PROTOBUF (message = '.test.TestRecord', schema.location = 'file://{}')"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let sql = "create materialized view mv1 as select t1.country from t1;";
        frontend.run_sql(sql).await.unwrap();

        let sql = r#"CREATE SINK snk1 FROM mv1
                    WITH (connector = 'jdbc', mysql.endpoint = '127.0.0.1:3306', mysql.table =
                        '<table_name>', mysql.database = '<database_name>', mysql.user = '<user_name>',
                        mysql.password = '<password>', type = 'append-only', force_append_only = 'true');"#.to_owned();
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t1")
            .unwrap();
        assert_eq!(source.name, "t1");

        // Check table exists.
        let (table, schema_name) = catalog_reader
            .get_created_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "mv1")
            .unwrap();
        assert_eq!(table.name(), "mv1");
        let schema_name = schema_name.to_owned();

        // Check sink exists.
        let (sink, _) = catalog_reader
            .get_created_sink_by_name(
                DEFAULT_DATABASE_NAME,
                SchemaPath::Name(&schema_name),
                "snk1",
            )
            .unwrap();
        assert_eq!(sink.name, "snk1");
        drop(catalog_reader);

        let sql = r#"REPLACE SINK snk1 FROM mv1
                    WITH (connector = 'jdbc', mysql.endpoint = '127.0.0.1:3306', mysql.table =
                        '<table_name>', mysql.database = '<database_name>', mysql.user = '<user_name>',
                        mysql.password = '<password>', type = 'append-only', force_append_only = 'true');"#.to_owned();
        frontend.run_sql(sql).await.unwrap();

        let catalog_reader = session.env().catalog_reader().read_guard();
        let (sink, _) = catalog_reader
            .get_created_sink_by_name(
                DEFAULT_DATABASE_NAME,
                SchemaPath::Name(&schema_name),
                "snk1",
            )
            .unwrap();
        assert_eq!(sink.name, "snk1");
        // Frontend leaves the replacement job foreground for the meta foreground wait path.
        // Meta switches it to background when marking the job Creating during cutover.
        assert_eq!(sink.create_type, CreateType::Foreground);
    }

    #[tokio::test]
    async fn test_create_fs_sink_requires_frontend_config() {
        let frontend = LocalFrontend::with_frontend_config(
            Default::default(),
            FrontendConfig {
                unsafe_enable_local_fs_connector: false,
                ..Default::default()
            },
        )
        .await;
        frontend.run_sql("CREATE TABLE t(v int);").await.unwrap();
        frontend
            .run_sql("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;")
            .await
            .unwrap();

        let err = frontend
            .run_sql(
                r#"CREATE SINK local_sink FROM mv
                    WITH (
                        connector = 'fs',
                        fs.path = '/tmp/rw-local-sink',
                        type = 'append-only',
                        force_append_only = 'true'
                    ) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');"#
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
}
