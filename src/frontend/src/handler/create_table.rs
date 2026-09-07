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
use std::sync::Arc;

use anyhow::{Context, anyhow};
use clap::ValueEnum;
use either::Either;
use fancy_regex::Regex;
use fixedbitset::FixedBitSet;
use iceberg::spec::FormatVersion;
use itertools::Itertools;
use percent_encoding::percent_decode_str;
use pgwire::pg_response::{PgResponse, StatementType};
use prost::Message as _;
use risingwave_common::acl::AclMode;
use risingwave_common::catalog::{
    CdcTableDesc, ColumnCatalog, ColumnDesc, ConflictBehavior, DEFAULT_SCHEMA_NAME, Engine,
    ICEBERG_SINK_PREFIX, ICEBERG_SOURCE_PREFIX, RISINGWAVE_ICEBERG_ROW_ID, ROW_ID_COLUMN_NAME,
    TableId,
};
use risingwave_common::config::MetaBackend;
use risingwave_common::global_jvm::Jvm;
use risingwave_common::session_config::sink_decouple::SinkDecouple;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_common::util::value_encoding::DatumToProtoExt;
use risingwave_common::{bail, bail_not_implemented};
use risingwave_connector::source::cdc::external::{
    DATABASE_NAME_KEY, ExternalCdcTableType, ExternalTableConfig, ExternalTableImpl,
    SCHEMA_NAME_KEY, SchemaTableName, TABLE_NAME_KEY,
};
use risingwave_connector::source::cdc::{
    build_cdc_table_id, normalize_simple_postgres_quoted_table_name,
};
use risingwave_connector::{
    AUTO_SCHEMA_CHANGE_KEY, WithOptionsSecResolved, WithPropertiesExt, source,
};
use risingwave_pb::catalog::connection::Info as ConnectionInfo;
use risingwave_pb::catalog::connection_params::ConnectionType;
use risingwave_pb::catalog::{PbSource, PbWebhookSourceInfo, WatermarkDesc};
use risingwave_pb::ddl_service::{PbTableJobType, TableJobType};
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_pb::plan_common::{
    AdditionalColumn, ColumnDescVersion, DefaultColumnDesc, GeneratedColumnDesc,
};
use risingwave_pb::secret::PbSecretRef;
use risingwave_pb::secret::secret_ref::PbRefAsType;
use risingwave_pb::stream_plan::StreamFragmentGraph;
use risingwave_sqlparser::ast::{
    CdcTableInfo, ColumnDef, ColumnOption, CompatibleFormatEncode, ConnectionRefValue, CreateSink,
    CreateSinkStatement, CreateSourceStatement, DataType as AstDataType, ExplainOptions, Format,
    FormatEncodeOptions, Ident, ObjectName, OnConflict, SecretRefAsType, SourceWatermark,
    Statement, TableConstraint, WebhookSourceInfo, WithProperties,
};
use risingwave_sqlparser::parser::IncludeOption;
use thiserror_ext::AsReport;

use super::RwPgResponse;
use super::create_source::{CreateSourceType, SqlColumnStrategy, bind_columns_from_source};
use crate::binder::{Clause, SecureCompareContext, WEBHOOK_PAYLOAD_FIELD_NAME, bind_data_type};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::table_catalog::TableVersion;
use crate::catalog::{ColumnId, DatabaseId, SchemaId, SourceId, check_column_name_not_reserved};
use crate::error::{ErrorCode, Result, RwError, bail_bind_error};
use crate::expr::{Expr, ExprImpl, ExprRewriter};
use crate::handler::HandlerArgs;
use crate::handler::create_source::{
    UPSTREAM_SOURCE_KEY, bind_connector_props, bind_create_source_or_table_with_connector,
    bind_source_watermark, handle_addition_columns, reject_variant_columns,
};
use crate::handler::privilege::ObjectCheckItem;
use crate::handler::util::{
    LongRunningNotificationAction, SourceSchemaCompatExt, execute_with_long_running_notification,
};
use crate::optimizer::plan_node::generic::{SourceNodeKind, build_cdc_scan_options_with_options};
use crate::optimizer::plan_node::{
    LogicalCdcScan, LogicalPlanRef, LogicalSource, StreamPlanRef as PlanRef,
    ensure_sync_log_store_fragment_root,
};
use crate::optimizer::property::{Order, RequiredDist};
use crate::optimizer::{OptimizerContext, OptimizerContextRef, PlanRoot};
use crate::session::SessionImpl;
use crate::session::current::notice_to_user;
use crate::stream_fragmenter::{GraphJobType, build_graph};
use crate::utils::OverwriteOptions;
use crate::{Binder, Explain, TableCatalog, WithOptions};

mod col_id_gen;
pub use col_id_gen::*;
use risingwave_connector::sink::SinkParam;
use risingwave_connector::sink::iceberg::{
    ENABLE_COMPACTION, ENABLE_MANIFEST_REWRITE, IcebergConfig, IcebergSink,
    is_iceberg_engine_option, parse_partition_by_exprs, validate_order_key_columns,
};
use risingwave_pb::ddl_service::create_iceberg_table_request::{PbSinkJobInfo, PbTableJobInfo};

use crate::handler::create_sink::{SinkPlanContext, gen_sink_plan};

fn ensure_column_options_supported(c: &ColumnDef) -> Result<()> {
    for option_def in &c.options {
        match option_def.option {
            ColumnOption::GeneratedColumns(_) => {}
            ColumnOption::DefaultValue(_) => {}
            ColumnOption::DefaultValueInternal { .. } => {}
            ColumnOption::Unique { is_primary: true } => {}
            ColumnOption::Null => {}
            ColumnOption::NotNull => {}
            _ => bail_not_implemented!("column constraints \"{}\"", option_def),
        }
    }
    Ok(())
}

/// Binds the column schemas declared in CREATE statement into `ColumnDesc`.
/// If a column is marked as `primary key`, its `ColumnId` is also returned.
/// This primary key is not combined with table constraints yet.
pub fn bind_sql_columns(
    column_defs: &[ColumnDef],
    is_for_drop_table_connector: bool,
) -> Result<Vec<ColumnCatalog>> {
    let mut columns = Vec::with_capacity(column_defs.len());

    for column in column_defs {
        ensure_column_options_supported(column)?;
        // Destruct to make sure all fields are properly handled rather than ignored.
        // Do NOT use `..` to ignore fields you do not want to deal with.
        // Reject them with a clear NotImplemented error.
        let ColumnDef {
            name,
            data_type,
            collation,
            options,
            ..
        } = column;

        let data_type = data_type
            .clone()
            .ok_or_else(|| ErrorCode::InvalidInputSyntax("data type is not specified".into()))?;
        if let Some(collation) = collation {
            // PostgreSQL will limit the datatypes that collate can work on.
            // https://www.postgresql.org/docs/16/collation.html#COLLATION-CONCEPTS
            //   > The built-in collatable data types are `text`, `varchar`, and `char`.
            //
            // But we don't support real collation, we simply ignore it here.
            if !["C", "POSIX"].contains(&collation.real_value().as_str()) {
                bail_not_implemented!(
                    "Collate collation other than `C` or `POSIX` is not implemented"
                );
            }

            match data_type {
                AstDataType::Text | AstDataType::Varchar | AstDataType::Char(_) => {}
                _ => {
                    return Err(ErrorCode::NotSupported(
                        format!("{} is not a collatable data type", data_type),
                        "The only built-in collatable data types are `varchar`, please check your type".into(),
                    ).into());
                }
            }
        }

        if !is_for_drop_table_connector {
            // additional column name may have prefix _rw
            // When converting dropping the connector from table, the additional columns are converted to normal columns and keep the original name.
            // Under this case, we loosen the check for _rw prefix.
            check_column_name_not_reserved(&name.real_value())?;
        }

        let nullable: bool = !options
            .iter()
            .any(|def| matches!(def.option, ColumnOption::NotNull));

        columns.push(ColumnCatalog {
            column_desc: ColumnDesc {
                data_type: bind_data_type(&data_type)?,
                column_id: ColumnId::placeholder(),
                name: name.real_value(),
                generated_or_default_column: None,
                description: None,
                additional_column: AdditionalColumn { column_type: None },
                version: ColumnDescVersion::LATEST,
                system_column: None,
                nullable,
            },
            is_hidden: false,
        });
    }

    Ok(columns)
}

fn check_generated_column_constraints(
    column_name: &String,
    column_id: ColumnId,
    expr: &ExprImpl,
    column_catalogs: &[ColumnCatalog],
    generated_column_names: &[String],
    pk_column_ids: &[ColumnId],
) -> Result<()> {
    let input_refs = expr.collect_input_refs(column_catalogs.len());
    for idx in input_refs.ones() {
        let referred_generated_column = &column_catalogs[idx].column_desc.name;
        if generated_column_names
            .iter()
            .any(|c| c == referred_generated_column)
        {
            return Err(ErrorCode::BindError(format!(
                "A generated column cannot reference another generated column. \
                Generated column \"{}\" references generated column \"{}\"",
                column_name, referred_generated_column
            ))
            .into());
        }
    }

    if pk_column_ids.contains(&column_id) && expr.is_impure() {
        return Err(ErrorCode::BindError(format!(
            "Generated columns with impure expressions should not be part of the primary key. \
            Here column \"{}\" is defined as part of the primary key.",
            column_name
        ))
        .into());
    }

    Ok(())
}

/// Binds constraints that can be only specified in column definitions,
/// currently generated columns and default columns.
pub fn bind_sql_column_constraints(
    session: &SessionImpl,
    table_name: String,
    column_catalogs: &mut [ColumnCatalog],
    columns: &[ColumnDef],
    pk_column_ids: &[ColumnId],
) -> Result<()> {
    let generated_column_names = {
        let mut names = vec![];
        for column in columns {
            for option_def in &column.options {
                if let ColumnOption::GeneratedColumns(_) = option_def.option {
                    names.push(column.name.real_value());
                    break;
                }
            }
        }
        names
    };

    let mut binder = Binder::new_for_ddl(session);
    binder.bind_columns_to_context(table_name, column_catalogs)?;

    for column in columns {
        let Some(idx) = column_catalogs
            .iter()
            .position(|c| c.name() == column.name.real_value())
        else {
            // It's possible that we don't follow the user defined columns in SQL but take the
            // ones resolved from the source, thus missing some columns. Simply ignore them.
            continue;
        };

        for option_def in &column.options {
            match &option_def.option {
                ColumnOption::GeneratedColumns(expr) => {
                    binder.set_clause(Some(Clause::GeneratedColumn));

                    let expr_impl = binder.bind_expr(expr).with_context(|| {
                        format!(
                            "failed to bind the expression in generated column \"{}\"",
                            column.name.real_value()
                        )
                    })?;

                    check_generated_column_constraints(
                        &column.name.real_value(),
                        column_catalogs[idx].column_id(),
                        &expr_impl,
                        column_catalogs,
                        &generated_column_names,
                        pk_column_ids,
                    )?;

                    column_catalogs[idx].column_desc.generated_or_default_column = Some(
                        GeneratedOrDefaultColumn::GeneratedColumn(GeneratedColumnDesc {
                            expr: Some(expr_impl.to_expr_proto()),
                        }),
                    );
                    binder.set_clause(None);
                }
                ColumnOption::DefaultValue(expr) => {
                    let expr_impl = binder
                        .bind_expr(expr)?
                        .cast_assign(column_catalogs[idx].data_type())?;

                    // Rewrite expressions to evaluate a snapshot value, used for missing values in the case of
                    // schema change.
                    //
                    // TODO: Currently we don't support impure expressions other than `now()` (like `random()`),
                    // so the rewritten expression should almost always be pure and we directly call `fold_const`
                    // here. Actually we do not require purity of the expression here since we're only to get a
                    // snapshot value.
                    let rewritten_expr_impl = session
                        .pinned_snapshot()
                        .inline_now_proc_time()
                        .rewrite_expr(expr_impl.clone());

                    if let Some(snapshot_value) = rewritten_expr_impl.try_fold_const() {
                        let snapshot_value = snapshot_value?;

                        column_catalogs[idx].column_desc.generated_or_default_column =
                            Some(GeneratedOrDefaultColumn::DefaultColumn(DefaultColumnDesc {
                                snapshot_value: Some(snapshot_value.to_protobuf()),
                                expr: Some(expr_impl.to_expr_proto()),
                                // persist the original expression
                            }));
                    } else {
                        return Err(ErrorCode::BindError(format!(
                            "Default expression used in column `{}` cannot be evaluated. \
                            Use generated columns instead if you mean to reference other columns.",
                            column.name
                        ))
                        .into());
                    }
                }
                ColumnOption::DefaultValueInternal { persisted, expr: _ } => {
                    // When a `DEFAULT INTERNAL` is used internally for schema change, the persisted value
                    // should already be set during purifcation. So if we encounter an empty value here, it
                    // means the user has specified it explicitly in the SQL statement, typically by
                    // directly copying the result of `SHOW CREATE TABLE` and executing it.
                    if persisted.is_empty() {
                        bail_bind_error!(
                            "DEFAULT INTERNAL is only used for internal purposes, \
                             please specify a concrete default value"
                        );
                    }

                    let desc = DefaultColumnDesc::decode(&**persisted)
                        .expect("failed to decode persisted `DefaultColumnDesc`");

                    column_catalogs[idx].column_desc.generated_or_default_column =
                        Some(GeneratedOrDefaultColumn::DefaultColumn(desc));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// Currently we only support Primary key table constraint, so just return pk names if it exists
pub fn bind_table_constraints(table_constraints: &[TableConstraint]) -> Result<Vec<String>> {
    let mut pk_column_names = vec![];

    for constraint in table_constraints {
        match constraint {
            TableConstraint::Unique {
                name: _,
                columns,
                is_primary: true,
            } => {
                if !pk_column_names.is_empty() {
                    return Err(multiple_pk_definition_err());
                }
                pk_column_names = columns.iter().map(|c| c.real_value()).collect_vec();
            }
            _ => bail_not_implemented!("table constraint \"{}\"", constraint),
        }
    }
    Ok(pk_column_names)
}

pub fn bind_sql_pk_names(
    columns_defs: &[ColumnDef],
    pk_names_from_table_constraints: Vec<String>,
) -> Result<Vec<String>> {
    let mut pk_column_names = pk_names_from_table_constraints;

    for column in columns_defs {
        for option_def in &column.options {
            if let ColumnOption::Unique { is_primary: true } = option_def.option {
                if !pk_column_names.is_empty() {
                    return Err(multiple_pk_definition_err());
                }
                pk_column_names.push(column.name.real_value());
            };
        }
    }

    Ok(pk_column_names)
}

fn multiple_pk_definition_err() -> RwError {
    ErrorCode::BindError("multiple primary keys are not allowed".into()).into()
}

/// Binds primary keys defined in SQL.
///
/// It returns the columns together with `pk_column_ids`, and an optional row id column index if
/// added.
pub fn bind_pk_and_row_id_on_relation(
    mut columns: Vec<ColumnCatalog>,
    pk_names: Vec<String>,
    must_need_pk: bool,
) -> Result<(Vec<ColumnCatalog>, Vec<ColumnId>, Option<usize>)> {
    for c in &columns {
        assert!(c.column_id() != ColumnId::placeholder());
    }

    // Mapping from column name to column id.
    let name_to_id = columns
        .iter()
        .map(|c| (c.name(), c.column_id()))
        .collect::<HashMap<_, _>>();

    let mut pk_column_ids: Vec<_> = pk_names
        .iter()
        .map(|name| {
            name_to_id.get(name.as_str()).copied().ok_or_else(|| {
                ErrorCode::BindError(format!("column \"{name}\" named in key does not exist"))
            })
        })
        .try_collect()?;

    // Add `_row_id` column if `pk_column_ids` is empty and must_need_pk
    let need_row_id = pk_column_ids.is_empty() && must_need_pk;

    let row_id_index = need_row_id.then(|| {
        let column = ColumnCatalog::row_id_column();
        let index = columns.len();
        pk_column_ids = vec![column.column_id()];
        columns.push(column);
        index
    });

    if let Some(col) = columns.iter().map(|c| c.name()).duplicates().next() {
        Err(ErrorCode::InvalidInputSyntax(format!(
            "column \"{col}\" specified more than once"
        )))?;
    }

    Ok((columns, pk_column_ids, row_id_index))
}

/// `gen_create_table_plan_with_source` generates the plan for creating a table with an external
/// stream source.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn gen_create_table_plan_with_source(
    mut handler_args: HandlerArgs,
    explain_options: ExplainOptions,
    table_name: ObjectName,
    column_defs: Vec<ColumnDef>,
    wildcard_idx: Option<usize>,
    constraints: Vec<TableConstraint>,
    format_encode: FormatEncodeOptions,
    source_watermarks: Vec<SourceWatermark>,
    mut col_id_gen: ColumnIdGenerator,
    include_column_options: IncludeOption,
    props: CreateTableProps,
    sql_column_strategy: SqlColumnStrategy,
) -> Result<(PlanRef, Option<SourceCatalog>, TableCatalog)> {
    if props.append_only
        && format_encode.format != Format::Plain
        && format_encode.format != Format::Native
    {
        return Err(ErrorCode::BindError(format!(
            "Append only table does not support format {}.",
            format_encode.format
        ))
        .into());
    }

    let session = &handler_args.session;
    let (with_properties, refresh_mode) =
        bind_connector_props(&handler_args, &format_encode, false)?;
    if with_properties.is_shareable_cdc_connector() {
        generated_columns_check_for_cdc_table(&column_defs)?;
        not_null_check_for_cdc_table(&wildcard_idx, &column_defs)?;
    } else if column_defs.iter().any(|col| {
        col.options
            .iter()
            .any(|def| matches!(def.option, ColumnOption::NotNull))
    }) {
        // if non-cdc source
        notice_to_user(
            "The table contains columns with NOT NULL constraints. Any rows from upstream violating the constraints will be ignored silently.",
        );
    }

    let db_name: &str = &session.database();
    let (schema_name, _) = Binder::resolve_schema_qualified_name(db_name, &table_name)?;

    // TODO: omit this step if `sql_column_strategy` is `Follow`.
    let (columns_from_resolve_source, source_info) = bind_columns_from_source(
        session,
        &format_encode,
        Either::Left(&with_properties),
        CreateSourceType::Table,
    )
    .await?;

    let overwrite_options = OverwriteOptions::new(&mut handler_args);
    let rate_limit = overwrite_options.source_rate_limit;
    let source = bind_create_source_or_table_with_connector(
        handler_args.clone(),
        table_name,
        format_encode,
        with_properties,
        &column_defs,
        constraints,
        wildcard_idx,
        source_watermarks,
        columns_from_resolve_source,
        source_info,
        include_column_options,
        &mut col_id_gen,
        CreateSourceType::Table,
        rate_limit,
        sql_column_strategy,
        refresh_mode,
    )
    .await?;

    let context = OptimizerContext::new(handler_args, explain_options);

    let (plan, table) = gen_table_plan_with_source(
        context.into(),
        schema_name,
        source.clone(),
        col_id_gen.into_version(),
        props,
    )?;

    Ok((plan, Some(source), table))
}

/// `gen_create_table_plan` generates the plan for creating a table without an external stream
/// source.
#[allow(clippy::too_many_arguments)]
pub(crate) fn gen_create_table_plan(
    context: OptimizerContext,
    table_name: ObjectName,
    column_defs: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    mut col_id_gen: ColumnIdGenerator,
    source_watermarks: Vec<SourceWatermark>,
    props: CreateTableProps,
    is_for_replace_plan: bool,
) -> Result<(PlanRef, TableCatalog)> {
    let mut columns = bind_sql_columns(&column_defs, is_for_replace_plan)?;
    for c in &mut columns {
        col_id_gen.generate(c)?;
    }

    let (_, secret_refs, connection_refs) = context.with_options().clone().into_parts();
    if !secret_refs.is_empty() || !connection_refs.is_empty() {
        return Err(crate::error::ErrorCode::InvalidParameterValue("Secret reference and Connection reference are not allowed in options when creating table without external source".to_owned()).into());
    }

    gen_create_table_plan_without_source(
        context,
        table_name,
        columns,
        column_defs,
        constraints,
        source_watermarks,
        col_id_gen.into_version(),
        props,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn gen_create_table_plan_without_source(
    context: OptimizerContext,
    table_name: ObjectName,
    columns: Vec<ColumnCatalog>,
    column_defs: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    source_watermarks: Vec<SourceWatermark>,
    version: TableVersion,
    props: CreateTableProps,
) -> Result<(PlanRef, TableCatalog)> {
    // XXX: Why not bind outside?
    let pk_names = bind_sql_pk_names(&column_defs, bind_table_constraints(&constraints)?)?;
    let (mut columns, pk_column_ids, row_id_index) =
        bind_pk_and_row_id_on_relation(columns, pk_names, true)?;

    let watermark_descs = bind_source_watermark(
        context.session_ctx(),
        table_name.real_value(),
        source_watermarks,
        &columns,
    )?;

    bind_sql_column_constraints(
        context.session_ctx(),
        table_name.real_value(),
        &mut columns,
        &column_defs,
        &pk_column_ids,
    )?;
    let session = context.session_ctx().clone();

    let db_name = &session.database();
    let (schema_name, table_name) = Binder::resolve_schema_qualified_name(db_name, &table_name)?;

    let info = CreateTableInfo {
        columns,
        pk_column_ids,
        row_id_index,
        watermark_descs,
        source_catalog: None,
        version,
    };

    gen_table_plan_inner(context.into(), schema_name, table_name, info, props)
}

fn gen_table_plan_with_source(
    context: OptimizerContextRef,
    schema_name: Option<String>,
    source_catalog: SourceCatalog,
    version: TableVersion,
    props: CreateTableProps,
) -> Result<(PlanRef, TableCatalog)> {
    let table_name = source_catalog.name.clone();

    let info = CreateTableInfo {
        columns: source_catalog.columns.clone(),
        pk_column_ids: source_catalog.pk_col_ids.clone(),
        row_id_index: source_catalog.row_id_index,
        watermark_descs: source_catalog.watermark_descs.clone(),
        source_catalog: Some(source_catalog),
        version,
    };

    gen_table_plan_inner(context, schema_name, table_name, info, props)
}

/// On-conflict behavior either from user input or existing table catalog.
#[derive(Clone, Copy)]
pub enum EitherOnConflict {
    Ast(Option<OnConflict>),
    Resolved(ConflictBehavior),
}

impl From<Option<OnConflict>> for EitherOnConflict {
    fn from(v: Option<OnConflict>) -> Self {
        Self::Ast(v)
    }
}

impl From<ConflictBehavior> for EitherOnConflict {
    fn from(v: ConflictBehavior) -> Self {
        Self::Resolved(v)
    }
}

impl EitherOnConflict {
    /// Resolves the conflict behavior based on the given information.
    pub fn to_behavior(self, append_only: bool, row_id_as_pk: bool) -> Result<ConflictBehavior> {
        let conflict_behavior = match self {
            EitherOnConflict::Ast(on_conflict) => {
                if append_only {
                    if row_id_as_pk {
                        // Primary key will be generated, no conflict check needed.
                        ConflictBehavior::NoCheck
                    } else {
                        // User defined PK on append-only table, enforce `DO NOTHING`.
                        if let Some(on_conflict) = on_conflict
                            && on_conflict != OnConflict::Nothing
                        {
                            return Err(ErrorCode::InvalidInputSyntax(
                                "When PRIMARY KEY constraint applied to an APPEND ONLY table, \
                                     the ON CONFLICT behavior must be DO NOTHING."
                                    .to_owned(),
                            )
                            .into());
                        }
                        ConflictBehavior::IgnoreConflict
                    }
                } else {
                    // Default to `UPDATE FULL` for non-append-only tables.
                    match on_conflict.unwrap_or(OnConflict::UpdateFull) {
                        OnConflict::UpdateFull => ConflictBehavior::Overwrite,
                        OnConflict::Nothing => ConflictBehavior::IgnoreConflict,
                        OnConflict::UpdateIfNotNull => ConflictBehavior::DoUpdateIfNotNull,
                    }
                }
            }
            EitherOnConflict::Resolved(b) => b,
        };

        Ok(conflict_behavior)
    }
}

/// Arguments of the functions that generate a table plan, part 1.
///
/// Compared to [`CreateTableProps`], this struct contains fields that need some work of binding
/// or resolving based on the user input.
pub struct CreateTableInfo {
    pub columns: Vec<ColumnCatalog>,
    pub pk_column_ids: Vec<ColumnId>,
    pub row_id_index: Option<usize>,
    pub watermark_descs: Vec<WatermarkDesc>,
    pub source_catalog: Option<SourceCatalog>,
    pub version: TableVersion,
}

/// Arguments of the functions that generate a table plan, part 2.
///
/// Compared to [`CreateTableInfo`], this struct contains fields that can be (relatively) simply
/// obtained from the input or the context.
pub struct CreateTableProps {
    pub definition: String,
    pub append_only: bool,
    pub on_conflict: EitherOnConflict,
    pub with_version_columns: Vec<String>,
    pub webhook_info: Option<PbWebhookSourceInfo>,
    pub engine: Engine,
}

#[allow(clippy::too_many_arguments)]
fn gen_table_plan_inner(
    context: OptimizerContextRef,
    schema_name: Option<String>,
    table_name: String,
    info: CreateTableInfo,
    props: CreateTableProps,
) -> Result<(PlanRef, TableCatalog)> {
    let CreateTableInfo {
        ref columns,
        row_id_index,
        ref watermark_descs,
        ref source_catalog,
        ..
    } = info;
    let CreateTableProps { append_only, .. } = props;

    let (database_id, schema_id) = context
        .session_ctx()
        .get_database_and_schema_id_for_create(schema_name)?;

    let session = context.session_ctx().clone();
    let retention_seconds = context.with_options().retention_seconds();

    let source_node: LogicalPlanRef = LogicalSource::new(
        source_catalog.clone().map(Rc::new),
        columns.clone(),
        row_id_index,
        SourceNodeKind::CreateTable,
        context.clone(),
        None,
    )?
    .into();

    let required_cols = FixedBitSet::with_capacity(columns.len());
    let plan_root = PlanRoot::new_with_logical_plan(
        source_node,
        RequiredDist::Any,
        Order::any(),
        required_cols,
        vec![],
    );

    let has_non_ttl_watermark = watermark_descs.iter().any(|d| !d.with_ttl);

    if !append_only && has_non_ttl_watermark {
        return Err(ErrorCode::NotSupported(
            "Defining watermarks on table requires the table to be append only.".to_owned(),
            "Use the key words `APPEND ONLY`".to_owned(),
        )
        .into());
    }

    if !append_only && retention_seconds.is_some() {
        if session
            .config()
            .unsafe_enable_storage_retention_for_non_append_only_tables()
        {
            tracing::warn!(
                "Storage retention is enabled for non-append-only table {}. This may lead to stream inconsistency.",
                table_name
            );
            const NOTICE: &str = "Storage retention is enabled for non-append-only table. \
                                  This may lead to stream inconsistency and unrecoverable \
                                  node failure if there is any row INSERT/UPDATE/DELETE operation \
                                  corresponding to the TTLed primary keys";
            session.notice_to_user(NOTICE);
        } else {
            return Err(ErrorCode::NotSupported(
                "Defining retention seconds on table requires the table to be append only."
                    .to_owned(),
                "Use the key words `APPEND ONLY`".to_owned(),
            )
            .into());
        }
    }

    let materialize =
        plan_root.gen_table_plan(context, table_name, database_id, schema_id, info, props)?;

    let mut table = materialize.table().clone();
    table.owner = session.user_id();

    Ok((
        ensure_sync_log_store_fragment_root(materialize.into()),
        table,
    ))
}

/// Generate stream plan for cdc table based on shared source.
/// In replace workflow, the `table_id` is the id of the table to be replaced
/// in create table workflow, the `table_id` is a placeholder will be filled in the Meta
#[allow(clippy::too_many_arguments)]
pub(crate) fn gen_create_table_plan_for_cdc_table(
    context: OptimizerContextRef,
    source: Arc<SourceCatalog>,
    external_table_name: String,
    column_defs: Vec<ColumnDef>,
    source_watermarks: Vec<SourceWatermark>,
    mut columns: Vec<ColumnCatalog>,
    pk_names: Vec<String>,
    cdc_with_options: WithOptionsSecResolved,
    mut col_id_gen: ColumnIdGenerator,
    on_conflict: Option<OnConflict>,
    with_version_columns: Vec<String>,
    include_column_options: IncludeOption,
    table_name: ObjectName,
    resolved_table_name: String, // table name without schema prefix
    database_id: DatabaseId,
    schema_id: SchemaId,
    table_id: TableId,
    engine: Engine,
) -> Result<(PlanRef, TableCatalog)> {
    let session = context.session_ctx().clone();

    // append additional columns to the end
    handle_addition_columns(
        None,
        &cdc_with_options,
        include_column_options,
        &mut columns,
        true,
    )?;

    for c in &mut columns {
        col_id_gen.generate(c)?;
    }

    let (mut columns, pk_column_ids, _row_id_index) =
        bind_pk_and_row_id_on_relation(columns, pk_names, true)?;

    let watermark_descs = bind_source_watermark(
        context.session_ctx(),
        table_name.real_value(),
        source_watermarks,
        &columns,
    )?;

    // NOTES: In auto schema change, default value is not provided in column definition.
    bind_sql_column_constraints(
        context.session_ctx(),
        table_name.real_value(),
        &mut columns,
        &column_defs,
        &pk_column_ids,
    )?;

    let definition = context.normalized_sql().to_owned();

    let pk_column_indices = {
        let mut id_to_idx = HashMap::new();
        columns.iter().enumerate().for_each(|(idx, c)| {
            id_to_idx.insert(c.column_id(), idx);
        });
        // pk column id must exist in table columns.
        pk_column_ids
            .iter()
            .map(|c| id_to_idx.get(c).copied().unwrap())
            .collect_vec()
    };
    let table_pk = pk_column_indices
        .iter()
        .map(|idx| ColumnOrder::new(*idx, OrderType::ascending()))
        .collect();

    let (options, secret_refs) = cdc_with_options.into_parts();

    let non_generated_column_descs = columns
        .iter()
        .filter(|&c| !c.is_generated())
        .map(|c| c.column_desc.clone())
        .collect_vec();
    let non_generated_column_num = non_generated_column_descs.len();
    let cdc_table_type = ExternalCdcTableType::from_properties(&options);
    let cdc_table_desc = CdcTableDesc {
        table_id,
        source_id: source.id, // id of cdc source streaming job
        external_table_name: external_table_name.clone(),
        pk: table_pk,
        columns: non_generated_column_descs,
        stream_key: pk_column_indices,
        connect_properties: options,
        secret_refs,
    };

    tracing::debug!(?cdc_table_desc, "create cdc table");
    let options = build_cdc_scan_options_with_options(context.with_options(), &cdc_table_type)?;

    let logical_scan = LogicalCdcScan::create(
        external_table_name.clone(),
        Rc::new(cdc_table_desc),
        context.clone(),
        options,
    );

    let scan_node: LogicalPlanRef = logical_scan.into();
    let required_cols = FixedBitSet::with_capacity(non_generated_column_num);
    let plan_root = PlanRoot::new_with_logical_plan(
        scan_node,
        RequiredDist::Any,
        Order::any(),
        required_cols,
        vec![],
    );

    let cdc_table_id_external_table_name = if let ExternalCdcTableType::Postgres = cdc_table_type
        && let Some(normalized_table_name) =
            normalize_simple_postgres_quoted_table_name(&external_table_name)
    {
        normalized_table_name
    } else {
        external_table_name
    };
    let cdc_table_id = build_cdc_table_id(source.id, &cdc_table_id_external_table_name);
    let materialize = plan_root.gen_table_plan(
        context,
        resolved_table_name,
        database_id,
        schema_id,
        CreateTableInfo {
            columns,
            pk_column_ids,
            row_id_index: None,
            watermark_descs,
            source_catalog: Some((*source).clone()),
            version: col_id_gen.into_version(),
        },
        CreateTableProps {
            definition,
            append_only: false,
            on_conflict: on_conflict.into(),
            with_version_columns,
            webhook_info: None,
            engine,
        },
    )?;

    let mut table = materialize.table().clone();
    table.owner = session.user_id();
    table.cdc_table_id = Some(cdc_table_id);
    table.cdc_table_type = Some(cdc_table_type);
    Ok((
        ensure_sync_log_store_fragment_root(materialize.into()),
        table,
    ))
}

/// Derive connector properties and normalize `external_table_name` for CDC tables.
///
/// Returns (`connector_properties`, `normalized_external_table_name`) where:
/// - For SQL Server: Normalizes 'db.schema.table' (3 parts) to 'schema.table' (2 parts),
///   because users can optionally include database name for verification, but it needs to be
///   stripped to match the format returned by Debezium's `extract_table_name()`.
/// - For MySQL/Postgres: Returns the original `external_table_name` unchanged.
fn derive_with_options_for_cdc_table(
    source_with_properties: &WithOptionsSecResolved,
    external_table_name: String,
) -> Result<(WithOptionsSecResolved, String)> {
    use source::cdc::{MYSQL_CDC_CONNECTOR, POSTGRES_CDC_CONNECTOR, SQL_SERVER_CDC_CONNECTOR};
    // we should remove the prefix from `full_table_name`
    let source_database_name: &str = source_with_properties
        .get("database.name")
        .ok_or_else(|| anyhow!("The source with properties does not contain 'database.name'"))?
        .as_str();
    let mut with_options = source_with_properties.clone();
    if let Some(connector) = source_with_properties.get(UPSTREAM_SOURCE_KEY) {
        match connector.as_str() {
            MYSQL_CDC_CONNECTOR => {
                // MySQL doesn't allow '.' in database name and table name, so we can split the
                // external table name by '.' to get the table name
                let (db_name, table_name) = external_table_name.split_once('.').ok_or_else(|| {
                    anyhow!("The upstream table name must contain database name prefix, e.g. 'database.table'")
                })?;
                // We allow multiple database names in the source definition
                if !source_database_name
                    .split(',')
                    .map(|s| s.trim())
                    .any(|name| name == db_name)
                {
                    return Err(anyhow!(
                        "The database name `{}` in the FROM clause is not included in the database name `{}` in source definition",
                        db_name,
                        source_database_name
                    ).into());
                }
                with_options.insert(DATABASE_NAME_KEY.into(), db_name.into());
                with_options.insert(TABLE_NAME_KEY.into(), table_name.into());
                // Return original external_table_name unchanged for MySQL
                return Ok((with_options, external_table_name));
            }
            POSTGRES_CDC_CONNECTOR => {
                let (schema_name, table_name) =
                    parse_postgres_cdc_external_table_name(&external_table_name)?;

                // insert 'schema.name' into connect properties
                with_options.insert(SCHEMA_NAME_KEY.into(), schema_name);
                with_options.insert(TABLE_NAME_KEY.into(), table_name);
                // Return original external_table_name unchanged for Postgres
                return Ok((with_options, external_table_name));
            }
            SQL_SERVER_CDC_CONNECTOR => {
                // SQL Server external table name must be in one of two formats:
                // 1. 'schemaName.tableName' (2 parts) - database is already specified in source
                // 2. 'databaseName.schemaName.tableName' (3 parts) - for explicit verification
                //
                // We do NOT allow single table name (e.g., 't') because:
                // - Unlike database name (already in source), schema name is NOT pre-specified
                // - User must explicitly provide schema (even if it's 'dbo')
                let parts: Vec<&str> = external_table_name.split('.').collect();
                let (schema_name, table_name) = match parts.len() {
                    3 => {
                        // Format: database.schema.table
                        // Verify that the database name matches the one in source definition
                        let db_name = parts[0];
                        let schema_name = parts[1];
                        let table_name = parts[2];

                        if db_name != source_database_name {
                            return Err(anyhow!(
                                "The database name '{}' in FROM clause does not match the database name '{}' specified in source definition. \
                                 You can either use 'schema.table' format (recommended) or ensure the database name matches.",
                                db_name,
                                source_database_name
                            ).into());
                        }
                        (schema_name, table_name)
                    }
                    2 => {
                        // Format: schema.table (recommended)
                        // Database name is taken from source definition
                        let schema_name = parts[0];
                        let table_name = parts[1];
                        (schema_name, table_name)
                    }
                    1 => {
                        // Format: table only
                        // Reject with clear error message
                        return Err(anyhow!(
                            "Invalid table name format '{}'. For SQL Server CDC, you must specify the schema name. \
                             Use 'schema.table' format (e.g., 'dbo.{}') or 'database.schema.table' format (e.g., '{}.dbo.{}').",
                            external_table_name,
                            external_table_name,
                            source_database_name,
                            external_table_name
                        ).into());
                    }
                    _ => {
                        // Invalid format (4+ parts or empty)
                        return Err(anyhow!(
                            "Invalid table name format '{}'. Expected 'schema.table' or 'database.schema.table'.",
                            external_table_name
                        ).into());
                    }
                };

                // Insert schema and table names into connector properties
                with_options.insert(SCHEMA_NAME_KEY.into(), schema_name.into());
                with_options.insert(TABLE_NAME_KEY.into(), table_name.into());

                // Normalize external_table_name to 'schema.table' format
                // This ensures consistency with extract_table_name() in message.rs
                let normalized_external_table_name = format!("{}.{}", schema_name, table_name);
                return Ok((with_options, normalized_external_table_name));
            }
            _ => {
                return Err(RwError::from(anyhow!(
                    "connector {} is not supported for cdc table",
                    connector
                )));
            }
        };
    }
    unreachable!("All valid CDC connectors should have returned by now")
}

/// Parse the schema/table name from the CDC `TABLE` clause.
///
/// Column names do not need the same parsing here: wildcard schema derivation reads
/// them from PostgreSQL catalogs after the exact table has been identified.
fn parse_postgres_cdc_external_table_name(external_table_name: &str) -> Result<(String, String)> {
    let mut parts = vec![];
    let mut current = String::new();
    let mut chars = external_table_name.chars().peekable();
    let mut in_quote = false;
    let mut just_closed_quote = false;

    while let Some(ch) = chars.next() {
        if in_quote {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    in_quote = false;
                    just_closed_quote = true;
                }
            } else {
                current.push(ch);
            }
        } else {
            match ch {
                '.' => {
                    if current.is_empty() {
                        return Err(anyhow!(
                            "Invalid Postgres CDC table name '{}'. Expected 'schema.table'.",
                            external_table_name
                        )
                        .into());
                    }
                    parts.push(std::mem::take(&mut current));
                    just_closed_quote = false;
                }
                '"' if current.is_empty() => {
                    in_quote = true;
                }
                '"' => {
                    return Err(anyhow!(
                        "Invalid Postgres CDC table name '{}'. Expected 'schema.table'.",
                        external_table_name
                    )
                    .into());
                }
                _ if just_closed_quote => {
                    return Err(anyhow!(
                        "Invalid Postgres CDC table name '{}'. Expected 'schema.table'.",
                        external_table_name
                    )
                    .into());
                }
                _ => current.push(ch),
            }
        }
    }

    if in_quote || current.is_empty() {
        return Err(anyhow!(
            "Invalid Postgres CDC table name '{}'. Expected 'schema.table'.",
            external_table_name
        )
        .into());
    }
    parts.push(current);

    if let [schema_name, table_name] = parts.as_slice() {
        Ok((schema_name.clone(), table_name.clone()))
    } else {
        Err(
            anyhow!("The upstream table name must contain schema name prefix, e.g. 'public.table'")
                .into(),
        )
    }
}

/// Reject `CREATE TABLE` when a primary-key column is filtered out of Debezium change-event
/// values via `debezium.column.exclude.list` or `debezium.column.include.list`.
///
/// Debezium's column filters only apply to the change-event **value** payload. Message keys are
/// always built from the upstream PRIMARY KEY and are not affected. If a PK column is filtered out
/// of the value, RisingWave reads NULL for that PK column from the payload, causing silent data
/// corruption: UPDATE turns into a fresh INSERT (PK mismatch with the original row) and DELETE
/// silently no-ops.
///
/// Debezium entries are regex patterns matched against the fully qualified column name
/// `<namespace>.<table>.<column>`, where namespace is `schema` for Postgres / SQL Server and
/// `database` for MySQL.
fn reject_pk_filtered_by_debezium_column_filter(
    pk_names: &[String],
    cdc_with_options: &WithOptionsSecResolved,
) -> Result<()> {
    const EXCLUDE_KEY: &str = "debezium.column.exclude.list";
    const INCLUDE_KEY: &str = "debezium.column.include.list";

    let st = SchemaTableName::from_properties(cdc_with_options.as_plaintext());
    reject_pk_filtered_by_debezium_column_filter_inner(
        pk_names,
        &st,
        cdc_with_options.get(EXCLUDE_KEY).map(String::as_str),
        cdc_with_options.get(INCLUDE_KEY).map(String::as_str),
    )
}

fn reject_pk_filtered_by_debezium_column_filter_inner(
    pk_names: &[String],
    st: &SchemaTableName,
    exclude_list: Option<&str>,
    include_list: Option<&str>,
) -> Result<()> {
    const EXCLUDE_KEY: &str = "debezium.column.exclude.list";
    const INCLUDE_KEY: &str = "debezium.column.include.list";

    let pk_full_names = pk_names
        .iter()
        .map(|pk| (pk, format!("{}.{}.{}", st.schema_name, st.table_name, pk)))
        .collect_vec();

    if let Some(exclude_list) = exclude_list {
        let patterns = compile_debezium_column_filter_patterns(EXCLUDE_KEY, exclude_list)?;
        for (pk, pk_full_name) in &pk_full_names {
            for (pattern, regex) in &patterns {
                if regex.is_match(pk_full_name).map_err(|err| {
                    ErrorCode::InvalidInputSyntax(format!(
                        "failed to evaluate Debezium column filter pattern `{pattern}` in `{EXCLUDE_KEY}`: {}",
                        err.as_report()
                    ))
                })? {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "primary key column `{pk}` is excluded by `{EXCLUDE_KEY}` pattern \
                         `{pattern}`. Excluding a PK column causes silent data corruption: \
                         Debezium keeps the PK in the message key but drops it from the payload, \
                         so RisingWave cannot match UPDATE/DELETE events against the original row."
                    ))
                    .into());
                }
            }
        }
    }

    if let Some(include_list) = include_list {
        let patterns = compile_debezium_column_filter_patterns(INCLUDE_KEY, include_list)?;
        for (pk, pk_full_name) in &pk_full_names {
            let mut included = false;
            for (_, regex) in &patterns {
                if regex.is_match(pk_full_name).map_err(|err| {
                    ErrorCode::InvalidInputSyntax(format!(
                        "failed to evaluate Debezium column filter pattern in `{INCLUDE_KEY}`: {}",
                        err.as_report()
                    ))
                })? {
                    included = true;
                    break;
                }
            }
            if !included {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "primary key column `{pk}` is not included by `{INCLUDE_KEY}`. Omitting a PK \
                     column causes silent data corruption: Debezium keeps the PK in the message key \
                     but drops it from the payload, so RisingWave cannot match UPDATE/DELETE events \
                     against the original row."
                ))
                .into());
            }
        }
    }

    Ok(())
}

fn compile_debezium_column_filter_patterns(
    key: &str,
    filter_list: &str,
) -> Result<Vec<(String, Regex)>> {
    filter_list
        .split(',')
        .map(str::trim)
        .filter(|pattern| !pattern.is_empty())
        .map(|pattern| {
            let anchored_pattern = format!("(?i:^(?:{pattern})$)");
            let regex = Regex::new(&anchored_pattern).map_err(|err| {
                ErrorCode::InvalidInputSyntax(format!(
                    "invalid Debezium column filter pattern `{pattern}` in `{key}`: {}",
                    err.as_report()
                ))
            })?;
            Ok((pattern.to_owned(), regex))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_create_table_plan(
    handler_args: HandlerArgs,
    explain_options: ExplainOptions,
    format_encode: Option<FormatEncodeOptions>,
    cdc_table_info: Option<CdcTableInfo>,
    table_name: &ObjectName,
    column_defs: Vec<ColumnDef>,
    wildcard_idx: Option<usize>,
    constraints: Vec<TableConstraint>,
    source_watermarks: Vec<SourceWatermark>,
    append_only: bool,
    on_conflict: Option<OnConflict>,
    with_version_columns: Vec<String>,
    include_column_options: IncludeOption,
    webhook_info: Option<WebhookSourceInfo>,
    engine: Engine,
) -> Result<(
    PlanRef,
    Option<SourceCatalog>,
    TableCatalog,
    TableJobType,
    Option<SourceId>,
)> {
    let col_id_gen = ColumnIdGenerator::new_initial();
    let format_encode = check_create_table_with_source(
        &handler_args.with_options,
        format_encode,
        &include_column_options,
        &cdc_table_info,
    )?;
    let webhook_info = webhook_info
        .map(|info| bind_webhook_info(&handler_args.session, &column_defs, info))
        .transpose()?;

    let props = CreateTableProps {
        definition: handler_args.normalized_sql.clone(),
        append_only,
        on_conflict: on_conflict.into(),
        with_version_columns: with_version_columns.clone(),
        webhook_info,
        engine,
    };

    let ((plan, source, table), job_type, shared_shource_id) = match (
        format_encode,
        cdc_table_info.as_ref(),
    ) {
        (Some(format_encode), None) => (
            gen_create_table_plan_with_source(
                handler_args,
                explain_options,
                table_name.clone(),
                column_defs,
                wildcard_idx,
                constraints,
                format_encode,
                source_watermarks,
                col_id_gen,
                include_column_options,
                props,
                SqlColumnStrategy::FollowChecked,
            )
            .await?,
            TableJobType::General,
            None,
        ),
        (None, None) => {
            let context = OptimizerContext::new(handler_args, explain_options);
            let (plan, table) = gen_create_table_plan(
                context,
                table_name.clone(),
                column_defs,
                constraints,
                col_id_gen,
                source_watermarks,
                props,
                false,
            )?;

            ((plan, None, table), TableJobType::General, None)
        }

        (None, Some(cdc_table)) => {
            sanity_check_for_table_on_cdc_source(
                append_only,
                &column_defs,
                &wildcard_idx,
                &constraints,
                &source_watermarks,
            )?;

            generated_columns_check_for_cdc_table(&column_defs)?;
            not_null_check_for_cdc_table(&wildcard_idx, &column_defs)?;

            let session = &handler_args.session;
            let db_name = &session.database();
            let user_name = &session.user_name();
            let search_path = session.config().search_path();
            let (schema_name, resolved_table_name) =
                Binder::resolve_schema_qualified_name(db_name, table_name)?;
            let (database_id, schema_id) =
                session.get_database_and_schema_id_for_create(schema_name.clone())?;

            // cdc table cannot be append-only
            let (source_schema, source_name) =
                Binder::resolve_schema_qualified_name(db_name, &cdc_table.source_name)?;

            let source = {
                let catalog_reader = session.env().catalog_reader().read_guard();
                let schema_path =
                    SchemaPath::new(source_schema.as_deref(), &search_path, user_name);

                let (source, _) = catalog_reader.get_source_by_name(
                    db_name,
                    schema_path,
                    source_name.as_str(),
                )?;
                source.clone()
            };
            check_cdc_source_select_privilege(session, &source)?;
            let (cdc_with_options, normalized_external_table_name) =
                derive_with_options_for_cdc_table(
                    &source.with_properties,
                    cdc_table.external_table_name.clone(),
                )?;

            let (columns, pk_names) = match wildcard_idx {
                Some(_) => bind_cdc_table_schema_externally(cdc_with_options.clone()).await?,
                None => {
                    for column_def in &column_defs {
                        for option_def in &column_def.options {
                            if let ColumnOption::DefaultValue(_)
                            | ColumnOption::DefaultValueInternal { .. } = option_def.option
                            {
                                return Err(ErrorCode::NotSupported(
                                            "Default value for columns defined on the table created from a CDC source".into(),
                                            "Remove the default value expression in the column definitions".into(),
                                        )
                                            .into());
                            }
                        }
                    }

                    let (columns, pk_names) =
                        bind_cdc_table_schema(&column_defs, &constraints, false)?;
                    // read default value definition from external db
                    let (options, secret_refs) = cdc_with_options.clone().into_parts();
                    let _config = ExternalTableConfig::try_from_btreemap(options, secret_refs)
                        .context("failed to extract external table config")?;

                    (columns, pk_names)
                }
            };

            // CDC-table branch only: if Debezium column filters remove a PK column from the
            // change-event value, RisingWave reads NULL for that PK from the payload and silently
            // corrupts downstream (UPDATE turns into a fresh INSERT, DELETE no-ops). Plain
            // (non-CDC) tables don't hit this check.
            reject_pk_filtered_by_debezium_column_filter(&pk_names, &cdc_with_options)?;

            let context: OptimizerContextRef =
                OptimizerContext::new(handler_args, explain_options).into();
            let shared_source_id = source.id;
            let (plan, table) = gen_create_table_plan_for_cdc_table(
                context,
                source,
                normalized_external_table_name,
                column_defs,
                source_watermarks,
                columns,
                pk_names,
                cdc_with_options,
                col_id_gen,
                on_conflict,
                with_version_columns,
                include_column_options,
                table_name.clone(),
                resolved_table_name,
                database_id,
                schema_id,
                TableId::placeholder(),
                engine,
            )?;

            (
                (plan, None, table),
                TableJobType::SharedCdcSource,
                Some(shared_source_id),
            )
        }
        (Some(_), Some(_)) => {
            return Err(ErrorCode::NotSupported(
                "Data format and encoding format doesn't apply to table created from a CDC source"
                    .into(),
                "Remove the FORMAT and ENCODE specification".into(),
            )
            .into());
        }
    };
    Ok((plan, source, table, job_type, shared_shource_id))
}

// For both table from cdc source and table with cdc connector
fn generated_columns_check_for_cdc_table(columns: &Vec<ColumnDef>) -> Result<()> {
    let mut found_generated_column = false;
    for column in columns {
        let mut is_generated = false;

        for option_def in &column.options {
            if let ColumnOption::GeneratedColumns(_) = option_def.option {
                is_generated = true;
                break;
            }
        }

        if is_generated {
            found_generated_column = true;
        } else if found_generated_column {
            return Err(ErrorCode::NotSupported(
                "Non-generated column found after a generated column.".into(),
                "Ensure that all generated columns appear at the end of the cdc table definition."
                    .into(),
            )
            .into());
        }
    }
    Ok(())
}

// For both table from cdc source and table with cdc connector
fn not_null_check_for_cdc_table(
    wildcard_idx: &Option<usize>,
    column_defs: &Vec<ColumnDef>,
) -> Result<()> {
    if !wildcard_idx.is_some()
        && column_defs.iter().any(|col| {
            col.options
                .iter()
                .any(|opt| matches!(opt.option, ColumnOption::NotNull))
        })
    {
        return Err(ErrorCode::NotSupported(
            "CDC table with NOT NULL constraint is not supported".to_owned(),
            "Please remove the NOT NULL constraint for columns".to_owned(),
        )
        .into());
    }
    Ok(())
}

// Only for table from cdc source
fn sanity_check_for_table_on_cdc_source(
    append_only: bool,
    column_defs: &Vec<ColumnDef>,
    wildcard_idx: &Option<usize>,
    constraints: &Vec<TableConstraint>,
    source_watermarks: &Vec<SourceWatermark>,
) -> Result<()> {
    // wildcard cannot be used with column definitions
    if wildcard_idx.is_some() && !column_defs.is_empty() {
        return Err(ErrorCode::NotSupported(
            "wildcard(*) and column definitions cannot be used together".to_owned(),
            "Remove the wildcard or column definitions".to_owned(),
        )
        .into());
    }

    // cdc table must have primary key constraint or primary key column
    if !wildcard_idx.is_some()
        && !constraints.iter().any(|c| {
            matches!(
                c,
                TableConstraint::Unique {
                    is_primary: true,
                    ..
                }
            )
        })
        && !column_defs.iter().any(|col| {
            col.options
                .iter()
                .any(|opt| matches!(opt.option, ColumnOption::Unique { is_primary: true }))
        })
    {
        return Err(ErrorCode::NotSupported(
            "CDC table without primary key constraint is not supported".to_owned(),
            "Please define a primary key".to_owned(),
        )
        .into());
    }

    if append_only {
        return Err(ErrorCode::NotSupported(
            "append only modifier on the table created from a CDC source".into(),
            "Remove the APPEND ONLY clause".into(),
        )
        .into());
    }

    if !source_watermarks.is_empty()
        && source_watermarks
            .iter()
            .any(|watermark| !watermark.with_ttl)
    {
        return Err(ErrorCode::NotSupported(
            "non-TTL watermark defined on the table created from a CDC source".into(),
            "Use `WATERMARK ... WITH TTL` instead.".into(),
        )
        .into());
    }

    Ok(())
}

/// Derive schema for cdc table when create a new Table or alter an existing Table
async fn bind_cdc_table_schema_externally(
    cdc_with_options: WithOptionsSecResolved,
) -> Result<(Vec<ColumnCatalog>, Vec<String>)> {
    // read cdc table schema from external db or parsing the schema from SQL definitions
    let (options, secret_refs) = cdc_with_options.into_parts();
    let config = ExternalTableConfig::try_from_btreemap(options, secret_refs)
        .context("failed to extract external table config")?;

    let table = ExternalTableImpl::connect(config)
        .await
        .context("failed to auto derive table schema")?;

    Ok((
        table
            .column_descs()
            .iter()
            .cloned()
            .map(|column_desc| ColumnCatalog {
                column_desc,
                is_hidden: false,
            })
            .collect(),
        table.pk_names().clone(),
    ))
}

/// Derive schema for cdc table when create a new Table or alter an existing Table
fn bind_cdc_table_schema(
    column_defs: &Vec<ColumnDef>,
    constraints: &Vec<TableConstraint>,
    is_for_replace_plan: bool,
) -> Result<(Vec<ColumnCatalog>, Vec<String>)> {
    let columns = bind_sql_columns(column_defs, is_for_replace_plan)?;
    // CDC parsers cannot produce variant values, and this path has no FORMAT/ENCODE gate.
    reject_variant_columns(&columns, "on a table created from a CDC source")?;

    let pk_names = bind_sql_pk_names(column_defs, bind_table_constraints(constraints)?)?;
    Ok((columns, pk_names))
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_create_table(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    column_defs: Vec<ColumnDef>,
    wildcard_idx: Option<usize>,
    constraints: Vec<TableConstraint>,
    if_not_exists: bool,
    format_encode: Option<FormatEncodeOptions>,
    source_watermarks: Vec<SourceWatermark>,
    append_only: bool,
    on_conflict: Option<OnConflict>,
    with_version_columns: Vec<String>,
    cdc_table_info: Option<CdcTableInfo>,
    include_column_options: IncludeOption,
    webhook_info: Option<WebhookSourceInfo>,
    ast_engine: risingwave_sqlparser::ast::Engine,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();

    if append_only {
        session.notice_to_user("APPEND ONLY TABLE is currently an experimental feature.");
    }

    session.check_cluster_limits().await?;

    let engine = match ast_engine {
        risingwave_sqlparser::ast::Engine::Hummock => Engine::Hummock,
        risingwave_sqlparser::ast::Engine::Iceberg => Engine::Iceberg,
    };

    if let Either::Right(resp) = session.check_relation_name_duplicated(
        table_name.clone(),
        StatementType::CREATE_TABLE,
        if_not_exists,
    )? {
        return Ok(resp);
    }

    let (graph, source, hummock_table, job_type, shared_source_id) = {
        let (plan, source, table, job_type, shared_source_id) = handle_create_table_plan(
            handler_args.clone(),
            ExplainOptions::default(),
            format_encode,
            cdc_table_info,
            &table_name,
            column_defs.clone(),
            wildcard_idx,
            constraints.clone(),
            source_watermarks,
            append_only,
            on_conflict,
            with_version_columns,
            include_column_options,
            webhook_info,
            engine,
        )
        .await?;
        tracing::trace!("table_plan: {:?}", plan.explain_to_string());

        let graph = build_graph(plan, Some(GraphJobType::Table))?;

        (graph, source, table, job_type, shared_source_id)
    };

    tracing::trace!(
        "name={}, graph=\n{}",
        table_name,
        serde_json::to_string_pretty(&graph).unwrap()
    );

    let dependencies = shared_source_id
        .map(|id| HashSet::from([id.as_object_id()]))
        .unwrap_or_default();

    // Handle engine
    match engine {
        Engine::Hummock => {
            let catalog_writer = session.catalog_writer()?;
            let action = match job_type {
                TableJobType::SharedCdcSource => LongRunningNotificationAction::MonitorBackfillJob,
                _ => LongRunningNotificationAction::DiagnoseBarrierLatency,
            };
            execute_with_long_running_notification(
                catalog_writer.create_table(
                    source.map(|s| s.to_prost()),
                    hummock_table.to_prost(),
                    graph,
                    job_type,
                    if_not_exists,
                    dependencies,
                ),
                &session,
                "CREATE TABLE",
                action,
            )
            .await?;
        }
        Engine::Iceberg => {
            let hummock_table_name = hummock_table.name.clone();
            session.create_staging_table(hummock_table.clone());
            let res = Box::pin(create_iceberg_engine_table(
                session.clone(),
                handler_args,
                source.map(|s| s.to_prost()),
                hummock_table,
                graph,
                table_name,
                job_type,
                if_not_exists,
            ))
            .await;
            session.drop_staging_table(&hummock_table_name);
            res?
        }
    }

    Ok(PgResponse::empty_result(StatementType::CREATE_TABLE))
}

fn build_iceberg_engine_sink_options(
    mut sink_options: BTreeMap<String, String>,
    user_options: &WithOptions,
    table: &TableCatalog,
    primary_key: &[String],
) -> Result<BTreeMap<String, String>> {
    sink_options.extend(
        user_options
            .iter()
            .filter(|(key, _)| is_iceberg_engine_option(key))
            .map(|(key, value)| (key.clone(), value.clone())),
    );

    sink_options
        .entry(ENABLE_COMPACTION.to_owned())
        .or_insert_with(|| "true".to_owned());
    sink_options.insert(
        "type".to_owned(),
        if table.append_only {
            "append-only"
        } else {
            "upsert"
        }
        .to_owned(),
    );

    // Supply the table primary key while parsing the typed config. In PK-index mode the
    // planner derives it from the upstream stream key, so it is removed again below.
    if !table.append_only {
        sink_options.insert("primary_key".to_owned(), primary_key.join(","));
    }

    sink_options.insert("create_table_if_not_exists".to_owned(), "true".to_owned());
    sink_options.insert("is_exactly_once".to_owned(), "true".to_owned());

    let config = IcebergConfig::from_btreemap(sink_options.clone())?;

    // Engine tables own their Iceberg maintenance policy, so enable manifest rewrites by default
    // whenever the table format supports them. Keep V3 disabled until rewrites preserve row lineage.
    if config.table_format_version() < FormatVersion::V3 {
        sink_options
            .entry(ENABLE_MANIFEST_REWRITE.to_owned())
            .or_insert_with(|| "true".to_owned());
    }

    if let Some(partition_by) = &config.partition_by {
        let mut partition_columns = vec![];
        for (column, _) in parse_partition_by_exprs(partition_by.clone())? {
            table
                .columns()
                .iter()
                .find(|col| col.name().eq_ignore_ascii_case(&column))
                .ok_or_else(|| {
                    ErrorCode::InvalidInputSyntax(format!(
                        "Partition source column does not exist in schema: {}",
                        column
                    ))
                })?;

            partition_columns.push(column);
        }

        ensure_partition_columns_are_prefix_of_primary_key(&partition_columns, primary_key)
            .map_err(|_| {
                ErrorCode::InvalidInputSyntax(
                    "The partition columns should be the prefix of the primary key".to_owned(),
                )
            })?;
    }

    if let Some(order_key) = &config.order_key {
        validate_order_key_columns(order_key, table.columns().iter().map(|col| col.name()))
            .map_err(|err| ErrorCode::InvalidInputSyntax(err.to_report_string()))?;
    }

    if config.enable_pk_index {
        sink_options.remove("primary_key");
    } else {
        sink_options.insert(AUTO_SCHEMA_CHANGE_KEY.to_owned(), "true".to_owned());
    }

    Ok(sink_options)
}

/// Iceberg table engine is composed of hummock table, iceberg sink and iceberg source.
///
/// 1. fetch iceberg engine options from the meta node. Or use iceberg engine connection provided by users.
/// 2. create a hummock table
/// 3. create an iceberg sink
/// 4. create an iceberg source
///
/// See <https://github.com/risingwavelabs/risingwave/issues/21586> for an architecture diagram.
#[allow(clippy::too_many_arguments)]
pub async fn create_iceberg_engine_table(
    session: Arc<SessionImpl>,
    handler_args: HandlerArgs,
    mut source: Option<PbSource>,
    table: TableCatalog,
    graph: StreamFragmentGraph,
    table_name: ObjectName,
    job_type: PbTableJobType,
    if_not_exists: bool,
) -> Result<()> {
    let rw_db_name = session
        .env()
        .catalog_reader()
        .read_guard()
        .get_database_by_id(table.database_id)?
        .name()
        .to_owned();
    let rw_schema_name = session
        .env()
        .catalog_reader()
        .read_guard()
        .get_schema_by_id(table.database_id, table.schema_id)?
        .name()
        .clone();
    let iceberg_catalog_name = rw_db_name.clone();
    let iceberg_database_name = rw_schema_name.clone();
    let iceberg_table_name = table_name.0.last().unwrap().real_value();

    let iceberg_engine_connection: String = session.config().iceberg_engine_connection();
    let sink_decouple = session.config().sink_decouple();
    if matches!(sink_decouple, SinkDecouple::Disable) {
        bail!(
            "Iceberg engine table only supports with sink decouple, try `set sink_decouple = true` to resolve it"
        );
    }

    let mut connection_ref = BTreeMap::new();
    let with_common = if iceberg_engine_connection.is_empty() {
        bail!("to use iceberg engine table, the variable `iceberg_engine_connection` must be set.");
    } else {
        let parts: Vec<&str> = iceberg_engine_connection.split('.').collect();
        assert_eq!(parts.len(), 2);
        let connection_catalog =
            session.get_connection_by_name(Some(parts[0].to_owned()), parts[1])?;
        if let ConnectionInfo::ConnectionParams(params) = &connection_catalog.info {
            if params.connection_type == ConnectionType::Iceberg as i32 {
                // With iceberg engine connection:
                connection_ref.insert(
                    "connection".to_owned(),
                    ConnectionRefValue {
                        connection_name: ObjectName::from(vec![
                            Ident::from(parts[0]),
                            Ident::from(parts[1]),
                        ]),
                    },
                );

                let mut with_common = BTreeMap::new();
                with_common.insert("connector".to_owned(), "iceberg".to_owned());
                with_common.insert("database.name".to_owned(), iceberg_database_name);
                with_common.insert("table.name".to_owned(), iceberg_table_name);

                let hosted_catalog = params
                    .properties
                    .get("hosted_catalog")
                    .map(|s| s.eq_ignore_ascii_case("true"))
                    .unwrap_or(false);
                if hosted_catalog {
                    let meta_client = session.env().meta_client();
                    let meta_store_endpoint = meta_client.get_meta_store_endpoint().await?;

                    let meta_store_endpoint =
                        url::Url::parse(&meta_store_endpoint).map_err(|_| {
                            ErrorCode::InternalError(
                                "failed to parse the meta store endpoint".to_owned(),
                            )
                        })?;
                    let meta_store_backend = meta_store_endpoint.scheme().to_owned();
                    let meta_store_user = meta_store_endpoint.username().to_owned();
                    let meta_store_password = match meta_store_endpoint.password() {
                        Some(password) => percent_decode_str(password)
                            .decode_utf8()
                            .map_err(|_| {
                                ErrorCode::InternalError(
                                    "failed to parse password from meta store endpoint".to_owned(),
                                )
                            })?
                            .into_owned(),
                        None => "".to_owned(),
                    };
                    let meta_store_host = meta_store_endpoint
                        .host_str()
                        .ok_or_else(|| {
                            ErrorCode::InternalError(
                                "failed to parse host from meta store endpoint".to_owned(),
                            )
                        })?
                        .to_owned();
                    let meta_store_port = meta_store_endpoint.port().ok_or_else(|| {
                        ErrorCode::InternalError(
                            "failed to parse port from meta store endpoint".to_owned(),
                        )
                    })?;
                    let meta_store_database = meta_store_endpoint
                        .path()
                        .trim_start_matches('/')
                        .to_owned();

                    let Ok(meta_backend) = MetaBackend::from_str(&meta_store_backend, true) else {
                        bail!("failed to parse meta backend: {}", meta_store_backend);
                    };

                    let catalog_uri = match meta_backend {
                        MetaBackend::Postgres => {
                            format!(
                                "jdbc:postgresql://{}:{}/{}",
                                meta_store_host, meta_store_port, meta_store_database
                            )
                        }
                        MetaBackend::Mysql => {
                            format!(
                                "jdbc:mysql://{}:{}/{}",
                                meta_store_host, meta_store_port, meta_store_database
                            )
                        }
                        MetaBackend::Sqlite | MetaBackend::Sql | MetaBackend::Mem => {
                            bail!(
                                "Unsupported meta backend for iceberg engine table: {}",
                                meta_store_backend
                            );
                        }
                    };

                    with_common.insert("catalog.type".to_owned(), "jdbc".to_owned());
                    with_common.insert("catalog.uri".to_owned(), catalog_uri);
                    with_common.insert("catalog.jdbc.user".to_owned(), meta_store_user);
                    with_common.insert("catalog.jdbc.password".to_owned(), meta_store_password);
                    with_common.insert("catalog.name".to_owned(), iceberg_catalog_name);
                }

                with_common
            } else {
                return Err(RwError::from(ErrorCode::InvalidParameterValue(
                    "Only iceberg connection could be used in iceberg engine".to_owned(),
                )));
            }
        } else {
            return Err(RwError::from(ErrorCode::InvalidParameterValue(
                "Private Link Service has been deprecated. Please create a new connection instead."
                    .to_owned(),
            )));
        }
    };

    // Iceberg sinks require a primary key, if none is provided, we will use the _row_id column
    // Fetch primary key from columns
    let mut pks = table
        .pk_column_names()
        .iter()
        .map(|c| c.to_string())
        .collect::<Vec<String>>();

    // For the table without primary key. We will use `_row_id` as primary key.
    if pks.len() == 1 && pks[0].eq(ROW_ID_COLUMN_NAME) {
        pks = vec![RISINGWAVE_ICEBERG_ROW_ID.to_owned()];
    }

    let sink_from = CreateSink::From(table_name.clone());

    let mut sink_name = table_name.clone();
    *sink_name.0.last_mut().unwrap() = Ident::from(
        (ICEBERG_SINK_PREFIX.to_owned() + &sink_name.0.last().unwrap().real_value()).as_str(),
    );
    let create_sink_stmt = CreateSinkStatement {
        or_replace: false,
        if_not_exists: false,
        sink_name,
        with_properties: WithProperties(vec![]),
        sink_from,
        columns: vec![],
        emit_mode: None,
        sink_schema: None,
        into_table_name: None,
    };

    let mut sink_handler_args = handler_args.clone();

    let sink_with = build_iceberg_engine_sink_options(
        with_common.clone(),
        &handler_args.with_options,
        &table,
        &pks,
    )?;

    if let Some(source) = source.as_mut() {
        source
            .with_properties
            .retain(|key, _| !is_iceberg_engine_option(key));
    }

    // sink_with.insert(SINK_SNAPSHOT_OPTION.to_owned(), "false".to_owned());
    //
    // Note: in theory, we don't need to backfill from the table to the sink,
    // but we don't have atomic DDL now https://github.com/risingwavelabs/risingwave/issues/21863
    // so it may have potential data loss problem on the first barrier.
    //
    // For non-append-only table, we can always solve it by the initial sink with backfill, since
    // data will be present in hummock table.
    //
    // For append-only table, we need to be more careful.
    //
    // The possible cases for a table:
    // - For table without connector: it doesn't matter, since there's no data before the table is created
    // - For table with connector: we workarounded it by setting SOURCE_RATE_LIMIT to 0
    //   + If we support blocking DDL for table with connector, we need to be careful.
    // - For table with an upstream job: Specifically, CDC table from shared CDC source.
    //   + Data may come from both upstream connector, and CDC table backfill, so we need to pause both of them.
    //   + For now we don't support APPEND ONLY CDC table, so it's safe.

    sink_handler_args.with_options =
        WithOptions::new(sink_with, Default::default(), connection_ref.clone());
    let SinkPlanContext {
        sink_plan,
        sink_catalog,
        ..
    } = gen_sink_plan(sink_handler_args, create_sink_stmt, None, true).await?;
    let sink_graph = build_graph(sink_plan, Some(GraphJobType::Sink))?;

    let mut source_name = table_name.clone();
    *source_name.0.last_mut().unwrap() = Ident::from(
        (ICEBERG_SOURCE_PREFIX.to_owned() + &source_name.0.last().unwrap().real_value()).as_str(),
    );
    let create_source_stmt = CreateSourceStatement {
        temporary: false,
        if_not_exists: false,
        columns: vec![],
        source_name,
        wildcard_idx: Some(0),
        constraints: vec![],
        with_properties: WithProperties(vec![]),
        format_encode: CompatibleFormatEncode::V2(FormatEncodeOptions::none()),
        source_watermarks: vec![],
        include_column_options: vec![],
    };

    let mut source_handler_args = handler_args.clone();
    let source_with = with_common;
    source_handler_args.with_options =
        WithOptions::new(source_with, Default::default(), connection_ref);

    let overwrite_options = OverwriteOptions::new(&mut source_handler_args);
    let format_encode = create_source_stmt.format_encode.into_v2_with_warning();
    let (with_properties, refresh_mode) =
        bind_connector_props(&source_handler_args, &format_encode, true)?;

    // Create iceberg sink table, used for iceberg source column binding. See `bind_columns_from_source_for_non_cdc` for more details.
    // TODO: We can derive the columns directly from table definition in the future, so that we don't need to pre-create the table catalog.
    let (iceberg_catalog, table_identifier, table_created) = {
        let sink_param = SinkParam::try_from_sink_catalog(sink_catalog.clone())?;
        let iceberg_sink = IcebergSink::try_from(sink_param)?;
        let table_created = iceberg_sink.create_table_if_not_exists().await?;

        let iceberg_catalog = iceberg_sink.config.create_catalog().await?;
        let table_identifier = iceberg_sink.config.full_table_name()?;
        (iceberg_catalog, table_identifier, table_created)
    };

    let create_source_type = CreateSourceType::for_newly_created(&session, &*with_properties);
    let (columns_from_resolve_source, source_info) = bind_columns_from_source(
        &session,
        &format_encode,
        Either::Left(&with_properties),
        create_source_type,
    )
    .await?;
    let mut col_id_gen = ColumnIdGenerator::new_initial();

    let iceberg_source_catalog = bind_create_source_or_table_with_connector(
        source_handler_args,
        create_source_stmt.source_name,
        format_encode,
        with_properties,
        &create_source_stmt.columns,
        create_source_stmt.constraints,
        create_source_stmt.wildcard_idx,
        create_source_stmt.source_watermarks,
        columns_from_resolve_source,
        source_info,
        create_source_stmt.include_column_options,
        &mut col_id_gen,
        create_source_type,
        overwrite_options.source_rate_limit,
        SqlColumnStrategy::FollowChecked,
        refresh_mode,
    )
    .await?;

    // before we create the table, ensure the JVM is initialized as we use jdbc catalog right now.
    // If JVM isn't initialized successfully, current not atomic ddl will result in a partially created iceberg engine table.
    let _ = Jvm::get_or_init()?;

    let catalog_writer = session.catalog_writer()?;
    let action = match job_type {
        TableJobType::SharedCdcSource => LongRunningNotificationAction::MonitorBackfillJob,
        _ => LongRunningNotificationAction::DiagnoseBarrierLatency,
    };
    let res = execute_with_long_running_notification(
        catalog_writer.create_iceberg_table(
            PbTableJobInfo {
                source,
                table: Some(table.to_prost()),
                fragment_graph: Some(graph),
                job_type: job_type as _,
            },
            PbSinkJobInfo {
                sink: Some(sink_catalog.to_proto()),
                fragment_graph: Some(sink_graph),
            },
            iceberg_source_catalog.to_prost(),
            if_not_exists,
        ),
        &session,
        "CREATE TABLE",
        action,
    )
    .await;

    if res.is_err() {
        // Only clean up an iceberg table this statement created; a pre-existing
        // external table must survive a failed CREATE.
        if table_created {
            let _ = iceberg_catalog
                .drop_table(&table_identifier)
                .await
                .inspect_err(|err| {
                    tracing::error!(
                        "failed to drop iceberg table {} after create iceberg engine table failed: {}",
                        table_identifier,
                        err.as_report()
                    );
                });
        }
        res?
    }

    Ok(())
}

pub fn check_create_table_with_source(
    with_options: &WithOptions,
    format_encode: Option<FormatEncodeOptions>,
    include_column_options: &IncludeOption,
    cdc_table_info: &Option<CdcTableInfo>,
) -> Result<Option<FormatEncodeOptions>> {
    // skip check for cdc table
    if cdc_table_info.is_some() {
        return Ok(format_encode);
    }
    let defined_source = with_options.is_source_connector();

    if !include_column_options.is_empty() && !defined_source {
        return Err(ErrorCode::InvalidInputSyntax(
            "INCLUDE should be used with a connector".to_owned(),
        )
        .into());
    }
    if defined_source {
        format_encode.as_ref().ok_or_else(|| {
            ErrorCode::InvalidInputSyntax("Please specify a source schema using FORMAT".to_owned())
        })?;
    }
    Ok(format_encode)
}

fn ensure_partition_columns_are_prefix_of_primary_key(
    partition_columns: &[String],
    primary_key_columns: &[String],
) -> std::result::Result<(), String> {
    if partition_columns.len() > primary_key_columns.len() {
        return Err("Partition columns cannot be longer than primary key columns.".to_owned());
    }

    for (i, partition_col) in partition_columns.iter().enumerate() {
        if primary_key_columns.get(i) != Some(partition_col) {
            return Err(format!(
                "Partition column '{}' is not a prefix of the primary key.",
                partition_col
            ));
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn generate_stream_graph_for_replace_table(
    _session: &Arc<SessionImpl>,
    table_name: ObjectName,
    original_catalog: &Arc<TableCatalog>,
    handler_args: HandlerArgs,
    statement: Statement,
    col_id_gen: ColumnIdGenerator,
    sql_column_strategy: SqlColumnStrategy,
) -> Result<(
    StreamFragmentGraph,
    TableCatalog,
    Option<SourceCatalog>,
    TableJobType,
)> {
    let Statement::CreateTable {
        columns,
        constraints,
        source_watermarks,
        append_only,
        on_conflict,
        with_version_columns,
        wildcard_idx,
        cdc_table_info,
        format_encode,
        include_column_options,
        engine,
        with_options,
        ..
    } = statement
    else {
        panic!("unexpected statement type: {:?}", statement);
    };

    let format_encode = format_encode
        .clone()
        .map(|format_encode| format_encode.into_v2_with_warning());

    let engine = match engine {
        risingwave_sqlparser::ast::Engine::Hummock => Engine::Hummock,
        risingwave_sqlparser::ast::Engine::Iceberg => Engine::Iceberg,
    };

    let is_drop_connector =
        original_catalog.associated_source_id().is_some() && format_encode.is_none();
    if is_drop_connector {
        debug_assert!(
            source_watermarks.is_empty()
                && include_column_options.is_empty()
                && with_options
                    .iter()
                    .all(|opt| opt.name.real_value().to_lowercase() != "connector")
        );
    }

    let props = CreateTableProps {
        definition: handler_args.normalized_sql.clone(),
        append_only,
        on_conflict: on_conflict.into(),
        with_version_columns: with_version_columns
            .iter()
            .map(|col| col.real_value())
            .collect(),
        webhook_info: original_catalog.webhook_info.clone(),
        engine,
    };

    let ((plan, mut source, mut table), job_type) = match (format_encode, cdc_table_info.as_ref()) {
        (Some(format_encode), None) => (
            gen_create_table_plan_with_source(
                handler_args,
                ExplainOptions::default(),
                table_name,
                columns,
                wildcard_idx,
                constraints,
                format_encode,
                source_watermarks,
                col_id_gen,
                include_column_options,
                props,
                sql_column_strategy,
            )
            .await?,
            TableJobType::General,
        ),
        (None, None) => {
            let context = OptimizerContext::from_handler_args(handler_args);
            let (plan, table) = gen_create_table_plan(
                context,
                table_name,
                columns,
                constraints,
                col_id_gen,
                source_watermarks,
                props,
                true,
            )?;
            ((plan, None, table), TableJobType::General)
        }
        (None, Some(cdc_table)) => {
            sanity_check_for_table_on_cdc_source(
                append_only,
                &columns,
                &wildcard_idx,
                &constraints,
                &source_watermarks,
            )?;

            let session = &handler_args.session;
            let (source, resolved_table_name) =
                get_source_and_resolved_table_name(session, cdc_table.clone(), table_name.clone())?;

            let (cdc_with_options, normalized_external_table_name) =
                derive_with_options_for_cdc_table(
                    &source.with_properties,
                    cdc_table.external_table_name.clone(),
                )?;

            let (column_catalogs, pk_names) = bind_cdc_table_schema(&columns, &constraints, true)?;

            // CDC-table branch only: see the comment at the symmetric call site in
            // `handle_create_table_plan`. Plain (non-CDC) tables don't hit this check.
            reject_pk_filtered_by_debezium_column_filter(&pk_names, &cdc_with_options)?;

            let context: OptimizerContextRef =
                OptimizerContext::new(handler_args, ExplainOptions::default()).into();
            let (plan, table) = gen_create_table_plan_for_cdc_table(
                context,
                source,
                normalized_external_table_name,
                columns,
                source_watermarks,
                column_catalogs,
                pk_names,
                cdc_with_options,
                col_id_gen,
                on_conflict,
                with_version_columns
                    .iter()
                    .map(|col| col.real_value())
                    .collect(),
                include_column_options,
                table_name,
                resolved_table_name,
                original_catalog.database_id,
                original_catalog.schema_id,
                original_catalog.id(),
                engine,
            )?;

            ((plan, None, table), TableJobType::SharedCdcSource)
        }
        (Some(_), Some(_)) => {
            return Err(ErrorCode::NotSupported(
                "Data format and encoding format doesn't apply to table created from a CDC source"
                    .into(),
                "Remove the FORMAT and ENCODE specification".into(),
            )
            .into());
        }
    };

    if table.pk_column_ids() != original_catalog.pk_column_ids() {
        Err(ErrorCode::InvalidInputSyntax(
            "alter primary key of table is not supported".to_owned(),
        ))?
    }

    let graph = build_graph(plan, Some(GraphJobType::Table))?;

    // Fill the original table ID.
    table.id = original_catalog.id();
    if !is_drop_connector && let Some(source_id) = original_catalog.associated_source_id() {
        table.associated_source_id = Some(source_id);

        let source = source.as_mut().unwrap();
        source.id = source_id;
        source.associated_table_id = Some(table.id());
    }

    Ok((graph, table, source, job_type))
}

fn get_source_and_resolved_table_name(
    session: &Arc<SessionImpl>,
    cdc_table: CdcTableInfo,
    table_name: ObjectName,
) -> Result<(Arc<SourceCatalog>, String)> {
    let db_name = &session.database();
    let (_, resolved_table_name) = Binder::resolve_schema_qualified_name(db_name, &table_name)?;

    let (source_schema, source_name) =
        Binder::resolve_schema_qualified_name(db_name, &cdc_table.source_name)?;

    let source = {
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_name = source_schema.unwrap_or(DEFAULT_SCHEMA_NAME.to_owned());
        let (source, _) = catalog_reader.get_source_by_name(
            db_name,
            SchemaPath::Name(schema_name.as_str()),
            source_name.as_str(),
        )?;
        source.clone()
    };
    check_cdc_source_select_privilege(session, &source)?;

    Ok((source, resolved_table_name))
}

fn check_cdc_source_select_privilege(session: &SessionImpl, source: &SourceCatalog) -> Result<()> {
    session.check_privileges(&[ObjectCheckItem::new(
        source.owner,
        AclMode::Select,
        source.name.clone(),
        source.id,
    )])
}

// validate the webhook_info and also bind the webhook_info to protobuf
fn bind_webhook_info(
    session: &Arc<SessionImpl>,
    column_defs: &[ColumnDef],
    webhook_info: WebhookSourceInfo,
) -> Result<PbWebhookSourceInfo> {
    let WebhookSourceInfo {
        secret_ref,
        signature_expr,
        wait_for_persistence,
        is_batched,
    } = webhook_info;

    for column in column_defs {
        for option_def in &column.options {
            match option_def.option {
                ColumnOption::Null => {}
                ColumnOption::GeneratedColumns(_) => {
                    return Err(ErrorCode::InvalidInputSyntax(
                        "generated columns are not supported for webhook tables".to_owned(),
                    )
                    .into());
                }
                ColumnOption::DefaultValue(_) | ColumnOption::DefaultValueInternal { .. } => {
                    return Err(ErrorCode::InvalidInputSyntax(
                        "default values are not supported for webhook tables".to_owned(),
                    )
                    .into());
                }
                ColumnOption::NotNull
                | ColumnOption::Unique { .. }
                | ColumnOption::ForeignKey { .. }
                | ColumnOption::Check(_)
                | ColumnOption::DialectSpecific(_) => {
                    return Err(ErrorCode::InvalidInputSyntax(
                        "only NULL column option is supported for webhook tables".to_owned(),
                    )
                    .into());
                }
            }
        }
    }

    // validate secret_ref
    let (pb_secret_ref, secret_name) = if let Some(secret_ref) = secret_ref {
        let db_name = &session.database();
        let (schema_name, secret_name) =
            Binder::resolve_schema_qualified_name(db_name, &secret_ref.secret_name)?;
        let secret_catalog = session.get_secret_by_name(schema_name, &secret_name)?;
        (
            Some(PbSecretRef {
                secret_id: secret_catalog.id,
                ref_as: match secret_ref.ref_as {
                    SecretRefAsType::Text => PbRefAsType::Text,
                    SecretRefAsType::File => PbRefAsType::File,
                }
                .into(),
            }),
            Some(secret_name),
        )
    } else {
        (None, None)
    };

    let signature_expr = if let Some(signature_expr) = signature_expr {
        let payload_name = if column_defs.len() == 1
            && column_defs[0].data_type.as_ref() == Some(&AstDataType::Jsonb)
        {
            column_defs[0].name.real_value()
        } else {
            WEBHOOK_PAYLOAD_FIELD_NAME.to_owned()
        };
        let secure_compare_context = SecureCompareContext {
            payload_name,
            secret_name,
        };
        let mut binder = Binder::new_for_ddl(session).with_secure_compare(secure_compare_context);
        let expr = binder.bind_expr(&signature_expr)?;

        // validate expr, ensuring it is SECURE_COMPARE()
        if expr.as_function_call().is_none()
            || expr.as_function_call().unwrap().func_type()
                != crate::optimizer::plan_node::generic::ExprType::SecureCompare
        {
            return Err(ErrorCode::InvalidInputSyntax(
                "The signature verification function must be SECURE_COMPARE()".to_owned(),
            )
            .into());
        }

        Some(expr.to_expr_proto())
    } else {
        session.notice_to_user(
            "VALIDATE clause is strongly recommended for safety or production usages",
        );
        None
    };

    let pb_webhook_info = PbWebhookSourceInfo {
        secret_ref: pb_secret_ref,
        signature_expr,
        wait_for_persistence,
        is_batched,
    };

    Ok(pb_webhook_info)
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{
        DEFAULT_DATABASE_NAME, ROW_ID_COLUMN_NAME, RW_TIMESTAMP_COLUMN_NAME,
    };
    use risingwave_common::types::{DataType, StructType};

    use super::*;
    use crate::test_utils::{LocalFrontend, PROTO_FILE_DATA, create_proto_file};

    fn test_schema_table_name() -> SchemaTableName {
        SchemaTableName {
            schema_name: "public".to_owned(),
            table_name: "orders".to_owned(),
        }
    }

    fn pk_names() -> Vec<String> {
        vec!["plan_id".to_owned(), "site_id".to_owned()]
    }

    #[tokio::test]
    async fn test_cdc_table_requires_select_privilege_on_source() {
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend
            .run_sql(
                r#"
                CREATE SOURCE cdc_source WITH (
                    connector = 'mysql-cdc',
                    hostname = 'localhost',
                    port = '3306',
                    username = 'root',
                    password = '',
                    database.name = 'db'
                ) FORMAT PLAIN ENCODE JSON
                "#,
            )
            .await
            .unwrap();
        frontend.run_sql("CREATE USER cdc_user").await.unwrap();
        frontend
            .run_sql("GRANT CREATE ON SCHEMA public TO cdc_user")
            .await
            .unwrap();

        let user_id = frontend
            .session_ref()
            .env()
            .user_info_reader()
            .read_guard()
            .get_user_by_name("cdc_user")
            .unwrap()
            .id;
        let user_session = frontend.session_user_ref(
            DEFAULT_DATABASE_NAME.to_owned(),
            "cdc_user".to_owned(),
            user_id,
        );
        let create_table =
            "CREATE TABLE cdc_table (id INT PRIMARY KEY) FROM cdc_source TABLE 'db.t'";

        let err = frontend
            .run_sql_with_session(user_session.clone(), create_table)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("permission denied for source \"cdc_source\": \"SELECT\""),
            "{err:?}"
        );

        frontend
            .run_sql("GRANT SELECT ON SOURCE cdc_source TO cdc_user")
            .await
            .unwrap();
        frontend
            .run_sql_with_session(user_session.clone(), create_table)
            .await
            .unwrap();

        frontend
            .run_sql("REVOKE SELECT ON SOURCE cdc_source FROM cdc_user")
            .await
            .unwrap();
        let err = frontend
            .run_sql_with_session(user_session, "ALTER TABLE cdc_table ADD COLUMN value INT")
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("permission denied for source \"cdc_source\": \"SELECT\""),
            "{err:?}"
        );
    }

    #[test]
    fn test_debezium_filter_rejects_literal_excluded_pk() {
        let err = reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            Some("public.orders.site_id"),
            None,
        )
        .unwrap_err();

        assert!(err.to_report_string().contains("site_id"));
        assert!(
            err.to_report_string()
                .contains("debezium.column.exclude.list")
        );
    }

    #[test]
    fn test_debezium_filter_rejects_include_list_missing_pk() {
        let err = reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            None,
            Some("public.orders.plan_id,public.orders.payload"),
        )
        .unwrap_err();

        assert!(err.to_report_string().contains("site_id"));
        assert!(
            err.to_report_string()
                .contains("debezium.column.include.list")
        );
    }

    #[test]
    fn test_debezium_filter_accepts_include_list_covering_all_pks() {
        reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            None,
            Some("public.orders.plan_id,public.orders.site_id,public.orders.payload"),
        )
        .unwrap();
    }

    #[test]
    fn test_debezium_filter_matches_regex_patterns() {
        reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            None,
            Some(r"public[.]orders[.](plan_id|site_id),public[.]orders[.]payload"),
        )
        .unwrap();

        let err = reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            Some(r".*[.]orders[.]site_id"),
            None,
        )
        .unwrap_err();

        assert!(err.to_report_string().contains("site_id"));
    }

    #[test]
    fn test_debezium_filter_matches_patterns_case_insensitively() {
        let err = reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            Some("PUBLIC.ORDERS.SITE_ID"),
            None,
        )
        .unwrap_err();

        assert!(err.to_report_string().contains("site_id"));

        reject_pk_filtered_by_debezium_column_filter_inner(
            &pk_names(),
            &test_schema_table_name(),
            None,
            Some("Public.Orders.Plan_ID,Public.Orders.Site_ID"),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn test_create_table_handler() {
        let sql =
            "create table t (v1 smallint, v2 struct<v3 bigint, v4 float, v5 double>) append only;";
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check table exists.
        let (table, _) = catalog_reader
            .get_created_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
            .unwrap();
        assert_eq!(table.name(), "t");

        let columns = table
            .columns
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<HashMap<&str, DataType>>();

        let expected_columns = maplit::hashmap! {
            ROW_ID_COLUMN_NAME => DataType::Serial,
            "v1" => DataType::Int16,
            "v2" => StructType::new(
                vec![("v3", DataType::Int64),("v4", DataType::Float64),("v5", DataType::Float64)],
            )
            .with_ids([3, 4, 5].map(ColumnId::new))
            .into(),
            RW_TIMESTAMP_COLUMN_NAME => DataType::Timestamptz,
        };

        assert_eq!(columns, expected_columns, "{columns:#?}");
    }

    #[tokio::test]
    async fn test_create_webhook_table_with_arbitrary_columns() {
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend
            .run_sql("create schema ingest_schema;")
            .await
            .unwrap();
        frontend
            .run_sql(
                r#"
                create table ingest_schema.orders (
                    id int,
                    customer_name varchar,
                    amount double precision,
                    primary key (id)
                ) with (
                    connector = 'webhook'
                ) validate as secure_compare(
                    headers->>'x-rw-signature',
                    'sha256=' || encode(hmac('webhook-secret', payload, 'sha256'), 'hex')
                );
                "#,
            )
            .await
            .unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let (table, _) = catalog_reader
            .get_created_table_by_name(
                DEFAULT_DATABASE_NAME,
                SchemaPath::Name("ingest_schema"),
                "orders",
            )
            .unwrap();

        assert!(table.webhook_info.is_some());
        assert_eq!(
            table
                .columns
                .iter()
                .filter(|column| column.can_dml())
                .count(),
            3
        );
    }

    #[tokio::test]
    async fn test_create_webhook_table_uses_single_jsonb_column_name_in_validate() {
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend
            .run_sql(
                r#"
                create table webhook_single_column (
                    body jsonb
                ) with (
                    connector = 'webhook'
                ) validate as secure_compare(
                    headers->>'x-rw-signature',
                    'sha256=' || encode(hmac('webhook-secret', body, 'sha256'), 'hex')
                );
                "#,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create_webhook_table_with_generated_columns() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let err = frontend
            .run_sql(
                r#"
                create table webhook_generated_columns (
                    id int,
                    amount double precision,
                    amount_with_fee double precision as amount + 1.0
                ) with (
                    connector = 'webhook'
                );
                "#,
            )
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("generated columns are not supported for webhook tables"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn test_create_webhook_table_with_default_value() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let err = frontend
            .run_sql(
                r#"
                create table webhook_default_value (
                    id int default 42,
                    amount double precision
                ) with (
                    connector = 'webhook'
                );
                "#,
            )
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("default values are not supported for webhook tables"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn test_create_webhook_table_with_not_null_option() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let err = frontend
            .run_sql(
                r#"
                create table webhook_not_null (
                    id int not null,
                    amount double precision
                ) with (
                    connector = 'webhook'
                );
                "#,
            )
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("only NULL column option is supported for webhook tables"),
            "{err:?}"
        );
    }

    #[test]
    fn test_parse_postgres_cdc_external_table_name() {
        for (input, expected) in [
            ("public.Note", ("public", "Note")),
            ("public.\"Note\"", ("public", "Note")),
            (
                "\"Mixed.Schema\".\"Note.Table\"",
                ("Mixed.Schema", "Note.Table"),
            ),
            ("public.\"Note\"\"Archive\"", ("public", "Note\"Archive")),
        ] {
            assert_eq!(
                parse_postgres_cdc_external_table_name(input).unwrap(),
                (expected.0.to_owned(), expected.1.to_owned()),
                "input: {input}"
            );
        }

        for input in [
            "Note",
            "public.",
            ".Note",
            "public.\"Note",
            "public.\"Note\"Archive",
            "public.Note.Archive",
        ] {
            assert!(
                parse_postgres_cdc_external_table_name(input).is_err(),
                "input should be rejected: {input}"
            );
        }
    }

    #[test]
    fn test_bind_primary_key() {
        // Note: Column ID 0 is reserved for row ID column.

        for (sql, expected) in [
            ("create table t (v1 int, v2 int)", Ok(&[0] as &[_])),
            ("create table t (v1 int primary key, v2 int)", Ok(&[1])),
            ("create table t (v1 int, v2 int primary key)", Ok(&[2])),
            (
                "create table t (v1 int primary key, v2 int primary key)",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int primary key primary key, v2 int)",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int, v2 int, primary key (v1))",
                Ok(&[1]),
            ),
            (
                "create table t (v1 int, primary key (v2), v2 int)",
                Ok(&[2]),
            ),
            (
                "create table t (primary key (v2, v1), v1 int, v2 int)",
                Ok(&[2, 1]),
            ),
            (
                "create table t (v1 int, primary key (v1), v2 int, primary key (v1))",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int primary key, primary key (v1), v2 int)",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int, primary key (V3), v2 int)",
                Err("column \"v3\" named in key does not exist"),
            ),
        ] {
            let mut ast = risingwave_sqlparser::parser::Parser::parse_sql(sql).unwrap();
            let risingwave_sqlparser::ast::Statement::CreateTable {
                columns: column_defs,
                constraints,
                ..
            } = ast.remove(0)
            else {
                panic!("test case should be create table")
            };
            let actual: Result<_> = (|| {
                let mut columns = bind_sql_columns(&column_defs, false)?;
                let mut col_id_gen = ColumnIdGenerator::new_initial();
                for c in &mut columns {
                    col_id_gen.generate(c)?;
                }

                let pk_names =
                    bind_sql_pk_names(&column_defs, bind_table_constraints(&constraints)?)?;
                let (_, pk_column_ids, _) =
                    bind_pk_and_row_id_on_relation(columns, pk_names, true)?;
                Ok(pk_column_ids)
            })();
            match (expected, actual) {
                (Ok(expected), Ok(actual)) => assert_eq!(
                    expected.iter().copied().map(ColumnId::new).collect_vec(),
                    actual,
                    "sql: {sql}"
                ),
                (Ok(_), Err(actual)) => panic!("sql: {sql}\nunexpected error: {actual:?}"),
                (Err(_), Ok(actual)) => panic!("sql: {sql}\nexpects error but got: {actual:?}"),
                (Err(expected), Err(actual)) => assert!(
                    actual.to_string().contains(expected),
                    "sql: {sql}\nexpected: {expected:?}\nactual: {actual:?}"
                ),
            }
        }
    }

    #[tokio::test]
    async fn test_duplicate_props_options() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE TABLE t
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
}
