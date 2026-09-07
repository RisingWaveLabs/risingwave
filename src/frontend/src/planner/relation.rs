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

use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;

use iceberg::spec::{Operation, TableMetadata};
use itertools::Itertools;
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{
    ColumnCatalog, Engine, Field, RISINGWAVE_ICEBERG_COMMIT_EPOCH, RISINGWAVE_ICEBERG_ROW_ID,
    ROW_ID_COLUMN_NAME, Schema,
};
use risingwave_common::constants::log_store::{
    EPOCH_COLUMN_NAME, INSERT_OP_CODE, ROW_OP_COLUMN_NAME, encode_epoch,
};
use risingwave_common::session_config::IcebergQueryStorageMode;
use risingwave_common::types::{DataType, Interval, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::iceberg::IcebergTimeTravelInfo;
use risingwave_sqlparser::ast::AsOf;
use thiserror_ext::AsReport;

use crate::TableCatalog;
use crate::binder::{
    BoundBaseTable, BoundGapFill, BoundIcebergMetadataTable, BoundJoin, BoundMatchRecognize,
    BoundShare, BoundShareInput, BoundSource, BoundSystemTable, BoundWatermark,
    BoundWindowTableFunction, Relation, WindowTableFunctionKind,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::{ErrorCode, Result};
use crate::expr::{CastContext, Expr, ExprImpl, ExprType, FunctionCall, InputRef, Literal};
use crate::optimizer::IcebergSnapshotInfo;
use crate::optimizer::plan_node::generic::{self, GenericPlanRef, SourceNodeKind};
use crate::optimizer::plan_node::utils::to_iceberg_time_travel_as_of;
use crate::optimizer::plan_node::{
    LogicalApply, LogicalGapFill, LogicalHopWindow, LogicalIcebergIntermediateScan,
    LogicalIcebergMetadataScan, LogicalJoin, LogicalMatchRecognize, LogicalPlanRef as PlanRef,
    LogicalProject, LogicalScan, LogicalShare, LogicalSource, LogicalSysScan, LogicalTableFunction,
    LogicalUnion, LogicalValues,
};
use crate::optimizer::property::Cardinality;
use crate::planner::{PlanFor, Planner};
use crate::utils::{ColIndexMapping, Condition};

const ERROR_WINDOW_SIZE_ARG: &str =
    "The size arg of window table function should be an interval literal.";

impl Planner {
    pub fn plan_relation(&mut self, relation: Relation) -> Result<PlanRef> {
        match relation {
            Relation::BaseTable(t) => self.plan_base_table(&t),
            Relation::SystemTable(st) => self.plan_sys_table(*st),
            Relation::IcebergMetadataTable(table) => self.plan_iceberg_metadata_table(*table),
            // TODO: order is ignored in the subquery
            Relation::Subquery(q) => Ok(self.plan_query(q.query)?.into_unordered_subplan()),
            Relation::Join(join) => self.plan_join(*join),
            Relation::Apply(join) => self.plan_apply(*join),
            Relation::WindowTableFunction(tf) => self.plan_window_table_function(*tf),
            Relation::Source(s) => self.plan_source(*s),
            Relation::TableFunction {
                expr: tf,
                with_ordinality,
            } => self.plan_table_function(tf, with_ordinality),
            Relation::Watermark(tf) => self.plan_watermark(*tf),
            Relation::Share(share) => self.plan_share(*share),
            Relation::GapFill(bound_gap_fill) => self.plan_gap_fill(*bound_gap_fill),
            Relation::MatchRecognize(mr) => self.plan_match_recognize(*mr),
        }
    }

    pub(crate) fn plan_sys_table(&mut self, sys_table: BoundSystemTable) -> Result<PlanRef> {
        Ok(LogicalSysScan::create(
            sys_table.sys_table_catalog,
            self.ctx(),
            Cardinality::unknown(), // TODO(card): cardinality of system table
        )
        .into())
    }

    fn plan_iceberg_metadata_table(&mut self, table: BoundIcebergMetadataTable) -> Result<PlanRef> {
        let timezone = self.ctx().get_session_timezone();
        let time_travel_info = to_iceberg_time_travel_as_of(&table.as_of, &timezone)?;
        let core = crate::optimizer::plan_node::generic::IcebergMetadataScan {
            metadata_type: table.metadata_type,
            properties: table.properties,
            secret_refs: table.secret_refs,
            time_travel_info,
            ctx: self.ctx(),
        };
        Ok(LogicalIcebergMetadataScan::new(core).into())
    }

    pub(super) fn plan_base_table(&mut self, base_table: &BoundBaseTable) -> Result<PlanRef> {
        let as_of = base_table.as_of.clone();
        let scan = LogicalScan::from_base_table(base_table, self.ctx(), as_of.clone());

        match base_table.table_catalog.engine {
            Engine::Hummock => {
                match as_of {
                    None
                    | Some(AsOf::ProcessTime)
                    | Some(AsOf::TimestampNum(_))
                    | Some(AsOf::TimestampString(_))
                    | Some(AsOf::ProcessTimeWithInterval(_)) => {}
                    Some(AsOf::VersionNum(_)) | Some(AsOf::VersionString(_)) => {
                        bail_not_implemented!("As Of Version is not supported yet.")
                    }
                };
                Ok(scan.into())
            }
            Engine::Iceberg => self.plan_iceberg_table(base_table, scan, as_of),
        }
    }

    fn plan_iceberg_table(
        &mut self,
        base_table: &BoundBaseTable,
        scan: LogicalScan,
        as_of: Option<AsOf>,
    ) -> Result<PlanRef> {
        let is_append_only = base_table.table_catalog.append_only;
        let iceberg_query_storage_mode = self
            .ctx()
            .session_ctx()
            .config()
            .iceberg_query_storage_mode();

        enum PlanTarget {
            TableScan,
            Source,
            IntermediateScan,
        }
        let plan_target = match self.plan_for() {
            PlanFor::StreamIcebergEngineInternal => PlanTarget::TableScan,
            PlanFor::BatchDql => match iceberg_query_storage_mode {
                // Append-only Iceberg engine tables use a dummy Hummock materialization and keep
                // their rows in Iceberg plus the sink log store.
                IcebergQueryStorageMode::Hummock if !is_append_only => PlanTarget::TableScan,
                _ => PlanTarget::IntermediateScan,
            },
            PlanFor::Stream => {
                if is_append_only {
                    PlanTarget::Source
                } else {
                    PlanTarget::TableScan
                }
            }
            PlanFor::Batch => {
                if is_append_only {
                    PlanTarget::IntermediateScan
                } else {
                    PlanTarget::TableScan
                }
            }
        };
        match as_of {
            None
            | Some(AsOf::VersionNum(_))
            | Some(AsOf::TimestampString(_))
            | Some(AsOf::TimestampNum(_)) => {}
            Some(AsOf::ProcessTime) | Some(AsOf::ProcessTimeWithInterval(_)) => {
                bail_not_implemented!("As Of ProcessTime() is not supported yet.")
            }
            Some(AsOf::VersionString(_)) => {
                bail_not_implemented!("As Of Version is not supported yet.")
            }
        }

        if matches!(plan_target, PlanTarget::TableScan) {
            return Ok(scan.into());
        }

        let source_catalog = self.get_iceberg_source_by_table_catalog(&base_table.table_catalog)
            .ok_or_else(|| {
                ErrorCode::BindError(format!(
                    "failed to plan an iceberg engine table: {}. Can't find the corresponding iceberg source. Maybe you need to recreate the table",
                    base_table.table_catalog.name()
                    ))
            })?;

        // Build type mapping: source column name → Hummock table type.
        // This lets the intermediate scan output Hummock types directly,
        // while the Iceberg scan path still adds explicit casts after
        // materialization to match the expected output types.
        let mut table_column_type_mapping = HashMap::new();
        let table_column_map: HashMap<&str, &DataType> = base_table
            .table_catalog
            .columns
            .iter()
            .map(|c| (c.name.as_str(), &c.column_desc.data_type))
            .collect();
        for source_col in &source_catalog.columns {
            let source_name = source_col.name();
            let table_name = if source_name == RISINGWAVE_ICEBERG_ROW_ID {
                ROW_ID_COLUMN_NAME
            } else {
                source_name
            };
            if let Some(&table_type) = table_column_map.get(table_name)
                && source_col.column_desc.data_type != *table_type
            {
                table_column_type_mapping.insert(source_name.to_owned(), table_type.clone());
            }
        }

        let column_map: HashMap<String, (usize, ColumnCatalog)> = source_catalog
            .columns
            .clone()
            .into_iter()
            .enumerate()
            .map(|(i, column)| (column.name().to_owned(), (i, column)))
            .collect();
        // For intermediate scan, it will output Hummock types directly. But for source, it still
        // outputs original types. So only when the plan target is source and the column type is
        // different, we need to add cast.
        let exprs = scan
            .table()
            .column_schema()
            .fields()
            .iter()
            .map(|field| {
                let source_filed_name = if field.name == ROW_ID_COLUMN_NAME {
                    RISINGWAVE_ICEBERG_ROW_ID
                } else {
                    &field.name
                };
                if let Some((i, source_column)) = column_map.get(source_filed_name) {
                    let input_type = &source_column.column_desc.data_type;
                    if matches!(plan_target, PlanTarget::Source) && input_type != &field.data_type {
                        let mut input_ref =
                            ExprImpl::InputRef(InputRef::new(*i, input_type.clone()).into());
                        FunctionCall::cast_mut(
                            &mut input_ref,
                            &field.data_type,
                            CastContext::Explicit,
                        )
                        .unwrap();
                        input_ref
                    } else {
                        ExprImpl::InputRef(InputRef::new(*i, field.data_type.clone()).into())
                    }
                } else {
                    // fields like `_rw_timestamp`, would not be found in source.
                    ExprImpl::Literal(Literal::new(None, field.data_type.clone()).into())
                }
            })
            .collect_vec();

        // Build source→table column index mapping for Hummock rewrite.
        // Must be built before source_catalog is moved into Rc.
        let table_col_index: HashMap<&str, usize> = base_table
            .table_catalog
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.as_str(), i))
            .collect();
        let source_to_table_mapping = ColIndexMapping::new(
            source_catalog
                .columns
                .iter()
                .map(|c| {
                    let table_name = if c.name() == RISINGWAVE_ICEBERG_ROW_ID {
                        ROW_ID_COLUMN_NAME
                    } else {
                        c.name()
                    };
                    table_col_index.get(table_name).copied()
                })
                .collect(),
            base_table.table_catalog.columns.len(),
        );

        let logical_source = LogicalSource::with_catalog(
            Rc::new(source_catalog),
            SourceNodeKind::CreateMViewOrBatch,
            self.ctx(),
            as_of.clone(),
        )?;
        if matches!(plan_target, PlanTarget::Source) {
            return Ok(LogicalProject::new(logical_source.into(), exprs).into());
        }

        // Pin Hummock before loading the latest Iceberg snapshot. If a sink commit races with
        // planning, this ordering guarantees that the Iceberg snapshot plus the filtered log
        // store still represents a coherent prefix of the table.
        let read_pending_log_store =
            matches!(self.plan_for(), PlanFor::BatchDql) && is_append_only && as_of.is_none();
        if read_pending_log_store {
            drop(self.ctx().session_ctx().pinned_snapshot());
        }

        let mut logical_iceberg_intermediate_scan = self.plan_iceberg_intermediate_scan(
            &logical_source,
            table_column_type_mapping,
            source_to_table_mapping,
        )?;

        if read_pending_log_store {
            let snapshot_info = self.fetch_current_snapshot_info(&logical_source)?;
            // A table without a snapshot has no committed rows, so all log-store rows are
            // pending. For an existing snapshot without a RisingWave epoch marker, fall back to
            // Iceberg-only reads to avoid returning duplicates from legacy/external snapshots.
            let committed_epoch = match snapshot_info {
                None => Some(None),
                Some(IcebergSnapshotInfo {
                    commit_epoch: Some(epoch),
                    ..
                }) => Some(Some(epoch)),
                Some(_) => None,
            };
            if let Some(committed_epoch) = committed_epoch
                && let Some(log_store_scan) = self.plan_iceberg_log_store_scan(
                    &base_table.table_catalog,
                    logical_iceberg_intermediate_scan.schema(),
                    &logical_source.core.column_catalog,
                    committed_epoch,
                )?
            {
                logical_iceberg_intermediate_scan = LogicalUnion::create(
                    true,
                    vec![logical_iceberg_intermediate_scan, log_store_scan],
                );
            }
        }

        Ok(LogicalProject::new(logical_iceberg_intermediate_scan, exprs).into())
    }

    pub(super) fn plan_source(&mut self, source: BoundSource) -> Result<PlanRef> {
        if source.is_shareable_cdc_connector() {
            Err(ErrorCode::InternalError(
                "Should not create MATERIALIZED VIEW or SELECT directly on shared CDC source. HINT: create TABLE from the source instead.".to_owned(),
            )
            .into())
        } else {
            let as_of = source.as_of.clone();
            match as_of {
                None
                | Some(AsOf::VersionNum(_))
                | Some(AsOf::TimestampString(_))
                | Some(AsOf::TimestampNum(_)) => {}
                Some(AsOf::ProcessTime) | Some(AsOf::ProcessTimeWithInterval(_)) => {
                    bail_not_implemented!("As Of ProcessTime() is not supported yet.")
                }
                Some(AsOf::VersionString(_)) => {
                    bail_not_implemented!("As Of Version is not supported yet.")
                }
            }
            let is_iceberg = source.catalog.is_iceberg_connector();

            // validate the source has pk. We raise an error here to avoid panic in expect_stream_key later
            // for a nicer error message.
            if matches!(self.plan_for(), PlanFor::Stream) {
                let has_pk =
                    source.catalog.row_id_index.is_some() || !source.catalog.pk_col_ids.is_empty();
                if !has_pk {
                    // in older version, iceberg source doesn't have row_id, thus may hit this
                    // only iceberg should hit this.
                    debug_assert!(is_iceberg);
                    if is_iceberg {
                        return Err(ErrorCode::BindError(format!(
                        "Cannot create a stream job from an iceberg source without a primary key.\nThe iceberg source might be created in an older version of RisingWave. Please try recreating the source.\nSource: {:?}",
                        source.catalog
                    ))
                    .into());
                    } else {
                        return Err(ErrorCode::BindError(format!(
                            "Cannot create a stream job from a source without a primary key.
This is a bug. We would appreciate a bug report at:
https://github.com/risingwavelabs/risingwave/issues/new?labels=type%2Fbug&template=bug_report.yml

source: {:?}",
                            source.catalog
                        ))
                        .into());
                    }
                }
            }

            let source = LogicalSource::with_catalog(
                Rc::new(source.catalog),
                SourceNodeKind::CreateMViewOrBatch,
                self.ctx(),
                as_of,
            )?;
            if is_iceberg && !matches!(self.plan_for(), PlanFor::Stream) {
                let num_cols = source.core.column_catalog.len();
                let intermediate_scan = self.plan_iceberg_intermediate_scan(
                    &source,
                    HashMap::new(),
                    ColIndexMapping::identity(num_cols),
                )?;
                Ok(intermediate_scan)
            } else {
                Ok(source.into())
            }
        }
    }

    pub(super) fn plan_join(&mut self, join: BoundJoin) -> Result<PlanRef> {
        let left = self.plan_relation(join.left)?;
        let right = self.plan_relation(join.right)?;
        let join_type = join.join_type;
        let on_clause = join.cond;
        if on_clause.has_subquery() {
            bail_not_implemented!("Subquery in join on condition");
        } else {
            Ok(LogicalJoin::create(left, right, join_type, on_clause))
        }
    }

    pub(super) fn plan_apply(&mut self, mut join: BoundJoin) -> Result<PlanRef> {
        let join_type = join.join_type;
        let on_clause = join.cond;
        if on_clause.has_subquery() {
            bail_not_implemented!("Subquery in join on condition");
        }

        let correlated_id = self.ctx.next_correlated_id();
        let correlated_indices = join
            .right
            .collect_correlated_indices_by_depth_and_assign_id(0, correlated_id);
        let left = self.plan_relation(join.left)?;
        let right = self.plan_relation(join.right)?;

        Ok(LogicalApply::create(
            left,
            right,
            join_type,
            Condition::with_expr(on_clause),
            correlated_id,
            correlated_indices,
            false,
        ))
    }

    pub(super) fn plan_window_table_function(
        &mut self,
        table_function: BoundWindowTableFunction,
    ) -> Result<PlanRef> {
        use WindowTableFunctionKind::*;
        match table_function.kind {
            Tumble => self.plan_tumble_window(
                table_function.input,
                table_function.time_col,
                table_function.args,
            ),
            Hop => self.plan_hop_window(
                table_function.input,
                table_function.time_col,
                table_function.args,
            ),
        }
    }

    pub(super) fn plan_table_function(
        &mut self,
        table_function: ExprImpl,
        with_ordinality: bool,
    ) -> Result<PlanRef> {
        // TODO: maybe we can unify LogicalTableFunction with LogicalValues
        match table_function {
            ExprImpl::TableFunction(tf) => {
                Ok(LogicalTableFunction::new(*tf, with_ordinality, self.ctx()).into())
            }
            expr => {
                let schema = Schema {
                    // TODO: should be named
                    fields: vec![Field::unnamed(expr.return_type())],
                };
                let expr_return_type = expr.return_type();
                let root = LogicalValues::create(vec![vec![expr]], schema, self.ctx());
                let input_ref = ExprImpl::from(InputRef::new(0, expr_return_type.clone()));
                let mut exprs = if let DataType::Struct(st) = expr_return_type {
                    st.iter()
                        .enumerate()
                        .map(|(i, (_, ty))| {
                            let idx = ExprImpl::literal_int(i.try_into().unwrap());
                            let args = vec![input_ref.clone(), idx];
                            FunctionCall::new_unchecked(ExprType::Field, args, ty.clone()).into()
                        })
                        .collect()
                } else {
                    vec![input_ref]
                };
                if with_ordinality {
                    exprs.push(ExprImpl::literal_bigint(1));
                }
                Ok(LogicalProject::create(root, exprs))
            }
        }
    }

    pub(super) fn plan_share(&mut self, share: BoundShare) -> Result<PlanRef> {
        match share.input {
            BoundShareInput::Query(query) => {
                let id = share.share_id;
                match self.share_cache.get(&id) {
                    None => {
                        let result = self.plan_query(query)?.into_unordered_subplan();
                        let logical_share = LogicalShare::create(result);
                        self.share_cache.insert(id, logical_share.clone());
                        Ok(logical_share)
                    }
                    Some(result) => Ok(result.clone()),
                }
            }
            BoundShareInput::ChangeLog(relation) => {
                let id = share.share_id;
                let result = self.plan_changelog(relation)?;
                let logical_share = LogicalShare::create(result);
                self.share_cache.insert(id, logical_share.clone());
                Ok(logical_share)
            }
        }
    }

    pub(super) fn plan_watermark(&mut self, _watermark: BoundWatermark) -> Result<PlanRef> {
        todo!("plan watermark");
    }

    pub(super) fn plan_gap_fill(&mut self, gap_fill: BoundGapFill) -> Result<PlanRef> {
        let input = self.plan_relation(gap_fill.input)?;
        Ok(LogicalGapFill::new(
            input,
            gap_fill.time_col,
            gap_fill.interval,
            gap_fill.fill_strategies,
            gap_fill.partition_by_cols,
        )
        .into())
    }

    pub(super) fn plan_match_recognize(&mut self, mr: BoundMatchRecognize) -> Result<PlanRef> {
        let input = self.plan_relation(mr.input)?;
        Ok(LogicalMatchRecognize::new(
            input,
            mr.partition_by,
            mr.order_by,
            mr.measures,
            mr.rows_per_match,
            mr.after_match_skip,
            mr.pattern,
            mr.defines,
            mr.within,
            mr.within_deadline,
        )
        .into())
    }

    fn collect_col_data_types_for_tumble_window(relation: &Relation) -> Result<Vec<DataType>> {
        let col_data_types = match relation {
            Relation::Source(s) => s
                .catalog
                .columns
                .iter()
                .map(|col| col.data_type().clone())
                .collect(),
            Relation::BaseTable(t) => t
                .table_catalog
                .columns
                .iter()
                .map(|col| col.data_type().clone())
                .collect(),
            Relation::Subquery(q) => q.query.schema().data_types(),
            Relation::Share(share) => share
                .input
                .fields()?
                .into_iter()
                .map(|(_, f)| f.data_type)
                .collect(),
            r => {
                return Err(ErrorCode::BindError(format!(
                    "Invalid input relation to tumble: {r:?}"
                ))
                .into());
            }
        };
        Ok(col_data_types)
    }

    fn plan_tumble_window(
        &mut self,
        input: Relation,
        time_col: InputRef,
        args: Vec<ExprImpl>,
    ) -> Result<PlanRef> {
        let mut args = args.into_iter();
        let col_data_types: Vec<_> = Self::collect_col_data_types_for_tumble_window(&input)?;

        match (args.next(), args.next(), args.next()) {
            (Some(window_size @ ExprImpl::Literal(_)), None, None) => {
                let mut exprs = Vec::with_capacity(col_data_types.len() + 2);
                for (idx, col_dt) in col_data_types.iter().enumerate() {
                    exprs.push(InputRef::new(idx, col_dt.clone()).into());
                }
                let window_start: ExprImpl = FunctionCall::new(
                    ExprType::TumbleStart,
                    vec![ExprImpl::InputRef(Box::new(time_col)), window_size.clone()],
                )?
                .into();
                // TODO: `window_end` may be optimized to avoid double calculation of
                // `tumble_start`, or we can depends on common expression
                // optimization.
                let window_end =
                    FunctionCall::new(ExprType::Add, vec![window_start.clone(), window_size])?
                        .into();
                exprs.push(window_start);
                exprs.push(window_end);
                let base = self.plan_relation(input)?;
                let project = LogicalProject::create(base, exprs);
                Ok(project)
            }
            (
                Some(window_size @ ExprImpl::Literal(_)),
                Some(window_offset @ ExprImpl::Literal(_)),
                None,
            ) => {
                let mut exprs = Vec::with_capacity(col_data_types.len() + 2);
                for (idx, col_dt) in col_data_types.iter().enumerate() {
                    exprs.push(InputRef::new(idx, col_dt.clone()).into());
                }
                let window_start: ExprImpl = FunctionCall::new(
                    ExprType::TumbleStart,
                    vec![
                        ExprImpl::InputRef(Box::new(time_col)),
                        window_size.clone(),
                        window_offset,
                    ],
                )?
                .into();
                // TODO: `window_end` may be optimized to avoid double calculation of
                // `tumble_start`, or we can depends on common expression
                // optimization.
                let window_end =
                    FunctionCall::new(ExprType::Add, vec![window_start.clone(), window_size])?
                        .into();
                exprs.push(window_start);
                exprs.push(window_end);
                let base = self.plan_relation(input)?;
                let project = LogicalProject::create(base, exprs);
                Ok(project)
            }
            _ => Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_owned()).into()),
        }
    }

    fn plan_hop_window(
        &mut self,
        input: Relation,
        time_col: InputRef,
        args: Vec<ExprImpl>,
    ) -> Result<PlanRef> {
        let input = self.plan_relation(input)?;
        let mut args = args.into_iter();
        let Some((ExprImpl::Literal(window_slide), ExprImpl::Literal(window_size))) =
            args.next_tuple()
        else {
            return Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_owned()).into());
        };

        let Some(ScalarImpl::Interval(window_slide)) = *window_slide.get_data() else {
            return Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_owned()).into());
        };
        let Some(ScalarImpl::Interval(window_size)) = *window_size.get_data() else {
            return Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_owned()).into());
        };

        let window_offset = match (args.next(), args.next()) {
            (Some(ExprImpl::Literal(window_offset)), None) => match *window_offset.get_data() {
                Some(ScalarImpl::Interval(window_offset)) => window_offset,
                _ => return Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_owned()).into()),
            },
            (None, None) => Interval::from_month_day_usec(0, 0, 0),
            _ => return Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_owned()).into()),
        };

        if !window_size.is_positive() || !window_slide.is_positive() {
            return Err(ErrorCode::BindError(format!(
                "window_size {} and window_slide {} must be positive",
                window_size, window_slide
            ))
            .into());
        }

        if window_size.exact_div(&window_slide).is_none() {
            return Err(ErrorCode::BindError(format!("Invalid arguments for HOP window function: window_size {} cannot be divided by window_slide {}",window_size, window_slide)).into());
        }

        Ok(LogicalHopWindow::create(
            input,
            time_col,
            window_slide,
            window_size,
            window_offset,
        ))
    }

    fn plan_iceberg_intermediate_scan(
        &self,
        source: &LogicalSource,
        table_column_type_mapping: HashMap<String, DataType>,
        source_to_table_mapping: ColIndexMapping,
    ) -> Result<PlanRef> {
        // If time travel is not specified, we use current timestamp to get the latest snapshot
        let timezone = self.ctx().get_session_timezone();
        let mut time_travel_info = to_iceberg_time_travel_as_of(&source.core.as_of, &timezone)?;
        if time_travel_info.is_none() {
            time_travel_info = self
                .fetch_current_snapshot_info(source)?
                .map(|info| IcebergTimeTravelInfo::Version(info.snapshot_id));
        }
        let Some(time_travel_info) = time_travel_info else {
            let mut schema = source.schema().clone();
            for field in &mut schema.fields {
                if let Some(target_type) = table_column_type_mapping.get(&field.name) {
                    field.data_type = target_type.clone();
                }
            }
            return Ok(LogicalValues::new(vec![], schema, self.ctx()).into());
        };
        let intermediate_scan = LogicalIcebergIntermediateScan::new(
            source,
            time_travel_info,
            table_column_type_mapping,
            source_to_table_mapping,
        );
        Ok(intermediate_scan.into())
    }

    fn fetch_current_snapshot_info(
        &self,
        source: &LogicalSource,
    ) -> Result<Option<IcebergSnapshotInfo>> {
        let mut map = self.ctx.iceberg_snapshot_info_map();
        let catalog = source.source_catalog().ok_or_else(|| {
            crate::error::ErrorCode::InternalError(
                "Iceberg source must have a valid source catalog".to_owned(),
            )
        })?;
        if let Some(&snapshot_info) = map.get(&catalog.id) {
            return Ok(snapshot_info);
        }

        #[cfg(madsim)]
        return Err(crate::error::ErrorCode::BindError(
            "iceberg source time travel can't be used in the madsim mode".to_string(),
        )
        .into());

        #[cfg(not(madsim))]
        {
            let ConnectorProperties::Iceberg(prop) =
                ConnectorProperties::extract(catalog.with_properties.clone(), false)?
            else {
                return Err(crate::error::ErrorCode::InternalError(
                    "Iceberg source must have Iceberg connector properties".to_owned(),
                )
                .into());
            };

            let snapshot_info = tokio::task::block_in_place(|| {
                crate::utils::FRONTEND_RUNTIME.block_on(async {
                    prop.load_table().await.map(|table| {
                        let metadata = table.metadata();
                        metadata
                            .current_snapshot()
                            .map(|snapshot| IcebergSnapshotInfo {
                                snapshot_id: snapshot.snapshot_id(),
                                commit_epoch: risingwave_iceberg_commit_epoch(
                                    metadata,
                                    snapshot.snapshot_id(),
                                ),
                            })
                    })
                })
            })?;
            map.insert(catalog.id, snapshot_info);
            Ok(snapshot_info)
        }
    }

    /// Build a scan of insert rows that have not yet reached the current Iceberg snapshot.
    /// Returns `None` for legacy/in-memory sinks whose persisted KV log store cannot be found.
    fn plan_iceberg_log_store_scan(
        &self,
        table_catalog: &TableCatalog,
        target_schema: &Schema,
        source_columns: &[ColumnCatalog],
        committed_epoch: Option<u64>,
    ) -> Result<Option<PlanRef>> {
        let Some(sink_name) = table_catalog.iceberg_sink_name() else {
            return Ok(None);
        };
        let log_store_table = {
            let catalog_reader = self.ctx.session_ctx().env().catalog_reader().read_guard();
            let Ok(schema) =
                catalog_reader.get_schema_by_id(table_catalog.database_id, table_catalog.schema_id)
            else {
                return Ok(None);
            };
            let Some(sink) = schema.get_created_sink_by_name(&sink_name) else {
                return Ok(None);
            };
            schema
                .iter_internal_table()
                .find(|table| {
                    table.job_id == Some(sink.id.as_job_id())
                        && table
                            .columns()
                            .iter()
                            .any(|column| column.name() == EPOCH_COLUMN_NAME)
                        && table
                            .columns()
                            .iter()
                            .any(|column| column.name() == ROW_OP_COLUMN_NAME)
                })
                .cloned()
        };
        let Some(log_store_table) = log_store_table else {
            return Ok(None);
        };

        let Some(epoch_idx) = log_store_table
            .columns()
            .iter()
            .position(|column| column.name() == EPOCH_COLUMN_NAME)
        else {
            return Ok(None);
        };
        let Some(row_op_idx) = log_store_table
            .columns()
            .iter()
            .position(|column| column.name() == ROW_OP_COLUMN_NAME)
        else {
            return Ok(None);
        };
        // The log-store payload can contain hidden upstream columns such as `_rw_timestamp`
        // that are not part of the Iceberg table schema. Do not let those implementation-only
        // columns prevent the pending rows from being planned.
        let payload_col_idx = log_store_table
            .columns()
            .iter()
            .enumerate()
            .skip(row_op_idx + 1)
            .filter_map(|(idx, column)| (!column.is_hidden).then_some(idx))
            .collect_vec();
        if source_columns.len() != target_schema.len()
            || payload_col_idx.len()
                != source_columns
                    .iter()
                    .filter(|column| !column.is_hidden)
                    .count()
        {
            return Ok(None);
        }

        let row_op_predicate: ExprImpl = FunctionCall::new(
            ExprType::Equal,
            vec![
                InputRef::new(row_op_idx, DataType::Int16).into(),
                Literal::new(Some(ScalarImpl::Int16(INSERT_OP_CODE)), DataType::Int16).into(),
            ],
        )?
        .into();
        let mut predicate = Condition::with_expr(row_op_predicate);
        if let Some(epoch) = committed_epoch {
            let epoch_predicate: ExprImpl = FunctionCall::new(
                ExprType::GreaterThan,
                vec![
                    InputRef::new(epoch_idx, DataType::Int64).into(),
                    Literal::new(
                        Some(ScalarImpl::Int64(encode_epoch(epoch))),
                        DataType::Int64,
                    )
                    .into(),
                ],
            )?
            .into();
            predicate = predicate.and(Condition::with_expr(epoch_predicate));
        }

        let log_store_scan: PlanRef = LogicalScan::from(generic::TableScan::new(
            payload_col_idx,
            log_store_table,
            vec![],
            vec![],
            self.ctx(),
            predicate,
            None,
        ))
        .into();

        let source_types = log_store_scan.schema().data_types();
        let target_types = target_schema.data_types();
        let mut payload_idx = 0;
        let project_exprs = source_columns
            .iter()
            .zip_eq_fast(target_types)
            .map(|(source_column, target_type)| {
                if source_column.is_hidden {
                    return Ok(Literal::new(None, target_type).into());
                }

                let source_type = source_types[payload_idx].clone();
                let mut expr: ExprImpl = InputRef::new(payload_idx, source_type).into();
                payload_idx += 1;
                FunctionCall::cast_mut(&mut expr, &target_type, CastContext::Explicit).map_err(
                    |error| {
                        ErrorCode::InternalError(format!(
                            "failed to align Iceberg log-store payload type: {}",
                            error.as_report()
                        ))
                    },
                )?;
                Ok(expr)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(LogicalProject::create(log_store_scan, project_exprs)))
    }

    fn get_iceberg_source_by_table_catalog(
        &self,
        table_catalog: &TableCatalog,
    ) -> Option<SourceCatalog> {
        let catalog_reader = self.ctx.session_ctx().env().catalog_reader().read_guard();

        let iceberg_source_name = table_catalog.iceberg_source_name()?;
        let schema = catalog_reader
            .get_schema_by_id(table_catalog.database_id, table_catalog.schema_id)
            .ok()?;
        let source_catalog = schema.get_source_by_name(&iceberg_source_name)?;
        Some(source_catalog.deref().clone())
    }
}

/// Find the latest known RisingWave commit boundary. Iceberg compaction produces `replace`
/// snapshots without changing table contents, so the marker is inherited across a chain of those
/// snapshots. We deliberately stop at any other unmarked operation: an external or legacy append
/// cannot be assigned a safe log-store boundary.
fn risingwave_iceberg_commit_epoch(metadata: &TableMetadata, snapshot_id: i64) -> Option<u64> {
    let mut snapshot = metadata.snapshot_by_id(snapshot_id)?;
    loop {
        if let Some(epoch) = snapshot
            .summary()
            .additional_properties
            .get(RISINGWAVE_ICEBERG_COMMIT_EPOCH)
        {
            return epoch.parse().ok();
        }
        if snapshot.summary().operation != Operation::Replace {
            return None;
        }
        snapshot = metadata.snapshot_by_id(snapshot.parent_snapshot_id()?)?;
    }
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{
        FormatVersion, MAIN_BRANCH, NestedField, PrimitiveType, Schema as IcebergSchema, Snapshot,
        SortOrder, Summary, TableMetadataBuilder, Type, UnboundPartitionSpec,
    };

    use super::*;

    #[test]
    fn test_risingwave_commit_epoch_inherits_only_across_replace_snapshots() {
        let append = snapshot(
            1,
            None,
            Operation::Append,
            HashMap::from([(RISINGWAVE_ICEBERG_COMMIT_EPOCH.to_owned(), "42".to_owned())]),
        );
        let replace = snapshot(2, Some(1), Operation::Replace, HashMap::new());
        let metadata = metadata_with_current(vec![append, replace]);
        assert_eq!(risingwave_iceberg_commit_epoch(&metadata, 2), Some(42));

        let external_append = snapshot(3, Some(2), Operation::Append, HashMap::new());
        let metadata = metadata_with_current(vec![
            snapshot(
                1,
                None,
                Operation::Append,
                HashMap::from([(RISINGWAVE_ICEBERG_COMMIT_EPOCH.to_owned(), "42".to_owned())]),
            ),
            snapshot(2, Some(1), Operation::Replace, HashMap::new()),
            external_append,
        ]);
        assert_eq!(risingwave_iceberg_commit_epoch(&metadata, 3), None);
    }

    fn metadata_with_current(mut snapshots: Vec<Snapshot>) -> TableMetadata {
        let current = snapshots.pop().unwrap();
        let mut builder = TableMetadataBuilder::new(
            IcebergSchema::builder()
                .with_fields(vec![
                    NestedField::new(1, "id", Type::Primitive(PrimitiveType::Long), false).into(),
                ])
                .build()
                .unwrap(),
            UnboundPartitionSpec::builder().build(),
            SortOrder::unsorted_order(),
            "s3://warehouse/db/table".to_owned(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap();
        for snapshot in snapshots {
            builder = builder.add_snapshot(snapshot).unwrap();
        }
        builder
            .set_branch_snapshot(current, MAIN_BRANCH)
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }

    fn snapshot(
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        operation: Operation,
        additional_properties: HashMap<String, String>,
    ) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(snapshot_id)
            .with_timestamp_ms(snapshot_id)
            .with_manifest_list(format!("/snap-{snapshot_id}.avro"))
            .with_summary(Summary {
                operation,
                additional_properties,
            })
            .with_schema_id(0)
            .build()
    }
}
