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

use std::rc::Rc;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::bail;
use risingwave_common::catalog::{
    ColumnCatalog, ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_FILE_POS_COLUMN_NAME,
    ICEBERG_SEQUENCE_NUM_COLUMN_NAME, ROW_ID_COLUMN_NAME,
};
use risingwave_pb::plan_common::GeneratedColumnDesc;
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_pb::plan_common::source_refresh_mode::RefreshMode;
use risingwave_sqlparser::ast::AsOf;

use super::generic::{GenericPlanRef, SourceNodeKind};
use super::stream_watermark_filter::StreamWatermarkFilter;
use super::utils::{Distill, childless_record};
use super::{
    BatchProject, BatchSource, ColPrunable, ExprRewritable, Logical, LogicalFilter,
    LogicalPlanRef as PlanRef, LogicalProject, PlanBase, PredicatePushdown, StreamPlanRef,
    StreamProject, StreamRowIdGen, StreamSource, StreamSourceScan, ToBatch, ToStream, generic,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::expr::{ExprImpl, ExprRewriter, ExprVisitor, InputRef};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::stream_fs_fetch::StreamFsFetch;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, StreamDedup,
    ToStreamContext,
};
use crate::optimizer::property::Distribution::HashShard;
use crate::optimizer::property::{
    Distribution, MonotonicityMap, RequiredDist, StreamKind, WatermarkColumns,
};
use crate::utils::{ColIndexMapping, Condition, IndexRewriter};

/// `LogicalSource` returns contents of a table or other equivalent object
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalSource {
    pub base: PlanBase<Logical>,
    pub core: generic::Source,

    /// Expressions to output. This field presents and will be turned to a `Project` when
    /// converting to a physical plan, only if there are generated columns.
    pub(crate) output_exprs: Option<Vec<ExprImpl>>,
    /// When there are generated columns, the `StreamRowIdGen`'s `row_id_index` is different from
    /// the one in `core`. So we store the one in `output_exprs` here.
    pub(crate) output_row_id_index: Option<usize>,
}

impl LogicalSource {
    pub fn new(
        source_catalog: Option<Rc<SourceCatalog>>,
        column_catalog: Vec<ColumnCatalog>,
        row_id_index: Option<usize>,
        kind: SourceNodeKind,
        ctx: OptimizerContextRef,
        as_of: Option<AsOf>,
    ) -> Result<Self> {
        // XXX: should we reorder the columns?
        // The order may be strange if the schema is changed, e.g., [foo:Varchar, _rw_kafka_timestamp:Timestamptz, _row_id:Serial, bar:Int32]
        // related: https://github.com/risingwavelabs/risingwave/issues/16486
        // The order does not matter much. The columns field is essentially a map indexed by the column id.
        // It will affect what users will see in `SELECT *`.
        // But not sure if we rely on the position of hidden column like `_row_id` somewhere. For `projected_row_id` we do so...
        let core = generic::Source {
            catalog: source_catalog,
            column_catalog,
            row_id_index,
            kind,
            ctx,
            as_of,
        };

        if core.as_of.is_some() && !core.support_time_travel() {
            bail!("Time travel is not supported for the source")
        }

        let base = PlanBase::new_logical_with_core(&core);

        let output_exprs = Self::derive_output_exprs_from_generated_columns(&core.column_catalog)?;
        let (core, output_row_id_index) = core.exclude_generated_columns();

        Ok(LogicalSource {
            base,
            core,
            output_exprs,
            output_row_id_index,
        })
    }

    pub fn with_catalog(
        source_catalog: Rc<SourceCatalog>,
        kind: SourceNodeKind,
        ctx: OptimizerContextRef,
        as_of: Option<AsOf>,
    ) -> Result<Self> {
        let column_catalogs = source_catalog.columns.clone();
        let row_id_index = source_catalog.row_id_index;
        if !source_catalog.append_only {
            assert!(row_id_index.is_none());
        }

        Self::new(
            Some(source_catalog),
            column_catalogs,
            row_id_index,
            kind,
            ctx,
            as_of,
        )
    }

    /// If there are no generated columns, returns `None`.
    ///
    /// Otherwise, the returned expressions correspond to all columns.
    /// Non-generated columns are represented by `InputRef`.
    pub fn derive_output_exprs_from_generated_columns(
        columns: &[ColumnCatalog],
    ) -> Result<Option<Vec<ExprImpl>>> {
        if !columns.iter().any(|c| c.is_generated()) {
            return Ok(None);
        }

        let col_mapping = {
            let mut mapping = vec![None; columns.len()];
            let mut cur = 0;
            for (idx, column) in columns.iter().enumerate() {
                if !column.is_generated() {
                    mapping[idx] = Some(cur);
                    cur += 1;
                } else {
                    mapping[idx] = None;
                }
            }
            ColIndexMapping::new(mapping, columns.len())
        };

        let mut rewriter = IndexRewriter::new(col_mapping);
        let mut exprs = Vec::with_capacity(columns.len());
        let mut cur = 0;
        for column in columns {
            let column_desc = &column.column_desc;
            let ret_data_type = column_desc.data_type.clone();

            if let Some(GeneratedOrDefaultColumn::GeneratedColumn(generated_column)) =
                &column_desc.generated_or_default_column
            {
                let GeneratedColumnDesc { expr } = generated_column;
                // TODO(yuhao): avoid this `from_expr_proto`.
                let proj_expr =
                    rewriter.rewrite_expr(ExprImpl::from_expr_proto(expr.as_ref().unwrap())?);
                let casted_expr = proj_expr.cast_assign(&ret_data_type)?;
                exprs.push(casted_expr);
            } else {
                let input_ref = InputRef {
                    data_type: ret_data_type,
                    index: cur,
                };
                cur += 1;
                exprs.push(ExprImpl::InputRef(Box::new(input_ref)));
            }
        }

        Ok(Some(exprs))
    }

    fn create_non_shared_source_plan(core: generic::Source) -> Result<StreamPlanRef> {
        let mut plan;
        if core.is_new_fs_connector() {
            // Streaming file sources list objects repeatedly and need a persistent
            // file-name dedup. FULL_RELOAD sources must reprocess the complete
            // current object set on every refresh, including names seen earlier.
            let dedup = !Self::is_full_reload_refresh(&core);
            plan = Self::create_list_plan(core.clone(), dedup)?;
            plan = StreamFsFetch::new(plan, core).into();
        } else if core.is_iceberg_connector() || core.is_batch_connector() {
            plan = Self::create_list_plan(core.clone(), false)?;
            plan = StreamFsFetch::new(plan, core).into();
        } else {
            plan = StreamSource::new(core).into()
        }
        Ok(plan)
    }

    fn is_full_reload_refresh(core: &generic::Source) -> bool {
        core.catalog.as_ref().is_some_and(|catalog| {
            catalog.refresh_mode.as_ref().is_some_and(|refresh_mode| {
                matches!(refresh_mode.refresh_mode, Some(RefreshMode::FullReload(_)))
            })
        })
    }

    /// `StreamSource` (list) -> shuffle -> (optional) `StreamDedup`
    fn create_list_plan(core: generic::Source, dedup: bool) -> Result<StreamPlanRef> {
        let downstream_columns = core.column_catalog.clone();
        let logical_source = generic::Source::file_list_node(core);
        let mut list_plan: StreamPlanRef = StreamSource {
            base: PlanBase::new_stream_with_core(
                &logical_source,
                Distribution::Single,
                StreamKind::AppendOnly, // `list` will keep listing all objects, it must be append-only
                false,
                WatermarkColumns::new(),
                MonotonicityMap::new(),
            ),
            core: logical_source,
            downstream_columns: Some(downstream_columns),
        }
        .into();
        list_plan = RequiredDist::shard_by_key(list_plan.schema().len(), &[0])
            .streaming_enforce_if_not_satisfies(list_plan)?;
        if dedup {
            list_plan = StreamDedup::new(generic::Dedup {
                input: list_plan,
                dedup_cols: vec![0],
            })
            .into();
        }

        Ok(list_plan)
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }

    pub fn clone_with_column_catalog(&self, column_catalog: Vec<ColumnCatalog>) -> Result<Self> {
        let row_id_index = column_catalog.iter().position(|c| c.is_row_id_column());
        let kind = self.core.kind.clone();
        let ctx = self.core.ctx.clone();
        let as_of = self.core.as_of.clone();
        Self::new(
            self.source_catalog(),
            column_catalog,
            row_id_index,
            kind,
            ctx,
            as_of,
        )
    }

    fn prune_col_for_iceberg_source(&self, required_cols: &[usize]) -> PlanRef {
        assert!(self.core.is_iceberg_connector());
        // Iceberg source supports column pruning at source level
        // Schema invariant: [table columns] + [_iceberg_sequence_number, _iceberg_file_path, _iceberg_file_pos, _row_id]
        // The last 4 columns are always: 3 iceberg hidden columns + _row_id

        let schema_len = self.schema().len();
        assert!(
            schema_len >= 4,
            "Iceberg source must have at least 4 columns (3 iceberg hidden + 1 row_id)"
        );

        assert_eq!(
            self.core.column_catalog[schema_len - 4].name(),
            ICEBERG_SEQUENCE_NUM_COLUMN_NAME
        );
        assert_eq!(
            self.core.column_catalog[schema_len - 3].name(),
            ICEBERG_FILE_PATH_COLUMN_NAME
        );
        assert_eq!(
            self.core.column_catalog[schema_len - 2].name(),
            ICEBERG_FILE_POS_COLUMN_NAME
        );
        assert_eq!(
            self.core.column_catalog[schema_len - 1].name(),
            ROW_ID_COLUMN_NAME
        );
        assert_eq!(self.output_row_id_index, Some(self.schema().len() - 1));

        let iceberg_start_idx = schema_len - 4;
        let row_id_idx = schema_len - 1;

        // Build source_cols: table columns from required_cols + always keep last 4 columns
        let mut source_cols = Vec::new();

        // Collect table columns (before the last 4 columns) from required_cols
        for &idx in required_cols {
            if idx < iceberg_start_idx {
                // Regular table column
                source_cols.push(idx);
            }
        }

        // Always append the last 4 columns: [_iceberg_sequence_number, _iceberg_file_path, _iceberg_file_pos, _row_id]
        source_cols.extend([
            iceberg_start_idx,
            iceberg_start_idx + 1,
            iceberg_start_idx + 2,
            row_id_idx,
        ]);

        // Clone with pruned columns - source_cols is never empty (always has last 4 columns)
        let mut core = self.core.clone();
        core.column_catalog = source_cols
            .iter()
            .map(|idx| core.column_catalog[*idx].clone())
            .collect();
        // row_id is always at the last position in the pruned schema
        core.row_id_index = Some(source_cols.len() - 1);

        let base = PlanBase::new_logical_with_core(&core);
        let output_exprs =
            Self::derive_output_exprs_from_generated_columns(&core.column_catalog).unwrap();
        let (core, _) = core.exclude_generated_columns();

        let pruned_source = LogicalSource {
            base,
            core,
            output_exprs,
            output_row_id_index: Some(source_cols.len() - 1),
        };

        // Build mapping from original schema indices to pruned schema indices
        let mut old_to_new = vec![None; self.schema().len()];
        for (new_idx, &old_idx) in source_cols.iter().enumerate() {
            old_to_new[old_idx] = Some(new_idx);
        }

        // Map required_cols to indices in the pruned schema
        let new_required: Vec<_> = required_cols
            .iter()
            .map(|&old_idx| old_to_new[old_idx].unwrap())
            .collect();

        let mapping =
            ColIndexMapping::with_remaining_columns(&new_required, pruned_source.schema().len());
        LogicalProject::with_mapping(pruned_source.into(), mapping).into()
    }

    pub fn is_shared_source(&self) -> bool {
        // Create MV on source.
        // We only check streaming_use_shared_source is true when `CREATE SOURCE`.
        // The value does not affect the behavior of `CREATE MATERIALIZED VIEW` here.
        self.source_catalog().is_some_and(|c| c.info.is_shared())
    }
}

impl_plan_tree_node_for_leaf! { Logical, LogicalSource}
impl Distill for LogicalSource {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = if let Some(catalog) = self.source_catalog() {
            let src = Pretty::from(catalog.name.clone());
            let mut fields = vec![
                ("source", src),
                ("is_shared", Pretty::debug(&catalog.info.is_shared())),
                ("columns", column_names_pretty(self.schema())),
            ];
            if let Some(as_of) = &self.core.as_of {
                fields.push(("as_of", Pretty::debug(as_of)));
            }
            fields
        } else {
            vec![]
        };
        childless_record("LogicalSource", fields)
    }
}

impl ColPrunable for LogicalSource {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        // For refreshable iceberg table, we do not expose iceberg hidden columns to the user.
        if self.core.is_iceberg_connector() && !Self::is_full_reload_refresh(&self.core) {
            self.prune_col_for_iceberg_source(required_cols)
        } else {
            // For other sources, use a LogicalProject to prune columns
            let mapping =
                ColIndexMapping::with_remaining_columns(required_cols, self.schema().len());
            LogicalProject::with_mapping(self.clone().into(), mapping).into()
        }
    }
}

impl ExprRewritable<Logical> for LogicalSource {
    fn has_rewritable_expr(&self) -> bool {
        self.output_exprs.is_some()
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut output_exprs = self.output_exprs.clone();

        for expr in output_exprs.iter_mut().flatten() {
            *expr = r.rewrite_expr(expr.clone());
        }

        Self {
            output_exprs,
            ..self.clone()
        }
        .into()
    }
}

impl ExprVisitable for LogicalSource {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.output_exprs
            .iter()
            .flatten()
            .for_each(|e| v.visit_expr(e));
    }
}

impl PredicatePushdown for LogicalSource {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalSource {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        assert!(
            !self.core.is_kafka_connector(),
            "LogicalSource with a kafka property should be converted to LogicalKafkaScan"
        );
        assert!(
            !self.core.is_iceberg_connector(),
            "LogicalSource with a iceberg property should be converted to LogicalIcebergScan"
        );
        let mut plan = BatchSource::new(self.core.clone()).into();

        if let Some(exprs) = &self.output_exprs {
            let logical_project = generic::Project::new(exprs.clone(), plan);
            plan = BatchProject::new(logical_project).into();
        }

        Ok(plan)
    }
}

impl ToStream for LogicalSource {
    fn to_stream(
        &self,
        _ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        let mut plan;

        match self.core.kind {
            SourceNodeKind::CreateTable | SourceNodeKind::CreateSharedSource => {
                // Note: for create table, row_id and generated columns is created in plan_root.gen_table_plan.
                // for shared source, row_id and generated columns is created after SourceBackfill node.
                plan = Self::create_non_shared_source_plan(self.core.clone())?;
            }
            SourceNodeKind::CreateMViewOrBatch => {
                if self.is_shared_source() {
                    plan = StreamSourceScan::new(self.core.clone()).into();
                } else {
                    // non-shared source
                    plan = Self::create_non_shared_source_plan(self.core.clone())?;
                }

                if let Some(exprs) = &self.output_exprs {
                    let logical_project = generic::Project::new(exprs.clone(), plan);
                    plan = if let Some(stream_key) = self.base.stream_key() {
                        StreamProject::new_with_stream_key(logical_project, stream_key.to_vec())
                            .into()
                    } else {
                        StreamProject::new(logical_project).into()
                    };
                }

                if let Some(row_id_index) = self.output_row_id_index {
                    plan = StreamRowIdGen::new_with_dist(
                        plan,
                        row_id_index,
                        HashShard(vec![row_id_index]),
                    )
                    .into();
                }

                if let Some(catalog) = self.source_catalog()
                    && !catalog.watermark_descs.is_empty()
                {
                    plan = StreamWatermarkFilter::new(plan, catalog.watermark_descs.clone()).into();
                }
            }
        }
        Ok(plan)
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        Ok((
            self.clone().into(),
            ColIndexMapping::identity(self.schema().len()),
        ))
    }
}
