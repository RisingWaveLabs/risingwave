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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{ColumnDesc, Schema};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_sqlparser::ast::AsOf;

use super::generic::{GenericPlanNode, GenericPlanRef};
use super::utils::{Distill, childless_record};
use super::{
    BackfillType, BatchFilter, BatchPlanRef, BatchProject, ColPrunable, ExprRewritable, Logical,
    LogicalPlanRef as PlanRef, PlanBase, PlanNodeId, PredicatePushdown, StreamFilter,
    StreamProject, StreamTableScan, ToBatch, ToStream, generic,
};
use crate::TableCatalog;
use crate::binder::BoundBaseTable;
use crate::catalog::ColumnId;
use crate::catalog::index_catalog::{IndexType, TableIndex, VectorIndex};
use crate::error::{ErrorCode, Result};
use crate::expr::{CorrelatedInputRef, ExprImpl, ExprRewriter, ExprVisitor, InputRef};
use crate::optimizer::ApplyResult;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::plan_node_meta::AnyPlanNodeMeta;
use crate::optimizer::plan_node::{
    BatchSeqScan, ColumnPruningContext, LogicalFilter, LogicalProject, LogicalValues,
    PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::{Cardinality, FunctionalDependencySet, Order, WatermarkColumns};
use crate::optimizer::rule::{IndexSelectionRule, StreamingIndexSelectionRule};
use crate::utils::{ColIndexMapping, Condition, ConditionDisplay};

/// `LogicalScan` returns contents of a table or other equivalent object
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalScan {
    pub base: PlanBase<Logical>,
    core: generic::TableScan,
}

impl From<generic::TableScan> for LogicalScan {
    fn from(core: generic::TableScan) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }
}

impl From<generic::TableScan> for PlanRef {
    fn from(core: generic::TableScan) -> Self {
        LogicalScan::from(core).into()
    }
}

impl GenericPlanRef for LogicalScan {
    fn id(&self) -> PlanNodeId {
        self.plan_base().id()
    }

    fn schema(&self) -> &Schema {
        self.plan_base().schema()
    }

    fn stream_key(&self) -> Option<&[usize]> {
        self.plan_base().stream_key()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.plan_base().ctx()
    }

    fn functional_dependency(&self) -> &FunctionalDependencySet {
        self.plan_base().functional_dependency()
    }
}

impl LogicalScan {
    /// Create a [`LogicalScan`] node. Used by planner.
    pub fn create(
        table_catalog: Arc<TableCatalog>,
        ctx: OptimizerContextRef,
        as_of: Option<AsOf>,
    ) -> Self {
        let output_col_idx: Vec<usize> = (0..table_catalog.columns().len()).collect();
        generic::TableScan::new(
            output_col_idx,
            table_catalog,
            vec![],
            vec![],
            ctx,
            Condition::true_cond(),
            as_of,
        )
        .into()
    }

    pub fn from_base_table(
        base_table: &BoundBaseTable,
        ctx: OptimizerContextRef,
        as_of: Option<AsOf>,
    ) -> Self {
        let table_catalog = base_table.table_catalog.clone();
        let output_col_idx: Vec<usize> = (0..table_catalog.columns().len()).collect();
        let mut table_indexes = vec![];
        let mut vector_indexes = vec![];
        for index in &base_table.table_indexes {
            match &index.index_type {
                IndexType::Table(index) => table_indexes.push(index.clone()),
                IndexType::Vector(index) => vector_indexes.push(index.clone()),
            }
        }
        generic::TableScan::new(
            output_col_idx,
            table_catalog,
            table_indexes,
            vector_indexes,
            ctx,
            Condition::true_cond(),
            as_of,
        )
        .into()
    }

    pub fn table_name(&self) -> &str {
        &self.core.table_catalog.name
    }

    pub fn as_of(&self) -> Option<AsOf> {
        self.core.as_of.clone()
    }

    /// The cardinality of the table **without** applying the predicate.
    pub fn table_cardinality(&self) -> Cardinality {
        self.core.table_catalog.cardinality
    }

    pub fn table(&self) -> &Arc<TableCatalog> {
        &self.core.table_catalog
    }

    /// Get the descs of the output columns.
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.core.column_descs()
    }

    /// Get the ids of the output columns.
    pub fn output_column_ids(&self) -> Vec<ColumnId> {
        self.core.output_column_ids()
    }

    /// Get all table indexes on this table
    pub fn table_indexes(&self) -> &[Arc<TableIndex>] {
        &self.core.table_indexes
    }

    /// Get all vector indexes on this table
    pub fn vector_indexes(&self) -> &[Arc<VectorIndex>] {
        &self.core.vector_indexes
    }

    /// Get the logical scan's filter predicate
    pub fn predicate(&self) -> &Condition {
        &self.core.predicate
    }

    /// Return indices of fields the output is ordered by and
    /// corresponding direction
    pub fn get_out_column_index_order(&self) -> Order {
        self.core.get_out_column_index_order()
    }

    pub fn distribution_key(&self) -> Option<Vec<usize>> {
        self.core.distribution_key()
    }

    pub fn watermark_columns(&self) -> WatermarkColumns {
        self.core.watermark_columns()
    }

    pub fn cross_database(&self) -> bool {
        self.core.cross_database()
    }

    /// Return indexes can satisfy the required order.
    pub fn indexes_satisfy_order(&self, required_order: &Order) -> Vec<&Arc<TableIndex>> {
        self.indexes_satisfy_order_with_prefix(required_order, &HashSet::new())
            .into_iter()
            .map(|(index, _)| index)
            .collect()
    }

    /// Return indexes can satisfy the required order.
    /// The `prefix` refers to optionally matching columns of the index
    /// It is unordered initially.
    /// If any are used, we will return the fixed `Order` prefix.
    pub fn indexes_satisfy_order_with_prefix(
        &self,
        required_order: &Order,
        prefix: &HashSet<ColumnOrder>,
    ) -> Vec<(&Arc<TableIndex>, Order)> {
        let output_col_map = self
            .output_col_idx()
            .iter()
            .cloned()
            .enumerate()
            .map(|(id, col)| (col, id))
            .collect::<BTreeMap<_, _>>();
        let unmatched_idx = output_col_map.len();
        let mut index_catalog_and_orders = vec![];
        for index in self.table_indexes() {
            let s2p_mapping = index.secondary_to_primary_mapping();
            let index_orders: Vec<ColumnOrder> = index
                .index_table
                .pk()
                .iter()
                .map(|idx_item| {
                    let idx = match s2p_mapping.get(&idx_item.column_index) {
                        Some(col_idx) => *output_col_map.get(col_idx).unwrap_or(&unmatched_idx),
                        // After we support index on expressions, we need to handle the case where the column is not in the `s2p_mapping`.
                        None => unmatched_idx,
                    };
                    ColumnOrder::new(idx, idx_item.order_type)
                })
                .collect();

            let mut index_orders_iter = index_orders.into_iter().peekable();

            // First check the prefix
            let fixed_prefix = {
                let mut fixed_prefix = vec![];
                loop {
                    match index_orders_iter.peek() {
                        Some(index_col_order) if prefix.contains(index_col_order) => {
                            let index_col_order = index_orders_iter.next().unwrap();
                            fixed_prefix.push(index_col_order);
                        }
                        _ => break,
                    }
                }
                Order {
                    column_orders: fixed_prefix,
                }
            };

            let remaining_orders = Order {
                column_orders: index_orders_iter.collect(),
            };
            if remaining_orders.satisfies(required_order) {
                index_catalog_and_orders.push((index, fixed_prefix));
            }
        }
        index_catalog_and_orders
    }

    /// If the index can cover the scan, transform it to the index scan.
    pub fn to_index_scan_if_index_covered(&self, index: &Arc<TableIndex>) -> Option<LogicalScan> {
        let p2s_mapping = index.primary_to_secondary_mapping();
        if self
            .required_col_idx()
            .iter()
            .all(|x| p2s_mapping.contains_key(x))
        {
            let index_scan = self.core.to_index_scan(
                index.index_table.clone(),
                p2s_mapping,
                index.function_mapping(),
            );
            Some(index_scan.into())
        } else {
            None
        }
    }

    pub fn primary_key(&self) -> &[ColumnOrder] {
        self.core.primary_key()
    }

    /// a vec of `InputRef` corresponding to `output_col_idx`, which can represent a pulled project.
    fn output_idx_to_input_ref(&self) -> Vec<ExprImpl> {
        self.output_col_idx()
            .iter()
            .enumerate()
            .map(|(i, &col_idx)| {
                InputRef::new(
                    i,
                    self.table().columns[col_idx].column_desc.data_type.clone(),
                )
                .into()
            })
            .collect_vec()
    }

    /// Undo predicate push down when predicate in scan is not supported.
    pub fn predicate_pull_up(&self) -> (generic::TableScan, Condition, Option<Vec<ExprImpl>>) {
        let mut predicate = self.predicate().clone();
        if predicate.always_true() {
            return (self.core.clone(), Condition::true_cond(), None);
        }

        let mut inverse_mapping = {
            let mapping = ColIndexMapping::new(
                self.required_col_idx().iter().map(|i| Some(*i)).collect(),
                self.table().columns.len(),
            );
            // Since `required_col_idx` mapping is not invertible, we need to inverse manually.
            let mut inverse_map = vec![None; mapping.target_size()];
            for (src, dst) in mapping.mapping_pairs() {
                inverse_map[dst] = Some(src);
            }
            ColIndexMapping::new(inverse_map, mapping.source_size())
        };

        predicate = predicate.rewrite_expr(&mut inverse_mapping);

        let scan_without_predicate = generic::TableScan::new(
            self.required_col_idx().clone(),
            self.core.table_catalog.clone(),
            self.table_indexes().to_vec(),
            self.vector_indexes().to_vec(),
            self.ctx(),
            Condition::true_cond(),
            self.as_of(),
        );
        let project_expr = if self.required_col_idx() != self.output_col_idx() {
            Some(self.output_idx_to_input_ref())
        } else {
            None
        };
        (scan_without_predicate, predicate, project_expr)
    }

    pub(crate) fn clone_with_predicate(&self, predicate: Condition) -> Self {
        generic::TableScan::new_inner(
            self.output_col_idx().clone(),
            self.table().clone(),
            self.table_indexes().to_vec(),
            self.vector_indexes().to_vec(),
            self.base.ctx(),
            predicate,
            self.as_of(),
            self.core.table_scan_stream_key().map(ToOwned::to_owned),
        )
        .into()
    }

    pub fn clone_with_output_indices(&self, output_col_idx: Vec<usize>) -> Self {
        generic::TableScan::new_inner(
            output_col_idx,
            self.core.table_catalog.clone(),
            self.table_indexes().to_vec(),
            self.vector_indexes().to_vec(),
            self.base.ctx(),
            self.predicate().clone(),
            self.as_of(),
            self.core.table_scan_stream_key().map(ToOwned::to_owned),
        )
        .into()
    }

    pub fn output_col_idx(&self) -> &Vec<usize> {
        &self.core.output_col_idx
    }

    pub fn required_col_idx(&self) -> &Vec<usize> {
        &self.core.required_col_idx
    }
}

impl_plan_tree_node_for_leaf! { Logical, LogicalScan}

impl Distill for LogicalScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(5);
        vec.push(("table", Pretty::from(self.table_name().to_owned())));
        let key_is_columns =
            self.predicate().always_true() || self.output_col_idx() == self.required_col_idx();
        let key = if key_is_columns {
            "columns"
        } else {
            "output_columns"
        };
        vec.push((key, self.core.columns_pretty(verbose)));
        if !key_is_columns {
            vec.push((
                "required_columns",
                Pretty::Array(
                    self.required_col_idx()
                        .iter()
                        .map(|i| {
                            let col_name = &self.table().columns[*i].name;
                            Pretty::from(if verbose {
                                format!("{}.{}", self.table_name(), col_name)
                            } else {
                                col_name.clone()
                            })
                        })
                        .collect(),
                ),
            ));
        }

        if !self.predicate().always_true() {
            let input_schema = self.core.fields_pretty_schema();
            vec.push((
                "predicate",
                Pretty::display(&ConditionDisplay {
                    condition: self.predicate(),
                    input_schema: &input_schema,
                }),
            ))
        }

        if self.table_cardinality() != Cardinality::unknown() {
            vec.push(("cardinality", Pretty::display(&self.table_cardinality())));
        }

        childless_record("LogicalScan", vec)
    }
}

impl ColPrunable for LogicalScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        let output_col_idx: Vec<usize> = required_cols
            .iter()
            .map(|i| self.required_col_idx()[*i])
            .collect();
        assert!(
            output_col_idx
                .iter()
                .all(|i| self.output_col_idx().contains(i))
        );

        self.clone_with_output_indices(output_col_idx).into()
    }
}

impl ExprRewritable<Logical> for LogicalScan {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self {
            base: self.base.clone_with_new_plan_id(),
            core,
        }
        .into()
    }
}

impl ExprVisitable for LogicalScan {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

impl PredicatePushdown for LogicalScan {
    fn predicate_pushdown(
        &self,
        mut predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // If the predicate contains `CorrelatedInputRef` or `now()`. We don't push down.
        // This case could come from the predicate push down before the subquery unnesting.
        struct HasCorrelated {
            has: bool,
        }
        impl ExprVisitor for HasCorrelated {
            fn visit_correlated_input_ref(&mut self, _: &CorrelatedInputRef) {
                self.has = true;
            }
        }
        let non_pushable_predicate: Vec<_> = predicate
            .conjunctions
            .extract_if(.., |expr| {
                if expr.count_nows() > 0 {
                    true
                } else {
                    let mut visitor = HasCorrelated { has: false };
                    visitor.visit_expr(expr);
                    visitor.has
                }
            })
            .collect();
        let predicate = predicate.rewrite_expr(&mut ColIndexMapping::new(
            self.output_col_idx().iter().map(|i| Some(*i)).collect(),
            self.table().columns.len(),
        ));
        if non_pushable_predicate.is_empty() {
            self.clone_with_predicate(predicate.and(self.predicate().clone()))
                .into()
        } else {
            LogicalFilter::create(
                self.clone_with_predicate(predicate.and(self.predicate().clone()))
                    .into(),
                Condition {
                    conjunctions: non_pushable_predicate,
                },
            )
        }
    }
}

impl LogicalScan {
    fn to_batch_inner_with_required(&self, required_order: &Order) -> Result<BatchPlanRef> {
        if self.predicate().always_true() {
            required_order
                .enforce_if_not_satisfies(BatchSeqScan::new(self.core.clone(), vec![], None).into())
        } else {
            let (scan_ranges, predicate) = self.predicate().clone().split_to_scan_ranges(
                self.table(),
                self.base.ctx().session_ctx().config().max_split_range_gap() as u64,
            )?;
            let mut scan = self.clone();
            scan.core.predicate = predicate; // We want to keep `required_col_idx` unchanged, so do not call `clone_with_predicate`.

            let plan: BatchPlanRef = if scan.core.predicate.always_false() {
                LogicalValues::create(vec![], scan.core.schema(), scan.core.ctx).to_batch()?
            } else {
                let (scan, predicate, project_expr) = scan.predicate_pull_up();

                let mut plan: BatchPlanRef = BatchSeqScan::new(scan, scan_ranges, None).into();
                if !predicate.always_true() {
                    plan = BatchFilter::new(generic::Filter::new(predicate, plan)).into();
                }
                if let Some(exprs) = project_expr {
                    plan = BatchProject::new(generic::Project::new(exprs, plan)).into()
                }
                plan
            };

            assert_eq!(plan.schema(), self.schema());
            required_order.enforce_if_not_satisfies(plan)
        }
    }

    // For every index, check if the order of the index satisfies the required_order
    // If yes, use an index scan
    fn use_index_scan_if_order_is_satisfied(
        &self,
        required_order: &Order,
    ) -> Option<Result<BatchPlanRef>> {
        if required_order.column_orders.is_empty() {
            return None;
        }

        let order_satisfied_index = self.indexes_satisfy_order(required_order);
        for index in order_satisfied_index {
            if let Some(index_scan) = self.to_index_scan_if_index_covered(index) {
                return Some(index_scan.to_batch());
            }
        }

        None
    }
}

impl ToBatch for LogicalScan {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        self.to_batch_with_order_required(&Order::any())
    }

    fn to_batch_with_order_required(
        &self,
        required_order: &Order,
    ) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        let new = self.clone_with_predicate(self.predicate().clone());

        if !new.table_indexes().is_empty()
            && self
                .base
                .ctx()
                .session_ctx()
                .config()
                .enable_index_selection()
        {
            let index_selection_rule = IndexSelectionRule::create();
            if let ApplyResult::Ok(applied) = index_selection_rule.apply(new.clone().into()) {
                if let Some(scan) = applied.as_logical_scan() {
                    // covering index
                    return required_order.enforce_if_not_satisfies(scan.to_batch()?);
                } else if let Some(join) = applied.as_logical_join() {
                    // index lookup join
                    if let Some(lookup_join) = join.index_lookup_join_to_batch_lookup_join()? {
                        return required_order.enforce_if_not_satisfies(lookup_join);
                    }
                } else {
                    unreachable!();
                }
            }

            // Try to make use of index if it satisfies the required order.
            // Also reach here when a cost-selected non-covering index candidate cannot be
            // converted to a physical lookup join.
            if let Some(plan_ref) = new.use_index_scan_if_order_is_satisfied(required_order) {
                return plan_ref;
            }
        }
        new.to_batch_inner_with_required(required_order)
    }
}

impl ToStream for LogicalScan {
    fn to_stream(
        &self,
        ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        if self.predicate().always_true() {
            if self.core.cross_database()
                && (ctx.backfill_type() == BackfillType::Replicated
                    || ctx.backfill_type().without_snapshot())
            {
                return Err(ErrorCode::NotSupported(
                    "We currently do not support cross database scan in replicated or without-snapshot mode."
                        .to_owned(),
                    "Please ensure the source table is in the same database.".to_owned(),
                )
                .into());
            }

            Ok(
                StreamTableScan::new_with_backfill_type(self.core.clone(), ctx.backfill_type())
                    .into(),
            )
        } else {
            if ctx.backfill_type().is_snapshot_backfill() {
                let (scan_ranges, _residual) = self.predicate().clone().split_to_scan_ranges(
                    self.table(),
                    self.base.ctx().session_ctx().config().max_split_range_gap() as u64,
                )?;

                if scan_ranges.len() == 1 && !scan_ranges[0].is_full_table_scan() {
                    // Single scan range — direct pushdown.
                    let (core, predicate, project_expr) = self.predicate_pull_up();
                    let scan = StreamTableScan::new_with_scan_range(
                        core,
                        ctx.backfill_type(),
                        Some(scan_ranges.into_iter().next().unwrap()),
                    );
                    LogicalFilter::check_stream_predicate(&predicate)?;

                    let mut plan: crate::optimizer::plan_node::StreamPlanRef =
                        StreamFilter::new(generic::Filter::new(predicate, scan.into())).into();
                    if let Some(exprs) = project_expr {
                        plan = StreamProject::new(generic::Project::new(exprs, plan)).into();
                    }
                    return Ok(plan);
                }
            }

            let (scan, predicate, project_expr) = self.predicate_pull_up();
            let mut plan = LogicalFilter::create(scan.into(), predicate);
            if let Some(exprs) = project_expr {
                plan = LogicalProject::create(plan, exprs)
            }
            plan.to_stream(ctx)
        }
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        // For snapshot backfill, apply streaming index selection rule which handles:
        // 1. Covering index selection (picks lowest-cost covering index)
        // 2. IN expansion (splits IN predicates into LogicalUnion of LogicalScans)
        // This must happen here (not in to_stream) so that upper operators see the
        // correct stream key from the rewritten plan.
        if ctx.backfill_type().is_snapshot_backfill()
            && !self.predicate().always_true()
            && self
                .base
                .ctx()
                .session_ctx()
                .config()
                .enable_index_selection()
            && let Some(rewritten) = StreamingIndexSelectionRule::rewrite(self)
        {
            return rewritten.logical_rewrite_for_stream(ctx);
        }

        let mut output_col_idx = self.output_col_idx().clone();
        let original_len = output_col_idx.len();

        // Ensure pk columns are in the output.
        for idx in self.table().pk.iter().map(|c| c.column_index) {
            if !output_col_idx.contains(&idx) {
                output_col_idx.push(idx);
            }
        }
        // Ensure catalog stream-key columns are in the output for stream planning.
        for idx in self.table().stream_key() {
            if !output_col_idx.contains(&idx) {
                output_col_idx.push(idx);
            }
        }

        let (mut scan, col_index_mapping) = if output_col_idx.len() == original_len {
            (self.clone(), ColIndexMapping::identity(self.schema().len()))
        } else {
            let new_len = output_col_idx.len();
            (
                self.clone_with_output_indices(output_col_idx),
                ColIndexMapping::identity_or_none(self.schema().len(), new_len),
            )
        };

        if ctx.backfill_type().is_snapshot_backfill() || self.core.cross_database() {
            // Snapshot and cross-database backfill must use the upstream table primary key while
            // planning operators above the scan, before `StreamTableScan` is built. Other scan
            // types keep the logical stream key here so they preserve the original
            // normal-backfill behavior.
            scan.core.extend_table_scan_stream_key_with_primary_key();
            scan.base = PlanBase::new_logical_with_core(&scan.core);
        }
        Ok((scan.into(), col_index_mapping))
    }

    fn try_better_locality(&self, columns: &[usize]) -> Option<PlanRef> {
        if columns.is_empty() {
            return None;
        }
        let enable_index_selection = self
            .core
            .ctx()
            .session_ctx()
            .config()
            .enable_index_selection();
        let has_indexes = !self.table_indexes().is_empty();
        let primary_order = self.get_out_column_index_order();
        let primary_dist_key_satisfied = self
            .distribution_key()
            .is_some_and(|dist_key| dist_key.iter().all(|k| columns.contains(k)));
        let orders = if columns.len() <= 3 {
            OrderType::all()
        } else {
            // Limit the number of order type combinations to avoid explosion.
            // For more than 3 columns, we only consider ascending nulls last and descending.
            // Since by default, indexes are created with ascending nulls last.
            // This is a heuristic to reduce the search space.
            vec![OrderType::ascending_nulls_last(), OrderType::descending()]
        };
        for order_type_combo in columns
            .iter()
            .map(|&col| orders.iter().map(move |ot| ColumnOrder::new(col, *ot)))
            .multi_cartesian_product()
            .take(256)
        // limit the number of combinations
        {
            let required_order = Order {
                column_orders: order_type_combo,
            };

            if primary_dist_key_satisfied && primary_order.satisfies(&required_order) {
                return Some(self.clone().into());
            }

            if !enable_index_selection || !has_indexes {
                continue;
            }

            let order_satisfied_index = self.indexes_satisfy_order(&required_order);
            for index in order_satisfied_index {
                if let Some(index_scan) = self.to_index_scan_if_index_covered(index) {
                    // The selected index's distribution key must be the subset the locality columns.
                    // Because index's stream key is [distribution key] + [primary table's primary key].
                    // For streaming queries, we have to ensure any updates ordering (U-/U+) isn't disturbed
                    // after the later shuffle introduced by the locality operator,
                    // so we have to ensure the distribution key of the index scan is the subset of the locality columns.
                    if let Some(dist_key) = index_scan.distribution_key()
                        && dist_key.iter().all(|k| columns.contains(k))
                    {
                        return Some(index_scan.into());
                    }
                }
            }
        }
        None
    }
}
