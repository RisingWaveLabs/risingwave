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

use fixedbitset::FixedBitSet;
use itertools::{EitherOrBoth, Itertools};
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::ColumnId;
use risingwave_expr::bail;
use risingwave_pb::expr::expr_node::PbType;
use risingwave_pb::plan_common::{AsOfJoinDesc, JoinType, PbAsOfJoinInequalityType};
use risingwave_sqlparser::ast::AsOf;

use super::generic::{
    GenericPlanNode, GenericPlanRef, push_down_into_join, push_down_join_condition,
};
use super::utils::{Distill, childless_record};
use super::{
    BackfillType, BatchPlanRef, ColPrunable, ExprRewritable, Logical, LogicalPlanRef as PlanRef,
    PlanBase, PlanTreeNodeBinary, PredicatePushdown, StreamHashJoin, StreamPlanRef, StreamProject,
    ToBatch, ToStream, generic, try_enforce_locality_requirement,
};
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{CollectInputRef, Expr, ExprImpl, ExprRewriter, ExprType, ExprVisitor, InputRef};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::DynamicFilter;
use crate::optimizer::plan_node::stream_asof_join::StreamAsOfJoin;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{
    BatchHashJoin, BatchLookupJoin, BatchNestedLoopJoin, ColumnPruningContext, EqJoinPredicate,
    LogicalFilter, LogicalScan, PredicatePushdownContext, RewriteStreamContext,
    StreamDynamicFilter, StreamFilter, StreamTableScan, StreamTemporalJoin, ToStreamContext,
};
use crate::optimizer::plan_visitor::LogicalCardinalityExt;
use crate::optimizer::property::{Distribution, RequiredDist};
use crate::utils::{ColIndexMapping, ColIndexMappingRewriteExt, Condition, ConditionDisplay};

/// `LogicalJoin` combines two relations according to some condition.
///
/// Each output row has fields from the left and right inputs. The set of output rows is a subset
/// of the cartesian product of the two inputs; precisely which subset depends on the join
/// condition. In addition, the output columns are a subset of the columns of the left and
/// right columns, dependent on the output indices provided. A repeat output index is illegal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalJoin {
    pub base: PlanBase<Logical>,
    core: generic::Join<PlanRef>,
}

impl Distill for LogicalJoin {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(if verbose { 3 } else { 2 });
        vec.push(("type", Pretty::debug(&self.join_type())));

        let concat_schema = self.core.concat_schema();
        let cond = Pretty::debug(&ConditionDisplay {
            condition: self.on(),
            input_schema: &concat_schema,
        });
        vec.push(("on", cond));

        if verbose {
            let data = IndicesDisplay::from_join(&self.core, &concat_schema);
            vec.push(("output", data));
        }

        childless_record("LogicalJoin", vec)
    }
}

impl LogicalJoin {
    pub(crate) fn new(left: PlanRef, right: PlanRef, join_type: JoinType, on: Condition) -> Self {
        let core = generic::Join::with_full_output(left, right, join_type, on);
        Self::with_core(core)
    }

    pub(crate) fn with_output_indices(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on: Condition,
        output_indices: Vec<usize>,
    ) -> Self {
        let core = generic::Join::new(left, right, on, join_type, output_indices);
        Self::with_core(core)
    }

    pub fn with_core(core: generic::Join<PlanRef>) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        LogicalJoin { base, core }
    }

    pub fn create(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on_clause: ExprImpl,
    ) -> PlanRef {
        Self::new(left, right, join_type, Condition::with_expr(on_clause)).into()
    }

    pub fn internal_column_num(&self) -> usize {
        self.core.internal_column_num()
    }

    pub fn i2l_col_mapping_ignore_join_type(&self) -> ColIndexMapping {
        self.core.i2l_col_mapping_ignore_join_type()
    }

    pub fn i2r_col_mapping_ignore_join_type(&self) -> ColIndexMapping {
        self.core.i2r_col_mapping_ignore_join_type()
    }

    /// Get a reference to the logical join's on.
    pub fn on(&self) -> &Condition {
        self.core
            .on
            .as_condition_ref()
            .expect("logical join should store predicate as Condition")
    }

    pub fn core(&self) -> &generic::Join<PlanRef> {
        &self.core
    }

    /// Collect all input ref in the on condition. And separate them into left and right.
    pub fn input_idx_on_condition(&self) -> (Vec<usize>, Vec<usize>) {
        let input_refs = self
            .core
            .on
            .as_condition_ref()
            .expect("logical join should store predicate as Condition")
            .collect_input_refs(self.core.left.schema().len() + self.core.right.schema().len());
        let index_group = input_refs
            .ones()
            .chunk_by(|i| *i < self.core.left.schema().len());
        let left_index = index_group
            .into_iter()
            .next()
            .map_or(vec![], |group| group.1.collect_vec());
        let right_index = index_group.into_iter().next().map_or(vec![], |group| {
            group
                .1
                .map(|i| i - self.core.left.schema().len())
                .collect_vec()
        });
        (left_index, right_index)
    }

    /// Get the join type of the logical join.
    pub fn join_type(&self) -> JoinType {
        self.core.join_type
    }

    /// Get the eq join key of the logical join.
    pub fn eq_indexes(&self) -> Vec<(usize, usize)> {
        self.core.eq_indexes()
    }

    /// Get the output indices of the logical join.
    pub fn output_indices(&self) -> &Vec<usize> {
        &self.core.output_indices
    }

    /// Clone with new output indices
    pub fn clone_with_output_indices(&self, output_indices: Vec<usize>) -> Self {
        Self::with_core(generic::Join {
            output_indices,
            ..self.core.clone()
        })
    }

    /// Clone with new `on` condition
    pub fn clone_with_cond(&self, on: Condition) -> Self {
        Self::with_core(generic::Join {
            on: generic::JoinOn::Condition(on),
            ..self.core.clone()
        })
    }

    pub fn is_left_join(&self) -> bool {
        matches!(self.join_type(), JoinType::LeftSemi | JoinType::LeftAnti)
    }

    pub fn is_right_join(&self) -> bool {
        matches!(self.join_type(), JoinType::RightSemi | JoinType::RightAnti)
    }

    pub fn is_full_out(&self) -> bool {
        self.core.is_full_out()
    }

    pub fn is_asof_join(&self) -> bool {
        self.join_type() == JoinType::AsofInner || self.join_type() == JoinType::AsofLeftOuter
    }

    pub fn output_indices_are_trivial(&self) -> bool {
        itertools::equal(
            self.output_indices().iter().cloned(),
            0..self.internal_column_num(),
        )
    }

    /// Try to simplify the outer join with the predicate on the top of the join
    ///
    /// now it is just a naive implementation for comparison expression, we can give a more general
    /// implementation with constant folding in future
    fn simplify_outer(predicate: &Condition, left_col_num: usize, join_type: JoinType) -> JoinType {
        let (mut gen_null_in_left, mut gen_null_in_right) = match join_type {
            JoinType::LeftOuter => (false, true),
            JoinType::RightOuter => (true, false),
            JoinType::FullOuter => (true, true),
            _ => return join_type,
        };

        for expr in &predicate.conjunctions {
            if let ExprImpl::FunctionCall(func) = expr {
                match func.func_type() {
                    ExprType::Equal
                    | ExprType::NotEqual
                    | ExprType::LessThan
                    | ExprType::LessThanOrEqual
                    | ExprType::GreaterThan
                    | ExprType::GreaterThanOrEqual => {
                        for input in func.inputs() {
                            if let ExprImpl::InputRef(input) = input {
                                let idx = input.index;
                                if idx < left_col_num {
                                    gen_null_in_left = false;
                                } else {
                                    gen_null_in_right = false;
                                }
                            }
                        }
                    }
                    _ => {}
                };
            }
        }

        match (gen_null_in_left, gen_null_in_right) {
            (true, true) => JoinType::FullOuter,
            (true, false) => JoinType::RightOuter,
            (false, true) => JoinType::LeftOuter,
            (false, false) => JoinType::Inner,
        }
    }

    /// Index Join:
    /// Try to convert logical join into batch lookup join and meanwhile it will do
    /// the index selection for the lookup table so that we can benefit from indexes.
    fn to_batch_lookup_join_with_index_selection(
        &self,
        predicate: EqJoinPredicate,
        batch_join: generic::Join<BatchPlanRef>,
    ) -> Result<Option<BatchLookupJoin>> {
        match batch_join.join_type {
            JoinType::Inner
            | JoinType::LeftOuter
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::AsofInner
            | JoinType::AsofLeftOuter => {}
            _ => return Ok(None),
        };

        // Index selection for index join.
        let right = self.right();
        // Lookup Join only supports basic tables on the join's right side.
        let logical_scan: &LogicalScan = if let Some(logical_scan) = right.as_logical_scan() {
            logical_scan
        } else {
            return Ok(None);
        };

        let mut result_plan = None;
        // Lookup primary table.
        if let Some(lookup_join) =
            self.to_batch_lookup_join(predicate.clone(), batch_join.clone())?
        {
            result_plan = Some(lookup_join);
        }

        if self
            .core
            .ctx()
            .session_ctx()
            .config()
            .enable_index_selection()
        {
            let indexes = logical_scan.table_indexes();
            for index in indexes {
                if let Some(index_scan) = logical_scan.to_index_scan_if_index_covered(index) {
                    let index_scan: PlanRef = index_scan.into();
                    let that = self.clone_with_left_right(self.left(), index_scan.clone());
                    let mut new_batch_join = batch_join.clone();
                    new_batch_join.right =
                        index_scan.to_batch().expect("index scan failed to batch");

                    // Lookup covered index.
                    if let Some(lookup_join) =
                        that.to_batch_lookup_join(predicate.clone(), new_batch_join)?
                    {
                        match &result_plan {
                            None => result_plan = Some(lookup_join),
                            Some(prev_lookup_join) => {
                                // Prefer to choose lookup join with longer lookup prefix len.
                                if prev_lookup_join.lookup_prefix_len()
                                    < lookup_join.lookup_prefix_len()
                                {
                                    result_plan = Some(lookup_join)
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(result_plan)
    }

    /// Try to convert logical join into batch lookup join.
    fn to_batch_lookup_join(
        &self,
        predicate: EqJoinPredicate,
        logical_join: generic::Join<BatchPlanRef>,
    ) -> Result<Option<BatchLookupJoin>> {
        let logical_scan: &LogicalScan =
            if let Some(logical_scan) = self.core.right.as_logical_scan() {
                logical_scan
            } else {
                return Ok(None);
            };
        Self::gen_batch_lookup_join(logical_scan, predicate, logical_join, self.is_asof_join())
    }

    /// Decides the lookup prefix, as a reordering of the eq keys, by matching the lookup
    /// table's order-key columns. The executor binds eq keys to the order key positionally,
    /// so a repeated order-key column (e.g. an MV with `ORDER BY a, a`) ends the prefix.
    fn lookup_prefix_reorder_idx(
        predicate: &EqJoinPredicate,
        output_column_ids: &[ColumnId],
        order_col_ids: &[ColumnId],
    ) -> Vec<usize> {
        let mut reorder_idx = Vec::with_capacity(order_col_ids.len());
        for order_col_id in order_col_ids {
            let mut found = false;
            for (i, eq_idx) in predicate.right_eq_indexes().into_iter().enumerate() {
                if *order_col_id == output_column_ids[eq_idx] {
                    if !reorder_idx.contains(&i) {
                        reorder_idx.push(i);
                        found = true;
                    }
                    break;
                }
            }
            if !found {
                break;
            }
        }
        reorder_idx
    }

    pub fn gen_batch_lookup_join(
        logical_scan: &LogicalScan,
        predicate: EqJoinPredicate,
        logical_join: generic::Join<BatchPlanRef>,
        is_as_of: bool,
    ) -> Result<Option<BatchLookupJoin>> {
        match logical_join.join_type {
            JoinType::Inner
            | JoinType::LeftOuter
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::AsofInner
            | JoinType::AsofLeftOuter => {}
            _ => return Ok(None),
        };

        let table = logical_scan.table();
        let output_column_ids = logical_scan.output_column_ids();

        // Verify that the right join key columns are the the prefix of the primary key and
        // also contain the distribution key.
        let order_col_ids = table.order_column_ids();
        let dist_key = table.distribution_key.clone();
        // The at least prefix of order key that contains distribution key.
        let mut dist_key_in_order_key_pos = vec![];
        for d in dist_key {
            let pos = table
                .order_column_indices()
                .position(|x| x == d)
                .expect("dist_key must in order_key");
            dist_key_in_order_key_pos.push(pos);
        }
        // The shortest prefix of order key that contains distribution key.
        //
        // If the lookup table has a singleton distribution (i.e. an empty distribution key),
        // any non-empty prefix of the order key can be used for the lookup, so we require a
        // prefix of length at least 1.
        let shortest_prefix_len = dist_key_in_order_key_pos
            .iter()
            .max()
            .map_or(1, |pos| pos + 1);

        // Reorder the join equal predicate to match the order key.
        let reorder_idx =
            Self::lookup_prefix_reorder_idx(&predicate, &output_column_ids, &order_col_ids);
        if reorder_idx.len() < shortest_prefix_len {
            return Ok(None);
        }
        let lookup_prefix_len = reorder_idx.len();
        let predicate = predicate.reorder(&reorder_idx);

        // Extract the predicate from logical scan. Only pure scan is supported.
        let (new_scan, scan_predicate, project_expr) = logical_scan.predicate_pull_up();
        // Construct output column to require column mapping
        let o2r = if let Some(project_expr) = project_expr {
            project_expr
                .into_iter()
                .map(|x| x.as_input_ref().unwrap().index)
                .collect_vec()
        } else {
            (0..logical_scan.output_col_idx().len()).collect_vec()
        };
        let left_schema_len = logical_join.left.schema().len();

        let mut join_predicate_rewriter = LookupJoinPredicateRewriter {
            offset: left_schema_len,
            mapping: o2r.clone(),
        };

        let new_eq_cond = predicate
            .eq_cond()
            .rewrite_expr(&mut join_predicate_rewriter);

        let mut scan_predicate_rewriter = LookupJoinScanPredicateRewriter {
            offset: left_schema_len,
        };

        let new_other_cond = predicate
            .other_cond()
            .clone()
            .rewrite_expr(&mut join_predicate_rewriter)
            .and(scan_predicate.rewrite_expr(&mut scan_predicate_rewriter));

        let new_join_on = new_eq_cond.and(new_other_cond);
        let new_predicate =
            EqJoinPredicate::create(left_schema_len, new_scan.schema().len(), new_join_on);

        // We discovered that we cannot use a lookup join after pulling up the predicate
        // from one side and simplifying the condition. Let's use some other join instead.
        if !new_predicate.has_eq() {
            return Ok(None);
        }

        // Rewrite the join output indices and all output indices referred to the old scan need to
        // rewrite.
        let new_join_output_indices = logical_join
            .output_indices
            .iter()
            .map(|&x| {
                if x < left_schema_len {
                    x
                } else {
                    o2r[x - left_schema_len] + left_schema_len
                }
            })
            .collect_vec();

        let new_scan_output_column_ids = new_scan.output_column_ids();
        let as_of = new_scan.as_of.clone();
        let new_logical_scan: LogicalScan = new_scan.into();

        // Construct a new logical join, because we have change its RHS.
        let new_logical_join = generic::Join::new_with_eq_predicate(
            logical_join.left,
            new_logical_scan.to_batch()?,
            new_predicate,
            logical_join.join_type,
            new_join_output_indices,
        );

        let asof_desc = is_as_of
            .then(|| {
                Self::get_inequality_desc_from_predicate(
                    predicate.other_cond().clone(),
                    left_schema_len,
                )
            })
            .transpose()?;

        Ok(Some(BatchLookupJoin::new(
            new_logical_join,
            table.clone(),
            new_scan_output_column_ids,
            lookup_prefix_len,
            false,
            as_of,
            asof_desc,
        )))
    }

    pub fn decompose(self) -> (PlanRef, PlanRef, Condition, JoinType, Vec<usize>) {
        self.core.decompose()
    }

    fn dynamic_filter_candidate(&self, predicate: &Condition) -> Option<(usize, PbType)> {
        // Dynamic filter only supports `Inner`/`LeftSemi`.
        if !matches!(self.join_type(), JoinType::Inner | JoinType::LeftSemi) {
            return None;
        }

        // Dynamic filter requires right side to be a scalar with one column.
        if !self.right().max_one_row() || self.right().schema().len() != 1 {
            return None;
        }

        // Dynamic filter only supports a single comparison predicate.
        if predicate.conjunctions.len() > 1 {
            return None;
        }
        let expr: ExprImpl = predicate.clone().into();
        let (left_ref, comparator, right_ref) = expr.as_comparison_cond()?;

        // Comparison must cross inputs: left input ref vs right scalar input ref.
        let left_len = self.left().schema().len();
        let condition_cross_inputs = left_ref.index < left_len && right_ref.index == left_len;
        if !condition_cross_inputs {
            return None;
        }

        // Comparison keys must be type aligned.
        if self.left().schema().fields()[left_ref.index].data_type
            != self.right().schema().fields()[0].data_type
        {
            return None;
        }

        // Dynamic filter output can only come from the left side.
        if !self.output_indices().iter().all(|i| *i < left_len) {
            return None;
        }

        Some((left_ref.index, comparator))
    }

    /// Check whether this join can be treated as a temporal filter for locality optimization.
    ///
    /// This is intentionally stricter than dynamic filter:
    /// - right side must be a `LogicalNow`,
    /// - and all dynamic filter preconditions must hold.
    fn temporal_filter_candidate(&self) -> bool {
        self.right().as_logical_now().is_some()
            && self.dynamic_filter_candidate(self.on()).is_some()
    }
}

impl PlanTreeNodeBinary<Logical> for LogicalJoin {
    fn left(&self) -> PlanRef {
        self.core.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.core.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::with_core(generic::Join {
            left,
            right,
            ..self.core.clone()
        })
    }

    fn rewrite_with_left_right(
        &self,
        left: PlanRef,
        left_col_change: ColIndexMapping,
        right: PlanRef,
        right_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let (new_on, new_output_indices) = {
            let (mut map, _) = left_col_change.clone().into_parts();
            let (mut right_map, _) = right_col_change.clone().into_parts();
            for i in right_map.iter_mut().flatten() {
                *i += left.schema().len();
            }
            map.append(&mut right_map);
            let mut mapping = ColIndexMapping::new(map, left.schema().len() + right.schema().len());

            let new_output_indices = self
                .output_indices()
                .iter()
                .map(|&i| mapping.map(i))
                .collect::<Vec<_>>();
            let new_on = self.on().clone().rewrite_expr(&mut mapping);
            (new_on, new_output_indices)
        };

        let join = Self::with_output_indices(
            left,
            right,
            self.join_type(),
            new_on,
            new_output_indices.clone(),
        );

        let new_i2o = ColIndexMapping::with_remaining_columns(
            &new_output_indices,
            join.internal_column_num(),
        );

        let old_o2i = self.core.o2i_col_mapping();

        let old_o2l = old_o2i
            .composite(&self.core.i2l_col_mapping())
            .composite(&left_col_change);
        let old_o2r = old_o2i
            .composite(&self.core.i2r_col_mapping())
            .composite(&right_col_change);
        let new_l2o = join.core.l2i_col_mapping().composite(&new_i2o);
        let new_r2o = join.core.r2i_col_mapping().composite(&new_i2o);

        let out_col_change = old_o2l
            .composite(&new_l2o)
            .union(&old_o2r.composite(&new_r2o));
        (join, out_col_change)
    }
}

impl_plan_tree_node_for_binary! { Logical, LogicalJoin }

impl ColPrunable for LogicalJoin {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        // make `required_cols` point to internal table instead of output schema.
        let required_cols = required_cols
            .iter()
            .map(|i| self.output_indices()[*i])
            .collect_vec();
        let left_len = self.left().schema().fields.len();

        let total_len = self.left().schema().len() + self.right().schema().len();
        let mut resized_required_cols = FixedBitSet::with_capacity(total_len);

        required_cols.iter().for_each(|&i| {
            if self.is_right_join() {
                resized_required_cols.insert(left_len + i);
            } else {
                resized_required_cols.insert(i);
            }
        });

        // add those columns which are required in the join condition to
        // to those that are required in the output
        let mut visitor = CollectInputRef::new(resized_required_cols);
        self.on().visit_expr(&mut visitor);
        let left_right_required_cols = FixedBitSet::from(visitor).ones().collect_vec();

        let mut left_required_cols = Vec::new();
        let mut right_required_cols = Vec::new();
        left_right_required_cols.iter().for_each(|&i| {
            if i < left_len {
                left_required_cols.push(i);
            } else {
                right_required_cols.push(i - left_len);
            }
        });

        let mut on = self.on().clone();
        let mut mapping =
            ColIndexMapping::with_remaining_columns(&left_right_required_cols, total_len);
        on = on.rewrite_expr(&mut mapping);

        let new_output_indices = {
            let required_inputs_in_output = if self.is_left_join() {
                &left_required_cols
            } else if self.is_right_join() {
                &right_required_cols
            } else {
                &left_right_required_cols
            };

            let mapping =
                ColIndexMapping::with_remaining_columns(required_inputs_in_output, total_len);
            required_cols.iter().map(|&i| mapping.map(i)).collect_vec()
        };

        LogicalJoin::with_output_indices(
            self.left().prune_col(&left_required_cols, ctx),
            self.right().prune_col(&right_required_cols, ctx),
            self.join_type(),
            on,
            new_output_indices,
        )
        .into()
    }
}

impl ExprRewritable<Logical> for LogicalJoin {
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

impl ExprVisitable for LogicalJoin {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

/// We are trying to derive a predicate to apply to the other side of a join if all
/// the `InputRef`s in the predicate are eq condition columns, and can hence be substituted
/// with the corresponding eq condition columns of the other side.
///
/// Strategy:
/// 1. If the function is pure except for any `InputRef` (which may refer to impure computation),
///    then we proceed. Else abort.
/// 2. Then, we collect `InputRef`s in the conjunction.
/// 3. If they are all columns in the given side of join eq condition, then we proceed. Else abort.
/// 4. We then rewrite the `ExprImpl`, by replacing `InputRef` column indices with the equivalent in
///    the other side.
///
/// # Arguments
///
/// Suppose we derive a predicate from the left side to be pushed to the right side.
/// * `expr`: An expr from the left side.
/// * `col_num`: The number of columns in the left side.
fn derive_predicate_from_eq_condition(
    expr: &ExprImpl,
    eq_condition: &EqJoinPredicate,
    col_num: usize,
    expr_is_left: bool,
) -> Option<ExprImpl> {
    if expr.is_impure() {
        return None;
    }
    let eq_indices = eq_condition
        .eq_indexes_typed()
        .iter()
        .filter_map(|(l, r)| {
            if l.return_type() != r.return_type() {
                None
            } else if expr_is_left {
                Some(l.index())
            } else {
                Some(r.index())
            }
        })
        .collect_vec();
    if expr
        .collect_input_refs(col_num)
        .ones()
        .any(|index| !eq_indices.contains(&index))
    {
        // expr contains an InputRef not in eq_condition
        return None;
    }
    // The function is pure except for `InputRef` and all `InputRef`s are `eq_condition` indices.
    // Hence, we can substitute those `InputRef`s with indices from the other side.
    let other_side_mapping = if expr_is_left {
        eq_condition.eq_indexes_typed().into_iter().collect()
    } else {
        eq_condition
            .eq_indexes_typed()
            .into_iter()
            .map(|(x, y)| (y, x))
            .collect()
    };
    struct InputRefsRewriter {
        mapping: HashMap<InputRef, InputRef>,
    }
    impl ExprRewriter for InputRefsRewriter {
        fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
            self.mapping[&input_ref].clone().into()
        }
    }
    Some(
        InputRefsRewriter {
            mapping: other_side_mapping,
        }
        .rewrite_expr(expr.clone()),
    )
}

/// Rewrite the join predicate and all columns referred to the scan side need to rewrite.
struct LookupJoinPredicateRewriter {
    offset: usize,
    mapping: Vec<usize>,
}
impl ExprRewriter for LookupJoinPredicateRewriter {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        if input_ref.index() < self.offset {
            input_ref.into()
        } else {
            InputRef::new(
                self.mapping[input_ref.index() - self.offset] + self.offset,
                input_ref.return_type(),
            )
            .into()
        }
    }
}

/// Rewrite the scan predicate so we can add it to the join predicate.
struct LookupJoinScanPredicateRewriter {
    offset: usize,
}
impl ExprRewriter for LookupJoinScanPredicateRewriter {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        InputRef::new(input_ref.index() + self.offset, input_ref.return_type()).into()
    }
}

impl PredicatePushdown for LogicalJoin {
    /// Pushes predicates above and within a join node into the join node and/or its children nodes.
    ///
    /// # Which predicates can be pushed
    ///
    /// For inner join, we can do all kinds of pushdown.
    ///
    /// For left/right semi join, we can push filter to left/right and on-clause,
    /// and push on-clause to left/right.
    ///
    /// For left/right anti join, we can push filter to left/right, but on-clause can not be pushed
    ///
    /// ## Outer Join
    ///
    /// Preserved Row table
    /// : The table in an Outer Join that must return all rows.
    ///
    /// Null Supplying table
    /// : This is the table that has nulls filled in for its columns in unmatched rows.
    ///
    /// |                          | Preserved Row table | Null Supplying table |
    /// |--------------------------|---------------------|----------------------|
    /// | Join predicate (on)      | Not Pushed          | Pushed               |
    /// | Where predicate (filter) | Pushed              | Not Pushed           |
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // rewrite output col referencing indices as internal cols
        let mut predicate = {
            let mut mapping = self.core.o2i_col_mapping();
            predicate.rewrite_expr(&mut mapping)
        };

        let left_col_num = self.left().schema().len();
        let right_col_num = self.right().schema().len();
        let join_type = LogicalJoin::simplify_outer(&predicate, left_col_num, self.join_type());

        let push_down_temporal_predicate = self.temporal_join_on().is_none();

        let (left_from_filter, right_from_filter, on) = push_down_into_join(
            &mut predicate,
            left_col_num,
            right_col_num,
            join_type,
            push_down_temporal_predicate,
        );

        let mut new_on = self.on().clone().and(on);
        let (left_from_on, right_from_on) = push_down_join_condition(
            &mut new_on,
            left_col_num,
            right_col_num,
            join_type,
            push_down_temporal_predicate,
        );

        let left_predicate = left_from_filter.and(left_from_on);
        let right_predicate = right_from_filter.and(right_from_on);

        // Derive conditions to push to the other side based on eq condition columns
        let eq_condition = EqJoinPredicate::create(left_col_num, right_col_num, new_on.clone());

        // Only push to RHS if RHS is inner side of a join (RHS requires match on LHS)
        let right_from_left = if matches!(
            join_type,
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightSemi | JoinType::LeftSemi
        ) {
            Condition {
                conjunctions: left_predicate
                    .conjunctions
                    .iter()
                    .filter_map(|expr| {
                        derive_predicate_from_eq_condition(expr, &eq_condition, left_col_num, true)
                    })
                    .collect(),
            }
        } else {
            Condition::true_cond()
        };

        // Only push to LHS if LHS is inner side of a join (LHS requires match on RHS)
        let left_from_right = if matches!(
            join_type,
            JoinType::Inner | JoinType::RightOuter | JoinType::LeftSemi | JoinType::RightSemi
        ) {
            Condition {
                conjunctions: right_predicate
                    .conjunctions
                    .iter()
                    .filter_map(|expr| {
                        derive_predicate_from_eq_condition(
                            expr,
                            &eq_condition,
                            right_col_num,
                            false,
                        )
                    })
                    .collect(),
            }
        } else {
            Condition::true_cond()
        };

        let left_predicate = left_predicate.and(left_from_right);
        let right_predicate = right_predicate.and(right_from_left);

        let new_left = self.left().predicate_pushdown(left_predicate, ctx);
        let new_right = self.right().predicate_pushdown(right_predicate, ctx);
        let new_join = LogicalJoin::with_output_indices(
            new_left,
            new_right,
            join_type,
            new_on,
            self.output_indices().clone(),
        );

        let mut mapping = self.core.i2o_col_mapping();
        predicate = predicate.rewrite_expr(&mut mapping);
        LogicalFilter::create(new_join.into(), predicate)
    }
}

#[derive(Clone, Copy)]
struct TemporalJoinScan<'a>(&'a LogicalScan);

impl<'a> Deref for TemporalJoinScan<'a> {
    type Target = LogicalScan;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl LogicalJoin {
    fn get_stream_input_for_hash_join(
        &self,
        predicate: &EqJoinPredicate,
        ctx: &mut ToStreamContext,
    ) -> Result<(StreamPlanRef, StreamPlanRef)> {
        use super::stream::prelude::*;

        let mut right = self.right().to_stream_with_dist_required(
            &RequiredDist::shard_by_key(self.right().schema().len(), &predicate.right_eq_indexes()),
            ctx,
        )?;
        let r2l =
            predicate.r2l_eq_columns_mapping(self.left().schema().len(), right.schema().len());
        let l2r =
            predicate.l2r_eq_columns_mapping(self.left().schema().len(), right.schema().len());
        let mut left;
        let right_dist = right.distribution();
        match right_dist {
            Distribution::HashShard(_) => {
                let left_dist = r2l
                    .rewrite_required_distribution(&RequiredDist::PhysicalDist(right_dist.clone()));
                left = self.left().to_stream_with_dist_required(&left_dist, ctx)?;
            }
            Distribution::UpstreamHashShard(_, _) => {
                left = self.left().to_stream_with_dist_required(
                    &RequiredDist::shard_by_key(
                        self.left().schema().len(),
                        &predicate.left_eq_indexes(),
                    ),
                    ctx,
                )?;
                let left_dist = left.distribution();
                match left_dist {
                    Distribution::HashShard(_) => {
                        let right_dist = l2r.rewrite_required_distribution(
                            &RequiredDist::PhysicalDist(left_dist.clone()),
                        );
                        right = right_dist.streaming_enforce_if_not_satisfies(right)?
                    }
                    Distribution::UpstreamHashShard(_, _) => {
                        left = RequiredDist::hash_shard(&predicate.left_eq_indexes())
                            .streaming_enforce_if_not_satisfies(left)?;
                        right = RequiredDist::hash_shard(&predicate.right_eq_indexes())
                            .streaming_enforce_if_not_satisfies(right)?;
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
        Ok((left, right))
    }

    fn to_stream_hash_join(
        &self,
        predicate: EqJoinPredicate,
        ctx: &mut ToStreamContext,
    ) -> Result<StreamPlanRef> {
        use super::stream::prelude::*;

        assert!(predicate.has_eq());
        let (left, right) = self.get_stream_input_for_hash_join(&predicate, ctx)?;

        let mut core = self.core.clone_with_inputs(left, right);
        core.on = generic::JoinOn::EqPredicate(predicate);

        // Convert to Hash Join for equal joins
        // For inner joins, pull non-equal conditions to a filter operator on top of it by default.
        // We do so as the filter operator can apply the non-equal condition batch-wise (vectorized)
        // as opposed to the HashJoin, which applies the condition row-wise.
        // However, the default behavior of pulling up non-equal conditions can be overridden by the
        // session variable `streaming_force_filter_inside_join` as it can save unnecessary
        // materialization of rows only to be filtered later.

        let stream_hash_join = StreamHashJoin::new(core.clone())?;
        let predicate = stream_hash_join.eq_join_predicate().clone();

        let force_filter_inside_join = self
            .base
            .ctx()
            .session_ctx()
            .config()
            .streaming_force_filter_inside_join();

        let pull_filter = self.join_type() == JoinType::Inner
            && stream_hash_join.eq_join_predicate().has_non_eq()
            && stream_hash_join.inequality_pairs().is_empty()
            && (!force_filter_inside_join);
        if pull_filter {
            let default_indices = (0..self.internal_column_num()).collect::<Vec<_>>();

            let mut core = core;
            core.output_indices = default_indices.clone();
            // Temporarily remove output indices.
            let eq_cond = EqJoinPredicate::new(
                Condition::true_cond(),
                predicate.eq_keys().to_vec(),
                self.left().schema().len(),
                self.right().schema().len(),
            );
            core.on = generic::JoinOn::EqPredicate(eq_cond);
            let hash_join = StreamHashJoin::new(core)?.into();
            let logical_filter = generic::Filter::new(predicate.non_eq_cond(), hash_join);
            let plan = StreamFilter::new(logical_filter).into();
            if self.output_indices() != &default_indices {
                let logical_project = generic::Project::with_mapping(
                    plan,
                    ColIndexMapping::with_remaining_columns(
                        self.output_indices(),
                        self.internal_column_num(),
                    ),
                );
                Ok(StreamProject::new(logical_project).into())
            } else {
                Ok(plan)
            }
        } else {
            Ok(stream_hash_join.into())
        }
    }

    pub fn should_be_temporal_join(&self) -> bool {
        self.temporal_join_on().is_some()
    }

    fn temporal_join_on(&self) -> Option<TemporalJoinScan<'_>> {
        if let Some(logical_scan) = self.core.right.as_logical_scan() {
            matches!(
                logical_scan.as_of(),
                Some(AsOf::ProcessTime | AsOf::ProcessTimeBroadcast)
            )
            .then_some(TemporalJoinScan(logical_scan))
        } else {
            None
        }
    }

    fn should_be_stream_temporal_join<'a>(
        &'a self,
        ctx: &ToStreamContext,
    ) -> Result<Option<TemporalJoinScan<'a>>> {
        Ok(if let Some(scan) = self.temporal_join_on() {
            if ctx.backfill_type().is_snapshot_backfill() {
                return Err(RwError::from(ErrorCode::NotSupported(
                    "Temporal join with snapshot backfill not supported".into(),
                    "Please use arrangement backfill".into(),
                )));
            }
            if scan.cross_database() {
                return Err(RwError::from(ErrorCode::NotSupported(
                        "Temporal join requires the lookup table to be in the same database as the stream source table".into(),
                        "Please ensure both tables are in the same database".into(),
                    )));
            }
            Some(scan)
        } else {
            None
        })
    }

    fn to_stream_temporal_join_with_index_selection(
        &self,
        logical_scan: TemporalJoinScan<'_>,
        predicate: EqJoinPredicate,
        ctx: &mut ToStreamContext,
    ) -> Result<StreamPlanRef> {
        // Use primary table.
        let mut result_plan: Result<StreamTemporalJoin> =
            self.to_stream_temporal_join(logical_scan, predicate.clone(), ctx);
        // Return directly if this temporal join can match the pk of its right table.
        if let Ok(temporal_join) = &result_plan
            && temporal_join.eq_join_predicate().eq_indexes().len()
                == logical_scan.primary_key().len()
        {
            return result_plan.map(|x| x.into());
        }
        if self
            .core
            .ctx()
            .session_ctx()
            .config()
            .enable_index_selection()
        {
            let indexes = logical_scan.table_indexes();
            for index in indexes {
                // Use index table
                if let Some(index_scan) = logical_scan.to_index_scan_if_index_covered(index) {
                    let index_scan: PlanRef = index_scan.into();
                    let that = self.clone_with_left_right(self.left(), index_scan.clone());
                    if let Ok(temporal_join) = that.to_stream_temporal_join(
                        that.temporal_join_on().expect(
                            "index scan created from temporal join scan must also be temporal join",
                        ),
                        predicate.clone(),
                        ctx,
                    ) {
                        match &result_plan {
                            Err(_) => result_plan = Ok(temporal_join),
                            Ok(prev_temporal_join) => {
                                // Prefer to the temporal join with a longer lookup prefix len.
                                if prev_temporal_join.eq_join_predicate().eq_indexes().len()
                                    < temporal_join.eq_join_predicate().eq_indexes().len()
                                {
                                    result_plan = Ok(temporal_join)
                                }
                            }
                        }
                    }
                }
            }
        }

        result_plan.map(|x| x.into())
    }

    fn temporal_join_scan_predicate_pull_up(
        logical_scan: TemporalJoinScan<'_>,
        predicate: EqJoinPredicate,
        output_indices: &[usize],
        left_schema_len: usize,
    ) -> Result<(StreamTableScan, EqJoinPredicate, Condition, Vec<usize>)> {
        // Extract the predicate from logical scan. Only pure scan is supported.
        let (new_scan, scan_predicate, project_expr) = logical_scan.predicate_pull_up();
        // Construct output column to require column mapping
        let o2r = if let Some(project_expr) = project_expr {
            project_expr
                .into_iter()
                .map(|x| x.as_input_ref().unwrap().index)
                .collect_vec()
        } else {
            (0..logical_scan.output_col_idx().len()).collect_vec()
        };
        let mut join_predicate_rewriter = LookupJoinPredicateRewriter {
            offset: left_schema_len,
            mapping: o2r.clone(),
        };

        let new_eq_cond = predicate
            .eq_cond()
            .rewrite_expr(&mut join_predicate_rewriter);

        let mut scan_predicate_rewriter = LookupJoinScanPredicateRewriter {
            offset: left_schema_len,
        };

        let new_other_cond = predicate
            .other_cond()
            .clone()
            .rewrite_expr(&mut join_predicate_rewriter)
            .and(scan_predicate.rewrite_expr(&mut scan_predicate_rewriter));

        let new_join_on = new_eq_cond.and(new_other_cond);

        let new_predicate = EqJoinPredicate::create(
            left_schema_len,
            new_scan.schema().len(),
            new_join_on.clone(),
        );

        // Rewrite the join output indices and all output indices referred to the old scan need to
        // rewrite.
        let new_join_output_indices = output_indices
            .iter()
            .map(|&x| {
                if x < left_schema_len {
                    x
                } else {
                    o2r[x - left_schema_len] + left_schema_len
                }
            })
            .collect_vec();

        let new_stream_table_scan =
            StreamTableScan::new_with_backfill_type(new_scan, BackfillType::Replicated);
        Ok((
            new_stream_table_scan,
            new_predicate,
            new_join_on,
            new_join_output_indices,
        ))
    }

    fn to_stream_temporal_join(
        &self,
        logical_scan: TemporalJoinScan<'_>,
        predicate: EqJoinPredicate,
        ctx: &mut ToStreamContext,
    ) -> Result<StreamTemporalJoin> {
        use super::stream::prelude::*;

        assert!(predicate.has_eq());

        let is_broadcast = matches!(logical_scan.as_of(), Some(AsOf::ProcessTimeBroadcast));

        let table = logical_scan.table();
        let output_column_ids = logical_scan.output_column_ids();

        // Verify that the right join key columns are the the prefix of the primary key and
        // also contain the distribution key.
        let order_col_ids = table.order_column_ids();
        let dist_key = table.distribution_key.clone();

        let mut dist_key_in_order_key_pos = vec![];
        for d in dist_key {
            let pos = table
                .order_column_indices()
                .position(|x| x == d)
                .expect("dist_key must in order_key");
            dist_key_in_order_key_pos.push(pos);
        }
        // The shortest prefix of order key that contains distribution key.
        let shortest_prefix_len = dist_key_in_order_key_pos
            .iter()
            .max()
            .map_or(0, |pos| pos + 1);

        // Reorder the join equal predicate to match the order key.
        let reorder_idx =
            Self::lookup_prefix_reorder_idx(&predicate, &output_column_ids, &order_col_ids);
        if reorder_idx.len() < shortest_prefix_len {
            return Err(RwError::from(ErrorCode::NotSupported(
                "Temporal join requires the equivalence join condition includes the key columns that form the distribution key of the lookup table".into(),
                concat!(
                    "Use DESCRIBE <table_name> to view the table's key information.\n",
                    "You can create an index on the lookup table to facilitate the temporal join if necessary."
                ).into(),
            )));
        }
        let lookup_prefix_len = reorder_idx.len();
        let predicate = predicate.reorder(&reorder_idx);

        let required_dist = if dist_key_in_order_key_pos.is_empty() {
            RequiredDist::single()
        } else {
            let left_eq_indexes = predicate.left_eq_indexes();
            let left_dist_key = dist_key_in_order_key_pos
                .iter()
                .map(|pos| left_eq_indexes[*pos])
                .collect_vec();

            RequiredDist::hash_shard(&left_dist_key)
        };

        let left = self.left().to_stream(ctx)?;
        let left = if is_broadcast {
            left.enforce_concrete_distribution()
        } else {
            // Enforce a shuffle for the temporal join LHS to let the scheduler be able to schedule
            // the join fragment together with the RHS with a `no_shuffle` exchange.
            required_dist.stream_enforce(left)
        };

        let (new_stream_table_scan, new_predicate, new_join_on, new_join_output_indices) =
            Self::temporal_join_scan_predicate_pull_up(
                logical_scan,
                predicate,
                self.output_indices(),
                self.left().schema().len(),
            )?;

        let right = if is_broadcast {
            RequiredDist::PhysicalDist(Distribution::Broadcast)
                .streaming_enforce_if_not_satisfies(new_stream_table_scan.into())?
        } else {
            RequiredDist::no_shuffle(new_stream_table_scan.into())
        };
        if !new_predicate.has_eq() {
            return Err(RwError::from(ErrorCode::NotSupported(
                "Temporal join requires a non trivial join condition".into(),
                "Please remove the false condition of the join".into(),
            )));
        }

        // Construct a new logical join, because we have change its RHS.
        let new_logical_join = generic::Join::new(
            left,
            right,
            new_join_on,
            self.join_type(),
            new_join_output_indices,
        );

        let new_predicate = new_predicate.retain_prefix_eq_key(lookup_prefix_len);

        let mut new_logical_join = new_logical_join;
        new_logical_join.on = generic::JoinOn::EqPredicate(new_predicate);
        StreamTemporalJoin::new(new_logical_join, false)
    }

    fn to_stream_nested_loop_temporal_join(
        &self,
        logical_scan: TemporalJoinScan<'_>,
        predicate: EqJoinPredicate,
        ctx: &mut ToStreamContext,
    ) -> Result<StreamPlanRef> {
        use super::stream::prelude::*;
        assert!(!predicate.has_eq());

        if matches!(logical_scan.as_of(), Some(AsOf::ProcessTimeBroadcast)) {
            return Err(RwError::from(ErrorCode::NotSupported(
                "Broadcast temporal join requires an equality condition".into(),
                "Please add an equality condition over a lookup-table key".into(),
            )));
        }

        let left = self.left().to_stream_with_dist_required(
            &RequiredDist::PhysicalDist(Distribution::Broadcast),
            ctx,
        )?;
        assert!(left.as_stream_exchange().is_some());

        if self.join_type() != JoinType::Inner {
            return Err(RwError::from(ErrorCode::NotSupported(
                "Temporal join requires an inner join".into(),
                "Please use an inner join".into(),
            )));
        }

        if !left.append_only() {
            return Err(RwError::from(ErrorCode::NotSupported(
                "Nested-loop Temporal join requires the left hash side to be append only".into(),
                "Please ensure the left hash side is append only".into(),
            )));
        }

        let (new_stream_table_scan, new_predicate, new_join_on, new_join_output_indices) =
            Self::temporal_join_scan_predicate_pull_up(
                logical_scan,
                predicate,
                self.output_indices(),
                self.left().schema().len(),
            )?;

        let right = RequiredDist::no_shuffle(new_stream_table_scan.into());

        // Construct a new logical join, because we have change its RHS.
        let new_logical_join = generic::Join::new(
            left,
            right,
            new_join_on,
            self.join_type(),
            new_join_output_indices,
        );

        let mut new_logical_join = new_logical_join;
        new_logical_join.on = generic::JoinOn::EqPredicate(new_predicate);
        Ok(StreamTemporalJoin::new(new_logical_join, true)?.into())
    }

    fn to_stream_dynamic_filter(
        &self,
        predicate: Condition,
        ctx: &mut ToStreamContext,
    ) -> Result<Option<StreamPlanRef>> {
        use super::stream::prelude::*;

        // If there is exactly one predicate, it is a comparison (<, <=, >, >=), and the
        // join is a `Inner` or `LeftSemi` join, we can convert the scalar subquery into a
        // `StreamDynamicFilter`
        let Some((left_key_idx, comparator)) = self.dynamic_filter_candidate(&predicate) else {
            return Ok(None);
        };

        let left = self.left().to_stream(ctx)?.enforce_concrete_distribution();
        let right = self.right().to_stream_with_dist_required(
            &RequiredDist::PhysicalDist(Distribution::Broadcast),
            ctx,
        )?;

        assert!(right.as_stream_exchange().is_some());
        assert_eq!(
            *Itertools::exactly_one(right.inputs().iter())
                .unwrap()
                .distribution(),
            Distribution::Single
        );

        let core = DynamicFilter::new(comparator, left_key_idx, left, right);
        let plan = StreamDynamicFilter::new(core)?.into();
        // TODO: `DynamicFilterExecutor` should support `output_indices` in `ChunkBuilder`
        if self
            .output_indices()
            .iter()
            .copied()
            .ne(0..self.left().schema().len())
        {
            // The schema of dynamic filter is always the same as the left side now, and we have
            // checked that all output columns are from the left side before.
            let logical_project = generic::Project::with_mapping(
                plan,
                ColIndexMapping::with_remaining_columns(
                    self.output_indices(),
                    self.left().schema().len(),
                ),
            );
            Ok(Some(StreamProject::new(logical_project).into()))
        } else {
            Ok(Some(plan))
        }
    }

    pub fn index_lookup_join_to_batch_lookup_join(&self) -> Result<Option<BatchPlanRef>> {
        let predicate = EqJoinPredicate::create(
            self.left().schema().len(),
            self.right().schema().len(),
            self.on().clone(),
        );
        assert!(predicate.has_eq());

        let join = self
            .core
            .clone_with_inputs(self.core.left.to_batch()?, self.core.right.to_batch()?);

        Ok(self.to_batch_lookup_join(predicate, join)?.map(Into::into))
    }

    fn to_stream_asof_join(
        &self,
        predicate: EqJoinPredicate,
        ctx: &mut ToStreamContext,
    ) -> Result<StreamPlanRef> {
        use super::stream::prelude::*;

        if predicate.eq_keys().is_empty() {
            return Err(ErrorCode::InvalidInputSyntax(
                "AsOf join requires at least 1 equal condition".to_owned(),
            )
            .into());
        }

        let (left, right) = self.get_stream_input_for_hash_join(&predicate, ctx)?;
        let left_len = left.schema().len();
        let mut core = self.core.clone_with_inputs(left, right);
        core.on = generic::JoinOn::EqPredicate(predicate);

        let inequality_desc = Self::get_inequality_desc_from_predicate(
            core.on
                .as_eq_predicate_ref()
                .expect("core predicate must exist")
                .other_cond()
                .clone(),
            left_len,
        )?;

        Ok(StreamAsOfJoin::new(core, inequality_desc)?.into())
    }

    /// Convert the logical join to a Hash join.
    fn to_batch_hash_join(
        &self,
        logical_join: generic::Join<BatchPlanRef>,
        predicate: EqJoinPredicate,
    ) -> Result<BatchPlanRef> {
        use super::batch::prelude::*;

        let left_schema_len = logical_join.left.schema().len();
        let asof_desc = self
            .is_asof_join()
            .then(|| {
                Self::get_inequality_desc_from_predicate(
                    predicate.other_cond().clone(),
                    left_schema_len,
                )
            })
            .transpose()?;

        let logical_join = generic::Join {
            on: generic::JoinOn::EqPredicate(predicate),
            ..logical_join
        };
        let batch_join = BatchHashJoin::new(logical_join, asof_desc);
        Ok(batch_join.into())
    }

    pub fn get_inequality_desc_from_predicate(
        predicate: Condition,
        left_input_len: usize,
    ) -> Result<AsOfJoinDesc> {
        let expr: ExprImpl = predicate.into();
        if let Some((left_input_ref, expr_type, right_input_ref)) = expr.as_comparison_cond() {
            if left_input_ref.index() < left_input_len && right_input_ref.index() >= left_input_len
            {
                Ok(AsOfJoinDesc {
                    left_idx: left_input_ref.index() as u32,
                    right_idx: (right_input_ref.index() - left_input_len) as u32,
                    inequality_type: Self::expr_type_to_comparison_type(expr_type)?.into(),
                })
            } else {
                bail!("inequal condition from the same side should be push down in optimizer");
            }
        } else {
            Err(ErrorCode::InvalidInputSyntax(
                "AsOf join requires exactly 1 ineuquality condition".to_owned(),
            )
            .into())
        }
    }

    fn expr_type_to_comparison_type(expr_type: PbType) -> Result<PbAsOfJoinInequalityType> {
        match expr_type {
            PbType::LessThan => Ok(PbAsOfJoinInequalityType::AsOfInequalityTypeLt),
            PbType::LessThanOrEqual => Ok(PbAsOfJoinInequalityType::AsOfInequalityTypeLe),
            PbType::GreaterThan => Ok(PbAsOfJoinInequalityType::AsOfInequalityTypeGt),
            PbType::GreaterThanOrEqual => Ok(PbAsOfJoinInequalityType::AsOfInequalityTypeGe),
            _ => Err(ErrorCode::InvalidInputSyntax(format!(
                "Invalid comparison type: {}",
                expr_type.as_str_name()
            ))
            .into()),
        }
    }
}

impl ToBatch for LogicalJoin {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        let predicate = EqJoinPredicate::create(
            self.left().schema().len(),
            self.right().schema().len(),
            self.on().clone(),
        );

        let batch_join = self
            .core
            .clone_with_inputs(self.core.left.to_batch()?, self.core.right.to_batch()?);

        let ctx = self.base.ctx();
        let config = ctx.session_ctx().config();

        if predicate.has_eq() {
            if !predicate.eq_keys_are_type_aligned() {
                return Err(ErrorCode::InternalError(format!(
                    "Join eq keys are not aligned for predicate: {predicate:?}"
                ))
                .into());
            }
            if config.batch_enable_lookup_join()
                && let Some(lookup_join) = self.to_batch_lookup_join_with_index_selection(
                    predicate.clone(),
                    batch_join.clone(),
                )?
            {
                return Ok(lookup_join.into());
            }
            self.to_batch_hash_join(batch_join, predicate)
        } else if self.is_asof_join() {
            Err(ErrorCode::InvalidInputSyntax(
                "AsOf join requires at least 1 equal condition".to_owned(),
            )
            .into())
        } else {
            // Convert to Nested-loop Join for non-equal joins
            Ok(BatchNestedLoopJoin::new(batch_join).into())
        }
    }
}

impl ToStream for LogicalJoin {
    fn to_stream(
        &self,
        ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        if self
            .on()
            .conjunctions
            .iter()
            .any(|cond| cond.count_nows() > 0)
        {
            return Err(ErrorCode::NotSupported(
                "optimizer has tried to separate the temporal predicate(with now() expression) from the on condition, but it still reminded in on join's condition. Considering move it into WHERE clause?".to_owned(),
                 "please refer to https://docs.risingwave.com/processing/sql/temporal-filters for more information".to_owned()).into());
        }

        let predicate = EqJoinPredicate::create(
            self.left().schema().len(),
            self.right().schema().len(),
            self.on().clone(),
        );

        if self.join_type() == JoinType::AsofInner || self.join_type() == JoinType::AsofLeftOuter {
            self.to_stream_asof_join(predicate, ctx)
        } else if predicate.has_eq() {
            if !predicate.eq_keys_are_type_aligned() {
                return Err(ErrorCode::InternalError(format!(
                    "Join eq keys are not aligned for predicate: {predicate:?}"
                ))
                .into());
            }

            if let Some(scan) = self.should_be_stream_temporal_join(ctx)? {
                self.to_stream_temporal_join_with_index_selection(scan, predicate, ctx)
            } else {
                self.to_stream_hash_join(predicate, ctx)
            }
        } else if let Some(scan) = self.should_be_stream_temporal_join(ctx)? {
            self.to_stream_nested_loop_temporal_join(scan, predicate, ctx)
        } else if let Some(dynamic_filter) =
            self.to_stream_dynamic_filter(self.on().clone(), ctx)?
        {
            Ok(dynamic_filter)
        } else {
            Err(RwError::from(ErrorCode::NotSupported(
                "streaming nested-loop join".to_owned(),
                "The non-equal join in the query requires a nested-loop join executor, which could be very expensive to run. \
                 Consider rewriting the query to use dynamic filter as a substitute if possible.\n\
                 See also: https://docs.risingwave.com/processing/sql/dynamic-filters".to_owned(),
            )))
        }
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let eq_indexes = self.eq_indexes();
        let (logical_left, logical_right) = if eq_indexes.is_empty() {
            (self.left(), self.right())
        } else {
            let lhs_join_key_idx = eq_indexes.iter().map(|(l, _)| *l).collect_vec();
            if self.should_be_temporal_join() {
                (
                    try_enforce_locality_requirement(
                        self.left(),
                        &lhs_join_key_idx,
                        ctx.locality_backfill_enabled(),
                    ),
                    self.right(),
                )
            } else {
                let rhs_join_key_idx = eq_indexes.iter().map(|(_, r)| *r).collect_vec();
                (
                    try_enforce_locality_requirement(
                        self.left(),
                        &lhs_join_key_idx,
                        ctx.locality_backfill_enabled(),
                    ),
                    try_enforce_locality_requirement(
                        self.right(),
                        &rhs_join_key_idx,
                        ctx.locality_backfill_enabled(),
                    ),
                )
            }
        };

        let (left, left_col_change) = logical_left.logical_rewrite_for_stream(ctx)?;
        let left_len = left.schema().len();
        let (right, right_col_change) = logical_right.logical_rewrite_for_stream(ctx)?;
        let (join, out_col_change) = self.rewrite_with_left_right(
            left.clone(),
            left_col_change,
            right.clone(),
            right_col_change,
        );

        let mapping = ColIndexMapping::with_remaining_columns(
            join.output_indices(),
            join.internal_column_num(),
        );

        let l2o = join.core.l2i_col_mapping().composite(&mapping);
        let r2o = join.core.r2i_col_mapping().composite(&mapping);

        // Add missing pk indices to the logical join
        let mut left_to_add = left
            .expect_stream_key()
            .iter()
            .cloned()
            .filter(|i| l2o.try_map(*i).is_none())
            .collect_vec();

        let mut right_to_add = right
            .expect_stream_key()
            .iter()
            .filter(|&&i| r2o.try_map(i).is_none())
            .map(|&i| i + left_len)
            .collect_vec();

        // NOTE(st1page): add join keys in the pk_indices a work around before we really have stream
        // key.
        let right_len = right.schema().len();
        let eq_predicate = EqJoinPredicate::create(left_len, right_len, join.on().clone());

        let either_or_both = self.core.add_which_join_key_to_pk();

        for (lk, rk) in eq_predicate.eq_indexes() {
            match either_or_both {
                EitherOrBoth::Left(_) => {
                    if l2o.try_map(lk).is_none() {
                        left_to_add.push(lk);
                    }
                }
                EitherOrBoth::Right(_) => {
                    if r2o.try_map(rk).is_none() {
                        right_to_add.push(rk + left_len)
                    }
                }
                EitherOrBoth::Both(_, _) => {
                    if l2o.try_map(lk).is_none() {
                        left_to_add.push(lk);
                    }
                    if r2o.try_map(rk).is_none() {
                        right_to_add.push(rk + left_len)
                    }
                }
            };
        }
        let left_to_add = left_to_add.into_iter().unique();
        let right_to_add = right_to_add.into_iter().unique();
        // NOTE(st1page) over

        let mut new_output_indices = join.output_indices().clone();
        if !join.is_right_join() {
            new_output_indices.extend(left_to_add);
        }
        if !join.is_left_join() {
            new_output_indices.extend(right_to_add);
        }

        let join_with_pk = join.clone_with_output_indices(new_output_indices);

        let plan = if join_with_pk.join_type() == JoinType::FullOuter {
            // ignore the all NULL to maintain the stream key's uniqueness, see https://github.com/risingwavelabs/risingwave/issues/8084 for more information

            let l2o = join_with_pk
                .core
                .l2i_col_mapping()
                .composite(&join_with_pk.core.i2o_col_mapping());
            let r2o = join_with_pk
                .core
                .r2i_col_mapping()
                .composite(&join_with_pk.core.i2o_col_mapping());
            let mut left_right_keys = join_with_pk
                .left()
                .expect_stream_key()
                .iter()
                .map(|i| l2o.map(*i))
                .collect_vec();
            left_right_keys.extend(
                join_with_pk
                    .right()
                    .expect_stream_key()
                    .iter()
                    .map(|i| r2o.map(*i)),
            );
            left_right_keys.extend(
                eq_predicate
                    .eq_indexes()
                    .iter()
                    .flat_map(|(lk, rk)| [l2o.map(*lk), r2o.map(*rk)]),
            );
            let left_right_keys = left_right_keys.into_iter().unique().collect_vec();
            let plan: PlanRef = join_with_pk.into();
            LogicalFilter::filter_out_all_null_keys(plan, &left_right_keys)
        } else {
            join_with_pk.into()
        };

        // the added columns is at the end, so it will not change the exists column index
        Ok((plan, out_col_change))
    }

    fn try_better_locality(&self, columns: &[usize]) -> Option<PlanRef> {
        // Only propagate locality for temporal-filter.
        if !self.temporal_filter_candidate() {
            return None;
        }

        // Temporal filter only outputs columns from left input, so mapping is safe.
        let o2i_mapping = self.core.o2i_col_mapping();
        let left_input_columns = columns
            .iter()
            .map(|&col| o2i_mapping.try_map(col))
            .collect::<Option<Vec<usize>>>()?;
        if let Some(better_left_plan) = self.left().try_better_locality(&left_input_columns) {
            return Some(
                self.clone_with_left_right(better_left_plan, self.right())
                    .into(),
            );
        }
        None
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashSet;

    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, Datum};
    use risingwave_pb::expr::expr_node::Type;

    use super::*;
    use crate::expr::{FunctionCall, Literal, assert_eq_input_ref};
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::LogicalValues;
    use crate::optimizer::property::FunctionalDependency;

    /// Pruning
    /// ```text
    /// Join(on: input_ref(1)=input_ref(3))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// with required columns [2,3] will result in
    /// ```text
    /// Project(input_ref(1), input_ref(2))
    ///   Join(on: input_ref(0)=input_ref(2))
    ///     TableScan(v2, v3)
    ///     TableScan(v4)
    /// ```
    #[tokio::test]
    async fn test_prune_join() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock();
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
        );
        let on: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(3, ty))),
                ],
            )
            .unwrap(),
        ));
        let join_type = JoinType::Inner;
        let join: PlanRef = LogicalJoin::new(
            left.into(),
            right.into(),
            join_type,
            Condition::with_expr(on),
        )
        .into();

        // Perform the prune
        let required_cols = vec![2, 3];
        let plan = join.prune_col(&required_cols, &mut ColumnPruningContext::new(join.clone()));

        // Check the result
        let join = plan.as_logical_join().unwrap();
        assert_eq!(join.schema().fields().len(), 2);
        assert_eq!(join.schema().fields()[0], fields[2]);
        assert_eq!(join.schema().fields()[1], fields[3]);

        let expr: ExprImpl = join.on().clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);
        assert_eq_input_ref!(&call.inputs()[1], 2);

        let left = join.left();
        let left = left.as_logical_values().unwrap();
        assert_eq!(left.schema().fields(), &fields[1..3]);
        let right = join.right();
        let right = right.as_logical_values().unwrap();
        assert_eq!(right.schema().fields(), &fields[3..4]);
    }

    /// Semi join panicked previously at `prune_col`. Add test to prevent regression.
    #[tokio::test]
    async fn test_prune_semi_join() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock();
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
        );
        let on: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(4, ty))),
                ],
            )
            .unwrap(),
        ));
        for join_type in [
            JoinType::LeftSemi,
            JoinType::RightSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
        ] {
            let join = LogicalJoin::new(
                left.clone().into(),
                right.clone().into(),
                join_type,
                Condition::with_expr(on.clone()),
            );

            let offset = if join.is_right_join() { 3 } else { 0 };
            let join: PlanRef = join.into();
            // Perform the prune
            let required_cols = vec![0];
            // key 0 is never used in the join (always key 1)
            let plan = join.prune_col(&required_cols, &mut ColumnPruningContext::new(join.clone()));
            let as_plan = plan.as_logical_join().unwrap();
            // Check the result
            assert_eq!(as_plan.schema().fields().len(), 1);
            assert_eq!(as_plan.schema().fields()[0], fields[offset]);

            // Perform the prune
            let required_cols = vec![0, 1, 2];
            // should not panic here
            let plan = join.prune_col(&required_cols, &mut ColumnPruningContext::new(join.clone()));
            let as_plan = plan.as_logical_join().unwrap();
            // Check the result
            assert_eq!(as_plan.schema().fields().len(), 3);
            assert_eq!(as_plan.schema().fields()[0], fields[offset]);
            assert_eq!(as_plan.schema().fields()[1], fields[offset + 1]);
            assert_eq!(as_plan.schema().fields()[2], fields[offset + 2]);
        }
    }

    /// Pruning
    /// ```text
    /// Join(on: input_ref(1)=input_ref(3))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// with required columns [1, 3] will result in
    /// ```text
    /// Join(on: input_ref(0)=input_ref(1))
    ///   TableScan(v2)
    ///   TableScan(v4)
    /// ```
    #[tokio::test]
    async fn test_prune_join_no_project() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock();
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
        );
        let on: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(3, ty))),
                ],
            )
            .unwrap(),
        ));
        let join_type = JoinType::Inner;
        let join: PlanRef = LogicalJoin::new(
            left.into(),
            right.into(),
            join_type,
            Condition::with_expr(on),
        )
        .into();

        // Perform the prune
        let required_cols = vec![1, 3];
        let plan = join.prune_col(&required_cols, &mut ColumnPruningContext::new(join.clone()));

        // Check the result
        let join = plan.as_logical_join().unwrap();
        assert_eq!(join.schema().fields().len(), 2);
        assert_eq!(join.schema().fields()[0], fields[1]);
        assert_eq!(join.schema().fields()[1], fields[3]);

        let expr: ExprImpl = join.on().clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);
        assert_eq_input_ref!(&call.inputs()[1], 1);

        let left = join.left();
        let left = left.as_logical_values().unwrap();
        assert_eq!(left.schema().fields(), &fields[1..2]);
        let right = join.right();
        let right = right.as_logical_values().unwrap();
        assert_eq!(right.schema().fields(), &fields[3..4]);
    }

    /// Convert
    /// ```text
    /// Join(on: ($1 = $3) AND ($2 == 42))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// to
    /// ```text
    /// Filter($2 == 42)
    ///   HashJoin(on: $1 = $3)
    ///     TableScan(v1, v2, v3)
    ///     TableScan(v4, v5, v6)
    /// ```
    #[tokio::test]
    async fn test_join_to_batch() {
        let ctx = OptimizerContext::mock();
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(DataType::Int32, format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
        );

        fn input_ref(i: usize) -> ExprImpl {
            ExprImpl::InputRef(Box::new(InputRef::new(i, DataType::Int32)))
        }
        let eq_cond = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(Type::Equal, vec![input_ref(1), input_ref(3)]).unwrap(),
        ));
        let non_eq_cond = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    input_ref(2),
                    ExprImpl::Literal(Box::new(Literal::new(
                        Datum::Some(42_i32.into()),
                        DataType::Int32,
                    ))),
                ],
            )
            .unwrap(),
        ));
        // Condition: ($1 = $3) AND ($2 == 42)
        let on_cond = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(Type::And, vec![eq_cond.clone(), non_eq_cond.clone()]).unwrap(),
        ));

        let join_type = JoinType::Inner;
        let logical_join = LogicalJoin::new(
            left.into(),
            right.into(),
            join_type,
            Condition::with_expr(on_cond),
        );

        // Perform `to_batch`
        let result = logical_join.to_batch().unwrap();

        // Expected plan:  HashJoin($1 = $3 AND $2 == 42)
        let hash_join = result.as_batch_hash_join().unwrap();
        assert_eq!(
            ExprImpl::from(hash_join.eq_join_predicate().eq_cond()),
            eq_cond
        );
        assert_eq!(
            *hash_join
                .eq_join_predicate()
                .non_eq_cond()
                .conjunctions
                .first()
                .unwrap(),
            non_eq_cond
        );
    }

    /// Pruning
    /// ```text
    /// Join(on: input_ref(1)=input_ref(3))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// with required columns [3, 2] will result in
    /// ```text
    /// Project(input_ref(2), input_ref(1))
    ///   Join(on: input_ref(0)=input_ref(2))
    ///     TableScan(v2, v3)
    ///     TableScan(v4)
    /// ```
    #[tokio::test]
    async fn test_join_column_prune_with_order_required() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock();
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
        );
        let on: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(3, ty))),
                ],
            )
            .unwrap(),
        ));
        let join_type = JoinType::Inner;
        let join: PlanRef = LogicalJoin::new(
            left.into(),
            right.into(),
            join_type,
            Condition::with_expr(on),
        )
        .into();

        // Perform the prune
        let required_cols = vec![3, 2];
        let plan = join.prune_col(&required_cols, &mut ColumnPruningContext::new(join.clone()));

        // Check the result
        let join = plan.as_logical_join().unwrap();
        assert_eq!(join.schema().fields().len(), 2);
        assert_eq!(join.schema().fields()[0], fields[3]);
        assert_eq!(join.schema().fields()[1], fields[2]);

        let expr: ExprImpl = join.on().clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);
        assert_eq_input_ref!(&call.inputs()[1], 2);

        let left = join.left();
        let left = left.as_logical_values().unwrap();
        assert_eq!(left.schema().fields(), &fields[1..3]);
        let right = join.right();
        let right = right.as_logical_values().unwrap();
        assert_eq!(right.schema().fields(), &fields[3..4]);
    }

    #[tokio::test]
    async fn fd_derivation_inner_outer_join() {
        // left: [l0, l1], right: [r0, r1, r2]
        // FD: l0 --> l1, r0 --> { r1, r2 }
        // On: l0 = 0 AND l1 = r1
        //
        // Inner Join:
        //  Schema: [l0, l1, r0, r1, r2]
        //  FD: l0 --> l1, r0 --> { r1, r2 }, {} --> l0, l1 --> r1, r1 --> l1
        // Left Outer Join:
        //  Schema: [l0, l1, r0, r1, r2]
        //  FD: l0 --> l1
        // Right Outer Join:
        //  Schema: [l0, l1, r0, r1, r2]
        //  FD: r0 --> { r1, r2 }
        // Full Outer Join:
        //  Schema: [l0, l1, r0, r1, r2]
        //  FD: empty
        // Left Semi/Anti Join:
        //  Schema: [l0, l1]
        //  FD: l0 --> l1
        // Right Semi/Anti Join:
        //  Schema: [r0, r1, r2]
        //  FD: r0 --> {r1, r2}
        let ctx = OptimizerContext::mock();
        let left = {
            let fields: Vec<Field> = vec![
                Field::with_name(DataType::Int32, "l0"),
                Field::with_name(DataType::Int32, "l1"),
            ];
            let mut values = LogicalValues::new(vec![], Schema { fields }, ctx.clone());
            // 0 --> 1
            values
                .base
                .functional_dependency_mut()
                .add_functional_dependency_by_column_indices(&[0], &[1]);
            values
        };
        let right = {
            let fields: Vec<Field> = vec![
                Field::with_name(DataType::Int32, "r0"),
                Field::with_name(DataType::Int32, "r1"),
                Field::with_name(DataType::Int32, "r2"),
            ];
            let mut values = LogicalValues::new(vec![], Schema { fields }, ctx);
            // 0 --> 1, 2
            values
                .base
                .functional_dependency_mut()
                .add_functional_dependency_by_column_indices(&[0], &[1, 2]);
            values
        };
        // l0 = 0 AND l1 = r1
        let on: ExprImpl = FunctionCall::new(
            Type::And,
            vec![
                FunctionCall::new(
                    Type::Equal,
                    vec![
                        InputRef::new(0, DataType::Int32).into(),
                        ExprImpl::literal_int(0),
                    ],
                )
                .unwrap()
                .into(),
                FunctionCall::new(
                    Type::Equal,
                    vec![
                        InputRef::new(1, DataType::Int32).into(),
                        InputRef::new(3, DataType::Int32).into(),
                    ],
                )
                .unwrap()
                .into(),
            ],
        )
        .unwrap()
        .into();
        let expected_fd_set = [
            (
                JoinType::Inner,
                [
                    // inherit from left
                    FunctionalDependency::with_indices(5, &[0], &[1]),
                    // inherit from right
                    FunctionalDependency::with_indices(5, &[2], &[3, 4]),
                    // constant column in join condition
                    FunctionalDependency::with_indices(5, &[], &[0]),
                    // eq column in join condition
                    FunctionalDependency::with_indices(5, &[1], &[3]),
                    FunctionalDependency::with_indices(5, &[3], &[1]),
                ]
                .into_iter()
                .collect::<HashSet<_>>(),
            ),
            (JoinType::FullOuter, HashSet::new()),
            (
                JoinType::RightOuter,
                [
                    // inherit from right
                    FunctionalDependency::with_indices(5, &[2], &[3, 4]),
                ]
                .into_iter()
                .collect::<HashSet<_>>(),
            ),
            (
                JoinType::LeftOuter,
                [
                    // inherit from left
                    FunctionalDependency::with_indices(5, &[0], &[1]),
                ]
                .into_iter()
                .collect::<HashSet<_>>(),
            ),
            (
                JoinType::LeftSemi,
                [
                    // inherit from left
                    FunctionalDependency::with_indices(2, &[0], &[1]),
                ]
                .into_iter()
                .collect::<HashSet<_>>(),
            ),
            (
                JoinType::LeftAnti,
                [
                    // inherit from left
                    FunctionalDependency::with_indices(2, &[0], &[1]),
                ]
                .into_iter()
                .collect::<HashSet<_>>(),
            ),
            (
                JoinType::RightSemi,
                [
                    // inherit from right
                    FunctionalDependency::with_indices(3, &[0], &[1, 2]),
                ]
                .into_iter()
                .collect::<HashSet<_>>(),
            ),
            (
                JoinType::RightAnti,
                [
                    // inherit from right
                    FunctionalDependency::with_indices(3, &[0], &[1, 2]),
                ]
                .into_iter()
                .collect::<HashSet<_>>(),
            ),
        ];

        for (join_type, expected_res) in expected_fd_set {
            let join = LogicalJoin::new(
                left.clone().into(),
                right.clone().into(),
                join_type,
                Condition::with_expr(on.clone()),
            );
            let fd_set = join
                .functional_dependency()
                .as_dependencies()
                .iter()
                .cloned()
                .collect::<HashSet<_>>();
            assert_eq!(fd_set, expected_res);
        }
    }
}
