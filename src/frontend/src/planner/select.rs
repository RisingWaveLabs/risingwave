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

use itertools::Itertools;
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::PROJECTED_ROW_ID_COLUMN_NAME;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_expr::ExprError;
use risingwave_pb::plan_common::JoinType;

use crate::OptimizerContextRef;
use crate::binder::{BoundDistinct, BoundSelect};
use crate::error::{ErrorCode, Result};
use crate::expr::{
    CorrelatedId, Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef, Subquery,
    SubqueryKind,
};
pub use crate::optimizer::plan_node::LogicalFilter;
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef, Project, ProjectBuilder};
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalApply, LogicalDedup, LogicalJoin, LogicalOverWindow,
    LogicalPlanRef as PlanRef, LogicalProject, LogicalProjectSet, LogicalTopN, LogicalValues,
    PlanAggCall,
};
use crate::optimizer::property::Order;
use crate::planner::Planner;
use crate::utils::{Condition, IndexSet};

impl Planner {
    pub(super) fn plan_select(
        &mut self,
        BoundSelect {
            from,
            where_clause,
            mut select_items,
            group_by,
            mut having,
            mut distinct,
            ..
        }: BoundSelect,
        extra_order_exprs: Vec<ExprImpl>,
        order: &[ColumnOrder],
    ) -> Result<PlanRef> {
        // Append expressions in ORDER BY.
        if distinct.is_distinct() && !extra_order_exprs.is_empty() {
            return Err(ErrorCode::InvalidInputSyntax(
                "for SELECT DISTINCT, ORDER BY expressions must appear in select list".into(),
            )
            .into());
        }
        select_items.extend(extra_order_exprs);
        // The DISTINCT ON expression(s) must match the leftmost ORDER BY expression(s).
        if let BoundDistinct::DistinctOn(exprs) = &distinct {
            let mut distinct_on_exprs: HashMap<ExprImpl, bool> =
                exprs.iter().map(|expr| (expr.clone(), false)).collect();
            let mut uncovered_distinct_on_exprs_cnt = distinct_on_exprs.len();
            let mut order_iter = order.iter().map(|o| &select_items[o.column_index]);
            while uncovered_distinct_on_exprs_cnt > 0
                && let Some(order_expr) = order_iter.next()
            {
                match distinct_on_exprs.get_mut(order_expr) {
                    Some(has_been_covered) => {
                        if !*has_been_covered {
                            *has_been_covered = true;
                            uncovered_distinct_on_exprs_cnt -= 1;
                        }
                    }
                    None => {
                        return Err(ErrorCode::InvalidInputSyntax(
                            "the SELECT DISTINCT ON expressions must match the leftmost ORDER BY expressions"
                                .into(),
                        )
                        .into());
                    }
                }
            }
        }

        // Plan the FROM clause.
        let mut root = match from {
            None => self.create_dummy_values(),
            Some(t) => self.plan_relation(t)?,
        };
        // Plan the WHERE clause.
        if let Some(where_clause) = where_clause {
            root = self.plan_where(root, where_clause)?;
        }
        // Plan the SELECT clause.
        // TODO: select-agg, group-by, having can also contain subquery exprs.
        let has_agg_call = select_items.iter().any(|expr| expr.has_agg_call());
        if !group_by.is_empty() || having.is_some() || has_agg_call {
            // The `DISTINCT ON` expressions must also be rewritten to reference the
            // aggregated results, just like the SELECT and ORDER BY expressions. By
            // appending them to `select_items` they go through the same agg rewriting,
            // which maps group-key references and rejects any column that neither
            // appears in GROUP BY nor inside an aggregate (matching PostgreSQL).
            let n_distinct_on = if let BoundDistinct::DistinctOn(exprs) = &distinct {
                select_items.extend(exprs.iter().cloned());
                exprs.len()
            } else {
                0
            };

            (root, select_items, having) =
                LogicalAgg::create(select_items, group_by, having, root)?;

            if n_distinct_on > 0 {
                let rewritten = select_items.split_off(select_items.len() - n_distinct_on);
                distinct = BoundDistinct::DistinctOn(rewritten);
            }
        }

        if let Some(having) = having {
            root = self.plan_where(root, having)?;
        }

        if select_items.iter().any(|e| e.has_subquery()) {
            (root, select_items) =
                self.substitute_subqueries_in_cross_join_way(root, select_items)?;
        }
        if select_items.iter().any(|e| e.has_window_function()) {
            (root, select_items) = LogicalOverWindow::create(root, select_items)?;
        }

        let original_select_items_len = select_items.len();

        // variable `distinct_list_index_to_select_items_index` is meaningful iff
        // `matches!(&distinct, BoundDistinct::DistinctOn(_))`
        let mut distinct_list_index_to_select_items_index = vec![];
        if let BoundDistinct::DistinctOn(distinct_list) = &distinct {
            distinct_list_index_to_select_items_index.reserve(distinct_list.len());
            let mut builder_index_to_select_items_index =
                Vec::with_capacity(original_select_items_len);
            let mut input_proj_builder = ProjectBuilder::default();
            for (select_item_index, select_item) in select_items.iter().enumerate() {
                let builder_index = input_proj_builder
                    .add_expr(select_item)
                    .map_err(|msg| ExprError::UnsupportedFunction(String::from(msg)))?;
                if builder_index >= builder_index_to_select_items_index.len() {
                    debug_assert_eq!(builder_index, builder_index_to_select_items_index.len());
                    builder_index_to_select_items_index.push(select_item_index);
                }
            }
            for distinct_expr in distinct_list {
                let builder_index = input_proj_builder
                    .add_expr(distinct_expr)
                    .map_err(|msg| ExprError::UnsupportedFunction(String::from(msg)))?;
                if builder_index >= builder_index_to_select_items_index.len() {
                    debug_assert_eq!(builder_index, builder_index_to_select_items_index.len());
                    select_items.push(distinct_expr.clone());
                    builder_index_to_select_items_index.push(select_items.len() - 1);
                }
                distinct_list_index_to_select_items_index
                    .push(builder_index_to_select_items_index[builder_index]);
            }
        }

        let need_restore_select_items = select_items.len() > original_select_items_len;

        root = LogicalProjectSet::create(root, select_items);

        if matches!(&distinct, BoundDistinct::DistinctOn(_)) {
            root = if order.is_empty() {
                // We only support deduplicating `DISTINCT ON` columns when there is no `ORDER BY`
                // clause now.
                LogicalDedup::new(root, distinct_list_index_to_select_items_index).into()
            } else {
                LogicalTopN::new(
                    root,
                    1,
                    0,
                    false,
                    Order::new(order.to_vec()),
                    distinct_list_index_to_select_items_index,
                )
                .into()
            };
        }

        if need_restore_select_items {
            root = LogicalProject::with_core(Project::with_out_col_idx(
                root,
                0..original_select_items_len,
            ))
            .into();
        }

        if let BoundDistinct::Distinct = distinct {
            let fields = root.schema().fields();
            let group_key = if let Some(field) = fields.first()
                && field.name == PROJECTED_ROW_ID_COLUMN_NAME
            {
                // Do not group by projected_row_id hidden column.
                (1..fields.len()).collect()
            } else {
                (0..fields.len()).collect()
            };
            root = Agg::new(vec![], group_key, root).into();
        }

        Ok(root)
    }

    /// Helper to create a dummy node as child of [`LogicalProject`].
    /// For example, `select 1+2, 3*4` will be `Project([1+2, 3+4]) - Values([[]])`.
    fn create_dummy_values(&self) -> PlanRef {
        LogicalValues::create_empty_scalar(self.ctx.clone())
    }

    /// Helper to create an `EXISTS` boolean operator with the given `input`.
    /// It is represented by `Project([$0 >= 1]) -> Agg(count(*)) -> input`
    fn create_exists(&self, input: PlanRef) -> Result<PlanRef> {
        let count_star = Agg::new(vec![PlanAggCall::count_star()], IndexSet::empty(), input);
        let ge = FunctionCall::new(
            ExprType::GreaterThanOrEqual,
            vec![
                InputRef::new(0, DataType::Int64).into(),
                ExprImpl::literal_int(1),
            ],
        )
        .unwrap();
        Ok(LogicalProject::create(count_star.into(), vec![ge.into()]))
    }

    /// For `(NOT) EXISTS subquery` or `(NOT) IN subquery`, we can plan it as
    /// `LeftSemi/LeftAnti` [`LogicalApply`]
    /// For other subqueries, we plan it as `LeftOuter` [`LogicalApply`] using
    /// [`Self::substitute_subqueries_in_left_deep_tree_way`].
    pub(super) fn plan_where(
        &mut self,
        mut input: PlanRef,
        where_clause: ExprImpl,
    ) -> Result<PlanRef> {
        if !where_clause.has_subquery() {
            return Ok(LogicalFilter::create_with_expr(input, where_clause));
        }
        let (subquery_conjunctions, not_subquery_conjunctions, others) =
            Condition::with_expr(where_clause)
                .group_by::<_, 3>(|expr| match expr {
                    ExprImpl::Subquery(_) => 0,
                    ExprImpl::FunctionCall(func_call)
                        if func_call.func_type() == ExprType::Not
                            && matches!(func_call.inputs()[0], ExprImpl::Subquery(_)) =>
                    {
                        1
                    }
                    _ => 2,
                })
                .into_iter()
                .next_tuple()
                .unwrap();

        // EXISTS and IN in WHERE.
        for expr in subquery_conjunctions {
            self.handle_exists_and_in(expr, false, &mut input)?;
        }

        // NOT EXISTS and NOT IN in WHERE.
        for expr in not_subquery_conjunctions {
            let not = expr.into_function_call().unwrap();
            let (_, expr) = not.decompose_as_unary();
            self.handle_exists_and_in(expr, true, &mut input)?;
        }

        if others.always_true() {
            Ok(input)
        } else {
            let (input, others) =
                self.substitute_subqueries_in_left_deep_tree_way(input, others.conjunctions)?;
            Ok(LogicalFilter::create(
                input,
                Condition {
                    conjunctions: others,
                },
            ))
        }
    }

    /// Handle (NOT) EXISTS and (NOT) IN in WHERE clause.
    ///
    /// We will use a = b to replace a in (select b from ....) for (NOT) IN thus avoiding adding a
    /// `LogicalFilter` on `LogicalApply`.
    fn handle_exists_and_in(
        &mut self,
        expr: ExprImpl,
        negated: bool,
        input: &mut PlanRef,
    ) -> Result<()> {
        let join_type = if negated {
            JoinType::LeftAnti
        } else {
            JoinType::LeftSemi
        };
        let correlated_id = self.ctx.next_correlated_id();
        let mut subquery = expr.into_subquery().unwrap();
        // we should call `subquery.query.collect_correlated_indices_by_depth_and_assign_id`
        // instead of `subquery.collect_correlated_indices_by_depth_and_assign_id`.
        // because current subquery containing struct `kind` expr which should never be correlated with the current subquery.
        let mut correlated_indices = subquery
            .query
            .collect_correlated_indices_by_depth_and_assign_id(0, correlated_id);
        correlated_indices.sort();
        correlated_indices.dedup();
        let output_column_type = subquery.query.data_types()[0].clone();
        let right_plan = self.plan_query(subquery.query)?.into_unordered_subplan();
        let on = match subquery.kind {
            SubqueryKind::Existential => ExprImpl::literal_bool(true),
            SubqueryKind::In(left_expr) => {
                let right_expr = InputRef::new(input.schema().len(), output_column_type);
                FunctionCall::new(ExprType::Equal, vec![left_expr, right_expr.into()])?.into()
            }
            kind => bail_not_implemented!(issue = 1343, "Not supported subquery kind: {:?}", kind),
        };
        *input = Self::create_apply(
            correlated_id,
            correlated_indices,
            input.clone(),
            right_plan,
            on,
            join_type,
            false,
        );
        Ok(())
    }

    /// Substitutes all [`Subquery`] in `exprs` in a left deep tree way.
    ///
    /// Each time a [`Subquery`] is found, it is replaced by a new [`InputRef`]. And `root` is
    /// replaced by a new `LeftOuter` [`LogicalApply`] whose left side is `root` and right side is
    /// the planned subquery.
    ///
    /// The [`InputRef`]s' indexes start from `root.schema().len()`,
    /// which means they are additional columns beyond the original `root`.
    ///
    /// The left-deep tree way only meaningful when there are multiple subqueries
    ///
    /// ```text
    ///             Apply
    ///            /    \
    ///          Apply  Subquery3
    ///         /    \
    ///     Apply    Subquery2
    ///     /   \
    ///   left  Subquery1
    /// ```
    ///
    /// Typically, this is used for subqueries in `WHERE` clause, because it is unlikely that users would write subqueries in the same where clause multiple times.
    /// But our dynamic filter sometimes generates subqueries in the same where clause multiple times (which couldn't work in the cross join way), so we need to support this case.
    pub(super) fn substitute_subqueries_in_left_deep_tree_way(
        &mut self,
        mut root: PlanRef,
        mut exprs: Vec<ExprImpl>,
    ) -> Result<(PlanRef, Vec<ExprImpl>)> {
        struct SubstituteSubQueries {
            input_col_num: usize,
            subqueries: Vec<Subquery>,
            correlated_indices_collection: Vec<Vec<usize>>,
            correlated_ids: Vec<CorrelatedId>,
            ctx: OptimizerContextRef,
        }

        // TODO: consider the multi-subquery case for normal predicate.
        impl ExprRewriter for SubstituteSubQueries {
            fn rewrite_subquery(&mut self, mut subquery: Subquery) -> ExprImpl {
                let correlated_id = self.ctx.next_correlated_id();
                self.correlated_ids.push(correlated_id);
                let input_ref = InputRef::new(self.input_col_num, subquery.return_type()).into();
                self.input_col_num += 1;
                self.correlated_indices_collection.push(
                    subquery.collect_correlated_indices_by_depth_and_assign_id(0, correlated_id),
                );
                self.subqueries.push(subquery);
                input_ref
            }
        }

        let mut rewriter = SubstituteSubQueries {
            input_col_num: root.schema().len(),
            subqueries: vec![],
            correlated_indices_collection: vec![],
            correlated_ids: vec![],
            ctx: self.ctx.clone(),
        };
        exprs = exprs
            .into_iter()
            .map(|e| rewriter.rewrite_expr(e))
            .collect();

        for ((subquery, correlated_indices), correlated_id) in rewriter
            .subqueries
            .into_iter()
            .zip_eq_fast(rewriter.correlated_indices_collection)
            .zip_eq_fast(rewriter.correlated_ids)
        {
            let return_type = subquery.return_type();
            let subroot = self.plan_query(subquery.query)?;

            let right = match subquery.kind {
                SubqueryKind::Scalar => subroot.into_unordered_subplan(),
                SubqueryKind::UpdateSet => {
                    let plan = subroot.into_unordered_subplan();

                    // Compose all input columns into a struct with `ROW` function.
                    let all_input_refs = plan
                        .schema()
                        .data_types()
                        .into_iter()
                        .enumerate()
                        .map(|(i, data_type)| InputRef::new(i, data_type).into())
                        .collect::<Vec<_>>();
                    let call =
                        FunctionCall::new_unchecked(ExprType::Row, all_input_refs, return_type);

                    LogicalProject::create(plan, vec![call.into()])
                }
                SubqueryKind::Existential => {
                    self.create_exists(subroot.into_unordered_subplan())?
                }
                SubqueryKind::Array => subroot.into_array_agg()?,
                _ => bail_not_implemented!(issue = 1343, "{:?}", subquery.kind),
            };

            // `Array` subqueries decorrelate to a scalar aggregate with
            // `Coalesce(array_agg, ARRAY[])`, guaranteeing one row per outer key, so
            // `Inner` is equivalent to `LeftOuter` in batch. `Inner` also avoids the
            // streaming transient-NULL caused by `StreamHashJoin(LeftOuter)`
            // null-padding the outer row before the right-side aggregate arrives.
            let join_type = if matches!(subquery.kind, SubqueryKind::Array) {
                JoinType::Inner
            } else {
                JoinType::LeftOuter
            };
            root = Self::create_apply(
                correlated_id,
                correlated_indices,
                root,
                right,
                ExprImpl::literal_bool(true),
                join_type,
                true,
            );
        }
        Ok((root, exprs))
    }

    /// Substitutes all [`Subquery`] in `exprs` in a cross join way.
    ///
    /// Each time a [`Subquery`] is found, it is replaced by a new [`InputRef`]. And `root` is
    /// replaced by a new `LeftOuter` [`LogicalApply`] whose left side is `root` and right side is
    /// the planned subquery.
    ///
    /// The [`InputRef`]s' indexes start from `root.schema().len()`,
    /// which means they are additional columns beyond the original `root`.
    ///
    ///
    /// The cross join way only meaningful when there are multiple subqueries
    ///
    /// ```text
    ///            Apply
    ///           /    \
    ///         left   CrossJoin
    ///                   /   \
    ///           Subquery1   CrossJoin
    ///                         /   \
    ///                 Subquery2   Subquery3
    /// ```
    /// Typically, this is used for scalar subqueries in select clauses, because users might write subqueries in exprs multiple times.
    /// If we use the left-deep tree way, it will generate a lot of `Apply` nodes, which is not efficient.
    pub(super) fn substitute_subqueries_in_cross_join_way(
        &mut self,
        mut root: PlanRef,
        mut exprs: Vec<ExprImpl>,
    ) -> Result<(PlanRef, Vec<ExprImpl>)> {
        struct SubstituteSubQueries {
            input_col_num: usize,
            subqueries: Vec<Subquery>,
            correlated_id: Option<CorrelatedId>,
            correlated_indices_collection: Vec<Vec<usize>>,
            ctx: OptimizerContextRef,
        }

        impl ExprRewriter for SubstituteSubQueries {
            fn rewrite_subquery(&mut self, mut subquery: Subquery) -> ExprImpl {
                if self.correlated_id.is_none() {
                    self.correlated_id = Some(self.ctx.next_correlated_id());
                }
                let input_ref = InputRef::new(self.input_col_num, subquery.return_type()).into();
                self.input_col_num += 1;
                self.correlated_indices_collection.push(
                    subquery.collect_correlated_indices_by_depth_and_assign_id(
                        0,
                        self.correlated_id.unwrap(),
                    ),
                );
                self.subqueries.push(subquery);
                input_ref
            }
        }

        let mut rewriter = SubstituteSubQueries {
            input_col_num: root.schema().len(),
            subqueries: vec![],
            correlated_id: None,
            correlated_indices_collection: vec![],
            ctx: self.ctx.clone(),
        };
        exprs = exprs
            .into_iter()
            .map(|e| rewriter.rewrite_expr(e))
            .collect();

        let mut right = None;
        let mut all_array_subqueries = true;

        for subquery in rewriter.subqueries {
            let return_type = subquery.return_type();
            let subroot = self.plan_query(subquery.query)?;

            let subplan = match subquery.kind {
                SubqueryKind::Scalar => subroot.into_unordered_subplan(),
                SubqueryKind::UpdateSet => {
                    let plan = subroot.into_unordered_subplan();

                    // Compose all input columns into a struct with `ROW` function.
                    let all_input_refs = plan
                        .schema()
                        .data_types()
                        .into_iter()
                        .enumerate()
                        .map(|(i, data_type)| InputRef::new(i, data_type).into())
                        .collect::<Vec<_>>();
                    let call =
                        FunctionCall::new_unchecked(ExprType::Row, all_input_refs, return_type);

                    LogicalProject::create(plan, vec![call.into()])
                }
                SubqueryKind::Existential => {
                    self.create_exists(subroot.into_unordered_subplan())?
                }
                SubqueryKind::Array => subroot.into_array_agg()?,
                _ => bail_not_implemented!(issue = 1343, "{:?}", subquery.kind),
            };
            if !matches!(subquery.kind, SubqueryKind::Array) {
                all_array_subqueries = false;
            }
            if right.is_none() {
                right = Some(subplan);
            } else {
                right = Some(LogicalJoin::create(
                    right.clone().unwrap(),
                    subplan,
                    JoinType::FullOuter,
                    ExprImpl::literal_bool(true),
                ));
            }
        }

        root = if let Some(right) = right {
            let mut correlated_indices = rewriter
                .correlated_indices_collection
                .iter()
                .flatten()
                .cloned()
                .collect::<Vec<_>>();
            correlated_indices.sort();
            correlated_indices.dedup();

            // Same reasoning as `substitute_subqueries_in_left_deep_tree_way`:
            // only safe when every combined subquery is `Array` (Scalar/Existential
            // legitimately need `LeftOuter` null-padding).
            let join_type = if all_array_subqueries {
                JoinType::Inner
            } else {
                JoinType::LeftOuter
            };
            Self::create_apply(
                rewriter.correlated_id.expect("must have a correlated id"),
                correlated_indices,
                root,
                right,
                ExprImpl::literal_bool(true),
                join_type,
                true,
            )
        } else {
            root
        };

        Ok((root, exprs))
    }

    fn create_apply(
        correlated_id: CorrelatedId,
        correlated_indices: Vec<usize>,
        left: PlanRef,
        right: PlanRef,
        on: ExprImpl,
        join_type: JoinType,
        max_one_row: bool,
    ) -> PlanRef {
        LogicalApply::create(
            left,
            right,
            join_type,
            Condition::with_expr(on),
            correlated_id,
            correlated_indices,
            max_one_row,
        )
    }
}
