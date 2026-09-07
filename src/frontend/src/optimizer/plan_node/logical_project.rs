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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pretty_xmlish::XmlNode;

use super::utils::{Distill, childless_record};
use super::{
    BatchPlanRef, BatchProject, ColPrunable, ExprRewritable, Logical, LogicalPlanRef as PlanRef,
    LogicalPlanRef, PlanBase, PlanTreeNodeUnary, PredicatePushdown, StreamMaterializedExprs,
    StreamPlanRef, StreamProject, ToBatch, ToStream, gen_filter_and_pushdown, generic,
};
use crate::error::Result;
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprVisitor, InputRef, collect_input_refs};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::stream::StreamPlanNodeMetadata;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::{Distribution, Order, RequiredDist, StreamKind};
use crate::optimizer::rule::IndexSelectionRule;
use crate::session::current;
use crate::utils::{ColIndexMapping, ColIndexMappingRewriteExt, Condition, Substitute};

/// `LogicalProject` computes a set of expressions from its input relation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalProject {
    pub base: PlanBase<Logical>,
    core: generic::Project<PlanRef>,
}

impl LogicalProject {
    pub fn create(input: PlanRef, exprs: Vec<ExprImpl>) -> PlanRef {
        Self::new(input, exprs).into()
    }

    // TODO(kwannoel): We only need create/new don't keep both.
    pub fn new(input: PlanRef, exprs: Vec<ExprImpl>) -> Self {
        let core = generic::Project::new(exprs, input);
        Self::with_core(core)
    }

    pub fn with_core(core: generic::Project<PlanRef>) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        LogicalProject { base, core }
    }

    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        self.core.o2i_col_mapping()
    }

    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        self.core.i2o_col_mapping()
    }

    /// Creates a `LogicalProject` which select some columns from the input.
    ///
    /// `mapping` should maps from `(0..input_fields.len())` to a consecutive range starting from 0.
    ///
    /// This is useful in column pruning when we want to add a project to ensure the output schema
    /// is correct.
    pub fn with_mapping(input: PlanRef, mapping: ColIndexMapping) -> Self {
        Self::with_core(generic::Project::with_mapping(input, mapping))
    }

    /// Creates a `LogicalProject` which select some columns from the input.
    pub fn with_out_fields(input: PlanRef, out_fields: &FixedBitSet) -> Self {
        Self::with_core(generic::Project::with_out_fields(input, out_fields))
    }

    /// Creates a `LogicalProject` which select some columns from the input.
    pub fn with_out_col_idx(input: PlanRef, out_fields: impl Iterator<Item = usize>) -> Self {
        Self::with_core(generic::Project::with_out_col_idx(input, out_fields))
    }

    pub fn exprs(&self) -> &Vec<ExprImpl> {
        &self.core.exprs
    }

    pub fn is_identity(&self) -> bool {
        self.core.is_identity()
    }

    pub fn try_as_projection(&self) -> Option<Vec<usize>> {
        self.core.try_as_projection()
    }

    pub fn decompose(self) -> (Vec<ExprImpl>, PlanRef) {
        self.core.decompose()
    }

    pub(crate) fn clone_with_input_and_exprs(&self, input: PlanRef, exprs: Vec<ExprImpl>) -> Self {
        let mut core = self.core.clone_with_input(input);
        core.exprs = exprs;
        // Replacing a computed expression with an input ref must not rename the output to the
        // physical index column.
        core.field_names = self
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| (index, field.name.clone()))
            .collect();
        let project = Self::with_core(core);
        debug_assert_eq!(project.schema(), self.schema());
        project
    }

    pub fn is_all_inputref(&self) -> bool {
        self.core.is_all_inputref()
    }
}

impl PlanTreeNodeUnary<Logical> for LogicalProject {
    fn input(&self) -> LogicalPlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: LogicalPlanRef) -> Self {
        Self::new(input, self.exprs().clone())
    }

    fn rewrite_with_input(
        &self,
        input: PlanRef,
        mut input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let exprs = self
            .exprs()
            .clone()
            .into_iter()
            .map(|expr| input_col_change.rewrite_expr(expr))
            .collect();
        let proj = Self::new(input, exprs);
        // change the input columns index will not change the output column index
        let out_col_change = ColIndexMapping::identity(self.schema().len());
        (proj, out_col_change)
    }
}

impl_plan_tree_node_for_unary! { Logical, LogicalProject}

impl Distill for LogicalProject {
    fn distill<'a>(&self) -> XmlNode<'a> {
        childless_record(
            "LogicalProject",
            self.core.fields_pretty(self.base.schema()),
        )
    }
}

impl ColPrunable for LogicalProject {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let input_col_num: usize = self.input().schema().len();
        let input_required_cols = collect_input_refs(
            input_col_num,
            required_cols.iter().map(|i| &self.exprs()[*i]),
        )
        .ones()
        .collect_vec();
        let new_input = self.input().prune_col(&input_required_cols, ctx);
        let mut mapping = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );
        // Rewrite each InputRef with new index.
        let exprs = required_cols
            .iter()
            .map(|&id| mapping.rewrite_expr(self.exprs()[id].clone()))
            .collect();

        // Reconstruct the LogicalProject.
        LogicalProject::new(new_input, exprs).into()
    }
}

impl ExprRewritable<Logical> for LogicalProject {
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

impl ExprVisitable for LogicalProject {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

impl PredicatePushdown for LogicalProject {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // convert the predicate to one that references the child of the project
        let mut subst = Substitute {
            mapping: self.exprs().clone(),
        };

        let impure_mask = {
            let mut impure_mask = FixedBitSet::with_capacity(self.exprs().len());
            for (i, e) in self.exprs().iter().enumerate() {
                impure_mask.set(i, e.is_impure())
            }
            impure_mask
        };
        // (with impure input, with pure input)
        let (remained_cond, pushed_cond) = predicate.split_disjoint(&impure_mask);
        let pushed_cond = pushed_cond.rewrite_expr(&mut subst);

        gen_filter_and_pushdown(self, remained_cond, pushed_cond, ctx)
    }
}

impl ToBatch for LogicalProject {
    fn to_batch(&self) -> Result<BatchPlanRef> {
        self.to_batch_with_order_required(&Order::any())
    }

    fn to_batch_with_order_required(&self, required_order: &Order) -> Result<BatchPlanRef> {
        if self
            .base
            .ctx()
            .session_ctx()
            .config()
            .enable_index_selection()
            && let Some(project) = IndexSelectionRule::select_expression_index_for_project(self)
        {
            // The rewritten input is an index-table scan without its own indexes, so the
            // recursive conversion cannot select another expression index.
            return project.to_batch_with_order_required(required_order);
        }

        let input_order = self
            .o2i_col_mapping()
            .rewrite_provided_order(required_order);
        let new_input = self.input().to_batch_with_order_required(&input_order)?;
        let project = self.core.clone_with_input(new_input);
        let batch_project = BatchProject::new(project);
        required_order.enforce_if_not_satisfies(batch_project.into())
    }
}

impl ToStream for LogicalProject {
    fn to_stream_with_dist_required(
        &self,
        required_dist: &RequiredDist,
        ctx: &mut ToStreamContext,
    ) -> Result<StreamPlanRef> {
        let input_required = if required_dist.satisfies(&RequiredDist::AnyShard) {
            RequiredDist::Any
        } else {
            let input_required = self
                .o2i_col_mapping()
                .rewrite_required_distribution(required_dist);
            match input_required {
                RequiredDist::PhysicalDist(dist) => match dist {
                    Distribution::Single => RequiredDist::Any,
                    _ => RequiredDist::PhysicalDist(dist),
                },
                _ => input_required,
            }
        };
        let new_input = self
            .input()
            .to_stream_with_dist_required(&input_required, ctx)?;

        let unsafe_allow_unmaterialized_impure_expr = current::config()
            .map(|c| c.read().streaming_unsafe_allow_unmaterialized_impure_expr())
            .unwrap_or_else(|| {
                self.base
                    .ctx()
                    .session_ctx()
                    .config()
                    .streaming_unsafe_allow_unmaterialized_impure_expr()
            });
        let should_materialize_expr = match new_input.stream_kind() {
            StreamKind::AppendOnly => None,
            StreamKind::Retract | StreamKind::Upsert if unsafe_allow_unmaterialized_impure_expr => {
                // This deliberately leaves impure expressions in `StreamProject` on retract/upsert
                // streams. The behavior is unsafe and only enabled by the explicit session option.
                None
            }
            kind @ (StreamKind::Retract | StreamKind::Upsert) => {
                // Extract impure functions to `MaterializedExprs` operator
                let mut impure_field_names = BTreeMap::new();
                let mut impure_expr_indices = HashSet::new();
                let impure_exprs: Vec<_> = self
                    .exprs()
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, expr)| {
                        // Extract impure expressions
                        if expr.is_impure() {
                            impure_expr_indices.insert(idx);
                            if let Some(name) = self.core.field_names.get(&idx) {
                                impure_field_names.insert(idx, name.clone());
                            }
                            Some(expr.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                if impure_exprs.is_empty() {
                    None
                } else if kind == StreamKind::Upsert
                    && new_input
                        .stream_key()
                        .into_iter()
                        .flatten()
                        .all(|stream_key_idx| !impure_expr_indices.contains(stream_key_idx))
                {
                    // We're operating on non-stream-key columns of upsert stream, no need to materialize.
                    None
                } else {
                    Some((impure_field_names, impure_expr_indices, impure_exprs))
                }
            }
        };

        let stream_plan = if let Some((impure_field_names, impure_expr_indices, impure_exprs)) =
            should_materialize_expr
        {
            {
                let new_input = new_input.enforce_concrete_distribution();

                // Create `MaterializedExprs` for impure expressions
                let mat_exprs_plan: StreamPlanRef = StreamMaterializedExprs::new(
                    new_input.clone(),
                    impure_exprs,
                    impure_field_names,
                )?
                .into();

                let input_len = new_input.schema().len();
                let mut materialized_pos = 0;

                // Create final expressions list with impure expressions replaced by `InputRef`s
                let final_exprs = self
                    .exprs()
                    .iter()
                    .enumerate()
                    .map(|(idx, expr)| {
                        if impure_expr_indices.contains(&idx) {
                            let output_idx = input_len + materialized_pos;
                            materialized_pos += 1;
                            InputRef::new(output_idx, expr.return_type()).into()
                        } else {
                            expr.clone()
                        }
                    })
                    .collect();

                let core = generic::Project::new(final_exprs, mat_exprs_plan);
                StreamProject::new(core).into()
            }
        } else {
            // No expressions to materialize or the feature is not enabled, create a regular `StreamProject`
            let core = generic::Project::new(self.exprs().clone(), new_input);
            StreamProject::new(core).into()
        };

        required_dist.streaming_enforce_if_not_satisfies(stream_plan)
    }

    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<StreamPlanRef> {
        self.to_stream_with_dist_required(&RequiredDist::Any, ctx)
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input().logical_rewrite_for_stream(ctx)?;
        let (proj, out_col_change) = self.rewrite_with_input(input.clone(), input_col_change);

        // Add missing columns of `input_pk` into the select list.
        let input_pk = input.expect_stream_key();
        let i2o = proj.i2o_col_mapping();
        let col_need_to_add = input_pk
            .iter()
            .cloned()
            .filter(|i| i2o.try_map(*i).is_none());
        let input_schema = input.schema();
        let exprs =
            proj.exprs()
                .iter()
                .cloned()
                .chain(col_need_to_add.map(|idx| {
                    InputRef::new(idx, input_schema.fields[idx].data_type.clone()).into()
                }))
                .collect();
        let proj = Self::new(input, exprs);
        // The added columns is at the end, so it will not change existing column indices.
        // But the target size of `out_col_change` should be the same as the length of the new
        // schema.
        let (map, _) = out_col_change.into_parts();
        let out_col_change = ColIndexMapping::new(map, proj.base.schema().len());
        Ok((proj.into(), out_col_change))
    }

    fn try_better_locality(&self, columns: &[usize]) -> Option<PlanRef> {
        if columns.is_empty() {
            return None;
        }

        let input_columns = columns
            .iter()
            .map(|&col| {
                // First try the original o2i mapping for direct InputRef
                if let Some(input_col) = self.o2i_col_mapping().try_map(col) {
                    return Some(input_col);
                }

                // If not a direct InputRef, check if it's a pure function with single InputRef
                let expr = &self.exprs()[col];
                if expr.is_pure() {
                    let input_refs = expr.collect_input_refs(self.input().schema().len());
                    // Check if expression references exactly one input column
                    if input_refs.count_ones(..) == 1 {
                        return input_refs.ones().next();
                    }
                }

                None
            })
            .collect::<Option<Vec<usize>>>()?;

        let new_input = self.input().try_better_locality(&input_columns)?;
        Some(self.clone_with_input(new_input).into())
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_pb::expr::expr_node::Type;

    use super::*;
    use crate::expr::{FunctionCall, Literal, assert_eq_input_ref};
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::LogicalValues;

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Project(1, input_ref(2), input_ref(0)<5)
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns `[1, 2]` will result in
    /// ```text
    /// Project(input_ref(1), input_ref(0)<5)
    ///   TableScan(v1, v3)
    /// ```
    async fn test_prune_project() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock();
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let values = LogicalValues::new(
            vec![],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );
        let project: PlanRef = LogicalProject::new(
            values.into(),
            vec![
                ExprImpl::Literal(Box::new(Literal::new(None, ty.clone()))),
                InputRef::new(2, ty.clone()).into(),
                ExprImpl::FunctionCall(Box::new(
                    FunctionCall::new(
                        Type::LessThan,
                        vec![
                            ExprImpl::InputRef(Box::new(InputRef::new(0, ty.clone()))),
                            ExprImpl::Literal(Box::new(Literal::new(None, ty))),
                        ],
                    )
                    .unwrap(),
                )),
            ],
        )
        .into();

        // Perform the prune
        let required_cols = vec![1, 2];
        let plan = project.prune_col(
            &required_cols,
            &mut ColumnPruningContext::new(project.clone()),
        );

        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 2);
        assert_eq_input_ref!(&project.exprs()[0], 1);

        let expr = project.exprs()[1].clone();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);

        let values = project.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields().len(), 2);
        assert_eq!(values.schema().fields()[0], fields[0]);
        assert_eq!(values.schema().fields()[1], fields[2]);
    }
}
