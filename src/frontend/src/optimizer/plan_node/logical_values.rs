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

use std::vec;

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, ScalarImpl};

use super::generic::GenericPlanRef;
use super::utils::impl_distill_by_unit;
use super::{
    BatchValues, ColPrunable, ExprRewritable, Logical, LogicalFilter, LogicalPlanRef as PlanRef,
    PlanBase, PredicatePushdown, StreamValues, ToBatch, ToStream, generic,
};
use crate::error::Result;
use crate::expr::{ExprImpl, ExprRewriter, ExprVisitor, Literal};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalValues` builds rows according to a list of expressions
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalValues {
    pub base: PlanBase<Logical>,
    pub(super) core: generic::Values,
}

impl LogicalValues {
    fn with_core(core: generic::Values) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }

    /// Create a [`LogicalValues`] node. Used internally by optimizer.
    pub fn new(rows: Vec<Vec<ExprImpl>>, schema: Schema, ctx: OptimizerContextRef) -> Self {
        Self::with_core(generic::Values::new(rows, schema, ctx))
    }

    /// Used only by `LogicalValues.rewrite_logical_for_stream`, set the `_row_id` column as pk
    fn new_with_pk(
        rows: Vec<Vec<ExprImpl>>,
        schema: Schema,
        ctx: OptimizerContextRef,
        pk_index: usize,
    ) -> Self {
        Self::with_core(generic::Values::new_with_stream_key(
            rows,
            schema,
            ctx,
            vec![pk_index],
        ))
    }

    /// Create a [`LogicalValues`] node. Used by planner.
    pub fn create(rows: Vec<Vec<ExprImpl>>, schema: Schema, ctx: OptimizerContextRef) -> PlanRef {
        // No additional checks after binder.
        Self::new(rows, schema, ctx).into()
    }

    /// Create a [`LogicalValues`] node with a single empty row, as a dummy input for `Project` or `ProjectSet`.
    pub fn create_empty_scalar(ctx: OptimizerContextRef) -> PlanRef {
        Self::new(vec![vec![]], Schema::new(vec![]), ctx).into()
    }

    /// Check whether this is an empty scalar, typically created by [`LogicalValues::create_empty_scalar`].
    pub fn is_empty_scalar(&self) -> bool {
        self.schema().is_empty() && self.rows().len() == 1 && self.rows()[0].is_empty()
    }

    /// Get a reference to the logical values' rows.
    pub fn rows(&self) -> &[Vec<ExprImpl>] {
        self.core.rows()
    }
}

impl_plan_tree_node_for_leaf! { Logical, LogicalValues }
impl_distill_by_unit!(LogicalValues, core, "LogicalValues");

impl ExprRewritable<Logical> for LogicalValues {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::with_core(core).into()
    }
}

impl ExprVisitable for LogicalValues {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

impl ColPrunable for LogicalValues {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        let rows = self
            .rows()
            .iter()
            .map(|row| required_cols.iter().map(|i| row[*i].clone()).collect())
            .collect();
        let fields = required_cols
            .iter()
            .map(|i| self.schema().fields[*i].clone())
            .collect();
        Self::new(rows, Schema { fields }, self.base.ctx()).into()
    }
}

impl PredicatePushdown for LogicalValues {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalValues {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        Ok(BatchValues::new(self.core.clone()).into())
    }
}

impl ToStream for LogicalValues {
    fn to_stream(
        &self,
        _ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        Ok(StreamValues::new(self.core.clone()).into())
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let row_id_index = self.schema().len();
        let col_index_mapping = ColIndexMapping::identity_or_none(row_id_index, row_id_index + 1);
        let ctx = self.ctx();
        let mut schema = self.schema().clone();
        schema
            .fields
            .push(Field::with_name(DataType::Int64, "_row_id"));
        let rows = self.rows().to_owned();
        let row_with_id = rows
            .into_iter()
            .enumerate()
            .map(|(i, mut r)| {
                r.push(Literal::new(Some(ScalarImpl::Int64(i as i64)), DataType::Int64).into());
                r
            })
            .collect_vec();
        let logical_values = Self::new_with_pk(row_with_id, schema, ctx, row_id_index);
        Ok((logical_values.into(), col_index_mapping))
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::types::Datum;

    use super::*;
    use crate::optimizer::optimizer_context::OptimizerContext;

    fn literal(val: i32) -> ExprImpl {
        Literal::new(Datum::Some(val.into()), DataType::Int32).into()
    }

    /// Pruning
    /// ```text
    /// Values([[0, 1, 2], [3, 4, 5])
    /// ```
    /// with required columns [0, 2] will result in
    /// ```text
    /// Values([[0, 2], [3, 5])
    /// ```
    #[tokio::test]
    async fn test_prune_filter() {
        let ctx = OptimizerContext::mock();
        let schema = Schema::new(vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
            Field::with_name(DataType::Int32, "v3"),
        ]);
        // Values([[0, 1, 2], [3, 4, 5])
        let values: PlanRef = LogicalValues::new(
            vec![
                vec![literal(0), literal(1), literal(2)],
                vec![literal(3), literal(4), literal(5)],
            ],
            schema,
            ctx,
        )
        .into();

        let required_cols = vec![0, 2];
        let pruned = values.prune_col(
            &required_cols,
            &mut ColumnPruningContext::new(values.clone()),
        );

        let values = pruned.as_logical_values().unwrap();
        let rows: &[Vec<ExprImpl>] = values.rows();

        // expected output: Values([[0, 2], [3, 5])
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].len(), 2);
        assert_eq!(rows[0][0], literal(0));
        assert_eq!(rows[0][1], literal(2));
        assert_eq!(rows[1].len(), 2);
        assert_eq!(rows[1][0], literal(3));
        assert_eq!(rows[1][1], literal(5));
    }
}
