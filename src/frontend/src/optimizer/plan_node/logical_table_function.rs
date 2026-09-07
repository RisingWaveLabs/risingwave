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

use super::utils::impl_distill_by_unit;
use super::{
    ColPrunable, ExprRewritable, Logical, LogicalFilter, LogicalPlanRef as PlanRef, LogicalProject,
    PlanBase, PredicatePushdown, ToBatch, ToStream, generic,
};
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor, TableFunction};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalTableFunction` is a scalar/table function used as a relation (in the `FROM` clause).
///
/// If the function returns a struct, it will be flattened into multiple columns.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalTableFunction {
    pub base: PlanBase<Logical>,
    core: generic::TableFunction,
}

impl LogicalTableFunction {
    fn with_core(core: generic::TableFunction) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }

    /// Create a [`LogicalTableFunction`] node. Used internally by optimizer.
    pub fn new(
        table_function: TableFunction,
        with_ordinality: bool,
        ctx: OptimizerContextRef,
    ) -> Self {
        Self::with_core(generic::TableFunction::new(
            table_function,
            with_ordinality,
            ctx,
        ))
    }

    pub fn table_function(&self) -> &TableFunction {
        &self.core.table_function
    }

    pub fn with_ordinality(&self) -> bool {
        self.core.with_ordinality
    }
}

impl_plan_tree_node_for_leaf! { Logical, LogicalTableFunction }
impl_distill_by_unit!(LogicalTableFunction, core, "LogicalTableFunction");

impl ColPrunable for LogicalTableFunction {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        // No pruning.
        LogicalProject::with_out_col_idx(self.clone().into(), required_cols.iter().copied()).into()
    }
}

impl ExprRewritable<Logical> for LogicalTableFunction {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::with_core(core).into()
    }
}

impl ExprVisitable for LogicalTableFunction {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

impl PredicatePushdown for LogicalTableFunction {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalTableFunction {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        unreachable!("TableFunction should be converted to ProjectSet")
    }
}

impl ToStream for LogicalTableFunction {
    fn to_stream(
        &self,
        _ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        unreachable!("TableFunction should be converted to ProjectSet")
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        unreachable!("TableFunction should be converted to ProjectSet")
    }
}
