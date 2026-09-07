// Copyright 2023 RisingWave Labs
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

use std::collections::HashSet;

use super::{DefaultBehavior, DefaultValue, LogicalPlanVisitor};
use crate::expr::{CorrelatedId, CorrelatedInputRef, ExprVisitor};
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalFilter, LogicalJoin, LogicalPlanRef as PlanRef, LogicalProject,
    LogicalProjectSet, LogicalTableFunction, PlanTreeNode,
};
use crate::optimizer::plan_visitor::PlanVisitor;

#[derive(Default)]
pub struct PlanCorrelatedIdFinder {
    correlated_id_set: HashSet<CorrelatedId>,
}

impl PlanCorrelatedIdFinder {
    pub fn contains(&self, correlated_id: &CorrelatedId) -> bool {
        self.correlated_id_set.contains(correlated_id)
    }

    pub fn find_correlated_id(plan: PlanRef, correlated_id: &CorrelatedId) -> bool {
        let mut plan_correlated_id_finder = Self::default();
        plan_correlated_id_finder.visit(plan);
        plan_correlated_id_finder.contains(correlated_id)
    }
}

impl LogicalPlanVisitor for PlanCorrelatedIdFinder {
    /// `correlated_input_ref` can only appear in `LogicalProject`, `LogicalFilter`,
    /// `LogicalJoin` or the `filter` clause of `PlanAggCall` of `LogicalAgg` now.
    type Result = ();

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        DefaultValue
    }

    fn visit_logical_join(&mut self, plan: &LogicalJoin) {
        let mut finder = ExprCorrelatedIdFinder::default();
        plan.on().visit_expr(&mut finder);
        self.correlated_id_set.extend(finder.correlated_id_set);

        plan.inputs()
            .into_iter()
            .for_each(|input| self.visit(input));
    }

    fn visit_logical_filter(&mut self, plan: &LogicalFilter) {
        let mut finder = ExprCorrelatedIdFinder::default();
        plan.predicate().visit_expr(&mut finder);
        self.correlated_id_set.extend(finder.correlated_id_set);

        plan.inputs()
            .into_iter()
            .for_each(|input| self.visit(input));
    }

    fn visit_logical_project(&mut self, plan: &LogicalProject) {
        let mut finder = ExprCorrelatedIdFinder::default();
        plan.exprs().iter().for_each(|expr| finder.visit_expr(expr));
        self.correlated_id_set.extend(finder.correlated_id_set);

        plan.inputs()
            .into_iter()
            .for_each(|input| self.visit(input));
    }

    fn visit_logical_agg(&mut self, plan: &LogicalAgg) {
        let mut finder = ExprCorrelatedIdFinder::default();
        plan.agg_calls()
            .iter()
            .for_each(|agg_call| agg_call.filter.visit_expr(&mut finder));
        self.correlated_id_set.extend(finder.correlated_id_set);

        plan.inputs()
            .into_iter()
            .for_each(|input| self.visit(input));
    }

    fn visit_logical_project_set(&mut self, plan: &LogicalProjectSet) {
        let mut finder = ExprCorrelatedIdFinder::default();
        plan.select_list()
            .iter()
            .for_each(|expr| finder.visit_expr(expr));
        self.correlated_id_set.extend(finder.correlated_id_set);

        plan.inputs()
            .into_iter()
            .for_each(|input| self.visit(input));
    }

    fn visit_logical_table_function(&mut self, plan: &LogicalTableFunction) {
        let mut finder = ExprCorrelatedIdFinder::default();
        plan.table_function()
            .args
            .iter()
            .for_each(|expr| finder.visit_expr(expr));
        self.correlated_id_set.extend(finder.correlated_id_set);

        plan.inputs()
            .into_iter()
            .for_each(|input| self.visit(input));
    }
}

#[derive(Default)]
pub struct ExprCorrelatedIdFinder {
    correlated_id_set: HashSet<CorrelatedId>,
}

impl ExprCorrelatedIdFinder {
    pub fn contains(&self, correlated_id: &CorrelatedId) -> bool {
        self.correlated_id_set.contains(correlated_id)
    }

    pub fn has_correlated_input_ref(&self) -> bool {
        !self.correlated_id_set.is_empty()
    }
}

impl ExprVisitor for ExprCorrelatedIdFinder {
    fn visit_correlated_input_ref(&mut self, correlated_input_ref: &CorrelatedInputRef) {
        self.correlated_id_set
            .insert(correlated_input_ref.correlated_id());
    }
}
