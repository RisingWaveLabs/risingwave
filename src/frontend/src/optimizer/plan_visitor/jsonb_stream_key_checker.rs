// Copyright 2024 RisingWave Labs
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

use risingwave_common::catalog::{Field, FieldDisplay};
use risingwave_common::types::DataType;

use super::{DefaultBehavior, LogicalPlanVisitor, Merge};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::*;
use crate::optimizer::plan_visitor::PlanVisitor;

/// Finds the first plan position where the checked type would become part of a key, and names the
/// SQL clause responsible so the error points at what the user wrote.
///
/// Positions checked only for `Variant` are deliberately not checked for `Jsonb`: widening the
/// pre-existing JSONB gate would be a breaking change.
#[derive(Debug, Clone, Copy)]
pub enum StreamKeyChecker {
    Jsonb,
    Variant,
}

impl StreamKeyChecker {
    fn visit_inputs(&mut self, plan: &impl LogicalPlanNode) -> Option<String> {
        let results = plan.inputs().into_iter().map(|input| self.visit(input));
        Self::default_behavior().apply(results)
    }

    fn err_msg(&self, target: &str, field: &Field) -> String {
        format!(
            "{} column \"{}\" should not be in the {}.",
            self.type_name(),
            FieldDisplay(field),
            target
        )
    }

    fn is_restricted_key_type(&self, data_type: &DataType) -> bool {
        match self {
            Self::Jsonb => matches!(data_type, DataType::Jsonb),
            // Unlike the pre-existing JSONB behavior, VARIANT is rejected even when nested.
            Self::Variant => data_type.contains_variant(),
        }
    }

    fn type_name(&self) -> &'static str {
        match self {
            Self::Jsonb => "JSONB",
            Self::Variant => "VARIANT",
        }
    }
}

impl LogicalPlanVisitor for StreamKeyChecker {
    type Result = Option<String>;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a: Option<String>, b| a.or(b))
    }

    fn visit_logical_dedup(&mut self, plan: &LogicalDedup) -> Self::Result {
        let input = plan.input();
        let schema = input.schema();
        let data_types = schema.data_types();
        for idx in plan.dedup_cols() {
            if self.is_restricted_key_type(&data_types[*idx]) {
                return Some(self.err_msg("distinct key", &schema[*idx]));
            }
        }
        self.visit_inputs(plan)
    }

    fn visit_logical_top_n(&mut self, plan: &LogicalTopN) -> Self::Result {
        let input = plan.input();
        let schema = input.schema();
        let data_types = schema.data_types();
        for idx in plan.group_key() {
            if self.is_restricted_key_type(&data_types[*idx]) {
                return Some(self.err_msg("TopN group key", &schema[*idx]));
            }
        }
        for idx in plan
            .topn_order()
            .column_orders
            .iter()
            .map(|c| c.column_index)
        {
            if self.is_restricted_key_type(&data_types[idx]) {
                return Some(self.err_msg("TopN order key", &schema[idx]));
            }
        }
        self.visit_inputs(plan)
    }

    fn visit_logical_union(&mut self, plan: &LogicalUnion) -> Self::Result {
        if !plan.all() {
            for field in &plan.inputs()[0].schema().fields {
                if self.is_restricted_key_type(&field.data_type()) {
                    return Some(self.err_msg("field", field));
                }
            }
        }
        self.visit_inputs(plan)
    }

    fn visit_logical_agg(&mut self, plan: &LogicalAgg) -> Self::Result {
        let input = plan.input();
        let schema = input.schema();
        let data_types = schema.data_types();
        for idx in plan.group_key().indices() {
            if self.is_restricted_key_type(&data_types[idx]) {
                return Some(self.err_msg("aggregation group key", &schema[idx]));
            }
        }
        // An aggregate call's own ORDER BY / DISTINCT lands in the state table key too.
        if matches!(self, Self::Variant) {
            for call in plan.agg_calls() {
                for idx in call.order_by.iter().map(|c| c.column_index) {
                    if self.is_restricted_key_type(&data_types[idx]) {
                        return Some(self.err_msg("aggregation order key", &schema[idx]));
                    }
                }
                for input in call.distinct.then_some(&call.inputs).into_iter().flatten() {
                    let idx = input.index();
                    if self.is_restricted_key_type(&data_types[idx]) {
                        return Some(self.err_msg("distinct aggregation argument", &schema[idx]));
                    }
                }
            }
        }
        self.visit_inputs(plan)
    }

    fn visit_logical_join(&mut self, plan: &LogicalJoin) -> Self::Result {
        // Only the equi keys become a hash key; the rest of `on` stays a per-row predicate.
        if matches!(self, Self::Variant) {
            let left = plan.left();
            let right = plan.right();
            let predicate = EqJoinPredicate::create(
                left.schema().len(),
                right.schema().len(),
                plan.on().clone(),
            );
            for (left_idx, right_idx) in predicate.eq_indexes() {
                for (schema, idx) in [(left.schema(), left_idx), (right.schema(), right_idx)] {
                    if self.is_restricted_key_type(&schema[idx].data_type()) {
                        return Some(self.err_msg("join key", &schema[idx]));
                    }
                }
            }
        }
        self.visit_inputs(plan)
    }

    fn visit_logical_over_window(&mut self, plan: &LogicalOverWindow) -> Self::Result {
        let input = plan.input();
        let schema = input.schema();
        let data_types = schema.data_types();

        for func in plan.window_functions() {
            for idx in func.partition_by.iter().map(|e| e.index()) {
                if self.is_restricted_key_type(&data_types[idx]) {
                    return Some(self.err_msg("over window partition key", &schema[idx]));
                }
            }

            for idx in func.order_by.iter().map(|c| c.column_index) {
                if self.is_restricted_key_type(&data_types[idx]) {
                    return Some(self.err_msg("over window order by key", &schema[idx]));
                }
            }
        }
        self.visit_inputs(plan)
    }
}
