// Copyright 2026 RisingWave Labs
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

use pretty_xmlish::{Pretty, Str, XmlNode};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{AfterMatchSkip, MatchRecognizePattern, RowsPerMatch};

use super::{DistillUnit, GenericPlanNode, GenericPlanRef};
use crate::OptimizerContextRef;
use crate::binder::{BoundMeasure, BoundSymbolDefinition, MeasureSlotKind};
use crate::expr::{Expr, ExprDisplay, ExprImpl, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::ColIndexMapping;
use crate::optimizer::plan_node::utils::childless_record;
use crate::optimizer::property::FunctionalDependencySet;

/// `MatchRecognize` is the generic core of a SQL `MATCH_RECOGNIZE` (row pattern recognition)
/// operation. For `ONE ROW PER MATCH`, its output schema is the `PARTITION BY` columns followed by
/// the `MEASURES` columns.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MatchRecognize<PlanRef> {
    pub input: PlanRef,
    pub partition_by: Vec<ExprImpl>,
    pub order_by: Vec<ExprImpl>,
    pub measures: Vec<BoundMeasure>,
    pub rows_per_match: Option<RowsPerMatch>,
    pub after_match_skip: Option<AfterMatchSkip>,
    pub pattern: MatchRecognizePattern,
    pub defines: Vec<BoundSymbolDefinition>,
    /// `WITHIN` span check over a synthetic `[last_order_key, first_order_key]` row; `None` if absent.
    pub within: Option<ExprImpl>,
    /// `WITHIN` deadline expr `first_order_key + interval` over a synthetic `[first_order_key]` row:
    /// the watermark at which a partial starting at that row can no longer complete within the
    /// bound. Used to wake an otherwise-idle partition so its expired partials are evicted. `None`
    /// if there is no `WITHIN`.
    pub within_deadline: Option<ExprImpl>,
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for MatchRecognize<PlanRef> {
    fn schema(&self) -> Schema {
        let mut fields = Vec::with_capacity(self.partition_by.len() + self.measures.len() + 1);
        for (i, e) in self.partition_by.iter().enumerate() {
            fields.push(Field::with_name(e.return_type(), format!("partition_{i}")));
        }
        for m in &self.measures {
            fields.push(Field::with_name(m.expr.return_type(), m.name.clone()));
        }
        // Hidden per-match id; the last column. See `stream_key`.
        fields.push(Field::with_name(DataType::Int64, "_match_id"));
        Schema::new(fields)
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        FunctionalDependencySet::new(self.partition_by.len() + self.measures.len() + 1)
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        // ONE ROW PER MATCH emits one append-only row per match; a partition can contain many
        // matches whose (partition + measures) output may be byte-identical, so those columns are
        // not a key. The executor appends a hidden per-match id (the trailing column); the partition
        // columns plus that id uniquely identify every emitted match. Keeping the partition columns
        // in the key preserves partition-based sharding (no reshuffle before the downstream MV).
        let match_id = self.partition_by.len() + self.measures.len();
        let mut key: Vec<usize> = (0..self.partition_by.len()).collect();
        key.push(match_id);
        Some(key)
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

impl<PlanRef: GenericPlanRef> crate::optimizer::plan_node::expr_visitable::ExprVisitable
    for MatchRecognize<PlanRef>
{
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.partition_by.iter().for_each(|e| v.visit_expr(e));
        self.order_by.iter().for_each(|e| v.visit_expr(e));
        self.measures.iter().for_each(|m| v.visit_expr(&m.expr));
        self.defines
            .iter()
            .for_each(|d| v.visit_expr(&d.definition));
        if let Some(within) = &self.within {
            v.visit_expr(within);
        }
        if let Some(within_deadline) = &self.within_deadline {
            v.visit_expr(within_deadline);
        }
    }
}

impl<PlanRef> MatchRecognize<PlanRef> {
    /// Input column indices of `PARTITION BY`, or `None` if any key is not a plain column.
    pub fn partition_key_indices(&self) -> Option<Vec<usize>> {
        self.partition_by
            .iter()
            .map(|e| e.as_input_ref().map(|r| r.index()))
            .collect()
    }

    /// Input column indices of `ORDER BY`, or `None` if any key is not a plain column.
    pub fn order_key_indices(&self) -> Option<Vec<usize>> {
        self.order_by
            .iter()
            .map(|e| e.as_input_ref().map(|r| r.index()))
            .collect()
    }

    pub fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.partition_by
            .iter_mut()
            .for_each(|e| *e = r.rewrite_expr(e.clone()));
        self.order_by
            .iter_mut()
            .for_each(|e| *e = r.rewrite_expr(e.clone()));
        self.measures
            .iter_mut()
            .for_each(|m| m.expr = r.rewrite_expr(m.expr.clone()));
        self.defines
            .iter_mut()
            .for_each(|d| d.definition = r.rewrite_expr(d.definition.clone()));
        if let Some(within) = &mut self.within {
            *within = r.rewrite_expr(within.clone());
        }
        if let Some(within_deadline) = &mut self.within_deadline {
            *within_deadline = r.rewrite_expr(within_deadline.clone());
        }
    }

    pub fn rewrite_with_col_index_mapping(&mut self, mapping: &mut ColIndexMapping) {
        self.partition_by
            .iter_mut()
            .for_each(|e| *e = mapping.rewrite_expr(e.clone()));
        self.order_by
            .iter_mut()
            .for_each(|e| *e = mapping.rewrite_expr(e.clone()));
        // Measure and DEFINE expressions are over synthetic per-match / per-candidate rows, not the
        // plan input, so the input-column remapping applies to the slots' input column indices.
        for m in &mut self.measures {
            for slot in &mut m.slots {
                if !matches!(slot.kind, MeasureSlotKind::Classifier) {
                    slot.col_idx = mapping.map(slot.col_idx);
                }
            }
        }
        for d in &mut self.defines {
            for slot in &mut d.slots {
                slot.col_idx = mapping.map(slot.col_idx);
            }
        }
    }
}

impl<PlanRef: GenericPlanRef> DistillUnit for MatchRecognize<PlanRef> {
    fn distill_with_name<'a>(&self, name: impl Into<Str<'a>>) -> XmlNode<'a> {
        // Schema-aware display (`t.ts`, not `$1`), like the sibling plan nodes.
        let input_schema = self.input.schema();
        let exprs = |es: &[ExprImpl]| {
            Pretty::Array(
                es.iter()
                    .map(|e| {
                        Pretty::display(&ExprDisplay {
                            expr: e,
                            input_schema,
                        })
                    })
                    .collect(),
            )
        };
        let measure_names: Vec<_> = self.measures.iter().map(|m| m.name.as_str()).collect();
        let fields = vec![
            ("partition_by", exprs(&self.partition_by)),
            ("order_by", exprs(&self.order_by)),
            ("measures", Pretty::debug(&measure_names)),
            ("pattern", Pretty::display(&self.pattern)),
        ];
        childless_record(name, fields)
    }
}
