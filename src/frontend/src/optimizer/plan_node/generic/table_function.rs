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

use educe::Educe;
use pretty_xmlish::{Pretty, Str, XmlNode};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;

use super::{DistillUnit, GenericPlanNode};
use crate::expr::{Expr, ExprRewriter, ExprVisitor, TableFunction as ExprTableFunction};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::utils::childless_record;
use crate::optimizer::property::FunctionalDependencySet;

/// The convention-independent core of a table function plan node.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct TableFunction {
    pub table_function: ExprTableFunction,
    pub with_ordinality: bool,

    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,
}

impl TableFunction {
    pub fn new(
        table_function: ExprTableFunction,
        with_ordinality: bool,
        ctx: OptimizerContextRef,
    ) -> Self {
        Self {
            table_function,
            with_ordinality,
            ctx,
        }
    }

    pub fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.table_function.args = std::mem::take(&mut self.table_function.args)
            .into_iter()
            .map(|e| r.rewrite_expr(e))
            .collect();
    }

    pub fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.table_function
            .args
            .iter()
            .for_each(|e| v.visit_expr(e));
    }
}

impl GenericPlanNode for TableFunction {
    fn schema(&self) -> Schema {
        let mut schema = if let DataType::Struct(s) = self.table_function.return_type() {
            // If the function returns a struct, it will be flattened into multiple columns.
            Schema::from(&s)
        } else {
            Schema {
                fields: vec![Field::with_name(
                    self.table_function.return_type(),
                    self.table_function.name(),
                )],
            }
        };
        if self.with_ordinality {
            schema
                .fields
                .push(Field::with_name(DataType::Int64, "ordinality"));
        }
        schema
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        None
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        FunctionalDependencySet::new(self.schema().len())
    }
}

impl DistillUnit for TableFunction {
    fn distill_with_name<'a>(&self, name: impl Into<Str<'a>>) -> XmlNode<'a> {
        childless_record(
            name,
            vec![("table_function", Pretty::debug(&self.table_function))],
        )
    }
}
