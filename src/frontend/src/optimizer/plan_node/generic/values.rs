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

use std::sync::Arc;

use educe::Educe;
use pretty_xmlish::{Pretty, Str, XmlNode};
use risingwave_common::catalog::Schema;

use super::{DistillUnit, GenericPlanNode};
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprVisitor};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::utils::childless_record;
use crate::optimizer::property::FunctionalDependencySet;

/// The convention-independent core of a values plan node.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Values {
    pub rows: Arc<[Vec<ExprImpl>]>,
    pub schema: Schema,
    pub stream_key: Option<Vec<usize>>,

    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,
}

impl Values {
    pub fn new(rows: Vec<Vec<ExprImpl>>, schema: Schema, ctx: OptimizerContextRef) -> Self {
        Self::new_inner(rows, schema, ctx, None)
    }

    pub fn new_with_stream_key(
        rows: Vec<Vec<ExprImpl>>,
        schema: Schema,
        ctx: OptimizerContextRef,
        stream_key: Vec<usize>,
    ) -> Self {
        Self::new_inner(rows, schema, ctx, Some(stream_key))
    }

    fn new_inner(
        rows: Vec<Vec<ExprImpl>>,
        schema: Schema,
        ctx: OptimizerContextRef,
        stream_key: Option<Vec<usize>>,
    ) -> Self {
        for exprs in &rows {
            for (i, expr) in exprs.iter().enumerate() {
                assert_eq!(schema.fields()[i].data_type(), expr.return_type());
            }
        }

        Self {
            rows: rows.into(),
            schema,
            stream_key,
            ctx,
        }
    }

    pub fn rows(&self) -> &[Vec<ExprImpl>] {
        self.rows.as_ref()
    }

    pub fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.rows = self
            .rows
            .iter()
            .map(|exprs| {
                exprs
                    .iter()
                    .map(|e| r.rewrite_expr(e.clone()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
            .into();
    }

    pub fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.rows.iter().flatten().for_each(|e| v.visit_expr(e));
    }

    pub fn rows_pretty<'a>(&self) -> Pretty<'a> {
        let data = self
            .rows()
            .iter()
            .map(|row| {
                let collect = row.iter().map(Pretty::debug).collect();
                Pretty::Array(collect)
            })
            .collect();
        Pretty::Array(data)
    }
}

impl GenericPlanNode for Values {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        self.stream_key.clone()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        FunctionalDependencySet::new(self.schema.len())
    }
}

impl DistillUnit for Values {
    fn distill_with_name<'a>(&self, name: impl Into<Str<'a>>) -> XmlNode<'a> {
        childless_record(
            name,
            vec![
                ("rows", self.rows_pretty()),
                ("schema", Pretty::debug(&self.schema)),
            ],
        )
    }
}
