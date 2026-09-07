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

use pretty_xmlish::XmlNode;
use risingwave_pb::stream_plan::ValuesNode;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;
use risingwave_pb::stream_plan::values_node::ExprTuple;

use super::stream::prelude::*;
use super::utils::{Distill, childless_record};
use super::{ExprRewritable, PlanBase, StreamNode, StreamPlanRef as PlanRef, generic};
use crate::expr::{Expr, ExprImpl, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, MonotonicityMap, WatermarkColumns};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamValues` implements `LogicalValues.to_stream()`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamValues {
    pub base: PlanBase<Stream>,
    core: generic::Values,
}

impl_plan_tree_node_for_leaf! { Stream, StreamValues }

impl StreamValues {
    /// `StreamValues` should enforce `Distribution::Single`
    pub fn new(core: generic::Values) -> Self {
        let base = PlanBase::new_stream_with_core(
            &core,
            Distribution::Single,
            StreamKind::AppendOnly,
            false,
            WatermarkColumns::new(),
            MonotonicityMap::new(),
        );
        Self { base, core }
    }

    fn row_to_protobuf(&self, row: &[ExprImpl]) -> ExprTuple {
        let cells = row.iter().map(|x| x.to_expr_proto()).collect();
        ExprTuple { cells }
    }
}

impl Distill for StreamValues {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let data = self.core.rows_pretty();
        childless_record("StreamValues", vec![("rows", data)])
    }
}

impl StreamNode for StreamValues {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        ProstStreamNode::Values(Box::new(ValuesNode {
            tuples: self
                .core
                .rows()
                .iter()
                .map(|row| self.row_to_protobuf(row))
                .collect(),
            fields: self
                .core
                .schema
                .fields()
                .iter()
                .map(|f| f.to_prost())
                .collect(),
        }))
    }
}

impl ExprRewritable<Stream> for StreamValues {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn crate::expr::ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core).into()
    }
}

impl ExprVisitable for StreamValues {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
