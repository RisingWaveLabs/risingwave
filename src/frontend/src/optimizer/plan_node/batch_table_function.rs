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

use risingwave_pb::batch_plan::TableFunctionNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::impl_distill_by_unit;
use super::{
    BatchPlanRef as PlanRef, ExprRewritable, PlanBase, PlanTreeNodeLeaf, ToBatchPb,
    ToDistributedBatch, generic,
};
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, Order};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchTableFunction {
    pub base: PlanBase<Batch>,
    core: generic::TableFunction,
}

impl PlanTreeNodeLeaf for BatchTableFunction {}
impl_plan_tree_node_for_leaf! { Batch, BatchTableFunction }

impl BatchTableFunction {
    pub fn new(core: generic::TableFunction) -> Self {
        Self::with_dist(core, Distribution::Single)
    }

    pub fn with_dist(core: generic::TableFunction, dist: Distribution) -> Self {
        let base = PlanBase::new_batch_with_core(&core, dist, Order::any());
        BatchTableFunction { base, core }
    }
}

impl_distill_by_unit!(BatchTableFunction, core, "BatchTableFunction");

impl ToDistributedBatch for BatchTableFunction {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(self.core.clone(), Distribution::Single).into())
    }
}

impl ToBatchPb for BatchTableFunction {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::TableFunction(TableFunctionNode {
            table_function: Some(self.core.table_function.to_protobuf()),
        })
    }
}

impl ToLocalBatch for BatchTableFunction {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(self.core.clone(), Distribution::Single).into())
    }
}

impl ExprRewritable<Batch> for BatchTableFunction {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core).into()
    }
}

impl ExprVisitable for BatchTableFunction {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
