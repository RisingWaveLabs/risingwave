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

use risingwave_pb::batch_plan::HopWindowNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::impl_distill_by_unit;
use super::{
    BatchPlanRef as PlanRef, ExprRewritable, PlanBase, PlanTreeNodeUnary, ToBatchPb,
    ToDistributedBatch, generic,
};
use crate::error::Result;
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Order, RequiredDist};
use crate::utils::ColIndexMappingRewriteExt;

/// `BatchHopWindow` implements [`super::LogicalHopWindow`] to evaluate specified expressions on
/// input rows
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchHopWindow {
    pub base: PlanBase<Batch>,
    core: generic::HopWindow<PlanRef>,
    window_start_exprs: Vec<ExprImpl>,
    window_end_exprs: Vec<ExprImpl>,
}

impl BatchHopWindow {
    pub fn new(
        core: generic::HopWindow<PlanRef>,
        window_start_exprs: Vec<ExprImpl>,
        window_end_exprs: Vec<ExprImpl>,
    ) -> Self {
        let distribution = core
            .i2o_col_mapping()
            .rewrite_provided_distribution(core.input.distribution());
        let orders = if core.window_slide == core.window_size {
            // A single window is emitted for each input chunk, so orders on pass-through columns
            // are preserved.
            core.input
                .orders()
                .into_iter()
                .map(|order| core.i2o_col_mapping().rewrite_provided_order(&order))
                .collect()
        } else {
            // `HopWindowExecutor` emits one whole input chunk for each overlapping window. An input
            // ordered as `[1, 2]` is therefore emitted as `[1, 2, 1, 2, ...]`, so even orders on
            // pass-through columns are not preserved.
            vec![Order::any()]
        };
        let base = PlanBase::new_batch_with_core_and_orders(&core, distribution, orders);
        BatchHopWindow {
            base,
            core,
            window_start_exprs,
            window_end_exprs,
        }
    }
}
impl_distill_by_unit!(BatchHopWindow, core, "BatchHopWindow");

impl PlanTreeNodeUnary<Batch> for BatchHopWindow {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(
            core,
            self.window_start_exprs.clone(),
            self.window_end_exprs.clone(),
        )
    }
}

impl_plan_tree_node_for_unary! { Batch, BatchHopWindow }

impl ToDistributedBatch for BatchHopWindow {
    fn to_distributed(&self) -> Result<PlanRef> {
        self.to_distributed_with_required(&Order::any(), &RequiredDist::Any)
    }

    fn to_distributed_with_required(
        &self,
        required_order: &Order,
        required_dist: &RequiredDist,
    ) -> Result<PlanRef> {
        // The hop operator will generate a multiplication of its input rows,
        // so shuffling its input instead of its output will reduce the shuffling data
        // communication.
        // We pass the required dist to its input.
        let input_required = self
            .core
            .o2i_col_mapping()
            .rewrite_required_distribution(required_dist);
        let input_required_order = if self.core.window_slide == self.core.window_size {
            self.core
                .o2i_col_mapping()
                .rewrite_required_order(required_order)
                .unwrap_or_else(Order::any)
        } else {
            // An overlapping hop window does not preserve input order, so sorting its input cannot
            // satisfy an output order requirement. Enforce the requirement on the output below.
            Order::any()
        };
        let new_input = self
            .input()
            .to_distributed_with_required(&input_required_order, &input_required)?;
        let mut new_logical = self.core.clone();
        new_logical.input = new_input;
        let batch_plan = BatchHopWindow::new(
            new_logical,
            self.window_start_exprs.clone(),
            self.window_end_exprs.clone(),
        );
        let batch_plan = required_order.enforce_if_not_satisfies(batch_plan.into())?;
        required_dist.batch_enforce_if_not_satisfies(batch_plan, required_order)
    }
}

impl ToBatchPb for BatchHopWindow {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::HopWindow(HopWindowNode {
            time_col: self.core.time_col.index() as _,
            window_slide: Some(self.core.window_slide.into()),
            window_size: Some(self.core.window_size.into()),
            output_indices: self.core.output_indices.iter().map(|&x| x as u32).collect(),
            window_start_exprs: self
                .window_start_exprs
                .clone()
                .iter()
                .map(|x| x.to_expr_proto())
                .collect(),
            window_end_exprs: self
                .window_end_exprs
                .clone()
                .iter()
                .map(|x| x.to_expr_proto())
                .collect(),
        })
    }
}

impl ToLocalBatch for BatchHopWindow {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = self.input().to_local()?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ExprRewritable<Batch> for BatchHopWindow {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        Self::new(
            self.core.clone(),
            self.window_start_exprs
                .clone()
                .into_iter()
                .map(|e| r.rewrite_expr(e))
                .collect(),
            self.window_end_exprs
                .clone()
                .into_iter()
                .map(|e| r.rewrite_expr(e))
                .collect(),
        )
        .into()
    }
}

impl ExprVisitable for BatchHopWindow {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.window_start_exprs.iter().for_each(|e| v.visit_expr(e));
        self.window_end_exprs.iter().for_each(|e| v.visit_expr(e));
    }
}
