// Copyright 2025 RisingWave Labs
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

use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::Schema;

use super::utils::impl_distill_by_unit;
use super::{
    ColPrunable, ExprRewritable, Logical, LogicalFilter, LogicalPlanRef as PlanRef, PlanBase,
    PredicatePushdown, ToBatch, ToStream, generic,
};
use crate::error::Result;
use crate::optimizer::plan_node::{
    ColumnPruningContext, LogicalProject, PredicatePushdownContext, RewriteStreamContext,
    ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalGetChannelDeltaStats` represents a plan node that retrieves channel statistics
/// from the dashboard API. It has no inputs and returns channel stats data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalGetChannelDeltaStats {
    pub base: PlanBase<Logical>,
    core: generic::GetChannelDeltaStats,
}

impl LogicalGetChannelDeltaStats {
    /// Create a new `LogicalGetChannelDeltaStats` node
    pub fn new(
        ctx: crate::OptimizerContextRef,
        schema: Schema,
        at_time: Option<u64>,
        time_offset: Option<u64>,
    ) -> Self {
        let core = generic::GetChannelDeltaStats::new(ctx, schema, at_time, time_offset);
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }

    /// Get the `at_time` parameter
    pub fn at_time(&self) -> Option<u64> {
        self.core.at_time
    }

    /// Get the `time_offset` parameter
    pub fn time_offset(&self) -> Option<u64> {
        self.core.time_offset
    }
}

impl_plan_tree_node_for_leaf! { Logical, LogicalGetChannelDeltaStats }
impl_distill_by_unit!(
    LogicalGetChannelDeltaStats,
    core,
    "LogicalGetChannelDeltaStats"
);

impl ExprRewritable<Logical> for LogicalGetChannelDeltaStats {}

impl crate::optimizer::plan_node::expr_visitable::ExprVisitable for LogicalGetChannelDeltaStats {}

impl ColPrunable for LogicalGetChannelDeltaStats {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        LogicalProject::with_out_col_idx(self.clone().into(), required_cols.iter().cloned()).into()
    }
}

impl PredicatePushdown for LogicalGetChannelDeltaStats {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalGetChannelDeltaStats {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        use crate::optimizer::plan_node::BatchGetChannelDeltaStats;
        Ok(BatchGetChannelDeltaStats::new(self.core.clone()).into())
    }
}

impl ToStream for LogicalGetChannelDeltaStats {
    fn to_stream(
        &self,
        _ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        bail_not_implemented!("Streaming not implemented for LogicalGetChannelDeltaStats")
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        bail_not_implemented!("Streaming not implemented for LogicalGetChannelDeltaStats")
    }
}
