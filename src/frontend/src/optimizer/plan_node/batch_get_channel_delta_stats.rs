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

use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::impl_distill_by_unit;
use super::{
    BatchPlanRef as PlanRef, ExprRewritable, PlanBase, PlanTreeNodeLeaf, ToBatchPb,
    ToDistributedBatch, generic,
};
use crate::error::Result;
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Distribution, Order};

/// `BatchGetChannelDeltaStats` represents a batch plan node that retrieves channel statistics
/// from the dashboard API. It has no inputs and returns channel stats data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchGetChannelDeltaStats {
    pub base: PlanBase<Batch>,
    core: generic::GetChannelDeltaStats,
}

impl PlanTreeNodeLeaf for BatchGetChannelDeltaStats {}
impl_plan_tree_node_for_leaf! { Batch, BatchGetChannelDeltaStats }

impl BatchGetChannelDeltaStats {
    pub fn new(core: generic::GetChannelDeltaStats) -> Self {
        Self::with_dist(core, Distribution::Single)
    }

    pub fn with_dist(core: generic::GetChannelDeltaStats, dist: Distribution) -> Self {
        let base = PlanBase::new_batch_with_core(&core, dist, Order::any());
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

impl_distill_by_unit!(BatchGetChannelDeltaStats, core, "BatchGetChannelDeltaStats");

impl ToDistributedBatch for BatchGetChannelDeltaStats {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(self.core.clone(), Distribution::Single).into())
    }
}

impl ToBatchPb for BatchGetChannelDeltaStats {
    fn to_batch_prost_body(&self) -> NodeBody {
        use risingwave_pb::batch_plan::GetChannelDeltaStatsNode;

        NodeBody::GetChannelDeltaStats(GetChannelDeltaStatsNode {
            at_time: self.core.at_time,
            time_offset: self.core.time_offset,
        })
    }
}

impl ToLocalBatch for BatchGetChannelDeltaStats {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(self.core.clone(), Distribution::Single).into())
    }
}

impl ExprRewritable<Batch> for BatchGetChannelDeltaStats {}

impl crate::optimizer::plan_node::expr_visitable::ExprVisitable for BatchGetChannelDeltaStats {}
