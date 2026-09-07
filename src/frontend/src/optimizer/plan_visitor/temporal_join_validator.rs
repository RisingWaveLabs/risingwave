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

use risingwave_sqlparser::ast::AsOf;

use super::{BatchPlanVisitor, DefaultBehavior, LogicalPlanVisitor, Merge, StreamPlanVisitor};
use crate::PlanRef;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    BatchSeqScan, ConventionMarker, LogicalScan, PlanTreeNodeBinary, StreamTableScan,
    StreamTemporalJoin,
};
use crate::optimizer::plan_visitor::PlanVisitor;

#[derive(Debug, Clone, Default)]
pub struct TemporalJoinValidator {
    found_non_append_only_temporal_join: bool,
}

impl TemporalJoinValidator {
    pub fn exist_dangling_temporal_scan<C: ConventionMarker>(plan: PlanRef<C>) -> bool
    where
        Self: PlanVisitor<C, Result = bool>,
    {
        let mut decider = TemporalJoinValidator {
            found_non_append_only_temporal_join: false,
        };
        let ctx = plan.ctx();
        let has_dangling_temporal_scan = decider.visit(plan);
        if decider.found_non_append_only_temporal_join {
            ctx.session_ctx().notice_to_user("A non-append-only temporal join is used in the query. It would introduce a additional memo-table comparing to append-only temporal join.");
        }
        has_dangling_temporal_scan
    }
}

impl LogicalPlanVisitor for TemporalJoinValidator {
    type Result = bool;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a, b| a | b)
    }

    fn visit_logical_scan(&mut self, logical_scan: &LogicalScan) -> bool {
        matches!(
            logical_scan.as_of(),
            Some(AsOf::ProcessTime | AsOf::ProcessTimeBroadcast)
        )
    }
}

impl BatchPlanVisitor for TemporalJoinValidator {
    type Result = bool;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a, b| a | b)
    }

    fn visit_batch_seq_scan(&mut self, batch_seq_scan: &BatchSeqScan) -> bool {
        matches!(
            batch_seq_scan.core().as_of,
            Some(AsOf::ProcessTime | AsOf::ProcessTimeBroadcast)
        )
    }
}

impl StreamPlanVisitor for TemporalJoinValidator {
    type Result = bool;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a, b| a | b)
    }

    fn visit_stream_table_scan(&mut self, stream_table_scan: &StreamTableScan) -> bool {
        matches!(
            stream_table_scan.core().as_of,
            Some(AsOf::ProcessTime | AsOf::ProcessTimeBroadcast)
        )
    }

    fn visit_stream_temporal_join(&mut self, stream_temporal_join: &StreamTemporalJoin) -> bool {
        if !stream_temporal_join.append_only() {
            self.found_non_append_only_temporal_join = true;
        }
        self.visit_stream(stream_temporal_join.left())
    }
}
