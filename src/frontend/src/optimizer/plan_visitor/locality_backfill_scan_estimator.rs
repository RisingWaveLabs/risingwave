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

use risingwave_pb::hummock::HummockVersionStats;

use super::{DefaultBehavior, LogicalPlanVisitor, Merge};
use crate::optimizer::plan_node::{LogicalPlanRef, LogicalScan};
use crate::optimizer::plan_visitor::PlanVisitor;

#[derive(Debug, Clone)]
pub struct LocalityBackfillScanEstimator<'a> {
    table_stats: &'a HummockVersionStats,
}

impl<'a> LocalityBackfillScanEstimator<'a> {
    pub fn estimate(plan: LogicalPlanRef, table_stats: &'a HummockVersionStats) -> u64 {
        let mut estimator = Self { table_stats };
        estimator.visit(plan)
    }
}

impl LocalityBackfillScanEstimator<'_> {
    fn table_size(&self, scan: &LogicalScan) -> u64 {
        self.table_stats
            .table_stats
            .get(&scan.table().id.as_raw_id())
            .map(|stats| {
                (stats.total_key_size.max(0) as u64)
                    .saturating_add(stats.total_value_size.max(0) as u64)
            })
            .unwrap_or_default()
    }
}

impl LogicalPlanVisitor for LocalityBackfillScanEstimator<'_> {
    type Result = u64;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(u64::saturating_add)
    }

    fn visit_logical_scan(&mut self, scan: &LogicalScan) -> Self::Result {
        self.table_size(scan)
    }
}
