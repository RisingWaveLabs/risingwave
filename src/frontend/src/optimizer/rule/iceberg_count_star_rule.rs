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

use risingwave_pb::batch_plan::iceberg_scan_node::IcebergScanType;

use super::prelude::*;
use crate::expr::ExprImpl;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalValues, PlanAggCall, PlanTreeNodeUnary};

pub struct IcebergCountStarRule;

impl Rule<Logical> for IcebergCountStarRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg = plan.as_logical_agg()?;
        let core = agg.core();
        if core.group_key.is_empty()
            && core.grouping_sets.is_empty()
            && core.agg_calls.len() == 1
            && core.agg_calls[0] == PlanAggCall::count_star()
        {
            let input = agg.input();
            let iceberg_scan = input.as_logical_iceberg_scan()?;
            if iceberg_scan.iceberg_scan_type() != IcebergScanType::DataScan {
                return None;
            }
            if iceberg_scan.task.predicate().is_some() {
                return None;
            }
            if iceberg_scan
                .task
                .tasks()
                .iter()
                .any(|task| !task.deletes.is_empty())
            {
                return None;
            }
            let count: i64 = iceberg_scan
                .task
                .tasks()
                .iter()
                .map(|task| task.record_count)
                .sum::<Option<u64>>()?
                .try_into()
                .ok()?;
            return Some(
                LogicalValues::new(
                    vec![vec![ExprImpl::literal_bigint(count)]],
                    agg.schema().clone(),
                    agg.ctx(),
                )
                .into(),
            );
        }
        None
    }
}

impl IcebergCountStarRule {
    pub fn create() -> BoxedRule {
        Box::new(Self)
    }
}
