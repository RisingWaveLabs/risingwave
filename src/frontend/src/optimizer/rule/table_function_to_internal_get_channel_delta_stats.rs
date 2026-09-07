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

use std::rc::Rc;

use anyhow::{anyhow, bail};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, ScalarImpl};

use super::prelude::{PlanRef, *};
use crate::expr::TableFunctionType;
use crate::optimizer::OptimizerContext;
use crate::optimizer::plan_node::{Logical, LogicalGetChannelDeltaStats, LogicalTableFunction};
use crate::optimizer::rule::{ApplyResult, FallibleRule};

/// Helper function to extract a constant u64 value from an `ExprImpl`.
/// Returns `Ok(Some(value))` if the expression can be folded to a constant u64,
/// `Ok(None)` if the expression is null, or an error if folding fails.
/// Negative values are rejected as they are not valid for time parameters.
fn expr_impl_to_u64_fn(arg: &crate::expr::ExprImpl) -> anyhow::Result<Option<u64>> {
    match arg
        .clone()
        .cast_implicit(&DataType::Int64)?
        .try_fold_const()
    {
        Some(Ok(value)) => {
            let Some(scalar) = value else {
                return Ok(None);
            };
            match scalar {
                ScalarImpl::Int64(value) => {
                    if value < 0 {
                        Err(anyhow::anyhow!(
                            "time parameter cannot be negative, got {}",
                            value
                        ))
                    } else {
                        Ok(Some(value as u64))
                    }
                }
                _ => Err(anyhow::anyhow!("expected int64, got {:?}", scalar)),
            }
        }
        Some(Err(err)) => Err(anyhow!(err).context("failed to fold constant")),
        None => Err(anyhow::anyhow!("expression must be a constant value")),
    }
}

/// Transform the `internal_get_channel_delta_stats()` table function
/// into a plan graph which will return channel statistics from the dashboard API.
/// It will return channel stats with `upstream_fragment_id` and `downstream_fragment_id` as primary key.
pub struct TableFunctionToInternalGetChannelDeltaStatsRule {}
impl FallibleRule<Logical> for TableFunctionToInternalGetChannelDeltaStatsRule {
    fn apply(&self, plan: PlanRef) -> ApplyResult<PlanRef> {
        let logical_table_function: &LogicalTableFunction = plan.as_logical_table_function()?;
        if logical_table_function.table_function().function_type
            != TableFunctionType::InternalGetChannelDeltaStats
        {
            return ApplyResult::NotApplicable;
        }
        let plan = Self::build_plan(plan.ctx(), logical_table_function.table_function())?;
        ApplyResult::Ok(plan)
    }
}

impl TableFunctionToInternalGetChannelDeltaStatsRule {
    fn build_plan(
        ctx: Rc<OptimizerContext>,
        table_function: &crate::expr::TableFunction,
    ) -> anyhow::Result<PlanRef> {
        let fields = vec![
            Field::new("upstream_fragment_id", DataType::Int32),
            Field::new("downstream_fragment_id", DataType::Int32),
            Field::new("backpressure_rate", DataType::Float64),
            Field::new("recv_throughput", DataType::Float64),
            Field::new("send_throughput", DataType::Float64),
        ];

        // Extract parameters if provided
        let (at_time, time_offset) = match table_function.args.len() {
            0 => (None, None), // No default, let the service handle it
            2 => {
                let at_expr = &table_function.args[0];
                let offset_expr = &table_function.args[1];

                let at_time = expr_impl_to_u64_fn(at_expr)?;
                let time_offset = expr_impl_to_u64_fn(offset_expr)?;

                (at_time, time_offset)
            }
            _ => {
                bail!("internal_get_channel_delta_stats expects 0 or 2 arguments");
            }
        };

        // Create a LogicalGetChannelDeltaStats node with the extracted parameters
        let plan = LogicalGetChannelDeltaStats::new(ctx, Schema::new(fields), at_time, time_offset);
        Ok(plan.into())
    }
}

impl TableFunctionToInternalGetChannelDeltaStatsRule {
    pub fn create() -> BoxedRule {
        Box::new(TableFunctionToInternalGetChannelDeltaStatsRule {})
    }
}
