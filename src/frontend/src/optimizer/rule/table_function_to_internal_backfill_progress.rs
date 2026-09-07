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
use std::sync::Arc;

use anyhow::bail;
use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema, is_backfill_table};
use risingwave_common::id::{FragmentId, JobId, TableId};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_expr::aggregate::AggType;
pub use risingwave_pb::expr::agg_call::PbKind as PbAggKind;

use super::prelude::{PlanRef, *};
use crate::TableCatalog;
use crate::catalog::catalog_service::CatalogReadGuard;
use crate::expr::{AggCall, ExprImpl, InputRef, Literal, OrderBy, TableFunctionType};
use crate::optimizer::OptimizerContext;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    Logical, LogicalAgg, LogicalProject, LogicalScan, LogicalTableFunction, LogicalUnion,
    LogicalValues, StreamTableScan,
};
use crate::optimizer::rule::{ApplyResult, FallibleRule};
use crate::utils::{Condition, GroupBy};

/// Transform the `internal_backfill_progress()` table function
/// into a plan graph which will scan the state tables of backfill nodes.
/// It will return the progress of the backfills, partitioned by the backfill node's fragment id.
pub struct TableFunctionToInternalBackfillProgressRule {}
impl FallibleRule<Logical> for TableFunctionToInternalBackfillProgressRule {
    fn apply(&self, plan: PlanRef) -> ApplyResult<PlanRef> {
        let logical_table_function: &LogicalTableFunction = plan.as_logical_table_function()?;
        if logical_table_function.table_function().function_type
            != TableFunctionType::InternalBackfillProgress
        {
            return ApplyResult::NotApplicable;
        }

        let reader = plan.ctx().session_ctx().env().catalog_reader().read_guard();
        let backfilling_tables = get_backfilling_tables(reader);
        let plan = Self::build_plan(plan.ctx(), backfilling_tables)?;
        ApplyResult::Ok(plan)
    }
}

impl TableFunctionToInternalBackfillProgressRule {
    fn build_plan(
        ctx: Rc<OptimizerContext>,
        backfilling_tables: Vec<Arc<TableCatalog>>,
    ) -> anyhow::Result<PlanRef> {
        if backfilling_tables.is_empty() {
            let fields = vec![
                Field::new("job_id", DataType::Int32),
                Field::new("fragment_id", DataType::Int32),
                Field::new("backfill_state_table_id", DataType::Int32),
                Field::new("current_row_count", DataType::Int64),
                Field::new("min_epoch", DataType::Int64),
            ];
            let plan = LogicalValues::new(vec![], Schema::new(fields), ctx);
            return Ok(plan.into());
        }

        let mut all_progress = Vec::with_capacity(backfilling_tables.len());
        for table in backfilling_tables {
            let backfill_info = BackfillInfo::new(&table)?;

            let scan = Self::build_scan(ctx.clone(), table);
            let agg = Self::build_agg(&backfill_info, scan)?;
            let project = Self::build_project(&backfill_info, agg)?;

            all_progress.push(project.into());
        }
        Ok(LogicalUnion::new(true, all_progress).into())
    }

    fn build_scan(ctx: Rc<OptimizerContext>, table: Arc<TableCatalog>) -> LogicalScan {
        LogicalScan::create(table, ctx, None)
    }

    fn build_agg(backfill_info: &BackfillInfo, scan: LogicalScan) -> anyhow::Result<PlanRef> {
        let epoch_expr = match backfill_info.epoch_column_index {
            Some(epoch_column_index) => ExprImpl::InputRef(Box::new(InputRef {
                index: epoch_column_index,
                data_type: DataType::Int64,
            })),
            None => ExprImpl::Literal(Box::new(Literal::new(None, DataType::Int64))),
        };
        let aggregated_min_epoch = ExprImpl::AggCall(Box::new(AggCall::new(
            AggType::Builtin(PbAggKind::Min),
            vec![epoch_expr],
            false,
            OrderBy::any(),
            Condition::true_cond(),
            vec![],
        )?));
        let aggregated_current_row_count = ExprImpl::AggCall(Box::new(AggCall::new(
            AggType::Builtin(PbAggKind::Sum),
            vec![ExprImpl::InputRef(Box::new(InputRef {
                index: backfill_info.row_count_column_index,
                data_type: DataType::Int64,
            }))],
            false,
            OrderBy::any(),
            Condition::true_cond(),
            vec![],
        )?));
        let select_exprs = vec![aggregated_current_row_count, aggregated_min_epoch];
        let group_by = GroupBy::GroupKey(vec![]);
        let (agg, _, _) = LogicalAgg::create(select_exprs, group_by, None, scan.into())?;
        Ok(agg)
    }

    fn build_project(backfill_info: &BackfillInfo, agg: PlanRef) -> anyhow::Result<LogicalProject> {
        let job_id_expr = Self::build_u32_expr(backfill_info.job_id.as_raw_id());
        let fragment_id_expr = Self::build_u32_expr(backfill_info.fragment_id.as_raw_id());
        let table_id_expr = Self::build_u32_expr(backfill_info.table_id.as_raw_id());

        let current_count_per_vnode = ExprImpl::InputRef(Box::new(InputRef {
            index: 0,
            data_type: DataType::Decimal,
        }))
        .cast_explicit(&DataType::Int64)?;
        let min_epoch = ExprImpl::InputRef(Box::new(InputRef {
            index: 1,
            data_type: DataType::Int64,
        }));

        Ok(LogicalProject::new(
            agg,
            vec![
                job_id_expr,
                fragment_id_expr,
                table_id_expr,
                current_count_per_vnode,
                min_epoch,
            ],
        ))
    }

    fn build_u32_expr(id: u32) -> ExprImpl {
        ExprImpl::Literal(Box::new(Literal::new(
            Some(ScalarImpl::Int32(id as i32)),
            DataType::Int32,
        )))
    }
}

fn get_backfilling_tables(reader: CatalogReadGuard) -> Vec<Arc<TableCatalog>> {
    reader
        .iter_backfilling_internal_tables()
        .filter(|table| is_backfill_table(&table.name))
        .cloned()
        .collect_vec()
}

impl TableFunctionToInternalBackfillProgressRule {
    pub fn create() -> BoxedRule {
        Box::new(TableFunctionToInternalBackfillProgressRule {})
    }
}

struct BackfillInfo {
    job_id: JobId,
    fragment_id: FragmentId,
    table_id: TableId,
    row_count_column_index: usize,
    epoch_column_index: Option<usize>,
}

impl BackfillInfo {
    fn new(table: &TableCatalog) -> anyhow::Result<Self> {
        let Some(job_id) = table.job_id else {
            bail!("`job_id` column not found in backfill table");
        };
        let Some(row_count_column_index) = table
            .columns
            .iter()
            .position(|c| c.name() == StreamTableScan::ROW_COUNT_COLUMN_NAME)
        else {
            bail!(
                "`{}` column not found in backfill table",
                StreamTableScan::ROW_COUNT_COLUMN_NAME
            );
        };
        let epoch_column_index = table
            .columns
            .iter()
            .position(|c| c.name() == StreamTableScan::EPOCH_COLUMN_NAME);
        let fragment_id = table.fragment_id;
        let table_id = table.id;

        Ok(Self {
            job_id,
            fragment_id,
            table_id,
            row_count_column_index,
            epoch_column_index,
        })
    }
}
