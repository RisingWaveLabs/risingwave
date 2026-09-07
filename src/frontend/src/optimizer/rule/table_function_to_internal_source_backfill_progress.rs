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
use risingwave_common::catalog::{Field, Schema, is_source_backfill_table};
use risingwave_common::id::{FragmentId, JobId, TableId};
use risingwave_common::types::{DataType, ScalarImpl};

use super::prelude::{PlanRef, *};
use crate::TableCatalog;
use crate::catalog::catalog_service::CatalogReadGuard;
use crate::expr::{ExprImpl, InputRef, Literal, TableFunctionType};
use crate::optimizer::OptimizerContext;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    Logical, LogicalProject, LogicalScan, LogicalTableFunction, LogicalUnion, LogicalValues,
    StreamSourceScan,
};
use crate::optimizer::rule::{ApplyResult, FallibleRule};

/// Transform the `internal_source_backfill_progress()` table function
/// into a plan graph which will scan the state tables of source backfill nodes.
/// It will return the progress of the source backfills,
/// partitioned by the backfill node's fragment id and partition id.
pub struct TableFunctionToInternalSourceBackfillProgressRule {}
impl FallibleRule<Logical> for TableFunctionToInternalSourceBackfillProgressRule {
    fn apply(&self, plan: PlanRef) -> ApplyResult<PlanRef> {
        let logical_table_function: &LogicalTableFunction = plan.as_logical_table_function()?;
        if logical_table_function.table_function().function_type
            != TableFunctionType::InternalSourceBackfillProgress
        {
            return ApplyResult::NotApplicable;
        }

        let reader = plan.ctx().session_ctx().env().catalog_reader().read_guard();
        let backfilling_tables = get_source_backfilling_tables(reader);
        let plan = Self::build_plan(plan.ctx(), backfilling_tables)?;
        ApplyResult::Ok(plan)
    }
}

impl TableFunctionToInternalSourceBackfillProgressRule {
    fn build_plan(
        ctx: Rc<OptimizerContext>,
        backfilling_tables: Vec<Arc<TableCatalog>>,
    ) -> anyhow::Result<PlanRef> {
        if backfilling_tables.is_empty() {
            let fields = vec![
                Field::new("job_id", DataType::Int32),
                Field::new("fragment_id", DataType::Int32),
                Field::new("backfill_state_table_id", DataType::Int32),
                Field::new("partition_id", DataType::Varchar),
                Field::new("backfill_progress", DataType::Jsonb),
            ];
            let plan = LogicalValues::new(vec![], Schema::new(fields), ctx);
            return Ok(plan.into());
        }

        let mut all_progress = Vec::with_capacity(backfilling_tables.len());
        for table in backfilling_tables {
            let backfill_info = SourceBackfillInfo::new(&table)?;

            let scan = Self::build_scan(ctx.clone(), table);
            let project = Self::build_project(&backfill_info, scan.into())?;

            all_progress.push(project.into());
        }
        Ok(LogicalUnion::new(true, all_progress).into())
    }

    fn build_scan(ctx: Rc<OptimizerContext>, table: Arc<TableCatalog>) -> LogicalScan {
        LogicalScan::create(table, ctx, None)
    }

    fn build_project(
        backfill_info: &SourceBackfillInfo,
        scan: PlanRef,
    ) -> anyhow::Result<LogicalProject> {
        let job_id_expr = Self::build_u32_expr(backfill_info.job_id.as_raw_id());
        let fragment_id_expr = Self::build_u32_expr(backfill_info.fragment_id.as_raw_id());
        let table_id_expr = Self::build_u32_expr(backfill_info.table_id.as_raw_id());

        let partition_id = ExprImpl::InputRef(Box::new(InputRef {
            index: backfill_info.partition_id_column_index,
            data_type: DataType::Varchar,
        }));

        let backfill_progress = ExprImpl::InputRef(Box::new(InputRef {
            index: backfill_info.backfill_progress_column_index,
            data_type: DataType::Jsonb,
        }));

        Ok(LogicalProject::new(
            scan,
            vec![
                job_id_expr,
                fragment_id_expr,
                table_id_expr,
                partition_id,
                backfill_progress,
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

fn get_source_backfilling_tables(reader: CatalogReadGuard) -> Vec<Arc<TableCatalog>> {
    reader
        .iter_backfilling_internal_tables()
        .filter(|table| is_source_backfill_table(&table.name))
        .cloned()
        .collect_vec()
}

impl TableFunctionToInternalSourceBackfillProgressRule {
    pub fn create() -> BoxedRule {
        Box::new(TableFunctionToInternalSourceBackfillProgressRule {})
    }
}

struct SourceBackfillInfo {
    job_id: JobId,
    fragment_id: FragmentId,
    table_id: TableId,
    partition_id_column_index: usize,
    backfill_progress_column_index: usize,
}

impl SourceBackfillInfo {
    fn new(table: &TableCatalog) -> anyhow::Result<Self> {
        let Some(job_id) = table.job_id else {
            bail!("`job_id` column not found in source backfill table catalog");
        };
        let Some(backfill_progress_column_index) = table
            .columns
            .iter()
            .position(|c| c.name() == StreamSourceScan::BACKFILL_PROGRESS_COLUMN_NAME)
        else {
            bail!(
                "`{}` column not found in source backfill state table schema",
                StreamSourceScan::BACKFILL_PROGRESS_COLUMN_NAME
            );
        };
        let Some(partition_id_column_index) = table
            .columns
            .iter()
            .position(|c| c.name() == StreamSourceScan::PARTITION_ID_COLUMN_NAME)
        else {
            bail!(
                "`{}` column not found in source backfill state table schema",
                StreamSourceScan::PARTITION_ID_COLUMN_NAME
            );
        };
        let fragment_id = table.fragment_id;
        let table_id = table.id;

        Ok(Self {
            job_id,
            fragment_id,
            table_id,
            partition_id_column_index,
            backfill_progress_column_index,
        })
    }
}
