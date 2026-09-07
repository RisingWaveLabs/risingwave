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

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, RecordBatchOptions, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;
use datafusion_common::stats::Precision;
use datafusion_common::{DataFusionError, Statistics, exec_err};
use futures::StreamExt;
use futures_async_stream::try_stream;
use iceberg::scan::FileScanTask;
use iceberg::table::Table;
use risingwave_common::catalog::{
    ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_FILE_POS_COLUMN_NAME, ICEBERG_SEQUENCE_NUM_COLUMN_NAME,
};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::source::iceberg::{
    IcebergFileScanTask, IcebergScanTaskBatchMode, IcebergScanTaskPlanner,
};

use super::{IcebergTableProvider, to_datafusion_error};

/// An execution plan for scanning Iceberg tables.
///
/// It utilizes the `IcebergProperties` to read data and produces `RecordBatches` accordingly.
#[derive(Debug, Clone)]
pub struct IcebergScan {
    inner: Arc<IcebergScanInner>,
}

#[derive(educe::Educe)]
#[educe(Debug)]
struct IcebergScanInner {
    #[educe(Debug(ignore))]
    table: Table,
    tasks: Vec<Vec<FileScanTask>>,
    statistics: Vec<Statistics>,
    total_statistics: Statistics,
    arrow_schema: SchemaRef,
    scan_column_names: Vec<String>,
    need_seq_num: bool,
    need_file_path_and_pos: bool,
    plan_properties: Arc<PlanProperties>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
}

impl DisplayAs for IcebergScan {
    fn fmt_as(
        &self,
        format_type: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        if format_type == DisplayFormatType::TreeRender {
            writeln!(f, "{}", self.inner.table.identifier())?;
            return Ok(());
        }

        write!(f, "IcebergScan: ")?;
        write!(f, "table={}", self.inner.table.identifier())?;
        write!(f, ", columns=")?;
        pretty_write_vector(f, &self.inner.column_names())?;
        if let Some(predicate) = self.inner.predicate() {
            write!(f, ", predicate={}", predicate)?;
        }
        if format_type == DisplayFormatType::Verbose {
            write!(f, ", scan_column_names={:?}", self.inner.scan_column_names)?;
            write!(f, ", need_seq_num={}", self.inner.need_seq_num)?;
            write!(
                f,
                ", need_file_path_and_pos={}",
                self.inner.need_file_path_and_pos
            )?;
            if let Some(projection) = &self.inner.projection {
                write!(f, ", projection={:?}", projection)?;
            }
            if let Some(limit) = self.inner.limit {
                write!(f, ", limit={}", limit)?;
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for IcebergScan {
    fn name(&self) -> &str {
        "IcebergScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.inner.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if partition >= self.inner.tasks.len() {
            return exec_err!("IcebergScan: partition out of bounds");
        }

        let chunk_size = context.session_config().batch_size();
        let stream = self.inner.clone().execute_inner(chunk_size, partition);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Statistics> {
        if let Some(partition) = partition {
            if partition >= self.inner.statistics.len() {
                return exec_err!("IcebergScan: partition out of bounds");
            }
            Ok(self.inner.statistics[partition].clone())
        } else {
            Ok(self.inner.total_statistics.clone())
        }
    }
}

impl IcebergScan {
    pub async fn new(
        provider: &IcebergTableProvider,
        projection: Option<&Vec<usize>>,
        // TODO: support filter pushdown
        _filters: &[Expr],
        limit: Option<usize>,
        batch_parallelism: usize,
    ) -> DFResult<Self> {
        let mut arrow_schema = provider.schema();
        if let Some(projection) = projection {
            let fields: Vec<Field> = projection
                .iter()
                .map(|i| arrow_schema.field(*i).clone())
                .collect();
            arrow_schema = Arc::new(Schema::new(fields));
        }
        let mut scan_column_names: Vec<String> = provider
            .arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let table = provider
            .iceberg_properties
            .load_table()
            .await
            .map_err(to_datafusion_error)?;
        let need_seq_num = scan_column_names
            .iter()
            .any(|name| name == ICEBERG_SEQUENCE_NUM_COLUMN_NAME);
        let need_file_path_and_pos = scan_column_names
            .iter()
            .any(|name| name == ICEBERG_FILE_PATH_COLUMN_NAME)
            && matches!(provider.task, IcebergFileScanTask::Data(_));
        scan_column_names.retain(|name| {
            ![
                ICEBERG_FILE_PATH_COLUMN_NAME,
                ICEBERG_SEQUENCE_NUM_COLUMN_NAME,
                ICEBERG_FILE_POS_COLUMN_NAME,
            ]
            .contains(&name.as_str())
        });

        let scan_tasks = provider.task.tasks();
        let tasks = IcebergScanTaskPlanner::plan_task_batches(
            provider.task.clone(),
            batch_parallelism,
            limit.map(|limit| limit.try_into().unwrap_or(u64::MAX)),
            IcebergScanTaskBatchMode::Compact,
        );
        let statistics = tasks
            .iter()
            .map(|tasks| calculate_statistics(tasks.iter(), &arrow_schema))
            .collect();
        let total_statistics = calculate_statistics(scan_tasks.iter(), &arrow_schema);
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(arrow_schema.clone()),
            Partitioning::UnknownPartitioning(tasks.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            inner: Arc::new(IcebergScanInner {
                table,
                tasks,
                statistics,
                total_statistics,
                arrow_schema,
                scan_column_names,
                need_seq_num,
                need_file_path_and_pos,
                plan_properties,
                projection: projection.cloned(),
                limit,
            }),
        })
    }
}

impl IcebergScanInner {
    #[try_stream(ok = RecordBatch, error = DataFusionError)]
    pub async fn execute_inner(self: Arc<Self>, chunk_size: usize, partition: usize) {
        let reader = self
            .table
            .reader_builder()
            .with_batch_size(chunk_size)
            .with_row_group_filtering_enabled(true)
            .build();
        let mut remaining_limit = self.limit;

        for task in &self.tasks[partition] {
            if matches!(remaining_limit, Some(0)) {
                return Ok(());
            }

            let stream = reader
                .clone()
                .read(tokio_stream::once(Ok(task.clone())).boxed())
                .map_err(to_datafusion_error)?
                .stream();
            let mut pos_start: i64 = 0;

            #[for_await]
            for batch in stream {
                let mut batch = batch.map_err(to_datafusion_error)?;
                if self.need_seq_num || self.need_file_path_and_pos {
                    batch = append_metadata(
                        batch,
                        self.need_seq_num,
                        self.need_file_path_and_pos,
                        task,
                        pos_start,
                    )?;
                }
                if let Some(projection) = &self.projection {
                    batch = batch.project(projection).map_err(to_datafusion_error)?;
                }
                batch = cast_batch(self.arrow_schema.clone(), batch)?;

                if let Some(remaining) = &mut remaining_limit {
                    if batch.num_rows() > *remaining {
                        yield batch.slice(0, *remaining);
                        return Ok(());
                    }

                    *remaining -= batch.num_rows();
                    pos_start += i64::try_from(batch.num_rows()).unwrap();
                    yield batch;

                    if *remaining == 0 {
                        return Ok(());
                    }
                } else {
                    pos_start += i64::try_from(batch.num_rows()).unwrap();
                    yield batch;
                }
            }
        }
    }

    fn column_names(&self) -> Vec<String> {
        self.arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    fn predicate(&self) -> Option<String> {
        let task = self.tasks.iter().flatten().next()?;
        let predicate = task.predicate()?;
        Some(predicate.to_string())
    }
}

fn append_metadata(
    batch: RecordBatch,
    need_seq_num: bool,
    need_file_path_and_pos: bool,
    task: &FileScanTask,
    pos_start: i64,
) -> Result<RecordBatch, DataFusionError> {
    let mut columns = batch.columns().to_vec();
    let mut fields = batch.schema().fields().to_vec();

    if need_seq_num {
        let seq_num_array = Int64Array::from_value(task.sequence_number, batch.num_rows());
        columns.push(Arc::new(seq_num_array));
        fields.push(Arc::new(Field::new(
            ICEBERG_SEQUENCE_NUM_COLUMN_NAME,
            DataType::Int64,
            false,
        )));
    }

    if need_file_path_and_pos {
        let file_path_array = StringArray::from_iter_values(std::iter::repeat_n(
            task.data_file_path(),
            batch.num_rows(),
        ));
        let file_pos_array = Int64Array::from_iter((pos_start..).take(batch.num_rows()));
        columns.push(Arc::new(file_path_array));
        columns.push(Arc::new(file_pos_array));
        fields.push(Arc::new(Field::new(
            ICEBERG_FILE_PATH_COLUMN_NAME,
            DataType::Utf8,
            false,
        )));
        fields.push(Arc::new(Field::new(
            ICEBERG_FILE_POS_COLUMN_NAME,
            DataType::Int64,
            false,
        )));
    }

    let new_schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(new_schema, columns).map_err(to_datafusion_error)
}

fn cast_batch(
    target_schema: SchemaRef,
    batch: RecordBatch,
) -> Result<RecordBatch, DataFusionError> {
    if batch.num_columns() != target_schema.fields().len() {
        return Err(DataFusionError::Internal(
            "column count must match schema column count".to_owned(),
        ));
    }

    let mut target_columns = Vec::with_capacity(batch.num_columns());
    for (column, target_field) in batch.columns().iter().zip_eq_fast(target_schema.fields()) {
        if column.data_type() == target_field.data_type() {
            target_columns.push(column.clone());
        } else {
            let casted_column = datafusion::arrow::compute::cast(column, target_field.data_type())?;
            target_columns.push(casted_column);
        }
    }

    let res = RecordBatch::try_new_with_options(
        target_schema.clone(),
        target_columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?;
    Ok(res)
}

fn calculate_statistics<'a>(
    tasks: impl Iterator<Item = &'a FileScanTask>,
    schema: &Schema,
) -> Statistics {
    let mut total_rows: Option<usize> = Some(0);
    let mut total_bytes: usize = 0;
    for task in tasks {
        match (task.record_count, &mut total_rows) {
            (Some(count), Some(rows)) => *rows += count as usize,
            (None, rows) => *rows = None,
            _ => {}
        }
        total_bytes += task.file_size_in_bytes as usize;
    }

    let num_rows = match total_rows {
        Some(rows) => Precision::Exact(rows),
        None => Precision::Absent,
    };
    Statistics {
        num_rows,
        total_byte_size: Precision::Exact(total_bytes),
        column_statistics: Statistics::unknown_column(schema),
    }
}

fn pretty_write_vector(f: &mut std::fmt::Formatter<'_>, vec: &[String]) -> std::fmt::Result {
    write!(f, "[")?;
    for (i, item) in vec.iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{}", item)?;
    }
    write!(f, "]")
}
