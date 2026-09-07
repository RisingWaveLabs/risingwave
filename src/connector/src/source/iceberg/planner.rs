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

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::time::{Duration, Instant};

use anyhow::Context;
use futures::StreamExt;
use futures::stream::BoxStream;
use futures_async_stream::try_stream;
use iceberg::expr::BoundPredicate;
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::{DataContentType, DataFileFormat, SchemaRef};
use iceberg::table::Table;
use risingwave_common::bail;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntCounter};
use risingwave_common::types::{JsonbRef, JsonbVal, ScalarRef};
use risingwave_pb::batch_plan::iceberg_scan_node::IcebergScanType;
use serde::{Deserialize, Serialize};

use super::metrics::GLOBAL_ICEBERG_SCAN_METRICS;
use super::{IcebergFileScanTask, IcebergProperties, IcebergScanOpts, IcebergSplit};
use crate::error::ConnectorResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergScanTaskBatchMode {
    /// Keep the requested parallelism by returning empty task batches when there are fewer files
    /// than workers. This is used by the distributed RisingWave batch scheduler.
    PreserveParallelism,
    /// Avoid empty partitions. This is used by DataFusion's local physical plan.
    Compact,
}

pub struct IcebergScanTaskPlanner;

pub type IcebergScanTaskStream = BoxStream<'static, ConnectorResult<FileScanTask>>;

#[derive(Debug, Clone, Default)]
pub struct IcebergScanProjection {
    columns: Option<Vec<String>>,
}

impl IcebergScanProjection {
    pub fn all() -> Self {
        Self { columns: None }
    }

    pub fn from_downstream_columns(columns: Option<&[ColumnCatalog]>) -> Self {
        Self {
            columns: columns.map(|columns| {
                columns
                    .iter()
                    .filter_map(|col| {
                        if col.is_hidden() {
                            None
                        } else {
                            Some(col.name().to_owned())
                        }
                    })
                    .collect()
            }),
        }
    }

    fn apply<'a>(
        &self,
        scan_builder: iceberg::scan::TableScanBuilder<'a>,
    ) -> iceberg::scan::TableScanBuilder<'a> {
        if let Some(columns) = &self.columns {
            scan_builder.select(columns)
        } else {
            scan_builder.select_all()
        }
    }
}

#[derive(Debug, Clone)]
pub struct IcebergScanMetricsLabels {
    source_id: String,
    source_name: String,
    table_name: String,
    snapshots_discovered_total: LabelGuardedIntCounter,
    list_duration_seconds: LabelGuardedHistogram,
    delete_files_per_data_file: LabelGuardedHistogram,
    data_files_discovered_total: LabelGuardedIntCounter,
    equality_delete_files_discovered_total: LabelGuardedIntCounter,
    position_delete_files_discovered_total: LabelGuardedIntCounter,
    list_errors_total: LabelGuardedIntCounter,
    fetch_errors_total: LabelGuardedIntCounter,
}

impl IcebergScanMetricsLabels {
    pub fn new(source_id: String, source_name: String, table_name: String) -> Self {
        let labels = [
            source_id.as_str(),
            source_name.as_str(),
            table_name.as_str(),
        ];
        Self {
            snapshots_discovered_total: GLOBAL_ICEBERG_SCAN_METRICS
                .iceberg_source_snapshots_discovered_total
                .with_guarded_label_values(&labels),
            list_duration_seconds: GLOBAL_ICEBERG_SCAN_METRICS
                .iceberg_source_list_duration_seconds
                .with_guarded_label_values(&labels),
            delete_files_per_data_file: GLOBAL_ICEBERG_SCAN_METRICS
                .iceberg_source_delete_files_per_data_file
                .with_guarded_label_values(&labels),
            data_files_discovered_total: GLOBAL_ICEBERG_SCAN_METRICS
                .iceberg_source_files_discovered_total
                .with_guarded_label_values(&[labels[0], labels[1], labels[2], "data"]),
            equality_delete_files_discovered_total: GLOBAL_ICEBERG_SCAN_METRICS
                .iceberg_source_files_discovered_total
                .with_guarded_label_values(&[labels[0], labels[1], labels[2], "eq_delete"]),
            position_delete_files_discovered_total: GLOBAL_ICEBERG_SCAN_METRICS
                .iceberg_source_files_discovered_total
                .with_guarded_label_values(&[labels[0], labels[1], labels[2], "pos_delete"]),
            list_errors_total: GLOBAL_ICEBERG_SCAN_METRICS
                .iceberg_source_scan_errors_total
                .with_guarded_label_values(&[labels[0], labels[1], labels[2], "list_error"]),
            fetch_errors_total: GLOBAL_ICEBERG_SCAN_METRICS
                .iceberg_source_scan_errors_total
                .with_guarded_label_values(&[labels[0], labels[1], labels[2], "fetch_error"]),
            source_id,
            source_name,
            table_name,
        }
    }

    pub fn record_scan_error(&self, error_kind: &str) {
        match error_kind {
            "list_error" => self.list_errors_total.inc(),
            "fetch_error" => self.fetch_errors_total.inc(),
            _ => tracing::warn!(error_kind, "unknown Iceberg scan error metric label"),
        }
    }

    pub fn record_snapshot_discovered(&self) {
        self.snapshots_discovered_total.inc();
    }

    pub fn record_snapshot_lag(&self, lag_secs: i64) {
        GLOBAL_ICEBERG_SCAN_METRICS
            .iceberg_source_snapshot_lag_seconds
            .with_guarded_label_values(&[
                self.source_id.as_str(),
                self.source_name.as_str(),
                self.table_name.as_str(),
            ])
            .set(lag_secs);
    }

    pub fn record_caught_up(&self) {
        self.record_snapshot_lag(0);
    }

    fn record_list_duration(&self, duration: Duration) {
        self.list_duration_seconds.observe(duration.as_secs_f64());
    }

    fn record_delete_files_per_data_file(&self, delete_file_count: usize) {
        self.delete_files_per_data_file
            .observe(delete_file_count as f64);
    }

    fn record_file_counts(&self, stats: &IcebergScanPlanStats) {
        for (metric, count) in [
            (&self.data_files_discovered_total, stats.data_file_count),
            (
                &self.equality_delete_files_discovered_total,
                stats.eq_delete_count,
            ),
            (
                &self.position_delete_files_discovered_total,
                stats.pos_delete_count,
            ),
        ] {
            if count > 0 {
                metric.inc_by(count);
            }
        }
    }

    pub fn record_fetch_error(&self) {
        self.fetch_errors_total.inc();
    }

    pub fn set_inflight_file_count(&self, count: usize) {
        GLOBAL_ICEBERG_SCAN_METRICS
            .iceberg_source_inflight_file_count
            .with_guarded_label_values(&[
                self.source_id.as_str(),
                self.source_name.as_str(),
                self.table_name.as_str(),
            ])
            .set(count as i64);
    }
}

#[derive(Debug, Default)]
struct IcebergScanPlanStats {
    data_file_count: u64,
    eq_delete_count: u64,
    pos_delete_count: u64,
}

impl IcebergScanPlanStats {
    fn record_task(&mut self, scan_task: &FileScanTask) {
        self.data_file_count += 1;
        for delete_task in &scan_task.deletes {
            match delete_task.file_type {
                DataContentType::EqualityDeletes => self.eq_delete_count += 1,
                DataContentType::PositionDeletes => self.pos_delete_count += 1,
                _ => {}
            }
        }
    }
}

pub struct IcebergScanPlan {
    pub snapshot_id: i64,
    pub tasks: IcebergScanTaskStream,
}

pub enum IcebergIncrementalScan {
    EmptyTable,
    UpToDate { current_snapshot_id: i64 },
    Planned(IcebergScanPlan),
}

#[derive(Clone)]
pub struct IcebergScanPlanner {
    properties: IcebergProperties,
    projection: IcebergScanProjection,
    metrics: Option<IcebergScanMetricsLabels>,
}

impl IcebergScanPlanner {
    pub fn new(
        properties: IcebergProperties,
        projection: IcebergScanProjection,
        metrics: Option<IcebergScanMetricsLabels>,
    ) -> Self {
        Self {
            properties,
            projection,
            metrics,
        }
    }

    pub async fn plan_current_snapshot(&self) -> ConnectorResult<Option<IcebergScanPlan>> {
        let table = self.properties.load_table().await?;
        let Some(current_snapshot) = table.metadata().current_snapshot() else {
            return Ok(None);
        };
        self.plan_snapshot(&table, current_snapshot.snapshot_id())
            .await
            .map(Some)
    }

    pub async fn plan_incremental(
        &self,
        last_snapshot: Option<i64>,
    ) -> ConnectorResult<IcebergIncrementalScan> {
        let table = self.properties.load_table().await?;

        let Some(current_snapshot) = table.metadata().current_snapshot() else {
            tracing::info!("Skip incremental scan because table is empty");
            return Ok(IcebergIncrementalScan::EmptyTable);
        };

        let current_snapshot_id = current_snapshot.snapshot_id();
        if Some(current_snapshot_id) == last_snapshot {
            if let Some(metrics) = &self.metrics {
                metrics.record_caught_up();
            }
            tracing::info!(
                "Current table snapshot is already enumerated: {}, no new snapshot available",
                current_snapshot_id
            );
            return Ok(IcebergIncrementalScan::UpToDate {
                current_snapshot_id,
            });
        }

        if let Some(metrics) = &self.metrics {
            if let Some(last_snapshot_id) = last_snapshot
                && let Some(last_ingested_snapshot) = table
                    .metadata()
                    .snapshots()
                    .find(|snapshot| snapshot.snapshot_id() == last_snapshot_id)
            {
                let lag_secs = (current_snapshot.timestamp_ms()
                    - last_ingested_snapshot.timestamp_ms())
                .max(0)
                    / 1000;
                metrics.record_snapshot_lag(lag_secs);
            }
            metrics.record_snapshot_discovered();
        }

        let mut scan_builder = table.scan().to_snapshot_id(current_snapshot_id);
        if let Some(last_snapshot) = last_snapshot {
            scan_builder = scan_builder.from_snapshot_id(last_snapshot);
        }
        let scan = self.projection.apply(scan_builder).build()?;
        let tasks = self.instrument_scan_task_stream(scan.plan_files().await?);

        Ok(IcebergIncrementalScan::Planned(IcebergScanPlan {
            snapshot_id: current_snapshot_id,
            tasks,
        }))
    }

    pub fn record_caught_up(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.record_caught_up();
        }
    }

    async fn plan_snapshot(
        &self,
        table: &Table,
        snapshot_id: i64,
    ) -> ConnectorResult<IcebergScanPlan> {
        let scan = self
            .projection
            .apply(table.scan().snapshot_id(snapshot_id))
            .build()?;
        let tasks = self.instrument_scan_task_stream(scan.plan_files().await?);

        Ok(IcebergScanPlan { snapshot_id, tasks })
    }

    fn instrument_scan_task_stream(
        &self,
        scan_tasks: iceberg::scan::FileScanTaskStream,
    ) -> IcebergScanTaskStream {
        instrument_scan_task_stream(scan_tasks, self.metrics.clone())
    }
}

#[try_stream(boxed, ok = FileScanTask, error = crate::error::ConnectorError)]
async fn instrument_scan_task_stream(
    scan_tasks: iceberg::scan::FileScanTaskStream,
    metrics: Option<IcebergScanMetricsLabels>,
) {
    let mut list_duration = Duration::default();
    let mut active_since = Instant::now();
    let mut stats = IcebergScanPlanStats::default();

    let mut scan_tasks = scan_tasks;
    while let Some(scan_task) = scan_tasks.next().await {
        let scan_task = scan_task?;
        if let Some(metrics) = &metrics {
            stats.record_task(&scan_task);
            metrics.record_delete_files_per_data_file(scan_task.deletes.len());
        }
        list_duration += active_since.elapsed();
        yield scan_task;
        active_since = Instant::now();
    }
    list_duration += active_since.elapsed();

    if let Some(metrics) = &metrics {
        metrics.record_list_duration(list_duration);
        metrics.record_file_counts(&stats);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedFileScanTask {
    pub start: u64,
    pub length: u64,
    pub record_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_row_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_sequence_number: Option<i64>,
    pub data_file_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub referenced_data_file: Option<String>,
    pub data_file_content: DataContentType,
    pub data_file_format: DataFileFormat,
    pub schema: SchemaRef,
    pub project_field_ids: Vec<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<BoundPredicate>,
    pub deletes: Vec<PersistedFileScanTask>,
    #[serde(default)]
    pub sequence_number: i64,
    #[serde(default)]
    pub equality_ids: Option<Vec<i32>>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub partition_spec_id: i32,
    pub file_size_in_bytes: u64,
    #[serde(default = "default_case_sensitive")]
    pub case_sensitive: bool,
}

fn default_case_sensitive() -> bool {
    true
}

fn is_zero(value: &i32) -> bool {
    *value == 0
}

impl PersistedFileScanTask {
    pub fn decode(jsonb_ref: JsonbRef<'_>) -> ConnectorResult<FileScanTask> {
        let json = jsonb_ref.to_owned_scalar().take();
        let persisted_task: Self = serde_json::from_value(json.clone()).with_context(|| {
            format!("failed to decode persisted iceberg file scan task from json `{json}`")
        })?;
        Ok(Self::to_task(persisted_task))
    }

    pub fn encode(task: FileScanTask) -> ConnectorResult<JsonbVal> {
        let persisted_task = Self::from_task(task);
        Ok(serde_json::to_value(persisted_task)?.into())
    }

    fn to_task(
        Self {
            start,
            length,
            record_count,
            first_row_id,
            data_sequence_number,
            data_file_path,
            referenced_data_file: _,
            data_file_content: _,
            data_file_format,
            schema,
            project_field_ids,
            predicate,
            deletes,
            sequence_number,
            equality_ids: _,
            partition_spec_id: _,
            file_size_in_bytes,
            case_sensitive,
        }: Self,
    ) -> FileScanTask {
        FileScanTask {
            start,
            length,
            record_count,
            first_row_id,
            data_sequence_number,
            data_file_path,
            data_file_format,
            schema,
            project_field_ids,
            predicate,
            deletes: deletes
                .into_iter()
                .map(PersistedFileScanTask::into_delete_task)
                .collect(),
            sequence_number,
            file_size_in_bytes,
            partition: None,
            partition_spec: None,
            name_mapping: None,
            unified_partition_type: None,
            case_sensitive,
            key_metadata: None,
            file_sequence_number: None,
        }
    }

    fn from_task(
        FileScanTask {
            start,
            length,
            record_count,
            first_row_id,
            data_sequence_number,
            data_file_path,
            data_file_format,
            schema,
            project_field_ids,
            predicate,
            deletes,
            sequence_number,
            file_sequence_number: _,
            file_size_in_bytes,
            case_sensitive,
            ..
        }: FileScanTask,
    ) -> Self {
        let persisted_deletes = deletes
            .into_iter()
            .map(|task| {
                PersistedFileScanTask::from_delete_task(
                    task,
                    schema.clone(),
                    project_field_ids.clone(),
                    case_sensitive,
                )
            })
            .collect();
        Self {
            start,
            length,
            record_count,
            first_row_id,
            data_sequence_number,
            data_file_path,
            referenced_data_file: None,
            data_file_content: DataContentType::Data,
            data_file_format,
            schema,
            project_field_ids,
            predicate,
            deletes: persisted_deletes,
            sequence_number,
            equality_ids: None,
            partition_spec_id: 0,
            file_size_in_bytes,
            case_sensitive,
        }
    }

    fn into_delete_task(self) -> FileScanTaskDeleteFile {
        let Self {
            start,
            length,
            record_count,
            data_file_path,
            referenced_data_file,
            data_file_content,
            data_file_format,
            equality_ids,
            sequence_number,
            partition_spec_id,
            file_size_in_bytes,
            ..
        } = self;
        let (content_offset, content_size_in_bytes) = if data_file_format == DataFileFormat::Puffin
        {
            (i64::try_from(start).ok(), i64::try_from(length).ok())
        } else {
            (None, None)
        };

        FileScanTaskDeleteFile {
            file_path: data_file_path,
            file_size_in_bytes,
            file_type: data_file_content,
            partition_spec_id,
            equality_ids,
            file_format: data_file_format,
            referenced_data_file,
            content_offset,
            content_size_in_bytes,
            record_count,
            sequence_number,
            // Persisted split state deliberately excludes plaintext encryption key material.
            key_metadata: None,
        }
    }

    fn from_delete_task(
        task: FileScanTaskDeleteFile,
        schema: SchemaRef,
        project_field_ids: Vec<i32>,
        case_sensitive: bool,
    ) -> Self {
        let (start, length) = if task.file_format == DataFileFormat::Puffin {
            (
                task.content_offset.unwrap_or_default().max(0) as u64,
                task.content_size_in_bytes.unwrap_or_default().max(0) as u64,
            )
        } else {
            (0, task.file_size_in_bytes)
        };

        Self {
            start,
            length,
            record_count: task.record_count,
            first_row_id: None,
            data_sequence_number: None,
            data_file_path: task.file_path,
            referenced_data_file: task.referenced_data_file,
            data_file_content: task.file_type,
            data_file_format: task.file_format,
            schema,
            project_field_ids,
            predicate: None,
            deletes: Vec::new(),
            sequence_number: task.sequence_number,
            equality_ids: task.equality_ids,
            partition_spec_id: task.partition_spec_id,
            file_size_in_bytes: task.file_size_in_bytes,
            case_sensitive,
        }
    }
}

impl IcebergFileScanTask {
    pub fn scan_type(&self) -> IcebergScanType {
        match self {
            IcebergFileScanTask::Data(_) => IcebergScanType::DataScan,
            IcebergFileScanTask::EqualityDelete(_) => IcebergScanType::EqualityDeleteScan,
            IcebergFileScanTask::PositionDelete(_) => IcebergScanType::PositionDeleteScan,
        }
    }

    pub fn from_tasks(
        scan_type: IcebergScanType,
        tasks: Vec<FileScanTask>,
    ) -> ConnectorResult<Self> {
        match scan_type {
            IcebergScanType::DataScan => Ok(IcebergFileScanTask::Data(tasks)),
            IcebergScanType::EqualityDeleteScan => Ok(IcebergFileScanTask::EqualityDelete(tasks)),
            IcebergScanType::PositionDeleteScan => Ok(IcebergFileScanTask::PositionDelete(tasks)),
            _ => {
                bail!("unsupported Iceberg file scan task type: {:?}", scan_type)
            }
        }
    }

    pub fn into_tasks(self) -> Vec<FileScanTask> {
        match self {
            IcebergFileScanTask::Data(tasks)
            | IcebergFileScanTask::EqualityDelete(tasks)
            | IcebergFileScanTask::PositionDelete(tasks) => tasks,
        }
    }
}

impl IcebergScanOpts {
    pub fn new(
        chunk_size: usize,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
        handle_delete_files: bool,
    ) -> Self {
        Self {
            chunk_size,
            need_seq_num,
            need_file_path_and_pos,
            handle_delete_files,
        }
    }
}

impl IcebergScanTaskPlanner {
    pub fn plan_splits(
        task: IcebergFileScanTask,
        split_num: usize,
        limit: Option<u64>,
    ) -> ConnectorResult<Vec<IcebergSplit>> {
        let scan_type = task.scan_type();
        if limit.is_some() && scan_type != IcebergScanType::DataScan {
            bail!("Iceberg scan limit can only be planned for data scan tasks");
        }

        let task_batches = Self::plan_task_batches(
            task,
            split_num,
            limit,
            IcebergScanTaskBatchMode::PreserveParallelism,
        );

        task_batches
            .into_iter()
            .enumerate()
            .map(|(id, tasks)| {
                Ok(IcebergSplit {
                    split_id: id.try_into().unwrap(),
                    task: IcebergFileScanTask::from_tasks(scan_type, tasks)?,
                    limit,
                })
            })
            .collect()
    }

    pub fn plan_task_batches(
        task: IcebergFileScanTask,
        split_num: usize,
        limit: Option<u64>,
        mode: IcebergScanTaskBatchMode,
    ) -> Vec<Vec<FileScanTask>> {
        let tasks = task.into_tasks();
        if limit.is_some() {
            return vec![tasks];
        }
        Self::plan_file_task_batches(tasks, split_num, mode)
    }

    pub fn plan_file_task_batches(
        file_scan_tasks: Vec<FileScanTask>,
        split_num: usize,
        mode: IcebergScanTaskBatchMode,
    ) -> Vec<Vec<FileScanTask>> {
        assert!(split_num > 0, "iceberg scan split number must be positive");
        if mode == IcebergScanTaskBatchMode::Compact && file_scan_tasks.len() <= split_num {
            return file_scan_tasks.into_iter().map(|task| vec![task]).collect();
        }

        Self::split_n_vecs(file_scan_tasks, split_num)
    }

    /// Uniformly distribute scan tasks to compute nodes.
    /// It's deterministic so that it can best utilize the data locality.
    ///
    /// # Arguments
    /// * `file_scan_tasks`: The file scan tasks to be split.
    /// * `split_num`: The number of splits to be created.
    ///
    /// This algorithm is based on a min-heap. It will push all groups into the heap, and then pop the smallest group and add the file scan task to it.
    /// Ensure that the total length of each group is as balanced as possible.
    /// The time complexity is O(n log k), where n is the number of file scan tasks and k is the number of splits.
    /// The space complexity is O(k), where k is the number of splits.
    /// The algorithm is stable, so the order of the file scan tasks will be preserved.
    pub fn split_n_vecs(
        file_scan_tasks: Vec<FileScanTask>,
        split_num: usize,
    ) -> Vec<Vec<FileScanTask>> {
        #[derive(Default)]
        struct FileScanTaskGroup {
            idx: usize,
            tasks: Vec<FileScanTask>,
            total_length: u64,
        }

        impl Ord for FileScanTaskGroup {
            fn cmp(&self, other: &Self) -> Ordering {
                // when total_length is the same, we will sort by index
                if self.total_length == other.total_length {
                    self.idx.cmp(&other.idx)
                } else {
                    self.total_length.cmp(&other.total_length)
                }
            }
        }

        impl PartialOrd for FileScanTaskGroup {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Eq for FileScanTaskGroup {}

        impl PartialEq for FileScanTaskGroup {
            fn eq(&self, other: &Self) -> bool {
                self.total_length == other.total_length && self.idx == other.idx
            }
        }

        let mut heap = BinaryHeap::new();
        // push all groups into heap
        for idx in 0..split_num {
            heap.push(Reverse(FileScanTaskGroup {
                idx,
                tasks: vec![],
                total_length: 0,
            }));
        }

        for file_task in file_scan_tasks {
            let mut group = heap.peek_mut().unwrap();
            group.0.total_length += file_task.length;
            group.0.tasks.push(file_task);
        }

        // convert heap into vec and extract tasks
        heap.into_vec()
            .into_iter()
            .map(|reverse_group| reverse_group.0.tasks)
            .collect()
    }
}
