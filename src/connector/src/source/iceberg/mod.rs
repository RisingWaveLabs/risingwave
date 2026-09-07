// Copyright 2024 RisingWave Labs
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

pub mod parquet_file_handler;
pub mod planner;

pub mod metrics;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::{for_await, try_stream};
use iceberg::Catalog;
use iceberg::expr::{BoundPredicate, Predicate as IcebergPredicate};
use iceberg::scan::FileScanTask;
use iceberg::spec::{FormatVersion, TableMetadata};
use iceberg::table::Table;
pub use parquet_file_handler::*;
use phf::{Set, phf_set};
pub use planner::{
    IcebergIncrementalScan, IcebergScanMetricsLabels, IcebergScanPlan, IcebergScanPlanner,
    IcebergScanProjection, IcebergScanTaskBatchMode, IcebergScanTaskPlanner, PersistedFileScanTask,
};
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::{
    ArrayBuilder, ArrayImpl, DataChunk, I64Array, Utf8Array, VariantArrayBuilder,
};
use risingwave_common::bail;
use risingwave_common::types::JsonbVal;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::batch_plan::iceberg_scan_node::IcebergScanType;
use serde::{Deserialize, Serialize};

pub use self::metrics::{GLOBAL_ICEBERG_SCAN_METRICS, IcebergFileScanMetrics, IcebergScanMetrics};
use crate::connector_common::{
    IcebergCommon, IcebergTableIdentifier, iceberg_java_catalog_props_from_options,
};
use crate::enforce_secret::{EnforceSecret, EnforceSecretError};
use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::ParserConfig;
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceEnumeratorContextRef, SourceProperties,
    SplitEnumerator, SplitId, SplitMetaData, SplitReader, UnknownFields,
};
pub const ICEBERG_CONNECTOR: &str = "iceberg";

#[derive(Clone, Debug, Deserialize, with_options::WithOptions)]
pub struct IcebergProperties {
    #[serde(flatten)]
    pub common: IcebergCommon,

    #[serde(flatten)]
    pub table: IcebergTableIdentifier,

    // For jdbc catalog
    #[serde(rename = "catalog.jdbc.user")]
    pub jdbc_user: Option<String>,
    #[serde(rename = "catalog.jdbc.password")]
    pub jdbc_password: Option<String>,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl EnforceSecret for IcebergProperties {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "catalog.jdbc.password",
    };

    fn enforce_secret<'a>(prop_iter: impl Iterator<Item = &'a str>) -> ConnectorResult<()> {
        for prop in prop_iter {
            IcebergCommon::enforce_one(prop)?;
            if Self::ENFORCE_SECRET_PROPERTIES.contains(prop) {
                return Err(EnforceSecretError {
                    key: prop.to_owned(),
                }
                .into());
            }
        }
        Ok(())
    }
}

impl IcebergProperties {
    fn java_catalog_props(&self) -> HashMap<String, String> {
        let mut java_catalog_props = iceberg_java_catalog_props_from_options(
            self.unknown_fields
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
        if let Some(jdbc_user) = self.jdbc_user.clone() {
            java_catalog_props.insert("jdbc.user".to_owned(), jdbc_user);
        }
        if let Some(jdbc_password) = self.jdbc_password.clone() {
            java_catalog_props.insert("jdbc.password".to_owned(), jdbc_password);
        }
        java_catalog_props
    }

    pub async fn create_catalog(&self) -> ConnectorResult<Arc<dyn Catalog>> {
        self.common
            .resolve_catalog_config(self.java_catalog_props())?
            .create_catalog()
            .await
    }

    pub async fn load_table(&self) -> ConnectorResult<Table> {
        self.common
            .resolve_catalog_config(self.java_catalog_props())?
            .load_table(&self.table)
            .await
    }
}

impl SourceProperties for IcebergProperties {
    type Split = IcebergSplit;
    type SplitEnumerator = IcebergSplitEnumerator;
    type SplitReader = IcebergFileReader;

    const SOURCE_NAME: &'static str = ICEBERG_CONNECTOR;
}

impl UnknownFields for IcebergProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IcebergFileScanTask {
    Data(Vec<FileScanTask>),
    EqualityDelete(Vec<FileScanTask>),
    PositionDelete(Vec<FileScanTask>),
}

impl IcebergFileScanTask {
    pub fn tasks(&self) -> &[FileScanTask] {
        match self {
            IcebergFileScanTask::Data(file_scan_tasks)
            | IcebergFileScanTask::EqualityDelete(file_scan_tasks)
            | IcebergFileScanTask::PositionDelete(file_scan_tasks) => file_scan_tasks,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tasks().is_empty()
    }

    pub fn files(&self) -> Vec<String> {
        self.tasks()
            .iter()
            .map(|task| task.data_file_path.clone())
            .collect()
    }

    pub fn predicate(&self) -> Option<&BoundPredicate> {
        let first_task = self.tasks().first()?;
        first_task.predicate.as_ref()
    }

    fn strip_non_serializable_planning_context(&mut self) {
        let tasks = match self {
            IcebergFileScanTask::Data(tasks)
            | IcebergFileScanTask::EqualityDelete(tasks)
            | IcebergFileScanTask::PositionDelete(tasks) => tasks,
        };

        for task in tasks {
            // These fields are deliberately not serializable in Iceberg. The previous
            // Iceberg dependency skipped them during serde, so retain that split format.
            task.partition = None;
            task.partition_spec = None;
            task.name_mapping = None;
            task.unified_partition_type = None;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergSplit {
    pub split_id: i64,
    pub task: IcebergFileScanTask,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

impl IcebergSplit {
    #[allow(deprecated)]
    pub fn empty(iceberg_scan_type: IcebergScanType) -> Self {
        let task = match iceberg_scan_type {
            IcebergScanType::DataScan => IcebergFileScanTask::Data(vec![]),
            IcebergScanType::EqualityDeleteScan => IcebergFileScanTask::EqualityDelete(vec![]),
            IcebergScanType::PositionDeleteScan => IcebergFileScanTask::PositionDelete(vec![]),
            IcebergScanType::Unspecified | IcebergScanType::CountStar => {
                // These scan types do not carry file tasks. Keep the split serializable without
                // introducing a new empty-task variant.
                IcebergFileScanTask::Data(vec![])
            }
        };
        Self {
            split_id: 0,
            task,
            limit: None,
        }
    }
}

impl SplitMetaData for IcebergSplit {
    fn id(&self) -> SplitId {
        self.split_id.to_string().into()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(|e| anyhow!(e).into())
    }

    fn encode_to_json(&self) -> JsonbVal {
        let mut split = self.clone();
        split.task.strip_non_serializable_planning_context();
        serde_json::to_value(split)
            .expect("iceberg split serialization should not fail")
            .into()
    }

    fn update_offset(&mut self, _last_seen_offset: String) -> ConnectorResult<()> {
        // Iceberg source progress is tracked by persisted file tasks in the stream state table.
        // A split does not carry an intra-file offset until partial-file reads are supported.
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct IcebergSplitEnumerator {
    config: IcebergProperties,
}

#[derive(Debug, Clone)]
pub struct IcebergDeleteParameters {
    pub equality_delete_columns: Vec<String>,
    pub has_position_delete: bool,
    pub snapshot_id: Option<i64>,
}

#[async_trait]
impl SplitEnumerator for IcebergSplitEnumerator {
    type Properties = IcebergProperties;
    type Split = IcebergSplit;

    async fn new(
        properties: Self::Properties,
        context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<Self> {
        Ok(Self::new_inner(properties, context))
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<Self::Split>> {
        // Like file source, iceberg streaming source has a List Executor and a Fetch Executor,
        // instead of relying on SplitEnumerator on meta.
        // TODO: add some validation logic here.
        Ok(vec![])
    }
}
impl IcebergSplitEnumerator {
    pub fn new_inner(properties: IcebergProperties, _context: SourceEnumeratorContextRef) -> Self {
        Self { config: properties }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IcebergTimeTravelInfo {
    Version(i64),
    TimestampMs(i64),
}

#[derive(Debug, Clone)]
pub struct IcebergListResult {
    pub data_files: Vec<FileScanTask>,
    pub equality_delete_files: Vec<FileScanTask>,
    pub position_delete_files: Vec<FileScanTask>,
    pub equality_delete_columns: Vec<String>,
    pub format_version: FormatVersion,
    pub schema: std::sync::Arc<iceberg::spec::Schema>,
}

impl IcebergSplitEnumerator {
    pub fn get_snapshot_id(
        table: &Table,
        time_travel_info: Option<IcebergTimeTravelInfo>,
    ) -> ConnectorResult<Option<i64>> {
        Self::get_snapshot_id_from_metadata(table.metadata(), time_travel_info)
    }

    fn get_snapshot_id_from_metadata(
        metadata: &TableMetadata,
        time_travel_info: Option<IcebergTimeTravelInfo>,
    ) -> ConnectorResult<Option<i64>> {
        let snapshot_id = match time_travel_info {
            Some(IcebergTimeTravelInfo::Version(version)) => {
                let Some(snapshot) = metadata.snapshot_by_id(version) else {
                    bail!("Cannot find the snapshot id in the iceberg table.");
                };
                Some(snapshot.snapshot_id())
            }
            Some(IcebergTimeTravelInfo::TimestampMs(timestamp)) => {
                let snapshot_log = metadata
                    .history()
                    .iter()
                    .rev()
                    .find(|snapshot_log| snapshot_log.timestamp_ms() <= timestamp);
                match snapshot_log {
                    Some(snapshot_log) => Some(snapshot_log.snapshot_id),
                    None => {
                        // convert unix time to human-readable time
                        let time = chrono::DateTime::from_timestamp_millis(timestamp);
                        if let Some(time) = time {
                            tracing::warn!("Cannot find a snapshot older than {}", time);
                        } else {
                            tracing::warn!("Cannot find a snapshot");
                        }
                        return Ok(None);
                    }
                }
            }
            None => metadata.current_snapshot_id(),
        };
        Ok(snapshot_id)
    }

    pub async fn list_scan_tasks(
        &self,
        time_travel_info: Option<IcebergTimeTravelInfo>,
        predicate: IcebergPredicate,
    ) -> ConnectorResult<Option<IcebergListResult>> {
        let table = self.config.load_table().await?;
        let snapshot_id = Self::get_snapshot_id(&table, time_travel_info)?;

        let Some(snapshot_id) = snapshot_id else {
            return Ok(None);
        };
        let res = self
            .list_scan_tasks_inner(&table, snapshot_id, predicate)
            .await?;
        Ok(Some(res))
    }

    async fn list_scan_tasks_inner(
        &self,
        table: &Table,
        snapshot_id: i64,
        predicate: IcebergPredicate,
    ) -> ConnectorResult<IcebergListResult> {
        let format_version = table.metadata().format_version();
        let table_schema = table.metadata().current_schema();
        tracing::debug!("iceberg_table_schema: {:?}", table_schema);

        let mut position_delete_files = vec![];
        let mut position_delete_files_set = HashSet::new();
        let mut data_files = vec![];
        let mut equality_delete_files = vec![];
        let mut equality_delete_files_set = HashSet::new();
        let mut equality_delete_ids = None;
        let mut scan_builder = table.scan().snapshot_id(snapshot_id).select_all();
        if predicate != IcebergPredicate::AlwaysTrue {
            scan_builder = scan_builder.with_filter(predicate.clone());
        }
        let scan = scan_builder.build()?;
        let file_scan_stream = scan.plan_files().await?;

        #[for_await]
        for task in file_scan_stream {
            let task: FileScanTask = task?;

            // Collect delete files for separate scan types, but keep task.deletes intact
            for delete_file in &task.deletes {
                match delete_file.file_type {
                    iceberg::spec::DataContentType::Data => {
                        bail!("Data file should not in task deletes");
                    }
                    iceberg::spec::DataContentType::EqualityDeletes => {
                        if equality_delete_files_set.insert(delete_file.file_path.clone()) {
                            if equality_delete_ids.is_none() {
                                equality_delete_ids = delete_file.equality_ids.clone();
                            } else if equality_delete_ids != delete_file.equality_ids {
                                bail!(
                                    "The schema of iceberg equality delete file must be consistent"
                                );
                            }
                            equality_delete_files.push(delete_file.to_file_scan_task(&task));
                        }
                    }
                    iceberg::spec::DataContentType::PositionDeletes => {
                        if position_delete_files_set.insert(delete_file.file_path.clone()) {
                            position_delete_files.push(delete_file.to_file_scan_task(&task));
                        }
                    }
                }
            }

            // Top-level scan tasks always represent data files. Keep their delete
            // descriptors intact so the SDK reader can apply them when requested.
            data_files.push(task);
        }
        let schema = table_schema.clone();
        let equality_delete_columns = equality_delete_ids
            .unwrap_or_default()
            .into_iter()
            .map(|id| match schema.name_by_field_id(id) {
                Some(name) => Ok::<std::string::String, ConnectorError>(name.to_owned()),
                None => bail!("Delete field id {} not found in schema", id),
            })
            .collect::<ConnectorResult<Vec<_>>>()?;

        Ok(IcebergListResult {
            data_files,
            equality_delete_files,
            position_delete_files,
            equality_delete_columns,
            format_version,
            schema,
        })
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
        IcebergScanTaskPlanner::split_n_vecs(file_scan_tasks, split_num)
    }
}

pub struct IcebergScanOpts {
    pub chunk_size: usize,
    pub need_seq_num: bool,
    pub need_file_path_and_pos: bool,
    pub handle_delete_files: bool,
}

/// Scan a data file. Delete files are handled by the iceberg-rust `reader.read` implementation.
#[try_stream(ok = DataChunk, error = ConnectorError)]
pub async fn scan_task_to_chunk_with_deletes(
    table: Table,
    mut data_file_scan_task: FileScanTask,
    IcebergScanOpts {
        chunk_size,
        need_seq_num,
        need_file_path_and_pos,
        handle_delete_files,
    }: IcebergScanOpts,
    metrics: Option<IcebergFileScanMetrics>,
) {
    let num_delete_files = data_file_scan_task.deletes.len();
    let expected_record_count = data_file_scan_task.record_count;
    let file_start = std::time::Instant::now();

    let read_metrics = metrics.clone();
    let mut read_bytes = scopeguard::guard(0u64, move |read_bytes| {
        if let Some(metrics) = read_metrics {
            metrics.record_read_bytes(read_bytes);
        }
    });

    let data_file_path = data_file_scan_task.data_file_path.clone();
    let data_sequence_number = data_file_scan_task.sequence_number;

    tracing::debug!(
        "scan_task_to_chunk_with_deletes: data_file={}, handle_delete_files={}, total_delete_files={}",
        data_file_path,
        handle_delete_files,
        data_file_scan_task.deletes.len()
    );

    if !handle_delete_files {
        // Keep the delete files from being applied when the caller opts out.
        data_file_scan_task.deletes.clear();
    }

    // Read the data file; delete application is delegated to the reader.
    let reader = table
        .reader_builder()
        .with_batch_size(chunk_size)
        .with_row_group_filtering_enabled(true)
        .build();
    let file_scan_stream = tokio_stream::once(Ok(data_file_scan_task.clone()));

    let mut record_batch_stream: iceberg::scan::ArrowRecordBatchStream =
        reader.read(Box::pin(file_scan_stream))?.stream();

    // The reader rejects a file with shredded variant columns before yielding any batch.
    // Retry without the variant columns; NULL columns are spliced back in below.
    let mut null_padded_variant_positions: Option<Vec<usize>> = None;
    let record_batch_stream: iceberg::scan::ArrowRecordBatchStream =
        match record_batch_stream.next().await {
            Some(Err(e)) if is_shredded_variant_rejection(&e) => {
                let (variant_field_ids, variant_positions, variant_names) =
                    projected_variant_columns(&data_file_scan_task);
                if variant_field_ids.is_empty() {
                    return Err(e.into());
                }
                tracing::warn!(
                    data_file_path,
                    columns = ?variant_names,
                    "shredded variant columns are not supported yet; reading them as NULL",
                );
                null_padded_variant_positions = Some(variant_positions);

                let mut reduced_task = data_file_scan_task;
                reduced_task
                    .project_field_ids
                    .retain(|id| !variant_field_ids.contains(id));
                let reader = table
                    .reader_builder()
                    .with_batch_size(chunk_size)
                    .with_row_group_filtering_enabled(true)
                    .build();
                reader
                    .read(Box::pin(tokio_stream::once(Ok(reduced_task))))?
                    .stream()
            }
            first => Box::pin(futures::stream::iter(first).chain(record_batch_stream)),
        };
    let mut record_batch_stream = record_batch_stream.enumerate();

    let mut total_rows_read: u64 = 0;

    // Process each record batch. Delete application is handled by the SDK.
    while let Some((batch_index, record_batch)) = record_batch_stream.next().await {
        let record_batch = record_batch?;
        let batch_start_pos = (batch_index * chunk_size) as i64;

        let mut chunk = IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;
        if let Some(positions) = &null_padded_variant_positions {
            chunk = pad_null_variant_columns(chunk, positions, record_batch.num_rows());
        }
        let row_count = chunk.capacity();
        total_rows_read += row_count as u64;

        // Add metadata columns if requested
        if need_seq_num {
            let (mut columns, visibility) = chunk.into_parts();
            columns.push(Arc::new(ArrayImpl::Int64(I64Array::from_iter(
                std::iter::repeat_n(data_sequence_number, row_count),
            ))));
            chunk = DataChunk::from_parts(columns.into(), visibility);
        }

        if need_file_path_and_pos {
            let (mut columns, visibility) = chunk.into_parts();
            columns.push(Arc::new(ArrayImpl::Utf8(Utf8Array::from_iter(
                std::iter::repeat_n(data_file_path.as_str(), row_count),
            ))));

            // Generate position values for each row in the batch
            let positions: Vec<i64> =
                (batch_start_pos..(batch_start_pos + row_count as i64)).collect();
            columns.push(Arc::new(ArrayImpl::Int64(I64Array::from_iter(positions))));

            chunk = DataChunk::from_parts(columns.into(), visibility);
        }

        *read_bytes += chunk.estimated_heap_size() as u64;
        yield chunk;
    }

    // Record per-file metrics after reading all batches.
    if let Some(metrics) = metrics {
        metrics.record_file_read_duration(file_start.elapsed().as_secs_f64());

        if total_rows_read > 0 {
            metrics.record_rows_read(total_rows_read);
        }

        metrics.record_file_read();

        // APPROXIMATE: Estimate delete rows applied. The delta between expected_record_count
        // and actual rows read may also include predicate pushdown / row-group pruning effects,
        // so this metric can overcount. It is still useful as an approximate signal for
        // detecting whether delete files cause significant row filtering.
        if handle_delete_files
            && num_delete_files > 0
            && let Some(expected) = expected_record_count
        {
            let deleted = expected.saturating_sub(total_rows_read);
            if deleted > 0 {
                metrics.record_delete_rows_applied(deleted);
            }
        }
    }
}

/// Whether the error is the reader's per-file rejection of shredded variant columns.
// `IcebergError` hides the inner error, so the raw one is needed to inspect kind/message.
#[expect(clippy::disallowed_types)]
fn is_shredded_variant_rejection(e: &iceberg::Error) -> bool {
    e.kind() == iceberg::ErrorKind::FeatureUnsupported && e.message().contains("shredded variant")
}

/// The projected VARIANT columns of a task: their field ids, their positions in the
/// projected column order, and their names.
fn projected_variant_columns(task: &FileScanTask) -> (Vec<i32>, Vec<usize>, Vec<String>) {
    let mut field_ids = Vec::new();
    let mut positions = Vec::new();
    let mut names = Vec::new();
    for (position, field_id) in task.project_field_ids.iter().enumerate() {
        if let Some(field) = task.schema.field_by_id(*field_id)
            && matches!(field.field_type.as_ref(), iceberg::spec::Type::Variant(_))
        {
            field_ids.push(*field_id);
            positions.push(position);
            names.push(field.name.clone());
        }
    }
    (field_ids, positions, names)
}

/// Insert all-NULL variant columns at the given projected positions.
fn pad_null_variant_columns(chunk: DataChunk, positions: &[usize], row_count: usize) -> DataChunk {
    let (mut columns, visibility) = chunk.into_parts();
    for &position in positions {
        let mut builder = VariantArrayBuilder::new(row_count);
        for _ in 0..row_count {
            builder.append_null();
        }
        columns.insert(position, Arc::new(ArrayImpl::Variant(builder.finish())));
    }
    DataChunk::from_parts(columns.into(), visibility)
}

#[derive(Debug)]
pub struct IcebergFileReader {}

#[async_trait]
impl SplitReader for IcebergFileReader {
    type Properties = IcebergProperties;
    type Split = IcebergSplit;

    async fn new(
        _props: IcebergProperties,
        _splits: Vec<IcebergSplit>,
        _parser_config: ParserConfig,
        _source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        unimplemented!()
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::scan::FileScanTask;
    use iceberg::spec::{
        FormatVersion, MAIN_BRANCH, NestedField, Operation, PrimitiveType, Schema, Snapshot,
        SortOrder, Struct, Summary, TableMetadataBuilder, Type, UnboundPartitionSpec,
    };

    use super::*;

    fn test_snapshot(
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        timestamp_ms: i64,
    ) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(snapshot_id)
            .with_timestamp_ms(timestamp_ms)
            .with_manifest_list(format!("/snap-{snapshot_id}.avro"))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(0)
            .build()
    }

    fn test_table_metadata_builder() -> TableMetadataBuilder {
        TableMetadataBuilder::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::new(1, "id", Type::Primitive(PrimitiveType::Long), false).into(),
                ])
                .build()
                .unwrap(),
            UnboundPartitionSpec::builder().build(),
            SortOrder::unsorted_order(),
            "s3://warehouse/db/table".to_owned(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
    }

    #[test]
    fn test_get_snapshot_id_uses_main_branch_history_for_timestamp() {
        let metadata = test_table_metadata_builder()
            .set_branch_snapshot(test_snapshot(1, None, 1_000), MAIN_BRANCH)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let metadata = metadata
            .into_builder(Some("s3://warehouse/db/table/v2.metadata.json".to_owned()))
            .set_branch_snapshot(test_snapshot(2, Some(1), 2_000), MAIN_BRANCH)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let metadata = metadata
            .into_builder(Some("s3://warehouse/db/table/v3.metadata.json".to_owned()))
            .set_branch_snapshot(test_snapshot(3, Some(1), 3_000), "audit")
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        assert_eq!(
            IcebergSplitEnumerator::get_snapshot_id_from_metadata(
                &metadata,
                Some(IcebergTimeTravelInfo::TimestampMs(3_500)),
            )
            .unwrap(),
            Some(2)
        );
        assert_eq!(
            IcebergSplitEnumerator::get_snapshot_id_from_metadata(
                &metadata,
                Some(IcebergTimeTravelInfo::TimestampMs(1_500)),
            )
            .unwrap(),
            Some(1)
        );
    }

    #[test]
    fn test_get_snapshot_id_version_without_current_snapshot() {
        let metadata = test_table_metadata_builder()
            .add_snapshot(test_snapshot(7, None, 1_000))
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        assert_eq!(metadata.current_snapshot_id(), None);
        assert_eq!(
            IcebergSplitEnumerator::get_snapshot_id_from_metadata(
                &metadata,
                Some(IcebergTimeTravelInfo::Version(7)),
            )
            .unwrap(),
            Some(7)
        );
        assert_eq!(
            IcebergSplitEnumerator::get_snapshot_id_from_metadata(&metadata, None).unwrap(),
            None
        );
    }

    fn create_file_scan_task(length: u64, id: u64) -> FileScanTask {
        FileScanTask {
            length,
            start: 0,
            record_count: Some(0),
            first_row_id: None,
            data_sequence_number: None,
            data_file_path: format!("test_{}.parquet", id),
            data_file_format: iceberg::spec::DataFileFormat::Parquet,
            schema: Arc::new(Schema::builder().build().unwrap()),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![],
            sequence_number: 0,
            file_sequence_number: Some(0),
            file_size_in_bytes: 0,
            partition: None,
            partition_spec: None,
            name_mapping: None,
            unified_partition_type: None,
            case_sensitive: true,
            key_metadata: None,
        }
    }

    #[test]
    fn test_split_serialization_strips_iceberg_planning_context() {
        let mut task = create_file_scan_task(100, 1);
        task.partition = Some(Struct::empty());
        task.partition_spec = Some(Arc::new(iceberg::spec::PartitionSpec::unpartition_spec()));

        let split = IcebergSplit {
            split_id: 1,
            task: IcebergFileScanTask::Data(vec![task]),
            limit: None,
        };
        let restored = IcebergSplit::restore_from_json(split.encode_to_json()).unwrap();
        let task = &restored.task.tasks()[0];

        assert!(task.partition.is_none());
        assert!(task.partition_spec.is_none());
        assert!(task.name_mapping.is_none());
        assert!(task.unified_partition_type.is_none());
    }

    #[test]
    fn test_split_n_vecs_basic() {
        let file_scan_tasks = (1..=12)
            .map(|i| create_file_scan_task(i + 100, i))
            .collect::<Vec<_>>(); // Ensure the correct function is called

        let groups = IcebergSplitEnumerator::split_n_vecs(file_scan_tasks, 3);

        assert_eq!(groups.len(), 3);

        let group_lengths: Vec<u64> = groups
            .iter()
            .map(|group| group.iter().map(|task| task.length).sum())
            .collect();

        let max_length = *group_lengths.iter().max().unwrap();
        let min_length = *group_lengths.iter().min().unwrap();
        assert!(max_length - min_length <= 10, "Groups should be balanced");

        let total_tasks: usize = groups.iter().map(|group| group.len()).sum();
        assert_eq!(total_tasks, 12);
    }

    #[test]
    fn test_split_n_vecs_empty() {
        let file_scan_tasks = Vec::new();
        let groups = IcebergSplitEnumerator::split_n_vecs(file_scan_tasks, 3);
        assert_eq!(groups.len(), 3);
        assert!(groups.iter().all(|group| group.is_empty()));
    }

    #[test]
    fn test_split_n_vecs_single_task() {
        let file_scan_tasks = vec![create_file_scan_task(100, 1)];
        let groups = IcebergSplitEnumerator::split_n_vecs(file_scan_tasks, 3);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups.iter().filter(|group| !group.is_empty()).count(), 1);
    }

    #[test]
    fn test_split_n_vecs_uneven_distribution() {
        let file_scan_tasks = vec![
            create_file_scan_task(1000, 1),
            create_file_scan_task(100, 2),
            create_file_scan_task(100, 3),
            create_file_scan_task(100, 4),
            create_file_scan_task(100, 5),
        ];

        let groups = IcebergSplitEnumerator::split_n_vecs(file_scan_tasks, 2);
        assert_eq!(groups.len(), 2);

        let group_with_large_task = groups
            .iter()
            .find(|group| group.iter().any(|task| task.length == 1000))
            .unwrap();
        assert_eq!(group_with_large_task.len(), 1);
    }

    #[test]
    fn test_split_n_vecs_same_files_distribution() {
        let file_scan_tasks = vec![
            create_file_scan_task(100, 1),
            create_file_scan_task(100, 2),
            create_file_scan_task(100, 3),
            create_file_scan_task(100, 4),
            create_file_scan_task(100, 5),
            create_file_scan_task(100, 6),
            create_file_scan_task(100, 7),
            create_file_scan_task(100, 8),
        ];

        let groups = IcebergSplitEnumerator::split_n_vecs(file_scan_tasks.clone(), 4)
            .iter()
            .map(|g| {
                g.iter()
                    .map(|task| task.data_file_path.clone())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        for _ in 0..10000 {
            let groups_2 = IcebergSplitEnumerator::split_n_vecs(file_scan_tasks.clone(), 4)
                .iter()
                .map(|g| {
                    g.iter()
                        .map(|task| task.data_file_path.clone())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            assert_eq!(groups, groups_2);
        }
    }
}
