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

use anyhow::anyhow;
use futures_async_stream::try_stream;
use iceberg::table::Table;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{Fields, JsonbVal, Timestamptz};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;

use crate::error::{ConnectorError, ConnectorResult};
use crate::source::iceberg::{IcebergSplitEnumerator, IcebergTimeTravelInfo};

/// The supported per-table Iceberg metadata relations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IcebergMetadataTableType {
    Snapshots,
    Manifests,
    Files,
}

impl IcebergMetadataTableType {
    pub fn from_suffix(suffix: &str) -> Option<Self> {
        match suffix {
            "snapshots" => Some(Self::Snapshots),
            "manifests" => Some(Self::Manifests),
            "files" => Some(Self::Files),
            _ => None,
        }
    }

    pub fn suffix(self) -> &'static str {
        match self {
            Self::Snapshots => "snapshots",
            Self::Manifests => "manifests",
            Self::Files => "files",
        }
    }

    pub fn schema(self) -> Schema {
        let fields = match self {
            Self::Snapshots => IcebergSnapshotRow::fields(),
            Self::Manifests => IcebergManifestRow::fields(),
            Self::Files => IcebergFileRow::fields(),
        }
        .into_iter()
        .map(|(name, data_type)| Field::with_name(data_type, name))
        .collect();
        Schema::new(fields)
    }
}

#[derive(Fields)]
struct IcebergSnapshotRow {
    committed_at: Timestamptz,
    snapshot_id: i64,
    parent_id: Option<i64>,
    sequence_number: i64,
    operation: String,
    manifest_list: String,
    summary: JsonbVal,
}

#[derive(Fields)]
struct IcebergManifestRow {
    content: String,
    path: String,
    length: i64,
    partition_spec_id: i32,
    sequence_number: i64,
    min_sequence_number: i64,
    added_snapshot_id: i64,
    added_files_count: Option<i32>,
    existing_files_count: Option<i32>,
    deleted_files_count: Option<i32>,
    added_rows_count: Option<i64>,
    existing_rows_count: Option<i64>,
    deleted_rows_count: Option<i64>,
    partition_summaries: Option<JsonbVal>,
    first_row_id: Option<i64>,
}

#[derive(Fields)]
struct IcebergFileRow {
    content: String,
    file_path: String,
    file_format: String,
    spec_id: i32,
    record_count: i64,
    file_size_in_bytes: i64,
    equality_ids: Option<Vec<i32>>,
    sort_order_id: Option<i32>,
    snapshot_id: Option<i64>,
    data_sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    manifest_path: String,
    referenced_data_file: Option<String>,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
}

fn to_i64(value: u64, field: &'static str) -> ConnectorResult<i64> {
    value
        .try_into()
        .map_err(|_| anyhow!("Iceberg {field} value {value} exceeds BIGINT").into())
}

fn optional_u64_to_i64(value: Option<u64>, field: &'static str) -> ConnectorResult<Option<i64>> {
    value.map(|value| to_i64(value, field)).transpose()
}

fn optional_u32_to_i32(value: Option<u32>, field: &'static str) -> ConnectorResult<Option<i32>> {
    value
        .map(|value| {
            value
                .try_into()
                .map_err(|_| anyhow!("Iceberg {field} value {value} exceeds INTEGER").into())
        })
        .transpose()
}

fn append_row(
    builder: &mut DataChunkBuilder,
    row: impl Fields,
) -> ConnectorResult<Option<DataChunk>> {
    Ok(builder.append_one_row(row.into_owned_row()))
}

/// Read one Iceberg metadata relation and emit bounded [`DataChunk`]s.
#[try_stream(ok = DataChunk, error = ConnectorError)]
pub async fn scan_iceberg_metadata(
    table: Table,
    metadata_type: IcebergMetadataTableType,
    time_travel_info: Option<IcebergTimeTravelInfo>,
    chunk_size: usize,
) {
    let mut builder = DataChunkBuilder::new(metadata_type.schema().data_types(), chunk_size);

    match metadata_type {
        IcebergMetadataTableType::Snapshots => {
            for snapshot in table.metadata().snapshots() {
                let committed_at = Timestamptz::from_millis(snapshot.timestamp_ms())
                    .ok_or_else(|| anyhow!("invalid Iceberg snapshot timestamp"))?;
                let summary =
                    serde_json::to_value(&snapshot.summary().additional_properties)?.into();
                let row = IcebergSnapshotRow {
                    committed_at,
                    snapshot_id: snapshot.snapshot_id(),
                    parent_id: snapshot.parent_snapshot_id(),
                    sequence_number: snapshot.sequence_number(),
                    operation: snapshot.summary().operation.as_str().to_owned(),
                    manifest_list: snapshot.manifest_list().to_owned(),
                    summary,
                };
                if let Some(chunk) = append_row(&mut builder, row)? {
                    yield chunk;
                }
            }
        }
        IcebergMetadataTableType::Manifests | IcebergMetadataTableType::Files => {
            let Some(snapshot_id) =
                IcebergSplitEnumerator::get_snapshot_id(&table, time_travel_info)?
            else {
                return Ok(());
            };
            let snapshot = table
                .metadata()
                .snapshot_by_id(snapshot_id)
                .ok_or_else(|| anyhow!("Iceberg snapshot {snapshot_id} not found"))?;
            let metadata = table.metadata_ref();
            let object_cache = table.object_cache();
            let manifest_list = object_cache.get_manifest_list(snapshot, &metadata).await?;

            if metadata_type == IcebergMetadataTableType::Manifests {
                for manifest in manifest_list.entries() {
                    let content = manifest.content.to_string();
                    let row = IcebergManifestRow {
                        content,
                        path: manifest.manifest_path.clone(),
                        length: manifest.manifest_length,
                        partition_spec_id: manifest.partition_spec_id,
                        sequence_number: manifest.sequence_number,
                        min_sequence_number: manifest.min_sequence_number,
                        added_snapshot_id: manifest.added_snapshot_id,
                        added_files_count: optional_u32_to_i32(
                            manifest.added_files_count,
                            "added_files_count",
                        )?,
                        existing_files_count: optional_u32_to_i32(
                            manifest.existing_files_count,
                            "existing_files_count",
                        )?,
                        deleted_files_count: optional_u32_to_i32(
                            manifest.deleted_files_count,
                            "deleted_files_count",
                        )?,
                        added_rows_count: optional_u64_to_i64(
                            manifest.added_rows_count,
                            "added_rows_count",
                        )?,
                        existing_rows_count: optional_u64_to_i64(
                            manifest.existing_rows_count,
                            "existing_rows_count",
                        )?,
                        deleted_rows_count: optional_u64_to_i64(
                            manifest.deleted_rows_count,
                            "deleted_rows_count",
                        )?,
                        partition_summaries: manifest
                            .partitions
                            .as_ref()
                            .map(serde_json::to_value)
                            .transpose()?
                            .map(Into::into),
                        first_row_id: optional_u64_to_i64(manifest.first_row_id, "first_row_id")?,
                    };
                    if let Some(chunk) = append_row(&mut builder, row)? {
                        yield chunk;
                    }
                }
            } else {
                for manifest_file in manifest_list.entries() {
                    let manifest_path = manifest_file.manifest_path.clone();
                    let manifest = manifest_file.load_manifest(table.file_io()).await?;
                    for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
                        let file = entry.data_file();
                        let content = format!("{:?}", file.content_type());
                        let row = IcebergFileRow {
                            content,
                            file_path: file.file_path().to_owned(),
                            file_format: file.file_format().to_string(),
                            spec_id: file.partition_spec_id(),
                            record_count: to_i64(file.record_count(), "record_count")?,
                            file_size_in_bytes: to_i64(
                                file.file_size_in_bytes(),
                                "file_size_in_bytes",
                            )?,
                            equality_ids: file.equality_ids(),
                            sort_order_id: file.sort_order_id(),
                            snapshot_id: entry.snapshot_id(),
                            data_sequence_number: entry.sequence_number(),
                            file_sequence_number: entry.file_sequence_number,
                            manifest_path: manifest_path.clone(),
                            referenced_data_file: file.referenced_data_file(),
                            content_offset: file.content_offset(),
                            content_size_in_bytes: file.content_size_in_bytes(),
                        };
                        if let Some(chunk) = append_row(&mut builder, row)? {
                            yield chunk;
                        }
                    }
                }
            }
        }
    }

    if let Some(chunk) = builder.consume_all() {
        yield chunk;
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;

    use super::*;

    #[test]
    fn test_metadata_table_suffix_and_schema() {
        assert_eq!(
            IcebergMetadataTableType::from_suffix("snapshots"),
            Some(IcebergMetadataTableType::Snapshots)
        );
        assert_eq!(IcebergMetadataTableType::from_suffix("entries"), None);
        assert_eq!(
            IcebergMetadataTableType::Files.schema().fields[0].data_type,
            DataType::Varchar
        );
        assert_eq!(
            IcebergMetadataTableType::Files.schema().fields[0].name,
            "content"
        );
    }
}
