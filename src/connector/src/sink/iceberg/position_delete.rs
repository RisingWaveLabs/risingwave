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

//! Shared iceberg position-delete (Puffin deletion vector) helpers.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use futures::StreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::delete_vector::DeleteVector;
use iceberg::io::FileIO;
use iceberg::puffin::{CompressionCodec, PuffinReader, PuffinWriter};
use iceberg::spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, PartitionKey};
use iceberg::table::Table;
use iceberg::writer::base_writer::position_delete_file_writer::POSITION_DELETE_SCHEMA;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, FileNameGenerator, LocationGenerator,
};
use iceberg::writer::file_writer::{
    FileWriter, FileWriterBuilder, ParquetWriter, ParquetWriterBuilder,
};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::properties::WriterProperties;
use risingwave_common::array::arrow::arrow_array_iceberg::{
    Array, ArrayRef, Int64Array, RecordBatch, StringArray,
};
use risingwave_common::array::arrow::arrow_schema_iceberg::SchemaRef as ArrowSchemaRef;

use crate::sink::iceberg::{IcebergConfig, PARQUET_CREATED_BY};
use crate::source::iceberg::parquet_file_handler::ParquetFileReader;

/// Puffin blob property for deletion vector cardinality.
const DELETION_VECTOR_PROPERTY_CARDINALITY: &str = "cardinality";
/// Puffin blob property for referenced data file path.
const DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE: &str = "referenced-data-file";

/// Reads the deletion-vector positions of a single Puffin DV `DataFile`.
pub async fn read_dv_positions_from_data_file(
    file_io: &FileIO,
    data_file: &DataFile,
) -> Result<DeleteVector> {
    let blob_offset = data_file.content_offset().with_context(|| {
        format!(
            "DV file {} missing content_offset for referenced data file {:?}",
            data_file.file_path(),
            data_file.referenced_data_file()
        )
    })?;
    let blob_length = data_file.content_size_in_bytes().with_context(|| {
        format!(
            "DV file {} missing content_size_in_bytes for referenced data file {:?}",
            data_file.file_path(),
            data_file.referenced_data_file()
        )
    })?;

    let input_file = file_io.new_input(data_file.file_path())?;
    let puffin_reader = PuffinReader::new(input_file).await?;
    let file_metadata = puffin_reader.file_metadata().await?;
    let blob_metadata = file_metadata
        .blobs()
        .iter()
        .find(|blob| blob.offset() == blob_offset as u64 && blob.length() == blob_length as u64)
        .with_context(|| {
            format!(
                "DV blob metadata not found in {} at offset={} length={}",
                data_file.file_path(),
                blob_offset,
                blob_length
            )
        })?;
    let blob = puffin_reader.blob(blob_metadata).await?;

    let delete_vector = DeleteVector::from_puffin_blob(blob)?;
    Ok(delete_vector)
}

/// Reads the positions stored in a V2 Parquet position-delete file into a [`DeleteVector`].
///
/// The file's schema is `(file_path, pos)`. Callers only invoke this after the entry's
/// `referenced_data_file` already matched the target data file, and the files we write
/// are file-scoped (every row shares one `file_path`), so the `file_path` column is
/// redundant here: we project only the `pos` column and read every value.
pub async fn read_parquet_position_deletes_from_file(
    file_io: &FileIO,
    delete_file: &DataFile,
) -> Result<DeleteVector> {
    let input_file = file_io.new_input(delete_file.file_path())?;
    let metadata = input_file.metadata().await?;
    let reader = input_file.reader().await?;
    let parquet_reader = ParquetFileReader::new(metadata, reader);
    let builder = ParquetRecordBatchStreamBuilder::new(parquet_reader).await?;
    // Project only the `pos` leaf (column index 1) so the `file_path` column is never decoded.
    let projection = ProjectionMask::leaves(builder.parquet_schema(), [1]);
    let mut stream = builder.with_projection(projection).build()?;

    let mut delete_vector = DeleteVector::default();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        // Only the projected `pos` column is present in the batch.
        let positions = batch.columns()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("position-delete pos column should be an Int64Array")?;
        for pos in positions {
            let pos = pos.with_context(|| {
                format!(
                    "null value in position-delete file {}",
                    delete_file.file_path()
                )
            })?;
            delete_vector.insert(pos as u64);
        }
    }

    Ok(delete_vector)
}

/// Reads the deleted positions of a single position-delete `DataFile` regardless of on-disk format,
pub async fn read_position_deletes_from_file(
    file_io: &FileIO,
    delete_file: &DataFile,
) -> Result<DeleteVector> {
    match delete_file.file_format() {
        DataFileFormat::Puffin => read_dv_positions_from_data_file(file_io, delete_file).await,
        DataFileFormat::Parquet => {
            read_parquet_position_deletes_from_file(file_io, delete_file).await
        }
        other => bail!(
            "position-delete file {} has unsupported format {:?}; expected Puffin or Parquet",
            delete_file.file_path(),
            other
        ),
    }
}

/// Writes `delete_vector` as a single Puffin deletion-vector blob referencing `data_file_path`,
/// and returns its [`DataFile`] metadata (content `PositionDeletes`, format `Puffin`) with
/// `referenced_data_file` set.
pub async fn write_dv_puffin_file(
    table: &Table,
    location_generator: &DefaultLocationGenerator,
    file_name_generator: &DefaultFileNameGenerator,
    data_file_path: String,
    delete_vector: &DeleteVector,
    partition_key: Option<&PartitionKey>,
) -> Result<DataFile> {
    let file_name = file_name_generator.generate_file_name();
    let location = location_generator.generate_location(partition_key, &file_name);
    let output_file = table.file_io().new_output(&location)?;
    let mut writer = PuffinWriter::new(&output_file, HashMap::new(), false).await?;

    let cardinality = delete_vector.len();
    let properties = HashMap::from([
        (
            DELETION_VECTOR_PROPERTY_CARDINALITY.to_owned(),
            cardinality.to_string(),
        ),
        (
            DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE.to_owned(),
            data_file_path.clone(),
        ),
    ]);
    let blob = delete_vector.to_puffin_blob(properties)?;
    writer.add(blob, CompressionCodec::None).await?;

    let result = writer.close_with_metadata().await?;
    let blob_metadata = result
        .blobs_metadata
        .first()
        .context("blob metadata should be present")?;

    let mut builder = DataFileBuilder::default();
    builder
        .content(DataContentType::PositionDeletes)
        .file_path(location)
        .file_format(DataFileFormat::Puffin)
        .record_count(cardinality)
        .file_size_in_bytes(result.file_size_in_bytes)
        .referenced_data_file(Some(data_file_path))
        .content_offset(Some(blob_metadata.offset() as i64))
        .content_size_in_bytes(Some(blob_metadata.length() as i64));
    if let Some(partition_key) = partition_key {
        builder
            .partition(partition_key.data().clone())
            .partition_spec_id(partition_key.spec().spec_id());
    }
    builder
        .build()
        .context("Failed to build deletion vector file metadata")
}

/// How many positions to buffer before flushing one `(file_path, pos)` batch to the writer.
const POSITION_DELETE_WRITE_CHUNK_SIZE: usize = 1024;

/// Writes `delete_vector` as a single file-scoped Parquet position-delete file referencing
/// `data_file_path`, and returns its [`DataFile`] metadata (content `PositionDeletes`, format
/// `Parquet`) with `referenced_data_file` set.
pub async fn write_parquet_position_delete_file(
    table: &Table,
    location_generator: &DefaultLocationGenerator,
    file_name_generator: &DefaultFileNameGenerator,
    config: &IcebergConfig,
    data_file_path: String,
    delete_vector: &DeleteVector,
    partition_key: Option<&PartitionKey>,
) -> Result<DataFile> {
    let file_name = file_name_generator.generate_file_name();
    let location = location_generator.generate_location(partition_key, &file_name);
    let output_file = table.file_io().new_output(&location)?;

    let parquet_writer_properties = WriterProperties::builder()
        .set_compression(config.get_parquet_compression())
        .set_max_row_group_bytes(config.write_parquet_max_row_group_bytes())
        .set_created_by(PARQUET_CREATED_BY.to_owned())
        .build();
    let mut writer = ParquetWriterBuilder::new(
        parquet_writer_properties,
        POSITION_DELETE_SCHEMA.clone().into(),
    )
    .build(output_file)
    .await?;

    // The position-delete schema is `(file_path, pos)` with reserved field IDs; derive the matching
    // Arrow schema so the written column field IDs line up.
    let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&POSITION_DELETE_SCHEMA)?);

    let mut positions: Vec<i64> = Vec::with_capacity(POSITION_DELETE_WRITE_CHUNK_SIZE);
    for pos in delete_vector.iter() {
        positions.push(pos as i64);
        if positions.len() == POSITION_DELETE_WRITE_CHUNK_SIZE {
            write_position_delete_chunk(
                &mut writer,
                &arrow_schema,
                &data_file_path,
                std::mem::take(&mut positions),
            )
            .await?;
            positions.reserve(POSITION_DELETE_WRITE_CHUNK_SIZE);
        }
    }
    if !positions.is_empty() {
        write_position_delete_chunk(&mut writer, &arrow_schema, &data_file_path, positions).await?;
    }

    let data_files = writer.close().await?;
    // `close` will yield exactly one builder here.
    let [mut builder] = data_files.try_into().map_err(|_| {
        anyhow!("position-delete writer produced invalid file count for {data_file_path}")
    })?;

    // `ParquetWriter` builds the file as `DataContentType::Data` with an empty partition; override
    // those for a file-scoped V2 position-delete file and attach `referenced_data_file`.
    builder
        .content(DataContentType::PositionDeletes)
        .referenced_data_file(Some(data_file_path));
    if let Some(partition_key) = partition_key {
        builder
            .partition(partition_key.data().clone())
            .partition_spec_id(partition_key.spec().spec_id());
    }
    builder
        .build()
        .context("Failed to build position-delete file metadata")
}

/// Writes one chunk of `positions` as a `(file_path, pos)` batch into `writer`. Every row shares
/// `data_file_path` because the delete file is file-scoped.
async fn write_position_delete_chunk(
    writer: &mut ParquetWriter,
    arrow_schema: &ArrowSchemaRef,
    data_file_path: &str,
    positions: Vec<i64>,
) -> Result<()> {
    let path_column: ArrayRef = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
        data_file_path,
        positions.len(),
    )));
    let pos_column: ArrayRef = Arc::new(Int64Array::from(positions));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![path_column, pos_column])
        .map_err(|e| anyhow!(e))?;
    writer.write(&batch).await?;
    Ok(())
}
