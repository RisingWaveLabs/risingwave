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

use std::collections::HashMap;
use std::future::IntoFuture;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, bail};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream, TryFutureExt};
use iceberg::io::{
    FileIOBuilder, FileMetadata, FileRead, S3_ACCESS_KEY_ID, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use itertools::Itertools;
use opendal::Operator;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::{Azblob, Gcs, S3};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask, parquet_to_arrow_schema};
use parquet::file::metadata::{FileMetaData, ParquetMetaData, ParquetMetaDataReader};
use prometheus::core::GenericCounter;
use risingwave_common::array::StreamChunk;
use risingwave_common::array::arrow::{IcebergArrowConvert, is_parquet_field_match_source_schema};
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::metrics::LabelGuardedMetric;
use risingwave_common::util::tokio_util::compat::FuturesAsyncReadCompatExt;
use url::Url;

use crate::error::ConnectorResult;
use crate::parser::ParquetParser;
use crate::source::{Column, SourceColumnDesc};

pub struct ParquetFileReader {
    meta: FileMetadata,
    r: Box<dyn FileRead>,
}

impl ParquetFileReader {
    pub fn new(meta: FileMetadata, r: Box<dyn FileRead>) -> Self {
        Self { meta, r }
    }
}

impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(
            self.r
                .read(range.start as _..range.end as _)
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err))),
        )
    }

    fn get_metadata(
        &mut self,
        _options: Option<&parquet::arrow::arrow_reader::ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let reader = ParquetMetaDataReader::new();
            let size = self.meta.size;
            let meta = reader.load_and_finish(self, size).await?;

            Ok(Arc::new(meta))
        }
        .boxed()
    }
}

pub async fn create_parquet_stream_builder(
    s3_region: String,
    s3_access_key: String,
    s3_secret_key: String,
    location: String,
) -> Result<ParquetRecordBatchStreamBuilder<ParquetFileReader>, anyhow::Error> {
    let mut props = HashMap::new();
    props.insert(S3_REGION, s3_region.clone());
    props.insert(S3_ACCESS_KEY_ID, s3_access_key.clone());
    props.insert(S3_SECRET_ACCESS_KEY, s3_secret_key.clone());

    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::s3()))
        .with_props(props)
        .build();
    let parquet_file = file_io.new_input(&location)?;

    let parquet_metadata = parquet_file.metadata().await?;
    let parquet_reader = parquet_file.reader().await?;
    let parquet_file_reader = ParquetFileReader::new(parquet_metadata, parquet_reader);

    ParquetRecordBatchStreamBuilder::new(parquet_file_reader)
        .await
        .map_err(Into::into)
}

pub fn new_s3_operator(
    s3_region: String,
    s3_access_key: String,
    s3_secret_key: String,
    bucket: String,
    s3_endpoint: String,
) -> ConnectorResult<Operator> {
    let mut builder = S3::default();
    builder = builder
        .region(&s3_region)
        .endpoint(&s3_endpoint)
        .access_key_id(&s3_access_key)
        .secret_access_key(&s3_secret_key)
        .bucket(&bucket)
        .disable_config_load();
    let op: Operator = Operator::new(builder)?
        .layer(LoggingLayer::default())
        .layer(RetryLayer::default());

    Ok(op)
}

pub fn new_gcs_operator(credential: String, bucket: String) -> ConnectorResult<Operator> {
    // Create gcs builder.
    let builder = Gcs::default().bucket(&bucket).credential(&credential);

    let operator: Operator = Operator::new(builder)?
        .layer(LoggingLayer::default())
        .layer(RetryLayer::default());
    Ok(operator)
}

pub fn new_azblob_operator(
    endpoint: String,
    account_name: String,
    account_key: String,
    container_name: String,
) -> ConnectorResult<Operator> {
    // Create azblob builder.
    let mut builder = Azblob::default();
    builder = builder
        .container(&container_name)
        .endpoint(&endpoint)
        .account_name(&account_name)
        .account_key(&account_key);

    let operator: Operator = Operator::new(builder)?
        .layer(LoggingLayer::default())
        .layer(RetryLayer::default());
    Ok(operator)
}

#[derive(Debug, Clone)]
pub enum FileScanBackend {
    S3,
    Gcs,
    Azblob,
}

pub fn extract_bucket_and_file_name(
    location: &str,
    file_scan_backend: &FileScanBackend,
) -> ConnectorResult<(String, String)> {
    let url = Url::parse(location)?;
    let bucket = url
        .host_str()
        .with_context(|| format!("Invalid url: {}, missing bucket", location))?
        .to_owned();
    let prefix = match file_scan_backend {
        FileScanBackend::S3 => format!("s3://{}/", bucket),
        FileScanBackend::Gcs => format!("gcs://{}/", bucket),
        FileScanBackend::Azblob => format!("azblob://{}/", bucket),
    };
    let file_name = location[prefix.len()..].to_string();
    Ok((bucket, file_name))
}

pub async fn list_data_directory(
    op: Operator,
    dir: String,
    file_scan_backend: &FileScanBackend,
) -> Result<Vec<String>, anyhow::Error> {
    let (bucket, file_name) = extract_bucket_and_file_name(&dir, file_scan_backend)?;
    let prefix = match file_scan_backend {
        FileScanBackend::S3 => format!("s3://{}/", bucket),
        FileScanBackend::Gcs => format!("gcs://{}/", bucket),
        FileScanBackend::Azblob => format!("azblob://{}/", bucket),
    };
    if dir.starts_with(&prefix) {
        op.list(&file_name).await.map_err(Into::into).map(|list| {
            list.into_iter()
                .map(|entry| prefix.clone() + entry.path())
                .collect()
        })
    } else {
        bail!("Invalid url: {}, should start with {}", dir, prefix)
    }
}

/// Extracts a suitable `ProjectionMask` from a Parquet file schema based on the user's requested schema.
///
/// This function is utilized for column pruning of Parquet files. It checks the user's requested schema
/// against the schema of the currently read Parquet file. If the provided `columns` are `None`
/// or if the Parquet file contains nested data types, it returns `ProjectionMask::all()`. Otherwise,
/// it returns only the columns where both the data type and column name match the requested schema,
/// facilitating efficient reading of the `RecordBatch`.
///
/// # Parameters
/// - `columns`: An optional vector of `Column` representing the user's requested schema.
/// - `metadata`: A reference to `FileMetaData` containing the schema and metadata of the Parquet file.
///
/// # Returns
/// - A `ConnectorResult<ProjectionMask>`, which represents the valid columns in the Parquet file schema
///   that correspond to the requested schema. If an error occurs during processing, it returns an
///   appropriate error.
pub fn get_project_mask(
    columns: Option<Vec<Column>>,
    metadata: &FileMetaData,
    case_insensitive: bool,
) -> ConnectorResult<ProjectionMask> {
    match columns {
        Some(rw_columns) => {
            let root_column_names = metadata
                .schema_descr()
                .root_schema()
                .get_fields()
                .iter()
                .map(|field| field.name())
                .collect_vec();

            let converted_arrow_schema =
                parquet_to_arrow_schema(metadata.schema_descr(), metadata.key_value_metadata())
                    .map_err(anyhow::Error::from)?;
            let mut lowercase_name_to_index: HashMap<String, Option<usize>> = HashMap::new();
            if case_insensitive {
                for (index, name) in root_column_names.iter().enumerate() {
                    let key = name.to_ascii_lowercase();
                    match lowercase_name_to_index.get(&key) {
                        Some(Some(_)) => {
                            lowercase_name_to_index.insert(key, None);
                        }
                        Some(None) => {}
                        None => {
                            lowercase_name_to_index.insert(key, Some(index));
                        }
                    }
                }
            }

            let valid_column_indices: Vec<usize> = rw_columns
                .iter()
                .filter_map(|column| {
                    let exact_pos = root_column_names
                        .iter()
                        .position(|&name| name == column.name);
                    let pos = match (case_insensitive, exact_pos) {
                        (_, Some(pos)) => Some(pos),
                        (true, None) => lowercase_name_to_index
                            .get(&column.name.to_ascii_lowercase())
                            .copied()
                            .flatten(),
                        _ => None,
                    }?;
                    let arrow_field = converted_arrow_schema.fields.get(pos)?;
                    let rw_data_type: &risingwave_common::types::DataType = &column.data_type;
                    if is_parquet_field_match_source_schema(arrow_field, rw_data_type) {
                        Some(pos)
                    } else {
                        // The parquet column exists but its type does not match the declared
                        // schema; it is NULL-filled rather than decoded to a diverging type.
                        tracing::warn!(
                            column = %column.name,
                            declared_type = %rw_data_type,
                            parquet_type = %arrow_field.data_type(),
                            parquet_extension = ?arrow_field.extension_type_name(),
                            "parquet column does not match the declared source schema; it will be NULL-filled",
                        );
                        None
                    }
                })
                .collect();

            Ok(ProjectionMask::roots(
                metadata.schema_descr(),
                valid_column_indices,
            ))
        }
        None => Ok(ProjectionMask::all()),
    }
}

/// Reads a specified Parquet file and converts its content into a stream of chunks.
pub async fn read_parquet_file(
    op: Operator,
    file_name: String,
    rw_columns: Option<Vec<Column>>,
    parser_columns: Option<Vec<SourceColumnDesc>>,
    case_insensitive: bool,
    batch_size: usize,
    offset: usize,
    file_source_input_row_count_metrics: Option<
        LabelGuardedMetric<GenericCounter<prometheus::core::AtomicU64>>,
    >,
    parquet_source_skip_row_count_metrics: Option<
        LabelGuardedMetric<GenericCounter<prometheus::core::AtomicU64>>,
    >,
) -> ConnectorResult<
    Pin<Box<dyn Stream<Item = Result<StreamChunk, crate::error::ConnectorError>> + Send>>,
> {
    let mut reader: tokio_util::compat::Compat<opendal::FuturesAsyncReader> = op
        .reader_with(&file_name)
        .into_future() // Unlike `rustc`, `try_stream` seems require manual `into_future`.
        .await?
        .into_futures_async_read(..)
        .await?
        .compat();
    let parquet_metadata = reader
        .get_metadata(None)
        .await
        .map_err(anyhow::Error::from)?;

    let file_metadata = parquet_metadata.file_metadata();
    {
        // Log parquet file-level metadata
        tracing::info!(
            "Reading parquet file: {}, from offset {}, num_row_groups={}, total_rows={}, kv_len={}",
            file_name,
            offset,
            parquet_metadata.row_groups().len(),
            file_metadata.num_rows(),
            file_metadata
                .key_value_metadata()
                .map(|m| m.len())
                .unwrap_or(0)
        );
        // Log each leaf column's path and types
        let schema_descr = file_metadata.schema_descr();
        for col in schema_descr.columns() {
            let path = col.path().string();
            let physical = col.physical_type();
            let logical = col.logical_type_ref();
            tracing::debug!(
                file = %file_name,
                column_path = path,
                physical_type = ?physical,
                logical_type = ?logical,
                type_length = ?col.type_length(),
                max_def_level = col.max_def_level(),
                max_rep_level = col.max_rep_level(),
                "Parquet file column schema: "
            );
        }
    }
    let projection_mask = get_project_mask(rw_columns, file_metadata, case_insensitive)?;

    // For the Parquet format, we directly convert from a record batch to a stream chunk.
    // Therefore, the offset of the Parquet file represents the current position in terms of the number of rows read from the file.
    let record_batch_stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .with_batch_size(batch_size)
        .with_projection(projection_mask)
        .with_offset(offset)
        .build()?;
    let converted_arrow_schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )?;
    let columns = match parser_columns {
        Some(columns) => columns,
        None => converted_arrow_schema
            .fields
            .iter()
            .enumerate()
            .map(|(index, field_ref)| {
                let data_type = IcebergArrowConvert
                    .type_from_field(field_ref)
                    .with_context(|| {
                        format!(
                            "cannot infer the RisingWave type of parquet column `{}` in file {}",
                            field_ref.name(),
                            file_name
                        )
                    })?;
                let column_desc = ColumnDesc::named(
                    field_ref.name().clone(),
                    ColumnId::new(index as i32),
                    data_type,
                );
                Ok(SourceColumnDesc::from(&column_desc))
            })
            .collect::<ConnectorResult<Vec<_>>>()?,
    };
    let parquet_parser = ParquetParser::new(columns, file_name, offset, case_insensitive)?;
    let msg_stream: Pin<
        Box<dyn Stream<Item = Result<StreamChunk, crate::error::ConnectorError>> + Send>,
    > = parquet_parser.into_stream(
        record_batch_stream,
        file_source_input_row_count_metrics,
        parquet_source_skip_row_count_metrics,
    );
    Ok(msg_stream)
}

pub async fn get_parquet_fields(
    op: Operator,
    file_name: String,
) -> ConnectorResult<risingwave_common::array::arrow::arrow_schema_iceberg::Fields> {
    let mut reader: tokio_util::compat::Compat<opendal::FuturesAsyncReader> = op
        .reader_with(&file_name)
        .into_future() // Unlike `rustc`, `try_stream` seems require manual `into_future`.
        .await?
        .into_futures_async_read(..)
        .await?
        .compat();
    let parquet_metadata = reader.get_metadata(None).await?;

    let file_metadata = parquet_metadata.file_metadata();
    let converted_arrow_schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )?;
    let fields: risingwave_common::array::arrow::arrow_schema_iceberg::Fields =
        converted_arrow_schema.fields;
    Ok(fields)
}
