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

use std::sync::Arc;

use futures_async_stream::try_stream;
use prometheus::core::GenericCounter;
use risingwave_common::array::arrow::arrow_array_iceberg::{ArrayRef, RecordBatch};
use risingwave_common::array::arrow::arrow_schema_iceberg::FieldRef;
use risingwave_common::array::arrow::{IcebergArrowConvert, is_parquet_field_match_source_schema};
use risingwave_common::array::{ArrayBuilderImpl, DataChunk, StreamChunk};
use risingwave_common::metrics::LabelGuardedMetric;
use risingwave_common::types::{Datum, ScalarImpl};
use thiserror_ext::AsReport;

use crate::parser::ConnectorResult;
use crate::source::SourceColumnDesc;
/// `ParquetParser` is responsible for converting the incoming `record_batch_stream`
/// into a `streamChunk`.
#[derive(Debug)]
pub struct ParquetParser {
    rw_columns: Vec<SourceColumnDesc>,
    file_name: String,
    offset: usize,
    case_insensitive: bool,
}

impl ParquetParser {
    pub fn new(
        rw_columns: Vec<SourceColumnDesc>,
        file_name: String,
        offset: usize,
        case_insensitive: bool,
    ) -> ConnectorResult<Self> {
        Ok(Self {
            rw_columns,
            file_name,
            offset,
            case_insensitive,
        })
    }

    #[try_stream(boxed, ok = StreamChunk, error = crate::error::ConnectorError)]
    pub async fn into_stream(
        mut self,
        record_batch_stream: parquet::arrow::async_reader::ParquetRecordBatchStream<
            tokio_util::compat::Compat<opendal::FuturesAsyncReader>,
        >,
        file_source_input_row_count_metrics: Option<
            LabelGuardedMetric<GenericCounter<prometheus::core::AtomicU64>>,
        >,
        parquet_source_skip_row_count_metrics: Option<
            LabelGuardedMetric<GenericCounter<prometheus::core::AtomicU64>>,
        >,
    ) {
        #[for_await]
        for record_batch in record_batch_stream {
            let record_batch: RecordBatch = record_batch?;
            // Convert each record batch into a stream chunk according to user defined schema.
            let chunk: StreamChunk = self.convert_record_batch_to_stream_chunk(
                record_batch,
                file_source_input_row_count_metrics.clone(),
                parquet_source_skip_row_count_metrics.clone(),
            )?;

            yield chunk;
        }
    }

    fn inc_offset(&mut self) {
        self.offset += 1;
    }

    /// The function `convert_record_batch_to_stream_chunk` is designed to transform the given `RecordBatch` into a `StreamChunk`.
    ///
    /// For each column in the source column:
    /// - If the column's schema matches a column in the `RecordBatch` (both the data type and column name are the same),
    ///   the corresponding records are converted into a column of the `StreamChunk`.
    /// - If the column's schema does not match, null values are inserted.
    /// - Hidden columns are handled separately by filling in the appropriate fields to ensure the data chunk maintains the correct format.
    /// - If a column in the Parquet file does not exist in the source schema, it is skipped.
    ///
    /// # Arguments
    ///
    /// * `record_batch` - The `RecordBatch` to be converted into a `StreamChunk`.
    ///
    /// # Returns
    ///
    /// A `StreamChunk` containing the converted data from the `RecordBatch`.
    ///
    /// The hidden columns that must be included here are `_rw_file` and `_rw_offset`.
    /// Depending on whether the user specifies a primary key (pk), there may be an additional hidden column `row_id`.
    /// Therefore, the maximum number of hidden columns is three.
    fn convert_record_batch_to_stream_chunk(
        &mut self,
        record_batch: RecordBatch,
        file_source_input_row_count_metrics: Option<
            LabelGuardedMetric<GenericCounter<prometheus::core::AtomicU64>>,
        >,
        parquet_source_skip_row_count_metrics: Option<
            LabelGuardedMetric<GenericCounter<prometheus::core::AtomicU64>>,
        >,
    ) -> Result<StreamChunk, crate::error::ConnectorError> {
        const MAX_HIDDEN_COLUMN_NUMS: usize = 3;
        let column_size = self.rw_columns.len();
        let mut chunk_columns = Vec::with_capacity(self.rw_columns.len() + MAX_HIDDEN_COLUMN_NUMS);

        for source_column in self.rw_columns.clone() {
            match source_column.column_type {
                crate::source::SourceColumnType::Normal => {
                    let rw_data_type: &risingwave_common::types::DataType =
                        &source_column.data_type;
                    let rw_column_name = &source_column.name;
                    if let Some((parquet_field, parquet_column)) =
                        self.find_parquet_column(&record_batch, rw_column_name)
                        && is_parquet_field_match_source_schema(parquet_field, rw_data_type)
                    {
                        // The match guard above verified this column converts to the
                        // declared type; decode by the declared-side field, which for
                        // variant carries the extension name that `from_array` routes on.
                        let arrow_field = IcebergArrowConvert
                            .to_arrow_field(rw_column_name, rw_data_type)
                            .map_err(|e| {
                                crate::parser::AccessError::ParquetParser {
                                    message: format!(
                                        "to_arrow_field failed, column='{}', rw_type='{}', offset={}, error={}",
                                        rw_column_name, rw_data_type, self.offset, e.as_report()
                                    )
                                }
                            })?;
                        let array_impl = IcebergArrowConvert
                            .array_from_arrow_array(&arrow_field, parquet_column)
                            .map_err(|e| {
                                crate::parser::AccessError::ParquetParser {
                                    message: format!(
                                        "array_from_arrow_array failed, column='{}', rw_type='{}', arrow_field='{}', parquet_type='{}', offset={}, error={}",
                                        rw_column_name,
                                        rw_data_type,
                                        arrow_field.data_type(),
                                        parquet_column.data_type(),
                                        self.offset,
                                        e.as_report()
                                    )
                                }
                            })?;
                        // The schema match and the decode are maintained in parallel; a
                        // diverging output type is a code bug — surface it instead of letting
                        // a mistyped column corrupt the chunk downstream.
                        if array_impl.data_type() != *rw_data_type {
                            return Err(crate::parser::AccessError::ParquetParser {
                                message: format!(
                                    "converted array type diverges from the declared column type, column='{}', rw_type='{}', converted='{}', parquet_type='{}', offset={}",
                                    rw_column_name,
                                    rw_data_type,
                                    array_impl.data_type(),
                                    parquet_column.data_type(),
                                    self.offset,
                                ),
                            }
                            .into());
                        }
                        chunk_columns.push(Arc::new(array_impl));
                    } else {
                        // Handle additional columns, for file source, the additional columns are offset and file name;
                        // for columns defined in the user schema but not present in the parquet file, fill with null.
                        let column = if let Some(additional_column_type) =
                            &source_column.additional_column.column_type
                        {
                            match additional_column_type {
                                risingwave_pb::plan_common::additional_column::ColumnType::Offset(_) => {
                                    let mut array_builder = ArrayBuilderImpl::with_type(column_size, source_column.data_type.clone());
                                    for _ in 0..record_batch.num_rows() {
                                        let datum: Datum = Some(ScalarImpl::Utf8((self.offset).to_string().into()));
                                        self.inc_offset();
                                        array_builder.append(datum);
                                    }
                                    Arc::new(array_builder.finish())
                                }
                                risingwave_pb::plan_common::additional_column::ColumnType::Filename(_) => {
                                    let mut array_builder = ArrayBuilderImpl::with_type(column_size, source_column.data_type.clone());
                                    let datum: Datum = Some(ScalarImpl::Utf8(self.file_name.clone().into()));
                                    array_builder.append_n(record_batch.num_rows(), datum);
                                    Arc::new(array_builder.finish())
                                }
                                _ => unreachable!(),
                            }
                        } else {
                            // For columns defined in the source schema but not present in the Parquet file, null values are filled in.
                            let mut array_builder =
                                ArrayBuilderImpl::with_type(column_size, rw_data_type.clone());
                            array_builder.append_n_null(record_batch.num_rows());
                            if let Some(metrics) = parquet_source_skip_row_count_metrics.clone() {
                                metrics.inc_by(record_batch.num_rows() as u64);
                            }
                            Arc::new(array_builder.finish())
                        };
                        chunk_columns.push(column);
                    }
                }
                crate::source::SourceColumnType::RowId => {
                    let mut array_builder =
                        ArrayBuilderImpl::with_type(column_size, source_column.data_type.clone());
                    let datum: Datum = None;
                    array_builder.append_n(record_batch.num_rows(), datum);
                    let res = array_builder.finish();
                    let column = Arc::new(res);
                    chunk_columns.push(column);
                }
                // The following fields are only used in CDC source
                crate::source::SourceColumnType::Offset | crate::source::SourceColumnType::Meta => {
                    unreachable!()
                }
            }
        }
        if let Some(metrics) = file_source_input_row_count_metrics {
            metrics.inc_by(record_batch.num_rows() as u64);
        }

        let data_chunk = DataChunk::new(chunk_columns.clone(), record_batch.num_rows());
        Ok(data_chunk.into())
    }

    fn find_parquet_column<'a>(
        &self,
        record_batch: &'a RecordBatch,
        column_name: &str,
    ) -> Option<(&'a FieldRef, &'a ArrayRef)> {
        let fields = record_batch.schema_ref().fields();
        if let Some(index) = fields.iter().position(|field| field.name() == column_name) {
            return Some((&fields[index], record_batch.column(index)));
        }
        if !self.case_insensitive {
            return None;
        }
        let mut matched_index: Option<usize> = None;
        for (index, field) in fields.iter().enumerate() {
            if field.name().eq_ignore_ascii_case(column_name) {
                if matched_index.is_some() {
                    return None;
                }
                matched_index = Some(index);
            }
        }
        matched_index.map(|index| (&fields[index], record_batch.column(index)))
    }
}
