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

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::BytesMut;
use chrono::{TimeZone, Utc};
use opendal::{FuturesAsyncWriter, Operator, Writer as OpendalWriter};
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::arrow::arrow_schema_iceberg::{self, SchemaRef};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_pb::id::ExecutorId;
use serde::Deserialize;
use serde_json::Value;
use serde_with::{DisplayFromStr, serde_as};
use strum_macros::{Display, EnumString};
use tokio_util::compat::{Compat, FuturesAsyncWriteCompatExt};
use uuid::Uuid;
use with_options::WithOptions;

use crate::enforce_secret::EnforceSecret;
use crate::sink::batching_log_sink::{BatchingLogSinker, BatchingSinkWriter};
use crate::sink::catalog::SinkEncode;
use crate::sink::encoder::{
    JsonEncoder, JsonbHandlingMode, RowEncoder, TimeHandlingMode, TimestampHandlingMode,
    TimestamptzHandlingMode,
};
use crate::sink::{
    Result, Sink, SinkDecouple, SinkError, SinkFormatDesc, SinkParam, UnknownFields,
};
use crate::source::TryFromBTreeMap;
use crate::with_options::WithOptions;

pub const DEFAULT_ROLLOVER_SECONDS: usize = 10;
pub const DEFAULT_MAX_ROW_COUNR: usize = 10240;

pub fn default_rollover_seconds() -> usize {
    DEFAULT_ROLLOVER_SECONDS
}
pub fn default_max_row_count() -> usize {
    DEFAULT_MAX_ROW_COUNR
}
/// The `FileSink` struct represents a file sink that uses the `OpendalSinkBackend` trait for its backend implementation.
///
/// # Type Parameters
///
/// - S: The type parameter S represents the concrete implementation of the `OpendalSinkBackend` trait used by this file sink.
#[derive(Debug, Clone)]
pub struct FileSink<S: OpendalSinkBackend> {
    pub(crate) op: Operator,
    /// The path to the file where the sink writes data.
    pub(crate) path: String,
    /// The schema describing the structure of the data being written to the file sink.
    pub(crate) schema: Schema,
    pub(crate) is_append_only: bool,
    /// The batching strategy for sinking data to files.
    pub(crate) batching_strategy: BatchingStrategy,

    /// The description of the sink's format.
    pub(crate) format_desc: SinkFormatDesc,
    pub(crate) engine_type: EngineType,
    pub(crate) unknown_fields: HashMap<String, String>,
    pub(crate) _marker: PhantomData<S>,
}

impl<S: OpendalSinkBackend> EnforceSecret for FileSink<S> {}

/// The `OpendalSinkBackend` trait unifies the behavior of various sink backends
/// implemented through `OpenDAL`(`<https://github.com/apache/opendal>`).
///
/// # Type Parameters
///
/// - Properties: Represents the necessary parameters for establishing a backend.
///
/// # Constants
///
/// - `SINK_NAME`: A static string representing the name of the sink.
///
/// # Functions
///
/// - `from_btreemap`: Automatically parse the required parameters from the input create sink statement.
/// - `new_operator`: Creates a new operator using the provided backend properties.
/// - `get_path`: Returns the path of the sink file specified by the user's create sink statement.
pub trait OpendalSinkBackend: Send + Sync + 'static + Clone + PartialEq {
    type Properties: TryFromBTreeMap + UnknownFields + Send + Sync + Clone + WithOptions;
    const SINK_NAME: &'static str;

    fn from_btreemap(btree_map: BTreeMap<String, String>) -> Result<Self::Properties>;
    fn new_operator(properties: Self::Properties) -> Result<Operator>;
    fn get_path(properties: Self::Properties) -> String;
    fn get_engine_type() -> EngineType;
    fn get_batching_strategy(properties: Self::Properties) -> BatchingStrategy;
}

#[derive(Clone, Debug)]
pub enum EngineType {
    Gcs,
    S3,
    Fs,
    Azblob,
    Webhdfs,
    Snowflake,
}

impl<S: OpendalSinkBackend> Sink for FileSink<S> {
    type LogSinker = BatchingLogSinker<OpenDalSinkWriter>;

    const SINK_NAME: &'static str = S::SINK_NAME;

    fn validate_unknown_fields(&self) -> Result<()> {
        crate::sink::validate_sink_unknown_fields(&self.unknown_fields)
    }

    /// A file is committed only once the batching strategy is met, so an untruncated barrier
    /// would block the checkpoint forever on the non-decoupled in-memory log store.
    fn is_sink_decouple(user_specified: &SinkDecouple) -> Result<bool> {
        match user_specified {
            SinkDecouple::Default | SinkDecouple::Enable => Ok(true),
            SinkDecouple::Disable => Err(SinkError::Config(anyhow!(
                "File sink can only be created with sink_decouple enabled. Please run `set sink_decouple = true` first."
            ))),
        }
    }

    async fn validate(&self) -> Result<()> {
        if matches!(self.engine_type, EngineType::Snowflake) {
            risingwave_common::license::Feature::SnowflakeSink
                .check_available()
                .map_err(|e| anyhow::anyhow!(e))?;
        }
        if !self.is_append_only {
            return Err(SinkError::Config(anyhow!(
                "File sink only supports append-only mode at present. \
                    Please change the query to append-only, and specify it \
                    explicitly after the `FORMAT ... ENCODE ...` statement. \
                    For example, `FORMAT xxx ENCODE xxx(force_append_only='true')`"
            )));
        }

        if self.format_desc.encode != SinkEncode::Parquet
            && self.format_desc.encode != SinkEncode::Json
        {
            return Err(SinkError::Config(anyhow!(
                "File sink only supports `PARQUET` and `JSON` encode at present."
            )));
        }

        match self.op.list(&self.path).await {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!(e).into()),
        }
    }

    async fn new_log_sinker(
        &self,
        writer_param: crate::sink::SinkWriterParam,
    ) -> Result<Self::LogSinker> {
        let writer = OpenDalSinkWriter::new(
            self.op.clone(),
            &self.path,
            self.schema.clone(),
            writer_param.executor_id,
            &self.format_desc,
            self.engine_type.clone(),
            self.batching_strategy.clone(),
        )?;
        Ok(BatchingLogSinker::new(writer))
    }
}

impl<S: OpendalSinkBackend> TryFrom<SinkParam> for FileSink<S> {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = S::from_btreemap(param.properties)?;
        let unknown_fields = crate::sink::UnknownFields::unknown_fields(&config);
        let path = S::get_path(config.clone());
        let op = S::new_operator(config.clone())?;
        let batching_strategy = S::get_batching_strategy(config);
        let engine_type = S::get_engine_type();
        let format_desc = match param.format_desc {
            Some(desc) => desc,
            None => {
                if let EngineType::Snowflake = engine_type {
                    SinkFormatDesc::plain_json_for_snowflake_only()
                } else {
                    return Err(SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")));
                }
            }
        };
        Ok(Self {
            op,
            path,
            schema,
            is_append_only: param.sink_type.is_append_only(),
            batching_strategy,
            format_desc,
            engine_type,
            unknown_fields,
            _marker: PhantomData,
        })
    }
}

pub struct OpenDalSinkWriter {
    schema: SchemaRef,
    operator: Operator,
    sink_writer: Option<FileWriterEnum>,
    write_path: String,
    executor_id: ExecutorId,
    unique_writer_id: Uuid,
    encode_type: SinkEncode,
    row_encoder: JsonEncoder,
    engine_type: EngineType,
    pub(crate) batching_strategy: BatchingStrategy,
    current_bached_row_num: usize,
    created_time: SystemTime,
    file_seq: u64,
}

/// The `FileWriterEnum` enum represents different types of file writers used for various sink
/// implementations.
///
/// # Variants
///
/// - `ParquetFileWriter`: Represents a Parquet file writer using the `AsyncArrowWriter<W>`
///   for writing data to a Parquet file. It accepts an implementation of W: `AsyncWrite` + `Unpin` + `Send`
///   as the underlying writer. In this case, the `OpendalWriter` serves as the underlying writer.
/// - `FileWriter`: Represents a basic `OpenDAL` writer, for writing files in encodes other than parquet.
///
/// The choice of writer used during the actual writing process depends on the encode type of the sink.
enum FileWriterEnum {
    ParquetFileWriter(AsyncArrowWriter<Compat<FuturesAsyncWriter>>),
    FileWriter(OpendalWriter),
}

#[async_trait]
impl BatchingSinkWriter for OpenDalSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.sink_writer.is_none() {
            assert_eq!(self.current_bached_row_num, 0);
            self.create_sink_writer().await?;
        };
        self.append_only(chunk).await?;
        Ok(())
    }

    async fn try_commit(&mut self) -> Result<bool> {
        if self.can_commit() {
            return self.commit().await;
        }
        Ok(false)
    }

    /// A barrier may also be truncated without a commit when no writer is active.
    async fn commit_on_barrier(&mut self) -> Result<bool> {
        Ok(self.try_commit().await? || self.sink_writer.is_none())
    }
}

/// Private methods related to batching.
impl OpenDalSinkWriter {
    fn can_commit(&self) -> bool {
        self.duration_seconds_since_writer_created() >= self.batching_strategy.rollover_seconds
            || self.current_bached_row_num >= self.batching_strategy.max_row_count
    }

    /// Closes the current writer, finishing the file. Returns `false` when no writer is active.
    async fn commit(&mut self) -> Result<bool> {
        if let Some(sink_writer) = self.sink_writer.take() {
            match sink_writer {
                FileWriterEnum::ParquetFileWriter(w) => {
                    let bytes_written = w.bytes_written();
                    if bytes_written > 0 {
                        w.close().await?;
                        tracing::debug!(
                            "writer {} (executor_id: {}, created_time: {}) finish write file, bytes_written: {}",
                            self.unique_writer_id,
                            self.executor_id,
                            self.created_time
                                .duration_since(UNIX_EPOCH)
                                .expect("Time went backwards")
                                .as_secs(),
                            bytes_written
                        );
                    }
                }
                FileWriterEnum::FileWriter(mut w) => {
                    w.close().await?;
                }
            };
            self.current_bached_row_num = 0;
            return Ok(true);
        }
        Ok(false)
    }

    fn path_partition_prefix(&self, duration: &Duration) -> String {
        let datetime = Utc
            .timestamp_opt(duration.as_secs() as i64, 0)
            .single()
            .expect("Failed to convert timestamp to DateTime<Utc>")
            .with_timezone(&Utc);
        let path_partition_prefix = self
            .batching_strategy
            .path_partition_prefix
            .as_ref()
            .unwrap_or(&PathPartitionPrefix::None);
        match path_partition_prefix {
            PathPartitionPrefix::None => "".to_owned(),
            PathPartitionPrefix::Day => datetime.format("%Y-%m-%d/").to_string(),
            PathPartitionPrefix::Month => datetime.format("/%Y-%m/").to_string(),
            PathPartitionPrefix::Hour => datetime.format("/%Y-%m-%d %H:00/").to_string(),
        }
    }

    fn duration_seconds_since_writer_created(&self) -> usize {
        let now = SystemTime::now();
        now.duration_since(self.created_time)
            .expect("Time went backwards")
            .as_secs() as usize
    }

    // Method for writing chunk and update related batching condition.
    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        match self
            .sink_writer
            .as_mut()
            .ok_or_else(|| SinkError::File("Sink writer is not created.".to_owned()))?
        {
            FileWriterEnum::ParquetFileWriter(w) => {
                let batch =
                    IcebergArrowConvert.to_record_batch(self.schema.clone(), chunk.data_chunk())?;
                let batch_row_nums = batch.num_rows();
                w.write(&batch).await?;
                self.current_bached_row_num += batch_row_nums;
            }
            FileWriterEnum::FileWriter(w) => {
                let mut chunk_buf = BytesMut::new();
                let batch_row_nums = chunk.data_chunk().capacity();
                // write the json representations of the row(s) in current chunk to `chunk_buf`
                for (op, row) in chunk.rows() {
                    assert_eq!(op, Op::Insert, "expect all `op(s)` to be `Op::Insert`");
                    // to prevent temporary string allocation,
                    // so we directly write to `chunk_buf` implicitly via `write_fmt`.
                    writeln!(
                        chunk_buf,
                        "{}",
                        Value::Object(self.row_encoder.encode(row)?)
                    )
                    .unwrap(); // write to a `BytesMut` should never fail
                }
                w.write(chunk_buf.freeze()).await?;
                self.current_bached_row_num += batch_row_nums;
            }
        }
        Ok(())
    }
}

/// Init methods.
impl OpenDalSinkWriter {
    pub fn new(
        operator: Operator,
        write_path: &str,
        rw_schema: Schema,
        executor_id: ExecutorId,
        format_desc: &SinkFormatDesc,
        engine_type: EngineType,
        batching_strategy: BatchingStrategy,
    ) -> Result<Self> {
        let arrow_schema = convert_rw_schema_to_arrow_schema(rw_schema.clone())?;
        let jsonb_handling_mode = JsonbHandlingMode::from_options(&format_desc.options)?;
        let row_encoder = JsonEncoder::new(
            rw_schema,
            None,
            crate::sink::encoder::DateHandlingMode::String,
            TimestampHandlingMode::String,
            TimestamptzHandlingMode::UtcString,
            TimeHandlingMode::String,
            jsonb_handling_mode,
        );
        Ok(Self {
            schema: Arc::new(arrow_schema),
            write_path: write_path.to_owned(),
            operator,
            sink_writer: None,
            executor_id,
            unique_writer_id: Uuid::now_v7(),
            encode_type: format_desc.encode.clone(),
            row_encoder,
            engine_type,
            batching_strategy,
            current_bached_row_num: 0,
            created_time: SystemTime::now(),
            file_seq: 0,
        })
    }

    async fn create_object_writer(&mut self) -> Result<OpendalWriter> {
        // Todo: specify more file suffixes based on encode_type.
        let suffix = match self.encode_type {
            SinkEncode::Parquet => "parquet",
            SinkEncode::Json => "json",
            _ => unimplemented!(),
        };

        let create_time = self
            .created_time
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        // With batching in place, the file writing process is decoupled from checkpoints.
        // The current file naming convention is as follows:
        // 1. A subdirectory is defined based on `path_partition_prefix` (e.g., by day、hour or month or none.).
        // 2. The file name includes a unique UUID (v7, which contains timestamp) and the creation time in seconds since the UNIX epoch.
        // If the engine type is `Fs`, the path is automatically handled, and the filename does not include a path prefix.
        // 3. For the Snowflake Sink, the `write_path` parameter can be empty.
        // When the `write_path` is not specified, the data will be written to the root of the specified bucket.
        let object_name = {
            let base_path = match self.engine_type {
                EngineType::Fs => "".to_owned(),
                EngineType::Snowflake if self.write_path.is_empty() => "".to_owned(),
                _ => format!("{}/", self.write_path),
            };
            let current_file_seq = self.file_seq;
            self.file_seq = self.file_seq.checked_add(1).expect("file seq overflow");

            format!(
                "{}{}{}_{}_{}.{}",
                base_path,
                self.path_partition_prefix(&create_time),
                self.unique_writer_id,
                create_time.as_secs(),
                current_file_seq,
                suffix,
            )
        };
        Ok(self
            .operator
            .writer_with(&object_name)
            .concurrent(8)
            .await?)
    }

    async fn create_sink_writer(&mut self) -> Result<()> {
        // Update the `created_time` to the current time when creating a new writer.
        self.created_time = SystemTime::now();

        let object_writer = self.create_object_writer().await?;
        match self.encode_type {
            SinkEncode::Parquet => {
                let props = WriterProperties::builder().set_compression(Compression::SNAPPY);
                let parquet_writer: tokio_util::compat::Compat<opendal::FuturesAsyncWriter> =
                    object_writer.into_futures_async_write().compat_write();
                self.sink_writer = Some(FileWriterEnum::ParquetFileWriter(
                    AsyncArrowWriter::try_new(
                        parquet_writer,
                        self.schema.clone(),
                        Some(props.build()),
                    )?,
                ));
            }
            _ => {
                self.sink_writer = Some(FileWriterEnum::FileWriter(object_writer));
            }
        }
        self.current_bached_row_num = 0;

        Ok(())
    }
}

fn convert_rw_schema_to_arrow_schema(
    rw_schema: risingwave_common::catalog::Schema,
) -> anyhow::Result<arrow_schema_iceberg::Schema> {
    let mut schema_fields = HashMap::new();
    rw_schema.fields.iter().for_each(|field| {
        let res = schema_fields.insert(&field.name, &field.data_type);
        // This assert is to make sure there is no duplicate field name in the schema.
        assert!(res.is_none())
    });
    let mut arrow_fields = vec![];
    for rw_field in &rw_schema.fields {
        let arrow_field = IcebergArrowConvert
            .to_arrow_field(&rw_field.name.clone(), &rw_field.data_type.clone())?;

        arrow_fields.push(arrow_field);
    }

    Ok(arrow_schema_iceberg::Schema::new(arrow_fields))
}

/// `BatchingStrategy` represents the strategy for batching data before writing to files.
///
/// This struct contains settings that control how data is collected and
/// partitioned based on specified criteria:
///
/// - `max_row_count`: Optional maximum number of rows to accumulate before writing.
/// - `rollover_seconds`: Optional time interval (in seconds) to trigger a write,
///   regardless of the number of accumulated rows.
/// - `path_partition_prefix`: Specifies how files are organized into directories
///   based on creation time (e.g., by day, month, or hour).

#[serde_as]
#[derive(Default, Deserialize, Debug, Clone, WithOptions)]
pub struct BatchingStrategy {
    #[serde(default = "default_max_row_count")]
    #[serde_as(as = "DisplayFromStr")]
    pub max_row_count: usize,
    #[serde(default = "default_rollover_seconds")]
    #[serde_as(as = "DisplayFromStr")]
    pub rollover_seconds: usize,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub path_partition_prefix: Option<PathPartitionPrefix>,
}

/// `PathPartitionPrefix` defines the granularity of file partitions based on creation time.
///
/// Each variant specifies how files are organized into directories:
/// - `None`: No partitioning.
/// - `Day`: Files are written in a directory for each day.
/// - `Month`: Files are written in a directory for each month.
/// - `Hour`: Files are written in a directory for each hour.
#[derive(Default, Debug, Clone, PartialEq, Display, Deserialize, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum PathPartitionPrefix {
    #[default]
    None = 0,
    #[serde(alias = "day")]
    Day = 1,
    #[serde(alias = "month")]
    Month = 2,
    #[serde(alias = "hour")]
    Hour = 3,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::file_sink::fs::FsSink;
    use crate::sink::file_sink::gcs::GcsSink;
    use crate::sink::file_sink::s3::{S3Sink, SnowflakeSink};

    #[test]
    fn test_requires_sink_decouple() {
        fn assert_requires_decouple<S: OpendalSinkBackend>() {
            assert!(FileSink::<S>::is_sink_decouple(&SinkDecouple::Default).unwrap());
            assert!(FileSink::<S>::is_sink_decouple(&SinkDecouple::Enable).unwrap());
            let err = FileSink::<S>::is_sink_decouple(&SinkDecouple::Disable).unwrap_err();
            assert!(
                err.to_string()
                    .contains("File sink can only be created with sink_decouple enabled")
            );
        }

        assert_requires_decouple::<FsSink>();
        assert_requires_decouple::<GcsSink>();
        assert_requires_decouple::<S3Sink>();
        assert_requires_decouple::<SnowflakeSink>();
    }
}
