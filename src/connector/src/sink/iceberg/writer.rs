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

use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use iceberg::arrow::{
    RecordBatchPartitionSplitter, arrow_schema_to_schema, schema_to_arrow_schema,
};
use iceberg::spec::{
    DataFile, FormatVersion, PartitionSpecRef, Schema, SchemaRef as IcebergSchemaRef,
    SerializedDataFile, StructType,
};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::deletion_vector_writer::{
    DeletionVectorWriter, DeletionVectorWriterBuilder,
};
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::base_writer::position_delete_file_writer::{
    POSITION_DELETE_SCHEMA, PositionDeleteFileWriter, PositionDeleteFileWriterBuilder,
    PositionDeleteInput,
};
use iceberg::writer::delta_writer::{DELETE_OP, DeltaWriterBuilder, INSERT_OP};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::task_writer::TaskWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use itertools::Itertools;
use parquet::file::properties::WriterProperties;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::arrow::arrow_array_iceberg::{Int32Array, RecordBatch};
use risingwave_common::array::arrow::arrow_schema_iceberg::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef,
};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntCounter};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::connector_service::SinkMetadata;
use thiserror_ext::AsReport;
use uuid::Uuid;

use super::prometheus::monitored_general_writer::MonitoredGeneralWriterBuilder;
use super::{
    GLOBAL_SINK_METRICS, IcebergCommitResult, IcebergConfig, PARQUET_CREATED_BY, SinkError,
    SinkWriterParam, create_and_validate_table_impl,
};
use crate::sink::writer::SinkWriter;
use crate::sink::{Result, SinkParam};

/// `None` means no projection is needed.
/// `Prepare` stores the extra partition column index.
/// `Done` stores the finalized projection indices.
///
/// `ProjectIdxVec` is initialized lazily. When we first encounter `Prepare`, we
/// derive the projection indices from the current chunk schema and cache them in
/// `Done`.
enum ProjectIdxVec {
    None,
    Prepare(usize),
    Done(Vec<usize>),
}

type DataFileWriterBuilderType =
    DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;
/// The concrete V2 position-delete Parquet writer builder type, shared by the
/// iceberg sink writer and the pk-index position-delete merger.
pub type PositionDeleteFileWriterBuilderType = PositionDeleteFileWriterBuilder<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;
type PositionDeleteFileWriterType = PositionDeleteFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;
type DeletionVectorWriterBuilderType =
    DeletionVectorWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>;
type DeletionVectorWriterType =
    DeletionVectorWriter<DefaultLocationGenerator, DefaultFileNameGenerator>;
type EqualityDeleteFileWriterBuilderType = EqualityDeleteFileWriterBuilder<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

#[derive(Clone)]
enum PositionDeleteWriterBuilderType {
    PositionDelete(PositionDeleteFileWriterBuilderType),
    DeletionVector(DeletionVectorWriterBuilderType),
}

enum PositionDeleteWriterType {
    PositionDelete(PositionDeleteFileWriterType),
    DeletionVector(DeletionVectorWriterType),
}

#[async_trait]
impl IcebergWriterBuilder<Vec<PositionDeleteInput>> for PositionDeleteWriterBuilderType {
    type R = PositionDeleteWriterType;

    async fn build(
        &self,
        partition_key: Option<iceberg::spec::PartitionKey>,
    ) -> iceberg::Result<Self::R> {
        match self {
            PositionDeleteWriterBuilderType::PositionDelete(builder) => Ok(
                PositionDeleteWriterType::PositionDelete(builder.build(partition_key).await?),
            ),
            PositionDeleteWriterBuilderType::DeletionVector(builder) => Ok(
                PositionDeleteWriterType::DeletionVector(builder.build(partition_key).await?),
            ),
        }
    }
}

#[async_trait]
impl IcebergWriter<Vec<PositionDeleteInput>> for PositionDeleteWriterType {
    async fn write(&mut self, input: Vec<PositionDeleteInput>) -> iceberg::Result<()> {
        match self {
            PositionDeleteWriterType::PositionDelete(writer) => writer.write(input).await,
            PositionDeleteWriterType::DeletionVector(writer) => writer.write(input).await,
        }
    }

    async fn close(&mut self) -> iceberg::Result<Vec<DataFile>> {
        match self {
            PositionDeleteWriterType::PositionDelete(writer) => writer.close().await,
            PositionDeleteWriterType::DeletionVector(writer) => writer.close().await,
        }
    }
}

#[derive(Clone)]
struct SharedIcebergWriterBuilder<B>(Arc<B>);

#[async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for SharedIcebergWriterBuilder<B> {
    type R = B::R;

    async fn build(
        &self,
        partition_key: Option<iceberg::spec::PartitionKey>,
    ) -> iceberg::Result<Self::R> {
        self.0.build(partition_key).await
    }
}

#[derive(Clone)]
struct TaskWriterBuilderWrapper<B: IcebergWriterBuilder> {
    inner: Arc<B>,
    fanout_enabled: bool,
    schema: IcebergSchemaRef,
    partition_spec: PartitionSpecRef,
    compute_partition: bool,
}

impl<B: IcebergWriterBuilder> TaskWriterBuilderWrapper<B> {
    fn new(
        inner: B,
        fanout_enabled: bool,
        schema: IcebergSchemaRef,
        partition_spec: PartitionSpecRef,
        compute_partition: bool,
    ) -> Self {
        Self {
            inner: Arc::new(inner),
            fanout_enabled,
            schema,
            partition_spec,
            compute_partition,
        }
    }

    fn build(&self) -> iceberg::Result<TaskWriter<SharedIcebergWriterBuilder<B>>> {
        let partition_splitter = match (
            self.partition_spec.is_unpartitioned(),
            self.compute_partition,
        ) {
            (true, _) => None,
            (false, true) => Some(RecordBatchPartitionSplitter::try_new_with_computed_values(
                self.schema.clone(),
                self.partition_spec.clone(),
            )?),
            (false, false) => Some(
                RecordBatchPartitionSplitter::try_new_with_precomputed_values(
                    self.schema.clone(),
                    self.partition_spec.clone(),
                )?,
            ),
        };

        Ok(TaskWriter::new_with_partition_splitter(
            SharedIcebergWriterBuilder(self.inner.clone()),
            self.fanout_enabled,
            self.schema.clone(),
            self.partition_spec.clone(),
            partition_splitter,
        ))
    }
}

pub enum IcebergSinkWriter {
    Created(IcebergSinkWriterArgs),
    Initialized(IcebergSinkWriterInner),
}

pub struct IcebergSinkWriterArgs {
    config: IcebergConfig,
    sink_param: SinkParam,
    writer_param: SinkWriterParam,
    upsert_primary_key_column_names: Option<Vec<String>>,
}

pub struct IcebergSinkWriterInner {
    writer: IcebergWriterDispatch,
    arrow_schema: SchemaRef,
    // See comments below
    metrics: IcebergWriterMetrics,
    // State of iceberg table for this writer
    table: Table,
    actor_id: String,
    sink_id: String,
    sink_name: String,
    table_name: String,
    writer_mode: &'static str,
    // For chunk with extra partition column, we should remove this column before write.
    // This project index vec is used to avoid create project idx each time.
    project_idx_vec: ProjectIdxVec,
}

enum IcebergWriterDispatch {
    Append {
        writer: Option<Box<dyn IcebergWriter>>,
        writer_builder:
            TaskWriterBuilderWrapper<MonitoredGeneralWriterBuilder<DataFileWriterBuilderType>>,
    },
    Upsert {
        writer: Option<Box<dyn IcebergWriter>>,
        writer_builder: TaskWriterBuilderWrapper<
            MonitoredGeneralWriterBuilder<
                DeltaWriterBuilder<
                    DataFileWriterBuilderType,
                    PositionDeleteWriterBuilderType,
                    EqualityDeleteFileWriterBuilderType,
                >,
            >,
        >,
        arrow_schema_with_op_column: SchemaRef,
    },
}

impl IcebergWriterDispatch {
    pub fn get_writer(&mut self) -> Option<&mut Box<dyn IcebergWriter>> {
        match self {
            IcebergWriterDispatch::Append { writer, .. }
            | IcebergWriterDispatch::Upsert { writer, .. } => writer.as_mut(),
        }
    }
}

pub struct IcebergWriterMetrics {
    // NOTE: These 2 metrics are not used directly by us, but only kept for lifecycle management.
    // They are actually used in `PrometheusWriterBuilder`:
    //     WriterMetrics::new(write_qps.deref().clone(), write_latency.deref().clone())
    // We keep them here to let the guard cleans the labels from metrics registry when dropped
    _write_qps: LabelGuardedIntCounter,
    _write_latency: LabelGuardedHistogram,
    write_bytes: LabelGuardedIntCounter,
}

impl IcebergSinkWriter {
    pub fn new(
        config: IcebergConfig,
        sink_param: SinkParam,
        writer_param: SinkWriterParam,
        upsert_primary_key_column_names: Option<Vec<String>>,
    ) -> Self {
        Self::Created(IcebergSinkWriterArgs {
            config,
            sink_param,
            writer_param,
            upsert_primary_key_column_names,
        })
    }
}

pub(super) fn resolve_equality_delete_field_ids(
    primary_key_column_names: &[String],
    iceberg_schema: &iceberg::spec::Schema,
) -> Result<Vec<i32>> {
    primary_key_column_names
        .iter()
        .map(|column_name| {
            iceberg_schema.field_id_by_name(column_name).ok_or_else(|| {
                SinkError::Config(anyhow!(
                    "Primary key column {} not found in Iceberg schema",
                    column_name
                ))
            })
        })
        .collect()
}

impl IcebergSinkWriterInner {
    pub fn build_append_only(
        config: &IcebergConfig,
        table: Table,
        writer_param: &SinkWriterParam,
    ) -> Result<Self> {
        let SinkWriterParam {
            extra_partition_col_idx,
            actor_id,
            sink_id,
            sink_name,
            ..
        } = writer_param;
        let metrics_labels = [
            &actor_id.to_string(),
            &sink_id.to_string(),
            sink_name.as_str(),
        ];

        // Metrics
        let write_qps = GLOBAL_SINK_METRICS
            .iceberg_write_qps
            .with_guarded_label_values(&metrics_labels);
        let write_latency = GLOBAL_SINK_METRICS
            .iceberg_write_latency
            .with_guarded_label_values(&metrics_labels);
        // # TODO
        // Unused. Add this metrics later.
        let _rolling_unflushed_data_file = GLOBAL_SINK_METRICS
            .iceberg_rolling_unflushed_data_file
            .with_guarded_label_values(&metrics_labels);
        let write_bytes = GLOBAL_SINK_METRICS
            .iceberg_write_bytes
            .with_guarded_label_values(&metrics_labels);

        let schema = table.metadata().current_schema();
        let partition_spec = table.metadata().default_partition_spec();
        let fanout_enabled = !partition_spec.fields().is_empty();
        let table_name = table.identifier().to_string();
        // To avoid duplicate file name, each time the sink created will generate a unique uuid as file name suffix.
        let unique_uuid_suffix = Uuid::now_v7();
        tracing::info!(
            iceberg_component = "sink_writer",
            iceberg_operation = "build_writer",
            actor_id = %actor_id,
            sink_id = %sink_id,
            sink_name = %sink_name,
            table = %table_name,
            writer_mode = "append_only",
            schema_id = table.metadata().current_schema_id(),
            partition_spec_id = table.metadata().default_partition_spec_id(),
            format_version = ?table.metadata().format_version(),
            fanout_enabled,
            target_file_size_mb = config.target_file_size_mb(),
            parquet_compression = ?config.get_parquet_compression(),
            "iceberg_sink_writer_initialized",
        );

        let parquet_writer_properties = WriterProperties::builder()
            .set_compression(config.get_parquet_compression())
            .set_max_row_group_bytes(config.write_parquet_max_row_group_bytes())
            .set_created_by(PARQUET_CREATED_BY.to_owned())
            .build();

        let parquet_writer_builder =
            ParquetWriterBuilder::new(parquet_writer_properties, schema.clone());
        let rolling_builder = RollingFileWriterBuilder::new(
            parquet_writer_builder,
            (config.target_file_size_mb() * 1024 * 1024) as usize,
            table.file_io().clone(),
            DefaultLocationGenerator::new(table.metadata())
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
            DefaultFileNameGenerator::new(
                writer_param.actor_id.to_string(),
                Some(unique_uuid_suffix.to_string()),
                iceberg::spec::DataFileFormat::Parquet,
            ),
        );
        let data_file_builder = DataFileWriterBuilder::new(rolling_builder);
        let monitored_builder = MonitoredGeneralWriterBuilder::new(
            data_file_builder,
            write_qps.clone(),
            write_latency.clone(),
        );
        let writer_builder = TaskWriterBuilderWrapper::new(
            monitored_builder,
            fanout_enabled,
            schema.clone(),
            partition_spec.clone(),
            true,
        );
        let inner_writer = Some(Box::new(
            writer_builder
                .build()
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
        ) as Box<dyn IcebergWriter>);
        Ok(Self {
            arrow_schema: Arc::new(
                schema_to_arrow_schema(table.metadata().current_schema())
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
            ),
            metrics: IcebergWriterMetrics {
                _write_qps: write_qps,
                _write_latency: write_latency,
                write_bytes,
            },
            writer: IcebergWriterDispatch::Append {
                writer: inner_writer,
                writer_builder,
            },
            table,
            actor_id: actor_id.to_string(),
            sink_id: sink_id.to_string(),
            sink_name: sink_name.clone(),
            table_name,
            writer_mode: "append_only",
            project_idx_vec: {
                if let Some(extra_partition_col_idx) = extra_partition_col_idx {
                    ProjectIdxVec::Prepare(*extra_partition_col_idx)
                } else {
                    ProjectIdxVec::None
                }
            },
        })
    }

    pub fn build_upsert(
        config: &IcebergConfig,
        table: Table,
        primary_key_column_names: Vec<String>,
        writer_param: &SinkWriterParam,
    ) -> Result<Self> {
        let SinkWriterParam {
            extra_partition_col_idx,
            actor_id,
            sink_id,
            sink_name,
            ..
        } = writer_param;
        let metrics_labels = [
            &actor_id.to_string(),
            &sink_id.to_string(),
            sink_name.as_str(),
        ];

        // Metrics
        let write_qps = GLOBAL_SINK_METRICS
            .iceberg_write_qps
            .with_guarded_label_values(&metrics_labels);
        let write_latency = GLOBAL_SINK_METRICS
            .iceberg_write_latency
            .with_guarded_label_values(&metrics_labels);
        // # TODO
        // Unused. Add this metrics later.
        let _rolling_unflushed_data_file = GLOBAL_SINK_METRICS
            .iceberg_rolling_unflushed_data_file
            .with_guarded_label_values(&metrics_labels);
        let write_bytes = GLOBAL_SINK_METRICS
            .iceberg_write_bytes
            .with_guarded_label_values(&metrics_labels);

        // Determine the schema id and partition spec id
        let schema = table.metadata().current_schema();
        let unique_column_ids =
            resolve_equality_delete_field_ids(&primary_key_column_names, schema)?;
        let partition_spec = table.metadata().default_partition_spec();
        let fanout_enabled = !partition_spec.fields().is_empty();
        let use_deletion_vectors = table.metadata().format_version() >= FormatVersion::V3;
        let table_name = table.identifier().to_string();

        // To avoid duplicate file name, each time the sink created will generate a unique uuid as file name suffix.
        let unique_uuid_suffix = Uuid::now_v7();
        tracing::info!(
            iceberg_component = "sink_writer",
            iceberg_operation = "build_writer",
            actor_id = %actor_id,
            sink_id = %sink_id,
            sink_name = %sink_name,
            table = %table_name,
            writer_mode = "upsert",
            schema_id = table.metadata().current_schema_id(),
            partition_spec_id = table.metadata().default_partition_spec_id(),
            format_version = ?table.metadata().format_version(),
            primary_key_columns = ?primary_key_column_names,
            equality_delete_field_ids = ?unique_column_ids,
            use_deletion_vectors,
            fanout_enabled,
            target_file_size_mb = config.target_file_size_mb(),
            parquet_compression = ?config.get_parquet_compression(),
            "iceberg_sink_writer_initialized",
        );

        let parquet_writer_properties = WriterProperties::builder()
            .set_compression(config.get_parquet_compression())
            .set_max_row_group_bytes(config.write_parquet_max_row_group_bytes())
            .set_created_by(PARQUET_CREATED_BY.to_owned())
            .build();

        let data_file_builder = {
            let parquet_writer_builder =
                ParquetWriterBuilder::new(parquet_writer_properties.clone(), schema.clone());
            let rolling_writer_builder = RollingFileWriterBuilder::new(
                parquet_writer_builder,
                (config.target_file_size_mb() * 1024 * 1024) as usize,
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata())
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                DefaultFileNameGenerator::new(
                    writer_param.actor_id.to_string(),
                    Some(unique_uuid_suffix.to_string()),
                    iceberg::spec::DataFileFormat::Parquet,
                ),
            );
            DataFileWriterBuilder::new(rolling_writer_builder)
        };
        let position_delete_builder = if use_deletion_vectors {
            let location_generator = DefaultLocationGenerator::new(table.metadata())
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;
            PositionDeleteWriterBuilderType::DeletionVector(DeletionVectorWriterBuilder::new(
                table.file_io().clone(),
                location_generator,
                DefaultFileNameGenerator::new(
                    writer_param.actor_id.to_string(),
                    Some(format!("delvec-{}", unique_uuid_suffix)),
                    iceberg::spec::DataFileFormat::Puffin,
                ),
            ))
        } else {
            let parquet_writer_builder = ParquetWriterBuilder::new(
                parquet_writer_properties.clone(),
                POSITION_DELETE_SCHEMA.clone().into(),
            );
            let rolling_writer_builder = RollingFileWriterBuilder::new(
                parquet_writer_builder,
                (config.target_file_size_mb() * 1024 * 1024) as usize,
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata())
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                DefaultFileNameGenerator::new(
                    writer_param.actor_id.to_string(),
                    Some(format!("pos-del-{}", unique_uuid_suffix)),
                    iceberg::spec::DataFileFormat::Parquet,
                ),
            );
            PositionDeleteWriterBuilderType::PositionDelete(PositionDeleteFileWriterBuilder::new(
                rolling_writer_builder,
            ))
        };
        let equality_delete_builder = {
            let eq_del_config = EqualityDeleteWriterConfig::new(
                unique_column_ids.clone(),
                table.metadata().current_schema().clone(),
            )
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;
            let parquet_writer_builder = ParquetWriterBuilder::new(
                parquet_writer_properties,
                Arc::new(
                    arrow_schema_to_schema(eq_del_config.projected_arrow_schema_ref())
                        .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                ),
            );
            let rolling_writer_builder = RollingFileWriterBuilder::new(
                parquet_writer_builder,
                (config.target_file_size_mb() * 1024 * 1024) as usize,
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata())
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                DefaultFileNameGenerator::new(
                    writer_param.actor_id.to_string(),
                    Some(format!("eq-del-{}", unique_uuid_suffix)),
                    iceberg::spec::DataFileFormat::Parquet,
                ),
            );

            EqualityDeleteFileWriterBuilder::new(rolling_writer_builder, eq_del_config)
        };
        let delta_builder = DeltaWriterBuilder::new(
            data_file_builder,
            position_delete_builder,
            equality_delete_builder,
            unique_column_ids,
            schema.clone(),
        );
        let original_arrow_schema = Arc::new(
            schema_to_arrow_schema(table.metadata().current_schema())
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
        );
        let schema_with_extra_op_column = {
            let mut new_fields = original_arrow_schema.fields().iter().cloned().collect_vec();
            new_fields.push(Arc::new(ArrowField::new(
                "op".to_owned(),
                ArrowDataType::Int32,
                false,
            )));
            Arc::new(ArrowSchema::new(new_fields))
        };
        let writer_builder = TaskWriterBuilderWrapper::new(
            MonitoredGeneralWriterBuilder::new(
                delta_builder,
                write_qps.clone(),
                write_latency.clone(),
            ),
            fanout_enabled,
            schema.clone(),
            partition_spec.clone(),
            true,
        );
        let inner_writer = Some(Box::new(
            writer_builder
                .build()
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
        ) as Box<dyn IcebergWriter>);
        Ok(Self {
            arrow_schema: original_arrow_schema,
            metrics: IcebergWriterMetrics {
                _write_qps: write_qps,
                _write_latency: write_latency,
                write_bytes,
            },
            table,
            writer: IcebergWriterDispatch::Upsert {
                writer: inner_writer,
                writer_builder,
                arrow_schema_with_op_column: schema_with_extra_op_column,
            },
            project_idx_vec: {
                if let Some(extra_partition_col_idx) = extra_partition_col_idx {
                    ProjectIdxVec::Prepare(*extra_partition_col_idx)
                } else {
                    ProjectIdxVec::None
                }
            },
            actor_id: actor_id.to_string(),
            sink_id: sink_id.to_string(),
            sink_name: sink_name.clone(),
            table_name,
            writer_mode: "upsert",
        })
    }

    fn prepare_writer(&mut self) -> Result<()> {
        match &mut self.writer {
            IcebergWriterDispatch::Append {
                writer,
                writer_builder,
            } => {
                if writer.is_none() {
                    *writer = Some(Box::new(
                        writer_builder
                            .build()
                            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                    ));
                }
            }
            IcebergWriterDispatch::Upsert {
                writer,
                writer_builder,
                ..
            } => {
                if writer.is_none() {
                    *writer = Some(Box::new(
                        writer_builder
                            .build()
                            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                    ));
                }
            }
        };
        Ok(())
    }

    fn process_chunk(&mut self, chunk: StreamChunk) -> Result<Option<(RecordBatch, usize)>> {
        let (mut chunk, ops) = chunk.compact_vis().into_parts();
        match &mut self.project_idx_vec {
            ProjectIdxVec::None => {}
            ProjectIdxVec::Prepare(idx) => {
                if *idx >= chunk.columns().len() {
                    return Err(SinkError::Iceberg(anyhow!(
                        "invalid extra partition column index {}",
                        idx
                    )));
                }
                let project_idx_vec = (0..*idx)
                    .chain(*idx + 1..chunk.columns().len())
                    .collect_vec();
                chunk = chunk.project(&project_idx_vec);
                self.project_idx_vec = ProjectIdxVec::Done(project_idx_vec);
            }
            ProjectIdxVec::Done(idx_vec) => {
                chunk = chunk.project(idx_vec);
            }
        }
        if ops.is_empty() {
            return Ok(None);
        }
        let write_batch_size = chunk.estimated_heap_size();
        let batch = match &self.writer {
            IcebergWriterDispatch::Append { .. } => {
                // separate out insert chunk
                let filters =
                    chunk.visibility() & ops.iter().map(|op| *op == Op::Insert).collect::<Bitmap>();
                chunk.set_visibility(filters);
                IcebergArrowConvert
                    .to_record_batch(self.arrow_schema.clone(), &chunk.compact_vis())
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?
            }
            IcebergWriterDispatch::Upsert {
                arrow_schema_with_op_column,
                ..
            } => {
                let chunk = IcebergArrowConvert
                    .to_record_batch(self.arrow_schema.clone(), &chunk)
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;
                let ops = Arc::new(Int32Array::from(
                    ops.iter()
                        .map(|op| match op {
                            Op::UpdateInsert | Op::Insert => INSERT_OP,
                            Op::UpdateDelete | Op::Delete => DELETE_OP,
                        })
                        .collect_vec(),
                ));
                let mut columns = chunk.columns().to_vec();
                columns.push(ops);
                RecordBatch::try_new(arrow_schema_with_op_column.clone(), columns)
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?
            }
        };
        Ok(Some((batch, write_batch_size)))
    }

    pub async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        self.prepare_writer()?;
        let Some((batch, write_batch_size)) = self.process_chunk(chunk)? else {
            return Ok(());
        };

        let writer = self.writer.get_writer().unwrap();
        let batch_rows = batch.num_rows();
        let batch_columns = batch.num_columns();
        writer
            .write(batch)
            .instrument_await("iceberg_write")
            .await
            .inspect_err(|err| {
                tracing::error!(
                    iceberg_component = "sink_writer",
                    iceberg_operation = "write_batch",
                    actor_id = %self.actor_id,
                    sink_id = %self.sink_id,
                    sink_name = %self.sink_name,
                    table = %self.table_name,
                    writer_mode = self.writer_mode,
                    batch_rows,
                    batch_columns,
                    write_bytes = write_batch_size,
                    error = %err.as_report(),
                    "iceberg_sink_writer_write_failed",
                );
            })?;
        self.metrics.write_bytes.inc_by(write_batch_size as _);
        Ok(())
    }

    pub async fn write_batch_with_position(
        &mut self,
        chunk: StreamChunk,
    ) -> Result<Vec<PositionDeleteInput>> {
        debug_assert!(
            matches!(self.writer, IcebergWriterDispatch::Append { .. }),
            "write_batch_with_position should only be called for append-only writer"
        );

        self.prepare_writer()?;
        let Some((batch, write_batch_size)) = self.process_chunk(chunk)? else {
            return Ok(vec![]);
        };

        let writer = self.writer.get_writer().unwrap();
        let batch_rows = batch.num_rows();
        let batch_columns = batch.num_columns();
        let positions = writer
            .write_with_position(batch)
            .instrument_await("iceberg_write")
            .await
            .inspect_err(|err| {
                tracing::error!(
                    iceberg_component = "sink_writer",
                    iceberg_operation = "write_batch_with_position",
                    actor_id = %self.actor_id,
                    sink_id = %self.sink_id,
                    sink_name = %self.sink_name,
                    table = %self.table_name,
                    writer_mode = self.writer_mode,
                    batch_rows,
                    batch_columns,
                    write_bytes = write_batch_size,
                    error = %err.as_report(),
                    "iceberg_sink_writer_write_with_position_failed",
                );
            })?;
        self.metrics.write_bytes.inc_by(write_batch_size as _);
        Ok(positions)
    }

    pub async fn close(&mut self) -> Result<Option<Vec<DataFile>>> {
        let res = match &mut self.writer {
            IcebergWriterDispatch::Append {
                writer,
                writer_builder,
            } => {
                let close_result = match writer.take() {
                    Some(mut writer) => {
                        let data_files = writer.close().instrument_await("iceberg_close").await?;
                        Some(data_files)
                    }
                    _ => None,
                };
                match writer_builder.build() {
                    Ok(new_writer) => {
                        *writer = Some(Box::new(new_writer));
                    }
                    Err(err) => {
                        // In this case, the writer is closed and we can't build a new writer. But we can't return the error
                        // here because current writer may close successfully. So we just log the error.
                        tracing::warn!(
                            iceberg_component = "sink_writer",
                            iceberg_operation = "rebuild_writer",
                            actor_id = %self.actor_id,
                            sink_id = %self.sink_id,
                            sink_name = %self.sink_name,
                            table = %self.table_name,
                            writer_mode = self.writer_mode,
                            error = %err.as_report(),
                            "iceberg_sink_writer_rebuild_failed_after_close",
                        );
                    }
                }
                close_result
            }
            IcebergWriterDispatch::Upsert {
                writer,
                writer_builder,
                ..
            } => {
                let close_result = match writer.take() {
                    Some(mut writer) => {
                        let data_files = writer.close().instrument_await("iceberg_close").await?;
                        Some(data_files)
                    }
                    _ => None,
                };
                match writer_builder.build() {
                    Ok(new_writer) => {
                        *writer = Some(Box::new(new_writer));
                    }
                    Err(err) => {
                        // In this case, the writer is closed and we can't build a new writer. But we can't return the error
                        // here because current writer may close successfully. So we just log the error.
                        tracing::warn!(
                            iceberg_component = "sink_writer",
                            iceberg_operation = "rebuild_writer",
                            actor_id = %self.actor_id,
                            sink_id = %self.sink_id,
                            sink_name = %self.sink_name,
                            table = %self.table_name,
                            writer_mode = self.writer_mode,
                            error = %err.as_report(),
                            "iceberg_sink_writer_rebuild_failed_after_close",
                        );
                    }
                }
                close_result
            }
        };
        Ok(res)
    }

    pub fn generate_commit_metadata(&self, data_files: Vec<DataFile>) -> Result<SinkMetadata> {
        let serialized_data_files = serialize_data_files_default_spec(&self.table, data_files)?;
        SinkMetadata::try_from(&IcebergCommitResult {
            data_files: serialized_data_files,
            schema_id: self.table.metadata().current_schema_id(),
            partition_spec_id: self.table.metadata().default_partition_spec_id(),
        })
    }

    pub fn partition_type(&self) -> &iceberg::spec::StructType {
        self.table.metadata().default_partition_type()
    }
}

#[async_trait]
impl SinkWriter for IcebergSinkWriter {
    type CommitMetadata = Option<SinkMetadata>;

    /// Begin a new epoch
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        let Self::Created(args) = self else {
            return Ok(());
        };

        let table = create_and_validate_table_impl(&args.config, &args.sink_param).await?;
        let inner = match &args.upsert_primary_key_column_names {
            Some(primary_key_column_names) => IcebergSinkWriterInner::build_upsert(
                &args.config,
                table,
                primary_key_column_names.clone(),
                &args.writer_param,
            )?,
            None => {
                IcebergSinkWriterInner::build_append_only(&args.config, table, &args.writer_param)?
            }
        };

        *self = IcebergSinkWriter::Initialized(inner);
        Ok(())
    }

    /// Write a stream chunk to sink
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        let Self::Initialized(inner) = self else {
            unreachable!("IcebergSinkWriter should be initialized before barrier");
        };
        inner.write_batch(chunk).await
    }

    /// Receive a barrier and mark the end of current epoch. When `is_checkpoint` is true, the sink
    /// writer should commit the current epoch.
    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<SinkMetadata>> {
        let Self::Initialized(inner) = self else {
            unreachable!("IcebergSinkWriter should be initialized before barrier");
        };

        // Skip it if not checkpoint
        if !is_checkpoint {
            return Ok(None);
        }

        let data_files = inner
            .close()
            .await?
            .ok_or_else(|| anyhow!("No writer to close"))?;
        let res = inner.generate_commit_metadata(data_files)?;
        Ok(Some(res))
    }
}

/// Maximum size for column statistics (min/max values) in bytes.
/// Column statistics larger than this will be truncated to avoid metadata bloat.
/// This is especially important for large fields like JSONB, TEXT, BINARY, etc.
///
/// Fix for large column statistics in `DataFile` metadata that can cause OOM errors.
/// We truncate at the `DataFile` level (before serialization) by directly modifying
/// the public `lower_bounds` and `upper_bounds` fields.
///
/// This prevents metadata from ballooning to gigabytes when dealing with large
/// JSONB, TEXT, or BINARY fields, while still preserving statistics for small fields
/// that benefit from query optimization.
const MAX_COLUMN_STAT_SIZE: usize = 10240; // 10KB

/// Serializes `files` against the table's current default partition spec, after
/// truncating oversized column statistics. Shared by the iceberg sink writer's
/// commit-metadata generation and the pk-index merger's delete-file serialization.
///
/// Only the DEFAULT-spec path is shared here; callers that must serialize each
/// file against its own (potentially older) partition spec keep their own logic.
pub fn serialize_data_files_default_spec(
    table: &Table,
    files: Vec<DataFile>,
) -> Result<Vec<SerializedDataFile>> {
    let format_version = table.metadata().format_version();
    let partition_type = table.metadata().default_partition_type();
    files
        .into_iter()
        .map(|f| {
            // Truncate large column statistics BEFORE serialization.
            let truncated = truncate_datafile(f);
            Ok(SerializedDataFile::try_from(
                truncated,
                partition_type,
                format_version,
            )?)
        })
        .collect()
}

/// Resolves a partition spec by id from the table metadata, with a consolidated
/// error message. Shared spec-lookup mechanic for the pk-index merger, commit
/// coordinator, and sink commit paths.
pub fn resolve_partition_spec(table: &Table, spec_id: i32) -> Result<PartitionSpecRef> {
    table
        .metadata()
        .partition_spec_by_id(spec_id)
        .cloned()
        .ok_or_else(|| SinkError::Iceberg(anyhow!("partition spec {} not found", spec_id)))
}

/// Resolves the partition [`StructType`] for the given `spec_id` against `schema`.
///
/// `schema` is passed explicitly (rather than read from the table) so callers can
/// preserve their chosen schema, and `spec_id` is passed explicitly so callers can
/// preserve their chosen spec (e.g. a file's own spec vs. the default spec).
pub fn resolve_partition_type(table: &Table, spec_id: i32, schema: &Schema) -> Result<StructType> {
    resolve_partition_spec(table, spec_id)?
        .partition_type(schema)
        .map_err(|e| SinkError::Iceberg(anyhow!(e)))
}

/// Truncate large column statistics from `DataFile` BEFORE serialization.
///
/// This function directly modifies `DataFile`'s `lower_bounds` and `upper_bounds`
/// to remove entries that exceed `MAX_COLUMN_STAT_SIZE`.
///
/// # Arguments
/// * `data_file` - A `DataFile` to process
///
/// # Returns
/// The modified `DataFile` with large statistics truncated
pub fn truncate_datafile(mut data_file: DataFile) -> DataFile {
    // Process lower_bounds - remove entries with large values
    data_file.lower_bounds_mut().retain(|field_id, datum| {
        // Use to_bytes() to get the actual binary size without JSON serialization overhead
        let size = match datum.to_bytes() {
            Ok(bytes) => bytes.len(),
            Err(_) => 0,
        };

        if size > MAX_COLUMN_STAT_SIZE {
            tracing::debug!(
                field_id = field_id,
                size = size,
                "Truncating large lower_bound statistic"
            );
            return false;
        }
        true
    });

    // Process upper_bounds - remove entries with large values
    data_file.upper_bounds_mut().retain(|field_id, datum| {
        // Use to_bytes() to get the actual binary size without JSON serialization overhead
        let size = match datum.to_bytes() {
            Ok(bytes) => bytes.len(),
            Err(_) => 0,
        };

        if size > MAX_COLUMN_STAT_SIZE {
            tracing::debug!(
                field_id = field_id,
                size = size,
                "Truncating large upper_bound statistic"
            );
            return false;
        }
        true
    });

    data_file
}
