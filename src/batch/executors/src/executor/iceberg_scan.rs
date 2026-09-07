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

use futures_async_stream::try_stream;
use futures_util::stream::StreamExt;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{
    Field, ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_SEQUENCE_NUM_COLUMN_NAME, Schema,
};
use risingwave_common::types::DataType;
use risingwave_connector::WithOptionsSecResolved;
use risingwave_connector::source::iceberg::{
    IcebergFileScanMetrics, IcebergFileScanTask, IcebergProperties, IcebergScanOpts, IcebergSplit,
    scan_task_to_chunk_with_deletes,
};
use risingwave_connector::source::{ConnectorProperties, SplitImpl, SplitMetaData};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{BoxedExecutor, BoxedExecutorBuilder, ExecutorBuilder};
use crate::error::BatchError;
use crate::executor::Executor;
use crate::monitor::BatchMetrics;

pub struct IcebergScanExecutor {
    iceberg_config: IcebergProperties,
    file_scan_tasks: Option<IcebergFileScanTask>,
    chunk_size: usize,
    schema: Schema,
    identity: String,
    file_scan_metrics: Option<IcebergFileScanMetrics>,
    need_seq_num: bool,
    need_file_path_and_pos: bool,
    limit: Option<u64>,
}

impl Executor for IcebergScanExecutor {
    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> super::BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

impl IcebergScanExecutor {
    pub fn new(
        iceberg_config: IcebergProperties,
        file_scan_tasks: IcebergFileScanTask,
        chunk_size: usize,
        schema: Schema,
        identity: String,
        metrics: Option<BatchMetrics>,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
        limit: Option<u64>,
    ) -> Self {
        let file_scan_metrics = metrics.as_ref().map(|metrics| {
            IcebergFileScanMetrics::new(
                &metrics.iceberg_scan_metrics(),
                iceberg_config.table.table_name(),
            )
        });
        Self {
            iceberg_config,
            chunk_size,
            schema,
            file_scan_tasks: Some(file_scan_tasks),
            identity,
            file_scan_metrics,
            need_seq_num,
            need_file_path_and_pos,
            limit,
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(mut self: Box<Self>) {
        let table = self.iceberg_config.load_table().await?;
        let data_types = self.schema.data_types();

        let data_file_scan_tasks = match Option::take(&mut self.file_scan_tasks) {
            Some(file_scan_tasks) => file_scan_tasks.into_tasks(),
            None => {
                bail!("file_scan_tasks must be Some")
            }
        };
        let mut remaining_limit = self
            .limit
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

        for data_file_scan_task in data_file_scan_tasks {
            if matches!(remaining_limit, Some(0)) {
                return Ok(());
            }

            #[for_await]
            for chunk in scan_task_to_chunk_with_deletes(
                table.clone(),
                data_file_scan_task,
                IcebergScanOpts {
                    chunk_size: self.chunk_size,
                    need_seq_num: self.need_seq_num,
                    need_file_path_and_pos: self.need_file_path_and_pos,
                    // Iceberg V2 scans expose delete files separately for delete-file scan nodes.
                    // From V3 onward, deletion vectors are attached to data-file tasks and must be
                    // applied by iceberg-rs during the data scan.
                    handle_delete_files: table.metadata().format_version()
                        >= iceberg::spec::FormatVersion::V3,
                },
                self.file_scan_metrics.clone(),
            ) {
                let chunk = chunk?;
                assert_eq!(chunk.data_types(), data_types);
                if let Some(remaining) = &mut remaining_limit {
                    if chunk.cardinality() > *remaining {
                        yield take_first_visible_rows(chunk, *remaining);
                        return Ok(());
                    }

                    *remaining -= chunk.cardinality();
                    yield chunk;

                    if *remaining == 0 {
                        return Ok(());
                    }
                } else {
                    yield chunk;
                }
            }
        }
    }
}

pub struct IcebergScanExecutorBuilder {}

impl BoxedExecutorBuilder for IcebergScanExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> crate::error::Result<BoxedExecutor> {
        ensure!(
            inputs.is_empty(),
            "Iceberg source should not have input executor!"
        );
        let source_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::IcebergScan
        )?;

        // prepare connector source
        let options_with_secret = WithOptionsSecResolved::new(
            source_node.with_properties.clone(),
            source_node.secret_refs.clone(),
        );
        let config = ConnectorProperties::extract(options_with_secret, false)?;

        let split_list = source_node
            .split
            .iter()
            .map(|split| SplitImpl::restore_from_bytes(split).unwrap())
            .collect_vec();
        assert_eq!(split_list.len(), 1);

        let fields = source_node
            .columns
            .iter()
            .map(|prost| {
                let column_desc = prost.column_desc.as_ref().unwrap();
                let data_type = DataType::from(column_desc.column_type.as_ref().unwrap());
                let name = column_desc.name.clone();
                Field::with_name(data_type, name)
            })
            .collect();
        let schema = Schema::new(fields);
        let metrics = source.context().batch_metrics();

        if let ConnectorProperties::Iceberg(iceberg_properties) = config
            && let SplitImpl::Iceberg(split) = &split_list[0]
        {
            let iceberg_properties: IcebergProperties = *iceberg_properties;
            let split: IcebergSplit = split.clone();
            let need_seq_num = schema
                .fields()
                .iter()
                .any(|f| f.name == ICEBERG_SEQUENCE_NUM_COLUMN_NAME);
            let need_file_path_and_pos = schema
                .fields()
                .iter()
                .any(|f| f.name == ICEBERG_FILE_PATH_COLUMN_NAME)
                && matches!(split.task, IcebergFileScanTask::Data(_));

            Ok(Box::new(IcebergScanExecutor::new(
                iceberg_properties,
                split.task,
                source.context().get_config().developer.chunk_size,
                schema,
                source.plan_node().get_identity().clone(),
                metrics,
                need_seq_num,
                need_file_path_and_pos,
                split.limit,
            )))
        } else {
            unreachable!()
        }
    }
}

fn take_first_visible_rows(chunk: DataChunk, limit: usize) -> DataChunk {
    if limit >= chunk.cardinality() {
        return chunk;
    }

    let indexes = chunk.visibility().iter_ones().take(limit).collect_vec();
    chunk.reorder_rows(&indexes)
}
