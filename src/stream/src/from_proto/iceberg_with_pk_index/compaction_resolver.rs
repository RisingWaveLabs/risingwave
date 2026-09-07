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
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::secret::LocalSecretManager;
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_pb::id::SinkId;
use risingwave_pb::stream_plan::CompactionResolverNode;
use risingwave_storage::StateStore;

use crate::error::StreamResult;
use crate::executor::{CompactionResolverExecutor, Executor, StreamExecutorError};
use crate::from_proto::ExecutorBuilder;
use crate::task::ExecutorParams;

pub struct CompactionResolverExecutorBuilder;

impl_stream_node_body!(CompactionResolver(CompactionResolverNode) => CompactionResolverExecutorBuilder);

impl ExecutorBuilder for CompactionResolverExecutorBuilder {
    type Node = CompactionResolverNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        assert!(
            params.input.is_empty(),
            "compaction resolver executor should not have input"
        );

        let sink_desc = node.sink_desc.as_ref().unwrap();
        let sink_id: SinkId = sink_desc.get_id();

        let properties_with_secret = LocalSecretManager::global().fill_secrets(
            sink_desc.get_properties().clone(),
            sink_desc.get_secret_refs().clone(),
        )?;
        let iceberg_config = IcebergConfig::from_btreemap(properties_with_secret)
            .map_err(|err| StreamExecutorError::from((err, sink_id)))?;

        // Primary-key column indices within the iceberg data-file row. The writer writes every input
        // column to iceberg verbatim, so these indices (`SinkDesc.downstream_pk`) also index the
        // data-file columns.
        let pk_indices = sink_desc
            .downstream_pk
            .iter()
            .map(|&idx| idx as usize)
            .collect::<Vec<_>>();
        if pk_indices.is_empty() {
            return Err(anyhow!("missing downstream pk in iceberg sink desc").into());
        }

        // The pk-index state table schema is `[pk.., file_path, position]`, so the first
        // `pk_indices.len()` columns are the pk columns (in `downstream_pk` order). Derive the output
        // chunk's pk column data types from them so `Writer_B` consumes a schema identical to the
        // index key columns.
        let pk_index_table = node.get_pk_index_table()?;
        let pk_data_types = pk_index_table
            .columns
            .iter()
            .take(pk_indices.len())
            .map(|col| {
                let column_desc = col
                    .column_desc
                    .as_ref()
                    .ok_or_else(|| anyhow!("pk-index table column missing column_desc"))?;
                Ok::<_, anyhow::Error>(ColumnDesc::from(column_desc).data_type)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if pk_data_types.len() != pk_indices.len() {
            return Err(anyhow!(
                "pk-index table has {} columns but sink has {} pk columns",
                pk_index_table.columns.len(),
                pk_indices.len()
            )
            .into());
        }

        let barrier_receiver = params
            .local_barrier_manager
            .subscribe_barrier(params.actor_context.id);
        let local_barrier_manager = params.local_barrier_manager.clone();
        let meta_client = params.env.meta_client().ok_or_else(|| {
            anyhow!("meta client is required for iceberg pk-index compaction resolver")
        })?;

        let exec = CompactionResolverExecutor::new(
            params.actor_context,
            sink_id,
            node.compaction_task_id,
            iceberg_config,
            pk_indices,
            pk_data_types,
            node.output_data_file_paths.clone(),
            node.input_data_file_paths.clone(),
            node.read_snapshot_id,
            params.config.developer.chunk_size,
            local_barrier_manager,
            barrier_receiver,
            meta_client,
        );
        Ok((params.info, exec).into())
    }
}
