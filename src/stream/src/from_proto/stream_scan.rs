// Copyright 2023 RisingWave Labs
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
use risingwave_common::catalog::ColumnId;
use risingwave_common::util::value_encoding::BasicSerde;
use risingwave_common::util::value_encoding::column_aware_row_encoding::ColumnAwareSerde;
use risingwave_pb::plan_common::StorageTableDesc;
use risingwave_pb::stream_plan::{StreamScanNode, StreamScanType};
use risingwave_storage::table::batch_table::BatchTable;

use super::*;
use crate::common::table::state_table::{ReplicatedStateTable, StateTableBuilder};
use crate::executor::{
    ArrangementBackfillExecutor, BackfillExecutor, ChainExecutor, TroublemakerExecutor,
    UpstreamTableExecutor,
};

pub struct StreamScanExecutorBuilder;

impl_stream_node_body!(StreamScan(StreamScanNode) => StreamScanExecutorBuilder);

impl ExecutorBuilder for StreamScanExecutorBuilder {
    type Node = StreamScanNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        state_store: impl StateStore,
    ) -> StreamResult<Executor> {
        // For reporting the progress.
        let progress = params
            .local_barrier_manager
            .register_create_mview_progress(&params.actor_context);

        let output_indices = node
            .output_indices
            .iter()
            .map(|&i| i as usize)
            .collect_vec();

        #[expect(deprecated)]
        let exec = match node.stream_scan_type() {
            StreamScanType::Chain | StreamScanType::Rearrange | StreamScanType::UpstreamOnly => {
                let [upstream, _snapshot]: [_; 2] = params.input.try_into().unwrap();
                if node.stream_scan_type() != StreamScanType::UpstreamOnly {
                    tracing::warn!(
                        actor_id = %params.actor_context.id,
                        fragment_id = %params.fragment_id,
                        stream_scan_type = ?node.stream_scan_type(),
                        "ChainExecutor only supports UpstreamOnly; ignoring snapshot input"
                    );
                }
                ChainExecutor::new(upstream, progress).boxed()
            }
            StreamScanType::Backfill => {
                let [upstream, _]: [_; 2] = params.input.try_into().unwrap();
                let table_desc: &StorageTableDesc = node.get_table_desc()?;

                let column_ids = node
                    .upstream_column_ids
                    .iter()
                    .map(ColumnId::from)
                    .collect_vec();

                let vnodes = params.vnode_bitmap.map(Arc::new);

                let state_table = if let Ok(table) = node.get_state_table() {
                    Some(
                        StateTableBuilder::new(table, state_store.clone(), vnodes.clone())
                            .enable_preload_all_rows_by_config(&params.config)
                            .build()
                            .await,
                    )
                } else {
                    None
                };

                let upstream_table =
                    BatchTable::new_partial(state_store.clone(), column_ids, vnodes, table_desc);

                BackfillExecutor::new(
                    upstream_table,
                    upstream,
                    state_table,
                    output_indices,
                    progress,
                    params.fragment_id,
                )
                .boxed()
            }
            StreamScanType::ArrangementBackfill => {
                let [upstream, _]: [_; 2] = params.input.try_into().unwrap();
                let column_ids = node
                    .upstream_column_ids
                    .iter()
                    .map(ColumnId::from)
                    .collect_vec();

                let vnodes = params.vnode_bitmap.map(Arc::new);

                let state_table = node.get_state_table().unwrap();
                let state_table =
                    StateTableBuilder::new(state_table, state_store.clone(), vnodes.clone())
                        .enable_preload_all_rows_by_config(&params.config)
                        .build()
                        .await;

                let upstream_table = node.get_arrangement_table().unwrap();
                let versioned = upstream_table.get_version().is_ok();

                macro_rules! new_executor {
                    ($SD:ident) => {{
                        // TODO: can it be ConsistentOldValue?
                        let upstream_table = ReplicatedStateTable::new_replicated(
                            upstream_table,
                            state_store.clone(),
                            vnodes,
                            column_ids,
                        )
                        .await;
                        ArrangementBackfillExecutor::<_, $SD>::new(
                            upstream_table,
                            upstream,
                            state_table,
                            output_indices,
                            progress,
                            params.executor_stats.clone(),
                            params.config.developer.chunk_size,
                            node.rate_limit.into(),
                            params.fragment_id,
                        )
                        .boxed()
                    }};
                }
                if versioned {
                    new_executor!(ColumnAwareSerde)
                } else {
                    new_executor!(BasicSerde)
                }
            }
            StreamScanType::CrossDbSnapshotBackfill => {
                let table_desc: &StorageTableDesc = node.get_table_desc()?;
                assert!(params.input.is_empty());

                let output_indices = node
                    .output_indices
                    .iter()
                    .map(|&i| i as usize)
                    .collect_vec();

                let column_ids = node
                    .upstream_column_ids
                    .iter()
                    .map(ColumnId::from)
                    .collect_vec();

                let vnodes = params.vnode_bitmap.map(Arc::new);
                let barrier_rx = params
                    .local_barrier_manager
                    .subscribe_barrier(params.actor_context.id);

                let upstream_table = BatchTable::new_partial(
                    state_store.clone(),
                    column_ids,
                    vnodes.clone(),
                    table_desc,
                );

                let state_table = node.get_state_table()?;
                let state_table = StateTableBuilder::new(state_table, state_store.clone(), vnodes)
                    .enable_preload_all_rows_by_config(&params.config)
                    .build()
                    .await;

                let chunk_size = params.config.developer.chunk_size;
                let snapshot_epoch = node
                    .snapshot_backfill_epoch
                    .ok_or_else(|| anyhow!("snapshot epoch not set for {:?}", node))?;

                UpstreamTableExecutor::new(
                    upstream_table.table_id(),
                    upstream_table,
                    state_table,
                    snapshot_epoch,
                    output_indices,
                    chunk_size,
                    node.rate_limit.into(),
                    params.actor_context,
                    barrier_rx,
                    progress,
                )
                .boxed()
            }
            StreamScanType::SnapshotBackfill => {
                unreachable!(
                    "SnapshotBackfillExecutor is handled specially when in `StreamActorManager::create_nodes_inner`"
                )
            }
            StreamScanType::Unspecified => unreachable!(),
        };

        if crate::consistency::insane() {
            let mut info = params.info.clone();
            info.identity = format!("{} (troubled)", info.identity);
            Ok((
                params.info,
                TroublemakerExecutor::new((info, exec).into(), params.config.developer.chunk_size),
            )
                .into())
        } else {
            Ok((params.info, exec).into())
        }
    }
}
