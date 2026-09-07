// Copyright 2025 RisingWave Labs
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

use risingwave_pb::stream_plan::LocalityProviderNode;
use risingwave_storage::StateStore;

use super::*;
use crate::common::table::state_table::{StateTableBuilder, StateTableOpConsistencyLevel};
use crate::executor::Executor;
use crate::executor::locality_provider::{LocalityProviderExecutor, SortBufferSettings};

impl_stream_node_body!(LocalityProvider(LocalityProviderNode) => LocalityProviderBuilder);

impl ExecutorBuilder for LocalityProviderBuilder {
    type Node = LocalityProviderNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let locality_columns = node
            .locality_columns
            .iter()
            .map(|&i| i as usize)
            .collect::<Vec<_>>();

        let input_schema = input.schema().clone();

        let vnodes = Some(Arc::new(
            params
                .vnode_bitmap
                .expect("vnodes not set for locality provider"),
        ));

        // Create state table for buffering input data
        let state_table = StateTableBuilder::new(
            node.get_state_table().unwrap(),
            store.clone(),
            vnodes.clone(),
        )
        .enable_preload_all_rows_by_config(&params.config)
        .build()
        .await;

        // Create progress table for tracking backfill progress
        let progress_table = StateTableBuilder::new(
            node.get_progress_table().unwrap(),
            store.clone(),
            vnodes.clone(),
        )
        .enable_preload_all_rows_by_config(&params.config)
        .build()
        .await;

        // Create the ephemeral sorted log table for the post-backfill sort buffer, if enabled at
        // plan time. Writes to it are append-only with unique keys, so consistency checks on old
        // values are unnecessary.
        let sort_buffer_table = match &node.sort_buffer_table {
            Some(table) => Some(
                StateTableBuilder::new(table, store, vnodes)
                    .with_op_consistency_level(StateTableOpConsistencyLevel::Inconsistent)
                    .forbid_preload_all_rows()
                    .build()
                    .await,
            ),
            None => None,
        };

        let progress = params
            .local_barrier_manager
            .register_create_mview_progress(&params.actor_context);

        let sort_buffer_settings = SortBufferSettings {
            enabled: params.config.developer.enable_locality_sort_buffer,
            activate_threshold: params
                .config
                .developer
                .locality_sort_buffer_activate_threshold,
        };

        let exec = LocalityProviderExecutor::new(
            input,
            locality_columns,
            state_table,
            progress_table,
            sort_buffer_table,
            sort_buffer_settings,
            input_schema,
            progress,
            params.executor_stats.clone(),
            params.config.developer.chunk_size,
            params.actor_context.fragment_id,
        );

        Ok((params.info, exec).into())
    }
}

pub struct LocalityProviderBuilder;
