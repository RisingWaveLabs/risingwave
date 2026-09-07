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

use risingwave_common::config::streaming::OverWindowCachePolicy;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_expr::window_function::WindowFuncCall;
use risingwave_pb::stream_plan::PbOverWindowNode;
use risingwave_storage::StateStore;

use super::ExecutorBuilder;
use crate::common::table::state_table::StateTableBuilder;
use crate::error::StreamResult;
use crate::executor::{Executor, OverWindowExecutor, OverWindowExecutorArgs};
use crate::task::ExecutorParams;

pub struct OverWindowExecutorBuilder;

impl_stream_node_body!(OverWindow(PbOverWindowNode) => OverWindowExecutorBuilder);

impl ExecutorBuilder for OverWindowExecutorBuilder {
    type Node = PbOverWindowNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let calls: Vec<_> = node
            .get_calls()
            .iter()
            .map(WindowFuncCall::from_protobuf)
            .try_collect()?;
        let partition_key_indices = node
            .get_partition_by()
            .iter()
            .map(|i| *i as usize)
            .collect();
        let (order_key_indices, order_key_order_types) = node
            .get_order_by()
            .iter()
            .map(ColumnOrder::from_protobuf)
            .map(|o| (o.column_index, o.order_type))
            .unzip();
        let vnodes = Some(Arc::new(
            params
                .vnode_bitmap
                .expect("vnodes not set for EOWC over window"),
        ));
        let state_table = StateTableBuilder::new(node.get_state_table()?, store, vnodes)
            .enable_preload_all_rows_by_config(&params.config)
            .build()
            .await;

        // Previously, the `cache_policy` is persisted in the plan node.
        // Now it's always `Unspecified` and we should refer to the job's config override.
        #[expect(deprecated)]
        let cache_policy = (node.get_cache_policy())
            .map_or(params.config.developer.over_window_cache_policy, |v| {
                OverWindowCachePolicy::from_protobuf(v)
            });

        let exec = OverWindowExecutor::new(OverWindowExecutorArgs {
            actor_ctx: params.actor_context,

            input,

            schema: params.info.schema.clone(),
            calls,
            partition_key_indices,
            order_key_indices,
            order_key_order_types,

            state_table,
            watermark_epoch: params.watermark_epoch,
            metrics: params.executor_stats,

            chunk_size: params.config.developer.chunk_size,
            cache_policy,
            enable_state_cleaning: node.enable_state_cleaning,
        });
        Ok((params.info, exec).into())
    }
}
