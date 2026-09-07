// Copyright 2022 RisingWave Labs
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

//! Build executor from protobuf.

macro_rules! impl_stream_node_body {
    ($variant:ident($node:ty) => $executor_builder:ty) => {
        paste::paste! {
            impl crate::from_proto::StreamNodeBody
                for risingwave_pb::stream_plan::stream_node::[<$variant Variant>]
            {
                type Node = $node;
                type ExecutorBuilder = $executor_builder;
            }
        }
    };
}

mod agg_common;
mod append_only_dedup;
mod asof_join;
mod barrier_recv;
mod batch_query;
mod cdc_filter;
mod changelog;
mod dml;
mod dynamic_filter;
mod eowc_gap_fill;
mod eowc_over_window;
mod expand;
mod filter;
mod gap_fill;
mod group_top_n;
mod hash_agg;
mod hash_join;
mod hop_window;
mod iceberg_with_pk_index;
mod locality_provider;
mod lookup;
mod lookup_union;
mod match_recognize;
mod materialized_exprs;
mod merge;
mod mview;
mod no_op;
mod now;
mod over_window;
mod project;
mod project_set;
mod row_id_gen;
mod simple_agg;
mod sink;
mod sort;
mod source;
mod source_backfill;
mod stateless_simple_agg;
mod stream_cdc_scan;
mod stream_scan;
mod temporal_join;
mod top_n;
mod union;
mod upstream_sink_union;
mod values;
mod watermark_filter;

mod row_merge;

mod approx_percentile;

mod sync_log_store;
mod vector_index_lookup_join;
mod vector_index_write;

// import for submodules
use itertools::Itertools;
use risingwave_common::dispatch_stream_node_body;
use risingwave_pb::stream_plan::{self, StreamNode, TemporalJoinNode};
use risingwave_storage::StateStore;

pub(crate) use self::merge::MergeExecutorBuilder;
use crate::error::StreamResult;
use crate::executor::{Execute, Executor, ExecutorInfo};
use crate::task::ExecutorParams;

trait ExecutorBuilder {
    type Node;

    /// Create an [`Executor`] from [`StreamNode`].
    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor>;
}

trait StreamNodeBody {
    type Node;
    type ExecutorBuilder: ExecutorBuilder<Node = Self::Node>;
}

struct UnreachableExecutorBuilder<T>(std::marker::PhantomData<T>);

impl<T> ExecutorBuilder for UnreachableExecutorBuilder<T> {
    type Node = T;

    async fn new_boxed_executor(
        _params: ExecutorParams,
        _node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        unreachable!()
    }
}

impl_stream_node_body!(
    Exchange(stream_plan::ExchangeNode) => UnreachableExecutorBuilder<stream_plan::ExchangeNode>
);
impl_stream_node_body!(
    DeltaIndexJoin(stream_plan::DeltaIndexJoinNode) => UnreachableExecutorBuilder<stream_plan::DeltaIndexJoinNode>
);

macro_rules! create_executor {
    ($params:expr, $node:expr, $store:expr) => {
        dispatch_stream_node_body!($node.get_node_body().unwrap(), NodeVariant, node_body => {
            <NodeVariant as StreamNodeBody>::ExecutorBuilder::new_boxed_executor(
                $params, node_body, $store,
            )
            .await
        })
    };
}

/// Create an executor from protobuf [`StreamNode`].
pub async fn create_executor(
    params: ExecutorParams,
    node: &StreamNode,
    store: impl StateStore,
) -> StreamResult<Executor> {
    create_executor!(params, node, store)
}
