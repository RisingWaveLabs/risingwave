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

use std::sync::Arc;

use anyhow::anyhow;
use futures::future::try_join_all;
use risingwave_pb::stream_plan::{DispatcherType, MergeNode};

use super::*;
use crate::executor::exchange::input::new_input;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ActorContextRef, MergeExecutor, MergeExecutorInput, MergeExecutorUpstream};
use crate::task::LocalBarrierManager;

pub struct MergeExecutorBuilder;

impl MergeExecutorBuilder {
    pub(crate) async fn new_input(
        local_barrier_manager: LocalBarrierManager,
        executor_stats: Arc<StreamingMetrics>,
        actor_context: ActorContextRef,
        info: ExecutorInfo,
        node: &MergeNode,
        chunk_size: usize,
    ) -> StreamResult<Option<MergeExecutorInput>> {
        let upstream_fragment_id = node.get_upstream_fragment_id();
        let upstream_actors = actor_context
            .initial_upstream_actors
            .get(&upstream_fragment_id);
        if upstream_actors.is_none() && !node.allow_no_initial_upstream {
            return Ok(None);
        }

        let inputs: Vec<_> = try_join_all(
            upstream_actors
                .into_iter()
                .flat_map(|upstream_actors| upstream_actors.actors.iter())
                .map(|upstream_actor| {
                    new_input(
                        &local_barrier_manager,
                        executor_stats.clone(),
                        actor_context.id,
                        actor_context.fragment_id,
                        upstream_actor,
                        upstream_fragment_id,
                        actor_context.config.clone(),
                    )
                }),
        )
        .await?;

        // If there's always only one upstream, we can use `ReceiverExecutor`. Note that it can't
        // scale to multiple upstreams. An initially empty merge must stay dynamic to accept a
        // later MergeUpdate, even for dispatcher kinds normally optimized to a singleton.
        let always_single_input = !inputs.is_empty()
            && match node.get_upstream_dispatcher_type()? {
                DispatcherType::Unspecified => unreachable!(),
                DispatcherType::Hash | DispatcherType::Broadcast => false,
                // There could be arbitrary number of upstreams with simple dispatcher.
                DispatcherType::Simple => false,
                // There should be always only one upstream with no-shuffle dispatcher.
                DispatcherType::NoShuffle => true,
            };

        let upstreams = if always_single_input {
            MergeExecutorUpstream::Singleton(Itertools::exactly_one(inputs.into_iter()).unwrap())
        } else {
            MergeExecutorUpstream::Merge(MergeExecutor::new_merge_upstream(
                inputs,
                &executor_stats,
                &actor_context,
                chunk_size,
                info.schema.clone(),
            ))
        };

        Ok(Some(MergeExecutorInput::new(
            upstreams,
            actor_context,
            upstream_fragment_id,
            local_barrier_manager,
            executor_stats,
            info,
        )))
    }
}

impl_stream_node_body!(Merge(MergeNode) => MergeExecutorBuilder);

impl ExecutorBuilder for MergeExecutorBuilder {
    type Node = MergeNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        let actor_id = params.actor_context.id;
        let fragment_id = params.actor_context.fragment_id;
        let barrier_rx = params.local_barrier_manager.subscribe_barrier(actor_id);
        Ok(Self::new_input(
            params.local_barrier_manager,
            params.executor_stats,
            params.actor_context,
            params.info,
            node,
            params.config.developer.chunk_size,
        )
        .await?
        .ok_or_else(|| {
            anyhow!(
                "no upstream actors found for actor {} in fragment {}",
                actor_id,
                fragment_id
            )
        })?
        .into_executor(barrier_rx))
    }
}
