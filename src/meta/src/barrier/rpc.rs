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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Debug, Formatter};
use std::future::poll_fn;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::anyhow;
use fail::fail_point;
use futures::future::{BoxFuture, join_all};
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{DatabaseId, FragmentTypeFlag, TableId};
use risingwave_common::id::JobId;
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::retry::exponential_backoff;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont;
use risingwave_common::util::tracing::TracingContext;
use risingwave_connector::source::SplitImpl;
use risingwave_meta_model::WorkerId;
use risingwave_pb::common::{HostAddress, WorkerNode};
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::id::PartialGraphId;
use risingwave_pb::source::{PbCdcTableSnapshotSplits, PbCdcTableSnapshotSplitsWithGeneration};
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    AddMutation, Barrier, BarrierMutation, IcebergPkIndexCompactionContext,
};
use risingwave_pb::stream_service::inject_barrier_request::build_actor_info::UpstreamActors;
use risingwave_pb::stream_service::inject_barrier_request::{
    BuildActorInfo, FragmentBuildActorInfo,
};
use risingwave_pb::stream_service::streaming_control_stream_request::{
    CreatePartialGraphRequest, PbCreatePartialGraphRequest, PbInitRequest,
    RemovePartialGraphRequest, ResetPartialGraphsRequest,
};
use risingwave_pb::stream_service::{
    InjectBarrierRequest, StreamingControlStreamRequest, streaming_control_stream_request,
    streaming_control_stream_response,
};
use risingwave_rpc_client::StreamingControlHandle;
use thiserror_ext::AsReport;
use tokio::time::{Instant, sleep};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::{BarrierKind, TracedEpoch};
use crate::barrier::BackfillOrderState;
use crate::barrier::backfill_order_control::get_nodes_with_backfill_dependencies;
use crate::barrier::cdc_progress::CdcTableBackfillTracker;
use crate::barrier::checkpoint::{
    BarrierWorkerState, BatchRefreshJobCheckpointControl, BatchRefreshRenderResult,
    CreatingStreamingJobControl, DatabaseCheckpointControl, DatabaseCheckpointControlMetrics,
    IndependentCheckpointJobControl,
};
use crate::barrier::context::{GlobalBarrierWorkerContext, GlobalBarrierWorkerContextImpl};
use crate::barrier::edge_builder::FragmentEdgeBuilder;
use crate::barrier::info::{
    BarrierInfo, CreateStreamingJobStatus, InflightDatabaseInfo, InflightStreamingJobInfo,
    SubscriberType,
};
use crate::barrier::partial_graph::PartialGraphRecoverer;
use crate::barrier::progress::CreateMviewProgressTracker;
use crate::barrier::utils::NodeToCollect;
use crate::controller::fragment::InflightFragmentInfo;
use crate::controller::utils::StreamingJobExtraInfo;
use crate::manager::MetaSrvEnv;
use crate::model::{
    ActorId, FragmentDownstreamRelation, FragmentId, StreamActor, StreamJobActorsToCreate,
    SubscriptionId,
};
use crate::stream::cdc::{
    CdcTableSnapshotSplits, is_parallelized_backfill_enabled_cdc_scan_fragment,
};
use crate::stream::{
    ExtendedFragmentBackfillOrder, StreamFragmentGraph, UserDefinedFragmentBackfillOrder,
    build_actor_connector_splits,
};
use crate::{MetaError, MetaResult};

pub(crate) fn to_partial_graph_id(
    database_id: DatabaseId,
    creating_job_id: Option<JobId>,
) -> PartialGraphId {
    let raw_job_id = creating_job_id
        .map(|job_id| {
            assert_ne!(job_id, u32::MAX);
            job_id.as_raw_id()
        })
        .unwrap_or(u32::MAX);
    (((database_id.as_raw_id() as u64) << 32) | (raw_job_id as u64)).into()
}

pub(super) fn from_partial_graph_id(
    partial_graph_id: PartialGraphId,
) -> (DatabaseId, Option<JobId>) {
    let id = partial_graph_id.as_raw_id();
    let database_id = (id >> 32) as u32;
    let raw_creating_job_id = (id & ((1 << 32) - 1)) as u32;
    let creating_job_id = if raw_creating_job_id == u32::MAX {
        None
    } else {
        Some(JobId::new(raw_creating_job_id))
    };
    (database_id.into(), creating_job_id)
}

pub(super) fn build_locality_fragment_state_table_mapping(
    fragment_infos: &HashMap<FragmentId, InflightFragmentInfo>,
) -> HashMap<FragmentId, Vec<TableId>> {
    let mut mapping = HashMap::new();

    for (fragment_id, fragment_info) in fragment_infos {
        let mut state_table_ids = Vec::new();
        visit_stream_node_cont(&fragment_info.nodes, |stream_node| {
            if let Some(NodeBody::LocalityProvider(locality_provider)) =
                stream_node.node_body.as_ref()
            {
                let state_table_id = locality_provider
                    .state_table
                    .as_ref()
                    .expect("must have state table")
                    .id;
                state_table_ids.push(state_table_id);
                false
            } else {
                true
            }
        });
        if !state_table_ids.is_empty() {
            mapping.insert(*fragment_id, state_table_ids);
        }
    }

    mapping
}

pub(super) fn database_partial_graphs<'a>(
    database_id: DatabaseId,
    creating_jobs: impl Iterator<Item = JobId> + Sized + 'a,
) -> impl Iterator<Item = PartialGraphId> + 'a {
    creating_jobs
        .map(Some)
        .chain([None])
        .map(move |creating_job_id| to_partial_graph_id(database_id, creating_job_id))
}

struct ControlStreamNode {
    worker_id: WorkerId,
    host: HostAddress,
    handle: StreamingControlHandle,
}

enum WorkerNodeState {
    Connected {
        control_stream: ControlStreamNode,
        removed: bool,
    },
    Reconnecting(BoxFuture<'static, StreamingControlHandle>),
}

pub(super) struct ControlStreamManager {
    workers: HashMap<WorkerId, (WorkerNode, WorkerNodeState)>,
    pub env: MetaSrvEnv,
}

impl ControlStreamManager {
    pub(super) fn new(env: MetaSrvEnv) -> Self {
        Self {
            workers: Default::default(),
            env,
        }
    }

    pub(super) fn host_addr(&self, worker_id: WorkerId) -> HostAddress {
        self.workers[&worker_id].0.host.clone().unwrap()
    }

    pub(super) async fn add_worker<'a>(
        &mut self,
        node: WorkerNode,
        partial_graphs: impl Iterator<Item = (PartialGraphId, &'a str)>,
        context: Arc<impl GlobalBarrierWorkerContext>,
    ) {
        let node_id = node.id;
        if let Entry::Occupied(entry) = self.workers.entry(node_id) {
            let (existing_node, worker_state) = entry.get();
            assert_eq!(existing_node.host, node.host);
            warn!(id = %node.id, host = ?node.host, "node already exists");
            match worker_state {
                WorkerNodeState::Connected { .. } => {
                    warn!(id = %node.id, host = ?node.host, "new node already connected");
                    return;
                }
                WorkerNodeState::Reconnecting(_) => {
                    warn!(id = %node.id, host = ?node.host, "remove previous pending worker connect request and reconnect");
                    entry.remove();
                }
            }
        }
        let node_host = node.host.clone().unwrap();
        let mut backoff =
            exponential_backoff(Duration::from_millis(100), 5, Duration::from_secs(3));
        const MAX_RETRY: usize = 5;
        for i in 1..=MAX_RETRY {
            match context.new_control_stream(&node).await {
                Ok(mut handle) => {
                    WorkerNodeConnected {
                        handle: &mut handle,
                        node: &node,
                    }
                    .initialize(partial_graphs);
                    info!(?node_host, "add control stream worker");
                    assert!(
                        self.workers
                            .insert(
                                node_id,
                                (
                                    node,
                                    WorkerNodeState::Connected {
                                        control_stream: ControlStreamNode {
                                            worker_id: node_id as _,
                                            host: node_host,
                                            handle,
                                        },
                                        removed: false
                                    }
                                )
                            )
                            .is_none()
                    );
                    return;
                }
                Err(e) => {
                    // It may happen that the dns information of newly registered worker node
                    // has not been propagated to the meta node and cause error. Wait for a while and retry
                    let delay = backoff.next().unwrap();
                    error!(
                        attempt = i,
                        backoff_delay = ?delay,
                        err = %e.as_report(),
                        ?node_host,
                        "failed to resolve the worker node address",
                    );
                    sleep(delay).await;
                }
            }
        }
        error!(?node_host, "failed to create the worker node after retries");
        assert!(
            self.workers
                .insert(
                    node_id,
                    (
                        node.clone(),
                        WorkerNodeState::Reconnecting(ControlStreamManager::retry_connect(
                            node, context,
                        ))
                    )
                )
                .is_none()
        );
    }

    pub(super) fn remove_worker(&mut self, node: WorkerNode) {
        if let Entry::Occupied(mut entry) = self.workers.entry(node.id) {
            let (_, worker_state) = entry.get_mut();
            match worker_state {
                WorkerNodeState::Connected { removed, .. } => {
                    info!(worker_id = %node.id, "mark connected worker as removed");
                    *removed = true;
                }
                WorkerNodeState::Reconnecting(_) => {
                    info!(worker_id = %node.id, "remove worker");
                    entry.remove();
                }
            }
        }
    }

    fn retry_connect(
        node: WorkerNode,
        context: Arc<impl GlobalBarrierWorkerContext>,
    ) -> BoxFuture<'static, StreamingControlHandle> {
        async move {
            let mut attempt = 0;
            let backoff = exponential_backoff(
                Duration::from_millis(100),
                5,
                Duration::from_mins(1),
            );
            for delay in backoff {
                attempt += 1;
                sleep(delay).await;
                match context.new_control_stream(&node).await {
                    Ok(handle) => {
                        return handle;
                    }
                    Err(e) => {
                        warn!(e = %e.as_report(), ?node, attempt, "failed to create the control stream worker");
                    }
                }
            }
            unreachable!("end of retry backoff")
        }.boxed()
    }

    pub(super) async fn recover(
        env: MetaSrvEnv,
        nodes: &HashMap<WorkerId, WorkerNode>,
        context: Arc<impl GlobalBarrierWorkerContext>,
    ) -> Self {
        let reset_start_time = Instant::now();
        let nodes = join_all(nodes.iter().map(|(worker_id, node)| async {
            let result = context.new_control_stream(node).await;
            (*worker_id, node.clone(), result)
        }))
        .await;
        let mut unconnected_workers = HashSet::new();
        let mut workers = HashMap::new();
        for (worker_id, node, result) in nodes {
            match result {
                Ok(handle) => {
                    let control_stream = ControlStreamNode {
                        worker_id: node.id,
                        host: node.host.clone().unwrap(),
                        handle,
                    };
                    assert!(
                        workers
                            .insert(
                                worker_id,
                                (
                                    node,
                                    WorkerNodeState::Connected {
                                        control_stream,
                                        removed: false
                                    }
                                )
                            )
                            .is_none()
                    );
                }
                Err(e) => {
                    unconnected_workers.insert(worker_id);
                    warn!(
                        e = %e.as_report(),
                        %worker_id,
                        ?node,
                        "failed to connect to node"
                    );
                    assert!(
                        workers
                            .insert(
                                worker_id,
                                (
                                    node.clone(),
                                    WorkerNodeState::Reconnecting(Self::retry_connect(
                                        node,
                                        context.clone()
                                    ))
                                )
                            )
                            .is_none()
                    );
                }
            }
        }

        info!(elapsed=?reset_start_time.elapsed(), ?unconnected_workers, "control stream reset");

        Self { workers, env }
    }

    /// Clear all nodes and response streams in the manager.
    pub(super) fn clear(&mut self) {
        *self = Self::new(self.env.clone());
    }
}

pub(super) struct WorkerNodeConnected<'a> {
    node: &'a WorkerNode,
    handle: &'a mut StreamingControlHandle,
}

impl<'a> WorkerNodeConnected<'a> {
    pub(super) fn initialize<'b>(
        self,
        partial_graphs: impl Iterator<Item = (PartialGraphId, &'b str)>,
    ) {
        for (partial_graph_id, term_id) in partial_graphs {
            if let Err(e) = self.handle.send_request(StreamingControlStreamRequest {
                request: Some(
                    streaming_control_stream_request::Request::CreatePartialGraph(
                        PbCreatePartialGraphRequest {
                            partial_graph_id,
                            term_id: term_id.to_owned(),
                        },
                    ),
                ),
            }) {
                warn!(e = %e.as_report(), node = ?self.node, "failed to send initial partial graph request");
            }
        }
    }
}

pub(super) enum WorkerNodeEvent<'a> {
    Response(MetaResult<streaming_control_stream_response::Response>),
    Connected(WorkerNodeConnected<'a>),
}

impl ControlStreamManager {
    fn poll_next_event<'a>(
        this_opt: &mut Option<&'a mut Self>,
        cx: &mut Context<'_>,
        context: &Arc<impl GlobalBarrierWorkerContext>,
        poll_reconnect: bool,
    ) -> Poll<(WorkerId, WorkerNodeEvent<'a>)> {
        let this = this_opt.as_mut().expect("Future polled after completion");
        if this.workers.is_empty() {
            return Poll::Pending;
        }
        {
            for (&worker_id, (node, worker_state)) in &mut this.workers {
                let control_stream = match worker_state {
                    WorkerNodeState::Connected { control_stream, .. } => control_stream,
                    WorkerNodeState::Reconnecting(_) if !poll_reconnect => {
                        continue;
                    }
                    WorkerNodeState::Reconnecting(join_handle) => {
                        match join_handle.poll_unpin(cx) {
                            Poll::Ready(handle) => {
                                info!(id=%node.id, host=?node.host, "reconnected to worker");
                                *worker_state = WorkerNodeState::Connected {
                                    control_stream: ControlStreamNode {
                                        worker_id: node.id,
                                        host: node.host.clone().unwrap(),
                                        handle,
                                    },
                                    removed: false,
                                };
                                let this = this_opt.take().expect("should exist");
                                let (node, worker_state) =
                                    this.workers.get_mut(&worker_id).expect("should exist");
                                let WorkerNodeState::Connected { control_stream, .. } =
                                    worker_state
                                else {
                                    unreachable!()
                                };
                                return Poll::Ready((
                                    worker_id,
                                    WorkerNodeEvent::Connected(WorkerNodeConnected {
                                        node,
                                        handle: &mut control_stream.handle,
                                    }),
                                ));
                            }
                            Poll::Pending => {
                                continue;
                            }
                        }
                    }
                };
                match control_stream.handle.response_stream.poll_next_unpin(cx) {
                    Poll::Ready(result) => {
                        {
                            let result = result
                                .ok_or_else(|| (false, anyhow!("end of stream").into()))
                                .and_then(|result| {
                                    result.map_err(|err| -> (bool, MetaError) { (false, err.into()) }).and_then(|resp| {
                                        match resp
                                            .response
                                            .ok_or_else(|| (false, anyhow!("empty response").into()))?
                                        {
                                            streaming_control_stream_response::Response::Shutdown(_) => Err((true, anyhow!(
                                                "worker node {worker_id} is shutting down"
                                            )
                                                .into())),
                                            streaming_control_stream_response::Response::Init(_) => {
                                                // This arm should be unreachable.
                                                Err((false, anyhow!("get unexpected init response").into()))
                                            }
                                            resp => {
                                                if let streaming_control_stream_response::Response::CompleteBarrier(barrier_resp) = &resp {
                                                    assert_eq!(worker_id, barrier_resp.worker_id);
                                                }
                                                Ok(resp)
                                            }
                                        }
                                    })
                                });
                            let result = match result {
                                Ok(resp) => Ok(resp),
                                Err((shutdown, err)) => {
                                    warn!(worker_id = %node.id, host = ?node.host, err = %err.as_report(), "get error from response stream");
                                    let WorkerNodeState::Connected { removed, .. } = worker_state
                                    else {
                                        unreachable!("checked connected")
                                    };
                                    if *removed || shutdown {
                                        this.workers.remove(&worker_id);
                                    } else {
                                        *worker_state = WorkerNodeState::Reconnecting(
                                            ControlStreamManager::retry_connect(
                                                node.clone(),
                                                context.clone(),
                                            ),
                                        );
                                    }
                                    Err(err)
                                }
                            };
                            return Poll::Ready((worker_id, WorkerNodeEvent::Response(result)));
                        }
                    }
                    Poll::Pending => {
                        continue;
                    }
                }
            }
        };

        Poll::Pending
    }

    #[await_tree::instrument("control_stream_next_event")]
    pub(super) async fn next_event<'a>(
        &'a mut self,
        context: &Arc<impl GlobalBarrierWorkerContext>,
    ) -> (WorkerId, WorkerNodeEvent<'a>) {
        let mut this = Some(self);
        poll_fn(|cx| Self::poll_next_event(&mut this, cx, context, true)).await
    }

    #[await_tree::instrument("control_stream_next_response")]
    pub(super) async fn next_response(
        &mut self,
        context: &Arc<impl GlobalBarrierWorkerContext>,
    ) -> (
        WorkerId,
        MetaResult<streaming_control_stream_response::Response>,
    ) {
        let mut this = Some(self);
        let (worker_id, event) =
            poll_fn(|cx| Self::poll_next_event(&mut this, cx, context, false)).await;
        match event {
            WorkerNodeEvent::Response(result) => (worker_id, result),
            WorkerNodeEvent::Connected(_) => {
                unreachable!("set poll_reconnect=false")
            }
        }
    }
}

pub(super) struct DatabaseInitialBarrierCollector {
    pub(super) database_id: DatabaseId,
    pub(super) initializing_partial_graphs: HashSet<PartialGraphId>,
    pub(super) database: DatabaseCheckpointControl,
}

impl Debug for DatabaseInitialBarrierCollector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseInitialBarrierCollector")
            .field("database_id", &self.database_id)
            .field("initializing_graphs", &self.initializing_partial_graphs)
            .finish()
    }
}

impl DatabaseInitialBarrierCollector {
    pub(super) fn is_collected(&self) -> bool {
        self.initializing_partial_graphs.is_empty()
    }

    pub(super) fn partial_graph_initialized(&mut self, partial_graph_id: PartialGraphId) {
        assert!(self.initializing_partial_graphs.remove(&partial_graph_id));
    }

    pub(super) fn all_partial_graphs(&self) -> impl Iterator<Item = PartialGraphId> + '_ {
        database_partial_graphs(
            self.database_id,
            self.database
                .independent_checkpoint_job_controls
                .keys()
                .copied(),
        )
    }

    pub(super) fn finish(self) -> DatabaseCheckpointControl {
        assert!(self.is_collected());
        self.database
    }

    pub(super) fn is_valid_after_worker_err(&self, worker_id: WorkerId) -> bool {
        self.database.is_valid_after_worker_err(worker_id)
    }
}

impl PartialGraphRecoverer<'_> {
    /// Extract information from the loaded runtime barrier worker snapshot info, and inject the initial barrier.
    #[expect(clippy::too_many_arguments)]
    pub(super) fn inject_database_initial_barrier(
        &mut self,
        database_id: DatabaseId,
        jobs: HashMap<JobId, HashMap<FragmentId, InflightFragmentInfo>>,
        job_extra_info: &HashMap<JobId, StreamingJobExtraInfo>,
        state_table_committed_epochs: &mut HashMap<TableId, u64>,
        state_table_log_epochs: &mut HashMap<TableId, Vec<(Vec<u64>, u64)>>,
        fragment_relations: &FragmentDownstreamRelation,
        stream_actors: &HashMap<ActorId, StreamActor>,
        source_splits: &mut HashMap<ActorId, Vec<SplitImpl>>,
        creating_jobs: &mut HashSet<JobId>,
        mv_depended_subscriptions: &mut HashMap<TableId, HashMap<SubscriptionId, u64>>,
        is_paused: bool,
        hummock_version_stats: &HummockVersionStats,
        cdc_table_snapshot_splits: &mut HashMap<JobId, CdcTableSnapshotSplits>,
        batch_refresh: HashMap<JobId, BatchRefreshRenderResult>,
    ) -> MetaResult<DatabaseCheckpointControl> {
        let term_id = Uuid::new_v4().to_string();

        fn collect_source_splits(
            fragment_infos: impl Iterator<Item = &InflightFragmentInfo>,
            source_splits: &mut HashMap<ActorId, Vec<SplitImpl>>,
        ) -> HashMap<ActorId, Vec<SplitImpl>> {
            fragment_infos
                .flat_map(|info| info.actors.keys())
                .filter_map(|actor_id| {
                    let actor_id = *actor_id as ActorId;
                    source_splits
                        .remove(&actor_id)
                        .map(|splits| (actor_id, splits))
                })
                .collect()
        }
        fn build_mutation(
            splits: &HashMap<ActorId, Vec<SplitImpl>>,
            cdc_table_snapshot_split_assignment: HashMap<ActorId, PbCdcTableSnapshotSplits>,
            backfill_orders: &ExtendedFragmentBackfillOrder,
            is_paused: bool,
        ) -> Mutation {
            let backfill_nodes_to_pause = get_nodes_with_backfill_dependencies(backfill_orders)
                .into_iter()
                .collect();
            Mutation::Add(AddMutation {
                // Actors built during recovery is not treated as newly added actors.
                actor_dispatchers: Default::default(),
                added_actors: Default::default(),
                actor_splits: build_actor_connector_splits(splits),
                actor_cdc_table_snapshot_splits: Some(PbCdcTableSnapshotSplitsWithGeneration {
                    splits: cdc_table_snapshot_split_assignment,
                }),
                pause: is_paused,
                subscriptions_to_add: Default::default(),
                backfill_nodes_to_pause,
                new_upstream_sinks: Default::default(),
                dropped_actors: Default::default(),
                sink_log_store_flush: Default::default(),
            })
        }

        fn resolve_jobs_committed_epoch(
            state_table_committed_epochs: &mut HashMap<TableId, u64>,
            table_ids: impl Iterator<Item = TableId>,
        ) -> u64 {
            let mut epochs = table_ids.map(|table_id| {
                (
                    table_id,
                    state_table_committed_epochs
                        .remove(&table_id)
                        .expect("should exist"),
                )
            });
            let (first_table_id, prev_epoch) = epochs.next().expect("non-empty");
            for (table_id, epoch) in epochs {
                assert_eq!(
                    prev_epoch, epoch,
                    "{} has different committed epoch to {}",
                    first_table_id, table_id
                );
            }
            prev_epoch
        }
        fn job_backfill_orders(
            job_extra_info: &HashMap<JobId, StreamingJobExtraInfo>,
            job_id: JobId,
        ) -> UserDefinedFragmentBackfillOrder {
            UserDefinedFragmentBackfillOrder::new(
                job_extra_info
                    .get(&job_id)
                    .and_then(|info| info.backfill_orders.clone())
                    .map_or_else(HashMap::new, |orders| orders.0),
            )
        }

        let mut subscribers: HashMap<_, HashMap<_, _>> = jobs
            .keys()
            .filter_map(|job_id| {
                mv_depended_subscriptions
                    .remove(&job_id.as_mv_table_id())
                    .map(|subscriptions| {
                        (
                            job_id.as_mv_table_id(),
                            subscriptions
                                .into_iter()
                                .map(|(subscription_id, retention)| {
                                    (
                                        subscription_id.as_subscriber_id(),
                                        SubscriberType::Subscription(retention),
                                    )
                                })
                                .collect(),
                        )
                    })
            })
            .collect();

        // Batch-refresh jobs are rendered outside `jobs`, but their upstream tables
        // must still start with log-store-enabled subscribers after recovery.
        for (job_id, render_result) in &batch_refresh {
            let snapshot_backfill_info = StreamFragmentGraph::collect_snapshot_backfill_info_impl(
                render_result
                    .fragment_infos
                    .values()
                    .map(|fragment| (&fragment.nodes, fragment.fragment_type_mask)),
            )?
            .0
            .ok_or_else(|| anyhow!("batch refresh job {} has no snapshot backfill info", job_id))?;

            for upstream_table_id in snapshot_backfill_info
                .upstream_mv_table_id_to_backfill_epoch
                .keys()
            {
                subscribers
                    .entry(*upstream_table_id)
                    .or_default()
                    .try_insert(job_id.as_subscriber_id(), SubscriberType::SnapshotBackfill)
                    .expect("non-duplicate");
            }
        }

        let mut database_jobs = HashMap::new();
        let mut snapshot_backfill_jobs = HashMap::new();

        for (job_id, job_fragments) in jobs {
            if creating_jobs.remove(&job_id) {
                if job_fragments.values().any(|fragment| {
                    fragment
                        .fragment_type_mask
                        .contains(FragmentTypeFlag::SnapshotBackfillStreamScan)
                }) {
                    debug!(%job_id, "recovered snapshot backfill job");
                    snapshot_backfill_jobs.insert(job_id, job_fragments);
                } else {
                    database_jobs.insert(job_id, (job_fragments, true));
                }
            } else {
                database_jobs.insert(job_id, (job_fragments, false));
            }
        }

        let database_job_log_epochs: HashMap<_, _> = database_jobs
            .keys()
            .filter_map(|job_id| {
                state_table_log_epochs
                    .remove(&job_id.as_mv_table_id())
                    .map(|epochs| (job_id.as_mv_table_id(), epochs))
            })
            .collect();

        let prev_epoch = resolve_jobs_committed_epoch(
            state_table_committed_epochs,
            InflightFragmentInfo::existing_table_ids(
                database_jobs.values().flat_map(|(job, _)| job.values()),
            ),
        );
        let prev_epoch = TracedEpoch::new(Epoch(prev_epoch));
        // Use a different `curr_epoch` for each recovery attempt.
        let curr_epoch = prev_epoch.next();
        let barrier_info = BarrierInfo {
            prev_epoch,
            curr_epoch,
            kind: BarrierKind::Initial,
        };

        let mut ongoing_snapshot_backfill_jobs: HashMap<JobId, _> = HashMap::new();
        for (job_id, fragment_infos) in snapshot_backfill_jobs {
            let committed_epoch = resolve_jobs_committed_epoch(
                state_table_committed_epochs,
                InflightFragmentInfo::existing_table_ids(fragment_infos.values()),
            );
            if committed_epoch == barrier_info.prev_epoch() {
                info!(
                    "recovered creating snapshot backfill job {} catch up with upstream already",
                    job_id
                );
                database_jobs
                    .try_insert(job_id, (fragment_infos, true))
                    .expect("non-duplicate");
                continue;
            }
            let snapshot_backfill_info = StreamFragmentGraph::collect_snapshot_backfill_info_impl(
                fragment_infos
                    .values()
                    .map(|fragment| (&fragment.nodes, fragment.fragment_type_mask)),
            )?
            .0
            .ok_or_else(|| {
                anyhow!(
                    "recovered snapshot backfill job {} has no snapshot backfill info",
                    job_id
                )
            })?;
            let mut snapshot_epoch = None;
            let upstream_table_ids: HashSet<_> = snapshot_backfill_info
                .upstream_mv_table_id_to_backfill_epoch
                .keys()
                .cloned()
                .collect();
            for (upstream_table_id, epoch) in
                snapshot_backfill_info.upstream_mv_table_id_to_backfill_epoch
            {
                let epoch = epoch.ok_or_else(|| anyhow!("recovered snapshot backfill job {} to upstream {} has not set snapshot epoch", job_id, upstream_table_id))?;
                let snapshot_epoch = snapshot_epoch.get_or_insert(epoch);
                if *snapshot_epoch != epoch {
                    return Err(anyhow!("snapshot epoch {} to upstream {} different to snapshot epoch {} to previous upstream", epoch, upstream_table_id, snapshot_epoch).into());
                }
            }
            let snapshot_epoch = snapshot_epoch.ok_or_else(|| {
                anyhow!(
                    "snapshot backfill job {} has not set snapshot epoch",
                    job_id
                )
            })?;
            for upstream_table_id in &upstream_table_ids {
                subscribers
                    .entry(*upstream_table_id)
                    .or_default()
                    .try_insert(job_id.as_subscriber_id(), SubscriberType::SnapshotBackfill)
                    .expect("non-duplicate");
            }
            ongoing_snapshot_backfill_jobs
                .try_insert(
                    job_id,
                    (
                        fragment_infos,
                        upstream_table_ids,
                        committed_epoch,
                        snapshot_epoch,
                    ),
                )
                .expect("non-duplicated");
        }

        let mut cdc_table_snapshot_split_assignment: HashMap<ActorId, PbCdcTableSnapshotSplits> =
            HashMap::new();

        let database_jobs: HashMap<JobId, InflightStreamingJobInfo> = {
            database_jobs
                .into_iter()
                .map(|(job_id, (fragment_infos, is_creating))| {
                    let status = if is_creating {
                        let backfill_ordering = job_backfill_orders(job_extra_info, job_id);
                        let backfill_ordering = StreamFragmentGraph::extend_fragment_backfill_ordering_with_locality_backfill(
                            backfill_ordering,
                            fragment_relations,
                            || fragment_infos.iter().map(|(fragment_id, fragment)| {
                            (*fragment_id, fragment.fragment_type_mask, &fragment.nodes)
                        }));
                        let locality_fragment_state_table_mapping =
                            build_locality_fragment_state_table_mapping(&fragment_infos);
                        let backfill_order_state = BackfillOrderState::recover_from_fragment_infos(
                            &backfill_ordering,
                            &fragment_infos,
                            locality_fragment_state_table_mapping,
                        );
                        CreateStreamingJobStatus::Creating {
                            tracker: CreateMviewProgressTracker::recover(
                                job_id,
                                &fragment_infos,
                                backfill_order_state,
                                hummock_version_stats,
                            ),
                        }
                    } else {
                        CreateStreamingJobStatus::Created
                    };
                    let cdc_table_backfill_tracker =
                        if let Some(splits) = cdc_table_snapshot_splits.remove(&job_id) {
                            let cdc_fragment = fragment_infos
                                .values()
                                .find(|fragment| {
                                    is_parallelized_backfill_enabled_cdc_scan_fragment(
                                        fragment.fragment_type_mask,
                                        &fragment.nodes,
                                    )
                                    .is_some()
                                })
                                .expect("should have parallel cdc fragment");
                            let cdc_actors = cdc_fragment.actors.keys().copied().collect();
                            let mut tracker =
                                CdcTableBackfillTracker::restore(cdc_fragment.fragment_id, splits);
                            cdc_table_snapshot_split_assignment
                                .extend(tracker.reassign_splits(cdc_actors)?);
                            Some(tracker)
                        } else {
                            None
                        };
                    Ok((
                        job_id,
                        InflightStreamingJobInfo {
                            job_id,
                            fragment_infos,
                            subscribers: subscribers
                                .remove(&job_id.as_mv_table_id())
                                .unwrap_or_default(),
                            status,
                            cdc_table_backfill_tracker,
                        },
                    ))
                })
                .try_collect::<_, _, MetaError>()
        }?;

        let control_stream_manager = self.control_stream_manager();
        let mut builder = FragmentEdgeBuilder::from_inflight_fragments(
            database_jobs.values().flat_map(|job| job.fragment_infos()),
            to_partial_graph_id(database_id, None),
            control_stream_manager,
        );
        builder.add_relations(fragment_relations);
        let mut edges = builder.build();

        {
            let new_actors =
                edges.collect_actors_to_create(database_jobs.values().flat_map(move |job| {
                    job.fragment_infos.values().map(move |fragment_infos| {
                        (
                            fragment_infos.fragment_id,
                            &fragment_infos.nodes,
                            fragment_infos.actors.iter().map(move |(actor_id, actor)| {
                                (
                                    stream_actors.get(actor_id).expect("should exist"),
                                    actor.worker_id,
                                )
                            }),
                            job.subscribers.keys().copied(),
                        )
                    })
                }));

            let nodes_actors =
                InflightFragmentInfo::actor_ids_to_collect(database_jobs.values().flatten());
            let database_job_source_splits =
                collect_source_splits(database_jobs.values().flatten(), source_splits);
            let database_backfill_orders =
                UserDefinedFragmentBackfillOrder::merge(database_jobs.values().map(|job| {
                    if matches!(job.status, CreateStreamingJobStatus::Creating { .. }) {
                        job_backfill_orders(job_extra_info, job.job_id)
                    } else {
                        UserDefinedFragmentBackfillOrder::default()
                    }
                }));
            let database_backfill_orders =
                StreamFragmentGraph::extend_fragment_backfill_ordering_with_locality_backfill(
                    database_backfill_orders,
                    fragment_relations,
                    || {
                        database_jobs.values().flat_map(|job_fragments| {
                            job_fragments
                                .fragment_infos
                                .iter()
                                .map(|(fragment_id, fragment)| {
                                    (*fragment_id, fragment.fragment_type_mask, &fragment.nodes)
                                })
                        })
                    },
                );
            let mutation = build_mutation(
                &database_job_source_splits,
                cdc_table_snapshot_split_assignment,
                &database_backfill_orders,
                is_paused,
            );

            let partial_graph_id = to_partial_graph_id(database_id, None);
            self.recover_graph(
                partial_graph_id,
                &term_id,
                mutation,
                &barrier_info,
                &nodes_actors,
                InflightFragmentInfo::existing_table_ids(database_jobs.values().flatten()),
                new_actors,
                DatabaseCheckpointControlMetrics::new(database_id),
            )?;
            debug!(
                %database_id,
                "inject initial barrier"
            );
        };

        let mut independent_checkpoint_job_controls: HashMap<
            JobId,
            IndependentCheckpointJobControl,
        > = HashMap::new();
        for (job_id, (info, upstream_table_ids, committed_epoch, snapshot_epoch)) in
            ongoing_snapshot_backfill_jobs
        {
            let database_job_source_splits =
                collect_source_splits(database_jobs.values().flatten(), source_splits);
            assert!(
                !cdc_table_snapshot_splits.contains_key(&job_id),
                "snapshot backfill job {job_id} should not have cdc backfill"
            );
            if is_paused {
                bail!("should not pause when having snapshot backfill job {job_id}");
            }
            let job_backfill_orders = job_backfill_orders(job_extra_info, job_id);
            let job_backfill_orders =
                StreamFragmentGraph::extend_fragment_backfill_ordering_with_locality_backfill(
                    job_backfill_orders,
                    fragment_relations,
                    || {
                        info.iter().map(|(fragment_id, fragment)| {
                            (*fragment_id, fragment.fragment_type_mask, &fragment.nodes)
                        })
                    },
                );
            let mutation = build_mutation(
                &database_job_source_splits,
                Default::default(), // no cdc backfill job for
                &job_backfill_orders,
                false,
            );

            let job = CreatingStreamingJobControl::recover(
                database_id,
                job_id,
                upstream_table_ids,
                &database_job_log_epochs,
                snapshot_epoch,
                committed_epoch,
                &barrier_info,
                info,
                job_backfill_orders,
                fragment_relations,
                hummock_version_stats,
                stream_actors,
                mutation.clone(),
                &term_id,
                self,
            )?;
            independent_checkpoint_job_controls.insert(
                job_id,
                IndependentCheckpointJobControl::CreatingStreamingJob(job),
            );
        }

        // Recover batch refresh jobs (both idle and consuming snapshot).
        // Actors were already rendered by `render_runtime_info()`.
        for (job_id, render_result) in batch_refresh {
            creating_jobs.remove(&job_id);
            debug!(%job_id, "recovered batch refresh job");

            // Resolve committed epoch from state tables.
            let committed_epoch = resolve_jobs_committed_epoch(
                state_table_committed_epochs,
                InflightFragmentInfo::existing_table_ids(render_result.fragment_infos.values()),
            );

            let snapshot_backfill_info = StreamFragmentGraph::collect_snapshot_backfill_info_impl(
                render_result
                    .fragment_infos
                    .values()
                    .map(|fragment| (&fragment.nodes, fragment.fragment_type_mask)),
            )?
            .0
            .ok_or_else(|| anyhow!("batch refresh job {} has no snapshot backfill info", job_id))?;

            let upstream_table_ids: HashSet<TableId> = snapshot_backfill_info
                .upstream_mv_table_id_to_backfill_epoch
                .keys()
                .copied()
                .collect();
            let snapshot_epoch = snapshot_backfill_info
                .upstream_mv_table_id_to_backfill_epoch
                .values()
                .find_map(|e| *e)
                .unwrap_or(committed_epoch);

            let job_backfill_orders = job_backfill_orders(job_extra_info, job_id);
            let job_backfill_orders =
                StreamFragmentGraph::extend_fragment_backfill_ordering_with_locality_backfill(
                    job_backfill_orders,
                    fragment_relations,
                    || {
                        render_result
                            .fragment_infos
                            .iter()
                            .map(|(fid, f)| (*fid, f.fragment_type_mask, &f.nodes))
                    },
                );
            let mutation = build_mutation(
                &Default::default(), // batch refresh has no source splits
                Default::default(),
                &job_backfill_orders,
                false,
            );

            let refresh_interval_sec = job_extra_info
                .get(&job_id)
                .and_then(|info| info.refresh_interval_sec)
                .expect("batch refresh job should have refresh_interval_sec in job extra info");

            let job = BatchRefreshJobCheckpointControl::recover(
                database_id,
                job_id,
                upstream_table_ids,
                snapshot_epoch,
                committed_epoch,
                job_backfill_orders,
                hummock_version_stats,
                mutation,
                render_result,
                &term_id,
                self,
                refresh_interval_sec,
            )?;
            independent_checkpoint_job_controls
                .insert(job_id, IndependentCheckpointJobControl::BatchRefresh(job));
        }

        self.control_stream_manager()
            .env
            .shared_actor_infos()
            .recover_database(
                database_id,
                database_jobs
                    .values()
                    .flat_map(|info| {
                        info.fragment_infos()
                            .map(move |fragment| (fragment, info.job_id))
                    })
                    .chain(
                        independent_checkpoint_job_controls
                            .iter()
                            .flat_map(|(job_id, job)| {
                                let job_id = *job_id;
                                job.fragment_infos()
                                    .into_iter()
                                    .flat_map(move |infos| infos.values().map(move |f| (f, job_id)))
                            }),
                    ),
            );

        let committed_epoch = barrier_info.prev_epoch();
        let new_epoch = barrier_info.curr_epoch;
        let database_info = InflightDatabaseInfo::recover(
            database_id,
            database_jobs.into_values(),
            self.control_stream_manager()
                .env
                .shared_actor_infos()
                .clone(),
        );
        let database_state = BarrierWorkerState::recovery(new_epoch, is_paused);
        Ok(DatabaseCheckpointControl::recovery(
            database_id,
            term_id,
            database_state,
            committed_epoch,
            database_info,
            independent_checkpoint_job_controls,
        ))
    }
}

impl ControlStreamManager {
    fn connected_workers(&self) -> impl Iterator<Item = (WorkerId, &ControlStreamNode)> + '_ {
        self.workers
            .iter()
            .filter_map(|(worker_id, (_, worker_state))| match worker_state {
                WorkerNodeState::Connected { control_stream, .. } => {
                    Some((*worker_id, control_stream))
                }
                WorkerNodeState::Reconnecting(_) => None,
            })
    }

    pub(super) fn inject_barrier(
        &mut self,
        partial_graph_id: PartialGraphId,
        mutation: Option<Mutation>,
        iceberg_pk_index_compaction: Option<IcebergPkIndexCompactionContext>,
        barrier_info: &BarrierInfo,
        node_actors: &HashMap<WorkerId, HashSet<ActorId>>,
        table_ids_to_sync: impl Iterator<Item = TableId>,
        nodes_to_sync_table: impl Iterator<Item = WorkerId>,
        mut new_actors: Option<StreamJobActorsToCreate>,
    ) -> MetaResult<NodeToCollect> {
        fail_point!("inject_barrier_err", |_| risingwave_common::bail!(
            "inject_barrier_err"
        ));

        let nodes_to_sync_table: HashSet<_> = nodes_to_sync_table.collect();

        nodes_to_sync_table.iter().for_each(|worker_id| {
            assert!(node_actors.contains_key(worker_id), "worker_id {worker_id} in nodes_to_sync_table {nodes_to_sync_table:?} but not in node_actors {node_actors:?}");
        });

        let mut node_need_collect = NodeToCollect::new();
        let table_ids_to_sync = table_ids_to_sync.collect_vec();

        node_actors.iter()
            .try_for_each(|(worker_id, actor_ids_to_collect)| {
                assert!(!actor_ids_to_collect.is_empty(), "empty actor_ids_to_collect on worker {worker_id} in node_actors {node_actors:?}");
                let table_ids_to_sync = if nodes_to_sync_table.contains(worker_id) {
                    table_ids_to_sync.clone()
                } else {
                    vec![]
                };

                let node = if let Some((_, worker_state)) = self.workers.get(worker_id)
                    &&
                    let WorkerNodeState::Connected { control_stream, .. } = worker_state
                {
                    control_stream
                } else {
                    return Err(anyhow!("unconnected worker node {}", worker_id).into());
                };

                {
                    let mutation = mutation.clone();
                    let barrier = Barrier {
                        epoch: Some(risingwave_pb::data::Epoch {
                            curr: barrier_info.curr_epoch(),
                            prev: barrier_info.prev_epoch(),
                        }),
                        mutation: mutation.clone().map(|_| BarrierMutation { mutation }),
                        tracing_context: TracingContext::from_span(barrier_info.curr_epoch.span())
                            .to_protobuf(),
                        kind: barrier_info.kind.to_protobuf() as i32,
                        iceberg_pk_index_compaction,
                    };

                    node.handle
                        .request_sender
                        .send(StreamingControlStreamRequest {
                            request: Some(
                                streaming_control_stream_request::Request::InjectBarrier(
                                    InjectBarrierRequest {
                                        request_id: Uuid::new_v4().to_string(),
                                        barrier: Some(barrier),
                                        actor_ids_to_collect: actor_ids_to_collect.iter().copied().collect(),
                                        table_ids_to_sync,
                                        partial_graph_id,
                                        actors_to_build: new_actors
                                            .as_mut()
                                            .map(|new_actors| new_actors.remove(worker_id))
                                            .into_iter()
                                            .flatten()
                                            .flatten()
                                            .map(|(fragment_id, (node, actors, initial_subscriber_ids))| {
                                                FragmentBuildActorInfo {
                                                    fragment_id,
                                                    node: Some(node),
                                                    actors: actors
                                                        .into_iter()
                                                        .map(|(actor, upstreams, dispatchers)| {
                                                            BuildActorInfo {
                                                                actor_id: actor.actor_id,
                                                                fragment_upstreams: upstreams
                                                                    .into_iter()
                                                                    .map(|(fragment_id, upstreams)| {
                                                                        (
                                                                            fragment_id,
                                                                            UpstreamActors {
                                                                                actors: upstreams
                                                                                    .into_values()
                                                                                    .collect(),
                                                                            },
                                                                        )
                                                                    })
                                                                    .collect(),
                                                                dispatchers,
                                                                vnode_bitmap: actor.vnode_bitmap.map(|bitmap| bitmap.to_protobuf()),
                                                                mview_definition: actor.mview_definition,
                                                                expr_context: actor.expr_context,
                                                                config_override: actor.config_override.to_string(),
                                                                initial_subscriber_ids: initial_subscriber_ids.iter().copied().collect(),
                                                            }
                                                        })
                                                        .collect(),
                                                }
                                            })
                                            .collect(),
                                    },
                                ),
                            ),
                        })
                        .map_err(|_| {
                            MetaError::from(anyhow!(
                                "failed to send request to {} {:?}",
                                node.worker_id,
                                node.host
                            ))
                        })?;

                    node_need_collect.insert(*worker_id);
                    Result::<_, MetaError>::Ok(())
                }
            })
            .inspect_err(|e| {
                // Record failure in event log.
                use risingwave_pb::meta::event_log;
                let event = event_log::EventInjectBarrierFail {
                    prev_epoch: barrier_info.prev_epoch(),
                    cur_epoch: barrier_info.curr_epoch(),
                    error: e.to_report_string(),
                };
                self.env
                    .event_log_manager_ref()
                    .add_event_logs(vec![event_log::Event::InjectBarrierFail(event)]);
            })?;
        Ok(node_need_collect)
    }

    pub(super) fn add_partial_graph(&mut self, partial_graph_id: PartialGraphId, term_id: &str) {
        self.connected_workers().for_each(|(_, node)| {
            if node
                .handle
                .request_sender
                .send(StreamingControlStreamRequest {
                    request: Some(
                        streaming_control_stream_request::Request::CreatePartialGraph(
                            CreatePartialGraphRequest {
                                partial_graph_id,
                                term_id: term_id.to_owned(),
                            },
                        ),
                    ),
                }).is_err() {
                let (database_id, creating_job_id) = from_partial_graph_id(partial_graph_id);
                warn!(%database_id, ?creating_job_id, worker_id = %node.worker_id, "failed to add the partial graph to the worker")
            }
        });
    }

    pub(super) fn remove_partial_graphs(&mut self, partial_graph_ids: Vec<PartialGraphId>) {
        self.connected_workers().for_each(|(_, node)| {
            if node.handle
                .request_sender
                .send(StreamingControlStreamRequest {
                    request: Some(
                        streaming_control_stream_request::Request::RemovePartialGraph(
                            RemovePartialGraphRequest {
                                partial_graph_ids: partial_graph_ids.clone(),
                            },
                        ),
                    ),
                })
                .is_err()
            {
                warn!(worker_id = %node.worker_id,node = ?node.host,"failed to send remove partial graph request");
            }
        })
    }

    pub(super) fn reset_partial_graphs(
        &mut self,
        partial_graph_ids: Vec<PartialGraphId>,
    ) -> HashSet<WorkerId> {
        self.connected_workers()
            .filter_map(|(worker_id, node)| {
                if node
                    .handle
                    .request_sender
                    .send(StreamingControlStreamRequest {
                        request: Some(
                            streaming_control_stream_request::Request::ResetPartialGraphs(
                                ResetPartialGraphsRequest {
                                    partial_graph_ids: partial_graph_ids.clone(),
                                },
                            ),
                        ),
                    })
                    .is_err()
                {
                    warn!(%worker_id, node = ?node.host,"failed to send reset database request");
                    None
                } else {
                    Some(worker_id)
                }
            })
            .collect()
    }
}

impl GlobalBarrierWorkerContextImpl {
    pub(super) async fn new_control_stream_impl(
        &self,
        node: &WorkerNode,
    ) -> MetaResult<StreamingControlHandle> {
        let handle = self
            .env
            .stream_client_pool()
            .get(node)
            .await?
            .start_streaming_control(PbInitRequest::default())
            .await?;
        Ok(handle)
    }
}

pub(super) fn merge_node_rpc_errors<E: Error + Send + Sync + 'static>(
    message: &str,
    errors: impl IntoIterator<Item = (WorkerId, E)>,
) -> MetaError {
    use std::fmt::Write;

    use risingwave_common::error::error_request_copy;
    use risingwave_common::error::tonic::extra::Score;

    let errors = errors.into_iter().collect_vec();

    if errors.is_empty() {
        return anyhow!(message.to_owned()).into();
    }

    // Create the error from the single error.
    let single_error = |(worker_id, e)| {
        anyhow::Error::from(e)
            .context(format!("{message}, in worker node {worker_id}"))
            .into()
    };

    if errors.len() == 1 {
        return single_error(errors.into_iter().next().unwrap());
    }

    // Find the error with the highest score.
    let max_score = errors
        .iter()
        .filter_map(|(_, e)| error_request_copy::<Score>(e))
        .max();

    if let Some(max_score) = max_score {
        let mut errors = errors;
        let max_scored = errors
            .extract_if(.., |(_, e)| {
                error_request_copy::<Score>(e) == Some(max_score)
            })
            .next()
            .unwrap();

        return single_error(max_scored);
    }

    // The errors do not have scores, so simply concatenate them.
    let concat: String = errors
        .into_iter()
        .fold(format!("{message}: "), |mut s, (w, e)| {
            write!(&mut s, " in worker node {}, {};", w, e.as_report()).unwrap();
            s
        });
    anyhow!(concat).into()
}

#[cfg(test)]
mod test_partial_graph_id {
    use crate::barrier::rpc::{from_partial_graph_id, to_partial_graph_id};

    #[test]
    fn test_partial_graph_id_conversion() {
        let database_id = 233.into();
        let job_id = 233.into();
        assert_eq!(
            (database_id, None),
            from_partial_graph_id(to_partial_graph_id(database_id, None))
        );
        assert_eq!(
            (database_id, Some(job_id)),
            from_partial_graph_id(to_partial_graph_id(database_id, Some(job_id)))
        );
    }
}
