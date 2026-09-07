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

use std::collections::Bound::Unbounded;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::time::Instant;

use educe::Educe;
use itertools::Itertools;
use risingwave_common::util::epoch::EpochPair;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::id::{ActorId, PartialGraphId, TableId, WorkerId};
use risingwave_pb::stream_plan::IcebergPkIndexCompactionContext;
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use risingwave_pb::stream_service::streaming_control_stream_response::{
    ResetPartialGraphResponse, Response,
};
use tracing::{debug, warn};

use crate::barrier::BarrierKind;
use crate::barrier::command::PostCollectCommand;
use crate::barrier::context::GlobalBarrierWorkerContext;
use crate::barrier::info::BarrierInfo;
use crate::barrier::notifier::{CollectionNotifier, NotifierStarter};
use crate::barrier::rpc::{ControlStreamManager, WorkerNodeEvent};
use crate::barrier::utils::{BarrierItemCollector, NodeToCollect, is_valid_after_worker_err};
use crate::manager::MetaSrvEnv;
use crate::model::StreamJobActorsToCreate;
use crate::{MetaError, MetaResult};

#[derive(Debug)]
pub(super) struct PartialGraphBarrierInfo {
    enqueue_time: Instant,
    pub(super) post_collect_command: PostCollectCommand,
    pub(super) barrier_info: BarrierInfo,
    pub(super) notifier: Option<CollectionNotifier>,
    pub(super) table_ids_to_commit: HashSet<TableId>,
}

impl PartialGraphBarrierInfo {
    pub(super) fn new(
        post_collect_command: PostCollectCommand,
        barrier_info: BarrierInfo,
        notifier: Option<&mut NotifierStarter>,
        table_ids_to_commit: HashSet<TableId>,
    ) -> Self {
        Self {
            enqueue_time: Instant::now(),
            post_collect_command,
            barrier_info,
            notifier: notifier.map(NotifierStarter::add_notify),
            table_ids_to_commit,
        }
    }

    pub(super) fn elapsed_secs(&self) -> f64 {
        self.enqueue_time.elapsed().as_secs_f64()
    }
}

pub(super) trait PartialGraphStat: Send + Sync + 'static {
    fn observe_barrier_latency(&self, epoch: EpochPair, barrier_latency_secs: f64);
    fn observe_barrier_num(&self, inflight_barrier_num: usize, collected_barrier_num: usize);
}

#[derive(Educe)]
#[educe(Debug)]
struct PartialGraphRunningState {
    barrier_item_collector:
        BarrierItemCollector<WorkerId, BarrierCompleteResponse, PartialGraphBarrierInfo>,
    completing_epoch: Option<u64>,
    #[educe(Debug(ignore))]
    stat: Box<dyn PartialGraphStat>,
}

impl PartialGraphRunningState {
    fn new(stat: Box<dyn PartialGraphStat>) -> Self {
        Self {
            barrier_item_collector: BarrierItemCollector::new(true),
            completing_epoch: None,
            stat,
        }
    }

    fn is_empty(&self) -> bool {
        self.barrier_item_collector.is_empty() && self.completing_epoch.is_none()
    }

    fn pending_barrier_num(&self) -> usize {
        self.barrier_item_collector.inflight_barrier_num()
            + self.barrier_item_collector.collected_barrier_num()
            + usize::from(self.completing_epoch.is_some())
    }

    fn enqueue(&mut self, node_to_collect: NodeToCollect, info: PartialGraphBarrierInfo) {
        let epoch = info.barrier_info.epoch();
        assert_ne!(info.barrier_info.kind, BarrierKind::Initial);
        if info.post_collect_command.should_checkpoint() {
            assert!(info.barrier_info.kind.is_checkpoint());
        }
        self.barrier_item_collector
            .enqueue(epoch, node_to_collect, info);
        self.stat.observe_barrier_num(
            self.barrier_item_collector.inflight_barrier_num(),
            self.barrier_item_collector.collected_barrier_num(),
        );
    }

    fn collect(&mut self, resp: BarrierCompleteResponse) {
        debug!(
            epoch = resp.epoch,
            worker_id = %resp.worker_id,
            partial_graph_id = %resp.partial_graph_id,
            "collect barrier from worker"
        );
        self.barrier_item_collector
            .collect(resp.epoch, resp.worker_id, resp);
    }

    fn barrier_collected<'a>(
        &mut self,
        temp_ref: &'a CollectedBarrierTempRef,
    ) -> Option<CollectedBarrier<'a>> {
        if let Some((epoch, info)) = self.barrier_item_collector.barrier_collected() {
            self.stat
                .observe_barrier_latency(epoch, info.elapsed_secs());
            self.stat.observe_barrier_num(
                self.barrier_item_collector.inflight_barrier_num(),
                self.barrier_item_collector.collected_barrier_num(),
            );
            Some(temp_ref.collected_barrier(epoch, self.pending_barrier_num()))
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct ResetPartialGraphCollector {
    remaining_workers: HashSet<WorkerId>,
    reset_resps: HashMap<WorkerId, ResetPartialGraphResponse>,
}

impl ResetPartialGraphCollector {
    fn collect(&mut self, worker_id: WorkerId, resp: ResetPartialGraphResponse) -> bool {
        assert!(self.remaining_workers.remove(&worker_id));
        self.reset_resps
            .try_insert(worker_id, resp)
            .expect("non-duplicate");
        self.remaining_workers.is_empty()
    }
}

#[derive(Educe)]
#[educe(Debug)]
enum PartialGraphStatus {
    Running {
        term_id: String,
        state: PartialGraphRunningState,
    },
    Resetting(ResetPartialGraphCollector),
    Initializing {
        term_id: String,
        epoch: EpochPair,
        node_to_collect: NodeToCollect,
        #[educe(Debug(ignore))]
        stat: Option<Box<dyn PartialGraphStat>>,
    },
}

impl PartialGraphStatus {
    fn collect<'a>(
        &mut self,
        worker_id: WorkerId,
        resp: BarrierCompleteResponse,
        temp_ref: &'a CollectedBarrierTempRef,
    ) -> Option<PartialGraphEvent<'a>> {
        assert_eq!(worker_id, resp.worker_id);
        match self {
            PartialGraphStatus::Running { state, .. } => {
                state.collect(resp);
                state
                    .barrier_collected(temp_ref)
                    .map(PartialGraphEvent::BarrierCollected)
            }
            PartialGraphStatus::Resetting(_) => None,
            PartialGraphStatus::Initializing {
                term_id,
                epoch,
                node_to_collect,
                stat,
            } => {
                assert_eq!(epoch.prev, resp.epoch);
                assert!(node_to_collect.remove(&worker_id));
                if node_to_collect.is_empty() {
                    *self = PartialGraphStatus::Running {
                        term_id: take(term_id),
                        state: PartialGraphRunningState::new(
                            stat.take().expect("should be taken for once"),
                        ),
                    };
                    Some(PartialGraphEvent::Initialized)
                } else {
                    None
                }
            }
        }
    }
}

/// Holding the reference to a static empty map as a temporary workaround for borrow checker limitation on `CollectedBarrier`.
/// Mod local method can create a `CollectedBarrierTempRef` locally, and the created `CollectedBarrier` will temporarily reference
/// to the `CollectedBarrierTempRef`. The method must fill in the correct reference before returning in a mod-pub method.
///
/// This struct *must* be private to ensure that the invalid lifetime won't be leaked outside.
struct CollectedBarrierTempRef {
    resps: &'static HashMap<WorkerId, BarrierCompleteResponse>,
}

impl CollectedBarrierTempRef {
    fn new() -> Self {
        static EMPTY: std::sync::LazyLock<HashMap<WorkerId, BarrierCompleteResponse>> =
            std::sync::LazyLock::new(HashMap::new);
        CollectedBarrierTempRef { resps: &EMPTY }
    }

    fn collected_barrier(
        &self,
        epoch: EpochPair,
        pending_barrier_num: usize,
    ) -> CollectedBarrier<'_> {
        CollectedBarrier {
            epoch,
            resps: self.resps,
            pending_barrier_num,
        }
    }

    fn correct_lifetime<'a>(
        &self,
        event: PartialGraphManagerEvent<'_>,
        manager: &'a PartialGraphManager,
    ) -> PartialGraphManagerEvent<'a> {
        match event {
            PartialGraphManagerEvent::PartialGraph(partial_graph_id, event) => {
                let event = match event {
                    PartialGraphEvent::BarrierCollected(collected) => {
                        let pending_barrier_num = collected.pending_barrier_num;
                        let state = manager.running_graph(partial_graph_id);
                        let (epoch, resps, _) = state
                            .barrier_item_collector
                            .last_collected()
                            .expect("should exist");
                        assert_eq!(epoch, collected.epoch);
                        PartialGraphEvent::BarrierCollected(CollectedBarrier {
                            epoch,
                            resps,
                            pending_barrier_num,
                        })
                    }
                    PartialGraphEvent::Reset(resps) => PartialGraphEvent::Reset(resps),
                    PartialGraphEvent::Initialized => PartialGraphEvent::Initialized,
                    PartialGraphEvent::Error(worker_id) => PartialGraphEvent::Error(worker_id),
                };
                PartialGraphManagerEvent::PartialGraph(partial_graph_id, event)
            }
            PartialGraphManagerEvent::Worker(worker_id, event) => {
                PartialGraphManagerEvent::Worker(worker_id, event)
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct CollectedBarrier<'a> {
    pub epoch: EpochPair,
    pub resps: &'a HashMap<WorkerId, BarrierCompleteResponse>,
    pub pending_barrier_num: usize,
}

pub(super) enum PartialGraphEvent<'a> {
    BarrierCollected(CollectedBarrier<'a>),
    Reset(HashMap<WorkerId, ResetPartialGraphResponse>),
    Initialized,
    Error(WorkerId),
}

pub(super) enum WorkerEvent {
    WorkerError {
        err: MetaError,
        affected_partial_graphs: HashSet<PartialGraphId>,
    },
    WorkerConnected,
}

fn existing_graphs(
    graphs: &HashMap<PartialGraphId, PartialGraphStatus>,
) -> impl Iterator<Item = (PartialGraphId, &str)> + '_ {
    graphs
        .iter()
        .filter_map(|(partial_graph_id, status)| match status {
            PartialGraphStatus::Running { term_id, .. }
            | PartialGraphStatus::Initializing { term_id, .. } => {
                Some((*partial_graph_id, term_id.as_str()))
            }
            PartialGraphStatus::Resetting(_) => None,
        })
}

pub(super) struct PartialGraphManager {
    control_stream_manager: ControlStreamManager,
    graphs: HashMap<PartialGraphId, PartialGraphStatus>,
}

impl PartialGraphManager {
    pub(super) fn uninitialized(env: MetaSrvEnv) -> Self {
        Self {
            control_stream_manager: ControlStreamManager::new(env),
            graphs: HashMap::new(),
        }
    }

    pub(super) async fn recover(
        env: MetaSrvEnv,
        nodes: &HashMap<WorkerId, WorkerNode>,
        context: Arc<impl GlobalBarrierWorkerContext>,
    ) -> Self {
        let control_stream_manager = ControlStreamManager::recover(env, nodes, context).await;
        Self {
            control_stream_manager,
            graphs: Default::default(),
        }
    }

    pub(super) fn control_stream_manager(&self) -> &ControlStreamManager {
        &self.control_stream_manager
    }

    pub(super) async fn add_worker(
        &mut self,
        node: WorkerNode,
        context: Arc<impl GlobalBarrierWorkerContext>,
    ) {
        self.control_stream_manager
            .add_worker(node, existing_graphs(&self.graphs), context)
            .await
    }

    pub(super) fn remove_worker(&mut self, node: WorkerNode) {
        self.control_stream_manager.remove_worker(node);
    }

    pub(super) fn clear_worker(&mut self) {
        self.control_stream_manager.clear();
    }

    pub(crate) fn notify_all_err(&mut self, err: &MetaError) {
        for (_, graph) in self.graphs.drain() {
            if let PartialGraphStatus::Running { state, .. } = graph {
                for info in state.barrier_item_collector.into_infos() {
                    if let Some(notifier) = info.notifier {
                        notifier.notify_collection_failed(err.clone());
                    }
                }
            }
        }
    }
}

#[must_use]
pub(super) struct PartialGraphAdder<'a> {
    partial_graph_id: PartialGraphId,
    manager: &'a mut PartialGraphManager,
    consumed: bool,
}

impl PartialGraphAdder<'_> {
    pub(super) fn added(mut self) {
        self.consumed = true;
    }

    pub(super) fn failed(mut self) {
        self.manager.reset_partial_graphs([self.partial_graph_id]);
        self.consumed = true;
    }

    pub(super) fn manager(&mut self) -> &mut PartialGraphManager {
        self.manager
    }
}

impl Drop for PartialGraphAdder<'_> {
    fn drop(&mut self) {
        debug_assert!(self.consumed, "unconsumed graph adder");
        if !self.consumed {
            warn!(partial_graph_id = %self.partial_graph_id, "unconsumed graph adder");
        }
    }
}

impl PartialGraphManager {
    pub(super) fn add_partial_graph(
        &mut self,
        partial_graph_id: PartialGraphId,
        term_id: &str,
        stat: impl PartialGraphStat,
    ) -> PartialGraphAdder<'_> {
        self.graphs
            .try_insert(
                partial_graph_id,
                PartialGraphStatus::Running {
                    term_id: term_id.to_owned(),
                    state: PartialGraphRunningState::new(Box::new(stat)),
                },
            )
            .expect("non-duplicated");
        self.control_stream_manager
            .add_partial_graph(partial_graph_id, term_id);
        PartialGraphAdder {
            partial_graph_id,
            manager: self,
            consumed: false,
        }
    }

    pub(super) fn remove_partial_graphs(&mut self, partial_graphs: Vec<PartialGraphId>) {
        for partial_graph_id in &partial_graphs {
            let graph = self.graphs.remove(partial_graph_id).expect("should exist");
            let PartialGraphStatus::Running { state, .. } = graph else {
                panic!("graph to be explicitly removed should be running");
            };
            assert!(state.is_empty());
        }
        self.control_stream_manager
            .remove_partial_graphs(partial_graphs);
    }

    pub(super) fn reset_partial_graphs(
        &mut self,
        partial_graph_ids: impl IntoIterator<Item = PartialGraphId>,
    ) {
        let partial_graph_ids = partial_graph_ids.into_iter().collect_vec();
        let remaining_workers = self
            .control_stream_manager
            .reset_partial_graphs(partial_graph_ids.clone());
        let new_collector = || ResetPartialGraphCollector {
            remaining_workers: remaining_workers.clone(),
            reset_resps: Default::default(),
        };
        for partial_graph_id in partial_graph_ids {
            match self.graphs.entry(partial_graph_id) {
                Entry::Vacant(entry) => {
                    entry.insert(PartialGraphStatus::Resetting(new_collector()));
                }
                Entry::Occupied(mut entry) => {
                    let graph = entry.get_mut();
                    match graph {
                        PartialGraphStatus::Resetting(_) => {
                            unreachable!("should not reset again")
                        }
                        PartialGraphStatus::Running { .. }
                        | PartialGraphStatus::Initializing { .. } => {
                            *graph = PartialGraphStatus::Resetting(new_collector());
                        }
                    }
                }
            }
        }
    }

    pub(super) fn assert_resetting(&self, partial_graph_id: PartialGraphId) {
        let graph = self.graphs.get(&partial_graph_id).expect("should exist");
        let PartialGraphStatus::Resetting(..) = graph else {
            panic!("should be at resetting but at {:?}", graph);
        };
    }

    pub(super) fn inject_barrier(
        &mut self,
        partial_graph_id: PartialGraphId,
        mutation: Option<Mutation>,
        compaction_context: Option<IcebergPkIndexCompactionContext>,
        node_actors: &HashMap<WorkerId, HashSet<ActorId>>,
        table_ids_to_sync: impl Iterator<Item = TableId>,
        nodes_to_sync_table: impl Iterator<Item = WorkerId>,
        new_actors: Option<StreamJobActorsToCreate>,
        info: PartialGraphBarrierInfo,
    ) -> MetaResult<()> {
        let graph = self
            .graphs
            .get_mut(&partial_graph_id)
            .expect("should exist");
        let node_to_collect = self.control_stream_manager.inject_barrier(
            partial_graph_id,
            mutation,
            compaction_context,
            &info.barrier_info,
            node_actors,
            table_ids_to_sync,
            nodes_to_sync_table,
            new_actors,
        )?;
        let PartialGraphStatus::Running { state, .. } = graph else {
            panic!("should not inject barrier on non-running status: {graph:?}")
        };
        state.enqueue(node_to_collect, info);
        Ok(())
    }

    fn running_graph(&self, partial_graph_id: PartialGraphId) -> &PartialGraphRunningState {
        let PartialGraphStatus::Running { state: graph, .. } = &self.graphs[&partial_graph_id]
        else {
            unreachable!("should be running")
        };
        graph
    }

    fn running_graph_mut(
        &mut self,
        partial_graph_id: PartialGraphId,
    ) -> &mut PartialGraphRunningState {
        let PartialGraphStatus::Running { state: graph, .. } = self
            .graphs
            .get_mut(&partial_graph_id)
            .expect("should exist")
        else {
            unreachable!("should be running")
        };
        graph
    }

    pub(super) fn pending_barrier_num(&self, partial_graph_id: PartialGraphId) -> usize {
        self.running_graph(partial_graph_id).pending_barrier_num()
    }

    pub(super) fn first_inflight_barrier(
        &self,
        partial_graph_id: PartialGraphId,
    ) -> Option<EpochPair> {
        self.running_graph(partial_graph_id)
            .barrier_item_collector
            .first_inflight_epoch()
    }

    pub(super) fn pending_barrier_infos(
        &self,
        partial_graph_id: PartialGraphId,
    ) -> impl Iterator<Item = &BarrierInfo> {
        self.running_graph(partial_graph_id)
            .barrier_item_collector
            .iter_infos()
            .map(|info| &info.barrier_info)
    }

    pub(super) fn start_completing(
        &mut self,
        partial_graph_id: PartialGraphId,
        epoch_end_bound: Bound<u64>,
        mut on_non_checkpoint_epoch: impl FnMut(
            EpochPair,
            HashMap<WorkerId, BarrierCompleteResponse>,
            PostCollectCommand,
        ),
    ) -> Option<(
        u64,
        HashMap<WorkerId, BarrierCompleteResponse>,
        PartialGraphBarrierInfo,
    )> {
        let graph = self.running_graph_mut(partial_graph_id);
        assert!(graph.completing_epoch.is_none());
        let epoch_range: (Bound<u64>, Bound<u64>) = (Unbounded, epoch_end_bound);
        while let Some((epoch, resps, info)) = graph
            .barrier_item_collector
            .take_collected_if(|epoch| epoch_range.contains(&epoch.prev))
        {
            if info.post_collect_command.should_checkpoint() {
                assert!(info.barrier_info.kind.is_checkpoint());
            } else if !info.barrier_info.kind.is_checkpoint() {
                if let Some(notifier) = info.notifier {
                    notifier.notify_collected();
                }
                on_non_checkpoint_epoch(epoch, resps, info.post_collect_command);
                continue;
            }
            let prev_epoch = info.barrier_info.prev_epoch();
            graph.completing_epoch = Some(prev_epoch);
            return Some((prev_epoch, resps, info));
        }
        None
    }

    pub(super) fn ack_completed(&mut self, partial_graph_id: PartialGraphId, prev_epoch: u64) {
        assert_eq!(
            self.running_graph_mut(partial_graph_id)
                .completing_epoch
                .take(),
            Some(prev_epoch)
        );
    }

    pub(super) fn has_pending_checkpoint_barrier(&self, partial_graph_id: PartialGraphId) -> bool {
        self.running_graph(partial_graph_id)
            .barrier_item_collector
            .iter_infos()
            .any(|info| info.barrier_info.kind.is_checkpoint())
    }

    pub(super) fn start_recover(&mut self) -> PartialGraphRecoverer<'_> {
        PartialGraphRecoverer {
            added_partial_graphs: Default::default(),
            manager: self,
            consumed: false,
        }
    }
}

#[must_use]
pub(super) struct PartialGraphRecoverer<'a> {
    added_partial_graphs: HashSet<PartialGraphId>,
    manager: &'a mut PartialGraphManager,
    consumed: bool,
}

impl PartialGraphRecoverer<'_> {
    pub(super) fn recover_graph(
        &mut self,
        partial_graph_id: PartialGraphId,
        term_id: &str,
        mutation: Mutation,
        barrier_info: &BarrierInfo,
        node_actors: &HashMap<WorkerId, HashSet<ActorId>>,
        table_ids_to_sync: impl Iterator<Item = TableId>,
        new_actors: StreamJobActorsToCreate,
        stat: impl PartialGraphStat,
    ) -> MetaResult<()> {
        assert!(
            self.added_partial_graphs.insert(partial_graph_id),
            "duplicated recover graph {partial_graph_id}"
        );
        self.manager
            .control_stream_manager
            .add_partial_graph(partial_graph_id, term_id);
        assert!(barrier_info.kind.is_initial());
        let node_to_collect = self.manager.control_stream_manager.inject_barrier(
            partial_graph_id,
            Some(mutation),
            None,
            barrier_info,
            node_actors,
            table_ids_to_sync,
            node_actors.keys().copied(),
            Some(new_actors),
        )?;
        self.manager
            .graphs
            .try_insert(
                partial_graph_id,
                PartialGraphStatus::Initializing {
                    term_id: term_id.to_owned(),
                    epoch: barrier_info.epoch(),
                    node_to_collect,
                    stat: Some(Box::new(stat)),
                },
            )
            .expect("non-duplicated");
        Ok(())
    }

    pub(super) fn control_stream_manager(&self) -> &ControlStreamManager {
        &self.manager.control_stream_manager
    }

    pub(super) fn all_initializing(mut self) -> HashSet<PartialGraphId> {
        self.consumed = true;
        take(&mut self.added_partial_graphs)
    }

    pub(super) fn failed(mut self) -> HashSet<PartialGraphId> {
        self.manager
            .reset_partial_graphs(self.added_partial_graphs.iter().copied());
        self.consumed = true;
        take(&mut self.added_partial_graphs)
    }
}

impl Drop for PartialGraphRecoverer<'_> {
    fn drop(&mut self) {
        debug_assert!(self.consumed, "unconsumed graph recoverer");
        if !self.consumed {
            warn!(partial_graph_ids = ?self.added_partial_graphs, "unconsumed graph recoverer");
        }
    }
}

#[must_use]
pub(super) enum PartialGraphManagerEvent<'a> {
    PartialGraph(PartialGraphId, PartialGraphEvent<'a>),
    Worker(WorkerId, WorkerEvent),
}

impl PartialGraphManager {
    pub(super) async fn next_event<'a>(
        &'a mut self,
        context: &Arc<impl GlobalBarrierWorkerContext>,
    ) -> PartialGraphManagerEvent<'a> {
        let temp_ref = CollectedBarrierTempRef::new();
        let event = self.next_event_inner(context, &temp_ref).await;
        temp_ref.correct_lifetime(event, self)
    }

    async fn next_event_inner<'a>(
        &mut self,
        context: &Arc<impl GlobalBarrierWorkerContext>,
        temp_ref: &'a CollectedBarrierTempRef,
    ) -> PartialGraphManagerEvent<'a> {
        for (&partial_graph_id, graph) in &mut self.graphs {
            match graph {
                PartialGraphStatus::Running { state, .. } => {
                    if let Some(collected) = state.barrier_collected(temp_ref) {
                        return PartialGraphManagerEvent::PartialGraph(
                            partial_graph_id,
                            PartialGraphEvent::BarrierCollected(collected),
                        );
                    }
                }
                PartialGraphStatus::Resetting(collector) => {
                    if collector.remaining_workers.is_empty() {
                        let resps = take(&mut collector.reset_resps);
                        self.graphs.remove(&partial_graph_id);
                        return PartialGraphManagerEvent::PartialGraph(
                            partial_graph_id,
                            PartialGraphEvent::Reset(resps),
                        );
                    }
                }
                PartialGraphStatus::Initializing {
                    term_id,
                    node_to_collect,
                    stat,
                    ..
                } => {
                    if node_to_collect.is_empty() {
                        *graph = PartialGraphStatus::Running {
                            term_id: take(term_id),
                            state: PartialGraphRunningState::new(
                                stat.take().expect("should be taken once"),
                            ),
                        };
                        return PartialGraphManagerEvent::PartialGraph(
                            partial_graph_id,
                            PartialGraphEvent::Initialized,
                        );
                    }
                }
            }
        }
        loop {
            let (worker_id, event) = self.control_stream_manager.next_event(context).await;
            match event {
                WorkerNodeEvent::Response(result) => match result {
                    Ok(resp) => match resp {
                        Response::CompleteBarrier(resp) => {
                            let partial_graph_id = resp.partial_graph_id;
                            if let Some(event) = self
                                .graphs
                                .get_mut(&partial_graph_id)
                                .expect("should exist")
                                .collect(worker_id, resp, temp_ref)
                            {
                                return PartialGraphManagerEvent::PartialGraph(
                                    partial_graph_id,
                                    event,
                                );
                            }
                        }
                        Response::ReportPartialGraphFailure(resp) => {
                            let partial_graph_id = resp.partial_graph_id;
                            let graph = self
                                .graphs
                                .get_mut(&partial_graph_id)
                                .expect("should exist");
                            match graph {
                                PartialGraphStatus::Resetting(_) => {
                                    // ignore reported error when resetting
                                }
                                PartialGraphStatus::Running { .. }
                                | PartialGraphStatus::Initializing { .. } => {
                                    return PartialGraphManagerEvent::PartialGraph(
                                        partial_graph_id,
                                        PartialGraphEvent::Error(worker_id),
                                    );
                                }
                            }
                        }
                        Response::ResetPartialGraph(resp) => {
                            let partial_graph_id = resp.partial_graph_id;
                            let graph = self
                                .graphs
                                .get_mut(&partial_graph_id)
                                .expect("should exist");
                            match graph {
                                PartialGraphStatus::Running { .. }
                                | PartialGraphStatus::Initializing { .. } => {
                                    if cfg!(debug_assertions) {
                                        unreachable!(
                                            "should not have reset request when not in resetting state"
                                        )
                                    } else {
                                        warn!(
                                            ?resp,
                                            "ignore reset resp when not in Resetting state"
                                        );
                                    }
                                }
                                PartialGraphStatus::Resetting(collector) => {
                                    if collector.collect(worker_id, resp) {
                                        let resps = take(&mut collector.reset_resps);
                                        self.graphs.remove(&partial_graph_id);
                                        return PartialGraphManagerEvent::PartialGraph(
                                            partial_graph_id,
                                            PartialGraphEvent::Reset(resps),
                                        );
                                    }
                                }
                            }
                        }
                        Response::Init(_) | Response::Shutdown(_) => {
                            unreachable!("should be handled in control stream manager")
                        }
                    },
                    Err(error) => {
                        let affected_partial_graphs = self
                            .graphs
                            .iter_mut()
                            .filter_map(|(partial_graph_id, graph)| match graph {
                                PartialGraphStatus::Running { state, .. } => state
                                    .barrier_item_collector
                                    .iter_to_collect()
                                    .any(|to_collect| {
                                        !is_valid_after_worker_err(to_collect, worker_id)
                                    })
                                    .then_some(*partial_graph_id),
                                PartialGraphStatus::Resetting(collector) => {
                                    collector.remaining_workers.remove(&worker_id);
                                    None
                                }
                                PartialGraphStatus::Initializing {
                                    node_to_collect, ..
                                } => (!is_valid_after_worker_err(node_to_collect, worker_id))
                                    .then_some(*partial_graph_id),
                            })
                            .collect();
                        return PartialGraphManagerEvent::Worker(
                            worker_id,
                            WorkerEvent::WorkerError {
                                err: error,
                                affected_partial_graphs,
                            },
                        );
                    }
                },
                WorkerNodeEvent::Connected(connected) => {
                    connected.initialize(existing_graphs(&self.graphs));
                    return PartialGraphManagerEvent::Worker(
                        worker_id,
                        WorkerEvent::WorkerConnected,
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::util::epoch::Epoch;

    use super::*;
    use crate::barrier::TracedEpoch;

    struct TestPartialGraphStat;

    impl PartialGraphStat for TestPartialGraphStat {
        fn observe_barrier_latency(&self, _epoch: EpochPair, _barrier_latency_secs: f64) {}

        fn observe_barrier_num(&self, _inflight_barrier_num: usize, _collected_barrier_num: usize) {
        }
    }

    fn barrier_info(prev_epoch: u64, curr_epoch: u64) -> PartialGraphBarrierInfo {
        PartialGraphBarrierInfo::new(
            PostCollectCommand::barrier(),
            BarrierInfo {
                prev_epoch: TracedEpoch::new(Epoch(prev_epoch)),
                curr_epoch: TracedEpoch::new(Epoch(curr_epoch)),
                kind: BarrierKind::Barrier,
            },
            None,
            HashSet::new(),
        )
    }

    #[test]
    fn test_pending_barrier_num_includes_collected_and_completing() {
        let mut state = PartialGraphRunningState::new(Box::new(TestPartialGraphStat));
        let worker_id: WorkerId = 1.into();
        state.barrier_item_collector.enqueue(
            EpochPair::new(2, 1),
            HashSet::from([worker_id]),
            barrier_info(1, 2),
        );
        state.barrier_item_collector.enqueue(
            EpochPair::new(3, 2),
            HashSet::from([worker_id]),
            barrier_info(2, 3),
        );
        assert_eq!(state.pending_barrier_num(), 2);

        state
            .barrier_item_collector
            .collect(1, worker_id, BarrierCompleteResponse::default());
        state.barrier_item_collector.barrier_collected();
        assert_eq!(state.pending_barrier_num(), 2);

        state
            .barrier_item_collector
            .take_collected_if(|epoch| epoch.prev == 1)
            .expect("the first barrier should be collected");
        state.completing_epoch = Some(1);
        assert_eq!(state.pending_barrier_num(), 2);
    }
}
