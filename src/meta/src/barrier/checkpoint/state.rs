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

use std::assert_matches;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::sync::atomic::AtomicU32;

use risingwave_common::bail;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VnodeCountCompat;
use risingwave_common::id::JobId;
use risingwave_common::util::epoch::Epoch;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_meta_model::{DispatcherType, WorkerId, streaming_job};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
use risingwave_pb::stream_plan::barrier_mutation::{Mutation, PbMutation};
use risingwave_pb::stream_plan::{
    AddMutation, Dispatchers, PbStartFragmentBackfillMutation, PbSubscriptionUpstreamInfo,
    PbUpdateMutation, PbUpstreamSinkInfo,
};
use tracing::warn;

use crate::barrier::cdc_progress::CdcTableBackfillTracker;
use crate::barrier::checkpoint::{
    BatchRefreshJobCheckpointControl, BatchRefreshLogicalFragments, CreatingStreamingJobControl,
    DatabaseCheckpointControl, IndependentCheckpointJobControl,
};
use crate::barrier::command::{
    CreateStreamingJobCommandInfo, PostCollectCommand, ReschedulePlan, ThrottleConfigMap,
};
use crate::barrier::context::CreateSnapshotBackfillJobCommandInfo;
use crate::barrier::edge_builder::{EdgeBuilderFragmentInfo, FragmentEdgeBuilder};
use crate::barrier::info::{
    BarrierInfo, CreateStreamingJobStatus, InflightDatabaseInfo, InflightStreamingJobInfo,
    SubscriberType,
};
use crate::barrier::notifier::NotifierStarter;
use crate::barrier::partial_graph::{PartialGraphBarrierInfo, PartialGraphManager};
use crate::barrier::rpc::to_partial_graph_id;
use crate::barrier::{BarrierKind, Command, CreateStreamingJobType, TracedEpoch};
use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::controller::scale::{
    ComponentFragmentAligner, EnsembleActorTemplate, LoadedFragment, NoShuffleEnsemble,
    build_no_shuffle_fragment_graph_edges, find_no_shuffle_graphs,
};
use crate::model::{
    ActorId, ActorNewNoShuffle, FragmentDownstreamRelation, FragmentId, StreamActor, StreamContext,
    StreamJobActorsToCreate, StreamJobFragmentsToCreate,
};
use crate::stream::cdc::parallel_cdc_table_backfill_fragment;
use crate::stream::{
    GlobalActorIdGen, ReplaceJobSplitPlan, SourceManager, SplitAssignment,
    fill_snapshot_backfill_epoch,
};
use crate::{MetaError, MetaResult};

/// The latest state of `GlobalBarrierWorker` after injecting the latest barrier.
pub(in crate::barrier) struct BarrierWorkerState {
    /// The last sent `prev_epoch`
    ///
    /// There's no need to persist this field. On recovery, we will restore this from the latest
    /// committed snapshot in `HummockManager`.
    in_flight_prev_epoch: TracedEpoch,

    /// The `prev_epoch` of pending non checkpoint barriers
    pending_non_checkpoint_barriers: Vec<u64>,

    /// Whether the cluster is paused.
    is_paused: bool,
}

impl BarrierWorkerState {
    pub(super) fn new() -> Self {
        Self {
            in_flight_prev_epoch: TracedEpoch::new(Epoch::now()),
            pending_non_checkpoint_barriers: vec![],
            is_paused: false,
        }
    }

    pub fn recovery(in_flight_prev_epoch: TracedEpoch, is_paused: bool) -> Self {
        Self {
            in_flight_prev_epoch,
            pending_non_checkpoint_barriers: vec![],
            is_paused,
        }
    }

    pub fn is_paused(&self) -> bool {
        self.is_paused
    }

    fn set_is_paused(&mut self, is_paused: bool) {
        if self.is_paused != is_paused {
            tracing::info!(
                currently_paused = self.is_paused,
                newly_paused = is_paused,
                "update paused state"
            );
            self.is_paused = is_paused;
        }
    }

    pub fn in_flight_prev_epoch(&self) -> &TracedEpoch {
        &self.in_flight_prev_epoch
    }

    /// Returns the `BarrierInfo` for the next barrier, and updates the state.
    pub fn next_barrier_info(
        &mut self,
        is_checkpoint: bool,
        curr_epoch: TracedEpoch,
    ) -> BarrierInfo {
        assert!(
            self.in_flight_prev_epoch.value() < curr_epoch.value(),
            "curr epoch regress. {} > {}",
            self.in_flight_prev_epoch.value(),
            curr_epoch.value()
        );
        let prev_epoch = self.in_flight_prev_epoch.clone();
        self.in_flight_prev_epoch = curr_epoch.clone();
        self.pending_non_checkpoint_barriers
            .push(prev_epoch.value().0);
        let kind = if is_checkpoint {
            let epochs = take(&mut self.pending_non_checkpoint_barriers);
            BarrierKind::Checkpoint(epochs)
        } else {
            BarrierKind::Barrier
        };
        BarrierInfo {
            prev_epoch,
            curr_epoch,
            kind,
        }
    }
}

pub(super) struct ApplyCommandInfo {
    pub jobs_to_wait: HashSet<JobId>,
}

/// Result tuple of `apply_command`: mutation, table IDs to commit, actors to create,
/// node actors, and post-collect command.
type ApplyCommandResult = (
    Option<Mutation>,
    HashSet<TableId>,
    Option<StreamJobActorsToCreate>,
    HashMap<WorkerId, HashSet<ActorId>>,
    PostCollectCommand,
);

/// Result of actor rendering for a create/replace streaming job.
pub(crate) struct RenderResult {
    /// Rendered actors grouped by fragment.
    pub stream_actors: HashMap<FragmentId, Vec<StreamActor>>,
    /// Worker placement for each actor.
    pub actor_location: HashMap<ActorId, WorkerId>,
}

/// Derive `NoShuffle` edges from fragment downstream relations and resolve ensembles.
///
/// This scans both the internal downstream relations (`fragments.downstreams`) and
/// the cross-boundary upstream-to-new-fragment relations (`upstream_fragment_downstreams`)
/// to find all `NoShuffle` edges. It then runs BFS to find connected components (ensembles)
/// and categorizes them into:
/// - Ensembles whose entry fragments include existing (non-new) fragments
/// - Ensembles whose entry fragments are all newly created
pub(crate) fn resolve_no_shuffle_ensembles(
    fragments: &StreamJobFragmentsToCreate,
    upstream_fragment_downstreams: &FragmentDownstreamRelation,
) -> MetaResult<Vec<NoShuffleEnsemble>> {
    // Derive FragmentNewNoShuffle from the two downstream relation maps.
    let mut new_no_shuffle: HashMap<_, HashSet<_>> = HashMap::new();

    // Internal edges (new → new) and edges from new → existing downstream (replace job).
    for (upstream_fid, relations) in &fragments.downstreams {
        for rel in relations {
            if rel.dispatcher_type == DispatcherType::NoShuffle {
                new_no_shuffle
                    .entry(*upstream_fid)
                    .or_default()
                    .insert(rel.downstream_fragment_id);
            }
        }
    }

    // Cross-boundary edges: existing upstream → new downstream.
    for (upstream_fid, relations) in upstream_fragment_downstreams {
        for rel in relations {
            if rel.dispatcher_type == DispatcherType::NoShuffle {
                new_no_shuffle
                    .entry(*upstream_fid)
                    .or_default()
                    .insert(rel.downstream_fragment_id);
            }
        }
    }

    let mut ensembles = if new_no_shuffle.is_empty() {
        Vec::new()
    } else {
        // Flatten into directed edge pairs for BFS.
        let no_shuffle_edges: Vec<(FragmentId, FragmentId)> = new_no_shuffle
            .iter()
            .flat_map(|(upstream_fid, downstream_fids)| {
                downstream_fids
                    .iter()
                    .map(move |downstream_fid| (*upstream_fid, *downstream_fid))
            })
            .collect();

        let all_fragment_ids: Vec<FragmentId> = no_shuffle_edges
            .iter()
            .flat_map(|(u, d)| [*u, *d])
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let (fwd, bwd) = build_no_shuffle_fragment_graph_edges(no_shuffle_edges);
        find_no_shuffle_graphs(&all_fragment_ids, &fwd, &bwd)?
    };

    // Add standalone fragments (not covered by any ensemble) as single-fragment ensembles.
    let covered: HashSet<FragmentId> = ensembles
        .iter()
        .flat_map(|e| e.component_fragments())
        .collect();
    for fragment_id in fragments.inner.fragments.keys() {
        if !covered.contains(fragment_id) {
            ensembles.push(NoShuffleEnsemble::singleton(*fragment_id));
        }
    }

    Ok(ensembles)
}

/// Render actors for a create or replace streaming job.
///
/// This determines the parallelism for each no-shuffle ensemble (either from an existing
/// inflight upstream or computed fresh), and produces `StreamActor` instances with worker
/// placements and actor-level no-shuffle mappings.
///
/// The process follows three steps:
/// 1. For each ensemble, resolve `EnsembleActorTemplate` (from existing or fresh).
/// 2. For each new component fragment, allocate actor IDs and compute worker/vnode assignments.
/// 3. Expand the simple assignments into full `StreamActor` structures.
pub(super) fn render_actors(
    fragments: &StreamJobFragmentsToCreate,
    database_info: &InflightDatabaseInfo,
    definition: &str,
    ctx: &StreamContext,
    streaming_job_model: &streaming_job::Model,
    actor_id_counter: &AtomicU32,
    worker_map: &HashMap<WorkerId, WorkerNode>,
    ensembles: &[NoShuffleEnsemble],
    database_resource_group: &str,
) -> MetaResult<RenderResult> {
    // Step 2: Render actors for each ensemble.
    // For each new fragment, produce a simple assignment: actor_id -> (worker_id, vnode_bitmap).
    let mut actor_assignments: HashMap<FragmentId, HashMap<ActorId, (WorkerId, Option<Bitmap>)>> =
        HashMap::new();

    for ensemble in ensembles {
        // Determine the EnsembleActorTemplate for this ensemble.
        //
        // Check if any component fragment in the ensemble already exists (i.e. is inflight).
        // If so, derive the actor assignment from an existing fragment. Otherwise render fresh.
        let existing_fragment_ids: Vec<FragmentId> = ensemble
            .component_fragments()
            .filter(|fragment_id| !fragments.inner.fragments.contains_key(fragment_id))
            .collect();

        let actor_template = if let Some(&first_existing) = existing_fragment_ids.first() {
            let template = EnsembleActorTemplate::from_existing_inflight_fragment(
                database_info.fragment(first_existing),
            );

            // Sanity check: all existing fragments in the same ensemble must be aligned —
            // same actor count and same worker placement per vnode.
            for &other_fragment_id in &existing_fragment_ids[1..] {
                let other = EnsembleActorTemplate::from_existing_inflight_fragment(
                    database_info.fragment(other_fragment_id),
                );
                template.assert_aligned_with(&other, first_existing, other_fragment_id);
            }

            template
        } else {
            // All fragments are new — render from scratch.
            let first_component = ensemble
                .component_fragments()
                .next()
                .expect("ensemble must have at least one component");
            let fragment = &fragments.inner.fragments[&first_component];
            let distribution_type: DistributionType = fragment.distribution_type.into();
            let vnode_count = fragment.vnode_count();

            // Assert all component fragments in this ensemble share the same vnode count.
            for fragment_id in ensemble.component_fragments() {
                let f = &fragments.inner.fragments[&fragment_id];
                assert_eq!(
                    vnode_count,
                    f.vnode_count(),
                    "component fragments {} and {} in the same no-shuffle ensemble have \
                     different vnode counts: {} vs {}",
                    first_component,
                    fragment_id,
                    vnode_count,
                    f.vnode_count(),
                );
            }

            EnsembleActorTemplate::render_new(
                streaming_job_model,
                worker_map,
                None,
                database_resource_group.to_owned(),
                distribution_type,
                vnode_count,
            )?
        };

        // Render each new component fragment in this ensemble.
        for fragment_id in ensemble.component_fragments() {
            if !fragments.inner.fragments.contains_key(&fragment_id) {
                continue; // Skip existing fragments.
            }
            let fragment = &fragments.inner.fragments[&fragment_id];
            let distribution_type: DistributionType = fragment.distribution_type.into();
            let aligner =
                ComponentFragmentAligner::new_persistent(&actor_template, actor_id_counter);
            let assignments = aligner.align_component_actor(distribution_type);
            actor_assignments.insert(fragment_id, assignments);
        }
    }

    // Step 3: Expand simple assignments into full StreamActor structures.
    let mut result_stream_actors: HashMap<FragmentId, Vec<StreamActor>> = HashMap::new();
    let mut result_actor_location: HashMap<ActorId, WorkerId> = HashMap::new();

    for (fragment_id, assignments) in &actor_assignments {
        let mut actors = Vec::with_capacity(assignments.len());
        for (&actor_id, (worker_id, vnode_bitmap)) in assignments {
            result_actor_location.insert(actor_id, *worker_id);
            actors.push(StreamActor {
                actor_id,
                fragment_id: *fragment_id,
                vnode_bitmap: vnode_bitmap.clone(),
                mview_definition: definition.to_owned(),
                expr_context: Some(ctx.to_expr_context()),
                config_override: ctx.config_override.clone(),
            });
        }
        result_stream_actors.insert(*fragment_id, actors);
    }

    Ok(RenderResult {
        stream_actors: result_stream_actors,
        actor_location: result_actor_location,
    })
}
impl DatabaseCheckpointControl {
    /// Collect table IDs to commit and actor IDs to collect from current fragment infos.
    fn collect_base_info(&self) -> (HashSet<TableId>, HashMap<WorkerId, HashSet<ActorId>>) {
        let table_ids_to_commit = self.database_info.existing_table_ids().collect();
        let node_actors =
            InflightFragmentInfo::actor_ids_to_collect(self.database_info.fragment_infos());
        (table_ids_to_commit, node_actors)
    }

    /// Helper for the simplest command variants: those that only need a
    /// pre-computed mutation and a command name, with no actors to create
    /// and no additional side effects on `self`.
    fn apply_simple_command(
        &self,
        mutation: Option<Mutation>,
        command_name: &'static str,
    ) -> ApplyCommandResult {
        let (table_ids, node_actors) = self.collect_base_info();
        (
            mutation,
            table_ids,
            None,
            node_actors,
            PostCollectCommand::Command(command_name.to_owned()),
        )
    }

    /// Returns the inflight actor infos that have included the newly added actors in the given command. The dropped actors
    /// will be removed from the state after the info get resolved.
    pub(super) fn apply_command(
        &mut self,
        command: Option<Command>,
        notifier: &mut Option<NotifierStarter>,
        barrier_info: BarrierInfo,
        partial_graph_manager: &mut PartialGraphManager,
        hummock_version_stats: &HummockVersionStats,
        worker_nodes: &HashMap<WorkerId, WorkerNode>,
    ) -> MetaResult<ApplyCommandInfo> {
        debug_assert!(
            !matches!(
                command,
                Some(Command::RescheduleIntent {
                    reschedule_plan: None,
                    ..
                })
            ),
            "reschedule intent must be resolved before apply"
        );
        if matches!(
            command,
            Some(Command::RescheduleIntent {
                reschedule_plan: None,
                ..
            })
        ) {
            bail!("reschedule intent must be resolved before apply");
        }

        /// Resolve source splits for a create streaming job command.
        ///
        /// Combines source fragment split resolution and backfill split alignment
        /// into one step, looking up existing upstream actor splits from the inflight database info.
        fn resolve_source_splits(
            info: &CreateStreamingJobCommandInfo,
            render_result: &RenderResult,
            actor_no_shuffle: &ActorNewNoShuffle,
            database_info: &InflightDatabaseInfo,
        ) -> MetaResult<SplitAssignment> {
            let fragment_actor_ids: HashMap<FragmentId, Vec<ActorId>> = render_result
                .stream_actors
                .iter()
                .map(|(fragment_id, actors)| {
                    (
                        *fragment_id,
                        actors.iter().map(|a| a.actor_id).collect::<Vec<_>>(),
                    )
                })
                .collect();
            let mut resolved = SourceManager::resolve_fragment_to_actor_splits(
                &info.stream_job_fragments,
                &info.init_split_assignment,
                &fragment_actor_ids,
            )?;
            resolved.extend(SourceManager::resolve_backfill_splits(
                &info.stream_job_fragments,
                actor_no_shuffle,
                |fragment_id, actor_id| {
                    database_info
                        .fragment(fragment_id)
                        .actors
                        .get(&actor_id)
                        .map(|info| info.splits.clone())
                },
            )?);
            Ok(resolved)
        }

        let mut notify_database_graph = command.is_some();
        let mut throttle_config: Option<ThrottleConfigMap> = None;

        // Each variant handles its own pre-apply, edge building, mutation generation,
        // collect base info, and post-apply. The match produces values consumed by the
        // common snapshot-backfill-merging code that follows.
        let (
            mutation,
            mut table_ids_to_commit,
            mut actors_to_create,
            mut node_actors,
            post_collect_command,
        ) = match command {
            None => self.apply_simple_command(None, "barrier"),
            Some(Command::CreateStreamingJob {
                mut info,
                job_type:
                    CreateStreamingJobType::SnapshotBackfill {
                        mut snapshot_backfill_info,
                        since_epoch,
                    },
                cross_db_snapshot_backfill_info,
            }) => {
                notify_database_graph = false;
                let ensembles = resolve_no_shuffle_ensembles(
                    &info.stream_job_fragments,
                    &info.upstream_fragment_downstreams,
                )?;
                let actors = render_actors(
                    &info.stream_job_fragments,
                    &self.database_info,
                    &info.definition,
                    &info.stream_job_fragments.inner.ctx,
                    &info.streaming_job_model,
                    partial_graph_manager
                        .control_stream_manager()
                        .env
                        .actor_id_generator(),
                    worker_nodes,
                    &ensembles,
                    &info.database_resource_group,
                )?;
                {
                    assert!(!self.state.is_paused());
                    let (snapshot_epoch, since_timestamp_upstream_log_epochs) =
                        if let Some(since_epoch) = &since_epoch {
                            let (snapshot_epoch, log_epochs) =
                                since_epoch.resolved.as_ref().ok_or_else(|| {
                            MetaError::from(anyhow::anyhow!(
                                "since_timestamp epoch has not been resolved for snapshot backfill"
                            ))
                        })?;
                            (
                                *snapshot_epoch,
                                Some((
                                    log_epochs,
                                    to_partial_graph_id(self.database_id, None),
                                    barrier_info.prev_epoch(),
                                )),
                            )
                        } else {
                            (barrier_info.prev_epoch(), None)
                        };
                    // set snapshot epoch of upstream table for snapshot backfill
                    for snapshot_backfill_epoch in snapshot_backfill_info
                        .upstream_mv_table_id_to_backfill_epoch
                        .values_mut()
                    {
                        assert_eq!(
                            snapshot_backfill_epoch.replace(snapshot_epoch),
                            None,
                            "must not set previously"
                        );
                    }
                    for fragment in info.stream_job_fragments.inner.fragments.values_mut() {
                        fill_snapshot_backfill_epoch(
                            &mut fragment.nodes,
                            Some(&snapshot_backfill_info),
                            &cross_db_snapshot_backfill_info,
                        )?;
                    }
                    let job_id = info.stream_job_fragments.stream_job_id();
                    let snapshot_backfill_upstream_tables = snapshot_backfill_info
                        .upstream_mv_table_id_to_backfill_epoch
                        .keys()
                        .cloned()
                        .collect();
                    // Build the independent graph's internal edges first. The no-shuffle mapping
                    // is needed for split resolution, while cross-graph edges are deferred until
                    // the job merges into the database graph.
                    let partial_graph_id =
                        to_partial_graph_id(info.streaming_job.database_id(), Some(job_id));
                    let mut edge_builder =
                        FragmentEdgeBuilder::new(info.stream_job_fragments.fragments.values().map(
                            |fragment| {
                                (
                                    fragment.fragment_id,
                                    EdgeBuilderFragmentInfo::from_fragment(
                                        fragment,
                                        &actors.stream_actors,
                                        &actors.actor_location,
                                        partial_graph_id,
                                        partial_graph_manager.control_stream_manager(),
                                    ),
                                )
                            },
                        ));
                    edge_builder.add_relations(&info.stream_job_fragments.downstreams);
                    let mut edges = edge_builder.build();
                    // Phase 2: Resolve source-level DiscoveredSplits to actor-level SplitAssignment
                    let resolved_split_assignment = resolve_source_splits(
                        &info,
                        &actors,
                        edges.actor_new_no_shuffle(),
                        &self.database_info,
                    )?;

                    let Entry::Vacant(entry) =
                        self.independent_checkpoint_job_controls.entry(job_id)
                    else {
                        panic!("duplicated creating snapshot backfill job {job_id}");
                    };

                    let term_id = self.term_id.as_str();
                    let job = CreatingStreamingJobControl::new(
                        entry,
                        CreateSnapshotBackfillJobCommandInfo {
                            info: info.clone(),
                            snapshot_backfill_info: snapshot_backfill_info.clone(),
                            cross_db_snapshot_backfill_info,
                            resolved_split_assignment: resolved_split_assignment.clone(),
                            refresh_interval_sec: None,
                        },
                        notifier.as_mut(),
                        snapshot_backfill_upstream_tables,
                        snapshot_epoch,
                        since_timestamp_upstream_log_epochs,
                        hummock_version_stats,
                        term_id,
                        partial_graph_manager,
                        &mut edges,
                        &resolved_split_assignment,
                        &actors,
                    )?;

                    if let Some(fragment_infos) = job.fragment_infos() {
                        self.database_info.shared_actor_infos.upsert(
                            self.database_id,
                            fragment_infos.values().map(|f| (f, job_id)),
                        );
                    }

                    for upstream_mv_table_id in snapshot_backfill_info
                        .upstream_mv_table_id_to_backfill_epoch
                        .keys()
                    {
                        self.database_info.register_subscriber(
                            upstream_mv_table_id.as_job_id(),
                            info.streaming_job.id().as_subscriber_id(),
                            SubscriberType::SnapshotBackfill,
                        );
                    }

                    let mutation = Command::create_streaming_job_to_mutation(
                        &info,
                        &CreateStreamingJobType::SnapshotBackfill {
                            snapshot_backfill_info,
                            since_epoch,
                        },
                        [],
                        self.state.is_paused(),
                        &mut edges,
                        partial_graph_manager.control_stream_manager(),
                        None,
                        &resolved_split_assignment,
                        &actors.stream_actors,
                        &actors.actor_location,
                    )?;

                    let (table_ids, node_actors) = self.collect_base_info();
                    (
                        Some(mutation),
                        table_ids,
                        None,
                        node_actors,
                        PostCollectCommand::barrier(),
                    )
                }
            }
            Some(Command::CreateStreamingJob {
                mut info,
                job_type: CreateStreamingJobType::BatchRefresh(mut batch_refresh_info),
                cross_db_snapshot_backfill_info,
            }) => {
                notify_database_graph = false;
                {
                    if self.state.is_paused() {
                        bail!("cannot create batch refresh job while database barrier is paused");
                    }
                    let snapshot_epoch = barrier_info.prev_epoch();
                    let job_id = info.stream_job_fragments.stream_job_id();
                    let database_id = info.streaming_job.database_id();

                    // 1. Fill snapshot backfill epochs.
                    let snapshot_backfill_info = &mut batch_refresh_info.snapshot_backfill_info;
                    for snapshot_backfill_epoch in snapshot_backfill_info
                        .upstream_mv_table_id_to_backfill_epoch
                        .values_mut()
                    {
                        assert_eq!(
                            snapshot_backfill_epoch.replace(snapshot_epoch),
                            None,
                            "must not set previously"
                        );
                    }
                    for fragment in info.stream_job_fragments.inner.fragments.values_mut() {
                        fill_snapshot_backfill_epoch(
                            &mut fragment.nodes,
                            Some(snapshot_backfill_info),
                            &cross_db_snapshot_backfill_info,
                        )?;
                    }
                    let snapshot_backfill_upstream_tables: HashSet<TableId> =
                        snapshot_backfill_info
                            .upstream_mv_table_id_to_backfill_epoch
                            .keys()
                            .cloned()
                            .collect();

                    // 2. Build BatchRefreshLogicalFragments (after epoch filling).
                    let logical = BatchRefreshLogicalFragments {
                        fragments: info
                            .stream_job_fragments
                            .inner
                            .fragments
                            .iter()
                            .map(|(&fid, fragment)| {
                                (
                                    fid,
                                    LoadedFragment {
                                        fragment_id: fid,
                                        job_id,
                                        fragment_type_mask: fragment.fragment_type_mask,
                                        distribution_type: fragment.distribution_type.into(),
                                        vnode_count: fragment.vnode_count(),
                                        nodes: fragment.nodes.clone(),
                                        state_table_ids: fragment
                                            .state_table_ids
                                            .iter()
                                            .cloned()
                                            .collect(),
                                        parallelism: None,
                                    },
                                )
                            })
                            .collect(),
                        downstreams: info.stream_job_fragments.downstreams.clone(),
                    };

                    // 3. Create BatchRefreshJobCheckpointControl. `new()` handles actor
                    //    rendering, the partial-graph initial barrier, and produces the
                    //    database-graph mutation for the main barrier.
                    assert!(
                        !self
                            .independent_checkpoint_job_controls
                            .contains_key(&job_id),
                        "duplicated creating batch refresh job {job_id}"
                    );

                    let snapshot_backfill_info_clone =
                        batch_refresh_info.snapshot_backfill_info.clone();
                    let refresh_interval_sec = batch_refresh_info.refresh_interval_sec;

                    // Database-graph `Add` mutation: batch refresh has no actors in the
                    // database graph; it only needs to register snapshot-backfill
                    // subscribers on the upstream MV tables.
                    let subscriber_id =
                        info.stream_job_fragments.stream_job_id().as_subscriber_id();
                    let mutation = Mutation::Add(AddMutation {
                        actor_dispatchers: Default::default(),
                        added_actors: Default::default(),
                        actor_splits: Default::default(),
                        pause: false,
                        subscriptions_to_add: snapshot_backfill_info_clone
                            .upstream_mv_table_id_to_backfill_epoch
                            .keys()
                            .map(|table_id| PbSubscriptionUpstreamInfo {
                                subscriber_id,
                                upstream_mv_table_id: *table_id,
                            })
                            .collect(),
                        backfill_nodes_to_pause: Default::default(),
                        actor_cdc_table_snapshot_splits: None,
                        new_upstream_sinks: Default::default(),
                        dropped_actors: Default::default(),
                        sink_log_store_flush: Default::default(),
                    });

                    let job = BatchRefreshJobCheckpointControl::new(
                        database_id,
                        job_id,
                        CreateSnapshotBackfillJobCommandInfo {
                            info: info.clone(),
                            snapshot_backfill_info: snapshot_backfill_info_clone.clone(),
                            cross_db_snapshot_backfill_info,
                            resolved_split_assignment: Default::default(),
                            refresh_interval_sec: Some(refresh_interval_sec),
                        },
                        notifier.as_mut(),
                        snapshot_backfill_upstream_tables,
                        snapshot_epoch,
                        hummock_version_stats,
                        self.term_id(),
                        partial_graph_manager,
                        &logical,
                        worker_nodes,
                        refresh_interval_sec,
                    )?;

                    if let Some(fragment_infos) = job.fragment_infos() {
                        self.database_info.shared_actor_infos.upsert(
                            self.database_id,
                            fragment_infos.values().map(|f| (f, job_id)),
                        );
                    }

                    self.independent_checkpoint_job_controls
                        .insert(job_id, IndependentCheckpointJobControl::BatchRefresh(job));

                    // Register permanent subscriber (never unregistered until MV is dropped)
                    for upstream_mv_table_id in snapshot_backfill_info_clone
                        .upstream_mv_table_id_to_backfill_epoch
                        .keys()
                    {
                        self.database_info.register_subscriber(
                            upstream_mv_table_id.as_job_id(),
                            info.streaming_job.id().as_subscriber_id(),
                            SubscriberType::SnapshotBackfill,
                        );
                    }

                    let (table_ids, node_actors) = self.collect_base_info();
                    (
                        Some(mutation),
                        table_ids,
                        None,
                        node_actors,
                        PostCollectCommand::barrier(),
                    )
                }
            }
            Some(Command::CreateStreamingJob {
                mut info,
                job_type:
                    job_type @ (CreateStreamingJobType::Normal
                    | CreateStreamingJobType::SinkIntoTable(_)),
                cross_db_snapshot_backfill_info,
            }) => {
                let ensembles = resolve_no_shuffle_ensembles(
                    &info.stream_job_fragments,
                    &info.upstream_fragment_downstreams,
                )?;
                let actors = render_actors(
                    &info.stream_job_fragments,
                    &self.database_info,
                    &info.definition,
                    &info.stream_job_fragments.inner.ctx,
                    &info.streaming_job_model,
                    partial_graph_manager
                        .control_stream_manager()
                        .env
                        .actor_id_generator(),
                    worker_nodes,
                    &ensembles,
                    &info.database_resource_group,
                )?;
                for fragment in info.stream_job_fragments.inner.fragments.values_mut() {
                    fill_snapshot_backfill_epoch(
                        &mut fragment.nodes,
                        None,
                        &cross_db_snapshot_backfill_info,
                    )?;
                }

                // Build edges
                let new_upstream_sink =
                    if let CreateStreamingJobType::SinkIntoTable(ref ctx) = job_type {
                        Some(ctx)
                    } else {
                        None
                    };

                let mut edges = self.database_info.build_edge(
                    Some(&info),
                    None,
                    new_upstream_sink,
                    partial_graph_manager.control_stream_manager(),
                    &actors.stream_actors,
                    &actors.actor_location,
                );
                // Phase 2: Resolve source-level DiscoveredSplits to actor-level SplitAssignment
                let resolved_split_assignment = resolve_source_splits(
                    &info,
                    &actors,
                    edges.actor_new_no_shuffle(),
                    &self.database_info,
                )?;

                let old_sink_job_id = info
                    .replace_sink
                    .as_ref()
                    .map(|old_sink_id| old_sink_id.as_job_id());
                // Pre-apply: add new job and fragments
                let cdc_tracker = if let Some(splits) = &info.cdc_table_snapshot_splits {
                    let (fragment, _) =
                        parallel_cdc_table_backfill_fragment(info.stream_job_fragments.fragments())
                            .expect("should have parallel cdc fragment");
                    Some(CdcTableBackfillTracker::new(
                        fragment.fragment_id,
                        splits.clone(),
                    ))
                } else {
                    None
                };
                self.database_info
                    .pre_apply_new_job(info.streaming_job.id(), cdc_tracker);
                self.database_info.pre_apply_new_fragments(
                    info.stream_job_fragments
                        .new_fragment_info(
                            &actors.stream_actors,
                            &actors.actor_location,
                            &resolved_split_assignment,
                        )
                        .map(|(fragment_id, fragment_infos)| {
                            (fragment_id, info.streaming_job.id(), fragment_infos)
                        }),
                );
                if let CreateStreamingJobType::SinkIntoTable(ref ctx) = job_type {
                    let downstream_fragment_id = ctx.new_sink_downstream.downstream_fragment_id;
                    self.database_info.pre_apply_add_node_upstream(
                        downstream_fragment_id,
                        &PbUpstreamSinkInfo {
                            upstream_fragment_id: ctx.sink_fragment_id,
                            sink_output_schema: ctx.sink_output_fields.clone(),
                            project_exprs: ctx.project_exprs.clone(),
                        },
                    );
                }

                let (table_ids, node_actors) = self.collect_base_info();
                let dropped_actors = if let Some(old_sink_job_id) = old_sink_job_id {
                    let Some(job) = self.database_info.post_apply_remove_job(old_sink_job_id)
                    else {
                        bail!(
                            "old sink job {} not found in barrier state",
                            old_sink_job_id
                        );
                    };
                    job.fragment_infos
                        .values()
                        .flat_map(|fragment| fragment.actors.keys().copied())
                        .collect()
                } else {
                    vec![]
                };

                // Actors to create
                let actors_to_create = Some(Command::create_streaming_job_actors_to_create(
                    &info,
                    &mut edges,
                    &actors.stream_actors,
                    &actors.actor_location,
                ));

                // CDC table snapshot splits
                let actor_cdc_table_snapshot_splits = self
                    .database_info
                    .assign_cdc_backfill_splits(info.stream_job_fragments.stream_job_id())?;

                // Mutation
                let is_currently_paused = self.state.is_paused();
                let mutation = Command::create_streaming_job_to_mutation(
                    &info,
                    &job_type,
                    dropped_actors,
                    is_currently_paused,
                    &mut edges,
                    partial_graph_manager.control_stream_manager(),
                    actor_cdc_table_snapshot_splits,
                    &resolved_split_assignment,
                    &actors.stream_actors,
                    &actors.actor_location,
                )?;

                (
                    Some(mutation),
                    table_ids,
                    actors_to_create,
                    node_actors,
                    PostCollectCommand::CreateStreamingJob {
                        info,
                        job_type,
                        cross_db_snapshot_backfill_info,
                        resolved_split_assignment,
                    },
                )
            }

            Some(Command::Flush) => self.apply_simple_command(None, "Flush"),

            Some(Command::Pause) => {
                let prev_is_paused = self.state.is_paused();
                self.state.set_is_paused(true);
                let mutation = Command::pause_to_mutation(prev_is_paused);
                let (table_ids, node_actors) = self.collect_base_info();
                (
                    mutation,
                    table_ids,
                    None,
                    node_actors,
                    PostCollectCommand::Command("Pause".to_owned()),
                )
            }

            Some(Command::Resume) => {
                let prev_is_paused = self.state.is_paused();
                self.state.set_is_paused(false);
                let mutation = Command::resume_to_mutation(prev_is_paused);
                let (table_ids, node_actors) = self.collect_base_info();
                (
                    mutation,
                    table_ids,
                    None,
                    node_actors,
                    PostCollectCommand::Command("Resume".to_owned()),
                )
            }

            Some(Command::Throttle { mut config }) => {
                let mutation = self.database_info.pre_apply_throttle(&mut config);
                notify_database_graph = mutation.is_some();
                throttle_config = Some(config);
                self.apply_simple_command(mutation, "Throttle")
            }

            Some(Command::DropStreamingJobs {
                streaming_job_ids,
                unregistered_state_table_ids: _,
                dropped_sink_fragment_by_targets,
            }) => {
                // pre_apply: drop node upstream for sink targets
                for (target_fragment, sink_fragments) in &dropped_sink_fragment_by_targets {
                    self.database_info
                        .pre_apply_drop_node_upstream(*target_fragment, sink_fragments);
                }

                let (table_ids, node_actors) = self.collect_base_info();

                let mut actors = Vec::new();
                for job_id in streaming_job_ids {
                    let Some(job) = self.database_info.post_apply_remove_job(job_id) else {
                        warn!(
                            %job_id,
                            "skip drop payload for streaming job that has already been removed from barrier worker"
                        );
                        continue;
                    };

                    for fragment in job.fragment_infos.values() {
                        actors.extend(fragment.actors.keys().copied());
                    }
                }

                let mutation = Some(Command::drop_streaming_jobs_to_mutation(
                    &actors,
                    &dropped_sink_fragment_by_targets,
                ));
                (
                    mutation,
                    table_ids,
                    None,
                    node_actors,
                    PostCollectCommand::DropStreamingJobs,
                )
            }

            Some(Command::RescheduleIntent {
                reschedule_plan, ..
            }) => {
                let ReschedulePlan {
                    reschedules,
                    fragment_actors,
                } = reschedule_plan
                    .as_ref()
                    .expect("reschedule intent should be resolved in global barrier worker");

                // Pre-apply: reschedule fragments
                for (fragment_id, reschedule) in reschedules {
                    self.database_info.pre_apply_reschedule(
                        *fragment_id,
                        reschedule
                            .added_actors
                            .iter()
                            .flat_map(|(node_id, actors): (&WorkerId, &Vec<ActorId>)| {
                                actors.iter().map(|actor_id| {
                                    (
                                        *actor_id,
                                        InflightActorInfo {
                                            worker_id: *node_id,
                                            vnode_bitmap: reschedule
                                                .newly_created_actors
                                                .get(actor_id)
                                                .expect("should exist")
                                                .0
                                                .0
                                                .vnode_bitmap
                                                .clone(),
                                            splits: reschedule
                                                .actor_splits
                                                .get(actor_id)
                                                .cloned()
                                                .unwrap_or_default(),
                                        },
                                    )
                                })
                            })
                            .collect(),
                        reschedule
                            .vnode_bitmap_updates
                            .iter()
                            .filter(|(actor_id, _)| {
                                !reschedule.newly_created_actors.contains_key(*actor_id)
                            })
                            .map(|(actor_id, bitmap)| (*actor_id, bitmap.clone()))
                            .collect(),
                        reschedule.actor_splits.clone(),
                    );
                }

                let (table_ids, node_actors) = self.collect_base_info();

                // Actors to create
                let actors_to_create = Some(Command::reschedule_actors_to_create(
                    reschedules,
                    fragment_actors,
                    &self.database_info,
                    partial_graph_manager.control_stream_manager(),
                ));

                // Post-apply: remove old actors
                self.database_info
                    .post_apply_reschedules(reschedules.iter().map(|(fragment_id, reschedule)| {
                        (
                            *fragment_id,
                            reschedule.removed_actors.iter().cloned().collect(),
                        )
                    }));

                // Mutation
                let mutation = Command::reschedule_to_mutation(
                    reschedules,
                    fragment_actors,
                    partial_graph_manager.control_stream_manager(),
                    &mut self.database_info,
                )?;

                let reschedules = reschedule_plan
                    .expect("reschedule intent should be resolved in global barrier worker")
                    .reschedules;
                (
                    mutation,
                    table_ids,
                    actors_to_create,
                    node_actors,
                    PostCollectCommand::Reschedule { reschedules },
                )
            }

            Some(Command::ReplaceStreamJob(plan)) => {
                let ensembles = resolve_no_shuffle_ensembles(
                    &plan.new_fragments,
                    &plan.upstream_fragment_downstreams,
                )?;
                let mut render_result = render_actors(
                    &plan.new_fragments,
                    &self.database_info,
                    "", // replace jobs don't need mview definition
                    &plan.new_fragments.inner.ctx,
                    &plan.streaming_job_model,
                    partial_graph_manager
                        .control_stream_manager()
                        .env
                        .actor_id_generator(),
                    worker_nodes,
                    &ensembles,
                    &plan.database_resource_group,
                )?;

                // Render actors for auto_refresh_schema_sinks.
                // Each sink's new_fragment inherits parallelism from its original_fragment.
                if let Some(sinks) = &plan.auto_refresh_schema_sinks {
                    let actor_id_counter = partial_graph_manager
                        .control_stream_manager()
                        .env
                        .actor_id_generator();
                    for sink_ctx in sinks {
                        let original_fragment_id = sink_ctx.original_fragment.fragment_id;
                        let original_frag_info = self.database_info.fragment(original_fragment_id);
                        let actor_template = EnsembleActorTemplate::from_existing_inflight_fragment(
                            original_frag_info,
                        );
                        let new_aligner = ComponentFragmentAligner::new_persistent(
                            &actor_template,
                            actor_id_counter,
                        );
                        let distribution_type: DistributionType =
                            sink_ctx.new_fragment.distribution_type.into();
                        let actor_assignments =
                            new_aligner.align_component_actor(distribution_type);
                        let new_fragment_id = sink_ctx.new_fragment.fragment_id;
                        let mut actors = Vec::with_capacity(actor_assignments.len());
                        for (&actor_id, (worker_id, vnode_bitmap)) in &actor_assignments {
                            render_result.actor_location.insert(actor_id, *worker_id);
                            actors.push(StreamActor {
                                actor_id,
                                fragment_id: new_fragment_id,
                                vnode_bitmap: vnode_bitmap.clone(),
                                mview_definition: String::new(),
                                expr_context: Some(sink_ctx.ctx.to_expr_context()),
                                config_override: sink_ctx.ctx.config_override.clone(),
                            });
                        }
                        render_result.stream_actors.insert(new_fragment_id, actors);
                    }
                }

                // Build edges first (needed for no-shuffle mapping used in split resolution)
                let mut edges = self.database_info.build_edge(
                    None,
                    Some(&plan),
                    None,
                    partial_graph_manager.control_stream_manager(),
                    &render_result.stream_actors,
                    &render_result.actor_location,
                );

                // Phase 2: Resolve splits to actor-level assignment.
                let fragment_actor_ids: HashMap<FragmentId, Vec<ActorId>> = render_result
                    .stream_actors
                    .iter()
                    .map(|(fragment_id, actors)| {
                        (
                            *fragment_id,
                            actors.iter().map(|a| a.actor_id).collect::<Vec<_>>(),
                        )
                    })
                    .collect();
                let resolved_split_assignment = match &plan.split_plan {
                    ReplaceJobSplitPlan::Discovered(discovered) => {
                        SourceManager::resolve_fragment_to_actor_splits(
                            &plan.new_fragments,
                            discovered,
                            &fragment_actor_ids,
                        )?
                    }
                    ReplaceJobSplitPlan::AlignFromPrevious => {
                        SourceManager::resolve_replace_source_splits(
                            &plan.new_fragments,
                            &plan.replace_upstream,
                            edges.actor_new_no_shuffle(),
                            |_fragment_id, actor_id| {
                                self.database_info.fragment_infos().find_map(|fragment| {
                                    fragment
                                        .actors
                                        .get(&actor_id)
                                        .map(|info| info.splits.clone())
                                })
                            },
                        )?
                    }
                };

                // Pre-apply: add new fragments and replace upstream
                self.database_info.pre_apply_new_fragments(
                    plan.new_fragments
                        .new_fragment_info(
                            &render_result.stream_actors,
                            &render_result.actor_location,
                            &resolved_split_assignment,
                        )
                        .map(|(fragment_id, new_fragment)| {
                            (fragment_id, plan.streaming_job.id(), new_fragment)
                        }),
                );
                for (fragment_id, replace_map) in &plan.replace_upstream {
                    self.database_info
                        .pre_apply_replace_node_upstream(*fragment_id, replace_map);
                }
                if let Some(sinks) = &plan.auto_refresh_schema_sinks {
                    self.database_info
                        .pre_apply_new_fragments(sinks.iter().map(|sink| {
                            (
                                sink.new_fragment.fragment_id,
                                sink.original_sink.id.as_job_id(),
                                sink.new_fragment_info(
                                    &render_result.stream_actors,
                                    &render_result.actor_location,
                                ),
                            )
                        }));
                }

                let (table_ids, node_actors) = self.collect_base_info();

                // Actors to create
                let actors_to_create = Some(Command::replace_stream_job_actors_to_create(
                    &plan,
                    &mut edges,
                    &self.database_info,
                    &render_result.stream_actors,
                    &render_result.actor_location,
                ));

                // Mutation (must be generated before removing old fragments,
                // because it reads actor info from database_info)
                let mutation = Command::replace_stream_job_to_mutation(
                    &plan,
                    &mut edges,
                    &mut self.database_info,
                    &resolved_split_assignment,
                )?;

                // Post-apply: remove old fragments
                {
                    let mut fragment_ids_to_remove: Vec<_> = plan
                        .old_fragments
                        .fragments
                        .values()
                        .map(|f| f.fragment_id)
                        .collect();
                    if let Some(sinks) = &plan.auto_refresh_schema_sinks {
                        fragment_ids_to_remove
                            .extend(sinks.iter().map(|sink| sink.original_fragment.fragment_id));
                    }
                    self.database_info
                        .post_apply_remove_fragments(fragment_ids_to_remove);
                }

                (
                    mutation,
                    table_ids,
                    actors_to_create,
                    node_actors,
                    PostCollectCommand::ReplaceStreamJob {
                        plan,
                        resolved_split_assignment,
                    },
                )
            }

            Some(Command::SourceChangeSplit(split_state)) => {
                // Pre-apply: split assignments
                self.database_info.pre_apply_split_assignments(
                    split_state
                        .split_assignment
                        .iter()
                        .map(|(&fragment_id, splits)| (fragment_id, splits.clone())),
                );

                let mutation = Some(Command::source_change_split_to_mutation(
                    &split_state.split_assignment,
                ));
                let (table_ids, node_actors) = self.collect_base_info();
                (
                    mutation,
                    table_ids,
                    None,
                    node_actors,
                    PostCollectCommand::SourceChangeSplit {
                        split_assignment: split_state.split_assignment,
                    },
                )
            }

            Some(Command::CreateSubscription {
                subscription_id,
                upstream_mv_table_id,
                retention_second,
            }) => {
                self.database_info.register_subscriber(
                    upstream_mv_table_id.as_job_id(),
                    subscription_id.as_subscriber_id(),
                    SubscriberType::Subscription(retention_second),
                );
                let mutation = Some(Command::create_subscription_to_mutation(
                    upstream_mv_table_id,
                    subscription_id,
                ));
                let (table_ids, node_actors) = self.collect_base_info();
                (
                    mutation,
                    table_ids,
                    None,
                    node_actors,
                    PostCollectCommand::CreateSubscription { subscription_id },
                )
            }

            Some(Command::DropSubscription {
                subscription_id,
                upstream_mv_table_id,
            }) => {
                if self
                    .database_info
                    .unregister_subscriber(
                        upstream_mv_table_id.as_job_id(),
                        subscription_id.as_subscriber_id(),
                    )
                    .is_none()
                {
                    warn!(%subscription_id, %upstream_mv_table_id, "no subscription to drop");
                }
                let mutation = Some(Command::drop_subscription_to_mutation(
                    upstream_mv_table_id,
                    subscription_id,
                ));
                let (table_ids, node_actors) = self.collect_base_info();
                (
                    mutation,
                    table_ids,
                    None,
                    node_actors,
                    PostCollectCommand::Command("DropSubscription".to_owned()),
                )
            }

            Some(Command::AlterSubscriptionRetention {
                subscription_id,
                upstream_mv_table_id,
                retention_second,
            }) => {
                self.database_info.update_subscription_retention(
                    upstream_mv_table_id.as_job_id(),
                    subscription_id.as_subscriber_id(),
                    retention_second,
                );
                self.apply_simple_command(None, "AlterSubscriptionRetention")
            }

            Some(Command::ConnectorPropsChange(config)) => {
                let mutation = Some(Command::connector_props_change_to_mutation(&config));
                let (table_ids, node_actors) = self.collect_base_info();
                (
                    mutation,
                    table_ids,
                    None,
                    node_actors,
                    PostCollectCommand::ConnectorPropsChange(config),
                )
            }

            Some(Command::Refresh {
                table_id,
                associated_source_id,
            }) => {
                let mutation = Some(Command::refresh_to_mutation(table_id, associated_source_id));
                self.apply_simple_command(mutation, "Refresh")
            }

            Some(Command::ListFinish {
                table_id: _,
                associated_source_id,
            }) => {
                let mutation = Some(Command::list_finish_to_mutation(associated_source_id));
                self.apply_simple_command(mutation, "ListFinish")
            }

            Some(Command::LoadFinish {
                table_id: _,
                associated_source_id,
            }) => {
                let mutation = Some(Command::load_finish_to_mutation(associated_source_id));
                self.apply_simple_command(mutation, "LoadFinish")
            }

            Some(Command::ResetSource { source_id }) => {
                let mutation = Some(Command::reset_source_to_mutation(source_id));
                self.apply_simple_command(mutation, "ResetSource")
            }

            Some(Command::ResumeBackfill { target }) => {
                let mutation = Command::resume_backfill_to_mutation(&target, &self.database_info)?;
                let (table_ids, node_actors) = self.collect_base_info();
                (
                    mutation,
                    table_ids,
                    None,
                    node_actors,
                    PostCollectCommand::ResumeBackfill { target },
                )
            }

            Some(Command::InjectSourceOffsets {
                source_id,
                split_offsets,
            }) => {
                let mutation = Some(Command::inject_source_offsets_to_mutation(
                    source_id,
                    &split_offsets,
                ));
                self.apply_simple_command(mutation, "InjectSourceOffsets")
            }
        };

        let mut finished_snapshot_backfill_jobs = HashSet::new();
        let mutation = match mutation {
            Some(mutation) => Some(mutation),
            None => {
                let mut finished_snapshot_backfill_job_info = HashMap::new();
                if barrier_info.kind.is_checkpoint() {
                    for (&job_id, job) in &mut self.independent_checkpoint_job_controls {
                        if let IndependentCheckpointJobControl::CreatingStreamingJob(creating_job) =
                            job
                            && creating_job.should_merge_to_upstream(partial_graph_manager)
                        {
                            // The independent actors will stop on this barrier. Apply throttle to
                            // the in-memory plan used to create the database-graph actors, and let
                            // the database barrier own the collection notification.
                            if throttle_config
                                .as_mut()
                                .and_then(|config| creating_job.pre_apply_throttle(config))
                                .is_some()
                            {
                                notify_database_graph = true;
                            }
                            let info = creating_job
                                .start_consume_upstream(partial_graph_manager, &barrier_info)?;
                            finished_snapshot_backfill_job_info
                                .try_insert(job_id, info)
                                .expect("non-duplicated");
                        }
                    }
                }

                if !finished_snapshot_backfill_job_info.is_empty() {
                    let actors_to_create = actors_to_create.get_or_insert_default();
                    let mut subscriptions_to_drop = vec![];
                    let mut actor_new_dispatchers: HashMap<ActorId, Dispatchers> = HashMap::new();
                    let mut actor_splits = HashMap::new();
                    for (job_id, info) in finished_snapshot_backfill_job_info {
                        finished_snapshot_backfill_jobs.insert(job_id);
                        subscriptions_to_drop.extend(
                            info.snapshot_backfill_upstream_tables.iter().map(
                                |upstream_table_id| PbSubscriptionUpstreamInfo {
                                    subscriber_id: job_id.as_subscriber_id(),
                                    upstream_mv_table_id: *upstream_table_id,
                                },
                            ),
                        );
                        for upstream_mv_table_id in &info.snapshot_backfill_upstream_tables {
                            assert_matches!(
                                self.database_info.unregister_subscriber(
                                    upstream_mv_table_id.as_job_id(),
                                    job_id.as_subscriber_id()
                                ),
                                Some(SubscriberType::SnapshotBackfill)
                            );
                        }

                        table_ids_to_commit.extend(
                            info.fragment_infos
                                .values()
                                .flat_map(|fragment| fragment.state_table_ids.iter())
                                .copied(),
                        );

                        let actor_len = info
                            .fragment_infos
                            .values()
                            .map(|fragment| fragment.actors.len() as u64)
                            .sum();
                        let id_gen = GlobalActorIdGen::new(
                            partial_graph_manager
                                .control_stream_manager()
                                .env
                                .actor_id_generator(),
                            actor_len,
                        );
                        let mut next_local_actor_id = 0;
                        // mapping from old_actor_id to new_actor_id
                        let actor_mapping: HashMap<_, _> = info
                            .fragment_infos
                            .values()
                            .flat_map(|fragment| fragment.actors.keys())
                            .map(|old_actor_id| {
                                let new_actor_id = id_gen.to_global_id(next_local_actor_id);
                                next_local_actor_id += 1;
                                (*old_actor_id, new_actor_id.as_global_id())
                            })
                            .collect();
                        let actor_mapping = &actor_mapping;
                        let new_stream_actors: HashMap<_, _> = info
                            .stream_actors
                            .into_iter()
                            .map(|(old_actor_id, mut actor)| {
                                let new_actor_id = actor_mapping[&old_actor_id];
                                actor.actor_id = new_actor_id;
                                (new_actor_id, actor)
                            })
                            .collect();
                        let new_fragment_info: HashMap<_, _> = info
                            .fragment_infos
                            .into_iter()
                            .map(|(fragment_id, mut fragment)| {
                                let actors = take(&mut fragment.actors);
                                fragment.actors = actors
                                    .into_iter()
                                    .map(|(old_actor_id, actor)| {
                                        let new_actor_id = actor_mapping[&old_actor_id];
                                        (new_actor_id, actor)
                                    })
                                    .collect();
                                (fragment_id, fragment)
                            })
                            .collect();
                        actor_splits.extend(
                            new_fragment_info
                                .values()
                                .flat_map(|fragment| &fragment.actors)
                                .map(|(actor_id, actor)| {
                                    (
                                        *actor_id,
                                        ConnectorSplits {
                                            splits: actor
                                                .splits
                                                .iter()
                                                .map(ConnectorSplit::from)
                                                .collect(),
                                        },
                                    )
                                }),
                        );
                        // new actors belong to the database partial graph
                        let partial_graph_id = to_partial_graph_id(self.database_id, None);
                        let mut edge_builder = FragmentEdgeBuilder::from_inflight_fragments(
                            info.upstream_fragment_downstreams
                                .keys()
                                .map(|upstream_fragment_id| {
                                    self.database_info.fragment(*upstream_fragment_id)
                                })
                                .chain(new_fragment_info.values()),
                            partial_graph_id,
                            partial_graph_manager.control_stream_manager(),
                        );
                        edge_builder.add_relations(&info.upstream_fragment_downstreams);
                        edge_builder.add_relations(&info.downstreams);
                        let mut edges = edge_builder.build();
                        let new_actors_to_create = edges.collect_actors_to_create(
                            new_fragment_info.values().map(|fragment| {
                                (
                                    fragment.fragment_id,
                                    &fragment.nodes,
                                    fragment.actors.iter().map(|(actor_id, actor)| {
                                        (&new_stream_actors[actor_id], actor.worker_id)
                                    }),
                                    [], // no initial subscriber for backfilling job
                                )
                            }),
                        );
                        for upstream_fragment_id in info.upstream_fragment_downstreams.keys() {
                            let new_actor_dispatchers = edges
                                .dispatchers
                                .remove(upstream_fragment_id)
                                .expect("should exist");
                            for (upstream_actor_id, dispatchers) in new_actor_dispatchers {
                                actor_new_dispatchers
                                    .entry(upstream_actor_id)
                                    .or_default()
                                    .dispatchers
                                    .extend(dispatchers);
                            }
                        }
                        assert!(edges.is_empty(), "remaining edges: {:?}", edges);
                        for (worker_id, worker_actors) in new_actors_to_create {
                            node_actors.entry(worker_id).or_default().extend(
                                worker_actors.values().flat_map(|(_, actors, _)| {
                                    actors.iter().map(|(actor, _, _)| actor.actor_id)
                                }),
                            );
                            actors_to_create
                                .entry(worker_id)
                                .or_default()
                                .extend(worker_actors);
                        }
                        self.database_info.add_existing(InflightStreamingJobInfo {
                            job_id,
                            fragment_infos: new_fragment_info,
                            subscribers: Default::default(), // no initial subscribers for newly created snapshot backfill
                            status: CreateStreamingJobStatus::Created,
                            cdc_table_backfill_tracker: None, // no cdc table backfill for snapshot backfill
                        });
                    }

                    Some(PbMutation::Update(PbUpdateMutation {
                        dispatcher_update: vec![], /* the cross-graph dispatchers did not exist before merging */
                        merge_update: vec![],      // no upstream update on existing actors
                        actor_vnode_bitmap_update: Default::default(), /* no in place update vnode bitmap happened */
                        dropped_actors: vec![], /* no actors to drop in the partial graph of database */
                        actor_splits,
                        actor_new_dispatchers,
                        actor_cdc_table_snapshot_splits: None, /* no cdc table backfill in snapshot backfill */
                        sink_schema_change: Default::default(), /* no sink auto schema change happened here */
                        subscriptions_to_drop,
                    }))
                } else {
                    let fragment_ids = self.database_info.take_pending_backfill_nodes();
                    if fragment_ids.is_empty() {
                        None
                    } else {
                        Some(PbMutation::StartFragmentBackfill(
                            PbStartFragmentBackfillMutation { fragment_ids },
                        ))
                    }
                }
            }
        };

        // Forward barrier to independent job controls
        for (job_id, job) in &mut self.independent_checkpoint_job_controls {
            match job {
                IndependentCheckpointJobControl::CreatingStreamingJob(creating_job) => {
                    if finished_snapshot_backfill_jobs.contains(job_id) {
                        continue;
                    }
                    let throttle_mutation = throttle_config.as_mut().and_then(|config| {
                        creating_job
                            .pre_apply_throttle(config)
                            .map(|mutation| (mutation, notifier.as_mut()))
                    });
                    creating_job.on_new_upstream_barrier(
                        partial_graph_manager,
                        &barrier_info,
                        throttle_mutation,
                    )?;
                }
                IndependentCheckpointJobControl::BatchRefresh(batch_refresh_job) => {
                    let throttle_mutation = throttle_config.as_mut().and_then(|config| {
                        batch_refresh_job
                            .pre_apply_throttle(config)
                            .map(|mutation| (mutation, notifier.as_mut()))
                    });
                    batch_refresh_job.on_new_upstream_barrier(
                        partial_graph_manager,
                        &barrier_info,
                        throttle_mutation,
                    )?;
                }
            }
        }

        let database_notifier = if notify_database_graph {
            notifier.as_mut()
        } else {
            None
        };
        partial_graph_manager.inject_barrier(
            to_partial_graph_id(self.database_id, None),
            mutation,
            None,
            &node_actors,
            InflightFragmentInfo::existing_table_ids(self.database_info.fragment_infos()),
            InflightFragmentInfo::workers(self.database_info.fragment_infos()),
            actors_to_create,
            PartialGraphBarrierInfo::new(
                post_collect_command,
                barrier_info,
                database_notifier,
                table_ids_to_commit,
            ),
        )?;

        // Publish the collection receivers only after all parts of a scheduled command have been
        // dispatched successfully. Periodic barriers do not have a notifier.
        if let Some(notifier) = notifier.take() {
            notifier.started();
        }

        Ok(ApplyCommandInfo {
            jobs_to_wait: finished_snapshot_backfill_jobs,
        })
    }
}
