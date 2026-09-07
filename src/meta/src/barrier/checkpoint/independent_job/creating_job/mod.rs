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

mod barrier_control;
mod status;

use std::cmp::max;
use std::collections::{HashMap, HashSet, VecDeque, hash_map};
use std::mem::take;
use std::ops::Bound::{Excluded, Unbounded};
use std::time::Duration;

use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::id::JobId;
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_common::util::epoch::Epoch;
use risingwave_meta_model::WorkerId;
use risingwave_pb::ddl_service::PbBackfillType;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::id::{ActorId, FragmentId, PartialGraphId};
use risingwave_pb::stream_plan::barrier::PbBarrierKind;
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::{AddMutation, StopMutation};
use risingwave_pb::stream_service::BarrierCompleteResponse;
use status::CreatingStreamingJobStatus;
use tracing::{debug, info};

use super::super::state::RenderResult;
use super::IndependentCheckpointJobControl;
use crate::MetaResult;
use crate::barrier::backfill_order_control::get_nodes_with_backfill_dependencies;
use crate::barrier::checkpoint::independent_job::creating_job::barrier_control::CreatingStreamingJobBarrierStats;
use crate::barrier::command::{
    PostCollectCommand, TableLogEpochs, ThrottleConfigMap, UpstreamTableLogEpochs,
};
use crate::barrier::context::CreateSnapshotBackfillJobCommandInfo;
use crate::barrier::edge_builder::{FragmentEdgeBuildResult, FragmentEdgeBuilder};
use crate::barrier::info::BarrierInfo;
use crate::barrier::notifier::NotifierStarter;
use crate::barrier::partial_graph::{
    CollectedBarrier, PartialGraphBarrierInfo, PartialGraphManager, PartialGraphRecoverer,
};
use crate::barrier::progress::{CreateMviewProgressTracker, TrackingJob, collect_done_fragments};
use crate::barrier::rpc::{build_locality_fragment_state_table_mapping, to_partial_graph_id};
use crate::barrier::{
    BackfillOrderState, BackfillProgress, BarrierKind, Command, FragmentBackfillProgress,
    TracedEpoch,
};
use crate::controller::fragment::InflightFragmentInfo;
use crate::manager::MetaOpts;
use crate::model::{FragmentDownstreamRelation, StreamActor, StreamJobActorsToCreate};
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::stream::source_manager::SplitAssignment;
use crate::stream::{ExtendedFragmentBackfillOrder, build_actor_connector_splits};

fn snapshot_backfill_max_pending_barrier_num(opts: &MetaOpts) -> usize {
    opts.in_flight_barrier_nums
        .saturating_mul(opts.snapshot_backfill_barrier_amplification_factor.max(1))
}

#[derive(Debug)]
pub(crate) struct CreatingJobInfo {
    pub fragment_infos: HashMap<FragmentId, InflightFragmentInfo>,
    pub upstream_fragment_downstreams: FragmentDownstreamRelation,
    pub downstreams: FragmentDownstreamRelation,
    pub snapshot_backfill_upstream_tables: HashSet<TableId>,
    pub stream_actors: HashMap<ActorId, StreamActor>,
}

#[derive(Debug)]
pub(crate) struct CreatingStreamingJobControl {
    job_id: JobId,
    partial_graph_id: PartialGraphId,
    snapshot_backfill_upstream_tables: HashSet<TableId>,
    snapshot_epoch: u64,

    node_actors: HashMap<WorkerId, HashSet<ActorId>>,
    state_table_ids: HashSet<TableId>,

    max_committed_epoch: Option<u64>,
    status: CreatingStreamingJobStatus,
    max_lagged_barrier_num: usize,
    max_pending_barrier_num: usize,

    upstream_lag: LabelGuardedIntGauge,
}

impl CreatingStreamingJobControl {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new<'a>(
        entry: hash_map::VacantEntry<'a, JobId, IndependentCheckpointJobControl>,
        create_info: CreateSnapshotBackfillJobCommandInfo,
        notifier: Option<&mut NotifierStarter>,
        snapshot_backfill_upstream_tables: HashSet<TableId>,
        snapshot_epoch: u64,
        since_timestamp_upstream_log_epochs: Option<(&TableLogEpochs, PartialGraphId, u64)>,
        version_stat: &HummockVersionStats,
        term_id: &str,
        partial_graph_manager: &mut PartialGraphManager,
        edges: &mut FragmentEdgeBuildResult,
        split_assignment: &SplitAssignment,
        actors: &RenderResult,
    ) -> MetaResult<&'a mut Self> {
        let info = create_info.info.clone();
        let job_id = info.stream_job_fragments.stream_job_id();
        let database_id = info.streaming_job.database_id();
        debug!(
            %job_id,
            definition = info.definition,
            "new creating job"
        );
        let fragment_infos = info
            .stream_job_fragments
            .new_fragment_info(
                &actors.stream_actors,
                &actors.actor_location,
                split_assignment,
            )
            .collect();
        let backfill_nodes_to_pause =
            get_nodes_with_backfill_dependencies(&info.fragment_backfill_ordering)
                .into_iter()
                .collect();
        let backfill_order_state = BackfillOrderState::new(
            &info.fragment_backfill_ordering,
            &fragment_infos,
            info.locality_fragment_state_table_mapping.clone(),
        );
        let create_mview_tracker = CreateMviewProgressTracker::recover(
            job_id,
            &fragment_infos,
            backfill_order_state,
            version_stat,
        );

        let actors_to_create = Command::create_streaming_job_actors_to_create(
            &info,
            edges,
            &actors.stream_actors,
            &actors.actor_location,
        );

        let mut prev_epoch_fake_physical_time = 0;
        let mut pending_non_checkpoint_barriers = vec![];

        let (initial_barrier_info, log_store_barriers_to_inject) = if let Some((
            upstream_log_epochs,
            upstream_partial_graph_id,
            new_upstream_barrier_prev_epoch,
        )) =
            since_timestamp_upstream_log_epochs
        {
            let (initial_barrier, barriers_to_inject) =
                Self::resolve_since_timestamp_upstream_log_epochs(
                    upstream_log_epochs,
                    partial_graph_manager.pending_barrier_infos(upstream_partial_graph_id),
                    snapshot_epoch,
                    new_upstream_barrier_prev_epoch,
                )?;
            (initial_barrier, Some(barriers_to_inject))
        } else {
            (
                CreatingStreamingJobStatus::new_fake_barrier(
                    &mut prev_epoch_fake_physical_time,
                    &mut pending_non_checkpoint_barriers,
                    PbBarrierKind::Checkpoint,
                ),
                None,
            )
        };

        let added_actors: Vec<ActorId> = actors
            .stream_actors
            .values()
            .flatten()
            .map(|actor| actor.actor_id)
            .collect();
        let actor_splits = split_assignment
            .values()
            .flat_map(build_actor_connector_splits)
            .collect();

        assert!(
            info.cdc_table_snapshot_splits.is_none(),
            "should not have cdc backfill for snapshot backfill job"
        );

        let initial_mutation = Mutation::Add(AddMutation {
            // for mutation of snapshot backfill job, we won't include changes to dispatchers of upstream actors.
            actor_dispatchers: Default::default(),
            added_actors,
            actor_splits,
            // we assume that when handling snapshot backfill, the cluster must not be paused
            pause: false,
            subscriptions_to_add: Default::default(),
            backfill_nodes_to_pause,
            actor_cdc_table_snapshot_splits: None,
            new_upstream_sinks: Default::default(),
            dropped_actors: Default::default(),
            sink_log_store_flush: Default::default(),
        });

        let node_actors = InflightFragmentInfo::actor_ids_to_collect(fragment_infos.values());
        let state_table_ids =
            InflightFragmentInfo::existing_table_ids(fragment_infos.values()).collect();

        let partial_graph_id = to_partial_graph_id(database_id, Some(job_id));
        let max_lagged_barrier_num = partial_graph_manager
            .control_stream_manager()
            .env
            .opts
            .snapshot_backfill_finish_max_lagged_barriers;
        let opts = &partial_graph_manager.control_stream_manager().env.opts;
        let max_pending_barrier_num = snapshot_backfill_max_pending_barrier_num(opts);

        let IndependentCheckpointJobControl::CreatingStreamingJob(job) = entry.insert(
            IndependentCheckpointJobControl::CreatingStreamingJob(Self {
                partial_graph_id,
                job_id,
                snapshot_backfill_upstream_tables,
                max_committed_epoch: None,
                snapshot_epoch,
                status: CreatingStreamingJobStatus::PlaceHolder, // filled in later code
                max_lagged_barrier_num,
                max_pending_barrier_num,
                upstream_lag: GLOBAL_META_METRICS
                    .snapshot_backfill_lag
                    .with_guarded_label_values(&[&format!("{}", job_id)]),
                node_actors,
                state_table_ids,
            }),
        ) else {
            unreachable!()
        };

        let mut graph_adder = partial_graph_manager.add_partial_graph(
            partial_graph_id,
            term_id,
            CreatingStreamingJobBarrierStats::new(job_id, snapshot_epoch),
        );

        if let Err(e) = Self::inject_barrier(
            partial_graph_id,
            graph_adder.manager(),
            &job.node_actors,
            &job.state_table_ids,
            false,
            initial_barrier_info,
            Some(actors_to_create),
            Some(initial_mutation),
            notifier,
            Some(create_info),
        ) {
            graph_adder.failed();
            job.status = CreatingStreamingJobStatus::Resetting(vec![]);
            Err(e)
        } else {
            graph_adder.added();
            let job_info = CreatingJobInfo {
                fragment_infos,
                upstream_fragment_downstreams: info.upstream_fragment_downstreams.clone(),
                downstreams: info.stream_job_fragments.downstreams,
                snapshot_backfill_upstream_tables: job.snapshot_backfill_upstream_tables.clone(),
                stream_actors: actors
                    .stream_actors
                    .values()
                    .flatten()
                    .map(|actor| (actor.actor_id, actor.clone()))
                    .collect(),
            };
            if let Some(log_store_barriers_to_inject) = log_store_barriers_to_inject {
                job.status = CreatingStreamingJobStatus::ConsumingLogStore {
                    tracking_job: TrackingJob::recovered(job_id, &job_info.fragment_infos),
                    info: job_info,
                    pending_barriers: log_store_barriers_to_inject.into(),
                };
            } else {
                assert!(pending_non_checkpoint_barriers.is_empty());
                job.status = CreatingStreamingJobStatus::ConsumingSnapshot {
                    prev_epoch_fake_physical_time,
                    pending_upstream_barriers: vec![],
                    version_stats: version_stat.clone(),
                    create_mview_tracker,
                    snapshot_epoch,
                    info: job_info,
                    pending_non_checkpoint_barriers,
                };
            };
            Ok(job)
        }
    }

    pub(super) fn gen_fragment_backfill_progress(&self) -> Vec<FragmentBackfillProgress> {
        match &self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot {
                create_mview_tracker,
                info,
                ..
            } => create_mview_tracker.collect_fragment_progress(&info.fragment_infos, true),
            CreatingStreamingJobStatus::ConsumingLogStore { info, .. } => {
                collect_done_fragments(self.job_id, &info.fragment_infos)
            }
            CreatingStreamingJobStatus::Finishing(_, _)
            | CreatingStreamingJobStatus::Resetting(_)
            | CreatingStreamingJobStatus::PlaceHolder => vec![],
        }
    }

    fn resolve_upstream_log_epochs(
        snapshot_backfill_upstream_tables: &HashSet<TableId>,
        upstream_table_log_epochs: &UpstreamTableLogEpochs,
        exclusive_start_log_epoch: u64,
        upstream_barrier_info: &BarrierInfo,
    ) -> MetaResult<Vec<BarrierInfo>> {
        let table_id = snapshot_backfill_upstream_tables
            .iter()
            .next()
            .expect("snapshot backfill job should have upstream");
        let epochs_iter = if let Some(epochs) = upstream_table_log_epochs.get(table_id) {
            let mut epochs_iter = epochs.iter();
            loop {
                let (_, checkpoint_epoch) =
                    epochs_iter.next().expect("not reach committed epoch yet");
                if *checkpoint_epoch < exclusive_start_log_epoch {
                    continue;
                }
                assert_eq!(*checkpoint_epoch, exclusive_start_log_epoch);
                break;
            }
            epochs_iter
        } else {
            // snapshot backfill job has been marked as creating, but upstream table has not committed a new epoch yet, so no table change log
            assert_eq!(
                upstream_barrier_info.prev_epoch(),
                exclusive_start_log_epoch
            );
            static EMPTY_VEC: Vec<(Vec<u64>, u64)> = Vec::new();
            EMPTY_VEC.iter()
        };

        let mut ret = vec![];
        let mut prev_epoch = exclusive_start_log_epoch;
        let mut pending_non_checkpoint_barriers = vec![];
        for (non_checkpoint_epochs, checkpoint_epoch) in epochs_iter {
            for (i, epoch) in non_checkpoint_epochs
                .iter()
                .chain([checkpoint_epoch])
                .enumerate()
            {
                assert!(*epoch > prev_epoch);
                pending_non_checkpoint_barriers.push(prev_epoch);
                ret.push(BarrierInfo {
                    prev_epoch: TracedEpoch::new(Epoch(prev_epoch)),
                    curr_epoch: TracedEpoch::new(Epoch(*epoch)),
                    kind: if i == 0 {
                        BarrierKind::Checkpoint(take(&mut pending_non_checkpoint_barriers))
                    } else {
                        BarrierKind::Barrier
                    },
                });
                prev_epoch = *epoch;
            }
        }
        ret.push(BarrierInfo {
            prev_epoch: TracedEpoch::new(Epoch(prev_epoch)),
            curr_epoch: TracedEpoch::new(Epoch(upstream_barrier_info.curr_epoch())),
            kind: BarrierKind::Checkpoint(pending_non_checkpoint_barriers),
        });
        Ok(ret)
    }

    /// Resolves the log-store barriers that must be injected before the create barrier.
    ///
    /// Example with pending upstream barriers:
    ///
    /// ```text
    /// snapshot epoch: 60
    /// changelog after snapshot: [61, 62, 63, 64] + 65
    /// pending upstream barriers: 66 -> 67 barrier, 67 -> 68 barrier,
    ///                            68 -> 69 barrier, 69 -> 70 barrier
    /// new create barrier: 70 -> 71
    ///
    /// injected: 60 -> 61 checkpoint, 61 -> 62 barrier, ..., 64 -> 65 barrier,
    ///           65 -> 66 checkpoint, 66 -> 67 barrier, 67 -> 68 barrier, ...,
    ///           69 -> 70 barrier
    /// current create barrier later injects: 70 -> 71 checkpoint
    /// ```
    ///
    /// Example without pending upstream barriers:
    ///
    /// ```text
    /// snapshot epoch: 60
    /// changelog after snapshot: [61, 62, 63, 64] + 65
    /// new create barrier: 66 -> 67
    ///
    /// injected: 60 -> 61 checkpoint, 61 -> 62 barrier, ..., 64 -> 65 barrier,
    ///           65 -> 66 checkpoint
    /// current create barrier later injects: 66 -> 67 checkpoint
    /// ```
    fn resolve_since_timestamp_upstream_log_epochs(
        upstream_log_epochs: &TableLogEpochs,
        pending_upstream_barriers: impl Iterator<Item = &BarrierInfo>,
        snapshot_epoch: u64,
        new_upstream_barrier_prev_epoch: u64,
    ) -> MetaResult<(BarrierInfo, Vec<BarrierInfo>)> {
        let mut initial_barrier = None;
        let mut barriers = vec![];
        fn emit_barrier(
            initial_barrier: &mut Option<BarrierInfo>,
            barriers: &mut Vec<BarrierInfo>,
            barrier: BarrierInfo,
        ) {
            if initial_barrier.is_none() {
                *initial_barrier = Some(barrier);
            } else {
                barriers.push(barrier);
            }
        }

        let mut prev_epoch = snapshot_epoch;
        let mut pending_non_checkpoint_barriers = vec![];
        for (non_checkpoint_epochs, checkpoint_epoch) in upstream_log_epochs {
            for (i, epoch) in non_checkpoint_epochs
                .iter()
                .chain([checkpoint_epoch])
                .enumerate()
            {
                assert!(
                    *epoch > prev_epoch,
                    "changelog epochs should be strictly increasing"
                );
                pending_non_checkpoint_barriers.push(prev_epoch);
                emit_barrier(
                    &mut initial_barrier,
                    &mut barriers,
                    BarrierInfo {
                        prev_epoch: TracedEpoch::new(Epoch(prev_epoch)),
                        curr_epoch: TracedEpoch::new(Epoch(*epoch)),
                        kind: if i == 0 {
                            BarrierKind::Checkpoint(take(&mut pending_non_checkpoint_barriers))
                        } else {
                            BarrierKind::Barrier
                        },
                    },
                );
                prev_epoch = *epoch;
            }
        }

        let mut pending_upstream_barriers = pending_upstream_barriers.peekable();
        pending_non_checkpoint_barriers.push(prev_epoch);
        if pending_upstream_barriers.peek().is_none() {
            assert!(
                new_upstream_barrier_prev_epoch > prev_epoch,
                "new upstream barrier prev epoch should be newer than the latest changelog epoch"
            );
            emit_barrier(
                &mut initial_barrier,
                &mut barriers,
                BarrierInfo {
                    prev_epoch: TracedEpoch::new(Epoch(prev_epoch)),
                    curr_epoch: TracedEpoch::new(Epoch(new_upstream_barrier_prev_epoch)),
                    kind: BarrierKind::Checkpoint(pending_non_checkpoint_barriers),
                },
            );
        } else {
            let first_pending_barrier = pending_upstream_barriers
                .peek()
                .expect("first pending upstream barrier should exist after peek");
            assert!(
                first_pending_barrier.prev_epoch() > prev_epoch,
                "first pending upstream barrier should be newer than the latest resolved changelog epoch"
            );
            emit_barrier(
                &mut initial_barrier,
                &mut barriers,
                BarrierInfo {
                    prev_epoch: TracedEpoch::new(Epoch(prev_epoch)),
                    curr_epoch: TracedEpoch::new(Epoch(first_pending_barrier.prev_epoch())),
                    kind: BarrierKind::Checkpoint(take(&mut pending_non_checkpoint_barriers)),
                },
            );
            prev_epoch = first_pending_barrier.prev_epoch();
            for pending_barrier in pending_upstream_barriers {
                assert_eq!(
                    pending_barrier.prev_epoch(),
                    prev_epoch,
                    "pending upstream barriers should continue from resolved changelog epochs"
                );
                pending_non_checkpoint_barriers.push(prev_epoch);
                emit_barrier(
                    &mut initial_barrier,
                    &mut barriers,
                    BarrierInfo {
                        prev_epoch: TracedEpoch::new(Epoch(prev_epoch)),
                        curr_epoch: TracedEpoch::new(Epoch(pending_barrier.curr_epoch())),
                        kind: if pending_barrier.kind.is_checkpoint() {
                            BarrierKind::Checkpoint(take(&mut pending_non_checkpoint_barriers))
                        } else {
                            BarrierKind::Barrier
                        },
                    },
                );
                prev_epoch = pending_barrier.curr_epoch();
            }
            assert_eq!(
                new_upstream_barrier_prev_epoch, prev_epoch,
                "new upstream barrier prev epoch should match the latest pending log-store epoch"
            );
        }
        let Some(initial_barrier) = initial_barrier else {
            return Err(anyhow::anyhow!(
                "missing lagging barriers for direct log-store start from snapshot epoch {}",
                snapshot_epoch
            )
            .into());
        };
        assert!(initial_barrier.kind.is_checkpoint());
        Ok((initial_barrier, barriers))
    }

    fn recover_consuming_snapshot(
        job_id: JobId,
        upstream_table_log_epochs: &UpstreamTableLogEpochs,
        snapshot_epoch: u64,
        committed_epoch: u64,
        upstream_barrier_info: &BarrierInfo,
        info: CreatingJobInfo,
        backfill_order_state: BackfillOrderState,
        version_stat: &HummockVersionStats,
    ) -> MetaResult<(CreatingStreamingJobStatus, BarrierInfo)> {
        let mut prev_epoch_fake_physical_time = Epoch(committed_epoch).physical_time();
        let mut pending_non_checkpoint_barriers = vec![];
        let create_mview_tracker = CreateMviewProgressTracker::recover(
            job_id,
            &info.fragment_infos,
            backfill_order_state,
            version_stat,
        );
        let barrier_info = CreatingStreamingJobStatus::new_fake_barrier(
            &mut prev_epoch_fake_physical_time,
            &mut pending_non_checkpoint_barriers,
            PbBarrierKind::Initial,
        );
        Ok((
            CreatingStreamingJobStatus::ConsumingSnapshot {
                prev_epoch_fake_physical_time,
                pending_upstream_barriers: Self::resolve_upstream_log_epochs(
                    &info.snapshot_backfill_upstream_tables,
                    upstream_table_log_epochs,
                    snapshot_epoch,
                    upstream_barrier_info,
                )?,
                version_stats: version_stat.clone(),
                create_mview_tracker,
                info,
                snapshot_epoch,
                pending_non_checkpoint_barriers,
            },
            barrier_info,
        ))
    }

    fn recover_consuming_log_store(
        job_id: JobId,
        upstream_table_log_epochs: &UpstreamTableLogEpochs,
        committed_epoch: u64,
        upstream_barrier_info: &BarrierInfo,
        info: CreatingJobInfo,
    ) -> MetaResult<(CreatingStreamingJobStatus, BarrierInfo)> {
        let mut pending_barriers: VecDeque<_> = Self::resolve_upstream_log_epochs(
            &info.snapshot_backfill_upstream_tables,
            upstream_table_log_epochs,
            committed_epoch,
            upstream_barrier_info,
        )?
        .into();
        let mut first_barrier = pending_barriers
            .pop_front()
            .expect("resolved upstream log epochs should not be empty");
        assert!(first_barrier.kind.is_checkpoint());
        first_barrier.kind = BarrierKind::Initial;

        Ok((
            CreatingStreamingJobStatus::ConsumingLogStore {
                tracking_job: TrackingJob::recovered(job_id, &info.fragment_infos),
                pending_barriers,
                info,
            },
            first_barrier,
        ))
    }

    #[expect(clippy::too_many_arguments)]
    pub(crate) fn recover(
        database_id: DatabaseId,
        job_id: JobId,
        snapshot_backfill_upstream_tables: HashSet<TableId>,
        upstream_table_log_epochs: &UpstreamTableLogEpochs,
        snapshot_epoch: u64,
        committed_epoch: u64,
        upstream_barrier_info: &BarrierInfo,
        fragment_infos: HashMap<FragmentId, InflightFragmentInfo>,
        backfill_order: ExtendedFragmentBackfillOrder,
        fragment_relations: &FragmentDownstreamRelation,
        version_stat: &HummockVersionStats,
        stream_actors: &HashMap<ActorId, StreamActor>,
        initial_mutation: Mutation,
        term_id: &str,
        partial_graph_recoverer: &mut PartialGraphRecoverer<'_>,
    ) -> MetaResult<Self> {
        info!(
            %job_id,
            "recovered creating snapshot backfill job"
        );

        let node_actors = InflightFragmentInfo::actor_ids_to_collect(fragment_infos.values());
        let state_table_ids: HashSet<_> =
            InflightFragmentInfo::existing_table_ids(fragment_infos.values()).collect();

        let partial_graph_id = to_partial_graph_id(database_id, Some(job_id));
        let mut builder = FragmentEdgeBuilder::from_inflight_fragments(
            fragment_infos.values(),
            partial_graph_id,
            partial_graph_recoverer.control_stream_manager(),
        );
        builder.add_relations(fragment_relations);
        let mut edges = builder.build();
        let new_actors = edges.collect_actors_to_create(fragment_infos.values().map(|fragment| {
            (
                fragment.fragment_id,
                &fragment.nodes,
                fragment.actors.iter().map(|(actor_id, actor)| {
                    (
                        stream_actors.get(actor_id).expect("should exist"),
                        actor.worker_id,
                    )
                }),
                vec![], // no subscribers for backfilling jobs
            )
        }));

        let mut upstream_fragment_downstreams: FragmentDownstreamRelation = Default::default();
        for (upstream_fragment_id, downstreams) in fragment_relations {
            if fragment_infos.contains_key(upstream_fragment_id) {
                continue;
            }
            for downstream in downstreams {
                if fragment_infos.contains_key(&downstream.downstream_fragment_id) {
                    upstream_fragment_downstreams
                        .entry(*upstream_fragment_id)
                        .or_default()
                        .push(downstream.clone());
                }
            }
        }
        let downstreams = fragment_infos
            .keys()
            .filter_map(|fragment_id| {
                fragment_relations
                    .get(fragment_id)
                    .map(|relation| (*fragment_id, relation.clone()))
            })
            .collect();
        let job_stream_actors = fragment_infos
            .values()
            .flat_map(|fragment| fragment.actors.keys())
            .map(|actor_id| {
                (
                    *actor_id,
                    stream_actors.get(actor_id).expect("should exist").clone(),
                )
            })
            .collect();

        let info = CreatingJobInfo {
            fragment_infos,
            upstream_fragment_downstreams,
            downstreams,
            snapshot_backfill_upstream_tables: snapshot_backfill_upstream_tables.clone(),
            stream_actors: job_stream_actors,
        };

        let (status, first_barrier_info) = if committed_epoch < snapshot_epoch {
            let locality_fragment_state_table_mapping =
                build_locality_fragment_state_table_mapping(&info.fragment_infos);
            let backfill_order_state = BackfillOrderState::recover_from_fragment_infos(
                &backfill_order,
                &info.fragment_infos,
                locality_fragment_state_table_mapping,
            );
            Self::recover_consuming_snapshot(
                job_id,
                upstream_table_log_epochs,
                snapshot_epoch,
                committed_epoch,
                upstream_barrier_info,
                info,
                backfill_order_state,
                version_stat,
            )?
        } else {
            Self::recover_consuming_log_store(
                job_id,
                upstream_table_log_epochs,
                committed_epoch,
                upstream_barrier_info,
                info,
            )?
        };

        let max_lagged_barrier_num = partial_graph_recoverer
            .control_stream_manager()
            .env
            .opts
            .snapshot_backfill_finish_max_lagged_barriers;
        let opts = &partial_graph_recoverer.control_stream_manager().env.opts;
        let max_pending_barrier_num = snapshot_backfill_max_pending_barrier_num(opts);

        partial_graph_recoverer.recover_graph(
            partial_graph_id,
            term_id,
            initial_mutation,
            &first_barrier_info,
            &node_actors,
            state_table_ids.iter().copied(),
            new_actors,
            CreatingStreamingJobBarrierStats::new(job_id, snapshot_epoch),
        )?;

        Ok(Self {
            job_id,
            partial_graph_id,
            snapshot_backfill_upstream_tables,
            snapshot_epoch,
            node_actors,
            state_table_ids,
            max_committed_epoch: Some(committed_epoch),
            status,
            max_lagged_barrier_num,
            max_pending_barrier_num,
            upstream_lag: GLOBAL_META_METRICS
                .snapshot_backfill_lag
                .with_guarded_label_values(&[&format!("{}", job_id)]),
        })
    }

    pub(crate) fn gen_backfill_progress(&self) -> BackfillProgress {
        let progress = match &self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot {
                create_mview_tracker,
                ..
            } => {
                if create_mview_tracker.is_finished() {
                    "Snapshot finished".to_owned()
                } else {
                    let progress = create_mview_tracker.gen_backfill_progress();
                    format!("Snapshot [{}]", progress)
                }
            }
            CreatingStreamingJobStatus::ConsumingLogStore {
                pending_barriers, ..
            } => format!("LogStore [pending barriers: {}]", pending_barriers.len()),
            CreatingStreamingJobStatus::Finishing(finish_epoch, ..) => {
                let committed_epoch = self.max_committed_epoch.expect("should have committed");
                let lag = Duration::from_millis(
                    Epoch(*finish_epoch).physical_time() - Epoch(committed_epoch).physical_time(),
                );
                format!("Finishing [epoch lag: {lag:?}]",)
            }
            CreatingStreamingJobStatus::Resetting(_) => "Resetting".to_owned(),
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        };
        BackfillProgress {
            progress,
            backfill_type: PbBackfillType::SnapshotBackfill,
        }
    }

    pub(super) fn pinned_upstream_tables(&self) -> HashSet<TableId> {
        self.snapshot_backfill_upstream_tables.clone()
    }

    fn inject_barrier(
        partial_graph_id: PartialGraphId,
        partial_graph_manager: &mut PartialGraphManager,
        node_actors: &HashMap<WorkerId, HashSet<ActorId>>,
        state_table_ids: &HashSet<TableId>,
        is_finishing: bool,
        barrier_info: BarrierInfo,
        new_actors: Option<StreamJobActorsToCreate>,
        mutation: Option<Mutation>,
        notifier: Option<&mut NotifierStarter>,
        first_create_info: Option<CreateSnapshotBackfillJobCommandInfo>,
    ) -> MetaResult<()> {
        let (table_ids_to_sync, nodes_to_sync_table) = if !is_finishing {
            (Some(state_table_ids), Some(node_actors.keys().copied()))
        } else {
            (None, None)
        };
        partial_graph_manager.inject_barrier(
            partial_graph_id,
            mutation,
            None,
            node_actors,
            table_ids_to_sync.into_iter().flatten().copied(),
            nodes_to_sync_table.into_iter().flatten(),
            new_actors,
            PartialGraphBarrierInfo::new(
                first_create_info.map_or_else(
                    PostCollectCommand::barrier,
                    CreateSnapshotBackfillJobCommandInfo::into_post_collect,
                ),
                barrier_info,
                notifier,
                state_table_ids.clone(),
            ),
        )?;
        Ok(())
    }

    pub(crate) fn start_consume_upstream(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        barrier_info: &BarrierInfo,
    ) -> MetaResult<CreatingJobInfo> {
        info!(
            job_id = %self.job_id,
            prev_epoch = barrier_info.prev_epoch(),
            "start consuming upstream"
        );
        let info = self.status.start_consume_upstream(barrier_info);
        Self::inject_barrier(
            self.partial_graph_id,
            partial_graph_manager,
            &self.node_actors,
            &self.state_table_ids,
            true,
            barrier_info.clone(),
            None,
            Some(Mutation::Stop(StopMutation {
                // stop all actors
                actors: info
                    .fragment_infos
                    .values()
                    .flat_map(|info| info.actors.keys().copied())
                    .collect(),
                dropped_sink_fragments: vec![], // not related to sink-into-table
            })),
            None, // no notifier when start consuming upstream
            None,
        )?;
        Ok(info)
    }

    pub(crate) fn on_new_upstream_barrier(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        barrier_info: &BarrierInfo,
        mutation: Option<(Mutation, Option<&mut NotifierStarter>)>,
    ) -> MetaResult<()> {
        let progress_epoch = if let Some(max_committed_epoch) = self.max_committed_epoch {
            max(max_committed_epoch, self.snapshot_epoch)
        } else {
            self.snapshot_epoch
        };
        self.upstream_lag.set(
            barrier_info
                .prev_epoch
                .value()
                .0
                .saturating_sub(progress_epoch) as _,
        );
        let (mut mutation, mut notifier) = match mutation {
            Some((mutation, notifier)) => (Some(mutation), notifier),
            None => (None, None),
        };
        for (barrier_to_inject, mutation) in self.status.on_new_upstream_epoch(
            partial_graph_manager,
            self.partial_graph_id,
            self.max_pending_barrier_num,
            barrier_info,
            mutation.take(),
        ) {
            Self::inject_barrier(
                self.partial_graph_id,
                partial_graph_manager,
                &self.node_actors,
                &self.state_table_ids,
                false,
                barrier_to_inject,
                None,
                mutation,
                notifier.take(),
                None,
            )?;
        }
        Ok(())
    }

    pub(crate) fn pre_apply_throttle(
        &mut self,
        config: &mut ThrottleConfigMap,
    ) -> Option<Mutation> {
        self.status.pre_apply_throttle(config)
    }

    /// Returns whether the next barrier should be forced to a checkpoint.
    pub(crate) fn collect(&mut self, collected_barrier: CollectedBarrier<'_>) -> bool {
        let pending_barrier_num = collected_barrier.pending_barrier_num;
        self.status.update_progress(
            collected_barrier
                .resps
                .values()
                .flat_map(|resp| &resp.create_mview_progress),
        );
        self.is_ready_to_merge() && pending_barrier_num <= self.max_lagged_barrier_num
    }

    fn is_ready_to_merge(&self) -> bool {
        if let CreatingStreamingJobStatus::ConsumingLogStore {
            pending_barriers, ..
        } = &self.status
        {
            pending_barriers.is_empty()
        } else {
            false
        }
    }

    pub(crate) fn should_merge_to_upstream(
        &self,
        partial_graph_manager: &PartialGraphManager,
    ) -> bool {
        if !self.is_ready_to_merge() {
            return false;
        }

        // A job that is ready to merge has finished initialization and is not resetting, so its
        // partial graph must be running.
        partial_graph_manager.pending_barrier_num(self.partial_graph_id)
            <= self.max_lagged_barrier_num
    }
}

impl CreatingStreamingJobControl {
    pub(crate) fn start_completing(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        min_upstream_inflight_epoch: Option<u64>,
        upstream_committed_epoch: u64,
    ) -> Option<(
        u64,
        HashMap<WorkerId, BarrierCompleteResponse>,
        PartialGraphBarrierInfo,
        bool,
    )> {
        // do not commit snapshot backfill job until upstream has committed the snapshot epoch
        if upstream_committed_epoch < self.snapshot_epoch {
            return None;
        }
        let (finished_at_epoch, epoch_end_bound) = match &self.status {
            CreatingStreamingJobStatus::Finishing(finish_at_epoch, _) => {
                let epoch_end_bound = min_upstream_inflight_epoch
                    .map(|upstream_epoch| {
                        if upstream_epoch < *finish_at_epoch {
                            Excluded(upstream_epoch)
                        } else {
                            Unbounded
                        }
                    })
                    .unwrap_or(Unbounded);
                (Some(*finish_at_epoch), epoch_end_bound)
            }
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. } => (
                None,
                min_upstream_inflight_epoch
                    .map(Excluded)
                    .unwrap_or(Unbounded),
            ),
            CreatingStreamingJobStatus::Resetting(..) => {
                return None;
            }
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        };
        partial_graph_manager
            .start_completing(
                self.partial_graph_id,
                epoch_end_bound,
                |non_checkpoint_epoch, _, _| {
                    if let Some(finish_at_epoch) = finished_at_epoch {
                        assert!(non_checkpoint_epoch.prev < finish_at_epoch);
                    }
                },
            )
            .map(|(epoch, resps, info)| {
                let is_finish_epoch = if let Some(finish_at_epoch) = finished_at_epoch {
                    assert!(!info.post_collect_command.should_checkpoint());
                    if epoch == finish_at_epoch {
                        // TODO: can early remove partial graph here
                        self.ack_completed(partial_graph_manager, epoch);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };
                (epoch, resps, info, is_finish_epoch)
            })
    }

    pub(super) fn ack_completed(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        completed_epoch: u64,
    ) {
        match &self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. }
            | CreatingStreamingJobStatus::Finishing(_, _) => {
                partial_graph_manager.ack_completed(self.partial_graph_id, completed_epoch);
                if let Some(prev_max_committed_epoch) =
                    self.max_committed_epoch.replace(completed_epoch)
                {
                    assert!(completed_epoch > prev_max_committed_epoch);
                }
            }
            CreatingStreamingJobStatus::Resetting(_) => {
                // The job was dropped while the completing task was running in the background.
                // The partial graph has already been reset, so skip the ack.
            }
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        }
    }

    pub(crate) fn fragment_infos(&self) -> Option<&HashMap<FragmentId, InflightFragmentInfo>> {
        self.status.fragment_infos()
    }

    pub fn into_tracking_job(self) -> TrackingJob {
        match self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. }
            | CreatingStreamingJobStatus::Resetting(..)
            | CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!("expect finish")
            }
            CreatingStreamingJobStatus::Finishing(_, tracking_job) => tracking_job,
        }
    }

    pub(super) fn on_partial_graph_reset(mut self) {
        match &mut self.status {
            CreatingStreamingJobStatus::Resetting(notifiers) => {
                for notifier in notifiers.drain(..) {
                    notifier.notify_collected();
                }
            }
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. }
            | CreatingStreamingJobStatus::Finishing(_, _) => {
                panic!(
                    "should be resetting when receiving reset partial graph resp, but at {:?}",
                    self.status
                )
            }
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        }
    }

    /// Drop a creating snapshot backfill job by directly resetting the partial graph
    /// Return `false` if the partial graph has been merged to upstream database, and `true` otherwise
    /// to mean that the job has been dropped.
    pub(super) fn drop(
        &mut self,
        notifier: Option<&mut NotifierStarter>,
        partial_graph_manager: &mut PartialGraphManager,
    ) -> bool {
        match &mut self.status {
            CreatingStreamingJobStatus::Resetting(existing_notifiers) => {
                existing_notifiers.extend(notifier.map(NotifierStarter::add_notify));
                true
            }
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. } => {
                partial_graph_manager.reset_partial_graphs([self.partial_graph_id]);
                self.status = CreatingStreamingJobStatus::Resetting(
                    notifier
                        .map(NotifierStarter::add_notify)
                        .into_iter()
                        .collect(),
                );
                true
            }
            CreatingStreamingJobStatus::Finishing(_, _) => false,
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        }
    }

    pub(crate) fn reset(self) -> bool {
        match self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. }
            | CreatingStreamingJobStatus::Finishing(_, _) => false,
            CreatingStreamingJobStatus::Resetting(notifiers) => {
                for notifier in notifiers {
                    notifier.notify_collected();
                }
                true
            }
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_backfill_max_pending_barrier_num() {
        let mut opts = MetaOpts::test(false);
        opts.in_flight_barrier_nums = 10;

        opts.snapshot_backfill_barrier_amplification_factor = 0;
        assert_eq!(snapshot_backfill_max_pending_barrier_num(&opts), 10);

        opts.snapshot_backfill_barrier_amplification_factor = 1;
        assert_eq!(snapshot_backfill_max_pending_barrier_num(&opts), 10);

        opts.snapshot_backfill_barrier_amplification_factor = 10;
        assert_eq!(snapshot_backfill_max_pending_barrier_num(&opts), 100);

        opts.in_flight_barrier_nums = usize::MAX;
        assert_eq!(snapshot_backfill_max_pending_barrier_num(&opts), usize::MAX);
    }

    #[test]
    fn test_resolve_since_timestamp_upstream_log_epochs() {
        let upstream_log_epochs = vec![(vec![45, 50], 55)];

        let (initial_barrier, barriers) =
            CreatingStreamingJobControl::resolve_since_timestamp_upstream_log_epochs(
                &upstream_log_epochs,
                [].iter(),
                40,
                60,
            )
            .unwrap();

        assert_eq!(
            (initial_barrier.prev_epoch(), initial_barrier.curr_epoch()),
            (40, 45)
        );
        assert!(initial_barrier.kind.is_checkpoint());
        assert_eq!(
            barriers
                .iter()
                .map(|barrier| (barrier.prev_epoch(), barrier.curr_epoch()))
                .collect::<Vec<_>>(),
            vec![(45, 50), (50, 55), (55, 60)]
        );
        assert_eq!(
            barriers
                .iter()
                .map(|barrier| match &barrier.kind {
                    BarrierKind::Checkpoint(epochs) => Some(epochs.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![None, None, Some(vec![45, 50, 55])]
        );
    }

    #[test]
    fn test_resolve_since_timestamp_upstream_log_epochs_with_pending_barriers() {
        let upstream_log_epochs = vec![(vec![45, 50], 55)];
        let pending_upstream_barriers = [
            BarrierInfo {
                prev_epoch: TracedEpoch::new(Epoch(60)),
                curr_epoch: TracedEpoch::new(Epoch(65)),
                kind: BarrierKind::Barrier,
            },
            BarrierInfo {
                prev_epoch: TracedEpoch::new(Epoch(65)),
                curr_epoch: TracedEpoch::new(Epoch(70)),
                kind: BarrierKind::Checkpoint(vec![60, 65]),
            },
        ];

        let (initial_barrier, barriers) =
            CreatingStreamingJobControl::resolve_since_timestamp_upstream_log_epochs(
                &upstream_log_epochs,
                pending_upstream_barriers.iter(),
                40,
                70,
            )
            .unwrap();

        assert_eq!(
            (initial_barrier.prev_epoch(), initial_barrier.curr_epoch()),
            (40, 45)
        );
        assert!(initial_barrier.kind.is_checkpoint());
        assert_eq!(
            barriers
                .iter()
                .map(|barrier| (barrier.prev_epoch(), barrier.curr_epoch()))
                .collect::<Vec<_>>(),
            vec![(45, 50), (50, 55), (55, 60), (60, 65), (65, 70)]
        );
        assert_eq!(
            barriers
                .iter()
                .map(|barrier| match &barrier.kind {
                    BarrierKind::Checkpoint(epochs) => Some(epochs.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![None, None, Some(vec![45, 50, 55]), None, Some(vec![60, 65])]
        );
    }

    #[test]
    fn test_resolve_since_timestamp_upstream_log_epochs_with_gap_before_pending_barriers() {
        let upstream_log_epochs = vec![(vec![61, 62, 63, 64], 65)];
        let pending_upstream_barriers = [
            BarrierInfo {
                prev_epoch: TracedEpoch::new(Epoch(66)),
                curr_epoch: TracedEpoch::new(Epoch(67)),
                kind: BarrierKind::Barrier,
            },
            BarrierInfo {
                prev_epoch: TracedEpoch::new(Epoch(67)),
                curr_epoch: TracedEpoch::new(Epoch(68)),
                kind: BarrierKind::Barrier,
            },
            BarrierInfo {
                prev_epoch: TracedEpoch::new(Epoch(68)),
                curr_epoch: TracedEpoch::new(Epoch(69)),
                kind: BarrierKind::Barrier,
            },
            BarrierInfo {
                prev_epoch: TracedEpoch::new(Epoch(69)),
                curr_epoch: TracedEpoch::new(Epoch(70)),
                kind: BarrierKind::Barrier,
            },
        ];

        let (initial_barrier, barriers) =
            CreatingStreamingJobControl::resolve_since_timestamp_upstream_log_epochs(
                &upstream_log_epochs,
                pending_upstream_barriers.iter(),
                60,
                70,
            )
            .unwrap();

        assert_eq!(
            (initial_barrier.prev_epoch(), initial_barrier.curr_epoch()),
            (60, 61)
        );
        assert!(initial_barrier.kind.is_checkpoint());
        assert_eq!(
            barriers
                .iter()
                .map(|barrier| (barrier.prev_epoch(), barrier.curr_epoch()))
                .collect::<Vec<_>>(),
            vec![
                (61, 62),
                (62, 63),
                (63, 64),
                (64, 65),
                (65, 66),
                (66, 67),
                (67, 68),
                (68, 69),
                (69, 70)
            ]
        );
        assert_eq!(
            barriers
                .iter()
                .map(|barrier| match &barrier.kind {
                    BarrierKind::Checkpoint(epochs) => Some(epochs.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![
                None,
                None,
                None,
                None,
                Some(vec![61, 62, 63, 64, 65]),
                None,
                None,
                None,
                None
            ]
        );
    }

    #[test]
    fn test_resolve_since_timestamp_upstream_log_epochs_without_pending_barriers() {
        let upstream_log_epochs = vec![(vec![61, 62, 63, 64], 65)];

        let (initial_barrier, barriers) =
            CreatingStreamingJobControl::resolve_since_timestamp_upstream_log_epochs(
                &upstream_log_epochs,
                [].iter(),
                60,
                66,
            )
            .unwrap();

        assert_eq!(
            (initial_barrier.prev_epoch(), initial_barrier.curr_epoch()),
            (60, 61)
        );
        assert!(initial_barrier.kind.is_checkpoint());
        assert_eq!(
            barriers
                .iter()
                .map(|barrier| (barrier.prev_epoch(), barrier.curr_epoch()))
                .collect::<Vec<_>>(),
            vec![(61, 62), (62, 63), (63, 64), (64, 65), (65, 66)]
        );
        assert_eq!(
            barriers
                .iter()
                .map(|barrier| match &barrier.kind {
                    BarrierKind::Checkpoint(epochs) => Some(epochs.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![None, None, None, None, Some(vec![61, 62, 63, 64, 65])]
        );
    }
}
