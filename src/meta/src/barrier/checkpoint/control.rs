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
use std::future::{Future, poll_fn};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::AtomicU32;
use std::task::Poll;

use anyhow::anyhow;
use fail::fail_point;
use itertools::Itertools;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::id::JobId;
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntGauge};
use risingwave_common::util::epoch::{Epoch, EpochPair};
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont;
use risingwave_meta_model::WorkerId;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::id::{FragmentId, PartialGraphId};
use risingwave_pb::stream_plan::DispatcherType as PbDispatcherType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use risingwave_pb::stream_service::streaming_control_stream_response::ResetPartialGraphResponse;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::barrier::cdc_progress::CdcProgress;
use crate::barrier::checkpoint::independent_job::{
    BatchRefreshJobTriggerContext, IndependentCheckpointJobControl,
};
use crate::barrier::checkpoint::recovery::{
    DatabaseRecoveringState, DatabaseStatusAction, EnterInitializing, EnterRunning,
    RecoveringStateAction,
};
use crate::barrier::checkpoint::state::{ApplyCommandInfo, BarrierWorkerState};
use crate::barrier::complete_task::{BarrierCompleteOutput, CompleteBarrierTask};
use crate::barrier::info::{InflightDatabaseInfo, SharedActorInfos};
use crate::barrier::notifier::Notifier;
use crate::barrier::partial_graph::{CollectedBarrier, PartialGraphManager, PartialGraphStat};
use crate::barrier::progress::TrackingJob;
use crate::barrier::rpc::{from_partial_graph_id, to_partial_graph_id};
use crate::barrier::schedule::{NewBarrier, PeriodicBarriers};
use crate::barrier::utils::{BarrierItemCollector, collect_independent_job_commit_epoch_info};
use crate::barrier::{
    BackfillProgress, Command, CreateStreamingJobType, FragmentBackfillProgress, Reschedule,
};
use crate::controller::fragment::InflightFragmentInfo;
use crate::controller::scale::{build_no_shuffle_fragment_graph_edges, find_no_shuffle_graphs};
use crate::manager::MetaSrvEnv;

fn fragment_has_online_unreschedulable_scan(fragment: &InflightFragmentInfo) -> bool {
    let mut has_unreschedulable_scan = false;
    visit_stream_node_cont(&fragment.nodes, |node| {
        if let Some(NodeBody::StreamScan(stream_scan)) = node.node_body.as_ref() {
            let scan_type = stream_scan.stream_scan_type();
            if !scan_type.is_reschedulable(true) {
                has_unreschedulable_scan = true;
                return false;
            }
        }
        true
    });
    has_unreschedulable_scan
}

fn collect_fragment_upstream_fragment_ids(
    fragment: &InflightFragmentInfo,
    upstream_fragment_ids: &mut HashSet<FragmentId>,
) {
    visit_stream_node_cont(&fragment.nodes, |node| {
        if let Some(NodeBody::Merge(merge)) = node.node_body.as_ref() {
            upstream_fragment_ids.insert(merge.upstream_fragment_id);
        }
        true
    });
}

use crate::model::ActorId;
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::{MetaError, MetaResult};

pub(crate) struct CheckpointControl {
    pub(crate) env: MetaSrvEnv,
    pub(super) databases: HashMap<DatabaseId, DatabaseCheckpointControlStatus>,
    pub(super) hummock_version_stats: HummockVersionStats,
    /// The maximum number of pending barriers in each partial graph.
    pub(crate) in_flight_barrier_nums: usize,
}

impl CheckpointControl {
    pub fn new(env: MetaSrvEnv) -> Self {
        Self {
            in_flight_barrier_nums: env.opts.in_flight_barrier_nums,
            env,
            databases: Default::default(),
            hummock_version_stats: Default::default(),
        }
    }

    pub(crate) fn recover(
        databases: HashMap<DatabaseId, DatabaseCheckpointControl>,
        failed_databases: HashMap<DatabaseId, HashSet<PartialGraphId>>, /* `database_id` -> set of resetting partial graph ids */
        hummock_version_stats: HummockVersionStats,
        env: MetaSrvEnv,
    ) -> Self {
        env.shared_actor_infos()
            .retain_databases(databases.keys().chain(failed_databases.keys()).cloned());
        Self {
            in_flight_barrier_nums: env.opts.in_flight_barrier_nums,
            env,
            databases: databases
                .into_iter()
                .map(|(database_id, control)| {
                    (
                        database_id,
                        DatabaseCheckpointControlStatus::Running(control),
                    )
                })
                .chain(failed_databases.into_iter().map(
                    |(database_id, resetting_partial_graphs)| {
                        (
                            database_id,
                            DatabaseCheckpointControlStatus::Recovering(
                                DatabaseRecoveringState::new_resetting(
                                    database_id,
                                    resetting_partial_graphs,
                                ),
                            ),
                        )
                    },
                ))
                .collect(),
            hummock_version_stats,
        }
    }

    pub(crate) fn ack_completed(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        output: BarrierCompleteOutput,
    ) {
        self.hummock_version_stats = output.hummock_version_stats;
        for (database_id, (command_prev_epoch, independent_job_epochs)) in output.epochs_to_ack {
            self.databases
                .get_mut(&database_id)
                .expect("should exist")
                .expect_running("should have wait for completing command before enter recovery")
                .ack_completed(
                    partial_graph_manager,
                    command_prev_epoch,
                    independent_job_epochs,
                );
        }
    }

    pub(crate) fn next_complete_barrier_task(
        &mut self,
        periodic_barriers: &mut PeriodicBarriers,
        partial_graph_manager: &mut PartialGraphManager,
    ) -> Option<CompleteBarrierTask> {
        let mut task = None;
        for database in self.databases.values_mut() {
            let Some(database) = database.running_state_mut() else {
                continue;
            };
            database.next_complete_barrier_task(
                periodic_barriers,
                partial_graph_manager,
                &mut task,
                &self.hummock_version_stats,
            );
        }
        task
    }

    pub(crate) fn barrier_collected(
        &mut self,
        partial_graph_id: PartialGraphId,
        collected_barrier: CollectedBarrier<'_>,
        periodic_barriers: &mut PeriodicBarriers,
    ) -> MetaResult<()> {
        let (database_id, _) = from_partial_graph_id(partial_graph_id);
        let database_status = self.databases.get_mut(&database_id).expect("should exist");
        match database_status {
            DatabaseCheckpointControlStatus::Running(database) => {
                database.barrier_collected(partial_graph_id, collected_barrier, periodic_barriers)
            }
            DatabaseCheckpointControlStatus::Recovering(_) => {
                if cfg!(debug_assertions) {
                    panic!(
                        "receive collected barrier {:?} on recovering database {} from partial graph {}",
                        collected_barrier, database_id, partial_graph_id
                    );
                } else {
                    warn!(?collected_barrier, %partial_graph_id, "ignore collected barrier on recovering database");
                }
                Ok(())
            }
        }
    }

    pub(crate) fn recovering_databases(&self) -> impl Iterator<Item = DatabaseId> + '_ {
        self.databases.iter().filter_map(|(database_id, database)| {
            database.running_state().is_none().then_some(*database_id)
        })
    }

    pub(crate) fn running_databases(&self) -> impl Iterator<Item = DatabaseId> + '_ {
        self.databases.iter().filter_map(|(database_id, database)| {
            database.running_state().is_some().then_some(*database_id)
        })
    }

    pub(crate) fn contains_database(&self, database_id: DatabaseId) -> bool {
        self.databases.contains_key(&database_id)
    }

    pub(crate) fn database_info(&self, database_id: DatabaseId) -> Option<&InflightDatabaseInfo> {
        self.databases
            .get(&database_id)
            .and_then(|database| database.running_state())
            .map(|database| &database.database_info)
    }

    /// return Some(failed `database_id` -> `err`)
    pub(crate) fn handle_new_barrier(
        &mut self,
        new_barrier: NewBarrier,
        partial_graph_manager: &mut PartialGraphManager,
        worker_nodes: &HashMap<WorkerId, WorkerNode>,
    ) -> MetaResult<()> {
        let NewBarrier {
            database_id,
            command,
            span,
            checkpoint,
        } = new_barrier;

        if let Some((mut command, notifier)) = command {
            if let &mut Command::CreateStreamingJob {
                ref mut cross_db_snapshot_backfill_info,
                ref info,
                ..
            } = &mut command
            {
                for (table_id, snapshot_epoch) in
                    &mut cross_db_snapshot_backfill_info.upstream_mv_table_id_to_backfill_epoch
                {
                    for database in self.databases.values() {
                        if let Some(database) = database.running_state()
                            && database.database_info.contains_job(table_id.as_job_id())
                        {
                            if let Some(committed_epoch) = database.committed_epoch {
                                *snapshot_epoch = Some(committed_epoch);
                            }
                            break;
                        }
                    }
                    if snapshot_epoch.is_none() {
                        let table_id = *table_id;
                        warn!(
                            ?cross_db_snapshot_backfill_info,
                            ?table_id,
                            ?info,
                            "database of cross db upstream table not found"
                        );
                        let err: MetaError =
                            anyhow!("database of cross db upstream table {} not found", table_id)
                                .into();
                        notifier.notify_start_failed(err);

                        return Ok(());
                    }
                }
            }

            let database = match self.databases.entry(database_id) {
                Entry::Occupied(entry) => entry
                    .into_mut()
                    .expect_running("should not have command when not running"),
                Entry::Vacant(entry) => match &command {
                    Command::CreateStreamingJob { info, job_type, .. } => {
                        let CreateStreamingJobType::Normal = job_type else {
                            if cfg!(debug_assertions) {
                                panic!(
                                    "unexpected first job of type {job_type:?} with info {info:?}"
                                );
                            } else {
                                notifier.notify_start_failed(anyhow!("unexpected job_type {job_type:?} for first job {} in database {database_id}", info.streaming_job.id()).into());
                                return Ok(());
                            }
                        };
                        let new_database = DatabaseCheckpointControl::new(
                            database_id,
                            self.env.shared_actor_infos().clone(),
                        );
                        let adder = partial_graph_manager.add_partial_graph(
                            to_partial_graph_id(database_id, None),
                            new_database.term_id(),
                            DatabaseCheckpointControlMetrics::new(database_id),
                        );
                        adder.added();
                        entry
                            .insert(DatabaseCheckpointControlStatus::Running(new_database))
                            .expect_running("just initialized as running")
                    }
                    Command::Flush
                    | Command::Pause
                    | Command::Resume
                    | Command::DropStreamingJobs { .. }
                    | Command::DropSubscription { .. } => {
                        notifier.start().started();
                        warn!(?command, "skip command for empty database");
                        return Ok(());
                    }
                    Command::RescheduleIntent { .. }
                    | Command::ReplaceStreamJob(_)
                    | Command::SourceChangeSplit(_)
                    | Command::Throttle { .. }
                    | Command::CreateSubscription { .. }
                    | Command::AlterSubscriptionRetention { .. }
                    | Command::ConnectorPropsChange(_)
                    | Command::Refresh { .. }
                    | Command::ListFinish { .. }
                    | Command::LoadFinish { .. }
                    | Command::ResetSource { .. }
                    | Command::ResumeBackfill { .. }
                    | Command::InjectSourceOffsets { .. } => {
                        if cfg!(debug_assertions) {
                            panic!(
                                "new database graph info can only be created for normal creating streaming job, but get command: {} {:?}",
                                database_id, command
                            )
                        } else {
                            warn!(%database_id, ?command, "database does not exist while handling the command");
                            notifier.notify_start_failed(anyhow!("database {database_id} does not exist while handling command {command:?}").into());
                            return Ok(());
                        }
                    }
                },
            };

            database.handle_new_barrier(
                Some((command, notifier)),
                checkpoint,
                span,
                partial_graph_manager,
                &self.hummock_version_stats,
                worker_nodes,
            )
        } else {
            let database = match self.databases.entry(database_id) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(_) => {
                    // If it does not exist in the HashMap yet, it means that the first streaming
                    // job has not been created, and we do not need to send a barrier.
                    return Ok(());
                }
            };
            let Some(database) = database.running_state_mut() else {
                // Skip new barrier for database which is not running.
                return Ok(());
            };
            if partial_graph_manager.pending_barrier_num(database.partial_graph_id)
                >= self.in_flight_barrier_nums
            {
                // Skip new barrier with no explicit command when the database should pause inject additional barrier
                return Ok(());
            }
            database.handle_new_barrier(
                None,
                checkpoint,
                span,
                partial_graph_manager,
                &self.hummock_version_stats,
                worker_nodes,
            )
        }
    }

    pub(crate) fn gen_backfill_progress(&self) -> HashMap<JobId, BackfillProgress> {
        let mut progress = HashMap::new();
        for status in self.databases.values() {
            let Some(database_checkpoint_control) = status.running_state() else {
                continue;
            };
            // Progress of normal backfill
            progress.extend(
                database_checkpoint_control
                    .database_info
                    .gen_backfill_progress(),
            );
            // Progress of independent checkpoint jobs
            for (job_id, job) in &database_checkpoint_control.independent_checkpoint_job_controls {
                if let Some(p) = job.gen_backfill_progress() {
                    progress.insert(*job_id, p);
                }
            }
        }
        progress
    }

    pub(crate) fn gen_fragment_backfill_progress(&self) -> Vec<FragmentBackfillProgress> {
        let mut progress = Vec::new();
        for status in self.databases.values() {
            let Some(database_checkpoint_control) = status.running_state() else {
                continue;
            };
            progress.extend(
                database_checkpoint_control
                    .database_info
                    .gen_fragment_backfill_progress(),
            );
            for job in database_checkpoint_control
                .independent_checkpoint_job_controls
                .values()
            {
                progress.extend(job.gen_fragment_backfill_progress());
            }
        }
        progress
    }

    pub(crate) fn gen_cdc_progress(&self) -> HashMap<JobId, CdcProgress> {
        let mut progress = HashMap::new();
        for status in self.databases.values() {
            let Some(database_checkpoint_control) = status.running_state() else {
                continue;
            };
            // Progress of normal backfill
            progress.extend(database_checkpoint_control.database_info.gen_cdc_progress());
        }
        progress
    }

    pub(crate) fn databases_failed_at_worker_err(
        &mut self,
        worker_id: WorkerId,
    ) -> impl Iterator<Item = DatabaseId> + '_ {
        self.databases
            .iter_mut()
            .filter_map(
                move |(database_id, database_status)| match database_status {
                    DatabaseCheckpointControlStatus::Running(control) => {
                        if !control.is_valid_after_worker_err(worker_id) {
                            Some(*database_id)
                        } else {
                            None
                        }
                    }
                    DatabaseCheckpointControlStatus::Recovering(state) => {
                        if !state.is_valid_after_worker_err(worker_id) {
                            Some(*database_id)
                        } else {
                            None
                        }
                    }
                },
            )
    }

    // ── Batch refresh trigger helpers (delegating to DatabaseCheckpointControl) ──

    pub(crate) fn get_batch_refresh_trigger_info(
        &self,
        database_id: DatabaseId,
        job_id: JobId,
    ) -> u64 {
        let database = self
            .databases
            .get(&database_id)
            .and_then(|s| s.running_state())
            .expect("database should be running for batch refresh trigger");
        database.get_batch_refresh_trigger_info(job_id)
    }

    pub(crate) fn start_batch_refresh_run(
        &mut self,
        database_id: DatabaseId,
        job_id: JobId,
        context: &BatchRefreshJobTriggerContext,
        worker_nodes: &HashMap<WorkerId, WorkerNode>,
        actor_id_counter: &AtomicU32,
        partial_graph_manager: &mut PartialGraphManager,
    ) -> MetaResult<bool> {
        let database = self
            .databases
            .get_mut(&database_id)
            .and_then(|s| s.running_state_mut())
            .expect("database should be running");
        database.start_batch_refresh_run(
            job_id,
            context,
            worker_nodes,
            actor_id_counter,
            partial_graph_manager,
        )
    }

    pub(crate) fn apply_batch_refresh_fragment_infos(
        &mut self,
        database_id: DatabaseId,
        job_id: JobId,
    ) {
        let database = self
            .databases
            .get_mut(&database_id)
            .and_then(|s| s.running_state_mut())
            .expect("database should be running");
        let br_job = match database
            .independent_checkpoint_job_controls
            .get(&job_id)
            .expect("job should exist")
        {
            IndependentCheckpointJobControl::BatchRefresh(job) => job,
            _ => panic!("expected batch refresh job"),
        };
        if let Some(fragment_infos) = br_job.fragment_infos() {
            database
                .database_info
                .shared_actor_infos
                .upsert(database_id, fragment_infos.values().map(|f| (f, job_id)));
        }
    }
}

pub(crate) enum CheckpointControlEvent<'a> {
    EnteringInitializing(DatabaseStatusAction<'a, EnterInitializing>),
    EnteringRunning(DatabaseStatusAction<'a, EnterRunning>),
    /// A batch refresh job is idle and its upstream has advanced past the refresh interval.
    /// Carries owned values so the async handler can call into context without borrowing self.
    BatchRefreshTrigger {
        database_id: DatabaseId,
        job_id: JobId,
    },
}

impl CheckpointControl {
    pub(crate) fn on_partial_graph_reset(
        &mut self,
        partial_graph_id: PartialGraphId,
        reset_resps: HashMap<WorkerId, ResetPartialGraphResponse>,
    ) {
        let (database_id, independent_job_id) = from_partial_graph_id(partial_graph_id);
        match self.databases.get_mut(&database_id).expect("should exist") {
            DatabaseCheckpointControlStatus::Running(database) => {
                if let Some(independent_job_id) = independent_job_id {
                    match database
                        .independent_checkpoint_job_controls
                        .remove(&independent_job_id)
                    {
                        Some(independent_job) => {
                            independent_job.on_partial_graph_reset();
                        }
                        None => {
                            if cfg!(debug_assertions) {
                                panic!(
                                    "receive reset partial graph resp on non-existing independent job {independent_job_id} in database {database_id}"
                                )
                            }
                            warn!(
                                %database_id,
                                %independent_job_id,
                                "ignore reset partial graph resp on non-existing independent job on running database"
                            );
                        }
                    }
                } else {
                    unreachable!("should not receive reset database resp when database running")
                }
            }
            DatabaseCheckpointControlStatus::Recovering(state) => {
                state.on_partial_graph_reset(partial_graph_id, reset_resps);
            }
        }
    }

    pub(crate) fn on_partial_graph_initialized(
        &mut self,
        partial_graph_id: PartialGraphId,
        partial_graph_manager: &mut PartialGraphManager,
    ) -> MetaResult<()> {
        let (database_id, independent_job_id) = from_partial_graph_id(partial_graph_id);
        match self.databases.get_mut(&database_id).expect("should exist") {
            DatabaseCheckpointControlStatus::Running(database) => {
                let Some(independent_job_id) = independent_job_id else {
                    unreachable!("database partial graph should not initialize when running")
                };
                let job = database
                    .independent_checkpoint_job_controls
                    .get_mut(&independent_job_id)
                    .expect("independent job should exist");
                match job {
                    IndependentCheckpointJobControl::BatchRefresh(job) => {
                        job.on_log_store_initialized(partial_graph_manager)
                    }
                    IndependentCheckpointJobControl::CreatingStreamingJob(_) => {
                        unreachable!("creating streaming job should not initialize when running")
                    }
                }
            }
            DatabaseCheckpointControlStatus::Recovering(state) => {
                state.partial_graph_initialized(partial_graph_id);
                Ok(())
            }
        }
    }

    pub(crate) fn next_event(
        &mut self,
    ) -> impl Future<Output = CheckpointControlEvent<'_>> + Send + '_ {
        let mut this = Some(self);
        poll_fn(move |cx| {
            let Some(this_mut) = this.as_mut() else {
                unreachable!("should not be polled after poll ready")
            };
            for (&database_id, database_status) in &mut this_mut.databases {
                match database_status {
                    DatabaseCheckpointControlStatus::Running(database) => {
                        // Check if any idle batch refresh job should start a refresh run.
                        if let Some(committed_epoch) = database.committed_epoch {
                            for (job_id, job) in &database.independent_checkpoint_job_controls {
                                if let IndependentCheckpointJobControl::BatchRefresh(br_job) = job
                                    && br_job.should_start_refresh(committed_epoch)
                                {
                                    let job_id = *job_id;
                                    let _ = this.take().expect("checked Some");
                                    return Poll::Ready(
                                        CheckpointControlEvent::BatchRefreshTrigger {
                                            database_id,
                                            job_id,
                                        },
                                    );
                                }
                            }
                        }
                    }
                    DatabaseCheckpointControlStatus::Recovering(state) => {
                        let poll_result = state.poll_next_event(cx);
                        if let Poll::Ready(action) = poll_result {
                            let this = this.take().expect("checked Some");
                            return Poll::Ready(match action {
                                RecoveringStateAction::EnterInitializing(reset_workers) => {
                                    CheckpointControlEvent::EnteringInitializing(
                                        this.new_database_status_action(
                                            database_id,
                                            EnterInitializing(reset_workers),
                                        ),
                                    )
                                }
                                RecoveringStateAction::EnterRunning => {
                                    CheckpointControlEvent::EnteringRunning(
                                        this.new_database_status_action(database_id, EnterRunning),
                                    )
                                }
                            });
                        }
                    }
                }
            }
            Poll::Pending
        })
    }
}

pub(crate) enum DatabaseCheckpointControlStatus {
    Running(DatabaseCheckpointControl),
    Recovering(DatabaseRecoveringState),
}

impl DatabaseCheckpointControlStatus {
    fn running_state(&self) -> Option<&DatabaseCheckpointControl> {
        match self {
            DatabaseCheckpointControlStatus::Running(state) => Some(state),
            DatabaseCheckpointControlStatus::Recovering(_) => None,
        }
    }

    fn running_state_mut(&mut self) -> Option<&mut DatabaseCheckpointControl> {
        match self {
            DatabaseCheckpointControlStatus::Running(state) => Some(state),
            DatabaseCheckpointControlStatus::Recovering(_) => None,
        }
    }

    fn expect_running(&mut self, reason: &'static str) -> &mut DatabaseCheckpointControl {
        match self {
            DatabaseCheckpointControlStatus::Running(state) => state,
            DatabaseCheckpointControlStatus::Recovering(_) => {
                panic!("should be at running: {}", reason)
            }
        }
    }
}

pub(in crate::barrier) struct DatabaseCheckpointControlMetrics {
    barrier_latency: LabelGuardedHistogram,
    in_flight_barrier_nums: LabelGuardedIntGauge,
    all_barrier_nums: LabelGuardedIntGauge,
}

impl DatabaseCheckpointControlMetrics {
    pub(in crate::barrier) fn new(database_id: DatabaseId) -> Self {
        let database_id_str = database_id.to_string();
        let barrier_latency = GLOBAL_META_METRICS
            .barrier_latency
            .with_guarded_label_values(&[&database_id_str]);
        let in_flight_barrier_nums = GLOBAL_META_METRICS
            .in_flight_barrier_nums
            .with_guarded_label_values(&[&database_id_str]);
        let all_barrier_nums = GLOBAL_META_METRICS
            .all_barrier_nums
            .with_guarded_label_values(&[&database_id_str]);
        Self {
            barrier_latency,
            in_flight_barrier_nums,
            all_barrier_nums,
        }
    }
}

impl PartialGraphStat for DatabaseCheckpointControlMetrics {
    fn observe_barrier_latency(&self, _epoch: EpochPair, barrier_latency_secs: f64) {
        self.barrier_latency.observe(barrier_latency_secs);
    }

    fn observe_barrier_num(&self, inflight_barrier_num: usize, collected_barrier_num: usize) {
        self.in_flight_barrier_nums.set(inflight_barrier_num as _);
        self.all_barrier_nums
            .set((inflight_barrier_num + collected_barrier_num) as _);
    }
}

/// Controls the concurrent execution of commands.
pub(in crate::barrier) struct DatabaseCheckpointControl {
    pub(super) database_id: DatabaseId,
    /// Identifies the current recovery incarnation of this database.
    pub(super) term_id: String,
    partial_graph_id: PartialGraphId,
    pub(super) state: BarrierWorkerState,

    finishing_jobs_collector:
        BarrierItemCollector<JobId, (Vec<BarrierCompleteResponse>, TrackingJob), ()>,
    /// The barrier that are completing.
    completing_barrier: Option<EpochPair>,

    committed_epoch: Option<u64>,

    /// `None` while the database has no streaming job, so that a frozen timestamp does not
    /// render as an ever-growing barrier pending time.
    last_committed_barrier_time: Option<LabelGuardedIntGauge>,

    pub(super) database_info: InflightDatabaseInfo,
    pub independent_checkpoint_job_controls: HashMap<JobId, IndependentCheckpointJobControl>,
}

impl DatabaseCheckpointControl {
    fn new(database_id: DatabaseId, shared_actor_infos: SharedActorInfos) -> Self {
        Self {
            database_id,
            term_id: Uuid::new_v4().to_string(),
            partial_graph_id: to_partial_graph_id(database_id, None),
            state: BarrierWorkerState::new(),
            finishing_jobs_collector: BarrierItemCollector::new(false),
            completing_barrier: None,
            committed_epoch: None,
            last_committed_barrier_time: None,
            database_info: InflightDatabaseInfo::empty(database_id, shared_actor_infos),
            independent_checkpoint_job_controls: Default::default(),
        }
    }

    pub(crate) fn recovery(
        database_id: DatabaseId,
        term_id: String,
        state: BarrierWorkerState,
        committed_epoch: u64,
        database_info: InflightDatabaseInfo,
        independent_checkpoint_job_controls: HashMap<JobId, IndependentCheckpointJobControl>,
    ) -> Self {
        Self {
            database_id,
            term_id,
            partial_graph_id: to_partial_graph_id(database_id, None),
            state,
            finishing_jobs_collector: BarrierItemCollector::new(false),
            completing_barrier: None,
            committed_epoch: Some(committed_epoch),
            last_committed_barrier_time: None,
            database_info,
            independent_checkpoint_job_controls,
        }
    }

    pub(super) fn term_id(&self) -> &str {
        &self.term_id
    }

    pub(crate) fn is_valid_after_worker_err(&self, worker_id: WorkerId) -> bool {
        !self.database_info.contains_worker(worker_id as _)
            && self
                .independent_checkpoint_job_controls
                .values()
                .all(|job| {
                    job.fragment_infos()
                        .map(|fragment_infos| {
                            !InflightFragmentInfo::contains_worker(
                                fragment_infos.values(),
                                worker_id,
                            )
                        })
                        .unwrap_or(true)
                })
    }

    /// Enqueue a barrier command
    fn enqueue_command(&mut self, epoch: EpochPair, independent_jobs_to_wait: HashSet<JobId>) {
        let prev_epoch = epoch.prev;
        tracing::trace!(prev_epoch, ?independent_jobs_to_wait, "enqueue command");
        if !independent_jobs_to_wait.is_empty() {
            self.finishing_jobs_collector
                .enqueue(epoch, independent_jobs_to_wait, ());
        }
    }

    /// Change the state of this `prev_epoch` to `Completed`. Return continuous nodes
    /// with `Completed` starting from first node [`Completed`..`InFlight`) and remove them.
    fn barrier_collected(
        &mut self,
        partial_graph_id: PartialGraphId,
        collected_barrier: CollectedBarrier<'_>,
        periodic_barriers: &mut PeriodicBarriers,
    ) -> MetaResult<()> {
        let prev_epoch = collected_barrier.epoch.prev;
        tracing::trace!(
            prev_epoch,
            partial_graph_id = %partial_graph_id,
            "barrier collected"
        );
        let (database_id, independent_job_id) = from_partial_graph_id(partial_graph_id);
        assert_eq!(self.database_id, database_id);
        if let Some(independent_job_id) = independent_job_id {
            let job = self
                .independent_checkpoint_job_controls
                .get_mut(&independent_job_id)
                .expect("should exist");
            let should_force_checkpoint = job.collect(collected_barrier);
            if should_force_checkpoint {
                periodic_barriers.force_checkpoint_in_next_barrier(self.database_id);
            }
        }
        Ok(())
    }
}

impl DatabaseCheckpointControl {
    fn collect_backfill_pinned_upstream_tables(&self) -> HashSet<TableId> {
        self.independent_checkpoint_job_controls
            .values()
            .flat_map(|job| job.pinned_upstream_tables())
            .collect()
    }

    fn collect_no_shuffle_fragment_relations_for_reschedule_check(
        &self,
    ) -> Vec<(FragmentId, FragmentId)> {
        let mut no_shuffle_relations = Vec::new();
        for fragment in self.database_info.fragment_infos() {
            let downstream_fragment_id = fragment.fragment_id;
            visit_stream_node_cont(&fragment.nodes, |node| {
                if let Some(NodeBody::Merge(merge)) = node.node_body.as_ref()
                    && merge.upstream_dispatcher_type == PbDispatcherType::NoShuffle as i32
                {
                    no_shuffle_relations.push((merge.upstream_fragment_id, downstream_fragment_id));
                }
                true
            });
        }

        for job in self.independent_checkpoint_job_controls.values() {
            if let Some(fragment_infos) = job.fragment_infos() {
                for fragment_info in fragment_infos.values() {
                    let downstream_fragment_id = fragment_info.fragment_id;
                    visit_stream_node_cont(&fragment_info.nodes, |node| {
                        if let Some(NodeBody::Merge(merge)) = node.node_body.as_ref()
                            && merge.upstream_dispatcher_type == PbDispatcherType::NoShuffle as i32
                        {
                            no_shuffle_relations
                                .push((merge.upstream_fragment_id, downstream_fragment_id));
                        }
                        true
                    });
                }
            }
        }
        no_shuffle_relations
    }

    fn collect_reschedule_blocked_jobs_for_independent_jobs_inflight(
        &self,
    ) -> MetaResult<HashSet<JobId>> {
        let mut initial_blocked_fragment_ids = HashSet::new();
        for job in self.independent_checkpoint_job_controls.values() {
            if let Some(fragment_infos) = job.fragment_infos() {
                for fragment_info in fragment_infos.values() {
                    if fragment_has_online_unreschedulable_scan(fragment_info) {
                        initial_blocked_fragment_ids.insert(fragment_info.fragment_id);
                        collect_fragment_upstream_fragment_ids(
                            fragment_info,
                            &mut initial_blocked_fragment_ids,
                        );
                    }
                }
            }
        }

        let mut blocked_fragment_ids = initial_blocked_fragment_ids.clone();
        if !initial_blocked_fragment_ids.is_empty() {
            let no_shuffle_relations =
                self.collect_no_shuffle_fragment_relations_for_reschedule_check();
            let (forward_edges, backward_edges) =
                build_no_shuffle_fragment_graph_edges(no_shuffle_relations);
            let initial_blocked_fragment_ids: Vec<_> =
                initial_blocked_fragment_ids.iter().copied().collect();
            for ensemble in find_no_shuffle_graphs(
                &initial_blocked_fragment_ids,
                &forward_edges,
                &backward_edges,
            )? {
                blocked_fragment_ids.extend(ensemble.fragments());
            }
        }

        let mut blocked_job_ids = HashSet::new();
        blocked_job_ids.extend(
            blocked_fragment_ids
                .into_iter()
                .filter_map(|fragment_id| self.database_info.job_id_by_fragment(fragment_id)),
        );
        Ok(blocked_job_ids)
    }

    fn collect_reschedule_blocked_job_ids(
        &self,
        reschedules: &HashMap<FragmentId, Reschedule>,
        fragment_actors: &HashMap<FragmentId, HashSet<ActorId>>,
        blocked_job_ids: &HashSet<JobId>,
    ) -> HashSet<JobId> {
        let mut affected_fragment_ids: HashSet<FragmentId> = reschedules.keys().copied().collect();
        affected_fragment_ids.extend(fragment_actors.keys().copied());
        for reschedule in reschedules.values() {
            affected_fragment_ids.extend(reschedule.downstream_fragment_ids.iter().copied());
            affected_fragment_ids.extend(
                reschedule
                    .upstream_fragment_dispatcher_ids
                    .iter()
                    .map(|(fragment_id, _)| *fragment_id),
            );
        }

        affected_fragment_ids
            .into_iter()
            .filter_map(|fragment_id| self.database_info.job_id_by_fragment(fragment_id))
            .filter(|job_id| blocked_job_ids.contains(job_id))
            .collect()
    }

    fn next_complete_barrier_task(
        &mut self,
        periodic_barriers: &mut PeriodicBarriers,
        partial_graph_manager: &mut PartialGraphManager,
        task: &mut Option<CompleteBarrierTask>,
        hummock_version_stats: &HummockVersionStats,
    ) {
        // `Vec::new` is a const fn, and do not have memory allocation, and therefore is lightweight enough
        let mut independent_jobs_task = vec![];
        if let Some(committed_epoch) = self.committed_epoch {
            // `Vec::new` is a const fn, and do not have memory allocation, and therefore is lightweight enough
            let mut finished_jobs = Vec::new();
            let min_upstream_inflight_barrier = partial_graph_manager
                .first_inflight_barrier(self.partial_graph_id)
                .map(|epoch| epoch.prev);
            for (job_id, job) in &mut self.independent_checkpoint_job_controls {
                match job {
                    IndependentCheckpointJobControl::CreatingStreamingJob(creating_job) => {
                        if let Some((epoch, resps, info, is_finish_epoch)) = creating_job
                            .start_completing(
                                partial_graph_manager,
                                min_upstream_inflight_barrier,
                                committed_epoch,
                            )
                        {
                            let resps = resps.into_values().collect_vec();
                            if is_finish_epoch {
                                assert!(info.notifier.is_none());
                                finished_jobs.push((*job_id, epoch, resps));
                                continue;
                            };
                            independent_jobs_task.push((*job_id, epoch, resps, info));
                        }
                    }
                    IndependentCheckpointJobControl::BatchRefresh(batch_refresh_job) => {
                        if let Some((epoch, resps, info, tracking_job)) = batch_refresh_job
                            .start_completing(partial_graph_manager, committed_epoch)
                        {
                            let resps = resps.into_values().collect_vec();
                            if let Some(tracking_job) = tracking_job {
                                let task = task.get_or_insert_default();
                                task.finished_jobs.push(tracking_job);
                            }
                            independent_jobs_task.push((*job_id, epoch, resps, info));
                        }
                    }
                }
            }
            if !finished_jobs.is_empty() {
                partial_graph_manager.remove_partial_graphs(
                    finished_jobs
                        .iter()
                        .map(|(job_id, ..)| to_partial_graph_id(self.database_id, Some(*job_id)))
                        .collect(),
                );
            }
            for (job_id, epoch, resps) in finished_jobs {
                debug!(epoch, %job_id, "finish creating job");
                // It's safe to remove the creating job, because on CompleteJobType::Finished,
                // all previous barriers have been collected and completed.
                let Some(IndependentCheckpointJobControl::CreatingStreamingJob(
                    creating_streaming_job,
                )) = self.independent_checkpoint_job_controls.remove(&job_id)
                else {
                    panic!("finished job {job_id} should be a creating streaming job");
                };
                let tracking_job = creating_streaming_job.into_tracking_job();
                self.finishing_jobs_collector
                    .collect(epoch, job_id, (resps, tracking_job));
            }
        }
        let mut observed_non_checkpoint = false;
        self.finishing_jobs_collector.advance_collected();
        let epoch_end_bound = self
            .finishing_jobs_collector
            .first_inflight_epoch()
            .map_or(Unbounded, |epoch| Excluded(epoch.prev));
        if let Some((epoch, resps, info)) = partial_graph_manager.start_completing(
            self.partial_graph_id,
            epoch_end_bound,
            |_, resps, post_collect_command| {
                observed_non_checkpoint = true;
                self.handle_refresh_table_info(task, &resps);
                self.database_info.apply_collected_command(
                    &post_collect_command,
                    &resps,
                    hummock_version_stats,
                );
            },
        ) {
            self.handle_refresh_table_info(task, &resps);
            self.database_info.apply_collected_command(
                &info.post_collect_command,
                &resps,
                hummock_version_stats,
            );
            let mut resps_to_commit = resps.into_values().collect_vec();
            let mut staging_commit_info = self.database_info.take_staging_commit_info();
            if let Some((_, finished_jobs, _)) =
                self.finishing_jobs_collector
                    .take_collected_if(|collected_epoch| {
                        assert!(epoch <= collected_epoch.prev);
                        epoch == collected_epoch.prev
                    })
            {
                finished_jobs
                    .into_iter()
                    .for_each(|(_, (resps, tracking_job))| {
                        resps_to_commit.extend(resps);
                        staging_commit_info.finished_jobs.push(tracking_job);
                    });
            }
            {
                let task = task.get_or_insert_default();
                Command::collect_commit_epoch_info(
                    &self.database_info,
                    &info,
                    task,
                    resps_to_commit,
                    self.collect_backfill_pinned_upstream_tables(),
                );
                self.completing_barrier = Some(info.barrier_info.epoch());
                task.finished_jobs.extend(staging_commit_info.finished_jobs);
                task.finished_cdc_table_backfill
                    .extend(staging_commit_info.finished_cdc_table_backfill);
                task.epoch_infos
                    .try_insert(self.partial_graph_id, info)
                    .expect("non duplicate");
                task.commit_info
                    .truncate_tables
                    .extend(staging_commit_info.table_ids_to_truncate);
            }
        } else if observed_non_checkpoint
            && self.database_info.has_pending_finished_jobs()
            && !partial_graph_manager.has_pending_checkpoint_barrier(self.partial_graph_id)
        {
            periodic_barriers.force_checkpoint_in_next_barrier(self.database_id);
        }
        if !independent_jobs_task.is_empty() {
            let task = task.get_or_insert_default();
            for (job_id, epoch, resps, info) in independent_jobs_task {
                collect_independent_job_commit_epoch_info(task, epoch, resps, &info);
                task.epoch_infos
                    .try_insert(to_partial_graph_id(self.database_id, Some(job_id)), info)
                    .expect("non duplicate");
            }
        }
    }

    fn ack_completed(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        command_prev_epoch: Option<u64>,
        independent_job_epochs: Vec<(JobId, u64)>,
    ) {
        {
            if let Some(epoch) = self.completing_barrier.take() {
                assert_eq!(command_prev_epoch, Some(epoch.prev));
                self.committed_epoch = Some(epoch.prev);
                partial_graph_manager.ack_completed(self.partial_graph_id, epoch.prev);
                self.last_committed_barrier_time
                    .get_or_insert_with(|| {
                        GLOBAL_META_METRICS
                            .last_committed_barrier_time
                            .with_guarded_label_values(&[&self.database_id.to_string()])
                    })
                    .set(Epoch(epoch.curr).as_unix_secs() as i64);
            } else {
                assert_eq!(command_prev_epoch, None);
            };
            for (job_id, epoch) in independent_job_epochs {
                if let Some(job) = self.independent_checkpoint_job_controls.get_mut(&job_id) {
                    job.ack_completed(partial_graph_manager, epoch);
                }
                // If the job is not found, it was dropped and already removed
                // by `on_partial_graph_reset` while the completing task was running.
            }
        }
    }

    fn handle_refresh_table_info(
        &self,
        task: &mut Option<CompleteBarrierTask>,
        resps: &HashMap<WorkerId, BarrierCompleteResponse>,
    ) {
        let list_finished_info = resps
            .values()
            .flat_map(|resp| resp.list_finished_sources.clone())
            .collect::<Vec<_>>();
        if !list_finished_info.is_empty() {
            let task = task.get_or_insert_default();
            task.list_finished_source_ids.extend(list_finished_info);
        }

        let load_finished_info = resps
            .values()
            .flat_map(|resp| resp.load_finished_sources.clone())
            .collect::<Vec<_>>();
        if !load_finished_info.is_empty() {
            let task = task.get_or_insert_default();
            task.load_finished_source_ids.extend(load_finished_info);
        }

        let refresh_finished_table_ids: Vec<JobId> = resps
            .values()
            .flat_map(|resp| {
                resp.refresh_finished_tables
                    .iter()
                    .map(|table_id| table_id.as_job_id())
            })
            .collect::<Vec<_>>();
        if !refresh_finished_table_ids.is_empty() {
            let task = task.get_or_insert_default();
            task.refresh_finished_table_job_ids
                .extend(refresh_finished_table_ids);
        }
    }
}

impl DatabaseCheckpointControl {
    /// Handle the new barrier from the scheduled queue and inject it.
    fn handle_new_barrier(
        &mut self,
        command: Option<(Command, Notifier)>,
        checkpoint: bool,
        span: tracing::Span,
        partial_graph_manager: &mut PartialGraphManager,
        hummock_version_stats: &HummockVersionStats,
        worker_nodes: &HashMap<WorkerId, WorkerNode>,
    ) -> MetaResult<()> {
        let curr_epoch = self.state.in_flight_prev_epoch().next();

        let (mut command, notifier) = if let Some((command, notifier)) = command {
            (Some(command), Some(notifier))
        } else {
            (None, None)
        };

        debug_assert!(
            !matches!(
                &command,
                Some(Command::RescheduleIntent {
                    reschedule_plan: None,
                    ..
                })
            ),
            "reschedule intent should be resolved before injection"
        );

        let mut notifier_start = notifier.map(Notifier::start);
        if let Some(Command::DropStreamingJobs {
            streaming_job_ids, ..
        }) = &mut command
        {
            streaming_job_ids.retain(|job_id| {
                let Some(job) = self.independent_checkpoint_job_controls.get_mut(job_id) else {
                    return true;
                };
                !job.drop(notifier_start.as_mut(), partial_graph_manager)
            });
            if streaming_job_ids.is_empty() {
                if let Some(notifier) = notifier_start {
                    notifier.started();
                }
                return Ok(());
            }
        }

        if let Some(Command::RescheduleIntent {
            reschedule_plan: Some(reschedule_plan),
            ..
        }) = &command
            && !self.independent_checkpoint_job_controls.is_empty()
        {
            let blocked_job_ids =
                self.collect_reschedule_blocked_jobs_for_independent_jobs_inflight()?;
            let blocked_reschedule_job_ids = self.collect_reschedule_blocked_job_ids(
                &reschedule_plan.reschedules,
                &reschedule_plan.fragment_actors,
                &blocked_job_ids,
            );
            if !blocked_reschedule_job_ids.is_empty() {
                warn!(
                    blocked_reschedule_job_ids = ?blocked_reschedule_job_ids,
                    "reject reschedule fragments related to creating unreschedulable backfill jobs"
                );
                if let Some(notifier) = notifier_start {
                    notifier.notify_start_failed(
                        anyhow!(
                            "cannot reschedule jobs {:?} when creating jobs with unreschedulable backfill fragments",
                            blocked_reschedule_job_ids
                        )
                            .into(),
                    );
                }
                return Ok(());
            }
        }

        if !matches!(&command, Some(Command::CreateStreamingJob { .. }))
            && self.database_info.is_empty()
        {
            assert!(
                self.independent_checkpoint_job_controls.is_empty(),
                "should not have snapshot backfill job when there is no normal job in database"
            );
            // Drop the guard to remove the metric series of this database.
            self.last_committed_barrier_time = None;
            // skip the command when there is nothing to do with the barrier
            if let Some(notifier) = notifier_start {
                notifier.started();
            }
            return Ok(());
        };

        if let Some(Command::CreateStreamingJob {
            job_type:
                CreateStreamingJobType::SnapshotBackfill { .. }
                | CreateStreamingJobType::BatchRefresh(_),
            ..
        }) = &command
            && self.state.is_paused()
        {
            warn!("cannot create streaming job with snapshot backfill when paused");
            if let Some(notifier) = notifier_start {
                notifier.notify_start_failed(
                    anyhow!("cannot create streaming job with snapshot backfill when paused",)
                        .into(),
                );
            }
            return Ok(());
        }

        let barrier_info = self.state.next_barrier_info(checkpoint, curr_epoch);
        // Tracing related stuff
        barrier_info.prev_epoch.span().in_scope(|| {
            tracing::info!(target: "rw_tracing", epoch = barrier_info.curr_epoch(), "new barrier enqueued");
        });
        span.record("epoch", barrier_info.curr_epoch());

        let epoch = barrier_info.epoch();
        let ApplyCommandInfo { jobs_to_wait } = match self.apply_command(
            command,
            &mut notifier_start,
            barrier_info,
            partial_graph_manager,
            hummock_version_stats,
            worker_nodes,
        ) {
            Ok(info) => {
                assert!(notifier_start.is_none());
                info
            }
            Err(err) => {
                if let Some(notifier) = notifier_start {
                    notifier.notify_start_failed(err.clone());
                }
                fail_point!("inject_barrier_err_success");
                return Err(err);
            }
        };

        // Record the in-flight barrier.
        self.enqueue_command(epoch, jobs_to_wait);

        Ok(())
    }

    // ── Batch refresh trigger helpers ────────────────────────────────────────

    /// Get the last committed epoch for a batch refresh job.
    pub(crate) fn get_batch_refresh_trigger_info(&self, job_id: JobId) -> u64 {
        let job = self
            .independent_checkpoint_job_controls
            .get(&job_id)
            .expect("batch refresh job should exist");
        match job {
            IndependentCheckpointJobControl::BatchRefresh(br_job) => br_job
                .last_committed_epoch()
                .expect("idle job must have a last_committed_epoch"),
            _ => panic!("job {} should be a batch refresh job", job_id),
        }
    }

    /// Whether the batch refresh job already has its cached context populated.
    /// Start a batch refresh logstore consumption run.
    /// Returns true if a run was started, false if no log epochs to consume.
    pub(crate) fn start_batch_refresh_run(
        &mut self,
        job_id: JobId,
        context: &BatchRefreshJobTriggerContext,
        worker_nodes: &HashMap<WorkerId, WorkerNode>,
        actor_id_counter: &AtomicU32,
        partial_graph_manager: &mut PartialGraphManager,
    ) -> MetaResult<bool> {
        let term_id = self.term_id.as_str();
        let job = self
            .independent_checkpoint_job_controls
            .get_mut(&job_id)
            .expect("batch refresh job should exist");
        match job {
            IndependentCheckpointJobControl::BatchRefresh(br_job) => br_job.start_refresh_run(
                context,
                worker_nodes,
                actor_id_counter,
                term_id,
                partial_graph_manager,
            ),
            _ => panic!("job {} should be a batch refresh job", job_id),
        }
    }
}
