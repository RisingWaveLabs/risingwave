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

use std::collections::HashMap;
use std::mem::take;

use risingwave_common::catalog::{FragmentTypeFlag, TableId};
use risingwave_common::id::JobId;
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_plan::StreamNode;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;

use crate::MetaResult;
use crate::barrier::backfill_order_control::BackfillOrderState;
use crate::barrier::info::InflightStreamingJobInfo;
use crate::barrier::{CreateStreamingJobCommandInfo, FragmentBackfillProgress};
use crate::controller::fragment::InflightFragmentInfo;
use crate::manager::MetadataManager;
use crate::model::{ActorId, BackfillUpstreamType, FragmentId, StreamJobFragments};
use crate::stream::{SourceChange, SourceManagerRef};

type ConsumedRows = u64;
type BufferedRows = u64;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ActorBackfillProgress {
    pub(crate) actor_id: ActorId,
    pub(crate) upstream_type: BackfillUpstreamType,
    pub(crate) consumed_rows: u64,
    pub(crate) done: bool,
}

#[derive(Clone, Copy, Debug)]
enum BackfillState {
    Init,
    ConsumingUpstream(#[expect(dead_code)] Epoch, ConsumedRows, BufferedRows),
    Done(ConsumedRows, BufferedRows),
}

/// Represents the backfill nodes that need to be scheduled or cleaned up.
#[derive(Debug, Default)]
pub(super) struct PendingBackfillFragments {
    /// Fragment IDs that should start backfilling in the next checkpoint
    pub next_backfill_nodes: Vec<FragmentId>,
    /// State tables of locality provider fragments that should be truncated
    pub truncate_locality_provider_state_tables: Vec<TableId>,
}

/// Progress of all actors containing backfill executors while creating mview.
#[derive(Debug)]
pub(super) struct Progress {
    job_id: JobId,
    // `states` and `done_count` decides whether the progress is done. See `is_done`.
    states: HashMap<ActorId, BackfillState>,
    backfill_order_state: BackfillOrderState,
    done_count: usize,

    /// Tells whether the backfill is from source or mv.
    backfill_upstream_types: HashMap<ActorId, BackfillUpstreamType>,

    // The following row counts are used to calculate the progress. See `calculate_progress`.
    /// Upstream mv count.
    /// Keep track of how many times each upstream MV
    /// appears in this stream job.
    upstream_mv_count: HashMap<TableId, usize>,
    /// Total key count of all the upstream materialized views
    upstream_mvs_total_key_count: u64,
    mv_backfill_consumed_rows: u64,
    source_backfill_consumed_rows: u64,
    /// Buffered rows (for locality backfill) that are yet to be consumed
    /// This is used to calculate precise progress: consumed / (`upstream_total` + buffered)
    mv_backfill_buffered_rows: u64,
}

impl Progress {
    /// Create a [`Progress`] for some creating mview, with all `actors` containing the backfill executors.
    fn new(
        job_id: JobId,
        actors: impl IntoIterator<Item = (ActorId, BackfillUpstreamType)>,
        upstream_mv_count: HashMap<TableId, usize>,
        upstream_total_key_count: u64,
        backfill_order_state: BackfillOrderState,
    ) -> Self {
        let mut states = HashMap::new();
        let mut backfill_upstream_types = HashMap::new();
        for (actor, backfill_upstream_type) in actors {
            states.insert(actor, BackfillState::Init);
            backfill_upstream_types.insert(actor, backfill_upstream_type);
        }
        assert!(!states.is_empty());

        Self {
            job_id,
            states,
            backfill_upstream_types,
            done_count: 0,
            upstream_mv_count,
            upstream_mvs_total_key_count: upstream_total_key_count,
            mv_backfill_consumed_rows: 0,
            source_backfill_consumed_rows: 0,
            mv_backfill_buffered_rows: 0,
            backfill_order_state,
        }
    }

    /// Update the progress of `actor`.
    /// Returns the backfill fragments that need to be scheduled or cleaned up.
    fn update(
        &mut self,
        actor: ActorId,
        new_state: BackfillState,
        upstream_total_key_count: u64,
    ) -> PendingBackfillFragments {
        let mut result = PendingBackfillFragments::default();
        self.upstream_mvs_total_key_count = upstream_total_key_count;
        let total_actors = self.states.len();
        let Some(backfill_upstream_type) = self.backfill_upstream_types.get(&actor) else {
            tracing::warn!(%actor, "receive progress from unknown actor, likely removed after reschedule");
            return result;
        };

        let mut old_consumed_row = 0;
        let mut new_consumed_row = 0;
        let mut old_buffered_row = 0;
        let mut new_buffered_row = 0;
        let Some(prev_state) = self.states.remove(&actor) else {
            tracing::warn!(%actor, "receive progress for actor not in state map");
            return result;
        };
        match prev_state {
            BackfillState::Init => {}
            BackfillState::ConsumingUpstream(_, consumed_rows, buffered_rows) => {
                old_consumed_row = consumed_rows;
                old_buffered_row = buffered_rows;
            }
            BackfillState::Done(_, _) => panic!("should not report done multiple times"),
        };
        match &new_state {
            BackfillState::Init => {}
            BackfillState::ConsumingUpstream(_, consumed_rows, buffered_rows) => {
                new_consumed_row = *consumed_rows;
                new_buffered_row = *buffered_rows;
            }
            BackfillState::Done(consumed_rows, buffered_rows) => {
                tracing::debug!("actor {} done", actor);
                new_consumed_row = *consumed_rows;
                new_buffered_row = *buffered_rows;
                self.done_count += 1;
                let before_backfill_nodes = self
                    .backfill_order_state
                    .current_backfill_node_fragment_ids();
                result.next_backfill_nodes = self.backfill_order_state.finish_actor(actor);
                let after_backfill_nodes = self
                    .backfill_order_state
                    .current_backfill_node_fragment_ids();
                // last_backfill_nodes = before_backfill_nodes - after_backfill_nodes
                let last_backfill_nodes_iter = before_backfill_nodes
                    .into_iter()
                    .filter(|x| !after_backfill_nodes.contains(x));
                result.truncate_locality_provider_state_tables = last_backfill_nodes_iter
                    .filter_map(|fragment_id| {
                        self.backfill_order_state
                            .get_locality_fragment_state_table_mapping()
                            .get(&fragment_id)
                    })
                    .flatten()
                    .copied()
                    .collect();
                tracing::debug!(
                    "{} actors out of {} complete",
                    self.done_count,
                    total_actors,
                );
            }
        };
        debug_assert!(
            new_consumed_row >= old_consumed_row,
            "backfill progress should not go backward"
        );
        debug_assert!(
            new_buffered_row >= old_buffered_row,
            "backfill progress should not go backward"
        );
        match backfill_upstream_type {
            BackfillUpstreamType::MView => {
                self.mv_backfill_consumed_rows += new_consumed_row - old_consumed_row;
            }
            BackfillUpstreamType::Source => {
                self.source_backfill_consumed_rows += new_consumed_row - old_consumed_row;
            }
            BackfillUpstreamType::Values => {
                // do not consider progress for values
            }
            BackfillUpstreamType::LocalityProvider => {
                // Track LocalityProvider progress similar to MView
                // Update buffered rows for precise progress calculation
                self.mv_backfill_consumed_rows += new_consumed_row - old_consumed_row;
                self.mv_backfill_buffered_rows += new_buffered_row - old_buffered_row;
            }
        }
        self.states.insert(actor, new_state);
        result
    }

    fn iter_actor_progress(&self) -> impl Iterator<Item = ActorBackfillProgress> + '_ {
        self.states.iter().filter_map(|(actor_id, state)| {
            let upstream_type = *self.backfill_upstream_types.get(actor_id)?;
            let (consumed_rows, done) = match *state {
                BackfillState::Init => (0, false),
                BackfillState::ConsumingUpstream(_, consumed_rows, _) => (consumed_rows, false),
                BackfillState::Done(consumed_rows, _) => (consumed_rows, true),
            };
            Some(ActorBackfillProgress {
                actor_id: *actor_id,
                upstream_type,
                consumed_rows,
                done,
            })
        })
    }

    /// Returns whether all backfill executors are done.
    fn is_done(&self) -> bool {
        tracing::trace!(
            "Progress::is_done? {}, {}, {:?}",
            self.done_count,
            self.states.len(),
            self.states
        );
        self.done_count == self.states.len()
    }

    /// `progress` = `consumed_rows` / `upstream_total_key_count`
    fn calculate_progress(&self) -> String {
        if self.is_done() || self.states.is_empty() {
            return "100%".to_owned();
        }
        let mut mv_count = 0;
        let mut source_count = 0;
        for backfill_upstream_type in self.backfill_upstream_types.values() {
            match backfill_upstream_type {
                BackfillUpstreamType::MView => mv_count += 1,
                BackfillUpstreamType::Source => source_count += 1,
                BackfillUpstreamType::Values => (),
                BackfillUpstreamType::LocalityProvider => mv_count += 1, /* Count LocalityProvider as an MView for progress */
            }
        }

        let mv_progress = (mv_count > 0).then_some({
            // Include buffered rows in total for precise progress calculation
            // Progress = consumed / (upstream_total + buffered)
            let total_rows_to_consume =
                self.upstream_mvs_total_key_count + self.mv_backfill_buffered_rows;
            if total_rows_to_consume == 0 {
                "99.99%".to_owned()
            } else {
                let mut progress =
                    self.mv_backfill_consumed_rows as f64 / (total_rows_to_consume as f64);
                if progress > 1.0 {
                    progress = 0.9999;
                }
                format!(
                    "{:.2}% ({}/{})",
                    progress * 100.0,
                    self.mv_backfill_consumed_rows,
                    total_rows_to_consume
                )
            }
        });
        let source_progress = (source_count > 0).then_some(format!(
            "{} rows consumed",
            self.source_backfill_consumed_rows
        ));
        match (mv_progress, source_progress) {
            (Some(mv_progress), Some(source_progress)) => {
                format!(
                    "MView Backfill: {}, Source Backfill: {}",
                    mv_progress, source_progress
                )
            }
            (Some(mv_progress), None) => mv_progress,
            (None, Some(source_progress)) => source_progress,
            (None, None) => "Unknown".to_owned(),
        }
    }
}

/// There are two kinds of `TrackingJobs`:
/// 1. if `is_recovered` is false, it is a "New" tracking job.
///    It is instantiated and managed by the stream manager.
///    On recovery, the stream manager will stop managing the job.
/// 2. if `is_recovered` is true, it is a "Recovered" tracking job.
///    On recovery, the barrier manager will recover and start managing the job.
pub struct TrackingJob {
    job_id: JobId,
    is_recovered: bool,
    source_change: Option<SourceChange>,
}

impl std::fmt::Display for TrackingJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}",
            self.job_id,
            if self.is_recovered { "<recovered>" } else { "" }
        )
    }
}

impl TrackingJob {
    /// Create a new tracking job.
    pub(crate) fn new(stream_job_fragments: &StreamJobFragments) -> Self {
        let finished_backfill_fragments = stream_job_fragments.source_backfill_fragments();
        // `None` when empty, consistent with the recovered-job constructor.
        let source_change = if finished_backfill_fragments.is_empty() {
            None
        } else {
            Some(SourceChange::CreateJobFinished {
                finished_backfill_fragments,
            })
        };
        Self {
            job_id: stream_job_fragments.stream_job_id,
            is_recovered: false,
            source_change,
        }
    }

    /// Create a recovered tracking job.
    pub(crate) fn recovered(
        job_id: JobId,
        fragment_infos: &HashMap<FragmentId, InflightFragmentInfo>,
    ) -> Self {
        Self::recovered_from_fragment_nodes(
            job_id,
            fragment_infos
                .iter()
                .map(|(fragment_id, fragment)| (*fragment_id, &fragment.nodes)),
        )
    }

    pub(crate) fn recovered_from_fragment_nodes<'a>(
        job_id: JobId,
        fragment_nodes: impl Iterator<Item = (FragmentId, &'a StreamNode)>,
    ) -> Self {
        let source_backfill_fragments =
            StreamJobFragments::source_backfill_fragments_impl(fragment_nodes);
        let source_change = if source_backfill_fragments.is_empty() {
            None
        } else {
            Some(SourceChange::CreateJobFinished {
                finished_backfill_fragments: source_backfill_fragments,
            })
        };
        Self {
            job_id,
            is_recovered: true,
            source_change,
        }
    }

    pub(crate) fn job_id(&self) -> JobId {
        self.job_id
    }

    /// Notify the metadata manager that the job is finished.
    pub(crate) async fn finish(
        self,
        metadata_manager: &MetadataManager,
        source_manager: &SourceManagerRef,
    ) -> MetaResult<()> {
        metadata_manager
            .catalog_controller
            .finish_streaming_job(self.job_id)
            .await?;
        if let Some(source_change) = self.source_change {
            source_manager.apply_source_change(source_change).await;
        }
        Ok(())
    }
}

impl std::fmt::Debug for TrackingJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.is_recovered {
            write!(f, "TrackingJob::New({})", self.job_id)
        } else {
            write!(f, "TrackingJob::Recovered({})", self.job_id)
        }
    }
}

/// Information collected during barrier completion that needs to be committed.
#[derive(Debug, Default)]
pub(super) struct StagingCommitInfo {
    /// Finished jobs that should be committed
    pub finished_jobs: Vec<TrackingJob>,
    /// Table IDs whose locality provider state tables need to be truncated
    pub table_ids_to_truncate: Vec<TableId>,
    pub finished_cdc_table_backfill: Vec<JobId>,
}

pub(super) enum UpdateProgressResult {
    None,
    /// The finished job, along with its pending backfill fragments for cleanup.
    Finished {
        truncate_locality_provider_state_tables: Vec<TableId>,
    },
    /// Backfill nodes have finished and new ones need to be scheduled.
    BackfillNodeFinished(PendingBackfillFragments),
}

#[derive(Debug)]
pub(super) struct CreateMviewProgressTracker {
    tracking_job: TrackingJob,
    status: CreateMviewStatus,
}

#[derive(Debug)]
enum CreateMviewStatus {
    Backfilling {
        /// Progress of the create-mview DDL.
        progress: Progress,

        /// Stash of pending backfill nodes. They will start backfilling on checkpoint.
        pending_backfill_nodes: Vec<FragmentId>,

        /// Table IDs whose locality provider state tables need to be truncated
        table_ids_to_truncate: Vec<TableId>,
    },
    CdcSourceInit,
    Finished {
        table_ids_to_truncate: Vec<TableId>,
    },
}

impl CreateMviewProgressTracker {
    pub fn recover(
        creating_job_id: JobId,
        fragment_infos: &HashMap<FragmentId, InflightFragmentInfo>,
        backfill_order_state: BackfillOrderState,
        version_stats: &HummockVersionStats,
    ) -> Self {
        let tracking_job = TrackingJob::recovered(creating_job_id, fragment_infos);
        let actors = InflightStreamingJobInfo::tracking_progress_actor_ids(fragment_infos);
        let status = if actors.is_empty() {
            CreateMviewStatus::Finished {
                table_ids_to_truncate: vec![],
            }
        } else {
            let mut states = HashMap::new();
            let mut backfill_upstream_types = HashMap::new();

            for (actor, backfill_upstream_type) in actors {
                states.insert(actor, BackfillState::ConsumingUpstream(Epoch(0), 0, 0));
                backfill_upstream_types.insert(actor, backfill_upstream_type);
            }

            let progress = Self::recover_progress(
                creating_job_id,
                states,
                backfill_upstream_types,
                StreamJobFragments::upstream_table_counts_impl(
                    fragment_infos.values().map(|fragment| &fragment.nodes),
                ),
                version_stats,
                backfill_order_state,
            );
            let pending_backfill_nodes = progress
                .backfill_order_state
                .current_backfill_node_fragment_ids();
            CreateMviewStatus::Backfilling {
                progress,
                pending_backfill_nodes,
                table_ids_to_truncate: vec![],
            }
        };
        Self {
            tracking_job,
            status,
        }
    }

    /// ## How recovery works
    ///
    /// The progress (number of rows consumed) is persisted in state tables.
    /// During recovery, the backfill executor will restore the number of rows consumed,
    /// and then it will just report progress like newly created executors.
    fn recover_progress(
        job_id: JobId,
        states: HashMap<ActorId, BackfillState>,
        backfill_upstream_types: HashMap<ActorId, BackfillUpstreamType>,
        upstream_mv_count: HashMap<TableId, usize>,
        version_stats: &HummockVersionStats,
        backfill_order_state: BackfillOrderState,
    ) -> Progress {
        let upstream_mvs_total_key_count =
            calculate_total_key_count(&upstream_mv_count, version_stats);
        Progress {
            job_id,
            states,
            backfill_order_state,
            backfill_upstream_types,
            done_count: 0, // Fill only after first barrier pass
            upstream_mv_count,
            upstream_mvs_total_key_count,
            mv_backfill_consumed_rows: 0, // Fill only after first barrier pass
            source_backfill_consumed_rows: 0, // Fill only after first barrier pass
            mv_backfill_buffered_rows: 0, // Fill only after first barrier pass
        }
    }

    pub fn gen_backfill_progress(&self) -> String {
        match &self.status {
            CreateMviewStatus::Backfilling { progress, .. } => progress.calculate_progress(),
            CreateMviewStatus::CdcSourceInit => "Initializing CDC source...".to_owned(),
            CreateMviewStatus::Finished { .. } => "100%".to_owned(),
        }
    }

    pub(crate) fn actor_progresses(&self) -> Vec<ActorBackfillProgress> {
        match &self.status {
            CreateMviewStatus::Backfilling { progress, .. } => {
                progress.iter_actor_progress().collect()
            }
            CreateMviewStatus::CdcSourceInit | CreateMviewStatus::Finished { .. } => vec![],
        }
    }

    /// Update the progress of tracked jobs, and add a new job to track if `info` is `Some`.
    /// Return the table ids whose locality provider state tables need to be truncated.
    pub(super) fn apply_progress(
        &mut self,
        create_mview_progress: &CreateMviewProgress,
        version_stats: &HummockVersionStats,
    ) {
        let CreateMviewStatus::Backfilling {
            progress,
            pending_backfill_nodes,
            table_ids_to_truncate,
        } = &mut self.status
        else {
            tracing::warn!(
                "update the progress of an backfill finished streaming job: {create_mview_progress:?}"
            );
            return;
        };
        {
            // Update the progress of all commands.
            {
                // Those with actors complete can be finished immediately.
                match progress.apply(create_mview_progress, version_stats) {
                    UpdateProgressResult::None => {
                        tracing::trace!(?progress, "update progress");
                    }
                    UpdateProgressResult::Finished {
                        truncate_locality_provider_state_tables,
                    } => {
                        let mut table_ids_to_truncate = take(table_ids_to_truncate);
                        table_ids_to_truncate.extend(truncate_locality_provider_state_tables);
                        tracing::trace!(?progress, "finish progress");
                        self.status = CreateMviewStatus::Finished {
                            table_ids_to_truncate,
                        };
                    }
                    UpdateProgressResult::BackfillNodeFinished(pending) => {
                        table_ids_to_truncate
                            .extend(pending.truncate_locality_provider_state_tables.clone());
                        tracing::trace!(
                            ?progress,
                            next_backfill_nodes = ?pending.next_backfill_nodes,
                            "start next backfill node"
                        );
                        pending_backfill_nodes.extend(pending.next_backfill_nodes);
                    }
                }
            }
        }
    }

    /// Refresh tracker state after reschedule so new actors can report progress correctly.
    pub fn refresh_after_reschedule(
        &mut self,
        fragment_infos: &HashMap<FragmentId, InflightFragmentInfo>,
        version_stats: &HummockVersionStats,
    ) {
        let CreateMviewStatus::Backfilling {
            progress,
            pending_backfill_nodes,
            ..
        } = &mut self.status
        else {
            return;
        };

        let new_tracking_actors = StreamJobFragments::tracking_progress_actor_ids_impl(
            fragment_infos
                .values()
                .map(|fragment| (fragment.fragment_type_mask, fragment.actors.keys().copied())),
        );

        #[cfg(debug_assertions)]
        {
            use std::collections::HashSet;
            let old_actor_ids: HashSet<_> = progress.states.keys().copied().collect();
            let new_actor_ids: HashSet<_> = new_tracking_actors
                .iter()
                .map(|(actor_id, _)| *actor_id)
                .collect();
            debug_assert!(
                old_actor_ids.is_disjoint(&new_actor_ids),
                "reschedule should rebuild backfill actors; old={old_actor_ids:?}, new={new_actor_ids:?}"
            );
        }

        let mut new_states = HashMap::new();
        let mut new_backfill_types = HashMap::new();
        for (actor_id, upstream_type) in new_tracking_actors {
            new_states.insert(actor_id, BackfillState::Init);
            new_backfill_types.insert(actor_id, upstream_type);
        }

        let fragment_actors: HashMap<_, _> = fragment_infos
            .iter()
            .map(|(fragment_id, info)| (*fragment_id, info.actors.keys().copied().collect()))
            .collect();

        let newly_scheduled = progress
            .backfill_order_state
            .refresh_actors(&fragment_actors);

        progress.backfill_upstream_types = new_backfill_types;
        progress.states = new_states;
        progress.done_count = 0;

        progress.upstream_mv_count = StreamJobFragments::upstream_table_counts_impl(
            fragment_infos.values().map(|fragment| &fragment.nodes),
        );
        progress.upstream_mvs_total_key_count =
            calculate_total_key_count(&progress.upstream_mv_count, version_stats);

        progress.mv_backfill_consumed_rows = 0;
        progress.source_backfill_consumed_rows = 0;
        progress.mv_backfill_buffered_rows = 0;

        let mut pending = progress
            .backfill_order_state
            .current_backfill_node_fragment_ids();
        pending.extend(newly_scheduled);
        pending.sort_unstable();
        pending.dedup();
        *pending_backfill_nodes = pending;
    }

    pub(super) fn take_pending_backfill_nodes(&mut self) -> impl Iterator<Item = FragmentId> + '_ {
        match &mut self.status {
            CreateMviewStatus::Backfilling {
                pending_backfill_nodes,
                ..
            } => Some(pending_backfill_nodes.drain(..)),
            CreateMviewStatus::CdcSourceInit => None,
            CreateMviewStatus::Finished { .. } => None,
        }
        .into_iter()
        .flatten()
    }

    pub(super) fn collect_staging_commit_info(
        &mut self,
    ) -> (bool, Box<dyn Iterator<Item = TableId> + '_>) {
        match &mut self.status {
            CreateMviewStatus::Backfilling {
                table_ids_to_truncate,
                ..
            } => (false, Box::new(table_ids_to_truncate.drain(..))),
            CreateMviewStatus::CdcSourceInit => (false, Box::new(std::iter::empty())),
            CreateMviewStatus::Finished {
                table_ids_to_truncate,
                ..
            } => (true, Box::new(table_ids_to_truncate.drain(..))),
        }
    }

    pub(super) fn is_finished(&self) -> bool {
        matches!(self.status, CreateMviewStatus::Finished { .. })
    }

    /// Mark CDC source as finished when offset is updated.
    pub(super) fn mark_cdc_source_finished(&mut self) {
        if matches!(self.status, CreateMviewStatus::CdcSourceInit) {
            self.status = CreateMviewStatus::Finished {
                table_ids_to_truncate: vec![],
            };
        }
    }

    pub(super) fn into_tracking_job(self) -> TrackingJob {
        let CreateMviewStatus::Finished { .. } = self.status else {
            panic!("should be called when finished");
        };
        self.tracking_job
    }

    pub(crate) fn job_id(&self) -> JobId {
        self.tracking_job.job_id
    }

    pub(crate) fn collect_fragment_progress(
        &self,
        fragment_infos: &HashMap<FragmentId, InflightFragmentInfo>,
        mark_done_when_empty: bool,
    ) -> Vec<FragmentBackfillProgress> {
        let actor_progresses = self.actor_progresses();
        if actor_progresses.is_empty() {
            if mark_done_when_empty && self.is_finished() {
                return collect_done_fragments(self.job_id(), fragment_infos);
            }
            return vec![];
        }
        collect_fragment_progress_from_actors(self.job_id(), fragment_infos, &actor_progresses)
    }

    /// Add a new create-mview DDL command to track.
    ///
    /// If the actors to track are empty, return the given command as it can be finished immediately.
    /// For CDC sources, mark as `CdcSourceInit` instead of Finished.
    pub fn new(
        info: &CreateStreamingJobCommandInfo,
        version_stats: &HummockVersionStats,
        fragment_infos: &HashMap<FragmentId, InflightFragmentInfo>,
    ) -> Self {
        tracing::trace!(?info, "add job to track");
        let CreateStreamingJobCommandInfo {
            stream_job_fragments,
            fragment_backfill_ordering,
            locality_fragment_state_table_mapping,
            streaming_job,
            ..
        } = info;
        let job_id = stream_job_fragments.stream_job_id();
        let actors = InflightStreamingJobInfo::tracking_progress_actor_ids(fragment_infos);
        let tracking_job = TrackingJob::new(&info.stream_job_fragments);
        if actors.is_empty() {
            // NOTE: This CDC source detection uses hardcoded property checks and should be replaced
            // with a more reliable identification method in the future.
            let is_cdc_source = matches!(
                streaming_job,
                crate::manager::StreamingJob::Source(source)
                    if source.info.as_ref().map(|info| info.is_shared()).unwrap_or(false) && source
                    .get_with_properties()
                    .get("connector")
                    .map(|connector| connector.to_lowercase().contains("-cdc"))
                    .unwrap_or(false)
            );
            if is_cdc_source {
                // Mark CDC source as CdcSourceInit, will be finished when offset is updated
                return Self {
                    tracking_job,
                    status: CreateMviewStatus::CdcSourceInit,
                };
            }
            // The command can be finished immediately.
            return Self {
                tracking_job,
                status: CreateMviewStatus::Finished {
                    table_ids_to_truncate: vec![],
                },
            };
        }

        let upstream_mv_count = stream_job_fragments.upstream_table_counts();
        let upstream_total_key_count: u64 =
            calculate_total_key_count(&upstream_mv_count, version_stats);

        let backfill_order_state = BackfillOrderState::new(
            fragment_backfill_ordering,
            fragment_infos,
            locality_fragment_state_table_mapping.clone(),
        );
        let progress = Progress::new(
            job_id,
            actors,
            upstream_mv_count,
            upstream_total_key_count,
            backfill_order_state,
        );
        let pending_backfill_nodes = progress
            .backfill_order_state
            .current_backfill_node_fragment_ids();
        Self {
            tracking_job,
            status: CreateMviewStatus::Backfilling {
                progress,
                pending_backfill_nodes,
                table_ids_to_truncate: vec![],
            },
        }
    }
}

impl Progress {
    /// Update the progress of `actor` according to the Pb struct.
    ///
    /// If all actors in this MV have finished, return the command.
    fn apply(
        &mut self,
        progress: &CreateMviewProgress,
        version_stats: &HummockVersionStats,
    ) -> UpdateProgressResult {
        tracing::trace!(?progress, "update progress");
        let actor = progress.backfill_actor_id;
        let job_id = self.job_id;

        let new_state = if progress.done {
            BackfillState::Done(progress.consumed_rows, progress.buffered_rows)
        } else {
            BackfillState::ConsumingUpstream(
                progress.consumed_epoch.into(),
                progress.consumed_rows,
                progress.buffered_rows,
            )
        };

        {
            {
                let progress_state = self;

                let upstream_total_key_count: u64 =
                    calculate_total_key_count(&progress_state.upstream_mv_count, version_stats);

                tracing::trace!(%job_id, "updating progress for table");
                let pending = progress_state.update(actor, new_state, upstream_total_key_count);

                if progress_state.is_done() {
                    tracing::debug!(
                        %job_id,
                        "all actors done for creating mview!",
                    );

                    let PendingBackfillFragments {
                        next_backfill_nodes,
                        truncate_locality_provider_state_tables,
                    } = pending;

                    assert!(next_backfill_nodes.is_empty());
                    UpdateProgressResult::Finished {
                        truncate_locality_provider_state_tables,
                    }
                } else if !pending.next_backfill_nodes.is_empty()
                    || !pending.truncate_locality_provider_state_tables.is_empty()
                {
                    UpdateProgressResult::BackfillNodeFinished(pending)
                } else {
                    UpdateProgressResult::None
                }
            }
        }
    }
}

fn calculate_total_key_count(
    table_count: &HashMap<TableId, usize>,
    version_stats: &HummockVersionStats,
) -> u64 {
    table_count
        .iter()
        .map(|(table_id, count)| {
            assert_ne!(*count, 0);
            *count as u64
                * version_stats
                    .table_stats
                    .get(table_id)
                    .map_or(0, |stat| stat.total_key_count as u64)
        })
        .sum()
}

pub(crate) fn collect_fragment_progress_from_actors(
    job_id: JobId,
    fragment_infos: &HashMap<FragmentId, InflightFragmentInfo>,
    actor_progresses: &[ActorBackfillProgress],
) -> Vec<FragmentBackfillProgress> {
    let mut actor_to_fragment = HashMap::new();
    for (fragment_id, info) in fragment_infos {
        for actor_id in info.actors.keys() {
            actor_to_fragment.insert(*actor_id, *fragment_id);
        }
    }

    let mut per_fragment: HashMap<FragmentId, (u64, usize, usize, BackfillUpstreamType)> =
        HashMap::new();
    for progress in actor_progresses {
        let Some(fragment_id) = actor_to_fragment.get(&progress.actor_id) else {
            continue;
        };
        let entry = per_fragment
            .entry(*fragment_id)
            .or_insert((0, 0, 0, progress.upstream_type));
        entry.0 = entry.0.saturating_add(progress.consumed_rows);
        entry.1 += progress.done as usize;
        entry.2 += 1;
    }

    per_fragment
        .into_iter()
        .map(
            |(fragment_id, (consumed_rows, done_cnt, total_cnt, upstream_type))| {
                FragmentBackfillProgress {
                    job_id,
                    fragment_id,
                    consumed_rows,
                    done: total_cnt > 0 && done_cnt == total_cnt,
                    upstream_type,
                }
            },
        )
        .collect()
}

pub(crate) fn collect_done_fragments(
    job_id: JobId,
    fragment_infos: &HashMap<FragmentId, InflightFragmentInfo>,
) -> Vec<FragmentBackfillProgress> {
    fragment_infos
        .iter()
        .filter(|(_, fragment)| {
            fragment.fragment_type_mask.contains_any([
                FragmentTypeFlag::StreamScan,
                FragmentTypeFlag::SourceScan,
                FragmentTypeFlag::LocalityProvider,
            ])
        })
        .map(|(fragment_id, fragment)| FragmentBackfillProgress {
            job_id,
            fragment_id: *fragment_id,
            consumed_rows: 0,
            done: true,
            upstream_type: BackfillUpstreamType::from_fragment_type_mask(
                fragment.fragment_type_mask,
            ),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use risingwave_common::catalog::{FragmentTypeFlag, FragmentTypeMask};
    use risingwave_common::id::WorkerId;
    use risingwave_meta_model::fragment::DistributionType;
    use risingwave_pb::stream_plan::StreamNode as PbStreamNode;

    use super::*;
    use crate::controller::fragment::InflightActorInfo;

    fn sample_inflight_fragment(
        fragment_id: FragmentId,
        actor_ids: &[ActorId],
        flag: FragmentTypeFlag,
    ) -> InflightFragmentInfo {
        let mut fragment_type_mask = FragmentTypeMask::empty();
        fragment_type_mask.add(flag);
        InflightFragmentInfo {
            fragment_id,
            distribution_type: DistributionType::Single,
            fragment_type_mask,
            vnode_count: 0,
            nodes: PbStreamNode::default(),
            actors: actor_ids
                .iter()
                .map(|actor_id| {
                    (
                        *actor_id,
                        InflightActorInfo {
                            worker_id: WorkerId::new(1),
                            vnode_bitmap: None,
                            splits: vec![],
                        },
                    )
                })
                .collect(),
            state_table_ids: HashSet::new(),
        }
    }

    fn sample_progress(actor_id: ActorId) -> Progress {
        Progress {
            job_id: JobId::new(1),
            states: HashMap::from([(actor_id, BackfillState::Init)]),
            backfill_order_state: BackfillOrderState::default(),
            done_count: 0,
            backfill_upstream_types: HashMap::from([(actor_id, BackfillUpstreamType::MView)]),
            upstream_mv_count: HashMap::new(),
            upstream_mvs_total_key_count: 0,
            mv_backfill_consumed_rows: 0,
            source_backfill_consumed_rows: 0,
            mv_backfill_buffered_rows: 0,
        }
    }

    #[test]
    fn update_ignores_unknown_actor() {
        let actor_known = ActorId::new(1);
        let actor_unknown = ActorId::new(2);
        let mut progress = sample_progress(actor_known);

        let pending = progress.update(
            actor_unknown,
            BackfillState::Done(0, 0),
            progress.upstream_mvs_total_key_count,
        );

        assert!(pending.next_backfill_nodes.is_empty());
        assert_eq!(progress.states.len(), 1);
        assert!(progress.states.contains_key(&actor_known));
    }

    #[test]
    fn refresh_rebuilds_tracking_after_reschedule() {
        let actor_old = ActorId::new(1);
        let actor_new = ActorId::new(2);

        let progress = Progress {
            job_id: JobId::new(1),
            states: HashMap::from([(actor_old, BackfillState::Done(5, 0))]),
            backfill_order_state: BackfillOrderState::default(),
            done_count: 1,
            backfill_upstream_types: HashMap::from([(actor_old, BackfillUpstreamType::MView)]),
            upstream_mv_count: HashMap::new(),
            upstream_mvs_total_key_count: 0,
            mv_backfill_consumed_rows: 5,
            source_backfill_consumed_rows: 0,
            mv_backfill_buffered_rows: 0,
        };

        let mut tracker = CreateMviewProgressTracker {
            tracking_job: TrackingJob {
                job_id: JobId::new(1),
                is_recovered: false,
                source_change: None,
            },
            status: CreateMviewStatus::Backfilling {
                progress,
                pending_backfill_nodes: vec![],
                table_ids_to_truncate: vec![],
            },
        };

        let fragment_infos = HashMap::from([(
            FragmentId::new(10),
            sample_inflight_fragment(
                FragmentId::new(10),
                &[actor_new],
                FragmentTypeFlag::StreamScan,
            ),
        )]);

        tracker.refresh_after_reschedule(&fragment_infos, &HummockVersionStats::default());

        let CreateMviewStatus::Backfilling { progress, .. } = tracker.status else {
            panic!("expected backfilling status");
        };
        assert!(progress.states.contains_key(&actor_new));
        assert!(!progress.states.contains_key(&actor_old));
        assert_eq!(progress.done_count, 0);
        assert_eq!(progress.mv_backfill_consumed_rows, 0);
        assert_eq!(progress.source_backfill_consumed_rows, 0);
    }

    // CDC sources should be initialized as CdcSourceInit
    #[test]
    fn test_cdc_source_initialized_as_cdc_source_init() {
        use std::collections::BTreeMap;

        use risingwave_meta_model::streaming_job;
        use risingwave_pb::catalog::{CreateType, PbSource, StreamSourceInfo};

        use crate::barrier::command::CreateStreamingJobCommandInfo;
        use crate::manager::{StreamingJob, StreamingJobType};
        use crate::model::StreamJobFragmentsToCreate;

        // Create a CDC source with cdc_source_job = true
        let source_info = StreamSourceInfo {
            cdc_source_job: true,
            ..Default::default()
        };

        let source = PbSource {
            id: risingwave_common::id::SourceId::new(100),
            info: Some(source_info),
            with_properties: BTreeMap::from([("connector".to_owned(), "fake-cdc".to_owned())]),
            ..Default::default()
        };

        // Create empty fragments (no actors to track)
        let fragments = StreamJobFragments::for_test(JobId::new(100), BTreeMap::new());
        let stream_job_fragments = StreamJobFragmentsToCreate {
            inner: fragments,
            downstreams: Default::default(),
        };

        let info = CreateStreamingJobCommandInfo {
            stream_job_fragments,
            upstream_fragment_downstreams: Default::default(),
            init_split_assignment: Default::default(),
            definition: "CREATE SOURCE ...".to_owned(),
            job_type: StreamingJobType::Source,
            create_type: CreateType::Foreground,
            streaming_job: StreamingJob::Source(source),
            database_resource_group: risingwave_common::util::worker_util::DEFAULT_RESOURCE_GROUP
                .to_owned(),
            fragment_backfill_ordering: Default::default(),
            cdc_table_snapshot_splits: None,
            locality_fragment_state_table_mapping: Default::default(),
            is_serverless: false,
            replace_sink: None,
            refresh_interval_sec: None,
            streaming_job_model: streaming_job::Model {
                job_id: JobId::new(100),
                job_status: risingwave_meta_model::JobStatus::Creating,
                create_type: risingwave_meta_model::CreateType::Foreground,
                timezone: None,
                config_override: None,
                adaptive_parallelism_strategy: None,
                parallelism: risingwave_meta_model::StreamingParallelism::Adaptive,
                backfill_parallelism: None,
                backfill_adaptive_parallelism_strategy: None,
                backfill_orders: None,
                max_parallelism: 256,
                specific_resource_group: None,
                is_serverless_backfill: false,
                refresh_interval_sec: None,
            },
        };

        let tracker = CreateMviewProgressTracker::new(
            &info,
            &HummockVersionStats::default(),
            &HashMap::new(),
        );

        // CDC source should be in CdcSourceInit state
        assert!(matches!(tracker.status, CreateMviewStatus::CdcSourceInit));
        assert!(!tracker.is_finished());
    }

    // CDC source should transition from CdcSourceInit to Finished when offset is updated
    #[test]
    fn test_cdc_source_transitions_to_finished_on_offset_update() {
        let mut tracker = CreateMviewProgressTracker {
            tracking_job: TrackingJob {
                job_id: JobId::new(300),
                is_recovered: false,
                source_change: None,
            },
            status: CreateMviewStatus::CdcSourceInit,
        };

        // Initially in CdcSourceInit state
        assert!(matches!(tracker.status, CreateMviewStatus::CdcSourceInit));
        assert!(!tracker.is_finished());

        // Mark as finished when offset is updated
        tracker.mark_cdc_source_finished();

        // Should now be in Finished state
        assert!(matches!(tracker.status, CreateMviewStatus::Finished { .. }));
        assert!(tracker.is_finished());
    }

    #[test]
    fn tracking_job_new_without_source_backfill_has_no_source_change() {
        use std::collections::BTreeMap;

        let fragments = StreamJobFragments::for_test(JobId::new(1), BTreeMap::new());
        let job = TrackingJob::new(&fragments);
        assert!(job.source_change.is_none());
    }

    #[test]
    fn tracking_job_new_with_source_backfill_tracks_finished_fragments() {
        use std::collections::{BTreeMap, BTreeSet};

        use risingwave_common::id::SourceId;
        use risingwave_pb::stream_plan::stream_node::NodeBody;
        use risingwave_pb::stream_plan::{MergeNode, SourceBackfillNode};

        use crate::model::Fragment;

        let source_id = SourceId::new(42);
        let backfill_fragment_id = FragmentId::new(2);
        let upstream_source_fragment_id = FragmentId::new(1);

        let nodes = PbStreamNode {
            node_body: Some(NodeBody::SourceBackfill(Box::new(SourceBackfillNode {
                upstream_source_id: source_id,
                ..Default::default()
            }))),
            input: vec![PbStreamNode {
                node_body: Some(NodeBody::Merge(Box::new(MergeNode {
                    upstream_fragment_id: upstream_source_fragment_id,
                    ..Default::default()
                }))),
                ..Default::default()
            }],
            ..Default::default()
        };
        let fragment = Fragment {
            fragment_id: backfill_fragment_id,
            nodes,
            ..Default::default()
        };
        let fragments = StreamJobFragments::for_test(
            JobId::new(1),
            BTreeMap::from([(backfill_fragment_id, fragment)]),
        );

        let job = TrackingJob::new(&fragments);
        let Some(SourceChange::CreateJobFinished {
            finished_backfill_fragments,
        }) = job.source_change
        else {
            panic!("expected CreateJobFinished");
        };
        assert_eq!(
            finished_backfill_fragments,
            HashMap::from([(
                source_id,
                BTreeSet::from([(backfill_fragment_id, upstream_source_fragment_id)]),
            )])
        );
    }
}
