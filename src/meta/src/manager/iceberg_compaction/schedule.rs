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

use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::{Duration, Instant};

use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::license::Feature;
use risingwave_connector::connector_common::{
    IcebergCommittedSnapshot, IcebergSinkCompactionUpdate,
};
use risingwave_connector::sink::SinkParam;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkId};
use risingwave_connector::sink::iceberg::{CompactionType, IcebergConfig, IcebergWriteMode};
use risingwave_hummock_sdk::HummockContextId;
use risingwave_pb::iceberg_compaction::IcebergCompactionTask;
use risingwave_pb::iceberg_compaction::iceberg_compaction_task::TaskType;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::ReportTask as IcebergReportTask;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::report_task::Status as IcebergReportTaskStatus;
use risingwave_pb::id::IcebergCompactionTaskId;
use thiserror_ext::AsReport;
use tokio::sync::oneshot;

use super::*;

const COMPACTION_RETRY_BACKOFF: Duration = Duration::from_secs(1);

/// Scheduler lifecycle for one sink.
#[derive(Debug, Clone)]
enum CompactionTrackState {
    /// Ready to accept commits and check for trigger conditions.
    ///
    /// `Idle` is not an active attempt state. A manual request may leave a
    /// one-shot task here for the next scheduler selection.
    Idle {
        next_compaction_time: Instant,
        /// A one-shot manual task, consumed when the next attempt starts.
        manual_task_type: Option<TaskType>,
    },
    /// An attempt has been selected, but its task has not been sent to a compactor.
    PendingDispatch { attempt: Arc<CompactionAttempt> },
    /// Compaction task is in-flight. `report_deadline` acts as a lease; if it
    /// expires before a report arrives, the task becomes retryable.
    InFlight {
        task_id: IcebergCompactionTaskId,
        compactor_context_id: HummockContextId,
        attempt: Arc<CompactionAttempt>,
        report_deadline: Instant,
    },
}

/// Immutable task parameters captured when the scheduler selects an attempt.
///
/// The track and async dispatch handle share these parameters so later commits
/// or config updates cannot change the task being dispatched.
#[derive(Debug)]
struct CompactionAttempt {
    task_type: TaskType,
    max_file_sequence_number: Option<i64>,
    pending_commit_count_at_start: usize,
    gc_watermark_snapshot: Option<IcebergCommittedSnapshot>,
}

#[derive(Debug, Clone, Copy)]
struct ScheduledCompactionTask {
    task_id: IcebergCompactionTaskId,
    compactor_context_id: HummockContextId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactionTrackFinishAction {
    KeepTrack,
    RemoveTrack,
}

#[derive(Debug, Clone)]
pub(super) struct CompactionTrack {
    /// Configured task type for the next automatic attempt.
    configured_task_type: TaskType,
    write_mode: IcebergWriteMode,
    trigger_interval_sec: u64,
    /// Minimum pending commit threshold to trigger compaction early.
    /// Compaction triggers when `pending_commit_count` >= this threshold, even before interval expires.
    trigger_snapshot_count: usize,
    report_timeout: Duration,
    last_config_refresh_at: Instant,
    pending_commit_count: usize,
    latest_observed_snapshot: Option<IcebergCommittedSnapshot>,
    /// Inclusive file sequence number boundary for the current automatic compaction round.
    round_max_file_sequence_number: Option<i64>,
    /// Track lifecycle policy after the queued task or active attempt finishes.
    /// Disabling automatic compaction lets an existing task finish before
    /// removing its track; re-enabling can restore `KeepTrack`.
    finish_action: CompactionTrackFinishAction,
    state: CompactionTrackState,
}

impl CompactionTrack {
    fn new(
        configured_task_type: TaskType,
        write_mode: IcebergWriteMode,
        trigger_interval_sec: u64,
        trigger_snapshot_count: usize,
        report_timeout: Duration,
        now: Instant,
    ) -> Self {
        Self {
            configured_task_type,
            write_mode,
            trigger_interval_sec,
            trigger_snapshot_count,
            report_timeout,
            last_config_refresh_at: now,
            pending_commit_count: 0,
            latest_observed_snapshot: None,
            round_max_file_sequence_number: None,
            finish_action: CompactionTrackFinishAction::KeepTrack,
            state: CompactionTrackState::Idle {
                next_compaction_time: now + Duration::from_secs(trigger_interval_sec),
                manual_task_type: None,
            },
        }
    }

    /// Determines if compaction should be triggered.
    ///
    /// Trigger conditions:
    /// - An active automatic round is controlled only by its next-attempt time.
    /// - Otherwise, pending commit threshold or an interval with pending commits
    ///   can trigger a task.
    ///
    /// This ensures:
    /// - `trigger_snapshot_count` is an early trigger threshold
    /// - `compaction_interval_sec` is the maximum wait time (as long as there are new snapshots)
    /// - Force compaction works by setting `next_compaction_time` to now
    /// - No legacy empty compaction runs (an active automatic round is intentionally retryable)
    fn should_trigger(&self, now: Instant) -> bool {
        let next_compaction_time = match &self.state {
            CompactionTrackState::Idle {
                next_compaction_time,
                ..
            } => *next_compaction_time,
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. } => return false,
        };

        let time_ready = now >= next_compaction_time;
        if self.round_max_file_sequence_number.is_some() {
            // Starting a sequence-bounded round consumes the triggering backlog, so
            // `pending_commit_count` tracks only commits after the fixed boundary. A
            // successful non-drained task must remain schedulable without such commits,
            // while a failed task must not let the commit threshold bypass its backoff.
            return time_ready;
        }

        let commit_ready = self.pending_commit_count >= self.trigger_snapshot_count;
        let has_commits = self.pending_commit_count > 0;
        commit_ready || (time_ready && has_commits)
    }

    fn record_observed_snapshot(&mut self, observed_snapshot: IcebergCommittedSnapshot) {
        self.latest_observed_snapshot = Some(observed_snapshot);
    }

    fn record_commit(&mut self) {
        self.pending_commit_count = self.pending_commit_count.saturating_add(1);
    }

    fn record_force_compaction(&mut self, now: Instant, forced_task_type: Option<TaskType>) {
        if let CompactionTrackState::Idle {
            next_compaction_time,
            manual_task_type,
        } = &mut self.state
        {
            if let Some(task_type) = forced_task_type {
                *manual_task_type = Some(task_type);
            }
            *next_compaction_time = now;
            // An automatic round already has a fixed boundary. A force signal
            // during its idle gap only advances the next attempt.
            if self.round_max_file_sequence_number.is_none() {
                self.pending_commit_count = self.pending_commit_count.max(1);
            }
        }
    }

    fn needs_config_refresh(&self, now: Instant, refresh_interval: Duration) -> bool {
        now.saturating_duration_since(self.last_config_refresh_at) >= refresh_interval
    }

    fn mark_config_refreshed(&mut self, now: Instant) {
        self.last_config_refresh_at = now;
    }

    fn effective_task_type(&self) -> TaskType {
        match &self.state {
            CompactionTrackState::Idle {
                manual_task_type, ..
            } => manual_task_type.unwrap_or(self.configured_task_type),
            CompactionTrackState::PendingDispatch { attempt }
            | CompactionTrackState::InFlight { attempt, .. } => attempt.task_type,
        }
    }

    fn start_attempt(&mut self) -> Arc<CompactionAttempt> {
        let CompactionTrackState::Idle {
            manual_task_type, ..
        } = &mut self.state
        else {
            unreachable!("Cannot start an attempt while another attempt is active")
        };

        let manual_task_type = manual_task_type.take();
        let task_type = manual_task_type.unwrap_or(self.configured_task_type);
        let new_round_boundary = if manual_task_type.is_none()
            && self.write_mode == IcebergWriteMode::MergeOnRead
            && self.round_max_file_sequence_number.is_none()
        {
            self.latest_observed_snapshot
                .as_ref()
                .expect("automatic merge-on-read compaction must start from an observed snapshot")
                .max_file_sequence_number
        } else {
            None
        };

        // Manual requests are one-shot tasks. Only an automatic sequence-bounded
        // task may start a round and consume the commits covered by its boundary.
        // Iceberg V1 has no file sequence numbers and keeps the legacy unbounded
        // single-task behavior.
        if let Some(max_file_sequence_number) = new_round_boundary {
            self.round_max_file_sequence_number = Some(max_file_sequence_number);
            self.pending_commit_count = 0;
        }
        let attempt = Arc::new(CompactionAttempt {
            task_type,
            max_file_sequence_number: self.round_max_file_sequence_number,
            pending_commit_count_at_start: self.pending_commit_count,
            gc_watermark_snapshot: self.latest_observed_snapshot.clone(),
        });
        self.state = CompactionTrackState::PendingDispatch {
            attempt: attempt.clone(),
        };
        attempt
    }

    fn mark_dispatched(
        &mut self,
        task_id: IcebergCompactionTaskId,
        compactor_context_id: HummockContextId,
        now: Instant,
    ) {
        let CompactionTrackState::PendingDispatch { attempt } = &self.state else {
            unreachable!("Only a pending attempt can be marked as dispatched")
        };
        self.state = CompactionTrackState::InFlight {
            task_id,
            compactor_context_id,
            attempt: attempt.clone(),
            report_deadline: now + self.report_timeout,
        };
    }

    pub(super) fn active_attempt_gc_watermark_snapshot(
        &self,
    ) -> Option<Option<&IcebergCommittedSnapshot>> {
        match &self.state {
            CompactionTrackState::PendingDispatch { attempt }
            | CompactionTrackState::InFlight { attempt, .. } => {
                Some(attempt.gc_watermark_snapshot.as_ref())
            }
            CompactionTrackState::Idle { .. } => None,
        }
    }

    fn is_pending_dispatch(&self) -> bool {
        matches!(self.state, CompactionTrackState::PendingDispatch { .. })
    }

    fn removes_track_after_finish(&self) -> bool {
        self.finish_action == CompactionTrackFinishAction::RemoveTrack
    }

    fn has_queued_task_or_active_attempt(&self) -> bool {
        matches!(
            &self.state,
            CompactionTrackState::Idle {
                manual_task_type: Some(_),
                ..
            } | CompactionTrackState::PendingDispatch { .. }
                | CompactionTrackState::InFlight { .. }
        )
    }

    pub(super) fn is_in_flight_task(&self, task_id: IcebergCompactionTaskId) -> bool {
        matches!(
            &self.state,
            CompactionTrackState::InFlight {
                task_id: current_task_id,
                ..
            } if *current_task_id == task_id
        )
    }

    fn scheduled_task(&self) -> Option<ScheduledCompactionTask> {
        match &self.state {
            CompactionTrackState::InFlight {
                task_id,
                compactor_context_id,
                ..
            } => Some(ScheduledCompactionTask {
                task_id: *task_id,
                compactor_context_id: *compactor_context_id,
            }),
            CompactionTrackState::Idle { .. } | CompactionTrackState::PendingDispatch { .. } => {
                None
            }
        }
    }

    fn is_report_timed_out(&self, now: Instant) -> bool {
        matches!(
            &self.state,
            CompactionTrackState::InFlight {
                report_deadline,
                ..
            } if now >= *report_deadline
        )
    }

    fn finish_failed(&mut self, now: Instant) -> CompactionTrackFinishAction {
        if !matches!(self.state, CompactionTrackState::InFlight { .. }) {
            unreachable!("Only an in-flight attempt can finish")
        }
        self.state = CompactionTrackState::Idle {
            next_compaction_time: now + COMPACTION_RETRY_BACKOFF,
            manual_task_type: None,
        };
        self.finish_action
    }

    /// Re-queue the track as idle after a pre-dispatch failure.
    ///
    /// `pending_commit_count` is intentionally preserved so commits that arrive
    /// while the track is pending dispatch are not lost if task dispatch fails
    /// before the compactor accepts the task.
    ///
    /// `next_compaction_time` starts a short retry backoff rather than restoring
    /// its previous timestamp. Candidates are dispatched in ascending order of
    /// this field, so restoring a stale timestamp would let a repeatedly-failing
    /// track sort ahead of every healthy sink and monopolize dispatch slots.
    fn revert_pre_dispatch_failure(&mut self, now: Instant) -> CompactionTrackFinishAction {
        if !self.is_pending_dispatch() {
            unreachable!("Only a pending attempt can be reverted")
        }
        self.state = CompactionTrackState::Idle {
            next_compaction_time: now + COMPACTION_RETRY_BACKOFF,
            manual_task_type: None,
        };
        self.finish_action
    }

    fn update_interval(&mut self, new_interval_sec: u64, now: Instant) {
        if self.trigger_interval_sec == new_interval_sec {
            return;
        }

        self.trigger_interval_sec = new_interval_sec;
        if self.round_max_file_sequence_number.is_some() {
            // The new interval applies after the round drains. The current deadline
            // already represents either immediate continuation or retry backoff.
            return;
        }

        match &mut self.state {
            CompactionTrackState::Idle {
                next_compaction_time,
                ..
            } => {
                *next_compaction_time = now + Duration::from_secs(new_interval_sec);
            }
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. } => {}
        }
    }

    fn finish_success(&mut self, now: Instant) -> CompactionTrackFinishAction {
        let CompactionTrackState::InFlight { attempt, .. } = &self.state else {
            unreachable!("Only an in-flight attempt can finish")
        };
        if attempt.max_file_sequence_number.is_some() {
            // Success means this attempt made progress, but only `Drained`
            // proves that no work remains below the fixed boundary.
            self.state = CompactionTrackState::Idle {
                next_compaction_time: now,
                manual_task_type: None,
            };
        } else {
            self.pending_commit_count = self
                .pending_commit_count
                .saturating_sub(attempt.pending_commit_count_at_start);
            self.state = CompactionTrackState::Idle {
                next_compaction_time: now + Duration::from_secs(self.trigger_interval_sec),
                manual_task_type: None,
            };
        }
        self.finish_action
    }

    /// Completes a sequence-bounded round while preserving commits that arrived
    /// after its fixed boundary for the next round.
    fn finish_drained(&mut self, now: Instant) -> CompactionTrackFinishAction {
        let CompactionTrackState::InFlight { attempt, .. } = &self.state else {
            unreachable!("Only an in-flight attempt can finish")
        };
        debug_assert_eq!(
            attempt.max_file_sequence_number,
            self.round_max_file_sequence_number
        );
        debug_assert!(attempt.max_file_sequence_number.is_some());
        self.round_max_file_sequence_number = None;
        self.state = CompactionTrackState::Idle {
            next_compaction_time: now + Duration::from_secs(self.trigger_interval_sec),
            manual_task_type: None,
        };
        self.finish_action
    }

    fn is_in_flight_bounded_attempt(&self) -> bool {
        matches!(
            &self.state,
            CompactionTrackState::InFlight { attempt, .. }
                if attempt.max_file_sequence_number.is_some()
        )
    }
}

pub(crate) struct IcebergCompactionHandle {
    sink_id: SinkId,
    attempt: Arc<CompactionAttempt>,
    inner: Arc<RwLock<IcebergCompactionManagerInner>>,
    metadata_manager: MetadataManager,
    dispatched: bool,
}

impl IcebergCompactionHandle {
    fn new(
        sink_id: SinkId,
        attempt: Arc<CompactionAttempt>,
        inner: Arc<RwLock<IcebergCompactionManagerInner>>,
        metadata_manager: MetadataManager,
    ) -> Self {
        Self {
            sink_id,
            attempt,
            inner,
            metadata_manager,
            dispatched: false,
        }
    }

    pub async fn send_compact_task(
        mut self,
        compactor: Arc<crate::hummock::IcebergCompactor>,
        task_id: IcebergCompactionTaskId,
    ) -> MetaResult<()> {
        let Some(prost_sink_catalog) = self
            .metadata_manager
            .catalog_controller
            .get_sink_by_id(self.sink_id)
            .await?
        else {
            tracing::warn!(
                iceberg_component = "compaction_scheduler",
                iceberg_operation = "dispatch_task",
                sink_id = %self.sink_id,
                task_id = %task_id,
                "iceberg_compaction_dispatch_sink_not_found",
            );
            return Ok(());
        };
        let sink_catalog = SinkCatalog::from(prost_sink_catalog);
        let param = SinkParam::try_from_sink_catalog(sink_catalog)?;

        self.try_dispatch_task(
            &compactor,
            IcebergCompactionTask {
                task_id,
                sink_id: self.sink_id.as_raw_id(),
                props: param.properties,
                task_type: self.attempt.task_type as i32,
                pk_index_coordinated: false,
                max_file_sequence_number: self.attempt.max_file_sequence_number,
            },
        )
    }

    fn try_dispatch_task(
        &mut self,
        compactor: &crate::hummock::IcebergCompactor,
        task: IcebergCompactionTask,
    ) -> MetaResult<()> {
        use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::Event as IcebergResponseEvent;

        let task_id = task.task_id;
        // Validate and send under the same lock so a cleared schedule cannot
        // dispatch a task after the sink has been removed.
        let mut guard = self.inner.write();
        let Some(track) = guard
            .sink_schedules
            .get_mut(&self.sink_id)
            .filter(|track| track.is_pending_dispatch())
        else {
            tracing::warn!(
                iceberg_component = "compaction_scheduler",
                iceberg_operation = "dispatch_task",
                sink_id = %self.sink_id,
                task_id = %task_id,
                "iceberg_compaction_dispatch_track_not_pending",
            );
            return Ok(());
        };

        let result = compactor.send_event(IcebergResponseEvent::CompactTask(task));
        if result.is_ok() {
            track.mark_dispatched(task_id, compactor.context_id(), Instant::now());
            self.dispatched = true;
        }
        result
    }
}

impl Drop for IcebergCompactionHandle {
    fn drop(&mut self) {
        let waiter = {
            let mut guard = self.inner.write();
            let finish_action = if !self.dispatched
                && let Some(track) = guard.sink_schedules.get_mut(&self.sink_id)
                && track.is_pending_dispatch()
            {
                Some(track.revert_pre_dispatch_failure(Instant::now()))
            } else {
                None
            };
            if let Some(finish_action) = finish_action {
                let waiter = guard.manual_compaction_waiters.remove(&self.sink_id);
                IcebergCompactionManager::apply_track_finish_action(
                    &mut guard,
                    self.sink_id,
                    finish_action,
                );
                waiter
            } else {
                None
            }
        };

        if let Some(waiter) = waiter {
            let _ = waiter.send(Err(anyhow!(
                "Iceberg compaction task failed before dispatch for sink {}",
                self.sink_id
            )
            .into()));
        }
    }
}

#[derive(Debug, Clone)]
enum SinkUpdateKind {
    /// A normal sink commit. It increases the pending snapshot count.
    Commit {
        observed_snapshot: IcebergCommittedSnapshot,
    },
    /// A force signal from the sink update path. It triggers the configured
    /// compaction type and still follows the automatic-compaction config gate.
    ForceCompaction {
        observed_snapshot: IcebergCommittedSnapshot,
    },
    /// A user-triggered manual request. It can bypass disabled automatic
    /// compaction and supplies the task type selected for this request.
    ManualForceCompaction { task_type: TaskType },
}

impl SinkUpdateKind {
    fn apply_to_track(self, track: &mut CompactionTrack, now: Instant) {
        match self {
            SinkUpdateKind::Commit { observed_snapshot } => {
                track.record_observed_snapshot(observed_snapshot);
                track.record_commit();
            }
            SinkUpdateKind::ForceCompaction { observed_snapshot } => {
                track.record_observed_snapshot(observed_snapshot);
                track.record_force_compaction(now, None);
            }
            SinkUpdateKind::ManualForceCompaction { task_type } => {
                track.record_force_compaction(now, Some(task_type))
            }
        }
    }

    fn allows_disabled_compaction(&self) -> bool {
        matches!(self, SinkUpdateKind::ManualForceCompaction { .. })
    }
}

/// Result of the read-only preparation step before applying a sink update.
///
/// This bundles the original update intent together with the metadata loaded
/// across the async gap, so the apply step can consume a single object.
///
/// `allow_track_initialization` stays `true` only when the sink had no track
/// before the async config load. This lets the apply step initialize a new
/// track for first-time updates, while preventing a stale update from
/// resurrecting a track that disappeared during the async gap.
struct PreparedSinkUpdate {
    sink_id: SinkId,
    kind: SinkUpdateKind,
    now: Instant,
    allow_track_initialization: bool,
    loaded_config: Option<IcebergConfig>,
}

#[derive(Debug, Clone)]
pub struct IcebergCompactionScheduleStatus {
    pub sink_id: SinkId,
    pub task_type: String,
    pub trigger_interval_sec: u64,
    pub trigger_snapshot_count: usize,
    pub schedule_state: String,
    pub next_compaction_after_sec: Option<u64>,
    pub pending_snapshot_count: Option<usize>,
    pub is_triggerable: bool,
}

impl IcebergCompactionManager {
    fn apply_track_finish_action(
        guard: &mut IcebergCompactionManagerInner,
        sink_id: SinkId,
        finish_action: CompactionTrackFinishAction,
    ) {
        match finish_action {
            CompactionTrackFinishAction::KeepTrack => {}
            CompactionTrackFinishAction::RemoveTrack => {
                guard.sink_schedules.remove(&sink_id);
            }
        }
    }

    pub(super) fn refresh_schedule_config(
        &self,
        track: &mut CompactionTrack,
        iceberg_config: &IcebergConfig,
        now: Instant,
    ) {
        let (configured_task_type, write_mode, trigger_interval_sec, trigger_snapshot_count) =
            self.resolve_schedule_values(iceberg_config);
        debug_assert_eq!(
            track.write_mode, write_mode,
            "Iceberg write mode cannot change while a schedule track exists"
        );
        track.configured_task_type = configured_task_type;
        track.trigger_snapshot_count = trigger_snapshot_count;
        track.update_interval(trigger_interval_sec, now);
        track.mark_config_refreshed(now);
    }

    pub async fn update_iceberg_commit_info(&self, msg: IcebergSinkCompactionUpdate) {
        let IcebergSinkCompactionUpdate {
            sink_id,
            force_compaction,
            observed_snapshot,
        } = msg;
        let kind = if force_compaction {
            SinkUpdateKind::ForceCompaction { observed_snapshot }
        } else {
            SinkUpdateKind::Commit { observed_snapshot }
        };
        let prepared_update = self
            .prepare_sink_update(sink_id, kind, Instant::now())
            .await;

        let mut guard = self.inner.write();
        self.apply_sink_update(&mut guard, prepared_update);
    }

    async fn prepare_sink_update(
        &self,
        sink_id: SinkId,
        kind: SinkUpdateKind,
        now: Instant,
    ) -> PreparedSinkUpdate {
        let refresh_interval = self.config_refresh_interval();
        let (allow_track_initialization, should_refresh_config) = {
            let guard = self.inner.read();
            match guard.sink_schedules.get(&sink_id) {
                Some(track) => (false, track.needs_config_refresh(now, refresh_interval)),
                None => (true, true),
            }
        };

        let loaded_config = if should_refresh_config {
            match self.load_iceberg_config(sink_id).await {
                Ok(config) => Some(config),
                Err(e) => {
                    tracing::warn!(
                        error = ?e.as_report(),
                        "Failed to load iceberg config for sink {}",
                        sink_id
                    );
                    None
                }
            }
        } else {
            None
        };

        PreparedSinkUpdate {
            sink_id,
            kind,
            now,
            allow_track_initialization,
            loaded_config,
        }
    }

    fn apply_sink_update(
        &self,
        guard: &mut IcebergCompactionManagerInner,
        prepared_update: PreparedSinkUpdate,
    ) -> bool {
        let PreparedSinkUpdate {
            sink_id,
            kind,
            now,
            allow_track_initialization,
            loaded_config,
        } = prepared_update;
        let refresh_interval = self.config_refresh_interval();

        if let Some(config) = loaded_config.as_ref() {
            if config.enable_snapshot_expiration {
                guard.snapshot_expiration_sink_ids.insert(sink_id);
            } else {
                guard.snapshot_expiration_sink_ids.remove(&sink_id);
            }
            if config.enable_manifest_rewrite {
                guard.manifest_rewrite_sink_ids.insert(sink_id);
            } else {
                guard.manifest_rewrite_sink_ids.remove(&sink_id);
            }

            if !config.enable_compaction && !kind.allows_disabled_compaction() {
                let keep_until_task_finishes =
                    guard.sink_schedules.get_mut(&sink_id).is_some_and(|track| {
                        let keep = track.has_queued_task_or_active_attempt();
                        if keep {
                            // Preserve the selected attempt, including its round boundary, so
                            // its report is interpreted consistently. The disabled track is
                            // removed as soon as that one-shot task finishes.
                            track.finish_action = CompactionTrackFinishAction::RemoveTrack;
                        }
                        keep
                    });
                if !keep_until_task_finishes {
                    guard.sink_schedules.remove(&sink_id);
                }
                return false;
            }
        }

        match guard.sink_schedules.entry(sink_id) {
            Entry::Occupied(entry) => {
                let track = entry.into_mut();
                if track.removes_track_after_finish()
                    && !kind.allows_disabled_compaction()
                    && !loaded_config
                        .as_ref()
                        .is_some_and(|config| config.enable_compaction)
                {
                    return false;
                }
                if track.needs_config_refresh(now, refresh_interval)
                    && let Some(config) = loaded_config.as_ref()
                {
                    self.refresh_schedule_config(track, config, now);
                }
                if let Some(config) = loaded_config.as_ref() {
                    track.finish_action =
                        if kind.allows_disabled_compaction() && !config.enable_compaction {
                            CompactionTrackFinishAction::RemoveTrack
                        } else {
                            CompactionTrackFinishAction::KeepTrack
                        };
                }

                kind.apply_to_track(track, now);
                true
            }
            Entry::Vacant(entry) => {
                if !allow_track_initialization {
                    tracing::warn!(
                        iceberg_component = "compaction_scheduler",
                        iceberg_operation = "apply_sink_update",
                        sink_id = %sink_id,
                        "iceberg_compaction_update_ignored_track_missing",
                    );
                    return false;
                }

                let Some(config) = loaded_config.as_ref() else {
                    tracing::warn!(
                        iceberg_component = "compaction_scheduler",
                        iceberg_operation = "apply_sink_update",
                        sink_id = %sink_id,
                        "iceberg_compaction_update_ignored_config_unavailable",
                    );
                    return false;
                };

                let track = entry.insert(self.create_compaction_track(config, now));
                track.finish_action =
                    if kind.allows_disabled_compaction() && !config.enable_compaction {
                        CompactionTrackFinishAction::RemoveTrack
                    } else {
                        CompactionTrackFinishAction::KeepTrack
                    };
                kind.apply_to_track(track, now);
                true
            }
        }
    }

    pub(super) fn create_compaction_track(
        &self,
        iceberg_config: &IcebergConfig,
        now: Instant,
    ) -> CompactionTrack {
        let (configured_task_type, write_mode, trigger_interval_sec, trigger_snapshot_count) =
            self.resolve_schedule_values(iceberg_config);

        CompactionTrack::new(
            configured_task_type,
            write_mode,
            trigger_interval_sec,
            trigger_snapshot_count,
            self.report_timeout(),
            now,
        )
    }

    fn resolve_schedule_values(
        &self,
        iceberg_config: &IcebergConfig,
    ) -> (TaskType, IcebergWriteMode, u64, usize) {
        // COW compaction type is an internal policy. Ignore the legacy persisted value so that
        // both existing and new COW sinks follow the license-based default.
        let configured_type = match iceberg_config.write_mode {
            IcebergWriteMode::CopyOnWrite => None,
            IcebergWriteMode::MergeOnRead => iceberg_config.compaction_type,
        };
        let compaction_type = match configured_type {
            Some(compaction_type) => compaction_type,
            None if Feature::IcebergCompaction.check_available().is_ok() => CompactionType::Auto,
            None => CompactionType::Full,
        };

        (
            match compaction_type {
                CompactionType::Auto => TaskType::Auto,
                CompactionType::Full => TaskType::Full,
                CompactionType::SmallFiles => TaskType::SmallFiles,
                CompactionType::FilesWithDelete => TaskType::FilesWithDelete,
            },
            iceberg_config.write_mode,
            iceberg_config.compaction_interval_sec(),
            iceberg_config.trigger_snapshot_count(),
        )
    }

    pub(super) async fn start_manual_compaction(
        &self,
        sink_id: SinkId,
    ) -> MetaResult<oneshot::Receiver<MetaResult<IcebergCompactionTaskId>>> {
        let prepared_update = self
            .prepare_sink_update(
                sink_id,
                SinkUpdateKind::ManualForceCompaction {
                    task_type: TaskType::Full,
                },
                Instant::now(),
            )
            .await;
        let mut guard = self.inner.write();
        let now = Instant::now();
        if guard.manual_compaction_waiters.contains_key(&sink_id) {
            return Err(anyhow!(
                "manual iceberg compaction is already waiting for sink {}",
                sink_id
            )
            .into());
        }

        if let Some(track) = guard.sink_schedules.get(&sink_id) {
            if track.round_max_file_sequence_number.is_some() {
                return Err(anyhow!(
                    "manual Full compaction is rejected while an automatic round is active for sink {}",
                    sink_id
                )
                .into());
            }
            match &track.state {
                CompactionTrackState::PendingDispatch { attempt } => {
                    return Err(anyhow!(
                        "iceberg compaction task is already running for sink {} \
                         (state=pending_dispatch, pending_commit_count_at_start={}, \
                         pending_commit_count={})",
                        sink_id,
                        attempt.pending_commit_count_at_start,
                        track.pending_commit_count
                    )
                    .into());
                }
                CompactionTrackState::InFlight {
                    task_id,
                    attempt,
                    report_deadline,
                    ..
                } => {
                    return Err(anyhow!(
                        "iceberg compaction task is already running for sink {} \
                         (state=in_flight, task_id={}, pending_commit_count_at_start={}, \
                         pending_commit_count={}, report_timeout_after_sec={})",
                        sink_id,
                        task_id,
                        attempt.pending_commit_count_at_start,
                        track.pending_commit_count,
                        report_deadline.saturating_duration_since(now).as_secs()
                    )
                    .into());
                }
                CompactionTrackState::Idle { .. } => {}
            }
        }

        if self.apply_sink_update(&mut guard, prepared_update) {
            let (tx, rx) = oneshot::channel();
            guard.manual_compaction_waiters.insert(sink_id, tx);
            Ok(rx)
        } else {
            Err(anyhow!(
                "failed to trigger manual iceberg compaction for sink {}",
                sink_id
            )
            .into())
        }
    }

    pub(super) fn cancel_manual_compaction_waiter(&self, sink_id: SinkId) {
        self.inner
            .write()
            .manual_compaction_waiters
            .remove(&sink_id);
    }

    fn finish_timed_out_compaction_tasks(
        guard: &mut IcebergCompactionManagerInner,
        now: Instant,
    ) -> Vec<(SinkId, ManualCompactionWaiter)> {
        let mut timed_out_tasks = Vec::new();
        for (&sink_id, track) in &mut guard.sink_schedules {
            if track.is_report_timed_out(now) {
                tracing::warn!(
                    iceberg_component = "compaction_scheduler",
                    iceberg_operation = "report_timeout",
                    sink_id = %sink_id,
                    "iceberg_compaction_task_report_timed_out",
                );
                timed_out_tasks.push((sink_id, track.finish_failed(now)));
            }
        }

        let mut timed_out_waiters = Vec::new();
        for (sink_id, finish_action) in timed_out_tasks {
            if let Some(waiter) = guard.manual_compaction_waiters.remove(&sink_id) {
                timed_out_waiters.push((sink_id, waiter));
            }
            Self::apply_track_finish_action(guard, sink_id, finish_action);
        }
        timed_out_waiters
    }

    pub(crate) fn get_top_n_iceberg_commit_sink_ids(
        &self,
        n: usize,
    ) -> Vec<IcebergCompactionHandle> {
        let now = Instant::now();
        let (handles, timed_out_waiters) = {
            let mut guard = self.inner.write();
            let timed_out_waiters = Self::finish_timed_out_compaction_tasks(&mut guard, now);

            let mut candidates = Vec::new();
            for (sink_id, track) in &guard.sink_schedules {
                if track.should_trigger(now)
                    && let CompactionTrackState::Idle {
                        next_compaction_time,
                        ..
                    } = &track.state
                {
                    candidates.push((*sink_id, *next_compaction_time));
                }
            }

            candidates.sort_by_key(|c| c.1);

            let handles = candidates
                .into_iter()
                .take(n)
                .filter_map(|(sink_id, _)| {
                    let track = guard.sink_schedules.get_mut(&sink_id)?;
                    let attempt = track.start_attempt();

                    Some(IcebergCompactionHandle::new(
                        sink_id,
                        attempt,
                        self.inner.clone(),
                        self.metadata_manager.clone(),
                    ))
                })
                .collect();

            (handles, timed_out_waiters)
        };

        for (sink_id, waiter) in timed_out_waiters {
            let _ = waiter.send(Err(anyhow!(
                "Iceberg compaction task report timed out for sink {}",
                sink_id
            )
            .into()));
        }

        handles
    }

    pub fn clear_iceberg_maintenance_by_sink_id(&self, sink_id: SinkId) {
        let (task_to_cancel, waiter) = {
            let mut guard = self.inner.write();
            let task_to_cancel = Self::remove_sink_schedule(&mut guard, sink_id);
            guard.snapshot_expiration_sink_ids.remove(&sink_id);
            guard.manifest_rewrite_sink_ids.remove(&sink_id);
            let waiter = guard.manual_compaction_waiters.remove(&sink_id);
            (task_to_cancel, waiter)
        };
        self.cancel_scheduled_task_if_any(sink_id, task_to_cancel);

        if let Some(waiter) = waiter {
            let _ = waiter.send(Err(anyhow!(
                "Iceberg compaction maintenance was cleared for sink {}",
                sink_id
            )
            .into()));
        }
    }

    fn remove_sink_schedule(
        guard: &mut IcebergCompactionManagerInner,
        sink_id: SinkId,
    ) -> Option<ScheduledCompactionTask> {
        guard
            .sink_schedules
            .remove(&sink_id)
            .and_then(|track| track.scheduled_task())
    }

    fn cancel_scheduled_task_if_any(&self, sink_id: SinkId, task: Option<ScheduledCompactionTask>) {
        let Some(ScheduledCompactionTask {
            task_id,
            compactor_context_id,
        }) = task
        else {
            return;
        };

        let Some(compactor) = self
            .iceberg_compactor_manager
            .get_compactor(compactor_context_id)
        else {
            tracing::warn!(
                sink_id = %sink_id,
                task_id = %task_id,
                compactor_context_id = %compactor_context_id,
                "Unable to cancel iceberg compaction task because compactor is no longer registered",
            );
            return;
        };

        tracing::info!(
            sink_id = %sink_id,
            task_id = %task_id,
            compactor_context_id = %compactor_context_id,
            "Cancelling iceberg compaction task for removed schedule",
        );

        if let Err(e) = compactor.cancel_task(task_id) {
            tracing::warn!(
                error = %e.as_report(),
                sink_id = %sink_id,
                task_id = %task_id,
                compactor_context_id = %compactor_context_id,
                "Failed to cancel iceberg compaction task for removed schedule",
            );
        }
    }

    pub fn list_compaction_statuses(&self) -> Vec<IcebergCompactionScheduleStatus> {
        let now = Instant::now();
        let schedules = {
            let guard = self.inner.read();
            guard
                .sink_schedules
                .iter()
                .map(|(&sink_id, track)| (sink_id, track.clone()))
                .collect_vec()
        };

        let mut statuses = schedules
            .into_iter()
            .map(|(sink_id, track)| {
                let next_compaction_after_sec = match &track.state {
                    CompactionTrackState::Idle {
                        next_compaction_time,
                        ..
                    } => Some(
                        next_compaction_time
                            .saturating_duration_since(now)
                            .as_secs(),
                    ),
                    CompactionTrackState::PendingDispatch { .. }
                    | CompactionTrackState::InFlight { .. } => None,
                };
                let is_triggerable = track.should_trigger(now);

                IcebergCompactionScheduleStatus {
                    sink_id,
                    task_type: track
                        .effective_task_type()
                        .as_str_name()
                        .to_ascii_lowercase(),
                    trigger_interval_sec: track.trigger_interval_sec,
                    trigger_snapshot_count: track.trigger_snapshot_count,
                    schedule_state: match track.state {
                        CompactionTrackState::Idle { .. } => "idle".to_owned(),
                        CompactionTrackState::PendingDispatch { .. }
                        | CompactionTrackState::InFlight { .. } => "processing".to_owned(),
                    },
                    next_compaction_after_sec,
                    pending_snapshot_count: Some(track.pending_commit_count),
                    is_triggerable,
                }
            })
            .collect_vec();

        statuses.sort_by_key(|status| status.sink_id);
        statuses
    }

    pub fn handle_report_task(&self, report: IcebergReportTask) {
        let sink_id = SinkId::from(report.sink_id);
        let task_id = report.task_id;
        let status = IcebergReportTaskStatus::try_from(report.status)
            .unwrap_or(IcebergReportTaskStatus::Unspecified);
        let now = Instant::now();

        let waiter = {
            let mut guard = self.inner.write();
            let mut waiter = None;

            match guard.sink_schedules.get_mut(&sink_id) {
                Some(track) if track.is_in_flight_task(task_id) => {
                    let finish_action = match status {
                        IcebergReportTaskStatus::Success => track.finish_success(now),
                        IcebergReportTaskStatus::Drained
                            if track.is_in_flight_bounded_attempt() =>
                        {
                            track.finish_drained(now)
                        }
                        IcebergReportTaskStatus::Drained
                        | IcebergReportTaskStatus::Failed
                        | IcebergReportTaskStatus::Unspecified => {
                            tracing::warn!(
                                iceberg_component = "compaction_scheduler",
                                iceberg_operation = "handle_report",
                                sink_id = %sink_id,
                                task_id = %task_id,
                                status = ?status,
                                error_message = report.error_message.as_deref().unwrap_or_default(),
                                "iceberg_compaction_task_reported_failure",
                            );
                            track.finish_failed(now)
                        }
                    };

                    Self::apply_track_finish_action(&mut guard, sink_id, finish_action);
                    waiter = guard.manual_compaction_waiters.remove(&sink_id);
                }
                Some(_) => {
                    tracing::warn!(
                        iceberg_component = "compaction_scheduler",
                        iceberg_operation = "handle_report",
                        sink_id = %sink_id,
                        task_id = %task_id,
                        status = ?status,
                        "iceberg_compaction_report_ignored_stale",
                    );
                }
                None => {
                    tracing::warn!(
                        iceberg_component = "compaction_scheduler",
                        iceberg_operation = "handle_report",
                        sink_id = %sink_id,
                        task_id = %task_id,
                        status = ?status,
                        "iceberg_compaction_report_unknown_sink",
                    );
                }
            }

            waiter
        };

        if let Some(waiter) = waiter {
            Self::complete_manual_task_waiter(waiter, &report);
        }
    }
}

#[cfg(test)]
mod tests;
