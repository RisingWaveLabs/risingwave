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

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{BoxStream, FuturesUnordered};
use futures::{StreamExt, pin_mut};
use itertools::Itertools;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::compact_task::{self, TaskStatus};
use risingwave_pb::hummock::level_handler::RunningCompactTask;
use rw_futures_util::select_all;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::IntervalStream;
use tracing::warn;

use crate::backup_restore::BackupManagerRef;
use crate::hummock::metrics_utils::{trigger_lsm_stat, trigger_mv_stat};
use crate::hummock::{HummockManager, TASK_NORMAL};

#[derive(Clone, Copy)]
enum SchedulingEvent {
    DynamicCompaction,
    SpaceReclaimCompaction,
    TtlCompaction,
    TombstoneCompaction,
    GroupSplit,
    GroupMerge,
}

#[derive(Clone, Copy)]
enum MaintenanceEvent {
    CheckDeadTask,
    FullGc,
}

fn interval_stream<E>(period: Duration, event: E) -> BoxStream<'static, E>
where
    E: Clone + Send + 'static,
{
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.reset();
    Box::pin(IntervalStream::new(interval).map(move |_| event.clone()))
}

/// Spawn a periodic background loop for a single timer.
///
/// A handler is never re-entered: if it runs longer than its period, the next tick is delayed.
/// Shutdown is observed between handler runs, which matches the legacy timer loop semantics.
fn spawn_periodic_loop<F, Fut>(
    name: &'static str,
    period: Duration,
    shutdown_rx: watch::Receiver<bool>,
    mut handler: F,
) -> JoinHandle<()>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    spawn_serial_timer_lane(
        name,
        vec![interval_stream(period, ())],
        shutdown_rx,
        move |()| handler(),
    )
}

/// Spawn a lane with multiple timers whose handlers must stay serialized.
fn spawn_serial_timer_lane<E, F, Fut>(
    name: &'static str,
    triggers: Vec<BoxStream<'static, E>>,
    mut shutdown_rx: watch::Receiver<bool>,
    mut handler: F,
) -> JoinHandle<()>
where
    E: Send + 'static,
    F: FnMut(E) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        if triggers.is_empty() {
            tracing::info!(
                "Hummock timer handler loop [{}] is disabled (no intervals configured)",
                name
            );
            return;
        }

        let event_stream = select_all(triggers);
        pin_mut!(event_stream);

        loop {
            tokio::select! {
                maybe_event = event_stream.next() => {
                    let Some(event) = maybe_event else {
                        tracing::warn!(
                            "Hummock timer handler loop [{}] event stream ended unexpectedly",
                            name
                        );
                        break;
                    };
                    handler(event).await;
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        tracing::info!("Hummock timer handler loop [{}] is stopped", name);
                        break;
                    }
                }
            }
        }
    })
}

fn spawn_scheduling_loop(
    hummock_manager: Arc<HummockManager>,
    split_interval_sec: u64,
    merge_interval_sec: u64,
    shutdown_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    let mut triggers = vec![
        interval_stream(
            Duration::from_secs(hummock_manager.env.opts.periodic_compaction_interval_sec),
            SchedulingEvent::DynamicCompaction,
        ),
        interval_stream(
            Duration::from_secs(
                hummock_manager
                    .env
                    .opts
                    .periodic_space_reclaim_compaction_interval_sec,
            ),
            SchedulingEvent::SpaceReclaimCompaction,
        ),
        interval_stream(
            Duration::from_secs(
                hummock_manager
                    .env
                    .opts
                    .periodic_ttl_reclaim_compaction_interval_sec,
            ),
            SchedulingEvent::TtlCompaction,
        ),
        interval_stream(
            Duration::from_secs(
                hummock_manager
                    .env
                    .opts
                    .periodic_tombstone_reclaim_compaction_interval_sec,
            ),
            SchedulingEvent::TombstoneCompaction,
        ),
    ];

    if split_interval_sec > 0 {
        triggers.push(interval_stream(
            Duration::from_secs(split_interval_sec),
            SchedulingEvent::GroupSplit,
        ));
    }
    if merge_interval_sec > 0 {
        triggers.push(interval_stream(
            Duration::from_secs(merge_interval_sec),
            SchedulingEvent::GroupMerge,
        ));
    }

    spawn_serial_timer_lane("scheduling", triggers, shutdown_rx, move |event| {
        let hummock_manager = hummock_manager.clone();
        async move {
            if hummock_manager.env.opts.compaction_deterministic_test {
                return;
            }

            match event {
                SchedulingEvent::DynamicCompaction => {
                    hummock_manager
                        .on_handle_trigger_multi_group(compact_task::TaskType::Dynamic)
                        .await;
                }
                SchedulingEvent::SpaceReclaimCompaction => {
                    hummock_manager
                        .on_handle_trigger_multi_group(compact_task::TaskType::SpaceReclaim)
                        .await;
                    // Share the same trigger with SpaceReclaim.
                    hummock_manager
                        .on_handle_trigger_multi_group(compact_task::TaskType::VnodeWatermark)
                        .await;
                }
                SchedulingEvent::TtlCompaction => {
                    hummock_manager
                        .on_handle_trigger_multi_group(compact_task::TaskType::Ttl)
                        .await;
                }
                SchedulingEvent::TombstoneCompaction => {
                    hummock_manager
                        .on_handle_trigger_multi_group(compact_task::TaskType::Tombstone)
                        .await;
                }
                SchedulingEvent::GroupSplit => {
                    hummock_manager.on_handle_schedule_group_split().await;
                }
                SchedulingEvent::GroupMerge => {
                    hummock_manager.on_handle_schedule_group_merge().await;
                }
            }
        }
    })
}

fn spawn_maintenance_loop(
    hummock_manager: Arc<HummockManager>,
    backup_manager: Option<BackupManagerRef>,
    shutdown_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    const CHECK_PENDING_TASK_PERIOD_SEC: u64 = 300;

    let triggers = vec![
        interval_stream(
            Duration::from_secs(CHECK_PENDING_TASK_PERIOD_SEC),
            MaintenanceEvent::CheckDeadTask,
        ),
        interval_stream(
            Duration::from_secs(hummock_manager.env.opts.full_gc_interval_sec),
            MaintenanceEvent::FullGc,
        ),
    ];

    spawn_serial_timer_lane("maintenance", triggers, shutdown_rx, move |event| {
        let hummock_manager = hummock_manager.clone();
        let backup_manager = backup_manager.clone();
        async move {
            match event {
                MaintenanceEvent::CheckDeadTask => {
                    if hummock_manager.env.opts.compaction_deterministic_test {
                        return;
                    }
                    hummock_manager.check_dead_task().await;
                }
                MaintenanceEvent::FullGc => {
                    let retention_sec = hummock_manager.env.opts.min_sst_retention_time_sec;
                    tokio::task::spawn(async move {
                        let _ = hummock_manager
                            .start_full_gc(Duration::from_secs(retention_sec), None, backup_manager)
                            .await
                            .inspect_err(|e| warn!(error = %e.as_report(), "Failed to start GC."));
                    });
                }
            }
        }
    })
}

async fn supervise_child_loops(
    mut shutdown_rx: Receiver<()>,
    child_shutdown_tx: watch::Sender<bool>,
    child_handles: Vec<(&'static str, JoinHandle<()>)>,
) {
    let mut child_handles = child_handles
        .into_iter()
        .map(|(name, handle)| async move { (name, handle.await) })
        .collect::<FuturesUnordered<_>>();

    // Child loops are expected to run until the parent timer task is shutting down. If any of
    // them finishes first, the timer task is in a partial-failure state and should fail fast.
    tokio::select! {
        _ = &mut shutdown_rx => {
            let _ = child_shutdown_tx.send(true);
            while let Some((name, result)) = child_handles.next().await {
                if let Err(e) = result {
                    warn!(
                        handler = name,
                        error = %e.as_report(),
                        "Hummock timer handler loop failed during shutdown"
                    );
                }
            }
        }
        Some((name, result)) = child_handles.next() => {
            match result {
                Ok(()) => {
                    panic!("Hummock timer handler loop [{name}] exited unexpectedly");
                }
                // A cancellation means the runtime is aborting detached tasks during shutdown;
                // treat it as a benign shutdown signal and drain the rest instead of failing.
                Err(e) if e.is_cancelled() => {
                    let _ = child_shutdown_tx.send(true);
                    while let Some((name, result)) = child_handles.next().await {
                        if let Err(e) = result {
                            warn!(
                                handler = name,
                                error = %e.as_report(),
                                "Hummock timer handler loop failed during shutdown"
                            );
                        }
                    }
                }
                Err(e) => {
                    panic!("Hummock timer handler loop [{name}] failed: {}", e.as_report());
                }
            }
        }
    }
}

impl HummockManager {
    pub fn hummock_timer_task(
        hummock_manager: Arc<Self>,
        backup_manager: Option<BackupManagerRef>,
    ) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            const STAT_REPORT_PERIOD_SEC: u64 = 20;
            const COMPACTION_HEARTBEAT_PERIOD_SEC: u64 = 1;

            let split_interval_sec = hummock_manager
                .env
                .opts
                .periodic_scheduling_compaction_group_split_interval_sec;
            let merge_interval_sec = hummock_manager
                .env
                .opts
                .periodic_scheduling_compaction_group_merge_interval_sec;

            tracing::info!(
                split_interval_sec,
                merge_interval_sec,
                report_interval_sec = STAT_REPORT_PERIOD_SEC,
                heartbeat_interval_sec = COMPACTION_HEARTBEAT_PERIOD_SEC,
                "Starting Hummock timer task"
            );

            let (child_shutdown_tx, child_shutdown_rx) = watch::channel(false);
            let mut child_handles: Vec<(&'static str, JoinHandle<()>)> = Vec::new();

            // Latency-sensitive liveness detection. Keep it isolated from slow maintenance/report
            // handlers to avoid head-of-line blocking.
            {
                let hummock_manager = hummock_manager.clone();
                child_handles.push((
                    "heartbeat",
                    spawn_periodic_loop(
                        "heartbeat",
                        Duration::from_secs(COMPACTION_HEARTBEAT_PERIOD_SEC),
                        child_shutdown_rx.clone(),
                        move || {
                            let hummock_manager = hummock_manager.clone();
                            async move {
                                hummock_manager
                                    .handle_timer_compaction_heartbeat_expired_check()
                                    .await;
                            }
                        },
                    ),
                ));
            }

            // Metrics reporting can be slow, but it should not block heartbeat checks.
            {
                let hummock_manager = hummock_manager.clone();
                child_handles.push((
                    "report",
                    spawn_periodic_loop(
                        "report",
                        Duration::from_secs(STAT_REPORT_PERIOD_SEC),
                        child_shutdown_rx.clone(),
                        move || {
                            let hummock_manager = hummock_manager.clone();
                            async move {
                                hummock_manager.handle_timer_report().await;
                            }
                        },
                    ),
                ));
            }

            // Compaction triggers and group split/merge all touch scheduling state. Keep them in
            // one serialized lane to preserve the legacy main-loop mutual exclusion semantics and
            // avoid adding timer-vs-timer scheduling lock contention.
            child_handles.push((
                "scheduling",
                spawn_scheduling_loop(
                    hummock_manager.clone(),
                    split_interval_sec,
                    merge_interval_sec,
                    child_shutdown_rx.clone(),
                ),
            ));

            // Low-frequency background maintenance. These tasks are not latency-sensitive and
            // should not block heartbeat checks.
            child_handles.push((
                "maintenance",
                spawn_maintenance_loop(
                    hummock_manager.clone(),
                    backup_manager,
                    child_shutdown_rx.clone(),
                ),
            ));

            supervise_child_loops(shutdown_rx, child_shutdown_tx, child_handles).await;

            tracing::info!("Hummock timer loop is stopped");
        });
        (join_handle, shutdown_tx)
    }

    async fn handle_timer_report(&self) {
        let mv_id_to_all_table_ids = self
            .metadata_manager
            .get_job_id_to_internal_table_ids_mapping()
            .await;
        let table_write_throughput_statistic_manager =
            self.table_write_throughput_statistic_manager.read().clone();
        let id_to_config = self.get_compaction_group_map().await;

        // Clone the in-memory snapshot while holding locks, then emit metrics outside the locks.
        // This keeps the report lane from blocking writers on `versioning` or
        // `compaction_group_manager` while doing potentially heavy metric iteration.
        let (current_version, version_stats) = {
            let versioning_guard = self
                .versioning
                .read_with_process_name("handle_timer_report")
                .await;
            (
                versioning_guard.current_version.clone(),
                versioning_guard.version_stats.clone(),
            )
        };

        if let Some(mv_id_to_all_table_ids) = mv_id_to_all_table_ids {
            trigger_mv_stat(&self.metrics, &version_stats, mv_id_to_all_table_ids);
        }

        let compaction_group_count = current_version.levels.len();
        self.metrics
            .compaction_group_count
            .set(compaction_group_count as i64);

        let max_statistic_expired_time = std::cmp::max(
            self.env.opts.table_stat_throuput_window_seconds_for_split,
            self.env.opts.table_stat_throuput_window_seconds_for_merge,
        );

        for (group_id, group_levels) in &current_version.levels {
            let Some(compaction_group_config) = id_to_config.get(group_id) else {
                warn!(
                    compaction_group_id = %group_id,
                    "Missing compaction group config when reporting metrics. Skip."
                );
                continue;
            };

            trigger_lsm_stat(
                &self.metrics,
                compaction_group_config.compaction_config(),
                group_levels,
                *group_id,
            );

            let member_table_ids = current_version
                .state_table_info
                .compaction_group_member_table_ids(*group_id);
            let group_size = member_table_ids
                .iter()
                .map(|table_id| {
                    version_stats
                        .table_stats
                        .get(table_id)
                        .map(|stats| stats.total_key_size + stats.total_value_size)
                        .unwrap_or(0)
                        .max(0) as u64
                })
                .sum::<u64>();
            self.metrics
                .compaction_group_size
                .with_label_values(&[&group_id.to_string()])
                .set(group_size as _);

            let mut avg_throughput = 0;
            for table_id in member_table_ids {
                avg_throughput += table_write_throughput_statistic_manager
                    .avg_write_throughput(*table_id, max_statistic_expired_time as i64)
                    as u64;
            }

            self.metrics
                .compaction_group_throughput
                .with_label_values(&[&group_id.to_string()])
                .set(avg_throughput as _);

            let file_count = group_levels.count_ssts();
            self.metrics
                .compaction_group_file_count
                .with_label_values(&[&group_id.to_string()])
                .set(file_count as _);
        }
    }

    async fn handle_timer_compaction_heartbeat_expired_check(&self) {
        // TODO: add metrics to track expired tasks
        // The cancel task has two paths
        // 1. compactor heartbeat cancels the expired task based on task
        // progress (meta + compactor)
        // 2. meta periodically scans the task and performs a cancel on
        // the meta side for tasks that are not updated by heartbeat
        let expired_tasks: Vec<u64> = self
            .compactor_manager
            .get_heartbeat_expired_tasks()
            .into_iter()
            .map(|task| task.task_id)
            .collect();
        if !expired_tasks.is_empty() {
            tracing::info!(
                expired_tasks = ?expired_tasks,
                "Heartbeat expired compaction tasks detected. Attempting to cancel tasks.",
            );
            if let Err(e) = self
                .cancel_compact_tasks(expired_tasks.clone(), TaskStatus::HeartbeatCanceled)
                .await
            {
                tracing::error!(
                    expired_tasks = ?expired_tasks,
                    error = %e.as_report(),
                    "Attempt to remove compaction task due to elapsed heartbeat failed. We will continue to track its heartbeat
                    until we can successfully report its status",
                );
            }
        }
    }
}

impl HummockManager {
    async fn maybe_normalize_compaction_groups_before_merge(&self) {
        if !self.env.opts.enable_compaction_group_normalize {
            return;
        }

        match self
            .normalize_overlapping_compaction_groups_with_limit(
                self.env
                    .opts
                    .max_normalize_splits_per_round
                    .try_into()
                    .unwrap_or(usize::MAX),
            )
            .await
        {
            Ok(split_count) => {
                if split_count > 0 {
                    tracing::info!(
                        "normalize compaction groups finished with {} split(s) before merge scheduling",
                        split_count
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e.as_report(),
                    "failed to normalize compaction groups before merge scheduling"
                );
            }
        }
    }

    async fn check_dead_task(&self) {
        const MAX_COMPACTION_L0_MULTIPLIER: u64 = 32;
        const MAX_COMPACTION_DURATION_SEC: u64 = 20 * 60;
        let slowdown_groups = {
            let versioning_guard = self
                .versioning
                .read_with_process_name("check_dead_task")
                .await;
            let compaction_group_manager = self
                .compaction_group_manager
                .read_with_process_name("check_dead_task")
                .await;
            let mut slowdown_groups: HashMap<CompactionGroupId, u64> = HashMap::default();

            for (group_id, group_levels) in &versioning_guard.current_version.levels {
                let compaction_group_config = compaction_group_manager
                    .try_get_compaction_group_config(*group_id)
                    .expect(
                        "compaction group config should exist for every group in current version",
                    );
                let l0_file_size = group_levels
                    .l0
                    .sub_levels
                    .iter()
                    .map(|level| level.total_file_size)
                    .sum::<u64>();
                if l0_file_size
                    > MAX_COMPACTION_L0_MULTIPLIER
                        * compaction_group_config
                            .compaction_config
                            .max_bytes_for_level_base
                {
                    slowdown_groups.insert(*group_id, l0_file_size);
                }
            }

            slowdown_groups
        };
        if slowdown_groups.is_empty() {
            return;
        }
        let mut pending_tasks: HashMap<u64, (CompactionGroupId, usize, RunningCompactTask)> =
            HashMap::default();
        {
            let compaction_guard = self
                .compaction
                .read_with_process_name("check_dead_task")
                .await;
            for group_id in slowdown_groups.keys() {
                if let Some(status) = compaction_guard.compaction_statuses.get(group_id) {
                    for (idx, level_handler) in status.level_handlers.iter().enumerate() {
                        let tasks = level_handler.pending_tasks().to_vec();
                        if tasks.is_empty() {
                            continue;
                        }
                        for task in tasks {
                            pending_tasks.insert(task.task_id, (*group_id, idx, task));
                        }
                    }
                }
            }
        }
        let task_ids = pending_tasks.keys().cloned().collect_vec();
        let task_infos = self
            .compactor_manager
            .check_tasks_status(&task_ids, Duration::from_secs(MAX_COMPACTION_DURATION_SEC));
        for (task_id, (compact_time, status)) in task_infos {
            if status == TASK_NORMAL {
                continue;
            }
            if let Some((group_id, level_id, task)) = pending_tasks.get(&task_id) {
                let group_size = *slowdown_groups.get(group_id).unwrap();
                warn!(
                    "COMPACTION SLOW: the task-{} of group-{}(size: {}MB) level-{} has not finished after {:?}, {}, it may cause pending sstable files({:?}) blocking other task.",
                    task_id,
                    group_id,
                    group_size / 1024 / 1024,
                    *level_id,
                    compact_time,
                    status,
                    task.ssts
                );
            }
        }
    }

    /// Try to schedule a compaction `split` for the given compaction groups.
    /// The `split` will be triggered if the following conditions are met:
    /// 1. `state table throughput`: If the table is in a high throughput state and it belongs to a multi table group, then an attempt will be made to split the table into separate compaction groups to increase its throughput and reduce the impact on write amplification.
    /// 2. `group size`: If the group size has exceeded the set upper limit, e.g. `max_group_size` * `split_group_size_ratio`
    async fn on_handle_schedule_group_split(&self) {
        let table_write_throughput = self.table_write_throughput_statistic_manager.read().clone();

        let mut group_infos = self.calculate_compaction_group_statistic().await;
        group_infos.sort_by_key(|group| Reverse(group.group_size));

        for group in group_infos {
            if group.table_statistic.len() == 1 {
                // no need to handle the separate compaciton group
                continue;
            }

            self.try_split_compaction_group(&table_write_throughput, group)
                .await;
        }
    }

    #[cfg(test)]
    pub async fn schedule_group_split_for_test(&self) {
        self.on_handle_schedule_group_split().await;
    }

    #[cfg(test)]
    pub async fn schedule_group_merge_for_test(&self) {
        self.on_handle_schedule_group_merge().await;
    }

    async fn on_handle_trigger_multi_group(&self, task_type: compact_task::TaskType) {
        for cg_id in self.compaction_group_ids().await {
            self.compaction_state.try_sched_compaction(
                cg_id,
                task_type,
                super::compaction::ScheduleTrigger::Periodic,
            );
        }
    }

    /// Try to schedule a compaction merge for the given compaction groups.
    /// The merge will be triggered if the following conditions are met:
    /// 1. The compaction group is not contains creating table.
    /// 2. The compaction group is a small group.
    /// 3. All tables in compaction group is in a low throughput state.
    async fn on_handle_schedule_group_merge(&self) {
        self.maybe_normalize_compaction_groups_before_merge().await;

        let created_tables = match self.metadata_manager.get_created_table_ids().await {
            Ok(created_tables) => HashSet::from_iter(created_tables),
            Err(err) => {
                tracing::warn!(error = %err.as_report(), "failed to fetch created table ids");
                return;
            }
        };
        let table_write_throughput_statistic_manager =
            self.table_write_throughput_statistic_manager.read().clone();
        let mut group_infos = self.calculate_compaction_group_statistic().await;
        // sort by first table id for deterministic merge order
        group_infos.sort_by_key(|group| group.table_statistic.keys().next().copied());

        let group_count = group_infos.len();
        if group_count < 2 {
            return;
        }

        let mut base = 0;
        let mut candidate = 1;

        while candidate < group_count {
            let group = &group_infos[base];
            let next_group = &group_infos[candidate];
            match self
                .try_merge_compaction_group(
                    &table_write_throughput_statistic_manager,
                    group,
                    next_group,
                    &created_tables,
                )
                .await
            {
                Ok(_) => candidate += 1,
                Err(e) => {
                    tracing::debug!(
                        error = %e.as_report(),
                        "Failed to merge compaction group",
                    );
                    base = candidate;
                    candidate = base + 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    mod merge_scheduling {
        use std::sync::Arc;
        use std::time::Duration;

        use itertools::Itertools;
        use risingwave_common::catalog::TableId;
        use risingwave_hummock_sdk::CompactionGroupId;
        use risingwave_hummock_sdk::version::HummockVersion;
        use risingwave_meta_model::WorkerId;
        use risingwave_pb::common::worker_node::Property;
        use risingwave_pb::common::{HostAddress, WorkerType};

        use crate::controller::catalog::CatalogController;
        use crate::controller::cluster::{ClusterController, ClusterControllerRef};
        use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
        use crate::hummock::{CompactorManager, HummockManager, HummockManagerRef};
        use crate::manager::{MetaOpts, MetaSrvEnv};

        async fn setup_compute_env_with_meta_opts(
            port: i32,
            opts: MetaOpts,
        ) -> (
            MetaSrvEnv,
            HummockManagerRef,
            ClusterControllerRef,
            WorkerId,
        ) {
            let env = MetaSrvEnv::for_test_opts(opts, |_| ()).await;
            let cluster_ctl = Arc::new(
                ClusterController::new(env.clone(), Duration::from_secs(1))
                    .await
                    .unwrap(),
            );
            let catalog_ctl = Arc::new(CatalogController::new(env.clone()).await.unwrap());
            let compactor_manager = Arc::new(CompactorManager::for_test());
            let (compactor_streams_change_tx, _compactor_streams_change_rx) =
                tokio::sync::mpsc::unbounded_channel();
            let config = CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(1)
                .level0_max_compact_file_number(130)
                .level0_sub_level_compact_level_count(1)
                .level0_overlapping_sub_level_compact_level_count(1)
                .build();
            let hummock_manager = HummockManager::with_config(
                env.clone(),
                cluster_ctl.clone(),
                catalog_ctl,
                Arc::new(Default::default()),
                compactor_manager,
                config,
                compactor_streams_change_tx,
            )
            .await;

            let worker_id = cluster_ctl
                .add_worker(
                    WorkerType::ComputeNode,
                    HostAddress {
                        host: "127.0.0.1".to_owned(),
                        port,
                    },
                    Property {
                        is_streaming: true,
                        is_serving: true,
                        parallelism: 4,
                        ..Default::default()
                    },
                    Default::default(),
                )
                .await
                .unwrap();
            (env, hummock_manager, cluster_ctl, worker_id)
        }

        async fn get_compaction_group_id_by_table_id(
            hummock_manager: HummockManagerRef,
            table_id: u32,
        ) -> CompactionGroupId {
            hummock_manager
                .get_current_version()
                .await
                .state_table_info
                .info()
                .get(&TableId::new(table_id))
                .unwrap()
                .compaction_group_id
        }

        fn member_table_ids(version: &HummockVersion, group_id: CompactionGroupId) -> Vec<u32> {
            version
                .state_table_info
                .compaction_group_member_table_ids(group_id)
                .iter()
                .map(|table_id| table_id.as_raw_id())
                .collect_vec()
        }

        fn assert_no_group_overlap(version: &HummockVersion) {
            let mut ranges = version
                .levels
                .keys()
                .filter_map(|group_id| {
                    let members = member_table_ids(version, *group_id);
                    (!members.is_empty())
                        .then(|| (*members.first().unwrap(), *members.last().unwrap()))
                })
                .collect_vec();
            ranges.sort_by_key(|(min_table_id, _)| *min_table_id);
            assert!(ranges.windows(2).all(|window| window[0].1 < window[1].0));
        }

        #[tokio::test]
        async fn test_merge_scheduling_normalizes_when_split_scheduling_is_disabled() {
            let mut opts = MetaOpts::test(false);
            opts.enable_compaction_group_normalize = true;
            opts.periodic_scheduling_compaction_group_split_interval_sec = 0;

            let (_env, hummock_manager, _, _worker_id) =
                setup_compute_env_with_meta_opts(80, opts).await;
            hummock_manager
                .register_table_ids_for_test(&[(64, 2.into()), (80, 2.into())])
                .await
                .unwrap();
            hummock_manager
                .register_table_ids_for_test(&[(65, 3.into()), (81, 3.into()), (83, 3.into())])
                .await
                .unwrap();

            let cg_64 = get_compaction_group_id_by_table_id(hummock_manager.clone(), 64).await;
            let cg_65 = get_compaction_group_id_by_table_id(hummock_manager.clone(), 65).await;

            hummock_manager.on_handle_schedule_group_merge().await;

            let version = hummock_manager.get_current_version().await;
            assert_eq!(member_table_ids(&version, cg_64), vec![64]);
            assert_eq!(member_table_ids(&version, cg_65), vec![65]);
            assert_no_group_overlap(&version);
        }
    }

    mod periodic_loop {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        use tokio::sync::watch;

        use super::super::{interval_stream, spawn_serial_timer_lane};

        #[tokio::test(start_paused = true)]
        async fn test_serial_timer_lane_does_not_run_handlers_concurrently() {
            #[derive(Clone, Copy)]
            enum TestEvent {
                Split,
                Merge,
            }

            struct DecrOnDrop(Arc<AtomicUsize>);

            impl Drop for DecrOnDrop {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }

            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let in_handler = Arc::new(AtomicUsize::new(0));
            let max_in_handler = Arc::new(AtomicUsize::new(0));
            let handled = Arc::new(AtomicUsize::new(0));

            let handle = spawn_serial_timer_lane(
                "test_serial_lane",
                vec![
                    interval_stream(Duration::from_millis(10), TestEvent::Split),
                    interval_stream(Duration::from_millis(10), TestEvent::Merge),
                ],
                shutdown_rx,
                {
                    let in_handler = in_handler.clone();
                    let max_in_handler = max_in_handler.clone();
                    let handled = handled.clone();
                    move |_event| {
                        let in_handler = in_handler.clone();
                        let max_in_handler = max_in_handler.clone();
                        let handled = handled.clone();
                        async move {
                            handled.fetch_add(1, Ordering::SeqCst);
                            let now = in_handler.fetch_add(1, Ordering::SeqCst) + 1;
                            let _decr = DecrOnDrop(in_handler);
                            max_in_handler.fetch_max(now, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(15)).await;
                        }
                    }
                },
            );

            tokio::time::sleep(Duration::from_millis(120)).await;
            shutdown_tx.send(true).unwrap();
            handle.await.unwrap();

            assert!(handled.load(Ordering::SeqCst) > 0);
            assert_eq!(max_in_handler.load(Ordering::SeqCst), 1);
        }
    }
}
