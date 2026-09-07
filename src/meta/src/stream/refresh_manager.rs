// Copyright 2025 RisingWave Labs
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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use parking_lot::Mutex;
use risingwave_common::catalog::{DatabaseId, FragmentTypeFlag, TableId};
use risingwave_common::metrics::LabelGuardedIntCounter;
use risingwave_common::util::epoch::Epoch;
use risingwave_meta_model::ActorId;
use risingwave_meta_model::refresh_job::{self, RefreshState};
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::id::SourceId;
use risingwave_pb::meta::{RefreshRequest, RefreshResponse};
use thiserror_ext::AsReport;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;

use crate::barrier::{BarrierScheduler, Command, SharedActorInfos};
use crate::manager::{MetaSrvEnv, MetadataManager};
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::{MetaError, MetaResult};

pub type GlobalRefreshManagerRef = Arc<GlobalRefreshManager>;

pub struct GlobalRefreshManager {
    metadata_manager: MetadataManager,
    barrier_scheduler: BarrierScheduler,
    shared_actor_infos: SharedActorInfos,
    progress_trackers: Mutex<GlobalRefreshTableProgressTracker>,
    refresh_job_metrics: Mutex<HashMap<TableId, RefreshJobMetrics>>,
    scheduler_notify: Notify,
    scheduler_interval: Duration,
}

impl GlobalRefreshManager {
    pub async fn start(
        metadata_manager: MetadataManager,
        barrier_scheduler: BarrierScheduler,
        env: &MetaSrvEnv,
        scheduler_interval: Duration,
    ) -> MetaResult<(GlobalRefreshManagerRef, JoinHandle<()>, oneshot::Sender<()>)> {
        let shared_actor_infos = env.shared_actor_infos().clone();
        let manager = Arc::new(Self {
            metadata_manager: metadata_manager.clone(),
            barrier_scheduler,
            shared_actor_infos,
            progress_trackers: Mutex::new(GlobalRefreshTableProgressTracker::default()),
            refresh_job_metrics: Mutex::new(HashMap::new()),
            scheduler_notify: Notify::new(),
            scheduler_interval,
        });

        manager
            .metadata_manager
            .reset_all_refresh_jobs_to_idle()
            .await?;
        manager.sync_refreshable_jobs().await?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let join_handle = Self::spawn_scheduler(manager.clone(), shutdown_rx);

        Ok((manager, join_handle, shutdown_tx))
    }

    fn spawn_scheduler(
        manager: GlobalRefreshManagerRef,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) -> JoinHandle<()> {
        let scheduler_interval = manager.scheduler_interval;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(scheduler_interval);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(err) = manager.handle_scheduler_tick().await {
                            tracing::warn!(error = %err.as_report(), "refresh scheduler tick failed");
                        }
                    }
                    _ = manager.scheduler_notify.notified() => {
                        if let Err(err) = manager.handle_scheduler_tick().await {
                            tracing::warn!(error = %err.as_report(), "refresh scheduler tick failed");
                        }
                    }
                    _ = &mut shutdown_rx => {
                        tracing::info!("refresh scheduler shutting down");
                        break;
                    }
                }
            }
        })
    }

    pub async fn trigger_manual_refresh(
        self: &Arc<Self>,
        request: RefreshRequest,
        shared_actor_infos: &SharedActorInfos,
    ) -> MetaResult<RefreshResponse> {
        let table_id = request.table_id;
        let associated_source_id = request.associated_source_id;
        tracing::info!(%table_id, %associated_source_id, "trigger manual refresh");

        self.ensure_refreshable(table_id, associated_source_id)
            .await?;

        let result = self
            .execute_refresh(table_id, associated_source_id, shared_actor_infos)
            .await;

        match result {
            Ok(_) => Ok(RefreshResponse { status: None }),
            Err(err) => Err(err),
        }
    }

    pub async fn mark_refresh_complete(&self, table_id: TableId) -> MetaResult<()> {
        self.metadata_manager
            .update_refresh_job_status(table_id, RefreshState::Idle, None, true)
            .await?;
        self.remove_progress_tracker(table_id, "success");
        tracing::info!(%table_id, "Table refresh completed, state updated to Idle");
        Ok(())
    }

    pub fn mark_list_stage_finished(
        &self,
        table_id: TableId,
        actors: &HashSet<ActorId>,
    ) -> MetaResult<bool> {
        let mut guard = self.progress_trackers.lock();
        let tracker = guard.inner.get_mut(&table_id).ok_or_else(|| {
            MetaError::from(anyhow!("Table tracker not found for table {}", table_id))
        })?;
        tracker.report_list_finished(actors.iter().copied());
        tracker.is_list_finished()
    }

    pub fn mark_load_stage_finished(
        &self,
        table_id: TableId,
        actors: &HashSet<ActorId>,
    ) -> MetaResult<bool> {
        let mut guard = self.progress_trackers.lock();
        let tracker = guard.inner.get_mut(&table_id).ok_or_else(|| {
            MetaError::from(anyhow!("Table tracker not found for table {}", table_id))
        })?;
        tracker.report_load_finished(actors.iter().copied());
        tracker.is_load_finished()
    }

    pub fn remove_trackers_by_database(&self, database_id: DatabaseId) {
        let mut guard = self.progress_trackers.lock();
        guard.remove_tracker_by_database_id(database_id);
    }

    pub fn notify_scheduler(&self) {
        self.scheduler_notify.notify_one();
    }

    async fn handle_scheduler_tick(self: &Arc<Self>) -> MetaResult<()> {
        let jobs = self.metadata_manager.list_refresh_jobs().await?;
        let active_table_ids = jobs.iter().map(|job| job.table_id).collect::<HashSet<_>>();
        self.refresh_job_metrics
            .lock()
            .retain(|table_id, _| active_table_ids.contains(table_id));
        for job in jobs {
            if let Err(err) = self.try_trigger_scheduled_refresh(&job).await {
                tracing::warn!(
                    table_id = %job.table_id,
                    error = %err.as_report(),
                    "failed to trigger scheduled refresh"
                );
            }
        }
        Ok(())
    }

    async fn sync_refreshable_jobs(&self) -> MetaResult<()> {
        let table_ids = self.metadata_manager.list_refreshable_table_ids().await?;
        for table_id in table_ids {
            self.metadata_manager.ensure_refresh_job(table_id).await?;
        }
        Ok(())
    }

    async fn try_trigger_scheduled_refresh(
        self: &Arc<Self>,
        job: &refresh_job::Model,
    ) -> MetaResult<()> {
        if job.current_status != RefreshState::Idle {
            self.refresh_job_metrics
                .lock()
                .entry(job.table_id)
                .or_insert_with(|| RefreshJobMetrics::new(job.table_id))
                .cron_miss_count
                .inc();
            tracing::warn!(table_id = %job.table_id, "skip scheduled refresh: current status is not idle: {:?}", job.current_status);
            return Ok(());
        }
        let Some(interval_secs) = job.trigger_interval_secs else {
            return Ok(());
        };
        if interval_secs <= 0 {
            return Ok(());
        }

        let interval = ChronoDuration::seconds(interval_secs);
        let last_run = if let Some(last_run) = job.last_trigger_time {
            last_run
        } else {
            self.metadata_manager
                .get_table_catalog_by_ids(&[job.table_id])
                .await?
                .first()
                .map(|t| {
                    Epoch(t.created_at_epoch())
                        .as_timestamptz()
                        .to_datetime_utc()
                        .timestamp_millis()
                })
                .unwrap()
        };
        let now = Utc::now().naive_utc();
        if now.signed_duration_since(
            DateTime::from_timestamp_millis(last_run)
                .unwrap()
                .naive_utc(),
        ) < interval
        {
            return Ok(());
        }

        let table = self
            .metadata_manager
            .catalog_controller
            .get_table_by_id(job.table_id)
            .await?;
        if !table.refreshable {
            return Ok(());
        }

        let Some(OptionalAssociatedSourceId::AssociatedSourceId(src_id)) =
            table.optional_associated_source_id
        else {
            tracing::warn!(
                table_id = %job.table_id,
                "skip scheduled refresh: missing associated source id"
            );
            return Ok(());
        };
        let associated_source_id = src_id;

        // Increment cron job trigger counter
        self.refresh_job_metrics
            .lock()
            .entry(job.table_id)
            .or_insert_with(|| RefreshJobMetrics::new(job.table_id))
            .cron_trigger_count
            .inc();
        tracing::info!(table_id = %job.table_id, "trigger scheduled refresh at interval {:?}", interval);

        self.ensure_refreshable(job.table_id, associated_source_id)
            .await?;
        self.execute_refresh(job.table_id, associated_source_id, &self.shared_actor_infos)
            .await?;
        Ok(())
    }

    async fn execute_refresh(
        self: &Arc<Self>,
        table_id: TableId,
        associated_source_id: SourceId,
        shared_actor_infos: &SharedActorInfos,
    ) -> MetaResult<()> {
        let trigger_time = Utc::now().naive_utc();
        let database_id = self
            .metadata_manager
            .catalog_controller
            .get_object_database_id(table_id)
            .await?;

        let job_fragments = self
            .metadata_manager
            .get_job_fragments_by_id(table_id.as_job_id())
            .await?;

        let mut tracker = SingleTableRefreshProgressTracker::new();
        {
            let fragment_info_guard = shared_actor_infos.read_guard();
            for (fragment_id, fragment) in &job_fragments.fragments {
                if fragment
                    .fragment_type_mask
                    .contains(FragmentTypeFlag::Source)
                    && !fragment.fragment_type_mask.contains(FragmentTypeFlag::Dml)
                {
                    let fragment_info = fragment_info_guard
                        .get_fragment(*fragment_id)
                        .ok_or_else(|| MetaError::fragment_not_found(*fragment_id))?;
                    tracker.expected_list_actors.extend(
                        fragment_info
                            .actors
                            .keys()
                            .map(|actor_id| *actor_id as ActorId),
                    );
                }

                if fragment
                    .fragment_type_mask
                    .contains(FragmentTypeFlag::FsFetch)
                    && let Some(fragment_info) = fragment_info_guard.get_fragment(*fragment_id)
                {
                    tracker.expected_fetch_actors.extend(
                        fragment_info
                            .actors
                            .keys()
                            .map(|actor_id| *actor_id as ActorId),
                    );
                }
            }
        }

        self.register_progress_tracker(table_id, database_id, tracker);

        self.metadata_manager
            .update_refresh_job_status(
                table_id,
                RefreshState::Refreshing,
                Some(trigger_time),
                false,
            )
            .await?;

        let refresh_command = Command::Refresh {
            table_id,
            associated_source_id,
        };

        let result = self
            .barrier_scheduler
            .run_command(database_id, refresh_command)
            .await;

        match result {
            Ok(_) => {
                tracing::info!(table_id = %table_id, "refresh command scheduled");
                Ok(())
            }
            Err(err) => {
                tracing::error!(
                    error = %err.as_report(),
                    table_id = %table_id,
                    "failed to execute refresh command"
                );
                self.metadata_manager
                    .update_refresh_job_status(table_id, RefreshState::Idle, None, false)
                    .await?;
                self.remove_progress_tracker(table_id, "failure");
                Err(anyhow!(err)
                    .context(format!("Failed to refresh table {}", table_id))
                    .into())
            }
        }
    }

    async fn ensure_refreshable(
        &self,
        table_id: TableId,
        associated_source_id: SourceId,
    ) -> MetaResult<()> {
        let table = self
            .metadata_manager
            .catalog_controller
            .get_table_by_id(table_id)
            .await?;

        if !table.refreshable {
            return Err(MetaError::invalid_parameter(format!(
                "Table '{}' is not refreshable. Only tables created with REFRESHABLE flag support refresh.",
                table.name
            )));
        }

        if table.optional_associated_source_id != Some(associated_source_id.into()) {
            return Err(MetaError::invalid_parameter(format!(
                "Table '{}' is not associated with source '{}'. table.optional_associated_source_id: {:?}",
                table.name, associated_source_id, table.optional_associated_source_id
            )));
        }

        let refresh_job_state = self
            .metadata_manager
            .catalog_controller
            .get_refresh_job_state_by_table_id(table_id)
            .await?;
        if refresh_job_state != RefreshState::Idle {
            return Err(MetaError::invalid_parameter(format!(
                "Table '{}' is not in idle state. Current state: {:?}",
                table.name, refresh_job_state
            )));
        }

        Ok(())
    }

    fn register_progress_tracker(
        &self,
        table_id: TableId,
        database_id: DatabaseId,
        tracker: SingleTableRefreshProgressTracker,
    ) {
        let mut guard = self.progress_trackers.lock();
        guard.inner.insert(table_id, tracker);
        guard
            .table_id_by_database_id
            .entry(database_id)
            .or_default()
            .insert(table_id);
    }

    pub fn remove_progress_tracker(&self, table_id: TableId, status: &str) {
        let elapsed = {
            let mut guard = self.progress_trackers.lock();
            let elapsed = guard
                .inner
                .remove(&table_id)
                .map(|entry| entry.start_time.elapsed().as_secs());
            guard.table_id_by_database_id.values_mut().for_each(|set| {
                set.remove(&table_id);
            });
            elapsed
        };
        if let Some(elapsed) = elapsed {
            let table_id_label = table_id.to_string();
            GLOBAL_META_METRICS
                .refresh_job_duration
                .with_guarded_label_values(&[&table_id_label, status])
                .set(elapsed);
            let mut metrics = self.refresh_job_metrics.lock();
            let metrics = metrics
                .entry(table_id)
                .or_insert_with(|| RefreshJobMetrics::new(table_id))
                .finished
                .entry(status.to_owned())
                .or_insert_with(|| RefreshFinishedMetrics::new(table_id, status));
            metrics.count.inc();
        }
    }
}

struct RefreshJobMetrics {
    cron_trigger_count: LabelGuardedIntCounter,
    cron_miss_count: LabelGuardedIntCounter,
    finished: HashMap<String, RefreshFinishedMetrics>,
}

impl RefreshJobMetrics {
    fn new(table_id: TableId) -> Self {
        let table_id = table_id.to_string();
        Self {
            cron_trigger_count: GLOBAL_META_METRICS
                .refresh_cron_job_trigger_cnt
                .with_guarded_label_values(&[&table_id]),
            cron_miss_count: GLOBAL_META_METRICS
                .refresh_cron_job_miss_cnt
                .with_guarded_label_values(&[&table_id]),
            finished: HashMap::new(),
        }
    }
}

struct RefreshFinishedMetrics {
    count: LabelGuardedIntCounter,
}

impl RefreshFinishedMetrics {
    fn new(table_id: TableId, status: &str) -> Self {
        let table_id = table_id.to_string();
        Self {
            count: GLOBAL_META_METRICS
                .refresh_job_finish_cnt
                .with_guarded_label_values(&[&table_id, status]),
        }
    }
}

#[derive(Default, Debug)]
pub struct GlobalRefreshTableProgressTracker {
    pub inner: HashMap<TableId, SingleTableRefreshProgressTracker>,
    pub table_id_by_database_id: HashMap<DatabaseId, HashSet<TableId>>,
}

impl GlobalRefreshTableProgressTracker {
    pub fn remove_tracker_by_database_id(&mut self, database_id: DatabaseId) {
        if let Some(table_ids) = self.table_id_by_database_id.remove(&database_id) {
            for table_id in table_ids {
                self.inner.remove(&table_id);
            }
        }
    }
}

#[derive(Debug)]
pub struct SingleTableRefreshProgressTracker {
    pub expected_list_actors: HashSet<ActorId>,
    pub expected_fetch_actors: HashSet<ActorId>,
    pub list_finished_actors: HashSet<ActorId>,
    pub fetch_finished_actors: HashSet<ActorId>,

    pub start_time: Instant,
}

impl SingleTableRefreshProgressTracker {
    pub fn new() -> Self {
        Self {
            expected_list_actors: HashSet::new(),
            expected_fetch_actors: HashSet::new(),
            list_finished_actors: HashSet::new(),
            fetch_finished_actors: HashSet::new(),
            start_time: Instant::now(),
        }
    }

    pub fn report_list_finished(&mut self, actor_ids: impl Iterator<Item = ActorId>) {
        self.list_finished_actors.extend(actor_ids);
    }

    pub fn is_list_finished(&self) -> MetaResult<bool> {
        if self.list_finished_actors.len() >= self.expected_list_actors.len() {
            if self.expected_list_actors == self.list_finished_actors {
                Ok(true)
            } else {
                Err(MetaError::from(anyhow!(
                    "list finished actors mismatch: expected: {:?}, actual: {:?}",
                    self.expected_list_actors,
                    self.list_finished_actors
                )))
            }
        } else {
            Ok(false)
        }
    }

    pub fn report_load_finished(&mut self, actor_ids: impl Iterator<Item = ActorId>) {
        self.fetch_finished_actors.extend(actor_ids);
    }

    pub fn is_load_finished(&self) -> MetaResult<bool> {
        if self.fetch_finished_actors.len() >= self.expected_fetch_actors.len() {
            if self.expected_fetch_actors == self.fetch_finished_actors {
                Ok(true)
            } else {
                Err(MetaError::from(anyhow!(
                    "fetch finished actors mismatch: expected: {:?}, actual: {:?}",
                    self.expected_fetch_actors,
                    self.fetch_finished_actors
                )))
            }
        } else {
            Ok(false)
        }
    }
}
