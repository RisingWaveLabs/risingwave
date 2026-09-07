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

use std::collections::HashMap;
use std::future::{Future, pending};
use std::mem::replace;
use std::sync::Arc;

use anyhow::Context;
use futures::future::try_join_all;
use risingwave_common::id::JobId;
use risingwave_common::must_match;
use risingwave_common::util::deployment::Deployment;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::id::{DatabaseId, PartialGraphId};
use risingwave_pb::stream_service::barrier_complete_response::{
    PbIcebergPkIndexSinkMetadata, PbListFinishedSource, PbLoadFinishedSource,
};
use tokio::task::JoinHandle;

use crate::barrier::checkpoint::CheckpointControl;
use crate::barrier::context::GlobalBarrierWorkerContext;
use crate::barrier::info::BarrierInfo;
use crate::barrier::partial_graph::{PartialGraphBarrierInfo, PartialGraphManager};
use crate::barrier::progress::TrackingJob;
use crate::barrier::rpc::from_partial_graph_id;
use crate::barrier::schedule::PeriodicBarriers;
use crate::hummock::CommitEpochInfo;
use crate::manager::MetaSrvEnv;
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::{MetaError, MetaResult};

pub(super) enum CompletingTask {
    None,
    Completing {
        #[expect(clippy::type_complexity)]
        /// `database_id` -> (`Some(database_graph_committed_epoch)`, [(`creating_job_id`, `creating_job_committed_epoch`)])
        epochs_to_ack: HashMap<DatabaseId, (Option<u64>, Vec<(JobId, u64)>)>,

        // The join handle of a spawned task that completes the barrier.
        // The return value indicate whether there is some create streaming job command
        // that has finished but not checkpointed. If there is any, we will force checkpoint on the next barrier
        join_handle: JoinHandle<MetaResult<HummockVersionStats>>,
    },
    #[expect(dead_code)]
    Err(MetaError),
}

/// Only for checkpoint barrier. For normal barrier, there won't be a task.
#[derive(Default)]
pub(super) struct CompleteBarrierTask {
    pub(super) commit_info: CommitEpochInfo,
    pub(super) finished_jobs: Vec<TrackingJob>,
    pub(super) finished_cdc_table_backfill: Vec<JobId>,
    /// `partial_graph_id` -> barrier info for post-collect processing
    pub(super) epoch_infos: HashMap<PartialGraphId, PartialGraphBarrierInfo>,
    /// Source listing completion events that need `ListFinish` commands
    pub(super) list_finished_source_ids: Vec<PbListFinishedSource>,
    /// Source load completion events that need `LoadFinish` commands
    pub(super) load_finished_source_ids: Vec<PbLoadFinishedSource>,
    /// Table IDs that have finished materialize refresh and need completion signaling
    pub(super) refresh_finished_table_job_ids: Vec<JobId>,
    /// Iceberg pk-index sink reports collected during this barrier
    pub(super) iceberg_pk_index_sink_metadata: Vec<PbIcebergPkIndexSinkMetadata>,
}

impl CompleteBarrierTask {
    #[expect(clippy::type_complexity)]
    pub(super) fn epochs_to_ack(&self) -> HashMap<DatabaseId, (Option<u64>, Vec<(JobId, u64)>)> {
        let mut epochs_to_ack: HashMap<DatabaseId, (Option<u64>, Vec<(JobId, u64)>)> =
            HashMap::new();
        for (partial_graph_id, info) in &self.epoch_infos {
            let (database_id, creating_job_id) = from_partial_graph_id(*partial_graph_id);
            let epoch = info.barrier_info.prev_epoch();
            let (database, jobs) = epochs_to_ack.entry(database_id).or_default();
            if let Some(job_id) = creating_job_id {
                jobs.push((job_id, epoch));
            } else {
                *database = Some(epoch);
            }
        }
        epochs_to_ack
    }
}

impl CompleteBarrierTask {
    pub(super) async fn complete_barrier(
        self,
        context: &impl GlobalBarrierWorkerContext,
        env: MetaSrvEnv,
    ) -> MetaResult<HummockVersionStats> {
        let mut notifiers = Vec::new();
        let result: MetaResult<HummockVersionStats> = try {
            let wait_commit_timer = GLOBAL_META_METRICS
                .barrier_wait_commit_latency
                .start_timer();

            // Iceberg pk-index sink metadata reports are handled in three steps around hummock `commit_epoch`:
            //   1. pre_commit: persist pending rows under `pending_sink_state` (no iceberg I/O).
            //   2. commit_epoch: advance hummock.
            //   3. commit: drive iceberg overwrite_files for queued epochs.
            //   4. advance_committed_epochs: advance the per-partial-graph committed epoch cursor.
            let mut iceberg_pk_index_commit_sink_ids = Vec::new();
            if !self.iceberg_pk_index_sink_metadata.is_empty() {
                let res = context
                    .pre_commit_iceberg_pk_index_sink_metadata(self.iceberg_pk_index_sink_metadata)
                    .await?;
                iceberg_pk_index_commit_sink_ids = res;
            }

            let version_stats = context.commit_epoch(self.commit_info).await?;

            if !iceberg_pk_index_commit_sink_ids.is_empty() {
                context
                    .commit_iceberg_pk_index_sink_metadata(iceberg_pk_index_commit_sink_ids)
                    .await?;
            }
            let epochs = self
                .epoch_infos
                .iter()
                .map(|(id, info)| (*id, info.barrier_info.prev_epoch()));
            context.advance_iceberg_pk_index_sink_committed_epochs(epochs);

            // Handle list finished source IDs for refreshable batch sources
            // Spawn this asynchronously to avoid deadlock during barrier collection
            //
            // This step is for fs-like refreshable-batch sources, which need to list the data first finishing loading. It guarantees finishing listing before loading.
            // The other sources can skip this step.

            if !self.list_finished_source_ids.is_empty() {
                context
                    .handle_list_finished_source_ids(self.list_finished_source_ids.clone())
                    .await?;
            }

            // Handle load finished source IDs for refreshable batch sources
            // Spawn this asynchronously to avoid deadlock during barrier collection
            if !self.load_finished_source_ids.is_empty() {
                context
                    .handle_load_finished_source_ids(self.load_finished_source_ids.clone())
                    .await?;
            }

            // Handle refresh finished table IDs for materialized view refresh completion
            if !self.refresh_finished_table_job_ids.is_empty() {
                context
                    .handle_refresh_finished_table_ids(self.refresh_finished_table_job_ids.clone())
                    .await?;
            }

            for (partial_graph_id, info) in self.epoch_infos {
                let (database_id, job_id) = from_partial_graph_id(partial_graph_id);
                let command_name = info.post_collect_command.command_name().to_owned();
                let elapsed_secs = info.elapsed_secs();
                notifiers.extend(info.notifier);
                context
                    .post_collect_command(info.post_collect_command)
                    .await?;
                if job_id.is_none() {
                    Self::report_complete_event(
                        &env,
                        database_id,
                        elapsed_secs,
                        &info.barrier_info,
                        command_name,
                    );
                }
            }

            wait_commit_timer.observe_duration();
            version_stats
        };

        let version_stats = {
            let version_stats = match result {
                Ok(version_stats) => version_stats,
                Err(e) => {
                    for notifier in notifiers {
                        notifier.notify_collection_failed(e.clone());
                    }
                    return Err(e);
                }
            };
            notifiers.into_iter().for_each(|notifier| {
                notifier.notify_collected();
            });
            try_join_all(
                self.finished_jobs
                    .into_iter()
                    .map(|finished_job| context.finish_creating_job(finished_job)),
            )
            .await?;
            try_join_all(
                self.finished_cdc_table_backfill
                    .into_iter()
                    .map(|job_id| context.finish_cdc_table_backfill(job_id)),
            )
            .await?;
            version_stats
        };

        Ok(version_stats)
    }
}

impl CompleteBarrierTask {
    fn report_complete_event(
        env: &MetaSrvEnv,
        database_id: DatabaseId,
        duration_sec: f64,
        barrier_info: &BarrierInfo,
        command: String,
    ) {
        // Record barrier latency in event log.
        use risingwave_pb::meta::event_log;
        let event = event_log::EventBarrierComplete {
            prev_epoch: barrier_info.prev_epoch(),
            cur_epoch: barrier_info.curr_epoch(),
            duration_sec,
            command,
            barrier_kind: barrier_info.kind.as_str_name().to_owned(),
            database_id: database_id.as_raw_id(),
        };
        if cfg!(debug_assertions) || Deployment::current().is_ci() {
            // Add a warning log so that debug mode / CI can observe it
            if duration_sec > 5.0 {
                tracing::warn!(event = ?event,"high barrier latency observed!")
            }
        }
        env.event_log_manager_ref()
            .add_event_logs(vec![event_log::Event::BarrierComplete(event)]);
    }
}

pub(super) struct BarrierCompleteOutput {
    #[expect(clippy::type_complexity)]
    /// `database_id` -> (`Some(database_graph_committed_epoch)`, [(`creating_job_id`, `creating_job_committed_epoch`)])
    pub epochs_to_ack: HashMap<DatabaseId, (Option<u64>, Vec<(JobId, u64)>)>,
    pub hummock_version_stats: HummockVersionStats,
}

impl CompletingTask {
    pub(super) fn next_completed_barrier<'a>(
        &'a mut self,
        periodic_barriers: &mut PeriodicBarriers,
        checkpoint_control: &mut CheckpointControl,
        partial_graph_manager: &mut PartialGraphManager,
        context: &Arc<impl GlobalBarrierWorkerContext>,
        env: &MetaSrvEnv,
    ) -> impl Future<Output = MetaResult<BarrierCompleteOutput>> + 'a {
        // If there is no completing barrier, try to start completing the earliest barrier if
        // it has been collected.
        if let CompletingTask::None = self
            && let Some(task) = checkpoint_control
                .next_complete_barrier_task(periodic_barriers, partial_graph_manager)
        {
            {
                let epochs_to_ack = task.epochs_to_ack();
                let context = context.clone();
                let await_tree_reg = env.await_tree_reg().clone();
                let env = env.clone();

                let fut = async move { task.complete_barrier(&*context, env).await };
                let fut = await_tree_reg
                    .register_derived_root("Barrier Completion Task")
                    .instrument(fut);
                let join_handle = tokio::spawn(fut);

                *self = CompletingTask::Completing {
                    epochs_to_ack,
                    join_handle,
                };
            }
        }

        async move {
            if !matches!(self, CompletingTask::Completing { .. }) {
                return pending().await;
            };
            self.next_completed_barrier_inner().await
        }
    }

    #[await_tree::instrument]
    pub(super) async fn wait_completing_task(
        &mut self,
    ) -> MetaResult<Option<BarrierCompleteOutput>> {
        match self {
            CompletingTask::None => Ok(None),
            CompletingTask::Completing { .. } => {
                self.next_completed_barrier_inner().await.map(Some)
            }
            CompletingTask::Err(_) => {
                unreachable!("should not be called on previous err")
            }
        }
    }

    async fn next_completed_barrier_inner(&mut self) -> MetaResult<BarrierCompleteOutput> {
        let CompletingTask::Completing { join_handle, .. } = self else {
            unreachable!()
        };

        {
            {
                let join_result: MetaResult<_> = try {
                    join_handle
                        .await
                        .context("failed to join completing command")
                        .map_err(MetaError::from)??
                };
                // It's important to reset the completing_command after await no matter the result is err
                // or not, and otherwise the join handle will be polled again after ready.
                let next_completing_command_status = if let Err(e) = &join_result {
                    CompletingTask::Err(e.clone())
                } else {
                    CompletingTask::None
                };
                let completed_command = replace(self, next_completing_command_status);
                let hummock_version_stats = join_result?;

                must_match!(completed_command, CompletingTask::Completing {
                    epochs_to_ack,
                    ..
                } => {
                    Ok(BarrierCompleteOutput {
                        epochs_to_ack,
                        hummock_version_stats,
                    })
                })
            }
        }
    }
}
