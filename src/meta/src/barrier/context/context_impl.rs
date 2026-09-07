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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Context;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use risingwave_common::catalog::{DatabaseId, FragmentTypeFlag, TableId};
use risingwave_common::id::{JobId, PartialGraphId, SinkId};
use risingwave_hummock_sdk::change_log::TableChangeLogs;
use risingwave_meta_model::ActorId;
use risingwave_meta_model::streaming_job::BackfillOrders;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::id::SourceId;
use risingwave_pb::meta::PbTableRefillRuntimeConfig;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::stream_service::barrier_complete_response::{
    PbIcebergPkIndexSinkMetadata, PbListFinishedSource, PbLoadFinishedSource,
};
use risingwave_rpc_client::StreamingControlHandle;
use thiserror_ext::AsReport;

use crate::barrier::cdc_progress::CdcTableBackfillTracker;
use crate::barrier::checkpoint::independent_job::BatchRefreshJobTriggerContext;
use crate::barrier::command::{
    PostCollectCommand, ResumeBackfillTarget, SinceTimestampResolvedEpoch,
};
use crate::barrier::context::{GlobalBarrierWorkerContext, GlobalBarrierWorkerContextImpl};
use crate::barrier::progress::TrackingJob;
use crate::barrier::schedule::MarkReadyOptions;
use crate::barrier::{
    BarrierManagerStatus, BarrierWorkerRuntimeInfoSnapshot, BatchRefreshInfo, Command,
    CreateStreamingJobCommandInfo, CreateStreamingJobType, DatabaseRuntimeInfoSnapshot,
    RecoveryReason, ReplaceStreamJobPlan, Scheduled,
};
use crate::hummock::CommitEpochInfo;
use crate::manager::LocalNotification;
use crate::model::FragmentDownstreamRelation;
use crate::serving::{fetch_serving_infos, sync_serving_table_vnode_mappings_to_hummock};
use crate::stream::{SourceChange, cleanup_dropped_streaming_jobs};
use crate::{MetaError, MetaResult};

fn resolve_since_timestamp_log_store_epoch(
    table_id: TableId,
    since_epoch: u64,
    upstream_committed_epoch: u64,
    table_change_log: &TableChangeLogs,
) -> MetaResult<SinceTimestampResolvedEpoch> {
    let change_log = table_change_log.get(&table_id).ok_or_else(|| {
        anyhow::anyhow!(
            "no table changelog found for upstream table {} when resolving since_timestamp",
            table_id
        )
    })?;
    let Some(first_log) = change_log.first() else {
        return Err(anyhow::anyhow!(
            "empty table changelog found for upstream table {} when resolving since_timestamp",
            table_id
        )
        .into());
    };
    let first_checkpoint_epoch = first_log.checkpoint_epoch;
    if since_epoch < first_checkpoint_epoch {
        return Err(anyhow::anyhow!(
            "since_timestamp is earlier than the retained changelog of upstream table {}: requested epoch {}, first retained checkpoint epoch {}",
            table_id,
            since_epoch,
            first_checkpoint_epoch,
        )
        .into());
    }
    if since_epoch >= upstream_committed_epoch {
        return Err(anyhow::anyhow!(
            "since_timestamp is not before the committed epoch of upstream table {}: requested epoch {}, committed epoch {}",
            table_id,
            since_epoch,
            upstream_committed_epoch,
        )
        .into());
    }
    let latest_log = change_log.last().expect("checked non-empty");
    if upstream_committed_epoch != latest_log.checkpoint_epoch {
        return Err(anyhow::anyhow!(
            "upstream committed epoch {} does not match latest changelog epoch {} for upstream table {}",
            upstream_committed_epoch,
            latest_log.checkpoint_epoch,
            table_id,
        )
        .into());
    }

    // `binary_search_by_checkpoint_epoch` searches only by the checkpoint epoch
    // of each changelog entry. An entry may still cover earlier non-checkpoint
    // epochs, e.g. `{ non_checkpoint_epochs: [30, 35], checkpoint_epoch: 40 }`
    // covers `(20, 40]` if the previous checkpoint is 20. For `since_epoch =
    // 35`, the search returns `Err(index_of_40)`, and 40 is the snapshot epoch.
    // For `since_epoch = 40`, it returns `Ok(index_of_40)`. In both cases, the
    // resolved snapshot epoch is the least checkpoint epoch >= `since_epoch`.
    let snapshot_epoch_index = match change_log.binary_search_by_checkpoint_epoch(since_epoch) {
        Ok(index) => index,
        Err(index) => index,
    };
    let snapshot_epoch = change_log
        .get(snapshot_epoch_index)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "since_timestamp is later than the latest changelog of upstream table {}: requested epoch {}, latest changelog epoch {}",
                table_id,
                since_epoch,
                upstream_committed_epoch,
            )
        })?
        .checkpoint_epoch;
    // A request inside the latest changelog entry may be smaller than
    // `upstream_committed_epoch`, but still resolve to the latest checkpoint.
    // That end-of-log case is not a usable historical snapshot epoch.
    if snapshot_epoch >= upstream_committed_epoch {
        return Err(anyhow::anyhow!(
            "since_timestamp is too new for upstream table {}: requested epoch {}, resolved snapshot epoch {}, latest changelog epoch {}",
            table_id,
            since_epoch,
            snapshot_epoch,
            upstream_committed_epoch,
        )
        .into());
    }
    let epochs = change_log
        .range((snapshot_epoch_index + 1)..)
        .map(|epoch_log| {
            (
                epoch_log.non_checkpoint_epochs.clone(),
                epoch_log.checkpoint_epoch,
            )
        })
        .collect::<Vec<_>>();

    Ok((snapshot_epoch, epochs))
}

impl GlobalBarrierWorkerContext for GlobalBarrierWorkerContextImpl {
    #[await_tree::instrument]
    async fn commit_epoch(&self, commit_info: CommitEpochInfo) -> MetaResult<HummockVersionStats> {
        self.hummock_manager.commit_epoch(commit_info).await?;
        Ok(self.hummock_manager.get_version_stats().await)
    }

    #[await_tree::instrument("next_scheduled_barrier")]
    async fn next_scheduled(&self) -> Scheduled {
        self.scheduled_barriers.next_scheduled().await
    }

    fn abort_and_mark_blocked(
        &self,
        database_id: Option<DatabaseId>,
        recovery_reason: RecoveryReason,
    ) {
        if database_id.is_none() {
            self.set_status(BarrierManagerStatus::Recovering(recovery_reason));
        }

        // Mark blocked and abort buffered schedules, they might be dirty already.
        self.scheduled_barriers
            .abort_and_mark_blocked(database_id, "cluster is under recovering");
    }

    fn mark_ready(&self, options: MarkReadyOptions) {
        let is_global = matches!(&options, MarkReadyOptions::Global { .. });
        self.scheduled_barriers.mark_ready(options);
        if is_global {
            self.set_status(BarrierManagerStatus::Running);
        }
    }

    async fn resolve_log_store_epoch<'a>(
        &'a self,
        upstream_table_ids: impl Iterator<Item = TableId> + Send + 'a,
        since_epoch: u64,
    ) -> MetaResult<SinceTimestampResolvedEpoch> {
        let upstream_table_ids = upstream_table_ids.collect::<Vec<_>>();
        if upstream_table_ids.is_empty() {
            return Err(
                anyhow::anyhow!("since_timestamp requires at least one upstream table").into(),
            );
        }

        self.hummock_manager
            .on_current_version_and_table_change_log(|version, table_change_log| {
                let mut unified_log_epochs = None;
                for &upstream_table_id in &upstream_table_ids {
                    let upstream_committed_epoch = version
                        .state_table_info
                        .info()
                        .get(&upstream_table_id)
                        .map(|info| info.committed_epoch)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "cannot get committed epoch for upstream table {}",
                                upstream_table_id
                            )
                        })?;
                    let table_log_epochs = resolve_since_timestamp_log_store_epoch(
                        upstream_table_id,
                        since_epoch,
                        upstream_committed_epoch,
                        table_change_log,
                    )?;
                    if let Some(unified_log_epochs) = &unified_log_epochs {
                        if unified_log_epochs != &table_log_epochs {
                            return Err(anyhow::anyhow!(
                                "resolved since_timestamp log epochs for upstream table {} do not match previous upstream table log epochs: {:?} vs {:?}",
                                upstream_table_id,
                                table_log_epochs,
                                unified_log_epochs,
                            )
                            .into());
                        }
                    } else {
                        unified_log_epochs = Some(table_log_epochs);
                    }
                }
                unified_log_epochs.ok_or_else(|| {
                    anyhow::anyhow!("since_timestamp requires at least one upstream table").into()
                })
            })
            .await
    }

    async fn refresh_table_refill_runtime_state_after_recovery(&self) -> MetaResult<()> {
        let policies = self
            .metadata_manager
            .catalog_controller
            .table_cache_refill_policies_snapshot()
            .await?;
        let (serving_workers, fragment_serving_infos) =
            fetch_serving_infos(&self.metadata_manager).await?;

        self.env
            .notification_manager()
            .notify_hummock(
                Operation::Update,
                Info::TableRefillRuntimeConfig(PbTableRefillRuntimeConfig {
                    table_cache_refill_policies: Some(policies),
                    ..Default::default()
                }),
            )
            .await;
        sync_serving_table_vnode_mappings_to_hummock(
            &self.env.notification_manager_ref(),
            &self.serving_vnode_mapping,
            &serving_workers,
            &fragment_serving_infos,
        )
        .await;
        Ok(())
    }

    #[await_tree::instrument("post_collect_command({command})")]
    async fn post_collect_command(&self, command: PostCollectCommand) -> MetaResult<()> {
        Box::pin(command.post_collect(self)).await
    }

    async fn notify_creating_job_failed(&self, database_id: Option<DatabaseId>, err: String) {
        self.metadata_manager
            .notify_finish_failed(database_id, err)
            .await
    }

    #[await_tree::instrument("finish_creating_job({job})")]
    async fn finish_creating_job(&self, job: TrackingJob) -> MetaResult<()> {
        let job_id = job.job_id();
        job.finish(&self.metadata_manager, &self.source_manager)
            .await?;
        self.env
            .notification_manager()
            .notify_local_subscribers(LocalNotification::StreamingJobBackfillFinished(job_id));
        Ok(())
    }

    #[await_tree::instrument("finish_cdc_table_backfill({job})")]
    async fn finish_cdc_table_backfill(&self, job: JobId) -> MetaResult<()> {
        CdcTableBackfillTracker::mark_complete_job(&self.env.meta_store().conn, job).await
    }

    #[await_tree::instrument("new_control_stream({})", node.id)]
    async fn new_control_stream(&self, node: &WorkerNode) -> MetaResult<StreamingControlHandle> {
        self.new_control_stream_impl(node).await
    }

    async fn reload_runtime_info(&self) -> MetaResult<BarrierWorkerRuntimeInfoSnapshot> {
        self.reload_runtime_info_impl().await
    }

    async fn reload_database_runtime_info(
        &self,
        database_id: DatabaseId,
    ) -> MetaResult<DatabaseRuntimeInfoSnapshot> {
        self.reload_database_runtime_info_impl(database_id).await
    }

    async fn handle_list_finished_source_ids(
        &self,
        list_finished: Vec<PbListFinishedSource>,
    ) -> MetaResult<()> {
        let mut list_finished_info: HashMap<(TableId, SourceId), HashSet<ActorId>> = HashMap::new();

        for list_finished in list_finished {
            let table_id = list_finished.table_id;
            let associated_source_id = list_finished.associated_source_id;
            list_finished_info
                .entry((table_id, associated_source_id))
                .or_default()
                .insert(list_finished.reporter_actor_id);
        }

        for ((table_id, associated_source_id), actors) in list_finished_info {
            let allow_yield = self
                .refresh_manager
                .mark_list_stage_finished(table_id, &actors)?;

            if !allow_yield {
                continue;
            }

            let Some(database_id) = self
                .get_source_database_id_for_refresh_stage(table_id, associated_source_id, "list")
                .await?
            else {
                continue;
            };

            // Create ListFinish command
            let list_finish_command = Command::ListFinish {
                table_id,
                associated_source_id,
            };

            // Schedule the command through the barrier system without waiting
            self.barrier_scheduler
                .run_command_no_wait(database_id, list_finish_command)
                .context("Failed to schedule ListFinish command")?;

            tracing::info!(
                %table_id,
                %associated_source_id,
                "ListFinish command scheduled successfully"
            );
        }
        Ok(())
    }

    async fn handle_load_finished_source_ids(
        &self,
        load_finished: Vec<PbLoadFinishedSource>,
    ) -> MetaResult<()> {
        let mut load_finished_info: HashMap<(TableId, SourceId), HashSet<ActorId>> = HashMap::new();

        for load_finished in load_finished {
            let table_id = load_finished.table_id;
            let associated_source_id = load_finished.associated_source_id;
            load_finished_info
                .entry((table_id, associated_source_id))
                .or_default()
                .insert(load_finished.reporter_actor_id);
        }

        for ((table_id, associated_source_id), actors) in load_finished_info {
            let allow_yield = self
                .refresh_manager
                .mark_load_stage_finished(table_id, &actors)?;

            if !allow_yield {
                continue;
            }

            let Some(database_id) = self
                .get_source_database_id_for_refresh_stage(table_id, associated_source_id, "load")
                .await?
            else {
                continue;
            };

            // Create LoadFinish command
            let load_finish_command = Command::LoadFinish {
                table_id,
                associated_source_id,
            };

            // Schedule the command through the barrier system without waiting
            self.barrier_scheduler
                .run_command_no_wait(database_id, load_finish_command)
                .context("Failed to schedule LoadFinish command")?;

            tracing::info!(
                %table_id,
                %associated_source_id,
                "LoadFinish command scheduled successfully"
            );
        }

        Ok(())
    }

    async fn handle_refresh_finished_table_ids(
        &self,
        refresh_finished_table_job_ids: Vec<JobId>,
    ) -> MetaResult<()> {
        for job_id in refresh_finished_table_job_ids {
            let table_id = job_id.as_mv_table_id();

            self.refresh_manager.mark_refresh_complete(table_id).await?;
        }

        Ok(())
    }

    async fn load_batch_refresh_trigger_context(
        &self,
        job_id: JobId,
        database_id: DatabaseId,
        last_committed_epoch: u64,
    ) -> MetaResult<BatchRefreshJobTriggerContext> {
        self.load_batch_refresh_trigger_context_impl(job_id, database_id, last_committed_epoch)
            .await
    }

    #[await_tree::instrument]
    async fn pre_commit_iceberg_pk_index_sink_metadata(
        &self,
        reports: Vec<PbIcebergPkIndexSinkMetadata>,
    ) -> MetaResult<Vec<SinkId>> {
        let grouped = group_reports_by_sink(reports)?;
        let success_ids: Vec<SinkId> = grouped.keys().cloned().collect();
        let futs = FuturesUnordered::new();
        for (sink_id, (prev_epoch, reports)) in grouped {
            if reports.is_empty() {
                continue;
            }
            let manager = &self.iceberg_pk_index_sink_manager;
            futs.push(async move {
                (
                    sink_id,
                    manager.pre_commit_epoch(sink_id, prev_epoch, reports).await,
                )
            });
        }

        // Drain all futures regardless of individual failures, so that no coordinator is left with
        // state inconsistent vs. the caller's view.
        let results: Vec<(SinkId, anyhow::Result<()>)> = futs.collect().await;
        let errs: Vec<(SinkId, anyhow::Error)> = results
            .into_iter()
            .filter_map(|(id, r)| r.err().map(|e| (id, e)))
            .collect();
        if errs.is_empty() {
            Ok(success_ids)
        } else {
            Err(aggregate_sink_errors("pre-commit", errs).into())
        }
    }

    #[await_tree::instrument]
    async fn commit_iceberg_pk_index_sink_metadata(&self, sink_ids: Vec<SinkId>) -> MetaResult<()> {
        let futs = FuturesUnordered::new();
        for sink_id in sink_ids {
            let manager = &self.iceberg_pk_index_sink_manager;
            futs.push(async move { (sink_id, manager.commit_epoch(sink_id).await) });
        }

        let results: Vec<(SinkId, anyhow::Result<()>)> = futs.collect().await;
        let errs: Vec<(SinkId, anyhow::Error)> = results
            .into_iter()
            .filter_map(|(id, r)| r.err().map(|e| (id, e)))
            .collect();
        if errs.is_empty() {
            Ok(())
        } else {
            Err(aggregate_sink_errors("commit", errs).into())
        }
    }

    fn advance_iceberg_pk_index_sink_committed_epochs(
        &self,
        epochs: impl IntoIterator<Item = (PartialGraphId, u64)>,
    ) {
        self.iceberg_pk_index_sink_manager
            .advance_committed_epochs(epochs);
    }
}

/// Combine per-sink errors from a fan-out into a single `anyhow::Error`. The first failing
/// sink's error is used as the source so the original chain is preserved; the message lists
/// every failing `sink_id` and its error stringified.
fn aggregate_sink_errors(
    phase: &'static str,
    mut errs: Vec<(SinkId, anyhow::Error)>,
) -> anyhow::Error {
    debug_assert!(!errs.is_empty());
    let sink_ids: Vec<String> = errs.iter().map(|(id, _)| id.to_string()).collect();
    let details: Vec<String> = errs
        .iter()
        .map(|(id, e)| format!("sink {}: {}", id, e.as_report()))
        .collect();
    // Preserve the first error's chain as the cause.
    let (_, first_err) = errs.remove(0);
    first_err.context(format!(
        "iceberg v3 sink {} failed for sink_id(s) [{}]: {}",
        phase,
        sink_ids.join(", "),
        details.join("; ")
    ))
}

fn group_reports_by_sink(
    reports: Vec<PbIcebergPkIndexSinkMetadata>,
) -> MetaResult<HashMap<SinkId, (u64, Vec<PbIcebergPkIndexSinkMetadata>)>> {
    let mut grouped: HashMap<SinkId, (u64, Vec<PbIcebergPkIndexSinkMetadata>)> = HashMap::new();
    for r in reports {
        let sink_id = r.sink_id;
        let prev_epoch = r.prev_epoch;
        let entry = grouped.entry(sink_id).or_insert((prev_epoch, Vec::new()));
        if entry.0 != prev_epoch {
            return Err(anyhow::anyhow!(
                "iceberg v3 sink {} reports disagree on prev_epoch: {} vs {}",
                sink_id,
                entry.0,
                prev_epoch
            )
            .into());
        }
        entry.1.push(r);
    }
    Ok(grouped)
}

impl GlobalBarrierWorkerContextImpl {
    async fn get_source_database_id_for_refresh_stage(
        &self,
        table_id: TableId,
        associated_source_id: SourceId,
        stage: &'static str,
    ) -> MetaResult<Option<DatabaseId>> {
        match self
            .metadata_manager
            .catalog_controller
            .get_object_database_id(associated_source_id)
            .await
        {
            Ok(database_id) => Ok(Some(database_id)),
            Err(err) if err.is_catalog_id_not_found("object") => {
                tracing::warn!(
                    %table_id,
                    %associated_source_id,
                    stage,
                    "skip refresh finish command because associated source is already dropped"
                );
                Ok(None)
            }
            Err(err) => Err(err)
                .with_context(|| {
                    format!(
                        "failed to get database id for refresh stage: table_id={}, associated_source_id={}, stage={stage}",
                        table_id, associated_source_id
                    )
                })
                .map_err(Into::into),
        }
    }

    fn set_status(&self, new_status: BarrierManagerStatus) {
        self.status.store(Arc::new(new_status));
    }

    /// Load the context metadata and resolve upstream log epochs for a batch refresh trigger.
    async fn load_batch_refresh_trigger_context_impl(
        &self,
        job_id: JobId,
        database_id: DatabaseId,
        last_committed_epoch: u64,
    ) -> MetaResult<BatchRefreshJobTriggerContext> {
        use itertools::Itertools;
        use sea_orm::TransactionTrait;

        use crate::controller::scale::load_fragment_context_for_jobs;

        // Load metadata from the catalog under a single transaction.
        let inner = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let txn = inner.db.begin().await?;

        // 1. Load fragment context (job model, database model).
        let fragment_context =
            load_fragment_context_for_jobs(&txn, HashSet::from([job_id])).await?;

        let streaming_job_model = fragment_context
            .job_map
            .get(&job_id)
            .ok_or_else(|| anyhow::anyhow!("streaming job model not found for job {}", job_id))?
            .clone();

        let database_model = fragment_context
            .database_map
            .get(&database_id)
            .ok_or_else(|| {
                anyhow::anyhow!("database model not found for database {}", database_id)
            })?;
        let database_resource_group = database_model.resource_group.clone();

        // 2. Load job definition.
        let mut job_extra_info = self
            .metadata_manager
            .catalog_controller
            .get_streaming_job_extra_info_in_txn(&txn, vec![job_id])
            .await?;
        let definition = job_extra_info
            .remove(&job_id)
            .ok_or_else(|| anyhow::anyhow!("extra info not found for job {}", job_id))?
            .job_definition;

        // 3. Get fragments from fragment_context and load downstream relations.
        let fragments = fragment_context
            .job_fragments
            .get(&job_id)
            .ok_or_else(|| anyhow::anyhow!("fragments not found for job {}", job_id))?
            .clone();

        // Derive upstream table IDs from the snapshot backfill scan nodes in the fragments.
        let upstream_table_ids: HashSet<TableId> = {
            use crate::stream::StreamFragmentGraph;
            let snapshot_backfill_info = StreamFragmentGraph::collect_snapshot_backfill_info_impl(
                fragments.values().map(|f| (&f.nodes, f.fragment_type_mask)),
            )?
            .0
            .ok_or_else(|| {
                anyhow::anyhow!("batch refresh job {} has no snapshot backfill info", job_id)
            })?;
            snapshot_backfill_info
                .upstream_mv_table_id_to_backfill_epoch
                .into_keys()
                .collect()
        };

        let fragment_ids: Vec<_> = fragments.keys().copied().collect();
        let downstreams = self
            .metadata_manager
            .catalog_controller
            .get_fragment_downstream_relations_in_txn(&txn, fragment_ids)
            .await?;

        txn.commit().await?;
        drop(inner);

        // Resolve upstream log epochs from the hummock changelog.
        let (upstream_table_log_epochs, target_upstream_epoch) = self
            .hummock_manager
            .on_current_version_and_table_change_log(|version, table_change_log| {
                let mut target_upstream_epoch = last_committed_epoch;
                let mut log_epochs: HashMap<TableId, Vec<(Vec<u64>, u64)>> = HashMap::new();

                for &upstream_table_id in &upstream_table_ids {
                    let upstream_committed_epoch = version
                        .state_table_info
                        .info()
                        .get(&upstream_table_id)
                        .map(|info| info.committed_epoch)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "cannot get committed epoch for upstream table {}",
                                upstream_table_id
                            )
                        })?;

                    target_upstream_epoch =
                        std::cmp::max(target_upstream_epoch, upstream_committed_epoch);

                    if upstream_committed_epoch <= last_committed_epoch {
                        continue;
                    }

                    if let Some(change_log) = table_change_log.get(&upstream_table_id) {
                        let epochs = change_log
                            .filter_epoch((last_committed_epoch, upstream_committed_epoch))
                            .map(|epoch_log| {
                                (
                                    epoch_log.non_checkpoint_epochs.clone(),
                                    epoch_log.checkpoint_epoch,
                                )
                            })
                            .collect_vec();
                        if !epochs.is_empty() {
                            log_epochs.insert(upstream_table_id, epochs);
                        }
                    } else {
                        anyhow::bail!(
                            "upstream table {} has lagged downstream on epoch {} but no table change log (upstream committed: {})",
                            upstream_table_id,
                            last_committed_epoch,
                            upstream_committed_epoch,
                        );
                    }
                }

                Ok((log_epochs, target_upstream_epoch))
            })
            .await?;

        Ok(BatchRefreshJobTriggerContext {
            fragments,
            downstreams,
            streaming_job_model,
            definition,
            database_resource_group,
            upstream_table_log_epochs,
            target_upstream_epoch,
        })
    }
}

impl PostCollectCommand {
    /// Do some stuffs after barriers are collected and the new storage version is committed, for
    /// the given command.
    pub async fn post_collect(
        self,
        barrier_manager_context: &GlobalBarrierWorkerContextImpl,
    ) -> MetaResult<()> {
        match self {
            PostCollectCommand::Command(_) => {}
            PostCollectCommand::SourceChangeSplit {
                split_assignment: assignment,
            } => {
                barrier_manager_context
                    .metadata_manager
                    .update_fragment_splits(&assignment)
                    .await?;
            }

            PostCollectCommand::DropStreamingJobs => {}
            PostCollectCommand::ConnectorPropsChange(obj_id_map_props) => {
                // todo: we dont know the type of the object id, it can be a source or a sink. Should carry more info in the barrier command.
                barrier_manager_context
                    .source_manager
                    .apply_source_change(SourceChange::UpdateSourceProps {
                        // Only sources are managed in source manager. Convert object IDs to source IDs and let
                        // source manager ignore unknown/unregistered sources.
                        source_id_map_new_props: obj_id_map_props
                            .iter()
                            .map(|(object_id, props)| (object_id.as_source_id(), props.clone()))
                            .collect(),
                    })
                    .await;
            }
            PostCollectCommand::ResumeBackfill { target } => match target {
                ResumeBackfillTarget::Job(job_id) => {
                    barrier_manager_context
                        .metadata_manager
                        .catalog_controller
                        .update_backfill_orders_by_job_id(job_id, None)
                        .await?;
                }
                ResumeBackfillTarget::Fragment(fragment_id) => {
                    let mut job_ids = barrier_manager_context
                        .metadata_manager
                        .catalog_controller
                        .get_fragment_job_id(vec![fragment_id])
                        .await?;
                    let job_id = job_ids.pop().ok_or_else(|| {
                        MetaError::invalid_parameter("fragment not found".to_owned())
                    })?;
                    let job_id = JobId::new(job_id.as_raw_id());

                    let extra_info = barrier_manager_context
                        .metadata_manager
                        .catalog_controller
                        .get_streaming_job_extra_info(vec![job_id])
                        .await?;
                    let mut backfill_orders: BackfillOrders = extra_info
                        .get(&job_id)
                        .cloned()
                        .ok_or_else(|| MetaError::invalid_parameter("job not found".to_owned()))?
                        .backfill_orders
                        .unwrap_or_default();

                    let resumed_fragment_id = fragment_id.as_raw_id();
                    for children in backfill_orders.0.values_mut() {
                        children.retain(|child| *child != resumed_fragment_id);
                    }
                    backfill_orders.0.retain(|_, children| !children.is_empty());

                    barrier_manager_context
                        .metadata_manager
                        .catalog_controller
                        .update_backfill_orders_by_job_id(job_id, Some(backfill_orders))
                        .await?;
                }
            },
            PostCollectCommand::CreateStreamingJob {
                info,
                job_type,
                cross_db_snapshot_backfill_info,
                resolved_split_assignment,
            } => {
                match &job_type {
                    CreateStreamingJobType::SinkIntoTable(_) | CreateStreamingJobType::Normal => {
                        barrier_manager_context
                            .metadata_manager
                            .catalog_controller
                            .fill_snapshot_backfill_epoch(
                                info.stream_job_fragments.fragments.iter().filter_map(
                                    |(fragment_id, fragment)| {
                                        if fragment.fragment_type_mask.contains(
                                            FragmentTypeFlag::CrossDbSnapshotBackfillStreamScan,
                                        ) {
                                            Some(*fragment_id as _)
                                        } else {
                                            None
                                        }
                                    },
                                ),
                                None,
                                &cross_db_snapshot_backfill_info,
                            )
                            .await?
                    }
                    CreateStreamingJobType::SnapshotBackfill {
                        snapshot_backfill_info,
                        ..
                    }
                    | CreateStreamingJobType::BatchRefresh(BatchRefreshInfo {
                        snapshot_backfill_info,
                        ..
                    }) => {
                        barrier_manager_context
                            .metadata_manager
                            .catalog_controller
                            .fill_snapshot_backfill_epoch(
                                info.stream_job_fragments.fragments.iter().filter_map(
                                    |(fragment_id, fragment)| {
                                        if fragment.fragment_type_mask.contains_any([
                                            FragmentTypeFlag::SnapshotBackfillStreamScan,
                                            FragmentTypeFlag::CrossDbSnapshotBackfillStreamScan,
                                        ]) {
                                            Some(*fragment_id as _)
                                        } else {
                                            None
                                        }
                                    },
                                ),
                                Some(snapshot_backfill_info),
                                &cross_db_snapshot_backfill_info,
                            )
                            .await?
                    }
                }

                // Do `post_collect_job_fragments` of the original streaming job in the end, so that in any previous failure,
                // we won't mark the job as `Creating`, and then the job will be later clean by the recovery triggered by the returned error.
                let CreateStreamingJobCommandInfo {
                    stream_job_fragments,
                    upstream_fragment_downstreams,
                    replace_sink,
                    ..
                } = info;
                let new_job_id = stream_job_fragments.stream_job_id();
                let new_sink_downstream =
                    if let CreateStreamingJobType::SinkIntoTable(ctx) = job_type {
                        let new_downstreams = ctx.new_sink_downstream.clone();
                        let new_downstreams = FragmentDownstreamRelation::from([(
                            ctx.sink_fragment_id,
                            vec![new_downstreams],
                        )]);
                        Some(new_downstreams)
                    } else {
                        None
                    };

                let old_state_table_ids = barrier_manager_context
                    .metadata_manager
                    .catalog_controller
                    .post_collect_job_fragments(
                        new_job_id,
                        &upstream_fragment_downstreams,
                        new_sink_downstream,
                        Some(&resolved_split_assignment),
                        replace_sink.as_ref(),
                        replace_sink.is_none(),
                    )
                    .await?;

                let added_source_fragments = stream_job_fragments.stream_source_fragments();
                let added_backfill_fragments = stream_job_fragments.source_backfill_fragments();
                // Skip the source-manager notification (and its `core` lock) when the job has no source/backfill fragments.
                if !added_source_fragments.is_empty() || !added_backfill_fragments.is_empty() {
                    barrier_manager_context
                        .source_manager
                        .apply_source_change(SourceChange::CreateJob {
                            added_source_fragments,
                            added_backfill_fragments,
                        })
                        .await;
                }

                if let Some(old_sink_id) = replace_sink {
                    barrier_manager_context
                        .sink_manager
                        .stop_sink_coordinator(vec![old_sink_id])
                        .await;
                    cleanup_dropped_streaming_jobs(
                        &barrier_manager_context.refresh_manager,
                        &barrier_manager_context.hummock_manager,
                        &barrier_manager_context.metadata_manager,
                        [old_sink_id.as_job_id()],
                        old_state_table_ids.expect("replace sink should return old state tables"),
                        "replace_sink",
                    )
                    .await?;
                }
            }
            PostCollectCommand::Reschedule { reschedules, .. } => {
                let fragment_splits = reschedules
                    .iter()
                    .map(|(fragment_id, reschedule)| {
                        (*fragment_id, reschedule.actor_splits.clone())
                    })
                    .collect();

                barrier_manager_context
                    .metadata_manager
                    .update_fragment_splits(&fragment_splits)
                    .await?;
            }

            PostCollectCommand::ReplaceStreamJob {
                plan: replace_plan,
                resolved_split_assignment,
            } => {
                let ReplaceStreamJobPlan {
                    old_fragments,
                    new_fragments,
                    upstream_fragment_downstreams,
                    to_drop_state_table_ids,
                    auto_refresh_schema_sinks,
                    ..
                } = &replace_plan;
                // Update actors and actor_dispatchers for new table fragments.
                barrier_manager_context
                    .metadata_manager
                    .catalog_controller
                    .post_collect_job_fragments(
                        new_fragments.stream_job_id,
                        upstream_fragment_downstreams,
                        None,
                        Some(&resolved_split_assignment),
                        None,
                        false,
                    )
                    .await?;

                if let Some(sinks) = auto_refresh_schema_sinks {
                    for sink in sinks {
                        barrier_manager_context
                            .metadata_manager
                            .catalog_controller
                            .post_collect_job_fragments(
                                sink.tmp_sink_id.as_job_id(),
                                &Default::default(), // upstream_fragment_downstreams is already inserted in the job of upstream table
                                None, // no replace plan
                                None, // no init split assignment
                                None,
                                false,
                            )
                            .await?;
                    }
                }

                // Apply the split changes in source manager.
                barrier_manager_context
                    .source_manager
                    .handle_replace_job(
                        old_fragments,
                        new_fragments.stream_source_fragments(),
                        &replace_plan,
                    )
                    .await;
                cleanup_dropped_streaming_jobs(
                    &barrier_manager_context.refresh_manager,
                    &barrier_manager_context.hummock_manager,
                    &barrier_manager_context.metadata_manager,
                    [],
                    to_drop_state_table_ids.clone(),
                    "replace_streaming_job",
                )
                .await?;
            }

            PostCollectCommand::CreateSubscription { subscription_id } => {
                barrier_manager_context
                    .metadata_manager
                    .catalog_controller
                    .finish_create_subscription_catalog(subscription_id)
                    .await?
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_hummock_sdk::change_log::{EpochNewChangeLog, TableChangeLog};

    use super::*;

    fn test_change_logs(table_id: TableId) -> TableChangeLogs {
        TableChangeLogs::from_iter([(
            table_id,
            TableChangeLog::new([
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    non_checkpoint_epochs: vec![10],
                    checkpoint_epoch: 20,
                },
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    non_checkpoint_epochs: vec![30],
                    checkpoint_epoch: 40,
                },
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    non_checkpoint_epochs: vec![50],
                    checkpoint_epoch: 60,
                },
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    non_checkpoint_epochs: vec![],
                    checkpoint_epoch: 80,
                },
            ]),
        )])
    }

    #[test]
    fn test_resolve_since_timestamp_log_store_epoch() {
        let table_id = TableId::new(233);
        let change_logs = test_change_logs(table_id);

        let resolved =
            resolve_since_timestamp_log_store_epoch(table_id, 45, 80, &change_logs).unwrap();

        assert_eq!(resolved, (60, vec![(vec![], 80)]));
    }

    #[test]
    fn test_resolve_since_timestamp_log_store_epoch_rejects_invalid_range() {
        let table_id = TableId::new(233);
        let change_logs = test_change_logs(table_id);

        let err = resolve_since_timestamp_log_store_epoch(table_id, 9, 80, &change_logs)
            .unwrap_err()
            .to_string();
        assert!(err.contains("earlier than the retained changelog"));

        let err = resolve_since_timestamp_log_store_epoch(table_id, 80, 80, &change_logs)
            .unwrap_err()
            .to_string();
        assert!(err.contains("is not before the committed epoch"));

        let err = resolve_since_timestamp_log_store_epoch(table_id, 45, 60, &change_logs)
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not match latest changelog epoch"));
    }

    #[test]
    fn test_resolve_since_timestamp_log_store_epoch_rejects_empty_or_missing_log() {
        let table_id = TableId::new(233);
        let empty_change_logs = TableChangeLogs::from_iter([(table_id, TableChangeLog::new([]))]);

        let err = resolve_since_timestamp_log_store_epoch(table_id, 30, 60, &empty_change_logs)
            .unwrap_err()
            .to_string();
        assert!(err.contains("empty table changelog"));

        let err = resolve_since_timestamp_log_store_epoch(table_id, 30, 60, &Default::default())
            .unwrap_err()
            .to_string();
        assert!(err.contains("no table changelog"));
    }

    #[test]
    fn test_resolve_since_timestamp_log_store_epoch_keeps_latest_non_checkpoint_epochs() {
        let table_id = TableId::new(233);
        let change_logs = TableChangeLogs::from_iter([(
            table_id,
            TableChangeLog::new([
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    non_checkpoint_epochs: vec![],
                    checkpoint_epoch: 20,
                },
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    non_checkpoint_epochs: vec![30],
                    checkpoint_epoch: 40,
                },
            ]),
        )]);

        let resolved =
            resolve_since_timestamp_log_store_epoch(table_id, 20, 40, &change_logs).unwrap();
        assert_eq!(resolved, (20, vec![(vec![30], 40)]));

        let err = resolve_since_timestamp_log_store_epoch(table_id, 30, 40, &change_logs)
            .unwrap_err()
            .to_string();
        assert!(err.contains("since_timestamp is too new"));
    }

    #[test]
    fn test_skip_refresh_finish_when_associated_source_missing() {
        let err = MetaError::catalog_id_not_found("object", 42);
        assert!(err.is_catalog_id_not_found("object"));
    }

    #[test]
    fn test_do_not_skip_refresh_finish_for_other_not_found_types() {
        let err = MetaError::catalog_id_not_found("table", 42);
        assert!(!err.is_catalog_id_not_found("object"));
    }
}
