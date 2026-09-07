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

use std::cmp::{Ordering, max, min};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU32;

use anyhow::{Context, anyhow};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::id::JobId;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont;
use risingwave_connector::source::SplitImpl;
use risingwave_hummock_sdk::change_log::TableChangeLogs;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_meta_model::SinkId;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use sea_orm::TransactionTrait;
use thiserror_ext::AsReport;
use tracing::{info, warn};

use super::BarrierWorkerRuntimeInfoSnapshot;
use crate::MetaResult;
use crate::barrier::DatabaseRuntimeInfoSnapshot;
use crate::barrier::checkpoint::{
    BatchRefreshJobCheckpointControl, BatchRefreshLogicalFragments, BatchRefreshRenderResult,
};
use crate::barrier::context::{GlobalBarrierWorkerContext, GlobalBarrierWorkerContextImpl};
use crate::barrier::progress::TrackingJob;
use crate::barrier::rpc::{ControlStreamManager, to_partial_graph_id};
use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::controller::scale::{
    FragmentRenderMap, LoadedFragment, LoadedFragmentContext, RenderedGraph,
    render_actor_assignments,
};
use crate::controller::utils::StreamingJobExtraInfo;
use crate::manager::ActiveStreamingWorkerNodes;
use crate::model::{ActorId, FragmentDownstreamRelation, FragmentId, StreamActor};
use crate::rpc::ddl_controller::refill_upstream_sink_union_in_table;
use crate::stream::cdc::reload_cdc_table_snapshot_splits;
use crate::stream::{
    SourceChange, StreamFragmentGraph, UpstreamSinkInfo, cleanup_dropped_streaming_jobs,
};

#[derive(Debug)]
pub(crate) struct UpstreamSinkRecoveryInfo {
    target_fragment_id: FragmentId,
    upstream_infos: Vec<UpstreamSinkInfo>,
}

#[derive(Debug)]
pub struct LoadedRecoveryContext {
    pub fragment_context: LoadedFragmentContext,
    pub job_extra_info: HashMap<JobId, StreamingJobExtraInfo>,
    pub upstream_sink_recovery: HashMap<JobId, UpstreamSinkRecoveryInfo>,
    pub fragment_relations: FragmentDownstreamRelation,
}

impl LoadedRecoveryContext {
    fn empty(fragment_context: LoadedFragmentContext) -> Self {
        Self {
            fragment_context,
            job_extra_info: HashMap::new(),
            upstream_sink_recovery: HashMap::new(),
            fragment_relations: FragmentDownstreamRelation::default(),
        }
    }
}

pub struct RenderedDatabaseRuntimeInfo {
    pub job_infos: HashMap<JobId, HashMap<FragmentId, InflightFragmentInfo>>,
    pub stream_actors: HashMap<ActorId, StreamActor>,
    pub source_splits: HashMap<ActorId, Vec<SplitImpl>>,
    /// Batch refresh jobs rendered during `render_runtime_info`.
    pub batch_refresh: HashMap<JobId, BatchRefreshRenderResult>,
}

pub(in crate::barrier) fn render_runtime_info(
    actor_id_generator: &AtomicU32,
    worker_nodes: &ActiveStreamingWorkerNodes,
    control_stream_manager: &ControlStreamManager,
    recovery_context: &LoadedRecoveryContext,
    database_id: DatabaseId,
) -> MetaResult<Option<RenderedDatabaseRuntimeInfo>> {
    let Some(mut per_database_context) =
        recovery_context.fragment_context.for_database(database_id)
    else {
        return Ok(None);
    };

    assert!(!per_database_context.is_empty());

    // Extract batch refresh jobs before rendering via `render_actor_assignments`.
    // They will be rendered independently using the unified `render_actors_and_build_job_info`.
    let batch_refresh_job_ids: HashSet<JobId> = per_database_context
        .job_map
        .iter()
        .filter(|(_, model)| model.refresh_interval_sec.is_some())
        .map(|(job_id, _)| *job_id)
        .collect();

    let mut batch_refresh_logical = HashMap::new();
    if !batch_refresh_job_ids.is_empty() {
        let batch_refresh_fragment_ids: HashSet<FragmentId> = batch_refresh_job_ids
            .iter()
            .flat_map(|job_id| {
                per_database_context
                    .job_fragments
                    .get(job_id)
                    .unwrap()
                    .keys()
                    .copied()
            })
            .collect();

        for &job_id in &batch_refresh_job_ids {
            let fragments = per_database_context.job_fragments.remove(&job_id).unwrap();
            let downstreams = fragments
                .keys()
                .filter_map(|fid| {
                    recovery_context
                        .fragment_relations
                        .get(fid)
                        .map(|r| (*fid, r.clone()))
                })
                .collect();
            batch_refresh_logical.insert(
                job_id,
                BatchRefreshLogicalFragments {
                    fragments,
                    downstreams,
                },
            );
        }

        // Remove ensembles that only contain batch refresh fragments.
        per_database_context.ensembles.retain(|ensemble| {
            !ensemble
                .component_fragments()
                .all(|fid| batch_refresh_fragment_ids.contains(&fid))
        });
    }

    // Render actors for each batch refresh job.
    let mut batch_refresh = HashMap::new();
    for (job_id, logical) in batch_refresh_logical {
        let extra = recovery_context
            .job_extra_info
            .get(&job_id)
            .expect("should have extra info");
        let streaming_job_model = per_database_context
            .job_map
            .get(&job_id)
            .expect("should have streaming job model");
        let database_model = &per_database_context.database_map[&database_id];
        let partial_graph_id = to_partial_graph_id(database_id, Some(job_id));

        let render_result = BatchRefreshJobCheckpointControl::render_actors_and_build_job_info(
            &logical.fragments,
            &logical.downstreams,
            &extra.job_definition,
            actor_id_generator,
            worker_nodes.current(),
            control_stream_manager,
            &database_model.resource_group,
            streaming_job_model,
            partial_graph_id,
        )?;

        batch_refresh.insert(job_id, render_result);
    }

    // If all fragments were batch refresh, no normal rendering needed.
    if per_database_context.ensembles.is_empty() {
        return Ok(Some(RenderedDatabaseRuntimeInfo {
            job_infos: HashMap::new(),
            stream_actors: HashMap::new(),
            source_splits: HashMap::new(),
            batch_refresh,
        }));
    }

    let RenderedGraph { mut fragments, .. } = render_actor_assignments(
        actor_id_generator,
        worker_nodes.current(),
        &per_database_context,
    )?;

    let single_database = match fragments.remove(&database_id) {
        Some(info) => info,
        None => return Ok(None),
    };

    let mut database_map = HashMap::from([(database_id, single_database)]);
    recovery_table_with_upstream_sinks(
        &mut database_map,
        &recovery_context.upstream_sink_recovery,
    )?;
    let stream_actors = build_stream_actors(&database_map, &recovery_context.job_extra_info)?;

    let job_infos = database_map
        .remove(&database_id)
        .expect("database entry must exist");

    let mut source_splits = HashMap::new();
    for fragment_infos in job_infos.values() {
        for fragment in fragment_infos.values() {
            for (actor_id, info) in &fragment.actors {
                source_splits.insert(*actor_id, info.splits.clone());
            }
        }
    }

    Ok(Some(RenderedDatabaseRuntimeInfo {
        job_infos,
        stream_actors,
        source_splits,
        batch_refresh,
    }))
}

/// For normal DDL operations, the `UpstreamSinkUnion` operator is modified dynamically, and does not persist the
/// newly added or deleted upstreams in meta-store. Therefore, when restoring jobs, we need to restore the
/// information required by the operator based on the current state of the upstream (sink) and downstream (table) of
/// the operator. All necessary metadata must be preloaded before rendering.
fn recovery_table_with_upstream_sinks(
    inflight_jobs: &mut FragmentRenderMap,
    upstream_sink_recovery: &HashMap<JobId, UpstreamSinkRecoveryInfo>,
) -> MetaResult<()> {
    if upstream_sink_recovery.is_empty() {
        return Ok(());
    }

    let mut seen_jobs = HashSet::new();

    for jobs in inflight_jobs.values_mut() {
        for (job_id, fragments) in jobs {
            if !seen_jobs.insert(*job_id) {
                return Err(anyhow::anyhow!("Duplicate job id found: {}", job_id).into());
            }

            if let Some(recovery) = upstream_sink_recovery.get(job_id) {
                if let Some(target_fragment) = fragments.get_mut(&recovery.target_fragment_id) {
                    refill_upstream_sink_union_in_table(
                        &mut target_fragment.nodes,
                        &recovery.upstream_infos,
                    );
                } else {
                    return Err(anyhow::anyhow!(
                        "target fragment {} not found for upstream sink recovery of job {}",
                        recovery.target_fragment_id,
                        job_id
                    )
                    .into());
                }
            }
        }
    }

    Ok(())
}

/// Assembles `StreamActor` instances from rendered fragment info and job context.
///
/// This function combines the actor assignments from `FragmentRenderMap` with
/// runtime context (timezone, config, definition) from `StreamingJobExtraInfo`
/// to produce the final `StreamActor` structures needed for recovery.
fn build_stream_actors(
    all_info: &FragmentRenderMap,
    job_extra_info: &HashMap<JobId, StreamingJobExtraInfo>,
) -> MetaResult<HashMap<ActorId, StreamActor>> {
    let mut stream_actors = HashMap::new();

    for (job_id, streaming_info) in all_info.values().flatten() {
        let extra_info = job_extra_info
            .get(job_id)
            .cloned()
            .ok_or_else(|| anyhow!("no streaming job info for {}", job_id))?;
        let expr_context = extra_info.stream_context().to_expr_context();
        let job_definition = extra_info.job_definition;
        let config_override = extra_info.config_override;

        for (fragment_id, fragment_infos) in streaming_info {
            for (actor_id, InflightActorInfo { vnode_bitmap, .. }) in &fragment_infos.actors {
                stream_actors.insert(
                    *actor_id,
                    StreamActor {
                        actor_id: *actor_id,
                        fragment_id: *fragment_id,
                        vnode_bitmap: vnode_bitmap.clone(),
                        mview_definition: job_definition.clone(),
                        expr_context: Some(expr_context.clone()),
                        config_override: config_override.clone(),
                    },
                );
            }
        }
    }
    Ok(stream_actors)
}

impl GlobalBarrierWorkerContextImpl {
    fn resolve_job_committed_epoch(
        job_id: JobId,
        fragments: &HashMap<FragmentId, LoadedFragment>,
        state_table_committed_epochs: &HashMap<TableId, u64>,
    ) -> MetaResult<u64> {
        let mut table_id_iter = fragments
            .values()
            .flat_map(|fragment| fragment.state_table_ids.iter().copied());
        let Some(first_table_id) = table_id_iter.next() else {
            bail!("job {} has no state table", job_id);
        };
        let committed_epoch = *state_table_committed_epochs
            .get(&first_table_id)
            .ok_or_else(|| anyhow!("cannot get committed epoch on table {}.", first_table_id))?;
        for table_id in table_id_iter {
            let table_committed_epoch = *state_table_committed_epochs
                .get(&table_id)
                .ok_or_else(|| anyhow!("cannot get committed epoch on table {}.", table_id))?;
            if committed_epoch != table_committed_epoch {
                bail!(
                    "table {} has committed epoch {} different to other table {} with committed epoch {} in job {}",
                    first_table_id,
                    committed_epoch,
                    table_id,
                    table_committed_epoch,
                    job_id
                );
            }
        }

        Ok(committed_epoch)
    }

    async fn finish_completed_batch_refresh_background_jobs(
        &self,
        recovery_context: &LoadedRecoveryContext,
        state_table_committed_epochs: &HashMap<TableId, u64>,
        creating_jobs: &mut HashSet<JobId>,
    ) -> MetaResult<()> {
        let creating_job_ids = creating_jobs.iter().copied().collect_vec();
        for job_id in creating_job_ids {
            let Some(job) = recovery_context.fragment_context.job_map.get(&job_id) else {
                continue;
            };
            if job.refresh_interval_sec.is_none() {
                continue;
            }
            let Some(fragments) = recovery_context.fragment_context.job_fragments.get(&job_id)
            else {
                continue;
            };

            let committed_epoch =
                Self::resolve_job_committed_epoch(job_id, fragments, state_table_committed_epochs)?;
            let snapshot_backfill_info = StreamFragmentGraph::collect_snapshot_backfill_info_impl(
                fragments
                    .values()
                    .map(|fragment| (&fragment.nodes, fragment.fragment_type_mask)),
            )?
            .0
            .ok_or_else(|| anyhow!("batch refresh job {} has no snapshot backfill info", job_id))?;
            let snapshot_epoch = snapshot_backfill_info
                .upstream_mv_table_id_to_backfill_epoch
                .values()
                .find_map(|e| *e)
                .unwrap_or(committed_epoch);
            if committed_epoch < snapshot_epoch {
                continue;
            }

            info!(
                %job_id,
                committed_epoch,
                snapshot_epoch,
                "finish completed batch refresh background job during recovery"
            );
            self.finish_creating_job(TrackingJob::recovered_from_fragment_nodes(
                job_id,
                fragments
                    .iter()
                    .map(|(fragment_id, fragment)| (*fragment_id, &fragment.nodes)),
            ))
            .await?;
            creating_jobs.remove(&job_id);
        }

        Ok(())
    }

    async fn apply_pre_applied_drop_cancel(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<bool> {
        let drop_cancel = self.scheduled_barriers.pre_apply_drop_cancel(database_id);
        let has_drop_streaming_jobs = !drop_cancel.streaming_job_ids.is_empty();
        cleanup_dropped_streaming_jobs(
            &self.refresh_manager,
            &self.hummock_manager,
            &self.metadata_manager,
            drop_cancel.streaming_job_ids,
            drop_cancel.dropped_state_table_ids,
            "drop_streaming_jobs",
        )
        .await?;
        Ok(has_drop_streaming_jobs)
    }

    /// Clean catalogs for jobs in the initial state and creating jobs whose progress cannot be
    /// recovered.
    async fn clean_dirty_streaming_jobs(&self, database_id: Option<DatabaseId>) -> MetaResult<()> {
        self.metadata_manager
            .catalog_controller
            .clean_dirty_subscription(database_id)
            .await?;

        let cleaned_dirty_jobs = self
            .metadata_manager
            .catalog_controller
            .clean_dirty_creating_jobs(database_id)
            .await?;
        for sink_id in &cleaned_dirty_jobs.sink_ids {
            self.iceberg_compaction_manager
                .clear_iceberg_maintenance_by_sink_id(*sink_id);
        }
        if database_id.is_some() {
            // Per-database recovery does not run the global Hummock purge below. Unregister the
            // dirty jobs cleaned in this database through the normal dropped-table cleanup path.
            cleanup_dropped_streaming_jobs(
                &self.refresh_manager,
                &self.hummock_manager,
                &self.metadata_manager,
                cleaned_dirty_jobs.streaming_job_ids,
                cleaned_dirty_jobs.dropped_table_ids,
                "clean_dirty_creating_jobs",
            )
            .await?;
        }
        self.metadata_manager
            .reset_all_refresh_jobs_to_idle()
            .await?;

        // unregister cleaned sources.
        self.source_manager
            .apply_source_change(SourceChange::DropSource {
                dropped_source_ids: cleaned_dirty_jobs.source_ids,
            })
            .await;

        Ok(())
    }

    async fn reset_sink_coordinator(&self, database_id: Option<DatabaseId>) -> MetaResult<()> {
        if let Some(database_id) = database_id {
            let sink_ids = self
                .metadata_manager
                .catalog_controller
                .list_sink_ids(Some(database_id))
                .await?;
            self.sink_manager
                .stop_sink_coordinator(sink_ids.clone())
                .await;
            self.iceberg_pk_index_sink_manager
                .unregister_sinks(sink_ids);
        } else {
            self.sink_manager.reset().await;
            self.iceberg_pk_index_sink_manager.reset();
        }
        Ok(())
    }

    /// Re-register iceberg pk-index sink commit coordinators after recovery wipes them.
    async fn reregister_iceberg_pk_index_sinks(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<()> {
        let pb_sinks = self
            .metadata_manager
            .catalog_controller
            .list_sinks()
            .await?;
        let mut futs = FuturesUnordered::new();
        for pb_sink in pb_sinks {
            if database_id.is_some_and(|db_id| pb_sink.database_id != db_id)
                || !crate::manager::iceberg_pk_index_sink::is_iceberg_pk_index_sink(
                    &pb_sink.properties,
                )
            {
                continue;
            }
            let config = crate::manager::iceberg_pk_index_sink::build_iceberg_config(&pb_sink)
                .with_context(|| {
                    format!(
                        "build iceberg config while re-registering v3 sink {}",
                        pb_sink.id
                    )
                })?;
            let state_table_id = self
                .metadata_manager
                .catalog_controller
                .get_sink_state_table_ids(pb_sink.id)
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    anyhow!(
                        "no state table found while re-registering iceberg v3 sink {}",
                        pb_sink.id
                    )
                })?;
            let recovered_epoch = self
                .hummock_manager
                .on_current_version(|version| version.table_committed_epoch(state_table_id))
                .await
                .ok_or_else(|| {
                    anyhow!(
                        "cannot get committed epoch for iceberg v3 sink {} state table {}",
                        pb_sink.id,
                        state_table_id
                    )
                })?;
            let manager = &self.iceberg_pk_index_sink_manager;
            futs.push(async move {
                let partial_graph_id = to_partial_graph_id(pb_sink.database_id, None);
                let result = manager
                    .register_sink(pb_sink.id, partial_graph_id, config)
                    .await;
                if result.is_ok() {
                    manager.advance_committed_epochs([(partial_graph_id, recovered_epoch)]);
                }
                (pb_sink.id, result)
            });
        }

        while let Some((id, res)) = futs.next().await {
            if let Err(e) = res {
                let msg = format!("register iceberg v3 sink {} during recovery", id);
                return Err(e.context(msg).into());
            }
        }
        Ok(())
    }

    async fn abort_dirty_pending_sink_state(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<()> {
        let pending_sinks: HashSet<SinkId> = self
            .metadata_manager
            .catalog_controller
            .list_all_pending_sinks(database_id)
            .await?;

        if pending_sinks.is_empty() {
            return Ok(());
        }

        let sink_with_state_tables: HashMap<SinkId, Vec<TableId>> = self
            .metadata_manager
            .catalog_controller
            .fetch_sink_with_state_table_ids(pending_sinks)
            .await?;

        let mut sink_committed_epoch: HashMap<SinkId, u64> = HashMap::new();

        for (sink_id, table_ids) in sink_with_state_tables {
            let Some(table_id) = table_ids.first() else {
                return Err(anyhow!("no state table id in sink: {}", sink_id).into());
            };

            self.hummock_manager
                .on_current_version(|version| -> MetaResult<()> {
                    if let Some(committed_epoch) = version.table_committed_epoch(*table_id) {
                        assert!(
                            sink_committed_epoch
                                .insert(sink_id, committed_epoch)
                                .is_none()
                        );
                        Ok(())
                    } else {
                        Err(anyhow!("cannot get committed epoch on table {}.", table_id).into())
                    }
                })
                .await?;
        }

        self.metadata_manager
            .catalog_controller
            .abort_pending_sink_epochs(sink_committed_epoch)
            .await?;

        Ok(())
    }

    async fn purge_state_table_from_hummock(
        &self,
        all_state_table_ids: &HashSet<TableId>,
    ) -> MetaResult<()> {
        self.hummock_manager.purge(all_state_table_ids).await?;
        Ok(())
    }

    async fn list_creating_jobs(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<HashSet<JobId>> {
        let mgr = &self.metadata_manager;
        Ok(mgr
            .catalog_controller
            .list_creating_jobs(false, database_id)
            .await?
            .into_iter()
            .map(|(job_id, _, _, _, _)| job_id)
            .collect())
    }

    async fn load_recovery_context(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<LoadedRecoveryContext> {
        let inner = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let txn = inner.db.begin().await?;

        let fragment_context = self
            .metadata_manager
            .catalog_controller
            .load_fragment_context_in_txn(&txn, database_id)
            .await
            .inspect_err(|err| {
                warn!(error = %err.as_report(), "load fragment context failed");
            })?;

        if fragment_context.is_empty() {
            return Ok(LoadedRecoveryContext::empty(fragment_context));
        }

        let job_ids = fragment_context.job_map.keys().copied().collect_vec();
        let job_extra_info = self
            .metadata_manager
            .catalog_controller
            .get_streaming_job_extra_info_in_txn(&txn, job_ids)
            .await?;

        let mut upstream_targets = HashMap::new();
        for fragment in fragment_context
            .job_fragments
            .values()
            .flat_map(|fragments| fragments.values())
        {
            let mut has_upstream_union = false;
            visit_stream_node_cont(&fragment.nodes, |node| {
                if let Some(PbNodeBody::UpstreamSinkUnion(_)) = node.node_body {
                    has_upstream_union = true;
                    false
                } else {
                    true
                }
            });

            if has_upstream_union
                && let Some(previous) =
                    upstream_targets.insert(fragment.job_id, fragment.fragment_id)
            {
                bail!(
                    "multiple upstream sink union fragments found for job {}, fragment {}, kept {}",
                    fragment.job_id,
                    fragment.fragment_id,
                    previous
                );
            }
        }

        let mut upstream_sink_recovery = HashMap::new();
        if !upstream_targets.is_empty() {
            let tables = self
                .metadata_manager
                .catalog_controller
                .get_user_created_table_by_ids_in_txn(&txn, upstream_targets.keys().copied())
                .await?;

            for table in tables {
                let job_id = table.id.as_job_id();
                let Some(target_fragment_id) = upstream_targets.get(&job_id) else {
                    // This should not happen unless catalog changes or legacy metadata are involved.
                    tracing::debug!(
                        job_id = %job_id,
                        "upstream sink union target fragment not found for table"
                    );
                    continue;
                };

                let upstream_infos = self
                    .metadata_manager
                    .catalog_controller
                    .get_all_upstream_sink_infos_in_txn(&txn, &table, *target_fragment_id as _)
                    .await?;

                upstream_sink_recovery.insert(
                    job_id,
                    UpstreamSinkRecoveryInfo {
                        target_fragment_id: *target_fragment_id,
                        upstream_infos,
                    },
                );
            }
        }

        let fragment_relations = self
            .metadata_manager
            .catalog_controller
            .get_fragment_downstream_relations_in_txn(
                &txn,
                fragment_context
                    .job_fragments
                    .values()
                    .flat_map(|fragments| fragments.keys().copied())
                    .collect_vec(),
            )
            .await?;

        Ok(LoadedRecoveryContext {
            fragment_context,
            job_extra_info,
            upstream_sink_recovery,
            fragment_relations,
        })
    }

    #[expect(clippy::type_complexity)]
    fn resolve_hummock_version_epochs(
        creating_jobs: impl Iterator<Item = (JobId, &HashMap<FragmentId, LoadedFragment>)>,
        version: &HummockVersion,
        table_change_log: &TableChangeLogs,
    ) -> MetaResult<(
        HashMap<TableId, u64>,
        HashMap<TableId, Vec<(Vec<u64>, u64)>>,
    )> {
        let table_committed_epoch: HashMap<_, _> = version
            .state_table_info
            .info()
            .iter()
            .map(|(table_id, info)| (*table_id, info.committed_epoch))
            .collect();
        let get_table_committed_epoch = |table_id| -> anyhow::Result<u64> {
            Ok(*table_committed_epoch
                .get(&table_id)
                .ok_or_else(|| anyhow!("cannot get committed epoch on table {}.", table_id))?)
        };
        let mut min_downstream_committed_epochs = HashMap::new();
        for (job_id, fragments) in creating_jobs {
            let job_committed_epoch =
                Self::resolve_job_committed_epoch(job_id, fragments, &table_committed_epoch)?;
            if let (Some(snapshot_backfill_info), _) =
                StreamFragmentGraph::collect_snapshot_backfill_info_impl(
                    fragments
                        .values()
                        .map(|fragment| (&fragment.nodes, fragment.fragment_type_mask)),
                )?
            {
                for (upstream_table, snapshot_epoch) in
                    snapshot_backfill_info.upstream_mv_table_id_to_backfill_epoch
                {
                    let snapshot_epoch = snapshot_epoch.ok_or_else(|| {
                        anyhow!(
                            "recovered snapshot backfill job {} has not filled snapshot epoch to upstream {}",
                            job_id, upstream_table
                        )
                    })?;
                    let pinned_epoch = max(snapshot_epoch, job_committed_epoch);
                    match min_downstream_committed_epochs.entry(upstream_table) {
                        Entry::Occupied(entry) => {
                            let prev_min_epoch = entry.into_mut();
                            *prev_min_epoch = min(*prev_min_epoch, pinned_epoch);
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(pinned_epoch);
                        }
                    }
                }
            }
        }
        let mut log_epochs = HashMap::new();
        for (upstream_table_id, downstream_committed_epoch) in min_downstream_committed_epochs {
            let upstream_committed_epoch = get_table_committed_epoch(upstream_table_id)?;
            match upstream_committed_epoch.cmp(&downstream_committed_epoch) {
                Ordering::Less => {
                    bail!(
                        "downstream epoch {} later than upstream epoch {} of table {}",
                        downstream_committed_epoch,
                        upstream_committed_epoch,
                        upstream_table_id
                    );
                }
                Ordering::Equal => {
                    continue;
                }
                Ordering::Greater => {
                    if let Some(table_change_log) = table_change_log.get(&upstream_table_id) {
                        let epochs = table_change_log
                            .filter_epoch((downstream_committed_epoch, upstream_committed_epoch))
                            .map(|epoch_log| {
                                (
                                    epoch_log.non_checkpoint_epochs.clone(),
                                    epoch_log.checkpoint_epoch,
                                )
                            })
                            .collect_vec();
                        let first_epochs = epochs.first();
                        if let Some((_, first_checkpoint_epoch)) = &first_epochs
                            && *first_checkpoint_epoch == downstream_committed_epoch
                        {
                        } else {
                            bail!(
                                "resolved first log epoch {:?} on table {} not matched with downstream committed epoch {}",
                                epochs,
                                upstream_table_id,
                                downstream_committed_epoch
                            );
                        }
                        log_epochs
                            .try_insert(upstream_table_id, epochs)
                            .expect("non-duplicated");
                    } else {
                        bail!(
                            "upstream table {} on epoch {} has lagged downstream on epoch {} but no table change log",
                            upstream_table_id,
                            upstream_committed_epoch,
                            downstream_committed_epoch
                        );
                    }
                }
            }
        }
        Ok((table_committed_epoch, log_epochs))
    }

    pub(super) async fn reload_runtime_info_impl(
        &self,
    ) -> MetaResult<BarrierWorkerRuntimeInfoSnapshot> {
        {
            {
                {
                    self.clean_dirty_streaming_jobs(None)
                        .await
                        .context("clean dirty streaming jobs")?;

                    self.reset_sink_coordinator(None)
                        .await
                        .context("reset sink coordinator")?;
                    self.abort_dirty_pending_sink_state(None)
                        .await
                        .context("abort dirty pending sink state")?;

                    // We must abort dirty pending sink state before registering iceberg pk-index sinks,
                    // otherwise recover_pending will take speculative (epoch > committed epoch) pending sink state
                    // as valid and cause duplicated iceberg commit.
                    self.reregister_iceberg_pk_index_sinks(None)
                        .await
                        .context("re-register iceberg v3 sinks after recovery")?;

                    // Creating job progress needs to be recovered.
                    tracing::info!("recovering creating job progress");
                    let mut initial_creating_jobs = self
                        .list_creating_jobs(None)
                        .await
                        .context("recover creating job progress should not fail")?;

                    tracing::info!("recovered creating job progress");

                    // This is a quick path to accelerate the process of dropping and canceling streaming jobs.
                    let _ = self.apply_pre_applied_drop_cancel(None).await?;
                    self.metadata_manager
                        .catalog_controller
                        .cleanup_dropped_tables()
                        .await;

                    let active_streaming_nodes =
                        ActiveStreamingWorkerNodes::new_snapshot(self.metadata_manager.clone())
                            .await?;

                    let creating_streaming_jobs =
                        initial_creating_jobs.iter().cloned().collect_vec();

                    tracing::info!(
                        "creating streaming jobs: {:?} total {}",
                        creating_streaming_jobs,
                        creating_streaming_jobs.len()
                    );

                    let unreschedulable_jobs = {
                        let mut unreschedulable_jobs = HashSet::new();

                        for job_id in creating_streaming_jobs {
                            let scan_types = self
                                .metadata_manager
                                .get_job_backfill_scan_types(job_id)
                                .await?;

                            if scan_types
                                .values()
                                .any(|scan_type| !scan_type.is_reschedulable(false))
                            {
                                unreschedulable_jobs.insert(job_id);
                            }
                        }

                        unreschedulable_jobs
                    };

                    if !unreschedulable_jobs.is_empty() {
                        info!("unreschedulable creating jobs: {:?}", unreschedulable_jobs);
                    }

                    // Resolve actor info for recovery. If there's no actor to recover, most of the
                    // following steps will be no-op, while the compute nodes will still be reset.
                    // TODO(error-handling): attach context to the errors and log them together, instead of inspecting everywhere.
                    if !unreschedulable_jobs.is_empty() {
                        bail!(
                            "Recovery for unreschedulable creating jobs is not yet implemented. \
                             This path is triggered when the following jobs have at least one scan type that is not reschedulable: {:?}.",
                            unreschedulable_jobs
                        );
                    }

                    let mut recovery_context = self.load_recovery_context(None).await?;
                    if self.apply_pre_applied_drop_cancel(None).await? {
                        recovery_context = self.load_recovery_context(None).await?;
                    }

                    self.purge_state_table_from_hummock(
                        &recovery_context
                            .fragment_context
                            .job_fragments
                            .values()
                            .flat_map(|fragments| fragments.values())
                            .flat_map(|fragment| fragment.state_table_ids.iter().copied())
                            .collect(),
                    )
                    .await
                    .context("purge state table from hummock")?;

                    let (state_table_committed_epochs, state_table_log_epochs) = self
                        .hummock_manager
                        .on_current_version_and_table_change_log(|version, table_change_log| {
                            Self::resolve_hummock_version_epochs(
                                recovery_context
                                    .fragment_context
                                    .job_fragments
                                    .iter()
                                    .filter_map(|(job_id, job)| {
                                        initial_creating_jobs
                                            .contains(job_id)
                                            .then_some((*job_id, job))
                                    }),
                                version,
                                table_change_log,
                            )
                        })
                        .await?;

                    self.finish_completed_batch_refresh_background_jobs(
                        &recovery_context,
                        &state_table_committed_epochs,
                        &mut initial_creating_jobs,
                    )
                    .await?;

                    let mv_depended_subscriptions = self
                        .metadata_manager
                        .get_mv_depended_subscriptions(None)
                        .await?;

                    // Refresh creating job progress for the final snapshot to reflect any catalog changes.
                    let creating_jobs = {
                        let mut refreshed_creating_jobs = self
                            .list_creating_jobs(None)
                            .await
                            .context("recover creating job progress should not fail")?;
                        recovery_context
                            .fragment_context
                            .job_map
                            .keys()
                            .filter_map(|job_id| {
                                refreshed_creating_jobs.remove(job_id).then_some(*job_id)
                            })
                            .collect()
                    };

                    let database_infos = self
                        .metadata_manager
                        .catalog_controller
                        .list_databases()
                        .await?;

                    let cdc_table_snapshot_splits =
                        reload_cdc_table_snapshot_splits(&self.env.meta_store_ref().conn, None)
                            .await?;

                    Ok(BarrierWorkerRuntimeInfoSnapshot {
                        active_streaming_nodes,
                        recovery_context,
                        state_table_committed_epochs,
                        state_table_log_epochs,
                        mv_depended_subscriptions,
                        creating_jobs,
                        hummock_version_stats: self.hummock_manager.get_version_stats().await,
                        database_infos,
                        cdc_table_snapshot_splits,
                    })
                }
            }
        }
    }

    pub(super) async fn reload_database_runtime_info_impl(
        &self,
        database_id: DatabaseId,
    ) -> MetaResult<DatabaseRuntimeInfoSnapshot> {
        self.clean_dirty_streaming_jobs(Some(database_id))
            .await
            .context("clean dirty streaming jobs")?;

        self.reset_sink_coordinator(Some(database_id))
            .await
            .context("reset sink coordinator")?;
        self.abort_dirty_pending_sink_state(Some(database_id))
            .await
            .context("abort dirty pending sink state")?;
        self.reregister_iceberg_pk_index_sinks(Some(database_id))
            .await
            .context("re-register iceberg v3 sinks after recovery")?;

        // Creating job progress needs to be recovered.
        tracing::info!(?database_id, "recovering creating job progress of database");

        let mut creating_jobs = self
            .list_creating_jobs(Some(database_id))
            .await
            .context("recover creating job progress of database should not fail")?;
        tracing::info!(?database_id, "recovered creating job progress");

        // This is a quick path to accelerate the process of dropping and canceling streaming jobs.
        let _ = self
            .apply_pre_applied_drop_cancel(Some(database_id))
            .await?;

        let recovery_context = self.load_recovery_context(Some(database_id)).await?;

        let missing_creating_jobs = creating_jobs
            .iter()
            .filter(|job_id| {
                !recovery_context
                    .fragment_context
                    .job_map
                    .contains_key(*job_id)
            })
            .copied()
            .collect_vec();
        if !missing_creating_jobs.is_empty() {
            warn!(
                database_id = %database_id,
                missing_job_ids = ?missing_creating_jobs,
                "creating jobs missing in rendered info"
            );
        }

        let (state_table_committed_epochs, state_table_log_epochs) = self
            .hummock_manager
            .on_current_version_and_table_change_log(|version, table_change_log| {
                Self::resolve_hummock_version_epochs(
                    creating_jobs.iter().filter_map(|job_id| {
                        recovery_context
                            .fragment_context
                            .job_fragments
                            .get(job_id)
                            .map(|job| (*job_id, job))
                    }),
                    version,
                    table_change_log,
                )
            })
            .await?;

        self.finish_completed_batch_refresh_background_jobs(
            &recovery_context,
            &state_table_committed_epochs,
            &mut creating_jobs,
        )
        .await?;

        let mv_depended_subscriptions = self
            .metadata_manager
            .get_mv_depended_subscriptions(Some(database_id))
            .await?;

        let cdc_table_snapshot_splits =
            reload_cdc_table_snapshot_splits(&self.env.meta_store_ref().conn, Some(database_id))
                .await?;

        self.refresh_manager
            .remove_trackers_by_database(database_id);

        Ok(DatabaseRuntimeInfoSnapshot {
            recovery_context,
            state_table_committed_epochs,
            state_table_log_epochs,
            mv_depended_subscriptions,
            creating_jobs,
            cdc_table_snapshot_splits,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::FragmentTypeMask;
    use risingwave_common::id::WorkerId;
    use risingwave_meta_model::DispatcherType;
    use risingwave_meta_model::fragment::DistributionType;
    use risingwave_pb::stream_plan::stream_node::PbNodeBody;
    use risingwave_pb::stream_plan::{
        PbDispatchOutputMapping, PbStreamNode, UpstreamSinkUnionNode as PbUpstreamSinkUnionNode,
    };

    use super::*;
    use crate::controller::fragment::InflightActorInfo;
    use crate::model::DownstreamFragmentRelation;
    use crate::stream::UpstreamSinkInfo;

    #[test]
    fn test_recovery_table_with_upstream_sinks_updates_union_node() {
        let database_id = DatabaseId::new(1);
        let job_id = JobId::new(10);
        let fragment_id = FragmentId::new(100);
        let sink_fragment_id = FragmentId::new(200);

        let mut inflight_jobs: FragmentRenderMap = HashMap::new();
        let fragment = InflightFragmentInfo {
            fragment_id,
            distribution_type: DistributionType::Hash,
            fragment_type_mask: FragmentTypeMask::empty(),
            vnode_count: 1,
            nodes: PbStreamNode {
                node_body: Some(PbNodeBody::UpstreamSinkUnion(Box::new(
                    PbUpstreamSinkUnionNode {
                        init_upstreams: vec![],
                    },
                ))),
                ..Default::default()
            },
            actors: HashMap::new(),
            state_table_ids: HashSet::new(),
        };

        inflight_jobs
            .entry(database_id)
            .or_default()
            .entry(job_id)
            .or_default()
            .insert(fragment_id, fragment);

        let upstream_sink_recovery = HashMap::from([(
            job_id,
            UpstreamSinkRecoveryInfo {
                target_fragment_id: fragment_id,
                upstream_infos: vec![UpstreamSinkInfo {
                    sink_id: SinkId::new(1),
                    sink_fragment_id,
                    sink_output_fields: vec![],
                    sink_original_target_columns: vec![],
                    project_exprs: vec![],
                    new_sink_downstream: DownstreamFragmentRelation {
                        downstream_fragment_id: FragmentId::new(300),
                        dispatcher_type: DispatcherType::Hash,
                        dist_key_indices: vec![],
                        output_mapping: PbDispatchOutputMapping::default(),
                    },
                }],
            },
        )]);

        recovery_table_with_upstream_sinks(&mut inflight_jobs, &upstream_sink_recovery).unwrap();

        let updated = inflight_jobs
            .get(&database_id)
            .unwrap()
            .get(&job_id)
            .unwrap()
            .get(&fragment_id)
            .unwrap();

        let PbNodeBody::UpstreamSinkUnion(updated_union) =
            updated.nodes.node_body.as_ref().unwrap()
        else {
            panic!("expected upstream sink union node");
        };

        assert_eq!(updated_union.init_upstreams.len(), 1);
        assert_eq!(
            updated_union.init_upstreams[0].upstream_fragment_id,
            sink_fragment_id.as_raw_id()
        );
    }

    #[test]
    fn test_build_stream_actors_uses_preloaded_extra_info() {
        let database_id = DatabaseId::new(2);
        let job_id = JobId::new(20);
        let fragment_id = FragmentId::new(120);
        let actor_id = ActorId::new(500);

        let mut inflight_jobs: FragmentRenderMap = HashMap::new();
        inflight_jobs
            .entry(database_id)
            .or_default()
            .entry(job_id)
            .or_default()
            .insert(
                fragment_id,
                InflightFragmentInfo {
                    fragment_id,
                    distribution_type: DistributionType::Hash,
                    fragment_type_mask: FragmentTypeMask::empty(),
                    vnode_count: 1,
                    nodes: PbStreamNode::default(),
                    actors: HashMap::from([(
                        actor_id,
                        InflightActorInfo {
                            worker_id: WorkerId::new(1),
                            vnode_bitmap: None,
                            splits: vec![],
                        },
                    )]),
                    state_table_ids: HashSet::new(),
                },
            );

        let job_extra_info = HashMap::from([(
            job_id,
            StreamingJobExtraInfo {
                timezone: Some("UTC".to_owned()),
                config_override: "cfg".into(),
                job_definition: "definition".to_owned(),
                backfill_orders: None,
                refresh_interval_sec: None,
            },
        )]);

        let stream_actors = build_stream_actors(&inflight_jobs, &job_extra_info).unwrap();

        let actor = stream_actors.get(&actor_id).unwrap();
        assert_eq!(actor.actor_id, actor_id);
        assert_eq!(actor.fragment_id, fragment_id);
        assert_eq!(actor.mview_definition, "definition");
        assert_eq!(&*actor.config_override, "cfg");
        let expr_ctx = actor.expr_context.as_ref().unwrap();
        assert_eq!(expr_ctx.time_zone, "UTC");
    }
}
