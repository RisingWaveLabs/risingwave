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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Debug, Formatter};

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::catalog::{DatabaseId, TableId, TableOption};
use risingwave_common::id::JobId;
use risingwave_connector::WithOptionsSecResolved;
use risingwave_meta_model::refresh_job::{self, RefreshState};
use risingwave_meta_model::{SinkId, SourceId, WorkerId};
use risingwave_pb::catalog::{PbSource, PbTable};
use risingwave_pb::common::worker_node::{PbResource, Property as AddNodeProperty, State};
use risingwave_pb::common::{HostAddress, PbWorkerNode, PbWorkerType, WorkerNode, WorkerType};
use risingwave_pb::meta::alter_connector_props_request::PbExtraOptions;
use risingwave_pb::meta::list_rate_limits_response::RateLimitInfo;
use risingwave_pb::secret::PbSecretRef;
use risingwave_pb::stream_plan::{PbDispatcherType, PbStreamNode, PbStreamScanType};
use sea_orm::TransactionTrait;
use sea_orm::prelude::DateTime;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tokio::sync::oneshot;
use tracing::warn;

use crate::MetaResult;
use crate::controller::catalog::CatalogControllerRef;
use crate::controller::cluster::{ClusterControllerRef, StreamingClusterInfo, WorkerExtraInfo};
use crate::controller::scale::find_fragment_no_shuffle_dags_detailed;
use crate::manager::{LocalNotification, NotificationVersion};
use crate::model::{ActorId, ClusterId, Fragment, FragmentId, StreamJobFragments, SubscriptionId};
use crate::stream::SplitAssignment;
use crate::telemetry::MetaTelemetryJobDesc;

#[derive(Clone)]
pub struct MetadataManager {
    pub cluster_controller: ClusterControllerRef,
    pub catalog_controller: CatalogControllerRef,
}

#[derive(Debug)]
pub(crate) enum ActiveStreamingWorkerChange {
    Add(WorkerNode),
    Remove(WorkerNode),
    Update(WorkerNode),
}

pub struct ActiveStreamingWorkerNodes {
    worker_nodes: HashMap<WorkerId, WorkerNode>,
    rx: UnboundedReceiver<LocalNotification>,
    #[cfg_attr(not(debug_assertions), expect(dead_code))]
    meta_manager: Option<MetadataManager>,
}

impl Debug for ActiveStreamingWorkerNodes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveStreamingWorkerNodes")
            .field("worker_nodes", &self.worker_nodes)
            .finish()
    }
}

impl ActiveStreamingWorkerNodes {
    pub(crate) fn uninitialized() -> Self {
        Self {
            worker_nodes: Default::default(),
            rx: unbounded_channel().1,
            meta_manager: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(worker_nodes: HashMap<WorkerId, WorkerNode>) -> Self {
        let (tx, rx) = unbounded_channel();
        let _join_handle = tokio::spawn(async move {
            let _tx = tx;
            std::future::pending::<()>().await
        });
        Self {
            worker_nodes,
            rx,
            meta_manager: None,
        }
    }

    /// Return an uninitialized one as a placeholder for future initialized
    pub(crate) async fn new_snapshot(meta_manager: MetadataManager) -> MetaResult<Self> {
        let (nodes, rx) = meta_manager
            .subscribe_active_streaming_compute_nodes()
            .await?;
        Ok(Self {
            worker_nodes: nodes
                .into_iter()
                .filter_map(|node| {
                    let is_streaming = node.property.as_ref().is_some_and(|p| p.is_streaming);
                    is_streaming.then_some((node.id, node))
                })
                .collect(),
            rx,
            meta_manager: Some(meta_manager),
        })
    }

    pub(crate) fn current(&self) -> &HashMap<WorkerId, WorkerNode> {
        &self.worker_nodes
    }

    pub(crate) async fn changed(&mut self) -> ActiveStreamingWorkerChange {
        loop {
            let notification = self
                .rx
                .recv()
                .await
                .expect("notification stopped or uninitialized");
            fn is_target_worker_node(worker: &WorkerNode) -> bool {
                worker.r#type == WorkerType::ComputeNode as i32
                    && worker.property.as_ref().unwrap().is_streaming
            }
            match notification {
                LocalNotification::WorkerNodeDeleted(worker) => {
                    let is_target_worker_node = is_target_worker_node(&worker);
                    let Some(prev_worker) = self.worker_nodes.remove(&worker.id) else {
                        if is_target_worker_node {
                            warn!(
                                ?worker,
                                "notify to delete an non-existing streaming compute worker"
                            );
                        }
                        continue;
                    };
                    if !is_target_worker_node {
                        warn!(
                            ?worker,
                            ?prev_worker,
                            "deleted worker has a different recent type"
                        );
                    }
                    if worker.state == State::Starting as i32 {
                        warn!(
                            id = %worker.id,
                            host = ?worker.host,
                            state = worker.state,
                            "a starting streaming worker is deleted"
                        );
                    }
                    break ActiveStreamingWorkerChange::Remove(prev_worker);
                }
                LocalNotification::WorkerNodeActivated(worker) => {
                    if !is_target_worker_node(&worker) {
                        if let Some(prev_worker) = self.worker_nodes.remove(&worker.id) {
                            warn!(
                                ?worker,
                                ?prev_worker,
                                "the type of a streaming worker is changed"
                            );
                            break ActiveStreamingWorkerChange::Remove(prev_worker);
                        } else {
                            continue;
                        }
                    }
                    assert_eq!(
                        worker.state,
                        State::Running as i32,
                        "not started worker added: {:?}",
                        worker
                    );
                    if let Some(prev_worker) = self.worker_nodes.insert(worker.id, worker.clone()) {
                        assert_eq!(prev_worker.host, worker.host);
                        assert_eq!(prev_worker.r#type, worker.r#type);
                        warn!(
                            ?prev_worker,
                            ?worker,
                            eq = prev_worker == worker,
                            "notify to update an existing active worker"
                        );
                        if prev_worker == worker {
                            continue;
                        } else {
                            break ActiveStreamingWorkerChange::Update(worker);
                        }
                    } else {
                        break ActiveStreamingWorkerChange::Add(worker);
                    }
                }
                _ => {
                    continue;
                }
            }
        }
    }

    #[cfg(debug_assertions)]
    pub(crate) async fn validate_change(&self) {
        use risingwave_pb::common::WorkerNode;
        use thiserror_ext::AsReport;
        let Some(meta_manager) = &self.meta_manager else {
            return;
        };
        match meta_manager.list_active_streaming_compute_nodes().await {
            Ok(worker_nodes) => {
                let ignore_irrelevant_info = |node: &WorkerNode| {
                    (
                        node.id,
                        WorkerNode {
                            id: node.id,
                            r#type: node.r#type,
                            host: node.host.clone(),
                            property: node.property.clone(),
                            resource: node.resource.clone(),
                            ..Default::default()
                        },
                    )
                };
                let worker_nodes: HashMap<_, _> =
                    worker_nodes.iter().map(ignore_irrelevant_info).collect();
                let curr_worker_nodes: HashMap<_, _> = self
                    .current()
                    .values()
                    .map(ignore_irrelevant_info)
                    .collect();
                if worker_nodes != curr_worker_nodes {
                    warn!(
                        ?worker_nodes,
                        ?curr_worker_nodes,
                        "different to global snapshot"
                    );
                }
            }
            Err(e) => {
                warn!(
                    e = ?e.as_report(),
                    "failed to list active streaming compute nodes for comparison with the local snapshot",
                );
            }
        }
    }
}

impl MetadataManager {
    pub fn new(
        cluster_controller: ClusterControllerRef,
        catalog_controller: CatalogControllerRef,
    ) -> Self {
        Self {
            cluster_controller,
            catalog_controller,
        }
    }

    pub async fn get_worker_by_id(&self, worker_id: WorkerId) -> MetaResult<Option<PbWorkerNode>> {
        self.cluster_controller.get_worker_by_id(worker_id).await
    }

    pub async fn count_worker_node(&self) -> MetaResult<HashMap<WorkerType, u64>> {
        let node_map = self.cluster_controller.count_worker_by_type().await?;
        Ok(node_map
            .into_iter()
            .map(|(ty, cnt)| (ty.into(), cnt as u64))
            .collect())
    }

    pub async fn get_worker_info_by_id(&self, worker_id: WorkerId) -> Option<WorkerExtraInfo> {
        self.cluster_controller
            .get_worker_info_by_id(worker_id as _)
            .await
    }

    pub async fn add_worker_node(
        &self,
        r#type: PbWorkerType,
        host_address: HostAddress,
        property: AddNodeProperty,
        resource: PbResource,
    ) -> MetaResult<WorkerId> {
        self.cluster_controller
            .add_worker(r#type, host_address, property, resource)
            .await
            .map(|id| id as WorkerId)
    }

    pub async fn list_worker_node(
        &self,
        worker_type: Option<WorkerType>,
        worker_state: Option<State>,
    ) -> MetaResult<Vec<PbWorkerNode>> {
        self.cluster_controller
            .list_workers(worker_type.map(Into::into), worker_state.map(Into::into))
            .await
    }

    pub async fn subscribe_active_streaming_compute_nodes(
        &self,
    ) -> MetaResult<(Vec<WorkerNode>, UnboundedReceiver<LocalNotification>)> {
        self.cluster_controller
            .subscribe_active_streaming_compute_nodes()
            .await
    }

    pub async fn list_active_streaming_compute_nodes(&self) -> MetaResult<Vec<PbWorkerNode>> {
        self.cluster_controller
            .list_active_streaming_workers()
            .await
    }

    pub async fn list_active_serving_compute_nodes(&self) -> MetaResult<Vec<PbWorkerNode>> {
        self.cluster_controller.list_active_serving_workers().await
    }

    pub async fn list_active_database_ids(&self) -> MetaResult<HashSet<DatabaseId>> {
        Ok(self
            .catalog_controller
            .list_fragment_database_ids(None)
            .await?
            .into_iter()
            .map(|(_, database_id)| database_id)
            .collect())
    }

    pub async fn split_fragment_map_by_database<T: Debug>(
        &self,
        fragment_map: HashMap<FragmentId, T>,
    ) -> MetaResult<HashMap<DatabaseId, HashMap<FragmentId, T>>> {
        let fragment_to_database_map: HashMap<_, _> = self
            .catalog_controller
            .list_fragment_database_ids(Some(
                fragment_map
                    .keys()
                    .map(|fragment_id| *fragment_id as _)
                    .collect(),
            ))
            .await?
            .into_iter()
            .map(|(fragment_id, database_id)| (fragment_id as FragmentId, database_id))
            .collect();
        let mut ret: HashMap<_, HashMap<_, _>> = HashMap::new();
        for (fragment_id, value) in fragment_map {
            let database_id = *fragment_to_database_map
                .get(&fragment_id)
                .ok_or_else(|| anyhow!("cannot get database_id of fragment {fragment_id}"))?;
            ret.entry(database_id)
                .or_default()
                .try_insert(fragment_id, value)
                .expect("non duplicate");
        }
        Ok(ret)
    }

    pub async fn list_creating_jobs(&self) -> MetaResult<HashSet<JobId>> {
        Ok(self
            .catalog_controller
            .list_creating_jobs(false, None)
            .await?
            .into_iter()
            .map(|(job_id, _, _, _, _)| job_id)
            .collect())
    }

    pub async fn list_sources(&self) -> MetaResult<Vec<PbSource>> {
        self.catalog_controller.list_sources().await
    }

    /// Get and filter the "**root**" fragments of the specified relations.
    /// The root fragment is the bottom-most fragment of its fragment graph, and can be a `MView` or a `Source`.
    ///
    /// See also [`crate::controller::catalog::CatalogController::get_root_fragments`].
    pub async fn get_upstream_root_fragments(
        &self,
        upstream_table_ids: &HashSet<TableId>,
    ) -> MetaResult<HashMap<JobId, Fragment>> {
        let upstream_root_fragments = self
            .catalog_controller
            .get_root_fragments(upstream_table_ids.iter().map(|id| id.as_job_id()).collect())
            .await?;

        Ok(upstream_root_fragments)
    }

    pub async fn get_streaming_cluster_info(&self) -> MetaResult<StreamingClusterInfo> {
        self.cluster_controller.get_streaming_cluster_info().await
    }

    pub async fn get_all_table_options(&self) -> MetaResult<HashMap<TableId, TableOption>> {
        self.catalog_controller.get_all_table_options().await
    }

    pub async fn get_table_name_type_mapping(
        &self,
    ) -> MetaResult<HashMap<TableId, (String, String)>> {
        self.catalog_controller.get_table_name_type_mapping().await
    }

    pub async fn get_created_table_ids(&self) -> MetaResult<Vec<TableId>> {
        self.catalog_controller.get_created_table_ids().await
    }

    pub async fn get_table_associated_source_id(
        &self,
        table_id: TableId,
    ) -> MetaResult<Option<SourceId>> {
        self.catalog_controller
            .get_table_associated_source_id(table_id)
            .await
    }

    pub async fn get_table_catalog_by_ids(&self, ids: &[TableId]) -> MetaResult<Vec<PbTable>> {
        self.catalog_controller
            .get_table_by_ids(ids.to_vec(), false)
            .await
    }

    pub async fn list_refresh_jobs(&self) -> MetaResult<Vec<refresh_job::Model>> {
        self.catalog_controller.list_refresh_jobs().await
    }

    pub async fn list_refreshable_table_ids(&self) -> MetaResult<Vec<TableId>> {
        self.catalog_controller.list_refreshable_table_ids().await
    }

    pub async fn ensure_refresh_job(&self, table_id: TableId) -> MetaResult<()> {
        self.catalog_controller.ensure_refresh_job(table_id).await
    }

    pub async fn update_refresh_job_status(
        &self,
        table_id: TableId,
        status: RefreshState,
        trigger_time: Option<DateTime>,
        is_success: bool,
    ) -> MetaResult<()> {
        self.catalog_controller
            .update_refresh_job_status(table_id, status, trigger_time, is_success)
            .await
    }

    pub async fn reset_all_refresh_jobs_to_idle(&self) -> MetaResult<()> {
        self.catalog_controller
            .reset_all_refresh_jobs_to_idle()
            .await
    }

    pub async fn update_refresh_job_interval(
        &self,
        table_id: TableId,
        trigger_interval_secs: Option<i64>,
    ) -> MetaResult<()> {
        self.catalog_controller
            .update_refresh_job_interval(table_id, trigger_interval_secs)
            .await
    }

    pub async fn get_sink_state_table_ids(&self, sink_id: SinkId) -> MetaResult<Vec<TableId>> {
        self.catalog_controller
            .get_sink_state_table_ids(sink_id)
            .await
    }

    pub async fn get_table_catalog_by_cdc_table_id(
        &self,
        cdc_table_id: &String,
    ) -> MetaResult<Vec<PbTable>> {
        self.catalog_controller
            .get_table_by_cdc_table_id(cdc_table_id)
            .await
    }

    pub async fn get_downstream_fragments(
        &self,
        job_id: JobId,
    ) -> MetaResult<Vec<(PbDispatcherType, Fragment)>> {
        self.catalog_controller
            .get_downstream_fragments(job_id)
            .await
    }

    pub async fn get_job_id_to_internal_table_ids_mapping(
        &self,
    ) -> Option<Vec<(JobId, Vec<TableId>)>> {
        self.catalog_controller.get_job_internal_table_ids().await
    }

    pub async fn get_job_fragments_by_id(&self, job_id: JobId) -> MetaResult<StreamJobFragments> {
        Ok(self
            .catalog_controller
            .get_job_fragments_by_id(job_id)
            .await?
            .0)
    }

    pub fn get_running_actors_of_fragment(&self, id: FragmentId) -> MetaResult<HashSet<ActorId>> {
        self.catalog_controller
            .get_running_actors_of_fragment(id as _)
    }

    // (backfill_actor_id, upstream_source_actor_id)
    pub async fn get_running_actors_for_source_backfill(
        &self,
        source_backfill_fragment_id: FragmentId,
        source_fragment_id: FragmentId,
    ) -> MetaResult<HashSet<(ActorId, ActorId)>> {
        let actor_ids = self
            .catalog_controller
            .get_running_actors_for_source_backfill(
                source_backfill_fragment_id as _,
                source_fragment_id as _,
            )
            .await?;
        Ok(actor_ids
            .into_iter()
            .map(|(id, upstream)| (id as ActorId, upstream as ActorId))
            .collect())
    }

    pub fn worker_actor_count(&self) -> MetaResult<HashMap<WorkerId, usize>> {
        let actor_cnt = self.catalog_controller.worker_actor_count()?;
        Ok(actor_cnt
            .into_iter()
            .map(|(id, cnt)| (id as WorkerId, cnt))
            .collect())
    }

    pub async fn count_streaming_job(&self) -> MetaResult<usize> {
        self.catalog_controller.count_streaming_jobs().await
    }

    pub async fn list_stream_job_desc(&self) -> MetaResult<Vec<MetaTelemetryJobDesc>> {
        self.catalog_controller
            .list_stream_job_desc_for_telemetry()
            .await
    }

    pub async fn update_source_rate_limit_by_source_id(
        &self,
        source_id: SourceId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, PbStreamNode>> {
        self.catalog_controller
            .update_source_rate_limit_by_source_id(source_id as _, rate_limit)
            .await
    }

    pub async fn update_backfill_rate_limit_by_job_id(
        &self,
        job_id: JobId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, PbStreamNode>> {
        self.catalog_controller
            .update_backfill_rate_limit_by_job_id(job_id, rate_limit)
            .await
    }

    pub async fn update_sink_rate_limit_by_sink_id(
        &self,
        sink_id: SinkId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, PbStreamNode>> {
        self.catalog_controller
            .update_sink_rate_limit_by_job_id(sink_id, rate_limit)
            .await
    }

    pub async fn update_dml_rate_limit_by_job_id(
        &self,
        job_id: JobId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, PbStreamNode>> {
        self.catalog_controller
            .update_dml_rate_limit_by_job_id(job_id, rate_limit)
            .await
    }

    pub async fn update_iceberg_table_props_by_table_id(
        &self,
        table_id: TableId,
        props: BTreeMap<String, String>,
        alter_secret_refs: BTreeMap<String, PbSecretRef>,
        alter_iceberg_table_props: Option<PbExtraOptions>,
    ) -> MetaResult<(WithOptionsSecResolved, SinkId)> {
        let (options_with_secret, sink_id) = self
            .catalog_controller
            .update_iceberg_table_props_by_table_id(
                table_id,
                props,
                alter_secret_refs,
                alter_iceberg_table_props,
            )
            .await?;
        Ok((options_with_secret, sink_id))
    }

    pub async fn update_fragment_rate_limit_by_fragment_id(
        &self,
        fragment_id: FragmentId,
        throttle_type: risingwave_pb::common::ThrottleType,
        rate_limit: Option<u32>,
    ) -> MetaResult<PbStreamNode> {
        self.catalog_controller
            .update_fragment_rate_limit_by_fragment_id(fragment_id as _, throttle_type, rate_limit)
            .await
    }

    #[await_tree::instrument]
    pub async fn update_fragment_splits(
        &self,
        split_assignment: &SplitAssignment,
    ) -> MetaResult<()> {
        let fragment_splits = split_assignment
            .iter()
            .map(|(fragment_id, splits)| {
                (
                    *fragment_id as _,
                    splits.values().flatten().cloned().collect_vec(),
                )
            })
            .collect();

        let inner = self.catalog_controller.inner.write().await;

        self.catalog_controller
            .update_fragment_splits(&inner.db, &fragment_splits)
            .await
    }

    pub async fn get_mv_depended_subscriptions(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<HashMap<TableId, HashMap<SubscriptionId, u64>>> {
        Ok(self
            .catalog_controller
            .get_mv_depended_subscriptions(database_id)
            .await?
            .into_iter()
            .map(|(table_id, subscriptions)| {
                (
                    table_id,
                    subscriptions
                        .into_iter()
                        .map(|(subscription_id, retention_time)| {
                            (subscription_id as SubscriptionId, retention_time)
                        })
                        .collect(),
                )
            })
            .collect())
    }

    pub async fn get_job_max_parallelism(&self, job_id: JobId) -> MetaResult<usize> {
        self.catalog_controller
            .get_max_parallelism_by_id(job_id)
            .await
    }

    pub async fn get_existing_job_resource_group(
        &self,
        streaming_job_id: JobId,
    ) -> MetaResult<String> {
        self.catalog_controller
            .get_existing_job_resource_group(streaming_job_id)
            .await
    }

    pub async fn get_database_resource_group(&self, database_id: DatabaseId) -> MetaResult<String> {
        self.catalog_controller
            .get_database_resource_group(database_id)
            .await
    }

    pub fn cluster_id(&self) -> &ClusterId {
        self.cluster_controller.cluster_id()
    }

    pub async fn list_rate_limits(&self) -> MetaResult<Vec<RateLimitInfo>> {
        let rate_limits = self.catalog_controller.list_rate_limits().await?;
        Ok(rate_limits)
    }

    pub async fn get_job_backfill_scan_types(
        &self,
        job_id: JobId,
    ) -> MetaResult<HashMap<FragmentId, PbStreamScanType>> {
        let backfill_types = self
            .catalog_controller
            .get_job_fragment_backfill_scan_type(job_id)
            .await?;
        Ok(backfill_types)
    }

    /// Returns jobs containing a scan that cannot be rescheduled online.
    pub async fn collect_online_unreschedulable_backfill_jobs(
        &self,
        job_ids: impl IntoIterator<Item = &JobId>,
    ) -> MetaResult<HashSet<JobId>> {
        let mut unreschedulable = HashSet::new();

        for job_id in job_ids {
            let scan_types = self
                .catalog_controller
                .get_job_fragment_backfill_scan_type(*job_id)
                .await?;
            if scan_types
                .values()
                .any(|scan_type| !scan_type.is_reschedulable(true))
            {
                unreschedulable.insert(*job_id);
            }
        }

        Ok(unreschedulable)
    }

    pub async fn collect_reschedule_blocked_jobs_for_creating_jobs(
        &self,
        creating_job_ids: impl IntoIterator<Item = &JobId>,
        is_online: bool,
    ) -> MetaResult<HashSet<JobId>> {
        let creating_job_ids: HashSet<_> = creating_job_ids.into_iter().copied().collect();
        if creating_job_ids.is_empty() {
            return Ok(HashSet::new());
        }

        let inner = self.catalog_controller.inner.read().await;
        let txn = inner.db.begin().await?;

        let mut initial_fragment_ids = HashSet::new();
        for job_id in &creating_job_ids {
            let scan_types = self
                .catalog_controller
                .get_job_fragment_backfill_scan_type_in_txn(&txn, *job_id)
                .await?;
            initial_fragment_ids.extend(scan_types.into_iter().filter_map(
                |(fragment_id, scan_type)| {
                    (!scan_type.is_reschedulable(is_online)).then_some(fragment_id)
                },
            ));
        }

        if !initial_fragment_ids.is_empty() {
            let upstream_fragments = self
                .catalog_controller
                .upstream_fragments_in_txn(&txn, initial_fragment_ids.iter().copied())
                .await?;
            initial_fragment_ids.extend(upstream_fragments.into_values().flatten());
        }

        let mut blocked_fragment_ids = initial_fragment_ids.clone();
        if !initial_fragment_ids.is_empty() {
            let initial_fragment_ids = initial_fragment_ids.into_iter().collect_vec();
            let ensembles =
                find_fragment_no_shuffle_dags_detailed(&txn, &initial_fragment_ids).await?;
            for ensemble in ensembles {
                blocked_fragment_ids.extend(ensemble.fragments());
            }
        }

        let mut blocked_job_ids = HashSet::new();
        if !blocked_fragment_ids.is_empty() {
            let fragment_ids = blocked_fragment_ids.into_iter().collect_vec();
            let fragment_job_ids = self
                .catalog_controller
                .get_fragment_job_id_in_txn(&txn, fragment_ids)
                .await?;
            blocked_job_ids.extend(
                fragment_job_ids
                    .into_iter()
                    .map(|job_id| job_id.as_job_id()),
            );
        }

        txn.commit().await?;

        Ok(blocked_job_ids)
    }
}

impl MetadataManager {
    /// Wait for job finishing notification in `TrackingJob::finish`.
    /// The progress is updated per barrier.
    #[await_tree::instrument]
    pub async fn wait_streaming_job_finished(
        &self,
        database_id: DatabaseId,
        id: JobId,
    ) -> MetaResult<NotificationVersion> {
        tracing::debug!("wait_streaming_job_finished: {id:?}");
        let mut mgr = self.catalog_controller.get_inner_write_guard().await;
        if mgr.streaming_job_is_finished(id).await? {
            return Ok(self.catalog_controller.notify_frontend_trivial().await);
        }
        let (tx, rx) = oneshot::channel();

        mgr.register_finish_notifier(database_id, id, tx);
        drop(mgr);
        rx.await
            .map_err(|_| "no received reason".to_owned())
            .and_then(|result| result)
            .map_err(|reason| anyhow!("failed to wait streaming job finish: {}", reason).into())
    }

    pub(crate) async fn notify_finish_failed(&self, database_id: Option<DatabaseId>, err: String) {
        let mut mgr = self.catalog_controller.get_inner_write_guard().await;
        mgr.notify_finish_failed(database_id, err);
    }
}
