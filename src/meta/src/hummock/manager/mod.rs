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

use std::collections::{BTreeMap, HashMap};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::anyhow;
use bytes::Bytes;
use futures::FutureExt;
use itertools::Itertools;
use parking_lot::lock_api::RwLock;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::monitor::MonitoredRwLock;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_hummock_sdk::change_log::TableChangeLog;
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_hummock_sdk::{
    CompactionGroupId, HummockCompactionTaskId, HummockContextId, HummockVersionId,
    version_archive_dir, version_checkpoint_path,
};
use risingwave_meta_model::{
    compaction_status, compaction_task, hummock_pinned_version, hummock_version_delta,
    hummock_version_stats,
};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::{
    HummockVersionStats, PbCompactionGroupInfo, SubscribeCompactionEventRequest,
};
use table_write_throughput_statistic::TableWriteThroughputStatisticManager;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{Mutex, Semaphore, oneshot};
use tonic::Streaming;

use crate::MetaResult;
use crate::hummock::CompactorManagerRef;
use crate::hummock::compaction::CompactStatus;
use crate::hummock::error::Result;
use crate::hummock::manager::checkpoint::HummockVersionCheckpoint;
use crate::hummock::manager::context::ContextInfo;
use crate::hummock::manager::gc::{FullGcState, GcManager};
use crate::hummock::manager::sequence::PrefetchedSequence;
use crate::hummock::model::ext::{compaction_task_model_to_assignment, to_table_change_log};
use crate::manager::{MetaSrvEnv, MetadataManager};
use crate::model::{ClusterId, MetadataModelError};
use crate::rpc::metrics::MetaMetrics;

mod context;
mod gc;
mod tests;
mod versioning;
pub use context::HummockVersionSafePoint;
use versioning::*;
pub(crate) mod checkpoint;
mod commit_epoch;
mod compaction;
pub mod sequence;
mod table_change_log;
pub mod table_write_throughput_statistic;
pub mod time_travel;
mod timer_task;
mod transaction;
mod utils;
mod worker;

pub use commit_epoch::{CommitEpochInfo, NewTableFragmentInfo};
pub use compaction::compaction_event_loop::*;
use compaction::*;
pub use compaction::{GroupState, GroupStateValidator, ManualCompactionTriggerResult};
pub(crate) use utils::*;

struct TableCommittedEpochNotifiers {
    txs: HashMap<TableId, Vec<UnboundedSender<u64>>>,
}

impl TableCommittedEpochNotifiers {
    fn notify_deltas(&mut self, deltas: &[HummockVersionDelta]) {
        self.txs.retain(|table_id, txs| {
            let mut is_dropped = false;
            let mut committed_epoch = None;
            for delta in deltas {
                if delta.removed_table_ids.contains(table_id) {
                    is_dropped = true;
                    break;
                }
                if let Some(info) = delta.state_table_info_delta.get(table_id) {
                    committed_epoch = Some(info.committed_epoch);
                }
            }
            if is_dropped {
                false
            } else if let Some(committed_epoch) = committed_epoch {
                txs.retain(|tx| tx.send(committed_epoch).is_ok());
                !txs.is_empty()
            } else {
                true
            }
        })
    }
}

#[derive(Clone, Debug)]
struct CompactionTaskReportResult {
    task_id: HummockCompactionTaskId,

    task_status: TaskStatus,
    reported: bool,
}

struct CompactionTaskReportNotifiers {
    txs: HashMap<HummockCompactionTaskId, Vec<oneshot::Sender<CompactionTaskReportResult>>>,
}

impl CompactionTaskReportNotifiers {
    fn register(
        &mut self,
        task_id: HummockCompactionTaskId,
        tx: oneshot::Sender<CompactionTaskReportResult>,
    ) {
        self.txs.entry(task_id).or_default().push(tx);
    }

    fn remove(&mut self, task_id: HummockCompactionTaskId) {
        self.txs.remove(&task_id);
    }

    fn notify(&mut self, result: CompactionTaskReportResult) {
        if let Some(txs) = self.txs.remove(&result.task_id) {
            for tx in txs {
                let _ = tx.send(result.clone());
            }
        }
    }
}
// Update to states are performed as follow:
// - Initialize ValTransaction for the meta state to update
// - Make changes on the ValTransaction.
// - Call `commit_multi_var` to commit the changes via meta store transaction. If transaction
//   succeeds, the in-mem state will be updated by the way.
pub struct HummockManager {
    pub env: MetaSrvEnv,

    metadata_manager: MetadataManager,
    /// Lock order: `compaction`, `versioning`, `compaction_group_manager`, `context_info`
    /// - Lock `compaction` first, then `versioning`, then `compaction_group_manager` and finally `context_info`.
    /// - This order should be strictly followed to prevent deadlock.
    compaction: MonitoredRwLock<Compaction>,
    versioning: MonitoredRwLock<Versioning>,
    /// `CompactionGroupManager` manages compaction configs for compaction groups.
    compaction_group_manager: MonitoredRwLock<CompactionGroupManager>,
    context_info: MonitoredRwLock<ContextInfo>,

    pub metrics: Arc<MetaMetrics>,

    pub compactor_manager: CompactorManagerRef,
    pub iceberg_compactor_manager: Arc<IcebergCompactorManager>,
    event_sender: HummockManagerEventSender,
    object_store: ObjectStoreRef,
    version_checkpoint_path: String,
    version_archive_dir: String,
    pause_version_checkpoint: AtomicBool,
    table_write_throughput_statistic_manager:
        parking_lot::RwLock<TableWriteThroughputStatisticManager>,
    table_committed_epoch_notifiers: parking_lot::Mutex<TableCommittedEpochNotifiers>,
    compaction_task_report_notifiers: parking_lot::Mutex<CompactionTaskReportNotifiers>,
    version_stat_tx: UnboundedSender<Arc<HummockVersion>>,

    // for compactor
    // `compactor_streams_change_tx` is used to pass the mapping from `context_id` to event_stream
    // and is maintained in memory. All event_streams are consumed through a separate event loop
    compactor_streams_change_tx:
        UnboundedSender<(HummockContextId, Streaming<SubscribeCompactionEventRequest>)>,

    // `compaction_state` will record the types of compact tasks that can be triggered in `hummock`
    // and suggest types with a certain priority.
    pub compaction_state: CompactionState,
    full_gc_state: Arc<FullGcState>,
    now: Mutex<u64>,
    inflight_time_travel_query: Semaphore,
    gc_manager: GcManager,

    /// In-memory cache of prefetched compaction task ids to reduce per-task DB round-trips.
    prefetched_compaction_task_ids: PrefetchedSequence,
    table_id_to_table_option: parking_lot::RwLock<HashMap<TableId, TableOption>>,
}

pub type HummockManagerRef = Arc<HummockManager>;

use risingwave_object_store::object::{ObjectError, ObjectStoreRef, build_remote_object_store};
use risingwave_pb::catalog::Table;

use super::IcebergCompactorManager;
use crate::controller::SqlMetaStore;
use crate::hummock::manager::compaction_group_manager::CompactionGroupManager;
use crate::hummock::manager::worker::HummockManagerEventSender;

impl HummockManager {
    pub async fn new(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        metrics: Arc<MetaMetrics>,
        compactor_manager: CompactorManagerRef,
        compactor_streams_change_tx: UnboundedSender<(
            HummockContextId,
            Streaming<SubscribeCompactionEventRequest>,
        )>,
    ) -> Result<HummockManagerRef> {
        let compaction_group_manager = CompactionGroupManager::new(&env).await?;
        Self::new_impl(
            env,
            metadata_manager,
            metrics,
            compactor_manager,
            compaction_group_manager,
            compactor_streams_change_tx,
        )
        .await
    }

    #[cfg(any(test, feature = "test"))]
    pub(super) async fn with_config(
        env: MetaSrvEnv,
        cluster_controller: crate::controller::cluster::ClusterControllerRef,
        catalog_controller: crate::controller::catalog::CatalogControllerRef,
        metrics: Arc<MetaMetrics>,
        compactor_manager: CompactorManagerRef,
        config: risingwave_pb::hummock::CompactionConfig,
        compactor_streams_change_tx: UnboundedSender<(
            HummockContextId,
            Streaming<SubscribeCompactionEventRequest>,
        )>,
    ) -> HummockManagerRef {
        let compaction_group_manager = CompactionGroupManager::new_with_config(&env, config)
            .await
            .unwrap();
        let metadata_manager = MetadataManager::new(cluster_controller, catalog_controller);
        Self::new_impl(
            env,
            metadata_manager,
            metrics,
            compactor_manager,
            compaction_group_manager,
            compactor_streams_change_tx,
        )
        .await
        .unwrap()
    }

    async fn new_impl(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        metrics: Arc<MetaMetrics>,
        compactor_manager: CompactorManagerRef,
        compaction_group_manager: CompactionGroupManager,
        compactor_streams_change_tx: UnboundedSender<(
            HummockContextId,
            Streaming<SubscribeCompactionEventRequest>,
        )>,
    ) -> Result<HummockManagerRef> {
        let sys_params = env.system_params_reader().await;
        let state_store_url = sys_params.state_store();
        let state_store_url = state_store_url.expose();

        let state_store_dir: &str = sys_params.data_directory();
        let use_new_object_prefix_strategy: bool = sys_params.use_new_object_prefix_strategy();
        let deterministic_mode = env.opts.compaction_deterministic_test;
        let mut object_store_config = env.opts.object_store_config.clone();
        // For fs and hdfs object store, operations are not always atomic.
        // We should manually enable atomicity guarantee by setting the atomic_write_dir config when building services.
        object_store_config.set_atomic_write_dir();
        let object_store = Arc::new(
            build_remote_object_store(
                state_store_url.strip_prefix("hummock+").unwrap_or("memory"),
                metrics.object_store_metric.clone(),
                "Version Checkpoint",
                Arc::new(object_store_config),
                state_store_dir,
            )
            .await,
        );
        // Make sure data dir is not used by another cluster.
        // Skip this check in e2e compaction test, which needs to start a secondary cluster with
        // same bucket
        if !deterministic_mode {
            write_exclusive_cluster_id(
                state_store_dir,
                env.cluster_id().clone(),
                object_store.clone(),
            )
            .await?;

            // config bucket lifecycle for new cluster.
            if let risingwave_object_store::object::ObjectStoreImpl::S3(s3) = object_store.as_ref()
                && !env.opts.do_not_config_object_storage_lifecycle
            {
                let is_bucket_expiration_configured =
                    s3.inner().configure_bucket_lifecycle(state_store_dir).await;
                if is_bucket_expiration_configured {
                    return Err(ObjectError::internal("Cluster cannot start with object expiration configured for bucket because RisingWave data will be lost when object expiration kicks in.
                    Please disable object expiration and restart the cluster.")
                    .into());
                }
            }
        }
        let version_checkpoint_path = version_checkpoint_path(state_store_dir);
        let version_archive_dir = version_archive_dir(state_store_dir);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (version_stat_tx, mut version_stat_rx) = tokio::sync::mpsc::unbounded_channel();
        let inflight_time_travel_query = env.opts.max_inflight_time_travel_query;
        let gc_manager = GcManager::new(
            object_store.clone(),
            state_store_dir,
            use_new_object_prefix_strategy,
        );

        let max_table_statistic_expired_time = std::cmp::max(
            env.opts.table_stat_throuput_window_seconds_for_split,
            env.opts.table_stat_throuput_window_seconds_for_merge,
        ) as i64;

        let iceberg_compactor_manager = Arc::new(IcebergCompactorManager::new());

        let instance = HummockManager {
            env,
            versioning: MonitoredRwLock::new(
                metrics.hummock_manager_lock_time.clone(),
                metrics.hummock_manager_real_process_time.clone(),
                Default::default(),
                "hummock_manager::versioning",
            ),
            compaction: MonitoredRwLock::new(
                metrics.hummock_manager_lock_time.clone(),
                metrics.hummock_manager_real_process_time.clone(),
                Default::default(),
                "hummock_manager::compaction",
            ),
            compaction_group_manager: MonitoredRwLock::new(
                metrics.hummock_manager_lock_time.clone(),
                metrics.hummock_manager_real_process_time.clone(),
                compaction_group_manager,
                "hummock_manager::compaction_group_manager",
            ),
            context_info: MonitoredRwLock::new(
                metrics.hummock_manager_lock_time.clone(),
                metrics.hummock_manager_real_process_time.clone(),
                Default::default(),
                "hummock_manager::context_info",
            ),
            metrics,
            metadata_manager,
            compactor_manager,
            iceberg_compactor_manager,
            event_sender: tx,
            object_store,
            version_checkpoint_path,
            version_archive_dir,
            pause_version_checkpoint: AtomicBool::new(false),
            table_write_throughput_statistic_manager: parking_lot::RwLock::new(
                TableWriteThroughputStatisticManager::new(max_table_statistic_expired_time),
            ),
            table_committed_epoch_notifiers: parking_lot::Mutex::new(
                TableCommittedEpochNotifiers {
                    txs: Default::default(),
                },
            ),
            compaction_task_report_notifiers: parking_lot::Mutex::new(
                CompactionTaskReportNotifiers {
                    txs: Default::default(),
                },
            ),
            version_stat_tx,
            compactor_streams_change_tx,
            compaction_state: CompactionState::new(),
            full_gc_state: FullGcState::new().into(),
            now: Mutex::new(0),
            inflight_time_travel_query: Semaphore::new(inflight_time_travel_query as usize),
            gc_manager,
            prefetched_compaction_task_ids: PrefetchedSequence::new(),
            table_id_to_table_option: RwLock::new(HashMap::new()),
        };
        let instance = Arc::new(instance);
        let version_stat_metrics = instance.metrics.clone();
        tokio::spawn(async move {
            while let Some(mut version) = version_stat_rx.recv().await {
                while let Some(Some(next_version)) = version_stat_rx.recv().now_or_never() {
                    version = next_version;
                }
                transaction::trigger_version_stat(&version_stat_metrics, version.as_ref());
            }
        });
        instance.init_time_travel_state().await?;
        instance.load_meta_store_state().await?;
        instance.start_worker(rx);
        instance.release_invalid_contexts().await?;
        // Release snapshots pinned by meta on restarting.
        instance.release_meta_context().await?;
        Ok(instance)
    }

    fn meta_store_ref(&self) -> &SqlMetaStore {
        self.env.meta_store_ref()
    }

    /// Load state from meta store.
    async fn load_meta_store_state(&self) -> Result<()> {
        let now = self.load_now().await?;
        *self.now.lock().await = now.unwrap_or(0);

        let mut compaction_guard = self
            .compaction
            .write_with_process_name("load_meta_store_state")
            .await;
        let mut versioning_guard = self
            .versioning
            .write_with_process_name("load_meta_store_state")
            .await;
        let mut context_info_guard = self
            .context_info
            .write_with_process_name("load_meta_store_state")
            .await;
        self.load_meta_store_state_impl(
            &mut compaction_guard,
            &mut versioning_guard,
            &mut context_info_guard,
        )
        .await
    }

    /// Load state from meta store.
    async fn load_meta_store_state_impl(
        &self,
        compaction_guard: &mut Compaction,
        versioning_guard: &mut Versioning,
        context_info: &mut ContextInfo,
    ) -> Result<()> {
        use sea_orm::EntityTrait;
        let meta_store = self.meta_store_ref();
        let compaction_statuses: BTreeMap<CompactionGroupId, CompactStatus> =
            compaction_status::Entity::find()
                .all(&meta_store.conn)
                .await
                .map_err(MetadataModelError::from)?
                .into_iter()
                .map(|m| (m.compaction_group_id as CompactionGroupId, m.into()))
                .collect();
        if !compaction_statuses.is_empty() {
            compaction_guard.compaction_statuses = compaction_statuses;
        }

        compaction_guard.compact_task_assignment = compaction_task::Entity::find()
            .all(&meta_store.conn)
            .await
            .map_err(MetadataModelError::from)?
            .into_iter()
            .map(|m| {
                (
                    m.id as HummockCompactionTaskId,
                    compaction_task_model_to_assignment(m),
                )
            })
            .collect();

        let hummock_version_deltas: BTreeMap<HummockVersionId, HummockVersionDelta> =
            hummock_version_delta::Entity::find()
                .all(&meta_store.conn)
                .await
                .map_err(MetadataModelError::from)?
                .into_iter()
                .map(|m| {
                    (
                        m.id,
                        HummockVersionDelta::from_persisted_protobuf_owned(m.into()),
                    )
                })
                .collect();

        let checkpoint = self.try_read_checkpoint().await?;
        let mut redo_state = if let Some(c) = checkpoint {
            versioning_guard.checkpoint = c;
            versioning_guard.checkpoint.version.as_ref().clone()
        } else {
            let default_compaction_config = self
                .compaction_group_manager
                .read_with_process_name("load_meta_store_state")
                .await
                .default_compaction_config();
            let checkpoint_version = HummockVersion::create_init_version(default_compaction_config);
            tracing::info!("init hummock version checkpoint");
            versioning_guard.checkpoint = HummockVersionCheckpoint {
                version: Arc::new(checkpoint_version.clone()),
                stale_objects: Default::default(),
            };
            self.write_checkpoint(&versioning_guard.checkpoint).await?;
            checkpoint_version
        };
        let mut applied_delta_count = 0;
        let total_to_apply = hummock_version_deltas.range(redo_state.id + 1..).count();
        tracing::info!(
            total_delta = hummock_version_deltas.len(),
            total_to_apply,
            "Start redo Hummock version."
        );
        for version_delta in hummock_version_deltas
            .range(redo_state.id + 1..)
            .map(|(_, v)| v)
        {
            assert_eq!(
                version_delta.prev_id, redo_state.id,
                "delta prev_id {}, redo state id {}",
                version_delta.prev_id, redo_state.id
            );
            redo_state.apply_version_delta(version_delta);
            applied_delta_count += 1;
            if applied_delta_count % 1000 == 0 {
                tracing::info!("Redo progress {applied_delta_count}/{total_to_apply}.");
            }
        }
        tracing::info!("Finish redo Hummock version.");
        let pruned_stale_table_id_count = redo_state.prune_stale_table_ids_from_ssts();
        if pruned_stale_table_id_count > 0 {
            tracing::warn!(
                pruned_stale_table_id_count,
                version_id = ?redo_state.id,
                "Pruned stale table ids from recovered Hummock SST metadata."
            );
        }
        versioning_guard.version_stats = hummock_version_stats::Entity::find()
            .one(&meta_store.conn)
            .await
            .map_err(MetadataModelError::from)?
            .map(HummockVersionStats::from)
            .unwrap_or_else(|| HummockVersionStats {
                // version_stats.hummock_version_id is always 0 in meta store.
                hummock_version_id: 0.into(),
                ..Default::default()
            });

        versioning_guard.current_version = Arc::new(redo_state);
        versioning_guard.hummock_version_deltas = hummock_version_deltas;
        versioning_guard.table_change_log =
            risingwave_meta_model::hummock_table_change_log::Entity::find()
                .all(&self.env.meta_store_ref().conn)
                .await
                .map_err(MetadataModelError::from)?
                .into_iter()
                .map(|m| (m.table_id, to_table_change_log(m)))
                .into_group_map()
                .into_iter()
                .map(|(table_id, unordered_change_logs)| {
                    (
                        table_id,
                        TableChangeLog::new(
                            unordered_change_logs
                                .into_iter()
                                .sorted_by_key(|l| l.checkpoint_epoch),
                        ),
                    )
                })
                .collect();

        context_info.pinned_versions = hummock_pinned_version::Entity::find()
            .all(&meta_store.conn)
            .await
            .map_err(MetadataModelError::from)?
            .into_iter()
            .map(|m| (m.context_id as HummockContextId, m.into()))
            .collect();

        self.initial_compaction_group_config_after_load(
            versioning_guard,
            self.compaction_group_manager
                .write_with_process_name("load_meta_store_state")
                .await
                .deref_mut(),
        )
        .await?;

        Ok(())
    }

    pub fn init_metadata_for_version_replay(
        &self,
        _table_catalogs: Vec<Table>,
        _compaction_groups: Vec<PbCompactionGroupInfo>,
    ) -> Result<()> {
        unimplemented!("kv meta store is deprecated");
    }

    /// Replay a version delta to current hummock version.
    /// Returns the `version_id`, `max_committed_epoch` of the new version and the modified
    /// compaction groups
    #[cfg(any(test, feature = "test"))]
    pub async fn replay_version_delta(
        &self,
        mut version_delta: HummockVersionDelta,
    ) -> Result<(HummockVersion, Vec<CompactionGroupId>)> {
        let mut versioning_guard = self
            .versioning
            .write_with_process_name("replay_version_delta")
            .await;
        // ensure the version id is ascending after replay
        version_delta.id = versioning_guard.current_version.next_version_id();
        version_delta.prev_id = versioning_guard.current_version.id;
        let mut version_new = versioning_guard.current_version.as_ref().clone();
        version_new.apply_version_delta(&version_delta);

        let compaction_group_ids = version_delta.group_deltas.keys().cloned().collect();
        versioning_guard.current_version = Arc::new(version_new.clone());
        Ok((version_new, compaction_group_ids))
    }

    pub async fn disable_commit_epoch(&self) -> Arc<HummockVersion> {
        let mut versioning_guard = self
            .versioning
            .write_with_process_name("disable_commit_epoch")
            .await;
        versioning_guard.disable_commit_epochs = true;
        versioning_guard.current_version.clone()
    }

    pub fn metadata_manager(&self) -> &MetadataManager {
        &self.metadata_manager
    }

    pub fn object_store_media_type(&self) -> &'static str {
        self.object_store.media_type()
    }

    pub fn update_table_id_to_table_option(
        &self,
        new_table_id_to_table_option: HashMap<TableId, TableOption>,
    ) {
        *self.table_id_to_table_option.write() = new_table_id_to_table_option;
    }

    pub fn metadata_manager_ref(&self) -> &MetadataManager {
        &self.metadata_manager
    }

    pub async fn subscribe_table_committed_epoch(
        &self,
        table_id: TableId,
    ) -> MetaResult<(u64, UnboundedReceiver<u64>)> {
        let version = self
            .versioning
            .read_with_process_name("subscribe_table_committed_epoch")
            .await;
        if let Some(epoch) = version.current_version.table_committed_epoch(table_id) {
            let (tx, rx) = unbounded_channel();
            self.table_committed_epoch_notifiers
                .lock()
                .txs
                .entry(table_id)
                .or_default()
                .push(tx);
            Ok((epoch, rx))
        } else {
            Err(anyhow!("table {} does not exist", table_id).into())
        }
    }
}

async fn write_exclusive_cluster_id(
    state_store_dir: &str,
    cluster_id: ClusterId,
    object_store: ObjectStoreRef,
) -> Result<()> {
    const CLUSTER_ID_DIR: &str = "cluster_id";
    const CLUSTER_ID_NAME: &str = "0";
    let cluster_id_dir = format!("{}/{}/", state_store_dir, CLUSTER_ID_DIR);
    let cluster_id_full_path = format!("{}{}", cluster_id_dir, CLUSTER_ID_NAME);
    tracing::info!("try reading cluster_id");
    match object_store.read(&cluster_id_full_path, ..).await {
        Ok(stored_cluster_id) => {
            let stored_cluster_id = String::from_utf8(stored_cluster_id.to_vec()).unwrap();
            if cluster_id.deref() == stored_cluster_id {
                return Ok(());
            }

            Err(ObjectError::internal(format!(
                "Data directory is already used by another cluster with id {:?}, path {}.",
                stored_cluster_id, cluster_id_full_path,
            ))
            .into())
        }
        Err(e) => {
            if e.is_object_not_found_error() {
                tracing::info!("cluster_id not found, writing cluster_id");
                object_store
                    .upload(&cluster_id_full_path, Bytes::from(String::from(cluster_id)))
                    .await?;
                return Ok(());
            }
            Err(e.into())
        }
    }
}
