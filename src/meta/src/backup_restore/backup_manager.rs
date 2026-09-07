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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use risingwave_backup::error::BackupError;
use risingwave_backup::storage::{MetaSnapshotStorage, ObjectStoreMetaSnapshotStorage};
use risingwave_backup::{MetaBackupJobId, MetaSnapshotId, MetaSnapshotManifest};
use risingwave_common::bail;
use risingwave_common::config::ObjectStoreConfig;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_hummock_sdk::HummockRawObjectId;
use risingwave_object_store::object::build_remote_object_store;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_pb::backup_service::{BackupJobStatus, MetaBackupManifestId};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use thiserror_ext::AsReport;
use tokio::task::JoinHandle;

use crate::MetaResult;
use crate::backup_restore::meta_snapshot_builder;
use crate::backup_restore::metrics::BackupManagerMetrics;
use crate::hummock::sequence::next_meta_backup_id;
use crate::hummock::{HummockManagerRef, HummockVersionSafePoint};
use crate::manager::{LocalNotification, MetaSrvEnv};
use crate::rpc::metrics::MetaMetrics;

pub enum BackupJobResult {
    Succeeded,
    Failed(BackupError),
}

/// `BackupJobHandle` tracks running job.
struct BackupJobHandle {
    job_id: u64,
    #[expect(dead_code)]
    hummock_version_safe_point: HummockVersionSafePoint,
    start_time: Instant,
}

impl BackupJobHandle {
    pub fn new(job_id: u64, hummock_version_safe_point: HummockVersionSafePoint) -> Self {
        Self {
            job_id,
            hummock_version_safe_point,
            start_time: Instant::now(),
        }
    }
}

pub type BackupManagerRef = Arc<BackupManager>;
/// (url, dir)
type StoreConfig = (String, String);

/// `BackupManager` manages lifecycle of all existent backups and the running backup job.
pub struct BackupManager {
    env: MetaSrvEnv,
    hummock_manager: HummockManagerRef,
    backup_store: ArcSwap<(ObjectStoreMetaSnapshotStorage, StoreConfig)>,
    /// Tracks the running backup job. Concurrent jobs is not supported.
    running_job_handle: tokio::sync::Mutex<Option<BackupJobHandle>>,
    metrics: BackupManagerMetrics,
    meta_metrics: Arc<MetaMetrics>,
    /// (job id, status, message)
    latest_job_info: ArcSwap<(MetaBackupJobId, BackupJobStatus, String)>,
}

impl BackupManager {
    pub async fn new(
        env: MetaSrvEnv,
        hummock_manager: HummockManagerRef,
        metrics: Arc<MetaMetrics>,
        store_url: &str,
        store_dir: &str,
    ) -> MetaResult<Arc<Self>> {
        let store_config = (store_url.to_owned(), store_dir.to_owned());
        let store = create_snapshot_store(
            &store_config,
            metrics.object_store_metric.clone(),
            &env.opts.object_store_config,
        )
        .await?;
        tracing::info!(
            "backup manager initialized: url={}, dir={}",
            store_config.0,
            store_config.1
        );
        let instance = Arc::new(Self::with_store(
            env.clone(),
            hummock_manager,
            metrics,
            (store, store_config),
        ));
        let (local_notification_tx, mut local_notification_rx) =
            tokio::sync::mpsc::unbounded_channel();
        env.notification_manager()
            .insert_local_sender(local_notification_tx);
        let this = instance.clone();
        tokio::spawn(async move {
            loop {
                match local_notification_rx.recv().await {
                    Some(notification) => {
                        if let LocalNotification::SystemParamsChange(p) = notification {
                            let new_config = (
                                p.backup_storage_url().to_owned(),
                                p.backup_storage_directory().to_owned(),
                            );
                            this.handle_new_config(new_config).await;
                        }
                    }
                    None => {
                        return;
                    }
                }
            }
        });
        Ok(instance)
    }

    async fn handle_new_config(&self, new_config: StoreConfig) {
        if self.backup_store.load().1 == new_config {
            return;
        }
        if let Err(e) = self.set_store(new_config.clone()).await {
            // Retry is driven by periodic system params notification.
            tracing::warn!(
                url = &new_config.0,
                dir = &new_config.1,
                error = %e.as_report(),
                "failed to apply new backup config",
            );
        }
    }

    fn with_store(
        env: MetaSrvEnv,
        hummock_manager: HummockManagerRef,
        meta_metrics: Arc<MetaMetrics>,
        backup_store: (ObjectStoreMetaSnapshotStorage, StoreConfig),
    ) -> Self {
        Self {
            env,
            hummock_manager,
            backup_store: ArcSwap::from_pointee(backup_store),
            running_job_handle: tokio::sync::Mutex::new(None),
            metrics: BackupManagerMetrics::default(),
            meta_metrics,
            latest_job_info: ArcSwap::from_pointee((0, BackupJobStatus::NotFound, "".into())),
        }
    }

    pub async fn set_store(&self, config: StoreConfig) -> MetaResult<()> {
        let new_store = create_snapshot_store(
            &config,
            self.meta_metrics.object_store_metric.clone(),
            &self.env.opts.object_store_config,
        )
        .await?;
        tracing::info!(
            "new backup config is applied: url={}, dir={}",
            config.0,
            config.1
        );
        self.backup_store.store(Arc::new((new_store, config)));
        Ok(())
    }

    #[cfg(test)]
    pub async fn for_test(env: MetaSrvEnv, hummock_manager: HummockManagerRef) -> Self {
        Self::with_store(
            env,
            hummock_manager,
            Arc::new(MetaMetrics::default()),
            (
                risingwave_backup::storage::unused().await,
                StoreConfig::default(),
            ),
        )
    }

    /// Starts a backup job in background. It's non-blocking.
    /// Returns job id.
    pub async fn start_backup_job(
        self: &Arc<Self>,
        remarks: Option<String>,
    ) -> MetaResult<MetaBackupJobId> {
        let mut guard = self.running_job_handle.lock().await;
        if let Some(job) = (*guard).as_ref() {
            bail!(format!(
                "concurrent backup job is not supported: existent job {}",
                job.job_id
            ));
        }
        // The reasons to limit number of meta snapshot are:
        // 1. limit size of `MetaSnapshotManifest`, which is kept in memory by
        // `ObjectStoreMetaSnapshotStorage`.
        // 2. limit number of pinned SSTs returned by
        // `list_pinned_ssts`, which subsequently is used by GC.
        const MAX_META_SNAPSHOT_NUM: usize = 100;
        let current_number = self
            .backup_store
            .load()
            .0
            .manifest()
            .await
            .snapshot_metadata
            .len();
        if current_number > MAX_META_SNAPSHOT_NUM {
            bail!(format!(
                "too many existent meta snapshots, expect at most {}",
                MAX_META_SNAPSHOT_NUM
            ))
        }

        let job_id = next_meta_backup_id(&self.env).await?;
        self.latest_job_info
            .store(Arc::new((job_id, BackupJobStatus::Running, "".into())));
        let hummock_version_safe_point = self.hummock_manager.register_safe_point().await;
        // Ideally `BackupWorker` and its r/w IO can be made external to meta node.
        // The justification of keeping `BackupWorker` in meta node are:
        // - It makes meta node the only writer of backup storage, which eases implementation.
        // - It's likely meta store is deployed in the same node with meta node.
        // - IO volume of metadata snapshot is not expected to be large.
        // - Backup job is not expected to be frequent.
        BackupWorker::new(self.clone()).start(job_id, remarks);
        let job_handle = BackupJobHandle::new(job_id, hummock_version_safe_point);
        *guard = Some(job_handle);
        self.metrics.job_count.inc();
        Ok(job_id)
    }

    pub fn get_backup_job_status(&self, job_id: MetaBackupJobId) -> (BackupJobStatus, String) {
        let last = self.latest_job_info.load();
        if last.0 == job_id {
            return (last.1, last.2.clone());
        }
        (BackupJobStatus::NotFound, "".into())
    }

    async fn finish_backup_job(&self, job_id: MetaBackupJobId, job_result: BackupJobResult) {
        // `job_handle` holds `hummock_version_safe_point` until the job is completed.
        let job_handle = self
            .take_job_handle_by_job_id(job_id)
            .await
            .expect("job id should match");
        let job_latency = job_handle.start_time.elapsed().as_secs_f64();
        match job_result {
            BackupJobResult::Succeeded => {
                self.metrics.job_latency_success.observe(job_latency);
                tracing::info!("succeeded backup job {}", job_id);
                self.env
                    .notification_manager()
                    .notify_hummock_without_version(
                        Operation::Update,
                        Info::MetaBackupManifestId(MetaBackupManifestId {
                            id: self.backup_store.load().0.manifest().await.manifest_id,
                        }),
                    );
                self.latest_job_info.store(Arc::new((
                    job_id,
                    BackupJobStatus::Succeeded,
                    "".into(),
                )));
            }
            BackupJobResult::Failed(e) => {
                self.metrics.job_latency_failure.observe(job_latency);
                let message = format!("failed backup job {}: {}", job_id, e.as_report());
                tracing::warn!(message);
                self.latest_job_info
                    .store(Arc::new((job_id, BackupJobStatus::Failed, message)));
            }
        }
    }

    async fn take_job_handle_by_job_id(&self, job_id: u64) -> Option<BackupJobHandle> {
        let mut guard = self.running_job_handle.lock().await;
        match (*guard).as_ref() {
            None => {
                return None;
            }
            Some(job_handle) => {
                if job_handle.job_id != job_id {
                    return None;
                }
            }
        }
        guard.take()
    }

    /// Deletes existent backups from backup storage.
    pub async fn delete_backups(&self, ids: &[MetaSnapshotId]) -> MetaResult<()> {
        self.backup_store.load().0.delete(ids).await?;
        self.env
            .notification_manager()
            .notify_hummock_without_version(
                Operation::Update,
                Info::MetaBackupManifestId(MetaBackupManifestId {
                    id: self.backup_store.load().0.manifest().await.manifest_id,
                }),
            );
        Ok(())
    }

    /// List id of all objects required by backups.
    pub async fn list_pinned_object_ids(&self) -> HashSet<HummockRawObjectId> {
        self.backup_store
            .load()
            .0
            .manifest()
            .await
            .snapshot_metadata
            .iter()
            .flat_map(|s| s.objects.iter().copied())
            .collect()
    }

    pub async fn manifest(&self) -> Arc<MetaSnapshotManifest> {
        self.backup_store.load().0.manifest().await
    }
}

/// `BackupWorker` creates a database snapshot.
struct BackupWorker {
    backup_manager: BackupManagerRef,
}

impl BackupWorker {
    fn new(backup_manager: BackupManagerRef) -> Self {
        Self { backup_manager }
    }

    fn start(self, job_id: u64, remarks: Option<String>) -> JoinHandle<()> {
        let backup_manager_clone = self.backup_manager.clone();
        let job = async move {
            let hummock_manager = backup_manager_clone.hummock_manager.clone();
            let hummock_version_builder = async move {
                hummock_manager
                    .on_current_version(|version| version.clone())
                    .await
            };
            let meta_store = backup_manager_clone.env.meta_store();
            let mut snapshot_builder =
                meta_snapshot_builder::MetaSnapshotV2Builder::new(meta_store);
            // Reuse job id as snapshot id.
            snapshot_builder
                .build(job_id, hummock_version_builder)
                .await?;
            let snapshot = snapshot_builder.finish()?;
            backup_manager_clone
                .backup_store
                .load()
                .0
                .create(&snapshot, remarks)
                .await?;
            Ok(BackupJobResult::Succeeded)
        };
        tokio::spawn(async move {
            let job_result = job.await.unwrap_or_else(BackupJobResult::Failed);
            self.backup_manager
                .finish_backup_job(job_id, job_result)
                .await;
        })
    }
}

async fn create_snapshot_store(
    config: &StoreConfig,
    metric: Arc<ObjectStoreMetrics>,
    object_store_config: &ObjectStoreConfig,
) -> MetaResult<ObjectStoreMetaSnapshotStorage> {
    let object_store = Arc::new(
        build_remote_object_store(
            &config.0,
            metric,
            "Meta Backup",
            Arc::new(object_store_config.clone()),
            "",
        )
        .await,
    );
    let store = ObjectStoreMetaSnapshotStorage::new(&config.1, object_store).await?;
    Ok(store)
}
