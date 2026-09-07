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

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use futures::FutureExt;
use futures::future::Shared;
use risingwave_backup::error::BackupError;
use risingwave_backup::meta_snapshot::{MetaSnapshot, Metadata};
use risingwave_backup::storage::{MetaSnapshotStorage, ObjectStoreMetaSnapshotStorage};
use risingwave_backup::{MetaSnapshotId, meta_snapshot_v1, meta_snapshot_v2};
use risingwave_common::catalog::TableId;
use risingwave_common::config::ObjectStoreConfig;
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_object_store::object::build_remote_object_store;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use thiserror_ext::AsReport;

use crate::error::{StorageError, StorageResult};
use crate::hummock::HummockError;
use crate::hummock::local_version::pinned_version::{PinVersionAction, PinnedVersion};

pub type BackupReaderRef = Arc<BackupReader>;

type VersionHolder = (
    PinnedVersion,
    tokio::sync::mpsc::UnboundedReceiver<PinVersionAction>,
);

async fn create_snapshot_store(
    config: &StoreConfig,
    object_store_config: &ObjectStoreConfig,
) -> StorageResult<ObjectStoreMetaSnapshotStorage> {
    let backup_object_store = Arc::new(
        build_remote_object_store(
            &config.0,
            Arc::new(ObjectStoreMetrics::unused()),
            "Meta Backup",
            Arc::new(object_store_config.clone()),
            "",
        )
        .await,
    );
    let store = ObjectStoreMetaSnapshotStorage::new(&config.1, backup_object_store).await?;
    Ok(store)
}

type InflightRequest = Shared<Pin<Box<dyn Future<Output = Result<PinnedVersion, String>> + Send>>>;
/// (url, dir)
type StoreConfig = (String, String);
/// `BackupReader` helps to access historical hummock versions,
/// which are persisted in meta snapshots (aka backups).
pub struct BackupReader {
    versions: parking_lot::RwLock<HashMap<MetaSnapshotId, VersionHolder>>,
    inflight_request: parking_lot::Mutex<HashMap<MetaSnapshotId, InflightRequest>>,
    store: ArcSwap<(ObjectStoreMetaSnapshotStorage, StoreConfig)>,
    refresh_tx: tokio::sync::mpsc::UnboundedSender<u64>,
    object_store_config: ObjectStoreConfig,
}

impl BackupReader {
    pub async fn new(
        storage_url: &str,
        storage_directory: &str,
        object_store_config: &ObjectStoreConfig,
    ) -> StorageResult<BackupReaderRef> {
        let config = (storage_url.to_owned(), storage_directory.to_owned());
        let store = create_snapshot_store(&config, object_store_config).await?;
        tracing::info!(
            "backup reader is initialized: url={}, dir={}",
            config.0,
            config.1
        );
        Ok(Self::with_store(
            (store, config),
            object_store_config.clone(),
        ))
    }

    fn with_store(
        store: (ObjectStoreMetaSnapshotStorage, StoreConfig),
        object_store_config: ObjectStoreConfig,
    ) -> BackupReaderRef {
        let (refresh_tx, refresh_rx) = tokio::sync::mpsc::unbounded_channel();
        let instance = Arc::new(Self {
            store: ArcSwap::from_pointee(store),
            versions: Default::default(),
            inflight_request: Default::default(),
            object_store_config,
            refresh_tx,
        });
        tokio::spawn(Self::start_manifest_refresher(instance.clone(), refresh_rx));
        instance
    }

    pub async fn unused() -> BackupReaderRef {
        Self::with_store(
            (
                risingwave_backup::storage::unused().await,
                StoreConfig::default(),
            ),
            ObjectStoreConfig::default(),
        )
    }

    async fn set_store(&self, config: StoreConfig) -> StorageResult<()> {
        let new_store = create_snapshot_store(&config, &self.object_store_config).await?;
        tracing::info!(
            "backup reader is updated: url={}, dir={}",
            config.0,
            config.1
        );
        self.store.store(Arc::new((new_store, config)));
        Ok(())
    }

    /// Watches latest manifest id to keep local manifest update to date.
    async fn start_manifest_refresher(
        backup_reader: BackupReaderRef,
        mut refresh_rx: tokio::sync::mpsc::UnboundedReceiver<u64>,
    ) {
        loop {
            let expect_manifest_id = refresh_rx.recv().await;
            if expect_manifest_id.is_none() {
                break;
            }
            let expect_manifest_id = expect_manifest_id.unwrap();
            // Use the same store throughout one run.
            let current_store = backup_reader.store.load_full();
            let previous_id = current_store.0.manifest().await.manifest_id;
            if expect_manifest_id <= previous_id {
                continue;
            }
            if let Err(e) = current_store.0.refresh_manifest().await {
                // reschedule refresh request
                tracing::warn!(error = %e.as_report(), "failed to refresh backup manifest, will retry");
                let backup_reader_clone = backup_reader.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    backup_reader_clone.try_refresh_manifest(expect_manifest_id);
                });
                continue;
            }
            // purge stale version cache
            let manifest: HashSet<MetaSnapshotId> = current_store
                .0
                .manifest()
                .await
                .snapshot_metadata
                .iter()
                .map(|s| s.id)
                .collect();
            backup_reader
                .versions
                .write()
                .retain(|k, _v| manifest.contains(k));
        }
    }

    pub fn try_refresh_manifest(self: &BackupReaderRef, min_manifest_id: u64) {
        let _ = self.refresh_tx.send(min_manifest_id).inspect_err(|_| {
            tracing::warn!(min_manifest_id, "failed to send refresh_manifest request")
        });
    }

    /// Tries to get a hummock version eligible for querying `epoch`.
    /// SSTs of the returned version are expected to be guarded by corresponding backup.
    /// Otherwise, reading the version may encounter object store error, due to SST absence.
    pub async fn try_get_hummock_version(
        self: &BackupReaderRef,
        table_id: TableId,
        epoch: u64,
    ) -> StorageResult<Option<PinnedVersion>> {
        // Use the same store throughout the call.
        let current_store = self.store.load_full();
        // 1. check manifest to locate snapshot, if any.
        let Some(snapshot_metadata) = current_store
            .0
            .manifest()
            .await
            .snapshot_metadata
            .iter()
            .find(|v| {
                if let Some(m) = v.state_table_info.get(&table_id) {
                    return epoch == m.committed_epoch;
                }
                false
            })
            .cloned()
        else {
            return Ok(None);
        };
        let snapshot_id = snapshot_metadata.id;
        // 2. load hummock version of chosen snapshot.
        let future = {
            let mut req_guard = self.inflight_request.lock();
            if let Some((v, _)) = self.versions.read().get(&snapshot_id) {
                return Ok(Some(v.clone()));
            }
            if let Some(f) = req_guard.get(&snapshot_id) {
                f.clone()
            } else {
                let this = self.clone();
                let f = async move {
                    let to_not_found_error = |e: BackupError| {
                        format!(
                            "failed to get meta snapshot {}: {}",
                            snapshot_id,
                            e.as_report()
                        )
                    };
                    let version_holder = if snapshot_metadata.format_version < 2 {
                        let snapshot: meta_snapshot_v1::MetaSnapshotV1 = current_store
                            .0
                            .get(snapshot_id)
                            .await
                            .map_err(to_not_found_error)?;
                        build_version_holder(snapshot)
                    } else {
                        let snapshot: meta_snapshot_v2::MetaSnapshotV2 = current_store
                            .0
                            .get(snapshot_id)
                            .await
                            .map_err(to_not_found_error)?;
                        build_version_holder(snapshot)
                    };
                    let version_clone = version_holder.0.clone();
                    this.versions.write().insert(snapshot_id, version_holder);
                    Ok(version_clone)
                }
                .boxed()
                .shared();
                req_guard.insert(snapshot_id, f.clone());
                f
            }
        };
        let result = future
            .await
            .map(Some)
            .map_err(|e| HummockError::read_backup_error(e).into());
        self.inflight_request.lock().remove(&snapshot_id);
        result
    }

    pub async fn watch_config_change(
        &self,
        mut rx: tokio::sync::watch::Receiver<SystemParamsReaderRef>,
    ) {
        loop {
            if rx.changed().await.is_err() {
                break;
            }
            let p = rx.borrow().load();
            let config = (
                p.backup_storage_url().to_owned(),
                p.backup_storage_directory().to_owned(),
            );
            if config == self.store.load().1 {
                continue;
            }
            if let Err(e) = self.set_store(config.clone()).await {
                // Retry is driven by periodic system params notification.
                tracing::warn!(
                    url = config.0, dir = config.1,
                    error = %e.as_report(),
                    "failed to update backup reader",
                );
            }
        }
    }
}

fn build_version_holder<S: Metadata>(s: MetaSnapshot<S>) -> VersionHolder {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (PinnedVersion::new(s.metadata.hummock_version(), tx), rx)
}

impl From<BackupError> for StorageError {
    fn from(e: BackupError) -> Self {
        HummockError::other(e).into()
    }
}
