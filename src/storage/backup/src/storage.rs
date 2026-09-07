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

use itertools::Itertools;
use risingwave_common::config::ObjectStoreConfig;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{
    InMemObjectStore, MonitoredObjectStore, MonitoredStreamingReader, ObjectError, ObjectStoreImpl,
    ObjectStoreRef,
};
use tokio::sync::RwLock;

use crate::meta_snapshot::{MetaSnapshot, Metadata};
use crate::{
    BackupError, BackupResult, MetaSnapshotId, MetaSnapshotManifest, MetaSnapshotMetadata,
};

pub type MetaSnapshotStorageRef = Arc<ObjectStoreMetaSnapshotStorage>;

#[async_trait::async_trait]
pub trait MetaSnapshotStorage: 'static + Sync + Send {
    /// Creates a snapshot.
    async fn create<S: Metadata>(
        &self,
        snapshot: &MetaSnapshot<S>,
        remarks: Option<String>,
    ) -> BackupResult<()>;

    /// Gets a snapshot by id.
    async fn get<S: Metadata>(&self, id: MetaSnapshotId) -> BackupResult<MetaSnapshot<S>>;

    /// Gets encoded snapshot bytes stream by id.
    async fn get_bytes_stream(&self, id: MetaSnapshotId) -> BackupResult<MonitoredStreamingReader>;

    /// Gets local snapshot manifest.
    async fn manifest(&self) -> Arc<MetaSnapshotManifest>;

    /// Refreshes local snapshot manifest.
    async fn refresh_manifest(&self) -> BackupResult<()>;

    /// Deletes snapshots by ids.
    async fn delete(&self, ids: &[MetaSnapshotId]) -> BackupResult<()>;
}

#[derive(Clone)]
pub struct ObjectStoreMetaSnapshotStorage {
    path: String,
    store: ObjectStoreRef,
    manifest: Arc<RwLock<Arc<MetaSnapshotManifest>>>,
}

// TODO #6482: purge stale snapshots that is not in manifest.
impl ObjectStoreMetaSnapshotStorage {
    pub async fn new(path: &str, store: ObjectStoreRef) -> BackupResult<Self> {
        let instance = Self {
            path: path.to_owned(),
            store,
            manifest: Default::default(),
        };
        instance.refresh_manifest().await?;
        Ok(instance)
    }

    async fn update_manifest(
        &self,
        update: impl FnOnce(MetaSnapshotManifest) -> MetaSnapshotManifest,
    ) -> BackupResult<()> {
        let mut guard = self.manifest.write().await;
        let new_manifest = update((**guard).clone());
        let bytes =
            serde_json::to_vec(&new_manifest).map_err(|e| BackupError::Encoding(e.into()))?;
        self.store
            .upload(&self.get_manifest_path(), bytes.into())
            .await?;
        *guard = Arc::new(new_manifest);
        Ok(())
    }

    async fn get_manifest(&self) -> BackupResult<Option<MetaSnapshotManifest>> {
        let manifest_path = self.get_manifest_path();
        let bytes = match self.store.read(&manifest_path, ..).await {
            Ok(bytes) => bytes,
            Err(e) => {
                if e.is_object_not_found_error() {
                    return Ok(None);
                }
                return Err(e.into());
            }
        };
        let manifest: MetaSnapshotManifest =
            serde_json::from_slice(&bytes).map_err(|e| BackupError::Encoding(e.into()))?;
        Ok(Some(manifest))
    }

    fn get_manifest_path(&self) -> String {
        format!("{}/manifest.json", self.path)
    }

    fn get_snapshot_path(&self, id: MetaSnapshotId) -> String {
        format!("{}/{}.snapshot", self.path, id)
    }

    #[expect(dead_code)]
    fn get_snapshot_id_from_path(path: &str) -> MetaSnapshotId {
        let split = path.split(&['/', '.']).collect_vec();
        debug_assert!(split.len() > 2);
        debug_assert!(split[split.len() - 1] == "snapshot");
        split[split.len() - 2]
            .parse::<MetaSnapshotId>()
            .expect("valid meta snapshot id")
    }
}

#[async_trait::async_trait]
impl MetaSnapshotStorage for ObjectStoreMetaSnapshotStorage {
    async fn create<S: Metadata>(
        &self,
        snapshot: &MetaSnapshot<S>,
        remarks: Option<String>,
    ) -> BackupResult<()> {
        let path = self.get_snapshot_path(snapshot.id);
        let uploader = self.store.streaming_upload(&path).await?;
        snapshot.encode_to_uploader(uploader).await?;
        self.update_manifest(|mut manifest: MetaSnapshotManifest| {
            manifest.manifest_id += 1;
            manifest.snapshot_metadata.push(MetaSnapshotMetadata::new(
                snapshot.id,
                snapshot.metadata.hummock_version_ref(),
                snapshot.format_version,
                remarks,
                snapshot.metadata.table_change_log_object_ids().into_iter(),
            ));
            manifest
        })
        .await?;
        Ok(())
    }

    async fn get<S: Metadata>(&self, id: MetaSnapshotId) -> BackupResult<MetaSnapshot<S>> {
        let reader = self.get_bytes_stream(id).await?;
        MetaSnapshot::decode_from_stream(reader).await
    }

    async fn get_bytes_stream(&self, id: MetaSnapshotId) -> BackupResult<MonitoredStreamingReader> {
        let path = self.get_snapshot_path(id);
        Ok(self.store.streaming_read(&path, ..).await?)
    }

    async fn manifest(&self) -> Arc<MetaSnapshotManifest> {
        self.manifest.read().await.clone()
    }

    async fn refresh_manifest(&self) -> BackupResult<()> {
        if let Some(manifest) = self.get_manifest().await? {
            let mut guard = self.manifest.write().await;
            if manifest.manifest_id > guard.manifest_id {
                *guard = Arc::new(manifest);
            }
        }
        Ok(())
    }

    async fn delete(&self, ids: &[MetaSnapshotId]) -> BackupResult<()> {
        let to_delete: HashSet<MetaSnapshotId> = HashSet::from_iter(ids.iter().cloned());
        self.update_manifest(|mut manifest: MetaSnapshotManifest| {
            manifest.manifest_id += 1;
            manifest
                .snapshot_metadata
                .retain(|m| !to_delete.contains(&m.id));
            manifest
        })
        .await?;
        let paths = ids
            .iter()
            .map(|id| self.get_snapshot_path(*id))
            .collect_vec();
        self.store.delete_objects(&paths).await?;
        Ok(())
    }
}

impl From<ObjectError> for BackupError {
    fn from(e: ObjectError) -> Self {
        BackupError::BackupStorage(e.into())
    }
}

// #[cfg(test)]
pub async fn unused() -> ObjectStoreMetaSnapshotStorage {
    ObjectStoreMetaSnapshotStorage::new(
        "",
        Arc::new(ObjectStoreImpl::InMem(MonitoredObjectStore::new(
            InMemObjectStore::for_test(),
            Arc::new(ObjectStoreMetrics::unused()),
            Arc::new(ObjectStoreConfig::default()),
        ))),
    )
    .await
    .unwrap()
}

#[cfg(test)]
mod tests {
    use risingwave_hummock_sdk::HummockVersionId;
    use risingwave_meta_model::hummock_sequence;

    use super::{MetaSnapshotStorage, unused};
    use crate::meta_snapshot::MetaSnapshot;
    use crate::meta_snapshot_v2::{MetadataV2, decode_hummock_sequences_from_stream};

    #[tokio::test]
    async fn test_create_v2_snapshot_with_streaming_upload() {
        let storage = unused().await;
        let mut metadata = MetadataV2::default();
        metadata.hummock_version.id = HummockVersionId::new(321);
        let snapshot = MetaSnapshot {
            format_version: 2,
            id: 123,
            metadata,
        };

        storage.create(&snapshot, None).await.unwrap();
        let decoded: MetaSnapshot<MetadataV2> = storage.get(snapshot.id).await.unwrap();

        assert_eq!(snapshot.format_version, decoded.format_version);
        assert_eq!(snapshot.id, decoded.id);
        assert_eq!(
            snapshot.metadata.hummock_version.id,
            decoded.metadata.hummock_version.id
        );
    }

    #[tokio::test]
    async fn test_decode_hummock_sequences_with_streaming_read() {
        let storage = unused().await;
        let snapshot = MetaSnapshot {
            format_version: 2,
            id: 456,
            metadata: MetadataV2 {
                hummock_sequences: vec![
                    hummock_sequence::Model {
                        name: "meta_backup".to_owned(),
                        seq: 42,
                    },
                    hummock_sequence::Model {
                        name: "sstable_object".to_owned(),
                        seq: 100,
                    },
                ],
                ..Default::default()
            },
        };

        storage.create(&snapshot, None).await.unwrap();
        let decoded = decode_hummock_sequences_from_stream(
            storage.get_bytes_stream(snapshot.id).await.unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(decoded, snapshot.metadata.hummock_sequences);
    }
}
