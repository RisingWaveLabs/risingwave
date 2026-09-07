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

use anyhow::anyhow;
use futures::TryStreamExt;
use risingwave_backup::MetaSnapshotId;
use risingwave_backup::error::{BackupError, BackupResult};
use risingwave_backup::meta_snapshot::Metadata;
use risingwave_backup::storage::{MetaSnapshotStorage, MetaSnapshotStorageRef};
use risingwave_common::config::{MetaBackend, ObjectStoreConfig};
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{
    HummockRawObjectId, try_get_object_id_from_path, version_checkpoint_path,
};
use risingwave_object_store::object::build_remote_object_store;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_pb::hummock::{PbHummockVersionCheckpoint, PbHummockVersionCheckpointEnvelope};
use thiserror_ext::AsReport;

use crate::backup_restore::restore_impl::v2::{LoaderV2, WriterModelV2ToMetaStoreV2};
use crate::backup_restore::restore_impl::{Loader, Writer};
use crate::backup_restore::utils::{get_backup_store, get_meta_store};
use crate::controller::SqlMetaStore;
use crate::hummock::compress_payload;

/// Command-line arguments for restore.
#[derive(clap::Args, Debug, Clone)]
pub struct RestoreOpts {
    /// Id of snapshot used to restore. Available snapshots can be found in
    /// <`storage_directory>/manifest.json`.
    #[clap(long)]
    pub meta_snapshot_id: u64,
    /// Type of meta store to restore.
    #[clap(long, value_enum, default_value_t = MetaBackend::Mem)]
    pub meta_store_type: MetaBackend,
    #[clap(long, default_value_t = String::from(""))]
    pub sql_endpoint: String,
    /// Username of sql backend, required when meta backend set to MySQL or PostgreSQL.
    #[clap(long, default_value = "")]
    pub sql_username: String,
    /// Password of sql backend, required when meta backend set to MySQL or PostgreSQL.
    #[clap(long, default_value = "")]
    pub sql_password: String,
    /// Database of sql backend, required when meta backend set to MySQL or PostgreSQL.
    #[clap(long, default_value = "")]
    pub sql_database: String,
    /// Params for the URL connection, such as `sslmode=disable`.
    /// Example: `param1=value1&param2=value2`
    #[clap(long, required = false)]
    pub sql_url_params: Option<String>,
    /// Url of storage to fetch meta snapshot from.
    #[clap(long)]
    pub backup_storage_url: String,
    /// Directory of storage to fetch meta snapshot from.
    #[clap(long, default_value_t = String::from("backup"))]
    pub backup_storage_directory: String,
    /// Url of storage to restore hummock version to.
    #[clap(long)]
    pub hummock_storage_url: String,
    /// Directory of storage to restore hummock version to.
    #[clap(long, default_value_t = String::from("hummock_001"))]
    pub hummock_storage_directory: String,
    /// Print the target snapshot, but won't restore to meta store.
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
    /// The read timeout for object store
    #[clap(long, default_value_t = 600000)]
    pub read_attempt_timeout_ms: u64,
    /// The maximum number of read retry attempts for the object store.
    #[clap(long, default_value_t = 3)]
    pub read_retry_attempts: u64,
    #[clap(long, default_value_t = false)]
    /// When enabled, some system parameters of in the restored meta store will be overwritten.
    /// Specifically, system parameters `state_store`, `data_directory`, `backup_storage_url` and `backup_storage_directory` will be overwritten
    /// with the specified opts `hummock_storage_url`, `hummock_storage_directory`, `overwrite_backup_storage_url` and `overwrite_backup_storage_directory`.
    pub overwrite_hummock_storage_endpoint: bool,
    #[clap(long, required = false)]
    pub overwrite_backup_storage_url: Option<String>,
    #[clap(long, required = false)]
    pub overwrite_backup_storage_directory: Option<String>,
    /// Verify that all referenced objects exist in object store.
    #[clap(long, default_value_t = false)]
    pub validate_integrity: bool,
}

async fn restore_hummock_version(
    hummock_storage_url: &str,
    hummock_storage_directory: &str,
    hummock_version: &HummockVersion,
) -> BackupResult<()> {
    let object_store = Arc::new(
        build_remote_object_store(
            hummock_storage_url,
            Arc::new(ObjectStoreMetrics::unused()),
            "Version Checkpoint",
            Arc::new(ObjectStoreConfig::default()),
            "",
        )
        .await,
    );
    let checkpoint_path = version_checkpoint_path(hummock_storage_directory);
    let checkpoint = PbHummockVersionCheckpoint {
        version: Some(hummock_version.into()),
        // Ignore stale objects. Full GC will clear them.
        stale_objects: Default::default(),
    };
    use anyhow::Context;
    use prost::Message;
    use risingwave_common::config::CheckpointCompression;

    // Use zstd compression by default. The next checkpoint written by the cluster
    // will use the configured compression algorithm from the config file.
    let compression = CheckpointCompression::Zstd;
    let raw_bytes = checkpoint.encode_to_vec();
    let compressed = compress_payload(compression, &raw_bytes)
        .context("zstd compression failed")
        .map_err(BackupError::Other)?;
    let checksum = crate::hummock::xxhash64_checksum(&compressed);
    let envelope = PbHummockVersionCheckpointEnvelope {
        compression_algorithm: compression as i32,
        payload: compressed,
        checksum: Some(checksum),
    };
    let buf = envelope.encode_to_vec();
    object_store
        .upload(&checkpoint_path, buf.into())
        .await
        .map_err(|e| BackupError::StateStorage(e.into()))?;
    Ok(())
}

/// Restores a meta store.
/// Uses `meta_store` and `backup_store` if provided.
/// Otherwise creates them based on `opts`.
async fn restore_impl(
    opts: RestoreOpts,
    meta_store: Option<SqlMetaStore>,
    backup_store: Option<MetaSnapshotStorageRef>,
) -> BackupResult<()> {
    if cfg!(not(test)) {
        assert!(meta_store.is_none());
        assert!(backup_store.is_none());
    }
    let meta_store = match meta_store {
        None => get_meta_store(opts.clone()).await?,
        Some(m) => m,
    };
    let backup_store = match backup_store {
        None => get_backup_store(opts.clone()).await?,
        Some(b) => b,
    };
    let target_id = opts.meta_snapshot_id;
    let snapshot_list = &backup_store.manifest().await.snapshot_metadata;
    let snapshot = match snapshot_list.iter().find(|m| m.id == target_id) {
        None => {
            return Err(BackupError::Other(anyhow::anyhow!(
                "snapshot id {} not found",
                target_id
            )));
        }
        Some(s) => s,
    };

    if opts.validate_integrity {
        tracing::info!("Start integrity validation.");
        validate_integrity(
            snapshot.objects.clone(),
            &opts.hummock_storage_url,
            &opts.hummock_storage_directory,
        )
        .await
        .inspect_err(|_| tracing::error!("Fail integrity validation."))?;
        tracing::info!("Succeed integrity validation.");
    }

    let format_version = snapshot.format_version;
    if format_version < 2 {
        unimplemented!("not supported: write model V1 to meta store V2");
    } else {
        dispatch(
            target_id,
            &opts,
            LoaderV2::new(backup_store),
            WriterModelV2ToMetaStoreV2::new(meta_store),
        )
        .await?;
    }

    Ok(())
}

async fn dispatch<L: Loader<S>, W: Writer<S>, S: Metadata>(
    target_id: MetaSnapshotId,
    opts: &RestoreOpts,
    loader: L,
    writer: W,
) -> BackupResult<()> {
    // Validate parameters.
    if opts.overwrite_hummock_storage_endpoint
        && (opts.overwrite_backup_storage_url.is_none()
            || opts.overwrite_backup_storage_directory.is_none())
    {
        return Err(BackupError::Other(anyhow::anyhow!("overwrite_hummock_storage_endpoint, overwrite_backup_storage_url, overwrite_backup_storage_directory must be set simultaneously".to_owned())));
    }

    // Restore meta store.
    let target_snapshot = loader.load(target_id).await?;
    if !opts.overwrite_hummock_storage_endpoint {
        let storage_url = target_snapshot.metadata.storage_url()?;
        if storage_url != opts.hummock_storage_url {
            return Err(BackupError::Other(anyhow::anyhow!(
                "storage_url mismatch: {} {}",
                storage_url,
                opts.hummock_storage_url
            )));
        }
        let storage_directory = target_snapshot.metadata.storage_directory()?;
        if storage_directory != opts.hummock_storage_directory {
            return Err(BackupError::Other(anyhow::anyhow!(
                "storage_directory mismatch: {} {}",
                storage_directory,
                opts.hummock_storage_directory
            )));
        }
    }

    if opts.dry_run {
        tracing::info!("Complete dry run.");
        return Ok(());
    }
    let hummock_version = target_snapshot.metadata.hummock_version_ref().clone();
    writer.write(target_snapshot).await?;
    if opts.overwrite_hummock_storage_endpoint {
        writer
            .overwrite(
                &format!("hummock+{}", opts.hummock_storage_url),
                &opts.hummock_storage_directory,
                opts.overwrite_backup_storage_url.as_ref().unwrap(),
                opts.overwrite_backup_storage_directory.as_ref().unwrap(),
            )
            .await?;
    }

    // Restore object store.
    restore_hummock_version(
        &opts.hummock_storage_url,
        &opts.hummock_storage_directory,
        &hummock_version,
    )
    .await?;
    Ok(())
}

pub async fn restore(opts: RestoreOpts) -> BackupResult<()> {
    tracing::info!("restore with opts: {:#?}", opts);
    let result = restore_impl(opts, None, None).await;
    match &result {
        Ok(_) => {
            tracing::info!("command succeeded");
        }
        Err(e) => {
            tracing::warn!(error = %e.as_report(), "command failed");
        }
    }
    result
}

async fn validate_integrity(
    mut object_ids: HashSet<HummockRawObjectId>,
    hummock_storage_url: &str,
    hummock_storage_directory: &str,
) -> BackupResult<()> {
    tracing::info!("expect {} objects", object_ids.len());
    let object_store = Arc::new(
        build_remote_object_store(
            hummock_storage_url,
            Arc::new(ObjectStoreMetrics::unused()),
            "Version Checkpoint",
            Arc::new(ObjectStoreConfig::default()),
            "",
        )
        .await,
    );
    let mut iter = object_store
        .list(hummock_storage_directory, None, None)
        .await?;
    while let Some(obj) = iter.try_next().await? {
        let Some(obj_id) = try_get_object_id_from_path(&obj.key) else {
            continue;
        };
        if object_ids.remove(&obj_id.as_raw()) && object_ids.is_empty() {
            break;
        }
    }
    if object_ids.is_empty() {
        return Ok(());
    }
    Err(BackupError::Other(anyhow!(
        "referenced objects not found in object store: {:?}",
        object_ids
    )))
}
