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

use std::sync::Arc;

use risingwave_backup::error::{BackupError, BackupResult};
use risingwave_backup::storage::{MetaSnapshotStorageRef, ObjectStoreMetaSnapshotStorage};
use risingwave_common::config::{MetaBackend, MetaStoreConfig, ObjectStoreConfig};
use risingwave_object_store::object::build_remote_object_store;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;

use crate::MetaStoreBackend;
use crate::backup_restore::RestoreOpts;
use crate::controller::SqlMetaStore;

// Code is copied from src/meta/src/rpc/server.rs. TODO #6482: extract method.
pub async fn get_meta_store(opts: RestoreOpts) -> BackupResult<SqlMetaStore> {
    let meta_store_backend = match opts.meta_store_type {
        MetaBackend::Mem => MetaStoreBackend::Mem,
        MetaBackend::Sql => MetaStoreBackend::Sql {
            endpoint: opts.sql_endpoint,
            config: MetaStoreConfig::default(),
        },
        MetaBackend::Sqlite => MetaStoreBackend::Sql {
            endpoint: format!("sqlite://{}?mode=rwc", opts.sql_endpoint),
            config: MetaStoreConfig::default(),
        },
        MetaBackend::Postgres => MetaStoreBackend::Sql {
            endpoint: format!(
                "postgres://{}:{}@{}/{}{}",
                opts.sql_username,
                opts.sql_password,
                opts.sql_endpoint,
                opts.sql_database,
                if let Some(params) = &opts.sql_url_params
                    && !params.is_empty()
                {
                    format!("?{}", params)
                } else {
                    "".to_owned()
                }
            ),
            config: MetaStoreConfig::default(),
        },
        MetaBackend::Mysql => MetaStoreBackend::Sql {
            endpoint: format!(
                "mysql://{}:{}@{}/{}{}",
                opts.sql_username,
                opts.sql_password,
                opts.sql_endpoint,
                opts.sql_database,
                if let Some(params) = &opts.sql_url_params
                    && !params.is_empty()
                {
                    format!("?{}", params)
                } else {
                    "".to_owned()
                }
            ),
            config: MetaStoreConfig::default(),
        },
    };

    SqlMetaStore::connect(meta_store_backend)
        .await
        .map_err(|e| BackupError::MetaStorage(e.into()))
}

pub async fn get_backup_store(opts: RestoreOpts) -> BackupResult<MetaSnapshotStorageRef> {
    let mut config = ObjectStoreConfig::default();
    config.retry.read_attempt_timeout_ms = opts.read_attempt_timeout_ms;
    config.retry.read_retry_attempts = opts.read_retry_attempts as usize;

    let object_store = build_remote_object_store(
        &opts.backup_storage_url,
        Arc::new(ObjectStoreMetrics::unused()),
        "Meta Backup",
        Arc::new(config),
        "",
    )
    .await;
    let backup_store =
        ObjectStoreMetaSnapshotStorage::new(&opts.backup_storage_directory, Arc::new(object_store))
            .await?;
    Ok(Arc::new(backup_store))
}
