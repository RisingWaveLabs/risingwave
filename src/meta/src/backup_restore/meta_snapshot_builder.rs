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

use std::future::Future;

use itertools::Itertools;
use risingwave_backup::MetaSnapshotId;
use risingwave_backup::error::{BackupError, BackupResult};
use risingwave_backup::meta_snapshot_v2::{MetaSnapshotV2, MetadataV2};
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_meta_model as model;
use risingwave_pb::hummock::PbHummockVersionDelta;
use sea_orm::{DbErr, EntityTrait, QueryOrder, TransactionTrait};

use crate::controller::SqlMetaStore;

const VERSION: u32 = 2;

fn map_db_err(e: DbErr) -> BackupError {
    BackupError::MetaStorage(e.into())
}

macro_rules! define_set_metadata {
    ($( {$name:ident, $mod_path:ident::$mod_name:ident} ),*) => {
        async fn set_metadata(
            metadata: &mut MetadataV2,
            txn: &sea_orm::DatabaseTransaction,
        ) -> BackupResult<()> {
          $(
              metadata.$name = $mod_path::$mod_name::Entity::find()
                                    .all(txn)
                                    .await
                                    .map_err(map_db_err)?;
          )*
          Ok(())
        }
    };
}

risingwave_backup::for_all_metadata_models_v2!(define_set_metadata);

pub struct MetaSnapshotV2Builder {
    snapshot: MetaSnapshotV2,
    meta_store: SqlMetaStore,
}

impl MetaSnapshotV2Builder {
    pub fn new(meta_store: SqlMetaStore) -> Self {
        Self {
            snapshot: MetaSnapshotV2::default(),
            meta_store,
        }
    }

    pub async fn build(
        &mut self,
        id: MetaSnapshotId,
        hummock_version_builder: impl Future<Output = HummockVersion>,
    ) -> BackupResult<()> {
        self.snapshot.format_version = VERSION;
        self.snapshot.id = id;
        // Get `hummock_version` before `meta_store_snapshot`.
        // We have ensure the required delta logs for replay is available, see
        // `HummockManager::delete_version_deltas`.
        let hummock_version = hummock_version_builder.await;
        let txn = self
            .meta_store
            .conn
            .begin_with_config(
                Some(sea_orm::IsolationLevel::Serializable),
                Some(sea_orm::AccessMode::ReadOnly),
            )
            .await
            .map_err(map_db_err)?;
        let version_deltas = model::prelude::HummockVersionDelta::find()
            .order_by_asc(model::hummock_version_delta::Column::Id)
            .all(&txn)
            .await
            .map_err(map_db_err)?
            .into_iter()
            .map_into::<PbHummockVersionDelta>()
            .map(HummockVersionDelta::from_persisted_protobuf_owned);
        let hummock_version = {
            let mut redo_state = hummock_version;
            let mut max_log_id = None;
            for version_delta in version_deltas {
                if version_delta.prev_id == redo_state.id {
                    redo_state.apply_version_delta(&version_delta);
                }
                max_log_id = Some(version_delta.id);
            }
            if let Some(max_log_id) = max_log_id
                && max_log_id != redo_state.id
            {
                return Err(BackupError::Other(anyhow::anyhow!(format!(
                    "inconsistent hummock version: expected {}, actual {}",
                    max_log_id, redo_state.id
                ))));
            }
            redo_state
        };
        let mut metadata = MetadataV2 {
            hummock_version,
            ..Default::default()
        };
        set_metadata(&mut metadata, &txn).await?;

        txn.commit().await.map_err(map_db_err)?;
        self.snapshot.metadata = metadata;
        Ok(())
    }

    pub fn finish(self) -> BackupResult<MetaSnapshotV2> {
        // Any sanity check goes here.
        Ok(self.snapshot)
    }
}
