// Copyright 2023 RisingWave Labs
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

use std::cmp;

use itertools::Itertools;
use risingwave_backup::MetaSnapshotId;
use risingwave_backup::error::{BackupError, BackupResult};
use risingwave_backup::meta_snapshot::MetaSnapshot;
use risingwave_backup::meta_snapshot_v2::{
    MetaSnapshotV2, MetadataV2, decode_hummock_sequences_from_stream,
};
use risingwave_backup::storage::{MetaSnapshotStorage, MetaSnapshotStorageRef};
use sea_orm::{DbErr, EntityTrait};

use crate::backup_restore::restore_impl::{Loader, Writer};
use crate::controller::SqlMetaStore;

pub struct LoaderV2 {
    backup_store: MetaSnapshotStorageRef,
}

impl LoaderV2 {
    pub fn new(backup_store: MetaSnapshotStorageRef) -> Self {
        Self { backup_store }
    }
}

#[async_trait::async_trait]
impl Loader<MetadataV2> for LoaderV2 {
    async fn load(&self, target_id: MetaSnapshotId) -> BackupResult<MetaSnapshot<MetadataV2>> {
        let snapshot_list = &self.backup_store.manifest().await.snapshot_metadata;
        let mut target_snapshot: MetaSnapshotV2 = self.backup_store.get(target_id).await?;
        tracing::debug!(
            "snapshot {} before rewrite:\n{}",
            target_id,
            target_snapshot
        );
        let newest_id = snapshot_list
            .iter()
            .map(|m| m.id)
            .max()
            .expect("should exist");
        assert!(
            newest_id >= target_id,
            "newest_id={}, target_id={}",
            newest_id,
            target_id
        );

        // validate and rewrite seq
        if newest_id > target_id {
            let newest_hummock_sequences = decode_hummock_sequences_from_stream(
                self.backup_store.get_bytes_stream(newest_id).await?,
            )
            .await?;
            for seq in &target_snapshot.metadata.hummock_sequences {
                let newest = newest_hummock_sequences
                    .iter()
                    .find(|s| s.name == seq.name)
                    .unwrap_or_else(|| {
                        panic!(
                            "violate superset requirement. Hummock sequence name {}",
                            seq.name
                        )
                    });
                assert!(newest.seq >= seq.seq, "violate monotonicity requirement");
            }
            target_snapshot.metadata.hummock_sequences = newest_hummock_sequences;
            tracing::info!(
                "snapshot {} is rewritten by snapshot {}:\n",
                target_id,
                newest_id,
            );
            tracing::debug!("{target_snapshot}");
        }
        Ok(target_snapshot)
    }
}

pub struct WriterModelV2ToMetaStoreV2 {
    meta_store: SqlMetaStore,
}

impl WriterModelV2ToMetaStoreV2 {
    pub fn new(meta_store: SqlMetaStore) -> Self {
        Self { meta_store }
    }
}

#[async_trait::async_trait]
impl Writer<MetadataV2> for WriterModelV2ToMetaStoreV2 {
    async fn write(&self, target_snapshot: MetaSnapshot<MetadataV2>) -> BackupResult<()> {
        let metadata = target_snapshot.metadata;
        let db = &self.meta_store.conn;
        ensure_all_meta_store_tables_are_empty(db).await?;
        insert_models(metadata.seaql_migrations.clone(), db).await?;
        insert_models(metadata.clusters.clone(), db).await?;
        insert_models(metadata.version_stats.clone(), db).await?;
        insert_models(metadata.compaction_configs.clone(), db).await?;
        insert_models(metadata.hummock_sequences.clone(), db).await?;
        insert_models(metadata.workers.clone(), db).await?;
        insert_models(metadata.worker_properties.clone(), db).await?;
        insert_models(metadata.users.clone(), db).await?;
        // The sort is required to pass table's foreign key check.
        use risingwave_meta_model::object::ObjectType;
        insert_models(
            metadata
                .objects
                .iter()
                .sorted_by(|a, b| match (a.obj_type, b.obj_type) {
                    (ObjectType::Database, ObjectType::Database) => a.oid.cmp(&b.oid),
                    (ObjectType::Database, _) => cmp::Ordering::Less,
                    (_, ObjectType::Database) => cmp::Ordering::Greater,
                    (ObjectType::Schema, ObjectType::Schema) => a.oid.cmp(&b.oid),
                    (ObjectType::Schema, _) => cmp::Ordering::Less,
                    (_, ObjectType::Schema) => cmp::Ordering::Greater,
                    (_, _) => a.oid.cmp(&b.oid),
                })
                .cloned(),
            db,
        )
        .await?;
        insert_models(
            metadata
                .user_privileges
                .iter()
                .sorted_by_key(|u| u.id)
                .cloned(),
            db,
        )
        .await?;
        insert_models(metadata.object_dependencies.clone(), db).await?;
        insert_models(metadata.databases.clone(), db).await?;
        insert_models(metadata.schemas.clone(), db).await?;
        insert_models(metadata.streaming_jobs.clone(), db).await?;
        insert_models(metadata.fragments.clone(), db).await?;
        insert_models(metadata.fragment_relation.clone(), db).await?;
        insert_models(metadata.connections.clone(), db).await?;
        insert_models(metadata.sources.clone(), db).await?;
        insert_models(metadata.tables.clone(), db).await?;
        insert_models(metadata.sinks.clone(), db).await?;
        insert_models(metadata.views.clone(), db).await?;
        insert_models(metadata.indexes.clone(), db).await?;
        insert_models(metadata.functions.clone(), db).await?;
        insert_models(metadata.system_parameters.clone(), db).await?;
        insert_models(metadata.catalog_versions.clone(), db).await?;
        insert_models(metadata.subscriptions.clone(), db).await?;
        insert_models(metadata.session_parameters.clone(), db).await?;
        insert_models(metadata.secrets.clone(), db).await?;
        insert_models(metadata.exactly_once_iceberg_sinks.clone(), db).await?;
        insert_models(metadata.iceberg_tables.clone(), db).await?;
        insert_models(metadata.iceberg_namespace_properties.clone(), db).await?;
        insert_models(metadata.user_default_privilege.clone(), db).await?;
        insert_models(metadata.fragment_splits.clone(), db).await?;
        insert_models(metadata.pending_sink_state.clone(), db).await?;
        insert_models(metadata.refresh_jobs.clone(), db).await?;
        insert_models(metadata.cdc_table_snapshot_splits.clone(), db).await?;
        insert_models(metadata.hummock_table_change_logs.clone(), db).await?;
        // update_auto_inc must be called last.
        update_auto_inc(&metadata, db).await?;
        Ok(())
    }

    async fn overwrite(
        &self,
        new_storage_url: &str,
        new_storage_dir: &str,
        new_backup_url: &str,
        new_backup_dir: &str,
    ) -> BackupResult<()> {
        let kvs = [
            ("state_store", new_storage_url),
            ("data_directory", new_storage_dir),
            ("backup_storage_url", new_backup_url),
            ("backup_storage_directory", new_backup_dir),
        ];
        for (k, v) in kvs {
            let Some(model) = risingwave_meta_model::system_parameter::Entity::find_by_id(k)
                .one(&self.meta_store.conn)
                .await
                .map_err(map_db_err)?
            else {
                return Err(BackupError::MetaStorage(
                    anyhow::anyhow!("{k} not found in system_parameter table").into(),
                ));
            };
            let mut kv: risingwave_meta_model::system_parameter::ActiveModel = model.into();
            kv.value = sea_orm::ActiveValue::Set(v.to_owned());
            risingwave_meta_model::system_parameter::Entity::update(kv)
                .exec(&self.meta_store.conn)
                .await
                .map_err(map_db_err)?;
        }
        Ok(())
    }
}

async fn ensure_all_meta_store_tables_are_empty(
    db: &impl sea_orm::ConnectionTrait,
) -> BackupResult<()> {
    macro_rules! ensure_entity_empty {
        ($($entity_mod:ident),* $(,)?) => {
            $(
                if risingwave_meta_model::$entity_mod::Entity::find()
                    .one(db)
                    .await
                    .map_err(map_db_err)?
                    .is_some()
                {
                    return Err(BackupError::NonemptyMetaStorage);
                }
            )*
        };
    }

    risingwave_meta_model::for_all_meta_model_entities!(ensure_entity_empty);
    Ok(())
}

fn map_db_err(e: DbErr) -> BackupError {
    BackupError::MetaStorage(e.into())
}

#[macro_export]
macro_rules! for_all_auto_increment {
    ($metadata:ident, $db:ident, $macro:ident) => {
        $macro! ($metadata, $db,
            {"worker", workers, worker_id},
            {"object", objects, oid},
            {"user", users, user_id},
            {"user_privilege", user_privileges, id},
            {"fragment", fragments, fragment_id},
            {"object_dependency", object_dependencies, id}
        )
    };
}

macro_rules! reset_sql_sequence {
    ($metadata:ident, $db:ident, $( {$table:expr, $model:ident, $id_field:ident} ),*) => {
        $(
        match $db.get_database_backend() {
            sea_orm::DbBackend::MySql => {
                if let Some(v) = $metadata.$model.iter().map(|w| w.$id_field + 1).max() {
                    $db.execute(sea_orm::Statement::from_string(
                        sea_orm::DatabaseBackend::MySql,
                        format!("ALTER TABLE {} AUTO_INCREMENT = {};", $table, v),
                    ))
                    .await
                    .map_err(map_db_err)?;
                }
            }
            sea_orm::DbBackend::Postgres => {
                $db.execute(sea_orm::Statement::from_string(
                    sea_orm::DatabaseBackend::Postgres,
                    format!("SELECT setval('{}_{}_seq', (SELECT MAX({}) FROM \"{}\"));", $table, stringify!($id_field), stringify!($id_field), $table),
                ))
                .await
                .map_err(map_db_err)?;
            }
            sea_orm::DbBackend::Sqlite => {}
            }
        )*
    };
}

/// Fixes `auto_increment` fields.
async fn update_auto_inc(
    metadata: &MetadataV2,
    db: &impl sea_orm::ConnectionTrait,
) -> BackupResult<()> {
    for_all_auto_increment!(metadata, db, reset_sql_sequence);
    Ok(())
}

async fn insert_models<S, A>(
    models: impl IntoIterator<Item = S>,
    db: &impl sea_orm::ConnectionTrait,
) -> BackupResult<()>
where
    S: sea_orm::ModelTrait + Sync + Send + Sized + sea_orm::IntoActiveModel<A>,
    A: sea_orm::ActiveModelTrait + sea_orm::ActiveModelBehavior + Send + Sync + From<S>,
    <<A as sea_orm::ActiveModelTrait>::Entity as sea_orm::EntityTrait>::Model:
        sea_orm::IntoActiveModel<A>,
{
    use sea_orm::EntityTrait;
    if <S as sea_orm::ModelTrait>::Entity::find()
        .one(db)
        .await
        .map_err(map_db_err)?
        .is_some()
    {
        return Err(BackupError::NonemptyMetaStorage);
    }
    for m in models {
        m.into_active_model().insert(db).await.map_err(map_db_err)?;
    }
    Ok(())
}
