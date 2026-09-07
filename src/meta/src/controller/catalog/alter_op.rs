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

use anyhow::Context;
use risingwave_common::cast::datetime_to_timestamp_millis;
use risingwave_common::catalog::{AlterDatabaseParam, ICEBERG_SINK_PREFIX, ICEBERG_SOURCE_PREFIX};
use risingwave_common::config::mutate::TomlTableMutateExt as _;
use risingwave_common::config::{StreamingConfig, merge_streaming_config_section};
use risingwave_common::id::JobId;
use risingwave_common::system_param::{OverrideValidate, Validate};
use risingwave_common::util::worker_util::DEFAULT_RESOURCE_GROUP;
use risingwave_meta_model::refresh_job::{self, RefreshState};
use risingwave_pb::catalog::subscription::PbSubscriptionState;
use sea_orm::ActiveValue::{NotSet, Set};
use sea_orm::prelude::DateTime;
use sea_orm::sea_query::Expr;
use sea_orm::{ActiveModelTrait, ConnectionTrait, DatabaseTransaction, SqlErr};
use thiserror_ext::AsReport;

use super::*;
use crate::controller::utils::load_streaming_jobs_by_ids;
use crate::error::bail_invalid_parameter;

impl CatalogController {
    async fn alter_database_name(
        &self,
        database_id: DatabaseId,
        name: &str,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        check_database_name_duplicate(name, &txn).await?;

        let active_model = database::ActiveModel {
            database_id: Set(database_id),
            name: Set(name.to_owned()),
            ..Default::default()
        };
        let database = active_model.update(&txn).await?;

        let obj = Object::find_by_id(database_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("database", database_id))?;

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::Database(ObjectModel(database, obj, None).into()),
            )
            .await;
        Ok(version)
    }

    async fn alter_schema_name(
        &self,
        schema_id: SchemaId,
        name: &str,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let obj = Object::find_by_id(schema_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("schema", schema_id))?;
        check_schema_name_duplicate(name, obj.database_id.unwrap(), &txn).await?;

        let active_model = schema::ActiveModel {
            schema_id: Set(schema_id),
            name: Set(name.to_owned()),
        };
        let schema = active_model.update(&txn).await?;

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::Schema(ObjectModel(schema, obj, None).into()),
            )
            .await;
        Ok(version)
    }

    pub async fn alter_name(
        &self,
        object_type: ObjectType,
        object_id: impl Into<ObjectId>,
        object_name: &str,
    ) -> MetaResult<NotificationVersion> {
        let object_id = object_id.into();
        if object_type == ObjectType::Database {
            return self
                .alter_database_name(object_id.as_database_id(), object_name)
                .await;
        } else if object_type == ObjectType::Schema {
            return self
                .alter_schema_name(object_id.as_schema_id(), object_name)
                .await;
        }

        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let obj: PartialObject = Object::find_by_id(object_id)
            .into_partial_model()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(object_type.as_str(), object_id))?;
        assert_eq!(obj.obj_type, object_type);
        check_relation_name_duplicate(
            object_name,
            obj.database_id.unwrap(),
            obj.schema_id.unwrap(),
            &txn,
        )
        .await?;

        // rename relation.
        let (mut to_update_relations, old_name) =
            rename_relation(&txn, object_type, object_id, object_name).await?;
        // rename referring relation name.
        to_update_relations.extend(
            rename_relation_refer(&txn, object_type, object_id, object_name, &old_name).await?,
        );

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects: to_update_relations,
                    dependencies: vec![],
                }),
            )
            .await;

        Ok(version)
    }

    pub async fn alter_swap_rename(
        &self,
        object_type: ObjectType,
        object_id: ObjectId,
        dst_object_id: ObjectId,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let dst_name: String = match object_type {
            ObjectType::Table => Table::find_by_id(dst_object_id.as_table_id())
                .select_only()
                .column(table::Column::Name)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| {
                    MetaError::catalog_id_not_found(object_type.as_str(), dst_object_id)
                })?,
            ObjectType::Source => Source::find_by_id(dst_object_id.as_source_id())
                .select_only()
                .column(source::Column::Name)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| {
                    MetaError::catalog_id_not_found(object_type.as_str(), dst_object_id)
                })?,
            ObjectType::Sink => Sink::find_by_id(dst_object_id.as_sink_id())
                .select_only()
                .column(sink::Column::Name)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| {
                    MetaError::catalog_id_not_found(object_type.as_str(), dst_object_id)
                })?,
            ObjectType::View => View::find_by_id(dst_object_id.as_view_id())
                .select_only()
                .column(view::Column::Name)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| {
                    MetaError::catalog_id_not_found(object_type.as_str(), dst_object_id)
                })?,
            ObjectType::Subscription => {
                Subscription::find_by_id(dst_object_id.as_subscription_id())
                    .select_only()
                    .column(subscription::Column::Name)
                    .into_tuple()
                    .one(&txn)
                    .await?
                    .ok_or_else(|| {
                        MetaError::catalog_id_not_found(object_type.as_str(), dst_object_id)
                    })?
            }
            _ => {
                return Err(MetaError::permission_denied(format!(
                    "swap rename not supported for object type: {:?}",
                    object_type
                )));
            }
        };

        // rename relations.
        let (mut to_update_relations, src_name) =
            rename_relation(&txn, object_type, object_id, &dst_name).await?;
        let (to_update_relations2, _) =
            rename_relation(&txn, object_type, dst_object_id, &src_name).await?;
        to_update_relations.extend(to_update_relations2);
        // rename referring relation name.
        to_update_relations.extend(
            rename_relation_refer(&txn, object_type, object_id, &dst_name, &src_name).await?,
        );
        to_update_relations.extend(
            rename_relation_refer(&txn, object_type, dst_object_id, &src_name, &dst_name).await?,
        );

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects: to_update_relations,
                    dependencies: vec![],
                }),
            )
            .await;

        Ok(version)
    }

    pub async fn alter_non_shared_source(
        &self,
        pb_source: PbSource,
    ) -> MetaResult<NotificationVersion> {
        let source_id: SourceId = pb_source.id;
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let original_version: i64 = Source::find_by_id(source_id)
            .select_only()
            .column(source::Column::Version)
            .into_tuple()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("source", source_id))?;
        if original_version + 1 != pb_source.version as i64 {
            return Err(MetaError::permission_denied(
                "source version is stale".to_owned(),
            ));
        }

        let source: source::ActiveModel = pb_source.clone().into();
        Source::update(source).exec(&txn).await?;
        txn.commit().await?;

        let version = self
            .notify_frontend_relation_info(
                NotificationOperation::Update,
                PbObjectInfo::Source(pb_source),
            )
            .await;
        Ok(version)
    }

    pub async fn alter_owner(
        &self,
        object_type: ObjectType,
        object_id: ObjectId,
        new_owner: UserId,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        ensure_user_id(new_owner, &txn).await?;

        let obj = Object::find_by_id(object_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(object_type.as_str(), object_id))?;
        if obj.owner_id == new_owner {
            return Ok(IGNORED_NOTIFICATION_VERSION);
        }
        let mut obj = obj.into_active_model();
        obj.owner_id = Set(new_owner);
        let obj = obj.update(&txn).await?;

        match object_type {
            ObjectType::Database => {
                let db = Database::find_by_id(object_id.as_database_id())
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("database", object_id))?;

                txn.commit().await?;

                let version = self
                    .notify_frontend(
                        NotificationOperation::Update,
                        NotificationInfo::Database(ObjectModel(db, obj, None).into()),
                    )
                    .await;
                return Ok(version);
            }
            ObjectType::Schema => {
                let schema = Schema::find_by_id(object_id.as_schema_id())
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("schema", object_id))?;

                txn.commit().await?;

                let version = self
                    .notify_frontend(
                        NotificationOperation::Update,
                        NotificationInfo::Schema(ObjectModel(schema, obj, None).into()),
                    )
                    .await;
                return Ok(version);
            }
            _ => {}
        }

        // Transfer the ownership of the companion objects together, and collect all the
        // affected objects.
        let mut updated_oids = vec![object_id];
        match object_type {
            ObjectType::Table => {
                let table = Table::find_by_id(object_id.as_table_id())
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("table", object_id))?;

                // associated source.
                if let Some(associated_source_id) = table.optional_associated_source_id {
                    object::ActiveModel {
                        oid: Set(associated_source_id.as_object_id()),
                        owner_id: Set(new_owner),
                        ..Default::default()
                    }
                    .update(&txn)
                    .await?;
                    updated_oids.push(associated_source_id.as_object_id());
                }

                // associated sink and source for iceberg table.
                if matches!(table.engine, Some(table::Engine::Iceberg)) {
                    let iceberg_sink = Sink::find()
                        .inner_join(Object)
                        .select_only()
                        .column(sink::Column::SinkId)
                        .filter(
                            object::Column::DatabaseId
                                .eq(obj.database_id)
                                .and(object::Column::SchemaId.eq(obj.schema_id))
                                .and(
                                    sink::Column::Name
                                        .eq(format!("{}{}", ICEBERG_SINK_PREFIX, table.name)),
                                ),
                        )
                        .into_tuple::<SinkId>()
                        .one(&txn)
                        .await?
                        .expect("iceberg sink must exist");
                    let iceberg_source = Source::find()
                        .inner_join(Object)
                        .select_only()
                        .column(source::Column::SourceId)
                        .filter(
                            object::Column::DatabaseId
                                .eq(obj.database_id)
                                .and(object::Column::SchemaId.eq(obj.schema_id))
                                .and(
                                    source::Column::Name
                                        .eq(format!("{}{}", ICEBERG_SOURCE_PREFIX, table.name)),
                                ),
                        )
                        .into_tuple::<SourceId>()
                        .one(&txn)
                        .await?
                        .expect("iceberg source must exist");
                    Self::transfer_objects_owner(
                        &txn,
                        new_owner,
                        vec![iceberg_sink.as_object_id(), iceberg_source.as_object_id()],
                        &mut updated_oids,
                    )
                    .await?;
                }

                // indexes, their storage tables, and the internal tables.
                let (index_ids, mut table_ids): (Vec<IndexId>, Vec<TableId>) = Index::find()
                    .select_only()
                    .columns([index::Column::IndexId, index::Column::IndexTableId])
                    .filter(index::Column::PrimaryTableId.eq(object_id))
                    .into_tuple::<(IndexId, TableId)>()
                    .all(&txn)
                    .await?
                    .into_iter()
                    .unzip();

                let internal_tables: Vec<TableId> = Table::find()
                    .select_only()
                    .column(table::Column::TableId)
                    .filter(
                        table::Column::BelongsToJobId.is_in(
                            table_ids
                                .iter()
                                .cloned()
                                .chain(std::iter::once(object_id.as_table_id())),
                        ),
                    )
                    .into_tuple()
                    .all(&txn)
                    .await?;
                table_ids.extend(internal_tables);

                Self::transfer_objects_owner(
                    &txn,
                    new_owner,
                    index_ids
                        .into_iter()
                        .map(|id| id.as_object_id())
                        .chain(table_ids.into_iter().map(|id| id.as_object_id()))
                        .collect(),
                    &mut updated_oids,
                )
                .await?;
            }
            ObjectType::Source => {
                let source = Source::find_by_id(object_id.as_source_id())
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("source", object_id))?;

                // Note: For non-shared source, we don't update their state tables, which
                // belongs to the MV.
                if source.is_shared() {
                    let internal_tables =
                        get_internal_tables_by_id(object_id.as_job_id(), &txn).await?;
                    Self::transfer_objects_owner(
                        &txn,
                        new_owner,
                        internal_tables
                            .into_iter()
                            .map(|id| id.as_object_id())
                            .collect(),
                        &mut updated_oids,
                    )
                    .await?;
                }
            }
            ObjectType::Sink => {
                let internal_tables =
                    get_internal_tables_by_id(object_id.as_job_id(), &txn).await?;
                Self::transfer_objects_owner(
                    &txn,
                    new_owner,
                    internal_tables
                        .into_iter()
                        .map(|id| id.as_object_id())
                        .collect(),
                    &mut updated_oids,
                )
                .await?;
            }
            ObjectType::Subscription
            | ObjectType::View
            | ObjectType::Connection
            | ObjectType::Function
            | ObjectType::Secret => {}
            _ => unreachable!("not supported object type: {:?}", object_type),
        };

        let objects = Self::build_updated_object_infos(&txn, updated_oids).await?;

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects: objects
                        .into_iter()
                        .map(|object| PbObject {
                            object_info: Some(object),
                        })
                        .collect(),
                    dependencies: vec![],
                }),
            )
            .await;
        Ok(version)
    }

    /// `REASSIGN OWNED BY`: transfer ownership of all objects in the given database owned by
    /// `old_owner_ids` to `new_owner`, along with `Database` objects they own (following the
    /// "shared objects" semantics of PostgreSQL). Companion objects (e.g. indexes and their
    /// storage tables, internal state tables) share their host object's owner by construction,
    /// so they are covered by the owned set itself and need no expansion like `alter_owner`.
    pub async fn reassign_owned(
        &self,
        old_owner_ids: Vec<UserId>,
        new_owner: UserId,
        database_id: DatabaseId,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        ensure_user_id(new_owner, &txn).await?;
        for user_id in &old_owner_ids {
            ensure_user_id(*user_id, &txn).await?;
        }

        let owned_objects: Vec<(ObjectId, ObjectType)> = Object::find()
            .select_only()
            .columns([object::Column::Oid, object::Column::ObjType])
            .filter(
                object::Column::OwnerId.is_in(old_owner_ids.clone()).and(
                    object::Column::DatabaseId
                        .eq(database_id)
                        .or(object::Column::ObjType.eq(ObjectType::Database)),
                ),
            )
            .into_tuple()
            .all(&txn)
            .await?;
        if owned_objects.is_empty() {
            return Ok(IGNORED_NOTIFICATION_VERSION);
        }

        // Frontends are not notified of some in-creation objects (e.g. foreground
        // `CREATE TABLE` / `CREATE INDEX`, and subscriptions whose log store is still
        // being created), so an `Update` notification for them would panic the frontend
        // observer.
        let creating =
            StreamingJob::find()
                .filter(streaming_job::Column::JobStatus.ne(JobStatus::Created).and(
                    streaming_job::Column::JobId.is_in(owned_objects.iter().map(|(oid, _)| *oid)),
                ))
                .count(&txn)
                .await?
                + Subscription::find()
                    .filter(
                        subscription::Column::SubscriptionState
                            .ne(PbSubscriptionState::Created as i32)
                            .and(
                                subscription::Column::SubscriptionId
                                    .is_in(owned_objects.iter().map(|(oid, _)| *oid)),
                            ),
                    )
                    .count(&txn)
                    .await?;
        if creating != 0 {
            return Err(MetaError::permission_denied(format!(
                "cannot reassign {creating} object(s) that are still being created, \
                 please cancel them or wait for the creation to complete first"
            )));
        }

        Object::update_many()
            .col_expr(object::Column::OwnerId, SimpleExpr::Value(new_owner.into()))
            .filter(object::Column::Oid.is_in(owned_objects.iter().map(|(oid, _)| *oid)))
            .exec(&txn)
            .await?;

        let mut databases = vec![];
        let mut schemas = vec![];
        let mut group_oids = vec![];
        for (oid, obj_type) in owned_objects {
            match obj_type {
                ObjectType::Database => {
                    let (db, obj) = Database::find_by_id(oid.as_database_id())
                        .find_also_related(Object)
                        .one(&txn)
                        .await?
                        .ok_or_else(|| MetaError::catalog_id_not_found("database", oid))?;
                    databases.push(PbDatabase::from(ObjectModel(db, obj.unwrap(), None)));
                }
                ObjectType::Schema => {
                    let (schema, obj) = Schema::find_by_id(oid.as_schema_id())
                        .find_also_related(Object)
                        .one(&txn)
                        .await?
                        .ok_or_else(|| MetaError::catalog_id_not_found("schema", oid))?;
                    schemas.push(PbSchema::from(ObjectModel(schema, obj.unwrap(), None)));
                }
                _ => group_oids.push(oid),
            }
        }
        let objects = Self::build_updated_object_infos(&txn, group_oids).await?;

        txn.commit().await?;

        let mut version = IGNORED_NOTIFICATION_VERSION;
        for db in databases {
            version = self
                .notify_frontend(
                    NotificationOperation::Update,
                    NotificationInfo::Database(db),
                )
                .await;
        }
        for schema in schemas {
            version = self
                .notify_frontend(
                    NotificationOperation::Update,
                    NotificationInfo::Schema(schema),
                )
                .await;
        }
        if !objects.is_empty() {
            version = self
                .notify_frontend(
                    NotificationOperation::Update,
                    NotificationInfo::ObjectGroup(PbObjectGroup {
                        objects: objects
                            .into_iter()
                            .map(|object| PbObject {
                                object_info: Some(object),
                            })
                            .collect(),
                        dependencies: vec![],
                    }),
                )
                .await;
        }
        Ok(version)
    }

    async fn transfer_objects_owner(
        txn: &DatabaseTransaction,
        new_owner: UserId,
        oids: Vec<ObjectId>,
        updated_oids: &mut Vec<ObjectId>,
    ) -> MetaResult<()> {
        if oids.is_empty() {
            return Ok(());
        }
        Object::update_many()
            .col_expr(object::Column::OwnerId, SimpleExpr::Value(new_owner.into()))
            .filter(object::Column::Oid.is_in(oids.iter().copied()))
            .exec(txn)
            .await?;
        updated_oids.extend(oids);
        Ok(())
    }

    /// Load the catalogs of the given objects, whose metadata was just updated in the
    /// transaction, and build the object infos for an `Update` notification. The storage
    /// tables of the involved indexes are included automatically, before the indexes, as
    /// the frontend derives an index's metadata (e.g. owner) from its storage table.
    /// `Database` and `Schema` objects are not supported and must be notified individually
    /// by the caller.
    async fn build_updated_object_infos(
        txn: &DatabaseTransaction,
        object_ids: Vec<ObjectId>,
    ) -> MetaResult<Vec<PbObjectInfo>> {
        let updated_objs: Vec<object::Model> = Object::find()
            .filter(object::Column::Oid.is_in(object_ids))
            .all(txn)
            .await?;
        let oids_of = |object_type: ObjectType| {
            updated_objs
                .iter()
                .filter(|obj| obj.obj_type == object_type)
                .map(|obj| obj.oid)
                .collect_vec()
        };
        debug_assert!(
            oids_of(ObjectType::Database).is_empty() && oids_of(ObjectType::Schema).is_empty()
        );

        let mut objects = vec![];
        let index_objs = Index::find()
            .find_also_related(Object)
            .filter(
                index::Column::IndexId.is_in(
                    oids_of(ObjectType::Index)
                        .into_iter()
                        .map(|oid| oid.as_index_id()),
                ),
            )
            .all(txn)
            .await?;
        let index_table_ids = index_objs
            .iter()
            .map(|(index, _)| index.index_table_id)
            .collect_vec();
        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(
                table::Column::TableId.is_in(
                    oids_of(ObjectType::Table)
                        .into_iter()
                        .map(|oid| oid.as_table_id())
                        .chain(index_table_ids),
                ),
            )
            .all(txn)
            .await?;
        let streaming_jobs =
            load_streaming_jobs_by_ids(txn, table_objs.iter().map(|(table, _)| table.job_id()))
                .await?;
        for (table, obj) in table_objs {
            let streaming_job = streaming_jobs.get(&table.job_id()).cloned();
            objects.push(PbObjectInfo::Table(
                ObjectModel(table, obj.unwrap(), streaming_job).into(),
            ));
        }

        let source_objs = Source::find()
            .find_also_related(Object)
            .filter(
                source::Column::SourceId.is_in(
                    oids_of(ObjectType::Source)
                        .into_iter()
                        .map(|oid| oid.as_source_id()),
                ),
            )
            .all(txn)
            .await?;
        for (source, obj) in source_objs {
            objects.push(PbObjectInfo::Source(
                ObjectModel(source, obj.unwrap(), None).into(),
            ));
        }

        let sink_objs = Sink::find()
            .find_also_related(Object)
            .filter(
                sink::Column::SinkId.is_in(
                    oids_of(ObjectType::Sink)
                        .into_iter()
                        .map(|oid| oid.as_sink_id()),
                ),
            )
            .all(txn)
            .await?;
        let streaming_jobs = load_streaming_jobs_by_ids(
            txn,
            sink_objs.iter().map(|(sink, _)| sink.sink_id.as_job_id()),
        )
        .await?;
        for (sink, obj) in sink_objs {
            let streaming_job = streaming_jobs.get(&sink.sink_id.as_job_id()).cloned();
            objects.push(PbObjectInfo::Sink(
                ObjectModel(sink, obj.unwrap(), streaming_job).into(),
            ));
        }

        // FIXME: frontend will update index/primary table from cache, requires apply updates of indexes after tables.
        let streaming_jobs = load_streaming_jobs_by_ids(
            txn,
            index_objs
                .iter()
                .map(|(index, _)| index.index_id.as_job_id()),
        )
        .await?;
        for (index, obj) in index_objs {
            let streaming_job = streaming_jobs.get(&index.index_id.as_job_id()).cloned();
            objects.push(PbObjectInfo::Index(
                ObjectModel(index, obj.unwrap(), streaming_job).into(),
            ));
        }

        let view_objs = View::find()
            .find_also_related(Object)
            .filter(
                view::Column::ViewId.is_in(
                    oids_of(ObjectType::View)
                        .into_iter()
                        .map(|oid| oid.as_view_id()),
                ),
            )
            .all(txn)
            .await?;
        for (view, obj) in view_objs {
            objects.push(PbObjectInfo::View(
                ObjectModel(view, obj.unwrap(), None).into(),
            ));
        }

        let subscription_objs = Subscription::find()
            .find_also_related(Object)
            .filter(
                subscription::Column::SubscriptionId.is_in(
                    oids_of(ObjectType::Subscription)
                        .into_iter()
                        .map(|oid| oid.as_subscription_id()),
                ),
            )
            .all(txn)
            .await?;
        for (subscription, obj) in subscription_objs {
            objects.push(PbObjectInfo::Subscription(
                ObjectModel(subscription, obj.unwrap(), None).into(),
            ));
        }

        let function_objs = Function::find()
            .find_also_related(Object)
            .filter(
                function::Column::FunctionId.is_in(
                    oids_of(ObjectType::Function)
                        .into_iter()
                        .map(|oid| oid.as_function_id()),
                ),
            )
            .all(txn)
            .await?;
        for (function, obj) in function_objs {
            objects.push(PbObjectInfo::Function(
                ObjectModel(function, obj.unwrap(), None).into(),
            ));
        }

        let connection_objs = Connection::find()
            .find_also_related(Object)
            .filter(
                connection::Column::ConnectionId.is_in(
                    oids_of(ObjectType::Connection)
                        .into_iter()
                        .map(|oid| oid.as_connection_id()),
                ),
            )
            .all(txn)
            .await?;
        for (connection, obj) in connection_objs {
            objects.push(PbObjectInfo::Connection(
                ObjectModel(connection, obj.unwrap(), None).into(),
            ));
        }

        let secret_objs = Secret::find()
            .find_also_related(Object)
            .filter(
                secret::Column::SecretId.is_in(
                    oids_of(ObjectType::Secret)
                        .into_iter()
                        .map(|oid| oid.as_secret_id()),
                ),
            )
            .all(txn)
            .await?;
        for (secret, obj) in secret_objs {
            objects.push(PbObjectInfo::Secret(
                ObjectModel(secret, obj.unwrap(), None).into(),
            ));
        }

        Ok(objects)
    }

    pub async fn alter_schema(
        &self,
        object_type: ObjectType,
        object_id: ObjectId,
        new_schema: SchemaId,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        ensure_object_id(ObjectType::Schema, new_schema, &txn).await?;

        let obj = Object::find_by_id(object_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(object_type.as_str(), object_id))?;
        if obj.obj_type != object_type {
            return Err(MetaError::catalog_id_not_found(
                object_type.as_str(),
                object_id,
            ));
        }
        if object_type == ObjectType::Table {
            let table_type = Table::find_by_id(object_id.as_table_id())
                .select_only()
                .column(table::Column::TableType)
                .into_tuple::<TableType>()
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("table", object_id))?;
            if table_type == TableType::Internal {
                return Err(MetaError::catalog_id_not_found("table", object_id));
            }
        }
        if obj.schema_id == Some(new_schema) {
            return Ok(IGNORED_NOTIFICATION_VERSION);
        }
        let database_id = obj
            .database_id
            .ok_or_else(|| anyhow!("catalog object {} has no database", object_id))?;

        // Indexes are named schema objects rather than objects belonging to their primary table.
        // Move them with a table explicitly, while subscriptions remain in their own schemas.
        let mut objects = vec![obj];
        if object_type == ObjectType::Table {
            let index_ids = Index::find()
                .select_only()
                .column(index::Column::IndexId)
                .filter(index::Column::PrimaryTableId.eq(object_id.as_table_id()))
                .into_tuple::<IndexId>()
                .all(&txn)
                .await?;
            objects.extend(
                Object::find()
                    .filter(
                        object::Column::Oid
                            .is_in(index_ids.into_iter().map(|id| id.as_object_id())),
                    )
                    .all(&txn)
                    .await?,
            );
        }

        let object_ids = objects.iter().map(|object| object.oid).collect_vec();
        let belonging_objects = get_belong_objects_by_ids(&txn, object_ids.iter().copied()).await?;
        objects.extend(belonging_objects.iter().cloned());
        let mut object_models = load_object_models(&txn, &objects).await?;

        prepare_object_models_for_schema_change(&txn, &mut object_models, database_id, new_schema)
            .await?;
        Object::update_many()
            .col_expr(object::Column::SchemaId, new_schema.into())
            .col_expr(
                object::Column::BelongToOid,
                new_schema.as_object_id().into(),
            )
            .filter(object::Column::Oid.is_in(object_ids))
            .exec(&txn)
            .await?;
        if !belonging_objects.is_empty() {
            Object::update_many()
                .col_expr(object::Column::SchemaId, new_schema.into())
                .filter(
                    object::Column::Oid.is_in(belonging_objects.iter().map(|object| object.oid)),
                )
                .exec(&txn)
                .await?;
        }
        let notification = NotificationInfo::ObjectGroup(PbObjectGroup {
            objects: object_models
                .into_iter()
                .map(|object_info| PbObject {
                    object_info: Some(object_info),
                })
                .collect(),
            dependencies: vec![],
        });

        txn.commit().await?;
        let version = self
            .notify_frontend(NotificationOperation::Update, notification)
            .await;
        Ok(version)
    }

    pub async fn alter_secret(
        &self,
        pb_secret: PbSecret,
        secret_plain_payload: Vec<u8>,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = pb_secret.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_secret.database_id, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_secret.schema_id, &txn).await?;

        ensure_object_id(ObjectType::Secret, pb_secret.id, &txn).await?;
        let secret: secret::ActiveModel = pb_secret.clone().into();
        Secret::update(secret).exec(&txn).await?;

        txn.commit().await?;

        // Notify the compute and frontend node plain secret
        let mut secret_plain = pb_secret;
        secret_plain.value.clone_from(&secret_plain_payload);

        LocalSecretManager::global().update_secret(secret_plain.id, secret_plain_payload);
        self.env
            .notification_manager()
            .notify_compute_without_version(Operation::Update, Info::Secret(secret_plain.clone()));

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::Secret(secret_plain),
            )
            .await;

        Ok(version)
    }

    // drop table associated source is a special case of drop relation, which just remove the source object and associated state table, keeping the streaming job and fragments.
    pub async fn drop_table_associated_source(
        txn: &DatabaseTransaction,
        drop_table_connector_ctx: &DropTableConnectorContext,
    ) -> MetaResult<(Vec<PbUserInfo>, Vec<PartialObject>)> {
        let to_drop_source_objects: Vec<PartialObject> = Object::find()
            .filter(object::Column::Oid.is_in(vec![drop_table_connector_ctx.to_remove_source_id]))
            .into_partial_model()
            .all(txn)
            .await?;
        let to_drop_internal_table_objs: Vec<PartialObject> = Object::find()
            .select_only()
            .filter(
                object::Column::Oid.is_in(vec![drop_table_connector_ctx.to_remove_state_table_id]),
            )
            .into_partial_model()
            .all(txn)
            .await?;
        let to_drop_objects = to_drop_source_objects
            .into_iter()
            .chain(to_drop_internal_table_objs)
            .collect_vec();
        // Find affect users with privileges on all this objects.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Oid.is_in(to_drop_objects.iter().map(|obj| obj.oid)))
            .into_tuple()
            .all(txn)
            .await?;

        tracing::debug!(
            "drop_table_associated_source: to_drop_objects: {:?}",
            to_drop_objects
        );

        // delete all in to_drop_objects.
        let res = Object::delete_many()
            .filter(object::Column::Oid.is_in(to_drop_objects.iter().map(|obj| obj.oid)))
            .exec(txn)
            .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found(
                ObjectType::Source.as_str(),
                drop_table_connector_ctx.to_remove_source_id,
            ));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, txn).await?;

        Ok((user_infos, to_drop_objects))
    }

    pub async fn alter_database_param(
        &self,
        database_id: DatabaseId,
        param: AlterDatabaseParam,
    ) -> MetaResult<(NotificationVersion, risingwave_meta_model::database::Model)> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let mut database = database::ActiveModel {
            database_id: Set(database_id),
            ..Default::default()
        };
        match param {
            AlterDatabaseParam::BarrierIntervalMs(interval) => {
                if let Some(ref interval) = interval {
                    OverrideValidate::barrier_interval_ms(interval)
                        .map_err(|e| anyhow::anyhow!(e))?;
                }
                database.barrier_interval_ms = Set(interval.map(|i| i as i32));
            }
            AlterDatabaseParam::CheckpointFrequency(frequency) => {
                if let Some(ref frequency) = frequency {
                    OverrideValidate::checkpoint_frequency(frequency)
                        .map_err(|e| anyhow::anyhow!(e))?;
                }
                database.checkpoint_frequency = Set(frequency.map(|f| f as i64));
            }
        }
        let database = database.update(&txn).await?;

        let obj = Object::find_by_id(database_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("database", database_id))?;

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::Database(ObjectModel(database.clone(), obj, None).into()),
            )
            .await;
        Ok((version, database))
    }

    pub async fn alter_database_resource_group(
        &self,
        database_id: DatabaseId,
        resource_group: Option<String>,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let database =
            database::ActiveModel {
                database_id: Set(database_id),
                resource_group: Set(
                    resource_group.unwrap_or_else(|| DEFAULT_RESOURCE_GROUP.to_owned())
                ),
                ..Default::default()
            }
            .update(&txn)
            .await?;

        let obj = Object::find_by_id(database_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("database", database_id))?;

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::Database(ObjectModel(database, obj, None).into()),
            )
            .await;
        Ok(version)
    }

    pub async fn alter_subscription_retention(
        &self,
        subscription_id: SubscriptionId,
        retention_seconds: u64,
        definition: String,
    ) -> MetaResult<(NotificationVersion, PbSubscription)> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let obj = Object::find_by_id(subscription_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("subscription", subscription_id))?;

        let active_model = subscription::ActiveModel {
            subscription_id: Set(subscription_id),
            retention_seconds: Set(retention_seconds as i64),
            definition: Set(definition),
            ..Default::default()
        };
        let subscription = active_model.update(&txn).await?;

        txn.commit().await?;

        let pb_subscription: PbSubscription = ObjectModel(subscription, obj, None).into();
        let subscription_info = PbObjectInfo::Subscription(pb_subscription.clone());

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects: vec![PbObject {
                        object_info: Some(subscription_info),
                    }],
                    dependencies: vec![],
                }),
            )
            .await;

        Ok((version, pb_subscription))
    }

    pub async fn alter_streaming_job_config(
        &self,
        job_id: JobId,
        entries_to_add: HashMap<String, String>,
        keys_to_remove: Vec<String>,
    ) -> MetaResult<NotificationVersion> {
        let updates_cache_refill_policy = entries_to_add
            .contains_key(STREAMING_CACHE_REFILL_POLICY_CONFIG_PATH)
            || keys_to_remove
                .iter()
                .any(|key| key == STREAMING_CACHE_REFILL_POLICY_CONFIG_PATH);

        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let config_override: Option<String> = StreamingJob::find_by_id(job_id)
            .select_only()
            .column(streaming_job::Column::ConfigOverride)
            .into_tuple()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", job_id))?;
        let config_override = config_override.unwrap_or_default();

        let mut table: toml::Table =
            toml::from_str(&config_override).context("invalid streaming job config")?;

        // The frontend guarantees that there's no duplicated keys in `to_add` and `to_remove`.
        for (key, value) in entries_to_add {
            let value: toml::Value = value
                .parse()
                .with_context(|| format!("invalid config value for path {key}"))?;
            table
                .upsert(&key, value)
                .with_context(|| format!("failed to set config path {key}"))?;
        }
        for key in keys_to_remove {
            table
                .delete(&key)
                .with_context(|| format!("failed to reset config path {key}"))?;
        }

        let updated_config_override = table.to_string();

        // Validate the config override by trying to merge it to the default config.
        let merged =
            merge_streaming_config_section(&StreamingConfig::default(), &updated_config_override)
                .context("invalid streaming job config override")?;

        // Reject unrecognized entries.
        // Note: If these unrecognized entries are pre-existing, we also reject them here.
        // Users are able to fix them by issuing a `RESET` first.
        if let Some(merged) = &merged {
            let unrecognized_keys = merged.unrecognized_keys().collect_vec();
            if !unrecognized_keys.is_empty() {
                bail_invalid_parameter!("unrecognized configs: {:?}", unrecognized_keys);
            }
        }

        StreamingJob::update(streaming_job::ActiveModel {
            job_id: Set(job_id),
            config_override: Set(Some(updated_config_override)),
            ..Default::default()
        })
        .exec(&txn)
        .await?;

        txn.commit().await?;
        drop(inner);

        if updates_cache_refill_policy {
            let policies = self.table_cache_refill_policies_snapshot().await?;
            self.env
                .notification_manager()
                .notify_hummock(
                    NotificationOperation::Update,
                    NotificationInfo::TableRefillRuntimeConfig(PbTableRefillRuntimeConfig {
                        table_cache_refill_policies: Some(policies),
                        ..Default::default()
                    }),
                )
                .await;
        }

        Ok(IGNORED_NOTIFICATION_VERSION)
    }

    pub async fn ensure_refresh_job(&self, table_id: TableId) -> MetaResult<()> {
        let inner = self.inner.read().await;
        let active = refresh_job::ActiveModel {
            table_id: Set(table_id),
            last_trigger_time: Set(None),
            trigger_interval_secs: Set(None),
            current_status: Set(RefreshState::Idle),
            last_success_time: Set(None),
        };
        match RefreshJob::insert(active)
            .on_conflict_do_nothing()
            .exec(&inner.db)
            .await
        {
            Ok(_) => Ok(()),
            Err(sea_orm::DbErr::RecordNotInserted) => {
                // This is expected when the refresh job already exists due to ON CONFLICT DO NOTHING
                tracing::debug!("refresh job already exists for table_id={}", table_id);
                Ok(())
            }
            Err(e) => {
                if should_skip_refresh_job_db_err(&inner.db, table_id, &e).await? {
                    tracing::warn!(
                        %table_id,
                        error = %e.as_report(),
                        "skip ensure_refresh_job for stale dropped table"
                    );
                    Ok(())
                } else {
                    Err(e.into())
                }
            }
        }
    }

    pub async fn update_refresh_job_status(
        &self,
        table_id: TableId,
        status: RefreshState,
        trigger_time: Option<DateTime>,
        is_success: bool,
    ) -> MetaResult<()> {
        self.ensure_refresh_job(table_id).await?;
        let inner = self.inner.read().await;

        // expect only update trigger_time when the status changes to Refreshing
        assert_eq!(trigger_time.is_some(), status == RefreshState::Refreshing);
        let active = refresh_job::ActiveModel {
            table_id: Set(table_id),
            current_status: Set(status),
            last_trigger_time: if trigger_time.is_some() {
                Set(trigger_time.map(datetime_to_timestamp_millis))
            } else {
                NotSet
            },
            last_success_time: if is_success {
                Set(Some(chrono::Utc::now().timestamp_millis()))
            } else {
                NotSet
            },
            ..Default::default()
        };
        match RefreshJob::update(active).exec(&inner.db).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if should_skip_refresh_job_db_err(&inner.db, table_id, &e).await? {
                    tracing::warn!(
                        %table_id,
                        error = %e.as_report(),
                        "skip update_refresh_job_status for stale dropped table"
                    );
                    Ok(())
                } else {
                    Err(e.into())
                }
            }
        }
    }

    pub async fn reset_all_refresh_jobs_to_idle(&self) -> MetaResult<()> {
        let inner = self.inner.read().await;
        RefreshJob::update_many()
            .col_expr(
                refresh_job::Column::CurrentStatus,
                Expr::value(RefreshState::Idle),
            )
            .exec(&inner.db)
            .await?;
        Ok(())
    }

    pub async fn update_refresh_job_interval(
        &self,
        table_id: TableId,
        trigger_interval_secs: Option<i64>,
    ) -> MetaResult<()> {
        self.ensure_refresh_job(table_id).await?;
        let inner = self.inner.read().await;
        let active = refresh_job::ActiveModel {
            table_id: Set(table_id),
            trigger_interval_secs: Set(trigger_interval_secs),
            ..Default::default()
        };
        RefreshJob::update(active).exec(&inner.db).await?;
        Ok(())
    }
}

async fn should_skip_refresh_job_db_err<C>(
    db: &C,
    table_id: TableId,
    err: &sea_orm::DbErr,
) -> MetaResult<bool>
where
    C: ConnectionTrait,
{
    if matches!(err, sea_orm::DbErr::RecordNotUpdated) {
        return Ok(true);
    }

    if !matches!(
        err.sql_err(),
        Some(SqlErr::ForeignKeyConstraintViolation(_))
    ) {
        return Ok(false);
    }

    let table_exists = Table::find_by_id(table_id).one(db).await?.is_some();
    Ok(!table_exists)
}
