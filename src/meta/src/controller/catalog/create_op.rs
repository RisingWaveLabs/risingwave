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

use risingwave_common::cast::datetime_to_timestamp_millis;
use risingwave_common::system_param::{OverrideValidate, Validate};
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::common::PbObjectType;
use risingwave_pb::meta::PbObjectDependency;

use super::*;
use crate::barrier::SnapshotBackfillInfo;

impl CatalogController {
    pub(crate) async fn create_object(
        txn: &DatabaseTransaction,
        obj_type: ObjectType,
        owner_id: UserId,
        belong_to_oid: Option<ObjectId>,
    ) -> MetaResult<object::Model> {
        let (database_id, schema_id) = match (obj_type, belong_to_oid) {
            (ObjectType::Database, None) => (None, None),
            (ObjectType::Database, Some(_)) => {
                return Err(MetaError::invalid_parameter(
                    "database object must not have a parent",
                ));
            }
            (ObjectType::Schema, Some(database_oid)) => {
                let database = Object::find_by_id(database_oid)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("database", database_oid))?;
                if database.obj_type != ObjectType::Database {
                    return Err(MetaError::invalid_parameter(
                        "schema object must belong to a database",
                    ));
                }
                (Some(database_oid.as_database_id()), None)
            }
            (ObjectType::Schema, None) => {
                return Err(MetaError::invalid_parameter(
                    "schema object must belong to a database",
                ));
            }
            (_, Some(parent_oid)) => {
                let parent = Object::find_by_id(parent_oid)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("parent object", parent_oid))?;
                if parent.obj_type == ObjectType::Schema {
                    (parent.database_id, Some(parent.oid.as_schema_id()))
                } else {
                    (parent.database_id, parent.schema_id)
                }
            }
            (_, None) => {
                return Err(MetaError::invalid_parameter(
                    "non-database object must have a parent",
                ));
            }
        };
        let active_db = object::ActiveModel {
            oid: Default::default(),
            obj_type: Set(obj_type),
            owner_id: Set(owner_id),
            schema_id: Set(schema_id),
            database_id: Set(database_id),
            belong_to_oid: Set(belong_to_oid),
            initialized_at: Default::default(),
            created_at: Default::default(),
            initialized_at_cluster_version: Set(Some(current_cluster_version())),
            created_at_cluster_version: Set(Some(current_cluster_version())),
        };
        Ok(active_db.insert(txn).await?)
    }

    pub async fn create_database(
        &self,
        db: PbDatabase,
    ) -> MetaResult<(NotificationVersion, risingwave_meta_model::database::Model)> {
        // validate first
        if let Some(ref interval) = db.barrier_interval_ms {
            OverrideValidate::barrier_interval_ms(interval).map_err(|e| anyhow::anyhow!(e))?;
        }
        if let Some(ref frequency) = db.checkpoint_frequency {
            OverrideValidate::checkpoint_frequency(frequency).map_err(|e| anyhow::anyhow!(e))?;
        }

        let inner = self.inner.write().await;
        let owner_id = db.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        check_database_name_duplicate(&db.name, &txn).await?;

        let db_obj = Self::create_object(&txn, ObjectType::Database, owner_id, None).await?;
        let mut db: database::ActiveModel = db.into();
        db.database_id = Set(db_obj.oid.as_database_id());
        let db = db.insert(&txn).await?;

        let mut schemas = vec![];
        for schema_name in iter::once(DEFAULT_SCHEMA_NAME).chain(SYSTEM_SCHEMAS) {
            let schema_obj =
                Self::create_object(&txn, ObjectType::Schema, owner_id, Some(db_obj.oid)).await?;
            let schema = schema::ActiveModel {
                schema_id: Set(schema_obj.oid.as_schema_id()),
                name: Set(schema_name.into()),
            };
            let schema = schema.insert(&txn).await?;
            schemas.push(ObjectModel(schema, schema_obj, None).into());
        }
        txn.commit().await?;

        let mut version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::Database(ObjectModel(db.clone(), db_obj, None).into()),
            )
            .await;
        for schema in schemas {
            version = self
                .notify_frontend(NotificationOperation::Add, NotificationInfo::Schema(schema))
                .await;
        }

        Ok((version, db))
    }

    pub async fn create_schema(&self, schema: PbSchema) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = schema.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, schema.database_id, &txn).await?;
        check_schema_name_duplicate(&schema.name, schema.database_id, &txn).await?;

        let schema_obj = Self::create_object(
            &txn,
            ObjectType::Schema,
            owner_id,
            Some(schema.database_id.as_object_id()),
        )
        .await?;
        let mut schema: schema::ActiveModel = schema.into();
        schema.schema_id = Set(schema_obj.oid.as_schema_id());
        let schema = schema.insert(&txn).await?;

        let updated_user_info =
            grant_default_privileges_automatically(&txn, schema_obj.oid).await?;

        txn.commit().await?;

        let mut version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::Schema(ObjectModel(schema, schema_obj, None).into()),
            )
            .await;

        // notify default privileges for schemas
        if !updated_user_info.is_empty() {
            version = self.notify_users_update(updated_user_info).await;
        }

        Ok(version)
    }

    pub async fn create_subscription_catalog(
        &self,
        pb_subscription: &mut PbSubscription,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        ensure_user_id(pb_subscription.owner as _, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_subscription.database_id, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_subscription.schema_id, &txn).await?;
        check_subscription_name_duplicate(pb_subscription, &txn).await?;

        let obj = Self::create_object(
            &txn,
            ObjectType::Subscription,
            pb_subscription.owner as _,
            Some(pb_subscription.schema_id.as_object_id()),
        )
        .await?;
        pb_subscription.id = obj.oid.as_subscription_id();
        let subscription: subscription::ActiveModel = pb_subscription.clone().into();
        Subscription::insert(subscription).exec(&txn).await?;

        // record object dependency.
        ObjectDependency::insert(object_dependency::ActiveModel {
            oid: Set(pb_subscription.dependent_table_id.into()),
            used_by: Set(pb_subscription.id.into()),
            ..Default::default()
        })
        .exec(&txn)
        .await?;
        txn.commit().await?;
        Ok(())
    }

    pub async fn create_source(
        &self,
        mut pb_source: PbSource,
        iceberg_table_id: Option<TableId>,
    ) -> MetaResult<(SourceId, NotificationVersion)> {
        let mut inner = self.inner.write().await;
        let owner_id = pb_source.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_source.database_id, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_source.schema_id, &txn).await?;
        check_relation_name_duplicate(
            &pb_source.name,
            pb_source.database_id,
            pb_source.schema_id,
            &txn,
        )
        .await?;

        // handle secret ref
        let secret_ids = get_referred_secret_ids_from_source(&pb_source)?;
        let connection_ids = get_referred_connection_ids_from_source(&pb_source);

        let source_obj = Self::create_object(
            &txn,
            ObjectType::Source,
            owner_id,
            iceberg_table_id
                .map(TableId::as_object_id)
                .or(Some(pb_source.schema_id.as_object_id())),
        )
        .await?;
        let source_id = source_obj.oid.as_source_id();
        pb_source.id = source_id;
        let source: source::ActiveModel = pb_source.clone().into();
        Source::insert(source).exec(&txn).await?;

        // add secret and connection dependency
        let dep_relation_ids = secret_ids
            .iter()
            .copied()
            .map_into()
            .chain(connection_ids.iter().copied().map_into());
        let dependencies = if !secret_ids.is_empty() || !connection_ids.is_empty() {
            ObjectDependency::insert_many(dep_relation_ids.map(|id| {
                object_dependency::ActiveModel {
                    oid: Set(id),
                    used_by: Set(source_id.as_object_id()),
                    ..Default::default()
                }
            }))
            .exec(&txn)
            .await?;
            secret_ids
                .iter()
                .map(|id| PbObjectDependency {
                    object_id: source_id.as_object_id(),
                    referenced_object_id: id.as_object_id(),
                    referenced_object_type: PbObjectType::Secret as _,
                })
                .chain(connection_ids.iter().map(|id| PbObjectDependency {
                    object_id: source_id.as_object_id(),
                    referenced_object_id: id.as_object_id(),
                    referenced_object_type: PbObjectType::Connection as _,
                }))
                .collect_vec()
        } else {
            vec![]
        };

        let mut job_notifications = vec![];
        let mut updated_user_info = vec![];
        // check if it belongs to iceberg table
        if let Some(table_id) = iceberg_table_id {
            // 1. finish iceberg table job.
            let table_notifications = self
                .finish_streaming_job_inner(&txn, table_id.as_job_id())
                .await?;
            job_notifications.push((table_id.as_job_id(), table_notifications));

            // 2. finish iceberg sink job.
            let sink_id = Sink::find()
                .select_only()
                .column(sink::Column::SinkId)
                .join(JoinType::InnerJoin, sink::Relation::Object.def())
                .filter(
                    object::Column::BelongToOid
                        .eq(table_id)
                        .and(object::Column::ObjType.eq(ObjectType::Sink)),
                )
                .into_tuple::<SinkId>()
                .one(&txn)
                .await?
                .ok_or_else(|| {
                    MetaError::catalog_id_not_found("iceberg sink for table", table_id)
                })?;
            let sink_job_id = sink_id.as_job_id();
            let sink_notifications = self.finish_streaming_job_inner(&txn, sink_job_id).await?;
            job_notifications.push((sink_job_id, sink_notifications));
        } else {
            updated_user_info = grant_default_privileges_automatically(&txn, source_id).await?;
        }

        txn.commit().await?;

        for (job_id, (op, objects, user_info, dependencies)) in job_notifications {
            let mut version = self
                .notify_frontend(
                    op,
                    NotificationInfo::ObjectGroup(PbObjectGroup {
                        objects,
                        dependencies,
                    }),
                )
                .await;
            if !user_info.is_empty() {
                version = self.notify_users_update(user_info).await;
            }
            inner
                .creating_table_finish_notifier
                .values_mut()
                .for_each(|creating_tables| {
                    if let Some(txs) = creating_tables.remove(&job_id) {
                        for tx in txs {
                            let _ = tx.send(Ok(version));
                        }
                    }
                });
        }

        let mut version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects: vec![PbObject {
                        object_info: Some(PbObjectInfo::Source(pb_source)),
                    }],
                    dependencies,
                }),
            )
            .await;

        // notify default privileges for source
        if !updated_user_info.is_empty() {
            version = self.notify_users_update(updated_user_info).await;
        }

        Ok((source_id, version))
    }

    pub async fn create_function(
        &self,
        mut pb_function: PbFunction,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = pb_function.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_function.database_id, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_function.schema_id, &txn).await?;
        check_function_signature_duplicate(&pb_function, &txn).await?;

        let function_obj = Self::create_object(
            &txn,
            ObjectType::Function,
            owner_id,
            Some(pb_function.schema_id.as_object_id()),
        )
        .await?;
        pb_function.id = function_obj.oid.as_function_id();
        pb_function.created_at_epoch = Some(
            Epoch::from_unix_millis(datetime_to_timestamp_millis(function_obj.created_at) as _).0,
        );
        pb_function.created_at_cluster_version = function_obj.created_at_cluster_version;
        let function: function::ActiveModel = pb_function.clone().into();
        Function::insert(function).exec(&txn).await?;

        let updated_user_info =
            grant_default_privileges_automatically(&txn, function_obj.oid).await?;

        txn.commit().await?;

        let mut version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::Function(pb_function),
            )
            .await;

        // notify default privileges for functions
        if !updated_user_info.is_empty() {
            version = self.notify_users_update(updated_user_info).await;
        }

        Ok(version)
    }

    pub async fn create_connection(
        &self,
        mut pb_connection: PbConnection,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = pb_connection.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_connection.database_id, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_connection.schema_id, &txn).await?;
        check_connection_name_duplicate(&pb_connection, &txn).await?;

        let mut dep_secrets: HashSet<SecretId> = HashSet::new();
        if let Some(ConnectionInfo::ConnectionParams(params)) = &pb_connection.info {
            dep_secrets.extend(
                params
                    .secret_refs
                    .values()
                    .map(|secret_ref| secret_ref.secret_id),
            );
        }

        let conn_obj = Self::create_object(
            &txn,
            ObjectType::Connection,
            owner_id,
            Some(pb_connection.schema_id.as_object_id()),
        )
        .await?;
        pb_connection.id = conn_obj.oid.as_connection_id();
        let connection: connection::ActiveModel = pb_connection.clone().into();
        Connection::insert(connection).exec(&txn).await?;

        for secret_id in &dep_secrets {
            ObjectDependency::insert(object_dependency::ActiveModel {
                oid: Set(secret_id.as_object_id()),
                used_by: Set(conn_obj.oid),
                ..Default::default()
            })
            .exec(&txn)
            .await?;
        }

        let updated_user_info = grant_default_privileges_automatically(&txn, conn_obj.oid).await?;

        txn.commit().await?;

        {
            // call meta telemetry here to report the connection creation
            report_event(
                PbTelemetryEventStage::Unspecified,
                "connection_create",
                pb_connection.get_id().as_raw_id() as i64,
                {
                    pb_connection.info.as_ref().and_then(|info| match info {
                        ConnectionInfo::ConnectionParams(params) => {
                            Some(params.connection_type().as_str_name().to_owned())
                        }
                        _ => None,
                    })
                },
                None,
                None,
            );
        }

        let mut version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects: vec![PbObject {
                        object_info: Some(PbObjectInfo::Connection(pb_connection)),
                    }],
                    dependencies: dep_secrets
                        .iter()
                        .map(|secret_id| PbObjectDependency {
                            object_id: conn_obj.oid,
                            referenced_object_id: secret_id.as_object_id(),
                            referenced_object_type: PbObjectType::Secret as _,
                        })
                        .collect(),
                }),
            )
            .await;

        // notify default privileges for connections
        if !updated_user_info.is_empty() {
            version = self.notify_users_update(updated_user_info).await;
        }

        Ok(version)
    }

    pub async fn create_secret(
        &self,
        mut pb_secret: PbSecret,
        secret_plain_payload: Vec<u8>,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = pb_secret.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_secret.database_id, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_secret.schema_id, &txn).await?;
        check_secret_name_duplicate(&pb_secret, &txn).await?;

        let secret_obj = Self::create_object(
            &txn,
            ObjectType::Secret,
            owner_id,
            Some(pb_secret.schema_id.as_object_id()),
        )
        .await?;
        pb_secret.id = secret_obj.oid.as_secret_id();
        let secret: secret::ActiveModel = pb_secret.clone().into();
        Secret::insert(secret).exec(&txn).await?;

        let updated_user_info =
            grant_default_privileges_automatically(&txn, secret_obj.oid).await?;

        txn.commit().await?;

        // Notify the compute and frontend node plain secret
        let mut secret_plain = pb_secret;
        secret_plain.value.clone_from(&secret_plain_payload);

        LocalSecretManager::global().add_secret(secret_plain.id, secret_plain_payload);
        self.env
            .notification_manager()
            .notify_compute_without_version(Operation::Add, Info::Secret(secret_plain.clone()));

        let mut version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::Secret(secret_plain),
            )
            .await;

        // notify default privileges for secrets
        if !updated_user_info.is_empty() {
            version = self.notify_users_update(updated_user_info).await;
        }

        Ok(version)
    }

    pub async fn create_view(
        &self,
        mut pb_view: PbView,
        dependencies: HashSet<ObjectId>,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = pb_view.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_view.database_id, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_view.schema_id, &txn).await?;
        check_relation_name_duplicate(&pb_view.name, pb_view.database_id, pb_view.schema_id, &txn)
            .await?;
        ensure_object_id(ObjectType::Schema, pb_view.schema_id, &txn).await?;
        check_relation_name_duplicate(&pb_view.name, pb_view.database_id, pb_view.schema_id, &txn)
            .await?;

        let view_obj = Self::create_object(
            &txn,
            ObjectType::View,
            owner_id,
            Some(pb_view.schema_id.as_object_id()),
        )
        .await?;
        pb_view.id = view_obj.oid.as_view_id();
        pb_view.created_at_epoch =
            Some(Epoch::from_unix_millis(datetime_to_timestamp_millis(view_obj.created_at) as _).0);
        pb_view.created_at_cluster_version = view_obj.created_at_cluster_version;

        let view: view::ActiveModel = pb_view.clone().into();
        View::insert(view).exec(&txn).await?;

        let dependencies = if !dependencies.is_empty() {
            ObjectDependency::insert_many(dependencies.into_iter().map(|obj_id| {
                object_dependency::ActiveModel {
                    oid: Set(obj_id),
                    used_by: Set(view_obj.oid),
                    ..Default::default()
                }
            }))
            .exec(&txn)
            .await?;
            list_object_dependencies_by_object_id(&txn, view_obj.oid).await?
        } else {
            vec![]
        };

        let updated_user_info = grant_default_privileges_automatically(&txn, view_obj.oid).await?;

        txn.commit().await?;
        let mut version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects: vec![PbObject {
                        object_info: Some(PbObjectInfo::View(pb_view)),
                    }],
                    dependencies,
                }),
            )
            .await;

        // notify default privileges for views
        if !updated_user_info.is_empty() {
            version = self.notify_users_update(updated_user_info).await;
        }

        Ok(version)
    }

    pub async fn validate_cross_db_snapshot_backfill(
        &self,
        cross_db_snapshot_backfill_info: &SnapshotBackfillInfo,
    ) -> MetaResult<()> {
        if cross_db_snapshot_backfill_info
            .upstream_mv_table_id_to_backfill_epoch
            .is_empty()
        {
            return Ok(());
        }

        let inner = self.inner.read().await;
        let table_ids = cross_db_snapshot_backfill_info
            .upstream_mv_table_id_to_backfill_epoch
            .keys()
            .copied()
            .map_into()
            .collect_vec();
        let cnt = Subscription::find()
            .select_only()
            .column(subscription::Column::DependentTableId)
            .distinct()
            .filter(subscription::Column::DependentTableId.is_in::<TableId, _>(table_ids))
            .count(&inner.db)
            .await? as usize;

        if cnt
            < cross_db_snapshot_backfill_info
                .upstream_mv_table_id_to_backfill_epoch
                .keys()
                .count()
        {
            return Err(MetaError::permission_denied(
                "Some upstream tables are not subscribed".to_owned(),
            ));
        }

        Ok(())
    }
}
