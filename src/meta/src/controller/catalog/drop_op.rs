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

use risingwave_common::catalog::ICEBERG_SINK_PREFIX;
use risingwave_pb::catalog::subscription::PbSubscriptionState;
use risingwave_pb::telemetry::PbTelemetryDatabaseObject;
use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, ModelTrait, QueryFilter};

use super::*;
impl CatalogController {
    // Drop all kinds of objects including databases,
    // schemas, relations, connections, functions, etc.
    pub async fn drop_object(
        &self,
        object_type: ObjectType,
        object_id: impl Into<ObjectId>,
        drop_mode: DropMode,
    ) -> MetaResult<(ReleaseContext, NotificationVersion)> {
        let object_id = object_id.into();
        let mut inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let obj: PartialObject = Object::find_by_id(object_id)
            .into_partial_model()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(object_type.as_str(), object_id))?;
        assert_eq!(obj.obj_type, object_type);
        let database_id = if object_type == ObjectType::Database {
            object_id.as_database_id()
        } else {
            obj.database_id
                .ok_or_else(|| anyhow!("dropped object should have database_id"))?
        };

        // Check the cross-db dependency info to see if the subscription can be dropped.
        if obj.obj_type == ObjectType::Subscription {
            validate_subscription_deletion(&txn, object_id.as_subscription_id()).await?;
        }

        let mut removed_objects = match drop_mode {
            DropMode::Cascade => {
                get_referring_objects_cascade(object_id, object_type, &txn).await?
            }
            DropMode::Restrict => match object_type {
                ObjectType::Database => unreachable!("database always be dropped in cascade mode"),
                ObjectType::Schema => {
                    ensure_schema_empty(object_id.as_schema_id(), &txn).await?;
                    Default::default()
                }
                ObjectType::Table => {
                    let objects = validate_restrict_drop_and_collect_owned_objects(
                        object_type,
                        object_id,
                        &txn,
                    )
                    .await?;
                    for obj in objects.iter().filter(|object| {
                        object.obj_type == ObjectType::Source || object.obj_type == ObjectType::Sink
                    }) {
                        report_drop_object(obj.obj_type, obj.oid, &txn).await;
                    }
                    assert!(
                        objects.iter().all(|obj| obj.obj_type == ObjectType::Index
                            || obj.obj_type == ObjectType::Sink),
                        "only index and iceberg sink could be dropped in restrict mode"
                    );
                    for obj in &objects {
                        validate_restrict_drop_and_collect_owned_objects(
                            obj.obj_type,
                            obj.oid,
                            &txn,
                        )
                        .await?;
                    }
                    objects
                }
                object_type @ (ObjectType::Source | ObjectType::Sink) => {
                    validate_restrict_drop_and_collect_owned_objects(object_type, object_id, &txn)
                        .await?;
                    report_drop_object(object_type, object_id, &txn).await;
                    vec![]
                }

                ObjectType::View
                | ObjectType::Index
                | ObjectType::Function
                | ObjectType::Connection
                | ObjectType::Subscription
                | ObjectType::Secret => {
                    validate_restrict_drop_and_collect_owned_objects(object_type, object_id, &txn)
                        .await?;
                    vec![]
                }
            },
        };

        removed_objects.push(obj);
        let mut removed_object_ids: HashSet<_> =
            removed_objects.iter().map(|obj| obj.oid).collect();

        for obj in &removed_objects {
            if obj.obj_type == ObjectType::Sink {
                let sink = Sink::find_by_id(obj.oid.as_sink_id())
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("sink", obj.oid))?;

                if let Some(target_table) = sink.target_table
                    && !removed_object_ids.contains(&target_table.as_object_id())
                    && !has_table_been_migrated(&txn, target_table).await?
                {
                    return Err(anyhow::anyhow!(
                        "Dropping sink into table is not allowed for unmigrated table {}. Please migrate it first.",
                        target_table
                    ).into());
                }
            }
        }

        // Load all objects that belong to the dropped objects before deletion. Cascaded rows are
        // still needed for notifications and resource cleanup.
        let root_objects = Object::find()
            .filter(object::Column::Oid.is_in(removed_object_ids.iter().copied()))
            .all(&txn)
            .await?;
        let belonging_objects =
            get_belong_objects_by_ids(&txn, removed_objects.iter().map(|obj| obj.oid)).await?;
        removed_object_ids.extend(belonging_objects.iter().map(|obj| obj.oid));
        let mut objects_to_remove = root_objects.clone();
        objects_to_remove.extend(belonging_objects.iter().cloned());
        let removed_catalog_models = load_object_models(&txn, &objects_to_remove).await?;
        removed_objects.extend(belonging_objects.into_iter().map(|obj| PartialObject {
            oid: obj.oid,
            obj_type: obj.obj_type,
            schema_id: obj.schema_id,
            database_id: obj.database_id,
        }));

        let removed_table_ids = removed_objects
            .iter()
            .filter(|obj| obj.obj_type == ObjectType::Table || obj.obj_type == ObjectType::Index)
            .map(|obj| obj.oid.as_table_id());

        let removed_iceberg_table_sinks: Vec<PbSink> = removed_catalog_models
            .iter()
            .filter_map(|object_info| match object_info {
                PbObjectInfo::Sink(sink) if sink.name.starts_with(ICEBERG_SINK_PREFIX) => {
                    Some(sink.clone())
                }
                _ => None,
            })
            .collect();

        // Iceberg sinks (and the pk-index subset) can be user-created with arbitrary
        // names, so unlike the iceberg-table cleanup above, identify them by
        // inspecting properties rather than by name prefix.
        let mut removed_iceberg_sink_ids: Vec<SinkId> = Vec::new();
        let mut removed_iceberg_pk_index_sink_ids: Vec<SinkId> = Vec::new();
        for object_info in &removed_catalog_models {
            if let PbObjectInfo::Sink(sink) = object_info {
                if crate::manager::iceberg_compaction::is_iceberg_sink(&sink.properties) {
                    removed_iceberg_sink_ids.push(sink.id);
                }
                if crate::manager::iceberg_pk_index_sink::is_iceberg_pk_index_sink(&sink.properties)
                {
                    removed_iceberg_pk_index_sink_ids.push(sink.id);
                }
            }
        }

        let removed_streaming_job_ids: Vec<JobId> = StreamingJob::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .filter(streaming_job::Column::JobId.is_in(removed_object_ids))
            .into_tuple()
            .all(&txn)
            .await?;

        // Check if there are any streaming jobs that are creating.
        if !removed_streaming_job_ids.is_empty() {
            let creating = StreamingJob::find()
                .filter(
                    streaming_job::Column::JobStatus
                        .ne(JobStatus::Created)
                        .and(streaming_job::Column::JobId.is_in(removed_streaming_job_ids.clone())),
                )
                .count(&txn)
                .await?;
            if creating != 0 {
                if creating == 1 && object_type == ObjectType::Sink {
                    info!("dropping creating sink job, it will be cancelled");
                } else {
                    return Err(MetaError::permission_denied(format!(
                        "cannot drop {creating} streaming job(s) that are still being created; please cancel them first"
                    )));
                }
            }
        }

        let removed_state_table_ids: HashSet<_> = removed_table_ids.clone().collect();

        let removed_source_ids: HashSet<_> = removed_objects
            .iter()
            .filter(|obj| obj.obj_type == ObjectType::Source)
            .map(|obj| obj.oid.as_source_id())
            .collect();

        let removed_secret_ids = removed_objects
            .iter()
            .filter(|obj| obj.obj_type == ObjectType::Secret)
            .map(|obj| obj.oid.as_secret_id())
            .collect();

        let removed_objects: HashMap<_, _> = removed_objects
            .into_iter()
            .map(|obj| (obj.oid, obj))
            .collect();

        // TODO: Support drop cascade for cross-database query.
        for obj in removed_objects.values() {
            if let Some(obj_database_id) = obj.database_id
                && obj_database_id != database_id
            {
                return Err(MetaError::permission_denied(format!(
                    "Referenced by other objects in database {obj_database_id}, please drop them manually"
                )));
            }
        }

        let (removed_source_fragments, removed_sink_fragments, removed_fragments) =
            get_fragments_for_jobs(&txn, removed_streaming_job_ids.clone()).await?;

        let sink_target_fragments = fetch_target_fragments(&txn, removed_sink_fragments).await?;
        let mut removed_sink_fragment_by_targets = HashMap::new();
        for (sink_fragment, target_fragments) in sink_target_fragments {
            assert!(
                target_fragments.len() <= 1,
                "sink should have at most one downstream fragment"
            );
            if let Some(target_fragment) = target_fragments.first()
                && !removed_fragments.contains(target_fragment)
            {
                removed_sink_fragment_by_targets
                    .entry(*target_fragment)
                    .or_insert_with(Vec::new)
                    .push(sink_fragment);
            }
        }

        // Find affect users with privileges on all this objects.
        let updated_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Oid.is_in(removed_objects.keys().cloned()))
            .into_tuple()
            .all(&txn)
            .await?;
        let dropped_tables = removed_catalog_models
            .into_iter()
            .filter_map(|object_info| match object_info {
                PbObjectInfo::Table(table) if removed_state_table_ids.contains(&table.id) => {
                    Some(table)
                }
                _ => None,
            });
        // Delete the explicitly selected objects. The self foreign key cascades to every object
        // that belongs to them.
        let res = Object::delete_many()
            .filter(object::Column::Oid.is_in(root_objects.iter().map(|obj| obj.oid)))
            .exec(&txn)
            .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found(
                object_type.as_str(),
                object_id,
            ));
        }
        let user_infos = list_user_info_by_ids(updated_user_ids, &txn).await?;

        txn.commit().await?;

        // notify about them.
        self.notify_users_update(user_infos).await;
        inner
            .dropped_tables
            .extend(dropped_tables.map(|t| (t.id, t)));

        let version = match object_type {
            ObjectType::Database => {
                // TODO: Notify objects in other databases when the cross-database query is supported.
                self.notify_frontend(
                    NotificationOperation::Delete,
                    NotificationInfo::Database(PbDatabase {
                        id: database_id,
                        ..Default::default()
                    }),
                )
                .await
            }
            ObjectType::Schema => {
                let (schema_obj, mut to_notify_objs): (Vec<_>, Vec<_>) = removed_objects
                    .into_values()
                    .partition(|obj| obj.obj_type == ObjectType::Schema && obj.oid == object_id);
                let schema_obj = Itertools::exactly_one(schema_obj.into_iter())
                    .expect("schema object not found");
                to_notify_objs.push(schema_obj);

                let relation_group = build_object_group_for_delete(to_notify_objs);
                self.notify_frontend(NotificationOperation::Delete, relation_group)
                    .await
            }
            _ => {
                // Hummock observers and compactor observers are notified once the corresponding barrier is completed.
                // They only need RelationInfo::Table.
                let relation_group =
                    build_object_group_for_delete(removed_objects.into_values().collect());
                self.notify_frontend(NotificationOperation::Delete, relation_group)
                    .await
            }
        };

        Ok((
            ReleaseContext {
                database_id,
                removed_streaming_job_ids,
                removed_state_table_ids: removed_state_table_ids.into_iter().collect(),
                removed_source_ids: removed_source_ids.into_iter().collect(),
                removed_secret_ids,
                removed_source_fragments,
                removed_fragments,
                removed_sink_fragment_by_targets,
                removed_iceberg_table_sinks,
                removed_iceberg_sink_ids,
                removed_iceberg_pk_index_sink_ids,
            },
            version,
        ))
    }

    pub async fn try_abort_creating_subscription(
        &self,
        subscription_id: SubscriptionId,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let subscription = Subscription::find_by_id(subscription_id).one(&txn).await?;
        let Some(subscription) = subscription else {
            tracing::warn!(
                %subscription_id,
                "subscription not found when aborting creation, might be cleaned by recovery"
            );
            return Ok(());
        };

        if subscription.subscription_state == PbSubscriptionState::Created as i32 {
            tracing::warn!(
                %subscription_id,
                "subscription is already created when aborting creation"
            );
            return Ok(());
        }

        subscription.delete(&txn).await?;
        txn.commit().await?;
        Ok(())
    }
}

async fn report_drop_object(
    object_type: ObjectType,
    object_id: ObjectId,
    txn: &DatabaseTransaction,
) {
    let connector_name = {
        match object_type {
            ObjectType::Sink => Sink::find_by_id(object_id.as_sink_id())
                .select_only()
                .column(sink::Column::Properties)
                .into_tuple::<Property>()
                .one(txn)
                .await
                .ok()
                .flatten()
                .and_then(|properties| properties.inner_ref().get("connector").cloned()),
            ObjectType::Source => Source::find_by_id(object_id.as_source_id())
                .select_only()
                .column(source::Column::WithProperties)
                .into_tuple::<Property>()
                .one(txn)
                .await
                .ok()
                .flatten()
                .and_then(|properties| properties.inner_ref().get("connector").cloned()),
            _ => unreachable!(),
        }
    };
    if let Some(connector_name) = connector_name {
        report_event(
            PbTelemetryEventStage::DropStreamJob,
            "source",
            object_id.as_raw_id() as _,
            Some(connector_name),
            Some(match object_type {
                ObjectType::Source => PbTelemetryDatabaseObject::Source,
                ObjectType::Sink => PbTelemetryDatabaseObject::Sink,
                _ => unreachable!(),
            }),
            None,
        );
    }
}
