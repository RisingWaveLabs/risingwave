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

use risingwave_common::catalog::FragmentTypeMask;
use risingwave_pb::stream_plan::stream_node::NodeBody;

use super::*;
use crate::controller::fragment::FragmentTypeMaskExt;
use crate::controller::utils::load_streaming_jobs_by_ids;

pub(crate) async fn prepare_object_models_for_schema_change(
    txn: &DatabaseTransaction,
    object_models: &mut [PbObjectInfo],
    database_id: DatabaseId,
    new_schema: SchemaId,
) -> MetaResult<()> {
    // Names live in the type-specific catalog tables, not in `object`. Relations also share a
    // namespace across object types, so preserve the existing type-specific duplicate checks.
    let index_ids = object_models
        .iter()
        .filter_map(|object_info| match object_info {
            PbObjectInfo::Index(index) => Some(index.id.as_object_id().as_table_id()),
            _ => None,
        })
        .collect::<HashSet<_>>();

    for object_info in object_models {
        match object_info {
            PbObjectInfo::Table(table) => table.schema_id = new_schema,
            PbObjectInfo::Source(source) => source.schema_id = new_schema,
            PbObjectInfo::Sink(sink) => sink.schema_id = new_schema,
            PbObjectInfo::View(view) => view.schema_id = new_schema,
            PbObjectInfo::Index(index) => index.schema_id = new_schema,
            PbObjectInfo::Function(function) => function.schema_id = new_schema,
            PbObjectInfo::Connection(connection) => connection.schema_id = new_schema,
            PbObjectInfo::Subscription(subscription) => subscription.schema_id = new_schema,
            PbObjectInfo::Secret(secret) => secret.schema_id = new_schema,
            PbObjectInfo::Database(_) | PbObjectInfo::Schema(_) => {}
        }

        match object_info {
            PbObjectInfo::Table(table) if !index_ids.contains(&table.id) => {
                check_relation_name_duplicate(&table.name, database_id, new_schema, txn).await?;
            }
            PbObjectInfo::Source(source) => {
                check_relation_name_duplicate(&source.name, database_id, new_schema, txn).await?;
            }
            PbObjectInfo::Sink(sink) => {
                check_relation_name_duplicate(&sink.name, database_id, new_schema, txn).await?;
            }
            PbObjectInfo::Index(index) => {
                check_relation_name_duplicate(&index.name, database_id, new_schema, txn).await?;
            }
            PbObjectInfo::View(view) => {
                check_relation_name_duplicate(&view.name, database_id, new_schema, txn).await?;
            }
            PbObjectInfo::Subscription(subscription) => {
                check_relation_name_duplicate(&subscription.name, database_id, new_schema, txn)
                    .await?;
                check_subscription_name_duplicate(subscription, txn).await?;
            }
            PbObjectInfo::Function(function) => {
                check_function_signature_duplicate(function, txn).await?;
            }
            PbObjectInfo::Connection(connection) => {
                check_connection_name_duplicate(connection, txn).await?;
            }
            PbObjectInfo::Secret(secret) => {
                check_secret_name_duplicate(secret, txn).await?;
            }
            PbObjectInfo::Database(_) | PbObjectInfo::Schema(_) | PbObjectInfo::Table(_) => {}
        }
    }

    Ok(())
}

pub(crate) async fn load_object_models(
    txn: &DatabaseTransaction,
    objects: &[object::Model],
) -> MetaResult<Vec<PbObjectInfo>> {
    let mut object_infos = vec![];

    let table_ids = objects
        .iter()
        .filter(|object| object.obj_type == ObjectType::Table)
        .map(|object| object.oid.as_table_id())
        .collect_vec();
    let table_objs = Table::find()
        .find_also_related(Object)
        .filter(table::Column::TableId.is_in(table_ids))
        .all(txn)
        .await?;
    let streaming_jobs =
        load_streaming_jobs_by_ids(txn, table_objs.iter().map(|(table, _)| table.job_id())).await?;
    for (table, table_obj) in table_objs {
        let streaming_job = streaming_jobs.get(&table.job_id()).cloned();
        object_infos.push(PbObjectInfo::Table(
            ObjectModel(table, table_obj.unwrap(), streaming_job).into(),
        ));
    }

    let index_ids = objects
        .iter()
        .filter(|object| object.obj_type == ObjectType::Index)
        .map(|object| object.oid.as_index_id())
        .collect_vec();
    let index_table_objs = Table::find()
        .find_also_related(Object)
        .filter(
            table::Column::TableId
                .is_in(index_ids.iter().map(|id| id.as_object_id().as_table_id())),
        )
        .all(txn)
        .await?;
    let index_streaming_jobs = load_streaming_jobs_by_ids(
        txn,
        index_table_objs.iter().map(|(table, _)| table.job_id()),
    )
    .await?;
    for (table, table_obj) in index_table_objs {
        let streaming_job = index_streaming_jobs.get(&table.job_id()).cloned();
        object_infos.push(PbObjectInfo::Table(
            ObjectModel(table, table_obj.unwrap(), streaming_job).into(),
        ));
    }
    let index_objs = Index::find()
        .find_also_related(Object)
        .filter(index::Column::IndexId.is_in(index_ids))
        .all(txn)
        .await?;
    for (index, index_obj) in index_objs {
        let streaming_job = index_streaming_jobs
            .get(&index.index_id.as_job_id())
            .cloned();
        object_infos.push(PbObjectInfo::Index(
            ObjectModel(index, index_obj.unwrap(), streaming_job).into(),
        ));
    }

    let source_ids = objects
        .iter()
        .filter(|object| object.obj_type == ObjectType::Source)
        .map(|object| object.oid.as_source_id())
        .collect_vec();
    for (source, source_obj) in Source::find()
        .find_also_related(Object)
        .filter(source::Column::SourceId.is_in(source_ids))
        .all(txn)
        .await?
    {
        object_infos.push(PbObjectInfo::Source(
            ObjectModel(source, source_obj.unwrap(), None).into(),
        ));
    }

    let sink_ids = objects
        .iter()
        .filter(|object| object.obj_type == ObjectType::Sink)
        .map(|object| object.oid.as_sink_id())
        .collect_vec();
    let sink_objs = Sink::find()
        .find_also_related(Object)
        .filter(sink::Column::SinkId.is_in(sink_ids))
        .all(txn)
        .await?;
    let sink_streaming_jobs = load_streaming_jobs_by_ids(
        txn,
        sink_objs.iter().map(|(sink, _)| sink.sink_id.as_job_id()),
    )
    .await?;
    for (sink, sink_obj) in sink_objs {
        let streaming_job = sink_streaming_jobs.get(&sink.sink_id.as_job_id()).cloned();
        object_infos.push(PbObjectInfo::Sink(
            ObjectModel(sink, sink_obj.unwrap(), streaming_job).into(),
        ));
    }

    let subscription_ids = objects
        .iter()
        .filter(|object| object.obj_type == ObjectType::Subscription)
        .map(|object| object.oid.as_subscription_id())
        .collect_vec();
    for (subscription, subscription_obj) in Subscription::find()
        .find_also_related(Object)
        .filter(subscription::Column::SubscriptionId.is_in(subscription_ids))
        .all(txn)
        .await?
    {
        object_infos.push(PbObjectInfo::Subscription(
            ObjectModel(subscription, subscription_obj.unwrap(), None).into(),
        ));
    }

    let view_ids = objects
        .iter()
        .filter(|object| object.obj_type == ObjectType::View)
        .map(|object| object.oid.as_view_id())
        .collect_vec();
    for (view, view_obj) in View::find()
        .find_also_related(Object)
        .filter(view::Column::ViewId.is_in(view_ids))
        .all(txn)
        .await?
    {
        object_infos.push(PbObjectInfo::View(
            ObjectModel(view, view_obj.unwrap(), None).into(),
        ));
    }

    let function_ids = objects
        .iter()
        .filter(|object| object.obj_type == ObjectType::Function)
        .map(|object| object.oid.as_function_id())
        .collect_vec();
    for (function, function_obj) in Function::find()
        .find_also_related(Object)
        .filter(function::Column::FunctionId.is_in(function_ids))
        .all(txn)
        .await?
    {
        object_infos.push(PbObjectInfo::Function(
            ObjectModel(function, function_obj.unwrap(), None).into(),
        ));
    }

    let connection_ids = objects
        .iter()
        .filter(|object| object.obj_type == ObjectType::Connection)
        .map(|object| object.oid.as_connection_id())
        .collect_vec();
    for (connection, connection_obj) in Connection::find()
        .find_also_related(Object)
        .filter(connection::Column::ConnectionId.is_in(connection_ids))
        .all(txn)
        .await?
    {
        object_infos.push(PbObjectInfo::Connection(
            ObjectModel(connection, connection_obj.unwrap(), None).into(),
        ));
    }

    let secret_ids = objects
        .iter()
        .filter(|object| object.obj_type == ObjectType::Secret)
        .map(|object| object.oid.as_secret_id())
        .collect_vec();
    for (secret, secret_obj) in Secret::find()
        .find_also_related(Object)
        .filter(secret::Column::SecretId.is_in(secret_ids))
        .all(txn)
        .await?
    {
        object_infos.push(PbObjectInfo::Secret(
            ObjectModel(secret, secret_obj.unwrap(), None).into(),
        ));
    }

    let database_ids = objects
        .iter()
        .filter(|object| object.obj_type == ObjectType::Database)
        .map(|object| object.oid.as_database_id())
        .collect_vec();
    for (database, database_obj) in Database::find()
        .find_also_related(Object)
        .filter(database::Column::DatabaseId.is_in(database_ids))
        .all(txn)
        .await?
    {
        object_infos.push(PbObjectInfo::Database(
            ObjectModel(database, database_obj.unwrap(), None).into(),
        ));
    }

    let schema_ids = objects
        .iter()
        .filter(|object| object.obj_type == ObjectType::Schema)
        .map(|object| object.oid.as_schema_id())
        .collect_vec();
    for (schema, schema_obj) in Schema::find()
        .find_also_related(Object)
        .filter(schema::Column::SchemaId.is_in(schema_ids))
        .all(txn)
        .await?
    {
        object_infos.push(PbObjectInfo::Schema(
            ObjectModel(schema, schema_obj.unwrap(), None).into(),
        ));
    }

    Ok(object_infos)
}

pub(crate) async fn update_internal_tables(
    txn: &DatabaseTransaction,
    object_id: ObjectId,
    column: object::Column,
    new_value: impl Into<Value>,
    objects_to_notify: &mut Vec<PbObjectInfo>,
) -> MetaResult<()> {
    let internal_tables = get_internal_tables_by_id(object_id.as_job_id(), txn).await?;

    if !internal_tables.is_empty() {
        Object::update_many()
            .col_expr(column, SimpleExpr::Value(new_value.into()))
            .filter(object::Column::Oid.is_in(internal_tables.clone()))
            .exec(txn)
            .await?;

        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::TableId.is_in(internal_tables))
            .all(txn)
            .await?;
        let streaming_jobs =
            load_streaming_jobs_by_ids(txn, table_objs.iter().map(|(table, _)| table.job_id()))
                .await?;
        for (table, table_obj) in table_objs {
            let job_id = table.job_id();
            let streaming_job = streaming_jobs.get(&job_id).cloned();
            objects_to_notify.push(PbObjectInfo::Table(
                ObjectModel(table, table_obj.unwrap(), streaming_job).into(),
            ));
        }
    }
    Ok(())
}

impl CatalogController {
    pub(crate) async fn init(&self) -> MetaResult<()> {
        self.table_catalog_cdc_table_id_update().await?;
        Ok(())
    }

    /// Fill in the `cdc_table_id` field for Table with empty `cdc_table_id` and parent Source job.
    /// NOTES: We assume Table with a parent Source job is a CDC table
    pub(crate) async fn table_catalog_cdc_table_id_update(&self) -> MetaResult<()> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        // select Tables which cdc_table_id is empty and has a parent Source job
        let table_and_source_id: Vec<(TableId, String, SourceId)> = Table::find()
            .join(JoinType::InnerJoin, table::Relation::ObjectDependency.def())
            .join(
                JoinType::InnerJoin,
                object_dependency::Relation::Source.def(),
            )
            .select_only()
            .columns([table::Column::TableId, table::Column::Definition])
            .columns([source::Column::SourceId])
            .filter(
                table::Column::TableType.eq(TableType::Table).and(
                    table::Column::CdcTableId
                        .is_null()
                        .or(table::Column::CdcTableId.eq("")),
                ),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        // return directly if the result set is empty.
        if table_and_source_id.is_empty() {
            return Ok(());
        }

        info!(table_and_source_id = ?table_and_source_id, "cdc table with empty cdc_table_id");

        let mut cdc_table_ids = HashMap::new();
        for (table_id, definition, source_id) in table_and_source_id {
            match extract_external_table_name_from_definition(&definition) {
                None => {
                    tracing::warn!(
                        %table_id,
                        definition,
                        "failed to extract cdc table name from table definition.",
                    )
                }
                Some(external_table_name) => {
                    cdc_table_ids.insert(
                        table_id,
                        build_cdc_table_id(source_id, &external_table_name),
                    );
                }
            }
        }

        for (table_id, cdc_table_id) in cdc_table_ids {
            Table::update(table::ActiveModel {
                table_id: Set(table_id as _),
                cdc_table_id: Set(Some(cdc_table_id)),
                ..Default::default()
            })
            .exec(&txn)
            .await?;
        }
        txn.commit().await?;
        Ok(())
    }

    pub(crate) async fn log_cleaned_dirty_jobs(
        &self,
        dirty_objs: &[PartialObject],
        txn: &DatabaseTransaction,
    ) -> MetaResult<()> {
        // Record cleaned streaming jobs in event logs.
        let mut dirty_table_ids = vec![];
        let mut dirty_source_ids = vec![];
        let mut dirty_sink_ids = vec![];
        for dirty_job_obj in dirty_objs {
            let job_id = dirty_job_obj.oid;
            let job_type = dirty_job_obj.obj_type;
            match job_type {
                ObjectType::Table | ObjectType::Index => dirty_table_ids.push(job_id),
                ObjectType::Source => dirty_source_ids.push(job_id),
                ObjectType::Sink => dirty_sink_ids.push(job_id),
                _ => unreachable!("unexpected streaming job type"),
            }
        }

        let mut event_logs = vec![];
        if !dirty_table_ids.is_empty() {
            let table_info: Vec<(TableId, String, String)> = Table::find()
                .select_only()
                .columns([
                    table::Column::TableId,
                    table::Column::Name,
                    table::Column::Definition,
                ])
                .filter(table::Column::TableId.is_in(dirty_table_ids))
                .into_tuple()
                .all(txn)
                .await?;
            for (table_id, name, definition) in table_info {
                let event = risingwave_pb::meta::event_log::EventDirtyStreamJobClear {
                    id: table_id.as_job_id(),
                    name,
                    definition,
                    error: "clear during recovery".to_owned(),
                };
                event_logs.push(risingwave_pb::meta::event_log::Event::DirtyStreamJobClear(
                    event,
                ));
            }
        }
        if !dirty_source_ids.is_empty() {
            let source_info: Vec<(SourceId, String, String)> = Source::find()
                .select_only()
                .columns([
                    source::Column::SourceId,
                    source::Column::Name,
                    source::Column::Definition,
                ])
                .filter(source::Column::SourceId.is_in(dirty_source_ids))
                .into_tuple()
                .all(txn)
                .await?;
            for (source_id, name, definition) in source_info {
                let event = risingwave_pb::meta::event_log::EventDirtyStreamJobClear {
                    id: source_id.as_share_source_job_id(),
                    name,
                    definition,
                    error: "clear during recovery".to_owned(),
                };
                event_logs.push(risingwave_pb::meta::event_log::Event::DirtyStreamJobClear(
                    event,
                ));
            }
        }
        if !dirty_sink_ids.is_empty() {
            let sink_info: Vec<(SinkId, String, String)> = Sink::find()
                .select_only()
                .columns([
                    sink::Column::SinkId,
                    sink::Column::Name,
                    sink::Column::Definition,
                ])
                .filter(sink::Column::SinkId.is_in(dirty_sink_ids))
                .into_tuple()
                .all(txn)
                .await?;
            for (sink_id, name, definition) in sink_info {
                let event = risingwave_pb::meta::event_log::EventDirtyStreamJobClear {
                    id: sink_id.as_job_id(),
                    name,
                    definition,
                    error: "clear during recovery".to_owned(),
                };
                event_logs.push(risingwave_pb::meta::event_log::Event::DirtyStreamJobClear(
                    event,
                ));
            }
        }
        self.env.event_log_manager_ref().add_event_logs(event_logs);
        Ok(())
    }

    pub(crate) async fn clean_dirty_sink_downstreams(txn: &DatabaseTransaction) -> MetaResult<()> {
        // clean incoming sink from (table)
        // clean upstream fragment ids from (fragment)
        // clean stream node from (fragment)
        // clean upstream actor ids from (actor)

        // The cleanup of fragment and StreamNode is to maintain compatibility with old versions of data. For the
        // current sink-into-table implementation, there is no need to restore the contents of StreamNode, because the
        // `UpstreamSinkUnion` operator does not persist any data, but relies on refill during recovery.

        let all_fragment_ids: Vec<FragmentId> = Fragment::find()
            .select_only()
            .column(fragment::Column::FragmentId)
            .into_tuple()
            .all(txn)
            .await?;

        let all_fragment_ids: HashSet<_> = all_fragment_ids.into_iter().collect();

        let all_sink_into_tables: Vec<Option<TableId>> = Sink::find()
            .select_only()
            .column(sink::Column::TargetTable)
            .filter(sink::Column::TargetTable.is_not_null())
            .into_tuple()
            .all(txn)
            .await?;

        let mut table_with_incoming_sinks: HashSet<TableId> = HashSet::new();
        for target_table_id in all_sink_into_tables {
            table_with_incoming_sinks.insert(target_table_id.expect("filter by non null"));
        }

        // no need to update, returning
        if table_with_incoming_sinks.is_empty() {
            return Ok(());
        }

        for table_id in table_with_incoming_sinks {
            tracing::info!("cleaning dirty table sink downstream table {}", table_id);

            let fragments: Vec<(FragmentId, StreamNode)> = Fragment::find()
                .select_only()
                .columns(vec![
                    fragment::Column::FragmentId,
                    fragment::Column::StreamNode,
                ])
                .filter(fragment::Column::JobId.eq(table_id).and(
                    // dirty downstream should be materialize fragment of table
                    FragmentTypeMask::intersects(FragmentTypeFlag::Mview),
                ))
                .into_tuple()
                .all(txn)
                .await?;

            for (fragment_id, stream_node) in fragments {
                {
                    let mut dirty_upstream_fragment_ids = HashSet::new();

                    let mut pb_stream_node = stream_node.to_protobuf();

                    visit_stream_node_cont_mut(&mut pb_stream_node, |node| {
                        if let Some(NodeBody::Union(_)) = node.node_body {
                            node.input.retain_mut(|input| match &mut input.node_body {
                                Some(NodeBody::Project(_)) => {
                                    let body = Itertools::exactly_one(input.input.iter()).unwrap();
                                    let Some(NodeBody::Merge(merge_node)) = &body.node_body else {
                                        unreachable!("expect merge node");
                                    };
                                    if all_fragment_ids.contains(&(merge_node.upstream_fragment_id))
                                    {
                                        true
                                    } else {
                                        dirty_upstream_fragment_ids
                                            .insert(merge_node.upstream_fragment_id);
                                        false
                                    }
                                }
                                Some(NodeBody::Merge(merge_node)) => {
                                    if all_fragment_ids.contains(&(merge_node.upstream_fragment_id))
                                    {
                                        true
                                    } else {
                                        dirty_upstream_fragment_ids
                                            .insert(merge_node.upstream_fragment_id);
                                        false
                                    }
                                }
                                _ => false,
                            });
                        }
                        true
                    });

                    tracing::info!(
                        "cleaning dirty table sink fragment {:?} from downstream fragment {}",
                        dirty_upstream_fragment_ids,
                        fragment_id
                    );

                    if !dirty_upstream_fragment_ids.is_empty() {
                        tracing::info!(
                            "fixing dirty stream node in downstream fragment {}",
                            fragment_id
                        );
                        Fragment::update_many()
                            .col_expr(
                                fragment::Column::StreamNode,
                                StreamNode::from(&pb_stream_node).into(),
                            )
                            .filter(fragment::Column::FragmentId.eq(fragment_id))
                            .exec(txn)
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn has_any_streaming_jobs(&self) -> MetaResult<bool> {
        let inner = self.inner.read().await;
        let count = streaming_job::Entity::find().count(&inner.db).await?;
        Ok(count > 0)
    }

    pub async fn find_creating_streaming_job_ids(
        &self,
        infos: Vec<PbCreatingJobInfo>,
    ) -> MetaResult<Vec<ObjectId>> {
        let inner = self.inner.read().await;

        type JobKey = (DatabaseId, SchemaId, String);

        // Index table is already included if we still assign the same name for index table as the index.
        let creating_tables: Vec<(ObjectId, String, DatabaseId, SchemaId)> = Table::find()
            .select_only()
            .columns([table::Column::TableId, table::Column::Name])
            .columns([object::Column::DatabaseId, object::Column::SchemaId])
            .join(JoinType::InnerJoin, table::Relation::Object1.def())
            .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
            .into_tuple()
            .all(&inner.db)
            .await?;
        let creating_sinks: Vec<(ObjectId, String, DatabaseId, SchemaId)> = Sink::find()
            .select_only()
            .columns([sink::Column::SinkId, sink::Column::Name])
            .columns([object::Column::DatabaseId, object::Column::SchemaId])
            .join(JoinType::InnerJoin, sink::Relation::Object.def())
            .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
            .into_tuple()
            .all(&inner.db)
            .await?;
        let creating_subscriptions: Vec<(ObjectId, String, DatabaseId, SchemaId)> =
            Subscription::find()
                .select_only()
                .columns([
                    subscription::Column::SubscriptionId,
                    subscription::Column::Name,
                ])
                .columns([object::Column::DatabaseId, object::Column::SchemaId])
                .join(JoinType::InnerJoin, subscription::Relation::Object.def())
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
                .into_tuple()
                .all(&inner.db)
                .await?;

        let mut job_mapping: HashMap<JobKey, ObjectId> = creating_tables
            .into_iter()
            .chain(creating_sinks)
            .chain(creating_subscriptions)
            .map(|(id, name, database_id, schema_id)| ((database_id, schema_id, name), id))
            .collect();

        Ok(infos
            .into_iter()
            .flat_map(|info| job_mapping.remove(&(info.database_id, info.schema_id, info.name)))
            .collect())
    }
}
