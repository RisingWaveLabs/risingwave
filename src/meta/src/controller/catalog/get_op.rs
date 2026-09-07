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

use std::collections::{HashMap, HashSet};

use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::id::JobId;
use sea_orm::ConnectionTrait;

use super::*;
use crate::controller::utils::{
    StreamingJobExtraInfo, get_database_resource_group, get_existing_job_resource_group,
    get_streaming_job_extra_info as fetch_streaming_job_extra_info, get_table_columns,
    load_streaming_jobs_by_ids,
};

impl CatalogController {
    pub async fn get_secret_by_id(&self, secret_id: SecretId) -> MetaResult<PbSecret> {
        let inner = self.inner.read().await;
        let (secret, obj) = Secret::find_by_id(secret_id)
            .find_also_related(Object)
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("secret", secret_id))?;
        Ok(ObjectModel(secret, obj.unwrap(), None).into())
    }

    pub async fn get_object_database_id(
        &self,
        object_id: impl Into<ObjectId>,
    ) -> MetaResult<DatabaseId> {
        let object_id = object_id.into();
        let inner = self.inner.read().await;
        let (database_id,): (Option<DatabaseId>,) = Object::find_by_id(object_id)
            .select_only()
            .select_column(object::Column::DatabaseId)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("object", object_id))?;
        Ok(database_id.ok_or_else(|| anyhow!("object has no database id: {object_id}"))?)
    }

    pub async fn get_connection_by_id(
        &self,
        connection_id: ConnectionId,
    ) -> MetaResult<PbConnection> {
        let inner = self.inner.read().await;
        let (conn, obj) = Connection::find_by_id(connection_id)
            .find_also_related(Object)
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("connection", connection_id))?;

        Ok(ObjectModel(conn, obj.unwrap(), None).into())
    }

    pub async fn get_table_catalog_by_name(
        &self,
        database_id: DatabaseId,
        schema_id: SchemaId,
        name: &str,
    ) -> MetaResult<Option<PbTable>> {
        let inner = self.inner.read().await;
        let table_obj = Table::find()
            .find_also_related(Object)
            .filter(
                table::Column::Name
                    .eq(name)
                    .and(object::Column::DatabaseId.eq(database_id))
                    .and(object::Column::SchemaId.eq(schema_id)),
            )
            .one(&inner.db)
            .await?;
        if let Some((table, obj)) = table_obj {
            let streaming_job = streaming_job::Entity::find_by_id(table.job_id())
                .one(&inner.db)
                .await?;
            Ok(Some(ObjectModel(table, obj.unwrap(), streaming_job).into()))
        } else {
            Ok(None)
        }
    }

    pub async fn get_table_by_name(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> MetaResult<Option<PbTable>> {
        let inner = self.inner.read().await;
        let table_obj = Table::find()
            .find_also_related(Object)
            .join(JoinType::InnerJoin, object::Relation::Database2.def())
            .filter(
                table::Column::Name
                    .eq(table_name)
                    .and(database::Column::Name.eq(database_name)),
            )
            .one(&inner.db)
            .await?;
        if let Some((table, obj)) = table_obj {
            let streaming_job = streaming_job::Entity::find_by_id(table.job_id())
                .one(&inner.db)
                .await?;
            Ok(Some(ObjectModel(table, obj.unwrap(), streaming_job).into()))
        } else {
            Ok(None)
        }
    }

    pub async fn get_table_associated_source_id(
        &self,
        table_id: TableId,
    ) -> MetaResult<Option<SourceId>> {
        let inner = self.inner.read().await;
        Table::find_by_id(table_id)
            .select_only()
            .select_column(table::Column::OptionalAssociatedSourceId)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("table", table_id))
    }

    pub async fn get_table_by_associate_source_id(
        &self,
        associated_source_id: SourceId,
    ) -> MetaResult<PbTable> {
        let inner = self.inner.read().await;
        let table_obj = Table::find()
            .find_also_related(Object)
            .filter(table::Column::OptionalAssociatedSourceId.eq(associated_source_id))
            .one(&inner.db)
            .await?
            .ok_or_else(|| {
                MetaError::catalog_id_not_found("table associated source", associated_source_id)
            })?;
        let (table, obj) = table_obj;
        let streaming_job = streaming_job::Entity::find_by_id(table.job_id())
            .one(&inner.db)
            .await?;
        Ok(ObjectModel(table, obj.unwrap(), streaming_job).into())
    }

    pub async fn get_table_by_id(&self, table_id: TableId) -> MetaResult<PbTable> {
        let inner = self.inner.read().await;
        let table_obj = Table::find_by_id(table_id)
            .find_also_related(Object)
            .one(&inner.db)
            .await?;
        if let Some((table, obj)) = table_obj {
            let streaming_job = streaming_job::Entity::find_by_id(table.job_id())
                .one(&inner.db)
                .await?;
            Ok(ObjectModel(table, obj.unwrap(), streaming_job).into())
        } else {
            Err(MetaError::catalog_id_not_found("table", table_id))
        }
    }

    pub async fn get_user_created_table_by_ids(
        &self,
        job_ids: impl Iterator<Item = JobId>,
    ) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;
        self.get_user_created_table_by_ids_in_txn(&inner.db, job_ids)
            .await
    }

    pub async fn get_user_created_table_by_ids_in_txn<C>(
        &self,
        txn: &C,
        job_ids: impl Iterator<Item = JobId>,
    ) -> MetaResult<Vec<PbTable>>
    where
        C: ConnectionTrait,
    {
        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(
                table::Column::TableId
                    .is_in(job_ids.map(|job_id| job_id.as_mv_table_id()).collect_vec())
                    .and(table::Column::TableType.eq(TableType::Table)),
            )
            .all(txn)
            .await?;
        let streaming_jobs = streaming_job::Entity::find()
            .filter(
                streaming_job::Column::JobId.is_in(
                    table_objs
                        .iter()
                        .map(|(table, _)| table.job_id())
                        .collect_vec(),
                ),
            )
            .all(txn)
            .await?
            .into_iter()
            .map(|job| (job.job_id, job))
            .collect::<HashMap<JobId, streaming_job::Model>>();
        Ok(table_objs
            .into_iter()
            .map(|(table, obj)| {
                let job_id = table.job_id();
                let streaming_job = streaming_jobs.get(&job_id).cloned();
                ObjectModel(table, obj.unwrap(), streaming_job).into()
            })
            .collect())
    }

    pub async fn get_table_by_ids(
        &self,
        table_ids: Vec<TableId>,
        include_dropped_table: bool,
    ) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;
        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::TableId.is_in(table_ids.clone()))
            .all(&inner.db)
            .await?;
        let streaming_jobs = load_streaming_jobs_by_ids(
            &inner.db,
            table_objs.iter().map(|(table, _)| table.job_id()),
        )
        .await?;
        let tables = table_objs.into_iter().map(|(table, obj)| {
            let job_id = table.job_id();
            let streaming_job = streaming_jobs.get(&job_id).cloned();
            ObjectModel(table, obj.unwrap(), streaming_job).into()
        });
        let tables = if include_dropped_table {
            tables
                .chain(inner.dropped_tables.iter().filter_map(|(id, t)| {
                    if table_ids.contains(id) {
                        Some(t.clone())
                    } else {
                        None
                    }
                }))
                .collect()
        } else {
            tables.collect()
        };
        Ok(tables)
    }

    pub async fn get_table_columns(&self, id: TableId) -> MetaResult<Vec<ColumnCatalog>> {
        let inner = self.inner.read().await;
        Ok(get_table_columns(&inner.db, id)
            .await?
            .to_protobuf()
            .into_iter()
            .map(|col| col.into())
            .collect())
    }

    pub async fn get_sink_by_id(&self, sink_id: SinkId) -> MetaResult<Option<PbSink>> {
        let inner = self.inner.read().await;
        let sink_objs = Sink::find_by_id(sink_id)
            .find_also_related(Object)
            .one(&inner.db)
            .await?;
        if let Some((sink, obj)) = sink_objs {
            let streaming_job = streaming_job::Entity::find_by_id(sink.sink_id.as_job_id())
                .one(&inner.db)
                .await?;
            Ok(Some(ObjectModel(sink, obj.unwrap(), streaming_job).into()))
        } else {
            Ok(None)
        }
    }

    pub async fn get_sink_auto_refresh_schema_from(
        &self,
        table_id: TableId,
    ) -> MetaResult<Vec<PbSink>> {
        let inner = self.inner.read().await;
        let sink_objs = Sink::find()
            .find_also_related(Object)
            .filter(sink::Column::AutoRefreshSchemaFromTable.eq(table_id))
            .all(&inner.db)
            .await?;
        let streaming_jobs = load_streaming_jobs_by_ids(
            &inner.db,
            sink_objs.iter().map(|(sink, _)| sink.sink_id.as_job_id()),
        )
        .await?;
        Ok(sink_objs
            .into_iter()
            .map(|(sink, obj)| {
                let streaming_job = streaming_jobs.get(&sink.sink_id.as_job_id()).cloned();
                ObjectModel(sink, obj.unwrap(), streaming_job).into()
            })
            .collect())
    }

    pub async fn get_sink_state_table_ids(&self, sink_id: SinkId) -> MetaResult<Vec<TableId>> {
        let inner = self.inner.read().await;
        let tables: Vec<I32Array> = Fragment::find()
            .select_only()
            .column(fragment::Column::StateTableIds)
            .filter(fragment::Column::JobId.eq(sink_id))
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(tables
            .into_iter()
            .flat_map(|ids| {
                ids.into_inner()
                    .into_iter()
                    .map(|table_id| TableId::new(table_id as _))
            })
            .collect())
    }

    pub async fn get_subscription_by_id(
        &self,
        subscription_id: SubscriptionId,
    ) -> MetaResult<PbSubscription> {
        let inner = self.inner.read().await;
        let subscription_objs = Subscription::find()
            .find_also_related(Object)
            .filter(subscription::Column::SubscriptionId.eq(subscription_id))
            .all(&inner.db)
            .await?;
        let subscription: PbSubscription = subscription_objs
            .into_iter()
            .map(|(subscription, obj)| ObjectModel(subscription, obj.unwrap(), None).into())
            .find_or_first(|_| true)
            .ok_or_else(|| MetaError::catalog_id_not_found("subscription", subscription_id))?;

        Ok(subscription)
    }

    pub async fn get_mv_depended_subscriptions(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<HashMap<TableId, HashMap<SubscriptionId, u64>>> {
        let inner = self.inner.read().await;
        let select = Subscription::find()
            .select_only()
            .select_column(subscription::Column::SubscriptionId)
            .select_column(subscription::Column::DependentTableId)
            .select_column(subscription::Column::RetentionSeconds);
        let select = if let Some(database_id) = database_id {
            select
                .join(JoinType::InnerJoin, subscription::Relation::Object.def())
                .filter(object::Column::DatabaseId.eq(database_id))
        } else {
            select
        };
        let subscription_objs: Vec<(SubscriptionId, TableId, i64)> =
            select.into_tuple().all(&inner.db).await?;
        let mut map: HashMap<_, HashMap<_, _>> = HashMap::new();
        // Write object at the same time we write subscription, so we must be able to get obj
        for (subscription_id, dependent_table_id, retention_seconds) in subscription_objs {
            map.entry(dependent_table_id)
                .or_default()
                .insert(subscription_id, retention_seconds as _);
        }
        Ok(map)
    }

    pub async fn get_all_table_options(&self) -> MetaResult<HashMap<TableId, TableOption>> {
        let inner = self.inner.read().await;
        let table_options: Vec<(TableId, Option<i32>)> = Table::find()
            .select_only()
            .columns([table::Column::TableId, table::Column::RetentionSeconds])
            .into_tuple::<(TableId, Option<i32>)>()
            .all(&inner.db)
            .await?;

        Ok(table_options
            .into_iter()
            .map(|(id, retention_seconds)| {
                (
                    id,
                    TableOption {
                        retention_seconds: retention_seconds.map(|i| i.try_into().unwrap()),
                    },
                )
            })
            .collect())
    }

    pub async fn get_all_streaming_parallelisms(
        &self,
    ) -> MetaResult<HashMap<ObjectId, StreamingParallelism>> {
        let inner = self.inner.read().await;

        let job_parallelisms = StreamingJob::find()
            .select_only()
            .columns([
                streaming_job::Column::JobId,
                streaming_job::Column::Parallelism,
            ])
            .into_tuple::<(ObjectId, StreamingParallelism)>()
            .all(&inner.db)
            .await?;

        Ok(job_parallelisms
            .into_iter()
            .collect::<HashMap<ObjectId, StreamingParallelism>>())
    }

    pub async fn get_table_name_type_mapping(
        &self,
    ) -> MetaResult<HashMap<TableId, (String, String)>> {
        let inner = self.inner.read().await;
        let table_name_types: Vec<(TableId, String, TableType)> = Table::find()
            .select_only()
            .columns([
                table::Column::TableId,
                table::Column::Name,
                table::Column::TableType,
            ])
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(table_name_types
            .into_iter()
            .map(|(id, name, table_type)| {
                (
                    id,
                    (name, PbTableType::from(table_type).as_str_name().to_owned()),
                )
            })
            .collect())
    }

    pub async fn get_table_by_cdc_table_id(
        &self,
        cdc_table_id: &String,
    ) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;
        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::CdcTableId.eq(cdc_table_id))
            .all(&inner.db)
            .await?;
        let streaming_jobs = load_streaming_jobs_by_ids(
            &inner.db,
            table_objs.iter().map(|(table, _)| table.job_id()),
        )
        .await?;
        Ok(table_objs
            .into_iter()
            .map(|(table, obj)| {
                let job_id = table.job_id();
                let streaming_job = streaming_jobs.get(&job_id).cloned();
                ObjectModel(table, obj.unwrap(), streaming_job).into()
            })
            .collect())
    }

    pub async fn get_created_table_ids(&self) -> MetaResult<Vec<TableId>> {
        let inner = self.inner.read().await;

        // created table ids.
        let mut table_ids: Vec<TableId> = StreamingJob::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
            .into_tuple()
            .all(&inner.db)
            .await?;

        // internal table ids.
        let internal_table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .filter(table::Column::BelongsToJobId.is_in(table_ids.clone()))
            .into_tuple()
            .all(&inner.db)
            .await?;
        table_ids.extend(internal_table_ids);

        Ok(table_ids)
    }

    /// Returns column ids of versioned tables.
    /// Being versioned implies using `ColumnAwareSerde`.
    pub async fn get_versioned_table_schemas(
        &self,
    ) -> MetaResult<HashMap<risingwave_common::catalog::TableId, Vec<i32>>> {
        let res = self
            .list_all_state_tables()
            .await?
            .into_iter()
            .filter_map(|t| {
                if t.version.is_some() {
                    let ret = (
                        t.id,
                        t.columns
                            .iter()
                            .map(|c| c.column_desc.as_ref().unwrap().column_id)
                            .collect_vec(),
                    );
                    return Some(ret);
                }
                None
            })
            .collect();
        Ok(res)
    }

    pub async fn get_existing_job_resource_group(
        &self,
        streaming_job_id: JobId,
    ) -> MetaResult<String> {
        let inner = self.inner.read().await;
        get_existing_job_resource_group(&inner.db, streaming_job_id).await
    }

    pub async fn get_database_resource_group(&self, database_id: DatabaseId) -> MetaResult<String> {
        let inner = self.inner.read().await;
        get_database_resource_group(&inner.db, database_id).await
    }

    pub async fn get_existing_job_resource_groups(
        &self,
        streaming_job_ids: Vec<JobId>,
    ) -> MetaResult<HashMap<JobId, String>> {
        let inner = self.inner.read().await;
        let mut resource_groups = HashMap::new();
        for job_id in streaming_job_ids {
            let resource_group = get_existing_job_resource_group(&inner.db, job_id).await?;
            resource_groups.insert(job_id, resource_group);
        }

        Ok(resource_groups)
    }

    pub async fn get_existing_job_database_resource_group(
        &self,
        streaming_job_id: JobId,
    ) -> MetaResult<String> {
        let inner = self.inner.read().await;
        let database_id: DatabaseId = StreamingJob::find_by_id(streaming_job_id)
            .select_only()
            .join(JoinType::InnerJoin, streaming_job::Relation::Object.def())
            .column(object::Column::DatabaseId)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", streaming_job_id))?;

        get_database_resource_group(&inner.db, database_id).await
    }

    pub async fn get_job_streaming_parallelisms(
        &self,
        streaming_job_id: JobId,
    ) -> MetaResult<StreamingParallelism> {
        let inner = self.inner.read().await;

        let job_parallelism: StreamingParallelism = StreamingJob::find_by_id(streaming_job_id)
            .select_only()
            .column(streaming_job::Column::Parallelism)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", streaming_job_id))?;

        Ok(job_parallelism)
    }

    pub async fn get_job_parallelisms(
        &self,
        streaming_job_id: JobId,
    ) -> MetaResult<
        Option<(
            StreamingParallelism,
            Option<String>,
            Option<StreamingParallelism>,
            Option<String>,
        )>,
    > {
        let inner = self.inner.read().await;

        let parallelisms = StreamingJob::find_by_id(streaming_job_id)
            .select_only()
            .columns([
                streaming_job::Column::Parallelism,
                streaming_job::Column::AdaptiveParallelismStrategy,
                streaming_job::Column::BackfillParallelism,
                streaming_job::Column::BackfillAdaptiveParallelismStrategy,
            ])
            .into_tuple::<(
                StreamingParallelism,
                Option<String>,
                Option<StreamingParallelism>,
                Option<String>,
            )>()
            .one(&inner.db)
            .await?;

        Ok(parallelisms)
    }

    pub async fn get_fragment_streaming_job_id(
        &self,
        fragment_id: FragmentId,
    ) -> MetaResult<JobId> {
        let inner = self.inner.read().await;
        let job_id: JobId = Fragment::find_by_id(fragment_id)
            .select_only()
            .column(fragment::Column::JobId)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("fragment", fragment_id))?;
        Ok(job_id)
    }

    pub async fn list_streaming_job_with_database(
        &self,
    ) -> MetaResult<HashMap<DatabaseId, Vec<JobId>>> {
        let inner = self.inner.read().await;
        let database_objects: Vec<(DatabaseId, JobId)> = StreamingJob::find()
            .select_only()
            .column(object::Column::DatabaseId)
            .column(streaming_job::Column::JobId)
            .join(JoinType::LeftJoin, streaming_job::Relation::Object.def())
            .into_tuple()
            .all(&inner.db)
            .await?;

        Ok(database_objects.into_iter().into_group_map())
    }

    // Output: Vec<(table id, db name, schema name, table name, resource group, table type)>
    pub async fn list_table_objects(
        &self,
    ) -> MetaResult<Vec<(TableId, String, String, String, String, TableType)>> {
        let inner = self.inner.read().await;
        Ok(Object::find()
            .select_only()
            .join(JoinType::InnerJoin, object::Relation::Table.def())
            .join(JoinType::InnerJoin, object::Relation::Database2.def())
            .join(JoinType::InnerJoin, object::Relation::Schema2.def())
            .column(object::Column::Oid)
            .column(database::Column::Name)
            .column(schema::Column::Name)
            .column(table::Column::Name)
            .column(database::Column::ResourceGroup)
            .column(table::Column::TableType)
            .into_tuple()
            .all(&inner.db)
            .await?)
    }

    // Output: Vec<(source id, db name, schema name, source name, resource group)>
    pub async fn list_source_objects(
        &self,
    ) -> MetaResult<Vec<(TableId, String, String, String, String)>> {
        let inner = self.inner.read().await;
        Ok(Object::find()
            .select_only()
            .join(JoinType::InnerJoin, object::Relation::Source.def())
            .join(JoinType::InnerJoin, object::Relation::Database2.def())
            .join(JoinType::InnerJoin, object::Relation::Schema2.def())
            .column(object::Column::Oid)
            .column(database::Column::Name)
            .column(schema::Column::Name)
            .column(source::Column::Name)
            .column(database::Column::ResourceGroup)
            .into_tuple()
            .all(&inner.db)
            .await?)
    }

    // Output: Vec<(sink id, db name, schema name, sink name, resource group)>
    pub async fn list_sink_objects(
        &self,
    ) -> MetaResult<Vec<(TableId, String, String, String, String)>> {
        let inner = self.inner.read().await;
        Ok(Object::find()
            .select_only()
            .join(JoinType::InnerJoin, object::Relation::Sink.def())
            .join(JoinType::InnerJoin, object::Relation::Database2.def())
            .join(JoinType::InnerJoin, object::Relation::Schema2.def())
            .column(object::Column::Oid)
            .column(database::Column::Name)
            .column(schema::Column::Name)
            .column(sink::Column::Name)
            .column(database::Column::ResourceGroup)
            .into_tuple()
            .all(&inner.db)
            .await?)
    }

    // Output: Vec<(id, db name, schema name, relation name, type)>
    pub async fn list_relation_objects_by_ids(
        &self,
        ids: &HashSet<ObjectId>,
    ) -> MetaResult<Vec<(ObjectId, String, String, String, String)>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let inner = self.inner.read().await;
        let ids: Vec<_> = ids.iter().cloned().collect();

        let tables: Vec<(ObjectId, String, String, String, TableType)> = Object::find()
            .select_only()
            .join(JoinType::InnerJoin, object::Relation::Table.def())
            .join(JoinType::InnerJoin, object::Relation::Database2.def())
            .join(JoinType::InnerJoin, object::Relation::Schema2.def())
            .filter(object::Column::Oid.is_in(ids.clone()))
            .column(object::Column::Oid)
            .column(database::Column::Name)
            .column(schema::Column::Name)
            .column(table::Column::Name)
            .column(table::Column::TableType)
            .into_tuple()
            .all(&inner.db)
            .await?;

        let sources: Vec<(ObjectId, String, String, String)> = Object::find()
            .select_only()
            .join(JoinType::InnerJoin, object::Relation::Source.def())
            .join(JoinType::InnerJoin, object::Relation::Database2.def())
            .join(JoinType::InnerJoin, object::Relation::Schema2.def())
            .filter(object::Column::Oid.is_in(ids.clone()))
            .column(object::Column::Oid)
            .column(database::Column::Name)
            .column(schema::Column::Name)
            .column(source::Column::Name)
            .into_tuple()
            .all(&inner.db)
            .await?;

        let mut result = Vec::with_capacity(tables.len() + sources.len());
        result.extend(
            tables
                .into_iter()
                .map(|(id, db, schema, name, table_type)| {
                    let relation_type = PbTableType::from(table_type).as_str_name();
                    (id, db, schema, name, relation_type.to_owned())
                }),
        );
        result.extend(
            sources
                .into_iter()
                .map(|(id, db, schema, name)| (id, db, schema, name, "source".to_owned())),
        );
        Ok(result)
    }

    pub async fn get_streaming_job_status(&self, streaming_job_id: JobId) -> MetaResult<JobStatus> {
        let inner = self.inner.read().await;
        let status = StreamingJob::find_by_id(streaming_job_id)
            .select_only()
            .column(streaming_job::Column::JobStatus)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", streaming_job_id))?;
        Ok(status)
    }

    pub async fn get_streaming_job_extra_info(
        &self,
        job_ids: Vec<JobId>,
    ) -> MetaResult<HashMap<JobId, StreamingJobExtraInfo>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        self.get_streaming_job_extra_info_in_txn(&txn, job_ids)
            .await
    }

    pub async fn get_streaming_job_extra_info_in_txn<C>(
        &self,
        txn: &C,
        job_ids: Vec<JobId>,
    ) -> MetaResult<HashMap<JobId, StreamingJobExtraInfo>>
    where
        C: ConnectionTrait,
    {
        fetch_streaming_job_extra_info(txn, job_ids).await
    }
}
