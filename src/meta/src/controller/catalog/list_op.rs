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
use risingwave_common::id::JobId;
use risingwave_meta_model::refresh_job::{self, RefreshState};
use risingwave_pb::meta::ObjectDependency as PbObjectDependency;
use sea_orm::prelude::DateTime;

use super::*;
use crate::controller::fragment::FragmentTypeMaskExt;
use crate::controller::utils::load_streaming_jobs_by_ids;

impl CatalogController {
    pub async fn list_time_travel_table_ids(&self) -> MetaResult<Vec<TableId>> {
        self.inner.read().await.list_time_travel_table_ids().await
    }

    pub async fn list_refresh_jobs(&self) -> MetaResult<Vec<refresh_job::Model>> {
        let inner = self.inner.read().await;
        Ok(RefreshJob::find().all(&inner.db).await?)
    }

    pub async fn get_refresh_job_state_by_table_id(
        &self,
        table_id: TableId,
    ) -> MetaResult<RefreshState> {
        let inner = self.inner.read().await;
        let (refresh_job_state,): (RefreshState,) = RefreshJob::find_by_id(table_id)
            .select_only()
            .select_column(refresh_job::Column::CurrentStatus)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("refresh_job", table_id))?;
        Ok(refresh_job_state)
    }

    pub async fn list_refreshable_table_ids(&self) -> MetaResult<Vec<TableId>> {
        let inner = self.inner.read().await;
        Ok(Table::find()
            .select_only()
            .column(table::Column::TableId)
            .filter(table::Column::Refreshable.eq(true))
            .into_tuple()
            .all(&inner.db)
            .await?)
    }

    pub async fn list_stream_job_desc_for_telemetry(
        &self,
    ) -> MetaResult<Vec<MetaTelemetryJobDesc>> {
        let inner = self.inner.read().await;
        let info: Vec<(TableId, Option<Property>)> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .column(source::Column::WithProperties)
            .join(JoinType::LeftJoin, table::Relation::Source.def())
            .filter(
                table::Column::TableType
                    .eq(TableType::Table)
                    .or(table::Column::TableType.eq(TableType::MaterializedView)),
            )
            .into_tuple()
            .all(&inner.db)
            .await?;

        Ok(info
            .into_iter()
            .map(|(table_id, properties)| {
                let connector_info = if let Some(inner_props) = properties {
                    inner_props
                        .inner_ref()
                        .get(UPSTREAM_SOURCE_KEY)
                        .map(|v| v.to_lowercase())
                } else {
                    None
                };
                MetaTelemetryJobDesc {
                    table_id,
                    connector: connector_info,
                    optimization: vec![],
                }
            })
            .collect())
    }

    pub async fn list_creating_jobs(
        &self,
        include_initial: bool,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<Vec<(JobId, String, DateTime, CreateType, bool)>> {
        let inner = self.inner.read().await;
        let status_cond = if include_initial {
            streaming_job::Column::JobStatus.is_in([JobStatus::Initial, JobStatus::Creating])
        } else {
            streaming_job::Column::JobStatus.eq(JobStatus::Creating)
        };
        let database_cond = database_id
            .map(|database_id| object::Column::DatabaseId.eq(database_id))
            .unwrap_or_else(|| SimpleExpr::from(true));
        let filter_cond = status_cond.and(database_cond);
        let object_columns = [object::Column::InitializedAt];
        let streaming_job_columns = [
            streaming_job::Column::CreateType,
            streaming_job::Column::IsServerlessBackfill,
        ];
        let mut table_info: Vec<(JobId, String, DateTime, CreateType, bool)> = Table::find()
            .select_only()
            .columns([table::Column::TableId, table::Column::Definition])
            .columns(object_columns)
            .columns(streaming_job_columns)
            .join(JoinType::LeftJoin, table::Relation::Object1.def())
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(filter_cond.clone())
            .into_tuple()
            .all(&inner.db)
            .await?;
        let sink_info: Vec<(JobId, String, DateTime, CreateType, bool)> = Sink::find()
            .select_only()
            .columns([sink::Column::SinkId, sink::Column::Definition])
            .columns(object_columns)
            .columns(streaming_job_columns)
            .join(JoinType::LeftJoin, sink::Relation::Object.def())
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(filter_cond)
            .into_tuple()
            .all(&inner.db)
            .await?;

        table_info.extend(sink_info);

        Ok(table_info)
    }

    pub async fn list_databases(&self) -> MetaResult<Vec<PbDatabase>> {
        let inner = self.inner.read().await;
        inner.list_databases().await
    }

    pub async fn list_all_object_dependencies(&self) -> MetaResult<Vec<PbObjectDependency>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;
        let dependencies = list_object_dependencies(&txn, true).await?;
        txn.commit().await?;
        Ok(dependencies)
    }

    pub async fn list_created_object_dependencies(&self) -> MetaResult<Vec<PbObjectDependency>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;
        let dependencies = list_object_dependencies(&txn, false).await?;
        txn.commit().await?;
        Ok(dependencies)
    }

    pub async fn list_schemas(&self) -> MetaResult<Vec<PbSchema>> {
        let inner = self.inner.read().await;
        inner.list_schemas().await
    }

    /// [`Self::list_tables_by_type`] with all types.
    pub async fn list_all_state_tables(&self) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;
        inner.list_all_state_tables().await
    }

    pub async fn list_readonly_table_ids(&self, schema_id: SchemaId) -> MetaResult<Vec<TableId>> {
        let inner = self.inner.read().await;
        let table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .join(JoinType::InnerJoin, table::Relation::Object1.def())
            .filter(
                object::Column::SchemaId
                    .eq(schema_id)
                    .and(table::Column::TableType.ne(TableType::Table)),
            )
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(table_ids)
    }

    pub async fn list_dml_table_ids(&self, schema_id: SchemaId) -> MetaResult<Vec<TableId>> {
        let inner = self.inner.read().await;
        let table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .join(JoinType::InnerJoin, table::Relation::Object1.def())
            .filter(
                object::Column::SchemaId
                    .eq(schema_id)
                    .and(table::Column::TableType.eq(TableType::Table)),
            )
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(table_ids)
    }

    pub async fn list_view_ids(&self, schema_id: SchemaId) -> MetaResult<Vec<ViewId>> {
        let inner = self.inner.read().await;
        let view_ids: Vec<ViewId> = View::find()
            .select_only()
            .column(view::Column::ViewId)
            .join(JoinType::InnerJoin, view::Relation::Object.def())
            .filter(object::Column::SchemaId.eq(schema_id))
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(view_ids)
    }

    /// Use [`Self::list_all_state_tables`] to get all types.
    pub async fn list_tables_by_type(&self, table_type: TableType) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;
        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::TableType.eq(table_type))
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

    pub async fn list_sources(&self) -> MetaResult<Vec<PbSource>> {
        let inner = self.inner.read().await;
        inner.list_sources().await
    }

    // Return a hashmap to distinguish whether each source is shared or not.
    pub async fn list_source_id_with_shared_types(&self) -> MetaResult<HashMap<SourceId, bool>> {
        let inner = self.inner.read().await;
        let source_ids: Vec<(SourceId, Option<StreamSourceInfo>)> = Source::find()
            .select_only()
            .columns([source::Column::SourceId, source::Column::SourceInfo])
            .into_tuple()
            .all(&inner.db)
            .await?;

        Ok(source_ids
            .into_iter()
            .map(|(source_id, info)| {
                (
                    source_id,
                    info.map(|info| info.to_protobuf().cdc_source_job)
                        .unwrap_or(false),
                )
            })
            .collect())
    }

    pub async fn list_connections(&self) -> MetaResult<Vec<PbConnection>> {
        let inner = self.inner.read().await;
        let conn_objs = Connection::find()
            .find_also_related(Object)
            .all(&inner.db)
            .await?;
        Ok(conn_objs
            .into_iter()
            .map(|(conn, obj)| ObjectModel(conn, obj.unwrap(), None).into())
            .collect())
    }

    pub async fn list_source_ids(&self, schema_id: SchemaId) -> MetaResult<Vec<SourceId>> {
        let inner = self.inner.read().await;
        let source_ids: Vec<SourceId> = Source::find()
            .select_only()
            .column(source::Column::SourceId)
            .join(JoinType::InnerJoin, source::Relation::Object.def())
            .filter(object::Column::SchemaId.eq(schema_id))
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(source_ids)
    }

    pub async fn list_indexes(&self) -> MetaResult<Vec<PbIndex>> {
        let inner = self.inner.read().await;
        inner.list_indexes().await
    }

    pub async fn list_sinks(&self) -> MetaResult<Vec<PbSink>> {
        let inner = self.inner.read().await;
        inner.list_sinks().await
    }

    pub async fn list_subscriptions(&self) -> MetaResult<Vec<PbSubscription>> {
        let inner = self.inner.read().await;
        inner.list_subscriptions().await
    }

    pub async fn list_views(&self) -> MetaResult<Vec<PbView>> {
        let inner = self.inner.read().await;
        inner.list_views().await
    }

    pub async fn list_users(&self) -> MetaResult<Vec<PbUserInfo>> {
        let inner = self.inner.read().await;
        inner.list_users().await
    }

    pub async fn list_functions(&self) -> MetaResult<Vec<PbFunction>> {
        let inner = self.inner.read().await;
        inner.list_functions().await
    }

    /// `Unmigrated` refers to table-fragments that have not yet been migrated to the new plan (for now, this means
    /// table-fragments that do not use `UpstreamSinkUnion` operator to receive multiple upstream sinks)
    pub async fn list_unmigrated_tables(&self) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;

        let table_objs = Table::find()
            .filter(table::Column::TableType.eq(TableType::Table))
            .find_also_related(Object)
            .join(JoinType::InnerJoin, object::Relation::Fragment.def())
            .filter(FragmentTypeMask::intersects(FragmentTypeFlag::Mview).and(
                FragmentTypeMask::disjoint(FragmentTypeFlag::UpstreamSinkUnion),
            ))
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

    pub async fn list_sink_ids(&self, database_id: Option<DatabaseId>) -> MetaResult<Vec<SinkId>> {
        let inner = self.inner.read().await;

        let mut query = Sink::find().select_only().column(sink::Column::SinkId);

        if let Some(database_id) = database_id {
            query = query
                .join(JoinType::InnerJoin, sink::Relation::Object.def())
                .filter(object::Column::DatabaseId.eq(database_id));
        }

        let sink_ids: Vec<SinkId> = query.into_tuple().all(&inner.db).await?;

        Ok(sink_ids)
    }
}
