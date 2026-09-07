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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::anyhow;
use parking_lot::lock_api::ArcRwLockReadGuard;
use parking_lot::{RawRwLock, RwLock};
use risingwave_common::catalog::{
    AlterDatabaseParam, CatalogVersion, FunctionId, IndexId, ObjectId,
};
use risingwave_common::id::{ConnectionId, JobId, SchemaId, SourceId, ViewId};
use risingwave_common::system_param::AdaptiveParallelismStrategy;
use risingwave_pb::catalog::{
    PbComment, PbCreateType, PbDatabase, PbFunction, PbIndex, PbSchema, PbSink, PbSource,
    PbSubscription, PbTable, PbView,
};
use risingwave_pb::ddl_service::create_iceberg_table_request::{PbSinkJobInfo, PbTableJobInfo};
use risingwave_pb::ddl_service::replace_job_plan::{
    ReplaceJob, ReplaceMaterializedView, ReplaceSink, ReplaceSource, ReplaceTable,
};
use risingwave_pb::ddl_service::{
    PbTableJobType, StreamingJobResourceType, TableJobType, WaitVersion, alter_name_request,
    alter_owner_request, alter_set_schema_request, alter_swap_rename_request,
    create_connection_request, streaming_job_resource_type,
};
use risingwave_pb::meta::PbTableParallelism;
use risingwave_pb::stream_plan::StreamFragmentGraph;
use risingwave_rpc_client::MetaClient;
use tokio::sync::watch::Receiver;

use super::root_catalog::Catalog;
use super::{DatabaseId, SecretId, SinkId, SubscriptionId, TableId};
use crate::error::Result;
use crate::scheduler::HummockSnapshotManagerRef;
use crate::session::current::notice_to_user;
use crate::user::UserId;

pub type CatalogReadGuard = ArcRwLockReadGuard<RawRwLock, Catalog>;

/// [`CatalogReader`] can read catalog from local catalog and force the holder can not modify it.
#[derive(Clone)]
pub struct CatalogReader(Arc<RwLock<Catalog>>);

impl CatalogReader {
    pub fn new(inner: Arc<RwLock<Catalog>>) -> Self {
        CatalogReader(inner)
    }

    pub fn read_guard(&self) -> CatalogReadGuard {
        // Make this recursive so that one can get this guard in the same thread without fear.
        self.0.read_arc_recursive()
    }
}

/// [`CatalogWriter`] initiate DDL operations (create table/schema/database/function/connection).
/// It will only send rpc to meta and get the catalog version as response.
/// Then it will wait for the local catalog to be synced to the version, which is performed by
/// [observer](`crate::observer::FrontendObserverNode`).
#[async_trait::async_trait]
pub trait CatalogWriter: Send + Sync {
    async fn create_database(
        &self,
        db_name: &str,
        owner: UserId,
        resource_group: &str,
        barrier_interval_ms: Option<u32>,
        checkpoint_frequency: Option<u64>,
    ) -> Result<()>;

    async fn create_schema(
        &self,
        db_id: DatabaseId,
        schema_name: &str,
        owner: UserId,
    ) -> Result<()>;

    async fn create_view(&self, view: PbView, dependencies: HashSet<ObjectId>) -> Result<()>;

    async fn create_materialized_view(
        &self,
        table: PbTable,
        graph: StreamFragmentGraph,
        dependencies: HashSet<ObjectId>,
        resource_type: streaming_job_resource_type::ResourceType,
        if_not_exists: bool,
        refresh_interval_sec: Option<u64>,
    ) -> Result<()>;

    async fn replace_materialized_view(
        &self,
        table: PbTable,
        graph: StreamFragmentGraph,
    ) -> Result<()>;

    async fn create_table(
        &self,
        source: Option<PbSource>,
        table: PbTable,
        graph: StreamFragmentGraph,
        job_type: PbTableJobType,
        if_not_exists: bool,
        dependencies: HashSet<ObjectId>,
    ) -> Result<()>;

    async fn replace_table(
        &self,
        source: Option<PbSource>,
        table: PbTable,
        graph: StreamFragmentGraph,
        job_type: TableJobType,
    ) -> Result<()>;

    async fn replace_source(&self, source: PbSource, graph: StreamFragmentGraph) -> Result<()>;

    async fn create_index(
        &self,
        index: PbIndex,
        table: PbTable,
        graph: StreamFragmentGraph,
        resource_type: streaming_job_resource_type::ResourceType,
        if_not_exists: bool,
    ) -> Result<()>;

    async fn create_source(
        &self,
        source: PbSource,
        graph: Option<StreamFragmentGraph>,
        if_not_exists: bool,
    ) -> Result<()>;

    async fn create_sink(
        &self,
        sink: PbSink,
        graph: StreamFragmentGraph,
        dependencies: HashSet<ObjectId>,
        resource_type: streaming_job_resource_type::ResourceType,
        if_not_exists: bool,
        since_timestamp_epoch: Option<u64>,
    ) -> Result<()>;

    async fn replace_sink(
        &self,
        old_sink_id: SinkId,
        sink: PbSink,
        graph: StreamFragmentGraph,
        dependencies: HashSet<ObjectId>,
        resource_type: streaming_job_resource_type::ResourceType,
    ) -> Result<()>;

    async fn create_subscription(&self, subscription: PbSubscription) -> Result<()>;

    async fn create_function(&self, function: PbFunction) -> Result<()>;

    async fn create_connection(
        &self,
        connection_name: String,
        database_id: DatabaseId,
        schema_id: SchemaId,
        owner_id: UserId,
        connection: create_connection_request::Payload,
    ) -> Result<()>;

    async fn create_secret(
        &self,
        secret_name: String,
        database_id: DatabaseId,
        schema_id: SchemaId,
        owner_id: UserId,
        payload: Vec<u8>,
    ) -> Result<()>;

    async fn comment_on(&self, comment: PbComment) -> Result<()>;

    async fn drop_table(
        &self,
        source_id: Option<SourceId>,
        table_id: TableId,
        cascade: bool,
    ) -> Result<()>;

    async fn drop_materialized_view(&self, table_id: TableId, cascade: bool) -> Result<()>;

    async fn drop_view(&self, view_id: ViewId, cascade: bool) -> Result<()>;

    async fn drop_source(&self, source_id: SourceId, cascade: bool) -> Result<()>;

    async fn reset_source(&self, source_id: SourceId) -> Result<()>;

    async fn drop_sink(&self, sink_id: SinkId, cascade: bool) -> Result<()>;

    async fn drop_subscription(&self, subscription_id: SubscriptionId, cascade: bool)
    -> Result<()>;

    async fn drop_database(&self, database_id: DatabaseId) -> Result<()>;

    async fn drop_schema(&self, schema_id: SchemaId, cascade: bool) -> Result<()>;

    async fn drop_index(&self, index_id: IndexId, cascade: bool) -> Result<()>;

    async fn drop_function(&self, function_id: FunctionId, cascade: bool) -> Result<()>;

    async fn drop_connection(&self, connection_id: ConnectionId, cascade: bool) -> Result<()>;

    async fn drop_secret(&self, secret_id: SecretId, cascade: bool) -> Result<()>;

    async fn alter_secret(
        &self,
        secret_id: SecretId,
        secret_name: String,
        database_id: DatabaseId,
        schema_id: SchemaId,
        owner_id: UserId,
        payload: Vec<u8>,
    ) -> Result<()>;

    async fn alter_subscription_retention(
        &self,
        subscription_id: SubscriptionId,
        retention_seconds: u64,
        definition: String,
    ) -> Result<()>;

    async fn alter_name(
        &self,
        object_id: alter_name_request::Object,
        object_name: &str,
    ) -> Result<()>;

    async fn alter_owner(
        &self,
        object: alter_owner_request::Object,
        owner_id: UserId,
    ) -> Result<()>;

    /// Transfer ownership of all objects in the database owned by `old_owner_ids` to
    /// `new_owner_id`.
    async fn reassign_owned(
        &self,
        old_owner_ids: Vec<UserId>,
        new_owner_id: UserId,
        database_id: DatabaseId,
    ) -> Result<()>;

    /// Revoke the privileges granted by or to `owner_ids` in the database, along with
    /// their default privileges. Fails if the users still own any objects.
    async fn drop_owned(
        &self,
        owner_ids: Vec<UserId>,
        database_id: DatabaseId,
        cascade: bool,
    ) -> Result<()>;

    /// Replace the source in the catalog.
    async fn alter_source(&self, source: PbSource) -> Result<()>;

    async fn alter_parallelism(
        &self,
        job_id: JobId,
        parallelism: PbTableParallelism,
        adaptive_parallelism_strategy: Option<AdaptiveParallelismStrategy>,
        deferred: bool,
    ) -> Result<()>;

    async fn alter_backfill_parallelism(
        &self,
        job_id: JobId,
        parallelism: Option<PbTableParallelism>,
        adaptive_parallelism_strategy: Option<AdaptiveParallelismStrategy>,
        deferred: bool,
    ) -> Result<()>;

    async fn alter_config(
        &self,
        job_id: JobId,
        entries_to_add: HashMap<String, String>,
        keys_to_remove: Vec<String>,
    ) -> Result<()>;

    async fn alter_resource_group(
        &self,
        job_id: JobId,
        resource_group: Option<String>,
        deferred: bool,
    ) -> Result<()>;

    async fn alter_database_resource_group(
        &self,
        database_id: DatabaseId,
        resource_group: Option<String>,
        deferred: bool,
    ) -> Result<()>;

    async fn alter_set_schema(
        &self,
        object: alter_set_schema_request::Object,
        new_schema_id: SchemaId,
    ) -> Result<()>;

    async fn alter_swap_rename(&self, object: alter_swap_rename_request::Object) -> Result<()>;

    async fn alter_database_param(
        &self,
        database_id: DatabaseId,
        param: AlterDatabaseParam,
    ) -> Result<()>;

    async fn create_iceberg_table(
        &self,
        table_job_info: PbTableJobInfo,
        sink_job_info: PbSinkJobInfo,
        iceberg_source: PbSource,
        if_not_exists: bool,
    ) -> Result<()>;

    async fn wait(&self, job_id: Option<JobId>) -> Result<()>;
}

#[derive(Clone)]
pub struct CatalogWriterImpl {
    meta_client: MetaClient,
    catalog_updated_rx: Receiver<CatalogVersion>,
    hummock_snapshot_manager: HummockSnapshotManagerRef,
}

#[async_trait::async_trait]
impl CatalogWriter for CatalogWriterImpl {
    async fn create_database(
        &self,
        db_name: &str,
        owner: UserId,
        resource_group: &str,
        barrier_interval_ms: Option<u32>,
        checkpoint_frequency: Option<u64>,
    ) -> Result<()> {
        let version = self
            .meta_client
            .create_database(PbDatabase {
                name: db_name.to_owned(),
                id: 0.into(),
                owner,
                resource_group: resource_group.to_owned(),
                barrier_interval_ms,
                checkpoint_frequency,
            })
            .await?;
        self.wait_version(version).await
    }

    async fn create_schema(
        &self,
        db_id: DatabaseId,
        schema_name: &str,
        owner: UserId,
    ) -> Result<()> {
        let version = self
            .meta_client
            .create_schema(PbSchema {
                id: 0.into(),
                name: schema_name.to_owned(),
                database_id: db_id,
                owner,
            })
            .await?;
        self.wait_version(version).await
    }

    // TODO: maybe here to pass a materialize plan node
    async fn create_materialized_view(
        &self,
        table: PbTable,
        graph: StreamFragmentGraph,
        dependencies: HashSet<ObjectId>,
        resource_type: streaming_job_resource_type::ResourceType,
        if_not_exists: bool,
        refresh_interval_sec: Option<u64>,
    ) -> Result<()> {
        let create_type = table.get_create_type().unwrap_or(PbCreateType::Foreground);
        let version = self
            .meta_client
            .create_materialized_view(
                table,
                graph,
                dependencies,
                resource_type,
                if_not_exists,
                refresh_interval_sec,
            )
            .await?;
        if matches!(create_type, PbCreateType::Foreground) {
            self.wait_version(version).await?
        }
        Ok(())
    }

    async fn replace_materialized_view(
        &self,
        table: PbTable,
        graph: StreamFragmentGraph,
    ) -> Result<()> {
        // TODO: this is a dummy implementation for debugging only.
        notice_to_user(format!("table: {table:#?}"));
        notice_to_user(format!("graph: {graph:#?}"));

        let version = self
            .meta_client
            .replace_job(
                graph,
                ReplaceJob::ReplaceMaterializedView(ReplaceMaterializedView { table: Some(table) }),
            )
            .await?;

        self.wait_version(version).await
    }

    async fn create_view(&self, view: PbView, dependencies: HashSet<ObjectId>) -> Result<()> {
        let version = self.meta_client.create_view(view, dependencies).await?;
        self.wait_version(version).await
    }

    async fn create_index(
        &self,
        index: PbIndex,
        table: PbTable,
        graph: StreamFragmentGraph,
        resource_type: streaming_job_resource_type::ResourceType,
        if_not_exists: bool,
    ) -> Result<()> {
        let version = self
            .meta_client
            .create_index(index, table, graph, resource_type, if_not_exists)
            .await?;
        self.wait_version(version).await
    }

    async fn create_table(
        &self,
        source: Option<PbSource>,
        table: PbTable,
        graph: StreamFragmentGraph,
        job_type: PbTableJobType,
        if_not_exists: bool,
        dependencies: HashSet<ObjectId>,
    ) -> Result<()> {
        let version = self
            .meta_client
            .create_table(source, table, graph, job_type, if_not_exists, dependencies)
            .await?;
        self.wait_version(version).await
    }

    async fn replace_table(
        &self,
        source: Option<PbSource>,
        table: PbTable,
        graph: StreamFragmentGraph,
        job_type: TableJobType,
    ) -> Result<()> {
        let version = self
            .meta_client
            .replace_job(
                graph,
                ReplaceJob::ReplaceTable(ReplaceTable {
                    source,
                    table: Some(table),
                    job_type: job_type as _,
                }),
            )
            .await?;
        self.wait_version(version).await
    }

    async fn replace_source(&self, source: PbSource, graph: StreamFragmentGraph) -> Result<()> {
        let version = self
            .meta_client
            .replace_job(
                graph,
                ReplaceJob::ReplaceSource(ReplaceSource {
                    source: Some(source),
                }),
            )
            .await?;
        self.wait_version(version).await
    }

    async fn create_source(
        &self,
        source: PbSource,
        graph: Option<StreamFragmentGraph>,
        if_not_exists: bool,
    ) -> Result<()> {
        let version = self
            .meta_client
            .create_source(source, graph, if_not_exists)
            .await?;
        self.wait_version(version).await
    }

    async fn create_sink(
        &self,
        sink: PbSink,
        graph: StreamFragmentGraph,
        dependencies: HashSet<ObjectId>,
        resource_type: streaming_job_resource_type::ResourceType,
        if_not_exists: bool,
        since_timestamp_epoch: Option<u64>,
    ) -> Result<()> {
        let version = self
            .meta_client
            .create_sink(
                sink,
                graph,
                dependencies,
                resource_type,
                if_not_exists,
                since_timestamp_epoch,
            )
            .await?;
        self.wait_version(version).await
    }

    async fn replace_sink(
        &self,
        old_sink_id: SinkId,
        sink: PbSink,
        graph: StreamFragmentGraph,
        dependencies: HashSet<ObjectId>,
        resource_type: streaming_job_resource_type::ResourceType,
    ) -> Result<()> {
        let version = self
            .meta_client
            .replace_job(
                graph,
                ReplaceJob::ReplaceSink(ReplaceSink {
                    sink: Some(sink),
                    old_sink_id,
                    dependencies: dependencies.into_iter().collect(),
                    resource_type: Some(StreamingJobResourceType {
                        resource_type: Some(resource_type),
                    }),
                }),
            )
            .await?;
        self.wait_version(version).await
    }

    async fn create_subscription(&self, subscription: PbSubscription) -> Result<()> {
        let version = self.meta_client.create_subscription(subscription).await?;
        self.wait_version(version).await
    }

    async fn create_function(&self, function: PbFunction) -> Result<()> {
        let version = self.meta_client.create_function(function).await?;
        self.wait_version(version).await
    }

    async fn create_connection(
        &self,
        connection_name: String,
        database_id: DatabaseId,
        schema_id: SchemaId,
        owner_id: UserId,
        connection: create_connection_request::Payload,
    ) -> Result<()> {
        let version = self
            .meta_client
            .create_connection(
                connection_name,
                database_id,
                schema_id,
                owner_id,
                connection,
            )
            .await?;
        self.wait_version(version).await
    }

    async fn create_secret(
        &self,
        secret_name: String,
        database_id: DatabaseId,
        schema_id: SchemaId,
        owner_id: UserId,
        payload: Vec<u8>,
    ) -> Result<()> {
        let version = self
            .meta_client
            .create_secret(secret_name, database_id, schema_id, owner_id, payload)
            .await?;
        self.wait_version(version).await
    }

    async fn comment_on(&self, comment: PbComment) -> Result<()> {
        let version = self.meta_client.comment_on(comment).await?;
        self.wait_version(version).await
    }

    async fn drop_table(
        &self,
        source_id: Option<SourceId>,
        table_id: TableId,
        cascade: bool,
    ) -> Result<()> {
        let version = self
            .meta_client
            .drop_table(source_id, table_id, cascade)
            .await?;
        self.wait_version(version).await
    }

    async fn drop_materialized_view(&self, table_id: TableId, cascade: bool) -> Result<()> {
        let version = self
            .meta_client
            .drop_materialized_view(table_id, cascade)
            .await?;
        self.wait_version(version).await
    }

    async fn drop_view(&self, view_id: ViewId, cascade: bool) -> Result<()> {
        let version = self.meta_client.drop_view(view_id, cascade).await?;
        self.wait_version(version).await
    }

    async fn drop_source(&self, source_id: SourceId, cascade: bool) -> Result<()> {
        let version = self.meta_client.drop_source(source_id, cascade).await?;
        self.wait_version(version).await
    }

    async fn reset_source(&self, source_id: SourceId) -> Result<()> {
        let version = self.meta_client.reset_source(source_id).await?;
        self.wait_version(version).await
    }

    async fn drop_sink(&self, sink_id: SinkId, cascade: bool) -> Result<()> {
        let version = self.meta_client.drop_sink(sink_id, cascade).await?;
        self.wait_version(version).await
    }

    async fn drop_subscription(
        &self,
        subscription_id: SubscriptionId,
        cascade: bool,
    ) -> Result<()> {
        let version = self
            .meta_client
            .drop_subscription(subscription_id, cascade)
            .await?;
        self.wait_version(version).await
    }

    async fn drop_index(&self, index_id: IndexId, cascade: bool) -> Result<()> {
        let version = self.meta_client.drop_index(index_id, cascade).await?;
        self.wait_version(version).await
    }

    async fn drop_function(&self, function_id: FunctionId, cascade: bool) -> Result<()> {
        let version = self.meta_client.drop_function(function_id, cascade).await?;
        self.wait_version(version).await
    }

    async fn drop_schema(&self, schema_id: SchemaId, cascade: bool) -> Result<()> {
        let version = self.meta_client.drop_schema(schema_id, cascade).await?;
        self.wait_version(version).await
    }

    async fn drop_database(&self, database_id: DatabaseId) -> Result<()> {
        let version = self.meta_client.drop_database(database_id).await?;
        self.wait_version(version).await
    }

    async fn drop_connection(&self, connection_id: ConnectionId, cascade: bool) -> Result<()> {
        let version = self
            .meta_client
            .drop_connection(connection_id, cascade)
            .await?;
        self.wait_version(version).await
    }

    async fn drop_secret(&self, secret_id: SecretId, cascade: bool) -> Result<()> {
        let version = self.meta_client.drop_secret(secret_id, cascade).await?;
        self.wait_version(version).await
    }

    async fn alter_name(
        &self,
        object_id: alter_name_request::Object,
        object_name: &str,
    ) -> Result<()> {
        let version = self.meta_client.alter_name(object_id, object_name).await?;
        self.wait_version(version).await
    }

    async fn alter_owner(
        &self,
        object: alter_owner_request::Object,
        owner_id: UserId,
    ) -> Result<()> {
        let version = self.meta_client.alter_owner(object, owner_id).await?;
        self.wait_version(version).await
    }

    async fn reassign_owned(
        &self,
        old_owner_ids: Vec<UserId>,
        new_owner_id: UserId,
        database_id: DatabaseId,
    ) -> Result<()> {
        let version = self
            .meta_client
            .reassign_owned(old_owner_ids, new_owner_id, database_id)
            .await?;
        self.wait_version(version).await
    }

    async fn drop_owned(
        &self,
        owner_ids: Vec<UserId>,
        database_id: DatabaseId,
        cascade: bool,
    ) -> Result<()> {
        let version = self
            .meta_client
            .drop_owned(owner_ids, database_id, cascade)
            .await?;
        self.wait_version(version).await
    }

    async fn alter_set_schema(
        &self,
        object: alter_set_schema_request::Object,
        new_schema_id: SchemaId,
    ) -> Result<()> {
        let version = self
            .meta_client
            .alter_set_schema(object, new_schema_id)
            .await?;
        self.wait_version(version).await
    }

    async fn alter_source(&self, source: PbSource) -> Result<()> {
        let version = self.meta_client.alter_source(source).await?;
        self.wait_version(version).await
    }

    async fn alter_parallelism(
        &self,
        job_id: JobId,
        parallelism: PbTableParallelism,
        adaptive_parallelism_strategy: Option<AdaptiveParallelismStrategy>,
        deferred: bool,
    ) -> Result<()> {
        self.meta_client
            .alter_parallelism(job_id, parallelism, adaptive_parallelism_strategy, deferred)
            .await?;
        Ok(())
    }

    async fn alter_backfill_parallelism(
        &self,
        job_id: JobId,
        parallelism: Option<PbTableParallelism>,
        adaptive_parallelism_strategy: Option<AdaptiveParallelismStrategy>,
        deferred: bool,
    ) -> Result<()> {
        self.meta_client
            .alter_backfill_parallelism(
                job_id,
                parallelism,
                adaptive_parallelism_strategy,
                deferred,
            )
            .await?;
        Ok(())
    }

    async fn alter_config(
        &self,
        job_id: JobId,
        entries_to_add: HashMap<String, String>,
        keys_to_remove: Vec<String>,
    ) -> Result<()> {
        self.meta_client
            .alter_streaming_job_config(job_id, entries_to_add, keys_to_remove)
            .await?;
        Ok(())
    }

    async fn alter_swap_rename(&self, object: alter_swap_rename_request::Object) -> Result<()> {
        let version = self.meta_client.alter_swap_rename(object).await?;
        self.wait_version(version).await
    }

    async fn alter_secret(
        &self,
        secret_id: SecretId,
        secret_name: String,
        database_id: DatabaseId,
        schema_id: SchemaId,
        owner_id: UserId,
        payload: Vec<u8>,
    ) -> Result<()> {
        let version = self
            .meta_client
            .alter_secret(
                secret_id,
                secret_name,
                database_id,
                schema_id,
                owner_id,
                payload,
            )
            .await?;
        self.wait_version(version).await
    }

    async fn alter_subscription_retention(
        &self,
        subscription_id: SubscriptionId,
        retention_seconds: u64,
        definition: String,
    ) -> Result<()> {
        let version = self
            .meta_client
            .alter_subscription_retention(subscription_id, retention_seconds, definition)
            .await?;
        self.wait_version(version).await
    }

    async fn alter_resource_group(
        &self,
        job_id: JobId,
        resource_group: Option<String>,
        deferred: bool,
    ) -> Result<()> {
        self.meta_client
            .alter_resource_group(job_id, resource_group, deferred)
            .await
            .map_err(|e| anyhow!(e))?;

        Ok(())
    }

    async fn alter_database_resource_group(
        &self,
        database_id: DatabaseId,
        resource_group: Option<String>,
        deferred: bool,
    ) -> Result<()> {
        let version = self
            .meta_client
            .alter_database_resource_group(database_id, resource_group, deferred)
            .await
            .map_err(|e| anyhow!(e))?;
        self.wait_version(version).await
    }

    async fn alter_database_param(
        &self,
        database_id: DatabaseId,
        param: AlterDatabaseParam,
    ) -> Result<()> {
        let version = self
            .meta_client
            .alter_database_param(database_id, param)
            .await
            .map_err(|e| anyhow!(e))?;
        self.wait_version(version).await
    }

    async fn create_iceberg_table(
        &self,
        table_job_info: PbTableJobInfo,
        sink_job_info: PbSinkJobInfo,
        iceberg_source: PbSource,
        if_not_exists: bool,
    ) -> Result<()> {
        let version = Box::pin(self.meta_client.create_iceberg_table(
            table_job_info,
            sink_job_info,
            iceberg_source,
            if_not_exists,
        ))
        .await?;
        self.wait_version(version).await
    }

    async fn wait(&self, job_id: Option<JobId>) -> Result<()> {
        let version = self
            .meta_client
            .wait(job_id)
            .await
            .map_err(|e| anyhow!(e))?;
        self.wait_version(version).await
    }
}

impl CatalogWriterImpl {
    pub fn new(
        meta_client: MetaClient,
        catalog_updated_rx: Receiver<CatalogVersion>,
        hummock_snapshot_manager: HummockSnapshotManagerRef,
    ) -> Self {
        Self {
            meta_client,
            catalog_updated_rx,
            hummock_snapshot_manager,
        }
    }

    async fn wait_version(&self, version: WaitVersion) -> Result<()> {
        let mut rx = self.catalog_updated_rx.clone();
        while *rx.borrow_and_update() < version.catalog_version {
            rx.changed().await.map_err(|e| anyhow!(e))?;
        }
        self.hummock_snapshot_manager
            .wait(version.hummock_version_id)
            .await;
        Ok(())
    }
}
