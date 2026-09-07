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

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use anyhow::{Context, anyhow};
use await_tree::InstrumentAwait;
use either::Either;
use itertools::Itertools;
use risingwave_common::catalog::{
    AlterDatabaseParam, ColumnCatalog, ColumnId, Field, FragmentTypeFlag,
};
use risingwave_common::hash::VnodeCountCompat;
use risingwave_common::id::{JobId, TableId};
use risingwave_common::secret::{LocalSecretManager, SecretEncryption};
use risingwave_common::system_param::adaptive_parallelism_strategy::parse_strategy;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont_mut;
use risingwave_common::{bail, bail_not_implemented};
use risingwave_connector::WithOptionsSecResolved;
use risingwave_connector::connector_common::validate_connection;
use risingwave_connector::sink::SinkParam;
use risingwave_connector::sink::iceberg::IcebergSink;
use risingwave_connector::source::cdc::CdcScanOptions;
use risingwave_connector::source::{
    ConnectorProperties, SourceEnumeratorContext, UPSTREAM_SOURCE_KEY,
};
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::{
    ConnectionId, DatabaseId, DispatcherType, FragmentId, FunctionId, IndexId, JobStatus, ObjectId,
    SchemaId, SecretId, SinkId, SourceId, StreamingParallelism, SubscriptionId, UserId, ViewId,
    streaming_job,
};
use risingwave_pb::catalog::{
    Comment, Connection, CreateType, Database, Function, PbTable, Schema, Secret, Source,
    Subscription, Table, View,
};
use risingwave_pb::ddl_service::alter_owner_request::Object;
use risingwave_pb::ddl_service::{
    DdlProgress, TableJobType, WaitVersion, alter_name_request, alter_set_schema_request,
    alter_swap_rename_request, streaming_job_resource_type,
};
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType as PbFragmentDistributionType;
use risingwave_pb::plan_common::PbColumnCatalog;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    PbDispatchOutputMapping, PbStreamFragmentGraph, PbStreamNode, PbUpstreamSinkInfo,
    StreamFragmentGraph as StreamFragmentGraphProto,
};
use risingwave_pb::telemetry::{PbTelemetryDatabaseObject, PbTelemetryEventStage};
use strum::Display;
use thiserror_ext::AsReport;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::Instrument;

use crate::barrier::{BarrierManagerRef, Command};
use crate::controller::catalog::{DropTableConnectorContext, ReleaseContext};
use crate::controller::streaming_job::{FinishAutoRefreshSchemaSinkContext, SinkIntoTableContext};
use crate::controller::utils::build_select_node_list;
use crate::error::{MetaErrorInner, bail_invalid_parameter};
use crate::manager::iceberg_compaction::IcebergCompactionManagerRef;
use crate::manager::iceberg_pk_index_sink::IcebergPkIndexSinkManager;
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{
    IGNORED_NOTIFICATION_VERSION, LocalNotification, MetaSrvEnv, MetadataManager,
    NotificationVersion, StreamingJob, StreamingJobType,
};
use crate::model::{
    DownstreamFragmentRelation, FragmentDownstreamRelation, FragmentId as CatalogFragmentId,
    StreamContext, StreamJobFragments, StreamJobFragmentsToCreate,
};
use crate::stream::cdc::{
    parallel_cdc_table_backfill_fragment, try_init_parallel_cdc_table_snapshot_splits,
};
use crate::stream::{
    ActorGraphBuildResult, ActorGraphBuilder, AutoRefreshSchemaSinkContext,
    CompleteStreamFragmentGraph, CreateStreamingJobContext, CreateStreamingJobOption,
    FragmentGraphDownstreamContext, FragmentGraphUpstreamContext, GlobalStreamManagerRef,
    ParallelismPolicy, ReplaceStreamJobContext, ReschedulePolicy, SourceChange, SourceManagerRef,
    StreamFragmentGraph, UpstreamSinkInfo, check_sink_fragments_support_refresh_schema,
    cleanup_dropped_streaming_jobs, create_source_worker, first_variant_column,
    rewrite_refresh_schema_sink_fragment, state_match, validate_sink,
};
use crate::telemetry::report_event;
use crate::{MetaError, MetaResult};

#[derive(PartialEq)]
pub enum DropMode {
    Restrict,
    Cascade,
}

impl DropMode {
    pub fn from_request_setting(cascade: bool) -> DropMode {
        if cascade {
            DropMode::Cascade
        } else {
            DropMode::Restrict
        }
    }
}

#[derive(strum::AsRefStr)]
pub enum StreamingJobId {
    MaterializedView(TableId),
    Sink(SinkId),
    Table(Option<SourceId>, TableId),
    Index(IndexId),
}

impl std::fmt::Display for StreamingJobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())?;
        write!(f, "({})", self.id())
    }
}

impl StreamingJobId {
    fn id(&self) -> JobId {
        match self {
            StreamingJobId::MaterializedView(id) | StreamingJobId::Table(_, id) => id.as_job_id(),
            StreamingJobId::Index(id) => id.as_job_id(),
            StreamingJobId::Sink(id) => id.as_job_id(),
        }
    }
}

/// It’s used to describe the information of the job that needs to be replaced
/// and it will be used during replacing table and creating sink into table operations.
pub struct ReplaceStreamJobInfo {
    pub streaming_job: StreamingJob,
    pub fragment_graph: StreamFragmentGraphProto,
}

#[derive(Display)]
pub enum DdlCommand {
    CreateDatabase(Database),
    DropDatabase(DatabaseId),
    CreateSchema(Schema),
    DropSchema(SchemaId, DropMode),
    CreateNonSharedSource(Source, Option<TableId>),
    DropSource(SourceId, DropMode),
    ResetSource(SourceId),
    CreateFunction(Function),
    DropFunction(FunctionId, DropMode),
    CreateView(View, HashSet<ObjectId>),
    DropView(ViewId, DropMode),
    CreateStreamingJob {
        stream_job: StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
        dependencies: HashSet<ObjectId>,
        resource_type: streaming_job_resource_type::ResourceType,
        if_not_exists: bool,
        refresh_interval_sec: Option<u64>,
        replace_sink: Option<SinkId>,
        since_timestamp_epoch: Option<u64>,
    },
    DropStreamingJob {
        job_id: StreamingJobId,
        drop_mode: DropMode,
    },
    AlterName(alter_name_request::Object, String),
    AlterSwapRename(alter_swap_rename_request::Object),
    ReplaceStreamJob(ReplaceStreamJobInfo),
    AlterNonSharedSource(Source),
    AlterObjectOwner(Object, UserId),
    ReassignOwned {
        old_owner_ids: Vec<UserId>,
        new_owner_id: UserId,
        database_id: DatabaseId,
    },
    DropOwned {
        owner_ids: Vec<UserId>,
        database_id: DatabaseId,
        cascade: bool,
    },
    AlterSetSchema(alter_set_schema_request::Object, SchemaId),
    CreateConnection(Connection),
    DropConnection(ConnectionId, DropMode),
    CreateSecret(Secret),
    AlterSecret(Secret),
    DropSecret(SecretId, DropMode),
    CommentOn(Comment),
    CreateSubscription(Subscription),
    DropSubscription(SubscriptionId, DropMode),
    AlterSubscriptionRetention {
        subscription_id: SubscriptionId,
        retention_seconds: u64,
        definition: String,
    },
    AlterDatabaseParam(DatabaseId, AlterDatabaseParam),
    AlterDatabaseResourceGroup(DatabaseId, Option<String>, bool),
    AlterStreamingJobConfig(JobId, HashMap<String, String>, Vec<String>),
}

impl DdlCommand {
    /// Returns the name or ID of the object that this command operates on, for observability and debugging.
    fn object(&self) -> Either<String, ObjectId> {
        use Either::*;
        match self {
            DdlCommand::CreateDatabase(database) => Left(database.name.clone()),
            DdlCommand::DropDatabase(id) => Right(id.as_object_id()),
            DdlCommand::CreateSchema(schema) => Left(schema.name.clone()),
            DdlCommand::DropSchema(id, _) => Right(id.as_object_id()),
            DdlCommand::CreateNonSharedSource(source, _) => Left(source.name.clone()),
            DdlCommand::DropSource(id, _) => Right(id.as_object_id()),
            DdlCommand::ResetSource(id) => Right(id.as_object_id()),
            DdlCommand::CreateFunction(function) => Left(function.name.clone()),
            DdlCommand::DropFunction(id, _) => Right(id.as_object_id()),
            DdlCommand::CreateView(view, _) => Left(view.name.clone()),
            DdlCommand::DropView(id, _) => Right(id.as_object_id()),
            DdlCommand::CreateStreamingJob { stream_job, .. } => Left(stream_job.name()),
            DdlCommand::DropStreamingJob { job_id, .. } => Right(job_id.id().as_object_id()),
            DdlCommand::AlterName(object, _) => Left(format!("{object:?}")),
            DdlCommand::AlterSwapRename(object) => Left(format!("{object:?}")),
            DdlCommand::ReplaceStreamJob(info) => Left(info.streaming_job.name()),
            DdlCommand::AlterNonSharedSource(source) => Left(source.name.clone()),
            DdlCommand::AlterObjectOwner(object, _) => Left(format!("{object:?}")),
            DdlCommand::ReassignOwned { old_owner_ids, .. } => {
                Left(format!("owned by users {old_owner_ids:?}"))
            }
            DdlCommand::DropOwned { owner_ids, .. } => {
                Left(format!("owned by users {owner_ids:?}"))
            }
            DdlCommand::AlterSetSchema(object, _) => Left(format!("{object:?}")),
            DdlCommand::CreateConnection(connection) => Left(connection.name.clone()),
            DdlCommand::DropConnection(id, _) => Right(id.as_object_id()),
            DdlCommand::CreateSecret(secret) => Left(secret.name.clone()),
            DdlCommand::AlterSecret(secret) => Left(secret.name.clone()),
            DdlCommand::DropSecret(id, _) => Right(id.as_object_id()),
            DdlCommand::CommentOn(comment) => Right(comment.table_id.into()),
            DdlCommand::CreateSubscription(subscription) => Left(subscription.name.clone()),
            DdlCommand::DropSubscription(id, _) => Right(id.as_object_id()),
            DdlCommand::AlterSubscriptionRetention {
                subscription_id, ..
            } => Right(subscription_id.as_object_id()),
            DdlCommand::AlterDatabaseParam(id, _) => Right(id.as_object_id()),
            DdlCommand::AlterDatabaseResourceGroup(id, _, _) => Right(id.as_object_id()),
            DdlCommand::AlterStreamingJobConfig(job_id, _, _) => Right(job_id.as_object_id()),
        }
    }

    fn allow_in_recovery(&self) -> bool {
        match self {
            DdlCommand::DropDatabase(_)
            | DdlCommand::DropSchema(_, _)
            | DdlCommand::DropSource(_, _)
            | DdlCommand::DropFunction(_, _)
            | DdlCommand::DropView(_, _)
            | DdlCommand::DropStreamingJob { .. }
            | DdlCommand::DropConnection(_, _)
            | DdlCommand::DropSecret(_, _)
            | DdlCommand::DropSubscription(_, _)
            | DdlCommand::AlterName(_, _)
            | DdlCommand::AlterObjectOwner(_, _)
            | DdlCommand::ReassignOwned { .. }
            | DdlCommand::DropOwned { .. }
            | DdlCommand::AlterSetSchema(_, _)
            | DdlCommand::CreateDatabase(_)
            | DdlCommand::CreateSchema(_)
            | DdlCommand::CreateFunction(_)
            | DdlCommand::CreateView(_, _)
            | DdlCommand::CreateConnection(_)
            | DdlCommand::CommentOn(_)
            | DdlCommand::CreateSecret(_)
            | DdlCommand::AlterSecret(_)
            | DdlCommand::AlterSwapRename(_)
            | DdlCommand::AlterDatabaseParam(_, _)
            | DdlCommand::AlterDatabaseResourceGroup(_, _, _)
            | DdlCommand::AlterStreamingJobConfig(_, _, _)
            | DdlCommand::AlterSubscriptionRetention { .. } => true,
            DdlCommand::CreateStreamingJob { .. }
            | DdlCommand::CreateNonSharedSource(_, _)
            | DdlCommand::ReplaceStreamJob(_)
            | DdlCommand::AlterNonSharedSource(_)
            | DdlCommand::ResetSource(_)
            | DdlCommand::CreateSubscription(_) => false,
        }
    }
}

#[derive(Clone)]
pub struct DdlController {
    pub(crate) env: MetaSrvEnv,

    pub(crate) metadata_manager: MetadataManager,
    pub(crate) stream_manager: GlobalStreamManagerRef,
    pub(crate) source_manager: SourceManagerRef,
    barrier_manager: BarrierManagerRef,
    sink_manager: SinkCoordinatorManager,
    iceberg_compaction_manager: IcebergCompactionManagerRef,
    iceberg_pk_index_sink_manager: IcebergPkIndexSinkManager,

    // The semaphore is used to limit the number of concurrent streaming job creation.
    pub(crate) creating_streaming_job_permits: Arc<CreatingStreamingJobPermit>,

    /// Sequence number for DDL commands, used for observability and debugging.
    seq: Arc<AtomicU64>,
}

#[derive(Clone)]
pub struct CreatingStreamingJobPermit {
    pub(crate) semaphore: Arc<Semaphore>,
}

impl CreatingStreamingJobPermit {
    async fn new(env: &MetaSrvEnv) -> Self {
        let mut permits = env
            .system_params_reader()
            .await
            .max_concurrent_creating_streaming_jobs() as usize;
        if permits == 0 {
            // if the system parameter is set to zero, use the max permitted value.
            permits = Semaphore::MAX_PERMITS;
        }
        let semaphore = Arc::new(Semaphore::new(permits));

        let (local_notification_tx, mut local_notification_rx) =
            tokio::sync::mpsc::unbounded_channel();
        env.notification_manager()
            .insert_local_sender(local_notification_tx);
        let semaphore_clone = semaphore.clone();
        tokio::spawn(async move {
            while let Some(notification) = local_notification_rx.recv().await {
                let LocalNotification::SystemParamsChange(p) = &notification else {
                    continue;
                };
                let mut new_permits = p.max_concurrent_creating_streaming_jobs() as usize;
                if new_permits == 0 {
                    new_permits = Semaphore::MAX_PERMITS;
                }
                match permits.cmp(&new_permits) {
                    Ordering::Less => {
                        semaphore_clone.add_permits(new_permits - permits);
                    }
                    Ordering::Equal => continue,
                    Ordering::Greater => {
                        let to_release = permits - new_permits;
                        let reduced = semaphore_clone.forget_permits(to_release);
                        // TODO: implement dynamic semaphore with limits by ourself.
                        if reduced != to_release {
                            tracing::warn!(
                                "no enough permits to release, expected {}, but reduced {}",
                                to_release,
                                reduced
                            );
                        }
                    }
                }
                tracing::info!(
                    "max_concurrent_creating_streaming_jobs changed from {} to {}",
                    permits,
                    new_permits
                );
                permits = new_permits;
            }
        });

        Self { semaphore }
    }
}

impl DdlController {
    fn validate_specified_parallelism(
        specified_parallelism: Option<NonZeroUsize>,
        specified_backfill_parallelism: Option<NonZeroUsize>,
        max_parallelism: NonZeroUsize,
    ) -> MetaResult<()> {
        if let Some(parallelism) = specified_parallelism
            && parallelism > max_parallelism
        {
            bail_invalid_parameter!(
                "specified parallelism {} should not exceed max parallelism {}",
                parallelism,
                max_parallelism,
            );
        }
        if let Some(backfill_parallelism) = specified_backfill_parallelism
            && backfill_parallelism > max_parallelism
        {
            bail_invalid_parameter!(
                "specified backfill parallelism {} should not exceed max parallelism {}",
                backfill_parallelism,
                max_parallelism,
            );
        }
        Ok(())
    }

    fn validate_serverless_backfill_enabled(
        &self,
        resource_type: &streaming_job_resource_type::ResourceType,
    ) -> MetaResult<()> {
        if matches!(
            resource_type,
            streaming_job_resource_type::ResourceType::ServerlessBackfill(true)
        ) && self.env.opts.serverless_backfill_controller_addr.is_empty()
        {
            bail_invalid_parameter!(
                "Serverless Backfill is disabled. Use RisingWave cloud at https://cloud.risingwave.com/auth/signup to try this feature"
            );
        }

        Ok(())
    }

    pub async fn new(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        stream_manager: GlobalStreamManagerRef,
        source_manager: SourceManagerRef,
        barrier_manager: BarrierManagerRef,
        sink_manager: SinkCoordinatorManager,
        iceberg_compaction_manager: IcebergCompactionManagerRef,
        iceberg_pk_index_sink_manager: IcebergPkIndexSinkManager,
    ) -> Self {
        let creating_streaming_job_permits = Arc::new(CreatingStreamingJobPermit::new(&env).await);
        Self {
            env,
            metadata_manager,
            stream_manager,
            source_manager,
            barrier_manager,
            sink_manager,
            iceberg_compaction_manager,
            iceberg_pk_index_sink_manager,
            creating_streaming_job_permits,
            seq: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Obtains the next sequence number for DDL commands, for observability and debugging purposes.
    pub fn next_seq(&self) -> u64 {
        // This is a simple atomic increment operation.
        self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// `run_command` spawns a tokio coroutine to execute the target ddl command. When the client
    /// has been interrupted during executing, the request will be cancelled by tonic. Since we have
    /// a lot of logic for revert, status management, notification and so on, ensuring consistency
    /// would be a huge hassle and pain if we don't spawn here.
    ///
    /// Though returning `Option`, it's always `Some`, to simplify the handling logic
    pub async fn run_command(&self, command: DdlCommand) -> MetaResult<Option<WaitVersion>> {
        if !command.allow_in_recovery() {
            self.barrier_manager.check_status_running()?;
        }

        let await_tree_key = format!("DDL Command {}", self.next_seq());
        let await_tree_span = await_tree::span!("{command}({})", command.object());

        let ctrl = self.clone();
        let fut = Box::pin(async move {
            match command {
                DdlCommand::CreateDatabase(database) => ctrl.create_database(database).await,
                DdlCommand::DropDatabase(database_id) => ctrl.drop_database(database_id).await,
                DdlCommand::CreateSchema(schema) => ctrl.create_schema(schema).await,
                DdlCommand::DropSchema(schema_id, drop_mode) => {
                    ctrl.drop_schema(schema_id, drop_mode).await
                }
                DdlCommand::CreateNonSharedSource(source, iceberg_table_id) => {
                    ctrl.create_non_shared_source(source, iceberg_table_id)
                        .await
                }
                DdlCommand::DropSource(source_id, drop_mode) => {
                    ctrl.drop_source(source_id, drop_mode).await
                }
                DdlCommand::ResetSource(source_id) => ctrl.reset_source(source_id).await,
                DdlCommand::CreateFunction(function) => ctrl.create_function(function).await,
                DdlCommand::DropFunction(function_id, drop_mode) => {
                    ctrl.drop_function(function_id, drop_mode).await
                }
                DdlCommand::CreateView(view, dependencies) => {
                    ctrl.create_view(view, dependencies).await
                }
                DdlCommand::DropView(view_id, drop_mode) => {
                    ctrl.drop_view(view_id, drop_mode).await
                }
                DdlCommand::CreateStreamingJob {
                    stream_job,
                    fragment_graph,
                    dependencies,
                    resource_type,
                    if_not_exists,
                    refresh_interval_sec,
                    replace_sink,
                    since_timestamp_epoch,
                } => {
                    ctrl.create_streaming_job(
                        stream_job,
                        fragment_graph,
                        dependencies,
                        resource_type,
                        if_not_exists,
                        refresh_interval_sec,
                        replace_sink,
                        since_timestamp_epoch,
                    )
                    .await
                }
                DdlCommand::DropStreamingJob { job_id, drop_mode } => {
                    ctrl.drop_streaming_job(job_id, drop_mode).await
                }
                DdlCommand::ReplaceStreamJob(ReplaceStreamJobInfo {
                    streaming_job,
                    fragment_graph,
                }) => ctrl.replace_job(streaming_job, fragment_graph).await,
                DdlCommand::AlterName(relation, name) => ctrl.alter_name(relation, &name).await,
                DdlCommand::AlterObjectOwner(object, owner_id) => {
                    ctrl.alter_owner(object, owner_id).await
                }
                DdlCommand::ReassignOwned {
                    old_owner_ids,
                    new_owner_id,
                    database_id,
                } => {
                    ctrl.reassign_owned(old_owner_ids, new_owner_id, database_id)
                        .await
                }
                DdlCommand::DropOwned {
                    owner_ids,
                    database_id,
                    cascade,
                } => ctrl.drop_owned(owner_ids, database_id, cascade).await,
                DdlCommand::AlterSetSchema(object, new_schema_id) => {
                    ctrl.alter_set_schema(object, new_schema_id).await
                }
                DdlCommand::CreateConnection(connection) => {
                    ctrl.create_connection(connection).await
                }
                DdlCommand::DropConnection(connection_id, drop_mode) => {
                    ctrl.drop_connection(connection_id, drop_mode).await
                }
                DdlCommand::CreateSecret(secret) => ctrl.create_secret(secret).await,
                DdlCommand::DropSecret(secret_id, drop_mode) => {
                    ctrl.drop_secret(secret_id, drop_mode).await
                }
                DdlCommand::AlterSecret(secret) => ctrl.alter_secret(secret).await,
                DdlCommand::AlterNonSharedSource(source) => {
                    ctrl.alter_non_shared_source(source).await
                }
                DdlCommand::CommentOn(comment) => ctrl.comment_on(comment).await,
                DdlCommand::CreateSubscription(subscription) => {
                    ctrl.create_subscription(subscription).await
                }
                DdlCommand::DropSubscription(subscription_id, drop_mode) => {
                    ctrl.drop_subscription(subscription_id, drop_mode).await
                }
                DdlCommand::AlterSubscriptionRetention {
                    subscription_id,
                    retention_seconds,
                    definition,
                } => {
                    ctrl.alter_subscription_retention(
                        subscription_id,
                        retention_seconds,
                        definition,
                    )
                    .await
                }
                DdlCommand::AlterSwapRename(objects) => ctrl.alter_swap_rename(objects).await,
                DdlCommand::AlterDatabaseParam(database_id, param) => {
                    ctrl.alter_database_param(database_id, param).await
                }
                DdlCommand::AlterDatabaseResourceGroup(database_id, resource_group, deferred) => {
                    ctrl.alter_database_resource_group(database_id, resource_group, deferred)
                        .await
                }
                DdlCommand::AlterStreamingJobConfig(job_id, entries_to_add, keys_to_remove) => {
                    ctrl.alter_streaming_job_config(job_id, entries_to_add, keys_to_remove)
                        .await
                }
            }
        })
        .in_current_span();
        let fut = (self.env.await_tree_reg())
            .register(await_tree_key, await_tree_span)
            .instrument(Box::pin(fut));
        let notification_version = tokio::spawn(fut).await.map_err(|e| anyhow!(e))??;
        Ok(Some(WaitVersion {
            catalog_version: notification_version,
            hummock_version_id: self.barrier_manager.get_hummock_version_id().await,
        }))
    }

    pub async fn get_ddl_progress(&self) -> MetaResult<Vec<DdlProgress>> {
        self.barrier_manager.get_ddl_progress().await
    }

    async fn create_database(&self, database: Database) -> MetaResult<NotificationVersion> {
        let (version, updated_db) = self
            .metadata_manager
            .catalog_controller
            .create_database(database)
            .await?;
        // If persistent successfully, notify `GlobalBarrierManager` to create database asynchronously.
        self.barrier_manager
            .update_database_barrier(
                updated_db.database_id,
                updated_db.barrier_interval_ms.map(|v| v as u32),
                updated_db.checkpoint_frequency.map(|v| v as u64),
            )
            .await?;
        Ok(version)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn reschedule_streaming_job(
        &self,
        job_id: JobId,
        target: ReschedulePolicy,
        mut deferred: bool,
    ) -> MetaResult<()> {
        tracing::info!("altering parallelism for job {}", job_id);
        if self.barrier_manager.check_status_running().is_err() {
            tracing::info!(
                "alter parallelism is set to deferred mode because the system is in recovery state"
            );
            deferred = true;
        }

        self.stream_manager
            .reschedule_streaming_job(job_id, target, deferred)
            .await
    }

    pub async fn reschedule_streaming_job_backfill_parallelism(
        &self,
        job_id: JobId,
        parallelism: Option<ParallelismPolicy>,
        mut deferred: bool,
    ) -> MetaResult<()> {
        tracing::info!("altering backfill parallelism for job {}", job_id);
        if self.barrier_manager.check_status_running().is_err() {
            tracing::info!(
                "alter backfill parallelism is set to deferred mode because the system is in recovery state"
            );
            deferred = true;
        }

        self.stream_manager
            .reschedule_streaming_job_backfill_parallelism(job_id, parallelism, deferred)
            .await
    }

    pub async fn reschedule_cdc_table_backfill(
        &self,
        job_id: JobId,
        target: ReschedulePolicy,
    ) -> MetaResult<()> {
        tracing::info!("alter CDC table backfill parallelism");
        if self.barrier_manager.check_status_running().is_err() {
            return Err(anyhow::anyhow!("CDC table backfill reschedule is unavailable because the system is in recovery state").into());
        }
        self.stream_manager
            .reschedule_cdc_table_backfill(job_id, target)
            .await
    }

    pub async fn reschedule_fragments(
        &self,
        fragment_targets: HashMap<FragmentId, Option<StreamingParallelism>>,
    ) -> MetaResult<()> {
        tracing::info!(
            "altering parallelism for fragments {:?}",
            fragment_targets.keys()
        );
        let fragment_targets = fragment_targets
            .into_iter()
            .map(|(fragment_id, parallelism)| (fragment_id as CatalogFragmentId, parallelism))
            .collect();

        self.stream_manager
            .reschedule_fragments(fragment_targets)
            .await
    }

    async fn drop_database(&self, database_id: DatabaseId) -> MetaResult<NotificationVersion> {
        self.drop_object(ObjectType::Database, database_id, DropMode::Cascade)
            .await
    }

    async fn create_schema(&self, schema: Schema) -> MetaResult<NotificationVersion> {
        self.metadata_manager
            .catalog_controller
            .create_schema(schema)
            .await
    }

    async fn drop_schema(
        &self,
        schema_id: SchemaId,
        drop_mode: DropMode,
    ) -> MetaResult<NotificationVersion> {
        self.drop_object(ObjectType::Schema, schema_id, drop_mode)
            .await
    }

    /// Shared source is handled in [`Self::create_streaming_job`]
    async fn create_non_shared_source(
        &self,
        source: Source,
        iceberg_table_id: Option<TableId>,
    ) -> MetaResult<NotificationVersion> {
        let handle = create_source_worker(
            &source,
            self.source_manager.metrics.clone(),
            self.env.await_tree_reg().clone(),
        )
        .await
        .context("failed to create source worker")?;

        let (source_id, version) = self
            .metadata_manager
            .catalog_controller
            .create_source(source, iceberg_table_id)
            .await?;
        self.source_manager
            .register_source_with_handle(source_id, handle)
            .await;
        Ok(version)
    }

    async fn drop_source(
        &self,
        source_id: SourceId,
        drop_mode: DropMode,
    ) -> MetaResult<NotificationVersion> {
        self.drop_object(ObjectType::Source, source_id, drop_mode)
            .await
    }

    async fn reset_source(&self, source_id: SourceId) -> MetaResult<NotificationVersion> {
        tracing::info!(source_id = %source_id, "resetting CDC source offset to latest");

        // Get database_id for the source
        let database_id = self
            .metadata_manager
            .catalog_controller
            .get_object_database_id(source_id)
            .await?;

        self.stream_manager
            .barrier_scheduler
            .run_command(database_id, Command::ResetSource { source_id })
            .await?;

        // RESET SOURCE doesn't modify catalog, so return the current catalog version
        let version = self
            .metadata_manager
            .catalog_controller
            .notify_frontend_trivial()
            .await;
        Ok(version)
    }

    /// This replaces the source in the catalog.
    /// Note: `StreamSourceInfo` in downstream MVs' `SourceExecutor`s are not updated.
    async fn alter_non_shared_source(&self, source: Source) -> MetaResult<NotificationVersion> {
        self.metadata_manager
            .catalog_controller
            .alter_non_shared_source(source)
            .await
    }

    async fn create_function(&self, function: Function) -> MetaResult<NotificationVersion> {
        self.metadata_manager
            .catalog_controller
            .create_function(function)
            .await
    }

    async fn drop_function(
        &self,
        function_id: FunctionId,
        drop_mode: DropMode,
    ) -> MetaResult<NotificationVersion> {
        self.drop_object(ObjectType::Function, function_id, drop_mode)
            .await
    }

    async fn create_view(
        &self,
        view: View,
        dependencies: HashSet<ObjectId>,
    ) -> MetaResult<NotificationVersion> {
        self.metadata_manager
            .catalog_controller
            .create_view(view, dependencies)
            .await
    }

    async fn drop_view(
        &self,
        view_id: ViewId,
        drop_mode: DropMode,
    ) -> MetaResult<NotificationVersion> {
        self.drop_object(ObjectType::View, view_id, drop_mode).await
    }

    async fn create_connection(&self, connection: Connection) -> MetaResult<NotificationVersion> {
        validate_connection(&connection).await?;
        self.metadata_manager
            .catalog_controller
            .create_connection(connection)
            .await
    }

    async fn drop_connection(
        &self,
        connection_id: ConnectionId,
        drop_mode: DropMode,
    ) -> MetaResult<NotificationVersion> {
        self.drop_object(ObjectType::Connection, connection_id, drop_mode)
            .await
    }

    async fn alter_database_param(
        &self,
        database_id: DatabaseId,
        param: AlterDatabaseParam,
    ) -> MetaResult<NotificationVersion> {
        let (version, updated_db) = self
            .metadata_manager
            .catalog_controller
            .alter_database_param(database_id, param)
            .await?;
        // If persistent successfully, notify `GlobalBarrierManager` to update param asynchronously.
        self.barrier_manager
            .update_database_barrier(
                database_id,
                updated_db.barrier_interval_ms.map(|v| v as u32),
                updated_db.checkpoint_frequency.map(|v| v as u64),
            )
            .await?;
        Ok(version)
    }

    async fn alter_database_resource_group(
        &self,
        database_id: DatabaseId,
        resource_group: Option<String>,
        _deferred: bool,
    ) -> MetaResult<NotificationVersion> {
        let version = self
            .metadata_manager
            .catalog_controller
            .alter_database_resource_group(database_id, resource_group)
            .await?;

        Ok(version)
    }

    // The 'secret' part of the request we receive from the frontend is in plaintext;
    // here, we need to encrypt it before storing it in the catalog.
    fn get_encrypted_payload(&self, secret: &Secret) -> MetaResult<Vec<u8>> {
        let secret_store_private_key = self
            .env
            .opts
            .secret_store_private_key
            .clone()
            .ok_or_else(|| anyhow!("secret_store_private_key is not configured"))?;

        let encrypted_payload = SecretEncryption::encrypt(
            secret_store_private_key.as_slice(),
            secret.get_value().as_slice(),
        )
        .context(format!("failed to encrypt secret {}", secret.name))?;
        Ok(encrypted_payload
            .serialize()
            .context(format!("failed to serialize secret {}", secret.name))?)
    }

    async fn create_secret(&self, mut secret: Secret) -> MetaResult<NotificationVersion> {
        // The 'secret' part of the request we receive from the frontend is in plaintext;
        // here, we need to encrypt it before storing it in the catalog.
        let secret_plain_payload = secret.value.clone();
        let encrypted_payload = self.get_encrypted_payload(&secret)?;
        secret.value = encrypted_payload;

        self.metadata_manager
            .catalog_controller
            .create_secret(secret, secret_plain_payload)
            .await
    }

    async fn drop_secret(
        &self,
        secret_id: SecretId,
        drop_mode: DropMode,
    ) -> MetaResult<NotificationVersion> {
        self.drop_object(ObjectType::Secret, secret_id, drop_mode)
            .await
    }

    async fn alter_secret(&self, mut secret: Secret) -> MetaResult<NotificationVersion> {
        let secret_plain_payload = secret.value.clone();
        let encrypted_payload = self.get_encrypted_payload(&secret)?;
        secret.value = encrypted_payload;
        self.metadata_manager
            .catalog_controller
            .alter_secret(secret, secret_plain_payload)
            .await
    }

    async fn create_subscription(
        &self,
        mut subscription: Subscription,
    ) -> MetaResult<NotificationVersion> {
        tracing::debug!("create subscription");
        let _permit = self
            .creating_streaming_job_permits
            .semaphore
            .acquire()
            .await
            .unwrap();
        let _reschedule_job_lock = self.stream_manager.reschedule_lock_read_guard().await;
        self.metadata_manager
            .catalog_controller
            .create_subscription_catalog(&mut subscription)
            .await?;
        if let Err(err) = self.stream_manager.create_subscription(&subscription).await {
            tracing::debug!(error = %err.as_report(), "failed to create subscription");
            let _ = self
                .metadata_manager
                .catalog_controller
                .try_abort_creating_subscription(subscription.id)
                .await
                .inspect_err(|e| {
                    tracing::error!(
                        error = %e.as_report(),
                        "failed to abort create subscription after failure"
                    );
                });
            return Err(err);
        }

        let version = self
            .metadata_manager
            .catalog_controller
            .notify_create_subscription(subscription.id)
            .await?;
        tracing::debug!("finish create subscription");
        Ok(version)
    }

    async fn drop_subscription(
        &self,
        subscription_id: SubscriptionId,
        drop_mode: DropMode,
    ) -> MetaResult<NotificationVersion> {
        tracing::debug!("preparing drop subscription");
        let _reschedule_job_lock = self.stream_manager.reschedule_lock_read_guard().await;
        let subscription = self
            .metadata_manager
            .catalog_controller
            .get_subscription_by_id(subscription_id)
            .await?;
        let table_id = subscription.dependent_table_id;
        let database_id = subscription.database_id;
        let (_, version) = self
            .metadata_manager
            .catalog_controller
            .drop_object(ObjectType::Subscription, subscription_id, drop_mode)
            .await?;
        self.stream_manager
            .drop_subscription(database_id, subscription_id, table_id)
            .await;
        tracing::debug!("finish drop subscription");
        Ok(version)
    }

    async fn alter_subscription_retention(
        &self,
        subscription_id: SubscriptionId,
        retention_seconds: u64,
        definition: String,
    ) -> MetaResult<NotificationVersion> {
        tracing::debug!("alter subscription retention");
        let _reschedule_job_lock = self.stream_manager.reschedule_lock_read_guard().await;
        let (version, subscription) = self
            .metadata_manager
            .catalog_controller
            .alter_subscription_retention(subscription_id, retention_seconds, definition)
            .await?;
        self.stream_manager
            .alter_subscription_retention(
                subscription.database_id,
                subscription.id,
                subscription.dependent_table_id,
                subscription.retention_seconds,
            )
            .await?;
        tracing::debug!("finish alter subscription retention");
        Ok(version)
    }

    /// Validates the connect properties in the `cdc_table_desc` stored in the `StreamCdcScan` node
    #[await_tree::instrument]
    pub(crate) async fn validate_cdc_table(
        &self,
        table: &Table,
        table_fragments: &StreamJobFragments,
    ) -> MetaResult<()> {
        let stream_scan_fragment =
            Itertools::exactly_one(table_fragments.fragments.values().filter(|f| {
                f.fragment_type_mask.contains(FragmentTypeFlag::StreamScan)
                    || f.fragment_type_mask
                        .contains(FragmentTypeFlag::StreamCdcScan)
            }))
            .ok()
            .with_context(|| {
                format!(
                    "expect exactly one stream scan fragment, got: {:?}",
                    table_fragments.fragments
                )
            })?;
        fn assert_parallelism(
            distribution_type: PbFragmentDistributionType,
            node_body: &Option<NodeBody>,
        ) {
            if let Some(NodeBody::StreamCdcScan(node)) = node_body {
                if let Some(o) = node.options
                    && CdcScanOptions::from_proto(&o).is_parallelized_backfill()
                {
                    // Use parallel CDC backfill.
                } else {
                    assert_eq!(
                        distribution_type,
                        PbFragmentDistributionType::Single,
                        "Non-parallelized CDC scan fragment should have Single distribution"
                    );
                }
            }
        }
        let mut found_cdc_scan = false;
        match &stream_scan_fragment.nodes.node_body {
            Some(NodeBody::StreamCdcScan(_)) => {
                assert_parallelism(
                    stream_scan_fragment.distribution_type,
                    &stream_scan_fragment.nodes.node_body,
                );
                if self
                    .validate_cdc_table_inner(&stream_scan_fragment.nodes.node_body, table.id)
                    .await?
                {
                    found_cdc_scan = true;
                }
            }
            // When there's generated columns, the cdc scan node is wrapped in a project node
            Some(NodeBody::Project(_)) => {
                for input in &stream_scan_fragment.nodes.input {
                    assert_parallelism(stream_scan_fragment.distribution_type, &input.node_body);
                    if self
                        .validate_cdc_table_inner(&input.node_body, table.id)
                        .await?
                    {
                        found_cdc_scan = true;
                    }
                }
            }
            _ => {
                bail!("Unexpected node body for stream cdc scan");
            }
        };
        if !found_cdc_scan {
            bail!("No stream cdc scan node found in stream scan fragment");
        }
        Ok(())
    }

    async fn validate_cdc_table_inner(
        &self,
        node_body: &Option<NodeBody>,
        table_id: TableId,
    ) -> MetaResult<bool> {
        if let Some(NodeBody::StreamCdcScan(stream_cdc_scan)) = node_body
            && let Some(ref cdc_table_desc) = stream_cdc_scan.cdc_table_desc
        {
            let options_with_secret = WithOptionsSecResolved::new(
                cdc_table_desc.connect_properties.clone(),
                cdc_table_desc.secret_refs.clone(),
            );

            let mut props = ConnectorProperties::extract(options_with_secret, true)?;
            props.init_from_pb_cdc_table_desc(cdc_table_desc);

            // Try creating a split enumerator to validate
            let _enumerator = props
                .create_split_enumerator(SourceEnumeratorContext::dummy().into())
                .await?;

            tracing::debug!(?table_id, "validate cdc table success");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn validate_table_for_sink(&self, table_id: TableId) -> MetaResult<()> {
        let migrated = self
            .metadata_manager
            .catalog_controller
            .has_table_been_migrated(table_id)
            .await?;
        if !migrated {
            Err(anyhow::anyhow!("Creating sink into table is not allowed for unmigrated table {}. Please migrate it first.", table_id).into())
        } else {
            Ok(())
        }
    }

    /// For [`CreateType::Foreground`], the function will only return after backfilling finishes
    /// ([`crate::manager::MetadataManager::wait_streaming_job_finished`]).
    #[await_tree::instrument(boxed, "create_streaming_job({streaming_job})")]
    pub async fn create_streaming_job(
        &self,
        mut streaming_job: StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
        dependencies: HashSet<ObjectId>,
        resource_type: streaming_job_resource_type::ResourceType,
        if_not_exists: bool,
        refresh_interval_sec: Option<u64>,
        replace_sink: Option<SinkId>,
        since_timestamp_epoch: Option<u64>,
    ) -> MetaResult<NotificationVersion> {
        let replace_sink_info = if let Some(old_sink_id) = replace_sink {
            let StreamingJob::Sink(sink, _) = &streaming_job else {
                bail!("replace sink requires a sink job")
            };
            if sink.target_table.is_some() {
                bail_not_implemented!("replace sink into table")
            }

            Some(old_sink_id)
        } else {
            if let StreamingJob::Sink(sink, _) = &streaming_job
                && let Some(target_table) = sink.target_table
            {
                self.validate_table_for_sink(target_table).await?;
            }
            None
        };
        self.validate_serverless_backfill_enabled(&resource_type)?;
        let ctx = StreamContext::from_protobuf(fragment_graph.get_ctx().unwrap());
        let adaptive_parallelism_strategy =
            (!fragment_graph.adaptive_parallelism_strategy.is_empty()).then(|| {
                parse_strategy(&fragment_graph.adaptive_parallelism_strategy)
                    .expect("adaptive parallelism strategy should be validated in frontend")
            });
        let backfill_adaptive_parallelism_strategy = (!fragment_graph
            .backfill_adaptive_parallelism_strategy
            .is_empty())
        .then(|| {
            parse_strategy(&fragment_graph.backfill_adaptive_parallelism_strategy)
                .expect("backfill adaptive parallelism strategy should be validated in frontend")
        });

        let streaming_job_model = match self
            .metadata_manager
            .catalog_controller
            .create_job_catalog(
                &mut streaming_job,
                &ctx,
                &fragment_graph.parallelism,
                fragment_graph.max_parallelism as _,
                dependencies,
                resource_type.clone(),
                &fragment_graph.backfill_parallelism,
                adaptive_parallelism_strategy,
                backfill_adaptive_parallelism_strategy,
                replace_sink_info.as_ref(),
                refresh_interval_sec,
            )
            .await
        {
            Ok(model) => model,
            Err(meta_err) => {
                if !if_not_exists {
                    return Err(meta_err);
                }
                return if let MetaErrorInner::Duplicated(_, _, Some(job_id)) = meta_err.inner() {
                    if streaming_job.create_type() == CreateType::Foreground {
                        let database_id = streaming_job.database_id();
                        self.metadata_manager
                            .wait_streaming_job_finished(database_id, *job_id)
                            .await
                    } else {
                        Ok(IGNORED_NOTIFICATION_VERSION)
                    }
                } else {
                    Err(meta_err)
                };
            }
        };
        let job_id = streaming_job.id();
        if let Some(old_sink_id) = replace_sink_info.as_ref() {
            tracing::debug!(
                old_sink_id = %old_sink_id,
                new_sink_id = %job_id,
                definition = streaming_job.definition(),
                create_type = streaming_job.create_type().as_str_name(),
                "starting replacement sink",
            );
        } else {
            tracing::debug!(
                id = %job_id,
                definition = streaming_job.definition(),
                create_type = streaming_job.create_type().as_str_name(),
                job_type = ?streaming_job.job_type(),
                "starting streaming job",
            );
        }
        // TODO: acquire permits for recovered background DDLs.
        let permit = self
            .creating_streaming_job_permits
            .semaphore
            .clone()
            .acquire_owned()
            .instrument_await("acquire_creating_streaming_job_permit")
            .await
            .unwrap();
        let reschedule_job_lock = self.stream_manager.reschedule_lock_read_guard().await;

        let name = streaming_job.name();
        let definition = streaming_job.definition();
        let database_id = streaming_job.database_id();
        let source_id = match &streaming_job {
            StreamingJob::Table(Some(src), _, _) | StreamingJob::Source(src) => Some(src.id),
            _ => None,
        };
        let create_result = match self
            .generate_streaming_job(
                ctx,
                streaming_job,
                fragment_graph,
                resource_type.clone(),
                streaming_job_model,
                replace_sink_info,
                since_timestamp_epoch,
            )
            .await
        {
            Ok((stream_job_fragments, ctx)) => {
                self.stream_manager
                    .create_streaming_job(stream_job_fragments, ctx, permit, reschedule_job_lock)
                    .await
            }
            Err(err) => Err((err, false, None)),
        };

        match create_result {
            Ok(version) => Ok(version),
            Err((err, is_cancelled, cancel_notifier)) => {
                tracing::error!(id = %job_id, error = %err.as_report(), "failed to create streaming job");
                let event = risingwave_pb::meta::event_log::EventCreateStreamJobFail {
                    id: job_id,
                    name,
                    definition,
                    error: err.as_report().to_string(),
                };
                self.env.event_log_manager_ref().add_event_logs(vec![
                    risingwave_pb::meta::event_log::Event::CreateStreamJobFail(event),
                ]);
                let abort_result = self
                    .metadata_manager
                    .catalog_controller
                    .try_abort_creating_streaming_job(job_id, is_cancelled)
                    .await?;
                self.iceberg_compaction_manager
                    .clear_maintenance_for_aborted_job(&abort_result);
                if let Some(cancel_info) = abort_result.cancel_info {
                    self.stream_manager
                        .barrier_scheduler
                        .run_command(database_id, cancel_info.command)
                        .await?;
                    cleanup_dropped_streaming_jobs(
                        &self.stream_manager.refresh_manager,
                        &self.stream_manager.hummock_manager,
                        &self.stream_manager.metadata_manager,
                        cancel_info.streaming_job_ids,
                        cancel_info.state_table_ids,
                        "cancel_streaming_job",
                    )
                    .await?;
                }
                if let Some(cancel_notifier) = cancel_notifier {
                    let _ = cancel_notifier.send(true).inspect_err(|err| {
                        tracing::warn!("failed to notify cancellation result: {err}")
                    });
                }
                if abort_result.aborted {
                    tracing::warn!(id = %job_id, is_cancelled, "aborted streaming job");
                    // FIXME: might also need other cleanup here
                    if let Some(source_id) = source_id {
                        self.source_manager
                            .apply_source_change(SourceChange::DropSource {
                                dropped_source_ids: vec![source_id],
                            })
                            .await;
                    }
                }
                Err(err)
            }
        }
    }

    #[await_tree::instrument(boxed)]
    async fn generate_streaming_job(
        &self,
        ctx: StreamContext,
        mut streaming_job: StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
        resource_type: streaming_job_resource_type::ResourceType,
        streaming_job_model: streaming_job::Model,
        replace_sink: Option<SinkId>,
        since_timestamp_epoch: Option<u64>,
    ) -> MetaResult<(StreamJobFragmentsToCreate, CreateStreamingJobContext)> {
        let mut fragment_graph =
            StreamFragmentGraph::new(&self.env, fragment_graph, &streaming_job)?;
        streaming_job.set_info_from_graph(&fragment_graph);

        // create internal table catalogs and refill table id.
        let incomplete_internal_tables = fragment_graph
            .incomplete_internal_tables()
            .into_values()
            .collect_vec();
        let table_id_map = self
            .metadata_manager
            .catalog_controller
            .create_internal_table_catalog(&streaming_job, incomplete_internal_tables)
            .await?;
        fragment_graph.refill_internal_table_ids(table_id_map);

        // create fragment and actor catalogs.
        tracing::debug!(id = %streaming_job.id(), "building streaming job");
        let (mut ctx, stream_job_fragments) = self
            .build_stream_job(
                ctx,
                streaming_job,
                fragment_graph,
                resource_type,
                streaming_job_model,
                since_timestamp_epoch,
            )
            .await?;
        ctx.replace_sink = replace_sink;

        let streaming_job = &ctx.streaming_job;

        match streaming_job {
            StreamingJob::Table(None, table, TableJobType::SharedCdcSource) => {
                self.validate_cdc_table(table, &stream_job_fragments)
                    .await?;
            }
            StreamingJob::Table(Some(source), ..) => {
                // Register the source on the connector node.
                self.source_manager.register_source(source).await?;
                let connector_name = source
                    .get_with_properties()
                    .get(UPSTREAM_SOURCE_KEY)
                    .cloned();
                let attr = source.info.as_ref().map(|source_info| {
                    jsonbb::json!({
                            "format": source_info.format().as_str_name(),
                            "encode": source_info.row_encode().as_str_name(),
                    })
                });
                report_create_object(
                    streaming_job.id(),
                    "source",
                    PbTelemetryDatabaseObject::Source,
                    connector_name,
                    attr,
                );
            }
            StreamingJob::Sink(sink, _) => {
                if sink.auto_refresh_schema_from_table.is_some() {
                    check_sink_fragments_support_refresh_schema(&stream_job_fragments.fragments)?;
                }
                // Validate the sink on the connector node.
                validate_sink(sink).await?;
                // For Iceberg pk-index sinks, spawn the per-sink commit worker now
                // so it's ready to receive epoch reports from the very first
                // barrier instead of relying on lazy registration on every
                // commit.
                if crate::manager::iceberg_pk_index_sink::is_iceberg_pk_index_sink(&sink.properties)
                {
                    let iceberg_config =
                        crate::manager::iceberg_pk_index_sink::build_iceberg_config(sink)?;
                    self.iceberg_pk_index_sink_manager
                        .register_sink(
                            sink.id,
                            crate::barrier::to_partial_graph_id(sink.database_id, None),
                            iceberg_config,
                        )
                        .await
                        .map_err(|e| anyhow!(e).context("register v3 sink worker"))?;
                }
                let connector_name = sink.get_properties().get(UPSTREAM_SOURCE_KEY).cloned();
                let attr = sink.format_desc.as_ref().map(|sink_info| {
                    jsonbb::json!({
                        "format": sink_info.format().as_str_name(),
                        "encode": sink_info.encode().as_str_name(),
                    })
                });
                report_create_object(
                    streaming_job.id(),
                    "sink",
                    PbTelemetryDatabaseObject::Sink,
                    connector_name,
                    attr,
                );
            }
            StreamingJob::Source(source) => {
                // Register the source on the connector node.
                self.source_manager.register_source(source).await?;
                let connector_name = source
                    .get_with_properties()
                    .get(UPSTREAM_SOURCE_KEY)
                    .cloned();
                let attr = source.info.as_ref().map(|source_info| {
                    jsonbb::json!({
                            "format": source_info.format().as_str_name(),
                            "encode": source_info.row_encode().as_str_name(),
                    })
                });
                report_create_object(
                    streaming_job.id(),
                    "source",
                    PbTelemetryDatabaseObject::Source,
                    connector_name,
                    attr,
                );
            }
            _ => {}
        }

        let backfill_orders = ctx.fragment_backfill_ordering.to_meta_model();
        self.metadata_manager
            .catalog_controller
            .prepare_stream_job_fragments(
                &stream_job_fragments,
                streaming_job,
                false,
                Some(backfill_orders),
            )
            .await?;

        Ok((stream_job_fragments, ctx))
    }

    /// `target_replace_info`: when dropping a sink into table, we need to replace the table.
    pub async fn drop_object(
        &self,
        object_type: ObjectType,
        object_id: impl Into<ObjectId>,
        drop_mode: DropMode,
    ) -> MetaResult<NotificationVersion> {
        let object_id = object_id.into();
        // Fence reschedule and source tick before catalog deletion so post-collect split updates
        // cannot race with dropped fragments.
        let _reschedule_job_lock = self.stream_manager.reschedule_lock_read_guard().await;
        let _source_tick_pause_guard = self.source_manager.pause_tick().await;

        let (release_ctx, version) = self
            .metadata_manager
            .catalog_controller
            .drop_object(object_type, object_id, drop_mode)
            .await?;

        if object_type == ObjectType::Source {
            self.env
                .notification_manager_ref()
                .notify_local_subscribers(LocalNotification::SourceDropped(object_id));
        }

        let ReleaseContext {
            database_id,
            removed_streaming_job_ids,
            removed_state_table_ids,
            removed_source_ids,
            removed_secret_ids: secret_ids,
            removed_source_fragments,
            removed_fragments,
            removed_sink_fragment_by_targets,
            removed_iceberg_table_sinks,
            removed_iceberg_sink_ids,
            removed_iceberg_pk_index_sink_ids,
        } = release_ctx;

        // Notify serving module about deleted fragments so it can clean up serving vnode mappings.
        // This is driven by the fragment model deletion (cascade from Object::delete_many),
        // decoupled from the barrier-driven streaming mapping notifications.
        self.env
            .notification_manager_ref()
            .notify_serving_fragment_mapping_delete(
                removed_fragments.iter().map(|id| *id as _).collect(),
            );

        self.stream_manager
            .drop_streaming_jobs(
                database_id,
                removed_streaming_job_ids,
                removed_state_table_ids,
                removed_sink_fragment_by_targets
                    .into_iter()
                    .map(|(target, sinks)| {
                        (target as _, sinks.into_iter().map(|id| id as _).collect())
                    })
                    .collect(),
            )
            .await;

        // clean up sources after dropping streaming jobs.
        // Otherwise, e.g., Kafka consumer groups might be recreated after deleted.
        self.source_manager
            .apply_source_change(SourceChange::DropSource {
                dropped_source_ids: removed_source_ids.into_iter().map(|id| id as _).collect(),
            })
            .await;

        // unregister fragments and actors from source manager.
        // FIXME: need also unregister source backfill fragments.
        let dropped_source_fragments = removed_source_fragments;
        self.source_manager
            .apply_source_change(SourceChange::DropMv {
                dropped_source_fragments,
            })
            .await;

        // clean up iceberg table sinks
        let iceberg_sink_ids: Vec<SinkId> = removed_iceberg_table_sinks
            .iter()
            .map(|sink| sink.id)
            .collect();

        for sink in removed_iceberg_table_sinks {
            let sink_param = SinkParam::try_from_sink_catalog(sink.into())
                .expect("Iceberg sink should be valid");
            let iceberg_sink =
                IcebergSink::try_from(sink_param).expect("Iceberg sink should be valid");
            if let Ok(iceberg_catalog) = iceberg_sink.config.create_catalog().await {
                let table_identifier = iceberg_sink.config.full_table_name().unwrap();
                tracing::info!(
                    "dropping iceberg table {} for dropped sink",
                    table_identifier
                );

                let _ = iceberg_catalog
                    .drop_table(&table_identifier)
                    .await
                    .inspect_err(|err| {
                        tracing::error!(
                            "failed to drop iceberg table {} during cleanup: {}",
                            table_identifier,
                            err.as_report()
                        );
                    });
            }
        }

        // stop sink coordinators for iceberg table sinks
        if !iceberg_sink_ids.is_empty() {
            self.sink_manager
                .stop_sink_coordinator(iceberg_sink_ids)
                .await;
        }

        // Covers user-created iceberg sinks dropped via CASCADE, which are not in
        // `removed_iceberg_table_sinks` above.
        for sink_id in removed_iceberg_sink_ids {
            self.iceberg_compaction_manager
                .clear_iceberg_maintenance_by_sink_id(sink_id);
        }

        // Unregister per-sink commit coordinators for any dropped pk-index iceberg sink,
        // including user-created sinks with arbitrary names (not just the
        // `__iceberg_sink_%` auto-created ones above).
        if !removed_iceberg_pk_index_sink_ids.is_empty() {
            self.iceberg_pk_index_sink_manager
                .unregister_sinks(removed_iceberg_pk_index_sink_ids);
        }

        // remove secrets.
        for secret in secret_ids {
            LocalSecretManager::global().remove_secret(secret);
        }
        Ok(version)
    }

    async fn reassign_owned(
        &self,
        old_owner_ids: Vec<UserId>,
        new_owner_id: UserId,
        database_id: DatabaseId,
    ) -> MetaResult<NotificationVersion> {
        self.metadata_manager
            .catalog_controller
            .reassign_owned(old_owner_ids, new_owner_id, database_id)
            .await
    }

    async fn drop_owned(
        &self,
        owner_ids: Vec<UserId>,
        database_id: DatabaseId,
        cascade: bool,
    ) -> MetaResult<NotificationVersion> {
        self.metadata_manager
            .catalog_controller
            .drop_owned(owner_ids, database_id, cascade)
            .await
    }

    /// This is used for `ALTER TABLE ADD/DROP COLUMN` / `ALTER SOURCE ADD COLUMN`.
    #[await_tree::instrument(boxed, "replace_streaming_job({streaming_job})")]
    pub async fn replace_job(
        &self,
        mut streaming_job: StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
    ) -> MetaResult<NotificationVersion> {
        match &streaming_job {
            StreamingJob::Table(..)
            | StreamingJob::Source(..)
            | StreamingJob::MaterializedView(..) => {}
            StreamingJob::Sink(..) | StreamingJob::Index(..) => {
                bail_not_implemented!("schema change for {}", streaming_job.job_type_str())
            }
        }

        let job_id = streaming_job.id();

        let _reschedule_job_lock = self.stream_manager.reschedule_lock_read_guard().await;
        let ctx = StreamContext::from_protobuf(fragment_graph.get_ctx().unwrap());

        // Ensure the max parallelism unchanged before replacing table.
        let original_max_parallelism = self
            .metadata_manager
            .get_job_max_parallelism(streaming_job.id())
            .await?;
        let fragment_graph = PbStreamFragmentGraph {
            max_parallelism: original_max_parallelism as _,
            ..fragment_graph
        };

        // 1. build fragment graph.
        let fragment_graph = StreamFragmentGraph::new(&self.env, fragment_graph, &streaming_job)?;
        streaming_job.set_info_from_graph(&fragment_graph);

        // make it immutable
        let streaming_job = streaming_job;

        let auto_refresh_schema_sinks = if let StreamingJob::Table(_, table, _) = &streaming_job {
            let auto_refresh_schema_sinks = self
                .metadata_manager
                .catalog_controller
                .get_sink_auto_refresh_schema_from(table.id)
                .await?;
            if !auto_refresh_schema_sinks.is_empty() {
                let original_table_columns = self
                    .metadata_manager
                    .catalog_controller
                    .get_table_columns(table.id)
                    .await?;
                // compare column id to find newly added and removed columns
                let original_table_column_ids: HashSet<_> = original_table_columns
                    .iter()
                    .map(|col| col.column_id())
                    .collect();
                let new_table_column_ids: HashSet<_> = table
                    .columns
                    .iter()
                    .map(|col| ColumnId::new(col.column_desc.as_ref().unwrap().column_id as _))
                    .collect();
                let newly_added_columns = table
                    .columns
                    .iter()
                    .filter(|col| {
                        !original_table_column_ids.contains(&ColumnId::new(
                            col.column_desc.as_ref().unwrap().column_id as _,
                        ))
                    })
                    .map(|col| ColumnCatalog::from(col.clone()))
                    .collect_vec();
                let removed_columns = original_table_columns
                    .iter()
                    .filter(|col| !new_table_column_ids.contains(&col.column_id()))
                    .cloned()
                    .collect_vec();
                // Fail before any fragment rewrite or catalog persistence so a rejected ALTER
                // leaves no partial state.
                if let Some(variant_column) = first_variant_column(&newly_added_columns) {
                    let sink_names = auto_refresh_schema_sinks
                        .iter()
                        .map(|sink| format!("`{}`", sink.name))
                        .join(", ");
                    return Err(MetaError::invalid_parameter(format!(
                        "cannot add VARIANT column `{}` because sink(s) {} with auto schema refresh do not support VARIANT",
                        variant_column.name_with_hidden(),
                        sink_names,
                    )));
                }
                let mut sinks = Vec::with_capacity(auto_refresh_schema_sinks.len());
                for sink in auto_refresh_schema_sinks {
                    let sink_job_fragments = self
                        .metadata_manager
                        .get_job_fragments_by_id(sink.id.as_job_id())
                        .await?;
                    if sink_job_fragments.fragments.len() != 1 {
                        return Err(anyhow!(
                            "auto schema refresh sink must have only one fragment, but got {}",
                            sink_job_fragments.fragments.len()
                        )
                        .into());
                    }
                    let sink_ctx = sink_job_fragments.ctx;
                    let original_sink_fragment =
                        sink_job_fragments.fragments.into_values().next().unwrap();
                    let (new_sink_fragment, new_schema, new_log_store_table) =
                        rewrite_refresh_schema_sink_fragment(
                            &original_sink_fragment,
                            &sink,
                            &newly_added_columns,
                            &removed_columns,
                            table,
                            fragment_graph.table_fragment_id(),
                            self.env.id_gen_manager(),
                        )?;

                    let streaming_job = StreamingJob::Sink(sink, None);

                    let tmp_sink_model = self
                        .metadata_manager
                        .catalog_controller
                        .create_job_catalog_for_replace(&streaming_job, None, None, None)
                        .await?;
                    let tmp_sink_id = tmp_sink_model.job_id.as_sink_id();
                    let StreamingJob::Sink(sink, _) = streaming_job else {
                        unreachable!()
                    };

                    sinks.push(AutoRefreshSchemaSinkContext {
                        tmp_sink_id,
                        original_sink: sink,
                        original_fragment: original_sink_fragment,
                        new_schema,
                        newly_add_fields: newly_added_columns
                            .iter()
                            .map(|col| Field::from(&col.column_desc))
                            .collect(),
                        removed_column_names: removed_columns
                            .iter()
                            .map(|col| col.name.clone())
                            .collect(),
                        new_fragment: new_sink_fragment,
                        new_log_store_table: new_log_store_table.map(Box::new),
                        ctx: sink_ctx,
                    });
                }
                Some(sinks)
            } else {
                None
            }
        } else {
            None
        };

        let streaming_job_model = self
            .metadata_manager
            .catalog_controller
            .create_job_catalog_for_replace(
                &streaming_job,
                Some(&ctx),
                fragment_graph.specified_parallelism().as_ref(),
                Some(fragment_graph.max_parallelism()),
            )
            .await?;
        let tmp_id = streaming_job_model.job_id;

        let tmp_sink_ids = auto_refresh_schema_sinks.as_ref().map(|sinks| {
            sinks
                .iter()
                .map(|sink| sink.tmp_sink_id.as_object_id())
                .collect_vec()
        });

        tracing::debug!(id = %job_id, "building replace streaming job");
        let mut updated_sink_catalogs = vec![];

        let mut drop_table_connector_ctx = None;
        let result: MetaResult<_> = try {
            let (mut ctx, mut stream_job_fragments) = self
                .build_replace_job(
                    ctx,
                    &streaming_job,
                    fragment_graph,
                    tmp_id,
                    auto_refresh_schema_sinks,
                    streaming_job_model,
                )
                .await?;
            drop_table_connector_ctx = ctx.drop_table_connector_ctx.clone();
            let auto_refresh_schema_sink_finish_ctx =
                ctx.auto_refresh_schema_sinks.as_ref().map(|sinks| {
                    sinks
                        .iter()
                        .map(|sink| FinishAutoRefreshSchemaSinkContext {
                            tmp_sink_id: sink.tmp_sink_id,
                            original_sink_id: sink.original_sink.id,
                            columns: sink.new_schema.clone(),
                            new_log_store_table: sink.new_log_store_table.clone(),
                        })
                        .collect()
                });

            // Handle table that has incoming sinks.
            if let StreamingJob::Table(_, table, ..) = &streaming_job {
                let union_fragment = stream_job_fragments.inner.union_fragment_for_table();
                let upstream_infos = self
                    .metadata_manager
                    .catalog_controller
                    .get_all_upstream_sink_infos(table, union_fragment.fragment_id as _)
                    .await?;
                refill_upstream_sink_union_in_table(&mut union_fragment.nodes, &upstream_infos);

                for upstream_info in &upstream_infos {
                    let upstream_fragment_id = upstream_info.sink_fragment_id;
                    ctx.upstream_fragment_downstreams
                        .entry(upstream_fragment_id)
                        .or_default()
                        .push(upstream_info.new_sink_downstream.clone());
                    if upstream_info.sink_original_target_columns.is_empty() {
                        updated_sink_catalogs.push(upstream_info.sink_id);
                    }
                }
            }

            let replace_upstream = ctx.replace_upstream.clone();

            if let Some(sinks) = &ctx.auto_refresh_schema_sinks {
                let empty_downstreams = FragmentDownstreamRelation::default();
                for sink in sinks {
                    self.metadata_manager
                        .catalog_controller
                        .prepare_streaming_job(
                            sink.tmp_sink_id.as_job_id(),
                            || [&sink.new_fragment].into_iter(),
                            &empty_downstreams,
                            true,
                            None,
                            None,
                        )
                        .await?;
                }
            }

            self.metadata_manager
                .catalog_controller
                .prepare_stream_job_fragments(&stream_job_fragments, &streaming_job, true, None)
                .await?;

            self.stream_manager
                .replace_stream_job(stream_job_fragments, ctx)
                .await?;
            (replace_upstream, auto_refresh_schema_sink_finish_ctx)
        };

        match result {
            Ok((replace_upstream, auto_refresh_schema_sink_finish_ctx)) => {
                let version = self
                    .metadata_manager
                    .catalog_controller
                    .finish_replace_streaming_job(
                        tmp_id,
                        streaming_job,
                        replace_upstream,
                        SinkIntoTableContext {
                            updated_sink_catalogs,
                        },
                        drop_table_connector_ctx.as_ref(),
                        auto_refresh_schema_sink_finish_ctx,
                    )
                    .await?;
                if let Some(drop_table_connector_ctx) = &drop_table_connector_ctx {
                    self.source_manager
                        .apply_source_change(SourceChange::DropSource {
                            dropped_source_ids: vec![drop_table_connector_ctx.to_remove_source_id],
                        })
                        .await;
                }
                Ok(version)
            }
            Err(err) => {
                tracing::error!(id = %job_id, error = ?err.as_report(), "failed to replace job");
                let _ = self.metadata_manager
                    .catalog_controller
                    .try_abort_replacing_streaming_job(tmp_id, tmp_sink_ids)
                    .await.inspect_err(|err| {
                    tracing::error!(id = %job_id, error = ?err.as_report(), "failed to abort replacing job");
                });
                Err(err)
            }
        }
    }

    #[await_tree::instrument(boxed, "drop_streaming_job{}({job_id})", if let DropMode::Cascade = drop_mode { "_cascade" } else { "" }
    )]
    async fn drop_streaming_job(
        &self,
        job_id: StreamingJobId,
        drop_mode: DropMode,
    ) -> MetaResult<NotificationVersion> {
        let (object_id, object_type) = match job_id {
            StreamingJobId::MaterializedView(id) => (id.as_object_id(), ObjectType::Table),
            StreamingJobId::Sink(id) => (id.as_object_id(), ObjectType::Sink),
            StreamingJobId::Table(_, id) => (id.as_object_id(), ObjectType::Table),
            StreamingJobId::Index(idx) => (idx.as_object_id(), ObjectType::Index),
        };

        let job_status = self
            .metadata_manager
            .catalog_controller
            .get_streaming_job_status(job_id.id())
            .await?;
        let version = match job_status {
            JobStatus::Initial => {
                let abort_result = self
                    .metadata_manager
                    .catalog_controller
                    .try_abort_creating_streaming_job(job_id.id(), true)
                    .await?;
                self.iceberg_compaction_manager
                    .clear_maintenance_for_aborted_job(&abort_result);
                IGNORED_NOTIFICATION_VERSION
            }
            JobStatus::Creating => {
                self.stream_manager
                    .cancel_streaming_jobs(vec![job_id.id()])
                    .await?;
                IGNORED_NOTIFICATION_VERSION
            }
            JobStatus::Created => self.drop_object(object_type, object_id, drop_mode).await?,
        };

        Ok(version)
    }

    /// Builds the actor graph:
    /// - Add the upstream fragments to the fragment graph
    /// - Schedule the fragments based on their distribution
    /// - Expand each fragment into one or several actors
    /// - Construct the fragment level backfill order control.
    #[await_tree::instrument]
    pub(crate) async fn build_stream_job(
        &self,
        stream_ctx: StreamContext,
        mut stream_job: StreamingJob,
        fragment_graph: StreamFragmentGraph,
        resource_type: streaming_job_resource_type::ResourceType,
        streaming_job_model: streaming_job::Model,
        since_timestamp_epoch: Option<u64>,
    ) -> MetaResult<(CreateStreamingJobContext, StreamJobFragmentsToCreate)> {
        let id = stream_job.id();
        let max_parallelism = NonZeroUsize::new(fragment_graph.max_parallelism()).unwrap();
        Self::validate_specified_parallelism(
            fragment_graph.specified_parallelism(),
            fragment_graph.specified_backfill_parallelism(),
            max_parallelism,
        )?;

        // 1. Fragment Level ordering graph
        let fragment_backfill_ordering = fragment_graph.create_fragment_backfill_ordering();

        // 2. Resolve the upstream fragments, extend the fragment graph to a complete graph that
        // contains all information needed for building the actor graph.

        let (snapshot_backfill_info, cross_db_snapshot_backfill_info) =
            fragment_graph.collect_snapshot_backfill_info()?;
        assert!(
            snapshot_backfill_info
                .iter()
                .chain([&cross_db_snapshot_backfill_info])
                .flat_map(|info| info.upstream_mv_table_id_to_backfill_epoch.values())
                .all(|backfill_epoch| backfill_epoch.is_none()),
            "should not set backfill epoch when initially build the job: {:?} {:?}",
            snapshot_backfill_info,
            cross_db_snapshot_backfill_info
        );

        let locality_fragment_state_table_mapping =
            fragment_graph.find_locality_provider_fragment_state_table_mapping();

        // check if log store exists for all cross-db upstreams
        self.metadata_manager
            .catalog_controller
            .validate_cross_db_snapshot_backfill(&cross_db_snapshot_backfill_info)
            .await?;

        let upstream_table_ids = fragment_graph
            .dependent_table_ids()
            .iter()
            .filter(|id| {
                !cross_db_snapshot_backfill_info
                    .upstream_mv_table_id_to_backfill_epoch
                    .contains_key(*id)
            })
            .cloned()
            .collect();

        let upstream_root_fragments = self
            .metadata_manager
            .get_upstream_root_fragments(&upstream_table_ids)
            .await?;

        if snapshot_backfill_info.is_some() {
            match stream_job {
                StreamingJob::MaterializedView(_)
                | StreamingJob::Sink(..)
                | StreamingJob::Index(_, _) => {}
                StreamingJob::Table(_, _, _) | StreamingJob::Source(_) => {
                    return Err(
                        anyhow!("snapshot_backfill not enabled for table and source").into(),
                    );
                }
            }
        }

        let complete_graph = CompleteStreamFragmentGraph::with_upstreams(
            fragment_graph,
            FragmentGraphUpstreamContext {
                upstream_root_fragments,
            },
            (&stream_job).into(),
        )?;
        let database_resource_group = self
            .metadata_manager
            .get_database_resource_group(stream_job.database_id())
            .await?;
        let is_serverless_backfill = matches!(
            &resource_type,
            streaming_job_resource_type::ResourceType::ServerlessBackfill(true)
        );

        // 3. Build the actor graph.
        let actor_graph_builder = ActorGraphBuilder::new(complete_graph)?;

        let ActorGraphBuildResult {
            graph,
            downstream_fragment_relations,
            upstream_fragment_downstreams,
            replace_upstream,
        } = actor_graph_builder.generate_graph()?;
        assert!(replace_upstream.is_empty());

        // 4. Build the table fragments structure that will be persisted in the stream manager,
        // and the context that contains all information needed for building the
        // actors on the compute nodes.

        let stream_job_fragments =
            StreamJobFragments::new(id, graph, stream_ctx.clone(), max_parallelism.get());

        if let Some(mview_fragment) = stream_job_fragments.mview_fragment() {
            stream_job.set_table_vnode_count(mview_fragment.vnode_count());
        }

        let new_upstream_sink = if let StreamingJob::Sink(sink, _) = &stream_job
            && let Ok(table_id) = sink.get_target_table()
        {
            let tables = self
                .metadata_manager
                .get_table_catalog_by_ids(&[*table_id])
                .await?;
            let target_table = tables
                .first()
                .ok_or_else(|| MetaError::catalog_id_not_found("table", *table_id))?;
            let sink_fragment = stream_job_fragments
                .sink_fragment()
                .ok_or_else(|| anyhow::anyhow!("sink fragment not found for sink {}", sink.id))?;
            let mview_fragment_id = self
                .metadata_manager
                .catalog_controller
                .get_mview_fragment_by_id(table_id.as_job_id())
                .await?;
            let upstream_sink_info = build_upstream_sink_info(
                sink.id,
                sink.original_target_columns.clone(),
                sink_fragment.fragment_id as _,
                target_table,
                mview_fragment_id,
            )?;
            Some(upstream_sink_info)
        } else {
            None
        };

        let mut cdc_table_snapshot_splits = None;
        if let StreamingJob::Table(None, table, TableJobType::SharedCdcSource) = &stream_job
            && let Some((_, stream_cdc_scan)) =
                parallel_cdc_table_backfill_fragment(stream_job_fragments.fragments.values())
        {
            {
                // Create parallel splits for a CDC table. The resulted split assignments are persisted and immutable.
                let splits = try_init_parallel_cdc_table_snapshot_splits(
                    table.id,
                    stream_cdc_scan.cdc_table_desc.as_ref().unwrap(),
                    self.env.meta_store_ref(),
                    stream_cdc_scan.options.as_ref().unwrap(),
                    self.env.opts.cdc_table_split_init_insert_batch_size,
                    self.env.opts.cdc_table_split_init_sleep_interval_splits,
                    self.env.opts.cdc_table_split_init_sleep_duration_millis,
                )
                .await?;
                cdc_table_snapshot_splits = Some(splits);
            }
        }

        let ctx = CreateStreamingJobContext {
            upstream_fragment_downstreams,
            database_resource_group,
            definition: stream_job.definition(),
            create_type: stream_job.create_type(),
            job_type: (&stream_job).into(),
            streaming_job: stream_job,
            new_upstream_sink,
            option: CreateStreamingJobOption {},
            snapshot_backfill_info,
            cross_db_snapshot_backfill_info,
            fragment_backfill_ordering,
            locality_fragment_state_table_mapping,
            cdc_table_snapshot_splits,
            is_serverless_backfill,
            resource_type,
            streaming_job_model: streaming_job_model.clone(),
            replace_sink: None,
            refresh_interval_sec: streaming_job_model.refresh_interval_sec.map(|s| s as u64),
            since_timestamp_epoch,
        };

        Ok((
            ctx,
            StreamJobFragmentsToCreate {
                inner: stream_job_fragments,
                downstreams: downstream_fragment_relations,
            },
        ))
    }

    /// `build_replace_table` builds a job replacement and returns the context and new job
    /// fragments.
    ///
    /// Note that we use a dummy ID for the new job fragments and replace it with the real one after
    /// replacement is finished.
    pub(crate) async fn build_replace_job(
        &self,
        stream_ctx: StreamContext,
        stream_job: &StreamingJob,
        mut fragment_graph: StreamFragmentGraph,
        tmp_job_id: JobId,
        auto_refresh_schema_sinks: Option<Vec<AutoRefreshSchemaSinkContext>>,
        streaming_job_model: streaming_job::Model,
    ) -> MetaResult<(ReplaceStreamJobContext, StreamJobFragmentsToCreate)> {
        match &stream_job {
            StreamingJob::Table(..)
            | StreamingJob::Source(..)
            | StreamingJob::MaterializedView(..) => {}
            StreamingJob::Sink(..) | StreamingJob::Index(..) => {
                bail_not_implemented!("schema change for {}", stream_job.job_type_str())
            }
        }

        let id = stream_job.id();

        // check if performing drop table connector
        let mut drop_table_associated_source_id = None;
        if let StreamingJob::Table(None, _, _) = &stream_job {
            drop_table_associated_source_id = self
                .metadata_manager
                .get_table_associated_source_id(id.as_mv_table_id())
                .await?;
        }

        let old_fragments = self.metadata_manager.get_job_fragments_by_id(id).await?;
        let old_internal_table_ids = old_fragments.internal_table_ids();

        // handle drop table's associated source
        let mut drop_table_connector_ctx = None;
        if let Some(to_remove_source_id) = drop_table_associated_source_id {
            // drop table's associated source means the fragment containing the table has just one internal table (associated source's state table)
            debug_assert!(old_internal_table_ids.len() == 1);

            drop_table_connector_ctx = Some(DropTableConnectorContext {
                // we do not remove the original table catalog as it's still needed for the streaming job
                // just need to remove the ref to the state table
                to_change_streaming_job_id: id,
                to_remove_state_table_id: old_internal_table_ids[0], // asserted before
                to_remove_source_id,
            });
        } else if stream_job.is_materialized_view() {
            // If it's ALTER MV, use `state::match` to match the internal tables, which is more complicated
            // but more robust.
            let old_fragments_upstreams = self
                .metadata_manager
                .catalog_controller
                .upstream_fragments(old_fragments.fragment_ids())
                .await?;

            let old_state_graph =
                state_match::Graph::from_existing(&old_fragments, &old_fragments_upstreams);
            let new_state_graph = state_match::Graph::from_building(&fragment_graph);
            let result = state_match::match_graph(&new_state_graph, &old_state_graph)
                .context("incompatible altering on the streaming job states")?;

            fragment_graph.fit_internal_table_ids_with_mapping(result.table_matches);
            fragment_graph.fit_snapshot_backfill_epochs(result.snapshot_backfill_epochs);
        } else {
            // If it's ALTER TABLE or SOURCE, use a trivial table id matching algorithm to keep the original behavior.
            // TODO(alter-mv): this is actually a special case of ALTER MV, can we merge the two branches?
            let old_internal_tables = self
                .metadata_manager
                .get_table_catalog_by_ids(&old_internal_table_ids)
                .await?;
            fragment_graph.fit_internal_tables_trivial(old_internal_tables)?;
        }

        // 1. Resolve the edges to the downstream fragments, extend the fragment graph to a complete
        // graph that contains all information needed for building the actor graph.
        let original_root_fragment = old_fragments
            .root_fragment()
            .expect("root fragment not found");

        let job_type = StreamingJobType::from(stream_job);

        // Extract the downstream fragments from the fragment graph.
        let mut downstream_fragments = self.metadata_manager.get_downstream_fragments(id).await?;

        if let Some(auto_refresh_schema_sinks) = &auto_refresh_schema_sinks {
            let mut remaining_fragment: HashSet<_> = auto_refresh_schema_sinks
                .iter()
                .map(|sink| sink.original_fragment.fragment_id)
                .collect();
            for (_, downstream_fragment) in &mut downstream_fragments {
                if let Some(sink) = auto_refresh_schema_sinks.iter().find(|sink| {
                    sink.original_fragment.fragment_id == downstream_fragment.fragment_id
                }) {
                    assert!(remaining_fragment.remove(&downstream_fragment.fragment_id));
                    // Actor locations will be resolved inside barrier worker during rendering.
                    // For now, just replace fragment info and nodes.
                    *downstream_fragment = sink.new_fragment.clone();
                }
            }
            assert!(remaining_fragment.is_empty());
        }

        // build complete graph based on the table job type
        let complete_graph = match &job_type {
            StreamingJobType::Table(TableJobType::General) | StreamingJobType::Source => {
                CompleteStreamFragmentGraph::with_downstreams(
                    fragment_graph,
                    FragmentGraphDownstreamContext {
                        original_root_fragment_id: original_root_fragment.fragment_id,
                        downstream_fragments,
                    },
                    job_type,
                )?
            }
            StreamingJobType::Table(TableJobType::SharedCdcSource)
            | StreamingJobType::MaterializedView => {
                // CDC tables or materialized views can have upstream jobs as well.
                let upstream_root_fragments = self
                    .metadata_manager
                    .get_upstream_root_fragments(fragment_graph.dependent_table_ids())
                    .await?;

                CompleteStreamFragmentGraph::with_upstreams_and_downstreams(
                    fragment_graph,
                    FragmentGraphUpstreamContext {
                        upstream_root_fragments,
                    },
                    FragmentGraphDownstreamContext {
                        original_root_fragment_id: original_root_fragment.fragment_id,
                        downstream_fragments,
                    },
                    job_type,
                )?
            }
            _ => unreachable!(),
        };

        let resource_group = self
            .metadata_manager
            .get_database_resource_group(stream_job.database_id())
            .await?;

        let actor_graph_builder = ActorGraphBuilder::new(complete_graph)?;

        let ActorGraphBuildResult {
            graph,
            downstream_fragment_relations,
            upstream_fragment_downstreams,
            mut replace_upstream,
        } = actor_graph_builder.generate_graph()?;

        // general table & source does not have upstream job, so the dispatchers should be empty
        if matches!(
            job_type,
            StreamingJobType::Source | StreamingJobType::Table(TableJobType::General)
        ) {
            assert!(upstream_fragment_downstreams.is_empty());
        }

        // 3. Build the table fragments structure that will be persisted in the stream manager, and
        // the context that contains all information needed for building the actors on the compute
        // nodes.
        let stream_job_fragments =
            StreamJobFragments::new(tmp_job_id, graph, stream_ctx, old_fragments.max_parallelism);

        if let Some(sinks) = &auto_refresh_schema_sinks {
            for sink in sinks {
                replace_upstream
                    .remove(&sink.new_fragment.fragment_id)
                    .expect("should exist");
            }
        }

        // Note: no need to set `vnode_count` as it's already set by the frontend.
        // See `get_replace_table_plan`.

        let ctx = ReplaceStreamJobContext {
            old_fragments,
            replace_upstream,
            upstream_fragment_downstreams,
            streaming_job: stream_job.clone(),
            database_resource_group: resource_group,
            tmp_id: tmp_job_id,
            drop_table_connector_ctx,
            auto_refresh_schema_sinks,
            streaming_job_model,
        };

        Ok((
            ctx,
            StreamJobFragmentsToCreate {
                inner: stream_job_fragments,
                downstreams: downstream_fragment_relations,
            },
        ))
    }

    async fn alter_name(
        &self,
        relation: alter_name_request::Object,
        new_name: &str,
    ) -> MetaResult<NotificationVersion> {
        let (obj_type, id): (ObjectType, ObjectId) = match relation {
            alter_name_request::Object::TableId(id) => (ObjectType::Table, id.into()),
            alter_name_request::Object::ViewId(id) => (ObjectType::View, id.into()),
            alter_name_request::Object::IndexId(id) => (ObjectType::Index, id.into()),
            alter_name_request::Object::SinkId(id) => (ObjectType::Sink, id.into()),
            alter_name_request::Object::SourceId(id) => (ObjectType::Source, id.into()),
            alter_name_request::Object::SchemaId(id) => (ObjectType::Schema, id.into()),
            alter_name_request::Object::DatabaseId(id) => (ObjectType::Database, id.into()),
            alter_name_request::Object::SubscriptionId(id) => (ObjectType::Subscription, id.into()),
        };
        self.metadata_manager
            .catalog_controller
            .alter_name(obj_type, id, new_name)
            .await
    }

    async fn alter_swap_rename(
        &self,
        object: alter_swap_rename_request::Object,
    ) -> MetaResult<NotificationVersion> {
        let (obj_type, src_id, dst_id) = match object {
            alter_swap_rename_request::Object::Schema(_) => unimplemented!("schema swap"),
            alter_swap_rename_request::Object::Table(objs) => {
                let (src_id, dst_id) = (objs.src_object_id, objs.dst_object_id);
                (ObjectType::Table, src_id, dst_id)
            }
            alter_swap_rename_request::Object::View(objs) => {
                let (src_id, dst_id) = (objs.src_object_id, objs.dst_object_id);
                (ObjectType::View, src_id, dst_id)
            }
            alter_swap_rename_request::Object::Source(objs) => {
                let (src_id, dst_id) = (objs.src_object_id, objs.dst_object_id);
                (ObjectType::Source, src_id, dst_id)
            }
            alter_swap_rename_request::Object::Sink(objs) => {
                let (src_id, dst_id) = (objs.src_object_id, objs.dst_object_id);
                (ObjectType::Sink, src_id, dst_id)
            }
            alter_swap_rename_request::Object::Subscription(objs) => {
                let (src_id, dst_id) = (objs.src_object_id, objs.dst_object_id);
                (ObjectType::Subscription, src_id, dst_id)
            }
        };

        self.metadata_manager
            .catalog_controller
            .alter_swap_rename(obj_type, src_id, dst_id)
            .await
    }

    async fn alter_owner(
        &self,
        object: Object,
        owner_id: UserId,
    ) -> MetaResult<NotificationVersion> {
        let (obj_type, id): (ObjectType, ObjectId) = match object {
            Object::TableId(id) => (ObjectType::Table, id.into()),
            Object::ViewId(id) => (ObjectType::View, id.into()),
            Object::SourceId(id) => (ObjectType::Source, id.into()),
            Object::SinkId(id) => (ObjectType::Sink, id.into()),
            Object::SchemaId(id) => (ObjectType::Schema, id.into()),
            Object::DatabaseId(id) => (ObjectType::Database, id.into()),
            Object::SubscriptionId(id) => (ObjectType::Subscription, id.into()),
            Object::ConnectionId(id) => (ObjectType::Connection, id.into()),
            Object::FunctionId(id) => (ObjectType::Function, id.into()),
            Object::SecretId(id) => (ObjectType::Secret, id.into()),
        };
        self.metadata_manager
            .catalog_controller
            .alter_owner(obj_type, id, owner_id as _)
            .await
    }

    async fn alter_set_schema(
        &self,
        object: alter_set_schema_request::Object,
        new_schema_id: SchemaId,
    ) -> MetaResult<NotificationVersion> {
        let (obj_type, id): (ObjectType, ObjectId) = match object {
            alter_set_schema_request::Object::TableId(id) => (ObjectType::Table, id.into()),
            alter_set_schema_request::Object::ViewId(id) => (ObjectType::View, id.into()),
            alter_set_schema_request::Object::SourceId(id) => (ObjectType::Source, id.into()),
            alter_set_schema_request::Object::SinkId(id) => (ObjectType::Sink, id.into()),
            alter_set_schema_request::Object::FunctionId(id) => (ObjectType::Function, id.into()),
            alter_set_schema_request::Object::ConnectionId(id) => {
                (ObjectType::Connection, id.into())
            }
            alter_set_schema_request::Object::SubscriptionId(id) => {
                (ObjectType::Subscription, id.into())
            }
        };
        self.metadata_manager
            .catalog_controller
            .alter_schema(obj_type, id, new_schema_id)
            .await
    }

    pub async fn wait(&self, job_id: Option<JobId>) -> MetaResult<WaitVersion> {
        if let Some(job_id) = job_id {
            let database_id = self
                .metadata_manager
                .catalog_controller
                .get_object_database_id(job_id)
                .await?;
            let catalog_version = self
                .metadata_manager
                .wait_streaming_job_finished(database_id, job_id)
                .await?;
            let hummock_version_id = self.barrier_manager.get_hummock_version_id().await;
            return Ok(WaitVersion {
                catalog_version,
                hummock_version_id,
            });
        }

        let timeout_ms = 2 * 60 * 60 * 1000;
        let poll_interval = Duration::from_millis(100);
        for _ in 0..(timeout_ms / poll_interval.as_millis() as usize) {
            let creating_jobs = self
                .metadata_manager
                .catalog_controller
                .list_creating_jobs(true, None)
                .await?;
            if creating_jobs.is_empty() {
                let catalog_version = self
                    .metadata_manager
                    .catalog_controller
                    .notify_frontend_trivial()
                    .await;
                let hummock_version_id = self.barrier_manager.get_hummock_version_id().await;
                return Ok(WaitVersion {
                    catalog_version,
                    hummock_version_id,
                });
            }

            sleep(poll_interval).await;
        }
        Err(MetaError::cancelled(format!(
            "timeout after {timeout_ms}ms"
        )))
    }

    async fn comment_on(&self, comment: Comment) -> MetaResult<NotificationVersion> {
        self.metadata_manager
            .catalog_controller
            .comment_on(comment)
            .await
    }

    async fn alter_streaming_job_config(
        &self,
        job_id: JobId,
        entries_to_add: HashMap<String, String>,
        keys_to_remove: Vec<String>,
    ) -> MetaResult<NotificationVersion> {
        self.metadata_manager
            .catalog_controller
            .alter_streaming_job_config(job_id, entries_to_add, keys_to_remove)
            .await
    }
}

fn report_create_object(
    job_id: JobId,
    event_name: &str,
    obj_type: PbTelemetryDatabaseObject,
    connector_name: Option<String>,
    attr_info: Option<jsonbb::Value>,
) {
    report_event(
        PbTelemetryEventStage::CreateStreamJob,
        event_name,
        job_id.as_raw_id() as _,
        connector_name,
        Some(obj_type),
        attr_info,
    );
}

pub fn build_upstream_sink_info(
    sink_id: SinkId,
    original_target_columns: Vec<PbColumnCatalog>,
    sink_fragment_id: FragmentId,
    target_table: &PbTable,
    target_fragment_id: FragmentId,
) -> MetaResult<UpstreamSinkInfo> {
    let sink_columns = if !original_target_columns.is_empty() {
        original_target_columns.clone()
    } else {
        // This is due to the fact that the value did not exist in earlier versions,
        // which means no schema changes such as `ADD/DROP COLUMN` have been made to the table.
        // Therefore the columns of the table at this point are `original_target_columns`.
        // This value of sink will be filled on the meta.
        target_table.columns.clone()
    };

    let sink_output_fields = sink_columns
        .iter()
        .map(|col| Field::from(col.column_desc.as_ref().unwrap()).to_prost())
        .collect_vec();
    let output_indices = (0..sink_output_fields.len())
        .map(|i| i as u32)
        .collect_vec();

    let dist_key_indices: anyhow::Result<Vec<u32>> = try {
        let sink_idx_by_col_id = sink_columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let column_id = col.column_desc.as_ref().unwrap().column_id;
                (column_id, idx as u32)
            })
            .collect::<HashMap<_, _>>();
        target_table
            .distribution_key
            .iter()
            .map(|dist_idx| {
                let column_id = target_table.columns[*dist_idx as usize]
                    .column_desc
                    .as_ref()
                    .unwrap()
                    .column_id;
                let sink_idx = sink_idx_by_col_id
                    .get(&column_id)
                    .ok_or_else(|| anyhow::anyhow!("column id {} not found in sink", column_id))?;
                Ok(*sink_idx)
            })
            .collect::<anyhow::Result<Vec<_>>>()?
    };
    let dist_key_indices =
        dist_key_indices.map_err(|e| e.context("failed to get distribution key indices"))?;
    let downstream_fragment_id = target_fragment_id as _;
    let new_downstream_relation = DownstreamFragmentRelation {
        downstream_fragment_id,
        dispatcher_type: DispatcherType::Hash,
        dist_key_indices,
        output_mapping: PbDispatchOutputMapping::simple(output_indices),
    };
    let current_target_columns = target_table.get_columns();
    let project_exprs = build_select_node_list(&sink_columns, current_target_columns)?;
    Ok(UpstreamSinkInfo {
        sink_id,
        sink_fragment_id: sink_fragment_id as _,
        sink_output_fields,
        sink_original_target_columns: original_target_columns,
        project_exprs,
        new_sink_downstream: new_downstream_relation,
    })
}

pub fn refill_upstream_sink_union_in_table(
    union_fragment_root: &mut PbStreamNode,
    upstream_sink_infos: &Vec<UpstreamSinkInfo>,
) {
    visit_stream_node_cont_mut(union_fragment_root, |node| {
        if let Some(NodeBody::UpstreamSinkUnion(upstream_sink_union)) = &mut node.node_body {
            let init_upstreams = upstream_sink_infos
                .iter()
                .map(|info| PbUpstreamSinkInfo {
                    upstream_fragment_id: info.sink_fragment_id,
                    sink_output_schema: info.sink_output_fields.clone(),
                    project_exprs: info.project_exprs.clone(),
                })
                .collect();
            upstream_sink_union.init_upstreams = init_upstreams;
            false
        } else {
            true
        }
    });
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;

    #[test]
    fn test_validate_specified_parallelism_accepts_within_max() {
        DdlController::validate_specified_parallelism(
            Some(NonZeroUsize::new(4).unwrap()),
            Some(NonZeroUsize::new(8).unwrap()),
            NonZeroUsize::new(8).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn test_validate_specified_parallelism_rejects_parallelism_over_max() {
        let result = DdlController::validate_specified_parallelism(
            Some(NonZeroUsize::new(9).unwrap()),
            None,
            NonZeroUsize::new(8).unwrap(),
        );
        assert!(matches!(
            result,
            Err(ref e) if matches!(e.inner(), MetaErrorInner::InvalidParameter(_))
        ));
    }

    #[test]
    fn test_validate_specified_parallelism_rejects_backfill_parallelism_over_max() {
        let result = DdlController::validate_specified_parallelism(
            None,
            Some(NonZeroUsize::new(9).unwrap()),
            NonZeroUsize::new(8).unwrap(),
        );
        assert!(matches!(
            result,
            Err(ref e) if matches!(e.inner(), MetaErrorInner::InvalidParameter(_))
        ));
    }
}
