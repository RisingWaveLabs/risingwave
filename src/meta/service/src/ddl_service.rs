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

use std::collections::{HashMap, HashSet};
use std::pin::pin;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use futures::future::select;
use rand::rng as thread_rng;
use rand::seq::IndexedRandom;
use replace_job_plan::{ReplaceSource, ReplaceTable};
use risingwave_common::catalog::cdc_type_compatibility::cdc_auto_schema_change_existing_type_compatible;
use risingwave_common::catalog::{AlterDatabaseParam, ColumnCatalog};
use risingwave_common::id::{ObjectId, TableId};
use risingwave_common::system_param::adaptive_parallelism_strategy::parse_strategy;
use risingwave_common::types::DataType;
use risingwave_common::util::stream_graph_visitor;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::iceberg::ENABLE_PK_INDEX;
use risingwave_meta::barrier::{BarrierScheduler, Command, ResumeBackfillTarget};
use risingwave_meta::manager::{EventLogManagerRef, MetadataManager, iceberg_compaction};
use risingwave_meta::model::TableParallelism as ModelTableParallelism;
use risingwave_meta::rpc::metrics::MetaMetrics;
use risingwave_meta::stream::{ParallelismPolicy, ReschedulePolicy, ResourceGroupPolicy};
use risingwave_meta::{MetaResult, bail_invalid_parameter, bail_unavailable};
use risingwave_meta_model::StreamingParallelism;
use risingwave_pb::catalog::connection::Info as ConnectionInfo;
use risingwave_pb::catalog::table::{CdcTableType as PbCdcTableType, OptionalAssociatedSourceId};
use risingwave_pb::catalog::{Comment, Connection, PbCreateType, Secret, Table};
use risingwave_pb::common::WorkerType;
use risingwave_pb::common::worker_node::State;
use risingwave_pb::ddl_service::create_iceberg_table_request::{PbSinkJobInfo, PbTableJobInfo};
use risingwave_pb::ddl_service::ddl_service_server::DdlService;
use risingwave_pb::ddl_service::drop_table_request::PbSourceId;
use risingwave_pb::ddl_service::replace_job_plan::ReplaceMaterializedView;
use risingwave_pb::ddl_service::{streaming_job_resource_type, *};
use risingwave_pb::frontend_service::GetTableReplacePlanRequest;
use risingwave_pb::meta::event_log;
use risingwave_pb::meta::table_parallelism::{FixedParallelism, Parallelism};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::throttle_mutation::ThrottleConfig;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tonic::{Request, Response, Status};

use crate::MetaError;
use crate::barrier::BarrierManagerRef;
use crate::manager::iceberg_pk_index_sink::IcebergPkIndexSinkManager;
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{MetaSrvEnv, StreamingJob};
use crate::rpc::ddl_controller::{
    DdlCommand, DdlController, DropMode, ReplaceStreamJobInfo, StreamingJobId,
};
use crate::stream::{GlobalStreamManagerRef, SourceManagerRef};

#[derive(Clone)]
pub struct DdlServiceImpl {
    env: MetaSrvEnv,

    metadata_manager: MetadataManager,
    sink_manager: SinkCoordinatorManager,
    ddl_controller: DdlController,
    meta_metrics: Arc<MetaMetrics>,
    iceberg_compaction_manager: iceberg_compaction::IcebergCompactionManagerRef,
    barrier_scheduler: BarrierScheduler,
    iceberg_pk_index_sink_manager: IcebergPkIndexSinkManager,
}

impl DdlServiceImpl {
    pub async fn new(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        stream_manager: GlobalStreamManagerRef,
        source_manager: SourceManagerRef,
        barrier_manager: BarrierManagerRef,
        sink_manager: SinkCoordinatorManager,
        meta_metrics: Arc<MetaMetrics>,
        iceberg_compaction_manager: iceberg_compaction::IcebergCompactionManagerRef,
        barrier_scheduler: BarrierScheduler,
        iceberg_pk_index_sink_manager: IcebergPkIndexSinkManager,
    ) -> Self {
        let ddl_controller = DdlController::new(
            env.clone(),
            metadata_manager.clone(),
            stream_manager,
            source_manager,
            barrier_manager,
            sink_manager.clone(),
            iceberg_compaction_manager.clone(),
            iceberg_pk_index_sink_manager.clone(),
        )
        .await;
        Self {
            env,
            metadata_manager,
            sink_manager,
            ddl_controller,
            meta_metrics,
            iceberg_compaction_manager,
            barrier_scheduler,
            iceberg_pk_index_sink_manager,
        }
    }

    fn extract_replace_table_info(
        ReplaceJobPlan {
            fragment_graph,
            replace_job,
        }: ReplaceJobPlan,
    ) -> ReplaceStreamJobInfo {
        let replace_streaming_job: StreamingJob = match replace_job.unwrap() {
            replace_job_plan::ReplaceJob::ReplaceTable(ReplaceTable {
                table,
                source,
                job_type,
            }) => StreamingJob::Table(
                source,
                table.unwrap(),
                TableJobType::try_from(job_type).unwrap(),
            ),
            replace_job_plan::ReplaceJob::ReplaceSource(ReplaceSource { source }) => {
                StreamingJob::Source(source.unwrap())
            }
            replace_job_plan::ReplaceJob::ReplaceMaterializedView(ReplaceMaterializedView {
                table,
            }) => StreamingJob::MaterializedView(table.unwrap()),
            replace_job_plan::ReplaceJob::ReplaceSink(_) => unreachable!("use replace sink path"),
        };

        ReplaceStreamJobInfo {
            streaming_job: replace_streaming_job,
            fragment_graph: fragment_graph.unwrap(),
        }
    }

    fn default_streaming_job_resource_type() -> streaming_job_resource_type::ResourceType {
        streaming_job_resource_type::ResourceType::Regular(true)
    }

    pub fn start_migrate_table_fragments(&self) -> (JoinHandle<()>, Sender<()>) {
        tracing::info!("start migrate legacy table fragments task");
        let env = self.env.clone();
        let metadata_manager = self.metadata_manager.clone();
        let ddl_controller = self.ddl_controller.clone();

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            async fn migrate_inner(
                env: &MetaSrvEnv,
                metadata_manager: &MetadataManager,
                ddl_controller: &DdlController,
            ) -> MetaResult<()> {
                let tables = metadata_manager
                    .catalog_controller
                    .list_unmigrated_tables()
                    .await?;

                if tables.is_empty() {
                    tracing::info!("no legacy table fragments need migration");
                    return Ok(());
                }

                let client = {
                    let workers = metadata_manager
                        .list_worker_node(Some(WorkerType::Frontend), Some(State::Running))
                        .await?;
                    if workers.is_empty() {
                        return Err(anyhow::anyhow!("no active frontend nodes found").into());
                    }
                    let worker = workers.choose(&mut thread_rng()).unwrap();
                    env.frontend_client_pool().get(worker).await?
                };

                for table in tables {
                    let start = tokio::time::Instant::now();
                    let req = GetTableReplacePlanRequest {
                        database_id: table.database_id,
                        table_id: table.id,
                        cdc_table_change: None,
                    };
                    let resp = client
                        .get_table_replace_plan(req)
                        .await
                        .context("failed to get table replace plan from frontend")?;

                    let plan = resp.into_inner().replace_plan.unwrap();
                    let replace_info = DdlServiceImpl::extract_replace_table_info(plan);
                    ddl_controller
                        .run_command(DdlCommand::ReplaceStreamJob(replace_info))
                        .await?;
                    tracing::info!(elapsed=?start.elapsed(), table_id=%table.id, "migrated table fragments");
                }
                tracing::info!("successfully migrated all legacy table fragments");

                Ok(())
            }

            let migrate_future = async move {
                let mut attempt = 0;
                loop {
                    match migrate_inner(&env, &metadata_manager, &ddl_controller).await {
                        Ok(_) => break,
                        Err(e) => {
                            attempt += 1;
                            tracing::error!(
                                "failed to migrate legacy table fragments: {}, attempt {}, retrying in 5 secs",
                                e.as_report(),
                                attempt
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        }
                    }
                }
            };

            select(pin!(migrate_future), shutdown_rx).await;
        });

        (join_handle, shutdown_tx)
    }
}

#[async_trait::async_trait]
impl DdlService for DdlServiceImpl {
    async fn create_database(
        &self,
        request: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        let req = request.into_inner();
        let database = req.get_db()?.clone();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateDatabase(database))
            .await?;

        Ok(Response::new(CreateDatabaseResponse {
            status: None,
            version,
        }))
    }

    async fn drop_database(
        &self,
        request: Request<DropDatabaseRequest>,
    ) -> Result<Response<DropDatabaseResponse>, Status> {
        let req = request.into_inner();
        let database_id = req.get_database_id();

        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropDatabase(database_id))
            .await?;

        Ok(Response::new(DropDatabaseResponse {
            status: None,
            version,
        }))
    }

    async fn create_secret(
        &self,
        request: Request<CreateSecretRequest>,
    ) -> Result<Response<CreateSecretResponse>, Status> {
        let req = request.into_inner();
        let pb_secret = Secret {
            id: 0.into(),
            name: req.get_name().clone(),
            database_id: req.get_database_id(),
            value: req.get_value().clone(),
            owner: req.get_owner_id(),
            schema_id: req.get_schema_id(),
        };
        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateSecret(pb_secret))
            .await?;

        Ok(Response::new(CreateSecretResponse { version }))
    }

    async fn drop_secret(
        &self,
        request: Request<DropSecretRequest>,
    ) -> Result<Response<DropSecretResponse>, Status> {
        let req = request.into_inner();
        let secret_id = req.get_secret_id();
        let drop_mode = DropMode::from_request_setting(req.cascade);
        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropSecret(secret_id, drop_mode))
            .await?;

        Ok(Response::new(DropSecretResponse {
            status: None,
            version,
        }))
    }

    async fn alter_secret(
        &self,
        request: Request<AlterSecretRequest>,
    ) -> Result<Response<AlterSecretResponse>, Status> {
        let req = request.into_inner();
        let pb_secret = Secret {
            id: req.get_secret_id(),
            name: req.get_name().clone(),
            database_id: req.get_database_id(),
            value: req.get_value().clone(),
            owner: req.get_owner_id(),
            schema_id: req.get_schema_id(),
        };
        let version = self
            .ddl_controller
            .run_command(DdlCommand::AlterSecret(pb_secret))
            .await?;

        Ok(Response::new(AlterSecretResponse { version }))
    }

    async fn create_schema(
        &self,
        request: Request<CreateSchemaRequest>,
    ) -> Result<Response<CreateSchemaResponse>, Status> {
        let req = request.into_inner();
        let schema = req.get_schema()?.clone();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateSchema(schema))
            .await?;

        Ok(Response::new(CreateSchemaResponse {
            status: None,
            version,
        }))
    }

    async fn drop_schema(
        &self,
        request: Request<DropSchemaRequest>,
    ) -> Result<Response<DropSchemaResponse>, Status> {
        let req = request.into_inner();
        let schema_id = req.get_schema_id();
        let drop_mode = DropMode::from_request_setting(req.cascade);
        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropSchema(schema_id, drop_mode))
            .await?;
        Ok(Response::new(DropSchemaResponse {
            status: None,
            version,
        }))
    }

    async fn create_source(
        &self,
        request: Request<CreateSourceRequest>,
    ) -> Result<Response<CreateSourceResponse>, Status> {
        let req = request.into_inner();
        let source = req.get_source()?.clone();

        match req.fragment_graph {
            None => {
                let version = self
                    .ddl_controller
                    .run_command(DdlCommand::CreateNonSharedSource(source, None))
                    .await?;
                Ok(Response::new(CreateSourceResponse {
                    status: None,
                    version,
                }))
            }
            Some(fragment_graph) => {
                // The id of stream job has been set above
                let stream_job = StreamingJob::Source(source);
                let version = self
                    .ddl_controller
                    .run_command(DdlCommand::CreateStreamingJob {
                        stream_job,
                        fragment_graph,
                        dependencies: HashSet::new(),
                        resource_type: Self::default_streaming_job_resource_type(),
                        if_not_exists: req.if_not_exists,
                        refresh_interval_sec: None,
                        replace_sink: None,
                        since_timestamp_epoch: None,
                    })
                    .await?;
                Ok(Response::new(CreateSourceResponse {
                    status: None,
                    version,
                }))
            }
        }
    }

    async fn drop_source(
        &self,
        request: Request<DropSourceRequest>,
    ) -> Result<Response<DropSourceResponse>, Status> {
        let request = request.into_inner();
        let source_id = request.source_id;
        let drop_mode = DropMode::from_request_setting(request.cascade);
        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropSource(source_id, drop_mode))
            .await?;

        Ok(Response::new(DropSourceResponse {
            status: None,
            version,
        }))
    }

    async fn reset_source(
        &self,
        request: Request<ResetSourceRequest>,
    ) -> Result<Response<ResetSourceResponse>, Status> {
        let request = request.into_inner();
        let source_id = request.source_id;

        tracing::info!(
            source_id = %source_id,
            "Received RESET SOURCE request, routing to DDL controller"
        );

        // Route to DDL controller
        let version = self
            .ddl_controller
            .run_command(DdlCommand::ResetSource(source_id))
            .await?;

        Ok(Response::new(ResetSourceResponse {
            status: None,
            version,
        }))
    }

    async fn create_sink(
        &self,
        request: Request<CreateSinkRequest>,
    ) -> Result<Response<CreateSinkResponse>, Status> {
        self.env.idle_manager().record_activity();

        let req = request.into_inner();

        let sink = req.get_sink()?.clone();
        let fragment_graph = req.get_fragment_graph()?.clone();
        let dependencies = req.get_dependencies().iter().copied().collect();
        let resource_type = req
            .resource_type
            .and_then(|resource_type| resource_type.resource_type)
            .unwrap_or_else(Self::default_streaming_job_resource_type);
        let since_timestamp_epoch = req.since_timestamp_epoch;

        let stream_job = StreamingJob::Sink(sink, None);

        let command = DdlCommand::CreateStreamingJob {
            stream_job,
            fragment_graph,
            dependencies,
            resource_type,
            if_not_exists: req.if_not_exists,
            refresh_interval_sec: None,
            replace_sink: None,
            since_timestamp_epoch,
        };

        let version = self.ddl_controller.run_command(command).await?;

        Ok(Response::new(CreateSinkResponse {
            status: None,
            version,
        }))
    }

    async fn drop_sink(
        &self,
        request: Request<DropSinkRequest>,
    ) -> Result<Response<DropSinkResponse>, Status> {
        let request = request.into_inner();
        let sink_id = request.sink_id;
        let drop_mode = DropMode::from_request_setting(request.cascade);

        let command = DdlCommand::DropStreamingJob {
            job_id: StreamingJobId::Sink(sink_id),
            drop_mode,
        };

        let version = self.ddl_controller.run_command(command).await?;

        self.sink_manager
            .stop_sink_coordinator(vec![SinkId::from(sink_id)])
            .await;
        self.iceberg_compaction_manager
            .clear_iceberg_maintenance_by_sink_id(SinkId::from(sink_id));

        Ok(Response::new(DropSinkResponse {
            status: None,
            version,
        }))
    }

    async fn create_subscription(
        &self,
        request: Request<CreateSubscriptionRequest>,
    ) -> Result<Response<CreateSubscriptionResponse>, Status> {
        self.env.idle_manager().record_activity();

        let req = request.into_inner();

        let subscription = req.get_subscription()?.clone();
        let command = DdlCommand::CreateSubscription(subscription);

        let version = self.ddl_controller.run_command(command).await?;

        Ok(Response::new(CreateSubscriptionResponse {
            status: None,
            version,
        }))
    }

    async fn drop_subscription(
        &self,
        request: Request<DropSubscriptionRequest>,
    ) -> Result<Response<DropSubscriptionResponse>, Status> {
        let request = request.into_inner();
        let subscription_id = request.subscription_id;
        let drop_mode = DropMode::from_request_setting(request.cascade);

        let command = DdlCommand::DropSubscription(subscription_id, drop_mode);

        let version = self.ddl_controller.run_command(command).await?;

        Ok(Response::new(DropSubscriptionResponse {
            status: None,
            version,
        }))
    }

    async fn create_materialized_view(
        &self,
        request: Request<CreateMaterializedViewRequest>,
    ) -> Result<Response<CreateMaterializedViewResponse>, Status> {
        self.env.idle_manager().record_activity();

        let req = request.into_inner();
        let mview = req.get_materialized_view()?.clone();
        let fragment_graph = req.get_fragment_graph()?.clone();
        let dependencies = req.get_dependencies().iter().copied().collect();
        let resource_type = req.resource_type.unwrap().resource_type.unwrap();

        let stream_job = StreamingJob::MaterializedView(mview);
        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateStreamingJob {
                stream_job,
                fragment_graph,
                dependencies,
                resource_type,
                if_not_exists: req.if_not_exists,
                refresh_interval_sec: req.refresh_interval_sec,
                replace_sink: None,
                since_timestamp_epoch: None,
            })
            .await?;

        Ok(Response::new(CreateMaterializedViewResponse {
            status: None,
            version,
        }))
    }

    async fn drop_materialized_view(
        &self,
        request: Request<DropMaterializedViewRequest>,
    ) -> Result<Response<DropMaterializedViewResponse>, Status> {
        self.env.idle_manager().record_activity();

        let request = request.into_inner();
        let table_id = request.table_id;
        let drop_mode = DropMode::from_request_setting(request.cascade);

        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropStreamingJob {
                job_id: StreamingJobId::MaterializedView(table_id),
                drop_mode,
            })
            .await?;

        Ok(Response::new(DropMaterializedViewResponse {
            status: None,
            version,
        }))
    }

    async fn create_index(
        &self,
        request: Request<CreateIndexRequest>,
    ) -> Result<Response<CreateIndexResponse>, Status> {
        self.env.idle_manager().record_activity();

        let req = request.into_inner();
        let index = req.get_index()?.clone();
        let index_table = req.get_index_table()?.clone();
        let fragment_graph = req.get_fragment_graph()?.clone();
        let resource_type = req
            .resource_type
            .and_then(|resource_type| resource_type.resource_type)
            .unwrap_or_else(Self::default_streaming_job_resource_type);

        let stream_job = StreamingJob::Index(index, index_table);
        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateStreamingJob {
                stream_job,
                fragment_graph,
                dependencies: HashSet::new(),
                resource_type,
                if_not_exists: req.if_not_exists,
                replace_sink: None,
                refresh_interval_sec: None,
                since_timestamp_epoch: None,
            })
            .await?;

        Ok(Response::new(CreateIndexResponse {
            status: None,
            version,
        }))
    }

    async fn drop_index(
        &self,
        request: Request<DropIndexRequest>,
    ) -> Result<Response<DropIndexResponse>, Status> {
        self.env.idle_manager().record_activity();

        let request = request.into_inner();
        let index_id = request.index_id;
        let drop_mode = DropMode::from_request_setting(request.cascade);
        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropStreamingJob {
                job_id: StreamingJobId::Index(index_id),
                drop_mode,
            })
            .await?;

        Ok(Response::new(DropIndexResponse {
            status: None,
            version,
        }))
    }

    async fn create_function(
        &self,
        request: Request<CreateFunctionRequest>,
    ) -> Result<Response<CreateFunctionResponse>, Status> {
        let req = request.into_inner();
        let function = req.get_function()?.clone();

        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateFunction(function))
            .await?;

        Ok(Response::new(CreateFunctionResponse {
            status: None,
            version,
        }))
    }

    async fn drop_function(
        &self,
        request: Request<DropFunctionRequest>,
    ) -> Result<Response<DropFunctionResponse>, Status> {
        let request = request.into_inner();

        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropFunction(
                request.function_id,
                DropMode::from_request_setting(request.cascade),
            ))
            .await?;

        Ok(Response::new(DropFunctionResponse {
            status: None,
            version,
        }))
    }

    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        let request = request.into_inner();
        let job_type = request.get_job_type().unwrap_or_default();
        let dependencies = request.get_dependencies().iter().copied().collect();
        let source = request.source;
        let mview = request.materialized_view.unwrap();
        let fragment_graph = request.fragment_graph.unwrap();

        let stream_job = StreamingJob::Table(source, mview, job_type);
        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateStreamingJob {
                stream_job,
                fragment_graph,
                dependencies,
                resource_type: Self::default_streaming_job_resource_type(),
                if_not_exists: request.if_not_exists,
                refresh_interval_sec: None,
                replace_sink: None,
                since_timestamp_epoch: None,
            })
            .await?;

        Ok(Response::new(CreateTableResponse {
            status: None,
            version,
        }))
    }

    async fn drop_table(
        &self,
        request: Request<DropTableRequest>,
    ) -> Result<Response<DropTableResponse>, Status> {
        let request = request.into_inner();
        let source_id = request.source_id;
        let table_id = request.table_id;

        let drop_mode = DropMode::from_request_setting(request.cascade);
        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropStreamingJob {
                job_id: StreamingJobId::Table(source_id.map(|PbSourceId::Id(id)| id), table_id),
                drop_mode,
            })
            .await?;

        Ok(Response::new(DropTableResponse {
            status: None,
            version,
        }))
    }

    async fn create_view(
        &self,
        request: Request<CreateViewRequest>,
    ) -> Result<Response<CreateViewResponse>, Status> {
        let req = request.into_inner();
        let view = req.get_view()?.clone();
        let dependencies = req
            .get_dependencies()
            .iter()
            .copied()
            .collect::<HashSet<_>>();

        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateView(view, dependencies))
            .await?;

        Ok(Response::new(CreateViewResponse {
            status: None,
            version,
        }))
    }

    async fn drop_view(
        &self,
        request: Request<DropViewRequest>,
    ) -> Result<Response<DropViewResponse>, Status> {
        let request = request.into_inner();
        let view_id = request.get_view_id();
        let drop_mode = DropMode::from_request_setting(request.cascade);
        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropView(view_id, drop_mode))
            .await?;
        Ok(Response::new(DropViewResponse {
            status: None,
            version,
        }))
    }

    async fn risectl_list_state_tables(
        &self,
        _request: Request<RisectlListStateTablesRequest>,
    ) -> Result<Response<RisectlListStateTablesResponse>, Status> {
        let tables = self
            .metadata_manager
            .catalog_controller
            .list_all_state_tables()
            .await?;
        Ok(Response::new(RisectlListStateTablesResponse { tables }))
    }

    async fn risectl_resume_backfill(
        &self,
        request: Request<RisectlResumeBackfillRequest>,
    ) -> Result<Response<RisectlResumeBackfillResponse>, Status> {
        let request = request.into_inner();
        let target = request
            .target
            .ok_or_else(|| Status::invalid_argument("missing resume backfill target"))?;

        match target {
            risectl_resume_backfill_request::Target::JobId(job_id) => {
                let database_id = self
                    .metadata_manager
                    .catalog_controller
                    .get_object_database_id(ObjectId::new(job_id.as_raw_id()))
                    .await?;
                self.barrier_scheduler
                    .run_command(
                        database_id,
                        Command::ResumeBackfill {
                            target: ResumeBackfillTarget::Job(job_id),
                        },
                    )
                    .await?;
            }
            risectl_resume_backfill_request::Target::FragmentId(fragment_id) => {
                let mut job_ids = self
                    .metadata_manager
                    .catalog_controller
                    .get_fragment_job_id(vec![fragment_id])
                    .await?;
                let job_id = job_ids
                    .pop()
                    .ok_or_else(|| Status::invalid_argument("fragment not found"))?;
                let database_id = self
                    .metadata_manager
                    .catalog_controller
                    .get_object_database_id(ObjectId::new(job_id.as_raw_id()))
                    .await?;
                self.barrier_scheduler
                    .run_command(
                        database_id,
                        Command::ResumeBackfill {
                            target: ResumeBackfillTarget::Fragment(fragment_id),
                        },
                    )
                    .await?;
            }
        }

        Ok(Response::new(RisectlResumeBackfillResponse {}))
    }

    async fn replace_job_plan(
        &self,
        request: Request<ReplaceJobPlanRequest>,
    ) -> Result<Response<ReplaceJobPlanResponse>, Status> {
        let ReplaceJobPlan {
            fragment_graph,
            replace_job,
        } = request.into_inner().get_plan().cloned()?;

        let command = match replace_job {
            Some(replace_job_plan::ReplaceJob::ReplaceSink(replace_sink)) => {
                let replace_job_plan::ReplaceSink {
                    sink,
                    old_sink_id,
                    dependencies,
                    resource_type,
                } = replace_sink;
                DdlCommand::CreateStreamingJob {
                    stream_job: StreamingJob::Sink(sink.unwrap(), None),
                    fragment_graph: fragment_graph.unwrap(),
                    dependencies: dependencies.into_iter().collect::<HashSet<_>>(),
                    resource_type: resource_type
                        .and_then(|resource_type| resource_type.resource_type)
                        .unwrap_or(streaming_job_resource_type::ResourceType::Regular(true)),
                    if_not_exists: false,
                    refresh_interval_sec: None,
                    replace_sink: Some(old_sink_id),
                    since_timestamp_epoch: None,
                }
            }
            replace_job => {
                DdlCommand::ReplaceStreamJob(Self::extract_replace_table_info(ReplaceJobPlan {
                    fragment_graph,
                    replace_job,
                }))
            }
        };

        let version = self.ddl_controller.run_command(command).await?;

        Ok(Response::new(ReplaceJobPlanResponse {
            status: None,
            version,
        }))
    }

    async fn get_table(
        &self,
        request: Request<GetTableRequest>,
    ) -> Result<Response<GetTableResponse>, Status> {
        let req = request.into_inner();
        let table = self
            .metadata_manager
            .catalog_controller
            .get_table_by_name(&req.database_name, &req.table_name)
            .await?;

        Ok(Response::new(GetTableResponse { table }))
    }

    async fn alter_name(
        &self,
        request: Request<AlterNameRequest>,
    ) -> Result<Response<AlterNameResponse>, Status> {
        let AlterNameRequest { object, new_name } = request.into_inner();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::AlterName(object.unwrap(), new_name))
            .await?;
        Ok(Response::new(AlterNameResponse {
            status: None,
            version,
        }))
    }

    /// Only support add column for now.
    async fn alter_source(
        &self,
        request: Request<AlterSourceRequest>,
    ) -> Result<Response<AlterSourceResponse>, Status> {
        let AlterSourceRequest { source } = request.into_inner();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::AlterNonSharedSource(source.unwrap()))
            .await?;
        Ok(Response::new(AlterSourceResponse {
            status: None,
            version,
        }))
    }

    async fn alter_owner(
        &self,
        request: Request<AlterOwnerRequest>,
    ) -> Result<Response<AlterOwnerResponse>, Status> {
        let AlterOwnerRequest { object, owner_id } = request.into_inner();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::AlterObjectOwner(object.unwrap(), owner_id as _))
            .await?;
        Ok(Response::new(AlterOwnerResponse {
            status: None,
            version,
        }))
    }

    async fn reassign_owned(
        &self,
        request: Request<ReassignOwnedRequest>,
    ) -> Result<Response<ReassignOwnedResponse>, Status> {
        let ReassignOwnedRequest {
            old_owner_ids,
            new_owner_id,
            database_id,
        } = request.into_inner();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::ReassignOwned {
                old_owner_ids,
                new_owner_id,
                database_id,
            })
            .await?;
        Ok(Response::new(ReassignOwnedResponse {
            status: None,
            version,
        }))
    }

    async fn drop_owned(
        &self,
        request: Request<DropOwnedRequest>,
    ) -> Result<Response<DropOwnedResponse>, Status> {
        let DropOwnedRequest {
            owner_ids,
            database_id,
            cascade,
        } = request.into_inner();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropOwned {
                owner_ids,
                database_id,
                cascade,
            })
            .await?;
        Ok(Response::new(DropOwnedResponse {
            status: None,
            version,
        }))
    }

    async fn alter_subscription_retention(
        &self,
        request: Request<AlterSubscriptionRetentionRequest>,
    ) -> Result<Response<AlterSubscriptionRetentionResponse>, Status> {
        let AlterSubscriptionRetentionRequest {
            subscription_id,
            retention_seconds,
            definition,
        } = request.into_inner();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::AlterSubscriptionRetention {
                subscription_id,
                retention_seconds,
                definition,
            })
            .await?;
        Ok(Response::new(AlterSubscriptionRetentionResponse {
            status: None,
            version,
        }))
    }

    async fn alter_set_schema(
        &self,
        request: Request<AlterSetSchemaRequest>,
    ) -> Result<Response<AlterSetSchemaResponse>, Status> {
        let AlterSetSchemaRequest {
            object,
            new_schema_id,
        } = request.into_inner();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::AlterSetSchema(object.unwrap(), new_schema_id))
            .await?;
        Ok(Response::new(AlterSetSchemaResponse {
            status: None,
            version,
        }))
    }

    async fn get_ddl_progress(
        &self,
        _request: Request<GetDdlProgressRequest>,
    ) -> Result<Response<GetDdlProgressResponse>, Status> {
        Ok(Response::new(GetDdlProgressResponse {
            ddl_progress: self.ddl_controller.get_ddl_progress().await?,
        }))
    }

    async fn create_connection(
        &self,
        request: Request<CreateConnectionRequest>,
    ) -> Result<Response<CreateConnectionResponse>, Status> {
        let req = request.into_inner();
        if req.payload.is_none() {
            return Err(Status::invalid_argument("request is empty"));
        }

        match req.payload.unwrap() {
            #[expect(deprecated)]
            create_connection_request::Payload::PrivateLink(_) => {
                panic!("Private Link Connection has been deprecated")
            }
            create_connection_request::Payload::ConnectionParams(params) => {
                let pb_connection = Connection {
                    id: 0.into(),
                    schema_id: req.schema_id,
                    database_id: req.database_id,
                    name: req.name,
                    info: Some(ConnectionInfo::ConnectionParams(params)),
                    owner: req.owner_id,
                };
                let version = self
                    .ddl_controller
                    .run_command(DdlCommand::CreateConnection(pb_connection))
                    .await?;
                Ok(Response::new(CreateConnectionResponse { version }))
            }
        }
    }

    async fn list_connections(
        &self,
        _request: Request<ListConnectionsRequest>,
    ) -> Result<Response<ListConnectionsResponse>, Status> {
        let conns = self
            .metadata_manager
            .catalog_controller
            .list_connections()
            .await?;

        Ok(Response::new(ListConnectionsResponse {
            connections: conns,
        }))
    }

    async fn drop_connection(
        &self,
        request: Request<DropConnectionRequest>,
    ) -> Result<Response<DropConnectionResponse>, Status> {
        let req = request.into_inner();
        let drop_mode = DropMode::from_request_setting(req.cascade);

        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropConnection(req.connection_id, drop_mode))
            .await?;

        Ok(Response::new(DropConnectionResponse {
            status: None,
            version,
        }))
    }

    async fn comment_on(
        &self,
        request: Request<CommentOnRequest>,
    ) -> Result<Response<CommentOnResponse>, Status> {
        let req = request.into_inner();
        let comment = req.get_comment()?.clone();

        let version = self
            .ddl_controller
            .run_command(DdlCommand::CommentOn(Comment {
                table_id: comment.table_id,
                schema_id: comment.schema_id,
                database_id: comment.database_id,
                column_index: comment.column_index,
                description: comment.description,
            }))
            .await?;

        Ok(Response::new(CommentOnResponse {
            status: None,
            version,
        }))
    }

    async fn get_tables(
        &self,
        request: Request<GetTablesRequest>,
    ) -> Result<Response<GetTablesResponse>, Status> {
        let GetTablesRequest {
            table_ids,
            include_dropped_tables,
        } = request.into_inner();
        let ret = self
            .metadata_manager
            .catalog_controller
            .get_table_by_ids(table_ids, include_dropped_tables)
            .await?;

        let mut tables = HashMap::default();
        for table in ret {
            tables.insert(table.id, table);
        }
        Ok(Response::new(GetTablesResponse { tables }))
    }

    async fn wait(&self, request: Request<WaitRequest>) -> Result<Response<WaitResponse>, Status> {
        let req = request.into_inner();
        let version = self.ddl_controller.wait(req.job_id).await?;
        Ok(Response::new(WaitResponse {
            version: Some(version),
        }))
    }

    async fn alter_cdc_table_backfill_parallelism(
        &self,
        request: Request<AlterCdcTableBackfillParallelismRequest>,
    ) -> Result<Response<AlterCdcTableBackfillParallelismResponse>, Status> {
        let req = request.into_inner();
        let job_id = req.get_table_id();
        let parallelism = *req.get_parallelism()?;

        let table_parallelism = ModelTableParallelism::from(parallelism);
        let streaming_parallelism = match table_parallelism {
            ModelTableParallelism::Fixed(n) => StreamingParallelism::Fixed(n),
            _ => bail_invalid_parameter!(
                "CDC table backfill parallelism must be set to a fixed value"
            ),
        };

        self.ddl_controller
            .reschedule_cdc_table_backfill(
                job_id,
                ReschedulePolicy::Parallelism(ParallelismPolicy {
                    parallelism: streaming_parallelism,
                    adaptive_parallelism_strategy: None,
                }),
            )
            .await?;
        Ok(Response::new(AlterCdcTableBackfillParallelismResponse {}))
    }

    async fn alter_parallelism(
        &self,
        request: Request<AlterParallelismRequest>,
    ) -> Result<Response<AlterParallelismResponse>, Status> {
        let req = request.into_inner();

        let job_id = req.get_table_id();
        let parallelism = *req.get_parallelism()?;
        let deferred = req.get_deferred();

        let adaptive_parallelism_strategy = req.adaptive_parallelism_strategy;
        let (parallelism, adaptive_parallelism_strategy) = match parallelism.get_parallelism()? {
            Parallelism::Fixed(FixedParallelism { parallelism }) => {
                (StreamingParallelism::Fixed(*parallelism as _), None)
            }
            Parallelism::Auto(_) | Parallelism::Adaptive(_) => (
                StreamingParallelism::Adaptive,
                Some(adaptive_parallelism_strategy.unwrap_or_else(|| "AUTO".to_owned())),
            ),
            Parallelism::Custom(_) => (
                StreamingParallelism::Adaptive,
                Some(adaptive_parallelism_strategy.ok_or_else(|| {
                    Status::invalid_argument(
                        "adaptive_parallelism_strategy is required for custom parallelism",
                    )
                })?),
            ),
        };

        if let Some(strategy) = adaptive_parallelism_strategy.as_deref() {
            parse_strategy(strategy).map_err(|e| {
                Status::invalid_argument(format!(
                    "invalid adaptive parallelism strategy: {}",
                    e.as_report()
                ))
            })?;
        };

        self.ddl_controller
            .reschedule_streaming_job(
                job_id,
                ReschedulePolicy::Parallelism(ParallelismPolicy {
                    parallelism,
                    adaptive_parallelism_strategy,
                }),
                deferred,
            )
            .await?;

        Ok(Response::new(AlterParallelismResponse {}))
    }

    async fn alter_backfill_parallelism(
        &self,
        request: Request<AlterBackfillParallelismRequest>,
    ) -> Result<Response<AlterBackfillParallelismResponse>, Status> {
        let req = request.into_inner();

        let job_id = req.get_table_id();
        let deferred = req.get_deferred();
        let adaptive_parallelism_strategy = req.adaptive_parallelism_strategy;

        let parallelism = match req.parallelism {
            None => None,
            Some(parallelism) => {
                let (parallelism, adaptive_parallelism_strategy) = match parallelism
                    .get_parallelism()?
                {
                    Parallelism::Fixed(FixedParallelism { parallelism }) => {
                        (StreamingParallelism::Fixed(*parallelism as _), None)
                    }
                    Parallelism::Auto(_) | Parallelism::Adaptive(_) => (
                        StreamingParallelism::Adaptive,
                        Some(adaptive_parallelism_strategy.unwrap_or_else(|| "AUTO".to_owned())),
                    ),
                    Parallelism::Custom(_) => (
                        StreamingParallelism::Adaptive,
                        Some(adaptive_parallelism_strategy.ok_or_else(|| {
                            Status::invalid_argument(
                                "adaptive_parallelism_strategy is required for custom parallelism",
                            )
                        })?),
                    ),
                };

                if let Some(strategy) = adaptive_parallelism_strategy.as_deref() {
                    parse_strategy(strategy).map_err(|e| {
                        Status::invalid_argument(format!(
                            "invalid adaptive parallelism strategy: {}",
                            e.as_report()
                        ))
                    })?;
                };

                Some(ParallelismPolicy {
                    parallelism,
                    adaptive_parallelism_strategy,
                })
            }
        };

        self.ddl_controller
            .reschedule_streaming_job_backfill_parallelism(job_id, parallelism, deferred)
            .await?;

        Ok(Response::new(AlterBackfillParallelismResponse {}))
    }

    async fn alter_fragment_parallelism(
        &self,
        request: Request<AlterFragmentParallelismRequest>,
    ) -> Result<Response<AlterFragmentParallelismResponse>, Status> {
        let req = request.into_inner();

        let fragment_ids = req.fragment_ids;
        if fragment_ids.is_empty() {
            return Err(Status::invalid_argument(
                "at least one fragment id must be provided",
            ));
        }

        let parallelism = match req.parallelism {
            Some(parallelism) => {
                let streaming_parallelism = match parallelism.get_parallelism()? {
                    Parallelism::Fixed(FixedParallelism { parallelism }) => {
                        StreamingParallelism::Fixed(*parallelism as _)
                    }
                    Parallelism::Auto(_) | Parallelism::Adaptive(_) => {
                        StreamingParallelism::Adaptive
                    }
                    _ => bail_unavailable!(),
                };
                Some(streaming_parallelism)
            }
            None => None,
        };

        let fragment_targets = fragment_ids
            .into_iter()
            .map(|fragment_id| (fragment_id, parallelism.clone()))
            .collect();

        self.ddl_controller
            .reschedule_fragments(fragment_targets)
            .await?;

        Ok(Response::new(AlterFragmentParallelismResponse {}))
    }

    async fn alter_streaming_job_config(
        &self,
        request: Request<AlterStreamingJobConfigRequest>,
    ) -> Result<Response<AlterStreamingJobConfigResponse>, Status> {
        let AlterStreamingJobConfigRequest {
            job_id,
            entries_to_add,
            keys_to_remove,
        } = request.into_inner();

        self.ddl_controller
            .run_command(DdlCommand::AlterStreamingJobConfig(
                job_id,
                entries_to_add,
                keys_to_remove,
            ))
            .await?;

        Ok(Response::new(AlterStreamingJobConfigResponse {}))
    }

    /// Auto schema change for cdc sources,
    /// called by the source parser when a schema change is detected.
    async fn auto_schema_change(
        &self,
        request: Request<AutoSchemaChangeRequest>,
    ) -> Result<Response<AutoSchemaChangeResponse>, Status> {
        let req = request.into_inner();

        // randomly select a frontend worker to get the replace table plan
        let workers = self
            .metadata_manager
            .list_worker_node(Some(WorkerType::Frontend), Some(State::Running))
            .await?;
        let worker = workers
            .choose(&mut thread_rng())
            .ok_or_else(|| MetaError::from(anyhow!("no frontend worker available")))?;

        let client = self
            .env
            .frontend_client_pool()
            .get(worker)
            .await
            .map_err(MetaError::from)?;

        let Some(schema_change) = req.schema_change else {
            return Err(Status::invalid_argument(
                "schema change message is required",
            ));
        };

        for table_change in schema_change.table_changes {
            for c in &table_change.columns {
                let c = ColumnCatalog::from(c.clone());

                let invalid_col_type = |column_type: &str, c: &ColumnCatalog| {
                    tracing::warn!(target: "auto_schema_change",
                      cdc_table_id = table_change.cdc_table_id,
                    upstraem_ddl = table_change.upstream_ddl,
                        "invalid column type from cdc table change");
                    Err(Status::invalid_argument(format!(
                        "invalid column type: {} from cdc table change, column: {:?}",
                        column_type, c
                    )))
                };
                if c.is_generated() {
                    return invalid_col_type("generated column", &c);
                }
                if c.is_rw_sys_column() {
                    return invalid_col_type("rw system column", &c);
                }
                if c.is_hidden {
                    return invalid_col_type("hidden column", &c);
                }
            }

            // get the table catalog corresponding to the cdc table
            let tables: Vec<Table> = self
                .metadata_manager
                .get_table_catalog_by_cdc_table_id(&table_change.cdc_table_id)
                .await?;

            for table in tables {
                // Since we only support `ADD` and `DROP` column, we check whether the new columns and the original columns
                // is a subset of the other.
                let original_columns_by_name: HashMap<String, ColumnCatalog> = table
                    .columns
                    .iter()
                    .filter_map(|col| {
                        let col = ColumnCatalog::from(col.clone());
                        cdc_auto_schema_change_comparable_column(&col).map(|(name, _)| (name, col))
                    })
                    .collect();

                let original_column_types: HashMap<String, DataType> = original_columns_by_name
                    .iter()
                    .map(|(name, col)| (name.clone(), col.data_type().clone()))
                    .collect();

                let original_column_names: HashSet<String> =
                    HashSet::from_iter(original_column_types.keys().cloned());

                let cdc_table_type =
                    PbCdcTableType::try_from(table.cdc_table_type.unwrap_or_default())
                        .unwrap_or(PbCdcTableType::Unspecified);
                let table_change = normalize_cdc_auto_schema_change_column_names(
                    table_change.clone(),
                    cdc_table_type,
                    &original_columns_by_name,
                );

                let collect_new_columns = |table_change: &TableSchemaChange| {
                    let mut new_columns: HashSet<(String, DataType)> =
                        HashSet::from_iter(table_change.columns.iter().filter_map(|col| {
                            let col = ColumnCatalog::from(col.clone());
                            cdc_auto_schema_change_comparable_column(&col)
                        }));

                    // For subset/superset check, we need to add visible connector additional columns defined by INCLUDE in the original table to new_columns.
                    // This includes both _rw columns and user-defined INCLUDE columns (e.g., INCLUDE TIMESTAMP AS xxx).
                    for col in &table.columns {
                        let col = ColumnCatalog::from(col.clone());
                        if col.is_connector_additional_column()
                            && !col.is_hidden()
                            && !col.is_generated()
                        {
                            new_columns
                                .insert((col.column_desc.name.clone(), col.data_type().clone()));
                        }
                    }

                    new_columns
                };

                let new_columns = collect_new_columns(&table_change);

                let new_column_names: HashSet<String> =
                    HashSet::from_iter(new_columns.iter().map(|(name, _)| name.clone()));
                let is_add_or_drop_by_name = original_column_names.is_subset(&new_column_names)
                    || original_column_names.is_superset(&new_column_names);

                // Debezium schema change events carry the full table schema. Preserve existing
                // validator-compatible RW types before both validation and replacement planning.
                let table_change =
                    if original_column_names != new_column_names && is_add_or_drop_by_name {
                        normalize_cdc_auto_schema_change_existing_column_types(
                            table_change,
                            cdc_table_type,
                            &original_columns_by_name,
                        )
                    } else {
                        // Keep the schema change without type normalization for non-add/drop-only
                        // cases, such as a mixed add-and-drop change. Existing validation below
                        // should reject unsupported schema changes without masking them.
                        table_change
                    };

                let original_columns: HashSet<(String, DataType)> = HashSet::from_iter(
                    original_column_types
                        .iter()
                        .map(|(name, data_type)| (name.clone(), data_type.clone())),
                );

                let new_columns = collect_new_columns(&table_change);

                if !(original_columns.is_subset(&new_columns)
                    || original_columns.is_superset(&new_columns))
                {
                    tracing::warn!(target: "auto_schema_change",
                                    table_id = %table.id,
                                    cdc_table_id = table.cdc_table_id,
                                    upstraem_ddl = table_change.upstream_ddl,
                                    original_columns = ?original_columns,
                                    new_columns = ?new_columns,
                                    "New columns should be a subset or superset of the original columns (including hidden columns), since only `ADD COLUMN` and `DROP COLUMN` is supported");

                    let fail_info = "New columns should be a subset or superset of the original columns (including hidden columns), since only `ADD COLUMN` and `DROP COLUMN` is supported".to_owned();
                    add_auto_schema_change_fail_event_log(
                        &self.meta_metrics,
                        table.id,
                        table.name.clone(),
                        table_change.cdc_table_id.clone(),
                        table_change.upstream_ddl.clone(),
                        &self.env.event_log_manager_ref(),
                        fail_info,
                    );

                    return Err(Status::invalid_argument(
                        "New columns should be a subset or superset of the original columns (including hidden columns)",
                    ));
                }
                // skip the schema change if there is no change to original columns
                if original_columns == new_columns {
                    tracing::warn!(target: "auto_schema_change",
                                   table_id = %table.id,
                                   cdc_table_id = table.cdc_table_id,
                                   upstraem_ddl = table_change.upstream_ddl,
                                    original_columns = ?original_columns,
                                    new_columns = ?new_columns,
                                   "No change to columns, skipping the schema change");
                    continue;
                }

                let latency_timer = self
                    .meta_metrics
                    .auto_schema_change_latency
                    .with_guarded_label_values(&[&table.id.to_string(), &table.name])
                    .start_timer();
                // send a request to the frontend to get the ReplaceJobPlan
                // will retry with exponential backoff if the request fails
                let resp = client
                    .get_table_replace_plan(GetTableReplacePlanRequest {
                        database_id: table.database_id,
                        table_id: table.id,
                        cdc_table_change: Some(table_change.clone()),
                    })
                    .await;

                match resp {
                    Ok(resp) => {
                        let resp = resp.into_inner();
                        if let Some(plan) = resp.replace_plan {
                            let plan = Self::extract_replace_table_info(plan);
                            plan.streaming_job.table().inspect(|t| {
                                tracing::info!(
                                    target: "auto_schema_change",
                                    table_id = %t.id,
                                    cdc_table_id = t.cdc_table_id,
                                    upstraem_ddl = table_change.upstream_ddl,
                                    "Start the replace config change")
                            });
                            // start the schema change procedure
                            let replace_res = self
                                .ddl_controller
                                .run_command(DdlCommand::ReplaceStreamJob(plan))
                                .await;

                            match replace_res {
                                Ok(_) => {
                                    tracing::info!(
                                        target: "auto_schema_change",
                                        table_id = %table.id,
                                        cdc_table_id = table.cdc_table_id,
                                        "Table replaced success");

                                    self.meta_metrics
                                        .auto_schema_change_success_cnt
                                        .with_guarded_label_values(&[
                                            &table.id.to_string(),
                                            &table.name,
                                        ])
                                        .inc();
                                    latency_timer.observe_duration();
                                }
                                Err(e) => {
                                    tracing::error!(
                                        target: "auto_schema_change",
                                        error = %e.as_report(),
                                        table_id = %table.id,
                                        cdc_table_id = table.cdc_table_id,
                                        upstraem_ddl = table_change.upstream_ddl,
                                        "failed to replace the table",
                                    );
                                    let fail_info =
                                        format!("failed to replace the table: {}", e.as_report());
                                    add_auto_schema_change_fail_event_log(
                                        &self.meta_metrics,
                                        table.id,
                                        table.name.clone(),
                                        table_change.cdc_table_id.clone(),
                                        table_change.upstream_ddl.clone(),
                                        &self.env.event_log_manager_ref(),
                                        fail_info,
                                    );
                                }
                            };
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            target: "auto_schema_change",
                            error = %e.as_report(),
                            table_id = %table.id,
                            cdc_table_id = table.cdc_table_id,
                            "failed to get replace table plan",
                        );
                        let fail_info =
                            format!("failed to get replace table plan: {}", e.as_report());
                        add_auto_schema_change_fail_event_log(
                            &self.meta_metrics,
                            table.id,
                            table.name.clone(),
                            table_change.cdc_table_id.clone(),
                            table_change.upstream_ddl.clone(),
                            &self.env.event_log_manager_ref(),
                            fail_info,
                        );
                    }
                };
            }
        }

        Ok(Response::new(AutoSchemaChangeResponse {}))
    }

    async fn wait_iceberg_pk_index_sink_epoch(
        &self,
        request: Request<WaitIcebergPkIndexSinkEpochRequest>,
    ) -> Result<Response<WaitIcebergPkIndexSinkEpochResponse>, Status> {
        let req = request.into_inner();
        let snapshot_id = self
            .iceberg_pk_index_sink_manager
            .wait_epoch(req.sink_id, req.epoch)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "Failed to wait for pk-index sink epoch: {}",
                    e.as_report()
                ))
            })?;
        Ok(Response::new(WaitIcebergPkIndexSinkEpochResponse {
            snapshot_id,
        }))
    }

    async fn alter_swap_rename(
        &self,
        request: Request<AlterSwapRenameRequest>,
    ) -> Result<Response<AlterSwapRenameResponse>, Status> {
        let req = request.into_inner();

        let version = self
            .ddl_controller
            .run_command(DdlCommand::AlterSwapRename(req.object.unwrap()))
            .await?;

        Ok(Response::new(AlterSwapRenameResponse {
            status: None,
            version,
        }))
    }

    async fn alter_resource_group(
        &self,
        request: Request<AlterResourceGroupRequest>,
    ) -> Result<Response<AlterResourceGroupResponse>, Status> {
        let req = request.into_inner();

        let job_id = req.get_job_id();
        let deferred = req.get_deferred();
        let resource_group = req.resource_group;

        self.ddl_controller
            .reschedule_streaming_job(
                job_id,
                ReschedulePolicy::ResourceGroup(ResourceGroupPolicy { resource_group }),
                deferred,
            )
            .await?;

        Ok(Response::new(AlterResourceGroupResponse {}))
    }

    async fn alter_database_resource_group(
        &self,
        request: Request<AlterDatabaseResourceGroupRequest>,
    ) -> Result<Response<AlterDatabaseResourceGroupResponse>, Status> {
        let req = request.into_inner();

        let version = self
            .ddl_controller
            .run_command(DdlCommand::AlterDatabaseResourceGroup(
                req.database_id,
                req.resource_group,
                req.deferred,
            ))
            .await?;

        Ok(Response::new(AlterDatabaseResourceGroupResponse {
            status: None,
            version,
        }))
    }

    async fn alter_database_param(
        &self,
        request: Request<AlterDatabaseParamRequest>,
    ) -> Result<Response<AlterDatabaseParamResponse>, Status> {
        let req = request.into_inner();
        let database_id = req.database_id;

        let param = match req.param.unwrap() {
            alter_database_param_request::Param::BarrierIntervalMs(value) => {
                AlterDatabaseParam::BarrierIntervalMs(value.value)
            }
            alter_database_param_request::Param::CheckpointFrequency(value) => {
                AlterDatabaseParam::CheckpointFrequency(value.value)
            }
        };
        let version = self
            .ddl_controller
            .run_command(DdlCommand::AlterDatabaseParam(database_id, param))
            .await?;

        return Ok(Response::new(AlterDatabaseParamResponse {
            status: None,
            version,
        }));
    }

    async fn compact_iceberg_table(
        &self,
        request: Request<CompactIcebergTableRequest>,
    ) -> Result<Response<CompactIcebergTableResponse>, Status> {
        let req = request.into_inner();
        let sink_id = req.sink_id;

        // Trigger manual compaction directly using the sink ID
        let task_id = self
            .iceberg_compaction_manager
            .trigger_manual_compaction(sink_id)
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to trigger compaction: {}", e.as_report()))
            })?;

        Ok(Response::new(CompactIcebergTableResponse {
            status: None,
            task_id,
        }))
    }

    async fn expire_iceberg_table_snapshots(
        &self,
        request: Request<ExpireIcebergTableSnapshotsRequest>,
    ) -> Result<Response<ExpireIcebergTableSnapshotsResponse>, Status> {
        let req = request.into_inner();
        let sink_id = req.sink_id;

        // Trigger manual snapshot expiration directly using the sink ID
        self.iceberg_compaction_manager
            .check_and_expire_snapshots(sink_id)
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to expire snapshots: {}", e.as_report()))
            })?;

        Ok(Response::new(ExpireIcebergTableSnapshotsResponse {
            status: None,
        }))
    }

    async fn rewrite_iceberg_table_manifests(
        &self,
        request: Request<RewriteIcebergTableManifestsRequest>,
    ) -> Result<Response<RewriteIcebergTableManifestsResponse>, Status> {
        let req = request.into_inner();
        let sink_id = req.sink_id;

        self.iceberg_compaction_manager
            .check_and_rewrite_manifests(sink_id)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "Failed to rewrite manifests for sink {}: {}",
                    sink_id,
                    e.as_report()
                ))
            })?;

        Ok(Response::new(RewriteIcebergTableManifestsResponse {
            status: None,
        }))
    }

    async fn create_iceberg_table(
        &self,
        request: Request<CreateIcebergTableRequest>,
    ) -> Result<Response<CreateIcebergTableResponse>, Status> {
        let req = request.into_inner();
        let CreateIcebergTableRequest {
            table_info,
            sink_info,
            iceberg_source,
            if_not_exists,
        } = req;

        // 1. create table job
        let PbTableJobInfo {
            source,
            table,
            fragment_graph,
            job_type,
        } = table_info.unwrap();
        let mut table = table.unwrap();
        let mut fragment_graph = fragment_graph.unwrap();
        let database_id = table.get_database_id();
        let schema_id = table.get_schema_id();
        let table_name = table.get_name().to_owned();

        // Mark table as background creation, so that it won't block sink creation.
        table.create_type = PbCreateType::Background as _;

        // Set the source rate limit to 0 and reset it back after the iceberg sink is backfilling.
        let source_rate_limit = if let Some(source) = &source {
            for fragment in fragment_graph.fragments.values_mut() {
                stream_graph_visitor::visit_fragment_mut(fragment, |node| {
                    if let NodeBody::Source(source_node) = node
                        && let Some(inner) = &mut source_node.source_inner
                    {
                        inner.rate_limit = Some(0);
                    }
                });
            }
            Some(source.rate_limit)
        } else {
            None
        };

        let stream_job =
            StreamingJob::Table(source, table, PbTableJobType::try_from(job_type).unwrap());
        let _ = self
            .ddl_controller
            .run_command(DdlCommand::CreateStreamingJob {
                stream_job,
                fragment_graph,
                dependencies: HashSet::new(),
                resource_type: Self::default_streaming_job_resource_type(),
                if_not_exists,
                refresh_interval_sec: None,
                replace_sink: None,
                since_timestamp_epoch: None,
            })
            .await?;

        let table_catalog = self
            .metadata_manager
            .catalog_controller
            .get_table_catalog_by_name(database_id, schema_id, &table_name)
            .await?
            .ok_or(Status::not_found("Internal error: table not found"))?;

        // 2. create iceberg sink job
        let PbSinkJobInfo {
            sink,
            fragment_graph,
        } = sink_info.unwrap();
        let mut sink = sink.unwrap();

        // Mark sink as background creation, so that it won't block source creation.
        sink.create_type = PbCreateType::Background as _;

        let enable_pk_index = sink
            .properties
            .get(ENABLE_PK_INDEX)
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));
        // The internal iceberg sink is planned before the table catalog exists, so this field
        // still carries a placeholder table id when the request reaches meta.
        sink.auto_refresh_schema_from_table = if enable_pk_index {
            None
        } else {
            Some(table_catalog.id)
        };

        let mut fragment_graph = fragment_graph.unwrap();

        assert_eq!(fragment_graph.dependent_table_ids.len(), 1);
        assert!(
            risingwave_common::catalog::TableId::from(fragment_graph.dependent_table_ids[0])
                .is_placeholder()
        );
        fragment_graph.dependent_table_ids[0] = table_catalog.id;
        for fragment in fragment_graph.fragments.values_mut() {
            stream_graph_visitor::visit_fragment_mut(fragment, |node| match node {
                NodeBody::StreamScan(scan) => {
                    scan.table_id = table_catalog.id;
                    if let Some(table_desc) = &mut scan.table_desc {
                        assert!(
                            risingwave_common::catalog::TableId::from(table_desc.table_id)
                                .is_placeholder()
                        );
                        table_desc.table_id = table_catalog.id;
                        table_desc.maybe_vnode_count = table_catalog.maybe_vnode_count;
                    }
                    if let Some(table) = &mut scan.arrangement_table {
                        assert!(
                            risingwave_common::catalog::TableId::from(table.id).is_placeholder()
                        );
                        *table = table_catalog.clone();
                    }
                }
                NodeBody::BatchPlan(plan) => {
                    if let Some(table_desc) = &mut plan.table_desc {
                        assert!(
                            risingwave_common::catalog::TableId::from(table_desc.table_id)
                                .is_placeholder()
                        );
                        table_desc.table_id = table_catalog.id;
                        table_desc.maybe_vnode_count = table_catalog.maybe_vnode_count;
                    }
                }
                _ => {}
            });
        }

        let table_id = table_catalog.id;
        let dependencies = HashSet::from_iter([table_id.into(), schema_id.into()]);
        let stream_job = StreamingJob::Sink(sink, Some(table_id));
        let res = self
            .ddl_controller
            .run_command(DdlCommand::CreateStreamingJob {
                stream_job,
                fragment_graph,
                dependencies,
                resource_type: Self::default_streaming_job_resource_type(),
                if_not_exists,
                refresh_interval_sec: None,
                replace_sink: None,
                since_timestamp_epoch: None,
            })
            .await;

        if res.is_err() {
            let _ = self
                .ddl_controller
                .run_command(DdlCommand::DropStreamingJob {
                    job_id: StreamingJobId::Table(None, table_id),
                    drop_mode: DropMode::Cascade,
                })
                .await
                .inspect_err(|err| {
                    tracing::error!(error = %err.as_report(),
                        "Failed to clean up table after iceberg sink creation failure",
                    );
                });
            res?;
        }

        // 3. reset source rate limit back to normal after sink creation
        if let Some(source_rate_limit) = source_rate_limit
            && source_rate_limit != Some(0)
        {
            let OptionalAssociatedSourceId::AssociatedSourceId(source_id) =
                table_catalog.optional_associated_source_id.unwrap();
            let fragment_nodes = self
                .metadata_manager
                .update_source_rate_limit_by_source_id(source_id, source_rate_limit)
                .await?;
            let throttle_config = ThrottleConfig {
                throttle_type: risingwave_pb::common::ThrottleType::Source.into(),
                rate_limit: source_rate_limit,
            };
            let config = fragment_nodes
                .into_iter()
                .map(|(fragment_id, stream_node)| (fragment_id, (throttle_config, stream_node)))
                .collect();
            let _ = self
                .barrier_scheduler
                .run_command(database_id, Command::Throttle { config })
                .await?;
        }

        // 4. create iceberg source
        let iceberg_source = iceberg_source.unwrap();
        let res = self
            .ddl_controller
            .run_command(DdlCommand::CreateNonSharedSource(
                iceberg_source,
                Some(table_id),
            ))
            .await;
        if res.is_err() {
            let _ = self
                .ddl_controller
                .run_command(DdlCommand::DropStreamingJob {
                    job_id: StreamingJobId::Table(None, table_id),
                    drop_mode: DropMode::Cascade,
                })
                .await
                .inspect_err(|err| {
                    tracing::error!(
                        error = %err.as_report(),
                        "Failed to clean up table after iceberg source creation failure",
                    );
                });
        }

        Ok(Response::new(CreateIcebergTableResponse {
            status: None,
            version: res?,
        }))
    }
}

fn add_auto_schema_change_fail_event_log(
    meta_metrics: &Arc<MetaMetrics>,
    table_id: TableId,
    table_name: String,
    cdc_table_id: String,
    upstream_ddl: String,
    event_log_manager: &EventLogManagerRef,
    fail_info: String,
) {
    meta_metrics
        .auto_schema_change_failure_cnt
        .with_guarded_label_values(&[&table_id.to_string(), &table_name])
        .inc();
    let event = event_log::EventAutoSchemaChangeFail {
        table_id,
        table_name,
        cdc_table_id,
        upstream_ddl,
        fail_info,
    };
    event_log_manager.add_event_logs(vec![event_log::Event::AutoSchemaChangeFail(event)]);
}

fn cdc_auto_schema_change_comparable_column(column: &ColumnCatalog) -> Option<(String, DataType)> {
    if column.is_generated() || column.is_hidden() {
        None
    } else {
        Some((column.column_desc.name.clone(), column.data_type().clone()))
    }
}

fn normalize_cdc_auto_schema_change_column_names(
    mut table_change: TableSchemaChange,
    cdc_table_type: PbCdcTableType,
    original_columns_by_name: &HashMap<String, ColumnCatalog>,
) -> TableSchemaChange {
    if cdc_table_type != PbCdcTableType::Mysql {
        return table_change;
    }

    // MySQL column names are case-insensitive, while RisingWave catalog names may preserve
    // explicitly quoted spelling. Reuse that spelling for existing columns and apply the same
    // lowercase convention as MySQL snapshot discovery only to genuinely new columns.
    let mut original_names_by_lowercase = HashMap::<String, Option<String>>::new();
    for original_name in original_columns_by_name.keys() {
        original_names_by_lowercase
            .entry(original_name.to_lowercase())
            .and_modify(|name| *name = None)
            .or_insert_with(|| Some(original_name.clone()));
    }

    for column in &mut table_change.columns {
        let mut column_catalog = ColumnCatalog::from(column.clone());
        if column_catalog.is_generated() || column_catalog.is_hidden() {
            continue;
        }

        let incoming_name = &column_catalog.column_desc.name;
        let lowercase_name = incoming_name.to_lowercase();
        let normalized_name = if original_columns_by_name.contains_key(incoming_name) {
            incoming_name.clone()
        } else {
            original_names_by_lowercase
                .get(&lowercase_name)
                .and_then(|name| name.clone())
                .unwrap_or(lowercase_name)
        };

        column_catalog.column_desc.name = normalized_name;
        *column = column_catalog.to_protobuf();
    }

    table_change
}

fn normalize_cdc_auto_schema_change_existing_column_types(
    mut table_change: TableSchemaChange,
    cdc_table_type: PbCdcTableType,
    original_columns_by_name: &HashMap<String, ColumnCatalog>,
) -> TableSchemaChange {
    for column in &mut table_change.columns {
        let mut column_catalog = ColumnCatalog::from(column.clone());
        if column_catalog.is_generated() || column_catalog.is_hidden() {
            continue;
        }

        if let Some(original_column) =
            original_columns_by_name.get(&column_catalog.column_desc.name)
            && cdc_auto_schema_change_existing_type_compatible(
                cdc_table_type,
                original_column.data_type(),
                column_catalog.data_type(),
            )
        {
            column_catalog.column_desc.data_type = original_column.data_type().clone();
            column_catalog.column_desc.generated_or_default_column = original_column
                .column_desc
                .generated_or_default_column
                .clone();
            *column = column_catalog.to_protobuf();
        }
    }

    table_change
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::types::ScalarImpl;
    use risingwave_pb::ddl_service::table_schema_change::TableChangeType;
    use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;

    use super::*;

    fn pb_column(name: &str, data_type: DataType) -> risingwave_pb::plan_common::ColumnCatalog {
        ColumnCatalog::visible(ColumnDesc::named(name, ColumnId::placeholder(), data_type))
            .to_protobuf()
    }

    fn pb_column_with_default_value(
        name: &str,
        data_type: DataType,
        snapshot_value: ScalarImpl,
    ) -> risingwave_pb::plan_common::ColumnCatalog {
        ColumnCatalog::visible(ColumnDesc::named_with_default_value(
            name,
            ColumnId::placeholder(),
            data_type,
            Some(snapshot_value),
        ))
        .to_protobuf()
    }

    fn pb_table_change(
        columns: Vec<risingwave_pb::plan_common::ColumnCatalog>,
    ) -> TableSchemaChange {
        TableSchemaChange {
            change_type: TableChangeType::Alter as _,
            cdc_table_id: "1.db.t".to_owned(),
            columns,
            upstream_ddl: "ALTER TABLE t ADD COLUMN note VARCHAR(255)".to_owned(),
        }
    }

    #[test]
    fn test_cdc_auto_schema_change_normalizes_compatible_existing_column_types() {
        let original_columns_by_name = HashMap::from([
            (
                "id".to_owned(),
                ColumnCatalog::visible(ColumnDesc::named(
                    "id",
                    ColumnId::placeholder(),
                    DataType::Int64,
                )),
            ),
            (
                "v".to_owned(),
                ColumnCatalog::visible(ColumnDesc::named(
                    "v",
                    ColumnId::placeholder(),
                    DataType::Varchar,
                )),
            ),
        ]);
        let table_change = pb_table_change(vec![
            pb_column("id", DataType::Int32),
            pb_column("v", DataType::Varchar),
            pb_column("note", DataType::Varchar),
        ]);

        let normalized = normalize_cdc_auto_schema_change_existing_column_types(
            table_change,
            PbCdcTableType::Mysql,
            &original_columns_by_name,
        );
        let columns = normalized
            .columns
            .into_iter()
            .map(ColumnCatalog::from)
            .map(|column| (column.column_desc.name, column.column_desc.data_type))
            .collect::<HashMap<_, _>>();

        assert_eq!(columns["id"], DataType::Int64);
        assert_eq!(columns["v"], DataType::Varchar);
        assert_eq!(columns["note"], DataType::Varchar);
    }

    #[test]
    fn test_mysql_cdc_auto_schema_change_normalizes_column_names() {
        let original_columns_by_name = HashMap::from([
            (
                "Id".to_owned(),
                ColumnCatalog::visible(ColumnDesc::named(
                    "Id",
                    ColumnId::placeholder(),
                    DataType::Int64,
                )),
            ),
            (
                "Details".to_owned(),
                ColumnCatalog::visible(ColumnDesc::named(
                    "Details",
                    ColumnId::placeholder(),
                    DataType::Varchar,
                )),
            ),
        ]);
        let table_change = pb_table_change(vec![
            pb_column("ID", DataType::Int32),
            pb_column("details", DataType::Varchar),
            pb_column("NewCol", DataType::Varchar),
        ]);

        let normalized = normalize_cdc_auto_schema_change_column_names(
            table_change,
            PbCdcTableType::Mysql,
            &original_columns_by_name,
        );
        let normalized = normalize_cdc_auto_schema_change_existing_column_types(
            normalized,
            PbCdcTableType::Mysql,
            &original_columns_by_name,
        );
        let columns = normalized
            .columns
            .into_iter()
            .map(ColumnCatalog::from)
            .map(|column| (column.column_desc.name, column.column_desc.data_type))
            .collect::<Vec<_>>();

        assert_eq!(
            columns,
            vec![
                ("Id".to_owned(), DataType::Int64),
                ("Details".to_owned(), DataType::Varchar),
                ("newcol".to_owned(), DataType::Varchar),
            ]
        );
    }

    #[test]
    fn test_non_mysql_cdc_auto_schema_change_preserves_column_names() {
        let original_columns_by_name = HashMap::from([(
            "Id".to_owned(),
            ColumnCatalog::visible(ColumnDesc::named(
                "Id",
                ColumnId::placeholder(),
                DataType::Int64,
            )),
        )]);
        let table_change = pb_table_change(vec![
            pb_column("ID", DataType::Int64),
            pb_column("NewCol", DataType::Varchar),
        ]);

        let normalized = normalize_cdc_auto_schema_change_column_names(
            table_change,
            PbCdcTableType::Postgres,
            &original_columns_by_name,
        );
        let column_names = normalized
            .columns
            .into_iter()
            .map(ColumnCatalog::from)
            .map(|column| column.column_desc.name)
            .collect::<Vec<_>>();

        assert_eq!(column_names, vec!["ID".to_owned(), "NewCol".to_owned()]);
    }

    #[test]
    fn test_cdc_auto_schema_change_preserves_default_when_normalizing_type() {
        let original = ColumnCatalog::visible(ColumnDesc::named_with_default_value(
            "v",
            ColumnId::placeholder(),
            DataType::Int64,
            Some(ScalarImpl::Int64(1)),
        ));
        let original_columns_by_name = HashMap::from([("v".to_owned(), original.clone())]);
        let table_change = pb_table_change(vec![pb_column_with_default_value(
            "v",
            DataType::Int32,
            ScalarImpl::Int32(1),
        )]);

        let normalized = normalize_cdc_auto_schema_change_existing_column_types(
            table_change,
            PbCdcTableType::Mysql,
            &original_columns_by_name,
        );
        let column = ColumnCatalog::from(normalized.columns.into_iter().next().unwrap());

        assert_eq!(column.column_desc.data_type, DataType::Int64);
        assert_eq!(
            column.column_desc.generated_or_default_column,
            original.column_desc.generated_or_default_column
        );
        assert!(matches!(
            column.column_desc.generated_or_default_column,
            Some(GeneratedOrDefaultColumn::DefaultColumn(_))
        ));
    }
}
