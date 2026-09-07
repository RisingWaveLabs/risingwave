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

use chrono::DateTime;
use itertools::Itertools;
use risingwave_common::id::JobId;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_mut;
use risingwave_connector::source::SplitMetaData;
use risingwave_meta::barrier::BarrierManagerRef;
use risingwave_meta::controller::fragment::StreamingJobInfo;
use risingwave_meta::controller::utils::FragmentDesc;
use risingwave_meta::manager::MetadataManager;
use risingwave_meta::manager::iceberg_compaction::IcebergCompactionManagerRef;
use risingwave_meta::stream::{GlobalRefreshManagerRef, SourceManagerRunningInfo};
use risingwave_meta::{MetaError, model};
use risingwave_meta_model::{ConnectionId, FragmentId, JobStatus, SourceId, StreamingParallelism};
use risingwave_pb::common::ThrottleType;
use risingwave_pb::meta::alter_connector_props_request::AlterConnectorPropsObject;
use risingwave_pb::meta::cancel_creating_jobs_request::Jobs;
use risingwave_pb::meta::list_actor_splits_response::FragmentType;
use risingwave_pb::meta::list_cdc_progress_response::PbCdcProgress;
use risingwave_pb::meta::list_refresh_table_states_response::RefreshTableState;
use risingwave_pb::meta::list_table_fragments_response::{
    ActorInfo, FragmentInfo, TableFragmentInfo,
};
use risingwave_pb::meta::stream_manager_service_server::StreamManagerService;
use risingwave_pb::meta::table_fragments::PbState;
use risingwave_pb::meta::table_fragments::fragment::PbFragmentDistributionType;
use risingwave_pb::meta::*;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::throttle_mutation::ThrottleConfig;
use tonic::{Request, Response, Status};

use crate::barrier::{BarrierScheduler, Command};
use crate::manager::MetaSrvEnv;
use crate::stream::GlobalStreamManagerRef;

pub type TonicResponse<T> = Result<Response<T>, Status>;

#[derive(Clone)]
pub struct StreamServiceImpl {
    env: MetaSrvEnv,
    barrier_scheduler: BarrierScheduler,
    barrier_manager: BarrierManagerRef,
    stream_manager: GlobalStreamManagerRef,
    metadata_manager: MetadataManager,
    refresh_manager: GlobalRefreshManagerRef,
    iceberg_compaction_manager: IcebergCompactionManagerRef,
}

impl StreamServiceImpl {
    pub fn new(
        env: MetaSrvEnv,
        barrier_scheduler: BarrierScheduler,
        barrier_manager: BarrierManagerRef,
        stream_manager: GlobalStreamManagerRef,
        metadata_manager: MetadataManager,
        refresh_manager: GlobalRefreshManagerRef,
        iceberg_compaction_manager: IcebergCompactionManagerRef,
    ) -> Self {
        StreamServiceImpl {
            env,
            barrier_scheduler,
            barrier_manager,
            stream_manager,
            metadata_manager,
            refresh_manager,
            iceberg_compaction_manager,
        }
    }
}

fn effective_streaming_job_parallelism(
    job_status: JobStatus,
    parallelism: StreamingParallelism,
    adaptive_parallelism_strategy: Option<String>,
    backfill_parallelism: Option<StreamingParallelism>,
    backfill_adaptive_parallelism_strategy: Option<String>,
) -> (StreamingParallelism, Option<String>) {
    if job_status != JobStatus::Created {
        (
            backfill_parallelism.unwrap_or(parallelism),
            backfill_adaptive_parallelism_strategy.or(adaptive_parallelism_strategy),
        )
    } else {
        (parallelism, adaptive_parallelism_strategy)
    }
}

#[async_trait::async_trait]
impl StreamManagerService for StreamServiceImpl {
    async fn flush(&self, request: Request<FlushRequest>) -> TonicResponse<FlushResponse> {
        self.env.idle_manager().record_activity();
        let req = request.into_inner();

        let version_id = self.barrier_scheduler.flush(req.database_id).await?;
        Ok(Response::new(FlushResponse {
            status: None,
            hummock_version_id: version_id,
        }))
    }

    async fn list_refresh_table_states(
        &self,
        _request: Request<ListRefreshTableStatesRequest>,
    ) -> TonicResponse<ListRefreshTableStatesResponse> {
        let refresh_jobs = self.metadata_manager.list_refresh_jobs().await?;
        let refresh_table_states = refresh_jobs
            .into_iter()
            .map(|job| RefreshTableState {
                table_id: job.table_id,
                current_status: job.current_status.to_string(),
                last_trigger_time: job
                    .last_trigger_time
                    .map(|time| DateTime::from_timestamp_millis(time).unwrap().to_string()),
                trigger_interval_secs: job.trigger_interval_secs,
                last_success_time: job
                    .last_success_time
                    .map(|time| DateTime::from_timestamp_millis(time).unwrap().to_string()),
            })
            .collect();
        Ok(Response::new(ListRefreshTableStatesResponse {
            states: refresh_table_states,
        }))
    }

    async fn list_iceberg_compaction_status(
        &self,
        _request: Request<ListIcebergCompactionStatusRequest>,
    ) -> TonicResponse<ListIcebergCompactionStatusResponse> {
        let statuses = self
            .iceberg_compaction_manager
            .list_compaction_statuses()
            .into_iter()
            .map(
                |status| list_iceberg_compaction_status_response::IcebergCompactionStatus {
                    sink_id: status.sink_id.as_raw_id(),
                    task_type: status.task_type,
                    trigger_interval_sec: status.trigger_interval_sec,
                    trigger_snapshot_count: status.trigger_snapshot_count as u64,
                    schedule_state: status.schedule_state,
                    next_compaction_after_sec: status.next_compaction_after_sec,
                    pending_snapshot_count: status.pending_snapshot_count.map(|count| count as u64),
                    is_triggerable: status.is_triggerable,
                },
            )
            .collect();

        Ok(Response::new(ListIcebergCompactionStatusResponse {
            statuses,
        }))
    }

    async fn pause(&self, _: Request<PauseRequest>) -> Result<Response<PauseResponse>, Status> {
        for database_id in self.metadata_manager.list_active_database_ids().await? {
            self.barrier_scheduler
                .run_command(database_id, Command::pause())
                .await?;
        }
        Ok(Response::new(PauseResponse {}))
    }

    async fn resume(&self, _: Request<ResumeRequest>) -> Result<Response<ResumeResponse>, Status> {
        for database_id in self.metadata_manager.list_active_database_ids().await? {
            self.barrier_scheduler
                .run_command(database_id, Command::resume())
                .await?;
        }
        Ok(Response::new(ResumeResponse {}))
    }

    async fn apply_throttle(
        &self,
        request: Request<ApplyThrottleRequest>,
    ) -> Result<Response<ApplyThrottleResponse>, Status> {
        let request = request.into_inner();

        // Decode enums from raw i32 fields to handle decoupled target/type.
        let throttle_target = request.throttle_target();
        let throttle_type = request.throttle_type();

        let raw_object_id: u32;
        let fragments: HashMap<FragmentId, _>;

        match (throttle_type, throttle_target) {
            (ThrottleType::Source, ThrottleTarget::Source | ThrottleTarget::Table) => {
                fragments = self
                    .metadata_manager
                    .update_source_rate_limit_by_source_id(request.id.into(), request.rate)
                    .await?;
                raw_object_id = request.id;
            }
            (ThrottleType::Backfill, ThrottleTarget::Mv)
            | (ThrottleType::Backfill, ThrottleTarget::Sink)
            | (ThrottleType::Backfill, ThrottleTarget::Table) => {
                fragments = self
                    .metadata_manager
                    .update_backfill_rate_limit_by_job_id(JobId::from(request.id), request.rate)
                    .await?;
                raw_object_id = request.id;
            }
            (ThrottleType::Dml, ThrottleTarget::Table) => {
                fragments = self
                    .metadata_manager
                    .update_dml_rate_limit_by_job_id(JobId::from(request.id), request.rate)
                    .await?;
                raw_object_id = request.id;
            }
            (ThrottleType::Sink, ThrottleTarget::Sink) => {
                fragments = self
                    .metadata_manager
                    .update_sink_rate_limit_by_sink_id(request.id.into(), request.rate)
                    .await?;
                raw_object_id = request.id;
            }
            // FIXME(kwannoel): specialize for throttle type x target
            (_, ThrottleTarget::Fragment) => {
                let fragment_id = request.id.into();
                let stream_node = self
                    .metadata_manager
                    .update_fragment_rate_limit_by_fragment_id(
                        fragment_id,
                        throttle_type,
                        request.rate,
                    )
                    .await?;
                fragments = [(fragment_id, stream_node)].into_iter().collect();
                let job_id = self
                    .metadata_manager
                    .catalog_controller
                    .get_fragment_streaming_job_id(fragment_id)
                    .await?;
                raw_object_id = job_id.as_raw_id();
            }
            _ => {
                return Err(Status::invalid_argument(format!(
                    "unsupported throttle target/type: {:?}/{:?}",
                    throttle_target, throttle_type
                )));
            }
        };

        let database_id = self
            .metadata_manager
            .catalog_controller
            .get_object_database_id(raw_object_id)
            .await?;

        let throttle_config = ThrottleConfig {
            rate_limit: request.rate,
            throttle_type: throttle_type.into(),
        };
        let config = fragments
            .into_iter()
            .map(|(fragment_id, stream_node)| (fragment_id, (throttle_config, stream_node)))
            .collect();
        let _i = self
            .barrier_scheduler
            .run_command(database_id, Command::Throttle { config })
            .await?;

        Ok(Response::new(ApplyThrottleResponse { status: None }))
    }

    async fn cancel_creating_jobs(
        &self,
        request: Request<CancelCreatingJobsRequest>,
    ) -> TonicResponse<CancelCreatingJobsResponse> {
        let req = request.into_inner();
        let job_ids = match req.jobs.unwrap() {
            Jobs::Infos(infos) => self
                .metadata_manager
                .catalog_controller
                .find_creating_streaming_job_ids(infos.infos)
                .await?
                .into_iter()
                .map(|id| id.as_job_id())
                .collect(),
            Jobs::Ids(jobs) => jobs.job_ids,
        };

        let canceled_jobs = self
            .stream_manager
            .cancel_streaming_jobs(job_ids)
            .await?
            .into_iter()
            .map(|id| id.as_raw_id())
            .collect_vec();
        Ok(Response::new(CancelCreatingJobsResponse {
            status: None,
            canceled_jobs,
        }))
    }

    async fn list_table_fragments(
        &self,
        request: Request<ListTableFragmentsRequest>,
    ) -> Result<Response<ListTableFragmentsResponse>, Status> {
        let req = request.into_inner();
        let table_ids = HashSet::<JobId>::from_iter(req.table_ids);

        let mut info = HashMap::new();
        for job_id in table_ids {
            let (table_fragments, fragment_actors, _actor_status) = self
                .metadata_manager
                .catalog_controller
                .get_job_fragments_by_id(job_id)
                .await?;
            let mut dispatchers = self
                .metadata_manager
                .catalog_controller
                .get_fragment_actor_dispatchers(
                    table_fragments.fragment_ids().map(|id| id as _).collect(),
                )
                .await?;
            let ctx = table_fragments.ctx.to_protobuf();
            info.insert(
                table_fragments.stream_job_id(),
                TableFragmentInfo {
                    fragments: table_fragments
                        .fragments
                        .into_iter()
                        .map(|(id, fragment)| FragmentInfo {
                            id,
                            actors: fragment_actors
                                .get(&id)
                                .into_iter()
                                .flat_map(|actors| actors.iter().map(|actor| actor.actor_id))
                                .map(|actor_id| ActorInfo {
                                    id: actor_id,
                                    node: Some(fragment.nodes.clone()),
                                    dispatcher: dispatchers
                                        .get_mut(&fragment.fragment_id)
                                        .and_then(|dispatchers| dispatchers.remove(&actor_id))
                                        .unwrap_or_default(),
                                })
                                .collect_vec(),
                        })
                        .collect_vec(),
                    ctx: Some(ctx),
                },
            );
        }

        Ok(Response::new(ListTableFragmentsResponse {
            table_fragments: info,
        }))
    }

    async fn list_streaming_job_states(
        &self,
        _request: Request<ListStreamingJobStatesRequest>,
    ) -> Result<Response<ListStreamingJobStatesResponse>, Status> {
        let job_infos = self
            .metadata_manager
            .catalog_controller
            .list_streaming_job_infos()
            .await?;
        let states = job_infos
            .into_iter()
            .map(
                |StreamingJobInfo {
                     job_id,
                     job_status,
                     name,
                     parallelism,
                     adaptive_parallelism_strategy,
                     backfill_parallelism,
                     backfill_adaptive_parallelism_strategy,
                     max_parallelism,
                     resource_group,
                     database_id,
                     schema_id,
                     config_override,
                     ..
                 }| {
                    // While a job is still being created, system tables should surface the
                    // temporary backfill override instead of the post-backfill target value.
                    let (parallelism, adaptive_parallelism_strategy) =
                        effective_streaming_job_parallelism(
                            job_status,
                            parallelism,
                            adaptive_parallelism_strategy,
                            backfill_parallelism,
                            backfill_adaptive_parallelism_strategy,
                        );
                    let parallelism = match parallelism {
                        StreamingParallelism::Adaptive => model::TableParallelism::Adaptive,
                        StreamingParallelism::Custom => model::TableParallelism::Custom,
                        StreamingParallelism::Fixed(n) => model::TableParallelism::Fixed(n as _),
                    };

                    list_streaming_job_states_response::StreamingJobState {
                        table_id: job_id,
                        name,
                        state: PbState::from(job_status) as _,
                        parallelism: Some(parallelism.into()),
                        max_parallelism: max_parallelism as _,
                        resource_group,
                        database_id,
                        schema_id,
                        config_override,
                        adaptive_parallelism_strategy,
                    }
                },
            )
            .collect_vec();

        Ok(Response::new(ListStreamingJobStatesResponse { states }))
    }

    async fn list_fragment_distribution(
        &self,
        _request: Request<ListFragmentDistributionRequest>,
    ) -> Result<Response<ListFragmentDistributionResponse>, Status> {
        let include_node = _request.into_inner().include_node.unwrap_or(true);
        let distributions = if include_node {
            self.metadata_manager
                .catalog_controller
                .list_fragment_descs_with_node(false)
                .await?
        } else {
            self.metadata_manager
                .catalog_controller
                .list_fragment_descs_without_node(false)
                .await?
        }
        .into_iter()
        .map(|(dist, _)| dist)
        .collect();

        Ok(Response::new(ListFragmentDistributionResponse {
            distributions,
        }))
    }

    async fn list_creating_fragment_distribution(
        &self,
        _request: Request<ListCreatingFragmentDistributionRequest>,
    ) -> Result<Response<ListCreatingFragmentDistributionResponse>, Status> {
        let include_node = _request.into_inner().include_node.unwrap_or(true);
        let distributions = if include_node {
            self.metadata_manager
                .catalog_controller
                .list_fragment_descs_with_node(true)
                .await?
        } else {
            self.metadata_manager
                .catalog_controller
                .list_fragment_descs_without_node(true)
                .await?
        }
        .into_iter()
        .map(|(dist, _)| dist)
        .collect();

        Ok(Response::new(ListCreatingFragmentDistributionResponse {
            distributions,
        }))
    }

    async fn list_sink_log_store_tables(
        &self,
        _request: Request<ListSinkLogStoreTablesRequest>,
    ) -> Result<Response<ListSinkLogStoreTablesResponse>, Status> {
        let tables = self
            .metadata_manager
            .catalog_controller
            .list_sink_log_store_tables()
            .await?
            .into_iter()
            .map(|(sink_id, internal_table_id)| {
                list_sink_log_store_tables_response::SinkLogStoreTable {
                    sink_id: sink_id.as_raw_id(),
                    internal_table_id: internal_table_id.as_raw_id(),
                }
            })
            .collect();

        Ok(Response::new(ListSinkLogStoreTablesResponse { tables }))
    }

    async fn get_fragment_by_id(
        &self,
        request: Request<GetFragmentByIdRequest>,
    ) -> Result<Response<GetFragmentByIdResponse>, Status> {
        let req = request.into_inner();
        let fragment_desc = self
            .metadata_manager
            .catalog_controller
            .get_fragment_desc_by_id(req.fragment_id)
            .await?;
        let distribution = fragment_desc
            .map(|(desc, upstreams)| fragment_desc_to_distribution(desc, upstreams, true));
        Ok(Response::new(GetFragmentByIdResponse { distribution }))
    }

    async fn get_fragment_vnodes(
        &self,
        request: Request<GetFragmentVnodesRequest>,
    ) -> Result<Response<GetFragmentVnodesResponse>, Status> {
        let req = request.into_inner();
        let fragment_id = req.fragment_id;

        let shared_actor_infos = self.env.shared_actor_infos();
        let guard = shared_actor_infos.read_guard();

        let fragment_info = guard
            .get_fragment(fragment_id)
            .ok_or_else(|| Status::not_found(format!("Fragment {} not found", fragment_id)))?;

        let actor_vnodes = fragment_info
            .actors
            .iter()
            .map(|(actor_id, actor_info)| {
                let vnode_indices = if let Some(ref vnode_bitmap) = actor_info.vnode_bitmap {
                    vnode_bitmap.iter_ones().map(|v| v as u32).collect()
                } else {
                    vec![]
                };

                get_fragment_vnodes_response::ActorVnodes {
                    actor_id: *actor_id,
                    vnode_indices,
                }
            })
            .collect();

        Ok(Response::new(GetFragmentVnodesResponse { actor_vnodes }))
    }

    async fn get_actor_vnodes(
        &self,
        request: Request<GetActorVnodesRequest>,
    ) -> Result<Response<GetActorVnodesResponse>, Status> {
        let req = request.into_inner();
        let actor_id = req.actor_id;

        let shared_actor_infos = self.env.shared_actor_infos();
        let guard = shared_actor_infos.read_guard();

        // Find the actor across all fragments
        let actor_info = guard
            .iter_over_fragments()
            .find_map(|(_, fragment_info)| fragment_info.actors.get(&actor_id))
            .ok_or_else(|| Status::not_found(format!("Actor {} not found", actor_id)))?;

        let vnode_indices = if let Some(ref vnode_bitmap) = actor_info.vnode_bitmap {
            vnode_bitmap.iter_ones().map(|v| v as u32).collect()
        } else {
            vec![]
        };

        Ok(Response::new(GetActorVnodesResponse { vnode_indices }))
    }

    async fn list_actor_states(
        &self,
        _request: Request<ListActorStatesRequest>,
    ) -> Result<Response<ListActorStatesResponse>, Status> {
        let actor_locations = self
            .metadata_manager
            .catalog_controller
            .list_actor_locations()?;
        let states = actor_locations
            .into_iter()
            .map(|actor_location| list_actor_states_response::ActorState {
                actor_id: actor_location.actor_id,
                fragment_id: actor_location.fragment_id,
                worker_id: actor_location.worker_id,
            })
            .collect_vec();

        Ok(Response::new(ListActorStatesResponse { states }))
    }

    async fn recover(
        &self,
        _request: Request<RecoverRequest>,
    ) -> Result<Response<RecoverResponse>, Status> {
        self.barrier_manager.adhoc_recovery().await?;
        Ok(Response::new(RecoverResponse {}))
    }

    async fn list_actor_splits(
        &self,
        _request: Request<ListActorSplitsRequest>,
    ) -> Result<Response<ListActorSplitsResponse>, Status> {
        let SourceManagerRunningInfo {
            source_fragments,
            backfill_fragments,
        } = self.stream_manager.source_manager.get_running_info().await;

        let mut actor_splits = self.env.shared_actor_infos().list_assignments();

        let source_actors: HashMap<_, _> = {
            let all_fragment_ids: HashSet<_> = backfill_fragments
                .values()
                .flat_map(|set| set.iter().flat_map(|&(id1, id2)| [id1, id2]))
                .chain(source_fragments.values().flatten().copied())
                .collect();

            let guard = self.env.shared_actor_infos().read_guard();
            guard
                .iter_over_fragments()
                .filter(|(frag_id, _)| all_fragment_ids.contains(*frag_id))
                .flat_map(|(fragment_id, fragment_info)| {
                    fragment_info
                        .actors
                        .keys()
                        .copied()
                        .map(|actor_id| (actor_id, *fragment_id))
                })
                .collect()
        };

        let is_shared_source = self
            .metadata_manager
            .catalog_controller
            .list_source_id_with_shared_types()
            .await?;

        let fragment_to_source: HashMap<_, _> = source_fragments
            .into_iter()
            .flat_map(|(source_id, fragment_ids)| {
                let source_type = if is_shared_source.get(&source_id).copied().unwrap_or(false) {
                    FragmentType::SharedSource
                } else {
                    FragmentType::NonSharedSource
                };

                fragment_ids
                    .into_iter()
                    .map(move |fragment_id| (fragment_id, (source_id, source_type)))
            })
            .chain(
                backfill_fragments
                    .into_iter()
                    .flat_map(|(source_id, fragment_ids)| {
                        fragment_ids.into_iter().flat_map(
                            move |(fragment_id, upstream_fragment_id)| {
                                [
                                    (fragment_id, (source_id, FragmentType::SharedSourceBackfill)),
                                    (
                                        upstream_fragment_id,
                                        (source_id, FragmentType::SharedSource),
                                    ),
                                ]
                            },
                        )
                    }),
            )
            .collect();

        let actor_splits = source_actors
            .into_iter()
            .flat_map(|(actor_id, fragment_id)| {
                let (source_id, fragment_type) = fragment_to_source
                    .get(&fragment_id)
                    .copied()
                    .unwrap_or_default();

                actor_splits
                    .remove(&actor_id)
                    .unwrap_or_default()
                    .into_iter()
                    .map(move |split| list_actor_splits_response::ActorSplit {
                        actor_id,
                        source_id,
                        fragment_id,
                        split_id: split.id().to_string(),
                        fragment_type: fragment_type.into(),
                    })
            })
            .collect_vec();

        Ok(Response::new(ListActorSplitsResponse { actor_splits }))
    }

    async fn list_rate_limits(
        &self,
        _request: Request<ListRateLimitsRequest>,
    ) -> Result<Response<ListRateLimitsResponse>, Status> {
        let rate_limits = self
            .metadata_manager
            .catalog_controller
            .list_rate_limits()
            .await?;
        Ok(Response::new(ListRateLimitsResponse { rate_limits }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn refresh(
        &self,
        request: Request<RefreshRequest>,
    ) -> Result<Response<RefreshResponse>, Status> {
        let req = request.into_inner();

        tracing::info!("Refreshing table with id: {}", req.table_id);

        let response = self
            .refresh_manager
            .trigger_manual_refresh(req, self.env.shared_actor_infos())
            .await?;

        Ok(Response::new(response))
    }

    async fn alter_connector_props(
        &self,
        request: Request<AlterConnectorPropsRequest>,
    ) -> Result<Response<AlterConnectorPropsResponse>, Status> {
        let request = request.into_inner();
        let secret_manager = LocalSecretManager::global();
        let (new_props_plaintext, object_id) = match AlterConnectorPropsObject::try_from(
            request.object_type,
        ) {
            Ok(AlterConnectorPropsObject::Sink) => (
                self.metadata_manager
                    .update_sink_props_by_sink_id(
                        request.object_id.into(),
                        request.changed_props.clone().into_iter().collect(),
                    )
                    .await?,
                request.object_id.into(),
            ),
            Ok(AlterConnectorPropsObject::IcebergTable) => {
                let (prop, sink_id) = self
                    .metadata_manager
                    .update_iceberg_table_props_by_table_id(
                        request.object_id.into(),
                        request.changed_props.clone().into_iter().collect(),
                        request.extra_options,
                    )
                    .await?;
                (prop, sink_id.as_object_id())
            }

            Ok(AlterConnectorPropsObject::Source) => {
                // alter source and table's associated source
                if request.connector_conn_ref.is_some() {
                    return Err(Status::invalid_argument(
                        "alter connector_conn_ref is not supported",
                    ));
                }
                let options_with_secret = self
                    .metadata_manager
                    .catalog_controller
                    .update_source_props_by_source_id(
                        request.object_id.into(),
                        request.changed_props.clone().into_iter().collect(),
                        request.changed_secret_refs.clone().into_iter().collect(),
                        false, // SQL ALTER SOURCE enforces alter-on-fly check
                    )
                    .await?;

                self.stream_manager
                    .source_manager
                    .validate_source_once(request.object_id.into(), options_with_secret.clone())
                    .await?;

                let (options, secret_refs) = options_with_secret.into_parts();
                (
                    secret_manager
                        .fill_secrets(options, secret_refs)
                        .map_err(MetaError::from)?
                        .into_iter()
                        .collect(),
                    request.object_id.into(),
                )
            }
            Ok(AlterConnectorPropsObject::Connection) => {
                // Update the connection and all dependent sources/sinks atomically, and later broadcast
                // the complete plaintext properties to dependents via barrier.
                let (
                    connection_options_with_secret,
                    updated_sources_with_props,
                    updated_sinks_with_props,
                ) = self
                    .metadata_manager
                    .catalog_controller
                    .update_connection_and_dependent_objects_props(
                        ConnectionId::from(request.object_id),
                        request.changed_props.clone().into_iter().collect(),
                        request.changed_secret_refs.clone().into_iter().collect(),
                    )
                    .await?;

                // Materialize connection plaintext for observability/debugging (not broadcast directly).
                let (options, secret_refs) = connection_options_with_secret.into_parts();
                let new_props_plaintext = secret_manager
                    .fill_secrets(options, secret_refs)
                    .map_err(MetaError::from)?
                    .into_iter()
                    .collect::<HashMap<String, String>>();

                // Broadcast changes to dependent sources and sinks if any exist.
                let mut dependent_mutation = HashMap::default();
                for (source_id, complete_source_props) in updated_sources_with_props {
                    dependent_mutation.insert(source_id.as_object_id(), complete_source_props);
                }
                for (sink_id, complete_sink_props) in updated_sinks_with_props {
                    dependent_mutation.insert(sink_id.as_object_id(), complete_sink_props);
                }

                if !dependent_mutation.is_empty() {
                    let database_id = self
                        .metadata_manager
                        .catalog_controller
                        .get_object_database_id(ConnectionId::from(request.object_id))
                        .await?;
                    tracing::info!(
                        "broadcasting connection {} property changes to dependent object ids: {:?}",
                        request.object_id,
                        dependent_mutation.keys().collect_vec()
                    );
                    let _version = self
                        .barrier_scheduler
                        .run_command(
                            database_id,
                            Command::ConnectorPropsChange(dependent_mutation),
                        )
                        .await?;
                }

                (
                    new_props_plaintext,
                    ConnectionId::from(request.object_id).as_object_id(),
                )
            }

            _ => {
                unimplemented!(
                    "Unsupported object type for AlterConnectorProps: {:?}",
                    request.object_type
                );
            }
        };

        let database_id = self
            .metadata_manager
            .catalog_controller
            .get_object_database_id(object_id)
            .await?;
        // Connection updates are broadcast to dependent sources/sinks inside the `Connection` branch above.
        // For sources/sinks/iceberg-table updates, broadcast the change to the object itself.
        if AlterConnectorPropsObject::try_from(request.object_type)
            .is_ok_and(|t| t != AlterConnectorPropsObject::Connection)
        {
            let mut mutation = HashMap::default();
            mutation.insert(object_id, new_props_plaintext);
            let _version = self
                .barrier_scheduler
                .run_command(database_id, Command::ConnectorPropsChange(mutation))
                .await?;
        }

        Ok(Response::new(AlterConnectorPropsResponse {}))
    }

    async fn set_sync_log_store_aligned(
        &self,
        request: Request<SetSyncLogStoreAlignedRequest>,
    ) -> Result<Response<SetSyncLogStoreAlignedResponse>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id;
        let aligned = req.aligned;

        self.metadata_manager
            .catalog_controller
            .mutate_fragments_by_job_id(
                job_id,
                |_mask, stream_node| {
                    let mut visited = false;
                    visit_stream_node_mut(stream_node, |body| {
                        if let NodeBody::SyncLogStore(sync_log_store) = body {
                            sync_log_store.aligned = aligned;
                            visited = true
                        }
                    });
                    Ok(visited)
                },
                "no fragments found with synced log store",
            )
            .await?;

        Ok(Response::new(SetSyncLogStoreAlignedResponse {}))
    }

    async fn list_cdc_progress(
        &self,
        _request: Request<ListCdcProgressRequest>,
    ) -> Result<Response<ListCdcProgressResponse>, Status> {
        let cdc_progress = self
            .barrier_manager
            .get_cdc_progress()
            .await?
            .into_iter()
            .map(|(job_id, p)| {
                (
                    job_id,
                    PbCdcProgress {
                        split_total_count: p.split_total_count,
                        split_backfilled_count: p.split_backfilled_count,
                        split_completed_count: p.split_completed_count,
                        backfilled_row_count: p.backfilled_row_count,
                        estimated_row_count: p.estimated_row_count,
                    },
                )
            })
            .collect();
        Ok(Response::new(ListCdcProgressResponse { cdc_progress }))
    }

    async fn list_unmigrated_tables(
        &self,
        _request: Request<ListUnmigratedTablesRequest>,
    ) -> Result<Response<ListUnmigratedTablesResponse>, Status> {
        let unmigrated_tables = self
            .metadata_manager
            .catalog_controller
            .list_unmigrated_tables()
            .await?
            .into_iter()
            .map(|table| list_unmigrated_tables_response::UnmigratedTable {
                table_id: table.id,
                table_name: table.name,
            })
            .collect();

        Ok(Response::new(ListUnmigratedTablesResponse {
            tables: unmigrated_tables,
        }))
    }

    /// Orchestrated source property update with pause/update/resume workflow.
    /// This is the "safe" version that pauses sources before updating and resumes after.
    async fn alter_source_properties_safe(
        &self,
        request: Request<AlterSourcePropertiesSafeRequest>,
    ) -> Result<Response<AlterSourcePropertiesSafeResponse>, Status> {
        let request = request.into_inner();
        let source_id = request.source_id;
        let options = request.options.unwrap_or_default();

        tracing::info!(
            source_id = source_id,
            reset_splits = options.reset_splits,
            "Starting orchestrated source property update"
        );

        // Get the database ID for the source
        let database_id = self
            .metadata_manager
            .catalog_controller
            .get_object_database_id(SourceId::from(source_id))
            .await?;

        // Step 1: Pause the stream (already commits state)
        tracing::info!(source_id = source_id, "Pausing stream");
        self.barrier_scheduler
            .run_command(database_id, Command::Pause)
            .await?;

        // Step 2: Update catalog and get the new properties
        let result = async {
            let secret_manager = LocalSecretManager::global();

            let options_with_secret = self
                .metadata_manager
                .catalog_controller
                .update_source_props_by_source_id(
                    source_id.into(),
                    request.changed_props.clone().into_iter().collect(),
                    request.changed_secret_refs.clone().into_iter().collect(),
                    true, // risectl admin operation skips alter-on-fly check
                )
                .await?;

            // Validate the source
            self.stream_manager
                .source_manager
                .validate_source_once(source_id.into(), options_with_secret.clone())
                .await?;

            let (props, secret_refs) = options_with_secret.into_parts();
            let new_props_plaintext: HashMap<String, String> = secret_manager
                .fill_secrets(props, secret_refs)
                .map_err(MetaError::from)?
                .into_iter()
                .collect();

            // Step 3: Issue ConnectorPropsChange barrier
            tracing::info!(
                source_id = source_id,
                "Issuing ConnectorPropsChange barrier"
            );
            let mut mutation = HashMap::default();
            mutation.insert(source_id.into(), new_props_plaintext);
            self.barrier_scheduler
                .run_command(database_id, Command::ConnectorPropsChange(mutation))
                .await?;

            // Step 4: Optional split reset
            if options.reset_splits {
                tracing::info!(source_id = source_id, "Resetting source splits");
                self.stream_manager
                    .source_manager
                    .reset_source_splits(source_id.into())
                    .await?;
            }

            Ok::<_, MetaError>(())
        }
        .await;

        // Step 5: Resume the stream (even if previous steps failed)
        tracing::info!(source_id = source_id, "Resuming stream");
        let resume_result = self
            .barrier_scheduler
            .run_command(database_id, Command::Resume)
            .await;

        // Return the first error if any
        result?;
        resume_result?;

        tracing::info!(
            source_id = source_id,
            "Orchestrated source property update completed successfully"
        );

        Ok(Response::new(AlterSourcePropertiesSafeResponse {}))
    }

    /// Reset source split assignments (UNSAFE - admin only).
    /// This clears persisted split metadata and triggers re-discovery.
    async fn reset_source_splits(
        &self,
        request: Request<ResetSourceSplitsRequest>,
    ) -> Result<Response<ResetSourceSplitsResponse>, Status> {
        let request = request.into_inner();
        let source_id = request.source_id;

        tracing::warn!(
            source_id = source_id,
            "UNSAFE: Resetting source splits - this may cause data duplication or loss"
        );

        self.stream_manager
            .source_manager
            .reset_source_splits(source_id.into())
            .await?;

        Ok(Response::new(ResetSourceSplitsResponse {}))
    }

    /// Inject specific offsets into source splits (UNSAFE - admin only).
    /// This can cause data duplication or loss depending on the correctness of the provided offsets.
    async fn inject_source_offsets(
        &self,
        request: Request<InjectSourceOffsetsRequest>,
    ) -> Result<Response<InjectSourceOffsetsResponse>, Status> {
        let request = request.into_inner();
        let source_id = request.source_id;
        let split_offsets = request.split_offsets;

        // Validate split IDs exist before proceeding
        let applied_split_ids = self
            .stream_manager
            .source_manager
            .validate_inject_source_offsets(source_id.into(), &split_offsets)
            .await?;

        tracing::warn!(
            source_id = source_id,
            num_offsets = split_offsets.len(),
            "UNSAFE: Injecting source offsets - this may cause data duplication or loss"
        );

        let database_id = self
            .metadata_manager
            .catalog_controller
            .get_object_database_id(SourceId::from(source_id))
            .await?;

        self.barrier_scheduler
            .run_command(
                database_id,
                Command::InjectSourceOffsets {
                    source_id: SourceId::from(source_id),
                    split_offsets,
                },
            )
            .await?;

        Ok(Response::new(InjectSourceOffsetsResponse {
            applied_split_ids,
        }))
    }
}

fn fragment_desc_to_distribution(
    fragment_desc: FragmentDesc,
    upstreams: Vec<FragmentId>,
    include_node: bool,
) -> FragmentDistribution {
    let node = include_node.then(|| fragment_desc.stream_node.to_protobuf());
    FragmentDistribution {
        fragment_id: fragment_desc.fragment_id,
        table_id: fragment_desc.job_id,
        distribution_type: PbFragmentDistributionType::from(fragment_desc.distribution_type) as _,
        state_table_ids: fragment_desc.state_table_ids.0,
        upstream_fragment_ids: upstreams,
        fragment_type_mask: fragment_desc.fragment_type_mask as _,
        parallelism: fragment_desc.parallelism as _,
        vnode_count: fragment_desc.vnode_count as _,
        node,
        parallelism_policy: fragment_desc.parallelism_policy,
    }
}

#[cfg(test)]
mod tests {
    use risingwave_meta_model::{JobStatus, StreamingParallelism};

    use super::effective_streaming_job_parallelism;

    #[test]
    fn test_effective_streaming_job_parallelism_prefers_backfill_override() {
        let (parallelism, strategy) = effective_streaming_job_parallelism(
            JobStatus::Creating,
            StreamingParallelism::Adaptive,
            Some("BOUNDED(4)".to_owned()),
            Some(StreamingParallelism::Adaptive),
            Some("BOUNDED(2)".to_owned()),
        );

        assert_eq!(parallelism, StreamingParallelism::Adaptive);
        assert_eq!(strategy.as_deref(), Some("BOUNDED(2)"));
    }

    #[test]
    fn test_effective_streaming_job_parallelism_falls_back_to_job_strategy() {
        let (parallelism, strategy) = effective_streaming_job_parallelism(
            JobStatus::Initial,
            StreamingParallelism::Adaptive,
            Some("RATIO(0.5)".to_owned()),
            Some(StreamingParallelism::Adaptive),
            None,
        );

        assert_eq!(parallelism, StreamingParallelism::Adaptive);
        assert_eq!(strategy.as_deref(), Some("RATIO(0.5)"));
    }

    #[test]
    fn test_effective_streaming_job_parallelism_ignores_backfill_after_creation() {
        let (parallelism, strategy) = effective_streaming_job_parallelism(
            JobStatus::Created,
            StreamingParallelism::Fixed(4),
            None,
            Some(StreamingParallelism::Fixed(2)),
            Some("BOUNDED(2)".to_owned()),
        );

        assert_eq!(parallelism, StreamingParallelism::Fixed(4));
        assert_eq!(strategy, None);
    }
}
