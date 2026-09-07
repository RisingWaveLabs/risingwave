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

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use await_tree::span;
use futures::future::join_all;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{DatabaseId, Field, FragmentTypeFlag, FragmentTypeMask, TableId};
use risingwave_common::hash::VnodeCountCompat;
use risingwave_common::id::{JobId, SinkId};
use risingwave_connector::source::CdcTableSnapshotSplitRaw;
use risingwave_meta_model::prelude::Fragment as FragmentModel;
use risingwave_meta_model::{StreamingParallelism, WorkerId, fragment, streaming_job};
use risingwave_pb::catalog::{CreateType, PbSink, PbTable, Subscription};
use risingwave_pb::ddl_service::streaming_job_resource_type;
use risingwave_pb::expr::PbExprNode;
use risingwave_pb::plan_common::{PbColumnCatalog, PbField};
use risingwave_pb::serverless_backfill_controller::{
    ProvisionRequest, node_group_controller_service_client,
};
use risingwave_rpc_client::error::TonicStatusWrapper;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QuerySelect};
use thiserror_ext::AsReport;
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLockReadGuard, oneshot};
use tokio::time::{Duration, Instant};
use tracing::Instrument;

use super::{
    GlobalRefreshManagerRef, ParallelismPolicy, ReschedulePolicy, ScaleControllerRef,
    StreamFragmentGraph, UserDefinedFragmentBackfillOrder,
};
use crate::barrier::{
    BarrierScheduler, BatchRefreshInfo, Command, CreateStreamingJobCommandInfo,
    CreateStreamingJobType, ReplaceStreamJobPlan, SinceEpochInfo, SnapshotBackfillInfo,
};
use crate::controller::catalog::DropTableConnectorContext;
use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::error::bail_invalid_parameter;
use crate::hummock::HummockManagerRef;
use crate::manager::iceberg_compaction::IcebergCompactionManagerRef;
use crate::manager::{
    MetaSrvEnv, MetadataManager, NotificationVersion, StreamingJob, StreamingJobType,
};
use crate::model::{
    ActorId, DownstreamFragmentRelation, Fragment, FragmentDownstreamRelation, FragmentId,
    FragmentReplaceUpstream, StreamActor, StreamContext, StreamJobFragments,
    StreamJobFragmentsToCreate, SubscriptionId,
};
use crate::stream::{ReplaceJobSplitPlan, SourceManagerRef};
use crate::{MetaError, MetaResult};

pub type GlobalStreamManagerRef = Arc<GlobalStreamManager>;

/// The error carries whether the caller should explicitly cancel the creating job and an optional
/// notifier for an awaited cancellation request.
pub type CreateStreamingJobResult =
    Result<NotificationVersion, (MetaError, bool, Option<oneshot::Sender<bool>>)>;

/// A user is assumed to stay focused on a streaming-job creation for at most 30 seconds. If an
/// error occurs during that time, cancel the job so that they can investigate the error. After
/// that, prioritize eventual completion by continuing to wait through transient errors.
const FOREGROUND_DDL_EARLY_FAILURE_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) async fn cleanup_dropped_streaming_jobs(
    refresh_manager: &GlobalRefreshManagerRef,
    hummock_manager: &HummockManagerRef,
    metadata_manager: &MetadataManager,
    streaming_job_ids: impl IntoIterator<Item = JobId>,
    state_table_ids: Vec<TableId>,
    progress_status: &str,
) -> MetaResult<()> {
    for job_id in streaming_job_ids {
        refresh_manager.remove_progress_tracker(job_id.as_mv_table_id(), progress_status);
    }

    if state_table_ids.is_empty() {
        return Ok(());
    }

    hummock_manager
        .unregister_table_ids(state_table_ids.clone())
        .await?;
    metadata_manager
        .catalog_controller
        .complete_dropped_tables(state_table_ids)
        .await;
    Ok(())
}

#[derive(Default)]
pub struct CreateStreamingJobOption {
    // leave empty as a placeholder for future option if there is any
}

#[derive(Debug, Clone)]
pub struct UpstreamSinkInfo {
    pub sink_id: SinkId,
    pub sink_fragment_id: FragmentId,
    pub sink_output_fields: Vec<PbField>,
    // for backwards compatibility
    pub sink_original_target_columns: Vec<PbColumnCatalog>,
    pub project_exprs: Vec<PbExprNode>,
    pub new_sink_downstream: DownstreamFragmentRelation,
}

/// [`CreateStreamingJobContext`] carries one-time infos for creating a streaming job.
///
/// Note: for better readability, keep this struct complete and immutable once created.
pub struct CreateStreamingJobContext {
    /// New fragment relation to add from upstream fragments to downstream fragments.
    pub upstream_fragment_downstreams: FragmentDownstreamRelation,

    /// The resource group of the database this job belongs to.
    pub database_resource_group: String,

    /// DDL definition.
    pub definition: String,

    pub create_type: CreateType,

    pub job_type: StreamingJobType,

    /// Used for sink-into-table.
    pub new_upstream_sink: Option<UpstreamSinkInfo>,

    pub snapshot_backfill_info: Option<SnapshotBackfillInfo>,
    pub cross_db_snapshot_backfill_info: SnapshotBackfillInfo,

    pub cdc_table_snapshot_splits: Option<Vec<CdcTableSnapshotSplitRaw>>,

    pub option: CreateStreamingJobOption,

    pub streaming_job: StreamingJob,

    pub fragment_backfill_ordering: UserDefinedFragmentBackfillOrder,

    pub locality_fragment_state_table_mapping: HashMap<FragmentId, Vec<TableId>>,

    pub is_serverless_backfill: bool,

    pub resource_type: streaming_job_resource_type::ResourceType,

    /// The `streaming_job::Model` for this job, loaded from meta store.
    pub streaming_job_model: streaming_job::Model,

    /// If set, this create command replaces an existing sink while creating the new sink job.
    pub replace_sink: Option<SinkId>,

    /// Batch refresh interval in seconds. If set, the MV uses batch refresh semantics.
    pub refresh_interval_sec: Option<u64>,

    pub since_timestamp_epoch: Option<u64>,
}

struct StreamingJobExecution {
    id: JobId,
    shutdown_tx: Option<oneshot::Sender<oneshot::Sender<bool>>>,
    _permit: OwnedSemaphorePermit,
}

impl StreamingJobExecution {
    fn new(
        id: JobId,
        shutdown_tx: oneshot::Sender<oneshot::Sender<bool>>,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            id,
            shutdown_tx: Some(shutdown_tx),
            _permit: permit,
        }
    }
}

#[derive(Default)]
struct CreatingStreamingJobInfo {
    streaming_jobs: Mutex<HashMap<JobId, StreamingJobExecution>>,
}

impl CreatingStreamingJobInfo {
    async fn add_job(&self, job: StreamingJobExecution) {
        let mut jobs = self.streaming_jobs.lock().await;
        jobs.insert(job.id, job);
    }

    async fn delete_job(&self, job_id: JobId) {
        let mut jobs = self.streaming_jobs.lock().await;
        jobs.remove(&job_id);
    }

    async fn cancel_jobs(
        &self,
        job_ids: Vec<JobId>,
    ) -> MetaResult<(HashMap<JobId, oneshot::Receiver<bool>>, Vec<JobId>)> {
        let mut jobs = self.streaming_jobs.lock().await;
        let mut receivers = HashMap::new();
        let mut background_job_ids = vec![];
        for job_id in job_ids {
            if let Some(job) = jobs.get_mut(&job_id) {
                if let Some(shutdown_tx) = job.shutdown_tx.take() {
                    let (tx, rx) = oneshot::channel();
                    match shutdown_tx.send(tx) {
                        Ok(()) => {
                            receivers.insert(job_id, rx);
                        }
                        Err(_) => {
                            return Err(anyhow::anyhow!(
                                "failed to send shutdown signal for streaming job {}: receiver dropped",
                                job_id
                            )
                            .into());
                        }
                    }
                }
            } else {
                // If these job ids do not exist in streaming_jobs, they should be background creating jobs.
                background_job_ids.push(job_id);
            }
        }

        Ok((receivers, background_job_ids))
    }
}

type CreatingStreamingJobInfoRef = Arc<CreatingStreamingJobInfo>;

#[derive(Debug, Clone)]
pub struct AutoRefreshSchemaSinkContext {
    pub tmp_sink_id: SinkId,
    pub original_sink: PbSink,
    pub original_fragment: Fragment,
    pub new_schema: Vec<PbColumnCatalog>,
    pub newly_add_fields: Vec<Field>,
    pub removed_column_names: Vec<String>,
    pub new_fragment: Fragment,
    pub new_log_store_table: Option<Box<PbTable>>,
    /// The sink's own stream context (timezone, `config_override`).
    pub ctx: StreamContext,
}

impl AutoRefreshSchemaSinkContext {
    pub fn new_fragment_info(
        &self,
        stream_actors: &HashMap<FragmentId, Vec<StreamActor>>,
        actor_location: &HashMap<ActorId, WorkerId>,
    ) -> InflightFragmentInfo {
        InflightFragmentInfo {
            fragment_id: self.new_fragment.fragment_id,
            distribution_type: self.new_fragment.distribution_type.into(),
            fragment_type_mask: self.new_fragment.fragment_type_mask,
            vnode_count: self.new_fragment.vnode_count(),
            nodes: self.new_fragment.nodes.clone(),
            actors: stream_actors
                .get(&self.new_fragment.fragment_id)
                .into_iter()
                .flatten()
                .map(|actor| {
                    (
                        actor.actor_id,
                        InflightActorInfo {
                            worker_id: actor_location[&actor.actor_id],
                            vnode_bitmap: actor.vnode_bitmap.clone(),
                            splits: vec![],
                        },
                    )
                })
                .collect(),
            state_table_ids: self.new_fragment.state_table_ids.iter().copied().collect(),
        }
    }
}

/// [`ReplaceStreamJobContext`] carries one-time infos for replacing the plan of an existing stream job.
///
/// Note: for better readability, keep this struct complete and immutable once created.
pub struct ReplaceStreamJobContext {
    /// The old job fragments to be replaced.
    pub old_fragments: StreamJobFragments,

    /// The updates to be applied to the downstream chain actors. Used for schema change.
    pub replace_upstream: FragmentReplaceUpstream,

    /// New fragment relation to add from existing upstream fragment to downstream fragment.
    pub upstream_fragment_downstreams: FragmentDownstreamRelation,

    pub streaming_job: StreamingJob,

    /// The resource group of the database this job belongs to.
    pub database_resource_group: String,

    pub tmp_id: JobId,

    /// Used for dropping an associated source. Dropping source and related internal tables.
    pub drop_table_connector_ctx: Option<DropTableConnectorContext>,

    pub auto_refresh_schema_sinks: Option<Vec<AutoRefreshSchemaSinkContext>>,

    /// The `streaming_job::Model` for this job, loaded from meta store.
    pub streaming_job_model: streaming_job::Model,
}

/// `GlobalStreamManager` manages all the streams in the system.
pub struct GlobalStreamManager {
    pub env: MetaSrvEnv,

    pub metadata_manager: MetadataManager,

    /// Broadcasts and collect barriers
    pub barrier_scheduler: BarrierScheduler,

    pub hummock_manager: HummockManagerRef,

    /// Maintains streaming sources from external system like kafka
    pub source_manager: SourceManagerRef,

    pub refresh_manager: GlobalRefreshManagerRef,

    pub iceberg_compaction_manager: IcebergCompactionManagerRef,

    /// Creating streaming job info.
    creating_job_info: CreatingStreamingJobInfoRef,

    pub scale_controller: ScaleControllerRef,
}

impl GlobalStreamManager {
    pub fn new(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        barrier_scheduler: BarrierScheduler,
        hummock_manager: HummockManagerRef,
        source_manager: SourceManagerRef,
        refresh_manager: GlobalRefreshManagerRef,
        iceberg_compaction_manager: IcebergCompactionManagerRef,
        scale_controller: ScaleControllerRef,
    ) -> MetaResult<Self> {
        Ok(Self {
            env,
            metadata_manager,
            barrier_scheduler,
            hummock_manager,
            source_manager,
            refresh_manager,
            iceberg_compaction_manager,
            creating_job_info: Arc::new(CreatingStreamingJobInfo::default()),
            scale_controller,
        })
    }

    /// Create streaming job, it works as follows:
    ///
    /// 1. Broadcast the actor info based on the scheduling result in the context, build the hanging
    ///    channels in upstream worker nodes.
    /// 2. (optional) Get the split information of the `StreamSource` via source manager and patch
    ///    actors.
    /// 3. Notify related worker nodes to update and build the actors.
    /// 4. Store related meta data.
    ///
    /// This function is a wrapper over [`Self::run_create_streaming_job_command`].
    #[await_tree::instrument]
    pub async fn create_streaming_job(
        self: &Arc<Self>,
        stream_job_fragments: StreamJobFragmentsToCreate,
        ctx: CreateStreamingJobContext,
        permit: OwnedSemaphorePermit,
        reschedule_job_lock: RwLockReadGuard<'_, ()>,
    ) -> CreateStreamingJobResult {
        let await_tree_key = format!("Create Streaming Job Worker ({})", ctx.streaming_job.id());
        let await_tree_span = span!(
            "{:?}({})",
            ctx.streaming_job.job_type(),
            ctx.streaming_job.name()
        );

        let job_id = stream_job_fragments.stream_job_id();
        let database_id = ctx.streaming_job.database_id();

        let (cancel_tx, cancel_rx) = oneshot::channel();
        let execution = StreamingJobExecution::new(job_id, cancel_tx, permit);
        self.creating_job_info.add_job(execution).await;

        let stream_manager = self.clone();
        let fut = async move {
            let create_type = ctx.create_type;
            let streaming_job = stream_manager
                .run_create_streaming_job_command(stream_job_fragments, ctx)
                .await
                .map_err(|err| (err, false, None))?;
            // The create command has been collected, so rescheduling no longer conflicts with
            // planning or scheduling this job. In particular, do not hold this lock while a
            // foreground job waits through recovery.
            drop(reschedule_job_lock);
            let version = match create_type {
                CreateType::Background => {
                    stream_manager
                        .metadata_manager
                        .catalog_controller
                        .notify_frontend_trivial()
                        .await
                }
                CreateType::Foreground => {
                    let job_id = streaming_job.id() as _;
                    let wait_started_at = Instant::now();
                    loop {
                        match stream_manager
                            .metadata_manager
                            .wait_streaming_job_finished(database_id, job_id)
                            .await
                        {
                            Ok(version) => break version,
                            Err(err) if err.is_catalog_id_not_found("streaming job") => {
                                return Err((err, false, None));
                            }
                            Err(err)
                                if wait_started_at.elapsed()
                                    < FOREGROUND_DDL_EARLY_FAILURE_TIMEOUT =>
                            {
                                tracing::warn!(
                                    id = %job_id,
                                    error = %err.as_report(),
                                    elapsed = ?wait_started_at.elapsed(),
                                    "foreground streaming job failed shortly after waiting started; cancelling it"
                                );
                                return Err((err, true, None));
                            }
                            Err(err) => {
                                tracing::warn!(
                                    id = %job_id,
                                    error = %err.as_report(),
                                    "failed to wait for foreground streaming job; registering another finish notifier"
                                );
                            }
                        }
                    }
                }
                CreateType::Unspecified => unreachable!(),
            };

            tracing::debug!(?streaming_job, "stream job finish");
            Ok(version)
        }
        .in_current_span();

        let create_fut = (self.env.await_tree_reg())
            .register(await_tree_key, await_tree_span)
            .instrument(Box::pin(fut));

        let result = async {
            tokio::select! {
                biased;

                res = create_fut => res,
                notifier = cancel_rx => {
                    let notifier = notifier.expect("sender should not be dropped");
                    tracing::debug!(id=%job_id, "cancelling streaming job");

                    enum CancelResult {
                        Completed(CreateStreamingJobResult),
                        Cancelled { explicitly_cancel: bool },
                    }

                    let cancel_res = if let Ok(job_fragments) =
                        self.metadata_manager.get_job_fragments_by_id(job_id).await
                    {
                        // try to cancel buffered creating command.
                        if self
                            .barrier_scheduler
                            .try_cancel_scheduled_create(database_id, job_id)
                        {
                            tracing::debug!(
                                id=%job_id,
                                "cancelling streaming job in buffer queue."
                            );
                            CancelResult::Cancelled {
                                explicitly_cancel: false,
                            }
                        } else if !job_fragments.is_created() {
                            tracing::debug!(
                                id=%job_id,
                                "cancelling streaming job by issue cancel command."
                            );
                            CancelResult::Cancelled {
                                explicitly_cancel: true,
                            }
                        } else {
                            // streaming job is already completed
                            CancelResult::Completed(
                                self.metadata_manager
                                    .wait_streaming_job_finished(database_id, job_id)
                                    .await
                                    .map_err(|err| (err, false, None)),
                            )
                        }
                    } else {
                        CancelResult::Cancelled {
                            explicitly_cancel: false,
                        }
                    };

                    match cancel_res {
                        CancelResult::Completed(result) => {
                            let _ = notifier.send(false).inspect_err(|err| {
                                tracing::warn!("failed to notify cancellation result: {err}")
                            });
                            result
                        }
                        CancelResult::Cancelled { explicitly_cancel } => {
                            Err((MetaError::cancelled("create"), explicitly_cancel, Some(notifier)))
                        }
                    }
                }
            }
        }
        .await;

        tracing::debug!("cleaning creating job info: {}", job_id);
        self.creating_job_info.delete_job(job_id).await;
        result
    }

    async fn provision_serverless_backfill_resource_group(&self) -> MetaResult<String> {
        let sbc_addr = &self.env.opts.serverless_backfill_controller_addr;
        if sbc_addr.is_empty() {
            bail_invalid_parameter!(
                "Serverless Backfill is disabled. Use RisingWave cloud at https://cloud.risingwave.com/auth/signup to try this feature"
            );
        }

        let request = tonic::Request::new(ProvisionRequest {});
        let mut client =
            node_group_controller_service_client::NodeGroupControllerServiceClient::connect(
                sbc_addr.clone(),
            )
            .await
            .with_context(|| {
                format!(
                    "unable to reach serverless backfill controller at addr {}",
                    sbc_addr
                )
            })?;

        match client.provision(request).await {
            Ok(resp) => Ok(resp.into_inner().resource_group),
            Err(e) => Err(anyhow::Error::new(TonicStatusWrapper::new(e))
                .context("serverless backfill controller returned error")
                .into()),
        }
    }

    async fn finalize_create_streaming_job_resource_group(
        &self,
        resource_type: &streaming_job_resource_type::ResourceType,
        streaming_job_model: &mut streaming_job::Model,
    ) -> MetaResult<()> {
        if !matches!(
            resource_type,
            streaming_job_resource_type::ResourceType::ServerlessBackfill(true)
        ) {
            return Ok(());
        }

        let group = self.provision_serverless_backfill_resource_group().await?;
        tracing::info!(
            resource_group = group,
            "provisioning serverless backfill resource group"
        );

        self.metadata_manager
            .catalog_controller
            .update_streaming_job_resource_group(streaming_job_model.job_id, group.clone())
            .await?;
        streaming_job_model.specific_resource_group = Some(group);

        Ok(())
    }

    /// The function will return after barrier collected
    /// ([`crate::manager::MetadataManager::wait_streaming_job_finished`]).
    #[await_tree::instrument]
    async fn run_create_streaming_job_command(
        &self,
        stream_job_fragments: StreamJobFragmentsToCreate,
        CreateStreamingJobContext {
            streaming_job,
            upstream_fragment_downstreams,
            database_resource_group,
            definition,
            create_type,
            job_type,
            new_upstream_sink,
            snapshot_backfill_info,
            cross_db_snapshot_backfill_info,
            fragment_backfill_ordering,
            locality_fragment_state_table_mapping,
            cdc_table_snapshot_splits,
            is_serverless_backfill,
            resource_type,
            mut streaming_job_model,
            replace_sink,
            refresh_interval_sec,
            since_timestamp_epoch,
            ..
        }: CreateStreamingJobContext,
    ) -> MetaResult<StreamingJob> {
        tracing::debug!(
            table_id = %stream_job_fragments.stream_job_id(),
            "built actors finished"
        );

        // Phase 1: Gather fragment-level split information.
        // - For source fragments: discover splits from the external source.
        // - For backfill fragments: splits will be aligned in Phase 2 inside the barrier worker
        //   using the actor-level no-shuffle mapping produced by render_actors.
        let init_split_assignment = self
            .source_manager
            .discover_splits(&stream_job_fragments)
            .await?;

        let fragment_backfill_ordering =
            StreamFragmentGraph::extend_fragment_backfill_ordering_with_locality_backfill(
                fragment_backfill_ordering,
                &stream_job_fragments.downstreams,
                || {
                    stream_job_fragments
                        .fragments
                        .iter()
                        .map(|(fragment_id, fragment)| {
                            (*fragment_id, fragment.fragment_type_mask, &fragment.nodes)
                        })
                },
            );

        self.finalize_create_streaming_job_resource_group(&resource_type, &mut streaming_job_model)
            .await?;

        let info = CreateStreamingJobCommandInfo {
            stream_job_fragments,
            upstream_fragment_downstreams,
            init_split_assignment,
            definition: definition.clone(),
            streaming_job: streaming_job.clone(),
            job_type,
            create_type,
            database_resource_group,
            fragment_backfill_ordering,
            cdc_table_snapshot_splits,
            locality_fragment_state_table_mapping,
            is_serverless: is_serverless_backfill,
            streaming_job_model,
            replace_sink,
            refresh_interval_sec,
        };

        let job_type = if let Some(refresh_interval_sec) = refresh_interval_sec {
            if since_timestamp_epoch.is_some() {
                bail!("since_timestamp should not be specified when no snapshot backfill");
            }
            let snapshot_backfill_info = snapshot_backfill_info.ok_or_else(|| {
                anyhow::anyhow!(
                    "batch refresh materialized view must have snapshot backfill upstream"
                )
            })?;
            // Batch refresh jobs must not contain source or source-backfill nodes,
            // because we skip split assignment resolution for them.
            for fragment in info.stream_job_fragments.inner.fragments.values() {
                let mask = fragment.fragment_type_mask;
                if mask.contains(FragmentTypeFlag::Source)
                    || mask.contains(FragmentTypeFlag::SourceScan)
                {
                    bail!(
                        "batch refresh materialized views must not depend on sources directly; \
                         fragment {} has source/source-backfill nodes",
                        fragment.fragment_id
                    );
                }
            }
            tracing::debug!(
                ?snapshot_backfill_info,
                refresh_interval_sec,
                "sending Command::CreateBatchRefreshStreamingJob"
            );
            CreateStreamingJobType::BatchRefresh(BatchRefreshInfo {
                snapshot_backfill_info,
                refresh_interval_sec,
            })
        } else if let Some(snapshot_backfill_info) = snapshot_backfill_info {
            tracing::debug!(
                ?snapshot_backfill_info,
                "sending Command::CreateSnapshotBackfillStreamingJob"
            );
            CreateStreamingJobType::SnapshotBackfill {
                snapshot_backfill_info,
                since_epoch: since_timestamp_epoch.map(|provided_since_epoch| SinceEpochInfo {
                    provided_since_epoch,
                    resolved: None,
                }),
            }
        } else {
            if since_timestamp_epoch.is_some() {
                bail!("since_timestamp should not be specified when no snapshot backfill");
            }
            tracing::debug!("sending Command::CreateStreamingJob");
            if let Some(new_upstream_sink) = new_upstream_sink {
                CreateStreamingJobType::SinkIntoTable(new_upstream_sink)
            } else {
                CreateStreamingJobType::Normal
            }
        };

        let command = Command::CreateStreamingJob {
            info,
            job_type,
            cross_db_snapshot_backfill_info,
        };

        self.barrier_scheduler
            .run_command(streaming_job.database_id(), command)
            .await?;

        tracing::debug!(?streaming_job, "first barrier collected for stream job");

        Ok(streaming_job)
    }

    /// Send replace job command to barrier scheduler.
    pub async fn replace_stream_job(
        &self,
        new_fragments: StreamJobFragmentsToCreate,
        ReplaceStreamJobContext {
            old_fragments,
            replace_upstream,
            upstream_fragment_downstreams,
            tmp_id,
            streaming_job,
            drop_table_connector_ctx,
            auto_refresh_schema_sinks,
            streaming_job_model,
            database_resource_group,
        }: ReplaceStreamJobContext,
    ) -> MetaResult<()> {
        // Phase 1: Gather fragment-level split information.
        // For replace source with existing downstream, splits will be aligned
        // in Phase 2 inside the barrier worker using actor-level no-shuffle produced by render_actors.
        // For replace source with no downstream (or non-source), discover splits fresh.
        let split_plan = if streaming_job.is_source() {
            match self
                .source_manager
                .discover_splits_for_replace_source(&new_fragments, &replace_upstream)
                .await?
            {
                Some(discovered) => ReplaceJobSplitPlan::Discovered(discovered),
                None => ReplaceJobSplitPlan::AlignFromPrevious,
            }
        } else {
            let discovered = self.source_manager.discover_splits(&new_fragments).await?;
            ReplaceJobSplitPlan::Discovered(discovered)
        };
        tracing::info!("replace_stream_job - split plan: {:?}", split_plan);

        self.barrier_scheduler
            .run_command(
                streaming_job.database_id(),
                Command::ReplaceStreamJob(ReplaceStreamJobPlan {
                    old_fragments,
                    new_fragments,
                    database_resource_group,
                    replace_upstream,
                    upstream_fragment_downstreams,
                    split_plan,
                    streaming_job,
                    streaming_job_model,
                    tmp_id,
                    to_drop_state_table_ids: {
                        if let Some(drop_table_connector_ctx) = &drop_table_connector_ctx {
                            vec![drop_table_connector_ctx.to_remove_state_table_id]
                        } else {
                            Vec::new()
                        }
                    },
                    auto_refresh_schema_sinks,
                }),
            )
            .await?;

        Ok(())
    }

    /// Drop streaming jobs by barrier manager, and clean up all related resources. The error will
    /// be ignored because the recovery process will take over it in cleaning part. Check
    /// [`Command::DropStreamingJobs`] for details.
    pub async fn drop_streaming_jobs(
        &self,
        database_id: DatabaseId,
        streaming_job_ids: Vec<JobId>,
        state_table_ids: Vec<TableId>,
        dropped_sink_fragment_by_targets: HashMap<FragmentId, Vec<FragmentId>>,
    ) {
        if !streaming_job_ids.is_empty() || !state_table_ids.is_empty() {
            let cleanup_streaming_job_ids = streaming_job_ids.clone();
            let cleanup_state_table_ids = state_table_ids.clone();
            let run_result = self
                .barrier_scheduler
                .run_command(
                    database_id,
                    Command::DropStreamingJobs {
                        streaming_job_ids: streaming_job_ids.into_iter().collect(),
                        unregistered_state_table_ids: state_table_ids.iter().copied().collect(),
                        dropped_sink_fragment_by_targets,
                    },
                )
                .await;
            let result = match run_result {
                Ok(()) => {
                    cleanup_dropped_streaming_jobs(
                        &self.refresh_manager,
                        &self.hummock_manager,
                        &self.metadata_manager,
                        cleanup_streaming_job_ids,
                        cleanup_state_table_ids,
                        "drop_streaming_jobs",
                    )
                    .await
                }
                Err(err) => Err(err),
            };
            let _ = result.inspect_err(|err| {
                tracing::error!(error = ?err.as_report(), "failed to run drop command");
            });
        }
    }

    /// Cancel streaming jobs and return the canceled table ids.
    /// 1. Send cancel message to stream jobs (via `cancel_jobs`).
    /// 2. Send cancel message to recovered stream jobs (via `barrier_scheduler`).
    ///
    /// Cleanup of their state is handled by the caller after the drop command is collected.
    pub async fn cancel_streaming_jobs(&self, job_ids: Vec<JobId>) -> MetaResult<Vec<JobId>> {
        if job_ids.is_empty() {
            return Ok(vec![]);
        }

        let _reschedule_job_lock = self.reschedule_lock_read_guard().await;
        let (receivers, background_job_ids) = self.creating_job_info.cancel_jobs(job_ids).await?;

        let futures = receivers.into_iter().map(|(id, receiver)| async move {
            if let Ok(cancelled) = receiver.await
                && cancelled
            {
                tracing::info!("canceled streaming job {id}");
                Ok(id)
            } else {
                Err(MetaError::from(anyhow::anyhow!(
                    "failed to cancel streaming job {id}"
                )))
            }
        });
        let mut cancelled_ids = join_all(futures)
            .await
            .into_iter()
            .collect::<MetaResult<Vec<_>>>()?;

        // NOTE(kwannoel): For background_job_ids stream jobs that not tracked in streaming manager,
        // we can directly cancel them by running the barrier command.
        let futures = background_job_ids.into_iter().map(|id| async move {
            let abort_result = self
                .metadata_manager
                .catalog_controller
                .try_abort_creating_streaming_job(id, true)
                .await?;
            self.iceberg_compaction_manager
                .clear_maintenance_for_aborted_job(&abort_result);
            let Some(cancel_info) = abort_result.cancel_info else {
                return Ok(None);
            };

            if let Some(database_id) = abort_result.database_id {
                self.barrier_scheduler
                    .run_command(database_id, cancel_info.command)
                    .await?;
                cleanup_dropped_streaming_jobs(
                    &self.refresh_manager,
                    &self.hummock_manager,
                    &self.metadata_manager,
                    cancel_info.streaming_job_ids,
                    cancel_info.state_table_ids,
                    "cancel_streaming_job",
                )
                .await?;
            }

            tracing::info!(?id, "cancelled background streaming job");
            Ok(Some(id))
        });
        let cancelled_recovered_ids = join_all(futures)
            .await
            .into_iter()
            .collect::<MetaResult<Vec<_>>>()?;

        cancelled_ids.extend(cancelled_recovered_ids.into_iter().flatten());
        Ok(cancelled_ids)
    }

    pub(crate) async fn reschedule_streaming_job(
        &self,
        job_id: JobId,
        policy: ReschedulePolicy,
        deferred: bool,
    ) -> MetaResult<()> {
        let _reschedule_job_lock = self.reschedule_lock_write_guard().await;

        let creating_jobs = self.metadata_manager.list_creating_jobs().await?;

        if !creating_jobs.is_empty() {
            let blocked_jobs = self
                .metadata_manager
                .collect_reschedule_blocked_jobs_for_creating_jobs(&creating_jobs, !deferred)
                .await?;

            if blocked_jobs.contains(&job_id) {
                bail!(
                    "Cannot alter the job {} because it is blocked by creating unreschedulable backfill jobs",
                    job_id,
                );
            }
        }

        let commands = self
            .scale_controller
            .reschedule_inplace(HashMap::from([(job_id, policy)]))
            .await?;

        if !deferred {
            let _source_pause_guard = self.source_manager.pause_tick().await;

            for (database_id, command) in commands {
                self.barrier_scheduler
                    .run_command(database_id, command)
                    .await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn reschedule_streaming_job_backfill_parallelism(
        &self,
        job_id: JobId,
        parallelism: Option<ParallelismPolicy>,
        deferred: bool,
    ) -> MetaResult<()> {
        let _reschedule_job_lock = self.reschedule_lock_write_guard().await;

        if !deferred {
            let creating_jobs = self.metadata_manager.list_creating_jobs().await?;

            if !creating_jobs.is_empty() {
                let jobs_with_unreschedulable_scan = self
                    .metadata_manager
                    .collect_online_unreschedulable_backfill_jobs(&creating_jobs)
                    .await?;

                if jobs_with_unreschedulable_scan.contains(&job_id) {
                    bail!(
                        "Cannot alter the job {} because its creating backfill contains a scan type that does not support online rescheduling",
                        job_id,
                    );
                }
            }
        }

        let commands = self
            .scale_controller
            .reschedule_backfill_parallelism_inplace(HashMap::from([(job_id, parallelism)]))
            .await?;

        if !deferred {
            let _source_pause_guard = self.source_manager.pause_tick().await;

            for (database_id, command) in commands {
                self.barrier_scheduler
                    .run_command(database_id, command)
                    .await?;
            }
        }

        Ok(())
    }

    /// This method is copied from `GlobalStreamManager::reschedule_streaming_job` and modified to handle reschedule CDC table backfill.
    pub(crate) async fn reschedule_cdc_table_backfill(
        &self,
        job_id: JobId,
        target: ReschedulePolicy,
    ) -> MetaResult<()> {
        let _reschedule_job_lock = self.reschedule_lock_write_guard().await;

        let parallelism_policy = match target {
            ReschedulePolicy::Parallelism(policy)
                if matches!(policy.parallelism, StreamingParallelism::Fixed(_)) =>
            {
                policy
            }
            _ => bail_invalid_parameter!(
                "CDC backfill reschedule only supports fixed parallelism targets"
            ),
        };

        let cdc_fragment_id = {
            let inner = self.metadata_manager.catalog_controller.inner.read().await;
            let fragments: Vec<(risingwave_meta_model::FragmentId, i32)> = FragmentModel::find()
                .select_only()
                .columns([
                    fragment::Column::FragmentId,
                    fragment::Column::FragmentTypeMask,
                ])
                .filter(fragment::Column::JobId.eq(job_id))
                .into_tuple()
                .all(&inner.db)
                .await?;

            let cdc_fragments = fragments
                .into_iter()
                .filter_map(|(fragment_id, mask)| {
                    FragmentTypeMask::from(mask)
                        .contains(FragmentTypeFlag::StreamCdcScan)
                        .then_some(fragment_id)
                })
                .collect_vec();

            match cdc_fragments.len() {
                0 => bail_invalid_parameter!("no StreamCdcScan fragments found for job {}", job_id),
                1 => cdc_fragments[0],
                _ => bail_invalid_parameter!(
                    "multiple StreamCdcScan fragments found for job {}; expected exactly one",
                    job_id
                ),
            }
        };

        let fragment_policy = HashMap::from([(
            cdc_fragment_id,
            Some(parallelism_policy.parallelism.clone()),
        )]);

        let commands = self
            .scale_controller
            .reschedule_fragment_inplace(fragment_policy)
            .await?;

        let _source_pause_guard = self.source_manager.pause_tick().await;

        for (database_id, command) in commands {
            self.barrier_scheduler
                .run_command(database_id, command)
                .await?;
        }

        Ok(())
    }

    pub(crate) async fn reschedule_fragments(
        &self,
        fragment_targets: HashMap<FragmentId, Option<StreamingParallelism>>,
    ) -> MetaResult<()> {
        if fragment_targets.is_empty() {
            return Ok(());
        }

        let _reschedule_job_lock = self.reschedule_lock_write_guard().await;

        let fragment_policy = fragment_targets
            .into_iter()
            .map(|(fragment_id, parallelism)| (fragment_id as _, parallelism))
            .collect();

        let commands = self
            .scale_controller
            .reschedule_fragment_inplace(fragment_policy)
            .await?;

        let _source_pause_guard = self.source_manager.pause_tick().await;

        for (database_id, command) in commands {
            self.barrier_scheduler
                .run_command(database_id, command)
                .await?;
        }

        Ok(())
    }

    // Don't need to add actor, just send a command
    pub async fn create_subscription(
        self: &Arc<Self>,
        subscription: &Subscription,
    ) -> MetaResult<()> {
        let command = Command::CreateSubscription {
            subscription_id: subscription.id,
            upstream_mv_table_id: subscription.dependent_table_id,
            retention_second: subscription.retention_seconds,
        };

        tracing::debug!("sending Command::CreateSubscription");
        self.barrier_scheduler
            .run_command(subscription.database_id, command)
            .await?;
        Ok(())
    }

    // Don't need to add actor, just send a command
    pub async fn drop_subscription(
        self: &Arc<Self>,
        database_id: DatabaseId,
        subscription_id: SubscriptionId,
        table_id: TableId,
    ) {
        let command = Command::DropSubscription {
            subscription_id,
            upstream_mv_table_id: table_id,
        };

        tracing::debug!("sending Command::DropSubscriptions");
        let _ = self
            .barrier_scheduler
            .run_command(database_id, command)
            .await
            .inspect_err(|err| {
                tracing::error!(error = ?err.as_report(), "failed to run drop command");
            });
    }

    pub async fn alter_subscription_retention(
        self: &Arc<Self>,
        database_id: DatabaseId,
        subscription_id: SubscriptionId,
        table_id: TableId,
        retention_second: u64,
    ) -> MetaResult<()> {
        let command = Command::AlterSubscriptionRetention {
            subscription_id,
            upstream_mv_table_id: table_id,
            retention_second,
        };

        tracing::debug!("sending Command::AlterSubscriptionRetention");
        self.barrier_scheduler
            .run_command(database_id, command)
            .await?;
        Ok(())
    }
}
