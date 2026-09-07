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

mod context_impl;
pub(crate) mod recovery;

use std::future::Future;
use std::sync::Arc;

use arc_swap::ArcSwap;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::id::{JobId, PartialGraphId};
use risingwave_meta_model::SinkId;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_service::barrier_complete_response::{
    IcebergPkIndexSinkMetadata as PbIcebergPkIndexSinkMetadata, PbListFinishedSource,
    PbLoadFinishedSource,
};
use risingwave_rpc_client::StreamingControlHandle;

use crate::MetaResult;
use crate::barrier::checkpoint::independent_job::BatchRefreshJobTriggerContext;
use crate::barrier::command::{PostCollectCommand, SinceTimestampResolvedEpoch};
use crate::barrier::progress::TrackingJob;
use crate::barrier::schedule::{MarkReadyOptions, ScheduledBarriers};
use crate::barrier::{
    BarrierManagerStatus, BarrierScheduler, BarrierWorkerRuntimeInfoSnapshot, BatchRefreshInfo,
    CreateStreamingJobCommandInfo, CreateStreamingJobType, DatabaseRuntimeInfoSnapshot,
    RecoveryReason, Scheduled, SnapshotBackfillInfo,
};
use crate::hummock::{CommitEpochInfo, HummockManagerRef};
use crate::manager::iceberg_compaction::IcebergCompactionManagerRef;
use crate::manager::iceberg_pk_index_sink::IcebergPkIndexSinkManager;
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{MetaSrvEnv, MetadataManager};
use crate::serving::ServingVnodeMappingRef;
use crate::stream::source_manager::SplitAssignment;
use crate::stream::{GlobalRefreshManagerRef, ScaleControllerRef, SourceManagerRef};

#[derive(Debug)]
pub(super) struct CreateSnapshotBackfillJobCommandInfo {
    pub info: CreateStreamingJobCommandInfo,
    pub snapshot_backfill_info: SnapshotBackfillInfo,
    pub cross_db_snapshot_backfill_info: SnapshotBackfillInfo,
    pub resolved_split_assignment: SplitAssignment,
    /// If set, this is a batch refresh job rather than a regular snapshot backfill.
    pub refresh_interval_sec: Option<u64>,
}

impl CreateSnapshotBackfillJobCommandInfo {
    pub(super) fn into_post_collect(self) -> PostCollectCommand {
        let job_type = if let Some(refresh_interval_sec) = self.refresh_interval_sec {
            CreateStreamingJobType::BatchRefresh(BatchRefreshInfo {
                snapshot_backfill_info: self.snapshot_backfill_info,
                refresh_interval_sec,
            })
        } else {
            CreateStreamingJobType::SnapshotBackfill {
                snapshot_backfill_info: self.snapshot_backfill_info,
                // `since_epoch` is only used before job creation barriers are injected, and
                // post-collect snapshot backfill does not go through that path.
                since_epoch: None,
            }
        };
        PostCollectCommand::CreateStreamingJob {
            info: self.info,
            job_type,
            cross_db_snapshot_backfill_info: self.cross_db_snapshot_backfill_info,
            resolved_split_assignment: self.resolved_split_assignment,
        }
    }
}

pub(super) trait GlobalBarrierWorkerContext: Send + Sync + 'static {
    fn commit_epoch(
        &self,
        commit_info: CommitEpochInfo,
    ) -> impl Future<Output = MetaResult<HummockVersionStats>> + Send + '_;

    async fn next_scheduled(&self) -> Scheduled;
    fn abort_and_mark_blocked(
        &self,
        database_id: Option<DatabaseId>,
        recovery_reason: RecoveryReason,
    );
    fn mark_ready(&self, options: MarkReadyOptions);
    fn resolve_log_store_epoch<'a>(
        &'a self,
        upstream_table_ids: impl Iterator<Item = TableId> + Send + 'a,
        since_epoch: u64,
    ) -> impl Future<Output = MetaResult<SinceTimestampResolvedEpoch>> + Send + 'a;

    async fn refresh_table_refill_runtime_state_after_recovery(&self) -> MetaResult<()> {
        Ok(())
    }

    fn post_collect_command(
        &self,
        command: PostCollectCommand,
    ) -> impl Future<Output = MetaResult<()>> + Send + '_;

    fn notify_creating_job_failed(
        &self,
        database_id: Option<DatabaseId>,
        err: String,
    ) -> impl Future<Output = ()> + Send + '_;

    fn finish_creating_job(
        &self,
        job: TrackingJob,
    ) -> impl Future<Output = MetaResult<()>> + Send + '_;

    fn finish_cdc_table_backfill(
        &self,
        job_id: JobId,
    ) -> impl Future<Output = MetaResult<()>> + Send + '_;

    fn new_control_stream<'a>(
        &'a self,
        node: &'a WorkerNode,
    ) -> impl Future<Output = MetaResult<StreamingControlHandle>> + Send + 'a;

    fn reload_runtime_info(
        &self,
    ) -> impl Future<Output = MetaResult<BarrierWorkerRuntimeInfoSnapshot>> + Send + '_;

    async fn reload_database_runtime_info(
        &self,
        database_id: DatabaseId,
    ) -> MetaResult<DatabaseRuntimeInfoSnapshot>;

    fn handle_list_finished_source_ids(
        &self,
        list_finished_source_ids: Vec<PbListFinishedSource>,
    ) -> impl Future<Output = MetaResult<()>> + Send + '_;

    fn handle_load_finished_source_ids(
        &self,
        load_finished_source_ids: Vec<PbLoadFinishedSource>,
    ) -> impl Future<Output = MetaResult<()>> + Send + '_;

    fn handle_refresh_finished_table_ids(
        &self,
        refresh_finished_table_job_ids: Vec<JobId>,
    ) -> impl Future<Output = MetaResult<()>> + Send + '_;

    /// Load the trigger context for a batch refresh job: fragment metadata, job model,
    /// upstream log epochs, and target upstream epoch — all bundled in one struct.
    fn load_batch_refresh_trigger_context(
        &self,
        job_id: JobId,
        database_id: DatabaseId,
        last_committed_epoch: u64,
    ) -> impl Future<Output = MetaResult<BatchRefreshJobTriggerContext>> + Send + '_;

    fn pre_commit_iceberg_pk_index_sink_metadata(
        &self,
        reports: Vec<PbIcebergPkIndexSinkMetadata>,
    ) -> impl Future<Output = MetaResult<Vec<SinkId>>> + Send + '_;

    fn commit_iceberg_pk_index_sink_metadata(
        &self,
        sink_ids: Vec<SinkId>,
    ) -> impl Future<Output = MetaResult<()>> + Send + '_;

    /// Advance per-database pk-index committed epochs after a checkpoint completion.
    fn advance_iceberg_pk_index_sink_committed_epochs(
        &self,
        epochs: impl IntoIterator<Item = (PartialGraphId, u64)>,
    );
}

pub(super) struct GlobalBarrierWorkerContextImpl {
    scheduled_barriers: ScheduledBarriers,

    status: Arc<ArcSwap<BarrierManagerStatus>>,

    pub(super) metadata_manager: MetadataManager,

    hummock_manager: HummockManagerRef,

    serving_vnode_mapping: ServingVnodeMappingRef,

    source_manager: SourceManagerRef,

    _scale_controller: ScaleControllerRef,

    pub(super) env: MetaSrvEnv,

    /// Barrier scheduler for scheduling load finish commands
    barrier_scheduler: BarrierScheduler,

    pub(super) refresh_manager: GlobalRefreshManagerRef,

    sink_manager: SinkCoordinatorManager,

    pub(super) iceberg_pk_index_sink_manager: IcebergPkIndexSinkManager,

    pub(super) iceberg_compaction_manager: IcebergCompactionManagerRef,
}

impl GlobalBarrierWorkerContextImpl {
    #[expect(clippy::too_many_arguments)]
    pub(super) fn new(
        scheduled_barriers: ScheduledBarriers,
        status: Arc<ArcSwap<BarrierManagerStatus>>,
        metadata_manager: MetadataManager,
        hummock_manager: HummockManagerRef,
        serving_vnode_mapping: ServingVnodeMappingRef,
        source_manager: SourceManagerRef,
        scale_controller: ScaleControllerRef,
        env: MetaSrvEnv,
        barrier_scheduler: BarrierScheduler,
        refresh_manager: GlobalRefreshManagerRef,
        sink_manager: SinkCoordinatorManager,
        iceberg_pk_index_sink_manager: IcebergPkIndexSinkManager,
        iceberg_compaction_manager: IcebergCompactionManagerRef,
    ) -> Self {
        Self {
            scheduled_barriers,
            status,
            metadata_manager,
            hummock_manager,
            serving_vnode_mapping,
            source_manager,
            _scale_controller: scale_controller,
            env,
            barrier_scheduler,
            refresh_manager,
            sink_manager,
            iceberg_pk_index_sink_manager,
            iceberg_compaction_manager,
        }
    }

    pub(super) fn status(&self) -> Arc<ArcSwap<BarrierManagerStatus>> {
        self.status.clone()
    }
}
