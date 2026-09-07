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

use anyhow::anyhow;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_pb::catalog::Database;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::meta::PbRecoveryStatus;
use tokio::sync::oneshot::Sender;

use self::notifier::Notifier;
use crate::barrier::info::BarrierInfo;
use crate::manager::ActiveStreamingWorkerNodes;
use crate::model::{ActorId, BackfillUpstreamType, FragmentId, StreamActor, SubscriptionId};
use crate::{MetaError, MetaResult};

mod backfill_order_control;
pub mod cdc_progress;
mod checkpoint;
mod command;
pub use command::RescheduleContext;
mod complete_task;
pub(super) mod context;
mod edge_builder;
mod info;
mod manager;
mod notifier;
mod partial_graph;
mod progress;
mod rpc;
mod schedule;
#[cfg(test)]
mod tests;
mod trace;
mod utils;
mod worker;

pub use backfill_order_control::{BackfillNode, BackfillOrderState};
use risingwave_common::id::JobId;
use risingwave_pb::ddl_service::PbBackfillType;

pub use self::command::{
    BarrierKind, BatchRefreshInfo, Command, CreateStreamingJobCommandInfo, CreateStreamingJobType,
    ReplaceStreamJobPlan, Reschedule, ReschedulePlan, ResumeBackfillTarget, SinceEpochInfo,
    SnapshotBackfillInfo,
};
pub(crate) use self::info::{SharedActorInfos, SharedFragmentInfo};
pub use self::manager::{BarrierManagerRef, GlobalBarrierManager};
pub(crate) use self::rpc::to_partial_graph_id;
pub use self::schedule::BarrierScheduler;
pub use self::trace::TracedEpoch;
use crate::barrier::cdc_progress::CdcProgress;
use crate::barrier::context::recovery::LoadedRecoveryContext;
use crate::controller::fragment::InflightFragmentInfo;
use crate::stream::cdc::CdcTableSnapshotSplits;

/// The reason why the cluster is recovering.
enum RecoveryReason {
    /// After bootstrap.
    Bootstrap,
    /// After failure.
    Failover(MetaError),
    /// Manually triggered
    Adhoc,
}

/// Status of barrier manager.
enum BarrierManagerStatus {
    /// Barrier manager is starting.
    Starting,
    /// Barrier manager is under recovery.
    Recovering(RecoveryReason),
    /// Barrier manager is running.
    Running,
}

/// Scheduled command with its notifier.
struct Scheduled {
    database_id: DatabaseId,
    command: Command,
    notifier: Notifier,
    span: tracing::Span,
}

impl From<&BarrierManagerStatus> for PbRecoveryStatus {
    fn from(status: &BarrierManagerStatus) -> Self {
        match status {
            BarrierManagerStatus::Starting => Self::StatusStarting,
            BarrierManagerStatus::Recovering(reason) => match reason {
                RecoveryReason::Bootstrap => Self::StatusStarting,
                RecoveryReason::Failover(_) | RecoveryReason::Adhoc => Self::StatusRecovering,
            },
            BarrierManagerStatus::Running => Self::StatusRunning,
        }
    }
}

pub(crate) struct BackfillProgress {
    pub(crate) progress: String,
    pub(crate) backfill_type: PbBackfillType,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FragmentBackfillProgress {
    pub(crate) job_id: JobId,
    pub(crate) fragment_id: FragmentId,
    pub(crate) consumed_rows: u64,
    pub(crate) done: bool,
    pub(crate) upstream_type: BackfillUpstreamType,
}

pub(crate) struct UpdateDatabaseBarrierRequest {
    pub database_id: DatabaseId,
    pub barrier_interval_ms: Option<u32>,
    pub checkpoint_frequency: Option<u64>,
    pub sender: Sender<()>,
}

pub(crate) enum BarrierManagerRequest {
    GetBackfillProgress(Sender<MetaResult<HashMap<JobId, BackfillProgress>>>),
    GetFragmentBackfillProgress(Sender<MetaResult<Vec<FragmentBackfillProgress>>>),
    GetCdcProgress(Sender<MetaResult<HashMap<JobId, CdcProgress>>>),
    AdhocRecovery {
        database_id: Option<DatabaseId>,
        sender: Sender<()>,
    },
    UpdateDatabaseBarrier(UpdateDatabaseBarrierRequest),
}

#[derive(Debug)]
struct BarrierWorkerRuntimeInfoSnapshot {
    active_streaming_nodes: ActiveStreamingWorkerNodes,
    recovery_context: LoadedRecoveryContext,
    state_table_committed_epochs: HashMap<TableId, u64>,
    /// `table_id` -> (`Vec<non-checkpoint epoch>`, checkpoint epoch)
    state_table_log_epochs: HashMap<TableId, Vec<(Vec<u64>, u64)>>,
    mv_depended_subscriptions: HashMap<TableId, HashMap<SubscriptionId, u64>>,
    creating_jobs: HashSet<JobId>,
    hummock_version_stats: HummockVersionStats,
    database_infos: Vec<Database>,
    cdc_table_snapshot_splits: HashMap<JobId, CdcTableSnapshotSplits>,
}

impl BarrierWorkerRuntimeInfoSnapshot {
    fn validate_database_info(
        database_id: DatabaseId,
        database_jobs: &HashMap<JobId, HashMap<FragmentId, InflightFragmentInfo>>,
        active_streaming_nodes: &ActiveStreamingWorkerNodes,
        stream_actors: &HashMap<ActorId, StreamActor>,
        state_table_committed_epochs: &HashMap<TableId, u64>,
    ) -> MetaResult<()> {
        {
            for fragment in database_jobs.values().flat_map(|job| job.values()) {
                for (actor_id, actor) in &fragment.actors {
                    if !active_streaming_nodes
                        .current()
                        .contains_key(&actor.worker_id)
                    {
                        return Err(anyhow!(
                            "worker_id {} for actor {} does not exist",
                            actor.worker_id,
                            actor_id
                        )
                        .into());
                    }
                    if !stream_actors.contains_key(actor_id) {
                        return Err(anyhow!("cannot find StreamActor of actor {}", actor_id).into());
                    }
                }
                for state_table_id in &fragment.state_table_ids {
                    if !state_table_committed_epochs.contains_key(state_table_id) {
                        return Err(anyhow!(
                            "state table {} is not registered to hummock",
                            state_table_id
                        )
                        .into());
                    }
                }
            }
            for (job_id, fragments) in database_jobs {
                let mut committed_epochs =
                    InflightFragmentInfo::existing_table_ids(fragments.values()).map(|table_id| {
                        (
                            table_id,
                            *state_table_committed_epochs
                                .get(&table_id)
                                .expect("checked exist"),
                        )
                    });
                let (first_table, first_epoch) = committed_epochs.next().ok_or_else(|| {
                    anyhow!(
                        "job {} in database {} has no state table after recovery",
                        job_id,
                        database_id
                    )
                })?;
                for (table_id, epoch) in committed_epochs {
                    if epoch != first_epoch {
                        return Err(anyhow!(
                            "job {} in database {} has tables with different table ids. {}:{}, {}:{}",
                            job_id,
                            database_id,
                            first_table,
                            first_epoch,
                            table_id,
                            epoch
                        )
                        .into());
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct DatabaseRuntimeInfoSnapshot {
    recovery_context: LoadedRecoveryContext,
    state_table_committed_epochs: HashMap<TableId, u64>,
    /// `table_id` -> (`Vec<non-checkpoint epoch>`, checkpoint epoch)
    state_table_log_epochs: HashMap<TableId, Vec<(Vec<u64>, u64)>>,
    mv_depended_subscriptions: HashMap<TableId, HashMap<SubscriptionId, u64>>,
    creating_jobs: HashSet<JobId>,
    cdc_table_snapshot_splits: HashMap<JobId, CdcTableSnapshotSplits>,
}
