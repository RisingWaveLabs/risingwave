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

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use arc_swap::ArcSwap;
use risingwave_common::bail;
use risingwave_common::cast::datetime_to_timestamp_millis;
use risingwave_hummock_sdk::HummockVersionId;
use risingwave_meta_model::DatabaseId;
use risingwave_pb::ddl_service::{DdlProgress, PbBackfillType};
use risingwave_pb::id::JobId;
use risingwave_pb::meta::PbRecoveryStatus;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::MetaResult;
use crate::barrier::cdc_progress::CdcProgress;
use crate::barrier::worker::GlobalBarrierWorker;
use crate::barrier::{
    BackfillProgress, BarrierManagerRequest, BarrierManagerStatus, FragmentBackfillProgress,
    RecoveryReason, UpdateDatabaseBarrierRequest, schedule,
};
use crate::hummock::HummockManagerRef;
use crate::manager::iceberg_compaction::IcebergCompactionManagerRef;
use crate::manager::iceberg_pk_index_sink::IcebergPkIndexSinkManager;
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{MetaSrvEnv, MetadataManager};
use crate::serving::ServingVnodeMappingRef;
use crate::stream::{GlobalRefreshManagerRef, ScaleControllerRef, SourceManagerRef};

pub struct GlobalBarrierManager {
    status: Arc<ArcSwap<BarrierManagerStatus>>,
    hummock_manager: HummockManagerRef,
    request_tx: mpsc::UnboundedSender<BarrierManagerRequest>,
    metadata_manager: MetadataManager,
}

pub type BarrierManagerRef = Arc<GlobalBarrierManager>;

impl GlobalBarrierManager {
    /// Serving `SHOW JOBS / SELECT * FROM rw_ddl_progress`
    pub async fn get_ddl_progress(&self) -> MetaResult<Vec<DdlProgress>> {
        let mut backfill_progress = {
            let (tx, rx) = oneshot::channel();
            self.request_tx
                .send(BarrierManagerRequest::GetBackfillProgress(tx))
                .context("failed to send get ddl progress request")?;
            rx.await.context("failed to receive get ddl progress")?
        };
        // If not in tracker, means the first barrier not collected yet.
        // In that case just return progress 0.
        let job_info = self
            .metadata_manager
            .catalog_controller
            .list_creating_jobs(true, None)
            .await?;
        Ok(job_info
            .into_iter()
            .map(
                |(job_id, definition, init_at, create_type, is_serverless_backfill)| {
                    let BackfillProgress {
                        progress,
                        backfill_type,
                    } = match &mut backfill_progress {
                        Ok(progress) => progress.remove(&job_id).unwrap_or_else(|| {
                            warn!(%job_id, "creating job has no ddl progress");
                            BackfillProgress {
                                progress: "0.0%".into(),
                                backfill_type: PbBackfillType::NormalBackfill,
                            }
                        }),
                        Err(e) => BackfillProgress {
                            progress: format!("Err[{}]", e.as_report()),
                            backfill_type: PbBackfillType::NormalBackfill,
                        },
                    };
                    DdlProgress {
                        id: job_id.as_raw_id() as u64,
                        statement: definition,
                        create_type: create_type.as_str().into(),
                        initialized_at_time_millis: datetime_to_timestamp_millis(init_at),
                        progress,
                        is_serverless_backfill,
                        backfill_type: backfill_type as _,
                    }
                },
            )
            .collect())
    }

    pub(crate) async fn get_fragment_backfill_progress(
        &self,
    ) -> MetaResult<Vec<FragmentBackfillProgress>> {
        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(BarrierManagerRequest::GetFragmentBackfillProgress(tx))
            .context("failed to send get fragment backfill progress request")?;
        rx.await
            .context("failed to receive get fragment backfill progress")?
    }

    pub async fn get_cdc_progress(&self) -> MetaResult<HashMap<JobId, CdcProgress>> {
        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(BarrierManagerRequest::GetCdcProgress(tx))
            .context("failed to send get ddl progress request")?;
        rx.await.context("failed to receive get ddl progress")?
    }

    pub async fn adhoc_recovery(&self, database_id: Option<DatabaseId>) -> MetaResult<()> {
        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(BarrierManagerRequest::AdhocRecovery {
                database_id,
                sender: tx,
            })
            .context("failed to send adhoc recovery request")?;
        rx.await.context("failed to wait adhoc recovery")?;
        Ok(())
    }

    pub async fn update_database_barrier(
        &self,
        database_id: DatabaseId,
        barrier_interval_ms: Option<u32>,
        checkpoint_frequency: Option<u64>,
    ) -> MetaResult<()> {
        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(BarrierManagerRequest::UpdateDatabaseBarrier(
                UpdateDatabaseBarrierRequest {
                    database_id,
                    barrier_interval_ms,
                    checkpoint_frequency,
                    sender: tx,
                },
            ))
            .context("failed to send update database barrier request")?;
        rx.await.context("failed to wait update database barrier")?;
        Ok(())
    }

    pub async fn get_hummock_version_id(&self) -> HummockVersionId {
        self.hummock_manager.get_version_id().await
    }
}

impl GlobalBarrierManager {
    /// Check the status of barrier manager, return error if it is not `Running`.
    pub fn check_status_running(&self) -> MetaResult<()> {
        let status = self.status.load();
        match &**status {
            BarrierManagerStatus::Starting
            | BarrierManagerStatus::Recovering(RecoveryReason::Bootstrap) => {
                bail!("The cluster is bootstrapping")
            }
            BarrierManagerStatus::Recovering(RecoveryReason::Failover(e)) => {
                Err(anyhow::anyhow!(e.clone()).context("The cluster is recovering"))?
            }
            BarrierManagerStatus::Recovering(RecoveryReason::Adhoc) => {
                bail!("The cluster is recovering-adhoc")
            }
            BarrierManagerStatus::Running => Ok(()),
        }
    }

    pub fn get_recovery_status(&self) -> PbRecoveryStatus {
        (&**self.status.load()).into()
    }
}

impl GlobalBarrierManager {
    #[expect(clippy::too_many_arguments)]
    pub async fn start(
        scheduled_barriers: schedule::ScheduledBarriers,
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        hummock_manager: HummockManagerRef,
        serving_vnode_mapping: ServingVnodeMappingRef,
        source_manager: SourceManagerRef,
        sink_manager: SinkCoordinatorManager,
        iceberg_pk_index_sink_manager: IcebergPkIndexSinkManager,
        iceberg_compaction_manager: IcebergCompactionManagerRef,
        scale_controller: ScaleControllerRef,
        barrier_scheduler: schedule::BarrierScheduler,
        refresh_manager: GlobalRefreshManagerRef,
    ) -> (Arc<Self>, JoinHandle<()>, oneshot::Sender<()>) {
        let (request_tx, request_rx) = unbounded_channel();
        let hummock_manager_clone = hummock_manager.clone();
        let metadata_manager_clone = metadata_manager.clone();
        let barrier_worker = GlobalBarrierWorker::new(
            scheduled_barriers,
            env,
            metadata_manager,
            hummock_manager,
            serving_vnode_mapping,
            source_manager,
            sink_manager,
            iceberg_pk_index_sink_manager,
            iceberg_compaction_manager,
            scale_controller,
            request_rx,
            barrier_scheduler,
            refresh_manager,
        )
        .await;
        let manager = Self {
            status: barrier_worker.context.status(),
            hummock_manager: hummock_manager_clone,
            request_tx,
            metadata_manager: metadata_manager_clone,
        };
        let (join_handle, shutdown_tx) = barrier_worker.start();
        (Arc::new(manager), join_handle, shutdown_tx)
    }
}
