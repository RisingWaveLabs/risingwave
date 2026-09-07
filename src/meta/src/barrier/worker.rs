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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::mem::replace;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use arc_swap::ArcSwap;
use futures::{TryFutureExt, pin_mut};
use itertools::Itertools;
use risingwave_common::catalog::DatabaseId;
use risingwave_common::id::JobId;
use risingwave_common::system_param::PAUSE_ON_NEXT_BOOTSTRAP_KEY;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_meta_model::WorkerId;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::Recovery;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use thiserror_ext::AsReport;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::task::JoinHandle;
use tonic::Status;
use tracing::{Instrument, debug, error, info, warn};

use crate::barrier::checkpoint::{CheckpointControl, CheckpointControlEvent};
use crate::barrier::complete_task::{BarrierCompleteOutput, CompletingTask};
use crate::barrier::context::recovery::{RenderedDatabaseRuntimeInfo, render_runtime_info};
use crate::barrier::context::{GlobalBarrierWorkerContext, GlobalBarrierWorkerContextImpl};
use crate::barrier::info::InflightDatabaseInfo;
use crate::barrier::rpc::{
    DatabaseInitialBarrierCollector, database_partial_graphs, from_partial_graph_id,
    merge_node_rpc_errors,
};
use crate::barrier::schedule::{MarkReadyOptions, PeriodicBarriers};
use crate::barrier::{
    BarrierManagerRequest, BarrierManagerStatus, BarrierWorkerRuntimeInfoSnapshot, Command,
    CreateStreamingJobType, RecoveryReason, RescheduleContext, UpdateDatabaseBarrierRequest,
    schedule,
};
use crate::controller::scale::{materialize_actor_assignments, preview_actor_assignments};
use crate::error::MetaErrorInner;
use crate::hummock::HummockManagerRef;
use crate::manager::iceberg_compaction::IcebergCompactionManagerRef;
use crate::manager::iceberg_pk_index_sink::IcebergPkIndexSinkManager;
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{
    ActiveStreamingWorkerChange, ActiveStreamingWorkerNodes, LocalNotification, MetaSrvEnv,
    MetadataManager,
};
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::serving::ServingVnodeMappingRef;
use crate::stream::{
    GlobalRefreshManagerRef, ScaleControllerRef, SourceManagerRef, build_reschedule_commands,
    rendered_layout_matches_current,
};
use crate::{MetaError, MetaResult};

/// [`crate::barrier::worker::GlobalBarrierWorker`] sends barriers to all registered compute nodes and
/// collect them, with monotonic increasing epoch numbers. On compute nodes, `LocalBarrierManager`
/// in `risingwave_stream` crate will serve these requests and dispatch them to source actors.
///
/// Configuration change in our system is achieved by the mutation in the barrier. Thus,
/// [`crate::barrier::worker::GlobalBarrierWorker`] provides a set of interfaces like a state machine,
/// accepting [`crate::barrier::command::Command`] that carries info to build `Mutation`. To keep the consistency between
/// barrier manager and meta store, some actions like "drop materialized view" or "create mv on mv"
/// must be done in barrier manager transactional using [`crate::barrier::command::Command`].
pub(super) struct GlobalBarrierWorker<C> {
    /// Enable recovery or not when failover.
    enable_recovery: bool,

    /// The queue of scheduled barriers.
    periodic_barriers: PeriodicBarriers,

    /// Whether per database failure isolation is enabled in system parameters.
    system_enable_per_database_isolation: bool,

    pub(super) context: Arc<C>,

    env: MetaSrvEnv,

    checkpoint_control: CheckpointControl,

    /// Command that has been collected but is still completing.
    /// The join handle of the completing future is stored.
    completing_task: CompletingTask,

    request_rx: mpsc::UnboundedReceiver<BarrierManagerRequest>,

    active_streaming_nodes: ActiveStreamingWorkerNodes,

    partial_graph_manager: PartialGraphManager,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::barrier::RescheduleContext;
    use crate::barrier::notifier::Notifier;

    #[tokio::test]
    async fn test_reschedule_intent_without_workers_notifies_start_failed() {
        let env = MetaSrvEnv::for_test().await;
        let database_id = DatabaseId::new(1);
        let database_info =
            InflightDatabaseInfo::empty(database_id, env.shared_actor_infos().clone());
        let (notifier, started_rx) = Notifier::new();

        let new_barrier = schedule::NewBarrier {
            database_id,
            command: Some((
                Command::RescheduleIntent {
                    context: RescheduleContext::empty(),
                    reschedule_plan: None,
                },
                notifier,
            )),
            span: tracing::Span::none(),
            checkpoint: false,
        };

        let result =
            resolve_reschedule_intent(env, HashMap::new(), Some(&database_info), new_barrier);

        assert!(matches!(result, Ok(None)));
        let started = started_rx.await.expect("started notifier dropped");
        assert!(started.is_err());
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    pub(super) async fn new_inner(
        env: MetaSrvEnv,
        request_rx: mpsc::UnboundedReceiver<BarrierManagerRequest>,
        context: Arc<C>,
    ) -> Self {
        let enable_recovery = env.opts.enable_recovery;

        let active_streaming_nodes = ActiveStreamingWorkerNodes::uninitialized();

        let partial_graph_manager = PartialGraphManager::uninitialized(env.clone());

        let reader = env.system_params_reader().await;
        let system_enable_per_database_isolation = reader.per_database_isolation();
        // Load config will be performed in bootstrap phase.
        let periodic_barriers = PeriodicBarriers::default();

        let checkpoint_control = CheckpointControl::new(env.clone());
        Self {
            enable_recovery,
            periodic_barriers,
            system_enable_per_database_isolation,
            context,
            env,
            checkpoint_control,
            completing_task: CompletingTask::None,
            request_rx,
            active_streaming_nodes,
            partial_graph_manager,
        }
    }
}

fn resolve_reschedule_intent(
    env: MetaSrvEnv,
    worker_nodes: HashMap<WorkerId, WorkerNode>,
    database_info: Option<&InflightDatabaseInfo>,
    mut new_barrier: schedule::NewBarrier,
) -> MetaResult<Option<schedule::NewBarrier>> {
    let Some((command, notifier)) = new_barrier.command.take() else {
        return Ok(Some(new_barrier));
    };

    match command {
        Command::RescheduleIntent {
            context,
            reschedule_plan,
        } => {
            if let Some(reschedule_plan) = reschedule_plan {
                new_barrier.command = Some((
                    Command::RescheduleIntent {
                        context,
                        reschedule_plan: Some(reschedule_plan),
                    },
                    notifier,
                ));
                return Ok(Some(new_barrier));
            }
            let span = tracing::info_span!(
                "resolve_reschedule_intent",
                database_id = %new_barrier.database_id
            );
            let reschedule_plan = {
                let _guard = span.enter();
                build_reschedule_from_context(
                    &env,
                    worker_nodes,
                    new_barrier.database_id,
                    context,
                    database_info.ok_or_else(|| {
                        anyhow!(
                            "database {} not found when resolving reschedule intent",
                            new_barrier.database_id
                        )
                    })?,
                )
            };
            match reschedule_plan {
                Ok(Some(reschedule_plan)) => {
                    new_barrier.command = Some((
                        Command::RescheduleIntent {
                            context: RescheduleContext::empty(),
                            reschedule_plan: Some(reschedule_plan),
                        },
                        notifier,
                    ));
                    Ok(Some(new_barrier))
                }
                Ok(None) => {
                    // No-op intent: notify to unblock callers even though no barrier is injected.
                    notifier.start().started();
                    Ok(None)
                }
                Err(err) => {
                    notifier.notify_start_failed(err);
                    Ok(None)
                }
            }
        }
        _ => {
            new_barrier.command = Some((command, notifier));
            Ok(Some(new_barrier))
        }
    }
}

fn build_reschedule_from_context(
    env: &MetaSrvEnv,
    worker_nodes: HashMap<WorkerId, WorkerNode>,
    database_id: DatabaseId,
    context: RescheduleContext,
    database_info: &InflightDatabaseInfo,
) -> MetaResult<Option<crate::barrier::ReschedulePlan>> {
    if worker_nodes.is_empty() {
        return Err(anyhow!("no active streaming workers for reschedule").into());
    }

    if context.is_empty() {
        return Ok(None);
    }

    // Barrier worker resolves this intent against a stable in-flight snapshot.
    // Reuse the same fragment view for preview comparison and command building.
    let all_prev_fragments = database_info
        .fragment_infos()
        .map(|fragment| (fragment.fragment_id, fragment))
        .collect();

    let previewed = preview_actor_assignments(&worker_nodes, &context.loaded)?;

    if rendered_layout_matches_current(&previewed.fragments, &all_prev_fragments)? {
        return Ok(None);
    }

    let actor_id_counter = env.actor_id_generator();
    // Materialization only replaces preview actor ids with real ids. Worker
    // placement, vnode ownership, and split assignment remain unchanged.
    let rendered = materialize_actor_assignments(actor_id_counter, previewed);
    let mut commands = build_reschedule_commands(rendered.fragments, context, all_prev_fragments)?;
    Ok(commands.remove(&database_id))
}

impl GlobalBarrierWorker<GlobalBarrierWorkerContextImpl> {
    /// Create a new [`crate::barrier::worker::GlobalBarrierWorker`].
    #[expect(clippy::too_many_arguments)]
    pub async fn new(
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
        request_rx: mpsc::UnboundedReceiver<BarrierManagerRequest>,
        barrier_scheduler: schedule::BarrierScheduler,
        refresh_manager: GlobalRefreshManagerRef,
    ) -> Self {
        let status = Arc::new(ArcSwap::new(Arc::new(BarrierManagerStatus::Starting)));

        let context = Arc::new(GlobalBarrierWorkerContextImpl::new(
            scheduled_barriers,
            status,
            metadata_manager,
            hummock_manager,
            serving_vnode_mapping,
            source_manager,
            scale_controller,
            env.clone(),
            barrier_scheduler,
            refresh_manager,
            sink_manager,
            iceberg_pk_index_sink_manager,
            iceberg_compaction_manager,
        ));

        Self::new_inner(env, request_rx, context).await
    }

    pub fn start(self) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let fut = (self.env.await_tree_reg())
            .register_derived_root("Global Barrier Worker")
            .instrument(self.run(shutdown_rx));
        let join_handle = tokio::spawn(fut);

        (join_handle, shutdown_tx)
    }

    /// Check whether we should pause on bootstrap from the system parameter and reset it.
    async fn take_pause_on_bootstrap(&mut self) -> MetaResult<bool> {
        let paused = self
            .env
            .system_params_reader()
            .await
            .pause_on_next_bootstrap()
            || self.env.opts.pause_on_next_bootstrap_offline;

        if paused {
            warn!(
                "The cluster will bootstrap with all data sources paused as specified by the system parameter `{}`. \
                 It will now be reset to `false`. \
                 To resume the data sources, either restart the cluster again or use `risectl meta resume`.",
                PAUSE_ON_NEXT_BOOTSTRAP_KEY
            );
            self.env
                .system_params_manager_impl_ref()
                .set_param(PAUSE_ON_NEXT_BOOTSTRAP_KEY, Some("false".to_owned()))
                .await?;
        }
        Ok(paused)
    }

    /// Start an infinite loop to take scheduled barriers and send them.
    async fn run(mut self, shutdown_rx: Receiver<()>) {
        tracing::info!(
            "Starting barrier manager with: enable_recovery={}, in_flight_barrier_nums={}",
            self.enable_recovery,
            self.checkpoint_control.in_flight_barrier_nums,
        );

        if !self.enable_recovery {
            let job_exist = self
                .context
                .metadata_manager
                .catalog_controller
                .has_any_streaming_jobs()
                .await
                .unwrap();
            if job_exist {
                panic!(
                    "Some streaming jobs already exist in meta, please start with recovery enabled \
                or clean up the metadata using `./risedev clean-data`"
                );
            }
        }

        {
            // Bootstrap recovery. Here we simply trigger a recovery process to achieve the
            // consistency.
            // Even if there's no actor to recover, we still go through the recovery process to
            // inject the first `Initial` barrier.
            let span = tracing::info_span!("bootstrap_recovery");
            crate::telemetry::report_event(
                risingwave_pb::telemetry::TelemetryEventStage::Recovery,
                "normal_recovery",
                0,
                None,
                None,
                None,
            );

            let paused = self.take_pause_on_bootstrap().await.unwrap_or(false);

            // Keep the bootstrap recovery future boxed so the outer barrier worker future
            // stays below clippy's large-futures threshold.
            Box::pin(
                self.recovery(paused, RecoveryReason::Bootstrap)
                    .instrument(span),
            )
            .await;
        }

        Box::pin(self.run_inner(shutdown_rx)).await
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    fn enable_per_database_isolation(&self) -> bool {
        self.system_enable_per_database_isolation && {
            if let Err(e) =
                risingwave_common::license::Feature::DatabaseFailureIsolation.check_available()
            {
                warn!(error = %e.as_report(), "DatabaseFailureIsolation disabled by license");
                false
            } else {
                true
            }
        }
    }

    async fn resolve_since_timestamp_snapshot_backfill(
        &mut self,
        new_barrier: &mut schedule::NewBarrier,
    ) -> MetaResult<bool> {
        let Some((
            Command::CreateStreamingJob {
                job_type:
                    CreateStreamingJobType::SnapshotBackfill {
                        snapshot_backfill_info,
                        since_epoch: Some(since_epoch),
                    },
                ..
            },
            _,
        )) = &mut new_barrier.command
        else {
            return Ok(true);
        };
        let since_timestamp_epoch = since_epoch.provided_since_epoch;

        // Complete any inflight command first so the committed upstream epoch and
        // table changelog view used for since_timestamp resolution cannot be stale.
        match self.completing_task.wait_completing_task().await {
            Ok(Some(output)) => self
                .checkpoint_control
                .ack_completed(&mut self.partial_graph_manager, output),
            Ok(None) => {}
            Err(err) => {
                error!(
                    err = %err.as_report(),
                    "failed to wait completing task before resolving since_timestamp"
                );
                return Err(err);
            }
        }

        match self
            .context
            .resolve_log_store_epoch(
                snapshot_backfill_info
                    .upstream_mv_table_id_to_backfill_epoch
                    .keys()
                    .copied(),
                since_timestamp_epoch,
            )
            .await
        {
            Ok(upstream_log_epochs) => {
                since_epoch.resolved = Some(upstream_log_epochs);
                Ok(true)
            }
            Err(err) => {
                error!(
                    err = %err.as_report(),
                    "failed to resolve log store epoch for since_timestamp"
                );
                let (_, notifier) = new_barrier
                    .command
                    .take()
                    .expect("matched command should still exist");
                notifier.notify_start_failed(err);
                Ok(false)
            }
        }
    }

    pub(super) async fn run_inner(mut self, mut shutdown_rx: Receiver<()>) {
        let (local_notification_tx, mut local_notification_rx) =
            tokio::sync::mpsc::unbounded_channel();
        self.env
            .notification_manager()
            .insert_local_sender(local_notification_tx);

        // Start the event loop.
        loop {
            tokio::select! {
                biased;

                // Shutdown
                _ = &mut shutdown_rx => {
                    tracing::info!("Barrier manager is stopped");
                    break;
                }

                request = self.request_rx.recv() => {
                    if let Some(request) = request {
                        match request {
                            BarrierManagerRequest::GetBackfillProgress(result_tx) => {
                                let progress = self.checkpoint_control.gen_backfill_progress();
                                if result_tx.send(Ok(progress)).is_err() {
                                    error!("failed to send get ddl progress");
                                }
                            }
                            BarrierManagerRequest::GetFragmentBackfillProgress(result_tx) => {
                                let progress =
                                    self.checkpoint_control.gen_fragment_backfill_progress();
                                if result_tx.send(Ok(progress)).is_err() {
                                    error!("failed to send get fragment backfill progress");
                                }
                            }
                            BarrierManagerRequest::GetCdcProgress(result_tx) => {
                                let progress = self.checkpoint_control.gen_cdc_progress();
                                if result_tx.send(Ok(progress)).is_err() {
                                    error!("failed to send get ddl progress");
                                }
                            }
                            // Handle adhoc recovery triggered by user.
                            BarrierManagerRequest::AdhocRecovery(sender) => {
                                self.adhoc_recovery().await;
                                if sender.send(()).is_err() {
                                    warn!("failed to notify finish of adhoc recovery");
                                }
                            }
                            BarrierManagerRequest::UpdateDatabaseBarrier( UpdateDatabaseBarrierRequest {
                                database_id,
                                barrier_interval_ms,
                                checkpoint_frequency,
                                sender,
                            }) => {
                                self.periodic_barriers
                                    .update_database_barrier(
                                        database_id,
                                        barrier_interval_ms,
                                        checkpoint_frequency,
                                    );
                                if sender.send(()).is_err() {
                                    warn!("failed to notify finish of update database barrier");
                                }
                            }
                        }
                    } else {
                        tracing::info!("end of request stream. meta node may be shutting down. Stop global barrier manager");
                        return;
                    }
                }

                changed_worker = self.active_streaming_nodes.changed() => {
                    #[cfg(debug_assertions)]
                    {
                        self.active_streaming_nodes.validate_change().await;
                    }

                    info!(?changed_worker, "worker changed");

                    match changed_worker {
                        ActiveStreamingWorkerChange::Add(node)
                        | ActiveStreamingWorkerChange::Update(node) => {
                            self.partial_graph_manager
                                .add_worker(node, self.context.clone())
                                .await;
                        }
                        ActiveStreamingWorkerChange::Remove(node) => {
                            self.partial_graph_manager.remove_worker(node);
                        }
                    }
                }

                notification = local_notification_rx.recv() => {
                    let notification = notification.unwrap();
                    if let LocalNotification::SystemParamsChange(p) = notification {
                        {
                            self.periodic_barriers.set_sys_barrier_interval(Duration::from_millis(p.barrier_interval_ms() as u64));
                            self.periodic_barriers
                                .set_sys_checkpoint_frequency(p.checkpoint_frequency());
                            self.system_enable_per_database_isolation = p.per_database_isolation();
                        }
                    }
                }
                complete_result = self
                    .completing_task
                    .next_completed_barrier(
                        &mut self.periodic_barriers,
                        &mut self.checkpoint_control,
                        &mut self.partial_graph_manager,
                        &self.context,
                        &self.env,
                ) => {
                    match complete_result {
                        Ok(output) => {
                            self.checkpoint_control.ack_completed(&mut self.partial_graph_manager, output);
                        }
                        Err(e) => {
                            self.failure_recovery(e).await;
                        }
                    }
                },
                event = self.checkpoint_control.next_event() => {
                    let result: MetaResult<()> = try {
                        match event {
                            CheckpointControlEvent::EnteringInitializing(entering_initializing) => {
                                let database_id = entering_initializing.database_id();
                                let error = merge_node_rpc_errors(&format!("database {} reset", database_id), entering_initializing.action.0.iter().filter_map(|(worker_id, resp)| {
                                    resp.root_err.as_ref().map(|root_err| {
                                        (*worker_id, ScoredError {
                                            error: Status::internal(&root_err.err_msg),
                                            score: Score(root_err.score)
                                        })
                                    })
                                }));
                                Self::report_collect_failure(&self.env, &error);
                                self.context.notify_creating_job_failed(Some(database_id), format!("{}", error.as_report())).await;
                                let result: MetaResult<_> = try {
                                    let runtime_info = self.context.reload_database_runtime_info(database_id).await.inspect_err(|err| {
                                        warn!(%database_id, err = %err.as_report(), "reload runtime info failed");
                                    })?;
                                    let rendered_info = render_runtime_info(
                                        self.env.actor_id_generator(),
                                        &self.active_streaming_nodes,
                                        self.partial_graph_manager.control_stream_manager(),
                                        &runtime_info.recovery_context,
                                        database_id,
                                    )
                                    .inspect_err(|err: &MetaError| {
                                        warn!(%database_id, err = %err.as_report(), "render runtime info failed");
                                    })?;
                                    if let Some(rendered_info) = rendered_info {
                                        BarrierWorkerRuntimeInfoSnapshot::validate_database_info(
                                            database_id,
                                            &rendered_info.job_infos,
                                            &self.active_streaming_nodes,
                                            &rendered_info.stream_actors,
                                            &runtime_info.state_table_committed_epochs,
                                        )
                                        .inspect_err(|err| {
                                            warn!(%database_id, err = ?err.as_report(), "database runtime info failed validation");
                                        })?;
                                        Some((runtime_info, rendered_info))
                                    } else {
                                        None
                                    }
                                };
                                match result {
                                    Ok(Some((runtime_info, rendered_info))) => {
                                        entering_initializing.enter(
                                            runtime_info,
                                            rendered_info,
                                            &mut self.partial_graph_manager,
                                        );
                                    }
                                    Ok(None) => {
                                        info!(%database_id, "database removed after reloading empty runtime info");
                                        entering_initializing.remove();
                                        self.context
                                            .refresh_table_refill_runtime_state_after_recovery()
                                            .await?;
                                        // Mark ready only after the refill runtime state is refreshed.
                                        self.context.mark_ready(MarkReadyOptions::Database(database_id));
                                    }
                                    Err(e) => {
                                        entering_initializing.fail_reload_runtime_info(e);
                                    }
                                }
                            }
                            CheckpointControlEvent::EnteringRunning(entering_running) => {
                                let database_id = entering_running.database_id();
                                entering_running.enter();
                                self.context
                                    .refresh_table_refill_runtime_state_after_recovery()
                                    .await?;
                                self.context
                                    .mark_ready(MarkReadyOptions::Database(database_id));
                            }
                            CheckpointControlEvent::BatchRefreshTrigger { database_id, job_id } => {
                                self.handle_batch_refresh_trigger(database_id, job_id).await?;
                            }
                        }
                    };
                    if let Err(e) = result {
                        self.failure_recovery(e).await;
                    }
                }
                event = self.partial_graph_manager.next_event(&self.context) => {
                    let result: MetaResult<()> = try {
                        match event {
                            PartialGraphManagerEvent::Worker(_worker_id, WorkerEvent::WorkerConnected) => {
                                // no handling on new worker connected event yet
                            }
                            PartialGraphManagerEvent::Worker(worker_id, WorkerEvent::WorkerError { err, affected_partial_graphs }) => {
                                let failed_databases = self
                                    .checkpoint_control
                                    .databases_failed_at_worker_err(worker_id)
                                    .chain(
                                        affected_partial_graphs
                                        .into_iter()
                                        .map(|partial_graph_id| {
                                            let (database_id, _) = from_partial_graph_id(partial_graph_id);
                                            database_id
                                        })
                                    )
                                    .collect::<HashSet<_>>();
                                if !failed_databases.is_empty() {
                                    if !self.enable_recovery {
                                        panic!("control stream to worker {} failed but recovery not enabled: {}", worker_id, err.as_report());
                                    }
                                    if !self.enable_per_database_isolation() {
                                        Err(err.clone())?;
                                    }
                                    Self::report_collect_failure(&self.env, &err);
                                    for database_id in failed_databases {
                                        if let Some(entering_recovery) = self.checkpoint_control.on_report_failure(database_id, &mut self.partial_graph_manager) {
                                            warn!(%worker_id, %database_id, "database entering recovery on node failure");
                                            self.context.abort_and_mark_blocked(Some(database_id), RecoveryReason::Failover(anyhow!("reset database: {}", database_id).into()));
                                            self.context.notify_creating_job_failed(Some(database_id), format!("database {} reset due to node {} failure: {}", database_id, worker_id, err.as_report())).await;
                                            // TODO: add log on blocking time
                                            let output = self.completing_task.wait_completing_task().await?;
                                            entering_recovery.enter(output, &mut self.partial_graph_manager);
                                        }
                                    }
                                }  else {
                                    warn!(%worker_id, "no barrier to collect from worker, ignore err");
                                }
                                continue;
                            }
                            PartialGraphManagerEvent::PartialGraph(partial_graph_id, event) => {
                                let (database_id, _creating_job_id) = from_partial_graph_id(partial_graph_id);
                                match event {
                                    PartialGraphEvent::BarrierCollected(collected_barrier) => {
                                        self.checkpoint_control.barrier_collected(partial_graph_id, collected_barrier, &mut self.periodic_barriers)?;
                                    }
                                    PartialGraphEvent::Error(worker_id) => {
                                        if !self.enable_recovery {
                                            panic!("database {database_id} failure reported from {worker_id} but recovery not enabled")
                                        }
                                        if !self.enable_per_database_isolation() {
                                                Err(MetaError::from(anyhow!("database {database_id} report failure from {worker_id}")))?;
                                            }
                                        if let Some(entering_recovery) = self.checkpoint_control.on_report_failure(database_id, &mut self.partial_graph_manager) {
                                            warn!(%database_id, "database entering recovery");
                                            self.context.abort_and_mark_blocked(Some(database_id), RecoveryReason::Failover(anyhow!("reset database: {}", database_id).into()));
                                            // TODO: add log on blocking time
                                            let output = self.completing_task.wait_completing_task().await?;
                                            entering_recovery.enter(output, &mut self.partial_graph_manager);
                                        }
                                    }
                                    PartialGraphEvent::Reset(reset_resps) => {
                                        self.checkpoint_control.on_partial_graph_reset(partial_graph_id, reset_resps);
                                    }
                                    PartialGraphEvent::Initialized => {
                                        self.checkpoint_control.on_partial_graph_initialized(
                                            partial_graph_id,
                                            &mut self.partial_graph_manager,
                                        )?;
                                    }
                                }
                            }
                        };
                    };
                    if let Err(e) = result {
                        self.failure_recovery(e).await;
                    }
                }
                new_barrier = self.periodic_barriers.next_barrier(&*self.context) => {
                    let database_id = new_barrier.database_id;
                    let mut new_barrier = if matches!(
                        new_barrier.command,
                        Some((Command::RescheduleIntent { .. }, _))
                    ) {
                        let env = self.env.clone();
                        let worker_nodes = self
                            .active_streaming_nodes
                            .current()
                            .iter()
                            .map(|(worker_id, worker)| (*worker_id, worker.clone()))
                            .collect();
                        let database_info = self.checkpoint_control.database_info(database_id);
                        match resolve_reschedule_intent(
                            env,
                            worker_nodes,
                            database_info,
                            new_barrier,
                        ) {
                            Ok(Some(new_barrier)) => new_barrier,
                            Ok(None) => continue,
                            Err(err) => {
                                self.failure_recovery(err).await;
                                continue;
                            }
                        }
                    } else {
                        new_barrier
                    };
                    match self
                        .resolve_since_timestamp_snapshot_backfill(&mut new_barrier)
                        .await
                    {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(err) => {
                            self.failure_recovery(err).await;
                            continue;
                        }
                    }
                    if let Err(e) = self.checkpoint_control.handle_new_barrier(
                        new_barrier,
                        &mut self.partial_graph_manager,
                        self.active_streaming_nodes.current()
                    ) {
                        if !self.enable_recovery {
                            panic!(
                                "failed to inject barrier to some databases but recovery not enabled: {:?}", (
                                    database_id,
                                    e.as_report()
                                )
                            );
                        }
                        let result: MetaResult<_> = try {
                            if !self.enable_per_database_isolation() {
                                let err = anyhow!("failed to inject barrier to databases: {:?}", (database_id, e.as_report()));
                                Err(MetaError::from(err))?;
                            } else if let Some(entering_recovery) = self.checkpoint_control.on_report_failure(database_id, &mut self.partial_graph_manager) {
                                warn!(%database_id, e = %e.as_report(),"database entering recovery on inject failure");
                                self.context.abort_and_mark_blocked(Some(database_id), RecoveryReason::Failover(anyhow!(e).context("inject barrier failure").into()));
                                // TODO: add log on blocking time
                                let output = self.completing_task.wait_completing_task().await?;
                                entering_recovery.enter(output, &mut self.partial_graph_manager);
                            }
                        };
                        if let Err(e) = result {
                            self.failure_recovery(e).await;
                        }
                    }
                }
            }
        }
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    /// We need to make sure there are no changes when doing recovery
    pub async fn clear_on_err(&mut self, err: &MetaError) {
        // join spawned completing command to finish no matter it succeeds or not.
        match replace(&mut self.completing_task, CompletingTask::None) {
            CompletingTask::None | CompletingTask::Err(_) => {}
            CompletingTask::Completing {
                epochs_to_ack,
                join_handle,
                ..
            } => {
                info!("waiting for completing command to finish in recovery");
                match join_handle.await {
                    Err(e) => {
                        warn!(err = %e.as_report(), "failed to join completing task");
                    }
                    Ok(Err(e)) => {
                        warn!(
                            err = %e.as_report(),
                            "failed to complete barrier during clear"
                        );
                    }
                    Ok(Ok(hummock_version_stats)) => {
                        self.checkpoint_control.ack_completed(
                            &mut self.partial_graph_manager,
                            BarrierCompleteOutput {
                                epochs_to_ack,
                                hummock_version_stats,
                            },
                        );
                    }
                }
            }
        };
        self.partial_graph_manager.notify_all_err(err);
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    /// Handle a batch refresh trigger: load metadata + log epochs, then start a logstore
    /// consumption run for the given batch refresh job.
    async fn handle_batch_refresh_trigger(
        &mut self,
        database_id: DatabaseId,
        job_id: JobId,
    ) -> MetaResult<()> {
        // 1. Get the last committed epoch for this job (read-only).
        let last_committed_epoch = self
            .checkpoint_control
            .get_batch_refresh_trigger_info(database_id, job_id);

        // 2. Load context metadata + resolve log epochs asynchronously.
        let context = self
            .context
            .load_batch_refresh_trigger_context(job_id, database_id, last_committed_epoch)
            .await?;

        // 3. Start the refresh run.
        let started = self.checkpoint_control.start_batch_refresh_run(
            database_id,
            job_id,
            &context,
            self.active_streaming_nodes.current(),
            self.env.actor_id_generator(),
            &mut self.partial_graph_manager,
        )?;

        // 5. Update shared_actor_infos with the new fragment infos.
        if started {
            self.checkpoint_control
                .apply_batch_refresh_fragment_infos(database_id, job_id);
        }

        Ok(())
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    /// Set barrier manager status.
    async fn failure_recovery(&mut self, err: MetaError) {
        self.clear_on_err(&err).await;

        if self.enable_recovery {
            let span = tracing::info_span!(
                "failure_recovery",
                error = %err.as_report(),
            );

            crate::telemetry::report_event(
                risingwave_pb::telemetry::TelemetryEventStage::Recovery,
                "failure_recovery",
                0,
                None,
                None,
                None,
            );

            let reason = RecoveryReason::Failover(err);

            // No need to clean dirty tables for barrier recovery,
            // The foreground stream job should cleanup their own tables.
            self.recovery(false, reason).instrument(span).await;
        } else {
            panic!(
                "a streaming error occurred while recovery is disabled, aborting: {:?}",
                err.as_report()
            );
        }
    }

    async fn adhoc_recovery(&mut self) {
        let err = MetaErrorInner::AdhocRecovery.into();
        self.clear_on_err(&err).await;

        let span = tracing::info_span!(
            "adhoc_recovery",
            error = %err.as_report(),
        );

        crate::telemetry::report_event(
            risingwave_pb::telemetry::TelemetryEventStage::Recovery,
            "adhoc_recovery",
            0,
            None,
            None,
            None,
        );

        // No need to clean dirty tables for barrier recovery,
        // The foreground stream job should cleanup their own tables.
        self.recovery(false, RecoveryReason::Adhoc)
            .instrument(span)
            .await;
    }
}

impl<C> GlobalBarrierWorker<C> {
    /// Send barrier-complete-rpc and wait for responses from all CNs
    pub(super) fn report_collect_failure(env: &MetaSrvEnv, error: &MetaError) {
        // Record failure in event log.
        use risingwave_pb::meta::event_log;
        let event = event_log::EventCollectBarrierFail {
            error: error.to_report_string(),
        };
        env.event_log_manager_ref()
            .add_event_logs(vec![event_log::Event::CollectBarrierFail(event)]);
    }
}

mod retry_strategy {
    use std::time::Duration;

    use risingwave_common::util::retry::exponential_backoff;
    use tokio_retry::strategy::jitter;

    // Retry base interval in milliseconds.
    const RECOVERY_RETRY_BASE_INTERVAL: u64 = 20;
    // Retry max interval.
    const RECOVERY_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(5);

    // MrCroxx: Use concrete type here to prevent unsolved compiler issue.
    // Feel free to replace the concrete type with TAIT after fixed.

    // mod retry_backoff_future {
    //     use std::future::Future;
    //     use std::time::Duration;
    //
    //     use tokio::time::sleep;
    //
    //     pub(crate) type RetryBackoffFuture = impl Future<Output = ()> + Unpin + Send + 'static;
    //
    //     #[define_opaque(RetryBackoffFuture)]
    //     pub(super) fn get_retry_backoff_future(duration: Duration) -> RetryBackoffFuture {
    //         Box::pin(sleep(duration))
    //     }
    // }
    // pub(crate) use retry_backoff_future::*;

    pub(crate) type RetryBackoffFuture = std::pin::Pin<Box<tokio::time::Sleep>>;

    pub(crate) fn get_retry_backoff_future(duration: Duration) -> RetryBackoffFuture {
        Box::pin(tokio::time::sleep(duration))
    }

    pub(crate) type RetryBackoffStrategy =
        impl Iterator<Item = RetryBackoffFuture> + Send + 'static;

    /// Initialize a retry strategy for operation in recovery.
    #[inline(always)]
    pub(crate) fn get_retry_strategy() -> impl Iterator<Item = Duration> + Send + 'static {
        exponential_backoff(
            Duration::from_millis(RECOVERY_RETRY_BASE_INTERVAL),
            RECOVERY_RETRY_BASE_INTERVAL,
            RECOVERY_RETRY_MAX_INTERVAL,
        )
        .map(jitter)
    }

    #[define_opaque(RetryBackoffStrategy)]
    pub(crate) fn get_retry_backoff_strategy() -> RetryBackoffStrategy {
        get_retry_strategy().map(get_retry_backoff_future)
    }
}

pub(crate) use retry_strategy::*;
use risingwave_common::error::tonic::extra::{Score, ScoredError};
use risingwave_pb::meta::event_log::{Event, EventRecovery};

use crate::barrier::partial_graph::{
    PartialGraphEvent, PartialGraphManager, PartialGraphManagerEvent, WorkerEvent,
};

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    /// Recovery the whole cluster from the latest epoch.
    ///
    /// If `paused_reason` is `Some`, all data sources (including connectors and DMLs) will be
    /// immediately paused after recovery, until the user manually resume them either by restarting
    /// the cluster or `risectl` command. Used for debugging purpose.
    ///
    /// Returns the new state of the barrier manager after recovery.
    pub async fn recovery(&mut self, is_paused: bool, recovery_reason: RecoveryReason) {
        // Clear all control streams to release resources (connections to compute nodes) first.
        self.partial_graph_manager.clear_worker();

        let reason_str = match &recovery_reason {
            RecoveryReason::Bootstrap => "bootstrap".to_owned(),
            RecoveryReason::Failover(err) => {
                format!("failed over: {}", err.as_report())
            }
            RecoveryReason::Adhoc => "adhoc recovery".to_owned(),
        };
        self.context.abort_and_mark_blocked(None, recovery_reason);

        self.recovery_inner(is_paused, reason_str).await;
        self.context.mark_ready(MarkReadyOptions::Global {
            blocked_databases: self.checkpoint_control.recovering_databases().collect(),
        });
    }

    #[await_tree::instrument("recovery({recovery_reason})")]
    async fn recovery_inner(&mut self, is_paused: bool, recovery_reason: String) {
        let event_log_manager_ref = self.env.event_log_manager_ref();

        tracing::info!("recovery start!");
        event_log_manager_ref.add_event_logs(vec![Event::Recovery(
            EventRecovery::global_recovery_start(recovery_reason.clone()),
        )]);

        let retry_strategy = get_retry_strategy();

        // We take retry into consideration because this is the latency user sees for a cluster to
        // get recovered.
        let recovery_timer = GLOBAL_META_METRICS
            .recovery_latency
            .with_label_values(&["global"])
            .start_timer();

        let enable_per_database_isolation = self.enable_per_database_isolation();

        let recovery_future = tokio_retry::Retry::spawn(retry_strategy, || async {
            self.env.stream_client_pool().invalidate_all();
            // Notify in every recovery retry so foreground creation handlers can either cancel an
            // early-failing job or re-register their finish notifier after recovery.
            self.context
                .notify_creating_job_failed(None, recovery_reason.clone())
                .await;

            let runtime_info_snapshot = self
                .context
                .reload_runtime_info()
                .await?;
            let BarrierWorkerRuntimeInfoSnapshot {
                active_streaming_nodes,
                recovery_context,
                mut state_table_committed_epochs,
                mut state_table_log_epochs,
                mut mv_depended_subscriptions,
                mut creating_jobs,
                hummock_version_stats,
                database_infos,
                mut cdc_table_snapshot_splits,
            } = runtime_info_snapshot;

            let mut partial_graph_manager = PartialGraphManager::recover(
                    self.env.clone(),
                    active_streaming_nodes.current(),
                    self.context.clone(),
                )
                .await;
            {
                let mut empty_databases = HashSet::new();
                let mut collected_databases = HashMap::new();
                let mut collecting_databases = HashMap::new();
                let mut failed_databases = HashMap::new();
                for &database_id in recovery_context.fragment_context.database_map.keys() {
                    let mut recoverer = partial_graph_manager.start_recover();
                    let result: MetaResult<_> = try {
                        let Some(rendered_info) = render_runtime_info(
                            self.env.actor_id_generator(),
                            &active_streaming_nodes,
                            recoverer.control_stream_manager(),
                            &recovery_context,
                            database_id,
                        )
                            .inspect_err(|err: &MetaError| {
                                warn!(%database_id, err = %err.as_report(), "render runtime info failed");
                            })? else {
                            empty_databases.insert(database_id);
                            continue;
                        };
                        BarrierWorkerRuntimeInfoSnapshot::validate_database_info(
                            database_id,
                            &rendered_info.job_infos,
                            &active_streaming_nodes,
                            &rendered_info.stream_actors,
                            &state_table_committed_epochs,
                        )
                        .inspect_err(|err| {
                            warn!(%database_id, err = %err.as_report(), "rendered runtime info failed validation");
                        })?;
                        let RenderedDatabaseRuntimeInfo {
                            job_infos,
                            stream_actors,
                            mut source_splits,
                            batch_refresh,
                        } = rendered_info;
                        recoverer.inject_database_initial_barrier(
                            database_id,
                            job_infos,
                            &recovery_context.job_extra_info,
                            &mut state_table_committed_epochs,
                            &mut state_table_log_epochs,
                            &recovery_context.fragment_relations,
                            &stream_actors,
                            &mut source_splits,
                            &mut creating_jobs,
                            &mut mv_depended_subscriptions,
                            is_paused,
                            &hummock_version_stats,
                            &mut cdc_table_snapshot_splits,
                            batch_refresh,
                        )?
                    };
                    let collector = match result {
                        Ok(database) => {
                            DatabaseInitialBarrierCollector {
                                database_id,
                                initializing_partial_graphs: recoverer.all_initializing(),
                                database,
                            }
                        }
                        Err(e) => {
                            warn!(%database_id, e = %e.as_report(), "failed to inject database initial barrier");
                            assert!(failed_databases.insert(database_id, recoverer.failed()).is_none(), "non-duplicate");
                            continue;
                        }
                    };
                    if !collector.is_collected() {
                        assert!(collecting_databases.insert(database_id, collector).is_none());
                    } else {
                        warn!(%database_id, "database has no node to inject initial barrier");
                        assert!(collected_databases.insert(database_id, collector.finish()).is_none());
                    }
                }
                if !empty_databases.is_empty() {
                    info!(?empty_databases, "empty database in global recovery");
                }
                while !collecting_databases.is_empty() {
                    match partial_graph_manager.next_event(&self.context).await {
                        PartialGraphManagerEvent::Worker(_, WorkerEvent::WorkerConnected) => {
                            // not handle WorkerConnected yet
                        }
                        PartialGraphManagerEvent::Worker(worker_id, WorkerEvent::WorkerError { err, affected_partial_graphs }) => {
                            let affected_databases: HashSet<_> = affected_partial_graphs.into_iter().map(|partial_graph_id| {
                                let (database_id, _) = from_partial_graph_id(partial_graph_id);
                                database_id
                            }).collect();
                            warn!(%worker_id, err = %err.as_report(), "worker node failure during recovery");
                            for (failed_database_id, collector) in collecting_databases.extract_if(|database_id, collector| {
                                !collector.is_valid_after_worker_err(worker_id) || affected_databases.contains(database_id)
                            }) {
                                warn!(%failed_database_id, %worker_id, "database failed to recovery in global recovery due to worker node err");
                                let resetting_partial_graphs: HashSet<_> = collector.all_partial_graphs().collect();
                                partial_graph_manager.reset_partial_graphs(resetting_partial_graphs.iter().copied());
                                assert!(failed_databases.insert(failed_database_id, resetting_partial_graphs).is_none());
                            }
                        }
                        PartialGraphManagerEvent::PartialGraph(partial_graph_id, event) => {
                            match event {
                                PartialGraphEvent::BarrierCollected(_) => {
                                    unreachable!("no barrier collected event on initializing")
                                }
                                PartialGraphEvent::Reset(_) => {
                                    // Reset responses only carry diagnostic root errors. Recovery
                                    // itself only needs to track the partial graphs still resetting.
                                    let (database_id, _) =
                                        from_partial_graph_id(partial_graph_id);
                                    let resetting_partial_graphs = failed_databases
                                        .get_mut(&database_id)
                                        .expect("reset partial graph should belong to a failed database");
                                    assert!(
                                        resetting_partial_graphs.remove(&partial_graph_id),
                                        "partial graph {partial_graph_id} should be resetting"
                                    );
                                }
                                PartialGraphEvent::Error(worker_id) => {
                                    let (database_id, _) = from_partial_graph_id(partial_graph_id);
                                    if let Some(collector) = collecting_databases.remove(&database_id) {
                                        warn!(%database_id, %worker_id, "database reset during global recovery");
                                        let resetting_partial_graphs: HashSet<_> = collector.all_partial_graphs().collect();
                                        partial_graph_manager.reset_partial_graphs(resetting_partial_graphs.iter().copied());
                                        assert!(failed_databases.insert(database_id, resetting_partial_graphs).is_none());
                                    } else if let Some(database) = collected_databases.remove(&database_id) {
                                        warn!(%database_id, %worker_id, "database initialized but later reset during global recovery");
                                        let resetting_partial_graphs: HashSet<_> = database_partial_graphs(database_id, database.independent_checkpoint_job_controls.keys().copied()).collect();
                                        partial_graph_manager.reset_partial_graphs(resetting_partial_graphs.iter().copied());
                                        assert!(failed_databases.insert(database_id, resetting_partial_graphs).is_none());
                                    } else {
                                        assert!(failed_databases.contains_key(&database_id));
                                    }
                                }
                                PartialGraphEvent::Initialized => {
                                    let (database_id, _) = from_partial_graph_id(partial_graph_id);
                                    if failed_databases.contains_key(&database_id) {
                                        assert!(!collecting_databases.contains_key(&database_id));
                                        // ignore the lately initialized partial graph of failed database
                                        continue;
                                    }
                                    let Entry::Occupied(mut entry) = collecting_databases.entry(database_id) else {
                                        unreachable!("should exist")
                                    };
                                    let collector = entry.get_mut();
                                    collector.partial_graph_initialized(partial_graph_id);
                                    if collector.is_collected() {
                                        let collector = entry.remove();
                                        assert!(collected_databases.insert(database_id, collector.finish()).is_none());
                                    }
                                }
                            }
                        }
                    }
                }
                debug!("collected initial barrier");
                if !creating_jobs.is_empty() {
                    warn!(job_ids = ?creating_jobs.iter().collect_vec(), "unused recovered creating jobs in recovery");
                }
                if !mv_depended_subscriptions.is_empty() {
                    warn!(?mv_depended_subscriptions, "unused subscription infos in recovery");
                }
                if !state_table_committed_epochs.is_empty() {
                    warn!(?state_table_committed_epochs, "unused state table committed epoch in recovery");
                }
                if !enable_per_database_isolation && !failed_databases.is_empty() {
                    return Err(anyhow!(
                        "global recovery failed due to failure of databases {:?}",
                        failed_databases.keys().collect_vec()).into()
                    );
                }
                let checkpoint_control = CheckpointControl::recover(
                    collected_databases,
                    failed_databases,
                    hummock_version_stats,
                    self.env.clone(),
                );

                let reader = self.env.system_params_reader().await;
                let checkpoint_frequency = reader.checkpoint_frequency();
                let barrier_interval = Duration::from_millis(reader.barrier_interval_ms() as u64);
                let periodic_barriers = PeriodicBarriers::new(
                    barrier_interval,
                    checkpoint_frequency,
                    database_infos,
                );

                self.context
                    .refresh_table_refill_runtime_state_after_recovery()
                    .await?;

                Ok((
                    active_streaming_nodes,
                    partial_graph_manager,
                    checkpoint_control,
                    periodic_barriers,
                ))
            }
        }.inspect_err(|err: &MetaError| {
            tracing::error!(error = %err.as_report(), "recovery failed");
            event_log_manager_ref.add_event_logs(vec![Event::Recovery(
                EventRecovery::global_recovery_failure(recovery_reason.clone(), err.to_report_string()),
            )]);
            GLOBAL_META_METRICS.recovery_failure_cnt.with_label_values(&["global"]).inc();
        }))
        .instrument(tracing::info_span!("recovery_attempt"));

        let mut recover_txs = vec![];
        let mut update_barrier_requests = vec![];
        pin_mut!(recovery_future);
        let mut request_rx_closed = false;
        let new_state = loop {
            select! {
                biased;
                new_state = &mut recovery_future => {
                    break new_state.expect("Retry until recovery success.");
                }
                request = pin!(self.request_rx.recv()), if !request_rx_closed => {
                    let Some(request) = request else {
                        warn!("request rx channel closed during recovery");
                        request_rx_closed = true;
                        continue;
                    };
                    match request {
                        BarrierManagerRequest::GetBackfillProgress(tx) => {
                            let _ = tx.send(Err(anyhow!("cluster under recovery[{}]", recovery_reason).into()));
                        }
                        BarrierManagerRequest::GetFragmentBackfillProgress(tx) => {
                            let _ = tx.send(Err(anyhow!("cluster under recovery[{}]", recovery_reason).into()));
                        }
                        BarrierManagerRequest::GetCdcProgress(tx) => {
                            let _ = tx.send(Err(anyhow!("cluster under recovery[{}]", recovery_reason).into()));
                        }
                        BarrierManagerRequest::AdhocRecovery(tx) => {
                            recover_txs.push(tx);
                        }
                        BarrierManagerRequest::UpdateDatabaseBarrier(request) => {
                            update_barrier_requests.push(request);
                        }
                    }
                }
            }
        };

        let duration = recovery_timer.stop_and_record();

        (
            self.active_streaming_nodes,
            self.partial_graph_manager,
            self.checkpoint_control,
            self.periodic_barriers,
        ) = new_state;

        tracing::info!("recovery success");

        for UpdateDatabaseBarrierRequest {
            database_id,
            barrier_interval_ms,
            checkpoint_frequency,
            sender,
        } in update_barrier_requests
        {
            self.periodic_barriers.update_database_barrier(
                database_id,
                barrier_interval_ms,
                checkpoint_frequency,
            );
            let _ = sender.send(());
        }

        for tx in recover_txs {
            let _ = tx.send(());
        }

        let recovering_databases = self
            .checkpoint_control
            .recovering_databases()
            .map(|database| database.as_raw_id())
            .collect_vec();
        let running_databases = self
            .checkpoint_control
            .running_databases()
            .map(|database| database.as_raw_id())
            .collect_vec();

        event_log_manager_ref.add_event_logs(vec![Event::Recovery(
            EventRecovery::global_recovery_success(
                recovery_reason.clone(),
                duration as f32,
                running_databases,
                recovering_databases,
            ),
        )]);

        self.env
            .notification_manager()
            .notify_frontend_without_version(Operation::Update, Info::Recovery(Recovery {}));
        self.env
            .notification_manager()
            .notify_compute_without_version(Operation::Update, Info::Recovery(Recovery {}));
    }
}
