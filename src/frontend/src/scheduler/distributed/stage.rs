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
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use std::{assert_matches, mem};

use StageEvent::Failed;
use anyhow::anyhow;
use arc_swap::ArcSwap;
use futures::stream::Fuse;
use futures::{StreamExt, TryStreamExt, stream};
use futures_async_stream::for_await;
use itertools::Itertools;
use risingwave_batch::error::BatchError;
use risingwave_batch::executor::ExecutorBuilder;
use risingwave_batch::task::{ShutdownMsg, ShutdownSender, ShutdownToken, TaskId as TaskIdBatch};
use risingwave_batch::worker_manager::worker_node_manager::WorkerNodeSelector;
use risingwave_common::array::DataChunk;
use risingwave_common::hash::WorkerSlotMapping;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::source::SplitMetaData;
use risingwave_expr::expr_context::expr_context_scope;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{
    DistributedLookupJoinNode, ExchangeNode, ExchangeSource, MergeSortExchangeNode, PlanFragment,
    PlanNode as PbPlanNode, PlanNode, TaskId as PbTaskId, TaskOutputId,
};
use risingwave_pb::common::{HostAddress, WorkerNode};
use risingwave_pb::plan_common::ExprContext;
use risingwave_pb::task_service::{CancelTaskRequest, TaskInfoResponse};
use risingwave_rpc_client::ComputeClientPoolRef;
use risingwave_rpc_client::error::RpcError;
use rw_futures_util::select_all;
use thiserror_ext::AsReport;
use tokio::spawn;
use tokio::sync::RwLock;
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::Streaming;
use tracing::{Instrument, debug, error, warn};

use crate::catalog::catalog_service::CatalogReader;
use crate::catalog::{FragmentId, TableId};
use crate::optimizer::plan_node::BatchPlanNodeType;
use crate::scheduler::SchedulerError::{TaskExecutionError, TaskRunningOutOfMemory};
use crate::scheduler::distributed::QueryMessage;
use crate::scheduler::distributed::stage::StageState::Pending;
use crate::scheduler::plan_fragmenter::{
    ExecutionPlanNode, PartitionInfo, Query, ROOT_TASK_ID, StageId, TaskId,
};
use crate::scheduler::{ExecutionContextRef, SchedulerError, SchedulerResult};

const TASK_SCHEDULING_PARALLELISM: usize = 10;

#[derive(Debug)]
enum StageState {
    /// We put `msg_sender` in `Pending` state to avoid holding it in `StageExecution`. In this
    /// way, it could be efficiently moved into `StageRunner` instead of being cloned. This also
    /// ensures that the sender can get dropped once it is used up, preventing some issues caused
    /// by unnecessarily long lifetime.
    Pending {
        msg_sender: Sender<QueryMessage>,
    },
    Started,
    Running,
    Completed,
    Failed,
}

#[derive(Debug)]
pub enum StageEvent {
    Scheduled(StageId),
    ScheduledRoot(Receiver<SchedulerResult<DataChunk>>),
    /// Stage failed.
    Failed {
        id: StageId,
        reason: SchedulerError,
    },
    /// All tasks in stage finished.
    Completed(#[expect(dead_code)] StageId),
}

#[derive(Clone)]
pub struct TaskStatus {
    _task_id: TaskId,

    // None before task is scheduled.
    location: Option<HostAddress>,
}

struct TaskStatusHolder {
    inner: ArcSwap<TaskStatus>,
}

pub struct StageExecution {
    stage_id: StageId,
    query: Arc<Query>,
    worker_node_manager: WorkerNodeSelector,
    tasks: Arc<HashMap<TaskId, TaskStatusHolder>>,
    state: Arc<RwLock<StageState>>,
    shutdown_tx: RwLock<Option<ShutdownSender>>,
    /// Children stage executions.
    ///
    /// We use `Vec` here since children's size is usually small.
    children: Vec<Arc<StageExecution>>,
    compute_client_pool: ComputeClientPoolRef,
    catalog_reader: CatalogReader,

    /// Execution context ref
    ctx: ExecutionContextRef,
}

struct StageRunner {
    state: Arc<RwLock<StageState>>,
    stage_id: StageId,
    query: Arc<Query>,
    worker_node_manager: WorkerNodeSelector,
    tasks: Arc<HashMap<TaskId, TaskStatusHolder>>,
    // Send message to `QueryRunner` to notify stage state change.
    msg_sender: Sender<QueryMessage>,
    children: Vec<Arc<StageExecution>>,
    compute_client_pool: ComputeClientPoolRef,
    catalog_reader: CatalogReader,

    ctx: ExecutionContextRef,
}

impl TaskStatusHolder {
    fn new(task_id: TaskId) -> Self {
        let task_status = TaskStatus {
            _task_id: task_id,
            location: None,
        };

        Self {
            inner: ArcSwap::new(Arc::new(task_status)),
        }
    }

    fn get_status(&self) -> Arc<TaskStatus> {
        self.inner.load_full()
    }
}

impl StageExecution {
    pub fn new(
        stage_id: StageId,
        query: Arc<Query>,
        worker_node_manager: WorkerNodeSelector,
        msg_sender: Sender<QueryMessage>,
        children: Vec<Arc<StageExecution>>,
        compute_client_pool: ComputeClientPoolRef,
        catalog_reader: CatalogReader,
        ctx: ExecutionContextRef,
    ) -> Self {
        let tasks = (0..query.stage(stage_id).parallelism.unwrap())
            .map(|task_id| (task_id as u64, TaskStatusHolder::new(task_id as u64)))
            .collect();

        Self {
            stage_id,
            query,
            worker_node_manager,
            tasks: Arc::new(tasks),
            state: Arc::new(RwLock::new(Pending { msg_sender })),
            shutdown_tx: RwLock::new(None),
            children,
            compute_client_pool,
            catalog_reader,
            ctx,
        }
    }

    /// Starts execution of this stage, returns error if already started.
    pub async fn start(&self) {
        let mut s = self.state.write().await;
        let cur_state = mem::replace(&mut *s, StageState::Failed);
        match cur_state {
            Pending { msg_sender } => {
                let runner = StageRunner {
                    stage_id: self.stage_id,
                    query: self.query.clone(),
                    worker_node_manager: self.worker_node_manager.clone(),
                    tasks: self.tasks.clone(),
                    msg_sender,
                    children: self.children.clone(),
                    state: self.state.clone(),
                    compute_client_pool: self.compute_client_pool.clone(),
                    catalog_reader: self.catalog_reader.clone(),
                    ctx: self.ctx.clone(),
                };

                // The channel used for shutdown signal messaging.
                let (sender, receiver) = ShutdownToken::new();
                // Fill the shutdown sender.
                let mut holder = self.shutdown_tx.write().await;
                *holder = Some(sender);

                // Change state before spawn runner.
                *s = StageState::Started;

                let span = tracing::info_span!(
                    "stage",
                    "otel.name" = format!("Stage {}-{}", self.query.query_id.id, self.stage_id),
                    query_id = self.query.query_id.id,
                    stage_id = %self.stage_id,
                );
                self.ctx
                    .session()
                    .env()
                    .compute_runtime()
                    .spawn(async move { runner.run(receiver).instrument(span).await });

                tracing::trace!(
                    "Stage {:?}-{:?} started.",
                    self.query.query_id.id,
                    self.stage_id
                )
            }
            _ => {
                unreachable!("Only expect to schedule stage once");
            }
        }
    }

    pub async fn stop(&self, error: Option<String>) {
        // Send message to tell Stage Runner stop.
        if let Some(shutdown_tx) = self.shutdown_tx.write().await.take() {
            // It's possible that the stage has not been scheduled, so the channel sender is
            // None.

            if !if let Some(error) = error {
                shutdown_tx.abort(error)
            } else {
                shutdown_tx.cancel()
            } {
                // The stage runner handle has already closed. so do no-op.
                tracing::trace!(
                    "Failed to send stop message stage: {:?}-{:?}",
                    self.query.query_id,
                    self.stage_id
                );
            }
        }
    }

    pub async fn is_scheduled(&self) -> bool {
        let s = self.state.read().await;
        matches!(*s, StageState::Running | StageState::Completed)
    }

    pub async fn is_pending(&self) -> bool {
        let s = self.state.read().await;
        matches!(*s, StageState::Pending { .. })
    }

    pub async fn state(&self) -> &'static str {
        let s = self.state.read().await;
        match *s {
            Pending { .. } => "Pending",
            StageState::Started => "Started",
            StageState::Running => "Running",
            StageState::Completed => "Completed",
            StageState::Failed => "Failed",
        }
    }

    /// Returns all exchange sources for `output_id`. Each `ExchangeSource` is identified by
    /// producer's `TaskId` and `output_id` (consumer's `TaskId`), since each task may produce
    /// output to several channels.
    ///
    /// When this method is called, all tasks should have been scheduled, and their `worker_node`
    /// should have been set.
    pub fn all_exchange_sources_for(&self, output_id: u64) -> Vec<ExchangeSource> {
        self.tasks
            .iter()
            .map(|(task_id, status_holder)| {
                let task_output_id = TaskOutputId {
                    task_id: Some(PbTaskId {
                        query_id: self.query.query_id.id.clone(),
                        stage_id: self.stage_id.into(),
                        task_id: *task_id,
                    }),
                    output_id,
                };

                ExchangeSource {
                    task_output_id: Some(task_output_id),
                    host: Some(status_holder.inner.load_full().location.clone().unwrap()),
                    local_execute_plan: None,
                }
            })
            .collect()
    }
}

impl StageRunner {
    async fn run(mut self, shutdown_rx: ShutdownToken) {
        if let Err(e) = self.schedule_tasks_for_all(shutdown_rx).await {
            error!(
                error = %e.as_report(),
                query_id = ?self.query.query_id,
                stage_id = ?self.stage_id,
                "Failed to schedule tasks"
            );
            self.send_event(QueryMessage::Stage(Failed {
                id: self.stage_id,
                reason: e,
            }))
            .await;
        }
    }

    /// Send stage event to listener.
    async fn send_event(&self, event: QueryMessage) {
        if let Err(_e) = self.msg_sender.send(event).await {
            warn!("Failed to send event to Query Runner, may be killed by previous failed event");
        }
    }

    /// Schedule all tasks to CN and wait process all status messages from RPC. Note that when all
    /// task is created, it should tell `QueryRunner` to schedule next.
    async fn schedule_tasks(
        &mut self,
        mut shutdown_rx: ShutdownToken,
        expr_context: ExprContext,
    ) -> SchedulerResult<()> {
        let mut futures = vec![];
        let stage = &self.query.stage(self.stage_id);

        if let Some(table_scan_info) = stage.table_scan_info.as_ref()
            && let Some(vnode_bitmaps) = table_scan_info.partitions()
        {
            // If the stage has table scan nodes, we create tasks according to the data distribution
            // and partition of the table.
            // We let each task read one partition by setting the `vnode_ranges` of the scan node in
            // the task.
            // We schedule the task to the worker node that owns the data partition.
            let worker_slot_ids = vnode_bitmaps.keys().cloned().collect_vec();
            let workers = self
                .worker_node_manager
                .manager
                .get_workers_by_worker_slot_ids(&worker_slot_ids)?;

            for (i, (worker_slot_id, worker)) in worker_slot_ids
                .into_iter()
                .zip_eq_fast(workers.into_iter())
                .enumerate()
            {
                let task_id = PbTaskId {
                    query_id: self.query.query_id.id.clone(),
                    stage_id: self.stage_id.into(),
                    task_id: i as u64,
                };
                let vnode_ranges = vnode_bitmaps[&worker_slot_id].clone();
                let plan_fragment =
                    self.create_plan_fragment(i as u64, Some(PartitionInfo::Table(vnode_ranges)));
                futures.push(self.schedule_task(
                    task_id,
                    plan_fragment,
                    Some(worker),
                    expr_context.clone(),
                ));
            }
        } else if let Some(source_info) = stage.source_info.as_ref() {
            // If there is no file in source, the `chunk_size` is set to 1.
            let chunk_size = ((source_info.split_info().unwrap().len() as f32
                / stage.parallelism.unwrap() as f32)
                .ceil() as usize)
                .max(1);
            if source_info.split_info().unwrap().is_empty() {
                // No file in source, schedule an empty task.
                const EMPTY_TASK_ID: u64 = 0;
                let task_id = PbTaskId {
                    query_id: self.query.query_id.id.clone(),
                    stage_id: self.stage_id.into(),
                    task_id: EMPTY_TASK_ID,
                };
                let plan_fragment =
                    self.create_plan_fragment(EMPTY_TASK_ID, Some(PartitionInfo::Source(vec![])));
                let worker =
                    self.choose_worker(&plan_fragment, EMPTY_TASK_ID as u32, stage.dml_table_id)?;
                futures.push(self.schedule_task(
                    task_id,
                    plan_fragment,
                    worker,
                    expr_context.clone(),
                ));
            } else {
                for (id, split) in source_info
                    .split_info()
                    .unwrap()
                    .chunks(chunk_size)
                    .enumerate()
                {
                    let task_id = PbTaskId {
                        query_id: self.query.query_id.id.clone(),
                        stage_id: self.stage_id.into(),
                        task_id: id as u64,
                    };
                    let plan_fragment = self.create_plan_fragment(
                        id as u64,
                        Some(PartitionInfo::Source(split.to_vec())),
                    );
                    let worker =
                        self.choose_worker(&plan_fragment, id as u32, stage.dml_table_id)?;
                    futures.push(self.schedule_task(
                        task_id,
                        plan_fragment,
                        worker,
                        expr_context.clone(),
                    ));
                }
            }
        } else if let Some(file_scan_info) = stage.file_scan_info.as_ref() {
            let chunk_size = (file_scan_info.file_location.len() as f32
                / stage.parallelism.unwrap() as f32)
                .ceil() as usize;
            for (id, files) in file_scan_info.file_location.chunks(chunk_size).enumerate() {
                let task_id = PbTaskId {
                    query_id: self.query.query_id.id.clone(),
                    stage_id: self.stage_id.into(),
                    task_id: id as u64,
                };
                let plan_fragment =
                    self.create_plan_fragment(id as u64, Some(PartitionInfo::File(files.to_vec())));
                let worker = self.choose_worker(&plan_fragment, id as u32, stage.dml_table_id)?;
                futures.push(self.schedule_task(
                    task_id,
                    plan_fragment,
                    worker,
                    expr_context.clone(),
                ));
            }
        } else {
            for id in 0..stage.parallelism.unwrap() {
                let task_id = PbTaskId {
                    query_id: self.query.query_id.id.clone(),
                    stage_id: self.stage_id.into(),
                    task_id: id as u64,
                };
                let plan_fragment = self.create_plan_fragment(id as u64, None);
                let worker = self.choose_worker(&plan_fragment, id, stage.dml_table_id)?;
                futures.push(self.schedule_task(
                    task_id,
                    plan_fragment,
                    worker,
                    expr_context.clone(),
                ));
            }
        }

        // Await each future and convert them into a set of streams.
        let buffered = stream::iter(futures).buffer_unordered(TASK_SCHEDULING_PARALLELISM);
        let buffered_streams = buffered.try_collect::<Vec<_>>().await?;

        // Merge different task streams into a single stream.
        let cancelled = pin!(shutdown_rx.cancelled());
        let mut all_streams = select_all(buffered_streams).take_until(cancelled);

        // Process the stream until finished.
        let mut running_task_cnt = 0;
        let mut finished_task_cnt = 0;
        let mut sent_signal_to_next = false;

        while let Some(status_res_inner) = all_streams.next().await {
            match status_res_inner {
                Ok(status) => {
                    use risingwave_pb::task_service::task_info_response::TaskStatus as PbTaskStatus;
                    match PbTaskStatus::try_from(status.task_status).unwrap() {
                        PbTaskStatus::Running => {
                            running_task_cnt += 1;
                            // The task running count should always less or equal than the
                            // registered tasks number.
                            assert!(running_task_cnt <= self.tasks.keys().len());
                            // All tasks in this stage have been scheduled. Notify query runner to
                            // schedule next stage.
                            if running_task_cnt == self.tasks.keys().len() {
                                self.notify_stage_scheduled(QueryMessage::Stage(
                                    StageEvent::Scheduled(self.stage_id),
                                ))
                                .await;
                                sent_signal_to_next = true;
                            }
                        }

                        PbTaskStatus::Finished => {
                            finished_task_cnt += 1;
                            assert!(finished_task_cnt <= self.tasks.keys().len());
                            assert!(running_task_cnt >= finished_task_cnt);
                            if finished_task_cnt == self.tasks.keys().len() {
                                // All tasks finished without failure, we should not break
                                // this loop
                                self.notify_stage_completed().await;
                                sent_signal_to_next = true;
                                break;
                            }
                        }
                        PbTaskStatus::Aborted => {
                            // Currently, the only reason that we receive an abort status is that
                            // the task's memory usage is too high so
                            // it's aborted.
                            error!(
                                "Abort task {:?} because of excessive memory usage. Please try again later.",
                                status.task_id.unwrap()
                            );
                            self.notify_stage_state_changed(
                                |_| StageState::Failed,
                                QueryMessage::Stage(Failed {
                                    id: self.stage_id,
                                    reason: TaskRunningOutOfMemory,
                                }),
                            )
                            .await;
                            sent_signal_to_next = true;
                            break;
                        }
                        PbTaskStatus::Failed => {
                            // Task failed, we should fail whole query
                            error!(
                                "Task {:?} failed, reason: {:?}",
                                status.task_id.unwrap(),
                                status.error_message,
                            );
                            self.notify_stage_state_changed(
                                |_| StageState::Failed,
                                QueryMessage::Stage(Failed {
                                    id: self.stage_id,
                                    reason: TaskExecutionError(status.error_message),
                                }),
                            )
                            .await;
                            sent_signal_to_next = true;
                            break;
                        }
                        PbTaskStatus::Ping => {
                            debug!("Receive ping from task {:?}", status.task_id.unwrap());
                        }
                        status => {
                            // The remain possible variant is Failed, but now they won't be pushed
                            // from CN.
                            unreachable!("Unexpected task status {:?}", status);
                        }
                    }
                }
                Err(e) => {
                    // rpc error here, we should also notify stage failure
                    error!(
                        "Fetching task status in stage {:?} failed, reason: {:?}",
                        self.stage_id,
                        e.message()
                    );
                    self.notify_stage_state_changed(
                        |_| StageState::Failed,
                        QueryMessage::Stage(Failed {
                            id: self.stage_id,
                            reason: RpcError::from_batch_status(e).into(),
                        }),
                    )
                    .await;
                    sent_signal_to_next = true;
                    break;
                }
            }
        }

        tracing::trace!(
            "Stage [{:?}-{:?}], running task count: {}, finished task count: {}, sent signal to next: {}",
            self.query.query_id,
            self.stage_id,
            running_task_cnt,
            finished_task_cnt,
            sent_signal_to_next,
        );

        if let Some(shutdown) = all_streams.take_future() {
            tracing::trace!(
                "Stage [{:?}-{:?}] waiting for stopping signal.",
                self.query.query_id,
                self.stage_id
            );
            // Waiting for shutdown signal.
            shutdown.await;
        }

        // Received shutdown signal from query runner, should send abort RPC to all CNs.
        // change state to aborted. Note that the task cancel can only happen after schedule
        // all these tasks to CN. This can be an optimization for future:
        // How to stop before schedule tasks.
        tracing::trace!(
            "Stopping stage: {:?}-{:?}, task_num: {}",
            self.query.query_id,
            self.stage_id,
            self.tasks.len()
        );
        self.cancel_all_scheducancled_tasks().await?;

        tracing::trace!(
            "Stage runner [{:?}-{:?}] exited.",
            self.query.query_id,
            self.stage_id
        );
        Ok(())
    }

    async fn schedule_tasks_for_root(
        &mut self,
        mut shutdown_rx: ShutdownToken,
        expr_context: ExprContext,
    ) -> SchedulerResult<()> {
        let root_stage_id = self.stage_id;
        // Currently, the dml or table scan should never be root fragment, so the partition is None.
        // And root fragment only contain one task.
        let plan_fragment = self.create_plan_fragment(ROOT_TASK_ID, None);
        let plan_node = plan_fragment.root.unwrap();
        let task_id = TaskIdBatch {
            query_id: self.query.query_id.id.clone(),
            stage_id: root_stage_id.into(),
            task_id: 0,
        };

        // Notify QueryRunner to poll chunk from result_rx.
        let (result_tx, result_rx) = tokio::sync::mpsc::channel(
            self.ctx
                .session
                .env()
                .batch_config()
                .developer
                .root_stage_channel_size,
        );
        self.notify_stage_scheduled(QueryMessage::Stage(StageEvent::ScheduledRoot(result_rx)))
            .await;

        let executor = ExecutorBuilder::new(
            &plan_node,
            &task_id,
            self.ctx.to_batch_task_context(),
            shutdown_rx.clone(),
        );

        let shutdown_rx0 = shutdown_rx.clone();

        let result = expr_context_scope(expr_context, async {
            let executor = executor.build().await?;
            let chunk_stream = executor.execute();
            let cancelled = pin!(shutdown_rx.cancelled());
            #[for_await]
            for chunk in chunk_stream.take_until(cancelled) {
                if let Err(ref e) = chunk {
                    if shutdown_rx0.is_cancelled() {
                        break;
                    }
                    let err_str = e.to_report_string();
                    // This is possible if The Query Runner drop early before schedule the root
                    // executor. Detail described in https://github.com/risingwavelabs/risingwave/issues/6883#issuecomment-1348102037.
                    // The error format is just channel closed so no care.
                    if let Err(_e) = result_tx.send(chunk.map_err(|e| e.into())).await {
                        warn!("Root executor has been dropped before receive any events so the send is failed");
                    }
                    // Different from below, return this function and report error.
                    return Err(TaskExecutionError(err_str));
                } else {
                    // Same for below.
                    if let Err(_e) = result_tx.send(chunk.map_err(|e| e.into())).await {
                        warn!("Root executor has been dropped before receive any events so the send is failed");
                    }
                }
            }
            Ok(())
        }).await;

        if let Err(err) = &result {
            // If we encountered error when executing root stage locally, we have to notify the result fetcher, which is
            // returned by `distribute_execute` and being listened by the FE handler task. Otherwise the FE handler cannot
            // properly throw the error to the PG client.
            if let Err(_e) = result_tx
                .send(Err(TaskExecutionError(err.to_report_string())))
                .await
            {
                warn!("Send task execution failed");
            }
        }

        // Terminated by other tasks execution error, so no need to return error here.
        match shutdown_rx0.message() {
            ShutdownMsg::Abort(err_str) => {
                // Tell Query Result Fetcher to stop polling and attach failure reason as str.
                if let Err(_e) = result_tx.send(Err(TaskExecutionError(err_str))).await {
                    warn!("Send task execution failed");
                }
            }
            _ => self.notify_stage_completed().await,
        }

        tracing::trace!(
            "Stage runner [{:?}-{:?}] existed. ",
            self.query.query_id,
            self.stage_id
        );

        // We still have to throw the error in this current task, so that `StageRunner::run` can further
        // send `Failed` event to stop other stages.
        result.map(|_| ())
    }

    async fn schedule_tasks_for_all(&mut self, shutdown_rx: ShutdownToken) -> SchedulerResult<()> {
        let expr_context = ExprContext {
            time_zone: self.ctx.session().config().timezone(),
            strict_mode: self.ctx.session().config().batch_expr_strict_mode(),
        };
        // If root, we execute it locally.
        if !self.is_root_stage() {
            self.schedule_tasks(shutdown_rx, expr_context).await?;
        } else {
            self.schedule_tasks_for_root(shutdown_rx, expr_context)
                .await?;
        }
        Ok(())
    }

    #[inline(always)]
    fn get_fragment_id(&self, table_id: TableId) -> SchedulerResult<FragmentId> {
        self.catalog_reader
            .read_guard()
            .get_any_table_by_id(table_id)
            .map(|table| table.fragment_id)
            .map_err(|e| SchedulerError::Internal(anyhow!(e)))
    }

    #[inline(always)]
    fn get_table_dml_vnode_mapping(&self, table_id: TableId) -> SchedulerResult<WorkerSlotMapping> {
        let guard = self.catalog_reader.read_guard();

        let table = guard
            .get_any_table_by_id(table_id)
            .map_err(|e| SchedulerError::Internal(anyhow!(e)))?;

        let fragment_id = match table.dml_fragment_id.as_ref() {
            Some(dml_fragment_id) => dml_fragment_id,
            // Backward compatibility for those table without `dml_fragment_id`.
            None => &table.fragment_id,
        };

        self.worker_node_manager
            .manager
            .get_streaming_fragment_mapping(fragment_id)
            .map_err(|e| e.into())
    }

    fn choose_worker(
        &self,
        plan_fragment: &PlanFragment,
        task_id: u32,
        dml_table_id: Option<TableId>,
    ) -> SchedulerResult<Option<WorkerNode>> {
        let plan_node = plan_fragment.root.as_ref().expect("fail to get plan node");

        if let Some(table_id) = dml_table_id {
            let vnode_mapping = self.get_table_dml_vnode_mapping(table_id)?;
            let worker_slot_ids = vnode_mapping.iter_unique().collect_vec();
            let candidates = self
                .worker_node_manager
                .manager
                .get_workers_by_worker_slot_ids(&worker_slot_ids)?;
            if candidates.is_empty() {
                return Err(BatchError::EmptyWorkerNodes.into());
            }
            let stage = &self.query.stage(self.stage_id);
            let candidate = if stage.batch_enable_distributed_dml {
                // If distributed dml is enabled, we need to try our best to distribute dml tasks evenly to each worker.
                // Using a `task_id` could be helpful in this case.
                candidates[task_id as usize % candidates.len()].clone()
            } else {
                // If distributed dml is disabled, we need to guarantee that dml from the same session would be sent to a fixed worker/channel to provide a order guarantee.
                candidates[stage.session_id.0 as usize % candidates.len()].clone()
            };
            return Ok(Some(candidate));
        };

        if let Some(distributed_lookup_join_node) =
            Self::find_distributed_lookup_join_node(plan_node)
        {
            let fragment_id = self.get_fragment_id(
                distributed_lookup_join_node
                    .inner_side_table_desc
                    .as_ref()
                    .unwrap()
                    .table_id,
            )?;
            let id_to_worker_slots = self
                .worker_node_manager
                .fragment_mapping(fragment_id, self.query.batch_parallelism())?
                .iter_unique()
                .collect_vec();

            let worker_slot_id = id_to_worker_slots[task_id as usize];
            let candidates = self
                .worker_node_manager
                .manager
                .get_workers_by_worker_slot_ids(&[worker_slot_id])?;
            if candidates.is_empty() {
                return Err(BatchError::EmptyWorkerNodes.into());
            }
            Ok(Some(candidates[0].clone()))
        } else {
            Ok(None)
        }
    }

    fn find_distributed_lookup_join_node(
        plan_node: &PlanNode,
    ) -> Option<&DistributedLookupJoinNode> {
        let node_body = plan_node.node_body.as_ref().expect("fail to get node body");

        match node_body {
            NodeBody::DistributedLookupJoin(distributed_lookup_join_node) => {
                Some(distributed_lookup_join_node)
            }
            _ => plan_node
                .children
                .iter()
                .find_map(Self::find_distributed_lookup_join_node),
        }
    }

    /// Write message into channel to notify query runner current stage have been scheduled.
    async fn notify_stage_scheduled(&self, msg: QueryMessage) {
        self.notify_stage_state_changed(
            |old_state| {
                assert_matches!(old_state, StageState::Started);
                StageState::Running
            },
            msg,
        )
        .await
    }

    /// Notify query execution that this stage completed.
    async fn notify_stage_completed(&self) {
        self.notify_stage_state_changed(
            |old_state| {
                assert_matches!(old_state, StageState::Running);
                StageState::Completed
            },
            QueryMessage::Stage(StageEvent::Completed(self.stage_id)),
        )
        .await
    }

    async fn notify_stage_state_changed<F>(&self, new_state: F, msg: QueryMessage)
    where
        F: FnOnce(StageState) -> StageState,
    {
        {
            let mut s = self.state.write().await;
            let old_state = mem::replace(&mut *s, StageState::Failed);
            *s = new_state(old_state);
        }

        self.send_event(msg).await;
    }

    /// Abort all registered tasks. Note that here we do not care which part of tasks has already
    /// failed or completed, cuz the abort task will not fail if the task has already die.
    /// See PR (#4560).
    async fn cancel_all_scheducancled_tasks(&self) -> SchedulerResult<()> {
        // Set state to failed.
        // {
        //     let mut state = self.state.write().await;
        //     // Ignore if already finished.
        //     if let &StageState::Completed = &*state {
        //         return Ok(());
        //     }
        //     // FIXME: Be careful for state jump back.
        //     *state = StageState::Failed
        // }

        for (task, task_status) in &*self.tasks {
            // 1. Collect task info and client.
            let loc = &task_status.get_status().location;
            let addr = loc.as_ref().expect("Get address should not fail");
            let client = self
                .compute_client_pool
                .get_by_addr(HostAddr::from(addr))
                .await
                .map_err(|e| anyhow!(e))?;

            // 2. Send RPC to each compute node for each task asynchronously.
            let query_id = self.query.query_id.id.clone();
            let stage_id = self.stage_id;
            let task_id = *task;
            spawn(async move {
                if let Err(e) = client
                    .cancel(CancelTaskRequest {
                        task_id: Some(risingwave_pb::batch_plan::TaskId {
                            query_id: query_id.clone(),
                            stage_id: stage_id.into(),
                            task_id,
                        }),
                    })
                    .await
                {
                    error!(
                        error = %e.as_report(),
                        ?task_id,
                        ?query_id,
                        ?stage_id,
                        "Abort task failed",
                    );
                };
            });
        }
        Ok(())
    }

    async fn schedule_task(
        &self,
        task_id: PbTaskId,
        plan_fragment: PlanFragment,
        worker: Option<WorkerNode>,
        expr_context: ExprContext,
    ) -> SchedulerResult<Fuse<Streaming<TaskInfoResponse>>> {
        let mut worker = worker.unwrap_or(self.worker_node_manager.next_random_worker()?);
        let worker_node_addr = worker.host.take().unwrap();
        let compute_client = self
            .compute_client_pool
            .get_by_addr((&worker_node_addr).into())
            .await
            .inspect_err(|_| self.mask_failed_serving_worker(&worker))
            .map_err(|e| anyhow!(e))?;

        let t_id = task_id.task_id;

        let stream_status: Fuse<Streaming<TaskInfoResponse>> = compute_client
            .create_task(task_id, plan_fragment, expr_context)
            .await
            .inspect_err(|_| self.mask_failed_serving_worker(&worker))
            .map_err(|e| anyhow!(e))?
            .fuse();

        self.tasks[&t_id].inner.store(Arc::new(TaskStatus {
            _task_id: t_id,
            location: Some(worker_node_addr),
        }));

        Ok(stream_status)
    }

    fn create_plan_fragment(
        &self,
        task_id: TaskId,
        partition: Option<PartitionInfo>,
    ) -> PlanFragment {
        // Used to maintain auto-increment identity_id of a task.
        let mut identity_id = 0;

        let stage = &self.query.stage(self.stage_id);

        let plan_node_prost =
            self.convert_plan_node(&stage.root, task_id, partition, &mut identity_id);
        let exchange_info = stage.exchange_info.clone().unwrap();

        PlanFragment {
            root: Some(plan_node_prost),
            exchange_info: Some(exchange_info),
        }
    }

    fn convert_plan_node(
        &self,
        execution_plan_node: &ExecutionPlanNode,
        task_id: TaskId,
        partition: Option<PartitionInfo>,
        identity_id: &mut u64,
    ) -> PbPlanNode {
        // Generate identity
        let identity = {
            let identity_type = execution_plan_node.plan_node_type;
            let id = *identity_id;
            *identity_id += 1;
            format!("{:?}-{}", identity_type, id)
        };

        match execution_plan_node.plan_node_type {
            BatchPlanNodeType::BatchExchange => {
                // Find the stage this exchange node should fetch from and get all exchange sources.
                let child_stage = self
                    .children
                    .iter()
                    .find(|child_stage| {
                        child_stage.stage_id == execution_plan_node.source_stage_id.unwrap()
                    })
                    .unwrap();
                let exchange_sources = child_stage.all_exchange_sources_for(task_id);

                match &execution_plan_node.node {
                    NodeBody::Exchange(exchange_node) => PbPlanNode {
                        children: vec![],
                        identity,
                        node_body: Some(NodeBody::Exchange(ExchangeNode {
                            sources: exchange_sources,
                            sequential: exchange_node.sequential,
                            input_schema: execution_plan_node.schema.clone(),
                        })),
                    },
                    NodeBody::MergeSortExchange(sort_merge_exchange_node) => PbPlanNode {
                        children: vec![],
                        identity,
                        node_body: Some(NodeBody::MergeSortExchange(MergeSortExchangeNode {
                            exchange: Some(ExchangeNode {
                                sources: exchange_sources,
                                sequential: false,
                                input_schema: execution_plan_node.schema.clone(),
                            }),
                            column_orders: sort_merge_exchange_node.column_orders.clone(),
                        })),
                    },
                    _ => unreachable!(),
                }
            }
            BatchPlanNodeType::BatchSeqScan => {
                let node_body = execution_plan_node.node.clone();
                let NodeBody::RowSeqScan(mut scan_node) = node_body else {
                    unreachable!();
                };
                let partition = partition
                    .expect("no partition info for seq scan")
                    .into_table()
                    .expect("PartitionInfo should be TablePartitionInfo");
                scan_node.vnode_bitmap = Some(partition.vnode_bitmap.to_protobuf());
                scan_node.scan_ranges = partition.scan_ranges;
                PbPlanNode {
                    children: vec![],
                    identity,
                    node_body: Some(NodeBody::RowSeqScan(scan_node)),
                }
            }
            BatchPlanNodeType::BatchLogSeqScan => {
                let node_body = execution_plan_node.node.clone();
                let NodeBody::LogRowSeqScan(mut scan_node) = node_body else {
                    unreachable!();
                };
                let partition = partition
                    .expect("no partition info for seq scan")
                    .into_table()
                    .expect("PartitionInfo should be TablePartitionInfo");
                scan_node.vnode_bitmap = Some(partition.vnode_bitmap.to_protobuf());
                PbPlanNode {
                    children: vec![],
                    identity,
                    node_body: Some(NodeBody::LogRowSeqScan(scan_node)),
                }
            }
            BatchPlanNodeType::BatchSource | BatchPlanNodeType::BatchKafkaScan => {
                let node_body = execution_plan_node.node.clone();
                let NodeBody::Source(mut source_node) = node_body else {
                    unreachable!();
                };

                let partition = partition
                    .expect("no partition info for seq scan")
                    .into_source()
                    .expect("PartitionInfo should be SourcePartitionInfo");
                source_node.split = partition
                    .into_iter()
                    .map(|split| split.encode_to_bytes().into())
                    .collect_vec();
                PbPlanNode {
                    children: vec![],
                    identity,
                    node_body: Some(NodeBody::Source(source_node)),
                }
            }
            BatchPlanNodeType::BatchIcebergScan => {
                let node_body = execution_plan_node.node.clone();
                let NodeBody::IcebergScan(mut iceberg_scan_node) = node_body else {
                    unreachable!();
                };

                let partition = partition
                    .expect("no partition info for seq scan")
                    .into_source()
                    .expect("PartitionInfo should be SourcePartitionInfo");
                iceberg_scan_node.split = partition
                    .into_iter()
                    .map(|split| split.encode_to_bytes().into())
                    .collect_vec();
                PbPlanNode {
                    children: vec![],
                    identity,
                    node_body: Some(NodeBody::IcebergScan(iceberg_scan_node)),
                }
            }
            _ => {
                let children = execution_plan_node
                    .children
                    .iter()
                    .map(|e| self.convert_plan_node(e, task_id, partition.clone(), identity_id))
                    .collect();

                PbPlanNode {
                    children,
                    identity,
                    node_body: Some(execution_plan_node.node.clone()),
                }
            }
        }
    }

    fn is_root_stage(&self) -> bool {
        self.stage_id == 0.into()
    }

    fn mask_failed_serving_worker(&self, worker: &WorkerNode) {
        if !worker.property.as_ref().is_some_and(|p| p.is_serving) {
            return;
        }
        let duration = Duration::from_secs(std::cmp::max(
            self.ctx
                .session
                .env()
                .batch_config()
                .mask_worker_temporary_secs as u64,
            1,
        ));
        self.worker_node_manager
            .manager
            .mask_worker_node(worker.id, duration);
    }
}
