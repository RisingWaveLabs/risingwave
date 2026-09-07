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

use std::fmt::{Debug, Formatter};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use anyhow::Context;
use futures::StreamExt;
use parking_lot::Mutex;
use risingwave_common::array::DataChunk;
use risingwave_common::util::panic::FutureCatchUnwindExt;
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use risingwave_common::util::tracing::TracingContext;
use risingwave_expr::expr_context::expr_context_scope;
use risingwave_pb::PbFieldNotFound;
use risingwave_pb::batch_plan::{PbTaskId, PbTaskOutputId, PlanFragment};
use risingwave_pb::plan_common::ExprContext;
use risingwave_pb::task_service::task_info_response::TaskStatus;
use risingwave_pb::task_service::{GetDataResponse, TaskInfoResponse};
use thiserror_ext::AsReport;
use tokio::select;
use tokio::task::JoinHandle;
use tracing::Instrument;

use crate::error::BatchError::SenderError;
use crate::error::{BatchError, Result, SharedResult};
use crate::executor::{BoxedExecutor, ExecutorBuilder};
use crate::rpc::service::exchange::ExchangeWriter;
use crate::rpc::service::task_service::TaskInfoResponseResult;
use crate::task::BatchTaskContext;
use crate::task::channel::{ChanReceiverImpl, ChanSenderImpl, create_output_channel};

// Now we will only at most have 2 status for each status channel. Running -> Failed or Finished.
pub const TASK_STATUS_BUFFER_SIZE: usize = 2;

/// Send batch task status (local/distributed) to frontend.
///
///
/// Local mode use `StateReporter::Local`, Distributed mode use `StateReporter::Distributed` to send
/// status (Failed/Finished) update. `StateReporter::Mock` is only used in test and do not takes any
/// effect. Local sender only report Failed update, Distributed sender will also report
/// Finished/Pending/Starting/Aborted etc.
#[derive(Clone)]
pub enum StateReporter {
    Distributed(tokio::sync::mpsc::Sender<TaskInfoResponseResult>),
    Mock(),
}

impl StateReporter {
    pub async fn send(&mut self, val: TaskInfoResponse) -> Result<()> {
        match self {
            Self::Distributed(s) => s.send(Ok(val)).await.map_err(|_| SenderError),
            Self::Mock() => Ok(()),
        }
    }

    pub fn new_with_dist_sender(s: tokio::sync::mpsc::Sender<TaskInfoResponseResult>) -> Self {
        Self::Distributed(s)
    }

    pub fn new_with_test() -> Self {
        Self::Mock()
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Debug, Default)]
pub struct TaskId {
    pub task_id: u64,
    pub stage_id: u32,
    pub query_id: String,
}

#[derive(PartialEq, Eq, Hash, Clone, Default)]
pub struct TaskOutputId {
    pub task_id: TaskId,
    pub output_id: u64,
}

/// More compact formatter compared to derived `fmt::Debug`.
impl Debug for TaskOutputId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "TaskOutputId {{ query_id: \"{}\", stage_id: {}, task_id: {}, output_id: {} }}",
            self.task_id.query_id, self.task_id.stage_id, self.task_id.task_id, self.output_id
        ))
    }
}

impl From<&PbTaskId> for TaskId {
    fn from(prost: &PbTaskId) -> Self {
        TaskId {
            task_id: prost.task_id,
            stage_id: prost.stage_id,
            query_id: prost.query_id.clone(),
        }
    }
}

impl TaskId {
    pub fn to_prost(&self) -> PbTaskId {
        PbTaskId {
            task_id: self.task_id,
            stage_id: self.stage_id,
            query_id: self.query_id.clone(),
        }
    }
}

impl TryFrom<&PbTaskOutputId> for TaskOutputId {
    type Error = PbFieldNotFound;

    fn try_from(prost: &PbTaskOutputId) -> std::result::Result<Self, PbFieldNotFound> {
        Ok(TaskOutputId {
            task_id: TaskId::from(prost.get_task_id()?),
            output_id: prost.get_output_id(),
        })
    }
}

impl TaskOutputId {
    pub fn to_prost(&self) -> PbTaskOutputId {
        PbTaskOutputId {
            task_id: Some(self.task_id.to_prost()),
            output_id: self.output_id,
        }
    }
}

pub struct TaskOutput {
    receiver: ChanReceiverImpl,
    output_id: TaskOutputId,
    failure: Arc<Mutex<Option<Arc<BatchError>>>>,
}

impl std::fmt::Debug for TaskOutput {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskOutput")
            .field("output_id", &self.output_id)
            .field("failure", &self.failure)
            .finish_non_exhaustive()
    }
}

impl TaskOutput {
    /// Write the data in serialized format to `ExchangeWriter`.
    /// Return whether the data stream is finished.
    async fn take_data_inner(
        &mut self,
        writer: &mut impl ExchangeWriter,
        at_most_num: Option<usize>,
    ) -> Result<bool> {
        let mut cnt: usize = 0;
        let limited = at_most_num.is_some();
        let at_most_num = at_most_num.unwrap_or(usize::MAX);
        loop {
            if limited && cnt >= at_most_num {
                return Ok(false);
            }
            match self.receiver.recv().await {
                // Received some data
                Ok(Some(chunk)) => {
                    trace!(
                        "Task output id: {:?}, data len: {:?}",
                        self.output_id,
                        chunk.cardinality()
                    );
                    let pb = chunk.to_protobuf().await;
                    let resp = GetDataResponse {
                        record_batch: Some(pb),
                    };
                    writer.write(Ok(resp)).await?;
                }
                // Reached EOF
                Ok(None) => {
                    break;
                }
                // Error happened
                Err(e) => {
                    writer.write(Err(tonic::Status::from(&*e))).await?;
                    break;
                }
            }
            cnt += 1;
        }
        Ok(true)
    }

    /// Take at most num data and write the data in serialized format to `ExchangeWriter`.
    /// Return whether the data stream is finished.
    pub async fn take_data_with_num(
        &mut self,
        writer: &mut impl ExchangeWriter,
        num: usize,
    ) -> Result<bool> {
        self.take_data_inner(writer, Some(num)).await
    }

    /// Take all data and write the data in serialized format to `ExchangeWriter`.
    pub async fn take_data(&mut self, writer: &mut impl ExchangeWriter) -> Result<()> {
        let finish = self.take_data_inner(writer, None).await?;
        assert!(finish);
        Ok(())
    }

    /// Directly takes data without serialization.
    pub async fn direct_take_data(&mut self) -> SharedResult<Option<DataChunk>> {
        Ok(self.receiver.recv().await?.map(|c| c.into_data_chunk()))
    }

    pub fn id(&self) -> &TaskOutputId {
        &self.output_id
    }
}

#[derive(Clone, Debug)]
pub enum ShutdownMsg {
    /// Used in init, it never occur in receiver later.
    Init,
    Abort(String),
    Cancel,
}

/// A token which can be used to signal a shutdown request.
#[derive(Clone)]
pub struct ShutdownSender(tokio::sync::watch::Sender<ShutdownMsg>);

impl ShutdownSender {
    /// Send a cancel message. Return true if the message is sent successfully.
    pub fn cancel(&self) -> bool {
        self.0.send(ShutdownMsg::Cancel).is_ok()
    }

    /// Send an abort message. Return true if the message is sent successfully.
    pub fn abort(&self, msg: impl Into<String>) -> bool {
        self.0.send(ShutdownMsg::Abort(msg.into())).is_ok()
    }
}

/// A token which can be used to receive a shutdown signal.
#[derive(Clone)]
pub struct ShutdownToken(tokio::sync::watch::Receiver<ShutdownMsg>);

impl ShutdownToken {
    /// Create an empty token.
    pub fn empty() -> Self {
        Self::new().1
    }

    /// Create a new token.
    pub fn new() -> (ShutdownSender, Self) {
        let (tx, rx) = tokio::sync::watch::channel(ShutdownMsg::Init);
        (ShutdownSender(tx), ShutdownToken(rx))
    }

    /// Return error if the shutdown token has been triggered.
    pub fn check(&self) -> Result<()> {
        match &*self.0.borrow() {
            ShutdownMsg::Init => Ok(()),
            msg => bail!("Receive shutdown msg: {msg:?}"),
        }
    }

    /// Wait until cancellation is requested.
    ///
    /// # Cancel safety
    /// This method is cancel safe.
    pub async fn cancelled(&mut self) {
        if matches!(*self.0.borrow(), ShutdownMsg::Init)
            && let Err(_err) = self.0.changed().await
        {
            std::future::pending::<()>().await;
        }
    }

    /// Return true if the shutdown token has been triggered.
    pub fn is_cancelled(&self) -> bool {
        !matches!(*self.0.borrow(), ShutdownMsg::Init)
    }

    /// Return the current shutdown message.
    pub fn message(&self) -> ShutdownMsg {
        self.0.borrow().clone()
    }
}

/// `BatchTaskExecution` represents a single task execution.
pub struct BatchTaskExecution {
    /// Task id.
    task_id: TaskId,

    /// Inner plan to execute.
    plan: PlanFragment,

    /// Task state.
    state: Mutex<TaskStatus>,

    /// Receivers data of the task.
    receivers: Mutex<Vec<Option<ChanReceiverImpl>>>,

    /// Sender for sending chunks between different executors.
    sender: ChanSenderImpl,

    /// Context for task execution
    context: Arc<dyn BatchTaskContext>,

    /// The execution failure.
    failure: Arc<Mutex<Option<Arc<BatchError>>>>,

    /// Runtime for the batch tasks.
    runtime: Arc<BackgroundShutdownRuntime>,

    shutdown_tx: ShutdownSender,
    shutdown_rx: ShutdownToken,
    heartbeat_join_handle: Mutex<Option<JoinHandle<()>>>,

    /// Registry for await-tree. The task will be registered as a root when spawned,
    /// so that it shows up in the await-tree dump.
    await_tree_reg: Option<await_tree::Registry>,
}

impl BatchTaskExecution {
    pub fn new(
        prost_tid: &PbTaskId,
        plan: PlanFragment,
        context: Arc<dyn BatchTaskContext>,
        runtime: Arc<BackgroundShutdownRuntime>,
        await_tree_reg: Option<await_tree::Registry>,
    ) -> Result<Self> {
        let task_id = TaskId::from(prost_tid);

        let (sender, receivers) = create_output_channel(
            plan.get_exchange_info()?,
            context.get_config().developer.output_channel_size,
        )?;

        let mut rts = Vec::new();
        rts.extend(receivers.into_iter().map(Some));

        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        Ok(Self {
            task_id,
            plan,
            state: Mutex::new(TaskStatus::Pending),
            receivers: Mutex::new(rts),
            failure: Arc::new(Mutex::new(None)),
            context,
            runtime,
            sender,
            shutdown_tx,
            shutdown_rx,
            heartbeat_join_handle: Mutex::new(None),
            await_tree_reg,
        })
    }

    pub fn get_task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// `async_execute` executes the task in background, it spawns a tokio coroutine and returns
    /// immediately. The result produced by the task will be sent to one or more channels, according
    /// to a particular shuffling strategy. For example, in hash shuffling, the result will be
    /// hash partitioned across multiple channels.
    /// To obtain the result, one must pick one of the channels to consume via [`TaskOutputId`]. As
    /// such, parallel consumers are able to consume the result independently.
    pub async fn async_execute(
        self: Arc<Self>,
        state_tx: Option<StateReporter>,
        tracing_context: TracingContext,
        expr_context: ExprContext,
    ) -> Result<()> {
        let mut state_tx = state_tx;
        trace!(
            "Prepare executing plan [{:?}]: {}",
            self.task_id,
            serde_json::to_string_pretty(self.plan.get_root()?).unwrap()
        );

        let exec = expr_context_scope(
            expr_context.clone(),
            ExecutorBuilder::new(
                self.plan.root.as_ref().unwrap(),
                &self.task_id,
                self.context.clone(),
                self.shutdown_rx.clone(),
            )
            .build(),
        )
        .await?;

        let sender = self.sender.clone();
        let _failure = self.failure.clone();
        let task_id = self.task_id.clone();

        // After we init the output receivers, it's must safe to schedule next stage -- able to send
        // TaskStatus::Running here.
        // Init the state receivers. Swap out later.
        self.change_state_notify(TaskStatus::Running, state_tx.as_mut(), None)
            .await?;

        // Clone `self` to make compiler happy because of the move block.
        let t_1 = self.clone();
        let this = self.clone();
        async fn notify_panic(
            this: &BatchTaskExecution,
            state_tx: Option<&mut StateReporter>,
            message: Option<&str>,
        ) {
            let err_str = if let Some(message) = message {
                format!("execution panic: {}", message)
            } else {
                "execution panic".into()
            };

            if let Err(e) = this
                .change_state_notify(TaskStatus::Failed, state_tx, Some(err_str))
                .await
            {
                warn!(
                    error = %e.as_report(),
                    "The status receiver in FE has closed so the status push is failed",
                );
            }
        }
        // Spawn task for real execution.
        let fut = async move {
            trace!("Executing plan [{:?}]", task_id);
            let sender = sender;
            let mut state_tx_1 = state_tx.clone();

            let task = |task_id: TaskId| async move {
                let span = tracing_context.attach(tracing::info_span!(
                    "batch_execute",
                    task_id = task_id.task_id,
                    stage_id = task_id.stage_id,
                    query_id = task_id.query_id,
                ));

                // We should only pass a reference of sender to execution because we should only
                // close it after task error has been set.
                expr_context_scope(
                    expr_context,
                    t_1.run(exec, sender, state_tx_1.as_mut()).instrument(span),
                )
                .await;
            };

            if let Err(error) = AssertUnwindSafe(task(task_id.clone()))
                .rw_catch_unwind()
                .await
            {
                let message = panic_message::get_panic_message(&error);
                error!(?task_id, error = message, "Batch task panic");
                notify_panic(&this, state_tx.as_mut(), message).await;
            }
        };

        if let Some(reg) = self.await_tree_reg.clone() {
            let key = crate::task::await_tree_key::BatchTask(self.task_id.clone());
            let span = await_tree::span!(
                "Batch Task (query {} stage {} task {})",
                self.task_id.query_id,
                self.task_id.stage_id,
                self.task_id.task_id
            );
            self.runtime.spawn(reg.register(key, span).instrument(fut));
        } else {
            self.runtime.spawn(fut);
        }

        Ok(())
    }

    /// Change state and notify frontend for task status via streaming GRPC.
    pub async fn change_state_notify(
        &self,
        task_status: TaskStatus,
        state_tx: Option<&mut StateReporter>,
        err_str: Option<String>,
    ) -> Result<()> {
        self.change_state(task_status);
        // Notify frontend the task status.
        if let Some(reporter) = state_tx {
            reporter
                .send(TaskInfoResponse {
                    task_id: Some(self.task_id.to_prost()),
                    task_status: task_status.into(),
                    error_message: err_str.unwrap_or("".to_owned()),
                })
                .await
        } else {
            Ok(())
        }
    }

    pub fn change_state(&self, task_status: TaskStatus) {
        *self.state.lock() = task_status;
        tracing::debug!(
            "Task {:?} state changed to {:?}",
            &self.task_id,
            task_status
        );
    }

    async fn run(
        &self,
        root: BoxedExecutor,
        mut sender: ChanSenderImpl,
        state_tx: Option<&mut StateReporter>,
    ) {
        self.context
            .batch_metrics()
            .as_ref()
            .inspect(|m| m.batch_manager_metrics().task_num.inc());
        let mut data_chunk_stream = root.execute();
        let mut state;
        let mut error = None;

        let mut shutdown_rx = self.shutdown_rx.clone();
        loop {
            select! {
                biased;
                // `shutdown_rx` can't be removed here to avoid `sender.send(data_chunk)` blocked whole execution.
                _ = shutdown_rx.cancelled() => {
                    match self.shutdown_rx.message() {
                        ShutdownMsg::Abort(e) => {
                            error = Some(BatchError::Aborted(e));
                            state = TaskStatus::Aborted;
                            break;
                        }
                        ShutdownMsg::Cancel => {
                            state = TaskStatus::Cancelled;
                            break;
                        }
                        ShutdownMsg::Init => {
                            unreachable!("Init message should not be received here!")
                        }
                    }
                }
                data_chunk = data_chunk_stream.next()=> {
                    match data_chunk {
                        Some(Ok(data_chunk)) => {
                            if let Err(e) = sender.send(data_chunk).await {
                                match e {
                                    BatchError::SenderError => {
                                        // This is possible since when we have limit executor in parent
                                        // stage, it may early stop receiving data from downstream, which
                                        // leads to close of channel.
                                        warn!("Task receiver closed!");
                                        state = TaskStatus::Finished;
                                        break;
                                    }
                                    x => {
                                        error!("Failed to send data!");
                                        error = Some(x);
                                        state = TaskStatus::Failed;
                                        break;
                                    }
                                }
                            }
                        }
                        Some(Err(e)) => match self.shutdown_rx.message() {
                            ShutdownMsg::Init => {
                                // There is no message received from shutdown channel, which means it caused
                                // task failed.
                                error!(error = %e.as_report(), "Batch task failed");
                                error = Some(e);
                                state = TaskStatus::Failed;
                                break;
                            }
                            ShutdownMsg::Abort(_) => {
                                error = Some(e);
                                state = TaskStatus::Aborted;
                                break;
                            }
                            ShutdownMsg::Cancel => {
                                state = TaskStatus::Cancelled;
                                break;
                            }
                        },
                        None => {
                            debug!("Batch task {:?} finished successfully.", self.task_id);
                            state = TaskStatus::Finished;
                            break;
                        }
                    }
                }
            }
        }

        let error = error.map(Arc::new);
        self.failure.lock().clone_from(&error);
        let err_str = error.as_ref().map(|e| e.to_report_string());
        if let Err(e) = sender.close(error).await {
            match e {
                SenderError => {
                    // This is possible since when we have limit executor in parent
                    // stage, it may early stop receiving data from downstream, which
                    // leads to close of channel.
                    warn!("Task receiver closed when sending None!");
                }
                _x => {
                    error!("Failed to close task output channel: {:?}", self.task_id);
                    state = TaskStatus::Failed;
                }
            }
        }

        if let Err(e) = self.change_state_notify(state, state_tx, err_str).await {
            warn!(
                error = %e.as_report(),
                "The status receiver in FE has closed so the status push is failed",
            );
        }

        self.context
            .batch_metrics()
            .as_ref()
            .inspect(|m| m.batch_manager_metrics().task_num.dec());
    }

    pub fn abort(&self, err_msg: String) {
        // No need to set state to be Aborted here cuz it will be set by shutdown receiver.
        // Stop task execution.
        if self.shutdown_tx.abort(err_msg) {
            info!("Abort task {:?} done", self.task_id);
        } else {
            debug!("The task has already died before this request.")
        }
    }

    pub fn cancel(&self) {
        if !self.shutdown_tx.cancel() {
            debug!("The task has already died before this request.");
        }
    }

    pub fn get_task_output(&self, output_id: &PbTaskOutputId) -> Result<TaskOutput> {
        let task_id = TaskId::from(output_id.get_task_id()?);
        let receiver = self.receivers.lock()[output_id.get_output_id() as usize]
            .take()
            .with_context(|| {
                format!(
                    "Task{:?}'s output{} has already been taken.",
                    task_id,
                    output_id.get_output_id(),
                )
            })?;
        let task_output = TaskOutput {
            receiver,
            output_id: output_id.try_into()?,
            failure: self.failure.clone(),
        };
        Ok(task_output)
    }

    pub fn check_if_running(&self) -> Result<()> {
        if *self.state.lock() != TaskStatus::Running {
            bail!("task {:?} is not running", self.get_task_id());
        }
        Ok(())
    }

    pub fn check_if_aborted(&self) -> Result<bool> {
        match *self.state.lock() {
            TaskStatus::Aborted => Ok(true),
            TaskStatus::Finished => bail!("task {:?} has been finished", self.get_task_id()),
            _ => Ok(false),
        }
    }

    /// Check the task status: whether has ended.
    pub fn is_end(&self) -> bool {
        let guard = self.state.lock();
        !(*guard == TaskStatus::Running || *guard == TaskStatus::Pending)
    }
}

impl BatchTaskExecution {
    pub(crate) fn set_heartbeat_join_handle(&self, join_handle: JoinHandle<()>) {
        *self.heartbeat_join_handle.lock() = Some(join_handle);
    }

    pub(crate) fn heartbeat_join_handle(&self) -> Option<JoinHandle<()>> {
        self.heartbeat_join_handle.lock().take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_output_id_debug() {
        let task_id = TaskId {
            task_id: 1,
            stage_id: 2,
            query_id: "abc".to_owned(),
        };
        let task_output_id = TaskOutputId {
            task_id,
            output_id: 3,
        };
        assert_eq!(
            format!("{:?}", task_output_id),
            "TaskOutputId { query_id: \"abc\", stage_id: 2, task_id: 1, output_id: 3 }"
        );
    }
}
