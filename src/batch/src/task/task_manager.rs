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

use std::collections::{HashMap, hash_map};
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use parking_lot::Mutex;
use risingwave_common::config::BatchConfig;
use risingwave_common::memory::MemoryContext;
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use risingwave_common::util::tracing::TracingContext;
use risingwave_pb::batch_plan::{PbTaskId, PbTaskOutputId, PlanFragment};
use risingwave_pb::plan_common::ExprContext;
use risingwave_pb::task_service::task_info_response::TaskStatus;
use risingwave_pb::task_service::{GetDataResponse, TaskInfoResponse};
use tokio::sync::mpsc::Sender;
use tonic::Status;

use super::BatchTaskContext;
use crate::error::Result;
use crate::monitor::BatchManagerMetrics;
use crate::rpc::service::exchange::GrpcExchangeWriter;
use crate::task::{BatchTaskExecution, StateReporter, TaskId, TaskOutput, TaskOutputId};

pub mod await_tree_key {
    /// Await-tree key type for batch tasks.
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct BatchTask(pub crate::task::TaskId);
}

/// `BatchManager` is responsible for managing all batch tasks.
#[derive(Clone)]
pub struct BatchManager {
    /// Every task id has a corresponding task execution.
    tasks: Arc<Mutex<HashMap<TaskId, Arc<BatchTaskExecution>>>>,

    /// Runtime for the batch manager.
    runtime: Arc<BackgroundShutdownRuntime>,

    /// Batch configuration
    config: BatchConfig,

    /// Memory context used for batch tasks in cn.
    mem_context: MemoryContext,

    /// Metrics for batch manager.
    metrics: Arc<BatchManagerMetrics>,

    /// Registry for await-tree.
    await_tree_reg: Option<await_tree::Registry>,
}

impl BatchManager {
    pub fn new(
        config: BatchConfig,
        metrics: Arc<BatchManagerMetrics>,
        mem_limit: u64,
        await_tree_config: Option<await_tree::Config>,
    ) -> Self {
        let runtime = {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            if let Some(worker_threads_num) = config.worker_threads_num {
                builder.worker_threads(worker_threads_num);
            }
            builder
                .thread_name("rw-batch")
                .enable_all()
                .build()
                .unwrap()
        };

        let mem_context = MemoryContext::root(metrics.batch_total_mem.clone(), mem_limit);
        BatchManager {
            tasks: Arc::new(Mutex::new(HashMap::new())),
            runtime: Arc::new(runtime.into()),
            config,
            metrics,
            mem_context,
            await_tree_reg: await_tree_config.map(await_tree::Registry::new),
        }
    }

    /// Get the registry of await-trees.
    pub fn await_tree_reg(&self) -> Option<&await_tree::Registry> {
        self.await_tree_reg.as_ref()
    }

    pub(crate) fn metrics(&self) -> Arc<BatchManagerMetrics> {
        self.metrics.clone()
    }

    pub fn memory_context_ref(&self) -> MemoryContext {
        self.mem_context.clone()
    }

    pub async fn fire_task(
        self: &Arc<Self>,
        tid: &PbTaskId,
        plan: PlanFragment,
        context: Arc<dyn BatchTaskContext>, // ComputeNodeContext
        state_reporter: StateReporter,
        tracing_context: TracingContext,
        expr_context: ExprContext,
    ) -> Result<()> {
        trace!("Received task id: {:?}, plan: {:?}", tid, plan);
        let task = BatchTaskExecution::new(
            tid,
            plan,
            context,
            self.runtime(),
            self.await_tree_reg.clone(),
        )?;
        let task_id = task.get_task_id().clone();
        let task = Arc::new(task);
        // Here the task id insert into self.tasks is put in front of `.async_execute`, cuz when
        // send `TaskStatus::Running` in `.async_execute`, the query runner may schedule next stage,
        // it's possible do not found parent task id in theory.
        let ret = if let hash_map::Entry::Vacant(e) = self.tasks.lock().entry(task_id.clone()) {
            e.insert(task.clone());

            let this = self.clone();
            let task_id = task_id.clone();
            let state_reporter = state_reporter.clone();
            let heartbeat_join_handle = self.runtime.spawn(async move {
                this.start_task_heartbeat(state_reporter, task_id).await;
            });
            task.set_heartbeat_join_handle(heartbeat_join_handle);

            Ok(())
        } else {
            bail!(
                "cannot create a duplicate task with the same ID: {:?}",
                task_id,
            );
        };
        task.async_execute(Some(state_reporter), tracing_context, expr_context)
            .await
            .inspect_err(|_| {
                self.cancel_task(&task_id.to_prost());
            })?;
        ret
    }

    #[cfg(test)]
    async fn fire_task_for_test(
        self: &Arc<Self>,
        tid: &PbTaskId,
        plan: PlanFragment,
    ) -> Result<()> {
        use crate::task::ComputeNodeContext;

        self.fire_task(
            tid,
            plan,
            ComputeNodeContext::for_test(),
            StateReporter::new_with_test(),
            TracingContext::none(),
            ExprContext {
                time_zone: "UTC".to_owned(),
                strict_mode: false,
            },
        )
        .await
    }

    async fn start_task_heartbeat(&self, mut state_reporter: StateReporter, task_id: TaskId) {
        let _metric_guard = scopeguard::guard((), |_| {
            tracing::debug!("heartbeat worker for task {:?} stopped", task_id);
            self.metrics.batch_heartbeat_worker_num.dec();
        });
        tracing::debug!("heartbeat worker for task {:?} started", task_id);
        self.metrics.batch_heartbeat_worker_num.inc();
        // The heartbeat is to ensure task cancellation when frontend's cancellation request fails
        // to reach compute node (for any reason like RPC fails, frontend crashes).
        let mut heartbeat_interval = tokio::time::interval(core::time::Duration::from_secs(60));
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat_interval.reset();
        loop {
            heartbeat_interval.tick().await;
            if !self.tasks.lock().contains_key(&task_id) {
                break;
            }
            if state_reporter
                .send(TaskInfoResponse {
                    task_id: Some(task_id.to_prost()),
                    task_status: TaskStatus::Ping.into(),
                    error_message: "".to_owned(),
                })
                .await
                .is_err()
            {
                tracing::warn!("try to cancel task {:?} due to heartbeat", task_id);
                // Task may have been cancelled, but it's fine to `cancel_task` again.
                self.cancel_task(&task_id.to_prost());
                break;
            }
        }
    }

    pub fn get_data(
        &self,
        tx: Sender<std::result::Result<GetDataResponse, Status>>,
        peer_addr: SocketAddr,
        pb_task_output_id: &PbTaskOutputId,
    ) -> Result<()> {
        let task_id = TaskOutputId::try_from(pb_task_output_id)?;
        tracing::debug!(target: "events::compute::exchange", peer_addr = %peer_addr, from = ?task_id, "serve exchange RPC");
        let mut task_output = self.take_output(pb_task_output_id)?;
        self.runtime.spawn(async move {
            let mut writer = GrpcExchangeWriter::new(tx.clone());
            tokio::select! {
                result = task_output.take_data(&mut writer) => match result {
                    Ok(_) => {
                        tracing::trace!(
                            from = ?task_id,
                            "exchanged {} chunks",
                            writer.written_chunks(),
                        );
                        Ok(())
                    }
                    Err(e) => tx.send(Err(e.into())).await,
                },
                _ = tx.closed() => Ok(()),
            }
        });
        Ok(())
    }

    pub fn take_output(&self, output_id: &PbTaskOutputId) -> Result<TaskOutput> {
        let task_id = TaskId::from(output_id.get_task_id()?);
        self.tasks
            .lock()
            .get(&task_id)
            .with_context(|| format!("task {:?} not found", task_id))?
            .get_task_output(output_id)
    }

    pub fn cancel_task(&self, sid: &PbTaskId) {
        let sid = TaskId::from(sid);
        match self.tasks.lock().remove(&sid) {
            Some(task) => {
                tracing::trace!("Removed task: {:?}", task.get_task_id());
                // Use `cancel` rather than `abort` here since this is not an error which should be
                // propagated to upstream.
                task.cancel();
                if let Some(heartbeat_join_handle) = task.heartbeat_join_handle() {
                    heartbeat_join_handle.abort();
                }
            }
            None => {
                warn!("Task {:?} not found for cancel", sid)
            }
        };
    }

    /// Returns error if task is not running.
    pub fn check_if_task_running(&self, task_id: &TaskId) -> Result<()> {
        match self.tasks.lock().get(task_id) {
            Some(task) => task.check_if_running(),
            None => bail!("task {:?} not found", task_id),
        }
    }

    pub fn check_if_task_aborted(&self, task_id: &TaskId) -> Result<bool> {
        match self.tasks.lock().get(task_id) {
            Some(task) => task.check_if_aborted(),
            None => bail!("task {:?} not found", task_id),
        }
    }

    #[cfg(test)]
    async fn wait_until_task_aborted(&self, task_id: &TaskId) -> Result<()> {
        use std::time::Duration;
        loop {
            match self.tasks.lock().get(task_id) {
                Some(task) => {
                    let ret = task.check_if_aborted();
                    match ret {
                        Ok(true) => return Ok(()),
                        Ok(false) => {}
                        Err(err) => return Err(err),
                    }
                }
                None => bail!("task {:?} not found", task_id),
            }
            tokio::time::sleep(Duration::from_millis(100)).await
        }
    }

    pub fn runtime(&self) -> Arc<BackgroundShutdownRuntime> {
        self.runtime.clone()
    }

    pub fn config(&self) -> &BatchConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::config::BatchConfig;
    use risingwave_pb::batch_plan::exchange_info::DistributionMode;
    use risingwave_pb::batch_plan::plan_node::NodeBody;
    use risingwave_pb::batch_plan::{
        ExchangeInfo, PbTaskId, PbTaskOutputId, PlanFragment, PlanNode,
    };

    use crate::monitor::BatchManagerMetrics;
    use crate::task::{BatchManager, TaskId};

    #[tokio::test]
    async fn test_task_not_found() {
        let manager = Arc::new(BatchManager::new(
            BatchConfig::default(),
            BatchManagerMetrics::for_test(),
            u64::MAX,
            None,
        ));
        let task_id = TaskId {
            task_id: 0,
            stage_id: 0,
            query_id: "abc".to_owned(),
        };

        let error = manager.check_if_task_running(&task_id).unwrap_err();
        assert!(error.to_string().contains("not found"), "{:?}", error);

        let output_id = PbTaskOutputId {
            task_id: Some(risingwave_pb::batch_plan::TaskId {
                stage_id: 0,
                task_id: 0,
                query_id: "".to_owned(),
            }),
            output_id: 0,
        };
        let error = manager.take_output(&output_id).unwrap_err();
        assert!(error.to_string().contains("not found"), "{:?}", error);
    }

    #[tokio::test]
    // see https://github.com/risingwavelabs/risingwave/issues/11979
    #[ignore]
    async fn test_task_cancel_for_busy_loop() {
        let manager = Arc::new(BatchManager::new(
            BatchConfig::default(),
            BatchManagerMetrics::for_test(),
            u64::MAX,
            None,
        ));
        let plan = PlanFragment {
            root: Some(PlanNode {
                children: vec![],
                identity: "".to_owned(),
                node_body: Some(NodeBody::BusyLoopExecutor(true)),
            }),
            exchange_info: Some(ExchangeInfo {
                mode: DistributionMode::Single as i32,
                distribution: None,
            }),
        };
        let task_id = PbTaskId {
            query_id: "".to_owned(),
            stage_id: 0,
            task_id: 0,
        };
        manager.fire_task_for_test(&task_id, plan).await.unwrap();
        manager.cancel_task(&task_id);
        let task_id = TaskId::from(&task_id);
        assert!(!manager.tasks.lock().contains_key(&task_id));
    }

    #[tokio::test]
    // see https://github.com/risingwavelabs/risingwave/issues/11979
    #[ignore]
    async fn test_task_abort_for_busy_loop() {
        let manager = Arc::new(BatchManager::new(
            BatchConfig::default(),
            BatchManagerMetrics::for_test(),
            u64::MAX,
            None,
        ));
        let plan = PlanFragment {
            root: Some(PlanNode {
                children: vec![],
                identity: "".to_owned(),
                node_body: Some(NodeBody::BusyLoopExecutor(true)),
            }),
            exchange_info: Some(ExchangeInfo {
                mode: DistributionMode::Single as i32,
                distribution: None,
            }),
        };
        let task_id = PbTaskId {
            query_id: "".to_owned(),
            stage_id: 0,
            task_id: 0,
        };
        manager.fire_task_for_test(&task_id, plan).await.unwrap();
        let task_id = TaskId::from(&task_id);
        manager
            .tasks
            .lock()
            .get(&task_id)
            .unwrap()
            .abort("Abort Test".to_owned());
        assert!(manager.wait_until_task_aborted(&task_id).await.is_ok());
    }
}
