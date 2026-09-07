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

//! Local execution for batch query.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use anyhow::anyhow;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use pgwire::pg_server::BoxedError;
use risingwave_batch::error::BatchError;
use risingwave_batch::executor::ExecutorBuilder;
use risingwave_batch::task::{ShutdownToken, TaskId};
use risingwave_batch::worker_manager::worker_node_manager::WorkerNodeSelector;
use risingwave_common::array::DataChunk;
use risingwave_common::bail;
use risingwave_common::hash::WorkerSlotMapping;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::tracing::{InstrumentStream, TracingContext};
use risingwave_connector::source::SplitMetaData;
use risingwave_pb::batch_plan::exchange_info::DistributionMode;
use risingwave_pb::batch_plan::exchange_source::LocalExecutePlan::Plan;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{
    ExchangeInfo, ExchangeSource, LocalExecutePlan, PbTaskId, PlanFragment, PlanNode as PbPlanNode,
    TaskOutputId,
};
use risingwave_pb::common::WorkerNode;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::debug;

use super::plan_fragmenter::{PartitionInfo, QueryStage};
use crate::catalog::{FragmentId, TableId};
use crate::error::RwError;
use crate::optimizer::plan_node::BatchPlanNodeType;
use crate::scheduler::plan_fragmenter::{ExecutionPlanNode, Query, StageId};
use crate::scheduler::task_context::FrontendBatchTaskContext;
use crate::scheduler::{SchedulerError, SchedulerResult};
use crate::session::{FrontendEnv, SessionImpl};

// TODO(error-handling): use a concrete error type.
/// A stream of raw chunks produced by a frontend-local batch query.
pub type LocalQueryStream = ReceiverStream<Result<DataChunk, BoxedError>>;
/// Owns the plan, snapshot context, and shutdown policy for a frontend-local batch query.
pub struct LocalQueryExecution {
    query: Query,
    front_env: FrontendEnv,
    session: Arc<SessionImpl>,
    worker_node_manager: WorkerNodeSelector,
    timeout: Option<Duration>,
    shutdown_rx: ShutdownToken,
}

impl LocalQueryExecution {
    /// Creates a local query execution with either caller-provided or session cancellation.
    pub fn new(
        query: Query,
        front_env: FrontendEnv,
        support_barrier_read: bool,
        session: Arc<SessionImpl>,
        timeout: Option<Duration>,
        shutdown_rx: ShutdownToken,
    ) -> Self {
        let worker_node_manager =
            WorkerNodeSelector::new(front_env.worker_node_manager_ref(), support_barrier_read);

        Self {
            query,
            front_env,
            session,
            worker_node_manager,
            timeout,
            shutdown_rx,
        }
    }

    fn shutdown_rx(&self) -> ShutdownToken {
        self.shutdown_rx.clone()
    }

    #[try_stream(ok = DataChunk, error = RwError)]
    /// Builds the local executor and yields chunks until it completes or is canceled. There could
    /// be remaining chunks discarded after the cancel signal received.
    pub async fn run_inner(self, shutdown_rx: ShutdownToken) {
        debug!(
            query_id = %self.query.query_id,
            "Starting to run query"
        );
        let context = FrontendBatchTaskContext::create(self.session.clone());
        let task_id = TaskId {
            query_id: self.query.query_id.id.clone(),
            stage_id: 0,
            task_id: 0,
        };

        let plan_fragment = self.create_plan_fragment()?;
        let plan_node = plan_fragment.root.unwrap();

        let executor = ExecutorBuilder::new(&plan_node, &task_id, context, shutdown_rx);
        let executor = executor.build().await?;
        // The following loop can be slow.
        // Release potential large object in Query and PlanNode early.
        drop(plan_node);
        drop(self);

        #[for_await]
        for chunk in executor.execute() {
            yield chunk?;
        }
    }

    fn run(self, shutdown_rx: ShutdownToken) -> BoxStream<'static, Result<DataChunk, RwError>> {
        let span = tracing::info_span!("local_execute", query_id = self.query.query_id.id,);
        Box::pin(self.run_inner(shutdown_rx).instrument(span))
    }

    /// Runs this execution on the frontend compute runtime and returns its raw chunk stream.
    pub fn stream_rows(self) -> LocalQueryStream {
        let compute_runtime = self.front_env.compute_runtime();
        let (sender, receiver) = mpsc::channel(10);
        let shutdown_rx = self.shutdown_rx();
        let executor_shutdown_rx = shutdown_rx.clone();

        let catalog_reader = self.front_env.catalog_reader().clone();
        let user_info_reader = self.front_env.user_info_reader().clone();
        let auth_context = self.session.auth_context();
        let db_name = self.session.database();
        let search_path = self.session.config().search_path();
        let time_zone = self.session.config().timezone();
        let strict_mode = self.session.config().batch_expr_strict_mode();
        let timeout = self.timeout;
        let meta_client = self.front_env.meta_client_ref();

        let sender1 = sender.clone();
        let exec = async move {
            let mut data_stream = self
                .run(executor_shutdown_rx)
                .map(|r| r.map_err(|e| Box::new(e) as BoxedError));
            while let Some(mut r) = data_stream.next().await {
                // append a query cancelled error if the query is cancelled.
                if r.is_err() && shutdown_rx.is_cancelled() {
                    r = Err(Box::new(SchedulerError::QueryCancelled(
                        "Cancelled by user".to_owned(),
                    )) as BoxedError);
                }
                if sender1.send(r).await.is_err() {
                    tracing::info!("Receiver closed.");
                    return;
                }
            }
        };

        use risingwave_expr::expr_context::*;

        use crate::expr::function_impl::context::{
            AUTH_CONTEXT, CATALOG_READER, DB_NAME, META_CLIENT, SEARCH_PATH, USER_INFO_READER,
        };

        // box is necessary, otherwise the size of `exec` will double each time it is nested.
        let exec = async move { CATALOG_READER::scope(catalog_reader, exec).await }.boxed();
        let exec = async move { USER_INFO_READER::scope(user_info_reader, exec).await }.boxed();
        let exec = async move { DB_NAME::scope(db_name, exec).await }.boxed();
        let exec = async move { SEARCH_PATH::scope(search_path, exec).await }.boxed();
        let exec = async move { AUTH_CONTEXT::scope(auth_context, exec).await }.boxed();
        let exec = async move { TIME_ZONE::scope(time_zone, exec).await }.boxed();
        let exec = async move { STRICT_MODE::scope(strict_mode, exec).await }.boxed();
        let exec = async move { META_CLIENT::scope(meta_client, exec).await }.boxed();

        if let Some(timeout) = timeout {
            let exec = async move {
                if let Err(_e) = tokio::time::timeout(timeout, exec).await {
                    tracing::error!(
                        "Local query execution timeout after {} seconds",
                        timeout.as_secs()
                    );
                    if sender
                        .send(Err(Box::new(SchedulerError::QueryCancelled(format!(
                            "timeout after {} seconds",
                            timeout.as_secs(),
                        ))) as BoxedError))
                        .await
                        .is_err()
                    {
                        tracing::info!("Receiver closed.");
                    }
                }
            };
            compute_runtime.spawn(exec);
        } else {
            compute_runtime.spawn(exec);
        }

        ReceiverStream::new(receiver)
    }

    /// Convert query to plan fragment.
    ///
    /// We can convert a query to plan fragment since in local execution mode, there are at most
    /// two layers, e.g. root stage and its optional input stage. If it does have input stage, it
    /// will be embedded in exchange source, so we can always convert a query into a plan fragment.
    ///
    /// We remark that the boundary to determine which part should be executed on the frontend and
    /// which part should be executed on the backend is `the first exchange operator` when looking
    /// from the the root of the plan to the leaves. The first exchange operator contains
    /// the pushed-down plan fragment.
    fn create_plan_fragment(&self) -> SchedulerResult<PlanFragment> {
        let next_executor_id = Arc::new(AtomicU32::new(0));
        let root_stage_id = self.query.root_stage_id();
        let root_stage = self.query.stage_graph.stages.get(&root_stage_id).unwrap();
        assert_eq!(root_stage.parallelism.unwrap(), 1);
        let second_stage_id = self.query.stage_graph.get_child_stages(&root_stage_id);
        let plan_node_prost = match second_stage_id {
            None => {
                debug!("Local execution mode converts a plan with a single stage");
                self.convert_plan_node(&root_stage.root, &mut None, None, next_executor_id)?
            }
            Some(second_stage_ids) => {
                debug!("Local execution mode converts a plan with two stages");
                if second_stage_ids.is_empty() {
                    // This branch is defensive programming. The semantics should be the same as
                    // `None`.
                    self.convert_plan_node(&root_stage.root, &mut None, None, next_executor_id)?
                } else {
                    let mut second_stages = HashMap::new();
                    for second_stage_id in second_stage_ids {
                        let second_stage =
                            self.query.stage_graph.stages.get(second_stage_id).unwrap();
                        second_stages.insert(*second_stage_id, second_stage);
                    }
                    let mut stage_id_to_plan = Some(second_stages);
                    let res = self.convert_plan_node(
                        &root_stage.root,
                        &mut stage_id_to_plan,
                        None,
                        next_executor_id,
                    )?;
                    assert!(
                        stage_id_to_plan.as_ref().unwrap().is_empty(),
                        "We expect that all the child stage plan fragments have been used"
                    );
                    res
                }
            }
        };

        Ok(PlanFragment {
            root: Some(plan_node_prost),
            // Intentionally leave this as `None` as this is the last stage for the frontend
            // to really get the output of computation, which is single distribution
            // but we do not need to explicitly specify this.
            exchange_info: None,
        })
    }

    fn convert_plan_node<'a>(
        &'a self,
        execution_plan_node: &ExecutionPlanNode,
        second_stages: &mut Option<HashMap<StageId, &'a QueryStage>>,
        partition: Option<PartitionInfo>,
        next_executor_id: Arc<AtomicU32>,
    ) -> SchedulerResult<PbPlanNode> {
        let identity = format!(
            "{:?}-{}",
            execution_plan_node.plan_node_type,
            next_executor_id.fetch_add(1, Ordering::Relaxed)
        );
        match execution_plan_node.plan_node_type {
            BatchPlanNodeType::BatchExchange => {
                let exchange_source_stage_id = execution_plan_node
                    .source_stage_id
                    .expect("We expect stage id for Exchange Operator");
                let Some(second_stages) = second_stages.as_mut() else {
                    bail!(
                        "Unexpected exchange detected. We are either converting a single stage plan or converting the second stage of the plan."
                    )
                };
                let second_stage = second_stages.remove(&exchange_source_stage_id).expect(
                    "We expect child stage fragment for Exchange Operator running in the frontend",
                );
                let mut node_body = execution_plan_node.node.clone();
                let sources = match &mut node_body {
                    NodeBody::Exchange(exchange_node) => &mut exchange_node.sources,
                    NodeBody::MergeSortExchange(merge_sort_exchange_node) => {
                        &mut merge_sort_exchange_node
                            .exchange
                            .as_mut()
                            .expect("MergeSortExchangeNode must have a exchange node")
                            .sources
                    }
                    _ => unreachable!(),
                };
                assert!(sources.is_empty());

                let tracing_context = TracingContext::from_current_span().to_protobuf();

                if let Some(table_scan_info) = second_stage.table_scan_info.clone()
                    && let Some(vnode_bitmaps) = table_scan_info.partitions()
                {
                    // Similar to the distributed case (StageRunner::schedule_tasks).
                    // Set `vnode_ranges` of the scan node in `local_execute_plan` of each
                    // `exchange_source`.
                    let (worker_ids, vnode_bitmaps): (Vec<_>, Vec<_>) =
                        vnode_bitmaps.clone().into_iter().unzip();
                    let workers = self
                        .worker_node_manager
                        .manager
                        .get_workers_by_worker_slot_ids(&worker_ids)?;
                    for (idx, (worker_node, partition)) in
                        (workers.into_iter().zip_eq_fast(vnode_bitmaps.into_iter())).enumerate()
                    {
                        let second_stage_plan_node = self.convert_plan_node(
                            &second_stage.root,
                            &mut None,
                            Some(PartitionInfo::Table(partition)),
                            next_executor_id.clone(),
                        )?;
                        let second_stage_plan_fragment = PlanFragment {
                            root: Some(second_stage_plan_node),
                            exchange_info: Some(ExchangeInfo {
                                mode: DistributionMode::Single as i32,
                                ..Default::default()
                            }),
                        };
                        let local_execute_plan = LocalExecutePlan {
                            plan: Some(second_stage_plan_fragment),
                            tracing_context: tracing_context.clone(),
                        };
                        let exchange_source = ExchangeSource {
                            task_output_id: Some(TaskOutputId {
                                task_id: Some(PbTaskId {
                                    task_id: idx as u64,
                                    stage_id: exchange_source_stage_id.into(),
                                    query_id: self.query.query_id.id.clone(),
                                }),
                                output_id: 0,
                            }),
                            host: Some(worker_node.host.as_ref().unwrap().clone()),
                            local_execute_plan: Some(Plan(local_execute_plan)),
                        };
                        sources.push(exchange_source);
                    }
                } else if let Some(source_info) = &second_stage.source_info {
                    // For file source batch read, all the files  to be read  are divide into several parts to prevent the task from taking up too many resources.

                    let chunk_size = (source_info.split_info().unwrap().len() as f32
                        / (self.worker_node_manager.schedule_unit_count()) as f32)
                        .ceil() as usize;
                    for (id, split) in source_info
                        .split_info()
                        .unwrap()
                        .chunks(chunk_size)
                        .enumerate()
                    {
                        let second_stage_plan_node = self.convert_plan_node(
                            &second_stage.root,
                            &mut None,
                            Some(PartitionInfo::Source(split.to_vec())),
                            next_executor_id.clone(),
                        )?;
                        let second_stage_plan_fragment = PlanFragment {
                            root: Some(second_stage_plan_node),
                            exchange_info: Some(ExchangeInfo {
                                mode: DistributionMode::Single as i32,
                                ..Default::default()
                            }),
                        };
                        let local_execute_plan = LocalExecutePlan {
                            plan: Some(second_stage_plan_fragment),
                            tracing_context: tracing_context.clone(),
                        };
                        // NOTE: select a random work node here.
                        let worker_node = self.worker_node_manager.next_random_worker()?;
                        let exchange_source = ExchangeSource {
                            task_output_id: Some(TaskOutputId {
                                task_id: Some(PbTaskId {
                                    task_id: id as u64,
                                    stage_id: exchange_source_stage_id.into(),
                                    query_id: self.query.query_id.id.clone(),
                                }),
                                output_id: 0,
                            }),
                            host: Some(worker_node.host.as_ref().unwrap().clone()),
                            local_execute_plan: Some(Plan(local_execute_plan)),
                        };
                        sources.push(exchange_source);
                    }
                } else if let Some(file_scan_info) = &second_stage.file_scan_info {
                    let chunk_size = (file_scan_info.file_location.len() as f32
                        / (self.worker_node_manager.schedule_unit_count()) as f32)
                        .ceil() as usize;
                    for (id, files) in file_scan_info.file_location.chunks(chunk_size).enumerate() {
                        let second_stage_plan_node = self.convert_plan_node(
                            &second_stage.root,
                            &mut None,
                            Some(PartitionInfo::File(files.to_vec())),
                            next_executor_id.clone(),
                        )?;
                        let second_stage_plan_fragment = PlanFragment {
                            root: Some(second_stage_plan_node),
                            exchange_info: Some(ExchangeInfo {
                                mode: DistributionMode::Single as i32,
                                ..Default::default()
                            }),
                        };
                        let local_execute_plan = LocalExecutePlan {
                            plan: Some(second_stage_plan_fragment),
                            tracing_context: tracing_context.clone(),
                        };
                        // NOTE: select a random work node here.
                        let worker_node = self.worker_node_manager.next_random_worker()?;
                        let exchange_source = ExchangeSource {
                            task_output_id: Some(TaskOutputId {
                                task_id: Some(PbTaskId {
                                    task_id: id as u64,
                                    stage_id: exchange_source_stage_id.into(),
                                    query_id: self.query.query_id.id.clone(),
                                }),
                                output_id: 0,
                            }),
                            host: Some(worker_node.host.as_ref().unwrap().clone()),
                            local_execute_plan: Some(Plan(local_execute_plan)),
                        };
                        sources.push(exchange_source);
                    }
                } else {
                    let second_stage_plan_node = self.convert_plan_node(
                        &second_stage.root,
                        &mut None,
                        None,
                        next_executor_id,
                    )?;
                    let second_stage_plan_fragment = PlanFragment {
                        root: Some(second_stage_plan_node),
                        exchange_info: Some(ExchangeInfo {
                            mode: DistributionMode::Single as i32,
                            ..Default::default()
                        }),
                    };

                    let local_execute_plan = LocalExecutePlan {
                        plan: Some(second_stage_plan_fragment),
                        tracing_context,
                    };

                    let workers = self.choose_worker(second_stage)?;
                    *sources = workers
                        .iter()
                        .enumerate()
                        .map(|(idx, worker_node)| ExchangeSource {
                            task_output_id: Some(TaskOutputId {
                                task_id: Some(PbTaskId {
                                    task_id: idx as u64,
                                    stage_id: exchange_source_stage_id.into(),
                                    query_id: self.query.query_id.id.clone(),
                                }),
                                output_id: 0,
                            }),
                            host: Some(worker_node.host.as_ref().unwrap().clone()),
                            local_execute_plan: Some(Plan(local_execute_plan.clone())),
                        })
                        .collect();
                }

                Ok(PbPlanNode {
                    // Since all the rest plan is embedded into the exchange node,
                    // there is no children any more.
                    children: vec![],
                    identity,
                    node_body: Some(node_body),
                })
            }
            BatchPlanNodeType::BatchSeqScan => {
                let mut node_body = execution_plan_node.node.clone();
                match &mut node_body {
                    NodeBody::RowSeqScan(scan_node) => {
                        if let Some(partition) = partition {
                            let partition = partition
                                .into_table()
                                .expect("PartitionInfo should be TablePartitionInfo here");
                            scan_node.vnode_bitmap = Some(partition.vnode_bitmap.to_protobuf());
                            scan_node.scan_ranges = partition.scan_ranges;
                        }
                    }
                    NodeBody::SysRowSeqScan(_) => {}
                    _ => unreachable!(),
                }

                Ok(PbPlanNode {
                    children: vec![],
                    identity,
                    node_body: Some(node_body),
                })
            }
            BatchPlanNodeType::BatchLogSeqScan => {
                let mut node_body = execution_plan_node.node.clone();
                match &mut node_body {
                    NodeBody::LogRowSeqScan(scan_node) => {
                        if let Some(partition) = partition {
                            let partition = partition
                                .into_table()
                                .expect("PartitionInfo should be TablePartitionInfo here");
                            scan_node.vnode_bitmap = Some(partition.vnode_bitmap.to_protobuf());
                        }
                    }
                    _ => unreachable!(),
                }

                Ok(PbPlanNode {
                    children: vec![],
                    identity,
                    node_body: Some(node_body),
                })
            }
            BatchPlanNodeType::BatchFileScan => {
                let mut node_body = execution_plan_node.node.clone();
                match &mut node_body {
                    NodeBody::FileScan(file_scan_node) => {
                        if let Some(partition) = partition {
                            let partition = partition
                                .into_file()
                                .expect("PartitionInfo should be FilePartitionInfo here");
                            file_scan_node.file_location = partition;
                        }
                    }
                    _ => unreachable!(),
                }

                Ok(PbPlanNode {
                    children: vec![],
                    identity,
                    node_body: Some(node_body),
                })
            }
            BatchPlanNodeType::BatchSource | BatchPlanNodeType::BatchKafkaScan => {
                let mut node_body = execution_plan_node.node.clone();
                match &mut node_body {
                    NodeBody::Source(source_node) => {
                        if let Some(partition) = partition {
                            let partition = partition
                                .into_source()
                                .expect("PartitionInfo should be SourcePartitionInfo here");
                            source_node.split = partition
                                .into_iter()
                                .map(|split| split.encode_to_bytes().into())
                                .collect_vec();
                        }
                    }
                    _ => unreachable!(),
                }

                Ok(PbPlanNode {
                    children: vec![],
                    identity,
                    node_body: Some(node_body),
                })
            }
            BatchPlanNodeType::BatchIcebergScan => {
                let mut node_body = execution_plan_node.node.clone();
                match &mut node_body {
                    NodeBody::IcebergScan(iceberg_scan_node) => {
                        if let Some(partition) = partition {
                            let partition = partition
                                .into_source()
                                .expect("PartitionInfo should be SourcePartitionInfo here");
                            iceberg_scan_node.split = partition
                                .into_iter()
                                .map(|split| split.encode_to_bytes().into())
                                .collect_vec();
                        }
                    }
                    _ => unreachable!(),
                }

                Ok(PbPlanNode {
                    children: vec![],
                    identity,
                    node_body: Some(node_body),
                })
            }
            BatchPlanNodeType::BatchLookupJoin => {
                let mut node_body = execution_plan_node.node.clone();
                match &mut node_body {
                    NodeBody::LocalLookupJoin(node) => {
                        let side_table_desc = node
                            .inner_side_table_desc
                            .as_ref()
                            .expect("no side table desc");
                        let mapping = self.worker_node_manager.fragment_mapping(
                            self.get_fragment_id(side_table_desc.table_id)?,
                            self.query.batch_parallelism(),
                        )?;

                        // TODO: should we use `pb::WorkerSlotMapping` here?
                        node.inner_side_vnode_mapping =
                            mapping.to_expanded().into_iter().map(u64::from).collect();
                        node.worker_nodes = self.worker_node_manager.manager.list_compute_nodes();
                    }
                    _ => unreachable!(),
                }

                let left_child = self.convert_plan_node(
                    &execution_plan_node.children[0],
                    second_stages,
                    partition,
                    next_executor_id,
                )?;

                Ok(PbPlanNode {
                    children: vec![left_child],
                    identity,
                    node_body: Some(node_body),
                })
            }
            _ => {
                let children = execution_plan_node
                    .children
                    .iter()
                    .map(|e| {
                        self.convert_plan_node(
                            e,
                            second_stages,
                            partition.clone(),
                            next_executor_id.clone(),
                        )
                    })
                    .collect::<SchedulerResult<Vec<PbPlanNode>>>()?;

                Ok(PbPlanNode {
                    children,
                    identity,
                    node_body: Some(execution_plan_node.node.clone()),
                })
            }
        }
    }

    #[inline(always)]
    fn get_fragment_id(&self, table_id: TableId) -> SchedulerResult<FragmentId> {
        let reader = self.front_env.catalog_reader().read_guard();
        reader
            .get_any_table_by_id(table_id)
            .map(|table| table.fragment_id)
            .map_err(|e| SchedulerError::Internal(anyhow!(e)))
    }

    #[inline(always)]
    fn get_table_dml_vnode_mapping(&self, table_id: TableId) -> SchedulerResult<WorkerSlotMapping> {
        let guard = self.front_env.catalog_reader().read_guard();

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

    fn choose_worker(&self, stage: &QueryStage) -> SchedulerResult<Vec<WorkerNode>> {
        if let Some(table_id) = stage.dml_table_id.as_ref() {
            // dml should use streaming vnode mapping
            let vnode_mapping = self.get_table_dml_vnode_mapping(*table_id)?;
            let worker_node = {
                let worker_ids = vnode_mapping.iter_unique().collect_vec();
                let candidates = self
                    .worker_node_manager
                    .manager
                    .get_workers_by_worker_slot_ids(&worker_ids)?;
                if candidates.is_empty() {
                    return Err(BatchError::EmptyWorkerNodes.into());
                }
                candidates[stage.session_id.0 as usize % candidates.len()].clone()
            };
            Ok(vec![worker_node])
        } else {
            let mut workers = Vec::with_capacity(stage.parallelism.unwrap() as usize);
            for _ in 0..stage.parallelism.unwrap() {
                workers.push(self.worker_node_manager.next_random_worker()?);
            }
            Ok(workers)
        }
    }
}
