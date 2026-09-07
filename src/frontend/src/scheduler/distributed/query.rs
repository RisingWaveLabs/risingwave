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
use std::fmt::{Debug, Formatter};
use std::mem;
use std::sync::Arc;

use anyhow::Context;
use futures::executor::block_on;
use petgraph::Graph;
use petgraph::dot::{Config, Dot};
use pgwire::pg_server::SessionId;
use risingwave_batch::worker_manager::worker_node_manager::WorkerNodeSelector;
use risingwave_common::array::DataChunk;
use risingwave_pb::batch_plan::{TaskId as PbTaskId, TaskOutputId as PbTaskOutputId};
use risingwave_pb::common::HostAddress;
use risingwave_rpc_client::ComputeClientPoolRef;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinHandle;
use tracing::{Instrument, debug, error, info, warn};

use super::{DistributedQueryMetrics, QueryExecutionInfoRef, QueryResultFetcher, StageEvent};
use crate::catalog::catalog_service::CatalogReader;
use crate::scheduler::distributed::StageEvent::Scheduled;
use crate::scheduler::distributed::StageExecution;
use crate::scheduler::distributed::query::QueryMessage::Stage;
use crate::scheduler::distributed::stage::StageEvent::ScheduledRoot;
use crate::scheduler::plan_fragmenter::{Query, ROOT_TASK_ID, ROOT_TASK_OUTPUT_ID, StageId};
use crate::scheduler::{ExecutionContextRef, SchedulerError, SchedulerResult};

/// Message sent to a `QueryRunner` to control its execution.
#[derive(Debug)]
pub enum QueryMessage {
    /// Events passed running execution.
    Stage(StageEvent),
    /// Cancelled by some reason
    CancelQuery(String),
}

enum QueryState {
    /// Not scheduled yet.
    ///
    /// We put `msg_receiver` in `Pending` state to avoid holding it in `QueryExecution`.
    Pending {
        msg_receiver: Receiver<QueryMessage>,
    },

    /// Running
    Running,

    /// Failed
    Failed,
}

pub struct QueryExecution {
    query: Arc<Query>,
    state: RwLock<QueryState>,
    shutdown_tx: Sender<QueryMessage>,
    /// Identified by `process_id`, `secret_key`. Query in the same session should have same key.
    pub session_id: SessionId,
    /// Whether a PostgreSQL `CancelRequest` for the session should cancel this query. For example,
    /// the query for a cursor should never be canceled by the `CancelRequest` for the session.
    can_session_cancel: bool,
    /// Permit to execute the query. Once query finishes execution, this is dropped.
    #[expect(dead_code)]
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

struct QueryRunner {
    query: Arc<Query>,
    stage_executions: HashMap<StageId, Arc<StageExecution>>,
    scheduled_stages_count: usize,
    /// Query messages receiver. For example, stage state change events, query commands.
    msg_receiver: Receiver<QueryMessage>,

    /// Will be set to `None` after all stage scheduled.
    root_stage_sender: Option<oneshot::Sender<SchedulerResult<QueryResultFetcher>>>,

    // Used for cleaning up `QueryExecution` after execution.
    query_execution_info: QueryExecutionInfoRef,

    query_metrics: Arc<DistributedQueryMetrics>,
    timeout_abort_task_handle: Option<JoinHandle<()>>,
}

impl QueryExecution {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query: Query,
        session_id: SessionId,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
        can_session_cancel: bool,
    ) -> Self {
        let query = Arc::new(query);
        let (sender, receiver) = channel(100);
        let state = QueryState::Pending {
            msg_receiver: receiver,
        };

        Self {
            query,
            state: RwLock::new(state),
            shutdown_tx: sender,
            session_id,
            can_session_cancel,
            permit,
        }
    }

    /// Returns whether PostgreSQL session cancellation should abort this query.
    pub fn can_session_cancel(&self) -> bool {
        self.can_session_cancel
    }

    /// Start execution of this query.
    /// Note the two shutdown channel sender and receivers are not dual.
    /// One is used for propagate error to `QueryResultFetcher`, one is used for listening on
    /// cancel request (from ctrl-c, cli, ui etc).
    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        self: Arc<Self>,
        context: ExecutionContextRef,
        worker_node_manager: WorkerNodeSelector,
        compute_client_pool: ComputeClientPoolRef,
        catalog_reader: CatalogReader,
        query_execution_info: QueryExecutionInfoRef,
        query_metrics: Arc<DistributedQueryMetrics>,
    ) -> SchedulerResult<QueryResultFetcher> {
        let mut state = self.state.write().await;
        let cur_state = mem::replace(&mut *state, QueryState::Failed);

        // Because the snapshot may be released before all stages are scheduled, we only pass a
        // reference of `pinned_snapshot`. Its ownership will be moved into `QueryRunner` so that it
        // can control when to release the snapshot.
        let stage_executions = self.gen_stage_executions(
            context.clone(),
            worker_node_manager,
            compute_client_pool.clone(),
            catalog_reader,
        );

        match cur_state {
            QueryState::Pending { msg_receiver } => {
                *state = QueryState::Running;

                // Start a timer to cancel the query
                let mut timeout_abort_task_handle: Option<JoinHandle<()>> = None;
                if let Some(timeout) = context.timeout() {
                    let this = self.clone();
                    timeout_abort_task_handle = Some(tokio::spawn(async move {
                        tokio::time::sleep(timeout).await;
                        warn!(
                            "Query {:?} timeout after {} seconds, sending cancel message.",
                            this.query.query_id,
                            timeout.as_secs(),
                        );
                        this.abort(format!("timeout after {} seconds", timeout.as_secs()))
                            .await;
                    }));
                }

                // Create a oneshot channel for QueryResultFetcher to get failed event.
                let (root_stage_sender, root_stage_receiver) =
                    oneshot::channel::<SchedulerResult<QueryResultFetcher>>();

                let runner = QueryRunner {
                    query: self.query.clone(),
                    stage_executions,
                    msg_receiver,
                    root_stage_sender: Some(root_stage_sender),
                    scheduled_stages_count: 0,
                    query_execution_info,
                    query_metrics,
                    timeout_abort_task_handle,
                };

                let span =
                    tracing::info_span!("distributed_execute", query_id = self.query.query_id.id,);

                tracing::trace!("Starting query: {:?}", self.query.query_id);

                // Not trace the error here, it will be processed in scheduler.
                tokio::spawn(async move { runner.run().instrument(span).await });

                let root_stage = root_stage_receiver
                    .await
                    .context("Starting query execution failed")??;

                tracing::trace!(
                    "Received root stage query result fetcher: {:?}, query id: {:?}",
                    root_stage,
                    self.query.query_id
                );

                tracing::trace!("Query {:?} started.", self.query.query_id);
                Ok(root_stage)
            }
            _ => {
                unreachable!("The query runner should not be scheduled twice");
            }
        }
    }

    /// Cancel execution of this query.
    pub async fn abort(self: Arc<Self>, reason: String) {
        if self
            .shutdown_tx
            .send(QueryMessage::CancelQuery(reason))
            .await
            .is_err()
        {
            warn!("Send cancel query request failed: the query has ended");
        } else {
            info!("Send cancel request to query-{:?}", self.query.query_id);
        };
    }

    fn gen_stage_executions(
        &self,
        context: ExecutionContextRef,
        worker_node_manager: WorkerNodeSelector,
        compute_client_pool: ComputeClientPoolRef,
        catalog_reader: CatalogReader,
    ) -> HashMap<StageId, Arc<StageExecution>> {
        let mut stage_executions: HashMap<StageId, Arc<StageExecution>> =
            HashMap::with_capacity(self.query.stage_graph.stages.len());

        for stage_id in self.query.stage_graph.stage_ids_by_topo_order() {
            let children_stages = self
                .query
                .stage_graph
                .get_child_stages_unchecked(&stage_id)
                .iter()
                .map(|s| stage_executions[s].clone())
                .collect::<Vec<Arc<StageExecution>>>();

            let stage_exec = Arc::new(StageExecution::new(
                stage_id,
                self.query.clone(),
                worker_node_manager.clone(),
                self.shutdown_tx.clone(),
                children_stages,
                compute_client_pool.clone(),
                catalog_reader.clone(),
                context.clone(),
            ));
            stage_executions.insert(stage_id, stage_exec);
        }
        stage_executions
    }
}

impl Drop for QueryRunner {
    fn drop(&mut self) {
        self.query_metrics.running_query_num.dec();
        self.timeout_abort_task_handle
            .as_ref()
            .inspect(|h| h.abort());
    }
}

impl Debug for QueryRunner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut graph = Graph::<String, String>::new();
        let mut stage_id_to_node_id = HashMap::new();
        for stage in &self.stage_executions {
            let node_id = graph.add_node(format!("{} {}", stage.0, block_on(stage.1.state())));
            stage_id_to_node_id.insert(stage.0, node_id);
        }

        for stage in &self.stage_executions {
            let stage_id = stage.0;
            if let Some(child_stages) = self.query.stage_graph.get_child_stages(stage_id) {
                for child_stage in child_stages {
                    graph.add_edge(
                        *stage_id_to_node_id.get(stage_id).unwrap(),
                        *stage_id_to_node_id.get(child_stage).unwrap(),
                        "".to_owned(),
                    );
                }
            }
        }

        // Visit https://dreampuf.github.io/GraphvizOnline/ to display the result
        writeln!(f, "{}", Dot::with_config(&graph, &[Config::EdgeNoLabel]))
    }
}

impl QueryRunner {
    async fn run(mut self) {
        self.query_metrics.running_query_num.inc();
        // Start leaf stages.
        let leaf_stages = self.query.leaf_stages();
        for stage_id in &leaf_stages {
            self.stage_executions[stage_id].start().await;
            tracing::trace!(
                "Query stage {:?}-{:?} started.",
                self.query.query_id,
                stage_id
            );
        }
        let mut stages_with_table_scan = self.query.stages_with_table_scan();
        let has_lookup_join_stage = self.query.has_lookup_join_stage();

        let mut finished_stage_cnt = 0usize;
        while let Some(msg_inner) = self.msg_receiver.recv().await {
            match msg_inner {
                Stage(Scheduled(stage_id)) => {
                    tracing::trace!(
                        "Query stage {:?}-{:?} scheduled.",
                        self.query.query_id,
                        stage_id
                    );
                    self.scheduled_stages_count += 1;
                    stages_with_table_scan.remove(&stage_id);
                    // If query contains lookup join we need to delay epoch unpin util the end of
                    // the query.
                    if !has_lookup_join_stage && stages_with_table_scan.is_empty() {
                        // We can be sure here that all the Hummock iterators have been created,
                        // thus they all successfully pinned a HummockVersion.
                        // So we can now unpin their epoch.
                        tracing::trace!(
                            "Query {:?} has scheduled all of its stages that have table scan (iterator creation).",
                            self.query.query_id
                        );
                    }

                    // For root stage, we execute in frontend local. We will pass the root fragment
                    // to QueryResultFetcher and execute to get a Chunk stream.
                    for parent in self.query.get_parents(&stage_id) {
                        if self.all_children_scheduled(parent).await
                                // Do not schedule same stage twice.
                                && self.stage_executions[parent].is_pending().await
                        {
                            self.stage_executions[parent].start().await;
                        }
                    }
                }
                Stage(ScheduledRoot(receiver)) => {
                    // We already schedule the root fragment, therefore we can notify query result
                    // fetcher.
                    self.send_root_stage_info(receiver);
                }
                Stage(StageEvent::Failed { id, reason }) => {
                    error!(
                        error = %reason.as_report(),
                        query_id = ?self.query.query_id,
                        stage_id = ?id,
                        "query stage failed"
                    );

                    self.clean_all_stages(Some(reason)).await;
                    // One stage failed, not necessary to execute schedule stages.
                    break;
                }
                Stage(StageEvent::Completed(_)) => {
                    finished_stage_cnt += 1;
                    assert!(finished_stage_cnt <= self.stage_executions.len());
                    if finished_stage_cnt == self.stage_executions.len() {
                        tracing::trace!(
                            "Query {:?} completed, starting to clean stage tasks.",
                            &self.query.query_id
                        );
                        // Now all stages completed, we should remove all
                        self.clean_all_stages(None).await;
                        break;
                    }
                }
                QueryMessage::CancelQuery(reason) => {
                    self.clean_all_stages(Some(SchedulerError::QueryCancelled(reason)))
                        .await;
                    // One stage failed, not necessary to execute schedule stages.
                    break;
                }
            }
        }
    }

    /// The `shutdown_tx` will only be Some if the stage is 1. In that case, we should keep the life
    /// of shutdown sender so that shutdown receiver won't be triggered.
    fn send_root_stage_info(&mut self, chunk_rx: Receiver<SchedulerResult<DataChunk>>) {
        let root_task_output_id = {
            let root_task_id_prost = PbTaskId {
                query_id: self.query.query_id.clone().id,
                stage_id: self.query.root_stage_id().into(),
                task_id: ROOT_TASK_ID,
            };

            PbTaskOutputId {
                task_id: Some(root_task_id_prost),
                output_id: ROOT_TASK_OUTPUT_ID,
            }
        };

        let root_stage_result = QueryResultFetcher::new(
            root_task_output_id,
            // Execute in local, so no need to fill meaningful address.
            HostAddress::default(),
            chunk_rx,
            self.query.query_id.clone(),
            self.query_execution_info.clone(),
        );

        // Consume sender here.
        let root_stage_sender = mem::take(&mut self.root_stage_sender);

        if let Err(e) = root_stage_sender.unwrap().send(Ok(root_stage_result)) {
            warn!("Query execution dropped: {:?}", e);
        } else {
            debug!("Root stage for {:?} sent.", self.query.query_id);
        }
    }

    async fn all_children_scheduled(&self, stage_id: &StageId) -> bool {
        for child in self.query.stage_graph.get_child_stages_unchecked(stage_id) {
            if !self.stage_executions[child].is_scheduled().await {
                return false;
            }
        }
        true
    }

    /// Handle ctrl-c query or failed execution. Should stop all executions and send error to query
    /// result fetcher.
    async fn clean_all_stages(&mut self, error: Option<SchedulerError>) {
        // TODO(error-handling): should prefer use error types than strings.
        let error_msg = error.as_ref().map(|e| e.to_report_string());
        if let Some(reason) = error {
            // Consume sender here and send error to root stage.
            let root_stage_sender = mem::take(&mut self.root_stage_sender);
            // It's possible we receive stage failed event message multi times and the
            // sender has been consumed in first failed event.
            if let Some(sender) = root_stage_sender {
                if let Err(e) = sender.send(Err(reason)) {
                    warn!("Query execution dropped: {:?}", e);
                } else {
                    debug!(
                        "Root stage failure event for {:?} sent.",
                        self.query.query_id
                    );
                }
            }

            // If root stage has been taken (None), then root stage is responsible for send error to
            // Query Result Fetcher.
        }

        tracing::trace!("Cleaning stages in query [{:?}]", self.query.query_id);
        // Stop all running stages.
        for stage_execution in self.stage_executions.values() {
            // The stop is return immediately so no need to spawn tasks.
            stage_execution.stop(error_msg.clone()).await;
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use fixedbitset::FixedBitSet;
    use pgwire::pg_server::SessionId;
    use risingwave_batch::worker_manager::worker_node_manager::{
        WorkerNodeManager, WorkerNodeSelector,
    };
    use risingwave_common::RW_VERSION;
    use risingwave_common::catalog::{
        ColumnCatalog, ColumnDesc, ConflictBehavior, CreateType, DEFAULT_SUPER_USER_ID, Engine,
        StreamJobStatus,
    };
    use risingwave_common::hash::{VirtualNode, VnodeCount, WorkerSlotId, WorkerSlotMapping};
    use risingwave_common::types::DataType;
    use risingwave_pb::common::worker_node::{Property, Resource};
    use risingwave_pb::common::{HostAddress, WorkerNode, WorkerType};
    use risingwave_pb::plan_common::JoinType;
    use risingwave_rpc_client::ComputeClientPool;
    use tokio::sync::mpsc::{Receiver, channel};

    use super::{QueryExecution, QueryMessage, QueryState};
    use crate::TableCatalog;
    use crate::catalog::catalog_service::CatalogReader;
    use crate::catalog::root_catalog::Catalog;
    use crate::catalog::table_catalog::TableType;
    use crate::expr::InputRef;
    use crate::optimizer::OptimizerContext;
    use crate::optimizer::plan_node::{
        BatchExchange, BatchFilter, BatchHashJoin, BatchPlanRef as PlanRef, EqJoinPredicate,
        LogicalScan, ToBatch, generic,
    };
    use crate::optimizer::property::{Cardinality, Distribution, Order};
    use crate::scheduler::plan_fragmenter::{BatchPlanFragmenter, Query};
    use crate::scheduler::{DistributedQueryMetrics, ExecutionContext, QueryExecutionInfo};
    use crate::session::SessionImpl;
    use crate::utils::Condition;

    fn running_query_execution_with_control_receiver(
        query: Query,
        session_id: SessionId,
        can_session_cancel: bool,
    ) -> (Arc<QueryExecution>, Receiver<QueryMessage>) {
        let (shutdown_tx, shutdown_rx) = channel(100);
        let query_execution = Arc::new(QueryExecution {
            query: Arc::new(query),
            state: tokio::sync::RwLock::new(QueryState::Running),
            shutdown_tx,
            session_id,
            can_session_cancel,
            permit: None,
        });
        (query_execution, shutdown_rx)
    }

    async fn assert_receive_query_cancellation_message(msg_receiver: &mut Receiver<QueryMessage>) {
        let message = tokio::time::timeout(Duration::from_secs(1), msg_receiver.recv())
            .await
            .expect("query cancellation message must arrive")
            .expect("query control channel must remain open");
        assert!(matches!(
            message,
            QueryMessage::CancelQuery(reason) if reason == "cancelled by user"
        ));
    }

    async fn assert_no_query_message(msg_receiver: &mut Receiver<QueryMessage>) {
        assert!(
            tokio::time::timeout(Duration::from_millis(100), msg_receiver.recv())
                .await
                .is_err(),
            "excluded query unexpectedly received a cancellation message"
        );
    }

    /// Verifies that session cancellation targets only ordinary queries in that session, while
    /// cursor-owned queries and queries in other sessions remain untouched.
    #[tokio::test]
    async fn test_distributed_query_cancellation_filters_by_session_and_cursor_ownership() {
        let query_manager = SessionImpl::mock().env().query_manager().clone();
        let target_session = (1, 2);

        let cancellable_query = create_query().await;
        let cancellable_query_id = cancellable_query.query_id().clone();
        let (cancellable_query, mut cancellable_query_rx) =
            running_query_execution_with_control_receiver(cancellable_query, target_session, true);
        query_manager.add_query(cancellable_query_id.clone(), cancellable_query.clone());

        let cursor_query = create_query().await;
        let cursor_query_id = cursor_query.query_id().clone();
        let (cursor_query, mut cursor_query_rx) =
            running_query_execution_with_control_receiver(cursor_query, target_session, false);
        query_manager.add_query(cursor_query_id.clone(), cursor_query.clone());

        let other_session_query = create_query().await;
        let other_session_query_id = other_session_query.query_id().clone();
        let (other_session_query, mut other_session_query_rx) =
            running_query_execution_with_control_receiver(other_session_query, (3, 4), true);
        query_manager.add_query(other_session_query_id.clone(), other_session_query.clone());

        query_manager.cancel_non_cursor_queries_in_session(target_session);
        assert_receive_query_cancellation_message(&mut cancellable_query_rx).await;
        assert_no_query_message(&mut cursor_query_rx).await;
        assert_no_query_message(&mut other_session_query_rx).await;
    }

    #[tokio::test]
    async fn test_query_should_not_hang_with_empty_worker() {
        let worker_node_manager = Arc::new(WorkerNodeManager::mock(vec![]));
        let worker_node_selector = WorkerNodeSelector::new(worker_node_manager.clone(), false);
        let compute_client_pool = Arc::new(ComputeClientPool::for_test());
        let catalog_reader =
            CatalogReader::new(Arc::new(parking_lot::RwLock::new(Catalog::default())));
        let query = create_query().await;
        let query_id = query.query_id().clone();
        let query_execution = Arc::new(QueryExecution::new(query, (0, 0), None, true));
        let query_execution_info = Arc::new(RwLock::new(QueryExecutionInfo::new_from_map(
            HashMap::from([(query_id, query_execution.clone())]),
        )));

        assert!(
            query_execution
                .start(
                    ExecutionContext::new(SessionImpl::mock().into(), None).into(),
                    worker_node_selector,
                    compute_client_pool,
                    catalog_reader,
                    query_execution_info,
                    Arc::new(DistributedQueryMetrics::for_test()),
                )
                .await
                .is_err()
        );
    }

    pub async fn create_query() -> Query {
        // Construct a Hash Join with Exchange node.
        // Logical plan:
        //
        //    HashJoin
        //     /    \
        //   Scan  Scan
        //
        let ctx = OptimizerContext::mock();
        let table_id = 0.into();
        let vnode_count = VirtualNode::COUNT_FOR_TEST;

        let table_catalog: TableCatalog = TableCatalog {
            id: table_id,
            schema_id: 0.into(),
            database_id: 0.into(),
            associated_source_id: None,
            name: "test".to_owned(),
            refreshable: false,
            columns: vec![
                ColumnCatalog {
                    column_desc: ColumnDesc::named("a", 0.into(), DataType::Int32),
                    is_hidden: false,
                },
                ColumnCatalog {
                    column_desc: ColumnDesc::named("b", 1.into(), DataType::Float64),
                    is_hidden: false,
                },
                ColumnCatalog {
                    column_desc: ColumnDesc::named("c", 2.into(), DataType::Int64),
                    is_hidden: false,
                },
            ],
            pk: vec![],
            stream_key: vec![],
            table_type: TableType::Table,
            distribution_key: vec![],
            append_only: false,
            owner: DEFAULT_SUPER_USER_ID,
            retention_seconds: None,
            fragment_id: 0.into(), // FIXME
            dml_fragment_id: None, // FIXME
            vnode_col_index: None,
            row_id_index: None,
            value_indices: vec![0, 1, 2],
            definition: "".to_owned(),
            conflict_behavior: ConflictBehavior::NoCheck,
            version_column_indices: vec![],
            read_prefix_len_hint: 0,
            version: None,
            watermark_columns: FixedBitSet::with_capacity(3),
            dist_key_in_pk: vec![],
            cardinality: Cardinality::unknown(),
            created_at_epoch: None,
            initialized_at_epoch: None,
            stream_job_status: StreamJobStatus::Creating,
            create_type: CreateType::Foreground,
            description: None,
            initialized_at_cluster_version: None,
            created_at_cluster_version: None,
            cdc_table_id: None,
            vnode_count: VnodeCount::set(vnode_count),
            webhook_info: None,
            job_id: None,
            engine: Engine::Hummock,
            clean_watermark_index_in_pk: None,
            clean_watermark_indices: vec![],
            vector_index_info: None,
            cdc_table_type: None,
        };
        let batch_plan_node = LogicalScan::create(table_catalog.into(), ctx, None)
            .to_batch()
            .unwrap()
            .to_distributed()
            .unwrap();
        let batch_filter = BatchFilter::new(generic::Filter::new(
            Condition {
                conjunctions: vec![],
            },
            batch_plan_node.clone(),
        ))
        .into();
        let batch_exchange_node1: PlanRef = BatchExchange::new(
            batch_plan_node.clone(),
            Order::default(),
            Distribution::HashShard(vec![0, 1]),
        )
        .into();
        let batch_exchange_node2: PlanRef = BatchExchange::new(
            batch_filter,
            Order::default(),
            Distribution::HashShard(vec![0, 1]),
        )
        .into();
        let logical_join_node = generic::Join::with_full_output(
            batch_exchange_node1.clone(),
            batch_exchange_node2.clone(),
            JoinType::Inner,
            Condition::true_cond(),
        );
        let eq_key_1 = (
            InputRef {
                index: 0,
                data_type: DataType::Int32,
            },
            InputRef {
                index: 2,
                data_type: DataType::Int32,
            },
            false,
        );
        let eq_key_2 = (
            InputRef {
                index: 1,
                data_type: DataType::Float64,
            },
            InputRef {
                index: 3,
                data_type: DataType::Float64,
            },
            false,
        );
        let eq_join_predicate =
            EqJoinPredicate::new(Condition::true_cond(), vec![eq_key_1, eq_key_2], 2, 2);
        let logical_join_node = generic::Join::with_full_output_eq_predicate(
            logical_join_node.left,
            logical_join_node.right,
            logical_join_node.join_type,
            eq_join_predicate,
        );
        let hash_join_node: PlanRef = BatchHashJoin::new(logical_join_node, None).into();
        let batch_exchange_node: PlanRef = BatchExchange::new(
            hash_join_node.clone(),
            Order::default(),
            Distribution::Single,
        )
        .into();

        let worker1 = WorkerNode {
            id: 0.into(),
            r#type: WorkerType::ComputeNode as i32,
            host: Some(HostAddress {
                host: "127.0.0.1".to_owned(),
                port: 5687,
            }),
            state: risingwave_pb::common::worker_node::State::Running as i32,
            property: Some(Property {
                parallelism: 8,
                is_serving: true,
                is_streaming: true,
                ..Default::default()
            }),
            transactional_id: Some(0),
            resource: Some(Resource {
                rw_version: RW_VERSION.to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let worker2 = WorkerNode {
            id: 1.into(),
            r#type: WorkerType::ComputeNode as i32,
            host: Some(HostAddress {
                host: "127.0.0.1".to_owned(),
                port: 5688,
            }),
            state: risingwave_pb::common::worker_node::State::Running as i32,
            property: Some(Property {
                parallelism: 8,
                is_serving: true,
                is_streaming: true,
                ..Default::default()
            }),
            transactional_id: Some(1),
            resource: Some(Resource {
                rw_version: RW_VERSION.to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let worker3 = WorkerNode {
            id: 2.into(),
            r#type: WorkerType::ComputeNode as i32,
            host: Some(HostAddress {
                host: "127.0.0.1".to_owned(),
                port: 5689,
            }),
            state: risingwave_pb::common::worker_node::State::Running as i32,
            property: Some(Property {
                parallelism: 8,
                is_serving: true,
                is_streaming: true,
                ..Default::default()
            }),
            transactional_id: Some(2),
            resource: Some(Resource {
                rw_version: RW_VERSION.to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let workers = vec![worker1, worker2, worker3];
        let worker_node_manager = Arc::new(WorkerNodeManager::mock(workers));
        let worker_node_selector = WorkerNodeSelector::new(worker_node_manager.clone(), false);
        let mapping = WorkerSlotMapping::new_uniform(
            std::iter::once(WorkerSlotId::new(0.into(), 0)),
            vnode_count,
        );
        worker_node_manager.insert_streaming_fragment_mapping(0.into(), mapping.clone());
        worker_node_manager
            .set_serving_fragment_mapping(vec![(0.into(), mapping)].into_iter().collect());
        let catalog = Arc::new(parking_lot::RwLock::new(Catalog::default()));
        catalog.write().insert_table_id_mapping(table_id, 0.into());
        let catalog_reader = CatalogReader::new(catalog);
        // Break the plan node into fragments.
        let fragmenter = BatchPlanFragmenter::new(
            worker_node_selector,
            catalog_reader,
            None,
            batch_exchange_node.clone(),
        )
        .unwrap();
        fragmenter.generate_complete_query().await.unwrap()
    }
}
