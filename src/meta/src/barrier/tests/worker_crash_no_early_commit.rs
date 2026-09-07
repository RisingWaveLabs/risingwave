// Copyright 2025 RisingWave Labs
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
use std::future::pending;
use std::sync::Arc;

use anyhow::anyhow;
use futures::StreamExt;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::hash::VirtualNode;
use risingwave_common::id::{JobId, PartialGraphId, SinkId};
use risingwave_common::util::epoch::test_epoch;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_meta_model::{
    CreateType, I32Array, JobStatus, StreamNode, StreamingParallelism, TableIdArray, database,
    fragment, streaming_job,
};
use risingwave_pb::catalog::Database;
use risingwave_pb::common::{HostAddress, PbWorkerType, WorkerNode, worker_node};
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_plan::PbStreamNode;
use risingwave_pb::stream_service::streaming_control_stream_request::Request;
use risingwave_pb::stream_service::streaming_control_stream_response::Response;
use risingwave_pb::stream_service::{
    BarrierCompleteResponse, StreamingControlStreamRequest, StreamingControlStreamResponse,
};
use risingwave_rpc_client::{StreamingControlHandle, UnboundedBidiStreamHandle};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, oneshot};
use tokio::task::yield_now;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::MetaResult;
use crate::barrier::command::PostCollectCommand;
use crate::barrier::context::GlobalBarrierWorkerContext;
use crate::barrier::context::recovery::LoadedRecoveryContext;
use crate::barrier::progress::TrackingJob;
use crate::barrier::rpc::from_partial_graph_id;
use crate::barrier::schedule::MarkReadyOptions;
use crate::barrier::worker::GlobalBarrierWorker;
use crate::barrier::{
    BarrierWorkerRuntimeInfoSnapshot, DatabaseRuntimeInfoSnapshot, RecoveryReason, Scheduled,
};
use crate::controller::scale::{LoadedFragment, LoadedFragmentContext, NoShuffleEnsemble};
use crate::controller::utils::StreamingJobExtraInfo;
use crate::hummock::CommitEpochInfo;
use crate::manager::{ActiveStreamingWorkerNodes, MetaOpts, MetaSrvEnv};
use crate::model::{FragmentDownstreamRelation, FragmentId};

enum ContextRequest {
    AbortAndMarkBlocked(RecoveryReason),
    MarkReady,
    ReloadRuntimeInfo(oneshot::Sender<BarrierWorkerRuntimeInfoSnapshot>),
    NewControlStream(WorkerNode, oneshot::Sender<StreamingControlHandle>),
}

struct MockBarrierWorkerContext(UnboundedSender<ContextRequest>);
impl GlobalBarrierWorkerContext for MockBarrierWorkerContext {
    async fn commit_epoch(&self, _commit_info: CommitEpochInfo) -> MetaResult<HummockVersionStats> {
        unreachable!()
    }

    async fn next_scheduled(&self) -> Scheduled {
        pending().await
    }

    fn abort_and_mark_blocked(
        &self,
        database_id: Option<DatabaseId>,
        recovery_reason: RecoveryReason,
    ) {
        assert_eq!(database_id, None);
        self.0
            .send(ContextRequest::AbortAndMarkBlocked(recovery_reason))
            .unwrap();
    }

    fn mark_ready(&self, options: MarkReadyOptions) {
        let MarkReadyOptions::Global { blocked_databases } = options else {
            unreachable!()
        };
        assert!(blocked_databases.is_empty());
        self.0.send(ContextRequest::MarkReady).unwrap();
    }

    async fn resolve_log_store_epoch<'a>(
        &'a self,
        _upstream_table_ids: impl Iterator<Item = risingwave_common::catalog::TableId> + Send + 'a,
        _since_epoch: u64,
    ) -> MetaResult<crate::barrier::command::SinceTimestampResolvedEpoch> {
        Ok(Default::default())
    }

    async fn post_collect_command(&self, _command: PostCollectCommand) -> MetaResult<()> {
        unreachable!()
    }

    async fn notify_creating_job_failed(&self, database_id: Option<DatabaseId>, _err: String) {
        assert_eq!(database_id, None);
    }

    async fn finish_creating_job(&self, _job: TrackingJob) -> MetaResult<()> {
        unreachable!()
    }

    async fn new_control_stream(&self, node: &WorkerNode) -> MetaResult<StreamingControlHandle> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ContextRequest::NewControlStream(node.clone(), tx))
            .unwrap();
        Ok(rx.await.unwrap())
    }

    async fn reload_runtime_info(&self) -> MetaResult<BarrierWorkerRuntimeInfoSnapshot> {
        let (tx, rx) = oneshot::channel();
        self.0.send(ContextRequest::ReloadRuntimeInfo(tx)).unwrap();
        Ok(rx.await.map_err(|_| anyhow!("finish"))?)
    }

    async fn reload_database_runtime_info(
        &self,
        _database_id: DatabaseId,
    ) -> MetaResult<DatabaseRuntimeInfoSnapshot> {
        unreachable!()
    }

    async fn handle_list_finished_source_ids(
        &self,
        _list_finished_source_ids: Vec<
            risingwave_pb::stream_service::barrier_complete_response::PbListFinishedSource,
        >,
    ) -> MetaResult<()> {
        unimplemented!()
    }

    async fn handle_load_finished_source_ids(
        &self,
        _load_finished_source_ids: Vec<
            risingwave_pb::stream_service::barrier_complete_response::PbLoadFinishedSource,
        >,
    ) -> MetaResult<()> {
        unimplemented!()
    }

    async fn finish_cdc_table_backfill(&self, _job_id: JobId) -> MetaResult<()> {
        unimplemented!()
    }

    async fn handle_refresh_finished_table_ids(
        &self,
        _refresh_finished_table_ids: Vec<JobId>,
    ) -> MetaResult<()> {
        unimplemented!()
    }

    async fn load_batch_refresh_trigger_context(
        &self,
        _job_id: JobId,
        _database_id: DatabaseId,
        _last_committed_epoch: u64,
    ) -> MetaResult<crate::barrier::checkpoint::independent_job::BatchRefreshJobTriggerContext>
    {
        unimplemented!()
    }

    async fn pre_commit_iceberg_pk_index_sink_metadata(
        &self,
        _reports: Vec<
            risingwave_pb::stream_service::barrier_complete_response::IcebergPkIndexSinkMetadata,
        >,
    ) -> MetaResult<Vec<SinkId>> {
        unimplemented!()
    }

    async fn commit_iceberg_pk_index_sink_metadata(
        &self,
        _sink_ids: Vec<SinkId>,
    ) -> MetaResult<()> {
        unimplemented!()
    }

    fn advance_iceberg_pk_index_sink_committed_epochs(
        &self,
        _epochs: impl IntoIterator<Item = (PartialGraphId, u64)>,
    ) {
        unimplemented!()
    }
}

#[tokio::test]
async fn test_barrier_manager_worker_crash_no_early_commit() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let context = Arc::new(MockBarrierWorkerContext(tx));
    let env = MetaSrvEnv::for_test_opts(MetaOpts::test(true), |param| {
        param.per_database_isolation = Some(false);
    })
    .await;
    let (_request_tx, request_rx) = mpsc::unbounded_channel();
    let mut worker = GlobalBarrierWorker::new_inner(env, request_rx, context).await;
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    let _join_handle = tokio::spawn(async move {
        worker.recovery(false, RecoveryReason::Bootstrap).await;
        Box::pin(worker.run_inner(shutdown_rx)).await
    });

    // bootstrap recovery
    let ContextRequest::AbortAndMarkBlocked(RecoveryReason::Bootstrap) = rx.recv().await.unwrap()
    else {
        unreachable!()
    };
    let ContextRequest::ReloadRuntimeInfo(tx) = rx.recv().await.unwrap() else {
        unreachable!()
    };
    let database_id = DatabaseId::new(233);
    let job_id1 = JobId::new(234);
    let job_id2 = JobId::new(235);
    let worker_node = |id: u32, resource_group: &str| WorkerNode {
        id: id.into(),
        r#type: PbWorkerType::ComputeNode as i32,
        host: Some(HostAddress {
            host: format!("host{}", id),
            port: 0,
        }),
        state: worker_node::State::Running as i32,
        property: Some(worker_node::Property {
            is_streaming: true,
            is_serving: true,
            internal_rpc_host_addr: "".to_owned(),
            parallelism: 1,
            resource_group: Some(resource_group.to_owned()),
            is_iceberg_compactor: false,
        }),
        transactional_id: None,
        resource: None,
        started_at: None,
    };
    let worker1_group = "rg-a";
    let worker2_group = "rg-b";
    let worker1 = worker_node(1, worker1_group);
    let worker2 = worker_node(2, worker2_group);
    let table1 = TableId::new(1);
    let table2 = TableId::new(2);
    let fragment1 = FragmentId::new(101);
    let fragment2 = FragmentId::new(102);
    let initial_epoch = test_epoch(100);

    let fragment_model = |fragment_id: FragmentId, job_id: JobId, table_id: TableId| {
        #[expect(deprecated)]
        LoadedFragment::from(fragment::Model {
            fragment_id,
            job_id,
            fragment_type_mask: 0,
            distribution_type: DistributionType::Single,
            stream_node: StreamNode::from(&PbStreamNode::default()),
            state_table_ids: TableIdArray(vec![table_id]),
            upstream_fragment_id: I32Array::default(),
            vnode_count: VirtualNode::COUNT_FOR_TEST as i32,
            parallelism: Some(StreamingParallelism::Fixed(1)),
        })
    };

    let fragment_map_job1 =
        HashMap::from([(fragment1, fragment_model(fragment1, job_id1, table1))]);
    let fragment_map_job2 =
        HashMap::from([(fragment2, fragment_model(fragment2, job_id2, table2))]);

    let build_job_model = |job_id: JobId, resource_group: &str| streaming_job::Model {
        job_id,
        job_status: JobStatus::Creating,
        create_type: CreateType::Foreground,
        timezone: None,
        config_override: None,
        adaptive_parallelism_strategy: None,
        parallelism: StreamingParallelism::Fixed(1),
        backfill_parallelism: None,
        backfill_adaptive_parallelism_strategy: None,
        backfill_orders: None,
        max_parallelism: 1,
        specific_resource_group: Some(resource_group.to_owned()),
        is_serverless_backfill: false,
        refresh_interval_sec: None,
    };
    let job_model1 = build_job_model(job_id1, worker1_group);
    let job_model2 = build_job_model(job_id2, worker2_group);

    let database_model = database::Model {
        database_id,
        name: "db".to_owned(),
        resource_group: "test".to_owned(),
        barrier_interval_ms: None,
        checkpoint_frequency: None,
    };

    let fragment_context = LoadedFragmentContext {
        ensembles: vec![
            NoShuffleEnsemble::for_test([fragment1], [fragment1]),
            NoShuffleEnsemble::for_test([fragment2], [fragment2]),
        ],
        job_fragments: HashMap::from([(job_id1, fragment_map_job1), (job_id2, fragment_map_job2)]),
        job_map: HashMap::from([(job_id1, job_model1), (job_id2, job_model2)]),
        streaming_job_databases: HashMap::from([(job_id1, database_id), (job_id2, database_id)]),
        database_map: HashMap::from([(database_id, database_model)]),
        fragment_source_ids: HashMap::new(),
        fragment_splits: HashMap::new(),
    };

    let recovery_context = LoadedRecoveryContext {
        fragment_context,
        job_extra_info: HashMap::from([
            (
                job_id1,
                StreamingJobExtraInfo {
                    timezone: None,
                    config_override: Arc::<str>::from(""),
                    job_definition: "".to_owned(),
                    backfill_orders: None,
                    refresh_interval_sec: None,
                },
            ),
            (
                job_id2,
                StreamingJobExtraInfo {
                    timezone: None,
                    config_override: Arc::<str>::from(""),
                    job_definition: "".to_owned(),
                    backfill_orders: None,
                    refresh_interval_sec: None,
                },
            ),
        ]),
        upstream_sink_recovery: HashMap::new(),
        fragment_relations: FragmentDownstreamRelation::default(),
    };

    tx.send(BarrierWorkerRuntimeInfoSnapshot {
        active_streaming_nodes: ActiveStreamingWorkerNodes::for_test(HashMap::from_iter([
            (worker1.id, worker1.clone()),
            (worker2.id, worker2.clone()),
        ])),
        recovery_context,
        state_table_committed_epochs: HashMap::from([
            (table1, initial_epoch),
            (table2, initial_epoch),
        ]),
        state_table_log_epochs: HashMap::new(),
        mv_depended_subscriptions: HashMap::new(),
        creating_jobs: HashSet::new(),
        hummock_version_stats: HummockVersionStats::default(),
        database_infos: vec![Database {
            id: database_id,
            name: "".to_owned(),
            owner: 0.into(),
            resource_group: "test".to_owned(),
            barrier_interval_ms: None,
            checkpoint_frequency: None,
        }],
        cdc_table_snapshot_splits: HashMap::new(),
    })
    .unwrap();
    let make_control_stream_handle = || {
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (response_tx, response_rx) = mpsc::unbounded_channel();
        let handle = UnboundedBidiStreamHandle {
            request_sender: request_tx,
            response_stream: UnboundedReceiverStream::new(response_rx).boxed(),
        };
        (request_rx, response_tx, Some(handle))
    };
    let (mut request_rx1, response_tx1, mut handle1) = make_control_stream_handle();
    let (mut request_rx2, response_tx2, mut handle2) = make_control_stream_handle();

    for _ in 0..2 {
        let ContextRequest::NewControlStream(worker, tx) = rx.recv().await.unwrap() else {
            unreachable!()
        };
        if worker == worker1 {
            tx.send(handle1.take().unwrap()).unwrap()
        } else if worker == worker2 {
            tx.send(handle2.take().unwrap()).unwrap()
        } else {
            unreachable!()
        }
    }

    let handle_initial_barrier = async |request_rx: &mut UnboundedReceiver<
        StreamingControlStreamRequest,
    >,
                                        response_tx: &UnboundedSender<_>,
                                        worker: &WorkerNode| {
        let Request::CreatePartialGraph(req) = request_rx.recv().await.unwrap().request.unwrap()
        else {
            unreachable!()
        };
        let (req_database_id, _) = from_partial_graph_id(req.partial_graph_id);
        let partial_graph_id = req.partial_graph_id;
        assert_eq!(req_database_id, database_id);
        let Request::InjectBarrier(init_inject_request) =
            request_rx.recv().await.unwrap().request.unwrap()
        else {
            unreachable!()
        };
        let (initial_req_database_id, _) =
            from_partial_graph_id(init_inject_request.partial_graph_id);
        let epoch = init_inject_request.barrier.unwrap().epoch.unwrap();
        assert_eq!(epoch.prev, initial_epoch);
        assert_eq!(partial_graph_id, init_inject_request.partial_graph_id);
        assert_eq!(initial_req_database_id, database_id);
        response_tx
            .send(Ok(StreamingControlStreamResponse {
                response: Some(Response::CompleteBarrier(BarrierCompleteResponse {
                    worker_id: worker.id as _,
                    partial_graph_id,
                    epoch: epoch.prev,
                    ..Default::default()
                })),
            }))
            .unwrap();
        (partial_graph_id, epoch)
    };

    let (partial_graph_id, initial_epoch) =
        handle_initial_barrier(&mut request_rx1, &response_tx1, &worker1).await;
    assert_eq!(
        handle_initial_barrier(&mut request_rx2, &response_tx2, &worker2).await,
        (partial_graph_id, initial_epoch)
    );

    let ContextRequest::MarkReady = rx.recv().await.unwrap() else {
        unreachable!()
    };

    // correctly collect from worker1
    let Request::InjectBarrier(init_inject_request) =
        request_rx1.recv().await.unwrap().request.unwrap()
    else {
        unreachable!()
    };
    let epoch1 = init_inject_request.barrier.unwrap().epoch.unwrap();
    assert_eq!(epoch1.prev, initial_epoch.curr);
    assert_eq!(partial_graph_id, init_inject_request.partial_graph_id);
    let (initial_request_database_id, _) =
        from_partial_graph_id(init_inject_request.partial_graph_id);
    assert_eq!(initial_request_database_id, database_id);
    response_tx1
        .send(Ok(StreamingControlStreamResponse {
            response: Some(Response::CompleteBarrier(BarrierCompleteResponse {
                worker_id: worker1.id as _,
                partial_graph_id,
                epoch: epoch1.prev,
                ..Default::default()
            })),
        }))
        .unwrap();

    // yield to ensure that the worker has processed the response
    for _ in 0..10 {
        yield_now().await;
    }

    // receive from worker2
    {
        let Request::InjectBarrier(init_inject_request) =
            request_rx2.recv().await.unwrap().request.unwrap()
        else {
            unreachable!()
        };
        let (initial_request_database_id, _) =
            from_partial_graph_id(init_inject_request.partial_graph_id);
        let epoch = init_inject_request.barrier.unwrap().epoch.unwrap();
        assert_eq!(epoch.prev, initial_epoch.curr);
        assert_eq!(partial_graph_id, init_inject_request.partial_graph_id);
        assert_eq!(initial_request_database_id, database_id);
    }

    // worker2 crashes before sending the response
    drop(response_tx2);

    // enter recovery
    let ContextRequest::AbortAndMarkBlocked(RecoveryReason::Failover(_)) = rx.recv().await.unwrap()
    else {
        unreachable!()
    };
}
