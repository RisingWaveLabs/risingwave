// Copyright 2023 RisingWave Labs
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

use anyhow::anyhow;
use futures::future::{BoxFuture, Either, join_all, select};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use risingwave_common::bitmap::Bitmap;
use risingwave_connector::connector_common::IcebergSinkCompactionUpdate;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::{SinkCommittedEpochSubscriber, SinkError, SinkParam};
use risingwave_pb::connector_service::coordinate_request::Msg;
use risingwave_pb::connector_service::{CoordinateRequest, CoordinateResponse, coordinate_request};
use rw_futures_util::pending_on_none;
use sea_orm::DatabaseConnection;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot::{Receiver, Sender, channel};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinError, JoinHandle};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Status;
use tracing::{debug, error, info, warn};

use crate::hummock::HummockManagerRef;
use crate::manager::MetadataManager;
use crate::manager::sink_coordination::SinkWriterRequestStream;
use crate::manager::sink_coordination::coordinator_worker::CoordinatorWorker;
use crate::manager::sink_coordination::handle::SinkWriterCoordinationHandle;

macro_rules! send_with_err_check {
    ($tx:expr, $msg:expr) => {
        if $tx.send($msg).is_err() {
            error!("unable to send msg");
        }
    };
}

macro_rules! send_await_with_err_check {
    ($tx:expr, $msg:expr) => {
        if $tx.send($msg).await.is_err() {
            error!("unable to send msg");
        }
    };
}

const BOUNDED_CHANNEL_SIZE: usize = 16;

enum ManagerRequest {
    NewSinkWriter(SinkWriterCoordinationHandle),
    StopCoordinator {
        finish_notifier: Sender<()>,
        /// sink id to stop. When `None`, stop all sink coordinator
        sink_ids: Option<Vec<SinkId>>,
    },
}

#[derive(Clone)]
pub struct SinkCoordinatorManager {
    request_tx: mpsc::Sender<ManagerRequest>,
}
fn new_committed_epoch_subscriber(
    hummock_manager: HummockManagerRef,
    metadata_manager: MetadataManager,
) -> SinkCommittedEpochSubscriber {
    Arc::new(move |sink_id| {
        let hummock_manager = hummock_manager.clone();
        let metadata_manager = metadata_manager.clone();
        async move {
            let state_table_ids = metadata_manager
                .get_sink_state_table_ids(sink_id)
                .await
                .map_err(SinkError::from)?;
            let Some(table_id) = state_table_ids.first() else {
                return Err(anyhow!("no state table id in sink: {}", sink_id).into());
            };
            hummock_manager
                .subscribe_table_committed_epoch(*table_id)
                .await
                .map_err(SinkError::from)
        }
        .boxed()
    })
}

impl SinkCoordinatorManager {
    pub fn start_worker(
        db: DatabaseConnection,
        hummock_manager: HummockManagerRef,
        metadata_manager: MetadataManager,
        iceberg_compact_stat_sender: UnboundedSender<IcebergSinkCompactionUpdate>,
        await_tree_reg: await_tree::Registry,
    ) -> (Self, (JoinHandle<()>, Sender<()>)) {
        let subscriber = new_committed_epoch_subscriber(hummock_manager, metadata_manager);
        Self::start_worker_with_spawn_worker({
            move |param, manager_request_stream| {
                let sink_id = param.sink_id;
                let fut = CoordinatorWorker::run(
                    param,
                    manager_request_stream,
                    db.clone(),
                    subscriber.clone(),
                    iceberg_compact_stat_sender.clone(),
                );
                tokio::spawn(
                    await_tree_reg
                        .register_derived_root(format!("Sink Coordinator {sink_id}"))
                        .instrument(fut),
                )
            }
        })
    }

    fn start_worker_with_spawn_worker(
        spawn_coordinator_worker: impl SpawnCoordinatorFn,
    ) -> (Self, (JoinHandle<()>, Sender<()>)) {
        let (request_tx, request_rx) = mpsc::channel(BOUNDED_CHANNEL_SIZE);
        let (shutdown_tx, shutdown_rx) = channel();
        let worker = ManagerWorker::new(request_rx, shutdown_rx);
        let join_handle = tokio::spawn(worker.execute(spawn_coordinator_worker));
        (
            SinkCoordinatorManager { request_tx },
            (join_handle, shutdown_tx),
        )
    }

    pub async fn handle_new_request(
        &self,
        mut request_stream: SinkWriterRequestStream,
    ) -> Result<impl Stream<Item = Result<CoordinateResponse, Status>> + use<>, Status> {
        let (param, vnode_bitmap) = match request_stream.try_next().await? {
            Some(CoordinateRequest {
                msg:
                    Some(Msg::StartRequest(coordinate_request::StartCoordinationRequest {
                        param: Some(param),
                        vnode_bitmap: Some(vnode_bitmap),
                    })),
            }) => (SinkParam::from_proto(param), Bitmap::from(&vnode_bitmap)),
            msg => {
                return Err(Status::invalid_argument(format!(
                    "expected CoordinateRequest::StartRequest in the first request, get {:?}",
                    msg
                )));
            }
        };
        let (response_tx, response_rx) = mpsc::unbounded_channel();
        self.request_tx
            .send(ManagerRequest::NewSinkWriter(
                SinkWriterCoordinationHandle::new(request_stream, response_tx, param, vnode_bitmap),
            ))
            .await
            .map_err(|_| {
                Status::unavailable(
                    "unable to send to sink manager worker. The worker may have stopped",
                )
            })?;

        Ok(UnboundedReceiverStream::new(response_rx))
    }

    async fn stop_coordinator(&self, sink_ids: Option<Vec<SinkId>>) {
        let (tx, rx) = channel();
        send_await_with_err_check!(
            self.request_tx,
            ManagerRequest::StopCoordinator {
                finish_notifier: tx,
                sink_ids: sink_ids.clone(),
            }
        );
        if rx.await.is_err() {
            error!("failed to wait for the sink manager worker to reset");
        }
        info!("successfully stop coordinator: {:?}", sink_ids);
    }

    pub async fn reset(&self) {
        self.stop_coordinator(None).await;
    }

    pub async fn stop_sink_coordinator(&self, sink_ids: Vec<SinkId>) {
        self.stop_coordinator(Some(sink_ids)).await;
    }
}

struct CoordinatorWorkerHandle {
    /// Sender to coordinator worker. Drop the sender as a stop signal
    request_sender: Option<UnboundedSender<SinkWriterCoordinationHandle>>,
    /// Notify when the coordinator worker stops
    finish_notifiers: Vec<Sender<()>>,
}

struct ManagerWorker {
    request_rx: mpsc::Receiver<ManagerRequest>,
    // Make it option so that it can be polled with &mut SinkManagerWorker
    shutdown_rx: Receiver<()>,

    running_coordinator_worker_join_handles:
        FuturesUnordered<BoxFuture<'static, (SinkId, Result<(), JoinError>)>>,
    running_coordinator_worker: HashMap<SinkId, CoordinatorWorkerHandle>,
}

enum ManagerEvent {
    NewRequest(ManagerRequest),
    CoordinatorWorkerFinished {
        sink_id: SinkId,
        join_result: Result<(), JoinError>,
    },
}

trait SpawnCoordinatorFn = FnMut(SinkParam, UnboundedReceiver<SinkWriterCoordinationHandle>) -> JoinHandle<()>
    + Send
    + 'static;

impl ManagerWorker {
    fn new(request_rx: mpsc::Receiver<ManagerRequest>, shutdown_rx: Receiver<()>) -> Self {
        ManagerWorker {
            request_rx,
            shutdown_rx,
            running_coordinator_worker_join_handles: Default::default(),
            running_coordinator_worker: Default::default(),
        }
    }

    async fn execute(mut self, mut spawn_coordinator_worker: impl SpawnCoordinatorFn) {
        while let Some(event) = self.next_event().await {
            match event {
                ManagerEvent::NewRequest(request) => match request {
                    ManagerRequest::NewSinkWriter(request) => {
                        self.handle_new_sink_writer(request, &mut spawn_coordinator_worker)
                    }
                    ManagerRequest::StopCoordinator {
                        finish_notifier,
                        sink_ids,
                    } => {
                        if let Some(sink_ids) = sink_ids {
                            let mut rxs = Vec::with_capacity(sink_ids.len());
                            for sink_id in sink_ids {
                                if let Some(worker_handle) =
                                    self.running_coordinator_worker.get_mut(&sink_id)
                                {
                                    let (tx, rx) = oneshot::channel();
                                    rxs.push(rx);
                                    worker_handle.finish_notifiers.push(tx);
                                    if let Some(sender) = worker_handle.request_sender.take() {
                                        // drop the sender as a signal to notify the coordinator worker
                                        // to stop
                                        drop(sender);
                                    }
                                } else {
                                    debug!(
                                        "sink coordinator of {} is not running, skip it",
                                        sink_id
                                    );
                                }
                            }
                            tokio::spawn(async move {
                                let notify_res = join_all(rxs).await;
                                for res in notify_res {
                                    if let Err(e) = res {
                                        error!(
                                            "failed to wait for the sink manager worker to reset: {}",
                                            e.as_report()
                                        );
                                    }
                                }
                                send_with_err_check!(finish_notifier, ());
                            });
                        } else {
                            self.clean_up().await;
                            send_with_err_check!(finish_notifier, ());
                        }
                    }
                },
                ManagerEvent::CoordinatorWorkerFinished {
                    sink_id,
                    join_result,
                } => self.handle_coordinator_finished(sink_id, join_result),
            }
        }
        self.clean_up().await;
        info!("sink manager worker exited");
    }

    async fn next_event(&mut self) -> Option<ManagerEvent> {
        match select(
            select(
                pin!(self.request_rx.recv()),
                pin!(pending_on_none(
                    self.running_coordinator_worker_join_handles.next()
                )),
            ),
            &mut self.shutdown_rx,
        )
        .await
        {
            Either::Left((either, _)) => match either {
                Either::Left((Some(request), _)) => Some(ManagerEvent::NewRequest(request)),
                Either::Left((None, _)) => None,
                Either::Right(((sink_id, join_result), _)) => {
                    Some(ManagerEvent::CoordinatorWorkerFinished {
                        sink_id,
                        join_result,
                    })
                }
            },
            Either::Right(_) => None,
        }
    }

    async fn clean_up(&mut self) {
        info!("sink manager worker start cleaning up");
        for worker_handle in self.running_coordinator_worker.values_mut() {
            if let Some(sender) = worker_handle.request_sender.take() {
                // drop the sender to notify the coordinator worker to stop
                drop(sender);
            }
        }
        while let Some((sink_id, join_result)) =
            self.running_coordinator_worker_join_handles.next().await
        {
            self.handle_coordinator_finished(sink_id, join_result);
        }
        info!("sink manager worker finished cleaning up");
    }

    fn handle_coordinator_finished(&mut self, sink_id: SinkId, join_result: Result<(), JoinError>) {
        let worker_handle = self
            .running_coordinator_worker
            .remove(&sink_id)
            .expect("finished coordinator should have an associated worker handle");
        for finish_notifier in worker_handle.finish_notifiers {
            send_with_err_check!(finish_notifier, ());
        }
        match join_result {
            Ok(()) => {
                info!(
                    id = %sink_id,
                    "sink coordinator has gracefully finished",
                );
            }
            Err(err) => {
                error!(
                    id = %sink_id,
                    error = %err.as_report(),
                    "sink coordinator finished with error",
                );
            }
        }
    }

    fn handle_new_sink_writer(
        &mut self,
        new_writer: SinkWriterCoordinationHandle,
        spawn_coordinator_worker: &mut impl SpawnCoordinatorFn,
    ) {
        let param = new_writer.param();
        let sink_id = param.sink_id;

        let handle = self
            .running_coordinator_worker
            .entry(param.sink_id)
            .or_insert_with(|| {
                // Launch the coordinator worker task if it is the first
                let (request_tx, request_rx) = unbounded_channel();
                let join_handle = spawn_coordinator_worker(param.clone(), request_rx);
                self.running_coordinator_worker_join_handles.push(
                    join_handle
                        .map(move |join_result| (sink_id, join_result))
                        .boxed(),
                );
                CoordinatorWorkerHandle {
                    request_sender: Some(request_tx),
                    finish_notifiers: Vec::new(),
                }
            });

        if let Some(sender) = handle.request_sender.as_mut() {
            send_with_err_check!(sender, new_writer);
        } else {
            warn!(
                "handle a new request while the sink coordinator is being stopped: {:?}",
                param
            );
            new_writer.abort(Status::internal("the sink is being stopped"));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicI32;
    use std::task::Poll;

    use anyhow::anyhow;
    use async_trait::async_trait;
    use futures::future::{join, try_join};
    use futures::{FutureExt, StreamExt, TryFutureExt};
    use itertools::Itertools;
    use rand::seq::SliceRandom;
    use risingwave_common::bitmap::BitmapBuilder;
    use risingwave_common::hash::VirtualNode;
    use risingwave_connector::sink::catalog::{SinkId, SinkType};
    use risingwave_connector::sink::{
        SinglePhaseCommitCoordinator, SinkCommitCoordinator, SinkError, SinkParam,
        TwoPhaseCommitCoordinator,
    };
    use risingwave_meta_model::SinkSchemachange;
    use risingwave_pb::connector_service::SinkMetadata;
    use risingwave_pb::connector_service::sink_metadata::{Metadata, SerializedMetadata};
    use risingwave_pb::data::PbDataType;
    use risingwave_pb::data::data_type::PbTypeName;
    use risingwave_pb::plan_common::PbField;
    use risingwave_pb::stream_plan::sink_schema_change::Op as SinkSchemachangeOp;
    use risingwave_pb::stream_plan::{PbSinkAddColumnsOp, PbSinkSchemaChange};
    use risingwave_rpc_client::CoordinatorStreamHandle;
    use sea_orm::{ConnectionTrait, Database, DatabaseConnection};
    use tokio::sync::mpsc::unbounded_channel;
    use tokio_stream::wrappers::ReceiverStream;

    use crate::manager::sink_coordination::SinkCoordinatorManager;
    use crate::manager::sink_coordination::coordinator_worker::CoordinatorWorker;
    use crate::manager::sink_coordination::manager::SinkCommittedEpochSubscriber;

    struct MockSinglePhaseCoordinator<
        C,
        F: FnMut(u64, Vec<SinkMetadata>, &mut C) -> Result<(), SinkError>,
    > {
        context: C,
        f: F,
    }

    impl<
        C: Send + 'static,
        F: FnMut(u64, Vec<SinkMetadata>, &mut C) -> Result<(), SinkError> + Send + 'static,
    > MockSinglePhaseCoordinator<C, F>
    {
        fn new_coordinator(context: C, f: F) -> SinkCommitCoordinator {
            SinkCommitCoordinator::SinglePhase(Box::new(MockSinglePhaseCoordinator { context, f }))
        }
    }

    #[async_trait]
    impl<C: Send, F: FnMut(u64, Vec<SinkMetadata>, &mut C) -> Result<(), SinkError> + Send>
        SinglePhaseCommitCoordinator for MockSinglePhaseCoordinator<C, F>
    {
        async fn init(&mut self) -> risingwave_connector::sink::Result<()> {
            Ok(())
        }

        async fn commit_data(
            &mut self,
            epoch: u64,
            metadata: Vec<SinkMetadata>,
        ) -> risingwave_connector::sink::Result<()> {
            (self.f)(epoch, metadata, &mut self.context)
        }

        async fn commit_schema_change(
            &mut self,
            _epoch: u64,
            _schema_change: PbSinkSchemaChange,
        ) -> risingwave_connector::sink::Result<()> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn test_basic() {
        let db = prepare_db_backend().await;

        let param = SinkParam {
            sink_id: SinkId::from(1),
            sink_name: "test".into(),
            properties: Default::default(),
            columns: vec![],
            downstream_pk: None,
            sink_type: SinkType::AppendOnly,
            ignore_delete: false,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let epoch0 = 232;
        let epoch1 = 233;
        let epoch2 = 234;

        let mut all_vnode = (0..VirtualNode::COUNT_FOR_TEST).collect_vec();
        all_vnode.shuffle(&mut rand::rng());
        let (first, second) = all_vnode.split_at(VirtualNode::COUNT_FOR_TEST / 2);
        let build_bitmap = |indexes: &[usize]| {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
            for i in indexes {
                builder.set(*i, true);
            }
            builder.finish()
        };
        let vnode1 = build_bitmap(first);
        let vnode2 = build_bitmap(second);

        let metadata = [
            [vec![1u8, 2u8], vec![3u8, 4u8]],
            [vec![5u8, 6u8], vec![7u8, 8u8]],
        ];
        let sender = Arc::new(tokio::sync::Mutex::new(None));
        let mock_subscriber: SinkCommittedEpochSubscriber = {
            let captured_sender = sender.clone();
            Arc::new(move |_sink_id: SinkId| {
                let (sender, receiver) = unbounded_channel();
                let captured_sender = captured_sender.clone();
                async move {
                    let mut guard = captured_sender.lock().await;
                    *guard = Some(sender);
                    Ok((1, receiver))
                }
                .boxed()
            })
        };

        let (manager, (_join_handle, _stop_tx)) =
            SinkCoordinatorManager::start_worker_with_spawn_worker({
                let expected_param = param.clone();
                let metadata = metadata.clone();
                let db = db.clone();
                move |param, new_writer_rx| {
                    let metadata = metadata.clone();
                    let expected_param = expected_param.clone();
                    let db = db.clone();
                    tokio::spawn({
                        let subscriber = mock_subscriber.clone();
                        async move {
                            // validate the start request
                            assert_eq!(param, expected_param);
                            CoordinatorWorker::execute_coordinator(
                                db,
                                param.clone(),
                                new_writer_rx,
                                MockSinglePhaseCoordinator::new_coordinator(
                                    0,
                                    move |epoch, metadata_list, count: &mut usize| {
                                        *count += 1;
                                        let mut metadata_list =
                                            metadata_list
                                                .into_iter()
                                                .map(|metadata| match metadata {
                                                    SinkMetadata {
                                                        metadata:
                                                            Some(Metadata::Serialized(
                                                                SerializedMetadata { metadata },
                                                            )),
                                                    } => metadata,
                                                    _ => unreachable!(),
                                                })
                                                .collect_vec();
                                        metadata_list.sort();
                                        match *count {
                                            1 => {
                                                assert_eq!(epoch, epoch1);
                                                assert_eq!(2, metadata_list.len());
                                                assert_eq!(metadata[0][0], metadata_list[0]);
                                                assert_eq!(metadata[0][1], metadata_list[1]);
                                            }
                                            2 => {
                                                assert_eq!(epoch, epoch2);
                                                assert_eq!(2, metadata_list.len());
                                                assert_eq!(metadata[1][0], metadata_list[0]);
                                                assert_eq!(metadata[1][1], metadata_list[1]);
                                            }
                                            _ => unreachable!(),
                                        }
                                        Ok(())
                                    },
                                ),
                                subscriber.clone(),
                            )
                            .await;
                        }
                    })
                }
            });

        let build_client = |vnode| async {
            CoordinatorStreamHandle::new_with_init_stream(param.to_proto(), vnode, |rx| async {
                Ok(tonic::Response::new(
                    manager
                        .handle_new_request(ReceiverStream::new(rx).map(Ok).boxed())
                        .await
                        .unwrap()
                        .boxed(),
                ))
            })
            .await
            .unwrap()
            .0
        };

        let (mut client1, mut client2) =
            join(build_client(vnode1), pin!(build_client(vnode2))).await;

        let (aligned_epoch1, aligned_epoch2) = try_join(
            client1.align_initial_epoch(epoch0),
            client2.align_initial_epoch(epoch1),
        )
        .await
        .unwrap();
        assert_eq!(aligned_epoch1, epoch1);
        assert_eq!(aligned_epoch2, epoch1);

        {
            // commit epoch1
            let mut commit_future = pin!(
                client2
                    .commit(
                        epoch1,
                        SinkMetadata {
                            metadata: Some(Metadata::Serialized(SerializedMetadata {
                                metadata: metadata[0][1].clone(),
                            })),
                        },
                        None,
                    )
                    .map(|result| result.unwrap())
            );
            assert!(
                poll_fn(|cx| Poll::Ready(commit_future.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            join(
                commit_future,
                client1
                    .commit(
                        epoch1,
                        SinkMetadata {
                            metadata: Some(Metadata::Serialized(SerializedMetadata {
                                metadata: metadata[0][0].clone(),
                            })),
                        },
                        None,
                    )
                    .map(|result| result.unwrap()),
            )
            .await;
        }

        // commit epoch2
        let mut commit_future = pin!(
            client1
                .commit(
                    epoch2,
                    SinkMetadata {
                        metadata: Some(Metadata::Serialized(SerializedMetadata {
                            metadata: metadata[1][0].clone(),
                        })),
                    },
                    None,
                )
                .map(|result| result.unwrap())
        );
        assert!(
            poll_fn(|cx| Poll::Ready(commit_future.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        join(
            commit_future,
            client2
                .commit(
                    epoch2,
                    SinkMetadata {
                        metadata: Some(Metadata::Serialized(SerializedMetadata {
                            metadata: metadata[1][1].clone(),
                        })),
                    },
                    None,
                )
                .map(|result| result.unwrap()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_single_writer() {
        let db = prepare_db_backend().await;
        let param = SinkParam {
            sink_id: SinkId::from(1),
            sink_name: "test".into(),
            properties: Default::default(),
            columns: vec![],
            downstream_pk: None,
            sink_type: SinkType::AppendOnly,
            ignore_delete: false,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let epoch1 = 233;
        let epoch2 = 234;

        let all_vnode = (0..VirtualNode::COUNT_FOR_TEST).collect_vec();
        let build_bitmap = |indexes: &[usize]| {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
            for i in indexes {
                builder.set(*i, true);
            }
            builder.finish()
        };
        let vnode = build_bitmap(&all_vnode);

        let metadata = [vec![1u8, 2u8], vec![3u8, 4u8]];
        let sender = Arc::new(tokio::sync::Mutex::new(None));
        let mock_subscriber: SinkCommittedEpochSubscriber = {
            let captured_sender = sender.clone();
            Arc::new(move |_sink_id: SinkId| {
                let (sender, receiver) = unbounded_channel();
                let captured_sender = captured_sender.clone();
                async move {
                    let mut guard = captured_sender.lock().await;
                    *guard = Some(sender);
                    Ok((1, receiver))
                }
                .boxed()
            })
        };
        let (manager, (_join_handle, _stop_tx)) =
            SinkCoordinatorManager::start_worker_with_spawn_worker({
                let expected_param = param.clone();
                let metadata = metadata.clone();
                let db = db.clone();
                move |param, new_writer_rx| {
                    let metadata = metadata.clone();
                    let expected_param = expected_param.clone();
                    let db = db.clone();
                    tokio::spawn({
                        let subscriber = mock_subscriber.clone();
                        async move {
                            // validate the start request
                            assert_eq!(param, expected_param);
                            CoordinatorWorker::execute_coordinator(
                                db,
                                param.clone(),
                                new_writer_rx,
                                MockSinglePhaseCoordinator::new_coordinator(
                                    0,
                                    move |epoch, metadata_list, count: &mut usize| {
                                        *count += 1;
                                        let mut metadata_list =
                                            metadata_list
                                                .into_iter()
                                                .map(|metadata| match metadata {
                                                    SinkMetadata {
                                                        metadata:
                                                            Some(Metadata::Serialized(
                                                                SerializedMetadata { metadata },
                                                            )),
                                                    } => metadata,
                                                    _ => unreachable!(),
                                                })
                                                .collect_vec();
                                        metadata_list.sort();
                                        match *count {
                                            1 => {
                                                assert_eq!(epoch, epoch1);
                                                assert_eq!(1, metadata_list.len());
                                                assert_eq!(metadata[0], metadata_list[0]);
                                            }
                                            2 => {
                                                assert_eq!(epoch, epoch2);
                                                assert_eq!(1, metadata_list.len());
                                                assert_eq!(metadata[1], metadata_list[0]);
                                            }
                                            _ => unreachable!(),
                                        }
                                        Ok(())
                                    },
                                ),
                                subscriber.clone(),
                            )
                            .await;
                        }
                    })
                }
            });

        let build_client = |vnode| async {
            CoordinatorStreamHandle::new_with_init_stream(param.to_proto(), vnode, |rx| async {
                Ok(tonic::Response::new(
                    manager
                        .handle_new_request(ReceiverStream::new(rx).map(Ok).boxed())
                        .await
                        .unwrap()
                        .boxed(),
                ))
            })
            .await
            .unwrap()
            .0
        };

        let mut client = build_client(vnode).await;

        let aligned_epoch = client.align_initial_epoch(epoch1).await.unwrap();
        assert_eq!(aligned_epoch, epoch1);

        client
            .commit(
                epoch1,
                SinkMetadata {
                    metadata: Some(Metadata::Serialized(SerializedMetadata {
                        metadata: metadata[0].clone(),
                    })),
                },
                None,
            )
            .await
            .unwrap();

        client
            .commit(
                epoch2,
                SinkMetadata {
                    metadata: Some(Metadata::Serialized(SerializedMetadata {
                        metadata: metadata[1].clone(),
                    })),
                },
                None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_partial_commit() {
        let db = prepare_db_backend().await;
        let param = SinkParam {
            sink_id: SinkId::from(1),
            sink_name: "test".into(),
            properties: Default::default(),
            columns: vec![],
            downstream_pk: None,
            sink_type: SinkType::AppendOnly,
            ignore_delete: false,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let epoch = 233;

        let mut all_vnode = (0..VirtualNode::COUNT_FOR_TEST).collect_vec();
        all_vnode.shuffle(&mut rand::rng());
        let (first, second) = all_vnode.split_at(VirtualNode::COUNT_FOR_TEST / 2);
        let build_bitmap = |indexes: &[usize]| {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
            for i in indexes {
                builder.set(*i, true);
            }
            builder.finish()
        };
        let vnode1 = build_bitmap(first);
        let vnode2 = build_bitmap(second);

        let sender = Arc::new(tokio::sync::Mutex::new(None));
        let mock_subscriber: SinkCommittedEpochSubscriber = {
            let captured_sender = sender.clone();
            Arc::new(move |_sink_id: SinkId| {
                let (sender, receiver) = unbounded_channel();
                let captured_sender = captured_sender.clone();
                async move {
                    let mut guard = captured_sender.lock().await;
                    *guard = Some(sender);
                    Ok((1, receiver))
                }
                .boxed()
            })
        };
        let (manager, (_join_handle, _stop_tx)) =
            SinkCoordinatorManager::start_worker_with_spawn_worker({
                let expected_param = param.clone();
                let db = db.clone();
                move |param, new_writer_rx| {
                    let expected_param = expected_param.clone();
                    let db = db.clone();
                    tokio::spawn({
                        let subscriber = mock_subscriber.clone();
                        async move {
                            // validate the start request
                            assert_eq!(param, expected_param);
                            CoordinatorWorker::execute_coordinator(
                                db,
                                param,
                                new_writer_rx,
                                MockSinglePhaseCoordinator::new_coordinator(
                                    (),
                                    |_, _, _| unreachable!(),
                                ),
                                subscriber.clone(),
                            )
                            .await;
                        }
                    })
                }
            });

        let build_client = |vnode| async {
            CoordinatorStreamHandle::new_with_init_stream(param.to_proto(), vnode, |rx| async {
                Ok(tonic::Response::new(
                    manager
                        .handle_new_request(ReceiverStream::new(rx).map(Ok).boxed())
                        .await
                        .unwrap()
                        .boxed(),
                ))
            })
            .await
            .unwrap()
            .0
        };

        let (mut client1, client2) = join(build_client(vnode1), build_client(vnode2)).await;

        // commit epoch
        let mut commit_future = pin!(client1.commit(
            epoch,
            SinkMetadata {
                metadata: Some(Metadata::Serialized(SerializedMetadata {
                    metadata: vec![],
                })),
            },
            None,
        ));
        assert!(
            poll_fn(|cx| Poll::Ready(commit_future.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        drop(client2);
        assert!(commit_future.await.is_err());
    }

    #[tokio::test]
    async fn test_fail_commit() {
        let db = prepare_db_backend().await;
        let param = SinkParam {
            sink_id: SinkId::from(1),
            sink_name: "test".into(),
            properties: Default::default(),
            columns: vec![],
            downstream_pk: None,
            sink_type: SinkType::AppendOnly,
            ignore_delete: false,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let epoch = 233;

        let mut all_vnode = (0..VirtualNode::COUNT_FOR_TEST).collect_vec();
        all_vnode.shuffle(&mut rand::rng());
        let (first, second) = all_vnode.split_at(VirtualNode::COUNT_FOR_TEST / 2);
        let build_bitmap = |indexes: &[usize]| {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
            for i in indexes {
                builder.set(*i, true);
            }
            builder.finish()
        };
        let vnode1 = build_bitmap(first);
        let vnode2 = build_bitmap(second);
        let sender = Arc::new(tokio::sync::Mutex::new(None));
        let mock_subscriber: SinkCommittedEpochSubscriber = {
            let captured_sender = sender.clone();
            Arc::new(move |_sink_id: SinkId| {
                let (sender, receiver) = unbounded_channel();
                let captured_sender = captured_sender.clone();
                async move {
                    let mut guard = captured_sender.lock().await;
                    *guard = Some(sender);
                    Ok((1, receiver))
                }
                .boxed()
            })
        };
        let (manager, (_join_handle, _stop_tx)) =
            SinkCoordinatorManager::start_worker_with_spawn_worker({
                let expected_param = param.clone();
                let db = db.clone();
                move |param, new_writer_rx| {
                    let expected_param = expected_param.clone();
                    let db = db.clone();
                    tokio::spawn({
                        let subscriber = mock_subscriber.clone();
                        {
                            async move {
                                // validate the start request
                                assert_eq!(param, expected_param);
                                CoordinatorWorker::execute_coordinator(
                                    db,
                                    param,
                                    new_writer_rx,
                                    MockSinglePhaseCoordinator::new_coordinator((), |_, _, _| {
                                        Err(SinkError::Coordinator(anyhow!("failed to commit")))
                                    }),
                                    subscriber.clone(),
                                )
                                .await;
                            }
                        }
                    })
                }
            });

        let build_client = |vnode| async {
            CoordinatorStreamHandle::new_with_init_stream(param.to_proto(), vnode, |rx| async {
                Ok(tonic::Response::new(
                    manager
                        .handle_new_request(ReceiverStream::new(rx).map(Ok).boxed())
                        .await
                        .unwrap()
                        .boxed(),
                ))
            })
            .await
            .unwrap()
            .0
        };

        let (mut client1, mut client2) = join(build_client(vnode1), build_client(vnode2)).await;

        // commit epoch
        let mut commit_future = pin!(client1.commit(
            epoch,
            SinkMetadata {
                metadata: Some(Metadata::Serialized(SerializedMetadata {
                    metadata: vec![],
                })),
            },
            None,
        ));
        assert!(
            poll_fn(|cx| Poll::Ready(commit_future.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        let (result1, result2) = join(
            commit_future,
            client2.commit(
                epoch,
                SinkMetadata {
                    metadata: Some(Metadata::Serialized(SerializedMetadata {
                        metadata: vec![],
                    })),
                },
                None,
            ),
        )
        .await;
        assert!(result1.is_err());
        assert!(result2.is_err());
    }

    #[tokio::test]
    async fn test_update_vnode_bitmap() {
        let db = prepare_db_backend().await;
        let param = SinkParam {
            sink_id: SinkId::from(1),
            sink_name: "test".into(),
            properties: Default::default(),
            columns: vec![],
            downstream_pk: None,
            sink_type: SinkType::AppendOnly,
            ignore_delete: false,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let epoch1 = 233;
        let epoch2 = 234;
        let epoch3 = 235;
        let epoch4 = 236;

        let mut all_vnode = (0..VirtualNode::COUNT_FOR_TEST).collect_vec();
        all_vnode.shuffle(&mut rand::rng());
        let (first, second) = all_vnode.split_at(VirtualNode::COUNT_FOR_TEST / 2);
        let build_bitmap = |indexes: &[usize]| {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
            for i in indexes {
                builder.set(*i, true);
            }
            builder.finish()
        };
        let vnode1 = build_bitmap(first);
        let vnode2 = build_bitmap(second);

        let metadata = [
            [vec![1u8, 2u8], vec![3u8, 4u8]],
            [vec![5u8, 6u8], vec![7u8, 8u8]],
        ];

        let metadata_scale_out = [vec![9u8, 10u8], vec![11u8, 12u8], vec![13u8, 14u8]];
        let metadata_scale_in = [vec![13u8, 14u8], vec![15u8, 16u8]];
        let sender = Arc::new(tokio::sync::Mutex::new(None));
        let mock_subscriber: SinkCommittedEpochSubscriber = {
            let captured_sender = sender.clone();
            Arc::new(move |_sink_id: SinkId| {
                let (sender, receiver) = unbounded_channel();
                let captured_sender = captured_sender.clone();
                async move {
                    let mut guard = captured_sender.lock().await;
                    *guard = Some(sender);
                    Ok((1, receiver))
                }
                .boxed()
            })
        };
        let (manager, (_join_handle, _stop_tx)) =
            SinkCoordinatorManager::start_worker_with_spawn_worker({
                let expected_param = param.clone();
                let metadata = metadata.clone();
                let metadata_scale_out = metadata_scale_out.clone();
                let metadata_scale_in = metadata_scale_in.clone();
                let db = db.clone();
                move |param, new_writer_rx| {
                    let metadata = metadata.clone();
                    let metadata_scale_out = metadata_scale_out.clone();
                    let metadata_scale_in = metadata_scale_in.clone();
                    let expected_param = expected_param.clone();
                    let db = db.clone();
                    tokio::spawn({
                        let subscriber = mock_subscriber.clone();
                        async move {
                            // validate the start request
                            assert_eq!(param, expected_param);
                            CoordinatorWorker::execute_coordinator(
                                db,
                                param.clone(),
                                new_writer_rx,
                                MockSinglePhaseCoordinator::new_coordinator(
                                    0,
                                    move |epoch, metadata_list, count: &mut usize| {
                                        *count += 1;
                                        let mut metadata_list =
                                            metadata_list
                                                .into_iter()
                                                .map(|metadata| match metadata {
                                                    SinkMetadata {
                                                        metadata:
                                                            Some(Metadata::Serialized(
                                                                SerializedMetadata { metadata },
                                                            )),
                                                    } => metadata,
                                                    _ => unreachable!(),
                                                })
                                                .collect_vec();
                                        metadata_list.sort();
                                        let (expected_epoch, expected_metadata_list) = match *count
                                        {
                                            1 => (epoch1, metadata[0].as_slice()),
                                            2 => (epoch2, metadata[1].as_slice()),
                                            3 => (epoch3, metadata_scale_out.as_slice()),
                                            4 => (epoch4, metadata_scale_in.as_slice()),
                                            _ => unreachable!(),
                                        };
                                        assert_eq!(expected_epoch, epoch);
                                        assert_eq!(expected_metadata_list, &metadata_list);
                                        Ok(())
                                    },
                                ),
                                subscriber.clone(),
                            )
                            .await;
                        }
                    })
                }
            });

        let build_client = |vnode| async {
            CoordinatorStreamHandle::new_with_init_stream(param.to_proto(), vnode, |rx| async {
                Ok(tonic::Response::new(
                    manager
                        .handle_new_request(ReceiverStream::new(rx).map(Ok).boxed())
                        .await
                        .unwrap()
                        .boxed(),
                ))
            })
            .await
        };

        let ((mut client1, _), (mut client2, _)) =
            try_join(build_client(vnode1), pin!(build_client(vnode2)))
                .await
                .unwrap();

        let (aligned_epoch1, aligned_epoch2) = try_join(
            client1.align_initial_epoch(epoch1),
            client2.align_initial_epoch(epoch1),
        )
        .await
        .unwrap();
        assert_eq!(aligned_epoch1, epoch1);
        assert_eq!(aligned_epoch2, epoch1);

        {
            // commit epoch1
            let mut commit_future = pin!(
                client2
                    .commit(
                        epoch1,
                        SinkMetadata {
                            metadata: Some(Metadata::Serialized(SerializedMetadata {
                                metadata: metadata[0][1].clone(),
                            })),
                        },
                        None,
                    )
                    .map(|result| result.unwrap())
            );
            assert!(
                poll_fn(|cx| Poll::Ready(commit_future.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            join(
                commit_future,
                client1
                    .commit(
                        epoch1,
                        SinkMetadata {
                            metadata: Some(Metadata::Serialized(SerializedMetadata {
                                metadata: metadata[0][0].clone(),
                            })),
                        },
                        None,
                    )
                    .map(|result| result.unwrap()),
            )
            .await;
        }

        let (vnode1, vnode2, vnode3) = {
            let (first, second) = all_vnode.split_at(VirtualNode::COUNT_FOR_TEST / 3);
            let (second, third) = second.split_at(VirtualNode::COUNT_FOR_TEST / 3);
            (
                build_bitmap(first),
                build_bitmap(second),
                build_bitmap(third),
            )
        };

        let mut build_client3_future = pin!(build_client(vnode3));
        assert!(
            poll_fn(|cx| Poll::Ready(build_client3_future.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        let mut client3;
        {
            {
                // commit epoch2
                let mut commit_future = pin!(
                    client1
                        .commit(
                            epoch2,
                            SinkMetadata {
                                metadata: Some(Metadata::Serialized(SerializedMetadata {
                                    metadata: metadata[1][0].clone(),
                                })),
                            },
                            None,
                        )
                        .map_err(Into::into)
                );
                assert!(
                    poll_fn(|cx| Poll::Ready(commit_future.as_mut().poll(cx)))
                        .await
                        .is_pending()
                );
                try_join(
                    commit_future,
                    client2.commit(
                        epoch2,
                        SinkMetadata {
                            metadata: Some(Metadata::Serialized(SerializedMetadata {
                                metadata: metadata[1][1].clone(),
                            })),
                        },
                        None,
                    ),
                )
                .await
                .unwrap();
            }

            client3 = {
                let (
                    (client3, init_epoch),
                    (update_vnode_bitmap_epoch1, update_vnode_bitmap_epoch2),
                ) = try_join(
                    build_client3_future,
                    try_join(
                        client1.update_vnode_bitmap(&vnode1),
                        client2.update_vnode_bitmap(&vnode2),
                    )
                    .map_err(Into::into),
                )
                .await
                .unwrap();
                assert_eq!(init_epoch, Some(epoch2));
                assert_eq!(update_vnode_bitmap_epoch1, epoch2);
                assert_eq!(update_vnode_bitmap_epoch2, epoch2);
                client3
            };
            let mut commit_future3 = pin!(client3.commit(
                epoch3,
                SinkMetadata {
                    metadata: Some(Metadata::Serialized(SerializedMetadata {
                        metadata: metadata_scale_out[2].clone(),
                    })),
                },
                None,
            ));
            assert!(
                poll_fn(|cx| Poll::Ready(commit_future3.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            let mut commit_future1 = pin!(client1.commit(
                epoch3,
                SinkMetadata {
                    metadata: Some(Metadata::Serialized(SerializedMetadata {
                        metadata: metadata_scale_out[0].clone(),
                    })),
                },
                None,
            ));
            assert!(
                poll_fn(|cx| Poll::Ready(commit_future1.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            assert!(
                poll_fn(|cx| Poll::Ready(commit_future3.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            try_join(
                client2.commit(
                    epoch3,
                    SinkMetadata {
                        metadata: Some(Metadata::Serialized(SerializedMetadata {
                            metadata: metadata_scale_out[1].clone(),
                        })),
                    },
                    None,
                ),
                try_join(commit_future1, commit_future3),
            )
            .await
            .unwrap();
        }

        let (vnode2, vnode3) = {
            let (first, second) = all_vnode.split_at(VirtualNode::COUNT_FOR_TEST / 3);
            (build_bitmap(first), build_bitmap(second))
        };

        {
            let (_, (update_vnode_bitmap_epoch2, update_vnode_bitmap_epoch3)) = try_join(
                client1.stop(),
                try_join(
                    client2.update_vnode_bitmap(&vnode2),
                    client3.update_vnode_bitmap(&vnode3),
                ),
            )
            .await
            .unwrap();
            assert_eq!(update_vnode_bitmap_epoch2, epoch3);
            assert_eq!(update_vnode_bitmap_epoch3, epoch3);
        }

        {
            let mut commit_future = pin!(
                client2
                    .commit(
                        epoch4,
                        SinkMetadata {
                            metadata: Some(Metadata::Serialized(SerializedMetadata {
                                metadata: metadata_scale_in[0].clone(),
                            })),
                        },
                        None,
                    )
                    .map(|result| result.unwrap())
            );
            assert!(
                poll_fn(|cx| Poll::Ready(commit_future.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            join(
                commit_future,
                client3
                    .commit(
                        epoch4,
                        SinkMetadata {
                            metadata: Some(Metadata::Serialized(SerializedMetadata {
                                metadata: metadata_scale_in[1].clone(),
                            })),
                        },
                        None,
                    )
                    .map(|result| result.unwrap()),
            )
            .await;
        }
    }

    struct MockTwoPhaseCoordinator<
        P: FnMut(
            u64,
            Vec<SinkMetadata>,
            Option<PbSinkSchemaChange>,
        ) -> Result<Option<Vec<u8>>, SinkError>,
        CD: FnMut(u64, Vec<u8>) -> Result<(), SinkError>,
        CS: FnMut(u64, PbSinkSchemaChange) -> Result<(), SinkError>,
    > {
        pre_commit: P,
        commit_data: CD,
        commit_schema_change: CS,
    }

    impl<
        P: FnMut(
                u64,
                Vec<SinkMetadata>,
                Option<PbSinkSchemaChange>,
            ) -> Result<Option<Vec<u8>>, SinkError>
            + Send
            + 'static,
        CD: FnMut(u64, Vec<u8>) -> Result<(), SinkError> + Send + 'static,
        CS: FnMut(u64, PbSinkSchemaChange) -> Result<(), SinkError> + Send + 'static,
    > MockTwoPhaseCoordinator<P, CD, CS>
    {
        fn new_coordinator(
            pre_commit: P,
            commit_data: CD,
            commit_schema_change: CS,
        ) -> SinkCommitCoordinator {
            SinkCommitCoordinator::TwoPhase(Box::new(MockTwoPhaseCoordinator {
                pre_commit,
                commit_data,
                commit_schema_change,
            }))
        }
    }

    #[async_trait]
    impl<
        P: FnMut(
                u64,
                Vec<SinkMetadata>,
                Option<PbSinkSchemaChange>,
            ) -> Result<Option<Vec<u8>>, SinkError>
            + Send
            + 'static,
        CD: FnMut(u64, Vec<u8>) -> Result<(), SinkError> + Send + 'static,
        CS: FnMut(u64, PbSinkSchemaChange) -> Result<(), SinkError> + Send + 'static,
    > TwoPhaseCommitCoordinator for MockTwoPhaseCoordinator<P, CD, CS>
    {
        async fn init(&mut self) -> risingwave_connector::sink::Result<()> {
            Ok(())
        }

        async fn pre_commit(
            &mut self,
            epoch: u64,
            metadata: Vec<SinkMetadata>,
            schema_change: Option<PbSinkSchemaChange>,
        ) -> risingwave_connector::sink::Result<Option<Vec<u8>>> {
            (self.pre_commit)(epoch, metadata, schema_change)
        }

        async fn commit_data(
            &mut self,
            epoch: u64,
            commit_metadata: Vec<u8>,
        ) -> risingwave_connector::sink::Result<()> {
            (self.commit_data)(epoch, commit_metadata)
        }

        async fn commit_schema_change(
            &mut self,
            epoch: u64,
            schema_change: PbSinkSchemaChange,
        ) -> risingwave_connector::sink::Result<()> {
            (self.commit_schema_change)(epoch, schema_change)
        }

        async fn abort(&mut self, _epoch: u64, _commit_metadata: Vec<u8>) {
            tracing::debug!("abort called");
        }
    }

    async fn prepare_db_backend() -> DatabaseConnection {
        let db: DatabaseConnection = Database::connect("sqlite::memory:").await.unwrap();
        let ddl = "
            CREATE TABLE IF NOT EXISTS pending_sink_state (
                sink_id i32 NOT NULL,
                epoch i64 NOT NULL,
                sink_state STRING NOT NULL,
                metadata BLOB,
                schema_change BLOB,
                PRIMARY KEY (sink_id, epoch)
            )
        ";
        db.execute(sea_orm::Statement::from_string(
            db.get_database_backend(),
            ddl.to_owned(),
        ))
        .await
        .unwrap();
        db
    }

    async fn list_rows(
        db: &DatabaseConnection,
    ) -> Vec<(
        i32,
        i64,
        String,
        Option<Vec<u8>>,
        Option<PbSinkSchemaChange>,
    )> {
        let sql =
            "SELECT sink_id, epoch, sink_state, metadata, schema_change FROM pending_sink_state";
        let rows = db
            .query_all(sea_orm::Statement::from_string(
                db.get_database_backend(),
                sql.to_owned(),
            ))
            .await
            .unwrap();
        rows.into_iter()
            .map(|row| {
                (
                    row.try_get("", "sink_id").unwrap(),
                    row.try_get("", "epoch").unwrap(),
                    row.try_get("", "sink_state").unwrap(),
                    row.try_get("", "metadata").unwrap(),
                    row.try_get::<Option<SinkSchemachange>>("", "schema_change")
                        .unwrap()
                        .map(|v| v.to_protobuf()),
                )
            })
            .collect()
    }

    async fn set_epoch_aborted(db: &DatabaseConnection, sink_id: SinkId, epoch: u64) {
        let sql = format!(
            "UPDATE pending_sink_state SET sink_state = 'ABORTED' WHERE sink_id = {} AND epoch = {}",
            sink_id, epoch as i64
        );
        db.execute(sea_orm::Statement::from_string(
            db.get_database_backend(),
            sql,
        ))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_init_response_skips_recovered_empty_pending_epoch() {
        let db = prepare_db_backend().await;

        let param = SinkParam {
            sink_id: SinkId::from(1),
            sink_name: "test".into(),
            properties: Default::default(),
            columns: vec![],
            downstream_pk: None,
            sink_type: SinkType::AppendOnly,
            ignore_delete: false,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let epoch1 = 233;

        let sql = format!(
            "INSERT INTO pending_sink_state (sink_id, epoch, sink_state) VALUES ({}, {}, 'PENDING')",
            param.sink_id, epoch1
        );
        db.execute(sea_orm::Statement::from_string(
            db.get_database_backend(),
            sql,
        ))
        .await
        .unwrap();

        let all_vnode = (0..VirtualNode::COUNT_FOR_TEST).collect_vec();
        let build_bitmap = |indexes: &[usize]| {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
            for i in indexes {
                builder.set(*i, true);
            }
            builder.finish()
        };
        let vnode = build_bitmap(&all_vnode);

        let sender = Arc::new(tokio::sync::Mutex::new(None));
        let mock_subscriber: SinkCommittedEpochSubscriber = {
            let captured_sender = sender.clone();
            Arc::new(move |_sink_id: SinkId| {
                let (epoch_sender, receiver) = unbounded_channel();
                let captured_sender = captured_sender.clone();
                async move {
                    let mut guard = captured_sender.lock().await;
                    *guard = Some(epoch_sender);
                    Ok((epoch1, receiver))
                }
                .boxed()
            })
        };

        let pre_commit_attempt = Arc::new(AtomicI32::new(0));
        let commit_attempt = Arc::new(AtomicI32::new(0));
        let (manager, (_join_handle, _stop_tx)) =
            SinkCoordinatorManager::start_worker_with_spawn_worker({
                let expected_param = param.clone();
                let db = db.clone();
                let pre_commit_attempt = pre_commit_attempt.clone();
                let commit_attempt = commit_attempt.clone();
                move |param, new_writer_rx| {
                    let expected_param = expected_param.clone();
                    let db = db.clone();
                    let pre_commit_attempt = pre_commit_attempt.clone();
                    let commit_attempt = commit_attempt.clone();
                    tokio::spawn({
                        let subscriber = mock_subscriber.clone();
                        async move {
                            assert_eq!(param, expected_param);
                            CoordinatorWorker::execute_coordinator(
                                db,
                                param.clone(),
                                new_writer_rx,
                                MockTwoPhaseCoordinator::new_coordinator(
                                    move |_epoch, _metadata_list, _schema_change| {
                                        pre_commit_attempt.fetch_add(
                                            1,
                                            std::sync::atomic::Ordering::SeqCst,
                                        );
                                        Err(SinkError::Coordinator(anyhow!(
                                            "pre_commit should not be called for a known pending epoch"
                                        )))
                                    },
                                    move |_epoch, _commit_metadata| {
                                        commit_attempt
                                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                        Ok(())
                                    },
                                    move |_epoch, _schema_change| unreachable!(),
                                ),
                                subscriber.clone(),
                            )
                            .await;
                        }
                    })
                }
            });

        let (_client, log_store_rewind_start_epoch) =
            CoordinatorStreamHandle::new_with_init_stream(param.to_proto(), vnode, |rx| async {
                Ok(tonic::Response::new(
                    manager
                        .handle_new_request(ReceiverStream::new(rx).map(Ok).boxed())
                        .await
                        .unwrap()
                        .boxed(),
                ))
            })
            .await
            .unwrap();
        assert_eq!(log_store_rewind_start_epoch, Some(epoch1));

        for _ in 0..50 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            let rows = list_rows(&db).await;
            if rows[0].2 == "COMMITTED" {
                break;
            }
        }

        assert_eq!(
            pre_commit_attempt.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(commit_attempt.load(std::sync::atomic::Ordering::SeqCst), 0);
        let rows = list_rows(&db).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, epoch1 as i64);
        assert_eq!(rows[0].2, "COMMITTED");
        assert_eq!(rows[0].3, None);
    }

    #[tokio::test]
    async fn test_pre_commit_failed() {
        let db = prepare_db_backend().await;

        let param = SinkParam {
            sink_id: SinkId::from(1),
            sink_name: "test".into(),
            properties: Default::default(),
            columns: vec![],
            downstream_pk: None,
            sink_type: SinkType::AppendOnly,
            ignore_delete: false,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let epoch1 = 233;

        let all_vnode = (0..VirtualNode::COUNT_FOR_TEST).collect_vec();
        let build_bitmap = |indexes: &[usize]| {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
            for i in indexes {
                builder.set(*i, true);
            }
            builder.finish()
        };
        let vnode = build_bitmap(&all_vnode);

        let metadata = vec![1u8, 2u8];
        let sender = Arc::new(tokio::sync::Mutex::new(None));
        let mock_subscriber: SinkCommittedEpochSubscriber = {
            let captured_sender = sender.clone();
            Arc::new(move |_sink_id: SinkId| {
                let (sender, receiver) = unbounded_channel();
                let captured_sender = captured_sender.clone();
                async move {
                    let mut guard = captured_sender.lock().await;
                    *guard = Some(sender);
                    Ok((epoch1, receiver))
                }
                .boxed()
            })
        };

        let (manager, (_join_handle, _stop_tx)) =
            SinkCoordinatorManager::start_worker_with_spawn_worker({
                let expected_param = param.clone();
                let db = db.clone();
                move |param, new_writer_rx| {
                    let expected_param = expected_param.clone();
                    let db = db.clone();
                    tokio::spawn({
                        let subscriber = mock_subscriber.clone();
                        async move {
                            // validate the start request
                            assert_eq!(param, expected_param);
                            CoordinatorWorker::execute_coordinator(
                                db,
                                param.clone(),
                                new_writer_rx,
                                MockTwoPhaseCoordinator::new_coordinator(
                                    move |_epoch, _metadata_list, _schema_change| {
                                        Err(SinkError::Coordinator(anyhow!("failed to pre commit")))
                                    },
                                    move |_epoch, _commit_metadata| unreachable!(),
                                    move |_epoch, _schema_change| unreachable!(),
                                ),
                                subscriber.clone(),
                            )
                            .await;
                        }
                    })
                }
            });

        let build_client = |vnode| async {
            CoordinatorStreamHandle::new_with_init_stream(param.to_proto(), vnode, |rx| async {
                Ok(tonic::Response::new(
                    manager
                        .handle_new_request(ReceiverStream::new(rx).map(Ok).boxed())
                        .await
                        .unwrap()
                        .boxed(),
                ))
            })
            .await
            .unwrap()
            .0
        };

        let mut client = build_client(vnode).await;

        let aligned_epoch = client.align_initial_epoch(1).await.unwrap();
        assert_eq!(aligned_epoch, 1);

        let commit_result = client
            .commit(
                epoch1,
                SinkMetadata {
                    metadata: Some(Metadata::Serialized(SerializedMetadata {
                        metadata: metadata.clone(),
                    })),
                },
                None,
            )
            .await;
        assert!(commit_result.is_err());

        let rows = list_rows(&db).await;
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn test_empty_pre_commit_waiting_on_checkpoint() {
        let db = prepare_db_backend().await;

        let param = SinkParam {
            sink_id: SinkId::from(1),
            sink_name: "test".into(),
            properties: Default::default(),
            columns: vec![],
            downstream_pk: None,
            sink_type: SinkType::AppendOnly,
            ignore_delete: false,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let epoch0 = 232;
        let epoch1 = 233;

        let all_vnode = (0..VirtualNode::COUNT_FOR_TEST).collect_vec();
        let build_bitmap = |indexes: &[usize]| {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
            for i in indexes {
                builder.set(*i, true);
            }
            builder.finish()
        };
        let vnode = build_bitmap(&all_vnode);

        let metadata = vec![1u8, 2u8];

        let sender = Arc::new(tokio::sync::Mutex::new(None));
        let mock_subscriber: SinkCommittedEpochSubscriber = {
            let captured_sender = sender.clone();
            Arc::new(move |_sink_id: SinkId| {
                let (sender, receiver) = unbounded_channel();
                let captured_sender = captured_sender.clone();
                async move {
                    let mut guard = captured_sender.lock().await;
                    *guard = Some(sender);
                    Ok((epoch0, receiver))
                }
                .boxed()
            })
        };

        let (manager, (_join_handle, _stop_tx)) =
            SinkCoordinatorManager::start_worker_with_spawn_worker({
                let expected_param = param.clone();
                let db = db.clone();
                move |param, new_writer_rx| {
                    let expected_param = expected_param.clone();
                    let db = db.clone();
                    tokio::spawn({
                        let subscriber = mock_subscriber.clone();
                        async move {
                            // validate the start request
                            assert_eq!(param, expected_param);
                            CoordinatorWorker::execute_coordinator(
                                db,
                                param.clone(),
                                new_writer_rx,
                                MockTwoPhaseCoordinator::new_coordinator(
                                    move |_epoch, metadata_list, _schema_change| {
                                        assert_eq!(metadata_list.len(), 1);
                                        Ok(None)
                                    },
                                    move |_epoch, _commit_metadata| unreachable!(),
                                    move |_epoch, _schema_change| unreachable!(),
                                ),
                                subscriber.clone(),
                            )
                            .await;
                        }
                    })
                }
            });

        let build_client = |vnode| async {
            CoordinatorStreamHandle::new_with_init_stream(param.to_proto(), vnode, |rx| async {
                Ok(tonic::Response::new(
                    manager
                        .handle_new_request(ReceiverStream::new(rx).map(Ok).boxed())
                        .await
                        .unwrap()
                        .boxed(),
                ))
            })
            .await
            .unwrap()
            .0
        };

        let mut client = build_client(vnode).await;

        let aligned_epoch = client.align_initial_epoch(1).await.unwrap();
        assert_eq!(aligned_epoch, 1);

        client
            .commit(
                epoch1,
                SinkMetadata {
                    metadata: Some(Metadata::Serialized(SerializedMetadata {
                        metadata: metadata.clone(),
                    })),
                },
                None,
            )
            .await
            .unwrap();

        {
            let rows = list_rows(&db).await;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1, epoch1 as i64);
            assert_eq!(rows[0].2, "PENDING");
            assert_eq!(rows[0].3, None);

            let guard = sender.lock().await;
            let sender = guard.as_ref().unwrap().clone();
            sender.send(epoch1).unwrap();
        }

        // wait max 5 seconds for the commit to be processed
        for _ in 0..50 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            let rows = list_rows(&db).await;
            if rows[0].2 == "COMMITTED" {
                break;
            }
        }

        {
            let rows = list_rows(&db).await;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1, epoch1 as i64);
            assert_eq!(rows[0].2, "COMMITTED");
            assert_eq!(rows[0].3, None);
        }
    }

    #[tokio::test]
    async fn test_commit_retry_loop() {
        let db = prepare_db_backend().await;

        let param = SinkParam {
            sink_id: SinkId::from(1),
            sink_name: "test".into(),
            properties: Default::default(),
            columns: vec![],
            downstream_pk: None,
            sink_type: SinkType::AppendOnly,
            ignore_delete: false,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let epoch1 = 233;

        let all_vnode = (0..VirtualNode::COUNT_FOR_TEST).collect_vec();
        let build_bitmap = |indexes: &[usize]| {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
            for i in indexes {
                builder.set(*i, true);
            }
            builder.finish()
        };
        let vnode = build_bitmap(&all_vnode);

        let metadata = vec![1u8, 2u8];
        let sender = Arc::new(tokio::sync::Mutex::new(None));
        let mock_subscriber: SinkCommittedEpochSubscriber = {
            let captured_sender = sender.clone();
            Arc::new(move |_sink_id: SinkId| {
                let (sender, receiver) = unbounded_channel();
                let captured_sender = captured_sender.clone();
                async move {
                    let mut guard = captured_sender.lock().await;
                    *guard = Some(sender);
                    Ok((epoch1, receiver))
                }
                .boxed()
            })
        };

        let commit_attempt = Arc::new(AtomicI32::new(0));

        let (manager, (_join_handle, _stop_tx)) =
            SinkCoordinatorManager::start_worker_with_spawn_worker({
                let expected_param = param.clone();
                let metadata = metadata.clone();
                let db = db.clone();
                let commit_attempt = commit_attempt.clone();
                move |param, new_writer_rx| {
                    let metadata = metadata.clone();
                    let expected_param = expected_param.clone();
                    let db = db.clone();
                    let commit_attempt = commit_attempt.clone();
                    tokio::spawn({
                        let subscriber = mock_subscriber.clone();
                        async move {
                            // validate the start request
                            assert_eq!(param, expected_param);
                            CoordinatorWorker::execute_coordinator(
                                db,
                                param.clone(),
                                new_writer_rx,
                                MockTwoPhaseCoordinator::new_coordinator(
                                    move |_epoch, metadata_list, _schema_change| {
                                        let metadata =
                                            Itertools::exactly_one(metadata_list.into_iter())
                                                .unwrap();
                                        Ok(match metadata.metadata {
                                            Some(Metadata::Serialized(SerializedMetadata {
                                                metadata,
                                            })) => Some(metadata),
                                            _ => unreachable!(),
                                        })
                                    },
                                    move |_epoch, commit_metadata| {
                                        assert_eq!(commit_metadata, metadata);
                                        if commit_attempt
                                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                                            < 2
                                        {
                                            Err(SinkError::Coordinator(anyhow!("failed to commit")))
                                        } else {
                                            Ok(())
                                        }
                                    },
                                    move |_epoch, _schema_change| unreachable!(),
                                ),
                                subscriber.clone(),
                            )
                            .await;
                        }
                    })
                }
            });

        let build_client = |vnode| async {
            CoordinatorStreamHandle::new_with_init_stream(param.to_proto(), vnode, |rx| async {
                Ok(tonic::Response::new(
                    manager
                        .handle_new_request(ReceiverStream::new(rx).map(Ok).boxed())
                        .await
                        .unwrap()
                        .boxed(),
                ))
            })
            .await
            .unwrap()
            .0
        };

        let mut client = build_client(vnode).await;

        let aligned_epoch = client.align_initial_epoch(1).await.unwrap();
        assert_eq!(aligned_epoch, 1);

        client
            .commit(
                epoch1,
                SinkMetadata {
                    metadata: Some(Metadata::Serialized(SerializedMetadata {
                        metadata: metadata.clone(),
                    })),
                },
                None,
            )
            .await
            .unwrap();

        // wait max 10 seconds for the commit to be processed
        for _ in 0..100 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            let rows = list_rows(&db).await;
            if rows[0].2 == "COMMITTED" {
                break;
            }
        }

        assert_eq!(commit_attempt.load(std::sync::atomic::Ordering::SeqCst), 3);

        {
            let rows = list_rows(&db).await;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1, epoch1 as i64);
            assert_eq!(rows[0].2, "COMMITTED");
        }
    }

    #[tokio::test]
    async fn test_aborted() {
        let db = prepare_db_backend().await;

        let param = SinkParam {
            sink_id: SinkId::from(1),
            sink_name: "test".into(),
            properties: Default::default(),
            columns: vec![],
            downstream_pk: None,
            sink_type: SinkType::AppendOnly,
            ignore_delete: false,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let epoch0 = 232;
        let epoch1 = 233;

        let all_vnode = (0..VirtualNode::COUNT_FOR_TEST).collect_vec();
        let build_bitmap = |indexes: &[usize]| {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
            for i in indexes {
                builder.set(*i, true);
            }
            builder.finish()
        };
        let vnode = build_bitmap(&all_vnode);

        let metadata = vec![1u8, 2u8];

        let sender = Arc::new(tokio::sync::Mutex::new(None));
        let mock_subscriber: SinkCommittedEpochSubscriber = {
            let captured_sender = sender.clone();
            Arc::new(move |_sink_id: SinkId| {
                let (sender, receiver) = unbounded_channel();
                let captured_sender = captured_sender.clone();
                async move {
                    let mut guard = captured_sender.lock().await;
                    *guard = Some(sender);
                    Ok((epoch0, receiver))
                }
                .boxed()
            })
        };

        let (manager, (_join_handle, _stop_tx)) =
            SinkCoordinatorManager::start_worker_with_spawn_worker({
                let expected_param = param.clone();
                let metadata = metadata.clone();
                let db = db.clone();
                move |param, new_writer_rx| {
                    let metadata = metadata.clone();
                    let expected_param = expected_param.clone();
                    let db = db.clone();
                    tokio::spawn({
                        let subscriber = mock_subscriber.clone();
                        async move {
                            // validate the start request
                            assert_eq!(param, expected_param);
                            CoordinatorWorker::execute_coordinator(
                                db,
                                param.clone(),
                                new_writer_rx,
                                MockTwoPhaseCoordinator::new_coordinator(
                                    move |_epoch, metadata_list, _schema_change| {
                                        let metadata =
                                            Itertools::exactly_one(metadata_list.into_iter())
                                                .unwrap();
                                        Ok(match metadata.metadata {
                                            Some(Metadata::Serialized(SerializedMetadata {
                                                metadata,
                                            })) => Some(metadata),
                                            _ => unreachable!(),
                                        })
                                    },
                                    move |_epoch, commit_metadata| {
                                        assert_eq!(commit_metadata, metadata);
                                        Ok(())
                                    },
                                    move |_epoch, _schema_change| unreachable!(),
                                ),
                                subscriber.clone(),
                            )
                            .await;
                        }
                    })
                }
            });

        let build_client = |vnode| async {
            CoordinatorStreamHandle::new_with_init_stream(param.to_proto(), vnode, |rx| async {
                Ok(tonic::Response::new(
                    manager
                        .handle_new_request(ReceiverStream::new(rx).map(Ok).boxed())
                        .await
                        .unwrap()
                        .boxed(),
                ))
            })
            .await
            .unwrap()
            .0
        };

        let mut client = build_client(vnode.clone()).await;

        let aligned_epoch = client.align_initial_epoch(1).await.unwrap();
        assert_eq!(aligned_epoch, 1);

        client
            .commit(
                epoch1,
                SinkMetadata {
                    metadata: Some(Metadata::Serialized(SerializedMetadata {
                        metadata: metadata.clone(),
                    })),
                },
                None,
            )
            .await
            .unwrap();

        manager.stop_sink_coordinator(vec![SinkId::from(1)]).await;

        {
            let rows = list_rows(&db).await;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1, epoch1 as i64);
            assert_eq!(rows[0].2, "PENDING");

            set_epoch_aborted(&db, SinkId::from(1), epoch1).await;
            let rows = list_rows(&db).await;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1, epoch1 as i64);
            assert_eq!(rows[0].2, "ABORTED");
        }

        let mut client = build_client(vnode).await;

        let aligned_epoch = client.align_initial_epoch(1).await.unwrap();
        assert_eq!(aligned_epoch, 1);

        {
            let rows = list_rows(&db).await;
            assert!(rows.is_empty());
        }
    }

    #[tokio::test]
    async fn test_flush_when_reschedule() {
        let db = prepare_db_backend().await;

        let param = SinkParam {
            sink_id: SinkId::from(1),
            sink_name: "test".into(),
            properties: Default::default(),
            columns: vec![],
            downstream_pk: None,
            sink_type: SinkType::AppendOnly,
            ignore_delete: false,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let epoch0 = 232;
        let epoch1 = 233;

        let all_vnode = (0..VirtualNode::COUNT_FOR_TEST).collect_vec();
        let build_bitmap = |indexes: &[usize]| {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
            for i in indexes {
                builder.set(*i, true);
            }
            builder.finish()
        };
        let vnode = build_bitmap(&all_vnode);

        let metadata = vec![1u8, 2u8];
        let schema_change = PbSinkSchemaChange {
            original_schema: vec![PbField {
                data_type: Some(PbDataType {
                    type_name: PbTypeName::Int32 as i32,
                    ..Default::default()
                }),
                name: "col_v1".into(),
            }],
            op: Some(SinkSchemachangeOp::AddColumns(PbSinkAddColumnsOp {
                fields: vec![PbField {
                    data_type: Some(PbDataType {
                        type_name: PbTypeName::Varchar as i32,
                        ..Default::default()
                    }),
                    name: "new_col".into(),
                }],
            })),
        };

        let sender = Arc::new(tokio::sync::Mutex::new(None));
        let mock_subscriber: SinkCommittedEpochSubscriber = {
            let captured_sender = sender.clone();
            Arc::new(move |_sink_id: SinkId| {
                let (sender, receiver) = unbounded_channel();
                let captured_sender = captured_sender.clone();
                async move {
                    let mut guard = captured_sender.lock().await;
                    *guard = Some(sender);
                    Ok((epoch0, receiver))
                }
                .boxed()
            })
        };

        let (manager, (_join_handle, _stop_tx)) =
            SinkCoordinatorManager::start_worker_with_spawn_worker({
                let expected_param = param.clone();
                let metadata = metadata.clone();
                let schema_change = schema_change.clone();
                let db = db.clone();
                move |param, new_writer_rx| {
                    let metadata = metadata.clone();
                    let schema_change_for_pre_commit = schema_change.clone();
                    let schema_change_for_commit = schema_change.clone();
                    let expected_param = expected_param.clone();
                    let db = db.clone();
                    tokio::spawn({
                        let subscriber = mock_subscriber.clone();
                        async move {
                            assert_eq!(param, expected_param);
                            CoordinatorWorker::execute_coordinator(
                                db,
                                param.clone(),
                                new_writer_rx,
                                MockTwoPhaseCoordinator::new_coordinator(
                                    move |_epoch, metadata_list, schema_change| {
                                        assert_eq!(
                                            schema_change,
                                            Some(schema_change_for_pre_commit.clone())
                                        );
                                        let metadata =
                                            Itertools::exactly_one(metadata_list.into_iter())
                                                .unwrap();
                                        Ok(match metadata.metadata {
                                            Some(Metadata::Serialized(SerializedMetadata {
                                                metadata,
                                            })) => Some(metadata),
                                            _ => unreachable!(),
                                        })
                                    },
                                    move |_epoch, commit_metadata| {
                                        assert_eq!(commit_metadata, metadata);
                                        Ok(())
                                    },
                                    move |_epoch, schema_change| {
                                        assert_eq!(schema_change, schema_change_for_commit.clone());
                                        Ok(())
                                    },
                                ),
                                subscriber.clone(),
                            )
                            .await;
                        }
                    })
                }
            });

        let build_client = |vnode| async {
            CoordinatorStreamHandle::new_with_init_stream(param.to_proto(), vnode, |rx| async {
                Ok(tonic::Response::new(
                    manager
                        .handle_new_request(ReceiverStream::new(rx).map(Ok).boxed())
                        .await
                        .unwrap()
                        .boxed(),
                ))
            })
            .await
        };

        let (mut client1, _) = build_client(vnode.clone()).await.unwrap();

        let aligned_epoch = client1.align_initial_epoch(1).await.unwrap();
        assert_eq!(aligned_epoch, 1);

        client1
            .commit(
                epoch1,
                SinkMetadata {
                    metadata: Some(Metadata::Serialized(SerializedMetadata {
                        metadata: metadata.clone(),
                    })),
                },
                Some(schema_change.clone()),
            )
            .await
            .unwrap();

        {
            let rows = list_rows(&db).await;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1, epoch1 as i64);
            assert_eq!(rows[0].2, "PENDING");
            assert_eq!(rows[0].4, Some(schema_change.clone()));
        }

        let mut build_client2_future = pin!(build_client(vnode.clone()));
        assert!(
            poll_fn(|cx| Poll::Ready(build_client2_future.as_mut().poll(cx)))
                .await
                .is_pending()
        );

        client1.stop().await.unwrap();

        assert!(
            poll_fn(|cx| Poll::Ready(build_client2_future.as_mut().poll(cx)))
                .await
                .is_pending()
        );

        {
            let guard = sender.lock().await;
            let sender = guard.as_ref().unwrap().clone();
            sender.send(epoch1).unwrap();
        }

        let (_, init_epoch) = build_client2_future.await.unwrap();
        assert_eq!(init_epoch, Some(epoch1));

        {
            let rows = list_rows(&db).await;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1, epoch1 as i64);
            assert_eq!(rows[0].2, "COMMITTED");
            assert_eq!(rows[0].4, Some(schema_change.clone()));
        }
    }
}
