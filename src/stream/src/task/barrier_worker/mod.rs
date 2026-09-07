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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::future::{pending, poll_fn};
use std::sync::Arc;
use std::task::Poll;

use anyhow::anyhow;
use await_tree::{InstrumentAwait, SpanExt};
use futures::future::{BoxFuture, join, join_all};
use futures::stream::{BoxStream, FuturesOrdered};
use futures::{FutureExt, StreamExt, TryFutureExt};
use itertools::Itertools;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_service::barrier_complete_response::{
    PbCdcSourceOffsetUpdated, PbCdcTableBackfillProgress, PbCreateMviewProgress,
    PbIcebergPkIndexSinkMetadata, PbListFinishedSource, PbLoadFinishedSource, PbLocalSstableInfo,
};
use risingwave_rpc_client::error::{ToTonicStatus, TonicStatusWrapper};
use risingwave_storage::store_impl::AsHummock;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::{select, spawn};
use tonic::{Code, Status};
use tracing::warn;

use self::managed_state::ManagedBarrierState;
use crate::error::{ScoredStreamError, StreamError, StreamResult};
#[cfg(test)]
use crate::task::LocalBarrierManager;
use crate::task::managed_state::{BarrierToComplete, ResetPartialGraphOutput};
use crate::task::{
    ActorId, AtomicU64Ref, CONFIG_OVERRIDE_CACHE_DEFAULT_CAPACITY, ConfigOverrideCache, FragmentId,
    PartialGraphId, StreamActorManager, StreamEnvironment, UpDownActorIds,
};
pub mod managed_state;
#[cfg(test)]
mod tests;

use risingwave_common::metrics::LabelGuardedHistogram;
use risingwave_hummock_sdk::table_stats::to_prost_table_stats_map;
use risingwave_hummock_sdk::{LocalSstableInfo, SyncResult};
use risingwave_pb::stream_service::streaming_control_stream_request::{
    InitRequest, Request, ResetPartialGraphsRequest,
};
use risingwave_pb::stream_service::streaming_control_stream_response::{
    InitResponse, ReportPartialGraphFailureResponse, ResetPartialGraphResponse, Response,
    ShutdownResponse,
};
use risingwave_pb::stream_service::{
    BarrierCompleteResponse, InjectBarrierRequest, PbScoredError, StreamingControlStreamRequest,
    StreamingControlStreamResponse, streaming_control_stream_response,
};

use crate::executor::Barrier;
use crate::executor::exchange::permit::Receiver;
use crate::executor::monitor::StreamingMetrics;
use crate::task::barrier_worker::managed_state::{
    ManagedBarrierStateDebugInfo, ManagedBarrierStateEvent, PartialGraphState, PartialGraphStatus,
};

/// If enabled, all actors will be grouped in the same tracing span within one epoch.
/// Note that this option will significantly increase the overhead of tracing.
pub const ENABLE_BARRIER_AGGREGATION: bool = false;

/// Collect result of some barrier on current compute node. Will be reported to the meta service in [`LocalBarrierWorker::on_epoch_completed`].
#[derive(Debug)]
pub struct BarrierCompleteResult {
    /// The result returned from `sync` of `StateStore`.
    pub sync_result: Option<SyncResult>,

    /// The updated creation progress of materialized view after this barrier.
    pub create_mview_progress: Vec<PbCreateMviewProgress>,

    /// The source IDs that have finished listing data for refreshable batch sources.
    pub list_finished_source_ids: Vec<PbListFinishedSource>,

    /// The source IDs that have finished loading data for refreshable batch sources.
    pub load_finished_source_ids: Vec<PbLoadFinishedSource>,

    pub cdc_table_backfill_progress: Vec<PbCdcTableBackfillProgress>,

    /// CDC sources that have updated their offset at least once.
    pub cdc_source_offset_updated: Vec<PbCdcSourceOffsetUpdated>,

    /// Iceberg pk-index sink metadata reports collected during this barrier.
    pub iceberg_pk_index_sink_metadata: Vec<PbIcebergPkIndexSinkMetadata>,

    /// The table IDs that should be truncated.
    pub truncate_tables: Vec<TableId>,
    /// The table IDs that have finished refresh.
    pub refresh_finished_tables: Vec<TableId>,
}

/// Lives in [`crate::task::barrier_worker::LocalBarrierWorker`],
/// Communicates with `ControlStreamManager` in meta.
/// Handles [`risingwave_pb::stream_service::streaming_control_stream_request::Request`].
pub(super) struct ControlStreamHandle {
    #[expect(clippy::type_complexity)]
    pair: Option<(
        UnboundedSender<Result<StreamingControlStreamResponse, Status>>,
        BoxStream<'static, Result<StreamingControlStreamRequest, Status>>,
    )>,
}

impl ControlStreamHandle {
    fn empty() -> Self {
        Self { pair: None }
    }

    pub(super) fn new(
        sender: UnboundedSender<Result<StreamingControlStreamResponse, Status>>,
        request_stream: BoxStream<'static, Result<StreamingControlStreamRequest, Status>>,
    ) -> Self {
        Self {
            pair: Some((sender, request_stream)),
        }
    }

    pub(super) fn connected(&self) -> bool {
        self.pair.is_some()
    }

    fn reset_stream_with_err(&mut self, err: Status) {
        if let Some((sender, _)) = self.pair.take() {
            // Note: `TonicStatusWrapper` provides a better error report.
            let err = TonicStatusWrapper::new(err);
            warn!(error = %err.as_report(), "control stream reset with error");

            let err = err.into_inner();
            if sender.send(Err(err)).is_err() {
                warn!("failed to notify reset of control stream");
            }
        }
    }

    /// Send `Shutdown` message to the control stream and wait for the stream to be closed
    /// by the meta service.
    async fn shutdown_stream(&mut self) {
        if let Some((sender, _)) = self.pair.take() {
            if sender
                .send(Ok(StreamingControlStreamResponse {
                    response: Some(streaming_control_stream_response::Response::Shutdown(
                        ShutdownResponse::default(),
                    )),
                }))
                .is_err()
            {
                warn!("failed to notify shutdown of control stream");
            } else {
                tracing::info!("waiting for meta service to close control stream...");

                // Wait for the stream to be closed, to ensure that the `Shutdown` message has
                // been acknowledged by the meta service for more precise error report.
                //
                // This is because the meta service will reset the control stream manager and
                // drop the connection to us upon recovery. As a result, the receiver part of
                // this sender will also be dropped, causing the stream to close.
                sender.closed().await;
            }
        } else {
            debug!("control stream has been reset, ignore shutdown");
        }
    }

    pub(super) fn ack_reset_partial_graph(
        &mut self,
        partial_graph_id: PartialGraphId,
        root_err: Option<ScoredStreamError>,
    ) {
        self.send_response(Response::ResetPartialGraph(ResetPartialGraphResponse {
            partial_graph_id,
            root_err: root_err.map(|err| PbScoredError {
                err_msg: err.error.to_report_string(),
                score: err.score.0,
            }),
        }));
    }

    fn send_response(&mut self, response: streaming_control_stream_response::Response) {
        if let Some((sender, _)) = self.pair.as_ref() {
            if sender
                .send(Ok(StreamingControlStreamResponse {
                    response: Some(response),
                }))
                .is_err()
            {
                self.pair = None;
                warn!("failed to send the response; the control stream was reset");
            }
        } else {
            debug!(?response, "control stream has been reset. ignore response");
        }
    }

    async fn next_request(&mut self) -> StreamingControlStreamRequest {
        if let Some((_, stream)) = &mut self.pair {
            match stream.next().await {
                Some(Ok(request)) => {
                    return request;
                }
                Some(Err(e)) => self.reset_stream_with_err(
                    anyhow!(TonicStatusWrapper::new(e)) // wrap the status to provide better error report
                        .context("failed to get request")
                        .to_status_unnamed(Code::Internal),
                ),
                None => self.reset_stream_with_err(Status::internal("end of stream")),
            }
        }
        pending().await
    }
}

pub(super) enum TakeReceiverRequest {
    Remote {
        result_sender: oneshot::Sender<StreamResult<Receiver>>,
        upstream_fragment_id: FragmentId,
    },
    Local(permit::Sender),
}

/// Sent from [`crate::task::stream_manager::LocalStreamManager`] to [`crate::task::barrier_worker::LocalBarrierWorker::run`].
///
/// See [`crate::task`] for architecture overview.
#[derive(strum_macros::Display)]
pub(super) enum LocalActorOperation {
    NewControlStream {
        handle: ControlStreamHandle,
        init_request: InitRequest,
    },
    TakeReceiver {
        partial_graph_id: PartialGraphId,
        term_id: String,
        ids: UpDownActorIds,
        request: TakeReceiverRequest,
    },
    #[cfg(test)]
    GetCurrentLocalBarrierManager(oneshot::Sender<LocalBarrierManager>),
    #[cfg(test)]
    TakePendingNewOutputRequest(ActorId, oneshot::Sender<Vec<(ActorId, NewOutputRequest)>>),
    #[cfg(test)]
    Flush(oneshot::Sender<()>),
    InspectState {
        result_sender: oneshot::Sender<String>,
    },
    Shutdown {
        result_sender: oneshot::Sender<()>,
    },
}

pub(super) struct LocalBarrierWorkerDebugInfo<'a> {
    managed_barrier_state:
        HashMap<PartialGraphId, (String, Option<ManagedBarrierStateDebugInfo<'a>>)>,
    has_control_stream_connected: bool,
}

impl Display for LocalBarrierWorkerDebugInfo<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "\nhas_control_stream_connected: {}",
            self.has_control_stream_connected
        )?;

        for (partial_graph_id, (status, managed_barrier_state)) in &self.managed_barrier_state {
            writeln!(
                f,
                "partial graph {} status: {} managed_barrier_state:\n{}",
                partial_graph_id,
                status,
                managed_barrier_state
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default()
            )?;
        }
        Ok(())
    }
}

/// [`LocalBarrierWorker`] manages barrier control flow.
/// Specifically, [`LocalBarrierWorker`] serves barrier injection from meta server, sends the
/// barriers to and collects them from all actors, and finally reports the progress.
///
/// Runs event loop in [`Self::run`]. Handles events sent by [`crate::task::LocalStreamManager`].
///
/// See [`crate::task`] for architecture overview.
pub(super) struct LocalBarrierWorker {
    /// Current barrier collection state.
    pub(super) state: ManagedBarrierState,

    /// Futures will be finished in the order of epoch in ascending order.
    await_epoch_completed_futures:
        HashMap<PartialGraphId, FuturesOrdered<AwaitEpochCompletedFuture>>,

    control_stream_handle: ControlStreamHandle,

    pub(super) actor_manager: Arc<StreamActorManager>,
}

impl LocalBarrierWorker {
    pub(super) fn new(actor_manager: Arc<StreamActorManager>) -> Self {
        Self {
            state: Default::default(),
            await_epoch_completed_futures: Default::default(),
            control_stream_handle: ControlStreamHandle::empty(),
            actor_manager,
        }
    }

    fn to_debug_info(&self) -> LocalBarrierWorkerDebugInfo<'_> {
        LocalBarrierWorkerDebugInfo {
            managed_barrier_state: self
                .state
                .partial_graphs
                .iter()
                .map(|(partial_graph_id, status)| {
                    (*partial_graph_id, {
                        match status {
                            PartialGraphStatus::ReceivedExchangeRequest(_) => {
                                ("ReceivedExchangeRequest".to_owned(), None)
                            }
                            PartialGraphStatus::Running(state) => {
                                ("running".to_owned(), Some(state.to_debug_info()))
                            }
                            PartialGraphStatus::Suspended(state) => {
                                (format!("suspended: {:?}", state.suspend_time), None)
                            }
                            PartialGraphStatus::Resetting => ("resetting".to_owned(), None),
                            PartialGraphStatus::Unspecified => {
                                unreachable!()
                            }
                        }
                    })
                })
                .collect(),
            has_control_stream_connected: self.control_stream_handle.connected(),
        }
    }

    async fn next_completed_epoch(
        futures: &mut HashMap<PartialGraphId, FuturesOrdered<AwaitEpochCompletedFuture>>,
    ) -> (PartialGraphId, Barrier, StreamResult<BarrierCompleteResult>) {
        poll_fn(|cx| {
            for (partial_graph_id, futures) in &mut *futures {
                if let Poll::Ready(Some((barrier, result))) = futures.poll_next_unpin(cx) {
                    return Poll::Ready((*partial_graph_id, barrier, result));
                }
            }
            Poll::Pending
        })
        .await
    }

    async fn run(mut self, mut actor_op_rx: UnboundedReceiver<LocalActorOperation>) {
        loop {
            select! {
                biased;
                event = self.state.next_event() => {
                    match event {
                        ManagedBarrierStateEvent::BarrierCollected{
                            partial_graph_id,
                            barrier,
                        } => {
                            // update await_epoch_completed_futures
                            // handled below in next_completed_epoch
                            self.complete_barrier(partial_graph_id, barrier.epoch.prev);
                        }
                        ManagedBarrierStateEvent::ActorError{
                            partial_graph_id,
                            actor_id,
                            err,
                        } => {
                            self.on_partial_graph_failure(partial_graph_id, Some(actor_id), err, "recv actor failure");
                        }
                        ManagedBarrierStateEvent::PartialGraphsReset(output) => {
                            for (partial_graph_id, output) in output {
                                self.ack_partial_graph_reset(partial_graph_id, Some(output));
                            }
                        }
                        ManagedBarrierStateEvent::RegisterLocalUpstreamOutput{
                            actor_id,
                            upstream_actor_id,
                            upstream_partial_graph_id,
                            term_id,
                            tx
                        } => {
                            self.handle_actor_op(LocalActorOperation::TakeReceiver {
                                partial_graph_id: upstream_partial_graph_id,
                                term_id,
                                ids: (upstream_actor_id, actor_id),
                                request: TakeReceiverRequest::Local(tx),
                            });
                        }
                    }
                }
                (partial_graph_id, barrier, result) = Self::next_completed_epoch(&mut self.await_epoch_completed_futures) => {
                    match result {
                        Ok(result) => {
                            self.on_epoch_completed(partial_graph_id, barrier.epoch.prev, result);
                        }
                        Err(err) => {
                            // TODO: may only report as partial graph failure instead of reset the stream
                            // when the HummockUploader support partial recovery. Currently the HummockUploader
                            // enter `Err` state and stop working until a global recovery to clear the uploader.
                            self.control_stream_handle.reset_stream_with_err(Status::internal(format!("failed to complete epoch: {} {:?} {:?}", partial_graph_id, barrier.epoch, err.as_report())));
                        }
                    }
                },
                actor_op = actor_op_rx.recv() => {
                    if let Some(actor_op) = actor_op {
                        match actor_op {
                            LocalActorOperation::NewControlStream { handle, init_request  } => {
                                self.control_stream_handle.reset_stream_with_err(Status::internal("control stream has been reset to a new one"));
                                self.reset(init_request).await;
                                self.control_stream_handle = handle;
                                self.control_stream_handle.send_response(streaming_control_stream_response::Response::Init(InitResponse {}));
                            }
                            LocalActorOperation::Shutdown { result_sender } => {
                                if self.state.partial_graphs.values().any(|graph| {
                                    match graph {
                                        PartialGraphStatus::Running(graph) => {
                                            !graph.actor_states.is_empty()
                                        }
                                        PartialGraphStatus::Suspended(_) | PartialGraphStatus::Resetting |
                                            PartialGraphStatus::ReceivedExchangeRequest(_) => {
                                            false
                                        }
                                        PartialGraphStatus::Unspecified => {
                                            unreachable!()
                                        }
                                    }
                                }) {
                                    tracing::warn!(
                                        "shutdown with running actors, scaling or migration will be triggered"
                                    );
                                }
                                self.control_stream_handle.shutdown_stream().await;
                                let _ = result_sender.send(());
                            }
                            actor_op => {
                                self.handle_actor_op(actor_op);
                            }
                        }
                    }
                    else {
                        break;
                    }
                },
                request = self.control_stream_handle.next_request() => {
                    let result = self.handle_streaming_control_request(request.request.expect("non empty"));
                    if let Err((partial_graph_id, err)) = result {
                        self.on_partial_graph_failure(partial_graph_id, None, err, "failed to inject barrier");
                    }
                },
            }
        }
    }

    fn handle_streaming_control_request(
        &mut self,
        request: Request,
    ) -> Result<(), (PartialGraphId, StreamError)> {
        match request {
            Request::InjectBarrier(req) => {
                let partial_graph_id = req.partial_graph_id;
                let result: StreamResult<()> = try {
                    let barrier = Barrier::from_protobuf(req.get_barrier().unwrap())
                        .map_err(StreamError::from)?;
                    self.send_barrier(&barrier, req)?;
                };
                result.map_err(|e| (partial_graph_id, e))?;
                Ok(())
            }
            Request::RemovePartialGraph(req) => {
                self.remove_partial_graphs(req.partial_graph_ids);
                Ok(())
            }
            Request::CreatePartialGraph(req) => {
                self.add_partial_graph(req.partial_graph_id, req.term_id);
                Ok(())
            }
            Request::ResetPartialGraphs(req) => {
                self.reset_partial_graphs(req);
                Ok(())
            }
            Request::Init(_) => {
                unreachable!()
            }
        }
    }

    fn handle_actor_op(&mut self, actor_op: LocalActorOperation) {
        match actor_op {
            LocalActorOperation::NewControlStream { .. } | LocalActorOperation::Shutdown { .. } => {
                unreachable!("event {actor_op} should be handled separately in async context")
            }
            LocalActorOperation::TakeReceiver {
                partial_graph_id,
                term_id,
                ids,
                request,
            } => {
                let err = match self.state.partial_graphs.entry(partial_graph_id) {
                    Entry::Occupied(mut entry) => match entry.get_mut() {
                        PartialGraphStatus::ReceivedExchangeRequest(pending_requests) => {
                            pending_requests.push((term_id, ids, request));
                            return;
                        }
                        PartialGraphStatus::Running(graph) => {
                            if graph.local_barrier_manager.term_id != term_id {
                                warn!(
                                    %partial_graph_id,
                                    ?ids,
                                    request_term_id = term_id,
                                    current_term_id = graph.local_barrier_manager.term_id,
                                    "take receiver on unmatched partial graph term"
                                );
                                anyhow!(
                                    "take receiver {:?} on unmatched partial graph term {} to current term {}",
                                    ids,
                                    term_id,
                                    graph.local_barrier_manager.term_id
                                )
                            } else {
                                let (upstream_actor_id, actor_id) = ids;
                                graph.new_actor_output_request(
                                    actor_id,
                                    upstream_actor_id,
                                    request,
                                );
                                return;
                            }
                        }
                        PartialGraphStatus::Suspended(_) => anyhow!("partial graph suspended"),
                        PartialGraphStatus::Resetting => anyhow!("partial graph resetting"),
                        PartialGraphStatus::Unspecified => unreachable!(),
                    },
                    Entry::Vacant(entry) => {
                        entry.insert(PartialGraphStatus::ReceivedExchangeRequest(vec![(
                            term_id, ids, request,
                        )]));
                        return;
                    }
                };
                if let TakeReceiverRequest::Remote { result_sender, .. } = request {
                    let _ = result_sender.send(Err(err.into()));
                }
            }
            #[cfg(test)]
            LocalActorOperation::GetCurrentLocalBarrierManager(sender) => {
                let partial_graph_status = self
                    .state
                    .partial_graphs
                    .get(&crate::task::TEST_PARTIAL_GRAPH_ID)
                    .unwrap();
                let partial_graph_state = risingwave_common::must_match!(partial_graph_status, PartialGraphStatus::Running(database_state) => database_state);
                let _ = sender.send(partial_graph_state.local_barrier_manager.clone());
            }
            #[cfg(test)]
            LocalActorOperation::TakePendingNewOutputRequest(actor_id, sender) => {
                let partial_graph_status = self
                    .state
                    .partial_graphs
                    .get_mut(&crate::task::TEST_PARTIAL_GRAPH_ID)
                    .unwrap();

                let partial_graph_state = risingwave_common::must_match!(partial_graph_status, PartialGraphStatus::Running(database_state) => database_state);
                assert!(!partial_graph_state.actor_states.contains_key(&actor_id));
                let requests = partial_graph_state
                    .actor_pending_new_output_requests
                    .remove(&actor_id)
                    .unwrap();
                let _ = sender.send(requests);
            }
            #[cfg(test)]
            LocalActorOperation::Flush(sender) => {
                use futures::FutureExt;
                while let Some(request) = self.control_stream_handle.next_request().now_or_never() {
                    self.handle_streaming_control_request(
                        request.request.expect("should not be empty"),
                    )
                    .unwrap();
                }
                while let Some(event) = self.state.next_event().now_or_never() {
                    match event {
                        ManagedBarrierStateEvent::BarrierCollected {
                            barrier,
                            partial_graph_id,
                        } => {
                            self.complete_barrier(partial_graph_id, barrier.epoch.prev);
                        }
                        ManagedBarrierStateEvent::ActorError { .. }
                        | ManagedBarrierStateEvent::PartialGraphsReset { .. }
                        | ManagedBarrierStateEvent::RegisterLocalUpstreamOutput { .. } => {
                            unreachable!()
                        }
                    }
                }
                sender.send(()).unwrap()
            }
            LocalActorOperation::InspectState { result_sender } => {
                let debug_info = self.to_debug_info();
                let _ = result_sender.send(debug_info.to_string());
            }
        }
    }
}

mod await_epoch_completed_future {
    use std::future::Future;

    use futures::FutureExt;
    use futures::future::BoxFuture;
    use risingwave_common::id::TableId;
    use risingwave_hummock_sdk::SyncResult;
    use risingwave_pb::stream_service::barrier_complete_response::{
        PbCdcSourceOffsetUpdated, PbCdcTableBackfillProgress, PbCreateMviewProgress,
        PbIcebergPkIndexSinkMetadata, PbListFinishedSource, PbLoadFinishedSource,
    };

    use crate::error::StreamResult;
    use crate::executor::Barrier;
    use crate::task::{BarrierCompleteResult, await_tree_key};

    pub(super) type AwaitEpochCompletedFuture =
        impl Future<Output = (Barrier, StreamResult<BarrierCompleteResult>)> + 'static;

    #[define_opaque(AwaitEpochCompletedFuture)]
    #[expect(clippy::too_many_arguments)]
    pub(super) fn instrument_complete_barrier_future(
        complete_barrier_future: Option<BoxFuture<'static, StreamResult<SyncResult>>>,
        barrier: Barrier,
        barrier_await_tree_reg: Option<&await_tree::Registry>,
        create_mview_progress: Vec<PbCreateMviewProgress>,
        list_finished_source_ids: Vec<PbListFinishedSource>,
        load_finished_source_ids: Vec<PbLoadFinishedSource>,
        cdc_table_backfill_progress: Vec<PbCdcTableBackfillProgress>,
        cdc_source_offset_updated: Vec<PbCdcSourceOffsetUpdated>,
        iceberg_pk_index_sink_metadata: Vec<PbIcebergPkIndexSinkMetadata>,
        truncate_tables: Vec<TableId>,
        refresh_finished_tables: Vec<TableId>,
    ) -> AwaitEpochCompletedFuture {
        let prev_epoch = barrier.epoch.prev;
        let future = async move {
            if let Some(future) = complete_barrier_future {
                let result = future.await;
                result.map(Some)
            } else {
                Ok(None)
            }
        }
        .map(move |result| {
            (
                barrier,
                result.map(|sync_result| BarrierCompleteResult {
                    sync_result,
                    create_mview_progress,
                    list_finished_source_ids,
                    load_finished_source_ids,
                    cdc_table_backfill_progress,
                    cdc_source_offset_updated,
                    iceberg_pk_index_sink_metadata,
                    truncate_tables,
                    refresh_finished_tables,
                }),
            )
        });
        if let Some(reg) = barrier_await_tree_reg {
            reg.register(
                await_tree_key::BarrierAwait { prev_epoch },
                format!("SyncEpoch({})", prev_epoch),
            )
            .instrument(future)
            .left_future()
        } else {
            future.right_future()
        }
    }
}

use await_epoch_completed_future::*;
use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::vector_index_delta::PbVectorIndexAdds;
use risingwave_storage::{StateStoreImpl, dispatch_state_store};

use crate::executor::exchange::permit;

fn sync_epoch(
    state_store: &StateStoreImpl,
    barrier_sync_latency: LabelGuardedHistogram,
    prev_epoch: u64,
    table_ids: HashSet<TableId>,
) -> BoxFuture<'static, StreamResult<SyncResult>> {
    let timer = barrier_sync_latency.start_timer();

    let state_store = state_store.clone();
    let future = async move {
        dispatch_state_store!(state_store, hummock, {
            hummock.sync(vec![(prev_epoch, table_ids)]).await
        })
    };

    future
        .instrument_await(await_tree::span!("sync_epoch (epoch {})", prev_epoch))
        .inspect_ok(move |_| {
            let _guard = &barrier_sync_latency;
            timer.observe_duration();
        })
        .map_err(move |e| {
            tracing::error!(
                prev_epoch,
                error = %e.as_report(),
                "Failed to sync state store",
            );
            e.into()
        })
        .boxed()
}

impl LocalBarrierWorker {
    fn complete_barrier(&mut self, partial_graph_id: PartialGraphId, prev_epoch: u64) {
        {
            let Some(graph_state) = self
                .state
                .partial_graphs
                .get_mut(&partial_graph_id)
                .expect("should exist")
                .state_for_request()
            else {
                return;
            };
            let BarrierToComplete {
                barrier,
                table_ids,
                create_mview_progress,
                list_finished_source_ids,
                load_finished_source_ids,
                cdc_table_backfill_progress,
                cdc_source_offset_updated,
                iceberg_pk_index_sink_metadata,
                truncate_tables,
                refresh_finished_tables,
            } = graph_state.pop_barrier_to_complete(prev_epoch);

            let complete_barrier_future = match &barrier.kind {
                BarrierKind::Unspecified => unreachable!(),
                BarrierKind::Initial => {
                    tracing::info!(
                        epoch = prev_epoch,
                        "ignore sealing data for the first barrier"
                    );
                    tracing::info!(?prev_epoch, "ignored syncing data for the first barrier");
                    None
                }
                BarrierKind::Barrier => None,
                BarrierKind::Checkpoint => Some(sync_epoch(
                    &self.actor_manager.env.state_store(),
                    graph_state.graph_state.barrier_sync_latency(),
                    prev_epoch,
                    table_ids.expect("should be Some on BarrierKind::Checkpoint"),
                )),
            };

            self.await_epoch_completed_futures
                .entry(partial_graph_id)
                .or_default()
                .push_back({
                    instrument_complete_barrier_future(
                        complete_barrier_future,
                        barrier,
                        self.actor_manager.await_tree_reg.as_ref(),
                        create_mview_progress,
                        list_finished_source_ids,
                        load_finished_source_ids,
                        cdc_table_backfill_progress,
                        cdc_source_offset_updated,
                        iceberg_pk_index_sink_metadata,
                        truncate_tables,
                        refresh_finished_tables,
                    )
                });
        }
    }

    fn on_epoch_completed(
        &mut self,
        partial_graph_id: PartialGraphId,
        epoch: u64,
        result: BarrierCompleteResult,
    ) {
        let BarrierCompleteResult {
            create_mview_progress,
            sync_result,
            list_finished_source_ids,
            load_finished_source_ids,
            cdc_table_backfill_progress,
            cdc_source_offset_updated,
            iceberg_pk_index_sink_metadata,
            truncate_tables,
            refresh_finished_tables,
        } = result;

        let (synced_sstables, table_watermarks, old_value_ssts, vector_index_adds) = sync_result
            .map(|sync_result| {
                (
                    sync_result.uncommitted_ssts,
                    sync_result.table_watermarks,
                    sync_result.old_value_ssts,
                    sync_result.vector_index_adds,
                )
            })
            .unwrap_or_default();

        let result = {
            {
                streaming_control_stream_response::Response::CompleteBarrier(
                    BarrierCompleteResponse {
                        request_id: "todo".to_owned(),
                        partial_graph_id,
                        epoch,
                        status: None,
                        create_mview_progress,
                        synced_sstables: synced_sstables
                            .into_iter()
                            .map(
                                |LocalSstableInfo {
                                     sst_info,
                                     table_stats,
                                     created_at,
                                 }| PbLocalSstableInfo {
                                    sst: Some(sst_info.into()),
                                    table_stats_map: to_prost_table_stats_map(table_stats),
                                    created_at,
                                },
                            )
                            .collect_vec(),
                        worker_id: self.actor_manager.env.worker_id(),
                        table_watermarks: table_watermarks
                            .into_iter()
                            .map(|(key, value)| (key, value.into()))
                            .collect(),
                        old_value_sstables: old_value_ssts
                            .into_iter()
                            .map(|sst| sst.sst_info.into())
                            .collect(),
                        list_finished_sources: list_finished_source_ids,
                        load_finished_sources: load_finished_source_ids,
                        cdc_source_offset_updated,
                        vector_index_adds: vector_index_adds
                            .into_iter()
                            .map(|(table_id, adds)| {
                                (
                                    table_id,
                                    PbVectorIndexAdds {
                                        adds: adds.into_iter().map(|add| add.into()).collect(),
                                    },
                                )
                            })
                            .collect(),
                        cdc_table_backfill_progress,
                        truncate_tables,
                        refresh_finished_tables,
                        iceberg_pk_index_sink_metadata,
                    },
                )
            }
        };

        self.control_stream_handle.send_response(result);
    }

    /// Broadcast a barrier to all senders. Save a receiver which will get notified when this
    /// barrier is finished, in managed mode.
    ///
    /// Note that the error returned here is typically a [`StreamError::barrier_send`], which is not
    /// the root cause of the failure. The caller should then call `try_find_root_failure`
    /// to find the root cause.
    fn send_barrier(
        &mut self,
        barrier: &Barrier,
        request: InjectBarrierRequest,
    ) -> StreamResult<()> {
        debug!(
            target: "events::stream::barrier::manager::send",
            "send barrier {:?}, actor_ids_to_collect = {:?}",
            barrier,
            request.actor_ids_to_collect
        );

        let status = self
            .state
            .partial_graphs
            .get_mut(&request.partial_graph_id)
            .expect("should exist");
        if let Some(state) = status.state_for_request() {
            state.transform_to_issued(barrier, request)?;
        }
        Ok(())
    }

    fn remove_partial_graphs(
        &mut self,
        partial_graph_ids: impl IntoIterator<Item = PartialGraphId>,
    ) {
        for partial_graph_id in partial_graph_ids {
            if let Some(mut graph) = self.state.partial_graphs.remove(&partial_graph_id) {
                if let Some(graph) = graph.state_for_request() {
                    assert!(
                        graph.graph_state.is_empty(),
                        "non empty graph to be removed: {}",
                        &graph.graph_state
                    );
                }
            } else {
                warn!(
                    partial_graph_id = %partial_graph_id,
                    "no partial graph to remove"
                );
            }
        }
    }

    fn add_partial_graph(&mut self, partial_graph_id: PartialGraphId, term_id: String) {
        match self.state.partial_graphs.entry(partial_graph_id) {
            Entry::Occupied(entry) => {
                let status = entry.into_mut();
                if let PartialGraphStatus::ReceivedExchangeRequest(pending_requests) = status {
                    let mut graph = PartialGraphState::new(
                        partial_graph_id,
                        term_id.clone(),
                        self.actor_manager.clone(),
                    );
                    for (request_term_id, (upstream_actor_id, actor_id), request) in
                        pending_requests.drain(..)
                    {
                        if request_term_id == term_id {
                            graph.new_actor_output_request(actor_id, upstream_actor_id, request);
                        } else {
                            warn!(
                                %partial_graph_id,
                                %upstream_actor_id,
                                %actor_id,
                                request_term_id,
                                current_term_id = term_id,
                                "reject buffered exchange request with stale partial graph term"
                            );
                            if let TakeReceiverRequest::Remote { result_sender, .. } = request {
                                let _ = result_sender.send(Err(anyhow!(
                                    "take receiver ({upstream_actor_id}, {actor_id}) on stale partial graph term {request_term_id}; current term is {term_id}"
                                )
                                .into()));
                            }
                        }
                    }
                    *status = PartialGraphStatus::Running(graph);
                } else {
                    panic!("duplicated partial graph: {}", partial_graph_id);
                }

                status
            }
            Entry::Vacant(entry) => entry.insert(PartialGraphStatus::Running(
                PartialGraphState::new(partial_graph_id, term_id, self.actor_manager.clone()),
            )),
        };
    }

    fn reset_partial_graphs(&mut self, req: ResetPartialGraphsRequest) {
        let mut table_ids_to_clear = HashSet::new();
        let mut reset_futures = HashMap::new();
        for partial_graph_id in req.partial_graph_ids {
            if let Some(status) = self.state.partial_graphs.get_mut(&partial_graph_id) {
                let reset_future = status.start_reset(
                    partial_graph_id,
                    self.await_epoch_completed_futures.remove(&partial_graph_id),
                    &mut table_ids_to_clear,
                );
                reset_futures.insert(partial_graph_id, reset_future);
            } else {
                self.ack_partial_graph_reset(partial_graph_id, None);
            }
        }
        if reset_futures.is_empty() {
            assert!(table_ids_to_clear.is_empty());
            return;
        }
        let state_store = self.actor_manager.env.state_store();
        self.state.resetting_graphs.push(spawn(async move {
            let outputs =
                join_all(
                    reset_futures
                        .into_iter()
                        .map(|(partial_graph_id, future)| async move {
                            (partial_graph_id, future.await)
                        }),
                )
                .await;
            if !table_ids_to_clear.is_empty()
                && let Some(hummock) = state_store.as_hummock()
            {
                hummock.clear_tables(table_ids_to_clear).await;
            }
            outputs
        }));
    }

    fn ack_partial_graph_reset(
        &mut self,
        partial_graph_id: PartialGraphId,
        reset_output: Option<ResetPartialGraphOutput>,
    ) {
        info!(
            %partial_graph_id,
            "partial graph reset successfully"
        );
        assert!(!self.state.partial_graphs.contains_key(&partial_graph_id));
        self.await_epoch_completed_futures.remove(&partial_graph_id);
        self.control_stream_handle.ack_reset_partial_graph(
            partial_graph_id,
            reset_output.and_then(|output| output.root_err),
        );
    }

    /// When some other failure happens (like failed to send barrier), the error is reported using
    /// this function. The control stream will be responded with a message to notify about the error,
    /// and the global barrier worker will later reset and rerun the partial graph.
    fn on_partial_graph_failure(
        &mut self,
        partial_graph_id: PartialGraphId,
        failed_actor: Option<ActorId>,
        err: StreamError,
        message: impl Into<String>,
    ) {
        let message = message.into();
        error!(%partial_graph_id, ?failed_actor, message, err = ?err.as_report(), "suspend partial graph on error");
        let completing_futures = self.await_epoch_completed_futures.remove(&partial_graph_id);
        self.state
            .partial_graphs
            .get_mut(&partial_graph_id)
            .expect("should exist")
            .suspend(failed_actor, err, completing_futures);
        self.control_stream_handle
            .send_response(Response::ReportPartialGraphFailure(
                ReportPartialGraphFailureResponse { partial_graph_id },
            ));
    }

    /// Force stop all actors on this worker, and then drop their resources.
    async fn reset(&mut self, _init_request: InitRequest) {
        join(
            join_all(
                self.state
                    .partial_graphs
                    .values_mut()
                    .map(|graph| graph.abort()),
            ),
            async {
                while let Some(join_result) = self.state.resetting_graphs.next().await {
                    join_result.expect("failed to join reset partial graphs handle");
                }
            },
        )
        .await;
        if let Some(m) = self.actor_manager.await_tree_reg.as_ref() {
            m.clear();
        }

        if let Some(hummock) = self.actor_manager.env.state_store().as_hummock() {
            hummock
                .clear_shared_buffer()
                .instrument_await("store_clear_shared_buffer".verbose())
                .await
        }
        self.actor_manager.env.dml_manager_ref().clear();
        *self = Self::new(self.actor_manager.clone());
        self.actor_manager.env.client_pool().invalidate_all();
    }

    /// Create a [`LocalBarrierWorker`] with managed mode.
    pub fn spawn(
        env: StreamEnvironment,
        streaming_metrics: Arc<StreamingMetrics>,
        await_tree_reg: Option<await_tree::Registry>,
        watermark_epoch: AtomicU64Ref,
        actor_op_rx: UnboundedReceiver<LocalActorOperation>,
    ) -> JoinHandle<()> {
        let runtime = {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            if let Some(worker_threads_num) = env.global_config().actor_runtime_worker_threads_num {
                builder.worker_threads(worker_threads_num);
            }
            builder
                .thread_name("rw-streaming")
                .enable_all()
                .build()
                .unwrap()
        };

        let actor_manager = Arc::new(StreamActorManager {
            env,
            streaming_metrics,
            watermark_epoch,
            await_tree_reg,
            runtime: runtime.into(),
            config_override_cache: ConfigOverrideCache::new(CONFIG_OVERRIDE_CACHE_DEFAULT_CAPACITY),
        });
        let worker = LocalBarrierWorker::new(actor_manager);
        tokio::spawn(worker.run(actor_op_rx))
    }
}

pub(super) struct EventSender<T>(pub(super) UnboundedSender<T>);

impl<T> Clone for EventSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> EventSender<T> {
    pub(super) fn send_event(&self, event: T) {
        self.0.send(event).expect("should be able to send event")
    }

    pub(super) async fn send_and_await<RSP>(
        &self,
        make_event: impl FnOnce(oneshot::Sender<RSP>) -> T,
    ) -> StreamResult<RSP> {
        let (tx, rx) = oneshot::channel();
        let event = make_event(tx);
        self.send_event(event);
        rx.await
            .map_err(|_| anyhow!("barrier manager maybe reset").into())
    }
}

pub(crate) enum NewOutputRequest {
    Local(permit::Sender),
    Remote(permit::Sender),
}

#[cfg(test)]
pub(crate) mod barrier_test_utils {
    use assert_matches::assert_matches;
    use futures::StreamExt;
    use risingwave_pb::stream_service::streaming_control_stream_request::{
        InitRequest, PbCreatePartialGraphRequest,
    };
    use risingwave_pb::stream_service::{
        InjectBarrierRequest, PbStreamingControlStreamRequest, StreamingControlStreamRequest,
        StreamingControlStreamResponse, streaming_control_stream_request,
        streaming_control_stream_response,
    };
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tonic::Status;

    use crate::executor::Barrier;
    use crate::task::barrier_worker::{ControlStreamHandle, EventSender, LocalActorOperation};
    use crate::task::{ActorId, LocalBarrierManager, NewOutputRequest, TEST_PARTIAL_GRAPH_ID};

    pub(crate) struct LocalBarrierTestEnv {
        pub local_barrier_manager: LocalBarrierManager,
        pub(super) actor_op_tx: EventSender<LocalActorOperation>,
        pub request_tx: UnboundedSender<Result<StreamingControlStreamRequest, Status>>,
        pub response_rx: UnboundedReceiver<Result<StreamingControlStreamResponse, Status>>,
    }

    impl LocalBarrierTestEnv {
        pub(crate) async fn for_test() -> Self {
            let actor_op_tx = LocalBarrierManager::spawn_for_test();

            let (request_tx, request_rx) = unbounded_channel();
            let (response_tx, mut response_rx) = unbounded_channel();

            request_tx
                .send(Ok(PbStreamingControlStreamRequest {
                    request: Some(
                        streaming_control_stream_request::Request::CreatePartialGraph(
                            PbCreatePartialGraphRequest {
                                partial_graph_id: TEST_PARTIAL_GRAPH_ID,
                                term_id: "for_test".into(),
                            },
                        ),
                    ),
                }))
                .unwrap();

            actor_op_tx.send_event(LocalActorOperation::NewControlStream {
                handle: ControlStreamHandle::new(
                    response_tx,
                    UnboundedReceiverStream::new(request_rx).boxed(),
                ),
                init_request: InitRequest {},
            });

            assert_matches!(
                response_rx.recv().await.unwrap().unwrap().response.unwrap(),
                streaming_control_stream_response::Response::Init(_)
            );

            let local_barrier_manager = actor_op_tx
                .send_and_await(LocalActorOperation::GetCurrentLocalBarrierManager)
                .await
                .unwrap();

            Self {
                local_barrier_manager,
                actor_op_tx,
                request_tx,
                response_rx,
            }
        }

        pub(crate) fn inject_barrier(
            &self,
            barrier: &Barrier,
            actor_to_collect: impl IntoIterator<Item = ActorId>,
        ) {
            self.request_tx
                .send(Ok(StreamingControlStreamRequest {
                    request: Some(streaming_control_stream_request::Request::InjectBarrier(
                        InjectBarrierRequest {
                            request_id: "".to_owned(),
                            barrier: Some(barrier.to_protobuf()),
                            actor_ids_to_collect: actor_to_collect.into_iter().collect(),
                            table_ids_to_sync: vec![],
                            partial_graph_id: TEST_PARTIAL_GRAPH_ID,
                            actors_to_build: vec![],
                        },
                    )),
                }))
                .unwrap();
        }

        pub(crate) async fn flush_all_events(&self) {
            Self::flush_all_events_impl(&self.actor_op_tx).await
        }

        pub(super) async fn flush_all_events_impl(actor_op_tx: &EventSender<LocalActorOperation>) {
            let (tx, rx) = oneshot::channel();
            actor_op_tx.send_event(LocalActorOperation::Flush(tx));
            rx.await.unwrap()
        }

        pub(crate) async fn take_pending_new_output_requests(
            &self,
            actor_id: ActorId,
        ) -> Vec<(ActorId, NewOutputRequest)> {
            self.actor_op_tx
                .send_and_await(|tx| LocalActorOperation::TakePendingNewOutputRequest(actor_id, tx))
                .await
                .unwrap()
        }
    }
}
