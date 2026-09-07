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

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::LazyLock;
use std::task::Poll;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use await_tree::InstrumentAwait;
use futures::future::{Either, pending, select};
use futures::pin_mut;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::util::retry::exponential_backoff;
use risingwave_connector::connector_common::IcebergSinkCompactionUpdate;
use risingwave_connector::dispatch_sink;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::{
    Sink, SinkCommitCoordinator, SinkCommittedEpochSubscriber, SinkError, SinkParam, build_sink,
};
use risingwave_meta_model::pending_sink_state::SinkState;
use risingwave_pb::connector_service::{SinkMetadata, coordinate_request};
use risingwave_pb::stream_plan::PbSinkSchemaChange;
use sea_orm::DatabaseConnection;
use thiserror_ext::AsReport;
use tokio::select;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::time::sleep;
use tokio_retry::strategy::jitter;
use tonic::Status;
use tracing::{error, warn};

use crate::manager::exactly_once_util::{
    clean_aborted_records, commit_and_prune_epoch, list_sink_states_ordered_by_epoch,
    persist_pre_commit_metadata,
};
use crate::manager::sink_coordination::handle::SinkWriterCoordinationHandle;

// Keep coordinator initialization below the default MetaStore pool size (10), leaving
// connections available for recovery and other Meta services.
const MAX_CONCURRENT_COORDINATOR_INITIALIZATIONS: usize = 8;
static COORDINATOR_INIT_SEMAPHORE: LazyLock<Semaphore> =
    LazyLock::new(|| Semaphore::new(MAX_CONCURRENT_COORDINATOR_INITIALIZATIONS));

async fn run_future_with_periodic_fn<F: Future>(
    future: F,
    interval: Duration,
    mut f: impl FnMut(),
) -> F::Output {
    pin_mut!(future);
    loop {
        match select(&mut future, pin!(sleep(interval))).await {
            Either::Left((output, _)) => {
                break output;
            }
            Either::Right(_) => f(),
        }
    }
}

type HandleId = usize;

pub(super) struct DrainRequest {
    pub target_epoch: u64,
    pub reply: oneshot::Sender<anyhow::Result<()>>,
}

pub(super) enum CoordinatorRequest {
    NewWriter(SinkWriterCoordinationHandle),
    Drain(DrainRequest),
}

#[derive(Default)]
struct AligningRequests<R> {
    requests: Vec<R>,
    handle_ids: HashSet<HandleId>,
    committed_bitmap: Option<Bitmap>, // lazy-initialized on first request
}

impl<R> AligningRequests<R> {
    fn add_new_request(
        &mut self,
        handle_id: HandleId,
        request: R,
        vnode_bitmap: &Bitmap,
    ) -> anyhow::Result<()>
    where
        R: Debug,
    {
        let committed_bitmap = self
            .committed_bitmap
            .get_or_insert_with(|| Bitmap::zeros(vnode_bitmap.len()));
        assert_eq!(committed_bitmap.len(), vnode_bitmap.len());

        let check_bitmap = (&*committed_bitmap) & vnode_bitmap;
        if check_bitmap.count_ones() > 0 {
            return Err(anyhow!(
                "duplicate vnode {:?}. request vnode: {:?}, prev vnode: {:?}. pending request: {:?}, request: {:?}",
                check_bitmap.iter_ones().collect_vec(),
                vnode_bitmap,
                committed_bitmap,
                self.requests,
                request
            ));
        }
        *committed_bitmap |= vnode_bitmap;
        self.requests.push(request);
        assert!(self.handle_ids.insert(handle_id));
        Ok(())
    }

    fn aligned(&self) -> bool {
        self.committed_bitmap.as_ref().is_some_and(|b| b.all())
    }
}

type RetryBackoffFuture = std::pin::Pin<Box<tokio::time::Sleep>>;
type RetryBackoffStrategy = impl Iterator<Item = RetryBackoffFuture> + Send + 'static;

struct TwoPhaseCommitHandler {
    db: DatabaseConnection,
    sink_id: SinkId,
    curr_hummock_committed_epoch: u64,
    job_committed_epoch_rx: UnboundedReceiver<u64>,
    last_committed_epoch: Option<u64>,
    pending_epochs: VecDeque<(u64, Option<Vec<u8>>, Option<PbSinkSchemaChange>)>,
    prepared_epochs: VecDeque<(u64, Option<Vec<u8>>, Option<PbSinkSchemaChange>)>,
    backoff_state: Option<(RetryBackoffFuture, RetryBackoffStrategy)>,
}

impl TwoPhaseCommitHandler {
    fn new(
        db: DatabaseConnection,
        sink_id: SinkId,
        initial_hummock_committed_epoch: u64,
        job_committed_epoch_rx: UnboundedReceiver<u64>,
        last_committed_epoch: Option<u64>,
    ) -> Self {
        Self {
            db,
            sink_id,
            curr_hummock_committed_epoch: initial_hummock_committed_epoch,
            job_committed_epoch_rx,
            last_committed_epoch,
            pending_epochs: VecDeque::new(),
            prepared_epochs: VecDeque::new(),
            backoff_state: None,
        }
    }

    #[define_opaque(RetryBackoffStrategy)]
    fn get_retry_backoff_strategy() -> RetryBackoffStrategy {
        exponential_backoff(Duration::from_millis(10), 10, Duration::from_secs(60))
            .map(jitter)
            .map(|delay| Box::pin(tokio::time::sleep(delay)))
    }

    async fn next_to_commit(
        &mut self,
    ) -> anyhow::Result<(u64, Option<Vec<u8>>, Option<PbSinkSchemaChange>)> {
        loop {
            let wait_backoff = async {
                if self.prepared_epochs.is_empty() {
                    pending::<()>().await;
                } else if let Some((backoff_fut, _)) = &mut self.backoff_state {
                    backoff_fut.await;
                }
            };

            select! {
                _ = wait_backoff => {
                    let item = self.prepared_epochs.front().cloned().expect("non-empty");
                    return Ok(item);
                }

                recv_epoch = self.job_committed_epoch_rx.recv() => {
                    let Some(recv_epoch) = recv_epoch else {
                        return Err(anyhow!(
                            "Hummock committed epoch sender closed unexpectedly"
                        ));
                    };
                    self.curr_hummock_committed_epoch = recv_epoch;
                    while let Some((epoch, metadata, schema_change)) = self.pending_epochs.pop_front_if(|(epoch, _, _)| *epoch <= recv_epoch) {
                        if let Some((last_epoch, _, _)) = self.prepared_epochs.back() {
                            assert!(epoch > *last_epoch, "prepared epochs must be in increasing order");
                        }
                        self.prepared_epochs.push_back((epoch, metadata, schema_change));
                    }
                }
            }
        }
    }

    fn push_new_item(
        &mut self,
        epoch: u64,
        metadata: Option<Vec<u8>>,
        schema_change: Option<PbSinkSchemaChange>,
    ) {
        if epoch > self.curr_hummock_committed_epoch {
            if let Some((last_epoch, _, _)) = self.pending_epochs.back() {
                assert!(
                    epoch > *last_epoch,
                    "pending epochs must be in increasing order"
                );
            }
            self.pending_epochs
                .push_back((epoch, metadata, schema_change));
        } else {
            assert!(self.pending_epochs.is_empty());
            if let Some((last_epoch, _, _)) = self.prepared_epochs.back() {
                assert!(
                    epoch > *last_epoch,
                    "prepared epochs must be in increasing order"
                );
            }
            self.prepared_epochs
                .push_back((epoch, metadata, schema_change));
        }
    }

    async fn ack_committed(&mut self, epoch: u64) -> anyhow::Result<()> {
        self.backoff_state = None;
        let (last_epoch, _, _) = self.prepared_epochs.pop_front().expect("non-empty");
        assert_eq!(last_epoch, epoch);

        commit_and_prune_epoch(&self.db, self.sink_id, epoch, self.last_committed_epoch).await?;
        self.last_committed_epoch = Some(epoch);
        Ok(())
    }

    fn failed_committed(&mut self, epoch: u64, err: SinkError) {
        assert_eq!(self.prepared_epochs.front().expect("non-empty").0, epoch,);
        if let Some((prev_fut, strategy)) = &mut self.backoff_state {
            let new_fut = strategy.next().expect("infinite");
            *prev_fut = new_fut;
        } else {
            let mut strategy = Self::get_retry_backoff_strategy();
            let backoff_fut = strategy.next().expect("infinite");
            self.backoff_state = Some((backoff_fut, strategy));
        }
        tracing::error!(
            error = %err.as_report(),
            %self.sink_id,
            "failed to commit epoch {}, Retrying after backoff",
            epoch,
        );
    }

    fn is_empty(&self) -> bool {
        self.pending_epochs.is_empty() && self.prepared_epochs.is_empty()
    }

    /// Whether there exists an uncommitted schema change.
    ///
    /// Per current design, if a `schema_change` exists, it should be attached to the latest
    /// uncommitted item across `pending_epochs` and `prepared_epochs`.
    fn has_uncommitted_schema_change(&self) -> bool {
        if let Some((_, _, schema_change)) = self.pending_epochs.back() {
            schema_change.is_some()
        } else if let Some((_, _, schema_change)) = self.prepared_epochs.back() {
            schema_change.is_some()
        } else {
            false
        }
    }
}

struct CoordinationHandleManager {
    param: SinkParam,
    writer_handles: HashMap<HandleId, SinkWriterCoordinationHandle>,
    next_handle_id: HandleId,
    request_rx: UnboundedReceiver<CoordinatorRequest>,
}

impl CoordinationHandleManager {
    fn start(
        &mut self,
        log_store_rewind_start_epoch: Option<u64>,
        handle_ids: impl IntoIterator<Item = HandleId>,
    ) -> anyhow::Result<()> {
        for handle_id in handle_ids {
            let handle = self
                .writer_handles
                .get_mut(&handle_id)
                .ok_or_else(|| anyhow!("failed to find handle {} to start", handle_id,))?;
            handle.start(log_store_rewind_start_epoch).map_err(|_| {
                anyhow!(
                    "failed to start {:?} for handle {}",
                    log_store_rewind_start_epoch,
                    handle_id
                )
            })?;
        }
        Ok(())
    }

    fn ack_aligned_initial_epoch(&mut self, aligned_initial_epoch: u64) -> anyhow::Result<()> {
        for (handle_id, handle) in &mut self.writer_handles {
            handle
                .ack_aligned_initial_epoch(aligned_initial_epoch)
                .map_err(|_| {
                    anyhow!(
                        "failed to ack aligned initial epoch {:?} for handle {}",
                        aligned_initial_epoch,
                        handle_id
                    )
                })?;
        }
        Ok(())
    }

    fn ack_commit(
        &mut self,
        epoch: u64,
        handle_ids: impl IntoIterator<Item = HandleId>,
    ) -> anyhow::Result<()> {
        for handle_id in handle_ids {
            let handle = self.writer_handles.get_mut(&handle_id).ok_or_else(|| {
                anyhow!(
                    "failed to find handle {} when acknowledging the commit for epoch {}",
                    handle_id,
                    epoch
                )
            })?;
            handle.ack_commit(epoch).map_err(|_| {
                anyhow!(
                    "failed to acknowledge the commit for epoch {} on handle {}",
                    epoch,
                    handle_id
                )
            })?;
        }
        Ok(())
    }

    async fn next_request_inner(
        writer_handles: &mut HashMap<HandleId, SinkWriterCoordinationHandle>,
    ) -> anyhow::Result<(HandleId, coordinate_request::Msg)> {
        poll_fn(|cx| {
            for (handle_id, handle) in writer_handles.iter_mut() {
                if let Poll::Ready(result) = handle.poll_next_request(cx) {
                    return Poll::Ready(result.map(|request| (*handle_id, request)));
                }
            }
            Poll::Pending
        })
        .await
    }
}

enum CoordinationHandleManagerEvent {
    NewHandle(HandleId),
    UpdateVnodeBitmap(HandleId),
    Stop(HandleId),
    CommitRequest {
        handle_id: HandleId,
        epoch: u64,
        metadata: SinkMetadata,
        schema_change: Option<PbSinkSchemaChange>,
    },
    AlignInitialEpoch {
        handle_id: HandleId,
        epoch: u64,
    },
    Drain(DrainRequest),
}

impl CoordinationHandleManagerEvent {
    fn name(&self) -> &'static str {
        match self {
            CoordinationHandleManagerEvent::NewHandle(_) => "NewHandle",
            CoordinationHandleManagerEvent::UpdateVnodeBitmap(_) => "UpdateVnodeBitmap",
            CoordinationHandleManagerEvent::Stop(_) => "Stop",
            CoordinationHandleManagerEvent::CommitRequest { .. } => "CommitRequest",
            CoordinationHandleManagerEvent::AlignInitialEpoch { .. } => "AlignInitialEpoch",
            CoordinationHandleManagerEvent::Drain(_) => "Drain",
        }
    }
}

impl CoordinationHandleManager {
    async fn next_event(&mut self) -> anyhow::Result<CoordinationHandleManagerEvent> {
        select! {
            request = self.request_rx.recv() => {
                let request = request.ok_or_else(|| anyhow!("end of coordinator request stream"))?;
                match request {
                    CoordinatorRequest::NewWriter(handle) => {
                        if handle.param() != &self.param {
                            warn!(prev_param = ?self.param, new_param = ?handle.param(), "sink param mismatch");
                        }
                        let handle_id = self.next_handle_id;
                        self.next_handle_id += 1;
                        self.writer_handles.insert(handle_id, handle);
                        Ok(CoordinationHandleManagerEvent::NewHandle(handle_id))
                    }
                    CoordinatorRequest::Drain(request) => {
                        Ok(CoordinationHandleManagerEvent::Drain(request))
                    }
                }
            }
            result = Self::next_request_inner(&mut self.writer_handles) => {
                let (handle_id, request) = result?;
                let event = match request {
                    coordinate_request::Msg::CommitRequest(request) => {
                        CoordinationHandleManagerEvent::CommitRequest {
                            handle_id,
                            epoch: request.epoch,
                            metadata: request.metadata.ok_or_else(|| anyhow!("empty sink metadata"))?,
                            schema_change: request.schema_change,
                        }
                    }
                    coordinate_request::Msg::AlignInitialEpochRequest(epoch) => {
                        CoordinationHandleManagerEvent::AlignInitialEpoch { handle_id, epoch }
                    }
                    coordinate_request::Msg::UpdateVnodeRequest(_) => {
                        CoordinationHandleManagerEvent::UpdateVnodeBitmap(handle_id)
                    }
                    coordinate_request::Msg::Stop(_) => {
                        CoordinationHandleManagerEvent::Stop(handle_id)
                    }
                    coordinate_request::Msg::StartRequest(_) => {
                        unreachable!("should have been handled");
                    }
                };
                Ok(event)
            }
        }
    }

    fn vnode_bitmap(&self, handle_id: HandleId) -> &Bitmap {
        self.writer_handles[&handle_id].vnode_bitmap()
    }

    fn stop_handle(&mut self, handle_id: HandleId) -> anyhow::Result<()> {
        self.writer_handles
            .remove(&handle_id)
            .expect("should exist")
            .stop()
    }

    async fn wait_init_handles(&mut self) -> anyhow::Result<HashSet<HandleId>> {
        assert!(self.writer_handles.is_empty());
        let mut init_requests = AligningRequests::default();
        while !init_requests.aligned() {
            let event = self.next_event().await?;
            let unexpected_event = match event {
                CoordinationHandleManagerEvent::NewHandle(handle_id) => {
                    init_requests.add_new_request(handle_id, (), self.vnode_bitmap(handle_id))?;
                    continue;
                }
                event => event.name(),
            };
            return Err(anyhow!(
                "expect new handle during init, but got {}",
                unexpected_event
            ));
        }
        Ok(init_requests.handle_ids)
    }

    async fn alter_parallelisms(
        &mut self,
        altered_handles: impl Iterator<Item = HandleId>,
    ) -> anyhow::Result<AlterParallelismsResult> {
        let mut requests = AligningRequests::default();
        for handle_id in altered_handles {
            requests.add_new_request(handle_id, (), self.vnode_bitmap(handle_id))?;
        }
        let mut remaining_handles: HashSet<_> = self
            .writer_handles
            .keys()
            .filter(|handle_id| !requests.handle_ids.contains(handle_id))
            .cloned()
            .collect();
        let mut drain_waiters = Vec::new();
        loop {
            if remaining_handles.is_empty()
                && (requests.aligned()
                    || (requests.handle_ids.is_empty() && !drain_waiters.is_empty()))
            {
                break;
            }

            match self.next_event().await? {
                CoordinationHandleManagerEvent::NewHandle(handle_id) => {
                    requests.add_new_request(handle_id, (), self.vnode_bitmap(handle_id))?;
                }
                CoordinationHandleManagerEvent::UpdateVnodeBitmap(handle_id) => {
                    assert!(remaining_handles.remove(&handle_id));
                    requests.add_new_request(handle_id, (), self.vnode_bitmap(handle_id))?;
                }
                CoordinationHandleManagerEvent::Stop(handle_id) => {
                    assert!(remaining_handles.remove(&handle_id));
                    self.stop_handle(handle_id)?;
                }
                CoordinationHandleManagerEvent::CommitRequest {
                    handle_id, epoch, ..
                } => {
                    bail!(
                        "receive commit request on epoch {} from handle {} during alter parallelism",
                        epoch,
                        handle_id
                    );
                }
                CoordinationHandleManagerEvent::AlignInitialEpoch { handle_id, epoch } => {
                    bail!(
                        "receive AlignInitialEpoch on epoch {} from handle {} during alter parallelism",
                        epoch,
                        handle_id
                    );
                }
                CoordinationHandleManagerEvent::Drain(drain_waiter) => {
                    drain_waiters.push(drain_waiter);
                }
            }
        }
        Ok(AlterParallelismsResult {
            handle_ids: requests.handle_ids,
            drain_waiters,
        })
    }
}

struct AlterParallelismsResult {
    handle_ids: HashSet<HandleId>,
    drain_waiters: Vec<DrainRequest>,
}

/// Represents the coordinator worker's state machine for handling schema changes.
///
/// - `Running`: Normal operation, handles can be started immediately
/// - `WaitingForFlushed`: Waiting for all pending two-phase commits to complete before starting new handles. This
///   ensures new sink executors load the correct schema.
enum CoordinatorWorkerState {
    Running,
    WaitingForFlushed(HashSet<HandleId>),
}

#[derive(Clone, Copy)]
enum CommitRequestAlignment {
    Idle,
    InProgress,
}

pub struct CoordinatorWorker {
    handle_manager: CoordinationHandleManager,
    drain_waiters: Vec<DrainRequest>,
    /// Last epoch whose commit has been acknowledged to sink writers.
    ///
    /// On recovery, pending sink states are treated as already acknowledged to writers, so this is
    /// initialized to the latest pending epoch if any. Otherwise, it starts from the latest
    /// committed epoch persisted in `pending_sink_state`.
    last_writer_acked_epoch: Option<u64>,
    curr_state: CoordinatorWorkerState,
}

enum CoordinatorWorkerEvent {
    HandleManagerEvent(CoordinationHandleManagerEvent),
    ReadyToCommit(u64, Option<Vec<u8>>, Option<PbSinkSchemaChange>),
}

impl CoordinatorWorker {
    pub async fn run(
        param: SinkParam,
        request_rx: UnboundedReceiver<CoordinatorRequest>,
        db: DatabaseConnection,
        subscriber: SinkCommittedEpochSubscriber,
        iceberg_compact_stat_sender: UnboundedSender<IcebergSinkCompactionUpdate>,
    ) {
        let sink = match build_sink(param.clone()) {
            Ok(sink) => sink,
            Err(e) => {
                error!(
                    error = %e.as_report(),
                    "unable to build sink with param {:?}",
                    param
                );
                return;
            }
        };

        dispatch_sink!(sink, sink, {
            let coordinator =
                match Box::pin(sink.new_coordinator(Some(iceberg_compact_stat_sender))).await {
                    Ok(coordinator) => coordinator,
                    Err(e) => {
                        error!(
                            error = %e.as_report(),
                            "unable to build coordinator with param {:?}",
                            param
                        );
                        return;
                    }
                };
            Self::execute_coordinator(db, param, request_rx, coordinator, subscriber).await
        });
    }

    pub async fn execute_coordinator(
        db: DatabaseConnection,
        param: SinkParam,
        request_rx: UnboundedReceiver<CoordinatorRequest>,
        coordinator: SinkCommitCoordinator,
        subscriber: SinkCommittedEpochSubscriber,
    ) {
        let mut worker = CoordinatorWorker {
            handle_manager: CoordinationHandleManager {
                param,
                writer_handles: HashMap::new(),
                next_handle_id: 0,
                request_rx,
            },
            drain_waiters: Vec::new(),
            last_writer_acked_epoch: None,
            curr_state: CoordinatorWorkerState::Running,
        };

        if let Err(e) = worker.run_coordination(db, coordinator, subscriber).await {
            for handle in worker.handle_manager.writer_handles.into_values() {
                handle.abort(Status::internal(format!(
                    "failed to run coordination: {:?}",
                    e.as_report()
                )))
            }
        }
    }

    async fn try_handle_init_requests(
        &mut self,
        pending_handle_ids: &HashSet<HandleId>,
        two_phase_handler: &mut TwoPhaseCommitHandler,
    ) -> anyhow::Result<()> {
        assert!(matches!(self.curr_state, CoordinatorWorkerState::Running));
        if pending_handle_ids.is_empty() {
            return Ok(());
        }
        if two_phase_handler.has_uncommitted_schema_change() {
            // Delay handling init requests until all pending epochs are flushed.
            self.curr_state = CoordinatorWorkerState::WaitingForFlushed(pending_handle_ids.clone());
        } else {
            self.handle_init_requests_impl(pending_handle_ids.clone())
                .await?;
        }
        Ok(())
    }

    async fn handle_init_requests_impl(
        &mut self,
        pending_handle_ids: impl IntoIterator<Item = HandleId>,
    ) -> anyhow::Result<()> {
        let log_store_rewind_start_epoch = self.last_writer_acked_epoch;
        self.handle_manager
            .start(log_store_rewind_start_epoch, pending_handle_ids)?;
        if log_store_rewind_start_epoch.is_none() {
            let mut align_requests = AligningRequests::default();
            while !align_requests.aligned() {
                let event = self.handle_manager.next_event().await?;
                match event {
                    CoordinationHandleManagerEvent::AlignInitialEpoch {
                        handle_id,
                        epoch: initial_epoch,
                    } => {
                        align_requests.add_new_request(
                            handle_id,
                            initial_epoch,
                            self.handle_manager.vnode_bitmap(handle_id),
                        )?;
                    }
                    other => {
                        return Err(anyhow!("expect AlignInitialEpoch but got {}", other.name()));
                    }
                }
            }
            let aligned_initial_epoch = align_requests
                .requests
                .into_iter()
                .max()
                .expect("non-empty");
            self.handle_manager
                .ack_aligned_initial_epoch(aligned_initial_epoch)?;
        }
        Ok(())
    }

    fn flush_drain_waiters_if_drained(
        &mut self,
        two_phase_handler: &TwoPhaseCommitHandler,
        commit_request_alignment: CommitRequestAlignment,
    ) {
        if matches!(commit_request_alignment, CommitRequestAlignment::InProgress)
            || !two_phase_handler.is_empty()
        {
            return;
        }

        let last_writer_acked_epoch = self.last_writer_acked_epoch;
        for waiter in self.drain_waiters.extract_if(.., |waiter| {
            last_writer_acked_epoch.is_some_and(|epoch| epoch >= waiter.target_epoch)
        }) {
            let _ = waiter.reply.send(Ok(()));
        }
    }

    async fn next_event(
        &mut self,
        two_phase_handler: &mut TwoPhaseCommitHandler,
        commit_request_alignment: CommitRequestAlignment,
    ) -> anyhow::Result<CoordinatorWorkerEvent> {
        self.flush_drain_waiters_if_drained(two_phase_handler, commit_request_alignment);

        if let CoordinatorWorkerState::WaitingForFlushed(pending_handle_ids) = &self.curr_state
            && two_phase_handler.is_empty()
        {
            let pending_handle_ids = pending_handle_ids.clone();
            self.handle_init_requests_impl(pending_handle_ids).await?;
            self.curr_state = CoordinatorWorkerState::Running;
        }

        select! {
            next_handle_event = self.handle_manager.next_event() => {
                Ok(CoordinatorWorkerEvent::HandleManagerEvent(next_handle_event?))
            }

            next_item_to_commit = two_phase_handler.next_to_commit() => {
                let (epoch, metadata, schema_change) = next_item_to_commit?;
                Ok(CoordinatorWorkerEvent::ReadyToCommit(epoch, metadata, schema_change))
            }
        }
    }

    async fn run_coordination(
        &mut self,
        db: DatabaseConnection,
        mut coordinator: SinkCommitCoordinator,
        subscriber: SinkCommittedEpochSubscriber,
    ) -> anyhow::Result<()> {
        let sink_id = self.handle_manager.param.sink_id;

        let coordinator_init_permit = COORDINATOR_INIT_SEMAPHORE
            .acquire()
            .instrument_await("acquire_sink_coordinator_init_permit")
            .await?;
        let mut two_phase_handler = self
            .init_state_from_store(&db, sink_id, subscriber, &mut coordinator)
            .await?;
        match &mut coordinator {
            SinkCommitCoordinator::SinglePhase(coordinator) => coordinator.init().await?,
            SinkCommitCoordinator::TwoPhase(coordinator) => coordinator.init().await?,
        }
        drop(coordinator_init_permit);

        let mut running_handles = self.handle_manager.wait_init_handles().await?;
        self.try_handle_init_requests(&running_handles, &mut two_phase_handler)
            .await?;

        let mut pending_epochs: BTreeMap<u64, AligningRequests<_>> = BTreeMap::new();
        let mut pending_new_handles = vec![];
        loop {
            let commit_request_alignment = if pending_epochs.is_empty() {
                CommitRequestAlignment::Idle
            } else {
                CommitRequestAlignment::InProgress
            };
            let event = self
                .next_event(&mut two_phase_handler, commit_request_alignment)
                .await?;
            let (handle_id, epoch, commit_request) = match event {
                CoordinatorWorkerEvent::HandleManagerEvent(event) => match event {
                    CoordinationHandleManagerEvent::NewHandle(handle_id) => {
                        pending_new_handles.push(handle_id);
                        continue;
                    }
                    CoordinationHandleManagerEvent::UpdateVnodeBitmap(handle_id) => {
                        let result = self
                            .handle_manager
                            .alter_parallelisms(pending_new_handles.drain(..).chain([handle_id]))
                            .await?;
                        running_handles = result.handle_ids;
                        self.drain_waiters.extend(result.drain_waiters);
                        self.try_handle_init_requests(&running_handles, &mut two_phase_handler)
                            .await?;
                        continue;
                    }
                    CoordinationHandleManagerEvent::Stop(handle_id) => {
                        self.handle_manager.stop_handle(handle_id)?;
                        let result = self
                            .handle_manager
                            .alter_parallelisms(pending_new_handles.drain(..))
                            .await?;
                        running_handles = result.handle_ids;
                        self.drain_waiters.extend(result.drain_waiters);
                        self.try_handle_init_requests(&running_handles, &mut two_phase_handler)
                            .await?;

                        continue;
                    }
                    CoordinationHandleManagerEvent::CommitRequest {
                        handle_id,
                        epoch,
                        metadata,
                        schema_change,
                    } => (handle_id, epoch, (metadata, schema_change)),
                    CoordinationHandleManagerEvent::AlignInitialEpoch { .. } => {
                        bail!("receive AlignInitialEpoch after initialization")
                    }
                    CoordinationHandleManagerEvent::Drain(drain_waiter) => {
                        self.drain_waiters.push(drain_waiter);
                        continue;
                    }
                },
                CoordinatorWorkerEvent::ReadyToCommit(epoch, metadata, schema_change) => {
                    let start_time = Instant::now();
                    let commit_fut = async {
                        match &mut coordinator {
                            SinkCommitCoordinator::SinglePhase(coordinator) => {
                                assert!(metadata.is_none());
                                if let Some(schema_change) = schema_change {
                                    coordinator
                                        .commit_schema_change(epoch, schema_change)
                                        .instrument_await(Self::commit_span(
                                            "single_phase_schema_change",
                                            sink_id,
                                            epoch,
                                        ))
                                        .await?;
                                }
                            }
                            SinkCommitCoordinator::TwoPhase(coordinator) => {
                                if let Some(metadata) = metadata {
                                    coordinator
                                        .commit_data(epoch, metadata)
                                        .instrument_await(Self::commit_span(
                                            "two_phase_commit_data",
                                            sink_id,
                                            epoch,
                                        ))
                                        .await?;
                                }
                                if let Some(schema_change) = schema_change {
                                    coordinator
                                        .commit_schema_change(epoch, schema_change)
                                        .instrument_await(Self::commit_span(
                                            "two_phase_commit_schema_change",
                                            sink_id,
                                            epoch,
                                        ))
                                        .await?;
                                }
                            }
                        }
                        Ok(())
                    };
                    let commit_res =
                        run_future_with_periodic_fn(commit_fut, Duration::from_secs(5), || {
                            warn!(
                                elapsed = ?start_time.elapsed(),
                                %sink_id,
                                "committing"
                            );
                        })
                        .await;

                    match commit_res {
                        Ok(_) => {
                            two_phase_handler.ack_committed(epoch).await?;
                        }
                        Err(e) => {
                            two_phase_handler.failed_committed(epoch, e);
                        }
                    }

                    continue;
                }
            };
            if !running_handles.contains(&handle_id) {
                bail!(
                    "receiving commit request from non-running handle {}, running handles: {:?}",
                    handle_id,
                    running_handles
                );
            }
            pending_epochs.entry(epoch).or_default().add_new_request(
                handle_id,
                commit_request,
                self.handle_manager.vnode_bitmap(handle_id),
            )?;
            if pending_epochs
                .first_key_value()
                .expect("non-empty")
                .1
                .aligned()
            {
                let (epoch, commit_requests) = pending_epochs.pop_first().expect("non-empty");
                let mut metadatas = Vec::with_capacity(commit_requests.requests.len());
                let mut requests = commit_requests.requests.into_iter();
                let (first_metadata, first_schema_change) = requests.next().expect("non-empty");
                metadatas.push(first_metadata);
                for (metadata, schema_change) in requests {
                    if first_schema_change != schema_change {
                        return Err(anyhow!(
                            "got different schema change {:?} to prev schema change {:?}",
                            schema_change,
                            first_schema_change
                        ));
                    }
                    metadatas.push(metadata);
                }

                match &mut coordinator {
                    SinkCommitCoordinator::SinglePhase(coordinator) => {
                        if !metadatas.is_empty() {
                            let start_time = Instant::now();
                            run_future_with_periodic_fn(
                                coordinator.commit_data(epoch, metadatas).instrument_await(
                                    Self::commit_span("single_phase_commit_data", sink_id, epoch),
                                ),
                                Duration::from_secs(5),
                                || {
                                    warn!(
                                        elapsed = ?start_time.elapsed(),
                                        %sink_id,
                                        "committing"
                                    );
                                },
                            )
                            .await
                            .map_err(|e| anyhow!(e))?;
                        }
                        if first_schema_change.is_some() {
                            persist_pre_commit_metadata(
                                &db,
                                sink_id as _,
                                epoch,
                                None,
                                first_schema_change.as_ref(),
                            )
                            .await?;
                            two_phase_handler.push_new_item(epoch, None, first_schema_change);
                        }
                    }
                    SinkCommitCoordinator::TwoPhase(coordinator) => {
                        let commit_metadata = coordinator
                            .pre_commit(epoch, metadatas, first_schema_change.clone())
                            .instrument_await(Self::commit_span(
                                "two_phase_pre_commit",
                                sink_id,
                                epoch,
                            ))
                            .await?;
                        // Persist every acknowledged epoch, even when there is no metadata or
                        // schema change. Writers may truncate the epoch as soon as they receive
                        // the acknowledgement, so recovery must retain the same progress. The
                        // commit handler treats a `None`/`None` item as an external no-op before
                        // marking it committed, and recovery re-enqueues it through the same path.
                        persist_pre_commit_metadata(
                            &db,
                            sink_id as _,
                            epoch,
                            commit_metadata.clone(),
                            first_schema_change.as_ref(),
                        )
                        .await?;
                        two_phase_handler.push_new_item(
                            epoch,
                            commit_metadata,
                            first_schema_change,
                        );
                    }
                }

                self.handle_manager
                    .ack_commit(epoch, commit_requests.handle_ids)?;
                self.last_writer_acked_epoch = Some(epoch);
            }
        }
    }

    /// Return `TwoPhaseCommitHandler` initialized from the persisted state in the meta store.
    async fn init_state_from_store(
        &mut self,
        db: &DatabaseConnection,
        sink_id: SinkId,
        subscriber: SinkCommittedEpochSubscriber,
        coordinator: &mut SinkCommitCoordinator,
    ) -> anyhow::Result<TwoPhaseCommitHandler> {
        let ordered_metadata = list_sink_states_ordered_by_epoch(db, sink_id as _).await?;

        let mut metadata_iter = ordered_metadata.into_iter().peekable();
        let last_committed_epoch = metadata_iter
            .next_if(|(_, state, _, _)| matches!(state, SinkState::Committed))
            .map(|(epoch, _, _, _)| epoch);

        let pending_items = metadata_iter
            .peeking_take_while(|(_, state, _, _)| matches!(state, SinkState::Pending))
            .map(|(epoch, _, metadata, schema_change)| (epoch, metadata, schema_change))
            .collect_vec();
        self.last_writer_acked_epoch = pending_items
            .last()
            .map(|(epoch, _, _)| *epoch)
            .or(last_committed_epoch);

        let mut aborted_epochs = vec![];

        for (epoch, state, metadata, _) in metadata_iter {
            match state {
                SinkState::Aborted => {
                    if let Some(metadata) = metadata
                        && let SinkCommitCoordinator::TwoPhase(coordinator) = coordinator
                    {
                        coordinator.abort(epoch, metadata).await;
                    }
                    aborted_epochs.push(epoch);
                }
                other => {
                    unreachable!(
                        "unexpected state {:?} after pending items at epoch {}",
                        other, epoch
                    );
                }
            }
        }

        // Records for all aborted epochs and previously committed epochs are no longer needed.
        clean_aborted_records(db, sink_id, aborted_epochs).await?;

        let (initial_hummock_committed_epoch, job_committed_epoch_rx) = subscriber(sink_id).await?;
        let mut two_phase_handler = TwoPhaseCommitHandler::new(
            db.clone(),
            sink_id,
            initial_hummock_committed_epoch,
            job_committed_epoch_rx,
            last_committed_epoch,
        );

        for (epoch, metadata, schema_change) in pending_items {
            two_phase_handler.push_new_item(epoch, metadata, schema_change);
        }

        Ok(two_phase_handler)
    }

    fn commit_span(stage: &str, sink_id: SinkId, epoch: u64) -> await_tree::Span {
        await_tree::span!("sink_coord_{stage} (sink_id {sink_id}, epoch {epoch})").long_running()
    }
}
