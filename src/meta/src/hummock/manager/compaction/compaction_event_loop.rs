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
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use anyhow::Context;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use risingwave_hummock_sdk::compact_task::ReportTask;
use risingwave_hummock_sdk::{CompactionGroupId, HummockContextId};
use risingwave_pb::hummock::compact_task::{TaskStatus, TaskType};
use risingwave_pb::hummock::subscribe_compaction_event_request::{
    Event as RequestEvent, HeartBeat, PullTask,
};
use risingwave_pb::hummock::subscribe_compaction_event_response::{
    Event as ResponseEvent, PullTaskAck,
};
use risingwave_pb::hummock::{CompactTaskProgress, SubscribeCompactionEventRequest};
use risingwave_pb::iceberg_compaction::SubscribeIcebergCompactionEventRequest;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::{
    Event as IcebergRequestEvent, PullTask as IcebergPullTask, ReportTask as IcebergReportTask,
};
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::{
    Event as IcebergResponseEvent, PullTaskAck as IcebergPullTaskAck,
};
use rw_futures_util::pending_on_none;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot::{Receiver as OneShotReceiver, Sender};
use tokio::task::JoinHandle;
use tonic::Streaming;
use tracing::warn;

use super::init_selectors;
use crate::hummock::HummockManager;
use crate::hummock::compaction::CompactionSelector;
use crate::hummock::compactor_manager::Compactor;
use crate::hummock::error::{Error, Result};
use crate::hummock::sequence::next_compaction_task_id;
use crate::manager::MetaOpts;
use crate::manager::iceberg_compaction::IcebergCompactionManagerRef;
use crate::rpc::metrics::MetaMetrics;

const MAX_SKIP_TIMES: usize = 8;
const MAX_REPORT_COUNT: usize = 16;

#[async_trait::async_trait]
pub trait CompactionEventDispatcher: Send + Sync + 'static {
    type EventType: Send + Sync + 'static;

    async fn on_event_locally(&self, context_id: HummockContextId, event: Self::EventType) -> bool;

    async fn on_event_remotely(
        &self,
        context_id: HummockContextId,
        event: Self::EventType,
    ) -> Result<()>;

    fn should_forward(&self, event: &Self::EventType) -> bool;

    fn remove_compactor(&self, context_id: HummockContextId);
}

pub trait CompactorStreamEvent: Send + Sync + 'static {
    type EventType: Send + Sync + 'static;
    fn take_event(self) -> Self::EventType;
    fn create_at(&self) -> u64;
}

pub struct CompactionEventLoop<
    D: CompactionEventDispatcher<EventType = E::EventType>,
    E: CompactorStreamEvent,
> {
    hummock_compactor_dispatcher: D,
    metrics: Arc<MetaMetrics>,
    compactor_streams_change_rx: UnboundedReceiver<(HummockContextId, Streaming<E>)>,
}

pub type HummockCompactionEventLoop =
    CompactionEventLoop<HummockCompactionEventDispatcher, SubscribeCompactionEventRequest>;

pub type IcebergCompactionEventLoop =
    CompactionEventLoop<IcebergCompactionEventDispatcher, SubscribeIcebergCompactionEventRequest>;

pub struct HummockCompactionEventDispatcher {
    meta_opts: Arc<MetaOpts>,
    hummock_compaction_event_handler: HummockCompactionEventHandler,
    tx: Option<UnboundedSender<(HummockContextId, RequestEvent)>>,
}

#[async_trait::async_trait]
impl CompactionEventDispatcher for HummockCompactionEventDispatcher {
    type EventType = RequestEvent;

    fn should_forward(&self, event: &Self::EventType) -> bool {
        if self.tx.is_none() {
            return false;
        }

        matches!(event, RequestEvent::PullTask(_)) || matches!(event, RequestEvent::ReportTask(_))
    }

    async fn on_event_locally(&self, context_id: HummockContextId, event: Self::EventType) -> bool {
        let mut compactor_alive = true;
        match event {
            RequestEvent::HeartBeat(HeartBeat { progress }) => {
                compactor_alive = self
                    .hummock_compaction_event_handler
                    .handle_heartbeat(context_id, progress)
                    .await;
            }

            RequestEvent::Register(_event) => {
                unreachable!()
            }

            RequestEvent::PullTask(pull_task) => {
                compactor_alive = self
                    .hummock_compaction_event_handler
                    .handle_pull_task_event(
                        context_id,
                        pull_task.pull_task_count as usize,
                        &mut init_selectors(),
                        self.meta_opts.max_get_task_probe_times,
                    )
                    .await;
            }

            RequestEvent::ReportTask(report_event) => {
                if let Err(e) = self
                    .hummock_compaction_event_handler
                    .handle_report_task_event(vec![report_event.into()])
                    .await
                {
                    tracing::error!(error = %e.as_report(), "report compact_tack fail")
                }
            }
        }

        compactor_alive
    }

    async fn on_event_remotely(
        &self,
        context_id: HummockContextId,
        event: Self::EventType,
    ) -> Result<()> {
        if let Some(tx) = &self.tx {
            tx.send((context_id, event))
                .with_context(|| format!("Failed to send event to compactor {context_id}"))?;
        } else {
            unreachable!();
        }
        Ok(())
    }

    fn remove_compactor(&self, context_id: HummockContextId) {
        self.hummock_compaction_event_handler
            .hummock_manager
            .compactor_manager
            .remove_compactor(context_id);
    }
}

impl HummockCompactionEventDispatcher {
    pub fn new(
        meta_opts: Arc<MetaOpts>,
        hummock_compaction_event_handler: HummockCompactionEventHandler,
        tx: Option<UnboundedSender<(HummockContextId, RequestEvent)>>,
    ) -> Self {
        Self {
            meta_opts,
            hummock_compaction_event_handler,
            tx,
        }
    }
}

#[derive(Clone)]
pub struct HummockCompactionEventHandler {
    pub hummock_manager: Arc<HummockManager>,
}

impl HummockCompactionEventHandler {
    pub fn new(hummock_manager: Arc<HummockManager>) -> Self {
        Self { hummock_manager }
    }

    async fn handle_heartbeat(
        &self,
        context_id: HummockContextId,
        progress: Vec<CompactTaskProgress>,
    ) -> bool {
        let mut compactor_alive = true;
        let compactor_manager = self.hummock_manager.compactor_manager.clone();
        let cancel_tasks = compactor_manager
            .update_task_heartbeats(&progress)
            .into_iter()
            .map(|task| task.task_id)
            .collect::<Vec<_>>();
        if !cancel_tasks.is_empty() {
            tracing::info!(
                ?cancel_tasks,
                %context_id,
                "Tasks cancel has expired due to lack of visible progress",
            );

            if let Err(e) = self
                .hummock_manager
                .cancel_compact_tasks(cancel_tasks.clone(), TaskStatus::HeartbeatProgressCanceled)
                .await
            {
                tracing::error!(
                    error = %e.as_report(),
                    "Attempt to remove compaction task due to elapsed heartbeat failed. We will continue to track its heartbeat
                    until we can successfully report its status."
                );
            }
        }

        match compactor_manager.get_compactor(context_id) {
            Some(compactor) => {
                // Forcefully cancel the task so that it terminates
                // early on the compactor
                // node.
                if !cancel_tasks.is_empty() {
                    let _ = compactor.cancel_tasks(&cancel_tasks);
                    tracing::info!(
                        ?cancel_tasks,
                        %context_id,
                        "CancelTask operation has been sent to compactor node",
                    );
                }
            }
            _ => {
                // Determine the validity of the compactor streaming rpc. When the compactor no longer exists in the manager, the stream will be removed.
                // Tip: Connectivity to the compactor will be determined through the `send_event` operation. When send fails, it will be removed from the manager
                compactor_alive = false;
            }
        }

        compactor_alive
    }

    async fn handle_pull_task_event(
        &self,
        context_id: HummockContextId,
        pull_task_count: usize,
        compaction_selectors: &mut HashMap<TaskType, Box<dyn CompactionSelector>>,
        max_get_task_probe_times: usize,
    ) -> bool {
        assert_ne!(0, pull_task_count);
        let Some(compactor) = self
            .hummock_manager
            .compactor_manager
            .get_compactor(context_id)
        else {
            return false;
        };

        // Task selection + dispatch (any early return inside is safe — PullTaskAck below is unreachable only if we return here)
        self.try_dispatch_tasks(
            &compactor,
            pull_task_count,
            compaction_selectors,
            max_get_task_probe_times,
        )
        .await;

        // PullTaskAck: structurally guaranteed to execute after try_dispatch_tasks returns
        if let Err(e) = compactor.send_event(ResponseEvent::PullTaskAck(PullTaskAck {})) {
            tracing::warn!(
                error = %e.as_report(),
                "Failed to send ack to {}",
                context_id,
            );
            return false;
        }

        true
    }

    /// Selects and dispatches compaction tasks to a compactor.
    ///
    /// Separated from `handle_pull_task_event` so that `PullTaskAck` cannot be
    /// accidentally skipped by early returns.
    async fn try_dispatch_tasks(
        &self,
        compactor: &Arc<Compactor>,
        pull_task_count: usize,
        compaction_selectors: &mut HashMap<TaskType, Box<dyn CompactionSelector>>,
        max_get_task_probe_times: usize,
    ) {
        let snapshot = self.hummock_manager.compaction_state.snapshot();
        let Some((groups, task_type)) = snapshot.pick_compaction_groups_and_type() else {
            return;
        };

        if let TaskType::Ttl = task_type {
            match self
                .hummock_manager
                .metadata_manager
                .get_all_table_options()
                .await
                .map_err(|err| Error::MetaStore(err.into()))
            {
                Ok(table_options) => {
                    self.hummock_manager
                        .update_table_id_to_table_option(table_options);
                }
                Err(err) => {
                    warn!(error = %err.as_report(), "Failed to get table options");
                }
            }
        }

        let selector: &mut Box<dyn CompactionSelector> =
            compaction_selectors.get_mut(&task_type).unwrap();

        let mut generated_task_count = 0;
        let mut existed_groups = groups.clone();
        let mut no_task_groups: HashSet<CompactionGroupId> = HashSet::default();
        let mut failed_tasks = vec![];
        let mut loop_times = 0;

        while generated_task_count < pull_task_count
            && failed_tasks.is_empty()
            && loop_times < max_get_task_probe_times
        {
            loop_times += 1;
            let compact_ret = self
                .hummock_manager
                .get_compact_tasks(
                    existed_groups.clone(),
                    pull_task_count - generated_task_count,
                    &mut **selector,
                )
                .await;

            match compact_ret {
                Ok((compact_tasks, unschedule_groups)) => {
                    no_task_groups.extend(unschedule_groups);
                    if compact_tasks.is_empty() {
                        break;
                    }
                    generated_task_count += compact_tasks.len();
                    for task in compact_tasks {
                        let task_id = task.task_id;
                        if let Err(e) =
                            compactor.send_event(ResponseEvent::CompactTask(task.into()))
                        {
                            tracing::warn!(
                                error = %e.as_report(),
                                "Failed to send task {} to {}",
                                task_id,
                                compactor.context_id(),
                            );
                            failed_tasks.push(task_id);
                        }
                    }
                    if !failed_tasks.is_empty() {
                        self.hummock_manager
                            .compactor_manager
                            .remove_compactor(compactor.context_id());
                    }
                    existed_groups.retain(|group_id| !no_task_groups.contains(group_id));
                }
                Err(err) => {
                    tracing::warn!(error = %err.as_report(), "Failed to get compaction task");
                    break;
                }
            };
        }
        for group in no_task_groups {
            self.hummock_manager.compaction_state.unschedule(
                group,
                task_type,
                snapshot.snapshot_time(),
            );
        }
        if let Err(err) = self
            .hummock_manager
            .cancel_compact_tasks(failed_tasks, TaskStatus::SendFailCanceled)
            .await
        {
            tracing::warn!(error = %err.as_report(), "Failed to cancel compaction task");
        }
    }

    async fn handle_report_task_event(&self, report_events: Vec<ReportTask>) -> Result<()> {
        if let Err(e) = self
            .hummock_manager
            .report_compact_tasks(report_events)
            .await
        {
            tracing::error!(error = %e.as_report(), "report compact_tack fail")
        }
        Ok(())
    }
}

impl<D: CompactionEventDispatcher<EventType = E::EventType>, E: CompactorStreamEvent>
    CompactionEventLoop<D, E>
{
    pub fn new(
        hummock_compactor_dispatcher: D,
        metrics: Arc<MetaMetrics>,
        compactor_streams_change_rx: UnboundedReceiver<(HummockContextId, Streaming<E>)>,
    ) -> Self {
        Self {
            hummock_compactor_dispatcher,
            metrics,
            compactor_streams_change_rx,
        }
    }

    pub fn run(mut self) -> (JoinHandle<()>, Sender<()>) {
        let mut compactor_request_streams = FuturesUnordered::new();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let shutdown_rx_shared = shutdown_rx.shared();

        let join_handle = tokio::spawn(async move {
            // A compactor reconnect reuses the same `context_id`, so an older stream may still
            // resolve after a newer stream has already been registered. Track a local generation
            // per context to fence off stale stream results from the previous session.
            let mut stream_generations = HashMap::<HummockContextId, u64>::new();
            let push_stream =
                |context_id: HummockContextId,
                 stream_generation: u64,
                 stream: Streaming<E>,
                 compactor_request_streams: &mut FuturesUnordered<_>| {
                    let future = StreamExt::into_future(stream)
                        .map(move |stream_future| (context_id, stream_generation, stream_future));

                    compactor_request_streams.push(future);
                };

            let mut event_loop_iteration_now = Instant::now();

            loop {
                let shutdown_rx_shared = shutdown_rx_shared.clone();
                self.metrics
                    .compaction_event_loop_iteration_latency
                    .observe(event_loop_iteration_now.elapsed().as_millis() as _);
                event_loop_iteration_now = Instant::now();

                tokio::select! {
                    _ = shutdown_rx_shared => { return; },

                    compactor_stream = self.compactor_streams_change_rx.recv() => {
                        if let Some((context_id, stream)) = compactor_stream {
                            tracing::info!("compactor {} enters the cluster", context_id);
                            let stream_generation = stream_generations
                                .entry(context_id)
                                .and_modify(|generation| *generation += 1)
                                .or_insert(1);
                            push_stream(
                                context_id,
                                *stream_generation,
                                stream,
                                &mut compactor_request_streams,
                            );
                        }
                    },

                    result = pending_on_none(compactor_request_streams.next()) => {
                        let (context_id, stream_generation, compactor_stream_req): (_, _, (std::option::Option<std::result::Result<E, _>>, _)) = result;
                        let Some(current_generation) = stream_generations.get(&context_id).copied() else {
                            continue;
                        };
                        if current_generation != stream_generation {
                            // Ignore events, EOF and poll errors from superseded streams. Without
                            // this fence, a late error from an old stream could remove the current
                            // compactor session for the same `context_id`.
                            continue;
                        }
                        let (event, create_at, stream) = match compactor_stream_req {
                            (Some(Ok(req)), stream) => {
                                let create_at = req.create_at();
                                let event  = req.take_event();
                                (event, create_at, stream)
                            }

                            (Some(Err(err)), _stream) => {
                                tracing::warn!(error = %err.as_report(), %context_id, "compactor stream poll with err, recv stream may be destroyed");
                                self.hummock_compactor_dispatcher.remove_compactor(context_id);
                                continue
                            }

                            _ => {
                                // remove compactor from compactor manager
                                tracing::warn!(%context_id, "compactor stream poll err, recv stream may be destroyed");
                                self.hummock_compactor_dispatcher.remove_compactor(context_id);
                                continue
                            },
                        };

                        {
                            let consumed_latency_ms = SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .expect("Clock may have gone backwards")
                                .as_millis()
                                as u64
                            - create_at;
                            self.metrics
                                .compaction_event_consumed_latency
                                .observe(consumed_latency_ms as _);
                        }

                        let mut compactor_alive = true;
                        if self
                            .hummock_compactor_dispatcher
                            .should_forward(&event)
                        {
                            if let Err(e) = self
                                .hummock_compactor_dispatcher
                                .on_event_remotely(context_id, event)
                                .await
                            {
                                tracing::warn!(error = %e.as_report(), "Failed to forward event");
                            }
                        } else {
                            compactor_alive = self.hummock_compactor_dispatcher.on_event_locally(
                                context_id,
                                event,
                            ).await;
                        }

                        if compactor_alive {
                            push_stream(
                                context_id,
                                stream_generation,
                                stream,
                                &mut compactor_request_streams,
                            );
                        } else {
                            tracing::warn!(%context_id, "compactor stream error, send stream may be destroyed");
                            self
                            .hummock_compactor_dispatcher
                            .remove_compactor(context_id);
                        }
                    },
                }
            }
        });

        (join_handle, shutdown_tx)
    }
}

impl CompactorStreamEvent for SubscribeCompactionEventRequest {
    type EventType = RequestEvent;

    fn take_event(self) -> Self::EventType {
        self.event.unwrap()
    }

    fn create_at(&self) -> u64 {
        self.create_at
    }
}

pub struct HummockCompactorDedicatedEventLoop {
    hummock_manager: Arc<HummockManager>,
    hummock_compaction_event_handler: HummockCompactionEventHandler,
}

impl HummockCompactorDedicatedEventLoop {
    pub fn new(
        hummock_manager: Arc<HummockManager>,
        hummock_compaction_event_handler: HummockCompactionEventHandler,
    ) -> Self {
        Self {
            hummock_manager,
            hummock_compaction_event_handler,
        }
    }

    /// dedicated event runtime for CPU/IO bound event
    async fn compact_task_dedicated_event_handler(
        &self,
        mut rx: UnboundedReceiver<(HummockContextId, RequestEvent)>,
        shutdown_rx: OneShotReceiver<()>,
    ) {
        let mut compaction_selectors = init_selectors();

        tokio::select! {
            _ = shutdown_rx => {}

            _ = async {
                while let Some((context_id, event)) = rx.recv().await {
                    let mut report_events = vec![];
                    let mut skip_times = 0;
                    match event {
                        RequestEvent::PullTask(PullTask { pull_task_count }) => {
                            self.hummock_compaction_event_handler.handle_pull_task_event(context_id, pull_task_count as usize, &mut compaction_selectors, self.hummock_manager.env.opts.max_get_task_probe_times).await;
                        }

                        RequestEvent::ReportTask(task) => {
                           report_events.push(task.into());
                        }

                        _ => unreachable!(),
                    }
                    while let Ok((context_id, event)) = rx.try_recv() {
                        match event {
                            RequestEvent::PullTask(PullTask { pull_task_count }) => {
                                self.hummock_compaction_event_handler.handle_pull_task_event(context_id, pull_task_count as usize, &mut compaction_selectors, self.hummock_manager.env.opts.max_get_task_probe_times).await;
                                if !report_events.is_empty() {
                                    if skip_times > MAX_SKIP_TIMES {
                                        break;
                                    }
                                    skip_times += 1;
                                }
                            }

                            RequestEvent::ReportTask(task) => {
                                report_events.push(task.into());
                                if report_events.len() >= MAX_REPORT_COUNT {
                                    break;
                                }
                            }
                        _ => unreachable!(),
                        }
                    }
                    if !report_events.is_empty()
                        && let Err(e) = self.hummock_compaction_event_handler.handle_report_task_event(report_events).await
                        {
                            tracing::error!(error = %e.as_report(), "report compact_tack fail")
                        }
                }
            } => {}
        }
    }

    pub fn run(
        self,
    ) -> (
        JoinHandle<()>,
        UnboundedSender<(HummockContextId, RequestEvent)>,
        Sender<()>,
    ) {
        let (tx, rx) = unbounded_channel();
        let (shutdon_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handler = tokio::spawn(async move {
            self.compact_task_dedicated_event_handler(rx, shutdown_rx)
                .await;
        });
        (join_handler, tx, shutdon_tx)
    }
}

pub struct IcebergCompactionEventHandler {
    compaction_manager: IcebergCompactionManagerRef,
}

impl IcebergCompactionEventHandler {
    pub fn new(compaction_manager: IcebergCompactionManagerRef) -> Self {
        Self { compaction_manager }
    }

    async fn handle_pull_task_event(
        &self,
        context_id: HummockContextId,
        pull_task_count: usize,
    ) -> bool {
        assert_ne!(0, pull_task_count);
        if let Some(compactor) = self
            .compaction_manager
            .iceberg_compactor_manager
            .get_compactor(context_id)
        {
            let mut compactor_alive = true;

            let iceberg_compaction_handles = self
                .compaction_manager
                .get_top_n_iceberg_commit_sink_ids(pull_task_count);

            for handle in iceberg_compaction_handles {
                let compactor = compactor.clone();
                // send iceberg commit task to compactor
                if let Err(e) = async move {
                    handle
                        .send_compact_task(
                            compactor,
                            next_compaction_task_id(&self.compaction_manager.env)
                                .await?
                                .into(),
                        )
                        .await
                }
                .await
                {
                    tracing::warn!(
                        error = %e.as_report(),
                        "Failed to send iceberg commit task to {}",
                        context_id,
                    );
                    compactor_alive = false;
                }
            }

            if let Err(e) =
                compactor.send_event(IcebergResponseEvent::PullTaskAck(IcebergPullTaskAck {}))
            {
                tracing::warn!(
                    error = %e.as_report(),
                    "Failed to send ask to {}",
                    context_id,
                );
                compactor_alive = false;
            }

            return compactor_alive;
        }

        false
    }

    fn apply_report_task_event(&self, report: IcebergReportTask) {
        self.compaction_manager.handle_report_task(report);
    }
}

pub struct IcebergCompactionEventDispatcher {
    compaction_event_handler: IcebergCompactionEventHandler,
}

#[async_trait::async_trait]
impl CompactionEventDispatcher for IcebergCompactionEventDispatcher {
    type EventType = IcebergRequestEvent;

    async fn on_event_locally(&self, context_id: HummockContextId, event: Self::EventType) -> bool {
        match event {
            IcebergRequestEvent::PullTask(IcebergPullTask { pull_task_count }) => {
                return self
                    .compaction_event_handler
                    .handle_pull_task_event(context_id, pull_task_count as usize)
                    .await;
            }
            IcebergRequestEvent::ReportTask(report) => {
                self.compaction_event_handler
                    .apply_report_task_event(report);
                return true;
            }
            _ => unreachable!(),
        }
    }

    async fn on_event_remotely(
        &self,
        _context_id: HummockContextId,
        _event: Self::EventType,
    ) -> Result<()> {
        unreachable!()
    }

    fn should_forward(&self, _event: &Self::EventType) -> bool {
        false
    }

    fn remove_compactor(&self, context_id: HummockContextId) {
        self.compaction_event_handler
            .compaction_manager
            .iceberg_compactor_manager
            .remove_compactor(context_id);
    }
}

impl IcebergCompactionEventDispatcher {
    pub fn new(compaction_event_handler: IcebergCompactionEventHandler) -> Self {
        Self {
            compaction_event_handler,
        }
    }
}

impl CompactorStreamEvent for SubscribeIcebergCompactionEventRequest {
    type EventType = IcebergRequestEvent;

    fn take_event(self) -> Self::EventType {
        self.event.unwrap()
    }

    fn create_at(&self) -> u64 {
        self.create_at
    }
}
