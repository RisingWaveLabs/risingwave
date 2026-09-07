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

mod compaction_executor;
mod compaction_filter;
pub mod compaction_utils;
mod iceberg_compaction;
use risingwave_hummock_sdk::compact_task::{CompactTask, ValidationTask};
use risingwave_pb::compactor::{DispatchCompactionTaskRequest, dispatch_compaction_task_request};
use risingwave_pb::hummock::PbCompactTask;
use risingwave_pb::hummock::report_compaction_task_request::{
    Event as ReportCompactionTaskEvent, HeartBeat as SharedHeartBeat,
    ReportTask as ReportSharedTask,
};
use risingwave_pb::iceberg_compaction::{
    SubscribeIcebergCompactionEventRequest, SubscribeIcebergCompactionEventResponse,
    subscribe_iceberg_compaction_event_request,
};
use risingwave_rpc_client::GrpcCompactorProxyClient;
use thiserror_ext::AsReport;
use tokio::sync::mpsc;
use tonic::Request;

pub mod compactor_runner;
mod context;
pub mod fast_compactor_runner;
mod iterator;
mod shared_buffer_compact;
pub(super) mod task_progress;

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use await_tree::{InstrumentAwait, SpanExt};
pub use compaction_executor::CompactionExecutor;
pub use compaction_filter::{
    CompactionFilter, DummyCompactionFilter, MultiCompactionFilter, TtlCompactionFilter,
};
pub use context::{
    CompactionAwaitTreeRegRef, CompactorContext, await_tree_key, new_compaction_await_tree_reg_ref,
};
use futures::{StreamExt, pin_mut};
// Import iceberg compactor runner types from the local `iceberg_compaction` module.
use iceberg_compaction::iceberg_compactor_runner::IcebergCompactorRunnerConfigBuilder;
#[cfg(madsim)]
pub use iceberg_compaction::set_simulated_pk_index_compaction_result;
#[cfg(madsim)]
use iceberg_compaction::simulated_pk_index_compaction_result;
use iceberg_compaction::{
    IcebergPlanCompletion, IcebergTaskQueue, IcebergTaskReport, IcebergTaskTracker, PushResult,
    ReportSendResult, build_drained_iceberg_task_report, build_iceberg_task_report,
    create_task_execution, flush_pending_iceberg_task_reports, send_or_buffer_iceberg_task_report,
};
pub use iterator::{ConcatSstableIterator, SstableStreamIterator};
use more_asserts::assert_ge;
use risingwave_hummock_sdk::table_stats::{TableStatsMap, to_prost_table_stats_map};
use risingwave_hummock_sdk::{
    HummockCompactionTaskId, HummockSstableObjectId, LocalSstableInfo, compact_task_to_string,
};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::subscribe_compaction_event_request::{
    Event as RequestEvent, HeartBeat, PullTask, ReportTask,
};
use risingwave_pb::hummock::subscribe_compaction_event_response::Event as ResponseEvent;
use risingwave_pb::hummock::{
    CompactTaskProgress, PbSstableFilterLayout, PbSstableFilterType, ReportCompactionTaskRequest,
    SubscribeCompactionEventRequest, SubscribeCompactionEventResponse,
};
use risingwave_pb::id::IcebergCompactionTaskId;
use risingwave_rpc_client::HummockMetaClient;
pub use shared_buffer_compact::compact;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub use self::compaction_utils::{
    CompactionStatistics, RemoteBuilderFactory, TaskConfig, check_compaction_result,
    check_flush_result,
};
pub use self::task_progress::TaskProgress;
use super::multi_builder::CapacitySplitTableBuilder;
use super::{
    GetObjectId, HummockError, HummockErrorInner, HummockResult, ObjectIdManager,
    SstableBuilderOptions, Xor8FilterBuilder, Xor16FilterBuilder,
};
use crate::compaction_catalog_manager::{
    CompactionCatalogAgentRef, CompactionCatalogManager, CompactionCatalogManagerRef,
};
use crate::hummock::compactor::compaction_utils::calculate_task_parallelism;
use crate::hummock::compactor::compactor_runner::{compact_and_build_sst, compact_done};
use crate::hummock::compactor::iceberg_compaction::TaskKey;
use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::{
    BlockedXor8FilterBuilder, BlockedXor16FilterBuilder, FilterBuilder, NoneFilterBuilder,
    SharedComapctorObjectIdManager, SstableWriterFactory, StreamingSstableWriterFactory,
    validate_ssts,
};
use crate::monitor::CompactorMetrics;

/// Heartbeat logging interval for compaction tasks
const COMPACTION_HEARTBEAT_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Represents the compaction task state for logging purposes
#[derive(Debug, Clone, PartialEq, Eq)]
struct CompactionLogState {
    running_parallelism: u32,
    pull_task_ack: bool,
    pending_pull_task_count: u32,
}

/// Represents the iceberg compaction task state for logging purposes
#[derive(Debug, Clone, PartialEq, Eq)]
struct IcebergCompactionLogState {
    running_parallelism: u32,
    waiting_parallelism: u32,
    available_parallelism: u32,
    pull_task_ack: bool,
    pending_pull_task_count: u32,
}

/// Controls periodic logging with state change detection
struct LogThrottler<T: PartialEq> {
    last_logged_state: Option<T>,
    last_heartbeat: Instant,
    heartbeat_interval: Duration,
}

impl<T: PartialEq> LogThrottler<T> {
    fn new(heartbeat_interval: Duration) -> Self {
        Self {
            last_logged_state: None,
            last_heartbeat: Instant::now(),
            heartbeat_interval,
        }
    }

    /// Returns true if logging should occur (state changed or heartbeat interval elapsed)
    fn should_log(&self, current_state: &T) -> bool {
        self.last_logged_state.as_ref() != Some(current_state)
            || self.last_heartbeat.elapsed() >= self.heartbeat_interval
    }

    /// Updates the state and heartbeat timestamp after logging
    fn update(&mut self, current_state: T) {
        self.last_logged_state = Some(current_state);
        self.last_heartbeat = Instant::now();
    }
}

/// Implementation of Hummock compaction.
pub struct Compactor {
    /// The context of the compactor.
    context: CompactorContext,
    object_id_getter: Arc<dyn GetObjectId>,
    task_config: TaskConfig,
    options: SstableBuilderOptions,
    get_id_time: Arc<AtomicU64>,
}

pub type CompactOutput = (usize, Vec<LocalSstableInfo>, CompactionStatistics);

impl Compactor {
    /// Create a new compactor.
    pub fn new(
        context: CompactorContext,
        options: SstableBuilderOptions,
        task_config: TaskConfig,
        object_id_getter: Arc<dyn GetObjectId>,
    ) -> Self {
        Self {
            context,
            options,
            task_config,
            get_id_time: Arc::new(AtomicU64::new(0)),
            object_id_getter,
        }
    }

    /// Compact the given key range and merge iterator.
    /// Upon a successful return, the built SSTs are already uploaded to object store.
    ///
    /// `task_progress` is only used for tasks on the compactor.
    async fn compact_key_range(
        &self,
        iter: impl HummockIterator<Direction = Forward>,
        compaction_filter: impl CompactionFilter,
        compaction_catalog_agent_ref: CompactionCatalogAgentRef,
        task_progress: Option<Arc<TaskProgress>>,
        task_id: Option<HummockCompactionTaskId>,
        split_index: Option<usize>,
    ) -> HummockResult<(Vec<LocalSstableInfo>, CompactionStatistics)> {
        // Monitor time cost building shared buffer to SSTs.
        let compact_timer = if self.context.is_share_buffer_compact {
            self.context
                .compactor_metrics
                .write_build_l0_sst_duration
                .start_timer()
        } else {
            self.context
                .compactor_metrics
                .compact_sst_duration
                .start_timer()
        };

        let (split_table_outputs, table_stats_map) = {
            let factory = StreamingSstableWriterFactory::new(self.context.sstable_store.clone());
            match (
                self.task_config.sstable_filter_type,
                self.task_config.sstable_filter_layout,
            ) {
                (PbSstableFilterType::SstableFilterNone, _) => {
                    self.compact_key_range_impl::<_, NoneFilterBuilder>(
                        factory,
                        iter,
                        compaction_filter,
                        compaction_catalog_agent_ref,
                        task_progress.clone(),
                        self.object_id_getter.clone(),
                    )
                    .instrument_await("compact".verbose())
                    .await?
                }
                (PbSstableFilterType::SstableFilterXor8, PbSstableFilterLayout::Blocked) => {
                    self.compact_key_range_impl::<_, BlockedXor8FilterBuilder>(
                        factory,
                        iter,
                        compaction_filter,
                        compaction_catalog_agent_ref,
                        task_progress.clone(),
                        self.object_id_getter.clone(),
                    )
                    .instrument_await("compact".verbose())
                    .await?
                }
                (PbSstableFilterType::SstableFilterXor8, PbSstableFilterLayout::Plain) => {
                    self.compact_key_range_impl::<_, Xor8FilterBuilder>(
                        factory,
                        iter,
                        compaction_filter,
                        compaction_catalog_agent_ref,
                        task_progress.clone(),
                        self.object_id_getter.clone(),
                    )
                    .instrument_await("compact".verbose())
                    .await?
                }
                (PbSstableFilterType::SstableFilterXor16, PbSstableFilterLayout::Blocked) => {
                    self.compact_key_range_impl::<_, BlockedXor16FilterBuilder>(
                        factory,
                        iter,
                        compaction_filter,
                        compaction_catalog_agent_ref,
                        task_progress.clone(),
                        self.object_id_getter.clone(),
                    )
                    .instrument_await("compact".verbose())
                    .await?
                }
                (PbSstableFilterType::SstableFilterXor16, PbSstableFilterLayout::Plain) => {
                    self.compact_key_range_impl::<_, Xor16FilterBuilder>(
                        factory,
                        iter,
                        compaction_filter,
                        compaction_catalog_agent_ref,
                        task_progress.clone(),
                        self.object_id_getter.clone(),
                    )
                    .instrument_await("compact".verbose())
                    .await?
                }
                (filter_type, layout) => {
                    return Err(HummockError::compaction_executor(format!(
                        "unresolved SST filter layout in task config: {:?}, {:?}",
                        filter_type, layout
                    )));
                }
            }
        };

        compact_timer.observe_duration();

        Self::report_progress(
            self.context.compactor_metrics.clone(),
            task_progress,
            &split_table_outputs,
            self.context.is_share_buffer_compact,
        );

        self.context
            .compactor_metrics
            .get_table_id_total_time_duration
            .observe(self.get_id_time.load(Ordering::Relaxed) as f64 / 1000.0 / 1000.0);

        debug_assert!(
            split_table_outputs
                .iter()
                .all(|table_info| table_info.sst_info.table_ids.is_sorted())
        );

        if task_id.is_some() {
            // skip shared buffer compaction
            tracing::info!(
                "Finish Task {:?} split_index {:?} sst count {}",
                task_id,
                split_index,
                split_table_outputs.len()
            );
        }
        Ok((split_table_outputs, table_stats_map))
    }

    pub fn report_progress(
        metrics: Arc<CompactorMetrics>,
        task_progress: Option<Arc<TaskProgress>>,
        ssts: &Vec<LocalSstableInfo>,
        is_share_buffer_compact: bool,
    ) {
        for sst_info in ssts {
            let sst_size = sst_info.file_size();
            if let Some(tracker) = &task_progress {
                tracker.inc_ssts_uploaded();
                tracker.dec_num_pending_write_io();
            }
            if is_share_buffer_compact {
                metrics.shared_buffer_to_sstable_size.observe(sst_size as _);
            } else {
                metrics.compaction_upload_sst_counts.inc();
            }
        }
    }

    async fn compact_key_range_impl<F: SstableWriterFactory, B: FilterBuilder>(
        &self,
        writer_factory: F,
        iter: impl HummockIterator<Direction = Forward>,
        compaction_filter: impl CompactionFilter,
        compaction_catalog_agent_ref: CompactionCatalogAgentRef,
        task_progress: Option<Arc<TaskProgress>>,
        object_id_getter: Arc<dyn GetObjectId>,
    ) -> HummockResult<(Vec<LocalSstableInfo>, CompactionStatistics)> {
        let builder_factory = RemoteBuilderFactory::<F, B> {
            object_id_getter,
            limiter: self.context.memory_limiter.clone(),
            options: self.options.clone(),
            policy: self.task_config.cache_policy,
            remote_rpc_cost: self.get_id_time.clone(),
            compaction_catalog_agent_ref: compaction_catalog_agent_ref.clone(),
            sstable_writer_factory: writer_factory,
            _phantom: PhantomData,
        };

        let mut sst_builder = CapacitySplitTableBuilder::new(
            builder_factory,
            self.context.compactor_metrics.clone(),
            task_progress.clone(),
            self.task_config.table_vnode_partition.clone(),
            self.context
                .storage_opts
                .compactor_concurrent_uploading_sst_count,
            compaction_catalog_agent_ref,
        );
        let compaction_statistics = compact_and_build_sst(
            &mut sst_builder,
            &self.task_config,
            self.context.compactor_metrics.clone(),
            iter,
            compaction_filter,
        )
        .instrument_await("compact_and_build_sst".verbose())
        .await?;

        let ssts = sst_builder
            .finish()
            .instrument_await("builder_finish".verbose())
            .await?;

        Ok((ssts, compaction_statistics))
    }
}

/// The background compaction thread that receives compaction tasks from hummock compaction
/// manager and runs compaction tasks.
#[must_use]
pub fn start_iceberg_compactor(
    compactor_context: CompactorContext,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    iceberg_compaction_memory_limit_bytes: usize,
) -> (JoinHandle<()>, Sender<()>) {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let stream_retry_interval = Duration::from_secs(30);
    let periodic_event_update_interval = Duration::from_millis(
        compactor_context
            .storage_opts
            .iceberg_compaction_pull_interval_ms,
    );
    let worker_num = compactor_context.compaction_executor.worker_num();

    let max_task_parallelism: u32 = (worker_num as f32
        * compactor_context.storage_opts.compactor_max_task_multiplier)
        .ceil() as u32;

    const MAX_PULL_TASK_COUNT: u32 = 4;
    let max_pull_task_count = std::cmp::min(max_task_parallelism, MAX_PULL_TASK_COUNT);

    assert_ge!(
        compactor_context.storage_opts.compactor_max_task_multiplier,
        0.0
    );

    let join_handle = tokio::spawn(async move {
        // Initialize task queue with event-driven scheduling
        let pending_parallelism_budget = (max_task_parallelism as f32
            * compactor_context
                .storage_opts
                .iceberg_compaction_pending_parallelism_budget_multiplier)
            .ceil() as u32;
        let mut task_queue = IcebergTaskQueue::new_with_metrics(
            max_task_parallelism,
            pending_parallelism_budget,
            iceberg_compaction_memory_limit_bytes,
            compactor_context.compactor_metrics.clone(),
        );

        // Shutdown tracking for running tasks (task_key -> shutdown_sender)
        let shutdown_map = Arc::new(Mutex::new(HashMap::<TaskKey, Sender<()>>::new()));

        // Channel for task completion notifications
        let (task_completion_tx, mut task_completion_rx) =
            tokio::sync::mpsc::unbounded_channel::<IcebergPlanCompletion>();
        let mut task_trackers = HashMap::<IcebergCompactionTaskId, IcebergTaskTracker>::new();
        // Buffers task reports that failed to send on the current stream.
        // The queue is flushed in FIFO order after the stream reconnects.
        let mut pending_task_reports = VecDeque::<IcebergTaskReport>::new();

        let mut min_interval = tokio::time::interval(stream_retry_interval);
        let mut periodic_event_interval = tokio::time::interval(periodic_event_update_interval);

        // Track last logged state to avoid duplicate logs
        let mut log_throttler =
            LogThrottler::<IcebergCompactionLogState>::new(COMPACTION_HEARTBEAT_LOG_INTERVAL);

        // This outer loop is to recreate stream.
        'start_stream: loop {
            // reset state
            // pull_task_ack.store(true, Ordering::SeqCst);
            let mut pull_task_ack = true;
            tokio::select! {
                // Wait for interval.
                _ = min_interval.tick() => {},
                // Shutdown compactor.
                _ = &mut shutdown_rx => {
                    tracing::info!("Compactor is shutting down");
                    return;
                }
            }

            let (request_sender, response_event_stream) = match hummock_meta_client
                .subscribe_iceberg_compaction_event()
                .await
            {
                Ok((request_sender, response_event_stream)) => {
                    tracing::debug!("Succeeded subscribe_iceberg_compaction_event.");
                    (request_sender, response_event_stream)
                }

                Err(e) => {
                    tracing::warn!(
                        error = %e.as_report(),
                        "Subscribing to iceberg compaction tasks failed with error. Will retry.",
                    );
                    continue 'start_stream;
                }
            };

            if matches!(
                flush_pending_iceberg_task_reports(&request_sender, &mut pending_task_reports),
                ReportSendResult::RestartStream
            ) {
                continue 'start_stream;
            }

            pin_mut!(response_event_stream);

            let _executor = compactor_context.compaction_executor.clone();

            // This inner loop is to consume stream or report task progress.
            let mut event_loop_iteration_now = Instant::now();
            'consume_stream: loop {
                {
                    // report
                    compactor_context
                        .compactor_metrics
                        .compaction_event_loop_iteration_latency
                        .observe(event_loop_iteration_now.elapsed().as_millis() as _);
                    event_loop_iteration_now = Instant::now();
                }

                let request_sender = request_sender.clone();
                let event: Option<Result<SubscribeIcebergCompactionEventResponse, _>> = tokio::select! {
                    // Handle task completion notifications
                    Some(plan_completion) = task_completion_rx.recv() => {
                        let task_key = plan_completion.task_key;
                        let error_message = plan_completion.error_message;
                        let pk_index_result = plan_completion.pk_index_result;
                        tracing::debug!(
                            task_id = %task_key.0,
                            plan_index = task_key.1,
                            success = error_message.is_none(),
                            "Plan completed, updating queue state"
                        );
                        task_queue.finish_running(task_key);

                        let completed_task_id = task_key.0;
                        let Entry::Occupied(mut tracker_entry) =
                            task_trackers.entry(completed_task_id)
                        else {
                            continue 'consume_stream;
                        };
                        tracker_entry
                            .get_mut()
                            .record_completion(error_message, pk_index_result);
                        if !tracker_entry.get().is_finished() {
                            continue 'consume_stream;
                        }

                        let tracker = tracker_entry.remove();
                        let sink_id = tracker.sink_id();
                        let admitted_plans = tracker.admitted_plans();
                        let successful_plans = tracker.successful_plans();
                        let failed_plans = tracker.failed_plans();
                        let report = tracker.into_report(completed_task_id);
                        let task_succeeded = report.error_message.is_none();
                        if matches!(
                            send_or_buffer_iceberg_task_report(
                                &request_sender,
                                &mut pending_task_reports,
                                report,
                            ),
                            ReportSendResult::RestartStream
                        ) {
                            continue 'start_stream;
                        }
                        if task_succeeded {
                            tracing::info!(
                                iceberg_component = "compaction_worker",
                                iceberg_operation = "report_task",
                                task_id = %completed_task_id,
                                sink_id = sink_id,
                                admitted_plans = admitted_plans,
                                successful_plans = successful_plans,
                                failed_plans = failed_plans,
                                "iceberg_compaction_task_succeeded",
                            );
                        }
                        continue 'consume_stream;
                    }

                    // Event-driven task scheduling - wait for tasks to become schedulable
                    _ = task_queue.wait_schedulable() => {
                        schedule_queued_tasks(
                            &mut task_queue,
                            &compactor_context,
                            &shutdown_map,
                            &task_completion_tx,
                        );
                        continue 'consume_stream;
                    }

                    _ = periodic_event_interval.tick() => {
                        // Only handle meta task pulling in periodic tick
                        let should_restart_stream = handle_meta_task_pulling(
                            &mut pull_task_ack,
                            &task_queue,
                            max_task_parallelism,
                            max_pull_task_count,
                            &request_sender,
                            &mut log_throttler,
                        );

                        if should_restart_stream {
                            continue 'start_stream;
                        }
                        continue;
                    }
                    event = response_event_stream.next() => {
                        event
                    }

                    _ = &mut shutdown_rx => {
                        tracing::info!("Iceberg Compactor is shutting down");
                        return
                    }
                };

                match event {
                    Some(Ok(SubscribeIcebergCompactionEventResponse {
                        event,
                        create_at: _create_at,
                    })) => {
                        let event = match event {
                            Some(event) => event,
                            None => continue 'consume_stream,
                        };

                        match event {
                            risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::Event::CompactTask(iceberg_compaction_task) => {
                                let task_id = iceberg_compaction_task.task_id;
                                let sink_id = iceberg_compaction_task.sink_id;
                                let is_bounded_round = iceberg_compaction_task.max_file_sequence_number.is_some();
                                // iceberg-rust internally spawns on native Tokio, which is not
                                // available inside a madsim node. Tests can replace only the
                                // rewrite result while retaining the real compactor stream and
                                // all downstream resolver/commit/recovery behavior.
                                #[cfg(madsim)]
                                if iceberg_compaction_task.pk_index_coordinated
                                    && fail::eval(
                                        "iceberg_pk_index_simulated_compaction_report",
                                        |_| (),
                                    )
                                    .is_some()
                                    && let Some(result) = simulated_pk_index_compaction_result()
                                {
                                    let mut report = build_iceberg_task_report(task_id, sink_id, None);
                                    report.pk_index_result = Some(result);
                                    if matches!(
                                        send_or_buffer_iceberg_task_report(
                                            &request_sender,
                                            &mut pending_task_reports,
                                            report,
                                        ),
                                        ReportSendResult::RestartStream
                                    ) {
                                        continue 'start_stream;
                                    }
                                    continue 'consume_stream;
                                }
                                // Note: write_parquet_properties is now built from sink config (IcebergConfig) in create_task_execution
                                let compactor_runner_config = match IcebergCompactorRunnerConfigBuilder::default()
                                    .max_parallelism((worker_num as f32 * compactor_context.storage_opts.iceberg_compaction_task_parallelism_ratio) as u32)
                                    .min_size_per_partition(compactor_context.storage_opts.iceberg_compaction_min_size_per_partition_mb as u64 * 1024 * 1024)
                                    .max_file_count_per_partition(compactor_context.storage_opts.iceberg_compaction_max_file_count_per_partition)
                                    .enable_validate_compaction(compactor_context.storage_opts.iceberg_compaction_enable_validate)
                                    .max_record_batch_rows(compactor_context.storage_opts.iceberg_compaction_max_record_batch_rows)
                                    .enable_heuristic_output_parallelism(compactor_context.storage_opts.iceberg_compaction_enable_heuristic_output_parallelism)
                                    .max_concurrent_closes(compactor_context.storage_opts.iceberg_compaction_max_concurrent_closes)
                                    .enable_prefetch(compactor_context.storage_opts.iceberg_compaction_enable_prefetch)
                                    .target_binpack_group_size_mb(
                                        compactor_context.storage_opts.iceberg_compaction_target_binpack_group_size_mb
                                    )
                                    .min_group_size_mb(
                                        compactor_context.storage_opts.iceberg_compaction_min_group_size_mb
                                    )
                                    .min_group_file_count(
                                        compactor_context.storage_opts.iceberg_compaction_min_group_file_count
                                    )
                                    .build() {
                                    Ok(config) => config,
                                    Err(e) => {
                                        tracing::warn!(
                                            iceberg_component = "compaction_worker",
                                            iceberg_operation = "build_runner_config",
                                            error = %e.as_report(),
                                            task_id = %task_id,
                                            sink_id = sink_id,
                                            "iceberg_compaction_runner_config_failed",
                                        );
                                        let report = build_iceberg_task_report(
                                            task_id,
                                            sink_id,
                                            Some(format!(
                                                "Failed to build iceberg compactor runner config: {}",
                                                e.as_report()
                                            )),
                                        );
                                        if matches!(
                                            send_or_buffer_iceberg_task_report(
                                                &request_sender,
                                                &mut pending_task_reports,
                                                report,
                                            ),
                                            ReportSendResult::RestartStream
                                        ) {
                                            continue 'start_stream;
                                        }
                                        continue 'consume_stream;
                                    }
                                };

                                // Create task execution context and plan runners from the task
                                let task_execution = match create_task_execution(
                                    iceberg_compaction_task,
                                    compactor_runner_config,
                                    compactor_context.compactor_metrics.clone(),
                                ).await {
                                    Ok(task_execution) => task_execution,
                                    Err(e) => {
                                        tracing::warn!(
                                            iceberg_component = "compaction_worker",
                                            iceberg_operation = "plan_task",
                                            error = %e.as_report(),
                                            task_id = %task_id,
                                            sink_id = sink_id,
                                            "iceberg_compaction_task_plan_failed",
                                        );
                                        let report = build_iceberg_task_report(
                                            task_id,
                                            sink_id,
                                            Some(format!(
                                                "Failed to create iceberg compaction task execution: {}",
                                                e.as_report()
                                            )),
                                        );
                                        if matches!(
                                            send_or_buffer_iceberg_task_report(
                                                &request_sender,
                                                &mut pending_task_reports,
                                                report,
                                            ),
                                            ReportSendResult::RestartStream
                                        ) {
                                            continue 'start_stream;
                                        }
                                        continue 'consume_stream;
                                    }
                                };

                                let sink_id = task_execution.sink_id;
                                let plan_runners = task_execution.plan_runners;

                                if plan_runners.is_empty() {
                                    // For a bounded round, an empty plan is the terminal proof.
                                    tracing::info!(
                                        iceberg_component = "compaction_worker",
                                        iceberg_operation = "enqueue_plan",
                                        task_id = %task_id,
                                        sink_id = sink_id,
                                        "iceberg_compaction_task_skipped_no_plans",
                                    );
                                    let report = if is_bounded_round {
                                        build_drained_iceberg_task_report(task_id, sink_id)
                                    } else {
                                        build_iceberg_task_report(task_id, sink_id, None)
                                    };
                                    if matches!(
                                        send_or_buffer_iceberg_task_report(
                                            &request_sender,
                                            &mut pending_task_reports,
                                            report,
                                        ),
                                        ReportSendResult::RestartStream
                                    ) {
                                        continue 'start_stream;
                                    }
                                    continue 'consume_stream;
                                }

                                // Enqueue each plan runner independently
                                let planned_count = plan_runners.len();
                                let mut admitted_count = 0;
                                let mut rejection_reason = None;

                                for runner in plan_runners {
                                    let meta = runner.to_meta();
                                    let plan_index = meta.plan_index;
                                    let required_parallelism = runner.required_parallelism();
                                    let memory_reservation_bytes = meta.memory_reservation_bytes;
                                    let runner_sink_id = runner.sink_id;
                                    let runner_compaction_kind = runner.compaction_kind();
                                    let runner_table = runner.table_ident.to_string();
                                    let push_result = task_queue.push(meta, Some(runner));

                                    match push_result {
                                        PushResult::Added => {
                                            admitted_count += 1;
                                            tracing::debug!(
                                                iceberg_component = "compaction_worker",
                                                iceberg_operation = "enqueue_plan",
                                                task_id = %task_id,
                                                sink_id = runner_sink_id,
                                                plan_index = plan_index,
                                                task_type = ?runner_compaction_kind,
                                                table = %runner_table,
                                                required_parallelism = required_parallelism,
                                                "iceberg_compaction_plan_enqueued",
                                            );
                                        },
                                        PushResult::RejectedCapacity => {
                                            tracing::warn!(
                                                iceberg_component = "compaction_worker",
                                                iceberg_operation = "enqueue_plan",
                                                task_id = %task_id,
                                                sink_id = runner_sink_id,
                                                plan_index = plan_index,
                                                task_type = ?runner_compaction_kind,
                                                table = %runner_table,
                                                required_parallelism = required_parallelism,
                                                pending_budget = pending_parallelism_budget,
                                                admitted_count = admitted_count,
                                                planned_count = planned_count,
                                                "iceberg_compaction_plan_rejected_capacity",
                                            );
                                            // Stop enqueuing remaining plans
                                            break;
                                        },
                                        PushResult::RejectedTooLarge => {
                                            rejection_reason.get_or_insert_with(|| {
                                                format!(
                                                    "Iceberg compaction plan {plan_index} requires parallelism {required_parallelism} and an estimated {memory_reservation_bytes} bytes of memory, exceeding worker capacities of {max_task_parallelism} parallelism and {iceberg_compaction_memory_limit_bytes} bytes"
                                                )
                                            });
                                            tracing::error!(
                                                iceberg_component = "compaction_worker",
                                                iceberg_operation = "enqueue_plan",
                                                task_id = %task_id,
                                                sink_id = runner_sink_id,
                                                plan_index = plan_index,
                                                task_type = ?runner_compaction_kind,
                                                table = %runner_table,
                                                required_parallelism = required_parallelism,
                                                max_parallelism = max_task_parallelism,
                                                memory_reservation_bytes,
                                                memory_budget_bytes = iceberg_compaction_memory_limit_bytes,
                                                "iceberg_compaction_plan_rejected_too_large",
                                            );
                                        },
                                        PushResult::RejectedInvalidParallelism => {
                                            tracing::error!(
                                                iceberg_component = "compaction_worker",
                                                iceberg_operation = "enqueue_plan",
                                                task_id = %task_id,
                                                sink_id = runner_sink_id,
                                                plan_index = plan_index,
                                                task_type = ?runner_compaction_kind,
                                                table = %runner_table,
                                                required_parallelism = required_parallelism,
                                                "iceberg_compaction_plan_rejected_invalid_parallelism",
                                            );
                                        },
                                        PushResult::RejectedDuplicate => {
                                            tracing::error!(
                                                iceberg_component = "compaction_worker",
                                                iceberg_operation = "enqueue_plan",
                                                task_id = %task_id,
                                                sink_id = runner_sink_id,
                                                plan_index = plan_index,
                                                task_type = ?runner_compaction_kind,
                                                table = %runner_table,
                                                "iceberg_compaction_plan_rejected_duplicate",
                                            );
                                        }
                                    }
                                }

                                if admitted_count == 0 {
                                    let report = build_iceberg_task_report(
                                        task_id,
                                        sink_id,
                                        Some(rejection_reason.unwrap_or_else(|| {
                                            "Failed to enqueue all iceberg compaction plans".to_owned()
                                        })),
                                    );
                                    if matches!(
                                        send_or_buffer_iceberg_task_report(
                                            &request_sender,
                                            &mut pending_task_reports,
                                            report,
                                        ),
                                        ReportSendResult::RestartStream
                                    ) {
                                        continue 'start_stream;
                                    }
                                } else {
                                    // A bounded round can drain with this task only if every planned
                                    // runner was admitted. Otherwise an immediate re-plan
                                    // rediscovers the deferred work.
                                    let fully_admitted_bounded_round =
                                        is_bounded_round && admitted_count == planned_count;
                                    task_trackers.insert(
                                        task_id,
                                        IcebergTaskTracker::new(
                                            sink_id,
                                            admitted_count,
                                            fully_admitted_bounded_round,
                                        ),
                                    );
                                }

                                tracing::info!(
                                    iceberg_component = "compaction_worker",
                                    iceberg_operation = "enqueue_plan",
                                    task_id = %task_id,
                                    sink_id = sink_id,
                                    planned_count = planned_count,
                                    admitted_count = admitted_count,
                                    "iceberg_compaction_task_enqueue_finished",
                                );
                            },
                            risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::Event::PullTaskAck(_) => {
                                // set flag
                                pull_task_ack = true;
                            },
                            risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::Event::CancelCompactTask(cancel_compact_task) => {
                                cancel_iceberg_task(
                                    cancel_compact_task.task_id,
                                    &mut task_queue,
                                    &shutdown_map,
                                    &mut task_trackers,
                                );
                            },
                        }
                    }
                    Some(Err(e)) => {
                        tracing::warn!("Failed to consume stream. {}", e.message());
                        continue 'start_stream;
                    }
                    _ => {
                        // The stream is exhausted
                        continue 'start_stream;
                    }
                }
            }
        }
    });

    (join_handle, shutdown_tx)
}

/// The background compaction thread that receives compaction tasks from hummock compaction
/// manager and runs compaction tasks.
#[must_use]
pub fn start_compactor(
    compactor_context: CompactorContext,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    object_id_manager: Arc<ObjectIdManager>,
    compaction_catalog_manager_ref: CompactionCatalogManagerRef,
) -> (JoinHandle<()>, Sender<()>) {
    type CompactionShutdownMap = Arc<Mutex<HashMap<u64, Sender<()>>>>;
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let stream_retry_interval = Duration::from_secs(30);
    let task_progress = compactor_context.task_progress_manager.clone();
    let periodic_event_update_interval = Duration::from_millis(1000);

    let max_task_parallelism: u32 = (compactor_context.compaction_executor.worker_num() as f32
        * compactor_context.storage_opts.compactor_max_task_multiplier)
        .ceil() as u32;
    let running_task_parallelism = Arc::new(AtomicU32::new(0));

    const MAX_PULL_TASK_COUNT: u32 = 4;
    let max_pull_task_count = std::cmp::min(max_task_parallelism, MAX_PULL_TASK_COUNT);

    assert_ge!(
        compactor_context.storage_opts.compactor_max_task_multiplier,
        0.0
    );

    let join_handle = tokio::spawn(async move {
        let shutdown_map = CompactionShutdownMap::default();
        let mut min_interval = tokio::time::interval(stream_retry_interval);
        let mut periodic_event_interval = tokio::time::interval(periodic_event_update_interval);

        // Track last logged state to avoid duplicate logs
        let mut log_throttler =
            LogThrottler::<CompactionLogState>::new(COMPACTION_HEARTBEAT_LOG_INTERVAL);

        // This outer loop is to recreate stream.
        'start_stream: loop {
            // reset state
            // pull_task_ack.store(true, Ordering::SeqCst);
            let mut pull_task_ack = true;
            tokio::select! {
                // Wait for interval.
                _ = min_interval.tick() => {},
                // Shutdown compactor.
                _ = &mut shutdown_rx => {
                    tracing::info!("Compactor is shutting down");
                    return;
                }
            }

            let (request_sender, response_event_stream) =
                match hummock_meta_client.subscribe_compaction_event().await {
                    Ok((request_sender, response_event_stream)) => {
                        tracing::debug!("Succeeded subscribe_compaction_event.");
                        (request_sender, response_event_stream)
                    }

                    Err(e) => {
                        tracing::warn!(
                            error = %e.as_report(),
                            "Subscribing to compaction tasks failed with error. Will retry.",
                        );
                        continue 'start_stream;
                    }
                };

            pin_mut!(response_event_stream);

            let executor = compactor_context.compaction_executor.clone();
            let object_id_manager = object_id_manager.clone();

            // This inner loop is to consume stream or report task progress.
            let mut event_loop_iteration_now = Instant::now();
            'consume_stream: loop {
                {
                    // report
                    compactor_context
                        .compactor_metrics
                        .compaction_event_loop_iteration_latency
                        .observe(event_loop_iteration_now.elapsed().as_millis() as _);
                    event_loop_iteration_now = Instant::now();
                }

                let running_task_parallelism = running_task_parallelism.clone();
                let request_sender = request_sender.clone();
                let event: Option<Result<SubscribeCompactionEventResponse, _>> = tokio::select! {
                    _ = periodic_event_interval.tick() => {
                        let progress_list = get_task_progress(task_progress.clone());

                        if let Err(e) = request_sender.send(SubscribeCompactionEventRequest {
                            event: Some(RequestEvent::HeartBeat(
                                HeartBeat {
                                    progress: progress_list
                                }
                            )),
                            create_at: SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .expect("Clock may have gone backwards")
                                .as_millis() as u64,
                        }) {
                            tracing::warn!(error = %e.as_report(), "Failed to report task progress");
                            // re subscribe stream
                            continue 'start_stream;
                        }


                        let mut pending_pull_task_count = 0;
                        if pull_task_ack {
                            // TODO: Compute parallelism on meta side
                            pending_pull_task_count = (max_task_parallelism - running_task_parallelism.load(Ordering::SeqCst)).min(max_pull_task_count);

                            if pending_pull_task_count > 0 {
                                if let Err(e) = request_sender.send(SubscribeCompactionEventRequest {
                                    event: Some(RequestEvent::PullTask(
                                        PullTask {
                                            pull_task_count: pending_pull_task_count,
                                        }
                                    )),
                                    create_at: SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .expect("Clock may have gone backwards")
                                        .as_millis() as u64,
                                }) {
                                    tracing::warn!(error = %e.as_report(), "Failed to pull task");

                                    // re subscribe stream
                                    continue 'start_stream;
                                } else {
                                    pull_task_ack = false;
                                }
                            }
                        }

                        let running_count = running_task_parallelism.load(Ordering::SeqCst);
                        let current_state = CompactionLogState {
                            running_parallelism: running_count,
                            pull_task_ack,
                            pending_pull_task_count,
                        };

                        // Log only when state changes or periodically as heartbeat
                        if log_throttler.should_log(&current_state) {
                            tracing::info!(
                                running_parallelism_count = %current_state.running_parallelism,
                                pull_task_ack = %current_state.pull_task_ack,
                                pending_pull_task_count = %current_state.pending_pull_task_count
                            );
                            log_throttler.update(current_state);
                        }

                        continue;
                    }
                    event = response_event_stream.next() => {
                        event
                    }

                    _ = &mut shutdown_rx => {
                        tracing::info!("Compactor is shutting down");
                        return
                    }
                };

                fn send_report_task_event(
                    compact_task: &CompactTask,
                    table_stats: TableStatsMap,
                    object_timestamps: HashMap<HummockSstableObjectId, u64>,
                    request_sender: &mpsc::UnboundedSender<SubscribeCompactionEventRequest>,
                ) {
                    if let Err(e) = request_sender.send(SubscribeCompactionEventRequest {
                        event: Some(RequestEvent::ReportTask(ReportTask {
                            task_id: compact_task.task_id,
                            task_status: compact_task.task_status.into(),
                            sorted_output_ssts: compact_task
                                .sorted_output_ssts
                                .iter()
                                .map(|sst| sst.into())
                                .collect(),
                            table_stats_change: to_prost_table_stats_map(table_stats),
                            object_timestamps,
                        })),
                        create_at: SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .expect("Clock may have gone backwards")
                            .as_millis() as u64,
                    }) {
                        let task_id = compact_task.task_id;
                        tracing::warn!(error = %e.as_report(), "Failed to report task {task_id:?}");
                    }
                }

                match event {
                    Some(Ok(SubscribeCompactionEventResponse { event, create_at })) => {
                        let event = match event {
                            Some(event) => event,
                            None => continue 'consume_stream,
                        };
                        let shutdown = shutdown_map.clone();
                        let context = compactor_context.clone();
                        let consumed_latency_ms = SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .expect("Clock may have gone backwards")
                            .as_millis() as u64
                            - create_at;
                        context
                            .compactor_metrics
                            .compaction_event_consumed_latency
                            .observe(consumed_latency_ms as _);

                        let object_id_manager = object_id_manager.clone();
                        let compaction_catalog_manager_ref = compaction_catalog_manager_ref.clone();

                        match event {
                            ResponseEvent::CompactTask(compact_task) => {
                                let compact_task = CompactTask::from(compact_task);
                                let parallelism =
                                    calculate_task_parallelism(&compact_task, &context);

                                assert_ne!(parallelism, 0, "splits cannot be empty");

                                if (max_task_parallelism
                                    - running_task_parallelism.load(Ordering::SeqCst))
                                    < parallelism as u32
                                {
                                    tracing::warn!(
                                        "Not enough core parallelism to serve the task {} task_parallelism {} running_task_parallelism {} max_task_parallelism {}",
                                        compact_task.task_id,
                                        parallelism,
                                        max_task_parallelism,
                                        running_task_parallelism.load(Ordering::Relaxed),
                                    );
                                    let (compact_task, table_stats, object_timestamps) =
                                        compact_done(
                                            compact_task,
                                            context.clone(),
                                            vec![],
                                            TaskStatus::NoAvailCpuResourceCanceled,
                                        );

                                    send_report_task_event(
                                        &compact_task,
                                        table_stats,
                                        object_timestamps,
                                        &request_sender,
                                    );

                                    continue 'consume_stream;
                                }

                                running_task_parallelism
                                    .fetch_add(parallelism as u32, Ordering::SeqCst);
                                executor.spawn(async move {
                                    let (tx, rx) = tokio::sync::oneshot::channel();
                                    let task_id = compact_task.task_id;
                                    shutdown.lock().unwrap().insert(task_id, tx);

                                    let (
                                        (compact_task, table_stats, object_timestamps),
                                        _memory_tracker,
                                    ) = compactor_runner::compact(
                                        context.clone(),
                                        compact_task,
                                        rx,
                                        object_id_manager.clone(),
                                        compaction_catalog_manager_ref.clone(),
                                    )
                                    .await;

                                    shutdown.lock().unwrap().remove(&task_id);
                                    running_task_parallelism
                                        .fetch_sub(parallelism as u32, Ordering::SeqCst);

                                    send_report_task_event(
                                        &compact_task,
                                        table_stats,
                                        object_timestamps,
                                        &request_sender,
                                    );

                                    let enable_check_compaction_result =
                                        context.storage_opts.check_compaction_result;
                                    let need_check_task =
                                        !compact_task.sorted_output_ssts.is_empty()
                                            && compact_task.task_status == TaskStatus::Success;

                                    if enable_check_compaction_result && need_check_task {
                                        let read_table_ids = compact_task
                                            .get_table_ids_from_input_ssts()
                                            .collect::<Vec<_>>();
                                        match compaction_catalog_manager_ref.acquire(read_table_ids).await {
                                            Ok(compaction_catalog_agent_ref) =>  {
                                                match check_compaction_result(&compact_task, context.clone(), compaction_catalog_agent_ref).await
                                                {
                                                    Err(e) => {
                                                        tracing::warn!(error = %e.as_report(), "Failed to check compaction task {}",compact_task.task_id);
                                                    }
                                                    Ok(true) => (),
                                                    Ok(false) => {
                                                        panic!("Failed to pass consistency check for result of compaction task:\n{:?}", compact_task_to_string(&compact_task));
                                                    }
                                                }
                                            },
                                            Err(e) => {
                                                tracing::warn!(error = %e.as_report(), "failed to acquire compaction catalog agent");
                                            }
                                        }
                                    }
                                });
                            }
                            #[expect(deprecated)]
                            ResponseEvent::VacuumTask(_) => {
                                unreachable!("unexpected vacuum task");
                            }
                            #[expect(deprecated)]
                            ResponseEvent::FullScanTask(_) => {
                                unreachable!("unexpected scan task");
                            }
                            #[expect(deprecated)]
                            ResponseEvent::ValidationTask(validation_task) => {
                                let validation_task = ValidationTask::from(validation_task);
                                executor.spawn(async move {
                                    validate_ssts(validation_task, context.sstable_store.clone())
                                        .await;
                                });
                            }
                            ResponseEvent::CancelCompactTask(cancel_compact_task) => match shutdown
                                .lock()
                                .unwrap()
                                .remove(&cancel_compact_task.task_id)
                            {
                                Some(tx) => {
                                    if tx.send(()).is_err() {
                                        tracing::warn!(
                                            "Cancellation of compaction task failed. task_id: {}",
                                            cancel_compact_task.task_id
                                        );
                                    }
                                }
                                _ => {
                                    tracing::warn!(
                                        "Attempting to cancel non-existent compaction task. task_id: {}",
                                        cancel_compact_task.task_id
                                    );
                                }
                            },

                            ResponseEvent::PullTaskAck(_pull_task_ack) => {
                                // set flag
                                pull_task_ack = true;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        tracing::warn!("Failed to consume stream. {}", e.message());
                        continue 'start_stream;
                    }
                    _ => {
                        // The stream is exhausted
                        continue 'start_stream;
                    }
                }
            }
        }
    });

    (join_handle, shutdown_tx)
}

/// The background compaction thread that receives compaction tasks from hummock compaction
/// manager and runs compaction tasks.
#[must_use]
pub fn start_shared_compactor(
    grpc_proxy_client: GrpcCompactorProxyClient,
    mut receiver: mpsc::UnboundedReceiver<Request<DispatchCompactionTaskRequest>>,
    context: CompactorContext,
) -> (JoinHandle<()>, Sender<()>) {
    type CompactionShutdownMap = Arc<Mutex<HashMap<u64, Sender<()>>>>;
    let task_progress = context.task_progress_manager.clone();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let periodic_event_update_interval = Duration::from_millis(1000);

    let join_handle = tokio::spawn(async move {
        let shutdown_map = CompactionShutdownMap::default();

        let mut periodic_event_interval = tokio::time::interval(periodic_event_update_interval);
        let executor = context.compaction_executor.clone();
        let report_heartbeat_client = grpc_proxy_client.clone();
        'consume_stream: loop {
            let request: Option<Request<DispatchCompactionTaskRequest>> = tokio::select! {
                _ = periodic_event_interval.tick() => {
                    let progress_list = get_task_progress(task_progress.clone());
                    let report_compaction_task_request = ReportCompactionTaskRequest{
                        event: Some(ReportCompactionTaskEvent::HeartBeat(
                            SharedHeartBeat {
                                progress: progress_list
                            }
                        )),
                     };
                    if let Err(e) = report_heartbeat_client.report_compaction_task(report_compaction_task_request).await{
                        tracing::warn!(error = %e.as_report(), "Failed to report heartbeat");
                    }
                    continue
                }


                _ = &mut shutdown_rx => {
                    tracing::info!("Compactor is shutting down");
                    return
                }

                request = receiver.recv() => {
                    request
                }

            };
            match request {
                Some(request) => {
                    let context = context.clone();
                    let shutdown = shutdown_map.clone();

                    let cloned_grpc_proxy_client = grpc_proxy_client.clone();
                    executor.spawn(async move {
                        let DispatchCompactionTaskRequest {
                            tables,
                            output_object_ids,
                            task: dispatch_task,
                        } = request.into_inner();
                        let table_id_to_catalog = tables.into_iter().fold(HashMap::new(), |mut acc, table| {
                            acc.insert(table.id, table);
                            acc
                        });

                        let mut output_object_ids_deque: VecDeque<_> = VecDeque::new();
                        output_object_ids_deque.extend(output_object_ids.into_iter().map(Into::<HummockSstableObjectId>::into));
                        let shared_compactor_object_id_manager =
                            SharedComapctorObjectIdManager::new(output_object_ids_deque, cloned_grpc_proxy_client.clone(), context.storage_opts.sstable_id_remote_fetch_number);
                            match dispatch_task.unwrap() {
                                dispatch_compaction_task_request::Task::CompactTask(compact_task) => {
                                    let compact_task = CompactTask::from(&compact_task);
                                    let (tx, rx) = tokio::sync::oneshot::channel();
                                    let task_id = compact_task.task_id;
                                    shutdown.lock().unwrap().insert(task_id, tx);

                                    let compaction_catalog_manager_ref =
                                        Arc::new(CompactionCatalogManager::new_preloaded(
                                            table_id_to_catalog,
                                        ));
                                    let ((compact_task, table_stats, object_timestamps), _memory_tracker)= compactor_runner::compact(
                                        context.clone(),
                                        compact_task,
                                        rx,
                                        shared_compactor_object_id_manager,
                                        compaction_catalog_manager_ref.clone(),
                                    )
                                    .await;
                                    shutdown.lock().unwrap().remove(&task_id);
                                    let report_compaction_task_request = ReportCompactionTaskRequest {
                                        event: Some(ReportCompactionTaskEvent::ReportTask(ReportSharedTask {
                                            compact_task: Some(PbCompactTask::from(&compact_task)),
                                            table_stats_change: to_prost_table_stats_map(table_stats),
                                            object_timestamps,
                                    })),
                                    };

                                    match cloned_grpc_proxy_client
                                        .report_compaction_task(report_compaction_task_request)
                                        .await
                                    {
                                        Ok(_) => {
                                            // TODO: remove this method after we have running risingwave cluster with fast compact algorithm stably for a long time.
                                            let enable_check_compaction_result =
                                                context.storage_opts.check_compaction_result;
                                            let need_check_task =
                                                !compact_task.sorted_output_ssts.is_empty()
                                                    && compact_task.task_status
                                                        == TaskStatus::Success;
                                            if enable_check_compaction_result && need_check_task {
                                                let read_table_ids = compact_task
                                                    .get_table_ids_from_input_ssts()
                                                    .collect::<Vec<_>>();
                                                match compaction_catalog_manager_ref.acquire(read_table_ids).await {
                                                    Ok(compaction_catalog_agent_ref) => {
                                                        match check_compaction_result(&compact_task, context.clone(), compaction_catalog_agent_ref).await
                                                        {
                                                            Err(e) => {
                                                                tracing::warn!(error = %e.as_report(), "Failed to check compaction task {}", task_id);
                                                            }
                                                            Ok(true) => (),
                                                            Ok(false) => {
                                                                panic!("Failed to pass consistency check for result of compaction task:\n{:?}", compact_task_to_string(&compact_task));
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        tracing::warn!(error = %e.as_report(), "failed to acquire compaction catalog agent");
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => tracing::warn!(error = %e.as_report(), "Failed to report task {task_id:?}"),
                                    }

                                }
                                dispatch_compaction_task_request::Task::VacuumTask(_) => {
                                    unreachable!("unexpected vacuum task");
                                }
                                dispatch_compaction_task_request::Task::FullScanTask(_) => {
                                    unreachable!("unexpected scan task");
                                }
                                dispatch_compaction_task_request::Task::ValidationTask(validation_task) => {
                                    let validation_task = ValidationTask::from(validation_task);
                                    validate_ssts(validation_task, context.sstable_store.clone()).await;
                                }
                                dispatch_compaction_task_request::Task::CancelCompactTask(cancel_compact_task) => {
                                    match shutdown
                                        .lock()
                                        .unwrap()
                                        .remove(&cancel_compact_task.task_id)
                                    { Some(tx) => {
                                        if tx.send(()).is_err() {
                                            tracing::warn!(
                                                "Cancellation of compaction task failed. task_id: {}",
                                                cancel_compact_task.task_id
                                            );
                                        }
                                    } _ => {
                                        tracing::warn!(
                                            "Attempting to cancel non-existent compaction task. task_id: {}",
                                            cancel_compact_task.task_id
                                        );
                                    }}
                                }
                            }
                    });
                }
                None => continue 'consume_stream,
            }
        }
    });
    (join_handle, shutdown_tx)
}

fn get_task_progress(
    task_progress: Arc<
        parking_lot::lock_api::Mutex<parking_lot::RawMutex, HashMap<u64, Arc<TaskProgress>>>,
    >,
) -> Vec<CompactTaskProgress> {
    let mut progress_list = Vec::new();
    for (&task_id, progress) in &*task_progress.lock() {
        progress_list.push(progress.snapshot(task_id));
    }
    progress_list
}

/// Schedule queued tasks if we have capacity
fn schedule_queued_tasks(
    task_queue: &mut IcebergTaskQueue,
    compactor_context: &CompactorContext,
    shutdown_map: &Arc<Mutex<HashMap<TaskKey, Sender<()>>>>,
    task_completion_tx: &tokio::sync::mpsc::UnboundedSender<IcebergPlanCompletion>,
) {
    while let Some(popped_task) = task_queue.pop() {
        let task_id = popped_task.meta.task_id;
        let plan_index = popped_task.meta.plan_index;
        let task_key = (task_id, plan_index);

        // Get unique_ident before moving runner
        let unique_ident = popped_task.runner.as_ref().map(|r| r.unique_ident());

        let Some(runner) = popped_task.runner else {
            tracing::error!(
                iceberg_component = "compaction_worker",
                iceberg_operation = "schedule_plan",
                task_id = %task_id,
                plan_index = plan_index,
                "iceberg_compaction_plan_missing_runner",
            );
            task_queue.finish_running(task_key);
            continue;
        };
        let runner_sink_id = runner.sink_id;
        let runner_compaction_kind = runner.compaction_kind();
        let runner_table = runner.table_ident.to_string();

        let executor = compactor_context.compaction_executor.clone();
        let shutdown_map_clone = shutdown_map.clone();
        let completion_tx_clone = task_completion_tx.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();

        {
            let mut shutdown_guard = shutdown_map.lock().unwrap();
            shutdown_guard.insert(task_key, tx);
        }

        tracing::info!(
            iceberg_component = "compaction_worker",
            iceberg_operation = "schedule_plan",
            task_id = %task_id,
            sink_id = runner_sink_id,
            plan_index = plan_index,
            task_type = ?runner_compaction_kind,
            table = %runner_table,
            unique_ident = ?unique_ident,
            required_parallelism = popped_task.meta.required_parallelism,
            "iceberg_compaction_plan_started_from_queue",
        );

        executor.spawn(async move {
            let _cleanup_guard = scopeguard::guard(shutdown_map_clone, move |shutdown_map| {
                let mut shutdown_guard = shutdown_map.lock().unwrap();
                shutdown_guard.remove(&task_key);
            });

            let result = Box::pin(runner.compact(rx)).await;

            let completion = match result {
                Ok((_stats, pk_index_result)) => IcebergPlanCompletion {
                    task_key,
                    error_message: None,
                    pk_index_result,
                },
                Err(e) => {
                    if is_cancelled_iceberg_compaction_error(&e) {
                        tracing::info!(
                            iceberg_component = "compaction_worker",
                            iceberg_operation = "execute_plan",
                            task_id = %task_key.0,
                            sink_id = runner_sink_id,
                            plan_index = task_key.1,
                            task_type = ?runner_compaction_kind,
                            table = %runner_table,
                            "iceberg_compaction_plan_cancelled",
                        );
                    } else {
                        tracing::warn!(
                            iceberg_component = "compaction_worker",
                            iceberg_operation = "execute_plan",
                            error = %e.as_report(),
                            task_id = %task_key.0,
                            sink_id = runner_sink_id,
                            plan_index = task_key.1,
                            task_type = ?runner_compaction_kind,
                            table = %runner_table,
                            "iceberg_compaction_plan_failed",
                        );
                    }
                    IcebergPlanCompletion {
                        task_key,
                        error_message: Some(e.to_report_string()),
                        pk_index_result: None,
                    }
                }
            };

            if completion_tx_clone.send(completion).is_err() {
                tracing::warn!(
                    iceberg_component = "compaction_worker",
                    iceberg_operation = "notify_plan_completion",
                    task_id = %task_key.0,
                    sink_id = runner_sink_id,
                    plan_index = task_key.1,
                    task_type = ?runner_compaction_kind,
                    table = %runner_table,
                    "iceberg_compaction_plan_completion_send_failed",
                );
            }
        });
    }
}

fn is_cancelled_iceberg_compaction_error(error: &crate::hummock::HummockError) -> bool {
    matches!(
        error.inner(),
        HummockErrorInner::CompactionExecutor(message) if message == "Plan cancelled"
    )
}

fn cancel_iceberg_task(
    task_id: IcebergCompactionTaskId,
    task_queue: &mut IcebergTaskQueue,
    shutdown_map: &Arc<Mutex<HashMap<TaskKey, Sender<()>>>>,
    task_trackers: &mut HashMap<IcebergCompactionTaskId, IcebergTaskTracker>,
) {
    // Meta assigns one task id to an Iceberg compact task, but the compactor
    // splits it into multiple plan runners tracked by `(task_id, plan_index)`.
    // A cancel event only carries `task_id`, so it must cancel all waiting and
    // running plan runners that belong to that task.
    let cancelled_waiting = task_queue.cancel_waiting_task(task_id);
    let removed_tracker = task_trackers.remove(&task_id).is_some();

    let cancelled_running = {
        let mut shutdown_guard = shutdown_map.lock().unwrap();
        let task_keys: Vec<_> = shutdown_guard
            .keys()
            .filter(|(running_task_id, _)| *running_task_id == task_id)
            .copied()
            .collect();

        for task_key in &task_keys {
            if let Some(tx) = shutdown_guard.remove(task_key)
                && tx.send(()).is_err()
            {
                tracing::debug!(
                    task_id = %task_key.0,
                    plan_index = task_key.1,
                    "Iceberg compaction plan shutdown receiver already closed during cancellation"
                );
            }
        }

        task_keys.len()
    };

    if cancelled_waiting == 0 && cancelled_running == 0 && !removed_tracker {
        tracing::warn!(
            task_id = %task_id,
            "Attempting to cancel non-existent iceberg compaction task"
        );
    } else {
        tracing::info!(
            task_id = %task_id,
            cancelled_waiting = cancelled_waiting,
            cancelled_running = cancelled_running,
            removed_tracker = removed_tracker,
            "Cancelled iceberg compaction task"
        );
    }
}

/// Handle pulling new tasks from meta service
/// Returns true if the stream should be restarted
fn handle_meta_task_pulling(
    pull_task_ack: &mut bool,
    task_queue: &IcebergTaskQueue,
    max_task_parallelism: u32,
    max_pull_task_count: u32,
    request_sender: &mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
    log_throttler: &mut LogThrottler<IcebergCompactionLogState>,
) -> bool {
    let mut pending_pull_task_count = 0;
    if *pull_task_ack {
        // Use queue's running parallelism for pull decision
        let current_running_parallelism = task_queue.running_parallelism_sum();
        pending_pull_task_count =
            (max_task_parallelism - current_running_parallelism).min(max_pull_task_count);

        if pending_pull_task_count > 0 {
            if let Err(e) = request_sender.send(SubscribeIcebergCompactionEventRequest {
                event: Some(subscribe_iceberg_compaction_event_request::Event::PullTask(
                    subscribe_iceberg_compaction_event_request::PullTask {
                        pull_task_count: pending_pull_task_count,
                    },
                )),
                create_at: SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("Clock may have gone backwards")
                    .as_millis() as u64,
            }) {
                tracing::warn!(error = %e.as_report(), "Failed to pull task - will retry on stream restart");
                return true; // Signal to restart stream
            } else {
                *pull_task_ack = false;
            }
        }
    }

    let running_count = task_queue.running_parallelism_sum();
    let waiting_count = task_queue.waiting_parallelism_sum();
    let available_count = max_task_parallelism.saturating_sub(running_count);
    let current_state = IcebergCompactionLogState {
        running_parallelism: running_count,
        waiting_parallelism: waiting_count,
        available_parallelism: available_count,
        pull_task_ack: *pull_task_ack,
        pending_pull_task_count,
    };

    // Log only when state changes or periodically as heartbeat
    if log_throttler.should_log(&current_state) {
        tracing::info!(
            running_parallelism_count = %current_state.running_parallelism,
            waiting_parallelism_count = %current_state.waiting_parallelism,
            available_parallelism = %current_state.available_parallelism,
            pull_task_ack = %current_state.pull_task_ack,
            pending_pull_task_count = %current_state.pending_pull_task_count
        );
        log_throttler.update(current_state);
    }

    false // No need to restart stream
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cancel_iceberg_task_removes_waiting_plans_and_tracker() {
        let task_id = IcebergCompactionTaskId::new(42);
        let mut task_queue = IcebergTaskQueue::new(10, 30, 1024);
        assert_eq!(
            task_queue.push(
                iceberg_compaction::IcebergTaskMeta {
                    task_id,
                    plan_index: 0,
                    required_parallelism: 3,
                    memory_reservation_bytes: 64,
                },
                None,
            ),
            PushResult::Added
        );
        assert_eq!(
            task_queue.push(
                iceberg_compaction::IcebergTaskMeta {
                    task_id,
                    plan_index: 1,
                    required_parallelism: 4,
                    memory_reservation_bytes: 64,
                },
                None,
            ),
            PushResult::Added
        );
        assert_eq!(task_queue.waiting_parallelism_sum(), 7);

        let shutdown_map = Arc::new(Mutex::new(HashMap::new()));
        let mut task_trackers = HashMap::from([(task_id, IcebergTaskTracker::new(10, 2, false))]);

        cancel_iceberg_task(task_id, &mut task_queue, &shutdown_map, &mut task_trackers);

        assert_eq!(task_queue.waiting_parallelism_sum(), 0);
        assert!(!task_trackers.contains_key(&task_id));
    }

    #[test]
    fn test_cancel_iceberg_task_shuts_down_running_plans_and_tracker() {
        let task_id = IcebergCompactionTaskId::new(43);
        let task_key = (task_id, 0);
        let mut task_queue = IcebergTaskQueue::new(10, 30, 1024);
        let shutdown_map = Arc::new(Mutex::new(HashMap::new()));
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        shutdown_map.lock().unwrap().insert(task_key, shutdown_tx);
        let mut task_trackers = HashMap::from([(task_id, IcebergTaskTracker::new(10, 1, false))]);

        cancel_iceberg_task(task_id, &mut task_queue, &shutdown_map, &mut task_trackers);

        assert!(shutdown_rx.try_recv().is_ok());
        assert!(shutdown_map.lock().unwrap().is_empty());
        assert!(!task_trackers.contains_key(&task_id));
    }

    #[test]
    fn test_log_state_equality() {
        // Test CompactionLogState
        let state1 = CompactionLogState {
            running_parallelism: 10,
            pull_task_ack: true,
            pending_pull_task_count: 2,
        };
        let state2 = CompactionLogState {
            running_parallelism: 10,
            pull_task_ack: true,
            pending_pull_task_count: 2,
        };
        let state3 = CompactionLogState {
            running_parallelism: 11,
            pull_task_ack: true,
            pending_pull_task_count: 2,
        };
        assert_eq!(state1, state2);
        assert_ne!(state1, state3);

        // Test IcebergCompactionLogState
        let ice_state1 = IcebergCompactionLogState {
            running_parallelism: 10,
            waiting_parallelism: 5,
            available_parallelism: 15,
            pull_task_ack: true,
            pending_pull_task_count: 2,
        };
        let ice_state2 = IcebergCompactionLogState {
            running_parallelism: 10,
            waiting_parallelism: 6,
            available_parallelism: 15,
            pull_task_ack: true,
            pending_pull_task_count: 2,
        };
        assert_ne!(ice_state1, ice_state2);
    }

    #[test]
    fn test_log_throttler_state_change_detection() {
        let mut throttler = LogThrottler::<CompactionLogState>::new(Duration::from_secs(60));
        let state1 = CompactionLogState {
            running_parallelism: 10,
            pull_task_ack: true,
            pending_pull_task_count: 2,
        };
        let state2 = CompactionLogState {
            running_parallelism: 11,
            pull_task_ack: true,
            pending_pull_task_count: 2,
        };

        // First call should always log
        assert!(throttler.should_log(&state1));
        throttler.update(state1.clone());

        // Same state should not log
        assert!(!throttler.should_log(&state1));

        // Changed state should log
        assert!(throttler.should_log(&state2));
        throttler.update(state2.clone());

        // Same state again should not log
        assert!(!throttler.should_log(&state2));
    }

    #[test]
    fn test_log_throttler_heartbeat() {
        let mut throttler = LogThrottler::<CompactionLogState>::new(Duration::from_millis(10));
        let state = CompactionLogState {
            running_parallelism: 10,
            pull_task_ack: true,
            pending_pull_task_count: 2,
        };

        // First call should log
        assert!(throttler.should_log(&state));
        throttler.update(state.clone());

        // Same state immediately should not log
        assert!(!throttler.should_log(&state));

        // Wait for heartbeat interval to pass
        std::thread::sleep(Duration::from_millis(15));

        // Same state after interval should log (heartbeat)
        assert!(throttler.should_log(&state));
    }
}
