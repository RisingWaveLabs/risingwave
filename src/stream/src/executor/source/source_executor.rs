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

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::anyhow;
use either::Either;
use itertools::Itertools;
use prometheus::core::{AtomicU64, GenericCounter};
use risingwave_common::array::ArrayRef;
use risingwave_common::catalog::TableId;
use risingwave_common::metrics::{GLOBAL_ERROR_METRICS, LabelGuardedIntGauge, LabelGuardedMetric};
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::types::JsonbVal;
use risingwave_common::util::epoch::{Epoch, EpochPair};
use risingwave_connector::source::cdc::split::{
    extract_postgres_lsn_from_offset_str, extract_sql_server_commit_lsn_from_offset_str,
};
use risingwave_connector::source::reader::desc::{SourceDesc, SourceDescBuilder};
use risingwave_connector::source::reader::reader::SourceReader;
use risingwave_connector::source::{
    ConnectorState, SplitId, SplitImpl, SplitMetaData, WaitCheckpointTask,
    build_pulsar_ack_channel_id,
};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::common::ThrottleType;
use risingwave_pb::id::SourceId;
use risingwave_storage::store::TryWaitEpochOptions;
use thiserror_ext::AsReport;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::Instant;

use super::executor_core::StreamSourceCore;
use super::{barrier_to_message_stream, get_split_offset_col_idx, prune_additional_cols};
use crate::executor::UpdateMutation;
use crate::executor::prelude::*;
use crate::executor::source::reader_stream::{SourceReaderEventWithState, StreamReaderBuilder};
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::task::LocalBarrierManager;

/// A constant to multiply when calculating the maximum time to wait for a barrier. This is due to
/// some latencies in network and cost in meta.
pub const WAIT_BARRIER_MULTIPLE_TIMES: u128 = 5;

fn lsn_u128_to_i64(lsn: u128) -> i64 {
    lsn.min(i64::MAX as u128) as i64
}

fn update_mysql_cdc_state_file_seq_metric(
    metric_guard: &mut Option<LabelGuardedIntGauge>,
    metrics: &StreamingMetrics,
    source_id: &str,
    split_impl: &SplitImpl,
) {
    let SplitImpl::MysqlCdc(mysql_split) = split_impl else {
        return;
    };

    if let Some((file_seq, _)) = mysql_split.mysql_binlog_offset() {
        metric_guard
            .get_or_insert_with(|| {
                metrics
                    .mysql_cdc_state_binlog_file_seq
                    .with_guarded_label_values(&[source_id])
            })
            .set(file_seq as i64);
    } else {
        *metric_guard = None;
    }
}

fn clear_mysql_cdc_state_file_seq_metric_if_unassigned(
    metric_guard: &mut Option<LabelGuardedIntGauge>,
    target_state: &HashMap<SplitId, SplitImpl>,
) {
    if !target_state
        .values()
        .any(|split| matches!(split, SplitImpl::MysqlCdc(_)))
    {
        *metric_guard = None;
    }
}

pub struct SourceExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Streaming source for external
    stream_source_core: StreamSourceCore<S>,

    /// Metrics for monitor.
    metrics: Arc<StreamingMetrics>,

    /// Keep the MySQL CDC state file sequence metric alive while this executor owns the split.
    mysql_cdc_state_binlog_file_seq_guard: Option<LabelGuardedIntGauge>,

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,

    /// System parameter reader to read barrier interval
    system_params: SystemParamsReaderRef,

    /// Rate limit in rows/s.
    rate_limit_rps: Option<u32>,

    is_shared_non_cdc: bool,

    /// Local barrier manager for reporting source load finished events
    barrier_manager: LocalBarrierManager,
}

impl<S: StateStore> SourceExecutor<S> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        system_params: SystemParamsReaderRef,
        rate_limit_rps: Option<u32>,
        is_shared_non_cdc: bool,
        barrier_manager: LocalBarrierManager,
    ) -> Self {
        Self {
            actor_ctx,
            stream_source_core,
            metrics,
            mysql_cdc_state_binlog_file_seq_guard: None,
            barrier_receiver: Some(barrier_receiver),
            system_params,
            rate_limit_rps,
            is_shared_non_cdc,
            barrier_manager,
        }
    }

    fn stream_reader_builder(&self, source_desc: SourceDesc) -> StreamReaderBuilder {
        StreamReaderBuilder {
            source_desc,
            rate_limit: self.rate_limit_rps,
            source_id: self.stream_source_core.source_id,
            source_name: self.stream_source_core.source_name.clone(),
            is_auto_schema_change_enable: self.is_auto_schema_change_enable(),
            actor_ctx: self.actor_ctx.clone(),
            reader_stream: None,
        }
    }

    async fn spawn_wait_checkpoint_worker(
        core: &StreamSourceCore<S>,
        source_reader: SourceReader,
        metrics: Arc<StreamingMetrics>,
    ) -> StreamExecutorResult<Option<WaitCheckpointTaskBuilder>> {
        let Some(initial_task) = source_reader.create_wait_checkpoint_task().await? else {
            return Ok(None);
        };
        let (wait_checkpoint_tx, wait_checkpoint_rx) = mpsc::unbounded_channel();
        let wait_checkpoint_worker = WaitCheckpointWorker {
            wait_checkpoint_rx,
            state_store: core.split_state_store.state_table().state_store().clone(),
            table_id: core.split_state_store.state_table().table_id(),
            source_id: core.source_id,
            source_name: core.source_name.clone(),
            metrics,
        };
        tokio::spawn(wait_checkpoint_worker.run());
        Ok(Some(WaitCheckpointTaskBuilder {
            wait_checkpoint_tx,
            building_task: initial_task,
        }))
    }

    fn is_auto_schema_change_enable(&self) -> bool {
        self.actor_ctx.config.developer.enable_auto_schema_change
    }

    /// `source_id | source_name | actor_id | fragment_id`
    #[inline]
    fn get_metric_labels(&self) -> [String; 4] {
        [
            self.stream_source_core.source_id.to_string(),
            self.stream_source_core.source_name.clone(),
            self.actor_ctx.id.to_string(),
            self.actor_ctx.fragment_id.to_string(),
        ]
    }

    /// - `should_trim_state`: whether to trim state for dropped splits.
    ///
    ///   For scaling, the connector splits can be migrated to other actors, but
    ///   won't be added or removed. Actors should not trim states for splits that
    ///   are moved to other actors.
    ///
    ///   For source split change, split will not be migrated and we can trim states
    ///   for deleted splits.
    async fn apply_split_change_after_yield_barrier<const BIASED: bool>(
        &mut self,
        barrier_epoch: EpochPair,
        source_desc: &SourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, SourceReaderEventWithState>,
        apply_mutation: ApplyMutationAfterBarrier<'_>,
    ) -> StreamExecutorResult<()> {
        {
            let mut should_rebuild_stream = false;
            match apply_mutation {
                ApplyMutationAfterBarrier::SplitChange {
                    target_splits,
                    should_trim_state,
                    split_change_count,
                } => {
                    split_change_count.inc();
                    if self
                        .update_state_if_changed(barrier_epoch, target_splits, should_trim_state)
                        .await?
                    {
                        should_rebuild_stream = true;
                    }
                }
                ApplyMutationAfterBarrier::ConnectorPropsChange => {
                    should_rebuild_stream = true;
                }
            }

            if should_rebuild_stream {
                self.rebuild_stream_reader(source_desc, stream)?;
            }
        }

        Ok(())
    }

    /// Returns `true` if split changed. Otherwise `false`.
    async fn update_state_if_changed(
        &mut self,
        barrier_epoch: EpochPair,
        target_splits: Vec<SplitImpl>,
        should_trim_state: bool,
    ) -> StreamExecutorResult<bool> {
        let mysql_file_seq_metric_guard = &mut self.mysql_cdc_state_binlog_file_seq_guard;
        let core = &mut self.stream_source_core;

        let target_splits: HashMap<_, _> = target_splits
            .into_iter()
            .map(|split| (split.id(), split))
            .collect();

        let mut target_state: HashMap<SplitId, SplitImpl> =
            HashMap::with_capacity(target_splits.len());

        let mut split_changed = false;

        let committed_reader = core
            .split_state_store
            .new_committed_reader(barrier_epoch)
            .await?;

        // Checks added splits
        for (split_id, split) in target_splits {
            if let Some(s) = core.latest_split_info.get(&split_id) {
                // For existing splits, we should use the latest offset from the cache.
                // `target_splits` is from meta and contains the initial offset.
                target_state.insert(split_id, s.clone());
            } else {
                split_changed = true;
                // write new assigned split to state cache. snapshot is base on cache.

                let initial_state = if let Some(recover_state) = committed_reader
                    .try_recover_from_state_store(&split)
                    .await?
                {
                    recover_state
                } else {
                    split
                };

                core.updated_splits_in_epoch
                    .entry(split_id.clone())
                    .or_insert_with(|| initial_state.clone());

                target_state.insert(split_id, initial_state);
            }
        }

        // Checks dropped splits
        for existing_split_id in core.latest_split_info.keys() {
            if !target_state.contains_key(existing_split_id) {
                tracing::info!("split dropping detected: {}", existing_split_id);
                split_changed = true;
            }
        }

        if split_changed {
            tracing::info!(
                actor_id = %self.actor_ctx.id,
                state = ?target_state,
                "apply split change"
            );

            core.updated_splits_in_epoch
                .retain(|split_id, _| target_state.contains_key(split_id));

            let dropped_splits = core
                .latest_split_info
                .extract_if(|split_id, _| !target_state.contains_key(split_id))
                .map(|(_, split)| split)
                .collect_vec();

            if should_trim_state && !dropped_splits.is_empty() {
                // trim dropped splits' state
                core.split_state_store.trim_state(&dropped_splits).await?;
            }

            core.latest_split_info = target_state;
        }

        clear_mysql_cdc_state_file_seq_metric_if_unassigned(
            mysql_file_seq_metric_guard,
            &core.latest_split_info,
        );

        Ok(split_changed)
    }

    /// Rebuild stream if there is a err in stream
    fn rebuild_stream_reader_from_error<const BIASED: bool>(
        &mut self,
        source_desc: &SourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, SourceReaderEventWithState>,
        e: StreamExecutorError,
    ) -> StreamExecutorResult<()> {
        let core = &mut self.stream_source_core;
        tracing::error!(
            error = ?e.as_report(),
            actor_id = %self.actor_ctx.id,
            source_id = %core.source_id,
            "stream source reader error",
        );
        GLOBAL_ERROR_METRICS.user_source_error.report([
            e.variant_name().to_owned(),
            core.source_id.to_string(),
            core.source_name.clone(),
            self.actor_ctx.fragment_id.to_string(),
        ]);

        self.rebuild_stream_reader(source_desc, stream)
    }

    fn rebuild_stream_reader<const BIASED: bool>(
        &mut self,
        source_desc: &SourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, SourceReaderEventWithState>,
    ) -> StreamExecutorResult<()> {
        let core = &mut self.stream_source_core;
        let target_state: Vec<SplitImpl> = core.latest_split_info.values().cloned().collect();

        tracing::info!(
            "actor {:?} apply source split change to {:?}",
            self.actor_ctx.id,
            target_state
        );

        // Replace the source reader with a new one of the new state.
        let reader_stream_builder = self.stream_reader_builder(source_desc.clone());
        let reader_stream = reader_stream_builder.into_retry_stream(Some(target_state), false);

        stream.replace_data_stream(reader_stream);

        Ok(())
    }

    /// Handle `InjectSourceOffsets` mutation by updating split offsets in place.
    /// Returns `true` if any offsets were injected and a stream rebuild is needed.
    ///
    /// This is an UNSAFE operation that can cause data duplication or loss
    /// depending on the correctness of the provided offsets.
    async fn handle_inject_source_offsets(
        &mut self,
        split_offsets: &HashMap<String, String>,
    ) -> StreamExecutorResult<bool> {
        tracing::warn!(
            actor_id = %self.actor_ctx.id,
            source_id = %self.stream_source_core.source_id,
            num_offsets = split_offsets.len(),
            "UNSAFE: Injecting source offsets - this may cause data duplication or loss"
        );

        // First, filter to only offsets for splits this actor owns
        let offsets_to_inject: Vec<(String, String)> = {
            let owned_splits: HashSet<&str> = self
                .stream_source_core
                .latest_split_info
                .keys()
                .map(|s| s.as_ref())
                .collect();
            split_offsets
                .iter()
                .filter(|(split_id, _)| owned_splits.contains(split_id.as_str()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };

        // Update splits and prepare JSON states for persistence
        let mut json_states: Vec<(String, JsonbVal)> = Vec::new();
        let mut failed_splits: Vec<String> = Vec::new();

        for (split_id, offset) in &offsets_to_inject {
            if let Some(split) = self
                .stream_source_core
                .latest_split_info
                .get_mut(split_id.as_str())
            {
                tracing::info!(
                    actor_id = %self.actor_ctx.id,
                    split_id = %split_id,
                    offset = %offset,
                    "Injecting offset for owned split"
                );
                // Update the split's offset in place
                if let Err(e) = split.update_in_place(offset.clone()) {
                    tracing::error!(
                        actor_id = %self.actor_ctx.id,
                        split_id = %split_id,
                        error = ?e.as_report(),
                        "Failed to update split offset"
                    );
                    failed_splits.push(split_id.clone());
                    continue;
                }
                // Mark this split as updated for persistence
                self.stream_source_core
                    .updated_splits_in_epoch
                    .insert(split_id.clone().into(), split.clone());
                // Parse the offset as JSON and store it
                let json_value: serde_json::Value = serde_json::from_str(offset)
                    .unwrap_or_else(|_| serde_json::json!({ "offset": offset }));
                json_states.push((split_id.clone(), JsonbVal::from(json_value)));
            }
        }

        if !failed_splits.is_empty() {
            return Err(StreamExecutorError::connector_error(anyhow!(
                "failed to inject offsets for splits: {:?}",
                failed_splits
            )));
        }

        let num_injected = json_states.len();
        if num_injected > 0 {
            // Store the injected offsets as JSON in the state table
            self.stream_source_core
                .split_state_store
                .set_states_json(json_states)
                .await?;

            tracing::info!(
                actor_id = %self.actor_ctx.id,
                source_id = %self.stream_source_core.source_id,
                num_injected = num_injected,
                "Offset injection completed for owned splits, triggering rebuild"
            );
            Ok(true)
        } else {
            tracing::info!(
                actor_id = %self.actor_ctx.id,
                source_id = %self.stream_source_core.source_id,
                "No owned splits to inject offsets for"
            );
            Ok(false)
        }
    }

    async fn persist_state_and_clear_cache(
        &mut self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<HashMap<SplitId, SplitImpl>> {
        let core = &mut self.stream_source_core;
        let mysql_file_seq_metric_guard = &mut self.mysql_cdc_state_binlog_file_seq_guard;

        let cache = core
            .updated_splits_in_epoch
            .values()
            .map(|split_impl| split_impl.to_owned())
            .collect_vec();

        if !cache.is_empty() {
            tracing::debug!(state = ?cache, "take snapshot");

            // Record metrics for CDC sources before moving cache
            let source_id = core.source_id.to_string();
            for split_impl in &cache {
                // Extract and record CDC-specific metrics based on split type
                match split_impl {
                    SplitImpl::PostgresCdc(pg_split) => {
                        if let Some(lsn_value) = pg_split.pg_lsn() {
                            self.metrics
                                .pg_cdc_state_table_lsn
                                .with_guarded_label_values(&[&source_id])
                                .set(lsn_value as i64);
                        }
                    }
                    SplitImpl::MysqlCdc(mysql_split) => {
                        update_mysql_cdc_state_file_seq_metric(
                            mysql_file_seq_metric_guard,
                            &self.metrics,
                            &source_id,
                            split_impl,
                        );
                        if let Some((_, position)) = mysql_split.mysql_binlog_offset() {
                            self.metrics
                                .mysql_cdc_state_binlog_position
                                .with_guarded_label_values(&[&source_id])
                                .set(position as i64);
                        }
                    }
                    SplitImpl::SqlServerCdc(sqlserver_split) => {
                        if let Some(lsn) = sqlserver_split.sql_server_change_lsn() {
                            self.metrics
                                .sqlserver_cdc_state_change_lsn
                                .with_guarded_label_values(&[&source_id])
                                .set(lsn_u128_to_i64(lsn));
                        }
                        if let Some(lsn) = sqlserver_split.sql_server_commit_lsn() {
                            self.metrics
                                .sqlserver_cdc_state_commit_lsn
                                .with_guarded_label_values(&[&source_id])
                                .set(lsn_u128_to_i64(lsn));
                        }
                    }
                    _ => {}
                }
            }

            core.split_state_store.set_states(cache).await?;
        }

        // commit anyway, even if no message saved
        core.split_state_store.commit(epoch).await?;

        let updated_splits = core.updated_splits_in_epoch.clone();

        core.updated_splits_in_epoch.clear();

        Ok(updated_splits)
    }

    /// try mem table spill
    async fn try_flush_data(&mut self) -> StreamExecutorResult<()> {
        let core = &mut self.stream_source_core;
        core.split_state_store.try_flush().await?;

        Ok(())
    }

    fn apply_latest_split_state(&mut self, latest_state: HashMap<SplitId, SplitImpl>) {
        for (split_id, new_split_impl) in &latest_state {
            if let Some(split_impl) = self.stream_source_core.latest_split_info.get_mut(split_id) {
                *split_impl = new_split_impl.clone();
            }
        }
        self.stream_source_core
            .updated_splits_in_epoch
            .extend(latest_state);
    }

    /// Report CDC source offset updated only the first time.
    fn maybe_report_cdc_source_offset(
        &self,
        updated_splits: &HashMap<SplitId, SplitImpl>,
        epoch: EpochPair,
        source_id: SourceId,
        must_report_cdc_offset_once: &mut bool,
        must_wait_cdc_offset_before_report: bool,
    ) {
        // Report CDC source offset updated only the first time
        if *must_report_cdc_offset_once
            && (!must_wait_cdc_offset_before_report
                || updated_splits
                    .values()
                    .any(|split| split.is_cdc_split() && !split.get_cdc_split_offset().is_empty()))
        {
            // Report only once, then ignore all subsequent offset updates
            self.barrier_manager.report_cdc_source_offset_updated(
                epoch,
                self.actor_ctx.id,
                source_id,
            );
            tracing::info!(
                actor_id = %self.actor_ctx.id,
                source_id = %source_id,
                epoch = ?epoch,
                "Reported CDC source offset updated to meta (first time only)"
            );
            // Mark as reported to prevent any future reports, even if offset changes
            *must_report_cdc_offset_once = false;
        }
    }

    /// A source executor with a stream source receives:
    /// 1. Barrier messages
    /// 2. Data from external source
    /// and acts accordingly.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let first_barrier = barrier_receiver
            .recv()
            .instrument_await("source_recv_first_barrier")
            .await
            .ok_or_else(|| {
                anyhow!(
                    "failed to receive the first barrier, actor_id: {:?}, source_id: {:?}",
                    self.actor_ctx.id,
                    self.stream_source_core.source_id
                )
            })?;
        let first_epoch = first_barrier.epoch;
        // must_report_cdc_offset is true if and only if the source is a CDC source.
        // must_wait_cdc_offset_before_report is true if and only if the source is a MySQL or SQL Server CDC source.
        let (mut boot_state, mut must_report_cdc_offset_once, must_wait_cdc_offset_before_report) =
            if let Some(splits) = first_barrier.initial_split_assignment(self.actor_ctx.id) {
                // CDC source must reach this branch.
                tracing::debug!(?splits, "boot with splits");
                // Skip report for non-CDC.
                let must_report_cdc_offset_once = splits.iter().any(|split| split.is_cdc_split());
                // Only for MySQL and SQL Server CDC, we need to wait for the offset to be non-empty before reporting.
                let must_wait_cdc_offset_before_report = must_report_cdc_offset_once
                    && splits.iter().any(|split| {
                        matches!(split, SplitImpl::MysqlCdc(_) | SplitImpl::SqlServerCdc(_))
                    });
                (
                    splits.to_vec(),
                    must_report_cdc_offset_once,
                    must_wait_cdc_offset_before_report,
                )
            } else {
                (Vec::default(), true, true)
            };
        let is_pause_on_startup = first_barrier.is_pause_on_startup();
        let mut is_uninitialized = first_barrier.is_newly_added(self.actor_ctx.id);

        yield Message::Barrier(first_barrier);

        let mut core = self.stream_source_core;
        let source_id = core.source_id;

        // Build source description from the builder.
        let source_desc_builder: SourceDescBuilder = core.source_desc_builder.take().unwrap();
        let mut source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;

        let mut wait_checkpoint_task_builder = Self::spawn_wait_checkpoint_worker(
            &core,
            source_desc.source.clone(),
            self.metrics.clone(),
        )
        .await?;

        let (Some(split_idx), Some(offset_idx), pulsar_message_id_idx) =
            get_split_offset_col_idx(&source_desc.columns)
        else {
            unreachable!("Partition and offset columns must be set.");
        };

        core.split_state_store.init_epoch(first_epoch).await?;
        {
            let source_id = source_id.to_string();
            let committed_reader = core
                .split_state_store
                .new_committed_reader(first_epoch)
                .await?;
            for ele in &mut boot_state {
                if let Some(recover_state) =
                    committed_reader.try_recover_from_state_store(ele).await?
                {
                    *ele = recover_state;
                    update_mysql_cdc_state_file_seq_metric(
                        &mut self.mysql_cdc_state_binlog_file_seq_guard,
                        &self.metrics,
                        &source_id,
                        ele,
                    );
                    // if state store is non-empty, we consider it's initialized.
                    is_uninitialized = false;
                } else {
                    // This is a new split, not in state table.
                    // make sure it is written to state table later.
                    // Then even it receives no messages, we can observe it in state table.
                    core.updated_splits_in_epoch.insert(ele.id(), ele.clone());
                }
            }
        }

        // init in-memory split states with persisted state if any
        core.init_split_state(boot_state.clone());

        // Return the ownership of `stream_source_core` to the source executor.
        self.stream_source_core = core;

        let recover_state: ConnectorState = (!boot_state.is_empty()).then_some(boot_state);
        tracing::debug!(state = ?recover_state, "start with state");

        let barrier_stream = barrier_to_message_stream(barrier_receiver).boxed();
        let mut reader_stream_builder = self.stream_reader_builder(source_desc.clone());
        let mut latest_splits = None;
        // Build the source stream reader.
        if is_uninitialized {
            let create_split_reader_result = reader_stream_builder
                .fetch_latest_splits(recover_state.clone(), self.is_shared_non_cdc)
                .await?;
            latest_splits = create_split_reader_result.latest_splits;
        }

        if let Some(latest_splits) = latest_splits {
            // make sure it is written to state table later.
            // Then even it receives no messages, we can observe it in state table.
            self.stream_source_core
                .updated_splits_in_epoch
                .extend(latest_splits.into_iter().map(|s| (s.id(), s)));
        }
        // Merge the chunks from source and the barriers into a single stream. We prioritize
        // barriers over source data chunks here.
        let mut stream = StreamReaderWithPause::<true, SourceReaderEventWithState>::new(
            barrier_stream,
            reader_stream_builder
                .into_retry_stream(recover_state, is_uninitialized && self.is_shared_non_cdc),
        );
        let mut command_paused = false;

        // - If the first barrier requires us to pause on startup, pause the stream.
        if is_pause_on_startup {
            tracing::info!("source paused on startup");
            stream.pause_stream();
            command_paused = true;
        }

        // We allow data to flow for `WAIT_BARRIER_MULTIPLE_TIMES` * `expected_barrier_latency_ms`
        // milliseconds, considering some other latencies like network and cost in Meta.
        let mut max_wait_barrier_time_ms =
            self.system_params.load().barrier_interval_ms() as u128 * WAIT_BARRIER_MULTIPLE_TIMES;
        let mut last_barrier_time = Instant::now();
        let mut self_paused = false;

        let source_output_row_count = self
            .metrics
            .source_output_row_count
            .with_guarded_label_values(&self.get_metric_labels());

        let source_split_change_count = self
            .metrics
            .source_split_change_count
            .with_guarded_label_values(&self.get_metric_labels());

        while let Some(msg) = stream.next().await {
            let Ok(msg) = msg else {
                tokio::time::sleep(Duration::from_millis(1000)).await;
                self.rebuild_stream_reader_from_error(&source_desc, &mut stream, msg.unwrap_err())?;
                continue;
            };

            match msg {
                // This branch will be preferred.
                Either::Left(Message::Barrier(barrier)) => {
                    last_barrier_time = Instant::now();

                    if self_paused {
                        self_paused = false;
                        // command_paused has a higher priority.
                        if !command_paused {
                            stream.resume_stream();
                        }
                    }

                    let epoch = barrier.epoch;
                    let mut split_change = None;

                    if let Some(mutation) = barrier.mutation.as_deref() {
                        match mutation {
                            Mutation::Pause => {
                                command_paused = true;
                                stream.pause_stream()
                            }
                            Mutation::Resume => {
                                command_paused = false;
                                stream.resume_stream()
                            }
                            Mutation::SourceChangeSplit(actor_splits) => {
                                tracing::info!(
                                    actor_id = %self.actor_ctx.id,
                                    actor_splits = ?actor_splits,
                                    "source change split received"
                                );

                                split_change = actor_splits.get(&self.actor_ctx.id).cloned().map(
                                    |target_splits| {
                                        (
                                            &source_desc,
                                            &mut stream,
                                            ApplyMutationAfterBarrier::SplitChange {
                                                target_splits,
                                                should_trim_state: true,
                                                split_change_count: &source_split_change_count,
                                            },
                                        )
                                    },
                                );
                            }

                            Mutation::ConnectorPropsChange(maybe_mutation) => {
                                if let Some(new_props) = maybe_mutation.get(&source_id.as_raw_id())
                                {
                                    // rebuild the stream reader with new props
                                    tracing::info!(
                                        actor_id = %self.actor_ctx.id,
                                        source_id = %source_id,
                                        "updating source connector properties",
                                    );
                                    source_desc.update_reader(new_props.clone())?;
                                    // suppose the connector props change will not involve state change
                                    split_change = Some((
                                        &source_desc,
                                        &mut stream,
                                        ApplyMutationAfterBarrier::ConnectorPropsChange,
                                    ));
                                }
                            }

                            Mutation::Update(UpdateMutation { actor_splits, .. }) => {
                                split_change = actor_splits.get(&self.actor_ctx.id).cloned().map(
                                    |target_splits| {
                                        (
                                            &source_desc,
                                            &mut stream,
                                            ApplyMutationAfterBarrier::SplitChange {
                                                target_splits,
                                                should_trim_state: false,
                                                split_change_count: &source_split_change_count,
                                            },
                                        )
                                    },
                                );
                            }
                            Mutation::Throttle(fragment_to_apply) => {
                                if let Some(entry) =
                                    fragment_to_apply.get(&self.actor_ctx.fragment_id)
                                    && entry.throttle_type() == ThrottleType::Source
                                    && entry.rate_limit != self.rate_limit_rps
                                {
                                    tracing::info!(
                                        "updating rate limit from {:?} to {:?}",
                                        self.rate_limit_rps,
                                        entry.rate_limit
                                    );
                                    self.rate_limit_rps = entry.rate_limit;
                                    // recreate from latest_split_info
                                    self.rebuild_stream_reader(&source_desc, &mut stream)?;
                                }
                            }
                            Mutation::ResetSource { source_id } => {
                                // Note: RESET SOURCE only clears the offset, does NOT pause the source.
                                // When offset is None, after recovery/restart, Debezium will automatically
                                // enter recovery mode and fetch the latest offset from upstream.
                                if *source_id == self.stream_source_core.source_id {
                                    tracing::info!(
                                        actor_id = %self.actor_ctx.id,
                                        source_id = source_id.as_raw_id(),
                                        "Resetting CDC source: clearing offset (set to None)"
                                    );

                                    // Step 1: Collect all current splits and clear their offsets
                                    let splits_with_cleared_offset: Vec<SplitImpl> = self.stream_source_core
                                        .latest_split_info
                                        .values()
                                        .map(|split| {
                                            // Clone the split and clear its offset
                                            let mut new_split = split.clone();
                                            match &mut new_split {
                                                SplitImpl::MysqlCdc(debezium_split) => {
                                                    if let Some(mysql_split) = debezium_split.mysql_split.as_mut() {
                                                        tracing::info!(
                                                            split_id = ?mysql_split.inner.split_id,
                                                            old_offset = ?mysql_split.inner.start_offset,
                                                            "Clearing MySQL CDC offset"
                                                        );
                                                        mysql_split.inner.start_offset = None;
                                                    }
                                                }
                                                SplitImpl::PostgresCdc(debezium_split) => {
                                                    if let Some(pg_split) = debezium_split.postgres_split.as_mut() {
                                                        tracing::info!(
                                                            split_id = ?pg_split.inner.split_id,
                                                            old_offset = ?pg_split.inner.start_offset,
                                                            "Clearing PostgreSQL CDC offset"
                                                        );
                                                        pg_split.inner.start_offset = None;
                                                    }
                                                }
                                                SplitImpl::MongodbCdc(debezium_split) => {
                                                    if let Some(mongo_split) = debezium_split.mongodb_split.as_mut() {
                                                        tracing::info!(
                                                            split_id = ?mongo_split.inner.split_id,
                                                            old_offset = ?mongo_split.inner.start_offset,
                                                            "Clearing MongoDB CDC offset"
                                                        );
                                                        mongo_split.inner.start_offset = None;
                                                    }
                                                }
                                                SplitImpl::CitusCdc(debezium_split) => {
                                                    if let Some(citus_split) = debezium_split.citus_split.as_mut() {
                                                        tracing::info!(
                                                            split_id = ?citus_split.inner.split_id,
                                                            old_offset = ?citus_split.inner.start_offset,
                                                            "Clearing Citus CDC offset"
                                                        );
                                                        citus_split.inner.start_offset = None;
                                                    }
                                                }
                                                SplitImpl::SqlServerCdc(debezium_split) => {
                                                    if let Some(sqlserver_split) = debezium_split.sql_server_split.as_mut() {
                                                        tracing::info!(
                                                            split_id = ?sqlserver_split.inner.split_id,
                                                            old_offset = ?sqlserver_split.inner.start_offset,
                                                            "Clearing SQL Server CDC offset"
                                                        );
                                                        sqlserver_split.inner.start_offset = None;
                                                    }
                                                }
                                                _ => {
                                                    tracing::warn!(
                                                        "RESET SOURCE called on non-CDC split type"
                                                    );
                                                }
                                            }
                                            new_split
                                        })
                                        .collect();

                                    if !splits_with_cleared_offset.is_empty() {
                                        tracing::info!(
                                            actor_id = %self.actor_ctx.id,
                                            split_count = splits_with_cleared_offset.len(),
                                            "Updating state table with cleared offsets"
                                        );

                                        // Step 2: Write splits back to state table with offset = None
                                        self.stream_source_core
                                            .split_state_store
                                            .set_states(splits_with_cleared_offset.clone())
                                            .await?;

                                        // Step 3: Update in-memory split info with cleared offsets
                                        for split in splits_with_cleared_offset {
                                            self.stream_source_core
                                                .latest_split_info
                                                .insert(split.id(), split.clone());
                                            self.stream_source_core
                                                .updated_splits_in_epoch
                                                .insert(split.id(), split);
                                        }

                                        tracing::info!(
                                            actor_id = %self.actor_ctx.id,
                                            source_id = source_id.as_raw_id(),
                                            "RESET SOURCE completed: offset cleared (set to None). \
                                             Trigger recovery/restart to fetch latest offset from upstream."
                                        );
                                    } else {
                                        tracing::warn!(
                                            actor_id = %self.actor_ctx.id,
                                            "No splits found to reset - source may not be initialized yet"
                                        );
                                    }
                                } else {
                                    tracing::debug!(
                                        actor_id = %self.actor_ctx.id,
                                        target_source_id = source_id.as_raw_id(),
                                        current_source_id = self.stream_source_core.source_id.as_raw_id(),
                                        "ResetSource mutation for different source, ignoring"
                                    );
                                }
                            }

                            Mutation::InjectSourceOffsets {
                                source_id,
                                split_offsets,
                            } => {
                                if *source_id == self.stream_source_core.source_id {
                                    if self.handle_inject_source_offsets(split_offsets).await? {
                                        // Trigger a rebuild to apply the new offsets
                                        split_change = Some((
                                            &source_desc,
                                            &mut stream,
                                            ApplyMutationAfterBarrier::ConnectorPropsChange,
                                        ));
                                    }
                                } else {
                                    tracing::debug!(
                                        actor_id = %self.actor_ctx.id,
                                        target_source_id = source_id.as_raw_id(),
                                        current_source_id = self.stream_source_core.source_id.as_raw_id(),
                                        "InjectSourceOffsets mutation for different source, ignoring"
                                    );
                                }
                            }

                            _ => {}
                        }
                    }

                    let updated_splits = self.persist_state_and_clear_cache(epoch).await?;

                    self.maybe_report_cdc_source_offset(
                        &updated_splits,
                        epoch,
                        source_id,
                        &mut must_report_cdc_offset_once,
                        must_wait_cdc_offset_before_report,
                    );

                    // when handle a checkpoint barrier, spawn a task to wait for epoch commit notification
                    if barrier.kind.is_checkpoint()
                        && let Some(task_builder) = &mut wait_checkpoint_task_builder
                    {
                        task_builder.update_task_on_checkpoint(updated_splits);

                        tracing::debug!("epoch to wait {:?}", epoch);
                        task_builder.send(Epoch(epoch.prev));
                    }

                    let barrier_epoch = barrier.epoch;
                    yield Message::Barrier(barrier);

                    if let Some((source_desc, stream, to_apply_mutation)) = split_change {
                        self.apply_split_change_after_yield_barrier(
                            barrier_epoch,
                            source_desc,
                            stream,
                            to_apply_mutation,
                        )
                        .await?;
                    }
                }
                Either::Left(_) => {
                    // For the source executor, the message we receive from this arm
                    // should always be barrier message.
                    unreachable!();
                }

                Either::Right(event) => {
                    let (chunk, latest_state) = match event {
                        SourceReaderEventWithState::Progress(latest_state) => {
                            self.apply_latest_split_state(latest_state);
                            continue;
                        }
                        SourceReaderEventWithState::Data((chunk, latest_state)) => {
                            (chunk, latest_state)
                        }
                    };

                    if let Some(task_builder) = &mut wait_checkpoint_task_builder {
                        if let Some(pulsar_message_id_idx) = pulsar_message_id_idx {
                            if chunk.capacity() > 0 {
                                // Each Pulsar chunk comes from one split, so all rows have the same
                                // split ID.
                                let (_, row, _) = chunk.row_at(0);
                                let split_id = SplitId::from(
                                    row.datum_at(split_idx).unwrap().into_utf8().to_owned(),
                                );
                                let pulsar_message_id_col = chunk.column_at(pulsar_message_id_idx);
                                task_builder.update_task_on_chunk(
                                    self.actor_ctx.id,
                                    source_id,
                                    Some(&split_id),
                                    pulsar_message_id_col.clone(),
                                );
                            }
                        } else {
                            let offset_col = chunk.column_at(offset_idx);
                            task_builder.update_task_on_chunk(
                                self.actor_ctx.id,
                                source_id,
                                None,
                                offset_col.clone(),
                            );
                        }
                    }
                    if last_barrier_time.elapsed().as_millis() > max_wait_barrier_time_ms {
                        // Exceeds the max wait barrier time, the source will be paused.
                        // Currently we can guarantee the
                        // source is not paused since it received stream
                        // chunks.
                        self_paused = true;
                        tracing::warn!(
                            "source paused, wait barrier for {:?}",
                            last_barrier_time.elapsed()
                        );
                        stream.pause_stream();

                        // Only update `max_wait_barrier_time_ms` to capture
                        // `barrier_interval_ms`
                        // changes here to avoid frequently accessing the shared
                        // `system_params`.
                        max_wait_barrier_time_ms = self.system_params.load().barrier_interval_ms()
                            as u128
                            * WAIT_BARRIER_MULTIPLE_TIMES;
                    }

                    self.apply_latest_split_state(latest_state);

                    let card = chunk.cardinality();
                    if card == 0 {
                        continue;
                    }
                    source_output_row_count.inc_by(card as u64);
                    let to_remove_col_indices =
                        if let Some(pulsar_message_id_idx) = pulsar_message_id_idx {
                            vec![split_idx, offset_idx, pulsar_message_id_idx]
                        } else {
                            vec![split_idx, offset_idx]
                        };
                    let chunk =
                        prune_additional_cols(&chunk, &to_remove_col_indices, &source_desc.columns);
                    yield Message::Chunk(chunk);
                    self.try_flush_data().await?;
                }
            }
        }

        // The source executor should only be stopped by the actor when finding a `Stop` mutation.
        tracing::error!(
            actor_id = %self.actor_ctx.id,
            "source executor exited unexpectedly"
        )
    }
}

#[derive(Debug, Clone)]
enum ApplyMutationAfterBarrier<'a> {
    SplitChange {
        target_splits: Vec<SplitImpl>,
        should_trim_state: bool,
        split_change_count: &'a LabelGuardedMetric<GenericCounter<AtomicU64>>,
    },
    ConnectorPropsChange,
}

impl<S: StateStore> Execute for SourceExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl<S: StateStore> Debug for SourceExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceExecutor")
            .field("source_id", &self.stream_source_core.source_id)
            .field("column_ids", &self.stream_source_core.column_ids)
            .finish()
    }
}

struct WaitCheckpointTaskBuilder {
    wait_checkpoint_tx: UnboundedSender<(Epoch, WaitCheckpointTask)>,
    building_task: WaitCheckpointTask,
}

impl WaitCheckpointTaskBuilder {
    fn update_task_on_chunk(
        &mut self,
        actor_id: ActorId,
        source_id: SourceId,
        pulsar_split_id: Option<&SplitId>,
        offset_col: ArrayRef,
    ) {
        match &mut self.building_task {
            WaitCheckpointTask::AckPubsubMessage(_, arrays) => {
                arrays.push(offset_col);
            }
            WaitCheckpointTask::AckNatsJetStream(_, arrays, _) => {
                arrays.push(offset_col);
            }
            WaitCheckpointTask::AckPulsarMessage(arrays) => {
                let split_id = pulsar_split_id.expect("Pulsar chunk must have a split ID");
                let pulsar_ack_channel_id =
                    build_pulsar_ack_channel_id(source_id, split_id, actor_id);
                arrays.push((pulsar_ack_channel_id, offset_col));
            }
            WaitCheckpointTask::CommitCdcOffset(_) => {}
        }
    }

    fn update_task_on_checkpoint(&mut self, updated_splits: HashMap<SplitId, SplitImpl>) {
        if let WaitCheckpointTask::CommitCdcOffset(offsets) = &mut self.building_task
            && !updated_splits.is_empty()
        {
            // cdc source only has one split
            assert_eq!(1, updated_splits.len());
            for (split_id, split_impl) in updated_splits {
                if split_impl.is_cdc_split() {
                    *offsets = Some((split_id, split_impl.get_cdc_split_offset()));
                } else {
                    unreachable!()
                }
            }
        }
    }

    /// Send the current task and reset to an empty one for the next epoch.
    /// Uses `reset_for_next_epoch()` to clone the client handle (e.g. `PubSub` `Subscription`)
    /// from the current task, avoiding network I/O on the checkpoint hot path.
    fn send(&mut self, epoch: Epoch) {
        let new_task = self.building_task.reset_for_next_epoch();
        self.wait_checkpoint_tx
            .send((epoch, std::mem::replace(&mut self.building_task, new_task)))
            .expect("wait_checkpoint_tx send should succeed");
    }
}

/// A worker used to do some work after each checkpoint epoch is committed.
///
/// # Usage Cases
///
/// Typically there are 2 issues related with ack on checkpoint:
///
/// 1. Correctness (at-least-once), or don't let upstream clean uncommitted data.
///    For message queueing semantics (delete after ack), we should ack to avoid redelivery,
///    and only ack after checkpoint to avoid data loss.
///
/// 2. Allow upstream to clean data after commit.
///
/// See also <https://github.com/risingwavelabs/risingwave/issues/16736#issuecomment-2109379790>
///
/// ## CDC
///
/// Commit last consumed offset to upstream DB, so that old data can be discarded.
///
/// ## Google Pub/Sub
///
/// Due to queueing semantics.
/// Although Pub/Sub supports `retain_acked_messages` and `seek` functionality,
/// it's quite limited unlike Kafka.
///
/// See also <https://cloud.google.com/pubsub/docs/subscribe-best-practices#process-messages>
struct WaitCheckpointWorker<S: StateStore> {
    wait_checkpoint_rx: UnboundedReceiver<(Epoch, WaitCheckpointTask)>,
    state_store: S,
    table_id: TableId,
    source_id: SourceId,
    source_name: String,
    metrics: Arc<StreamingMetrics>,
}

impl<S: StateStore> WaitCheckpointWorker<S> {
    pub async fn run(mut self) {
        tracing::debug!("wait epoch worker start success");
        loop {
            // poll the rx and wait for the epoch commit
            match self.wait_checkpoint_rx.recv().await {
                Some((epoch, task)) => {
                    tracing::debug!("start to wait epoch {}", epoch.0);
                    let ret = self
                        .state_store
                        .try_wait_epoch(
                            HummockReadEpoch::Committed(epoch.0),
                            TryWaitEpochOptions {
                                table_id: self.table_id,
                            },
                        )
                        .await;
                    if self.wait_checkpoint_rx.is_closed() {
                        // The task must not be committed since the associated epoch may not succeed.
                        // The wait_checkpoint_rx lifetime is tied to the lifecycle of the source executor actor.
                        // The old actor must be dropped before any subsequent recovery can succeed; see PartialGraphState::abort_and_wait_actors
                        tracing::debug!(epoch = epoch.0, "Drop stale wait checkpoint task.");
                        break;
                    }
                    match ret {
                        Ok(()) => {
                            tracing::debug!(epoch = epoch.0, "wait epoch success");

                            // Run task with callback to record LSN after successful commit
                            task.run_with_on_commit_success(
                                self.source_id,
                                &self.source_name,
                                |source_id: u64, offset| {
                                    if let Some(lsn_value) =
                                        extract_postgres_lsn_from_offset_str(offset)
                                    {
                                        self.metrics
                                            .pg_cdc_jni_commit_offset_lsn
                                            .with_guarded_label_values(&[&source_id.to_string()])
                                            .set(lsn_value as i64);
                                    }
                                    if let Some(lsn_value) =
                                        extract_sql_server_commit_lsn_from_offset_str(offset)
                                    {
                                        self.metrics
                                            .sqlserver_cdc_jni_commit_offset_lsn
                                            .with_guarded_label_values(&[&source_id.to_string()])
                                            .set(lsn_u128_to_i64(lsn_value));
                                    }
                                },
                            )
                            .await;
                        }
                        Err(e) => {
                            tracing::error!(
                            error = %e.as_report(),
                            "wait epoch {} failed", epoch.0
                            );
                        }
                    }
                }
                None => {
                    tracing::error!("wait epoch rx closed");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use maplit::{btreemap, convert_args, hashmap};
    use prometheus::Registry;
    use prometheus::core::Collector;
    use risingwave_common::array::{Array, BytesArray};
    use risingwave_common::catalog::{ColumnId, Field};
    use risingwave_common::config::MetricLevel;
    use risingwave_common::id::SourceId;
    use risingwave_common::system_param::local_manager::LocalSystemParamsManager;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::util::epoch::{EpochExt, test_epoch};
    use risingwave_connector::source::cdc::{DebeziumCdcSplit, Mysql};
    use risingwave_connector::source::datagen::DatagenSplit;
    use risingwave_connector::source::reader::desc::test_utils::create_source_desc_builder;
    use risingwave_pb::catalog::StreamSourceInfo;
    use risingwave_pb::plan_common::PbRowFormatType;
    use risingwave_storage::memory::MemoryStateStore;
    use tokio::sync::mpsc::unbounded_channel;
    use tracing_test::traced_test;

    use super::*;
    use crate::executor::AddMutation;
    use crate::executor::source::{SourceStateTableHandler, default_source_internal_table};
    use crate::task::LocalBarrierManager;

    const MOCK_SOURCE_NAME: &str = "mock_source";

    fn message_ids<const N: usize>(values: [Option<&[u8]>; N]) -> ArrayRef {
        BytesArray::from_iter(values).into_ref()
    }

    #[test]
    fn test_build_pulsar_ack_task_for_interleaved_splits() {
        let (wait_checkpoint_tx, _wait_checkpoint_rx) = unbounded_channel();
        let mut builder = WaitCheckpointTaskBuilder {
            wait_checkpoint_tx,
            building_task: WaitCheckpointTask::AckPulsarMessage(vec![]),
        };
        let actor_id = ActorId::new(11);
        let source_id = SourceId::new(7);
        let split_a = SplitId::from("persistent://public/default/topic-a");
        let split_b = SplitId::from("persistent://public/default/topic-b");

        builder.update_task_on_chunk(
            actor_id,
            source_id,
            Some(&split_a),
            message_ids([Some(b"a-0")]),
        );
        builder.update_task_on_chunk(
            actor_id,
            source_id,
            Some(&split_b),
            message_ids([Some(b"b-0")]),
        );
        builder.update_task_on_chunk(
            actor_id,
            source_id,
            Some(&split_a),
            message_ids([Some(b"a-1")]),
        );

        let WaitCheckpointTask::AckPulsarMessage(ack_arrays) = builder.building_task else {
            unreachable!();
        };
        let channel_ids = ack_arrays
            .into_iter()
            .map(|(channel_id, _)| channel_id)
            .collect_vec();

        assert_eq!(
            channel_ids,
            vec![
                build_pulsar_ack_channel_id(source_id, &split_a, actor_id),
                build_pulsar_ack_channel_id(source_id, &split_b, actor_id),
                build_pulsar_ack_channel_id(source_id, &split_a, actor_id),
            ]
        );
    }

    #[tokio::test]
    async fn test_recovered_mysql_cdc_state_file_seq_metric_is_initialized()
    -> StreamExecutorResult<()> {
        let mut state_table_handler = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            MemoryStateStore::new(),
        )
        .await;
        let offset = r#"{"sourceOffset":{"file":"binlog.000123","pos":45678}}"#;
        let persisted_split = SplitImpl::MysqlCdc(DebeziumCdcSplit::<Mysql>::new(
            1001,
            Some(offset.to_owned()),
            None,
        ));
        let uninitialized_split =
            SplitImpl::MysqlCdc(DebeziumCdcSplit::<Mysql>::new(1001, None, None));
        let epoch_1 = EpochPair::new_test_epoch(test_epoch(1));
        let epoch_2 = EpochPair::new_test_epoch(test_epoch(2));
        let epoch_3 = EpochPair::new_test_epoch(test_epoch(3));

        state_table_handler.init_epoch(epoch_1).await?;
        state_table_handler
            .set_states(vec![persisted_split.clone()])
            .await?;
        state_table_handler
            .state_table_mut()
            .commit_for_test(epoch_2)
            .await?;
        state_table_handler
            .state_table_mut()
            .commit_for_test(epoch_3)
            .await?;

        let recovered_split = state_table_handler
            .new_committed_reader(epoch_3)
            .await?
            .try_recover_from_state_store(&uninitialized_split)
            .await?
            .unwrap();
        assert_eq!(recovered_split, persisted_split);

        let registry = Registry::new();
        let metrics = StreamingMetrics::new(&registry, MetricLevel::Debug);
        let mut metric_guard = None;
        update_mysql_cdc_state_file_seq_metric(&mut metric_guard, &metrics, "1", &recovered_split);

        for _ in 0..2 {
            let file_seq = metrics
                .mysql_cdc_state_binlog_file_seq
                .collect()
                .pop()
                .unwrap();
            assert_eq!(
                123.0,
                file_seq.get_metric()[0]
                    .get_gauge()
                    .as_ref()
                    .unwrap()
                    .value()
            );
        }

        Ok(())
    }

    #[test]
    fn test_mysql_cdc_state_file_seq_metric_is_cleared() {
        let registry = Registry::new();
        let metrics = StreamingMetrics::new(&registry, MetricLevel::Debug);
        let mut metric_guard = None;
        let split_with_offset = SplitImpl::MysqlCdc(DebeziumCdcSplit::<Mysql>::new(
            1001,
            Some(r#"{"sourceOffset":{"file":"binlog.000123","pos":45678}}"#.to_owned()),
            None,
        ));
        let split_without_offset =
            SplitImpl::MysqlCdc(DebeziumCdcSplit::<Mysql>::new(1001, None, None));

        update_mysql_cdc_state_file_seq_metric(
            &mut metric_guard,
            &metrics,
            "1",
            &split_with_offset,
        );
        assert!(metric_guard.is_some());

        update_mysql_cdc_state_file_seq_metric(
            &mut metric_guard,
            &metrics,
            "1",
            &split_without_offset,
        );
        assert!(metric_guard.is_none());

        drop(metrics.mysql_cdc_state_binlog_file_seq.collect());
        assert!(
            metrics
                .mysql_cdc_state_binlog_file_seq
                .collect()
                .pop()
                .unwrap()
                .get_metric()
                .is_empty()
        );

        update_mysql_cdc_state_file_seq_metric(
            &mut metric_guard,
            &metrics,
            "1",
            &split_with_offset,
        );
        assert!(metric_guard.is_some());

        clear_mysql_cdc_state_file_seq_metric_if_unassigned(&mut metric_guard, &HashMap::new());
        assert!(metric_guard.is_none());

        drop(metrics.mysql_cdc_state_binlog_file_seq.collect());
        assert!(
            metrics
                .mysql_cdc_state_binlog_file_seq
                .collect()
                .pop()
                .unwrap()
                .get_metric()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_source_executor() {
        let source_id = 0.into();
        let schema = Schema {
            fields: vec![Field::with_name(DataType::Int32, "sequence_int")],
        };
        let row_id_index = None;
        let source_info = StreamSourceInfo {
            row_format: PbRowFormatType::Native as i32,
            ..Default::default()
        };
        let (barrier_tx, barrier_rx) = unbounded_channel::<Barrier>();
        let column_ids = vec![0].into_iter().map(ColumnId::from).collect();

        // This datagen will generate 3 rows at one time.
        let properties = convert_args!(btreemap!(
            "connector" => "datagen",
            "datagen.rows.per.second" => "3",
            "fields.sequence_int.kind" => "sequence",
            "fields.sequence_int.start" => "11",
            "fields.sequence_int.end" => "11111",
        ));
        let source_desc_builder =
            create_source_desc_builder(&schema, row_id_index, source_info, properties, vec![]);
        let split_state_store = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            MemoryStateStore::new(),
        )
        .await;
        let core = StreamSourceCore::<MemoryStateStore> {
            source_id,
            column_ids,
            source_desc_builder: Some(source_desc_builder),
            latest_split_info: HashMap::new(),
            split_state_store,
            updated_splits_in_epoch: HashMap::new(),
            source_name: MOCK_SOURCE_NAME.to_owned(),
        };

        let system_params_manager = LocalSystemParamsManager::for_test();

        let executor = SourceExecutor::new(
            ActorContext::for_test(0),
            core,
            Arc::new(StreamingMetrics::unused()),
            barrier_rx,
            system_params_manager.get_params(),
            None,
            false,
            LocalBarrierManager::for_test(),
        );
        let mut executor = executor.boxed().execute();

        let init_barrier =
            Barrier::new_test_barrier(test_epoch(1)).with_mutation(Mutation::Add(AddMutation {
                splits: hashmap! {
                    ActorId::default() => vec![
                        SplitImpl::Datagen(DatagenSplit {
                            split_index: 0,
                            split_num: 1,
                            start_offset: None,
                        }),
                    ],
                },
                ..Default::default()
            }));
        barrier_tx.send(init_barrier).unwrap();

        // Consume barrier.
        executor.next().await.unwrap().unwrap();

        // Consume data chunk.
        let msg = executor.next().await.unwrap().unwrap();

        // Row id will not be filled here.
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " i
                + 11
                + 12
                + 13"
            )
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_split_change_mutation() {
        let source_id = SourceId::new(0);
        let schema = Schema {
            fields: vec![Field::with_name(DataType::Int32, "v1")],
        };
        let row_id_index = None;
        let source_info = StreamSourceInfo {
            row_format: PbRowFormatType::Native as i32,
            ..Default::default()
        };
        let properties = convert_args!(btreemap!(
            "connector" => "datagen",
            "fields.v1.kind" => "sequence",
            "fields.v1.start" => "11",
            "fields.v1.end" => "11111",
        ));

        let source_desc_builder =
            create_source_desc_builder(&schema, row_id_index, source_info, properties, vec![]);
        let mem_state_store = MemoryStateStore::new();

        let column_ids = vec![ColumnId::from(0)];
        let (barrier_tx, barrier_rx) = unbounded_channel::<Barrier>();
        let split_state_store = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            mem_state_store.clone(),
        )
        .await;

        let core = StreamSourceCore::<MemoryStateStore> {
            source_id,
            column_ids: column_ids.clone(),
            source_desc_builder: Some(source_desc_builder),
            latest_split_info: HashMap::new(),
            split_state_store,
            updated_splits_in_epoch: HashMap::new(),
            source_name: MOCK_SOURCE_NAME.to_owned(),
        };

        let system_params_manager = LocalSystemParamsManager::for_test();

        let executor = SourceExecutor::new(
            ActorContext::for_test(0),
            core,
            Arc::new(StreamingMetrics::unused()),
            barrier_rx,
            system_params_manager.get_params(),
            None,
            false,
            LocalBarrierManager::for_test(),
        );
        let mut handler = executor.boxed().execute();

        let mut epoch = test_epoch(1);
        let init_barrier =
            Barrier::new_test_barrier(epoch).with_mutation(Mutation::Add(AddMutation {
                splits: hashmap! {
                    ActorId::default() => vec![
                        SplitImpl::Datagen(DatagenSplit {
                            split_index: 0,
                            split_num: 3,
                            start_offset: None,
                        }),
                    ],
                },
                ..Default::default()
            }));
        barrier_tx.send(init_barrier).unwrap();

        // Consume barrier.
        handler
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_barrier()
            .unwrap();

        let mut ready_chunks = handler.ready_chunks(10);

        let _ = ready_chunks.next().await.unwrap();

        let new_assignment = vec![
            SplitImpl::Datagen(DatagenSplit {
                split_index: 0,
                split_num: 3,
                start_offset: None,
            }),
            SplitImpl::Datagen(DatagenSplit {
                split_index: 1,
                split_num: 3,
                start_offset: None,
            }),
            SplitImpl::Datagen(DatagenSplit {
                split_index: 2,
                split_num: 3,
                start_offset: None,
            }),
        ];

        epoch.inc_epoch();
        let change_split_mutation =
            Barrier::new_test_barrier(epoch).with_mutation(Mutation::SourceChangeSplit(hashmap! {
                ActorId::default() => new_assignment.clone()
            }));

        barrier_tx.send(change_split_mutation).unwrap();

        let _ = ready_chunks.next().await.unwrap(); // barrier

        epoch.inc_epoch();
        let barrier = Barrier::new_test_barrier(epoch);
        barrier_tx.send(barrier).unwrap();

        ready_chunks.next().await.unwrap(); // barrier

        let mut source_state_handler = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            mem_state_store.clone(),
        )
        .await;

        // there must exist state for new add partition
        source_state_handler
            .init_epoch(EpochPair::new_test_epoch(epoch))
            .await
            .unwrap();
        source_state_handler
            .get(&new_assignment[1].id())
            .await
            .unwrap()
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let _ = ready_chunks.next().await.unwrap();

        epoch.inc_epoch();
        let barrier = Barrier::new_test_barrier(epoch).with_mutation(Mutation::Pause);
        barrier_tx.send(barrier).unwrap();

        epoch.inc_epoch();
        let barrier = Barrier::new_test_barrier(epoch).with_mutation(Mutation::Resume);
        barrier_tx.send(barrier).unwrap();

        // receive all
        ready_chunks.next().await.unwrap();

        let prev_assignment = new_assignment;
        let new_assignment = vec![prev_assignment[2].clone()];

        epoch.inc_epoch();
        let drop_split_mutation =
            Barrier::new_test_barrier(epoch).with_mutation(Mutation::SourceChangeSplit(hashmap! {
                ActorId::default() => new_assignment.clone()
            }));

        barrier_tx.send(drop_split_mutation).unwrap();

        ready_chunks.next().await.unwrap(); // barrier

        epoch.inc_epoch();
        let barrier = Barrier::new_test_barrier(epoch);
        barrier_tx.send(barrier).unwrap();

        ready_chunks.next().await.unwrap(); // barrier

        let mut source_state_handler = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            mem_state_store.clone(),
        )
        .await;

        let new_epoch = EpochPair::new_test_epoch(epoch);
        source_state_handler.init_epoch(new_epoch).await.unwrap();

        let committed_reader = source_state_handler
            .new_committed_reader(new_epoch)
            .await
            .unwrap();
        assert!(
            committed_reader
                .try_recover_from_state_store(&prev_assignment[0])
                .await
                .unwrap()
                .is_none()
        );

        assert!(
            committed_reader
                .try_recover_from_state_store(&prev_assignment[1])
                .await
                .unwrap()
                .is_none()
        );

        assert!(
            committed_reader
                .try_recover_from_state_store(&prev_assignment[2])
                .await
                .unwrap()
                .is_some()
        );
    }
}
