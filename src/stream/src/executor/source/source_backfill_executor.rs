// Copyright 2024 RisingWave Labs
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

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use anyhow::anyhow;
use either::Either;
use futures::stream::{PollNext, select_with_strategy};
use itertools::Itertools;
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::catalog::ColumnId;
use risingwave_common::id::SourceId;
use risingwave_common::metrics::{GLOBAL_ERROR_METRICS, LabelGuardedIntCounter};
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::types::JsonbVal;
use risingwave_common::util::epoch::EpochPair;
use risingwave_connector::source::reader::desc::{SourceDesc, SourceDescBuilder};
use risingwave_connector::source::{
    BackfillInfo, BoxSourceReaderEventStream, SourceContext, SourceCtrlOpts, SourceReaderEvent,
    SplitId, SplitImpl, SplitMetaData,
};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::common::ThrottleType;
use risingwave_storage::store::TryWaitEpochOptions;
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;

use super::executor_core::StreamSourceCore;
use super::source_backfill_state_table::BackfillStateTableHandler;
use super::{apply_rate_limit_to_source_reader_event, get_split_offset_col_idx};
use crate::common::rate_limit::limited_chunk_size;
use crate::executor::UpdateMutation;
use crate::executor::prelude::*;
use crate::executor::source::source_executor::WAIT_BARRIER_MULTIPLE_TIMES;
use crate::task::CreateMviewProgressReporter;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum BackfillState {
    /// `None` means not started yet. It's the initial state.
    /// XXX: perhaps we can also set to low-watermark instead of `None`
    Backfilling(Option<String>),
    /// Backfill is stopped at this offset (inclusive). Source needs to filter out messages before this offset.
    SourceCachingUp(String),
    Finished,
}
pub type BackfillStates = HashMap<SplitId, BackfillStateWithProgress>;

/// Only `state` field is the real state for fail-over.
/// Other fields are for observability (but we still need to persist them).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct BackfillStateWithProgress {
    pub state: BackfillState,
    pub num_consumed_rows: u64,
    /// The latest offset from upstream (inclusive). After we reach this offset, we can stop backfilling.
    /// This is initialized with the latest available offset in the connector (if the connector provides the ability to fetch it)
    /// so that we can finish backfilling even when upstream doesn't emit any data.
    pub target_offset: Option<String>,
}

impl BackfillStateWithProgress {
    pub fn encode_to_json(self) -> JsonbVal {
        serde_json::to_value(self).unwrap().into()
    }

    pub fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
        serde_json::from_value(value.take()).map_err(|e| anyhow!(e))
    }
}

pub struct SourceBackfillExecutor<S: StateStore> {
    pub inner: SourceBackfillExecutorInner<S>,
    /// Upstream changelog stream which may contain metadata columns, e.g. `_rw_offset`
    pub input: Executor,
}

pub struct SourceBackfillExecutorInner<S: StateStore> {
    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Streaming source for external
    source_id: SourceId,
    source_name: String,
    column_ids: Vec<ColumnId>,
    source_desc_builder: Option<SourceDescBuilder>,
    backfill_state_store: BackfillStateTableHandler<S>,

    /// Metrics for monitor.
    metrics: Arc<StreamingMetrics>,
    source_split_change_count: LabelGuardedIntCounter,

    // /// Receiver of barrier channel.
    // barrier_receiver: Option<UnboundedReceiver<Barrier>>,
    /// System parameter reader to read barrier interval
    system_params: SystemParamsReaderRef,

    /// Rate limit in rows/s.
    rate_limit_rps: Option<u32>,

    progress: CreateMviewProgressReporter,
}

/// Local variables used in the backfill stage.
///
/// See <https://github.com/risingwavelabs/risingwave/issues/18299> for a state diagram about how it works.
///
/// Note: all off the fields should contain all available splits, and we can `unwrap()` safely when `get()`.
#[derive(Debug)]
struct BackfillStage {
    states: BackfillStates,
    /// A copy of all splits (incl unfinished and finished ones) assigned to the actor.
    ///
    /// Note: the offsets are not updated. Should use `state`'s offset to update before using it (`get_latest_unfinished_splits`).
    splits: Vec<SplitImpl>,
}

enum ApplyMutationAfterBarrier {
    SourceChangeSplit {
        target_splits: Vec<SplitImpl>,
        should_trim_state: bool,
    },
    ConnectorPropsChange,
}

impl BackfillStage {
    fn total_backfilled_rows(&self) -> u64 {
        self.states.values().map(|s| s.num_consumed_rows).sum()
    }

    fn debug_assert_consistent(&self) {
        if cfg!(debug_assertions) {
            let all_splits: HashSet<_> = self.splits.iter().map(|split| split.id()).collect();
            assert_eq!(
                self.states.keys().cloned().collect::<HashSet<_>>(),
                all_splits
            );
        }
    }

    /// Get unfinished splits with latest offsets according to the backfill states.
    fn get_latest_unfinished_splits(&self) -> StreamExecutorResult<Vec<SplitImpl>> {
        let mut unfinished_splits = Vec::new();
        for split in &self.splits {
            let state = &self.states.get(split.id().as_ref()).unwrap().state;
            match state {
                BackfillState::Backfilling(Some(offset)) => {
                    let mut updated_split = split.clone();
                    updated_split.update_in_place(offset.clone())?;
                    unfinished_splits.push(updated_split);
                }
                BackfillState::Backfilling(None) => unfinished_splits.push(split.clone()),
                BackfillState::SourceCachingUp(_) | BackfillState::Finished => {}
            }
        }
        Ok(unfinished_splits)
    }

    /// Updates backfill states and `target_offsets` from upstream `SourceExecutor` chunks, and
    /// returns whether the row is visible.
    ///
    /// Note that the online catch-up logic intentionally relies on the upstream chunk stream
    /// carrying split/offset columns. We do not reconcile live upstream progress from source state
    /// tables here, because source backfill is designed to advance according to the upstream data
    /// stream instead of a side channel.
    fn handle_upstream_row(&mut self, split_id: &str, offset: &str) -> bool {
        let mut vis = false;
        let state = self.states.get_mut(split_id).unwrap();
        let state_inner = &mut state.state;
        match state_inner {
            BackfillState::Backfilling(None) => {
                // backfilling for this split is not started yet. Ignore this row
            }
            BackfillState::Backfilling(Some(backfill_offset)) => {
                match compare_kafka_offset(backfill_offset, offset) {
                    Ordering::Less => {
                        // continue backfilling. Ignore this row
                    }
                    Ordering::Equal => {
                        // backfilling for this split is finished just right.
                        *state_inner = BackfillState::Finished;
                    }
                    Ordering::Greater => {
                        // backfilling for this split produced more data than current source's progress.
                        // We should stop backfilling, and filter out rows from upstream with offset <= backfill_offset.
                        *state_inner = BackfillState::SourceCachingUp(backfill_offset.clone());
                    }
                }
            }
            BackfillState::SourceCachingUp(backfill_offset) => {
                match compare_kafka_offset(backfill_offset, offset) {
                    Ordering::Less => {
                        // Source caught up, but doesn't contain the last backfilled row.
                        // This may happen e.g., if Kafka performed compaction.
                        vis = true;
                        *state_inner = BackfillState::Finished;
                    }
                    Ordering::Equal => {
                        // Source just caught up with backfilling.
                        *state_inner = BackfillState::Finished;
                    }
                    Ordering::Greater => {
                        // Source is still behind backfilling.
                    }
                }
            }
            BackfillState::Finished => {
                vis = true;
                // This split's backfilling is finished, we are waiting for other splits
            }
        }
        if matches!(state_inner, BackfillState::Backfilling(_)) {
            state.target_offset = Some(offset.to_owned());
        }
        if vis {
            debug_assert_eq!(*state_inner, BackfillState::Finished);
        }
        vis
    }

    /// Updates backfill states and returns whether the row backfilled from external system is visible.
    fn handle_backfill_row(&mut self, split_id: &str, offset: &str) -> bool {
        let state = self.states.get_mut(split_id).unwrap();
        state.num_consumed_rows += 1;
        let state_inner = &mut state.state;
        match state_inner {
            BackfillState::Backfilling(_old_offset) => {
                if let Some(target_offset) = &state.target_offset
                    && compare_kafka_offset(offset, target_offset).is_ge()
                {
                    // Note1: If target_offset = offset, it seems we can mark the state as Finished without waiting for upstream to catch up
                    // and dropping duplicated messages.
                    // But it's not true if target_offset is fetched from other places, like Kafka high watermark.
                    // In this case, upstream hasn't reached the target_offset yet.
                    //
                    // Note2: after this, all following rows in the current chunk will be invisible.
                    //
                    // Note3: if target_offset is None (e.g., when upstream doesn't emit messages at all), we will
                    // keep backfilling.
                    *state_inner = BackfillState::SourceCachingUp(offset.to_owned());
                } else {
                    *state_inner = BackfillState::Backfilling(Some(offset.to_owned()));
                }
                true
            }
            BackfillState::SourceCachingUp(_) | BackfillState::Finished => {
                // backfilling stopped. ignore
                false
            }
        }
    }

    /// Updates backfill state with split progress emitted by the backfill reader when there are no
    /// corresponding visible rows.
    ///
    /// This is needed for connectors such as Kafka under `read_committed`, where the reader can
    /// reach a later offset because of EOF/control records without yielding a data chunk. This
    /// supplements `handle_backfill_row`, but does not replace the upstream-chunk-based progress
    /// tracking in `handle_upstream_row`.
    fn handle_backfill_progress(&mut self, split_id: &str, offset: &str) {
        let Some(state) = self.states.get_mut(split_id) else {
            return;
        };
        let state_inner = &mut state.state;
        match state_inner {
            BackfillState::Backfilling(_old_offset) => {
                if let Some(target_offset) = &state.target_offset
                    && compare_kafka_offset(offset, target_offset).is_ge()
                {
                    *state_inner = BackfillState::SourceCachingUp(offset.to_owned());
                } else {
                    *state_inner = BackfillState::Backfilling(Some(offset.to_owned()));
                }
            }
            BackfillState::SourceCachingUp(backfill_offset) => {
                if compare_kafka_offset(backfill_offset, offset).is_lt() {
                    *backfill_offset = offset.to_owned();
                }
            }
            BackfillState::Finished => {}
        }
    }
}

impl<S: StateStore> SourceBackfillExecutorInner<S> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        info: ExecutorInfo,
        stream_source_core: StreamSourceCore<S>,
        metrics: Arc<StreamingMetrics>,
        system_params: SystemParamsReaderRef,
        backfill_state_store: BackfillStateTableHandler<S>,
        rate_limit_rps: Option<u32>,
        progress: CreateMviewProgressReporter,
    ) -> Self {
        let StreamSourceCore {
            source_id,
            source_name,
            column_ids,
            source_desc_builder,
            ..
        } = stream_source_core;
        let source_split_change_count = metrics
            .source_split_change_count
            .with_guarded_label_values(&[
                &actor_ctx.id.to_string(),
                &source_id.to_string(),
                &source_name,
                &actor_ctx.fragment_id.to_string(),
            ]);

        Self {
            actor_ctx,
            info,
            source_id,
            source_name,
            column_ids,
            source_desc_builder,
            backfill_state_store,
            metrics,
            source_split_change_count,
            system_params,
            rate_limit_rps,
            progress,
        }
    }

    async fn build_stream_source_reader(
        &self,
        source_desc: &SourceDesc,
        splits: Vec<SplitImpl>,
    ) -> StreamExecutorResult<(BoxSourceReaderEventStream, HashMap<SplitId, BackfillInfo>)> {
        let column_ids = source_desc
            .columns
            .iter()
            .map(|column_desc| column_desc.column_id)
            .collect_vec();
        let source_ctx = SourceContext::new(
            self.actor_ctx.id,
            self.source_id,
            self.actor_ctx.fragment_id,
            self.source_name.clone(),
            source_desc.metrics.clone(),
            SourceCtrlOpts {
                chunk_size: limited_chunk_size(self.rate_limit_rps),
                split_txn: self.rate_limit_rps.is_some(), // when rate limiting, we may split txn
            },
            source_desc.source.config.clone(),
            None,
        );

        // We will check watermark to decide whether we need to backfill.
        // e.g., when there's a Kafka topic-partition without any data,
        // we don't need to backfill at all. But if we do not check here,
        // the executor can only know it's finished when data coming in.
        // For blocking DDL, this would be annoying.

        let (stream, res) = source_desc
            .source
            .build_stream(Some(splits), column_ids, Arc::new(source_ctx), false)
            .await
            .map_err(StreamExecutorError::connector_error)?;
        Ok((
            apply_rate_limit_to_source_reader_event(stream, self.rate_limit_rps).boxed(),
            res.backfill_info,
        ))
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute(mut self, input: Executor) {
        let mut input = input.execute();

        // Poll the upstream to get the first barrier.
        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        let owned_splits = barrier
            .initial_split_assignment(self.actor_ctx.id)
            .unwrap_or(&[])
            .to_vec();

        let mut pause_control = PauseControl::new();
        if barrier.is_backfill_pause_on_startup(self.actor_ctx.fragment_id) {
            pause_control.backfill_pause();
        }
        if barrier.is_pause_on_startup() {
            pause_control.command_pause();
        }
        yield Message::Barrier(barrier);

        let source_desc_builder: SourceDescBuilder = self.source_desc_builder.take().unwrap();
        let mut source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;

        // source backfill only applies to kafka, so we don't need to get pulsar's `message_id_data_idx`.
        let (Some(split_idx), Some(offset_idx), _) = get_split_offset_col_idx(&source_desc.columns)
        else {
            unreachable!("Partition and offset columns must be set.");
        };

        self.backfill_state_store.init_epoch(first_epoch).await?;

        let mut backfill_states: BackfillStates = HashMap::new();
        {
            let committed_reader = self
                .backfill_state_store
                .new_committed_reader(first_epoch)
                .await?;
            for split in &owned_splits {
                let split_id = split.id();
                let backfill_state = committed_reader
                    .try_recover_from_state_store(&split_id)
                    .await?
                    .unwrap_or(BackfillStateWithProgress {
                        state: BackfillState::Backfilling(None),
                        num_consumed_rows: 0,
                        target_offset: None, // init with None
                    });
                backfill_states.insert(split_id, backfill_state);
            }
        }
        let mut backfill_stage = BackfillStage {
            states: backfill_states,
            splits: owned_splits,
        };
        backfill_stage.debug_assert_consistent();

        let (source_chunk_reader, backfill_info) = self
            .build_stream_source_reader(
                &source_desc,
                backfill_stage.get_latest_unfinished_splits()?,
            )
            .instrument_await("source_build_reader")
            .await?;
        for (split_id, info) in &backfill_info {
            let state = backfill_stage.states.get_mut(split_id).unwrap();
            match info {
                BackfillInfo::NoDataToBackfill => {
                    state.state = BackfillState::Finished;
                }
                BackfillInfo::HasDataToBackfill { latest_offset } => {
                    // Note: later we will override it with the offset from the source message, and it's possible to become smaller than this value.
                    state.target_offset = Some(latest_offset.clone());
                }
            }
        }
        tracing::debug!(?backfill_stage, "source backfill started");

        fn select_strategy(_: &mut ()) -> PollNext {
            futures::stream::PollNext::Left
        }

        // We choose "preferring upstream" strategy here, because:
        // - When the upstream source's workload is high (i.e., Kafka has new incoming data), it just makes backfilling slower.
        //   For chunks from upstream, they are simply dropped, so there's no much overhead.
        //   So possibly this can also affect other running jobs less.
        // - When the upstream Source's becomes less busy, SourceBackfill can begin to catch up.
        let mut backfill_stream = select_with_strategy(
            input.by_ref().map(Either::Left),
            source_chunk_reader.map(Either::Right),
            select_strategy,
        );

        type PausedReader = Option<impl Stream>;
        let mut paused_reader: PausedReader = None;

        macro_rules! pause_reader {
            () => {
                if !pause_control.reader_paused {
                    let (left, right) = backfill_stream.into_inner();
                    backfill_stream = select_with_strategy(
                        left,
                        futures::stream::pending().boxed().map(Either::Right),
                        select_strategy,
                    );
                    // XXX: do we have to store the original reader? Can we simply rebuild the reader later?
                    paused_reader = Some(right);
                    pause_control.reader_paused = true;
                }
            };
        }

        macro_rules! resume_reader {
            () => {
                if pause_control.reader_paused {
                    backfill_stream = select_with_strategy(
                        input.by_ref().map(Either::Left),
                        paused_reader
                            .take()
                            .expect("should have paused reader to resume"),
                        select_strategy,
                    );
                    pause_control.reader_paused = false;
                }
            };
        }

        if pause_control.is_paused() {
            pause_reader!();
        }

        let state_store = self
            .backfill_state_store
            .state_store()
            .state_store()
            .clone();
        let table_id = self.backfill_state_store.state_store().table_id();
        let mut state_table_initialized = false;
        {
            let source_backfill_row_count = self
                .metrics
                .source_backfill_row_count
                .with_guarded_label_values(&[
                    &self.actor_ctx.id.to_string(),
                    &self.source_id.to_string(),
                    &self.source_name,
                    &self.actor_ctx.fragment_id.to_string(),
                ]);

            // We allow data to flow for `WAIT_BARRIER_MULTIPLE_TIMES` * `expected_barrier_latency_ms`
            // milliseconds, considering some other latencies like network and cost in Meta.
            let mut max_wait_barrier_time_ms = self.system_params.load().barrier_interval_ms()
                as u128
                * WAIT_BARRIER_MULTIPLE_TIMES;
            let mut last_barrier_time = Instant::now();

            // The main logic of the loop is in handle_upstream_row and handle_backfill_row.
            'backfill_loop: while let Some(either) = backfill_stream.next().await {
                match either {
                    // Upstream
                    Either::Left(msg) => {
                        let Ok(msg) = msg else {
                            let e = msg.unwrap_err();
                            tracing::error!(
                                error = ?e.as_report(),
                                source_id = %self.source_id,
                                "stream source reader error",
                            );
                            GLOBAL_ERROR_METRICS.user_source_error.report([
                                "SourceReaderError".to_owned(),
                                self.source_id.to_string(),
                                self.source_name.clone(),
                                self.actor_ctx.fragment_id.to_string(),
                            ]);

                            let (reader, _backfill_info) = self
                                .build_stream_source_reader(
                                    &source_desc,
                                    backfill_stage.get_latest_unfinished_splits()?,
                                )
                                .await?;

                            backfill_stream = select_with_strategy(
                                input.by_ref().map(Either::Left),
                                reader.map(Either::Right),
                                select_strategy,
                            );
                            continue;
                        };
                        match msg {
                            Message::Barrier(barrier) => {
                                last_barrier_time = Instant::now();

                                if pause_control.self_resume() {
                                    resume_reader!();
                                }

                                let mut maybe_mutation = None;
                                if let Some(ref mutation) = barrier.mutation.as_deref() {
                                    match mutation {
                                        Mutation::Pause => {
                                            // pause_reader should not be invoked consecutively more than once.
                                            pause_control.command_pause();
                                            pause_reader!();
                                        }
                                        Mutation::Resume => {
                                            // pause_reader.take should not be invoked consecutively more than once.
                                            if pause_control.command_resume() {
                                                resume_reader!();
                                            }
                                        }
                                        Mutation::StartFragmentBackfill { fragment_ids } => {
                                            if fragment_ids.contains(&self.actor_ctx.fragment_id)
                                                && pause_control.backfill_resume()
                                            {
                                                resume_reader!();
                                            }
                                        }
                                        Mutation::SourceChangeSplit(actor_splits) => {
                                            tracing::info!(
                                                actor_splits = ?actor_splits,
                                                "source change split received"
                                            );
                                            maybe_mutation = actor_splits
                                                .get(&self.actor_ctx.id)
                                                .cloned()
                                                .map(|target_splits| {
                                                    ApplyMutationAfterBarrier::SourceChangeSplit {
                                                        target_splits,
                                                        should_trim_state: true,
                                                    }
                                                });
                                        }
                                        Mutation::Update(UpdateMutation {
                                            actor_splits, ..
                                        }) => {
                                            maybe_mutation = actor_splits
                                                .get(&self.actor_ctx.id)
                                                .cloned()
                                                .map(|target_splits| {
                                                    ApplyMutationAfterBarrier::SourceChangeSplit {
                                                        target_splits,
                                                        should_trim_state: false,
                                                    }
                                                });
                                        }
                                        Mutation::ConnectorPropsChange(props_change) => {
                                            if let Some(props_plaintext) =
                                                props_change.get(&self.source_id.as_raw_id())
                                            {
                                                source_desc
                                                    .update_reader(props_plaintext.clone())?;

                                                maybe_mutation = Some(
                                                    ApplyMutationAfterBarrier::ConnectorPropsChange,
                                                );
                                            }
                                        }
                                        Mutation::Throttle(fragment_to_apply) => {
                                            if let Some(entry) =
                                                fragment_to_apply.get(&self.actor_ctx.fragment_id)
                                                && entry.throttle_type() == ThrottleType::Backfill
                                                && entry.rate_limit != self.rate_limit_rps
                                            {
                                                tracing::info!(
                                                    "updating rate limit from {:?} to {:?}",
                                                    self.rate_limit_rps,
                                                    entry.rate_limit
                                                );
                                                self.rate_limit_rps = entry.rate_limit;
                                                // rebuild reader
                                                let (reader, _backfill_info) = self
                                                    .build_stream_source_reader(
                                                        &source_desc,
                                                        backfill_stage
                                                            .get_latest_unfinished_splits()?,
                                                    )
                                                    .await?;

                                                backfill_stream = select_with_strategy(
                                                    input.by_ref().map(Either::Left),
                                                    reader.map(Either::Right),
                                                    select_strategy,
                                                );
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                async fn rebuild_reader_on_split_changed(
                                    this: &SourceBackfillExecutorInner<impl StateStore>,
                                    backfill_stage: &BackfillStage,
                                    source_desc: &SourceDesc,
                                ) -> StreamExecutorResult<BoxSourceReaderEventStream>
                                {
                                    // rebuild backfill_stream
                                    // Note: we don't put this part in a method, due to some complex lifetime issues.

                                    let latest_unfinished_splits =
                                        backfill_stage.get_latest_unfinished_splits()?;
                                    tracing::info!(
                                        "actor {:?} apply source split change to {:?}",
                                        this.actor_ctx.id,
                                        latest_unfinished_splits
                                    );

                                    // Replace the source reader with a new one of the new state.
                                    let (reader, _backfill_info) = this
                                        .build_stream_source_reader(
                                            source_desc,
                                            latest_unfinished_splits,
                                        )
                                        .await?;

                                    Ok(reader)
                                }

                                self.backfill_state_store
                                    .set_states(backfill_stage.states.clone())
                                    .await?;
                                self.backfill_state_store.commit(barrier.epoch).await?;

                                if self.should_report_finished(&backfill_stage.states) {
                                    // drop the backfill kafka consumers
                                    backfill_stream = select_with_strategy(
                                        input.by_ref().map(Either::Left),
                                        futures::stream::pending().boxed().map(Either::Right),
                                        select_strategy,
                                    );

                                    self.progress.finish(
                                        barrier.epoch,
                                        backfill_stage.total_backfilled_rows(),
                                    );

                                    let barrier_epoch = barrier.epoch;
                                    let is_checkpoint = barrier.is_checkpoint();
                                    // yield barrier after reporting progress
                                    yield Message::Barrier(barrier);

                                    if let Some(to_apply_mutation) = maybe_mutation {
                                        self.apply_split_change_after_yield_barrier(
                                            barrier_epoch,
                                            &mut backfill_stage,
                                            to_apply_mutation,
                                        )
                                        .await?;
                                    }

                                    if !state_table_initialized {
                                        if is_checkpoint {
                                            // This is for self.backfill_finished() to be safe: wait until this actor can read all actors' written data.
                                            // We wait for 2nd epoch
                                            let epoch = barrier_epoch.prev;
                                            tracing::info!("waiting for epoch: {}", epoch);
                                            state_store
                                                .try_wait_epoch(
                                                    HummockReadEpoch::Committed(epoch),
                                                    TryWaitEpochOptions { table_id },
                                                )
                                                .await?;
                                            tracing::info!("finished waiting for epoch: {}", epoch);
                                            state_table_initialized = true;
                                        }
                                    } else {
                                        // After we reported finished, we still don't exit the loop.
                                        // Because we need to handle split migration.
                                        assert!(
                                            state_table_initialized,
                                            "state table should be initialized before checking backfill finished"
                                        );
                                        if self.backfill_finished(&backfill_stage.states).await? {
                                            tracing::info!("source backfill finished");
                                            break 'backfill_loop;
                                        }
                                    }
                                } else {
                                    self.progress.update_for_source_backfill(
                                        barrier.epoch,
                                        backfill_stage.total_backfilled_rows(),
                                    );

                                    let barrier_epoch = barrier.epoch;
                                    // yield barrier after reporting progress
                                    yield Message::Barrier(barrier);

                                    if let Some(to_apply_mutation) = maybe_mutation
                                        && self
                                            .apply_split_change_after_yield_barrier(
                                                barrier_epoch,
                                                &mut backfill_stage,
                                                to_apply_mutation,
                                            )
                                            .await?
                                    {
                                        let reader = rebuild_reader_on_split_changed(
                                            &self,
                                            &backfill_stage,
                                            &source_desc,
                                        )
                                        .await?;

                                        backfill_stream = select_with_strategy(
                                            input.by_ref().map(Either::Left),
                                            reader.map(Either::Right),
                                            select_strategy,
                                        );
                                    }
                                }
                            }
                            Message::Chunk(chunk) => {
                                // We need to iterate over all rows because there might be multiple splits in a chunk.
                                // Note: We assume offset from the source is monotonically increasing for the algorithm to work correctly.
                                let mut new_vis = BitmapBuilder::zeroed(chunk.visibility().len());

                                for (i, (_, row)) in chunk.rows().enumerate() {
                                    let split = row.datum_at(split_idx).unwrap().into_utf8();
                                    let offset = row.datum_at(offset_idx).unwrap().into_utf8();
                                    let vis = backfill_stage.handle_upstream_row(split, offset);
                                    new_vis.set(i, vis);
                                }
                                // emit chunk if vis is not empty. i.e., some splits finished backfilling.
                                let new_vis = new_vis.finish();
                                if new_vis.count_ones() != 0 {
                                    let new_chunk = chunk.clone_with_vis(new_vis);
                                    yield Message::Chunk(new_chunk);
                                }
                            }
                            Message::Watermark(_) => {
                                // Ignore watermark during backfill.
                            }
                        }
                    }
                    // backfill
                    Either::Right(msg) => {
                        let chunk = match msg? {
                            SourceReaderEvent::DataChunk(chunk) => chunk,
                            SourceReaderEvent::SplitProgress(split_progress) => {
                                // `SplitProgress` is consumed only from the backfill reader side.
                                // The upstream side of source backfill still advances based on
                                // normal chunks with split/offset columns.
                                for (split_id, offset) in split_progress {
                                    let previous_state =
                                        backfill_stage.states.get(&split_id).cloned();
                                    backfill_stage
                                        .handle_backfill_progress(split_id.as_ref(), &offset);
                                    let updated_state =
                                        backfill_stage.states.get(&split_id).cloned();
                                    if previous_state != updated_state {
                                        tracing::info!(
                                            actor_id = %self.actor_ctx.id,
                                            fragment_id = %self.actor_ctx.fragment_id,
                                            source_id = %self.source_id,
                                            source_name = self.source_name,
                                            split_id = %split_id,
                                            applied_offset = %offset,
                                            previous_state = ?previous_state,
                                            updated_state = ?updated_state,
                                            "source backfill executor applied split progress offset"
                                        );
                                    }
                                }
                                continue;
                            }
                        };

                        if last_barrier_time.elapsed().as_millis() > max_wait_barrier_time_ms {
                            // Pause to let barrier catch up via backpressure of snapshot stream.
                            pause_control.self_pause();
                            pause_reader!();

                            // Exceeds the max wait barrier time, the source will be paused.
                            // Currently we can guarantee the
                            // source is not paused since it received stream
                            // chunks.
                            tracing::warn!(
                                "source {} paused, wait barrier for {:?}",
                                self.info.identity,
                                last_barrier_time.elapsed()
                            );

                            // Only update `max_wait_barrier_time_ms` to capture
                            // `barrier_interval_ms`
                            // changes here to avoid frequently accessing the shared
                            // `system_params`.
                            max_wait_barrier_time_ms =
                                self.system_params.load().barrier_interval_ms() as u128
                                    * WAIT_BARRIER_MULTIPLE_TIMES;
                        }
                        let mut new_vis = BitmapBuilder::zeroed(chunk.visibility().len());

                        for (i, (_, row)) in chunk.rows().enumerate() {
                            let split_id = row.datum_at(split_idx).unwrap().into_utf8();
                            let offset = row.datum_at(offset_idx).unwrap().into_utf8();
                            let vis = backfill_stage.handle_backfill_row(split_id, offset);
                            new_vis.set(i, vis);
                        }

                        let new_vis = new_vis.finish();
                        let card = new_vis.count_ones();
                        if card != 0 {
                            let new_chunk = chunk.clone_with_vis(new_vis);
                            yield Message::Chunk(new_chunk);
                            source_backfill_row_count.inc_by(card as u64);
                        }
                    }
                }
            }
        }

        std::mem::drop(backfill_stream);
        let mut states = backfill_stage.states;
        // Make sure `Finished` state is persisted.
        self.backfill_state_store.set_states(states.clone()).await?;

        // All splits finished backfilling. Now we only forward the source data.
        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Barrier(barrier) => {
                    let mut split_changed = None;
                    if let Some(ref mutation) = barrier.mutation.as_deref() {
                        match mutation {
                            Mutation::Pause | Mutation::Resume => {
                                // We don't need to do anything. Handled by upstream.
                            }
                            Mutation::SourceChangeSplit(actor_splits) => {
                                tracing::info!(
                                    actor_splits = ?actor_splits,
                                    "source change split received"
                                );
                                split_changed = actor_splits
                                    .get(&self.actor_ctx.id)
                                    .cloned()
                                    .map(|target_splits| (target_splits, &mut states, true));
                            }
                            Mutation::Update(UpdateMutation { actor_splits, .. }) => {
                                split_changed = actor_splits
                                    .get(&self.actor_ctx.id)
                                    .cloned()
                                    .map(|target_splits| (target_splits, &mut states, false));
                            }
                            _ => {}
                        }
                    }
                    self.backfill_state_store.commit(barrier.epoch).await?;
                    let barrier_epoch = barrier.epoch;
                    yield Message::Barrier(barrier);

                    if let Some((target_splits, state, should_trim_state)) = split_changed {
                        self.apply_split_change_forward_stage_after_yield_barrier(
                            barrier_epoch,
                            target_splits,
                            state,
                            should_trim_state,
                        )
                        .await?;
                    }
                }
                Message::Chunk(chunk) => {
                    yield Message::Chunk(chunk);
                }
                Message::Watermark(watermark) => {
                    yield Message::Watermark(watermark);
                }
            }
        }
    }

    /// When we should call `progress.finish()` to let blocking DDL return.
    /// We report as soon as `SourceCachingUp`. Otherwise the DDL might be blocked forever until upstream messages come.
    ///
    /// Note: split migration (online scaling) is related with progress tracking.
    /// - For foreground DDL, scaling is not allowed before progress is finished.
    /// - For background DDL, scaling is skipped when progress is not finished, and can be triggered by recreating actors during recovery.
    ///
    /// See <https://github.com/risingwavelabs/risingwave/issues/18300> for more details.
    fn should_report_finished(&self, states: &BackfillStates) -> bool {
        states.values().all(|state| {
            matches!(
                state.state,
                BackfillState::Finished | BackfillState::SourceCachingUp(_)
            )
        })
    }

    /// All splits entered `Finished` state.
    ///
    /// We check all splits for the source, including other actors' splits here, before going to the forward stage.
    /// Otherwise if we `break` early, but after rescheduling, an unfinished split is migrated to
    /// this actor, we still need to backfill it.
    ///
    /// Note: at the beginning, the actor will only read the state written by itself.
    /// It needs to _wait until it can read all actors' written data_.
    /// i.e., wait for the second checkpoint has been available.
    ///
    /// See <https://github.com/risingwavelabs/risingwave/issues/18300> for more details.
    async fn backfill_finished(&self, states: &BackfillStates) -> StreamExecutorResult<bool> {
        Ok(states
            .values()
            .all(|state| matches!(state.state, BackfillState::Finished))
            && self
                .backfill_state_store
                .scan_may_stale()
                .await?
                .into_iter()
                .all(|state| matches!(state.state, BackfillState::Finished)))
    }

    /// For newly added splits, we do not need to backfill and can directly forward from upstream.
    async fn apply_split_change_after_yield_barrier(
        &mut self,
        barrier_epoch: EpochPair,
        stage: &mut BackfillStage,
        to_apply_mutation: ApplyMutationAfterBarrier,
    ) -> StreamExecutorResult<bool> {
        match to_apply_mutation {
            ApplyMutationAfterBarrier::SourceChangeSplit {
                target_splits,
                should_trim_state,
            } => {
                self.source_split_change_count.inc();
                {
                    if self
                        .update_state_if_changed(
                            barrier_epoch,
                            target_splits,
                            stage,
                            should_trim_state,
                        )
                        .await?
                    {
                        // Note: we don't rebuild backfill_stream here, due to some complex lifetime issues.
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                }
            }
            ApplyMutationAfterBarrier::ConnectorPropsChange => Ok(true),
        }
    }

    /// Returns `true` if split changed. Otherwise `false`.
    async fn update_state_if_changed(
        &mut self,
        barrier_epoch: EpochPair,
        target_splits: Vec<SplitImpl>,
        stage: &mut BackfillStage,
        should_trim_state: bool,
    ) -> StreamExecutorResult<bool> {
        let mut target_state: BackfillStates = HashMap::with_capacity(target_splits.len());

        let mut split_changed = false;
        // Take out old states (immutable, only used to build target_state and check for added/dropped splits).
        // Will be set to target_state in the end.
        let old_states = std::mem::take(&mut stage.states);
        let committed_reader = self
            .backfill_state_store
            .new_committed_reader(barrier_epoch)
            .await?;
        // Iterate over the target (assigned) splits
        // - check if any new splits are added
        // - build target_state
        for split in &target_splits {
            let split_id = split.id();
            if let Some(s) = old_states.get(&split_id) {
                target_state.insert(split_id, s.clone());
            } else {
                split_changed = true;

                let backfill_state = committed_reader
                    .try_recover_from_state_store(&split_id)
                    .await?;
                match backfill_state {
                    None => {
                        // Newly added split. We don't need to backfill.
                        // Note that this branch is different from the initial barrier (BackfillStateInner::Backfilling(None) there).
                        target_state.insert(
                            split_id,
                            BackfillStateWithProgress {
                                state: BackfillState::Finished,
                                num_consumed_rows: 0,
                                target_offset: None,
                            },
                        );
                    }
                    Some(backfill_state) => {
                        // Migrated split. Backfill if unfinished.
                        target_state.insert(split_id, backfill_state);
                    }
                }
            }
        }

        // Checks dropped splits
        for existing_split_id in old_states.keys() {
            if !target_state.contains_key(existing_split_id) {
                tracing::info!("split dropping detected: {}", existing_split_id);
                split_changed = true;
            }
        }

        if split_changed {
            let dropped_splits = stage
                .states
                .extract_if(|split_id, _| !target_state.contains_key(split_id))
                .map(|(split_id, _)| split_id);

            if should_trim_state {
                // trim dropped splits' state
                self.backfill_state_store.trim_state(dropped_splits).await?;
            }
            tracing::info!(old_state=?old_states, new_state=?target_state, "finish split change");
        } else {
            debug_assert_eq!(old_states, target_state);
        }
        stage.states = target_state;
        stage.splits = target_splits;
        stage.debug_assert_consistent();
        Ok(split_changed)
    }

    /// For split change during forward stage, all newly added splits should be already finished.
    // We just need to update the state store if necessary.
    async fn apply_split_change_forward_stage_after_yield_barrier(
        &mut self,
        barrier_epoch: EpochPair,
        target_splits: Vec<SplitImpl>,
        states: &mut BackfillStates,
        should_trim_state: bool,
    ) -> StreamExecutorResult<()> {
        self.source_split_change_count.inc();
        {
            self.update_state_if_changed_forward_stage(
                barrier_epoch,
                target_splits,
                states,
                should_trim_state,
            )
            .await?;
        }

        Ok(())
    }

    async fn update_state_if_changed_forward_stage(
        &mut self,
        barrier_epoch: EpochPair,
        target_splits: Vec<SplitImpl>,
        states: &mut BackfillStates,
        should_trim_state: bool,
    ) -> StreamExecutorResult<()> {
        let target_splits: HashSet<SplitId> =
            target_splits.into_iter().map(|split| split.id()).collect();

        let mut split_changed = false;
        let mut newly_added_splits = vec![];

        let committed_reader = self
            .backfill_state_store
            .new_committed_reader(barrier_epoch)
            .await?;

        // Checks added splits
        for split_id in &target_splits {
            if !states.contains_key(split_id) {
                split_changed = true;

                let backfill_state = committed_reader
                    .try_recover_from_state_store(split_id)
                    .await?;
                match &backfill_state {
                    None => {
                        // Newly added split. We don't need to backfill!
                        newly_added_splits.push(split_id.clone());
                    }
                    Some(backfill_state) => {
                        // Migrated split. It should also be finished since we are in forwarding stage.
                        match backfill_state.state {
                            BackfillState::Finished => {}
                            _ => {
                                return Err(anyhow::anyhow!(
                                    "Unexpected backfill state: {:?}",
                                    backfill_state
                                )
                                .into());
                            }
                        }
                    }
                }
                states.insert(
                    split_id.clone(),
                    backfill_state.unwrap_or(BackfillStateWithProgress {
                        state: BackfillState::Finished,
                        num_consumed_rows: 0,
                        target_offset: None,
                    }),
                );
            }
        }

        // Checks dropped splits
        for existing_split_id in states.keys() {
            if !target_splits.contains(existing_split_id) {
                tracing::info!("split dropping detected: {}", existing_split_id);
                split_changed = true;
            }
        }

        if split_changed {
            tracing::info!(
                target_splits = ?target_splits,
                "apply split change"
            );

            let dropped_splits = states.extract_if(|split_id, _| !target_splits.contains(split_id));

            if should_trim_state {
                // trim dropped splits' state
                self.backfill_state_store
                    .trim_state(dropped_splits.map(|(k, _v)| k))
                    .await?;
            }

            // For migrated splits, and existing splits, we do not need to update
            // state store, but only for newly added splits.
            self.backfill_state_store
                .set_states(
                    newly_added_splits
                        .into_iter()
                        .map(|split_id| {
                            (
                                split_id,
                                BackfillStateWithProgress {
                                    state: BackfillState::Finished,
                                    num_consumed_rows: 0,
                                    target_offset: None,
                                },
                            )
                        })
                        .collect(),
                )
                .await?;
        }

        Ok(())
    }
}

fn compare_kafka_offset(a: &str, b: &str) -> Ordering {
    let a = a.parse::<i64>().unwrap();
    let b = b.parse::<i64>().unwrap();
    a.cmp(&b)
}

impl<S: StateStore> Execute for SourceBackfillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.inner.execute(self.input).boxed()
    }
}

impl<S: StateStore> Debug for SourceBackfillExecutorInner<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceBackfillExecutor")
            .field("source_id", &self.source_id)
            .field("column_ids", &self.column_ids)
            .field("stream_key", &self.info.stream_key)
            .finish()
    }
}

struct PauseControl {
    // Paused due to backfill order control
    backfill_paused: bool,
    // Paused due to self-pause, e.g. let barrier catch up
    self_paused: bool,
    // Paused due to Pause command from meta, pause_on_next_bootstrap
    command_paused: bool,
    // reader paused
    reader_paused: bool,
}

impl PauseControl {
    fn new() -> Self {
        Self {
            backfill_paused: false,
            self_paused: false,
            command_paused: false,
            reader_paused: false,
        }
    }

    fn is_paused(&self) -> bool {
        self.backfill_paused || self.command_paused || self.self_paused
    }

    /// returns whether we need to pause the reader.
    fn backfill_pause(&mut self) {
        if self.backfill_paused {
            tracing::warn!("backfill_pause invoked twice");
        }
        self.backfill_paused = true;
    }

    /// returns whether we need to resume the reader.
    /// same precedence as command.
    fn backfill_resume(&mut self) -> bool {
        if !self.backfill_paused {
            tracing::warn!("backfill_resume invoked twice");
        }
        !self.command_paused
    }

    /// returns whether we need to pause the reader.
    fn self_pause(&mut self) {
        assert!(
            !self.backfill_paused,
            "backfill stream should not be read when backfill_pause is set"
        );
        assert!(
            !self.command_paused,
            "backfill stream should not be read when command_pause is set"
        );
        if self.self_paused {
            tracing::warn!("self_pause invoked twice");
        }
        self.self_paused = true;
    }

    /// returns whether we need to resume the reader.
    /// `self_resume` has the lowest precedence,
    /// it can only resume if we are not paused due to `backfill_paused` or `command_paused`.
    fn self_resume(&mut self) -> bool {
        self.self_paused = false;
        !(self.backfill_paused || self.command_paused)
    }

    /// returns whether we need to pause the reader.
    fn command_pause(&mut self) {
        if self.command_paused {
            tracing::warn!("command_pause invoked twice");
        }
        self.command_paused = true;
    }

    /// returns whether we need to resume the reader.
    /// same precedence as backfill.
    fn command_resume(&mut self) -> bool {
        if !self.command_paused {
            tracing::warn!("command_resume invoked twice");
        }
        self.command_paused = false;
        !self.backfill_paused
    }
}
