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

use std::collections::BTreeMap;

use either::Either;
use futures::stream;
use futures::stream::select_with_strategy;
use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::catalog::{ColumnDesc, Field};
use risingwave_common::row::{RowDeserializer, RowExt};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::{OrderType, cmp_datum};
use risingwave_connector::source::cdc::CdcScanOptions;
use risingwave_connector::source::cdc::external::{CdcOffset, ExternalCdcTableType};
use risingwave_connector::source::{CdcTableSnapshotSplit, CdcTableSnapshotSplitRaw};
use rw_futures_util::pausable;
use thiserror_ext::AsReport;

use crate::executor::backfill::cdc::cdc_backfill::{
    build_reader_and_poll_upstream, create_table_reader_with_retry,
    get_cdc_json_parse_handling_from_properties, transform_upstream,
};
use crate::executor::backfill::cdc::state_v2::ParallelizedCdcBackfillState;
use crate::executor::backfill::cdc::upstream_table::external::ExternalStorageTable;
use crate::executor::backfill::cdc::upstream_table::snapshot::{
    SplitSnapshotReadArgs, UpstreamTableRead, UpstreamTableReader,
};
use crate::executor::backfill::utils::{
    cmp_pk_unsigned_aware, get_cdc_chunk_last_offset, get_new_pos, mapping_chunk, mapping_message,
};
use crate::executor::prelude::*;
use crate::task::cdc_progress::CdcProgressReporter;
pub struct ParallelizedCdcBackfillExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// The external table to be backfilled
    external_table: ExternalStorageTable,

    /// Upstream changelog stream which may contain metadata columns, e.g. `_rw_offset`
    upstream: Executor,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    output_indices: Vec<usize>,

    /// The schema of output chunk, including additional columns if any
    output_columns: Vec<ColumnDesc>,

    /// Rate limit in rows/s.
    rate_limit_rps: Option<u32>,

    options: CdcScanOptions,

    state_table: StateTable<S>,

    properties: BTreeMap<String, String>,

    progress: Option<CdcProgressReporter>,
}

enum SnapshotAttemptState {
    Reading,
    Failed,
    Finished(CdcOffset),
}

impl<S: StateStore> ParallelizedCdcBackfillExecutor<S> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        external_table: ExternalStorageTable,
        upstream: Executor,
        output_indices: Vec<usize>,
        output_columns: Vec<ColumnDesc>,
        _metrics: Arc<StreamingMetrics>,
        state_table: StateTable<S>,
        rate_limit_rps: Option<u32>,
        options: CdcScanOptions,
        properties: BTreeMap<String, String>,
        progress: Option<CdcProgressReporter>,
    ) -> Self {
        Self {
            actor_ctx,
            external_table,
            upstream,
            output_indices,
            output_columns,
            rate_limit_rps,
            options,
            state_table,
            properties,
            progress,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        assert!(!self.options.disable_backfill);
        // The indices to primary key columns
        let pk_indices = self.external_table.pk_indices().to_vec();
        let pk_order = self.external_table.pk_order_types().to_vec();
        let pk_in_output_indices = pk_indices
            .iter()
            .map(|pk_idx| {
                self.output_indices
                    .iter()
                    .position(|output_idx| output_idx == pk_idx)
                    .expect("primary key column must be present in CDC backfill output")
            })
            .collect_vec();
        let pk_names = self.external_table.pk_names();
        let table_id = self.external_table.table_id();
        let upstream_table_name = self.external_table.qualified_table_name();
        let schema_table_name = self.external_table.schema_table_name().clone();
        let external_database_name = self.external_table.database_name().to_owned();
        let additional_columns = self
            .output_columns
            .iter()
            .filter(|col| col.additional_column.column_type.is_some())
            .cloned()
            .collect_vec();
        assert!(
            (self.options.backfill_split_pk_column_index as usize) < pk_indices.len(),
            "split pk column index {} out of bound",
            self.options.backfill_split_pk_column_index
        );
        let snapshot_split_column_index =
            pk_indices[self.options.backfill_split_pk_column_index as usize];
        let snapshot_split_column_in_output_index = self
            .output_indices
            .iter()
            .position(|&idx| idx == snapshot_split_column_index)
            .expect("snapshot split column must be present in CDC backfill output");
        let cdc_table_snapshot_split_column =
            vec![self.external_table.schema().fields[snapshot_split_column_index].clone()];

        let mut upstream = self.upstream.execute();
        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        // Make sure to use mapping_message after transform_upstream.

        // If user sets debezium.time.precision.mode to "connect", it means the user can guarantee
        // that the upstream data precision is MilliSecond. In this case, we don't use GuessNumberUnit
        // mode to guess precision, but use Milli mode directly, which can handle extreme timestamps.
        let (timestamp_handling, timestamptz_handling, time_handling, bigint_unsigned_handling) =
            get_cdc_json_parse_handling_from_properties(&self.properties);
        // Only postgres-cdc connector may trigger TOAST.
        let handle_toast_columns: bool =
            self.external_table.table_type() == &ExternalCdcTableType::Postgres;
        let mut upstream = transform_upstream(
            upstream,
            self.output_columns.clone(),
            timestamp_handling,
            timestamptz_handling,
            time_handling,
            bigint_unsigned_handling,
            handle_toast_columns,
        )
        .boxed();
        let mut next_reset_barrier = Some(first_barrier);
        let mut is_reset = false;
        let mut state_impl = ParallelizedCdcBackfillState::new(self.state_table, pk_indices.len());
        // The buffered chunks have already been mapped.
        let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];

        // Need reset on CDC table snapshot splits reschedule.
        'with_cdc_table_snapshot_splits: loop {
            assert!(upstream_chunk_buffer.is_empty());
            let reset_barrier = next_reset_barrier.take().unwrap();
            let all_snapshot_splits = match reset_barrier.mutation.as_deref() {
                Some(Mutation::Add(add)) => &add.actor_cdc_table_snapshot_splits.splits,

                Some(Mutation::Update(update)) => &update.actor_cdc_table_snapshot_splits.splits,
                _ => {
                    return Err(anyhow::anyhow!("ParallelizedCdcBackfillExecutor expects either Mutation::Add or Mutation::Update to initialize CDC table snapshot splits.").into());
                }
            };
            let mut actor_snapshot_splits = vec![];
            let mut generation = None;
            // TODO(zw): optimization: remove consumed splits to reduce barrier size for downstream.
            if let Some((splits, snapshot_generation)) = all_snapshot_splits.get(&self.actor_ctx.id)
            {
                actor_snapshot_splits = splits
                    .iter()
                    .map(|s: &CdcTableSnapshotSplitRaw| {
                        let de = RowDeserializer::new(
                            cdc_table_snapshot_split_column
                                .iter()
                                .map(Field::data_type)
                                .collect_vec(),
                        );
                        let left_bound_inclusive =
                            de.deserialize(s.left_bound_inclusive.as_ref()).unwrap();
                        let right_bound_exclusive =
                            de.deserialize(s.right_bound_exclusive.as_ref()).unwrap();
                        CdcTableSnapshotSplit {
                            split_id: s.split_id,
                            left_bound_inclusive,
                            right_bound_exclusive,
                        }
                    })
                    .collect();
                generation = Some(*snapshot_generation);
            }
            tracing::debug!(?actor_snapshot_splits, ?generation, "actor splits");
            assert_consecutive_splits(&actor_snapshot_splits);

            let mut is_snapshot_paused = reset_barrier.is_pause_on_startup();
            let barrier_epoch = reset_barrier.epoch;
            yield Message::Barrier(reset_barrier);
            if !is_reset {
                state_impl.init_epoch(barrier_epoch).await?;
                is_reset = true;
                tracing::info!(%table_id, "Initialize executor.");
            } else {
                tracing::info!(%table_id, "Reset executor.");
            }

            let mut current_actor_bounds = None;
            let mut actor_cdc_offset_high: Option<CdcOffset> = None;
            let mut actor_cdc_offset_low: Option<CdcOffset> = None;
            // Find next split that need backfill.
            let mut next_split_idx = actor_snapshot_splits.len();
            'restore_split_state: for (idx, split) in actor_snapshot_splits.iter().enumerate() {
                let state = state_impl.restore_state(split.split_id).await?;
                if !state.is_finished {
                    next_split_idx = idx;
                    break 'restore_split_state;
                }
                extends_current_actor_bound(&mut current_actor_bounds, split);
                if let Some(ref cdc_offset) = state.cdc_offset_low {
                    if let Some(ref cur) = actor_cdc_offset_low {
                        if *cur > *cdc_offset {
                            actor_cdc_offset_low = state.cdc_offset_low.clone();
                        }
                    } else {
                        actor_cdc_offset_low = state.cdc_offset_low.clone();
                    }
                }
                if let Some(ref cdc_offset) = state.cdc_offset_high {
                    if let Some(ref cur) = actor_cdc_offset_high {
                        if *cur < *cdc_offset {
                            actor_cdc_offset_high = state.cdc_offset_high.clone();
                        }
                    } else {
                        actor_cdc_offset_high = state.cdc_offset_high.clone();
                    }
                }
            }
            for split in actor_snapshot_splits.iter().skip(next_split_idx) {
                // Initialize state so that overall progress can be measured.
                state_impl.init_state_if_absent(split.split_id).await?;
            }
            let mut should_report_actor_backfill_progress = if next_split_idx > 0 {
                Some((
                    actor_snapshot_splits[0].split_id,
                    actor_snapshot_splits[next_split_idx - 1].split_id,
                ))
            } else {
                None
            };

            let offset_parse_func = self.external_table.table_type().get_cdc_offset_parser()?;
            let mut pk_needs_unsigned_i64_compare = vec![false; pk_indices.len()];

            // A reader is only needed while at least one assigned snapshot split is unfinished.
            // Once all splits are complete, the executor only forwards the table-filtered CDC
            // stream and must not depend on the upstream snapshot table still existing.
            if next_split_idx < actor_snapshot_splits.len() {
                let external_table = self.external_table.clone();
                let actor_id = self.actor_ctx.id;
                let fragment_id = self.actor_ctx.fragment_id;
                let mut future = Box::pin(create_table_reader_with_retry(
                    external_table,
                    actor_id,
                    fragment_id,
                ));

                let table_reader = loop {
                    match build_reader_and_poll_upstream(&mut upstream, &mut future).await? {
                        Either::Left(msg) => {
                            if let Some(msg) = mapping_message(msg, &self.output_indices) {
                                match msg {
                                    Message::Barrier(barrier) => {
                                        state_impl.commit_state(barrier.epoch).await?;

                                        if is_reset_barrier(&barrier, self.actor_ctx.id) {
                                            next_reset_barrier = Some(barrier);
                                            continue 'with_cdc_table_snapshot_splits;
                                        } else {
                                            yield Message::Barrier(barrier)
                                        }
                                    }
                                    Message::Chunk(_) => {
                                        // ignore chunk if we need backfill, since we can read the data from the snapshot
                                    }
                                    Message::Watermark(_) => {
                                        // Ignore watermark, like the `CdcBackfillExecutor`.
                                    }
                                }
                            }
                        }
                        Either::Right(table_reader) => break table_reader,
                    }
                };
                pk_needs_unsigned_i64_compare =
                    table_reader.pk_column_unsigned_i64_compare_flags(&pk_names)?;

                tracing::info!(
                    %table_id,
                    upstream_table_name,
                    "table reader created successfully"
                );

                let mut upstream_table_reader =
                    UpstreamTableReader::new(self.external_table.clone(), table_reader);

                // Backfill snapshot splits sequentially.
                for split in actor_snapshot_splits.iter().skip(next_split_idx) {
                    tracing::info!(
                        %table_id,
                        upstream_table_name,
                        ?split,
                        is_snapshot_paused,
                        "start cdc backfill split"
                    );
                    let finished_split_bounds = current_actor_bounds.clone();
                    let current_split_bounds = Some((
                        split.left_bound_inclusive.clone(),
                        split.right_bound_exclusive.clone(),
                    ));
                    let restored_state = state_impl.restore_state(split.split_id).await?;
                    let mut current_pk_pos = restored_state.current_pk_pos;
                    let mut row_count = restored_state.row_count as u64;
                    let mut split_cdc_offset_low = restored_state.cdc_offset_low;

                    'backfill_loop: loop {
                        if split_cdc_offset_low.is_none() {
                            static CDC_CONN_SEMAPHORE: tokio::sync::Semaphore =
                                tokio::sync::Semaphore::const_new(10);
                            let _permit = CDC_CONN_SEMAPHORE.acquire().await.unwrap();
                            split_cdc_offset_low =
                                upstream_table_reader.current_cdc_offset().await?;
                        }
                        if let Some(ref cdc_offset) = split_cdc_offset_low {
                            if actor_cdc_offset_low
                                .as_ref()
                                .is_none_or(|cur| cur > cdc_offset)
                            {
                                actor_cdc_offset_low = split_cdc_offset_low.clone();
                            }
                        }

                        // why needed?
                        // Apply changes to the already snapshotted prefix before starting the new
                        // query. Changes after the cursor are reflected by that query instead.
                        let (emitted_chunks, _) = partition_current_split_buffer(
                            std::mem::take(&mut upstream_chunk_buffer),
                            current_pk_pos.as_ref(),
                            &pk_in_output_indices,
                            &pk_order,
                            &pk_needs_unsigned_i64_compare,
                        );

                        for chunk in emitted_chunks {
                            yield Message::Chunk(chunk);
                        }

                        let attempt_state = {
                            let left_upstream = upstream.by_ref().map(Either::Left);
                            let read_args = SplitSnapshotReadArgs::new(
                                current_pk_pos.clone(),
                                pk_names.clone(),
                                (!is_leftmost_bound(&split.left_bound_inclusive))
                                    .then(|| split.left_bound_inclusive.clone()),
                                (!is_rightmost_bound(&split.right_bound_exclusive))
                                    .then(|| split.right_bound_exclusive.clone()),
                                cdc_table_snapshot_split_column.clone(),
                                self.rate_limit_rps,
                                additional_columns.clone(),
                                schema_table_name.clone(),
                                external_database_name.clone(),
                            );
                            let right_snapshot = pin!(
                                upstream_table_reader
                                    .snapshot_read_table_split(read_args)
                                    .map(Either::Right)
                            );
                            let (right_snapshot, snapshot_valve) = pausable(right_snapshot);
                            if is_snapshot_paused {
                                snapshot_valve.pause();
                            }
                            let mut backfill_stream = select_with_strategy(
                                left_upstream,
                                right_snapshot,
                                |_: &mut ()| stream::PollNext::Left,
                            );
                            let mut attempt_state = SnapshotAttemptState::Reading;

                            #[for_await]
                            'backfill_stream: for either in &mut backfill_stream {
                                match either {
                                    Either::Left(upstream_message) => match upstream_message? {
                                        Message::Barrier(barrier) => {
                                            // emit chunks where the PK has advanced past before persisting PK state,
                                            // or else if we crash after the barrier, past events before PK will be lost
                                            let (emitted_chunks, retained_chunks) =
                                                partition_current_split_buffer(
                                                    std::mem::take(&mut upstream_chunk_buffer),
                                                    current_pk_pos.as_ref(),
                                                    &pk_in_output_indices,
                                                    &pk_order,
                                                    &pk_needs_unsigned_i64_compare,
                                                );

                                            upstream_chunk_buffer = retained_chunks;

                                            for chunk in emitted_chunks {
                                                yield Message::Chunk(chunk);
                                            }

                                            state_impl
                                                .mutate_state(
                                                    split.split_id,
                                                    current_pk_pos.clone(),
                                                    false,
                                                    row_count,
                                                    split_cdc_offset_low.clone(),
                                                    None,
                                                )
                                                .await?;
                                            state_impl.commit_state(barrier.epoch).await?;

                                            if let Some(mutation) = barrier.mutation.as_deref() {
                                                use crate::executor::Mutation;
                                                match mutation {
                                                    Mutation::Pause => {
                                                        is_snapshot_paused = true;
                                                        snapshot_valve.pause();
                                                    }
                                                    Mutation::Resume => {
                                                        is_snapshot_paused = false;
                                                        snapshot_valve.resume();
                                                    }
                                                    Mutation::Throttle(_) => {
                                                        if let Some(entry) = mutation
                                                            .backfill_throttle_config(
                                                                self.actor_ctx.fragment_id,
                                                            )
                                                        {
                                                            self.rate_limit_rps = entry.rate_limit;
                                                        }
                                                    }
                                                    mutation
                                                        if mutation.is_stop(self.actor_ctx.id) =>
                                                    {
                                                        tracing::info!(
                                                            %table_id,
                                                            upstream_table_name,
                                                            "CdcBackfill has been dropped due to config change"
                                                        );

                                                        yield Message::Barrier(barrier);

                                                        let () = futures::future::pending().await;
                                                        unreachable!();
                                                    }
                                                    _ => (),
                                                }
                                            }

                                            if is_reset_barrier(&barrier, self.actor_ctx.id) {
                                                upstream_chunk_buffer.clear();
                                                next_reset_barrier = Some(barrier);
                                                // restart to apply new split state
                                                continue 'with_cdc_table_snapshot_splits;
                                            }

                                            if let (Some(split_range), Some(progress)) = (
                                                should_report_actor_backfill_progress.take(),
                                                self.progress.as_ref(),
                                            ) {
                                                progress.update(
                                                    self.actor_ctx.fragment_id,
                                                    self.actor_ctx.id,
                                                    barrier.epoch,
                                                    generation.expect("should have set generation when having progress to report"),
                                                    split_range,
                                                );
                                            }

                                            yield Message::Barrier(barrier);

                                            if matches!(attempt_state, SnapshotAttemptState::Failed)
                                            {
                                                break 'backfill_stream;
                                            }
                                        }
                                        Message::Chunk(chunk) => {
                                            if !chunk.has_visible_rows() {
                                                continue 'backfill_stream;
                                            }

                                            // emit chunks belonging to past splits which are processed
                                            let chunk = mapping_chunk(chunk, &self.output_indices);
                                            let (finished_chunk, current_chunk) =
                                                split_finished_and_current_chunk(
                                                    chunk,
                                                    &finished_split_bounds,
                                                    &current_split_bounds,
                                                    snapshot_split_column_in_output_index,
                                                );

                                            if let Some(finished_chunk) = finished_chunk {
                                                yield Message::Chunk(finished_chunk);
                                            }

                                            if let Some(current_chunk) = current_chunk {
                                                upstream_chunk_buffer.push(current_chunk);
                                            }
                                        }
                                        Message::Watermark(_) => {
                                            // ignore watermark during backfill
                                        }
                                    },
                                    Either::Right(snapshot) => {
                                        // if snapshot already failed, continue polling upstream until barrier arrives to reconstruct reader
                                        if !matches!(attempt_state, SnapshotAttemptState::Reading) {
                                            continue 'backfill_stream;
                                        }

                                        match snapshot {
                                            Ok(None) => {
                                                tracing::info!(
                                                    %table_id,
                                                    split_id = split.split_id,
                                                    "snapshot read stream ends"
                                                );

                                                for chunk in upstream_chunk_buffer.drain(..) {
                                                    yield Message::Chunk(chunk);
                                                }

                                                static CDC_CONN_SEMAPHORE: tokio::sync::Semaphore =
                                                    tokio::sync::Semaphore::const_new(10);
                                                let _permit =
                                                    CDC_CONN_SEMAPHORE.acquire().await.unwrap();
                                                let high = upstream_table_reader
                                                .current_cdc_offset()
                                                .await?
                                                .expect(
                                                    "CDC offset must be available after snapshot completion",
                                                );
                                                attempt_state =
                                                    SnapshotAttemptState::Finished(high);

                                                break 'backfill_stream;
                                            }
                                            Ok(Some(chunk)) => {
                                                current_pk_pos =
                                                    Some(get_new_pos(&chunk, &pk_indices));
                                                row_count = row_count
                                                    .saturating_add(chunk.cardinality() as u64);

                                                yield Message::Chunk(mapping_chunk(
                                                    chunk,
                                                    &self.output_indices,
                                                ));
                                            }
                                            Err(error) => {
                                                attempt_state = SnapshotAttemptState::Failed;
                                                tracing::warn!(
                                                    error = %error.as_report(),
                                                    %table_id,
                                                    upstream_table_name,
                                                    "failed to read CDC snapshot; rebuilding reader after a barrier"
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            attempt_state
                        };

                        match attempt_state {
                            SnapshotAttemptState::Failed => {
                                upstream_table_reader.disconnect().await?;

                                let mut future = Box::pin(create_table_reader_with_retry(
                                    self.external_table.clone(),
                                    self.actor_ctx.id,
                                    self.actor_ctx.fragment_id,
                                ));
                                let table_reader = 'rebuild_reader: loop {
                                    match build_reader_and_poll_upstream(&mut upstream, &mut future)
                                        .await?
                                    {
                                        Either::Left(msg) => {
                                            if let Some(msg) =
                                                mapping_message(msg, &self.output_indices)
                                            {
                                                match msg {
                                                    Message::Barrier(barrier) => {
                                                        // Rows after the cursor will be reread by the
                                                        // snapshot query started by the new reader.
                                                        let (emitted_chunks, _retained_chunks) =
                                                            partition_current_split_buffer(
                                                                std::mem::take(
                                                                    &mut upstream_chunk_buffer,
                                                                ),
                                                                current_pk_pos.as_ref(),
                                                                &pk_in_output_indices,
                                                                &pk_order,
                                                                &pk_needs_unsigned_i64_compare,
                                                            );
                                                        for chunk in emitted_chunks {
                                                            yield Message::Chunk(chunk);
                                                        }

                                                        state_impl
                                                            .mutate_state(
                                                                split.split_id,
                                                                current_pk_pos.clone(),
                                                                false,
                                                                row_count,
                                                                split_cdc_offset_low.clone(),
                                                                None,
                                                            )
                                                            .await?;
                                                        state_impl
                                                            .commit_state(barrier.epoch)
                                                            .await?;

                                                        if let Some(mutation) =
                                                            barrier.mutation.as_deref()
                                                        {
                                                            use crate::executor::Mutation;
                                                            match mutation {
                                                                Mutation::Pause => {
                                                                    is_snapshot_paused = true;
                                                                }
                                                                Mutation::Resume => {
                                                                    is_snapshot_paused = false;
                                                                }
                                                                Mutation::Throttle(_) => {
                                                                    if let Some(entry) = mutation
                                                                        .backfill_throttle_config(
                                                                            self.actor_ctx
                                                                                .fragment_id,
                                                                        )
                                                                    {
                                                                        self.rate_limit_rps =
                                                                            entry.rate_limit;
                                                                    }
                                                                }
                                                                mutation
                                                                    if mutation.is_stop(
                                                                        self.actor_ctx.id,
                                                                    ) =>
                                                                {
                                                                    yield Message::Barrier(barrier);
                                                                    let () =
                                                                        futures::future::pending()
                                                                            .await;
                                                                    unreachable!();
                                                                }
                                                                _ => (),
                                                            }
                                                        }

                                                        if is_reset_barrier(
                                                            &barrier,
                                                            self.actor_ctx.id,
                                                        ) {
                                                            next_reset_barrier = Some(barrier);
                                                            continue 'with_cdc_table_snapshot_splits;
                                                        }

                                                        yield Message::Barrier(barrier);
                                                    }
                                                    Message::Chunk(chunk) => {
                                                        let (finished_chunk, current_chunk) =
                                                        split_finished_and_current_chunk(
                                                            chunk,
                                                            &finished_split_bounds,
                                                            &current_split_bounds,
                                                            snapshot_split_column_in_output_index,
                                                        );
                                                        if let Some(finished_chunk) = finished_chunk
                                                        {
                                                            yield Message::Chunk(finished_chunk);
                                                        }
                                                        if let Some(current_chunk) = current_chunk {
                                                            upstream_chunk_buffer
                                                                .push(current_chunk);
                                                        }
                                                    }
                                                    Message::Watermark(_) => {}
                                                }
                                            }
                                        }
                                        Either::Right(table_reader) => {
                                            break 'rebuild_reader table_reader;
                                        }
                                    }
                                };

                                pk_needs_unsigned_i64_compare =
                                    table_reader.pk_column_unsigned_i64_compare_flags(&pk_names)?;
                                upstream_table_reader = UpstreamTableReader::new(
                                    self.external_table.clone(),
                                    table_reader,
                                );

                                tracing::info!(
                                    %table_id,
                                    upstream_table_name,
                                    "CDC table reader rebuilt successfully"
                                );

                                continue 'backfill_loop;
                            }
                            SnapshotAttemptState::Finished(split_cdc_offset_high) => {
                                if actor_cdc_offset_high
                                    .as_ref()
                                    .is_none_or(|cur| cur < &split_cdc_offset_high)
                                {
                                    actor_cdc_offset_high = Some(split_cdc_offset_high.clone());
                                }
                                state_impl
                                    .mutate_state(
                                        split.split_id,
                                        current_pk_pos.clone(),
                                        true,
                                        row_count,
                                        split_cdc_offset_low.clone(),
                                        Some(split_cdc_offset_high),
                                    )
                                    .await?;

                                break 'backfill_loop;
                            }
                            SnapshotAttemptState::Reading => {
                                unreachable!(
                                    "backfill stream must not end while the snapshot attempt is still reading"
                                );
                            }
                        }
                    }

                    extends_current_actor_bound(&mut current_actor_bounds, split);
                    if let Some((_, right_split)) = &mut should_report_actor_backfill_progress {
                        assert!(
                            *right_split < split.split_id,
                            "{} {}",
                            *right_split,
                            split.split_id
                        );
                        *right_split = split.split_id;
                    } else {
                        should_report_actor_backfill_progress =
                            Some((split.split_id, split.split_id));
                    }
                }

                upstream_table_reader.disconnect().await?;
            }
            tracing::info!(
                %table_id,
                upstream_table_name,
                "CdcBackfill has already finished and will forward messages directly to the downstream"
            );

            let mut should_report_actor_backfill_done = false;
            // After backfill progress finished
            // we can forward messages directly to the downstream,
            // as backfill is finished.
            #[for_await]
            'forward_upstream: for msg in &mut upstream {
                let msg = msg?;
                match msg {
                    Message::Barrier(barrier) => {
                        state_impl.commit_state(barrier.epoch).await?;
                        if is_reset_barrier(&barrier, self.actor_ctx.id) {
                            next_reset_barrier = Some(barrier);
                            continue 'with_cdc_table_snapshot_splits;
                        }
                        if let Some(split_range) = should_report_actor_backfill_progress.take()
                            && let Some(ref progress) = self.progress
                        {
                            progress.update(
                                self.actor_ctx.fragment_id,
                                self.actor_ctx.id,
                                barrier.epoch,
                                generation.expect(
                                    "should have set generation when having progress to report",
                                ),
                                split_range,
                            );
                        }
                        if should_report_actor_backfill_done {
                            should_report_actor_backfill_done = false;
                            assert!(!actor_snapshot_splits.is_empty());
                            if let Some(ref progress) = self.progress {
                                progress.finish(
                                    self.actor_ctx.fragment_id,
                                    self.actor_ctx.id,
                                    barrier.epoch,
                                    generation.expect(
                                        "should have set generation when having progress to report",
                                    ),
                                    (
                                        actor_snapshot_splits[0].split_id,
                                        actor_snapshot_splits[actor_snapshot_splits.len() - 1]
                                            .split_id,
                                    ),
                                );
                            }
                        }
                        yield Message::Barrier(barrier);
                    }
                    Message::Chunk(chunk) => {
                        if actor_snapshot_splits.is_empty() || !chunk.has_visible_rows() {
                            continue 'forward_upstream;
                        }

                        let chunk_cdc_offset =
                            get_cdc_chunk_last_offset(&offset_parse_func, &chunk)?;
                        // // TODO(zw): re-enable
                        // if *self.external_table.table_type() == ExternalCdcTableType::Postgres
                        //     && let Some(cur) = actor_cdc_offset_low.as_ref()
                        //     && let Some(ref chunk_offset) = chunk_cdc_offset
                        //     && *chunk_offset < *cur
                        // {
                        //     continue;
                        // }

                        // should_report_actor_backfill_done is set to true at most once.
                        if let Some(high) = actor_cdc_offset_high.as_ref() {
                            if state_impl.is_legacy_state() {
                                // Since the legacy state does not track CDC offsets, report backfill completion immediately.
                                actor_cdc_offset_high = None;
                                should_report_actor_backfill_done = true;
                            } else if let Some(ref chunk_offset) = chunk_cdc_offset
                                && *chunk_offset >= *high
                            {
                                // Report backfill completion once the latest CDC offset exceeds the highest offset tracked during the backfill.
                                actor_cdc_offset_high = None;
                                should_report_actor_backfill_done = true;
                            }
                        }
                        let chunk = mapping_chunk(chunk, &self.output_indices);
                        if let Some(filtered_chunk) = filter_stream_chunk(
                            chunk,
                            &current_actor_bounds,
                            snapshot_split_column_in_output_index,
                        ) {
                            yield Message::Chunk(filtered_chunk);
                        }
                    }
                    msg @ Message::Watermark(_) => {
                        if let Some(msg) = mapping_message(msg, &self.output_indices) {
                            yield msg;
                        }
                    }
                }
            }
        }
    }
}

/// Partition buffered CDC rows into those already covered by the current snapshot and those
/// beyond the snapshot cursor.
///
/// The buffer has already been filtered to the current split and mapped to the executor's
/// output schema, so `pk_indices` must refer to positions in that mapped schema.
fn partition_current_split_buffer(
    buffered_chunks: Vec<StreamChunk>,
    current_pk_pos: Option<&OwnedRow>,
    pk_indices: &[usize],
    pk_order: &[OrderType],
    pk_needs_unsigned_i64_compare: &[bool],
) -> (Vec<StreamChunk>, Vec<StreamChunk>) {
    let Some(current_pk_pos) = current_pk_pos else {
        return (vec![], buffered_chunks);
    };

    let mut emitted_chunks = Vec::with_capacity(buffered_chunks.len());
    let mut retained_chunks = Vec::with_capacity(buffered_chunks.len());

    for chunk in buffered_chunks {
        let mut emitted_vis = BitmapBuilder::zeroed(chunk.capacity());
        let mut retained_vis = BitmapBuilder::zeroed(chunk.capacity());

        for (op, row) in chunk.rows() {
            let idx = row.index();

            let reached_current_pos = cmp_pk_unsigned_aware(
                row.project(pk_indices).iter(),
                current_pk_pos.iter(),
                pk_order,
                pk_needs_unsigned_i64_compare,
            )
            .is_le();

            match op {
                Op::Insert | Op::Delete => {
                    if reached_current_pos {
                        emitted_vis.set(idx, true);
                    } else {
                        retained_vis.set(idx, true);
                    }
                }
                Op::UpdateDelete | Op::UpdateInsert => {
                    unreachable!("CDC buffered chunks should not contain update pairs")
                }
            }
        }

        for (vis, output) in [
            (emitted_vis.finish(), &mut emitted_chunks),
            (retained_vis.finish(), &mut retained_chunks),
        ] {
            if vis.any() {
                let new_chunk = chunk.clone_with_vis(vis).compact_vis();
                output.push(new_chunk);
            }
        }
    }

    (emitted_chunks, retained_chunks)
}

/// Split a CDC chunk into rows belonging to already-finished snapshot splits and rows
/// belonging to the snapshot split currently being processed.
///
/// Finished-split rows can be emitted immediately. Current-split rows must be buffered
/// until the snapshot cursor reaches them. Rows outside both ranges are omitted.
///
/// For example, with finished bounds `[0, 100)`, current bounds `[100, 200)`, and CDC rows with
/// split keys `[50, 120, 180, 250]`, this returns `[50]` as the finished chunk and `[120, 180]`
/// as the current chunk. The row with split key `250` is omitted.
fn split_finished_and_current_chunk(
    chunk: StreamChunk,
    finished_split_bounds: &Option<(OwnedRow, OwnedRow)>,
    current_split_bounds: &Option<(OwnedRow, OwnedRow)>,
    snapshot_split_column_index: usize,
) -> (Option<StreamChunk>, Option<StreamChunk>) {
    let finished_chunk = filter_stream_chunk(
        chunk.clone(),
        finished_split_bounds,
        snapshot_split_column_index,
    )
    .map(StreamChunk::compact_vis);
    let current_chunk =
        filter_stream_chunk(chunk, current_split_bounds, snapshot_split_column_index)
            .map(StreamChunk::compact_vis);
    (finished_chunk, current_chunk)
}

/// Keep rows whose snapshot split-column value falls within `bound`'s half-open range
/// `[left, right)`, preserving their operations and relative order through a visibility bitmap.
/// Returns `None` when no bounds are supplied or no visible rows fall within the range.
///
/// For example, filtering split keys `[50, 100, 150, 200]` with bounds `[100, 200)` keeps
/// `[100, 150]`: the left bound is inclusive and the right bound is exclusive.
fn filter_stream_chunk(
    chunk: StreamChunk,
    bound: &Option<(OwnedRow, OwnedRow)>,
    snapshot_split_column_index: usize,
) -> Option<StreamChunk> {
    let Some((left, right)) = bound else {
        return None;
    };
    assert_eq!(left.len(), 1, "multiple split columns is not supported yet");
    assert_eq!(
        right.len(),
        1,
        "multiple split columns is not supported yet"
    );
    let left_split_key = left.datum_at(0);
    let right_split_key = right.datum_at(0);
    let is_leftmost_bound = is_leftmost_bound(left);
    let is_rightmost_bound = is_rightmost_bound(right);
    if is_leftmost_bound && is_rightmost_bound {
        return chunk.has_visible_rows().then_some(chunk);
    }
    let mut new_bitmap = BitmapBuilder::with_capacity(chunk.capacity());
    let (ops, columns, visibility) = chunk.into_inner();
    'filter_rows: for (row_split_key, v) in columns[snapshot_split_column_index]
        .iter()
        .zip_eq_fast(visibility.iter())
    {
        if !v {
            new_bitmap.append(false);
            continue 'filter_rows;
        }
        let mut is_in_range = true;
        if !is_leftmost_bound {
            is_in_range = cmp_datum(
                row_split_key,
                left_split_key,
                OrderType::ascending_nulls_first(),
            )
            .is_ge();
        }
        if is_in_range && !is_rightmost_bound {
            is_in_range = cmp_datum(
                row_split_key,
                right_split_key,
                OrderType::ascending_nulls_first(),
            )
            .is_lt();
        }
        if !is_in_range {
            tracing::trace!(?row_split_key, ?left_split_key, ?right_split_key, snapshot_split_column_index, data_type = ?columns[snapshot_split_column_index].data_type(), "filter out row")
        }
        new_bitmap.append(is_in_range);
    }

    let visibility = new_bitmap.finish();

    visibility
        .any()
        .then_some(StreamChunk::with_visibility(ops, columns, visibility))
}

fn is_leftmost_bound(row: &OwnedRow) -> bool {
    row.iter().all(|d| d.is_none())
}

fn is_rightmost_bound(row: &OwnedRow) -> bool {
    row.iter().all(|d| d.is_none())
}

impl<S: StateStore> Execute for ParallelizedCdcBackfillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

fn extends_current_actor_bound(
    current: &mut Option<(OwnedRow, OwnedRow)>,
    split: &CdcTableSnapshotSplit,
) {
    if current.is_none() {
        *current = Some((
            split.left_bound_inclusive.clone(),
            split.right_bound_exclusive.clone(),
        ));
    } else {
        current.as_mut().unwrap().1 = split.right_bound_exclusive.clone();
    }
}

fn is_reset_barrier(barrier: &Barrier, actor_id: ActorId) -> bool {
    match barrier.mutation.as_deref() {
        Some(Mutation::Update(update)) => update
            .actor_cdc_table_snapshot_splits
            .splits
            .contains_key(&actor_id),
        _ => false,
    }
}

fn assert_consecutive_splits(actor_snapshot_splits: &[CdcTableSnapshotSplit]) {
    for i in 1..actor_snapshot_splits.len() {
        assert_eq!(
            actor_snapshot_splits[i].split_id,
            actor_snapshot_splits[i - 1].split_id + 1,
            "{:?}",
            actor_snapshot_splits
        );
        assert!(
            cmp_datum(
                actor_snapshot_splits[i - 1]
                    .right_bound_exclusive
                    .datum_at(0),
                actor_snapshot_splits[i].right_bound_exclusive.datum_at(0),
                OrderType::ascending_nulls_last(),
            )
            .is_lt()
        );
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::StreamChunk;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::ScalarImpl;

    use crate::executor::backfill::cdc::cdc_backill_v2::{
        filter_stream_chunk, split_finished_and_current_chunk,
    };

    #[test]
    fn test_filter_stream_chunk() {
        use risingwave_common::array::StreamChunkTestExt;
        let chunk = StreamChunk::from_pretty(
            "  I I
             + 1 6
             - 2 .
            U- 3 7
            U+ 4 .",
        );
        let bound = None;
        let c = filter_stream_chunk(chunk.clone(), &bound, 0);
        assert!(c.is_none());

        let bound = Some((OwnedRow::new(vec![None]), OwnedRow::new(vec![None])));
        let c = filter_stream_chunk(chunk.clone(), &bound, 0);
        assert_eq!(c.unwrap().compact_vis(), chunk);

        let bound = Some((
            OwnedRow::new(vec![None]),
            OwnedRow::new(vec![Some(ScalarImpl::Int64(3))]),
        ));
        let c = filter_stream_chunk(chunk.clone(), &bound, 0);
        assert_eq!(
            c.unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I I
             + 1 6
             - 2 .",
            )
        );

        let bound = Some((
            OwnedRow::new(vec![Some(ScalarImpl::Int64(3))]),
            OwnedRow::new(vec![None]),
        ));
        let c = filter_stream_chunk(chunk.clone(), &bound, 0);
        assert_eq!(
            c.unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I I
            U- 3 7
            U+ 4 .",
            )
        );

        let bound = Some((
            OwnedRow::new(vec![Some(ScalarImpl::Int64(2))]),
            OwnedRow::new(vec![Some(ScalarImpl::Int64(4))]),
        ));
        let c = filter_stream_chunk(chunk.clone(), &bound, 0);
        assert_eq!(
            c.unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I I
             - 2 .
            U- 3 7",
            )
        );

        // Test NULL value.
        let bound = None;
        let c = filter_stream_chunk(chunk.clone(), &bound, 1);
        assert!(c.is_none());

        let bound = Some((OwnedRow::new(vec![None]), OwnedRow::new(vec![None])));
        let c = filter_stream_chunk(chunk.clone(), &bound, 1);
        assert_eq!(c.unwrap().compact_vis(), chunk);

        let bound = Some((
            OwnedRow::new(vec![None]),
            OwnedRow::new(vec![Some(ScalarImpl::Int64(7))]),
        ));
        let c = filter_stream_chunk(chunk.clone(), &bound, 1);
        assert_eq!(
            c.unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I I
             + 1 6
             - 2 .
            U+ 4 .",
            )
        );

        let bound = Some((
            OwnedRow::new(vec![Some(ScalarImpl::Int64(7))]),
            OwnedRow::new(vec![None]),
        ));
        let c = filter_stream_chunk(chunk, &bound, 1);
        assert_eq!(
            c.unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I I
            U- 3 7",
            )
        );
    }

    #[test]
    fn test_split_finished_and_current_chunk() {
        use risingwave_common::array::StreamChunkTestExt;

        let chunk = StreamChunk::from_pretty(
            "  I I
             + 1 11
             + 6 10
             + 199 40",
        );
        let finished_split_bounds = Some((
            OwnedRow::new(vec![Some(ScalarImpl::Int64(1))]),
            OwnedRow::new(vec![Some(ScalarImpl::Int64(6))]),
        ));
        let current_split_bounds = Some((
            OwnedRow::new(vec![Some(ScalarImpl::Int64(6))]),
            OwnedRow::new(vec![Some(ScalarImpl::Int64(100))]),
        ));

        let (finished_chunk, current_chunk) = split_finished_and_current_chunk(
            chunk,
            &finished_split_bounds,
            &current_split_bounds,
            0,
        );

        assert_eq!(
            finished_chunk.unwrap(),
            StreamChunk::from_pretty(
                "  I I
                 + 1 11",
            )
        );
        assert_eq!(
            current_chunk.unwrap(),
            StreamChunk::from_pretty(
                "  I I
                 + 6 10",
            )
        );
    }
}
