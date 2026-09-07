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

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Arc;

use futures::future::{Either as FutureEither, select};
use futures::{StreamExt, TryStreamExt, pin_mut};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{
    ArrayImpl, DataChunk, I16Array, I64Array, Op, StreamChunk, StreamChunkBuilder,
};
use risingwave_common::catalog::Schema;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, Datum, ScalarImpl, ToOwnedDatum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::sort_util::cmp_datum_iter;
use risingwave_common_rate_limit::RateLimit;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;

use crate::common::table::state_table::{FlushedStateTableReader, StateTable};
use crate::consistency::consistency_panic;
use crate::executor::backfill::utils::create_builder;
use crate::executor::prelude::*;
use crate::task::{CreateMviewProgressReporter, FragmentId};

type Builders = HashMap<VirtualNode, DataChunkBuilder>;

/// Runtime settings for the sort buffer of [`LocalityProviderExecutor`].
#[derive(Debug, Clone, Copy)]
pub struct SortBufferSettings {
    /// Runtime switch. When false, the executor always passes through input directly even if the
    /// sort buffer table is present in the plan.
    pub enabled: bool,
    /// The number of input rows within one epoch after which buffering activates.
    pub activate_threshold: u64,
}

/// Op code of a buffered change, stored in the `_rw_op` column of the sort buffer table.
/// Update ops are normalized to `Insert`/`Delete` on write (see [`Op::normalize_update`]),
/// and reconstructed as `Update` records by per-key compaction on replay.
fn op_to_code(op: Op) -> i16 {
    op.normalize_update().to_i16()
}

/// Apply one buffered change to the per-key compaction slot, mirroring the semantics of
/// `ChangeBuffer`: consecutive changes of the same key are merged into at most one record.
fn apply_change_to_slot(slot: &mut Option<Record<OwnedRow>>, is_insert: bool, row: OwnedRow) {
    let prev = slot.take();
    *slot = if is_insert {
        match prev {
            None => Some(Record::Insert { new_row: row }),
            Some(Record::Delete { old_row }) => Some(Record::Update {
                old_row,
                new_row: row,
            }),
            Some(Record::Insert { .. }) => {
                consistency_panic!("locality sort buffer: double-inserting the same key");
                Some(Record::Insert { new_row: row })
            }
            Some(Record::Update { old_row, .. }) => {
                consistency_panic!("locality sort buffer: double-inserting the same key");
                Some(Record::Update {
                    old_row,
                    new_row: row,
                })
            }
        }
    } else {
        match prev {
            None => Some(Record::Delete { old_row: row }),
            Some(Record::Insert { .. }) => None,
            Some(Record::Update { old_row, .. }) => Some(Record::Delete { old_row }),
            Some(Record::Delete { .. }) => {
                consistency_panic!("locality sort buffer: double-deleting the same key");
                Some(Record::Delete { old_row: row })
            }
        }
    };
}

/// Take the compacted record of the current key, dropping no-op updates.
fn take_compacted_record(slot: &mut Option<Record<OwnedRow>>) -> Option<Record<OwnedRow>> {
    match slot.take() {
        Some(Record::Update { old_row, new_row }) if old_row == new_row => None,
        record => record,
    }
}

/// Progress state for tracking backfill per vnode
#[derive(Clone, Debug, PartialEq, Eq)]
enum LocalityBackfillProgress {
    /// Backfill not started for this vnode
    NotStarted,
    /// Backfill in progress, tracking current position
    InProgress {
        /// Current position in the locality-ordered scan
        current_pos: OwnedRow,
        /// Number of rows processed for this vnode
        processed_rows: u64,
    },
    /// Backfill completed for this vnode
    Completed {
        /// Final position reached
        final_pos: OwnedRow,
        /// Total rows processed for this vnode
        total_rows: u64,
    },
}

/// State management for locality provider backfill process
#[derive(Clone, Debug)]
struct LocalityBackfillState {
    /// Progress per vnode
    per_vnode: HashMap<VirtualNode, LocalityBackfillProgress>,
    /// Total snapshot rows read across all vnodes
    total_snapshot_rows: u64,
}

impl LocalityBackfillState {
    fn new(vnodes: impl Iterator<Item = VirtualNode>) -> Self {
        let per_vnode = vnodes
            .map(|vnode| (vnode, LocalityBackfillProgress::NotStarted))
            .collect();
        Self {
            per_vnode,
            total_snapshot_rows: 0,
        }
    }

    fn is_completed(&self) -> bool {
        self.per_vnode
            .values()
            .all(|progress| matches!(progress, LocalityBackfillProgress::Completed { .. }))
    }

    fn vnodes(&self) -> impl Iterator<Item = (VirtualNode, &LocalityBackfillProgress)> {
        self.per_vnode
            .iter()
            .map(|(&vnode, progress)| (vnode, progress))
    }

    fn has_progress(&self) -> bool {
        self.per_vnode
            .values()
            .any(|progress| matches!(progress, LocalityBackfillProgress::InProgress { .. }))
    }

    fn update_progress(&mut self, vnode: VirtualNode, new_pos: OwnedRow, row_count_delta: u64) {
        let progress = self.per_vnode.get_mut(&vnode).unwrap();
        match progress {
            LocalityBackfillProgress::NotStarted => {
                *progress = LocalityBackfillProgress::InProgress {
                    current_pos: new_pos,
                    processed_rows: row_count_delta,
                };
            }
            LocalityBackfillProgress::InProgress { processed_rows, .. } => {
                *progress = LocalityBackfillProgress::InProgress {
                    current_pos: new_pos,
                    processed_rows: *processed_rows + row_count_delta,
                };
            }
            LocalityBackfillProgress::Completed { .. } => {
                // Already completed, shouldn't update
            }
        }
        self.total_snapshot_rows += row_count_delta;
    }

    fn finish_vnode(&mut self, vnode: VirtualNode, pk_len: usize) {
        let progress = self.per_vnode.get_mut(&vnode).unwrap();
        match progress {
            LocalityBackfillProgress::NotStarted => {
                // Create a final position with pk_len NULL values to indicate completion
                let final_pos = OwnedRow::new(vec![None; pk_len]);
                *progress = LocalityBackfillProgress::Completed {
                    final_pos,
                    total_rows: 0,
                };
            }
            LocalityBackfillProgress::InProgress {
                current_pos,
                processed_rows,
            } => {
                *progress = LocalityBackfillProgress::Completed {
                    final_pos: current_pos.clone(),
                    total_rows: *processed_rows,
                };
            }
            LocalityBackfillProgress::Completed { .. } => {
                // Already completed
            }
        }
    }

    fn get_progress(&self, vnode: &VirtualNode) -> &LocalityBackfillProgress {
        self.per_vnode.get(vnode).unwrap()
    }
}

/// The `LocalityProviderExecutor` provides locality for operators during backfilling.
/// It buffers input data into a state table using locality columns as primary key prefix.
///
/// The executor implements a proper backfill process similar to arrangement backfill:
/// 1. Backfill phase: Buffer incoming data and provide locality-ordered snapshot reads
/// 2. Completion transition: Drain buffered input and forward new input directly until the
///    barrier that persists backfill completion
/// 3. Forward phase: Optionally sort amplified epochs after that completion barrier
///
/// The sort buffer intentionally does not cover the completion transition. Input drained when the
/// snapshot reaches EOF, or received while waiting for the completion barrier, is forwarded in
/// arrival order. Only input after that barrier is eligible for sorting.
///
/// Key improvements over the original implementation:
/// - Removes arbitrary barrier buffer limit
/// - Implements proper upstream chunk tracking during backfill
/// - Uses per-vnode progress tracking for better state management
pub struct LocalityProviderExecutor<S: StateStore> {
    /// Upstream input
    upstream: Executor,

    /// Locality columns (indices in input schema)
    #[expect(dead_code)]
    locality_columns: Vec<usize>,

    /// State table for buffering input data
    state_table: StateTable<S>,

    /// Progress table for tracking backfill progress per vnode
    progress_table: StateTable<S>,

    /// Ephemeral sorted log table for buffering and re-ordering amplified input within an epoch
    /// after backfill is finished. `None` if the feature was not enabled at plan time.
    sort_buffer_table: Option<StateTable<S>>,

    /// Runtime settings for the sort buffer.
    sort_buffer_settings: SortBufferSettings,

    input_schema: Schema,

    /// Progress reporter for materialized view creation
    progress: CreateMviewProgressReporter,

    fragment_id: FragmentId,

    actor_id: ActorId,

    /// Metrics
    metrics: Arc<StreamingMetrics>,

    /// Chunk size for output
    chunk_size: usize,
}

impl<S: StateStore> LocalityProviderExecutor<S> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        upstream: Executor,
        locality_columns: Vec<usize>,
        state_table: StateTable<S>,
        progress_table: StateTable<S>,
        sort_buffer_table: Option<StateTable<S>>,
        sort_buffer_settings: SortBufferSettings,
        input_schema: Schema,
        progress: CreateMviewProgressReporter,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
        fragment_id: FragmentId,
    ) -> Self {
        Self {
            upstream,
            locality_columns,
            state_table,
            progress_table,
            sort_buffer_table,
            sort_buffer_settings,
            input_schema,
            actor_id: progress.actor_id(),
            progress,
            metrics,
            chunk_size,
            fragment_id,
        }
    }

    /// Creates a snapshot stream that reads from state table in locality order
    #[try_stream(ok = (VirtualNode, OwnedRow), error = StreamExecutorError)]
    async fn make_snapshot_stream(
        reader: FlushedStateTableReader<S>,
        backfill_state: LocalityBackfillState,
    ) {
        // Read from state table per vnode in locality order
        for vnode in reader.vnodes().iter_vnodes() {
            let progress = backfill_state.get_progress(&vnode);

            let current_pos = match progress {
                LocalityBackfillProgress::NotStarted => None,
                LocalityBackfillProgress::Completed { .. } => {
                    // Skip completed vnodes
                    continue;
                }
                LocalityBackfillProgress::InProgress { current_pos, .. } => {
                    Some(current_pos.clone())
                }
            };

            // Compute range bounds for iteration based on current position
            let range_bounds = if let Some(ref pos) = current_pos {
                let start_bound = std::ops::Bound::Excluded(pos.as_inner());
                (start_bound, std::ops::Bound::<&[Datum]>::Unbounded)
            } else {
                (
                    std::ops::Bound::<&[Datum]>::Unbounded,
                    std::ops::Bound::<&[Datum]>::Unbounded,
                )
            };

            // Iterate over rows for this vnode
            let iter = reader
                .iter_with_vnode(
                    vnode,
                    &range_bounds,
                    PrefetchOptions::prefetch_for_small_range_scan(),
                )
                .await?;
            pin_mut!(iter);

            while let Some(row) = iter.try_next().await? {
                yield (vnode, row);
            }
        }
    }

    /// Persist backfill state to progress table
    async fn persist_backfill_state(
        progress_table: &mut StateTable<S>,
        backfill_state: &LocalityBackfillState,
    ) -> StreamExecutorResult<()> {
        for (vnode, progress) in &backfill_state.per_vnode {
            let (is_finished, current_pos, row_count) = match progress {
                LocalityBackfillProgress::NotStarted => continue, // Don't persist NotStarted
                LocalityBackfillProgress::InProgress {
                    current_pos,
                    processed_rows,
                } => (false, current_pos.clone(), *processed_rows),
                LocalityBackfillProgress::Completed {
                    final_pos,
                    total_rows,
                } => (true, final_pos.clone(), *total_rows),
            };

            // Build progress row: vnode + current_pos + is_finished + row_count
            let mut row_data = vec![Some(vnode.to_scalar().into())];
            row_data.extend(current_pos);
            row_data.push(Some(risingwave_common::types::ScalarImpl::Bool(
                is_finished,
            )));
            row_data.push(Some(risingwave_common::types::ScalarImpl::Int64(
                row_count as i64,
            )));

            let new_row = OwnedRow::new(row_data);

            // Check if there's an existing row for this vnode to determine insert vs update
            // This ensures state operation consistency - update existing rows, insert new ones
            let key_data = vec![Some(vnode.to_scalar().into())];
            let key = OwnedRow::new(key_data);

            if let Some(existing_row) = progress_table.get_row(&key).await? {
                // Update existing state - ensures proper state transition for recovery
                progress_table.update(existing_row, new_row);
            } else {
                // Insert new state - first time persisting for this vnode
                progress_table.insert(new_row);
            }
        }
        Ok(())
    }

    /// Load backfill state from progress table
    async fn load_backfill_state(
        progress_table: &StateTable<S>,
    ) -> StreamExecutorResult<LocalityBackfillState> {
        let mut backfill_state = LocalityBackfillState::new(progress_table.vnodes().iter_vnodes());
        let mut total_snapshot_rows = 0;

        // For each vnode, try to get its progress state
        for vnode in progress_table.vnodes().iter_vnodes() {
            // Build key: vnode + NULL values for locality columns (to match progress table schema)
            let key_data = vec![Some(vnode.to_scalar().into())];

            let key = OwnedRow::new(key_data);

            if let Some(row) = progress_table.get_row(&key).await? {
                // Parse is_finished flag (second to last column)
                let finished_col_idx = row.len() - 2;
                let is_finished = row
                    .datum_at(finished_col_idx)
                    .map(|d| d.into_bool())
                    .unwrap_or(false);

                // Parse row count (last column)
                let row_count = row
                    .datum_at(row.len() - 1)
                    .map(|d| d.into_int64() as u64)
                    .unwrap_or(0);

                let current_pos_data: Vec<Datum> = (1..finished_col_idx)
                    .map(|i| row.datum_at(i).to_owned_datum())
                    .collect();
                let current_pos = OwnedRow::new(current_pos_data);

                // Set progress based on is_finished flag
                let progress = if is_finished {
                    LocalityBackfillProgress::Completed {
                        final_pos: current_pos,
                        total_rows: row_count,
                    }
                } else {
                    LocalityBackfillProgress::InProgress {
                        current_pos,
                        processed_rows: row_count,
                    }
                };

                backfill_state.per_vnode.insert(vnode, progress);
                total_snapshot_rows += row_count;
            }
            // If no row found, keep the default NotStarted state
        }

        backfill_state.total_snapshot_rows = total_snapshot_rows;
        Ok(backfill_state)
    }

    /// Mark chunk for forwarding based on backfill progress
    fn mark_chunk(
        chunk: StreamChunk,
        backfill_state: &LocalityBackfillState,
        state_table: &StateTable<S>,
    ) -> StreamExecutorResult<StreamChunk> {
        let chunk = chunk.compact_vis();
        let (data, ops) = chunk.into_parts();
        let mut new_visibility = risingwave_common::bitmap::BitmapBuilder::with_capacity(ops.len());

        let pk_indices = state_table.pk_indices();
        let pk_order = state_table.pk_serde().get_order_types();

        for row in data.rows() {
            // Project to primary key columns for comparison
            let pk = row.project(pk_indices);
            let vnode = state_table.compute_vnode_by_pk(pk);

            let visible = match backfill_state.get_progress(&vnode) {
                LocalityBackfillProgress::Completed { .. } => true,
                LocalityBackfillProgress::NotStarted => false,
                LocalityBackfillProgress::InProgress { current_pos, .. } => {
                    // Compare primary key with current position
                    cmp_datum_iter(pk.iter(), current_pos.iter(), pk_order.iter().copied()).is_le()
                }
            };

            new_visibility.append(visible);
        }

        let (columns, _) = data.into_parts();
        let chunk = StreamChunk::with_visibility(ops, columns, new_visibility.finish());
        Ok(chunk)
    }

    fn handle_snapshot_chunk(
        data_chunk: DataChunk,
        vnode: VirtualNode,
        pk_indices: &[usize],
        backfill_state: &mut LocalityBackfillState,
        cur_barrier_snapshot_processed_rows: &mut u64,
    ) -> StreamExecutorResult<StreamChunk> {
        let chunk = StreamChunk::from_parts(vec![Op::Insert; data_chunk.cardinality()], data_chunk);
        let chunk_cardinality = chunk.cardinality() as u64;

        // Extract primary key from the last row to update progress
        // As snapshot read streams are ordered by pk, we can use the last row to update current_pos
        if let Some(last_row) = chunk.rows().last() {
            let pk = last_row.1.project(pk_indices);
            let pk_owned = pk.into_owned_row();
            backfill_state.update_progress(vnode, pk_owned, chunk_cardinality);
        }

        *cur_barrier_snapshot_processed_rows += chunk_cardinality;
        Ok(chunk)
    }

    /// Write a compacted input chunk into the sort buffer table as an append-only log.
    /// Each row is extended with `gen` (the current epoch generation), `op` (normalized op code)
    /// and `seq` (monotonic sequence number within the epoch), all writes using `Op::Insert` with
    /// unique keys.
    fn write_chunk_to_sort_buffer(
        sort_buffer_table: &mut StateTable<S>,
        chunk: &StreamChunk,
        generation: i64,
        seq: &mut i64,
    ) {
        let cardinality = chunk.cardinality();
        debug_assert_eq!(
            cardinality,
            chunk.capacity(),
            "chunk must be compacted before buffering"
        );

        let mut columns = chunk.data_chunk().columns().to_vec();
        columns.push(Arc::new(ArrayImpl::Int64(I64Array::from_iter(
            std::iter::repeat_n(Some(generation), cardinality),
        ))));
        columns.push(Arc::new(ArrayImpl::Int16(I16Array::from_iter(
            chunk.ops().iter().map(|op| Some(op_to_code(*op))),
        ))));
        columns.push(Arc::new(ArrayImpl::Int64(I64Array::from_iter(
            (*seq..*seq + cardinality as i64).map(Some),
        ))));
        *seq += cardinality as i64;

        let log_chunk = StreamChunk::new(vec![Op::Insert; cardinality], columns);
        sort_buffer_table.write_chunk(log_chunk);
    }

    /// Replay this epoch's buffered changes from the sort buffer table in locality order,
    /// compacting consecutive changes of the same key (locality columns + stream key) into at
    /// most one record.
    ///
    /// Must be called *after* the sort buffer table has been committed for the epoch, so that all
    /// buffered rows are visible to the flushed snapshot reader (staging imms + spilled SSTs).
    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn make_sort_buffer_replay_stream(
        reader: FlushedStateTableReader<S>,
        generation: i64,
        group_key_indices: Vec<usize>,
        input_len: usize,
        data_types: Vec<DataType>,
        chunk_size: usize,
    ) {
        // Scan exactly this epoch's generation via a prefix range on the first pk column.
        let range = (
            Bound::Included(OwnedRow::new(vec![Some(ScalarImpl::Int64(generation))])),
            Bound::Excluded(OwnedRow::new(vec![Some(ScalarImpl::Int64(generation + 1))])),
        );

        let mut chunk_builder = StreamChunkBuilder::new(chunk_size, data_types);
        let mut cur_key: Option<OwnedRow> = None;
        let mut slot: Option<Record<OwnedRow>> = None;

        // Each key belongs to exactly one vnode, so a per-vnode sequential scan already yields
        // all changes of one key contiguously, which is what downstream locality needs.
        for vnode in reader.vnodes().iter_vnodes() {
            let iter = reader
                .iter_with_vnode(
                    vnode,
                    &range,
                    PrefetchOptions::prefetch_for_large_range_scan(),
                )
                .await?;
            pin_mut!(iter);

            while let Some(row) = iter.try_next().await? {
                // Row layout: input columns ++ [gen, op, seq]
                let is_insert = row
                    .datum_at(input_len + 1)
                    .expect("op column must not be null")
                    .into_int16()
                    == Op::Insert.to_i16();

                let same_key = match &cur_key {
                    Some(key) => (&row).project(&group_key_indices).iter().eq(key.iter()),
                    None => false,
                };
                if !same_key {
                    if let Some(record) = take_compacted_record(&mut slot)
                        && let Some(chunk) = chunk_builder.append_record(record)
                    {
                        yield chunk;
                    }
                    cur_key = Some((&row).project(&group_key_indices).into_owned_row());
                }

                let input_row = OwnedRow::new(row.as_inner()[..input_len].to_vec());
                apply_change_to_slot(&mut slot, is_insert, input_row);
            }
        }

        if let Some(record) = take_compacted_record(&mut slot)
            && let Some(chunk) = chunk_builder.append_record(record)
        {
            yield chunk;
        }
        if let Some(chunk) = chunk_builder.take() {
            yield chunk;
        }
    }
}

impl<S: StateStore> Execute for LocalityProviderExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl<S: StateStore> LocalityProviderExecutor<S> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut upstream = self.upstream.execute();

        // Wait for first barrier to initialize
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let first_epoch = first_barrier.epoch;

        // Propagate the first barrier
        yield Message::Barrier(first_barrier);

        let mut state_table = self.state_table;
        let mut progress_table = self.progress_table;
        let mut sort_buffer_table = self.sort_buffer_table;

        // Initialize state tables
        state_table.init_epoch(first_epoch).await?;
        progress_table.init_epoch(first_epoch).await?;
        if let Some(sort_table) = &mut sort_buffer_table {
            sort_table.init_epoch(first_epoch).await?;
        }

        // The current write epoch, used as the generation (`gen`) of buffered changes in the
        // sort buffer table. Updated at every barrier.
        let mut cur_gen: u64 = first_epoch.curr;

        // Load backfill state from progress table
        let mut backfill_state = Self::load_backfill_state(&progress_table).await?;

        // Get pk info from state table
        let pk_indices = state_table.pk_indices().iter().cloned().collect_vec();

        let need_backfill = !backfill_state.is_completed();
        let mut report_finished_on_first_barrier = !need_backfill;

        let need_buffering = backfill_state
            .per_vnode
            .values()
            .all(|progress| matches!(progress, LocalityBackfillProgress::NotStarted));
        // Initial buffering phase before backfill - wait for StartFragmentBackfill mutation (if needed)
        if need_buffering {
            // Enter buffering phase - buffer data until StartFragmentBackfill is received
            let mut start_backfill = false;

            #[for_await]
            for msg in upstream.by_ref() {
                let msg = msg?;

                match msg {
                    Message::Watermark(_) => {
                        // Ignore watermarks during initial buffering
                    }
                    Message::Chunk(chunk) => {
                        state_table.write_chunk(chunk);
                        state_table.try_flush().await?;
                    }
                    Message::Barrier(barrier) => {
                        let epoch = barrier.epoch;

                        // Check for StartFragmentBackfill mutation
                        if let Some(mutation) = barrier.mutation.as_deref() {
                            use crate::executor::Mutation;
                            if let Mutation::StartFragmentBackfill { fragment_ids } = mutation
                                && fragment_ids.contains(&self.fragment_id)
                            {
                                tracing::info!(
                                    "Start backfill of locality provider with fragment id: {:?}",
                                    &self.fragment_id
                                );
                                start_backfill = true;
                            }
                        }

                        // Commit state tables
                        barrier.assume_no_update_vnode_bitmap(self.actor_id)?;
                        state_table
                            .commit_assert_no_update_vnode_bitmap(epoch)
                            .await?;
                        progress_table
                            .commit_assert_no_update_vnode_bitmap(epoch)
                            .await?;
                        if let Some(sort_table) = &mut sort_buffer_table {
                            sort_table
                                .commit_assert_no_update_vnode_bitmap(epoch)
                                .await?;
                        }
                        cur_gen = epoch.curr;

                        yield Message::Barrier(barrier);

                        // Start backfill when StartFragmentBackfill mutation is received
                        if start_backfill {
                            break;
                        }
                    }
                }
            }
        }

        // Locality Provider Backfill Algorithm (adapted from Arrangement Backfill):
        //
        //   backfill_stream
        //  /               \
        // upstream       snapshot (from state_table)
        //
        // We construct a backfill stream with upstream as its left input and locality-ordered
        // snapshot read stream as its right input. When a chunk comes from upstream, we buffer it.
        //
        // When a barrier comes from upstream:
        //  - For each row of the upstream chunk buffer, compute vnode.
        //  - Get the `current_pos` corresponding to the vnode. Forward it to downstream if its
        //    locality key <= `current_pos`, otherwise ignore it.
        //  - Flush all buffered upstream_chunks to state table.
        //  - Persist backfill progress to progress table.
        //  - Reconstruct the snapshot read stream only if buffered upstream chunks changed the
        //    state table. Otherwise, continue the same snapshot read stream across the barrier.
        //
        // When a chunk comes from snapshot, we forward it to the downstream and raise
        // `current_pos`.
        //
        // When we reach the end of the snapshot read stream, it means backfill has been
        // finished.
        //
        // Once the backfill loop ends, we forward the upstream directly to the downstream.

        if need_backfill {
            let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];

            let metrics = self
                .metrics
                .new_backfill_metrics(state_table.table_id(), self.actor_id);

            // Create builders for snapshot data chunks
            let snapshot_data_types = self.input_schema.data_types();
            let mut builders: Builders = state_table
                .vnodes()
                .iter_vnodes()
                .map(|vnode| {
                    let builder = create_builder(
                        RateLimit::Disabled,
                        self.chunk_size,
                        snapshot_data_types.clone(),
                    );
                    (vnode, builder)
                })
                .collect();

            let snapshot_reader = state_table.flushed_snapshot_reader();
            let snapshot_stream =
                Self::make_snapshot_stream(snapshot_reader.clone(), backfill_state.clone());
            pin_mut!(snapshot_stream);

            'backfill_loop: loop {
                let mut cur_barrier_snapshot_processed_rows: u64 = 0;
                let mut cur_barrier_upstream_processed_rows: u64 = 0;

                // Prefer upstream so a ready barrier can pause snapshot output promptly, while
                // keeping the snapshot stream itself alive across barriers with no upstream data.
                let barrier = loop {
                    let upstream_next = upstream.next();
                    let mut snapshot_stream_ref = snapshot_stream.as_mut();
                    let snapshot_next = snapshot_stream_ref.next();
                    pin_mut!(upstream_next);
                    pin_mut!(snapshot_next);

                    match select(upstream_next, snapshot_next).await {
                        FutureEither::Left((msg, _)) => match msg.transpose()? {
                            Some(Message::Barrier(barrier)) => {
                                // Process the barrier after draining the snapshot builders.
                                break barrier;
                            }
                            Some(Message::Chunk(chunk)) => {
                                // Buffer the upstream chunk.
                                upstream_chunk_buffer.push(chunk.compact_vis());
                            }
                            Some(Message::Watermark(_)) => {
                                // Ignore watermark during backfill.
                            }
                            None => {
                                return Err(anyhow::anyhow!(
                                    "locality provider upstream ended unexpectedly during backfill"
                                )
                                .into());
                            }
                        },
                        FutureEither::Right((msg, _)) => match msg.transpose()? {
                            Some((vnode, row)) => {
                                // Use builder to batch rows efficiently
                                let builder = builders.get_mut(&vnode).unwrap();
                                if let Some(data_chunk) = builder.append_one_row(row) {
                                    // Builder is full, handle the chunk
                                    let chunk = Self::handle_snapshot_chunk(
                                        data_chunk,
                                        vnode,
                                        &pk_indices,
                                        &mut backfill_state,
                                        &mut cur_barrier_snapshot_processed_rows,
                                    )?;
                                    yield Message::Chunk(chunk);
                                }
                                // If append_one_row returns None, row is buffered but no chunk is produced yet
                                // Progress will be updated when the builder is consumed later
                            }
                            None => {
                                // End of the snapshot read stream.
                                // Consume remaining rows in the builders.
                                for (vnode, builder) in &mut builders {
                                    if let Some(data_chunk) = builder.consume_all() {
                                        let chunk = Self::handle_snapshot_chunk(
                                            data_chunk,
                                            *vnode,
                                            &pk_indices,
                                            &mut backfill_state,
                                            &mut cur_barrier_snapshot_processed_rows,
                                        )?;
                                        yield Message::Chunk(chunk);
                                    }
                                }

                                // Consume remaining rows in the upstream buffer.
                                for chunk in upstream_chunk_buffer.drain(..) {
                                    let chunk_cardinality = chunk.cardinality() as u64;
                                    cur_barrier_upstream_processed_rows += chunk_cardinality;
                                    yield Message::Chunk(chunk);
                                }
                                metrics
                                    .backfill_snapshot_read_row_count
                                    .inc_by(cur_barrier_snapshot_processed_rows);
                                metrics
                                    .backfill_upstream_output_row_count
                                    .inc_by(cur_barrier_upstream_processed_rows);
                                break 'backfill_loop;
                            }
                        },
                    }
                };

                // Consume remaining rows from builders at barrier
                for (vnode, builder) in &mut builders {
                    if let Some(data_chunk) = builder.consume_all() {
                        let chunk = Self::handle_snapshot_chunk(
                            data_chunk,
                            *vnode,
                            &pk_indices,
                            &mut backfill_state,
                            &mut cur_barrier_snapshot_processed_rows,
                        )?;
                        yield Message::Chunk(chunk);
                    }
                }

                // Process upstream buffer chunks with marking
                let should_refresh_snapshot = !upstream_chunk_buffer.is_empty();
                for chunk in upstream_chunk_buffer.drain(..) {
                    cur_barrier_upstream_processed_rows += chunk.cardinality() as u64;

                    // Mark chunk based on backfill progress
                    if backfill_state.has_progress() {
                        let marked_chunk =
                            Self::mark_chunk(chunk.clone(), &backfill_state, &state_table)?;
                        yield Message::Chunk(marked_chunk);
                    }

                    // Persist buffered upstream chunk into state table so subsequent snapshot
                    // iterations see the latest writes.
                    state_table.write_chunk(chunk);
                }

                let barrier_epoch = barrier.epoch;
                barrier.assume_no_update_vnode_bitmap(self.actor_id)?;
                state_table
                    .commit_assert_no_update_vnode_bitmap(barrier_epoch)
                    .await?;
                if let Some(sort_table) = &mut sort_buffer_table {
                    sort_table
                        .commit_assert_no_update_vnode_bitmap(barrier_epoch)
                        .await?;
                }
                cur_gen = barrier_epoch.curr;

                // Update progress with current epoch and snapshot read count
                // Report both consumed rows and buffered rows separately for precise progress
                let total_snapshot_processed_rows: u64 = backfill_state
                    .vnodes()
                    .map(|(_, progress)| match *progress {
                        LocalityBackfillProgress::InProgress { processed_rows, .. } => {
                            processed_rows
                        }
                        LocalityBackfillProgress::Completed { total_rows, .. } => total_rows,
                        LocalityBackfillProgress::NotStarted => 0,
                    })
                    .sum();

                self.progress.update_with_buffered_rows(
                    barrier.epoch,
                    barrier.epoch.curr, // Use barrier epoch as snapshot read epoch
                    total_snapshot_processed_rows,
                    0,
                );

                // Persist backfill progress
                Self::persist_backfill_state(&mut progress_table, &backfill_state).await?;
                progress_table
                    .commit_assert_no_update_vnode_bitmap(barrier_epoch)
                    .await?;

                metrics
                    .backfill_snapshot_read_row_count
                    .inc_by(cur_barrier_snapshot_processed_rows);
                metrics
                    .backfill_upstream_output_row_count
                    .inc_by(cur_barrier_upstream_processed_rows);

                yield Message::Barrier(barrier);

                if should_refresh_snapshot {
                    snapshot_stream.set(Self::make_snapshot_stream(
                        snapshot_reader.clone(),
                        backfill_state.clone(),
                    ));
                }
            }
        }

        tracing::debug!("Locality provider backfill finished, forwarding upstream directly");

        // Wait for the first barrier after snapshot EOF to mark progress as finished. Chunks in
        // this transition are forwarded directly; the sort buffer starts only after this barrier.
        if need_backfill && !backfill_state.is_completed() {
            while let Some(Ok(msg)) = upstream.next().await {
                match msg {
                    Message::Barrier(barrier) => {
                        barrier.assume_no_update_vnode_bitmap(self.actor_id)?;

                        // no-op commit state table
                        state_table
                            .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                            .await?;
                        if let Some(sort_table) = &mut sort_buffer_table {
                            sort_table
                                .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                                .await?;
                        }
                        cur_gen = barrier.epoch.curr;

                        // Mark all vnodes as completed
                        for vnode in state_table.vnodes().iter_vnodes() {
                            backfill_state.finish_vnode(vnode, pk_indices.len());
                        }

                        // Calculate final total processed rows
                        let total_snapshot_processed_rows: u64 = backfill_state
                            .vnodes()
                            .map(|(_, progress)| match *progress {
                                LocalityBackfillProgress::Completed { total_rows, .. } => {
                                    total_rows
                                }
                                LocalityBackfillProgress::InProgress { processed_rows, .. } => {
                                    processed_rows
                                }
                                LocalityBackfillProgress::NotStarted => 0,
                            })
                            .sum();

                        // Finish progress reporting with any remaining buffered rows
                        // At completion, we report `total_snapshot_processed_rows` as buffered rows to make progress accurate.
                        self.progress.finish_with_buffered_rows(
                            barrier.epoch,
                            total_snapshot_processed_rows,
                            total_snapshot_processed_rows,
                        );

                        // Persist final state
                        Self::persist_backfill_state(&mut progress_table, &backfill_state).await?;
                        progress_table
                            .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                            .await?;

                        yield Message::Barrier(barrier);
                        break; // Exit the loop after processing the barrier
                    }
                    Message::Chunk(chunk) => {
                        // Forward chunks directly during completion phase
                        yield Message::Chunk(chunk);
                    }
                    Message::Watermark(watermark) => {
                        // Forward watermarks directly during completion phase
                        yield Message::Watermark(watermark);
                    }
                }
            }
        }

        // After the completion barrier, forward messages directly. When the sort buffer is enabled
        // and an epoch turns out to be amplified (row count exceeds the activation threshold),
        // switch to buffering the rest of the epoch into the sort buffer table and replay it in
        // locality order at the barrier, so that downstream operators process the amplified
        // changes with much better cache locality, while the buffer memory stays bounded by the
        // shared buffer (spillable to L0).
        let sort_buffer_active = sort_buffer_table.is_some() && self.sort_buffer_settings.enabled;
        let activate_threshold = self.sort_buffer_settings.activate_threshold;

        // The group key (locality columns + stream key) for per-key compaction during replay.
        // It equals the pk of the buffer state table (which is `gen` + group key + `seq`) without
        // the `gen`/`seq` columns, and also equals the pk of the backfill state table.
        let group_key_indices = state_table.pk_indices().to_vec();
        let input_data_types = self.input_schema.data_types();
        let input_len = self.input_schema.len();

        // Per-epoch states of the sort buffer. `buffering` switches one way (pass-through ->
        // buffering) within an epoch and resets at barriers, so that per-key ordering is
        // preserved: the passed-through prefix is emitted before any buffered change.
        let mut buffering = false;
        let mut rows_in_epoch: u64 = 0;
        let mut seq: i64 = 0;
        // Advance the state-clean watermark at the next barrier: initially true to clean
        // possible leftovers of previous incarnations after recovery, and set again after each
        // buffered epoch to clean its generation.
        let mut needs_cleanup = true;
        // Watermark messages held back while buffering, keyed by column index. They are emitted
        // after the replayed chunks so that they never precede the buffered rows they constrain.
        let mut pending_watermarks: BTreeMap<usize, Watermark> = BTreeMap::new();

        #[for_await]
        for msg in upstream {
            let msg = msg?;

            match msg {
                Message::Chunk(chunk) => {
                    if !buffering {
                        rows_in_epoch += chunk.cardinality() as u64;
                        if sort_buffer_active && rows_in_epoch > activate_threshold {
                            tracing::info!(
                                actor_id = %self.actor_id,
                                fragment_id = %self.fragment_id,
                                rows_in_epoch,
                                activate_threshold,
                                "locality provider sort buffer activated for this epoch"
                            );
                            buffering = true;
                        }
                    }
                    if buffering {
                        let chunk = chunk.compact_vis();
                        let sort_table = sort_buffer_table.as_mut().unwrap();
                        Self::write_chunk_to_sort_buffer(
                            sort_table,
                            &chunk,
                            cur_gen as i64,
                            &mut seq,
                        );
                        sort_table.try_flush().await?;
                    } else {
                        yield Message::Chunk(chunk);
                    }
                }
                Message::Watermark(watermark) => {
                    if buffering {
                        pending_watermarks.insert(watermark.col_idx, watermark);
                    } else {
                        yield Message::Watermark(watermark);
                    }
                }
                Message::Barrier(barrier) => {
                    barrier.assume_no_update_vnode_bitmap(self.actor_id)?;

                    // Commit state tables but don't modify them
                    state_table
                        .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                        .await?;
                    progress_table
                        .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                        .await?;

                    if let Some(sort_table) = &mut sort_buffer_table {
                        debug_assert_eq!(cur_gen, barrier.epoch.prev);

                        if buffering || needs_cleanup {
                            // Advancing the watermark to the current generation invalidates all
                            // older generations, which have been fully replayed before their
                            // barriers (or discarded by recovery). The current generation
                            // survives (cleaning is exclusive) and gets cleaned at the next
                            // such barrier.
                            sort_table.update_watermark(ScalarImpl::Int64(cur_gen as i64));
                        }
                        // Commit *before* replay: sealing the epoch makes all buffered rows of
                        // this epoch stably visible to the flushed snapshot reader.
                        sort_table
                            .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                            .await?;

                        if buffering {
                            let replay_stream = Self::make_sort_buffer_replay_stream(
                                sort_table.flushed_snapshot_reader(),
                                cur_gen as i64,
                                group_key_indices.clone(),
                                input_len,
                                input_data_types.clone(),
                                self.chunk_size,
                            );
                            pin_mut!(replay_stream);
                            while let Some(chunk) = replay_stream.try_next().await? {
                                yield Message::Chunk(chunk);
                            }

                            for (_, watermark) in std::mem::take(&mut pending_watermarks) {
                                yield Message::Watermark(watermark);
                            }
                        }
                        needs_cleanup = buffering;
                    }

                    if report_finished_on_first_barrier {
                        // At completion, we report `total_snapshot_rows` as buffered rows to make progress accurate.
                        self.progress.finish_with_buffered_rows(
                            barrier.epoch,
                            backfill_state.total_snapshot_rows,
                            backfill_state.total_snapshot_rows,
                        );
                        report_finished_on_first_barrier = false;
                    }

                    cur_gen = barrier.epoch.curr;
                    buffering = false;
                    rows_in_epoch = 0;
                    seq = 0;

                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use risingwave_common::array::StreamChunkTestExt;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::{EpochPair, test_epoch};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;
    use tokio::time::timeout;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::test_utils::{MockSource, StreamExecutorTestExt};
    use crate::task::LocalBarrierManager;

    fn row(values: impl IntoIterator<Item = i32>) -> OwnedRow {
        OwnedRow::new(
            values
                .into_iter()
                .map(|v| Some(ScalarImpl::Int32(v)))
                .collect(),
        )
    }

    /// Destructure a record into comparable parts, as [`Record`] does not implement `PartialEq`.
    fn parts(record: Option<Record<OwnedRow>>) -> Option<(&'static str, Vec<OwnedRow>)> {
        record.map(|record| match record {
            Record::Insert { new_row } => ("insert", vec![new_row]),
            Record::Delete { old_row } => ("delete", vec![old_row]),
            Record::Update { old_row, new_row } => ("update", vec![old_row, new_row]),
        })
    }

    #[test]
    fn test_per_key_compaction_slot() {
        // Insert then delete cancels out.
        let mut slot = None;
        apply_change_to_slot(&mut slot, true, row([1]));
        apply_change_to_slot(&mut slot, false, row([1]));
        assert_eq!(parts(take_compacted_record(&mut slot)), None);

        // Delete then insert becomes an update.
        let mut slot = None;
        apply_change_to_slot(&mut slot, false, row([1]));
        apply_change_to_slot(&mut slot, true, row([2]));
        assert_eq!(
            parts(take_compacted_record(&mut slot)),
            Some(("update", vec![row([1]), row([2])]))
        );

        // Delete, insert, delete collapses to a single delete of the original row.
        let mut slot = None;
        apply_change_to_slot(&mut slot, false, row([1]));
        apply_change_to_slot(&mut slot, true, row([2]));
        apply_change_to_slot(&mut slot, false, row([2]));
        assert_eq!(
            parts(take_compacted_record(&mut slot)),
            Some(("delete", vec![row([1])]))
        );

        // No-op update (same old and new row) is dropped.
        let mut slot = None;
        apply_change_to_slot(&mut slot, false, row([1]));
        apply_change_to_slot(&mut slot, true, row([1]));
        assert_eq!(parts(take_compacted_record(&mut slot)), None);

        // Plain insert is kept.
        let mut slot = None;
        apply_change_to_slot(&mut slot, true, row([1]));
        assert_eq!(
            parts(take_compacted_record(&mut slot)),
            Some(("insert", vec![row([1])]))
        );
    }

    /// Write interleaved changes of one epoch into the sort buffer table, commit, and replay.
    /// Verifies that the replay is grouped by key (locality + stream key), per-key compacted,
    /// and only covers the requested generation.
    #[tokio::test]
    async fn test_sort_buffer_write_and_replay() {
        // Input schema: a (locality column), b (stream key), c (payload)
        let input_data_types = vec![DataType::Int32, DataType::Int32, DataType::Int32];
        let input_len = input_data_types.len();
        let group_key_indices = vec![0, 1];

        // Buffer table: input columns ++ [_rw_gen, _rw_op, _rw_seq],
        // pk = gen + locality (a) + stream key (b) + seq
        let column_descs = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int32),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int32),
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int32),
            ColumnDesc::unnamed(ColumnId::new(3), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(4), DataType::Int16),
            ColumnDesc::unnamed(ColumnId::new(5), DataType::Int64),
        ];
        let pk_indices = vec![3, 0, 1, 5];
        let order_types = vec![OrderType::ascending(); 4];

        let mut table = StateTable::from_table_catalog(
            &gen_pbtable(TableId::new(1), column_descs, order_types, pk_indices, 0),
            MemoryStateStore::new(),
            None,
        )
        .await;

        let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
        table.init_epoch(epoch).await.unwrap();

        let generation = test_epoch(1) as i64;
        let mut seq = 0;

        // Changes of the current generation, arriving in arbitrary key order:
        // - key (3, 1): insert then delete -> cancels out
        // - key (1, 1): plain insert
        // - key (2, 1): update pair
        // - key (1, 2): insert
        let chunk = StreamChunk::from_pretty(
            "  i i i
             + 3 1 100
             + 1 1 10
            U- 2 1 20
            U+ 2 1 21
             + 1 2 11
             - 3 1 100",
        );
        LocalityProviderExecutor::<MemoryStateStore>::write_chunk_to_sort_buffer(
            &mut table, &chunk, generation, &mut seq,
        );
        assert_eq!(seq, 6);

        // Changes of a different (newer) generation must not appear in the replay.
        let other_chunk = StreamChunk::from_pretty(
            " i i i
            + 9 9 99",
        );
        let mut other_seq = 0;
        LocalityProviderExecutor::<MemoryStateStore>::write_chunk_to_sort_buffer(
            &mut table,
            &other_chunk,
            test_epoch(2) as i64,
            &mut other_seq,
        );

        // Commit before replay, mirroring the executor's barrier handling.
        epoch.inc_for_test();
        table.commit_for_test(epoch).await.unwrap();

        let replay_stream =
            LocalityProviderExecutor::<MemoryStateStore>::make_sort_buffer_replay_stream(
                table.flushed_snapshot_reader(),
                generation,
                group_key_indices,
                input_len,
                input_data_types,
                2,
            );
        pin_mut!(replay_stream);
        let mut chunks = vec![];
        while let Some(chunk) = replay_stream.try_next().await.unwrap() {
            chunks.push(chunk);
        }

        // Replay is ordered by (a, b), per-key compacted, with the cancelled key dropped.
        assert_eq!(chunks.len(), 2);
        assert_eq!(
            chunks[0],
            StreamChunk::from_pretty(
                " i i i
                + 1 1 10
                + 1 2 11",
            )
        );
        assert_eq!(
            chunks[1],
            StreamChunk::from_pretty(
                "  i i i
                U- 2 1 20
                U+ 2 1 21",
            )
        );
    }

    #[tokio::test]
    async fn test_sort_buffer_starts_after_backfill_completion_barrier() {
        let store = MemoryStateStore::new();
        let input_schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let input_columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int32),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int32),
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int32),
        ];

        // State table: input columns, pk = locality column + stream key.
        let state_table = StateTable::from_table_catalog(
            &gen_pbtable(
                TableId::new(1),
                input_columns.clone(),
                vec![OrderType::ascending(); 2],
                vec![0, 1],
                0,
            ),
            store.clone(),
            None,
        )
        .await;

        // Progress table: vnode + state-table pk + completion flag + processed rows.
        let progress_catalog = gen_pbtable(
            TableId::new(2),
            vec![
                ColumnDesc::unnamed(ColumnId::new(0), VirtualNode::RW_TYPE),
                ColumnDesc::unnamed(ColumnId::new(1), DataType::Int32),
                ColumnDesc::unnamed(ColumnId::new(2), DataType::Int32),
                ColumnDesc::unnamed(ColumnId::new(3), DataType::Boolean),
                ColumnDesc::unnamed(ColumnId::new(4), DataType::Int64),
            ],
            vec![OrderType::ascending()],
            vec![0],
            1,
        );
        let progress_table =
            StateTable::from_table_catalog(&progress_catalog, store.clone(), None).await;

        // Sort buffer: input columns ++ [gen, op, seq],
        // pk = gen + locality column + stream key + seq.
        let mut sort_buffer_catalog = gen_pbtable(
            TableId::new(3),
            vec![
                input_columns[0].clone(),
                input_columns[1].clone(),
                input_columns[2].clone(),
                ColumnDesc::unnamed(ColumnId::new(3), DataType::Int64),
                ColumnDesc::unnamed(ColumnId::new(4), DataType::Int16),
                ColumnDesc::unnamed(ColumnId::new(5), DataType::Int64),
            ],
            vec![OrderType::ascending(); 4],
            vec![3, 0, 1, 5],
            0,
        );
        sort_buffer_catalog.clean_watermark_indices = vec![3];
        let sort_buffer_table =
            StateTable::from_table_catalog_inconsistent_op(&sort_buffer_catalog, store, None).await;

        let fragment_id: FragmentId = 1.into();
        let progress = CreateMviewProgressReporter::for_test(LocalBarrierManager::for_test());
        let (mut tx, source) = MockSource::channel();
        let upstream = source.into_executor(input_schema.clone(), vec![1]);
        let mut executor = LocalityProviderExecutor::new(
            upstream,
            vec![0],
            state_table,
            progress_table,
            Some(sort_buffer_table),
            SortBufferSettings {
                enabled: true,
                activate_threshold: 0,
            },
            input_schema,
            progress,
            Arc::new(StreamingMetrics::unused()),
            1024,
            fragment_id,
        )
        .boxed()
        .execute();

        tx.push_barrier(test_epoch(1), false);
        assert_eq!(
            executor.expect_barrier().await.epoch,
            EpochPair::new_test_epoch(test_epoch(1))
        );

        tx.send_barrier(Barrier::new_test_barrier(test_epoch(2)).with_mutation(
            Mutation::StartFragmentBackfill {
                fragment_ids: [fragment_id].into(),
            },
        ));
        assert_eq!(
            executor.expect_barrier().await.epoch,
            EpochPair::new_test_epoch(test_epoch(2))
        );

        // Drive the empty snapshot to EOF. The executor is now waiting for the barrier that
        // persists completion, and must still pass through chunks even though the threshold is 0.
        assert!(
            timeout(Duration::from_millis(200), executor.next())
                .await
                .is_err()
        );
        let transition_chunk = StreamChunk::from_pretty(
            " i i i
            + 2 1 20
            + 1 1 10",
        );
        tx.push_chunk(transition_chunk.clone());
        assert_eq!(executor.expect_chunk().await, transition_chunk);

        tx.push_barrier(test_epoch(3), false);
        assert_eq!(
            executor.expect_barrier().await.epoch,
            EpochPair::new_test_epoch(test_epoch(3))
        );

        // Starting with the next epoch, the same unsorted input is buffered and replayed in
        // locality order at the barrier.
        tx.push_chunk(StreamChunk::from_pretty(
            " i i i
            + 4 1 40
            + 3 1 30",
        ));
        tx.push_barrier(test_epoch(4), false);
        assert_eq!(
            executor.expect_chunk().await,
            StreamChunk::from_pretty(
                " i i i
                + 3 1 30
                + 4 1 40",
            )
        );
        assert_eq!(
            executor.expect_barrier().await.epoch,
            EpochPair::new_test_epoch(test_epoch(4))
        );
    }
}
