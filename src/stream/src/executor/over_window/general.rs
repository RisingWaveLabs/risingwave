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

use std::collections::{BTreeMap, HashSet, btree_map};
use std::marker::PhantomData;
use std::ops::RangeInclusive;

use delta_btree_map::Change;
use itertools::Itertools;
use risingwave_common::array::stream_record::Record;
use risingwave_common::config::streaming::OverWindowCachePolicy as CachePolicy;
use risingwave_common::row::RowExt;
use risingwave_common::types::{DefaultOrd, DefaultOrdered, ScalarImpl};
use risingwave_common::util::memcmp_encoding::{self, MemcmpEncoded};
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::window_function::{
    RangeFrameBounds, RowsFrameBounds, StateKey, WindowFuncCall,
};

use super::frame_finder::merge_rows_frames;
use super::over_partition::{OverPartition, PartitionDelta};
use super::range_cache::{CacheKey, PartitionCache};
use crate::cache::ManagedLruCache;
use crate::common::change_buffer::ChangeBuffer;
use crate::common::metrics::MetricsInfo;
use crate::consistency::consistency_panic;
use crate::executor::monitor::OverWindowMetrics;
use crate::executor::prelude::*;

/// [`OverWindowExecutor`] consumes retractable input stream and produces window function outputs.
/// One [`OverWindowExecutor`] can handle one combination of partition key and order key.
///
/// - State table schema = output schema, state table pk = `partition key | order key | input pk`.
/// - Output schema = input schema + window function results.
/// - When [`StateCleaning`] is enabled, stale rows below the watermark of the first order key
///   column are deleted from recently touched partitions at barriers.
pub struct OverWindowExecutor<S: StateStore> {
    input: Executor,
    inner: ExecutorInner<S>,
}

struct ExecutorInner<S: StateStore> {
    actor_ctx: ActorContextRef,

    schema: Schema,
    calls: Calls,
    deduped_part_key_indices: Vec<usize>,
    order_key_indices: Vec<usize>,
    order_key_data_types: Vec<DataType>,
    order_key_order_types: Vec<OrderType>,
    input_stream_key: Vec<usize>,
    state_key_to_table_sub_pk_proj: Vec<usize>,

    state_table: StateTable<S>,
    watermark_sequence: AtomicU64Ref,

    /// The maximum size of the chunk produced by executor at a time.
    chunk_size: usize,
    cache_policy: CachePolicy,
    /// Watermark-driven state cleaning strategy, `None` if disabled.
    state_cleaning: Option<StateCleaning>,
}

struct ExecutionVars<S: StateStore> {
    /// partition key => partition range cache.
    cached_partitions: ManagedLruCache<OwnedRow, PartitionCache>,
    /// partition key => recently accessed range.
    recently_accessed_ranges: BTreeMap<DefaultOrdered<OwnedRow>, RangeInclusive<StateKey>>,
    /// The latest watermark received on the watermark column for state cleaning.
    cleaning_watermark: Option<ScalarImpl>,
    /// Partitions touched since the last barrier, which need state cleaning at the next barrier.
    touched_partitions: HashSet<OwnedRow>,
    stats: ExecutionStats,
    _phantom: PhantomData<S>,
}

/// Watermark-driven state cleaning strategy of [`OverWindowExecutor`].
///
/// This is only enabled when the optimizer decides it's safe, i.e., when the input is append-only,
/// all window frames are bounded `ROWS` frames, and the first order key column is a watermark
/// column with NULLs ordered as the largest values.
///
/// Under these conditions, a row can only affect (and be affected by) a bounded number of
/// neighboring rows in the partition. Once a watermark `wm` is received, no row with order key
/// `< wm` will ever arrive, so rows with order key `< wm` (*stale rows*) can never get new
/// neighbors on the "smaller" side. Therefore, among the stale rows, only the `n_retain` ones that
/// are closest to the watermark boundary can still be involved in future computation (as frame
/// members of, or as rows affected by, rows arriving in the future), and all the others can be
/// safely deleted from the state table.
///
/// The cleaning happens at barriers for partitions touched since the last barrier. This keeps the
/// work proportional to recently active partitions and avoids keeping per-partition cleanup
/// metadata in memory. Active partitions retain only the required rows plus rows not yet behind
/// the watermark. A partition that stops receiving rows is cleaned lazily when it's touched again.
#[derive(Debug)]
pub(super) struct StateCleaning {
    /// Index of the watermark column (the first order key column) in the input schema.
    pub watermark_col_idx: usize,
    /// Whether stale rows are at the front (`ASC` order key) or at the back (`DESC` order key)
    /// of the partition.
    pub stale_rows_at_front: bool,
    /// Number of stale rows to retain in each partition, which is the number of preceding rows
    /// plus the number of following rows of the union of all `ROWS` frames.
    pub n_retain: usize,
}

#[derive(Default)]
struct ExecutionStats {
    cache_miss: u64,
    cache_lookup: u64,
}

impl<S: StateStore> Execute for OverWindowExecutor<S> {
    fn execute(self: Box<Self>) -> crate::executor::BoxedMessageStream {
        self.executor_inner().boxed()
    }
}

impl<S: StateStore> ExecutorInner<S> {
    /// Get deduplicated partition key from a full row, which happened to be the prefix of table PK.
    fn get_partition_key(&self, full_row: impl Row) -> OwnedRow {
        full_row
            .project(&self.deduped_part_key_indices)
            .into_owned_row()
    }

    fn get_input_pk(&self, full_row: impl Row) -> OwnedRow {
        full_row.project(&self.input_stream_key).into_owned_row()
    }

    /// `full_row` can be an input row or state table row.
    fn encode_order_key(&self, full_row: impl Row) -> StreamExecutorResult<MemcmpEncoded> {
        Ok(memcmp_encoding::encode_row(
            full_row.project(&self.order_key_indices),
            &self.order_key_order_types,
        )?)
    }

    fn row_to_cache_key(&self, full_row: impl Row + Copy) -> StreamExecutorResult<CacheKey> {
        Ok(CacheKey::Normal(StateKey {
            order_key: self.encode_order_key(full_row)?,
            pk: self.get_input_pk(full_row).into(),
        }))
    }
}

pub struct OverWindowExecutorArgs<S: StateStore> {
    pub actor_ctx: ActorContextRef,

    pub input: Executor,

    pub schema: Schema,
    pub calls: Vec<WindowFuncCall>,
    pub partition_key_indices: Vec<usize>,
    pub order_key_indices: Vec<usize>,
    pub order_key_order_types: Vec<OrderType>,

    pub state_table: StateTable<S>,
    pub watermark_epoch: AtomicU64Ref,
    pub metrics: Arc<StreamingMetrics>,

    pub chunk_size: usize,
    pub cache_policy: CachePolicy,
    /// Whether to enable watermark-driven state cleaning. See [`StateCleaning`].
    pub enable_state_cleaning: bool,
}

/// Information about the window function calls.
/// Contains the original calls and many other information that can be derived from the calls to avoid
/// repeated calculation.
pub(super) struct Calls {
    calls: Vec<WindowFuncCall>,

    /// The `ROWS` frame that is the union of all `ROWS` frames.
    pub(super) super_rows_frame_bounds: RowsFrameBounds,
    /// All `RANGE` frames.
    pub(super) range_frames: Vec<RangeFrameBounds>,
    pub(super) start_is_unbounded: bool,
    pub(super) end_is_unbounded: bool,
    /// Deduplicated indices of all arguments of all calls.
    pub(super) all_arg_indices: Vec<usize>,

    // TODO(rc): The following flags are used to optimize for `row_number`, `rank` and `dense_rank`.
    // We should try our best to remove these flags while maintaining the performance in the future.
    pub(super) numbering_only: bool,
    pub(super) has_rank: bool,
}

impl Calls {
    fn new(calls: Vec<WindowFuncCall>) -> Self {
        let rows_frames = calls
            .iter()
            .filter_map(|call| call.frame.bounds.as_rows())
            .collect::<Vec<_>>();
        let super_rows_frame_bounds = merge_rows_frames(&rows_frames);
        let range_frames = calls
            .iter()
            .filter_map(|call| call.frame.bounds.as_range())
            .cloned()
            .collect::<Vec<_>>();

        let start_is_unbounded = calls
            .iter()
            .any(|call| call.frame.bounds.start_is_unbounded());
        let end_is_unbounded = calls
            .iter()
            .any(|call| call.frame.bounds.end_is_unbounded());

        let all_arg_indices = calls
            .iter()
            .flat_map(|call| call.args.val_indices().iter().copied())
            .dedup()
            .collect();

        let numbering_only = calls.iter().all(|call| call.kind.is_numbering());
        let has_rank = calls.iter().any(|call| call.kind.is_rank());

        Self {
            calls,
            super_rows_frame_bounds,
            range_frames,
            start_is_unbounded,
            end_is_unbounded,
            all_arg_indices,
            numbering_only,
            has_rank,
        }
    }

    pub(super) fn iter(&self) -> impl ExactSizeIterator<Item = &WindowFuncCall> {
        self.calls.iter()
    }

    pub(super) fn len(&self) -> usize {
        self.calls.len()
    }
}

impl<S: StateStore> OverWindowExecutor<S> {
    pub fn new(args: OverWindowExecutorArgs<S>) -> Self {
        let calls = Calls::new(args.calls);

        let input_info = args.input.info().clone();
        let input_schema = &input_info.schema;

        let has_unbounded_frame = calls.start_is_unbounded || calls.end_is_unbounded;
        let cache_policy = if has_unbounded_frame {
            // For unbounded frames, we finally need all entries of the partition in the cache,
            // so for simplicity we just use full cache policy for these cases.
            CachePolicy::Full
        } else {
            args.cache_policy
        };

        let order_key_data_types = args
            .order_key_indices
            .iter()
            .map(|i| input_schema[*i].data_type())
            .collect();

        let state_key_to_table_sub_pk_proj = RowConverter::calc_state_key_to_table_sub_pk_proj(
            &args.partition_key_indices,
            &args.order_key_indices,
            &input_info.stream_key,
        );

        let deduped_part_key_indices = {
            let mut dedup = HashSet::new();
            args.partition_key_indices
                .iter()
                .filter(|i| dedup.insert(**i))
                .copied()
                .collect()
        };

        let state_cleaning = if args.enable_state_cleaning {
            let all_frames_bounded_rows = calls
                .iter()
                .all(|call| call.frame.bounds.is_rows() && !call.frame.bounds.is_unbounded());
            let bounds = &calls.super_rows_frame_bounds;
            match (bounds.n_preceding_rows(), bounds.n_following_rows()) {
                (Some(n_preceding), Some(n_following))
                    if all_frames_bounded_rows && !args.order_key_indices.is_empty() =>
                {
                    Some(StateCleaning {
                        watermark_col_idx: args.order_key_indices[0],
                        stale_rows_at_front: args.order_key_order_types[0].is_ascending(),
                        n_retain: n_preceding.saturating_add(n_following),
                    })
                }
                _ => {
                    // The optimizer should never enable state cleaning in this case.
                    tracing::warn!(
                        "state cleaning is enabled for over window with unbounded or non-`ROWS` frames, ignoring"
                    );
                    None
                }
            }
        } else {
            None
        };

        Self {
            input: args.input,
            inner: ExecutorInner {
                actor_ctx: args.actor_ctx,
                schema: args.schema,
                calls,
                deduped_part_key_indices,
                order_key_indices: args.order_key_indices,
                order_key_data_types,
                order_key_order_types: args.order_key_order_types,
                input_stream_key: input_info.stream_key,
                state_key_to_table_sub_pk_proj,
                state_table: args.state_table,
                watermark_sequence: args.watermark_epoch,
                chunk_size: args.chunk_size,
                cache_policy,
                state_cleaning,
            },
        }
    }

    /// Merge changes by input pk in the given chunk, return a change iterator which guarantees that
    /// each pk only appears once. This method also validates the consistency of the input
    /// chunk.
    ///
    /// TODO(rc): We may want to optimize this by handling changes on the same pk during generating
    /// partition [`Change`]s.
    fn merge_changes_in_chunk<'a>(
        this: &'_ ExecutorInner<S>,
        chunk: &'a StreamChunk,
    ) -> impl Iterator<Item = Record<RowRef<'a>>> {
        let mut cb = ChangeBuffer::with_capacity(chunk.cardinality());
        for record in chunk.records() {
            cb.apply_record(record, |row| this.get_input_pk(row));
        }
        cb.into_records()
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn apply_chunk<'a>(
        this: &'a mut ExecutorInner<S>,
        vars: &'a mut ExecutionVars<S>,
        chunk: StreamChunk,
        metrics: &'a OverWindowMetrics,
    ) {
        // (deduped) partition key => (
        //   significant changes happened in the partition,
        //   no-effect changes happened in the partition,
        // )
        let mut deltas: BTreeMap<DefaultOrdered<OwnedRow>, (PartitionDelta, PartitionDelta)> =
            BTreeMap::new();
        // input pk of update records of which the order key is changed.
        let mut key_change_updated_pks = HashSet::new();

        // Collect changes for each partition.
        for record in Self::merge_changes_in_chunk(this, &chunk) {
            match record {
                Record::Insert { new_row } => {
                    let part_key = this.get_partition_key(new_row).into();
                    let (delta, _) = deltas.entry(part_key).or_default();
                    delta.insert(
                        this.row_to_cache_key(new_row)?,
                        Change::Insert(new_row.into_owned_row()),
                    );
                }
                Record::Delete { old_row } => {
                    let part_key = this.get_partition_key(old_row).into();
                    let (delta, _) = deltas.entry(part_key).or_default();
                    delta.insert(this.row_to_cache_key(old_row)?, Change::Delete);
                }
                Record::Update { old_row, new_row } => {
                    let old_part_key = this.get_partition_key(old_row).into();
                    let new_part_key = this.get_partition_key(new_row).into();
                    let old_state_key = this.row_to_cache_key(old_row)?;
                    let new_state_key = this.row_to_cache_key(new_row)?;
                    if old_part_key == new_part_key && old_state_key == new_state_key {
                        // not a key-change update
                        let (delta, no_effect_delta) = deltas.entry(old_part_key).or_default();
                        if old_row.project(&this.calls.all_arg_indices)
                            == new_row.project(&this.calls.all_arg_indices)
                        {
                            // partition key, order key and arguments are all the same
                            no_effect_delta
                                .insert(old_state_key, Change::Insert(new_row.into_owned_row()));
                        } else {
                            delta.insert(old_state_key, Change::Insert(new_row.into_owned_row()));
                        }
                    } else if old_part_key == new_part_key {
                        // order-change update, split into delete + insert, will be merged after
                        // building changes
                        key_change_updated_pks.insert(this.get_input_pk(old_row));
                        let (delta, _) = deltas.entry(old_part_key).or_default();
                        delta.insert(old_state_key, Change::Delete);
                        delta.insert(new_state_key, Change::Insert(new_row.into_owned_row()));
                    } else {
                        // partition-change update, split into delete + insert
                        // NOTE(rc): Since we append partition key to logical pk, we can't merge the
                        // delete + insert back to update later.
                        // TODO: IMO this behavior is problematic. Deep discussion is needed.
                        let (old_part_delta, _) = deltas.entry(old_part_key).or_default();
                        old_part_delta.insert(old_state_key, Change::Delete);
                        let (new_part_delta, _) = deltas.entry(new_part_key).or_default();
                        new_part_delta
                            .insert(new_state_key, Change::Insert(new_row.into_owned_row()));
                    }
                }
            }
        }

        // `input pk` => `Record`
        let mut key_change_update_buffer: BTreeMap<DefaultOrdered<OwnedRow>, Record<OwnedRow>> =
            BTreeMap::new();
        let mut chunk_builder = StreamChunkBuilder::new(this.chunk_size, this.schema.data_types());

        // Build final changes partition by partition.
        for (part_key, (delta, no_effect_delta)) in deltas {
            vars.stats.cache_lookup += 1;
            if !vars.cached_partitions.contains(&part_key.0) {
                vars.stats.cache_miss += 1;
                vars.cached_partitions
                    .put(part_key.0.clone(), PartitionCache::new());
            }
            let mut cache = vars.cached_partitions.get_mut(&part_key).unwrap();

            // First, handle `Update`s that don't affect window function outputs.
            // Be careful that changes in `delta` may (though we believe unlikely) affect the
            // window function outputs of rows in `no_effect_delta`, so before handling `delta`
            // we need to write all changes to state table, range cache and chunk builder.
            for (key, change) in no_effect_delta {
                let new_row = change.into_insert().unwrap(); // new row of an `Update`

                let (old_row, from_cache) = if let Some(old_row) = cache.inner().get(&key).cloned()
                {
                    // Got old row from range cache.
                    (old_row, true)
                } else {
                    // Retrieve old row from state table.
                    let table_pk = (&new_row).project(this.state_table.pk_indices());
                    // The accesses to the state table is ordered by table PK, so ideally we
                    // can leverage the block cache under the hood.
                    if let Some(old_row) = this.state_table.get_row(table_pk).await? {
                        (old_row, false)
                    } else {
                        consistency_panic!(?part_key, ?key, ?new_row, "updating non-existing row");
                        continue;
                    }
                };

                // concatenate old outputs
                let input_len = new_row.len();
                let new_row = OwnedRow::new(
                    new_row
                        .into_iter()
                        .chain(old_row.as_inner().iter().skip(input_len).cloned()) // chain old outputs
                        .collect(),
                );

                // apply & emit the change
                let record = Record::Update {
                    old_row: &old_row,
                    new_row: &new_row,
                };
                if let Some(chunk) = chunk_builder.append_record(record.as_ref()) {
                    yield chunk;
                }
                this.state_table.write_record(record);
                if from_cache {
                    cache.insert(key, new_row);
                }
            }

            let mut partition = OverPartition::new(
                &part_key,
                &mut cache,
                this.cache_policy,
                &this.calls,
                RowConverter {
                    state_key_to_table_sub_pk_proj: &this.state_key_to_table_sub_pk_proj,
                    order_key_indices: &this.order_key_indices,
                    order_key_data_types: &this.order_key_data_types,
                    order_key_order_types: &this.order_key_order_types,
                    input_stream_key_indices: &this.input_stream_key,
                },
            );

            if delta.is_empty() {
                continue;
            }

            if this.state_cleaning.is_some() {
                vars.touched_partitions.insert(part_key.0.clone());
            }

            // Build changes for current partition.
            let (part_changes, accessed_range) =
                partition.build_changes(&this.state_table, delta).await?;

            for (key, record) in part_changes {
                // Build chunk and yield if needed.
                if !key_change_updated_pks.contains(&key.pk) {
                    if let Some(chunk) = chunk_builder.append_record(record.as_ref()) {
                        yield chunk;
                    }
                } else {
                    // For key-change updates, we should wait for both `Delete` and `Insert` changes
                    // and merge them together.
                    let pk = key.pk.clone();
                    let record = record.clone();
                    if let Some(existed) = key_change_update_buffer.remove(&key.pk) {
                        match (existed, record) {
                            (Record::Insert { new_row }, Record::Delete { old_row })
                            | (Record::Delete { old_row }, Record::Insert { new_row }) => {
                                // merge `Delete` and `Insert` into `Update`
                                if let Some(chunk) =
                                    chunk_builder.append_record(Record::Update { old_row, new_row })
                                {
                                    yield chunk;
                                }
                            }
                            (existed, record) => {
                                // when stream is inconsistent, there may be an `Update` of which the old pk does not actually exist
                                consistency_panic!(
                                    ?existed,
                                    ?record,
                                    "other cases should not exist",
                                );

                                key_change_update_buffer.insert(pk, record);
                                if let Some(chunk) = chunk_builder.append_record(existed) {
                                    yield chunk;
                                }
                            }
                        }
                    } else {
                        key_change_update_buffer.insert(pk, record);
                    }
                }

                // Apply the change record.
                partition.write_record(&mut this.state_table, key, record);
            }

            if !key_change_update_buffer.is_empty() {
                consistency_panic!(
                    ?key_change_update_buffer,
                    "key-change update buffer should be empty after processing"
                );
                // if in non-strict mode, we can reach here, but we don't know the `StateKey`,
                // so just ignore the buffer.
            }

            let cache_len = partition.cache_real_len();
            let stats = partition.summarize();
            metrics
                .over_window_range_cache_entry_count
                .set(cache_len as i64);
            metrics
                .over_window_range_cache_lookup_count
                .inc_by(stats.lookup_count);
            metrics
                .over_window_range_cache_left_miss_count
                .inc_by(stats.left_miss_count);
            metrics
                .over_window_range_cache_right_miss_count
                .inc_by(stats.right_miss_count);
            metrics
                .over_window_accessed_entry_count
                .inc_by(stats.accessed_entry_count);
            metrics
                .over_window_compute_count
                .inc_by(stats.compute_count);
            metrics
                .over_window_same_output_count
                .inc_by(stats.same_output_count);

            // Update recently accessed range for later shrinking cache.
            if !this.cache_policy.is_full()
                && let Some(accessed_range) = accessed_range
            {
                match vars.recently_accessed_ranges.entry(part_key) {
                    btree_map::Entry::Vacant(vacant) => {
                        vacant.insert(accessed_range);
                    }
                    btree_map::Entry::Occupied(mut occupied) => {
                        let recently_accessed_range = occupied.get_mut();
                        let min_start = accessed_range
                            .start()
                            .min(recently_accessed_range.start())
                            .clone();
                        let max_end = accessed_range
                            .end()
                            .max(recently_accessed_range.end())
                            .clone();
                        *recently_accessed_range = min_start..=max_end;
                    }
                }
            }
        }

        // Yield remaining changes to downstream.
        if let Some(chunk) = chunk_builder.take() {
            yield chunk;
        }
    }

    /// Clean up stale rows of the partitions touched since the last barrier, according to the
    /// latest watermark received.
    /// Returns the number of rows deleted. See [`StateCleaning`].
    async fn clean_state(
        this: &mut ExecutorInner<S>,
        vars: &mut ExecutionVars<S>,
    ) -> StreamExecutorResult<usize> {
        let touched = std::mem::take(&mut vars.touched_partitions);
        let (Some(cleaning), Some(watermark)) = (&this.state_cleaning, &vars.cleaning_watermark)
        else {
            return Ok(0);
        };
        if touched.is_empty() {
            return Ok(0);
        }

        let row_conv = RowConverter {
            state_key_to_table_sub_pk_proj: &this.state_key_to_table_sub_pk_proj,
            order_key_indices: &this.order_key_indices,
            order_key_data_types: &this.order_key_data_types,
            order_key_order_types: &this.order_key_order_types,
            input_stream_key_indices: &this.input_stream_key,
        };

        let mut n_deleted = 0;
        for part_key in touched {
            let mut cache_guard = vars.cached_partitions.get_mut(&part_key);
            // If the partition is not cached (e.g. evicted), use a temporary cache with only
            // sentinels, so that `OverPartition` scans the state table for stale rows.
            let mut temp_cache = PartitionCache::new();
            let cache = match cache_guard.as_deref_mut() {
                Some(cache) => cache,
                None => &mut temp_cache,
            };
            let mut partition =
                OverPartition::new(&part_key, cache, this.cache_policy, &this.calls, row_conv);
            let (n, has_more) = partition
                .clean_stale_rows(&mut this.state_table, cleaning, watermark)
                .await?;
            n_deleted += n;
            if has_more {
                // continue to clean this partition at the next barrier
                vars.touched_partitions.insert(part_key.clone());
            }
        }
        Ok(n_deleted)
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn executor_inner(self) {
        let OverWindowExecutor {
            input,
            inner: mut this,
        } = self;

        let metrics_info = MetricsInfo::new(
            this.actor_ctx.streaming_metrics.clone(),
            this.state_table.table_id(),
            this.actor_ctx.id,
            "OverWindow",
        );

        let metrics = metrics_info.metrics.new_over_window_metrics(
            this.state_table.table_id(),
            this.actor_ctx.id,
            this.actor_ctx.fragment_id,
        );

        let mut vars = ExecutionVars {
            cached_partitions: ManagedLruCache::unbounded(
                this.watermark_sequence.clone(),
                metrics_info,
            ),
            recently_accessed_ranges: Default::default(),
            cleaning_watermark: None,
            touched_partitions: Default::default(),
            stats: Default::default(),
            _phantom: PhantomData::<S>,
        };

        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        this.state_table.init_epoch(first_epoch).await?;

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Watermark(watermark) => {
                    if let Some(cleaning) = &this.state_cleaning
                        && watermark.col_idx == cleaning.watermark_col_idx
                        && vars
                            .cleaning_watermark
                            .as_ref()
                            .is_none_or(|old| old.default_cmp(&watermark.val).is_lt())
                    {
                        // Only used for state cleaning at the next barrier.
                        vars.cleaning_watermark = Some(watermark.val);
                    }
                    // TODO(rc): We don't propagate watermarks to downstream for now, because rows
                    // below the watermark may still be updated by later rows if there's any
                    // following frame bound, e.g. `lead`. We need to think about it carefully.
                    continue;
                }
                Message::Chunk(chunk) => {
                    #[for_await]
                    for chunk in Self::apply_chunk(&mut this, &mut vars, chunk, &metrics) {
                        yield Message::Chunk(chunk?);
                    }
                    this.state_table.try_flush().await?;

                    // Also apply the LRU watermark at chunk boundaries, so that cold
                    // partitions can be released without waiting for the next barrier,
                    // which can be a long time away with large barrier intervals. This
                    // is safe because the range cache is write-through: at this point
                    // all changes have been applied to both the state table and the
                    // cache, so an evicted partition can be reloaded from the state
                    // table with identical content.
                    vars.cached_partitions.evict();
                }
                Message::Barrier(barrier) => {
                    let n_cleaned = Self::clean_state(&mut this, &mut vars).await?;
                    metrics
                        .over_window_state_cleaned_row_count
                        .inc_by(n_cleaned as u64);

                    let post_commit = this.state_table.commit(barrier.epoch).await?;

                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(this.actor_ctx.id);
                    yield Message::Barrier(barrier);

                    vars.cached_partitions.evict();

                    metrics
                        .over_window_cached_entry_count
                        .set(vars.cached_partitions.len() as _);
                    metrics
                        .over_window_cache_lookup_count
                        .inc_by(std::mem::take(&mut vars.stats.cache_lookup));
                    metrics
                        .over_window_cache_miss_count
                        .inc_by(std::mem::take(&mut vars.stats.cache_miss));

                    if let Some((_, cache_may_stale)) =
                        post_commit.post_yield_barrier(update_vnode_bitmap).await?
                        && cache_may_stale
                    {
                        vars.cached_partitions.clear();
                        vars.recently_accessed_ranges.clear();
                        vars.touched_partitions.clear();
                    }

                    if !this.cache_policy.is_full() {
                        for (part_key, recently_accessed_range) in
                            std::mem::take(&mut vars.recently_accessed_ranges)
                        {
                            if let Some(mut range_cache) =
                                vars.cached_partitions.get_mut(&part_key.0)
                            {
                                range_cache.shrink(
                                    &part_key.0,
                                    this.cache_policy,
                                    recently_accessed_range,
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

/// A converter that helps convert [`StateKey`] to state table sub-PK and convert executor input/output
/// row to [`StateKey`].
///
/// ## Notes
///
/// - [`StateKey`]: Over window range cache key type, containing order key and input pk.
/// - State table sub-PK: State table PK = PK prefix (partition key) + sub-PK (order key + input pk).
/// - Input/output row: Input schema is the prefix of output schema.
///
/// You can see that the content of [`StateKey`] is very similar to state table sub-PK. There's only
/// one difference: the state table PK and sub-PK don't have duplicated columns, while in [`StateKey`],
/// `order_key` and (input)`pk` may contain duplicated columns.
#[derive(Debug, Clone, Copy)]
pub(super) struct RowConverter<'a> {
    state_key_to_table_sub_pk_proj: &'a [usize],
    order_key_indices: &'a [usize],
    order_key_data_types: &'a [DataType],
    order_key_order_types: &'a [OrderType],
    input_stream_key_indices: &'a [usize],
}

impl<'a> RowConverter<'a> {
    /// Calculate the indices needed for projection from [`StateKey`] to state table sub-PK (used to do
    /// prefixed table scanning). Ideally this function should be called only once by each executor instance.
    /// The projection indices vec is the *selected column indices* in [`StateKey`].`order_key.chain(input_pk)`.
    pub(super) fn calc_state_key_to_table_sub_pk_proj(
        partition_key_indices: &[usize],
        order_key_indices: &[usize],
        input_stream_key_indices: &'a [usize],
    ) -> Vec<usize> {
        // This process is corresponding to `StreamOverWindow::infer_state_table`.
        let mut projection =
            Vec::with_capacity(order_key_indices.len() + input_stream_key_indices.len());
        let mut col_dedup: HashSet<usize> = partition_key_indices.iter().copied().collect();
        for (proj_idx, key_idx) in order_key_indices
            .iter()
            .chain(input_stream_key_indices.iter())
            .enumerate()
        {
            if col_dedup.insert(*key_idx) {
                projection.push(proj_idx);
            }
        }
        projection.shrink_to_fit();
        projection
    }

    /// Convert [`StateKey`] to sub-PK (table PK without partition key) as [`OwnedRow`].
    pub(super) fn state_key_to_table_sub_pk(
        &self,
        key: &StateKey,
    ) -> StreamExecutorResult<OwnedRow> {
        Ok(memcmp_encoding::decode_row(
            &key.order_key,
            self.order_key_data_types,
            self.order_key_order_types,
        )?
        .chain(key.pk.as_inner())
        .project(self.state_key_to_table_sub_pk_proj)
        .into_owned_row())
    }

    /// Convert full input/output row to [`StateKey`].
    pub(super) fn row_to_state_key(
        &self,
        full_row: impl Row + Copy,
    ) -> StreamExecutorResult<StateKey> {
        Ok(StateKey {
            order_key: memcmp_encoding::encode_row(
                full_row.project(self.order_key_indices),
                self.order_key_order_types,
            )?,
            pk: full_row
                .project(self.input_stream_key_indices)
                .into_owned_row()
                .into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use futures::TryStreamExt;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::util::epoch::{EpochPair, test_epoch};
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::store::PrefetchOptions;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable;

    #[tokio::test]
    async fn test_state_cleaning_large_retention() {
        for order_type in [OrderType::ascending(), OrderType::descending()] {
            for cached in [true, false] {
                // Both are valid sums of two Int64 ROWS offsets on 64-bit targets.
                // The first makes the old collection limit wrap to 1, so even three
                // rows catch the erroneous deletion with overflow checks disabled.
                for n_retain in [usize::MAX - 65_534, usize::MAX - 1] {
                    let mut table = StateTable::from_table_catalog(
                        &gen_pbtable(
                            TableId::new(1),
                            vec![ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64)],
                            vec![order_type],
                            vec![0],
                            0,
                        ),
                        MemoryStateStore::new(),
                        None,
                    )
                    .await;
                    table
                        .init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
                        .await
                        .unwrap();
                    let rows = [10i64, 20, 30].map(|value| OwnedRow::new(vec![Some(value.into())]));
                    let row_conv = RowConverter {
                        state_key_to_table_sub_pk_proj: &[0],
                        order_key_indices: &[0],
                        order_key_data_types: &[DataType::Int64],
                        order_key_order_types: &[order_type],
                        input_stream_key_indices: &[0],
                    };
                    let mut cache = if cached {
                        PartitionCache::new_without_sentinels()
                    } else {
                        PartitionCache::new()
                    };
                    for row in &rows {
                        table.insert(row.clone());
                        if cached {
                            cache.insert(
                                CacheKey::from(row_conv.row_to_state_key(row).unwrap()),
                                row.clone(),
                            );
                        }
                    }
                    table
                        .commit_for_test(EpochPair::new_test_epoch(test_epoch(2)))
                        .await
                        .unwrap();

                    let partition_key = OwnedRow::empty();
                    let calls = Calls::new(vec![]);
                    let mut partition = OverPartition::new(
                        &partition_key,
                        &mut cache,
                        CachePolicy::Full,
                        &calls,
                        row_conv,
                    );
                    let cleaning = StateCleaning {
                        watermark_col_idx: 0,
                        stale_rows_at_front: order_type.is_ascending(),
                        n_retain,
                    };
                    assert_eq!(
                        partition
                            .clean_stale_rows(&mut table, &cleaning, &100i64.into())
                            .await
                            .unwrap(),
                        (0, false),
                        "order={order_type:?}, cached={cached}, n_retain={n_retain}",
                    );
                    table
                        .commit_for_test(EpochPair::new_test_epoch(test_epoch(3)))
                        .await
                        .unwrap();
                    let range: (Bound<OwnedRow>, Bound<OwnedRow>) =
                        (Bound::Unbounded, Bound::Unbounded);
                    let remaining: Vec<OwnedRow> = table
                        .iter_with_prefix(&partition_key, &range, PrefetchOptions::default())
                        .await
                        .unwrap()
                        .try_collect()
                        .await
                        .unwrap();
                    let mut expected = rows.to_vec();
                    if !order_type.is_ascending() {
                        expected.reverse();
                    }
                    assert_eq!(remaining, expected);
                    assert_eq!(cache.normal_len(), if cached { rows.len() } else { 0 });
                }
            }
        }
    }
}
