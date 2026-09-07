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

use std::collections::HashMap;
use std::marker::PhantomData;

use futures::future::try_join_all;
use futures::stream;
use itertools::Itertools;
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
use risingwave_common::hash::{HashKey, PrecomputedBuildHasher};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_common_estimate_size::collections::EstimatedHashMap;
use risingwave_expr::aggregate::{AggCall, BoxedAggregateFunction, build_retractable};
use risingwave_pb::stream_plan::PbAggNodeVersion;

use super::agg_group::{
    AggGroup as GenericAggGroup, AggStateCacheStats, GroupKey, OnlyOutputIfHasInput,
};
use super::agg_state::AggStateStorage;
use super::distinct::DistinctDeduplicater;
use super::{AggExecutorArgs, HashAggExecutorExtraArgs, agg_call_filter_res, iter_table_storage};
use crate::cache::ManagedLruCache;
use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::StateTablePostCommit;
use crate::executor::eowc::SortBuffer;
use crate::executor::monitor::HashAggMetrics;
use crate::executor::prelude::*;

type AggGroup<S> = GenericAggGroup<S, OnlyOutputIfHasInput>;
type BoxedAggGroup<S> = Box<AggGroup<S>>;

impl<S: StateStore> EstimateSize for BoxedAggGroup<S> {
    fn estimated_heap_size(&self) -> usize {
        self.as_ref().estimated_size()
    }
}

type AggGroupCache<K, S> = ManagedLruCache<K, Option<BoxedAggGroup<S>>, PrecomputedBuildHasher>;

/// [`HashAggExecutor`] could process large amounts of data using a state backend. It works as
/// follows:
///
/// * The executor pulls data from the upstream, and apply the data chunks to the corresponding
///   aggregation states.
/// * While processing, it will record which keys have been modified in this epoch using
///   `group_change_set`.
/// * Upon a barrier is received, the executor will call `.flush` on the storage backend, so that
///   all modifications will be flushed to the storage backend. Meanwhile, the executor will go
///   through `group_change_set`, and produce a stream chunk based on the state changes.
pub struct HashAggExecutor<K: HashKey, S: StateStore> {
    input: Executor,
    inner: ExecutorInner<K, S>,
}

struct ExecutorInner<K: HashKey, S: StateStore> {
    _phantom: PhantomData<K>,

    /// Version of aggregation executors.
    version: PbAggNodeVersion,

    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Stream key indices from input. Only used by `AggNodeVersion` before `ISSUE_13465`.
    input_stream_key: Vec<usize>,

    /// Schema from input.
    input_schema: Schema,

    /// Indices of the columns
    /// all of the aggregation functions in this executor should depend on same group of keys
    group_key_indices: Vec<usize>,

    // The projection from group key in table schema to table pk.
    group_key_table_pk_projection: Arc<[usize]>,

    /// A [`HashAggExecutor`] may have multiple [`AggCall`]s.
    agg_calls: Vec<AggCall>,

    /// Aggregate functions.
    agg_funcs: Vec<BoxedAggregateFunction>,

    /// Index of row count agg call (`count(*)`) in the call list.
    row_count_index: usize,

    /// State storages for each aggregation calls.
    /// `None` means the agg call need not to maintain a state table by itself.
    storages: Vec<AggStateStorage<S>>,

    /// Intermediate state table for value-state agg calls.
    /// The state of all value-state aggregates are collected and stored in this
    /// table when `flush_data` is called.
    /// Also serves as EOWC sort buffer table.
    intermediate_state_table: StateTable<S>,

    /// State tables for deduplicating rows on distinct key for distinct agg calls.
    /// One table per distinct column (may be shared by multiple agg calls).
    distinct_dedup_tables: HashMap<usize, StateTable<S>>,

    /// Watermark epoch.
    watermark_sequence: AtomicU64Ref,

    /// State cache size for extreme agg.
    extreme_cache_size: usize,

    /// The maximum size of the chunk produced by executor at a time.
    chunk_size: usize,

    /// The maximum heap size of dirty groups. If exceeds, the executor should flush dirty groups.
    max_dirty_groups_heap_size: usize,

    /// Should emit on window close according to watermark?
    emit_on_window_close: bool,
}

impl<K: HashKey, S: StateStore> ExecutorInner<K, S> {
    fn all_state_tables_mut(&mut self) -> impl Iterator<Item = &mut StateTable<S>> {
        iter_table_storage(&mut self.storages)
            .chain(self.distinct_dedup_tables.values_mut())
            .chain(std::iter::once(&mut self.intermediate_state_table))
    }
}

struct ExecutionVars<K: HashKey, S: StateStore> {
    metrics: HashAggMetrics,

    // Stats collected during execution, will be flushed to metrics at the end of each barrier.
    stats: ExecutionStats,

    /// Cache for [`AggGroup`]s. `HashKey` -> `AggGroup`.
    agg_group_cache: AggGroupCache<K, S>,

    /// Changed [`AggGroup`]s in the current epoch (before next flush).
    dirty_groups: EstimatedHashMap<K, BoxedAggGroup<S>>,

    /// Distinct deduplicater to deduplicate input rows for each distinct agg call.
    distinct_dedup: DistinctDeduplicater<S>,

    /// Buffer watermarks on group keys received since last barrier.
    buffered_watermarks: Vec<Option<Watermark>>,

    /// Latest watermark on window column.
    window_watermark: Option<ScalarImpl>,

    /// Stream chunk builder.
    chunk_builder: StreamChunkBuilder,

    buffer: SortBuffer<S>,
}

#[derive(Debug, Default)]
struct ExecutionStats {
    /// Agg group cache (LRU) stats.
    lookup_miss_count: u64,
    total_lookup_count: u64,

    /// Agg group cache stats in chunk-granularity.
    chunk_lookup_miss_count: u64,
    chunk_total_lookup_count: u64,

    /// Agg state cache stats.
    agg_state_cache_lookup_count: u64,
    agg_state_cache_miss_count: u64,
}

impl ExecutionStats {
    fn merge_state_cache_stats(&mut self, other: AggStateCacheStats) {
        self.agg_state_cache_lookup_count += other.agg_state_cache_lookup_count;
        self.agg_state_cache_miss_count += other.agg_state_cache_miss_count;
    }
}

impl<K: HashKey, S: StateStore> Execute for HashAggExecutor<K, S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl<K: HashKey, S: StateStore> HashAggExecutor<K, S> {
    pub fn new(args: AggExecutorArgs<S, HashAggExecutorExtraArgs>) -> StreamResult<Self> {
        let input_info = args.input.info().clone();

        let group_key_len = args.extra.group_key_indices.len();
        // NOTE: we assume the prefix of table pk is exactly the group key
        let group_key_table_pk_projection =
            &args.intermediate_state_table.pk_indices()[..group_key_len];
        assert!(
            group_key_table_pk_projection
                .iter()
                .sorted()
                .copied()
                .eq(0..group_key_len)
        );

        Ok(Self {
            input: args.input,
            inner: ExecutorInner {
                _phantom: PhantomData,
                version: args.version,
                actor_ctx: args.actor_ctx,
                info: args.info,
                input_stream_key: input_info.stream_key,
                input_schema: input_info.schema,
                group_key_indices: args.extra.group_key_indices,
                group_key_table_pk_projection: group_key_table_pk_projection.to_vec().into(),
                agg_funcs: args.agg_calls.iter().map(build_retractable).try_collect()?,
                agg_calls: args.agg_calls,
                row_count_index: args.row_count_index,
                storages: args.storages,
                intermediate_state_table: args.intermediate_state_table,
                distinct_dedup_tables: args.distinct_dedup_tables,
                watermark_sequence: args.watermark_epoch,
                extreme_cache_size: args.extreme_cache_size,
                chunk_size: args.extra.chunk_size,
                max_dirty_groups_heap_size: args.extra.max_dirty_groups_heap_size,
                emit_on_window_close: args.extra.emit_on_window_close,
            },
        })
    }

    /// Get visibilities that mask rows in the chunk for each group. The returned visibility
    /// is a `Bitmap` rather than `Option<Bitmap>` because it's likely to have multiple groups
    /// in one chunk.
    ///
    /// * `keys`: Hash Keys of rows.
    /// * `base_visibility`: Visibility of rows.
    fn get_group_visibilities(keys: Vec<K>, base_visibility: &Bitmap) -> Vec<(K, Bitmap)> {
        let n_rows = keys.len();
        let mut vis_builders = HashMap::new();
        for (row_idx, key) in keys
            .into_iter()
            .enumerate()
            .filter(|(row_idx, _)| base_visibility.is_set(*row_idx))
        {
            vis_builders
                .entry(key)
                .or_insert_with(|| BitmapBuilder::zeroed(n_rows))
                .set(row_idx, true);
        }
        vis_builders
            .into_iter()
            .map(|(key, vis_builder)| (key, vis_builder.finish()))
            .collect()
    }

    /// Touch the [`AggGroup`]s for the given keys, which means move them from cache to the `dirty_groups` map.
    /// If the [`AggGroup`] doesn't exist in the cache before, it will be created or recovered from state table.
    async fn touch_agg_groups(
        this: &ExecutorInner<K, S>,
        vars: &mut ExecutionVars<K, S>,
        keys: impl IntoIterator<Item = &K>,
    ) -> StreamExecutorResult<()> {
        let group_key_types = &this.info.schema.data_types()[..this.group_key_indices.len()];
        let futs = keys
            .into_iter()
            .filter_map(|key| {
                vars.stats.total_lookup_count += 1;
                if vars.dirty_groups.contains_key(key) {
                    // already dirty
                    return None;
                }
                match vars.agg_group_cache.get_mut(key) {
                    Some(mut agg_group) => {
                        let agg_group: &mut Option<_> = &mut agg_group;
                        assert!(
                            agg_group.is_some(),
                            "invalid state: AggGroup is None in cache but not dirty"
                        );
                        // move from cache to `dirty_groups`
                        vars.dirty_groups
                            .insert(key.clone(), agg_group.take().unwrap());
                        None // no need to create
                    }
                    None => {
                        vars.stats.lookup_miss_count += 1;
                        Some(async {
                            // Create `AggGroup` for the current group if not exists. This will
                            // restore agg states from the intermediate state table.
                            let (agg_group, stats) = AggGroup::create(
                                this.version,
                                Some(GroupKey::new(
                                    key.deserialize(group_key_types)?,
                                    Some(this.group_key_table_pk_projection.clone()),
                                )),
                                &this.agg_calls,
                                &this.agg_funcs,
                                &this.storages,
                                &this.intermediate_state_table,
                                &this.input_stream_key,
                                this.row_count_index,
                                this.emit_on_window_close,
                                this.extreme_cache_size,
                                &this.input_schema,
                            )
                            .await?;
                            Ok::<_, StreamExecutorError>((key.clone(), Box::new(agg_group), stats))
                        })
                    }
                }
            })
            .collect_vec(); // collect is necessary to avoid lifetime issue of `agg_group_cache`

        vars.stats.chunk_total_lookup_count += 1;
        if !futs.is_empty() {
            // If not all the required states/keys are in the cache, it's a chunk-level cache miss.
            vars.stats.chunk_lookup_miss_count += 1;
            let mut buffered = stream::iter(futs).buffer_unordered(10).fuse();
            while let Some(result) = buffered.next().await {
                let (key, agg_group, stats) = result?;
                vars.stats.merge_state_cache_stats(stats);
                let none = vars.dirty_groups.insert(key, agg_group);
                debug_assert!(none.is_none());
            }
        }
        Ok(())
    }

    async fn apply_chunk(
        this: &mut ExecutorInner<K, S>,
        vars: &mut ExecutionVars<K, S>,
        chunk: StreamChunk,
    ) -> StreamExecutorResult<()> {
        // Find groups in this chunk and generate visibility for each group key.
        let keys = K::build_many(&this.group_key_indices, chunk.data_chunk());
        let group_visibilities = Self::get_group_visibilities(keys, chunk.visibility());

        // Ensure all `AggGroup`s are in `dirty_groups`.
        Self::touch_agg_groups(this, vars, group_visibilities.iter().map(|(k, _)| k)).await?;

        // Calculate the row visibility for every agg call.
        let mut call_visibilities = Vec::with_capacity(this.agg_calls.len());
        for agg_call in &this.agg_calls {
            let agg_call_filter_res = agg_call_filter_res(agg_call, &chunk).await?;
            call_visibilities.push(agg_call_filter_res);
        }

        // Materialize input chunk if needed and possible.
        // For aggregations without distinct, we can materialize before grouping.
        for ((call, storage), visibility) in (this.agg_calls.iter())
            .zip_eq_fast(&mut this.storages)
            .zip_eq_fast(call_visibilities.iter())
        {
            if let AggStateStorage::MaterializedInput { table, mapping, .. } = storage
                && !call.distinct
            {
                let chunk = chunk.project_with_vis(mapping.upstream_columns(), visibility.clone());
                table.write_chunk(chunk);
            }
        }

        // Apply chunk to each of the state (per agg_call), for each group.
        for (key, visibility) in group_visibilities {
            let agg_group: &mut BoxedAggGroup<_> = &mut vars.dirty_groups.get_mut(&key).unwrap();

            let visibilities = call_visibilities
                .iter()
                .map(|call_vis| call_vis & &visibility)
                .collect();
            let visibilities = vars
                .distinct_dedup
                .dedup_chunk(
                    chunk.ops(),
                    chunk.columns(),
                    visibilities,
                    &mut this.distinct_dedup_tables,
                    agg_group.group_key(),
                )
                .await?;
            for ((call, storage), visibility) in (this.agg_calls.iter())
                .zip_eq_fast(&mut this.storages)
                .zip_eq_fast(visibilities.iter())
            {
                if let AggStateStorage::MaterializedInput { table, mapping, .. } = storage
                    && call.distinct
                {
                    let chunk =
                        chunk.project_with_vis(mapping.upstream_columns(), visibility.clone());
                    table.write_chunk(chunk);
                }
            }
            agg_group
                .apply_chunk(&chunk, &this.agg_calls, &this.agg_funcs, visibilities)
                .await?;
        }

        // Update the metrics.
        vars.metrics
            .agg_dirty_groups_count
            .set(vars.dirty_groups.len() as i64);
        vars.metrics
            .agg_dirty_groups_heap_size
            .set(vars.dirty_groups.estimated_heap_size() as i64);

        Ok(())
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn flush_data<'a>(this: &'a mut ExecutorInner<K, S>, vars: &'a mut ExecutionVars<K, S>) {
        let window_watermark = vars.window_watermark.take();

        // flush changed states into intermediate state table
        for mut agg_group in vars.dirty_groups.values_mut() {
            let Some(inter_states_change) = agg_group.build_states_change(&this.agg_funcs)? else {
                continue;
            };

            if this.emit_on_window_close {
                vars.buffer
                    .apply_change(inter_states_change, &mut this.intermediate_state_table);
            } else {
                this.intermediate_state_table
                    .write_record(inter_states_change);
            }
        }

        if this.emit_on_window_close {
            // remove all groups under watermark and emit their results
            if let Some(watermark) = window_watermark.as_ref() {
                #[for_await]
                for row in vars
                    .buffer
                    .consume(watermark.clone(), &mut this.intermediate_state_table)
                {
                    let row = row?;
                    let group_key = row
                        .clone()
                        .into_iter()
                        .take(this.group_key_indices.len())
                        .collect();
                    let inter_states = row.into_iter().skip(this.group_key_indices.len()).collect();

                    let mut agg_group = AggGroup::<S>::for_eowc_output(
                        this.version,
                        Some(GroupKey::new(
                            group_key,
                            Some(this.group_key_table_pk_projection.clone()),
                        )),
                        &this.agg_calls,
                        &this.agg_funcs,
                        &this.storages,
                        &inter_states,
                        &this.input_stream_key,
                        this.row_count_index,
                        this.emit_on_window_close,
                        this.extreme_cache_size,
                        &this.input_schema,
                    )?;

                    let (change, stats) = agg_group
                        .build_outputs_change(&this.storages, &this.agg_funcs)
                        .await?;
                    vars.stats.merge_state_cache_stats(stats);

                    if let Some(change) = change
                        && let Some(chunk) = vars.chunk_builder.append_record(change)
                    {
                        yield chunk;
                    }
                }
            }
        } else {
            let storage = &this.storages;
            let agg_funcs = &this.agg_funcs;
            let futs = vars
                .dirty_groups
                .drain()
                .map(|(key, mut agg_group)| async move {
                    let (change, stats) =
                        agg_group.build_outputs_change(storage, agg_funcs).await?;
                    Ok::<_, StreamExecutorError>((change, stats, key, agg_group))
                });
            let mut buffered = stream::iter(futs).buffer_unordered(10).fuse();
            while let Some(result) = buffered.next().await {
                let (change, stats, key, agg_group) = result?;
                vars.stats.merge_state_cache_stats(stats);
                if let Some(change) = change
                    && let Some(chunk) = vars.chunk_builder.append_record(change)
                {
                    yield chunk;
                }
                // move dirty groups back to cache
                vars.agg_group_cache.put(key, Some(agg_group));
            }
        }

        // Yield the remaining rows in chunk builder.
        if let Some(chunk) = vars.chunk_builder.take() {
            yield chunk;
        }

        if let Some(watermark) = window_watermark {
            // Update watermark of state tables, for state cleaning.
            this.all_state_tables_mut()
                .for_each(|table| table.update_watermark(watermark.clone()));
        }

        // Flush distinct dedup state.
        vars.distinct_dedup.flush(&mut this.distinct_dedup_tables)?;

        // Evict cache to target capacity.
        vars.agg_group_cache.evict();
    }

    fn flush_metrics(_this: &ExecutorInner<K, S>, vars: &mut ExecutionVars<K, S>) {
        vars.metrics
            .agg_lookup_miss_count
            .inc_by(std::mem::take(&mut vars.stats.lookup_miss_count));
        vars.metrics
            .agg_total_lookup_count
            .inc_by(std::mem::take(&mut vars.stats.total_lookup_count));
        vars.metrics
            .agg_cached_entry_count
            .set(vars.agg_group_cache.len() as i64);
        vars.metrics
            .agg_chunk_lookup_miss_count
            .inc_by(std::mem::take(&mut vars.stats.chunk_lookup_miss_count));
        vars.metrics
            .agg_chunk_total_lookup_count
            .inc_by(std::mem::take(&mut vars.stats.chunk_total_lookup_count));
        vars.metrics
            .agg_state_cache_lookup_count
            .inc_by(std::mem::take(&mut vars.stats.agg_state_cache_lookup_count));
        vars.metrics
            .agg_state_cache_miss_count
            .inc_by(std::mem::take(&mut vars.stats.agg_state_cache_miss_count));
    }

    async fn commit_state_tables(
        this: &mut ExecutorInner<K, S>,
        epoch: EpochPair,
    ) -> StreamExecutorResult<Vec<StateTablePostCommit<'_, S>>> {
        futures::future::try_join_all(
            this.all_state_tables_mut()
                .map(|table| async { table.commit(epoch).await }),
        )
        .await
    }

    async fn try_flush_data(this: &mut ExecutorInner<K, S>) -> StreamExecutorResult<()> {
        futures::future::try_join_all(
            this.all_state_tables_mut()
                .map(|table| async { table.try_flush().await }),
        )
        .await?;
        Ok(())
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let HashAggExecutor {
            input,
            inner: mut this,
        } = self;

        let actor_id = this.actor_ctx.id;

        let window_col_idx_in_group_key = this.intermediate_state_table.pk_indices()[0];
        let window_col_idx = this.group_key_indices[window_col_idx_in_group_key];

        let agg_group_cache_metrics_info = MetricsInfo::new(
            this.actor_ctx.streaming_metrics.clone(),
            this.intermediate_state_table.table_id(),
            this.actor_ctx.id,
            "agg intermediate state table",
        );
        let metrics = this.actor_ctx.streaming_metrics.new_hash_agg_metrics(
            this.intermediate_state_table.table_id(),
            this.actor_ctx.id,
            this.actor_ctx.fragment_id,
        );

        let mut vars = ExecutionVars {
            metrics,
            stats: ExecutionStats::default(),
            agg_group_cache: ManagedLruCache::unbounded_with_hasher(
                this.watermark_sequence.clone(),
                agg_group_cache_metrics_info,
                PrecomputedBuildHasher,
            ),
            dirty_groups: Default::default(),
            distinct_dedup: DistinctDeduplicater::new(
                &this.agg_calls,
                this.watermark_sequence.clone(),
                &this.distinct_dedup_tables,
                &this.actor_ctx,
            ),
            buffered_watermarks: vec![None; this.group_key_indices.len()],
            window_watermark: None,
            chunk_builder: StreamChunkBuilder::new(this.chunk_size, this.info.schema.data_types()),
            buffer: SortBuffer::new(window_col_idx_in_group_key, &this.intermediate_state_table),
        };

        // TODO(rc): use something like a `ColumnMapping` type
        let group_key_invert_idx = {
            let mut group_key_invert_idx = vec![None; input.info().schema.len()];
            for (group_key_seq, group_key_idx) in this.group_key_indices.iter().enumerate() {
                group_key_invert_idx[*group_key_idx] = Some(group_key_seq);
            }
            group_key_invert_idx
        };

        // First barrier
        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        for table in this.all_state_tables_mut() {
            table.init_epoch(first_epoch).await?;
        }

        #[for_await]
        for msg in input {
            let msg = msg?;
            vars.agg_group_cache.evict();
            match msg {
                Message::Watermark(watermark) => {
                    let group_key_seq = group_key_invert_idx[watermark.col_idx];
                    if let Some(group_key_seq) = group_key_seq {
                        if watermark.col_idx == window_col_idx {
                            vars.window_watermark = Some(watermark.val.clone());
                        }
                        vars.buffered_watermarks[group_key_seq] =
                            Some(watermark.with_idx(group_key_seq));
                    }
                }
                Message::Chunk(chunk) => {
                    Self::apply_chunk(&mut this, &mut vars, chunk).await?;

                    if vars.dirty_groups.estimated_heap_size() >= this.max_dirty_groups_heap_size {
                        // flush dirty groups if heap size is too large, to better prevent from OOM
                        #[for_await]
                        for chunk in Self::flush_data(&mut this, &mut vars) {
                            yield Message::Chunk(chunk?);
                        }
                    }

                    Self::try_flush_data(&mut this).await?;
                }
                Message::Barrier(barrier) => {
                    #[for_await]
                    for chunk in Self::flush_data(&mut this, &mut vars) {
                        yield Message::Chunk(chunk?);
                    }
                    Self::flush_metrics(&this, &mut vars);
                    let emit_on_window_close = this.emit_on_window_close;
                    let post_commits = Self::commit_state_tables(&mut this, barrier.epoch).await?;

                    if emit_on_window_close {
                        // ignore watermarks on other columns
                        if let Some(watermark) =
                            vars.buffered_watermarks[window_col_idx_in_group_key].take()
                        {
                            yield Message::Watermark(watermark);
                        }
                    } else {
                        for buffered_watermark in &mut vars.buffered_watermarks {
                            if let Some(watermark) = buffered_watermark.take() {
                                yield Message::Watermark(watermark);
                            }
                        }
                    }

                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(actor_id);
                    yield Message::Barrier(barrier);

                    // Update the vnode bitmap for state tables of all agg calls if asked.
                    if let Some(keyed_cache_may_stale) =
                        try_join_all(post_commits.into_iter().map(|post_commit| {
                            post_commit.post_yield_barrier(update_vnode_bitmap.clone())
                        }))
                        .await?
                        .pop()
                        .expect("should have at least one table")
                        .map(|(_, cache_may_stale)| cache_may_stale)
                    {
                        if emit_on_window_close {
                            // `SortBuffer` may output data directly from its in-memory cache without
                            // checking current vnode ownership. Therefore, we must rebuild the cache
                            // whenever the vnode bitmap is updated to avoid emitting rows that no
                            // longer belong to this actor.
                            vars.buffer
                                .refill_cache(None, &this.intermediate_state_table)
                                .await?;
                        }

                        // Manipulate the cache if necessary.
                        if keyed_cache_may_stale {
                            vars.agg_group_cache.clear();
                            vars.distinct_dedup.dedup_caches_mut().for_each(|cache| {
                                cache.clear();
                            });
                        }
                    }
                }
            }
        }
    }
}
