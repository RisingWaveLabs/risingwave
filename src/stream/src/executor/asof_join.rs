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

//! `AsOf` join executor with optional LRU cache.
//!
//! When `use_cache` is false, the executor directly queries the state table.
//! When `use_cache` is true, the `AsOfJoinHashMap` maintains an LRU cache.
//! Controlled by the session variable `streaming_asof_join_use_cache`.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::time::Duration;

use either::Either;
use itertools::Itertools;
use multimap::MultiMap;
use risingwave_common::array::Op;
use risingwave_common::metrics::LabelGuardedHistogram;
use risingwave_common::row::RowExt;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::sort_util::cmp_rows_ascending;
use tokio::time::Instant;

use super::barrier_align::*;
use super::join::asof_join::*;
use super::join::builder::JoinStreamChunkBuilder;
use super::join::row::JoinRow;
use super::join::*;
use super::watermark::*;
use crate::executor::join::builder::JoinChunkBuilder;
use crate::executor::prelude::*;

pub struct JoinParams {
    /// Indices of the join keys
    pub join_key_indices: Vec<usize>,
    /// Indices of the input pk after dedup
    pub deduped_pk_indices: Vec<usize>,
}

impl JoinParams {
    pub fn new(join_key_indices: Vec<usize>, deduped_pk_indices: Vec<usize>) -> Self {
        Self {
            join_key_indices,
            deduped_pk_indices,
        }
    }
}

struct JoinSide<S: StateStore, E: AsOfRowEncoding> {
    /// Store all data from a one side stream
    ht: AsOfJoinHashMap<S, E>,
    /// Indices of the join key columns
    join_key_indices: Vec<usize>,
    /// The data type of all columns without degree.
    all_data_types: Vec<DataType>,
    /// The mapping from input indices of a side to output columns.
    i2o_mapping: Vec<(usize, usize)>,
    i2o_mapping_indexed: MultiMap<usize, usize>,
    /// The index of the inequality column.
    inequal_key_idx: usize,
}

impl<S: StateStore, E: AsOfRowEncoding> std::fmt::Debug for JoinSide<S, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinSide")
            .field("join_key_indices", &self.join_key_indices)
            .field("col_types", &self.all_data_types)
            .field("i2o_mapping", &self.i2o_mapping)
            .finish()
    }
}

impl<S: StateStore, E: AsOfRowEncoding> JoinSide<S, E> {
    pub async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.ht.init(epoch).await
    }
}

/// `AsOfJoinExecutor` for streaming as-of joins.
///
/// This executor uses `AsOfJoinHashMap` to support both execution modes:
/// it can either maintain an LRU cache or query the state table directly,
/// depending on the `use_cache` configuration.
pub struct AsOfJoinExecutor<S: StateStore, const T: AsOfJoinTypePrimitive, E: AsOfRowEncoding> {
    ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Left input executor
    input_l: Option<Executor>,
    /// Right input executor
    input_r: Option<Executor>,
    /// The data types of the formed new columns
    actual_output_data_types: Vec<DataType>,
    /// The parameters of the left join executor
    side_l: JoinSide<S, E>,
    /// The parameters of the right join executor
    side_r: JoinSide<S, E>,

    /// Whether nulls are considered equal for each join key column
    null_safe: Vec<bool>,

    metrics: Arc<StreamingMetrics>,
    /// The maximum size of the chunk produced by executor at a time
    chunk_size: usize,
    /// watermark column index -> `BufferedWatermarks`
    watermark_buffers: BTreeMap<usize, BufferedWatermarks<SideTypePrimitive>>,
    /// `AsOf` join description
    asof_desc: AsOfDesc,
    /// Row counter for periodic cache eviction
    cnt_rows_received: u32,
    /// Number of processed rows between periodic manual evictions of the join cache.
    join_cache_evict_interval_rows: u32,
    /// Threshold for logging high join amplification warnings.
    high_join_amplification_threshold: usize,
}

impl<S: StateStore, const T: AsOfJoinTypePrimitive, E: AsOfRowEncoding> std::fmt::Debug
    for AsOfJoinExecutor<S, T, E>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsOfJoinExecutor")
            .field("join_type", &T)
            .field("input_left", &self.input_l.as_ref().unwrap().identity())
            .field("input_right", &self.input_r.as_ref().unwrap().identity())
            .field("side_l", &self.side_l)
            .field("side_r", &self.side_r)
            .field("stream_key", &self.info.stream_key)
            .field("schema", &self.info.schema)
            .field("actual_output_data_types", &self.actual_output_data_types)
            .finish()
    }
}

impl<S: StateStore, const T: AsOfJoinTypePrimitive, E: AsOfRowEncoding> Execute
    for AsOfJoinExecutor<S, T, E>
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

struct EqJoinArgs<'a, S: StateStore, E: AsOfRowEncoding> {
    ctx: &'a ActorContextRef,
    side_l: &'a mut JoinSide<S, E>,
    side_r: &'a mut JoinSide<S, E>,
    null_safe: &'a [bool],
    asof_desc: &'a AsOfDesc,
    actual_output_data_types: &'a [DataType],
    chunk: StreamChunk,
    chunk_size: usize,
    cnt_rows_received: &'a mut u32,
    join_cache_evict_interval_rows: u32,
    high_join_amplification_threshold: usize,
    /// Bound at executor scope so the guard isn't dropped between chunks (which resets the series).
    join_matched_join_keys: &'a LabelGuardedHistogram,
}

impl<S: StateStore, const T: AsOfJoinTypePrimitive, E: AsOfRowEncoding> AsOfJoinExecutor<S, T, E> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        info: ExecutorInfo,
        input_l: Executor,
        input_r: Executor,
        params_l: JoinParams,
        params_r: JoinParams,
        null_safe: Vec<bool>,
        output_indices: Vec<usize>,
        state_table_l: StateTable<S>,
        state_table_r: StateTable<S>,
        watermark_epoch: AtomicU64Ref,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
        asof_desc: AsOfDesc,
        use_cache: bool,
        high_join_amplification_threshold: usize,
    ) -> Self {
        let join_cache_evict_interval_rows = ctx
            .config
            .developer
            .join_hash_map_evict_interval_rows
            .max(1);
        let cache_epoch = if use_cache {
            Some(watermark_epoch)
        } else {
            None
        };
        let schema_fields = [
            input_l.schema().fields.clone(),
            input_r.schema().fields.clone(),
        ]
        .concat();

        let original_output_data_types = schema_fields
            .iter()
            .map(|field| field.data_type())
            .collect_vec();
        let actual_output_data_types = output_indices
            .iter()
            .map(|&idx| original_output_data_types[idx].clone())
            .collect_vec();

        let state_all_data_types_l = input_l.schema().data_types();
        let state_all_data_types_r = input_r.schema().data_types();

        let state_join_key_indices_l = params_l.join_key_indices;
        let state_join_key_indices_r = params_r.join_key_indices;

        let join_key_data_types_l = state_join_key_indices_l
            .iter()
            .map(|idx| state_all_data_types_l[*idx].clone())
            .collect_vec();

        let join_key_data_types_r = state_join_key_indices_r
            .iter()
            .map(|idx| state_all_data_types_r[*idx].clone())
            .collect_vec();

        assert_eq!(join_key_data_types_l, join_key_data_types_r);

        let (left_to_output, right_to_output) = {
            let (left_len, right_len) = if is_left_semi_or_anti(T) {
                (state_all_data_types_l.len(), 0usize)
            } else if is_right_semi_or_anti(T) {
                (0usize, state_all_data_types_r.len())
            } else {
                (state_all_data_types_l.len(), state_all_data_types_r.len())
            };
            JoinStreamChunkBuilder::get_i2o_mapping(&output_indices, left_len, right_len)
        };

        let l2o_indexed = MultiMap::from_iter(left_to_output.iter().copied());
        let r2o_indexed = MultiMap::from_iter(right_to_output.iter().copied());

        let watermark_buffers = BTreeMap::new();

        let inequal_key_idx_l = asof_desc.left_idx;
        let inequal_key_idx_r = asof_desc.right_idx;

        Self {
            ctx: ctx.clone(),
            info,
            input_l: Some(input_l),
            input_r: Some(input_r),
            actual_output_data_types,
            null_safe,
            side_l: JoinSide {
                ht: AsOfJoinHashMap::new(
                    state_join_key_indices_l.clone(),
                    state_table_l,
                    params_l.deduped_pk_indices,
                    inequal_key_idx_l,
                    state_all_data_types_l.clone(),
                    cache_epoch.clone(),
                    metrics.clone(),
                    ctx.id,
                    ctx.fragment_id,
                    "left",
                ),
                join_key_indices: state_join_key_indices_l,
                all_data_types: state_all_data_types_l,
                i2o_mapping: left_to_output,
                i2o_mapping_indexed: l2o_indexed,
                inequal_key_idx: inequal_key_idx_l,
            },
            side_r: JoinSide {
                ht: AsOfJoinHashMap::new(
                    state_join_key_indices_r.clone(),
                    state_table_r,
                    params_r.deduped_pk_indices,
                    inequal_key_idx_r,
                    state_all_data_types_r.clone(),
                    cache_epoch,
                    metrics.clone(),
                    ctx.id,
                    ctx.fragment_id,
                    "right",
                ),
                join_key_indices: state_join_key_indices_r,
                all_data_types: state_all_data_types_r,
                i2o_mapping: right_to_output,
                i2o_mapping_indexed: r2o_indexed,
                inequal_key_idx: inequal_key_idx_r,
            },
            metrics,
            chunk_size,
            watermark_buffers,
            asof_desc,
            cnt_rows_received: 0,
            join_cache_evict_interval_rows,
            high_join_amplification_threshold,
        }
    }

    /// Periodically evict the LRU cache to prevent memory buildup between barriers.
    fn evict_cache(
        side_l: &mut JoinSide<S, E>,
        side_r: &mut JoinSide<S, E>,
        cnt_rows_received: &mut u32,
        join_cache_evict_interval_rows: u32,
    ) {
        *cnt_rows_received += 1;
        if *cnt_rows_received >= join_cache_evict_interval_rows {
            side_l.ht.evict_cache();
            side_r.ht.evict_cache();
            *cnt_rows_received = 0;
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let input_l = self.input_l.take().unwrap();
        let input_r = self.input_r.take().unwrap();
        let aligned_stream = barrier_align(
            input_l.execute(),
            input_r.execute(),
            self.ctx.id,
            self.ctx.fragment_id,
            self.metrics.clone(),
            "Join",
        );
        pin_mut!(aligned_stream);
        let actor_id = self.ctx.id;

        let barrier = expect_first_barrier_from_aligned_stream(&mut aligned_stream).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        self.side_l.init(first_epoch).await?;
        self.side_r.init(first_epoch).await?;

        let actor_id_str = self.ctx.id.to_string();
        let fragment_id_str = self.ctx.fragment_id.to_string();

        let join_actor_input_waiting_duration_ns = self
            .metrics
            .join_actor_input_waiting_duration_ns
            .with_guarded_label_values(&[&actor_id_str, &fragment_id_str]);
        let left_join_match_duration_ns = self
            .metrics
            .join_match_duration_ns
            .with_guarded_label_values(&[actor_id_str.as_str(), fragment_id_str.as_str(), "left"]);
        let right_join_match_duration_ns = self
            .metrics
            .join_match_duration_ns
            .with_guarded_label_values(&[actor_id_str.as_str(), fragment_id_str.as_str(), "right"]);

        let barrier_join_match_duration_ns = self
            .metrics
            .join_match_duration_ns
            .with_guarded_label_values(&[
                actor_id_str.as_str(),
                fragment_id_str.as_str(),
                "barrier",
            ]);

        let left_join_cached_entry_count = self
            .metrics
            .join_cached_entry_count
            .with_guarded_label_values(&[actor_id_str.as_str(), fragment_id_str.as_str(), "left"]);

        let right_join_cached_entry_count = self
            .metrics
            .join_cached_entry_count
            .with_guarded_label_values(&[actor_id_str.as_str(), fragment_id_str.as_str(), "right"]);

        // Bind at executor scope: a per-chunk guard would be dropped between chunks and reset the series.
        let left_table_id_str = self.side_l.ht.table_id().to_string();
        let right_table_id_str = self.side_r.ht.table_id().to_string();
        let left_join_matched_join_keys = self
            .metrics
            .join_matched_join_keys
            .with_guarded_label_values(&[
                actor_id_str.as_str(),
                fragment_id_str.as_str(),
                left_table_id_str.as_str(),
            ]);
        let right_join_matched_join_keys = self
            .metrics
            .join_matched_join_keys
            .with_guarded_label_values(&[
                actor_id_str.as_str(),
                fragment_id_str.as_str(),
                right_table_id_str.as_str(),
            ]);

        let mut start_time = Instant::now();

        while let Some(msg) = aligned_stream
            .next()
            .instrument_await("hash_join_barrier_align")
            .await
        {
            join_actor_input_waiting_duration_ns.inc_by(start_time.elapsed().as_nanos() as u64);
            match msg? {
                AlignedMessage::WatermarkLeft(watermark) => {
                    for watermark_to_emit in self.handle_watermark(SideType::Left, watermark)? {
                        yield Message::Watermark(watermark_to_emit);
                    }
                }
                AlignedMessage::WatermarkRight(watermark) => {
                    for watermark_to_emit in self.handle_watermark(SideType::Right, watermark)? {
                        yield Message::Watermark(watermark_to_emit);
                    }
                }
                AlignedMessage::Left(chunk) => {
                    let mut left_time = Duration::from_nanos(0);
                    let mut left_start_time = Instant::now();
                    #[for_await]
                    for chunk in Self::eq_join_left(EqJoinArgs {
                        ctx: &self.ctx,
                        side_l: &mut self.side_l,
                        side_r: &mut self.side_r,
                        null_safe: &self.null_safe,
                        asof_desc: &self.asof_desc,
                        actual_output_data_types: &self.actual_output_data_types,
                        chunk,
                        chunk_size: self.chunk_size,
                        cnt_rows_received: &mut self.cnt_rows_received,
                        join_cache_evict_interval_rows: self.join_cache_evict_interval_rows,
                        high_join_amplification_threshold: self.high_join_amplification_threshold,
                        join_matched_join_keys: &left_join_matched_join_keys,
                    }) {
                        left_time += left_start_time.elapsed();
                        yield Message::Chunk(chunk?);
                        left_start_time = Instant::now();
                    }
                    left_time += left_start_time.elapsed();
                    left_join_match_duration_ns.inc_by(left_time.as_nanos() as u64);
                    self.try_flush_data().await?;
                }
                AlignedMessage::Right(chunk) => {
                    let mut right_time = Duration::from_nanos(0);
                    let mut right_start_time = Instant::now();
                    #[for_await]
                    for chunk in Self::eq_join_right(EqJoinArgs {
                        ctx: &self.ctx,
                        side_l: &mut self.side_l,
                        side_r: &mut self.side_r,
                        null_safe: &self.null_safe,
                        asof_desc: &self.asof_desc,
                        actual_output_data_types: &self.actual_output_data_types,
                        chunk,
                        chunk_size: self.chunk_size,
                        cnt_rows_received: &mut self.cnt_rows_received,
                        join_cache_evict_interval_rows: self.join_cache_evict_interval_rows,
                        high_join_amplification_threshold: self.high_join_amplification_threshold,
                        join_matched_join_keys: &right_join_matched_join_keys,
                    }) {
                        right_time += right_start_time.elapsed();
                        yield Message::Chunk(chunk?);
                        right_start_time = Instant::now();
                    }
                    right_time += right_start_time.elapsed();
                    right_join_match_duration_ns.inc_by(right_time.as_nanos() as u64);
                    self.try_flush_data().await?;
                }
                AlignedMessage::Barrier(barrier) => {
                    let barrier_start_time = Instant::now();
                    let (left_post_commit, right_post_commit) =
                        self.flush_data(barrier.epoch).await?;

                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(actor_id);
                    yield Message::Barrier(barrier);

                    // Update the vnode bitmap for state tables of both sides if asked.
                    right_post_commit
                        .post_yield_barrier(update_vnode_bitmap.clone())
                        .await?;
                    if left_post_commit
                        .post_yield_barrier(update_vnode_bitmap)
                        .await?
                        .unwrap_or(false)
                    {
                        self.watermark_buffers
                            .values_mut()
                            .for_each(|buffers| buffers.clear());
                    }

                    // Report metrics of cached join rows/entries
                    for (join_cached_entry_count, ht) in [
                        (&left_join_cached_entry_count, &self.side_l.ht),
                        (&right_join_cached_entry_count, &self.side_r.ht),
                    ] {
                        join_cached_entry_count.set(ht.entry_count() as i64);
                    }

                    barrier_join_match_duration_ns
                        .inc_by(barrier_start_time.elapsed().as_nanos() as u64);
                }
            }
            start_time = Instant::now();
        }
    }

    async fn flush_data(
        &mut self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<(
        AsOfJoinHashMapPostCommit<'_, S, E>,
        AsOfJoinHashMapPostCommit<'_, S, E>,
    )> {
        let left = self.side_l.ht.flush(epoch).await?;
        let right = self.side_r.ht.flush(epoch).await?;
        Ok((left, right))
    }

    async fn try_flush_data(&mut self) -> StreamExecutorResult<()> {
        self.side_l.ht.try_flush().await?;
        self.side_r.ht.try_flush().await?;
        Ok(())
    }

    fn handle_watermark(
        &mut self,
        side: SideTypePrimitive,
        watermark: Watermark,
    ) -> StreamExecutorResult<Vec<Watermark>> {
        let (side_update, side_match) = if side == SideType::Left {
            (&mut self.side_l, &mut self.side_r)
        } else {
            (&mut self.side_r, &mut self.side_l)
        };

        // State cleaning
        if side_update.join_key_indices[0] == watermark.col_idx {
            side_match.ht.update_watermark(watermark.val.clone());
        }

        // Select watermarks to yield.
        let wm_in_jk = side_update
            .join_key_indices
            .iter()
            .positions(|idx| *idx == watermark.col_idx);
        let mut watermarks_to_emit = vec![];
        for idx in wm_in_jk {
            let buffers = self
                .watermark_buffers
                .entry(idx)
                .or_insert_with(|| BufferedWatermarks::with_ids([SideType::Left, SideType::Right]));
            if let Some(selected_watermark) = buffers.handle_watermark(side, watermark.clone()) {
                let empty_indices = vec![];
                let output_indices = side_update
                    .i2o_mapping_indexed
                    .get_vec(&side_update.join_key_indices[idx])
                    .unwrap_or(&empty_indices)
                    .iter()
                    .chain(
                        side_match
                            .i2o_mapping_indexed
                            .get_vec(&side_match.join_key_indices[idx])
                            .unwrap_or(&empty_indices),
                    );
                for output_idx in output_indices {
                    watermarks_to_emit.push(selected_watermark.clone().with_idx(*output_idx));
                }
            };
        }
        Ok(watermarks_to_emit)
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn eq_join_left(args: EqJoinArgs<'_, S, E>) {
        let EqJoinArgs {
            ctx,
            side_l,
            side_r,
            null_safe,
            asof_desc,
            actual_output_data_types,
            chunk,
            chunk_size,
            cnt_rows_received,
            join_cache_evict_interval_rows,
            high_join_amplification_threshold,
            join_matched_join_keys,
        } = args;

        let (side_update, side_match) = (side_l, side_r);

        let mut join_chunk_builder =
            JoinChunkBuilder::<T, { SideType::Left }>::new(JoinStreamChunkBuilder::new(
                chunk_size,
                actual_output_data_types.to_vec(),
                side_update.i2o_mapping.clone(),
                side_match.i2o_mapping.clone(),
            ));

        // The inequality key is always a single column; wrap in an array for `project()`.
        let inequal_key_idx_update = [side_update.inequal_key_idx];

        for r in chunk.rows_with_holes() {
            let Some((op, row)) = r else {
                continue;
            };
            Self::evict_cache(
                side_update,
                side_match,
                cnt_rows_received,
                join_cache_evict_interval_rows,
            );

            // Check null-safe: if any non-null-safe join key column is NULL, skip matching.
            // Rows that can never match are not stored in state (consistent with old behavior).
            let join_key_null_not_safe = side_update
                .join_key_indices
                .iter()
                .zip_eq(null_safe.iter())
                .any(|(idx, ns)| !ns && row.datum_at(*idx).is_none());
            if join_key_null_not_safe {
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        if let Some(chunk) =
                            join_chunk_builder.forward_if_not_matched(Op::Insert, row)
                        {
                            yield chunk;
                        }
                    }
                    Op::Delete | Op::UpdateDelete => {
                        if let Some(chunk) =
                            join_chunk_builder.forward_if_not_matched(Op::Delete, row)
                        {
                            yield chunk;
                        }
                    }
                }
                join_matched_join_keys.observe(0.0);
                continue;
            }

            let join_key = row.project(&side_update.join_key_indices);

            let inequal_key_is_null = side_update.ht.check_inequal_key_null(&row);
            let inequal_key = row.project(&inequal_key_idx_update);

            let mut join_matched_rows_cnt = 0;

            if !inequal_key_is_null {
                let matched_row_by_inequality = match asof_desc.inequality_type {
                    AsOfInequalityType::Lt => {
                        side_match
                            .ht
                            .lower_bound_by_inequality_with_jk_prefix(
                                &join_key,
                                Bound::Excluded(&inequal_key),
                            )
                            .await
                    }
                    AsOfInequalityType::Le => {
                        side_match
                            .ht
                            .lower_bound_by_inequality_with_jk_prefix(
                                &join_key,
                                Bound::Included(&inequal_key),
                            )
                            .await
                    }
                    AsOfInequalityType::Gt => {
                        side_match
                            .ht
                            .upper_bound_by_inequality_with_jk_prefix(
                                &join_key,
                                Bound::Excluded(&inequal_key),
                            )
                            .await
                    }
                    AsOfInequalityType::Ge => {
                        side_match
                            .ht
                            .upper_bound_by_inequality_with_jk_prefix(
                                &join_key,
                                Bound::Included(&inequal_key),
                            )
                            .await
                    }
                }?
                .map(|row| JoinRow::new(row, 0));
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        if let Some(matched_row) = matched_row_by_inequality {
                            join_matched_rows_cnt += 1;
                            if let Some(chunk) =
                                join_chunk_builder.with_match_on_insert(&row, &matched_row)
                            {
                                yield chunk;
                            }
                        } else if let Some(chunk) =
                            join_chunk_builder.forward_if_not_matched(Op::Insert, row)
                        {
                            yield chunk;
                        }
                        side_update.ht.insert(row)?;
                    }
                    Op::Delete | Op::UpdateDelete => {
                        if let Some(matched_row) = matched_row_by_inequality {
                            join_matched_rows_cnt += 1;
                            if let Some(chunk) =
                                join_chunk_builder.with_match_on_delete(&row, &matched_row)
                            {
                                yield chunk;
                            }
                        } else if let Some(chunk) =
                            join_chunk_builder.forward_if_not_matched(Op::Delete, row)
                        {
                            yield chunk;
                        }
                        side_update.ht.delete(row)?;
                    }
                }
            } else {
                // Inequality key is NULL, which can never satisfy the inequality predicate,
                // so we skip storing and only forward as unmatched for left outer join.
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        if let Some(chunk) =
                            join_chunk_builder.forward_if_not_matched(Op::Insert, row)
                        {
                            yield chunk;
                        }
                    }
                    Op::Delete | Op::UpdateDelete => {
                        if let Some(chunk) =
                            join_chunk_builder.forward_if_not_matched(Op::Delete, row)
                        {
                            yield chunk;
                        }
                    }
                }
            }
            join_matched_join_keys.observe(join_matched_rows_cnt as _);
            if join_matched_rows_cnt > high_join_amplification_threshold {
                tracing::warn!(target: "high_join_amplification",
                    matched_rows_len = join_matched_rows_cnt,
                    update_table_id = %side_update.ht.table_id(),
                    match_table_id = %side_match.ht.table_id(),
                    join_key = ?join_key,
                    actor_id = %ctx.id,
                    fragment_id = %ctx.fragment_id,
                    "large rows matched for join key when AsOf join updating left side",
                );
            }
        }
        if let Some(chunk) = join_chunk_builder.take() {
            yield chunk;
        }
    }

    fn cmp_pk_rows(pk1: &impl Row, pk2: &impl Row) -> Ordering {
        cmp_rows_ascending(pk1, pk2)
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn eq_join_right(args: EqJoinArgs<'_, S, E>) {
        let EqJoinArgs {
            ctx,
            side_l,
            side_r,
            null_safe,
            asof_desc,
            actual_output_data_types,
            chunk,
            chunk_size,
            cnt_rows_received,
            join_cache_evict_interval_rows,
            high_join_amplification_threshold,
            join_matched_join_keys,
        } = args;

        let (side_update, side_match) = (side_r, side_l);

        let mut join_chunk_builder = JoinStreamChunkBuilder::new(
            chunk_size,
            actual_output_data_types.to_vec(),
            side_update.i2o_mapping.clone(),
            side_match.i2o_mapping.clone(),
        );

        // The inequality key is always a single column; wrap in an array for `project()`.
        let inequal_key_idx_update = [side_update.inequal_key_idx];

        for r in chunk.rows_with_holes() {
            let Some((op, row)) = r else {
                continue;
            };
            Self::evict_cache(
                side_update,
                side_match,
                cnt_rows_received,
                join_cache_evict_interval_rows,
            );

            // Check null-safe: if any non-null-safe join key column is NULL, skip.
            // Rows that can never match are not stored in state (consistent with old behavior).
            let join_key_null_not_safe = side_update
                .join_key_indices
                .iter()
                .zip_eq(null_safe.iter())
                .any(|(idx, ns)| !ns && row.datum_at(*idx).is_none());
            if join_key_null_not_safe {
                join_matched_join_keys.observe(0.0);
                continue;
            }

            let join_key = row.project(&side_update.join_key_indices);

            let inequal_key_is_null = side_update.ht.check_inequal_key_null(&row);
            let inequal_key = row.project(&inequal_key_idx_update);

            let mut join_matched_rows_cnt = 0;

            if !inequal_key_is_null {
                let (row_to_delete_r, row_to_insert_r) = {
                    let (first_row, second_row) = side_update
                        .ht
                        .first_two_by_inequality_with_jk_prefix(&join_key, &inequal_key)
                        .await?;
                    if let Some(first_row) = first_row {
                        let row_pk = side_update.ht.get_pk_from_row(row);
                        let first_pk = side_update.ht.get_pk_from_row(&first_row).to_owned_row();
                        match op {
                            Op::Insert | Op::UpdateInsert => {
                                // If there are multiple rows match the inequality key in the right table, we use one with smallest pk.
                                if Self::cmp_pk_rows(&first_pk, &row_pk) == Ordering::Greater {
                                    (Some(Either::Left(first_row)), Some(Either::Right(row)))
                                } else {
                                    // No affected row in the right table.
                                    (None, None)
                                }
                            }
                            Op::Delete | Op::UpdateDelete => {
                                if Self::cmp_pk_rows(&first_pk, &row_pk) == Ordering::Equal {
                                    if let Some(second_row) = second_row {
                                        (Some(Either::Right(row)), Some(Either::Left(second_row)))
                                    } else {
                                        (Some(Either::Right(row)), None)
                                    }
                                } else {
                                    // No affected row in the right table.
                                    (None, None)
                                }
                            }
                        }
                    } else {
                        match op {
                            // Decide the row_to_delete later
                            Op::Insert | Op::UpdateInsert => (None, Some(Either::Right(row))),
                            // Decide the row_to_insert later
                            Op::Delete | Op::UpdateDelete => (Some(Either::Right(row)), None),
                        }
                    }
                };
                // 4 cases for row_to_delete_r and row_to_insert_r:
                // 1. Some(_), Some(_): delete row_to_delete_r and insert row_to_insert_r
                // 2. None, Some(_)   : row_to_delete to be decided by the nearest inequality key
                // 3. Some(_), None   : row_to_insert to be decided by the nearest inequality key
                // 4. None, None      : do nothing
                if row_to_delete_r.is_none() && row_to_insert_r.is_none() {
                    // no row to delete or insert.
                } else {
                    let prev_inequality_key = side_update
                        .ht
                        .upper_bound_by_inequality_with_jk_prefix(
                            &join_key,
                            Bound::Excluded(&inequal_key),
                        )
                        .await?
                        .map(|r| r.project(&inequal_key_idx_update));
                    let next_inequality_key = side_update
                        .ht
                        .lower_bound_by_inequality_with_jk_prefix(
                            &join_key,
                            Bound::Excluded(&inequal_key),
                        )
                        .await?
                        .map(|r| r.project(&inequal_key_idx_update));

                    let affected_inequality_key_r = match asof_desc.inequality_type {
                        AsOfInequalityType::Lt | AsOfInequalityType::Le => &next_inequality_key,
                        AsOfInequalityType::Gt | AsOfInequalityType::Ge => &prev_inequality_key,
                    };
                    let affected_row_r =
                        if let Some(affected_inequality_key_r) = affected_inequality_key_r {
                            side_update
                                .ht
                                .first_by_inequality_with_jk_prefix(
                                    &join_key,
                                    &affected_inequality_key_r,
                                )
                                .await?
                        } else {
                            None
                        }
                        .map(Either::Left);

                    let (row_to_delete_r, row_to_insert_r) =
                        match (&row_to_delete_r, &row_to_insert_r) {
                            (Some(_), Some(_)) => (row_to_delete_r, row_to_insert_r),
                            (None, Some(_)) => (affected_row_r, row_to_insert_r),
                            (Some(_), None) => (row_to_delete_r, affected_row_r),
                            (None, None) => unreachable!(),
                        };
                    let range = match asof_desc.inequality_type {
                        AsOfInequalityType::Lt => (
                            prev_inequality_key
                                .map(Either::Left)
                                .map_or_else(|| Bound::Unbounded, Bound::Included),
                            Bound::Excluded(Either::Right(&inequal_key)),
                        ),
                        AsOfInequalityType::Le => (
                            prev_inequality_key
                                .map(Either::Left)
                                .map_or_else(|| Bound::Unbounded, Bound::Excluded),
                            Bound::Included(Either::Right(&inequal_key)),
                        ),
                        AsOfInequalityType::Gt => (
                            Bound::Excluded(Either::Right(&inequal_key)),
                            next_inequality_key
                                .map(Either::Left)
                                .map_or_else(|| Bound::Unbounded, Bound::Included),
                        ),
                        AsOfInequalityType::Ge => (
                            Bound::Included(Either::Right(&inequal_key)),
                            next_inequality_key
                                .map(Either::Left)
                                .map_or_else(|| Bound::Unbounded, Bound::Excluded),
                        ),
                    };

                    let rows_l_stream = side_match
                        .ht
                        .range_by_inequality_with_jk_prefix(&join_key, &range)
                        .await?;
                    #[for_await]
                    for row_l in rows_l_stream {
                        let row_l = row_l?;
                        join_matched_rows_cnt += 1;
                        if let Some(row_to_delete_r) = &row_to_delete_r {
                            if let Some(chunk) =
                                join_chunk_builder.append_row(Op::Delete, row_to_delete_r, &row_l)
                            {
                                yield chunk;
                            }
                        } else if is_as_of_left_outer(T)
                            && let Some(chunk) =
                                join_chunk_builder.append_row_matched(Op::Delete, &row_l)
                        {
                            yield chunk;
                        }
                        if let Some(row_to_insert_r) = &row_to_insert_r {
                            if let Some(chunk) =
                                join_chunk_builder.append_row(Op::Insert, row_to_insert_r, &row_l)
                            {
                                yield chunk;
                            }
                        } else if is_as_of_left_outer(T)
                            && let Some(chunk) =
                                join_chunk_builder.append_row_matched(Op::Insert, &row_l)
                        {
                            yield chunk;
                        }
                    }
                }

                match op {
                    Op::Insert | Op::UpdateInsert => {
                        side_update.ht.insert(row)?;
                    }
                    Op::Delete | Op::UpdateDelete => {
                        side_update.ht.delete(row)?;
                    }
                }
            } else {
                // Inequality key is NULL, which can never satisfy the inequality predicate,
                // so we skip storing. No action needed on the right side.
            }
            join_matched_join_keys.observe(join_matched_rows_cnt as _);
            if join_matched_rows_cnt > high_join_amplification_threshold {
                let join_key = row.project(&side_update.join_key_indices);
                tracing::warn!(target: "high_join_amplification",
                    matched_rows_len = join_matched_rows_cnt,
                    update_table_id = %side_update.ht.table_id(),
                    match_table_id = %side_match.ht.table_id(),
                    join_key = ?join_key,
                    actor_id = %ctx.id,
                    fragment_id = %ctx.fragment_id,
                    "large rows matched for join key when AsOf join updating right side",
                );
            }
        }
        if let Some(chunk) = join_chunk_builder.take() {
            yield chunk;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use prometheus::Registry;
    use risingwave_common::array::*;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, TableId};
    use risingwave_common::config::MetricLevel;
    use risingwave_common::metrics::get_label;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};

    async fn create_in_memory_state_table(
        mem_state: MemoryStateStore,
        data_types: &[DataType],
        order_types: &[OrderType],
        pk_indices: &[usize],
        table_id: u32,
    ) -> StateTable<MemoryStateStore> {
        let column_descs = data_types
            .iter()
            .enumerate()
            .map(|(id, data_type)| ColumnDesc::unnamed(ColumnId::new(id as i32), data_type.clone()))
            .collect_vec();
        StateTable::from_table_catalog(
            &gen_pbtable(
                TableId::new(table_id),
                column_descs,
                order_types.to_vec(),
                pk_indices.to_vec(),
                0,
            ),
            mem_state.clone(),
            None,
        )
        .await
    }

    async fn create_executor<const T: AsOfJoinTypePrimitive>(
        asof_desc: AsOfDesc,
        use_cache: bool,
    ) -> (MessageSender, MessageSender, BoxedMessageStream) {
        create_executor_with_metrics::<T>(
            asof_desc,
            use_cache,
            Arc::new(StreamingMetrics::unused()),
        )
        .await
    }

    async fn create_executor_with_metrics<const T: AsOfJoinTypePrimitive>(
        asof_desc: AsOfDesc,
        use_cache: bool,
        metrics: Arc<StreamingMetrics>,
    ) -> (MessageSender, MessageSender, BoxedMessageStream) {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64), // join key
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };
        let (tx_l, source_l) = MockSource::channel();
        let source_l = source_l.into_executor(schema.clone(), vec![1]);
        let (tx_r, source_r) = MockSource::channel();
        let source_r = source_r.into_executor(schema, vec![1]);
        let params_l = JoinParams::new(vec![0], vec![1]);
        let params_r = JoinParams::new(vec![0], vec![1]);

        let mem_state = MemoryStateStore::new();

        let state_l = create_in_memory_state_table(
            mem_state.clone(),
            &[DataType::Int64, DataType::Int64, DataType::Int64],
            &[
                OrderType::ascending(),
                OrderType::ascending(),
                OrderType::ascending(),
            ],
            &[0, asof_desc.left_idx, 1],
            0,
        )
        .await;

        let state_r = create_in_memory_state_table(
            mem_state,
            &[DataType::Int64, DataType::Int64, DataType::Int64],
            &[
                OrderType::ascending(),
                OrderType::ascending(),
                OrderType::ascending(),
            ],
            &[0, asof_desc.right_idx, 1],
            1,
        )
        .await;

        let schema: Schema = [source_l.schema().fields(), source_r.schema().fields()]
            .concat()
            .into_iter()
            .collect();
        let schema_len = schema.len();
        let info = ExecutorInfo::for_test(schema, vec![1], "AsOfJoinExecutor".to_owned(), 0);

        let executor = AsOfJoinExecutor::<MemoryStateStore, T, AsOfCpuEncoding>::new(
            ActorContext::for_test(123),
            info,
            source_l,
            source_r,
            params_l,
            params_r,
            vec![false],
            (0..schema_len).collect_vec(),
            state_l,
            state_r,
            Arc::new(AtomicU64::new(0)),
            metrics,
            1024,
            asof_desc,
            use_cache,
            2048, // high_join_amplification_threshold
        );
        (tx_l, tx_r, executor.boxed().execute())
    }

    fn join_matched_sample_count(registry: &Registry, table_id: &str) -> u64 {
        registry
            .gather()
            .iter()
            .find(|metric_family| metric_family.name() == "stream_join_matched_join_keys")
            .and_then(|metric_family| {
                metric_family.get_metric().iter().find(|metric| {
                    get_label::<String>(metric, "table_id").as_deref() == Some(table_id)
                })
            })
            .map(|metric| metric.get_histogram().get_sample_count())
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn test_asof_join_records_null_equi_key_probe_metrics() -> StreamExecutorResult<()> {
        let asof_desc = AsOfDesc {
            left_idx: 1,
            right_idx: 1,
            inequality_type: AsOfInequalityType::Lt,
        };
        let registry = Registry::new();
        let metrics = Arc::new(StreamingMetrics::new(&registry, MetricLevel::Debug));
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_executor_with_metrics::<{ AsOfJoinType::Inner }>(asof_desc, false, metrics)
                .await;

        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        tx_l.push_chunk(StreamChunk::from_pretty(
            "  I I I
             + . 10 1",
        ));
        hash_join.next_unwrap_pending();

        tx_r.push_chunk(StreamChunk::from_pretty(
            "  I I I
             + . 20 2",
        ));
        hash_join.next_unwrap_pending();

        assert_eq!(join_matched_sample_count(&registry, "0"), 1);
        assert_eq!(join_matched_sample_count(&registry, "1"), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_asof_inner_join() -> StreamExecutorResult<()> {
        test_asof_inner_join_impl(false).await
    }

    #[tokio::test]
    async fn test_asof_inner_join_with_cache() -> StreamExecutorResult<()> {
        test_asof_inner_join_impl(true).await
    }

    async fn test_asof_inner_join_impl(use_cache: bool) -> StreamExecutorResult<()> {
        let asof_desc = AsOfDesc {
            left_idx: 0,
            right_idx: 2,
            inequality_type: AsOfInequalityType::Lt,
        };

        let chunk_l1 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 7
             + 2 5 8
             + 3 6 9",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I I
             + 3 8 1
             - 3 8 1",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I I
             + 2 1 7
             + 2 2 1
             + 2 3 4
             + 2 4 2
             + 6 1 9
             + 6 2 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I I
             - 2 3 4",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I I I
             + 2 3 3",
        );
        let chunk_l3 = StreamChunk::from_pretty(
            "  I I I
             - 2 5 8",
        );
        let chunk_l4 = StreamChunk::from_pretty(
            "  I I I
             + 6 3 1
             + 6 4 1",
        );
        let chunk_r4 = StreamChunk::from_pretty(
            "  I I I
             - 6 1 9",
        );

        let (mut tx_l, mut tx_r, mut hash_join) =
            create_executor::<{ AsOfJoinType::Inner }>(asof_desc, use_cache).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        hash_join.next_unwrap_pending();

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                + 2 5 8 2 1 7
                - 2 5 8 2 1 7
                + 2 5 8 2 3 4"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                - 2 5 8 2 3 4
                + 2 5 8 2 1 7"
            )
        );

        // push the 3rd right chunk
        tx_r.push_chunk(chunk_r3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                - 2 5 8 2 1 7
                + 2 5 8 2 3 3"
            )
        );

        // push the 3rd left chunk
        tx_l.push_chunk(chunk_l3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                - 2 5 8 2 3 3"
            )
        );

        // push the 4th left chunk
        tx_l.push_chunk(chunk_l4);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                + 6 3 1 6 1 9
                + 6 4 1 6 1 9"
            )
        );

        // push the 4th right chunk
        tx_r.push_chunk(chunk_r4);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                - 6 3 1 6 1 9
                + 6 3 1 6 2 9
                - 6 4 1 6 1 9
                + 6 4 1 6 2 9"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_asof_left_outer_join() -> StreamExecutorResult<()> {
        test_asof_left_outer_join_impl(false).await
    }

    #[tokio::test]
    async fn test_asof_left_outer_join_with_cache() -> StreamExecutorResult<()> {
        test_asof_left_outer_join_impl(true).await
    }

    async fn test_asof_left_outer_join_impl(use_cache: bool) -> StreamExecutorResult<()> {
        let asof_desc = AsOfDesc {
            left_idx: 1,
            right_idx: 2,
            inequality_type: AsOfInequalityType::Ge,
        };

        let chunk_l1 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 7
             + 2 5 8
             + 3 6 9",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I I
             + 3 8 1
             - 3 8 1",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I I
             + 2 3 4
             + 2 2 5
             + 2 1 5
             + 6 1 8
             + 6 2 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I I
             - 2 3 4
             - 2 1 5
             - 2 2 5",
        );
        let chunk_l3 = StreamChunk::from_pretty(
            "  I I I
             + 6 8 9",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I I I
             - 6 1 8",
        );

        let (mut tx_l, mut tx_r, mut hash_join) =
            create_executor::<{ AsOfJoinType::LeftOuter }>(asof_desc, use_cache).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                + 1 4 7 . . .
                + 2 5 8 . . .
                + 3 6 9 . . ."
            )
        );

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                + 3 8 1 . . . D
                - 3 8 1 . . . D"
            )
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                - 2 5 8 . . .
                + 2 5 8 2 3 4
                - 2 5 8 2 3 4
                + 2 5 8 2 2 5
                - 2 5 8 2 2 5
                + 2 5 8 2 1 5"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                - 2 5 8 2 1 5
                + 2 5 8 2 2 5
                - 2 5 8 2 2 5
                + 2 5 8 . . ."
            )
        );

        // push the 3rd left chunk
        tx_l.push_chunk(chunk_l3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                + 6 8 9 6 1 8"
            )
        );

        // push the 3rd right chunk
        tx_r.push_chunk(chunk_r3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                - 6 8 9 6 1 8
                + 6 8 9 . . ."
            )
        );
        Ok(())
    }
}
