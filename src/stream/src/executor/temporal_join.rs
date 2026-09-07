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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::ops::Bound;

use anyhow::Context;
use either::Either;
use futures::TryStreamExt;
use futures::stream::{self, PollNext};
use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::hash::{HashKey, NullBitmap};
use risingwave_common::row::RowExt;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common_estimate_size::{EstimateSize, KvSize};
use risingwave_expr::expr::NonStrictExpression;
use risingwave_storage::row_serde::value_serde::ValueRowSerde;
use risingwave_storage::store::PrefetchOptions;

use super::join::{JoinType, JoinTypePrimitive};
use super::monitor::TemporalJoinMetrics;
use crate::cache::ManagedLruCache;
use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::ReplicatedStateTable;
use crate::executor::join::builder::JoinStreamChunkBuilder;
use crate::executor::prelude::*;

pub struct TemporalJoinExecutor<
    K: HashKey,
    S: StateStore,
    SD: ValueRowSerde,
    const T: JoinTypePrimitive,
    const APPEND_ONLY: bool,
> {
    ctx: ActorContextRef,
    #[expect(dead_code)]
    info: ExecutorInfo,
    left: Executor,
    right: Executor,
    right_table: TemporalSide<K, S, SD>,
    left_join_keys: Vec<usize>,
    right_join_keys: Vec<usize>,
    null_safe: Vec<bool>,
    condition: Option<NonStrictExpression>,
    output_indices: Vec<usize>,
    chunk_size: usize,
    memo_table: Option<StateTable<S>>,
    metrics: TemporalJoinMetrics,
    is_broadcast: bool,
}

#[derive(Default)]
pub struct JoinEntry {
    /// pk -> row
    cached: HashMap<OwnedRow, OwnedRow>,
    kv_heap_size: KvSize,
}

impl EstimateSize for JoinEntry {
    fn estimated_heap_size(&self) -> usize {
        // TODO: Add internal size.
        // https://github.com/risingwavelabs/risingwave/issues/9713
        self.kv_heap_size.size()
    }
}

impl JoinEntry {
    /// Insert into the cache.
    pub fn insert(&mut self, key: OwnedRow, value: OwnedRow) {
        // Lookup might refill the cache before the `insert` messages from the temporal side
        // upstream.
        if let Entry::Vacant(e) = self.cached.entry(key) {
            self.kv_heap_size.add(e.key(), &value);
            e.insert(value);
        } else {
            panic!("value {:?} double insert", value);
        }
    }

    /// Delete from the cache.
    pub fn remove(&mut self, key: &OwnedRow) {
        if let Some(value) = self.cached.remove(key) {
            self.kv_heap_size.sub(key, &value);
        } else {
            panic!("key {:?} should be in the cache", key);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.cached.is_empty()
    }
}

struct TemporalSide<K: HashKey, S: StateStore, SD: ValueRowSerde> {
    source: ReplicatedStateTable<S, SD>,
    table_stream_key_indices: Vec<usize>,
    cache: ManagedLruCache<K, JoinEntry>,
    join_key_data_types: Vec<DataType>,
}

impl<K: HashKey, S: StateStore, SD: ValueRowSerde> TemporalSide<K, S, SD> {
    /// Fetch records from temporal side table and ensure the entry in the cache.
    /// If already exists, the entry will be promoted.
    async fn fetch_or_promote_keys(
        &mut self,
        keys: impl Iterator<Item = &K>,
        metrics: &TemporalJoinMetrics,
    ) -> StreamExecutorResult<()> {
        let mut futs = Vec::with_capacity(keys.size_hint().1.unwrap_or(0));
        for key in keys {
            metrics.temporal_join_total_query_cache_count.inc();

            if self.cache.get(key).is_none() {
                metrics.temporal_join_cache_miss_count.inc();

                futs.push(async {
                    let pk_prefix = key.deserialize(&self.join_key_data_types)?;

                    let iter = self
                        .source
                        .iter_with_prefix(
                            &pk_prefix,
                            &(Bound::<OwnedRow>::Unbounded, Bound::<OwnedRow>::Unbounded),
                            PrefetchOptions::default(),
                        )
                        .await?;

                    let mut entry = JoinEntry::default();

                    pin_mut!(iter);
                    while let Some(row) = iter.next().await {
                        let row: OwnedRow = row?;
                        entry.insert(
                            row.as_ref()
                                .project(&self.table_stream_key_indices)
                                .into_owned_row(),
                            row,
                        );
                    }
                    let key = key.clone();
                    Ok((key, entry)) as StreamExecutorResult<_>
                });
            }
        }

        #[for_await]
        for res in stream::iter(futs).buffered(16) {
            let (key, entry) = res?;
            self.cache.put(key, entry);
        }

        Ok(())
    }

    fn force_peek(&self, key: &K) -> &JoinEntry {
        self.cache.peek(key).expect("key should exist")
    }

    fn update(
        &mut self,
        chunks: Vec<StreamChunk>,
        join_keys: &[usize],
        right_stream_key_indices: &[usize],
    ) -> StreamExecutorResult<()> {
        for chunk in chunks {
            let keys = K::build_many(join_keys, chunk.data_chunk());
            for (r, key) in chunk.rows_with_holes().zip_eq_debug(keys.into_iter()) {
                let Some((op, row)) = r else {
                    continue;
                };
                if self.cache.contains(&key) {
                    // Update cache
                    let mut entry = self.cache.get_mut(&key).unwrap();
                    let stream_key = row.project(right_stream_key_indices).into_owned_row();
                    match op {
                        Op::Insert | Op::UpdateInsert => {
                            entry.insert(stream_key, row.into_owned_row())
                        }
                        Op::Delete | Op::UpdateDelete => entry.remove(&stream_key),
                    };
                }
            }
            self.source.write_chunk(chunk);
        }
        Ok(())
    }
}

pub(super) enum InternalMessage {
    Chunk(StreamChunk),
    Barrier(Vec<StreamChunk>, Barrier),
    WaterMark(Watermark),
}

#[try_stream(ok = StreamChunk, error = StreamExecutorError)]
async fn chunks_until_barrier(stream: impl MessageStream, expected_barrier: Barrier) {
    #[for_await]
    for item in stream {
        match item? {
            Message::Watermark(_) => {
                // ignore
            }
            Message::Chunk(c) => yield c,
            Message::Barrier(b) if b.epoch != expected_barrier.epoch => {
                return Err(StreamExecutorError::align_barrier(expected_barrier, b));
            }
            Message::Barrier(_) => return Ok(()),
        }
    }
}

#[try_stream(ok = InternalMessage, error = StreamExecutorError)]
async fn internal_messages_until_barrier(stream: impl MessageStream, expected_barrier: Barrier) {
    #[for_await]
    for item in stream {
        match item? {
            Message::Watermark(w) => {
                yield InternalMessage::WaterMark(w);
            }
            Message::Chunk(c) => yield InternalMessage::Chunk(c),
            Message::Barrier(b) if b.epoch != expected_barrier.epoch => {
                return Err(StreamExecutorError::align_barrier(expected_barrier, b));
            }
            Message::Barrier(_) => return Ok(()),
        }
    }
}

pub(super) async fn expect_first_barrier(
    stream: &mut (impl Stream<Item = StreamExecutorResult<InternalMessage>> + Unpin),
) -> StreamExecutorResult<Barrier> {
    let InternalMessage::Barrier(updates, barrier) = stream
        .try_next()
        .instrument_await("expect_first_barrier")
        .await?
        .context("failed to extract the first message: stream closed unexpectedly")?
    else {
        unreachable!("unexpected internal message");
    };
    assert!(updates.is_empty());
    Ok(barrier)
}

// Align the left and right inputs according to their barriers,
// such that in the produced stream, an aligned interval starts with
// any number of `InternalMessage::Chunk(left_chunk)` and followed by
// `InternalMessage::Barrier(right_chunks, barrier)`.
#[try_stream(ok = InternalMessage, error = StreamExecutorError)]
pub(super) async fn align_input<const YIELD_RIGHT_CHUNKS: bool>(left: Executor, right: Executor) {
    let mut left = pin!(left.execute());
    let mut right = pin!(right.execute());
    // Keep producing intervals until stream exhaustion or errors.
    loop {
        let mut right_chunks = vec![];
        // Produce an aligned interval.
        'inner: loop {
            let mut combined = stream::select_with_strategy(
                left.by_ref().map(Either::Left),
                right.by_ref().map(Either::Right),
                |_: &mut ()| PollNext::Left,
            );
            match combined.next().await {
                Some(Either::Left(Ok(Message::Chunk(c)))) => yield InternalMessage::Chunk(c),
                Some(Either::Right(Ok(Message::Chunk(c)))) => {
                    if YIELD_RIGHT_CHUNKS {
                        right_chunks.push(c);
                    }
                }
                Some(Either::Left(Ok(Message::Barrier(b)))) => {
                    let mut remain = chunks_until_barrier(right.by_ref(), b.clone())
                        .try_collect()
                        .await?;
                    if YIELD_RIGHT_CHUNKS {
                        right_chunks.append(&mut remain);
                    }
                    yield InternalMessage::Barrier(right_chunks, b);
                    break 'inner;
                }
                Some(Either::Right(Ok(Message::Barrier(b)))) => {
                    #[for_await]
                    for internal_message in
                        internal_messages_until_barrier(left.by_ref(), b.clone())
                    {
                        yield internal_message?;
                    }
                    yield InternalMessage::Barrier(right_chunks, b);
                    break 'inner;
                }
                Some(Either::Left(Err(e)) | Either::Right(Err(e))) => return Err(e),
                Some(Either::Left(Ok(Message::Watermark(w)))) => {
                    yield InternalMessage::WaterMark(w);
                }
                Some(Either::Right(Ok(Message::Watermark(_)))) => {
                    // ignore right side watermark
                }
                None => return Ok(()),
            }
        }
    }
}

pub(super) fn apply_indices_map(chunk: StreamChunk, indices: &[usize]) -> StreamChunk {
    let (data_chunk, ops) = chunk.into_parts();
    let (columns, vis) = data_chunk.into_parts();
    let output_columns = indices
        .iter()
        .cloned()
        .map(|idx| columns[idx].clone())
        .collect();
    StreamChunk::with_visibility(ops, output_columns, vis)
}

pub(super) mod phase1 {
    use std::ops::Bound;

    use futures::{StreamExt, pin_mut};
    use futures_async_stream::try_stream;
    use risingwave_common::array::stream_chunk_builder::StreamChunkBuilder;
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_common::hash::{HashKey, NullBitmap};
    use risingwave_common::row::{self, OwnedRow, Row, RowExt};
    use risingwave_common::types::{DataType, DatumRef};
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_storage::StateStore;
    use risingwave_storage::row_serde::value_serde::ValueRowSerde;

    use super::{StreamExecutorError, TemporalSide};
    use crate::common::table::state_table::StateTable;
    use crate::executor::monitor::TemporalJoinMetrics;

    pub trait Phase1Evaluation {
        /// Called when a matched row is found.
        #[must_use = "consume chunk if produced"]
        fn append_matched_row(
            op: Op,
            builder: &mut StreamChunkBuilder,
            left_row: impl Row,
            right_row: impl Row,
        ) -> Option<StreamChunk>;

        /// Called when all matched rows of a join key are appended.
        #[must_use = "consume chunk if produced"]
        fn match_end(
            builder: &mut StreamChunkBuilder,
            op: Op,
            left_row: impl Row,
            right_size: usize,
            matched: bool,
        ) -> Option<StreamChunk>;
    }

    pub struct Inner;
    pub struct LeftOuter;
    pub struct LeftOuterWithCond;

    impl Phase1Evaluation for Inner {
        fn append_matched_row(
            op: Op,
            builder: &mut StreamChunkBuilder,
            left_row: impl Row,
            right_row: impl Row,
        ) -> Option<StreamChunk> {
            builder.append_row(op, left_row.chain(right_row))
        }

        fn match_end(
            _builder: &mut StreamChunkBuilder,
            _op: Op,
            _left_row: impl Row,
            _right_size: usize,
            _matched: bool,
        ) -> Option<StreamChunk> {
            None
        }
    }

    impl Phase1Evaluation for LeftOuter {
        fn append_matched_row(
            op: Op,
            builder: &mut StreamChunkBuilder,
            left_row: impl Row,
            right_row: impl Row,
        ) -> Option<StreamChunk> {
            builder.append_row(op, left_row.chain(right_row))
        }

        fn match_end(
            builder: &mut StreamChunkBuilder,
            op: Op,
            left_row: impl Row,
            right_size: usize,
            matched: bool,
        ) -> Option<StreamChunk> {
            if !matched {
                // If no rows matched, a marker row should be inserted.
                builder.append_row(
                    op,
                    left_row.chain(row::repeat_n(DatumRef::None, right_size)),
                )
            } else {
                None
            }
        }
    }

    impl Phase1Evaluation for LeftOuterWithCond {
        fn append_matched_row(
            op: Op,
            builder: &mut StreamChunkBuilder,
            left_row: impl Row,
            right_row: impl Row,
        ) -> Option<StreamChunk> {
            builder.append_row(op, left_row.chain(right_row))
        }

        fn match_end(
            builder: &mut StreamChunkBuilder,
            op: Op,
            left_row: impl Row,
            right_size: usize,
            _matched: bool,
        ) -> Option<StreamChunk> {
            // A marker row should always be inserted and mark as invisible for non-lookup filters evaluation.
            // The row will be converted to visible in the further steps if no rows matched after all filters evaluated.
            builder.append_row_invisible(
                op,
                left_row.chain(row::repeat_n(DatumRef::None, right_size)),
            )
        }
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    #[expect(clippy::too_many_arguments)]
    pub(super) async fn handle_chunk<
        'a,
        K: HashKey,
        S: StateStore,
        SD: ValueRowSerde,
        E: Phase1Evaluation,
        const APPEND_ONLY: bool,
    >(
        chunk_size: usize,
        right_size: usize,
        full_schema: Vec<DataType>,
        left_join_keys: &'a [usize],
        right_table: &'a mut TemporalSide<K, S, SD>,
        memo_table_lookup_prefix: &'a [usize],
        memo_table: &'a mut Option<StateTable<S>>,
        null_matched: &'a K::Bitmap,
        chunk: StreamChunk,
        metrics: &'a TemporalJoinMetrics,
    ) {
        let mut builder = StreamChunkBuilder::new(chunk_size, full_schema);
        let keys = K::build_many(left_join_keys, chunk.data_chunk());
        let to_fetch_keys = chunk
            .visibility()
            .iter()
            .zip_eq_debug(keys.iter())
            .zip_eq_debug(chunk.ops())
            .filter_map(|((vis, key), op)| {
                if vis {
                    if APPEND_ONLY {
                        assert_eq!(*op, Op::Insert);
                        Some(key)
                    } else {
                        match op {
                            Op::Insert | Op::UpdateInsert => Some(key),
                            Op::Delete | Op::UpdateDelete => None,
                        }
                    }
                } else {
                    None
                }
            });
        right_table
            .fetch_or_promote_keys(to_fetch_keys, metrics)
            .await?;

        for (r, key) in chunk.rows_with_holes().zip_eq_debug(keys.into_iter()) {
            let Some((op, left_row)) = r else {
                continue;
            };

            let mut matched = false;

            if APPEND_ONLY {
                // Append-only temporal join
                if key.null_bitmap().is_subset(null_matched)
                    && let join_entry = right_table.force_peek(&key)
                    && !join_entry.is_empty()
                {
                    matched = true;
                    for right_row in join_entry.cached.values() {
                        if let Some(chunk) =
                            E::append_matched_row(op, &mut builder, left_row, right_row)
                        {
                            yield chunk;
                        }
                    }
                }
            } else {
                // Non-append-only temporal join
                // The memo table persists each matched right row followed by
                // `join_key + left_stream_key`. For broadcast temporal joins, its distribution key
                // refers to the `left_stream_key` portion, so the memo state follows the left side.
                //
                // Write pattern:
                //   for each left input row (with insert op), persist every matched right row.
                // Read pattern:
                //   for each left input row (with delete op), fetch the historical right rows by
                //   memo prefix and delete them from the memo table.
                //
                // Temporal join supports inner join and left outer join, additionally, it could contain other conditions.
                // Surprisingly, we could handle them in a unified way with memo table.
                // The memo table would persist rows fetched from the right table and appending the `join_key` and `left_pk` from the left row.
                // The null rows generated by outer join and the other condition somehow is a stateless operation which means we can handle them without the memo table.
                let memo_table = memo_table.as_mut().unwrap();
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        if key.null_bitmap().is_subset(null_matched)
                            && let join_entry = right_table.force_peek(&key)
                            && !join_entry.is_empty()
                        {
                            matched = true;
                            for right_row in join_entry.cached.values() {
                                let right_row: OwnedRow = right_row.clone();
                                // Insert into memo table
                                memo_table.insert(right_row.clone().chain(
                                    left_row.project(memo_table_lookup_prefix).into_owned_row(),
                                ));
                                if let Some(chunk) = E::append_matched_row(
                                    Op::Insert,
                                    &mut builder,
                                    left_row,
                                    right_row,
                                ) {
                                    yield chunk;
                                }
                            }
                        }
                    }
                    Op::Delete | Op::UpdateDelete => {
                        let mut memo_rows_to_delete = vec![];
                        if key.null_bitmap().is_subset(null_matched) {
                            let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) =
                                &(Bound::Unbounded, Bound::Unbounded);
                            let prefix = left_row.project(memo_table_lookup_prefix);
                            let state_table_iter = memo_table
                                .iter_with_prefix(prefix, sub_range, Default::default())
                                .await?;
                            pin_mut!(state_table_iter);

                            while let Some(memo_row) = state_table_iter.next().await {
                                matched = true;
                                let memo_row = memo_row?.into_owned_row();
                                memo_rows_to_delete.push(memo_row.clone());
                                if let Some(chunk) = E::append_matched_row(
                                    Op::Delete,
                                    &mut builder,
                                    left_row,
                                    memo_row.slice(0..right_size),
                                ) {
                                    yield chunk;
                                }
                            }
                        }
                        for memo_row in memo_rows_to_delete {
                            // Delete from memo table
                            memo_table.delete(memo_row);
                        }
                    }
                }
            }
            if let Some(chunk) = E::match_end(
                &mut builder,
                match op {
                    Op::Insert | Op::UpdateInsert => Op::Insert,
                    Op::Delete | Op::UpdateDelete => Op::Delete,
                },
                left_row,
                right_size,
                matched,
            ) {
                yield chunk;
            }
        }

        if let Some(chunk) = builder.take() {
            yield chunk;
        }
    }
}

impl<
    K: HashKey,
    S: StateStore,
    SD: ValueRowSerde,
    const T: JoinTypePrimitive,
    const APPEND_ONLY: bool,
> TemporalJoinExecutor<K, S, SD, T, APPEND_ONLY>
{
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        info: ExecutorInfo,
        left: Executor,
        right: Executor,
        table: ReplicatedStateTable<S, SD>,
        left_join_keys: Vec<usize>,
        right_join_keys: Vec<usize>,
        null_safe: Vec<bool>,
        condition: Option<NonStrictExpression>,
        output_indices: Vec<usize>,
        table_stream_key_indices: Vec<usize>,
        watermark_sequence: AtomicU64Ref,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
        join_key_data_types: Vec<DataType>,
        memo_table: Option<StateTable<S>>,
        is_broadcast: bool,
    ) -> Self {
        let metrics_info =
            MetricsInfo::new(metrics.clone(), table.table_id(), ctx.id, "temporal join");
        let cache = ManagedLruCache::unbounded(watermark_sequence, metrics_info);

        let metrics = metrics.new_temporal_join_metrics(table.table_id(), ctx.id, ctx.fragment_id);

        Self {
            ctx,
            info,
            left,
            right,
            right_table: TemporalSide {
                source: table,
                table_stream_key_indices,
                cache,
                join_key_data_types,
            },
            left_join_keys,
            right_join_keys,
            null_safe,
            condition,
            output_indices,
            chunk_size,
            memo_table,
            metrics,
            is_broadcast,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let right_size = self.right.schema().len();

        let (left_map, _right_map) = JoinStreamChunkBuilder::get_i2o_mapping(
            &self.output_indices,
            self.left.schema().len(),
            right_size,
        );

        let left_to_output: HashMap<usize, usize> = HashMap::from_iter(left_map.iter().cloned());

        let left_stream_key_indices = self.left.stream_key().to_vec();
        let right_stream_key_indices = self.right.stream_key().to_vec();
        let memo_table_lookup_prefix = self
            .left_join_keys
            .iter()
            .cloned()
            .chain(left_stream_key_indices)
            .collect_vec();

        let null_matched = K::Bitmap::from_bool_vec(self.null_safe);

        let full_schema: Vec<_> = self
            .left
            .schema()
            .data_types()
            .into_iter()
            .chain(self.right.schema().data_types().into_iter())
            .collect();

        let input = align_input::<true>(self.left, self.right);
        pin_mut!(input);
        let barrier = expect_first_barrier(&mut input).await?;
        let barrier_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        self.right_table.source.init_epoch(barrier_epoch).await?;
        if !APPEND_ONLY {
            self.memo_table
                .as_mut()
                .unwrap()
                .init_epoch(barrier_epoch)
                .await?;
        }

        #[for_await]
        for msg in input {
            self.right_table.cache.evict();
            self.metrics
                .temporal_join_cached_entry_count
                .set(self.right_table.cache.len() as i64);
            match msg? {
                InternalMessage::WaterMark(watermark) => {
                    let output_watermark_col_idx = *left_to_output.get(&watermark.col_idx).unwrap();
                    yield Message::Watermark(watermark.with_idx(output_watermark_col_idx));
                }
                InternalMessage::Chunk(chunk) => {
                    let full_schema = full_schema.clone();

                    if T == JoinType::Inner {
                        let st1 = phase1::handle_chunk::<K, S, SD, phase1::Inner, APPEND_ONLY>(
                            self.chunk_size,
                            right_size,
                            full_schema,
                            &self.left_join_keys,
                            &mut self.right_table,
                            &memo_table_lookup_prefix,
                            &mut self.memo_table,
                            &null_matched,
                            chunk,
                            &self.metrics,
                        );
                        #[for_await]
                        for chunk in st1 {
                            let chunk = chunk?;
                            let new_chunk = if let Some(ref cond) = self.condition {
                                let (data_chunk, ops) = chunk.into_parts();
                                let passed_bitmap = cond.eval_infallible(&data_chunk).await;
                                let passed_bitmap =
                                    Arc::unwrap_or_clone(passed_bitmap).into_bool().to_bitmap();
                                let (columns, vis) = data_chunk.into_parts();
                                let new_vis = vis & passed_bitmap;
                                StreamChunk::with_visibility(ops, columns, new_vis)
                            } else {
                                chunk
                            };
                            let new_chunk = apply_indices_map(new_chunk, &self.output_indices);
                            yield Message::Chunk(new_chunk);
                        }
                    } else if let Some(ref cond) = self.condition {
                        // Joined result without evaluating non-lookup conditions.
                        let st1 = phase1::handle_chunk::<
                            K,
                            S,
                            SD,
                            phase1::LeftOuterWithCond,
                            APPEND_ONLY,
                        >(
                            self.chunk_size,
                            right_size,
                            full_schema,
                            &self.left_join_keys,
                            &mut self.right_table,
                            &memo_table_lookup_prefix,
                            &mut self.memo_table,
                            &null_matched,
                            chunk,
                            &self.metrics,
                        );
                        let mut matched_count = 0usize;
                        #[for_await]
                        for chunk in st1 {
                            let chunk = chunk?;
                            let (data_chunk, ops) = chunk.into_parts();
                            let passed_bitmap = cond.eval_infallible(&data_chunk).await;
                            let passed_bitmap =
                                Arc::unwrap_or_clone(passed_bitmap).into_bool().to_bitmap();
                            let (columns, vis) = data_chunk.into_parts();
                            let mut new_vis = BitmapBuilder::with_capacity(vis.len());
                            for (passed, not_match_end) in
                                passed_bitmap.iter().zip_eq_debug(vis.iter())
                            {
                                let is_match_end = !not_match_end;
                                let vis = if is_match_end && matched_count == 0 {
                                    // Nothing is matched, so the marker row should be visible.
                                    true
                                } else if is_match_end {
                                    // reset the count
                                    matched_count = 0;
                                    // rows found, so the marker row should be invisible.
                                    false
                                } else {
                                    if passed {
                                        matched_count += 1;
                                    }
                                    passed
                                };
                                new_vis.append(vis);
                            }
                            let new_chunk = apply_indices_map(
                                StreamChunk::with_visibility(ops, columns, new_vis.finish()),
                                &self.output_indices,
                            );
                            yield Message::Chunk(new_chunk);
                        }
                        // The last row should always be marker row,
                        assert_eq!(matched_count, 0);
                    } else {
                        let st1 = phase1::handle_chunk::<K, S, SD, phase1::LeftOuter, APPEND_ONLY>(
                            self.chunk_size,
                            right_size,
                            full_schema,
                            &self.left_join_keys,
                            &mut self.right_table,
                            &memo_table_lookup_prefix,
                            &mut self.memo_table,
                            &null_matched,
                            chunk,
                            &self.metrics,
                        );
                        #[for_await]
                        for chunk in st1 {
                            let chunk = chunk?;
                            let new_chunk = apply_indices_map(chunk, &self.output_indices);
                            yield Message::Chunk(new_chunk);
                        }
                    }
                }
                InternalMessage::Barrier(updates, barrier) => {
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.ctx.id);
                    let right_update_vnode_bitmap = if self.is_broadcast {
                        None
                    } else {
                        update_vnode_bitmap.clone()
                    };

                    // Write right-side chunks to the replicated state table and update LRU cache.
                    // Must happen before commit.
                    self.right_table.update(
                        updates,
                        &self.right_join_keys,
                        &right_stream_key_indices,
                    )?;
                    let right_post_commit = self.right_table.source.commit(barrier.epoch).await?;
                    let memo_post_commit = if !APPEND_ONLY {
                        Some(
                            self.memo_table
                                .as_mut()
                                .unwrap()
                                .commit(barrier.epoch)
                                .await?,
                        )
                    } else {
                        None
                    };

                    yield Message::Barrier(barrier);

                    if let Some((_, true)) = right_post_commit
                        .post_yield_barrier(right_update_vnode_bitmap)
                        .await?
                    {
                        self.right_table.cache.clear();
                    }
                    if let Some(memo_post_commit) = memo_post_commit {
                        memo_post_commit
                            .post_yield_barrier(update_vnode_bitmap.clone())
                            .await?;
                    }
                }
            }
        }
    }
}

impl<
    K: HashKey,
    S: StateStore,
    SD: ValueRowSerde,
    const T: JoinTypePrimitive,
    const APPEND_ONLY: bool,
> Execute for TemporalJoinExecutor<K, S, SD, T, APPEND_ONLY>
{
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.into_stream().boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use risingwave_common::array::*;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::hash::Key32;
    use risingwave_common::types::{DataType, ScalarRefImpl};
    use risingwave_common::util::epoch::{EpochPair, test_epoch};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_common::util::value_encoding::BasicSerde;
    use risingwave_hummock_test::test_utils::prepare_hummock_test_env;
    use risingwave_storage::hummock::HummockStorage;

    use super::*;
    use crate::common::table::state_table::{
        StateTable, StateTableBuilder, StateTableOpConsistencyLevel,
    };
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::monitor::StreamingMetrics;
    use crate::executor::test_utils::{MockSource, StreamExecutorTestExt};
    use crate::executor::{ActorContext, ExecutorInfo, JoinType};

    /// Tests that a temporal join on a pk-prefix (join key is a strict prefix of the right table's
    /// pk) correctly merges rows committed in epoch1 with rows staged/committed during epoch2, and
    /// produces the right results when the left side arrives in epoch3.
    ///
    /// Right table: (key INT, seq INT, val INT), pk = (key, seq), SINGLETON distribution.
    /// Join condition: `left.left_key` = right.key  (pk prefix: join uses only the first pk column).
    ///
    /// Pre-commit at epoch1: (1,1,100), (1,2,200), (2,1,300)
    /// Epoch2:  right side sends insert (3,1,400) — written to replicated state table, committed
    ///          at the epoch2 barrier.
    /// Epoch3:  left side sends (`left_key=1`, `left_val=111`) and (`left_key=3`, `left_val=333`).
    ///
    /// Expected join output in epoch3:
    ///   (1, 111, 1, 1, 100)  — key=1 row matched from epoch1 data (cache miss → state store read)
    ///   (1, 111, 1, 2, 200)  — key=1 row matched from epoch1 data (same cache entry)
    ///   (3, 333, 3, 1, 400)  — key=3 row matched from epoch2 data (cache miss → state store read)
    #[tokio::test]
    async fn test_temporal_join_pk_prefix_staging_merge() {
        let test_env = prepare_hummock_test_env().await;
        let table_id = TableId::new(1);

        // Right table schema: (key INT col_id=1, seq INT col_id=2, val INT col_id=3)
        // pk = (key idx=0, seq idx=1), SINGLETON distribution (empty distribution_key),
        // read_prefix_len_hint = 2 (= full pk length).
        let right_col_descs = vec![
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int32),
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int32),
            ColumnDesc::unnamed(ColumnId::new(3), DataType::Int32),
        ];
        let order_types = vec![OrderType::ascending(), OrderType::ascending()];
        let pk_indices = vec![0usize, 1];
        let pbtable = gen_pbtable(table_id, right_col_descs, order_types, pk_indices, 2);

        test_env.register_table(pbtable.clone()).await;

        // Pre-commit epoch1 data via a plain StateTable (bypasses the executor).
        {
            let mut setup_table = StateTable::<HummockStorage>::from_table_catalog_inconsistent_op(
                &pbtable,
                test_env.storage.clone(),
                None,
            )
            .await;
            test_env
                .storage
                .start_epoch(test_epoch(1), HashSet::from_iter([table_id]));
            setup_table
                .init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
                .await
                .unwrap();
            setup_table.insert(OwnedRow::new(vec![
                Some(1i32.into()),
                Some(1i32.into()),
                Some(100i32.into()),
            ]));
            setup_table.insert(OwnedRow::new(vec![
                Some(1i32.into()),
                Some(2i32.into()),
                Some(200i32.into()),
            ]));
            setup_table.insert(OwnedRow::new(vec![
                Some(2i32.into()),
                Some(1i32.into()),
                Some(300i32.into()),
            ]));
            test_env
                .storage
                .start_epoch(test_epoch(2), HashSet::from_iter([table_id]));
            setup_table
                .commit_for_test(EpochPair::new_test_epoch(test_epoch(2)))
                .await
                .unwrap();
            // Seal and commit epoch1 data in Hummock so it is visible to all readers.
            test_env.commit_epoch(test_epoch(1)).await;
        }

        // Build the replicated state table for the executor (all 3 columns are output).
        let output_column_ids = vec![ColumnId::new(1), ColumnId::new(2), ColumnId::new(3)];
        let right_table = StateTableBuilder::<_, BasicSerde, true, _>::new(
            &pbtable,
            test_env.storage.clone(),
            None,
        )
        .with_op_consistency_level(StateTableOpConsistencyLevel::Inconsistent)
        .with_output_column_ids(output_column_ids)
        .forbid_preload_all_rows()
        .build()
        .await;

        // Left source: (left_key INT, left_val INT), stream_key = [0].
        let left_schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let (mut left_tx, left_source) = MockSource::channel();
        let left_executor = left_source.into_executor(left_schema.clone(), vec![0]);

        // Right source: mirrors the right table columns (key INT, seq INT, val INT),
        // stream_key = [0, 1] (the pk).
        let right_schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let (mut right_tx, right_source) = MockSource::channel();
        let right_executor = right_source.into_executor(right_schema.clone(), vec![0, 1]);

        // table_stream_key_indices: pk of right table within the output row = [0, 1] (key, seq).
        let table_stream_key_indices = vec![0usize, 1];

        // Join on left.left_key (col 0) = right.key (col 0 in right source).
        let left_join_keys = vec![0usize];
        let right_join_keys = vec![0usize];
        let null_safe = vec![false];
        let join_key_data_types = vec![DataType::Int32];

        // Output: all 5 columns — [left_key, left_val, key, seq, val].
        let output_indices = vec![0usize, 1, 2, 3, 4];
        let output_schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let info = ExecutorInfo::for_test(output_schema, vec![], "TemporalJoinTest".to_owned(), 0);

        let executor = TemporalJoinExecutor::<
            Key32,
            HummockStorage,
            BasicSerde,
            { JoinType::Inner },
            true,
        >::new(
            ActorContext::for_test(0),
            info.clone(),
            left_executor,
            right_executor,
            right_table,
            left_join_keys,
            right_join_keys,
            null_safe,
            None, // no extra non-equi condition
            output_indices,
            table_stream_key_indices,
            Arc::new(AtomicU64::new(0)),
            Arc::new(StreamingMetrics::unused()),
            1024,
            join_key_data_types,
            None, // no memo table (append-only inner join)
            false,
        );

        let mut stream = Box::new(executor).execute();

        // Push the first barrier (init epoch2, prev = epoch1) on both sides.
        left_tx.push_barrier_with_prev_epoch_for_test(test_epoch(2), test_epoch(1), false);
        right_tx.push_barrier_with_prev_epoch_for_test(test_epoch(2), test_epoch(1), false);
        stream.expect_barrier().await;

        // Epoch2: right side inserts (3, 1, 400); left side is quiet.
        // The epoch2→epoch3 barrier will trigger write_chunk + commit for the right table,
        // making (3,1,400) visible as committed epoch2 data.
        right_tx.push_chunk(StreamChunk::from_pretty(
            " i i   i
            + 3 1 400",
        ));
        // Start epoch3 before the barrier that commits epoch2 data.
        test_env
            .storage
            .start_epoch(test_epoch(3), HashSet::from_iter([table_id]));
        left_tx.push_barrier_with_prev_epoch_for_test(test_epoch(3), test_epoch(2), false);
        right_tx.push_barrier_with_prev_epoch_for_test(test_epoch(3), test_epoch(2), false);
        stream.expect_barrier().await;

        // Start epoch4 before the stop barrier that commits epoch3 data.
        test_env
            .storage
            .start_epoch(test_epoch(4), HashSet::from_iter([table_id]));

        // Epoch3: left side sends two rows.
        //   key=1 → cache miss → state store read → finds (1,1,100) and (1,2,200) from epoch1.
        //   key=3 → cache miss → state store read → finds (3,1,400) from epoch2.
        left_tx.push_chunk(StreamChunk::from_pretty(
            " i   i
            + 1 111
            + 3 333",
        ));
        left_tx.push_barrier_with_prev_epoch_for_test(test_epoch(4), test_epoch(3), true);
        right_tx.push_barrier_with_prev_epoch_for_test(test_epoch(4), test_epoch(3), true);

        // Collect all output rows before the stop barrier.
        let mut output_rows: Vec<[i32; 5]> = vec![];
        loop {
            match stream.next().await.unwrap().unwrap() {
                Message::Chunk(chunk) => {
                    for (op, row) in chunk.rows() {
                        assert_eq!(op, Op::Insert);
                        let row: [i32; 5] =
                            std::array::from_fn(|i| match row.datum_at(i).unwrap() {
                                ScalarRefImpl::Int32(v) => v,
                                _ => panic!("expected Int32"),
                            });
                        output_rows.push(row);
                    }
                }
                Message::Barrier(_) => break,
                _ => {}
            }
        }

        output_rows.sort();
        assert_eq!(
            output_rows,
            vec![
                [1, 111, 1, 1, 100], // key=1 matched epoch1 row (1,1,100)
                [1, 111, 1, 2, 200], // key=1 matched epoch1 row (1,2,200)
                [3, 333, 3, 1, 400], // key=3 matched epoch2 row (3,1,400)
            ]
        );
    }
}
