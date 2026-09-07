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

use std::ops::{Bound, Deref, DerefMut};
use std::sync::Arc;

use anyhow::Context;
use futures::StreamExt;
use futures_async_stream::for_await;
use join_row_set::JoinRowSet;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::{HashKey, PrecomputedBuildHasher};
use risingwave_common::metrics::LabelGuardedIntCounter;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;

use super::row::{CachedJoinRow, DegreeType, build_degree_row};
use crate::cache::ManagedLruCache;
use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::{StateTable, StateTablePostCommit};
use crate::consistency::{consistency_error, enable_strict_consistency};
use crate::executor::error::StreamExecutorResult;
use crate::executor::join::row::JoinRow;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{JoinEncoding, StreamExecutorError};
use crate::task::{ActorId, AtomicU64Ref, FragmentId};

/// Memcomparable encoding.
type PkType = Vec<u8>;
pub type HashValueType<E> = Box<JoinEntryState<E>>;

impl<E: JoinEncoding> EstimateSize for Box<JoinEntryState<E>> {
    fn estimated_heap_size(&self) -> usize {
        self.as_ref().estimated_heap_size()
    }
}

/// The wrapper for [`JoinEntryState`] which should be `Some` most of the time in the hash table.
///
/// When the executor is operating on the specific entry of the map, it can hold the ownership of
/// the entry by taking the value out of the `Option`, instead of holding a mutable reference to the
/// map, which can make the compiler happy.
struct HashValueWrapper<E: JoinEncoding>(Option<HashValueType<E>>);

pub(crate) enum CacheResult<E: JoinEncoding> {
    NeverMatch,            // Will never match, will not be in cache at all.
    Miss,                  // Cache-miss
    Hit(HashValueType<E>), // Cache-hit
}

impl<E: JoinEncoding> EstimateSize for HashValueWrapper<E> {
    fn estimated_heap_size(&self) -> usize {
        self.0.estimated_heap_size()
    }
}

impl<E: JoinEncoding> HashValueWrapper<E> {
    const MESSAGE: &'static str = "the state should always be `Some`";

    /// Take the value out of the wrapper. Panic if the value is `None`.
    pub fn take(&mut self) -> HashValueType<E> {
        self.0.take().expect(Self::MESSAGE)
    }
}

impl<E: JoinEncoding> Deref for HashValueWrapper<E> {
    type Target = HashValueType<E>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect(Self::MESSAGE)
    }
}

impl<E: JoinEncoding> DerefMut for HashValueWrapper<E> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect(Self::MESSAGE)
    }
}

type JoinHashMapInner<K, E> = ManagedLruCache<K, HashValueWrapper<E>, PrecomputedBuildHasher>;

pub struct JoinHashMapMetrics {
    /// Basic information
    /// How many times have we hit the cache of join executor
    lookup_miss_count: usize,
    total_lookup_count: usize,
    /// How many times have we miss the cache when insert row
    insert_cache_miss_count: usize,

    // Metrics
    join_lookup_total_count_metric: LabelGuardedIntCounter,
    join_lookup_miss_count_metric: LabelGuardedIntCounter,
    join_insert_cache_miss_count_metrics: LabelGuardedIntCounter,
}

impl JoinHashMapMetrics {
    pub fn new(
        metrics: &StreamingMetrics,
        actor_id: ActorId,
        fragment_id: FragmentId,
        side: &'static str,
        join_table_id: TableId,
    ) -> Self {
        let actor_id = actor_id.to_string();
        let fragment_id = fragment_id.to_string();
        let join_table_id = join_table_id.to_string();
        let join_lookup_total_count_metric = metrics
            .join_lookup_total_count
            .with_guarded_label_values(&[&actor_id, (side), &join_table_id, &fragment_id]);
        let join_lookup_miss_count_metric = metrics
            .join_lookup_miss_count
            .with_guarded_label_values(&[&actor_id, (side), &join_table_id, &fragment_id]);
        let join_insert_cache_miss_count_metrics = metrics
            .join_insert_cache_miss_count
            .with_guarded_label_values(&[&actor_id, (side), &join_table_id, &fragment_id]);

        Self {
            lookup_miss_count: 0,
            total_lookup_count: 0,
            insert_cache_miss_count: 0,
            join_lookup_total_count_metric,
            join_lookup_miss_count_metric,
            join_insert_cache_miss_count_metrics,
        }
    }

    pub fn inc_lookup(&mut self) {
        self.total_lookup_count += 1;
    }

    pub fn inc_lookup_miss(&mut self) {
        self.lookup_miss_count += 1;
    }

    pub fn inc_insert_cache_miss(&mut self) {
        self.insert_cache_miss_count += 1;
    }

    pub fn flush(&mut self) {
        self.join_lookup_total_count_metric
            .inc_by(self.total_lookup_count as u64);
        self.join_lookup_miss_count_metric
            .inc_by(self.lookup_miss_count as u64);
        self.join_insert_cache_miss_count_metrics
            .inc_by(self.insert_cache_miss_count as u64);
        self.total_lookup_count = 0;
        self.lookup_miss_count = 0;
        self.insert_cache_miss_count = 0;
    }
}

pub struct JoinHashMap<K: HashKey, S: StateStore, E: JoinEncoding> {
    /// Store the join states.
    inner: JoinHashMapInner<K, E>,
    /// Data types of the join key columns
    join_key_data_types: Vec<DataType>,
    /// Null safe bitmap for each join pair
    null_matched: K::Bitmap,
    /// The memcomparable serializer of primary key.
    pk_serializer: OrderedRowSerde,
    /// State table. Contains the data from upstream.
    state: TableInner<S>,
    /// Degree table.
    ///
    /// The degree is generated from the hash join executor.
    /// Each row in `state` has a corresponding degree in `degree state`.
    /// A degree value `d` in for a row means the row has `d` matched row in the other join side.
    ///
    /// It will only be used when needed in a side.
    ///
    /// - Full Outer: both side
    /// - Left Outer/Semi/Anti: left side
    /// - Right Outer/Semi/Anti: right side
    /// - Inner: neither side.
    ///
    /// Should be set to `None` if `need_degree_table` was set to `false`.
    ///
    /// The degree of each row will tell us if we need to emit `NULL` for the row.
    /// For instance, given `lhs LEFT JOIN rhs`,
    /// If the degree of a row in `lhs` is 0, it means the row does not have a match in `rhs`.
    /// If the degree of a row in `lhs` is 2, it means the row has two matches in `rhs`.
    /// Now, when emitting the result of the join, we need to emit `NULL` for the row in `lhs` if
    /// the degree is 0.
    ///
    /// Why don't just use a boolean value instead of a degree count?
    /// Consider the case where we delete a matched record from `rhs`.
    /// Since we can delete a record,
    /// there must have been a record in `rhs` that matched the record in `lhs`.
    /// So this value is `true`.
    /// But we don't know how many records are matched after removing this record,
    /// since we only stored a boolean value rather than the count.
    /// Hence we need to store the count of matched records.
    degree_state: Option<TableInner<S>>,
    // TODO(kwannoel): Make this `const` instead.
    /// If degree table is need
    need_degree_table: bool,
    /// Pk is part of the join key.
    pk_contained_in_jk: bool,
    /// Metrics of the hash map
    metrics: JoinHashMapMetrics,
    _marker: std::marker::PhantomData<E>,
}

impl<K: HashKey, S: StateStore, E: JoinEncoding> JoinHashMap<K, S, E> {
    pub(crate) fn get_degree_state_mut_ref(&mut self) -> (&[usize], &mut Option<TableInner<S>>) {
        (&self.state.order_key_indices, &mut self.degree_state)
    }

    /// NOTE(kwannoel): This allows us to concurrently stream records from the `state_table`,
    /// and update the degree table, without using `unsafe` code.
    ///
    /// This is because we obtain separate references to separate parts of the `JoinHashMap`,
    /// instead of reusing the same reference to `JoinHashMap` for concurrent read access to `state_table`,
    /// and write access to the degree table.
    pub(crate) async fn fetch_matched_rows_and_get_degree_table_ref<'a>(
        &'a mut self,
        key: &'a K,
    ) -> StreamExecutorResult<(
        impl Stream<Item = StreamExecutorResult<(PkType, JoinRow<OwnedRow>)>> + 'a,
        &'a [usize],
        &'a mut Option<TableInner<S>>,
    )> {
        let degree_state = &mut self.degree_state;
        let (order_key_indices, pk_indices, state_table) = (
            &self.state.order_key_indices,
            &self.state.pk_indices,
            &mut self.state.table,
        );
        let degrees = if let Some(degree_state) = degree_state {
            Some(fetch_degrees(key, &self.join_key_data_types, &degree_state.table).await?)
        } else {
            None
        };
        let stream = into_stream(
            &self.join_key_data_types,
            pk_indices,
            &self.pk_serializer,
            state_table,
            key,
            degrees,
        );
        Ok((stream, order_key_indices, &mut self.degree_state))
    }
}

#[try_stream(ok = (PkType, JoinRow<OwnedRow>), error = StreamExecutorError)]
pub(crate) async fn into_stream<'a, K: HashKey, S: StateStore>(
    join_key_data_types: &'a [DataType],
    pk_indices: &'a [usize],
    pk_serializer: &'a OrderedRowSerde,
    state_table: &'a StateTable<S>,
    key: &'a K,
    degrees: Option<Vec<DegreeType>>,
) {
    let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
    let decoded_key = key.deserialize(join_key_data_types)?;
    let table_iter = state_table
        .iter_with_prefix_respecting_watermark(&decoded_key, sub_range, PrefetchOptions::default())
        .await?;

    #[for_await]
    for (i, entry) in table_iter.enumerate() {
        let encoded_row = entry?;
        let encoded_pk = encoded_row
            .as_ref()
            .project(pk_indices)
            .memcmp_serialize(pk_serializer);
        let join_row = JoinRow::new(encoded_row, degrees.as_ref().map_or(0, |d| d[i]));
        yield (encoded_pk, join_row);
    }
}

/// We use this to fetch ALL degrees into memory.
/// We use this instead of a streaming interface.
/// It is necessary because we must update the `degree_state_table` concurrently.
/// If we obtain the degrees in a stream,
/// we will need to hold an immutable reference to the state table for the entire lifetime,
/// preventing us from concurrently updating the state table.
///
/// The cost of fetching all degrees upfront is acceptable. We currently already do so
/// in `fetch_cached_state`.
/// The memory use should be limited since we only store a u64.
///
/// Let's say we have amplification of 1B, we will have 1B * 8 bytes ~= 8GB
///
/// We can also have further optimization, to permit breaking the streaming update,
/// to flush the in-memory degrees, if this is proven to have high memory consumption.
///
/// TODO(kwannoel): Perhaps we can cache these separately from matched rows too.
/// Because matched rows may occupy a larger capacity.
///
/// Argument for this:
/// We only hit this when cache miss. When cache miss, we will have this as one off cost.
/// Keeping this cached separately from matched rows is beneficial.
/// Then we can evict matched rows, without touching the degrees.
async fn fetch_degrees<K: HashKey, S: StateStore>(
    key: &K,
    join_key_data_types: &[DataType],
    degree_state_table: &StateTable<S>,
) -> StreamExecutorResult<Vec<DegreeType>> {
    let key = key.deserialize(join_key_data_types)?;
    let mut degrees = vec![];
    let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
    let table_iter = degree_state_table
        .iter_with_prefix_respecting_watermark(key, sub_range, PrefetchOptions::default())
        .await?;
    let degree_col_idx = degree_col_idx_in_row(degree_state_table);
    #[for_await]
    for entry in table_iter {
        let degree_row = entry?;
        debug_assert!(
            degree_row.len() > degree_col_idx,
            "degree row should have at least pk_len + 1 columns"
        );
        let degree_i64 = degree_row
            .datum_at(degree_col_idx)
            .expect("degree should not be NULL");
        degrees.push(degree_i64.into_int64() as u64);
    }
    Ok(degrees)
}

fn degree_col_idx_in_row<S: StateStore>(degree_state_table: &StateTable<S>) -> usize {
    // Degree column is at index pk_len in the full schema: [pk..., _degree, inequality?].
    let degree_col_idx = degree_state_table.pk_indices().len();
    match degree_state_table.value_indices() {
        Some(value_indices) => value_indices
            .iter()
            .position(|idx| *idx == degree_col_idx)
            .expect("degree column should be included in value indices"),
        None => degree_col_idx,
    }
}

// NOTE(kwannoel): This is not really specific to `TableInner`.
// A degree table is `TableInner`, a `TableInner` might not be a degree table.
// Hence we don't specify it in its impl block.
pub(crate) fn update_degree<S: StateStore, const INCREMENT: bool>(
    order_key_indices: &[usize],
    degree_state: &mut TableInner<S>,
    matched_row: &mut JoinRow<impl Row>,
) {
    let inequality_idx = degree_state.degree_inequality_idx;
    let old_degree_row = build_degree_row(
        order_key_indices,
        matched_row.degree,
        inequality_idx,
        &matched_row.row,
    );
    if INCREMENT {
        matched_row.degree += 1;
    } else {
        // DECREMENT
        matched_row.degree -= 1;
    }
    let new_degree_row = build_degree_row(
        order_key_indices,
        matched_row.degree,
        inequality_idx,
        &matched_row.row,
    );
    degree_state.table.update(old_degree_row, new_degree_row);
}

pub struct TableInner<S: StateStore> {
    /// Indices of the (cache) pk in a state row
    pub(crate) pk_indices: Vec<usize>,
    /// Indices of the join key in a state row
    join_key_indices: Vec<usize>,
    /// The order key of the join side has the following format:
    /// | `join_key` ... | pk ... |
    /// Where `join_key` contains all the columns not in the pk.
    /// It should be a superset of the pk.
    order_key_indices: Vec<usize>,
    /// Optional: index of inequality column in the input row for degree table.
    /// Used for inequality-based watermark cleaning of degree tables.
    /// When present, the degree table schema is: [pk..., _degree, `inequality_val`].
    pub(crate) degree_inequality_idx: Option<usize>,
    pub(crate) table: StateTable<S>,
}

impl<S: StateStore> TableInner<S> {
    pub fn new(
        pk_indices: Vec<usize>,
        join_key_indices: Vec<usize>,
        table: StateTable<S>,
        degree_inequality_idx: Option<usize>,
    ) -> Self {
        let order_key_indices = table.pk_indices().to_vec();
        Self {
            pk_indices,
            join_key_indices,
            order_key_indices,
            degree_inequality_idx,
            table,
        }
    }

    fn error_context(&self, row: &impl Row) -> String {
        let pk = row.project(&self.pk_indices);
        let jk = row.project(&self.join_key_indices);
        format!(
            "join key: {}, pk: {}, row: {}, state_table_id: {}",
            jk.display(),
            pk.display(),
            row.display(),
            self.table.table_id()
        )
    }
}

impl<K: HashKey, S: StateStore, E: JoinEncoding> JoinHashMap<K, S, E> {
    /// Create a [`JoinHashMap`] with the given LRU capacity.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        watermark_sequence: AtomicU64Ref,
        join_key_data_types: Vec<DataType>,
        state_join_key_indices: Vec<usize>,
        state_all_data_types: Vec<DataType>,
        state_table: StateTable<S>,
        state_pk_indices: Vec<usize>,
        degree_state: Option<TableInner<S>>,
        null_matched: K::Bitmap,
        pk_contained_in_jk: bool,
        metrics: Arc<StreamingMetrics>,
        actor_id: ActorId,
        fragment_id: FragmentId,
        side: &'static str,
    ) -> Self {
        // TODO: unify pk encoding with state table.
        let pk_data_types = state_pk_indices
            .iter()
            .map(|i| state_all_data_types[*i].clone())
            .collect();
        let pk_serializer = OrderedRowSerde::new(
            pk_data_types,
            vec![OrderType::ascending(); state_pk_indices.len()],
        );

        let join_table_id = state_table.table_id();
        let state = TableInner {
            pk_indices: state_pk_indices,
            join_key_indices: state_join_key_indices,
            order_key_indices: state_table.pk_indices().to_vec(),
            degree_inequality_idx: None,
            table: state_table,
        };

        let need_degree_table = degree_state.is_some();

        let metrics_info = MetricsInfo::new(
            metrics.clone(),
            join_table_id,
            actor_id,
            format!("hash join {}", side),
        );

        let cache = ManagedLruCache::unbounded_with_hasher(
            watermark_sequence,
            metrics_info,
            PrecomputedBuildHasher,
        );

        Self {
            inner: cache,
            join_key_data_types,
            null_matched,
            pk_serializer,
            state,
            degree_state,
            need_degree_table,
            pk_contained_in_jk,
            metrics: JoinHashMapMetrics::new(&metrics, actor_id, fragment_id, side, join_table_id),
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state.table.init_epoch(epoch).await?;
        if let Some(degree_state) = &mut self.degree_state {
            degree_state.table.init_epoch(epoch).await?;
        }
        Ok(())
    }
}

impl<K: HashKey, S: StateStore, E: JoinEncoding> JoinHashMapPostCommit<'_, K, S, E> {
    pub async fn post_yield_barrier(
        self,
        vnode_bitmap: Option<Arc<Bitmap>>,
    ) -> StreamExecutorResult<Option<bool>> {
        let cache_may_stale = self.state.post_yield_barrier(vnode_bitmap.clone()).await?;
        if let Some(degree_state) = self.degree_state {
            let _ = degree_state.post_yield_barrier(vnode_bitmap).await?;
        }
        let cache_may_stale = cache_may_stale.map(|(_, cache_may_stale)| cache_may_stale);
        if cache_may_stale.unwrap_or(false) {
            self.inner.clear();
        }
        Ok(cache_may_stale)
    }
}
impl<K: HashKey, S: StateStore, E: JoinEncoding> JoinHashMap<K, S, E> {
    pub fn update_watermark(&mut self, watermark: ScalarImpl) {
        // TODO: remove data in cache.
        self.state.table.update_watermark(watermark.clone());
        if let Some(degree_state) = &mut self.degree_state {
            degree_state.table.update_watermark(watermark);
        }
    }

    /// Take the state for the given `key` out of the hash table and return it. One **MUST** call
    /// `update_state` after some operations to put the state back.
    ///
    /// If the state does not exist in the cache, fetch the remote storage and return. If it still
    /// does not exist in the remote storage, a [`JoinEntryState`] with empty cache will be
    /// returned.
    ///
    /// Note: This will NOT remove anything from remote storage.
    pub fn take_state_opt(&mut self, key: &K) -> CacheResult<E> {
        self.metrics.total_lookup_count += 1;
        if self.inner.contains(key) {
            tracing::trace!("hit cache for join key: {:?}", key);
            // Do not update the LRU statistics here with `peek_mut` since we will put the state
            // back.
            let mut state = self.inner.peek_mut(key).expect("checked contains");
            CacheResult::Hit(state.take())
        } else {
            self.metrics.lookup_miss_count += 1;
            tracing::trace!("miss cache for join key: {:?}", key);
            CacheResult::Miss
        }
    }

    pub async fn flush(
        &mut self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<JoinHashMapPostCommit<'_, K, S, E>> {
        self.metrics.flush();
        let state_post_commit = self.state.table.commit(epoch).await?;
        let degree_state_post_commit = if let Some(degree_state) = &mut self.degree_state {
            Some(degree_state.table.commit(epoch).await?)
        } else {
            None
        };
        Ok(JoinHashMapPostCommit {
            state: state_post_commit,
            degree_state: degree_state_post_commit,
            inner: &mut self.inner,
        })
    }

    pub async fn try_flush(&mut self) -> StreamExecutorResult<()> {
        self.state.table.try_flush().await?;
        if let Some(degree_state) = &mut self.degree_state {
            degree_state.table.try_flush().await?;
        }
        Ok(())
    }

    pub fn insert_handle_degree(
        &mut self,
        key: &K,
        value: JoinRow<impl Row>,
    ) -> StreamExecutorResult<()> {
        if self.need_degree_table {
            self.insert(key, value)
        } else {
            self.insert_row(key, value.row)
        }
    }

    /// Insert a join row
    pub fn insert(&mut self, key: &K, value: JoinRow<impl Row>) -> StreamExecutorResult<()> {
        let pk = self.serialize_pk_from_row(&value.row);

        // TODO(yuhao): avoid this `contains`.
        // https://github.com/risingwavelabs/risingwave/issues/9233
        if self.inner.contains(key) {
            // Update cache
            let mut entry = self.inner.get_mut(key).expect("checked contains");
            entry
                .insert(pk, E::encode(&value))
                .with_context(|| self.state.error_context(&value.row))?;
        } else if self.pk_contained_in_jk {
            // Refill cache when the join key exist in neither cache or storage.
            self.metrics.insert_cache_miss_count += 1;
            let mut entry: JoinEntryState<E> = JoinEntryState::default();
            entry
                .insert(pk, E::encode(&value))
                .with_context(|| self.state.error_context(&value.row))?;
            self.update_state(key, entry.into());
        }

        // Update the flush buffer.
        if let Some(degree_state) = self.degree_state.as_mut() {
            let (row, degree) = value.to_table_rows(
                &self.state.order_key_indices,
                degree_state.degree_inequality_idx,
            );
            self.state.table.insert(row);
            degree_state.table.insert(degree);
        } else {
            self.state.table.insert(value.row);
        }
        Ok(())
    }

    /// Insert a row.
    /// Used when the side does not need to update degree.
    pub fn insert_row(&mut self, key: &K, value: impl Row) -> StreamExecutorResult<()> {
        let join_row = JoinRow::new(&value, 0);
        self.insert(key, join_row)?;
        Ok(())
    }

    pub fn delete_row_in_mem(&mut self, key: &K, value: &impl Row) -> StreamExecutorResult<()> {
        if let Some(mut entry) = self.inner.get_mut(key) {
            let pk = (&value)
                .project(&self.state.pk_indices)
                .memcmp_serialize(&self.pk_serializer);
            entry
                .remove(pk)
                .with_context(|| self.state.error_context(&value))?;
        }
        Ok(())
    }

    pub fn delete_handle_degree(
        &mut self,
        key: &K,
        value: JoinRow<impl Row>,
    ) -> StreamExecutorResult<()> {
        if self.need_degree_table {
            self.delete(key, value)
        } else {
            self.delete_row(key, value.row)
        }
    }

    /// Delete a join row
    pub fn delete(&mut self, key: &K, value: JoinRow<impl Row>) -> StreamExecutorResult<()> {
        self.delete_row_in_mem(key, &value.row)?;

        // If no cache maintained, only update the state table.
        let degree_state = self.degree_state.as_mut().expect("degree table missing");
        let (row, degree) = value.to_table_rows(
            &self.state.order_key_indices,
            degree_state.degree_inequality_idx,
        );
        self.state.table.delete(row);
        degree_state.table.delete(degree);
        Ok(())
    }

    /// Delete a row
    /// Used when the side does not need to update degree.
    pub fn delete_row(&mut self, key: &K, value: impl Row) -> StreamExecutorResult<()> {
        self.delete_row_in_mem(key, &value)?;

        // If no cache maintained, only update the state table.
        self.state.table.delete(value);
        Ok(())
    }

    /// Update a [`JoinEntryState`] into the hash table.
    pub fn update_state(&mut self, key: &K, state: HashValueType<E>) {
        self.inner.put(key.clone(), HashValueWrapper(Some(state)));
    }

    /// Evict the cache.
    pub fn evict(&mut self) {
        self.inner.evict();
    }

    /// Cached entry count for this hash table.
    pub fn entry_count(&self) -> usize {
        self.inner.len()
    }

    pub fn null_matched(&self) -> &K::Bitmap {
        &self.null_matched
    }

    pub fn table_id(&self) -> TableId {
        self.state.table.table_id()
    }

    pub fn join_key_data_types(&self) -> &[DataType] {
        &self.join_key_data_types
    }

    pub fn serialize_pk_from_row(&self, row: impl Row) -> PkType {
        row.project(&self.state.pk_indices)
            .memcmp_serialize(&self.pk_serializer)
    }
}

#[must_use]
pub struct JoinHashMapPostCommit<'a, K: HashKey, S: StateStore, E: JoinEncoding> {
    state: StateTablePostCommit<'a, S>,
    degree_state: Option<StateTablePostCommit<'a, S>>,
    inner: &'a mut JoinHashMapInner<K, E>,
}

use risingwave_common::catalog::TableId;
use risingwave_common_estimate_size::KvSize;
use thiserror::Error;

use super::*;
use crate::executor::prelude::{Stream, try_stream};

/// We manages a `HashMap` in memory for all entries belonging to a join key.
/// When evicted, `cached` does not hold any entries.
///
/// If a `JoinEntryState` exists for a join key, the all records under this
/// join key will be presented in the cache.
#[derive(Default)]
pub struct JoinEntryState<E: JoinEncoding> {
    /// The full copy of the state.
    cached: JoinRowSet<PkType, E::EncodedRow>,
    kv_heap_size: KvSize,
}

impl<E: JoinEncoding> EstimateSize for JoinEntryState<E> {
    fn estimated_heap_size(&self) -> usize {
        // TODO: Add btreemap internal size.
        // https://github.com/risingwavelabs/risingwave/issues/9713
        self.kv_heap_size.size()
    }
}

#[derive(Error, Debug)]
pub enum JoinEntryError {
    #[error("double inserting a join state entry")]
    Occupied,
    #[error("removing a join state entry but it is not in the cache")]
    Remove,
}

impl<E: JoinEncoding> JoinEntryState<E> {
    /// Insert into the cache.
    pub fn insert(
        &mut self,
        key: PkType,
        value: E::EncodedRow,
    ) -> Result<&mut E::EncodedRow, JoinEntryError> {
        let mut removed = false;
        if !enable_strict_consistency() {
            // strict consistency is off, let's remove existing (if any) first
            if let Some(old_value) = self.cached.remove(&key) {
                self.kv_heap_size.sub(&key, &old_value);
                removed = true;
            }
        }

        self.kv_heap_size.add(&key, &value);

        let ret = self.cached.try_insert(key.clone(), value);

        if !enable_strict_consistency() {
            assert!(ret.is_ok(), "we have removed existing entry, if any");
            if removed {
                // if not silent, we should log the error
                consistency_error!(?key, "double inserting a join state entry");
            }
        }

        ret.map_err(|_| JoinEntryError::Occupied)
    }

    /// Delete from the cache.
    pub fn remove(&mut self, pk: PkType) -> Result<(), JoinEntryError> {
        if let Some(value) = self.cached.remove(&pk) {
            self.kv_heap_size.sub(&pk, &value);
            Ok(())
        } else if enable_strict_consistency() {
            Err(JoinEntryError::Remove)
        } else {
            consistency_error!(?pk, "removing a join state entry but it's not in the cache");
            Ok(())
        }
    }

    pub fn get(
        &self,
        pk: &PkType,
        data_types: &[DataType],
    ) -> Option<StreamExecutorResult<JoinRow<E::DecodedRow>>> {
        self.cached
            .get(pk)
            .map(|encoded| encoded.decode(data_types))
    }

    /// Note: the first item in the tuple is the mutable reference to the value in this entry, while
    /// the second item is the decoded value. To mutate the degree, one **must not** forget to apply
    /// the changes to the first item.
    ///
    /// WARNING: Should not change the heap size of `StateValueType` with the mutable reference.
    pub fn values_mut<'a>(
        &'a mut self,
        data_types: &'a [DataType],
    ) -> impl Iterator<
        Item = (
            &'a mut E::EncodedRow,
            StreamExecutorResult<JoinRow<E::DecodedRow>>,
        ),
    > + 'a {
        self.cached.values_mut().map(|encoded| {
            let decoded = encoded.decode(data_types);
            (encoded, decoded)
        })
    }

    pub fn len(&self) -> usize {
        self.cached.len()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::*;
    use risingwave_common::types::ScalarRefImpl;
    use risingwave_common::util::iter_util::ZipEqDebug;

    use super::*;
    use crate::executor::MemoryEncoding;

    fn insert_chunk<E: JoinEncoding>(
        managed_state: &mut JoinEntryState<E>,
        pk_indices: &[usize],
        col_types: &[DataType],
        data_chunk: &DataChunk,
    ) {
        let pk_col_type = pk_indices
            .iter()
            .map(|idx| col_types[*idx].clone())
            .collect_vec();
        let pk_serializer =
            OrderedRowSerde::new(pk_col_type, vec![OrderType::ascending(); pk_indices.len()]);
        for row_ref in data_chunk.rows() {
            let row: OwnedRow = row_ref.into_owned_row();
            let value_indices = (0..row.len() - 1).collect_vec();
            let pk = pk_indices.iter().map(|idx| row[*idx].clone()).collect_vec();
            // Pk is only a `i64` here, so encoding method does not matter.
            let pk = OwnedRow::new(pk)
                .project(&value_indices)
                .memcmp_serialize(&pk_serializer);
            let join_row = JoinRow { row, degree: 0 };
            managed_state.insert(pk, E::encode(&join_row)).unwrap();
        }
    }

    fn check<E: JoinEncoding>(
        managed_state: &mut JoinEntryState<E>,
        col_types: &[DataType],
        col1: &[i64],
        col2: &[i64],
    ) {
        for ((_, matched_row), (d1, d2)) in managed_state
            .values_mut(col_types)
            .zip_eq_debug(col1.iter().zip_eq_debug(col2.iter()))
        {
            let matched_row = matched_row.unwrap();
            assert_eq!(matched_row.row.datum_at(0), Some(ScalarRefImpl::Int64(*d1)));
            assert_eq!(matched_row.row.datum_at(1), Some(ScalarRefImpl::Int64(*d2)));
            assert_eq!(matched_row.degree, 0);
        }
    }

    #[tokio::test]
    async fn test_managed_join_state() {
        let mut managed_state: JoinEntryState<MemoryEncoding> = JoinEntryState::default();
        let col_types = vec![DataType::Int64, DataType::Int64];
        let pk_indices = [0];

        let col1 = [3, 2, 1];
        let col2 = [4, 5, 6];
        let data_chunk1 = DataChunk::from_pretty(
            "I I
             3 4
             2 5
             1 6",
        );

        // `Vec` in state
        insert_chunk::<MemoryEncoding>(&mut managed_state, &pk_indices, &col_types, &data_chunk1);
        check::<MemoryEncoding>(&mut managed_state, &col_types, &col1, &col2);

        // `BtreeMap` in state
        let col1 = [1, 2, 3, 4, 5];
        let col2 = [6, 5, 4, 9, 8];
        let data_chunk2 = DataChunk::from_pretty(
            "I I
             5 8
             4 9",
        );
        insert_chunk(&mut managed_state, &pk_indices, &col_types, &data_chunk2);
        check(&mut managed_state, &col_types, &col1, &col2);
    }
}
