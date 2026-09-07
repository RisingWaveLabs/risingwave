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

//! Types and functions that store or manipulate state/cache inside one single over window
//! partition.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::ops::{Bound, RangeInclusive};

use delta_btree_map::{Change, DeltaBTreeMap};
use educe::Educe;
use futures::StreamExt;
use futures_async_stream::for_await;
use risingwave_common::array::stream_record::Record;
use risingwave_common::config::streaming::OverWindowCachePolicy as CachePolicy;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{Datum, DefaultOrd, ScalarImpl, Sentinelled};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::window_function::{StateKey, WindowStates, create_window_state};
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;

use super::general::{Calls, RowConverter, StateCleaning};
use super::range_cache::{CacheKey, PartitionCache};
use crate::common::table::state_table::{BoxedRowStream, StateTable};
use crate::consistency::{consistency_error, enable_strict_consistency};
use crate::executor::StreamExecutorResult;
use crate::executor::over_window::frame_finder::*;

/// Changes happened in one over window partition.
pub(super) type PartitionDelta = BTreeMap<CacheKey, Change<OwnedRow>>;

#[derive(Default, Debug)]
pub(super) struct OverPartitionStats {
    // stats for range cache operations
    pub lookup_count: u64,
    pub left_miss_count: u64,
    pub right_miss_count: u64,

    // stats for window function state computation
    pub accessed_entry_count: u64,
    pub compute_count: u64,
    pub same_output_count: u64,
}

/// [`AffectedRange`] represents a range of keys that are affected by a delta.
/// The [`CacheKey`] fields are keys in the partition range cache + delta, which is
/// represented by [`DeltaBTreeMap`].
///
/// - `first_curr_key` and `last_curr_key` are the current keys of the first and the last
///   windows affected. They are used to pinpoint the bounds where state needs to be updated.
/// - `first_frame_start` and `last_frame_end` are the frame start and end of the first and
///   the last windows affected. They are used to pinpoint the bounds where state needs to be
///   included for computing the new state.
#[derive(Debug, Educe)]
#[educe(Clone, Copy)]
pub(super) struct AffectedRange<'a> {
    pub first_frame_start: &'a CacheKey,
    pub first_curr_key: &'a CacheKey,
    pub last_curr_key: &'a CacheKey,
    pub last_frame_end: &'a CacheKey,
}

impl<'a> AffectedRange<'a> {
    fn new(
        first_frame_start: &'a CacheKey,
        first_curr_key: &'a CacheKey,
        last_curr_key: &'a CacheKey,
        last_frame_end: &'a CacheKey,
    ) -> Self {
        Self {
            first_frame_start,
            first_curr_key,
            last_curr_key,
            last_frame_end,
        }
    }
}

/// A wrapper of [`PartitionCache`] that provides helper methods to manipulate the cache.
/// By putting this type inside `private` module, we can avoid misuse of the internal fields and
/// methods.
pub(super) struct OverPartition<'a, S: StateStore> {
    deduped_part_key: &'a OwnedRow,
    range_cache: &'a mut PartitionCache,
    cache_policy: CachePolicy,

    calls: &'a Calls,
    row_conv: RowConverter<'a>,

    stats: OverPartitionStats,

    _phantom: PhantomData<S>,
}

const MAGIC_BATCH_SIZE: usize = 512;

/// Maximum number of stale rows to delete from one partition in one round of state cleaning, to
/// bound the number of writes buffered in one epoch when there's a large backlog of stale rows.
const MAX_STALE_ROWS_TO_DELETE_PER_ROUND: usize = 1 << 16;

impl<'a, S: StateStore> OverPartition<'a, S> {
    pub fn new(
        deduped_part_key: &'a OwnedRow,
        cache: &'a mut PartitionCache,
        cache_policy: CachePolicy,
        calls: &'a Calls,
        row_conv: RowConverter<'a>,
    ) -> Self {
        Self {
            deduped_part_key,
            range_cache: cache,
            cache_policy,

            calls,
            row_conv,

            stats: Default::default(),

            _phantom: PhantomData,
        }
    }

    /// Get a summary for the execution happened in the [`OverPartition`] in current round.
    /// This will consume the [`OverPartition`] value itself.
    pub fn summarize(self) -> OverPartitionStats {
        // We may extend this function in the future.
        self.stats
    }

    /// Get the number of cached entries ignoring sentinels.
    pub fn cache_real_len(&self) -> usize {
        self.range_cache.normal_len()
    }

    /// Build changes for the partition, with the given `delta`. Necessary maintenance of the range
    /// cache will be done during this process, like loading rows from the `table` into the cache.
    pub async fn build_changes(
        &mut self,
        table: &StateTable<S>,
        mut delta: PartitionDelta,
    ) -> StreamExecutorResult<(
        BTreeMap<StateKey, Record<OwnedRow>>,
        Option<RangeInclusive<StateKey>>,
    )> {
        let calls = self.calls;
        let input_schema_len = table.get_data_types().len() - calls.len();
        let numbering_only = calls.numbering_only;
        let has_rank = calls.has_rank;

        // return values
        let mut part_changes = BTreeMap::new();
        let mut accessed_range: Option<RangeInclusive<StateKey>> = None;

        // stats
        let mut accessed_entry_count = 0;
        let mut compute_count = 0;
        let mut same_output_count = 0;

        // Find affected ranges, this also ensures that all rows in the affected ranges are loaded into the cache.
        let (part_with_delta, affected_ranges) =
            self.find_affected_ranges(table, &mut delta).await?;

        let snapshot = part_with_delta.snapshot();
        let delta = part_with_delta.delta();
        let last_delta_key = delta.last_key_value().map(|(k, _)| k.as_normal_expect());

        // Generate delete changes first, because deletes are skipped during iteration over
        // `part_with_delta` in the next step.
        for (key, change) in delta {
            if change.is_delete() {
                part_changes.insert(
                    key.as_normal_expect().clone(),
                    Record::Delete {
                        old_row: snapshot.get(key).unwrap().clone(),
                    },
                );
            }
        }

        for AffectedRange {
            first_frame_start,
            first_curr_key,
            last_curr_key,
            last_frame_end,
        } in affected_ranges
        {
            assert!(first_frame_start <= first_curr_key);
            assert!(first_curr_key <= last_curr_key);
            assert!(last_curr_key <= last_frame_end);
            assert!(first_frame_start.is_normal());
            assert!(first_curr_key.is_normal());
            assert!(last_curr_key.is_normal());
            assert!(last_frame_end.is_normal());

            let last_delta_key = last_delta_key.unwrap();

            if let Some(accessed_range) = accessed_range.as_mut() {
                let min_start = first_frame_start
                    .as_normal_expect()
                    .min(accessed_range.start())
                    .clone();
                let max_end = last_frame_end
                    .as_normal_expect()
                    .max(accessed_range.end())
                    .clone();
                *accessed_range = min_start..=max_end;
            } else {
                accessed_range = Some(
                    first_frame_start.as_normal_expect().clone()
                        ..=last_frame_end.as_normal_expect().clone(),
                );
            }

            let mut states =
                WindowStates::new(calls.iter().map(create_window_state).try_collect()?);

            // Populate window states with the affected range of rows.
            {
                let mut cursor = part_with_delta
                    .before(first_frame_start)
                    .expect("first frame start key must exist");

                while let Some((key, row)) = cursor.next() {
                    accessed_entry_count += 1;

                    for (call, state) in calls.iter().zip_eq_fast(states.iter_mut()) {
                        // TODO(rc): batch appending
                        // TODO(rc): append not only the arguments but also the old output for optimization
                        state.append(
                            key.as_normal_expect().clone(),
                            row.project(call.args.val_indices())
                                .into_owned_row()
                                .as_inner()
                                .into(),
                        );
                    }

                    if key == last_frame_end {
                        break;
                    }
                }
            }

            // Slide to the first affected key. We can safely pass in `first_curr_key` here
            // because it definitely exists in the states by the definition of affected range.
            states.just_slide_to(first_curr_key.as_normal_expect())?;
            let mut curr_key_cursor = part_with_delta.before(first_curr_key).unwrap();
            assert_eq!(
                states.curr_key(),
                curr_key_cursor
                    .peek_next()
                    .map(|(k, _)| k)
                    .map(CacheKey::as_normal_expect)
            );

            // Slide and generate changes.
            while let Some((key, row)) = curr_key_cursor.next() {
                let mut should_stop = false;

                let output = states.slide_no_evict_hint()?;
                compute_count += 1;

                let old_output = &row.as_inner()[input_schema_len..];
                if !old_output.is_empty() && old_output == output {
                    same_output_count += 1;

                    if numbering_only {
                        if has_rank {
                            // It's possible that an `Insert` doesn't affect it's ties but affects
                            // all the following rows, so we need to check the `order_key`.
                            if key.as_normal_expect().order_key > last_delta_key.order_key {
                                // there won't be any more changes after this point, we can stop early
                                should_stop = true;
                            }
                        } else if key.as_normal_expect() >= last_delta_key {
                            // there won't be any more changes after this point, we can stop early
                            should_stop = true;
                        }
                    }
                }

                let new_row = OwnedRow::new(
                    row.as_inner()
                        .iter()
                        .take(input_schema_len)
                        .cloned()
                        .chain(output)
                        .collect(),
                );

                if let Some(old_row) = snapshot.get(key).cloned() {
                    // update
                    if old_row != new_row {
                        part_changes.insert(
                            key.as_normal_expect().clone(),
                            Record::Update { old_row, new_row },
                        );
                    }
                } else {
                    // insert
                    part_changes.insert(key.as_normal_expect().clone(), Record::Insert { new_row });
                }

                if should_stop || key == last_curr_key {
                    break;
                }
            }
        }

        self.stats.accessed_entry_count += accessed_entry_count;
        self.stats.compute_count += compute_count;
        self.stats.same_output_count += same_output_count;

        Ok((part_changes, accessed_range))
    }

    /// Write a change record to state table and cache.
    /// This function must be called after finding affected ranges, which means the change records
    /// should never exceed the cached range.
    pub fn write_record(
        &mut self,
        table: &mut StateTable<S>,
        key: StateKey,
        record: Record<OwnedRow>,
    ) {
        table.write_record(record.as_ref());
        match record {
            Record::Insert { new_row } | Record::Update { new_row, .. } => {
                self.range_cache.insert(CacheKey::from(key), new_row);
            }
            Record::Delete { .. } => {
                self.range_cache.remove(&CacheKey::from(key));

                if self.range_cache.normal_len() == 0 && self.range_cache.len() == 1 {
                    // only one sentinel remains, should insert the other
                    self.range_cache
                        .insert(CacheKey::Smallest, OwnedRow::empty());
                    self.range_cache
                        .insert(CacheKey::Largest, OwnedRow::empty());
                }
            }
        }
    }

    /// Clean up stale rows of the partition, i.e., rows whose watermark column value is below the
    /// given `watermark`, except the `n_retain` ones that are closest to the watermark boundary.
    /// See [`StateCleaning`] for why this is safe.
    ///
    /// Returns the number of deleted rows, and whether there may be more stale rows to delete
    /// because of the per-round limit.
    pub async fn clean_stale_rows(
        &mut self,
        table: &mut StateTable<S>,
        cleaning: &StateCleaning,
        watermark: &ScalarImpl,
    ) -> StreamExecutorResult<(usize, bool)> {
        let watermark_ref = watermark.as_scalar_ref_impl();
        let is_stale = |row: &OwnedRow| match row.datum_at(cleaning.watermark_col_idx) {
            Some(value) => value.default_cmp(&watermark_ref).is_lt(),
            None => false, // NULLs are ordered as the largest values, never stale
        };
        // We collect at most this many stale rows, ordered from the farthest to the closest to
        // the watermark boundary. If the limit is reached, there're at least `n_retain` collected
        // stale rows closer to the boundary than the first `MAX_STALE_ROWS_TO_DELETE_PER_ROUND`
        // ones, so it's safe to delete the latter.
        let max_to_collect = MAX_STALE_ROWS_TO_DELETE_PER_ROUND.saturating_add(cleaning.n_retain);

        let cache_covers_stale_end = if cleaning.stale_rows_at_front {
            !self.range_cache.left_is_sentinel()
        } else {
            !self.range_cache.right_is_sentinel()
        };

        let mut stale_rows: Vec<(CacheKey, OwnedRow)> = Vec::new();
        if cache_covers_stale_end {
            // All stale rows are in the cache, no need to scan the table.
            let entries: Box<dyn Iterator<Item = (&CacheKey, &OwnedRow)> + '_> =
                if cleaning.stale_rows_at_front {
                    Box::new(self.range_cache.inner().iter())
                } else {
                    Box::new(self.range_cache.inner().iter().rev())
                };
            stale_rows.extend(
                entries
                    .take_while(|(key, row)| key.is_normal() && is_stale(row))
                    .take(max_to_collect)
                    .map(|(key, row)| (key.clone(), row.clone())),
            );
        } else {
            // The cache doesn't cover the stale end of the partition, scan the table instead.
            let watermark_row = OwnedRow::new(vec![Some(watermark.clone())]);
            let sub_range: (Bound<OwnedRow>, Bound<OwnedRow>) = if cleaning.stale_rows_at_front {
                (Bound::Unbounded, Bound::Excluded(watermark_row))
            } else {
                (Bound::Excluded(watermark_row), Bound::Unbounded)
            };
            let stream: BoxedRowStream<'_> = if cleaning.stale_rows_at_front {
                table
                    .iter_with_prefix(
                        self.deduped_part_key,
                        &sub_range,
                        PrefetchOptions::default(),
                    )
                    .await?
                    .boxed()
            } else {
                table
                    .rev_iter_with_prefix(
                        self.deduped_part_key,
                        &sub_range,
                        PrefetchOptions::default(),
                    )
                    .await?
                    .boxed()
            };

            #[for_await]
            for row in stream {
                let row: OwnedRow = row?.into_owned_row();
                if !is_stale(&row) {
                    break;
                }
                let key = self.row_conv.row_to_state_key(&row)?;
                stale_rows.push((CacheKey::from(key), row));
                if stale_rows.len() >= max_to_collect {
                    break;
                }
            }
        }

        let has_more = stale_rows.len() >= max_to_collect;
        let n_to_delete = if has_more {
            MAX_STALE_ROWS_TO_DELETE_PER_ROUND
        } else {
            stale_rows.len().saturating_sub(cleaning.n_retain)
        };
        for (key, row) in stale_rows.into_iter().take(n_to_delete) {
            table.delete(row);
            self.range_cache.remove(&key);
        }
        if n_to_delete > 0 && self.range_cache.normal_len() == 0 && self.range_cache.len() == 1 {
            // only one sentinel remains, should insert the other
            self.range_cache
                .insert(CacheKey::Smallest, OwnedRow::empty());
            self.range_cache
                .insert(CacheKey::Largest, OwnedRow::empty());
        }

        tracing::trace!(
            partition=?self.deduped_part_key,
            n_deleted=n_to_delete,
            has_more,
            "cleaned stale rows in the partition"
        );

        Ok((n_to_delete, has_more))
    }

    /// Find all ranges in the partition that are affected by the given delta.
    /// The returned ranges are guaranteed to be sorted and non-overlapping. All keys in the ranges
    /// are guaranteed to be cached, which means they should be [`Sentinelled::Normal`]s.
    async fn find_affected_ranges<'s, 'delta>(
        &'s mut self,
        table: &StateTable<S>,
        delta: &'delta mut PartitionDelta,
    ) -> StreamExecutorResult<(
        DeltaBTreeMap<'delta, CacheKey, OwnedRow>,
        Vec<AffectedRange<'delta>>,
    )>
    where
        'a: 'delta,
        's: 'delta,
    {
        if delta.is_empty() {
            return Ok((DeltaBTreeMap::new(self.range_cache.inner(), delta), vec![]));
        }

        self.ensure_delta_in_cache(table, delta).await?;
        let delta = &*delta; // let's make it immutable

        let delta_first = delta.first_key_value().unwrap().0.as_normal_expect();
        let delta_last = delta.last_key_value().unwrap().0.as_normal_expect();

        let range_frame_logical_curr =
            calc_logical_curr_for_range_frames(&self.calls.range_frames, delta_first, delta_last);

        loop {
            // TERMINATEABILITY: `extend_cache_leftward_by_n` and `extend_cache_rightward_by_n` keep
            // pushing the cache to the boundary of current partition. In these two methods, when
            // any side of boundary is reached, the sentinel key will be removed, so finally
            // `Self::find_affected_ranges_readonly` will return `Ok`.

            // SAFETY: Here we shortly borrow the range cache and turn the reference into a
            // `'delta` one to bypass the borrow checker. This is safe because we only return
            // the reference once we don't need to do any further mutation.
            let cache_inner = unsafe { &*(self.range_cache.inner() as *const _) };
            let part_with_delta = DeltaBTreeMap::new(cache_inner, delta);

            self.stats.lookup_count += 1;
            let res = self
                .find_affected_ranges_readonly(part_with_delta, range_frame_logical_curr.as_ref());

            let (need_extend_leftward, need_extend_rightward) = match res {
                Ok(ranges) => return Ok((part_with_delta, ranges)),
                Err(cache_extend_hint) => cache_extend_hint,
            };

            if need_extend_leftward {
                self.stats.left_miss_count += 1;
                tracing::trace!(partition=?self.deduped_part_key, "partition cache left extension triggered");
                let left_most = self
                    .range_cache
                    .first_normal_key()
                    .unwrap_or(delta_first)
                    .clone();
                self.extend_cache_leftward_by_n(table, &left_most).await?;
            }
            if need_extend_rightward {
                self.stats.right_miss_count += 1;
                tracing::trace!(partition=?self.deduped_part_key, "partition cache right extension triggered");
                let right_most = self
                    .range_cache
                    .last_normal_key()
                    .unwrap_or(delta_last)
                    .clone();
                self.extend_cache_rightward_by_n(table, &right_most).await?;
            }
            tracing::trace!(partition=?self.deduped_part_key, "partition cache extended");
        }
    }

    async fn ensure_delta_in_cache(
        &mut self,
        table: &StateTable<S>,
        delta: &mut PartitionDelta,
    ) -> StreamExecutorResult<()> {
        if delta.is_empty() {
            return Ok(());
        }

        let delta_first = delta.first_key_value().unwrap().0.as_normal_expect();
        let delta_last = delta.last_key_value().unwrap().0.as_normal_expect();

        if self.cache_policy.is_full() {
            // ensure everything is in the cache
            self.extend_cache_to_boundary(table).await?;
        } else {
            // TODO(rc): later we should extend cache using `self.calls.super_rows_frame_bounds` and
            // `range_frame_logical_curr` as hints.

            // ensure the cache covers all delta (if possible)
            self.extend_cache_by_range(table, delta_first..=delta_last)
                .await?;
        }

        if !enable_strict_consistency() {
            // in non-strict mode, we should ensure the delta is consistent with the cache
            let cache = self.range_cache.inner();
            delta.retain(|key, change| match &*change {
                Change::Insert(_) => {
                    // this also includes the case of double-insert and ghost-update,
                    // but since we already lost the information, let's just ignore it
                    true
                }
                Change::Delete => {
                    // if the key is not in the cache, it's a ghost-delete
                    let consistent = cache.contains_key(key);
                    if !consistent {
                        consistency_error!(?key, "removing a row with non-existing key");
                    }
                    consistent
                }
            });
        }

        Ok(())
    }

    /// Try to find affected ranges on immutable range cache + delta. If the algorithm reaches
    /// any sentinel node in the cache, which means some entries in the affected range may be
    /// in the state table, it returns an `Err((bool, bool))` to notify the caller that the
    /// left side or the right side or both sides of the cache should be extended.
    ///
    /// TODO(rc): Currently at most one range will be in the result vector. Ideally we should
    /// recognize uncontinuous changes in the delta and find multiple ranges, but that will be
    /// too complex for now.
    fn find_affected_ranges_readonly<'delta>(
        &self,
        part_with_delta: DeltaBTreeMap<'delta, CacheKey, OwnedRow>,
        range_frame_logical_curr: Option<&(Sentinelled<Datum>, Sentinelled<Datum>)>,
    ) -> std::result::Result<Vec<AffectedRange<'delta>>, (bool, bool)> {
        if part_with_delta.first_key().is_none() {
            // nothing is left after applying the delta, meaning all entries are deleted
            return Ok(vec![]);
        }

        let delta_first_key = part_with_delta.delta().first_key_value().unwrap().0;
        let delta_last_key = part_with_delta.delta().last_key_value().unwrap().0;
        let cache_key_pk_len = delta_first_key.as_normal_expect().pk.len();

        if part_with_delta.snapshot().is_empty() {
            // all existing keys are inserted in the delta
            return Ok(vec![AffectedRange::new(
                delta_first_key,
                delta_first_key,
                delta_last_key,
                delta_last_key,
            )]);
        }

        let first_key = part_with_delta.first_key().unwrap();
        let last_key = part_with_delta.last_key().unwrap();

        let first_curr_key = if self.calls.end_is_unbounded || delta_first_key == first_key {
            // If the frame end is unbounded, or, the first key is in delta, then the frame corresponding
            // to the first key is always affected.
            first_key
        } else {
            let mut key = find_first_curr_for_rows_frame(
                &self.calls.super_rows_frame_bounds,
                part_with_delta,
                delta_first_key,
            );

            if let Some((logical_first_curr, _)) = range_frame_logical_curr {
                let logical_curr = logical_first_curr.as_normal_expect(); // otherwise should go `end_is_unbounded` branch
                let new_key = find_left_for_range_frames(
                    &self.calls.range_frames,
                    part_with_delta,
                    logical_curr,
                    cache_key_pk_len,
                );
                key = std::cmp::min(key, new_key);
            }

            key
        };

        let last_curr_key = if self.calls.start_is_unbounded || delta_last_key == last_key {
            // similar to `first_curr_key`
            last_key
        } else {
            let mut key = find_last_curr_for_rows_frame(
                &self.calls.super_rows_frame_bounds,
                part_with_delta,
                delta_last_key,
            );

            if let Some((_, logical_last_curr)) = range_frame_logical_curr {
                let logical_curr = logical_last_curr.as_normal_expect(); // otherwise should go `start_is_unbounded` branch
                let new_key = find_right_for_range_frames(
                    &self.calls.range_frames,
                    part_with_delta,
                    logical_curr,
                    cache_key_pk_len,
                );
                key = std::cmp::max(key, new_key);
            }

            key
        };

        {
            // We quickly return if there's any sentinel in `[first_curr_key, last_curr_key]`,
            // just for the sake of simplicity.
            let mut need_extend_leftward = false;
            let mut need_extend_rightward = false;
            for key in [first_curr_key, last_curr_key] {
                if key.is_smallest() {
                    need_extend_leftward = true;
                } else if key.is_largest() {
                    need_extend_rightward = true;
                }
            }
            if need_extend_leftward || need_extend_rightward {
                return Err((need_extend_leftward, need_extend_rightward));
            }
        }

        // From now on we definitely have two normal `curr_key`s.

        if first_curr_key > last_curr_key {
            // Note that we cannot move the this check before the above block, because for example,
            // if the range cache contains `[Smallest, 5, Largest]`, and the delta contains only
            // `Delete 5`, the frame is `RANGE BETWEEN CURRENT ROW AND CURRENT ROW`, then
            // `first_curr_key` will be `Largest`, `last_curr_key` will be `Smallest`, in this case
            // there may be some other entries with order value `5` in the table, which should be
            // *affected*.
            return Ok(vec![]);
        }

        let range_frame_logical_boundary = calc_logical_boundary_for_range_frames(
            &self.calls.range_frames,
            first_curr_key.as_normal_expect(),
            last_curr_key.as_normal_expect(),
        );

        let first_frame_start = if self.calls.start_is_unbounded || first_curr_key == first_key {
            // If the frame start is unbounded, or, the first curr key is the first key, then the first key
            // always need to be included in the affected range.
            first_key
        } else {
            let mut key = find_frame_start_for_rows_frame(
                &self.calls.super_rows_frame_bounds,
                part_with_delta,
                first_curr_key,
            );

            if let Some((logical_first_start, _)) = range_frame_logical_boundary.as_ref() {
                let logical_boundary = logical_first_start.as_normal_expect(); // otherwise should go `end_is_unbounded` branch
                let new_key = find_left_for_range_frames(
                    &self.calls.range_frames,
                    part_with_delta,
                    logical_boundary,
                    cache_key_pk_len,
                );
                key = std::cmp::min(key, new_key);
            }

            key
        };
        assert!(first_frame_start <= first_curr_key);

        let last_frame_end = if self.calls.end_is_unbounded || last_curr_key == last_key {
            // similar to `first_frame_start`
            last_key
        } else {
            let mut key = find_frame_end_for_rows_frame(
                &self.calls.super_rows_frame_bounds,
                part_with_delta,
                last_curr_key,
            );

            if let Some((_, logical_last_end)) = range_frame_logical_boundary.as_ref() {
                let logical_boundary = logical_last_end.as_normal_expect(); // otherwise should go `end_is_unbounded` branch
                let new_key = find_right_for_range_frames(
                    &self.calls.range_frames,
                    part_with_delta,
                    logical_boundary,
                    cache_key_pk_len,
                );
                key = std::cmp::max(key, new_key);
            }

            key
        };
        assert!(last_frame_end >= last_curr_key);

        let mut need_extend_leftward = false;
        let mut need_extend_rightward = false;
        for key in [
            first_curr_key,
            last_curr_key,
            first_frame_start,
            last_frame_end,
        ] {
            if key.is_smallest() {
                need_extend_leftward = true;
            } else if key.is_largest() {
                need_extend_rightward = true;
            }
        }

        if need_extend_leftward || need_extend_rightward {
            Err((need_extend_leftward, need_extend_rightward))
        } else {
            Ok(vec![AffectedRange::new(
                first_frame_start,
                first_curr_key,
                last_curr_key,
                last_frame_end,
            )])
        }
    }

    async fn extend_cache_to_boundary(
        &mut self,
        table: &StateTable<S>,
    ) -> StreamExecutorResult<()> {
        if self.range_cache.normal_len() == self.range_cache.len() {
            // no sentinel in the cache, meaning we already cached all entries of this partition
            return Ok(());
        }

        tracing::trace!(partition=?self.deduped_part_key, "loading the whole partition into cache");

        let mut new_cache = PartitionCache::new_without_sentinels(); // shouldn't use `new` here because we are extending to boundary
        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
        let table_iter = table
            .iter_with_prefix(self.deduped_part_key, sub_range, PrefetchOptions::default())
            .await?;

        #[for_await]
        for row in table_iter {
            let row: OwnedRow = row?.into_owned_row();
            new_cache.insert(self.row_conv.row_to_state_key(&row)?.into(), row);
        }
        *self.range_cache = new_cache;

        Ok(())
    }

    /// Try to load the given range of entries from table into cache.
    /// When the function returns, it's guaranteed that there's no entry in the table that is within
    /// the given range but not in the cache.
    async fn extend_cache_by_range(
        &mut self,
        table: &StateTable<S>,
        range: RangeInclusive<&StateKey>,
    ) -> StreamExecutorResult<()> {
        if self.range_cache.normal_len() == self.range_cache.len() {
            // no sentinel in the cache, meaning we already cached all entries of this partition
            return Ok(());
        }
        assert!(self.range_cache.len() >= 2);

        let cache_first_normal_key = self.range_cache.first_normal_key();
        let cache_last_normal_key = self.range_cache.last_normal_key();

        if cache_first_normal_key.is_some() && *range.end() < cache_first_normal_key.unwrap()
            || cache_last_normal_key.is_some() && *range.start() > cache_last_normal_key.unwrap()
        {
            // completely not overlapping, for the sake of simplicity, we re-init the cache
            tracing::debug!(
                partition=?self.deduped_part_key,
                cache_first=?cache_first_normal_key,
                cache_last=?cache_last_normal_key,
                range=?range,
                "modified range is completely non-overlapping with the cached range, re-initializing the cache"
            );
            *self.range_cache = PartitionCache::new();
        }

        if self.cache_real_len() == 0 {
            // no normal entry in the cache, just load the given range
            let table_sub_range = (
                Bound::Included(self.row_conv.state_key_to_table_sub_pk(range.start())?),
                Bound::Included(self.row_conv.state_key_to_table_sub_pk(range.end())?),
            );
            tracing::debug!(
                partition=?self.deduped_part_key,
                table_sub_range=?table_sub_range,
                "cache is empty, just loading the given range"
            );
            return self
                .extend_cache_by_range_inner(table, table_sub_range)
                .await;
        }

        let cache_real_first_key = self
            .range_cache
            .first_normal_key()
            .expect("cache real len is not 0");
        if self.range_cache.left_is_sentinel() && *range.start() < cache_real_first_key {
            // extend leftward only if there's smallest sentinel
            let table_sub_range = (
                Bound::Included(self.row_conv.state_key_to_table_sub_pk(range.start())?),
                Bound::Excluded(
                    self.row_conv
                        .state_key_to_table_sub_pk(cache_real_first_key)?,
                ),
            );
            tracing::trace!(
                partition=?self.deduped_part_key,
                table_sub_range=?table_sub_range,
                "loading the left half of given range"
            );
            self.extend_cache_by_range_inner(table, table_sub_range)
                .await?;
        }

        let cache_real_last_key = self
            .range_cache
            .last_normal_key()
            .expect("cache real len is not 0");
        if self.range_cache.right_is_sentinel() && *range.end() > cache_real_last_key {
            // extend rightward only if there's largest sentinel
            let table_sub_range = (
                Bound::Excluded(
                    self.row_conv
                        .state_key_to_table_sub_pk(cache_real_last_key)?,
                ),
                Bound::Included(self.row_conv.state_key_to_table_sub_pk(range.end())?),
            );
            tracing::trace!(
                partition=?self.deduped_part_key,
                table_sub_range=?table_sub_range,
                "loading the right half of given range"
            );
            self.extend_cache_by_range_inner(table, table_sub_range)
                .await?;
        }

        // prefetch rows before the start of the range
        self.extend_cache_leftward_by_n(table, range.start())
            .await?;

        // prefetch rows after the end of the range
        self.extend_cache_rightward_by_n(table, range.end()).await
    }

    async fn extend_cache_leftward_by_n(
        &mut self,
        table: &StateTable<S>,
        hint_key: &StateKey,
    ) -> StreamExecutorResult<()> {
        if self.range_cache.normal_len() == self.range_cache.len() {
            // no sentinel in the cache, meaning we already cached all entries of this partition
            return Ok(());
        }
        assert!(self.range_cache.len() >= 2);

        let left_second = {
            let mut iter = self.range_cache.inner().iter();
            let left_first = iter.next().unwrap().0;
            if left_first.is_normal() {
                // the leftside already reaches the beginning of this partition in the table
                return Ok(());
            }
            iter.next().unwrap().0
        };
        let range_to_exclusive = match left_second {
            CacheKey::Normal(smallest_in_cache) => smallest_in_cache,
            CacheKey::Largest => hint_key, // no normal entry in the cache
            _ => unreachable!(),
        }
        .clone();

        self.extend_cache_leftward_by_n_inner(table, &range_to_exclusive)
            .await?;

        if self.cache_real_len() == 0 {
            // Cache was empty, and extending leftward didn't add anything to the cache, but we
            // can't just remove the smallest sentinel, we must also try extending rightward.
            self.extend_cache_rightward_by_n_inner(table, hint_key)
                .await?;
            if self.cache_real_len() == 0 {
                // still empty, meaning the table is empty
                self.range_cache.remove(&CacheKey::Smallest);
                self.range_cache.remove(&CacheKey::Largest);
            }
        }

        Ok(())
    }

    async fn extend_cache_rightward_by_n(
        &mut self,
        table: &StateTable<S>,
        hint_key: &StateKey,
    ) -> StreamExecutorResult<()> {
        if self.range_cache.normal_len() == self.range_cache.len() {
            // no sentinel in the cache, meaning we already cached all entries of this partition
            return Ok(());
        }
        assert!(self.range_cache.len() >= 2);

        let right_second = {
            let mut iter = self.range_cache.inner().iter();
            let right_first = iter.next_back().unwrap().0;
            if right_first.is_normal() {
                // the rightside already reaches the end of this partition in the table
                return Ok(());
            }
            iter.next_back().unwrap().0
        };
        let range_from_exclusive = match right_second {
            CacheKey::Normal(largest_in_cache) => largest_in_cache,
            CacheKey::Smallest => hint_key, // no normal entry in the cache
            _ => unreachable!(),
        }
        .clone();

        self.extend_cache_rightward_by_n_inner(table, &range_from_exclusive)
            .await?;

        if self.cache_real_len() == 0 {
            // Cache was empty, and extending rightward didn't add anything to the cache, but we
            // can't just remove the smallest sentinel, we must also try extending leftward.
            self.extend_cache_leftward_by_n_inner(table, hint_key)
                .await?;
            if self.cache_real_len() == 0 {
                // still empty, meaning the table is empty
                self.range_cache.remove(&CacheKey::Smallest);
                self.range_cache.remove(&CacheKey::Largest);
            }
        }

        Ok(())
    }

    async fn extend_cache_by_range_inner(
        &mut self,
        table: &StateTable<S>,
        table_sub_range: (Bound<impl Row>, Bound<impl Row>),
    ) -> StreamExecutorResult<()> {
        let stream = table
            .iter_with_prefix(
                self.deduped_part_key,
                &table_sub_range,
                PrefetchOptions::default(),
            )
            .await?;

        #[for_await]
        for row in stream {
            let row: OwnedRow = row?.into_owned_row();
            let key = self.row_conv.row_to_state_key(&row)?;
            self.range_cache.insert(CacheKey::from(key), row);
        }

        Ok(())
    }

    async fn extend_cache_leftward_by_n_inner(
        &mut self,
        table: &StateTable<S>,
        range_to_exclusive: &StateKey,
    ) -> StreamExecutorResult<()> {
        let mut n_extended = 0usize;
        {
            let sub_range = (
                Bound::<OwnedRow>::Unbounded,
                Bound::Excluded(
                    self.row_conv
                        .state_key_to_table_sub_pk(range_to_exclusive)?,
                ),
            );
            let rev_stream = table
                .rev_iter_with_prefix(
                    self.deduped_part_key,
                    &sub_range,
                    PrefetchOptions::default(),
                )
                .await?;

            #[for_await]
            for row in rev_stream {
                let row: OwnedRow = row?.into_owned_row();

                let key = self.row_conv.row_to_state_key(&row)?;
                self.range_cache.insert(CacheKey::from(key), row);

                n_extended += 1;
                if n_extended == MAGIC_BATCH_SIZE {
                    break;
                }
            }
        }

        if n_extended < MAGIC_BATCH_SIZE && self.cache_real_len() > 0 {
            // we reached the beginning of this partition in the table
            self.range_cache.remove(&CacheKey::Smallest);
        }

        Ok(())
    }

    async fn extend_cache_rightward_by_n_inner(
        &mut self,
        table: &StateTable<S>,
        range_from_exclusive: &StateKey,
    ) -> StreamExecutorResult<()> {
        let mut n_extended = 0usize;
        {
            let sub_range = (
                Bound::Excluded(
                    self.row_conv
                        .state_key_to_table_sub_pk(range_from_exclusive)?,
                ),
                Bound::<OwnedRow>::Unbounded,
            );
            let stream = table
                .iter_with_prefix(
                    self.deduped_part_key,
                    &sub_range,
                    PrefetchOptions::default(),
                )
                .await?;

            #[for_await]
            for row in stream {
                let row: OwnedRow = row?.into_owned_row();

                let key = self.row_conv.row_to_state_key(&row)?;
                self.range_cache.insert(CacheKey::from(key), row);

                n_extended += 1;
                if n_extended == MAGIC_BATCH_SIZE {
                    break;
                }
            }
        }

        if n_extended < MAGIC_BATCH_SIZE && self.cache_real_len() > 0 {
            // we reached the end of this partition in the table
            self.range_cache.remove(&CacheKey::Largest);
        }

        Ok(())
    }
}
