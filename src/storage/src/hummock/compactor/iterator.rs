// Copyright 2022 RisingWave Labs
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
use std::collections::HashSet;
use std::ops::Range;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, atomic};
use std::time::Instant;

use await_tree::{InstrumentAwait, SpanExt};
use fail::fail_point;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::KeyComparator;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::sstable_info::SstableInfo;

use crate::hummock::block_stream::BlockDataStream;
use crate::hummock::compactor::task_progress::TaskProgress;
use crate::hummock::iterator::{Forward, HummockIterator, ValueMeta};
use crate::hummock::sstable::SstableMetaHandle;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::value::HummockValue;
use crate::hummock::{BlockHolder, BlockIterator, HummockResult, TableHolder};
use crate::monitor::StoreLocalStatistic;

const PROGRESS_KEY_INTERVAL: usize = 100;

/// Iterates over the KV-pairs of an SST while downloading it.
/// `SstableStreamIterator` encapsulates operations on `sstables`, constructing block streams and accessing the corresponding data via `block_metas`.
///  Note that a `block_meta` does not necessarily correspond to the entire sstable, but rather to a subset, which is documented via the `block_idx`.
pub struct SstableStreamIterator {
    sstable_store: SstableStoreRef,
    sstable: TableHolder,
    /// The range of block metas to iterate over
    block_metas_range: Range<usize>,
    /// The downloading stream.
    block_stream: Option<BlockDataStream>,

    /// Iterates over the KV-pairs of the current block.
    block_iter: Option<BlockIterator>,

    /// Index of the current block within the range.
    block_idx: usize,

    /// Counts the time used for IO.
    stats_ptr: Arc<AtomicU64>,

    /// For key sanity check of divided SST and debugging
    sstable_info: SstableInfo,

    /// Table ids used to decide which blocks should be read from this SST.
    read_table_ids: HashSet<TableId>,
    task_progress: Arc<TaskProgress>,
    io_retry_times: usize,
    max_io_retry_times: usize,

    // key range cache
    key_range_left: FullKey<Vec<u8>>,
    key_range_right: FullKey<Vec<u8>>,
    key_range_right_exclusive: bool,
}

impl SstableStreamIterator {
    // We have to handle two internal iterators.
    //   `block_stream`: iterates over the blocks of the table.
    //     `block_iter`: iterates over the KV-pairs of the current block.
    // These iterators work in different ways.

    // BlockIterator works as follows: After new(), we call seek(). That brings us
    // to the first element. Calling next() then brings us to the second element and does not
    // return anything.

    // BlockStream follows a different approach. After new(), we do not seek, instead next()
    // returns the first value.

    /// Initialises a new [`SstableStreamIterator`] which iterates over the given [`BlockDataStream`].
    /// The iterator reads at most `max_block_count` from the stream.
    pub fn new(
        sstable: TableHolder,
        block_metas_range: Range<usize>,
        sstable_info: SstableInfo,
        stats: &StoreLocalStatistic,
        task_progress: Arc<TaskProgress>,
        sstable_store: SstableStoreRef,
        max_io_retry_times: usize,
    ) -> Self {
        let read_table_ids = HashSet::from_iter(sstable_info.table_ids.iter().copied());
        // Further filter the block_metas_range with sstable_info.key_range
        // This is necessary when the SST is split into multiple SstableInfo with different key ranges
        let meta_handle = SstableMetaHandle::v2(&sstable);
        let block_metas_range = filter_block_metas_with_handle(
            &meta_handle,
            block_metas_range,
            &read_table_ids,
            sstable_info.key_range.clone(),
        );

        let key_range_left = FullKey::decode(&sstable_info.key_range.left).to_vec();
        let key_range_right = FullKey::decode(&sstable_info.key_range.right).to_vec();
        let key_range_right_exclusive = sstable_info.key_range.right_exclusive;

        Self {
            block_stream: None,
            block_iter: None,
            sstable,
            block_metas_range,
            block_idx: 0,
            stats_ptr: stats.remote_io_time.clone(),
            read_table_ids,
            sstable_info,
            sstable_store,
            task_progress,
            io_retry_times: 0,
            max_io_retry_times,
            key_range_left,
            key_range_right,
            key_range_right_exclusive,
        }
    }

    /// Returns the number of blocks in this iterator.
    #[inline]
    fn block_count(&self) -> usize {
        self.block_metas_range.len()
    }

    async fn create_stream(&mut self) -> HummockResult<()> {
        let block_metas = SstableMetaHandle::v2(&self.sstable).block_metas_vec(
            self.block_metas_range.start + self.block_idx..self.block_metas_range.end,
        );
        let block_stream = self
            .sstable_store
            .get_stream_for_block_metas(self.sstable_info.object_id, block_metas)
            .instrument_await("stream_iter_get_stream".verbose())
            .await?;
        self.block_stream = Some(block_stream);
        Ok(())
    }

    async fn prune_from_valid_block_iter(&mut self) -> HummockResult<()> {
        while let Some(block_iter) = self.block_iter.as_mut() {
            if self.read_table_ids.contains(&block_iter.table_id()) {
                return Ok(());
            } else {
                self.next_block().await?;
            }
        }
        Ok(())
    }

    /// Initialises the iterator by moving it to the first KV-pair in the stream's first block where
    /// key >= `seek_key`. If that block does not contain such a KV-pair, the iterator continues to
    /// the first KV-pair of the next block. If `seek_key` is not given, the iterator will move to
    /// the very first KV-pair of the stream's first block.
    pub async fn seek(&mut self, seek_key: Option<FullKey<&[u8]>>) -> HummockResult<()> {
        // Load first block.
        self.next_block().await?;

        // We assume that a block always contains at least one KV pair. Subsequently, if
        // `next_block()` loads a new block (i.e., `block_iter` is not `None`), then `block_iter` is
        // also valid and pointing on the block's first KV-pair.

        let seek_key = if let Some(seek_key) = seek_key {
            if seek_key.cmp(&self.key_range_left.to_ref()).is_lt() {
                Some(self.key_range_left.to_ref())
            } else {
                Some(seek_key)
            }
        } else {
            Some(self.key_range_left.to_ref())
        };

        if let (Some(block_iter), Some(seek_key)) = (self.block_iter.as_mut(), seek_key) {
            block_iter.seek(seek_key);

            if !block_iter.is_valid() {
                // `seek_key` is larger than everything in the first block.
                self.next_block().await?;
            }
        }

        self.prune_from_valid_block_iter().await?;
        Ok(())
    }

    /// Loads a new block, creates a new iterator for it, and stores that iterator in
    /// `self.block_iter`. The created iterator points to the block's first KV-pair. If the end of
    /// the stream is reached or `self.remaining_blocks` is zero, then the function sets
    /// `self.block_iter` to `None`.
    async fn next_block(&mut self) -> HummockResult<()> {
        // Check if we want and if we can load the next block.
        let now = Instant::now();
        let _time_stat = scopeguard::guard(self.stats_ptr.clone(), |stats_ptr: Arc<AtomicU64>| {
            let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
            stats_ptr.fetch_add(add as u64, atomic::Ordering::Relaxed);
        });
        if self.block_idx < self.block_count() {
            loop {
                let ret = match &mut self.block_stream {
                    Some(block_stream) => block_stream.next_block().await,
                    None => {
                        self.create_stream().await?;
                        continue;
                    }
                };
                match ret {
                    Ok(Some(block)) => {
                        let mut block_iter =
                            BlockIterator::new(BlockHolder::from_owned_block(block));
                        block_iter.seek_to_first();
                        self.block_idx += 1;
                        self.block_iter = Some(block_iter);
                        return Ok(());
                    }
                    Ok(None) => break,
                    Err(e) => {
                        if !e.is_object_error() || !self.need_recreate_io_stream() {
                            return Err(e);
                        }
                        self.block_stream.take();
                        self.io_retry_times += 1;
                        fail_point!("create_stream_err");

                        tracing::warn!(
                            "retry create stream for sstable {} times, sstinfo={}",
                            self.io_retry_times,
                            self.sst_debug_info()
                        );
                    }
                }
            }
        }
        self.block_idx = self.block_count();
        self.block_iter = None;

        Ok(())
    }

    /// Moves to the next KV-pair in the table. Assumes that the current position is valid. Even if
    /// the next position is invalid, the function returns `Ok(())`.
    ///
    /// Do not use `next()` to initialise the iterator (i.e. do not use it to find the first
    /// KV-pair). Instead, use `seek()`. Afterwards, use `next()` to reach the second KV-pair and
    /// onwards.
    pub async fn next(&mut self) -> HummockResult<()> {
        if !self.is_valid() {
            return Ok(());
        }

        let block_iter = self.block_iter.as_mut().expect("no block iter");
        block_iter.next();
        if !block_iter.is_valid() {
            self.next_block().await?;
            self.prune_from_valid_block_iter().await?;
        }

        if !self.is_valid() {
            return Ok(());
        }

        // Check if we need to skip the block.
        let key = self
            .block_iter
            .as_ref()
            .unwrap_or_else(|| panic!("no block iter sstinfo={}", self.sst_debug_info()))
            .key();

        if self.exceed_key_range_right(key) {
            self.block_iter = None;
        }

        Ok(())
    }

    pub fn key(&self) -> FullKey<&[u8]> {
        let key = self
            .block_iter
            .as_ref()
            .unwrap_or_else(|| panic!("no block iter sstinfo={}", self.sst_debug_info()))
            .key();

        assert!(
            !self.exceed_key_range_left(key),
            "key {:?} key_range_left {:?}",
            key,
            self.key_range_left.to_ref()
        );

        assert!(
            !self.exceed_key_range_right(key),
            "key {:?} key_range_right {:?} key_range_right_exclusive {}",
            key,
            self.key_range_right.to_ref(),
            self.key_range_right_exclusive
        );

        key
    }

    pub fn value(&self) -> HummockValue<&[u8]> {
        let raw_value = self
            .block_iter
            .as_ref()
            .unwrap_or_else(|| panic!("no block iter sstinfo={}", self.sst_debug_info()))
            .value();
        HummockValue::from_slice(raw_value)
            .unwrap_or_else(|_| panic!("decode error sstinfo={}", self.sst_debug_info()))
    }

    pub fn is_valid(&self) -> bool {
        // True iff block_iter exists and is valid.
        self.block_iter.as_ref().is_some_and(|i| i.is_valid())
    }

    fn sst_debug_info(&self) -> String {
        format!(
            "object_id={}, sst_id={}, meta_offset={}, table_ids={:?}",
            self.sstable_info.object_id,
            self.sstable_info.sst_id,
            self.sstable_info.meta_offset,
            self.sstable_info.table_ids
        )
    }

    fn need_recreate_io_stream(&self) -> bool {
        self.io_retry_times < self.max_io_retry_times
    }

    fn exceed_key_range_left(&self, key: FullKey<&[u8]>) -> bool {
        key.cmp(&self.key_range_left.to_ref()).is_lt()
    }

    fn exceed_key_range_right(&self, key: FullKey<&[u8]>) -> bool {
        if self.key_range_right_exclusive {
            key.cmp(&self.key_range_right.to_ref()).is_ge()
        } else {
            key.cmp(&self.key_range_right.to_ref()).is_gt()
        }
    }
}

impl Drop for SstableStreamIterator {
    fn drop(&mut self) {
        self.task_progress.dec_num_pending_read_io()
    }
}

/// Iterates over the KV-pairs of a given list of SSTs. The key-ranges of these SSTs are assumed to
/// be consecutive and non-overlapping.
pub struct ConcatSstableIterator {
    /// **CAUTION:** `key_range` is used for optimization. It doesn't guarantee value returned by
    /// the iterator is in this range.
    key_range: KeyRange,

    /// The iterator of the current table.
    sstable_iter: Option<SstableStreamIterator>,

    /// Current table index.
    cur_idx: usize,

    /// All non-overlapping tables.
    sstables: Vec<SstableInfo>,

    sstable_store: SstableStoreRef,

    stats: StoreLocalStatistic,
    task_progress: Arc<TaskProgress>,
    max_io_retry_times: usize,
}

impl ConcatSstableIterator {
    /// Caller should make sure that `tables` are non-overlapping,
    /// arranged in ascending order when it serves as a forward iterator,
    /// and arranged in descending order when it serves as a backward iterator.
    pub fn new(
        sst_infos: Vec<SstableInfo>,
        key_range: KeyRange,
        sstable_store: SstableStoreRef,
        task_progress: Arc<TaskProgress>,
        max_io_retry_times: usize,
    ) -> Self {
        Self {
            key_range,
            sstable_iter: None,
            cur_idx: 0,
            sstables: sst_infos,
            sstable_store,
            task_progress,
            stats: StoreLocalStatistic::default(),
            max_io_retry_times,
        }
    }

    #[cfg(test)]
    pub fn for_test(
        sst_infos: Vec<SstableInfo>,
        key_range: KeyRange,
        sstable_store: SstableStoreRef,
    ) -> Self {
        Self::new(
            sst_infos,
            key_range,
            sstable_store,
            Arc::new(TaskProgress::default()),
            0,
        )
    }

    /// Resets the iterator, loads the specified SST, and seeks in that SST to `seek_key` if given.
    async fn seek_idx(
        &mut self,
        idx: usize,
        seek_key: Option<FullKey<&[u8]>>,
    ) -> HummockResult<()> {
        self.sstable_iter.take();
        let mut seek_key: Option<FullKey<&[u8]>> = match (seek_key, self.key_range.left.is_empty())
        {
            (Some(seek_key), false) => match seek_key.cmp(&FullKey::decode(&self.key_range.left)) {
                Ordering::Less | Ordering::Equal => Some(FullKey::decode(&self.key_range.left)),
                Ordering::Greater => Some(seek_key),
            },
            (Some(seek_key), true) => Some(seek_key),
            (None, true) => None,
            (None, false) => Some(FullKey::decode(&self.key_range.left)),
        };

        self.cur_idx = idx;
        while self.cur_idx < self.sstables.len() {
            let table_info = &self.sstables[self.cur_idx];
            let read_table_ids = HashSet::from_iter(table_info.table_ids.iter().copied());
            if read_table_ids.is_empty() {
                self.cur_idx += 1;
                seek_key = None;
                continue;
            }
            let sstable = self
                .sstable_store
                .sstable(table_info, &mut self.stats)
                .instrument_await("stream_iter_sstable".verbose())
                .await?;

            let filter_key_range = match seek_key {
                Some(seek_key) => {
                    KeyRange::new(seek_key.encode().into(), self.key_range.right.clone())
                }
                None => self.key_range.clone(),
            };

            let meta_handle = SstableMetaHandle::v2(&sstable);
            let block_metas_range = filter_block_metas_with_handle(
                &meta_handle,
                0..meta_handle.block_count(),
                &read_table_ids,
                filter_key_range,
            );

            let mut found = true;
            if block_metas_range.is_empty() {
                found = false;
            } else {
                self.task_progress.inc_num_pending_read_io();
                let mut sstable_iter = SstableStreamIterator::new(
                    sstable,
                    block_metas_range,
                    table_info.clone(),
                    &self.stats,
                    self.task_progress.clone(),
                    self.sstable_store.clone(),
                    self.max_io_retry_times,
                );
                sstable_iter.seek(seek_key).await?;

                if sstable_iter.is_valid() {
                    self.sstable_iter = Some(sstable_iter);
                } else {
                    found = false;
                }
            }

            if found {
                return Ok(());
            } else {
                self.cur_idx += 1;
                seek_key = None;
            }
        }
        Ok(())
    }
}

impl HummockIterator for ConcatSstableIterator {
    type Direction = Forward;

    async fn next(&mut self) -> HummockResult<()> {
        let sstable_iter = self.sstable_iter.as_mut().expect("no table iter");

        // Does just calling `next()` suffice?
        sstable_iter.next().await?;
        if sstable_iter.is_valid() {
            Ok(())
        } else {
            // No, seek to next table.
            self.seek_idx(self.cur_idx + 1, None).await?;
            Ok(())
        }
    }

    fn key(&self) -> FullKey<&[u8]> {
        self.sstable_iter.as_ref().expect("no table iter").key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.sstable_iter.as_ref().expect("no table iter").value()
    }

    fn is_valid(&self) -> bool {
        self.sstable_iter.as_ref().is_some_and(|i| i.is_valid())
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.seek_idx(0, None).await
    }

    /// Resets the iterator and seeks to the first position where the stored key >= `key`.
    async fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> HummockResult<()> {
        let seek_key = if self.key_range.left.is_empty() {
            key
        } else {
            match key.cmp(&FullKey::decode(&self.key_range.left)) {
                Ordering::Less | Ordering::Equal => FullKey::decode(&self.key_range.left),
                Ordering::Greater => key,
            }
        };
        let table_idx = self.sstables.partition_point(|table| {
            // We use the maximum key of an SST for the search. That way, we guarantee that the
            // resulting SST contains either that key or the next-larger KV-pair. Subsequently,
            // we avoid calling `seek_idx()` twice if the determined SST does not contain `key`.

            // Note that we need to use `<` instead of `<=` to ensure that all keys in an SST
            // (including its max. key) produce the same search result.
            let max_sst_key = &table.key_range.right;
            FullKey::decode(max_sst_key).cmp(&seek_key) == Ordering::Less
        });

        self.seek_idx(table_idx, Some(key)).await
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        stats.add(&self.stats)
    }

    fn value_meta(&self) -> ValueMeta {
        let iter = self.sstable_iter.as_ref().expect("no table iter");
        // sstable_iter's block_idx must have advanced at least one.
        // See SstableStreamIterator::next_block.
        assert!(iter.block_idx >= 1);
        // block_idx is relative to block_metas_range.start, so we need to add it back
        let absolute_block_idx = iter.block_metas_range.start + iter.block_idx - 1;
        ValueMeta {
            object_id: Some(iter.sstable_info.object_id),
            block_id: Some(absolute_block_idx as u64),
        }
    }
}

pub struct MonitoredCompactorIterator<I> {
    inner: I,
    task_progress: Arc<TaskProgress>,

    processed_key_num: usize,
}

impl<I: HummockIterator<Direction = Forward>> MonitoredCompactorIterator<I> {
    pub fn new(inner: I, task_progress: Arc<TaskProgress>) -> Self {
        Self {
            inner,
            task_progress,
            processed_key_num: 0,
        }
    }
}

impl<I: HummockIterator<Direction = Forward>> HummockIterator for MonitoredCompactorIterator<I> {
    type Direction = Forward;

    async fn next(&mut self) -> HummockResult<()> {
        self.inner.next().await?;
        self.processed_key_num += 1;

        if self.processed_key_num.is_multiple_of(PROGRESS_KEY_INTERVAL) {
            self.task_progress
                .inc_progress_key(PROGRESS_KEY_INTERVAL as _);
        }

        Ok(())
    }

    fn key(&self) -> FullKey<&[u8]> {
        self.inner.key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.inner.value()
    }

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.processed_key_num = 0;
        self.inner.rewind().await?;
        Ok(())
    }

    async fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> HummockResult<()> {
        self.processed_key_num = 0;
        self.inner.seek(key).await?;
        Ok(())
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        self.inner.collect_local_statistic(stats)
    }

    fn value_meta(&self) -> ValueMeta {
        self.inner.value_meta()
    }
}

fn filter_block_metas_with_handle(
    meta_handle: &SstableMetaHandle<'_>,
    block_metas_range: Range<usize>,
    read_table_ids: &HashSet<TableId>,
    key_range: KeyRange,
) -> Range<usize> {
    if block_metas_range.is_empty() {
        return block_metas_range.start..block_metas_range.start;
    }

    let global_range = block_metas_range.start..=block_metas_range.end - 1;
    let mut start_index = if key_range.left.is_empty() {
        block_metas_range.start
    } else {
        // start_index points to the greatest block whose smallest_key <= seek_key.
        meta_handle
            .block_metas_partition_point(global_range.clone(), |block| {
                KeyComparator::compare_encoded_full_key(&key_range.left, &block.smallest_key)
                    != Ordering::Less
            })
            .saturating_sub(1)
            .max(block_metas_range.start)
    };

    let mut end_index = if key_range.right.is_empty() {
        block_metas_range.end
    } else {
        let ret = meta_handle.block_metas_partition_point(global_range, |block| {
            KeyComparator::compare_encoded_full_key(&block.smallest_key, &key_range.right)
                != Ordering::Greater
        });

        if ret == block_metas_range.start {
            // not found
            return block_metas_range.start..block_metas_range.start;
        }

        ret
    }
    .saturating_sub(1);

    // Skip blocks that are not in the SST read table ids.
    while start_index <= end_index {
        let start_block_table_id = meta_handle.block_meta(start_index).table_id();
        if read_table_ids.contains(&start_block_table_id) {
            break;
        }

        // skip this table_id
        let old_start_index = start_index;
        start_index = meta_handle
            .block_metas_partition_point(start_index..=end_index, |block_meta| {
                block_meta.table_id() == start_block_table_id
            });

        if old_start_index == start_index {
            // no more blocks with the same table_id
            break;
        }
    }

    while start_index <= end_index {
        let end_block_table_id = meta_handle.block_meta(end_index).table_id();
        if read_table_ids.contains(&end_block_table_id) {
            break;
        }

        let old_end_index = end_index;
        end_index = meta_handle
            .block_metas_partition_point(start_index..=end_index, |block_meta| {
                block_meta.table_id() < end_block_table_id
            })
            .saturating_sub(1);

        if end_index == old_end_index {
            // no more blocks with the same table_id
            break;
        }
    }

    if start_index > end_index {
        return block_metas_range.start..block_metas_range.start;
    }

    start_index..(end_index + 1)
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::collections::HashSet;
    use std::ops::Range;

    use risingwave_common::catalog::TableId;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::key::{FullKey, FullKeyTracker, next_full_key, prev_full_key};
    use risingwave_hummock_sdk::key_range::KeyRange;
    use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};

    use crate::hummock::compactor::ConcatSstableIterator;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::iterator::{HummockIterator, MergeIterator};
    use crate::hummock::sstable::SstableMetaHandle;
    use crate::hummock::test_utils::{
        TEST_KEYS_COUNT, default_builder_opt_for_test, gen_test_sstable_info,
        gen_test_sstable_with_table_ids, test_key_of, test_value_of,
    };
    use crate::hummock::value::HummockValue;
    use crate::hummock::{BlockMeta, Sstable, SstableMeta};

    #[tokio::test]
    async fn test_concat_iterator() {
        let sstable_store = mock_sstable_store().await;
        let mut table_infos = vec![];
        for object_id in 0..3 {
            let start_index = object_id * TEST_KEYS_COUNT;
            let end_index = (object_id + 1) * TEST_KEYS_COUNT;
            let table_info = gen_test_sstable_info(
                default_builder_opt_for_test(),
                object_id as u64,
                (start_index..end_index)
                    .map(|i| (test_key_of(i), HummockValue::put(test_value_of(i)))),
                sstable_store.clone(),
            )
            .await;
            table_infos.push(table_info);
        }
        let start_index = 5000;
        let end_index = 25000;

        let kr = KeyRange::new(
            test_key_of(start_index).encode().into(),
            test_key_of(end_index).encode().into(),
        );
        let mut iter =
            ConcatSstableIterator::for_test(table_infos.clone(), kr.clone(), sstable_store.clone());
        iter.seek(FullKey::decode(&kr.left)).await.unwrap();

        for idx in start_index..end_index {
            let key = iter.key();
            let val = iter.value();
            assert_eq!(key, test_key_of(idx).to_ref(), "failed at {}", idx);
            assert_eq!(
                val.into_user_value().unwrap(),
                test_value_of(idx).as_slice()
            );
            iter.next().await.unwrap();
        }

        // seek non-overlap range
        let kr = KeyRange::new(
            test_key_of(30000).encode().into(),
            test_key_of(40000).encode().into(),
        );
        let mut iter =
            ConcatSstableIterator::for_test(table_infos.clone(), kr.clone(), sstable_store.clone());
        iter.seek(FullKey::decode(&kr.left)).await.unwrap();
        assert!(!iter.is_valid());
        let kr = KeyRange::new(
            test_key_of(start_index).encode().into(),
            test_key_of(40000).encode().into(),
        );
        let mut iter =
            ConcatSstableIterator::for_test(table_infos.clone(), kr.clone(), sstable_store.clone());
        iter.seek(FullKey::decode(&kr.left)).await.unwrap();
        for idx in start_index..30000 {
            let key = iter.key();
            let val = iter.value();
            assert_eq!(key, test_key_of(idx).to_ref(), "failed at {}", idx);
            assert_eq!(
                val.into_user_value().unwrap(),
                test_value_of(idx).as_slice()
            );
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // Test seek. Result is dominated by given seek key rather than key range.
        let kr = KeyRange::new(
            test_key_of(0).encode().into(),
            test_key_of(40000).encode().into(),
        );
        let mut iter =
            ConcatSstableIterator::for_test(table_infos.clone(), kr.clone(), sstable_store.clone());
        iter.seek(test_key_of(10000).to_ref()).await.unwrap();
        assert!(iter.is_valid() && iter.cur_idx == 1 && iter.key() == test_key_of(10000).to_ref());
        iter.seek(test_key_of(10001).to_ref()).await.unwrap();
        assert!(iter.is_valid() && iter.cur_idx == 1 && iter.key() == test_key_of(10001).to_ref());
        iter.seek(test_key_of(9999).to_ref()).await.unwrap();
        assert!(iter.is_valid() && iter.cur_idx == 0 && iter.key() == test_key_of(9999).to_ref());
        iter.seek(test_key_of(1).to_ref()).await.unwrap();
        assert!(iter.is_valid() && iter.cur_idx == 0 && iter.key() == test_key_of(1).to_ref());
        iter.seek(test_key_of(29999).to_ref()).await.unwrap();
        assert!(iter.is_valid() && iter.cur_idx == 2 && iter.key() == test_key_of(29999).to_ref());
        iter.seek(test_key_of(30000).to_ref()).await.unwrap();
        assert!(!iter.is_valid());

        // Test seek. Result is dominated by key range rather than given seek key.
        let kr = KeyRange::new(
            test_key_of(6000).encode().into(),
            test_key_of(16000).encode().into(),
        );
        let mut iter =
            ConcatSstableIterator::for_test(table_infos.clone(), kr.clone(), sstable_store.clone());
        iter.seek(test_key_of(17000).to_ref()).await.unwrap();
        assert!(!iter.is_valid());
        iter.seek(test_key_of(1).to_ref()).await.unwrap();
        assert!(iter.is_valid() && iter.cur_idx == 0 && iter.key() == FullKey::decode(&kr.left));
    }

    #[tokio::test]
    async fn test_concat_iterator_seek_idx() {
        let sstable_store = mock_sstable_store().await;
        let mut table_infos = vec![];
        for object_id in 0..3 {
            let start_index = object_id * TEST_KEYS_COUNT + TEST_KEYS_COUNT / 2;
            let end_index = (object_id + 1) * TEST_KEYS_COUNT;
            let table_info = gen_test_sstable_info(
                default_builder_opt_for_test(),
                object_id as u64,
                (start_index..end_index)
                    .map(|i| (test_key_of(i), HummockValue::put(test_value_of(i)))),
                sstable_store.clone(),
            )
            .await;
            table_infos.push(table_info);
        }

        // Test seek_idx. Result is dominated by given seek key rather than key range.
        let kr = KeyRange::new(
            test_key_of(0).encode().into(),
            test_key_of(40000).encode().into(),
        );
        let mut iter =
            ConcatSstableIterator::for_test(table_infos.clone(), kr.clone(), sstable_store.clone());
        let sst = sstable_store
            .sstable(&iter.sstables[0], &mut iter.stats)
            .await
            .unwrap();
        let block_metas = &sst.meta.block_metas;
        let block_1_smallest_key = block_metas[1].smallest_key.clone();
        let block_2_smallest_key = block_metas[2].smallest_key.clone();
        // Use block_1_smallest_key as seek key and result in the first KV of block 1.
        let seek_key = block_1_smallest_key.clone();
        iter.seek_idx(0, Some(FullKey::decode(&seek_key)))
            .await
            .unwrap();
        assert!(iter.is_valid() && iter.key() == FullKey::decode(block_1_smallest_key.as_slice()));
        // Use prev_full_key(block_1_smallest_key) as seek key and result in the first KV of block
        // 1.
        let seek_key = prev_full_key(block_1_smallest_key.as_slice());
        iter.seek_idx(0, Some(FullKey::decode(&seek_key)))
            .await
            .unwrap();
        assert!(iter.is_valid() && iter.key() == FullKey::decode(block_1_smallest_key.as_slice()));
        iter.next().await.unwrap();
        let block_1_second_key = iter.key().to_vec();
        // Use a big enough seek key and result in invalid iterator.
        let seek_key = test_key_of(30001);
        iter.seek_idx(table_infos.len() - 1, Some(seek_key.to_ref()))
            .await
            .unwrap();
        assert!(!iter.is_valid());

        // Test seek_idx. Result is dominated by key range rather than given seek key.
        let kr = KeyRange::new(
            next_full_key(&block_1_smallest_key).into(),
            prev_full_key(&block_2_smallest_key).into(),
        );
        let mut iter =
            ConcatSstableIterator::for_test(table_infos.clone(), kr.clone(), sstable_store.clone());
        // Use block_2_smallest_key as seek key and result in invalid iterator.
        let seek_key = FullKey::decode(&block_2_smallest_key);
        assert!(seek_key.cmp(&FullKey::decode(&kr.right)) == Ordering::Greater);
        iter.seek_idx(0, Some(seek_key)).await.unwrap();
        assert!(!iter.is_valid());
        // Use a small enough seek key and result in the second KV of block 1.
        let seek_key = test_key_of(0).encode();
        iter.seek_idx(0, Some(FullKey::decode(&seek_key)))
            .await
            .unwrap();
        assert!(iter.is_valid());
        assert_eq!(iter.key(), block_1_second_key.to_ref());

        // Use None seek key and result in the second KV of block 1.
        iter.seek_idx(0, None).await.unwrap();
        assert!(iter.is_valid());
        assert_eq!(iter.key(), block_1_second_key.to_ref());
    }

    #[tokio::test]
    async fn test_filter_block_metas() {
        fn filter_block_metas_for_test(
            block_metas: &[BlockMeta],
            read_table_ids: &HashSet<TableId>,
            key_range: KeyRange,
        ) -> Range<usize> {
            let sstable = Sstable::new(
                0.into(),
                SstableMeta {
                    block_metas: block_metas.to_vec(),
                    ..Default::default()
                },
                false,
            );
            super::filter_block_metas_with_handle(
                &SstableMetaHandle::v2(&sstable),
                0..sstable.block_count(),
                read_table_ids,
                key_range,
            )
        }

        {
            let block_metas = Vec::default();

            let ret =
                filter_block_metas_for_test(&block_metas, &HashSet::default(), KeyRange::default());

            assert!(ret.is_empty());
        }

        {
            let block_metas = vec![
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(1), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(2), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(3), Vec::default(), 0).encode(),
                    ..Default::default()
                },
            ];

            let ret = filter_block_metas_for_test(
                &block_metas,
                &HashSet::from_iter(vec![1_u32.into(), 2.into(), 3.into()].into_iter()),
                KeyRange::default(),
            );
            let ret = &block_metas[ret];

            assert_eq!(3, ret.len());
            assert_eq!(
                1,
                FullKey::decode(&ret[0].smallest_key)
                    .user_key
                    .table_id
                    .as_raw_id()
            );
            assert_eq!(
                3,
                FullKey::decode(&ret[2].smallest_key)
                    .user_key
                    .table_id
                    .as_raw_id()
            );
        }

        {
            let block_metas = vec![
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(1), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(2), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(3), Vec::default(), 0).encode(),
                    ..Default::default()
                },
            ];

            let ret = filter_block_metas_for_test(
                &block_metas,
                &HashSet::from_iter(vec![2_u32.into(), 3.into()].into_iter()),
                KeyRange::default(),
            );
            let ret = &block_metas[ret];

            assert_eq!(2, ret.len());
            assert_eq!(
                2,
                FullKey::decode(&ret[0].smallest_key)
                    .user_key
                    .table_id
                    .as_raw_id()
            );
            assert_eq!(
                3,
                FullKey::decode(&ret[1].smallest_key)
                    .user_key
                    .table_id
                    .as_raw_id()
            );
        }

        {
            let block_metas = vec![
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(1), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(2), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(3), Vec::default(), 0).encode(),
                    ..Default::default()
                },
            ];

            let ret = filter_block_metas_for_test(
                &block_metas,
                &HashSet::from_iter(vec![1_u32.into(), 2_u32.into()].into_iter()),
                KeyRange::default(),
            );
            let ret = &block_metas[ret];

            assert_eq!(2, ret.len());
            assert_eq!(
                1,
                FullKey::decode(&ret[0].smallest_key)
                    .user_key
                    .table_id
                    .as_raw_id()
            );
            assert_eq!(
                2,
                FullKey::decode(&ret[1].smallest_key)
                    .user_key
                    .table_id
                    .as_raw_id()
            );
        }

        {
            let block_metas = vec![
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(1), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(2), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(3), Vec::default(), 0).encode(),
                    ..Default::default()
                },
            ];
            let ret = filter_block_metas_for_test(
                &block_metas,
                &HashSet::from_iter(vec![2_u32.into()].into_iter()),
                KeyRange::default(),
            );
            let ret = &block_metas[ret];

            assert_eq!(1, ret.len());
            assert_eq!(
                2,
                FullKey::decode(&ret[0].smallest_key)
                    .user_key
                    .table_id
                    .as_raw_id()
            );
        }

        {
            let block_metas = vec![
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(1), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(1), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(1), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(2), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(3), Vec::default(), 0).encode(),
                    ..Default::default()
                },
            ];
            let ret = filter_block_metas_for_test(
                &block_metas,
                &HashSet::from_iter(vec![2_u32.into()].into_iter()),
                KeyRange::default(),
            );
            let ret = &block_metas[ret];

            assert_eq!(1, ret.len());
            assert_eq!(
                2,
                FullKey::decode(&ret[0].smallest_key)
                    .user_key
                    .table_id
                    .as_raw_id()
            );
        }

        {
            let block_metas = vec![
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(1), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(2), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(3), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(3), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(3), Vec::default(), 0).encode(),
                    ..Default::default()
                },
            ];

            let ret = filter_block_metas_for_test(
                &block_metas,
                &HashSet::from_iter(vec![2_u32.into()].into_iter()),
                KeyRange::default(),
            );
            let ret = &block_metas[ret];

            assert_eq!(1, ret.len());
            assert_eq!(
                2,
                FullKey::decode(&ret[0].smallest_key)
                    .user_key
                    .table_id
                    .as_raw_id()
            );
        }

        {
            let block_metas = vec![
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(1), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(2), Vec::default(), 0).encode(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: FullKey::for_test(TableId::new(3), Vec::default(), 0).encode(),
                    ..Default::default()
                },
            ];

            let ret = filter_block_metas_for_test(
                &block_metas,
                &HashSet::from_iter(vec![1_u32.into(), 3_u32.into()].into_iter()),
                KeyRange::default(),
            );
            let ret = &block_metas[ret];

            assert_eq!(3, ret.len());
            assert_eq!(
                1,
                FullKey::decode(&ret[0].smallest_key)
                    .user_key
                    .table_id
                    .as_raw_id()
            );
            assert_eq!(
                2,
                FullKey::decode(&ret[1].smallest_key)
                    .user_key
                    .table_id
                    .as_raw_id()
            );
            assert_eq!(
                3,
                FullKey::decode(&ret[2].smallest_key)
                    .user_key
                    .table_id
                    .as_raw_id()
            );
        }
    }

    #[tokio::test]
    async fn test_iterator_same_obj() {
        let sstable_store = mock_sstable_store().await;

        let table_info = gen_test_sstable_info(
            default_builder_opt_for_test(),
            1_u64,
            (1..10000).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i)))),
            sstable_store.clone(),
        )
        .await;

        let split_key = test_key_of(5000).encode();
        let sst_1: SstableInfo = SstableInfoInner {
            key_range: KeyRange {
                left: table_info.key_range.left.clone(),
                right: split_key.clone().into(),
                right_exclusive: true,
            },
            ..table_info.get_inner()
        }
        .into();

        let total_key_count = sst_1.total_key_count;
        let sst_2: SstableInfo = SstableInfoInner {
            sst_id: sst_1.sst_id + 1,
            key_range: KeyRange {
                left: split_key.clone().into(),
                right: table_info.key_range.right.clone(),
                right_exclusive: table_info.key_range.right_exclusive,
            },
            ..table_info.get_inner()
        }
        .into();

        {
            // test concate
            let mut full_key_tracker = FullKeyTracker::<Vec<u8>>::new(FullKey::default());

            let mut iter = ConcatSstableIterator::for_test(
                vec![sst_1.clone(), sst_2.clone()],
                KeyRange::default(),
                sstable_store.clone(),
            );

            iter.rewind().await.unwrap();

            let mut key_count = 0;
            while iter.is_valid() {
                let is_new_user_key = full_key_tracker.observe(iter.key());
                assert!(is_new_user_key);
                key_count += 1;
                iter.next().await.unwrap();
            }

            assert_eq!(total_key_count, key_count);
        }

        {
            let mut full_key_tracker = FullKeyTracker::<Vec<u8>>::new(FullKey::default());
            let concat_1 = ConcatSstableIterator::for_test(
                vec![sst_1.clone()],
                KeyRange::default(),
                sstable_store.clone(),
            );

            let concat_2 = ConcatSstableIterator::for_test(
                vec![sst_2.clone()],
                KeyRange::default(),
                sstable_store.clone(),
            );

            let mut key_count = 0;
            let mut iter = MergeIterator::for_compactor(vec![concat_1, concat_2]);
            iter.rewind().await.unwrap();
            while iter.is_valid() {
                full_key_tracker.observe(iter.key());
                key_count += 1;
                iter.next().await.unwrap();
            }
            assert_eq!(total_key_count, key_count);
        }
    }

    #[tokio::test]
    async fn test_concat_iterator_skips_hole_table_blocks() {
        let sstable_store = mock_sstable_store().await;

        let key_1 = FullKey::for_test(TableId::new(1), b"a".to_vec(), test_epoch(1));
        let key_2 = FullKey::for_test(TableId::new(2), b"b".to_vec(), test_epoch(1));
        let key_3 = FullKey::for_test(TableId::new(3), b"c".to_vec(), test_epoch(1));
        let kv_pairs = vec![
            (key_1.clone(), HummockValue::put(b"value-1".to_vec())),
            (key_2.clone(), HummockValue::put(b"value-2".to_vec())),
            (key_3.clone(), HummockValue::put(b"value-3".to_vec())),
        ];

        let (_sstable, table_info) = gen_test_sstable_with_table_ids(
            default_builder_opt_for_test(),
            10,
            kv_pairs.into_iter(),
            sstable_store.clone(),
            vec![1, 2, 3],
        )
        .await;

        let table_info: SstableInfo = SstableInfoInner {
            table_ids: vec![1.into(), 3.into()],
            ..table_info.get_inner()
        }
        .into();

        let mut iter = ConcatSstableIterator::for_test(
            vec![table_info.clone()],
            KeyRange::default(),
            sstable_store.clone(),
        );
        iter.rewind().await.unwrap();
        assert!(iter.is_valid());
        assert_eq!(iter.key(), key_1.to_ref());

        iter.next().await.unwrap();
        assert!(iter.is_valid());
        assert_eq!(iter.key(), key_3.to_ref());

        iter.next().await.unwrap();
        assert!(!iter.is_valid());

        let mut iter =
            ConcatSstableIterator::for_test(vec![table_info], KeyRange::default(), sstable_store);
        iter.seek(key_2.to_ref()).await.unwrap();
        assert!(iter.is_valid());
        assert_eq!(iter.key(), key_3.to_ref());
    }
}
