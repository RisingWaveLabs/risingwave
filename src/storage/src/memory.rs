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
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::mem::take;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::array::VectorRef;
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::TableId;
use risingwave_common::dispatch_distance_measurement;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::id::FragmentId;
use risingwave_common::types::ScalarRef;
use risingwave_common::util::epoch::{EpochPair, MAX_EPOCH};
use risingwave_hummock_sdk::key::{
    FullKey, TableKey, TableKeyRange, UserKey, prefixed_range_with_vnode,
};
use risingwave_hummock_sdk::table_watermark::WatermarkDirection;
use risingwave_hummock_sdk::{HummockEpoch, HummockReadEpoch};
use thiserror_ext::AsReport;
use tokio::task::yield_now;
use tracing::error;

use crate::error::StorageResult;
use crate::hummock::HummockError;
use crate::hummock::utils::{
    do_delete_sanity_check, do_insert_sanity_check, do_update_sanity_check, merge_stream,
    sanity_check_enabled,
};
use crate::mem_table::{KeyOp, MemTable};
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::vector::{MeasureDistanceBuilder, NearestBuilder};

pub type BytesFullKey = FullKey<Bytes>;
pub type BytesFullKeyRange = (Bound<BytesFullKey>, Bound<BytesFullKey>);

pub trait RangeKv: Clone + Send + Sync + 'static {
    fn range(
        &self,
        range: BytesFullKeyRange,
        limit: Option<usize>,
    ) -> StorageResult<Vec<(BytesFullKey, Option<Bytes>)>>;

    fn rev_range(
        &self,
        range: BytesFullKeyRange,
        limit: Option<usize>,
    ) -> StorageResult<Vec<(BytesFullKey, Option<Bytes>)>>;

    fn ingest_batch(
        &self,
        kv_pairs: impl Iterator<Item = (BytesFullKey, Option<Bytes>)>,
    ) -> StorageResult<()>;

    fn flush(&self) -> StorageResult<()>;
}

pub type BTreeMapRangeKv = Arc<RwLock<BTreeMap<BytesFullKey, Option<Bytes>>>>;

impl RangeKv for BTreeMapRangeKv {
    fn range(
        &self,
        range: BytesFullKeyRange,
        limit: Option<usize>,
    ) -> StorageResult<Vec<(BytesFullKey, Option<Bytes>)>> {
        let limit = limit.unwrap_or(usize::MAX);
        Ok(self
            .read()
            .range(range)
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn rev_range(
        &self,
        range: BytesFullKeyRange,
        limit: Option<usize>,
    ) -> StorageResult<Vec<(BytesFullKey, Option<Bytes>)>> {
        let limit = limit.unwrap_or(usize::MAX);
        Ok(self
            .read()
            .range(range)
            .rev()
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn ingest_batch(
        &self,
        kv_pairs: impl Iterator<Item = (BytesFullKey, Option<Bytes>)>,
    ) -> StorageResult<()> {
        let mut inner = self.write();
        for (key, value) in kv_pairs {
            inner.insert(key, value);
        }
        Ok(())
    }

    fn flush(&self) -> StorageResult<()> {
        Ok(())
    }
}

pub mod sled {
    use std::fs::create_dir_all;
    use std::ops::RangeBounds;

    use bytes::Bytes;
    use risingwave_hummock_sdk::key::FullKey;

    use crate::error::StorageResult;
    use crate::memory::{BytesFullKey, BytesFullKeyRange, RangeKv, RangeKvStateStore};

    #[derive(Clone)]
    pub struct SledRangeKv {
        inner: sled::Db,
    }

    impl SledRangeKv {
        pub fn new(path: impl AsRef<std::path::Path>) -> Self {
            SledRangeKv {
                inner: sled::open(path).expect("open"),
            }
        }

        pub fn new_temp() -> Self {
            create_dir_all("./.risingwave/sled").expect("should create");
            let path = tempfile::TempDir::new_in("./.risingwave/sled")
                .expect("find temp dir")
                .keep();
            Self::new(path)
        }
    }

    const EMPTY: u8 = 1;
    const NON_EMPTY: u8 = 0;

    impl RangeKv for SledRangeKv {
        fn range(
            &self,
            range: BytesFullKeyRange,
            limit: Option<usize>,
        ) -> StorageResult<Vec<(BytesFullKey, Option<Bytes>)>> {
            let (left, right) = range;
            let full_key_ref_bound = (
                left.as_ref().map(FullKey::to_ref),
                right.as_ref().map(FullKey::to_ref),
            );
            let left_encoded = left.as_ref().map(|key| key.to_ref().encode_reverse_epoch());
            let right_encoded = right
                .as_ref()
                .map(|key| key.to_ref().encode_reverse_epoch());
            let limit = limit.unwrap_or(usize::MAX);
            let mut ret = vec![];
            for result in self.inner.range((left_encoded, right_encoded)).take(limit) {
                let (key, value) = result?;
                let full_key = FullKey::decode_reverse_epoch(key.as_ref()).copy_into();
                if !full_key_ref_bound.contains(&full_key.to_ref()) {
                    continue;
                }
                let value = match value.as_ref() {
                    [EMPTY] => None,
                    [NON_EMPTY, rest @ ..] => Some(Bytes::from(Vec::from(rest))),
                    _ => unreachable!("malformed value: {:?}", value),
                };
                ret.push((full_key, value))
            }
            Ok(ret)
        }

        fn rev_range(
            &self,
            range: BytesFullKeyRange,
            limit: Option<usize>,
        ) -> StorageResult<Vec<(BytesFullKey, Option<Bytes>)>> {
            let (left, right) = range;
            let full_key_ref_bound = (
                left.as_ref().map(FullKey::to_ref),
                right.as_ref().map(FullKey::to_ref),
            );
            let left_encoded = left.as_ref().map(|key| key.to_ref().encode_reverse_epoch());
            let right_encoded = right
                .as_ref()
                .map(|key| key.to_ref().encode_reverse_epoch());
            let limit = limit.unwrap_or(usize::MAX);
            let mut ret = vec![];
            for result in self
                .inner
                .range((left_encoded, right_encoded))
                .rev()
                .take(limit)
            {
                let (key, value) = result?;
                let full_key = FullKey::decode_reverse_epoch(key.as_ref()).copy_into();
                if !full_key_ref_bound.contains(&full_key.to_ref()) {
                    continue;
                }
                let value = match value.as_ref() {
                    [EMPTY] => None,
                    [NON_EMPTY, rest @ ..] => Some(Bytes::from(Vec::from(rest))),
                    _ => unreachable!("malformed value: {:?}", value),
                };
                ret.push((full_key, value))
            }
            Ok(ret)
        }

        fn ingest_batch(
            &self,
            kv_pairs: impl Iterator<Item = (BytesFullKey, Option<Bytes>)>,
        ) -> StorageResult<()> {
            let mut batch = sled::Batch::default();
            for (key, value) in kv_pairs {
                let encoded_key = key.encode_reverse_epoch();
                let key = sled::IVec::from(encoded_key);
                let mut buffer =
                    Vec::with_capacity(value.as_ref().map(|v| v.len()).unwrap_or_default() + 1);
                if let Some(value) = value {
                    buffer.push(NON_EMPTY);
                    buffer.extend_from_slice(value.as_ref());
                } else {
                    buffer.push(EMPTY);
                }
                let value = sled::IVec::from(buffer);
                batch.insert(key, value);
            }
            self.inner.apply_batch(batch)?;
            Ok(())
        }

        fn flush(&self) -> StorageResult<()> {
            Ok(self.inner.flush().map(|_| {})?)
        }
    }

    pub type SledStateStore = RangeKvStateStore<SledRangeKv>;

    impl SledStateStore {
        pub fn new(path: impl AsRef<std::path::Path>) -> Self {
            RangeKvStateStore {
                inner: SledRangeKv::new(path),
                tables: Default::default(),
                vectors: Default::default(),
            }
        }

        pub fn new_temp() -> Self {
            RangeKvStateStore {
                inner: SledRangeKv::new_temp(),
                tables: Default::default(),
                vectors: Default::default(),
            }
        }
    }

    #[cfg(test)]
    mod test {
        use std::ops::{Bound, RangeBounds};

        use bytes::Bytes;
        use risingwave_common::catalog::TableId;
        use risingwave_common::util::epoch::EPOCH_SPILL_TIME_MASK;
        use risingwave_hummock_sdk::EpochWithGap;
        use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKey};

        use crate::memory::RangeKv;
        use crate::memory::sled::SledRangeKv;

        #[test]
        fn test_filter_variable_key_length_false_positive() {
            let table_id = TableId::new(233);
            let epoch = u64::MAX - u64::from_be_bytes([1, 2, 3, 4, 5, 6, 7, 8]);
            let excluded_short_table_key = [0, 1, 0, 0];
            let included_long_table_key = [0, 1, 0, 0, 1, 2];
            let left_table_key = [0, 1, 0, 0, 1];
            let right_table_key = [0, 1, 1, 1];

            let to_full_key = |table_key: &[u8]| FullKey {
                user_key: UserKey {
                    table_id,
                    table_key: TableKey(Bytes::from(table_key.to_vec())),
                },
                epoch_with_gap: EpochWithGap::new_from_epoch(epoch & !EPOCH_SPILL_TIME_MASK),
            };

            let left_full_key = to_full_key(&left_table_key[..]);
            let right_full_key = to_full_key(&right_table_key[..]);
            let included_long_full_key = to_full_key(&included_long_table_key[..]);
            let excluded_short_full_key = to_full_key(&excluded_short_table_key[..]);

            assert!(
                (
                    Bound::Included(left_full_key.to_ref()),
                    Bound::Included(right_full_key.to_ref())
                )
                    .contains(&included_long_full_key.to_ref())
            );
            assert!(
                !(
                    Bound::Included(left_full_key.to_ref()),
                    Bound::Included(right_full_key.to_ref())
                )
                    .contains(&excluded_short_full_key.to_ref())
            );

            let left_encoded = left_full_key.encode_reverse_epoch();
            let right_encoded = right_full_key.encode_reverse_epoch();

            assert!(
                (
                    Bound::Included(left_encoded.clone()),
                    Bound::Included(right_encoded.clone())
                )
                    .contains(&included_long_full_key.encode_reverse_epoch())
            );
            assert!(
                (
                    Bound::Included(left_encoded),
                    Bound::Included(right_encoded)
                )
                    .contains(&excluded_short_full_key.encode_reverse_epoch())
            );

            let sled_range_kv = SledRangeKv::new_temp();
            sled_range_kv
                .ingest_batch(
                    vec![
                        (included_long_full_key.clone(), None),
                        (excluded_short_full_key, None),
                    ]
                    .into_iter(),
                )
                .unwrap();
            let kvs = sled_range_kv
                .range(
                    (
                        Bound::Included(left_full_key),
                        Bound::Included(right_full_key),
                    ),
                    None,
                )
                .unwrap();
            assert_eq!(1, kvs.len());
            assert_eq!(included_long_full_key.to_ref(), kvs[0].0.to_ref());
            assert!(kvs[0].1.is_none());
        }
    }
}

mod batched_iter {

    use super::*;

    /// A utility struct for iterating over a range of keys in a locked `BTreeMap`, which will batch
    /// some records to make a trade-off between the copying overhead and the times of acquiring
    /// the lock.
    ///
    /// Therefore, it's not guaranteed that we're iterating over a consistent snapshot of the map.
    /// Users should handle MVCC by themselves.
    pub struct Iter<R: RangeKv> {
        inner: R,
        range: BytesFullKeyRange,
        current: std::vec::IntoIter<(FullKey<Bytes>, Option<Bytes>)>,
        rev: bool,
    }

    impl<R: RangeKv> Iter<R> {
        pub fn new(inner: R, range: BytesFullKeyRange, rev: bool) -> Self {
            Self {
                inner,
                range,
                rev,
                current: Vec::new().into_iter(),
            }
        }
    }

    impl<R: RangeKv> Iter<R> {
        const BATCH_SIZE: usize = 256;

        /// Get the next batch of records and fill the `current` buffer.
        fn refill(&mut self) -> StorageResult<()> {
            assert!(self.current.is_empty());

            let batch = if self.rev {
                self.inner.rev_range(
                    (self.range.0.clone(), self.range.1.clone()),
                    Some(Self::BATCH_SIZE),
                )?
            } else {
                self.inner.range(
                    (self.range.0.clone(), self.range.1.clone()),
                    Some(Self::BATCH_SIZE),
                )?
            };

            if let Some((last_key, _)) = batch.last() {
                let full_key = FullKey::new_with_gap_epoch(
                    last_key.user_key.table_id,
                    TableKey(last_key.user_key.table_key.0.clone()),
                    last_key.epoch_with_gap,
                );
                if self.rev {
                    self.range.1 = Bound::Excluded(full_key);
                } else {
                    self.range.0 = Bound::Excluded(full_key);
                }
            }
            self.current = batch.into_iter();
            Ok(())
        }
    }

    impl<R: RangeKv> Iter<R> {
        pub fn next(&mut self) -> StorageResult<Option<(BytesFullKey, Option<Bytes>)>> {
            match self.current.next() {
                Some((key, value)) => Ok(Some((key, value))),
                None => {
                    self.refill()?;
                    Ok(self.current.next())
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use rand::Rng;

        use super::*;
        use crate::memory::sled::SledRangeKv;

        #[test]
        fn test_btreemap_iter_chaos() {
            let map = Arc::new(RwLock::new(BTreeMap::new()));
            test_iter_chaos_inner(map, 1000);
        }

        #[cfg(not(madsim))]
        #[test]
        fn test_sled_iter_chaos() {
            let map = SledRangeKv::new_temp();
            test_iter_chaos_inner(map, 100);
        }

        fn test_iter_chaos_inner(map: impl RangeKv, count: usize) {
            let key_range = 1..=10000;
            let num_to_bytes = |k: i32| Bytes::from(format!("{:06}", k).as_bytes().to_vec());
            let num_to_full_key =
                |k: i32| FullKey::new(TableId::default(), TableKey(num_to_bytes(k)), 0);

            map.ingest_batch(key_range.clone().map(|k| {
                let key = num_to_full_key(k);
                let b = key.user_key.table_key.0.clone();

                (key, Some(b))
            }))
            .unwrap();

            let rand_bound = || {
                let key = rand::rng().random_range(key_range.clone());
                let key = num_to_full_key(key);
                match rand::rng().random_range(1..=5) {
                    1 | 2 => Bound::Included(key),
                    3 | 4 => Bound::Excluded(key),
                    _ => Bound::Unbounded,
                }
            };

            for _ in 0..count {
                let range = loop {
                    let range = (rand_bound(), rand_bound());
                    let (start, end) = (range.start_bound(), range.end_bound());

                    // Filter out invalid ranges. Code migrated from `BTreeMap::range`.
                    match (start, end) {
                        (Bound::Excluded(s), Bound::Excluded(e)) if s == e => {
                            continue;
                        }
                        (
                            Bound::Included(s) | Bound::Excluded(s),
                            Bound::Included(e) | Bound::Excluded(e),
                        ) if s > e => {
                            continue;
                        }
                        _ => break range,
                    }
                };

                let v1 = {
                    let mut v = vec![];
                    let mut iter = Iter::new(map.clone(), range.clone(), false);
                    while let Some((key, value)) = iter.next().unwrap() {
                        v.push((key, value));
                    }
                    v
                };
                let v2 = map.range(range, None).unwrap();

                // Items iterated from the batched iterator should be the same as normaliterator.
                assert_eq!(v1, v2);
            }
        }
    }
}

pub type MemoryStateStore = RangeKvStateStore<BTreeMapRangeKv>;

struct TableState {
    init_epoch: u64,
    next_epochs: BTreeMap<u64, u64>,
    latest_sealed_epoch: Option<u64>,
    sealing_epochs: BTreeMap<u64, BitmapBuilder>,
}

impl TableState {
    fn new(init_epoch: u64) -> Self {
        Self {
            init_epoch,
            next_epochs: Default::default(),
            latest_sealed_epoch: None,
            sealing_epochs: Default::default(),
        }
    }

    async fn wait_epoch(
        tables: &parking_lot::Mutex<HashMap<TableId, Self>>,
        table_id: TableId,
        epoch: u64,
    ) {
        loop {
            {
                let tables = tables.lock();
                let table_state = tables.get(&table_id).expect("should exist");
                assert!(epoch >= table_state.init_epoch);
                if epoch == table_state.init_epoch {
                    return;
                }
                if let Some(latest_sealed_epoch) = table_state.latest_sealed_epoch
                    && latest_sealed_epoch >= epoch
                {
                    return;
                }
            }
            yield_now().await;
        }
    }
}

type InMemVectorStore = Arc<RwLock<HashMap<TableId, Vec<(Vector, Bytes, u64)>>>>;

/// An in-memory state store
///
/// The in-memory state store is a [`BTreeMap`], which maps [`FullKey`] to value. It
/// never does GC, so the memory usage will be high. Therefore, in-memory state store should never
/// be used in production.
#[derive(Clone, Default)]
pub struct RangeKvStateStore<R: RangeKv> {
    /// Stores (key, epoch) -> user value.
    inner: R,
    /// `table_id` -> `prev_epoch` -> `curr_epoch`
    tables: Arc<parking_lot::Mutex<HashMap<TableId, TableState>>>,

    vectors: InMemVectorStore,
}

fn to_full_key_range<R, B>(table_id: TableId, table_key_range: R) -> BytesFullKeyRange
where
    R: RangeBounds<B> + Send,
    B: AsRef<[u8]>,
{
    let start = match table_key_range.start_bound() {
        Included(k) => Included(FullKey::new(
            table_id,
            TableKey(Bytes::from(k.as_ref().to_vec())),
            HummockEpoch::MAX,
        )),
        Excluded(k) => Excluded(FullKey::new(
            table_id,
            TableKey(Bytes::from(k.as_ref().to_vec())),
            0,
        )),
        Unbounded => Included(FullKey::new(
            table_id,
            TableKey(Bytes::from(b"".to_vec())),
            HummockEpoch::MAX,
        )),
    };
    let end = match table_key_range.end_bound() {
        Included(k) => Included(FullKey::new(
            table_id,
            TableKey(Bytes::from(k.as_ref().to_vec())),
            0,
        )),
        Excluded(k) => Excluded(FullKey::new(
            table_id,
            TableKey(Bytes::from(k.as_ref().to_vec())),
            HummockEpoch::MAX,
        )),
        Unbounded => {
            if let Some(next_table_id) = table_id.as_raw_id().checked_add(1) {
                Excluded(FullKey::new(
                    next_table_id.into(),
                    TableKey(Bytes::from(b"".to_vec())),
                    HummockEpoch::MAX,
                ))
            } else {
                Unbounded
            }
        }
    };
    (start, end)
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn shared() -> Self {
        static STORE: LazyLock<MemoryStateStore> = LazyLock::new(MemoryStateStore::new);
        STORE.clone()
    }
}

impl<R: RangeKv> RangeKvStateStore<R> {
    fn scan(
        &self,
        key_range: TableKeyRange,
        epoch: u64,
        table_id: TableId,
        limit: Option<usize>,
    ) -> StorageResult<Vec<(Bytes, Bytes)>> {
        let mut data = vec![];
        if limit == Some(0) {
            return Ok(vec![]);
        }
        let mut last_user_key = None;
        for (key, value) in self
            .inner
            .range(to_full_key_range(table_id, key_range), None)?
        {
            if key.epoch_with_gap.pure_epoch() > epoch {
                continue;
            }
            if Some(&key.user_key) != last_user_key.as_ref() {
                if let Some(value) = value {
                    data.push((Bytes::from(key.encode()), value.clone()));
                }
                last_user_key = Some(key.user_key.clone());
            }
            if let Some(limit) = limit
                && data.len() >= limit
            {
                break;
            }
        }
        Ok(data)
    }
}

#[derive(Clone)]
pub struct RangeKvStateStoreReadSnapshot<R: RangeKv> {
    inner: RangeKvStateStore<R>,
    epoch: u64,
    table_id: TableId,
}

impl<R: RangeKv> StateStoreGet for RangeKvStateStoreReadSnapshot<R> {
    async fn on_key_value<'a, O: Send + 'a>(
        &'a self,
        key: TableKey<Bytes>,
        _read_options: ReadOptions,
        on_key_value_fn: impl KeyValueFn<'a, O>,
    ) -> StorageResult<Option<O>> {
        self.inner
            .get_keyed_row_impl(key, self.epoch, self.table_id)
            .and_then(|option| {
                if let Some((key, value)) = option {
                    on_key_value_fn(key.to_ref(), value.as_ref()).map(Some)
                } else {
                    Ok(None)
                }
            })
    }
}

impl<R: RangeKv> StateStoreRead for RangeKvStateStoreReadSnapshot<R> {
    type Iter = RangeKvStateStoreIter<R>;
    type RevIter = RangeKvStateStoreRevIter<R>;

    async fn iter(
        &self,
        key_range: TableKeyRange,
        _read_options: ReadOptions,
    ) -> StorageResult<Self::Iter> {
        self.inner.iter_impl(key_range, self.epoch, self.table_id)
    }

    async fn rev_iter(
        &self,
        key_range: TableKeyRange,
        _read_options: ReadOptions,
    ) -> StorageResult<Self::RevIter> {
        self.inner
            .rev_iter_impl(key_range, self.epoch, self.table_id)
    }
}

impl<R: RangeKv> StateStoreReadVector for RangeKvStateStoreReadSnapshot<R> {
    async fn nearest<'a, O: Send + 'a>(
        &'a self,
        vec: VectorRef<'a>,
        options: VectorNearestOptions,
        on_nearest_item_fn: impl OnNearestItemFn<'a, O>,
    ) -> StorageResult<Vec<O>> {
        fn nearest_impl<'a, M: MeasureDistanceBuilder, O>(
            store: &'a InMemVectorStore,
            epoch: u64,
            table_id: TableId,
            vec: VectorRef<'a>,
            options: VectorNearestOptions,
            on_nearest_item_fn: impl OnNearestItemFn<'a, O>,
        ) -> Vec<O> {
            let mut builder = NearestBuilder::<'_, O, M>::new(vec, options.top_n);
            builder.add(
                store
                    .read()
                    .get(&table_id)
                    .map(|vec| vec.iter())
                    .into_iter()
                    .flatten()
                    .filter(|(_, _, vector_epoch)| epoch >= *vector_epoch)
                    .map(|(vec, info, _)| (vec.to_ref(), info.as_ref())),
                on_nearest_item_fn,
            );
            builder.finish()
        }
        dispatch_distance_measurement!(options.measure, MeasurementType, {
            Ok(nearest_impl::<MeasurementType, O>(
                &self.inner.vectors,
                self.epoch,
                self.table_id,
                vec,
                options,
                on_nearest_item_fn,
            ))
        })
    }
}

impl<R: RangeKv> RangeKvStateStore<R> {
    fn get_keyed_row_impl(
        &self,
        key: TableKey<Bytes>,
        epoch: u64,
        table_id: TableId,
    ) -> StorageResult<Option<StateStoreKeyedRow>> {
        let range_bounds = (Bound::Included(key.clone()), Bound::Included(key));
        // We do not really care about vnodes here, so we just use the default value.
        let res = self.scan(range_bounds, epoch, table_id, Some(1))?;

        Ok(match res.as_slice() {
            [] => None,
            [(key, value)] => Some((
                FullKey::decode(key.as_ref()).to_vec().into_bytes(),
                value.clone(),
            )),
            _ => unreachable!(),
        })
    }

    fn iter_impl(
        &self,
        key_range: TableKeyRange,
        epoch: u64,
        table_id: TableId,
    ) -> StorageResult<RangeKvStateStoreIter<R>> {
        Ok(RangeKvStateStoreIter::new(
            batched_iter::Iter::new(
                self.inner.clone(),
                to_full_key_range(table_id, key_range),
                false,
            ),
            epoch,
            true,
        ))
    }

    fn rev_iter_impl(
        &self,
        key_range: TableKeyRange,
        epoch: u64,
        table_id: TableId,
    ) -> StorageResult<RangeKvStateStoreRevIter<R>> {
        Ok(RangeKvStateStoreRevIter::new(
            batched_iter::Iter::new(
                self.inner.clone(),
                to_full_key_range(table_id, key_range),
                true,
            ),
            epoch,
            true,
        ))
    }
}

impl<R: RangeKv> StateStoreReadLog for RangeKvStateStore<R> {
    type ChangeLogIter = RangeKvStateStoreChangeLogIter<R>;

    async fn next_epoch(&self, epoch: u64, options: NextEpochOptions) -> StorageResult<u64> {
        loop {
            {
                let tables = self.tables.lock();
                let Some(tables) = tables.get(&options.table_id) else {
                    return Err(HummockError::next_epoch(format!(
                        "table {} does not exist",
                        options.table_id
                    ))
                    .into());
                };
                if let Some(next_epoch) = tables.next_epochs.get(&epoch) {
                    break Ok(*next_epoch);
                }
            }
            yield_now().await;
        }
    }

    async fn iter_log(
        &self,
        (min_epoch, max_epoch): (u64, u64),
        key_range: TableKeyRange,
        options: ReadLogOptions,
    ) -> StorageResult<Self::ChangeLogIter> {
        let new_value_iter = RangeKvStateStoreIter::new(
            batched_iter::Iter::new(
                self.inner.clone(),
                to_full_key_range(options.table_id, key_range.clone()),
                false,
            ),
            max_epoch,
            true,
        );
        let old_value_iter = RangeKvStateStoreIter::new(
            batched_iter::Iter::new(
                self.inner.clone(),
                to_full_key_range(options.table_id, key_range),
                false,
            ),
            min_epoch,
            false,
        );
        RangeKvStateStoreChangeLogIter::new(new_value_iter, old_value_iter)
    }
}

impl<R: RangeKv> RangeKvStateStore<R> {
    fn new_read_snapshot_impl(
        &self,
        epoch: u64,
        table_id: TableId,
    ) -> RangeKvStateStoreReadSnapshot<R> {
        RangeKvStateStoreReadSnapshot {
            inner: self.clone(),
            epoch,
            table_id,
        }
    }

    pub(crate) fn ingest_batch(
        &self,
        mut kv_pairs: Vec<(TableKey<Bytes>, StorageValue)>,
        delete_ranges: Vec<(Bound<Bytes>, Bound<Bytes>)>,
        epoch: u64,
        table_id: TableId,
    ) -> StorageResult<usize> {
        let mut delete_keys = BTreeSet::new();
        for del_range in delete_ranges {
            for (key, _) in self.inner.range(
                (
                    del_range
                        .0
                        .map(|table_key| FullKey::new(table_id, TableKey(table_key), epoch)),
                    del_range
                        .1
                        .map(|table_key| FullKey::new(table_id, TableKey(table_key), epoch)),
                ),
                None,
            )? {
                delete_keys.insert(key.user_key.table_key);
            }
        }
        for key in delete_keys {
            kv_pairs.push((key, StorageValue::new_delete()));
        }

        let mut size = 0;
        self.inner
            .ingest_batch(kv_pairs.into_iter().map(|(key, value)| {
                size += key.len() + value.size();
                (FullKey::new(table_id, key, epoch), value.user_value)
            }))?;
        Ok(size)
    }

    fn ingest_vectors(&self, table_id: TableId, epoch: u64, vecs: Vec<(Vector, Bytes)>) {
        self.vectors
            .write()
            .entry(table_id)
            .or_default()
            .extend(vecs.into_iter().map(|(vec, info)| (vec, info, epoch)));
    }
}

impl<R: RangeKv> StateStore for RangeKvStateStore<R> {
    type Local = RangeKvLocalStateStore<R>;
    type ReadSnapshot = RangeKvStateStoreReadSnapshot<R>;
    type VectorWriter = RangeKvLocalStateStore<R>;

    async fn try_wait_epoch(
        &self,
        _epoch: HummockReadEpoch,
        _options: TryWaitEpochOptions,
    ) -> StorageResult<()> {
        // memory backend doesn't need to wait for epoch, so this is a no-op.
        Ok(())
    }

    async fn new_local(&self, option: NewLocalOptions) -> Self::Local {
        RangeKvLocalStateStore::new(self.clone(), option)
    }

    async fn new_read_snapshot(
        &self,
        epoch: HummockReadEpoch,
        options: NewReadSnapshotOptions,
    ) -> StorageResult<Self::ReadSnapshot> {
        Ok(self.new_read_snapshot_impl(epoch.get_epoch(), options.table_id))
    }

    async fn new_vector_writer(&self, options: NewVectorWriterOptions) -> Self::VectorWriter {
        RangeKvLocalStateStore::new(
            self.clone(),
            NewLocalOptions {
                table_id: options.table_id,
                fragment_id: FragmentId::default(),
                op_consistency_level: Default::default(),
                table_option: Default::default(),
                is_replicated: false,
                vnodes: Arc::new(Bitmap::from_bool_slice(&[true])),
                upload_on_flush: true,
            },
        )
    }
}

pub struct RangeKvLocalStateStore<R: RangeKv> {
    mem_table: MemTable,
    vectors: Vec<(Vector, Bytes)>,
    inner: RangeKvStateStore<R>,

    epoch: Option<EpochPair>,

    table_id: TableId,
    op_consistency_level: OpConsistencyLevel,
    vnodes: Arc<Bitmap>,
}

impl<R: RangeKv> RangeKvLocalStateStore<R> {
    pub fn new(inner: RangeKvStateStore<R>, option: NewLocalOptions) -> Self {
        Self {
            inner,
            mem_table: MemTable::new(option.table_id, option.op_consistency_level.clone()),
            epoch: None,
            table_id: option.table_id,
            op_consistency_level: option.op_consistency_level,
            vnodes: option.vnodes,
            vectors: vec![],
        }
    }

    fn epoch(&self) -> u64 {
        self.epoch.expect("should have set the epoch").curr
    }
}

impl<R: RangeKv> StateStoreGet for RangeKvLocalStateStore<R> {
    async fn on_key_value<'a, O: Send + 'a>(
        &'a self,
        key: TableKey<Bytes>,
        _read_options: ReadOptions,
        on_key_value_fn: impl KeyValueFn<'a, O>,
    ) -> StorageResult<Option<O>> {
        if let Some((key, value)) = match self.mem_table.buffer.get(&key) {
            None => self
                .inner
                .get_keyed_row_impl(key, self.epoch(), self.table_id)?,
            Some(op) => match op {
                KeyOp::Insert(value) | KeyOp::Update((_, value)) => Some((
                    FullKey::new(self.table_id, key, self.epoch()),
                    value.clone(),
                )),
                KeyOp::Delete(_) => None,
            },
        } {
            Ok(Some(on_key_value_fn(key.to_ref(), value.as_ref())?))
        } else {
            Ok(None)
        }
    }
}

impl<R: RangeKv> LocalStateStore for RangeKvLocalStateStore<R> {
    type FlushedSnapshotReader = RangeKvStateStoreReadSnapshot<R>;

    type Iter<'a> = impl StateStoreIter + 'a;
    type RevIter<'a> = impl StateStoreIter + 'a;

    async fn iter(
        &self,
        key_range: TableKeyRange,
        _read_options: ReadOptions,
    ) -> StorageResult<Self::Iter<'_>> {
        let iter = self
            .inner
            .iter_impl(key_range.clone(), self.epoch(), self.table_id)?;
        Ok(FromStreamStateStoreIter::new(Box::pin(merge_stream(
            self.mem_table.iter(key_range),
            iter.into_stream(to_owned_item),
            self.table_id,
            self.epoch(),
            false,
        ))))
    }

    async fn rev_iter(
        &self,
        key_range: TableKeyRange,
        _read_options: ReadOptions,
    ) -> StorageResult<Self::RevIter<'_>> {
        let iter = self
            .inner
            .rev_iter_impl(key_range.clone(), self.epoch(), self.table_id)?;
        Ok(FromStreamStateStoreIter::new(Box::pin(merge_stream(
            self.mem_table.rev_iter(key_range),
            iter.into_stream(to_owned_item),
            self.table_id,
            self.epoch(),
            true,
        ))))
    }

    fn insert(
        &mut self,
        key: TableKey<Bytes>,
        new_val: Bytes,
        old_val: Option<Bytes>,
    ) -> StorageResult<()> {
        match old_val {
            None => self.mem_table.insert(key, new_val)?,
            Some(old_val) => self.mem_table.update(key, old_val, new_val)?,
        };
        Ok(())
    }

    fn delete(&mut self, key: TableKey<Bytes>, old_val: Bytes) -> StorageResult<()> {
        Ok(self.mem_table.delete(key, old_val)?)
    }

    async fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) -> StorageResult<Arc<Bitmap>> {
        if self.vnodes.len() > 1 {
            TableState::wait_epoch(
                &self.inner.tables,
                self.table_id,
                self.epoch.expect("should have init").prev,
            )
            .await;
        }
        Ok(std::mem::replace(&mut self.vnodes, vnodes))
    }

    fn get_table_watermark(&self, _vnode: VirtualNode) -> Option<Bytes> {
        // TODO: may store the written table watermark and have a correct implementation
        None
    }

    fn new_flushed_snapshot_reader(&self) -> Self::FlushedSnapshotReader {
        self.inner.new_read_snapshot_impl(MAX_EPOCH, self.table_id)
    }
}

impl<R: RangeKv> StateStoreWriteEpochControl for RangeKvLocalStateStore<R> {
    async fn flush(&mut self) -> StorageResult<usize> {
        let buffer = self.mem_table.drain().into_parts();
        let mut kv_pairs = Vec::with_capacity(buffer.len());
        let sanity_check_read_snapshot = if sanity_check_enabled() {
            Some(self.inner.new_read_snapshot_impl(MAX_EPOCH, self.table_id))
        } else {
            None
        };
        for (key, key_op) in buffer {
            match key_op {
                // Currently, some executors do not strictly comply with these semantics. As
                // a workaround you may call disable the check by initializing the
                // state store with `op_consistency_level=Inconsistent`.
                KeyOp::Insert(value) => {
                    if let Some(sanity_check_read_snapshot) = &sanity_check_read_snapshot {
                        do_insert_sanity_check(
                            self.table_id,
                            &key,
                            &value,
                            sanity_check_read_snapshot,
                            &self.op_consistency_level,
                        )
                        .await?;
                    }
                    kv_pairs.push((key, StorageValue::new_put(value)));
                }
                KeyOp::Delete(old_value) => {
                    if let Some(sanity_check_read_snapshot) = &sanity_check_read_snapshot {
                        do_delete_sanity_check(
                            self.table_id,
                            &key,
                            &old_value,
                            sanity_check_read_snapshot,
                            &self.op_consistency_level,
                        )
                        .await?;
                    }
                    kv_pairs.push((key, StorageValue::new_delete()));
                }
                KeyOp::Update((old_value, new_value)) => {
                    if let Some(sanity_check_read_snapshot) = &sanity_check_read_snapshot {
                        do_update_sanity_check(
                            self.table_id,
                            &key,
                            &old_value,
                            &new_value,
                            sanity_check_read_snapshot,
                            &self.op_consistency_level,
                        )
                        .await?;
                    }
                    kv_pairs.push((key, StorageValue::new_put(new_value)));
                }
            }
        }
        let epoch = self.epoch();
        self.inner
            .ingest_vectors(self.table_id, epoch, take(&mut self.vectors));
        self.inner
            .ingest_batch(kv_pairs, vec![], epoch, self.table_id)
    }

    async fn init(&mut self, options: InitOptions) -> StorageResult<()> {
        assert_eq!(
            self.epoch.replace(options.epoch),
            None,
            "epoch in local state store of table id {:?} is init for more than once",
            self.table_id
        );
        self.inner
            .tables
            .lock()
            .entry(self.table_id)
            .or_insert_with(|| TableState::new(options.epoch.prev))
            .next_epochs
            .insert(options.epoch.prev, options.epoch.curr);
        if self.vnodes.len() > 1 {
            TableState::wait_epoch(&self.inner.tables, self.table_id, options.epoch.prev).await;
        }

        Ok(())
    }

    fn seal_current_epoch(&mut self, next_epoch: u64, opts: SealCurrentEpochOptions) {
        assert!(!self.mem_table.is_dirty());
        if let Some(value_checker) = opts.switch_op_consistency_level {
            self.mem_table.op_consistency_level.update(&value_checker);
        }
        let epoch = self
            .epoch
            .as_mut()
            .expect("should have init epoch before seal the first epoch");
        let prev_epoch = epoch.curr;
        epoch.prev = prev_epoch;
        epoch.curr = next_epoch;
        assert!(
            next_epoch > prev_epoch,
            "new epoch {} should be greater than current epoch: {}",
            next_epoch,
            prev_epoch
        );

        let mut tables = self.inner.tables.lock();
        let table_state = tables
            .get_mut(&self.table_id)
            .expect("should be set when init");

        table_state.next_epochs.insert(prev_epoch, next_epoch);
        if self.vnodes.len() > 1 {
            let sealing_epoch_vnodes = table_state
                .sealing_epochs
                .entry(prev_epoch)
                .or_insert_with(|| BitmapBuilder::zeroed(self.vnodes.len()));
            assert_eq!(self.vnodes.len(), sealing_epoch_vnodes.len());
            for vnode in self.vnodes.iter_ones() {
                assert!(!sealing_epoch_vnodes.is_set(vnode));
                sealing_epoch_vnodes.set(vnode, true);
            }
            if (0..self.vnodes.len()).all(|vnode| sealing_epoch_vnodes.is_set(vnode)) {
                let (all_sealed_epoch, _) =
                    table_state.sealing_epochs.pop_first().expect("non-empty");
                assert_eq!(
                    all_sealed_epoch, prev_epoch,
                    "new all_sealed_epoch must be the current prev epoch"
                );
                if let Some(prev_latest_sealed_epoch) =
                    table_state.latest_sealed_epoch.replace(prev_epoch)
                {
                    assert!(prev_epoch > prev_latest_sealed_epoch);
                }
            }
        }

        if let Some((direction, watermarks, _watermark_type)) = opts.table_watermarks {
            let delete_ranges = watermarks
                .iter()
                .flat_map(|vnode_watermark| {
                    let inner_range = match direction {
                        WatermarkDirection::Ascending => {
                            (Unbounded, Excluded(vnode_watermark.watermark().clone()))
                        }
                        WatermarkDirection::Descending => {
                            (Excluded(vnode_watermark.watermark().clone()), Unbounded)
                        }
                    };
                    vnode_watermark
                        .vnode_bitmap()
                        .iter_vnodes()
                        .map(move |vnode| {
                            let (start, end) =
                                prefixed_range_with_vnode(inner_range.clone(), vnode);
                            (start.map(|key| key.0), end.map(|key| key.0))
                        })
                })
                .collect_vec();
            if let Err(e) =
                self.inner
                    .ingest_batch(Vec::new(), delete_ranges, self.epoch(), self.table_id)
            {
                error!(error = %e.as_report(), "failed to write delete ranges of table watermark");
            }
        }
    }

    async fn try_flush(&mut self) -> StorageResult<()> {
        Ok(())
    }
}

impl<R: RangeKv> StateStoreWriteVector for RangeKvLocalStateStore<R> {
    fn insert(&mut self, vec: VectorRef<'_>, info: Bytes) -> StorageResult<()> {
        self.vectors.push((vec.to_owned_scalar(), info));
        Ok(())
    }
}

pub struct RangeKvStateStoreIter<R: RangeKv> {
    inner: batched_iter::Iter<R>,

    epoch: HummockEpoch,
    is_inclusive_epoch: bool,

    last_key: Option<UserKey<Bytes>>,

    item_buffer: Option<StateStoreKeyedRow>,
}

impl<R: RangeKv> RangeKvStateStoreIter<R> {
    pub fn new(
        inner: batched_iter::Iter<R>,
        epoch: HummockEpoch,
        is_inclusive_epoch: bool,
    ) -> Self {
        Self {
            inner,
            epoch,
            is_inclusive_epoch,
            last_key: None,
            item_buffer: None,
        }
    }
}

impl<R: RangeKv> StateStoreIter for RangeKvStateStoreIter<R> {
    async fn try_next(&mut self) -> StorageResult<Option<StateStoreKeyedRowRef<'_>>> {
        self.next_inner()?;
        Ok(self
            .item_buffer
            .as_ref()
            .map(|(key, value)| (key.to_ref(), value.as_ref())))
    }
}

impl<R: RangeKv> RangeKvStateStoreIter<R> {
    fn next_inner(&mut self) -> StorageResult<()> {
        self.item_buffer = None;
        while let Some((key, value)) = self.inner.next()? {
            let epoch = key.epoch_with_gap.pure_epoch();
            if epoch > self.epoch {
                continue;
            }
            if epoch == self.epoch && !self.is_inclusive_epoch {
                continue;
            }
            if Some(key.user_key.as_ref()) != self.last_key.as_ref().map(|key| key.as_ref()) {
                self.last_key = Some(key.user_key.clone());
                if let Some(value) = value {
                    self.item_buffer = Some((key, value));
                    break;
                }
            }
        }
        Ok(())
    }
}

pub struct RangeKvStateStoreRevIter<R: RangeKv> {
    inner: batched_iter::Iter<R>,

    epoch: HummockEpoch,
    is_inclusive_epoch: bool,

    item_buffer: VecDeque<StateStoreKeyedRow>,
}

impl<R: RangeKv> RangeKvStateStoreRevIter<R> {
    pub fn new(
        inner: batched_iter::Iter<R>,
        epoch: HummockEpoch,
        is_inclusive_epoch: bool,
    ) -> Self {
        Self {
            inner,
            epoch,
            is_inclusive_epoch,
            item_buffer: VecDeque::default(),
        }
    }
}

impl<R: RangeKv> StateStoreIter for RangeKvStateStoreRevIter<R> {
    async fn try_next(&mut self) -> StorageResult<Option<StateStoreKeyedRowRef<'_>>> {
        self.next_inner()?;
        Ok(self
            .item_buffer
            .back()
            .map(|(key, value)| (key.to_ref(), value.as_ref())))
    }
}

impl<R: RangeKv> RangeKvStateStoreRevIter<R> {
    fn next_inner(&mut self) -> StorageResult<()> {
        self.item_buffer.pop_back();
        while let Some((key, value)) = self.inner.next()? {
            let epoch = key.epoch_with_gap.pure_epoch();
            if epoch > self.epoch {
                continue;
            }
            if epoch == self.epoch && !self.is_inclusive_epoch {
                continue;
            }

            let v = match value {
                Some(v) => v,
                None => {
                    if let Some(last_key) = self.item_buffer.front()
                        && key.user_key.as_ref() == last_key.0.user_key.as_ref()
                    {
                        self.item_buffer.clear();
                    }
                    continue;
                }
            };

            if let Some(last_key) = self.item_buffer.front() {
                if key.user_key.as_ref() != last_key.0.user_key.as_ref() {
                    self.item_buffer.push_front((key, v));
                    break;
                } else {
                    self.item_buffer.pop_front();
                    self.item_buffer.push_front((key, v));
                }
            } else {
                self.item_buffer.push_front((key, v));
            }
        }
        Ok(())
    }
}

pub struct RangeKvStateStoreChangeLogIter<R: RangeKv> {
    new_value_iter: RangeKvStateStoreIter<R>,
    old_value_iter: RangeKvStateStoreIter<R>,
    item_buffer: Option<(TableKey<Bytes>, ChangeLogValue<Bytes>)>,
}

impl<R: RangeKv> RangeKvStateStoreChangeLogIter<R> {
    fn new(
        mut new_value_iter: RangeKvStateStoreIter<R>,
        mut old_value_iter: RangeKvStateStoreIter<R>,
    ) -> StorageResult<Self> {
        new_value_iter.next_inner()?;
        old_value_iter.next_inner()?;
        Ok(Self {
            new_value_iter,
            old_value_iter,
            item_buffer: None,
        })
    }
}

impl<R: RangeKv> StateStoreIter<StateStoreReadLogItem> for RangeKvStateStoreChangeLogIter<R> {
    async fn try_next(&mut self) -> StorageResult<Option<StateStoreReadLogItemRef<'_>>> {
        loop {
            match (
                &self.new_value_iter.item_buffer,
                &self.old_value_iter.item_buffer,
            ) {
                (None, None) => {
                    self.item_buffer = None;
                    break;
                }
                (Some((key, new_value)), None) => {
                    self.item_buffer = Some((
                        key.user_key.table_key.clone(),
                        ChangeLogValue::Insert(new_value.clone()),
                    ));
                    self.new_value_iter.next_inner()?;
                }
                (None, Some((key, old_value))) => {
                    self.item_buffer = Some((
                        key.user_key.table_key.clone(),
                        ChangeLogValue::Delete(old_value.clone()),
                    ));
                    self.old_value_iter.next_inner()?;
                }
                (Some((new_value_key, new_value)), Some((old_value_key, old_value))) => {
                    match new_value_key.user_key.cmp(&old_value_key.user_key) {
                        Ordering::Less => {
                            self.item_buffer = Some((
                                new_value_key.user_key.table_key.clone(),
                                ChangeLogValue::Insert(new_value.clone()),
                            ));
                            self.new_value_iter.next_inner()?;
                        }
                        Ordering::Greater => {
                            self.item_buffer = Some((
                                old_value_key.user_key.table_key.clone(),
                                ChangeLogValue::Delete(old_value.clone()),
                            ));
                            self.old_value_iter.next_inner()?;
                        }
                        Ordering::Equal => {
                            if new_value == old_value {
                                self.new_value_iter.next_inner()?;
                                self.old_value_iter.next_inner()?;
                                continue;
                            }
                            self.item_buffer = Some((
                                new_value_key.user_key.table_key.clone(),
                                ChangeLogValue::Update {
                                    new_value: new_value.clone(),
                                    old_value: old_value.clone(),
                                },
                            ));
                            self.new_value_iter.next_inner()?;
                            self.old_value_iter.next_inner()?;
                        }
                    }
                }
            };
            break;
        }
        Ok(self
            .item_buffer
            .as_ref()
            .map(|(key, value)| (key.to_ref(), value.to_ref())))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::util::epoch::test_epoch;

    use super::*;
    use crate::hummock::iterator::test_utils::{
        iterator_test_table_key_of, iterator_test_value_of,
    };
    use crate::hummock::test_utils::{ReadOptions, *};
    use crate::memory::sled::SledStateStore;

    #[tokio::test]
    async fn test_snapshot_isolation_memory() {
        let state_store = MemoryStateStore::new();
        test_snapshot_isolation_inner(state_store).await;
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_snapshot_isolation_sled() {
        let state_store = SledStateStore::new_temp();
        test_snapshot_isolation_inner(state_store).await;
    }

    async fn test_snapshot_isolation_inner(state_store: RangeKvStateStore<impl RangeKv>) {
        state_store
            .ingest_batch(
                vec![
                    (
                        TableKey(Bytes::from(b"a".to_vec())),
                        StorageValue::new_put(b"v1".to_vec()),
                    ),
                    (
                        TableKey(Bytes::from(b"b".to_vec())),
                        StorageValue::new_put(b"v1".to_vec()),
                    ),
                ],
                vec![],
                0,
                Default::default(),
            )
            .unwrap();
        state_store
            .ingest_batch(
                vec![
                    (
                        TableKey(Bytes::from(b"a".to_vec())),
                        StorageValue::new_put(b"v2".to_vec()),
                    ),
                    (
                        TableKey(Bytes::from(b"b".to_vec())),
                        StorageValue::new_delete(),
                    ),
                ],
                vec![],
                test_epoch(1),
                Default::default(),
            )
            .unwrap();
        assert_eq!(
            state_store
                .scan(
                    (
                        Bound::Included(TableKey(Bytes::from("a"))),
                        Bound::Included(TableKey(Bytes::from("b"))),
                    ),
                    0,
                    TableId::default(),
                    None,
                )
                .unwrap(),
            vec![
                (
                    FullKey::for_test(Default::default(), Bytes::from("a"), 0)
                        .encode()
                        .into(),
                    b"v1".to_vec().into()
                ),
                (
                    FullKey::for_test(Default::default(), Bytes::from("b"), 0)
                        .encode()
                        .into(),
                    b"v1".to_vec().into()
                )
            ]
        );
        assert_eq!(
            state_store
                .scan(
                    (
                        Bound::Included(TableKey(Bytes::from("a"))),
                        Bound::Included(TableKey(Bytes::from("b"))),
                    ),
                    0,
                    TableId::default(),
                    Some(1),
                )
                .unwrap(),
            vec![(
                FullKey::for_test(Default::default(), b"a".to_vec(), 0)
                    .encode()
                    .into(),
                b"v1".to_vec().into()
            )]
        );
        assert_eq!(
            state_store
                .scan(
                    (
                        Bound::Included(TableKey(Bytes::from("a"))),
                        Bound::Included(TableKey(Bytes::from("b"))),
                    ),
                    test_epoch(1),
                    TableId::default(),
                    None,
                )
                .unwrap(),
            vec![(
                FullKey::for_test(Default::default(), b"a".to_vec(), test_epoch(1))
                    .encode()
                    .into(),
                b"v2".to_vec().into()
            )]
        );
        assert_eq!(
            state_store
                .get(TableKey(Bytes::from("a")), 0, ReadOptions::default())
                .await
                .unwrap(),
            Some(Bytes::from("v1"))
        );
        assert_eq!(
            state_store
                .get(
                    TableKey(Bytes::copy_from_slice(b"b")),
                    0,
                    ReadOptions::default(),
                )
                .await
                .unwrap(),
            Some(b"v1".to_vec().into())
        );
        assert_eq!(
            state_store
                .get(
                    TableKey(Bytes::copy_from_slice(b"c")),
                    0,
                    ReadOptions::default(),
                )
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            state_store
                .get(
                    TableKey(Bytes::copy_from_slice(b"a")),
                    test_epoch(1),
                    ReadOptions::default(),
                )
                .await
                .unwrap(),
            Some(b"v2".to_vec().into())
        );
        assert_eq!(
            state_store
                .get(
                    TableKey(Bytes::from("b")),
                    test_epoch(1),
                    ReadOptions::default(),
                )
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            state_store
                .get(
                    TableKey(Bytes::from("c")),
                    test_epoch(1),
                    ReadOptions::default()
                )
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn test_iter_log_memory() {
        let state_store = MemoryStateStore::new();
        test_iter_log_inner(state_store).await;
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_iter_log_sled() {
        let state_store = SledStateStore::new_temp();
        test_iter_log_inner(state_store).await;
    }

    async fn test_iter_log_inner(state_store: RangeKvStateStore<impl RangeKv>) {
        let table_id = TableId::new(233);
        let epoch1 = test_epoch(1);
        let key_idx = [1, 2, 4];
        let make_key = |i| TableKey(Bytes::from(iterator_test_table_key_of(i)));
        let make_value = |i| Bytes::from(iterator_test_value_of(i));
        state_store
            .ingest_batch(
                key_idx
                    .iter()
                    .map(|i| (make_key(*i), StorageValue::new_put(make_value(*i))))
                    .collect(),
                vec![],
                epoch1,
                table_id,
            )
            .unwrap();
        {
            let mut iter = state_store
                .iter_log(
                    (epoch1, epoch1),
                    (Unbounded, Unbounded),
                    ReadLogOptions { table_id },
                )
                .await
                .unwrap();
            for i in key_idx {
                let (iter_key, change_value) = iter.try_next().await.unwrap().unwrap();
                assert_eq!(make_key(i).to_ref(), iter_key);
                assert_eq!(change_value, ChangeLogValue::Insert(make_value(i).as_ref()));
            }
            assert!(iter.try_next().await.unwrap().is_none());
        }

        let epoch2 = test_epoch(2);
        state_store
            .ingest_batch(
                vec![
                    (make_key(1), StorageValue::new_put(make_value(12))), // update
                    (make_key(2), StorageValue::new_delete()),            // delete
                    (make_key(3), StorageValue::new_put(make_value(3))),
                ],
                vec![],
                epoch2,
                table_id,
            )
            .unwrap();

        // check iter log between two epoch
        {
            let expected = vec![
                (
                    make_key(1),
                    ChangeLogValue::Update {
                        new_value: make_value(12),
                        old_value: make_value(1),
                    },
                ),
                (make_key(2), ChangeLogValue::Delete(make_value(2))),
                (make_key(3), ChangeLogValue::Insert(make_value(3))),
            ];
            let mut iter = state_store
                .iter_log(
                    (epoch2, epoch2),
                    (Unbounded, Unbounded),
                    ReadLogOptions { table_id },
                )
                .await
                .unwrap();
            for (key, change_log_value) in expected {
                let (iter_key, iter_value) = iter.try_next().await.unwrap().unwrap();
                assert_eq!(
                    key.to_ref(),
                    iter_key,
                    "{:?} {:?}",
                    change_log_value.to_ref(),
                    iter_value
                );
                assert_eq!(change_log_value.to_ref(), iter_value);
            }
            assert!(iter.try_next().await.unwrap().is_none());
        }
        // check iter log on the original old epoch
        {
            let mut iter = state_store
                .iter_log(
                    (epoch1, epoch1),
                    (Unbounded, Unbounded),
                    ReadLogOptions { table_id },
                )
                .await
                .unwrap();
            for i in key_idx {
                let (iter_key, change_value) = iter.try_next().await.unwrap().unwrap();
                assert_eq!(make_key(i).to_ref(), iter_key);
                assert_eq!(change_value, ChangeLogValue::Insert(make_value(i).as_ref()));
            }
            assert!(iter.try_next().await.unwrap().is_none());
        }
        // check iter on merging the two epochs
        {
            let mut iter = state_store
                .iter_log(
                    (epoch1, epoch2),
                    (Unbounded, Unbounded),
                    ReadLogOptions { table_id },
                )
                .await
                .unwrap();
            let (iter_key, change_value) = iter.try_next().await.unwrap().unwrap();
            assert_eq!(make_key(1).to_ref(), iter_key);
            assert_eq!(
                change_value,
                ChangeLogValue::Insert(make_value(12).as_ref())
            );
            for i in [3, 4] {
                let (iter_key, change_value) = iter.try_next().await.unwrap().unwrap();
                assert_eq!(make_key(i).to_ref(), iter_key);
                assert_eq!(change_value, ChangeLogValue::Insert(make_value(i).as_ref()));
            }
            assert!(iter.try_next().await.unwrap().is_none());
        }
    }
}
