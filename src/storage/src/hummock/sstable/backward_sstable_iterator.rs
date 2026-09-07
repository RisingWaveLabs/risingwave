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

use std::cmp::Ordering::{Equal, Less};
use std::sync::Arc;

use foyer::Hint;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::sstable_info::SstableInfo;

use crate::hummock::iterator::{Backward, HummockIterator, ValueMeta};
use crate::hummock::sstable::{SstableIteratorReadOptions, SstableMetaHandle};
use crate::hummock::value::HummockValue;
use crate::hummock::{
    BlockIterator, HummockResult, SstableIteratorType, SstableStoreRef, TableHolder,
};
use crate::monitor::StoreLocalStatistic;

/// Iterates backwards on a sstable.
pub struct BackwardSstableIterator {
    /// The iterator of the current block.
    block_iter: Option<BlockIterator>,

    /// Current block index.
    cur_idx: usize,

    /// Reference to the sstable
    sst: TableHolder,

    sstable_store: SstableStoreRef,

    stats: StoreLocalStatistic,

    // used for checking if the block is valid, filter out the block that is not in the table-id range
    read_block_meta_range: (usize, usize),
}

impl BackwardSstableIterator {
    pub fn new(
        sstable: TableHolder,
        sstable_store: SstableStoreRef,
        sstable_info_ref: &SstableInfo,
    ) -> Self {
        let meta_handle = SstableMetaHandle::v2(&sstable);
        let mut start_idx = 0;
        assert!(
            !sstable_info_ref.table_ids.is_empty(),
            "BackwardSstableIterator: SST {} (object {}) has empty table_ids",
            sstable_info_ref.sst_id,
            sstable_info_ref.object_id,
        );
        let read_table_id_range = (
            *sstable_info_ref.table_ids.first().unwrap(),
            *sstable_info_ref.table_ids.last().unwrap(),
        );
        assert!(
            read_table_id_range.0 <= read_table_id_range.1,
            "invalid table id range {} - {}",
            read_table_id_range.0,
            read_table_id_range.1
        );
        let block_meta_count = meta_handle.block_count();
        assert!(block_meta_count > 0);
        let mut end_idx = block_meta_count - 1;
        assert!(
            meta_handle.block_meta(0).table_id() <= read_table_id_range.0,
            "table id {} not found table_ids in block_meta {:?}",
            read_table_id_range.0,
            (0..block_meta_count)
                .map(|idx| meta_handle.block_meta(idx).table_id())
                .collect::<Vec<_>>()
        );
        assert!(
            meta_handle.block_meta(block_meta_count - 1).table_id() >= read_table_id_range.1,
            "table id {} not found table_ids in block_meta {:?}",
            read_table_id_range.1,
            (0..block_meta_count)
                .map(|idx| meta_handle.block_meta(idx).table_id())
                .collect::<Vec<_>>()
        );

        while start_idx < block_meta_count
            && meta_handle.block_meta(start_idx).table_id() < read_table_id_range.0
        {
            start_idx += 1;
        }
        // We assume that the table id read must exist in the sstable, otherwise it is a fatal error.
        assert!(
            start_idx < block_meta_count,
            "table id {} not found table_ids in block_meta {:?}",
            read_table_id_range.0,
            (0..block_meta_count)
                .map(|idx| meta_handle.block_meta(idx).table_id())
                .collect::<Vec<_>>()
        );

        while end_idx > start_idx
            && meta_handle.block_meta(end_idx).table_id() > read_table_id_range.1
        {
            end_idx -= 1;
        }
        assert!(
            end_idx >= start_idx,
            "end_idx {} < start_idx {} block_meta_count {}",
            end_idx,
            start_idx,
            block_meta_count
        );

        Self {
            block_iter: None,
            cur_idx: end_idx,
            sst: sstable,
            sstable_store,
            stats: StoreLocalStatistic::default(),
            read_block_meta_range: (start_idx, end_idx),
        }
    }

    /// Seeks to a block, and then seeks to the key if `seek_key` is given.
    async fn seek_idx(
        &mut self,
        idx: isize,
        seek_key: Option<FullKey<&[u8]>>,
    ) -> HummockResult<()> {
        let meta_handle = SstableMetaHandle::v2(&self.sst);
        if idx >= meta_handle.block_count() as isize || idx < self.read_block_meta_range.0 as isize
        {
            self.block_iter = None;
        } else {
            let block = self
                .sstable_store
                .get(
                    &self.sst,
                    idx as usize,
                    crate::hummock::CachePolicy::Fill(Hint::Normal),
                    &mut self.stats,
                )
                .await?;
            let mut block_iter = BlockIterator::new(block);
            if let Some(key) = seek_key {
                block_iter.seek_le(key);
            } else {
                block_iter.seek_to_last();
            }

            self.block_iter = Some(block_iter);
            self.cur_idx = idx as usize;
        }

        Ok(())
    }
}

impl HummockIterator for BackwardSstableIterator {
    type Direction = Backward;

    async fn next(&mut self) -> HummockResult<()> {
        self.stats.total_key_count += 1;
        let block_iter = self.block_iter.as_mut().expect("no block iter");
        if block_iter.try_prev() {
            Ok(())
        } else {
            // seek to the previous block
            self.seek_idx(self.cur_idx as isize - 1, None).await
        }
    }

    fn key(&self) -> FullKey<&[u8]> {
        self.block_iter.as_ref().expect("no block iter").key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        let raw_value = self.block_iter.as_ref().expect("no block iter").value();

        HummockValue::from_slice(raw_value).expect("decode error")
    }

    fn is_valid(&self) -> bool {
        self.block_iter.as_ref().is_some_and(|i| i.is_valid())
    }

    /// Instead of setting idx to 0th block, a `BackwardSstableIterator` rewinds to the last block
    /// in the sstable.
    async fn rewind(&mut self) -> HummockResult<()> {
        self.seek_idx(self.read_block_meta_range.1 as isize, None)
            .await
    }

    async fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> HummockResult<()> {
        let meta_handle = SstableMetaHandle::v2(&self.sst);
        let block_idx = meta_handle
            .block_metas_partition_point(0..=meta_handle.block_count() - 1, |block_meta| {
                // Compare by version comparator
                // Note: we are comparing against the `smallest_key` of the `block`, thus the
                // partition point should be `prev(<=)` instead of `<`.
                let ord = FullKey::decode(&block_meta.smallest_key).cmp(&key);
                ord == Less || ord == Equal
            })
            .saturating_sub(1); // considering the boundary of 0
        let block_idx = block_idx as isize;

        self.seek_idx(block_idx, Some(key)).await?;
        if !self.is_valid() {
            // Seek to prev block
            self.seek_idx(block_idx - 1, None).await?;
        }

        Ok(())
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        stats.add(&self.stats)
    }

    fn value_meta(&self) -> ValueMeta {
        ValueMeta {
            object_id: Some(self.sst.id),
            block_id: Some(self.cur_idx as _),
        }
    }
}

impl SstableIteratorType for BackwardSstableIterator {
    fn create(
        sstable: TableHolder,
        sstable_store: SstableStoreRef,
        _: Arc<SstableIteratorReadOptions>,
        sstable_info_ref: &SstableInfo,
    ) -> Self {
        BackwardSstableIterator::new(sstable, sstable_store, sstable_info_ref)
    }
}

/// Mirror the tests used for `SstableIterator`
#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rand::prelude::*;
    use rand::rng as thread_rng;
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::EpochWithGap;
    use risingwave_hummock_sdk::key::UserKey;
    use risingwave_hummock_sdk::sstable_info::SstableInfoInner;

    use super::*;
    use crate::assert_bytes_eq;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{
        TEST_KEYS_COUNT, default_builder_opt_for_test, gen_default_test_sstable,
        gen_test_sstable_with_table_ids, test_key_of, test_value_of,
    };

    #[tokio::test]
    async fn test_backward_sstable_iterator() {
        // build remote sstable
        let sstable_store = mock_sstable_store().await;
        let (handle, sstable_info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                .await;
        // We should have at least 10 blocks, so that sstable iterator test could cover more code
        // path.
        assert!(handle.block_count() > 10);
        let mut sstable_iter = BackwardSstableIterator::new(handle, sstable_store, &sstable_info);
        let mut cnt = TEST_KEYS_COUNT;
        sstable_iter.rewind().await.unwrap();

        while sstable_iter.is_valid() {
            cnt -= 1;
            let key = sstable_iter.key();
            let value = sstable_iter.value();
            assert_eq!(key, test_key_of(cnt).to_ref());
            assert_bytes_eq!(value.into_user_value().unwrap(), test_value_of(cnt));
            sstable_iter.next().await.unwrap();
        }

        assert_eq!(cnt, 0);
    }

    #[tokio::test]
    async fn test_backward_sstable_seek() {
        let sstable_store = mock_sstable_store().await;
        let (sstable, sstable_info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                .await;
        // We should have at least 10 blocks, so that sstable iterator test could cover more code
        // path.
        assert!(sstable.block_count() > 10);
        let mut sstable_iter = BackwardSstableIterator::new(sstable, sstable_store, &sstable_info);
        let mut all_key_to_test = (0..TEST_KEYS_COUNT).collect_vec();
        let mut rng = thread_rng();
        all_key_to_test.shuffle(&mut rng);

        // We seek and access all the keys in random order
        for i in all_key_to_test {
            sstable_iter.seek(test_key_of(i).to_ref()).await.unwrap();
            // sstable_iter.next().await.unwrap();
            let key = sstable_iter.key();
            assert_eq!(key, test_key_of(i).to_ref());
        }

        // Seek to key #TEST_KEYS_COUNT-500 and start iterating
        sstable_iter
            .seek(test_key_of(TEST_KEYS_COUNT - 500).to_ref())
            .await
            .unwrap();
        for i in (0..TEST_KEYS_COUNT - 500 + 1).rev() {
            let key = sstable_iter.key();
            assert_eq!(key, test_key_of(i).to_ref(), "key index:{}", i);
            sstable_iter.next().await.unwrap();
        }
        assert!(!sstable_iter.is_valid());

        let largest_key = FullKey::for_test(
            TableId::default(),
            [
                VirtualNode::ZERO.to_be_bytes().as_slice(),
                format!("key_zzzz_{:05}", 0).as_bytes(),
            ]
            .concat(),
            test_epoch(1),
        );
        sstable_iter.seek(largest_key.to_ref()).await.unwrap();
        let key = sstable_iter.key();
        assert_eq!(key, test_key_of(TEST_KEYS_COUNT - 1).to_ref());

        // Seek to > last key
        let smallest_key = FullKey::for_test(
            TableId::default(),
            [
                VirtualNode::ZERO.to_be_bytes().as_slice(),
                format!("key_aaaa_{:05}", 0).as_bytes(),
            ]
            .concat(),
            test_epoch(1),
        );
        sstable_iter.seek(smallest_key.to_ref()).await.unwrap();
        assert!(!sstable_iter.is_valid());

        // Seek to non-existing key
        for idx in (1..TEST_KEYS_COUNT).rev() {
            // Seek to the previous key of each existing key. e.g.,
            // Our key space is `key_test_00000`, `key_test_00002`, `key_test_00004`, ...
            // And we seek to `key_test_00001` (will produce `key_test_00002`), `key_test_00003`
            // (will produce `key_test_00004`).
            sstable_iter
                .seek(
                    FullKey::for_test(
                        TableId::default(),
                        [
                            VirtualNode::ZERO.to_be_bytes().as_slice(),
                            format!("key_test_{:05}", idx * 2 - 1).as_bytes(),
                        ]
                        .concat(),
                        0,
                    )
                    .to_ref(),
                )
                .await
                .unwrap();

            let key = sstable_iter.key();
            assert_eq!(key, test_key_of(idx - 1).to_ref());
            sstable_iter.next().await.unwrap();
        }
        assert!(!sstable_iter.is_valid());
    }

    #[tokio::test]
    async fn test_read_table_id_range() {
        {
            let sstable_store = mock_sstable_store().await;
            // test key_range right
            let k1 = {
                let mut table_key = VirtualNode::ZERO.to_be_bytes().to_vec();
                table_key.extend_from_slice(format!("key_test_{:05}", 1).as_bytes());
                let uk = UserKey::for_test(TableId::from(1), table_key);
                FullKey {
                    user_key: uk,
                    epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(1)),
                }
            };

            let k2 = {
                let mut table_key = VirtualNode::ZERO.to_be_bytes().to_vec();
                table_key.extend_from_slice(format!("key_test_{:05}", 2).as_bytes());
                let uk = UserKey::for_test(TableId::from(2), table_key);
                FullKey {
                    user_key: uk,
                    epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(1)),
                }
            };

            let k3 = {
                let mut table_key = VirtualNode::ZERO.to_be_bytes().to_vec();
                table_key.extend_from_slice(format!("key_test_{:05}", 3).as_bytes());
                let uk = UserKey::for_test(TableId::from(3), table_key);
                FullKey {
                    user_key: uk,
                    epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(1)),
                }
            };

            {
                let kv_pairs = vec![
                    (k1.clone(), HummockValue::put(test_value_of(1))),
                    (k2.clone(), HummockValue::put(test_value_of(2))),
                    (k3.clone(), HummockValue::put(test_value_of(3))),
                ];

                let (sstable, _sstable_info) = gen_test_sstable_with_table_ids(
                    default_builder_opt_for_test(),
                    10,
                    kv_pairs.into_iter(),
                    sstable_store.clone(),
                    vec![1, 2, 3],
                )
                .await;
                let mut sstable_iter = BackwardSstableIterator::create(
                    sstable,
                    sstable_store.clone(),
                    Arc::new(SstableIteratorReadOptions::default()),
                    &SstableInfo::from(SstableInfoInner {
                        table_ids: vec![1.into(), 2.into(), 3.into()],
                        ..Default::default()
                    }),
                );
                sstable_iter.rewind().await.unwrap();
                assert!(sstable_iter.is_valid());
                assert!(sstable_iter.key().eq(&k3.to_ref()));

                let mut cnt = 0;
                let mut last_key = k1.clone();
                while sstable_iter.is_valid() {
                    last_key = sstable_iter.key().to_vec();
                    cnt += 1;
                    sstable_iter.next().await.unwrap();
                }

                assert_eq!(3, cnt);
                assert_eq!(last_key, k1.clone());
            }

            {
                let kv_pairs = vec![
                    (k1.clone(), HummockValue::put(test_value_of(1))),
                    (k2.clone(), HummockValue::put(test_value_of(2))),
                    (k3.clone(), HummockValue::put(test_value_of(3))),
                ];

                let (sstable, _sstable_info) = gen_test_sstable_with_table_ids(
                    default_builder_opt_for_test(),
                    10,
                    kv_pairs.into_iter(),
                    sstable_store.clone(),
                    vec![1, 2, 3],
                )
                .await;

                let mut sstable_iter = BackwardSstableIterator::create(
                    sstable,
                    sstable_store.clone(),
                    Arc::new(SstableIteratorReadOptions::default()),
                    &SstableInfo::from(SstableInfoInner {
                        table_ids: vec![1.into(), 2.into()],
                        ..Default::default()
                    }),
                );
                sstable_iter.rewind().await.unwrap();
                assert!(sstable_iter.is_valid());
                assert!(sstable_iter.key().eq(&k2.to_ref()));

                let mut cnt = 0;
                let mut last_key = k1.clone();
                while sstable_iter.is_valid() {
                    last_key = sstable_iter.key().to_vec();
                    cnt += 1;
                    sstable_iter.next().await.unwrap();
                }

                assert_eq!(2, cnt);
                assert_eq!(last_key, k1.clone());
            }

            {
                let kv_pairs = vec![
                    (k1.clone(), HummockValue::put(test_value_of(1))),
                    (k2.clone(), HummockValue::put(test_value_of(2))),
                    (k3.clone(), HummockValue::put(test_value_of(3))),
                ];

                let (sstable, _sstable_info) = gen_test_sstable_with_table_ids(
                    default_builder_opt_for_test(),
                    10,
                    kv_pairs.into_iter(),
                    sstable_store.clone(),
                    vec![1, 2, 3],
                )
                .await;

                let mut sstable_iter = BackwardSstableIterator::create(
                    sstable,
                    sstable_store.clone(),
                    Arc::new(SstableIteratorReadOptions::default()),
                    &SstableInfo::from(SstableInfoInner {
                        table_ids: vec![2.into(), 3.into()],
                        ..Default::default()
                    }),
                );
                sstable_iter.rewind().await.unwrap();
                assert!(sstable_iter.is_valid());
                assert!(sstable_iter.key().eq(&k3.to_ref()));

                let mut cnt = 0;
                let mut last_key = k1.clone();
                while sstable_iter.is_valid() {
                    last_key = sstable_iter.key().to_vec();
                    cnt += 1;
                    sstable_iter.next().await.unwrap();
                }

                assert_eq!(2, cnt);
                assert_eq!(last_key, k2.clone());
            }

            {
                let kv_pairs = vec![
                    (k1.clone(), HummockValue::put(test_value_of(1))),
                    (k2.clone(), HummockValue::put(test_value_of(2))),
                    (k3.clone(), HummockValue::put(test_value_of(3))),
                ];

                let (sstable, _sstable_info) = gen_test_sstable_with_table_ids(
                    default_builder_opt_for_test(),
                    10,
                    kv_pairs.into_iter(),
                    sstable_store.clone(),
                    vec![1, 2, 3],
                )
                .await;

                let mut sstable_iter = BackwardSstableIterator::create(
                    sstable,
                    sstable_store.clone(),
                    Arc::new(SstableIteratorReadOptions::default()),
                    &SstableInfo::from(SstableInfoInner {
                        table_ids: vec![2.into()],
                        ..Default::default()
                    }),
                );
                sstable_iter.rewind().await.unwrap();
                assert!(sstable_iter.is_valid());
                assert!(sstable_iter.key().eq(&k2.to_ref()));

                let mut cnt = 0;
                let mut last_key = k1.clone();
                while sstable_iter.is_valid() {
                    last_key = sstable_iter.key().to_vec();
                    cnt += 1;
                    sstable_iter.next().await.unwrap();
                }

                assert_eq!(1, cnt);
                assert_eq!(last_key, k2.clone());
            }
        }
    }
}
