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

use crate::hummock::SstableIterator;
use crate::hummock::iterator::concat_inner::ConcatIteratorInner;

/// Iterates on multiple non-overlapping tables.
pub type ConcatIterator = ConcatIteratorInner<SstableIterator>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    #[cfg(feature = "failpoints")]
    use foyer::Hint;

    use super::*;
    #[cfg(feature = "failpoints")]
    use crate::hummock::CachePolicy;
    use crate::hummock::iterator::HummockIterator;
    use crate::hummock::iterator::test_utils::{
        TEST_KEYS_COUNT, default_builder_opt_for_test, gen_iterator_test_sstable_info,
        iterator_test_key_of, iterator_test_value_of, mock_sstable_store,
    };
    use crate::hummock::sstable::SstableIteratorReadOptions;
    #[cfg(feature = "failpoints")]
    use crate::monitor::StoreLocalStatistic;

    #[tokio::test]
    async fn test_concat_iterator() {
        let sstable_store = mock_sstable_store().await;
        let table0 = gen_iterator_test_sstable_info(
            0,
            default_builder_opt_for_test(),
            |x| x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table1 = gen_iterator_test_sstable_info(
            1,
            default_builder_opt_for_test(),
            |x| TEST_KEYS_COUNT + x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table2 = gen_iterator_test_sstable_info(
            2,
            default_builder_opt_for_test(),
            |x| TEST_KEYS_COUNT * 2 + x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let mut iter = ConcatIterator::new(
            vec![table0, table1, table2],
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
        );
        let mut i = 0;
        iter.rewind().await.unwrap();

        while iter.is_valid() {
            let key = iter.key();
            let val = iter.value();
            assert_eq!(key, iterator_test_key_of(i).to_ref());
            assert_eq!(
                val.into_user_value().unwrap(),
                iterator_test_value_of(i).as_slice()
            );
            i += 1;
            iter.next().await.unwrap();
            if i == TEST_KEYS_COUNT * 3 {
                assert!(!iter.is_valid());
                break;
            }
        }

        iter.rewind().await.unwrap();
        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(0).to_ref());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(0).as_slice()
        );
    }

    #[tokio::test]
    async fn test_concat_seek() {
        let sstable_store = mock_sstable_store().await;
        let table0 = gen_iterator_test_sstable_info(
            0,
            default_builder_opt_for_test(),
            |x| x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table1 = gen_iterator_test_sstable_info(
            1,
            default_builder_opt_for_test(),
            |x| TEST_KEYS_COUNT + x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table2 = gen_iterator_test_sstable_info(
            2,
            default_builder_opt_for_test(),
            |x| TEST_KEYS_COUNT * 2 + x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let mut iter = ConcatIterator::new(
            vec![table0, table1, table2],
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
        );

        iter.seek(iterator_test_key_of(TEST_KEYS_COUNT + 1).to_ref())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(TEST_KEYS_COUNT + 1).to_ref());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(TEST_KEYS_COUNT + 1).as_slice()
        );

        // Left edge case
        iter.seek(iterator_test_key_of(0).to_ref()).await.unwrap();
        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(0).to_ref());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(0).as_slice()
        );

        // Right edge case
        iter.seek(iterator_test_key_of(3 * TEST_KEYS_COUNT - 1).to_ref())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(3 * TEST_KEYS_COUNT - 1).to_ref());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(3 * TEST_KEYS_COUNT - 1).as_slice()
        );

        // Right overflow case
        iter.seek(iterator_test_key_of(3 * TEST_KEYS_COUNT).to_ref())
            .await
            .unwrap();
        assert!(!iter.is_valid());
    }

    #[tokio::test]
    async fn test_concat_seek_not_exists() {
        let sstable_store = mock_sstable_store().await;
        let table0 = gen_iterator_test_sstable_info(
            0,
            default_builder_opt_for_test(),
            |x| x * 2,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table1 = gen_iterator_test_sstable_info(
            1,
            default_builder_opt_for_test(),
            |x| (TEST_KEYS_COUNT + x) * 2,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table2 = gen_iterator_test_sstable_info(
            2,
            default_builder_opt_for_test(),
            |x| (2 * TEST_KEYS_COUNT + x) * 2,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let mut iter = ConcatIterator::new(
            vec![table0, table1, table2],
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
        );

        iter.seek(iterator_test_key_of(TEST_KEYS_COUNT + 1).to_ref())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(TEST_KEYS_COUNT + 2).to_ref());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(TEST_KEYS_COUNT + 2).as_slice()
        );

        // seek the last of table1
        iter.seek(iterator_test_key_of((TEST_KEYS_COUNT + 9) * 2 + 1).to_ref())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(TEST_KEYS_COUNT * 4).to_ref());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(TEST_KEYS_COUNT * 4).as_slice()
        );
    }

    #[tokio::test]
    #[cfg(feature = "failpoints")]
    async fn test_concat_init_error_retains_current_iterator_and_candidate_stats() {
        let sstable_store = mock_sstable_store().await;
        let table0 = gen_iterator_test_sstable_info(
            0,
            default_builder_opt_for_test(),
            |x| x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table1 = gen_iterator_test_sstable_info(
            1,
            default_builder_opt_for_test(),
            |x| TEST_KEYS_COUNT + x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;

        // Warm the candidate's first block so its initialization records a cache hit.
        let mut warmup_stats = StoreLocalStatistic::default();
        let sstable = sstable_store
            .sstable(&table1, &mut warmup_stats)
            .await
            .unwrap();
        sstable_store
            .get(
                &sstable,
                0,
                CachePolicy::Fill(Hint::Normal),
                &mut warmup_stats,
            )
            .await
            .unwrap();
        warmup_stats.discard();

        let mut iter = ConcatIterator::new(
            vec![table0, table1],
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
        );
        iter.rewind().await.unwrap();

        let mut before = StoreLocalStatistic::default();
        iter.collect_local_statistic(&mut before);

        fail::cfg("concat_iter_after_seek_before_init_complete", "return").unwrap();
        let result = iter
            .seek(iterator_test_key_of(TEST_KEYS_COUNT).to_ref())
            .await;
        fail::remove("concat_iter_after_seek_before_init_complete");

        assert!(result.is_err());
        assert!(iter.is_valid());
        assert_eq!(iter.key(), iterator_test_key_of(0).to_ref());

        let mut after = StoreLocalStatistic::default();
        iter.collect_local_statistic(&mut after);
        assert_eq!(
            after.cache_data_block_total,
            before.cache_data_block_total + 1,
            "failed candidate initialization must settle its cache-hit statistic"
        );
    }
}
