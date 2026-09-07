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

use std::ops::Bound::Unbounded;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::hummock::compactor::fast_compactor_runner::BlockStreamIterator;
use crate::hummock::compactor::{SstableStreamIterator, TaskProgress};
use crate::hummock::iterator::test_utils::{
    TEST_KEYS_COUNT, gen_iterator_test_sstable_base, gen_iterator_test_sstable_info,
    iterator_test_bytes_key_of, iterator_test_key_of, iterator_test_user_key_of,
    iterator_test_value_of, mock_sstable_store,
};
use crate::hummock::iterator::{
    BackwardConcatIterator, BackwardUserIterator, ConcatIterator, HummockIterator, MergeIterator,
    UserIterator,
};
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::test_utils::{
    default_builder_opt_for_test, default_writer_opt_for_test, gen_test_sstable_data, put_sst,
    test_key_of, test_value_of,
};
use crate::hummock::value::HummockValue;
use crate::hummock::{BackwardSstableIterator, SstableIterator};
use crate::monitor::StoreLocalStatistic;

#[tokio::test]
#[cfg(feature = "failpoints")]
async fn test_failpoints_concat_read_err() {
    fail::cfg("disable_block_cache", "return").unwrap();
    let mem_read_err = "mem_read_err";
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
    let mut iter = ConcatIterator::new(
        vec![table0, table1],
        sstable_store,
        Arc::new(SstableIteratorReadOptions::default()),
    );
    iter.rewind().await.unwrap();
    fail::cfg(mem_read_err, "return").unwrap();
    let result = iter.seek(iterator_test_key_of(22).to_ref()).await;
    assert!(result.is_err());
    let result = iter
        .seek(iterator_test_key_of(4 * TEST_KEYS_COUNT).to_ref())
        .await;
    assert!(result.is_err());
    let result = iter.seek(iterator_test_key_of(23).to_ref()).await;
    assert!(result.is_err());
    fail::remove(mem_read_err);
    iter.rewind().await.unwrap();
    fail::cfg(mem_read_err, "return").unwrap();
    let mut i = 0;
    while iter.is_valid() {
        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(i * 2).to_ref());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(i * 2).as_slice()
        );
        i += 1;
        let result = iter.next().await;
        if result.is_err() {
            assert!(i < 2 * TEST_KEYS_COUNT);
            break;
        }
    }
    assert!(i < 2 * TEST_KEYS_COUNT);
    assert!(!iter.is_valid());
    fail::remove(mem_read_err);
}
#[tokio::test]
#[cfg(feature = "failpoints")]
async fn test_failpoints_backward_concat_read_err() {
    fail::cfg("disable_block_cache", "return").unwrap();
    let mem_read_err = "mem_read_err";
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
    let mut iter = BackwardConcatIterator::new(
        vec![table1, table0],
        sstable_store.clone(),
        Arc::new(SstableIteratorReadOptions::default()),
    );
    iter.rewind().await.unwrap();
    fail::cfg(mem_read_err, "return").unwrap();
    let result = iter.seek(iterator_test_key_of(2).to_ref()).await;
    assert!(result.is_err());
    let result = iter.seek(iterator_test_key_of(3).to_ref()).await;
    assert!(result.is_err());
    fail::remove(mem_read_err);
    iter.rewind().await.unwrap();
    fail::cfg(mem_read_err, "return").unwrap();
    let mut i = TEST_KEYS_COUNT * 2;
    while iter.is_valid() {
        i -= 1;
        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(i * 2).to_ref());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(i * 2).as_slice()
        );
        let result = iter.next().await;
        if result.is_err() {
            assert!(i > 0);
            break;
        }
    }
    assert!(i > 0);
    assert!(!iter.is_valid());
    fail::remove(mem_read_err);
}
#[tokio::test]
#[cfg(feature = "failpoints")]
async fn test_failpoints_merge_invalid_key() {
    fail::cfg("disable_block_cache", "return").unwrap();
    let mem_read_err = "mem_read_err";
    let sstable_store = mock_sstable_store().await;
    let (table0, sstable_info_0) = gen_iterator_test_sstable_base(
        0,
        default_builder_opt_for_test(),
        |x| x,
        sstable_store.clone(),
        200,
    )
    .await;
    let (table1, sstable_info_1) = gen_iterator_test_sstable_base(
        1,
        default_builder_opt_for_test(),
        |x| 200 + x,
        sstable_store.clone(),
        200,
    )
    .await;
    let tables = vec![(table0, sstable_info_0), (table1, sstable_info_1)];
    let mut mi = MergeIterator::new({
        let mut iters = vec![];
        for (table, sstable_info) in tables {
            iters.push(SstableIterator::new(
                table,
                sstable_store.clone(),
                Arc::new(SstableIteratorReadOptions::default()),
                &sstable_info,
            ));
        }
        iters
    });
    mi.rewind().await.unwrap();
    let mut count = 0;
    fail::cfg(mem_read_err, "return").unwrap();
    while mi.is_valid() {
        count += 1;
        if (mi.next().await).is_err() {
            assert!(count < 200 * 2);
        }
    }
    assert!(count < 200 * 2);
    mi.seek(iterator_test_key_of(350).to_ref()).await.unwrap();
    assert!(!mi.is_valid());
    fail::remove(mem_read_err);
}
#[tokio::test]
#[cfg(feature = "failpoints")]
async fn test_failpoints_backward_merge_invalid_key() {
    fail::cfg("disable_block_cache", "return").unwrap();
    let mem_read_err = "mem_read_err";
    let sstable_store = mock_sstable_store().await;
    let (table0, sstable_info_0) = gen_iterator_test_sstable_base(
        0,
        default_builder_opt_for_test(),
        |x| x,
        sstable_store.clone(),
        200,
    )
    .await;
    let (table1, sstable_info_1) = gen_iterator_test_sstable_base(
        1,
        default_builder_opt_for_test(),
        |x| 200 + x,
        sstable_store.clone(),
        200,
    )
    .await;
    let tables = vec![(table0, sstable_info_0), (table1, sstable_info_1)];
    let mut mi = MergeIterator::new({
        let mut iters = vec![];
        for (table, sstable_info) in tables {
            iters.push(BackwardSstableIterator::new(
                table,
                sstable_store.clone(),
                &sstable_info,
            ));
        }
        iters
    });
    mi.rewind().await.unwrap();
    let mut count = 0;
    fail::cfg(mem_read_err, "return").unwrap();
    while mi.is_valid() {
        count += 1;
        if (mi.next().await).is_err() {
            assert!(count < 200 * 2);
        }
    }
    assert!(count < 200 * 2);
    mi.seek(iterator_test_key_of(10).to_ref()).await.unwrap();
    assert!(!mi.is_valid());
    fail::remove(mem_read_err);
}
#[tokio::test]
#[cfg(feature = "failpoints")]
async fn test_failpoints_user_read_err() {
    fail::cfg("disable_block_cache", "return").unwrap();
    let mem_read_err = "mem_read_err";
    let sstable_store = mock_sstable_store().await;
    let (table0, sstable_info_0) = gen_iterator_test_sstable_base(
        0,
        default_builder_opt_for_test(),
        |x| x,
        sstable_store.clone(),
        200,
    )
    .await;
    let (table1, sstable_info_1) = gen_iterator_test_sstable_base(
        1,
        default_builder_opt_for_test(),
        |x| 200 + x,
        sstable_store.clone(),
        200,
    )
    .await;
    let iters = vec![
        SstableIterator::new(
            table0,
            sstable_store.clone(),
            Arc::new(SstableIteratorReadOptions::default()),
            &sstable_info_0,
        ),
        SstableIterator::new(
            table1,
            sstable_store.clone(),
            Arc::new(SstableIteratorReadOptions::default()),
            &sstable_info_1,
        ),
    ];

    let mi = MergeIterator::new(iters);
    let mut ui = UserIterator::for_test(mi, (Unbounded, Unbounded));
    ui.rewind().await.unwrap();

    fail::cfg(mem_read_err, "return").unwrap();
    let mut i = 0;
    while ui.is_valid() {
        let key = ui.key();
        let val = ui.value();
        assert_eq!(key, iterator_test_bytes_key_of(i).to_ref());
        assert_eq!(val, iterator_test_value_of(i).as_slice());
        i += 1;
        let result = ui.next().await;
        if result.is_err() {
            assert!(i < 400);
        }
    }
    assert!(i < 400);
    ui.seek(iterator_test_user_key_of(350).as_ref())
        .await
        .unwrap();
    assert!(!ui.is_valid());
    fail::remove(mem_read_err);
}

#[tokio::test]
#[cfg(feature = "failpoints")]
async fn test_failpoints_backward_user_read_err() {
    fail::cfg("disable_block_cache", "return").unwrap();
    let mem_read_err = "mem_read_err";
    let sstable_store = mock_sstable_store().await;
    let (table0, sstable_info_0) = gen_iterator_test_sstable_base(
        0,
        default_builder_opt_for_test(),
        |x| x,
        sstable_store.clone(),
        200,
    )
    .await;
    let (table1, sstable_info_1) = gen_iterator_test_sstable_base(
        1,
        default_builder_opt_for_test(),
        |x| 200 + x,
        sstable_store.clone(),
        200,
    )
    .await;
    let iters = vec![
        BackwardSstableIterator::new(table0, sstable_store.clone(), &sstable_info_0),
        BackwardSstableIterator::new(table1, sstable_store.clone(), &sstable_info_1),
    ];

    let mi = MergeIterator::new(iters);
    let mut ui = BackwardUserIterator::for_test(mi, (Unbounded, Unbounded));
    ui.rewind().await.unwrap();

    fail::cfg(mem_read_err, "return").unwrap();
    let mut i = 2 * 200;
    while ui.is_valid() {
        i -= 1;
        let key = ui.key();
        let val = ui.value();
        assert_eq!(key, iterator_test_bytes_key_of(i).to_ref());
        assert_eq!(val, iterator_test_value_of(i).as_slice());
        let result = ui.next().await;
        if result.is_err() {
            assert!(i > 0);
        }
    }
    assert!(i > 0);
    ui.seek(iterator_test_user_key_of(10).as_ref())
        .await
        .unwrap();
    assert!(!ui.is_valid());
    fail::remove(mem_read_err);
}

#[tokio::test]
#[cfg(feature = "failpoints")]
async fn test_failpoints_compactor_iterator_recreate() {
    let get_stream_err = "get_stream_err";
    let stream_read_err = "stream_read_err";
    let create_stream_err = "create_stream_err";
    let sstable_store = mock_sstable_store().await;
    // when upload data is successful, but upload meta is fail and delete is fail
    let has_create = Arc::new(AtomicBool::new(false));
    fail::cfg_callback(get_stream_err, move || {
        if has_create.load(Ordering::Acquire) {
            fail::remove(stream_read_err);
            fail::remove(get_stream_err);
        } else {
            has_create.store(true, Ordering::Release);
            fail::cfg(stream_read_err, "return").unwrap();
        }
    })
    .unwrap();
    let meet_err = Arc::new(AtomicBool::new(false));
    let other = meet_err.clone();
    fail::cfg_callback(create_stream_err, move || {
        other.store(true, Ordering::Release);
    })
    .unwrap();

    let table_id = 0;
    let kv_iter =
        (0..TEST_KEYS_COUNT).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i))));
    let (data, meta) = gen_test_sstable_data(default_builder_opt_for_test(), kv_iter).await;
    let info = put_sst(
        table_id,
        data.clone(),
        meta.clone(),
        sstable_store.clone(),
        default_writer_opt_for_test(),
        vec![table_id as u32],
    )
    .await
    .unwrap();

    let mut stats = StoreLocalStatistic::default();

    let table = sstable_store.sstable(&info, &mut stats).await.unwrap();
    let block_metas_range = 0..table.block_count();
    let mut sstable_iter = SstableStreamIterator::new(
        table,
        block_metas_range,
        info,
        &stats,
        Arc::new(TaskProgress::default()),
        sstable_store,
        100,
    );
    let mut cnt = 0;
    sstable_iter.seek(None).await.unwrap();
    while sstable_iter.is_valid() {
        let key = sstable_iter.key();
        let value = sstable_iter.value();
        assert_eq!(key, test_key_of(cnt).to_ref());
        let expected = test_value_of(cnt);
        let expected_slice = expected.as_slice();
        assert_eq!(value.into_user_value().unwrap(), expected_slice);
        cnt += 1;
        sstable_iter.next().await.unwrap();
    }
    assert_eq!(cnt, TEST_KEYS_COUNT);
    assert!(meet_err.load(Ordering::Acquire));
}

#[tokio::test]
#[cfg(feature = "failpoints")]
async fn test_failpoints_fast_compactor_iterator_recreate() {
    let get_stream_err = "get_stream_err";
    let stream_read_err = "stream_read_err";
    let create_stream_err = "create_stream_err";
    let sstable_store = mock_sstable_store().await;
    // when upload data is successful, but upload meta is fail and delete is fail
    let has_create = Arc::new(AtomicBool::new(false));
    fail::cfg_callback(get_stream_err, move || {
        if has_create.load(Ordering::Acquire) {
            fail::remove(stream_read_err);
            fail::remove(get_stream_err);
        } else {
            has_create.store(true, Ordering::Release);
            fail::cfg(stream_read_err, "return").unwrap();
        }
    })
    .unwrap();
    let meet_err = Arc::new(AtomicBool::new(false));
    let other = meet_err.clone();
    fail::cfg_callback(create_stream_err, move || {
        other.store(true, Ordering::Release);
    })
    .unwrap();

    let table_id = 0;
    let kv_iter =
        (0..TEST_KEYS_COUNT).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i))));
    let (data, meta) = gen_test_sstable_data(default_builder_opt_for_test(), kv_iter).await;
    let info = put_sst(
        table_id,
        data.clone(),
        meta.clone(),
        sstable_store.clone(),
        default_writer_opt_for_test(),
        vec![table_id as u32],
    )
    .await
    .unwrap();

    let mut stats = StoreLocalStatistic::default();

    let table = sstable_store.sstable(&info, &mut stats).await.unwrap();
    let mut sstable_iter = BlockStreamIterator::new(
        table,
        Arc::new(TaskProgress::default()),
        sstable_store.clone(),
        info.clone(),
        10,
        Arc::new(AtomicU64::new(0)),
    );

    let mut cnt = 0;
    while sstable_iter.is_valid() {
        let (buf, _, meta) = match sstable_iter.download_next_block().await.unwrap() {
            Some(x) => x,
            None => break,
        };
        sstable_iter
            .init_block_iter(buf, meta.uncompressed_size as usize)
            .unwrap();

        let block_iter = sstable_iter.iter_mut();

        while block_iter.is_valid() {
            let key = block_iter.key();
            let value = HummockValue::from_slice(block_iter.value()).unwrap();
            assert_eq!(test_key_of(cnt).to_ref(), key);
            let expected = test_value_of(cnt);
            let expected_slice = expected.as_slice();
            assert_eq!(value.into_user_value().unwrap(), expected_slice);
            cnt += 1;
            block_iter.next();
        }
    }
    assert_eq!(cnt, TEST_KEYS_COUNT);
    assert!(meet_err.load(Ordering::Acquire));
}
