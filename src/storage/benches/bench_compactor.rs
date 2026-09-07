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

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use criterion::async_executor::FuturesExecutor;
use criterion::{Criterion, criterion_group, criterion_main};
use foyer::{CacheBuilder, Hint, HybridCacheBuilder};
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::config::{MetricLevel, ObjectStoreConfig};
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;
use risingwave_common::util::value_encoding::ValueRowSerializer;
use risingwave_common::util::value_encoding::column_aware_row_encoding::ColumnAwareSerde;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{InMemObjectStore, ObjectStore, ObjectStoreImpl};
use risingwave_pb::hummock::{PbSstableFilterLayout, PbTableSchema};
use risingwave_storage::compaction_catalog_manager::CompactionCatalogAgent;
use risingwave_storage::hummock::compactor::compactor_runner::compact_and_build_sst;
use risingwave_storage::hummock::compactor::{
    ConcatSstableIterator, DummyCompactionFilter, TaskConfig, TaskProgress,
};
use risingwave_storage::hummock::iterator::{
    ConcatIterator, Forward, HummockIterator, MergeIterator,
};
use risingwave_storage::hummock::multi_builder::{
    CapacitySplitTableBuilder, LocalTableBuilderFactory,
};
use risingwave_storage::hummock::none::NoneRecentFilter;
use risingwave_storage::hummock::sstable::SstableIteratorReadOptions;
use risingwave_storage::hummock::sstable_store::SstableStoreRef;
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{
    CachePolicy, SstableBuilder, SstableBuilderOptions, SstableIterator, SstableStore,
    SstableStoreConfig, SstableWriterOptions, Xor16FilterBuilder,
};
use risingwave_storage::monitor::{
    CompactorMetrics, StoreLocalStatistic, global_hummock_state_store_metrics,
};

pub async fn mock_sstable_store() -> SstableStoreRef {
    let store = InMemObjectStore::for_test().monitored(
        Arc::new(ObjectStoreMetrics::unused()),
        Arc::new(ObjectStoreConfig::default()),
    );
    let store = Arc::new(ObjectStoreImpl::InMem(store));
    let path = "test".to_owned();
    let meta_cache = HybridCacheBuilder::new()
        .memory(64 << 20)
        .with_shards(2)
        .storage()
        .build()
        .await
        .unwrap();
    let block_cache = HybridCacheBuilder::new()
        .memory(128 << 20)
        .with_shards(2)
        .storage()
        .build()
        .await
        .unwrap();
    Arc::new(SstableStore::new(SstableStoreConfig {
        store,
        path,

        prefetch_buffer_capacity: 64 << 20,
        max_prefetch_block_number: 16,
        recent_filter: Arc::new(NoneRecentFilter::default().into()),
        state_store_metrics: Arc::new(global_hummock_state_store_metrics(MetricLevel::Disabled)),
        use_new_object_prefix_strategy: true,
        skip_bloom_filter_in_serde: false,

        meta_cache,
        block_cache,
        vector_meta_cache: CacheBuilder::new(1 << 10).build(),
        vector_block_cache: CacheBuilder::new(1 << 10).build(),
    }))
}

pub fn default_writer_opts() -> SstableWriterOptions {
    SstableWriterOptions {
        capacity_hint: None,
        tracker: None,
        policy: CachePolicy::Fill(Hint::Normal),
    }
}

pub fn test_key_of(idx: usize, epoch: u64, table_id: TableId) -> FullKey<Vec<u8>> {
    FullKey::for_test(
        table_id,
        [
            VirtualNode::ZERO.to_be_bytes().as_slice(),
            format!("key_test_{:08}", idx * 2).as_bytes(),
        ]
        .concat(),
        epoch,
    )
}

/// 8M keys.
const MAX_KEY_COUNT: usize = 8 * 1024 * 1024;

async fn build_table(
    sstable_store: SstableStoreRef,
    sstable_object_id: u64,
    range: Range<u64>,
    epoch: u64,
) -> SstableInfo {
    let opt = SstableBuilderOptions {
        capacity: 32 * 1024 * 1024,
        block_capacity: 16 * 1024,
        restart_interval: 16,
        bloom_false_positive: 0.001,
        ..Default::default()
    };
    let writer = sstable_store.create_sst_writer(
        sstable_object_id,
        SstableWriterOptions {
            capacity_hint: None,
            tracker: None,
            policy: CachePolicy::Fill(Hint::Normal),
        },
    );
    let table_id_to_vnode = HashMap::from_iter(vec![(0, VirtualNode::COUNT_FOR_TEST)]);
    let table_id_to_watermark_serde = HashMap::from_iter(vec![(0, None)]);
    let mut builder = SstableBuilder::<_, Xor16FilterBuilder>::for_test(
        sstable_object_id,
        writer,
        opt,
        table_id_to_vnode,
        table_id_to_watermark_serde,
    );
    let value = b"1234567890123456789";
    let mut full_key = test_key_of(0, epoch, TableId::new(0));
    let table_key_len = full_key.user_key.table_key.len();
    for i in range {
        let start = (i % 8) as usize;
        let end = start + 8;
        full_key.user_key.table_key[table_key_len - 8..].copy_from_slice(&i.to_be_bytes());
        builder
            .add_for_test(full_key.to_ref(), HummockValue::put(&value[start..end]))
            .await
            .unwrap();
    }
    let output = builder.finish().await.unwrap();
    let handle = output.writer_output;
    let sst = output.sst_info.sst_info;
    handle.await.unwrap().unwrap();
    sst
}

async fn build_table_2(
    sstable_store: SstableStoreRef,
    sstable_object_id: u64,
    range: Range<u64>,
    epoch: u64,
    table_id: u32,
    column_num: usize,
) -> SstableInfo {
    let opt = SstableBuilderOptions {
        capacity: 32 * 1024 * 1024,
        block_capacity: 16 * 1024,
        restart_interval: 16,
        bloom_false_positive: 0.001,
        ..Default::default()
    };
    let writer = sstable_store.create_sst_writer(
        sstable_object_id,
        SstableWriterOptions {
            capacity_hint: None,
            tracker: None,
            policy: CachePolicy::Fill(Hint::Normal),
        },
    );

    let table_id_to_vnode = HashMap::from_iter(vec![(table_id, VirtualNode::COUNT_FOR_TEST)]);
    let table_id_to_watermark_serde = HashMap::from_iter(vec![(0, None)]);

    let mut builder = SstableBuilder::<_, Xor16FilterBuilder>::for_test(
        sstable_object_id,
        writer,
        opt,
        table_id_to_vnode,
        table_id_to_watermark_serde,
    );
    let mut full_key = test_key_of(0, epoch, TableId::new(table_id));
    let table_key_len = full_key.user_key.table_key.len();

    let schema = vec![DataType::Int64; column_num];
    let column_ids = (0..column_num as i32).map(ColumnId::new);
    use risingwave_common::types::ScalarImpl;
    let row = OwnedRow::new(vec![Some(ScalarImpl::Int64(5)); column_num]);
    let table_columns: Vec<_> = column_ids
        .clone()
        .map(|id| ColumnDesc::unnamed(id, schema.get(id.get_id() as usize).unwrap().clone()))
        .collect();
    use risingwave_storage::row_serde::value_serde::ValueRowSerdeNew;
    let serializer = ColumnAwareSerde::new(
        Arc::from_iter(column_ids.map(|id| id.get_id() as usize)),
        table_columns.into(),
    );
    let row_bytes = serializer.serialize(row);

    for i in range {
        full_key.user_key.table_key[table_key_len - 8..].copy_from_slice(&i.to_be_bytes());
        builder
            .add_for_test(full_key.to_ref(), HummockValue::put(&row_bytes))
            .await
            .unwrap();
    }
    let output = builder.finish().await.unwrap();
    let handle = output.writer_output;
    let sst = output.sst_info.sst_info;
    handle.await.unwrap().unwrap();
    sst
}

async fn scan_all_table(info: &SstableInfo, sstable_store: SstableStoreRef) {
    let mut stats = StoreLocalStatistic::default();
    let table = sstable_store.sstable(info, &mut stats).await.unwrap();
    let default_read_options = Arc::new(SstableIteratorReadOptions::default());
    // warm up to make them all in memory. I do not use CachePolicy::Fill because it will fetch
    // block from meta.
    let mut iter = SstableIterator::new(table, sstable_store.clone(), default_read_options, info);
    iter.rewind().await.unwrap();
    while iter.is_valid() {
        iter.next().await.unwrap();
    }
}

fn bench_table_build(c: &mut Criterion) {
    c.bench_function("bench_table_build", |b| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let sstable_store = runtime.block_on(mock_sstable_store());
        b.to_async(&runtime).iter(|| async {
            build_table(sstable_store.clone(), 0, 0..(MAX_KEY_COUNT as u64), 1).await;
        });
    });
}

fn bench_table_scan(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let sstable_store = runtime.block_on(mock_sstable_store());
    let info = runtime.block_on(async {
        build_table(sstable_store.clone(), 0, 0..(MAX_KEY_COUNT as u64), 1).await
    });
    // warm up to make them all in memory. I do not use CachePolicy::Fill because it will fetch
    // block from meta.
    let sstable_store1 = sstable_store.clone();
    let info1 = info.clone();
    runtime.block_on(async move {
        scan_all_table(&info1, sstable_store1).await;
    });

    c.bench_function("bench_table_iterator", |b| {
        let info1 = info.clone();
        b.to_async(&runtime)
            .iter(|| scan_all_table(&info1, sstable_store.clone()));
    });
}

async fn compact<I: HummockIterator<Direction = Forward>>(
    iter: I,
    sstable_store: SstableStoreRef,
    task_config: Option<TaskConfig>,
) {
    let opt = SstableBuilderOptions {
        capacity: 32 * 1024 * 1024,
        block_capacity: 64 * 1024,
        restart_interval: 16,
        bloom_false_positive: 0.001,
        ..Default::default()
    };
    let compaction_catalog_agent_ref = CompactionCatalogAgent::for_test(vec![0]);
    let mut builder = CapacitySplitTableBuilder::for_test(
        LocalTableBuilderFactory::new(32, sstable_store, opt),
        compaction_catalog_agent_ref,
    );

    let task_config = task_config.unwrap_or_else(|| {
        TaskConfig::for_test(
            KeyRange::inf(),
            CachePolicy::Disable,
            false,
            PbSstableFilterLayout::Blocked,
            HashMap::new(),
        )
    });
    compact_and_build_sst(
        &mut builder,
        &task_config,
        Arc::new(CompactorMetrics::unused()),
        iter,
        DummyCompactionFilter,
    )
    .await
    .unwrap();
}

fn bench_merge_iterator_compactor(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let sstable_store = runtime.block_on(mock_sstable_store());
    let test_key_size = 256 * 1024;
    let info1 = runtime
        .block_on(async { build_table(sstable_store.clone(), 1, 0..test_key_size / 2, 1).await });
    let info2 = runtime.block_on(async {
        build_table(
            sstable_store.clone(),
            2,
            test_key_size / 2..test_key_size,
            1,
        )
        .await
    });
    let level1 = vec![info1, info2];

    let info1 = runtime
        .block_on(async { build_table(sstable_store.clone(), 3, 0..(test_key_size / 2), 2).await });
    let info2 = runtime.block_on(async {
        build_table(
            sstable_store.clone(),
            4,
            (test_key_size / 2)..test_key_size,
            2,
        )
        .await
    });
    let level2 = vec![info1, info2];
    let read_options = Arc::new(SstableIteratorReadOptions {
        cache_policy: CachePolicy::Fill(Hint::Normal),
        scan_end_user_key: None,
        prefetch: false,
        max_preload_retry_times: 0,
    });
    c.bench_function("bench_union_merge_iterator", |b| {
        b.to_async(FuturesExecutor).iter(|| {
            let sstable_store1 = sstable_store.clone();
            let sub_iters = vec![
                ConcatIterator::new(level1.clone(), sstable_store.clone(), read_options.clone()),
                ConcatIterator::new(level2.clone(), sstable_store.clone(), read_options.clone()),
            ];
            let iter = MergeIterator::for_compactor(sub_iters);
            async move { compact(iter, sstable_store1, None).await }
        });
    });
    c.bench_function("bench_merge_iterator", |b| {
        b.to_async(&runtime).iter(|| {
            let sub_iters = vec![
                ConcatSstableIterator::new(
                    level1.clone(),
                    KeyRange::inf(),
                    sstable_store.clone(),
                    Arc::new(TaskProgress::default()),
                    0,
                ),
                ConcatSstableIterator::new(
                    level2.clone(),
                    KeyRange::inf(),
                    sstable_store.clone(),
                    Arc::new(TaskProgress::default()),
                    0,
                ),
            ];
            let iter = MergeIterator::for_compactor(sub_iters);
            let sstable_store1 = sstable_store.clone();
            async move { compact(iter, sstable_store1, None).await }
        });
    });
}

fn bench_drop_column_compaction_impl(c: &mut Criterion, column_num: usize) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let sstable_store = runtime.block_on(mock_sstable_store());
    let test_key_size = 256 * 1024;
    let info1 = runtime.block_on(async {
        build_table_2(
            sstable_store.clone(),
            1,
            0..test_key_size,
            1,
            10,
            column_num,
        )
        .await
    });
    let info2 = runtime.block_on(async {
        build_table_2(
            sstable_store.clone(),
            2,
            0..test_key_size,
            1,
            11,
            column_num,
        )
        .await
    });
    let level1 = vec![info1, info2];

    let info1 = runtime.block_on(async {
        build_table_2(
            sstable_store.clone(),
            3,
            0..test_key_size,
            2,
            10,
            column_num,
        )
        .await
    });
    let info2 = runtime.block_on(async {
        build_table_2(
            sstable_store.clone(),
            4,
            0..test_key_size,
            2,
            11,
            column_num,
        )
        .await
    });
    let level2 = vec![info1, info2];

    let task_config_no_schema = TaskConfig::for_test(
        KeyRange::inf(),
        CachePolicy::Disable,
        false,
        PbSstableFilterLayout::Blocked,
        HashMap::new(),
    );

    let task_config_schema = TaskConfig::for_test(
        KeyRange::inf(),
        CachePolicy::Disable,
        false,
        PbSstableFilterLayout::Blocked,
        HashMap::from([
            (
                10.into(),
                PbTableSchema {
                    column_ids: (0..column_num as i32).collect(),
                },
            ),
            (
                11.into(),
                PbTableSchema {
                    column_ids: (0..column_num as i32).collect(),
                },
            ),
        ]),
    );

    let task_config_schema_cause_drop = TaskConfig::for_test(
        KeyRange::inf(),
        CachePolicy::Disable,
        false,
        PbSstableFilterLayout::Blocked,
        HashMap::from([
            (
                10.into(),
                PbTableSchema {
                    column_ids: (0..column_num as i32 / 2).collect(),
                },
            ),
            (
                11.into(),
                PbTableSchema {
                    column_ids: (0..column_num as i32 / 2).collect(),
                },
            ),
        ]),
    );

    let get_iter = || {
        let sub_iters = vec![
            ConcatSstableIterator::new(
                level1.clone(),
                KeyRange::inf(),
                sstable_store.clone(),
                Arc::new(TaskProgress::default()),
                0,
            ),
            ConcatSstableIterator::new(
                level2.clone(),
                KeyRange::inf(),
                sstable_store.clone(),
                Arc::new(TaskProgress::default()),
                0,
            ),
        ];
        MergeIterator::for_compactor(sub_iters)
    };

    c.bench_function(
        &format!("bench_drop_column_compaction_baseline_c{column_num}"),
        |b| {
            b.to_async(&runtime).iter(|| {
                let iter = get_iter();
                let sstable_store1 = sstable_store.clone();
                let task_config_clone = task_config_no_schema.clone();
                async move { compact(iter, sstable_store1, Some(task_config_clone)).await }
            });
        },
    );

    c.bench_function(
        &format!("bench_drop_column_compaction_without_drop_c{column_num}"),
        |b| {
            b.to_async(&runtime).iter(|| {
                let iter = get_iter();
                let sstable_store1 = sstable_store.clone();
                let task_config_clone = task_config_schema.clone();
                async move { compact(iter, sstable_store1, Some(task_config_clone)).await }
            });
        },
    );

    c.bench_function(
        &format!("bench_drop_column_compaction_without_drop_disable_optimization_c{column_num}"),
        |b| {
            b.to_async(&runtime).iter(|| {
                let iter = get_iter();
                let sstable_store1 = sstable_store.clone();
                let task_config_clone = task_config_schema
                    .clone()
                    .with_disable_drop_column_optimization(true);
                async move { compact(iter, sstable_store1, Some(task_config_clone)).await }
            });
        },
    );

    c.bench_function(
        &format!("bench_drop_column_compaction_with_drop_c{column_num}"),
        |b| {
            b.to_async(&runtime).iter(|| {
                let iter = get_iter();
                let sstable_store1 = sstable_store.clone();
                let task_config_clone = task_config_schema_cause_drop.clone();
                async move { compact(iter, sstable_store1, Some(task_config_clone)).await }
            });
        },
    );
}

fn bench_drop_column_compaction_small(c: &mut Criterion) {
    bench_drop_column_compaction_impl(c, 10);
}

fn bench_drop_column_compaction_large(c: &mut Criterion) {
    bench_drop_column_compaction_impl(c, 100);
}

criterion_group!(
    benches,
    bench_table_build,
    bench_table_scan,
    bench_merge_iterator_compactor,
    bench_drop_column_compaction_small,
    bench_drop_column_compaction_large
);
criterion_main!(benches);
