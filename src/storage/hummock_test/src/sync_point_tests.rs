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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use foyer::Hint;
use risingwave_common::catalog::hummock::CompactionFilterFlag;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::key::{next_key, user_key};
use risingwave_hummock_sdk::table_stats::to_prost_table_stats_map;
use risingwave_hummock_sdk::version::MAX_HUMMOCK_VERSION_ID;
use risingwave_meta::hummock::compaction::compaction_config::CompactionConfigBuilder;
use risingwave_meta::hummock::compaction::selector::ManualCompactionOption;
use risingwave_meta::hummock::test_utils::{setup_compute_env, setup_compute_env_with_config};
use risingwave_meta::hummock::{HummockManagerRef, MockHummockMetaClient};
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::StateStore;
use risingwave_storage::compaction_catalog_manager::CompactionCatalogAgentRef;
use risingwave_storage::hummock::compactor::CompactorContext;
use risingwave_storage::hummock::compactor::compactor_runner::compact_with_agent;
use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
use risingwave_storage::hummock::test_utils::{ReadOptions, *};
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{
    CachePolicy, GetObjectId, ObjectIdManager, get_from_sstable_info,
};
use risingwave_storage::monitor::StoreLocalStatistic;
use risingwave_storage::store::*;
use serial_test::serial;

use super::compactor_tests::tests::get_hummock_storage;
use crate::compactor_tests::tests::{flush_and_commit, get_compactor_context};
use crate::get_notification_client_for_test;
use crate::local_state_store_test_utils::LocalStateStoreTestExt;
use crate::test_utils::gen_key_from_bytes;

#[cfg(feature = "sync_point")]
struct SyncPointActionGuard(sync_point::SyncPoint);

#[cfg(feature = "sync_point")]
impl Drop for SyncPointActionGuard {
    fn drop(&mut self) {
        sync_point::remove_action(self.0);
    }
}

#[tokio::test]
async fn test_get_from_sstable_info_success_settles_nested_stats_once() {
    let sstable_store = mock_sstable_store().await;
    let (_sstable, sstable_info) = gen_test_sstable(
        default_builder_opt_for_test(),
        0,
        (0..10).map(|idx| (test_key_of(idx), HummockValue::put(test_value_of(idx)))),
        sstable_store.clone(),
    )
    .await;
    let mut parent_stats = StoreLocalStatistic {
        cache_data_block_total: 7,
        ..Default::default()
    };
    let full_key = test_key_of(0);
    let read_options = risingwave_storage::store::ReadOptions::default();

    let iter = get_from_sstable_info(
        sstable_store,
        &sstable_info,
        full_key.to_ref(),
        &read_options,
        None,
        &mut parent_stats,
    )
    .await
    .unwrap()
    .unwrap();
    drop(iter);

    assert_eq!(parent_stats.cache_data_block_total, 8);
}

#[tokio::test]
#[cfg(feature = "sync_point")]
#[serial]
async fn test_get_from_sstable_info_cancellation_settles_nested_stats() {
    const PAUSE: &str = "SSTABLE_ITERATOR::SEEK::BEFORE_NEXT_BLOCK";
    const PAUSED: &str = "SSTABLE_ITERATOR::SEEK::BEFORE_NEXT_BLOCK::PAUSED";

    sync_point::reset();
    sync_point::hook(PAUSE, || async {
        sync_point::on(PAUSED).await;
        futures::future::pending::<()>().await;
    });
    let _pause_cleanup = SyncPointActionGuard(PAUSE);

    let sstable_store = mock_sstable_store().await;
    let (_sstable, sstable_info) = gen_test_sstable(
        default_builder_opt_for_test(),
        0,
        (0..10).map(|idx| (test_key_of(idx), HummockValue::put(test_value_of(idx)))),
        sstable_store.clone(),
    )
    .await;
    let mut parent_stats = StoreLocalStatistic::default();
    let full_key = test_key_of(10);
    let read_options = risingwave_storage::store::ReadOptions::default();
    let mut get = Box::pin(get_from_sstable_info(
        sstable_store,
        &sstable_info,
        full_key.to_ref(),
        &read_options,
        None,
        &mut parent_stats,
    ));

    tokio::select! {
        result = sync_point::wait_timeout(PAUSED, Duration::from_secs(10)) => {
            result.expect("GET did not reach the cancellation hook");
        }
        _ = get.as_mut() => panic!("GET completed before cancellation"),
    }
    drop(get);

    assert_eq!(parent_stats.cache_data_block_total, 1);
}

#[tokio::test]
#[cfg(feature = "sync_point")]
#[serial]
async fn test_syncpoints_object_id_manager() {
    let (_env, hummock_manager_ref, _cluster_manager_ref, worker_id) =
        setup_compute_env(8080).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_id as _,
    ));
    let object_id_manager = Arc::new(ObjectIdManager::new(hummock_meta_client.clone(), 5));

    // Block filling cache after fetching ids.
    sync_point::hook("MAP_NEXT_SST_OBJECT_ID.BEFORE_FILL_CACHE", || async {
        sync_point::wait_timeout(
            "MAP_NEXT_SST_OBJECT_ID.SIG_FILL_CACHE",
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    });

    // Start the task that fetches new ids.
    let object_id_manager_clone = object_id_manager.clone();
    let leader_task = tokio::spawn(async move {
        object_id_manager_clone
            .get_new_sst_object_id()
            .await
            .unwrap();
    });
    sync_point::wait_timeout(
        "MAP_NEXT_SST_OBJECT_ID.AFTER_FETCH",
        Duration::from_secs(10),
    )
    .await
    .unwrap();

    // Start tasks that waits to be notified.
    let mut follower_tasks = vec![];
    for _ in 0..3 {
        let object_id_manager_clone = object_id_manager.clone();
        let follower_task = tokio::spawn(async move {
            object_id_manager_clone
                .get_new_sst_object_id()
                .await
                .unwrap();
        });
        sync_point::wait_timeout(
            "MAP_NEXT_SST_OBJECT_ID.AS_FOLLOWER",
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        follower_tasks.push(follower_task);
    }

    // Continue to fill cache.
    sync_point::on("MAP_NEXT_SST_OBJECT_ID.SIG_FILL_CACHE").await;

    leader_task.await.unwrap();
    for follower_task in follower_tasks {
        follower_task.await.unwrap();
    }
}

#[cfg(all(feature = "sync_point", feature = "failpoints"))]
#[tokio::test]
#[serial]
async fn test_syncpoints_test_failpoints_fetch_ids() {
    let (_env, hummock_manager_ref, _cluster_manager_ref, worker_id) =
        setup_compute_env(8080).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_id as _,
    ));
    let object_id_manager = Arc::new(ObjectIdManager::new(hummock_meta_client.clone(), 5));

    // Block fetching ids.
    sync_point::hook("MAP_NEXT_SST_OBJECT_ID.BEFORE_FETCH", || async {
        sync_point::wait_timeout("MAP_NEXT_SST_OBJECT_ID.SIG_FETCH", Duration::from_secs(10))
            .await
            .unwrap();
        sync_point::remove_action("MAP_NEXT_SST_OBJECT_ID.BEFORE_FETCH");
    });

    // Start the task that fetches new ids.
    let object_id_manager_clone = object_id_manager.clone();
    let leader_task = tokio::spawn(async move {
        fail::cfg("get_new_sst_ids_err", "return").unwrap();
        object_id_manager_clone
            .get_new_sst_object_id()
            .await
            .unwrap_err();
        fail::remove("get_new_sst_ids_err");
    });
    sync_point::wait_timeout("MAP_NEXT_SST_OBJECT_ID.AS_LEADER", Duration::from_secs(10))
        .await
        .unwrap();

    // Start tasks that waits to be notified.
    let mut follower_tasks = vec![];
    for _ in 0..3 {
        let object_id_manager_clone = object_id_manager.clone();
        let follower_task = tokio::spawn(async move {
            object_id_manager_clone
                .get_new_sst_object_id()
                .await
                .unwrap();
        });
        sync_point::wait_timeout(
            "MAP_NEXT_SST_OBJECT_ID.AS_FOLLOWER",
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        follower_tasks.push(follower_task);
    }

    // Continue to fetch ids.
    sync_point::on("MAP_NEXT_SST_OBJECT_ID.SIG_FETCH").await;

    leader_task.await.unwrap();
    // Failed leader task doesn't block follower tasks.
    for follower_task in follower_tasks {
        follower_task.await.unwrap();
    }
}

pub async fn compact_once(
    hummock_manager_ref: HummockManagerRef,
    compact_ctx: CompactorContext,
    compaction_catalog_agent_ref: CompactionCatalogAgentRef,
    object_id_manager: Arc<ObjectIdManager>,
) {
    // 2. get compact task
    let manual_compcation_option = ManualCompactionOption {
        level: 0,
        ..Default::default()
    };
    // 2. get compact task
    let mut compact_task = hummock_manager_ref
        .manual_get_compact_task(
            StaticCompactionGroupId::StateDefault,
            manual_compcation_option,
        )
        .await
        .unwrap()
        .unwrap();
    compact_task.gc_delete_keys = false;

    let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN;
    compact_task.compaction_filter_mask = compaction_filter_flag.bits();
    // 3. compact
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let ((result_task, task_stats, object_timestamps), _) = compact_with_agent(
        compact_ctx,
        compact_task.clone(),
        rx,
        object_id_manager,
        compaction_catalog_agent_ref.clone(),
    )
    .await;

    hummock_manager_ref
        .report_compact_task(
            result_task.task_id,
            result_task.task_status,
            result_task.sorted_output_ssts,
            Some(to_prost_table_stats_map(task_stats)),
            object_timestamps,
        )
        .await
        .unwrap();
}

#[ignore]
#[tokio::test]
#[cfg(feature = "sync_point")]
#[serial]
async fn test_syncpoints_get_in_delete_range_boundary() {
    let config = CompactionConfigBuilder::new()
        .level0_tier_compact_file_number(1)
        .max_bytes_for_level_base(4096)
        .build();
    let (env, hummock_manager_ref, cluster_ctl_ref, worker_id) =
        setup_compute_env_with_config(8080, config).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_id as _,
    ));
    let existing_table_id = 1.into();

    let storage = get_hummock_storage(
        hummock_meta_client.clone(),
        get_notification_client_for_test(
            env,
            hummock_manager_ref.clone(),
            cluster_ctl_ref,
            worker_id,
        )
        .await,
        &hummock_manager_ref,
        &[existing_table_id],
    )
    .await;

    let compact_ctx = get_compactor_context(&storage);
    let compaction_catalog_agent_ref = storage
        .compaction_catalog_manager_ref()
        .acquire(vec![existing_table_id])
        .await
        .unwrap();

    let object_id_manager = Arc::new(ObjectIdManager::new(
        hummock_meta_client.clone(),
        storage
            .storage_opts()
            .clone()
            .sstable_id_remote_fetch_number,
    ));

    let mut local = storage
        .new_local(NewLocalOptions::for_test(existing_table_id))
        .await;

    // 1. add sstables
    let val0 = Bytes::from(b"0"[..].repeat(1 << 10)); // 1024 Byte value
    let val1 = Bytes::from(b"1"[..].repeat(1 << 10)); // 1024 Byte value

    local.init_for_test(test_epoch(100)).await.unwrap();
    let mut start_key = b"aaa".to_vec();
    for _ in 0..10 {
        local
            .insert(
                gen_key_from_bytes(VirtualNode::ZERO, start_key.as_slice()),
                val0.clone(),
                None,
            )
            .unwrap();
        start_key = next_key(start_key.as_slice());
    }
    local
        .insert(
            gen_key_from_bytes(VirtualNode::ZERO, b"ggg"),
            val0.clone(),
            None,
        )
        .unwrap();
    local
        .insert(
            gen_key_from_bytes(VirtualNode::ZERO, b"hhh"),
            val0.clone(),
            None,
        )
        .unwrap();
    local
        .insert(
            gen_key_from_bytes(VirtualNode::ZERO, b"kkk"),
            val0.clone(),
            None,
        )
        .unwrap();
    local.flush().await.unwrap();
    local.seal_current_epoch(
        test_epoch(101),
        risingwave_storage::store::SealCurrentEpochOptions::for_test(),
    );
    flush_and_commit(
        &hummock_meta_client,
        &storage,
        test_epoch(100),
        local.table_id(),
    )
    .await;

    compact_once(
        hummock_manager_ref.clone(),
        compact_ctx.clone(),
        compaction_catalog_agent_ref.clone(),
        object_id_manager.clone(),
    )
    .await;

    local
        .insert(
            gen_key_from_bytes(VirtualNode::ZERO, b"aaa"),
            val1.clone(),
            None,
        )
        .unwrap();
    local
        .insert(
            gen_key_from_bytes(VirtualNode::ZERO, b"bbb"),
            val1.clone(),
            None,
        )
        .unwrap();
    // local
    //     .flush(vec![(
    //         Bound::Included(Bytes::from(b"\0\0ggg".as_slice())),
    //         Bound::Excluded(Bytes::from(b"\0\0hhh".as_slice())),
    //     )])
    //     .await
    //     .unwrap();
    local.flush().await.unwrap();
    local.seal_current_epoch(
        test_epoch(102),
        risingwave_storage::store::SealCurrentEpochOptions::for_test(),
    );
    flush_and_commit(
        &hummock_meta_client,
        &storage,
        test_epoch(101),
        local.table_id(),
    )
    .await;
    compact_once(
        hummock_manager_ref.clone(),
        compact_ctx.clone(),
        compaction_catalog_agent_ref.clone(),
        object_id_manager.clone(),
    )
    .await;

    local
        .insert(
            gen_key_from_bytes(VirtualNode::ZERO, b"hhh"),
            val1.clone(),
            None,
        )
        .unwrap();
    local
        .insert(
            gen_key_from_bytes(VirtualNode::ZERO, b"iii"),
            val1.clone(),
            None,
        )
        .unwrap();
    // local
    //     .flush(vec![(
    //         Bound::Included(Bytes::from(b"\0\0jjj".as_slice())),
    //         Bound::Excluded(Bytes::from(b"\0\0kkk".as_slice())),
    //     )])
    //     .await
    //     .unwrap();
    local.flush().await.unwrap();
    local.seal_current_epoch(
        test_epoch(103),
        risingwave_storage::store::SealCurrentEpochOptions::for_test(),
    );
    flush_and_commit(
        &hummock_meta_client,
        &storage,
        test_epoch(102),
        local.table_id(),
    )
    .await;
    // move this two file to the same level.
    compact_once(
        hummock_manager_ref.clone(),
        compact_ctx.clone(),
        compaction_catalog_agent_ref.clone(),
        object_id_manager.clone(),
    )
    .await;

    local
        .insert(
            gen_key_from_bytes(VirtualNode::ZERO, b"lll"),
            val1.clone(),
            None,
        )
        .unwrap();
    local
        .insert(
            gen_key_from_bytes(VirtualNode::ZERO, b"mmm"),
            val1.clone(),
            None,
        )
        .unwrap();
    local.flush().await.unwrap();
    local.seal_current_epoch(
        u64::MAX,
        risingwave_storage::store::SealCurrentEpochOptions::for_test(),
    );
    flush_and_commit(
        &hummock_meta_client,
        &storage,
        test_epoch(103),
        local.table_id(),
    )
    .await;
    // move this two file to the same level.
    compact_once(
        hummock_manager_ref.clone(),
        compact_ctx.clone(),
        compaction_catalog_agent_ref.clone(),
        object_id_manager.clone(),
    )
    .await;

    // 4. get the latest version and check
    let version = hummock_manager_ref.get_current_version().await;
    let base_level = &version
        .get_compaction_group_levels(StaticCompactionGroupId::StateDefault)
        .levels[4];
    assert_eq!(base_level.table_infos.len(), 3);
    assert!(base_level.table_infos[0].key_range.right_exclusive);
    assert_eq!(
        user_key(&base_level.table_infos[0].key_range.right),
        user_key(&base_level.table_infos[1].key_range.left),
    );
    storage.wait_version(version).await;
    let read_options = ReadOptions {
        table_id: existing_table_id,
        cache_policy: CachePolicy::Fill(Hint::Normal),
        ..Default::default()
    };
    let get_result = storage
        .get(
            gen_key_from_bytes(VirtualNode::ZERO, b"hhh"),
            test_epoch(120),
            read_options.clone(),
        )
        .await
        .unwrap();
    assert_eq!(get_result.unwrap(), val1);
    let get_result = storage
        .get(
            gen_key_from_bytes(VirtualNode::ZERO, b"ggg"),
            test_epoch(120),
            read_options.clone(),
        )
        .await
        .unwrap();
    assert!(get_result.is_none());
    let get_result = storage
        .get(
            gen_key_from_bytes(VirtualNode::ZERO, b"aaa"),
            test_epoch(120),
            read_options.clone(),
        )
        .await
        .unwrap();
    assert_eq!(get_result.unwrap(), val1);
    let get_result = storage
        .get(
            gen_key_from_bytes(VirtualNode::ZERO, b"aab"),
            test_epoch(120),
            read_options.clone(),
        )
        .await
        .unwrap();
    assert_eq!(get_result.unwrap(), val0);
    let skip_flag = Arc::new(AtomicBool::new(false));
    let skip_flag_hook = skip_flag.clone();
    sync_point::hook("HUMMOCK_V2::GET::SKIP_BY_NO_FILE", move || {
        let flag = skip_flag_hook.clone();
        async move {
            flag.store(true, Ordering::Release);
        }
    });
    let get_result = storage
        .get(
            gen_key_from_bytes(VirtualNode::ZERO, b"kkk"),
            test_epoch(120),
            read_options.clone(),
        )
        .await
        .unwrap();
    assert_eq!(get_result.unwrap(), val0);
    assert!(skip_flag.load(Ordering::Acquire));
}

#[tokio::test]
#[cfg(feature = "sync_point")]
#[serial]
async fn test_syncpoints_hummock_version_safe_point() {
    let (_env, hummock_manager, _, _) = setup_compute_env(80).await;
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        MAX_HUMMOCK_VERSION_ID
    );
    let v = hummock_manager.get_current_version().await;
    let sp = hummock_manager.register_safe_point().await;
    assert_eq!(v.id, sp.id);
    assert_eq!(hummock_manager.get_min_pinned_version_id().await, v.id);
    hummock_manager.unregister_safe_point(sp.id).await;
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        MAX_HUMMOCK_VERSION_ID
    );

    let sp = hummock_manager.register_safe_point().await;
    assert_eq!(hummock_manager.get_min_pinned_version_id().await, v.id);
    drop(sp);
    sync_point::wait_timeout(
        "UNREGISTER_HUMMOCK_VERSION_SAFE_POINT",
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        MAX_HUMMOCK_VERSION_ID
    );
}
