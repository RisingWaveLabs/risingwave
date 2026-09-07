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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, LazyLock};

use await_tree::InstrumentAwait;
use bytes::Bytes;
use foyer::Hint;
use futures::future::try_join;
use futures::{FutureExt, StreamExt, stream};
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{EPOCH_LEN, FullKey};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::{EpochWithGap, KeyComparator, LocalSstableInfo};
use risingwave_pb::hummock::{PbSstableFilterLayout, PbSstableFilterType};
use thiserror_ext::AsReport;
use tracing::error;

use crate::compaction_catalog_manager::{CompactionCatalogAgentRef, CompactionCatalogManagerRef};
use crate::hummock::compactor::compaction_filter::DummyCompactionFilter;
use crate::hummock::compactor::compaction_utils::{
    blocked_xor_filter_key_count_threshold, estimate_output_key_count_by_size,
};
use crate::hummock::compactor::context::{CompactorContext, await_tree_key};
use crate::hummock::compactor::{CompactOutput, Compactor, check_flush_result};
use crate::hummock::event_handler::uploader::UploadTaskOutput;
use crate::hummock::iterator::{Forward, HummockIterator, MergeIterator, UserIterator};
use crate::hummock::{
    CachePolicy, GetObjectId, HummockError, HummockResult, ObjectIdManagerRef,
    SstableBuilderOptions,
};
use crate::mem_table::ImmutableMemtable;
use crate::opts::StorageOpts;

const GC_DELETE_KEYS_FOR_FLUSH: bool = false;

/// Flush shared buffer to level0. Resulted SSTs are grouped by compaction group.
pub async fn compact(
    context: CompactorContext,
    object_id_manager: ObjectIdManagerRef,
    payload: Vec<ImmutableMemtable>,
    compaction_catalog_manager_ref: CompactionCatalogManagerRef,
) -> HummockResult<UploadTaskOutput> {
    let table_ids_with_old_value: HashSet<TableId> = payload
        .iter()
        .filter(|imm| imm.has_old_value())
        .map(|imm| imm.table_id)
        .collect();
    let mut non_log_store_new_value_payload = Vec::with_capacity(payload.len());
    let mut log_store_new_value_payload = Vec::with_capacity(payload.len());
    let mut old_value_payload = Vec::with_capacity(payload.len());
    for imm in payload {
        if table_ids_with_old_value.contains(&imm.table_id) {
            if imm.has_old_value() {
                old_value_payload.push(imm.clone());
            }
            log_store_new_value_payload.push(imm);
        } else {
            assert!(!imm.has_old_value());
            non_log_store_new_value_payload.push(imm);
        }
    }
    let non_log_store_new_value_future = async {
        if non_log_store_new_value_payload.is_empty() {
            Ok(vec![])
        } else {
            compact_shared_buffer::<true>(
                context.clone(),
                object_id_manager.clone(),
                compaction_catalog_manager_ref.clone(),
                non_log_store_new_value_payload,
            )
            .instrument_await("shared_buffer_compact_non_log_store_new_value")
            .await
        }
    };

    let log_store_new_value_future = async {
        if log_store_new_value_payload.is_empty() {
            Ok(vec![])
        } else {
            compact_shared_buffer::<true>(
                context.clone(),
                object_id_manager.clone(),
                compaction_catalog_manager_ref.clone(),
                log_store_new_value_payload,
            )
            .instrument_await("shared_buffer_compact_log_store_new_value")
            .await
        }
    };

    let old_value_future = async {
        if old_value_payload.is_empty() {
            Ok(vec![])
        } else {
            compact_shared_buffer::<false>(
                context.clone(),
                object_id_manager.clone(),
                compaction_catalog_manager_ref.clone(),
                old_value_payload,
            )
            .instrument_await("shared_buffer_compact_log_store_old_value")
            .await
        }
    };

    // Note that the output is reordered compared with input `payload`.
    let ((non_log_store_new_value_ssts, log_store_new_value_ssts), old_value_ssts) = try_join(
        try_join(non_log_store_new_value_future, log_store_new_value_future),
        old_value_future,
    )
    .await?;

    let mut new_value_ssts = non_log_store_new_value_ssts;
    new_value_ssts.extend(log_store_new_value_ssts);

    Ok(UploadTaskOutput {
        new_value_ssts,
        old_value_ssts,
        wait_poll_timer: None,
    })
}

/// For compaction from shared buffer to level 0, this is the only function gets called.
///
/// The `IS_NEW_VALUE` flag means for the given payload, we are doing compaction using its new value or old value.
/// When `IS_NEW_VALUE` is false, we are compacting with old value, and the payload imms should have `old_values` not `None`
async fn compact_shared_buffer<const IS_NEW_VALUE: bool>(
    context: CompactorContext,
    object_id_manager: ObjectIdManagerRef,
    compaction_catalog_manager_ref: CompactionCatalogManagerRef,
    mut payload: Vec<ImmutableMemtable>,
) -> HummockResult<Vec<LocalSstableInfo>> {
    if !IS_NEW_VALUE {
        assert!(payload.iter().all(|imm| imm.has_old_value()));
    }
    // Local memory compaction looks at all key ranges.
    let existing_table_ids: HashSet<TableId> =
        payload.iter().map(|imm| imm.table_id).dedup().collect();
    assert!(!existing_table_ids.is_empty());

    let compaction_catalog_agent_ref = compaction_catalog_manager_ref
        .acquire(existing_table_ids.iter().copied().collect())
        .await?;
    let existing_table_ids = compaction_catalog_agent_ref
        .table_ids()
        .collect::<HashSet<_>>();
    payload.retain(|imm| {
        let ret = existing_table_ids.contains(&imm.table_id);
        if !ret {
            error!(
                "cannot find table {:?}; it may have been removed by the meta service",
                imm.table_id
            );
        }
        ret
    });

    let total_key_count = payload.iter().map(|imm| imm.key_count()).sum::<usize>();
    let (splits, sub_compaction_sstable_size, table_vnode_partition, compact_data_size) =
        generate_splits(&payload, &existing_table_ids, context.storage_opts.as_ref());
    let parallelism = splits.len();
    let mut compact_success = true;
    let mut output_ssts = Vec::with_capacity(parallelism);
    let mut compaction_futures = vec![];
    // Shared buffer compaction always goes to L0. Use a blocked filter when kv_count is large.
    // Use None to apply the default threshold since shared buffer flush doesn't have a CompactTask.
    let estimated_output_key_count = estimate_output_key_count_by_size(
        total_key_count as u64,
        compact_data_size,
        sub_compaction_sstable_size as usize,
    );
    let sstable_filter_layout =
        if risingwave_hummock_sdk::filter_utils::should_use_blocked_xor_filter_by_kv_count(
            estimated_output_key_count as u64,
            None,
        ) {
            PbSstableFilterLayout::Blocked
        } else {
            PbSstableFilterLayout::Plain
        };

    for (split_index, key_range) in splits.into_iter().enumerate() {
        let compactor = SharedBufferCompactRunner::new(
            split_index,
            key_range,
            context.clone(),
            sub_compaction_sstable_size as usize,
            estimated_output_key_count,
            table_vnode_partition.clone(),
            sstable_filter_layout,
            object_id_manager.clone(),
        );
        let mut forward_iters = Vec::with_capacity(payload.len());
        for imm in &payload {
            forward_iters.push(imm.clone().into_directed_iter::<Forward, IS_NEW_VALUE>());
        }
        let compaction_executor = context.compaction_executor.clone();
        let compaction_catalog_agent_ref = compaction_catalog_agent_ref.clone();
        let handle = compaction_executor.spawn({
            static NEXT_SHARED_BUFFER_COMPACT_ID: LazyLock<AtomicUsize> =
                LazyLock::new(|| AtomicUsize::new(0));
            let tree_root = context.await_tree_reg.as_ref().map(|reg| {
                let id = NEXT_SHARED_BUFFER_COMPACT_ID.fetch_add(1, Relaxed);
                reg.register(
                    await_tree_key::CompactSharedBuffer { id },
                    format!(
                        "Compact Shared Buffer: {:?}",
                        payload
                            .iter()
                            .map(|imm| imm.epoch())
                            .collect::<BTreeSet<_>>()
                    ),
                )
            });
            let future = compactor.run(
                MergeIterator::new(forward_iters),
                compaction_catalog_agent_ref,
            );
            if let Some(root) = tree_root {
                root.instrument(future).left_future()
            } else {
                future.right_future()
            }
        });
        compaction_futures.push(handle);
    }

    let mut buffered = stream::iter(compaction_futures).buffer_unordered(parallelism);
    let mut err = None;
    while let Some(future_result) = buffered.next().await {
        match future_result {
            Ok(Ok((split_index, ssts, table_stats_map))) => {
                output_ssts.push((split_index, ssts, table_stats_map));
            }
            Ok(Err(e)) => {
                compact_success = false;
                tracing::warn!(error = %e.as_report(), "Shared Buffer Compaction failed with error");
                err = Some(e);
            }
            Err(e) => {
                compact_success = false;
                tracing::warn!(
                    error = %e.as_report(),
                    "Shared Buffer Compaction failed with future error",
                );
                err = Some(HummockError::compaction_executor(
                    "failed while execute in tokio",
                ));
            }
        }
    }

    // Sort by split/key range index.
    output_ssts.sort_by_key(|(split_index, ..)| *split_index);

    if compact_success {
        let mut level0 = Vec::with_capacity(parallelism);
        let mut sst_infos = vec![];
        for (_, ssts, _) in output_ssts {
            for sst_info in &ssts {
                context
                    .compactor_metrics
                    .write_build_l0_bytes
                    .inc_by(sst_info.file_size());

                sst_infos.push(sst_info.sst_info.clone());
            }
            level0.extend(ssts);
        }
        if context.storage_opts.check_compaction_result {
            let compaction_executor = context.compaction_executor.clone();
            let mut forward_iters = Vec::with_capacity(payload.len());
            for imm in &payload {
                if !existing_table_ids.contains(&imm.table_id) {
                    continue;
                }
                forward_iters.push(imm.clone().into_forward_iter());
            }
            let iter = MergeIterator::new(forward_iters);
            let left_iter = UserIterator::new(
                iter,
                (Bound::Unbounded, Bound::Unbounded),
                u64::MAX,
                0,
                None,
            );
            compaction_executor.spawn(async move {
                match check_flush_result(left_iter, sst_infos, context).await {
                    Err(e) => {
                        tracing::warn!(
                            error = %e.as_report(),
                            "failed to check the memtable flush result",
                        );
                    }
                    Ok(true) => (),
                    Ok(false) => {
                        panic!(
                            "failed to check flush result consistency of state-table {:?}",
                            existing_table_ids
                        );
                    }
                }
            });
        }
        Ok(level0)
    } else {
        Err(err.unwrap())
    }
}

///  Based on the incoming payload and opts, calculate the sharding method and sstable size of shared buffer compaction.
fn generate_splits(
    payload: &Vec<ImmutableMemtable>,
    existing_table_ids: &HashSet<TableId>,
    storage_opts: &StorageOpts,
) -> (Vec<KeyRange>, u64, BTreeMap<TableId, u32>, u64) {
    let mut size_and_start_user_keys = vec![];
    let mut compact_data_size = 0;
    let mut table_size_infos: HashMap<TableId, u64> = HashMap::default();
    let mut table_vnode_partition = BTreeMap::default();
    for imm in payload {
        let data_size = {
            // calculate encoded bytes of key var length
            (imm.value_count() * EPOCH_LEN + imm.size()) as u64
        };
        compact_data_size += data_size;
        size_and_start_user_keys.push((
            data_size,
            FullKey {
                user_key: imm.start_user_key(),
                epoch_with_gap: EpochWithGap::new_max_epoch(),
            }
            .encode(),
        ));
        let v = table_size_infos.entry(imm.table_id).or_insert(0);
        *v += data_size;
    }

    size_and_start_user_keys
        .sort_by(|a, b| KeyComparator::compare_encoded_full_key(a.1.as_ref(), b.1.as_ref()));
    let mut splits = Vec::with_capacity(size_and_start_user_keys.len());
    splits.push(KeyRange::new(Bytes::new(), Bytes::new()));
    let sstable_size = (storage_opts.sstable_size_mb as u64) << 20;
    let min_sstable_size = (storage_opts.min_sstable_size_mb as u64) << 20;
    let parallel_compact_size = (storage_opts.parallel_compact_size_mb as u64) << 20;
    let parallelism = std::cmp::min(
        storage_opts.share_buffers_sync_parallelism as u64,
        size_and_start_user_keys.len() as u64,
    );
    let sub_compaction_data_size = if compact_data_size > parallel_compact_size && parallelism > 1 {
        compact_data_size / parallelism
    } else {
        compact_data_size
    };

    if parallelism > 1 && compact_data_size > sstable_size {
        let mut last_buffer_size = 0;
        let mut last_key: Vec<u8> = vec![];
        for (data_size, key) in size_and_start_user_keys {
            if last_buffer_size >= sub_compaction_data_size && !last_key.eq(&key) {
                splits.last_mut().unwrap().right = Bytes::from(key.clone());
                splits.push(KeyRange::new(Bytes::from(key.clone()), Bytes::default()));
                last_buffer_size = data_size;
            } else {
                last_buffer_size += data_size;
            }

            last_key = key;
        }
    }

    if compact_data_size > sstable_size {
        // Meta node will calculate size of each state-table in one task in `risingwave_meta::hummock::manager::compaction::calculate_vnode_partition`.
        // To make the calculate result more accurately we shall split the large state-table from other small ones.
        for table_id in existing_table_ids {
            if let Some(table_size) = table_size_infos.get(table_id)
                && *table_size > min_sstable_size
            {
                table_vnode_partition.insert(*table_id, 1);
            }
        }
    }

    // mul 1.2 for other extra memory usage.
    // Ensure that the size of each sstable is still less than `sstable_size` after optimization to avoid generating a huge size sstable which will affect the object store
    let sub_compaction_sstable_size = std::cmp::min(sstable_size, sub_compaction_data_size * 6 / 5);
    (
        splits,
        sub_compaction_sstable_size,
        table_vnode_partition,
        compact_data_size,
    )
}

pub struct SharedBufferCompactRunner {
    compactor: Compactor,
    split_index: usize,
}

impl SharedBufferCompactRunner {
    pub fn new(
        split_index: usize,
        key_range: KeyRange,
        context: CompactorContext,
        sub_compaction_sstable_size: usize,
        estimated_output_key_count: usize,
        table_vnode_partition: BTreeMap<TableId, u32>,
        sstable_filter_layout: PbSstableFilterLayout,
        object_id_getter: Arc<dyn GetObjectId>,
    ) -> Self {
        let mut options: SstableBuilderOptions = context.storage_opts.as_ref().into();
        options.capacity = sub_compaction_sstable_size;
        options.estimated_output_key_count = Some(estimated_output_key_count);
        options.filter_hash_prealloc_key_count_cap = blocked_xor_filter_key_count_threshold(None);
        let compactor = Compactor::new(
            context,
            options,
            super::TaskConfig {
                key_range,
                cache_policy: CachePolicy::Fill(Hint::Normal),
                gc_delete_keys: GC_DELETE_KEYS_FOR_FLUSH,
                retain_multiple_version: true,
                table_vnode_partition,
                sstable_filter_layout,
                // L0 flush writes overlapping SSTs, so keep a conservative filter type here.
                sstable_filter_type: PbSstableFilterType::SstableFilterXor16,
                table_schemas: Default::default(),
                disable_drop_column_optimization: false,
            },
            object_id_getter,
        );
        Self {
            compactor,
            split_index,
        }
    }

    pub async fn run(
        self,
        iter: impl HummockIterator<Direction = Forward>,
        compaction_catalog_agent_ref: CompactionCatalogAgentRef,
    ) -> HummockResult<CompactOutput> {
        let dummy_compaction_filter = DummyCompactionFilter {};
        let (ssts, table_stats_map) = self
            .compactor
            .compact_key_range(
                iter,
                dummy_compaction_filter,
                compaction_catalog_agent_ref,
                None,
                None,
                None,
            )
            .await?;
        Ok((self.split_index, ssts, table_stats_map))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use bytes::Bytes;
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::key::{TableKey, prefix_slice_with_vnode};

    use crate::hummock::compactor::shared_buffer_compact::generate_splits;
    use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferValue;
    use crate::mem_table::ImmutableMemtable;
    use crate::opts::StorageOpts;

    fn generate_key(key: &str) -> TableKey<Bytes> {
        TableKey(prefix_slice_with_vnode(
            VirtualNode::from_index(1),
            key.as_bytes(),
        ))
    }

    #[tokio::test]
    async fn test_generate_splits_in_order() {
        let imm1 = ImmutableMemtable::build_shared_buffer_batch_for_test(
            test_epoch(3),
            0,
            vec![(
                generate_key("dddd"),
                SharedBufferValue::Insert(Bytes::from_static(b"v3")),
            )],
            1024 * 1024,
            TableId::new(1),
        );
        let imm2 = ImmutableMemtable::build_shared_buffer_batch_for_test(
            test_epoch(3),
            0,
            vec![(
                generate_key("abb"),
                SharedBufferValue::Insert(Bytes::from_static(b"v3")),
            )],
            (1024 + 256) * 1024,
            TableId::new(1),
        );

        let imm3 = ImmutableMemtable::build_shared_buffer_batch_for_test(
            test_epoch(2),
            0,
            vec![(
                generate_key("abc"),
                SharedBufferValue::Insert(Bytes::from_static(b"v2")),
            )],
            (1024 + 512) * 1024,
            TableId::new(1),
        );
        let imm4 = ImmutableMemtable::build_shared_buffer_batch_for_test(
            test_epoch(3),
            0,
            vec![(
                generate_key("aaa"),
                SharedBufferValue::Insert(Bytes::from_static(b"v3")),
            )],
            (1024 + 512) * 1024,
            TableId::new(1),
        );

        let imm5 = ImmutableMemtable::build_shared_buffer_batch_for_test(
            test_epoch(3),
            0,
            vec![(
                generate_key("aaa"),
                SharedBufferValue::Insert(Bytes::from_static(b"v3")),
            )],
            (1024 + 256) * 1024,
            TableId::new(2),
        );

        let storage_opts = StorageOpts {
            share_buffers_sync_parallelism: 3,
            parallel_compact_size_mb: 1,
            sstable_size_mb: 1,
            ..Default::default()
        };
        let payload = vec![imm1, imm2, imm3, imm4, imm5];
        let (splits, _sstable_capacity, vnodes, _) = generate_splits(
            &payload,
            &HashSet::from_iter([1.into(), 2.into()]),
            &storage_opts,
        );
        assert_eq!(
            splits.len(),
            storage_opts.share_buffers_sync_parallelism as usize
        );
        assert!(vnodes.is_empty());

        // Basic validation: splits should be continuous and monotonic
        for i in 1..splits.len() {
            assert_eq!(splits[i].left, splits[i - 1].right);
            assert!(splits[i].left > splits[i - 1].left);
            assert!(splits[i].right.is_empty() || splits[i].left < splits[i].right);
        }
    }

    #[tokio::test]
    async fn test_generate_splits_no_duplicate_keys() {
        // Create test data with specific ordering to detect sorting issues
        // Make data sizes large enough to trigger splitting
        let imm1 = ImmutableMemtable::build_shared_buffer_batch_for_test(
            test_epoch(1),
            0,
            vec![(
                generate_key("zzz"), // This should be last after sorting
                SharedBufferValue::Insert(Bytes::from_static(b"v1")),
            )],
            2 * 1024 * 1024, // 2MB to ensure compact_data_size > sstable_size
            TableId::new(1),
        );

        let imm2 = ImmutableMemtable::build_shared_buffer_batch_for_test(
            test_epoch(1),
            0,
            vec![(
                generate_key("aaa"), // This should be first after sorting
                SharedBufferValue::Insert(Bytes::from_static(b"v1")),
            )],
            2 * 1024 * 1024, // 2MB
            TableId::new(1),
        );

        let imm3 = ImmutableMemtable::build_shared_buffer_batch_for_test(
            test_epoch(1),
            0,
            vec![(
                generate_key("mmm"), // This should be middle after sorting
                SharedBufferValue::Insert(Bytes::from_static(b"v1")),
            )],
            2 * 1024 * 1024, // 2MB
            TableId::new(1),
        );

        let storage_opts = StorageOpts {
            share_buffers_sync_parallelism: 3, // Enable parallelism
            parallel_compact_size_mb: 2,       // Small threshold to trigger splitting
            sstable_size_mb: 1,                // Small SSTable size to trigger condition
            ..Default::default()
        };

        // Test with payload in wrong order (zzz, aaa, mmm) instead of sorted order (aaa, mmm, zzz)
        let payload = vec![imm1, imm2, imm3];
        let (splits, _sstable_capacity, _vnodes, _) =
            generate_splits(&payload, &HashSet::from_iter([1.into()]), &storage_opts);

        // Should have multiple splits due to large data size
        assert!(
            splits.len() > 1,
            "Expected multiple splits, got {}",
            splits.len()
        );

        // Verify no key range overlaps between splits
        for i in 0..splits.len() {
            for j in (i + 1)..splits.len() {
                let split_i = &splits[i];
                let split_j = &splits[j];

                // Check that splits don't overlap
                if !split_i.right.is_empty() && !split_j.left.is_empty() {
                    assert!(
                        split_i.right <= split_j.left || split_j.right <= split_i.left,
                        "Split {} and {} overlap: [{:?}, {:?}) vs [{:?}, {:?})",
                        i,
                        j,
                        split_i.left,
                        split_i.right,
                        split_j.left,
                        split_j.right
                    );
                }
            }
        }

        // Additional verification: ensure splits are sorted
        for i in 1..splits.len() {
            if !splits[i - 1].right.is_empty() && !splits[i].left.is_empty() {
                assert!(
                    splits[i - 1].right <= splits[i].left,
                    "Splits are not in sorted order at index {}: {:?} > {:?}",
                    i,
                    splits[i - 1].right,
                    splits[i].left
                );
            }
        }
    }
}
