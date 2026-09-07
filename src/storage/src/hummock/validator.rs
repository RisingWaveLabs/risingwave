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

use std::borrow::BorrowMut;
use std::cmp;
use std::collections::HashMap;
use std::sync::Arc;

use risingwave_hummock_sdk::compact_task::ValidationTask;
use risingwave_hummock_sdk::key::FullKey;

use crate::hummock::iterator::HummockIterator;
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{CachePolicy, SstableIterator};
use crate::monitor::StoreLocalStatistic;

/// Validate SSTs in terms of Ordered, Locally unique and Globally unique.
///
/// See `src/storage/src/hummock/state_store.rs`
pub async fn validate_ssts(task: ValidationTask, sstable_store: SstableStoreRef) {
    let mut visited_keys = HashMap::new();
    let mut unused = StoreLocalStatistic::default();
    for sstable_info in task.sst_infos {
        let mut key_counts = 0;
        let worker_id = *task
            .sst_id_to_worker_id
            .get(&sstable_info.object_id)
            .expect("valid worker_id");
        tracing::debug!(
            "Validating SST sst_id {} object_id {} from worker {}",
            sstable_info.sst_id,
            sstable_info.object_id,
            worker_id,
        );
        let holder = match sstable_store
            .sstable(&sstable_info, unused.borrow_mut())
            .await
        {
            Ok(holder) => holder,
            Err(_err) => {
                // One reasonable cause is the SST has been vacuumed.
                tracing::info!(
                    "Skip sanity check for SST sst_id {} object_id {} .",
                    sstable_info.sst_id,
                    sstable_info.object_id,
                );
                continue;
            }
        };

        let mut iter = SstableIterator::new(
            holder,
            sstable_store.clone(),
            Arc::new(SstableIteratorReadOptions {
                cache_policy: CachePolicy::NotFill,
                scan_end_user_key: None,
                prefetch: false,
                max_preload_retry_times: 0,
            }),
            &sstable_info,
        );
        let mut previous_key: Option<FullKey<Vec<u8>>> = None;
        if let Err(_err) = iter.rewind().await {
            tracing::info!(
                "Skip sanity check for SST sst_id {} object_id {}.",
                sstable_info.sst_id,
                sstable_info.object_id
            );
        }
        while iter.is_valid() {
            key_counts += 1;
            let current_key = iter.key().to_vec();
            // Locally unique and Globally unique
            if let Some((duplicate_sst_object_id, duplicate_worker_id)) =
                visited_keys.get(&current_key).cloned()
            {
                panic!(
                    "SST sanity check failed: Duplicate key {:x?} in SST object {} from worker {} and SST object {} from worker {}",
                    current_key,
                    sstable_info.object_id,
                    worker_id,
                    duplicate_sst_object_id,
                    duplicate_worker_id
                )
            }
            visited_keys.insert(current_key.clone(), (sstable_info.object_id, worker_id));
            // Ordered and Locally unique
            if let Some(previous_key) = previous_key.take() {
                let cmp = previous_key.cmp(&current_key);
                if cmp != cmp::Ordering::Less {
                    panic!(
                        "SST sanity check failed: For SST sst_id {} object_id {}, expect {:x?} < {:x?}, got {:#?}",
                        sstable_info.sst_id, sstable_info.object_id, previous_key, current_key, cmp
                    )
                }
            }
            previous_key = Some(current_key);
            if let Err(_err) = iter.next().await {
                tracing::info!(
                    "Skip remaining sanity check for SST {}",
                    sstable_info.object_id
                );
                break;
            }
        }
        tracing::debug!(
            "Validated {} keys for SST sst_id {} object_id {}",
            key_counts,
            sstable_info.sst_id,
            sstable_info.object_id
        );
        iter.collect_local_statistic(&mut unused);
        unused.ignore();
    }
}
