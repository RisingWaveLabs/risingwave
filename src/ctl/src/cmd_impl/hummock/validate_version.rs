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

use std::cmp::Ordering;

use chrono::DateTime;
use chrono::offset::Utc;
use itertools::Itertools;
use risingwave_common::util::epoch::Epoch;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext;
use risingwave_hummock_sdk::key::{FullKey, UserKey};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_hummock_sdk::{HummockVersionId, version_archive_dir};
use risingwave_object_store::object::ObjectStoreRef;
use risingwave_pb::hummock::HummockVersionArchive;
use risingwave_pb::hummock::group_delta::DeltaType;
use risingwave_pb::id::HummockSstableId;
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::hummock::sstable::SstableMetaHandle;
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{Block, BlockHolder, BlockIterator, SstableStoreRef};
use risingwave_storage::monitor::StoreLocalStatistic;

use crate::CtlContext;
use crate::common::HummockServiceOpts;

pub async fn validate_version(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let version = meta_client.get_current_version().await?;
    let result = hummock_version_ext::validate_version(&version);
    if !result.is_empty() {
        println!("Invalid HummockVersion. Violation lists:");
        for s in result {
            println!("{}", s);
        }
    }

    Ok(())
}

async fn get_archive(
    archive_id: HummockVersionId,
    data_dir: &str,
    archive_object_store: ObjectStoreRef,
) -> anyhow::Result<HummockVersionArchive> {
    use prost::Message;
    let archive_dir = version_archive_dir(data_dir);
    let archive_path = format!("{archive_dir}/{archive_id}");
    let archive_bytes = archive_object_store.read(&archive_path, ..).await?;
    let archive: HummockVersionArchive = HummockVersionArchive::decode(archive_bytes)?;
    Ok(archive)
}

pub async fn print_user_key_in_archive(
    context: &CtlContext,
    archive_ids: impl IntoIterator<Item = HummockVersionId>,
    data_dir: String,
    user_key: String,
    use_new_object_prefix_strategy: bool,
) -> anyhow::Result<()> {
    let user_key_bytes = hex::decode(user_key.clone()).unwrap_or_else(|_| {
        panic!("cannot decode user key {} into raw bytes", user_key);
    });
    let user_key = UserKey::decode(&user_key_bytes);
    println!("user key: {user_key:?}");

    let hummock_opts =
        HummockServiceOpts::from_env(Some(data_dir.clone()), use_new_object_prefix_strategy)?;
    let hummock = context.hummock_store(hummock_opts).await?;
    let sstable_store = hummock.sstable_store();
    let archive_object_store = sstable_store.store();
    for archive_id in archive_ids.into_iter().sorted() {
        println!("search archive {archive_id}");
        let archive = get_archive(archive_id, &data_dir, archive_object_store.clone()).await?;
        let mut base_version =
            HummockVersion::from_persisted_protobuf(archive.version.as_ref().unwrap());
        print_user_key_in_version(sstable_store.clone(), &base_version, &user_key).await?;
        for delta in &archive.version_deltas {
            base_version.apply_version_delta(&HummockVersionDelta::from_persisted_protobuf(delta));
            print_user_key_in_version(sstable_store.clone(), &base_version, &user_key).await?;
        }
    }
    Ok(())
}

async fn print_user_key_in_version(
    sstable_store: SstableStoreRef,
    version: &HummockVersion,
    target_key: &UserKey<&[u8]>,
) -> anyhow::Result<()> {
    println!("print key {:?} in version {}", target_key, version.id);
    for cg in version.levels.values() {
        for level in cg.l0.sub_levels.iter().rev().chain(cg.levels.iter()) {
            for sstable_info in &level.table_infos {
                let key_range = &sstable_info.key_range;
                let left_user_key = FullKey::decode(&key_range.left);
                let right_user_key = FullKey::decode(&key_range.right);
                if left_user_key.user_key > *target_key || *target_key > right_user_key.user_key {
                    continue;
                }
                print_user_key_in_sst(sstable_store.clone(), sstable_info, target_key).await?;
            }
        }
    }
    Ok(())
}

async fn print_user_key_in_sst(
    sstable_store: SstableStoreRef,
    sst: &SstableInfo,
    user_key: &UserKey<&[u8]>,
) -> anyhow::Result<()> {
    // The implementation is mostly the same as `sst_dump`, with additional filter by `user_key`.
    let mut dummy = StoreLocalStatistic::default();
    let sst_metadata = sstable_store.sstable(sst, &mut dummy).await?;
    dummy.ignore();
    let data_path = sstable_store.get_sst_data_path(sst_metadata.id);
    let meta_handle = SstableMetaHandle::v2(&sst_metadata);
    let mut is_first = true;
    for block_idx in 0..meta_handle.block_count() {
        let (range, uncompressed_capacity) = meta_handle.block_range(block_idx);
        let block_data = sstable_store.store().read(&data_path, range).await?;
        let block = Box::new(Block::decode(block_data, uncompressed_capacity).unwrap());
        let holder = BlockHolder::from_owned_block(block);
        let mut block_iter = BlockIterator::new(holder);
        block_iter.seek_to_first();
        while block_iter.is_valid() {
            let full_key = block_iter.key();
            if full_key.user_key.cmp(user_key) != Ordering::Equal {
                block_iter.next();
                continue;
            }
            let full_val = block_iter.value();
            let hummock_val = HummockValue::from_slice(full_val)?;
            let epoch = Epoch::from(full_key.epoch_with_gap.pure_epoch());
            let date_time = DateTime::<Utc>::from(epoch.as_system_time());
            if is_first {
                is_first = false;
                println!("\t\tSST id: {}, object id: {}", sst.sst_id, sst.object_id);
            }
            println!("\t\t   key: {:?}, len={}", full_key, full_key.encoded_len());
            println!(
                "\t\t value: {:?}, len={}",
                hummock_val,
                hummock_val.encoded_len()
            );
            println!(
                "\t\t epoch: {} offset = {}  ({})",
                epoch,
                full_key.epoch_with_gap.offset(),
                date_time
            );
            println!();
            block_iter.next();
        }
    }
    Ok(())
}

pub async fn print_version_delta_in_archive(
    context: &CtlContext,
    archive_ids: impl IntoIterator<Item = HummockVersionId>,
    data_dir: String,
    sst_id: HummockSstableId,
    use_new_object_prefix_strategy: bool,
) -> anyhow::Result<()> {
    let hummock_opts =
        HummockServiceOpts::from_env(Some(data_dir.clone()), use_new_object_prefix_strategy)?;
    let hummock = context.hummock_store(hummock_opts).await?;
    let sstable_store = hummock.sstable_store();
    let archive_object_store = sstable_store.store();
    for archive_id in archive_ids.into_iter().sorted() {
        println!("search archive {archive_id}");
        let archive = get_archive(archive_id, &data_dir, archive_object_store.clone()).await?;
        for delta in &archive.version_deltas {
            let mut is_first = true;
            for (cg_id, deltas) in &delta.group_deltas {
                for d in &deltas.group_deltas {
                    let d = d.delta_type.as_ref().unwrap();
                    if match_delta(d, sst_id) {
                        if is_first {
                            is_first = false;
                            println!(
                                "delta: id {}, prev_id {}, trivial_move {}",
                                delta.id, delta.prev_id, delta.trivial_move
                            );
                        }
                        println!("compaction group id {cg_id}");
                        print_delta(d);
                    }
                }
            }
        }
    }
    Ok(())
}

fn match_delta(delta: &DeltaType, sst_id: HummockSstableId) -> bool {
    match delta {
        DeltaType::GroupConstruct(_)
        | DeltaType::GroupDestroy(_)
        | DeltaType::GroupMerge(_)
        | DeltaType::TruncateTables(_) => false,
        DeltaType::IntraLevel(delta) => {
            delta
                .inserted_table_infos
                .iter()
                .any(|sst| sst.sst_id == sst_id)
                || delta.removed_table_ids.contains(&sst_id)
        }
        DeltaType::NewL0SubLevel(delta) => delta
            .inserted_table_infos
            .iter()
            .any(|sst| sst.sst_id == sst_id),
    }
}

fn print_delta(delta: &DeltaType) {
    println!("{:?}", delta);
}
