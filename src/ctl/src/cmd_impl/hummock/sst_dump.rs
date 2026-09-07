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
use std::sync::Arc;

use bytes::{Buf, Bytes};
use chrono::DateTime;
use chrono::offset::Utc;
use clap::Args;
use futures::TryStreamExt;
use itertools::Itertools;
use risingwave_common::types::ToText;
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::value_encoding::column_aware_row_encoding::ColumnAwareSerde;
use risingwave_common::util::value_encoding::{BasicSerde, EitherSerde, ValueRowDeserializer};
use risingwave_frontend::TableCatalog;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::level::Level;
use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
use risingwave_hummock_sdk::{HummockObjectId, HummockSstableObjectId};
use risingwave_object_store::object::{ObjectMetadata, ObjectStoreImpl};
use risingwave_pb::hummock::{PbSstableFilterLayout, PbSstableFilterType};
use risingwave_pb::id::TableId;
use risingwave_rpc_client::MetaClient;
use risingwave_storage::hummock::sstable::SstableMetaHandle;
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{
    Block, BlockHolder, BlockIterator, CompressionAlgorithm, Sstable, SstableStore,
};
use risingwave_storage::monitor::StoreLocalStatistic;
use risingwave_storage::row_serde::value_serde::ValueRowSerdeNew;

use crate::CtlContext;
use crate::common::HummockServiceOpts;

type TableData = HashMap<TableId, TableCatalog>;

#[derive(Args, Debug)]
pub struct SstDumpArgs {
    #[clap(short, long = "object-id")]
    object_id: Option<u64>,
    #[clap(short, long = "block-id")]
    block_id: Option<u64>,
    #[clap(short = 'p', long = "print-entry")]
    print_entry: bool,
    #[clap(short = 'l', long = "print-level")]
    print_level: bool,
    #[clap(short = 't', long = "print-table")]
    print_table: bool,
    #[clap(short = 'v', long = "print-vnode-stats")]
    print_vnode_stats: bool,
    #[clap(short = 'd')]
    data_dir: Option<String>,
    #[clap(short, long = "use-new-object-prefix-strategy", default_value = "true")]
    use_new_object_prefix_strategy: bool,
}

pub async fn sst_dump(context: &CtlContext, args: SstDumpArgs) -> anyhow::Result<()> {
    println!("Start sst dump with args: {:?}", args);
    let table_data = if args.print_entry && args.print_table {
        let meta_client = context.meta_client().await?;
        load_table_schemas(&meta_client).await?
    } else {
        TableData::default()
    };
    if args.print_level {
        // Level information is retrieved from meta service
        let hummock = context
            .hummock_store(HummockServiceOpts::from_env(
                args.data_dir.clone(),
                args.use_new_object_prefix_strategy,
            )?)
            .await?;
        let version = hummock.inner().get_pinned_version().clone();
        let sstable_store = hummock.sstable_store();
        for level in version.get_combined_levels() {
            for sstable_info in &level.table_infos {
                if let Some(object_id) = &args.object_id {
                    if *object_id == sstable_info.object_id {
                        print_level(level, sstable_info, &args);
                        sst_dump_via_sstable_store(
                            &sstable_store,
                            sstable_info.object_id,
                            sstable_info.meta_offset,
                            sstable_info.file_size,
                            &table_data,
                            &args,
                        )
                        .await?;
                        return Ok(());
                    }
                } else {
                    print_level(level, sstable_info, &args);
                    sst_dump_via_sstable_store(
                        &sstable_store,
                        sstable_info.object_id,
                        sstable_info.meta_offset,
                        sstable_info.file_size,
                        &table_data,
                        &args,
                    )
                    .await?;
                }
            }
        }
    } else {
        // Object information is retrieved from object store. Meta service is not required.
        let hummock_service_opts = HummockServiceOpts::from_env(
            args.data_dir.clone(),
            args.use_new_object_prefix_strategy,
        )?;
        let sstable_store = hummock_service_opts
            .create_sstable_store(args.use_new_object_prefix_strategy)
            .await?;
        if let Some(obj_id) = &args.object_id {
            let obj_store = sstable_store.store();
            let obj_path = sstable_store.get_sst_data_path(*obj_id);
            let obj = obj_store.metadata(&obj_path).await?;
            print_object(&obj);
            let meta_offset = get_meta_offset_from_object(&obj, obj_store.as_ref()).await?;
            sst_dump_via_sstable_store(
                &sstable_store,
                (*obj_id).into(),
                meta_offset,
                obj.total_size as u64,
                &table_data,
                &args,
            )
            .await?;
        } else {
            let mut metadata_iter = sstable_store
                .list_sst_object_metadata_from_object_store(None, None, None)
                .await?;
            while let Some(obj) = metadata_iter.try_next().await? {
                print_object(&obj);
                let obj_id = SstableStore::get_object_id_from_path(&obj.key);
                let obj_id = match obj_id {
                    HummockObjectId::Sstable(obj_id) => obj_id,
                    HummockObjectId::VectorFile(_) | HummockObjectId::HnswGraphFile(_) => {
                        println!(
                            "object id {:?} not a sstable object id: {}. skip",
                            obj_id, obj.key
                        );
                        continue;
                    }
                };
                let meta_offset =
                    get_meta_offset_from_object(&obj, sstable_store.store().as_ref()).await?;
                sst_dump_via_sstable_store(
                    &sstable_store,
                    obj_id,
                    meta_offset,
                    obj.total_size as u64,
                    &table_data,
                    &args,
                )
                .await?;
            }
        }
    }

    Ok(())
}

fn print_level(level: &Level, sst_info: &SstableInfo, args: &SstDumpArgs) {
    println!("Level Type: {}", level.level_type.as_str_name());
    println!("Level Idx: {}", level.level_idx);
    if level.level_idx == 0 {
        println!("L0 Sub-Level Idx: {}", level.sub_level_id);
    }
    println!("SST id: {}", sst_info.sst_id);
    println!("SST table_ids: {:?}", sst_info.table_ids);
    if args.print_vnode_stats {
        if let Some(ref stats) = sst_info.vnode_statistics {
            println!("Vnode Statistics: {:?}", stats);
        } else {
            println!("Vnode Statistics: None");
        }
    }
}

fn print_object(obj: &ObjectMetadata) {
    println!("Object Key: {}", obj.key);
    println!("Object Size: {}", obj.total_size);
    println!("Object Last Modified: {}", obj.last_modified);
}

async fn get_meta_offset_from_object(
    obj: &ObjectMetadata,
    obj_store: &ObjectStoreImpl,
) -> anyhow::Result<u64> {
    let start = obj.total_size
        - (
            // version, magic
            2 * std::mem::size_of::<u32>() +
        // footer, checksum
        2 * std::mem::size_of::<u64>()
        );
    let end = start + std::mem::size_of::<u64>();
    Ok(obj_store.read(&obj.key, start..end).await?.get_u64_le())
}

pub async fn sst_dump_via_sstable_store(
    sstable_store: &SstableStore,
    object_id: HummockSstableObjectId,
    meta_offset: u64,
    file_size: u64,
    table_data: &TableData,
    args: &SstDumpArgs,
) -> anyhow::Result<()> {
    let sstable_info = SstableInfoInner {
        object_id,
        file_size,
        meta_offset,
        // below are default unused value
        sst_id: 0.into(),
        key_range: Default::default(),
        table_ids: vec![],
        stale_key_count: 0,
        total_key_count: 0,
        min_epoch: 0,
        max_epoch: 0,
        uncompressed_file_size: 0,
        range_tombstone_count: 0,
        filter_type: PbSstableFilterType::SstableFilterNone,
        filter_layout: PbSstableFilterLayout::Unspecified,
        sst_size: 0,
        vnode_statistics: None,
    }
    .into();
    let sstable_cache = sstable_store
        .sstable(&sstable_info, &mut StoreLocalStatistic::default())
        .await?;
    let sstable = sstable_cache.as_ref();
    let sstable_meta = &sstable.meta;
    let meta_handle = SstableMetaHandle::v2(sstable);
    let smallest_key = FullKey::decode(meta_handle.smallest_key());
    let largest_key = FullKey::decode(meta_handle.largest_key());

    println!("SST object id: {}", object_id);
    println!("-------------------------------------");
    println!("File Size: {}", meta_handle.estimated_size());

    println!("Key Range:");
    println!(
        "\tleft:\t{:?}\n\tright:\t{:?}\n\t",
        smallest_key, largest_key,
    );

    println!("Estimated Table Size: {}", meta_handle.estimated_size());
    println!("Bloom Filter Size: {}", sstable_meta.bloom_filter.len());
    println!("Key Count: {}", sstable_meta.key_count);
    println!("Version: {}", sstable_meta.version);

    println!("Block Count: {}", meta_handle.block_count());
    for i in 0..meta_handle.block_count() {
        if let Some(block_id) = &args.block_id {
            if *block_id == i as u64 {
                print_block(i, table_data, sstable_store, sstable, args).await?;
                return Ok(());
            }
        } else {
            print_block(i, table_data, sstable_store, sstable, args).await?;
        }
    }
    Ok(())
}

/// Determine all database tables and adds their information into a hash table with the table-ID as
/// key.
async fn load_table_schemas(meta_client: &MetaClient) -> anyhow::Result<TableData> {
    let mut tables = HashMap::new();

    let mvs = meta_client.risectl_list_state_tables().await?;
    mvs.iter().for_each(|tbl| {
        tables.insert(tbl.id, tbl.into());
    });

    Ok(tables)
}

/// Prints a block of a given SST including all contained KV-pairs.
async fn print_block(
    block_idx: usize,
    table_data: &TableData,
    sstable_store: &SstableStore,
    sst: &Sstable,
    args: &SstDumpArgs,
) -> anyhow::Result<()> {
    println!("\tBlock {}", block_idx);
    println!("\t-----------");

    let meta_handle = SstableMetaHandle::v2(sst);
    let block_meta = meta_handle.block_meta(block_idx);
    let smallest_key = FullKey::decode(&block_meta.smallest_key);
    let data_path = sstable_store.get_sst_data_path(sst.id);

    // Retrieve encoded block data in bytes
    let store = sstable_store.store();
    let (range, _) = meta_handle.block_range(block_idx);
    let block_data = store.read(&data_path, range).await?;

    // Retrieve checksum and compression algorithm used from the encoded block data
    let len = block_data.len();
    let checksum = (&block_data[len - 8..]).get_u64_le();
    let compression = CompressionAlgorithm::decode(&mut &block_data[len - 9..len - 8])?;

    println!(
        "\tOffset: {}, Size: {}, Checksum: {}, Compression Algorithm: {:?}, Smallest Key: {:?}",
        block_meta.offset, block_meta.len, checksum, compression, smallest_key
    );

    if args.print_entry {
        print_kv_pairs(
            block_data,
            table_data,
            block_meta.uncompressed_size as usize,
            args,
        )?;
    }

    Ok(())
}

/// Prints the data of KV-Pairs of a given block out to the terminal.
fn print_kv_pairs(
    block_data: Bytes,
    table_data: &TableData,
    uncompressed_capacity: usize,
    args: &SstDumpArgs,
) -> anyhow::Result<()> {
    println!("\tKV-Pairs:");

    let block = Box::new(Block::decode(block_data, uncompressed_capacity).unwrap());
    let holder = BlockHolder::from_owned_block(block);
    let mut block_iter = BlockIterator::new(holder);
    block_iter.seek_to_first();

    while block_iter.is_valid() {
        let full_key = block_iter.key();
        let full_val = block_iter.value();
        let humm_val = HummockValue::from_slice(full_val)?;

        let epoch = Epoch::from(full_key.epoch_with_gap.pure_epoch());
        let date_time = DateTime::<Utc>::from(epoch.as_system_time());

        println!("\t\t   key: {:?}, len={}", full_key, full_key.encoded_len());
        println!("\t\t value: {:?}, len={}", humm_val, humm_val.encoded_len());
        println!(
            "\t\t epoch: {} offset = {}  ({})",
            epoch,
            full_key.epoch_with_gap.offset(),
            date_time
        );
        if args.print_table {
            print_table_column(full_key, humm_val, table_data)?;
        }

        println!();

        block_iter.next();
    }

    Ok(())
}

/// If possible, prints information about the table, column, and stored value.
fn print_table_column(
    full_key: FullKey<&[u8]>,
    humm_val: HummockValue<&[u8]>,
    table_data: &TableData,
) -> anyhow::Result<()> {
    let table_id = full_key.user_key.table_id;

    print!("\t\t table: id={}, ", table_id);
    let table_catalog = match table_data.get(&table_id) {
        None => {
            // Table may have been dropped.
            println!("(unknown)");
            return Ok(());
        }
        Some(table) => table,
    };
    println!(
        "name={}, version={:?}",
        table_catalog.name,
        table_catalog.version()
    );

    if let Some(user_val) = humm_val.into_user_value() {
        let column_desc = table_catalog
            .value_indices
            .iter()
            .map(|idx| table_catalog.columns[*idx].column_desc.name.clone())
            .collect_vec();

        let row_deserializer: EitherSerde = if table_catalog.version().is_some() {
            ColumnAwareSerde::new(
                table_catalog.value_indices.clone().into(),
                Arc::from_iter(
                    table_catalog
                        .columns()
                        .iter()
                        .cloned()
                        .map(|c| c.column_desc),
                ),
            )
            .into()
        } else {
            BasicSerde::new(
                table_catalog.value_indices.clone().into(),
                Arc::from_iter(
                    table_catalog
                        .columns()
                        .iter()
                        .cloned()
                        .map(|c| c.column_desc),
                ),
            )
            .into()
        };
        let row = row_deserializer.deserialize(user_val)?;
        for (c, v) in column_desc.iter().zip_eq_fast(row.iter()) {
            println!(
                "\t\tcolumn: {} {:?}",
                c,
                v.as_ref().map(|v| v.as_scalar_ref_impl().to_text())
            );
        }
    }

    Ok(())
}
