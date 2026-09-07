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

#![feature(coroutines)]
#![feature(proc_macro_hygiene)]
#![feature(register_tool)]
#![register_tool(rw)]
#![allow(rw::format_error)] // test code

#[macro_use]
mod replay_impl;

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use clap::Parser;
use foyer::{CacheBuilder, HybridCacheBuilder};
use replay_impl::{GlobalReplayImpl, get_replay_notification_client};
use risingwave_common::config::{
    NoOverride, ObjectStoreConfig, Role, extract_storage_memory_config, load_config,
};
use risingwave_common::system_param::reader::SystemParamsReader;
use risingwave_hummock_trace::{
    GlobalReplay, HummockReplay, Operation, Record, Result, TraceReader, TraceReaderImpl, USE_TRACE,
};
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_object_store::object::build_remote_object_store;
use risingwave_storage::compaction_catalog_manager::{
    CompactionCatalogManager, FakeRemoteTableAccessor,
};
use risingwave_storage::hummock::none::NoneRecentFilter;
use risingwave_storage::hummock::{HummockStorage, SstableStore, SstableStoreConfig};
use risingwave_storage::monitor::{CompactorMetrics, HummockStateStoreMetrics, ObjectStoreMetrics};
use risingwave_storage::opts::StorageOpts;

// use a large offset to avoid collision with real sstables
const SST_OFFSET: u64 = 2147383647000;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    path: String,

    // path to config file
    #[arg(short, long, default_value = "src/config/hummock-trace.toml")]
    config: String,

    #[arg(short, long)]
    object_storage: String,

    #[arg(short, long)]
    use_new_object_prefix_strategy: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();
    // disable runtime tracing when replaying
    unsafe { std::env::set_var(USE_TRACE, "false") };
    Box::pin(run_replay(args)).await.unwrap();
}

async fn run_replay(args: Args) -> Result<()> {
    let path = Path::new(&args.path);
    let f = BufReader::new(File::open(path)?);
    let mut reader = TraceReaderImpl::new_bincode(f)?;
    // first record is the snapshot
    let r: Record = reader.read().unwrap();
    let replay_interface = Box::pin(create_replay_hummock(r, &args)).await.unwrap();
    let mut replayer = HummockReplay::new(reader, replay_interface);
    replayer.run().await.unwrap();

    Ok(())
}

async fn create_replay_hummock(r: Record, args: &Args) -> Result<impl GlobalReplay + use<>> {
    let config = load_config(&args.config, NoOverride);
    let storage_memory_config = extract_storage_memory_config(&config);
    let system_params_reader =
        SystemParamsReader::from(config.system.clone().into_init_system_params());

    let storage_opts = Arc::new(StorageOpts::from((
        &config,
        &system_params_reader,
        &storage_memory_config,
    )));

    let state_store_metrics = Arc::new(HummockStateStoreMetrics::unused());
    let object_store_metrics = Arc::new(ObjectStoreMetrics::unused());

    let compactor_metrics = Arc::new(CompactorMetrics::unused());

    let object_store = build_remote_object_store(
        &args.object_storage,
        object_store_metrics,
        "Hummock",
        Arc::new(ObjectStoreConfig::default()),
        "",
    )
    .await;

    let meta_cache = HybridCacheBuilder::new()
        .memory(storage_opts.meta_cache_capacity_mb * (1 << 20))
        .with_shards(storage_opts.meta_cache_shard_num)
        .storage()
        .build()
        .await
        .unwrap();
    let block_cache = HybridCacheBuilder::new()
        .memory(storage_opts.block_cache_capacity_mb * (1 << 20))
        .with_shards(storage_opts.block_cache_shard_num)
        .storage()
        .build()
        .await
        .unwrap();

    let sstable_store = Arc::new(SstableStore::new(SstableStoreConfig {
        store: Arc::new(object_store),
        path: storage_opts.data_directory.clone(),
        prefetch_buffer_capacity: storage_opts.prefetch_buffer_capacity_mb * (1 << 20),
        max_prefetch_block_number: storage_opts.max_prefetch_block_number,
        recent_filter: Arc::new(NoneRecentFilter::default().into()),
        state_store_metrics: state_store_metrics.clone(),
        use_new_object_prefix_strategy: args.use_new_object_prefix_strategy,
        skip_bloom_filter_in_serde: storage_opts.sst_skip_bloom_filter_in_serde,
        meta_cache,
        block_cache,
        vector_meta_cache: CacheBuilder::new(1 << 10).build(),
        vector_block_cache: CacheBuilder::new(1 << 10).build(),
    }));

    let (hummock_meta_client, notification_client, notifier) = {
        let (env, hummock_manager_ref, cluster_controller_ref, worker_id) =
            setup_compute_env(8080).await;
        let notifier = env.notification_manager_ref();

        let worker_node = cluster_controller_ref
            .get_worker_by_id(worker_id)
            .await
            .unwrap()
            .unwrap();

        let notification_client = match r.operation {
            Operation::MetaMessage(resp) => get_replay_notification_client(env, worker_node, resp),
            _ => panic!("unexpected operation, found {:?}", r.operation),
        };

        (
            Arc::new(MockHummockMetaClient::with_sst_offset(
                hummock_manager_ref,
                worker_id as _,
                SST_OFFSET,
            )),
            notification_client,
            notifier,
        )
    };

    let storage = HummockStorage::new(
        Role::None,
        storage_opts,
        sstable_store,
        hummock_meta_client.clone(),
        notification_client,
        Arc::new(CompactionCatalogManager::new(Box::new(
            FakeRemoteTableAccessor {},
        ))),
        state_store_metrics,
        compactor_metrics,
        None,
    )
    .await
    .expect("failed to create a HummockStorage object");
    let replay_interface = GlobalReplayImpl::new(storage, notifier);

    Ok(replay_interface)
}
