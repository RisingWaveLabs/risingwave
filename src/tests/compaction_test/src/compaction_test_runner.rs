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

use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::anyhow;
use bytes::{BufMut, Bytes, BytesMut};
use clap::Parser;
use foyer::Hint;
use risingwave_common::catalog::TableOption;
use risingwave_common::config::{
    MetaConfig, NoOverride, Role, extract_storage_memory_config, load_config,
};
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::tokio_util::sync::CancellationToken;
use risingwave_hummock_sdk::key::TableKey;
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_hummock_sdk::{
    CompactionGroupId, FIRST_VERSION_ID, HummockEpoch, HummockReadEpoch, HummockVersionId,
};
use risingwave_pb::common::WorkerType;
use risingwave_pb::id::TableId;
use risingwave_rpc_client::{HummockMetaClient, MetaClient};
use risingwave_storage::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use risingwave_storage::hummock::{CachePolicy, HummockStorage};
use risingwave_storage::monitor::{
    CompactorMetrics, HummockMetrics, HummockStateStoreMetrics, MonitoredStateStore,
    MonitoredStorageMetrics, ObjectStoreMetrics,
};
use risingwave_storage::opts::StorageOpts;
use risingwave_storage::store::{NewReadSnapshotOptions, ReadOptions, StateStoreRead};
use risingwave_storage::{StateStore, StateStoreImpl, StateStoreIter};

const SST_ID_SHIFT_COUNT: u32 = 1000000;
const CHECKPOINT_FREQ_FOR_REPLAY: u64 = 99999999;

use crate::CompactionTestOpts;

struct CompactionTestMetrics {
    num_expect_check: u64,
    num_uncheck: u64,
}

impl CompactionTestMetrics {
    fn new() -> CompactionTestMetrics {
        Self {
            num_expect_check: 0,
            num_uncheck: 0,
        }
    }
}

/// Steps to use the compaction test tool
///
/// 1. Start the cluster with ci-compaction-test config: `./risedev d ci-compaction-test`
/// 2. Ingest enough L0 SSTs, for example we can use the tpch-bench tool
/// 3. Disable hummock manager commit new epochs: `./risedev ctl hummock disable-commit-epoch`, and
///    it will print the current max committed epoch in Meta.
/// 4. Use the test tool to replay hummock version deltas and trigger compactions:
///    `./risedev compaction-test --state-store hummock+s3://your-bucket -t <table_id>`
pub async fn compaction_test_main(
    _listen_addr: SocketAddr,
    advertise_addr: HostAddr,
    opts: CompactionTestOpts,
) -> anyhow::Result<()> {
    let meta_listen_addr = opts
        .meta_address
        .strip_prefix("http://")
        .unwrap()
        .to_owned();

    let _meta_handle = tokio::spawn(start_meta_node(
        meta_listen_addr.clone(),
        opts.state_store.clone(),
        opts.config_path_for_meta.clone(),
    ));

    // Wait for meta starts
    tokio::time::sleep(Duration::from_secs(1)).await;
    tracing::info!("Started embedded Meta");

    let (compactor_thrd, compactor_shutdown_tx) = start_compactor_thread(
        opts.meta_address.clone(),
        advertise_addr.to_string(),
        opts.config_path.clone(),
    );

    let original_meta_endpoint = "http://127.0.0.1:5690";
    let mut table_id = opts.table_id.into();

    init_metadata_for_replay(
        original_meta_endpoint,
        &opts.meta_address,
        &advertise_addr,
        opts.ci_mode,
        &mut table_id,
    )
    .await?;

    assert_ne!(table_id, 0, "Invalid table_id for correctness checking");

    let version_deltas = pull_version_deltas(original_meta_endpoint, &advertise_addr).await?;

    tracing::info!(
        "Pulled delta logs from Meta: len(logs): {}",
        version_deltas.len()
    );

    let replay_thrd = start_replay_thread(opts, table_id, version_deltas);
    replay_thrd.join().unwrap();
    compactor_shutdown_tx.send(()).unwrap();
    compactor_thrd.join().unwrap();
    Ok(())
}

pub async fn start_meta_node(listen_addr: String, state_store: String, config_path: String) {
    let meta_opts = risingwave_meta_node::MetaNodeOpts::parse_from([
        "meta-node",
        "--listen-addr",
        &listen_addr,
        "--advertise-addr",
        &listen_addr,
        "--backend",
        "mem",
        "--state-store",
        &state_store,
        "--config-path",
        &config_path,
    ]);
    let config = load_config(&meta_opts.config_path, &meta_opts);
    // We set a large checkpoint frequency to prevent the embedded meta node
    // to commit new epochs to avoid bumping the hummock version during version log replay.
    assert_eq!(
        CHECKPOINT_FREQ_FOR_REPLAY,
        config.system.checkpoint_frequency.unwrap()
    );
    assert!(
        config.meta.enable_compaction_deterministic,
        "enable_compaction_deterministic should be set"
    );

    risingwave_meta_node::start(meta_opts, CancellationToken::new() /* dummy */).await
}

async fn start_compactor_node(
    meta_rpc_endpoint: String,
    advertise_addr: String,
    config_path: String,
) {
    let opts = risingwave_compactor::CompactorOpts::parse_from([
        "compactor-node",
        "--listen-addr",
        "127.0.0.1:5550",
        "--advertise-addr",
        &advertise_addr,
        "--meta-address",
        &meta_rpc_endpoint,
        "--config-path",
        &config_path,
    ]);
    risingwave_compactor::start(opts, CancellationToken::new() /* dummy */).await
}

pub fn start_compactor_thread(
    meta_endpoint: String,
    advertise_addr: String,
    config_path: String,
) -> (JoinHandle<()>, std::sync::mpsc::Sender<()>) {
    let (tx, rx) = std::sync::mpsc::channel();
    let compact_func = move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            tokio::spawn(async {
                tracing::info!("Starting compactor node");
                start_compactor_node(meta_endpoint, advertise_addr, config_path).await
            });
            rx.recv().unwrap();
        });
    };

    (std::thread::spawn(compact_func), tx)
}

fn start_replay_thread(
    opts: CompactionTestOpts,
    table_id: TableId,
    version_deltas: Vec<HummockVersionDelta>,
) -> JoinHandle<()> {
    let replay_func = move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime
            .block_on(start_replay(opts, table_id, version_deltas))
            .expect("repaly error occurred");
    };

    std::thread::spawn(replay_func)
}

async fn init_metadata_for_replay(
    cluster_meta_endpoint: &str,
    new_meta_endpoint: &str,
    advertise_addr: &HostAddr,
    ci_mode: bool,
    table_id: &mut TableId,
) -> anyhow::Result<()> {
    // The compactor needs to receive catalog notification from the new Meta node,
    // and we should wait the compactor finishes setup the subscription channel
    // before registering the table catalog to the new Meta node. Otherwise the
    // filter key manager will fail to acquire a key extractor.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let meta_config = MetaConfig::default();
    let meta_client: MetaClient;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl+C received, now exiting");
            std::process::exit(0);
        },
        ret = MetaClient::register_new(cluster_meta_endpoint.parse()?, WorkerType::RiseCtl, advertise_addr, Default::default(), Arc::new(meta_config.clone())) => {
            (meta_client, _) = ret;
        },
    }
    let worker_id = meta_client.worker_id();
    tracing::info!("Assigned init worker id {}", worker_id);
    meta_client.activate(advertise_addr).await.unwrap();

    let tables = meta_client.risectl_list_state_tables().await?;

    let (new_meta_client, _) = MetaClient::register_new(
        new_meta_endpoint.parse()?,
        WorkerType::RiseCtl,
        advertise_addr,
        Default::default(),
        Arc::new(meta_config.clone()),
    )
    .await;
    new_meta_client.activate(advertise_addr).await.unwrap();
    if ci_mode {
        let table_to_check = tables.iter().find(|t| t.name == "nexmark_q7").unwrap();
        *table_id = table_to_check.id;
    }

    // No need to init compaction_groups, because it will be done when replaying version delta.
    new_meta_client
        .init_metadata_for_replay(tables, vec![])
        .await?;

    // shift the sst id to avoid conflict with the original meta node
    let _ = new_meta_client
        .get_new_object_ids(SST_ID_SHIFT_COUNT)
        .await?;

    tracing::info!("Finished initializing the new Meta");
    Ok(())
}

async fn pull_version_deltas(
    cluster_meta_endpoint: &str,
    advertise_addr: &HostAddr,
) -> anyhow::Result<Vec<HummockVersionDelta>> {
    // Register to the cluster.
    // We reuse the RiseCtl worker type here
    let (meta_client, _) = MetaClient::register_new(
        cluster_meta_endpoint.parse()?,
        WorkerType::RiseCtl,
        advertise_addr,
        Default::default(),
        Arc::new(MetaConfig::default()),
    )
    .await;
    let worker_id = meta_client.worker_id();
    tracing::info!("Assigned pull worker id {}", worker_id);
    meta_client.activate(advertise_addr).await.unwrap();

    let (handle, shutdown_tx) =
        MetaClient::start_heartbeat_loop(meta_client.clone(), Duration::from_millis(1000));
    let res = meta_client
        .list_version_deltas(HummockVersionId::new(0), u32::MAX, u64::MAX)
        .await
        .unwrap();

    if let Err(err) = shutdown_tx.send(()) {
        tracing::warn!("Failed to send shutdown to heartbeat task: {:?}", err);
    }
    handle.await?;
    tracing::info!("Shutdown the pull worker");
    Ok(res)
}

async fn start_replay(
    opts: CompactionTestOpts,
    table_to_check: TableId,
    version_delta_logs: Vec<HummockVersionDelta>,
) -> anyhow::Result<()> {
    let advertise_addr = "127.0.0.1:7770".parse().unwrap();
    tracing::info!(
        "Start to replay. Advertise address is {}, Table id {}",
        advertise_addr,
        table_to_check
    );

    let mut metric = CompactionTestMetrics::new();
    let config = load_config(&opts.config_path_for_meta, NoOverride);
    tracing::info!(
        "Starting replay with config {:?} and opts {:?}",
        config,
        opts
    );

    // Register to the cluster.
    // We reuse the RiseCtl worker type here
    let (meta_client, system_params) = MetaClient::register_new(
        opts.meta_address.parse()?,
        WorkerType::RiseCtl,
        &advertise_addr,
        Default::default(),
        Arc::new(config.meta.clone()),
    )
    .await;
    let worker_id = meta_client.worker_id();
    tracing::info!("Assigned replay worker id {}", worker_id);
    meta_client.activate(&advertise_addr).await.unwrap();

    let sub_tasks = vec![MetaClient::start_heartbeat_loop(
        meta_client.clone(),
        Duration::from_millis(1000),
    )];

    // Prevent the embedded meta to commit new epochs during version replay
    let latest_version = meta_client.disable_commit_epoch().await?;
    assert_eq!(FIRST_VERSION_ID, latest_version.id);
    // The new meta should not have any data at this time
    for level in latest_version.levels.values() {
        level.levels.iter().for_each(|lvl| {
            assert!(lvl.table_infos.is_empty());
            assert_eq!(0, lvl.total_file_size);
        });
    }

    // Creates a hummock state store *after* we reset the hummock version
    let storage_memory_config = extract_storage_memory_config(&config);
    let storage_opts = Arc::new(StorageOpts::from((
        &config,
        &system_params,
        &storage_memory_config,
    )));
    let hummock = create_hummock_store_with_metrics(&meta_client, storage_opts, &opts).await?;

    // Replay version deltas from FIRST_VERSION_ID to the version before reset
    let mut modified_compaction_groups = HashSet::<CompactionGroupId>::new();
    let mut replay_count: u64 = 0;
    let mut replayed_epochs = vec![];
    let mut check_result_task: Option<tokio::task::JoinHandle<_>> = None;

    for delta in version_delta_logs {
        let (current_version, compaction_groups) = meta_client.replay_version_delta(delta).await?;
        let (version_id, committed_epoch) = (
            current_version.id,
            current_version
                .table_committed_epoch(table_to_check)
                .unwrap_or_default(),
        );
        tracing::info!(
            "Replayed version delta version_id: {}, committed_epoch: {}, compaction_groups: {:?}",
            version_id,
            committed_epoch,
            compaction_groups
        );

        hummock
            .inner()
            .update_version_and_wait(current_version.clone())
            .await;

        replay_count += 1;
        replayed_epochs.push(committed_epoch);
        compaction_groups
            .into_iter()
            .map(|c| modified_compaction_groups.insert(c))
            .count();

        // We can custom more conditions for compaction triggering
        // For now I just use a static way here
        if replay_count.is_multiple_of(opts.num_trigger_frequency)
            && !modified_compaction_groups.is_empty()
        {
            // join previously spawned check result task
            if let Some(handle) = check_result_task {
                handle.await??;
            }

            metric.num_expect_check += 1;

            // pop the latest epoch
            replayed_epochs.pop();
            let mut epochs = vec![committed_epoch];
            epochs.extend(pin_old_snapshots(&meta_client, &replayed_epochs, 1));
            tracing::info!("===== Prepare to check snapshots: {:?}", epochs);

            let old_version_iters = open_hummock_iters(&hummock, &epochs, table_to_check).await?;

            tracing::info!(
                "Trigger compaction for version {}, epoch {} compaction_groups: {:?}",
                version_id,
                committed_epoch,
                modified_compaction_groups,
            );
            // Try trigger multiple rounds of compactions but doesn't wait for finish
            let is_multi_round = opts.num_trigger_rounds > 1;
            for _ in 0..opts.num_trigger_rounds {
                meta_client
                    .trigger_compaction_deterministic(
                        version_id,
                        Vec::from_iter(modified_compaction_groups.iter().copied()),
                    )
                    .await?;
                if is_multi_round {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }

            let old_task_num = meta_client.get_assigned_compact_task_num().await?;
            // Poll for compaction task status
            let (schedule_ok, version_diff) =
                poll_compaction_schedule_status(&meta_client, old_task_num).await;

            tracing::info!(
                "Compaction schedule_ok {}, version_diff {}",
                schedule_ok,
                version_diff,
            );
            let (compaction_ok, new_version) = poll_compaction_tasks_status(
                &meta_client,
                schedule_ok,
                version_diff as u32,
                &current_version,
            )
            .await;

            tracing::info!(
                "Compaction schedule_ok {}, version_diff {} compaction_ok {}",
                schedule_ok,
                version_diff,
                compaction_ok,
            );

            let new_version_id = new_version.id;
            assert!(
                new_version_id >= version_id,
                "new_version_id: {}",
                new_version_id,
            );

            if new_version_id != version_id {
                hummock.inner().update_version_and_wait(new_version).await;

                let new_version_iters =
                    open_hummock_iters(&hummock, &epochs, table_to_check).await?;

                // spawn a task to check the results
                check_result_task = Some(tokio::spawn(check_compaction_results(
                    new_version_id,
                    old_version_iters,
                    new_version_iters,
                )));
            } else {
                check_result_task = None;
                metric.num_uncheck += 1;
            }
            modified_compaction_groups.clear();
            replayed_epochs.clear();
        }
    }

    // join previously spawned check result task if any
    if let Some(handle) = check_result_task {
        handle.await??;
    }
    tracing::info!(
        "Replay finished. Expect check count: {}, actual check count: {}",
        metric.num_expect_check,
        metric.num_expect_check - metric.num_uncheck
    );

    assert_ne!(0, metric.num_expect_check - metric.num_uncheck);

    for (join_handle, shutdown_sender) in sub_tasks {
        if let Err(err) = shutdown_sender.send(()) {
            tracing::warn!("Failed to send shutdown: {:?}", err);
            continue;
        }
        if let Err(err) = join_handle.await {
            tracing::warn!("Failed to join shutdown: {:?}", err);
        }
    }

    Ok(())
}

fn pin_old_snapshots(
    _meta_client: &MetaClient,
    replayed_epochs: &[HummockEpoch],
    num: usize,
) -> Vec<HummockEpoch> {
    let mut old_epochs = vec![];
    for &epoch in replayed_epochs.iter().rev().take(num) {
        old_epochs.push(epoch);
    }
    old_epochs
}

/// Poll the compaction task assignment to aware whether scheduling is success.
/// Returns (whether scheduling is success, expected number of new versions)
async fn poll_compaction_schedule_status(
    meta_client: &MetaClient,
    old_task_num: usize,
) -> (bool, i32) {
    let poll_timeout = Duration::from_secs(2);
    let poll_interval = Duration::from_millis(20);
    let mut poll_duration_cnt = Duration::from_millis(0);
    let mut new_task_num = meta_client.get_assigned_compact_task_num().await.unwrap();
    let mut schedule_ok = false;
    loop {
        // New task has been scheduled
        if new_task_num > old_task_num {
            schedule_ok = true;
            break;
        }

        if poll_duration_cnt >= poll_timeout {
            break;
        }
        tokio::time::sleep(poll_interval).await;
        poll_duration_cnt += poll_interval;
        new_task_num = meta_client.get_assigned_compact_task_num().await.unwrap();
    }
    (
        schedule_ok,
        (new_task_num as i32 - old_task_num as i32).abs(),
    )
}

async fn poll_compaction_tasks_status(
    meta_client: &MetaClient,
    schedule_ok: bool,
    version_diff: u32,
    base_version: &HummockVersion,
) -> (bool, HummockVersion) {
    // Polls current version to check whether its id become large,
    // which means compaction tasks have finished. If schedule ok,
    // we poll for a little long while.
    let poll_timeout = if schedule_ok {
        Duration::from_secs(120)
    } else {
        Duration::from_secs(5)
    };
    let poll_interval = Duration::from_millis(50);
    let mut duration_cnt = Duration::from_millis(0);
    let mut compaction_ok = false;

    let mut cur_version = meta_client.get_current_version().await.unwrap();
    loop {
        if (cur_version.id > base_version.id)
            && (cur_version.id - base_version.id >= version_diff as u64)
        {
            tracing::info!(
                "Collected all of compact tasks. Actual version diff {}",
                cur_version.id - base_version.id
            );
            compaction_ok = true;
            break;
        }
        if duration_cnt >= poll_timeout {
            break;
        }
        tokio::time::sleep(poll_interval).await;
        duration_cnt += poll_interval;
        cur_version = meta_client.get_current_version().await.unwrap();
    }
    (compaction_ok, cur_version)
}

type StateStoreIterType = Pin<
    Box<
        <<MonitoredStateStore<HummockStorage> as StateStore>::ReadSnapshot as StateStoreRead>::Iter,
    >,
>;

async fn open_hummock_iters(
    hummock: &MonitoredStateStore<HummockStorage>,
    snapshots: &[HummockEpoch],
    table_id: TableId,
) -> anyhow::Result<BTreeMap<HummockEpoch, StateStoreIterType>> {
    let mut results = BTreeMap::new();

    // Set the `table_id` to the prefix of key, since the table_id in
    // the `ReadOptions` will not be used to filter kv pairs
    let mut buf = BytesMut::with_capacity(5);
    buf.put_u32(table_id.as_raw_id());
    let b = buf.freeze();
    let range = (
        Bound::Included(b.clone()).map(TableKey),
        Bound::Excluded(Bytes::from(risingwave_hummock_sdk::key::next_key(
            b.as_ref(),
        )))
        .map(TableKey),
    );

    for &epoch in snapshots {
        let snapshot = hummock
            .new_read_snapshot(
                HummockReadEpoch::NoWait(epoch),
                NewReadSnapshotOptions {
                    table_id,
                    table_option: TableOption::default(),
                },
            )
            .await?;
        let iter = snapshot
            .iter(
                range.clone(),
                ReadOptions {
                    cache_policy: CachePolicy::Fill(Hint::Normal),
                    ..Default::default()
                },
            )
            .await?;
        results.insert(epoch, Box::pin(iter));
    }
    Ok(results)
}

pub async fn check_compaction_results(
    version_id: HummockVersionId,
    mut expect_results: BTreeMap<HummockEpoch, StateStoreIterType>,
    mut actual_results: BTreeMap<HummockEpoch, StateStoreIterType>,
) -> anyhow::Result<()> {
    let combined = expect_results
        .iter_mut()
        .zip_eq_fast(actual_results.iter_mut());
    for ((e1, expect_iter), (e2, actual_iter)) in combined {
        assert_eq!(e1, e2);
        tracing::info!(
            "Check results for version: id: {}, epoch: {}",
            version_id,
            e1,
        );
        let mut expect_cnt = 0;
        let mut actual_cnt = 0;

        while let Some(kv_expect) = expect_iter.try_next().await? {
            expect_cnt += 1;
            let ret = actual_iter.try_next().await?;
            match ret {
                None => {
                    break;
                }
                Some(kv_actual) => {
                    actual_cnt += 1;
                    assert_eq!(kv_expect.0, kv_actual.0, "Key mismatch");
                    assert_eq!(kv_expect.1, kv_actual.1, "Value mismatch");
                }
            }
        }
        assert_eq!(expect_cnt, actual_cnt);
    }
    Ok(())
}

struct StorageMetrics {
    pub hummock_metrics: Arc<HummockMetrics>,
    pub state_store_metrics: Arc<HummockStateStoreMetrics>,
    pub object_store_metrics: Arc<ObjectStoreMetrics>,
    pub storage_metrics: Arc<MonitoredStorageMetrics>,
    pub compactor_metrics: Arc<CompactorMetrics>,
}

pub async fn create_hummock_store_with_metrics(
    meta_client: &MetaClient,
    storage_opts: Arc<StorageOpts>,
    opts: &CompactionTestOpts,
) -> anyhow::Result<MonitoredStateStore<HummockStorage>> {
    let metrics = StorageMetrics {
        hummock_metrics: Arc::new(HummockMetrics::unused()),
        state_store_metrics: Arc::new(HummockStateStoreMetrics::unused()),
        object_store_metrics: Arc::new(ObjectStoreMetrics::unused()),
        storage_metrics: Arc::new(MonitoredStorageMetrics::unused()),
        compactor_metrics: Arc::new(CompactorMetrics::unused()),
    };

    let state_store_impl = StateStoreImpl::new(
        &opts.state_store,
        Role::None,
        storage_opts,
        Arc::new(MonitoredHummockMetaClient::new(
            meta_client.clone(),
            metrics.hummock_metrics.clone(),
        )),
        metrics.state_store_metrics.clone(),
        metrics.object_store_metrics.clone(),
        metrics.storage_metrics.clone(),
        metrics.compactor_metrics.clone(),
        None,
        true,
    )
    .await?;

    if let Some(hummock_state_store) = state_store_impl.as_hummock() {
        Ok(hummock_state_store
            .clone()
            .monitored(metrics.storage_metrics))
    } else {
        Err(anyhow!("only Hummock state store is supported!"))
    }
}
