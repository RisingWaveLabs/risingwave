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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use risingwave_common::config::{
    AsyncStackTraceOption, MetricLevel, RwConfig, extract_storage_memory_config, load_config,
};
use risingwave_common::monitor::{RouterExt, TcpConfig};
use risingwave_common::system_param::local_manager::LocalSystemParamsManager;
use risingwave_common::system_param::reader::{SystemParamsRead, SystemParamsReader};
use risingwave_common::telemetry::manager::TelemetryManager;
use risingwave_common::telemetry::telemetry_env_enabled;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::resource_util::memory::system_memory_available_bytes;
use risingwave_common::util::tokio_util::sync::CancellationToken;
use risingwave_common::{GIT_SHA, RW_VERSION};
use risingwave_common_heap_profiling::HeapProfiler;
use risingwave_common_service::{MetricsManager, ObserverManager};
use risingwave_object_store::object::build_remote_object_store;
use risingwave_object_store::object::object_metrics::GLOBAL_OBJECT_STORE_METRICS;
use risingwave_pb::common::WorkerType;
use risingwave_pb::common::worker_node::Property;
use risingwave_pb::compactor::compactor_service_server::CompactorServiceServer;
use risingwave_pb::configured_monitor_service_server;
use risingwave_pb::monitor_service::monitor_service_server::MonitorServiceServer;
use risingwave_rpc_client::{GrpcCompactorProxyClient, MetaClient};
use risingwave_storage::compaction_catalog_manager::{
    CompactionCatalogManager, RemoteTableAccessor,
};
use risingwave_storage::hummock::compactor::{
    CompactionAwaitTreeRegRef, CompactionExecutor, CompactorContext,
    new_compaction_await_tree_reg_ref,
};
use risingwave_storage::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use risingwave_storage::hummock::utils::HummockMemoryCollector;
use risingwave_storage::hummock::{MemoryLimiter, ObjectIdManager, SstableStore};
use risingwave_storage::monitor::{
    CompactorMetrics, GLOBAL_COMPACTOR_METRICS, GLOBAL_HUMMOCK_METRICS, monitor_cache,
};
use risingwave_storage::opts::StorageOpts;
use tokio::sync::mpsc;
use tracing::info;

use super::compactor_observer::observer_manager::CompactorObserverNode;
use crate::rpc::{CompactorServiceImpl, MonitorServiceImpl};
use crate::telemetry::CompactorTelemetryCreator;
use crate::{
    CompactorMode, CompactorOpts, default_rpc_max_decoding_message_size_bytes,
    default_rpc_max_encoding_message_size_bytes,
};

pub async fn prepare_start_parameters(
    compactor_opts: &CompactorOpts,
    config: RwConfig,
    system_params_reader: SystemParamsReader,
) -> (
    Arc<SstableStore>,
    Arc<MemoryLimiter>,
    HeapProfiler,
    Option<CompactionAwaitTreeRegRef>,
    Arc<StorageOpts>,
    Arc<CompactorMetrics>,
) {
    // Boot compactor
    let object_metrics = Arc::new(GLOBAL_OBJECT_STORE_METRICS.clone());
    let compactor_metrics = Arc::new(GLOBAL_COMPACTOR_METRICS.clone());

    let state_store_url = system_params_reader.state_store();
    let state_store_url = state_store_url.expose();

    let storage_memory_config = extract_storage_memory_config(&config);
    let storage_opts: Arc<StorageOpts> = Arc::new(StorageOpts::from((
        &config,
        &system_params_reader,
        &storage_memory_config,
    )));
    let non_reserved_memory_bytes = (compactor_opts.compactor_total_memory_bytes as f64
        * config.storage.compactor_memory_available_proportion)
        as usize;
    let meta_cache_capacity_bytes = compactor_opts.compactor_meta_cache_memory_bytes;
    let mut compactor_memory_limit_bytes = match config.storage.compactor_memory_limit_mb {
        Some(compactor_memory_limit_mb) => compactor_memory_limit_mb * (1 << 20),
        None => non_reserved_memory_bytes,
    };

    compactor_memory_limit_bytes = compactor_memory_limit_bytes.checked_sub(compactor_opts.compactor_meta_cache_memory_bytes).unwrap_or_else(|| {
        panic!(
            "compactor_memory_limit_bytes{} is too small to hold compactor_meta_cache_memory_bytes {}",
            compactor_memory_limit_bytes,
            meta_cache_capacity_bytes
        );
    });

    tracing::info!(
        "Compactor non_reserved_memory_bytes {} meta_cache_capacity_bytes {} compactor_memory_limit_bytes {} sstable_size_bytes {} block_size_bytes {}",
        non_reserved_memory_bytes,
        meta_cache_capacity_bytes,
        compactor_memory_limit_bytes,
        storage_opts.sstable_size_mb * (1 << 20),
        storage_opts.block_size_kb * (1 << 10),
    );

    // check memory config
    {
        // This is a similar logic to SstableBuilder memory detection, to ensure that we can find
        // configuration problems as quickly as possible
        let min_compactor_memory_limit_bytes = (storage_opts.sstable_size_mb * (1 << 20)
            + storage_opts.block_size_kb * (1 << 10))
            as u64;

        assert!(compactor_memory_limit_bytes > min_compactor_memory_limit_bytes as usize * 2);
    }

    let object_store = build_remote_object_store(
        state_store_url
            .strip_prefix("hummock+")
            .expect("object store must be hummock for compactor server"),
        object_metrics,
        "Hummock",
        Arc::new(config.storage.object_store.clone()),
        &storage_opts.data_directory,
    )
    .await;

    let object_store = Arc::new(object_store);
    let sstable_store = Arc::new(
        SstableStore::for_compactor(
            object_store,
            storage_opts.data_directory.clone(),
            0,
            meta_cache_capacity_bytes,
            system_params_reader.use_new_object_prefix_strategy(),
        )
        .await
        // FIXME(MrCroxx): Handle this error.
        .unwrap(),
    );

    let memory_limiter = Arc::new(MemoryLimiter::new(compactor_memory_limit_bytes as u64));
    let storage_memory_config = extract_storage_memory_config(&config);
    let memory_collector = Arc::new(HummockMemoryCollector::new(
        sstable_store.clone(),
        memory_limiter.clone(),
        storage_memory_config,
    ));

    let heap_profiler = HeapProfiler::new(
        system_memory_available_bytes(),
        config.server.heap_profiling.clone(),
    );

    monitor_cache(memory_collector);

    let await_tree_config = match &config.streaming.async_stack_trace {
        AsyncStackTraceOption::Off => None,
        c => await_tree::ConfigBuilder::default()
            .verbose(c.is_verbose().unwrap())
            .build()
            .ok(),
    };
    let await_tree_reg = await_tree_config.map(new_compaction_await_tree_reg_ref);

    (
        sstable_store,
        memory_limiter,
        heap_profiler,
        await_tree_reg,
        storage_opts,
        compactor_metrics,
    )
}

/// Resolves the memory budget dedicated to Iceberg compaction.
fn resolve_iceberg_compaction_memory_budget(
    configured_limit_mb: Option<usize>,
    compactor_total_memory_bytes: usize,
    available_proportion: f64,
) -> usize {
    const MB: usize = 1 << 20;
    let budget = match configured_limit_mb {
        Some(limit_mb) => limit_mb
            .checked_mul(MB)
            .expect("Iceberg compaction memory limit overflows usize"),
        None => (compactor_total_memory_bytes as f64 * available_proportion) as usize,
    };
    assert!(
        budget > 0,
        "Iceberg compaction memory limit must be positive"
    );
    budget
}

/// Fetches and runs compaction tasks.
///
/// Returns when the `shutdown` token is triggered.
pub async fn compactor_serve(
    listen_addr: SocketAddr,
    advertise_addr: HostAddr,
    opts: CompactorOpts,
    shutdown: CancellationToken,
    compactor_mode: CompactorMode,
) {
    let config = load_config(&opts.config_path, &opts);
    info!("Starting compactor node",);
    info!("> config: {:?}", config);
    info!(
        "> debug assertions: {}",
        if cfg!(debug_assertions) { "on" } else { "off" }
    );
    info!("> version: {} ({})", RW_VERSION, GIT_SHA);

    let is_iceberg_compactor = matches!(
        compactor_mode,
        CompactorMode::DedicatedIceberg | CompactorMode::SharedIceberg
    );

    let compaction_executor = Arc::new(CompactionExecutor::new(
        opts.compaction_worker_threads_number,
    ));

    let max_task_parallelism: u32 = (compaction_executor.worker_num() as f32
        * config.storage.compactor_max_task_multiplier)
        .ceil() as u32;

    // Register to the cluster.
    let (meta_client, system_params_reader) = MetaClient::register_new(
        opts.meta_address.clone(),
        WorkerType::Compactor,
        &advertise_addr,
        Property {
            is_iceberg_compactor,
            parallelism: max_task_parallelism,
            ..Default::default()
        },
        Arc::new(config.meta.clone()),
    )
    .await;

    info!("Assigned compactor id {}", meta_client.worker_id());

    let hummock_metrics = Arc::new(GLOBAL_HUMMOCK_METRICS.clone());

    let hummock_meta_client = Arc::new(MonitoredHummockMetaClient::new(
        meta_client.clone(),
        hummock_metrics.clone(),
    ));

    let (
        sstable_store,
        memory_limiter,
        heap_profiler,
        await_tree_reg,
        storage_opts,
        compactor_metrics,
    ) = Box::pin(prepare_start_parameters(
        &opts,
        config.clone(),
        system_params_reader.clone(),
    ))
    .await;
    let iceberg_memory_budget_bytes = matches!(compactor_mode, CompactorMode::DedicatedIceberg)
        .then(|| {
            resolve_iceberg_compaction_memory_budget(
                config.storage.iceberg_compaction_memory_limit_mb,
                opts.compactor_total_memory_bytes,
                config.storage.compactor_memory_available_proportion,
            )
        });

    let compaction_catalog_manager_ref = Arc::new(CompactionCatalogManager::new(Box::new(
        RemoteTableAccessor::new(meta_client.clone()),
    )));

    let system_params_manager = Arc::new(LocalSystemParamsManager::new(system_params_reader));
    let compactor_observer_node = CompactorObserverNode::new(
        compaction_catalog_manager_ref.clone(),
        system_params_manager.clone(),
    );
    let observer_manager =
        ObserverManager::new_with_meta_client(meta_client.clone(), compactor_observer_node).await;

    // Run a background heap profiler
    heap_profiler.start();

    // use half of limit because any memory which would hold in meta-cache will be allocate by
    // limited at first.
    let _observer_join_handle = observer_manager.start().await;

    let object_id_manager = Arc::new(ObjectIdManager::new(
        hummock_meta_client.clone(),
        storage_opts.sstable_id_remote_fetch_number,
    ));

    let compactor_context = CompactorContext {
        storage_opts,
        sstable_store: sstable_store.clone(),
        compactor_metrics,
        is_share_buffer_compact: false,
        compaction_executor,
        memory_limiter,
        task_progress_manager: Default::default(),
        await_tree_reg: await_tree_reg.clone(),
    };

    // TODO(shutdown): don't collect sub-tasks as there's no need to gracefully shutdown them.
    let mut sub_tasks = vec![
        MetaClient::start_heartbeat_loop(
            meta_client.clone(),
            Duration::from_millis(config.server.heartbeat_interval_ms as u64),
        ),
        match compactor_mode {
            CompactorMode::Dedicated => risingwave_storage::hummock::compactor::start_compactor(
                compactor_context.clone(),
                hummock_meta_client.clone(),
                object_id_manager.clone(),
                compaction_catalog_manager_ref,
            ),
            CompactorMode::Shared => unreachable!(),
            CompactorMode::DedicatedIceberg => {
                risingwave_storage::hummock::compactor::start_iceberg_compactor(
                    compactor_context.clone(),
                    hummock_meta_client.clone(),
                    iceberg_memory_budget_bytes
                        .expect("Iceberg memory budget must be resolved for dedicated startup"),
                )
            }
            CompactorMode::SharedIceberg => unreachable!(),
        },
    ];

    let telemetry_manager = TelemetryManager::new(
        Arc::new(meta_client.clone()),
        Arc::new(CompactorTelemetryCreator::new()),
    );
    // if the toml config file or env variable disables telemetry, do not watch system params change
    // because if any of configs disable telemetry, we should never start it
    if config.server.telemetry_enabled && telemetry_env_enabled() {
        sub_tasks.push(telemetry_manager.start().await);
    } else {
        tracing::info!("Telemetry didn't start due to config");
    }

    let compactor_srv = CompactorServiceImpl::default();
    let monitor_srv = MonitorServiceImpl::new(await_tree_reg, config.server.clone());
    let server = tonic::transport::Server::builder()
        .add_service(CompactorServiceServer::new(compactor_srv))
        .add_service(configured_monitor_service_server(
            MonitorServiceServer::new(monitor_srv),
        ))
        .monitored_serve_with_shutdown(
            listen_addr,
            "grpc-compactor-node-service",
            TcpConfig {
                tcp_nodelay: true,
                keepalive_duration: None,
            },
            shutdown.clone().cancelled_owned(),
        );
    let _server_handle = tokio::spawn(server);

    // Boot metrics service.
    if config.server.metrics_level > MetricLevel::Disabled {
        MetricsManager::boot_metrics_service(opts.prometheus_listener_addr.clone());
    }

    // All set, let the meta service know we're ready.
    meta_client.activate(&advertise_addr).await.unwrap();

    // Wait for the shutdown signal.
    shutdown.cancelled().await;
    // Run shutdown logic.
    meta_client.try_unregister().await;
}

/// Fetches and runs compaction tasks under shared mode.
///
/// Returns when the `shutdown` token is triggered.
pub async fn shared_compactor_serve(
    listen_addr: SocketAddr,
    opts: CompactorOpts,
    shutdown: CancellationToken,
) {
    let config = load_config(&opts.config_path, &opts);
    info!("Starting shared compactor node",);
    info!("> config: {:?}", config);
    info!(
        "> debug assertions: {}",
        if cfg!(debug_assertions) { "on" } else { "off" }
    );
    info!("> version: {} ({})", RW_VERSION, GIT_SHA);

    let grpc_proxy_client = GrpcCompactorProxyClient::new(opts.proxy_rpc_endpoint.clone()).await;
    let system_params_response = grpc_proxy_client
        .get_system_params()
        .await
        .expect("Fail to get system params, the compactor pod cannot be started.");
    let system_params = system_params_response.into_inner().params.unwrap();

    let (
        sstable_store,
        memory_limiter,
        heap_profiler,
        await_tree_reg,
        storage_opts,
        compactor_metrics,
    ) = Box::pin(prepare_start_parameters(
        &opts,
        config.clone(),
        system_params.into(),
    ))
    .await;
    let (sender, receiver) = mpsc::unbounded_channel();
    let compactor_srv: CompactorServiceImpl = CompactorServiceImpl::new(sender);

    let monitor_srv = MonitorServiceImpl::new(await_tree_reg.clone(), config.server.clone());

    // Run a background heap profiler
    heap_profiler.start();

    let compaction_executor = Arc::new(CompactionExecutor::new(
        opts.compaction_worker_threads_number,
    ));
    let compactor_context = CompactorContext {
        storage_opts,
        sstable_store,
        compactor_metrics,
        is_share_buffer_compact: false,
        compaction_executor,
        memory_limiter,
        task_progress_manager: Default::default(),
        await_tree_reg,
    };

    // TODO(shutdown): don't collect there's no need to gracefully shutdown them.
    // Hold the join handle and tx to keep the compactor running.
    let _compactor_handle = risingwave_storage::hummock::compactor::start_shared_compactor(
        grpc_proxy_client,
        receiver,
        compactor_context,
    );

    let rpc_max_encoding_message_size_bytes = opts
        .rpc_max_encoding_message_size_bytes
        .unwrap_or(default_rpc_max_encoding_message_size_bytes());

    let rpc_max_decoding_message_size_bytes = opts
        .rpc_max_decoding_message_size_bytes
        .unwrap_or(default_rpc_max_decoding_message_size_bytes());

    let server = tonic::transport::Server::builder()
        .add_service(
            CompactorServiceServer::new(compactor_srv)
                .max_decoding_message_size(rpc_max_decoding_message_size_bytes)
                .max_encoding_message_size(rpc_max_encoding_message_size_bytes),
        )
        .add_service(configured_monitor_service_server(
            MonitorServiceServer::new(monitor_srv),
        ))
        .monitored_serve_with_shutdown(
            listen_addr,
            "grpc-compactor-node-service",
            TcpConfig {
                tcp_nodelay: true,
                keepalive_duration: None,
            },
            shutdown.clone().cancelled_owned(),
        );

    let _server_handle = tokio::spawn(server);

    // Boot metrics service.
    if config.server.metrics_level > MetricLevel::Disabled {
        MetricsManager::boot_metrics_service(opts.prometheus_listener_addr.clone());
    }

    // Wait for the shutdown signal.
    shutdown.cancelled().await;

    // TODO(shutdown): shall we notify the proxy that we are shutting down?
}
