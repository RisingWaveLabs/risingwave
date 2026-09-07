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

use risingwave_batch::monitor::{
    GLOBAL_BATCH_EXECUTOR_METRICS, GLOBAL_BATCH_MANAGER_METRICS, GLOBAL_BATCH_SPILL_METRICS,
};
use risingwave_batch::rpc::service::task_service::BatchServiceImpl;
use risingwave_batch::spill::spill_op::SpillOp;
use risingwave_batch::task::{BatchEnvironment, BatchManager};
use risingwave_common::config::{
    AsyncStackTraceOption, MAX_CONNECTION_WINDOW_SIZE, MetricLevel, STREAM_WINDOW_SIZE,
    StorageMemoryConfig, load_config,
};
use risingwave_common::license::LicenseManager;
use risingwave_common::lru::init_global_sequencer_args;
use risingwave_common::monitor::{RouterExt, TcpConfig};
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::system_param::local_manager::LocalSystemParamsManager;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::telemetry::manager::TelemetryManager;
use risingwave_common::telemetry::telemetry_env_enabled;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::pretty_bytes::convert;
use risingwave_common::util::tokio_util::sync::CancellationToken;
use risingwave_common::{DATA_DIRECTORY, GIT_SHA, RW_VERSION, STATE_STORE_URL};
use risingwave_common_heap_profiling::HeapProfiler;
use risingwave_common_service::{MetricsManager, ObserverManager, TracingExtractLayer};
use risingwave_connector::source::iceberg::GLOBAL_ICEBERG_SCAN_METRICS;
use risingwave_connector::source::monitor::GLOBAL_SOURCE_METRICS;
use risingwave_dml::dml_manager::DmlManager;
use risingwave_pb::common::WorkerType;
use risingwave_pb::common::worker_node::Property;
use risingwave_pb::compute::config_service_server::ConfigServiceServer;
use risingwave_pb::configured_monitor_service_server;
use risingwave_pb::health::health_server::HealthServer;
use risingwave_pb::monitor_service::monitor_service_server::MonitorServiceServer;
use risingwave_pb::stream_service::stream_service_server::StreamServiceServer;
use risingwave_pb::task_service::batch_exchange_service_server::BatchExchangeServiceServer;
use risingwave_pb::task_service::stream_exchange_service_server::StreamExchangeServiceServer;
use risingwave_pb::task_service::task_service_server::TaskServiceServer;
use risingwave_rpc_client::{ComputeClientPool, MetaClient};
use risingwave_storage::StateStoreImpl;
use risingwave_storage::hummock::MemoryLimiter;
use risingwave_storage::hummock::compactor::{
    CompactionExecutor, CompactorContext, new_compaction_await_tree_reg_ref, start_compactor,
};
use risingwave_storage::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use risingwave_storage::hummock::utils::HummockMemoryCollector;
use risingwave_storage::monitor::{
    GLOBAL_COMPACTOR_METRICS, GLOBAL_HUMMOCK_METRICS, GLOBAL_OBJECT_STORE_METRICS,
    global_hummock_state_store_metrics, global_storage_metrics, monitor_cache,
};
use risingwave_storage::opts::StorageOpts;
use risingwave_stream::executor::monitor::global_streaming_metrics;
use risingwave_stream::task::{LocalStreamManager, StreamEnvironment};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tower::Layer;

use crate::ComputeNodeOpts;
use crate::memory::config::{
    MIN_COMPUTE_MEMORY_MB, batch_mem_limit, reserve_memory_bytes, storage_memory_config,
};
use crate::memory::manager::{MemoryManager, MemoryManagerConfig};
use crate::observer::observer_manager::ComputeObserverNode;
use crate::rpc::service::batch_exchange_service::BatchExchangeServiceImpl;
use crate::rpc::service::config_service::ConfigServiceImpl;
use crate::rpc::service::health_service::HealthServiceImpl;
use crate::rpc::service::monitor_service::{AwaitTreeMiddlewareLayer, MonitorServiceImpl};
use crate::rpc::service::stream_exchange_service::{
    GLOBAL_STREAM_EXCHANGE_SERVICE_METRICS, StreamExchangeServiceImpl,
};
use crate::rpc::service::stream_service::StreamServiceImpl;
use crate::telemetry::ComputeTelemetryCreator;
/// Bootstraps the compute-node.
///
/// Returns when the `shutdown` token is triggered.
pub async fn compute_node_serve(
    listen_addr: SocketAddr,
    advertise_addr: HostAddr,
    opts: Arc<ComputeNodeOpts>,
    shutdown: CancellationToken,
) {
    // Load the configuration.
    let config = Arc::new(load_config(&opts.config_path, &*opts));
    info!("Starting compute node",);
    info!("> config: {:?}", &*config);
    info!(
        "> debug assertions: {}",
        if cfg!(debug_assertions) { "on" } else { "off" }
    );
    info!("> version: {} ({})", RW_VERSION, GIT_SHA);

    // Initialize all the configs
    let stream_config = Arc::new(config.streaming.clone());
    let batch_config = Arc::new(config.batch.clone());

    // Initialize operator lru cache global sequencer args.
    init_global_sequencer_args(
        config
            .streaming
            .developer
            .memory_controller_sequence_tls_step,
        config
            .streaming
            .developer
            .memory_controller_sequence_tls_lag,
    );

    // Register to the cluster. We're not ready to serve until activate is called.
    let (meta_client, system_params) = MetaClient::register_new(
        opts.meta_address.clone(),
        WorkerType::ComputeNode,
        &advertise_addr,
        Property {
            parallelism: opts.parallelism as u32,
            is_streaming: opts.role.for_streaming(),
            is_serving: opts.role.for_serving(),
            internal_rpc_host_addr: "".to_owned(),
            resource_group: Some(opts.resource_group.clone()),
            is_iceberg_compactor: false,
        },
        Arc::new(config.meta.clone()),
    )
    .await;
    // TODO(shutdown): remove this as there's no need to gracefully shutdown the sub-tasks.
    let mut sub_tasks: Vec<(JoinHandle<()>, Sender<()>)> = vec![];
    sub_tasks.push(MetaClient::start_heartbeat_loop(
        meta_client.clone(),
        Duration::from_millis(config.server.heartbeat_interval_ms as u64),
    ));

    let state_store_url = system_params.state_store();
    let state_store_url = state_store_url.expose();

    let data_directory = system_params.data_directory();

    let embedded_compactor_enabled =
        embedded_compactor_enabled(state_store_url, config.storage.disable_remote_compactor);

    let (reserved_memory_bytes, non_reserved_memory_bytes) = reserve_memory_bytes(&opts);
    let storage_memory_config = storage_memory_config(
        non_reserved_memory_bytes,
        embedded_compactor_enabled,
        &config.storage,
        !opts.role.for_streaming(),
    );

    let storage_memory_bytes = total_storage_memory_limit_bytes(&storage_memory_config);
    let compute_memory_bytes = validate_compute_node_memory_config(
        opts.total_memory_bytes,
        reserved_memory_bytes,
        storage_memory_bytes,
    );
    print_memory_config(
        opts.total_memory_bytes,
        compute_memory_bytes,
        storage_memory_bytes,
        &storage_memory_config,
        embedded_compactor_enabled,
        reserved_memory_bytes,
    );

    let storage_opts = Arc::new(StorageOpts::from((
        &*config,
        &system_params,
        &storage_memory_config,
    )));

    let worker_id = meta_client.worker_id();
    info!("Assigned worker node id {}", worker_id);

    // Initialize the metrics subsystem.
    risingwave_connector::init_connector_metrics(config.server.metrics_level);
    let source_metrics = Arc::new(GLOBAL_SOURCE_METRICS.clone());
    let hummock_metrics = Arc::new(GLOBAL_HUMMOCK_METRICS.clone());
    let streaming_metrics = Arc::new(global_streaming_metrics(config.server.metrics_level));
    let batch_executor_metrics = Arc::new(GLOBAL_BATCH_EXECUTOR_METRICS.clone());
    let batch_manager_metrics = Arc::new(GLOBAL_BATCH_MANAGER_METRICS.clone());
    let stream_exchange_srv_metrics = Arc::new(GLOBAL_STREAM_EXCHANGE_SERVICE_METRICS.clone());
    let batch_spill_metrics = Arc::new(GLOBAL_BATCH_SPILL_METRICS.clone());
    let iceberg_scan_metrics = Arc::new(GLOBAL_ICEBERG_SCAN_METRICS.clone());

    // Initialize state store.
    let state_store_metrics = Arc::new(global_hummock_state_store_metrics(
        config.server.metrics_level,
    ));
    let object_store_metrics = Arc::new(GLOBAL_OBJECT_STORE_METRICS.clone());
    let storage_metrics = Arc::new(global_storage_metrics(config.server.metrics_level));
    let compactor_metrics = Arc::new(GLOBAL_COMPACTOR_METRICS.clone());
    let hummock_meta_client = Arc::new(MonitoredHummockMetaClient::new(
        meta_client.clone(),
        hummock_metrics.clone(),
    ));

    let await_tree_config = match &config.streaming.async_stack_trace {
        AsyncStackTraceOption::Off => None,
        c => await_tree::ConfigBuilder::default()
            .verbose(c.is_verbose().unwrap())
            .build()
            .ok(),
    };
    // Store the state_store_url in a static OnceLock for later use in JNI crate
    // Check the return value and if the variable is set, assert that the value is the same.
    if let Err(existing_url) = STATE_STORE_URL.set(state_store_url.to_owned()) {
        assert_eq!(
            existing_url, state_store_url,
            "STATE_STORE_URL already set with different value"
        );
    }

    // Store the data_directory in a static OnceLock for later use in JNI crate
    // To be extra safe, check the return value and if the variable is set, assert that the value is the same
    if let Err(existing_dir) = DATA_DIRECTORY.set(data_directory.to_owned()) {
        assert_eq!(
            existing_dir, data_directory,
            "DATA_DIRECTORY already set with different value"
        );
    }
    LicenseManager::get().refresh(system_params.license_key());
    let state_store = Box::pin(StateStoreImpl::new(
        state_store_url,
        opts.role,
        storage_opts.clone(),
        hummock_meta_client.clone(),
        state_store_metrics.clone(),
        object_store_metrics,
        storage_metrics.clone(),
        compactor_metrics.clone(),
        await_tree_config.clone(),
        system_params.use_new_object_prefix_strategy(),
    ))
    .await
    .unwrap();

    LocalSecretManager::init(
        opts.temp_secret_file_dir.clone(),
        meta_client.cluster_id().to_owned(),
        worker_id,
    );

    // Initialize observer manager.
    let batch_client_pool = Arc::new(ComputeClientPool::new(
        config.batch_exchange_connection_pool_size(),
        config.batch.developer.compute_client_config.clone(),
    ));
    let system_params_manager = Arc::new(LocalSystemParamsManager::new(system_params.clone()));
    let compute_observer_node =
        ComputeObserverNode::new(system_params_manager.clone(), batch_client_pool.clone());
    let observer_manager =
        ObserverManager::new_with_meta_client(meta_client.clone(), compute_observer_node).await;
    observer_manager.start().await;

    if let Some(storage) = state_store.as_hummock() {
        if embedded_compactor_enabled {
            tracing::info!("start embedded compactor");
            let memory_limiter = Arc::new(MemoryLimiter::new(
                storage_opts.compactor_memory_limit_mb as u64 * 1024 * 1024 / 2,
            ));

            let compaction_executor = Arc::new(CompactionExecutor::new(Some(1)));
            let compactor_context = CompactorContext {
                storage_opts,
                sstable_store: storage.sstable_store(),
                compactor_metrics: compactor_metrics.clone(),
                is_share_buffer_compact: false,
                compaction_executor,
                memory_limiter,

                task_progress_manager: Default::default(),
                await_tree_reg: await_tree_config
                    .clone()
                    .map(new_compaction_await_tree_reg_ref),
            };

            let (handle, shutdown_sender) = start_compactor(
                compactor_context,
                hummock_meta_client.clone(),
                storage.object_id_manager().clone(),
                storage.compaction_catalog_manager_ref(),
            );
            sub_tasks.push((handle, shutdown_sender));
        }
        let flush_limiter = storage.get_memory_limiter();
        let memory_collector = Arc::new(HummockMemoryCollector::new(
            storage.sstable_store(),
            flush_limiter,
            storage_memory_config,
        ));
        monitor_cache(memory_collector);
        let backup_reader = storage.backup_reader();
        let system_params_mgr = system_params_manager.clone();
        tokio::spawn(async move {
            backup_reader
                .watch_config_change(system_params_mgr.watch_params())
                .await;
        });
    }

    // Initialize the managers.
    let batch_mgr = Arc::new(BatchManager::new(
        config.batch.clone(),
        batch_manager_metrics,
        batch_mem_limit(compute_memory_bytes, opts.role.for_serving()),
        await_tree_config.clone(),
    ));

    let target_memory = if let Some(v) = opts.memory_manager_target_bytes {
        v
    } else {
        compute_memory_bytes + storage_memory_bytes
    };

    let memory_mgr = MemoryManager::new(MemoryManagerConfig {
        target_memory,
        threshold_aggressive: config
            .streaming
            .developer
            .memory_controller_threshold_aggressive,
        threshold_graceful: config
            .streaming
            .developer
            .memory_controller_threshold_graceful,
        threshold_stable: config
            .streaming
            .developer
            .memory_controller_threshold_stable,
        eviction_factor_stable: config
            .streaming
            .developer
            .memory_controller_eviction_factor_stable,
        eviction_factor_graceful: config
            .streaming
            .developer
            .memory_controller_eviction_factor_graceful,
        eviction_factor_aggressive: config
            .streaming
            .developer
            .memory_controller_eviction_factor_aggressive,
        metrics: streaming_metrics.clone(),
    });

    // Run a background memory manager
    tokio::spawn(
        memory_mgr.clone().run(Duration::from_millis(
            config
                .streaming
                .developer
                .memory_controller_update_interval_ms as _,
        )),
    );

    let heap_profiler = HeapProfiler::new(
        opts.total_memory_bytes,
        config.server.heap_profiling.clone(),
    );
    // Run a background heap profiler
    heap_profiler.start();

    let dml_mgr = Arc::new(DmlManager::new(
        worker_id,
        config.streaming.developer.dml_channel_initial_permits,
    ));

    // Initialize batch environment.
    let batch_env = BatchEnvironment::new(
        batch_mgr.clone(),
        advertise_addr.clone(),
        batch_config,
        worker_id,
        state_store.clone(),
        batch_executor_metrics.clone(),
        batch_client_pool,
        dml_mgr.clone(),
        source_metrics.clone(),
        batch_spill_metrics.clone(),
        iceberg_scan_metrics.clone(),
        config.server.metrics_level,
    );

    // Initialize the streaming environment.
    let stream_client_pool = Arc::new(ComputeClientPool::new(
        config.streaming_exchange_connection_pool_size(),
        config.streaming.developer.compute_client_config.clone(),
    ));
    let stream_env = StreamEnvironment::new(
        advertise_addr.clone(),
        stream_config,
        worker_id,
        state_store.clone(),
        dml_mgr,
        system_params_manager.clone(),
        source_metrics,
        meta_client.clone(),
        stream_client_pool,
    );

    let stream_mgr = LocalStreamManager::new(
        stream_env.clone(),
        streaming_metrics.clone(),
        await_tree_config.clone(),
        memory_mgr.get_watermark_sequence(),
    );

    // Boot the runtime gRPC services.
    let batch_srv = BatchServiceImpl::new(batch_mgr.clone(), batch_env);
    let batch_exchange_srv = BatchExchangeServiceImpl::new(batch_mgr.clone());
    let stream_exchange_srv =
        StreamExchangeServiceImpl::new(stream_mgr.clone(), stream_exchange_srv_metrics);
    let stream_srv = StreamServiceImpl::new(stream_mgr.clone(), stream_env.clone());
    let (meta_cache, block_cache, hummock_storage) = if let Some(hummock) = state_store.as_hummock()
    {
        (
            Some(hummock.sstable_store().meta_cache().clone()),
            Some(hummock.sstable_store().block_cache().clone()),
            Some(hummock.clone()),
        )
    } else {
        (None, None, None)
    };
    let monitor_srv = MonitorServiceImpl::new(
        stream_mgr.clone(),
        batch_mgr.clone(),
        config.server.clone(),
        meta_cache.clone(),
        block_cache.clone(),
        hummock_storage,
    );
    let config_srv = ConfigServiceImpl::new(
        batch_mgr.clone(),
        stream_mgr.clone(),
        meta_cache,
        block_cache,
    );
    let health_srv = HealthServiceImpl::new();

    let telemetry_manager = TelemetryManager::new(
        Arc::new(meta_client.clone()),
        Arc::new(ComputeTelemetryCreator::new()),
    );

    // if the toml config file or env variable disables telemetry, do not watch system params change
    // because if any of configs disable telemetry, we should never start it
    if config.server.telemetry_enabled && telemetry_env_enabled() {
        sub_tasks.push(telemetry_manager.start().await);
    } else {
        tracing::info!("Telemetry didn't start due to config");
    }

    // Clean up the spill directory.
    #[cfg(not(madsim))]
    if config.batch.enable_spill {
        SpillOp::clean_spill_directory().await.unwrap();
    }

    let server = tonic::transport::Server::builder()
        .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
        .initial_stream_window_size(STREAM_WINDOW_SIZE)
        .http2_max_pending_accept_reset_streams(Some(config.server.grpc_max_reset_stream as usize))
        .layer(TracingExtractLayer::new())
        // XXX: unlimit the max message size to allow arbitrary large SQL input.
        .add_service({
            let await_tree_reg = batch_mgr.await_tree_reg().cloned();
            let srv = TaskServiceServer::new(batch_srv).max_decoding_message_size(usize::MAX);
            #[cfg(madsim)]
            {
                srv
            }
            #[cfg(not(madsim))]
            {
                AwaitTreeMiddlewareLayer::new_optional(await_tree_reg).layer(srv)
            }
        })
        .add_service(
            BatchExchangeServiceServer::new(batch_exchange_srv)
                .max_decoding_message_size(usize::MAX),
        )
        .add_service(
            StreamExchangeServiceServer::new(stream_exchange_srv)
                .max_decoding_message_size(usize::MAX),
        )
        .add_service({
            let await_tree_reg = stream_srv.mgr.await_tree_reg().cloned();
            let srv = StreamServiceServer::new(stream_srv).max_decoding_message_size(usize::MAX);
            #[cfg(madsim)]
            {
                srv
            }
            #[cfg(not(madsim))]
            {
                AwaitTreeMiddlewareLayer::new_optional(await_tree_reg).layer(srv)
            }
        })
        .add_service(configured_monitor_service_server(
            MonitorServiceServer::new(monitor_srv),
        ))
        .add_service(ConfigServiceServer::new(config_srv))
        .add_service(HealthServer::new(health_srv))
        .monitored_serve_with_shutdown(
            listen_addr,
            "grpc-compute-node-service",
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

    // Unregister from the meta service, then...
    // - batch queries will not be scheduled to this compute node,
    // - streaming actors will not be scheduled to this compute node after next recovery.
    meta_client.try_unregister().await;
    // Shutdown the streaming manager.
    let _ = stream_mgr.shutdown().await;

    // NOTE(shutdown): We can't simply join the tonic server here because it only returns when all
    // existing connections are closed, while we have long-running streaming calls that never
    // close. From the other side, there's also no need to gracefully shutdown them if we have
    // unregistered from the meta service.
}

/// Check whether the compute node has enough memory to perform computing tasks. Apart from storage,
/// it is recommended to reserve at least `MIN_COMPUTE_MEMORY_MB` for computing and
/// `SYSTEM_RESERVED_MEMORY_PROPORTION` of total memory for other system usage. If the requirement
/// is not met, we will print out a warning and enforce the memory used for computing tasks as
/// `MIN_COMPUTE_MEMORY_MB`.
fn validate_compute_node_memory_config(
    cn_total_memory_bytes: usize,
    reserved_memory_bytes: usize,
    storage_memory_bytes: usize,
) -> usize {
    if storage_memory_bytes > cn_total_memory_bytes {
        tracing::warn!(
            "The storage memory exceeds the total compute node memory:\nTotal compute node memory: {}\nStorage memory: {}\nWe recommend that at least 4 GiB memory should be reserved for RisingWave. Please increase the total compute node memory or decrease the storage memory in configurations.",
            convert(cn_total_memory_bytes as _),
            convert(storage_memory_bytes as _)
        );
        MIN_COMPUTE_MEMORY_MB << 20
    } else if storage_memory_bytes + (MIN_COMPUTE_MEMORY_MB << 20) + reserved_memory_bytes
        >= cn_total_memory_bytes
    {
        tracing::warn!(
            "Not enough memory remains for compute workloads and other system usage:\nTotal compute node memory: {}\nStorage memory: {}\nWe recommend reserving at least 4 GiB of memory for RisingWave. Please increase the total compute node memory or decrease the configured storage memory.",
            convert(cn_total_memory_bytes as _),
            convert(storage_memory_bytes as _)
        );
        MIN_COMPUTE_MEMORY_MB << 20
    } else {
        cn_total_memory_bytes - storage_memory_bytes - reserved_memory_bytes
    }
}

/// The maximal memory that storage components may use based on the configurations in bytes. Note
/// that this is the total storage memory for one compute node instead of the whole cluster.
fn total_storage_memory_limit_bytes(storage_memory_config: &StorageMemoryConfig) -> usize {
    let total_storage_memory_mb = storage_memory_config.block_cache_capacity_mb
        + storage_memory_config.meta_cache_capacity_mb
        + storage_memory_config.shared_buffer_capacity_mb
        + storage_memory_config.compactor_memory_limit_mb;
    total_storage_memory_mb << 20
}

/// Checks whether an embedded compactor starts with a compute node.
fn embedded_compactor_enabled(state_store_url: &str, disable_remote_compactor: bool) -> bool {
    // Always start an embedded compactor if the state store is in-memory.
    state_store_url.starts_with("hummock+memory")
        || state_store_url.starts_with("hummock+disk")
        || disable_remote_compactor
}

// Print out the memory outline of the compute node.
fn print_memory_config(
    cn_total_memory_bytes: usize,
    compute_memory_bytes: usize,
    storage_memory_bytes: usize,
    storage_memory_config: &StorageMemoryConfig,
    embedded_compactor_enabled: bool,
    reserved_memory_bytes: usize,
) {
    let memory_config = format!(
        "Memory outline:\n\
        > total_memory: {}\n\
        >     storage_memory: {}\n\
        >         block_cache_capacity: {}\n\
        >         meta_cache_capacity: {}\n\
        >         shared_buffer_capacity: {}\n\
        >         compactor_memory_limit: {}\n\
        >     compute_memory: {}\n\
        >     reserved_memory: {}",
        convert(cn_total_memory_bytes as _),
        convert(storage_memory_bytes as _),
        convert((storage_memory_config.block_cache_capacity_mb << 20) as _),
        convert((storage_memory_config.meta_cache_capacity_mb << 20) as _),
        convert((storage_memory_config.shared_buffer_capacity_mb << 20) as _),
        if embedded_compactor_enabled {
            convert((storage_memory_config.compactor_memory_limit_mb << 20) as _)
        } else {
            "Not enabled".to_owned()
        },
        convert(compute_memory_bytes as _),
        convert(reserved_memory_bytes as _),
    );
    info!("{}", memory_config);
}
