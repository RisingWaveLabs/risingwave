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

use std::sync::Arc;
use std::time::Duration;

use otlp_embedded::TraceServiceServer;
use regex::Regex;
use risingwave_common::config::SessionInitConfig;
use risingwave_common::monitor::{RouterExt, TcpConfig};
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::telemetry::manager::TelemetryManager;
use risingwave_common::telemetry::{report_scarf_enabled, report_to_scarf, telemetry_env_enabled};
use risingwave_common::util::tokio_util::sync::CancellationToken;
use risingwave_common_service::{AwaitTreeMiddlewareLayer, MetricsManager, TracingExtractLayer};
use risingwave_meta::MetaStoreBackend;
use risingwave_meta::barrier::GlobalBarrierManager;
use risingwave_meta::controller::catalog::CatalogController;
use risingwave_meta::controller::cluster::ClusterController;
use risingwave_meta::hummock::IcebergCompactorManager;
use risingwave_meta::manager::iceberg_compaction::IcebergCompactionManager;
use risingwave_meta::manager::{META_NODE_ID, MetadataManager};
use risingwave_meta::rpc::ElectionClientRef;
use risingwave_meta::rpc::election::dummy::DummyElectionClient;
use risingwave_meta::rpc::intercept::MetricsMiddlewareLayer;
use risingwave_meta::stream::{GlobalRefreshManager, ScaleController};
use risingwave_meta_service::AddressInfo;
use risingwave_meta_service::backup_service::BackupServiceImpl;
use risingwave_meta_service::cloud_service::CloudServiceImpl;
use risingwave_meta_service::cluster_limit_service::ClusterLimitServiceImpl;
use risingwave_meta_service::cluster_service::ClusterServiceImpl;
use risingwave_meta_service::ddl_service::DdlServiceImpl;
use risingwave_meta_service::event_log_service::EventLogServiceImpl;
use risingwave_meta_service::health_service::HealthServiceImpl;
use risingwave_meta_service::heartbeat_service::HeartbeatServiceImpl;
use risingwave_meta_service::hosted_iceberg_catalog_service::HostedIcebergCatalogServiceImpl;
use risingwave_meta_service::hummock_service::HummockServiceImpl;
use risingwave_meta_service::meta_member_service::MetaMemberServiceImpl;
use risingwave_meta_service::monitor_service::MonitorServiceImpl;
use risingwave_meta_service::notification_service::NotificationServiceImpl;
use risingwave_meta_service::scale_service::ScaleServiceImpl;
use risingwave_meta_service::serving_service::ServingServiceImpl;
use risingwave_meta_service::session_config::SessionParamsServiceImpl;
use risingwave_meta_service::sink_coordination_service::SinkCoordinationServiceImpl;
use risingwave_meta_service::stream_service::StreamServiceImpl;
use risingwave_meta_service::system_params_service::SystemParamsServiceImpl;
use risingwave_meta_service::telemetry_service::TelemetryInfoServiceImpl;
use risingwave_meta_service::user_service::UserServiceImpl;
use risingwave_pb::backup_service::backup_service_server::BackupServiceServer;
use risingwave_pb::cloud_service::cloud_service_server::CloudServiceServer;
use risingwave_pb::configured_monitor_service_server;
use risingwave_pb::connector_service::sink_coordination_service_server::SinkCoordinationServiceServer;
use risingwave_pb::ddl_service::ddl_service_server::DdlServiceServer;
use risingwave_pb::health::health_server::HealthServer;
use risingwave_pb::hummock::hummock_manager_service_server::HummockManagerServiceServer;
use risingwave_pb::meta::SystemParams;
use risingwave_pb::meta::cluster_limit_service_server::ClusterLimitServiceServer;
use risingwave_pb::meta::cluster_service_server::ClusterServiceServer;
use risingwave_pb::meta::event_log_service_server::EventLogServiceServer;
use risingwave_pb::meta::heartbeat_service_server::HeartbeatServiceServer;
use risingwave_pb::meta::hosted_iceberg_catalog_service_server::HostedIcebergCatalogServiceServer;
use risingwave_pb::meta::meta_member_service_server::MetaMemberServiceServer;
use risingwave_pb::meta::notification_service_server::NotificationServiceServer;
use risingwave_pb::meta::scale_service_server::ScaleServiceServer;
use risingwave_pb::meta::serving_service_server::ServingServiceServer;
use risingwave_pb::meta::session_param_service_server::SessionParamServiceServer;
use risingwave_pb::meta::stream_manager_service_server::StreamManagerServiceServer;
use risingwave_pb::meta::system_params_service_server::SystemParamsServiceServer;
use risingwave_pb::meta::telemetry_info_service_server::TelemetryInfoServiceServer;
use risingwave_pb::monitor_service::monitor_service_server::MonitorServiceServer;
use risingwave_pb::user::user_service_server::UserServiceServer;
use sea_orm::{ConnectionTrait, DbBackend};
use thiserror_ext::AsReport;
use tokio::sync::watch;

use crate::backup_restore::BackupManager;
use crate::barrier::BarrierScheduler;
use crate::controller::SqlMetaStore;
use crate::controller::system_param::SystemParamsController;
use crate::hummock::HummockManager;
use crate::manager::iceberg_pk_index_sink::IcebergPkIndexSinkManager;
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{IdleManager, MetaOpts, MetaSrvEnv};
use crate::rpc::election::sql::{MySqlDriver, PostgresDriver, SqlBackendElectionClient};
use crate::rpc::metrics::{
    GLOBAL_META_METRICS, init_meta_metrics, start_info_monitor, start_worker_info_monitor,
};
use crate::serving::ServingVnodeMapping;
use crate::stream::{GlobalStreamManager, SourceManager};
use crate::telemetry::{MetaReportCreator, MetaTelemetryInfoFetcher};
use crate::{MetaError, MetaResult, hummock, serving};

/// Used for standalone mode checking the status of the meta service.
/// This can be easier and more accurate than checking the TCP connection.
pub mod started {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::Relaxed;

    static STARTED: AtomicBool = AtomicBool::new(false);

    /// Mark the meta service as started.
    pub(crate) fn set() {
        STARTED.store(true, Relaxed);
    }

    /// Check if the meta service has started.
    pub fn get() -> bool {
        STARTED.load(Relaxed)
    }
}

/// A wrapper around [`rpc_serve_with_store`] that dispatches different store implementations.
///
/// For the timing of returning, see [`rpc_serve_with_store`].
pub async fn rpc_serve(
    address_info: AddressInfo,
    meta_store_backend: MetaStoreBackend,
    max_cluster_heartbeat_interval: Duration,
    lease_interval_secs: u64,
    server_config: risingwave_common::config::ServerConfig,
    opts: MetaOpts,
    init_system_params: SystemParams,
    session_init: SessionInitConfig,
    shutdown: CancellationToken,
) -> MetaResult<()> {
    let meta_store_impl = SqlMetaStore::connect(meta_store_backend.clone()).await?;

    let election_client = match meta_store_backend {
        MetaStoreBackend::Mem => {
            // Use a dummy election client.
            Arc::new(DummyElectionClient::new(
                address_info.advertise_addr.clone(),
            ))
        }
        MetaStoreBackend::Sql { .. } => {
            // Init election client.
            let id = address_info.advertise_addr.clone();
            let conn = meta_store_impl.conn.clone();
            let election_client: ElectionClientRef = match conn.get_database_backend() {
                DbBackend::Sqlite => Arc::new(DummyElectionClient::new(id)),
                DbBackend::Postgres => {
                    Arc::new(SqlBackendElectionClient::new(id, PostgresDriver::new(conn)))
                }
                DbBackend::MySql => {
                    Arc::new(SqlBackendElectionClient::new(id, MySqlDriver::new(conn)))
                }
            };
            election_client.init().await?;

            election_client
        }
    };

    Box::pin(rpc_serve_with_store(
        meta_store_impl,
        election_client,
        address_info,
        max_cluster_heartbeat_interval,
        lease_interval_secs,
        server_config,
        opts,
        init_system_params,
        session_init,
        shutdown,
    ))
    .await
}

/// Bootstraps the follower or leader service based on the election status.
///
/// Returns when the `shutdown` token is triggered, or when leader status is lost, or if the leader
/// service fails to start.
pub async fn rpc_serve_with_store(
    meta_store_impl: SqlMetaStore,
    election_client: ElectionClientRef,
    address_info: AddressInfo,
    max_cluster_heartbeat_interval: Duration,
    lease_interval_secs: u64,
    server_config: risingwave_common::config::ServerConfig,
    opts: MetaOpts,
    init_system_params: SystemParams,
    session_init: SessionInitConfig,
    shutdown: CancellationToken,
) -> MetaResult<()> {
    init_meta_metrics(server_config.metrics_level);

    // TODO(shutdown): directly use cancellation token
    let (election_shutdown_tx, election_shutdown_rx) = watch::channel(());

    let election_handle = tokio::spawn({
        let shutdown = shutdown.clone();
        let election_client = election_client.clone();

        async move {
            while let Err(e) = election_client
                .run_once(lease_interval_secs as i64, election_shutdown_rx.clone())
                .await
            {
                tracing::error!(error = %e.as_report(), "an election error occurred");
            }
            // Leader lost, shutdown the service.
            shutdown.cancel();
        }
    });

    // Spawn and run the follower service if not the leader.
    // Watch the leader status and switch to the leader service when elected.
    // TODO: the branch seems to be always hit since the default value of `is_leader` is false until
    // the election is done (unless using `DummyElectionClient`).
    if !election_client.is_leader() {
        // The follower service can be shutdown separately if we're going to be the leader.
        let follower_shutdown = shutdown.child_token();

        let follower_handle = tokio::spawn(start_service_as_election_follower(
            follower_shutdown.clone(),
            address_info.clone(),
            election_client.clone(),
        ));

        // Watch and wait until we become the leader.
        let mut is_leader_watcher = election_client.subscribe();

        while !*is_leader_watcher.borrow_and_update() {
            tokio::select! {
                // External shutdown signal. Directly return without switching to leader.
                _ = shutdown.cancelled() => return Ok(()),

                res = is_leader_watcher.changed() => {
                    if res.is_err() {
                        tracing::error!("failed to receive a leader watcher update");
                    }
                }
            }
        }

        tracing::info!("elected as leader, shutting down follower services");
        follower_shutdown.cancel();
        let _ = follower_handle.await;
    }

    // Run the leader service.
    let result = start_service_as_election_leader(
        meta_store_impl,
        address_info,
        max_cluster_heartbeat_interval,
        opts,
        init_system_params,
        session_init,
        server_config,
        election_client,
        shutdown,
    )
    .await;

    // Leader service has stopped, shutdown the election service to gracefully resign.
    election_shutdown_tx.send(()).ok();
    let _ = election_handle.await;

    result
}

/// Starts all services needed for the meta follower node.
///
/// Returns when the `shutdown` token is triggered.
pub async fn start_service_as_election_follower(
    shutdown: CancellationToken,
    address_info: AddressInfo,
    election_client: ElectionClientRef,
) {
    tracing::info!("starting follower services");

    let meta_member_srv = MetaMemberServiceImpl::new(election_client);

    let health_srv = HealthServiceImpl::new();

    let server = tonic::transport::Server::builder()
        .layer(MetricsMiddlewareLayer::new(Arc::new(
            GLOBAL_META_METRICS.clone(),
        )))
        .layer(TracingExtractLayer::new())
        .add_service(MetaMemberServiceServer::new(meta_member_srv))
        .add_service(HealthServer::new(health_srv))
        .monitored_serve_with_shutdown(
            address_info.listen_addr,
            "grpc-meta-follower-service",
            TcpConfig {
                tcp_nodelay: true,
                keepalive_duration: None,
            },
            shutdown.clone().cancelled_owned(),
        );
    let server_handle = tokio::spawn(server);
    started::set();

    // Wait for the shutdown signal.
    shutdown.cancelled().await;
    // Wait for the server to shutdown. This is necessary because we may be transitioning from follower
    // to leader, and conflicts on the services must be avoided.
    let _ = server_handle.await;
}

/// Starts all services needed for the meta leader node.
///
/// Returns when the `shutdown` token is triggered, or if the service initialization fails.
pub async fn start_service_as_election_leader(
    meta_store_impl: SqlMetaStore,
    address_info: AddressInfo,
    max_cluster_heartbeat_interval: Duration,
    opts: MetaOpts,
    init_system_params: SystemParams,
    session_init: SessionInitConfig,
    server_config: risingwave_common::config::ServerConfig,
    election_client: ElectionClientRef,
    shutdown: CancellationToken,
) -> MetaResult<()> {
    tracing::info!("starting leader services");

    let env = MetaSrvEnv::new(
        opts.clone(),
        init_system_params,
        session_init,
        meta_store_impl,
    )
    .await?;
    tracing::info!("MetaSrvEnv started");
    let _ = env.may_start_watch_license_key_file()?;
    let system_params_reader = env.system_params_reader().await;

    let data_directory = system_params_reader.data_directory();
    if !is_correct_data_directory(data_directory) {
        return Err(MetaError::system_params(format!(
            "The data directory {:?} is misconfigured.
            Please use a combination of uppercase and lowercase letters and numbers, i.e. [a-z, A-Z, 0-9].
            The string cannot start or end with '/', and consecutive '/' are not allowed.
            The data directory cannot be empty and its length should not exceed 800 characters.",
            data_directory
        )));
    }

    let cluster_controller = Arc::new(
        ClusterController::new(env.clone(), max_cluster_heartbeat_interval)
            .await
            .unwrap(),
    );
    let catalog_controller = Arc::new(CatalogController::new(env.clone()).await?);
    let metadata_manager = MetadataManager::new(cluster_controller, catalog_controller);

    let serving_vnode_mapping = Arc::new(ServingVnodeMapping::default());
    let max_serving_parallelism = env
        .session_params_manager_impl_ref()
        .get_params()
        .await
        .batch_parallelism()
        .map(|p| p.get());
    serving::on_meta_start(
        env.notification_manager_ref(),
        &metadata_manager,
        serving_vnode_mapping.clone(),
        max_serving_parallelism,
    )
    .await;

    let compactor_manager = Arc::new(
        hummock::CompactorManager::with_meta(env.clone())
            .await
            .unwrap(),
    );
    tracing::info!("CompactorManager started");

    let heartbeat_srv = HeartbeatServiceImpl::new(metadata_manager.clone());
    tracing::info!("HeartbeatServiceImpl started");

    let (compactor_streams_change_tx, compactor_streams_change_rx) =
        tokio::sync::mpsc::unbounded_channel();

    let meta_metrics = Arc::new(GLOBAL_META_METRICS.clone());

    let hummock_manager = hummock::HummockManager::new(
        env.clone(),
        metadata_manager.clone(),
        meta_metrics.clone(),
        compactor_manager.clone(),
        compactor_streams_change_tx,
    )
    .await
    .unwrap();
    tracing::info!("HummockManager started");
    let object_store_media_type = hummock_manager.object_store_media_type();

    let meta_member_srv = MetaMemberServiceImpl::new(election_client.clone());

    let prometheus_client = opts.prometheus_endpoint.as_ref().map(|x| {
        use std::str::FromStr;
        prometheus_http_query::Client::from_str(x).unwrap()
    });
    let prometheus_selector = opts.prometheus_selector.unwrap_or_default();

    let trace_state = otlp_embedded::State::new(otlp_embedded::Config {
        max_length: opts.cached_traces_num,
        max_memory_usage: opts.cached_traces_memory_limit_bytes,
    });
    let trace_srv = otlp_embedded::TraceServiceImpl::new(trace_state.clone());

    let (barrier_scheduler, scheduled_barriers) =
        BarrierScheduler::new_pair(hummock_manager.clone());
    tracing::info!("BarrierScheduler started");

    // Initialize services.
    let backup_manager = BackupManager::new(
        env.clone(),
        hummock_manager.clone(),
        meta_metrics.clone(),
        system_params_reader.backup_storage_url(),
        system_params_reader.backup_storage_directory(),
    )
    .await?;
    tracing::info!("BackupManager started");

    LocalSecretManager::init(
        opts.temp_secret_file_dir,
        env.cluster_id().to_string(),
        META_NODE_ID,
    );
    tracing::info!("LocalSecretManager started");

    let notification_srv = NotificationServiceImpl::new(
        env.clone(),
        metadata_manager.clone(),
        hummock_manager.clone(),
        backup_manager.clone(),
        serving_vnode_mapping.clone(),
    )
    .await?;
    tracing::info!("NotificationServiceImpl started");

    let source_manager = Arc::new(
        SourceManager::new(
            barrier_scheduler.clone(),
            metadata_manager.clone(),
            meta_metrics.clone(),
            env.clone(),
        )
        .await?,
    );
    tracing::info!("SourceManager started");

    let (iceberg_compaction_stat_tx, iceberg_compaction_stat_rx) =
        tokio::sync::mpsc::unbounded_channel();
    let (sink_manager, shutdown_handle) = SinkCoordinatorManager::start_worker(
        env.meta_store_ref().conn.clone(),
        hummock_manager.clone(),
        metadata_manager.clone(),
        iceberg_compaction_stat_tx,
        env.await_tree_reg().clone(),
    );
    tracing::info!("SinkCoordinatorManager started");
    // TODO(shutdown): remove this as there's no need to gracefully shutdown some of these sub-tasks.
    let mut sub_tasks = vec![shutdown_handle];

    // Register before the barrier manager starts recovery. Dirty creating-job cleanup emits local
    // serving mapping deletes, which must not be dropped during bootstrap.
    sub_tasks.push(serving::start_serving_vnode_mapping_worker(
        env.notification_manager_ref(),
        metadata_manager.clone(),
        serving_vnode_mapping.clone(),
        env.session_params_manager_impl_ref(),
    ));

    let iceberg_pk_index_sink_manager =
        IcebergPkIndexSinkManager::new(env.meta_store_ref().conn.clone());
    tracing::info!("IcebergPkIndexSinkManager started");

    let iceberg_compactor_manager = Arc::new(IcebergCompactorManager::new());

    // TODO: introduce compactor event stream handler to handle iceberg compaction events.
    let (iceberg_compaction_mgr, iceberg_compactor_event_rx) = IcebergCompactionManager::build(
        env.clone(),
        metadata_manager.clone(),
        iceberg_compactor_manager.clone(),
        meta_metrics.clone(),
    );

    sub_tasks.push(IcebergCompactionManager::compaction_stat_loop(
        iceberg_compaction_mgr.clone(),
        iceberg_compaction_stat_rx,
    ));

    sub_tasks.push(IcebergCompactionManager::gc_loop(
        iceberg_compaction_mgr.clone(),
        env.opts.iceberg_gc_interval_sec,
    ));

    let refresh_scheduler_interval = Duration::from_secs(env.opts.refresh_scheduler_interval_sec);
    let (refresh_manager, refresh_handle, refresh_shutdown) = GlobalRefreshManager::start(
        metadata_manager.clone(),
        barrier_scheduler.clone(),
        &env,
        refresh_scheduler_interval,
    )
    .await?;
    sub_tasks.push((refresh_handle, refresh_shutdown));

    let scale_controller = Arc::new(ScaleController::new(
        &metadata_manager,
        source_manager.clone(),
        env.clone(),
    ));

    let (barrier_manager, join_handle, shutdown_rx) = GlobalBarrierManager::start(
        scheduled_barriers,
        env.clone(),
        metadata_manager.clone(),
        hummock_manager.clone(),
        serving_vnode_mapping.clone(),
        source_manager.clone(),
        sink_manager.clone(),
        iceberg_pk_index_sink_manager.clone(),
        iceberg_compaction_mgr.clone(),
        scale_controller.clone(),
        barrier_scheduler.clone(),
        refresh_manager.clone(),
    )
    .await;
    tracing::info!("GlobalBarrierManager started");
    sub_tasks.push((join_handle, shutdown_rx));

    {
        let source_manager = source_manager.clone();
        tokio::spawn(async move {
            source_manager.run().await.unwrap();
        });
    }

    let stream_manager = Arc::new(
        GlobalStreamManager::new(
            env.clone(),
            metadata_manager.clone(),
            barrier_scheduler.clone(),
            hummock_manager.clone(),
            source_manager.clone(),
            refresh_manager.clone(),
            iceberg_compaction_mgr.clone(),
            scale_controller.clone(),
        )
        .unwrap(),
    );

    hummock_manager
        .may_fill_backward_state_table_info()
        .await
        .unwrap();

    let ddl_srv = DdlServiceImpl::new(
        env.clone(),
        metadata_manager.clone(),
        stream_manager.clone(),
        source_manager.clone(),
        barrier_manager.clone(),
        sink_manager.clone(),
        meta_metrics.clone(),
        iceberg_compaction_mgr.clone(),
        barrier_scheduler.clone(),
        iceberg_pk_index_sink_manager.clone(),
    )
    .await;

    if env.opts.enable_legacy_table_migration {
        sub_tasks.push(ddl_srv.start_migrate_table_fragments());
    }

    let user_srv = UserServiceImpl::new(metadata_manager.clone());

    let scale_srv = ScaleServiceImpl::new(
        metadata_manager.clone(),
        stream_manager.clone(),
        barrier_manager.clone(),
        env.clone(),
    );

    let cluster_srv = ClusterServiceImpl::new(metadata_manager.clone(), barrier_manager.clone());
    let stream_srv = StreamServiceImpl::new(
        env.clone(),
        barrier_scheduler.clone(),
        barrier_manager.clone(),
        stream_manager.clone(),
        metadata_manager.clone(),
        refresh_manager.clone(),
        iceberg_compaction_mgr.clone(),
    );
    let sink_coordination_srv = SinkCoordinationServiceImpl::new(sink_manager);
    let hummock_srv = HummockServiceImpl::new(
        hummock_manager.clone(),
        metadata_manager.clone(),
        backup_manager.clone(),
        iceberg_compaction_mgr.clone(),
    );

    let health_srv = HealthServiceImpl::new();
    let backup_srv = BackupServiceImpl::new(backup_manager.clone());
    let telemetry_srv = TelemetryInfoServiceImpl::new(env.meta_store());
    let system_params_srv = SystemParamsServiceImpl::new(
        env.system_params_manager_impl_ref(),
        metadata_manager.clone(),
        env.opts.license_key_path.is_some(),
    );
    let session_params_srv = SessionParamsServiceImpl::new(env.session_params_manager_impl_ref());
    let serving_srv =
        ServingServiceImpl::new(serving_vnode_mapping.clone(), metadata_manager.clone());
    let cloud_srv = CloudServiceImpl::new();
    let event_log_srv = EventLogServiceImpl::new(env.event_log_manager_ref());
    let cluster_limit_srv = ClusterLimitServiceImpl::new(env.clone(), metadata_manager.clone());
    let hosted_iceberg_catalog_srv = HostedIcebergCatalogServiceImpl::new(env.clone());
    let monitor_srv = MonitorServiceImpl::new(
        metadata_manager.clone(),
        env.await_tree_reg().clone(),
        server_config.clone(),
    );
    let diagnose_command = Arc::new(risingwave_meta::manager::diagnose::DiagnoseCommand::new(
        metadata_manager.clone(),
        env.await_tree_reg().clone(),
        hummock_manager.clone(),
        iceberg_compaction_mgr.clone(),
        env.event_log_manager_ref(),
        prometheus_client.clone(),
        prometheus_selector.clone(),
        opts.redact_sql_option_keywords.clone(),
        env.system_params_manager_impl_ref(),
    ));

    #[cfg(not(madsim))]
    let _dashboard_task = if let Some(ref dashboard_addr) = address_info.dashboard_addr {
        use risingwave_common::config::RpcClientConfig;
        use risingwave_rpc_client::MonitorClientPool;

        let dashboard_service = crate::dashboard::DashboardService {
            await_tree_reg: env.await_tree_reg().clone(),
            dashboard_addr: *dashboard_addr,
            prometheus_client,
            prometheus_selector,
            metadata_manager: metadata_manager.clone(),
            hummock_manager: hummock_manager.clone(),
            monitor_clients: MonitorClientPool::new(1, RpcClientConfig::default()),
            diagnose_command,
            profile_service: risingwave_common_heap_profiling::ProfileServiceImpl::new(
                server_config.clone(),
            ),
            trace_state,
        };
        let task = tokio::spawn(dashboard_service.serve());
        Some(task)
    } else {
        None
    };

    if let Some(prometheus_addr) = address_info.prometheus_addr {
        MetricsManager::boot_metrics_service(prometheus_addr.to_string())
    }

    // sub_tasks executed concurrently. Can be shutdown via shutdown_all
    sub_tasks.extend(hummock::start_hummock_workers(
        hummock_manager.clone(),
        backup_manager.clone(),
        &env.opts,
        {
            let catalog_controller = metadata_manager.catalog_controller.clone();
            Box::new(move || {
                let catalog_controller = catalog_controller.clone();
                Box::pin(async move {
                    catalog_controller
                        .get_table_change_log_truncate_info()
                        .await
                        .map(Some)
                        .unwrap_or_else(|e| {
                            tracing::warn!(err = %e.as_report(), "failed to collect table change log retention metadata");
                            None
                        })
                })
            })
        },
        {
            let catalog_controller = metadata_manager.catalog_controller.clone();
            Box::new(move || {
                let catalog_controller = catalog_controller.clone();
                Box::pin(async move {
                    catalog_controller
                        .get_pinned_snapshot_epochs()
                        .await
                        .map(Some)
                        .unwrap_or_else(|e| {
                            tracing::warn!(
                                err = %e.as_report(),
                                "failed to collect pinned snapshot epochs; pausing time-travel vacuum",
                            );
                            None
                        })
                })
            })
        },
    ));
    sub_tasks.push(start_worker_info_monitor(
        metadata_manager.clone(),
        election_client.clone(),
        Duration::from_secs(env.opts.node_num_monitor_interval_sec),
        meta_metrics.clone(),
    ));
    sub_tasks.push(start_info_monitor(
        metadata_manager.clone(),
        hummock_manager.clone(),
        barrier_manager.clone(),
        env.system_params_manager_impl_ref(),
        meta_metrics.clone(),
    ));
    sub_tasks.push(SystemParamsController::start_params_notifier(
        env.system_params_manager_impl_ref(),
    ));
    sub_tasks.push(HummockManager::hummock_timer_task(
        hummock_manager.clone(),
        Some(backup_manager),
    ));
    sub_tasks.extend(HummockManager::compaction_event_loop(
        hummock_manager.clone(),
        compactor_streams_change_rx,
    ));

    sub_tasks.extend(IcebergCompactionManager::iceberg_compaction_event_loop(
        iceberg_compaction_mgr.clone(),
        iceberg_compactor_event_rx,
    ));

    {
        sub_tasks.push(ClusterController::start_heartbeat_checker(
            metadata_manager.cluster_controller.clone(),
            Duration::from_secs(1),
        ));

        if !env.opts.disable_automatic_parallelism_control {
            sub_tasks.push(stream_manager.start_auto_parallelism_monitor());
        }
    }

    let _idle_checker_handle = IdleManager::start_idle_checker(
        env.idle_manager_ref(),
        Duration::from_secs(30),
        shutdown.clone(),
    );

    let (abort_sender, abort_recv) = tokio::sync::oneshot::channel();
    let notification_mgr = env.notification_manager_ref();
    let stream_abort_handler = tokio::spawn(async move {
        let _ = abort_recv.await;
        notification_mgr.abort_all();
        compactor_manager.abort_all_compactors();
    });
    sub_tasks.push((stream_abort_handler, abort_sender));

    let telemetry_manager = TelemetryManager::new(
        Arc::new(MetaTelemetryInfoFetcher::new(env.cluster_id().clone())),
        Arc::new(MetaReportCreator::new(
            metadata_manager.clone(),
            object_store_media_type,
        )),
    );

    // May start telemetry reporting
    if env.opts.telemetry_enabled && telemetry_env_enabled() {
        sub_tasks.push(telemetry_manager.start().await);
    } else {
        tracing::info!("Telemetry didn't start due to meta backend or config");
    }
    if !cfg!(madsim) && report_scarf_enabled() {
        tokio::spawn(report_to_scarf());
    } else {
        tracing::info!("Scarf reporting is disabled");
    };

    if let Some(pair) = env.event_log_manager_ref().take_join_handle() {
        sub_tasks.push(pair);
    }

    tracing::info!("Assigned cluster id {:?}", *env.cluster_id());
    tracing::info!("Starting meta services");

    let event = risingwave_pb::meta::event_log::EventMetaNodeStart {
        advertise_addr: address_info.advertise_addr,
        listen_addr: address_info.listen_addr.to_string(),
        opts: serde_json::to_string(&env.opts).unwrap(),
    };
    env.event_log_manager_ref().add_event_logs(vec![
        risingwave_pb::meta::event_log::Event::MetaNodeStart(event),
    ]);

    let server_builder = tonic::transport::Server::builder()
        .layer(MetricsMiddlewareLayer::new(meta_metrics))
        .layer(TracingExtractLayer::new())
        .layer(AwaitTreeMiddlewareLayer::new(env.await_tree_reg().clone()))
        .add_service(HeartbeatServiceServer::new(heartbeat_srv))
        .add_service(ClusterServiceServer::new(cluster_srv))
        .add_service(StreamManagerServiceServer::new(stream_srv))
        .add_service(
            HummockManagerServiceServer::new(hummock_srv).max_decoding_message_size(usize::MAX),
        )
        .add_service(NotificationServiceServer::new(notification_srv))
        .add_service(MetaMemberServiceServer::new(meta_member_srv))
        .add_service(DdlServiceServer::new(ddl_srv).max_decoding_message_size(usize::MAX))
        .add_service(UserServiceServer::new(user_srv))
        .add_service(CloudServiceServer::new(cloud_srv))
        .add_service(ScaleServiceServer::new(scale_srv).max_decoding_message_size(usize::MAX))
        .add_service(HealthServer::new(health_srv))
        .add_service(BackupServiceServer::new(backup_srv))
        .add_service(SystemParamsServiceServer::new(system_params_srv))
        .add_service(SessionParamServiceServer::new(session_params_srv))
        .add_service(TelemetryInfoServiceServer::new(telemetry_srv))
        .add_service(ServingServiceServer::new(serving_srv))
        .add_service(
            SinkCoordinationServiceServer::new(sink_coordination_srv)
                .max_decoding_message_size(usize::MAX),
        )
        .add_service(
            EventLogServiceServer::new(event_log_srv).max_decoding_message_size(usize::MAX),
        )
        .add_service(ClusterLimitServiceServer::new(cluster_limit_srv))
        .add_service(HostedIcebergCatalogServiceServer::new(
            hosted_iceberg_catalog_srv,
        ))
        .add_service(configured_monitor_service_server(
            MonitorServiceServer::new(monitor_srv),
        ));

    #[cfg(not(madsim))] // `otlp-embedded` does not use madsim-patched tonic
    let server_builder = server_builder.add_service(TraceServiceServer::new(trace_srv));

    let server = server_builder.monitored_serve_with_shutdown(
        address_info.listen_addr,
        "grpc-meta-leader-service",
        TcpConfig {
            tcp_nodelay: true,
            keepalive_duration: None,
        },
        shutdown.clone().cancelled_owned(),
    );
    started::set();
    let _server_handle = tokio::spawn(server);

    // Wait for the shutdown signal.
    shutdown.cancelled().await;
    // TODO(shutdown): may warn user if there's any other node still running in the cluster.
    // TODO(shutdown): do we have any other shutdown tasks?
    Ok(())
}

fn is_correct_data_directory(data_directory: &str) -> bool {
    let data_directory_regex = Regex::new(r"^[0-9a-zA-Z_/-]{1,}$").unwrap();
    if data_directory.is_empty()
        || !data_directory_regex.is_match(data_directory)
        || data_directory.ends_with('/')
        || data_directory.starts_with('/')
        || data_directory.contains("//")
        || data_directory.len() > 800
    {
        return false;
    }
    true
}
