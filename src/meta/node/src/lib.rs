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

#![cfg_attr(coverage, feature(coverage_attribute))]

mod server;

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use educe::Educe;
pub use error::{MetaError, MetaResult};
use redact::Secret;
use risingwave_common::config::OverrideConfig;
use risingwave_common::license::LicenseKey;
use risingwave_common::system_param::StateStoreUrl;
use risingwave_common::util::meta_addr::MetaAddressStrategy;
use risingwave_common::util::resource_util;
use risingwave_common::util::tokio_util::sync::CancellationToken;
use risingwave_common::{GIT_SHA, RW_VERSION};
use risingwave_common_heap_profiling::HeapProfiler;
use risingwave_meta::*;
use risingwave_meta_service::*;
pub use rpc::{ElectionClient, ElectionMember};
use server::rpc_serve;
pub use server::started::get as is_server_started;

use crate::manager::MetaOpts;

#[derive(Educe, Clone, Parser, OverrideConfig)]
#[educe(Debug)]
#[command(version, about = "The central metadata management service")]
pub struct MetaNodeOpts {
    // TODO: use `SocketAddr`
    #[clap(long, env = "RW_LISTEN_ADDR", default_value = "127.0.0.1:5690")]
    pub listen_addr: String,

    /// The address for contacting this instance of the service.
    /// This would be synonymous with the service's "public address"
    /// or "identifying address".
    /// It will serve as a unique identifier in cluster
    /// membership and leader election. Must be specified for meta backend.
    #[clap(long, env = "RW_ADVERTISE_ADDR", default_value = "127.0.0.1:5690")]
    pub advertise_addr: String,

    #[clap(long, env = "RW_DASHBOARD_HOST")]
    pub dashboard_host: Option<String>,

    /// We will start a http server at this address via `MetricsManager`.
    /// Then the prometheus instance will poll the metrics from this address.
    #[clap(long, env = "RW_PROMETHEUS_HOST", alias = "prometheus-host")]
    pub prometheus_listener_addr: Option<String>,

    /// Endpoint of the SQL service, make it non-option when SQL service is required.
    #[clap(long, hide = true, env = "RW_SQL_ENDPOINT")]
    pub sql_endpoint: Option<Secret<String>>,

    /// Username of sql backend, required when meta backend set to MySQL or PostgreSQL.
    #[clap(long, hide = true, env = "RW_SQL_USERNAME", default_value = "")]
    pub sql_username: String,

    /// Password of sql backend, required when meta backend set to MySQL or PostgreSQL.
    #[clap(long, hide = true, env = "RW_SQL_PASSWORD", default_value = "")]
    pub sql_password: Secret<String>,

    /// Database of sql backend, required when meta backend set to MySQL or PostgreSQL.
    #[clap(long, hide = true, env = "RW_SQL_DATABASE", default_value = "")]
    pub sql_database: String,

    /// Params for the URL connection, such as `sslmode=disable`.
    /// Example: `param1=value1&param2=value2`
    #[clap(long, hide = true, env = "RW_SQL_URL_PARAMS")]
    pub sql_url_params: Option<String>,

    /// The HTTP REST-API address of the Prometheus instance associated to this cluster.
    /// This address is used to serve `PromQL` queries to Prometheus.
    /// It is also used by Grafana Dashboard Service to fetch metrics and visualize them.
    #[clap(long, env = "RW_PROMETHEUS_ENDPOINT")]
    pub prometheus_endpoint: Option<String>,

    /// The additional selector used when querying Prometheus.
    ///
    /// The format is same as `PromQL`. Example: `instance="foo",namespace="bar"`
    #[clap(long, env = "RW_PROMETHEUS_SELECTOR")]
    pub prometheus_selector: Option<String>,

    /// Default tag for the endpoint created when creating a privatelink connection.
    /// Will be appended to the tags specified in the `tags` field in with clause in `create
    /// connection`.
    #[clap(long, hide = true, env = "RW_PRIVATELINK_ENDPOINT_DEFAULT_TAGS")]
    pub privatelink_endpoint_default_tags: Option<String>,

    #[clap(long, hide = true, env = "RW_VPC_ID")]
    pub vpc_id: Option<String>,

    #[clap(long, hide = true, env = "RW_VPC_SECURITY_GROUP_ID")]
    pub security_group_id: Option<String>,

    /// The path of `risingwave.toml` configuration file.
    ///
    /// If empty, default configuration values will be used.
    #[clap(long, env = "RW_CONFIG_PATH", default_value = "")]
    pub config_path: String,

    #[clap(long, hide = true, env = "RW_BACKEND", value_enum)]
    #[override_opts(path = meta.backend)]
    pub backend: Option<MetaBackend>,

    /// The interval of periodic barrier.
    #[clap(long, hide = true, env = "RW_BARRIER_INTERVAL_MS")]
    #[override_opts(path = system.barrier_interval_ms)]
    pub barrier_interval_ms: Option<u32>,

    /// Target size of the Sstable.
    #[clap(long, hide = true, env = "RW_SSTABLE_SIZE_MB")]
    #[override_opts(path = system.sstable_size_mb)]
    pub sstable_size_mb: Option<u32>,

    /// Size of each block in bytes in SST.
    #[clap(long, hide = true, env = "RW_BLOCK_SIZE_KB")]
    #[override_opts(path = system.block_size_kb)]
    pub block_size_kb: Option<u32>,

    /// Deprecated: Bloom filter is no longer a supported SST filter implementation.
    #[clap(long, hide = true, env = "RW_BLOOM_FALSE_POSITIVE")]
    #[override_opts(path = system.bloom_false_positive)]
    pub bloom_false_positive: Option<f64>,

    /// State store url
    #[clap(long, hide = true, env = "RW_STATE_STORE")]
    #[override_opts(path = system.state_store)]
    pub state_store: Option<StateStoreUrl>,

    /// Remote directory for storing data and metadata objects.
    #[clap(long, hide = true, env = "RW_DATA_DIRECTORY")]
    #[override_opts(path = system.data_directory)]
    pub data_directory: Option<String>,

    /// Whether config object storage bucket lifecycle to purge stale data.
    #[clap(long, hide = true, env = "RW_DO_NOT_CONFIG_BUCKET_LIFECYCLE")]
    #[override_opts(path = meta.do_not_config_object_storage_lifecycle)]
    pub do_not_config_object_storage_lifecycle: Option<bool>,

    /// Remote storage url for storing snapshots.
    #[clap(long, hide = true, env = "RW_BACKUP_STORAGE_URL")]
    #[override_opts(path = system.backup_storage_url)]
    pub backup_storage_url: Option<String>,

    /// Remote directory for storing snapshots.
    #[clap(long, hide = true, env = "RW_BACKUP_STORAGE_DIRECTORY")]
    #[override_opts(path = system.backup_storage_directory)]
    pub backup_storage_directory: Option<String>,

    /// Enable heap profile dump when memory usage is high.
    #[clap(long, hide = true, env = "RW_HEAP_PROFILING_DIR")]
    #[override_opts(path = server.heap_profiling.dir)]
    pub heap_profiling_dir: Option<String>,

    /// Exit if idle for a certain period of time.
    #[clap(long, hide = true, env = "RW_DANGEROUS_MAX_IDLE_SECS")]
    #[override_opts(path = meta.dangerous_max_idle_secs)]
    pub dangerous_max_idle_secs: Option<u64>,

    /// Endpoint of the connector node.
    #[deprecated = "connector node has been deprecated."]
    #[clap(long, hide = true, env = "RW_CONNECTOR_RPC_ENDPOINT")]
    pub connector_rpc_endpoint: Option<String>,

    /// The license key to activate enterprise features.
    #[clap(long, hide = true, env = "RW_LICENSE_KEY")]
    #[override_opts(path = system.license_key)]
    pub license_key: Option<LicenseKey>,

    /// The path of the license key file to be watched and hot-reloaded.
    #[clap(long, env = "RW_LICENSE_KEY_PATH")]
    pub license_key_path: Option<PathBuf>,

    /// 128-bit AES key for secret store in HEX format.
    #[clap(long, hide = true, env = "RW_SECRET_STORE_PRIVATE_KEY_HEX")]
    pub secret_store_private_key_hex: Option<Secret<String>>,

    /// The path of the temp secret file directory.
    #[clap(
        long,
        hide = true,
        env = "RW_TEMP_SECRET_FILE_DIR",
        default_value = "./secrets"
    )]
    pub temp_secret_file_dir: String,

    /// Address of the serverless backfill controller.
    /// Needed if meta receives a streaming job with serverless backfill enabled.
    /// Feature disabled by default.
    #[clap(long, env = "RW_SBC_ADDR", default_value = "")]
    pub serverless_backfill_controller_addr: String,
}

impl risingwave_common::opts::Opts for MetaNodeOpts {
    fn name() -> &'static str {
        "meta"
    }

    fn meta_addr(&self) -> MetaAddressStrategy {
        format!("http://{}", self.listen_addr)
            .parse()
            .expect("invalid listen address")
    }
}

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use risingwave_common::config::{MetaBackend, RwConfig, load_config};
use tracing::info;

/// Start meta node
pub fn start(
    opts: MetaNodeOpts,
    shutdown: CancellationToken,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        info!("Starting meta node");
        info!("> options: {:?}", opts);
        let config = load_config(&opts.config_path, &opts);
        info!("> config: {:?}", config);
        info!("> version: {} ({})", RW_VERSION, GIT_SHA);
        let listen_addr = opts.listen_addr.parse().unwrap();
        let dashboard_addr = opts.dashboard_host.map(|x| x.parse().unwrap());
        let prometheus_addr = opts.prometheus_listener_addr.map(|x| x.parse().unwrap());
        let meta_store_config = config.meta.meta_store_config.clone();
        let backend = match config.meta.backend {
            MetaBackend::Mem => {
                if opts.sql_endpoint.is_some() {
                    tracing::warn!("`--sql-endpoint` is ignored when using `mem` backend");
                }
                MetaStoreBackend::Mem
            }
            MetaBackend::Sql => MetaStoreBackend::Sql {
                endpoint: opts
                    .sql_endpoint
                    .expect("sql endpoint is required")
                    .expose_secret()
                    .clone(),
                config: meta_store_config,
            },
            MetaBackend::Sqlite => MetaStoreBackend::Sql {
                endpoint: format!(
                    "sqlite://{}?mode=rwc",
                    opts.sql_endpoint
                        .expect("sql endpoint is required")
                        .expose_secret()
                ),
                config: meta_store_config,
            },
            MetaBackend::Postgres => MetaStoreBackend::Sql {
                endpoint: format!(
                    "postgres://{}:{}@{}/{}{}",
                    opts.sql_username,
                    opts.sql_password.expose_secret(),
                    opts.sql_endpoint
                        .expect("sql endpoint is required")
                        .expose_secret(),
                    opts.sql_database,
                    if let Some(params) = &opts.sql_url_params
                        && !params.is_empty()
                    {
                        format!("?{}", params)
                    } else {
                        "".to_owned()
                    }
                ),
                config: meta_store_config,
            },
            MetaBackend::Mysql => MetaStoreBackend::Sql {
                endpoint: format!(
                    "mysql://{}:{}@{}/{}{}",
                    opts.sql_username,
                    opts.sql_password.expose_secret(),
                    opts.sql_endpoint
                        .expect("sql endpoint is required")
                        .expose_secret(),
                    opts.sql_database,
                    if let Some(params) = &opts.sql_url_params
                        && !params.is_empty()
                    {
                        format!("?{}", params)
                    } else {
                        "".to_owned()
                    }
                ),
                config: meta_store_config,
            },
        };
        validate_config(&config);

        let total_memory_bytes = resource_util::memory::system_memory_available_bytes();
        let heap_profiler =
            HeapProfiler::new(total_memory_bytes, config.server.heap_profiling.clone());
        // Run a background heap profiler
        heap_profiler.start();

        let secret_store_private_key = opts
            .secret_store_private_key_hex
            .map(|key| hex::decode(key.expose_secret()).unwrap());
        let max_heartbeat_interval =
            Duration::from_secs(config.meta.max_heartbeat_interval_secs as u64);
        let max_idle_ms = config.meta.dangerous_max_idle_secs.unwrap_or(0) * 1000;
        let in_flight_barrier_nums = config.streaming.in_flight_barrier_nums;
        let snapshot_backfill_finish_max_lagged_barriers = config
            .streaming
            .snapshot_backfill_finish_max_lagged_barriers;
        let snapshot_backfill_barrier_amplification_factor = config
            .streaming
            .snapshot_backfill_barrier_amplification_factor;
        let privatelink_endpoint_default_tags =
            opts.privatelink_endpoint_default_tags.map(|tags| {
                tags.split(',')
                    .map(|s| {
                        let key_val = s.split_once('=').unwrap();
                        (key_val.0.to_owned(), key_val.1.to_owned())
                    })
                    .collect()
            });

        let add_info = AddressInfo {
            advertise_addr: opts.advertise_addr.clone(),
            listen_addr,
            prometheus_addr,
            dashboard_addr,
        };

        const MIN_TIMEOUT_INTERVAL_SEC: u64 = 20;
        let compaction_task_max_progress_interval_secs = {
            let retry_config = &config.storage.object_store.retry;
            let max_streaming_read_timeout_ms = (retry_config.streaming_read_attempt_timeout_ms
                + retry_config.req_backoff_max_delay_ms)
                * retry_config.streaming_read_retry_attempts as u64;
            let max_streaming_upload_timeout_ms = (retry_config
                .streaming_upload_attempt_timeout_ms
                + retry_config.req_backoff_max_delay_ms)
                * retry_config.streaming_upload_retry_attempts as u64;
            let max_upload_timeout_ms = (retry_config.upload_attempt_timeout_ms
                + retry_config.req_backoff_max_delay_ms)
                * retry_config.upload_retry_attempts as u64;
            let max_read_timeout_ms = (retry_config.read_attempt_timeout_ms
                + retry_config.req_backoff_max_delay_ms)
                * retry_config.read_retry_attempts as u64;
            let max_timeout_ms = max_streaming_read_timeout_ms
                .max(max_upload_timeout_ms)
                .max(max_streaming_upload_timeout_ms)
                .max(max_read_timeout_ms)
                .max(config.meta.compaction_task_max_progress_interval_secs * 1000);
            max_timeout_ms / 1000
        } + MIN_TIMEOUT_INTERVAL_SEC;

        Box::pin(rpc_serve(
            add_info,
            backend,
            max_heartbeat_interval,
            config.meta.meta_leader_lease_secs,
            config.server.clone(),
            MetaOpts {
                enable_recovery: !config.meta.disable_recovery,
                clean_all_foreground_jobs_on_recovery: config
                    .meta
                    .clean_all_foreground_jobs_on_recovery,
                disable_automatic_parallelism_control: config
                    .meta
                    .disable_automatic_parallelism_control,
                parallelism_control_batch_size: config.meta.parallelism_control_batch_size,
                parallelism_control_trigger_period_sec: config
                    .meta
                    .parallelism_control_trigger_period_sec,
                parallelism_control_trigger_first_delay_sec: config
                    .meta
                    .parallelism_control_trigger_first_delay_sec,
                in_flight_barrier_nums,
                snapshot_backfill_finish_max_lagged_barriers,
                snapshot_backfill_barrier_amplification_factor,
                max_idle_ms,
                compaction_deterministic_test: config.meta.enable_compaction_deterministic,
                default_parallelism: config.meta.default_parallelism,
                vacuum_interval_sec: config.meta.vacuum_interval_sec,
                time_travel_vacuum_interval_sec: config
                    .meta
                    .developer
                    .time_travel_vacuum_interval_sec,
                time_travel_vacuum_max_version_count: config
                    .meta
                    .developer
                    .time_travel_vacuum_max_version_count,
                vacuum_spin_interval_ms: config.meta.vacuum_spin_interval_ms,
                iceberg_gc_interval_sec: config.meta.iceberg_gc_interval_sec,
                iceberg_compaction_report_timeout_sec: config
                    .meta
                    .iceberg_compaction_report_timeout_sec,
                iceberg_compaction_config_refresh_interval_sec: config
                    .meta
                    .iceberg_compaction_config_refresh_interval_sec,
                hummock_version_checkpoint_interval_sec: config
                    .meta
                    .hummock_version_checkpoint_interval_sec,
                enable_hummock_data_archive: config.meta.enable_hummock_data_archive,
                checkpoint_compression_algorithm: config.meta.checkpoint_compression_algorithm,
                checkpoint_read_chunk_size: config.meta.checkpoint_read_chunk_size,
                checkpoint_read_max_in_flight_chunks: config
                    .meta
                    .checkpoint_read_max_in_flight_chunks,
                hummock_time_travel_snapshot_interval: config
                    .meta
                    .hummock_time_travel_snapshot_interval,
                hummock_time_travel_sst_info_fetch_batch_size: config
                    .meta
                    .developer
                    .hummock_time_travel_sst_info_fetch_batch_size,
                hummock_time_travel_sst_info_insert_batch_size: config
                    .meta
                    .developer
                    .hummock_time_travel_sst_info_insert_batch_size,
                hummock_time_travel_epoch_version_insert_batch_size: config
                    .meta
                    .developer
                    .hummock_time_travel_epoch_version_insert_batch_size,
                hummock_time_travel_delta_fetch_batch_size: config
                    .meta
                    .developer
                    .hummock_time_travel_delta_fetch_batch_size,
                hummock_gc_history_insert_batch_size: config
                    .meta
                    .developer
                    .hummock_gc_history_insert_batch_size,
                hummock_time_travel_filter_out_objects_batch_size: config
                    .meta
                    .developer
                    .hummock_time_travel_filter_out_objects_batch_size,
                hummock_time_travel_filter_out_objects_v1: config
                    .meta
                    .developer
                    .hummock_time_travel_filter_out_objects_v1,
                hummock_time_travel_filter_out_objects_list_version_batch_size: config
                    .meta
                    .developer
                    .hummock_time_travel_filter_out_objects_list_version_batch_size,
                hummock_time_travel_filter_out_objects_list_delta_batch_size: config
                    .meta
                    .developer
                    .hummock_time_travel_filter_out_objects_list_delta_batch_size,
                min_delta_log_num_for_hummock_version_checkpoint: config
                    .meta
                    .min_delta_log_num_for_hummock_version_checkpoint,
                min_sst_retention_time_sec: config.meta.min_sst_retention_time_sec,
                full_gc_interval_sec: config.meta.full_gc_interval_sec,
                full_gc_object_limit: config.meta.full_gc_object_limit,
                gc_history_retention_time_sec: config.meta.gc_history_retention_time_sec,
                max_inflight_time_travel_query: config.meta.max_inflight_time_travel_query,
                enable_committed_sst_sanity_check: config.meta.enable_committed_sst_sanity_check,
                periodic_compaction_interval_sec: config.meta.periodic_compaction_interval_sec,
                node_num_monitor_interval_sec: config.meta.node_num_monitor_interval_sec,
                prometheus_endpoint: opts.prometheus_endpoint,
                prometheus_selector: opts.prometheus_selector,
                vpc_id: opts.vpc_id,
                security_group_id: opts.security_group_id,
                privatelink_endpoint_default_tags,
                periodic_space_reclaim_compaction_interval_sec: config
                    .meta
                    .periodic_space_reclaim_compaction_interval_sec,
                telemetry_enabled: config.server.telemetry_enabled,
                periodic_ttl_reclaim_compaction_interval_sec: config
                    .meta
                    .periodic_ttl_reclaim_compaction_interval_sec,
                periodic_tombstone_reclaim_compaction_interval_sec: config
                    .meta
                    .periodic_tombstone_reclaim_compaction_interval_sec,
                periodic_scheduling_compaction_group_split_interval_sec: config
                    .meta
                    .periodic_scheduling_compaction_group_split_interval_sec,
                enable_compaction_group_normalize: config.meta.enable_compaction_group_normalize,
                max_normalize_splits_per_round: config.meta.max_normalize_splits_per_round,
                periodic_scheduling_compaction_group_merge_interval_sec: config
                    .meta
                    .periodic_scheduling_compaction_group_merge_interval_sec,
                compaction_group_merge_dimension_threshold: config
                    .meta
                    .compaction_group_merge_dimension_threshold,
                table_high_write_throughput_threshold: config
                    .meta
                    .table_high_write_throughput_threshold,
                table_low_write_throughput_threshold: config
                    .meta
                    .table_low_write_throughput_threshold,
                partition_vnode_count: config.meta.partition_vnode_count,
                compact_task_table_size_partition_threshold_low: config
                    .meta
                    .compact_task_table_size_partition_threshold_low,
                compact_task_table_size_partition_threshold_high: config
                    .meta
                    .compact_task_table_size_partition_threshold_high,
                do_not_config_object_storage_lifecycle: config
                    .meta
                    .do_not_config_object_storage_lifecycle,
                compaction_task_max_heartbeat_interval_secs: config
                    .meta
                    .compaction_task_max_heartbeat_interval_secs,
                compaction_task_max_progress_interval_secs,
                compaction_task_id_refill_capacity: config.meta.compaction_task_id_refill_capacity,
                compaction_config: Some(config.meta.compaction_config),
                hybrid_partition_node_count: config.meta.hybrid_partition_vnode_count,
                event_log_enabled: config.meta.event_log_enabled,
                event_log_channel_max_size: config.meta.event_log_channel_max_size,
                advertise_addr: opts.advertise_addr,
                cached_traces_num: config.meta.developer.cached_traces_num,
                cached_traces_memory_limit_bytes: config
                    .meta
                    .developer
                    .cached_traces_memory_limit_bytes,
                enable_trivial_move: config.meta.developer.enable_trivial_move,
                enable_check_task_level_overlap: config
                    .meta
                    .developer
                    .enable_check_task_level_overlap,
                enable_dropped_column_reclaim: config.meta.enable_dropped_column_reclaim,
                split_group_size_ratio: config.meta.split_group_size_ratio,
                refresh_scheduler_interval_sec: config
                    .streaming
                    .developer
                    .refresh_scheduler_interval_sec,
                table_stat_high_write_throughput_ratio_for_split: config
                    .meta
                    .table_stat_high_write_throughput_ratio_for_split,
                table_stat_low_write_throughput_ratio_for_merge: config
                    .meta
                    .table_stat_low_write_throughput_ratio_for_merge,
                table_stat_throuput_window_seconds_for_split: config
                    .meta
                    .table_stat_throuput_window_seconds_for_split,
                table_stat_throuput_window_seconds_for_merge: config
                    .meta
                    .table_stat_throuput_window_seconds_for_merge,
                object_store_config: config.storage.object_store,
                max_trivial_move_task_count_per_loop: config
                    .meta
                    .developer
                    .max_trivial_move_task_count_per_loop,
                max_get_task_probe_times: config.meta.developer.max_get_task_probe_times,
                secret_store_private_key,
                temp_secret_file_dir: opts.temp_secret_file_dir,
                actor_cnt_per_worker_parallelism_hard_limit: config
                    .meta
                    .developer
                    .actor_cnt_per_worker_parallelism_hard_limit,
                actor_cnt_per_worker_parallelism_soft_limit: config
                    .meta
                    .developer
                    .actor_cnt_per_worker_parallelism_soft_limit,
                table_change_log_insert_batch_size: config
                    .meta
                    .developer
                    .table_change_log_insert_batch_size,
                table_change_log_delete_batch_size: config
                    .meta
                    .developer
                    .table_change_log_delete_batch_size,
                table_change_log_truncate_interval_sec: config
                    .meta
                    .developer
                    .table_change_log_truncate_interval_sec,
                license_key_path: opts.license_key_path,
                compute_client_config: config.meta.developer.compute_client_config.clone(),
                stream_client_config: config.meta.developer.stream_client_config.clone(),
                frontend_client_config: config.meta.developer.frontend_client_config.clone(),
                redact_sql_option_keywords: Arc::new(
                    config
                        .batch
                        .redact_sql_option_keywords
                        .into_iter()
                        .collect(),
                ),
                cdc_table_split_init_sleep_interval_splits: config
                    .meta
                    .cdc_table_split_init_sleep_interval_splits,
                cdc_table_split_init_sleep_duration_millis: config
                    .meta
                    .cdc_table_split_init_sleep_duration_millis,
                cdc_table_split_init_insert_batch_size: config
                    .meta
                    .cdc_table_split_init_insert_batch_size,

                enable_legacy_table_migration: config.meta.enable_legacy_table_migration,
                pause_on_next_bootstrap_offline: config.meta.pause_on_next_bootstrap_offline,
                serverless_backfill_controller_addr: opts.serverless_backfill_controller_addr,
            },
            config.system.into_init_system_params(),
            config.session_init,
            shutdown,
        ))
        .await
        .unwrap();
    })
}

fn validate_config(config: &RwConfig) {
    if config.meta.meta_leader_lease_secs <= 2 {
        let error_msg = "`meta_leader_lease_secs` must be greater than 2";
        tracing::error!(error_msg);
        panic!("{}", error_msg);
    }

    if config.meta.parallelism_control_batch_size == 0 {
        let error_msg = "`parallelism_control_batch_size` must be greater than 0";
        tracing::error!(error_msg);
        panic!("{}", error_msg);
    }

    if config.meta.checkpoint_read_chunk_size == 0 {
        let error_msg = "`checkpoint_read_chunk_size` must be greater than 0";
        tracing::error!(error_msg);
        panic!("{}", error_msg);
    }

    if config.meta.checkpoint_read_max_in_flight_chunks == 0 {
        let error_msg = "`checkpoint_read_max_in_flight_chunks` must be greater than 0";
        tracing::error!(error_msg);
        panic!("{}", error_msg);
    }

    if config.meta.compaction_task_id_refill_capacity == 0 {
        let error_msg = "`compaction_task_id_refill_capacity` must be greater than 0";
        tracing::error!(error_msg);
        panic!("{}", error_msg);
    }

    if config.meta.iceberg_compaction_report_timeout_sec == 0 {
        let error_msg = "`iceberg_compaction_report_timeout_sec` must be greater than 0";
        tracing::error!(error_msg);
        panic!("{}", error_msg);
    }

    if config.meta.iceberg_compaction_config_refresh_interval_sec == 0 {
        let error_msg = "`iceberg_compaction_config_refresh_interval_sec` must be greater than 0";
        tracing::error!(error_msg);
        panic!("{}", error_msg);
    }
}
