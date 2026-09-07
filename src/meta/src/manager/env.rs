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

use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use anyhow::Context;
use risingwave_common::config::{
    CompactionConfig, DefaultParallelism, ObjectStoreConfig, RpcClientConfig, SessionInitConfig,
};
use risingwave_common::system_param::reader::SystemParamsReader;
use risingwave_common::{bail, system_param};
use risingwave_meta_model::prelude::Cluster;
use risingwave_pb::meta::SystemParams;
use risingwave_rpc_client::{
    FrontendClientPool, FrontendClientPoolRef, StreamClientPool, StreamClientPoolRef,
};
use risingwave_sqlparser::ast::RedactSqlOptionKeywordsRef;
use sea_orm::EntityTrait;

use crate::MetaResult;
use crate::barrier::SharedActorInfos;
use crate::controller::SqlMetaStore;
use crate::controller::id::{
    IdGeneratorManager as SqlIdGeneratorManager, IdGeneratorManagerRef as SqlIdGeneratorManagerRef,
};
use crate::controller::session_params::{SessionParamsController, SessionParamsControllerRef};
use crate::controller::system_param::{SystemParamsController, SystemParamsControllerRef};
use crate::hummock::sequence::SequenceGenerator;
use crate::manager::event_log::{EventLogManagerRef, start_event_log_manager};
use crate::manager::{IdleManager, IdleManagerRef, NotificationManager, NotificationManagerRef};
use crate::model::ClusterId;

/// [`MetaSrvEnv`] is the global environment in Meta service. The instance will be shared by all
/// kind of managers inside Meta.
#[derive(Clone)]
pub struct MetaSrvEnv {
    /// id generator manager.
    id_gen_manager_impl: SqlIdGeneratorManagerRef,

    /// system param manager.
    system_param_manager_impl: SystemParamsControllerRef,

    /// session param manager.
    session_param_manager_impl: SessionParamsControllerRef,

    /// meta store.
    meta_store_impl: SqlMetaStore,

    /// notification manager.
    notification_manager: NotificationManagerRef,

    pub shared_actor_info: SharedActorInfos,

    /// stream client pool memorization.
    stream_client_pool: StreamClientPoolRef,

    /// rpc client pool for frontend nodes.
    frontend_client_pool: FrontendClientPoolRef,

    /// idle status manager.
    idle_manager: IdleManagerRef,

    event_log_manager: EventLogManagerRef,

    /// Unique identifier of the cluster.
    cluster_id: ClusterId,

    pub hummock_seq: Arc<SequenceGenerator>,

    /// The await-tree registry of the current meta node.
    await_tree_reg: await_tree::Registry,

    /// options read by all services
    pub opts: Arc<MetaOpts>,

    actor_id_generator: Arc<AtomicU32>,
}

/// Options shared by all meta service instances
#[derive(Clone, serde::Serialize)]
pub struct MetaOpts {
    /// Whether to enable the recovery of the cluster. If disabled, the meta service will exit on
    /// abnormal cases.
    pub enable_recovery: bool,
    /// Whether to clean up all foreground creating streaming jobs during recovery.
    pub clean_all_foreground_jobs_on_recovery: bool,
    /// Whether to disable the auto-scaling feature.
    pub disable_automatic_parallelism_control: bool,
    /// The number of streaming jobs per scaling operation.
    pub parallelism_control_batch_size: usize,
    /// The period of parallelism control trigger.
    pub parallelism_control_trigger_period_sec: u64,
    /// The first delay of parallelism control.
    pub parallelism_control_trigger_first_delay_sec: u64,
    /// The maximum number of pending barriers in each partial graph.
    pub in_flight_barrier_nums: usize,
    /// The maximum number of lagged barriers when finishing snapshot backfill.
    pub snapshot_backfill_finish_max_lagged_barriers: usize,
    /// The multiplier applied to the pending-barrier limit for snapshot backfill partial graphs.
    pub snapshot_backfill_barrier_amplification_factor: usize,
    /// After specified seconds of idle (no mview or flush), the process will be exited.
    /// 0 for infinite, process will never be exited due to long idle time.
    pub max_idle_ms: u64,
    /// Whether run in compaction detection test mode
    pub compaction_deterministic_test: bool,
    /// Default parallelism of units for all streaming jobs.
    pub default_parallelism: DefaultParallelism,

    /// Interval of invoking a vacuum job, to remove stale metadata from meta store and objects
    /// from object store.
    pub vacuum_interval_sec: u64,
    /// The spin interval inside a vacuum job. It avoids the vacuum job monopolizing resources of
    /// meta node.
    pub vacuum_spin_interval_ms: u64,
    /// Interval of invoking iceberg garbage collection, to expire old snapshots.
    pub iceberg_gc_interval_sec: u64,
    /// Maximum time to wait for an iceberg compaction task report before the lease expires.
    pub iceberg_compaction_report_timeout_sec: u64,
    /// Maximum time to reuse cached iceberg compaction schedule config before refreshing it.
    pub iceberg_compaction_config_refresh_interval_sec: u64,
    pub time_travel_vacuum_interval_sec: u64,
    pub time_travel_vacuum_max_version_count: Option<u32>,
    /// Interval of hummock version checkpoint.
    pub hummock_version_checkpoint_interval_sec: u64,
    pub enable_hummock_data_archive: bool,
    /// Compression algorithm for hummock version checkpoint: "zstd", "lz4", or "none".
    pub checkpoint_compression_algorithm: risingwave_common::config::CheckpointCompression,
    /// Chunk size in bytes for reading large checkpoints.
    pub checkpoint_read_chunk_size: usize,
    /// Maximum number of concurrent chunk reads for large checkpoints.
    pub checkpoint_read_max_in_flight_chunks: usize,
    pub hummock_time_travel_snapshot_interval: u64,
    pub hummock_time_travel_sst_info_fetch_batch_size: usize,
    pub hummock_time_travel_sst_info_insert_batch_size: usize,
    pub hummock_time_travel_epoch_version_insert_batch_size: usize,
    pub hummock_time_travel_delta_fetch_batch_size: usize,
    pub hummock_gc_history_insert_batch_size: usize,
    pub hummock_time_travel_filter_out_objects_batch_size: usize,
    pub hummock_time_travel_filter_out_objects_v1: bool,
    pub hummock_time_travel_filter_out_objects_list_version_batch_size: usize,
    pub hummock_time_travel_filter_out_objects_list_delta_batch_size: usize,
    /// The minimum delta log number a new checkpoint should compact, otherwise the checkpoint
    /// attempt is rejected. Greater value reduces object store IO, meanwhile it results in
    /// more loss of in memory `HummockVersionCheckpoint::stale_objects` state when meta node is
    /// restarted.
    pub min_delta_log_num_for_hummock_version_checkpoint: u64,
    /// Objects within `min_sst_retention_time_sec` won't be deleted by hummock full GC, even they
    /// are dangling.
    pub min_sst_retention_time_sec: u64,
    /// Interval of automatic hummock full GC.
    pub full_gc_interval_sec: u64,
    /// Max number of object per full GC job can fetch.
    pub full_gc_object_limit: u64,
    /// Duration in seconds to retain garbage collection history data.
    pub gc_history_retention_time_sec: u64,
    /// Max number of inflight time travel query.
    pub max_inflight_time_travel_query: u64,
    /// Enable sanity check when SSTs are committed
    pub enable_committed_sst_sanity_check: bool,
    /// Schedule compaction for all compaction groups with this interval.
    pub periodic_compaction_interval_sec: u64,
    /// Interval of reporting the number of nodes in the cluster.
    pub node_num_monitor_interval_sec: u64,
    /// The Prometheus endpoint for Meta Dashboard Service.
    /// The Dashboard service uses this in the following ways:
    /// 1. Query Prometheus for relevant metrics to find Stream Graph Bottleneck, and display it.
    /// 2. Provide cluster diagnostics, at `/api/monitor/diagnose` to troubleshoot cluster.
    ///    These are just examples which show how the Meta Dashboard Service queries Prometheus.
    pub prometheus_endpoint: Option<String>,

    /// The additional selector used when querying Prometheus.
    pub prometheus_selector: Option<String>,

    /// The VPC id of the cluster.
    pub vpc_id: Option<String>,

    /// A usable security group id to assign to a vpc endpoint
    pub security_group_id: Option<String>,

    /// Default tag for the endpoint created when creating a privatelink connection.
    /// Will be appended to the tags specified in the `tags` field in with clause in `create
    /// connection`.
    pub privatelink_endpoint_default_tags: Option<Vec<(String, String)>>,

    /// Schedule `space_reclaim_compaction` for all compaction groups with this interval.
    pub periodic_space_reclaim_compaction_interval_sec: u64,

    /// telemetry enabled in config file or not
    pub telemetry_enabled: bool,
    /// Schedule `ttl_reclaim_compaction` for all compaction groups with this interval.
    pub periodic_ttl_reclaim_compaction_interval_sec: u64,

    /// Schedule `tombstone_reclaim_compaction` for all compaction groups with this interval.
    pub periodic_tombstone_reclaim_compaction_interval_sec: u64,

    /// Schedule the regular compaction-group split job for all compaction groups with this interval.
    pub periodic_scheduling_compaction_group_split_interval_sec: u64,
    /// Whether to enable overlap normalization before the regular merge scheduler.
    pub enable_compaction_group_normalize: bool,
    /// Maximum normalize splits in one scheduler round. Must be greater than 0.
    pub max_normalize_splits_per_round: u64,

    /// Whether config object storage bucket lifecycle to purge stale data.
    pub do_not_config_object_storage_lifecycle: bool,

    pub partition_vnode_count: u32,

    /// threshold of high write throughput of state-table, unit: B/sec
    pub table_high_write_throughput_threshold: u64,
    /// threshold of low write throughput of state-table, unit: B/sec
    pub table_low_write_throughput_threshold: u64,

    pub compaction_task_max_heartbeat_interval_secs: u64,
    pub compaction_task_max_progress_interval_secs: u64,
    pub compaction_task_id_refill_capacity: u32,
    pub compaction_config: Option<CompactionConfig>,

    /// hybrid compaction group config
    ///
    /// `hybrid_partition_vnode_count` determines the granularity of vnodes in the hybrid compaction group for SST alignment.
    /// When `hybrid_partition_vnode_count` > 0, in hybrid compaction group
    /// - Tables with high write throughput will be split at vnode granularity
    /// - Tables with high size tables will be split by table granularity
    ///   When `hybrid_partition_vnode_count` = 0,no longer be special alignment operations for the hybrid compaction group
    pub hybrid_partition_node_count: u32,

    pub event_log_enabled: bool,
    pub event_log_channel_max_size: u32,
    pub advertise_addr: String,
    /// The number of traces to be cached in-memory by the tracing collector
    /// embedded in the meta node.
    pub cached_traces_num: u32,
    /// The maximum memory usage in bytes for the tracing collector embedded
    /// in the meta node.
    pub cached_traces_memory_limit_bytes: usize,

    /// l0 picker whether to select trivial move task
    pub enable_trivial_move: bool,

    /// l0 multi level picker whether to check the overlap accuracy between sub levels
    pub enable_check_task_level_overlap: bool,
    pub enable_dropped_column_reclaim: bool,

    /// Whether to split the compaction group when the size of the group exceeds the threshold.
    pub split_group_size_ratio: f64,

    /// The interval in seconds for the refresh scheduler to check and trigger scheduled refreshes.
    pub refresh_scheduler_interval_sec: u64,

    /// To split the compaction group when the high throughput statistics of the group exceeds the threshold.
    pub table_stat_high_write_throughput_ratio_for_split: f64,

    /// To merge the compaction group when the low throughput statistics of the group exceeds the threshold.
    pub table_stat_low_write_throughput_ratio_for_merge: f64,

    /// The window seconds of table throughput statistic history for split compaction group.
    pub table_stat_throuput_window_seconds_for_split: usize,

    /// The window seconds of table throughput statistic history for merge compaction group.
    pub table_stat_throuput_window_seconds_for_merge: usize,

    /// The configuration of the object store
    pub object_store_config: ObjectStoreConfig,

    /// The maximum number of trivial move tasks to be picked in a single loop
    pub max_trivial_move_task_count_per_loop: usize,

    /// The maximum number of times to probe for `PullTaskEvent`
    pub max_get_task_probe_times: usize,

    pub compact_task_table_size_partition_threshold_low: u64,
    pub compact_task_table_size_partition_threshold_high: u64,

    pub periodic_scheduling_compaction_group_merge_interval_sec: u64,

    pub compaction_group_merge_dimension_threshold: f64,

    // The private key for the secret store, used when the secret is stored in the meta.
    pub secret_store_private_key: Option<Vec<u8>>,
    /// The path of the temp secret file directory.
    pub temp_secret_file_dir: String,

    // Cluster limits
    pub actor_cnt_per_worker_parallelism_hard_limit: usize,
    pub actor_cnt_per_worker_parallelism_soft_limit: usize,

    pub table_change_log_insert_batch_size: u64,
    pub table_change_log_delete_batch_size: u64,
    pub table_change_log_truncate_interval_sec: u64,

    pub license_key_path: Option<PathBuf>,

    pub compute_client_config: RpcClientConfig,
    pub stream_client_config: RpcClientConfig,
    pub frontend_client_config: RpcClientConfig,
    pub redact_sql_option_keywords: RedactSqlOptionKeywordsRef,

    pub cdc_table_split_init_sleep_interval_splits: u64,
    pub cdc_table_split_init_sleep_duration_millis: u64,
    pub cdc_table_split_init_insert_batch_size: u64,

    pub enable_legacy_table_migration: bool,
    pub pause_on_next_bootstrap_offline: bool,
    pub serverless_backfill_controller_addr: String,
}

impl MetaOpts {
    /// Default opts for testing. Some tests need `enable_recovery=true`
    pub fn test(enable_recovery: bool) -> Self {
        Self {
            enable_recovery,
            clean_all_foreground_jobs_on_recovery: false,
            disable_automatic_parallelism_control: false,
            parallelism_control_batch_size: 1,
            parallelism_control_trigger_period_sec: 10,
            parallelism_control_trigger_first_delay_sec: 30,
            in_flight_barrier_nums: 40,
            snapshot_backfill_finish_max_lagged_barriers: 100,
            snapshot_backfill_barrier_amplification_factor: 1,
            max_idle_ms: 0,
            compaction_deterministic_test: false,
            default_parallelism: DefaultParallelism::Full,
            vacuum_interval_sec: 30,
            time_travel_vacuum_interval_sec: 30,
            time_travel_vacuum_max_version_count: None,
            vacuum_spin_interval_ms: 0,
            iceberg_gc_interval_sec: 3600,
            iceberg_compaction_report_timeout_sec: 30 * 60,
            iceberg_compaction_config_refresh_interval_sec: 60,
            hummock_version_checkpoint_interval_sec: 30,
            enable_hummock_data_archive: false,
            checkpoint_compression_algorithm:
                risingwave_common::config::CheckpointCompression::Zstd,
            checkpoint_read_chunk_size: 128 * 1024 * 1024,
            checkpoint_read_max_in_flight_chunks: 4,
            hummock_time_travel_snapshot_interval: 0,
            hummock_time_travel_sst_info_fetch_batch_size: 10_000,
            hummock_time_travel_sst_info_insert_batch_size: 10,
            hummock_time_travel_epoch_version_insert_batch_size: 1000,
            hummock_time_travel_delta_fetch_batch_size: 1000,
            hummock_gc_history_insert_batch_size: 1000,
            hummock_time_travel_filter_out_objects_batch_size: 1000,
            hummock_time_travel_filter_out_objects_v1: false,
            hummock_time_travel_filter_out_objects_list_version_batch_size: 10,
            hummock_time_travel_filter_out_objects_list_delta_batch_size: 1000,
            min_delta_log_num_for_hummock_version_checkpoint: 1,
            min_sst_retention_time_sec: 3600 * 24 * 7,
            full_gc_interval_sec: 3600 * 24 * 7,
            full_gc_object_limit: 100_000,
            gc_history_retention_time_sec: 3600 * 24 * 7,
            max_inflight_time_travel_query: 1000,
            enable_committed_sst_sanity_check: false,
            periodic_compaction_interval_sec: 300,
            node_num_monitor_interval_sec: 10,
            prometheus_endpoint: None,
            prometheus_selector: None,
            vpc_id: None,
            security_group_id: None,
            privatelink_endpoint_default_tags: None,
            periodic_space_reclaim_compaction_interval_sec: 60,
            telemetry_enabled: false,
            periodic_ttl_reclaim_compaction_interval_sec: 60,
            periodic_tombstone_reclaim_compaction_interval_sec: 60,
            periodic_scheduling_compaction_group_split_interval_sec: 60,
            enable_compaction_group_normalize: false,
            max_normalize_splits_per_round: 4,
            compact_task_table_size_partition_threshold_low: 128 * 1024 * 1024,
            compact_task_table_size_partition_threshold_high: 512 * 1024 * 1024,
            table_high_write_throughput_threshold: 128 * 1024 * 1024,
            table_low_write_throughput_threshold: 64 * 1024 * 1024,
            do_not_config_object_storage_lifecycle: true,
            partition_vnode_count: 32,
            compaction_task_max_heartbeat_interval_secs: 0,
            compaction_task_max_progress_interval_secs: 1,
            compaction_task_id_refill_capacity: 64,
            compaction_config: None,
            hybrid_partition_node_count: 4,
            event_log_enabled: false,
            event_log_channel_max_size: 1,
            advertise_addr: "".to_owned(),
            cached_traces_num: 1,
            cached_traces_memory_limit_bytes: usize::MAX,
            enable_trivial_move: true,
            enable_check_task_level_overlap: true,
            enable_dropped_column_reclaim: false,
            object_store_config: ObjectStoreConfig::default(),
            max_trivial_move_task_count_per_loop: 256,
            max_get_task_probe_times: 5,
            secret_store_private_key: Some(
                hex::decode("0123456789abcdef0123456789abcdef").unwrap(),
            ),
            temp_secret_file_dir: "./secrets".to_owned(),
            actor_cnt_per_worker_parallelism_hard_limit: usize::MAX,
            actor_cnt_per_worker_parallelism_soft_limit: usize::MAX,
            split_group_size_ratio: 0.9,
            table_stat_high_write_throughput_ratio_for_split: 0.5,
            table_stat_low_write_throughput_ratio_for_merge: 0.7,
            table_stat_throuput_window_seconds_for_split: 60,
            table_stat_throuput_window_seconds_for_merge: 240,
            periodic_scheduling_compaction_group_merge_interval_sec: 60 * 10,
            compaction_group_merge_dimension_threshold: 1.2,
            license_key_path: None,
            compute_client_config: RpcClientConfig::default(),
            stream_client_config: RpcClientConfig::default(),
            frontend_client_config: RpcClientConfig::default(),
            redact_sql_option_keywords: Arc::new(Default::default()),
            cdc_table_split_init_sleep_interval_splits: 1000,
            cdc_table_split_init_sleep_duration_millis: 10,
            cdc_table_split_init_insert_batch_size: 1000,
            enable_legacy_table_migration: true,
            refresh_scheduler_interval_sec: 60,
            pause_on_next_bootstrap_offline: false,
            serverless_backfill_controller_addr: String::new(),
            table_change_log_insert_batch_size: 1000,
            table_change_log_delete_batch_size: 1000,
            table_change_log_truncate_interval_sec: 600,
        }
    }
}

impl MetaSrvEnv {
    pub async fn new(
        opts: MetaOpts,
        mut init_system_params: SystemParams,
        session_init: SessionInitConfig,
        meta_store_impl: SqlMetaStore,
    ) -> MetaResult<Self> {
        let idle_manager = Arc::new(IdleManager::new(opts.max_idle_ms));
        let stream_client_pool =
            Arc::new(StreamClientPool::new(1, opts.stream_client_config.clone())); // typically no need for plural clients
        let frontend_client_pool = Arc::new(FrontendClientPool::new(
            1,
            opts.frontend_client_config.clone(),
        ));
        let event_log_manager = Arc::new(start_event_log_manager(
            opts.event_log_enabled,
            opts.event_log_channel_max_size,
        ));

        // When license key path is specified, license key from system parameters can be easily
        // overwritten. So we simply reject this case.
        if opts.license_key_path.is_some()
            && init_system_params.license_key
                != system_param::default::license_key_opt().map(Into::into)
        {
            bail!(
                "argument `--license-key-path` (or env var `RW_LICENSE_KEY_PATH`) and \
                 system parameter `license_key` (or env var `RW_LICENSE_KEY`) may not \
                 be set at the same time"
            );
        }

        let cluster_first_launch = meta_store_impl.up().await.context(
            "Failed to initialize the meta store, \
            this may happen if there's existing metadata incompatible with the current version of RisingWave, \
            e.g., downgrading from a newer release or a nightly build to an older one. \
            For a single-node deployment, you may want to reset all data by deleting the data directory, \
            typically located at `~/.risingwave`.",
        )?;

        let notification_manager =
            Arc::new(NotificationManager::new(meta_store_impl.clone()).await);
        let cluster_id = Cluster::find()
            .one(&meta_store_impl.conn)
            .await?
            .map(|c| c.cluster_id.to_string().into())
            .unwrap();

        // For new clusters:
        // - the name of the object store needs to be prefixed according to the object id.
        //
        // For old clusters
        // - the prefix is ​​not divided for the sake of compatibility.
        init_system_params.use_new_object_prefix_strategy = Some(cluster_first_launch);

        let system_param_controller = Arc::new(
            SystemParamsController::new(
                meta_store_impl.clone(),
                notification_manager.clone(),
                init_system_params,
            )
            .await?,
        );
        let session_param_controller = Arc::new(
            SessionParamsController::new(
                meta_store_impl.clone(),
                notification_manager.clone(),
                session_init,
            )
            .await?,
        );
        Ok(Self {
            id_gen_manager_impl: Arc::new(SqlIdGeneratorManager::new(&meta_store_impl.conn).await?),
            system_param_manager_impl: system_param_controller,
            session_param_manager_impl: session_param_controller,
            meta_store_impl: meta_store_impl.clone(),
            shared_actor_info: SharedActorInfos::new(notification_manager.clone()),
            notification_manager,
            stream_client_pool,
            frontend_client_pool,
            idle_manager,
            event_log_manager,
            cluster_id,
            hummock_seq: Arc::new(SequenceGenerator::new(meta_store_impl.conn.clone())),
            opts: opts.into(),
            // Await trees on the meta node is lightweight, thus always enabled.
            await_tree_reg: await_tree::Registry::new(Default::default()),
            actor_id_generator: Arc::new(AtomicU32::new(0)),
        })
    }

    pub fn meta_store(&self) -> SqlMetaStore {
        self.meta_store_impl.clone()
    }

    pub fn meta_store_ref(&self) -> &SqlMetaStore {
        &self.meta_store_impl
    }

    pub fn id_gen_manager(&self) -> &SqlIdGeneratorManagerRef {
        &self.id_gen_manager_impl
    }

    pub fn notification_manager_ref(&self) -> NotificationManagerRef {
        self.notification_manager.clone()
    }

    pub fn notification_manager(&self) -> &NotificationManager {
        self.notification_manager.deref()
    }

    pub fn idle_manager_ref(&self) -> IdleManagerRef {
        self.idle_manager.clone()
    }

    pub fn idle_manager(&self) -> &IdleManager {
        self.idle_manager.deref()
    }

    pub fn actor_id_generator(&self) -> &AtomicU32 {
        self.actor_id_generator.deref()
    }

    pub async fn system_params_reader(&self) -> SystemParamsReader {
        self.system_param_manager_impl.get_params().await
    }

    pub fn system_params_manager_impl_ref(&self) -> SystemParamsControllerRef {
        self.system_param_manager_impl.clone()
    }

    pub fn session_params_manager_impl_ref(&self) -> SessionParamsControllerRef {
        self.session_param_manager_impl.clone()
    }

    pub fn stream_client_pool_ref(&self) -> StreamClientPoolRef {
        self.stream_client_pool.clone()
    }

    pub fn stream_client_pool(&self) -> &StreamClientPool {
        self.stream_client_pool.deref()
    }

    pub fn frontend_client_pool(&self) -> &FrontendClientPool {
        self.frontend_client_pool.deref()
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn event_log_manager_ref(&self) -> EventLogManagerRef {
        self.event_log_manager.clone()
    }

    pub fn await_tree_reg(&self) -> &await_tree::Registry {
        &self.await_tree_reg
    }

    pub fn shared_actor_infos(&self) -> &SharedActorInfos {
        &self.shared_actor_info
    }
}

#[cfg(any(test, feature = "test"))]
impl MetaSrvEnv {
    // Instance for test.
    pub async fn for_test() -> Self {
        Self::for_test_opts(MetaOpts::test(false), |_| ()).await
    }

    pub async fn for_test_opts(
        opts: MetaOpts,
        on_test_system_params: impl FnOnce(&mut risingwave_pb::meta::PbSystemParams),
    ) -> Self {
        let mut system_params = risingwave_common::system_param::system_params_for_test();
        on_test_system_params(&mut system_params);
        Self::new(
            opts,
            system_params,
            Default::default(),
            SqlMetaStore::for_test().await,
        )
        .await
        .unwrap()
    }
}
