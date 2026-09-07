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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;

use prometheus::core::{AtomicF64, GenericGaugeVec};
use prometheus::{
    GaugeVec, Histogram, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Registry,
    exponential_buckets, histogram_opts, register_gauge_vec_with_registry,
    register_histogram_vec_with_registry, register_histogram_with_registry,
    register_int_counter_vec_with_registry, register_int_gauge_vec_with_registry,
    register_int_gauge_with_registry,
};
use risingwave_common::catalog::{FragmentTypeFlag, TableId};
use risingwave_common::config::MetricLevel;
use risingwave_common::metrics::{
    LabelGuardedHistogramVec, LabelGuardedIntCounterVec, LabelGuardedIntGaugeVec,
    LabelGuardedUintGaugeVec, MetricVecRelabelExt, RelabeledMetricVec,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::util::stream_graph_visitor::{
    visit_stream_node_source_backfill, visit_stream_node_stream_scan,
};
use risingwave_common::{
    register_guarded_histogram_vec_with_registry, register_guarded_int_counter_vec_with_registry,
    register_guarded_int_gauge_vec_with_registry, register_guarded_uint_gauge_vec_with_registry,
};
use risingwave_connector::source::monitor::EnumeratorMetrics as SourceEnumeratorMetrics;
use risingwave_meta_model::table::TableType;
use risingwave_meta_model::{ObjectId, WorkerId};
use risingwave_object_store::object::object_metrics::{
    GLOBAL_OBJECT_STORE_METRICS, ObjectStoreMetrics,
};
use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::FragmentDistribution;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use crate::barrier::BarrierManagerRef;
use crate::controller::catalog::CatalogControllerRef;
use crate::controller::cluster::ClusterControllerRef;
use crate::controller::system_param::SystemParamsControllerRef;
use crate::controller::utils::PartialFragmentStateTables;
use crate::hummock::HummockManagerRef;
use crate::manager::MetadataManager;
use crate::rpc::ElectionClientRef;

struct BackfillFragmentInfo {
    job_id: u32,
    fragment_id: u32,
    backfill_state_table_id: u32,
    backfill_target_relation_id: u32,
    backfill_type: &'static str,
    backfill_epoch: u64,
}

#[derive(Clone)]
pub struct MetaMetrics {
    // ********************************** Meta ************************************
    /// The number of workers in the cluster.
    pub worker_num: IntGaugeVec,
    /// The roles of all meta nodes in the cluster.
    pub meta_type: IntGaugeVec,

    // ********************************** gRPC ************************************
    /// gRPC latency of meta services
    pub grpc_latency: HistogramVec,

    // ********************************** Barrier ************************************
    /// The duration from barrier injection to commit
    /// It is the sum of inflight-latency, sync-latency and wait-commit-latency
    pub barrier_latency: LabelGuardedHistogramVec,
    /// The duration from barrier complete to commit
    pub barrier_wait_commit_latency: Histogram,
    /// The number of all barriers. It is the sum of barriers that are in-flight or completed but
    /// waiting for other barriers
    pub all_barrier_nums: LabelGuardedIntGaugeVec,
    /// The number of in-flight barriers
    pub in_flight_barrier_nums: LabelGuardedIntGaugeVec,
    /// The timestamp (UNIX epoch seconds) of the last committed barrier's epoch time.
    pub last_committed_barrier_time: LabelGuardedIntGaugeVec,
    /// The barrier interval of each database
    pub barrier_interval_by_database: GaugeVec,

    // ********************************** Snapshot Backfill ***************************
    /// The barrier latency in second of `table_id` and snapshto backfill `barrier_type`
    pub snapshot_backfill_barrier_latency: LabelGuardedHistogramVec, // (table_id, barrier_type)
    /// The lags between the upstream epoch and the downstream epoch.
    pub snapshot_backfill_lag: LabelGuardedIntGaugeVec, // (table_id, )
    /// The number of inflight barriers of `table_id`
    pub snapshot_backfill_inflight_barrier_num: LabelGuardedIntGaugeVec, // (table_id, _)

    // ********************************** Recovery ************************************
    pub recovery_failure_cnt: IntCounterVec,
    pub recovery_latency: HistogramVec,

    // ********************************** Hummock ************************************
    /// Max committed epoch
    pub max_committed_epoch: IntGauge,
    /// Min committed epoch
    pub min_committed_epoch: IntGauge,
    /// The number of SSTs in each level
    pub level_sst_num: IntGaugeVec,
    /// The number of SSTs to be merged to next level in each level
    pub level_compact_cnt: IntGaugeVec,
    /// The number of compact tasks
    pub compact_frequency: IntCounterVec,
    /// Size of each level
    pub level_file_size: IntGaugeVec,
    /// Hummock version size
    pub version_size: IntGauge,
    /// The version Id of current version.
    pub current_version_id: IntGauge,
    /// The version id of checkpoint version.
    pub checkpoint_version_id: IntGauge,
    /// The smallest version id that is being pinned by worker nodes.
    pub min_pinned_version_id: IntGauge,
    /// The smallest version id that is being guarded by meta node safe points.
    pub min_safepoint_version_id: IntGauge,
    /// Compaction groups that is in write stop state.
    pub write_stop_compaction_groups: IntGaugeVec,
    /// The number of attempts to trigger full GC.
    pub full_gc_trigger_count: IntGauge,
    /// The number of candidate object to delete after scanning object store.
    pub full_gc_candidate_object_count: Histogram,
    /// The number of object to delete after filtering by meta node.
    pub full_gc_selected_object_count: Histogram,
    /// Hummock version stats
    pub version_stats: IntGaugeVec,
    /// Hummock version stats
    pub materialized_view_stats: IntGaugeVec,
    /// Total number of objects that is no longer referenced by versions.
    pub stale_object_count: IntGauge,
    /// Total size of objects that is no longer referenced by versions.
    pub stale_object_size: IntGauge,
    /// Total number of objects that is still referenced by non-current versions.
    pub old_version_object_count: IntGauge,
    /// Total size of objects that is still referenced by non-current versions.
    pub old_version_object_size: IntGauge,
    /// Total number of objects that is referenced by time travel.
    pub time_travel_object_count: IntGauge,
    /// Total number of objects that is referenced by current version.
    pub current_version_object_count: IntGauge,
    /// Total size of objects that is referenced by current version.
    pub current_version_object_size: IntGauge,
    /// Total number of objects that includes dangling objects.
    pub total_object_count: IntGauge,
    /// Total size of objects that includes dangling objects.
    pub total_object_size: IntGauge,
    /// Number of objects per table change log.
    pub table_change_log_object_count: IntGaugeVec,
    /// Size of objects per table change log.
    pub table_change_log_object_size: IntGaugeVec,
    /// Min epoch currently retained in table change log.
    pub table_change_log_min_epoch: IntGaugeVec,
    /// Latency of serving table change log requests.
    pub table_change_log_get_latency: Histogram,
    /// Latency of truncating persisted table change logs.
    pub table_change_log_truncate_latency: Histogram,
    /// The number of hummock version delta log.
    pub delta_log_count: IntGauge,
    /// latency of version checkpoint
    pub version_checkpoint_latency: Histogram,
    /// Latency for hummock manager to acquire lock
    pub hummock_manager_lock_time: HistogramVec,
    /// Latency for hummock manager to really process a request after acquire the lock
    pub hummock_manager_real_process_time: HistogramVec,
    /// The number of compactions from one level to another level that have been skipped
    pub compact_skip_frequency: IntCounterVec,
    /// Bytes of lsm tree needed to reach balance
    pub compact_pending_bytes: IntGaugeVec,
    /// Per level compression ratio
    pub compact_level_compression_ratio: GenericGaugeVec<AtomicF64>,
    /// Per level number of running compaction task
    pub level_compact_task_cnt: IntGaugeVec,
    pub time_after_last_observation: Arc<AtomicU64>,
    pub l0_compact_level_count: HistogramVec,
    pub compact_task_size: HistogramVec,
    pub compact_task_file_count: HistogramVec,
    pub compact_task_batch_count: HistogramVec,
    pub split_compaction_group_count: IntCounterVec,
    pub state_table_count: IntGaugeVec,
    pub branched_sst_count: IntGaugeVec,
    pub compact_task_trivial_move_sst_count: HistogramVec,

    pub compaction_event_consumed_latency: Histogram,
    pub compaction_event_loop_iteration_latency: Histogram,
    pub time_travel_vacuum_metadata_latency: Histogram,
    pub time_travel_write_metadata_latency: Histogram,

    // ********************************** Object Store ************************************
    // Object store related metrics (for backup/restore and version checkpoint)
    pub object_store_metric: Arc<ObjectStoreMetrics>,

    // ********************************** Source ************************************
    /// supervisor for which source is still up.
    pub source_is_up: LabelGuardedIntGaugeVec,
    /// Duration of a source worker `tick` execution (`list_splits` + `on_tick`), in seconds.
    pub source_worker_tick_duration_seconds: LabelGuardedHistogramVec,
    /// Number of source enumerator `on_tick` monitor round-trip failures.
    pub source_enumerator_monitor_error_count: LabelGuardedIntCounterVec,
    pub source_enumerator_metrics: Arc<SourceEnumeratorMetrics>,

    // ********************************** Fragment ************************************
    /// A dummy gauge metrics with its label to be the mapping from actor id to fragment id.
    ///
    /// This is the only metric allowlisted to retain `actor_id` below `Debug`, because dashboards
    /// use it to map actors to fragments and compute nodes and to count actors on each node.
    pub actor_info: IntGaugeVec,
    /// A dummy gauge metrics with its label to be the mapping from table id to actor id
    pub table_info: IntGaugeVec,
    /// A dummy gauge metrics with its label to be the mapping from actor id to sink id
    pub sink_info: RelabeledMetricVec<IntGaugeVec>,
    /// A dummy gauge metrics with its label to be relation info
    pub relation_info: IntGaugeVec,
    /// A dummy gauge metrics with its label to be the mapping from database id to database name
    pub database_info: IntGaugeVec,
    /// Backfill progress per fragment
    pub backfill_fragment_progress: IntGaugeVec,
    /// Max subscription retention configured for the table's changelog.
    pub streaming_table_change_log_retention_seconds: IntGaugeVec,

    // ********************************** System Params ************************************
    /// A dummy gauge metric with labels carrying system parameter info.
    /// Labels: (name, value)
    pub system_param_info: IntGaugeVec,

    /// Write throughput of commit epoch for each stable
    pub table_write_throughput: IntCounterVec,

    /// The number of compaction groups that have been triggered to move
    pub merge_compaction_group_count: IntCounterVec,

    // ********************************** Auto Schema Change ************************************
    pub auto_schema_change_failure_cnt: LabelGuardedIntCounterVec,
    pub auto_schema_change_success_cnt: LabelGuardedIntCounterVec,
    pub auto_schema_change_latency: LabelGuardedHistogramVec,

    pub time_travel_version_replay_latency: Histogram,

    pub compaction_group_count: IntGauge,
    pub compaction_group_size: IntGaugeVec,
    pub compaction_group_file_count: IntGaugeVec,
    pub compaction_group_throughput: IntGaugeVec,

    // ********************************** Refresh Manager ************************************
    pub refresh_job_duration: LabelGuardedUintGaugeVec,
    pub refresh_job_finish_cnt: LabelGuardedIntCounterVec,
    pub refresh_cron_job_trigger_cnt: LabelGuardedIntCounterVec,
    pub refresh_cron_job_miss_cnt: LabelGuardedIntCounterVec,
}

static META_METRICS_LEVEL: OnceLock<MetricLevel> = OnceLock::new();

pub fn init_meta_metrics(metric_level: MetricLevel) {
    META_METRICS_LEVEL.get_or_init(|| metric_level);
}

fn meta_metrics_level() -> MetricLevel {
    META_METRICS_LEVEL
        .get()
        .copied()
        .unwrap_or(MetricLevel::Debug)
}

pub static GLOBAL_META_METRICS: LazyLock<MetaMetrics> =
    LazyLock::new(|| MetaMetrics::new(&GLOBAL_METRICS_REGISTRY, meta_metrics_level()));

fn latency_buckets(max: f64, count: usize) -> Vec<f64> {
    const MIN: f64 = 0.1;

    assert!(count > 1);
    let factor = (max / MIN).powf(1.0 / (count - 1) as f64);
    let mut buckets = exponential_buckets(MIN, factor, count).unwrap();
    *buckets.last_mut().unwrap() = max;
    buckets
}

impl MetaMetrics {
    fn new(registry: &Registry, metric_level: MetricLevel) -> Self {
        let opts = histogram_opts!(
            "meta_grpc_duration_seconds",
            "gRPC latency of meta services",
            exponential_buckets(0.0001, 2.0, 20).unwrap() // max 52s
        );
        let grpc_latency =
            register_histogram_vec_with_registry!(opts, &["path"], registry).unwrap();

        let opts = histogram_opts!(
            "meta_barrier_duration_seconds",
            "barrier latency",
            latency_buckets(600.0, 20)
        );
        let barrier_latency =
            register_guarded_histogram_vec_with_registry!(opts, &["database_id"], registry)
                .unwrap();

        let opts = histogram_opts!(
            "meta_barrier_wait_commit_duration_seconds",
            "barrier_wait_commit_latency",
            latency_buckets(10.0, 10)
        );
        let barrier_wait_commit_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let barrier_interval_by_database = register_gauge_vec_with_registry!(
            "meta_barrier_interval_by_database",
            "barrier interval of each database",
            &["database_id"],
            registry
        )
        .unwrap();

        let all_barrier_nums = register_guarded_int_gauge_vec_with_registry!(
            "all_barrier_nums",
            "num of of all_barrier",
            &["database_id"],
            registry
        )
        .unwrap();
        let in_flight_barrier_nums = register_guarded_int_gauge_vec_with_registry!(
            "in_flight_barrier_nums",
            "num of of in_flight_barrier",
            &["database_id"],
            registry
        )
        .unwrap();
        let last_committed_barrier_time = register_guarded_int_gauge_vec_with_registry!(
            "last_committed_barrier_time",
            "The timestamp (UNIX epoch seconds) of the last committed barrier's epoch time.",
            &["database_id"],
            registry
        )
        .unwrap();

        // snapshot backfill metrics
        let opts = histogram_opts!(
            "meta_snapshot_backfill_barrier_duration_seconds",
            "snapshot backfill barrier latency",
            latency_buckets(600.0, 20)
        );
        let snapshot_backfill_barrier_latency = register_guarded_histogram_vec_with_registry!(
            opts,
            &["table_id", "barrier_type"],
            registry
        )
        .unwrap();

        let snapshot_backfill_lag = register_guarded_int_gauge_vec_with_registry!(
            "meta_snapshot_backfill_upstream_lag",
            "snapshot backfill upstream_lag",
            &["table_id"],
            registry
        )
        .unwrap();
        let snapshot_backfill_inflight_barrier_num = register_guarded_int_gauge_vec_with_registry!(
            "meta_snapshot_backfill_inflight_barrier_num",
            "snapshot backfill inflight_barrier_num",
            &["table_id"],
            registry
        )
        .unwrap();

        let max_committed_epoch = register_int_gauge_with_registry!(
            "storage_max_committed_epoch",
            "max committed epoch",
            registry
        )
        .unwrap();

        let min_committed_epoch = register_int_gauge_with_registry!(
            "storage_min_committed_epoch",
            "min committed epoch",
            registry
        )
        .unwrap();

        let level_sst_num = register_int_gauge_vec_with_registry!(
            "storage_level_sst_num",
            "num of SSTs in each level",
            &["level_index"],
            registry
        )
        .unwrap();

        let level_compact_cnt = register_int_gauge_vec_with_registry!(
            "storage_level_compact_cnt",
            "num of SSTs to be merged to next level in each level",
            &["level_index"],
            registry
        )
        .unwrap();

        let compact_frequency = register_int_counter_vec_with_registry!(
            "storage_level_compact_frequency",
            "The number of compactions from one level to another level that have completed or failed.",
            &["compactor", "group", "task_type", "result"],
            registry
        )
        .unwrap();
        let compact_skip_frequency = register_int_counter_vec_with_registry!(
            "storage_skip_compact_frequency",
            "The number of compactions from one level to another level that have been skipped.",
            &["level", "type"],
            registry
        )
        .unwrap();

        let version_size =
            register_int_gauge_with_registry!("storage_version_size", "version size", registry)
                .unwrap();

        let current_version_id = register_int_gauge_with_registry!(
            "storage_current_version_id",
            "current version id",
            registry
        )
        .unwrap();

        let checkpoint_version_id = register_int_gauge_with_registry!(
            "storage_checkpoint_version_id",
            "checkpoint version id",
            registry
        )
        .unwrap();

        let min_pinned_version_id = register_int_gauge_with_registry!(
            "storage_min_pinned_version_id",
            "min pinned version id",
            registry
        )
        .unwrap();

        let write_stop_compaction_groups = register_int_gauge_vec_with_registry!(
            "storage_write_stop_compaction_groups",
            "compaction groups of write stop state",
            &["compaction_group_id"],
            registry
        )
        .unwrap();

        let full_gc_trigger_count = register_int_gauge_with_registry!(
            "storage_full_gc_trigger_count",
            "the number of attempts to trigger full GC",
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "storage_full_gc_candidate_object_count",
            "the number of candidate object to delete after scanning object store",
            exponential_buckets(1.0, 10.0, 6).unwrap()
        );
        let full_gc_candidate_object_count =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "storage_full_gc_selected_object_count",
            "the number of object to delete after filtering by meta node",
            exponential_buckets(1.0, 10.0, 6).unwrap()
        );
        let full_gc_selected_object_count =
            register_histogram_with_registry!(opts, registry).unwrap();

        let min_safepoint_version_id = register_int_gauge_with_registry!(
            "storage_min_safepoint_version_id",
            "min safepoint version id",
            registry
        )
        .unwrap();

        let level_file_size = register_int_gauge_vec_with_registry!(
            "storage_level_total_file_size",
            "KBs total file bytes in each level",
            &["level_index"],
            registry
        )
        .unwrap();

        let version_stats = register_int_gauge_vec_with_registry!(
            "storage_version_stats",
            "per table stats in current hummock version",
            &["table_id", "metric"],
            registry
        )
        .unwrap();

        let materialized_view_stats = register_int_gauge_vec_with_registry!(
            "storage_materialized_view_stats",
            "per materialized view stats in current hummock version",
            &["table_id", "metric"],
            registry
        )
        .unwrap();

        let stale_object_count = register_int_gauge_with_registry!(
            "storage_stale_object_count",
            "total number of objects that is no longer referenced by versions.",
            registry
        )
        .unwrap();

        let stale_object_size = register_int_gauge_with_registry!(
            "storage_stale_object_size",
            "total size of objects that is no longer referenced by versions.",
            registry
        )
        .unwrap();

        let old_version_object_count = register_int_gauge_with_registry!(
            "storage_old_version_object_count",
            "total number of objects that is still referenced by non-current versions",
            registry
        )
        .unwrap();

        let old_version_object_size = register_int_gauge_with_registry!(
            "storage_old_version_object_size",
            "total size of objects that is still referenced by non-current versions",
            registry
        )
        .unwrap();

        let current_version_object_count = register_int_gauge_with_registry!(
            "storage_current_version_object_count",
            "total number of objects that is referenced by current version",
            registry
        )
        .unwrap();

        let current_version_object_size = register_int_gauge_with_registry!(
            "storage_current_version_object_size",
            "total size of objects that is referenced by current version",
            registry
        )
        .unwrap();

        let total_object_count = register_int_gauge_with_registry!(
            "storage_total_object_count",
            "Total number of objects that includes dangling objects. Note that the metric is updated right before full GC. So subsequent full GC may reduce the actual value significantly, without updating the metric.",
            registry
        ).unwrap();

        let total_object_size = register_int_gauge_with_registry!(
            "storage_total_object_size",
            "Total size of objects that includes dangling objects. Note that the metric is updated right before full GC. So subsequent full GC may reduce the actual value significantly, without updating the metric.",
            registry
        ).unwrap();

        let table_change_log_object_count = register_int_gauge_vec_with_registry!(
            "storage_table_change_log_object_count",
            "per table change log object count",
            &["table_id"],
            registry
        )
        .unwrap();

        let table_change_log_object_size = register_int_gauge_vec_with_registry!(
            "storage_table_change_log_object_size",
            "per table change log object size",
            &["table_id"],
            registry
        )
        .unwrap();

        let table_change_log_min_epoch = register_int_gauge_vec_with_registry!(
            "storage_table_change_log_min_epoch",
            "min epoch currently retained in table change log",
            &["table_id"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "storage_table_change_log_get_latency",
            "latency of serving table change log requests",
            exponential_buckets(0.001, 5.0, 7).unwrap()
        );
        let table_change_log_get_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let table_change_log_truncate_latency = register_histogram_with_registry!(
            "storage_table_change_log_truncate_latency",
            "latency of truncating persisted table change logs",
            registry
        )
        .unwrap();

        let time_travel_object_count = register_int_gauge_with_registry!(
            "storage_time_travel_object_count",
            "total number of objects that is referenced by time travel.",
            registry
        )
        .unwrap();

        let delta_log_count = register_int_gauge_with_registry!(
            "storage_delta_log_count",
            "total number of hummock version delta log",
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "storage_version_checkpoint_latency",
            "hummock version checkpoint latency",
            exponential_buckets(0.1, 1.5, 20).unwrap()
        );
        let version_checkpoint_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "hummock_manager_lock_time",
            "latency for hummock manager to acquire the rwlock",
            exponential_buckets(0.02, 2.5, 10).unwrap() // max 76s
        );
        let hummock_manager_lock_time =
            register_histogram_vec_with_registry!(opts, &["lock_name", "lock_type"], registry)
                .unwrap();

        let opts = histogram_opts!(
            "meta_hummock_manager_real_process_time",
            "latency for hummock manager to really process the request",
            exponential_buckets(0.02, 2.5, 10).unwrap() // max 76s
        );
        let hummock_manager_real_process_time =
            register_histogram_vec_with_registry!(opts, &["method", "lock_name"], registry)
                .unwrap();

        let worker_num = register_int_gauge_vec_with_registry!(
            "worker_num",
            "number of nodes in the cluster",
            &["worker_type"],
            registry,
        )
        .unwrap();

        let meta_type = register_int_gauge_vec_with_registry!(
            "meta_num",
            "role of meta nodes in the cluster",
            &["worker_addr", "role"],
            registry,
        )
        .unwrap();

        let compact_pending_bytes = register_int_gauge_vec_with_registry!(
            "storage_compact_pending_bytes",
            "bytes of lsm tree needed to reach balance",
            &["group"],
            registry
        )
        .unwrap();

        let compact_level_compression_ratio = register_gauge_vec_with_registry!(
            "storage_compact_level_compression_ratio",
            "compression ratio of each level of the lsm tree",
            &["group", "level", "algorithm"],
            registry
        )
        .unwrap();

        let level_compact_task_cnt = register_int_gauge_vec_with_registry!(
            "storage_level_compact_task_cnt",
            "num of compact_task organized by group and level",
            &["task"],
            registry
        )
        .unwrap();

        let time_travel_vacuum_metadata_latency = register_histogram_with_registry!(
            histogram_opts!(
                "storage_time_travel_vacuum_metadata_latency",
                "Latency of vacuuming metadata for time travel",
                exponential_buckets(0.1, 1.5, 20).unwrap()
            ),
            registry
        )
        .unwrap();
        let time_travel_write_metadata_latency = register_histogram_with_registry!(
            histogram_opts!(
                "storage_time_travel_write_metadata_latency",
                "Latency of writing metadata for time travel",
                exponential_buckets(0.1, 1.5, 20).unwrap()
            ),
            registry
        )
        .unwrap();

        let object_store_metric = Arc::new(GLOBAL_OBJECT_STORE_METRICS.clone());

        let recovery_failure_cnt = register_int_counter_vec_with_registry!(
            "recovery_failure_cnt",
            "Number of failed recovery attempts",
            &["recovery_type"],
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "recovery_latency",
            "Latency of the recovery process",
            exponential_buckets(0.1, 1.5, 20).unwrap() // max 221s
        );
        let recovery_latency =
            register_histogram_vec_with_registry!(opts, &["recovery_type"], registry).unwrap();

        let auto_schema_change_failure_cnt = register_guarded_int_counter_vec_with_registry!(
            "auto_schema_change_failure_cnt",
            "Number of failed auto schema change",
            &["table_id", "table_name"],
            registry
        )
        .unwrap();

        let auto_schema_change_success_cnt = register_guarded_int_counter_vec_with_registry!(
            "auto_schema_change_success_cnt",
            "Number of success auto schema change",
            &["table_id", "table_name"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "auto_schema_change_latency",
            "Latency of the auto schema change process",
            exponential_buckets(0.1, 1.5, 20).unwrap() // max 221s
        );
        let auto_schema_change_latency = register_guarded_histogram_vec_with_registry!(
            opts,
            &["table_id", "table_name"],
            registry
        )
        .unwrap();

        let source_is_up = register_guarded_int_gauge_vec_with_registry!(
            "source_status_is_up",
            "source is up or not",
            &["source_id", "source_name"],
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "source_worker_tick_duration_seconds",
            "Duration of a source worker tick (list_splits + on_tick) in seconds",
            exponential_buckets(0.1, 1.5, 20).unwrap() // max ~221s
        );
        let source_worker_tick_duration_seconds = register_guarded_histogram_vec_with_registry!(
            opts,
            &["source_id", "source_name"],
            registry
        )
        .unwrap();
        let source_enumerator_monitor_error_count =
            register_guarded_int_counter_vec_with_registry!(
                "source_enumerator_monitor_error_count",
                "Number of source enumerator on_tick monitor round-trip failures",
                &["source_id", "source_name"],
                registry
            )
            .unwrap();
        let source_enumerator_metrics = Arc::new(SourceEnumeratorMetrics::default());

        let actor_info = register_int_gauge_vec_with_registry!(
            "actor_info",
            "Mapping from actor id to (fragment id, compute node)",
            &["actor_id", "fragment_id", "compute_node"],
            registry
        )
        .unwrap();

        let table_info = register_int_gauge_vec_with_registry!(
            "table_info",
            "Mapping from table id to (actor id, table name)",
            &[
                "materialized_view_id",
                "table_id",
                "fragment_id",
                "table_name",
                "table_type",
                "compaction_group_id"
            ],
            registry
        )
        .unwrap();

        let sink_info = register_int_gauge_vec_with_registry!(
            "sink_info",
            "Mapping from actor id to (actor id, sink name)",
            &["actor_id", "sink_id", "sink_name",],
            registry
        )
        .unwrap()
        .relabel_debug_1(metric_level);

        let relation_info = register_int_gauge_vec_with_registry!(
            "relation_info",
            "Information of the database relation (table/source/sink/materialized view/index/internal)",
            &["id", "database", "schema", "name", "resource_group", "type"],
            registry
        )
        .unwrap();

        let streaming_table_change_log_retention_seconds = register_int_gauge_vec_with_registry!(
            "streaming_table_change_log_retention_seconds",
            "Max subscription retention configured for the table change log in seconds",
            &["table_id"],
            registry
        )
        .unwrap();

        let database_info = register_int_gauge_vec_with_registry!(
            "database_info",
            "Mapping from database id to database name",
            &["database_id", "database_name"],
            registry
        )
        .unwrap();

        let backfill_fragment_progress = register_int_gauge_vec_with_registry!(
            "backfill_fragment_progress",
            "Backfill progress per fragment",
            &[
                "job_id",
                "fragment_id",
                "backfill_state_table_id",
                "backfill_target_relation_id",
                "backfill_target_relation_name",
                "backfill_target_relation_type",
                "backfill_type",
                "backfill_epoch",
                "upstream_type",
                "backfill_progress",
            ],
            registry
        )
        .unwrap();

        let l0_compact_level_count = register_histogram_vec_with_registry!(
            "storage_l0_compact_level_count",
            "level_count of l0 compact task",
            &["group", "type"],
            registry
        )
        .unwrap();

        // System parameter info
        let system_param_info = register_int_gauge_vec_with_registry!(
            "system_param_info",
            "Information of system parameters",
            &["name", "value"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "storage_compact_task_size",
            "Total size of compact that have been issued to state store",
            exponential_buckets(1048576.0, 2.0, 16).unwrap()
        );

        let compact_task_size =
            register_histogram_vec_with_registry!(opts, &["group", "type"], registry).unwrap();

        let compact_task_file_count = register_histogram_vec_with_registry!(
            "storage_compact_task_file_count",
            "file count of compact task",
            &["group", "type"],
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "storage_compact_task_batch_count",
            "count of compact task batch",
            exponential_buckets(1.0, 2.0, 8).unwrap()
        );
        let compact_task_batch_count =
            register_histogram_vec_with_registry!(opts, &["type"], registry).unwrap();

        let table_write_throughput = register_int_counter_vec_with_registry!(
            "storage_commit_write_throughput",
            "The number of compactions from one level to another level that have been skipped.",
            &["table_id"],
            registry
        )
        .unwrap();

        let split_compaction_group_count = register_int_counter_vec_with_registry!(
            "storage_split_compaction_group_count",
            "Count of trigger split compaction group",
            &["group"],
            registry
        )
        .unwrap();

        let state_table_count = register_int_gauge_vec_with_registry!(
            "storage_state_table_count",
            "Count of stable table per compaction group",
            &["group"],
            registry
        )
        .unwrap();

        let branched_sst_count = register_int_gauge_vec_with_registry!(
            "storage_branched_sst_count",
            "Count of branched sst per compaction group",
            &["group"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "storage_compaction_event_consumed_latency",
            "The latency(ms) of each event being consumed",
            exponential_buckets(1.0, 1.5, 30).unwrap() // max 191s
        );
        let compaction_event_consumed_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "storage_compaction_event_loop_iteration_latency",
            "The latency(ms) of each iteration of the compaction event loop",
            exponential_buckets(1.0, 1.5, 30).unwrap() // max 191s
        );
        let compaction_event_loop_iteration_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let merge_compaction_group_count = register_int_counter_vec_with_registry!(
            "storage_merge_compaction_group_count",
            "Count of trigger merge compaction group",
            &["group"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "storage_time_travel_version_replay_latency",
            "The latency(ms) of replaying a hummock version for time travel",
            exponential_buckets(0.01, 10.0, 6).unwrap()
        );
        let time_travel_version_replay_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let compaction_group_count = register_int_gauge_with_registry!(
            "storage_compaction_group_count",
            "The number of compaction groups",
            registry,
        )
        .unwrap();

        let compaction_group_size = register_int_gauge_vec_with_registry!(
            "storage_compaction_group_size",
            "The size of compaction group",
            &["group"],
            registry
        )
        .unwrap();

        let compaction_group_file_count = register_int_gauge_vec_with_registry!(
            "storage_compaction_group_file_count",
            "The file count of compaction group",
            &["group"],
            registry
        )
        .unwrap();

        let compaction_group_throughput = register_int_gauge_vec_with_registry!(
            "storage_compaction_group_throughput",
            "The throughput of compaction group",
            &["group"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "storage_compact_task_trivial_move_sst_count",
            "sst count of compact trivial-move task",
            exponential_buckets(1.0, 2.0, 8).unwrap()
        );
        let compact_task_trivial_move_sst_count =
            register_histogram_vec_with_registry!(opts, &["group"], registry).unwrap();

        let refresh_job_duration = register_guarded_uint_gauge_vec_with_registry!(
            "meta_refresh_job_duration",
            "The duration of refresh job",
            &["table_id", "status"],
            registry
        )
        .unwrap();
        let refresh_job_finish_cnt = register_guarded_int_counter_vec_with_registry!(
            "meta_refresh_job_finish_cnt",
            "The number of finished refresh jobs",
            &["table_id", "status"],
            registry
        )
        .unwrap();
        let refresh_cron_job_trigger_cnt = register_guarded_int_counter_vec_with_registry!(
            "meta_refresh_cron_job_trigger_cnt",
            "The number of cron refresh jobs triggered",
            &["table_id"],
            registry
        )
        .unwrap();
        let refresh_cron_job_miss_cnt = register_guarded_int_counter_vec_with_registry!(
            "meta_refresh_cron_job_miss_cnt",
            "The number of cron refresh jobs missed",
            &["table_id"],
            registry
        )
        .unwrap();

        Self {
            grpc_latency,
            barrier_latency,
            barrier_wait_commit_latency,
            all_barrier_nums,
            in_flight_barrier_nums,
            last_committed_barrier_time,
            barrier_interval_by_database,
            snapshot_backfill_barrier_latency,
            snapshot_backfill_lag,
            snapshot_backfill_inflight_barrier_num,
            recovery_failure_cnt,
            recovery_latency,

            max_committed_epoch,
            min_committed_epoch,
            level_sst_num,
            level_compact_cnt,
            compact_frequency,
            compact_skip_frequency,
            level_file_size,
            version_size,
            version_stats,
            materialized_view_stats,
            stale_object_count,
            stale_object_size,
            old_version_object_count,
            old_version_object_size,
            time_travel_object_count,
            current_version_object_count,
            current_version_object_size,
            total_object_count,
            total_object_size,
            table_change_log_object_count,
            table_change_log_object_size,
            table_change_log_min_epoch,
            table_change_log_get_latency,
            table_change_log_truncate_latency,
            delta_log_count,
            version_checkpoint_latency,
            current_version_id,
            checkpoint_version_id,
            min_pinned_version_id,
            min_safepoint_version_id,
            write_stop_compaction_groups,
            full_gc_trigger_count,
            full_gc_candidate_object_count,
            full_gc_selected_object_count,
            hummock_manager_lock_time,
            hummock_manager_real_process_time,
            time_after_last_observation: Arc::new(AtomicU64::new(0)),
            worker_num,
            meta_type,
            compact_pending_bytes,
            compact_level_compression_ratio,
            level_compact_task_cnt,
            object_store_metric,
            source_is_up,
            source_worker_tick_duration_seconds,
            source_enumerator_monitor_error_count,
            source_enumerator_metrics,
            actor_info,
            table_info,
            sink_info,
            relation_info,
            database_info,
            backfill_fragment_progress,
            streaming_table_change_log_retention_seconds,
            system_param_info,
            l0_compact_level_count,
            compact_task_size,
            compact_task_file_count,
            compact_task_batch_count,
            compact_task_trivial_move_sst_count,
            table_write_throughput,
            split_compaction_group_count,
            state_table_count,
            branched_sst_count,
            compaction_event_consumed_latency,
            compaction_event_loop_iteration_latency,
            auto_schema_change_failure_cnt,
            auto_schema_change_success_cnt,
            auto_schema_change_latency,
            merge_compaction_group_count,
            time_travel_version_replay_latency,
            compaction_group_count,
            compaction_group_size,
            compaction_group_file_count,
            compaction_group_throughput,
            refresh_job_duration,
            refresh_job_finish_cnt,
            refresh_cron_job_trigger_cnt,
            refresh_cron_job_miss_cnt,
            time_travel_vacuum_metadata_latency,
            time_travel_write_metadata_latency,
        }
    }

    #[cfg(test)]
    pub fn for_test(registry: &Registry) -> Self {
        Self::new(registry, MetricLevel::Debug)
    }
}
impl Default for MetaMetrics {
    fn default() -> Self {
        GLOBAL_META_METRICS.clone()
    }
}

/// Refresh `system_param_info` metrics by reading current system parameters.
pub async fn refresh_system_param_info_metrics(
    system_params_controller: &SystemParamsControllerRef,
    meta_metrics: Arc<MetaMetrics>,
) {
    let params_info = system_params_controller.get_params().await.get_all();

    meta_metrics.system_param_info.reset();
    for info in params_info {
        meta_metrics
            .system_param_info
            .with_label_values(&[info.name, &info.value])
            .set(1);
    }
}

pub fn start_worker_info_monitor(
    metadata_manager: MetadataManager,
    election_client: ElectionClientRef,
    interval: Duration,
    meta_metrics: Arc<MetaMetrics>,
) -> (JoinHandle<()>, Sender<()>) {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let mut monitor_interval = tokio::time::interval(interval);
        monitor_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // Wait for interval
                _ = monitor_interval.tick() => {},
                // Shutdown monitor
                _ = &mut shutdown_rx => {
                    tracing::info!("Worker number monitor is stopped");
                    return;
                }
            }

            let node_map = match metadata_manager.count_worker_node().await {
                Ok(node_map) => node_map,
                Err(err) => {
                    tracing::warn!(error = %err.as_report(), "failed to count worker nodes");
                    continue;
                }
            };

            // Reset metrics to clean the stale labels e.g. invalid lease ids
            meta_metrics.worker_num.reset();
            meta_metrics.meta_type.reset();

            for (worker_type, worker_num) in node_map {
                meta_metrics
                    .worker_num
                    .with_label_values(&[(worker_type.as_str_name())])
                    .set(worker_num as i64);
            }
            if let Ok(meta_members) = election_client.get_members().await {
                meta_metrics
                    .worker_num
                    .with_label_values(&[WorkerType::Meta.as_str_name()])
                    .set(meta_members.len() as i64);
                meta_members.into_iter().for_each(|m| {
                    let role = if m.is_leader { "leader" } else { "follower" };
                    meta_metrics
                        .meta_type
                        .with_label_values(&[m.id.as_str(), role])
                        .set(1);
                });
            }
        }
    });

    (join_handle, shutdown_tx)
}

pub async fn refresh_fragment_info_metrics(
    catalog_controller: &CatalogControllerRef,
    cluster_controller: &ClusterControllerRef,
    hummock_manager: &HummockManagerRef,
    meta_metrics: Arc<MetaMetrics>,
) {
    let worker_nodes = match cluster_controller
        .list_workers(Some(WorkerType::ComputeNode.into()), None)
        .await
    {
        Ok(worker_nodes) => worker_nodes,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "failed to list worker nodes");
            return;
        }
    };
    let actor_locations = match catalog_controller.list_actor_locations() {
        Ok(actor_locations) => actor_locations,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "failed to get actor locations");
            return;
        }
    };
    let sink_actor_mapping = match catalog_controller.list_sink_actor_mapping().await {
        Ok(sink_actor_mapping) => sink_actor_mapping,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "failed to get sink actor mappings");
            return;
        }
    };
    let fragment_state_tables = match catalog_controller.list_fragment_state_tables().await {
        Ok(fragment_state_tables) => fragment_state_tables,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "failed to get fragment state tables");
            return;
        }
    };
    let table_name_and_type_mapping = match catalog_controller.get_table_name_type_mapping().await {
        Ok(mapping) => mapping,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "failed to get the table name mapping");
            return;
        }
    };

    let worker_addr_mapping: HashMap<WorkerId, String> = worker_nodes
        .into_iter()
        .map(|worker_node| {
            let addr = match worker_node.host {
                Some(host) => format!("{}:{}", host.host, host.port),
                None => "".to_owned(),
            };
            (worker_node.id, addr)
        })
        .collect();
    let table_compaction_group_id_mapping = hummock_manager
        .get_table_compaction_group_id_mapping()
        .await;

    // Start fresh with a reset to clear all outdated labels. This is safe since we always
    // report full info on each interval.
    meta_metrics.actor_info.reset();
    meta_metrics.table_info.reset();
    meta_metrics.sink_info.reset();
    for actor_location in actor_locations {
        let actor_id_str = actor_location.actor_id.to_string();
        let fragment_id_str = actor_location.fragment_id.to_string();
        // Report a dummy gauge metrics with (fragment id, actor id, node
        // address) as its label
        if let Some(address) = worker_addr_mapping.get(&actor_location.worker_id) {
            meta_metrics
                .actor_info
                .with_label_values(&[&actor_id_str, &fragment_id_str, address])
                .set(1);
        }
    }
    for (sink_id, (sink_name, actor_ids)) in sink_actor_mapping {
        let sink_id_str = sink_id.to_string();
        for actor_id in actor_ids {
            let actor_id_str = actor_id.to_string();
            meta_metrics
                .sink_info
                .with_label_values(&[&actor_id_str, &sink_id_str, &sink_name])
                .set(1);
        }
    }
    for PartialFragmentStateTables {
        fragment_id,
        job_id,
        state_table_ids,
    } in fragment_state_tables
    {
        let fragment_id_str = fragment_id.to_string();
        let job_id_str = job_id.to_string();
        for table_id in state_table_ids.into_inner() {
            let table_id_str = table_id.to_string();
            let (table_name, table_type) = table_name_and_type_mapping
                .get(&table_id)
                .cloned()
                .unwrap_or_else(|| ("unknown".to_owned(), "unknown".to_owned()));
            let compaction_group_id = table_compaction_group_id_mapping
                .get(&table_id)
                .map(|cg_id| cg_id.to_string())
                .unwrap_or_else(|| "unknown".to_owned());
            meta_metrics
                .table_info
                .with_label_values(&[
                    &job_id_str,
                    &table_id_str,
                    &fragment_id_str,
                    &table_name,
                    &table_type,
                    &compaction_group_id,
                ])
                .set(1);
        }
    }
}

pub async fn refresh_relation_info_metrics(
    catalog_controller: &CatalogControllerRef,
    meta_metrics: Arc<MetaMetrics>,
) {
    let table_objects = match catalog_controller.list_table_objects().await {
        Ok(table_objects) => table_objects,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "failed to get table objects");
            return;
        }
    };

    let source_objects = match catalog_controller.list_source_objects().await {
        Ok(source_objects) => source_objects,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "failed to get source objects");
            return;
        }
    };

    let sink_objects = match catalog_controller.list_sink_objects().await {
        Ok(sink_objects) => sink_objects,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "failed to get sink objects");
            return;
        }
    };
    let subscriptions = match catalog_controller.list_subscriptions().await {
        Ok(subscriptions) => subscriptions,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "fail to get subscription objects");
            return;
        }
    };

    meta_metrics.relation_info.reset();
    meta_metrics
        .streaming_table_change_log_retention_seconds
        .reset();

    for (id, db, schema, name, resource_group, table_type) in table_objects {
        let relation_type = match table_type {
            TableType::Table => "table",
            TableType::MaterializedView => "materialized_view",
            TableType::Index | TableType::VectorIndex => "index",
            TableType::Internal => "internal",
        };
        meta_metrics
            .relation_info
            .with_label_values(&[
                &id.to_string(),
                &db,
                &schema,
                &name,
                &resource_group,
                &relation_type.to_owned(),
            ])
            .set(1);
    }

    for (id, db, schema, name, resource_group) in source_objects {
        meta_metrics
            .relation_info
            .with_label_values(&[
                &id.to_string(),
                &db,
                &schema,
                &name,
                &resource_group,
                &"source".to_owned(),
            ])
            .set(1);
    }

    for (id, db, schema, name, resource_group) in sink_objects {
        meta_metrics
            .relation_info
            .with_label_values(&[
                &id.to_string(),
                &db,
                &schema,
                &name,
                &resource_group,
                &"sink".to_owned(),
            ])
            .set(1);
    }

    let mut max_retention_by_table = HashMap::new();
    for subscription in subscriptions {
        max_retention_by_table
            .entry(subscription.dependent_table_id)
            .and_modify(|retention: &mut u64| {
                *retention = (*retention).max(subscription.retention_seconds);
            })
            .or_insert(subscription.retention_seconds);
    }
    for (table_id, retention_seconds) in max_retention_by_table {
        meta_metrics
            .streaming_table_change_log_retention_seconds
            .with_label_values(&[&table_id.to_string()])
            .set(retention_seconds as _);
    }
}

pub async fn refresh_database_info_metrics(
    catalog_controller: &CatalogControllerRef,
    meta_metrics: Arc<MetaMetrics>,
) {
    let databases = match catalog_controller.list_databases().await {
        Ok(databases) => databases,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "failed to get databases");
            return;
        }
    };

    meta_metrics.database_info.reset();

    for db in databases {
        meta_metrics
            .database_info
            .with_label_values(&[&db.id.to_string(), &db.name])
            .set(1);
    }
}

fn extract_backfill_fragment_info(
    distribution: &FragmentDistribution,
) -> Option<BackfillFragmentInfo> {
    let backfill_type =
        if distribution.fragment_type_mask & FragmentTypeFlag::SourceScan as u32 != 0 {
            "SOURCE"
        } else if distribution.fragment_type_mask
            & (FragmentTypeFlag::SnapshotBackfillStreamScan as u32
                | FragmentTypeFlag::CrossDbSnapshotBackfillStreamScan as u32)
            != 0
        {
            "SNAPSHOT_BACKFILL"
        } else if distribution.fragment_type_mask & FragmentTypeFlag::StreamScan as u32 != 0 {
            "ARRANGEMENT_OR_NO_SHUFFLE"
        } else {
            return None;
        };

    let stream_node = distribution.node.as_ref()?;
    let mut info = None;
    match backfill_type {
        "SOURCE" => {
            visit_stream_node_source_backfill(stream_node, |node| {
                info = Some(BackfillFragmentInfo {
                    job_id: distribution.table_id.as_raw_id(),
                    fragment_id: distribution.fragment_id.as_raw_id(),
                    backfill_state_table_id: node
                        .state_table
                        .as_ref()
                        .map(|table| table.id.as_raw_id())
                        .unwrap_or_default(),
                    backfill_target_relation_id: node.upstream_source_id.as_raw_id(),
                    backfill_type,
                    backfill_epoch: 0,
                });
            });
        }
        "SNAPSHOT_BACKFILL" | "ARRANGEMENT_OR_NO_SHUFFLE" => {
            visit_stream_node_stream_scan(stream_node, |node| {
                info = Some(BackfillFragmentInfo {
                    job_id: distribution.table_id.as_raw_id(),
                    fragment_id: distribution.fragment_id.as_raw_id(),
                    backfill_state_table_id: node
                        .state_table
                        .as_ref()
                        .map(|table| table.id.as_raw_id())
                        .unwrap_or_default(),
                    backfill_target_relation_id: node.table_id.as_raw_id(),
                    backfill_type,
                    backfill_epoch: node.snapshot_backfill_epoch.unwrap_or_default(),
                });
            });
        }
        _ => {}
    }

    info
}

pub async fn refresh_backfill_progress_metrics(
    catalog_controller: &CatalogControllerRef,
    hummock_manager: &HummockManagerRef,
    barrier_manager: &BarrierManagerRef,
    meta_metrics: Arc<MetaMetrics>,
) {
    let fragment_descs = match catalog_controller.list_fragment_descs_with_node(true).await {
        Ok(fragment_descs) => fragment_descs,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "failed to list fragment descriptions for creating jobs");
            return;
        }
    };

    let backfill_infos: HashMap<(u32, u32), BackfillFragmentInfo> = fragment_descs
        .iter()
        .filter_map(|(distribution, _)| extract_backfill_fragment_info(distribution))
        .map(|info| ((info.job_id, info.fragment_id), info))
        .collect();

    let fragment_progresses = match barrier_manager.get_fragment_backfill_progress().await {
        Ok(progress) => progress,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "failed to get fragment backfill progress");
            return;
        }
    };

    let progress_by_fragment: HashMap<(u32, u32), _> = fragment_progresses
        .into_iter()
        .map(|progress| {
            (
                (
                    progress.job_id.as_raw_id(),
                    progress.fragment_id.as_raw_id(),
                ),
                progress,
            )
        })
        .collect();

    let version_stats = hummock_manager.get_version_stats().await;

    let relation_ids: HashSet<_> = backfill_infos
        .values()
        .map(|info| ObjectId::new(info.backfill_target_relation_id))
        .collect();
    let relation_objects = match catalog_controller
        .list_relation_objects_by_ids(&relation_ids)
        .await
    {
        Ok(relation_objects) => relation_objects,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "failed to get relation objects");
            return;
        }
    };

    let mut relation_info = HashMap::new();
    for (id, db, schema, name, rel_type) in relation_objects {
        relation_info.insert(id.as_raw_id(), (db, schema, name, rel_type));
    }

    meta_metrics.backfill_fragment_progress.reset();

    for info in backfill_infos.values() {
        let progress = progress_by_fragment.get(&(info.job_id, info.fragment_id));
        let (db, schema, name, rel_type) = relation_info
            .get(&info.backfill_target_relation_id)
            .cloned()
            .unwrap_or_else(|| {
                (
                    "unknown".to_owned(),
                    "unknown".to_owned(),
                    "unknown".to_owned(),
                    "unknown".to_owned(),
                )
            });

        let job_id_str = info.job_id.to_string();
        let fragment_id_str = info.fragment_id.to_string();
        let backfill_state_table_id_str = info.backfill_state_table_id.to_string();
        let backfill_target_relation_id_str = info.backfill_target_relation_id.to_string();
        let backfill_target_relation_name_str = format!("{db}.{schema}.{name}");
        let backfill_target_relation_type_str = rel_type;
        let backfill_type_str = info.backfill_type.to_owned();
        let backfill_epoch_str = info.backfill_epoch.to_string();
        let total_key_count = version_stats
            .table_stats
            .get(&TableId::new(info.backfill_target_relation_id))
            .map(|stats| stats.total_key_count as u64);

        let progress_label = match (info.backfill_type, progress) {
            ("SOURCE", Some(progress)) => format!("{} consumed rows", progress.consumed_rows),
            ("SOURCE", None) => "0 consumed rows".to_owned(),
            (_, Some(progress)) if progress.done => {
                let total = total_key_count.unwrap_or(0);
                format!("100.0000% ({}/{})", total, total)
            }
            (_, Some(progress)) => {
                let total = total_key_count.unwrap_or(0);
                if total == 0 {
                    "100.0000% (0/0)".to_owned()
                } else {
                    let raw = (progress.consumed_rows as f64) / (total as f64) * 100.0;
                    format!(
                        "{:.4}% ({}/{})",
                        raw.min(100.0),
                        progress.consumed_rows,
                        total
                    )
                }
            }
            (_, None) => "0.0000% (0/0)".to_owned(),
        };
        let upstream_type_str = progress
            .map(|progress| progress.upstream_type.to_string())
            .unwrap_or_else(|| "Unknown".to_owned());

        meta_metrics
            .backfill_fragment_progress
            .with_label_values(&[
                &job_id_str,
                &fragment_id_str,
                &backfill_state_table_id_str,
                &backfill_target_relation_id_str,
                &backfill_target_relation_name_str,
                &backfill_target_relation_type_str,
                &backfill_type_str,
                &backfill_epoch_str,
                &upstream_type_str,
                &progress_label,
            ])
            .set(1);
    }
}

pub fn start_info_monitor(
    metadata_manager: MetadataManager,
    hummock_manager: HummockManagerRef,
    barrier_manager: BarrierManagerRef,
    system_params_controller: SystemParamsControllerRef,
    meta_metrics: Arc<MetaMetrics>,
) -> (JoinHandle<()>, Sender<()>) {
    const COLLECT_INTERVAL_SECONDS: u64 = 60;

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let mut monitor_interval =
            tokio::time::interval(Duration::from_secs(COLLECT_INTERVAL_SECONDS));
        monitor_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // Wait for interval
                _ = monitor_interval.tick() => {},
                // Shutdown monitor
                _ = &mut shutdown_rx => {
                    tracing::info!("Meta info monitor is stopped");
                    return;
                }
            }

            // Fragment and relation info
            refresh_fragment_info_metrics(
                &metadata_manager.catalog_controller,
                &metadata_manager.cluster_controller,
                &hummock_manager,
                meta_metrics.clone(),
            )
            .await;

            refresh_relation_info_metrics(
                &metadata_manager.catalog_controller,
                meta_metrics.clone(),
            )
            .await;

            refresh_database_info_metrics(
                &metadata_manager.catalog_controller,
                meta_metrics.clone(),
            )
            .await;

            refresh_backfill_progress_metrics(
                &metadata_manager.catalog_controller,
                &hummock_manager,
                &barrier_manager,
                meta_metrics.clone(),
            )
            .await;

            // System parameter info
            refresh_system_param_info_metrics(&system_params_controller, meta_metrics.clone())
                .await;
        }
    });

    (join_handle, shutdown_tx)
}

#[cfg(test)]
mod tests {
    use risingwave_common::metrics::get_label;

    use super::*;

    fn actor_id(registry: &Registry, metric_name: &str) -> String {
        let metric_family = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == metric_name)
            .unwrap();
        get_label(metric_family.get_metric().first().unwrap(), "actor_id").unwrap()
    }

    #[test]
    fn actor_info_is_the_only_meta_actor_id_allowlist() {
        for level in [MetricLevel::Critical, MetricLevel::Info] {
            let registry = Registry::new();
            let metrics = MetaMetrics::new(&registry, level);

            metrics
                .actor_info
                .with_label_values(&["11", "22", "compute-0"])
                .set(1);
            metrics
                .sink_info
                .with_label_values(&["11", "33", "sink"])
                .set(1);

            assert_eq!(actor_id(&registry, "actor_info"), "11");
            assert_eq!(actor_id(&registry, "sink_info"), "");
        }
    }

    #[test]
    fn debug_meta_metrics_retain_actor_id() {
        let registry = Registry::new();
        let metrics = MetaMetrics::new(&registry, MetricLevel::Debug);

        metrics
            .sink_info
            .with_label_values(&["11", "33", "sink"])
            .set(1);

        assert_eq!(actor_id(&registry, "sink_info"), "11");
    }
}
