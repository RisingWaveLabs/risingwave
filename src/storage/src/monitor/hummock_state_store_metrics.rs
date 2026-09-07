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

use std::sync::{Arc, OnceLock};

use prometheus::core::{AtomicU64, Collector, Desc, GenericCounter};
use prometheus::{
    Gauge, Histogram, HistogramVec, IntGauge, IntGaugeVec, Opts, Registry, exponential_buckets,
    histogram_opts, proto, register_histogram_vec_with_registry, register_histogram_with_registry,
    register_int_counter_vec_with_registry, register_int_gauge_vec_with_registry,
    register_int_gauge_with_registry,
};
use risingwave_common::config::MetricLevel;
use risingwave_common::metrics::{
    RelabeledCounterVec, RelabeledGuardedHistogramVec, RelabeledGuardedIntCounterVec,
    RelabeledGuardedIntGaugeVec, RelabeledHistogramVec, RelabeledMetricVec, UintGauge,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::{
    register_guarded_histogram_vec_with_registry, register_guarded_int_counter_vec_with_registry,
    register_guarded_int_gauge_vec_with_registry,
};
use thiserror_ext::AsReport;
use tracing::warn;

/// [`HummockStateStoreMetrics`] stores the performance and IO metrics of `XXXStore` such as
/// `RocksDBStateStore` and `TikvStateStore`.
/// In practice, keep in mind that this represents the whole Hummock utilization of
/// a `RisingWave` instance. More granular utilization of per `materialization view`
/// job or an executor should be collected by views like `StateStats` and `JobStats`.
#[derive(Debug, Clone)]
pub struct HummockStateStoreMetrics {
    pub bloom_filter_true_negative_counts: RelabeledGuardedIntCounterVec,
    pub bloom_filter_check_counts: RelabeledGuardedIntCounterVec,
    pub iter_merge_sstable_counts: RelabeledHistogramVec,
    pub vnode_pruning_counts: RelabeledGuardedIntCounterVec,
    pub sst_store_block_request_counts: RelabeledGuardedIntCounterVec,
    pub iter_scan_key_counts: RelabeledGuardedIntCounterVec,
    pub get_shared_buffer_hit_counts: RelabeledCounterVec,
    pub remote_read_time: RelabeledHistogramVec,
    pub iter_fetch_meta_duration: RelabeledGuardedHistogramVec,
    pub iter_fetch_meta_cache_unhits: IntGauge,
    pub iter_slow_fetch_meta_cache_unhits: IntGauge,

    pub vector_object_request_counts: RelabeledGuardedIntCounterVec,
    pub vector_request_stats: RelabeledGuardedHistogramVec,
    pub vector_hnsw_graph_level_node_count: RelabeledGuardedIntGaugeVec,
    pub vector_index_file_count: RelabeledGuardedIntGaugeVec,
    pub vector_index_file_size: RelabeledGuardedIntGaugeVec,

    pub read_req_bloom_filter_positive_counts: RelabeledGuardedIntCounterVec,
    pub read_req_positive_but_non_exist_counts: RelabeledGuardedIntCounterVec,
    pub read_req_check_bloom_filter_counts: RelabeledGuardedIntCounterVec,

    pub write_batch_tuple_counts: RelabeledGuardedIntCounterVec,
    pub write_batch_duration: RelabeledGuardedHistogramVec,
    pub write_batch_size: RelabeledGuardedHistogramVec,

    // spill task counts from unsealed
    pub spill_task_counts_from_unsealed: GenericCounter<AtomicU64>,
    // spill task size from unsealed
    pub spill_task_size_from_unsealed: GenericCounter<AtomicU64>,

    // uploading task
    pub uploader_uploading_task_size: UintGauge,
    pub uploader_uploading_task_count: IntGauge,
    pub uploader_imm_size: UintGauge,
    pub uploader_upload_task_latency: Histogram,
    pub uploader_syncing_epoch_count: IntGauge,
    pub uploader_wait_poll_latency: Histogram,
    pub uploader_per_table_imm_size: RelabeledGuardedIntGaugeVec,
    pub uploader_per_table_imm_count: RelabeledGuardedIntGaugeVec,

    // memory
    pub per_table_imm_size: RelabeledGuardedIntGaugeVec,
    pub per_table_imm_count: RelabeledGuardedIntGaugeVec,
    pub mem_table_spill_counts: RelabeledGuardedIntCounterVec,
    pub old_value_size: RelabeledGuardedIntGaugeVec,

    // block statistics
    pub block_efficiency_histogram: Histogram,

    pub event_handler_pending_event: IntGaugeVec,
    pub event_handler_latency: HistogramVec,

    pub safe_version_hit: GenericCounter<AtomicU64>,
    pub safe_version_miss: GenericCounter<AtomicU64>,
    pub table_change_log_fetch_latency: Histogram,
    pub table_change_log_cache_hit: GenericCounter<AtomicU64>,
    pub table_change_log_cache_miss: GenericCounter<AtomicU64>,
}

pub static GLOBAL_HUMMOCK_STATE_STORE_METRICS: OnceLock<HummockStateStoreMetrics> = OnceLock::new();

pub fn global_hummock_state_store_metrics(metric_level: MetricLevel) -> HummockStateStoreMetrics {
    GLOBAL_HUMMOCK_STATE_STORE_METRICS
        .get_or_init(|| HummockStateStoreMetrics::new(&GLOBAL_METRICS_REGISTRY, metric_level))
        .clone()
}

impl HummockStateStoreMetrics {
    pub fn new(registry: &Registry, metric_level: MetricLevel) -> Self {
        // 10ms ~ max 2.7h
        let time_buckets = exponential_buckets(0.01, 10.0, 7).unwrap();

        // 1ms - 100s
        let state_store_read_time_buckets = exponential_buckets(0.001, 10.0, 5).unwrap();

        let bloom_filter_true_negative_counts = register_guarded_int_counter_vec_with_registry!(
            "state_store_bloom_filter_true_negative_counts",
            "Total number of sstables that have been considered true negative by SST filters",
            &["table_id", "type"],
            registry
        )
        .unwrap();
        let bloom_filter_true_negative_counts = RelabeledMetricVec::with_metric_level(
            MetricLevel::Debug,
            bloom_filter_true_negative_counts,
            metric_level,
        );

        let bloom_filter_check_counts = register_guarded_int_counter_vec_with_registry!(
            "state_store_bloom_filter_check_counts",
            "Total number of read request to check SST filters",
            &["table_id", "type"],
            registry
        )
        .unwrap();
        let bloom_filter_check_counts = RelabeledMetricVec::with_metric_level(
            MetricLevel::Debug,
            bloom_filter_check_counts,
            metric_level,
        );

        // ----- iter -----
        let opts = histogram_opts!(
            "state_store_iter_merge_sstable_counts",
            "Number of child iterators merged into one MergeIterator",
            vec![1.0, 10.0, 100.0, 1000.0, 10000.0]
        );
        let iter_merge_sstable_counts =
            register_histogram_vec_with_registry!(opts, &["table_id", "type"], registry).unwrap();
        let iter_merge_sstable_counts = RelabeledHistogramVec::with_metric_level(
            MetricLevel::Debug,
            iter_merge_sstable_counts,
            metric_level,
        );

        let vnode_pruning_counts = register_guarded_int_counter_vec_with_registry!(
            "state_store_vnode_pruning_counts",
            "Total number of SST pruning operations by vnode key range hints",
            &["table_id", "operation", "result"],
            registry
        )
        .unwrap();

        let vnode_pruning_counts = RelabeledGuardedIntCounterVec::with_metric_level(
            MetricLevel::Debug,
            vnode_pruning_counts,
            metric_level,
        );

        // ----- sst store -----
        let sst_store_block_request_counts = register_guarded_int_counter_vec_with_registry!(
            "state_store_sst_store_block_request_counts",
            "Total number of sst block requests that have been issued to sst store",
            &["table_id", "type"],
            registry
        )
        .unwrap();
        let sst_store_block_request_counts = RelabeledGuardedIntCounterVec::with_metric_level(
            MetricLevel::Info,
            sst_store_block_request_counts,
            metric_level,
        );

        let iter_scan_key_counts = register_guarded_int_counter_vec_with_registry!(
            "state_store_iter_scan_key_counts",
            "Total number of keys read by iterator",
            &["table_id", "type"],
            registry
        )
        .unwrap();
        let iter_scan_key_counts = RelabeledGuardedIntCounterVec::with_metric_level(
            MetricLevel::Info,
            iter_scan_key_counts,
            metric_level,
        );

        let get_shared_buffer_hit_counts = register_int_counter_vec_with_registry!(
            "state_store_get_shared_buffer_hit_counts",
            "Total number of get requests that have been fulfilled by shared buffer",
            &["table_id"],
            registry
        )
        .unwrap();
        let get_shared_buffer_hit_counts = RelabeledCounterVec::with_metric_level(
            MetricLevel::Debug,
            get_shared_buffer_hit_counts,
            metric_level,
        );

        let opts = histogram_opts!(
            "state_store_remote_read_time_per_task",
            "Total time of operations which read from remote storage when enable prefetch",
            time_buckets.clone(),
        );
        let remote_read_time =
            register_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();
        let remote_read_time = RelabeledHistogramVec::with_metric_level(
            MetricLevel::Debug,
            remote_read_time,
            metric_level,
        );

        let opts = histogram_opts!(
            "state_store_iter_fetch_meta_duration",
            "Histogram of iterator fetch SST meta time that have been issued to state store",
            state_store_read_time_buckets.clone(),
        );
        let iter_fetch_meta_duration =
            register_guarded_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();
        let iter_fetch_meta_duration = RelabeledGuardedHistogramVec::with_metric_level(
            MetricLevel::Info,
            iter_fetch_meta_duration,
            metric_level,
        );

        let iter_fetch_meta_cache_unhits = register_int_gauge_with_registry!(
            "state_store_iter_fetch_meta_cache_unhits",
            "Number of SST meta cache unhit during one iterator meta fetch",
            registry
        )
        .unwrap();

        let iter_slow_fetch_meta_cache_unhits = register_int_gauge_with_registry!(
            "state_store_iter_slow_fetch_meta_cache_unhits",
            "Number of SST meta cache unhit during a iterator meta fetch which is slow (costs >5 seconds)",
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "state_store_table_change_log_fetch_latency",
            "Latency of fetching table change logs",
            state_store_read_time_buckets,
        );
        let table_change_log_fetch_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let table_change_log_cache_counts = register_int_counter_vec_with_registry!(
            "state_store_table_change_log_cache_counts",
            "Total number of table change log cache lookups",
            &["result"],
            registry
        )
        .unwrap();
        let table_change_log_cache_hit = table_change_log_cache_counts.with_label_values(&["hit"]);
        let table_change_log_cache_miss =
            table_change_log_cache_counts.with_label_values(&["miss"]);

        // ----- vector -----
        let vector_object_request_counts = register_guarded_int_counter_vec_with_registry!(
            "state_store_vector_object_request_counts",
            "Metrics about vector object requests that have been issued",
            &["table_id", "type", "mode"],
            registry
        )
        .unwrap();
        let vector_object_request_counts = RelabeledGuardedIntCounterVec::with_metric_level(
            MetricLevel::Critical,
            vector_object_request_counts,
            metric_level,
        );

        let opts = histogram_opts!(
            "state_store_vector_request_stats",
            "Metrics about vector requests",
            exponential_buckets(100.0, 10.0, 5).unwrap(),
        );

        let vector_request_stats = register_guarded_histogram_vec_with_registry!(
            opts,
            &["table_id", "type", "mode", "top_n", "ef"],
            registry
        )
        .unwrap();
        let vector_request_stats = RelabeledGuardedHistogramVec::with_metric_level(
            MetricLevel::Critical,
            vector_request_stats,
            metric_level,
        );

        let vector_hnsw_graph_level_node_count = register_guarded_int_gauge_vec_with_registry!(
            "state_store_vector_hnsw_graph_level_node_count",
            "Number of nodes in each level of hnsw graph",
            &["table_id", "level"],
            registry
        )
        .unwrap();
        let vector_hnsw_graph_level_node_count = RelabeledGuardedIntGaugeVec::with_metric_level(
            MetricLevel::Critical,
            vector_hnsw_graph_level_node_count,
            metric_level,
        );

        let vector_index_file_count = register_guarded_int_gauge_vec_with_registry!(
            "state_store_vector_index_file_count",
            "Number of vector file",
            &["table_id"],
            registry
        )
        .unwrap();
        let vector_index_file_count = RelabeledGuardedIntGaugeVec::with_metric_level(
            MetricLevel::Critical,
            vector_index_file_count,
            metric_level,
        );

        let vector_index_file_size = register_guarded_int_gauge_vec_with_registry!(
            "state_store_vector_index_file_size",
            "total size of vector index file",
            &["table_id", "type"],
            registry
        )
        .unwrap();
        let vector_index_file_size = RelabeledGuardedIntGaugeVec::with_metric_level(
            MetricLevel::Critical,
            vector_index_file_size,
            metric_level,
        );

        // ----- write_batch -----
        let write_batch_tuple_counts = register_guarded_int_counter_vec_with_registry!(
            "state_store_write_batch_tuple_counts",
            "Total number of batched write kv pairs requests that have been issued to state store",
            &["table_id"],
            registry
        )
        .unwrap();
        let write_batch_tuple_counts = RelabeledGuardedIntCounterVec::with_metric_level(
            MetricLevel::Debug,
            write_batch_tuple_counts,
            metric_level,
        );

        let opts = histogram_opts!(
            "state_store_write_batch_duration",
            "Total time of batched write that have been issued to state store. With shared buffer on, this is the latency writing to the shared buffer",
            time_buckets.clone()
        );
        let write_batch_duration =
            register_guarded_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();
        let write_batch_duration = RelabeledGuardedHistogramVec::with_metric_level(
            MetricLevel::Debug,
            write_batch_duration,
            metric_level,
        );

        let opts = histogram_opts!(
            "state_store_write_batch_size",
            "Total size of batched write that have been issued to state store",
            exponential_buckets(256.0, 16.0, 7).unwrap() // min 256B ~ max 4GB
        );
        let write_batch_size =
            register_guarded_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();
        let write_batch_size = RelabeledGuardedHistogramVec::with_metric_level(
            MetricLevel::Debug,
            write_batch_size,
            metric_level,
        );

        let spill_task_counts = register_int_counter_vec_with_registry!(
            "state_store_spill_task_counts",
            "Total number of started spill tasks",
            &["uploader_stage"],
            registry
        )
        .unwrap();

        let spill_task_size = register_int_counter_vec_with_registry!(
            "state_store_spill_task_size",
            "Total task of started spill tasks",
            &["uploader_stage"],
            registry
        )
        .unwrap();

        let uploader_uploading_task_size = UintGauge::new(
            "state_store_uploader_uploading_task_size",
            "Total size of uploader uploading tasks",
        )
        .unwrap();
        registry
            .register(Box::new(uploader_uploading_task_size.clone()))
            .unwrap();

        let uploader_uploading_task_count = register_int_gauge_with_registry!(
            "state_store_uploader_uploading_task_count",
            "Total number of uploader uploading tasks",
            registry
        )
        .unwrap();

        let uploader_imm_size = UintGauge::new(
            "state_store_uploader_imm_size",
            "Total size of imms tracked by uploader",
        )
        .unwrap();
        registry
            .register(Box::new(uploader_imm_size.clone()))
            .unwrap();

        let opts = histogram_opts!(
            "state_store_uploader_upload_task_latency",
            "Latency of uploader uploading tasks",
            time_buckets
        );

        let uploader_upload_task_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_uploader_wait_poll_latency",
            "Latency of upload uploading task being polled after finish",
            exponential_buckets(0.001, 5.0, 7).unwrap(), // 1ms - 15s
        );

        let uploader_wait_poll_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let uploader_syncing_epoch_count = register_int_gauge_with_registry!(
            "state_store_uploader_syncing_epoch_count",
            "Total number of syncing epoch",
            registry
        )
        .unwrap();

        let uploader_per_table_imm_size = register_guarded_int_gauge_vec_with_registry!(
            "state_store_uploader_per_table_imm_size",
            "Total uploader-tracked imm size per table",
            &["table_id"],
            registry
        )
        .unwrap();

        let uploader_per_table_imm_size = RelabeledGuardedIntGaugeVec::with_metric_level(
            MetricLevel::Debug,
            uploader_per_table_imm_size,
            metric_level,
        );

        let uploader_per_table_imm_count = register_guarded_int_gauge_vec_with_registry!(
            "state_store_uploader_per_table_imm_count",
            "Total uploader-tracked imm count per table",
            &["table_id"],
            registry
        )
        .unwrap();

        let uploader_per_table_imm_count = RelabeledGuardedIntGaugeVec::with_metric_level(
            MetricLevel::Debug,
            uploader_per_table_imm_count,
            metric_level,
        );

        let per_table_imm_size = register_guarded_int_gauge_vec_with_registry!(
            "state_store_per_table_imm_size",
            "Total imm size per table",
            &["table_id", "fragment_id"],
            registry
        )
        .unwrap();

        let per_table_imm_size = RelabeledGuardedIntGaugeVec::with_metric_level_relabel_n(
            MetricLevel::Debug,
            per_table_imm_size,
            metric_level,
            1,
        );

        let per_table_imm_count = register_guarded_int_gauge_vec_with_registry!(
            "state_store_per_table_imm_count",
            "Total imm count per table",
            &["table_id"],
            registry
        )
        .unwrap();

        let per_table_imm_count = RelabeledGuardedIntGaugeVec::with_metric_level(
            MetricLevel::Debug,
            per_table_imm_count,
            metric_level,
        );

        let read_req_bloom_filter_positive_counts =
            register_guarded_int_counter_vec_with_registry!(
                "state_store_read_req_bloom_filter_positive_counts",
                "Total number of read request with at least one SST filter check returns positive",
                &["table_id", "type"],
                registry
            )
            .unwrap();
        let read_req_bloom_filter_positive_counts =
            RelabeledGuardedIntCounterVec::with_metric_level_relabel_n(
                MetricLevel::Info,
                read_req_bloom_filter_positive_counts,
                metric_level,
                1,
            );

        let read_req_positive_but_non_exist_counts = register_guarded_int_counter_vec_with_registry!(
            "state_store_read_req_positive_but_non_exist_counts",
            "Total number of read request on non-existent key/prefix with at least one SST filter check returns positive",
            &["table_id", "type"],
            registry
        )
        .unwrap();
        let read_req_positive_but_non_exist_counts =
            RelabeledGuardedIntCounterVec::with_metric_level(
                MetricLevel::Info,
                read_req_positive_but_non_exist_counts,
                metric_level,
            );

        let read_req_check_bloom_filter_counts = register_guarded_int_counter_vec_with_registry!(
            "state_store_read_req_check_bloom_filter_counts",
            "Total number of read request that checks an SST filter with a prefix hint",
            &["table_id", "type"],
            registry
        )
        .unwrap();

        let read_req_check_bloom_filter_counts = RelabeledGuardedIntCounterVec::with_metric_level(
            MetricLevel::Info,
            read_req_check_bloom_filter_counts,
            metric_level,
        );

        let mem_table_spill_counts = register_guarded_int_counter_vec_with_registry!(
            "state_store_mem_table_spill_counts",
            "Total number of mem table spill occurs for one table",
            &["table_id"],
            registry
        )
        .unwrap();

        let mem_table_spill_counts = RelabeledGuardedIntCounterVec::with_metric_level(
            MetricLevel::Info,
            mem_table_spill_counts,
            metric_level,
        );

        let old_value_size = register_guarded_int_gauge_vec_with_registry!(
            "state_store_old_value_size",
            "The size of old value",
            &["table_id"],
            registry
        )
        .unwrap();

        let old_value_size = RelabeledGuardedIntGaugeVec::with_metric_level(
            MetricLevel::Info,
            old_value_size,
            metric_level,
        );

        let opts = histogram_opts!(
            "block_efficiency_histogram",
            "Access ratio of in-memory block.",
            exponential_buckets(0.001, 2.0, 11).unwrap(),
        );
        let block_efficiency_histogram = register_histogram_with_registry!(opts, registry).unwrap();

        let event_handler_pending_event = register_int_gauge_vec_with_registry!(
            "state_store_event_handler_pending_event",
            "The number of sent but unhandled events",
            &["event_type"],
            registry,
        )
        .unwrap();

        let opts = histogram_opts!(
            "state_store_event_handler_latency",
            "Latency to handle event",
            exponential_buckets(0.001, 5.0, 7).unwrap(), // 1ms - 15s
        );

        let event_handler_latency =
            register_histogram_vec_with_registry!(opts, &["event_type"], registry).unwrap();

        let safe_version_hit = GenericCounter::new(
            "state_store_safe_version_hit",
            "The total count of a safe version that can be retrieved successfully",
        )
        .unwrap();
        registry
            .register(Box::new(safe_version_hit.clone()))
            .unwrap();

        let safe_version_miss = GenericCounter::new(
            "state_store_safe_version_miss",
            "The total count of a safe version that cannot be retrieved",
        )
        .unwrap();
        registry
            .register(Box::new(safe_version_miss.clone()))
            .unwrap();

        Self {
            bloom_filter_true_negative_counts,
            bloom_filter_check_counts,
            iter_merge_sstable_counts,
            vnode_pruning_counts,
            sst_store_block_request_counts,
            iter_scan_key_counts,
            get_shared_buffer_hit_counts,
            remote_read_time,
            iter_fetch_meta_duration,
            iter_fetch_meta_cache_unhits,
            iter_slow_fetch_meta_cache_unhits,
            vector_object_request_counts,
            vector_request_stats,
            vector_hnsw_graph_level_node_count,
            vector_index_file_count,
            vector_index_file_size,
            read_req_bloom_filter_positive_counts,
            read_req_positive_but_non_exist_counts,
            read_req_check_bloom_filter_counts,
            write_batch_tuple_counts,
            write_batch_duration,
            write_batch_size,
            spill_task_counts_from_unsealed: spill_task_counts.with_label_values(&["unsealed"]),
            spill_task_size_from_unsealed: spill_task_size.with_label_values(&["unsealed"]),
            uploader_uploading_task_size,
            uploader_uploading_task_count,
            uploader_imm_size,
            uploader_upload_task_latency,
            uploader_syncing_epoch_count,
            uploader_wait_poll_latency,
            uploader_per_table_imm_size,
            uploader_per_table_imm_count,
            per_table_imm_size,
            per_table_imm_count,
            mem_table_spill_counts,
            old_value_size,

            block_efficiency_histogram,
            event_handler_pending_event,
            event_handler_latency,
            safe_version_hit,
            safe_version_miss,
            table_change_log_fetch_latency,
            table_change_log_cache_hit,
            table_change_log_cache_miss,
        }
    }

    pub fn unused() -> Self {
        global_hummock_state_store_metrics(MetricLevel::Disabled)
    }
}

pub trait MemoryCollector: Sync + Send {
    fn get_meta_memory_usage(&self) -> u64;
    fn get_data_memory_usage(&self) -> u64;
    fn get_vector_meta_memory_usage(&self) -> u64;
    fn get_vector_data_memory_usage(&self) -> u64;
    fn get_uploading_memory_usage(&self) -> u64;
    fn get_prefetch_memory_usage(&self) -> usize;
    fn get_meta_cache_memory_usage_ratio(&self) -> f64;
    fn get_block_cache_memory_usage_ratio(&self) -> f64;
    fn get_vector_meta_cache_memory_usage_ratio(&self) -> f64;
    fn get_vector_data_cache_memory_usage_ratio(&self) -> f64;
    fn get_shared_buffer_usage_ratio(&self) -> f64;
}

#[derive(Clone)]
struct StateStoreCollector {
    memory_collector: Arc<dyn MemoryCollector>,
    collectors: Vec<Arc<dyn Collector>>,
    block_cache_size: IntGauge,
    meta_cache_size: IntGauge,
    vector_data_cache_size: IntGauge,
    vector_meta_cache_size: IntGauge,
    uploading_memory_size: IntGauge,
    prefetch_memory_size: IntGauge,
    meta_cache_usage_ratio: Gauge,
    block_cache_usage_ratio: Gauge,
    vector_data_cache_usage_ratio: Gauge,
    vector_meta_cache_usage_ratio: Gauge,
    uploading_memory_usage_ratio: Gauge,
}

impl StateStoreCollector {
    pub fn new(memory_collector: Arc<dyn MemoryCollector>) -> Self {
        let mut collectors = Vec::new();

        let block_cache_size = IntGauge::with_opts(Opts::new(
            "state_store_block_cache_size",
            "the size of cache for data block cache",
        ))
        .unwrap();
        collectors.push(Arc::new(block_cache_size.clone()) as _);

        let block_cache_usage_ratio = Gauge::with_opts(Opts::new(
            "state_store_block_cache_usage_ratio",
            "the ratio of block cache to it's pre-allocated memory",
        ))
        .unwrap();
        collectors.push(Arc::new(block_cache_usage_ratio.clone()) as _);

        let meta_cache_size = IntGauge::with_opts(Opts::new(
            "state_store_meta_cache_size",
            "the size of cache for meta file cache",
        ))
        .unwrap();
        collectors.push(Arc::new(meta_cache_size.clone()) as _);

        let meta_cache_usage_ratio = Gauge::with_opts(Opts::new(
            "state_store_meta_cache_usage_ratio",
            "the ratio of meta cache to it's pre-allocated memory",
        ))
        .unwrap();
        collectors.push(Arc::new(meta_cache_usage_ratio.clone()) as _);

        let vector_data_cache_size = IntGauge::with_opts(Opts::new(
            "state_store_vector_data_cache_size",
            "the size of cache for vector data file cache",
        ))
        .unwrap();
        collectors.push(Arc::new(vector_data_cache_size.clone()) as _);

        let vector_data_cache_usage_ratio = Gauge::with_opts(Opts::new(
            "state_store_vector_data_cache_usage_ratio",
            "the ratio of vector data cache to it's pre-allocated memory",
        ))
        .unwrap();
        collectors.push(Arc::new(vector_data_cache_usage_ratio.clone()) as _);

        let vector_meta_cache_size = IntGauge::with_opts(Opts::new(
            "state_store_vector_meta_cache_size",
            "the size of cache for vector meta file cache",
        ))
        .unwrap();
        collectors.push(Arc::new(vector_meta_cache_size.clone()) as _);

        let vector_meta_cache_usage_ratio = Gauge::with_opts(Opts::new(
            "state_store_vector_meta_cache_usage_ratio",
            "the ratio of vector meta cache to it's pre-allocated memory",
        ))
        .unwrap();
        collectors.push(Arc::new(vector_meta_cache_usage_ratio.clone()) as _);

        let uploading_memory_size = IntGauge::with_opts(Opts::new(
            "uploading_memory_size",
            "the size of uploading SSTs memory usage",
        ))
        .unwrap();
        collectors.push(Arc::new(uploading_memory_size.clone()) as _);

        let uploading_memory_usage_ratio = Gauge::with_opts(Opts::new(
            "state_store_uploading_memory_usage_ratio",
            "the ratio of uploading SSTs memory usage to it's pre-allocated memory",
        ))
        .unwrap();
        collectors.push(Arc::new(uploading_memory_usage_ratio.clone()) as _);

        let prefetch_memory_size = IntGauge::with_opts(Opts::new(
            "state_store_prefetch_memory_size",
            "the size of prefetch memory usage",
        ))
        .unwrap();
        collectors.push(Arc::new(prefetch_memory_size.clone()) as _);

        Self {
            memory_collector,
            collectors,
            block_cache_size,
            meta_cache_size,
            vector_data_cache_size,
            vector_meta_cache_size,
            uploading_memory_size,
            prefetch_memory_size,
            meta_cache_usage_ratio,
            block_cache_usage_ratio,

            vector_data_cache_usage_ratio,
            vector_meta_cache_usage_ratio,
            uploading_memory_usage_ratio,
        }
    }
}

impl Collector for StateStoreCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.collectors.iter().flat_map(|c| c.desc()).collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        self.block_cache_size
            .set(self.memory_collector.get_data_memory_usage() as i64);
        self.meta_cache_size
            .set(self.memory_collector.get_meta_memory_usage() as i64);
        self.vector_data_cache_size
            .set(self.memory_collector.get_vector_data_memory_usage() as _);
        self.vector_meta_cache_size
            .set(self.memory_collector.get_vector_meta_memory_usage() as _);
        self.uploading_memory_size
            .set(self.memory_collector.get_uploading_memory_usage() as i64);
        self.prefetch_memory_size
            .set(self.memory_collector.get_prefetch_memory_usage() as i64);
        self.meta_cache_usage_ratio
            .set(self.memory_collector.get_meta_cache_memory_usage_ratio());
        self.block_cache_usage_ratio
            .set(self.memory_collector.get_block_cache_memory_usage_ratio());
        self.vector_meta_cache_usage_ratio.set(
            self.memory_collector
                .get_vector_meta_cache_memory_usage_ratio(),
        );
        self.vector_data_cache_usage_ratio.set(
            self.memory_collector
                .get_vector_data_cache_memory_usage_ratio(),
        );
        self.uploading_memory_usage_ratio
            .set(self.memory_collector.get_shared_buffer_usage_ratio());
        // collect MetricFamilies.
        self.collectors.iter().flat_map(|c| c.collect()).collect()
    }
}

pub fn monitor_cache(memory_collector: Arc<dyn MemoryCollector>) {
    let collector = Box::new(StateStoreCollector::new(memory_collector));
    if let Err(e) = GLOBAL_METRICS_REGISTRY.register(collector) {
        warn!(
            "unable to monitor cache. May have been registered if in all-in-one deployment: {}",
            e.as_report()
        );
    }
}
