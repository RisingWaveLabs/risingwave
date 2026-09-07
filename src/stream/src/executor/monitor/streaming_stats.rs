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

use std::sync::OnceLock;

use prometheus::{
    Histogram, IntCounter, IntGauge, Registry, exponential_buckets, histogram_opts,
    register_histogram_with_registry, register_int_counter_with_registry,
    register_int_gauge_with_registry,
};
use risingwave_common::catalog::TableId;
use risingwave_common::config::MetricLevel;
use risingwave_common::metrics::{
    LabelGuardedHistogramVec, LabelGuardedIntCounter, LabelGuardedIntCounterVec,
    LabelGuardedIntGauge, LabelGuardedIntGaugeVec, MetricVecRelabelExt,
    RelabeledGuardedHistogramVec, RelabeledGuardedIntCounterVec, RelabeledGuardedIntGaugeVec,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::monitor::in_mem::CountMap;
use risingwave_common::{
    register_guarded_histogram_vec_with_registry, register_guarded_int_counter_vec_with_registry,
    register_guarded_int_gauge_vec_with_registry,
};
use risingwave_connector::sink::catalog::SinkId;
use risingwave_pb::id::ExecutorId;

use crate::common::log_store_impl::kv_log_store::{
    REWIND_BACKOFF_MULTIPLIER, REWIND_INITIAL_DELAY, REWIND_MAX_DELAY,
};
use crate::executor::prelude::ActorId;
use crate::task::FragmentId;

#[derive(Clone)]
pub struct StreamingMetrics {
    pub level: MetricLevel,

    // Executor metrics (disabled by default)
    pub executor_row_count: RelabeledGuardedIntCounterVec,

    // Profiling Metrics:
    // Aggregated per operator rather than per actor.
    // These are purely in-memory, never collected by prometheus.
    pub mem_stream_node_output_row_count: CountMap<ExecutorId>,
    pub mem_stream_node_output_blocking_duration_ns: CountMap<ExecutorId>,

    // Streaming actor metrics from tokio (disabled by default)
    actor_scheduled_duration: RelabeledGuardedIntCounterVec,
    actor_scheduled_cnt: RelabeledGuardedIntCounterVec,
    actor_poll_duration: RelabeledGuardedIntCounterVec,
    actor_poll_cnt: RelabeledGuardedIntCounterVec,
    actor_idle_duration: RelabeledGuardedIntCounterVec,
    actor_idle_cnt: RelabeledGuardedIntCounterVec,

    // Streaming actor
    pub actor_count: LabelGuardedIntGaugeVec,
    pub actor_in_record_cnt: RelabeledGuardedIntCounterVec,
    pub actor_out_record_cnt: RelabeledGuardedIntCounterVec,
    pub fragment_channel_buffered_bytes: LabelGuardedIntGaugeVec,
    pub actor_current_epoch: RelabeledGuardedIntGaugeVec,
    pub project_expr_inflight_window_size: RelabeledGuardedIntGaugeVec,

    // Source
    pub source_output_row_count: RelabeledGuardedIntCounterVec,
    pub source_split_change_count: RelabeledGuardedIntCounterVec,
    pub source_backfill_row_count: RelabeledGuardedIntCounterVec,

    // Sink
    sink_input_row_count: RelabeledGuardedIntCounterVec,
    sink_input_bytes: RelabeledGuardedIntCounterVec,
    sink_chunk_buffer_size: RelabeledGuardedIntGaugeVec,

    // Exchange (see also `compute::ExchangeServiceMetrics`)
    pub exchange_frag_recv_size: LabelGuardedIntCounterVec,

    // Streaming Merge (We breakout this metric from `barrier_align_duration` because
    // the alignment happens on different levels)
    pub merge_barrier_align_duration: RelabeledGuardedIntCounterVec,

    // Backpressure
    pub actor_output_buffer_blocking_duration_ns: RelabeledGuardedIntCounterVec,
    actor_input_buffer_blocking_duration_ns: RelabeledGuardedIntCounterVec,

    // Streaming Join
    pub join_lookup_miss_count: RelabeledGuardedIntCounterVec,
    pub join_lookup_total_count: RelabeledGuardedIntCounterVec,
    pub join_insert_cache_miss_count: RelabeledGuardedIntCounterVec,
    pub join_actor_input_waiting_duration_ns: RelabeledGuardedIntCounterVec,
    pub join_match_duration_ns: RelabeledGuardedIntCounterVec,
    pub join_cached_entry_count: RelabeledGuardedIntGaugeVec,
    pub join_matched_join_keys: RelabeledGuardedHistogramVec,

    // Streaming Join, Streaming Dynamic Filter and Streaming Union
    pub barrier_align_duration: RelabeledGuardedIntCounterVec,

    // Streaming Aggregation
    agg_lookup_miss_count: RelabeledGuardedIntCounterVec,
    agg_total_lookup_count: RelabeledGuardedIntCounterVec,
    agg_cached_entry_count: RelabeledGuardedIntGaugeVec,
    agg_chunk_lookup_miss_count: RelabeledGuardedIntCounterVec,
    agg_chunk_total_lookup_count: RelabeledGuardedIntCounterVec,
    agg_dirty_groups_count: RelabeledGuardedIntGaugeVec,
    agg_dirty_groups_heap_size: RelabeledGuardedIntGaugeVec,
    agg_distinct_cache_miss_count: RelabeledGuardedIntCounterVec,
    agg_distinct_total_cache_count: RelabeledGuardedIntCounterVec,
    agg_distinct_cached_entry_count: RelabeledGuardedIntGaugeVec,
    agg_state_cache_lookup_count: RelabeledGuardedIntCounterVec,
    agg_state_cache_miss_count: RelabeledGuardedIntCounterVec,

    // Streaming TopN
    group_top_n_cache_miss_count: RelabeledGuardedIntCounterVec,
    group_top_n_total_query_cache_count: RelabeledGuardedIntCounterVec,
    group_top_n_cached_entry_count: RelabeledGuardedIntGaugeVec,
    // TODO(rc): why not just use the above three?
    group_top_n_appendonly_cache_miss_count: RelabeledGuardedIntCounterVec,
    group_top_n_appendonly_total_query_cache_count: RelabeledGuardedIntCounterVec,
    group_top_n_appendonly_cached_entry_count: RelabeledGuardedIntGaugeVec,

    // Lookup executor
    lookup_cache_miss_count: RelabeledGuardedIntCounterVec,
    lookup_total_query_cache_count: RelabeledGuardedIntCounterVec,
    lookup_cached_entry_count: RelabeledGuardedIntGaugeVec,

    // temporal join
    temporal_join_cache_miss_count: RelabeledGuardedIntCounterVec,
    temporal_join_total_query_cache_count: RelabeledGuardedIntCounterVec,
    temporal_join_cached_entry_count: RelabeledGuardedIntGaugeVec,

    // Backfill
    backfill_snapshot_read_row_count: RelabeledGuardedIntCounterVec,
    backfill_upstream_output_row_count: RelabeledGuardedIntCounterVec,

    // CDC Backfill
    cdc_backfill_snapshot_read_row_count: RelabeledGuardedIntCounterVec,
    cdc_backfill_upstream_output_row_count: RelabeledGuardedIntCounterVec,

    // Snapshot Backfill
    pub(crate) snapshot_backfill_consume_row_count: RelabeledGuardedIntCounterVec,

    // Over Window
    over_window_cached_entry_count: RelabeledGuardedIntGaugeVec,
    over_window_cache_lookup_count: RelabeledGuardedIntCounterVec,
    over_window_cache_miss_count: RelabeledGuardedIntCounterVec,
    over_window_range_cache_entry_count: RelabeledGuardedIntGaugeVec,
    over_window_range_cache_lookup_count: RelabeledGuardedIntCounterVec,
    over_window_range_cache_left_miss_count: RelabeledGuardedIntCounterVec,
    over_window_range_cache_right_miss_count: RelabeledGuardedIntCounterVec,
    over_window_accessed_entry_count: RelabeledGuardedIntCounterVec,
    over_window_compute_count: RelabeledGuardedIntCounterVec,
    over_window_same_output_count: RelabeledGuardedIntCounterVec,

    /// The duration from receipt of barrier to all actors collection.
    /// The max of all nodes' `barrier_inflight_latency` for a partial graph is the latency for a
    /// barrier to flow through that partial graph.
    pub barrier_inflight_latency: LabelGuardedHistogramVec,
    /// The duration of sync to storage.
    pub barrier_sync_latency: LabelGuardedHistogramVec,
    pub barrier_batch_size: Histogram,
    /// The progress made by the earliest in-flight barriers in the local barrier manager.
    pub barrier_manager_progress: LabelGuardedIntCounterVec,

    pub kv_log_store_storage_write_count: RelabeledGuardedIntCounterVec,
    pub kv_log_store_storage_write_size: RelabeledGuardedIntCounterVec,
    pub kv_log_store_rewind_count: RelabeledGuardedIntCounterVec,
    pub kv_log_store_rewind_delay: RelabeledGuardedHistogramVec,
    pub kv_log_store_storage_read_count: RelabeledGuardedIntCounterVec,
    pub kv_log_store_storage_read_size: RelabeledGuardedIntCounterVec,
    pub kv_log_store_buffer_unconsumed_item_count: RelabeledGuardedIntGaugeVec,
    pub kv_log_store_buffer_unconsumed_row_count: RelabeledGuardedIntGaugeVec,
    pub kv_log_store_buffer_unconsumed_epoch_count: RelabeledGuardedIntGaugeVec,
    pub kv_log_store_buffer_unconsumed_min_epoch: RelabeledGuardedIntGaugeVec,
    pub kv_log_store_buffer_memory_bytes: RelabeledGuardedIntGaugeVec,

    pub crossdb_last_consumed_min_epoch: RelabeledGuardedIntGaugeVec,

    pub sync_kv_log_store_read_count: RelabeledGuardedIntCounterVec,
    pub sync_kv_log_store_read_size: RelabeledGuardedIntCounterVec,
    pub sync_kv_log_store_write_pause_duration_ns: RelabeledGuardedIntCounterVec,
    pub sync_kv_log_store_state: RelabeledGuardedIntCounterVec,
    pub sync_kv_log_store_wait_next_poll_ns: RelabeledGuardedIntCounterVec,
    pub sync_kv_log_store_storage_write_count: RelabeledGuardedIntCounterVec,
    pub sync_kv_log_store_storage_write_size: RelabeledGuardedIntCounterVec,
    pub sync_kv_log_store_buffer_unconsumed_item_count: RelabeledGuardedIntGaugeVec,
    pub sync_kv_log_store_buffer_unconsumed_row_count: RelabeledGuardedIntGaugeVec,
    pub sync_kv_log_store_buffer_unconsumed_epoch_count: RelabeledGuardedIntGaugeVec,
    pub sync_kv_log_store_buffer_unconsumed_min_epoch: RelabeledGuardedIntGaugeVec,
    pub sync_kv_log_store_buffer_memory_bytes: RelabeledGuardedIntGaugeVec,

    // Memory management
    pub lru_runtime_loop_count: IntCounter,
    pub lru_latest_sequence: IntGauge,
    pub lru_watermark_sequence: IntGauge,
    pub lru_eviction_policy: IntGauge,
    pub jemalloc_allocated_bytes: IntGauge,
    pub jemalloc_active_bytes: IntGauge,
    pub jemalloc_resident_bytes: IntGauge,
    pub jemalloc_metadata_bytes: IntGauge,
    pub jvm_allocated_bytes: IntGauge,
    pub jvm_active_bytes: IntGauge,
    pub stream_memory_usage: RelabeledGuardedIntGaugeVec,

    // Materialized view
    materialize_cache_hit_count: RelabeledGuardedIntCounterVec,
    materialize_data_exist_count: RelabeledGuardedIntCounterVec,
    materialize_cache_total_count: RelabeledGuardedIntCounterVec,
    materialize_input_row_count: RelabeledGuardedIntCounterVec,
    pub materialize_current_epoch: RelabeledGuardedIntGaugeVec,

    // PostgreSQL CDC LSN monitoring
    pub pg_cdc_state_table_lsn: LabelGuardedIntGaugeVec,
    pub pg_cdc_jni_commit_offset_lsn: LabelGuardedIntGaugeVec,

    // MySQL CDC binlog monitoring
    pub mysql_cdc_state_binlog_file_seq: LabelGuardedIntGaugeVec,
    pub mysql_cdc_state_binlog_position: LabelGuardedIntGaugeVec,

    // SQL Server CDC LSN monitoring
    pub sqlserver_cdc_state_change_lsn: LabelGuardedIntGaugeVec,
    pub sqlserver_cdc_state_commit_lsn: LabelGuardedIntGaugeVec,
    pub sqlserver_cdc_jni_commit_offset_lsn: LabelGuardedIntGaugeVec,

    // Gap Fill
    pub gap_fill_generated_rows_count: RelabeledGuardedIntCounterVec,

    // State Table
    pub state_table_iter_count: RelabeledGuardedIntCounterVec,
    pub state_table_get_count: RelabeledGuardedIntCounterVec,
    pub state_table_iter_vnode_pruned_count: RelabeledGuardedIntCounterVec,
    pub state_table_get_vnode_pruned_count: RelabeledGuardedIntCounterVec,
}

pub static GLOBAL_STREAMING_METRICS: OnceLock<StreamingMetrics> = OnceLock::new();

fn latency_buckets(max: f64, count: usize) -> Vec<f64> {
    const MIN: f64 = 0.1;

    assert!(count > 1);
    let factor = (max / MIN).powf(1.0 / (count - 1) as f64);
    let mut buckets = exponential_buckets(MIN, factor, count).unwrap();
    *buckets.last_mut().unwrap() = max;
    buckets
}

pub fn global_streaming_metrics(metric_level: MetricLevel) -> StreamingMetrics {
    GLOBAL_STREAMING_METRICS
        .get_or_init(|| StreamingMetrics::new(&GLOBAL_METRICS_REGISTRY, metric_level))
        .clone()
}

impl StreamingMetrics {
    pub fn new(registry: &Registry, level: MetricLevel) -> Self {
        let executor_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_executor_row_count",
            "Total number of rows that have been output from each executor",
            &["actor_id", "fragment_id", "executor_identity"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let stream_node_output_row_count = CountMap::new();
        let stream_node_output_blocking_duration_ns = CountMap::new();

        let source_output_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_source_output_rows_counts",
            "Total number of rows that have been output from source",
            &["actor_id", "source_id", "source_name", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let source_split_change_count = register_guarded_int_counter_vec_with_registry!(
            "stream_source_split_change_event_count",
            "Total number of split change events that have been operated by source",
            &["actor_id", "source_id", "source_name", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let source_backfill_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_source_backfill_rows_counts",
            "Total number of rows that have been backfilled for source",
            &["actor_id", "source_id", "source_name", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let sink_input_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_sink_input_row_count",
            "Total number of rows streamed into sink executors",
            &["actor_id", "sink_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let sink_input_bytes = register_guarded_int_counter_vec_with_registry!(
            "stream_sink_input_bytes",
            "Total size of chunks streamed into sink executors",
            &["actor_id", "sink_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let materialize_input_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_mview_input_row_count",
            "Total number of rows streamed into materialize executors",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let materialize_current_epoch = register_guarded_int_gauge_vec_with_registry!(
            "stream_mview_current_epoch",
            "The current epoch of the materialized executor",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let pg_cdc_state_table_lsn = register_guarded_int_gauge_vec_with_registry!(
            "stream_pg_cdc_state_table_lsn",
            "Current LSN value stored in PostgreSQL CDC state table",
            &["source_id"],
            registry,
        )
        .unwrap();

        let pg_cdc_jni_commit_offset_lsn = register_guarded_int_gauge_vec_with_registry!(
            "stream_pg_cdc_jni_commit_offset_lsn",
            "LSN value when JNI commit offset is called for PostgreSQL CDC",
            &["source_id"],
            registry,
        )
        .unwrap();

        let mysql_cdc_state_binlog_file_seq = register_guarded_int_gauge_vec_with_registry!(
            "stream_mysql_cdc_state_binlog_file_seq",
            "Current binlog file sequence number stored in MySQL CDC state table",
            &["source_id"],
            registry,
        )
        .unwrap();

        let mysql_cdc_state_binlog_position = register_guarded_int_gauge_vec_with_registry!(
            "stream_mysql_cdc_state_binlog_position",
            "Current binlog position stored in MySQL CDC state table",
            &["source_id"],
            registry,
        )
        .unwrap();

        let sqlserver_cdc_state_change_lsn = register_guarded_int_gauge_vec_with_registry!(
            "stream_sqlserver_cdc_state_change_lsn",
            "Current change_lsn value stored in SQL Server CDC state table",
            &["source_id"],
            registry,
        )
        .unwrap();

        let sqlserver_cdc_state_commit_lsn = register_guarded_int_gauge_vec_with_registry!(
            "stream_sqlserver_cdc_state_commit_lsn",
            "Current commit_lsn value stored in SQL Server CDC state table",
            &["source_id"],
            registry,
        )
        .unwrap();

        let sqlserver_cdc_jni_commit_offset_lsn = register_guarded_int_gauge_vec_with_registry!(
            "stream_sqlserver_cdc_jni_commit_offset_lsn",
            "LSN value when JNI commit offset is called for SQL Server CDC",
            &["source_id"],
            registry,
        )
        .unwrap();

        let sink_chunk_buffer_size = register_guarded_int_gauge_vec_with_registry!(
            "stream_sink_chunk_buffer_size",
            "Total size of chunks buffered in a barrier",
            &["actor_id", "sink_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);
        let actor_output_buffer_blocking_duration_ns =
            register_guarded_int_counter_vec_with_registry!(
                "stream_actor_output_buffer_blocking_duration_ns",
                "Total blocking duration (ns) of output buffer",
                &["actor_id", "fragment_id", "downstream_fragment_id"],
                registry
            )
            .unwrap()
            // mask the first label `actor_id` if the level is less verbose than `Debug`
            .relabel_debug_1(level);

        let actor_input_buffer_blocking_duration_ns =
            register_guarded_int_counter_vec_with_registry!(
                "stream_actor_input_buffer_blocking_duration_ns",
                "Total blocking duration (ns) of input buffer",
                &["actor_id", "fragment_id", "upstream_fragment_id"],
                registry
            )
            .unwrap()
            // mask the first label `actor_id` if the level is less verbose than `Debug`
            .relabel_debug_1(level);

        let fragment_channel_buffered_bytes = register_guarded_int_gauge_vec_with_registry!(
            "stream_fragment_channel_buffered_bytes",
            "Estimated buffered bytes for actor channels by fragment",
            &["fragment_id"],
            registry
        )
        .unwrap();

        let exchange_frag_recv_size = register_guarded_int_counter_vec_with_registry!(
            "stream_exchange_frag_recv_size",
            "Total size of messages that have been received from upstream Fragment",
            &["up_fragment_id", "down_fragment_id"],
            registry
        )
        .unwrap();

        let actor_poll_duration = register_guarded_int_counter_vec_with_registry!(
            "stream_actor_poll_duration",
            "tokio's metrics",
            &["actor_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let actor_poll_cnt = register_guarded_int_counter_vec_with_registry!(
            "stream_actor_poll_cnt",
            "tokio's metrics",
            &["actor_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let actor_scheduled_duration = register_guarded_int_counter_vec_with_registry!(
            "stream_actor_scheduled_duration",
            "tokio's metrics",
            &["actor_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let actor_scheduled_cnt = register_guarded_int_counter_vec_with_registry!(
            "stream_actor_scheduled_cnt",
            "tokio's metrics",
            &["actor_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let actor_idle_duration = register_guarded_int_counter_vec_with_registry!(
            "stream_actor_idle_duration",
            "tokio's metrics",
            &["actor_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let actor_idle_cnt = register_guarded_int_counter_vec_with_registry!(
            "stream_actor_idle_cnt",
            "tokio's metrics",
            &["actor_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let actor_in_record_cnt = register_guarded_int_counter_vec_with_registry!(
            "stream_actor_in_record_cnt",
            "Total number of rows actor received",
            &["actor_id", "fragment_id", "upstream_fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let actor_out_record_cnt = register_guarded_int_counter_vec_with_registry!(
            "stream_actor_out_record_cnt",
            "Total number of rows actor sent",
            &["actor_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let actor_current_epoch = register_guarded_int_gauge_vec_with_registry!(
            "stream_actor_current_epoch",
            "Current epoch of actor",
            &["actor_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let project_expr_inflight_window_size = register_guarded_int_gauge_vec_with_registry!(
            "stream_project_expr_inflight_window_size",
            "Number of messages waiting in ProjectExecutor's ordered projection window",
            &["actor_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let actor_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_actor_count",
            "Total number of actors (parallelism)",
            &["fragment_id"],
            registry
        )
        .unwrap();

        let merge_barrier_align_duration = register_guarded_int_counter_vec_with_registry!(
            "stream_merge_barrier_align_duration_ns",
            "Total merge barrier alignment duration (ns)",
            &["actor_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let join_lookup_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_join_lookup_miss_count",
            "Join executor lookup miss duration",
            &["actor_id", "side", "join_table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let join_lookup_total_count = register_guarded_int_counter_vec_with_registry!(
            "stream_join_lookup_total_count",
            "Join executor lookup total operation",
            &["actor_id", "side", "join_table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let join_insert_cache_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_join_insert_cache_miss_count",
            "Join executor cache miss when insert operation",
            &["actor_id", "side", "join_table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let join_actor_input_waiting_duration_ns = register_guarded_int_counter_vec_with_registry!(
            "stream_join_actor_input_waiting_duration_ns",
            "Total waiting duration (ns) of input buffer of join actor",
            &["actor_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let join_match_duration_ns = register_guarded_int_counter_vec_with_registry!(
            "stream_join_match_duration_ns",
            "Matching duration for each side",
            &["actor_id", "fragment_id", "side"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let barrier_align_duration = register_guarded_int_counter_vec_with_registry!(
            "stream_barrier_align_duration_ns",
            "Duration of join align barrier",
            &["actor_id", "fragment_id", "wait_side", "executor"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let join_cached_entry_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_join_cached_entry_count",
            "Number of cached entries in streaming join operators",
            &["actor_id", "fragment_id", "side"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let join_matched_join_keys_opts = histogram_opts!(
            "stream_join_matched_join_keys",
            "The number of keys matched in the opposite side",
            exponential_buckets(16.0, 2.0, 28).unwrap() // max 2^31
        );

        let join_matched_join_keys = register_guarded_histogram_vec_with_registry!(
            join_matched_join_keys_opts,
            &["actor_id", "fragment_id", "table_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let agg_lookup_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_agg_lookup_miss_count",
            "Aggregation executor lookup miss duration",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let agg_total_lookup_count = register_guarded_int_counter_vec_with_registry!(
            "stream_agg_lookup_total_count",
            "Aggregation executor lookup total operation",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let agg_distinct_cache_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_agg_distinct_cache_miss_count",
            "Aggregation executor dinsinct miss duration",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let agg_distinct_total_cache_count = register_guarded_int_counter_vec_with_registry!(
            "stream_agg_distinct_total_cache_count",
            "Aggregation executor distinct total operation",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let agg_distinct_cached_entry_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_agg_distinct_cached_entry_count",
            "Total entry counts in distinct aggregation executor cache",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let agg_dirty_groups_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_agg_dirty_groups_count",
            "Total dirty group counts in aggregation executor",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let agg_dirty_groups_heap_size = register_guarded_int_gauge_vec_with_registry!(
            "stream_agg_dirty_groups_heap_size",
            "Total dirty group heap size in aggregation executor",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let agg_state_cache_lookup_count = register_guarded_int_counter_vec_with_registry!(
            "stream_agg_state_cache_lookup_count",
            "Aggregation executor state cache lookup count",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let agg_state_cache_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_agg_state_cache_miss_count",
            "Aggregation executor state cache miss count",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let group_top_n_cache_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_group_top_n_cache_miss_count",
            "Group top n executor cache miss count",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let group_top_n_total_query_cache_count = register_guarded_int_counter_vec_with_registry!(
            "stream_group_top_n_total_query_cache_count",
            "Group top n executor query cache total count",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let group_top_n_cached_entry_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_group_top_n_cached_entry_count",
            "Total entry counts in group top n executor cache",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let group_top_n_appendonly_cache_miss_count =
            register_guarded_int_counter_vec_with_registry!(
                "stream_group_top_n_appendonly_cache_miss_count",
                "Group top n appendonly executor cache miss count",
                &["actor_id", "table_id", "fragment_id"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let group_top_n_appendonly_total_query_cache_count =
            register_guarded_int_counter_vec_with_registry!(
                "stream_group_top_n_appendonly_total_query_cache_count",
                "Group top n appendonly executor total cache count",
                &["actor_id", "table_id", "fragment_id"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let group_top_n_appendonly_cached_entry_count =
            register_guarded_int_gauge_vec_with_registry!(
                "stream_group_top_n_appendonly_cached_entry_count",
                "Total entry counts in group top n appendonly executor cache",
                &["actor_id", "table_id", "fragment_id"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let lookup_cache_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_lookup_cache_miss_count",
            "Lookup executor cache miss count",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let lookup_total_query_cache_count = register_guarded_int_counter_vec_with_registry!(
            "stream_lookup_total_query_cache_count",
            "Lookup executor query cache total count",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let lookup_cached_entry_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_lookup_cached_entry_count",
            "Total entry counts in lookup executor cache",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let temporal_join_cache_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_temporal_join_cache_miss_count",
            "Temporal join executor cache miss count",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let temporal_join_total_query_cache_count =
            register_guarded_int_counter_vec_with_registry!(
                "stream_temporal_join_total_query_cache_count",
                "Temporal join executor query cache total count",
                &["actor_id", "table_id", "fragment_id"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let temporal_join_cached_entry_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_temporal_join_cached_entry_count",
            "Total entry count in temporal join executor cache",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let agg_cached_entry_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_agg_cached_entry_count",
            "Number of cached keys in streaming aggregation operators",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let agg_chunk_lookup_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_agg_chunk_lookup_miss_count",
            "Aggregation executor chunk-level lookup miss duration",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let agg_chunk_total_lookup_count = register_guarded_int_counter_vec_with_registry!(
            "stream_agg_chunk_lookup_total_count",
            "Aggregation executor chunk-level lookup total operation",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let backfill_snapshot_read_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_backfill_snapshot_read_row_count",
            "Total number of rows that have been read from the backfill snapshot",
            &["actor_id", "table_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let backfill_upstream_output_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_backfill_upstream_output_row_count",
            "Total number of rows that have been output from the backfill upstream",
            &["actor_id", "table_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let cdc_backfill_snapshot_read_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_cdc_backfill_snapshot_read_row_count",
            "Total number of rows that have been read from the cdc_backfill snapshot",
            &["actor_id", "table_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let cdc_backfill_upstream_output_row_count =
            register_guarded_int_counter_vec_with_registry!(
                "stream_cdc_backfill_upstream_output_row_count",
                "Total number of rows that have been output from the cdc_backfill upstream",
                &["actor_id", "table_id"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let snapshot_backfill_consume_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_snapshot_backfill_consume_snapshot_row_count",
            "Total number of rows that have been output from snapshot backfill",
            &["actor_id", "table_id", "stage"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let over_window_cached_entry_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_over_window_cached_entry_count",
            "Total entry (partition) count in over window executor cache",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let over_window_cache_lookup_count = register_guarded_int_counter_vec_with_registry!(
            "stream_over_window_cache_lookup_count",
            "Over window executor cache lookup count",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let over_window_cache_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_over_window_cache_miss_count",
            "Over window executor cache miss count",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let over_window_range_cache_entry_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_over_window_range_cache_entry_count",
            "Over window partition range cache entry count",
            &["actor_id", "table_id", "fragment_id"],
            registry,
        )
        .unwrap()
        .relabel_debug_1(level);

        let over_window_range_cache_lookup_count = register_guarded_int_counter_vec_with_registry!(
            "stream_over_window_range_cache_lookup_count",
            "Over window partition range cache lookup count",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let over_window_range_cache_left_miss_count =
            register_guarded_int_counter_vec_with_registry!(
                "stream_over_window_range_cache_left_miss_count",
                "Over window partition range cache left miss count",
                &["actor_id", "table_id", "fragment_id"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let over_window_range_cache_right_miss_count =
            register_guarded_int_counter_vec_with_registry!(
                "stream_over_window_range_cache_right_miss_count",
                "Over window partition range cache right miss count",
                &["actor_id", "table_id", "fragment_id"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let over_window_accessed_entry_count = register_guarded_int_counter_vec_with_registry!(
            "stream_over_window_accessed_entry_count",
            "Over window accessed entry count",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let over_window_compute_count = register_guarded_int_counter_vec_with_registry!(
            "stream_over_window_compute_count",
            "Over window compute count",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let over_window_same_output_count = register_guarded_int_counter_vec_with_registry!(
            "stream_over_window_same_output_count",
            "Over window same output count",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let barrier_inflight_latency = register_guarded_histogram_vec_with_registry!(
            "stream_barrier_inflight_duration_seconds",
            "barrier_inflight_latency",
            &["partial_graph"],
            latency_buckets(600.0, 20),
            registry
        )
        .unwrap();

        let barrier_sync_latency = register_guarded_histogram_vec_with_registry!(
            "stream_barrier_sync_storage_duration_seconds",
            "barrier_sync_latency",
            &["partial_graph"],
            latency_buckets(600.0, 20),
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "stream_barrier_batch_size",
            "barrier_batch_size",
            exponential_buckets(1.0, 2.0, 8).unwrap()
        );
        let barrier_batch_size = register_histogram_with_registry!(opts, registry).unwrap();

        let barrier_manager_progress = register_guarded_int_counter_vec_with_registry!(
            "stream_barrier_manager_progress",
            "The number of actors that have processed the earliest in-flight barriers",
            &["partial_graph"],
            registry
        )
        .unwrap();

        let sync_kv_log_store_wait_next_poll_ns = register_guarded_int_counter_vec_with_registry!(
            "sync_kv_log_store_wait_next_poll_ns",
            "Total duration (ns) of waiting for next poll",
            &["actor_id", "target", "fragment_id", "relation"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let sync_kv_log_store_read_count = register_guarded_int_counter_vec_with_registry!(
            "sync_kv_log_store_read_count",
            "read row count throughput of sync_kv log store",
            &["actor_id", "type", "target", "fragment_id", "relation"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let sync_kv_log_store_read_size = register_guarded_int_counter_vec_with_registry!(
            "sync_kv_log_store_read_size",
            "read size throughput of sync_kv log store",
            &["actor_id", "type", "target", "fragment_id", "relation"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let sync_kv_log_store_write_pause_duration_ns =
            register_guarded_int_counter_vec_with_registry!(
                "sync_kv_log_store_write_pause_duration_ns",
                "Duration (ns) of sync_kv log store write pause",
                &["actor_id", "target", "fragment_id", "relation"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let sync_kv_log_store_state = register_guarded_int_counter_vec_with_registry!(
            "sync_kv_log_store_state",
            "clean/unclean state transition for sync_kv log store",
            &["actor_id", "state", "target", "fragment_id", "relation"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let sync_kv_log_store_storage_write_count =
            register_guarded_int_counter_vec_with_registry!(
                "sync_kv_log_store_storage_write_count",
                "Write row count throughput of sync_kv log store",
                &["actor_id", "target", "fragment_id", "relation"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let sync_kv_log_store_storage_write_size = register_guarded_int_counter_vec_with_registry!(
            "sync_kv_log_store_storage_write_size",
            "Write size throughput of sync_kv log store",
            &["actor_id", "target", "fragment_id", "relation"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let sync_kv_log_store_buffer_unconsumed_item_count =
            register_guarded_int_gauge_vec_with_registry!(
                "sync_kv_log_store_buffer_unconsumed_item_count",
                "Number of Unconsumed Item in buffer",
                &["actor_id", "target", "fragment_id", "relation"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let sync_kv_log_store_buffer_unconsumed_row_count =
            register_guarded_int_gauge_vec_with_registry!(
                "sync_kv_log_store_buffer_unconsumed_row_count",
                "Number of Unconsumed Row in buffer",
                &["actor_id", "target", "fragment_id", "relation"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let sync_kv_log_store_buffer_unconsumed_epoch_count =
            register_guarded_int_gauge_vec_with_registry!(
                "sync_kv_log_store_buffer_unconsumed_epoch_count",
                "Number of Unconsumed Epoch in buffer",
                &["actor_id", "target", "fragment_id", "relation"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let sync_kv_log_store_buffer_unconsumed_min_epoch =
            register_guarded_int_gauge_vec_with_registry!(
                "sync_kv_log_store_buffer_unconsumed_min_epoch",
                "Number of Unconsumed Epoch in buffer",
                &["actor_id", "target", "fragment_id", "relation"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);
        let sync_kv_log_store_buffer_memory_bytes =
            register_guarded_int_gauge_vec_with_registry!(
                "sync_kv_log_store_buffer_memory_bytes",
                "Estimated heap bytes used by synced kv log store buffer (unconsumed + consumed but not truncated)",
                &["actor_id", "target", "fragment_id", "relation"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let kv_log_store_storage_write_count = register_guarded_int_counter_vec_with_registry!(
            "kv_log_store_storage_write_count",
            "Write row count throughput of kv log store",
            &["actor_id", "connector", "sink_id", "sink_name"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let kv_log_store_storage_write_size = register_guarded_int_counter_vec_with_registry!(
            "kv_log_store_storage_write_size",
            "Write size throughput of kv log store",
            &["actor_id", "connector", "sink_id", "sink_name"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let kv_log_store_storage_read_count = register_guarded_int_counter_vec_with_registry!(
            "kv_log_store_storage_read_count",
            "Write row count throughput of kv log store",
            &["actor_id", "connector", "sink_id", "sink_name", "read_type"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let kv_log_store_storage_read_size = register_guarded_int_counter_vec_with_registry!(
            "kv_log_store_storage_read_size",
            "Write size throughput of kv log store",
            &["actor_id", "connector", "sink_id", "sink_name", "read_type"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let kv_log_store_rewind_count = register_guarded_int_counter_vec_with_registry!(
            "kv_log_store_rewind_count",
            "Kv log store rewind rate",
            &["actor_id", "connector", "sink_id", "sink_name"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let kv_log_store_rewind_delay_opts = {
            let first_delay_secs = REWIND_INITIAL_DELAY.as_secs_f64();
            let base = REWIND_BACKOFF_MULTIPLIER as f64;
            let bucket_count = (REWIND_MAX_DELAY.as_secs_f64() / first_delay_secs)
                .log(base)
                .ceil() as usize;
            let buckets = exponential_buckets(first_delay_secs, base, bucket_count).unwrap();
            histogram_opts!(
                "kv_log_store_rewind_delay",
                "Kv log store rewind delay",
                buckets,
            )
        };

        let kv_log_store_rewind_delay = register_guarded_histogram_vec_with_registry!(
            kv_log_store_rewind_delay_opts,
            &["actor_id", "connector", "sink_id", "sink_name"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let kv_log_store_buffer_unconsumed_item_count =
            register_guarded_int_gauge_vec_with_registry!(
                "kv_log_store_buffer_unconsumed_item_count",
                "Number of Unconsumed Item in buffer",
                &["actor_id", "connector", "sink_id", "sink_name"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let kv_log_store_buffer_unconsumed_row_count =
            register_guarded_int_gauge_vec_with_registry!(
                "kv_log_store_buffer_unconsumed_row_count",
                "Number of Unconsumed Row in buffer",
                &["actor_id", "connector", "sink_id", "sink_name"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let kv_log_store_buffer_unconsumed_epoch_count =
            register_guarded_int_gauge_vec_with_registry!(
                "kv_log_store_buffer_unconsumed_epoch_count",
                "Number of Unconsumed Epoch in buffer",
                &["actor_id", "connector", "sink_id", "sink_name"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let kv_log_store_buffer_unconsumed_min_epoch =
            register_guarded_int_gauge_vec_with_registry!(
                "kv_log_store_buffer_unconsumed_min_epoch",
                "Number of Unconsumed Epoch in buffer",
                &["actor_id", "connector", "sink_id", "sink_name"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let crossdb_last_consumed_min_epoch = register_guarded_int_gauge_vec_with_registry!(
            "crossdb_last_consumed_min_epoch",
            "Last consumed min epoch for cross-database changelog stream scan",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let kv_log_store_buffer_memory_bytes =
            register_guarded_int_gauge_vec_with_registry!(
                "kv_log_store_buffer_memory_bytes",
                "Estimated heap bytes used by kv log store buffer (unconsumed + consumed but not truncated)",
                &["actor_id", "connector", "sink_id", "sink_name"],
                registry
            )
            .unwrap()
            .relabel_debug_1(level);

        let lru_runtime_loop_count = register_int_counter_with_registry!(
            "lru_runtime_loop_count",
            "The counts of the eviction loop in LRU manager per second",
            registry
        )
        .unwrap();

        let lru_latest_sequence = register_int_gauge_with_registry!(
            "lru_latest_sequence",
            "Current LRU global sequence",
            registry,
        )
        .unwrap();

        let lru_watermark_sequence = register_int_gauge_with_registry!(
            "lru_watermark_sequence",
            "Current LRU watermark sequence",
            registry,
        )
        .unwrap();

        let lru_eviction_policy = register_int_gauge_with_registry!(
            "lru_eviction_policy",
            "Current LRU eviction policy",
            registry,
        )
        .unwrap();

        let jemalloc_allocated_bytes = register_int_gauge_with_registry!(
            "jemalloc_allocated_bytes",
            "The allocated memory jemalloc, got from jemalloc_ctl",
            registry
        )
        .unwrap();

        let jemalloc_active_bytes = register_int_gauge_with_registry!(
            "jemalloc_active_bytes",
            "The active memory jemalloc, got from jemalloc_ctl",
            registry
        )
        .unwrap();

        let jemalloc_resident_bytes = register_int_gauge_with_registry!(
            "jemalloc_resident_bytes",
            "The active memory jemalloc, got from jemalloc_ctl",
            registry
        )
        .unwrap();

        let jemalloc_metadata_bytes = register_int_gauge_with_registry!(
            "jemalloc_metadata_bytes",
            "The active memory jemalloc, got from jemalloc_ctl",
            registry
        )
        .unwrap();

        let jvm_allocated_bytes = register_int_gauge_with_registry!(
            "jvm_allocated_bytes",
            "The allocated jvm memory",
            registry
        )
        .unwrap();

        let jvm_active_bytes = register_int_gauge_with_registry!(
            "jvm_active_bytes",
            "The active jvm memory",
            registry
        )
        .unwrap();

        let materialize_cache_hit_count = register_guarded_int_counter_vec_with_registry!(
            "stream_materialize_cache_hit_count",
            "Materialize executor cache hit count",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let materialize_data_exist_count = register_guarded_int_counter_vec_with_registry!(
            "stream_materialize_data_exist_count",
            "Materialize executor data exist count",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let materialize_cache_total_count = register_guarded_int_counter_vec_with_registry!(
            "stream_materialize_cache_total_count",
            "Materialize executor cache total operation",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let stream_memory_usage = register_guarded_int_gauge_vec_with_registry!(
            "stream_memory_usage",
            "Memory usage for stream executors",
            &["actor_id", "table_id", "desc"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let gap_fill_generated_rows_count = register_guarded_int_counter_vec_with_registry!(
            "gap_fill_generated_rows_count",
            "Total number of rows generated by gap fill executor",
            &["actor_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let state_table_iter_count = register_guarded_int_counter_vec_with_registry!(
            "state_table_iter_count",
            "Total number of state table iter operations",
            &["actor_id", "fragment_id", "table_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let state_table_get_count = register_guarded_int_counter_vec_with_registry!(
            "state_table_get_count",
            "Total number of state table get operations",
            &["actor_id", "fragment_id", "table_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let state_table_iter_vnode_pruned_count = register_guarded_int_counter_vec_with_registry!(
            "state_table_iter_vnode_pruned_count",
            "Total number of state table iter operations pruned by vnode statistics",
            &["actor_id", "fragment_id", "table_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let state_table_get_vnode_pruned_count = register_guarded_int_counter_vec_with_registry!(
            "state_table_get_vnode_pruned_count",
            "Total number of state table get operations pruned by vnode statistics",
            &["actor_id", "fragment_id", "table_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        Self {
            level,
            executor_row_count,
            mem_stream_node_output_row_count: stream_node_output_row_count,
            mem_stream_node_output_blocking_duration_ns: stream_node_output_blocking_duration_ns,
            actor_scheduled_duration,
            actor_scheduled_cnt,
            actor_poll_duration,
            actor_poll_cnt,
            actor_idle_duration,
            actor_idle_cnt,
            actor_count,
            actor_in_record_cnt,
            actor_out_record_cnt,
            fragment_channel_buffered_bytes,
            actor_current_epoch,
            project_expr_inflight_window_size,
            source_output_row_count,
            source_split_change_count,
            source_backfill_row_count,
            sink_input_row_count,
            sink_input_bytes,
            sink_chunk_buffer_size,
            exchange_frag_recv_size,
            merge_barrier_align_duration,
            actor_output_buffer_blocking_duration_ns,
            actor_input_buffer_blocking_duration_ns,
            join_lookup_miss_count,
            join_lookup_total_count,
            join_insert_cache_miss_count,
            join_actor_input_waiting_duration_ns,
            join_match_duration_ns,
            join_cached_entry_count,
            join_matched_join_keys,
            barrier_align_duration,
            agg_lookup_miss_count,
            agg_total_lookup_count,
            agg_cached_entry_count,
            agg_chunk_lookup_miss_count,
            agg_chunk_total_lookup_count,
            agg_dirty_groups_count,
            agg_dirty_groups_heap_size,
            agg_distinct_cache_miss_count,
            agg_distinct_total_cache_count,
            agg_distinct_cached_entry_count,
            agg_state_cache_lookup_count,
            agg_state_cache_miss_count,
            group_top_n_cache_miss_count,
            group_top_n_total_query_cache_count,
            group_top_n_cached_entry_count,
            group_top_n_appendonly_cache_miss_count,
            group_top_n_appendonly_total_query_cache_count,
            group_top_n_appendonly_cached_entry_count,
            lookup_cache_miss_count,
            lookup_total_query_cache_count,
            lookup_cached_entry_count,
            temporal_join_cache_miss_count,
            temporal_join_total_query_cache_count,
            temporal_join_cached_entry_count,
            backfill_snapshot_read_row_count,
            backfill_upstream_output_row_count,
            cdc_backfill_snapshot_read_row_count,
            cdc_backfill_upstream_output_row_count,
            snapshot_backfill_consume_row_count,
            over_window_cached_entry_count,
            over_window_cache_lookup_count,
            over_window_cache_miss_count,
            over_window_range_cache_entry_count,
            over_window_range_cache_lookup_count,
            over_window_range_cache_left_miss_count,
            over_window_range_cache_right_miss_count,
            over_window_accessed_entry_count,
            over_window_compute_count,
            over_window_same_output_count,
            barrier_inflight_latency,
            barrier_sync_latency,
            barrier_batch_size,
            barrier_manager_progress,
            kv_log_store_storage_write_count,
            kv_log_store_storage_write_size,
            kv_log_store_rewind_count,
            kv_log_store_rewind_delay,
            kv_log_store_storage_read_count,
            kv_log_store_storage_read_size,
            kv_log_store_buffer_unconsumed_item_count,
            kv_log_store_buffer_unconsumed_row_count,
            kv_log_store_buffer_unconsumed_epoch_count,
            kv_log_store_buffer_unconsumed_min_epoch,
            kv_log_store_buffer_memory_bytes,
            crossdb_last_consumed_min_epoch,
            sync_kv_log_store_read_count,
            sync_kv_log_store_read_size,
            sync_kv_log_store_write_pause_duration_ns,
            sync_kv_log_store_state,
            sync_kv_log_store_wait_next_poll_ns,
            sync_kv_log_store_storage_write_count,
            sync_kv_log_store_storage_write_size,
            sync_kv_log_store_buffer_unconsumed_item_count,
            sync_kv_log_store_buffer_unconsumed_row_count,
            sync_kv_log_store_buffer_unconsumed_epoch_count,
            sync_kv_log_store_buffer_unconsumed_min_epoch,
            sync_kv_log_store_buffer_memory_bytes,
            lru_runtime_loop_count,
            lru_latest_sequence,
            lru_watermark_sequence,
            lru_eviction_policy,
            jemalloc_allocated_bytes,
            jemalloc_active_bytes,
            jemalloc_resident_bytes,
            jemalloc_metadata_bytes,
            jvm_allocated_bytes,
            jvm_active_bytes,
            stream_memory_usage,
            materialize_cache_hit_count,
            materialize_data_exist_count,
            materialize_cache_total_count,
            materialize_input_row_count,
            materialize_current_epoch,
            pg_cdc_state_table_lsn,
            pg_cdc_jni_commit_offset_lsn,
            mysql_cdc_state_binlog_file_seq,
            mysql_cdc_state_binlog_position,
            sqlserver_cdc_state_change_lsn,
            sqlserver_cdc_state_commit_lsn,
            sqlserver_cdc_jni_commit_offset_lsn,
            gap_fill_generated_rows_count,
            state_table_iter_count,
            state_table_get_count,
            state_table_iter_vnode_pruned_count,
            state_table_get_vnode_pruned_count,
        }
    }

    /// Create a new `StreamingMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        global_streaming_metrics(MetricLevel::Disabled)
    }

    pub fn new_actor_metrics(&self, actor_id: ActorId, fragment_id: FragmentId) -> ActorMetrics {
        let label_list: &[&str; 2] = &[&actor_id.to_string(), &fragment_id.to_string()];
        let actor_scheduled_duration = self
            .actor_scheduled_duration
            .with_guarded_label_values(label_list);
        let actor_scheduled_cnt = self
            .actor_scheduled_cnt
            .with_guarded_label_values(label_list);
        let actor_poll_duration = self
            .actor_poll_duration
            .with_guarded_label_values(label_list);
        let actor_poll_cnt = self.actor_poll_cnt.with_guarded_label_values(label_list);
        let actor_idle_duration = self
            .actor_idle_duration
            .with_guarded_label_values(label_list);
        let actor_idle_cnt = self.actor_idle_cnt.with_guarded_label_values(label_list);
        ActorMetrics {
            actor_scheduled_duration,
            actor_scheduled_cnt,
            actor_poll_duration,
            actor_poll_cnt,
            actor_idle_duration,
            actor_idle_cnt,
        }
    }

    pub(crate) fn new_actor_input_metrics(
        &self,
        actor_id: ActorId,
        fragment_id: FragmentId,
        upstream_fragment_id: FragmentId,
    ) -> ActorInputMetrics {
        let actor_id_str = actor_id.to_string();
        let fragment_id_str = fragment_id.to_string();
        let upstream_fragment_id_str = upstream_fragment_id.to_string();
        ActorInputMetrics {
            actor_in_record_cnt: self.actor_in_record_cnt.with_guarded_label_values(&[
                &actor_id_str,
                &fragment_id_str,
                &upstream_fragment_id_str,
            ]),
            actor_input_buffer_blocking_duration_ns: self
                .actor_input_buffer_blocking_duration_ns
                .with_guarded_label_values(&[
                    &actor_id_str,
                    &fragment_id_str,
                    &upstream_fragment_id_str,
                ]),
        }
    }

    pub fn new_sink_exec_metrics(
        &self,
        id: SinkId,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> SinkExecutorMetrics {
        let label_list: &[&str; 3] = &[
            &actor_id.to_string(),
            &id.to_string(),
            &fragment_id.to_string(),
        ];
        SinkExecutorMetrics {
            sink_input_row_count: self
                .sink_input_row_count
                .with_guarded_label_values(label_list),
            sink_input_bytes: self.sink_input_bytes.with_guarded_label_values(label_list),
            sink_chunk_buffer_size: self
                .sink_chunk_buffer_size
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_group_top_n_metrics(
        &self,
        table_id: TableId,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> GroupTopNMetrics {
        let label_list: &[&str; 3] = &[
            &actor_id.to_string(),
            &table_id.to_string(),
            &fragment_id.to_string(),
        ];

        GroupTopNMetrics {
            group_top_n_cache_miss_count: self
                .group_top_n_cache_miss_count
                .with_guarded_label_values(label_list),
            group_top_n_total_query_cache_count: self
                .group_top_n_total_query_cache_count
                .with_guarded_label_values(label_list),
            group_top_n_cached_entry_count: self
                .group_top_n_cached_entry_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_append_only_group_top_n_metrics(
        &self,
        table_id: TableId,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> GroupTopNMetrics {
        let label_list: &[&str; 3] = &[
            &actor_id.to_string(),
            &table_id.to_string(),
            &fragment_id.to_string(),
        ];

        GroupTopNMetrics {
            group_top_n_cache_miss_count: self
                .group_top_n_appendonly_cache_miss_count
                .with_guarded_label_values(label_list),
            group_top_n_total_query_cache_count: self
                .group_top_n_appendonly_total_query_cache_count
                .with_guarded_label_values(label_list),
            group_top_n_cached_entry_count: self
                .group_top_n_appendonly_cached_entry_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_lookup_executor_metrics(
        &self,
        table_id: TableId,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> LookupExecutorMetrics {
        let label_list: &[&str; 3] = &[
            &actor_id.to_string(),
            &table_id.to_string(),
            &fragment_id.to_string(),
        ];

        LookupExecutorMetrics {
            lookup_cache_miss_count: self
                .lookup_cache_miss_count
                .with_guarded_label_values(label_list),
            lookup_total_query_cache_count: self
                .lookup_total_query_cache_count
                .with_guarded_label_values(label_list),
            lookup_cached_entry_count: self
                .lookup_cached_entry_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_hash_agg_metrics(
        &self,
        table_id: TableId,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> HashAggMetrics {
        let label_list: &[&str; 3] = &[
            &actor_id.to_string(),
            &table_id.to_string(),
            &fragment_id.to_string(),
        ];
        HashAggMetrics {
            agg_lookup_miss_count: self
                .agg_lookup_miss_count
                .with_guarded_label_values(label_list),
            agg_total_lookup_count: self
                .agg_total_lookup_count
                .with_guarded_label_values(label_list),
            agg_cached_entry_count: self
                .agg_cached_entry_count
                .with_guarded_label_values(label_list),
            agg_chunk_lookup_miss_count: self
                .agg_chunk_lookup_miss_count
                .with_guarded_label_values(label_list),
            agg_chunk_total_lookup_count: self
                .agg_chunk_total_lookup_count
                .with_guarded_label_values(label_list),
            agg_dirty_groups_count: self
                .agg_dirty_groups_count
                .with_guarded_label_values(label_list),
            agg_dirty_groups_heap_size: self
                .agg_dirty_groups_heap_size
                .with_guarded_label_values(label_list),
            agg_state_cache_lookup_count: self
                .agg_state_cache_lookup_count
                .with_guarded_label_values(label_list),
            agg_state_cache_miss_count: self
                .agg_state_cache_miss_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_agg_distinct_dedup_metrics(
        &self,
        table_id: TableId,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> AggDistinctDedupMetrics {
        let label_list: &[&str; 3] = &[
            &actor_id.to_string(),
            &table_id.to_string(),
            &fragment_id.to_string(),
        ];
        AggDistinctDedupMetrics {
            agg_distinct_cache_miss_count: self
                .agg_distinct_cache_miss_count
                .with_guarded_label_values(label_list),
            agg_distinct_total_cache_count: self
                .agg_distinct_total_cache_count
                .with_guarded_label_values(label_list),
            agg_distinct_cached_entry_count: self
                .agg_distinct_cached_entry_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_temporal_join_metrics(
        &self,
        table_id: TableId,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> TemporalJoinMetrics {
        let label_list: &[&str; 3] = &[
            &actor_id.to_string(),
            &table_id.to_string(),
            &fragment_id.to_string(),
        ];
        TemporalJoinMetrics {
            temporal_join_cache_miss_count: self
                .temporal_join_cache_miss_count
                .with_guarded_label_values(label_list),
            temporal_join_total_query_cache_count: self
                .temporal_join_total_query_cache_count
                .with_guarded_label_values(label_list),
            temporal_join_cached_entry_count: self
                .temporal_join_cached_entry_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_backfill_metrics(&self, table_id: TableId, actor_id: ActorId) -> BackfillMetrics {
        let label_list: &[&str; 2] = &[&actor_id.to_string(), &table_id.to_string()];
        BackfillMetrics {
            backfill_snapshot_read_row_count: self
                .backfill_snapshot_read_row_count
                .with_guarded_label_values(label_list),
            backfill_upstream_output_row_count: self
                .backfill_upstream_output_row_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_cdc_backfill_metrics(
        &self,
        table_id: TableId,
        actor_id: ActorId,
    ) -> CdcBackfillMetrics {
        let label_list: &[&str; 2] = &[&actor_id.to_string(), &table_id.to_string()];
        CdcBackfillMetrics {
            cdc_backfill_snapshot_read_row_count: self
                .cdc_backfill_snapshot_read_row_count
                .with_guarded_label_values(label_list),
            cdc_backfill_upstream_output_row_count: self
                .cdc_backfill_upstream_output_row_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_over_window_metrics(
        &self,
        table_id: TableId,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> OverWindowMetrics {
        let label_list: &[&str; 3] = &[
            &actor_id.to_string(),
            &table_id.to_string(),
            &fragment_id.to_string(),
        ];
        OverWindowMetrics {
            over_window_cached_entry_count: self
                .over_window_cached_entry_count
                .with_guarded_label_values(label_list),
            over_window_cache_lookup_count: self
                .over_window_cache_lookup_count
                .with_guarded_label_values(label_list),
            over_window_cache_miss_count: self
                .over_window_cache_miss_count
                .with_guarded_label_values(label_list),
            over_window_range_cache_entry_count: self
                .over_window_range_cache_entry_count
                .with_guarded_label_values(label_list),
            over_window_range_cache_lookup_count: self
                .over_window_range_cache_lookup_count
                .with_guarded_label_values(label_list),
            over_window_range_cache_left_miss_count: self
                .over_window_range_cache_left_miss_count
                .with_guarded_label_values(label_list),
            over_window_range_cache_right_miss_count: self
                .over_window_range_cache_right_miss_count
                .with_guarded_label_values(label_list),
            over_window_accessed_entry_count: self
                .over_window_accessed_entry_count
                .with_guarded_label_values(label_list),
            over_window_compute_count: self
                .over_window_compute_count
                .with_guarded_label_values(label_list),
            over_window_same_output_count: self
                .over_window_same_output_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_materialize_cache_metrics(
        &self,
        table_id: TableId,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> MaterializeCacheMetrics {
        let label_list: &[&str; 3] = &[
            &actor_id.to_string(),
            &table_id.to_string(),
            &fragment_id.to_string(),
        ];
        MaterializeCacheMetrics {
            materialize_cache_hit_count: self
                .materialize_cache_hit_count
                .with_guarded_label_values(label_list),
            materialize_data_exist_count: self
                .materialize_data_exist_count
                .with_guarded_label_values(label_list),
            materialize_cache_total_count: self
                .materialize_cache_total_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_materialize_metrics(
        &self,
        table_id: TableId,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> MaterializeMetrics {
        let label_list: &[&str; 3] = &[
            &actor_id.to_string(),
            &table_id.to_string(),
            &fragment_id.to_string(),
        ];
        MaterializeMetrics {
            materialize_input_row_count: self
                .materialize_input_row_count
                .with_guarded_label_values(label_list),
            materialize_current_epoch: self
                .materialize_current_epoch
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_state_table_metrics(
        &self,
        table_id: TableId,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> StateTableMetrics {
        let label_list: &[&str; 3] = &[
            &actor_id.to_string(),
            &fragment_id.to_string(),
            &table_id.to_string(),
        ];
        StateTableMetrics {
            iter_count: self
                .state_table_iter_count
                .with_guarded_label_values(label_list),
            get_count: self
                .state_table_get_count
                .with_guarded_label_values(label_list),
            iter_vnode_pruned_count: self
                .state_table_iter_vnode_pruned_count
                .with_guarded_label_values(label_list),
            get_vnode_pruned_count: self
                .state_table_get_vnode_pruned_count
                .with_guarded_label_values(label_list),
        }
    }
}

pub(crate) struct ActorInputMetrics {
    pub(crate) actor_in_record_cnt: LabelGuardedIntCounter,
    pub(crate) actor_input_buffer_blocking_duration_ns: LabelGuardedIntCounter,
}

/// Tokio metrics for actors
pub struct ActorMetrics {
    pub actor_scheduled_duration: LabelGuardedIntCounter,
    pub actor_scheduled_cnt: LabelGuardedIntCounter,
    pub actor_poll_duration: LabelGuardedIntCounter,
    pub actor_poll_cnt: LabelGuardedIntCounter,
    pub actor_idle_duration: LabelGuardedIntCounter,
    pub actor_idle_cnt: LabelGuardedIntCounter,
}

pub struct SinkExecutorMetrics {
    pub sink_input_row_count: LabelGuardedIntCounter,
    pub sink_input_bytes: LabelGuardedIntCounter,
    pub sink_chunk_buffer_size: LabelGuardedIntGauge,
}

pub struct MaterializeCacheMetrics {
    pub materialize_cache_hit_count: LabelGuardedIntCounter,
    pub materialize_data_exist_count: LabelGuardedIntCounter,
    pub materialize_cache_total_count: LabelGuardedIntCounter,
}

pub struct MaterializeMetrics {
    pub materialize_input_row_count: LabelGuardedIntCounter,
    pub materialize_current_epoch: LabelGuardedIntGauge,
}

pub struct GroupTopNMetrics {
    pub group_top_n_cache_miss_count: LabelGuardedIntCounter,
    pub group_top_n_total_query_cache_count: LabelGuardedIntCounter,
    pub group_top_n_cached_entry_count: LabelGuardedIntGauge,
}

pub struct LookupExecutorMetrics {
    pub lookup_cache_miss_count: LabelGuardedIntCounter,
    pub lookup_total_query_cache_count: LabelGuardedIntCounter,
    pub lookup_cached_entry_count: LabelGuardedIntGauge,
}

pub struct HashAggMetrics {
    pub agg_lookup_miss_count: LabelGuardedIntCounter,
    pub agg_total_lookup_count: LabelGuardedIntCounter,
    pub agg_cached_entry_count: LabelGuardedIntGauge,
    pub agg_chunk_lookup_miss_count: LabelGuardedIntCounter,
    pub agg_chunk_total_lookup_count: LabelGuardedIntCounter,
    pub agg_dirty_groups_count: LabelGuardedIntGauge,
    pub agg_dirty_groups_heap_size: LabelGuardedIntGauge,
    pub agg_state_cache_lookup_count: LabelGuardedIntCounter,
    pub agg_state_cache_miss_count: LabelGuardedIntCounter,
}

pub struct AggDistinctDedupMetrics {
    pub agg_distinct_cache_miss_count: LabelGuardedIntCounter,
    pub agg_distinct_total_cache_count: LabelGuardedIntCounter,
    pub agg_distinct_cached_entry_count: LabelGuardedIntGauge,
}

pub struct TemporalJoinMetrics {
    pub temporal_join_cache_miss_count: LabelGuardedIntCounter,
    pub temporal_join_total_query_cache_count: LabelGuardedIntCounter,
    pub temporal_join_cached_entry_count: LabelGuardedIntGauge,
}

pub struct BackfillMetrics {
    pub backfill_snapshot_read_row_count: LabelGuardedIntCounter,
    pub backfill_upstream_output_row_count: LabelGuardedIntCounter,
}

#[derive(Clone)]
pub struct CdcBackfillMetrics {
    pub cdc_backfill_snapshot_read_row_count: LabelGuardedIntCounter,
    pub cdc_backfill_upstream_output_row_count: LabelGuardedIntCounter,
}

pub struct OverWindowMetrics {
    pub over_window_cached_entry_count: LabelGuardedIntGauge,
    pub over_window_cache_lookup_count: LabelGuardedIntCounter,
    pub over_window_cache_miss_count: LabelGuardedIntCounter,
    pub over_window_range_cache_entry_count: LabelGuardedIntGauge,
    pub over_window_range_cache_lookup_count: LabelGuardedIntCounter,
    pub over_window_range_cache_left_miss_count: LabelGuardedIntCounter,
    pub over_window_range_cache_right_miss_count: LabelGuardedIntCounter,
    pub over_window_accessed_entry_count: LabelGuardedIntCounter,
    pub over_window_compute_count: LabelGuardedIntCounter,
    pub over_window_same_output_count: LabelGuardedIntCounter,
}

#[derive(Clone)]
pub struct StateTableMetrics {
    pub iter_count: LabelGuardedIntCounter,
    pub get_count: LabelGuardedIntCounter,
    pub iter_vnode_pruned_count: LabelGuardedIntCounter,
    pub get_vnode_pruned_count: LabelGuardedIntCounter,
}

#[cfg(test)]
mod tests {
    use risingwave_common::metrics::get_label;

    use super::*;

    fn assert_metric_labels(registry: &Registry, metric_name: &str, expected: &[(&str, &str)]) {
        let metric_family = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == metric_name)
            .unwrap();
        let metric = metric_family.get_metric().first().unwrap();
        for (name, value) in expected {
            assert_eq!(get_label::<String>(metric, name).as_deref(), Some(*value));
        }
    }

    fn record_reordered_metrics(
        metrics: &StreamingMetrics,
    ) -> (LabelGuardedIntCounter, SinkExecutorMetrics, HashAggMetrics) {
        let source_output_row_count = metrics
            .source_output_row_count
            .with_guarded_label_values(&["11", "22", "source", "33"]);
        source_output_row_count.inc();
        let sink_metrics =
            metrics.new_sink_exec_metrics(SinkId::new(44), ActorId::new(11), FragmentId::new(33));
        sink_metrics.sink_input_row_count.inc();
        let agg_metrics =
            metrics.new_hash_agg_metrics(TableId::new(55), ActorId::new(11), FragmentId::new(33));
        agg_metrics.agg_lookup_miss_count.inc();
        (source_output_row_count, sink_metrics, agg_metrics)
    }

    #[test]
    fn info_and_critical_metrics_blank_actor_id_without_shifting_other_labels() {
        for level in [MetricLevel::Critical, MetricLevel::Info] {
            let registry = Registry::new();
            let metrics = StreamingMetrics::new(&registry, level);
            let _recorded_metrics = record_reordered_metrics(&metrics);

            assert_metric_labels(
                &registry,
                "stream_source_output_rows_counts",
                &[
                    ("actor_id", ""),
                    ("source_id", "22"),
                    ("source_name", "source"),
                    ("fragment_id", "33"),
                ],
            );
            assert_metric_labels(
                &registry,
                "stream_sink_input_row_count",
                &[("actor_id", ""), ("sink_id", "44"), ("fragment_id", "33")],
            );
            assert_metric_labels(
                &registry,
                "stream_agg_lookup_miss_count",
                &[("actor_id", ""), ("table_id", "55"), ("fragment_id", "33")],
            );
        }
    }

    #[test]
    fn debug_metrics_retain_actor_id() {
        let registry = Registry::new();
        let metrics = StreamingMetrics::new(&registry, MetricLevel::Debug);
        let _recorded_metrics = record_reordered_metrics(&metrics);

        assert_metric_labels(
            &registry,
            "stream_sink_input_row_count",
            &[("actor_id", "11"), ("sink_id", "44"), ("fragment_id", "33")],
        );
    }
}
