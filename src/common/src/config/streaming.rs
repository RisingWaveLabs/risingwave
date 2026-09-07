// Copyright 2025 RisingWave Labs
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

use std::time::Duration;

use risingwave_common_proc_macro::serde_prefix_all;

use super::*;

mod async_stack_trace;
mod cache_refill;
mod join_encoding_type;
mod over_window;

pub use async_stack_trace::*;
pub use cache_refill::*;
pub use join_encoding_type::*;
pub use over_window::*;

/// The section `[streaming]` in `risingwave.toml`.
#[serde_with::apply(Option => #[serde(with = "none_as_empty_string")])]
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct StreamingConfig {
    /// The maximum number of pending barriers in each partial graph. Pending barriers include
    /// in-flight, collected but not committed, and currently completing barriers.
    #[serde(default = "default::streaming::in_flight_barrier_nums")]
    pub in_flight_barrier_nums: usize,

    /// The maximum number of lagged barriers allowed when merging a snapshot backfill job into
    /// the database graph.
    #[serde(default = "default::streaming::snapshot_backfill_finish_max_lagged_barriers")]
    pub snapshot_backfill_finish_max_lagged_barriers: usize,

    /// The multiplier applied to `in_flight_barrier_nums` when limiting pending barriers in a
    /// snapshot backfill partial graph. A value of 0 is treated as 1.
    #[serde(default = "default::streaming::snapshot_backfill_barrier_amplification_factor")]
    pub snapshot_backfill_barrier_amplification_factor: usize,

    /// The thread number of the streaming actor runtime in the compute node. The default value is
    /// decided by `tokio`.
    #[serde(default)]
    pub actor_runtime_worker_threads_num: Option<usize>,

    /// Enable async stack tracing through `await-tree` for risectl.
    #[serde(default = "default::streaming::async_stack_trace")]
    pub async_stack_trace: AsyncStackTraceOption,

    #[serde(default)]
    #[config_doc(nested)]
    pub developer: StreamingDeveloperConfig,

    /// Max unique user stream errors per actor
    #[serde(default = "default::streaming::unique_user_stream_errors")]
    pub unique_user_stream_errors: usize,

    /// Disable strict stream consistency checks.
    #[serde(default = "default::streaming::unsafe_disable_strict_consistency")]
    pub unsafe_disable_strict_consistency: bool,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

/// The subsections `[streaming.developer]`.
///
/// It is put at [`StreamingConfig::developer`].
#[serde_prefix_all("stream_", mode = "alias")]
#[serde_with::apply(Option => #[serde(with = "none_as_empty_string")])]
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct StreamingDeveloperConfig {
    /// Set to true to enable per-executor row count metrics. This will produce a lot of timeseries
    /// and might affect the prometheus performance. If you only need actor input and output
    /// rows data, see `stream_actor_in_record_cnt` and `stream_actor_out_record_cnt` instead.
    #[serde(default = "default::developer::stream_enable_executor_row_count")]
    pub enable_executor_row_count: bool,

    /// The capacity of the chunks in the channel that connects between `ConnectorSource` and
    /// `SourceExecutor`.
    #[serde(default = "default::developer::connector_message_buffer_size")]
    pub connector_message_buffer_size: usize,

    /// Limit number of the cached entries in an extreme aggregation call.
    #[serde(default = "default::developer::unsafe_stream_extreme_cache_size")]
    pub unsafe_extreme_cache_size: usize,

    /// Minimum cache size for TopN cache per group key.
    #[serde(default = "default::developer::stream_topn_cache_min_capacity")]
    pub topn_cache_min_capacity: usize,

    /// The maximum size of the chunk produced by executor at a time.
    #[serde(default = "default::developer::stream_chunk_size")]
    pub chunk_size: usize,

    /// The initial permits that a channel holds, i.e., the maximum row count can be buffered in
    /// the channel.
    #[serde(default = "default::developer::stream_exchange_initial_permits")]
    pub exchange_initial_permits: usize,

    /// The permits that are batched to add back, for reducing the backward `AddPermits` messages
    /// in remote exchange.
    #[serde(default = "default::developer::stream_exchange_batched_permits")]
    pub exchange_batched_permits: usize,

    /// The maximum number of concurrent barriers in an exchange channel.
    #[serde(default = "default::developer::stream_exchange_concurrent_barriers")]
    pub exchange_concurrent_barriers: usize,

    /// The concurrency for dispatching messages to different downstream jobs.
    ///
    /// - `1` means no concurrency, i.e., dispatch messages to downstream jobs one by one.
    /// - `0` means unlimited concurrency.
    #[serde(default = "default::developer::stream_exchange_concurrent_dispatchers")]
    pub exchange_concurrent_dispatchers: usize,

    /// The maximum number of chunks that `ProjectExecutor` evaluates concurrently.
    ///
    /// - `1` means no chunk-level concurrency.
    /// - `0` means unlimited concurrency.
    #[serde(default = "default::developer::stream_project_expr_concurrency")]
    pub project_expr_concurrency: usize,

    /// The maximum number of in-flight projection evaluation requests in `ProjectExecutor`.
    ///
    /// An in-flight request has started projection evaluation but has not finished yet. A finished
    /// request no longer counts against this limit even if its result is still waiting to be emitted
    /// in order.
    ///
    /// - `0` means unlimited in-flight requests.
    #[serde(default = "default::developer::stream_project_expr_inflight_request_concurrency")]
    pub project_expr_inflight_request_concurrency: usize,

    /// The initial permits for a dml channel, i.e., the maximum row count can be buffered in
    /// the channel.
    #[serde(default = "default::developer::stream_dml_channel_initial_permits")]
    pub dml_channel_initial_permits: usize,

    /// The max heap size of dirty groups of `HashAggExecutor`.
    #[serde(default = "default::developer::stream_hash_agg_max_dirty_groups_heap_size")]
    pub hash_agg_max_dirty_groups_heap_size: usize,

    #[serde(default = "default::developer::memory_controller_threshold_aggressive")]
    pub memory_controller_threshold_aggressive: f64,

    #[serde(default = "default::developer::memory_controller_threshold_graceful")]
    pub memory_controller_threshold_graceful: f64,

    #[serde(default = "default::developer::memory_controller_threshold_stable")]
    pub memory_controller_threshold_stable: f64,

    #[serde(default = "default::developer::memory_controller_eviction_factor_aggressive")]
    pub memory_controller_eviction_factor_aggressive: f64,

    #[serde(default = "default::developer::memory_controller_eviction_factor_graceful")]
    pub memory_controller_eviction_factor_graceful: f64,

    #[serde(default = "default::developer::memory_controller_eviction_factor_stable")]
    pub memory_controller_eviction_factor_stable: f64,

    #[serde(default = "default::developer::memory_controller_update_interval_ms")]
    pub memory_controller_update_interval_ms: usize,

    #[serde(default = "default::developer::memory_controller_sequence_tls_step")]
    pub memory_controller_sequence_tls_step: u64,

    #[serde(default = "default::developer::memory_controller_sequence_tls_lag")]
    pub memory_controller_sequence_tls_lag: u64,

    #[serde(default = "default::developer::stream_enable_arrangement_backfill")]
    /// Deprecated and ignored for new streaming jobs. Arrangement backfill is always used as the
    /// fallback backfill type.
    #[deprecated(
        note = "Deprecated and ignored for new streaming jobs. Arrangement backfill is always used as the fallback backfill type."
    )]
    pub enable_arrangement_backfill: bool,

    #[serde(default = "default::developer::stream_enable_snapshot_backfill")]
    /// Enable snapshot backfill
    /// If false, the snapshot backfill will be disabled,
    /// even if session variable set.
    /// If true, it's decided by session variable `streaming_use_snapshot_backfill` (default true)
    pub enable_snapshot_backfill: bool,

    #[serde(default = "default::developer::stream_high_join_amplification_threshold")]
    /// If number of hash join matches exceeds this threshold number,
    /// it will be logged.
    pub high_join_amplification_threshold: usize,

    #[serde(default = "default::developer::stream_high_gap_fill_amplification_threshold")]
    /// If number of rows generated by gap fill between two anchor rows exceeds this threshold
    /// number, it will be logged.
    pub high_gap_fill_amplification_threshold: usize,

    /// Actor tokio metrics is enabled if `enable_actor_tokio_metrics` is set or metrics level >= Debug.
    #[serde(default = "default::developer::enable_actor_tokio_metrics")]
    pub enable_actor_tokio_metrics: bool,

    /// The number of the connections for streaming remote exchange between two nodes.
    /// If not specified, the value of `server.connection_pool_size` will be used.
    #[serde(default = "default::developer::stream_exchange_connection_pool_size")]
    pub(super) exchange_connection_pool_size: Option<u16>,

    /// A flag to allow disabling the auto schema change handling
    #[serde(default = "default::developer::stream_enable_auto_schema_change")]
    pub enable_auto_schema_change: bool,

    #[serde(default = "default::developer::enable_shared_source")]
    /// Enable shared source
    /// If false, the shared source will be disabled,
    /// even if session variable set.
    /// If true, it's decided by session variable `streaming_use_shared_source` (default true)
    pub enable_shared_source: bool,

    #[serde(default = "default::developer::switch_jdbc_pg_to_native")]
    /// When true, all jdbc sinks with connector='jdbc' and jdbc.url="jdbc:postgresql://..."
    /// will be switched from jdbc postgresql sinks to rust native (connector='postgres') sinks.
    pub switch_jdbc_pg_to_native: bool,

    /// The maximum number of consecutive barriers allowed in a message when sent between actors.
    #[serde(default = "default::developer::stream_max_barrier_batch_size")]
    pub max_barrier_batch_size: u32,

    /// Configure the system-wide cache row cardinality of hash join.
    /// For example, if this is set to 1000, it means we can have at most 1000 rows in cache.
    #[serde(default = "default::developer::streaming_hash_join_entry_state_max_rows")]
    pub hash_join_entry_state_max_rows: usize,

    /// Number of processed rows between periodic join cache evictions.
    /// Values smaller than 1 will be clamped to 1 by the executor.
    #[serde(default = "default::developer::streaming_join_hash_map_evict_interval_rows")]
    pub join_hash_map_evict_interval_rows: u32,

    #[serde(default = "default::developer::streaming_now_progress_ratio")]
    pub now_progress_ratio: Option<f32>,

    /// Enable / Disable profiling stats used by `EXPLAIN ANALYZE`
    #[serde(default = "default::developer::enable_explain_analyze_stats")]
    pub enable_explain_analyze_stats: bool,

    #[serde(default)]
    pub compute_client_config: RpcClientConfig,

    /// The interval in seconds to rebuild snapshot iterators during snapshot backfill.
    #[serde(default = "default::developer::stream_snapshot_iter_rebuild_interval_secs")]
    pub snapshot_iter_rebuild_interval_secs: u64,

    /// `IcebergListExecutor`: The interval in seconds for Iceberg source to list new files.
    #[serde(default = "default::developer::iceberg_list_interval_sec")]
    pub iceberg_list_interval_sec: u64,

    /// `IcebergFetchExecutor`: The number of files the executor will fetch concurrently in a batch.
    #[serde(default = "default::developer::iceberg_fetch_batch_size")]
    pub iceberg_fetch_batch_size: u64,

    /// `IcebergSink`: The size of the cache for positional delete in the sink.
    #[serde(default = "default::developer::iceberg_sink_positional_delete_cache_size")]
    pub iceberg_sink_positional_delete_cache_size: usize,

    /// `IcebergSink`: The maximum number of rows in a row group when writing Parquet files.
    #[serde(default = "default::developer::iceberg_sink_write_parquet_max_row_group_rows")]
    pub iceberg_sink_write_parquet_max_row_group_rows: usize,

    /// When enabled, materialized views using default `NoCheck` conflict behavior will be forced
    /// to use `Overwrite`. Useful to avoid propagating inconsistent changelog downstream.
    #[serde(default = "default::developer::materialize_force_overwrite_on_no_check")]
    pub materialize_force_overwrite_on_no_check: bool,

    /// Whether by default enable preloading all rows in memory for state table.
    /// If true, all capable state tables will preload its state to memory
    #[serde(default = "default::streaming::default_enable_mem_preload_state_table")]
    pub default_enable_mem_preload_state_table: bool,

    /// The list of state table ids to *enable* preloading all rows in memory for state table.
    /// Only takes effect when `default_enable_mem_preload_state_table` is false.
    #[serde(default)]
    pub mem_preload_state_table_ids_whitelist: Vec<u32>,

    /// The list of state table ids to *disable* preloading all rows in memory for state table.
    /// Only takes effect when `default_enable_mem_preload_state_table` is true.
    #[serde(default)]
    pub mem_preload_state_table_ids_blacklist: Vec<u32>,

    /// Eliminate unnecessary updates aggressively, even if it impacts performance. Enable this
    /// only if it's confirmed that no-op updates are causing significant streaming amplification.
    #[serde(default)]
    pub aggressive_noop_update_elimination: bool,

    /// The interval in seconds for the refresh scheduler to check and trigger scheduled refreshes.
    #[serde(default = "default::developer::refresh_scheduler_interval_sec")]
    pub refresh_scheduler_interval_sec: u64,

    /// Determine which encoding will be used to encode join rows in operator cache.
    #[serde(default)]
    pub join_encoding_type: JoinEncodingType,

    /// The timeout for reading from the buffer of the sync log store on barrier.
    /// Every epoch we will attempt to read the full buffer of the sync log store.
    /// If we hit the timeout, we will stop reading and continue.
    #[serde(default = "default::developer::sync_log_store_pause_duration_ms")]
    pub sync_log_store_pause_duration_ms: usize,

    /// The max buffer size for sync logstore, before we start flushing.
    #[serde(default = "default::developer::sync_log_store_buffer_size")]
    pub sync_log_store_buffer_size: usize,

    /// Disable the optimized dispatcher path for sync log store.
    #[serde(default = "default::developer::disable_sync_log_store_dispatcher")]
    pub disable_sync_log_store_dispatcher: bool,

    /// Cache policy for partition cache in streaming over window.
    /// Can be `full`, `recent`, `recent_first_n` or `recent_last_n`.
    #[serde(default)]
    pub over_window_cache_policy: OverWindowCachePolicy,

    /// When enabled, vnode stats pruning is applied in production.
    /// When disabled, vnode stats pruning is in dry-run mode: we still maintain vnode stats
    /// and verify that pruning would be correct, but we don't actually use the pruning
    /// results — we still use cache and storage to fulfill the read. This is useful for
    /// validating the correctness of vnode stats pruning before enabling it in production.
    #[serde(default = "default::developer::enable_state_table_vnode_stats_pruning")]
    pub enable_state_table_vnode_stats_pruning: bool,

    /// Runtime switch for the sort buffer of `LocalityProviderExecutor`. Only takes effect on
    /// streaming jobs planned with the `enable_locality_sort_buffer` session variable enabled.
    /// Setting it to false makes such locality providers always pass through input directly.
    #[serde(default = "default::developer::enable_locality_sort_buffer")]
    pub enable_locality_sort_buffer: bool,

    /// The number of input rows within one epoch after which the sort buffer of
    /// `LocalityProviderExecutor` activates. Below this threshold, input is passed through
    /// directly without buffering, so that no extra latency is introduced for normal workloads.
    #[serde(default = "default::developer::locality_sort_buffer_activate_threshold")]
    pub locality_sort_buffer_activate_threshold: u64,

    /// Cache refill policy for streaming cache refill feature.
    /// Can be `enabled`, `disabled`, `streaming`, `serving` or `both`.
    #[serde(default = "default::developer::cache_refill_policy")]
    pub cache_refill_policy: CacheRefillPolicy,

    /// Whether `MaterializeExecutor` enables vnode key stats for its state table.
    #[serde(default = "default::developer::enable_vnode_key_stats_for_materialize")]
    pub enable_vnode_key_stats_for_materialize: bool,

    /// The maximum number of kv log store readers that can concurrently read historical data
    /// (i.e., from the state store) during initialization. A reader is considered "initializing"
    /// until it has read at least one row from the historical stream or the stream returns empty.
    /// Set to 0 to disable the limit (unlimited concurrency).
    #[serde(default = "default::developer::max_concurrent_kv_log_store_historical_read")]
    pub max_concurrent_kv_log_store_historical_read: usize,

    #[serde(default, flatten)]
    #[serde_prefix_all(skip)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

impl StreamingDeveloperConfig {
    pub fn snapshot_iter_rebuild_interval(&self) -> Duration {
        let rebuild_interval = if self.snapshot_iter_rebuild_interval_secs < 10 {
            tracing::warn!(
                "too small rebuild_interval {} second. rewrite to 10",
                self.snapshot_iter_rebuild_interval_secs
            );
            10
        } else {
            self.snapshot_iter_rebuild_interval_secs
        };
        Duration::from_secs(rebuild_interval)
    }
}

impl StreamingConfig {
    /// Returns the dot-separated keys of all unrecognized fields, including those in `developer` section.
    pub fn unrecognized_keys(&self) -> impl Iterator<Item = String> {
        std::iter::from_coroutine(
            #[coroutine]
            || {
                for k in self.unrecognized.inner().keys() {
                    yield format!("streaming.{k}");
                }
                for k in self.developer.unrecognized.inner().keys() {
                    yield format!("streaming.developer.{k}");
                }
            },
        )
    }
}

pub mod default {
    pub use crate::config::default::developer;

    pub mod streaming {
        use tracing::info;

        use crate::config::AsyncStackTraceOption;
        use crate::util::env_var::env_var_is_true;

        pub fn in_flight_barrier_nums() -> usize {
            // quick fix
            // TODO: remove this limitation from code
            10000
        }

        pub fn snapshot_backfill_finish_max_lagged_barriers() -> usize {
            100
        }

        pub fn snapshot_backfill_barrier_amplification_factor() -> usize {
            1
        }

        pub fn async_stack_trace() -> AsyncStackTraceOption {
            AsyncStackTraceOption::default()
        }

        pub fn unique_user_stream_errors() -> usize {
            10
        }

        pub fn unsafe_disable_strict_consistency() -> bool {
            false
        }

        pub fn default_enable_mem_preload_state_table() -> bool {
            if env_var_is_true("DEFAULT_ENABLE_MEM_PRELOAD_STATE_TABLE") {
                info!("enabled mem_preload_state_table globally by env var");
                true
            } else {
                false
            }
        }
    }
}
