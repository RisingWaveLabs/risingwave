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

use foyer::{Compression, LfuConfig, LruConfig, RecoverMode, S3FifoConfig, Throttle};
use serde::de::Error as _;

use super::*;

/// The section `[storage]` in `risingwave.toml`.
#[serde_with::apply(Option => #[serde(with = "none_as_empty_string")])]
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct StorageConfig {
    /// parallelism while syncing share buffers into L0 SST. Should NOT be 0.
    #[serde(default = "default::storage::share_buffers_sync_parallelism")]
    pub share_buffers_sync_parallelism: u32,

    /// Worker threads number of dedicated tokio runtime for share buffer compaction. 0 means use
    /// tokio's default value (number of CPU core).
    #[serde(default = "default::storage::share_buffer_compaction_worker_threads_number")]
    pub share_buffer_compaction_worker_threads_number: u32,

    /// Configure the maximum shared buffer size in MB explicitly. Writes attempting to exceed the capacity
    /// will stall until there is enough space. The overridden value will only be effective if:
    /// 1. `block_cache_capacity_mb` and `meta_cache_capacity_mb` are also configured explicitly.
    /// 2. `block_cache_capacity_mb` + `meta_cache_capacity_mb` + `meta_cache_capacity_mb` doesn't exceed 0.3 * non-reserved memory.
    #[serde(default)]
    pub shared_buffer_capacity_mb: Option<usize>,

    /// The shared buffer will start flushing data to object when the ratio of memory usage to the
    /// shared buffer capacity exceed such ratio.
    #[serde(default = "default::storage::shared_buffer_flush_ratio")]
    pub shared_buffer_flush_ratio: f32,

    /// The minimum total flush size of shared buffer spill. When a shared buffer spilled is trigger,
    /// the total flush size across multiple epochs should be at least higher than this size.
    #[serde(default = "default::storage::shared_buffer_min_batch_flush_size_mb")]
    pub shared_buffer_min_batch_flush_size_mb: usize,

    /// The threshold for the number of immutable memtables to merge to a new imm.
    #[serde(default = "default::storage::imm_merge_threshold")]
    #[deprecated]
    pub imm_merge_threshold: usize,

    /// Whether to enable write conflict detection
    #[serde(default = "default::storage::write_conflict_detection_enabled")]
    pub write_conflict_detection_enabled: bool,

    #[serde(default)]
    #[config_doc(nested)]
    pub cache: CacheConfig,

    /// DEPRECATED: This config will be deprecated in the future version, use `storage.cache.block_cache_capacity_mb` instead.
    #[serde(default)]
    pub block_cache_capacity_mb: Option<usize>,

    /// DEPRECATED: This config will be deprecated in the future version, use `storage.cache.meta_cache_capacity_mb` instead.
    #[serde(default)]
    pub meta_cache_capacity_mb: Option<usize>,

    /// DEPRECATED: This config will be deprecated in the future version, use `storage.cache.block_cache_eviction.high_priority_ratio_in_percent` with `storage.cache.block_cache_eviction.algorithm = "Lru"` instead.
    #[serde(default)]
    pub high_priority_ratio_in_percent: Option<usize>,

    /// max memory usage for large query
    #[serde(default)]
    pub prefetch_buffer_capacity_mb: Option<usize>,

    #[serde(default = "default::storage::max_cached_recent_versions_number")]
    pub max_cached_recent_versions_number: usize,

    /// max prefetch block number
    #[serde(
        default = "default::storage::max_prefetch_block_number",
        deserialize_with = "deserialize_max_prefetch_block_number"
    )]
    pub max_prefetch_block_number: usize,

    #[serde(default = "default::storage::disable_remote_compactor")]
    pub disable_remote_compactor: bool,

    /// Number of tasks shared buffer can upload in parallel.
    #[serde(default = "default::storage::share_buffer_upload_concurrency")]
    pub share_buffer_upload_concurrency: usize,

    #[serde(default)]
    pub compactor_memory_limit_mb: Option<usize>,

    /// Compactor calculates the maximum number of tasks that can be executed on the node based on
    /// `worker_num` and `compactor_max_task_multiplier`.
    /// `max_pull_task_count` = `worker_num` * `compactor_max_task_multiplier`
    #[serde(default = "default::storage::compactor_max_task_multiplier")]
    pub compactor_max_task_multiplier: f32,

    /// The percentage of memory available when compactor is deployed separately.
    /// `non_reserved_memory_bytes` = `system_memory_available_bytes` * `compactor_memory_available_proportion`
    #[serde(default = "default::storage::compactor_memory_available_proportion")]
    pub compactor_memory_available_proportion: f64,

    /// Number of SST ids fetched from meta per RPC
    #[serde(default = "default::storage::sstable_id_remote_fetch_number")]
    pub sstable_id_remote_fetch_number: u32,

    #[serde(default = "default::storage::min_sstable_size_mb")]
    pub min_sstable_size_mb: u32,

    #[serde(default)]
    #[config_doc(nested)]
    pub data_file_cache: FileCacheConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub meta_file_cache: FileCacheConfig,

    /// sst serde happens when a sst meta is written to meta disk cache.
    /// Excluding the SST filter from serde can reduce the meta disk cache entry size
    /// and reduce disk IO throughput at the cost of making the SST filter useless.
    #[serde(default = "default::storage::sst_skip_bloom_filter_in_serde")]
    pub sst_skip_bloom_filter_in_serde: bool,

    #[serde(default)]
    #[config_doc(nested)]
    pub cache_refill: CacheRefillConfig,

    /// Whether to enable streaming upload for sstable.
    #[serde(default = "default::storage::min_sst_size_for_streaming_upload")]
    pub min_sst_size_for_streaming_upload: u64,

    #[serde(default = "default::storage::max_concurrent_compaction_task_number")]
    pub max_concurrent_compaction_task_number: u64,

    #[serde(default = "default::storage::max_preload_wait_time_mill")]
    pub max_preload_wait_time_mill: u64,

    #[serde(default = "default::storage::max_version_pinning_duration_sec")]
    pub max_version_pinning_duration_sec: u64,

    #[serde(default = "default::storage::compactor_max_sst_key_count")]
    pub compactor_max_sst_key_count: u64,
    // DEPRECATED: This config will be deprecated in the future version, use `storage.compactor_iter_max_io_retry_times` instead.
    #[serde(default = "default::storage::compact_iter_recreate_timeout_ms")]
    pub compact_iter_recreate_timeout_ms: u64,
    #[serde(default = "default::storage::compactor_max_sst_size")]
    pub compactor_max_sst_size: u64,
    #[serde(default = "default::storage::enable_fast_compaction")]
    pub enable_fast_compaction: bool,
    #[serde(default = "default::storage::check_compaction_result")]
    pub check_compaction_result: bool,
    #[serde(default = "default::storage::max_preload_io_retry_times")]
    pub max_preload_io_retry_times: usize,
    #[serde(default = "default::storage::compactor_fast_max_compact_delete_ratio")]
    pub compactor_fast_max_compact_delete_ratio: u32,
    #[serde(default = "default::storage::compactor_fast_max_compact_task_size")]
    pub compactor_fast_max_compact_task_size: u64,
    #[serde(default = "default::storage::compactor_iter_max_io_retry_times")]
    pub compactor_iter_max_io_retry_times: usize,

    /// If set, block metadata keys will be shortened when their length exceeds this threshold.
    /// This reduces `SSTable` metadata size by storing only the minimal distinguishing prefix.
    /// - `None`: Disabled (default)
    /// - `Some(n)`: Only shorten keys with length >= n bytes
    #[serde(default = "default::storage::shorten_block_meta_key_threshold")]
    pub shorten_block_meta_key_threshold: Option<usize>,

    /// Deprecated: The window size of table info statistic history.
    #[serde(default = "default::storage::table_info_statistic_history_times")]
    #[deprecated]
    pub table_info_statistic_history_times: usize,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,

    /// The spill threshold for mem table.
    #[serde(default = "default::storage::mem_table_spill_threshold")]
    pub mem_table_spill_threshold: usize,

    /// The concurrent uploading number of `SSTables` of builder
    #[serde(default = "default::storage::compactor_concurrent_uploading_sst_count")]
    pub compactor_concurrent_uploading_sst_count: Option<usize>,

    #[serde(default = "default::storage::compactor_max_overlap_sst_count")]
    pub compactor_max_overlap_sst_count: usize,

    /// The maximum number of meta files that can be preloaded.
    /// If the number of meta files exceeds this value, the compactor will try to compute parallelism only through `SstableInfo`, no longer preloading `SstableMeta`.
    /// This is to prevent the compactor from consuming too much memory, but it may cause the compactor to be less efficient.
    #[serde(default = "default::storage::compactor_max_preload_meta_file_count")]
    pub compactor_max_preload_meta_file_count: usize,

    #[serde(default = "default::storage::vector_file_block_size_kb")]
    pub vector_file_block_size_kb: usize,

    /// Object storage configuration
    /// 1. General configuration
    /// 2. Some special configuration of Backend
    /// 3. Retry and timeout configuration
    #[serde(default)]
    pub object_store: ObjectStoreConfig,

    #[serde(default = "default::storage::time_travel_version_cache_capacity")]
    pub time_travel_version_cache_capacity: u64,

    #[serde(default = "default::storage::table_change_log_cache_capacity")]
    pub table_change_log_cache_capacity: u64,

    // iceberg compaction
    /// Estimated heap memory budget used to schedule tasks in the dedicated Iceberg compactor, in
    /// megabytes. This controls admission only; it is not a hard `DataFusion` allocation limit.
    /// When unset, the budget is derived from the compactor's available memory.
    #[serde(default)]
    pub iceberg_compaction_memory_limit_mb: Option<usize>,
    #[serde(default = "default::storage::iceberg_compaction_enable_validate")]
    pub iceberg_compaction_enable_validate: bool,
    #[serde(default = "default::storage::iceberg_compaction_max_record_batch_rows")]
    pub iceberg_compaction_max_record_batch_rows: usize,
    #[serde(default = "default::storage::iceberg_compaction_min_size_per_partition_mb")]
    pub iceberg_compaction_min_size_per_partition_mb: u32,
    #[serde(default = "default::storage::iceberg_compaction_max_file_count_per_partition")]
    pub iceberg_compaction_max_file_count_per_partition: u32,
    /// DEPRECATED: This config will be deprecated in the future version.
    /// Use sink config `compaction.write_parquet_max_row_group_rows` instead.
    #[serde(default = "default::storage::iceberg_compaction_write_parquet_max_row_group_rows")]
    #[deprecated(
        note = "This config is deprecated. Use sink config `compaction.write_parquet_max_row_group_rows` instead."
    )]
    pub iceberg_compaction_write_parquet_max_row_group_rows: usize,

    /// The ratio of iceberg compaction max parallelism to the number of CPU cores
    #[serde(default = "default::storage::iceberg_compaction_task_parallelism_ratio")]
    pub iceberg_compaction_task_parallelism_ratio: f32,
    /// Whether to enable heuristic output parallelism in iceberg compaction.
    #[serde(default = "default::storage::iceberg_compaction_enable_heuristic_output_parallelism")]
    pub iceberg_compaction_enable_heuristic_output_parallelism: bool,
    /// Maximum number of concurrent file close operations
    #[serde(default = "default::storage::iceberg_compaction_max_concurrent_closes")]
    pub iceberg_compaction_max_concurrent_closes: usize,
    /// Whether to enable dynamic size estimation for iceberg compaction.
    #[serde(default = "default::storage::iceberg_compaction_enable_dynamic_size_estimation")]
    pub iceberg_compaction_enable_dynamic_size_estimation: bool,
    /// The smoothing factor for size estimation in iceberg compaction.(default: 0.3)
    #[serde(default = "default::storage::iceberg_compaction_size_estimation_smoothing_factor")]
    pub iceberg_compaction_size_estimation_smoothing_factor: f64,
    /// Multiplier for pending waiting parallelism budget for iceberg compaction task queue.
    /// Effective pending budget = `ceil(max_task_parallelism * multiplier)`. Default 4.0.
    /// Set < 1.0 to reduce buffering (may increase `PullTask` RPC frequency); set higher to batch more tasks.
    #[serde(
        default = "default::storage::iceberg_compaction_pending_parallelism_budget_multiplier"
    )]
    pub iceberg_compaction_pending_parallelism_budget_multiplier: f32,
    /// Pull interval for iceberg compaction task requests in milliseconds.
    #[serde(
        default = "default::storage::iceberg_compaction_pull_interval_ms",
        deserialize_with = "deserialize_iceberg_compaction_pull_interval_ms"
    )]
    pub iceberg_compaction_pull_interval_ms: u64,
    /// Enable prefetching entire data files before compacting them.
    ///
    /// When enabled, each input file is downloaded with a single HTTP GET before compaction
    /// begins, replacing the default pattern of N+1 range reads (1 footer + N column chunks)
    /// with a single sequential read per file. This reduces object storage READ API calls
    /// from D×(1+N) to D per compaction cycle.
    ///
    /// Trade-off: higher peak memory — one full file is held in memory per concurrent
    /// compaction task. Enable only when object storage API cost is a priority and memory headroom is
    /// sufficient. See also the memory-protection config knobs such as
    /// `iceberg_compaction_task_parallelism_ratio`.
    #[serde(default = "default::storage::iceberg_compaction_enable_prefetch")]
    pub iceberg_compaction_enable_prefetch: bool,

    #[serde(default = "default::storage::iceberg_compaction_target_binpack_group_size_mb")]
    pub iceberg_compaction_target_binpack_group_size_mb: Option<u64>,
    #[serde(default = "default::storage::iceberg_compaction_min_group_size_mb")]
    pub iceberg_compaction_min_group_size_mb: Option<u64>,
    #[serde(default = "default::storage::iceberg_compaction_min_group_file_count")]
    pub iceberg_compaction_min_group_file_count: Option<usize>,
}

/// the section `[storage.cache]` in `risingwave.toml`.
#[serde_with::apply(Option => #[serde(with = "none_as_empty_string")])]
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct CacheConfig {
    /// Configure the capacity of the block cache in MB explicitly.
    /// The overridden value will only be effective if:
    /// 1. `meta_cache_capacity_mb` and `shared_buffer_capacity_mb` are also configured explicitly.
    /// 2. `block_cache_capacity_mb` + `meta_cache_capacity_mb` + `meta_cache_capacity_mb` doesn't exceed 0.3 * non-reserved memory.
    #[serde(default)]
    pub block_cache_capacity_mb: Option<usize>,

    /// Configure the number of shards in the block cache explicitly.
    /// If not set, the shard number will be determined automatically based on cache capacity.
    #[serde(default)]
    pub block_cache_shard_num: Option<usize>,

    #[serde(default)]
    #[config_doc(omitted)]
    pub block_cache_eviction: CacheEvictionConfig,

    /// Configure the capacity of the block cache in MB explicitly.
    /// The overridden value will only be effective if:
    /// 1. `block_cache_capacity_mb` and `shared_buffer_capacity_mb` are also configured explicitly.
    /// 2. `block_cache_capacity_mb` + `meta_cache_capacity_mb` + `meta_cache_capacity_mb` doesn't exceed 0.3 * non-reserved memory.
    #[serde(default)]
    pub meta_cache_capacity_mb: Option<usize>,

    /// Configure the number of shards in the meta cache explicitly.
    /// If not set, the shard number will be determined automatically based on cache capacity.
    #[serde(default)]
    pub meta_cache_shard_num: Option<usize>,

    #[serde(default)]
    #[config_doc(omitted)]
    pub meta_cache_eviction: CacheEvictionConfig,

    #[serde(default = "default::storage::vector_block_cache_capacity_mb")]
    pub vector_block_cache_capacity_mb: usize,
    #[serde(default = "default::storage::vector_block_cache_shard_num")]
    pub vector_block_cache_shard_num: usize,
    #[serde(default)]
    #[config_doc(omitted)]
    pub vector_block_cache_eviction_config: CacheEvictionConfig,
    #[serde(default = "default::storage::vector_meta_cache_capacity_mb")]
    pub vector_meta_cache_capacity_mb: usize,
    #[serde(default = "default::storage::vector_meta_cache_shard_num")]
    pub vector_meta_cache_shard_num: usize,
    #[serde(default)]
    #[config_doc(omitted)]
    pub vector_meta_cache_eviction_config: CacheEvictionConfig,
}

/// the section `[storage.cache.eviction]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "algorithm")]
pub enum CacheEvictionConfig {
    Lru {
        high_priority_ratio_in_percent: Option<usize>,
    },
    Lfu {
        window_capacity_ratio_in_percent: Option<usize>,
        protected_capacity_ratio_in_percent: Option<usize>,
        cmsketch_eps: Option<f64>,
        cmsketch_confidence: Option<f64>,
    },
    S3Fifo {
        small_queue_capacity_ratio_in_percent: Option<usize>,
        ghost_queue_capacity_ratio_in_percent: Option<usize>,
        small_to_main_freq_threshold: Option<u8>,
    },
}

impl Default for CacheEvictionConfig {
    fn default() -> Self {
        Self::Lru {
            high_priority_ratio_in_percent: None,
        }
    }
}

#[serde_with::apply(Option => #[serde(with = "none_as_empty_string")])]
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct CacheRefillConfig {
    /// Inflight meta cache refill tasks limit.
    ///
    /// 0 for unlimited.
    #[serde(default = "default::cache_refill::meta_refill_concurrency")]
    pub meta_refill_concurrency: usize,

    /// `SSTable` levels to refill.
    #[serde(default = "default::cache_refill::data_refill_levels")]
    pub data_refill_levels: Vec<u32>,

    /// Cache refill maximum timeout to apply version delta.
    #[serde(default = "default::cache_refill::timeout_ms")]
    pub timeout_ms: u64,

    /// Inflight data cache refill tasks.
    #[serde(default = "default::cache_refill::concurrency")]
    pub concurrency: usize,

    /// Block count that a data cache refill request fetches.
    #[serde(default = "default::cache_refill::unit")]
    pub unit: usize,

    /// Data cache refill unit admission ratio.
    ///
    /// Only unit whose blocks are admitted above the ratio will be refilled.
    #[serde(default = "default::cache_refill::threshold")]
    pub threshold: f64,

    /// Recent filter layer shards.
    #[serde(default = "default::cache_refill::recent_filter_shards")]
    pub recent_filter_shards: usize,

    /// Recent filter layer count.
    #[serde(default = "default::cache_refill::recent_filter_layers")]
    pub recent_filter_layers: usize,

    /// Recent filter layer rotate interval.
    #[serde(default = "default::cache_refill::recent_filter_rotate_interval_ms")]
    pub recent_filter_rotate_interval_ms: usize,

    /// Skip checking recent filter on data refill.
    ///
    /// This option is suitable for a single compute node or debugging.
    #[serde(default = "default::cache_refill::skip_recent_filter")]
    pub skip_recent_filter: bool,

    /// Skip checking inheritance filter on data refill.
    ///
    /// The inheritance filter only runs after recent-filter admission, so this
    /// option has no effect when `skip_recent_filter` is enabled.
    ///
    /// This option is suitable for a single compute node or debugging.
    #[serde(default = "default::cache_refill::skip_inheritance_filter")]
    pub skip_inheritance_filter: bool,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FileCacheTokioRuntimeConfig {
    /// Dedicated runtime worker threads. `0` uses the Tokio default.
    pub worker_threads: usize,

    /// Maximum number of blocking threads. `0` uses the Tokio default.
    pub max_blocking_threads: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FileCacheRuntimeConfig {
    /// Use the runtime that creates the file cache.
    Disabled,
    /// Use one dedicated runtime for all file cache tasks.
    Unified(FileCacheTokioRuntimeConfig),
    /// Legacy configuration for separate read and write runtimes.
    ///
    /// Foyer 0.22 uses one spawner, so this configuration is no longer supported.
    Separated {
        read_runtime_options: FileCacheTokioRuntimeConfig,
        write_runtime_options: FileCacheTokioRuntimeConfig,
    },
}

/// The subsection `[storage.data_file_cache]` and `[storage.meta_file_cache]` in `risingwave.toml`.
///
/// It's put at [`StorageConfig::data_file_cache`] and  [`StorageConfig::meta_file_cache`].
#[serde_with::apply(Option => #[serde(with = "none_as_empty_string")])]
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct FileCacheConfig {
    #[serde(default = "default::file_cache::dir")]
    pub dir: String,

    #[serde(default = "default::file_cache::capacity_mb")]
    pub capacity_mb: usize,

    #[serde(default = "default::file_cache::file_capacity_mb")]
    pub file_capacity_mb: usize,

    #[serde(default = "default::file_cache::flushers")]
    pub flushers: usize,

    #[serde(default = "default::file_cache::reclaimers")]
    pub reclaimers: usize,

    #[serde(default = "default::file_cache::recover_concurrency")]
    pub recover_concurrency: usize,

    /// Deprecated soon. Please use `throttle` to do I/O throttling instead.
    #[serde(default = "default::file_cache::insert_rate_limit_mb")]
    pub insert_rate_limit_mb: usize,

    #[serde(default = "default::file_cache::indexer_shards")]
    pub indexer_shards: usize,

    #[serde(default = "default::file_cache::compression")]
    pub compression: Compression,

    #[serde(default = "default::file_cache::flush_buffer_threshold_mb")]
    pub flush_buffer_threshold_mb: Option<usize>,

    /// Maximum estimated size of cache entries waiting in the flusher submit queues. New cache
    /// entries are ignored when the pending size exceeds this threshold.
    #[serde(default = "default::file_cache::submit_queue_size_threshold_mb")]
    pub submit_queue_size_threshold_mb: usize,

    #[serde(default = "default::file_cache::throttle")]
    pub throttle: Throttle,

    #[serde(default = "default::file_cache::fifo_probation_ratio")]
    pub fifo_probation_ratio: f64,

    /// Set the blob index size for each blob.
    ///
    /// A larger blob index size can hold more blob entries, but it will also increase the io size of each blob part
    /// write.
    ///
    /// NOTE:
    ///
    /// - The size will be aligned up to a multiplier of 4K.
    /// - Modifying this configuration will invalidate all existing file cache data.
    ///
    /// Default: 16 `KiB`
    #[serde(default = "default::file_cache::blob_index_size_kb")]
    pub blob_index_size_kb: usize,

    /// Recover mode.
    ///
    /// Options:
    ///
    /// - "None": Do not recover disk cache.
    /// - "Quiet": Recover disk cache and skip errors.
    /// - "Strict": Recover disk cache and panic on errors.
    ///
    /// More details, see [`RecoverMode::None`], [`RecoverMode::Quiet`] and [`RecoverMode::Strict`],
    #[serde(default = "default::file_cache::recover_mode")]
    pub recover_mode: RecoverMode,

    #[serde(default = "default::file_cache::runtime_config")]
    pub runtime_config: FileCacheRuntimeConfig,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

/// The subsections `[storage.object_store]`.
#[serde_with::apply(Option => #[serde(with = "none_as_empty_string")])]
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct ObjectStoreConfig {
    // alias is for backward compatibility
    #[serde(
        default = "default::object_store_config::set_atomic_write_dir",
        alias = "object_store_set_atomic_write_dir"
    )]
    pub set_atomic_write_dir: bool,

    /// Retry and timeout configuration
    /// Description retry strategy driven by exponential back-off
    /// Exposes the timeout and retries of each Object store interface. Therefore, the total timeout for each interface is determined based on the interface's timeout/retry configuration and the exponential back-off policy.
    #[serde(default)]
    pub retry: ObjectStoreRetryConfig,

    /// Some special configuration of S3 Backend
    #[serde(default)]
    pub s3: S3ObjectStoreConfig,

    /// Maximum number of concurrent object store requests (read, `streaming_read`, metadata, etc.).
    /// 0 means unlimited. When set to a positive value, a semaphore will be used to limit
    /// the number of in-flight requests to the object store, preventing HTTP connection pool
    /// contention under high concurrency.
    #[serde(default = "default::object_store_config::object_store_req_concurrency_limit")]
    pub req_concurrency_limit: usize,

    /// Maximum number of concurrent HTTP requests used by the `OpenDAL` GCS backend.
    /// 0 means unlimited.
    #[serde(default = "default::object_store_config::http_concurrent_limit")]
    pub http_concurrent_limit: usize,

    // TODO: the following field will be deprecated after opendal is stabilized
    #[serde(default = "default::object_store_config::opendal_upload_concurrency")]
    pub opendal_upload_concurrency: usize,

    // TODO: the following field will be deprecated after opendal is stabilized
    #[serde(default)]
    pub opendal_writer_abort_on_err: bool,

    #[serde(default = "default::object_store_config::upload_part_size")]
    pub upload_part_size: usize,
}

fn deserialize_max_prefetch_block_number<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = usize::deserialize(deserializer)?;
    if value == 0 {
        return Err(D::Error::custom(
            "storage.max_prefetch_block_number must be greater than 0",
        ));
    }
    Ok(value)
}

fn deserialize_iceberg_compaction_pull_interval_ms<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    if value == 0 {
        return Err(D::Error::custom(
            "storage.iceberg_compaction_pull_interval_ms must be greater than 0",
        ));
    }
    Ok(value)
}

impl ObjectStoreConfig {
    pub fn set_atomic_write_dir(&mut self) {
        self.set_atomic_write_dir = true;
    }
}

/// The subsections `[storage.object_store.s3]`.
#[serde_with::apply(Option => #[serde(with = "none_as_empty_string")])]
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct S3ObjectStoreConfig {
    // alias is for backward compatibility
    #[serde(
        default = "default::object_store_config::s3::keepalive_ms",
        alias = "object_store_keepalive_ms"
    )]
    pub keepalive_ms: Option<u64>,
    #[serde(
        default = "default::object_store_config::s3::recv_buffer_size",
        alias = "object_store_recv_buffer_size"
    )]
    pub recv_buffer_size: Option<usize>,
    #[serde(
        default = "default::object_store_config::s3::send_buffer_size",
        alias = "object_store_send_buffer_size"
    )]
    pub send_buffer_size: Option<usize>,
    #[serde(
        default = "default::object_store_config::s3::nodelay",
        alias = "object_store_nodelay"
    )]
    pub nodelay: Option<bool>,
    /// For backwards compatibility, users should use `S3ObjectStoreDeveloperConfig` instead.
    #[serde(default = "default::object_store_config::s3::developer::retry_unknown_service_error")]
    pub retry_unknown_service_error: bool,
    #[serde(default = "default::object_store_config::s3::identity_resolution_timeout_s")]
    pub identity_resolution_timeout_s: u64,
    #[serde(default)]
    pub developer: S3ObjectStoreDeveloperConfig,
}

/// The subsections `[storage.object_store.s3.developer]`.
#[serde_with::apply(Option => #[serde(with = "none_as_empty_string")])]
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct S3ObjectStoreDeveloperConfig {
    /// Whether to retry s3 sdk error from which no error metadata is provided.
    #[serde(
        default = "default::object_store_config::s3::developer::retry_unknown_service_error",
        alias = "object_store_retry_unknown_service_error"
    )]
    pub retry_unknown_service_error: bool,
    /// An array of error codes that should be retried.
    /// e.g. `["SlowDown", "TooManyRequests"]`
    #[serde(
        default = "default::object_store_config::s3::developer::retryable_service_error_codes",
        alias = "object_store_retryable_service_error_codes"
    )]
    pub retryable_service_error_codes: Vec<String>,

    // TODO: deprecate this config when we are completely deprecate aws sdk.
    #[serde(default = "default::object_store_config::s3::developer::use_opendal")]
    pub use_opendal: bool,
}

#[serde_with::apply(Option => #[serde(with = "none_as_empty_string")])]
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct ObjectStoreRetryConfig {
    // A retry strategy driven by exponential back-off.
    // The retry strategy is used for all object store operations.
    /// Given a base duration for retry strategy in milliseconds.
    #[serde(default = "default::object_store_config::object_store_req_backoff_interval_ms")]
    pub req_backoff_interval_ms: u64,

    /// The max delay interval for the retry strategy. No retry delay will be longer than this `Duration`.
    #[serde(default = "default::object_store_config::object_store_req_backoff_max_delay_ms")]
    pub req_backoff_max_delay_ms: u64,

    /// A multiplicative factor that will be applied to the exponential back-off retry delay.
    #[serde(default = "default::object_store_config::object_store_req_backoff_factor")]
    pub req_backoff_factor: u64,

    /// Maximum timeout for `upload` operation
    #[serde(default = "default::object_store_config::object_store_upload_attempt_timeout_ms")]
    pub upload_attempt_timeout_ms: u64,

    /// Total counts of `upload` operation retries
    #[serde(default = "default::object_store_config::object_store_upload_retry_attempts")]
    pub upload_retry_attempts: usize,

    /// Maximum timeout for `streaming_upload_init` and `streaming_upload`
    #[serde(
        default = "default::object_store_config::object_store_streaming_upload_attempt_timeout_ms"
    )]
    pub streaming_upload_attempt_timeout_ms: u64,

    /// Total counts of `streaming_upload` operation retries
    #[serde(
        default = "default::object_store_config::object_store_streaming_upload_retry_attempts"
    )]
    pub streaming_upload_retry_attempts: usize,

    /// Maximum timeout for `read` operation
    #[serde(default = "default::object_store_config::object_store_read_attempt_timeout_ms")]
    pub read_attempt_timeout_ms: u64,

    /// Total counts of `read` operation retries
    #[serde(default = "default::object_store_config::object_store_read_retry_attempts")]
    pub read_retry_attempts: usize,

    /// Maximum timeout for `streaming_read_init` and `streaming_read` operation
    #[serde(
        default = "default::object_store_config::object_store_streaming_read_attempt_timeout_ms"
    )]
    pub streaming_read_attempt_timeout_ms: u64,

    /// Total counts of `streaming_read operation` retries
    #[serde(default = "default::object_store_config::object_store_streaming_read_retry_attempts")]
    pub streaming_read_retry_attempts: usize,

    /// Maximum timeout for `metadata` operation
    #[serde(default = "default::object_store_config::object_store_metadata_attempt_timeout_ms")]
    pub metadata_attempt_timeout_ms: u64,

    /// Total counts of `metadata` operation retries
    #[serde(default = "default::object_store_config::object_store_metadata_retry_attempts")]
    pub metadata_retry_attempts: usize,

    /// Maximum timeout for `delete` operation
    #[serde(default = "default::object_store_config::object_store_delete_attempt_timeout_ms")]
    pub delete_attempt_timeout_ms: u64,

    /// Total counts of `delete` operation retries
    #[serde(default = "default::object_store_config::object_store_delete_retry_attempts")]
    pub delete_retry_attempts: usize,

    /// Maximum timeout for `delete_object` operation
    #[serde(
        default = "default::object_store_config::object_store_delete_objects_attempt_timeout_ms"
    )]
    pub delete_objects_attempt_timeout_ms: u64,

    /// Total counts of `delete_object` operation retries
    #[serde(default = "default::object_store_config::object_store_delete_objects_retry_attempts")]
    pub delete_objects_retry_attempts: usize,

    /// Maximum timeout for `list` operation
    #[serde(default = "default::object_store_config::object_store_list_attempt_timeout_ms")]
    pub list_attempt_timeout_ms: u64,

    /// Total counts of `list` operation retries
    #[serde(default = "default::object_store_config::object_store_list_retry_attempts")]
    pub list_retry_attempts: usize,
}

#[derive(Debug, Clone)]
pub enum EvictionConfig {
    Lru(LruConfig),
    Lfu(LfuConfig),
    S3Fifo(S3FifoConfig),
}

impl EvictionConfig {
    pub fn for_test() -> Self {
        Self::Lru(LruConfig {
            high_priority_pool_ratio: 0.0,
        })
    }
}

impl From<EvictionConfig> for foyer::EvictionConfig {
    fn from(value: EvictionConfig) -> Self {
        match value {
            EvictionConfig::Lru(lru) => foyer::EvictionConfig::Lru(lru),
            EvictionConfig::Lfu(lfu) => foyer::EvictionConfig::Lfu(lfu),
            EvictionConfig::S3Fifo(s3fifo) => foyer::EvictionConfig::S3Fifo(s3fifo),
        }
    }
}

pub struct StorageMemoryConfig {
    pub block_cache_capacity_mb: usize,
    pub block_cache_shard_num: usize,
    pub meta_cache_capacity_mb: usize,
    pub meta_cache_shard_num: usize,
    pub vector_block_cache_capacity_mb: usize,
    pub vector_block_cache_shard_num: usize,
    pub vector_meta_cache_capacity_mb: usize,
    pub vector_meta_cache_shard_num: usize,
    pub shared_buffer_capacity_mb: usize,
    pub compactor_memory_limit_mb: usize,
    pub prefetch_buffer_capacity_mb: usize,
    pub block_cache_eviction_config: EvictionConfig,
    pub meta_cache_eviction_config: EvictionConfig,
    pub vector_block_cache_eviction_config: EvictionConfig,
    pub vector_meta_cache_eviction_config: EvictionConfig,
    pub block_file_cache_flush_buffer_threshold_mb: usize,
    pub meta_file_cache_flush_buffer_threshold_mb: usize,
}

pub fn extract_storage_memory_config(s: &RwConfig) -> StorageMemoryConfig {
    let block_cache_capacity_mb = s.storage.cache.block_cache_capacity_mb.unwrap_or(
        // adapt to old version
        s.storage
            .block_cache_capacity_mb
            .unwrap_or(default::storage::block_cache_capacity_mb()),
    );
    let meta_cache_capacity_mb = s.storage.cache.meta_cache_capacity_mb.unwrap_or(
        // adapt to old version
        s.storage
            .block_cache_capacity_mb
            .unwrap_or(default::storage::meta_cache_capacity_mb()),
    );
    let shared_buffer_capacity_mb = s
        .storage
        .shared_buffer_capacity_mb
        .unwrap_or(default::storage::shared_buffer_capacity_mb());
    let meta_cache_shard_num = s.storage.cache.meta_cache_shard_num.unwrap_or_else(|| {
        let mut shard_bits = MAX_META_CACHE_SHARD_BITS;
        while (meta_cache_capacity_mb >> shard_bits) < MIN_BUFFER_SIZE_PER_SHARD && shard_bits > 0 {
            shard_bits -= 1;
        }
        shard_bits
    });
    let block_cache_shard_num = s.storage.cache.block_cache_shard_num.unwrap_or_else(|| {
        let mut shard_bits = MAX_BLOCK_CACHE_SHARD_BITS;
        while (block_cache_capacity_mb >> shard_bits) < MIN_BUFFER_SIZE_PER_SHARD && shard_bits > 0
        {
            shard_bits -= 1;
        }
        shard_bits
    });
    let compactor_memory_limit_mb = s
        .storage
        .compactor_memory_limit_mb
        .unwrap_or(default::storage::compactor_memory_limit_mb());

    let get_eviction_config = |c: &CacheEvictionConfig| {
        match c {
            CacheEvictionConfig::Lru {
                high_priority_ratio_in_percent,
            } => EvictionConfig::Lru(LruConfig {
                high_priority_pool_ratio: high_priority_ratio_in_percent.unwrap_or(
                    // adapt to old version
                    s.storage
                        .high_priority_ratio_in_percent
                        .unwrap_or(default::storage::high_priority_ratio_in_percent()),
                ) as f64
                    / 100.0,
            }),
            CacheEvictionConfig::Lfu {
                window_capacity_ratio_in_percent,
                protected_capacity_ratio_in_percent,
                cmsketch_eps,
                cmsketch_confidence,
            } => EvictionConfig::Lfu(LfuConfig {
                window_capacity_ratio: window_capacity_ratio_in_percent
                    .unwrap_or(default::storage::window_capacity_ratio_in_percent())
                    as f64
                    / 100.0,
                protected_capacity_ratio: protected_capacity_ratio_in_percent
                    .unwrap_or(default::storage::protected_capacity_ratio_in_percent())
                    as f64
                    / 100.0,
                cmsketch_eps: cmsketch_eps.unwrap_or(default::storage::cmsketch_eps()),
                cmsketch_confidence: cmsketch_confidence
                    .unwrap_or(default::storage::cmsketch_confidence()),
            }),
            CacheEvictionConfig::S3Fifo {
                small_queue_capacity_ratio_in_percent,
                ghost_queue_capacity_ratio_in_percent,
                small_to_main_freq_threshold,
            } => EvictionConfig::S3Fifo(S3FifoConfig {
                small_queue_capacity_ratio: small_queue_capacity_ratio_in_percent
                    .unwrap_or(default::storage::small_queue_capacity_ratio_in_percent())
                    as f64
                    / 100.0,
                ghost_queue_capacity_ratio: ghost_queue_capacity_ratio_in_percent
                    .unwrap_or(default::storage::ghost_queue_capacity_ratio_in_percent())
                    as f64
                    / 100.0,
                small_to_main_freq_threshold: small_to_main_freq_threshold
                    .unwrap_or(default::storage::small_to_main_freq_threshold()),
            }),
        }
    };

    let block_cache_eviction_config = get_eviction_config(&s.storage.cache.block_cache_eviction);
    let meta_cache_eviction_config = get_eviction_config(&s.storage.cache.meta_cache_eviction);
    let vector_block_cache_eviction_config =
        get_eviction_config(&s.storage.cache.vector_block_cache_eviction_config);
    let vector_meta_cache_eviction_config =
        get_eviction_config(&s.storage.cache.vector_meta_cache_eviction_config);

    let prefetch_buffer_capacity_mb =
        s.storage
            .shared_buffer_capacity_mb
            .unwrap_or(match &block_cache_eviction_config {
                EvictionConfig::Lru(lru) => {
                    ((1.0 - lru.high_priority_pool_ratio) * block_cache_capacity_mb as f64) as usize
                }
                EvictionConfig::Lfu(lfu) => {
                    ((1.0 - lfu.protected_capacity_ratio) * block_cache_capacity_mb as f64) as usize
                }
                EvictionConfig::S3Fifo(s3fifo) => {
                    (s3fifo.small_queue_capacity_ratio * block_cache_capacity_mb as f64) as usize
                }
            });

    let block_file_cache_flush_buffer_threshold_mb = s
        .storage
        .data_file_cache
        .flush_buffer_threshold_mb
        .unwrap_or(default::storage::block_file_cache_flush_buffer_threshold_mb());
    let meta_file_cache_flush_buffer_threshold_mb = s
        .storage
        .meta_file_cache
        .flush_buffer_threshold_mb
        .unwrap_or(default::storage::block_file_cache_flush_buffer_threshold_mb());

    StorageMemoryConfig {
        block_cache_capacity_mb,
        block_cache_shard_num,
        meta_cache_capacity_mb,
        meta_cache_shard_num,
        vector_block_cache_capacity_mb: s.storage.cache.vector_block_cache_capacity_mb,
        vector_block_cache_shard_num: s.storage.cache.vector_block_cache_shard_num,
        vector_meta_cache_capacity_mb: s.storage.cache.vector_meta_cache_capacity_mb,
        vector_meta_cache_shard_num: s.storage.cache.vector_meta_cache_shard_num,
        shared_buffer_capacity_mb,
        compactor_memory_limit_mb,
        prefetch_buffer_capacity_mb,
        block_cache_eviction_config,
        meta_cache_eviction_config,
        vector_block_cache_eviction_config,
        vector_meta_cache_eviction_config,
        block_file_cache_flush_buffer_threshold_mb,
        meta_file_cache_flush_buffer_threshold_mb,
    }
}

pub mod default {

    pub mod storage {
        pub fn share_buffers_sync_parallelism() -> u32 {
            1
        }

        pub fn share_buffer_compaction_worker_threads_number() -> u32 {
            4
        }

        pub fn shared_buffer_capacity_mb() -> usize {
            1024
        }

        pub fn shared_buffer_flush_ratio() -> f32 {
            0.8
        }

        pub fn shared_buffer_min_batch_flush_size_mb() -> usize {
            800
        }

        pub fn imm_merge_threshold() -> usize {
            0 // disable
        }

        pub fn write_conflict_detection_enabled() -> bool {
            cfg!(debug_assertions)
        }

        pub fn max_cached_recent_versions_number() -> usize {
            60
        }

        pub fn block_cache_capacity_mb() -> usize {
            512
        }

        pub fn high_priority_ratio_in_percent() -> usize {
            70
        }

        pub fn window_capacity_ratio_in_percent() -> usize {
            10
        }

        pub fn protected_capacity_ratio_in_percent() -> usize {
            80
        }

        pub fn cmsketch_eps() -> f64 {
            0.002
        }

        pub fn cmsketch_confidence() -> f64 {
            0.95
        }

        pub fn small_queue_capacity_ratio_in_percent() -> usize {
            10
        }

        pub fn ghost_queue_capacity_ratio_in_percent() -> usize {
            1000
        }

        pub fn small_to_main_freq_threshold() -> u8 {
            1
        }

        pub fn meta_cache_capacity_mb() -> usize {
            128
        }

        pub fn disable_remote_compactor() -> bool {
            false
        }

        pub fn share_buffer_upload_concurrency() -> usize {
            8
        }

        pub fn compactor_memory_limit_mb() -> usize {
            512
        }

        pub fn compactor_max_task_multiplier() -> f32 {
            match std::env::var("RW_COMPACTOR_MODE")
                .unwrap_or_default()
                .as_str()
            {
                mode if mode.contains("iceberg") => 12.0000,
                _ => 3.0000,
            }
        }

        pub fn compactor_memory_available_proportion() -> f64 {
            0.8
        }

        pub fn sstable_id_remote_fetch_number() -> u32 {
            10
        }

        pub fn min_sstable_size_mb() -> u32 {
            32
        }

        pub fn min_sst_size_for_streaming_upload() -> u64 {
            // 32MB
            32 * 1024 * 1024
        }

        pub fn max_concurrent_compaction_task_number() -> u64 {
            16
        }

        pub fn max_preload_wait_time_mill() -> u64 {
            0
        }

        pub fn max_version_pinning_duration_sec() -> u64 {
            3 * 3600
        }

        pub fn compactor_max_sst_key_count() -> u64 {
            2 * 1024 * 1024 // 200w
        }

        pub fn compact_iter_recreate_timeout_ms() -> u64 {
            10 * 60 * 1000
        }

        pub fn compactor_iter_max_io_retry_times() -> usize {
            8
        }

        pub fn shorten_block_meta_key_threshold() -> Option<usize> {
            None
        }

        pub fn compactor_max_sst_size() -> u64 {
            512 * 1024 * 1024 // 512m
        }

        pub fn enable_fast_compaction() -> bool {
            true
        }

        pub fn check_compaction_result() -> bool {
            false
        }

        pub fn max_preload_io_retry_times() -> usize {
            3
        }

        pub fn mem_table_spill_threshold() -> usize {
            4 << 20
        }

        pub fn compactor_fast_max_compact_delete_ratio() -> u32 {
            40
        }

        pub fn compactor_fast_max_compact_task_size() -> u64 {
            2 * 1024 * 1024 * 1024 // 2g
        }

        pub fn max_prefetch_block_number() -> usize {
            16
        }

        pub fn compactor_concurrent_uploading_sst_count() -> Option<usize> {
            None
        }

        pub fn compactor_max_overlap_sst_count() -> usize {
            64
        }

        pub fn compactor_max_preload_meta_file_count() -> usize {
            32
        }

        pub fn vector_file_block_size_kb() -> usize {
            1024
        }

        pub fn vector_block_cache_capacity_mb() -> usize {
            16
        }

        pub fn vector_block_cache_shard_num() -> usize {
            16
        }

        pub fn vector_meta_cache_capacity_mb() -> usize {
            16
        }

        pub fn vector_meta_cache_shard_num() -> usize {
            16
        }

        // deprecated
        pub fn table_info_statistic_history_times() -> usize {
            240
        }

        pub fn block_file_cache_flush_buffer_threshold_mb() -> usize {
            256
        }

        pub fn meta_file_cache_flush_buffer_threshold_mb() -> usize {
            64
        }

        pub fn time_travel_version_cache_capacity() -> u64 {
            10
        }

        pub fn table_change_log_cache_capacity() -> u64 {
            60
        }

        pub fn sst_skip_bloom_filter_in_serde() -> bool {
            false
        }

        pub fn iceberg_compaction_enable_validate() -> bool {
            false
        }

        pub fn iceberg_compaction_max_record_batch_rows() -> usize {
            1024
        }

        pub fn iceberg_compaction_write_parquet_max_row_group_rows() -> usize {
            1024 * 100 // 100k
        }

        pub fn iceberg_compaction_min_size_per_partition_mb() -> u32 {
            1024
        }

        pub fn iceberg_compaction_max_file_count_per_partition() -> u32 {
            32
        }

        pub fn iceberg_compaction_task_parallelism_ratio() -> f32 {
            4.0
        }

        pub fn iceberg_compaction_enable_heuristic_output_parallelism() -> bool {
            false
        }

        pub fn iceberg_compaction_max_concurrent_closes() -> usize {
            8
        }

        pub fn iceberg_compaction_enable_dynamic_size_estimation() -> bool {
            true
        }

        pub fn iceberg_compaction_size_estimation_smoothing_factor() -> f64 {
            0.3
        }

        pub fn iceberg_compaction_pending_parallelism_budget_multiplier() -> f32 {
            4.0
        }

        pub fn iceberg_compaction_pull_interval_ms() -> u64 {
            5000
        }

        pub fn iceberg_compaction_enable_prefetch() -> bool {
            false
        }

        pub fn iceberg_compaction_target_binpack_group_size_mb() -> Option<u64> {
            Some(100 * 1024) // 100GB
        }

        pub fn iceberg_compaction_min_group_size_mb() -> Option<u64> {
            None
        }

        pub fn iceberg_compaction_min_group_file_count() -> Option<usize> {
            None
        }
    }

    pub mod file_cache {
        use std::num::NonZeroUsize;

        use foyer::{Compression, RecoverMode, Throttle};

        use super::super::{FileCacheRuntimeConfig, FileCacheTokioRuntimeConfig};

        pub fn dir() -> String {
            "".to_owned()
        }

        pub fn capacity_mb() -> usize {
            1024
        }

        pub fn file_capacity_mb() -> usize {
            64
        }

        pub fn flushers() -> usize {
            4
        }

        pub fn reclaimers() -> usize {
            4
        }

        pub fn recover_concurrency() -> usize {
            8
        }

        pub fn insert_rate_limit_mb() -> usize {
            0
        }

        pub fn indexer_shards() -> usize {
            64
        }

        pub fn compression() -> Compression {
            Compression::None
        }

        pub fn flush_buffer_threshold_mb() -> Option<usize> {
            None
        }

        pub fn submit_queue_size_threshold_mb() -> usize {
            16
        }

        pub fn fifo_probation_ratio() -> f64 {
            0.1
        }

        pub fn blob_index_size_kb() -> usize {
            16
        }

        pub fn recover_mode() -> RecoverMode {
            RecoverMode::Quiet
        }

        pub fn runtime_config() -> FileCacheRuntimeConfig {
            FileCacheRuntimeConfig::Unified(FileCacheTokioRuntimeConfig::default())
        }

        pub fn throttle() -> Throttle {
            Throttle::new()
                .with_iops_counter(foyer::IopsCounter::PerIoSize(
                    NonZeroUsize::new(128 * 1024).unwrap(),
                ))
                .with_read_iops(100000)
                .with_write_iops(100000)
                .with_write_throughput(1024 * 1024 * 1024)
                .with_read_throughput(1024 * 1024 * 1024)
        }
    }

    pub mod cache_refill {
        pub fn meta_refill_concurrency() -> usize {
            0
        }

        pub fn data_refill_levels() -> Vec<u32> {
            vec![]
        }

        pub fn timeout_ms() -> u64 {
            6000
        }

        pub fn concurrency() -> usize {
            10
        }

        pub fn unit() -> usize {
            64
        }

        pub fn threshold() -> f64 {
            0.5
        }

        pub fn recent_filter_shards() -> usize {
            16
        }

        pub fn recent_filter_layers() -> usize {
            6
        }

        pub fn recent_filter_rotate_interval_ms() -> usize {
            10000
        }

        pub fn skip_recent_filter() -> bool {
            false
        }

        pub fn skip_inheritance_filter() -> bool {
            false
        }
    }

    pub mod object_store_config {
        const DEFAULT_REQ_BACKOFF_INTERVAL_MS: u64 = 1000; // 1s
        const DEFAULT_REQ_BACKOFF_MAX_DELAY_MS: u64 = 10 * 1000; // 10s
        const DEFAULT_REQ_MAX_RETRY_ATTEMPTS: usize = 3;

        pub fn set_atomic_write_dir() -> bool {
            false
        }

        pub fn object_store_req_concurrency_limit() -> usize {
            0
        }

        pub fn http_concurrent_limit() -> usize {
            0
        }

        pub fn object_store_req_backoff_interval_ms() -> u64 {
            DEFAULT_REQ_BACKOFF_INTERVAL_MS
        }

        pub fn object_store_req_backoff_max_delay_ms() -> u64 {
            DEFAULT_REQ_BACKOFF_MAX_DELAY_MS // 10s
        }

        pub fn object_store_req_backoff_factor() -> u64 {
            2
        }

        pub fn object_store_upload_attempt_timeout_ms() -> u64 {
            8 * 1000 // 8s
        }

        pub fn object_store_upload_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        // init + upload_part + finish
        pub fn object_store_streaming_upload_attempt_timeout_ms() -> u64 {
            5 * 1000 // 5s
        }

        pub fn object_store_streaming_upload_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        // tips: depend on block_size
        pub fn object_store_read_attempt_timeout_ms() -> u64 {
            8 * 1000 // 8s
        }

        pub fn object_store_read_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        pub fn object_store_streaming_read_attempt_timeout_ms() -> u64 {
            3 * 1000 // 3s
        }

        pub fn object_store_streaming_read_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        pub fn object_store_metadata_attempt_timeout_ms() -> u64 {
            60 * 1000 // 1min
        }

        pub fn object_store_metadata_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        pub fn object_store_delete_attempt_timeout_ms() -> u64 {
            5 * 1000
        }

        pub fn object_store_delete_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        // tips: depend on batch size
        pub fn object_store_delete_objects_attempt_timeout_ms() -> u64 {
            5 * 1000
        }

        pub fn object_store_delete_objects_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        pub fn object_store_list_attempt_timeout_ms() -> u64 {
            10 * 60 * 1000
        }

        pub fn object_store_list_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        pub fn opendal_upload_concurrency() -> usize {
            256
        }

        pub fn upload_part_size() -> usize {
            // 16m
            16 * 1024 * 1024
        }

        pub mod s3 {
            const DEFAULT_IDENTITY_RESOLUTION_TIMEOUT_S: u64 = 5;

            const DEFAULT_KEEPALIVE_MS: u64 = 600 * 1000; // 10min

            pub fn keepalive_ms() -> Option<u64> {
                Some(DEFAULT_KEEPALIVE_MS) // 10min
            }

            pub fn recv_buffer_size() -> Option<usize> {
                Some(1 << 21) // 2m
            }

            pub fn send_buffer_size() -> Option<usize> {
                None
            }

            pub fn nodelay() -> Option<bool> {
                Some(true)
            }

            pub fn identity_resolution_timeout_s() -> u64 {
                DEFAULT_IDENTITY_RESOLUTION_TIMEOUT_S
            }

            pub mod developer {
                pub fn retry_unknown_service_error() -> bool {
                    false
                }

                pub fn retryable_service_error_codes() -> Vec<String> {
                    vec!["SlowDown".into(), "TooManyRequests".into()]
                }

                pub fn use_opendal() -> bool {
                    true
                }
            }
        }
    }
}
