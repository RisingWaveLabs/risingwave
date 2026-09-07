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

use risingwave_common::config::storage::FileCacheRuntimeConfig;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_common::config::{
    EvictionConfig, ObjectStoreConfig, RwConfig, StorageMemoryConfig, extract_storage_memory_config,
};
use risingwave_common::system_param::reader::{SystemParamsRead, SystemParamsReader};
use risingwave_common::system_param::system_params_for_test;

#[derive(Clone, Debug)]
pub struct StorageOpts {
    /// The size of parallel task for one compact/flush job.
    pub parallel_compact_size_mb: u32,
    /// Target size of the Sstable.
    pub sstable_size_mb: u32,
    /// Minimal target size of the Sstable to store data of different state-table in independent files as soon as possible.
    pub min_sstable_size_mb: u32,
    /// Size of each block in bytes in SST.
    pub block_size_kb: u32,
    /// Deprecated and ignored by SST filter builders; kept for backward compatibility.
    pub bloom_false_positive: f64,
    /// parallelism while syncing share buffers into L0 SST. Should NOT be 0.
    pub share_buffers_sync_parallelism: u32,
    /// Worker threads number of dedicated tokio runtime for share buffer compaction. 0 means use
    /// tokio's default value (number of CPU core).
    pub share_buffer_compaction_worker_threads_number: u32,
    /// Maximum shared buffer size, writes attempting to exceed the capacity will stall until there
    /// is enough space.
    pub shared_buffer_capacity_mb: usize,
    /// The shared buffer will start flushing data to object when the ratio of memory usage to the
    /// shared buffer capacity exceed such ratio.
    pub shared_buffer_flush_ratio: f32,
    /// The minimum total flush size of shared buffer spill. When a shared buffer spill is trigger,
    /// the total flush size across multiple epochs should be at least higher than this size.
    pub shared_buffer_min_batch_flush_size_mb: usize,
    /// Remote directory for storing data and metadata objects.
    pub data_directory: String,
    /// Whether to enable write conflict detection
    pub write_conflict_detection_enabled: bool,
    /// Capacity of sstable block cache.
    pub block_cache_capacity_mb: usize,
    /// the number of block-cache shard. Less shard means that more concurrent-conflict.
    pub block_cache_shard_num: usize,
    /// Eviction config for block cache.
    pub block_cache_eviction_config: EvictionConfig,
    /// Capacity of sstable meta cache.
    pub meta_cache_capacity_mb: usize,
    /// the number of meta-cache shard. Less shard means that more concurrent-conflict.
    pub meta_cache_shard_num: usize,
    /// Eviction config for meta cache.
    pub meta_cache_eviction_config: EvictionConfig,
    /// max memory usage for large query.
    pub prefetch_buffer_capacity_mb: usize,

    pub max_cached_recent_versions_number: usize,

    pub max_prefetch_block_number: usize,

    pub disable_remote_compactor: bool,
    /// Number of tasks shared buffer can upload in parallel.
    pub share_buffer_upload_concurrency: usize,
    /// Capacity of sstable meta cache.
    pub compactor_memory_limit_mb: usize,
    /// compactor streaming iterator recreate timeout.
    /// deprecated
    pub compact_iter_recreate_timeout_ms: u64,
    /// Number of SST ids fetched from meta per RPC
    pub sstable_id_remote_fetch_number: u32,
    /// Whether to enable streaming upload for sstable.
    pub min_sst_size_for_streaming_upload: u64,
    pub max_concurrent_compaction_task_number: u64,
    pub max_version_pinning_duration_sec: u64,
    pub compactor_iter_max_io_retry_times: usize,

    /// If set, block metadata keys will be shortened when their length exceeds this threshold.
    pub shorten_block_meta_key_threshold: Option<usize>,

    pub data_file_cache_dir: String,
    pub data_file_cache_capacity_mb: usize,
    pub data_file_cache_file_capacity_mb: usize,
    pub data_file_cache_flushers: usize,
    pub data_file_cache_reclaimers: usize,
    pub data_file_cache_recover_mode: foyer::RecoverMode,
    pub data_file_cache_recover_concurrency: usize,
    pub data_file_cache_indexer_shards: usize,
    pub data_file_cache_compression: foyer::Compression,
    pub data_file_cache_flush_buffer_threshold_mb: usize,
    pub data_file_cache_submit_queue_size_threshold_mb: usize,
    pub data_file_cache_fifo_probation_ratio: f64,
    pub data_file_cache_blob_index_size_kb: usize,
    pub data_file_cache_runtime_config: FileCacheRuntimeConfig,
    pub data_file_cache_throttle: foyer::Throttle,

    pub cache_refill_data_refill_levels: Vec<u32>,
    pub cache_refill_timeout_ms: u64,
    pub cache_refill_meta_refill_concurrency: usize,
    pub cache_refill_concurrency: usize,
    pub cache_refill_recent_filter_shards: usize,
    pub cache_refill_recent_filter_layers: usize,
    pub cache_refill_recent_filter_rotate_interval_ms: usize,
    pub cache_refill_unit: usize,
    pub cache_refill_threshold: f64,
    pub cache_refill_skip_recent_filter: bool,
    pub cache_refill_skip_inheritance_filter: bool,
    pub cache_refill_table_cache_refill_default_policy: CacheRefillPolicy,

    pub meta_file_cache_dir: String,
    pub meta_file_cache_capacity_mb: usize,
    pub meta_file_cache_file_capacity_mb: usize,
    pub meta_file_cache_flushers: usize,
    pub meta_file_cache_reclaimers: usize,
    pub meta_file_cache_recover_mode: foyer::RecoverMode,
    pub meta_file_cache_recover_concurrency: usize,
    pub meta_file_cache_indexer_shards: usize,
    pub meta_file_cache_compression: foyer::Compression,
    pub meta_file_cache_flush_buffer_threshold_mb: usize,
    pub meta_file_cache_submit_queue_size_threshold_mb: usize,
    pub meta_file_cache_fifo_probation_ratio: f64,
    pub meta_file_cache_blob_index_size_kb: usize,
    pub meta_file_cache_runtime_config: FileCacheRuntimeConfig,
    pub meta_file_cache_throttle: foyer::Throttle,
    pub sst_skip_bloom_filter_in_serde: bool,

    pub vector_file_block_size_kb: usize,
    pub vector_block_cache_capacity_mb: usize,
    pub vector_block_cache_shard_num: usize,
    pub vector_block_cache_eviction_config: EvictionConfig,
    pub vector_meta_cache_capacity_mb: usize,
    pub vector_meta_cache_shard_num: usize,
    pub vector_meta_cache_eviction_config: EvictionConfig,

    /// The storage url for storing backups.
    pub backup_storage_url: String,
    /// The storage directory for storing backups.
    pub backup_storage_directory: String,
    /// max time which wait for preload. 0 represent do not do any preload.
    pub max_preload_wait_time_mill: u64,

    pub compactor_max_sst_key_count: u64,
    pub compactor_max_task_multiplier: f32,
    pub compactor_max_sst_size: u64,
    /// enable `FastCompactorRunner`.
    pub enable_fast_compaction: bool,
    pub check_compaction_result: bool,
    pub max_preload_io_retry_times: usize,
    pub compactor_fast_max_compact_delete_ratio: u32,
    pub compactor_fast_max_compact_task_size: u64,

    pub mem_table_spill_threshold: usize,

    pub compactor_concurrent_uploading_sst_count: Option<usize>,

    pub compactor_max_overlap_sst_count: usize,

    /// The maximum number of meta files that can be preloaded.
    pub compactor_max_preload_meta_file_count: usize,

    pub object_store_config: ObjectStoreConfig,
    pub time_travel_version_cache_capacity: u64,
    pub table_change_log_cache_capacity: u64,

    pub iceberg_compaction_enable_validate: bool,
    pub iceberg_compaction_max_record_batch_rows: usize,
    pub iceberg_compaction_write_parquet_max_row_group_rows: usize,
    pub iceberg_compaction_min_size_per_partition_mb: u32,
    pub iceberg_compaction_max_file_count_per_partition: u32,
    pub iceberg_compaction_target_binpack_group_size_mb: Option<u64>,
    pub iceberg_compaction_min_group_size_mb: Option<u64>,
    pub iceberg_compaction_min_group_file_count: Option<usize>,

    /// The ratio of iceberg compaction max parallelism to the number of CPU cores
    pub iceberg_compaction_task_parallelism_ratio: f32,
    /// Whether to enable heuristic output parallelism in iceberg compaction.
    pub iceberg_compaction_enable_heuristic_output_parallelism: bool,
    /// Maximum number of concurrent file close operations
    pub iceberg_compaction_max_concurrent_closes: usize,
    /// Whether to enable dynamic size estimation for iceberg compaction.
    pub iceberg_compaction_enable_dynamic_size_estimation: bool,
    /// The smoothing factor for size estimation in iceberg compaction.(default: 0.3)
    pub iceberg_compaction_size_estimation_smoothing_factor: f64,
    /// Multiplier for pending waiting parallelism budget for iceberg compaction task queue.
    pub iceberg_compaction_pending_parallelism_budget_multiplier: f32,
    /// Pull interval for iceberg compaction task requests in milliseconds.
    pub iceberg_compaction_pull_interval_ms: u64,
    /// Whether to enable prefetch for iceberg compaction.
    pub iceberg_compaction_enable_prefetch: bool,
}

impl Default for StorageOpts {
    fn default() -> Self {
        let c = RwConfig::default();
        let p = system_params_for_test();
        let s = extract_storage_memory_config(&c);
        Self::from((&c, &p.into(), &s))
    }
}

impl From<(&RwConfig, &SystemParamsReader, &StorageMemoryConfig)> for StorageOpts {
    fn from((c, p, s): (&RwConfig, &SystemParamsReader, &StorageMemoryConfig)) -> Self {
        let mut data_file_cache_throttle = c.storage.data_file_cache.throttle.clone();
        if data_file_cache_throttle.write_throughput.is_none() {
            data_file_cache_throttle = data_file_cache_throttle.with_write_throughput(
                c.storage.data_file_cache.insert_rate_limit_mb * 1024 * 1024,
            );
        }
        let mut meta_file_cache_throttle = c.storage.meta_file_cache.throttle.clone();
        if meta_file_cache_throttle.write_throughput.is_none() {
            meta_file_cache_throttle = meta_file_cache_throttle.with_write_throughput(
                c.storage.meta_file_cache.insert_rate_limit_mb * 1024 * 1024,
            );
        }

        Self {
            parallel_compact_size_mb: p.parallel_compact_size_mb(),
            sstable_size_mb: p.sstable_size_mb(),
            min_sstable_size_mb: c.storage.min_sstable_size_mb,
            block_size_kb: p.block_size_kb(),
            bloom_false_positive: p.bloom_false_positive(),
            share_buffers_sync_parallelism: c.storage.share_buffers_sync_parallelism,
            share_buffer_compaction_worker_threads_number: c
                .storage
                .share_buffer_compaction_worker_threads_number,
            shared_buffer_capacity_mb: s.shared_buffer_capacity_mb,
            shared_buffer_flush_ratio: c.storage.shared_buffer_flush_ratio,
            shared_buffer_min_batch_flush_size_mb: c.storage.shared_buffer_min_batch_flush_size_mb,
            data_directory: p.data_directory().to_owned(),
            write_conflict_detection_enabled: c.storage.write_conflict_detection_enabled,
            block_cache_capacity_mb: s.block_cache_capacity_mb,
            block_cache_shard_num: s.block_cache_shard_num,
            block_cache_eviction_config: s.block_cache_eviction_config.clone(),
            meta_cache_capacity_mb: s.meta_cache_capacity_mb,
            meta_cache_shard_num: s.meta_cache_shard_num,
            meta_cache_eviction_config: s.meta_cache_eviction_config.clone(),
            prefetch_buffer_capacity_mb: s.prefetch_buffer_capacity_mb,
            max_cached_recent_versions_number: c.storage.max_cached_recent_versions_number,
            max_prefetch_block_number: c.storage.max_prefetch_block_number,
            disable_remote_compactor: c.storage.disable_remote_compactor,
            share_buffer_upload_concurrency: c.storage.share_buffer_upload_concurrency,
            compactor_memory_limit_mb: s.compactor_memory_limit_mb,
            sstable_id_remote_fetch_number: c.storage.sstable_id_remote_fetch_number,
            min_sst_size_for_streaming_upload: c.storage.min_sst_size_for_streaming_upload,
            max_concurrent_compaction_task_number: c.storage.max_concurrent_compaction_task_number,
            max_version_pinning_duration_sec: c.storage.max_version_pinning_duration_sec,
            data_file_cache_dir: c.storage.data_file_cache.dir.clone(),
            data_file_cache_capacity_mb: c.storage.data_file_cache.capacity_mb,
            data_file_cache_file_capacity_mb: c.storage.data_file_cache.file_capacity_mb,
            data_file_cache_flushers: c.storage.data_file_cache.flushers,
            data_file_cache_reclaimers: c.storage.data_file_cache.reclaimers,
            data_file_cache_recover_mode: c.storage.data_file_cache.recover_mode,
            data_file_cache_recover_concurrency: c.storage.data_file_cache.recover_concurrency,
            data_file_cache_indexer_shards: c.storage.data_file_cache.indexer_shards,
            data_file_cache_compression: c.storage.data_file_cache.compression,
            data_file_cache_flush_buffer_threshold_mb: s.block_file_cache_flush_buffer_threshold_mb,
            data_file_cache_submit_queue_size_threshold_mb: c
                .storage
                .data_file_cache
                .submit_queue_size_threshold_mb,
            data_file_cache_fifo_probation_ratio: c.storage.data_file_cache.fifo_probation_ratio,
            data_file_cache_blob_index_size_kb: c.storage.data_file_cache.blob_index_size_kb,
            data_file_cache_runtime_config: c.storage.data_file_cache.runtime_config.clone(),
            data_file_cache_throttle,
            meta_file_cache_dir: c.storage.meta_file_cache.dir.clone(),
            meta_file_cache_capacity_mb: c.storage.meta_file_cache.capacity_mb,
            meta_file_cache_file_capacity_mb: c.storage.meta_file_cache.file_capacity_mb,
            meta_file_cache_flushers: c.storage.meta_file_cache.flushers,
            meta_file_cache_reclaimers: c.storage.meta_file_cache.reclaimers,
            meta_file_cache_recover_mode: c.storage.meta_file_cache.recover_mode,
            meta_file_cache_recover_concurrency: c.storage.meta_file_cache.recover_concurrency,
            meta_file_cache_indexer_shards: c.storage.meta_file_cache.indexer_shards,
            meta_file_cache_compression: c.storage.meta_file_cache.compression,
            meta_file_cache_flush_buffer_threshold_mb: s.meta_file_cache_flush_buffer_threshold_mb,
            meta_file_cache_submit_queue_size_threshold_mb: c
                .storage
                .meta_file_cache
                .submit_queue_size_threshold_mb,
            meta_file_cache_fifo_probation_ratio: c.storage.meta_file_cache.fifo_probation_ratio,
            meta_file_cache_blob_index_size_kb: c.storage.meta_file_cache.blob_index_size_kb,
            meta_file_cache_runtime_config: c.storage.meta_file_cache.runtime_config.clone(),
            meta_file_cache_throttle,
            sst_skip_bloom_filter_in_serde: c.storage.sst_skip_bloom_filter_in_serde,
            cache_refill_data_refill_levels: c.storage.cache_refill.data_refill_levels.clone(),
            cache_refill_timeout_ms: c.storage.cache_refill.timeout_ms,
            cache_refill_meta_refill_concurrency: c.storage.cache_refill.meta_refill_concurrency,
            cache_refill_concurrency: c.storage.cache_refill.concurrency,
            cache_refill_recent_filter_shards: c.storage.cache_refill.recent_filter_shards,
            cache_refill_recent_filter_layers: c.storage.cache_refill.recent_filter_layers,
            cache_refill_recent_filter_rotate_interval_ms: c
                .storage
                .cache_refill
                .recent_filter_rotate_interval_ms,
            cache_refill_unit: c.storage.cache_refill.unit,
            cache_refill_threshold: c.storage.cache_refill.threshold,
            cache_refill_skip_recent_filter: c.storage.cache_refill.skip_recent_filter,
            cache_refill_skip_inheritance_filter: c.storage.cache_refill.skip_inheritance_filter,
            cache_refill_table_cache_refill_default_policy: c
                .streaming
                .developer
                .cache_refill_policy,
            max_preload_wait_time_mill: c.storage.max_preload_wait_time_mill,
            compact_iter_recreate_timeout_ms: c.storage.compact_iter_recreate_timeout_ms,

            max_preload_io_retry_times: c.storage.max_preload_io_retry_times,
            backup_storage_url: p.backup_storage_url().to_owned(),
            backup_storage_directory: p.backup_storage_directory().to_owned(),
            compactor_max_sst_key_count: c.storage.compactor_max_sst_key_count,
            compactor_max_task_multiplier: c.storage.compactor_max_task_multiplier,
            compactor_max_sst_size: c.storage.compactor_max_sst_size,
            enable_fast_compaction: c.storage.enable_fast_compaction,
            check_compaction_result: c.storage.check_compaction_result,
            mem_table_spill_threshold: c.storage.mem_table_spill_threshold,
            object_store_config: c.storage.object_store.clone(),
            compactor_fast_max_compact_delete_ratio: c
                .storage
                .compactor_fast_max_compact_delete_ratio,
            compactor_fast_max_compact_task_size: c.storage.compactor_fast_max_compact_task_size,
            compactor_iter_max_io_retry_times: c.storage.compactor_iter_max_io_retry_times,
            shorten_block_meta_key_threshold: c.storage.shorten_block_meta_key_threshold,
            compactor_concurrent_uploading_sst_count: c
                .storage
                .compactor_concurrent_uploading_sst_count,
            time_travel_version_cache_capacity: c.storage.time_travel_version_cache_capacity,
            table_change_log_cache_capacity: c.storage.table_change_log_cache_capacity,
            compactor_max_overlap_sst_count: c.storage.compactor_max_overlap_sst_count,
            compactor_max_preload_meta_file_count: c.storage.compactor_max_preload_meta_file_count,

            iceberg_compaction_enable_validate: c.storage.iceberg_compaction_enable_validate,
            iceberg_compaction_max_record_batch_rows: c
                .storage
                .iceberg_compaction_max_record_batch_rows,
            #[expect(deprecated)]
            iceberg_compaction_write_parquet_max_row_group_rows: c
                .storage
                .iceberg_compaction_write_parquet_max_row_group_rows,
            iceberg_compaction_min_size_per_partition_mb: c
                .storage
                .iceberg_compaction_min_size_per_partition_mb,
            iceberg_compaction_max_file_count_per_partition: c
                .storage
                .iceberg_compaction_max_file_count_per_partition,
            iceberg_compaction_task_parallelism_ratio: c
                .storage
                .iceberg_compaction_task_parallelism_ratio,
            iceberg_compaction_enable_heuristic_output_parallelism: c
                .storage
                .iceberg_compaction_enable_heuristic_output_parallelism,
            iceberg_compaction_max_concurrent_closes: c
                .storage
                .iceberg_compaction_max_concurrent_closes,
            iceberg_compaction_enable_dynamic_size_estimation: c
                .storage
                .iceberg_compaction_enable_dynamic_size_estimation,
            iceberg_compaction_size_estimation_smoothing_factor: c
                .storage
                .iceberg_compaction_size_estimation_smoothing_factor,
            iceberg_compaction_pending_parallelism_budget_multiplier: c
                .storage
                .iceberg_compaction_pending_parallelism_budget_multiplier,
            iceberg_compaction_pull_interval_ms: c.storage.iceberg_compaction_pull_interval_ms,
            iceberg_compaction_enable_prefetch: c.storage.iceberg_compaction_enable_prefetch,
            iceberg_compaction_target_binpack_group_size_mb: c
                .storage
                .iceberg_compaction_target_binpack_group_size_mb,
            iceberg_compaction_min_group_size_mb: c.storage.iceberg_compaction_min_group_size_mb,
            iceberg_compaction_min_group_file_count: c
                .storage
                .iceberg_compaction_min_group_file_count,
            vector_file_block_size_kb: c.storage.vector_file_block_size_kb,
            vector_block_cache_capacity_mb: s.vector_block_cache_capacity_mb,
            vector_block_cache_shard_num: s.vector_block_cache_shard_num,
            vector_block_cache_eviction_config: s.vector_block_cache_eviction_config.clone(),
            vector_meta_cache_capacity_mb: s.vector_meta_cache_capacity_mb,
            vector_meta_cache_shard_num: s.vector_meta_cache_shard_num,
            vector_meta_cache_eviction_config: s.vector_meta_cache_eviction_config.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_cache_submit_queue_size_threshold() {
        let mut config = RwConfig::default();
        config
            .storage
            .data_file_cache
            .submit_queue_size_threshold_mb = 256;
        config
            .storage
            .meta_file_cache
            .submit_queue_size_threshold_mb = 32;
        let system_params = system_params_for_test();
        let storage_memory_config = extract_storage_memory_config(&config);

        let opts = StorageOpts::from((&config, &system_params.into(), &storage_memory_config));

        assert_eq!(opts.data_file_cache_submit_queue_size_threshold_mb, 256);
        assert_eq!(opts.meta_file_cache_submit_queue_size_threshold_mb, 32);
    }
}
