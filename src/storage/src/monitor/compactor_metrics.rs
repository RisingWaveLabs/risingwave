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

use std::sync::LazyLock;

use prometheus::core::{AtomicU64, GenericCounter, GenericCounterVec};
use prometheus::{
    Histogram, HistogramVec, IntGauge, Registry, exponential_buckets, histogram_opts,
    register_histogram_vec_with_registry, register_histogram_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_with_registry,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;

#[derive(Debug, Clone)]
pub struct CompactorMetrics {
    pub compaction_upload_sst_counts: GenericCounter<AtomicU64>,
    pub compact_fast_runner_bytes: GenericCounter<AtomicU64>,
    pub compact_write_bytes: GenericCounterVec<AtomicU64>,
    pub compact_read_current_level: GenericCounterVec<AtomicU64>,
    pub compact_read_next_level: GenericCounterVec<AtomicU64>,
    pub compact_read_sstn_current_level: GenericCounterVec<AtomicU64>,
    pub compact_read_sstn_next_level: GenericCounterVec<AtomicU64>,
    pub compact_write_sstn: GenericCounterVec<AtomicU64>,
    pub compact_sst_duration: Histogram,
    pub compact_task_duration: HistogramVec,
    pub compact_task_pending_num: IntGauge,
    pub compact_task_pending_parallelism: IntGauge,
    pub write_build_l0_sst_duration: Histogram,
    pub shared_buffer_to_sstable_size: Histogram,
    pub get_table_id_total_time_duration: Histogram,
    pub remote_read_time: Histogram,
    pub sstable_bloom_filter_size: Histogram,
    pub sstable_file_size: Histogram,
    pub sstable_avg_key_size: Histogram,
    pub sstable_avg_value_size: Histogram,
    pub iter_scan_key_counts: GenericCounterVec<AtomicU64>,
    pub write_build_l0_bytes: GenericCounter<AtomicU64>,
    pub sstable_distinct_epoch_count: Histogram,
    pub compaction_event_consumed_latency: Histogram,
    pub compaction_event_loop_iteration_latency: Histogram,
    pub sstable_block_size: Histogram,
    pub iceberg_compaction_memory_budget_bytes: IntGauge,
    pub iceberg_compaction_running_memory_reservation_bytes: IntGauge,
}

pub static GLOBAL_COMPACTOR_METRICS: LazyLock<CompactorMetrics> =
    LazyLock::new(|| CompactorMetrics::new(&GLOBAL_METRICS_REGISTRY));

impl CompactorMetrics {
    fn new(registry: &Registry) -> Self {
        // 256B - 4GB, finer resolution near 256MB (adds two extra buckets)
        let size_buckets = exponential_buckets(256.0, 8.0, 9).unwrap();
        // 10ms - 2.7h
        let time_buckets = exponential_buckets(0.01, 10.0, 7).unwrap();
        let opts = histogram_opts!(
            "compactor_shared_buffer_to_sstable_size",
            "Histogram of batch size compacted from shared buffer to remote storage",
            size_buckets.clone()
        );
        let shared_buffer_to_sstable_size =
            register_histogram_with_registry!(opts, registry).unwrap();

        let compaction_upload_sst_counts = register_int_counter_with_registry!(
            "compactor_compaction_upload_sst_counts",
            "Total number of sst uploads during compaction",
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "compactor_compact_sst_duration",
            "Total time of compact_key_range that have been issued to state store",
            time_buckets.clone()
        );
        let compact_sst_duration = register_histogram_with_registry!(opts, registry).unwrap();
        let opts = histogram_opts!(
            "compactor_compact_task_duration",
            "Total time of compact that have been issued to state store",
            time_buckets.clone()
        );
        let compact_task_duration =
            register_histogram_vec_with_registry!(opts, &["group", "level"], registry).unwrap();

        let opts = histogram_opts!(
            "compactor_get_table_id_total_time_duration",
            "Total time of compact that have been issued to state store",
            time_buckets.clone()
        );
        let get_table_id_total_time_duration =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "compactor_remote_read_time",
            "Total time of operations which read from remote storage when enable prefetch",
            time_buckets.clone()
        );
        let remote_read_time = register_histogram_with_registry!(opts, registry).unwrap();

        let compact_read_current_level = register_int_counter_vec_with_registry!(
            "storage_level_compact_read_curr",
            "KBs read from current level during history compactions to next level",
            &["group", "level_index"],
            registry
        )
        .unwrap();

        let compact_read_next_level = register_int_counter_vec_with_registry!(
            "storage_level_compact_read_next",
            "KBs read from next level during history compactions to next level",
            &["group", "level_index"],
            registry
        )
        .unwrap();

        let compact_write_bytes = register_int_counter_vec_with_registry!(
            "storage_level_compact_write",
            "KBs written into next level during history compactions to next level",
            &["group", "level_index"],
            registry
        )
        .unwrap();

        let compact_read_sstn_current_level = register_int_counter_vec_with_registry!(
            "storage_level_compact_read_sstn_curr",
            "num of SSTs read from current level during history compactions to next level",
            &["group", "level_index"],
            registry
        )
        .unwrap();

        let compact_read_sstn_next_level = register_int_counter_vec_with_registry!(
            "storage_level_compact_read_sstn_next",
            "num of SSTs read from next level during history compactions to next level",
            &["group", "level_index"],
            registry
        )
        .unwrap();

        let compact_write_sstn = register_int_counter_vec_with_registry!(
            "storage_level_compact_write_sstn",
            "num of SSTs written into next level during history compactions to next level",
            &["group", "level_index"],
            registry
        )
        .unwrap();

        let compact_task_pending_num = register_int_gauge_with_registry!(
            "storage_compact_task_pending_num",
            "the num of storage compact task",
            registry
        )
        .unwrap();

        let compact_task_pending_parallelism = register_int_gauge_with_registry!(
            "storage_compact_task_pending_parallelism",
            "the num of storage compact parallelism",
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "compactor_sstable_bloom_filter_size",
            "Total bytes gotten from the SST filter payload, for observing filter size",
            exponential_buckets(16.0, 16.0, 7).unwrap() // max 256MB
        );

        let sstable_bloom_filter_size = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "compactor_sstable_file_size",
            "Total bytes gotten from sstable_file_size, for observing sstable_file_size",
            size_buckets.clone()
        );

        let sstable_file_size = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "compactor_sstable_avg_key_size",
            "Total bytes gotten from sstable_avg_key_size, for observing sstable_avg_key_size",
            size_buckets.clone()
        );

        let sstable_avg_key_size = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "compactor_sstable_avg_value_size",
            "Total bytes gotten from sstable_avg_value_size, for observing sstable_avg_value_size",
            size_buckets.clone()
        );

        let sstable_avg_value_size = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_write_build_l0_sst_duration",
            "Total time of batch_write_build_table that have been issued to state store",
            time_buckets.clone()
        );
        let write_build_l0_sst_duration =
            register_histogram_with_registry!(opts, registry).unwrap();

        let iter_scan_key_counts = register_int_counter_vec_with_registry!(
            "compactor_iter_scan_key_counts",
            "Total number of keys read by iterator",
            &["type"],
            registry
        )
        .unwrap();

        let write_build_l0_bytes = register_int_counter_with_registry!(
            "compactor_write_build_l0_bytes",
            "Total size of compaction files size that have been written to object store from shared buffer",
            registry
        ).unwrap();
        let compact_fast_runner_bytes = register_int_counter_with_registry!(
            "compactor_fast_compact_bytes",
            "Total size of compaction files size of fast compactor runner",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "compactor_sstable_distinct_epoch_count",
            "Total number gotten from sstable_distinct_epoch_count, for observing sstable_distinct_epoch_count",
            exponential_buckets(1.0, 10.0, 6).unwrap()
        );

        let sstable_distinct_epoch_count =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "compactor_compaction_event_consumed_latency",
            "The latency of each event being consumed",
            time_buckets.clone()
        );
        let compaction_event_consumed_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "compactor_compaction_event_loop_iteration_latency",
            "The latency of each iteration of the compaction event loop",
            time_buckets
        );
        let compaction_event_loop_iteration_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "compactor_sstable_block_size",
            "Total bytes gotten from sstable_block_size, for observing sstable_block_size",
            size_buckets,
        );

        let sstable_block_size = register_histogram_with_registry!(opts, registry).unwrap();

        let iceberg_compaction_memory_budget_bytes = register_int_gauge_with_registry!(
            "storage_iceberg_compaction_memory_budget_bytes",
            "Heap admission budget for running Iceberg compaction plans on this compactor node",
            registry
        )
        .unwrap();
        let iceberg_compaction_running_memory_reservation_bytes =
            register_int_gauge_with_registry!(
                "storage_iceberg_compaction_running_memory_reservation_bytes",
                "Estimated heap peak reservations of currently running Iceberg compaction plans",
                registry
            )
            .unwrap();
        Self {
            compaction_upload_sst_counts,
            compact_fast_runner_bytes,
            compact_write_bytes,
            compact_read_current_level,
            compact_read_next_level,
            compact_read_sstn_current_level,
            compact_read_sstn_next_level,
            compact_write_sstn,
            compact_sst_duration,
            compact_task_duration,
            compact_task_pending_num,
            compact_task_pending_parallelism,
            write_build_l0_sst_duration,
            shared_buffer_to_sstable_size,
            get_table_id_total_time_duration,
            remote_read_time,
            sstable_bloom_filter_size,
            sstable_file_size,
            sstable_avg_key_size,
            sstable_avg_value_size,
            iter_scan_key_counts,
            write_build_l0_bytes,
            sstable_distinct_epoch_count,
            compaction_event_consumed_latency,
            compaction_event_loop_iteration_latency,
            sstable_block_size,
            iceberg_compaction_memory_budget_bytes,
            iceberg_compaction_running_memory_reservation_bytes,
        }
    }

    /// Creates a new `HummockStateStoreMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        GLOBAL_COMPACTOR_METRICS.clone()
    }
}
