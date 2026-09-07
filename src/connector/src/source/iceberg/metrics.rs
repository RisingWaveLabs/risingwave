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

use std::sync::{Arc, LazyLock};

use prometheus::{Registry, exponential_buckets, histogram_opts};
use risingwave_common::metrics::{
    LabelGuardedHistogram, LabelGuardedHistogramVec, LabelGuardedIntCounter,
    LabelGuardedIntCounterVec, LabelGuardedIntGaugeVec,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::{
    register_guarded_histogram_vec_with_registry, register_guarded_int_counter_vec_with_registry,
    register_guarded_int_gauge_vec_with_registry,
};

#[derive(Clone)]
pub struct IcebergScanMetrics {
    // -- Existing --
    pub iceberg_read_bytes: LabelGuardedIntCounterVec,

    // -- Snapshot & Discovery (List Executor) --
    /// Time difference (seconds) between the latest available snapshot and
    /// the last ingested snapshot.
    pub iceberg_source_snapshot_lag_seconds: LabelGuardedIntGaugeVec,

    /// Total number of snapshots discovered via incremental scan.
    pub iceberg_source_snapshots_discovered_total: LabelGuardedIntCounterVec,

    /// Time spent planning files from a snapshot (metadata operation).
    pub iceberg_source_list_duration_seconds: LabelGuardedHistogramVec,

    /// Files discovered per scan, labeled by `file_type` (data, `eq_delete`, `pos_delete`).
    pub iceberg_source_files_discovered_total: LabelGuardedIntCounterVec,

    // -- Data Reading (used in scan_task_to_chunk_with_deletes, labeled by table_name) --
    /// Per-file read duration.
    pub iceberg_source_file_read_duration_seconds: LabelGuardedHistogramVec,

    /// Total rows read from Iceberg source.
    pub iceberg_source_rows_read_total: LabelGuardedIntCounterVec,

    /// Total files read from Iceberg source, labeled by `file_type`.
    pub iceberg_source_files_read_total: LabelGuardedIntCounterVec,

    // -- Delete Handling --
    /// Rows removed by delete processing, labeled by `delete_type`.
    pub iceberg_source_delete_rows_applied_total: LabelGuardedIntCounterVec,

    /// Histogram of delete files attached per data file scan task.
    pub iceberg_source_delete_files_per_data_file: LabelGuardedHistogramVec,

    // -- Operational Health --
    /// Number of files currently being fetched by the active reader.
    pub iceberg_source_inflight_file_count: LabelGuardedIntGaugeVec,

    /// Categorized scan error counter.
    pub iceberg_source_scan_errors_total: LabelGuardedIntCounterVec,
}

#[derive(Clone)]
pub struct IcebergFileScanMetrics {
    read_bytes: LabelGuardedIntCounter,
    file_read_duration_seconds: LabelGuardedHistogram,
    rows_read_total: LabelGuardedIntCounter,
    files_read_total: LabelGuardedIntCounter,
    delete_rows_applied_total: LabelGuardedIntCounter,
}

impl IcebergFileScanMetrics {
    pub fn new(metrics: &IcebergScanMetrics, table_name: &str) -> Self {
        Self {
            read_bytes: metrics
                .iceberg_read_bytes
                .with_guarded_label_values(&[table_name]),
            file_read_duration_seconds: metrics
                .iceberg_source_file_read_duration_seconds
                .with_guarded_label_values(&[table_name]),
            rows_read_total: metrics
                .iceberg_source_rows_read_total
                .with_guarded_label_values(&[table_name]),
            files_read_total: metrics
                .iceberg_source_files_read_total
                .with_guarded_label_values(&[table_name, "data"]),
            delete_rows_applied_total: metrics
                .iceberg_source_delete_rows_applied_total
                .with_guarded_label_values(&[table_name, "sdk_applied_approx"]),
        }
    }

    pub fn record_read_bytes(&self, bytes: u64) {
        self.read_bytes.inc_by(bytes);
    }

    pub fn record_file_read_duration(&self, seconds: f64) {
        self.file_read_duration_seconds.observe(seconds);
    }

    pub fn record_rows_read(&self, rows: u64) {
        self.rows_read_total.inc_by(rows);
    }

    pub fn record_file_read(&self) {
        self.files_read_total.inc();
    }

    pub fn record_delete_rows_applied(&self, rows: u64) {
        self.delete_rows_applied_total.inc_by(rows);
    }
}

impl IcebergScanMetrics {
    fn new(registry: &Registry) -> Self {
        let iceberg_read_bytes = register_guarded_int_counter_vec_with_registry!(
            "iceberg_read_bytes",
            "Total size of iceberg read requests",
            &["table_name"],
            registry
        )
        .unwrap();

        let iceberg_source_snapshot_lag_seconds = register_guarded_int_gauge_vec_with_registry!(
            "iceberg_source_snapshot_lag_seconds",
            "Lag between latest available snapshot and last ingested snapshot in seconds",
            &["source_id", "source_name", "table_name"],
            registry
        )
        .unwrap();

        let iceberg_source_snapshots_discovered_total =
            register_guarded_int_counter_vec_with_registry!(
                "iceberg_source_snapshots_discovered_total",
                "Total number of snapshots discovered via incremental scan",
                &["source_id", "source_name", "table_name"],
                registry
            )
            .unwrap();

        let iceberg_source_list_duration_seconds = register_guarded_histogram_vec_with_registry!(
            histogram_opts!(
                "iceberg_source_list_duration_seconds",
                "Time spent planning files from a snapshot",
                exponential_buckets(0.01, 2.0, 15).unwrap() // 10ms to ~164s
            ),
            &["source_id", "source_name", "table_name"],
            registry
        )
        .unwrap();

        let iceberg_source_files_discovered_total =
            register_guarded_int_counter_vec_with_registry!(
                "iceberg_source_files_discovered_total",
                "Total number of files discovered per scan",
                &["source_id", "source_name", "table_name", "file_type"],
                registry
            )
            .unwrap();

        // Note: file-read metrics use ["table_name"] labels (matching iceberg_read_bytes)
        // because the scan function doesn't have source-level context.
        let iceberg_source_file_read_duration_seconds =
            register_guarded_histogram_vec_with_registry!(
                histogram_opts!(
                    "iceberg_source_file_read_duration_seconds",
                    "Per-file read duration",
                    exponential_buckets(0.01, 2.0, 15).unwrap() // 10ms to ~164s
                ),
                &["table_name"],
                registry
            )
            .unwrap();

        let iceberg_source_rows_read_total = register_guarded_int_counter_vec_with_registry!(
            "iceberg_source_rows_read_total",
            "Total rows read from Iceberg source",
            &["table_name"],
            registry
        )
        .unwrap();

        let iceberg_source_files_read_total = register_guarded_int_counter_vec_with_registry!(
            "iceberg_source_files_read_total",
            "Total files read from Iceberg source",
            &["table_name", "file_type"],
            registry
        )
        .unwrap();

        let iceberg_source_delete_rows_applied_total =
            register_guarded_int_counter_vec_with_registry!(
                "iceberg_source_delete_rows_applied_total",
                "Total rows removed by delete processing",
                &["table_name", "delete_type"],
                registry
            )
            .unwrap();

        let iceberg_source_delete_files_per_data_file =
            register_guarded_histogram_vec_with_registry!(
                histogram_opts!(
                    "iceberg_source_delete_files_per_data_file",
                    "Number of delete files attached per data file scan task",
                    // 1, 2, 4, 8, 16, 32, 64, 128
                    exponential_buckets(1.0, 2.0, 8).unwrap()
                ),
                &["source_id", "source_name", "table_name"],
                registry
            )
            .unwrap();

        let iceberg_source_inflight_file_count = register_guarded_int_gauge_vec_with_registry!(
            "iceberg_source_inflight_file_count",
            "Number of files currently being fetched by the active reader",
            &["source_id", "source_name", "table_name"],
            registry
        )
        .unwrap();

        let iceberg_source_scan_errors_total = register_guarded_int_counter_vec_with_registry!(
            "iceberg_source_scan_errors_total",
            "Total number of scan errors categorized by error type",
            &["source_id", "source_name", "table_name", "error_type"],
            registry
        )
        .unwrap();

        Self {
            iceberg_read_bytes,
            iceberg_source_snapshot_lag_seconds,
            iceberg_source_snapshots_discovered_total,
            iceberg_source_list_duration_seconds,
            iceberg_source_files_discovered_total,
            iceberg_source_file_read_duration_seconds,
            iceberg_source_rows_read_total,
            iceberg_source_files_read_total,
            iceberg_source_delete_rows_applied_total,
            iceberg_source_delete_files_per_data_file,
            iceberg_source_inflight_file_count,
            iceberg_source_scan_errors_total,
        }
    }

    pub fn for_test() -> Arc<Self> {
        Arc::new(GLOBAL_ICEBERG_SCAN_METRICS.clone())
    }
}

pub static GLOBAL_ICEBERG_SCAN_METRICS: LazyLock<IcebergScanMetrics> =
    LazyLock::new(|| IcebergScanMetrics::new(&GLOBAL_METRICS_REGISTRY));
