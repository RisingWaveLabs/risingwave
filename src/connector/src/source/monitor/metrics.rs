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

use std::sync::{Arc, LazyLock};

use prometheus::{
    IntCounterVec, Registry, exponential_buckets, histogram_opts,
    register_int_counter_vec_with_registry,
};
use risingwave_common::config::MetricLevel;
use risingwave_common::metrics::{
    LabelGuardedHistogramVec, LabelGuardedIntCounterVec, LabelGuardedIntGaugeVec,
    MetricVecRelabelExt, RelabeledGuardedIntCounterVec, RelabeledGuardedIntGaugeVec,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::{
    register_guarded_histogram_vec_with_registry, register_guarded_int_counter_vec_with_registry,
    register_guarded_int_gauge_vec_with_registry,
};

use crate::connector_metrics_level;
use crate::source::kafka::stats::RdKafkaStats;

/// Low-cardinality connector ack failure categories.
///
/// Keep this list bounded. Do not add raw connector error messages, topics,
/// partitions, or split identifiers as metric label values.
#[derive(Debug, Clone, Copy)]
pub enum ConnectorAckFailureType {
    Error,
    Timeout,
    EmptyMessageId,
    ChannelMissing,
    ChannelSendError,
    DecodeError,
    BrokerError,
}

impl ConnectorAckFailureType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Timeout => "timeout",
            Self::EmptyMessageId => "empty_message_id",
            Self::ChannelMissing => "channel_missing",
            Self::ChannelSendError => "channel_send_error",
            Self::DecodeError => "decode_error",
            Self::BrokerError => "broker_error",
        }
    }
}

#[derive(Debug, Clone)]
pub struct EnumeratorMetrics {
    pub high_watermark: LabelGuardedIntGaugeVec,
    /// Kafka consumer group delete attempts that failed during source enumerator cleanup.
    ///
    /// The `consumer_group` label is fragment-derived and emitted only on cleanup failures.
    pub kafka_consumer_group_delete_failure_count: IntCounterVec,
    /// PostgreSQL CDC confirmed flush LSN monitoring
    pub pg_cdc_confirmed_flush_lsn: LabelGuardedIntGaugeVec,
    /// PostgreSQL CDC upstream max LSN monitoring
    pub pg_cdc_upstream_max_lsn: LabelGuardedIntGaugeVec,
    /// MySQL CDC binlog file sequence number (min)
    pub mysql_cdc_binlog_file_seq_min: LabelGuardedIntGaugeVec,
    /// MySQL CDC binlog file sequence number (max)
    pub mysql_cdc_binlog_file_seq_max: LabelGuardedIntGaugeVec,
    /// SQL Server CDC upstream minimum LSN
    pub sqlserver_cdc_upstream_min_lsn: LabelGuardedIntGaugeVec,
    /// SQL Server CDC upstream maximum LSN
    pub sqlserver_cdc_upstream_max_lsn: LabelGuardedIntGaugeVec,
}

pub static GLOBAL_ENUMERATOR_METRICS: LazyLock<EnumeratorMetrics> =
    LazyLock::new(|| EnumeratorMetrics::new(&GLOBAL_METRICS_REGISTRY));

impl EnumeratorMetrics {
    fn new(registry: &Registry) -> Self {
        let high_watermark = register_guarded_int_gauge_vec_with_registry!(
            "source_kafka_high_watermark",
            "High watermark for a exec per partition",
            &["source_id", "partition"],
            registry,
        )
        .unwrap();

        let kafka_consumer_group_delete_failure_count = register_int_counter_vec_with_registry!(
            "source_kafka_consumer_group_delete_failure_count",
            "Total number of Kafka consumer group delete attempts that failed during source enumerator cleanup",
            &["source_id", "consumer_group"],
            registry,
        )
        .unwrap();

        let pg_cdc_confirmed_flush_lsn = register_guarded_int_gauge_vec_with_registry!(
            "pg_cdc_confirmed_flush_lsn",
            "PostgreSQL CDC confirmed flush LSN",
            &["source_id", "slot_name"],
            registry,
        )
        .unwrap();

        let pg_cdc_upstream_max_lsn = register_guarded_int_gauge_vec_with_registry!(
            "pg_cdc_upstream_max_lsn",
            "PostgreSQL CDC upstream max LSN (pg_current_wal_lsn)",
            &["source_id", "slot_name"],
            registry,
        )
        .unwrap();

        let mysql_cdc_binlog_file_seq_min = register_guarded_int_gauge_vec_with_registry!(
            "mysql_cdc_binlog_file_seq_min",
            "MySQL CDC upstream binlog file sequence number (minimum/oldest)",
            &["source_id", "hostname", "port"],
            registry,
        )
        .unwrap();

        let mysql_cdc_binlog_file_seq_max = register_guarded_int_gauge_vec_with_registry!(
            "mysql_cdc_binlog_file_seq_max",
            "MySQL CDC upstream binlog file sequence number (maximum/newest)",
            &["source_id", "hostname", "port"],
            registry,
        )
        .unwrap();

        let sqlserver_cdc_upstream_min_lsn = register_guarded_int_gauge_vec_with_registry!(
            "sqlserver_cdc_upstream_min_lsn",
            "SQL Server CDC upstream minimum LSN",
            &["source_id"],
            registry,
        )
        .unwrap();

        let sqlserver_cdc_upstream_max_lsn = register_guarded_int_gauge_vec_with_registry!(
            "sqlserver_cdc_upstream_max_lsn",
            "SQL Server CDC upstream maximum LSN",
            &["source_id"],
            registry,
        )
        .unwrap();

        EnumeratorMetrics {
            high_watermark,
            kafka_consumer_group_delete_failure_count,
            pg_cdc_confirmed_flush_lsn,
            pg_cdc_upstream_max_lsn,
            mysql_cdc_binlog_file_seq_min,
            mysql_cdc_binlog_file_seq_max,
            sqlserver_cdc_upstream_min_lsn,
            sqlserver_cdc_upstream_max_lsn,
        }
    }

    pub fn unused() -> Self {
        Default::default()
    }
}

impl Default for EnumeratorMetrics {
    fn default() -> Self {
        GLOBAL_ENUMERATOR_METRICS.clone()
    }
}

#[derive(Debug, Clone)]
pub struct SourceMetrics {
    pub partition_input_count: RelabeledGuardedIntCounterVec,

    // **Note**: for normal messages, the metric is the message's payload size.
    // For messages from load generator, the metric is the size of stream chunk.
    pub partition_input_bytes: RelabeledGuardedIntCounterVec,
    /// Report latest message id
    pub latest_message_id: RelabeledGuardedIntGaugeVec,
    pub partition_eof_count: LabelGuardedIntCounterVec,
    pub partition_eof_offset: LabelGuardedIntGaugeVec,
    pub rdkafka_native_metric: Arc<RdKafkaStats>,

    pub direct_cdc_event_lag_latency: LabelGuardedHistogramVec,

    pub parquet_source_skip_row_count: RelabeledGuardedIntCounterVec,
    pub file_source_input_row_count: RelabeledGuardedIntCounterVec,
    pub file_source_dirty_split_count: RelabeledGuardedIntGaugeVec,
    pub file_source_failed_split_count: RelabeledGuardedIntCounterVec,

    // kinesis source
    pub kinesis_throughput_exceeded_count: LabelGuardedIntCounterVec,
    pub kinesis_timeout_count: LabelGuardedIntCounterVec,
    pub kinesis_rebuild_shard_iter_count: LabelGuardedIntCounterVec,
    pub kinesis_early_terminate_shard_count: LabelGuardedIntCounterVec,
    pub kinesis_lag_latency_ms: LabelGuardedHistogramVec,

    /// Total connector ack failures after checkpoint commit by bounded failure category.
    connector_ack_failure_count: IntCounterVec,
    /// Total successful connector acks after checkpoint commit.
    connector_ack_success_count: IntCounterVec,
}

pub static GLOBAL_SOURCE_METRICS: LazyLock<SourceMetrics> =
    LazyLock::new(|| SourceMetrics::new(&GLOBAL_METRICS_REGISTRY, connector_metrics_level()));

impl SourceMetrics {
    pub fn inc_connector_ack_failure_count(
        &self,
        source_name: &str,
        connector_type: &'static str,
        failure_type: ConnectorAckFailureType,
    ) {
        self.connector_ack_failure_count
            .with_label_values(&[source_name, connector_type, failure_type.as_str()])
            .inc();
    }

    pub fn inc_connector_ack_success_count(&self, source_name: &str, connector_type: &'static str) {
        self.connector_ack_success_count
            .with_label_values(&[source_name, connector_type])
            .inc();
    }

    fn new(registry: &Registry, metric_level: MetricLevel) -> Self {
        let partition_input_count = register_guarded_int_counter_vec_with_registry!(
            "source_partition_input_count",
            "Total number of rows that have been input from specific partition",
            &[
                "actor_id",
                "source_id",
                "partition",
                "source_name",
                "fragment_id"
            ],
            registry
        )
        .unwrap()
        .relabel_debug_1(metric_level);
        let partition_input_bytes = register_guarded_int_counter_vec_with_registry!(
            "source_partition_input_bytes",
            "Total bytes that have been input from specific partition",
            &[
                "actor_id",
                "source_id",
                "partition",
                "source_name",
                "fragment_id"
            ],
            registry
        )
        .unwrap()
        .relabel_debug_1(metric_level);
        let latest_message_id = register_guarded_int_gauge_vec_with_registry!(
            "source_latest_message_id",
            "Latest message id for a exec per partition",
            &["actor_id", "source_id", "partition"],
            registry,
        )
        .unwrap()
        .relabel_debug_1(metric_level);
        let partition_eof_count = register_guarded_int_counter_vec_with_registry!(
            "source_partition_eof_count",
            "Total number of EOF events received from specific partition",
            &["source_id", "partition", "source_name", "fragment_id"],
            registry
        )
        .unwrap();
        let partition_eof_offset = register_guarded_int_gauge_vec_with_registry!(
            "source_partition_eof_offset",
            "Latest resolved EOF offset for specific partition",
            &["source_id", "partition", "source_name", "fragment_id"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "source_cdc_event_lag_duration_milliseconds",
            "source_cdc_lag_latency",
            exponential_buckets(1.0, 2.0, 21).unwrap(), // max 1048s
        );

        let parquet_source_skip_row_count = register_guarded_int_counter_vec_with_registry!(
            "parquet_source_skip_row_count",
            "Total number of rows that have been set to null in parquet source",
            &["actor_id", "source_id", "source_name", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(metric_level);

        let direct_cdc_event_lag_latency =
            register_guarded_histogram_vec_with_registry!(opts, &["table_name"], registry).unwrap();

        let rdkafka_native_metric = Arc::new(RdKafkaStats::new(registry.clone()));

        let file_source_input_row_count = register_guarded_int_counter_vec_with_registry!(
            "file_source_input_row_count",
            "Total number of rows that have been read in file source",
            &["actor_id", "source_id", "source_name", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(metric_level);
        let file_source_dirty_split_count = register_guarded_int_gauge_vec_with_registry!(
            "file_source_dirty_split_count",
            "Current number of dirty file splits in file source",
            &["actor_id", "source_id", "source_name", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(metric_level);
        let file_source_failed_split_count = register_guarded_int_counter_vec_with_registry!(
            "file_source_failed_split_count",
            "Total number of file splits marked dirty in file source",
            &["actor_id", "source_id", "source_name", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(metric_level);

        let kinesis_throughput_exceeded_count = register_guarded_int_counter_vec_with_registry!(
            "kinesis_throughput_exceeded_count",
            "Total number of times throughput exceeded in kinesis source",
            &["source_id", "source_name", "fragment_id", "shard_id"],
            registry
        )
        .unwrap();

        let kinesis_timeout_count = register_guarded_int_counter_vec_with_registry!(
            "kinesis_timeout_count",
            "Total number of times timeout in kinesis source",
            &["source_id", "source_name", "fragment_id", "shard_id"],
            registry
        )
        .unwrap();

        let kinesis_rebuild_shard_iter_count = register_guarded_int_counter_vec_with_registry!(
            "kinesis_rebuild_shard_iter_count",
            "Total number of times rebuild shard iter in kinesis source",
            &["source_id", "source_name", "fragment_id", "shard_id"],
            registry
        )
        .unwrap();

        let kinesis_early_terminate_shard_count = register_guarded_int_counter_vec_with_registry!(
            "kinesis_early_terminate_shard_count",
            "Total number of times early terminate shard in kinesis source",
            &["source_id", "source_name", "fragment_id", "shard_id"],
            registry
        )
        .unwrap();

        let kinesis_lag_latency_ms = register_guarded_histogram_vec_with_registry!(
            "kinesis_lag_latency_ms",
            "Lag latency in kinesis source",
            &["source_id", "source_name", "fragment_id", "shard_id"],
            registry
        )
        .unwrap();

        let connector_ack_failure_count = register_int_counter_vec_with_registry!(
            "source_connector_ack_failure_count",
            "Total number of connector ack failures after checkpoint commit by bounded failure category",
            &["source_name", "connector_type", "error_type"],
            registry
        )
        .unwrap();
        let connector_ack_success_count = register_int_counter_vec_with_registry!(
            "source_connector_ack_success_count",
            "Total number of successful connector acks after checkpoint commit",
            &["source_name", "connector_type"],
            registry
        )
        .unwrap();

        SourceMetrics {
            partition_input_count,
            partition_input_bytes,
            latest_message_id,
            partition_eof_count,
            partition_eof_offset,
            rdkafka_native_metric,
            direct_cdc_event_lag_latency,
            parquet_source_skip_row_count,
            file_source_input_row_count,
            file_source_dirty_split_count,
            file_source_failed_split_count,

            kinesis_throughput_exceeded_count,
            kinesis_timeout_count,
            kinesis_rebuild_shard_iter_count,
            kinesis_early_terminate_shard_count,
            kinesis_lag_latency_ms,

            connector_ack_failure_count,
            connector_ack_success_count,
        }
    }
}

impl Default for SourceMetrics {
    fn default() -> Self {
        GLOBAL_SOURCE_METRICS.clone()
    }
}
