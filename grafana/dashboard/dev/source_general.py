from ..common import *
from . import section

@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "General",
            [
                panels.subheader("Source"),
                panels.timeseries_rowsps(
                    "Source Throughput(rows/s)",
                    "The figure shows the number of rows read by each source per second.",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_source_output_rows_counts')}[$__rate_interval])) by (source_id, source_name, fragment_id)",
                            "{{source_id}} {{source_name}} (fragment {{fragment_id}})",
                        ),
                        panels.target(
                            f"sum(rate({metric('file_source_input_row_count')}[$__rate_interval])) by (source_id, source_name, fragment_id)",
                            "{{source_name}} source_id {{source_id}} (fragment {{fragment_id}})",
                        )
                    ],
                ),
                panels.timeseries_rowsps(
                    "Source Throughput(rows/s) Per Partition",
                    "Each query is executed in parallel with a user-defined parallelism. This figure shows the throughput of "
                    "each parallelism. The throughput of all the parallelism added up is equal to Source Throughput(rows).",
                    [
                        panels.target(
                            f"rate({metric('source_partition_input_count')}[$__rate_interval])",
                            "actor={{actor_id}} source={{source_id}} partition={{partition}} fragment_id={{fragment_id}}",
                        )
                    ],
                ),
                panels.timeseries_bytesps(
                    "Source Throughput(MB/s)",
                    "The figure shows the number of bytes read by each source per second.",
                    [
                        panels.target(
                            f"(sum by (source_id, source_name, fragment_id)(rate({metric('source_partition_input_bytes')}[$__rate_interval])))/(1000*1000)",
                            "{{source_id}} {{source_name}} (fragment {{fragment_id}})",
                        )
                    ],
                ),
                panels.timeseries_bytesps(
                    "Source Throughput(MB/s) Per Partition",
                    "Each query is executed in parallel with a user-defined parallelism. This figure shows the throughput of "
                    "each parallelism. The throughput of all the parallelism added up is equal to Source Throughput(MB/s).",
                    [
                        panels.target(
                            f"(rate({metric('source_partition_input_bytes')}[$__rate_interval]))/(1000*1000)",
                            "actor={{actor_id}} source={{source_id}} partition={{partition}} fragment_id={{fragment_id}}",
                        )
                    ],
                ),
                panels.timeseries_rowsps(
                    "Source Backfill Throughput(rows/s)",
                    "The figure shows the number of rows read by each source per second.",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_source_backfill_rows_counts')}[$__rate_interval])) by (source_id, source_name, fragment_id)",
                            "{{source_id}} {{source_name}} (fragment {{fragment_id}})",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Source Upstream Status",
                    "Monitor each source upstream, 0 means the upstream is not normal, 1 means the source is ready.",
                    [
                        panels.target(
                            f"{metric('source_status_is_up')}",
                            "source_id={{source_id}}, source_name={{source_name}} @ {{%s}}"
                            % NODE_LABEL,
                        )
                    ],
                ),
                panels.timeseries_latency(
                    "Source Worker Tick Duration",
                    "Duration of each source worker tick (split discovery + enumerator monitoring). Long ticks indicate slow or hanging network I/O toward the upstream system.",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('source_worker_tick_duration_seconds_bucket')}[$__rate_interval])) by (le, source_id, source_name))",
                            f"p{legend} - {{{{source_id}}}} {{{{source_name}}}}",
                        ),
                        [50, 90, 99, "max"],
                    ),
                ),
                panels.timeseries_ops(
                    "Source Enumerator Monitor Errors (errors/s)",
                    "Rate of enumerator monitoring errors per source. Nonzero means the periodic upstream health check is failing (drives Source Upstream Status to 0).",
                    [
                        panels.target(
                            f"rate({metric('source_enumerator_monitor_error_count')}[$__rate_interval])",
                            "source_id={{source_id}} source_name={{source_name}}",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Source Split Change Events frequency(events/s)",
                    "Source Split Change Events frequency by source_id and actor_id",
                    [
                        panels.target(
                            f"rate({metric('stream_source_split_change_event_count')}[$__rate_interval])",
                            "source={{source_name}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        )
                    ],
                ),
                panels.timeseries_count(
                    "File Source Dirty Split Count",
                    "Current number of dirty file splits by source.",
                    [
                        panels.target(
                            f"{metric('file_source_dirty_split_count')}",
                            "{{source_name}} source_id {{source_id}} (fragment {{fragment_id}})",
                        )
                    ],
                ),
                panels.timeseries_count(
                    "File Source Failed Split Count",
                    "Total number of file splits marked dirty by source.",
                    [
                        panels.target(
                            f"{metric('file_source_failed_split_count')}",
                            "{{source_name}} source_id {{source_id}} (fragment {{fragment_id}})",
                        )
                    ],
                ),
                panels.timeseries_ops(
                    "Source Connector Ack Successes",
                    "Rate of successful connector acks after checkpoint by source and connector type.",
                    [
                        panels.target(
                            f"sum(rate({metric('source_connector_ack_success_count')}[$__rate_interval])) by (source_name, connector_type)",
                            "{{source_name}} {{connector_type}}",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Source Connector Ack Failures",
                    "Rate of connector ack failures after checkpoint by source, connector type, and bounded failure category.",
                    [
                        panels.target(
                            f"sum(rate({metric('source_connector_ack_failure_count')}[$__rate_interval])) by (source_name, connector_type, error_type)",
                            "{{source_name}} {{connector_type}} {{error_type}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Kafka Consumer Lag Size",
                    "Kafka Consumer Lag Size by source_id, partition and actor_id",
                    [
                        panels.target(
                            f"clamp_min({metric('source_kafka_high_watermark')} - on(source_id, partition) group_right() {metric('source_latest_message_id')}, 0)",
                            "source={{source_id}} partition={{partition}} actor_id={{actor_id}}",
                        ),
                    ],
                ),
            ],
        )
    ]
