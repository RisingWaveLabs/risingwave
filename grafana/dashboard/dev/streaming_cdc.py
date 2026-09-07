from ..common import *
from . import section

mysql_cdc_binlog_file_seq_min = metric(
    "mysql_cdc_binlog_file_seq_min", node_filter_enabled=False
)
mysql_cdc_binlog_file_seq_max = metric(
    "mysql_cdc_binlog_file_seq_max", node_filter_enabled=False
)
stream_mysql_cdc_state_binlog_file_seq = metric(
    "stream_mysql_cdc_state_binlog_file_seq", node_filter_enabled=False
)


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming CDC",
            [
                panels.timeseries_rowsps(
                    "CDC Backfill Snapshot Read Throughput(rows)",
                    "Total number of rows that have been read from the cdc backfill snapshot",
                    [
                        panels.target(
                            f"rate({table_metric('stream_cdc_backfill_snapshot_read_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "CDC Backfill Upstream Throughput(rows)",
                    "Total number of rows that have been output from the cdc backfill upstream",
                    [
                        panels.target(
                            f"rate({table_metric('stream_cdc_backfill_upstream_output_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency_ms(
                    "CDC Consume Lag Latency",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('source_cdc_event_lag_duration_milliseconds_bucket')}[$__rate_interval])) by (le, table_name))",
                                f"lag p{legend}" + " - {{table_name}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Auto Schema Change Failure Count",
                    "Total number of failed auto schema change attempts of CDC Table",
                    [
                        panels.target(
                            f"sum({metric('auto_schema_change_failure_cnt')}) by (table_id, table_name)",
                            "{{table_id}} - {{table_name}}",
                        )
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "Auto Schema Change Success Count",
                    "Total number of succeeded auto schema change of CDC Table",
                    [
                        panels.target(
                            f"sum({metric('auto_schema_change_success_cnt')}) by (table_id, table_name)",
                            "{{table_id}} - {{table_name}}",
                        )
                    ],
                    ["last"],
                ),
                panels.timeseries_latency(
                    "Auto Schema Change Latency (sec)",
                    "Latency of Auto Schema Change Process",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('auto_schema_change_latency_bucket')}[$__rate_interval])) by (le, table_id, table_name))",
                                f"lag p{legend}" + "{{table_id}} - {{table_name}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "PostgreSQL CDC LSN Progression",
                    "LSN progression through the CDC pipeline by source_id",
                    [
                        panels.target(
                            f"{metric('stream_pg_cdc_state_table_lsn')}",
                            "source_id {{source_id}} - State Table LSN",
                        ),
                        panels.target(
                            f"{metric('stream_pg_cdc_jni_commit_offset_lsn')}",
                            "source_id {{source_id}} - JNI Commit Offset LSN",
                        ),
                        panels.target(
                            f"{metric('pg_cdc_confirmed_flush_lsn')}",
                            "slot {{slot_name}} source_id {{source_id}} - Confirmed Flush LSN",
                        ),
                        panels.target(
                            f"{metric('pg_cdc_upstream_max_lsn')}",
                            "slot {{slot_name}} source_id {{source_id}} - Upstream Max LSN",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "PostgreSQL CDC Slot WAL Lag (bytes)",
                    "Upstream WAL lag in bytes: pg_current_wal_lsn - confirmed_flush_lsn (computed in PromQL).",
                    [
                        panels.target(
                            f"{metric('pg_cdc_upstream_max_lsn')} - {metric('pg_cdc_confirmed_flush_lsn')}",
                            "slot {{slot_name}} source_id {{source_id}} - WAL Lag",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "MySQL CDC Binlog Progression",
                    "Binlog file sequence and position progression by source_id",
                    [
                        panels.target(
                            f"{metric('stream_mysql_cdc_state_binlog_file_seq')}",
                            "source_id {{source_id}} - State Table Binlog File Seq",
                        ),
                        panels.target(
                            f"{metric('stream_mysql_cdc_state_binlog_position')}",
                            "source_id {{source_id}} - State Table Binlog Position (bytes)",
                        ),
                        panels.target(
                            f"{metric('mysql_cdc_binlog_file_seq_min')}",
                            "source_id {{source_id}} {{hostname}}:{{port}} - Upstream Binlog File Min Seq (oldest)",
                        ),
                        panels.target(
                            f"{metric('mysql_cdc_binlog_file_seq_max')}",
                            "source_id {{source_id}} {{hostname}}:{{port}} - Upstream Binlog File Max Seq (newest)",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "MySQL CDC Binlog File Lag",
                    "Lag measured as the number of binlog files between the upstream newest file and the RisingWave checkpoint.",
                    [
                        panels.target(
                            f"clamp_min({mysql_cdc_binlog_file_seq_max} - on(source_id) {stream_mysql_cdc_state_binlog_file_seq}, 0)",
                            "source_id {{source_id}} {{hostname}}:{{port}} - Binlog File Lag",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "MySQL CDC Binlog Retention Risk Margin",
                    "Retained binlog range midpoint minus the RisingWave checkpoint file sequence. A non-negative value means the checkpoint has entered the older half of the retained range.",
                    [
                        panels.target(
                            f"(({mysql_cdc_binlog_file_seq_max} + {mysql_cdc_binlog_file_seq_min}) / 2 - on(source_id) {stream_mysql_cdc_state_binlog_file_seq})",
                            "source_id {{source_id}} {{hostname}}:{{port}} - Retention Risk Margin",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "SQL Server CDC LSN Progression",
                    "LSN progression through the SQL Server CDC pipeline by source_id",
                    [
                        panels.target(
                            f"{metric('stream_sqlserver_cdc_state_change_lsn')}",
                            "source_id {{source_id}} - State Table Change LSN",
                        ),
                        panels.target(
                            f"{metric('stream_sqlserver_cdc_state_commit_lsn')}",
                            "source_id {{source_id}} - State Table Commit LSN",
                        ),
                        panels.target(
                            f"{metric('stream_sqlserver_cdc_jni_commit_offset_lsn')}",
                            "source_id {{source_id}} - JNI Commit Offset LSN",
                        ),
                        panels.target(
                            f"{metric('sqlserver_cdc_upstream_min_lsn')}",
                            "source_id {{source_id}} - Upstream Min LSN",
                        ),
                        panels.target(
                            f"{metric('sqlserver_cdc_upstream_max_lsn')}",
                            "source_id {{source_id}} - Upstream Max LSN",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "SQL Server CDC State Table LSN Lag",
                    "Lag measured as SQL Server LSN distance: upstream_max_lsn - state_change_lsn (not bytes).",
                    [
                        panels.target(
                            f"clamp_min({metric('sqlserver_cdc_upstream_max_lsn')} - on(source_id) {metric('stream_sqlserver_cdc_state_change_lsn')}, 0)",
                            "source_id {{source_id}} - LSN Lag",
                        ),
                    ],
                ),
            ],
        ),
    ]
