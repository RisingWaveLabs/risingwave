from ..common import *
from . import section

cross_db_last_consumed_min_epoch = (
    f"min({metric('crossdb_last_consumed_min_epoch', node_filter_enabled=False, table_id_filter_enabled=True)} != 0) by (table_id)"
)
cross_db_log_expiry_headroom = (
    f"({epoch_to_unix_millis(cross_db_last_consumed_min_epoch)} / 1000"
    f" - (time() - max({metric('streaming_table_change_log_retention_seconds', node_filter_enabled=False, table_id_filter_enabled=True)}) by (table_id)))"
)
mysql_cdc_binlog_file_seq_min = metric(
    "mysql_cdc_binlog_file_seq_min", node_filter_enabled=False
)
mysql_cdc_binlog_file_seq_max = metric(
    "mysql_cdc_binlog_file_seq_max", node_filter_enabled=False
)
stream_mysql_cdc_state_binlog_file_seq = metric(
    "stream_mysql_cdc_state_binlog_file_seq", node_filter_enabled=False
)
mysql_cdc_binlog_file_lag = (
    f"clamp_min({mysql_cdc_binlog_file_seq_max} - on(source_id) "
    f"{stream_mysql_cdc_state_binlog_file_seq}, 0)"
)
mysql_cdc_binlog_retention_risk_margin = (
    f"(({mysql_cdc_binlog_file_seq_max} + "
    f"{mysql_cdc_binlog_file_seq_min}) / 2 - on(source_id) "
    f"{stream_mysql_cdc_state_binlog_file_seq})"
)
mysql_cdc_binlog_retention_risk = (
    f"{alert_when(mysql_cdc_binlog_file_lag)} and "
    f"{alert_threshold(mysql_cdc_binlog_retention_risk_margin, 0)}"
)

@section
def _(outer_panels: Panels):
    panels = outer_panels
    return [
        outer_panels.row_collapsed(
            "Cluster Alerts",
            [
                panels.subheader(
                    "Streaming Alerts",
"""[Alert Reference]
- Recovery Triggered: cluster recovery is triggered. Check `Errors by Type` / `Node Count` panels to find the root cause. Check the error logs as well.
- Too Many Barriers: there are too many uncommitted barriers generated. This means the streaming graph is stuck.
  Check the following panels to follow-up:
  - `Streaming Backfill`: Check if there's any throughput in the panels, if yes, backfill is in progress. If throughput is high, it could lead to additional pressure on the streaming graph.
  - `Storage Alerts`: Look at the alerts in the storage section, specifically `Write Stall`. That will cause backpressure and the streaming graph being stuck.
  - `Cluster Resource Alerts`: If Relative CPU or Memory usage is high, it can lead to the streaming graph being stuck.
  - `Barrier Latency`: Get the latency of the barrier, it should be high.
  - `Streaming Relations`: Look at the TopN relations by CPU usage and busy rate. These relations are likely to be the bottleneck.
  - `Streaming Operators by Operator`: Look at the alerts in the streaming operators by operator section, the following panels are more likely to be the bottleneck:
    - Merger Barrier Align: If the merger barrier align is high, it means the merger is not able to align the barriers in time.
    - Join Amplification: If the join amplification is high, it means the join is not able to process the data in time.
- Cross-DB Log Retention Expiring: a cross-database MV changelog consumer's last consumed changelog epoch will expire within 12 hours.
- PG CDC WAL Lag Too High: the PostgreSQL CDC WAL lag (upstream_max_lsn - state_table_lsn) exceeds 20 GiB. Check `Streaming CDC` > `PostgreSQL CDC State Table WAL Lag` and verify replication slot health.
- MySQL CDC Binlog File Lag Too High: the MySQL CDC checkpoint is at least 20 files behind the upstream newest file, or it is behind the newest file and has entered the older half of the retained binlog range. Check `Streaming CDC` > `MySQL CDC Binlog File Lag` and `MySQL CDC Binlog Retention Risk Margin`.
""",
                    height=10,
                ),
                panels.timeseries_count(
                    "Streaming Alert Signals",
                    "",
                    [
                        panels.target(
                            alert_when(
                                f"sum(rate({metric('recovery_latency_count')}[$__rate_interval])) by (recovery_type) + "
                                + f"sum(rate({metric('recovery_failure_cnt')}[$__rate_interval])) by (recovery_type)"
                            ),
                            "Recovery Triggered {{recovery_type}}",
                        ),
                        panels.target(
                            alert_threshold(metric("all_barrier_nums"), 200),
                            "Too Many Barriers {{database_id}}",
                        ),
                        panels.target(
                            alert_threshold(
                                cross_db_log_expiry_headroom,
                                43200,
                                "<",
                            ),
                            "Cross-DB Log Retention Expiring table {{table_id}}",
                        ),
                        panels.target(
                            alert_threshold(
                                f"clamp_min({metric('pg_cdc_upstream_max_lsn')} - on(source_id) group_left(slot_name) {metric('stream_pg_cdc_state_table_lsn')}, 0)",
                                20 * 1024 * 1024 * 1024,
                            ),
                            "PG CDC WAL Lag Too High slot {{slot_name}} source {{source_id}}",
                        ),
                        panels.target(
                            f"{alert_threshold(mysql_cdc_binlog_file_lag, 20)} or "
                            f"({mysql_cdc_binlog_retention_risk})",
                            "MySQL CDC Binlog File Lag Too High source {{source_id}} {{hostname}}:{{port}}",
                        ),
                    ],
                    ["last"],
                ),
                panels.subheader(
                    "Cluster Resource Alerts",
                    """[Alert Reference]
- Unexpected Termination: components are exiting unexpectedly (OOMKilled, Error, etc).
  - Follow-up: Check the termination reasons in `Cluster Errors` > `Termination reasons (OOMKilled, etc...)`.
- CPU Saturation: the average CPU utilization per core is too high, and the system may be throttled.
  - Mitigate: Scale the corresponding components to avoid performance issues.
  - Follow-up: If it's a streaming issue, check `Streaming Relation Metrics` > `CPU Usage Per Streaming Job` and look for top streaming relations by CPU time.
""",
                    height=6,
                ),
                panels.timeseries_count(
                    "Cluster Resource Alert Signals",
                    "",
                    [
                        panels.target(
                            alert_threshold(
                                f"sum(rate({metric('process_cpu_seconds_total')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / "
                                + f"avg({metric('process_cpu_core_num')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                                0.9,
                                ">",
                            ),
                            "CPU Saturation (avg/core) - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            alert_threshold(
                                'sum(topk by (namespace, pod) (1, rate(container_cpu_usage_seconds_total{namespace=~"$namespace",container=~"$component",pod=~"$pod"}[$__rate_interval]))) by (namespace, pod) / '
                                + 'sum by(namespace, pod) (topk(1, kube_pod_container_resource_limits{namespace=~"$namespace",pod=~"$pod",container=~"$component", resource="cpu"}) by (namespace, pod))',
                                0.9,
                                ">",
                            ),
                            "CPU Saturation (k8s limit) - {{namespace}}/{{pod}}",
                        ),
                        # kube-state-metrics may be scraped by multiple sources (HA replicas, BYOC
                        # federation). Join timestamp<->reason within each source first (full label
                        # set keeps sources apart), then `max by` dedups across sources.
                        panels.target(
                            alert_when(
                                "changes((max by (cluster,namespace,pod,container,reason) ("
                                + 'kube_pod_container_status_last_terminated_timestamp{cluster=~"$cluster",namespace=~"$namespace",pod=~"$pod"}'
                                + " * ignoring(reason) group_left(reason) "
                                + 'kube_pod_container_status_last_terminated_reason{cluster=~"$cluster",namespace=~"$namespace",pod=~"$pod",reason!~"Completed"}'
                                + "))[$__rate_interval:])"
                            ),
                            "[{{reason}}] {{container}} {{pod}}",
                        ),
                    ],
                    ["last"],
                ),
                panels.subheader(
                    "Storage Alerts",
                    """[Alert Reference]
- Lagging Version: the checkpointed or pinned version id is lagging behind the current version id. Check `Hummock Manager` section in dev dashboard.
- Lagging Compaction: there are too many ssts in L0. This can be caused by compactor failure or lag of compactor resource. Check `Compaction` section in dev dashboard, and take care of the type of `Commit Flush Bytes` and `Compaction Throughput`, whether the throughput is too low.
- Lagging Vacuum: there are too many stale files waiting to be cleaned. This can be caused by compactor failure or lag of compactor resource. Check `Compaction` section in dev dashboard.
- Abnormal Uploading Memory Usage: uploading memory is more than 70 percent of the expected, and is about to spill.
- Write Stall: Compaction cannot keep up. Stall foreground write, Check `Compaction` section in dev dashboard.
- Abnormal Version Size: the size of the version is too large, exceeding the expected 300MB. Check `Hummock Manager` section in dev dashboard.
- Abnormal Delta Log Number: the number of delta logs is too large, exceeding the expected 5000. Check `Hummock Manager` and `Compaction` section in dev dashboard and take care of the type of `Compaction Success Count`, whether the number of trivial-move tasks spiking.
- Abnormal Pending Event Number: the number of pending events is too large, exceeding the expected 10000000. Check `Hummock Write` section in dev dashboard and take care of the `Event handle latency`, whether the time consumed exceeds the barrier latency.
- Abnormal Object Storage Failure: object storage failures are occurring. Check `Object Storage` section in dev dashboard and take care of the `Object Storage Failure Rate`, whether the rate is too high.
""",
                    height=12,
                ),
                panels.timeseries_count(
                    "Storage Alert Signals",
                    "",
                    [
                        panels.target(
                            alert_threshold(
                                f"{metric('storage_current_version_id')} - {metric('storage_checkpoint_version_id')}",
                                1000,
                            ),
                            "Lagging Version (checkpoint)",
                        ),
                        panels.target(
                            alert_threshold(
                                f"{metric('storage_current_version_id')} - {metric('storage_min_pinned_version_id')}",
                                1000,
                            ),
                            "Lagging Version (pinned)",
                        ),
                        panels.target(
                            alert_threshold(
                                f"sum(label_replace({metric('storage_level_total_file_size')}, 'L0', 'L0', 'level_index', '.*_L0') unless "
                                + f"{metric('storage_level_total_file_size')}) by (L0)",
                                52428800,
                            ),
                            "Lagging Compaction",
                        ),
                        panels.target(
                            alert_threshold(f"min_over_time({metric('storage_stale_object_count')}[5m])", 200),
                            "Lagging Vacuum",
                        ),
                        panels.target(
                            alert_threshold(
                                metric("state_store_uploading_memory_usage_ratio", filter=f'{COMPONENT_LABEL}="compute"'), 0.8
                            ),
                            "Abnormal Uploading Memory Usage @ {{%s}}" % (NODE_LABEL),
                        ),
                        panels.target(
                            alert_threshold(
                                metric("storage_write_stop_compaction_groups"), 0, ">"
                            ),
                            "Write Stall (group {{compaction_group_id}})",
                        ),
                        panels.target(
                            alert_threshold(metric("storage_version_size"), 314572800),
                            "Abnormal Version Size",
                        ),
                        panels.target(
                            alert_threshold(metric("storage_delta_log_count"), 5000),
                            "Abnormal Delta Log Number",
                        ),
                        panels.target(
                            alert_threshold(f"sum({metric('state_store_event_handler_pending_event')}) by ({NODE_LABEL})", 10000000),
                            "Abnormal Pending Event Number @ {{%s}}" % (NODE_LABEL),
                        ),
                        panels.target(
                            alert_when(
                                f"sum(rate({metric('object_store_failure_count')}[$__rate_interval])) by (type)"
                            ),
                            "Abnormal Object Storage Failure ({{type}})",
                        ),
                    ],
                    ["last"],
                ),
            ],
        )
    ]
