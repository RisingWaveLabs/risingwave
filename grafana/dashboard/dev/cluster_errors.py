from ..common import *
from . import section

@section
def _(outer_panels: Panels):
    panels = outer_panels
    return [
        outer_panels.row_collapsed(
            "Cluster Errors",
            [
                panels.subheader("Container Termination Reasons"),
                panels.timeseries_count(
                    "Termination reasons (OOMKilled, etc...)",
                    "The reasons for the termination of the containers.",
                    [
                        # kube-state-metrics may be scraped by multiple sources (HA replicas, BYOC
                        # federation). Join timestamp<->reason within each source first (full label
                        # set keeps sources apart), then `max by` dedups across sources.
                        panels.target(
                            'max by (cluster,namespace,pod,container,reason) ('
                            'kube_pod_container_status_last_terminated_timestamp{cluster=~"$cluster",namespace=~"$namespace",pod=~"$pod"}'
                            ' * ignoring(reason) group_left(reason) '
                            'kube_pod_container_status_last_terminated_reason{cluster=~"$cluster",namespace=~"$namespace",pod=~"$pod"}'
                            ') * 1000',
                            "[{{reason}}] {{container}} {{pod}}",
                        )
                    ],
                    legendCols=["last"],
                    unit='dateTimeAsIso',
                ),

                panels.subheader("User Streaming Errors"),
                panels.timeseries_count(
                    "Errors",
                    "Errors in the system group by type",
                    [
                        panels.target(
                            f"{metric('source_status_is_up')} == 0",
                            "source error: source_id={{source_id}}, source_name={{source_name}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_failure_count')}[$__rate_interval])) by ({NODE_LABEL}, {COMPONENT_LABEL}, type)",
                            "remote storage error {{type}}: {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        # We add a small constant 0.05 to make sure that the counter jumps from null to not-null,
                        # the line will be flat at y=0.05 instead of disappearing.
                        panels.target(
                            f"(sum(irate({metric('user_compute_error_cnt')}[$__rate_interval])) by (error_type, executor_name, fragment_id) or "
                            + f"sum({metric('user_compute_error_cnt')}) by (error_type, executor_name, fragment_id) * 0 + 0.05 "
                            + f") unless on({COMPONENT_LABEL}, {NODE_LABEL}) ((absent_over_time({metric('user_compute_error_cnt')}[20s])) > 0)",
                            "{{error_type}} @ {{executor_name}} (fragment_id={{fragment_id}})",
                        ),
                        panels.target(
                            f"(sum(irate({metric('user_source_error_cnt')}[$__rate_interval])) by (error_type, source_id, source_name, fragment_id) or "
                            + f"sum({metric('user_source_error_cnt')}) by (error_type, source_id, source_name, fragment_id) * 0 + 0.05 "
                            + f") unless on({COMPONENT_LABEL}, {NODE_LABEL}) ((absent_over_time({metric('user_source_error_cnt')}[20s])) > 0)",
                            "{{error_type}} @ {{source_name}} (source_id={{source_id}} fragment_id={{fragment_id}})",
                        ),
                        panels.target(
                            f"(sum(irate({metric('user_sink_error_cnt')}[$__rate_interval])) by (error_type, sink_id, sink_name, fragment_id) or "
                            + f"sum({metric('user_sink_error_cnt')}) by (error_type, sink_id, sink_name, fragment_id) * 0 + 0.05 "
                            + f") unless on({COMPONENT_LABEL}, {NODE_LABEL}) ((absent_over_time({metric('user_sink_error_cnt')}[20s])) > 0)",
                            "{{error_type}} @ {{sink_name}} (sink_id={{sink_id}} fragment_id={{fragment_id}})",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compute Errors by Type",
                    "Errors that happened during computation. Check the logs for detailed error message.",
                    [
                        panels.target(
                            f"(sum(irate({metric('user_compute_error_cnt')}[$__rate_interval])) by (error_type, executor_name, fragment_id) or "
                            + f"sum({metric('user_compute_error_cnt')}) by (error_type, executor_name, fragment_id) * 0 + 0.05 "
                            + f") unless on({COMPONENT_LABEL}, {NODE_LABEL}) ((absent_over_time({metric('user_compute_error_cnt')}[20s])) > 0)",
                            "{{error_type}} @ {{executor_name}} (fragment_id={{fragment_id}})",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Source Errors by Type",
                    "Errors that happened during source data ingestion. Check the logs for detailed error message.",
                    [
                        panels.target(
                            f"(sum(irate({metric('user_source_error_cnt')}[$__rate_interval])) by (error_type, source_id, source_name, fragment_id) or "
                            + f"sum({metric('user_source_error_cnt')}) by (error_type, source_id, source_name, fragment_id) * 0 + 0.05 "
                            + f") unless on({COMPONENT_LABEL}, {NODE_LABEL}) ((absent_over_time({metric('user_source_error_cnt')}[20s])) > 0)",
                            "{{error_type}} @ {{source_name}} (source_id={{source_id}} fragment_id={{fragment_id}})",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Parquet Source Skip Count",
                    "Errors that happened during source data ingestion. Check the logs for detailed error message.",
                    [
                        panels.target(
                            f"{metric('parquet_source_skip_row_count')}",
                            "source_id={{source_id}} @ source_name =  {{source_name}}",
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Sink Errors by Type",
                    "Errors that happened during data sink out. Check the logs for detailed error message.",
                    [
                        panels.target(
                            f"(sum(irate({metric('user_sink_error_cnt')}[$__rate_interval])) by (error_type, sink_id, sink_name, fragment_id) or "
                            + f"sum({metric('user_sink_error_cnt')}) by (error_type, sink_id, sink_name, fragment_id) * 0 + 0.05 "
                            + f") unless on({COMPONENT_LABEL}, {NODE_LABEL}) ((absent_over_time({metric('user_sink_error_cnt')}[20s])) > 0)",
                            "{{error_type}} @ {{sink_name}} (sink_id={{sink_id}} fragment_id={{fragment_id}})",
                        ),
                    ],
                ),
            ],
        )
    ]
