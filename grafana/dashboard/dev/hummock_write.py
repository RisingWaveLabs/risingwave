from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Hummock (Write)",
            [
                panels.subheader("Event Handling"),
                panels.timeseries_count(
                    "Event Handler Pending Event Count",
                    "",
                    [
                        panels.target(
                            f"sum({metric('state_store_event_handler_pending_event')}) by ({COMPONENT_LABEL}, {NODE_LABEL}, event_type)",
                            "{{event_type}} {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Event Handler Latency",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_event_handler_latency_bucket')}[$__rate_interval])) by (le, event_type, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " {{event_type}} {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_uploader_wait_poll_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " finished_task_wait_poll {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.subheader("Uploader"),
                panels.timeseries_bytes(
                    "Uploader Memory Size",
                    "This metric shows the real memory usage of uploader.",
                    [
                        panels.target(
                            f"sum({metric('uploading_memory_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "uploading imm size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('state_store_uploader_imm_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "uploader imm size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('state_store_uploader_imm_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL}) - "
                            f"sum({metric('state_store_uploader_uploading_task_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "unflushed imm size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('uploading_memory_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL}) - "
                            f"sum({metric('state_store_uploader_imm_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "in channel imm size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('state_store_per_table_imm_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL}) - "
                            f"sum({metric('uploading_memory_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "orphan imm size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('state_store_old_value_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "old value size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({table_metric('state_store_uploader_per_table_imm_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL}, table_id)",
                            "Uploader per-table imm size - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({table_metric('state_store_per_table_imm_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL}, table_id)",
                            "Per-table imm size - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('state_store_per_table_imm_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "All imm size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Build and Sync SSTable Duration",
                    "Histogram of time spent on compacting shared buffer to remote storage.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_sync_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " Sync duration - {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_sync_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_sync_duration_count')}[$__rate_interval])) > 0",
                            "avg Sync duration - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_uploader_upload_task_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " upload task duration - {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Streaming Relation Write Size",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f'sum(histogram_quantile({quantile}, sum(rate({metric("state_store_write_batch_size_bucket")}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)) * on(table_id) group_left(materialized_view_id) (group({metric("table_info")}) by (materialized_view_id, table_id))) by (materialized_view_id, table_name)',
                                f"write p{legend} - relation {{{{materialized_view_id}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Uploader Task Count",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('state_store_spill_task_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},uploader_stage)",
                            "Uploader spill tasks count rate - {{uploader_stage}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum({metric('state_store_uploader_uploading_task_count')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "Uploader total task count - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('state_store_uploader_syncing_epoch_count')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "syncing epoch count - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Uploader Task Size",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('state_store_spill_task_size')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},uploader_stage)",
                            "Uploader spill tasks size rate - {{uploader_stage}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum({metric('state_store_uploader_uploading_task_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "Total uploading task size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                    ],
                ),
                panels.subheader("Write"),
                panels.timeseries_ops(
                    "Write Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_write_batch_duration_count')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "write batch - {{table_id}} @ {{%s}} @ {{%s}} "
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('state_store_sync_duration_count')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "l0 - {{%s}} @ {{%s}} " % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Write Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_write_batch_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"write to shared_buffer p{legend}"
                                + " - {{table_id}} @ {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)(rate({table_metric('state_store_write_batch_duration_sum')}[$__rate_interval]))  / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)(rate({table_metric('state_store_write_batch_duration_count')}[$__rate_interval])) > 0",
                            "write to shared_buffer avg - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Write Item Count",
                    "",
                    [
                        panels.target(
                            f"sum(irate({table_metric('state_store_write_batch_tuple_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "write_batch_kv_pair_count - {{table_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Write Throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_write_batch_size_sum')}[$__rate_interval]))by({COMPONENT_LABEL},{NODE_LABEL},table_id) / sum(rate({table_metric('state_store_write_batch_size_count')}[$__rate_interval]))by({COMPONENT_LABEL},{NODE_LABEL},table_id) > 0",
                            "shared_buffer - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('compactor_shared_buffer_to_sstable_size_sum')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL}) / sum(rate({metric('compactor_shared_buffer_to_sstable_size_count')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL}) > 0",
                            "sync - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Write Batch Size",
                    "This metric shows the statistics of mem_table size on flush. By default only max (p100) is shown.",
                    [
                        panels.target(
                            f"histogram_quantile(1.0, sum(rate({metric('state_store_write_batch_size_bucket')}[$__rate_interval])) by (le, table_id, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "pmax - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_write_batch_size_sum')}[$__rate_interval])) / sum by(le, table_id, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_write_batch_size_count')}[$__rate_interval])) > 0",
                            "avg - {{table_id}} {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Mem Table Spill Count",
                    "",
                    [
                        panels.target(
                            f"sum(irate({table_metric('state_store_mem_table_spill_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "mem table spill table id - {{table_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Checkpoint Sync Size",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_sync_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_sync_size_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_sync_size_count')}[$__rate_interval])) > 0",
                            "avg - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
            ],
        )
    ]
