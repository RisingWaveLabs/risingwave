from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    clean_state = 'state="clean"'
    dirty_state = 'state="dirty"'
    return [
        outer_panels.row_collapsed(
            "Sync Log Store Metrics",
            [
                panels.timeseries_epoch(
                    "Sync Log Store Unconsumed Min Epoch",
                    "",
                    [
                        panels.target(
                            f"{metric('sync_kv_log_store_buffer_unconsumed_min_epoch')}",
                            "Sync Kv log store unconsumed min epoch @ {{fragment_id}} {{relation}} ({{target}}) actor {{actor_id}}",
                        ),
                    ],
                ),
                # FIXME(kwannoel): These are missing metrics
                panels.timeseries_latency(
                    "Log Store Lag",
                    "",
                    [
                        panels.target(
                            f"(max({metric('log_store_latest_write_epoch')} != 0) by (fragment_id, actor_id, relation)"
                            + f"- max({metric('log_store_latest_read_epoch')} != 0) by (fragment_id, actor_id, relation)) / (2^16) / 1000",
                            "{{fragment_id}} {{relation}} @ actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Log Store Backpressure Ratio",
                    "",
                    [
                        panels.target(
                            f"avg(rate({metric('sync_kv_log_store_wait_next_poll_ns')}[$__rate_interval])) by (target, fragment_id, actor_id, relation) / 1000000000",
                            "Backpressure @ {{fragment_id}} {{relation}} ({{target}}) actor {{actor_id}}",
                        ),
                    ],
                ),
                # FIXME(kwannoel): These are missing metrics
                panels.timeseries_latency(
                    "Log Store Consume Persistent Log Lag",
                    "",
                    [
                        panels.target(
                            f"clamp_min((max({metric('log_store_first_write_epoch')} != 0) by (fragment_id, actor_id, relation)"
                            + f"- max({metric('log_store_latest_read_epoch')} != 0) by (fragment_id, actor_id, relation)) / (2^16) / 1000, 0)",
                            "{{fragment_id}} {{relation}} @ actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Log Store Consume Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('sync_kv_log_store_read_count')}[$__rate_interval])) by (type, target, fragment_id, relation)",
                            "{{type}} {{fragment_id}} {{relation}} ({{target}})",
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "Log Store Consume Throughput(MB/s)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('sync_kv_log_store_read_size')}[$__rate_interval])) by (type, target, fragment_id, relation) / (1000*1000)",
                            "{{type}} {{fragment_id}} {{relation}} ({{target}})",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Log Store Write Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('sync_kv_log_store_storage_write_count')}[$__rate_interval])) by (fragment_id, relation)",
                            "{{fragment_id}} {{relation}}",
                        ),
                        panels.target_hidden(
                            f"sum(rate({metric('sync_kv_log_store_storage_write_count')}[$__rate_interval])) by ({NODE_LABEL}, fragment_id, actor_id, relation)",
                            "{{fragment_id}} {{relation}} @ actor {{actor_id}} {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Kv Log Store Buffer State (clean <= 0, dirty > 0)",
                    "",
                    [
                        panels.target_hidden(
                            f"sum(rate({metric('sync_kv_log_store_state')}[$__rate_interval])) by (state, fragment_id, relation)",
                            "{{state}} {{fragment_id}} {{relation}}",
                        ),
                        panels.target(
                            f"sum({metric('sync_kv_log_store_state', filter=clean_state)}) by (fragment_id, relation) - sum({metric('sync_kv_log_store_state', filter=dirty_state)}) by (fragment_id, relation)",
                            "current_state {{fragment_id}} {{relation}}",
                        ),
                    ]
                ),
                panels.timeseries_ops(
                    "Kv Log Store Write Storage Row Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('sync_kv_log_store_storage_write_count')}[$__rate_interval])) by (actor_id, target, fragment_id, relation)",
                            "{{fragment_id}} {{relation}} actor {{actor_id}} ({{target}})",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Kv Log Store Write Storage Size",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('sync_kv_log_store_storage_write_size')}[$__rate_interval])) by (actor_id, target, fragment_id, relation)",
                            "{{fragment_id}} {{relation}} actor {{actor_id}} ({{target}})",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Kv Log Store Buffer State",
                    "",
                    [
                        panels.target(
                            f"{metric('sync_kv_log_store_buffer_unconsumed_item_count')}",
                            "Unconsumed item count @ {{fragment_id}} {{relation}} ({{target}}) actor {{actor_id}}",
                        ),
                        panels.target(
                            f"{metric('sync_kv_log_store_buffer_unconsumed_row_count')}",
                            "Unconsumed row count @ {{fragment_id}} {{relation}} ({{target}}) actor {{actor_id}}",
                        ),
                        panels.target(
                            f"{metric('sync_kv_log_store_buffer_unconsumed_epoch_count')}",
                            "Unconsumed epoch count @ {{fragment_id}} {{relation}} ({{target}}) actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Kv Log Store Buffer Memory",
                    "Estimated heap bytes used by sync kv log store buffer (unconsumed + consumed but not truncated)",
                    [
                        panels.target(
                            f"{metric('sync_kv_log_store_buffer_memory_bytes')}",
                            "buffer memory @ {{fragment_id}} {{relation}} ({{target}}) actor {{actor_id}}",
                        ),
                    ],
                ),
                # FIXME(kwannoel): These are missing metrics
                panels.timeseries_bytes(
                    "Chunk Buffer Size",
                    "Total size of chunks buffered in a barrier",
                    [
                        panels.target(
                            f"sum({metric('stream_sink_chunk_buffer_size')}) by (fragment_id, actor_id, relation) * on(actor_id) group_left(relation) {metric('sink_info')}",
                            "sink {{fragment_id}} {{relation}} - actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Log Store Pause Ratio",
                    "",
                    [
                        panels.target(
                            f"avg(rate({metric('sync_kv_log_store_write_pause_duration_ns')}[$__rate_interval])) by (fragment_id, relation) / 1000000000",
                            "{{fragment_id}} {{relation}}",
                        )
                    ],
                ),
            ],
        )
    ]
