from ..common import *
from . import section

cross_db_last_consumed_min_epoch = (
    f"min({metric('crossdb_last_consumed_min_epoch', node_filter_enabled=False, table_id_filter_enabled=True)} != 0) by (table_id)"
)
cross_db_log_expiry_headroom = (
    f"({epoch_to_unix_millis(cross_db_last_consumed_min_epoch)} / 1000"
    f" - (time() - max({metric('streaming_table_change_log_retention_seconds', node_filter_enabled=False, table_id_filter_enabled=True)}) by (table_id)))"
)

@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming Backfill",
            [
                # FIXME(kwannoel): We should add the job id as the label, rather than solely the upstream table id.
                panels.timeseries_rowsps(
                    "Backfill Snapshot-Read Throughput (rows/s)",
                    "Rows/sec that we read from the backfill snapshot by table",
                    [
                        panels.target(
                            f"""
                                sum by (table_id) (
                                  rate({metric('stream_backfill_snapshot_read_row_count', node_filter_enabled=False, table_id_filter_enabled=True)}[$__rate_interval])
                                )
                                * on(table_id) group_left(table_name) (
                                  group({metric('table_info', node_filter_enabled=False)}) by (table_name, table_id)
                                )
                            """,
                            "table_name={{table_name}} table_id={{table_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_backfill_snapshot_read_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                        panels.target(
                            f"""
                                sum by (table_id) (
                                    rate({table_metric('stream_snapshot_backfill_consume_snapshot_row_count')}[$__rate_interval])
                                )
                                * on(table_id) group_left(table_name) (
                                  group({metric('table_info', node_filter_enabled=False)}) by (table_name, table_id)
                                )
                            """,
                            "table_name={{table_name}} table_id={{table_id}} consume snapshot"
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Backfill Upstream Throughput (rows/s)",
                    "Total number of rows that have been output from the backfill upstream by table",
                    [
                        panels.target(
                            f"""
                                sum by (table_id) (
                                  rate({metric('stream_backfill_upstream_output_row_count', node_filter_enabled=False, table_id_filter_enabled=True)}[$__rate_interval])
                                )
                                * on(table_id) group_left(table_name) (
                                  group({metric('table_info', node_filter_enabled=False)}) by (table_name, table_id)
                                )
                            """,
                            "table_name={{table_name}} table_id={{table_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_backfill_upstream_output_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Snapshot Backfill Lag",
                    "",
                    [
                        panels.target(
                            f"{metric('meta_snapshot_backfill_upstream_lag')} / (2^16) / 1000",
                            "lag @ {{table_id}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Cross-DB Change Log Expiry Headroom",
                    "How long until each cross-database changelog stream scan's last consumed epoch expires under the upstream table's subscription retention. Negative values mean the consumer has fallen behind retention and next_epoch may fail.",
                    [
                        panels.target(
                            cross_db_log_expiry_headroom,
                            "table {{table_id}}",
                        ),
                    ],
                ),
                panels.table_info(
                    "Backfill Fragment Progress",
                    "Backfill progress per fragment",
                    [
                        panels.table_target(
                            f"group({metric('backfill_fragment_progress')}) by (job_id, fragment_id, backfill_state_table_id, backfill_target_relation_id, backfill_target_relation_name, backfill_target_relation_type, backfill_type, backfill_epoch, upstream_type, backfill_progress)"
                        )
                    ],
                    [
                        "job_id",
                        "fragment_id",
                        "backfill_state_table_id",
                        "backfill_target_relation_id",
                        "backfill_target_relation_name",
                        "backfill_target_relation_type",
                        "backfill_type",
                        "backfill_epoch",
                        "upstream_type",
                        "backfill_progress",
                    ],
                ),
            ],
        )
    ]
