from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Iceberg Compaction Metrics",
            [
                panels.subheader("Commit"),
                panels.timeseries_count(
                    "Iceberg Compaction Commit Count",
                    "iceberg compaction commit count",
                    [
                        panels.target(
                            f"sum({metric('iceberg_compaction_commit_counter')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),
                panels.subheader("IO"),
                panels.timeseries_count(
                    "Iceberg Compaction Input File Count",
                    "iceberg compaction input file counts",
                    [
                        panels.target(
                            f"sum({metric('iceberg_compaction_input_files_count')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_bytes(
                    "Iceberg Compaction Input Bytes",
                    "iceberg compaction input bytes",
                    [
                        panels.target(
                            f"sum({metric('iceberg_compaction_input_bytes_total')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_count(
                    "Iceberg Compaction Output File Count",
                    "iceberg compaction output file counts",
                    [
                        panels.target(
                            f"sum({metric('iceberg_compaction_output_files_count')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_bytes(
                    "Iceberg Compaction Output Bytes",
                    "iceberg compaction output bytes",
                    [
                        panels.target(
                            f"sum({metric('iceberg_compaction_output_bytes_total')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_latency_ms(
                    "Iceberg Compaction Commit Duration",
                    "iceberg compaction commit duration in milliseconds",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('iceberg_compaction_commit_duration_bucket')}[$__rate_interval])) by (le, catalog_name, table_ident))",
                                f"p{legend}" + " @ {{catalog_name}} {{table_ident}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),

                panels.timeseries_latency_ms(
                    "Iceberg Compaction Duration",
                    "iceberg compaction duration in milliseconds",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('iceberg_compaction_duration_bucket')}[$__rate_interval])) by (le, catalog_name, table_ident))",
                                f"p{legend}" + " @ {{catalog_name}} {{table_ident}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.subheader("OpenDAL FileIO"),
                panels.timeseries_ops(
                    "Iceberg FileIO Operation Rate",
                    "completed logical OpenDAL operations per second",
                    [
                        panels.target(
                            f"sum(rate({metric('opendal_operation_duration_seconds_count')}[$__rate_interval])) by (scheme, operation)",
                            "{{scheme}} {{operation}}",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Iceberg Object Store Request Rate",
                    "underlying HTTP requests per second; for S3 this is the closest approximation to request IOPS and includes multipart requests and retries",
                    [
                        panels.target(
                            f"sum(rate({metric('opendal_http_request_duration_seconds_count')}[$__rate_interval])) by (scheme, operation, service_operation)",
                            "{{scheme}} {{service_operation}} ({{operation}})",
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Iceberg FileIO Throughput",
                    "logical bytes processed by completed OpenDAL operations per second",
                    [
                        panels.target(
                            f"sum(rate({metric('opendal_operation_bytes_sum')}[$__rate_interval])) by (scheme, operation)",
                            "{{scheme}} {{operation}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Iceberg FileIO Operation Duration",
                    "end-to-end OpenDAL operation duration in seconds",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('opendal_operation_duration_seconds_bucket')}[$__rate_interval])) by (le, scheme, operation))",
                                "{{scheme}} {{operation}}" + f" p{legend}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Iceberg FileIO Error Rate",
                    "logical OpenDAL failures and underlying HTTP connection or status errors per second",
                    [
                        panels.target(
                            f"sum(rate({metric('opendal_operation_errors_total')}[$__rate_interval])) by (scheme, operation, error)",
                            "operation {{scheme}} {{operation}} {{error}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('opendal_http_connection_errors_total')}[$__rate_interval])) by (scheme, operation, service_operation)",
                            "connection {{scheme}} {{service_operation}} ({{operation}})",
                        ),
                        panels.target(
                            f"sum(rate({metric('opendal_http_status_errors_total')}[$__rate_interval])) by (scheme, operation, service_operation, status_code)",
                            "status {{scheme}} {{service_operation}} {{status_code}} ({{operation}})",
                        ),
                    ],
                ),
                panels.subheader("DataFusion"),
                panels.timeseries_count(
                    "Iceberg Compaction Execution Error Count",
                    "iceberg compaction execution error count",
                    [
                        panels.target(
                            f"sum({metric('iceberg_compaction_executor_error_counter')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_count(
                    "Iceberg Compaction Datafusion Record Processed Count",
                    "iceberg compaction datafusion record processed count",
                    [
                        panels.target(
                            f"sum({metric('iceberg_compaction_datafusion_records_processed_total')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_bytes(
                    "Iceberg Compaction Datafusion Bytes Processed",
                    "iceberg compaction datafusion bytes processed",
                    [
                        panels.target(
                            f"sum({metric('iceberg_compaction_datafusion_bytes_processed_total')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_latency_ms(
                    "Iceberg Compaction Datafusion Batch Fetch Duration",
                    "iceberg compaction datafusion batch fetch duration in seconds",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('iceberg_compaction_datafusion_batch_fetch_duration_bucket')}[$__rate_interval])) by (le, catalog_name, table_ident))",
                                f"p{legend}" + " @ {{catalog_name}} {{table_ident}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),

                panels.timeseries_latency_ms(
                    "Iceberg Compaction Datafusion Batch Write Duration",
                    "iceberg compaction datafusion batch write duration in seconds",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('iceberg_compaction_datafusion_batch_write_duration_bucket')}[$__rate_interval])) by (le, catalog_name, table_ident))",
                                f"p{legend}" + " @ {{catalog_name}} {{table_ident}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),

                panels.timeseries_count(
                    "Iceberg Compaction Datafusion Batch Row Count Distribution",
                    "iceberg compaction datafusion batch row count distribution",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('iceberg_compaction_datafusion_batch_row_count_dist_bucket')}[$__rate_interval])) by (le, catalog_name, table_ident))",
                                f"p{legend}" + " @ {{catalog_name}} {{table_ident}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),

                panels.timeseries_bytes(
                    "Iceberg Compaction Datafusion Batch Bytes Distribution",
                    "iceberg compaction datafusion batch bytes distribution",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('iceberg_compaction_datafusion_batch_bytes_dist_bucket')}[$__rate_interval])) by (le, catalog_name, table_ident))",
                                f"p{legend}" + " @ {{catalog_name}} {{table_ident}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),

                panels.subheader("Memory"),
                panels.timeseries_bytes(
                    "Iceberg Compaction Memory Budget",
                    "heap admission budget for running iceberg compaction plans on this compactor node",
                    [
                        panels.target(
                            f"{metric('storage_iceberg_compaction_memory_budget_bytes')}",
                            "memory budget",
                        ),
                    ],
                ),

                panels.timeseries_bytes(
                    "Iceberg Compaction Running Reservation",
                    "estimated heap peaks reserved for actively executing plans",
                    [
                        panels.target(
                            f"{metric('storage_iceberg_compaction_running_memory_reservation_bytes')}",
                            "running",
                        ),
                    ],
                ),

            ]
        )
    ]
