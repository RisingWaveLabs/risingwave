from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    successful_compaction_filter = "result='SUCCESS'"
    failed_compaction_filter = "result!='SUCCESS'"
    sublevel_filter = "level_index=~'cg.*l0.*'"
    level_sst_count_filter = "level_index=~'cg.*L.*'"
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Compaction",
            [
                panels.subheader("Highlights"),
                panels.timeseries_count(
                    "Compaction Failure Rate",
                    "The rate of compactions from one level to another level that have completed or failed",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_frequency', failed_compaction_filter)}[$__rate_interval])) by (compactor, group, task_type, result)",
                            "{{task_type}} - {{result}} - group-{{group}} @ {{compactor}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Success Rate",
                    "The rate of compactions from one level to another level that have completed or failed",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_frequency', successful_compaction_filter)}[$__rate_interval])) by (compactor, group, task_type, result)",
                            "{{task_type}} - {{result}} - group-{{group}} @ {{compactor}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Skip Rate",
                    "The rate of compactions from one level to another level that have been skipped.",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_skip_compact_frequency')}[$__rate_interval])) by (level, type)",
                            "{{level}}-{{type}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compactor Running Task Count",
                    "The number of compactions from one level to another level that are running.",
                    [
                        panels.target(
                            f"avg({metric('storage_compact_task_pending_num')}) by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "compactor_task_count - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"avg({metric('storage_compact_task_pending_parallelism')}) by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "compactor_task_pending_parallelism - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "LSM Compact Pending Bytes",
                    "bytes of Lsm tree needed to reach balance",
                    [
                        panels.target(
                            f"sum({metric('storage_compact_pending_bytes')}) by ({NODE_LABEL}, group)",
                            "compact pending bytes - {{group}} @ {{%s}} " % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "L0 Sub-level Count",
                    "The number of L0 sub-levels for each compaction group",
                    [
                        panels.target(
                            f"sum({metric('storage_level_sst_num', sublevel_filter)}) by ({NODE_LABEL}, level_index)",
                            "L{{level_index}}",
                        ),
                    ],
                ),

                panels.subheader("SSTables"),
                panels.timeseries_count(
                    "SSTable Count",
                    "The number of SSTables at each level",
                    [
                        panels.target(
                            f"sum({metric('storage_level_sst_num', level_sst_count_filter)}) by ({NODE_LABEL}, level_index)",
                            "L{{level_index}}",
                        ),
                    ],
                ),
                panels.timeseries_kilobytes(
                    "SSTable Size (KB)",
                    "The size(KB) of SSTables at each level",
                    [
                        panels.target(
                            f"sum({metric('storage_level_total_file_size')}) by ({NODE_LABEL}, level_index)",
                            "L{{level_index}}",
                        ),
                    ],
                ),
                panels.subheader("Tasks & Results"),
                panels.timeseries_bytesps(
                    "Commit Flush Bytes by Table",
                    "The number of bytes that have been written by commit epoch per second.",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_commit_write_throughput')}[$__rate_interval])) by (table_id)",
                            "write - {{table_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Task L0 Select Level Count",
                    "Avg l0 select_level_count of the compact task, and categorize it according to different cg, levels and task types",
                    [
                        panels.target(
                            f"sum by(le, group, type)(irate({metric('storage_l0_compact_level_count_sum')}[$__rate_interval]))  / sum by(le, group, type)(irate({metric('storage_l0_compact_level_count_count')}[$__rate_interval])) > 0",
                            "avg cg{{group}}@{{type}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Task File Count",
                    "Avg file count of the compact task, and categorize it according to different cg, levels and task types",
                    [
                        panels.target(
                            f"sum by(le, group, type)(irate({metric('storage_compact_task_file_count_sum')}[$__rate_interval]))  / sum by(le, group, type)(irate({metric('storage_compact_task_file_count_count')}[$__rate_interval])) > 0",
                            "avg cg{{group}}@{{type}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Compaction Task Size Distribution",
                    "The distribution of the compact task size triggered, including p90 and max. and categorize it according to different cg, levels and task types.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('storage_compact_task_size_bucket')}[$__rate_interval])) by (le, group, type))",
                                f"p{legend}" + " - cg{{group}}@{{type}}",
                            ),
                            [50, 90, "max"],
                        ),
                    ],
                ),

                panels.subheader("Performance"),
                panels.timeseries_latency(
                    "Compaction Duration",
                    "compact-task: The total time have been spent on compaction.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('compactor_compact_task_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}))",
                                f"compact-task p{legend}"
                                + " - {{%s}}" % COMPONENT_LABEL,
                            ),
                            [50, 90, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('compactor_compact_sst_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}))",
                                f"compact-key-range p{legend}"
                                + " - {{%s}}" % COMPONENT_LABEL,
                            ),
                            [90, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('compactor_get_table_id_total_time_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}))",
                                f"get-table-id p{legend}"
                                + " - {{%s}}" % COMPONENT_LABEL,
                            ),
                            [90, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('compactor_remote_read_time_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}))",
                                f"remote-io p{legend}" + " - {{%s}}" % COMPONENT_LABEL,
                            ),
                            [90, "max"],
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(rate({metric('refill_duration_bucket')}[$__rate_interval])) by (le, type, op, {COMPONENT_LABEL}))",
                            "refill_duration_p99 - {{type}} {{op}} - {{%s}}"
                            % COMPONENT_LABEL,
                        ),
                        panels.target(
                            f"sum by(le)(rate({metric('compactor_compact_task_duration_sum')}[$__rate_interval])) / sum by(le)(rate({metric('compactor_compact_task_duration_count')}[$__rate_interval])) > 0",
                            "compact-task avg",
                        ),
                        panels.target(
                            f"sum by({COMPONENT_LABEL})(rate({metric('compactor_compact_sst_duration_sum')}[$__rate_interval])) / sum by({COMPONENT_LABEL})(rate({metric('compactor_compact_sst_duration_count')}[$__rate_interval])) > 0",
                            "compact-key-range avg - {{%s}}" % COMPONENT_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Compaction Throughput",
                    "KBs read from next level during history compactions to next level",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_read_next')}[$__rate_interval])) by({COMPONENT_LABEL}) + sum(rate("
                            f"{metric('storage_level_compact_read_curr')}[$__rate_interval])) by({COMPONENT_LABEL})",
                            "read - {{%s}}" % COMPONENT_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_write')}[$__rate_interval])) by({COMPONENT_LABEL})",
                            "write - {{%s}}" % COMPONENT_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('compactor_write_build_l0_bytes')}[$__rate_interval]))by ({COMPONENT_LABEL})",
                            "flush - {{%s}}" % COMPONENT_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('compactor_fast_compact_bytes')}[$__rate_interval]))by ({COMPONENT_LABEL})",
                            "fast compact - {{%s}}" % COMPONENT_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Compaction Write Bytes(GiB)",
                    "The number of bytes that have been written by compaction."
                    "Flush refers to the process of compacting Memtables to SSTables at Level 0."
                    "Write refers to the process of compacting SSTables at one level to another level.",
                    [
                        panels.target(
                            f"sum({metric('storage_level_compact_write')}) by ({COMPONENT_LABEL})",
                            "write - {{%s}}" % COMPONENT_LABEL,
                        ),
                        panels.target(
                            f"sum({metric('compactor_write_build_l0_bytes')}) by ({COMPONENT_LABEL})",
                            "flush - {{%s}}" % COMPONENT_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Compaction Write Amplification",
                    "Write amplification is the amount of bytes written to the remote storage by compaction for each "
                    "one byte of flushed SSTable data. Write amplification is by definition higher than 1.0 because "
                    "we write each piece of data to L0, and then write it again to an SSTable, and then compaction "
                    "may read this piece of data and write it to a new SSTable, that's another write.",
                    [
                        panels.target(
                            f"sum({metric('storage_level_compact_write')}) / sum({metric('compactor_write_build_l0_bytes')}) > 0",
                            "write amplification",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compacting SSTable Count",
                    "The number of SSTables that is being compacted at each level",
                    [
                        panels.target(
                            f"{metric('storage_level_compact_cnt')}", "L{{level_index}}"
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compacting Task Count",
                    "num of compact_task",
                    [
                        panels.target(
                            f"{metric('storage_level_compact_task_cnt')}", "{{task}}"
                        ),
                    ],
                ),
                panels.subheader("Per-Level IO & LSM"),
                panels.timeseries_bytes_per_sec(
                    "KBs Read/Write by Level",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_read_next')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} read from next level",
                        ),
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_read_curr')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} read from current level",
                        ),
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_write')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} write to next level",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Count of SSTs Read/Write by Level",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('storage_level_compact_write_sstn')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} write to next level",
                        ),
                        panels.target(
                            f"sum(irate({metric('storage_level_compact_read_sstn_next')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} read from next level",
                        ),
                        panels.target(
                            f"sum(irate({metric('storage_level_compact_read_sstn_curr')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} read from current level",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Hummock SSTable Bloom Filter Size",
                    "For observing bloom_filter size, sstable file size, sstable block size etc.",
                    [
                        panels.target(
                            f"histogram_quantile(0.50, sum(rate({metric('compactor_sstable_bloom_filter_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "bloom_filter_size_p50 - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"histogram_quantile(0.90, sum(rate({metric('compactor_sstable_bloom_filter_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "bloom_filter_size_p90 - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Hummock SSTable File Size",
                    "For observing sstable file size",
                    [
                        panels.target(
                            f"histogram_quantile(0.50, sum(rate({metric('compactor_sstable_file_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "sstable_file_size_p50 - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"histogram_quantile(0.90, sum(rate({metric('compactor_sstable_file_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "sstable_file_size_p90 - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Hummock SSTable Block Size",
                    "For observing sstable block size",
                    [
                        panels.target(
                            f"histogram_quantile(0.50, sum(rate({metric('compactor_sstable_block_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "sstable_block_size_p50 - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"histogram_quantile(0.90, sum(rate({metric('compactor_sstable_block_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "sstable_block_size_p90 - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Hummock SSTable Avg Key/Value Size",
                    "For observing avg key and value count",
                    [
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_avg_key_size_sum')}[$__rate_interval]))  / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_avg_key_size_count')}[$__rate_interval])) > 0",
                            "avg_key_size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_avg_value_size_sum')}[$__rate_interval]))  / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_avg_value_size_count')}[$__rate_interval]))",
                            "avg_value_size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Hummock Remote Read Duration",
                    "Total time of operations which read from remote storage when enable prefetch",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_remote_read_time_per_task_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"remote-io p{legend}"
                                + " - {{table_id}} @ {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [90, "max"],
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Compactor Iter keys",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('compactor_iter_scan_key_counts')}[$__rate_interval])) by ({NODE_LABEL}, type)",
                            "iter keys flow - {{type}} @ {{%s}} " % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "LSM Level Compression Ratio",
                    "compression ratio of each level of the lsm tree",
                    [
                        panels.target(
                            f"sum({metric('storage_compact_level_compression_ratio')}) by ({NODE_LABEL}, group, level, algorithm)",
                            "lsm compression ratio - cg{{group}} @ L{{level}} - {{algorithm}} {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Group Count",
                    "The number of compaction groups",
                    [
                        panels.target(
                            f"{metric('storage_compaction_group_count')}",
                            "compaction group count",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Compaction Group Size",
                    "The size of compaction group",
                    [
                        panels.target(
                            f"sum({metric('storage_compaction_group_size')}) by (group)",
                            "compaction group size - cg{{group}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Group File Count",
                    "The file count of compaction group",
                    [
                        panels.target(
                            f"sum({metric('storage_compaction_group_file_count')}) by (group)",
                            "compaction group file count - cg{{group}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Compaction Group Throughput",
                    "The throughput of compaction group",
                    [
                        panels.target(
                            f"sum({metric('storage_compaction_group_throughput')}) by (group)",
                            "compaction group throughput - cg{{group}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Group Schedule",
                    "The times of move_state_table occurs",
                    [
                        panels.target(
                            f"sum({table_metric('storage_split_compaction_group_count')}) by (group)",
                            "split compaction group - left cg{{group}}",
                        ),
                        panels.target(
                            f"sum({table_metric('storage_merge_compaction_group_count')}) by (group)",
                            "merge compaction group - left cg{{group}}",
                        ),
                    ],
                ),
            ],
        )
    ]
