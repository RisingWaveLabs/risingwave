from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    file_cache_hit_filter = 'op="lookup",extra="hit"'
    file_cache_miss_filter = 'op="lookup",extra="miss"'
    refill_ops_filter = 'type=~"meta|data",op!~"filtered|ignored"'
    inheritance_parent_lookup_filter = 'type="parent_meta"'
    inheritance_parent_lookup_hit_filter = 'type="parent_meta",op="hit"'
    inheritance_parent_lookup_miss_filter = 'type="parent_meta",op="miss"'
    unit_inheritance_filter = 'type="unit_inheritance"'
    unit_inheritance_hit_filter = 'type="unit_inheritance",op="hit"'
    unit_inheritance_miss_filter = 'type="unit_inheritance",op="miss"'
    block_refill_filter = 'type="block"'
    block_refill_success_filter = 'type="block",op="success"'
    block_refill_unfiltered_filter = 'type="block",op="unfiltered"'
    cache_hit_filter = 'op="hit"'
    cache_miss_filter = 'op="miss"'
    meta_miss_filter = "type='meta_miss'"
    meta_total_filter = "type='meta_total'"
    data_miss_filter = "type='data_miss'"
    data_total_filter = "type='data_total'"
    return [
        outer_panels.row_collapsed(
            "Hummock Tiered Cache",
            [
                panels.subheader("Cache"),
                panels.timeseries_ops(
                    "Per Table Cache Ops (filter by threshold data_miss > 50, meta_miss > 10)",
                    "Shows cache miss operations per table that exceed threshold rates. High data miss rates (>50 ops/sec) or meta miss rates (>10 ops/sec) may indicate insufficient cache capacity or poor cache effectiveness for specific tables.",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_sst_store_block_request_counts', data_miss_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, table_id, type) > 50",
                            "{{table_id}} @ {{type}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({table_metric('state_store_sst_store_block_request_counts', meta_miss_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, table_id, type) > 10",
                            "{{table_id}} @ {{type}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        )
                    ],
                ),
                panels.timeseries_percentage(
                    "Per Table Cache Miss Ratio (filter by threshold data_miss_ratio > 0.5, meta_miss_ratio > 0.1)",
                    "Shows cache miss ratios per table that exceed threshold ratios. High data miss ratios (>50%) or meta miss ratios (>10%) may indicate that the cache is not effectively serving requests for those tables.",
                    [
                        panels.target(
                            f"(sum(rate({table_metric('state_store_sst_store_block_request_counts', meta_miss_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)) / (sum(rate({table_metric('state_store_sst_store_block_request_counts', meta_total_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)) > 0.1",
                            "meta cache miss ratio - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"(sum(rate({table_metric('state_store_sst_store_block_request_counts', data_miss_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)) / (sum(rate({table_metric('state_store_sst_store_block_request_counts', data_total_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)) > 0.5",
                            "block cache miss ratio - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),

                panels.timeseries_ops(
                    "Hybrid Cache Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_hybrid_op_total')}[$__rate_interval])) by (name, op, {NODE_LABEL})",
                            "{{name}} - hybrid - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Hybrid Cache Op Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('foyer_hybrid_op_duration_bucket')}[$__rate_interval])) by (le, name, op, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{name}} - hybrid - {{op}} @ {{%s}}" % NODE_LABEL,
                            ),
                            [50, 90, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Hybrid Cache Hit Ratio",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_hybrid_op_total', cache_hit_filter)}[$__rate_interval])) by (name, {NODE_LABEL}) / (sum(rate({metric('foyer_hybrid_op_total', cache_hit_filter)}[$__rate_interval])) by (name, {NODE_LABEL}) + sum(rate({metric('foyer_hybrid_op_total', cache_miss_filter)}[$__rate_interval])) by (name, {NODE_LABEL}))",
                            "{{name}} - hybrid - hit ratio @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.subheader("Memory Cache"),
                panels.timeseries_ops(
                    "Memory Cache Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_memory_op_total')}[$__rate_interval])) by (name, op, {NODE_LABEL})",
                            "{{name}} - memory - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Memory Cache Size",
                    "",
                    [
                        panels.target(
                            f"sum({metric('foyer_memory_usage')}) by (name, {NODE_LABEL})",
                            "{{name}} - memory - size @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Memory Cache Hit Ratio",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_memory_op_total', cache_hit_filter)}[$__rate_interval])) by (name, {NODE_LABEL}) / (sum(rate({metric('foyer_memory_op_total', cache_hit_filter)}[$__rate_interval])) by (name, {NODE_LABEL}) + sum(rate({metric('foyer_memory_op_total', cache_miss_filter)}[$__rate_interval])) by (name, {NODE_LABEL}))",
                            "{{name}} - memory - hit ratio @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.subheader("Disk Cache"),
                panels.timeseries_ops(
                    "Disk Cache Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_storage_op_total')}[$__rate_interval])) by (name, op, {NODE_LABEL})",
                            "{{name}} - storage - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Disk Cache Inner Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_storage_inner_op_total')}[$__rate_interval])) by (name, op, {NODE_LABEL})",
                            "{{name}} - storage - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Disk Cache Op Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('foyer_storage_op_duration_bucket')}[$__rate_interval])) by (le, name, op, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{name}} - storage - {{op}} @ {{%s}}"
                                % NODE_LABEL,
                            ),
                            [50, 90, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Disk Cache Inner Op Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('foyer_storage_inner_op_duration_bucket')}[$__rate_interval])) by (le, name, op, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{name}} - storage - {{op}} @ {{%s}}"
                                % NODE_LABEL,
                            ),
                            [50, 90, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Disk Cache Hit Ratio",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_storage_op_total', cache_hit_filter)}[$__rate_interval])) by (name, {NODE_LABEL}) / (sum(rate({metric('foyer_storage_op_total', cache_hit_filter)}[$__rate_interval])) by (name, {NODE_LABEL}) + sum(rate({metric('foyer_storage_op_total', cache_miss_filter)}[$__rate_interval])) by (name, {NODE_LABEL}))",
                            "{{name}} - storage - hit ratio @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Disk Cache Block Engine Block Usage",
                    "",
                    [
                        panels.target(
                            f"sum({metric('foyer_storage_block_engine_block')}) by (name, type, {NODE_LABEL}) * on(name, {NODE_LABEL}) group_left() avg({metric('foyer_storage_block_engine_block_size_bytes')}) by (name, type, {NODE_LABEL})",
                            "{{name}} - {{type}} block - size @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Disk Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_storage_disk_io_total')}[$__rate_interval])) by (name, op, {NODE_LABEL})",
                            "{{name}} - disk - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Disk Op Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('foyer_storage_disk_io_duration_bucket')}[$__rate_interval])) by (le, name, op, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{name}} - disk - {{op}} @ {{%s}}" % NODE_LABEL,
                            ),
                            [50, 90, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Disk Op Throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_storage_disk_io_bytes_total')}[$__rate_interval])) by (name, op, {NODE_LABEL})",
                            "{{name}} - disk - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.subheader("Refill"),
                panels.timeseries_ops(
                    "Refill Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('refill_duration_count')}[$__rate_interval])) by (type, op, {NODE_LABEL})",
                            "{{type}} file cache refill - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('refill_total', refill_ops_filter)}[$__rate_interval])) by (type, op, {NODE_LABEL})",
                            "{{type}} file cache refill - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Refill Throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('refill_bytes')}[$__rate_interval])) by (type, op, {NODE_LABEL})",
                            "{{type}} file cache - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Refill Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('refill_duration_bucket')}[$__rate_interval])) by (le, type, op, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{type}} cache refill - {{op}} @ {{%s}}"
                                % NODE_LABEL,
                            ),
                            [50, 90, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Refill Queue Length",
                    "",
                    [
                        panels.target(
                            f"sum(refill_queue_total) by ({NODE_LABEL})",
                            "refill queue length @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Inheritance - Parent Meta Lookup Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('refill_total', inheritance_parent_lookup_filter)}[$__rate_interval])) by (op, {NODE_LABEL})",
                            "parent meta lookup {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Inheritance - Parent Meta Lookup Ratio",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('refill_total', inheritance_parent_lookup_hit_filter)}[$__rate_interval])) by ({NODE_LABEL}) / (sum(rate({metric('refill_total', inheritance_parent_lookup_hit_filter)}[$__rate_interval])) by ({NODE_LABEL}) + sum(rate({metric('refill_total', inheritance_parent_lookup_miss_filter)}[$__rate_interval])) by ({NODE_LABEL})) >= 0",
                            "parent meta lookup hit ratio @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Inheritance - Unit Inheritance Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('refill_total', unit_inheritance_filter)}[$__rate_interval])) by (op, {NODE_LABEL})",
                            "unit inheritance {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Inheritance - Unit Inheritance Ratio",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('refill_total', unit_inheritance_hit_filter)}[$__rate_interval])) by ({NODE_LABEL}) / (sum(rate({metric('refill_total', unit_inheritance_hit_filter)}[$__rate_interval])) by ({NODE_LABEL}) + sum(rate({metric('refill_total', unit_inheritance_miss_filter)}[$__rate_interval])) by ({NODE_LABEL})) >= 0",
                            "unit inheritance ratio @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Block Refill Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('refill_total', block_refill_filter)}[$__rate_interval])) by (op, {NODE_LABEL})",
                            "block refill {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Block Refill Ratio",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('refill_total', block_refill_success_filter)}[$__rate_interval])) by ({NODE_LABEL}) / sum(rate({metric('refill_total', block_refill_unfiltered_filter)}[$__rate_interval])) by ({NODE_LABEL}) >= 0",
                            "block refill ratio @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Recent Filter Size",
                    "Item numbers of the recent filter.",
                    [
                        panels.target(
                            f"sum({metric('recent_filter_items')}) by ({NODE_LABEL})",
                            "items @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Recent Filter Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('recent_filter_ops')}[$__rate_interval])) by (op, {NODE_LABEL})",
                            "recent filter {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Block Cache Efficiency",
                    "Histogram of the estimated hit ratio of a block while in the block cache.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"clamp_max(histogram_quantile({quantile}, sum(rate({metric('block_efficiency_histogram_bucket')}[$__rate_interval])) by (le,{COMPONENT_LABEL},{NODE_LABEL})), 1)",
                                f"block cache efficiency - p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [10, 25, 50, 75, 90, 100],
                        ),
                    ],
                ),
            ],
        )
    ]
