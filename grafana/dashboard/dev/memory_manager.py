from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Memory manager",
            [
                panels.subheader("LRU manager"),
                panels.timeseries_count(
                    "LRU manager loop count per sec",
                    "",
                    [
                        panels.target(
                            f"rate({metric('lru_runtime_loop_count')}[$__rate_interval])",
                            "",
                        ),
                    ],
                ),
                panels.timeseries(
                    "LRU manager eviction policy",
                    "",
                    [
                        panels.target(
                            f"{metric('lru_eviction_policy')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries(
                    "LRU manager sequence",
                    "",
                    [
                        panels.target(
                            f"{metric('lru_latest_sequence')}",
                            "",
                        ),
                        panels.target(
                            f"{metric('lru_watermark_sequence')}",
                            "",
                        ),
                    ],
                ),
                panels.subheader("Jemalloc"),
                panels.timeseries_memory(
                    "The allocated memory of jemalloc",
                    "",
                    [
                        panels.target(
                            f"{metric('jemalloc_allocated_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The active memory of jemalloc",
                    "",
                    [
                        panels.target(
                            f"{metric('jemalloc_active_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The resident memory of jemalloc",
                    "",
                    [
                        panels.target(
                            f"{metric('jemalloc_resident_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The metadata memory of jemalloc",
                    "",
                    [
                        panels.target(
                            f"{metric('jemalloc_metadata_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.subheader("JVM"),
                panels.timeseries_memory(
                    "The allocated memory of jvm",
                    "",
                    [
                        panels.target(
                            f"{metric('jvm_allocated_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The active memory of jvm",
                    "",
                    [
                        panels.target(
                            f"{metric('jvm_active_bytes')}",
                            "",
                        ),
                    ],
                ),
            ],
        ),
    ]
