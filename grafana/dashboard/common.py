import os
from typing import Union

from grafanalib.core import *

# We use DASHBOARD_NAMESPACE_ENABLED env variable to indicate whether to add
# a filter for the namespace field in the prometheus metric.
NAMESPACE_FILTER_ENABLED = "DASHBOARD_NAMESPACE_FILTER_ENABLED"
# We use RISINGWAVE_NAME_FILTER_ENABLED env variable to indicate whether to add
# a filter for the namespace_filter field in the prometheus metric.
RISINGWAVE_NAME_FILTER_ENABLED = "DASHBOARD_RISINGWAVE_NAME_FILTER_ENABLED"
# We use DASHBOARD_SOURCE_UID env variable to pass custom source uid
SOURCE_UID = "DASHBOARD_SOURCE_UID"
# We use DASHBOARD_UID env variable to pass custom dashboard uid
DASHBOARD_UID = "DASHBOARD_UID"
# We use DASHBOARD_VERSION env variable to pass custom version
DASHBOARD_VERSION = "DASHBOARD_VERSION"
# We use DASHBOARD_VERSION env variable to indicate whether to use a variable as the datasource
DASHBOARD_DYNAMIC_SOURCE = "DASHBOARD_DYNAMIC_SOURCE"

namespace_filter_enabled = os.environ.get(NAMESPACE_FILTER_ENABLED, "") == "true"
if namespace_filter_enabled:
    print("Enable filter for namespace field in the generated prometheus query")
risingwave_name_filter_enabled = (
    os.environ.get(RISINGWAVE_NAME_FILTER_ENABLED, "") == "true"
)
if risingwave_name_filter_enabled:
    print("Enable filter for namespace_filter field in the generated prometheus query")
dynamic_source_enabled = os.environ.get(DASHBOARD_DYNAMIC_SOURCE, "") == "true"
if dynamic_source_enabled:
    print("Enable use the datasource variable as the dashboard datasource")

COMPONENT_LABEL = "job"
COMPONENT_VARIABLE_LABEL = "Job"
COMPONENT_VARIABLE = "job"
NODE_LABEL = "instance"
NODE_VARIABLE_LABEL = "Node"
NODE_VARIABLE = "node"
NODE_VARIABLE_CUSTOM_ALL_VALUE = None

# Use different labels for role and instance when namespace filter enabled. (Kubernetes)
if namespace_filter_enabled:
    COMPONENT_LABEL = "risingwave_component"
    COMPONENT_VARIABLE_LABEL = "Component"
    COMPONENT_VARIABLE = "component"
    NODE_LABEL = "pod"
    NODE_VARIABLE_LABEL = "Pod"
    NODE_VARIABLE = "pod"
    NODE_VARIABLE_CUSTOM_ALL_VALUE = ".+"

templating = Templating()
if namespace_filter_enabled:
    templating = Templating(
        list=[
            {
                "definition": 'label_values(up{risingwave_name=~".+"}, namespace)',
                "description": "Kubernetes namespace.",
                "hide": 0,
                "includeAll": False,
                "label": "Namespace",
                "multi": True,
                "name": "namespace",
                "options": [],
                "query": {
                    "query": 'label_values(up{risingwave_name=~".+"}, namespace)',
                    "refId": "StandardVariableQuery",
                },
                "refresh": 2,
                "regex": "",
                "skipUrlSync": False,
                "sort": 0,
                "type": "query",
            }
        ]
    )


class Layout:
    def __init__(self):
        self.x = 0
        self.y = 0
        self.w = 0
        self.h = 0

    def next_row(self):
        self.y += self.h
        self.x = 0
        self.w = 24
        self.h = 1
        (x, y) = (self.x, self.y)
        return GridPos(h=1, w=24, x=x, y=y)

    def next_half_width_graph(self):
        if self.x + self.w > 24 - 12:
            self.y += self.h
            self.x = 0
        else:
            self.x += self.w
        (x, y) = (self.x, self.y)
        self.h = 8
        self.w = 12
        return GridPos(h=8, w=12, x=x, y=y)

    def next_one_third_width_graph(self):
        if self.x + self.w > 24 - 8:
            self.y += self.h
            self.x = 0
        else:
            self.x += self.w
        (x, y) = (self.x, self.y)
        self.h = 8
        self.w = 8
        return GridPos(h=8, w=8, x=x, y=y)


class Panels:
    # Common options for timeseries panels
    common_options = {
        "fillOpacity": 10,
        "interval": "1s",
        "maxDataPoints": 1000,
        "legendDisplayMode": "table",
    }

    def __init__(self, datasource):
        self.layout = Layout()
        self.datasource = datasource

    def row(
        self,
        title,
    ):
        gridPos = self.layout.next_row()
        return RowPanel(title=title, gridPos=gridPos)

    def row_collapsed(self, title, panels):
        gridPos = self.layout.next_row()
        return RowPanel(title=title, gridPos=gridPos, collapsed=True, panels=panels)

    def target(self, expr, legendFormat, hide=False):
        return Target(
            expr=expr, legendFormat=legendFormat, datasource=self.datasource, hide=hide
        )

    def target_hidden(self, expr, legendFormat):
        return Target(
            expr=expr, legendFormat=legendFormat, datasource=self.datasource, hide=True
        )

    def table_target(self, expr, hide=False):
        return Target(
            expr=expr,
            datasource=self.datasource,
            hide=hide,
            instant=True,
            format="table",
        )

    def timeseries(self, title, description, targets):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            **self.common_options,
        )

    def timeseries_count(self, title, description, targets, legendCols=["mean"], unit=''):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit=unit,
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_percentage(self, title, description, targets, legendCols=["mean"]):
        # Percentage should fall into 0.0-1.0
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="percentunit",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_latency(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="s",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_latency_ms(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="ms",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_latency_ns(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="ns",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_actor_latency(
        self, title, description, targets, legendCols=["mean"]
    ):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="s",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_actor_latency_small(
        self, title, description, targets, legendCols=["mean"]
    ):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="s",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_query_per_sec(
        self, title, description, targets, legendCols=["mean"]
    ):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="Qps",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_bytes_per_sec(
        self, title, description, targets, legendCols=["mean"]
    ):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="Bps",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_bytes(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="bytes",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_row(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="row",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_ms(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_kilobytes(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="kbytes",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_dollar(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="$",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_ops(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="ops",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_actor_ops(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="ops",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_actor_ops_small(
        self, title, description, targets, legendCols=["mean"]
    ):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="ops",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_rowsps(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="rows/s",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_bytesps(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="MB/s",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_actor_rowsps(self, title, description, targets):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="rows/s",
            **self.common_options,
        )

    def timeseries_memory(self, title, description, targets):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="bytes",
            **self.common_options,
        )

    def timeseries_cpu(self, title, description, targets):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="percentunit",
            **self.common_options,
        )

    def timeseries_latency_small(self, title, description, targets):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="s",
            **self.common_options,
        )

    def timeseries_epoch(self, title, description, targets):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            **self.common_options,
        )

    def table_info(
        self,
        title,
        description,
        targets,
        columns,
        excludeByName=dict.fromkeys(["Time", "Value"], True),
        renameByName=None,
        unitByName=None,
    ):
        gridPos = self.layout.next_one_third_width_graph()
        column_indices = {column: index for index, column in enumerate(columns)}
        organize_options = {
            "indexByName": column_indices,
            "excludeByName": excludeByName,
        }
        if renameByName:
            organize_options["renameByName"] = renameByName
        transformations = [
            {
                "id": "organize",
                "options": organize_options,
            }
        ]
        overrides = []
        if unitByName:
            for name, unit_value in unitByName.items():
                overrides.append(
                    {
                        "matcher": {"id": "byName", "options": name},
                        "properties": [{"id": "unit", "value": unit_value}],
                    }
                )
        return Table(
            title=title,
            dataSource=self.datasource,
            description=description,
            targets=targets,
            gridPos=gridPos,
            showHeader=True,
            filterable=True,
            transformations=transformations,
            overrides=overrides,
        )

    def subheader(self, title="", content="", height=1):
        gridPos = self.layout.next_row()
        # Keep layout state consistent with the actual panel height, otherwise the next panel may overlap
        # when callers use a custom `height`.
        gridPos.h = height
        self.layout.h = height
        return Text(
            title=title,
            gridPos=gridPos,
            content=content,
            transparent=True,
        )

    def sub_panel(self):
        return Panels(self.datasource)


def metric(name, filter=None, node_filter_enabled=True, table_id_filter_enabled=False):
    filters = [filter] if filter else []
    if namespace_filter_enabled:
        filters.append('cluster=~"$cluster"')
        filters.append('namespace=~"$namespace"')
    if risingwave_name_filter_enabled:
        filters.append('risingwave_name=~"$instance"')
    if table_id_filter_enabled:
        # We use "%table|" instead of "%table" here to match empty string for the table_id filter
        filters.append('table_id=~"$table|"')
    if node_filter_enabled:
        filters.append(f'{COMPONENT_LABEL}=~"${COMPONENT_VARIABLE}"')
        filters.append(f'{NODE_LABEL}=~"${NODE_VARIABLE}"')
    if filters:
        return f"{name}{{{','.join(filters)}}}"
    else:
        return name


def table_metric(name, filter=None):
    return metric(name, filter, True, True)


def quantile(f, percentiles):
    quantile_map = {
        "10": ["0.1", "10"],
        "25": ["0.25", "25"],
        "50": ["0.5", "50"],
        "60": ["0.6", "60"],
        "75": ["0.75", "75"],
        "90": ["0.9", "90"],
        "99": ["0.99", "99"],
        "999": ["0.999", "999"],
        "100": ["1.0", "100"],
        "max": ["1.0", "max"],
    }
    return list(
        map(lambda p: f(quantile_map[str(p)][0], quantile_map[str(p)][1]), percentiles)
    )


def epoch_to_unix_millis(epoch_expr):
    # UNIX_RISINGWAVE_DATE_SEC
    return f"(1617235200000+({epoch_expr} != 0)/65536)"


def alert_when(expr: str) -> str:
    """Return 1 when expr > 0, and drop the series when expr <= 0."""
    return f"(({expr}) > bool 0) > 0"


def alert_threshold(expr: str, threshold: Union[int, float], comparator: str = ">=") -> str:
    """Return 1 when expr compares true against threshold, and drop the series otherwise."""
    return f"(({expr}) {comparator} bool {threshold}) > 0"
