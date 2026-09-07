import logging
import os
import sys

from grafanalib.core import (
    Dashboard,
    GridPos,
    RowPanel,
    Target,
    Templating,
    Time,
    TimeSeries,
)

p = os.path.dirname(__file__)
sys.path.append(p)

from jsonmerge import merge

from dashboard.common import *
from dashboard.user import generate_panels

source_uid = os.environ.get(SOURCE_UID, "risedev-prometheus")
dashboard_uid = os.environ.get(DASHBOARD_UID, "Fcy3uV1nz")
dashboard_version = int(os.environ.get(DASHBOARD_VERSION, "0"))
datasource = {"type": "prometheus", "uid": f"{source_uid}"}
datasource_const = "datasource"
if dynamic_source_enabled:
    datasource = {"type": "prometheus", "uid": "${datasource}"}

panels = Panels(datasource)
logging.basicConfig(level=logging.WARN)


templating_list = []
if dynamic_source_enabled:
    templating_list.append(
        {
            "hide": 0,
            "includeAll": False,
            "multi": False,
            "name": f"{datasource_const}",
            "options": [],
            "query": "prometheus",
            "queryValue": "",
            "refresh": 2,
            "skipUrlSync": False,
            "type": "datasource",
        }
    )

if namespace_filter_enabled:
    namespace_json = {
        "definition": 'label_values(up{risingwave_name=~".+"}, namespace)',
        "description": "Kubernetes namespace.",
        "hide": 0,
        "includeAll": False,
        "label": "Namespace",
        "multi": False,
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

    cluster_json = {
        "current": {"selected": False, "text": "All", "value": "__all"},
        "definition": 'label_values(up{risingwave_name=~".+", namespace="$namespace"}, cluster)',
        "description": "Kubernetes cluster.",
        "hide": 2,
        "includeAll": False,
        "label": "Cluster",
        "multi": False,
        "name": "cluster",
        "options": [],
        "query": {
            "query": 'label_values(up{risingwave_name=~".+", namespace="$namespace"}, cluster)',
            "refId": "StandardVariableQuery",
        },
        "refresh": 2,
        "regex": "",
        "skipUrlSync": False,
        "sort": 0,
        "type": "query",
    }

    name_json = {
        "current": {"selected": False, "text": "risingwave", "value": "risingwave"},
        "definition": 'label_values(up{namespace="$namespace", risingwave_name=~".+"}, risingwave_name)',
        "hide": 0,
        "includeAll": False,
        "label": "RisingWave",
        "multi": False,
        "name": "instance",
        "options": [],
        "query": {
            "query": 'label_values(up{namespace="$namespace", risingwave_name=~".+"}, risingwave_name)',
            "refId": "StandardVariableQuery",
        },
        "refresh": 2,
        "regex": "",
        "skipUrlSync": False,
        "sort": 6,
        "type": "query",
    }
    if dynamic_source_enabled:
        cluster_json = merge(cluster_json, {"datasource": datasource})
        namespace_json = merge(namespace_json, {"datasource": datasource})
        name_json = merge(name_json, {"datasource": datasource})

    templating_list.append(namespace_json)
    templating_list.append(cluster_json)
    templating_list.append(name_json)


node_json = {
    "current": {"selected": False, "text": "All", "value": "__all"},
    "definition": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, {NODE_LABEL})",
    "description": "Reporting instance of the metric",
    "hide": 0,
    "includeAll": True,
    "label": f"{NODE_VARIABLE_LABEL}",
    "multi": True,
    "name": f"{NODE_VARIABLE}",
    "options": [],
    "query": {
        "query": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, {NODE_LABEL})",
        "refId": "StandardVariableQuery",
    },
    "refresh": 2,
    "regex": "",
    "skipUrlSync": False,
    "sort": 6,
    "type": "query",
}
if NODE_VARIABLE_CUSTOM_ALL_VALUE is not None:
    node_json["allValue"] = NODE_VARIABLE_CUSTOM_ALL_VALUE

job_json = {
    "current": {"selected": False, "text": "All", "value": "__all"},
    "definition": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, {COMPONENT_LABEL})",
    "description": "Reporting job of the metric",
    "hide": 0,
    "includeAll": True,
    "label": f"{COMPONENT_VARIABLE_LABEL}",
    "multi": True,
    "name": f"{COMPONENT_VARIABLE}",
    "options": [],
    "query": {
        "query": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, {COMPONENT_LABEL})",
        "refId": "StandardVariableQuery",
    },
    "refresh": 2,
    "regex": "",
    "skipUrlSync": False,
    "sort": 6,
    "type": "query",
}

if dynamic_source_enabled:
    node_json = merge(node_json, {"datasource": datasource})
    job_json = merge(job_json, {"datasource": datasource})

templating_list.append(node_json)
templating_list.append(job_json)
templating = Templating(templating_list)

dashboard = Dashboard(
    title="risingwave_dashboard",
    description="RisingWave Dashboard",
    tags=["risingwave"],
    timezone="browser",
    editable=True,
    uid=dashboard_uid,
    time=Time(start="now-30m", end="now"),
    sharedCrosshair=True,
    templating=templating,
    version=dashboard_version,
    refresh="",
    panels=generate_panels(panels),
).auto_panel_ids()
