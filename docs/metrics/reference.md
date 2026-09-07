# RisingWave Metrics Reference

This document is the operational reference for RisingWave's Prometheus metrics.
It describes how metrics are exposed, the labels they carry, and — in the later
phases — what each metric measures, how it is intended to be used, and (where
the project defines them) recommended threshold values for alerting.

The document is written for two audiences:

- **Application teams** consuming RisingWave who want to monitor their use
  cases, interpret numbers, and troubleshoot.
- **SRE / platform teams** building dashboards and alert rules (including in
  ELK or any other Prometheus-compatible stack).

Metrics that only matter for deep platform internals (e.g. per-level
compaction stats, per-cache-shard counters) are grouped into an appendix and
annotated tersely. The main narrative focuses on metrics that application
teams actually watch.

> **Version.** This reference is derived from the code in this repository at
> the date of generation. A complete flat inventory of every registered
> metric — name, type, labels, help string, and source file — is kept
> alongside this document at [`inventory.tsv`](./inventory.tsv).
> When you find a discrepancy, the TSV is the ground truth.

---

## 1. How RisingWave exposes metrics

Every RisingWave component runs an independent HTTP server that exposes
metrics in Prometheus text format on its own port. There is no centralised
metrics endpoint; each component must be scraped separately.

### 1.1 Endpoints and default ports

| Component        | Flag (+ alias)                                                   | Env var                        | Default address      |
|------------------|------------------------------------------------------------------|--------------------------------|----------------------|
| Compute node     | `--prometheus-listener-addr`                                     | `RW_PROMETHEUS_LISTENER_ADDR`  | `127.0.0.1:1222`     |
| Frontend         | `--prometheus-listener-addr`                                     | `RW_PROMETHEUS_LISTENER_ADDR`  | `127.0.0.1:2222`     |
| Compactor        | `--prometheus-listener-addr`                                     | `RW_PROMETHEUS_LISTENER_ADDR`  | `127.0.0.1:1260`     |
| Meta             | `--prometheus-listener-addr` (alias `--prometheus-host`)         | `RW_PROMETHEUS_HOST`           | disabled unless set  |

Each listener serves a single path:

- `GET /metrics` — all metrics in Prometheus text format.
- `GET /metrics?include=<name>&include=<name>` — filter to named metric families.
- `GET /metrics?exclude=<name>&exclude=<name>` — exclude named metric families.
  (`include` and `exclude` are mutually exclusive; passing both returns HTTP 400.)

You can set `include`/`exclude` through Prometheus scrape-config `params`.
Each value is matched as an exact metric family name (prefix wildcards are
not supported):

```yaml
scrape_configs:
  - job_name: compute-node
    static_configs:
      - targets: ['compute-0:1222', 'compute-1:1222']
    params:
      exclude: ['stream_actor_poll_duration', 'stream_actor_scheduled_duration']
```

Source: `src/common/common_service/src/metrics_manager.rs`.

### 1.2 Scrape targets and the `job` label

The Grafana dashboards shipped with this repository assume one Prometheus
job per component (`job=compute`, `job=frontend`, `job=meta`, `job=compactor`).
If you scrape the cluster with different `job` names, all queries built on
the `job=~"..."` selector have to be adjusted accordingly. In Kubernetes
deployments the community dashboards optionally use `risingwave_component` and
`pod` labels instead — see §2.1 below.

### 1.3 Metric naming

Metric names are stable across releases unless explicitly called out. Histograms
follow Prometheus conventions — a histogram `foo_bar_duration_seconds` produces
`foo_bar_duration_seconds_bucket{le="..."}`, `_sum`, and `_count` series.

Every metric defined in the code is captured in `inventory.tsv` with:

| Column        | Meaning                                                   |
|---------------|-----------------------------------------------------------|
| `metric_name` | Name as exposed on `/metrics`.                            |
| `type`        | Prometheus metric type — one of `counter`, `gauge`, `histogram`. (The Rust source uses richer concrete types like `IntCounterVec`, `LabelGuardedHistogramVec`, etc., but those are an internal distinction; on the wire `/metrics` only exposes the three Prometheus types, and the `labels` column already tells you whether the metric carries labels.) |
| `labels`      | Comma-separated list of label names (empty for scalar metrics). |
| `help`        | Human-readable description written into the `/metrics` response as `# HELP`. |
| `buckets`     | Populated only when histogram buckets are declared as a literal list. Most histograms use `exponential_buckets(base, factor, count)` and the value is computed at runtime. |
| `file`        | Source file the registration lives in (`grep -n metric_name <file>` from the repo root finds the exact line). Line numbers aren't stored in the TSV because they churn on every unrelated edit and would make CI diffs noisy. |

---

## 2. Common labels and dimensions

Most RisingWave metrics carry one or more of the following labels. Knowing
them up front removes a lot of repetition in the per-metric sections that
follow.

### 2.1 Cluster topology labels (added by the scraper or by k8s)

| Label                  | Source                        | Meaning |
|------------------------|-------------------------------|---------|
| `job`                  | Prometheus scrape config      | Component class — `compute`, `frontend`, `meta`, `compactor`. Dashboards split queries by this label. |
| `instance`             | Prometheus scrape config      | `host:port` of the scrape target. The node identity. |
| `risingwave_component` | Alternate component label used by the k8s dashboards (set when `DASHBOARD_NAMESPACE_FILTER_ENABLED=true` is used to generate the dashboard). Equivalent to `job`. |
| `pod`                  | k8s service discovery         | Pod name. Equivalent to `instance` in k8s deployments. |
| `namespace`            | k8s service discovery         | k8s namespace. Only present in k8s scrapes. |
| `risingwave_name`      | k8s service discovery (operator-set) | Distinguishes multiple RisingWave clusters sharing the same Prometheus. |

None of these come from the RisingWave code — they are applied by the
Prometheus scrape configuration or by the k8s service-discovery layer. Every
metric below inherits them.

### 2.2 Domain labels (added by RisingWave itself)

The table below lists the labels that appear most frequently in the 508
registered metrics, with their stable meaning. Counts are "number of metrics
that carry this label".

| Label                    | Count | What it identifies |
|--------------------------|------:|---------------------|
| `actor_id`               | 125   | A streaming actor — the unit of parallelism inside a streaming job. An actor runs on exactly one compute node at a time and belongs to exactly one fragment. |
| `fragment_id`            | 114   | A streaming fragment — a DAG node in the streaming plan. One fragment produces many actors (one per parallel unit). Shared across all actors of the same operator. |
| `table_id`               | 106   | Internal table identifier. A streaming job is backed by one or more internal state tables; each materialized view is itself a table. Join `table_id` to `table_name` via the `table_info` metric when you need human-readable names on a dashboard. |
| `id`                     |  87   | librdkafka client identifier on every `rdkafka_*` metric (86 of the 87 occurrences). Also used on `relation_info` as the streaming relation's catalog ID. |
| `client_id`              |  86   | librdkafka client name. Paired with `id` on `rdkafka_*` metrics. |
| `topic`                  |  41   | Kafka topic. Appears on the per-topic `rdkafka_topic_*` metrics. |
| `source_id`              |  38   | Catalog ID of a source. Most source metrics also carry `source_name` as a sibling label; some (mostly CDC internals — `stream_mysql_cdc_*`, `stream_pg_cdc_*`, `stream_sqlserver_cdc_*`, `source_kafka_high_watermark`, `source_latest_message_id`, `pg_cdc_*_lsn`) carry `source_id` **without** `source_name` (they may carry other labels like `partition`, `actor_id`, or `slot_name`, but not the name). When you need the name for those, resolve it from the system catalog `rw_catalog.rw_sources` rather than joining on another metric — there is no `source_info` metric. |
| `broker`                 |  37   | Kafka broker endpoint. Used by the per-broker `rdkafka_broker_*` metrics. |
| `partition`              |  32   | Kafka partition number. |
| `sink_id`                |  31   | Catalog ID of a sink. Join to `sink_name` via the `sink_info` metric. |
| `sink_name`              |  29   | Human-readable sink name. Attached directly on a subset of sink metrics; for others, join through `sink_info`. |
| `state`                  |  29   | Generic state label. Meaning is metric-specific — Kafka rdkafka per-broker / per-consumer-group state (`Up`, `Down`, `Stable`, …) on `rdkafka_*`; backup-job phase on `backup_job_latency`; clean / dirty on `sync_kv_log_store_state`. |
| `type`                   |  28   | Generic type discriminator. Meaning is metric-specific — `object_store_failure_count` / `object_store_operation_latency` / `object_store_operation_bytes` carry the object-storage operation kind (`upload`, `read`, `list`, `delete`, …; full set in §7.3); `state_store_iter_scan_key_counts` and similar key-count metrics carry key categories (`processed`, `skip_multi_version`, `skip_delete`, `total`); `refill_*` carry the tiered-cache refill kind; etc. Note that recovery-classification and error-classification are separate labels (`recovery_type`, `error_type`) — not `type`. |
| `source_name`            |  26   | Human-readable source name. Attached directly on most source metrics; for the few that carry only `source_id` (see the row above), resolve the name via `rw_catalog.rw_sources`. |
| `group`                  |  21   | Generic grouping label. Usually `compaction_group_id` or a storage-level group. |
| `table_name`             |  18   | Human-readable table name. |
| `connector`              |  16   | Connector kind (e.g. `kafka`, `pulsar`, `mysql-cdc`, `iceberg`, `postgres-cdc`). |
| `target`                 |  12   | Sink / log-store target identifier on the `sync_kv_log_store_*` family of metrics (buffer sizing, reader progress, storage writes). |
| `relation`               |  12   | Catalog name of a streaming relation (materialized view, table, index) — used on the relation-aggregated metrics. |
| `name`                   |  10   | Generic name label. UDF name, event name, or similar. |
| `level_index`            |   9   | Hummock LSM level identifier. Two value styles co-exist depending on the emitter: meta-side metrics use `cg<compaction_group_id>_L<level_idx>` (e.g. `cg2_L0`); compactor-side metrics use the numeric level (`0`, `1`, …). |
| `uri`                    |   8   | Remote endpoint URI. On `connection_*` metrics only. |
| `link`                   |   8   | UDF link identifier (process URL / embedded runtime tag). |
| `language`               |   8   | UDF language (`python`, `javascript`, `rust`, `wasm`, etc.). |
| `connection_type`        |   8   | gRPC connection class on `connection_*` metrics. |
| `database_id`            |   7   | Database identifier. Barrier-latency metrics are emitted per database because checkpointing is per-database. |
| `error_type`             |   5   | Error classification. Present on the three `user_*_error_cnt` metrics and a handful of others. |
| `side`                   |   5   | `left` / `right` on join-related metrics. |
| `shard_id`               |   5   | Kinesis shard identifier. |
| `iter_type`              |   6   | Storage iterator type on `state_store_iter_*` metrics. Values: `iter` (forward range scan) and `iter_log` (change-log scan). |

The remaining rarer labels (`op`, `level`, `upstream_fragment_id`, `slot_name`,
`status`, `top_n`, `event_type`, `uploader_stage`, `executor_name`, …) are
explained inline with the metrics that use them.

### 2.3 The info/join metrics

Several metrics exist purely to carry human-readable names alongside numeric
IDs, for dashboard joining:

| Info metric      | Labels                                      | Purpose |
|------------------|---------------------------------------------|---------|
| `table_info`     | `materialized_view_id`, `table_id`, `fragment_id`, `table_name`, `table_type`, `compaction_group_id` | Resolves `table_id` → `table_name` (and gives the owning fragment / compaction group / containing MV). Value is 1 while the table exists. |
| `sink_info`      | `actor_id`, `sink_id`, `sink_name`          | Resolves `sink_id` → `sink_name` (and the backing actor). There is no equivalent `source_info` metric — see the note on `source_id` in §2.2 for how to resolve source names. |
| `worker_num`     | `worker_type`                               | Current count of workers per type. `worker_type` values are the protobuf `WorkerType::as_str_name()` strings — `WORKER_TYPE_COMPUTE_NODE`, `WORKER_TYPE_FRONTEND`, `WORKER_TYPE_META`, `WORKER_TYPE_COMPACTOR`. Useful for "at least N of each" alerts. |
| `actor_info`     | `actor_id`, `fragment_id`, `compute_node`   | Resolves `actor_id` → location. |

Dashboards routinely use the PromQL pattern:

```promql
sum(rate(stream_sink_input_row_count[$__rate_interval])) by (sink_id)
  * on (sink_id) group_left(sink_name) group(sink_info) by (sink_id, sink_name)
```

— multiplying the throughput series by the `sink_info` series is a zero-cost
way to add the `sink_name` label to every point, so the legend reads
"sink 42 orders_out" instead of just "sink 42".

---

## 3. Monitoring posture (Appendix A): alert thresholds shipped with RisingWave

The Grafana dashboards in this repository ship two sets of alert rules:

- **Dev dashboard** — `grafana/dashboard/dev/cluster_alerts.py`. The
  authoritative set, used on the detailed dev dashboard. It splits alerts by
  sub-type (e.g. two separate "Lagging Version" signals, one for checkpoint
  lag, one for pin lag) and adds resource-side alerts (CPU saturation,
  unexpected pod termination).
- **User dashboard** — the `Alerts` panel in `grafana/dashboard/user/overview.py`.
  A simpler, older superset of streaming + storage alerts. Its thresholds
  agree with the dev dashboard except where noted; its expressions are
  sometimes coarser (combined alerts, cumulative counts instead of rates).

The tables below list each alert, give the PromQL as defined in the dev
dashboard (the one you should adopt for ELK / Alertmanager rules), the
threshold, and — where relevant — how the user dashboard differs.

In `cluster_alerts.py` the expressions are wrapped by two helper functions:

- `alert_when(expr)` → `((expr) > bool 0) > 0` — fires if `expr > 0`.
- `alert_threshold(expr, T, cmp=">=")` → `((expr) cmp bool T) > 0` — fires if
  `expr cmp T` holds.

Both evaluate to `1` when firing and are dropped otherwise, which means you
can `sum`, `count`, or alert on `last_over_time` without additional
manipulation.

Every PromQL below uses Grafana's `$__rate_interval` for `rate(...)` windows;
for a static alert rule, replace it with an explicit interval (`5m` is the
project's de-facto default on the dashboard panels).

### 3.1 Streaming alerts

| Alert              | PromQL signal (dev dashboard)                                                                 | Threshold  | What it means / first action |
|--------------------|-----------------------------------------------------------------------------------------------|------------|-------------------------------|
| Recovery Triggered | `sum(rate(recovery_latency_count[$__rate_interval])) by (recovery_type) + sum(rate(recovery_failure_cnt[$__rate_interval])) by (recovery_type)` wrapped in `alert_when` | `> 0`      | The cluster entered recovery. Inspect `Cluster Errors` and the component logs on the affected nodes for the root cause. **Note:** PromQL `+` is a vector match, not a logical or — for a given `recovery_type`, both sides need a series for the result to exist. A fresh recovery type that has only `recovery_latency_count` samples (e.g. the first-ever `global` recovery, with no failures yet) won't produce a result here. If you want strict "any recovery activity ever fires it", use `sum(rate(recovery_latency_count[$__rate_interval])) by (recovery_type) or sum(rate(recovery_failure_cnt[$__rate_interval])) by (recovery_type)` instead. |
| Too Many Barriers  | `all_barrier_nums` (per `database_id`)                                                        | `>= 200`   | The streaming graph has accumulated 200+ uncommitted barriers. The graph is either stuck or under load. Check `Streaming Backfill` (is backfill saturating the graph?), `Storage Alerts: Write Stall`, `Cluster Resource Alerts`, barrier latency, and the top-CPU / high-busy-rate streaming relations. |

Source: `grafana/dashboard/dev/cluster_alerts.py`.

User-dashboard drift (`grafana/dashboard/user/overview.py:91-93`): "Recovery
Triggered" uses

```
sum(rate(recovery_latency_count[$__rate_interval])) > bool 0 + sum(recovery_failure_cnt) > bool 0
```

which, given that `+` binds tighter than `>` in PromQL, parses as
`(A > bool (0 + B)) > bool 0` — a chained comparison between the recovery
rate `A` and the cumulative failure counter `B`. This is almost certainly
not the intended semantics (once any failure ever happens, the cumulative
counter dominates any rate and the signal stops firing). Use the
dev-dashboard expression for alerting.

### 3.2 Cluster resource alerts (dev dashboard only)

| Alert                        | PromQL signal                                                                                                                          | Threshold | What it means / first action |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|-----------|-------------------------------|
| CPU Saturation (avg/core)    | `sum(rate(process_cpu_seconds_total[$__rate_interval])) by (job, instance) / avg(process_cpu_core_num) by (job, instance)`             | `> 0.9`   | A RisingWave process is using more than 90 % of its available CPU cores on average. Scale the affected component; check `Streaming Relation Metrics: CPU Usage Per Streaming Job` to find the hottest jobs. |
| CPU Saturation (k8s limit)   | `sum(rate(container_cpu_usage_seconds_total{namespace=~"$namespace",container=~"$component",pod=~"$pod"}[$__rate_interval])) by (namespace, pod) / sum by (namespace, pod) (topk(1, kube_pod_container_resource_limits{namespace=~"$namespace",pod=~"$pod",container=~"$component", resource="cpu"}) by (namespace, pod))` | `> 0.9`   | A pod is using >90 % of its k8s CPU limit. Same remediation as above, plus consider raising the container limit if the cluster has headroom. |
| Unexpected Termination       | `changes((kube_pod_container_status_last_terminated_timestamp{cluster=~"$cluster",namespace=~"$namespace",pod=~"$pod"} * on(namespace,pod,container) group_left(reason) kube_pod_container_status_last_terminated_reason{cluster=~"$cluster",namespace=~"$namespace",pod=~"$pod",reason!~"Completed"})[$__rate_interval:])` | `> 0` | A pod terminated with a non-`Completed` reason (OOMKilled, Error, …). Cross-reference `Cluster Errors: Termination reasons` to see why. |

Source: `grafana/dashboard/dev/cluster_alerts.py`.
Note: the two k8s-dependent signals require `kube-state-metrics` and
`cAdvisor`; if you scrape RisingWave outside kubernetes, drop those rules.
These alerts are not present on the user dashboard.
The `$namespace`, `$component`, `$pod`, `$cluster` placeholders are Grafana
template variables. For standalone Alertmanager / ELK rules, replace them
with concrete regexes or drop the label selectors entirely (e.g. to alert
on any namespace/pod, use `container_cpu_usage_seconds_total` with no
label selectors).

### 3.3 Storage alerts

| Alert                          | PromQL signal (dev dashboard)                                                                 | Threshold                    | What it means / first action |
|--------------------------------|-----------------------------------------------------------------------------------------------|------------------------------|------------------------------|
| Lagging Version (checkpoint)   | `storage_current_version_id - storage_checkpoint_version_id`                                  | `>= 1000`                    | Hummock checkpointing is falling >1000 versions behind the latest. Look at `Hummock Manager` to see whether checkpoint worker is running and why checkpoints are delayed. |
| Lagging Version (pinned)       | `storage_current_version_id - storage_min_pinned_version_id`                                  | `>= 1000`                    | A reader is pinning an old version and blocking vacuum. Identify which client holds the old pin in `Hummock Manager: Min Pinned Version`. |
| Lagging Compaction             | `sum(label_replace(storage_level_total_file_size, 'L0', 'L0', 'level_index', '.*_L0') unless storage_level_total_file_size) by (L0)` | `>= 52,428,800` (≈ 50 GiB; `storage_level_total_file_size` is in **kilobytes**, see §10.5 / §10.6) | L0 data has grown past ~50 GiB — compactor is behind. Investigate: compactor failing (`Compaction Success Count`)? Throughput too low (`Commit Flush Bytes`, `Compaction Throughput`)? Scale or restart compactor. |
| Lagging Vacuum                 | `min_over_time(storage_stale_object_count[5m])`                                               | `>= 200`                     | Stale objects are piling up. Compactor may be behind or vacuum is disabled. See `Compaction` dashboard. |
| Abnormal Uploading Memory Usage| `state_store_uploading_memory_usage_ratio{job="compute"}`                                     | `>= 0.8`                     | Compute node is using ≥80 % of the memory budget for pending uploads. Spill is imminent. Check sink / materialization throughput and compactor lag. |
| Write Stall                    | `storage_write_stop_compaction_groups`                                                        | `> 0`                        | A compaction group has stopped accepting foreground writes because compaction cannot keep up. See the Compaction dashboard for the specific group; scale compactor. The `compaction_group_id` label identifies the group. |
| Abnormal Version Size          | `storage_version_size`                                                                        | `>= 314,572,800` (300 MiB)   | The Hummock version metadata has ballooned. Investigate `Hummock Manager` — typically caused by many small SSTs or pinned old versions. |
| Abnormal Delta Log Number      | `storage_delta_log_count`                                                                     | `>= 5000`                    | Too many delta logs pending GC. Often correlates with a spike of trivial-move compaction tasks. See `Hummock Manager` + `Compaction`. |
| Abnormal Pending Event Number  | `sum(state_store_event_handler_pending_event) by (instance)`                                  | `>= 10,000,000`              | The hummock event handler is backlogged. Check `Hummock Write: Event handle latency` — if it exceeds barrier latency, storage writes are the bottleneck. |
| Abnormal Object Storage Failure| `sum(rate(object_store_failure_count[$__rate_interval])) by (type)` wrapped in `alert_when`   | `> 0` (any failure)          | Object-storage operations are failing. `type` identifies the operation (`upload`, `read`, `list`, `delete`, …; full set in §7.3). See `Object Storage` dashboard. |

Source: `grafana/dashboard/dev/cluster_alerts.py`.

User-dashboard drift (`grafana/dashboard/user/overview.py`):

- **Lagging Version** is a single combined alert that fires when *either*
  `(current − checkpoint) >= 1000` or `(current − min_pinned) >= 1000`. The
  dev dashboard splits these into two distinct signals so you can tell the
  two failure modes apart — prefer the split form for alerting.
- **Abnormal Object Storage Failure** uses the cumulative counter
  `object_store_failure_count >= 50` instead of the rate-based any-failure
  rule. Cumulative thresholds quietly break after a restart (the counter
  resets) and need to be manually re-armed; prefer the rate-based dev form.
- **Lagging Compaction, Lagging Vacuum, Abnormal Uploading Memory Usage,
  Write Stall, Abnormal Version Size, Abnormal Delta Log Number, Abnormal
  Pending Event Number** — expressions and thresholds match the dev
  dashboard exactly.

---

## 4. Throughput

These are the top-line "is data flowing" metrics. They appear on the user
dashboard's Overview and Streaming sections and are what application teams
check first.

Most of these are counters (`counter` / `counter`)
queried with `rate(...[$__rate_interval])` for per-second throughput; the
exceptions are the gauges `source_partition_eof_offset`,
`stream_sink_chunk_buffer_size`, and `stream_mview_current_epoch`, which are
read directly. The metric type is called out in each row so you know which
is which. For a precise short-window rate on a counter, use
`irate(...[1m])`.

### 4.1 Source input (from the external system to the connector)

| Metric                                | Labels                                                  | What it measures |
|---------------------------------------|---------------------------------------------------------|------------------|
| `source_partition_input_count`        | `actor_id`, `source_id`, `partition`, `source_name`, `fragment_id` | Rows read by the source connector from a specific partition. |
| `source_partition_input_bytes`        | `actor_id`, `source_id`, `partition`, `source_name`, `fragment_id` | Bytes read from the same partition. |
| `source_partition_eof_count`          | `source_id`, `partition`, `source_name`, `fragment_id`  | EOF events received from a partition. For bounded sources this ticks up when the partition finishes. |
| `source_partition_eof_offset`         | `source_id`, `partition`, `source_name`, `fragment_id`  | Offset at which the EOF was observed. Gauge. |
| `file_source_input_row_count`         | `source_id`, `source_name`, `actor_id`, `fragment_id`   | Rows read by a **file source** (e.g. S3 / GCS ingest). File sources don't have partitions in the Kafka sense so they get a separate counter. |

**Per-source throughput in rows/s (user dashboard):**
```promql
sum(rate(stream_source_output_rows_counts[$__rate_interval])) by (source_id, source_name, fragment_id)
```

**Per-source throughput in MB/s:**
```promql
sum(rate(source_partition_input_bytes[$__rate_interval])) by (source_id, source_name, fragment_id) / (1000*1000)
```

### 4.2 Source output (from the connector into the streaming graph)

| Metric                                  | Labels                                                  | What it measures |
|-----------------------------------------|---------------------------------------------------------|------------------|
| `stream_source_output_rows_counts`      | `source_id`, `source_name`, `actor_id`, `fragment_id`   | Rows emitted by the source executor into the streaming graph. Equals `source_partition_input_count` summed over partitions when there is no row-level filtering at the source. |
| `stream_source_split_change_event_count`| `source_id`, `source_name`, `actor_id`, `fragment_id`   | Split reassignment / rebalance events received by the source executor. A steady non-zero rate usually indicates a Kafka consumer-group rebalance storm. |
| `stream_source_backfill_rows_counts`    | `source_id`, `source_name`, `actor_id`, `fragment_id`   | Rows read specifically during source-backfill (e.g. when a source-backed materialised view is still catching up on its initial snapshot). |

### 4.3 Sink output (from the streaming graph to the external system)

| Metric                           | Labels                                  | What it measures |
|----------------------------------|-----------------------------------------|------------------|
| `stream_sink_input_row_count`    | `sink_id`, `actor_id`, `fragment_id`    | Rows handed to the sink executor for delivery. |
| `stream_sink_input_bytes`        | `sink_id`, `actor_id`, `fragment_id`    | Bytes handed to the sink executor. |
| `stream_sink_chunk_buffer_size`  | `sink_id`, `actor_id`, `fragment_id`    | Estimated byte size of chunks currently buffered inside the barrier window, waiting to be flushed to the sink (sum of each chunk's `estimated_heap_size()` in the sink executor's chunk buffer). Gauge. Sustained growth indicates sink back-pressure. |

Because these carry only `sink_id` (not `sink_name`), dashboard queries join
against `sink_info` to add the name — see the PromQL pattern in §2.3.

**Per-sink throughput in rows/s (user dashboard):**
```promql
sum(rate(stream_sink_input_row_count[$__rate_interval])) by (sink_id)
  * on (sink_id) group_left(sink_name) group(sink_info) by (sink_id, sink_name)
```

### 4.4 Materialized view and internal-table writes

| Metric                         | Labels                              | What it measures |
|--------------------------------|-------------------------------------|------------------|
| `stream_mview_input_row_count` | `actor_id`, `table_id`, `fragment_id`| Rows handed to the `MaterializeExecutor` — i.e. the rows written into a materialized view or a `CREATE TABLE`-backed table, from `src/stream/src/executor/mview/materialize.rs`. It does **not** cover internal state-store writes performed by other executors (those have their own `state_store_*_write_*` metrics — see Phase 4). |
| `stream_mview_current_epoch`   | `actor_id`, `table_id`, `fragment_id`| Epoch currently being applied by the materialize executor. Gauge. Used for the freshness / lag metric in §5.3. |

### 4.5 Backfill (MV-on-MV, CDC, and snapshot backfill)

RisingWave has three different backfill paths. The first two (MV-on-MV and
CDC) each emit a *pair* of counters — rows read from the historical
snapshot vs. rows forwarded from the live upstream. The newer snapshot
backfill path consolidates everything under a single counter discriminated
by a `stage` label.

| Metric                                                | Labels                            | Backfill kind |
|-------------------------------------------------------|-----------------------------------|---------------|
| `stream_backfill_snapshot_read_row_count`             | `table_id`, `actor_id`            | MV-on-MV: rows read from the existing materialised view's snapshot. |
| `stream_backfill_upstream_output_row_count`           | `table_id`, `actor_id`            | MV-on-MV: rows coming from the live upstream during backfill. |
| `stream_cdc_backfill_snapshot_read_row_count`         | `table_id`, `actor_id`            | CDC: rows read from the source's initial snapshot. |
| `stream_cdc_backfill_upstream_output_row_count`       | `table_id`, `actor_id`            | CDC: live binlog / WAL rows during backfill. |
| `stream_snapshot_backfill_consume_snapshot_row_count` | `table_id`, `actor_id`, `stage`   | Snapshot-backfill (the newer path that reads from object store directly). `stage` takes one of three values — `consuming_snapshot` (Phase 1: consume the frozen snapshot), `consuming_log_store` (Phase 2: catch up via the kv log store), `consume_upstream` (Phase 3: switch to the live upstream feed). See `src/stream/src/executor/backfill/snapshot_backfill/executor.rs`. |

Monitoring both counters of an MV-on-MV or CDC backfill tells you whether
backfill is *running* (snapshot rate > 0) and whether it is *keeping up
with the upstream* (upstream rate ≈ upstream throughput). For snapshot
backfill, plot the one counter split by `stage` — you should see
`consuming_snapshot` spike, then settle, then `consuming_log_store` /
`consume_upstream` take over.

The user dashboard's "Backfill Throughput" panel joins these keyed on
`table_id` against `table_info` so the legend shows table names. If a table
is visible in `table_info` but none of these counters increment, backfill is
either done or stuck — cross-check the streaming graph's backpressure
metrics (§6).

---

## 5. Streaming freshness

Freshness metrics tell you **when data produced "now" will be visible in
downstream materialised views and sinks**. They are the most important
operational metrics after "is it running at all".

### 5.1 Barrier latency — the canonical freshness metric

A barrier is injected at every checkpoint interval. Its life-cycle inside
meta is:

1. **queued** (on meta's scheduler)
2. **dispatched** to compute nodes
3. **processed by every actor** and ACKed back by every compute node
4. **committed** (Hummock commit)

RisingWave tracks each stage with its own metric:

- `meta_barrier_send_duration_seconds` measures **step 1**: the time a
  barrier spent sitting on the scheduler queue waiting to be dispatched.
- `meta_barrier_duration_seconds` measures **steps 2 + 3**: from when the
  collector enqueued the dispatched barrier to when all compute nodes have
  ACKed completion. Observed inside `PartialGraphRunningState::barrier_collected`
  at `src/meta/src/barrier/partial_graph.rs:137`.
- `meta_barrier_wait_commit_duration_seconds` measures **step 4**: the wait
  for Hummock to commit after the barrier has been collected.

The "time until data from after this barrier is visible downstream" is the
sum of steps 2+3+4 (everything after dispatch). In healthy clusters the
dominant term is `meta_barrier_duration_seconds`; `*_wait_commit_*` spikes
when Hummock write is the bottleneck; `*_send_*` spikes when meta's
scheduler is overloaded or blocked.

| Metric                                        | Type                     | Labels         | Purpose |
|-----------------------------------------------|--------------------------|----------------|---------|
| `meta_barrier_duration_seconds`               | histogram | `database_id`  | Barrier collection latency: enqueued by meta → collected (all compute nodes have ACKed). The main barrier-latency metric to alert on. |
| `meta_barrier_send_duration_seconds`          | histogram | `database_id`  | Time a barrier sat on meta's scheduler queue waiting to be dispatched — measured from `push_back` at `src/meta/src/barrier/schedule.rs:211` to `pop_front` at `:587`. High values mean meta's checkpoint manager was busy with prior barriers or was blocked, **before** the graph saw this one. |
| `meta_barrier_wait_commit_duration_seconds`   | histogram                | —              | Time spent waiting for storage to commit after the graph finished processing the barrier. High values point at Hummock. |
| `stream_barrier_inflight_duration_seconds`    | histogram                | —              | Per-compute-node: time from when this node received the barrier until all its actors had collected it. The graph-wide barrier latency is the **max** of this across nodes (see the field-level comment in `src/stream/src/executor/monitor/streaming_stats.rs:161-163`). |
| `stream_barrier_sync_storage_duration_seconds`| histogram                | —              | Time spent flushing to storage as part of the barrier sync step, per compute node. |
| `meta_barrier_interval_by_database`           | gauge                 | `database_id`  | The currently-configured barrier interval per database (milliseconds). Gauge — not a histogram. Useful to confirm the system is running at the interval you expect. |

The five latency rows are Prometheus histograms — query with
`histogram_quantile(0.99, sum(rate(<metric>_bucket[$__rate_interval])) by (le, …))`.
`meta_barrier_interval_by_database` is a plain gauge and is read directly.

**Barrier collection latency p50 / p99, per database (user dashboard's
"Barrier Latency" panel):**
```promql
histogram_quantile(0.5,  sum(rate(meta_barrier_duration_seconds_bucket[$__rate_interval])) by (le, database_id))
histogram_quantile(0.99, sum(rate(meta_barrier_duration_seconds_bucket[$__rate_interval])) by (le, database_id))
```

This is what the "Barrier Latency" panel on the user dashboard plots and is
the right single number to monitor. It measures steps 2+3 only (dispatch →
all nodes ACKed). To reason about end-to-end freshness, plot
`meta_barrier_wait_commit_duration_seconds_bucket` (post-collection commit
wait) and `meta_barrier_send_duration_seconds_bucket` (pre-dispatch queue
wait) as their own quantile series on the same panel — one
`histogram_quantile(...)` line per stage. Prometheus histograms cannot be
summed into a single end-to-end distribution, so read each stage separately
and watch for the one that spikes.

**Interpretation thresholds** (no hard alert is shipped — use your own
SLO-derived thresholds):

- **< configured barrier interval** (typically 1 s): healthy.
- **1–10 × barrier interval**: the streaming graph is building up back-pressure;
  check the backpressure metrics in §6 and CPU saturation.
- **≥ 10 × barrier interval, rising**: the graph is stuck. Expect the "Too
  Many Barriers" alert (§3.1) to fire shortly afterwards (threshold 200).

### 5.2 Uncommitted barrier counters

| Metric                         | Type                      | Labels         | Meaning |
|--------------------------------|---------------------------|----------------|---------|
| `all_barrier_nums`             | gauge   | `database_id`  | Total number of barriers currently uncommitted. Fires "Too Many Barriers" (§3.1) at 200. |
| `in_flight_barrier_nums`       | gauge   | `database_id`  | Subset of the above currently traversing the graph (i.e. dispatched by meta but not yet fully synced). |
| `last_committed_barrier_time`  | gauge               | `database_id`  | UNIX epoch seconds of the most recently committed barrier. `time() - last_committed_barrier_time` gives "seconds since the last successful commit" — a useful watchdog when the histogram buckets max out. |
| `stream_barrier_manager_progress` | counter             | —              | Per-node counter that increments by 1 each time an in-flight barrier reaches the `AllCollected` state on this node — i.e. all the local actors have processed it. `rate(...[$__rate_interval])` is the per-second barrier-collection rate on this node; a flat line means the node is stuck on the current barrier. |

### 5.3 MV and sink lag (epoch-based)

The streaming graph's epochs encode wall-clock time in their upper bits, so
you can compute freshness directly from a materialize/sink executor's current
epoch without needing any external clock alignment:

```promql
# Materialized view lag in seconds
max(
  timestamp(stream_mview_current_epoch)
  - (1617235200000 + (stream_mview_current_epoch != 0) / 65536) / 1000
) by (table_id)
  * on(table_id) group_left(table_name) group(table_info) by (table_id, table_name)
```

The `(stream_mview_current_epoch != 0)` term is not a boolean — it is
PromQL's filter form, which returns the original metric value when it is
non-zero and drops the sample otherwise. So this evaluates to
`stream_mview_current_epoch / 65536` (milliseconds since the RisingWave
epoch origin 2021-04-01) while also filtering out the unhealthy "no epoch
seen" state. Dividing by 1000 converts to seconds and `timestamp(...)`
supplies the scrape wall-clock in seconds for the subtraction. The helper
lives in `grafana/dashboard/common.py` as `epoch_to_unix_millis`.

The sink counterpart uses `log_store_latest_read_epoch` (how far downstream
delivery has reached) and `log_store_latest_write_epoch` (how far the
in-memory log-store has been written):

| Metric                       | Labels                                 | Interpretation |
|------------------------------|----------------------------------------|----------------|
| `stream_mview_current_epoch` | `actor_id`, `table_id`, `fragment_id`  | Current epoch being applied by the MV executor — subtract from wall clock for lag. |
| `log_store_latest_write_epoch` | `actor_id`, `sink_id`, `sink_name`   | Epoch most recently handed to the sink's log store — enqueue lag. |
| `log_store_latest_read_epoch`  | `actor_id`, `connector`, `sink_id`, `sink_name` | Epoch most recently **read out of the log store** by the sink reader. Updated in `MonitoredLogReader::next_item`, before the sink connector confirms external-system delivery. So this gauge moves ahead of the sink-commit point — for true delivery lag also watch `sink_commit_duration` and the connector's own commit counters (see §9.4 for Iceberg, etc.). |

Sink users: if `write_epoch - read_epoch` is growing, the log store is
buffering faster than the sink can drain. That's the metric to page on for
sink-level backpressure.

### 5.4 Snapshot backfill progress and lag

Newer RisingWave snapshot-backfill jobs emit their own barrier and lag series
keyed on `table_id`:

| Metric                                                          | Type                      | Labels                | Meaning |
|-----------------------------------------------------------------|---------------------------|-----------------------|---------|
| `meta_snapshot_backfill_barrier_duration_seconds`               | histogram  | `table_id`, `barrier_type` | Barrier latency for a snapshot-backfill job. `barrier_type` takes values `consuming_snapshot` (Phase 1, reading the frozen snapshot) and `consuming_log_store` (Phase 2, catching up via the kv log store) — see `src/meta/src/barrier/checkpoint/independent_job/creating_job/barrier_control.rs`. |
| `meta_snapshot_backfill_inflight_barrier_num`                   | gauge   | `table_id`            | Uncommitted barriers for this snapshot-backfill job. |
| `meta_snapshot_backfill_upstream_lag`                           | gauge   | `table_id`            | Lag between the snapshot-backfill job's current epoch and the upstream — shows how far behind the live feed it is. |

### 5.5 Recovery events

| Metric                | Type          | Labels          | Meaning |
|-----------------------|---------------|-----------------|---------|
| `recovery_latency`    | histogram  | `recovery_type` | Time taken to complete a recovery. Histogram buckets tell you whether recoveries finish in seconds or minutes. |
| `recovery_failure_cnt`| counter | `recovery_type` | Count of recovery attempts that failed. A non-zero rate fires the "Recovery Triggered" alert (§3.1). |

A single recovery is typically fast (<30 s); sustained recovery activity or
any failure is always worth a page — cross-reference with component logs on
the affected nodes to find the root cause.

---

## 6. Backpressure

RisingWave's streaming graph is a DAG of actors connected by bounded
channels. Two blocking-time counters at each connection tell you, from both
sides, whether the connection is keeping up:

- **Output-side blocking** on the upstream actor: the downstream's channel
  was full, so the upstream's `try_send` blocked. This is the classic
  *backpressure* signal — downstream is too slow.
- **Input-side blocking** on the downstream actor: the input channel was
  empty, so the downstream had to wait. This is the mirror image —
  *upstream is too slow* (or has nothing to send); it is not backpressure
  per se but it's useful for localising which side of a join / union is
  starving.

Dividing each counter's per-second rate by 1 s (i.e. by 1 000 000 000 ns)
yields a ratio in [0, 1] — the fraction of time the actor spent blocked.

| Metric                                           | Type                      | Labels                                               | Records |
|--------------------------------------------------|---------------------------|------------------------------------------------------|---------|
| `stream_actor_output_buffer_blocking_duration_ns`| counter | `actor_id`, `fragment_id`, `downstream_fragment_id`  | Cumulative nanoseconds the upstream actor's output buffer was full (downstream couldn't accept). |
| `stream_actor_input_buffer_blocking_duration_ns` | counter | `actor_id`, `fragment_id`, `upstream_fragment_id`    | Cumulative nanoseconds the downstream actor's input buffer was empty and it was waiting for upstream data. |

The user dashboard computes the output ratio as:

```promql
avg(rate(stream_actor_output_buffer_blocking_duration_ns[$__rate_interval]))
  by (fragment_id, downstream_fragment_id) / 1e9
```

**Interpretation:**

- **Ratio < 0.3**: healthy.
- **0.3–0.7**: one of the downstream fragments is starting to bottleneck.
  Look at the specific `fragment_id → downstream_fragment_id` pair — that is
  where to scale.
- **> 0.7 and climbing**: the downstream fragment is saturated. Barrier
  latency will follow shortly.
- **~1.0 on a sink-facing fragment**: the sink is the bottleneck, not the
  streaming graph. Check §5.3's `log_store_latest_read_epoch` lag and the
  sink's underlying commit metrics (Phase 3 will cover those in detail).

The *input* variant is the mirror image: high input blocking on a fragment
means its upstream isn't producing fast enough — the bottleneck is higher up.
Generally start with the output variant; the input variant helps localise
*which side* of a join or union is the slow one.

---

## 7. Errors

Four metrics summarise errors visible to RisingWave components. They are the
metrics that dashboards plot as "Errors" and that alerting should track
separately from performance.

### 7.1 User-surfaced errors

All three are `counter` with an `error_type` label that carries a
short classification string. They are emitted in the compute path and are
immediately queryable without log aggregation.

| Metric                    | Labels                                                | Meaning |
|---------------------------|-------------------------------------------------------|---------|
| `user_compute_error_cnt`  | `error_type`, `executor_name`, `fragment_id`          | Errors raised during streaming compute (expression evaluation, type coercion, unsupported cast, divide-by-zero, etc.). `executor_name` is the class name of the offending executor. |
| `user_source_error_cnt`   | `error_type`, `source_id`, `source_name`, `fragment_id` | Errors raised while reading or parsing from a source (protocol parse failure, schema mismatch, connectivity loss, etc.). |
| `user_sink_error_cnt`     | `error_type`, `sink_id`, `sink_name`, `fragment_id`   | Errors raised while writing to a sink (commit failure, auth, schema mismatch, downstream unavailable). |

**Typical alert rules:**
```promql
# Any new source/sink/compute error, grouped by type
sum by (error_type, source_name) (rate(user_source_error_cnt[5m])) > 0
sum by (error_type, sink_name)   (rate(user_sink_error_cnt[5m]))   > 0
sum by (error_type, executor_name, fragment_id) (rate(user_compute_error_cnt[5m])) > 0
```

The user-dashboard "Errors" panel (`grafana/dashboard/user/overview.py:137-156`)
is intended to plot all three of these, split by their respective join
labels. Threshold: any non-zero rate is worth attention — these counters
increment sparingly in healthy clusters.

> **Caveat.** The user dashboard's Errors panel currently queries the
> unsuffixed names `user_compute_error`, `user_source_error`, `user_sink_error`
> (overview.py:141, 145, 149) — **those families do not exist** in the
> metrics registry; the real names are the `_cnt`-suffixed ones above. As a
> result the panel is effectively blank in current builds. When wiring up
> your own ELK / Alertmanager rules, use the names in the table above.

### 7.2 Source availability

| Metric                | Type                      | Labels                     | Meaning |
|-----------------------|---------------------------|----------------------------|---------|
| `source_status_is_up` | gauge   | `source_id`, `source_name` | `1` after the meta source manager's most recent `SplitEnumerator::list_splits()` succeeded, `0` if it failed. Reflects source-enumerator connectivity / auth, not individual compute-side reader health. See §8.1 for the full callsite reference. |

```promql
source_status_is_up == 0    # alert: this source is down
```

### 7.3 Object-storage errors

| Metric                       | Type          | Labels | Meaning |
|------------------------------|---------------|--------|---------|
| `object_store_failure_count` | counter | `type` | Count of failed object-storage operations, classified by `type`. Valid values of `type` are `upload`, `streaming_upload_init`, `streaming_upload`, `streaming_upload_finish`, `read`, `streaming_read_init`, `streaming_read`, `metadata`, `delete`, `delete_objects`, `list` (see `OperationType` in `src/object_store/src/object/mod.rs`). |

This backs the "Abnormal Object Storage Failure" alert (§3.3). Any sustained
non-zero rate is operationally relevant. If you see `list` failures on
startup only, check IAM; if you see sustained `upload` / `streaming_upload`
failures, check the bucket's availability and quotas.

---

## 8. Sources and CDC

§4.1 and §4.2 covered the *throughput* side of sources — rows and bytes in
and out. This section covers everything else: source health and per-connector
internals (Kafka, Kinesis, file sources, CDC).

### 8.1 Source availability and errors

| Metric                         | Type                      | Labels                                          | Meaning |
|--------------------------------|---------------------------|-------------------------------------------------|---------|
| `source_status_is_up`          | gauge   | `source_id`, `source_name`                      | Set by the meta-side source manager on every `tick()`: `1` after a successful `SplitEnumerator::list_splits()` against the upstream, `0` if that call fails (`src/meta/src/stream/source_manager/worker.rs:338-353`). So it measures source-enumerator connectivity / auth rather than individual compute-side reader failures. See §7.2. |
| `source_connector_ack_failure_count` | counter         | `source_name`, `connector_type`, `error_type`   | Count of `ack` failures during checkpoint for source connectors (e.g. when a source connector fails to commit its offset back to the upstream system on a checkpoint boundary). Non-zero sustained rate means checkpoints succeed in RisingWave but the upstream doesn't know about them — replays will re-read data. |

### 8.2 Kafka — per-partition offset and lag

These come from the Kafka connector's internal source tracking (not
librdkafka). They are the right metrics for **application-team-level
monitoring** of a Kafka source.

| Metric                          | Type                         | Labels                                                        | Meaning |
|---------------------------------|------------------------------|---------------------------------------------------------------|---------|
| `source_kafka_high_watermark`   | gauge      | `source_id`, `partition`                                      | Kafka high-watermark offset for this partition — the *next* offset the broker will assign (one past the last produced message). Pair with `source_latest_message_id` (last *processed* offset) for lag. Note this means a fully caught-up partition naturally reports `high_watermark - latest_message_id == 1`, not 0. |
| `source_latest_message_id`      | gauge      | `source_id`, `actor_id`, `partition`                          | Latest message offset that the source reader has processed. Use `source_kafka_high_watermark - source_latest_message_id` for per-partition lag. |
| `source_partition_eof_count`    | counter    | `source_id`, `partition`, `source_name`, `fragment_id`        | Count of end-of-partition events observed. For bounded sources this ticks up when the partition is drained. |
| `source_partition_eof_offset`   | gauge      | `source_id`, `partition`, `source_name`, `fragment_id`        | Offset at which the EOF was observed. |
| `stream_source_split_change_event_count` | counter | `source_id`, `source_name`, `actor_id`, `fragment_id`    | Split-reassignment events (Kafka consumer-group rebalance or split migration). A sustained non-zero rate indicates churn. |

**Per-partition Kafka lag** (matches the dev-dashboard panel in
`grafana/dashboard/dev/source_general.py`):

```promql
clamp_min(
  source_kafka_high_watermark
  - on(source_id, partition) group_right() source_latest_message_id,
  0
)
```

`source_kafka_high_watermark` has no `actor_id` while
`source_latest_message_id` does. `group_right()` is the one-to-many match
keyed on `(source_id, partition)`, with the right side as the "many" — its
labels (`actor_id`, `partition`, `source_id`) are preserved on each output
series so the legend tells you *which* parallel reader is behind.
`clamp_min(..., 0)` guards against transient cases where the two metrics
are scraped out of order and the difference goes briefly negative.

Because `source_kafka_high_watermark` is the broker's *next* offset and
`source_latest_message_id` is the *last processed* offset, this expression
reports `1` for a fully caught-up partition rather than `0`. Subtract `1`
on the dashboard if you want the visual to read zero when caught up. To
collapse to a pure per-partition view, wrap in `max by (source_id, partition)`.

### 8.3 Kafka — librdkafka internal stats (`rdkafka_*`)

RisingWave re-exports the full librdkafka statistics object as Prometheus
metrics. These are deep internals that the Dev dashboard's **Kafka** panel
exposes. Application teams typically do not need to watch them unless
diagnosing a specific connector-level issue (authentication failure, buffer
saturation, broker misbehaviour); in that case, this table is the reference.

They fall into four families plus a *window-stats* schema used for rolling
latency/size histograms:

| Family                       | Labels                                        | Examples |
|------------------------------|-----------------------------------------------|----------|
| `rdkafka_top_*` (client-level)  | `id`, `client_id`                          | `rdkafka_top_msg_cnt`, `rdkafka_top_rx_bytes`, `rdkafka_top_tx_bytes`, `rdkafka_top_age`, `rdkafka_top_time`, `rdkafka_top_ts`, `rdkafka_top_metadata_cache_cnt`, `rdkafka_top_replyq`, `rdkafka_top_simple_cnt` — 18 metrics total. |
| `rdkafka_broker_*` (per broker)   | `id`, `client_id`, `broker`, `state`    | Connections, disconnects, bytes in/out, request counts, buffer growth, errors, timeouts — 23 metrics in total including `rdkafka_broker_tx`, `rdkafka_broker_rx`, `rdkafka_broker_tx_bytes`, `rdkafka_broker_rx_bytes`, `rdkafka_broker_connects`, `rdkafka_broker_disconnects`, `rdkafka_broker_state_age`, `rdkafka_broker_req_timeouts`. One exception: `rdkafka_broker_req` carries an extra `type` label that discriminates request kinds (`Produce`, `Fetch`, `Metadata`, …). |
| `rdkafka_topic_metadata_age`   | `id`, `client_id`, `topic`                   | Topic-level metadata. |
| `rdkafka_topic_partition_*` (per partition) | `id`, `client_id`, `topic`, `partition` | Per-partition offsets and queue sizes — `rdkafka_topic_partition_consumer_lag` (broker-reported lag), `rdkafka_topic_partition_hi_offset`, `rdkafka_topic_partition_lo_offset`, `rdkafka_topic_partition_next_offset`, `rdkafka_topic_partition_committed_offset`, `rdkafka_topic_partition_fetchq_cnt`, etc. — 26 metrics. |
| `rdkafka_consumer_group_*` (consumer group) | `id`, `client_id`, `state`            | `rdkafka_consumer_group_state_age`, `rdkafka_consumer_group_rebalance_age`, `rdkafka_consumer_group_rebalance_cnt`, `rdkafka_consumer_group_assignment_size`. |

**Window stats.** For rolling latency / size distributions librdkafka emits
a fixed schema and RisingWave registers 14 suffixes per window
(min / max / avg / sum / cnt / stddev / hdrsize / p50 / p75 / p90 / p95 /
p99 / p99_99 / out_of_range). However the current `StatsWindow::report`
implementation in `src/connector/src/source/kafka/stats.rs` only writes
samples for **min / max / avg / sum / cnt / stddev / hdrsize / p50 / p75 /
p90 / p99_99 / out_of_range** — `p95` and `p99` are registered but never
populated, so those two will always read as zero or stale. Until that is
fixed, use `p99_99` (or compute your own quantile from p50/p75/p90) instead
of `p99`. The inventory stores these as `rdkafka_<PATH>_<stat>` templates;
the actual exposed metric names substitute a concrete path. Six window-stat
instances are registered:

| Path                  | Concrete metric name prefix | Labels                                        | Window semantics |
|-----------------------|-----------------------------|-----------------------------------------------|-------------------|
| `topic_batchsize`     | `rdkafka_topic_batchsize_*` | `id`, `client_id`, `broker`, `topic`          | Bytes per produce/consume batch. |
| `topic_batchcnt`      | `rdkafka_topic_batchcnt_*`  | `id`, `client_id`, `broker`, `topic`          | Messages per produce/consume batch. |
| `broker_intlatency`   | `rdkafka_broker_intlatency_*` | `id`, `client_id`, `broker`, `topic`        | Internal queue-to-socket latency (µs). |
| `broker_outbuflatency`| `rdkafka_broker_outbuflatency_*` | `id`, `client_id`, `broker`, `topic`     | Time in the broker's output buffer (µs). |
| `broker_rtt`          | `rdkafka_broker_rtt_*`      | `id`, `client_id`, `broker`, `topic`          | Request/response round-trip (µs). |
| `broker_throttle`     | `rdkafka_broker_throttle_*` | `id`, `client_id`, `broker`, `topic`          | Kafka broker-side throttle time (µs). |

So e.g. the 99.99th-percentile broker RTT is
`rdkafka_broker_rtt_p99_99{id=...,client_id=...,broker=...,topic=...}`
(`rdkafka_broker_rtt_p99` is registered but unpopulated as noted above).
There are 14 × 6 = 84 window-stat metric names in total; the inventory
lists them as 14 template rows to stay compact.

### 8.4 Kinesis source

Emitted by the AWS Kinesis connector. All five carry the same label set:
`source_id`, `source_name`, `fragment_id`, `shard_id`. Four are counters;
`kinesis_lag_latency_ms` is a histogram.

| Metric                                | Type                      | Meaning |
|---------------------------------------|---------------------------|---------|
| `kinesis_lag_latency_ms`              | histogram  | Histogram of `MillisBehindLatest` reported by AWS on each `GetRecords` response — i.e. how far behind the tip of the shard the latest fetched batch is, in milliseconds. Not per-record age. Use `histogram_quantile(0.99, sum(rate(kinesis_lag_latency_ms_bucket[$__rate_interval])) by (le, source_id, shard_id))`. |
| `kinesis_throughput_exceeded_count`   | counter | `ProvisionedThroughputExceededException` hit count. Sustained non-zero → shard throughput limit too low; reshard or increase the shard count on the stream. |
| `kinesis_timeout_count`               | counter | Kinesis GetRecords timeouts. Usually transient. |
| `kinesis_rebuild_shard_iter_count`    | counter | Shard iterator rebuilds. Bursty events are normal; sustained high rate indicates instability. |
| `kinesis_early_terminate_shard_count` | counter | Count of times a shard was closed / split early by AWS and the connector had to release it. |

### 8.5 File sources (S3 / GCS / etc.)

| Metric                            | Type                      | Labels                                                        | Meaning |
|-----------------------------------|---------------------------|---------------------------------------------------------------|---------|
| `file_source_input_row_count`     | counter | `source_id`, `source_name`, `actor_id`, `fragment_id`         | Rows read by the file source. Counter. |
| `file_source_dirty_split_count`   | gauge   | `source_id`, `source_name`, `actor_id`, `fragment_id`         | Current number of splits that failed and are retained for retry / inspection. Gauge. |
| `file_source_failed_split_count`  | counter | `source_id`, `source_name`, `actor_id`, `fragment_id`         | Cumulative count of splits that ended in failure. |

### 8.6 CDC (Postgres / MySQL / SQL Server)

CDC sources emit LSN (or binlog) offset metrics that let you measure
replication lag and confirm checkpoints are advancing on the upstream.

| Metric                                       | Type                    | Labels                   | Source / Meaning |
|----------------------------------------------|-------------------------|--------------------------|-------------------|
| `source_cdc_event_lag_duration_milliseconds` | histogram| `table_name`             | Per-event lag between the source's commit timestamp and RisingWave's ingest timestamp — the canonical "CDC freshness" metric. Use `histogram_quantile(0.99, …)`. |
| **Postgres CDC** |  |  |  |
| `pg_cdc_confirmed_flush_lsn`                 | gauge | `source_id`, `slot_name` | LSN up to which RisingWave has confirmed the replication slot can advance. If this stops moving, the upstream WAL will grow unbounded. |
| `pg_cdc_upstream_max_lsn`                    | gauge | `source_id`, `slot_name` | Upstream's latest LSN. Subtract `pg_cdc_confirmed_flush_lsn` for replication lag in LSN units. |
| `stream_pg_cdc_state_table_lsn`              | gauge | `source_id`              | LSN stored in the CDC executor's state table at the last checkpoint. Divergence from `pg_cdc_confirmed_flush_lsn` indicates a checkpoint / commit gap. |
| `stream_pg_cdc_jni_commit_offset_lsn`        | gauge | `source_id`              | LSN committed via the JNI bridge to the Debezium engine. |
| **MySQL CDC** |  |  |  |
| `stream_mysql_cdc_state_binlog_file_seq`     | gauge | `source_id`              | Numeric suffix of the current binlog file (`mysql-bin.000123` → `123`). |
| `stream_mysql_cdc_state_binlog_position`     | gauge | `source_id`              | Byte position within the current binlog file. |
| **SQL Server CDC** |  |  |  |
| `sqlserver_cdc_upstream_max_lsn`             | gauge | `source_id`              | Upstream's latest LSN. |
| `sqlserver_cdc_upstream_min_lsn`             | gauge | `source_id`              | Upstream's earliest available LSN (anything below is truncated). |
| `stream_sqlserver_cdc_state_change_lsn`      | gauge | `source_id`              | LSN of the last change record ingested. |
| `stream_sqlserver_cdc_state_commit_lsn`      | gauge | `source_id`              | LSN of the last committed transaction. |
| `stream_sqlserver_cdc_jni_commit_offset_lsn` | gauge | `source_id`              | LSN committed to the Debezium engine. |

### 8.7 Iceberg source

If you are reading from an Iceberg table as a source (not writing to one as
a sink), these metrics report the scan behaviour.

| Metric                                         | Type                      | Labels                                                        | Meaning |
|------------------------------------------------|---------------------------|---------------------------------------------------------------|---------|
| `iceberg_source_snapshots_discovered_total`    | counter | `source_id`, `source_name`, `table_name`                      | Snapshots discovered via incremental scan. |
| `iceberg_source_snapshot_lag_seconds`          | gauge   | `source_id`, `source_name`, `table_name`                      | Time between the Iceberg table's latest snapshot and the snapshot RisingWave most recently ingested. |
| `iceberg_source_files_discovered_total`        | counter | `source_id`, `source_name`, `table_name`, `file_type`         | Files discovered per scan. `file_type` ∈ `data`, `eq_delete`, `pos_delete`. |
| `iceberg_source_files_read_total`              | counter | `table_name`, `file_type`                                     | Files read from Iceberg. |
| `iceberg_source_inflight_file_count`           | gauge   | `source_id`, `source_name`, `table_name`                      | Files currently being fetched by the active reader. |
| `iceberg_source_list_duration_seconds`         | histogram  | `source_id`, `source_name`, `table_name`                      | Time spent planning files from a snapshot. |
| `iceberg_source_file_read_duration_seconds`    | histogram  | `table_name`                                                  | Per-file read duration. |
| `iceberg_source_rows_read_total`               | counter | `table_name`                                                  | Rows read from Iceberg. |
| `iceberg_source_delete_files_per_data_file`    | histogram  | `source_id`, `source_name`, `table_name`                      | Delete-files attached per data-file scan. Higher → delete-merge burden on reader. |
| `iceberg_source_delete_rows_applied_total`     | counter | `table_name`, `delete_type`                                   | Rows removed by delete processing. Currently the only emitted `delete_type` value is `sdk_applied_approx` — the iceberg-rust SDK doesn't break the count out by position vs equality delete, so the label exists but has one value today. |
| `iceberg_source_scan_errors_total`             | counter | `source_id`, `source_name`, `table_name`, `error_type`        | Scan errors classified by `error_type`. |
| `iceberg_read_bytes`                           | counter | `table_name`                                                  | Total bytes read from Iceberg. |

---

## 9. Sinks

Beyond the top-line `stream_sink_input_row_count` / `_bytes` / `_chunk_buffer_size`
from §4.3, sinks have three additional metric surfaces:

1. **The sink executor's commit path** — `sink_commit_duration` + the log-store
   write side.
2. **The log store** — a buffer layer between the streaming graph and the
   external sink system. Two flavours are shipped: the in-memory **kv log
   store** (used by most sinks) and the on-disk **sync kv log store** (used
   when the external system is slow and we need a Hummock-backed buffer).
3. **Iceberg sinks** — sink-specific writer metrics.

### 9.1 Sink commit + log-store write

| Metric                         | Type                      | Labels                                                        | Meaning |
|--------------------------------|---------------------------|---------------------------------------------------------------|---------|
| `sink_commit_duration`         | histogram  | `actor_id`, `connector`, `sink_id`, `sink_name`               | End-to-end time the sink spent in its commit callback (network I/O, external-system round-trip, transaction commit, etc.). p99 growing → external system slow, sink about to backpressure. |
| `log_store_latest_write_epoch` | gauge   | `actor_id`, `sink_id`, `sink_name`                            | Epoch most recently written into the log store. |
| `log_store_latest_read_epoch`  | gauge   | `actor_id`, `connector`, `sink_id`, `sink_name`               | Epoch most recently **read out** of the log store by the sink reader (updated in `MonitoredLogReader::next_item`). This is *before* the sink connector commits to the external system — see the §5.3 caveat. |
| `log_store_first_write_epoch`  | gauge   | `actor_id`, `sink_id`, `sink_name`                            | First epoch this log writer saw at init (set once in `MonitoredLogWriter::init()`; does not track the earliest epoch still buffered). Useful as a bootstrap marker; for live lag use `log_store_latest_write_epoch` vs `log_store_latest_read_epoch`. |
| `log_store_write_rows`         | counter | `actor_id`, `sink_id`, `sink_name`                            | Rows written into the log store. |
| `log_store_read_rows`          | counter | `actor_id`, `connector`, `sink_id`, `sink_name`               | Rows read from the log store (consumed by the sink). |
| `log_store_read_bytes`         | counter | `actor_id`, `connector`, `sink_id`, `sink_name`               | Bytes read from the log store. |
| `log_store_reader_wait_new_future_duration_ns` | counter | `actor_id`, `connector`, `sink_id`, `sink_name` | Total nanoseconds the log-store reader was idle waiting to be driven. Divided by wall-clock → reader idle ratio. |

**Sink lag in seconds (user dashboard):**
```promql
max(
  timestamp(log_store_latest_write_epoch)
  - (1617235200000 + (log_store_latest_write_epoch != 0) / 65536) / 1000
) by (sink_id, sink_name)
```
That's enqueue-side sink lag (how far behind wall-clock the log-writer is).
Swap in `log_store_latest_read_epoch` for delivery-side lag (how far behind
wall-clock the sink reader is). If delivery-side lag is growing while
enqueue-side is steady, the log store is buffering faster than the sink
connector can drain — the sink is the bottleneck.

### 9.2 The kv log store (in-memory path)

The kv log store sits between the sink executor and the sink connector. All
rows destined for a sink first go into this buffer, and the sink reader
pulls from it. When the sink stalls, the buffer grows; once it fills, the
streaming graph backpressures.

| Metric                                           | Type                      | Meaning |
|--------------------------------------------------|---------------------------|---------|
| `kv_log_store_buffer_memory_bytes`               | gauge   | Estimated heap bytes currently held (unconsumed + consumed-but-not-truncated). The headline "how big is the buffer" gauge. |
| `kv_log_store_buffer_unconsumed_item_count`      | gauge   | Chunks not yet read by the sink. |
| `kv_log_store_buffer_unconsumed_row_count`       | gauge   | Rows not yet read. |
| `kv_log_store_buffer_unconsumed_epoch_count`     | gauge   | Number of distinct epochs still buffered. |
| `kv_log_store_buffer_unconsumed_min_epoch`       | gauge   | Earliest unconsumed epoch. Pair with `log_store_latest_write_epoch` for "how far behind is the reader". |
| `kv_log_store_storage_write_count`               | counter | Rows written into the log store. |
| `kv_log_store_storage_write_size`                | counter | Bytes written into the log store. |
| `kv_log_store_storage_read_count`                | counter | Rows read from the **persistent log** side of the log store. `read_type` label distinguishes `persistent_log` (rows read directly from Hummock) from `flushed_buffer` (rows that had been flushed to Hummock and are read back from there). Pure in-memory buffer reads are *not* counted here — they go through the stream path, not the storage path. |
| `kv_log_store_storage_read_size`                 | counter | Bytes read from the persistent-log side; same `read_type` values. |
| `kv_log_store_rewind_count`                      | counter | Log-store rewind events — recovery-induced replays from a prior epoch. Spikes are normal during recovery; sustained non-zero rate in steady state indicates a sink that cannot commit its epoch. |
| `kv_log_store_rewind_delay`                      | histogram  | Duration of rewind operations. |

All of the above carry the same labels: `actor_id`, `connector`, `sink_id`,
`sink_name` (storage_read_count/size also carry `read_type`). Because the
log-store is per-actor, aggregate by `sink_id` when alerting unless you
specifically need per-parallel-unit detail.

### 9.3 The sync kv log store (on-disk path)

The sync kv log store backs sinks that use a Hummock-persisted log. Its
metrics share semantics with §9.2 but use the `target` / `relation`
label set — it is also used for MV-on-MV buffering, not just sinks, hence
the different label convention.

| Metric                                                  | Type                      | Labels                                                 | Meaning |
|---------------------------------------------------------|---------------------------|--------------------------------------------------------|---------|
| `sync_kv_log_store_buffer_memory_bytes`                 | gauge   | `actor_id`, `target`, `fragment_id`, `relation`        | Bytes held. |
| `sync_kv_log_store_buffer_unconsumed_item_count`        | gauge   | …                                                      | Unconsumed chunks. |
| `sync_kv_log_store_buffer_unconsumed_row_count`         | gauge   | …                                                      | Unconsumed rows. |
| `sync_kv_log_store_buffer_unconsumed_epoch_count`       | gauge   | …                                                      | Unconsumed epochs. |
| `sync_kv_log_store_buffer_unconsumed_min_epoch`         | gauge   | …                                                      | Earliest unconsumed epoch. |
| `sync_kv_log_store_storage_write_count`                 | counter | …                                                      | Rows written through the storage path. |
| `sync_kv_log_store_storage_write_size`                  | counter | …                                                      | Bytes written. |
| `sync_kv_log_store_read_count`                          | counter | `type`, `actor_id`, `target`, `fragment_id`, `relation`| Rows read; `type` ∈ `buffer` (served directly from in-memory buffer), `total` (all reads), `persistent_log` (read from Hummock-backed persistent log), `flushed_buffer` (read from a buffer that has been flushed to storage). |
| `sync_kv_log_store_read_size`                           | counter | same as `_read_count`                                  | Bytes read. Same label space as `_read_count`, but in current code only the `persistent_log` and `flushed_buffer` `type` values are actually incremented — the `buffer` and `total` size series are constructed and exported but stay at zero. |
| `sync_kv_log_store_wait_next_poll_ns`                   | counter | `actor_id`, `target`, `fragment_id`, `relation`        | Cumulative idle time waiting for the next poll. |
| `sync_kv_log_store_write_pause_duration_ns`             | counter | `actor_id`, `target`, `fragment_id`, `relation`        | Total nanoseconds writes were paused because the buffer was full. Directly translates to "how long the sink's upstream was backpressured". |
| `sync_kv_log_store_state`                               | counter | `state`, `actor_id`, `target`, `fragment_id`, `relation`| Counts state transitions of the log store — `state` ∈ `clean` / `dirty`. |

### 9.4 Iceberg sinks

| Metric                                     | Type                      | Labels                                                 | Meaning |
|--------------------------------------------|---------------------------|--------------------------------------------------------|---------|
| `iceberg_write_qps`                        | counter | `actor_id`, `sink_id`, `sink_name`                     | Write operations per second (use `rate(...)`). |
| `iceberg_write_bytes`                      | counter | `actor_id`, `sink_id`, `sink_name`                     | Bytes written. |
| `iceberg_write_latency`                    | histogram  | `actor_id`, `sink_id`, `sink_name`                     | Per-write latency. |
| `iceberg_partition_num`                    | gauge   | `actor_id`, `sink_id`, `sink_name`                     | Current number of distinct partitions the partition writer is tracking. Sustained growth → write amplification. |
| `iceberg_position_delete_cache_num`        | gauge   | `actor_id`, `sink_id`, `sink_name`                     | Size of the in-memory position-delete cache. |
| `iceberg_rolling_unflushed_data_file`      | gauge   | `actor_id`, `sink_id`, `sink_name`                     | Count of rolling data files not yet flushed. |
| `iceberg_snapshot_num`                     | gauge   | `sink_name`, `catalog_name`, `table_name`              | Count of snapshots currently visible on the target Iceberg table. Used to confirm snapshots are advancing. |

---

## 10. Storage (Hummock)

RisingWave's storage engine (Hummock) is a tiered LSM backed by object
storage and a local block cache. The interesting operational metrics split
into four layers:

1. **State-store API** — what the streaming/batch executors observe when
   they read and write (`state_store_*`, 60+ metrics).
2. **Compactor** — how compaction is performing (`compactor_*` and a subset
   of `storage_*`).
3. **Hummock version management** — the metrics behind the Storage Alerts
   in §3.3 (version ids, delta logs, stale objects — covered there).
4. **Object storage I/O** — the HTTP-level view of calls to S3/GCS/etc.
   (`object_store_*`, §10.4).

Full metric lists live in Appendix B. The narrative below highlights the
metrics application teams and SREs most often care about.

### 10.1 State-store reads (executor's view)

Every streaming operator that accesses an internal state table reads
through the state store. Key metrics:

| Metric                                         | Type                      | Labels                            | Use |
|------------------------------------------------|---------------------------|-----------------------------------|------|
| `state_store_get_duration`                     | histogram  | `table_id`                        | Latency of single-key point `get` calls. p99 > 10 ms → cache or remote read hot spot. |
| `state_store_iter_init_duration`               | histogram  | `table_id`, `iter_type`           | Iterator setup cost (seeking into an SST, checking bloom filters). |
| `state_store_iter_scan_duration`               | histogram  | `table_id`, `iter_type`           | Time spent scanning once open. Dominates when ranges are large. |
| `state_store_iter_item`                        | histogram  | `table_id`, `iter_type`           | Rows returned per iter. Bimodal output usually indicates a plan change (selective vs full scan). |
| `state_store_iter_size`                        | histogram  | `table_id`, `iter_type`           | Bytes returned per iter. |
| `state_store_iter_counts`                      | counter | `table_id`, `iter_type`           | Iterator invocations. `iter_type` values: `iter` and `iter_log` (`monitored_storage_metrics.rs:49`). |
| `state_store_iter_in_progress_counts`          | gauge   | `table_id`, `iter_type`           | Open iterators right now. A non-returning large value signals a stuck executor. |
| `state_store_get_shared_buffer_hit_counts`     | counter             | `table_id`                        | `get` calls satisfied out of the in-memory shared buffer (no block cache or remote hit needed). |
| `state_store_bloom_filter_check_counts`        | counter | `table_id`, `type`                | Bloom filter probes. Pair with `state_store_bloom_filter_true_negative_counts` for hit rate — low true-negative rate means bloom filters aren't pruning reads. |
| `state_store_bloom_filter_true_negative_counts`| counter | `table_id`, `type`                | Bloom filter probes that correctly rejected a non-existent key. |
| `state_store_read_req_check_bloom_filter_counts` | counter | `table_id`, `type`              | Read requests that went through bloom filter checking. |
| `state_store_read_req_bloom_filter_positive_counts` | counter | `table_id`, `type`           | Positive bloom-filter hits (must then do the actual read). |
| `state_store_read_req_positive_but_non_exist_counts`| counter | `table_id`, `type`           | False positives — bloom said "might exist", actual read said "no". Rising means filter sizing is inadequate. |
| `state_store_iter_log_op_type_counts`          | counter | `table_id`, `op_type`             | Log-store iterator op mix. |
| `state_store_iter_fetch_meta_duration`         | histogram  | `table_id`                        | Time fetching SST metadata (first iter into an SST). |
| `state_store_iter_fetch_meta_cache_unhits`     | gauge                  | —                                 | Registered for symmetry but currently unused at runtime — no code path writes to it. The slow-path counterpart `state_store_iter_slow_fetch_meta_cache_unhits` (also gauge) is the one to read; it's set by the slow-fetch iterator in `src/storage/src/hummock/store/version.rs:1176` to the SST-meta unhit count for that fetch. |
| `state_store_iter_merge_sstable_counts`        | histogram              | `table_id`, `type`                | Number of SSTs merged into a single iter. High values → read amplification, compaction lagging. |
| `state_store_iter_scan_key_counts`             | counter | `table_id`, `type`                | Keys scanned per iter. |
| `state_store_get_key_size`                     | histogram  | `table_id`                        | Distribution of get-key sizes. |
| `state_store_get_value_size`                   | histogram  | `table_id`                        | Distribution of get-value sizes. |
| `state_store_old_value_size`                   | gauge   | `table_id`                        | Size of "old-value" entries currently held (used by changelog / time-travel). |
| `state_store_safe_version_hit`                 | counter                   | —                                 | Successful attempts to retrieve a safe version (time-travel / snapshot reads). |
| `state_store_safe_version_miss`                | counter                   | —                                 | Attempts that could not find a safe version. |
| `state_store_remote_read_time_per_task`        | histogram              | `table_id`                        | Wall-clock remote-read time attributed to a task. |

The `state_table_*` companion metrics sit one layer above — they are
per-state-table counts of `get` / `iter` calls and vnode-pruning
effectiveness, keyed on `table_id`, `actor_id`, `fragment_id`.

### 10.2 State-store writes and uploads (compute side)

| Metric                                      | Type                      | Labels                                 | Use |
|---------------------------------------------|---------------------------|----------------------------------------|------|
| `state_store_write_batch_duration`          | histogram  | `table_id`                             | Time to apply a write batch to memtable. |
| `state_store_write_batch_size`              | histogram  | `table_id`                             | Bytes per write batch. |
| `state_store_sync_duration`                 | histogram                 | —                                      | Time to flush dirty memtables to object storage at the end of a barrier. |
| `state_store_sync_size`                     | histogram                 | —                                      | Bytes flushed per sync. |
| `state_store_spill_task_counts`             | counter             | `uploader_stage`                       | Spill tasks started. The label is declared `uploader_stage` but the only value emitted today is `unsealed` (i.e. spilling unsealed imm batches before a sync). |
| `state_store_spill_task_size`               | counter             | `uploader_stage`                       | Bytes spilled. Same single `unsealed` label value as above. |
| `state_store_uploader_imm_size`             | gauge                 | —                                      | Total size of immutable memtables currently tracked. |
| `state_store_uploader_uploading_task_size`  | gauge                 | —                                      | Total bytes of upload tasks in flight. |
| `state_store_uploader_uploading_task_count` | gauge                  | —                                      | Number of upload tasks in flight. |
| `state_store_uploader_upload_task_latency`  | histogram                 | —                                      | Upload task latency. |
| `state_store_uploader_syncing_epoch_count`  | gauge                  | —                                      | Number of epochs currently syncing. |
| `state_store_uploader_per_table_imm_size`   | gauge   | `table_id`                             | Imm size per table. |
| `state_store_uploader_per_table_imm_count`  | gauge   | `table_id`                             | Imm count per table. |
| `state_store_mem_table_spill_counts`        | counter | `table_id`                             | Forced memtable spills (ran out of budget before the barrier boundary — one signal that the compute node is memory-starved for writes). |
| `state_store_event_handler_latency`         | histogram              | `event_type`                           | Time the Hummock event handler spent processing each event type. Tied to the "Abnormal Pending Event Number" alert in §3.3. |
| `state_store_event_handler_pending_event`   | gauge               | `event_type`                           | Backlog of events in the handler queue. |
| `state_store_uploading_memory_usage_ratio`  | gauge                     | —                                      | Fraction (0–1) of the uploading-memory budget currently used. Tied to the "Abnormal Uploading Memory Usage" alert (§3.3) at 0.8. |
| `uploading_memory_size`                     | gauge                  | —                                      | Absolute bytes currently used for uploading. |
| `state_store_prefetch_memory_size`          | gauge                  | —                                      | Bytes currently used by read-ahead prefetch. |

### 10.3 Block cache, meta cache, tiered cache (foyer)

| Metric                              | Type                 | Labels | Use |
|-------------------------------------|----------------------|--------|------|
| `state_store_block_cache_size`      | gauge             | —      | Current bytes occupied by the block cache (sampled from `block_cache().memory().usage()`). Compare against the configured cache capacity to know whether you're memory-bound. |
| `state_store_block_cache_usage_ratio` | gauge              | —      | Block-cache usage / capacity. |
| `state_store_meta_cache_size`       | gauge             | —      | Current bytes occupied by the SST-metadata cache. |
| `state_store_meta_cache_usage_ratio`| gauge                | —      | Meta-cache usage / capacity. |
| `block_efficiency_histogram`        | histogram            | —      | Fraction of each fetched block that was actually read. Long tail near 0 → small scans are paying full-block I/O cost. |

RisingWave's tiered cache is the **foyer** crate. It emits its own family
of `foyer_*` metrics (memory hits/misses, disk hits/misses, flush /
reclaim / fill counters), labelled by cache name and op/type. These come
from the upstream `foyer` crate and are not in `inventory.tsv` (the
inventory only scans this repo's `src/`). The Grafana **Hummock Tiered
Cache** panel is the curated view; for definitions see the foyer crate's
own metrics module.

The **refill** family tracks the background re-warming of the tiered cache
after a new version is installed:

| Metric            | Type          | Labels | Use |
|-------------------|---------------|--------|------|
| `refill_total`    | counter | `type`, `op` | Total refill operations, by cache kind (`type` ∈ `data`, `meta`, `parent_meta`, `unit_inheritance`, `block`) and outcome (`op` ∈ `attempts`, `started`, `success`, `filtered`, `unfiltered`, `hit`, `miss`). See `src/storage/src/hummock/event_handler/refiller.rs:102-138`. |
| `refill_bytes`    | counter | `type`, `op` | Bytes refilled. Although the metric carries the same `(type, op)` label space as `refill_total`, in current code only `refill_bytes{type="data",op="ideal"}` (target bytes the refill aimed for) and `refill_bytes{type="data",op="success"}` (bytes actually refilled) are written. Other `(type, op)` combinations exist as label-cardinality possibilities but are not emitted. |
| `refill_duration` | histogram  | `type`, `op` | Refill latency — same labels. |
| `recent_filter_items` | gauge  | —      | Entries in the recent-filter (a small hot-key filter). |
| `recent_filter_ops`   | counter | `op` | Recent-filter operations, by op type. |

### 10.4 Object-storage I/O

| Metric                            | Type                 | Labels                               | Use |
|-----------------------------------|----------------------|--------------------------------------|------|
| `object_store_operation_latency`  | histogram         | `media_type`, `type`                 | Per-operation latency histogram. `type` values follow `OperationType` (see §7.3). `media_type` identifies the backend — values include `mem`, `s3` (legacy direct S3), and the OpenDAL-backed names `Fs`, `S3`, `Minio`, `Gcs`, `Azblob`, `Obs`, `Oss`, `Webhdfs` (mixed case is intentional, matches OpenDAL `Scheme` names). |
| `object_store_operation_bytes`    | histogram         | `type`                               | Per-operation payload size, by operation `type`. |
| `object_store_failure_count`      | counter        | `type`                               | Failure count — §7.3 / alert in §3.3. |
| `object_store_request_retry_count`| counter        | `type`                               | RisingWave's object-store wrapper retry counter — incremented by the retry policy in `src/object_store/src/object/mod.rs` when an operation of `type` (the `OperationType` set from §7.3) is retried. Distinct from the underlying AWS SDK's own retry counter (`aws_sdk_retry_counts`). Non-zero sustained rate → degraded upstream. |
| `object_store_read_bytes`         | counter           | —                                    | Total bytes read by the compute/compactor process. |
| `object_store_write_bytes`        | counter           | —                                    | Total bytes written. |
| `aws_sdk_retry_counts`            | counter           | —                                    | Global AWS SDK retry counter, incremented by the logger's tracing-event hook on every `aws_smithy_client::retry` event. Covers all AWS SDK clients in the process (S3 / Kinesis / etc.), not S3 only. |

### 10.5 Compactor

| Metric                                         | Type                | Labels                           | Use |
|------------------------------------------------|---------------------|----------------------------------|------|
| `storage_compaction_group_count`               | gauge            | —                                | Number of active compaction groups. |
| `storage_compaction_group_size`                | gauge         | `group`                          | Bytes in each compaction group. |
| `storage_compaction_group_file_count`          | gauge         | `group`                          | SST count per group. |
| `storage_compaction_group_throughput`          | gauge         | `group`                          | Write throughput per group. |
| `storage_compact_task_pending_num`             | gauge            | —                                | Compaction tasks currently queued. |
| `storage_compact_task_pending_parallelism`     | gauge            | —                                | Parallelism budgets occupied by pending tasks. |
| `storage_compact_pending_bytes`                | gauge         | `group`                          | Bytes awaiting compaction per group. |
| `storage_level_compact_cnt`                    | gauge         | `level_index`                    | Number of SSTs in this level that are queued / pending to be merged into the next level (`pending_file_count()` in source — help: "num of SSTs to be merged to next level in each level"). Despite the name, **not** a count of currently-running compaction tasks. |
| `storage_level_compact_frequency`              | counter       | `compactor`, `group`, `task_type`, `result` | Completed compactions split by `task_type` and `result`. Values come from the protobuf `as_str_name()` and are upper-snake-case. `task_type` ∈ `DYNAMIC`, `SPACE_RECLAIM`, `MANUAL`, `TTL`, `EMERGENCY`, `TOMBSTONE`, `VNODE_WATERMARK`, `TYPE_UNSPECIFIED`, `SHARED_BUFFER`; `result` ∈ `UNSPECIFIED`, `PENDING`, `SUCCESS`, `HEARTBEAT_CANCELED`, `NO_AVAIL_MEMORY_RESOURCE_CANCELED`, `NO_AVAIL_CPU_RESOURCE_CANCELED`, `ASSIGN_FAIL_CANCELED`, `SEND_FAIL_CANCELED`, `MANUAL_CANCELED`, `INVALID_GROUP_CANCELED`, `INPUT_OUTDATED_CANCELED`, `EXECUTE_FAILED`, `JOIN_HANDLE_FAILED`, `TRACK_SST_OBJECT_ID_FAILED`, `HEARTBEAT_PROGRESS_CANCELED`, `RETENTION_TIME_REJECTED`, `SERVERLESS_SEND_FAIL_CANCELED`, `SERVERLESS_TABLE_NOT_FOUND_CANCELED` (full list from `compact_task::TaskStatus`). Use `rate(...)` for QPS. |
| `storage_level_compact_read_curr` / `_next`    | counter       | `group`, `level_index`           | **Kilobytes** read from the current level / next level during compaction (help string says "KBs read from … level during history compactions to next level"). |
| `storage_level_compact_write`                  | counter       | `group`, `level_index`           | **Kilobytes** written to the next level during compaction (help: "KBs written into next level…"). |
| `storage_level_compact_read_sstn_curr` / `_next` | counter     | `group`, `level_index`           | SSTs read from each level. |
| `storage_level_compact_write_sstn`             | counter       | `group`, `level_index`           | SSTs written out. |
| `storage_level_sst_num`                        | gauge         | `level_index`                    | Live SST count per level. Useful for read-amplification analysis. (The "Lagging Compaction" alert in §3.3 actually watches `storage_level_total_file_size` at the L0 sub-levels, not SST count.) |
| `storage_level_total_file_size`                | gauge         | `level_index`                    | Live SST file size per level, **in kilobytes** (the source help string is "KBs total file bytes in each level" and the value is `level_sst_size / 1024`). When using this metric in alert thresholds, remember the unit. |
| `storage_compact_level_compression_ratio`      | gauge            | `group`, `level`, `algorithm`    | Post-compaction compression ratio by algorithm (`zstd`, `lz4`, …). |
| `storage_compact_task_size`                    | histogram        | `group`, `type`                  | Per-task size distribution. |
| `storage_compact_task_file_count`              | histogram        | `group`, `type`                  | Per-task SST count. |
| `storage_compact_task_batch_count`             | histogram        | `type`                           | Batched-compaction task count distribution. |
| `storage_compact_task_trivial_move_sst_count`  | histogram        | `group`                          | Number of trivial-move SSTs. |
| `storage_l0_compact_level_count`               | histogram        | `group`, `type`                  | Number of L0 sub-levels involved. |
| `storage_branched_sst_count`                   | gauge         | `group`                          | SSTs shared across compaction-group branches (rare; tied to version-size growth). |
| `storage_full_gc_trigger_count`                | gauge            | —                                | Full-GC runs triggered. |
| `storage_full_gc_candidate_object_count`       | histogram           | —                                | Objects considered per full-GC run. |
| `storage_full_gc_selected_object_count`        | histogram           | —                                | Objects selected for deletion. |
| `compactor_compact_task_duration`              | histogram        | `group`, `level`                 | End-to-end compact-task latency. |
| `compactor_compact_sst_duration`               | histogram           | —                                | Time to compact a single SST. |
| `compactor_iter_scan_key_counts`               | counter       | `type`                           | Keys scanned during compaction. |
| `compactor_shared_buffer_to_sstable_size`      | histogram           | —                                | Bytes of shared-buffer memory materialised into one SST. |
| `compactor_fast_compact_bytes`                 | counter          | —                                | Bytes the fast-compactor was able to skip as raw blocks (i.e. blocks whose key range did not overlap and could be moved directly without re-encoding) during a `DYNAMIC` compaction task. Distinct from the `trivial-move` task type, which is its own thing — fast compaction is an optimisation **inside** dynamic-level compaction. |
| `compactor_remote_read_time`                   | histogram           | —                                | Per-block remote-read time during compaction. |
| `compactor_get_table_id_total_time_duration`   | histogram           | —                                | Time to resolve table ids at the start of a compaction task. |
| `compactor_sstable_*`                          | histogram (six)     | —                                | Per-compaction SST shape distributions: `compactor_sstable_block_size`, `compactor_sstable_file_size`, `compactor_sstable_bloom_filter_size`, `compactor_sstable_distinct_epoch_count`, `compactor_sstable_avg_key_size`, `compactor_sstable_avg_value_size`. Useful for tuning compaction. |

### 10.6 Version metadata and GC (tied to §3.3 alerts)

Already covered by the Storage Alerts table in §3.3. The series are:
`storage_current_version_id`, `storage_checkpoint_version_id`,
`storage_min_pinned_version_id`, `storage_version_size`,
`storage_delta_log_count`, `storage_stale_object_count`,
`storage_write_stop_compaction_groups`, plus the object-count /
object-size pairs `storage_{stale,old_version,current_version,total}_object_{count,size}`,
`storage_time_travel_object_count`, and the per-table change-log tracking
`storage_table_change_log_object_count`, `storage_table_change_log_object_size`,
`storage_table_change_log_min_epoch`.

---

## 11. Memory

### 11.1 Managed memory (LRU-driven cache eviction)

RisingWave's streaming executors share an LRU budget. A manager periodically
scans caches and evicts entries to keep total usage within budget.

| Metric                                | Type                      | Labels        | Use |
|---------------------------------------|---------------------------|---------------|------|
| `lru_runtime_loop_count`              | counter                | —             | Iterations of the LRU manager loop. `rate(...)` should be non-zero; flat line → manager stuck. |
| `lru_eviction_policy`                 | gauge                  | —             | Policy currently in effect (enum code). |
| `lru_latest_sequence`                 | gauge                  | —             | Highest sequence number assigned to an LRU entry so far. |
| `lru_watermark_sequence`              | gauge                  | —             | Current watermark sequence used by the evictor — everything below is a candidate for eviction. |
| `jemalloc_allocated_bytes`            | gauge                  | —             | jemalloc `stats.allocated` — bytes currently allocated. |
| `jemalloc_active_bytes`               | gauge                  | —             | jemalloc `stats.active` — bytes in active pages. |
| `jemalloc_resident_bytes`             | gauge                  | —             | jemalloc `stats.resident` — bytes resident in RAM. |
| `jemalloc_metadata_bytes`             | gauge                  | —             | jemalloc `stats.metadata` — heap metadata overhead. |
| `jvm_allocated_bytes`                 | gauge                  | —             | JVM heap allocated (only non-zero if a JNI-based connector with JVM is in play, e.g. Debezium CDC). |
| `jvm_active_bytes`                    | gauge                  | —             | JVM active bytes. |
| `stream_memory_usage`                 | gauge   | `actor_id`, `table_id`, `desc` | Estimated bytes held by each streaming executor's managed cache. `desc` distinguishes the cache kind (e.g. `join left`, `hash agg state`). |

### 11.2 Stream-operator cache hit rates

Most stateful streaming operators have per-executor cache hit/miss
counters. A low hit rate usually means the executor has more working-set
than fits in its share of the LRU budget, and remote reads dominate.

| Metric                                    | Type                      | Labels                                    | Cache for |
|-------------------------------------------|---------------------------|-------------------------------------------|-----------|
| `stream_join_lookup_total_count` / `_miss_count` | counter | `side`, `join_table_id`, `actor_id`, `fragment_id` | Hash-join probe-side cache. |
| `stream_join_insert_cache_miss_count`     | counter | `side`, `join_table_id`, `actor_id`, `fragment_id` | Join state writes that had to hit storage. |
| `stream_agg_lookup_total_count` / `_miss_count`  | counter | `table_id`, `actor_id`, `fragment_id`            | Hash-agg cache. |
| `stream_agg_state_cache_lookup_count` / `_miss_count` | counter | `table_id`, `actor_id`, `fragment_id`       | Per-group state cache (approx agg, top-N on-group, etc). |
| `stream_group_top_n_total_query_cache_count` / `_cache_miss_count` | counter | `table_id`, `actor_id`, `fragment_id` | Group TopN operator cache. |
| `stream_materialize_cache_total_count` / `stream_materialize_cache_hit_count` / `stream_materialize_data_exist_count` | counter | `actor_id`, `table_id`, `fragment_id` | Materialize executor upsert cache. Note that the third metric drops the `cache_` infix — it counts how often, on a write, the row already existed in storage (i.e. an UPSERT that becomes UPDATE rather than INSERT). |
| `stream_lookup_total_query_cache_count` / `stream_lookup_cache_miss_count` | counter | `table_id`, `actor_id`, `fragment_id` | **Lookup-executor** (index lookup join) cache total vs misses. Pair with `stream_lookup_cached_entry_count` (gauge) for current cached-row count. |
| `stream_temporal_join_total_query_cache_count` / `stream_temporal_join_cache_miss_count` | counter | `table_id`, `actor_id`, `fragment_id` | **Temporal-join** cache total vs misses. Pair with `stream_temporal_join_cached_entry_count` (gauge). Distinct executor from the lookup-join family above. |
| `stream_over_window_cached_entry_count` / `stream_over_window_cache_lookup_count` / `_cache_miss_count` / `stream_over_window_range_cache_left_miss_count` / `_right_miss_count` / `stream_over_window_accessed_entry_count` / `_compute_count` / `_same_output_count` | various | see TSV | Over-window operator internals. |

Compute hit rate as `1 - (miss_count / total_count)` over a rolling window.

### 11.3 Batch-side memory

| Metric                   | Type          | Labels | Meaning |
|--------------------------|---------------|--------|---------|
| `compute_batch_total_mem`| gauge  | —      | Sum of memory currently held by running batch tasks on this compute node. |
| `frontend_batch_total_mem` | gauge | —     | Batch-executor memory held by the frontend (for local query execution). |

Both are exposed as Prometheus `gauge`s. They use the `TrAdderGauge` Rust
type internally (a thread-safe add/subtract gauge) but on the wire they
look identical to any other gauge.

---

## 12. Process, CPU, network, tokio runtime

### 12.1 Process-level

Emitted by the process collector (`src/common/metrics/src/monitor/process.rs`):

| Metric                          | Type       | Meaning |
|---------------------------------|------------|---------|
| `process_cpu_seconds_total`     | counter | Total user+system CPU seconds used by this process. Divide by `process_cpu_core_num` to derive utilization per core (what the CPU Saturation alert in §3.2 does). |
| `process_cpu_core_num`          | gauge   | CPU cores visible to the process. |
| `process_resident_memory_bytes` | gauge   | Resident memory (RSS). |
| `process_virtual_memory_bytes`  | gauge   | Virtual memory size. |

### 12.2 Network connection pool

From `src/common/metrics/src/monitor/connection.rs`, these cover the gRPC
connection pools between components (meta ↔ compute, compute ↔ compute via
exchange, compute → compactor):

| Metric                      | Type             | Labels                      | Meaning |
|-----------------------------|------------------|-----------------------------|---------|
| `connection_count`          | gauge      | `connection_type`, `uri`    | Current open connections. |
| `connection_create_rate`    | counter    | `connection_type`, `uri`    | New connection openings. Churn here → upstream flapping or idle-timeout mistuning. |
| `connection_err_rate`       | counter    | `connection_type`, `uri`    | Connection-open errors. |
| `connection_read_rate`      | counter    | `connection_type`, `uri`    | Bytes read. |
| `connection_write_rate`     | counter    | `connection_type`, `uri`    | Bytes written. |
| `connection_reader_count`   | gauge      | `connection_type`, `uri`    | Readers currently active. |
| `connection_writer_count`   | gauge      | `connection_type`, `uri`    | Writers currently active. |
| `connection_io_err_rate`    | counter | `connection_type`, `uri`, `op_type`, `error_kind` | I/O errors on an open connection. `op_type` = `read` / `write`, `error_kind` classifies the I/O error. |

### 12.3 Stream exchange (inter-fragment shuffle)

| Metric                               | Type                      | Labels                                 | Meaning |
|--------------------------------------|---------------------------|----------------------------------------|---------|
| `stream_exchange_frag_send_size`     | counter             | `up_fragment_id`, `down_fragment_id`   | Bytes sent between two fragments. |
| `stream_exchange_frag_recv_size`     | counter | `up_fragment_id`, `down_fragment_id`   | Bytes received. A send/recv gap indicates in-flight data in the transport; a sustained gap indicates a slow or disconnected receiver. |
| `stream_fragment_channel_buffered_bytes` | gauge | `fragment_id`                         | Bytes buffered in the local exchange channels of a fragment. Gauge of local backpressure. |

### 12.4 Tokio runtime and actor scheduling

The core tokio runtime metrics are exposed on streaming actors:

| Metric                             | Type                      | Labels                       | Meaning |
|------------------------------------|---------------------------|------------------------------|---------|
| `stream_actor_poll_duration`       | counter | `actor_id`, `fragment_id`    | Cumulative time the actor was actively executing (tokio-reported). |
| `stream_actor_poll_cnt`            | counter | `actor_id`, `fragment_id`    | Number of polls. |
| `stream_actor_scheduled_duration`  | counter | `actor_id`, `fragment_id`    | Time the actor was scheduled but not yet running. |
| `stream_actor_scheduled_cnt`       | counter | `actor_id`, `fragment_id`    | Schedule events. |
| `stream_actor_idle_duration`       | counter | `actor_id`, `fragment_id`    | Idle time. |
| `stream_actor_idle_cnt`            | counter | `actor_id`, `fragment_id`    | Idle-count events. |
| `stream_actor_in_record_cnt`       | counter | `actor_id`, `fragment_id`, `upstream_fragment_id` | Total rows the actor received from upstream. |
| `stream_actor_out_record_cnt`      | counter | `actor_id`, `fragment_id`    | Total rows the actor emitted. |
| `stream_actor_current_epoch`       | gauge   | `actor_id`, `fragment_id`    | Current epoch the actor is processing. |
| `stream_actor_count`               | gauge   | `fragment_id`                | Number of actors currently materialised for the fragment (i.e. parallelism). |
| `stream_actor_output_buffer_blocking_duration_ns` | counter | `actor_id`, `fragment_id`, `downstream_fragment_id` | Backpressure (covered in §6). |
| `stream_actor_input_buffer_blocking_duration_ns`  | counter | `actor_id`, `fragment_id`, `upstream_fragment_id`   | Upstream-starvation mirror (covered in §6). |

Per-actor CPU attribution = `rate(stream_actor_poll_duration[$__rate_interval]) / 1e9`
— i.e. how many cores-worth of work the actor is doing.

---

## 13. Batch queries

Batch queries run on compute nodes alongside streaming. They have their own
smaller set of metrics:

| Metric                                  | Type          | Labels | Meaning |
|-----------------------------------------|---------------|--------|---------|
| `batch_task_num`                        | gauge      | —      | Batch tasks currently resident in memory. |
| `batch_heartbeat_worker_num`            | gauge      | —      | Workers sending heartbeats for batch tasks. |
| `batch_exchange_recv_row_number`        | counter    | —      | Rows received from upstream exchange in batch mode. |
| `batch_row_seq_scan_next_duration`      | histogram     | —      | Time to deserialize the next row during a row-oriented table scan. |
| `batch_spill_read_bytes`                | counter    | —      | Bytes read from spill files during batch execution. |
| `batch_spill_write_bytes`               | counter    | —      | Bytes written to spill files. |
| `distributed_running_query_num`         | gauge      | —      | Distributed-mode queries currently running on the frontend. |
| `distributed_rejected_query_counter`    | counter    | —      | Queries the scheduler rejected (typically because there are already too many running). |
| `distributed_completed_query_counter`   | counter    | —      | Queries that completed successfully in distributed mode. |
| `distributed_query_latency`             | histogram     | —      | Distributed-query end-to-end latency. |
| `frontend_query_counter_local_execution`| counter    | —      | Queries that executed entirely on the frontend (local mode). |
| `frontend_latency_local_execution`      | histogram     | —      | Local-mode latency. |

**Batch QPS (user dashboard Overview):**
```promql
rate(frontend_query_counter_local_execution[$__rate_interval])  # Local mode
rate(distributed_completed_query_counter[$__rate_interval])     # Distributed mode
```

---

## 14. UDF (user-defined functions)

| Metric                 | Type                       | Labels                                          | Meaning |
|------------------------|----------------------------|-------------------------------------------------|---------|
| `udf_success_count`    | counter  | `link`, `language`, `name`, `fragment_id`       | Successful UDF invocations. |
| `udf_failure_count`    | counter  | `link`, `language`, `name`, `fragment_id`       | Failed invocations. |
| `udf_retry_count`      | counter  | `link`, `language`, `name`, `fragment_id`       | Retried invocations (e.g. transient network / runtime error). |
| `udf_input_rows`       | counter  | `link`, `language`, `name`, `fragment_id`       | Rows sent as UDF input. |
| `udf_input_bytes`      | counter  | `link`, `language`, `name`, `fragment_id`       | Input bytes. |
| `udf_input_chunk_rows` | histogram   | `link`, `language`, `name`, `fragment_id`       | Histogram of rows per UDF call (batch size). Buckets: 1–1024 exponential. |
| `udf_latency`          | histogram   | `link`, `language`, `name`, `fragment_id`       | Invocation latency. Buckets: 1 µs – ~1000 s. |
| `udf_memory_usage`     | gauge    | `link`, `language`, `name`, `fragment_id`, `instance_id` | Memory held by a UDF runtime (Python, JavaScript, Wasm, Rust). Tracks leaks. |

Labels:
- `link` — the UDF's delivery channel (e.g. `python`, `wasm`, `arrow-flight:...`).
- `language` — the implementation language (`python`, `javascript`, `wasm`, `rust`).
- `name` — the SQL-level UDF name.
- `fragment_id` — which streaming fragment invoked it.

---

## 15. Vector search

Vector search metrics live on the state-store side (indexes are stored as
Hummock files; search runs against them). All names are prefixed
`state_store_vector_*`.

| Metric                                               | Type                      | Labels                                        | Meaning |
|------------------------------------------------------|---------------------------|-----------------------------------------------|---------|
| `state_store_vector_nearest_duration`                | histogram  | `table_id`, `top_n`, `ef_search`              | Total latency of a vector-nearest query issued to the state store. `top_n` and `ef_search` are the HNSW query parameters. |
| `state_store_vector_request_stats`                   | histogram  | `table_id`, `type`, `mode`, `top_n`, `ef`     | Per-request HNSW search internals. `type` ∈ `distances_computed` (count of distance comparisons) and `n_hops` (graph traversal hops). For object/block-level request counts use `state_store_vector_object_request_counts` instead. |
| `state_store_vector_object_request_counts`           | counter | `table_id`, `type`, `mode`                    | Object-storage requests issued while serving vector queries. |
| `state_store_vector_hnsw_graph_level_node_count`     | gauge   | `table_id`, `level`                           | Number of nodes per HNSW graph level. |
| `state_store_vector_index_file_count`                | gauge   | `table_id`                                    | Number of vector index files. |
| `state_store_vector_index_file_size`                 | gauge   | `table_id`, `type`                            | Total bytes of vector index files by `type` ∈ `vector_file_data` (raw vectors), `vector_file_meta` (file-level metadata), `hnsw_graph_file` (HNSW graph). |
| `state_store_vector_data_cache_size`                 | gauge                  | —                                             | Current bytes used by the vector-data cache (sampled from `vector_block_cache.usage()`). Compare against capacity for headroom. |
| `state_store_vector_data_cache_usage_ratio`          | gauge                     | —                                             | Vector-data cache usage / capacity. |
| `state_store_vector_meta_cache_size`                 | gauge                  | —                                             | Current bytes used by the vector-meta cache (`vector_meta_cache.usage()`). |
| `state_store_vector_meta_cache_usage_ratio`          | gauge                     | —                                             | Vector-meta cache usage / capacity. |

See `grafana/dashboard/dev/vector_search.py` for the curated panels.

---

## 16. Iceberg compaction (sink-side table maintenance)

Beyond the sink-writer metrics in §9.4, RisingWave runs an optional
Iceberg-side compactor that rewrites small files / delete files into larger
ones for read efficiency. The metrics below come from the external
`iceberg-compaction-core` crate (not from `src/`), so they appear on
`/metrics` only when this feature is enabled; they are **not** in
`inventory.tsv` (the inventory only scans `src/`).

Metric names referenced by `grafana/dashboard/dev/iceberg_compaction_metrics.py`:

| Metric                                                | Use |
|-------------------------------------------------------|------|
| `iceberg_compaction_commit_counter`                   | Counter of Iceberg commits performed. |
| `iceberg_compaction_commit_duration`                  | Histogram — time spent on the Iceberg commit. |
| `iceberg_compaction_duration`                         | Histogram — end-to-end task duration. |
| `iceberg_compaction_input_files_count`                | Files read per task. |
| `iceberg_compaction_output_files_count`               | Files produced per task. |
| `iceberg_compaction_input_bytes_total`                | Bytes read. |
| `iceberg_compaction_output_bytes_total`               | Bytes written. |
| `iceberg_compaction_executor_error_counter`           | Task-level errors. |
| `iceberg_compaction_datafusion_batch_fetch_duration`  | DataFusion batch fetch latency (reader side). |
| `iceberg_compaction_datafusion_batch_write_duration`  | DataFusion batch write latency (writer side). |
| `iceberg_compaction_datafusion_batch_bytes_dist`      | Batch size distribution. |
| `iceberg_compaction_datafusion_batch_row_count_dist`  | Batch row-count distribution. |
| `iceberg_compaction_datafusion_records_processed_total` | Cumulative records processed. |
| `iceberg_compaction_datafusion_bytes_processed_total` | Cumulative bytes processed. |

For exact types and labels, inspect the metric at runtime via `/metrics` or
check the `iceberg-compaction-core` crate source.

---

## 17. Meta, DDL, backup, recovery

| Metric                                     | Type                      | Labels                             | Meaning |
|--------------------------------------------|---------------------------|------------------------------------|---------|
| `meta_num`                                 | gauge               | `worker_addr`, `role`              | One series per running meta replica, per role. Count of series = meta cluster size (leader + followers). |
| `meta_grpc_duration_seconds`               | histogram              | `path`                             | Meta gRPC request latency by RPC path. |
| `storage_max_committed_epoch`              | gauge                  | —                                  | Largest committed epoch across all `state_table_info` entries (cluster-wide max). |
| `storage_min_committed_epoch`              | gauge                  | —                                  | Smallest committed epoch across all `state_table_info` entries (cluster-wide min — single unlabeled series, not per database). A growing gap between min and max indicates one or more state tables falling behind. |
| `hummock_manager_lock_time`                | histogram              | `lock_name`, `lock_type`           | Time spent **waiting to acquire** a HummockManager rwlock. `lock_name` identifies the lock; `lock_type` ∈ `read` / `write`. Help string: "latency for hummock manager to acquire the rwlock". High values → meta lock contention. |
| `meta_hummock_manager_real_process_time`   | histogram              | `method`                           | Time spent on the actual meta operation **after** the lock has been acquired, per method name. Together with `hummock_manager_lock_time`: lock_time + real_process_time = total meta-call latency. |
| `storage_full_gc_trigger_count`            | gauge                  | —                                  | Number of attempts to trigger full GC. |
| `backup_job_count`                         | counter                | —                                  | Backup jobs run. |
| `backup_job_latency`                       | histogram              | `state`                            | Backup latency by state. |
| `recovery_latency`                         | histogram              | `recovery_type`                    | §5.5. |
| `recovery_failure_cnt`                     | counter             | `recovery_type`                    | §5.5. |
| `auto_schema_change_success_cnt`           | counter | `table_id`, `table_name`           | Automatic schema-change successes. |
| `auto_schema_change_failure_cnt`           | counter | `table_id`, `table_name`           | Automatic schema-change failures. |
| `auto_schema_change_latency`               | histogram  | `table_id`, `table_name`           | Auto schema-change duration. |
| `database_info`                            | gauge               | `database_id`, `database_name`     | Info join metric for databases. |
| `relation_info`                            | gauge               | `id`, `database`, `schema`, `name`, `resource_group`, `type` | Info join metric for relations (MV / table / index / source / sink). Use for "humanise the id" joins. |
| `system_param_info`                        | gauge               | `name`, `value`                    | Current system parameter values (one series per (name, value) pair). |

---

## 18. Frontend and subscription cursors

| Metric                                      | Type          | Labels                  | Meaning |
|---------------------------------------------|---------------|-------------------------|---------|
| `frontend_active_sessions`                  | gauge      | —                       | Current PostgreSQL sessions connected to this frontend. |
| `subscription_cursor_nums`                  | gauge      | —                       | Subscription cursors currently open. |
| `invalid_subscription_cursor_nums`          | gauge      | —                       | Cursors in an invalid state. |
| `subscription_cursor_error_count`           | counter    | —                       | Subscription-cursor errors. |
| `subscription_cursor_declare_duration`      | histogram  | `subscription_name`     | Time to DECLARE a subscription cursor. |
| `subscription_cursor_query_duration`        | histogram  | `subscription_name`     | Time to run the next-row query. |
| `subscription_cursor_fetch_duration`        | histogram  | `subscription_name`     | Time to FETCH. |
| `subscription_cursor_last_fetch_duration`   | histogram  | `subscription_name`     | Time since the last fetch call. |

Plus three datafusion-specific metrics emitted only when the datafusion
batch engine is enabled — the counters `datafusion_completed_query_counter`
and `datafusion_failed_query_counter`, plus the histogram
`datafusion_latency`. All three live in `src/frontend/src/datafusion/metrics.rs`.

---

## 19. Appendix B: complete metric index

Every metric registered in the codebase has a row in
[`inventory.tsv`](./inventory.tsv) with its exact name,
Prometheus type, labels, help string, and source file. Sections 4
through 18 of this document narrate the operationally-important subset; the
TSV is the authoritative list.

A few notes on reading the TSV:

1. **The `type` column shows the Prometheus type** — `counter`, `gauge`, or
   `histogram` — which is what scrape consumers see on `/metrics`. The Rust
   source uses richer types (e.g. `IntCounter`, `LabelGuardedIntCounterVec`)
   to enforce label safety / numeric width inside the process, but those
   distinctions don't reach the wire format.
2. **Histograms expose three series** on `/metrics`:
   `<name>_bucket{le="…"}`, `<name>_sum`, `<name>_count`. Query with
   `histogram_quantile(q, sum(rate(<name>_bucket[$__rate_interval])) by (le, …))`.
3. **Counters never decrease** — use `rate()` or `increase()` to get
   useful numbers. Gauges can go up or down and are usually read as-is.
4. **Kafka `rdkafka_<PATH>_*` window metrics** appear as templates in the
   TSV. The `<PATH>` placeholder expands to one of the six window-stat paths
   listed in §8.3; with 14 suffixes each this is 84 concrete metric families.
5. **Kinesis, CDC, Iceberg, UDF, vector-search** metrics are registered on
   the components that actually use the corresponding feature (typically
   compute nodes for streaming sources/sinks; meta for source-status
   tracking) — they aren't guaranteed to appear on every component's
   `/metrics` endpoint. Within a component, the metric family is registered
   lazily on first use, so a metric absent from a freshly-started node
   doesn't necessarily mean the feature is disabled.

The inventory has 508 metric rows, all with unique names. 14 of those rows
are `rdkafka_<PATH>_<stat>` templates; each template expands at runtime to
6 concrete metrics (one per `<PATH>` value listed in §8.3), so the
exposed-on-`/metrics` count is 508 − 14 + (14 × 6) = 578 distinct families
when the Kafka source is in use. If a metric name you expect to see is
absent, either the feature isn't compiled in, or it was renamed; grep the
Rust source (`src/**/monitor/*.rs`, `src/**/metrics.rs`) to confirm.

---

## Document provenance and maintenance

`inventory.tsv` is generated mechanically from `src/**/*.rs` by a
small Python extractor (it parses all registration macros + direct
constructions, resolves `let labels = ...` / `let opts = ...` / closure-
based name templates, skips `#[cfg(test)]` blocks and wrapper definitions —
no LLM involvement). The intended workflow:

1. After a metric registration is added or changed in Rust, rerun the
   extractor to regenerate `docs/metrics/inventory.tsv`.
2. CI runs the same extractor and diffs against the committed TSV — any
   drift fails the build, the same way `cargo fmt` / `clippy` does.
3. The narrative sections of this document are reviewed by a human (or
   another LLM, like we did with `codex` here) when a new metric needs an
   operational story; the TSV alone is enough for SRE / dashboard / alert
   tooling.

The extractor and the CI hook live at:

- `docs/metrics/extract.py` — the extractor (run with `python3 docs/metrics/extract.py`).
- `ci/scripts/check-metrics-inventory.sh` — runs the extractor and `diff`s
  against `docs/metrics/inventory.tsv`; non-zero
  exit on drift.
