// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::fmt::Write;
use std::sync::Arc;

use itertools::Itertools;
use prometheus_http_query::response::Data::Vector;
use risingwave_common::id::ObjectId;
use risingwave_common::system_param::AdaptiveParallelismStrategy;
use risingwave_common::system_param::adaptive_parallelism_strategy::parse_strategy;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::types::Timestamptz;
use risingwave_common::util::StackTraceResponseExt;
use risingwave_common::util::epoch::Epoch;
use risingwave_hummock_sdk::level::Level;
use risingwave_hummock_sdk::{CompactionGroupId, HummockSstableId};
use risingwave_license::LicenseManager;
use risingwave_meta_model::{JobStatus, StreamingParallelism};
use risingwave_pb::catalog::table::PbTableType;
use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::EventLog;
use risingwave_pb::meta::event_log::Event;
use risingwave_pb::monitor_service::stack_trace_request::ActorTracesFormat;
use risingwave_sqlparser::ast::RedactSqlOptionKeywordsRef;
use risingwave_sqlparser::parser::Parser;
use serde_json::json;
use thiserror_ext::AsReport;

use crate::MetaResult;
use crate::controller::system_param::SystemParamsControllerRef;
use crate::hummock::HummockManagerRef;
use crate::manager::MetadataManager;
use crate::manager::event_log::EventLogManagerRef;
use crate::manager::iceberg_compaction::IcebergCompactionManagerRef;
use crate::rpc::await_tree::dump_cluster_await_tree;

pub type DiagnoseCommandRef = Arc<DiagnoseCommand>;

pub struct DiagnoseCommand {
    metadata_manager: MetadataManager,
    await_tree_reg: await_tree::Registry,
    hummock_manger: HummockManagerRef,
    iceberg_compaction_manager: IcebergCompactionManagerRef,
    event_log_manager: EventLogManagerRef,
    prometheus_client: Option<prometheus_http_query::Client>,
    prometheus_selector: String,
    redact_sql_option_keywords: RedactSqlOptionKeywordsRef,
    system_params_controller: SystemParamsControllerRef,
}

impl DiagnoseCommand {
    pub fn new(
        metadata_manager: MetadataManager,
        await_tree_reg: await_tree::Registry,
        hummock_manger: HummockManagerRef,
        iceberg_compaction_manager: IcebergCompactionManagerRef,
        event_log_manager: EventLogManagerRef,
        prometheus_client: Option<prometheus_http_query::Client>,
        prometheus_selector: String,
        redact_sql_option_keywords: RedactSqlOptionKeywordsRef,
        system_params_controller: SystemParamsControllerRef,
    ) -> Self {
        Self {
            metadata_manager,
            await_tree_reg,
            hummock_manger,
            iceberg_compaction_manager,
            event_log_manager,
            prometheus_client,
            prometheus_selector,
            redact_sql_option_keywords,
            system_params_controller,
        }
    }

    pub async fn report(&self, actor_traces_format: ActorTracesFormat) -> String {
        let mut report = String::new();
        let _ = writeln!(
            report,
            "report created at: {}\nversion: {}",
            chrono::DateTime::<chrono::offset::Utc>::from(std::time::SystemTime::now()),
            risingwave_common::current_cluster_version(),
        );
        let _ = writeln!(report);
        self.write_license(&mut report);
        let _ = writeln!(report);
        self.write_catalog(&mut report).await;
        let _ = writeln!(report);
        self.write_worker_nodes(&mut report).await;
        let _ = writeln!(report);
        self.write_streaming_prometheus(&mut report).await;
        let _ = writeln!(report);
        self.write_storage(&mut report).await;
        let _ = writeln!(report);
        self.write_iceberg_compaction_schedule(&mut report).await;
        let _ = writeln!(report);
        self.write_event_logs(&mut report);
        let _ = writeln!(report);
        self.write_params(&mut report).await;
        let _ = writeln!(report);
        self.write_await_tree(&mut report, actor_traces_format)
            .await;

        report
    }

    async fn write_catalog(&self, s: &mut String) {
        self.write_catalog_inner(s).await;
        let _ = self.write_table_definition(s).await.inspect_err(|e| {
            tracing::warn!(
                error = e.to_report_string(),
                "failed to display table definition"
            )
        });
    }

    async fn write_catalog_inner(&self, s: &mut String) {
        let stats = self.metadata_manager.catalog_controller.stats().await;

        let stat = match stats {
            Ok(stat) => stat,
            Err(err) => {
                tracing::warn!(error=?err.as_report(), "failed to get catalog stats");
                return;
            }
        };
        let _ = writeln!(s, "number of database: {}", stat.database_num);
        let _ = writeln!(s, "number of fragment: {}", stat.streaming_job_num);
        let _ = writeln!(s, "number of actor: {}", stat.actor_num);
        let _ = writeln!(s, "number of source: {}", stat.source_num);
        let _ = writeln!(s, "number of table: {}", stat.table_num);
        let _ = writeln!(s, "number of materialized view: {}", stat.mview_num);
        let _ = writeln!(s, "number of sink: {}", stat.sink_num);
        let _ = writeln!(s, "number of index: {}", stat.index_num);
        let _ = writeln!(s, "number of function: {}", stat.function_num);

        self.write_databases(s).await;
        self.write_schemas(s).await;
    }

    async fn write_databases(&self, s: &mut String) {
        let databases = match self
            .metadata_manager
            .catalog_controller
            .list_databases()
            .await
        {
            Ok(databases) => databases,
            Err(err) => {
                tracing::warn!(error=?err.as_report(), "failed to list databases");
                return;
            }
        };

        use comfy_table::{Row, Table};
        let mut table = Table::new();
        table.set_header({
            let mut row = Row::new();
            row.add_cell("id".into());
            row.add_cell("name".into());
            row.add_cell("resource_group".into());
            row.add_cell("barrier_interval_ms".into());
            row.add_cell("checkpoint_frequency".into());
            row
        });
        for db in databases {
            let mut row = Row::new();
            row.add_cell(db.id.into());
            row.add_cell(db.name.into());
            row.add_cell(db.resource_group.into());
            row.add_cell(
                db.barrier_interval_ms
                    .map(|v| v.to_string())
                    .unwrap_or("default".into())
                    .into(),
            );
            row.add_cell(
                db.checkpoint_frequency
                    .map(|v| v.to_string())
                    .unwrap_or("default".into())
                    .into(),
            );
            table.add_row(row);
        }

        let _ = writeln!(s, "DATABASE");
        let _ = writeln!(s, "{table}");
    }

    async fn write_schemas(&self, s: &mut String) {
        let schemas = match self
            .metadata_manager
            .catalog_controller
            .list_schemas()
            .await
        {
            Ok(schemas) => schemas,
            Err(err) => {
                tracing::warn!(error=?err.as_report(), "failed to list schemas");
                return;
            }
        };

        use comfy_table::{Row, Table};
        let mut table = Table::new();
        table.set_header({
            let mut row = Row::new();
            row.add_cell("id".into());
            row.add_cell("database_id".into());
            row.add_cell("name".into());
            row
        });
        for schema in schemas {
            let mut row = Row::new();
            row.add_cell(schema.id.into());
            row.add_cell(schema.database_id.into());
            row.add_cell(schema.name.into());
            table.add_row(row);
        }

        let _ = writeln!(s, "SCHEMA");
        let _ = writeln!(s, "{table}");
    }

    async fn write_worker_nodes(&self, s: &mut String) {
        let Ok(worker_actor_count) = self.metadata_manager.worker_actor_count() else {
            tracing::warn!("failed to get worker actor count");
            return;
        };

        use comfy_table::{Row, Table};
        let Ok(worker_nodes) = self.metadata_manager.list_worker_node(None, None).await else {
            tracing::warn!("failed to get worker nodes");
            return;
        };
        let mut table = Table::new();
        table.set_header({
            let mut row = Row::new();
            row.add_cell("id".into());
            row.add_cell("host".into());
            row.add_cell("hostname".into());
            row.add_cell("type".into());
            row.add_cell("state".into());
            row.add_cell("parallelism".into());
            row.add_cell("resource_group".into());
            row.add_cell("is_streaming".into());
            row.add_cell("is_serving".into());
            row.add_cell("is_iceberg_compactor".into());
            row.add_cell("rw_version".into());
            row.add_cell("total_memory_bytes".into());
            row.add_cell("total_cpu_cores".into());
            row.add_cell("started_at".into());
            row.add_cell("actor_count".into());
            row
        });
        for worker_node in worker_nodes {
            let mut row = Row::new();
            row.add_cell(worker_node.id.into());
            try_add_cell(
                &mut row,
                worker_node
                    .host
                    .as_ref()
                    .map(|h| format!("{}:{}", h.host, h.port)),
            );
            try_add_cell(
                &mut row,
                worker_node.resource.as_ref().map(|r| r.hostname.clone()),
            );
            try_add_cell(
                &mut row,
                worker_node.get_type().ok().map(|t| t.as_str_name()),
            );
            try_add_cell(
                &mut row,
                worker_node.get_state().ok().map(|s| s.as_str_name()),
            );
            try_add_cell(&mut row, worker_node.parallelism());
            try_add_cell(
                &mut row,
                worker_node
                    .property
                    .as_ref()
                    .map(|p| p.resource_group.clone().unwrap_or("".to_owned())),
            );
            // is_streaming and is_serving are only meaningful for ComputeNode
            let (is_streaming, is_serving) = {
                if let Ok(t) = worker_node.get_type()
                    && t == WorkerType::ComputeNode
                {
                    (
                        worker_node.property.as_ref().map(|p| p.is_streaming),
                        worker_node.property.as_ref().map(|p| p.is_serving),
                    )
                } else {
                    (None, None)
                }
            };
            try_add_cell(&mut row, is_streaming);
            try_add_cell(&mut row, is_serving);
            // is_iceberg_compactor is only meaningful for Compactor worker type
            let is_iceberg_compactor = {
                if let Ok(t) = worker_node.get_type()
                    && t == WorkerType::Compactor
                {
                    worker_node
                        .property
                        .as_ref()
                        .map(|p| p.is_iceberg_compactor)
                } else {
                    None
                }
            };
            try_add_cell(&mut row, is_iceberg_compactor);
            try_add_cell(
                &mut row,
                worker_node.resource.as_ref().map(|r| r.rw_version.clone()),
            );
            try_add_cell(
                &mut row,
                worker_node.resource.as_ref().map(|r| r.total_memory_bytes),
            );
            try_add_cell(
                &mut row,
                worker_node.resource.as_ref().map(|r| r.total_cpu_cores),
            );
            try_add_cell(
                &mut row,
                worker_node
                    .started_at
                    .and_then(|ts| Timestamptz::from_secs(ts as _).map(|t| t.to_string())),
            );
            let actor_count = {
                if let Ok(t) = worker_node.get_type()
                    && t != WorkerType::ComputeNode
                {
                    None
                } else {
                    match worker_actor_count.get(&worker_node.id) {
                        None => Some(0),
                        Some(c) => Some(*c),
                    }
                }
            };
            try_add_cell(&mut row, actor_count);
            table.add_row(row);
        }
        let _ = writeln!(s, "{table}");
    }

    fn write_event_logs(&self, s: &mut String) {
        let event_logs = self
            .event_log_manager
            .list_event_logs()
            .into_iter()
            .sorted_by(|a, b| {
                a.timestamp
                    .unwrap_or(0)
                    .cmp(&b.timestamp.unwrap_or(0))
                    .reverse()
            })
            .collect_vec();

        let _ = writeln!(s, "latest barrier completions");
        Self::write_event_logs_impl(
            s,
            event_logs.iter(),
            |e| {
                let Event::BarrierComplete(info) = e else {
                    return None;
                };
                Some(json!(info).to_string())
            },
            Some(10),
        );

        let _ = writeln!(s);
        let _ = writeln!(s, "latest barrier collection failures");
        Self::write_event_logs_impl(
            s,
            event_logs.iter(),
            |e| {
                let Event::CollectBarrierFail(info) = e else {
                    return None;
                };
                Some(json!(info).to_string())
            },
            Some(3),
        );

        let _ = writeln!(s);
        let _ = writeln!(s, "latest barrier injection failures");
        Self::write_event_logs_impl(
            s,
            event_logs.iter(),
            |e| {
                let Event::InjectBarrierFail(info) = e else {
                    return None;
                };
                Some(json!(info).to_string())
            },
            Some(3),
        );

        let _ = writeln!(s);
        let _ = writeln!(s, "latest worker node panics");
        Self::write_event_logs_impl(
            s,
            event_logs.iter(),
            |e| {
                let Event::WorkerNodePanic(info) = e else {
                    return None;
                };
                Some(json!(info).to_string())
            },
            Some(10),
        );

        let _ = writeln!(s);
        let _ = writeln!(s, "latest create stream job failures");
        Self::write_event_logs_impl(
            s,
            event_logs.iter(),
            |e| {
                let Event::CreateStreamJobFail(info) = e else {
                    return None;
                };
                Some(json!(info).to_string())
            },
            Some(3),
        );

        let _ = writeln!(s);
        let _ = writeln!(s, "latest dirty stream job clear-ups");
        Self::write_event_logs_impl(
            s,
            event_logs.iter(),
            |e| {
                let Event::DirtyStreamJobClear(info) = e else {
                    return None;
                };
                Some(json!(info).to_string())
            },
            Some(3),
        );
    }

    fn write_event_logs_impl<'a, F>(
        s: &mut String,
        event_logs: impl Iterator<Item = &'a EventLog>,
        get_event_info: F,
        limit: Option<usize>,
    ) where
        F: Fn(&Event) -> Option<String>,
    {
        use comfy_table::{Row, Table};
        let mut table = Table::new();
        table.set_header({
            let mut row = Row::new();
            row.add_cell("created_at".into());
            row.add_cell("info".into());
            row
        });
        let mut row_count = 0;
        for event_log in event_logs {
            let Some(ref inner) = event_log.event else {
                continue;
            };
            if let Some(limit) = limit
                && row_count >= limit
            {
                break;
            }
            let mut row = Row::new();
            let ts = event_log
                .timestamp
                .and_then(|ts| Timestamptz::from_millis(ts as _).map(|ts| ts.to_string()));
            try_add_cell(&mut row, ts);
            if let Some(info) = get_event_info(inner) {
                row.add_cell(info.into());
                row_count += 1;
            } else {
                continue;
            }
            table.add_row(row);
        }
        let _ = writeln!(s, "{table}");
    }

    async fn write_storage(&self, s: &mut String) {
        let mut sst_num = 0;
        let mut sst_total_file_size = 0;
        let back_pressured_compaction_groups = self
            .hummock_manger
            .write_limits()
            .await
            .into_iter()
            .filter_map(|(k, v)| {
                if v.table_ids.is_empty() {
                    None
                } else {
                    Some(k)
                }
            })
            .join(",");
        if !back_pressured_compaction_groups.is_empty() {
            let _ = writeln!(
                s,
                "back pressured compaction groups: {back_pressured_compaction_groups}"
            );
        }

        #[derive(PartialEq, Eq)]
        struct SstableSort {
            compaction_group_id: CompactionGroupId,
            sst_id: HummockSstableId,
            delete_ratio: u64,
        }
        impl PartialOrd for SstableSort {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for SstableSort {
            fn cmp(&self, other: &Self) -> Ordering {
                self.delete_ratio.cmp(&other.delete_ratio)
            }
        }
        fn top_k_sstables(
            top_k: usize,
            heap: &mut BinaryHeap<Reverse<SstableSort>>,
            e: SstableSort,
        ) {
            if heap.len() < top_k {
                heap.push(Reverse(e));
            } else if let Some(mut p) = heap.peek_mut()
                && e.delete_ratio > p.0.delete_ratio
            {
                *p = Reverse(e);
            }
        }

        let top_k = 10;
        let mut top_tombstone_delete_sst = BinaryHeap::with_capacity(top_k);
        let compaction_group_num = self
            .hummock_manger
            .on_current_version(|version| {
                for compaction_group in version.levels.values() {
                    let mut visit_level = |level: &Level| {
                        sst_num += level.table_infos.len();
                        sst_total_file_size +=
                            level.table_infos.iter().map(|t| t.sst_size).sum::<u64>();
                        for sst in &level.table_infos {
                            if sst.total_key_count == 0 {
                                continue;
                            }
                            let tombstone_delete_ratio =
                                sst.stale_key_count * 10000 / sst.total_key_count;
                            let e = SstableSort {
                                compaction_group_id: compaction_group.group_id,
                                sst_id: sst.sst_id,
                                delete_ratio: tombstone_delete_ratio,
                            };
                            top_k_sstables(top_k, &mut top_tombstone_delete_sst, e);
                        }
                    };
                    let l0 = &compaction_group.l0;
                    // FIXME: why chaining levels iter leads to segmentation fault?
                    for level in &l0.sub_levels {
                        visit_level(level);
                    }
                    for level in &compaction_group.levels {
                        visit_level(level);
                    }
                }
                version.levels.len()
            })
            .await;

        let _ = writeln!(s, "number of SSTables: {sst_num}");
        let _ = writeln!(s, "total size of SSTables (byte): {sst_total_file_size}");
        let _ = writeln!(s, "number of compaction groups: {compaction_group_num}");
        use comfy_table::{Row, Table};
        fn format_table(heap: BinaryHeap<Reverse<SstableSort>>) -> Table {
            let mut table = Table::new();
            table.set_header({
                let mut row = Row::new();
                row.add_cell("compaction group id".into());
                row.add_cell("sstable id".into());
                row.add_cell("delete ratio".into());
                row
            });
            for sst in &heap.into_sorted_vec() {
                let mut row = Row::new();
                row.add_cell(sst.0.compaction_group_id.into());
                row.add_cell(sst.0.sst_id.into());
                row.add_cell(format!("{:.2}%", sst.0.delete_ratio as f64 / 100.0).into());
                table.add_row(row);
            }
            table
        }
        let _ = writeln!(s);
        let _ = writeln!(s, "top tombstone delete ratio");
        let _ = writeln!(s, "{}", format_table(top_tombstone_delete_sst));
        let _ = writeln!(s);

        let _ = writeln!(s);
        self.write_storage_prometheus(s).await;
    }

    async fn write_iceberg_compaction_schedule(&self, s: &mut String) {
        use comfy_table::{Row, Table};

        let sink_names = match self.metadata_manager.catalog_controller.list_sinks().await {
            Ok(sinks) => sinks
                .into_iter()
                .map(|sink| (sink.id.as_raw_id(), sink.name))
                .collect::<BTreeMap<u32, String>>(),
            Err(err) => {
                tracing::warn!(error=?err.as_report(), "failed to list sinks");
                return;
            }
        };

        let statuses = self.iceberg_compaction_manager.list_compaction_statuses();

        let _ = writeln!(s, "ICEBERG COMPACTION SCHEDULE");

        let mut table = Table::new();
        table.set_header({
            let mut row = Row::new();
            row.add_cell("sink_id".into());
            row.add_cell("sink_name".into());
            row.add_cell("task_type".into());
            row.add_cell("schedule_state".into());
            row.add_cell("trigger_interval_sec".into());
            row.add_cell("trigger_snapshot_count".into());
            row.add_cell("pending_snapshot_count".into());
            row.add_cell("next_compaction_after_sec".into());
            row.add_cell("is_triggerable".into());
            row
        });

        for status in statuses {
            let mut row = Row::new();
            row.add_cell(status.sink_id.as_raw_id().into());
            try_add_cell(
                &mut row,
                sink_names.get(&status.sink_id.as_raw_id()).cloned(),
            );
            row.add_cell(status.task_type.into());
            row.add_cell(status.schedule_state.into());
            row.add_cell(status.trigger_interval_sec.into());
            row.add_cell((status.trigger_snapshot_count as u64).into());
            try_add_cell(
                &mut row,
                status.pending_snapshot_count.map(|count| count as u64),
            );
            try_add_cell(&mut row, status.next_compaction_after_sec);
            row.add_cell(status.is_triggerable.into());
            table.add_row(row);
        }

        let _ = writeln!(s, "{table}");
    }

    async fn write_streaming_prometheus(&self, s: &mut String) {
        let _ = writeln!(s, "top sources by throughput (rows/s)");
        let query = format!(
            "topk(3, sum(rate(stream_source_output_rows_counts{{{}}}[10m]))by (source_name))",
            self.prometheus_selector
        );
        self.write_instant_vector_impl(s, &query, vec!["source_name"])
            .await;

        let _ = writeln!(s);
        let _ = writeln!(s, "top materialized views by throughput (rows/s)");
        let query = format!(
            "topk(3, sum(rate(stream_mview_input_row_count{{{}}}[10m]))by (table_id))",
            self.prometheus_selector
        );
        self.write_instant_vector_impl(s, &query, vec!["table_id"])
            .await;

        let _ = writeln!(s);
        let _ = writeln!(s, "top join executor by matched rows");
        let query = format!(
            "topk(3, histogram_quantile(0.9, sum(rate(stream_join_matched_join_keys_bucket{{{}}}[10m])) by (le, fragment_id, table_id)))",
            self.prometheus_selector
        );
        self.write_instant_vector_impl(s, &query, vec!["table_id", "fragment_id"])
            .await;
    }

    async fn write_storage_prometheus(&self, s: &mut String) {
        let _ = writeln!(s, "top Hummock Get by duration (second)");
        let query = format!(
            "topk(3, histogram_quantile(0.9, sum(rate(state_store_get_duration_bucket{{{}}}[10m])) by (le, table_id)))",
            self.prometheus_selector
        );
        self.write_instant_vector_impl(s, &query, vec!["table_id"])
            .await;

        let _ = writeln!(s);
        let _ = writeln!(s, "top Hummock Iter Init by duration (second)");
        let query = format!(
            "topk(3, histogram_quantile(0.9, sum(rate(state_store_iter_init_duration_bucket{{{}}}[10m])) by (le, table_id)))",
            self.prometheus_selector
        );
        self.write_instant_vector_impl(s, &query, vec!["table_id"])
            .await;

        let _ = writeln!(s);
        let _ = writeln!(s, "top table commit flush by size (byte)");
        let query = format!(
            "topk(3, sum(rate(storage_commit_write_throughput{{{}}}[10m])) by (table_id))",
            self.prometheus_selector
        );
        self.write_instant_vector_impl(s, &query, vec!["table_id"])
            .await;

        let _ = writeln!(s);
        let _ = writeln!(s, "object store read throughput (bytes/s)");
        let query = format!(
            "sum(rate(object_store_read_bytes{{{}}}[10m])) by (job, instance)",
            merge_prometheus_selector([&self.prometheus_selector, "job=~\"compute|compactor\""])
        );
        self.write_instant_vector_impl(s, &query, vec!["job", "instance"])
            .await;

        let _ = writeln!(s);
        let _ = writeln!(s, "object store write throughput (bytes/s)");
        let query = format!(
            "sum(rate(object_store_write_bytes{{{}}}[10m])) by (job, instance)",
            merge_prometheus_selector([&self.prometheus_selector, "job=~\"compute|compactor\""])
        );
        self.write_instant_vector_impl(s, &query, vec!["job", "instance"])
            .await;

        let _ = writeln!(s);
        let _ = writeln!(s, "object store operation rate");
        let query = format!(
            "sum(rate(object_store_operation_latency_count{{{}}}[10m])) by (le, type, job, instance)",
            merge_prometheus_selector([
                &self.prometheus_selector,
                "job=~\"compute|compactor\", type!~\"streaming_upload_write_bytes|streaming_read_read_bytes|streaming_read\""
            ])
        );
        self.write_instant_vector_impl(s, &query, vec!["type", "job", "instance"])
            .await;

        let _ = writeln!(s);
        let _ = writeln!(s, "object store operation duration (second)");
        let query = format!(
            "histogram_quantile(0.9, sum(rate(object_store_operation_latency_bucket{{{}}}[10m])) by (le, type, job, instance))",
            merge_prometheus_selector([
                &self.prometheus_selector,
                "job=~\"compute|compactor\", type!~\"streaming_upload_write_bytes|streaming_read\""
            ])
        );
        self.write_instant_vector_impl(s, &query, vec!["type", "job", "instance"])
            .await;
    }

    async fn write_instant_vector_impl(&self, s: &mut String, query: &str, labels: Vec<&str>) {
        let Some(ref client) = self.prometheus_client else {
            return;
        };
        if let Ok(Vector(instant_vec)) = client
            .query(query)
            .get()
            .await
            .map(|result| result.into_inner().0)
        {
            for i in instant_vec {
                let l = labels
                    .iter()
                    .map(|label| {
                        format!(
                            "{}={}",
                            *label,
                            i.metric()
                                .get(*label)
                                .map(|s| s.as_str())
                                .unwrap_or_default()
                        )
                    })
                    .join(",");
                let _ = writeln!(s, "{}: {:.3}", l, i.sample().value());
            }
        }
    }

    async fn write_await_tree(&self, s: &mut String, actor_traces_format: ActorTracesFormat) {
        let all = dump_cluster_await_tree(
            &self.metadata_manager,
            &self.await_tree_reg,
            actor_traces_format,
        )
        .await;

        if let Ok(all) = all {
            write!(s, "{}", all.output()).unwrap();
        } else {
            tracing::warn!("failed to dump await tree");
        }
    }

    async fn write_table_definition(&self, s: &mut String) -> MetaResult<()> {
        let sources = self
            .metadata_manager
            .catalog_controller
            .list_sources()
            .await?
            .into_iter()
            .map(|s| {
                (
                    s.id.into(),
                    (
                        s.name,
                        s.database_id,
                        s.schema_id,
                        s.definition,
                        s.created_at_epoch,
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut user_tables = BTreeMap::new();
        let mut mvs = BTreeMap::new();
        let mut indexes = BTreeMap::new();
        let mut internal_tables = BTreeMap::new();
        {
            let grouped = self
                .metadata_manager
                .catalog_controller
                .list_all_state_tables()
                .await?
                .into_iter()
                .chunk_by(|t| t.table_type());
            for (table_type, tables) in &grouped {
                let tables = tables.into_iter().map(|t| {
                    (
                        t.id.into(),
                        (
                            t.name,
                            t.database_id,
                            t.schema_id,
                            t.definition,
                            t.created_at_epoch,
                        ),
                    )
                });
                match table_type {
                    PbTableType::Table => user_tables.extend(tables),
                    PbTableType::MaterializedView => mvs.extend(tables),
                    PbTableType::Index | PbTableType::VectorIndex => indexes.extend(tables),
                    PbTableType::Internal => internal_tables.extend(tables),
                    PbTableType::Unspecified => {
                        tracing::error!("unspecified table type: {:?}", tables.collect_vec());
                    }
                }
            }
        }
        let sinks = self
            .metadata_manager
            .catalog_controller
            .list_sinks()
            .await?
            .into_iter()
            .map(|s| {
                (
                    s.id.into(),
                    (
                        s.name,
                        s.database_id,
                        s.schema_id,
                        s.definition,
                        s.created_at_epoch,
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let views = self
            .metadata_manager
            .catalog_controller
            .list_views()
            .await?
            .into_iter()
            .map(|v| {
                (
                    v.id.into(),
                    (v.name, v.database_id, v.schema_id, v.sql, None),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut streaming_jobs = self
            .metadata_manager
            .catalog_controller
            .list_streaming_job_infos()
            .await?;
        streaming_jobs.sort_by_key(|info| (info.obj_type as usize, info.job_id));
        {
            use comfy_table::{Row, Table};
            let mut table = Table::new();
            table.set_header({
                let mut row = Row::new();
                row.add_cell("job_id".into());
                row.add_cell("name".into());
                row.add_cell("obj_type".into());
                row.add_cell("state".into());
                row.add_cell("parallelism".into());
                row.add_cell("max_parallelism".into());
                row.add_cell("resource_group".into());
                row.add_cell("database_id".into());
                row.add_cell("schema_id".into());
                row.add_cell("config_override".into());
                row
            });
            for job in streaming_jobs {
                let mut row = Row::new();
                row.add_cell(job.job_id.into());
                row.add_cell(job.name.into());
                row.add_cell(job.obj_type.as_str().into());
                row.add_cell(format_job_status(job.job_status).into());
                row.add_cell(
                    format_streaming_parallelism(
                        &job.parallelism,
                        job.adaptive_parallelism_strategy.as_deref(),
                    )
                    .into(),
                );
                row.add_cell(job.max_parallelism.into());
                row.add_cell(job.resource_group.into());
                row.add_cell(job.database_id.into());
                row.add_cell(job.schema_id.into());
                row.add_cell(job.config_override.into());
                table.add_row(row);
            }
            let _ = writeln!(s);
            let _ = writeln!(s, "STREAMING JOB");
            let _ = writeln!(s, "{table}");
        }
        let catalogs = [
            ("SOURCE", sources),
            ("TABLE", user_tables),
            ("MATERIALIZED VIEW", mvs),
            ("INDEX", indexes),
            ("SINK", sinks),
            ("VIEW", views),
            ("INTERNAL TABLE", internal_tables),
        ];
        let mut obj_id_to_name: HashMap<ObjectId, _> = HashMap::new();
        for (title, items) in catalogs {
            use comfy_table::{Row, Table};
            let mut table = Table::new();
            table.set_header({
                let mut row = Row::new();
                row.add_cell("id".into());
                row.add_cell("name".into());
                row.add_cell("database_id".into());
                row.add_cell("schema_id".into());
                row.add_cell("created_at".into());
                row.add_cell("definition".into());
                row
            });
            for (id, (name, database_id, schema_id, definition, created_at_epoch)) in items {
                obj_id_to_name.insert(id, name.clone());
                let mut row = Row::new();
                let may_redact = redact_sql(&definition, self.redact_sql_option_keywords.clone())
                    .unwrap_or_else(|| "[REDACTED]".into());
                let created_at = if let Some(created_at_epoch) = created_at_epoch {
                    format!("{}", Epoch::from(created_at_epoch).as_timestamptz())
                } else {
                    "".into()
                };
                row.add_cell(id.into());
                row.add_cell(name.into());
                row.add_cell(database_id.into());
                row.add_cell(schema_id.into());
                row.add_cell(created_at.into());
                row.add_cell(may_redact.into());
                table.add_row(row);
            }
            let _ = writeln!(s);
            let _ = writeln!(s, "{title}");
            let _ = writeln!(s, "{table}");
        }

        let mut fragments = BTreeMap::new();
        for (actor_id, fragment_id, job_id, schema_id, obj_type) in self
            .metadata_manager
            .catalog_controller
            .list_actor_info()
            .await?
        {
            let entry = fragments
                .entry(fragment_id)
                .or_insert_with(|| (job_id, schema_id, obj_type, Vec::new()));
            entry.3.push(actor_id);
        }

        use comfy_table::{Row, Table};
        let mut table = Table::new();
        table.set_header({
            let mut row = Row::new();
            row.add_cell("fragment_id".into());
            row.add_cell("job_id".into());
            row.add_cell("schema_id".into());
            row.add_cell("type".into());
            row.add_cell("name".into());
            row.add_cell("actor_count".into());
            row.add_cell("actors".into());
            row
        });
        for (fragment_id, (job_id, schema_id, obj_type, mut actor_ids)) in fragments {
            actor_ids.sort_unstable();
            let mut row = Row::new();
            row.add_cell(fragment_id.into());
            row.add_cell(job_id.into());
            row.add_cell(schema_id.into());
            row.add_cell(obj_type.as_str().into());
            row.add_cell(
                obj_id_to_name
                    .get(&job_id)
                    .cloned()
                    .unwrap_or_default()
                    .into(),
            );
            row.add_cell(actor_ids.len().into());
            row.add_cell(actor_ids.iter().join(", ").into());
            table.add_row(row);
        }
        let _ = writeln!(s);
        let _ = writeln!(s, "FRAGMENT");
        let _ = writeln!(s, "{table}");
        Ok(())
    }

    fn write_license(&self, s: &mut String) {
        use comfy_table::presets::ASCII_BORDERS_ONLY;
        use comfy_table::{ContentArrangement, Row, Table};

        let mut table = Table::new();
        table.load_preset(ASCII_BORDERS_ONLY);
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header({
            let mut row = Row::new();
            row.add_cell("field".into());
            row.add_cell("value".into());
            row
        });

        match LicenseManager::get().license() {
            Ok(license) => {
                let fmt_option = |value: Option<u64>| match value {
                    Some(v) => v.to_string(),
                    None => "unlimited".to_owned(),
                };

                let expires_at = if license.exp == u64::MAX {
                    "never".to_owned()
                } else {
                    let exp_i64 = license.exp as i64;
                    chrono::DateTime::<chrono::Utc>::from_timestamp(exp_i64, 0)
                        .map(|ts| ts.to_rfc3339())
                        .unwrap_or_else(|| format!("invalid ({})", license.exp))
                };

                let mut row = Row::new();
                row.add_cell("status".into());
                row.add_cell("valid".into());
                table.add_row(row);

                let mut row = Row::new();
                row.add_cell("tier".into());
                row.add_cell(license.tier.name().into());
                table.add_row(row);

                let mut row = Row::new();
                row.add_cell("expires_at".into());
                row.add_cell(expires_at.into());
                table.add_row(row);

                let mut row = Row::new();
                row.add_cell("rwu_limit".into());
                row.add_cell(fmt_option(license.rwu_limit.map(|v| v.get())).into());
                table.add_row(row);

                let mut row = Row::new();
                row.add_cell("cpu_core_limit".into());
                row.add_cell(fmt_option(license.cpu_core_limit()).into());
                table.add_row(row);

                let mut row = Row::new();
                row.add_cell("memory_limit_bytes".into());
                row.add_cell(fmt_option(license.memory_limit()).into());
                table.add_row(row);

                let mut features: Vec<_> = license
                    .tier
                    .available_features()
                    .map(|f| f.name())
                    .collect();
                features.sort_unstable();
                let feature_summary = format_features(&features);

                let mut row = Row::new();
                row.add_cell("available_features".into());
                row.add_cell(feature_summary.into());
                table.add_row(row);
            }
            Err(error) => {
                let mut row = Row::new();
                row.add_cell("status".into());
                row.add_cell("invalid".into());
                table.add_row(row);

                let mut row = Row::new();
                row.add_cell("error".into());
                row.add_cell(error.to_report_string().into());
                table.add_row(row);
            }
        }

        let _ = writeln!(s, "LICENSE");
        let _ = writeln!(s, "{table}");
    }

    async fn write_params(&self, s: &mut String) {
        let params = self.system_params_controller.get_params().await;

        use comfy_table::{Row, Table};
        let mut table = Table::new();
        table.set_header({
            let mut row = Row::new();
            row.add_cell("key".into());
            row.add_cell("value".into());
            row
        });

        let mut row = Row::new();
        row.add_cell("barrier_interval_ms".into());
        row.add_cell(params.barrier_interval_ms().to_string().into());
        table.add_row(row);

        let mut row = Row::new();
        row.add_cell("checkpoint_frequency".into());
        row.add_cell(params.checkpoint_frequency().to_string().into());
        table.add_row(row);

        let mut row = Row::new();
        row.add_cell("state_store".into());
        row.add_cell(params.state_store().to_string().into());
        table.add_row(row);

        let mut row = Row::new();
        row.add_cell("data_directory".into());
        row.add_cell(params.data_directory().to_owned().into());
        table.add_row(row);

        let mut row = Row::new();
        row.add_cell("max_concurrent_creating_streaming_jobs".into());
        row.add_cell(
            params
                .max_concurrent_creating_streaming_jobs()
                .to_string()
                .into(),
        );
        table.add_row(row);

        let mut row = Row::new();
        row.add_cell("time_travel_retention_ms".into());
        row.add_cell(params.time_travel_retention_ms().to_string().into());
        table.add_row(row);

        let mut row = Row::new();
        row.add_cell("per_database_isolation".into());
        row.add_cell(params.per_database_isolation().to_string().into());
        table.add_row(row);

        let _ = writeln!(s, "SYSTEM PARAMS");
        let _ = writeln!(s, "{table}");
    }
}

fn try_add_cell<T: Into<comfy_table::Cell>>(row: &mut comfy_table::Row, t: Option<T>) {
    match t {
        Some(t) => {
            row.add_cell(t.into());
        }
        None => {
            row.add_cell("".into());
        }
    }
}

fn merge_prometheus_selector<'a>(selectors: impl IntoIterator<Item = &'a str>) -> String {
    selectors.into_iter().filter(|s| !s.is_empty()).join(",")
}

fn redact_sql(sql: &str, keywords: RedactSqlOptionKeywordsRef) -> Option<String> {
    match Parser::parse_sql(sql) {
        Ok(sqls) => Some(
            sqls.into_iter()
                .map(|sql| sql.to_redacted_string(keywords.clone()))
                .join(";"),
        ),
        Err(_) => None,
    }
}

fn format_features(features: &[&'static str]) -> String {
    if features.is_empty() {
        return "(none)".into();
    }

    const PER_LINE: usize = 6;
    features
        .chunks(PER_LINE)
        .map(|chunk| format!("  {}", chunk.join(", ")))
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_job_status(status: JobStatus) -> &'static str {
    match status {
        JobStatus::Initial => "initial",
        JobStatus::Creating => "creating",
        JobStatus::Created => "created",
    }
}

fn format_streaming_parallelism(
    parallelism: &StreamingParallelism,
    adaptive_parallelism_strategy: Option<&str>,
) -> String {
    match parallelism {
        StreamingParallelism::Adaptive => adaptive_parallelism_strategy
            .and_then(format_adaptive_parallelism_strategy)
            .unwrap_or_else(|| "adaptive".into()),
        StreamingParallelism::Fixed(n) => n.to_string(),
        StreamingParallelism::Custom => adaptive_parallelism_strategy
            .and_then(format_adaptive_parallelism_strategy)
            .unwrap_or_else(|| "custom".into()),
    }
}

fn format_adaptive_parallelism_strategy(strategy: &str) -> Option<String> {
    parse_strategy(strategy)
        .ok()
        .map(|strategy| match strategy {
            AdaptiveParallelismStrategy::Auto | AdaptiveParallelismStrategy::Full => {
                "adaptive".to_owned()
            }
            AdaptiveParallelismStrategy::Bounded(n) => format!("bounded({n})"),
            AdaptiveParallelismStrategy::Ratio(r) => format!("ratio({r})"),
        })
}
