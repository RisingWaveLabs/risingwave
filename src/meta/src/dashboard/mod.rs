// Copyright 2022 RisingWave Labs
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

mod prometheus;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context as _, Result, anyhow};
use axum::Router;
use axum::extract::{Extension, Path};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use risingwave_common_heap_profiling::ProfileServiceImpl;
use risingwave_rpc_client::MonitorClientPool;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{self, CorsLayer};

use crate::hummock::HummockManagerRef;
use crate::manager::MetadataManager;
use crate::manager::diagnose::DiagnoseCommandRef;

#[derive(Clone)]
pub struct DashboardService {
    pub await_tree_reg: await_tree::Registry,
    pub dashboard_addr: SocketAddr,
    pub prometheus_client: Option<prometheus_http_query::Client>,
    pub prometheus_selector: String,
    pub metadata_manager: MetadataManager,
    pub hummock_manager: HummockManagerRef,
    pub monitor_clients: MonitorClientPool,
    pub diagnose_command: DiagnoseCommandRef,
    pub profile_service: ProfileServiceImpl,
    pub trace_state: otlp_embedded::StateRef,
}

pub type Service = Arc<DashboardService>;

pub(super) mod handlers {
    use std::cmp::min;
    use std::collections::HashMap;

    use anyhow::Context;
    use axum::Json;
    use axum::extract::Query;
    use futures::future::join_all;
    use itertools::Itertools;
    use risingwave_common::id::JobId;
    use risingwave_meta_model::WorkerId;
    use risingwave_pb::catalog::table::TableType;
    use risingwave_pb::catalog::{
        Index, PbDatabase, PbFunction, PbSchema, Sink, Source, Subscription, Table, View,
    };
    use risingwave_pb::common::{WorkerNode, WorkerType};
    use risingwave_pb::hummock::TableStats;
    use risingwave_pb::meta::{
        ActorIds, FragmentIdToActorIdMap, FragmentToRelationMap, PbTableFragments, RelationIdInfos,
    };
    use risingwave_pb::monitor_service::stack_trace_request::ActorTracesFormat;
    use risingwave_pb::monitor_service::{
        AnalyzeHeapRequest, ChannelDeltaStats, GetStreamingPrometheusStatsResponse,
        GetStreamingStatsResponse, HeapProfilingRequest, HeapProfilingResponse,
        ListHeapProfilingRequest, ListHeapProfilingResponse, ProfilingRequest, StackTraceResponse,
    };
    use risingwave_pb::user::PbUserInfo;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use thiserror_ext::AsReport;
    use tonic::Request;

    use super::*;
    use crate::controller::fragment::StreamingJobInfo;
    use crate::rpc::await_tree::{dump_cluster_await_tree, dump_worker_node_await_tree};

    #[derive(Serialize)]
    pub struct TableWithStats {
        #[serde(flatten)]
        pub table: Table,
        pub total_size_bytes: i64,
        pub total_key_count: i64,
        pub total_key_size: i64,
        pub total_value_size: i64,
        pub compressed_size: u64,
    }

    impl TableWithStats {
        pub fn from_table_and_stats(table: Table, stats: Option<&TableStats>) -> Self {
            match stats {
                Some(stats) => Self {
                    total_size_bytes: stats.total_key_size + stats.total_value_size,
                    total_key_count: stats.total_key_count,
                    total_key_size: stats.total_key_size,
                    total_value_size: stats.total_value_size,
                    compressed_size: stats.total_compressed_size,
                    table,
                },
                None => Self {
                    total_size_bytes: 0,
                    total_key_count: 0,
                    total_key_size: 0,
                    total_value_size: 0,
                    compressed_size: 0,
                    table,
                },
            }
        }
    }

    pub struct DashboardError(anyhow::Error);
    pub type Result<T> = std::result::Result<T, DashboardError>;

    pub fn err(err: impl Into<anyhow::Error>) -> DashboardError {
        DashboardError(err.into())
    }

    impl From<anyhow::Error> for DashboardError {
        fn from(value: anyhow::Error) -> Self {
            DashboardError(value)
        }
    }

    impl IntoResponse for DashboardError {
        fn into_response(self) -> axum::response::Response {
            let mut resp = Json(json!({
                "error": self.0.to_report_string(),
            }))
            .into_response();
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            resp
        }
    }

    pub async fn list_clusters(
        Path(ty): Path<i32>,
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<WorkerNode>>> {
        let worker_type = match WorkerType::try_from(ty) {
            Ok(WorkerType::Unspecified) | Err(_) => {
                return Err(err(anyhow!("invalid worker type: {ty}")));
            }
            Ok(worker_type) => worker_type,
        };
        let mut result = srv
            .metadata_manager
            .list_worker_node(Some(worker_type), None)
            .await
            .map_err(err)?;
        result.sort_unstable_by_key(|n| n.id);
        Ok(result.into())
    }

    async fn list_table_catalogs_inner(
        metadata_manager: &MetadataManager,
        hummock_manager: &HummockManagerRef,
        table_type: TableType,
    ) -> Result<Json<Vec<TableWithStats>>> {
        let tables = metadata_manager
            .catalog_controller
            .list_tables_by_type(table_type.into())
            .await
            .map_err(err)?;

        // Get table statistics from hummock manager
        let version_stats = hummock_manager.get_version_stats().await;

        let tables_with_stats = tables
            .into_iter()
            .map(|table| {
                let stats = version_stats.table_stats.get(&table.id);
                TableWithStats::from_table_and_stats(table, stats)
            })
            .collect();

        Ok(Json(tables_with_stats))
    }

    pub async fn list_materialized_views(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<TableWithStats>>> {
        list_table_catalogs_inner(
            &srv.metadata_manager,
            &srv.hummock_manager,
            TableType::MaterializedView,
        )
        .await
    }

    pub async fn list_tables(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<TableWithStats>>> {
        list_table_catalogs_inner(
            &srv.metadata_manager,
            &srv.hummock_manager,
            TableType::Table,
        )
        .await
    }

    pub async fn list_index_tables(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<TableWithStats>>> {
        list_table_catalogs_inner(
            &srv.metadata_manager,
            &srv.hummock_manager,
            TableType::Index,
        )
        .await
    }

    pub async fn list_indexes(Extension(srv): Extension<Service>) -> Result<Json<Vec<Index>>> {
        let indexes = srv
            .metadata_manager
            .catalog_controller
            .list_indexes()
            .await
            .map_err(err)?;

        Ok(Json(indexes))
    }

    pub async fn list_subscription(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<Subscription>>> {
        let subscriptions = srv
            .metadata_manager
            .catalog_controller
            .list_subscriptions()
            .await
            .map_err(err)?;

        Ok(Json(subscriptions))
    }

    pub async fn list_internal_tables(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<TableWithStats>>> {
        list_table_catalogs_inner(
            &srv.metadata_manager,
            &srv.hummock_manager,
            TableType::Internal,
        )
        .await
    }

    pub async fn list_sources(Extension(srv): Extension<Service>) -> Result<Json<Vec<Source>>> {
        let sources = srv.metadata_manager.list_sources().await.map_err(err)?;

        Ok(Json(sources))
    }

    pub async fn list_sinks(Extension(srv): Extension<Service>) -> Result<Json<Vec<Sink>>> {
        let sinks = srv
            .metadata_manager
            .catalog_controller
            .list_sinks()
            .await
            .map_err(err)?;

        Ok(Json(sinks))
    }

    pub async fn list_views(Extension(srv): Extension<Service>) -> Result<Json<Vec<View>>> {
        let views = srv
            .metadata_manager
            .catalog_controller
            .list_views()
            .await
            .map_err(err)?;

        Ok(Json(views))
    }

    pub async fn list_functions(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<PbFunction>>> {
        let functions = srv
            .metadata_manager
            .catalog_controller
            .list_functions()
            .await
            .map_err(err)?;

        Ok(Json(functions))
    }

    pub async fn list_streaming_jobs(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<StreamingJobInfo>>> {
        let streaming_jobs = srv
            .metadata_manager
            .catalog_controller
            .list_streaming_job_infos()
            .await
            .map_err(err)?;

        Ok(Json(streaming_jobs))
    }

    /// In the ddl backpressure graph, we want to compute the backpressure between relations.
    /// So we need to know which are the fragments which are connected to external relations.
    /// These fragments form the vertices of the graph.
    /// We can get collection of backpressure values, keyed by vertex_id-vertex_id.
    /// This function will return a map of fragment vertex id to relation id.
    /// We can convert `fragment_id-fragment_id` to `relation_id-relation_id` using that.
    /// Finally, we have a map of `relation_id-relation_id` to backpressure values.
    pub async fn get_fragment_to_relation_map(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<FragmentToRelationMap>> {
        let table_fragments = srv
            .metadata_manager
            .catalog_controller
            .table_fragments()
            .await
            .map_err(err)?;
        let mut fragment_to_relation_map = HashMap::new();
        for (relation_id, (tf, _, _)) in table_fragments {
            for fragment_id in tf.fragments.keys() {
                fragment_to_relation_map.insert(*fragment_id, relation_id);
            }
        }
        let map = FragmentToRelationMap {
            fragment_to_relation_map,
        };
        Ok(Json(map))
    }

    /// Provides a hierarchy of relation ids to fragments to actors.
    pub async fn get_relation_id_infos(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<RelationIdInfos>> {
        let table_fragments = srv
            .metadata_manager
            .catalog_controller
            .table_fragments()
            .await
            .map_err(err)?;
        let mut map = HashMap::new();
        for (id, (tf, fragment_actors, _actor_status)) in table_fragments {
            let mut fragment_id_to_actor_ids = HashMap::new();
            for fragment_id in tf.fragments.keys() {
                let actor_ids = fragment_actors
                    .get(fragment_id)
                    .into_iter()
                    .flat_map(|actors| actors.iter().map(|a| a.actor_id))
                    .collect_vec();
                fragment_id_to_actor_ids.insert(*fragment_id, ActorIds { ids: actor_ids });
            }
            map.insert(
                id.as_raw_id(),
                FragmentIdToActorIdMap {
                    map: fragment_id_to_actor_ids,
                },
            );
        }
        let relation_id_infos = RelationIdInfos { map };

        Ok(Json(relation_id_infos))
    }

    pub async fn list_fragments_by_job_id(
        Extension(srv): Extension<Service>,
        Path(job_id): Path<u32>,
    ) -> Result<Json<PbTableFragments>> {
        let job_id = JobId::new(job_id);
        let (table_fragments, fragment_actors, actor_status) = srv
            .metadata_manager
            .catalog_controller
            .get_job_fragments_by_id(job_id)
            .await
            .map_err(err)?;
        let upstream_fragments = srv
            .metadata_manager
            .catalog_controller
            .upstream_fragments(table_fragments.fragment_ids())
            .await
            .map_err(err)?;
        let dispatchers = srv
            .metadata_manager
            .catalog_controller
            .get_fragment_actor_dispatchers(
                table_fragments.fragment_ids().map(|id| id as _).collect(),
            )
            .await
            .map_err(err)?;
        Ok(Json(table_fragments.to_protobuf(
            &fragment_actors,
            &upstream_fragments,
            &dispatchers,
            actor_status,
        )))
    }

    pub async fn list_users(Extension(srv): Extension<Service>) -> Result<Json<Vec<PbUserInfo>>> {
        let users = srv
            .metadata_manager
            .catalog_controller
            .list_users()
            .await
            .map_err(err)?;

        Ok(Json(users))
    }

    pub async fn list_databases(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<PbDatabase>>> {
        let databases = srv
            .metadata_manager
            .catalog_controller
            .list_databases()
            .await
            .map_err(err)?;

        Ok(Json(databases))
    }

    pub async fn list_schemas(Extension(srv): Extension<Service>) -> Result<Json<Vec<PbSchema>>> {
        let schemas = srv
            .metadata_manager
            .catalog_controller
            .list_schemas()
            .await
            .map_err(err)?;

        Ok(Json(schemas))
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct DashboardObjectDependency {
        pub object_id: u32,
        pub referenced_object_id: u32,
    }

    pub async fn list_object_dependencies(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<DashboardObjectDependency>>> {
        let object_dependencies = srv
            .metadata_manager
            .catalog_controller
            .list_all_object_dependencies()
            .await
            .map_err(err)?;

        let result = object_dependencies
            .into_iter()
            .map(|dependency| DashboardObjectDependency {
                object_id: dependency.object_id.as_raw_id(),
                referenced_object_id: dependency.referenced_object_id.as_raw_id(),
            })
            .collect();

        Ok(Json(result))
    }

    #[derive(Debug, Deserialize)]
    pub struct AwaitTreeDumpParams {
        #[serde(default = "await_tree_default_format")]
        format: String,
    }

    impl AwaitTreeDumpParams {
        /// Parse the `format` parameter to [`ActorTracesFormat`].
        pub fn actor_traces_format(&self) -> Result<ActorTracesFormat> {
            Ok(match self.format.as_str() {
                "text" => ActorTracesFormat::Text,
                "json" => ActorTracesFormat::Json,
                _ => {
                    return Err(err(anyhow!(
                        "Unsupported format `{}`, only `text` and `json` are supported for now",
                        self.format
                    )));
                }
            })
        }
    }

    fn await_tree_default_format() -> String {
        // In dashboard, await tree is usually for engineer to debug, so we use human-readable text format by default here.
        "text".to_owned()
    }

    pub async fn dump_await_tree_all(
        Query(params): Query<AwaitTreeDumpParams>,
        Extension(srv): Extension<Service>,
    ) -> Result<Json<StackTraceResponse>> {
        let actor_traces_format = params.actor_traces_format()?;

        let res = dump_cluster_await_tree(
            &srv.metadata_manager,
            &srv.await_tree_reg,
            actor_traces_format,
        )
        .await
        .map_err(err)?;

        Ok(res.into())
    }

    pub async fn dump_await_tree(
        Path(worker_id): Path<WorkerId>,
        Query(params): Query<AwaitTreeDumpParams>,
        Extension(srv): Extension<Service>,
    ) -> Result<Json<StackTraceResponse>> {
        let actor_traces_format = params.actor_traces_format()?;

        let worker_node = srv
            .metadata_manager
            .get_worker_by_id(worker_id)
            .await
            .map_err(err)?
            .context("worker node not found")
            .map_err(err)?;

        let res = dump_worker_node_await_tree(std::iter::once(&worker_node), actor_traces_format)
            .await
            .map_err(err)?;

        Ok(res.into())
    }

    pub async fn heap_profile(
        Path(worker_id): Path<WorkerId>,
        Extension(srv): Extension<Service>,
    ) -> Result<Json<HeapProfilingResponse>> {
        if worker_id == crate::manager::META_NODE_ID {
            let result = srv
                .profile_service
                .heap_profiling(Request::new(HeapProfilingRequest { dir: "".to_owned() }))
                .map_err(err)?
                .into_inner();
            return Ok(result.into());
        }

        let worker_node = srv
            .metadata_manager
            .get_worker_by_id(worker_id)
            .await
            .map_err(err)?
            .context("worker node not found")
            .map_err(err)?;

        let client = srv.monitor_clients.get(&worker_node).await.map_err(err)?;

        let result = client.heap_profile("".to_owned()).await.map_err(err)?;

        Ok(result.into())
    }

    pub async fn cpu_profile(
        Path((worker_id, duration_secs)): Path<(WorkerId, u64)>,
        Extension(srv): Extension<Service>,
    ) -> Result<Response> {
        if duration_secs == 0 {
            return Err(err(anyhow!(
                "CPU profiling duration must be greater than zero"
            )));
        }

        let flamegraph = if worker_id == crate::manager::META_NODE_ID {
            srv.profile_service
                .profiling(Request::new(ProfilingRequest {
                    sleep_s: duration_secs,
                }))
                .await
                .map_err(err)?
                .into_inner()
                .result
        } else {
            let worker_node = srv
                .metadata_manager
                .get_worker_by_id(worker_id)
                .await
                .map_err(err)?
                .context("worker node not found")
                .map_err(err)?;

            let client = srv.monitor_clients.get(&worker_node).await.map_err(err)?;
            client.profile(duration_secs).await.map_err(err)?.result
        };

        Response::builder()
            .header("Content-Type", "image/svg+xml")
            .body(flamegraph.into())
            .map_err(err)
    }

    pub async fn list_heap_profile(
        Path(worker_id): Path<WorkerId>,
        Extension(srv): Extension<Service>,
    ) -> Result<Json<ListHeapProfilingResponse>> {
        if worker_id == crate::manager::META_NODE_ID {
            let result = srv
                .profile_service
                .list_heap_profiling(Request::new(ListHeapProfilingRequest {}))
                .map_err(err)?
                .into_inner();
            return Ok(result.into());
        }

        let worker_node = srv
            .metadata_manager
            .get_worker_by_id(worker_id)
            .await
            .map_err(err)?
            .context("worker node not found")
            .map_err(err)?;

        let client = srv.monitor_clients.get(&worker_node).await.map_err(err)?;

        let result = client.list_heap_profile().await.map_err(err)?;
        Ok(result.into())
    }

    pub async fn analyze_heap(
        Path((worker_id, file_path)): Path<(WorkerId, String)>,
        Extension(srv): Extension<Service>,
    ) -> Result<Response> {
        let file_path =
            String::from_utf8(base64_url::decode(&file_path).map_err(err)?).map_err(err)?;

        let collapsed_bin = if worker_id == crate::manager::META_NODE_ID {
            srv.profile_service
                .analyze_heap(Request::new(AnalyzeHeapRequest {
                    path: file_path.clone(),
                }))
                .await
                .map_err(err)?
                .into_inner()
                .result
        } else {
            let worker_node = srv
                .metadata_manager
                .get_worker_by_id(worker_id)
                .await
                .map_err(err)?
                .context("worker node not found")
                .map_err(err)?;

            let client = srv.monitor_clients.get(&worker_node).await.map_err(err)?;
            client
                .analyze_heap(file_path.clone())
                .await
                .map_err(err)?
                .result
        };
        let collapsed_str = String::from_utf8_lossy(&collapsed_bin).to_string();

        let response = Response::builder()
            .header("Content-Type", "application/octet-stream")
            .body(collapsed_str.into());

        response.map_err(err)
    }

    #[derive(Debug, Deserialize)]
    pub struct DiagnoseParams {
        #[serde(default = "await_tree_default_format")]
        actor_traces_format: String,
    }

    pub async fn diagnose(
        Query(params): Query<DiagnoseParams>,
        Extension(srv): Extension<Service>,
    ) -> Result<String> {
        let actor_traces_format = match params.actor_traces_format.as_str() {
            "text" => ActorTracesFormat::Text,
            "json" => ActorTracesFormat::Json,
            _ => {
                return Err(err(anyhow!(
                    "Unsupported actor_traces_format `{}`, only `text` and `json` are supported for now",
                    params.actor_traces_format
                )));
            }
        };
        Ok(srv.diagnose_command.report(actor_traces_format).await)
    }

    /// NOTE(kwannoel): Although we fetch the BP for the entire graph via this API,
    /// the workload should be reasonable.
    /// In most cases, we can safely assume each node has most 2 outgoing edges (e.g. join).
    /// In such a scenario, the number of edges is linear to the number of nodes.
    /// So the workload is proportional to the relation id graph we fetch in `get_relation_id_infos`.
    pub async fn get_streaming_stats(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<GetStreamingStatsResponse>> {
        let worker_nodes = srv
            .metadata_manager
            .list_active_streaming_compute_nodes()
            .await
            .map_err(err)?;

        let mut futures = Vec::new();

        for worker_node in worker_nodes {
            let client = srv.monitor_clients.get(&worker_node).await.map_err(err)?;
            let client = Arc::new(client);
            let fut = async move {
                let result = client.get_streaming_stats().await.map_err(err)?;
                Ok::<_, DashboardError>(result)
            };
            futures.push(fut);
        }
        let results = join_all(futures).await;

        let mut all = GetStreamingStatsResponse::default();

        for result in results {
            let result = result
                .map_err(|_| anyhow!("Failed to get back pressure"))
                .map_err(err)?;

            // Aggregate fragment_stats
            for (fragment_id, fragment_stats) in result.fragment_stats {
                if let Some(s) = all.fragment_stats.get_mut(&fragment_id) {
                    s.actor_count += fragment_stats.actor_count;
                    s.current_epoch = min(s.current_epoch, fragment_stats.current_epoch);
                } else {
                    all.fragment_stats.insert(fragment_id, fragment_stats);
                }
            }

            // Aggregate relation_stats
            for (relation_id, relation_stats) in result.relation_stats {
                if let Some(s) = all.relation_stats.get_mut(&relation_id) {
                    s.actor_count += relation_stats.actor_count;
                    s.current_epoch = min(s.current_epoch, relation_stats.current_epoch);
                } else {
                    all.relation_stats.insert(relation_id, relation_stats);
                }
            }

            // Aggregate channel_stats
            for (key, channel_stats) in result.channel_stats {
                if let Some(s) = all.channel_stats.get_mut(&key) {
                    s.actor_count += channel_stats.actor_count;
                    s.output_blocking_duration += channel_stats.output_blocking_duration;
                    s.recv_row_count += channel_stats.recv_row_count;
                    s.send_row_count += channel_stats.send_row_count;
                } else {
                    all.channel_stats.insert(key, channel_stats);
                }
            }
        }

        Ok(all.into())
    }

    #[derive(Debug, Deserialize)]
    pub struct StreamingStatsPrometheusParams {
        /// Unix timestamp in seconds for the evaluation time. If not set, defaults to current Prometheus server time.
        #[serde(default)]
        at: Option<i64>,
        /// Time offset for throughput and backpressure rate calculation in seconds. If not set, defaults to 60s.
        #[serde(default = "streaming_stats_default_time_offset")]
        time_offset: i64,
    }

    fn streaming_stats_default_time_offset() -> i64 {
        60
    }

    pub async fn get_streaming_stats_from_prometheus(
        Query(params): Query<StreamingStatsPrometheusParams>,
        Extension(srv): Extension<Service>,
    ) -> Result<Json<GetStreamingPrometheusStatsResponse>> {
        let mut all = GetStreamingPrometheusStatsResponse::default();

        // Get fragment and relation stats from workers
        let worker_nodes = srv
            .metadata_manager
            .list_active_streaming_compute_nodes()
            .await
            .map_err(err)?;

        let mut futures = Vec::new();

        for worker_node in worker_nodes {
            let client = srv.monitor_clients.get(&worker_node).await.map_err(err)?;
            let client = Arc::new(client);
            let fut = async move {
                let result = client.get_streaming_stats().await.map_err(err)?;
                Ok::<_, DashboardError>(result)
            };
            futures.push(fut);
        }
        let results = join_all(futures).await;

        for result in results {
            let result = result
                .map_err(|_| anyhow!("Failed to get streaming stats from worker"))
                .map_err(err)?;

            // Aggregate fragment_stats
            for (fragment_id, fragment_stats) in result.fragment_stats {
                if let Some(s) = all.fragment_stats.get_mut(&fragment_id) {
                    s.actor_count += fragment_stats.actor_count;
                    s.current_epoch = min(s.current_epoch, fragment_stats.current_epoch);
                } else {
                    all.fragment_stats.insert(fragment_id, fragment_stats);
                }
            }

            // Aggregate relation_stats
            for (relation_id, relation_stats) in result.relation_stats {
                if let Some(s) = all.relation_stats.get_mut(&relation_id) {
                    s.actor_count += relation_stats.actor_count;
                    s.current_epoch = min(s.current_epoch, relation_stats.current_epoch);
                } else {
                    all.relation_stats.insert(relation_id, relation_stats);
                }
            }
        }

        // Get channel delta stats from Prometheus
        if let Some(ref client) = srv.prometheus_client {
            // Query channel delta stats: throughput and backpressure rate
            let channel_input_throughput_query = format!(
                "sum(rate(stream_actor_in_record_cnt{{{}}}[{}s])) by (fragment_id, upstream_fragment_id)",
                srv.prometheus_selector, params.time_offset
            );
            let channel_output_throughput_query = format!(
                "sum(rate(stream_actor_out_record_cnt{{{}}}[{}s])) by (fragment_id, upstream_fragment_id)",
                srv.prometheus_selector, params.time_offset
            );
            let channel_backpressure_query = format!(
                "sum(rate(stream_actor_output_buffer_blocking_duration_ns{{{}}}[{}s])) by (fragment_id, downstream_fragment_id) \
                 / ignoring (downstream_fragment_id) group_left sum(stream_actor_count) by (fragment_id)",
                srv.prometheus_selector, params.time_offset
            );

            // Execute all queries concurrently with optional time parameter
            let (
                channel_input_throughput_result,
                channel_output_throughput_result,
                channel_backpressure_result,
            ) = {
                let mut input_query = client.query(channel_input_throughput_query);
                let mut output_query = client.query(channel_output_throughput_query);
                let mut backpressure_query = client.query(channel_backpressure_query);

                // Set the evaluation time if provided
                if let Some(at_time) = params.at {
                    input_query = input_query.at(at_time);
                    output_query = output_query.at(at_time);
                    backpressure_query = backpressure_query.at(at_time);
                }

                tokio::try_join!(
                    input_query.get(),
                    output_query.get(),
                    backpressure_query.get(),
                )
                .map_err(err)?
            };

            // Process channel delta stats
            let mut channel_data = HashMap::new();

            // Collect input throughput
            if let Some(channel_input_throughput_data) =
                channel_input_throughput_result.data().as_vector()
            {
                for sample in channel_input_throughput_data {
                    if let Some(fragment_id_str) = sample.metric().get("fragment_id")
                        && let Some(upstream_fragment_id_str) =
                            sample.metric().get("upstream_fragment_id")
                        && let (Ok(fragment_id), Ok(upstream_fragment_id)) = (
                            fragment_id_str.parse::<u32>(),
                            upstream_fragment_id_str.parse::<u32>(),
                        )
                    {
                        let key = format!("{}_{}", upstream_fragment_id, fragment_id);
                        channel_data
                            .entry(key)
                            .or_insert_with(|| ChannelDeltaStats {
                                actor_count: 0,
                                backpressure_rate: 0.0,
                                recv_throughput: 0.0,
                                send_throughput: 0.0,
                            })
                            .recv_throughput = sample.sample().value();
                    }
                }
            }

            // Collect output throughput
            if let Some(channel_output_throughput_data) =
                channel_output_throughput_result.data().as_vector()
            {
                for sample in channel_output_throughput_data {
                    if let Some(fragment_id_str) = sample.metric().get("fragment_id")
                        && let Some(upstream_fragment_id_str) =
                            sample.metric().get("upstream_fragment_id")
                        && let (Ok(fragment_id), Ok(upstream_fragment_id)) = (
                            fragment_id_str.parse::<u32>(),
                            upstream_fragment_id_str.parse::<u32>(),
                        )
                    {
                        let key = format!("{}_{}", upstream_fragment_id, fragment_id);
                        channel_data
                            .entry(key)
                            .or_insert_with(|| ChannelDeltaStats {
                                actor_count: 0,
                                backpressure_rate: 0.0,
                                recv_throughput: 0.0,
                                send_throughput: 0.0,
                            })
                            .send_throughput = sample.sample().value();
                    }
                }
            }

            // Collect backpressure rate
            if let Some(channel_backpressure_data) = channel_backpressure_result.data().as_vector()
            {
                for sample in channel_backpressure_data {
                    if let Some(fragment_id_str) = sample.metric().get("fragment_id")
                        && let Some(downstream_fragment_id_str) =
                            sample.metric().get("downstream_fragment_id")
                        && let (Ok(fragment_id), Ok(downstream_fragment_id)) = (
                            fragment_id_str.parse::<u32>(),
                            downstream_fragment_id_str.parse::<u32>(),
                        )
                    {
                        let key = format!("{}_{}", fragment_id, downstream_fragment_id);
                        channel_data
                            .entry(key)
                            .or_insert_with(|| ChannelDeltaStats {
                                actor_count: 0,
                                backpressure_rate: 0.0,
                                recv_throughput: 0.0,
                                send_throughput: 0.0,
                            })
                            .backpressure_rate = sample.sample().value() / 1_000_000_000.0; // Convert ns to seconds
                    }
                }
            }

            // Set actor count for channels (using fragment actor count as approximation)
            for (key, channel_stats) in &mut channel_data {
                let parts: Vec<&str> = key.split('_').collect();
                if parts.len() == 2
                    && let Ok(fragment_id) = parts[1].parse::<u32>()
                    && let Some(fragment_stats) = all.fragment_stats.get(&fragment_id)
                {
                    channel_stats.actor_count = fragment_stats.actor_count;
                }
            }

            all.channel_stats = channel_data;

            Ok(Json(all))
        } else {
            Err(err(anyhow!("Prometheus endpoint is not set")))
        }
    }

    pub async fn get_version(Extension(_srv): Extension<Service>) -> Result<Json<String>> {
        Ok(Json(risingwave_common::current_cluster_version()))
    }
}

impl DashboardService {
    pub async fn serve(self) -> Result<()> {
        use handlers::*;
        let srv = Arc::new(self);

        let cors_layer = CorsLayer::new()
            .allow_origin(cors::Any)
            .allow_methods(vec![Method::GET]);

        let api_router = Router::new()
            .route("/version", get(get_version))
            .route("/clusters/{ty}", get(list_clusters))
            .route("/streaming_jobs", get(list_streaming_jobs))
            .route("/fragments/job_id/{job_id}", get(list_fragments_by_job_id))
            .route("/relation_id_infos", get(get_relation_id_infos))
            .route(
                "/fragment_to_relation_map",
                get(get_fragment_to_relation_map),
            )
            .route("/views", get(list_views))
            .route("/functions", get(list_functions))
            .route("/materialized_views", get(list_materialized_views))
            .route("/tables", get(list_tables))
            .route("/indexes", get(list_index_tables))
            .route("/index_items", get(list_indexes))
            .route("/subscriptions", get(list_subscription))
            .route("/internal_tables", get(list_internal_tables))
            .route("/sources", get(list_sources))
            .route("/sinks", get(list_sinks))
            .route("/users", get(list_users))
            .route("/databases", get(list_databases))
            .route("/schemas", get(list_schemas))
            .route("/object_dependencies", get(list_object_dependencies))
            .route("/metrics/cluster", get(prometheus::list_prometheus_cluster))
            .route("/metrics/streaming_stats", get(get_streaming_stats))
            .route(
                "/metrics/streaming_stats_prometheus",
                get(get_streaming_stats_from_prometheus),
            )
            // /monitor/await_tree/{worker_id}/?format={text or json}
            .route("/monitor/await_tree/{worker_id}", get(dump_await_tree))
            // /monitor/await_tree/?format={text or json}
            .route("/monitor/await_tree/", get(dump_await_tree_all))
            .route(
                "/monitor/dump_cpu_profile/{worker_id}/{duration_secs}",
                get(cpu_profile),
            )
            .route("/monitor/dump_heap_profile/{worker_id}", get(heap_profile))
            .route(
                "/monitor/list_heap_profile/{worker_id}",
                get(list_heap_profile),
            )
            .route("/monitor/analyze/{worker_id}/{*path}", get(analyze_heap))
            // /monitor/diagnose/?format={text or json}
            .route("/monitor/diagnose/", get(diagnose))
            .layer(
                ServiceBuilder::new()
                    .layer(AddExtensionLayer::new(srv.clone()))
                    .into_inner(),
            )
            .layer(cors_layer);

        let trace_ui_router = otlp_embedded::ui_app(srv.trace_state.clone(), "/trace/");
        let dashboard_router = risingwave_meta_dashboard::router();

        let app = Router::new()
            .fallback_service(dashboard_router)
            .nest("/api", api_router)
            .nest("/trace", trace_ui_router)
            .layer(CompressionLayer::new());

        let listener = TcpListener::bind(&srv.dashboard_addr)
            .await
            .context("failed to bind dashboard address")?;
        axum::serve(listener, app)
            .await
            .context("failed to serve dashboard service")?;

        Ok(())
    }
}
