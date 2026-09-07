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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use foyer::{HybridCache, TracingOptions};
use prometheus::core::Collector;
use prometheus::proto::Metric;
use risingwave_batch::task::BatchManager;
use risingwave_batch::task::await_tree_key::BatchTask;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::{MetricLevel, ServerConfig};
use risingwave_common_heap_profiling::ProfileServiceImpl;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_jni_core::jvm_runtime::dump_jvm_stack_traces;
use risingwave_pb::monitor_service::monitor_service_server::MonitorService;
use risingwave_pb::monitor_service::stack_trace_request::ActorTracesFormat;
use risingwave_pb::monitor_service::{
    AnalyzeHeapRequest, AnalyzeHeapResponse, ChannelStats, FragmentStats, GetProfileStatsRequest,
    GetProfileStatsResponse, GetStreamingStatsRequest, GetStreamingStatsResponse,
    GetTableCacheRefillStatsRequest, GetTableCacheRefillStatsResponse, HeapProfilingRequest,
    HeapProfilingResponse, ListHeapProfilingRequest, ListHeapProfilingResponse, ProfilingRequest,
    ProfilingResponse, RelationStats, StackTraceRequest, StackTraceResponse,
    TieredCacheTracingRequest, TieredCacheTracingResponse,
};
use risingwave_storage::hummock::compactor::await_tree_key::Compaction;
use risingwave_storage::hummock::event_handler::refiller::{
    TableCacheRefillContext, TableCacheRefillMonitorSnapshot,
};
use risingwave_storage::hummock::store::HummockStorage;
use risingwave_storage::hummock::{Block, Sstable, SstableBlockIndex};
use risingwave_stream::executor::monitor::global_streaming_metrics;
use risingwave_stream::task::LocalStreamManager;
use risingwave_stream::task::await_tree_key::{Actor, BarrierAwait};
use thiserror_ext::AsReport;
use tonic::{Request, Response, Status};

type MetaCache = HybridCache<HummockSstableObjectId, Box<Sstable>>;
type BlockCache = HybridCache<SstableBlockIndex, Box<Block>>;

#[derive(Clone)]
pub struct MonitorServiceImpl {
    stream_mgr: LocalStreamManager,
    batch_mgr: Arc<BatchManager>,
    profile_service: ProfileServiceImpl,
    meta_cache: Option<MetaCache>,
    block_cache: Option<BlockCache>,
    hummock_storage: Option<HummockStorage>,
}

impl MonitorServiceImpl {
    pub fn new(
        stream_mgr: LocalStreamManager,
        batch_mgr: Arc<BatchManager>,
        server_config: ServerConfig,
        meta_cache: Option<MetaCache>,
        block_cache: Option<BlockCache>,
        hummock_storage: Option<HummockStorage>,
    ) -> Self {
        Self {
            stream_mgr,
            batch_mgr,
            profile_service: ProfileServiceImpl::new(server_config),
            meta_cache,
            block_cache,
            hummock_storage,
        }
    }
}

#[async_trait::async_trait]
impl MonitorService for MonitorServiceImpl {
    async fn stack_trace(
        &self,
        request: Request<StackTraceRequest>,
    ) -> Result<Response<StackTraceResponse>, Status> {
        let req = request.into_inner();

        let actor_traces = if let Some(reg) = self.stream_mgr.await_tree_reg() {
            reg.collect::<Actor>()
                .into_iter()
                .map(|(k, v)| {
                    (
                        k.0.as_raw_id(),
                        if req.actor_traces_format == ActorTracesFormat::Text as i32 {
                            v.to_string()
                        } else {
                            serde_json::to_string(&v).unwrap()
                        },
                    )
                })
                .collect()
        } else {
            Default::default()
        };

        let barrier_traces = if let Some(reg) = self.stream_mgr.await_tree_reg() {
            reg.collect::<BarrierAwait>()
                .into_iter()
                .map(|(k, v)| (k.prev_epoch, v.to_string()))
                .collect()
        } else {
            Default::default()
        };

        let mut rpc_traces: BTreeMap<_, _> = if let Some(reg) = self.stream_mgr.await_tree_reg() {
            reg.collect::<GrpcCall>()
                .into_iter()
                .map(|(k, v)| (k.desc, v.to_string()))
                .collect()
        } else {
            Default::default()
        };

        if let Some(reg) = self.batch_mgr.await_tree_reg() {
            rpc_traces.extend(
                reg.collect::<GrpcCall>()
                    .into_iter()
                    .map(|(k, v)| (k.desc, v.to_string())),
            );
        }

        let batch_traces = if let Some(reg) = self.batch_mgr.await_tree_reg() {
            reg.collect::<BatchTask>()
                .into_iter()
                .map(|(k, v)| {
                    let id = &k.0;
                    (
                        format!("{}-{}-{}", id.query_id, id.stage_id, id.task_id),
                        v.to_string(),
                    )
                })
                .collect()
        } else {
            Default::default()
        };

        let compaction_task_traces = if let Some(hummock) =
            self.stream_mgr.env.state_store().as_hummock()
            && let Some(m) = hummock.compaction_await_tree_reg()
        {
            m.collect::<Compaction>()
                .into_iter()
                .map(|(k, v)| (format!("{k:?}"), v.to_string()))
                .collect()
        } else {
            Default::default()
        };

        let barrier_worker_state = self.stream_mgr.inspect_barrier_state().await?;

        let jvm_stack_traces = match dump_jvm_stack_traces() {
            Ok(None) => None,
            Err(err) => Some(err.as_report().to_string()),
            Ok(Some(stack_traces)) => Some(stack_traces),
        };

        Ok(Response::new(StackTraceResponse {
            actor_traces,
            rpc_traces,
            compaction_task_traces,
            inflight_barrier_traces: barrier_traces,
            barrier_worker_state: BTreeMap::from_iter([(
                self.stream_mgr.env.worker_id(),
                barrier_worker_state,
            )]),
            jvm_stack_traces: match jvm_stack_traces {
                Some(stack_traces) => {
                    BTreeMap::from_iter([(self.stream_mgr.env.worker_id(), stack_traces)])
                }
                None => BTreeMap::new(),
            },
            meta_traces: Default::default(),
            node_errors: Default::default(),
            batch_traces,
        }))
    }

    async fn profiling(
        &self,
        request: Request<ProfilingRequest>,
    ) -> Result<Response<ProfilingResponse>, Status> {
        self.profile_service.profiling(request).await
    }

    async fn heap_profiling(
        &self,
        request: Request<HeapProfilingRequest>,
    ) -> Result<Response<HeapProfilingResponse>, Status> {
        self.profile_service.heap_profiling(request)
    }

    async fn list_heap_profiling(
        &self,
        _request: Request<ListHeapProfilingRequest>,
    ) -> Result<Response<ListHeapProfilingResponse>, Status> {
        self.profile_service.list_heap_profiling(_request)
    }

    async fn analyze_heap(
        &self,
        request: Request<AnalyzeHeapRequest>,
    ) -> Result<Response<AnalyzeHeapResponse>, Status> {
        self.profile_service.analyze_heap(request).await
    }

    async fn get_profile_stats(
        &self,
        request: Request<GetProfileStatsRequest>,
    ) -> Result<Response<GetProfileStatsResponse>, Status> {
        let metrics = global_streaming_metrics(MetricLevel::Info);
        let inner = request.into_inner();
        let executor_ids = &inner.executor_ids;
        let fragment_ids = HashSet::from_iter(inner.dispatcher_fragment_ids);
        let stream_node_output_row_count = metrics
            .mem_stream_node_output_row_count
            .collect(executor_ids);
        let stream_node_output_blocking_duration_ns = metrics
            .mem_stream_node_output_blocking_duration_ns
            .collect(executor_ids);

        // Collect count metrics by fragment_ids
        fn collect_by_fragment_ids<T: Collector>(
            m: &T,
            fragment_ids: &HashSet<FragmentId>,
        ) -> HashMap<FragmentId, u64> {
            let mut metrics = HashMap::new();
            for mut metric_family in m.collect() {
                for metric in metric_family.take_metric() {
                    let fragment_id = get_label_infallible(&metric, "fragment_id");
                    if fragment_ids.contains(&fragment_id) {
                        let entry = metrics.entry(fragment_id).or_insert(0);
                        *entry += metric.get_counter().value() as u64;
                    }
                }
            }
            metrics
        }

        let dispatch_fragment_output_row_count =
            collect_by_fragment_ids(&metrics.actor_out_record_cnt, &fragment_ids);
        let dispatch_fragment_output_blocking_duration_ns = collect_by_fragment_ids(
            &metrics.actor_output_buffer_blocking_duration_ns,
            &fragment_ids,
        );
        Ok(Response::new(GetProfileStatsResponse {
            stream_node_output_row_count,
            stream_node_output_blocking_duration_ns,
            dispatch_fragment_output_row_count,
            dispatch_fragment_output_blocking_duration_ns,
        }))
    }

    async fn get_streaming_stats(
        &self,
        _request: Request<GetStreamingStatsRequest>,
    ) -> Result<Response<GetStreamingStatsResponse>, Status> {
        let metrics = global_streaming_metrics(MetricLevel::Info);

        fn collect<T: Collector>(m: &T) -> Vec<Metric> {
            m.collect().into_iter().next().unwrap().take_metric()
        }

        let actor_output_buffer_blocking_duration_ns =
            collect(&metrics.actor_output_buffer_blocking_duration_ns);
        let actor_count = collect(&metrics.actor_count);

        let actor_count: HashMap<_, _> = actor_count
            .iter()
            .map(|m| {
                let fragment_id: u32 = get_label_infallible(m, "fragment_id");
                let count = m.get_gauge().value() as u32;
                (fragment_id, count)
            })
            .collect();

        let mut fragment_stats: HashMap<u32, FragmentStats> = HashMap::new();
        for (&fragment_id, &actor_count) in &actor_count {
            fragment_stats.insert(
                fragment_id,
                FragmentStats {
                    actor_count,
                    current_epoch: 0,
                },
            );
        }

        let actor_current_epoch = collect(&metrics.actor_current_epoch);
        for m in &actor_current_epoch {
            let fragment_id: u32 = get_label_infallible(m, "fragment_id");
            let epoch = m.get_gauge().value() as u64;
            if let Some(s) = fragment_stats.get_mut(&fragment_id) {
                s.current_epoch = if s.current_epoch == 0 {
                    epoch
                } else {
                    u64::min(s.current_epoch, epoch)
                }
            } else {
                warn!(
                    fragment_id = fragment_id,
                    "Miss corresponding actor count metrics"
                );
            }
        }

        let mut relation_stats: HashMap<u32, RelationStats> = HashMap::new();
        let mview_current_epoch = collect(&metrics.materialize_current_epoch);
        for m in &mview_current_epoch {
            let table_id: u32 = get_label_infallible(m, "table_id");
            let epoch = m.get_gauge().value() as u64;
            if let Some(s) = relation_stats.get_mut(&table_id) {
                s.current_epoch = if s.current_epoch == 0 {
                    epoch
                } else {
                    u64::min(s.current_epoch, epoch)
                };
                s.actor_count += 1;
            } else {
                relation_stats.insert(
                    table_id,
                    RelationStats {
                        actor_count: 1,
                        current_epoch: epoch,
                    },
                );
            }
        }

        let mut channel_stats: BTreeMap<String, ChannelStats> = BTreeMap::new();

        for metric in actor_output_buffer_blocking_duration_ns {
            let fragment_id: u32 = get_label_infallible(&metric, "fragment_id");
            let downstream_fragment_id: u32 =
                get_label_infallible(&metric, "downstream_fragment_id");

            let actor_count_to_add =
                if get_label_infallible::<String>(&metric, "actor_id").is_empty() {
                    match actor_count.get(&fragment_id) {
                        Some(&count) => count,
                        None => {
                            // Metrics can be momentarily inconsistent (or stale) across families.
                            // Skip this channel instead of crashing the compute node.
                            warn!(
                                fragment_id = fragment_id,
                                downstream_fragment_id = downstream_fragment_id,
                                "Miss corresponding actor count metrics"
                            );
                            continue;
                        }
                    }
                } else {
                    1
                };

            let key = format!("{}_{}", fragment_id, downstream_fragment_id);
            let channel_stat = channel_stats.entry(key).or_insert_with(|| ChannelStats {
                actor_count: 0,
                output_blocking_duration: 0.,
                recv_row_count: 0,
                send_row_count: 0,
            });

            // When metrics level is Debug, `actor_id` will be removed to reduce metrics.
            // See `src/common/metrics/src/relabeled_metric.rs`
            channel_stat.actor_count += actor_count_to_add;
            channel_stat.output_blocking_duration += metric.get_counter().value();
        }

        let actor_output_row_count = collect(&metrics.actor_out_record_cnt);
        for metric in actor_output_row_count {
            let fragment_id: u32 = get_label_infallible(&metric, "fragment_id");

            // Find out and write to all downstream channels
            let key_prefix = format!("{}_", fragment_id);
            let key_range_end = format!("{}`", fragment_id); // '`' is next to `_`
            for (_, s) in channel_stats.range_mut(key_prefix..key_range_end) {
                s.send_row_count += metric.get_counter().value() as u64;
            }
        }

        let actor_input_row_count = collect(&metrics.actor_in_record_cnt);
        for metric in actor_input_row_count {
            let upstream_fragment_id: u32 = get_label_infallible(&metric, "upstream_fragment_id");
            let fragment_id: u32 = get_label_infallible(&metric, "fragment_id");

            let key = format!("{}_{}", upstream_fragment_id, fragment_id);
            if let Some(s) = channel_stats.get_mut(&key) {
                s.recv_row_count += metric.get_counter().value() as u64;
            }
        }

        let channel_stats = channel_stats.into_iter().collect();
        Ok(Response::new(GetStreamingStatsResponse {
            channel_stats,
            fragment_stats,
            relation_stats,
        }))
    }

    async fn tiered_cache_tracing(
        &self,
        request: Request<TieredCacheTracingRequest>,
    ) -> Result<Response<TieredCacheTracingResponse>, Status> {
        let req = request.into_inner();

        tracing::info!("Update tiered cache tracing config: {req:?}");

        if let Some(cache) = &self.meta_cache {
            if req.enable {
                cache.enable_tracing();
            } else {
                cache.disable_tracing();
            }
            let mut options = TracingOptions::new();
            if let Some(threshold) = req.record_hybrid_insert_threshold_ms {
                options = options
                    .with_record_hybrid_insert_threshold(Duration::from_millis(threshold as _));
            }
            if let Some(threshold) = req.record_hybrid_get_threshold_ms {
                options =
                    options.with_record_hybrid_get_threshold(Duration::from_millis(threshold as _));
            }
            if let Some(threshold) = req.record_hybrid_remove_threshold_ms {
                options = options
                    .with_record_hybrid_remove_threshold(Duration::from_millis(threshold as _));
            }
            if let Some(threshold) = req.record_hybrid_fetch_threshold_ms {
                options = options.with_record_hybrid_get_or_fetch_threshold(Duration::from_millis(
                    threshold as _,
                ));
            }
            cache.update_tracing_options(options);
        }

        if let Some(cache) = &self.block_cache {
            if req.enable {
                cache.enable_tracing();
            } else {
                cache.disable_tracing();
            }
            let mut options = TracingOptions::new();
            if let Some(threshold) = req.record_hybrid_insert_threshold_ms {
                options = options
                    .with_record_hybrid_insert_threshold(Duration::from_millis(threshold as _));
            }
            if let Some(threshold) = req.record_hybrid_get_threshold_ms {
                options =
                    options.with_record_hybrid_get_threshold(Duration::from_millis(threshold as _));
            }
            if let Some(threshold) = req.record_hybrid_remove_threshold_ms {
                options = options
                    .with_record_hybrid_remove_threshold(Duration::from_millis(threshold as _));
            }
            if let Some(threshold) = req.record_hybrid_fetch_threshold_ms {
                options = options.with_record_hybrid_get_or_fetch_threshold(Duration::from_millis(
                    threshold as _,
                ));
            }
            cache.update_tracing_options(options);
        }

        Ok(Response::new(TieredCacheTracingResponse::default()))
    }

    async fn get_table_cache_refill_stats(
        &self,
        _request: Request<GetTableCacheRefillStatsRequest>,
    ) -> Result<Response<GetTableCacheRefillStatsResponse>, Status> {
        let Some(hummock_storage) = &self.hummock_storage else {
            return Ok(Response::new(GetTableCacheRefillStatsResponse {
                stats: "{}".to_owned(),
            }));
        };

        let monitor_snapshot = hummock_storage
            .table_cache_refill_monitor_snapshot()
            .await
            .map_err(|err| {
                Status::internal(format!(
                    "failed to get table cache refill monitor snapshot: {e}",
                    e = err.as_report()
                ))
            })?;
        let stats = TableCacheRefillStats::from(&monitor_snapshot);
        let json_value = serde_json::to_value(stats).map_err(|err| {
            Status::internal(format!(
                "failed to serialize stats: {e}",
                e = err.as_report()
            ))
        })?;
        let json_string_pretty = serde_json::to_string_pretty(&json_value).map_err(|err| {
            Status::internal(format!(
                "failed to serialize stats: {e}",
                e = err.as_report()
            ))
        })?;
        Ok(Response::new(GetTableCacheRefillStatsResponse {
            stats: json_string_pretty,
        }))
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TableCacheRefillStats {
    contexts: HashMap<u32, TableCacheRefillTableStats>,
    streaming: HashMap<u32, Vec<u16>>,
    serving: HashMap<u32, Vec<u16>>,
    policies: HashMap<u32, String>,
    default_policy: String,
    internal: TableCacheRefillInternalStats,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TableCacheRefillTableStats {
    streaming: Option<Vec<u16>>,
    serving: Option<Vec<u16>>,
    policy: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TableCacheRefillInternalStats {
    streaming: HashMap<u32, Vec<Vec<u16>>>,
    serving: HashMap<u32, Vec<u16>>,
}

impl From<&TableCacheRefillMonitorSnapshot> for TableCacheRefillStats {
    fn from(snapshot: &TableCacheRefillMonitorSnapshot) -> Self {
        let contexts = snapshot
            .contexts
            .iter()
            .map(|(table_id, context)| {
                (
                    table_id.as_raw_id(),
                    TableCacheRefillTableStats::from(context.clone()),
                )
            })
            .collect::<HashMap<_, _>>();
        let streaming = contexts
            .iter()
            .filter_map(|(table_id, context)| {
                context
                    .streaming
                    .as_ref()
                    .map(|vnodes| (*table_id, vnodes.clone()))
            })
            .collect();
        let serving = contexts
            .iter()
            .filter_map(|(table_id, context)| {
                context
                    .serving
                    .as_ref()
                    .map(|vnodes| (*table_id, vnodes.clone()))
            })
            .collect();
        let policies = snapshot
            .policies
            .iter()
            .map(|(table_id, policy)| (table_id.as_raw_id(), policy.to_string()))
            .collect();
        let internal_streaming = snapshot
            .streaming_table_vnode_mapping
            .iter()
            // Keep the historical list-of-lists JSON shape. The refiller stores one
            // table-level union bitmap per table.
            .map(|(table_id, bitmap)| (table_id.as_raw_id(), vec![bitmap_to_vnodes(bitmap)]))
            .collect();
        let internal_serving = snapshot
            .serving_table_vnode_mapping
            .iter()
            .map(|(table_id, bitmap)| (table_id.as_raw_id(), bitmap_to_vnodes(bitmap)))
            .collect();

        Self {
            contexts,
            streaming,
            serving,
            policies,
            default_policy: snapshot.default_policy.to_string(),
            internal: TableCacheRefillInternalStats {
                streaming: internal_streaming,
                serving: internal_serving,
            },
        }
    }
}

impl From<TableCacheRefillContext> for TableCacheRefillTableStats {
    fn from(context: TableCacheRefillContext) -> Self {
        Self {
            streaming: context
                .streaming_vnode_bitmap
                .as_ref()
                .map(bitmap_to_vnodes),
            serving: context.serving_vnode_bitmap.as_ref().map(bitmap_to_vnodes),
            policy: context.policy.to_string(),
        }
    }
}

fn bitmap_to_vnodes(bitmap: &Bitmap) -> Vec<u16> {
    bitmap.iter_ones().map(|idx| idx as u16).collect()
}

pub use grpc_middleware::*;
use risingwave_common::metrics::get_label_infallible;
use risingwave_pb::id::FragmentId;

pub mod grpc_middleware {
    pub use risingwave_common_service::{
        AwaitTreeMiddleware, AwaitTreeMiddlewareLayer, AwaitTreeRegistryRef, GrpcCall,
    };
}
