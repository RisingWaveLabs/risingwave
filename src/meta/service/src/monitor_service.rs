// Copyright 2025 RisingWave Labs
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

use risingwave_common::config::ServerConfig;
use risingwave_common_heap_profiling::ProfileServiceImpl;
use risingwave_meta::manager::MetadataManager;
use risingwave_meta::rpc::await_tree::dump_cluster_await_tree;
use risingwave_pb::monitor_service::monitor_service_server::MonitorService;
use risingwave_pb::monitor_service::{self, StackTraceRequest, StackTraceResponse};
use tonic::{Request, Response, Status};

/// The [`MonitorService`] implementation for meta node.
///
/// Currently, only [`MonitorService::stack_trace`] is implemented, which returns the await tree of
/// all nodes in the cluster.
pub struct MonitorServiceImpl {
    pub metadata_manager: MetadataManager,
    pub await_tree_reg: await_tree::Registry,
    pub profile_service: ProfileServiceImpl,
}

impl MonitorServiceImpl {
    pub fn new(
        metadata_manager: MetadataManager,
        await_tree_reg: await_tree::Registry,
        server_config: ServerConfig,
    ) -> Self {
        Self {
            metadata_manager,
            await_tree_reg,
            profile_service: ProfileServiceImpl::new(server_config),
        }
    }
}

#[tonic::async_trait]
impl MonitorService for MonitorServiceImpl {
    async fn stack_trace(
        &self,
        request: Request<StackTraceRequest>,
    ) -> Result<Response<StackTraceResponse>, Status> {
        let request = request.into_inner();
        let actor_traces_format = request.actor_traces_format();

        let result = dump_cluster_await_tree(
            &self.metadata_manager,
            &self.await_tree_reg,
            actor_traces_format,
        )
        .await?;

        Ok(Response::new(result))
    }

    async fn profiling(
        &self,
        request: Request<monitor_service::ProfilingRequest>,
    ) -> Result<Response<monitor_service::ProfilingResponse>, Status> {
        self.profile_service.profiling(request).await
    }

    async fn heap_profiling(
        &self,
        request: Request<monitor_service::HeapProfilingRequest>,
    ) -> Result<Response<monitor_service::HeapProfilingResponse>, Status> {
        self.profile_service.heap_profiling(request)
    }

    async fn list_heap_profiling(
        &self,
        request: Request<monitor_service::ListHeapProfilingRequest>,
    ) -> Result<Response<monitor_service::ListHeapProfilingResponse>, Status> {
        self.profile_service.list_heap_profiling(request)
    }

    async fn analyze_heap(
        &self,
        request: Request<monitor_service::AnalyzeHeapRequest>,
    ) -> Result<Response<monitor_service::AnalyzeHeapResponse>, Status> {
        self.profile_service.analyze_heap(request).await
    }

    async fn get_streaming_stats(
        &self,
        _request: Request<monitor_service::GetStreamingStatsRequest>,
    ) -> Result<Response<monitor_service::GetStreamingStatsResponse>, Status> {
        Err(Status::unimplemented("not implemented in meta node"))
    }

    async fn tiered_cache_tracing(
        &self,
        _request: Request<monitor_service::TieredCacheTracingRequest>,
    ) -> Result<Response<monitor_service::TieredCacheTracingResponse>, Status> {
        Err(Status::unimplemented("not implemented in meta node"))
    }

    async fn get_profile_stats(
        &self,
        _request: Request<monitor_service::GetProfileStatsRequest>,
    ) -> Result<Response<monitor_service::GetProfileStatsResponse>, Status> {
        Err(Status::unimplemented("not implemented in meta node"))
    }

    async fn get_table_cache_refill_stats(
        &self,
        _request: Request<monitor_service::GetTableCacheRefillStatsRequest>,
    ) -> Result<Response<monitor_service::GetTableCacheRefillStatsResponse>, Status> {
        Err(Status::unimplemented("not implemented in meta node"))
    }
}
