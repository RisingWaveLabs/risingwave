// Copyright 2026 RisingWave Labs
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
use risingwave_pb::monitor_service::monitor_service_server::MonitorService;
use risingwave_pb::monitor_service::{
    AnalyzeHeapRequest, AnalyzeHeapResponse, GetProfileStatsRequest, GetProfileStatsResponse,
    GetStreamingStatsRequest, GetStreamingStatsResponse, GetTableCacheRefillStatsRequest,
    GetTableCacheRefillStatsResponse, HeapProfilingRequest, HeapProfilingResponse,
    ListHeapProfilingRequest, ListHeapProfilingResponse, ProfilingRequest, ProfilingResponse,
    StackTraceRequest, StackTraceResponse, TieredCacheTracingRequest, TieredCacheTracingResponse,
};
use tonic::{Request, Response, Status};

/// Frontend implementation of monitor RPCs, currently reusing the profile service only.
pub struct MonitorServiceImpl {
    profile_service: ProfileServiceImpl,
}

impl MonitorServiceImpl {
    pub fn new(server_config: ServerConfig) -> Self {
        Self {
            profile_service: ProfileServiceImpl::new(server_config),
        }
    }
}

#[async_trait::async_trait]
impl MonitorService for MonitorServiceImpl {
    async fn stack_trace(
        &self,
        _request: Request<StackTraceRequest>,
    ) -> Result<Response<StackTraceResponse>, Status> {
        Err(Status::unimplemented("not implemented in frontend node"))
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
        request: Request<ListHeapProfilingRequest>,
    ) -> Result<Response<ListHeapProfilingResponse>, Status> {
        self.profile_service.list_heap_profiling(request)
    }

    async fn analyze_heap(
        &self,
        request: Request<AnalyzeHeapRequest>,
    ) -> Result<Response<AnalyzeHeapResponse>, Status> {
        self.profile_service.analyze_heap(request).await
    }

    async fn get_streaming_stats(
        &self,
        _request: Request<GetStreamingStatsRequest>,
    ) -> Result<Response<GetStreamingStatsResponse>, Status> {
        Err(Status::unimplemented("not implemented in frontend node"))
    }

    async fn tiered_cache_tracing(
        &self,
        _request: Request<TieredCacheTracingRequest>,
    ) -> Result<Response<TieredCacheTracingResponse>, Status> {
        Err(Status::unimplemented("not implemented in frontend node"))
    }

    async fn get_profile_stats(
        &self,
        _request: Request<GetProfileStatsRequest>,
    ) -> Result<Response<GetProfileStatsResponse>, Status> {
        Err(Status::unimplemented("not implemented in frontend node"))
    }

    async fn get_table_cache_refill_stats(
        &self,
        _request: Request<GetTableCacheRefillStatsRequest>,
    ) -> Result<Response<GetTableCacheRefillStatsResponse>, Status> {
        Err(Status::unimplemented("not implemented in frontend node"))
    }
}
