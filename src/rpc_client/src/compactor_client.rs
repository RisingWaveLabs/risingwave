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

use std::sync::Arc;
use std::time::Duration;

use risingwave_common::monitor::EndpointExt;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::retry::exponential_backoff;
use risingwave_pb::configured_monitor_service_client;
use risingwave_pb::hummock::hummock_manager_service_client::HummockManagerServiceClient;
use risingwave_pb::hummock::{
    GetNewObjectIdsRequest, GetNewObjectIdsResponse, ReportCompactionTaskRequest,
    ReportCompactionTaskResponse,
};
use risingwave_pb::meta::system_params_service_client::SystemParamsServiceClient;
use risingwave_pb::meta::{GetSystemParamsRequest, GetSystemParamsResponse};
use risingwave_pb::monitor_service::monitor_service_client::MonitorServiceClient;
use risingwave_pb::monitor_service::{StackTraceRequest, StackTraceResponse};
use tokio::sync::RwLock;
use tokio_retry::strategy::jitter;
use tonic::transport::{Channel, Endpoint};

use crate::error::{Result, RpcError};
use crate::retry_rpc;
const ENDPOINT_KEEP_ALIVE_INTERVAL_SEC: u64 = 60;
const ENDPOINT_KEEP_ALIVE_TIMEOUT_SEC: u64 = 60;

const DEFAULT_RETRY_INTERVAL: u64 = 20;
const DEFAULT_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);
const DEFAULT_RETRY_MAX_ATTEMPTS: usize = 3;
#[derive(Clone)]
pub struct CompactorClient {
    pub monitor_client: MonitorServiceClient<Channel>,
}

impl CompactorClient {
    pub async fn new(host_addr: HostAddr) -> Result<Self> {
        let channel = Endpoint::from_shared(format!("http://{}", host_addr))?
            .connect_timeout(Duration::from_secs(5))
            .monitored_connect("grpc-compactor-client", Default::default())
            .await?;
        Ok(Self {
            monitor_client: configured_monitor_service_client(MonitorServiceClient::new(channel)),
        })
    }

    pub async fn stack_trace(&self, req: StackTraceRequest) -> Result<StackTraceResponse> {
        Ok(self
            .monitor_client
            .clone()
            .stack_trace(req)
            .await
            .map_err(RpcError::from_compactor_status)?
            .into_inner())
    }
}

#[derive(Debug, Clone)]
pub struct GrpcCompactorProxyClientCore {
    hummock_client: HummockManagerServiceClient<Channel>,
    system_params_client: SystemParamsServiceClient<Channel>,
}

impl GrpcCompactorProxyClientCore {
    pub(crate) fn new(channel: Channel) -> Self {
        let hummock_client =
            HummockManagerServiceClient::new(channel.clone()).max_decoding_message_size(usize::MAX);
        let system_params_client = SystemParamsServiceClient::new(channel);

        Self {
            hummock_client,
            system_params_client,
        }
    }
}

/// Client to proxy server. Cloning the instance is lightweight.
///
/// Todo(wcy-fdu): add refresh client interface.
#[derive(Debug, Clone)]
pub struct GrpcCompactorProxyClient {
    pub core: Arc<RwLock<GrpcCompactorProxyClientCore>>,
    endpoint: String,
}

impl GrpcCompactorProxyClient {
    pub async fn new(endpoint: String) -> Self {
        let channel = Self::connect_to_endpoint(endpoint.clone()).await;
        let core = Arc::new(RwLock::new(GrpcCompactorProxyClientCore::new(channel)));
        Self { core, endpoint }
    }

    async fn recreate_core(&self) {
        tracing::info!("GrpcCompactorProxyClient rpc transfer failed, try to reconnect");
        let channel = Self::connect_to_endpoint(self.endpoint.clone()).await;
        let mut core = self.core.write().await;
        *core = GrpcCompactorProxyClientCore::new(channel);
    }

    async fn connect_to_endpoint(endpoint: String) -> Channel {
        let endpoint = Endpoint::from_shared(endpoint).expect("Fail to construct tonic Endpoint");
        endpoint
            .http2_keep_alive_interval(Duration::from_secs(ENDPOINT_KEEP_ALIVE_INTERVAL_SEC))
            .keep_alive_timeout(Duration::from_secs(ENDPOINT_KEEP_ALIVE_TIMEOUT_SEC))
            .connect_timeout(Duration::from_secs(5))
            .monitored_connect("grpc-compactor-proxy-client", Default::default())
            .await
            .expect("Failed to create channel via proxy rpc endpoint.")
    }

    pub async fn get_new_sst_ids(
        &self,
        request: GetNewObjectIdsRequest,
    ) -> std::result::Result<tonic::Response<GetNewObjectIdsResponse>, tonic::Status> {
        retry_rpc!(self, get_new_object_ids, request, GetNewObjectIdsResponse)
    }

    pub async fn report_compaction_task(
        &self,
        request: ReportCompactionTaskRequest,
    ) -> std::result::Result<tonic::Response<ReportCompactionTaskResponse>, tonic::Status> {
        retry_rpc!(
            self,
            report_compaction_task,
            request,
            ReportCompactionTaskResponse
        )
    }

    pub async fn get_system_params(
        &self,
    ) -> std::result::Result<tonic::Response<GetSystemParamsResponse>, tonic::Status> {
        tokio_retry::RetryIf::spawn(
            Self::get_retry_strategy(),
            || async {
                let mut system_params_client = self.core.read().await.system_params_client.clone();
                let rpc_res = system_params_client
                    .get_system_params(GetSystemParamsRequest {})
                    .await;
                if rpc_res.is_err() {
                    self.recreate_core().await;
                }
                rpc_res
            },
            Self::should_retry,
        )
        .await
    }

    #[inline(always)]
    fn get_retry_strategy() -> impl Iterator<Item = Duration> {
        exponential_backoff(
            Duration::from_millis(DEFAULT_RETRY_INTERVAL),
            DEFAULT_RETRY_INTERVAL,
            DEFAULT_RETRY_MAX_DELAY,
        )
        .take(DEFAULT_RETRY_MAX_ATTEMPTS)
        .map(jitter)
    }

    #[inline(always)]
    fn should_retry(status: &tonic::Status) -> bool {
        if status.code() == tonic::Code::Unavailable
            || status.code() == tonic::Code::Unknown
            || (status.code() == tonic::Code::Unauthenticated
                && status.message().contains("invalid auth token"))
        {
            return true;
        }
        false
    }
}

#[macro_export]
macro_rules! retry_rpc {
    ($self:expr, $rpc_call:ident, $request:expr, $response:ty) => {
        tokio_retry::RetryIf::spawn(
            Self::get_retry_strategy(),
            || async {
                let mut hummock_client = $self.core.read().await.hummock_client.clone();
                let rpc_res = hummock_client.$rpc_call($request.clone()).await;
                if rpc_res.is_err() {
                    $self.recreate_core().await;
                }
                rpc_res
            },
            Self::should_retry,
        )
        .await
    };
}
