// Copyright 2024 RisingWave Labs
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

use async_trait::async_trait;
use risingwave_common::config::{MAX_CONNECTION_WINDOW_SIZE, RpcClientConfig};
use risingwave_common::monitor::{EndpointExt, TcpConfig};
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::retry::exponential_backoff;
use risingwave_pb::frontend_service::frontend_service_client::FrontendServiceClient;
use risingwave_pb::frontend_service::{
    CancelRunningSqlRequest, CancelRunningSqlResponse, GetAllCursorsRequest, GetAllCursorsResponse,
    GetAllSubCursorsRequest, GetAllSubCursorsResponse, GetRunningSqlsRequest,
    GetRunningSqlsResponse, GetTableReplacePlanRequest, GetTableReplacePlanResponse,
};
use tokio_retry::strategy::jitter;
use tonic::Response;
use tonic::transport::Endpoint;

use crate::channel::{Channel, WrappedChannelExt};
use crate::error::Result;
use crate::{RpcClient, RpcClientPool};

const DEFAULT_RETRY_INTERVAL: u64 = 50;
const DEFAULT_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);
const DEFAULT_RETRY_MAX_ATTEMPTS: usize = 10;

#[derive(Clone)]
struct FrontendClient(FrontendServiceClient<Channel>);

impl FrontendClient {
    async fn new(host_addr: HostAddr, opts: &RpcClientConfig) -> Result<Self> {
        let channel = Endpoint::from_shared(format!("http://{}", host_addr))?
            .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
            .connect_timeout(Duration::from_secs(opts.connect_timeout_secs))
            .monitored_connect(
                "grpc-frontend-client",
                TcpConfig {
                    tcp_nodelay: true,
                    ..Default::default()
                },
            )
            .await?
            .wrapped();

        Ok(Self(
            FrontendServiceClient::new(channel).max_decoding_message_size(usize::MAX),
        ))
    }
}

// similar to the stream_client used in the Meta node
pub type FrontendClientPool = RpcClientPool<FrontendRetryClient>;
pub type FrontendClientPoolRef = Arc<FrontendClientPool>;

#[async_trait]
impl RpcClient for FrontendRetryClient {
    async fn new_client(host_addr: HostAddr, opts: &RpcClientConfig) -> Result<Self> {
        Self::new(host_addr, opts).await
    }
}

#[derive(Clone)]
pub struct FrontendRetryClient {
    client: FrontendClient,
}

impl FrontendRetryClient {
    async fn new(host_addr: HostAddr, opts: &RpcClientConfig) -> Result<Self> {
        let client = FrontendClient::new(host_addr, opts).await?;
        Ok(Self { client })
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

    fn should_retry(status: &tonic::Status) -> bool {
        if status.code() == tonic::Code::Unavailable
            || status.code() == tonic::Code::Unknown
            || status.code() == tonic::Code::Unauthenticated
            || status.code() == tonic::Code::Aborted
        {
            return true;
        }
        false
    }

    pub async fn get_table_replace_plan(
        &self,
        request: GetTableReplacePlanRequest,
    ) -> std::result::Result<Response<GetTableReplacePlanResponse>, tonic::Status> {
        tokio_retry::RetryIf::spawn(
            Self::get_retry_strategy(),
            || async {
                self.client
                    .clone()
                    .0
                    .get_table_replace_plan(request.clone())
                    .await
            },
            Self::should_retry,
        )
        .await
    }

    pub async fn get_running_sqls(
        &self,
        request: GetRunningSqlsRequest,
    ) -> std::result::Result<Response<GetRunningSqlsResponse>, tonic::Status> {
        self.client.0.clone().get_running_sqls(request).await
    }

    pub async fn get_all_cursors(
        &self,
        request: GetAllCursorsRequest,
    ) -> std::result::Result<Response<GetAllCursorsResponse>, tonic::Status> {
        self.client.0.clone().get_all_cursors(request).await
    }

    pub async fn get_all_sub_cursors(
        &self,
        request: GetAllSubCursorsRequest,
    ) -> std::result::Result<Response<GetAllSubCursorsResponse>, tonic::Status> {
        self.client.0.clone().get_all_sub_cursors(request).await
    }

    pub async fn cancel_running_sql(
        &self,
        request: CancelRunningSqlRequest,
    ) -> std::result::Result<Response<CancelRunningSqlResponse>, tonic::Status> {
        self.client.0.clone().cancel_running_sql(request).await
    }
}
