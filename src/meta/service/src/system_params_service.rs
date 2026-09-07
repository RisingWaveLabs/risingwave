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

use async_trait::async_trait;
use futures::future::try_join_all;
use risingwave_common::config::RpcClientConfig;
use risingwave_common::system_param::LICENSE_KEY_KEY;
use risingwave_meta::controller::system_param::SystemParamsControllerRef;
use risingwave_meta::manager::MetadataManager;
use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::system_params_service_server::SystemParamsService;
use risingwave_pb::meta::{
    ClearFileCacheRequest, ClearFileCacheResponse, GetSystemParamsRequest, GetSystemParamsResponse,
    SetSystemParamRequest, SetSystemParamResponse,
};
use risingwave_rpc_client::ComputeClient;
use thiserror_ext::AsReport;
use tonic::{Request, Response, Status};

pub struct SystemParamsServiceImpl {
    system_params_manager: SystemParamsControllerRef,
    metadata_manager: MetadataManager,

    /// Whether the license key is managed by license key file, i.e., `--license-key-path` is set.
    managed_license_key: bool,
}

impl SystemParamsServiceImpl {
    pub fn new(
        system_params_manager: SystemParamsControllerRef,
        metadata_manager: MetadataManager,
        managed_license_key: bool,
    ) -> Self {
        Self {
            system_params_manager,
            metadata_manager,
            managed_license_key,
        }
    }
}

#[async_trait]
impl SystemParamsService for SystemParamsServiceImpl {
    async fn get_system_params(
        &self,
        _request: Request<GetSystemParamsRequest>,
    ) -> Result<Response<GetSystemParamsResponse>, Status> {
        let params = self.system_params_manager.get_pb_params().await;

        Ok(Response::new(GetSystemParamsResponse {
            params: Some(params),
        }))
    }

    async fn set_system_param(
        &self,
        request: Request<SetSystemParamRequest>,
    ) -> Result<Response<SetSystemParamResponse>, Status> {
        let req = request.into_inner();

        // When license key path is specified, license key from system parameters can be easily
        // overwritten. So we simply reject this case.
        if self.managed_license_key && req.param == LICENSE_KEY_KEY {
            return Err(Status::permission_denied(
                "cannot alter license key manually when \
                argument `--license-key-path` (or env var `RW_LICENSE_KEY_PATH`) is set, \
                please update the license key file instead",
            ));
        }

        let params = self
            .system_params_manager
            .set_param(&req.param, req.value)
            .await?;

        Ok(Response::new(SetSystemParamResponse {
            params: Some(params),
        }))
    }

    async fn clear_file_cache(
        &self,
        request: Request<ClearFileCacheRequest>,
    ) -> Result<Response<ClearFileCacheResponse>, Status> {
        let request = request.into_inner();
        let worker_nodes = self
            .metadata_manager
            .list_worker_node(Some(WorkerType::ComputeNode), None)
            .await
            .map_err(|e| Status::internal(e.to_report_string()))?;

        let clear_futures = worker_nodes.into_iter().map(|worker| async move {
            let worker_id = worker.id;
            let host = worker
                .get_host()
                .map_err(|e| Status::internal(e.to_report_string()))?
                .clone();
            let client = ComputeClient::new((&host).into(), &RpcClientConfig::default())
                .await
                .map_err(|e| {
                    Status::internal(format!(
                        "connect to compute node {worker_id}: {}",
                        e.as_report()
                    ))
                })?;
            client
                .resize_cache(risingwave_pb::compute::ResizeCacheRequest {
                    meta_cache_capacity: 0,
                    data_cache_capacity: 0,
                    clear_meta_cache: request.clear_meta_cache,
                    clear_data_cache: request.clear_data_cache,
                })
                .await
                .map_err(|e| {
                    Status::internal(format!(
                        "clear file cache on {worker_id}: {}",
                        e.as_report()
                    ))
                })?;
            Ok::<_, Status>(())
        });

        try_join_all(clear_futures).await?;
        Ok(Response::new(ClearFileCacheResponse {}))
    }
}
