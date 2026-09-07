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

use risingwave_meta::manager::MetadataManager;
use risingwave_pb::meta::heartbeat_service_server::HeartbeatService;
use risingwave_pb::meta::{HeartbeatRequest, HeartbeatResponse};
use thiserror_ext::AsReport;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct HeartbeatServiceImpl {
    metadata_manager: MetadataManager,
}

impl HeartbeatServiceImpl {
    pub fn new(metadata_manager: MetadataManager) -> Self {
        HeartbeatServiceImpl { metadata_manager }
    }
}

#[async_trait::async_trait]
impl HeartbeatService for HeartbeatServiceImpl {
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .metadata_manager
            .cluster_controller
            .heartbeat(req.node_id)
            .await;

        match result {
            Ok(_) => Ok(Response::new(HeartbeatResponse { status: None })),
            Err(e) => {
                if e.is_invalid_worker() {
                    return Ok(Response::new(HeartbeatResponse {
                        status: Some(risingwave_pb::common::Status {
                            code: risingwave_pb::common::status::Code::UnknownWorker as i32,
                            message: e.to_report_string(),
                        }),
                    }));
                }
                Err(e.into())
            }
        }
    }
}
