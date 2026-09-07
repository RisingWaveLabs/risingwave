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

use std::sync::Arc;

use foyer::HybridCache;
use risingwave_batch::task::BatchManager;
use risingwave_common::error::tonic::ToTonicStatus;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_pb::compute::config_service_server::ConfigService;
use risingwave_pb::compute::{
    ResizeCacheRequest, ResizeCacheResponse, ShowConfigRequest, ShowConfigResponse,
};
use risingwave_storage::hummock::{Block, Sstable, SstableBlockIndex};
use risingwave_stream::task::LocalStreamManager;
use thiserror_ext::AsReport;
use tonic::{Code, Request, Response, Status};

pub struct ConfigServiceImpl {
    batch_mgr: Arc<BatchManager>,
    stream_mgr: LocalStreamManager,
    meta_cache: Option<HybridCache<HummockSstableObjectId, Box<Sstable>>>,
    block_cache: Option<HybridCache<SstableBlockIndex, Box<Block>>>,
}

#[async_trait::async_trait]
impl ConfigService for ConfigServiceImpl {
    async fn show_config(
        &self,
        _request: Request<ShowConfigRequest>,
    ) -> Result<Response<ShowConfigResponse>, Status> {
        let batch_config = serde_json::to_string(self.batch_mgr.config())
            .map_err(|e| e.to_status(Code::Internal, "compute"))?;
        // TODO(config): show overridden config for specific job
        let stream_config = serde_json::to_string(&self.stream_mgr.env.global_config())
            .map_err(|e| e.to_status(Code::Internal, "compute"))?;

        let show_config_response = ShowConfigResponse {
            batch_config,
            stream_config,
        };
        Ok(Response::new(show_config_response))
    }

    async fn resize_cache(
        &self,
        request: Request<ResizeCacheRequest>,
    ) -> Result<Response<ResizeCacheResponse>, Status> {
        let req = request.into_inner();

        // A zero capacity means "do not resize" for backward compatibility. The clear flags
        // independently trigger HybridCache::clear() and never change the configured capacity.
        if let Some(meta_cache) = &self.meta_cache {
            if req.clear_meta_cache {
                meta_cache
                    .clear()
                    .await
                    .map_err(|e| Status::internal(e.to_report_string()))?;
                tracing::info!("clear meta file cache");
            }

            if req.meta_cache_capacity > 0 {
                match meta_cache.memory().resize(req.meta_cache_capacity as _) {
                    Ok(_) => tracing::info!(
                        "resize meta cache capacity to {:?}",
                        req.meta_cache_capacity
                    ),
                    Err(e) => return Err(Status::internal(e.to_report_string())),
                }
            }
        }

        if let Some(block_cache) = &self.block_cache {
            if req.clear_data_cache {
                block_cache
                    .clear()
                    .await
                    .map_err(|e| Status::internal(e.to_report_string()))?;
                tracing::info!("clear data file cache");
            }

            if req.data_cache_capacity > 0 {
                match block_cache.memory().resize(req.data_cache_capacity as _) {
                    Ok(_) => tracing::info!(
                        "resize data cache capacity to {:?}",
                        req.data_cache_capacity
                    ),
                    Err(e) => return Err(Status::internal(e.to_report_string())),
                }
            }
        }

        Ok(Response::new(ResizeCacheResponse {}))
    }
}

impl ConfigServiceImpl {
    pub fn new(
        batch_mgr: Arc<BatchManager>,
        stream_mgr: LocalStreamManager,
        meta_cache: Option<HybridCache<HummockSstableObjectId, Box<Sstable>>>,
        block_cache: Option<HybridCache<SstableBlockIndex, Box<Block>>>,
    ) -> Self {
        Self {
            batch_mgr,
            stream_mgr,
            meta_cache,
            block_cache,
        }
    }
}
