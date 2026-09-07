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

use std::process::exit;

use futures::future::try_join_all;
use risingwave_common::config::RpcClientConfig;
use risingwave_pb::compute::ResizeCacheRequest;
use risingwave_pb::meta::GetClusterInfoResponse;
use risingwave_rpc_client::ComputeClient;
use thiserror_ext::AsReport;

use crate::common::CtlContext;

macro_rules! fail {
    ($($arg:tt)*) => {{
        println!($($arg)*);
        exit(1);
    }};
}

pub async fn resize_cache(
    context: &CtlContext,
    meta_cache_capacity: Option<u64>,
    data_cache_capacity: Option<u64>,
    clear_meta_cache: bool,
    clear_data_cache: bool,
) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;

    let GetClusterInfoResponse { worker_nodes, .. } = match meta_client.get_cluster_info().await {
        Ok(resp) => resp,
        Err(e) => {
            fail!("Failed to get cluster info: {}", e.as_report());
        }
    };

    let futures = worker_nodes.iter().map(|worker| async {
        let addr = worker.get_host().expect("worker host must be set");
        let client = ComputeClient::new(addr.into(), &RpcClientConfig::default())
            .await
            .unwrap_or_else(|_| panic!("Cannot open client to compute node {addr:?}"));
        client
            .resize_cache(ResizeCacheRequest {
                meta_cache_capacity: meta_cache_capacity.unwrap_or(0),
                data_cache_capacity: data_cache_capacity.unwrap_or(0),
                clear_meta_cache,
                clear_data_cache,
            })
            .await
    });

    if let Err(e) = try_join_all(futures).await {
        fail!("Failed to resize cache: {}", e.as_report())
    }

    Ok(())
}
