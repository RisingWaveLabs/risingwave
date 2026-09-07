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

pub mod opendal_object_store;

use opendal::Operator;
use opendal::layers::ConcurrentLimitLayer;
pub use opendal_object_store::*;
use risingwave_common::config::ObjectStoreConfig;

#[cfg(feature = "hdfs-backend")]
pub mod hdfs;
pub mod webhdfs;

pub mod gcs;

pub mod obs;

pub mod azblob;
pub mod oss;
pub mod s3;

pub mod fs;

// To make sure the the operation is consistent, we should specially set `atomic_write_dir` for fs, hdfs and webhdfs services.
const ATOMIC_WRITE_DIR: &str = "atomic_write_dir/";

fn new_operator(config: &ObjectStoreConfig, op: Operator) -> Operator {
    // Tokio semaphore rejects values above `usize::MAX >> 3`.
    const UNLIMITED_OPERATION_CONCURRENCY: usize = usize::MAX >> 3;

    if config.req_concurrency_limit > 0 || config.http_concurrent_limit > 0 {
        let operation_concurrency_limit = if config.req_concurrency_limit > 0 {
            config.req_concurrency_limit
        } else {
            UNLIMITED_OPERATION_CONCURRENCY
        };
        let mut concurrent_limit_layer = ConcurrentLimitLayer::new(operation_concurrency_limit);
        if config.http_concurrent_limit > 0 {
            concurrent_limit_layer =
                concurrent_limit_layer.with_http_concurrent_limit(config.http_concurrent_limit);
        }
        op.layer(concurrent_limit_layer)
    } else {
        op
    }
}
