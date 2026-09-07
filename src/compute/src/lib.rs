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

#![feature(coroutines)]
#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]
#![cfg_attr(coverage, feature(coverage_attribute))]
#![warn(clippy::large_futures, clippy::large_stack_frames)]

#[macro_use]
extern crate tracing;

pub mod memory;
pub mod observer;
pub mod rpc;
pub mod server;
pub mod telemetry;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use clap::Parser;
use risingwave_common::config::{AsyncStackTraceOption, MetricLevel, OverrideConfig, Role};
use risingwave_common::util::meta_addr::MetaAddressStrategy;
use risingwave_common::util::resource_util::cpu::total_cpu_available;
use risingwave_common::util::resource_util::memory::system_memory_available_bytes;
use risingwave_common::util::tokio_util::sync::CancellationToken;
use risingwave_common::util::worker_util::DEFAULT_RESOURCE_GROUP;

/// If `total_memory_bytes` is not specified, the default memory limit will be set to
/// the system memory limit multiplied by this proportion
const DEFAULT_MEMORY_PROPORTION: f64 = 0.7;

/// Command-line arguments for compute-node.
#[derive(Parser, Clone, Debug, OverrideConfig)]
#[command(
    version,
    about = "The worker node that executes query plans and handles data ingestion and output"
)]
pub struct ComputeNodeOpts {
    // TODO: rename to listen_addr and separate out the port.
    /// The address that this service listens to.
    /// Usually the localhost + desired port.
    #[clap(long, env = "RW_LISTEN_ADDR", default_value = "127.0.0.1:5688")]
    pub listen_addr: String,

    /// The address for contacting this instance of the service.
    /// This would be synonymous with the service's "public address"
    /// or "identifying address".
    /// Optional, we will use `listen_addr` if not specified.
    #[clap(long, env = "RW_ADVERTISE_ADDR")]
    pub advertise_addr: Option<String>,

    /// We will start a http server at this address via `MetricsManager`.
    /// Then the prometheus instance will poll the metrics from this address.
    #[clap(
        long,
        env = "RW_PROMETHEUS_LISTENER_ADDR",
        default_value = "127.0.0.1:1222"
    )]
    pub prometheus_listener_addr: String,

    #[clap(long, env = "RW_META_ADDR", default_value = "http://127.0.0.1:5690")]
    pub meta_address: MetaAddressStrategy,

    /// The path of `risingwave.toml` configuration file.
    ///
    /// If empty, default configuration values will be used.
    #[clap(long, env = "RW_CONFIG_PATH", default_value = "")]
    pub config_path: String,

    /// Total available memory for the compute node in bytes. Used by both computing and storage.
    #[clap(long, env = "RW_TOTAL_MEMORY_BYTES", default_value_t = default_total_memory_bytes())]
    pub total_memory_bytes: usize,

    /// Reserved memory for the compute node in bytes.
    /// If not set, a portion (default to 30% for the first 16GB and 20% for the rest)
    /// for the `total_memory_bytes` will be used as the reserved memory.
    ///
    /// The total memory compute and storage can use is `total_memory_bytes` - `reserved_memory_bytes`.
    #[clap(long, env = "RW_RESERVED_MEMORY_BYTES")]
    pub reserved_memory_bytes: Option<usize>,

    /// Target memory usage for Memory Manager.
    /// If not set, the default value is `total_memory_bytes` - `reserved_memory_bytes`
    ///
    /// It's strongly recommended to set it for standalone deployment.
    ///
    /// ## Why need this?
    ///
    /// Our [`crate::memory::manager::MemoryManager`] works by reading the memory statistics from
    /// Jemalloc. This is fine when running the compute node alone; while for standalone mode,
    /// the memory usage of **all nodes** are counted. Thus, we need to pass a reasonable total
    /// usage so that the memory is kept around this value.
    #[clap(long, env = "RW_MEMORY_MANAGER_TARGET_BYTES")]
    pub memory_manager_target_bytes: Option<usize>,

    /// The parallelism that the compute node will register to the scheduler of the meta service.
    #[clap(long, env = "RW_PARALLELISM", default_value_t = default_parallelism())]
    #[override_opts(if_absent, path = streaming.actor_runtime_worker_threads_num)]
    pub parallelism: usize,

    /// Resource group for scheduling, default value is "default"
    #[clap(long, env = "RW_RESOURCE_GROUP", default_value_t = default_resource_group())]
    pub resource_group: String,

    /// Decides whether the compute node can be used for streaming and serving.
    #[clap(long, env = "RW_COMPUTE_NODE_ROLE", value_enum, default_value_t = default_role())]
    pub role: Role,

    /// Used for control the metrics level, similar to log level.
    ///
    /// level = 0: disable metrics
    /// level > 0: enable metrics
    #[clap(long, hide = true, env = "RW_METRICS_LEVEL")]
    #[override_opts(path = server.metrics_level)]
    pub metrics_level: Option<MetricLevel>,

    /// Path to data file cache data directory.
    /// Left empty to disable file cache.
    #[clap(long, hide = true, env = "RW_DATA_FILE_CACHE_DIR")]
    #[override_opts(path = storage.data_file_cache.dir)]
    pub data_file_cache_dir: Option<String>,

    /// Path to meta file cache data directory.
    /// Left empty to disable file cache.
    #[clap(long, hide = true, env = "RW_META_FILE_CACHE_DIR")]
    #[override_opts(path = storage.meta_file_cache.dir)]
    pub meta_file_cache_dir: Option<String>,

    /// Enable async stack tracing through `await-tree` for risectl.
    #[clap(long, hide = true, env = "RW_ASYNC_STACK_TRACE", value_enum)]
    #[override_opts(path = streaming.async_stack_trace)]
    pub async_stack_trace: Option<AsyncStackTraceOption>,

    /// Enable heap profile dump when memory usage is high.
    #[clap(long, hide = true, env = "RW_HEAP_PROFILING_DIR")]
    #[override_opts(path = server.heap_profiling.dir)]
    pub heap_profiling_dir: Option<String>,

    /// Endpoint of the connector node.
    #[deprecated = "connector node has been deprecated."]
    #[clap(long, hide = true, env = "RW_CONNECTOR_RPC_ENDPOINT")]
    pub connector_rpc_endpoint: Option<String>,

    /// The path of the temp secret file directory.
    #[clap(
        long,
        hide = true,
        env = "RW_TEMP_SECRET_FILE_DIR",
        default_value = "./secrets"
    )]
    pub temp_secret_file_dir: String,
}

impl risingwave_common::opts::Opts for ComputeNodeOpts {
    fn name() -> &'static str {
        "compute"
    }

    fn meta_addr(&self) -> MetaAddressStrategy {
        self.meta_address.clone()
    }
}

fn validate_opts(opts: &ComputeNodeOpts) {
    let system_memory_available_bytes = system_memory_available_bytes();
    if opts.total_memory_bytes > system_memory_available_bytes {
        let error_msg = format!(
            "total_memory_bytes {} is larger than the total memory available bytes {} that can be acquired.",
            opts.total_memory_bytes, system_memory_available_bytes
        );
        tracing::error!(error_msg);
        panic!("{}", error_msg);
    }
    if opts.parallelism == 0 {
        let error_msg = "parallelism should not be zero";
        tracing::error!(error_msg);
        panic!("{}", error_msg);
    }
    let total_cpu_available = total_cpu_available().ceil() as usize;
    if opts.parallelism > total_cpu_available {
        let error_msg = format!(
            "parallelism {} is larger than the total cpu available {} that can be acquired.",
            opts.parallelism, total_cpu_available
        );
        tracing::warn!(error_msg);
    }
}

use crate::server::compute_node_serve;

/// Start compute node
pub fn start(
    opts: ComputeNodeOpts,
    shutdown: CancellationToken,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        tracing::info!("options: {:?}", opts);
        validate_opts(&opts);

        let listen_addr = opts.listen_addr.parse().unwrap();

        let advertise_addr = opts
            .advertise_addr
            .as_ref()
            .unwrap_or_else(|| {
                tracing::warn!("advertise addr is not specified, defaulting to listen_addr");
                &opts.listen_addr
            })
            .parse()
            .unwrap();
        tracing::info!("advertise addr is {}", advertise_addr);

        compute_node_serve(listen_addr, advertise_addr, Arc::new(opts), shutdown).await;
    })
}

pub fn default_total_memory_bytes() -> usize {
    (system_memory_available_bytes() as f64 * DEFAULT_MEMORY_PROPORTION) as usize
}

pub fn default_parallelism() -> usize {
    total_cpu_available().ceil() as usize
}

pub fn default_resource_group() -> String {
    DEFAULT_RESOURCE_GROUP.to_owned()
}

pub fn default_role() -> Role {
    Role::Both
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_role_rejects_none() {
        assert!(ComputeNodeOpts::try_parse_from(["compute", "--role", "none"]).is_err());
    }
}
