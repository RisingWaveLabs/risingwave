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

use clap::Parser;
use home::home_dir;
use risingwave_common::config::MetaBackend;
use risingwave_common::util::resource_util::memory::system_memory_available_bytes;
use risingwave_compactor::CompactorOpts;
use risingwave_compute::ComputeNodeOpts;
use risingwave_compute::memory::config::gradient_reserve_memory_bytes;
use risingwave_frontend::FrontendOpts;
use risingwave_meta_node::MetaNodeOpts;

use crate::ParsedStandaloneOpts;

#[derive(Eq, PartialOrd, PartialEq, Debug, Clone, Parser)]
#[command(
    version,
    about = "[default] The Single Node mode. Start all services in one process, with process-level options. This will be executed if no subcommand is specified"
)]
/// Here we define our own defaults for the single node mode.
pub struct SingleNodeOpts {
    /// The address prometheus polls metrics from.
    #[clap(long, env = "RW_SINGLE_NODE_PROMETHEUS_LISTENER_ADDR")]
    prometheus_listener_addr: Option<String>,

    /// The path to the cluster configuration file.
    #[clap(long, env = "RW_SINGLE_NODE_CONFIG_PATH")]
    config_path: Option<String>,

    /// The store directory used by meta store and object store.
    #[clap(long, env = "RW_SINGLE_NODE_STORE_DIRECTORY")]
    store_directory: Option<String>,

    /// Start RisingWave in-memory.
    #[clap(long, env = "RW_SINGLE_NODE_IN_MEMORY")]
    in_memory: bool,

    /// Exit RisingWave after specified seconds of inactivity.
    #[clap(long, hide = true, env = "RW_SINGLE_NODE_MAX_IDLE_SECS")]
    max_idle_secs: Option<u64>,

    #[clap(flatten)]
    node_opts: NodeSpecificOpts,
}

impl SingleNodeOpts {
    pub fn new_for_playground() -> Self {
        let empty_args = vec![] as Vec<String>;
        let mut opts = SingleNodeOpts::parse_from(empty_args);
        opts.in_memory = true;
        opts
    }
}

/// # Node-Specific Options
///
/// ## Which node-specific options should be here?
///
/// This only includes the options displayed in the CLI parameters.
///
/// 1. An option that will be forced to override by single-node deployment should not be here.
///    - e.g. `meta_addr` will be automatically set to localhost.
/// 2. An option defined in the config file and hidden in the command-line help should not be here.
///    - e.g. `sql_endpoint` is encouraged to be set from config file or environment variables.
/// 3. An option that is only for cloud deployment should not be here.
///    - e.g. `proxy_rpc_endpoint` is used in cloud deployment only, and should not be used by open-source users.
///
/// ## How to add an new option here?
///
/// Options for the other nodes are defined with following convention:
///
/// 1. The option name is the same as the definition in node's `Opts` struct. May add a prefix to avoid conflicts when necessary.
/// 2. The option doesn't have a default value and must be `Option<T>`, so that the default value in the node's `Opts` struct can be used.
/// 3. The option doesn't need to read from environment variables, which will be done in the node's `Opts` struct.
#[derive(Eq, PartialOrd, PartialEq, Debug, Clone, Parser)]
pub struct NodeSpecificOpts {
    // ------- Compute Node Options -------
    /// Total available memory for all the nodes
    #[clap(long)]
    pub total_memory_bytes: Option<usize>,

    /// The parallelism that the compute node will register to the scheduler of the meta service.
    #[clap(long)]
    pub parallelism: Option<usize>,

    // ------- Frontend Node Options -------
    /// The address that this service listens to.
    /// Usually the localhost + desired port.
    #[clap(long)]
    pub listen_addr: Option<String>,

    // ------- Meta Node Options -------
    /// The HTTP REST-API address of the Prometheus instance associated to this cluster.
    /// This address is used to serve `PromQL` queries to Prometheus.
    /// It is also used by Grafana Dashboard Service to fetch metrics and visualize them.
    #[clap(long)]
    pub prometheus_endpoint: Option<String>,

    /// The additional selector used when querying Prometheus.
    ///
    /// The format is same as `PromQL`. Example: `instance="foo",namespace="bar"`
    #[clap(long)]
    pub prometheus_selector: Option<String>,

    // ------- Compactor Node Options -------
    #[clap(long)]
    pub compaction_worker_threads_number: Option<usize>,
}

const HUMMOCK_IN_MEMORY: &str = "hummock+memory";

pub fn map_single_node_opts_to_standalone_opts(opts: SingleNodeOpts) -> ParsedStandaloneOpts {
    // Parse from empty strings to get the default values.
    // Note that environment variables will be used if they are set.
    let empty_args = vec![] as Vec<String>;
    let mut meta_opts = MetaNodeOpts::parse_from(&empty_args);
    let mut compute_opts = ComputeNodeOpts::parse_from(&empty_args);
    let mut frontend_opts = FrontendOpts::parse_from(&empty_args);
    let mut compactor_opts = CompactorOpts::parse_from(&empty_args);

    if let Some(max_idle_secs) = opts.max_idle_secs {
        meta_opts.dangerous_max_idle_secs = Some(max_idle_secs);
    }

    if let Some(prometheus_listener_addr) = &opts.prometheus_listener_addr {
        meta_opts.prometheus_listener_addr = Some(prometheus_listener_addr.clone());
        compute_opts
            .prometheus_listener_addr
            .clone_from(prometheus_listener_addr);
        frontend_opts
            .prometheus_listener_addr
            .clone_from(prometheus_listener_addr);
        compactor_opts
            .prometheus_listener_addr
            .clone_from(prometheus_listener_addr);
    }
    if let Some(config_path) = &opts.config_path {
        meta_opts.config_path.clone_from(config_path);
        compute_opts.config_path.clone_from(config_path);
        frontend_opts.config_path.clone_from(config_path);
        compactor_opts.config_path.clone_from(config_path);
    }

    let store_directory = opts.store_directory.unwrap_or_else(|| {
        let mut home_path = home_dir().unwrap();
        home_path.push(".risingwave");
        home_path.to_str().unwrap().to_owned()
    });

    // Set state store for meta (if not set). It could be set by environment variables before this.
    if meta_opts.state_store.is_none() {
        if opts.in_memory {
            meta_opts.state_store = Some(HUMMOCK_IN_MEMORY.to_owned().into());
        } else {
            let state_store_dir = format!("{}/state_store", store_directory);
            std::fs::create_dir_all(&state_store_dir).unwrap();
            let state_store_url = format!("hummock+fs://{}", state_store_dir);
            meta_opts.state_store = Some(state_store_url.into());
        }

        // FIXME: otherwise it reports: missing system param "data_directory", but I think it should be set by this way...
        meta_opts.data_directory = Some("hummock_001".to_owned());
    }

    // Set meta store for meta (if not set). It could be set by environment variables before this.
    let meta_backend_is_set = match meta_opts.backend {
        Some(MetaBackend::Sql)
        | Some(MetaBackend::Sqlite)
        | Some(MetaBackend::Postgres)
        | Some(MetaBackend::Mysql) => meta_opts.sql_endpoint.is_some(),
        Some(MetaBackend::Mem) => true,
        None => false,
    };
    if !meta_backend_is_set {
        if opts.in_memory {
            meta_opts.backend = Some(MetaBackend::Mem);
        } else {
            meta_opts.backend = Some(MetaBackend::Sqlite);
            let meta_store_dir = format!("{}/meta_store", store_directory);
            std::fs::create_dir_all(&meta_store_dir).unwrap();
            let meta_store_endpoint = format!("{}/single_node.db", meta_store_dir);
            meta_opts.sql_endpoint = Some(meta_store_endpoint.into());
        }
    }

    // Set listen addresses (force to override)
    meta_opts.listen_addr = "127.0.0.1:5690".to_owned();
    meta_opts.advertise_addr = "127.0.0.1:5690".to_owned();
    meta_opts.dashboard_host = Some("0.0.0.0:5691".to_owned());
    compute_opts.listen_addr = "127.0.0.1:5688".to_owned();
    compactor_opts.listen_addr = "127.0.0.1:6660".to_owned();

    // Set Meta addresses for all nodes (force to override)
    let meta_addr = "http://127.0.0.1:5690".to_owned();
    compute_opts.meta_address = meta_addr.parse().unwrap();
    frontend_opts.meta_addr = meta_addr.parse().unwrap();
    compactor_opts.meta_address = meta_addr.parse().unwrap();

    // Allocate memory for each node
    let total_memory_bytes = if let Some(total_memory_bytes) = opts.node_opts.total_memory_bytes {
        total_memory_bytes
    } else {
        system_memory_available_bytes()
    };
    frontend_opts.frontend_total_memory_bytes = memory_for_frontend(total_memory_bytes);
    compactor_opts.compactor_total_memory_bytes = memory_for_compactor(total_memory_bytes);
    compute_opts.total_memory_bytes = total_memory_bytes
        - memory_for_frontend(total_memory_bytes)
        - memory_for_compactor(total_memory_bytes);
    compute_opts.memory_manager_target_bytes =
        Some(gradient_reserve_memory_bytes(total_memory_bytes));

    // Apply node-specific options
    if let Some(parallelism) = opts.node_opts.parallelism {
        compute_opts.parallelism = parallelism;
    }
    if let Some(listen_addr) = opts.node_opts.listen_addr {
        frontend_opts.listen_addr = listen_addr;
    }
    if let Some(prometheus_endpoint) = opts.node_opts.prometheus_endpoint {
        meta_opts.prometheus_endpoint = Some(prometheus_endpoint);
    }
    if let Some(prometheus_selector) = opts.node_opts.prometheus_selector {
        meta_opts.prometheus_selector = Some(prometheus_selector);
    }
    if let Some(n) = opts.node_opts.compaction_worker_threads_number {
        compactor_opts.compaction_worker_threads_number = Some(n);
    }

    let in_memory_state_store = meta_opts
        .state_store
        .as_ref()
        .unwrap()
        .expose()
        .starts_with(HUMMOCK_IN_MEMORY);

    ParsedStandaloneOpts {
        meta_opts: Some(meta_opts),
        compute_opts: Some(compute_opts),
        frontend_opts: Some(frontend_opts),
        // If the state store is in-memory, the compute node will start an embedded compactor.
        compactor_opts: if in_memory_state_store {
            None
        } else {
            Some(compactor_opts)
        },
    }
}

fn memory_for_frontend(total_memory_bytes: usize) -> usize {
    if total_memory_bytes <= (16 << 30) {
        total_memory_bytes / 8
    } else {
        (total_memory_bytes - (16 << 30)) / 16 + (16 << 30) / 8
    }
}

fn memory_for_compactor(total_memory_bytes: usize) -> usize {
    if total_memory_bytes <= (16 << 30) {
        total_memory_bytes / 8
    } else {
        (total_memory_bytes - (16 << 30)) / 16 + (16 << 30) / 8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_playground_in_memory_state_store() {
        let opts = SingleNodeOpts::new_for_playground();
        let standalone_opts = map_single_node_opts_to_standalone_opts(opts);

        // Should not start a compactor.
        assert!(standalone_opts.compactor_opts.is_none());

        assert_eq!(
            (standalone_opts.meta_opts.as_ref().unwrap())
                .state_store
                .as_ref()
                .unwrap()
                .expose(),
            HUMMOCK_IN_MEMORY,
        );
    }

    #[test]
    fn test_secret_store_private_key_is_redacted_in_debug_output() {
        let private_key = "00112233445566778899aabbccddeeff";
        let opts =
            MetaNodeOpts::parse_from(["meta-node", "--secret-store-private-key-hex", private_key]);

        let debug_output = format!("{opts:?}");
        assert!(debug_output.contains("secret_store_private_key_hex"));
        assert!(!debug_output.contains(private_key));
        assert!(debug_output.contains("secret_store_private_key_hex: Some([REDACTED"));
    }
}
