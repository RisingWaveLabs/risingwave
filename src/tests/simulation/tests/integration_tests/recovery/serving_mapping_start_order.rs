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

#![cfg(madsim)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use itertools::Itertools;
use madsim::net::ipvs::ServiceAddr;
use risingwave_batch::rpc::service::madsim_test_utils::{
    create_task_count, reset_create_task_counts,
};
use risingwave_common::RW_VERSION;
use risingwave_common::config::RwConfig;
use risingwave_common::id::TableId;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::meta_addr::MetaAddressStrategy;
use risingwave_common::util::tokio_util::sync::CancellationToken;
use risingwave_common::util::worker_util::DEFAULT_RESOURCE_GROUP;
use risingwave_pb::common::worker_node::Property;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_rpc_client::MetaClient;
use risingwave_simulation::client::RisingWave;
use risingwave_simulation::cluster::{Cluster, Configuration};
use sqllogictest::{AsyncDB, DBOutput};
use tokio::time::sleep;

const WAIT_TIMEOUT: Duration = Duration::from_secs(100);
const POLL_INTERVAL: Duration = Duration::from_millis(500);
const FRONTEND_HOSTS: [&str; 2] = ["192.168.2.1", "192.168.2.2"];
const COMPUTE_HOSTS: [&str; 2] = ["192.168.3.1", "192.168.3.2"];
const COMPUTE_NODES: [&str; 2] = ["compute-1", "compute-2"];
const UPGRADED_VERSION: &str = "rw-version-from-newer-binary";
const SIMULATED_RW_VERSION_ENV: &str = "RW_SIMULATED_RW_VERSION";
const SIMULATED_RW_VERSION_NODES_ENV: &str = "RW_SIMULATED_RW_VERSION_NODES";

struct SimulatedRwVersionGuard;

impl SimulatedRwVersionGuard {
    fn enable(version: &str, nodes: &[&str]) -> Self {
        unsafe {
            std::env::set_var(SIMULATED_RW_VERSION_ENV, version);
            std::env::set_var(SIMULATED_RW_VERSION_NODES_ENV, nodes.join(","));
        }
        Self
    }
}

impl Drop for SimulatedRwVersionGuard {
    fn drop(&mut self) {
        unsafe {
            std::env::remove_var(SIMULATED_RW_VERSION_ENV);
            std::env::remove_var(SIMULATED_RW_VERSION_NODES_ENV);
        }
    }
}

fn serving_mapping_config() -> Configuration {
    let mut config = Configuration::default();
    config.frontend_nodes = 0;
    config.compute_nodes = 0;
    config.compactor_nodes = 0;
    config.compute_node_cores = 1;
    config
}

fn start_frontend_node(cluster: &Cluster, index: usize) {
    let config = cluster.config();
    madsim::net::NetSim::current().global_ipvs().add_server(
        ServiceAddr::Tcp("192.168.2.0:4566".into()),
        &format!("192.168.2.{index}:4566"),
    );

    let opts = risingwave_frontend::FrontendOpts::parse_from([
        "frontend-node",
        "--config-path",
        config.config_path.as_str(),
        "--listen-addr",
        "0.0.0.0:4566",
        "--health-check-listener-addr",
        "0.0.0.0:6786",
        "--advertise-addr",
        &format!("192.168.2.{index}:4566"),
        "--temp-secret-file-dir",
        &format!("./secrets/frontend-{index}"),
    ]);
    cluster
        .handle()
        .create_node()
        .name(format!("frontend-{index}"))
        .ip([192, 168, 2, index as u8].into())
        .init(move || risingwave_frontend::start(opts.clone(), CancellationToken::new()))
        .build();
}

fn start_compute_node(cluster: &Cluster, index: usize) {
    let config = cluster.config();
    let opts = risingwave_compute::ComputeNodeOpts::parse_from([
        "compute-node",
        "--config-path",
        config.config_path.as_str(),
        "--listen-addr",
        "0.0.0.0:5688",
        "--advertise-addr",
        &format!("192.168.3.{index}:5688"),
        "--total-memory-bytes",
        "6979321856",
        "--parallelism",
        &config.compute_node_cores.to_string(),
        "--temp-secret-file-dir",
        &format!("./secrets/compute-{index}"),
        "--resource-group",
        DEFAULT_RESOURCE_GROUP,
        "--role",
        "both",
    ]);
    cluster
        .handle()
        .create_node()
        .name(format!("compute-{index}"))
        .ip([192, 168, 3, index as u8].into())
        .cores(config.compute_node_cores)
        .init(move || risingwave_compute::start(opts.clone(), CancellationToken::new()))
        .build();
}

async fn start_frontend_nodes(cluster: &Cluster) {
    for index in 1..=2 {
        start_frontend_node(cluster, index);
    }
    sleep(Duration::from_secs(5)).await;
}

async fn start_compute_nodes(cluster: &Cluster) {
    for index in 1..=2 {
        start_compute_node(cluster, index);
    }
    sleep(Duration::from_secs(5)).await;
}

async fn run_on_frontend(
    cluster: &Cluster,
    host: impl Into<String>,
    sql: impl Into<String>,
) -> Result<String> {
    let host = host.into();
    let sql = sql.into();
    cluster
        .run_on_client(async move {
            let mut client = RisingWave::connect(host.clone(), "dev".to_owned())
                .await
                .with_context(|| format!("failed to connect to frontend {host}"))?;
            let output = client
                .run(&sql)
                .await
                .with_context(|| format!("failed to run query on frontend {host}: {sql}"))?;
            Ok(match output {
                DBOutput::Rows { rows, .. } => rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|value| value.to_string())
                            .collect_vec()
                            .join(" ")
                    })
                    .collect_vec()
                    .join("\n"),
                _ => String::new(),
            })
        })
        .await
}

async fn wait_frontend_result(
    cluster: &Cluster,
    host: &str,
    sql: &str,
    expected: &str,
) -> Result<()> {
    let host = host.to_owned();
    let sql = sql.to_owned();
    let expected = expected.to_owned();
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            if run_on_frontend(cluster, host.clone(), sql.clone())
                .await
                .is_ok_and(|result| result.trim() == expected)
            {
                return Ok(());
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| anyhow!("timed out waiting for `{sql}` on {host} to return `{expected}`"))?
}

async fn wait_worker_versions_on_all_frontends(cluster: &Cluster, expected: &str) -> Result<()> {
    let sql = format!(
        "SELECT count(*), count(*) FILTER (WHERE rw_version = '{}') \
         FROM rw_catalog.rw_worker_nodes \
         WHERE type IN (\
             'WORKER_TYPE_META', \
             'WORKER_TYPE_FRONTEND', \
             'WORKER_TYPE_COMPUTE_NODE'\
         );",
        RW_VERSION
    );
    for host in FRONTEND_HOSTS {
        wait_frontend_result(cluster, host, &sql, expected).await?;
    }
    Ok(())
}

async fn wait_batch_query_on_frontend(
    cluster: &Cluster,
    frontend_host: &str,
    table_name: &str,
) -> Result<()> {
    let sql = format!("SELECT count(*), sum(v) FROM {table_name};");
    wait_frontend_result(cluster, frontend_host, &sql, "1000 500500").await
}

async fn wait_frontend_schedules_batch_query_to_same_version_compute(
    cluster: &Cluster,
    frontend_host: &str,
    table_name: &str,
    expected_computes: &[&str],
    mismatched_computes: &[&str],
) -> Result<()> {
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            reset_create_task_counts();
            wait_batch_query_on_frontend(cluster, frontend_host, table_name).await?;

            let expected_counts = expected_computes
                .iter()
                .map(|compute| (*compute, create_task_count(compute)))
                .collect_vec();
            let mismatched_counts = mismatched_computes
                .iter()
                .map(|compute| (*compute, create_task_count(compute)))
                .collect_vec();
            if expected_counts.iter().all(|(_, count)| *count > 0)
                && mismatched_counts.iter().all(|(_, count)| *count == 0)
            {
                return Ok(());
            }

            tracing::info!(
                frontend_host,
                ?expected_counts,
                ?mismatched_counts,
                "frontend scheduler has not applied rw_version filter yet"
            );
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| {
        anyhow!(
            "{frontend_host} did not schedule batch tasks only to same-version computes: \
             expected {expected_computes:?}, mismatched {mismatched_computes:?}"
        )
    })?
}

async fn wait_frontends_schedule_batch_query_to_same_version_computes(
    cluster: &Cluster,
    table_name: &str,
    frontend_compute_sets: &[(&str, &[&str], &[&str])],
) -> Result<()> {
    for (frontend_host, expected_computes, mismatched_computes) in frontend_compute_sets {
        wait_frontend_schedules_batch_query_to_same_version_compute(
            cluster,
            frontend_host,
            table_name,
            expected_computes,
            mismatched_computes,
        )
        .await?;
    }
    Ok(())
}

async fn create_test_table(cluster: &mut Cluster, table_name: &str) -> Result<u32> {
    cluster
        .run(format!("CREATE TABLE {table_name}(v int);"))
        .await?;
    cluster
        .run(format!(
            "INSERT INTO {table_name} SELECT * FROM generate_series(1, 1000);"
        ))
        .await?;
    cluster.run("FLUSH;").await?;
    cluster
        .run(format!(
            "SELECT id FROM rw_catalog.rw_tables WHERE name = '{table_name}';"
        ))
        .await?
        .trim()
        .parse()
        .with_context(|| format!("failed to parse table id for {table_name}"))
}

async fn wait_for_cluster_ready(cluster: &Cluster) -> Result<()> {
    wait_worker_versions_on_all_frontends(cluster, "5 5").await
}

async fn wait_for_expected_serving_mapping(
    cluster: &Cluster,
    table_id: u32,
    expected_worker_hosts: &[&str],
) -> Result<()> {
    let expected_worker_hosts = expected_worker_hosts
        .iter()
        .map(|host| (*host).to_owned())
        .collect::<HashSet<_>>();
    let expected_worker_count = expected_worker_hosts.len();
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            match serving_mapping_worker_hosts(cluster, table_id).await {
                Ok(worker_hosts) if worker_hosts == expected_worker_hosts => return Ok(()),
                Ok(worker_hosts) => {
                    tracing::info!(
                        ?worker_hosts,
                        ?expected_worker_hosts,
                        "serving mapping not ready"
                    );
                }
                Err(error) => {
                    tracing::info!(%error, "serving mapping not ready");
                }
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| {
        anyhow!(
            "timed out waiting for serving mapping of table {table_id} to cover \
             {expected_worker_count} expected workers"
        )
    })?
}

async fn serving_mapping_worker_hosts(cluster: &Cluster, table_id: u32) -> Result<HashSet<String>> {
    cluster
        .run_on_client(async move {
            let meta_addr = "http://meta-1:5690".parse::<MetaAddressStrategy>()?;
            let host_addr = "serving-start-order-test-meta-client:0".parse::<HostAddr>()?;
            let (meta_client, _) = MetaClient::register_new(
                meta_addr,
                WorkerType::RiseCtl,
                &host_addr,
                Property::default(),
                Arc::new(RwConfig::default().meta),
            )
            .await;

            let result = serving_mapping_worker_hosts_inner(&meta_client, table_id).await;
            meta_client.try_unregister().await;
            result
        })
        .await
}

async fn serving_mapping_worker_hosts_inner(
    meta_client: &MetaClient,
    table_id: u32,
) -> Result<HashSet<String>> {
    let table_id = TableId::new(table_id);
    let result_fragments = meta_client
        .list_fragment_distributions(false)
        .await?
        .into_iter()
        .filter(|fragment| fragment.state_table_ids.contains(&table_id))
        .collect_vec();
    let [result_fragment] = result_fragments.as_slice() else {
        return Err(anyhow!(
            "table {} must have one result fragment, got {}",
            table_id.as_raw_id(),
            result_fragments.len()
        ));
    };

    let worker_id_to_host = meta_client
        .list_worker_nodes(None)
        .await?
        .into_iter()
        .filter(|worker| worker.r#type() == WorkerType::ComputeNode)
        .filter_map(|worker| worker.host.map(|host| (worker.id, host.host)))
        .collect::<HashMap<_, _>>();
    let serving_mappings = meta_client.list_serving_vnode_mappings().await?;
    let (_, serving_mapping) = serving_mappings
        .get(&result_fragment.fragment_id)
        .context("serving vnode mapping is missing result fragment")?;

    let worker_ids = serving_mapping
        .iter_unique()
        .map(|slot| slot.worker_id())
        .collect::<HashSet<_>>();
    worker_ids
        .into_iter()
        .map(|worker_id| {
            worker_id_to_host
                .get(&worker_id)
                .cloned()
                .with_context(|| format!("serving worker {} has no host", worker_id.as_raw_id()))
        })
        .collect()
}

async fn assert_serving_cluster(cluster: &mut Cluster, table_name: &str) -> Result<()> {
    wait_for_cluster_ready(cluster).await?;
    let table_id = create_test_table(cluster, table_name).await?;
    wait_for_expected_serving_mapping(cluster, table_id, &COMPUTE_HOSTS).await?;
    wait_frontends_schedule_batch_query_to_same_version_computes(
        cluster,
        table_name,
        &[
            (FRONTEND_HOSTS[0], &COMPUTE_NODES, &[]),
            (FRONTEND_HOSTS[1], &COMPUTE_NODES, &[]),
        ],
    )
    .await
}

async fn wait_worker_hosts_registered(
    cluster: &Cluster,
    expected_hosts: &[&str],
) -> Result<Vec<WorkerNode>> {
    let expected_hosts = expected_hosts
        .iter()
        .map(|host| (*host).to_owned())
        .collect::<HashSet<_>>();
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            let workers = cluster
                .run_on_client(async {
                    let meta_addr = "http://meta-1:5690".parse::<MetaAddressStrategy>()?;
                    let host_addr = "serving-start-order-worker-wait:0".parse::<HostAddr>()?;
                    let (meta_client, _) = MetaClient::register_new(
                        meta_addr,
                        WorkerType::RiseCtl,
                        &host_addr,
                        Property::default(),
                        Arc::new(RwConfig::default().meta),
                    )
                    .await;
                    let workers = meta_client
                        .list_worker_nodes(None)
                        .await
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|worker| worker.r#type() == WorkerType::ComputeNode)
                        .collect_vec();
                    let _ = meta_client.try_unregister().await;
                    Ok::<_, anyhow::Error>(workers)
                })
                .await?;
            let registered_hosts = workers
                .iter()
                .filter_map(|worker| worker.host.as_ref().map(|host| host.host.clone()))
                .collect::<HashSet<_>>();
            if expected_hosts.is_subset(&registered_hosts) {
                return Ok(workers);
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| anyhow!("timed out waiting for compute workers {expected_hosts:?}"))?
}

#[tokio::test]
async fn test_serving_mapping_frontend_then_serving_join() -> Result<()> {
    let mut cluster = Cluster::start(serving_mapping_config()).await?;

    start_frontend_nodes(&cluster).await;
    start_compute_nodes(&cluster).await;

    assert_serving_cluster(&mut cluster, "serve_join_fe_first").await
}

#[tokio::test]
async fn test_serving_mapping_serving_then_frontend_join() -> Result<()> {
    let mut cluster = Cluster::start(serving_mapping_config()).await?;

    start_compute_nodes(&cluster).await;
    wait_worker_hosts_registered(&cluster, &COMPUTE_HOSTS).await?;
    start_frontend_nodes(&cluster).await;

    assert_serving_cluster(&mut cluster, "serve_join_compute_first").await
}

#[tokio::test]
async fn test_serving_mapping_after_meta_restart() -> Result<()> {
    let mut cluster = Cluster::start(serving_mapping_config()).await?;

    start_compute_nodes(&cluster).await;
    start_frontend_nodes(&cluster).await;
    assert_serving_cluster(&mut cluster, "serve_meta_restart").await?;

    cluster.simple_kill_nodes(["meta-1"]).await;
    sleep(Duration::from_secs(5)).await;
    cluster.simple_restart_nodes(["meta-1"]).await;

    wait_worker_versions_on_all_frontends(&cluster, "5 5").await?;
    let table_id = cluster
        .run("SELECT id FROM rw_catalog.rw_tables WHERE name = 'serve_meta_restart';")
        .await?
        .trim()
        .parse()
        .context("failed to parse serve_meta_restart table id")?;
    wait_for_expected_serving_mapping(&cluster, table_id, &COMPUTE_HOSTS).await?;
    wait_frontends_schedule_batch_query_to_same_version_computes(
        &cluster,
        "serve_meta_restart",
        &[
            (FRONTEND_HOSTS[0], &COMPUTE_NODES, &[]),
            (FRONTEND_HOSTS[1], &COMPUTE_NODES, &[]),
        ],
    )
    .await
}

#[tokio::test]
async fn test_serving_mapping_masks_worker_with_mismatched_version() -> Result<()> {
    let mut cluster = Cluster::start(serving_mapping_config()).await?;

    start_compute_nodes(&cluster).await;
    start_frontend_nodes(&cluster).await;
    wait_for_cluster_ready(&cluster).await?;
    let table_id = create_test_table(&mut cluster, "serve_version_mask").await?;
    wait_for_expected_serving_mapping(&cluster, table_id, &COMPUTE_HOSTS).await?;

    let _version_guard =
        SimulatedRwVersionGuard::enable(UPGRADED_VERSION, &["meta-1", "frontend-1", "compute-1"]);
    cluster
        .simple_kill_nodes(["meta-1", "frontend-1", "compute-1"])
        .await;
    sleep(Duration::from_secs(5)).await;
    cluster
        .simple_restart_nodes(["meta-1", "frontend-1", "compute-1"])
        .await;

    let version_sql = format!(
        "SELECT count(*), count(*) FILTER (WHERE rw_version = '{}'), \
         count(*) FILTER (WHERE rw_version = '{}') \
         FROM rw_catalog.rw_worker_nodes \
         WHERE type IN (\
             'WORKER_TYPE_META', \
             'WORKER_TYPE_FRONTEND', \
             'WORKER_TYPE_COMPUTE_NODE'\
         );",
        RW_VERSION, UPGRADED_VERSION
    );
    for host in FRONTEND_HOSTS {
        wait_frontend_result(&cluster, host, &version_sql, "5 2 3").await?;
    }

    wait_worker_hosts_registered(&cluster, &COMPUTE_HOSTS).await?;
    wait_for_expected_serving_mapping(&cluster, table_id, &COMPUTE_HOSTS).await?;
    wait_frontends_schedule_batch_query_to_same_version_computes(
        &cluster,
        "serve_version_mask",
        &[
            (FRONTEND_HOSTS[0], &["compute-1"], &["compute-2"]),
            (FRONTEND_HOSTS[1], &["compute-2"], &["compute-1"]),
        ],
    )
    .await
}
