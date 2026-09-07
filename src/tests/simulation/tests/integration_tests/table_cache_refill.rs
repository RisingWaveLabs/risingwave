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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use risingwave_common::config::RwConfig;
use risingwave_common::id::{TableId, WorkerId};
use risingwave_common::monitor::EndpointExt;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::meta_addr::MetaAddressStrategy;
use risingwave_common::util::tokio_util::sync::CancellationToken;
use risingwave_pb::common::WorkerType;
use risingwave_pb::common::worker_node::Property;
use risingwave_pb::monitor_service::GetTableCacheRefillStatsRequest;
use risingwave_pb::monitor_service::monitor_service_client::MonitorServiceClient;
use risingwave_rpc_client::MetaClient;
use risingwave_simulation::cluster::{Cluster, Configuration, Session};
use serde::Deserialize;
use tokio::time::sleep;
use tonic::transport::Endpoint;

const WAIT_TIMEOUT: Duration = Duration::from_secs(100);
const POLL_INTERVAL: Duration = Duration::from_secs(1);
const EXISTING_WORKER_HOST: &str = "192.168.3.2";
const NEW_SERVING_WORKER_HOST: &str = "192.168.3.3";

#[derive(Debug)]
struct VnodeOwnership {
    streaming: Vec<u16>,
    serving: Vec<u16>,
    vnode_count: usize,
}

#[derive(Debug)]
struct ObservedRefillState {
    policy: Option<String>,
    streaming: Vec<u16>,
    serving: Vec<u16>,
}

#[derive(Debug, Deserialize)]
struct TableCacheRefillStats {
    policies: HashMap<u32, String>,
    internal: InternalRefillStats,
}

#[derive(Debug, Deserialize)]
struct InternalRefillStats {
    streaming: HashMap<u32, Vec<Vec<u16>>>,
    serving: HashMap<u32, Vec<u16>>,
}

impl TableCacheRefillStats {
    fn state_for(&self, table_id: u32) -> ObservedRefillState {
        let mut streaming = self
            .internal
            .streaming
            .get(&table_id)
            .into_iter()
            .flatten()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        streaming.sort_unstable();
        streaming.dedup();

        let mut serving = self
            .internal
            .serving
            .get(&table_id)
            .cloned()
            .unwrap_or_default();
        serving.sort_unstable();
        serving.dedup();

        ObservedRefillState {
            policy: self.policies.get(&table_id).cloned(),
            streaming,
            serving,
        }
    }
}

#[tokio::test]
async fn refill_state_rebuilt_after_database_recovery() -> Result<()> {
    let mut config = Configuration::for_auto_parallelism(10, true);
    config.compute_nodes = 2;
    config.compute_node_cores = 1;

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();
    let result_table_id = create_stateful_both_mv(&mut session).await?;
    let surviving_worker = wait_for_compute_worker(&cluster, EXISTING_WORKER_HOST).await?;

    let before = wait_for_meta_ownership(
        &cluster,
        surviving_worker.id,
        result_table_id,
        |ownership| {
            ownership.vnode_count > 1
                && !ownership.streaming.is_empty()
                && ownership.streaming.len() < ownership.vnode_count
                && !ownership.serving.is_empty()
                && ownership.serving.len() < ownership.vnode_count
        },
    )
    .await?;
    wait_for_refill_state(
        &cluster,
        &surviving_worker,
        result_table_id,
        Some("both"),
        Some(&before.streaming),
        Some(&before.serving),
    )
    .await?;

    cluster.simple_kill_nodes(["compute-1"]).await;
    wait_until_run_ok(&mut session, "insert into refill_source values (1);").await?;

    let after = wait_for_meta_ownership(
        &cluster,
        surviving_worker.id,
        result_table_id,
        |ownership| {
            ownership.streaming.len() == ownership.vnode_count
                && ownership.serving.len() == ownership.vnode_count
        },
    )
    .await?;
    assert_ne!(before.streaming, after.streaming);
    assert_ne!(before.serving, after.serving);

    wait_for_refill_state(
        &cluster,
        &surviving_worker,
        result_table_id,
        Some("both"),
        Some(&after.streaming),
        Some(&after.serving),
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn serving_refill_mapping_updated_on_worker_activation() -> Result<()> {
    let mut config = Configuration::for_auto_parallelism(10, true);
    config.compute_nodes = 2;
    config.compute_node_cores = 1;
    config.compute_node_roles =
        HashMap::from([(1, "streaming".to_owned()), (2, "serving".to_owned())]);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();
    let result_table_id = create_stateful_both_mv(&mut session).await?;

    // Activate the persisted policy before changing topology, so worker activation is the only
    // cause of the state transition observed below.
    session.run("recover;").await?;

    let existing_worker = wait_for_compute_worker(&cluster, EXISTING_WORKER_HOST).await?;
    let before =
        wait_for_meta_ownership(&cluster, existing_worker.id, result_table_id, |ownership| {
            ownership.serving.len() == ownership.vnode_count
        })
        .await?;
    wait_for_refill_state(
        &cluster,
        &existing_worker,
        result_table_id,
        Some("both"),
        None,
        Some(&before.serving),
    )
    .await?;

    create_compute_node(&cluster, 3, "serving");
    let new_worker = wait_for_compute_worker(&cluster, NEW_SERVING_WORKER_HOST).await?;

    let existing_after =
        wait_for_meta_ownership(&cluster, existing_worker.id, result_table_id, |ownership| {
            !ownership.serving.is_empty() && ownership.serving.len() < ownership.vnode_count
        })
        .await?;
    wait_for_refill_state(
        &cluster,
        &existing_worker,
        result_table_id,
        Some("both"),
        None,
        Some(&existing_after.serving),
    )
    .await?;

    let new_worker_ownership =
        wait_for_meta_ownership(&cluster, new_worker.id, result_table_id, |ownership| {
            !ownership.serving.is_empty()
        })
        .await?;
    wait_for_refill_state(
        &cluster,
        &new_worker,
        result_table_id,
        Some("both"),
        None,
        Some(&new_worker_ownership.serving),
    )
    .await?;

    let mut combined = existing_after.serving.clone();
    combined.extend_from_slice(&new_worker_ownership.serving);
    let combined_len = combined.len();
    combined.sort_unstable();
    combined.dedup();
    assert_eq!(combined.len(), existing_after.vnode_count);
    assert_eq!(combined.len(), combined_len, "serving ownership overlaps");

    Ok(())
}

async fn create_stateful_both_mv(session: &mut Session) -> Result<u32> {
    session.run("create table refill_source(v int);").await?;
    session
        .run(
            "create materialized view refill_mv as \
             select v, count(*) as count from refill_source group by v;",
        )
        .await?;
    session
        .run(
            "alter materialized view refill_mv set config(\
             streaming.developer.cache_refill_policy = 'both');",
        )
        .await?;

    session
        .run("select id from rw_catalog.rw_materialized_views where name = 'refill_mv';")
        .await?
        .trim()
        .parse()
        .context("failed to parse refill_mv table id")
}

struct ComputeWorker {
    id: WorkerId,
    endpoint: String,
}

async fn wait_for_compute_worker(cluster: &Cluster, worker_host: &str) -> Result<ComputeWorker> {
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            if let Some(worker) = cluster
                .get_cluster_info()
                .await?
                .worker_nodes
                .into_iter()
                .find(|worker| {
                    worker.r#type() == WorkerType::ComputeNode
                        && worker
                            .host
                            .as_ref()
                            .is_some_and(|host| host.host == worker_host)
                })
            {
                let host = worker.host.context("compute worker has no host")?;
                return Ok(ComputeWorker {
                    id: worker.id,
                    endpoint: format!("http://{}:{}", host.host, host.port),
                });
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| anyhow!("timed out waiting for compute worker {worker_host}"))?
}

async fn wait_until_run_ok(session: &mut Session, sql: &str) -> Result<()> {
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            if session.run(sql).await.is_ok() {
                return;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| anyhow!("timed out waiting for query to succeed: {sql}"))
}

async fn wait_for_meta_ownership(
    cluster: &Cluster,
    worker_id: WorkerId,
    result_table_id: u32,
    mut predicate: impl FnMut(&VnodeOwnership) -> bool + Send + 'static,
) -> Result<VnodeOwnership> {
    cluster
        .run_on_client(async move {
            let meta_addr = "http://meta-1:5690".parse::<MetaAddressStrategy>()?;
            let host_addr = "table-refill-test-meta-client:0".parse::<HostAddr>()?;
            let (meta_client, _) = MetaClient::register_new(
                meta_addr,
                WorkerType::RiseCtl,
                &host_addr,
                Property::default(),
                Arc::new(RwConfig::default().meta),
            )
            .await;
            let mut last_observed = None;
            let result = tokio::time::timeout(WAIT_TIMEOUT, async {
                loop {
                    match meta_ownership_inner(&meta_client, worker_id, result_table_id).await {
                        Ok(ownership) if predicate(&ownership) => return ownership,
                        Ok(ownership) => last_observed = Some(format!("{ownership:?}")),
                        Err(error) => last_observed = Some(error.to_string()),
                    }
                    sleep(POLL_INTERVAL).await;
                }
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "timed out waiting for Meta ownership of table {result_table_id} on worker {}; \
                     last observed: {last_observed:?}",
                    worker_id.as_raw_id()
                )
            });
            meta_client.try_unregister().await;
            result
        })
        .await
}

async fn meta_ownership_inner(
    meta_client: &MetaClient,
    worker_id: WorkerId,
    result_table_id: u32,
) -> Result<VnodeOwnership> {
    let result_table_id = TableId::new(result_table_id);
    let result_fragments = meta_client
        .list_fragment_distributions(false)
        .await?
        .into_iter()
        .filter(|fragment| fragment.state_table_ids.contains(&result_table_id))
        .collect::<Vec<_>>();
    let [result_fragment] = result_fragments.as_slice() else {
        return Err(anyhow!(
            "result table {} must have one owning fragment, got {}",
            result_table_id.as_raw_id(),
            result_fragments.len()
        ));
    };

    let mut streaming = Vec::new();
    for actor in meta_client
        .list_actor_states()
        .await?
        .into_iter()
        .filter(|actor| {
            actor.fragment_id == result_fragment.fragment_id && actor.worker_id == worker_id
        })
    {
        streaming.extend(meta_client.get_actor_vnodes(actor.actor_id).await?);
    }
    let mut streaming = streaming
        .into_iter()
        .map(u16::try_from)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    streaming.sort_unstable();
    streaming.dedup();

    let serving_mappings = meta_client.list_serving_vnode_mappings().await?;
    let (job_id, serving_mapping) = serving_mappings
        .get(&result_fragment.fragment_id)
        .context("serving vnode mapping is missing result fragment")?;
    if job_id.as_raw_id() != result_table_id.as_raw_id() {
        return Err(anyhow!(
            "serving vnode mapping belongs to job {}, expected {}",
            job_id.as_raw_id(),
            result_table_id.as_raw_id()
        ));
    }
    let mut serving = Vec::new();
    for (worker_slot_id, bitmap) in serving_mapping.to_bitmaps() {
        if worker_slot_id.worker_id() == worker_id {
            serving.extend(bitmap.iter_ones().map(|vnode| vnode as u16));
        }
    }
    serving.sort_unstable();
    serving.dedup();

    Ok(VnodeOwnership {
        streaming,
        serving,
        vnode_count: result_fragment.vnode_count as usize,
    })
}

async fn wait_for_refill_state(
    cluster: &Cluster,
    worker: &ComputeWorker,
    table_id: u32,
    expected_policy: Option<&str>,
    expected_streaming: Option<&[u16]>,
    expected_serving: Option<&[u16]>,
) -> Result<ObservedRefillState> {
    let endpoint = worker.endpoint.clone();
    let expected_policy = expected_policy.map(str::to_owned);
    let expected_streaming = expected_streaming.map(<[u16]>::to_vec);
    let expected_serving = expected_serving.map(<[u16]>::to_vec);

    cluster
        .run_on_client(async move {
            let channel = Endpoint::from_shared(endpoint)?
                .connect_timeout(Duration::from_secs(5))
                .monitored_connect("grpc-table-cache-refill-stats-client", Default::default())
                .await?;
            let mut client = MonitorServiceClient::new(channel);
            let mut last_observed = None;

            tokio::time::timeout(WAIT_TIMEOUT, async {
                loop {
                    if let Ok(response) = client
                        .get_table_cache_refill_stats(GetTableCacheRefillStatsRequest {})
                        .await
                        && let Ok(stats) = serde_json::from_str::<TableCacheRefillStats>(
                            &response.into_inner().stats,
                        )
                    {
                        let observed = stats.state_for(table_id);
                        let matches = observed.policy == expected_policy
                            && expected_streaming
                                .as_ref()
                                .is_none_or(|vnodes| &observed.streaming == vnodes)
                            && expected_serving
                                .as_ref()
                                .is_none_or(|vnodes| &observed.serving == vnodes);
                        if matches {
                            return Ok(observed);
                        }
                        last_observed = Some(observed);
                    }
                    sleep(POLL_INTERVAL).await;
                }
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "timed out waiting for refill state of table {table_id}; \
                     last observed: {last_observed:?}"
                )
            })?
        })
        .await
}

fn create_compute_node(cluster: &Cluster, index: usize, role: &str) {
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
        "--role",
        role,
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
