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

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Result, anyhow};
use risingwave_simulation::cluster::{Cluster, KillOpts, Session};
use tokio::time::{Instant, sleep};

pub(crate) async fn kill_cn_and_wait_recover(cluster: &mut Cluster) {
    cluster
        .kill_nodes(["compute-1", "compute-2", "compute-3"], 0)
        .await;
    // Keep the sleep: probing too early can read a stale RUNNING before the kill lands.
    sleep(Duration::from_secs(10)).await;
    wait_all_database_recovered(cluster).await;
}

pub(crate) async fn kill_cn_and_meta_and_wait_recover(cluster: &mut Cluster) {
    cluster
        .kill_nodes(["compute-1", "compute-2", "compute-3", "meta-1"], 0)
        .await;
    sleep(Duration::from_secs(10)).await;
    wait_all_database_recovered(cluster).await;
}

pub(crate) async fn kill_random_and_wait_recover(cluster: &mut Cluster) {
    // Kill it again
    for _ in 0..3 {
        sleep(Duration::from_secs(2)).await;
        cluster.kill_node(&KillOpts::ALL_FAST).await;
    }
    sleep(Duration::from_secs(10)).await;
    cluster.wait_for_recovery().await.unwrap();
}

pub(crate) async fn kill_cn_meta_and_wait_full_recovery(cluster: &mut Cluster) {
    cluster
        .kill_nodes(["compute-1", "compute-2", "compute-3", "meta-1"], 0)
        .await;
    wait_all_database_recovered(cluster).await;
}

pub(crate) async fn member_table_ids(session: &mut Session) -> Result<HashSet<u32>> {
    let rows = session
        .run("select member_tables from rw_catalog.rw_hummock_compaction_group_configs;")
        .await?;
    let mut ids = HashSet::new();
    for line in rows.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let member_tables: Vec<u32> = serde_json::from_str(line)?;
        ids.extend(member_tables);
    }
    Ok(ids)
}

pub(crate) async fn wait_jobs_running(session: &mut Session) -> Result<String> {
    for _ in 0..60 {
        let jobs = session.run("show jobs;").await?;
        if !jobs.trim().is_empty() {
            return Ok(jobs);
        }
        sleep(Duration::from_millis(200)).await;
    }
    Err(anyhow!("jobs are still not running after waiting"))
}

pub(crate) async fn wait_for_jobs_cleared(session: &mut Session) -> Result<()> {
    for _ in 0..30 {
        if session.run("show jobs;").await?.trim().is_empty() {
            return Ok(());
        }
        sleep(Duration::from_secs(1)).await;
    }
    Err(anyhow!("failed to observe empty show jobs"))
}

pub(crate) async fn wait_member_table_ids(
    session: &mut Session,
    expected: &HashSet<u32>,
) -> Result<()> {
    for _ in 0..60 {
        let current = member_table_ids(session).await?;
        if &current == expected {
            return Ok(());
        }
        sleep(Duration::from_millis(200)).await;
    }
    Err(anyhow!(
        "member tables do not match expected set after waiting"
    ))
}

/// Whether every database in the cluster reports `RUNNING` in
/// `rw_catalog.rw_recovery_info`. Errors (e.g. meta still down) map to `false`.
async fn cluster_fully_running(cluster: &mut Cluster) -> bool {
    let mut session = cluster.start_session();
    let Ok(output) = session
        .run("SELECT recovery_state FROM rw_catalog.rw_recovery_info;")
        .await
    else {
        return false;
    };
    let states: Vec<_> = (output.lines())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    !states.is_empty() && states.iter().all(|s| *s == "RUNNING")
}

const WAIT_ALL_DB_TIMEOUT: Duration = Duration::from_secs(100);
const WAIT_ALL_DB_INTERVAL: Duration = Duration::from_millis(500);

pub(crate) async fn wait_all_database_recovered(cluster: &mut Cluster) {
    let start = Instant::now();
    while start.elapsed() < WAIT_ALL_DB_TIMEOUT {
        if cluster_fully_running(cluster).await {
            return;
        }
        sleep(WAIT_ALL_DB_INTERVAL).await;
    }
}
