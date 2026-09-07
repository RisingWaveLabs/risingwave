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

use std::time::Duration;

use anyhow::{Result, anyhow};
use risingwave_common::error::AsReport;
use risingwave_simulation::cluster::{Cluster, Configuration, Session};
use tokio::time::sleep;

use crate::utils::wait_jobs_running;

const MAX_HEARTBEAT_INTERVAL_SEC: u64 = 5;

async fn wait_until(session: &mut Session, sql: &str, target: &str) -> Result<()> {
    for _ in 0..60 {
        match session.run(sql).await {
            Ok(result) if result == target => return Ok(()),
            _ => sleep(Duration::from_secs(1)).await,
        }
    }
    Err(anyhow!(
        "timed out waiting for query `{}` to return `{}`",
        sql,
        target
    ))
}

async fn run_until_meta_ready(session: &mut Session, sql: &str) -> Result<()> {
    let mut last_error = None;
    for _ in 0..60 {
        match session.run(sql).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                let report = format!("{:#}", err.as_report());
                if !report.contains("service is currently unavailable")
                    && !report.contains("connection reset")
                    && !report.contains("transport error")
                {
                    return Err(err);
                }
                last_error = Some(report);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
    Err(anyhow!(
        "timed out waiting for query `{}` to succeed, last error: {}",
        sql,
        last_error.unwrap_or_else(|| "none".to_owned())
    ))
}

#[tokio::test]
async fn test_drop_streaming_job_after_meta_restart_without_compute_node() -> Result<()> {
    let mut config = Configuration::for_auto_parallelism(MAX_HEARTBEAT_INTERVAL_SEC, true);
    config.compute_nodes = 3;
    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session.run("create table t (v int);").await?;
    session
        .run("create sink s as select count(*) from t with (connector = 'blackhole');")
        .await?;
    session.run("wait;").await?;

    let compute_node_count_sql =
        "select count(*) from rw_catalog.rw_worker_nodes where type = 'WORKER_TYPE_COMPUTE_NODE';";
    wait_until(&mut session, compute_node_count_sql, "3").await?;

    cluster
        .simple_kill_nodes(["compute-1", "compute-2", "compute-3"])
        .await;
    sleep(Duration::from_secs(MAX_HEARTBEAT_INTERVAL_SEC * 2)).await;
    wait_until(&mut session, compute_node_count_sql, "0").await?;

    cluster.simple_kill_nodes(["meta-1"]).await;
    cluster.simple_restart_nodes(["meta-1"]).await;
    run_until_meta_ready(&mut session, "show jobs;").await?;

    run_until_meta_ready(&mut session, "drop sink s;").await?;
    wait_until(
        &mut session,
        "select count(*) from rw_catalog.rw_sinks where name = 's';",
        "0",
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_drop_creating_streaming_job_after_meta_restart_without_compute_node() -> Result<()> {
    let mut config = Configuration::for_auto_parallelism(MAX_HEARTBEAT_INTERVAL_SEC, true);
    config.compute_nodes = 3;
    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session.run("create table t (v int);").await?;
    session
        .run("insert into t select * from generate_series(1, 10000);")
        .await?;
    session.run("flush;").await?;

    let mut create_session = cluster.start_session();
    let create_handle = tokio::spawn(async move {
        create_session.run("set streaming_parallelism = 1;").await?;
        create_session.run("set backfill_rate_limit = 1;").await?;
        create_session.run("set background_ddl = true;").await?;
        create_session
            .run("create sink s as select * from t with (connector = 'blackhole');")
            .await
    });

    let running_jobs = wait_jobs_running(&mut session).await?;
    tracing::info!("running jobs before killing compute nodes: {running_jobs}");

    let compute_node_count_sql =
        "select count(*) from rw_catalog.rw_worker_nodes where type = 'WORKER_TYPE_COMPUTE_NODE';";
    wait_until(&mut session, compute_node_count_sql, "3").await?;

    cluster
        .simple_kill_nodes(["compute-1", "compute-2", "compute-3"])
        .await;
    sleep(Duration::from_secs(MAX_HEARTBEAT_INTERVAL_SEC * 2)).await;
    wait_until(&mut session, compute_node_count_sql, "0").await?;

    cluster.simple_kill_nodes(["meta-1"]).await;
    cluster.simple_restart_nodes(["meta-1"]).await;
    run_until_meta_ready(&mut session, "show jobs;").await?;

    session.run("drop sink s;").await?;
    wait_until(
        &mut session,
        "select count(*) from rw_catalog.rw_sinks where name = 's';",
        "0",
    )
    .await?;

    create_handle.abort();

    Ok(())
}
