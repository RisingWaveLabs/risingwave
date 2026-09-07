// Copyright 2025 RisingWave Labs
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
use risingwave_simulation::ctl_ext::predicate::{identity_contains, no_identity_contains};
use risingwave_simulation::utils::AssertResult;
use tokio::time::sleep;

#[tokio::test]
async fn test_background_ddl_alter_parallelism_without_arrangement_backfill() -> Result<()> {
    let config = Configuration::for_background_ddl();
    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session
        .run("set STREAMING_USE_ARRANGEMENT_BACKFILL = false;")
        .await?;
    session.run("set BACKGROUND_DDL = true;").await?;
    session.run("set BACKFILL_RATE_LIMIT = 1;").await?;

    session.run("create table t(v int);").await?;
    session
        .run("insert into t select * from generate_series(1, 30);")
        .await?;
    session
        .run("create materialized view m as select * from t;")
        .await?;

    // alter should fail when arrangement backfill is disabled
    let err = session
        .run("alter materialized view m set parallelism = 1;")
        .await;
    assert!(
        err.is_err(),
        "alter parallelism unexpectedly succeeded without arrangement backfill"
    );

    Ok(())
}

#[tokio::test]
async fn test_background_arrangement_backfill_offline_scaling() -> Result<()> {
    let config = Configuration::for_background_ddl();

    let cores_per_node = config.compute_node_cores;
    let node_count = config.compute_nodes;

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session
        .run("SET STREAMING_USE_ARRANGEMENT_BACKFILL = true;")
        .await?;
    session.run("create table t (v int);").await?;
    session
        .run("insert into t select * from generate_series(1, 1000);")
        .await?;

    session.run("SET BACKGROUND_DDL=true;").await?;
    session.run("SET BACKFILL_RATE_LIMIT=1;").await?;

    session
        .run("create materialized view m as select * from t;")
        .await?;

    let mat_fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("union"),
        ])
        .await?;

    assert_eq!(mat_fragment.inner.actors.len(), cores_per_node * node_count);

    sleep(Duration::from_secs(10)).await;

    cluster.simple_kill_nodes(["compute-2", "compute-3"]).await;

    sleep(Duration::from_secs(100)).await;

    let mat_fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("union"),
        ])
        .await?;

    assert_eq!(mat_fragment.inner.actors.len(), cores_per_node);

    sleep(Duration::from_secs(2000)).await;

    // job is finished
    session.run("show jobs;").await?.assert_result_eq("");

    Ok(())
}

#[tokio::test]
async fn test_background_ddl_scale_during_backfill() -> Result<()> {
    let mut config = Configuration::for_background_ddl();
    let mut per_session_queries = (*config.per_session_queries).clone();
    // TODO: remove it after support online scale of snapshot backfill
    per_session_queries.push("SET streaming_use_snapshot_backfill = false".into());
    config.per_session_queries = per_session_queries.into();
    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session.run("create table t(v int);").await?;
    session
        .run("insert into t select * from generate_series(1, 30);")
        .await?;
    session.run("set BACKFILL_RATE_LIMIT = 1;").await?;
    session.run("set BACKGROUND_DDL = true;").await?;

    session
        .run("create materialized view m as select * from t;")
        .await?;
    wait_for_materialized_view(&mut session, "m").await?;

    for parallelism in [1, 2, 3] {
        session
            .run(format!(
                "alter materialized view m set parallelism = {parallelism};"
            ))
            .await?;
    }

    // Wait for the background job to finish after scaling.
    let mut last_jobs = String::new();
    let mut finished = false;
    for _ in 0..60 {
        last_jobs = session.run("show jobs;").await?;
        if last_jobs.trim().is_empty() {
            finished = true;
            break;
        }
        sleep(Duration::from_secs(2)).await;
    }
    assert!(
        finished,
        "background ddl job not finished in time, remaining jobs: {last_jobs}"
    );

    session
        .run("select count(*) from m;")
        .await?
        .assert_result_eq("30");

    session
        .run("insert into t select * from generate_series(31, 60);")
        .await?;

    session.run("flush").await?;

    session
        .run("select count(*) from m;")
        .await?
        .assert_result_eq("60");

    Ok(())
}

async fn wait_for_materialized_view(session: &mut Session, name: &str) -> Result<()> {
    let sql = format!("show create materialized view {name};");
    let mut last_error = String::new();
    for _ in 0..60 {
        match session.run(sql.clone()).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                last_error = err.as_report().to_string();
                sleep(Duration::from_millis(200)).await;
            }
        }
    }
    Err(anyhow!(
        "timed out waiting for materialized view `{name}` to become visible, last error: {last_error}"
    ))
}

#[tokio::test]
async fn test_background_ddl_scale_out_after_cn_restart() -> Result<()> {
    let mut config = Configuration::for_background_ddl();
    // ensure we have at least 3 CNs to see scale-out effect
    config.compute_nodes = config.compute_nodes.max(3);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    // Kill one CN before creating the MV to force initial lower parallelism.
    cluster.simple_kill_nodes(["compute-2"]).await;

    sleep(Duration::from_secs(100)).await;

    session.run("create table t(v int);").await?;
    session
        .run("insert into t select * from generate_series(1, 30);")
        .await?;
    session.run("set BACKFILL_RATE_LIMIT = 1;").await?;
    session.run("set BACKGROUND_DDL = true;").await?;

    session
        .run("create materialized view m as select * from t;")
        .await?;

    // Record initial actor count with reduced CNs.
    let initial_fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("union"),
        ])
        .await?;
    let initial_actors = initial_fragment.inner.actors.len();

    // Restart the killed CN to trigger auto scale-out.
    cluster.simple_restart_nodes(["compute-2"]).await;

    // Wait for auto-parallelism control to take effect.
    let mut scaled = false;
    for _ in 0..60 {
        let frag = cluster
            .locate_one_fragment([
                identity_contains("materialize"),
                no_identity_contains("union"),
            ])
            .await?;
        if frag.inner.actors.len() > initial_actors {
            scaled = true;
            break;
        }
        sleep(Duration::from_secs(2)).await;
    }
    assert!(scaled, "fragment did not scale out from {initial_actors}");

    // Wait for backfill to finish.
    let mut last_jobs = String::new();
    let mut finished = false;
    for _ in 0..60 {
        last_jobs = session.run("show jobs;").await?;
        if last_jobs.trim().is_empty() {
            finished = true;
            break;
        }
        sleep(Duration::from_secs(2)).await;
    }
    assert!(
        finished,
        "background ddl job not finished in time, remaining jobs: {last_jobs}"
    );

    // Validate data before and after more inserts.
    session
        .run("select count(*) from m;")
        .await?
        .assert_result_eq("30");

    session
        .run("insert into t select * from generate_series(31, 60);")
        .await?;

    session.run("flush").await?;

    session
        .run("select count(*) from m;")
        .await?
        .assert_result_eq("60");

    Ok(())
}
