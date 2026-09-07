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

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use risingwave_common::util::worker_util::DEFAULT_RESOURCE_GROUP;
use risingwave_simulation::cluster::{Cluster, Configuration, Session};
use tokio::time::sleep;

use crate::utils::wait_all_database_recovered;

const WAIT_RETRY_INTERVAL: Duration = Duration::from_millis(200);
const TABLE_CHANGE_LOG_TRUNCATE_INTERVAL_SEC: u64 = 10;

async fn wait_for_query_result(
    session: &mut Session,
    sql: &str,
    expected: &str,
    retry: usize,
) -> Result<()> {
    for _ in 0..retry {
        let result = session.run(sql).await?;
        if result.trim() == expected {
            return Ok(());
        }
        sleep(WAIT_RETRY_INTERVAL).await;
    }

    Err(anyhow!(
        "query did not return expected result after {retry} retries: sql={sql}, expected={expected}"
    ))
}

async fn recovery_event_count(session: &mut Session) -> Result<u64> {
    let result = session
        .run("select count(*) from rw_catalog.rw_event_logs where event_type like '%RECOVERY%';")
        .await?;
    Ok(result.trim().parse()?)
}

async fn max_table_change_log_epoch(session: &mut Session) -> Result<u64> {
    let result = session
        .run(
            r#"
WITH epochs AS (
    SELECT
        (jsonb_array_elements_text((jsonb_array_elements(change_log->'changeLogs'))->'epochs'))::bigint AS epoch
    FROM rw_catalog.rw_hummock_table_change_log
    WHERE table_id = (SELECT id FROM rw_catalog.rw_tables WHERE name = 't')
)
SELECT max(epoch) FROM epochs;
"#,
        )
        .await?;
    result
        .trim()
        .parse()
        .context("failed to get the latest change-log epoch for table t")
}

fn init_logger() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .try_init();
}

#[tokio::test]
async fn test_cross_db_retention_miss_after_restart_does_not_recover() -> Result<()> {
    init_logger();

    let mut config = Configuration::for_background_ddl();
    let mut config_file = OpenOptions::new()
        .append(true)
        .open(config.config_path.as_str())?;
    writeln!(
        config_file,
        "\n[meta.developer]\ntable_change_log_truncate_interval_sec = {TABLE_CHANGE_LOG_TRUNCATE_INTERVAL_SEC}"
    )?;
    config.compute_resource_groups = HashMap::from([
        (1, "src_group".to_owned()),
        (2, "dst_group".to_owned()),
        (3, DEFAULT_RESOURCE_GROUP.to_owned()),
    ]);
    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session
        .run("create database src_db with resource_group = 'src_group';")
        .await?;
    session
        .run("create database dst_db with resource_group = 'dst_group';")
        .await?;

    session.run("use src_db;").await?;
    session
        .run("create table t(id int primary key, v int);")
        .await?;
    session
        .run("insert into t select i, i from generate_series(1, 50000) as g(i);")
        .await?;
    session.run("flush;").await?;
    session
        .run("create subscription sub from t with(retention = '1 second');")
        .await?;

    session.run("use dst_db;").await?;
    session.run("set background_ddl = true;").await?;
    session
        .run("create materialized view mv_retention as select * from src_db.public.t;")
        .await?;

    session.run("wait;").await?;
    let mv_count_after_snapshot = session.run("select count(*) from mv_retention;").await?;
    assert_eq!(
        mv_count_after_snapshot.trim(),
        "50000",
        "cross-db MV should finish its initial snapshot before testing changelog retention"
    );

    wait_for_query_result(
        &mut session,
        "select count(*) from rw_catalog.rw_worker_nodes where host = '192.168.3.2' and resource_group = 'dst_group';",
        "1",
        100,
    )
    .await?;

    let recovery_count_before_kill = recovery_event_count(&mut session).await?;
    let downstream_node = "compute-2";

    cluster.simple_kill_nodes([downstream_node]).await;

    session.run("use src_db;").await?;
    session
        .run("insert into t select i, i from generate_series(50001, 50100) as g(i);")
        .await?;
    session.run("flush;").await?;
    let first_offline_batch_max_epoch = max_table_change_log_epoch(&mut session).await?;
    sleep(Duration::from_secs(3)).await;
    session.run("insert into t values (50101, 50101);").await?;
    session.run("flush;").await?;

    // Wait through one async truncation interval, then confirm that the changelog prefix needed by
    // the offline actor is gone before restarting it.
    sleep(Duration::from_secs(TABLE_CHANGE_LOG_TRUNCATE_INTERVAL_SEC)).await;
    wait_for_query_result(
        &mut session,
        &format!(
            r#"
WITH epochs AS (
    SELECT
        (jsonb_array_elements_text((jsonb_array_elements(change_log->'changeLogs'))->'epochs'))::bigint AS epoch
    FROM rw_catalog.rw_hummock_table_change_log
    WHERE table_id = (SELECT id FROM rw_catalog.rw_tables WHERE name = 't')
)
SELECT count(*) FROM epochs WHERE epoch <= {first_offline_batch_max_epoch};
"#
        ),
        "0",
        100,
    )
    .await?;

    cluster.simple_restart_nodes([downstream_node]).await;
    wait_all_database_recovered(&mut cluster).await;
    drop(session);
    let mut session = cluster.start_session();

    let recovery_count_after_restart = recovery_event_count(&mut session).await?;
    assert!(
        recovery_count_after_restart >= recovery_count_before_kill,
        "recovery count should not decrease: before={recovery_count_before_kill}, after_restart={recovery_count_after_restart}"
    );

    sleep(Duration::from_secs(20)).await;

    let recovery_count_after_retention_miss = recovery_event_count(&mut session).await?;
    assert_eq!(
        recovery_count_after_retention_miss, recovery_count_after_restart,
        "cross-db retention miss should not trigger another recovery after compute restart"
    );

    session.run("use dst_db;").await?;
    let mv_count_after_retention_miss = session.run("select count(*) from mv_retention;").await?;
    assert_eq!(
        mv_count_after_retention_miss.trim(),
        mv_count_after_snapshot.trim(),
        "cross-db MV should stop producing further changes after upstream changelog retention is exceeded"
    );

    Ok(())
}
