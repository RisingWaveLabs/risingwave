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

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use risingwave_common::config::RwConfig;
use risingwave_common::error::AsReport;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::meta_addr::MetaAddressStrategy;
use risingwave_pb::common::WorkerType;
use risingwave_pb::common::worker_node::Property;
use risingwave_rpc_client::{HummockMetaClient, MetaClient};
use risingwave_simulation::cluster::{Cluster, Configuration, Session};
use tokio::time::sleep;

use crate::utils::{
    kill_cn_and_meta_and_wait_recover, kill_cn_and_wait_recover,
    kill_cn_meta_and_wait_full_recovery, kill_random_and_wait_recover, member_table_ids,
    wait_all_database_recovered, wait_for_jobs_cleared, wait_jobs_running, wait_member_table_ids,
};

const CREATE_TABLE: &str = "CREATE TABLE t(v1 int);";
const DROP_TABLE: &str = "DROP TABLE t;";
const SEED_TABLE_500: &str = "INSERT INTO t SELECT generate_series FROM generate_series(1, 500);";
const SEED_TABLE_100: &str = "INSERT INTO t SELECT generate_series FROM generate_series(1, 100);";
const SET_BACKGROUND_DDL: &str = "SET BACKGROUND_DDL=true;";
const RESET_BACKGROUND_DDL: &str = "SET BACKGROUND_DDL=false;";
const SET_RATE_LIMIT_0: &str = "SET BACKFILL_RATE_LIMIT=0;";
const SET_RATE_LIMIT_2: &str = "SET BACKFILL_RATE_LIMIT=2;";
const SET_RATE_LIMIT_1: &str = "SET BACKFILL_RATE_LIMIT=1;";
const RESET_RATE_LIMIT: &str = "SET BACKFILL_RATE_LIMIT=DEFAULT;";
const CREATE_MV1: &str = "CREATE MATERIALIZED VIEW mv1 as SELECT * FROM t;";
const DROP_MV1: &str = "DROP MATERIALIZED VIEW mv1;";
const WAIT: &str = "WAIT;";
const MAX_HEARTBEAT_INTERVAL_SEC: u64 = 10;

async fn cancel_stream_jobs(session: &mut Session) -> Result<Vec<u32>> {
    tracing::info!("finding streaming jobs to cancel");
    let ids = session
        .run("select ddl_id from rw_catalog.rw_ddl_progress;")
        .await?;
    tracing::info!("selected streaming jobs to cancel {:?}", ids);
    tracing::info!("cancelling streaming jobs");
    let ids = ids.split('\n').collect::<Vec<_>>().join(",");
    let result = session.run(&format!("cancel jobs {};", ids)).await?;
    tracing::info!("cancelled streaming jobs, {}", result);
    let ids = result
        .split('\n')
        .map(|s| {
            s.parse::<u32>()
                .map_err(|_e| anyhow!("failed to parse {}", s))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ids)
}

async fn database_id_mapping(session: &mut Session) -> Result<HashMap<String, u32>> {
    let rows = session.run("select name, id from rw_databases").await?;
    Ok(rows
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let (name, id) = line.rsplit_once(' ').unwrap();
            (name.to_owned(), u32::from_str(id.trim()).unwrap())
        })
        .collect())
}

async fn wait_until(session: &mut Session, sql: &str, target: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(100), async {
        loop {
            if session.run(sql).await.unwrap() == target {
                return;
            }
            sleep(Duration::from_secs(1)).await;
        }
    })
    .await?;
    Ok(())
}

async fn wait_for_database_recovery_state(
    session: &mut Session,
    database_id: u32,
    target: &str,
) -> Result<()> {
    wait_until(
        session,
        &format!(
            "select recovery_state from rw_catalog.rw_recovery_info where database_id = {};",
            database_id
        ),
        target,
    )
    .await
}

async fn wait_for_stream_job_and_member_tables(
    session: &mut Session,
    baseline_member_tables: &HashSet<u32>,
) -> Result<HashSet<u32>> {
    for _ in 0..60 {
        let jobs = session.run("show jobs;").await?;
        if !jobs.trim().is_empty() {
            let current_member_tables = member_table_ids(session).await?;
            let created_member_tables = current_member_tables
                .difference(baseline_member_tables)
                .copied()
                .collect::<HashSet<_>>();
            if !created_member_tables.is_empty() {
                return Ok(created_member_tables);
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
    Err(anyhow!(
        "failed to observe a creating stream job with new member tables"
    ))
}

async fn sstable_ids_by_table_id(session: &mut Session, table_id: u32) -> Result<Vec<u64>> {
    let rows = session
        .run(format!(
            "SELECT sstable_id FROM rw_hummock_sstables WHERE table_ids @> '[{table_id}]'::jsonb ORDER BY sstable_id;"
        ))
        .await?;
    rows.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| Ok(u64::from_str(line.trim())?))
        .collect()
}

async fn wait_for_sstable_ids_by_table_id(
    session: &mut Session,
    table_id: u32,
) -> Result<Vec<u64>> {
    for _ in 0..60 {
        let sstable_ids = sstable_ids_by_table_id(session, table_id).await?;
        if !sstable_ids.is_empty() {
            return Ok(sstable_ids);
        }
        sleep(Duration::from_secs(1)).await;
    }
    Err(anyhow!("failed to observe sstables for table {}", table_id))
}

async fn assert_hummock_state_tables_can_be_fetched_from_catalog(cluster: &Cluster) {
    let (mut hummock_table_ids, mut catalog_table_ids, mut missing_table_ids) = cluster
        .run_on_client(async move {
            let meta_addr = "http://meta-1:5690".parse::<MetaAddressStrategy>().unwrap();
            let host_addr = "repro-meta-client:0".parse::<HostAddr>().unwrap();
            let meta_config = Arc::new(RwConfig::default().meta);
            let (meta_client, _) = MetaClient::register_new(
                meta_addr,
                WorkerType::RiseCtl,
                &host_addr,
                Property::default(),
                meta_config,
            )
            .await;

            let hummock_version = meta_client.get_current_version().await.unwrap();
            let hummock_table_ids = hummock_version
                .state_table_info
                .info()
                .keys()
                .copied()
                .collect::<Vec<_>>();
            let catalog_table_ids = meta_client
                .get_tables(hummock_table_ids.clone(), true)
                .await
                .unwrap()
                .into_keys()
                .map(|table_id| table_id.as_raw_id())
                .collect::<HashSet<_>>();
            let hummock_table_ids = hummock_table_ids
                .into_iter()
                .map(|table_id| table_id.as_raw_id())
                .collect::<HashSet<_>>();
            let missing_table_ids = hummock_table_ids
                .difference(&catalog_table_ids)
                .copied()
                .collect::<HashSet<_>>();

            meta_client.try_unregister().await;
            (
                hummock_table_ids.into_iter().collect::<Vec<_>>(),
                catalog_table_ids.into_iter().collect::<Vec<_>>(),
                missing_table_ids.into_iter().collect::<Vec<_>>(),
            )
        })
        .await;

    hummock_table_ids.sort_unstable();
    catalog_table_ids.sort_unstable();
    missing_table_ids.sort_unstable();
    tracing::info!(
        ?hummock_table_ids,
        ?catalog_table_ids,
        ?missing_table_ids,
        "checked hummock state table through meta client"
    );
    assert!(
        !hummock_table_ids.is_empty(),
        "hummock version state table info is empty"
    );
    assert!(
        missing_table_ids.is_empty(),
        "tables {:?} are in hummock version state table info but cannot be fetched from catalog RPC",
        missing_table_ids
    );
}

fn init_logger() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .try_init();
}

async fn create_mv(session: &mut Session) -> Result<()> {
    session.run(CREATE_MV1).await?;
    sleep(Duration::from_secs(2)).await;
    Ok(())
}

#[tokio::test]
async fn test_foreground_snapshot_backfill_recovery_keeps_hummock_table_catalog_fetchable() {
    init_logger();
    let mut config = Configuration::for_auto_parallelism(MAX_HEARTBEAT_INTERVAL_SEC, true);
    config.compute_nodes = 3;
    config.compute_node_cores = 2;
    config.compute_resource_groups = HashMap::from([
        (1, "group1".to_owned()),
        (2, "group2".to_owned()),
        (3, "group3".to_owned()),
    ]);
    let mut cluster = Cluster::start(config).await.unwrap();
    let mut session = cluster.start_session();

    session
        .run("create database group1 with resource_group='group1'")
        .await
        .unwrap();
    session
        .run("create database group2 with resource_group='group2'")
        .await
        .unwrap();
    session.run("set rw_implicit_flush = true;").await.unwrap();

    let database_id_mapping = database_id_mapping(&mut session).await.unwrap();
    let group1_database_id = database_id_mapping["group1"];

    session.run("use group1;").await.unwrap();
    session.run("set background_ddl = false;").await.unwrap();
    session
        .run("set streaming_use_snapshot_backfill = true;")
        .await
        .unwrap();
    session.run("set backfill_rate_limit = 1;").await.unwrap();
    session
        .run("create table t (k int primary key, v int);")
        .await
        .unwrap();
    session
        .run("insert into t select i, i from generate_series(1, 10000) as i;")
        .await
        .unwrap();
    session.run("flush;").await.unwrap();

    let baseline_member_tables = member_table_ids(&mut session).await.unwrap();

    let mut create_session = cluster.start_session();
    let create_handle = tokio::spawn(async move {
        create_session.run("use group1;").await.unwrap();
        create_session
            .run("set background_ddl = false;")
            .await
            .unwrap();
        create_session
            .run("set streaming_use_snapshot_backfill = true;")
            .await
            .unwrap();
        create_session
            .run("set backfill_rate_limit = 1;")
            .await
            .unwrap();
        create_session
            .run("create materialized view mv1 as select * from t;")
            .await
    });

    let created_member_tables =
        wait_for_stream_job_and_member_tables(&mut session, &baseline_member_tables)
            .await
            .unwrap();
    let candidate_table_id = *created_member_tables.iter().min().unwrap();
    wait_for_sstable_ids_by_table_id(&mut session, candidate_table_id)
        .await
        .unwrap();
    tracing::info!(
        ?created_member_tables,
        candidate_table_id,
        "observed creating foreground snapshot-backfill state tables"
    );

    let mut monitor_session = cluster.start_session();
    monitor_session.run("use group2;").await.unwrap();

    cluster.simple_kill_nodes(["compute-1"]).await;
    wait_until(
        &mut monitor_session,
        "select count(*) from rw_catalog.rw_worker_nodes where host = '192.168.3.1';",
        "0",
    )
    .await
    .unwrap();
    wait_for_database_recovery_state(&mut monitor_session, group1_database_id, "RECOVERING")
        .await
        .unwrap();

    let create_result = tokio::time::timeout(Duration::from_secs(30), create_handle)
        .await
        .unwrap()
        .unwrap();
    tracing::info!(
        ?create_result,
        "foreground ddl result after database recovery"
    );

    assert_hummock_state_tables_can_be_fetched_from_catalog(&cluster).await;
    cluster.simple_restart_nodes(["compute-1"]).await;
    wait_all_database_recovered(&mut cluster).await;
}

#[tokio::test]
async fn test_background_mv_barrier_recovery() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();

    session.run(CREATE_TABLE).await?;
    session
        .run("INSERT INTO t SELECT generate_series FROM generate_series(1, 200);")
        .await?;
    session.flush().await?;
    session.run(SET_RATE_LIMIT_2).await?;
    session.run(SET_BACKGROUND_DDL).await?;
    create_mv(&mut session).await?;

    // If the CN is killed before first barrier pass for the MV, the MV will be dropped.
    // This is because it's table fragments will NOT be committed until first barrier pass.
    kill_cn_and_wait_recover(&mut cluster).await;

    // Send some upstream updates.
    session
        .run("INSERT INTO t SELECT generate_series FROM generate_series(201, 400);")
        .await?;
    session.flush().await?;

    kill_random_and_wait_recover(&mut cluster).await;

    // Now just wait for it to complete.
    session.run(WAIT).await?;

    let t_count = session.run("SELECT COUNT(v1) FROM t").await?;
    let mv1_count = session.run("SELECT COUNT(v1) FROM mv1").await?;
    if t_count != mv1_count {
        let missing_rows = session
            .run("SELECT v1 FROM t WHERE v1 NOT IN (SELECT v1 FROM mv1)")
            .await?;
        tracing::debug!(missing_rows);
        let missing_rows_with_row_id = session
            .run("SELECT _row_id, v1 FROM t WHERE v1 NOT IN (SELECT v1 FROM mv1)")
            .await?;
        tracing::debug!(missing_rows_with_row_id);
    }
    assert_eq!(t_count, mv1_count);

    // Make sure that if MV killed and restarted
    // it will not be dropped.
    session.run(DROP_MV1).await?;
    session.run(DROP_TABLE).await?;

    Ok(())
}

#[tokio::test]
async fn test_background_join_mv_recovery() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();

    session.run("CREATE TABLE t1 (v1 int)").await?;
    session.run("CREATE TABLE t2 (v1 int)").await?;
    session
        .run("INSERT INTO t1 SELECT generate_series FROM generate_series(1, 200);")
        .await?;
    session
        .run("INSERT INTO t2 SELECT generate_series FROM generate_series(1, 200);")
        .await?;
    session.flush().await?;
    session.run(SET_RATE_LIMIT_2).await?;
    session.run(SET_BACKGROUND_DDL).await?;
    session
        .run("CREATE MATERIALIZED VIEW mv1 as select t1.v1 from t1 join t2 on t1.v1 = t2.v1;")
        .await?;
    sleep(Duration::from_secs(2)).await;

    kill_cn_and_meta_and_wait_recover(&mut cluster).await;

    // Now just wait for it to complete.
    session.run(WAIT).await?;

    let t_count = session.run("SELECT COUNT(v1) FROM t1").await?;
    let mv1_count = session.run("SELECT COUNT(v1) FROM mv1").await?;
    assert_eq!(t_count, mv1_count);

    // Make sure that if MV killed and restarted
    // it will not be dropped.
    session.run("DROP MATERIALIZED VIEW mv1;").await?;
    session.run("DROP TABLE t1;").await?;
    session.run("DROP TABLE t2;").await?;

    Ok(())
}

#[tokio::test]
async fn test_background_temporal_join_recovery() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();

    session
        .run("CREATE TABLE stream (id int primary key, value int);")
        .await?;
    session
        .run("CREATE TABLE version (id int primary key, value int);")
        .await?;
    session
        .run("INSERT INTO stream SELECT i, i FROM generate_series(1, 20) AS i;")
        .await?;
    session
        .run("INSERT INTO version SELECT i, i FROM generate_series(1, 20) AS i;")
        .await?;
    session.flush().await?;

    session.run("SET streaming_parallelism = 1;").await?;
    session.run(SET_RATE_LIMIT_1).await?;
    session.run(SET_BACKGROUND_DDL).await?;
    session
        .run(
            "CREATE MATERIALIZED VIEW mv AS \
             SELECT stream.id, version.value \
             FROM stream JOIN version FOR SYSTEM_TIME AS OF PROCTIME() \
             ON stream.id = version.id;",
        )
        .await?;

    wait_jobs_running(&mut session).await?;
    sleep(Duration::from_secs(2)).await;
    kill_cn_meta_and_wait_full_recovery(&mut cluster).await;

    tokio::time::timeout(Duration::from_secs(60), session.run(WAIT)).await??;
    assert_eq!(session.run("SELECT count(*) FROM mv;").await?, "20");

    session.run("DROP MATERIALIZED VIEW mv;").await?;
    session.run("DROP TABLE stream;").await?;
    session.run("DROP TABLE version;").await?;

    Ok(())
}

/// Test cancel for background ddl, foreground ddl.
#[tokio::test]
async fn test_ddl_cancel() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();
    session.run(CREATE_TABLE).await?;
    session.run(SEED_TABLE_500).await?;
    session.flush().await?;
    session.run(SET_RATE_LIMIT_1).await?;
    session.run(SET_BACKGROUND_DDL).await?;

    for _ in 0..5 {
        create_mv(&mut session).await?;
        let ids = cancel_stream_jobs(&mut session).await?;
        assert_eq!(ids.len(), 1);
    }

    session.run(SET_RATE_LIMIT_1).await?;
    create_mv(&mut session).await?;

    // Test cancel after kill cn
    kill_cn_and_wait_recover(&mut cluster).await;
    let ids = cancel_stream_jobs(&mut session).await?;
    assert_eq!(ids.len(), 1);
    tracing::info!("tested cancel background_ddl after recovery");

    sleep(Duration::from_secs(2)).await;

    create_mv(&mut session).await?;

    // Test cancel after kill random nodes
    kill_random_and_wait_recover(&mut cluster).await;
    let ids = cancel_stream_jobs(&mut session).await?;
    assert_eq!(ids.len(), 1);
    tracing::info!("tested cancel background_ddl after recovery from random node kill");

    // Test cancel by sigkill (only works for foreground mv)
    let mut session2 = cluster.start_session();
    tokio::spawn(async move {
        session2.run(RESET_BACKGROUND_DDL).await.unwrap();
        session2.run(SET_RATE_LIMIT_1).await.unwrap();
        let _ = create_mv(&mut session2).await;
    });

    // Keep searching for the process in process list
    loop {
        let processlist = session.run("SHOW PROCESSLIST;").await?;
        if let Some(line) = processlist
            .lines()
            .find(|line| line.to_lowercase().contains("mv1"))
        {
            tracing::info!("found mv1 process: {}", line);
            let mut splits = line.split_whitespace();
            let _worker_id = splits.next().unwrap();
            let pid = splits.next().unwrap();
            session.run(format!("kill '{}';", pid)).await?;
            sleep(Duration::from_secs(10)).await;
            break;
        }
        sleep(Duration::from_secs(2)).await;
    }

    session.run(RESET_RATE_LIMIT).await?;
    session.run(SET_BACKGROUND_DDL).await?;

    // Make sure MV can be created after all these cancels
    // Keep retrying since cancel happens async.
    loop {
        let result = create_mv(&mut session).await;
        match result {
            Ok(_) => break,
            Err(e) if e.to_string().contains("still being created") => {
                tracing::info!("create mv failed, retrying: {}", e);
            }
            Err(e) => {
                return Err(e);
            }
        }
        sleep(Duration::from_secs(2)).await;
        tracing::info!("create mv failed, retrying");
    }

    // Wait for job to finish
    session.run(WAIT).await?;

    session.run("DROP MATERIALIZED VIEW mv1").await?;
    session.run("DROP TABLE t").await?;

    Ok(())
}

#[tokio::test]
async fn test_cancel_snapshot_backfill_unregisters_table_ids() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 1;").await?;
    session.run("set backfill_rate_limit = 1;").await?;
    session
        .run("set streaming_use_snapshot_backfill = true;")
        .await?;
    session.run("set background_ddl = true;").await?;

    session
        .run("create table t (k int primary key, v int);")
        .await?;
    session
        .run("insert into t select i, i from generate_series(1, 10000) as i;")
        .await?;
    session.flush().await?;

    let baseline_member_tables = member_table_ids(&mut session).await?;

    let mut create_session = cluster.start_session();
    let create_handle = tokio::spawn(async move {
        create_session.run("set streaming_parallelism = 1;").await?;
        create_session.run("set backfill_rate_limit = 1;").await?;
        create_session
            .run("set streaming_use_snapshot_backfill = true;")
            .await?;
        create_session.run("set background_ddl = true;").await?;
        create_session
            .run("create materialized view mv as select * from t;")
            .await
    });

    let running_jobs = wait_jobs_running(&mut session).await?;
    tracing::info!("running jobs before cancel: {running_jobs}");
    sleep(Duration::from_secs(2)).await;

    let member_tables_before_cancel = member_table_ids(&mut session).await?;
    assert!(
        member_tables_before_cancel.len() > baseline_member_tables.len(),
        "snapshot backfill did not create extra member tables before cancel: baseline={baseline_member_tables:?}, current={member_tables_before_cancel:?}"
    );

    let job_id = running_jobs
        .lines()
        .find(|line| !line.trim().is_empty())
        .and_then(|line| line.split_whitespace().next())
        .ok_or_else(|| anyhow!("failed to parse job id from show jobs output: {running_jobs}"))?;
    let cancel_result = session.run(format!("cancel job {job_id};")).await?;
    assert!(
        cancel_result.contains(job_id),
        "unexpected cancel result: {cancel_result}"
    );

    wait_for_jobs_cleared(&mut session).await?;
    wait_member_table_ids(&mut session, &baseline_member_tables).await?;

    let member_tables_after_cancel = member_table_ids(&mut session).await?;
    assert_eq!(member_tables_after_cancel, baseline_member_tables);

    let create_result = create_handle.await.unwrap();
    if let Err(err) = create_result {
        assert!(
            err.to_string().contains("cancel")
                || err.to_string().contains("drop")
                || err.to_string().contains("not found"),
            "unexpected create mv result after cancel: {err}"
        );
    }

    session.run("drop table t;").await?;
    Ok(())
}

/// When cancelling a stream job under high latency,
/// the cancel should take a long time to take effect.
/// If we trigger a recovery however, the cancel should take effect immediately,
/// since cancel will immediately drop the table fragment.
async fn test_high_barrier_latency_cancel(config: Configuration) -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    // Join 2 fact tables together to create a high barrier latency scenario.

    session.run("CREATE TABLE fact1 (v1 int)").await?;
    session
        .run("INSERT INTO fact1 select 1 from generate_series(1, 10000)")
        .await?;

    session.run("CREATE TABLE fact2 (v1 int)").await?;
    session
        .run("INSERT INTO fact2 select 1 from generate_series(1, 10000)")
        .await?;
    session.flush().await?;

    tracing::info!("seeded base tables");

    // Create high barrier latency scenario
    // Keep creating mv1, if it's not created.
    loop {
        session.run(SET_BACKGROUND_DDL).await?;
        session.run(SET_RATE_LIMIT_2).await?;
        session.run("CREATE MATERIALIZED VIEW mv1 as select fact1.v1 from fact1 join fact2 on fact1.v1 = fact2.v1").await?;
        tracing::info!("created mv in background");
        sleep(Duration::from_secs(1)).await;

        cluster
            .kill_nodes_and_restart(["compute-1", "compute-2", "compute-3"], 2)
            .await;
        sleep(Duration::from_secs(2)).await;

        tracing::debug!("killed cn, waiting recovery");

        // Check if mv stream job is created in the background
        match session
            .run("select * from rw_catalog.rw_ddl_progress;")
            .await
        {
            Ok(s) if s.is_empty() => {
                // MV was dropped
                continue;
            }
            Err(e) => {
                return Err(e);
            }
            Ok(s) => {
                tracing::info!("created mv stream job with status: {}", s);
                break;
            }
        }
    }

    tracing::info!("restarted cn: trigger stream job recovery");

    // Make sure there's some progress first.
    loop {
        // Wait until at least 10% of records are ingested.
        let progress = session
            .run("select progress from rw_catalog.rw_ddl_progress;")
            .await
            .unwrap();
        tracing::info!(progress, "get progress before cancel stream job");
        let progress = progress.split_once("%").unwrap().0;
        let progress = progress.parse::<f64>().unwrap();
        if progress >= 0.01 {
            break;
        } else {
            sleep(Duration::from_micros(1)).await;
        }
    }
    // Loop in case the cancel gets dropped after
    // cn kill, before it drops the table fragment.
    for iteration in 0..5 {
        tracing::info!(iteration, "cancelling stream job");
        let mut session2 = cluster.start_session();
        let handle = tokio::spawn(async move {
            let result = cancel_stream_jobs(&mut session2).await;
            tracing::info!(?result, "cancel stream jobs");
        });

        sleep(Duration::from_millis(500)).await;
        kill_cn_and_wait_recover(&mut cluster).await;
        tracing::info!("restarted cn: cancel should take effect");

        handle.await.unwrap();

        // Create MV with same relation name should succeed,
        // since the previous job should be cancelled.
        tracing::info!("recreating mv");
        session.run("SET BACKGROUND_DDL=false").await?;
        if let Err(e) = session
            .run("CREATE MATERIALIZED VIEW mv1 as values(1)")
            .await
        {
            tracing::info!(error = %e.as_report(), "Recreate mv failed");
            continue;
        } else {
            tracing::info!("recreated mv");
            break;
        }
    }

    session.run(DROP_MV1).await?;
    session.run("DROP TABLE fact1").await?;
    session.run("DROP TABLE fact2").await?;

    Ok(())
}

#[tokio::test]
async fn test_high_barrier_latency_cancel_for_arrangement_backfill() -> Result<()> {
    test_high_barrier_latency_cancel(Configuration::for_arrangement_backfill()).await
}

#[tokio::test]
async fn test_high_barrier_latency_cancel_for_no_shuffle() -> Result<()> {
    test_high_barrier_latency_cancel(Configuration::for_scale_no_shuffle()).await
}

// A foreground DDL that has waited past the early-failure timeout stays attached through recovery.
#[tokio::test]
async fn test_late_foreground_ddl_recovery() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();
    session.run(CREATE_TABLE).await?;
    session.run(SEED_TABLE_100).await?;
    session.flush().await?;

    let mut session2 = cluster.start_session();
    let create_handle = tokio::spawn(async move {
        session2.run(SET_RATE_LIMIT_0).await.unwrap();
        create_mv(&mut session2).await
    });

    // Wait for job to start
    sleep(Duration::from_secs(2)).await;
    // Keep the paused backfill waiting past the 30-second early-failure timeout.
    sleep(Duration::from_secs(31)).await;

    // Killing a CN wakes the finish waiter, which should re-register after recovery.
    kill_cn_and_wait_recover(&mut cluster).await;
    assert!(!create_handle.is_finished());

    // The preserved job still owns the relation name.
    session.run(SET_RATE_LIMIT_2).await?;
    let err = create_mv(&mut session)
        .await
        .expect_err("duplicate CREATE should observe the recovered job");
    assert!(
        err.to_string().contains("still being created"),
        "unexpected duplicate CREATE error: {err}"
    );

    // Explicit cancellation wakes the re-registered waiter and allows the job to be recreated.
    assert_eq!(cancel_stream_jobs(&mut session).await?.len(), 1);
    wait_for_jobs_cleared(&mut session).await?;
    assert!(create_handle.await?.is_err());
    session.run(RESET_RATE_LIMIT).await?;
    create_mv(&mut session).await?;

    session.run(DROP_MV1).await?;
    session.run(DROP_TABLE).await?;

    Ok(())
}

#[tokio::test]
async fn test_foreground_index_cancel() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();
    session.run(CREATE_TABLE).await?;
    session.run(SEED_TABLE_100).await?;
    session.flush().await?;

    let mut session2 = cluster.start_session();
    tokio::spawn(async move {
        session2.run(SET_RATE_LIMIT_2).await.unwrap();
        let result = session2.run("CREATE INDEX i ON t (v1);").await;
        assert!(result.is_err());
    });

    // Wait for job to start
    sleep(Duration::from_secs(2)).await;

    // Kill CN should stop the job
    cancel_stream_jobs(&mut session).await?;

    // Create MV should succeed, since the previous foreground job should be cancelled.
    session.run(SET_RATE_LIMIT_2).await?;
    session.run("CREATE INDEX i ON t (v1);").await?;

    session.run("DROP INDEX i;").await?;
    session.run(DROP_TABLE).await?;

    Ok(())
}

#[tokio::test]
async fn test_background_sink_create() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();
    session.run(CREATE_TABLE).await?;
    session.run(SEED_TABLE_100).await?;
    session.flush().await?;

    let mut session2 = cluster.start_session();
    tokio::spawn(async move {
        session2.run(SET_BACKGROUND_DDL).await.unwrap();
        session2.run(SET_RATE_LIMIT_2).await.unwrap();
        session2
            .run("CREATE SINK s FROM t WITH (connector='blackhole');")
            .await
            .expect("create sink should succeed");
    });

    // Wait for job to start
    sleep(Duration::from_secs(2)).await;

    kill_cn_meta_and_wait_full_recovery(&mut cluster).await;

    // Sink job should still be present, and we can drop it.
    session.run("DROP SINK s;").await?;
    session.run(DROP_TABLE).await?;

    Ok(())
}

#[tokio::test]
async fn test_background_agg_mv_recovery() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();

    session.run("CREATE TABLE t1 (v1 int)").await?;
    session
        .run("INSERT INTO t1 SELECT generate_series FROM generate_series(1, 200);")
        .await?;
    session.flush().await?;
    session.run(SET_RATE_LIMIT_1).await?;
    session.run(SET_BACKGROUND_DDL).await?;
    session
        .run("CREATE MATERIALIZED VIEW mv1 as select v1, count(*) from t1 group by v1;")
        .await?;
    sleep(Duration::from_secs(2)).await;

    kill_cn_and_meta_and_wait_recover(&mut cluster).await;

    // Now just wait for it to complete.
    session.run(WAIT).await?;

    let t_count = session.run("SELECT COUNT(v1) FROM t1").await?;
    let mv1_count = session.run("SELECT COUNT(v1) FROM mv1").await?;
    assert_eq!(t_count, mv1_count);

    // Make sure that if MV killed and restarted
    // it will not be dropped.
    session.run("DROP MATERIALIZED VIEW mv1;").await?;
    session.run("DROP TABLE t1;").await?;

    Ok(())
}

#[tokio::test]
async fn test_background_index_creation() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();

    // Create table and insert some data
    session.run("CREATE TABLE t(v1 int, v2 int);").await?;
    session
        .run("INSERT INTO t SELECT generate_series, generate_series * 2 FROM generate_series(1, 100);")
        .await?;
    session.flush().await?;

    // Enable background DDL and create index
    session.run(SET_RATE_LIMIT_2).await?;
    session.run(SET_BACKGROUND_DDL).await?;
    session.run("CREATE INDEX idx_v1 ON t(v1);").await?;

    // Kill CN and recover to test background index recovery
    kill_cn_and_wait_recover(&mut cluster).await;

    // Add more data
    session
        .run("INSERT INTO t SELECT generate_series, generate_series * 2 FROM generate_series(101, 200);")
        .await?;
    session.flush().await?;

    // Wait for background index creation to complete
    session.run(WAIT).await?;

    // Verify the index was created successfully
    let index_exists = session
        .run("SELECT 1 FROM rw_catalog.rw_indexes WHERE name = 'idx_v1';")
        .await?;
    assert!(!index_exists.is_empty(), "Index should be created");

    // Verify index can be used (basic functionality test)
    let count = session.run("SELECT COUNT(*) FROM t WHERE v1 = 50;").await?;
    assert_eq!(count.trim(), "1");

    // Clean up
    session.run("DROP INDEX idx_v1;").await?;
    session.run("DROP TABLE t;").await?;

    Ok(())
}
