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

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use risingwave_simulation::cluster::{Cluster, Configuration, Session};
use tokio::time::sleep;

const DATABASE_RECOVERY_START: &str = "DATABASE_RECOVERY_START";
const DATABASE_RECOVERY_SUCCESS: &str = "DATABASE_RECOVERY_SUCCESS";

async fn assert_no_expected_global_recovery(session: &mut Session) -> Result<()> {
    let global_recovery_events = global_recovery_events(session).await?;

    assert!(
        global_recovery_events
            .iter()
            .all(|(_, reason)| ["adhoc recovery", "bootstrap"].contains(&&**reason)),
        "unexpected recovery reason: {:?}",
        global_recovery_events
    );
    Ok(())
}

const MAX_HEARTBEAT_INTERVAL_SEC: u64 = 10;

#[tokio::test]
async fn test_adhoc_database_recovery() -> Result<()> {
    let (_cluster, mut session) = prepare_isolation_env().await?;

    session.run("use group1").await?;
    session.run("create table t1 (v int)").await?;
    session.run("use group2").await?;
    session.run("create table t2 (v int)").await?;

    let database_mapping = database_id_mapping(&mut session).await?;
    let group1_database_id = database_mapping["group1"];
    let group2_database_id = database_mapping["group2"];
    let global_events_before = global_recovery_events(&mut session).await?;

    session.run("recover group1").await?;

    let mut database_events = database_recovery_events(&mut session).await?;
    assert_eq!(
        database_events.remove(&group1_database_id),
        Some(vec![
            DATABASE_RECOVERY_START.to_owned(),
            DATABASE_RECOVERY_SUCCESS.to_owned(),
        ])
    );
    assert!(!database_events.contains_key(&group2_database_id));
    assert_eq!(
        global_recovery_events(&mut session).await?,
        global_events_before
    );

    session.run("use group1").await?;
    session.run("insert into t1 values (1)").await?;
    session.run("use group2").await?;
    session.run("insert into t2 values (2)").await?;

    Ok(())
}

#[tokio::test]
async fn test_isolation_simple_two_databases() -> Result<()> {
    let (cluster, mut session) = prepare_isolation_env().await?;

    session.run("use group1").await?;
    session.run("create table t1 (v int);").await?;
    session.run("use group2").await?;
    session.run("create table t2 (v int);").await?;

    cluster.simple_kill_nodes(["compute-1"]).await;

    session.run("use group1").await?;

    let database_mapping = database_id_mapping(&mut session).await?;

    let group1_database_id = database_mapping["group1"];
    let group2_database_id = database_mapping["group2"];

    // should fail
    assert!(
        session
            .run("insert into t1 select * from generate_series(1, 100);")
            .await
            .is_err()
    );

    session.run("use group2").await?;
    session
        .run("insert into t2 select * from generate_series(1, 100);")
        .await?;

    cluster.simple_restart_nodes(["compute-1"]).await;

    sleep(Duration::from_secs(MAX_HEARTBEAT_INTERVAL_SEC)).await;

    session.run("use group1").await?;
    session
        .run("insert into t1 select * from generate_series(1, 100);")
        .await?;

    let mut database_recovery_events = database_recovery_events(&mut session).await?;

    assert!(!database_recovery_events.contains_key(&group2_database_id));
    assert_eq!(
        database_recovery_events.remove(&group1_database_id),
        Some(vec![
            DATABASE_RECOVERY_START.to_owned(),
            DATABASE_RECOVERY_SUCCESS.to_owned()
        ])
    );

    assert_no_expected_global_recovery(&mut session).await?;

    Ok(())
}

async fn database_id_mapping(session: &mut Session) -> Result<HashMap<String, u32>> {
    let events = session.run("select name, id from rw_databases").await?;

    let databases: HashMap<_, _> = events
        .lines()
        .map(|line| {
            let (name, num_str) = line.rsplit_once(' ').unwrap();
            let num = u32::from_str(num_str.trim()).unwrap();
            (name.to_owned(), num)
        })
        .collect();

    Ok(databases)
}

async fn database_recovery_events(session: &mut Session) -> Result<HashMap<u32, Vec<String>>> {
    let events = session.run("
    select event_type,
       case event_type
           when 'DATABASE_RECOVERY_START' then info -> 'recovery' -> 'databaseStart' ->> 'databaseId'
           when 'DATABASE_RECOVERY_SUCCESS' then info -> 'recovery' -> 'databaseSuccess' ->> 'databaseId'
           when 'DATABASE_RECOVERY_FAILURE' then info -> 'recovery' -> 'databaseFailure' ->> 'databaseId'
           end as database_id
from rw_catalog.rw_event_logs
where event_type like '%DATABASE_RECOVERY%'
order by timestamp;").await?;

    let mut result = HashMap::new();

    for line in events.lines() {
        let (event_type, num_str) = line.rsplit_once(' ').unwrap();
        let num = u32::from_str(num_str.trim())?;
        result
            .entry(num)
            .or_insert_with(Vec::new)
            .push(event_type.to_owned());
    }

    Ok(result)
}

async fn global_recovery_events(session: &mut Session) -> Result<Vec<(String, String)>> {
    let events = session
        .run(
            "select event_type,
       case event_type
           when 'GLOBAL_RECOVERY_START' then info -> 'recovery' -> 'globalStart' ->> 'reason'
           when 'GLOBAL_RECOVERY_SUCCESS' then info -> 'recovery' -> 'globalSuccess' ->> 'reason'
           end as reason
from rw_catalog.rw_event_logs
where event_type like '%GLOBAL_RECOVERY%'
order by timestamp;",
        )
        .await?;

    let events = events
        .lines()
        .map(|line| {
            let (event_type, reason) = line.split_once(' ').unwrap();
            (event_type.to_owned(), reason.to_owned())
        })
        .collect_vec();

    Ok(events)
}

#[tokio::test]
async fn test_isolation_simple_two_databases_join() -> Result<()> {
    let (cluster, mut session) = prepare_isolation_env().await?;

    let database_id_mapping = database_id_mapping(&mut session).await?;

    let group1_database_id = database_id_mapping["group1"];
    let group2_database_id = database_id_mapping["group2"];

    session.run("use group1").await?;
    session.run("create table t1 (v int);").await?;
    session
        .run("insert into t1 select * from generate_series(1, 100);")
        .await?;
    session
        .run("create subscription sub1 from t1 with(retention = '1D');")
        .await?;

    session.run("use group2").await?;
    session.run("create table t2 (v int);").await?;

    session
        .run("insert into t2 select * from generate_series(1, 50);")
        .await?;

    wait_until(
        &mut session,
        "select count(*) from group1.public.t1;",
        "100",
    )
    .await?;

    session
        .run("create materialized view mv_join as select t2.v as v from group1.public.t1 join t2 on t1.v = t2.v;")
        .await?;

    wait_until(&mut session, "select count(*) from mv_join;", "50").await?;

    cluster.simple_kill_nodes(["compute-1"]).await;

    session.run("use group1").await?;

    // should fail
    assert!(session.run("insert into t1 values (1)").await.is_err());

    session.run("use group2").await?;
    session
        .run("insert into t2 select * from generate_series(51, 120)")
        .await?;

    wait_until(&mut session, "select max(v) from t2", "120").await?;
    wait_until(&mut session, "select count(*) from mv_join;", "100").await?;

    // Wait until the killed compute node is removed from `rw_worker_nodes`.
    let wait_worker_unregister_sql = "select count(*) from rw_catalog.rw_worker_nodes \
where host = '192.168.3.1';";
    wait_until(&mut session, wait_worker_unregister_sql, "0").await?;

    session.run("recover").await?;

    cluster.simple_restart_nodes(["compute-1"]).await;

    sleep(Duration::from_secs(MAX_HEARTBEAT_INTERVAL_SEC * 2)).await;

    session.run("use group1").await?;
    session
        .run("insert into t1 select * from generate_series(101, 110);")
        .await?;

    session.run("use group2").await?;

    // flush is only oriented to the current database, so flush is required here
    session.run("flush").await?;

    wait_until(&mut session, "select count(*) from mv_join;", "110").await?;

    let mut database_recovery_events = database_recovery_events(&mut session).await?;

    assert!(!database_recovery_events.contains_key(&group2_database_id));

    assert_eq!(
        database_recovery_events.remove(&group1_database_id),
        Some(vec![
            DATABASE_RECOVERY_START.to_owned(),
            DATABASE_RECOVERY_SUCCESS.to_owned(),
        ])
    );

    assert_no_expected_global_recovery(&mut session).await?;

    Ok(())
}

#[tokio::test]
async fn test_isolation_simple_two_databases_join_in_other() -> Result<()> {
    let (cluster, mut session) = prepare_isolation_env().await?;

    let database_id_mapping = database_id_mapping(&mut session).await?;

    let group1_database_id = database_id_mapping["group1"];
    let group2_database_id = database_id_mapping["group2"];
    let group3_database_id = database_id_mapping["group3"];

    // group1
    session.run("use group1").await?;
    session.run("create table t1 (v int);").await?;
    session
        .run("insert into t1 select * from generate_series(1, 100);")
        .await?;
    session
        .run("create subscription sub1 from t1 with(retention = '1D');")
        .await?;

    // group2
    session.run("use group2").await?;
    session.run("create table t2 (v int);").await?;

    session
        .run("insert into t2 select * from generate_series(1, 50);")
        .await?;
    session
        .run("create subscription sub2 from t2 with(retention = '1D');")
        .await?;

    // group3
    session.run("use group3").await?;
    session
        .run("create materialized view mv_join as select t2.v as v from group1.public.t1 join group2.public.t2 on t1.v = t2.v;")
        .await?;

    wait_until(&mut session, "select count(*) from mv_join;", "50").await?;

    cluster.simple_kill_nodes(["compute-1", "compute-3"]).await;

    session.run("use group1").await?;

    // should fail
    assert!(session.run("insert into t1 values (1)").await.is_err());

    // should fail
    assert!(session.run("flush").await.is_err());

    session.run("use group2").await?;
    session
        .run("insert into t2 select * from generate_series(51, 120)")
        .await?;

    session.run("flush").await?;

    cluster.simple_restart_nodes(["compute-1"]).await;

    sleep(Duration::from_secs(MAX_HEARTBEAT_INTERVAL_SEC)).await;

    session.run("use group1").await?;

    session
        .run("insert into t1 select * from generate_series(101, 110);")
        .await?;

    cluster.simple_restart_nodes(["compute-3"]).await;

    sleep(Duration::from_secs(MAX_HEARTBEAT_INTERVAL_SEC)).await;

    session.run("use group3").await?;

    wait_until(&mut session, "select count(*) from mv_join;", "110").await?;

    let mut database_recovery_events = database_recovery_events(&mut session).await?;

    assert!(!database_recovery_events.contains_key(&group2_database_id));

    assert_eq!(
        database_recovery_events.remove(&group1_database_id),
        Some(vec![
            DATABASE_RECOVERY_START.to_owned(),
            DATABASE_RECOVERY_SUCCESS.to_owned()
        ])
    );

    assert_eq!(
        database_recovery_events.remove(&group3_database_id),
        Some(vec![
            DATABASE_RECOVERY_START.to_owned(),
            DATABASE_RECOVERY_SUCCESS.to_owned()
        ])
    );

    assert_no_expected_global_recovery(&mut session).await?;

    Ok(())
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

async fn prepare_isolation_env() -> Result<(Cluster, Session)> {
    let mut config = Configuration::for_auto_parallelism(MAX_HEARTBEAT_INTERVAL_SEC, true);

    config.compute_nodes = 3;
    config.compute_node_cores = 2;
    config.compute_resource_groups = HashMap::from([
        (1, "group1".to_owned()),
        (2, "group2".to_owned()),
        (3, "group3".to_owned()),
    ]);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session
        .run("create database group1 with resource_group='group1'")
        .await?;
    session
        .run("create database group2 with resource_group='group2'")
        .await?;
    session
        .run("create database group3 with resource_group='group3'")
        .await?;

    session.run("set rw_implicit_flush = true;").await?;

    Ok((cluster, session))
}
