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

//! Tests `rw_catalog.rw_recovery_info` after known recovery scenarios by
//! re-deriving every column of the view from the raw `rw_event_logs` rows, so
//! a divergence points at the view or at meta's recovery event emission.

use std::time::Duration;

use anyhow::{Result, anyhow};
use risingwave_simulation::cluster::{Cluster, Configuration, Session};
use serde::Deserialize;
use tokio::time::sleep;

use crate::utils::{kill_cn_and_meta_and_wait_recover, kill_cn_and_wait_recover};

const VIEW_SQL: &str = r#"
SELECT jsonb_build_object(
    'database_id', database_id,
    'database_name', database_name,
    'recovery_state', recovery_state,
    'last_database_event', last_database_event,
    'last_global_event', last_global_event,
    'in_global_running', in_global_running,
    'in_global_recovering', in_global_recovering
) AS json_line
FROM rw_catalog.rw_recovery_info
ORDER BY database_id;
"#;

const EVENTS_SQL: &str = r#"
SELECT jsonb_build_object(
    'event_type', event_type,
    'ts', extract(epoch FROM timestamp)::double precision,
    'info', info
) AS json_line
FROM rw_catalog.rw_event_logs
WHERE event_type LIKE 'DATABASE_RECOVERY%' OR event_type LIKE 'GLOBAL_RECOVERY%'
ORDER BY timestamp, event_type;
"#;

#[derive(Debug, Deserialize)]
struct RecoveryInfoRow {
    database_id: u32,
    database_name: String,
    recovery_state: String,
    last_database_event: String,
    last_global_event: String,
    // NULL when no global recovery event has been logged yet.
    in_global_running: Option<bool>,
    in_global_recovering: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct RecoveryEventRow {
    event_type: String,
    ts: f64,
    info: serde_json::Value,
}

fn parse_lines<T: for<'a> Deserialize<'a>>(output: &str) -> Result<Vec<T>> {
    output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| Ok(serde_json::from_str(line)?))
        .collect()
}

/// Fetches the view and the recovery events, retrying until no event is
/// flushed between the two reads.
async fn stable_snapshot(
    session: &mut Session,
) -> Result<(Vec<RecoveryInfoRow>, Vec<RecoveryEventRow>)> {
    for _ in 0..20 {
        let events_before = session.run(EVENTS_SQL).await?;
        let rows = session.run(VIEW_SQL).await?;
        let events_after = session.run(EVENTS_SQL).await?;
        if events_before == events_after {
            return Ok((parse_lines(&rows)?, parse_lines(&events_before)?));
        }
        sleep(Duration::from_millis(200)).await;
    }
    Err(anyhow!("event log kept changing while snapshotting"))
}

fn json_u32(value: &serde_json::Value) -> Option<u32> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        .map(|id| id as u32)
}

/// The database a `DATABASE_RECOVERY_*` event refers to. `None` mirrors the
/// view dropping events whose id it cannot extract.
fn event_database_id(info: &serde_json::Value) -> Option<u32> {
    let recovery = info.get("recovery")?;
    ["databaseStart", "databaseSuccess", "databaseFailure"]
        .iter()
        .find_map(|key| recovery.get(key))
        .and_then(|event| event.get("databaseId"))
        .and_then(json_u32)
}

fn global_id_list(info: &serde_json::Value, key: &str) -> Vec<u32> {
    info.get("recovery")
        .and_then(|r| r.get("globalSuccess"))
        .and_then(|s| s.get(key))
        .and_then(|v| v.as_array())
        .map(|ids| ids.iter().filter_map(json_u32).collect())
        .unwrap_or_default()
}

#[derive(Debug, PartialEq)]
struct Expected {
    last_database_event: String,
    last_global_event: String,
    in_global_running: bool,
    in_global_recovering: bool,
    recovery_state: String,
}

/// Re-derives one view row from the latest global recovery event and the
/// database's latest own event, following the view's SQL exactly.
fn derive_expected(
    database_id: u32,
    global: Option<&RecoveryEventRow>,
    database: Option<&RecoveryEventRow>,
) -> Expected {
    let last_database_event = database
        .map(|d| {
            d.event_type
                .trim_start_matches("DATABASE_RECOVERY_")
                .to_owned()
        })
        .unwrap_or_else(|| "UNKNOWN".to_owned());

    let mut in_global_running = false;
    let mut in_global_recovering = false;
    let mut last_global_event = "UNKNOWN";
    if let Some(g) = global {
        if g.event_type == "GLOBAL_RECOVERY_SUCCESS" {
            // The view only trusts a global event that is at least as new as the
            // database's own latest event.
            if g.ts >= database.map_or(g.ts, |d| d.ts) {
                in_global_running =
                    global_id_list(&g.info, "runningDatabaseIds").contains(&database_id);
                in_global_recovering =
                    global_id_list(&g.info, "recoveringDatabaseIds").contains(&database_id);
                last_global_event = if in_global_running {
                    "RUNNING"
                } else if in_global_recovering {
                    "RECOVERING"
                } else {
                    "UNKNOWN"
                };
            }
        } else if g.event_type == "GLOBAL_RECOVERY_FAILURE" {
            last_global_event = "RECOVERING";
        }
    }

    let recovery_state = if last_database_event == "SUCCESS"
        || (last_global_event == "RUNNING" && in_global_running)
    {
        "RUNNING"
    } else if (last_global_event == "RUNNING" && in_global_recovering)
        || (last_global_event == "RECOVERING" && in_global_recovering)
        || last_database_event == "START"
        || last_global_event == "RECOVERING"
    {
        "RECOVERING"
    } else {
        "UNKNOWN"
    };

    Expected {
        last_database_event,
        last_global_event: last_global_event.to_owned(),
        in_global_running,
        in_global_recovering,
        recovery_state: recovery_state.to_owned(),
    }
}

/// Checks every view row against the raw event log. Events sharing the maximal
/// timestamp are all tried, since the view breaks such ties arbitrarily.
fn check_view_against_events(rows: &[RecoveryInfoRow], events: &[RecoveryEventRow]) {
    let globals: Vec<_> = events
        .iter()
        .filter(|e| {
            matches!(
                e.event_type.as_str(),
                "GLOBAL_RECOVERY_SUCCESS" | "GLOBAL_RECOVERY_FAILURE"
            )
        })
        .collect();
    let global_max_ts = globals
        .iter()
        .map(|e| e.ts)
        .fold(f64::NEG_INFINITY, f64::max);
    let global_candidates: Vec<&RecoveryEventRow> = globals
        .iter()
        .copied()
        .filter(|e| e.ts == global_max_ts)
        .collect();

    for row in rows {
        let db_events: Vec<_> = events
            .iter()
            .filter(|e| e.event_type.starts_with("DATABASE_RECOVERY_"))
            .filter(|e| event_database_id(&e.info) == Some(row.database_id))
            .collect();
        let db_max_ts = db_events
            .iter()
            .map(|e| e.ts)
            .fold(f64::NEG_INFINITY, f64::max);
        let db_candidates: Vec<&RecoveryEventRow> = db_events
            .iter()
            .copied()
            .filter(|e| e.ts == db_max_ts)
            .collect();

        let global_options: Vec<Option<&RecoveryEventRow>> = if global_candidates.is_empty() {
            vec![None]
        } else {
            global_candidates.iter().map(|e| Some(*e)).collect()
        };
        let db_options: Vec<Option<&RecoveryEventRow>> = if db_candidates.is_empty() {
            vec![None]
        } else {
            db_candidates.iter().map(|e| Some(*e)).collect()
        };

        let mut mismatches = Vec::new();
        let matched = global_options.iter().any(|global| {
            db_options.iter().any(|database| {
                let expected = derive_expected(row.database_id, *global, *database);
                let ok = expected.last_database_event == row.last_database_event
                    && expected.last_global_event == row.last_global_event
                    && expected.in_global_running == row.in_global_running.unwrap_or(false)
                    && expected.in_global_recovering == row.in_global_recovering.unwrap_or(false)
                    && expected.recovery_state == row.recovery_state;
                if !ok {
                    mismatches.push(expected);
                }
                ok
            })
        });
        assert!(
            matched,
            "rw_recovery_info row diverges from the event log:\nrow: {row:?}\nexpected one of ({} global x {} database candidates): {mismatches:?}",
            global_options.len(),
            db_options.len(),
        );
    }
}

fn row<'a>(rows: &'a [RecoveryInfoRow], name: &str) -> &'a RecoveryInfoRow {
    rows.iter()
        .find(|r| r.database_name == name)
        .unwrap_or_else(|| panic!("database {name} missing from rw_recovery_info: {rows:?}"))
}

/// Both databases need streaming state so that recovery tracks them.
async fn create_streaming_jobs(session: &mut Session) -> Result<()> {
    session.run("create table t (v int);").await?;
    session
        .run("create materialized view mv as select count(*) as c from t;")
        .await?;
    Ok(())
}

async fn setup_two_databases(cluster: &mut Cluster) -> Result<Session> {
    let mut session = cluster.start_session();
    create_streaming_jobs(&mut session).await?;
    session.run("create database db2;").await?;
    session.run("use db2;").await?;
    create_streaming_jobs(&mut session).await?;
    Ok(session)
}

#[tokio::test]
async fn test_recovery_info_view_after_cn_kill() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = setup_two_databases(&mut cluster).await?;

    let (rows, events) = stable_snapshot(&mut session).await?;
    check_view_against_events(&rows, &events);
    // A database created after the last recovery has no event referencing it.
    assert_eq!(row(&rows, "db2").recovery_state, "UNKNOWN");
    let events_before = events.len();
    drop(session);

    kill_cn_and_wait_recover(&mut cluster).await;

    let mut session = cluster.start_session();
    let (rows, events) = stable_snapshot(&mut session).await?;
    check_view_against_events(&rows, &events);
    assert!(
        events.len() > events_before,
        "killing compute nodes must append recovery events: before={events_before}, after={}",
        events.len()
    );
    for name in ["dev", "db2"] {
        assert_eq!(row(&rows, name).recovery_state, "RUNNING");
    }
    Ok(())
}

#[tokio::test]
async fn test_recovery_info_view_after_meta_kill() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let session = setup_two_databases(&mut cluster).await?;
    drop(session);

    kill_cn_and_meta_and_wait_recover(&mut cluster).await;

    let mut session = cluster.start_session();
    let (rows, events) = stable_snapshot(&mut session).await?;
    check_view_against_events(&rows, &events);
    // The event log is in-memory on meta, so everything present was emitted by
    // the restarted meta, whose bootstrap is a global recovery.
    assert!(
        events
            .iter()
            .any(|e| e.event_type == "GLOBAL_RECOVERY_SUCCESS"),
        "restarted meta must log a global recovery success: {events:?}"
    );
    for name in ["dev", "db2"] {
        let r = row(&rows, name);
        assert_eq!(r.recovery_state, "RUNNING", "database {name}: {r:?}");
        // RUNNING must be explained by one of the two mechanisms: the last
        // global recovery brought the database up, or the database logged its
        // own success afterwards.
        assert!(
            r.in_global_running.unwrap_or(false) || r.last_database_event == "SUCCESS",
            "database {name} is RUNNING without a mechanism explaining it: {r:?}"
        );
    }
    Ok(())
}
