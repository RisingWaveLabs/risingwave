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

use std::io::Write;
use std::time::Duration;

use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration};
use tokio::time::sleep;

// The system parameter enforces a minimum time travel retention of ten minutes.
const TIME_TRAVEL_RETENTION_MS: u64 = 10 * 60 * 1000;
const INSERT_INTERVAL: Duration = Duration::from_secs(60);

fn init_logger() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .try_init();
}

fn cluster_config() -> Configuration {
    let config_path = {
        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp config file");
        file.write_all(
            br#"
[server]
telemetry_enabled = false
metrics_level = "Disabled"

[meta]
hummock_time_travel_snapshot_interval = 10

[meta.developer]
time_travel_vacuum_interval_sec = 5

[streaming.developer]
stream_chunk_size = 1

[system]
barrier_interval_ms = 1000
max_concurrent_creating_streaming_jobs = 4
"#,
        )
        .expect("failed to write config file");
        file.into_temp_path()
    };

    Configuration {
        config_path: ConfigPath::Temp(config_path.into()),
        frontend_nodes: 1,
        compute_nodes: 1,
        meta_nodes: 1,
        compactor_nodes: 2,
        compute_node_cores: 1,
        ..Default::default()
    }
}

#[tokio::test]
async fn test_time_travel_vacuum_during_snapshot_backfill() -> Result<()> {
    init_logger();

    let mut cluster = Cluster::start(cluster_config()).await?;
    let mut session = cluster.start_session();

    session
        .run(format!(
            "alter system set time_travel_retention_ms = {TIME_TRAVEL_RETENTION_MS};"
        ))
        .await?;
    session.run("set rw_implicit_flush = true;").await?;
    session.run("create table healthy_t(id int);").await?;
    session
        .run("create table backfill_t(id int primary key);")
        .await?;
    session
        .run("insert into backfill_t select * from generate_series(1, 300);")
        .await?;
    session.run("flush;").await?;

    session.run("set streaming_parallelism = 1;").await?;
    session
        .run("set streaming_use_snapshot_backfill = true;")
        .await?;
    session.run("set background_ddl = true;").await?;
    session.run("set backfill_rate_limit = 1;").await?;
    session
        .run("create materialized view backfill_mv as select * from backfill_t;")
        .await?;

    // Snapshot backfill can commit a lower table epoch at a later global version. Keep the job
    // active while creating several retained wall-clock mappings that precede those versions.
    for id in 1..=3 {
        session
            .run(format!("insert into healthy_t values ({id});"))
            .await?;
        sleep(INSERT_INTERVAL).await;
    }

    assert_eq!(
        session
            .run("select count(*) from rw_catalog.rw_ddl_progress;")
            .await?,
        "1"
    );
    session.run("wait;").await?;
    // Once the job's version pin is released, give vacuum time to process the mixed epoch order.
    sleep(Duration::from_secs(10)).await;

    // Drop compute-side recent-version and time-travel caches before validating the archived
    // metadata path.
    cluster.simple_kill_nodes(["compute-1"]).await;
    cluster.simple_restart_nodes(["compute-1"]).await;
    sleep(Duration::from_secs(10)).await;

    session
        .run(
            "select count(*) from healthy_t \
             for system_time as of now() - '3' minute;",
        )
        .await?;

    Ok(())
}
