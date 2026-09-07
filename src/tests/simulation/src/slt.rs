// Copyright 2022 RisingWave Labs
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

use std::cmp::min;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use rand::{Rng, rng as thread_rng};
use sqllogictest::substitution::well_known;
use sqllogictest::{
    Condition, ParallelTestError, Partitioner, QueryExpect, Record, StatementExpect,
};

use crate::client::RisingWave;
use crate::cluster::Cluster;
use crate::parse::extract_sql_command;
use crate::slt::background_ddl_mode::*;
use crate::slt::runner::{random_vnode_count, run_kill_not_allowed, run_no_kill};
use crate::slt::slt_env::{Env, Opts};
use crate::slt::vnode_mode::*;
use crate::utils::TimedExt;
use crate::{evaluate_skip, wait_or_retry_background_ddl};

// retry a maximum times until it succeed
const MAX_RETRY: usize = 10;

#[derive(Debug, PartialEq, Eq)]
pub enum SqlCmd {
    /// Other create statements.
    Create {
        is_create_table_as: bool,
    },
    /// Create Materialized views
    CreateMaterializedView {
        name: String,
    },
    /// Set background ddl
    SetBackgroundDdl {
        enable: bool,
    },
    Drop,
    Dml,
    Flush,
    Others,
}

impl SqlCmd {
    fn allow_kill(&self) -> bool {
        matches!(
            self,
            SqlCmd::Create {
                // `create table as` is also not atomic in our system.
                is_create_table_as: false,
                ..
            } | SqlCmd::CreateMaterializedView { .. }
                | SqlCmd::Drop
        )
        // We won't kill during insert/update/delete/alter since the atomicity is not guaranteed.
        // TODO: For `SqlCmd::Alter`, since table fragment and catalog commit for table schema change
        // are not transactional, we can't kill during `alter table add/drop columns` for now, will
        // remove it until transactional commit of table fragment and catalog is supported.
    }

    fn is_create(&self) -> bool {
        matches!(
            self,
            SqlCmd::Create { .. } | SqlCmd::CreateMaterializedView { .. }
        )
    }
}

const KILL_IGNORE_FILES: &[&str] = &[
    // TPCH queries are too slow for recovery.
    "tpch_snapshot.slt",
    "tpch_upstream.slt",
    // Drop is not retryable in search path test.
    "search_path.slt",
    // Transaction statements are not retryable.
    "transaction/now.slt",
    "transaction/read_only_multi_conn.slt",
    "transaction/read_only.slt",
    "transaction/tolerance.slt",
    "transaction/cursor.slt",
    "transaction/cursor_multi_conn.slt",
];

/// Randomly set DDL statements to use `background_ddl`
mod background_ddl_mode {
    use anyhow::bail;
    use rand::Rng;
    use rand_chacha::ChaChaRng;
    use sqllogictest::{Condition, Record, StatementExpect};

    use crate::client::RisingWave;
    use crate::slt::SqlCmd;
    use crate::slt::slt_env::Env;

    /// Wait for background mv to finish creating
    pub(super) async fn wait_background_mv_finished(mview_name: &str) -> anyhow::Result<()> {
        let Ok(rw) = RisingWave::connect("frontend".into(), "dev".into()).await else {
            bail!("failed to connect to frontend for {mview_name}");
        };
        let client = rw.pg_client();
        if client.simple_query("WAIT;").await.is_err() {
            bail!("failed to wait for background mv to finish creating for {mview_name}");
        }

        let Ok(result) = client
            .query(
                "select count(*) from pg_matviews where matviewname=$1;",
                &[&mview_name],
            )
            .await
        else {
            bail!("failed to query pg_matviews for {mview_name}");
        };

        match result[0].try_get::<_, i64>(0) {
            Ok(1) => Ok(()),
            r => bail!("expected 1 row in pg_matviews, got {r:#?} instead for {mview_name}"),
        }
    }

    // TODO(kwannoel): move rng, `background_ddl` to `Env` struct
    pub(super) async fn set_background_ddl<D, M, T>(
        tester: &mut sqllogictest::Runner<D, M>,
        env: &Env,
        record: &Record<T>,
        cmd: &SqlCmd,
        manual_background_ddl_enabled: bool,
        rng: &mut ChaChaRng,
        background_ddl_enabled: &mut bool,
    ) where
        D: sqllogictest::AsyncDB<ColumnType = T>,
        M: sqllogictest::MakeConnection<Conn = D>,
        T: sqllogictest::ColumnType,
    {
        if let Record::Statement {
            loc,
            conditions,
            connection,
            ..
        } = &record
            && matches!(cmd, SqlCmd::CreateMaterializedView { .. })
            && !manual_background_ddl_enabled
            && conditions.iter().all(|c| {
                *c != Condition::SkipIf {
                    label: "madsim".to_owned(),
                }
            })
            && env.background_ddl_rate() > 0.0
        {
            let background_ddl_setting = rng.random_bool(env.background_ddl_rate());
            let set_background_ddl = Record::Statement {
                loc: loc.clone(),
                conditions: conditions.clone(),
                connection: connection.clone(),
                expected: StatementExpect::Ok,
                sql: format!("SET BACKGROUND_DDL={background_ddl_setting};"),
                retry: None,
            };
            tester.run_async(set_background_ddl).await.unwrap();
            *background_ddl_enabled = background_ddl_setting;
        };
    }

    // NOTE(kwannoel): only applicable to mvs currently
    #[macro_export]
    macro_rules! wait_or_retry_background_ddl {
        ($record:expr, $name:expr, $iteration:expr) => {
            let record = $record;
            let i = $iteration;
            tracing::debug!(iteration = i, "Retry for background ddl");
            match wait_background_mv_finished($name).await {
                Ok(_) => {
                    tracing::debug!(
                        iteration = i,
                        "Record with background_ddl {:?} finished",
                        record
                    );
                    break;
                }
                Err(err) => {
                    tracing::error!(
                        iteration = i,
                        ?err,
                        "failed to wait for background mv to finish creating"
                    );
                    if i >= MAX_RETRY {
                        panic!("failed to run test after retry {i} times, error={err:#?}");
                    }
                    continue;
                }
            }
        };
    }
}

mod vnode_mode {
    use std::env;
    use std::hash::{DefaultHasher, Hash, Hasher};
    use std::sync::LazyLock;

    use anyhow::bail;
    use sqllogictest::Partitioner;

    // Copied from sqllogictest-bin.
    #[derive(Clone)]
    pub(super) struct HashPartitioner {
        count: u64,
        id: u64,
    }

    impl HashPartitioner {
        pub(super) fn new(count: u64, id: u64) -> anyhow::Result<Self> {
            if count == 0 {
                bail!("partition count must be greater than zero");
            }
            if id >= count {
                bail!("partition id (zero-based) must be less than count");
            }
            Ok(Self { count, id })
        }
    }

    impl Partitioner for HashPartitioner {
        fn matches(&self, file_name: &str) -> bool {
            let mut hasher = DefaultHasher::new();
            file_name.hash(&mut hasher);
            hasher.finish() % self.count == self.id
        }
    }

    pub(super) static PARTITIONER: LazyLock<Option<HashPartitioner>> = LazyLock::new(|| {
        let count = env::var("BUILDKITE_PARALLEL_JOB_COUNT")
            .ok()?
            .parse::<u64>()
            .unwrap();
        let id = env::var("BUILDKITE_PARALLEL_JOB")
            .ok()?
            .parse::<u64>()
            .unwrap();
        Some(HashPartitioner::new(count, id).unwrap())
    });
}

pub mod slt_env {
    use rand::SeedableRng;
    use rand_chacha::ChaChaRng;

    use crate::cluster::KillOpts;

    pub struct Opts {
        pub kill_opts: KillOpts,
        /// Probability of `background_ddl` being set to true per ddl record.
        pub background_ddl_rate: f64,
        /// Set vnode count (`STREAMING_MAX_PARALLELISM`) to random value before running DDL.
        pub random_vnode_count: bool,
    }

    pub(super) struct Env {
        opts: Opts,
    }

    impl Env {
        pub fn new(opts: Opts) -> Self {
            Self { opts }
        }

        pub fn get_rng() -> ChaChaRng {
            let seed = std::env::var("MADSIM_TEST_SEED")
                .unwrap_or("0".to_owned())
                .parse::<u64>()
                .unwrap();
            ChaChaRng::seed_from_u64(seed)
        }

        pub fn background_ddl_rate(&self) -> f64 {
            self.opts.background_ddl_rate
        }

        pub fn kill(&self) -> bool {
            self.opts.kill_opts.kill_compute
                || self.opts.kill_opts.kill_meta
                || self.opts.kill_opts.kill_frontend
                || self.opts.kill_opts.kill_compactor
        }

        pub fn random_vnode_count(&self) -> bool {
            self.opts.random_vnode_count
        }

        pub fn kill_opts(&self) -> KillOpts {
            self.opts.kill_opts
        }

        pub fn kill_rate(&self) -> f64 {
            self.opts.kill_opts.kill_rate as f64
        }
    }
}

mod runner {
    use std::time::Duration;

    use rand::prelude::IteratorRandom;
    use rand::rng as thread_rng;
    use sqllogictest::{Record, StatementExpect, TestError};

    use crate::slt::slt_env::Env;
    use crate::slt::{MAX_RETRY, SqlCmd};
    use crate::utils::TimedExt;
    #[macro_export]
    macro_rules! evaluate_skip {
        ($env:expr, $path:expr) => {
            if let Some(partitioner) = PARTITIONER.as_ref()
                && !partitioner.matches($path.to_str().unwrap())
            {
                println!("[skip partition] {}", $path.display());
                continue;
            } else if $env.kill() && KILL_IGNORE_FILES.iter().any(|s| $path.ends_with(s)) {
                println!("[skip kill] {}", $path.display());
                continue;
            } else {
                println!("[run] {}", $path.display());
            }
        };
    }

    pub(super) fn random_vnode_count<T: sqllogictest::ColumnType>(
        env: &Env,
        records: &[Record<T>],
    ) -> bool {
        env.random_vnode_count()
            && records.iter().all(|record| {
                if let Record::Statement { sql, .. } | Record::Query { sql, .. } = record
                    && sql.to_lowercase().contains("parallelism")
                {
                    println!("[RANDOM VNODE COUNT] skip: {}", sql);
                    false
                } else {
                    true
                }
            })
    }

    /// `kill` is totally disabled.
    pub(super) async fn run_no_kill<D, M, T>(
        tester: &mut sqllogictest::Runner<D, M>,
        random_vnode_count: bool,
        cmd: &SqlCmd,
        record: Record<T>,
    ) -> Result<(), TestError>
    where
        D: sqllogictest::AsyncDB<ColumnType = T>,
        M: sqllogictest::MakeConnection<Conn = D>,
        T: sqllogictest::ColumnType,
    {
        // Set random vnode count if needed.
        if random_vnode_count
            && cmd.is_create()
            && let Record::Statement {
                loc,
                conditions,
                connection,
                ..
            } = &record
        {
            let vnode_count = (2..=64) // small
                .chain(224..=288) // normal
                .chain(992..=1056) // 1024 affects row id gen behavior
                .choose(&mut thread_rng())
                .unwrap();
            let sql = format!("SET STREAMING_MAX_PARALLELISM = {vnode_count};");
            println!("[RANDOM VNODE COUNT] set: {vnode_count}");
            let set_random_vnode_count = Record::Statement {
                loc: loc.clone(),
                conditions: conditions.clone(),
                connection: connection.clone(),
                sql,
                expected: StatementExpect::Ok,
                retry: None,
            };
            tester.run_async(set_random_vnode_count).await.unwrap();
            println!("[RANDOM VNODE COUNT] run: {record}");
        }

        tester.run_async(record).await.map(|_| ())
    }

    /// Used when `kill` is not allowed for some specific commands
    pub(super) async fn run_kill_not_allowed<D, M, T>(
        tester: &mut sqllogictest::Runner<D, M>,
        record: Record<T>,
    ) where
        D: sqllogictest::AsyncDB<ColumnType = T>,
        M: sqllogictest::MakeConnection<Conn = D>,
        T: sqllogictest::ColumnType,
    {
        for i in 0usize.. {
            let delay = Duration::from_secs(1 << i);
            if let Err(err) = tester
                .run_async(record.clone())
                .timed(|_res, elapsed| {
                    tracing::debug!("Record {:?} finished in {:?}", record, elapsed)
                })
                .await
            {
                let err_string = err.to_string().to_ascii_lowercase();
                // cluster could be still under recovering if killed before, retry if
                // meets `no reader for dml in table with id {}`.
                let allowed_errs = [
                    "no reader for dml in table",
                    "error reading a body from connection: broken pipe",
                    "failed to inject barrier",
                    "get error from control stream",
                    "cluster is under recovering",
                    "streaming vnode mapping has not been initialized",
                    "streaming vnode mapping not found",
                ];
                let should_retry = i < MAX_RETRY
                    && allowed_errs
                        .iter()
                        .any(|allowed_err| err_string.contains(&allowed_err.to_ascii_lowercase()));
                if !should_retry {
                    panic!("{}", err);
                }
                tracing::error!("failed to run test: {err}\nretry after {delay:?}");
            } else {
                break;
            }
            tokio::time::sleep(delay).await;
        }
    }
}

/// Run the sqllogictest files in `glob`.
pub async fn run_slt_task(cluster: Arc<Cluster>, glob: &str, opts: Opts) {
    let env = slt_env::Env::new(opts);
    tracing::info!("background_ddl_rate: {}", env.background_ddl_rate());
    let mut rng = Env::get_rng();
    let files = glob::glob(glob).expect("failed to read glob pattern");
    for file in files {
        // use a session per file
        let mut tester =
            sqllogictest::Runner::new(|| RisingWave::connect("frontend".into(), "dev".into()));
        tester.add_label("madsim");
        tester.set_var(well_known::DATABASE.to_owned(), "dev".to_owned());

        let file = file.unwrap();
        let path = file.as_path();

        evaluate_skip!(env, path);

        // XXX: hack for kafka source test
        let tempfile = (path.ends_with("kafka.slt") || path.ends_with("kafka_batch.slt"))
            .then(|| hack_kafka_test(path));
        let path = tempfile.as_ref().map(|p| p.path()).unwrap_or(path);

        // NOTE(kwannoel): For background ddl
        let mut background_ddl_enabled = false;

        // If background ddl is set to true within the test case, prevent random setting of background_ddl to true.
        // We can revert it back to false only if we encounter a record that sets background_ddl to false.
        let mut manual_background_ddl_enabled = false;

        let records = sqllogictest::parse_file(path).expect("failed to parse file");
        let random_vnode_count = random_vnode_count(&env, &records);

        for record in records {
            // uncomment to print metrics for task counts
            // let metrics = madsim::runtime::Handle::current().metrics();
            // println!("{:#?}", metrics);
            // println!("{}", metrics.num_tasks_by_node_by_spawn());
            if let sqllogictest::Record::Halt { .. } = record {
                break;
            }

            let cmd = match &record {
                sqllogictest::Record::Statement {
                    sql, conditions, ..
                }
                | sqllogictest::Record::Query {
                    sql, conditions, ..
                } if conditions
                    .iter()
                    .all(|c| !matches!(c, Condition::SkipIf{ label } if label == "madsim"))
                    && !conditions
                        .iter()
                        .any(|c| matches!(c, Condition::OnlyIf{ label} if label != "madsim" )) =>
                {
                    extract_sql_command(sql).unwrap_or(SqlCmd::Others)
                }
                _ => SqlCmd::Others,
            };

            // For normal records.
            if !env.kill() {
                match run_no_kill(&mut tester, random_vnode_count, &cmd, record).await {
                    Ok(_) => continue,
                    Err(e) => panic!("{}", e),
                }
            }

            // For kill enabled.
            tracing::debug!(?cmd, "Running");

            if let SqlCmd::SetBackgroundDdl { enable } = cmd {
                manual_background_ddl_enabled = enable;
                background_ddl_enabled = enable;
            }

            // For each background ddl compatible statement, provide a chance for background_ddl=true.
            set_background_ddl(
                &mut tester,
                &env,
                &record,
                &cmd,
                manual_background_ddl_enabled,
                &mut rng,
                &mut background_ddl_enabled,
            )
            .await;

            if !cmd.allow_kill() {
                run_kill_not_allowed(&mut tester, record.clone()).await;
                continue;
            }

            let should_kill = thread_rng().random_bool(env.kill_rate());
            // spawn a background task to kill nodes
            let handle = if should_kill {
                let cluster = cluster.clone();
                let opts = env.kill_opts();
                Some(tokio::spawn(async move {
                    let t = thread_rng().random_range(Duration::default()..Duration::from_secs(1));
                    tokio::time::sleep(t).await;
                    cluster.kill_node(&opts).await;
                    tokio::time::sleep(Duration::from_secs(15)).await;
                }))
            } else {
                None
            };

            for i in 0usize.. {
                tracing::debug!(iteration = i, "retry count");
                let delay = Duration::from_secs(min(1 << i, 10));
                if i > 0 {
                    tokio::time::sleep(delay).await;
                }
                match tester
                    .run_async(record.clone())
                    .timed(|_res, elapsed| {
                        tracing::debug!("Record {:?} finished in {:?}", record, elapsed)
                    })
                    .await
                {
                    Ok(_) => {
                        // For background ddl
                        if let SqlCmd::CreateMaterializedView { ref name } = cmd
                            && background_ddl_enabled
                            && !matches!(
                                record,
                                Record::Statement {
                                    expected: StatementExpect::Error(_),
                                    ..
                                } | Record::Query {
                                    expected: QueryExpect::Error(_),
                                    ..
                                }
                            )
                        {
                            wait_or_retry_background_ddl!(&record, name, i);
                        }
                        break;
                    }
                    Err(e) => {
                        match cmd {
                            // allow 'table exists' error when retry CREATE statement
                            SqlCmd::Create {
                                is_create_table_as: false,
                            }
                            | SqlCmd::CreateMaterializedView { .. }
                                if i != 0
                                    && let e = e.to_string()
                                    // It should not be a gRPC request to meta error,
                                    // otherwise it means that the catalog is not yet populated to fe.
                                    && !e.contains("gRPC request to meta service failed")
                                    && e.contains("exists")
                                    && !e.contains("still being created")
                                    && e.contains("Catalog error") =>
                            {
                                break;
                            }
                            // allow 'not found' error when retry DROP statement
                            SqlCmd::Drop
                                if i != 0
                                    && e.to_string().contains("not found")
                                    && e.to_string().contains("Catalog error") =>
                            {
                                break;
                            }

                            // Keep i >= MAX_RETRY for other errors. Since these errors indicate that the MV might not yet be created.
                            _ if i >= MAX_RETRY => {
                                panic!("failed to run test after retry {i} times: {e}")
                            }
                            SqlCmd::CreateMaterializedView { ref name }
                                if i != 0
                                    && e.to_string().contains("table is in creating procedure")
                                    && background_ddl_enabled =>
                            {
                                wait_or_retry_background_ddl!(&record, name, i);
                            }
                            _ => tracing::error!(
                                iteration = i,
                                "failed to run test: {e}\nretry after {delay:?}"
                            ),
                        }
                    }
                }
            }
            if let SqlCmd::SetBackgroundDdl { enable } = cmd {
                background_ddl_enabled = enable;
            };
            if let Some(handle) = handle {
                handle.await.unwrap();
            }
        }
    }
}

pub async fn run_parallel_slt_task(glob: &str, jobs: usize) -> Result<(), ParallelTestError> {
    let mut tester =
        sqllogictest::Runner::new(|| RisingWave::connect("frontend".into(), "dev".into()));
    tester.add_label("madsim");

    if let Some(partitioner) = PARTITIONER.as_ref() {
        tester.with_partitioner(partitioner.clone());
    }

    tester
        .run_parallel_async(
            glob,
            vec!["frontend".into()],
            |host, dbname| async move { RisingWave::connect(host, dbname).await.unwrap() },
            jobs,
        )
        .await
        .map_err(|e| panic!("{e}"))
}

/// Replace some strings in kafka.slt and write to a new temp file.
fn hack_kafka_test(path: &Path) -> tempfile::NamedTempFile {
    let content = std::fs::read_to_string(path).expect("failed to read file");
    let simple_avsc_full_path =
        std::fs::canonicalize("src/connector/src/test_data/simple-schema.avsc")
            .expect("failed to get schema path");
    let complex_avsc_full_path =
        std::fs::canonicalize("src/connector/src/test_data/complex-schema.avsc")
            .expect("failed to get schema path");
    let json_schema_full_path =
        std::fs::canonicalize("src/connector/src/test_data/complex-schema.json")
            .expect("failed to get schema path");
    let content = content
        .replace("127.0.0.1:29092", "192.168.11.1:29092")
        .replace("localhost:29092", "192.168.11.1:29092")
        .replace(
            "/risingwave/avro-simple-schema.avsc",
            simple_avsc_full_path.to_str().unwrap(),
        )
        .replace(
            "/risingwave/avro-complex-schema.avsc",
            complex_avsc_full_path.to_str().unwrap(),
        )
        .replace(
            "/risingwave/json-complex-schema",
            json_schema_full_path.to_str().unwrap(),
        );
    let file = tempfile::NamedTempFile::new().expect("failed to create temp file");
    std::fs::write(file.path(), content).expect("failed to write file");
    println!("created a temp file for kafka test: {:?}", file.path());
    file
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use expect_test::{Expect, expect};

    use super::*;

    fn check(actual: impl Debug, expect: Expect) {
        let actual = format!("{:#?}", actual);
        expect.assert_eq(&actual);
    }

    #[test]
    fn test_extract_sql_command() {
        check(
            extract_sql_command("create  table  t as select 1;"),
            expect![[r#"
                Ok(
                    Create {
                        is_create_table_as: true,
                    },
                )"#]],
        );
        check(
            extract_sql_command("  create table  t (a int);"),
            expect![[r#"
                Ok(
                    Create {
                        is_create_table_as: false,
                    },
                )"#]],
        );
        check(
            extract_sql_command(" create materialized   view  m_1 as select 1;"),
            expect![[r#"
                Ok(
                    CreateMaterializedView {
                        name: "m_1",
                    },
                )"#]],
        );
        check(
            extract_sql_command("set background_ddl= true;"),
            expect![[r#"
                Ok(
                    SetBackgroundDdl {
                        enable: true,
                    },
                )"#]],
        );
        check(
            extract_sql_command("SET BACKGROUND_DDL=true;"),
            expect![[r#"
                Ok(
                    SetBackgroundDdl {
                        enable: true,
                    },
                )"#]],
        );
        check(
            extract_sql_command("CREATE MATERIALIZED VIEW if not exists m_1 as select 1;"),
            expect![[r#"
                Ok(
                    CreateMaterializedView {
                        name: "m_1",
                    },
                )"#]],
        )
    }
}
