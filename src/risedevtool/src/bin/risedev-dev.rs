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

#![feature(trait_alias)]

use std::collections::HashMap;
use std::env;
use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use console::style;
use fs_err::OpenOptions;
use indicatif::{MultiProgress, ProgressBar};
use risedev::util::{begin_spin, complete_spin, fail_spin};
use risedev::{
    CompactorService, ComputeNodeService, ConfigExpander, ConfigureTmuxTask, DummyService,
    ElasticSearchService, EnsureStopService, ExecuteContext, FrontendService, GrafanaService,
    KafkaService, LakekeeperService, MetaNodeService, MinioService, MoatService, MongoDbService,
    MongoDbSetupTask, MotoService, MqttService, MySqlService, NatsService, OpenSearchService,
    PostgresService, PrometheusService, PubsubService, PulsarService, RISEDEV_NAME, RedisService,
    SchemaRegistryService, ServiceConfig, SqlServerService, SqliteConfig, StarrocksReadyCheckTask,
    StarrocksService, Task, TaskGroup, TempoService, generate_risedev_env, preflight_check,
};
use sqlx::mysql::MySqlConnectOptions;
use sqlx::postgres::PgConnectOptions;
use tempfile::tempdir;
use thiserror_ext::AsReport;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use yaml_rust::YamlEmitter;

pub struct ProgressManager {
    pa: MultiProgress,
}

impl ProgressManager {
    pub fn new() -> Self {
        let pa = MultiProgress::default();
        pa.set_move_cursor(true);
        Self { pa }
    }

    /// Create a new progress bar from task
    pub fn new_progress(&mut self) -> ProgressBar {
        let pb = risedev::util::new_spinner().with_finish(indicatif::ProgressFinish::AndLeave);
        pb.enable_steady_tick(Duration::from_millis(100));
        self.pa.add(pb)
    }
}

fn task_main(
    manager: &mut ProgressManager,
    services: &Vec<ServiceConfig>,
    env: Vec<String>,
) -> Result<(Vec<(String, Duration)>, String)> {
    let log_path = env::var("PREFIX_LOG")?;

    let mut logger = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(Path::new(&log_path).join("risedev.log"))?;

    let status_dir = Arc::new(tempdir()?);

    let mut log_buffer = String::new();

    // Start Tmux and kill previous services
    {
        let mut ctx = ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
        let mut service = ConfigureTmuxTask::new(env)?;
        service.execute(&mut ctx)?;

        writeln!(
            log_buffer,
            "* Run {} to attach to the tmux console.",
            style(format!("tmux -L {RISEDEV_NAME} a -t {RISEDEV_NAME}"))
                .blue()
                .bold()
        )?;
    }

    // Firstly, ensure that all ports needed is not occupied by previous runs.
    let mut ports = vec![];

    for service in services {
        if let Some(port) = service.port() {
            ports.push((port, service.id().to_owned(), service.user_managed()));
        }
    }

    {
        let mut ctx = ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
        let mut service = EnsureStopService::new(ports)?;
        service.execute(&mut ctx)?;
    }

    // Then, start services one by one

    let mut tasks = TaskScheduler::new();

    for service in services {
        if let ServiceConfig::Frontend(c) = service {
            writeln!(
                log_buffer,
                "* Run {} to start Postgres interactive shell.",
                style(format_args!(
                    "psql -h localhost -p {} -d dev -U root",
                    c.port
                ))
                .blue()
                .bold()
            )?;
        }
        let service_ = service.clone();
        let progress_bar = manager.new_progress();
        progress_bar.set_prefix(service.id().to_owned());
        progress_bar.set_message("waiting for previous service to start...".to_owned());
        let status_dir = status_dir.clone();
        let closure = move || {
            let mut log = Vec::new();
            let start_time = Instant::now();
            let mut ctx = ExecuteContext::new(&mut log, progress_bar, status_dir);
            let service = service_;
            let id = service.id().to_owned();
            match service {
                ServiceConfig::Minio(c) => {
                    let mut service = MinioService::new(c.clone())?;
                    service.execute(&mut ctx)?;

                    let mut task = risedev::ConfigureMinioTask::new(c)?;
                    task.execute(&mut ctx)?;
                }
                ServiceConfig::Sqlite(c) => {
                    struct SqliteService(SqliteConfig);
                    impl Task for SqliteService {
                        fn execute(
                            &mut self,
                            _ctx: &mut ExecuteContext<impl std::io::Write>,
                        ) -> anyhow::Result<()> {
                            Ok(())
                        }

                        fn id(&self) -> String {
                            self.0.id.clone()
                        }
                    }

                    let prefix_data = env::var("PREFIX_DATA")?;
                    let file_dir = PathBuf::from(&prefix_data).join(&c.id);
                    std::fs::create_dir_all(&file_dir)?;
                    let file_path = file_dir.join(&c.file);

                    ctx.service(&SqliteService(c));
                    ctx.complete_spin();
                    ctx.pb
                        .set_message(format!("using local sqlite: {:?}", file_path));
                }
                ServiceConfig::Prometheus(c) => {
                    let mut service = PrometheusService::new(c.clone())?;
                    service.execute(&mut ctx)?;
                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, false)?;
                    task.execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("api http://{}:{}/", c.address, c.port));
                }
                ServiceConfig::ComputeNode(c) => {
                    let mut service = ComputeNodeService::new(c.clone())?;
                    service.execute(&mut ctx)?;

                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                    task.execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("api grpc://{}:{}/", c.address, c.port));
                }
                ServiceConfig::MetaNode(c) => {
                    let mut service = MetaNodeService::new(c.clone())?;
                    service.execute(&mut ctx)?;
                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                    task.execute(&mut ctx)?;
                    ctx.pb.set_message(format!(
                        "api grpc://{}:{}/, dashboard http://{}:{}/",
                        c.address, c.port, c.address, c.dashboard_port
                    ));
                }
                ServiceConfig::Frontend(c) => {
                    let mut service = FrontendService::new(c.clone())?;
                    service.execute(&mut ctx)?;
                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                    task.execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("api postgres://{}:{}/", c.address, c.port));
                }
                ServiceConfig::Compactor(c) => {
                    let mut service = CompactorService::new(c.clone())?;
                    service.execute(&mut ctx)?;
                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                    task.execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("compactor {}:{}", c.address, c.port));
                }
                ServiceConfig::Grafana(c) => {
                    let mut service = GrafanaService::new(c.clone())?;
                    service.execute(&mut ctx)?;
                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, false)?;
                    task.execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("dashboard http://{}:{}/", c.address, c.port));
                }
                ServiceConfig::Tempo(c) => {
                    let mut service = TempoService::new(c.clone())?;
                    service.execute(&mut ctx)?;
                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.listen_address.clone(), c.port, false)?;
                    task.execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("api http://{}:{}/", c.listen_address, c.port));
                }
                ServiceConfig::AwsS3(c) => {
                    DummyService::new(&c.id).execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("using AWS s3 bucket {}", c.bucket));
                }
                ServiceConfig::Opendal(c) => {
                    DummyService::new(&c.id).execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("using Opendal, namenode =  {}", c.namenode));
                }
                ServiceConfig::Moto(c) => {
                    MotoService::new(c.clone()).execute(&mut ctx)?;
                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                    task.execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("moto http://{}:{}/", c.address, c.port));
                }
                ServiceConfig::Kafka(c) => {
                    let mut service = KafkaService::new(c.clone());
                    service.execute(&mut ctx)?;
                    let mut task = risedev::KafkaReadyCheckTask::new(c.clone())?;
                    task.execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("kafka {}:{}", c.address, c.port));
                }
                ServiceConfig::SchemaRegistry(c) => {
                    let mut service = SchemaRegistryService::new(c.clone());
                    service.execute(&mut ctx)?;
                    if c.user_managed {
                        let mut task = risedev::TcpReadyCheckTask::new(
                            c.address.clone(),
                            c.port,
                            c.user_managed,
                        )?;
                        task.execute(&mut ctx)?;
                    } else {
                        let mut task = risedev::LogReadyCheckTask::new(
                            "Server started, listening for requests",
                        )?;
                        task.execute(&mut ctx)?;
                    }
                    ctx.pb
                        .set_message(format!("schema registry http://{}:{}", c.address, c.port));
                }

                ServiceConfig::Pubsub(c) => {
                    let mut service = PubsubService::new(c.clone())?;
                    service.execute(&mut ctx)?;
                    let mut task = risedev::PubsubReadyTaskCheck::new(c.clone())?;
                    task.execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("pubsub {}:{}", c.address, c.port));
                }
                ServiceConfig::Pulsar(c) => {
                    PulsarService::new(c.clone()).execute(&mut ctx)?;
                    let mut task = risedev::TcpReadyCheckTask::new(
                        c.address.clone(),
                        c.broker_port,
                        c.user_managed,
                    )?;
                    task.execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("pulsar {}:{}", c.address, c.broker_port));
                }
                ServiceConfig::Redis(c) => {
                    let mut service = RedisService::new(c.clone());
                    service.execute(&mut ctx)?;
                    let mut task = risedev::RedisReadyCheckTask::new(c.clone())?;
                    task.execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("redis {}:{}", c.address, c.port));
                }
                ServiceConfig::Starrocks(c) => {
                    let mut service = StarrocksService::new(c.clone());
                    service.execute(&mut ctx)?;
                    let mut task = StarrocksReadyCheckTask::new(c.clone());
                    task.execute(&mut ctx)?;
                    ctx.pb.set_message(format!(
                        "starrocks http://{}:{}, query {}",
                        c.address, c.http_port, c.query_port
                    ));
                }
                ServiceConfig::MySql(c) => {
                    MySqlService::new(c.clone()).execute(&mut ctx)?;
                    let mut task = risedev::DbReadyCheckTask::new(
                        MySqlConnectOptions::new()
                            .host(&c.address)
                            .port(c.port)
                            .username(&c.user)
                            .password(&c.password),
                    );
                    task.execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("mysql {}:{}", c.address, c.port));
                }
                ServiceConfig::Postgres(c) => {
                    PostgresService::new(c.clone()).execute(&mut ctx)?;
                    let mut task = risedev::DbReadyCheckTask::new(
                        PgConnectOptions::new_without_pgpass()
                            .host(&c.address)
                            .port(c.port)
                            .database("template1")
                            .username(&c.user)
                            .password(&c.password),
                    );
                    task.execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("postgres {}:{}", c.address, c.port));
                }
                ServiceConfig::SqlServer(c) => {
                    // only `c.password` will be used in `SqlServerService` as the password for user `sa`.
                    SqlServerService::new(c.clone()).execute(&mut ctx)?;
                    if c.user_managed {
                        let mut task = risedev::TcpReadyCheckTask::new(
                            c.address.clone(),
                            c.port,
                            c.user_managed,
                        )?;
                        task.execute(&mut ctx)?;
                    } else {
                        let mut task = risedev::LogReadyCheckTask::new(
                            "SQL Server is now ready for client connections.",
                        )?;
                        task.execute(&mut ctx)?;
                    }
                    ctx.pb
                        .set_message(format!("sqlserver {}:{}", c.address, c.port));
                }
                ServiceConfig::MongoDb(c) => {
                    MongoDbService::new(c.clone()).execute(&mut ctx)?;
                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                    task.execute(&mut ctx)?;
                    MongoDbSetupTask::new(c.clone()).execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("mongodb {}:{}", c.address, c.port));
                }
                ServiceConfig::ElasticSearch(c) => {
                    ElasticSearchService::new(c.clone()).execute(&mut ctx)?;
                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                    task.execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("elasticsearch http://{}:{}", c.address, c.port));
                }
                ServiceConfig::OpenSearch(c) => {
                    OpenSearchService::new(c.clone()).execute(&mut ctx)?;
                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                    task.execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("opensearch http://{}:{}", c.address, c.port));
                }
                ServiceConfig::Nats(c) => {
                    NatsService::new(c.clone()).execute(&mut ctx)?;
                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                    task.execute(&mut ctx)?;
                    ctx.pb.set_message(format!(
                        "nats {}:{}, monitor {}",
                        c.address, c.port, c.monitor_port
                    ));
                }
                ServiceConfig::Mqtt(c) => {
                    MqttService::new(c.clone()).execute(&mut ctx)?;
                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                    task.execute(&mut ctx)?;
                    ctx.pb.set_message(format!("mqtt {}:{}", c.address, c.port));
                }
                ServiceConfig::Lakekeeper(c) => {
                    let mut service = LakekeeperService::new(c.clone())?;
                    service.execute(&mut ctx)?;
                    if c.user_managed {
                        let mut task = risedev::TcpReadyCheckTask::new(
                            c.address.clone(),
                            c.port,
                            c.user_managed,
                        )?;
                        task.execute(&mut ctx)?;
                    } else {
                        let mut task =
                            risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, false)?;
                        task.execute(&mut ctx)?;
                    }
                    ctx.pb
                        .set_message(format!("lakekeeper http://{}:{}/", c.address, c.port));
                }
                ServiceConfig::Moat(c) => {
                    let mut service = MoatService::new(c.clone())?;
                    service.execute(&mut ctx)?;

                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, false)?;
                    task.execute(&mut ctx)?;
                    ctx.pb
                        .set_message(format!("moat http://{}:{}/", c.address, c.port));
                }
            }

            let duration = Instant::now() - start_time;
            Ok(TaskResult {
                id,
                time: duration,
                log: String::from_utf8(log)?,
            })
        };
        tasks.add(service, closure);
    }

    let stat = tasks.run(&mut logger)?;

    Ok((stat, log_buffer))
}

#[derive(Debug)]
struct TaskResult {
    id: String,
    time: Duration,
    log: String,
}
trait TaskFn = FnOnce() -> anyhow::Result<TaskResult> + Send + 'static;
struct TaskScheduler {
    /// In each group, the tasks are executed in sequence.
    task_groups: HashMap<TaskGroup, Vec<Box<dyn TaskFn>>>,
}

impl TaskScheduler {
    fn new() -> Self {
        Self {
            task_groups: HashMap::new(),
        }
    }

    fn add(&mut self, config: &ServiceConfig, task: impl TaskFn) {
        self.task_groups
            .entry(config.task_group())
            .or_default()
            .push(Box::new(task));
    }

    fn run(self, logger: &mut impl std::io::Write) -> anyhow::Result<Vec<(String, Duration)>> {
        let mut handles: Vec<JoinHandle<anyhow::Result<Vec<TaskResult>>>> = vec![];
        let mut stats = vec![];

        let task_groups = self.task_groups;
        for (_, tasks) in task_groups {
            handles.push(std::thread::spawn(move || {
                let mut res = vec![];
                for task in tasks {
                    let res_ = task()?;
                    res.push(res_);
                }
                Ok(res)
            }));
        }
        for handle in handles {
            let join_res = handle.join();
            let Ok(res) = join_res else {
                let panic = join_res.unwrap_err();
                anyhow::bail!(
                    "failed to join thread, likely panicked: {}",
                    panic_message::panic_message(&panic)
                );
            };
            for TaskResult { id, time, log } in res? {
                stats.push((id, time));
                write!(logger, "{}", log)?;
            }
        }
        Ok(stats)
    }
}

fn main() -> Result<()> {
    // Intentionally disable backtrace to provide more compact error message for `risedev dev`.
    // Backtraces for RisingWave components are enabled in `Task::execute`.
    // safety: single-threaded code.
    unsafe { std::env::set_var("RUST_BACKTRACE", "0") };

    // Init logger from a customized env var.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .with_env_var("RISEDEV_RUST_LOG")
                .from_env_lossy()
                // This log may pollute the progress bar.
                .add_directive("librdkafka=off".parse().unwrap()),
        )
        .init();

    preflight_check()?;

    let profile = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "default".to_owned());

    let (config_path, env, risedev_config) = ConfigExpander::expand(".", &profile)?;

    if let Some(config_path) = &config_path {
        let target = Path::new(&env::var("PREFIX_CONFIG")?).join("risingwave.toml");
        fs_err::copy(config_path, target).context("config file not found")?;
    }

    {
        let mut out_str = String::new();
        let mut emitter = YamlEmitter::new(&mut out_str);
        emitter.dump(&risedev_config)?;
        fs_err::write(
            Path::new(&env::var("PREFIX_CONFIG")?).join("risedev-expanded.yml"),
            &out_str,
        )?;
    }
    let services = ConfigExpander::deserialize(&risedev_config)?;

    let mut manager = ProgressManager::new();
    // Always create a progress before calling `task_main`. Otherwise the progress bar won't be
    // shown.
    let p = manager.new_progress();
    begin_spin(&p);
    p.set_prefix("dev cluster");
    p.set_message(format!(
        "starting {} services for {}...",
        services.len(),
        profile
    ));
    let task_result = task_main(&mut manager, &services, env);

    match task_result {
        Ok(_) => {
            p.set_message(format!(
                "done bootstrapping with config {}",
                style(profile).bold()
            ));
            complete_spin(&p);
        }
        Err(_) => {
            p.set_message(format!(
                "failed to bootstrap with config {}",
                style(profile).bold()
            ));
            fail_spin(&p);
        }
    }
    p.finish();

    use risedev::util::stylized_risedev_subcmd as r;

    match task_result {
        Ok((stat, log_buffer)) => {
            println!("---- summary of startup time ----");
            for (task_name, duration) in stat {
                println!("{}: {:.2}s", task_name, duration.as_secs_f64());
            }
            println!("-------------------------------");
            println!();

            fs_err::write(
                Path::new(&env::var("PREFIX_CONFIG")?).join("risedev-env"),
                generate_risedev_env(&services),
            )?;

            println!("All services started successfully.");

            print!("{}", log_buffer);

            println!("* You may find logs using {} command", r("l"));

            println!("* Run {} to kill cluster.", r("k"));

            println!("* Run {} to run `risedev` anywhere!", r("install"));

            Ok(())
        }
        Err(err) => {
            println!(
                "{} - Failed to start: {:#}", // pretty with `Caused by`
                style("ERROR").red().bold(),
                err.as_report(),
            );
            println!();
            println!(
                "* Use `{}` to enable new components, if they are missing.",
                r("configure")
            );
            println!(
                "* Use `{}` to view logs, or visit `{}`",
                r("l"),
                env::var("PREFIX_LOG")?
            );
            println!("* Run `{}` to clean up cluster.", r("k"));
            println!(
                "* Run `{}` to clean data, which might potentially fix the issue.",
                r("clean-data")
            );
            println!("---");
            println!();

            // As we have already printed the error above, we don't need to print that error again.
            // However, to return with a proper exit code, still return an error here.
            Err(anyhow!(
                "Failed to start all services. See details and instructions above."
            ))
        }
    }
}
