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

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, anyhow};
use serde_json::json;
use sqlx::ConnectOptions;
use thiserror_ext::AsReport;

use super::{ExecuteContext, Task};
use crate::LakekeeperConfig;
use crate::util::stylized_risedev_subcmd;

pub struct LakekeeperService {
    config: LakekeeperConfig,
}

impl LakekeeperService {
    pub fn new(config: LakekeeperConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn lakekeeper_path(&self) -> Result<PathBuf> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Path::new(&prefix_bin).join("lakekeeper"))
    }

    fn lakekeeper(&self) -> Result<Command> {
        Ok(Command::new(self.lakekeeper_path()?))
    }

    fn base_uri(&self) -> String {
        format!("http://{}:{}", self.config.address, self.config.port)
    }

    /// Apply command args according to config
    pub fn apply_command_args(cmd: &mut Command, config: &LakekeeperConfig) -> Result<()> {
        // Set basic environment variables
        let base_uri = format!("http://{}:{}", config.address, config.port);
        cmd.env("LAKEKEEPER__BASE_URI", &base_uri)
            .env("LAKEKEEPER__LISTEN_PORT", config.port.to_string())
            .env("LAKEKEEPER__PG_ENCRYPTION_KEY", &config.encryption_key);

        // Configure database backend
        if let Some(postgres_configs) = &config.provide_postgres_backend
            && let Some(pg_config) = postgres_configs.first()
        {
            let database_url = format!(
                "postgres://{}:{}@{}:{}/{}",
                pg_config.user, pg_config.password, pg_config.address, pg_config.port, "lakekeeper"
            );
            cmd.env("LAKEKEEPER__PG_DATABASE_URL_READ", &database_url)
                .env("LAKEKEEPER__PG_DATABASE_URL_WRITE", &database_url);
        }

        Ok(())
    }

    fn initialize_lakekeeper_database(
        &self,
        ctx: &mut ExecuteContext<impl std::io::Write>,
    ) -> Result<()> {
        if let Some(postgres_configs) = &self.config.provide_postgres_backend
            && let Some(pg_config) = postgres_configs.first()
        {
            // Wait for PostgreSQL to be ready first
            ctx.pb.set_message("waiting for PostgreSQL to be ready...");
            let mut tcp_check = crate::TcpReadyCheckTask::new(
                pg_config.address.clone(),
                pg_config.port,
                pg_config.user_managed,
            )?;
            tcp_check.execute(ctx)?;

            // Give PostgreSQL a bit more time to fully initialize after TCP is ready
            std::thread::sleep(std::time::Duration::from_secs(2));

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            let db_name = "lakekeeper";
            let host = pg_config.address.clone();
            let port = pg_config.port;
            let username = pg_config.user.clone();
            let password = pg_config.password.clone();

            rt.block_on(async move {
                use sqlx::postgres::*;
                use tokio::time::{sleep, Duration};
                let options = PgConnectOptions::new_without_pgpass()
                    .host(&host)
                    .port(port)
                    .username(&username)
                    .password(&password)
                    .database("template1")
                    .ssl_mode(sqlx::postgres::PgSslMode::Prefer);

                // Retry connection with exponential backoff
                let mut attempts = 0;
                let max_attempts = 5;
                let mut conn = loop {
                    match options.connect().await {
                        Ok(conn) => break conn,
                        Err(_) if attempts < max_attempts => {
                            attempts += 1;
                            let delay = Duration::from_millis(500 * (1 << attempts)); // 1s, 2s, 4s, 8s, 16s
                            sleep(delay).await;
                        }
                        Err(e) => {
                            return Err(e).context("failed to connect to template database for lakekeeper after retries")?;
                        }
                    }
                };

                // Check if database exists before creating it
                let db_exists_query = format!(
                    "SELECT 1 FROM pg_database WHERE datname = '{}';",
                    db_name
                );
                let db_exists = match sqlx::raw_sql(&db_exists_query)
                    .fetch_one(&mut conn)
                    .await
                {
                    Ok(_) => true,
                    Err(sqlx::Error::RowNotFound) => false,
                    Err(e) => return Err(e.into()),
                };

                if !db_exists {
                    // Create database only if it doesn't exist
                    // Intentionally not executing in a transaction because Postgres does not allow it.
                    sqlx::raw_sql(&format!("CREATE DATABASE {};", db_name))
                        .execute(&mut conn)
                        .await?;
                } else {
                    // Database already exists, skipping creation
                }

                Ok::<_, anyhow::Error>(())
            })
            .context("failed to initialize lakekeeper database")?;
        }

        Ok(())
    }

    fn run_migrate(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> Result<()> {
        ctx.pb.set_message("running database migration...");
        let mut cmd = self.lakekeeper()?;
        Self::apply_command_args(&mut cmd, &self.config)?;
        cmd.arg("migrate");
        ctx.run_command(cmd)?;
        Ok(())
    }

    fn bootstrap_lakekeeper(&self, ctx: &mut ExecuteContext<impl std::io::Write>) -> Result<()> {
        ctx.pb.set_message("bootstrapping lakekeeper...");

        // Wait for lakekeeper service to be ready
        std::thread::sleep(std::time::Duration::from_secs(3));

        let bootstrap_config = json!({
            "accept-terms-of-use": true,
            "is-operator": true
        });

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let bootstrap_json = bootstrap_config.to_string();
        let lakekeeper_url = format!("{}/management/v1/bootstrap", self.base_uri());

        rt.block_on(async move {
            use tokio::time::{sleep, Duration};

            // Retry bootstrap with exponential backoff
            let mut attempts = 0;
            let max_attempts = 5;

            loop {
                let client = reqwest::Client::new();
                match client
                    .post(&lakekeeper_url)
                    .header("Content-Type", "application/json")
                    .body(bootstrap_json.clone())
                    .send()
                    .await
                {
                    Ok(response) => {
                        if response.status().is_success() {
                            let _response_text = response.text().await.unwrap_or_default();
                            // Successfully bootstrapped lakekeeper
                            break;
                        } else if response.status() == reqwest::StatusCode::CONFLICT || response.status() == reqwest::StatusCode::BAD_REQUEST {
                            let error_text = response.text().await.unwrap_or_default();
                            if error_text.contains("already bootstrapped") || error_text.contains("Server already initialized") {
                                // Lakekeeper already bootstrapped, skipping
                                break;
                            } else {
                                eprintln!("Bootstrap conflict: {}", error_text);
                                break;
                            }
                        } else {
                            let status = response.status();
                            let error_text = response.text().await.unwrap_or_default();
                            eprintln!("Failed to bootstrap lakekeeper ({}): {}", status, error_text);

                            if attempts >= max_attempts {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        if attempts >= max_attempts {
                            eprintln!("Failed to bootstrap lakekeeper after {} attempts: {}", max_attempts + 1, e.as_report());
                            break;
                        }

                        attempts += 1;
                        let delay = Duration::from_millis(1000 * (1 << attempts)); // 2s, 4s, 8s, 16s, 32s
                        eprintln!("Failed to connect to lakekeeper for bootstrap, retrying in {}s... (attempt {}/{})",
                               delay.as_secs(), attempts, max_attempts + 1);
                        sleep(delay).await;
                    }
                }
            }

            Ok::<_, anyhow::Error>(())
        }).context("failed to bootstrap lakekeeper")?;

        Ok(())
    }

    fn create_default_warehouse(
        &self,
        ctx: &mut ExecuteContext<impl std::io::Write>,
    ) -> Result<()> {
        ctx.pb.set_message("creating default warehouse...");

        // Brief wait after bootstrap
        std::thread::sleep(std::time::Duration::from_secs(1));

        let warehouse_config = if let Some(minio_configs) = &self.config.provide_minio
            && let Some(minio_config) = minio_configs.first()
        {
            json!({
                "warehouse-name": "risingwave-warehouse",
                "storage-profile": {
                    "type": "s3",
                    "bucket": "hummock001",
                    "key-prefix": "risingwave-lakekeeper",
                    "region": "us-east-1",
                    "sts-enabled": false,
                    "flavor": "s3-compat",
                    "endpoint": format!("http://{}:{}/", minio_config.address, minio_config.port),
                    "path-style-access": true
                },
                "storage-credential": {
                    "type": "s3",
                    "credential-type": "access-key",
                    "aws-access-key-id": minio_config.root_user,
                    "aws-secret-access-key": minio_config.root_password
                }
            })
        } else {
            // Default S3 configuration if no MinIO is configured
            json!({
                "warehouse-name": "risingwave-warehouse",
                "storage-profile": {
                    "type": "s3",
                    "bucket": "hummock001",
                    "key-prefix": "risingwave-lakekeeper",
                    "region": "us-east-1",
                    "sts-enabled": false,
                    "flavor": "s3-compat",
                    "endpoint": "http://127.0.0.1:9301/",
                    "path-style-access": true
                },
                "storage-credential": {
                    "type": "s3",
                    "credential-type": "access-key",
                    "aws-access-key-id": "hummockadmin",
                    "aws-secret-access-key": "hummockadmin"
                }
            })
        };

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let warehouse_json = warehouse_config.to_string();
        let lakekeeper_url = format!("{}/management/v1/warehouse", self.base_uri());

        rt.block_on(async move {
            use tokio::time::{Duration, sleep};

            // Retry warehouse creation with exponential backoff
            let mut attempts = 0;
            let max_attempts = 5;

            loop {
                let client = reqwest::Client::new();
                match client
                    .post(&lakekeeper_url)
                    .header("Content-Type", "application/json")
                    .body(warehouse_json.clone())
                    .send()
                    .await
                {
                    Ok(response) => {
                        if response.status().is_success() {
                            let _response_text = response.text().await.unwrap_or_default();
                            // Successfully created warehouse
                            break;
                        } else if response.status() == reqwest::StatusCode::BAD_REQUEST {
                            let error_text = response.text().await.unwrap_or_default();
                            if error_text.contains("Storage profile overlaps") {
                                // Warehouse already exists, skipping creation
                                break;
                            } else {
                                eprintln!("Failed to create warehouse: {}", error_text);
                                break;
                            }
                        } else {
                            let status = response.status();
                            let error_text = response.text().await.unwrap_or_default();
                            eprintln!("Failed to create warehouse ({}): {}", status, error_text);

                            if attempts >= max_attempts {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        if attempts >= max_attempts {
                            eprintln!(
                                "Failed to create warehouse after {} attempts: {}",
                                max_attempts + 1,
                                e.as_report()
                            );
                            break;
                        }

                        attempts += 1;
                        let delay = Duration::from_millis(1000 * (1 << attempts)); // 2s, 4s, 8s, 16s, 32s
                        eprintln!(
                            "Failed to connect to lakekeeper, retrying in {}s... (attempt {}/{})",
                            delay.as_secs(),
                            attempts,
                            max_attempts + 1
                        );
                        sleep(delay).await;
                    }
                }
            }

            Ok::<_, anyhow::Error>(())
        })
        .context("failed to create default warehouse")?;

        Ok(())
    }
}

impl Task for LakekeeperService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let path = self.lakekeeper_path()?;
        if !path.exists() {
            return Err(anyhow!(
                "lakekeeper binary not found in {:?}\nDid you enable lakekeeper feature in `{}`?",
                path,
                stylized_risedev_subcmd("configure")
            ));
        }

        // Initialize and migrate database if using postgres backend
        if self.config.provide_postgres_backend.is_some() {
            ctx.pb.set_message("initializing lakekeeper database...");
            self.initialize_lakekeeper_database(ctx)?;
            self.run_migrate(ctx)?;
        }

        let mut cmd = self.lakekeeper()?;
        Self::apply_command_args(&mut cmd, &self.config)?;
        cmd.arg("serve");

        let prefix_config = env::var("PREFIX_CONFIG")?;
        let data_path = Path::new(&env::var("PREFIX_DATA")?).join(self.id());
        fs_err::create_dir_all(&data_path)?;

        // Create config directory
        let config_dir = Path::new(&prefix_config).join("lakekeeper");
        fs_err::create_dir_all(&config_dir)?;

        ctx.run_command(ctx.tmux_run(cmd)?)?;

        ctx.pb.set_message("started");

        // Bootstrap lakekeeper after service starts
        if let Err(e) = self.bootstrap_lakekeeper(ctx) {
            eprintln!("Warning: Failed to bootstrap lakekeeper: {}", e.as_report());
            eprintln!("You can bootstrap it manually later using the lakekeeper API");
        } else {
            // Create default warehouse after successful bootstrap
            if let Err(e) = self.create_default_warehouse(ctx) {
                eprintln!(
                    "Warning: Failed to create default warehouse: {}",
                    e.as_report()
                );
                eprintln!("You can create it manually later using the lakekeeper API");
            }
        }

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
