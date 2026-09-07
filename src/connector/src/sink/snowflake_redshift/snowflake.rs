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

use core::num::NonZeroU64;
use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::anyhow;
use phf::{Set, phf_set};
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use risingwave_pb::connector_service::{SinkMetadata, sink_metadata};
use risingwave_pb::stream_plan::PbSinkSchemaChange;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::time::{MissedTickBehavior, interval};
use tonic::async_trait;
use with_options::WithOptions;

use crate::connector_common::IcebergSinkCompactionUpdate;
use crate::enforce_secret::EnforceSecret;
use crate::sink::catalog::SinkId;
use crate::sink::coordinate::CoordinatedLogSinker;
use crate::sink::decouple_checkpoint_log_sink::default_commit_checkpoint_interval;
use crate::sink::file_sink::s3::S3Common;
use crate::sink::jdbc_jni_client::{self, JdbcJniClient};
use crate::sink::snowflake_redshift::{
    __OP, __ROW_ID, SnowflakeRedshiftSinkJdbcWriter, SnowflakeRedshiftSinkS3Writer,
};
use crate::sink::writer::SinkWriter;
use crate::sink::{
    Result, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
    SinglePhaseCommitCoordinator, Sink, SinkCommitCoordinator, SinkError, SinkParam,
    SinkWriterParam,
};

pub const SNOWFLAKE_SINK_V2: &str = "snowflake_v2";

const AUTH_METHOD_PASSWORD: &str = "password";
const AUTH_METHOD_KEY_PAIR_FILE: &str = "key_pair_file";
const AUTH_METHOD_KEY_PAIR_OBJECT: &str = "key_pair_object";
const PROP_AUTH_METHOD: &str = "auth.method";

pub fn build_full_table_name(database: &str, schema_name: &str, table_name: &str) -> String {
    format!(r#""{}"."{}"."{}""#, database, schema_name, table_name)
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions)]
pub struct SnowflakeV2Config {
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "intermediate.table.name")]
    pub snowflake_cdc_table_name: Option<String>,

    #[serde(rename = "table.name")]
    pub snowflake_target_table_name: Option<String>,

    #[serde(rename = "database")]
    pub snowflake_database: Option<String>,

    #[serde(rename = "schema")]
    pub snowflake_schema: Option<String>,

    #[serde(default = "default_target_interval_schedule")]
    #[serde(rename = "write.target.interval.seconds")]
    #[serde_as(as = "DisplayFromStr")]
    pub writer_target_interval_seconds: u64,

    #[serde(default = "default_intermediate_interval_schedule")]
    #[serde(rename = "write.intermediate.interval.seconds")]
    #[serde_as(as = "DisplayFromStr")]
    pub write_intermediate_interval_seconds: u64,

    #[serde(rename = "warehouse")]
    pub snowflake_warehouse: Option<String>,

    #[serde(default, rename = "task.serverless")]
    #[serde_as(as = "DisplayFromStr")]
    pub task_serverless: bool,

    #[serde(rename = "task.target_completion_interval")]
    pub task_target_completion_interval: Option<String>,

    #[serde(rename = "jdbc.url")]
    pub jdbc_url: Option<String>,

    #[serde(rename = "username")]
    pub username: Option<String>,

    #[serde(rename = "password")]
    pub password: Option<String>,

    // Authentication method control (password | key_pair_file | key_pair_object)
    #[serde(rename = "auth.method")]
    pub auth_method: Option<String>,

    // Key-pair authentication via connection Properties (Option 2: file-based)
    #[serde(rename = "private_key_file")]
    pub private_key_file: Option<String>,

    #[serde(rename = "private_key_file_pwd")]
    pub private_key_file_pwd: Option<String>,

    // Key-pair authentication via connection Properties (Option 1: object-based, PEM content)
    #[serde(rename = "private_key_pem")]
    pub private_key_pem: Option<String>,

    /// Commit every n(>0) checkpoints, default is 10.
    #[serde(default = "default_commit_checkpoint_interval")]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    pub commit_checkpoint_interval: u64,

    /// Enable auto schema change for upsert sink.
    /// If enabled, the sink will automatically alter the target table to add new columns.
    #[serde(default)]
    #[serde(rename = "auto.schema.change")]
    #[serde_as(as = "DisplayFromStr")]
    pub auto_schema_change: bool,

    #[serde(default)]
    #[serde(rename = "create_table_if_not_exists")]
    #[serde_as(as = "DisplayFromStr")]
    pub create_table_if_not_exists: bool,

    #[serde(default = "default_with_s3")]
    #[serde(rename = "with_s3")]
    #[serde_as(as = "DisplayFromStr")]
    pub with_s3: bool,

    #[serde(flatten)]
    pub s3_inner: Option<S3Common>,

    #[serde(rename = "stage")]
    pub stage: Option<String>,

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(SnowflakeV2Config);

fn default_target_interval_schedule() -> u64 {
    3600 // Default to 1 hour
}

fn default_intermediate_interval_schedule() -> u64 {
    1800 // Default to 0.5 hour
}

fn default_with_s3() -> bool {
    true
}

impl SnowflakeV2Config {
    /// Build JDBC Properties for the Snowflake JDBC connection (no URL parameters).
    /// Returns (`jdbc_url`, `driver_properties`).
    /// - `driver_properties` are transformed/used by the Java runner and passed to `DriverManager::getConnection(url, props)`
    ///
    /// Note: This method assumes the config has been validated by `from_btreemap`.
    pub fn build_jdbc_connection_properties(&self) -> Result<(String, Vec<(String, String)>)> {
        let jdbc_url = self
            .jdbc_url
            .clone()
            .ok_or(SinkError::Config(anyhow!("jdbc.url is required")))?;
        let username = self
            .username
            .clone()
            .ok_or(SinkError::Config(anyhow!("username is required")))?;

        let mut connection_properties: Vec<(String, String)> = vec![("user".to_owned(), username)];

        // auth_method is guaranteed to be Some after validation in from_btreemap
        match self.auth_method.as_deref().unwrap() {
            AUTH_METHOD_PASSWORD => {
                // password is guaranteed to exist by from_btreemap validation
                connection_properties.push(("password".to_owned(), self.password.clone().unwrap()));
            }
            AUTH_METHOD_KEY_PAIR_FILE => {
                // private_key_file is guaranteed to exist by from_btreemap validation
                connection_properties.push((
                    "private_key_file".to_owned(),
                    self.private_key_file.clone().unwrap(),
                ));
                if let Some(pwd) = self.private_key_file_pwd.clone() {
                    connection_properties.push(("private_key_file_pwd".to_owned(), pwd));
                }
            }
            AUTH_METHOD_KEY_PAIR_OBJECT => {
                connection_properties.push((
                    PROP_AUTH_METHOD.to_owned(),
                    AUTH_METHOD_KEY_PAIR_OBJECT.to_owned(),
                ));
                // private_key_pem is guaranteed to exist by from_btreemap validation
                connection_properties.push((
                    "private_key_pem".to_owned(),
                    self.private_key_pem.clone().unwrap(),
                ));
                if let Some(pwd) = self.private_key_file_pwd.clone() {
                    connection_properties.push(("private_key_file_pwd".to_owned(), pwd));
                }
            }
            _ => {
                // This should never happen since from_btreemap validates auth_method
                unreachable!(
                    "Invalid auth_method - should have been caught during config validation"
                )
            }
        }

        Ok((jdbc_url, connection_properties))
    }

    pub fn from_btreemap(properties: &BTreeMap<String, String>) -> Result<Self> {
        let mut config =
            serde_json::from_value::<SnowflakeV2Config>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        let has_upsert_task_config = config.snowflake_cdc_table_name.is_some()
            || properties.contains_key("write.target.interval.seconds")
            || config.snowflake_warehouse.is_some()
            || config.task_serverless
            || config.task_target_completion_interval.is_some();
        if config.r#type != SINK_TYPE_UPSERT && has_upsert_task_config {
            return Err(SinkError::Config(anyhow!(
                "`intermediate.table.name`, `write.target.interval.seconds`, `warehouse`, \
                 `task.serverless`, and `task.target_completion_interval` require `{}` = {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_UPSERT
            )));
        }
        if config.task_target_completion_interval.is_some() && !config.task_serverless {
            return Err(SinkError::Config(anyhow!(
                "`task.target_completion_interval` requires `task.serverless` to be true"
            )));
        }
        if config.task_serverless && config.snowflake_warehouse.is_some() {
            return Err(SinkError::Config(anyhow!(
                "`task.serverless` must not be combined with `warehouse`"
            )));
        }

        // Normalize and validate authentication method
        let has_password = config.password.is_some();
        let has_file = config.private_key_file.is_some();
        let has_pem = config.private_key_pem.as_deref().is_some();

        let normalized_auth_method = match config
            .auth_method
            .as_deref()
            .map(|s| s.trim().to_ascii_lowercase())
        {
            Some(method) if method == AUTH_METHOD_PASSWORD => {
                if !has_password {
                    return Err(SinkError::Config(anyhow!(
                        "auth.method=password requires `password`"
                    )));
                }
                if has_file || has_pem {
                    return Err(SinkError::Config(anyhow!(
                        "auth.method=password must not set `private_key_file`/`private_key_pem`"
                    )));
                }
                AUTH_METHOD_PASSWORD.to_owned()
            }
            Some(method) if method == AUTH_METHOD_KEY_PAIR_FILE => {
                if !has_file {
                    return Err(SinkError::Config(anyhow!(
                        "auth.method=key_pair_file requires `private_key_file`"
                    )));
                }
                if has_password {
                    return Err(SinkError::Config(anyhow!(
                        "auth.method=key_pair_file must not set `password`"
                    )));
                }
                if has_pem {
                    return Err(SinkError::Config(anyhow!(
                        "auth.method=key_pair_file must not set `private_key_pem`"
                    )));
                }
                AUTH_METHOD_KEY_PAIR_FILE.to_owned()
            }
            Some(method) if method == AUTH_METHOD_KEY_PAIR_OBJECT => {
                if !has_pem {
                    return Err(SinkError::Config(anyhow!(
                        "auth.method=key_pair_object requires `private_key_pem`"
                    )));
                }
                if has_password {
                    return Err(SinkError::Config(anyhow!(
                        "auth.method=key_pair_object must not set `password`"
                    )));
                }
                AUTH_METHOD_KEY_PAIR_OBJECT.to_owned()
            }
            Some(other) => {
                return Err(SinkError::Config(anyhow!(
                    "invalid auth.method: {} (allowed: password | key_pair_file | key_pair_object)",
                    other
                )));
            }
            None => {
                // Infer auth method from supplied fields
                match (has_password, has_file, has_pem) {
                    (true, false, false) => AUTH_METHOD_PASSWORD.to_owned(),
                    (false, true, false) => AUTH_METHOD_KEY_PAIR_FILE.to_owned(),
                    (false, false, true) => AUTH_METHOD_KEY_PAIR_OBJECT.to_owned(),
                    (true, true, _) | (true, _, true) | (false, true, true) => {
                        return Err(SinkError::Config(anyhow!(
                            "ambiguous auth: multiple auth options provided; remove one or set `auth.method`"
                        )));
                    }
                    _ => {
                        return Err(SinkError::Config(anyhow!(
                            "no authentication configured: set either `password`, or `private_key_file`, or `private_key_pem` (or provide `auth.method`)"
                        )));
                    }
                }
            }
        };
        config.auth_method = Some(normalized_auth_method);
        Ok(config)
    }

    pub fn build_snowflake_task_ctx_jdbc_client(
        &self,
        is_append_only: bool,
        schema: &Schema,
        pk_indices: &Vec<usize>,
    ) -> Result<Option<(SnowflakeTaskContext, JdbcJniClient)>> {
        if !self.auto_schema_change
            && is_append_only
            && !self.create_table_if_not_exists
            && !self.with_s3
        {
            // append-only + no auto schema change is not need to create a client
            return Ok(None);
        }
        let target_table_name = self
            .snowflake_target_table_name
            .clone()
            .ok_or(SinkError::Config(anyhow!("table.name is required")))?;
        let database = self
            .snowflake_database
            .clone()
            .ok_or(SinkError::Config(anyhow!("database is required")))?;
        let schema_name = self
            .snowflake_schema
            .clone()
            .ok_or(SinkError::Config(anyhow!("schema is required")))?;
        let mut snowflake_task_ctx = SnowflakeTaskContext {
            target_table_name: target_table_name.clone(),
            database,
            schema_name,
            schema: schema.clone(),
            ..Default::default()
        };

        let (jdbc_url, connection_properties) = self.build_jdbc_connection_properties()?;
        let client = JdbcJniClient::new_with_props(jdbc_url, connection_properties)?;

        if self.with_s3 {
            let stage = self
                .stage
                .clone()
                .ok_or(SinkError::Config(anyhow!("stage is required")))?;
            snowflake_task_ctx.stage = Some(stage);
            snowflake_task_ctx.pipe_name = Some(format!("{}_pipe", target_table_name));
        }
        if !is_append_only {
            let cdc_table_name = self
                .snowflake_cdc_table_name
                .clone()
                .ok_or(SinkError::Config(anyhow!(
                    "intermediate.table.name is required"
                )))?;
            snowflake_task_ctx.cdc_table_name = Some(cdc_table_name.clone());
            snowflake_task_ctx.writer_target_interval_seconds = self.writer_target_interval_seconds;
            snowflake_task_ctx.task_serverless = self.task_serverless;
            snowflake_task_ctx.task_target_completion_interval =
                self.task_target_completion_interval.clone();
            if !self.task_serverless {
                snowflake_task_ctx.warehouse = Some(
                    self.snowflake_warehouse
                        .clone()
                        .ok_or(SinkError::Config(anyhow!("warehouse is required")))?,
                );
            }
            let pk_column_names: Vec<_> = schema
                .fields
                .iter()
                .enumerate()
                .filter(|(index, _)| pk_indices.contains(index))
                .map(|(_, field)| field.name.clone())
                .collect();
            if pk_column_names.is_empty() {
                return Err(SinkError::Config(anyhow!(
                    "Primary key columns not found. Please set the `primary_key` column in the sink properties, or ensure that the sink contains the primary key columns from the upstream."
                )));
            }
            snowflake_task_ctx.pk_column_names = Some(pk_column_names);
            snowflake_task_ctx.all_column_names = Some(
                schema
                    .fields
                    .iter()
                    .map(|field| field.name.clone())
                    .collect(),
            );
            snowflake_task_ctx.task_name = Some(format!(
                "rw_snowflake_sink_from_{cdc_table_name}_to_{target_table_name}"
            ));
        }
        Ok(Some((snowflake_task_ctx, client)))
    }
}

impl EnforceSecret for SnowflakeV2Config {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "username",
        "password",
        "jdbc.url",
        // Key-pair authentication secrets
        "private_key_file_pwd",
        "private_key_pem",
    };
}

#[derive(Clone, Debug)]
pub struct SnowflakeV2Sink {
    config: SnowflakeV2Config,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    param: SinkParam,
}

impl EnforceSecret for SnowflakeV2Sink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::sink::ConnectorResult<()> {
        for prop in prop_iter {
            SnowflakeV2Config::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for SnowflakeV2Sink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = SnowflakeV2Config::from_btreemap(&param.properties)?;
        let is_append_only = param.sink_type.is_append_only();
        let pk_indices = param.downstream_pk_or_empty();
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
            param,
        })
    }
}

impl Sink for SnowflakeV2Sink {
    type LogSinker = CoordinatedLogSinker<SnowflakeSinkWriter>;

    const SINK_NAME: &'static str = SNOWFLAKE_SINK_V2;

    crate::impl_validate_sink_unknown_fields!();

    async fn validate(&self) -> Result<()> {
        risingwave_common::license::Feature::SnowflakeSink
            .check_available()
            .map_err(|e| anyhow::anyhow!(e))?;
        if let Some((snowflake_task_ctx, client)) =
            self.config.build_snowflake_task_ctx_jdbc_client(
                self.is_append_only,
                &self.schema,
                &self.pk_indices,
            )?
        {
            let client = SnowflakeJniClient::new(client, snowflake_task_ctx);
            client.execute_create_table().await?;
            client.execute_create_pipe().await?;
        }

        Ok(())
    }

    fn support_schema_change() -> bool {
        true
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        SnowflakeV2Config::from_btreemap(config)?;
        Ok(())
    }

    async fn new_log_sinker(
        &self,
        writer_param: crate::sink::SinkWriterParam,
    ) -> Result<Self::LogSinker> {
        let writer = SnowflakeSinkWriter::new(
            self.config.clone(),
            self.is_append_only,
            writer_param.clone(),
            self.param.clone(),
        )
        .await?;

        let commit_checkpoint_interval =
            NonZeroU64::new(self.config.commit_checkpoint_interval).expect(
                "commit_checkpoint_interval should be greater than 0, and it should be checked in config validation",
            );

        CoordinatedLogSinker::new(
            &writer_param,
            self.param.clone(),
            writer,
            commit_checkpoint_interval,
        )
        .await
    }

    fn is_coordinated_sink(&self) -> bool {
        true
    }

    async fn new_coordinator(
        &self,
        _iceberg_compact_stat_sender: Option<UnboundedSender<IcebergSinkCompactionUpdate>>,
    ) -> Result<SinkCommitCoordinator> {
        let coordinator = SnowflakeSinkCommitter::new(
            self.config.clone(),
            &self.schema,
            &self.pk_indices,
            self.is_append_only,
            self.param.sink_id,
        )?;
        Ok(SinkCommitCoordinator::SinglePhase(Box::new(coordinator)))
    }
}

pub enum SnowflakeSinkWriter {
    S3(SnowflakeRedshiftSinkS3Writer),
    Jdbc(SnowflakeRedshiftSinkJdbcWriter),
}

impl SnowflakeSinkWriter {
    pub async fn new(
        config: SnowflakeV2Config,
        is_append_only: bool,
        writer_param: SinkWriterParam,
        param: SinkParam,
    ) -> Result<Self> {
        let schema = param.schema();
        let database = config.snowflake_database.ok_or_else(|| {
            SinkError::Config(anyhow!("database is required for Snowflake JDBC sink"))
        })?;
        let schema_name = config.snowflake_schema.ok_or_else(|| {
            SinkError::Config(anyhow!("schema is required for Snowflake JDBC sink"))
        })?;
        let table_name = config.snowflake_target_table_name.ok_or_else(|| {
            SinkError::Config(anyhow!("table.name is required for Snowflake JDBC sink"))
        })?;
        if config.with_s3 {
            let s3_writer = SnowflakeRedshiftSinkS3Writer::new(
                config.s3_inner.ok_or_else(|| {
                    SinkError::Config(anyhow!(
                        "S3 configuration is required for Snowflake S3 sink"
                    ))
                })?,
                schema,
                is_append_only,
                table_name,
            )?;
            Ok(Self::S3(s3_writer))
        } else {
            let jdbc_writer = SnowflakeRedshiftSinkJdbcWriter::new(
                is_append_only,
                writer_param,
                param,
                build_full_table_name(&database, &schema_name, &table_name),
            )
            .await?;
            Ok(Self::Jdbc(jdbc_writer))
        }
    }
}

#[async_trait]
impl SinkWriter for SnowflakeSinkWriter {
    type CommitMetadata = Option<SinkMetadata>;

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        match self {
            Self::S3(writer) => writer.begin_epoch(epoch),
            Self::Jdbc(writer) => writer.begin_epoch(epoch).await,
        }
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        match self {
            Self::S3(writer) => writer.write_batch(chunk).await,
            Self::Jdbc(writer) => writer.write_batch(chunk).await,
        }
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<SinkMetadata>> {
        match self {
            Self::S3(writer) => {
                writer.barrier(is_checkpoint).await?;
            }
            Self::Jdbc(writer) => {
                writer.barrier(is_checkpoint).await?;
            }
        }
        Ok(Some(SinkMetadata {
            metadata: Some(sink_metadata::Metadata::Serialized(
                risingwave_pb::connector_service::sink_metadata::SerializedMetadata {
                    metadata: vec![],
                },
            )),
        }))
    }

    async fn abort(&mut self) -> Result<()> {
        if let Self::Jdbc(writer) = self {
            writer.abort().await
        } else {
            Ok(())
        }
    }
}

#[derive(Default, Clone)]
pub struct SnowflakeTaskContext {
    // required for task creation
    pub target_table_name: String,
    pub database: String,
    pub schema_name: String,
    pub schema: Schema,

    // only upsert
    pub task_name: Option<String>,
    pub cdc_table_name: Option<String>,
    pub writer_target_interval_seconds: u64,
    pub warehouse: Option<String>,
    pub task_serverless: bool,
    pub task_target_completion_interval: Option<String>,
    pub pk_column_names: Option<Vec<String>>,
    pub all_column_names: Option<Vec<String>>,

    // only s3 writer
    pub stage: Option<String>,
    pub pipe_name: Option<String>,
}
pub struct SnowflakeSinkCommitter {
    client: Option<SnowflakeJniClient>,
    _periodic_task_handle: Option<tokio::task::JoinHandle<()>>,
    shutdown_sender: Option<tokio::sync::mpsc::UnboundedSender<()>>,
}

impl SnowflakeSinkCommitter {
    pub fn new(
        config: SnowflakeV2Config,
        schema: &Schema,
        pk_indices: &Vec<usize>,
        is_append_only: bool,
        sink_id: SinkId,
    ) -> Result<Self> {
        let (client, periodic_task_handle, shutdown_sender) =
            if let Some((snowflake_task_ctx, client)) =
                config.build_snowflake_task_ctx_jdbc_client(is_append_only, schema, pk_indices)?
            {
                let (shutdown_sender, shutdown_receiver) = unbounded_channel();
                let snowflake_client =
                    SnowflakeJniClient::new(client.clone(), snowflake_task_ctx.clone());
                let periodic_task_handle = tokio::spawn(async move {
                    Self::run_periodic_query_task(
                        snowflake_client,
                        config.write_intermediate_interval_seconds,
                        sink_id,
                        shutdown_receiver,
                    )
                    .await;
                });
                (
                    Some(SnowflakeJniClient::new(client, snowflake_task_ctx)),
                    Some(periodic_task_handle),
                    Some(shutdown_sender),
                )
            } else {
                (None, None, None)
            };

        Ok(Self {
            client,
            _periodic_task_handle: periodic_task_handle,
            shutdown_sender,
        })
    }

    async fn run_periodic_query_task(
        client: SnowflakeJniClient,
        write_intermediate_interval_seconds: u64,
        sink_id: SinkId,
        mut shutdown_receiver: tokio::sync::mpsc::UnboundedReceiver<()>,
    ) {
        let mut copy_timer = interval(Duration::from_secs(write_intermediate_interval_seconds));
        copy_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown_receiver.recv() => break,
                _ = copy_timer.tick() => {
                    if let Err(e) = async {
                        client.execute_flush_pipe().await?;
                        Ok::<(),SinkError>(())
                    }.await {
                        tracing::error!("Failed to execute copy into task for sink id {}: {}", sink_id, e.as_report());
                    }
                }
            }
        }
        tracing::info!("Periodic query task stopped for sink id {}", sink_id);
    }
}

#[async_trait]
impl SinglePhaseCommitCoordinator for SnowflakeSinkCommitter {
    async fn init(&mut self) -> Result<()> {
        if let Some(client) = &self.client {
            // Todo: move this to validate
            client.execute_create_pipe().await?;
            client.execute_create_merge_into_task().await?;
        }
        Ok(())
    }

    async fn commit_data(&mut self, _epoch: u64, _metadata: Vec<SinkMetadata>) -> Result<()> {
        Ok(())
    }

    async fn commit_schema_change(
        &mut self,
        _epoch: u64,
        schema_change: PbSinkSchemaChange,
    ) -> Result<()> {
        use risingwave_pb::stream_plan::sink_schema_change::PbOp as SinkSchemaChangeOp;
        let schema_change_op = schema_change
            .op
            .ok_or_else(|| SinkError::Coordinator(anyhow!("Invalid schema change operation")))?;
        let SinkSchemaChangeOp::AddColumns(add_columns) = schema_change_op else {
            return Err(SinkError::Coordinator(anyhow!(
                "Only AddColumns schema change is supported for Snowflake sink"
            )));
        };
        let client = self.client.as_mut().ok_or_else(|| {
            SinkError::Config(anyhow!("Snowflake sink committer is not initialized."))
        })?;
        client
            .execute_alter_add_columns(
                &add_columns
                    .fields
                    .into_iter()
                    .map(|f| {
                        let dt = DataType::from(f.data_type.unwrap());
                        Ok((f.name, convert_snowflake_data_type(&dt)?))
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
            .await
    }
}

impl Drop for SnowflakeSinkCommitter {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            if let Some(sender) = self.shutdown_sender.take() {
                let _ = sender.send(()); // Ignore the result, as the receiver may have been dropped.
            }
            tokio::spawn(async move {
                client.execute_drop_task().await.ok();
            });
        }
    }
}

pub struct SnowflakeJniClient {
    jdbc_client: JdbcJniClient,
    snowflake_task_context: SnowflakeTaskContext,
}

impl SnowflakeJniClient {
    pub fn new(jdbc_client: JdbcJniClient, snowflake_task_context: SnowflakeTaskContext) -> Self {
        Self {
            jdbc_client,
            snowflake_task_context,
        }
    }

    pub async fn execute_alter_add_columns(
        &mut self,
        columns: &Vec<(String, String)>,
    ) -> Result<()> {
        self.execute_drop_task().await?;
        if let Some(names) = self.snowflake_task_context.all_column_names.as_mut() {
            names.extend(columns.iter().map(|(name, _)| name.clone()));
        }
        if let Some(cdc_table_name) = &self.snowflake_task_context.cdc_table_name {
            let alter_add_column_cdc_table_sql = build_alter_add_column_sql(
                cdc_table_name,
                &self.snowflake_task_context.database,
                &self.snowflake_task_context.schema_name,
                columns,
            );
            self.jdbc_client
                .execute_sql_sync(vec![alter_add_column_cdc_table_sql])
                .await?;
        }

        let alter_add_column_target_table_sql = build_alter_add_column_sql(
            &self.snowflake_task_context.target_table_name,
            &self.snowflake_task_context.database,
            &self.snowflake_task_context.schema_name,
            columns,
        );
        self.jdbc_client
            .execute_sql_sync(vec![alter_add_column_target_table_sql])
            .await?;

        self.execute_create_merge_into_task().await?;
        Ok(())
    }

    pub async fn execute_create_merge_into_task(&self) -> Result<()> {
        if self.snowflake_task_context.task_name.is_some() {
            let create_task_sql = build_create_merge_into_task_sql(&self.snowflake_task_context);
            let start_task_sql = build_start_task_sql(&self.snowflake_task_context);
            self.jdbc_client
                .execute_sql_sync(vec![create_task_sql])
                .await?;
            self.jdbc_client
                .execute_sql_sync(vec![start_task_sql])
                .await?;
        }
        Ok(())
    }

    pub async fn execute_drop_task(&self) -> Result<()> {
        if self.snowflake_task_context.task_name.is_some() {
            let sql = build_drop_task_sql(&self.snowflake_task_context);
            if let Err(e) = self.jdbc_client.execute_sql_sync(vec![sql]).await {
                tracing::error!(
                    "Failed to drop Snowflake sink task {:?}: {:?}",
                    self.snowflake_task_context.task_name,
                    e.as_report()
                );
            } else {
                tracing::info!(
                    "Snowflake sink task {:?} dropped",
                    self.snowflake_task_context.task_name
                );
            }
        }
        Ok(())
    }

    pub async fn execute_create_table(&self) -> Result<()> {
        // create target table
        let create_target_table_sql = build_create_table_sql(
            &self.snowflake_task_context.target_table_name,
            &self.snowflake_task_context.database,
            &self.snowflake_task_context.schema_name,
            &self.snowflake_task_context.schema,
            false,
        )?;
        self.jdbc_client
            .execute_sql_sync(vec![create_target_table_sql])
            .await?;
        if let Some(cdc_table_name) = &self.snowflake_task_context.cdc_table_name {
            let create_cdc_table_sql = build_create_table_sql(
                cdc_table_name,
                &self.snowflake_task_context.database,
                &self.snowflake_task_context.schema_name,
                &self.snowflake_task_context.schema,
                true,
            )?;
            self.jdbc_client
                .execute_sql_sync(vec![create_cdc_table_sql])
                .await?;
        }
        Ok(())
    }

    pub async fn execute_create_pipe(&self) -> Result<()> {
        if let Some(pipe_name) = &self.snowflake_task_context.pipe_name {
            let table_name =
                if let Some(table_name) = self.snowflake_task_context.cdc_table_name.as_ref() {
                    table_name
                } else {
                    &self.snowflake_task_context.target_table_name
                };
            let create_pipe_sql = build_create_pipe_sql(
                table_name,
                &self.snowflake_task_context.database,
                &self.snowflake_task_context.schema_name,
                self.snowflake_task_context.stage.as_ref().ok_or_else(|| {
                    SinkError::Config(anyhow!("snowflake.stage is required for S3 writer"))
                })?,
                pipe_name,
                &self.snowflake_task_context.target_table_name,
            );
            self.jdbc_client
                .execute_sql_sync(vec![create_pipe_sql])
                .await?;
        }
        Ok(())
    }

    pub async fn execute_flush_pipe(&self) -> Result<()> {
        if let Some(pipe_name) = &self.snowflake_task_context.pipe_name {
            let flush_pipe_sql = build_flush_pipe_sql(
                &self.snowflake_task_context.database,
                &self.snowflake_task_context.schema_name,
                pipe_name,
            );
            self.jdbc_client
                .execute_sql_sync(vec![flush_pipe_sql])
                .await?;
        }
        Ok(())
    }
}

fn build_create_table_sql(
    table_name: &str,
    database: &str,
    schema_name: &str,
    schema: &Schema,
    need_op_and_row_id: bool,
) -> Result<String> {
    let full_table_name = build_full_table_name(database, schema_name, table_name);
    let mut columns: Vec<String> = schema
        .fields
        .iter()
        .map(|field| {
            let data_type = convert_snowflake_data_type(&field.data_type)?;
            Ok(format!(r#""{}" {}"#, field.name, data_type))
        })
        .collect::<Result<Vec<String>>>()?;
    if need_op_and_row_id {
        columns.push(format!(r#""{}" STRING"#, __ROW_ID));
        columns.push(format!(r#""{}" INT"#, __OP));
    }
    let columns_str = columns.join(", ");
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} ({}) ENABLE_SCHEMA_EVOLUTION  = true",
        full_table_name, columns_str
    ))
}

fn convert_snowflake_data_type(data_type: &DataType) -> Result<String> {
    let data_type = match data_type {
        DataType::Int16 => "SMALLINT".to_owned(),
        DataType::Int32 => "INTEGER".to_owned(),
        DataType::Int64 => "BIGINT".to_owned(),
        DataType::Float32 => "FLOAT4".to_owned(),
        DataType::Float64 => "FLOAT8".to_owned(),
        DataType::Boolean => "BOOLEAN".to_owned(),
        DataType::Varchar => "STRING".to_owned(),
        DataType::Date => "DATE".to_owned(),
        DataType::Timestamp => "TIMESTAMP".to_owned(),
        DataType::Timestamptz => "TIMESTAMP_TZ".to_owned(),
        DataType::Jsonb => "STRING".to_owned(),
        // RisingWave uses rust_decimal with MAX_PRECISION=28. Snowflake's DECIMAL without
        // explicit precision defaults to (38,0), which drops all fractional digits. We use
        // DECIMAL(38, 10) to preserve up to 10 fractional digits, matching the Iceberg sink
        // convention, though values with more than 10 fractional digits may still lose precision.
        DataType::Decimal => "DECIMAL(38, 10)".to_owned(),
        DataType::Bytea => "BINARY".to_owned(),
        DataType::Time => "TIME".to_owned(),
        _ => {
            return Err(SinkError::Config(anyhow!(
                "Dont support auto create table for datatype: {}",
                data_type
            )));
        }
    };
    Ok(data_type)
}

fn build_create_pipe_sql(
    table_name: &str,
    database: &str,
    schema: &str,
    stage: &str,
    pipe_name: &str,
    target_table_name: &str,
) -> String {
    let pipe_name = format!(r#""{}"."{}"."{}""#, database, schema, pipe_name);
    // Trailing `/` is required to enforce exact directory matching.
    // Without it, a PIPE for table "dim_project" would also match files under
    // "dim_project_contract/" since Snowflake uses prefix matching.
    let stage = format!(
        r#""{}"."{}"."{}"/{}/"#,
        database, schema, stage, target_table_name
    );
    let table_name = format!(r#""{}"."{}"."{}""#, database, schema, table_name);
    format!(
        "CREATE OR REPLACE PIPE {} AUTO_INGEST = FALSE AS COPY INTO {} FROM @{} MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FILE_FORMAT = (type = 'JSON');",
        pipe_name, table_name, stage
    )
}

fn build_flush_pipe_sql(database: &str, schema: &str, pipe_name: &str) -> String {
    let pipe_name = format!(r#""{}"."{}"."{}""#, database, schema, pipe_name);
    format!("ALTER PIPE {} REFRESH;", pipe_name,)
}

fn build_alter_add_column_sql(
    table_name: &str,
    database: &str,
    schema: &str,
    columns: &Vec<(String, String)>,
) -> String {
    let full_table_name = build_full_table_name(database, schema, table_name);
    jdbc_jni_client::build_alter_add_column_sql(&full_table_name, columns, true)
}

fn build_start_task_sql(snowflake_task_context: &SnowflakeTaskContext) -> String {
    let SnowflakeTaskContext {
        task_name,
        database,
        schema_name: schema,
        ..
    } = snowflake_task_context;
    let full_task_name = format!(
        r#""{}"."{}"."{}""#,
        database,
        schema,
        task_name.as_ref().unwrap()
    );
    format!("ALTER TASK {} RESUME", full_task_name)
}

fn build_drop_task_sql(snowflake_task_context: &SnowflakeTaskContext) -> String {
    let SnowflakeTaskContext {
        task_name,
        database,
        schema_name: schema,
        ..
    } = snowflake_task_context;
    let full_task_name = format!(
        r#""{}"."{}"."{}""#,
        database,
        schema,
        task_name.as_ref().unwrap()
    );
    format!("DROP TASK IF EXISTS {}", full_task_name)
}

fn build_create_merge_into_task_sql(snowflake_task_context: &SnowflakeTaskContext) -> String {
    let SnowflakeTaskContext {
        task_name,
        cdc_table_name,
        target_table_name,
        writer_target_interval_seconds,
        warehouse,
        task_serverless,
        task_target_completion_interval,
        pk_column_names,
        all_column_names,
        database,
        schema_name,
        ..
    } = snowflake_task_context;
    let full_task_name = format!(
        r#""{}"."{}"."{}""#,
        database,
        schema_name,
        task_name.as_ref().unwrap()
    );
    let full_cdc_table_name = format!(
        r#""{}"."{}"."{}""#,
        database,
        schema_name,
        cdc_table_name.as_ref().unwrap()
    );
    let full_target_table_name = format!(
        r#""{}"."{}"."{}""#,
        database, schema_name, target_table_name
    );

    let pk_names_str = pk_column_names
        .as_ref()
        .unwrap()
        .iter()
        .map(|name| format!(r#""{}""#, name))
        .collect::<Vec<String>>()
        .join(", ");
    let pk_names_eq_str = pk_column_names
        .as_ref()
        .unwrap()
        .iter()
        .map(|name| format!(r#"target."{}" = source."{}""#, name, name))
        .collect::<Vec<String>>()
        .join(" AND ");
    let all_column_names_set_str = all_column_names
        .as_ref()
        .unwrap()
        .iter()
        .map(|name| format!(r#"target."{}" = source."{}""#, name, name))
        .collect::<Vec<String>>()
        .join(", ");
    let all_column_names_str = all_column_names
        .as_ref()
        .unwrap()
        .iter()
        .map(|name| format!(r#""{}""#, name))
        .collect::<Vec<String>>()
        .join(", ");
    let all_column_names_insert_str = all_column_names
        .as_ref()
        .unwrap()
        .iter()
        .map(|name| format!(r#"source."{}""#, name))
        .collect::<Vec<String>>()
        .join(", ");

    let compute_clause = if *task_serverless {
        task_target_completion_interval
            .as_ref()
            .map(|interval| format!("TARGET_COMPLETION_INTERVAL = '{}'", interval))
    } else {
        Some(format!("WAREHOUSE = {}", warehouse.as_ref().unwrap()))
    };

    format!(
        r#"CREATE OR REPLACE TASK {task_name}
{compute_clause}
SCHEDULE = '{writer_target_interval_seconds} SECONDS'
AS
BEGIN
    LET max_row_id STRING;

    SELECT COALESCE(MAX("{snowflake_sink_row_id}"), '0') INTO :max_row_id
    FROM {cdc_table_name};

    MERGE INTO {target_table_name} AS target
    USING (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {pk_names_str} ORDER BY "{snowflake_sink_row_id}" DESC) AS dedupe_id
            FROM {cdc_table_name}
            WHERE "{snowflake_sink_row_id}" <= :max_row_id
        ) AS subquery
        WHERE dedupe_id = 1
    ) AS source
    ON {pk_names_eq_str}
    WHEN MATCHED AND source."{snowflake_sink_op}" IN (2, 4) THEN DELETE
    WHEN MATCHED AND source."{snowflake_sink_op}" IN (1, 3) THEN UPDATE SET {all_column_names_set_str}
    WHEN NOT MATCHED AND source."{snowflake_sink_op}" IN (1, 3) THEN INSERT ({all_column_names_str}) VALUES ({all_column_names_insert_str});

    DELETE FROM {cdc_table_name}
    WHERE "{snowflake_sink_row_id}" <= :max_row_id;
END;"#,
        task_name = full_task_name,
        compute_clause = compute_clause
            .map(|clause| format!("{clause}\n"))
            .unwrap_or_default(),
        writer_target_interval_seconds = writer_target_interval_seconds,
        cdc_table_name = full_cdc_table_name,
        target_table_name = full_target_table_name,
        pk_names_str = pk_names_str,
        pk_names_eq_str = pk_names_eq_str,
        all_column_names_set_str = all_column_names_set_str,
        all_column_names_str = all_column_names_str,
        all_column_names_insert_str = all_column_names_insert_str,
        snowflake_sink_row_id = __ROW_ID,
        snowflake_sink_op = __OP,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::sink::jdbc_jni_client::normalize_sql;

    fn base_properties() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("type".to_owned(), "append-only".to_owned()),
            ("jdbc.url".to_owned(), "jdbc:snowflake://account".to_owned()),
            ("username".to_owned(), "RW_USER".to_owned()),
        ])
    }

    #[test]
    fn test_build_jdbc_props_password() {
        let mut props = base_properties();
        props.insert("password".to_owned(), "secret".to_owned());
        let config = SnowflakeV2Config::from_btreemap(&props).unwrap();
        let (url, connection_properties) = config.build_jdbc_connection_properties().unwrap();
        assert_eq!(url, "jdbc:snowflake://account");
        let map: BTreeMap<_, _> = connection_properties.into_iter().collect();
        assert_eq!(map.get("user"), Some(&"RW_USER".to_owned()));
        assert_eq!(map.get("password"), Some(&"secret".to_owned()));
        assert!(!map.contains_key("authenticator"));
    }

    #[test]
    fn test_build_jdbc_props_key_pair_file() {
        let mut props = base_properties();
        props.insert(
            "auth.method".to_owned(),
            AUTH_METHOD_KEY_PAIR_FILE.to_owned(),
        );
        props.insert("private_key_file".to_owned(), "/tmp/rsa_key.p8".to_owned());
        props.insert("private_key_file_pwd".to_owned(), "dummy".to_owned());
        let config = SnowflakeV2Config::from_btreemap(&props).unwrap();
        let (url, connection_properties) = config.build_jdbc_connection_properties().unwrap();
        assert_eq!(url, "jdbc:snowflake://account");
        let map: BTreeMap<_, _> = connection_properties.into_iter().collect();
        assert_eq!(map.get("user"), Some(&"RW_USER".to_owned()));
        assert_eq!(
            map.get("private_key_file"),
            Some(&"/tmp/rsa_key.p8".to_owned())
        );
        assert_eq!(map.get("private_key_file_pwd"), Some(&"dummy".to_owned()));
    }

    #[test]
    fn test_build_jdbc_props_key_pair_object() {
        let mut props = base_properties();
        props.insert(
            "auth.method".to_owned(),
            AUTH_METHOD_KEY_PAIR_OBJECT.to_owned(),
        );
        props.insert(
            "private_key_pem".to_owned(),
            "-----BEGIN PRIVATE KEY-----
...
-----END PRIVATE KEY-----"
                .to_owned(),
        );
        let config = SnowflakeV2Config::from_btreemap(&props).unwrap();
        let (url, connection_properties) = config.build_jdbc_connection_properties().unwrap();
        assert_eq!(url, "jdbc:snowflake://account");
        let map: BTreeMap<_, _> = connection_properties.into_iter().collect();
        assert_eq!(
            map.get("private_key_pem"),
            Some(
                &"-----BEGIN PRIVATE KEY-----
...
-----END PRIVATE KEY-----"
                    .to_owned()
            )
        );
        assert!(!map.contains_key("private_key_file"));
    }

    #[test]
    fn test_snowflake_task_target_completion_interval_requires_serverless() {
        let mut props = base_properties();
        props.insert("password".to_owned(), "secret".to_owned());
        props.insert("type".to_owned(), "upsert".to_owned());
        props.insert(
            "task.target_completion_interval".to_owned(),
            "5 MINUTES".to_owned(),
        );

        let err = SnowflakeV2Config::from_btreemap(&props).unwrap_err();
        assert!(
            err.as_report().to_string().contains(
                "`task.target_completion_interval` requires `task.serverless` to be true"
            )
        );
    }

    #[test]
    fn test_snowflake_serverless_task_rejects_warehouse() {
        let mut props = base_properties();
        props.insert("password".to_owned(), "secret".to_owned());
        props.insert("type".to_owned(), "upsert".to_owned());
        props.insert("task.serverless".to_owned(), "true".to_owned());
        props.insert("warehouse".to_owned(), "test_warehouse".to_owned());

        let err = SnowflakeV2Config::from_btreemap(&props).unwrap_err();
        assert!(
            err.as_report()
                .to_string()
                .contains("`task.serverless` must not be combined with `warehouse`")
        );
    }

    #[test]
    fn test_snowflake_append_only_rejects_upsert_task_options() {
        for (key, value) in [
            ("intermediate.table.name", "test_intermediate"),
            ("write.target.interval.seconds", "3600"),
            ("warehouse", "test_warehouse"),
            ("task.serverless", "true"),
            ("task.target_completion_interval", "5 MINUTES"),
        ] {
            let mut props = base_properties();
            props.insert("password".to_owned(), "secret".to_owned());
            props.insert(key.to_owned(), value.to_owned());

            let err = SnowflakeV2Config::from_btreemap(&props).unwrap_err();
            assert!(
                err.as_report().to_string().contains(
                    "`intermediate.table.name`, `write.target.interval.seconds`, `warehouse`, \
                 `task.serverless`, and `task.target_completion_interval` require `type` = upsert"
                ),
                "option {key} should be rejected for append-only sink"
            );
        }
    }

    #[test]
    fn test_snowflake_sink_commit_coordinator() {
        let snowflake_task_context = SnowflakeTaskContext {
            task_name: Some("test_task".to_owned()),
            cdc_table_name: Some("test_cdc_table".to_owned()),
            target_table_name: "test_target_table".to_owned(),
            writer_target_interval_seconds: 3600,
            warehouse: Some("test_warehouse".to_owned()),
            task_serverless: false,
            task_target_completion_interval: None,
            pk_column_names: Some(vec!["v1".to_owned()]),
            all_column_names: Some(vec!["v1".to_owned(), "v2".to_owned()]),
            database: "test_db".to_owned(),
            schema_name: "test_schema".to_owned(),
            schema: Schema { fields: vec![] },
            stage: None,
            pipe_name: None,
        };
        let task_sql = build_create_merge_into_task_sql(&snowflake_task_context);
        let expected = r#"CREATE OR REPLACE TASK "test_db"."test_schema"."test_task"
WAREHOUSE = test_warehouse
SCHEDULE = '3600 SECONDS'
AS
BEGIN
    LET max_row_id STRING;

    SELECT COALESCE(MAX("__row_id"), '0') INTO :max_row_id
    FROM "test_db"."test_schema"."test_cdc_table";

    MERGE INTO "test_db"."test_schema"."test_target_table" AS target
    USING (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY "v1" ORDER BY "__row_id" DESC) AS dedupe_id
            FROM "test_db"."test_schema"."test_cdc_table"
            WHERE "__row_id" <= :max_row_id
        ) AS subquery
        WHERE dedupe_id = 1
    ) AS source
    ON target."v1" = source."v1"
    WHEN MATCHED AND source."__op" IN (2, 4) THEN DELETE
    WHEN MATCHED AND source."__op" IN (1, 3) THEN UPDATE SET target."v1" = source."v1", target."v2" = source."v2"
    WHEN NOT MATCHED AND source."__op" IN (1, 3) THEN INSERT ("v1", "v2") VALUES (source."v1", source."v2");

    DELETE FROM "test_db"."test_schema"."test_cdc_table"
    WHERE "__row_id" <= :max_row_id;
END;"#;
        assert_eq!(normalize_sql(&task_sql), normalize_sql(expected));
    }

    #[test]
    fn test_snowflake_sink_commit_coordinator_multi_pk() {
        let snowflake_task_context = SnowflakeTaskContext {
            task_name: Some("test_task_multi_pk".to_owned()),
            cdc_table_name: Some("cdc_multi_pk".to_owned()),
            target_table_name: "target_multi_pk".to_owned(),
            writer_target_interval_seconds: 300,
            warehouse: Some("multi_pk_warehouse".to_owned()),
            task_serverless: false,
            task_target_completion_interval: None,
            pk_column_names: Some(vec!["id1".to_owned(), "id2".to_owned()]),
            all_column_names: Some(vec!["id1".to_owned(), "id2".to_owned(), "val".to_owned()]),
            database: "test_db".to_owned(),
            schema_name: "test_schema".to_owned(),
            schema: Schema { fields: vec![] },
            stage: None,
            pipe_name: None,
        };
        let task_sql = build_create_merge_into_task_sql(&snowflake_task_context);
        let expected = r#"CREATE OR REPLACE TASK "test_db"."test_schema"."test_task_multi_pk"
WAREHOUSE = multi_pk_warehouse
SCHEDULE = '300 SECONDS'
AS
BEGIN
    LET max_row_id STRING;

    SELECT COALESCE(MAX("__row_id"), '0') INTO :max_row_id
    FROM "test_db"."test_schema"."cdc_multi_pk";

    MERGE INTO "test_db"."test_schema"."target_multi_pk" AS target
    USING (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY "id1", "id2" ORDER BY "__row_id" DESC) AS dedupe_id
            FROM "test_db"."test_schema"."cdc_multi_pk"
            WHERE "__row_id" <= :max_row_id
        ) AS subquery
        WHERE dedupe_id = 1
    ) AS source
    ON target."id1" = source."id1" AND target."id2" = source."id2"
    WHEN MATCHED AND source."__op" IN (2, 4) THEN DELETE
    WHEN MATCHED AND source."__op" IN (1, 3) THEN UPDATE SET target."id1" = source."id1", target."id2" = source."id2", target."val" = source."val"
    WHEN NOT MATCHED AND source."__op" IN (1, 3) THEN INSERT ("id1", "id2", "val") VALUES (source."id1", source."id2", source."val");

    DELETE FROM "test_db"."test_schema"."cdc_multi_pk"
    WHERE "__row_id" <= :max_row_id;
END;"#;
        assert_eq!(normalize_sql(&task_sql), normalize_sql(expected));
    }

    #[test]
    fn test_snowflake_sink_commit_coordinator_serverless_task() {
        let snowflake_task_context = SnowflakeTaskContext {
            task_name: Some("test_serverless_task".to_owned()),
            cdc_table_name: Some("serverless_cdc_table".to_owned()),
            target_table_name: "serverless_target_table".to_owned(),
            writer_target_interval_seconds: 120,
            warehouse: None,
            task_serverless: true,
            task_target_completion_interval: Some("5 MINUTES".to_owned()),
            pk_column_names: Some(vec!["id".to_owned()]),
            all_column_names: Some(vec!["id".to_owned(), "val".to_owned()]),
            database: "test_db".to_owned(),
            schema_name: "test_schema".to_owned(),
            schema: Schema { fields: vec![] },
            stage: None,
            pipe_name: None,
        };
        let task_sql = build_create_merge_into_task_sql(&snowflake_task_context);
        let expected = r#"CREATE OR REPLACE TASK "test_db"."test_schema"."test_serverless_task"
TARGET_COMPLETION_INTERVAL = '5 MINUTES'
SCHEDULE = '120 SECONDS'
AS
BEGIN
    LET max_row_id STRING;

    SELECT COALESCE(MAX("__row_id"), '0') INTO :max_row_id
    FROM "test_db"."test_schema"."serverless_cdc_table";

    MERGE INTO "test_db"."test_schema"."serverless_target_table" AS target
    USING (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "__row_id" DESC) AS dedupe_id
            FROM "test_db"."test_schema"."serverless_cdc_table"
            WHERE "__row_id" <= :max_row_id
        ) AS subquery
        WHERE dedupe_id = 1
    ) AS source
    ON target."id" = source."id"
    WHEN MATCHED AND source."__op" IN (2, 4) THEN DELETE
    WHEN MATCHED AND source."__op" IN (1, 3) THEN UPDATE SET target."id" = source."id", target."val" = source."val"
    WHEN NOT MATCHED AND source."__op" IN (1, 3) THEN INSERT ("id", "val") VALUES (source."id", source."val");

    DELETE FROM "test_db"."test_schema"."serverless_cdc_table"
    WHERE "__row_id" <= :max_row_id;
END;"#;
        assert_eq!(normalize_sql(&task_sql), normalize_sql(expected));
    }

    #[test]
    fn test_build_create_pipe_sql_stage_has_trailing_slash() {
        let sql = build_create_pipe_sql(
            "reservations_intermediate",
            "test_db",
            "test_schema",
            "RW_S3_STAGE",
            "reservations_pipe",
            "reservations",
        );
        assert!(
            sql.contains(r#"FROM @"test_db"."test_schema"."RW_S3_STAGE"/reservations/ "#),
            "unexpected pipe sql: {sql}"
        );
    }

    #[test]
    fn test_convert_snowflake_decimal_data_type() {
        assert_eq!(
            convert_snowflake_data_type(&DataType::Decimal).unwrap(),
            "DECIMAL(38, 10)"
        );
    }
}
