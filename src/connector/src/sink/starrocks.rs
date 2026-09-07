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

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use chrono_tz::Tz;
use mysql_async::Opts;
use mysql_async::prelude::Queryable;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::{DisplayFromStr, serde_as};
use thiserror_ext::AsReport;
use url::form_urlencoded;
use uuid::Uuid;
use with_options::WithOptions;

use super::decouple_checkpoint_log_sink::DEFAULT_COMMIT_CHECKPOINT_INTERVAL_WITH_SINK_DECOUPLE;
use super::doris_starrocks_connector::{
    HeaderBuilder, InserterInner, STARROCKS_DELETE_SIGN, STARROCKS_SUCCESS_STATUS,
    StarrocksTxnRequestBuilder,
};
use super::encoder::{JsonEncoder, RowEncoder};
use super::{
    SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, SinkError, SinkParam,
    SinkWriterMetrics,
};
use crate::enforce_secret::EnforceSecret;
use crate::sink::decouple_checkpoint_log_sink::DecoupleCheckpointLogSinkerOf;
use crate::sink::writer::SinkWriter;
use crate::sink::{Result, Sink, SinkWriterParam};

pub const STARROCKS_SINK: &str = "starrocks";
const STARROCK_MYSQL_PREFER_SOCKET: &str = "false";
const STARROCK_MYSQL_MAX_ALLOWED_PACKET: usize = 1024;
const STARROCK_MYSQL_WAIT_TIMEOUT: usize = 28800;
pub const fn _default_stream_load_http_timeout_ms() -> u64 {
    30 * 1000
}

const fn default_use_https() -> bool {
    false
}

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct StarrocksCommon {
    /// The `StarRocks` host address.
    #[serde(rename = "starrocks.host")]
    pub host: String,
    /// The port to the MySQL server of `StarRocks` FE.
    #[serde(rename = "starrocks.mysqlport", alias = "starrocks.query_port")]
    pub mysql_port: String,
    /// The port to the HTTP server of `StarRocks` FE.
    #[serde(rename = "starrocks.httpport", alias = "starrocks.http_port")]
    pub http_port: String,
    /// The user name used to access the `StarRocks` database.
    #[serde(rename = "starrocks.user")]
    pub user: String,
    /// The password associated with the user.
    #[serde(rename = "starrocks.password")]
    pub password: String,
    /// The `StarRocks` database where the target table is located
    #[serde(rename = "starrocks.database")]
    pub database: String,
    /// The `StarRocks` table you want to sink data to.
    #[serde(rename = "starrocks.table")]
    pub table: String,

    /// Whether to use https to connect to the `StarRocks` server.
    #[serde(rename = "starrocks.use_https")]
    #[serde(default = "default_use_https")]
    #[serde_as(as = "DisplayFromStr")]
    pub use_https: bool,
}

impl EnforceSecret for StarrocksCommon {
    const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf::phf_set! {
        "starrocks.password", "starrocks.user"
    };
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct StarrocksConfig {
    #[serde(flatten)]
    pub common: StarrocksCommon,

    /// The timeout in milliseconds for stream load http request, defaults to 10 seconds.
    #[serde(
        rename = "starrocks.stream_load.http.timeout.ms",
        default = "_default_stream_load_http_timeout_ms"
    )]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    pub stream_load_http_timeout_ms: u64,

    /// Set this option to a positive integer n, RisingWave will try to commit data
    /// to Starrocks at every n checkpoints by leveraging the
    /// [StreamLoad Transaction API](https://docs.starrocks.io/docs/loading/Stream_Load_transaction_interface/),
    /// also, in this time, the `sink_decouple` option should be enabled as well.
    /// Defaults to 10 if `commit_checkpoint_interval` <= 0
    #[serde(default = "default_commit_checkpoint_interval")]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    pub commit_checkpoint_interval: u64,

    /// Enable partial update
    #[serde(rename = "starrocks.partial_update")]
    pub partial_update: Option<String>,

    /// The maximum size in bytes for each `StarRocks` stream load request payload.
    /// Defaults to unlimited.
    #[serde(rename = "starrocks.max_batch_size_bytes")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub max_batch_size_bytes: Option<u64>,

    pub r#type: String, // accept "append-only" or "upsert"

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(StarrocksConfig);

impl EnforceSecret for StarrocksConfig {
    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        StarrocksCommon::enforce_one(prop)
    }
}

fn default_commit_checkpoint_interval() -> u64 {
    DEFAULT_COMMIT_CHECKPOINT_INTERVAL_WITH_SINK_DECOUPLE
}

impl StarrocksConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<StarrocksConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        if config.commit_checkpoint_interval == 0 {
            return Err(SinkError::Config(anyhow!(
                "`commit_checkpoint_interval` must be greater than 0"
            )));
        }
        if let Some(0) = config.max_batch_size_bytes {
            return Err(SinkError::Config(anyhow!(
                "`starrocks.max_batch_size_bytes` must be greater than 0"
            )));
        }
        Ok(config)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct LoadRequestSizeDecision {
    finish_current_load: bool,
    next_batch_size_bytes: u64,
}

fn decide_load_request_size(
    current_batch_size_bytes: u64,
    row_size: u64,
    max_batch_size_bytes: u64,
) -> Result<LoadRequestSizeDecision> {
    if row_size > max_batch_size_bytes {
        return Err(SinkError::Starrocks(format!(
            "single row payload size {} bytes exceeds `starrocks.max_batch_size_bytes` limit {} bytes",
            row_size, max_batch_size_bytes
        )));
    }

    if current_batch_size_bytes > 0
        && current_batch_size_bytes
            .checked_add(row_size)
            .is_none_or(|next_batch_size_bytes| next_batch_size_bytes > max_batch_size_bytes)
    {
        return Ok(LoadRequestSizeDecision {
            finish_current_load: true,
            next_batch_size_bytes: row_size,
        });
    }

    Ok(LoadRequestSizeDecision {
        finish_current_load: false,
        next_batch_size_bytes: current_batch_size_bytes
            .checked_add(row_size)
            .expect("sum is checked against max_batch_size_bytes above"),
    })
}

#[derive(Debug)]
pub struct StarrocksSink {
    pub config: StarrocksConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl EnforceSecret for StarrocksSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            StarrocksConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl StarrocksSink {
    pub fn new(param: SinkParam, config: StarrocksConfig, schema: Schema) -> Result<Self> {
        let pk_indices = param.downstream_pk_or_empty();
        let is_append_only = param.sink_type.is_append_only();
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
        })
    }
}

impl StarrocksSink {
    fn starrocks_data_type_contains_any(
        starrocks_data_type: &str,
        expected_types: &[&str],
    ) -> bool {
        expected_types
            .iter()
            .any(|expected_type| starrocks_data_type.contains(expected_type))
    }

    fn check_column_name_and_type(
        &self,
        starrocks_columns_desc: HashMap<String, String>,
    ) -> Result<()> {
        let rw_fields_name = self.schema.fields();
        if rw_fields_name.len() > starrocks_columns_desc.len() {
            return Err(SinkError::Starrocks("The columns of the sink must be equal to or a superset of the target table's columns.".to_owned()));
        }

        for i in rw_fields_name {
            let value = starrocks_columns_desc.get(&i.name).ok_or_else(|| {
                SinkError::Starrocks(format!(
                    "Column name don't find in starrocks, risingwave is {:?} ",
                    i.name
                ))
            })?;
            if !Self::check_and_correct_column_type(&i.data_type, value)? {
                return Err(SinkError::Starrocks(format!(
                    "Column type don't match, column name is {:?}. starrocks type is {:?} risingwave type is {:?} ",
                    i.name, value, i.data_type
                )));
            }
        }
        Ok(())
    }

    fn check_and_correct_column_type(
        rw_data_type: &DataType,
        starrocks_data_type: &str,
    ) -> Result<bool> {
        match rw_data_type {
            risingwave_common::types::DataType::Boolean => {
                Ok(Self::starrocks_data_type_contains_any(
                    starrocks_data_type,
                    &["tinyint", "boolean"],
                ))
            }
            risingwave_common::types::DataType::Int16 => {
                Ok(starrocks_data_type.contains("smallint"))
            }
            risingwave_common::types::DataType::Int32 => Ok(starrocks_data_type.contains("int")),
            risingwave_common::types::DataType::Int64 => Ok(starrocks_data_type.contains("bigint")),
            risingwave_common::types::DataType::Float32 => {
                Ok(starrocks_data_type.contains("float"))
            }
            risingwave_common::types::DataType::Float64 => {
                Ok(starrocks_data_type.contains("double"))
            }
            risingwave_common::types::DataType::Decimal => {
                Ok(starrocks_data_type.contains("decimal"))
            }
            risingwave_common::types::DataType::Date => Ok(starrocks_data_type.contains("date")),
            risingwave_common::types::DataType::Varchar => {
                Ok(starrocks_data_type.contains("varchar"))
            }
            risingwave_common::types::DataType::Time => Err(SinkError::Starrocks(
                "TIME is not supported for Starrocks sink. Please convert to VARCHAR or other supported types.".to_owned(),
            )),
            risingwave_common::types::DataType::Timestamp => {
                Ok(starrocks_data_type.contains("datetime"))
            }
            risingwave_common::types::DataType::Timestamptz => {
                // StarRocks JSON encoding writes timestamptz as a timestamp string without a
                // timezone suffix, so StarRocks can parse it into DATETIME or store it as text.
                Ok(Self::starrocks_data_type_contains_any(
                    starrocks_data_type,
                    &["datetime", "varchar", "char", "string"],
                ))
            }
            risingwave_common::types::DataType::Interval => Err(SinkError::Starrocks(
                "INTERVAL is not supported for Starrocks sink. Please convert to VARCHAR or other supported types.".to_owned(),
            )),
            risingwave_common::types::DataType::Struct(_) => Err(SinkError::Starrocks(
                "STRUCT is not supported for Starrocks sink.".to_owned(),
            )),
            risingwave_common::types::DataType::List(list) => {
                // For compatibility with older versions starrocks
                if starrocks_data_type.contains("unknown") {
                    return Ok(true);
                }
                let check_result = Self::check_and_correct_column_type(list.elem(), starrocks_data_type)?;
                Ok(check_result && starrocks_data_type.contains("array"))
            }
            risingwave_common::types::DataType::Bytea => Err(SinkError::Starrocks(
                "BYTEA is not supported for Starrocks sink. Please convert to VARCHAR or other supported types.".to_owned(),
            )),
            risingwave_common::types::DataType::Jsonb => Ok(starrocks_data_type.contains("json")),
            risingwave_common::types::DataType::Variant => Err(SinkError::Starrocks(
                "VARIANT is not supported for Starrocks sink.".to_owned(),
            )),
            risingwave_common::types::DataType::Serial => {
                Ok(starrocks_data_type.contains("bigint"))
            }
            risingwave_common::types::DataType::Int256 => Err(SinkError::Starrocks(
                "INT256 is not supported for Starrocks sink.".to_owned(),
            )),
            risingwave_common::types::DataType::Map(_) => Err(SinkError::Starrocks(
                "MAP is not supported for Starrocks sink.".to_owned(),
            )),
            DataType::Vector(_) => Err(SinkError::Starrocks(
                "VECTOR is not supported for Starrocks sink.".to_owned(),
            )),
        }
    }
}

impl Sink for StarrocksSink {
    type LogSinker = DecoupleCheckpointLogSinkerOf<StarrocksSinkWriter>;

    const SINK_NAME: &'static str = STARROCKS_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Primary key not defined for upsert starrocks sink (please define in `primary_key` field)"
            )));
        }
        // check reachability
        let mut client = StarrocksSchemaClient::new(
            self.config.common.host.clone(),
            self.config.common.mysql_port.clone(),
            self.config.common.table.clone(),
            self.config.common.database.clone(),
            self.config.common.user.clone(),
            self.config.common.password.clone(),
        )
        .await?;
        let (read_model, pks) = client.get_pk_from_starrocks().await?;

        if !self.is_append_only && read_model.ne("PRIMARY_KEYS") {
            return Err(SinkError::Config(anyhow!(
                "If you want to use upsert, please set the keysType of starrocks to PRIMARY_KEY"
            )));
        }

        for (index, filed) in self.schema.fields().iter().enumerate() {
            if self.pk_indices.contains(&index) && !pks.contains(&filed.name) {
                return Err(SinkError::Starrocks(format!(
                    "Can't find pk {:?} in starrocks",
                    filed.name
                )));
            }
        }

        let starrocks_columns_desc = client.get_columns_from_starrocks().await?;

        self.check_column_name_and_type(starrocks_columns_desc)?;
        Ok(())
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        StarrocksConfig::from_btreemap(config.clone())?;
        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let commit_checkpoint_interval =
            NonZeroU64::new(self.config.commit_checkpoint_interval).expect(
                "commit_checkpoint_interval should be greater than 0, and it should be checked in config validation",
            );

        let writer = StarrocksSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
            writer_param.time_zone,
        )?;

        let metrics = SinkWriterMetrics::new(&writer_param);

        Ok(DecoupleCheckpointLogSinkerOf::new(
            writer,
            metrics,
            commit_checkpoint_interval,
        ))
    }
}

pub struct StarrocksSinkWriter {
    #[expect(dead_code)]
    schema: Schema,
    #[expect(dead_code)]
    pk_indices: Vec<usize>,
    is_append_only: bool,
    client: Option<StarrocksClient>,
    txn_client: Arc<StarrocksTxnClient>,
    row_encoder: JsonEncoder,
    curr_txn_label: Option<String>,
    max_batch_size_bytes: Option<u64>,
    current_batch_size_bytes: u64,
}

impl TryFrom<SinkParam> for StarrocksSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = StarrocksConfig::from_btreemap(param.properties.clone())?;
        StarrocksSink::new(param, config, schema)
    }
}

impl StarrocksSinkWriter {
    pub fn new(
        config: StarrocksConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
        time_zone: Tz,
    ) -> Result<Self> {
        let mut field_names = schema.names_str();
        if !is_append_only {
            field_names.push(STARROCKS_DELETE_SIGN);
        };
        // we should quote field names in `MySQL` style to prevent `StarRocks` from rejecting the request due to
        // a field name being a reserved word. For example, `order`, 'from`, etc.
        let field_names = field_names
            .into_iter()
            .map(|name| format!("`{}`", name))
            .collect::<Vec<String>>();
        let field_names_str = field_names
            .iter()
            .map(|name| name.as_str())
            .collect::<Vec<&str>>();

        let header = HeaderBuilder::new()
            .add_common_header()
            .set_user_password(config.common.user.clone(), config.common.password.clone())
            .add_json_format()
            .set_partial_update(config.partial_update.clone())
            .set_columns_name(field_names_str)
            .set_db(config.common.database.clone())
            .set_table(config.common.table.clone())
            .build();

        let url = if config.common.use_https {
            format!("https://{}:{}", config.common.host, config.common.http_port)
        } else {
            format!("http://{}:{}", config.common.host, config.common.http_port)
        };
        let txn_request_builder =
            StarrocksTxnRequestBuilder::new(url, header, config.stream_load_http_timeout_ms)?;

        Ok(Self {
            schema: schema.clone(),
            pk_indices,
            is_append_only,
            client: None,
            txn_client: Arc::new(StarrocksTxnClient::new(txn_request_builder)),
            row_encoder: JsonEncoder::new_with_starrocks(schema, None, time_zone),
            curr_txn_label: None,
            max_batch_size_bytes: config.max_batch_size_bytes,
            current_batch_size_bytes: 0,
        })
    }

    async fn finish_load_request(&mut self) -> Result<()> {
        if let Some(client) = self.client.take() {
            client.finish().await?;
            self.current_batch_size_bytes = 0;
        }
        Ok(())
    }

    async fn ensure_load_request(&mut self) -> Result<()> {
        if self.client.is_none() {
            let txn_label = self.curr_txn_label.clone().ok_or_else(|| {
                SinkError::Starrocks("Can't find current starrocks transaction label".to_owned())
            })?;
            self.client = Some(StarrocksClient::new(self.txn_client.load(txn_label).await?));
            self.current_batch_size_bytes = 0;
        }
        Ok(())
    }

    async fn write_row_json(&mut self, row_json_string: String) -> Result<()> {
        let row_size = row_json_string.len() as u64;
        let size_decision = self
            .max_batch_size_bytes
            .map(|max_batch_size_bytes| {
                decide_load_request_size(
                    self.current_batch_size_bytes,
                    row_size,
                    max_batch_size_bytes,
                )
            })
            .transpose()?;
        if size_decision
            .as_ref()
            .is_some_and(|decision| decision.finish_current_load)
        {
            self.finish_load_request().await?;
        }
        self.ensure_load_request().await?;
        self.client
            .as_mut()
            .ok_or_else(|| SinkError::Starrocks("Can't find starrocks sink insert".to_owned()))?
            .write(row_json_string.into())
            .await?;
        if let Some(size_decision) = size_decision {
            self.current_batch_size_bytes = size_decision.next_batch_size_bytes;
        }
        Ok(())
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                continue;
            }
            let row_json_string = Value::Object(self.row_encoder.encode(row)?).to_string();
            self.write_row_json(row_json_string).await?;
        }
        Ok(())
    }

    async fn upsert(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            match op {
                Op::Insert => {
                    let mut row_json_value = self.row_encoder.encode(row)?;
                    row_json_value.insert(
                        STARROCKS_DELETE_SIGN.to_owned(),
                        Value::String("0".to_owned()),
                    );
                    let row_json_string = serde_json::to_string(&row_json_value).map_err(|e| {
                        SinkError::Starrocks(format!("Json serialize error: {}", e.as_report()))
                    })?;
                    self.write_row_json(row_json_string).await?;
                }
                Op::Delete => {
                    let mut row_json_value = self.row_encoder.encode(row)?;
                    row_json_value.insert(
                        STARROCKS_DELETE_SIGN.to_owned(),
                        Value::String("1".to_owned()),
                    );
                    let row_json_string = serde_json::to_string(&row_json_value).map_err(|e| {
                        SinkError::Starrocks(format!("Json serialize error: {}", e.as_report()))
                    })?;
                    self.write_row_json(row_json_string).await?;
                }
                Op::UpdateDelete => {}
                Op::UpdateInsert => {
                    let mut row_json_value = self.row_encoder.encode(row)?;
                    row_json_value.insert(
                        STARROCKS_DELETE_SIGN.to_owned(),
                        Value::String("0".to_owned()),
                    );
                    let row_json_string = serde_json::to_string(&row_json_value).map_err(|e| {
                        SinkError::Starrocks(format!("Json serialize error: {}", e.as_report()))
                    })?;
                    self.write_row_json(row_json_string).await?;
                }
            }
        }
        Ok(())
    }

    /// Generating a new transaction label, should be unique across all `SinkWriters` even under rewinding.
    #[inline(always)]
    fn new_txn_label(&self) -> String {
        format!(
            "rw-txn-{}-{}",
            Uuid::new_v4(),
            chrono::Utc::now().timestamp_micros()
        )
    }

    async fn prepare_and_commit(&self, txn_label: String) -> Result<()> {
        tracing::debug!(?txn_label, "prepare transaction");
        let txn_label_res = self.txn_client.prepare(txn_label.clone()).await?;
        if txn_label != txn_label_res {
            return Err(SinkError::Starrocks(format!(
                "label {} returned from prepare transaction {} differs from the current one",
                txn_label, txn_label_res
            )));
        }
        tracing::debug!(?txn_label, "commit transaction");
        let txn_label_res = self.txn_client.commit(txn_label.clone()).await?;
        if txn_label != txn_label_res {
            return Err(SinkError::Starrocks(format!(
                "label {} returned from commit transaction {} differs from the current one",
                txn_label, txn_label_res
            )));
        }
        Ok(())
    }
}

impl Drop for StarrocksSinkWriter {
    fn drop(&mut self) {
        if let Some(txn_label) = self.curr_txn_label.take() {
            let txn_client = self.txn_client.clone();
            tokio::spawn(async move {
                if let Err(e) = txn_client.rollback(txn_label.clone()).await {
                    tracing::error!(
                        "starrocks rollback transaction error: {:?}, txn label: {}",
                        e.as_report(),
                        txn_label
                    );
                }
            });
        }
    }
}

#[async_trait]
impl SinkWriter for StarrocksSinkWriter {
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        // We check whether start a new transaction in `write_batch`. Therefore, if no data has been written
        // within the `commit_checkpoint_interval` period, no meta requests will be made. Otherwise if we request
        // `prepare` against an empty transaction, the `StarRocks` will report a `hasn't send any data yet` error.
        if self.curr_txn_label.is_none() {
            let txn_label = self.new_txn_label();
            tracing::debug!(?txn_label, "begin transaction");
            let txn_label_res = self.txn_client.begin(txn_label.clone()).await?;
            if txn_label != txn_label_res {
                return Err(SinkError::Starrocks(format!(
                    "label {} returned from StarRocks {} differs from generated one",
                    txn_label, txn_label_res
                )));
            }
            self.curr_txn_label = Some(txn_label);
        }
        if self.is_append_only {
            self.append_only(chunk).await
        } else {
            self.upsert(chunk).await
        }
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<()> {
        // Here we finish the `/api/transaction/load` request when a barrier is received. Therefore,
        // one or more load requests should be made within one commit_checkpoint_interval period.
        // StarRocks will take care of merging those splits into a larger one during prepare transaction.
        // Thus, only one version will be produced when the transaction is committed. See Stream Load
        // transaction interface for more information.
        self.finish_load_request().await?;

        if is_checkpoint
            && let Some(txn_label) = self.curr_txn_label.take()
            && let Err(err) = self.prepare_and_commit(txn_label.clone()).await
        {
            match self.txn_client.rollback(txn_label.clone()).await {
                Ok(_) => tracing::warn!(
                    ?txn_label,
                    "transaction is successfully rolled back due to commit failure"
                ),
                Err(err) => {
                    tracing::warn!(?txn_label, error = ?err.as_report(), "Couldn't roll back transaction after commit failed")
                }
            }

            return Err(err);
        }
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        if let Some(txn_label) = self.curr_txn_label.take() {
            tracing::debug!(?txn_label, "rollback transaction");
            self.txn_client.rollback(txn_label).await?;
        }
        Ok(())
    }
}

pub struct StarrocksSchemaClient {
    table: String,
    db: String,
    conn: mysql_async::Conn,
}

impl StarrocksSchemaClient {
    pub async fn new(
        host: String,
        port: String,
        table: String,
        db: String,
        user: String,
        password: String,
    ) -> Result<Self> {
        // username & password may contain special chars, so we need to do URL encoding on them.
        // Otherwise, Opts::from_url may report a `Parse error`
        let user = form_urlencoded::byte_serialize(user.as_bytes()).collect::<String>();
        let password = form_urlencoded::byte_serialize(password.as_bytes()).collect::<String>();

        let conn_uri = format!(
            "mysql://{}:{}@{}:{}/{}?prefer_socket={}&max_allowed_packet={}&wait_timeout={}",
            user,
            password,
            host,
            port,
            db,
            STARROCK_MYSQL_PREFER_SOCKET,
            STARROCK_MYSQL_MAX_ALLOWED_PACKET,
            STARROCK_MYSQL_WAIT_TIMEOUT
        );
        let pool = mysql_async::Pool::new(
            Opts::from_url(&conn_uri)
                .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?,
        );
        let conn = pool
            .get_conn()
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?;

        Ok(Self { table, db, conn })
    }

    pub async fn get_columns_from_starrocks(&mut self) -> Result<HashMap<String, String>> {
        let query = format!(
            "select column_name, column_type from information_schema.columns where table_name = {:?} and table_schema = {:?};",
            self.table, self.db
        );
        let mut query_map: HashMap<String, String> = HashMap::default();
        self.conn
            .query_map(query, |(column_name, column_type)| {
                query_map.insert(column_name, column_type)
            })
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?;
        Ok(query_map)
    }

    pub async fn get_pk_from_starrocks(&mut self) -> Result<(String, String)> {
        let query = format!(
            "select table_model, primary_key, sort_key from information_schema.tables_config where table_name = {:?} and table_schema = {:?};",
            self.table, self.db
        );
        let table_mode_pk: (String, String) = self
            .conn
            .query_map(
                query,
                |(table_model, primary_key, sort_key): (String, String, String)| match table_model
                    .as_str()
                {
                    // Get primary key of aggregate table from the sort_key field
                    // https://docs.starrocks.io/docs/table_design/table_types/table_capabilities/
                    // https://docs.starrocks.io/docs/sql-reference/information_schema/tables_config/
                    "AGG_KEYS" => (table_model, sort_key),
                    _ => (table_model, primary_key),
                },
            )
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?
            .first()
            .ok_or_else(|| {
                SinkError::Starrocks(format!(
                    "Can't find schema for StarRocks table {:?} in database {:?}. Please check that the table exists and the StarRocks user has write permission on the target table.",
                    self.table, self.db
                ))
            })?
            .clone();
        Ok(table_mode_pk)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StarrocksInsertResultResponse {
    #[serde(rename = "TxnId")]
    pub txn_id: Option<i64>,
    #[serde(rename = "Seq")]
    pub seq: Option<i64>,
    #[serde(rename = "Label")]
    pub label: Option<String>,
    #[serde(rename = "Status")]
    pub status: String,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "NumberTotalRows")]
    pub number_total_rows: Option<i64>,
    #[serde(rename = "NumberLoadedRows")]
    pub number_loaded_rows: Option<i64>,
    #[serde(rename = "NumberFilteredRows")]
    pub number_filtered_rows: Option<i32>,
    #[serde(rename = "NumberUnselectedRows")]
    pub number_unselected_rows: Option<i32>,
    #[serde(rename = "LoadBytes")]
    pub load_bytes: Option<i64>,
    #[serde(rename = "LoadTimeMs")]
    pub load_time_ms: Option<i32>,
    #[serde(rename = "BeginTxnTimeMs")]
    pub begin_txn_time_ms: Option<i32>,
    #[serde(rename = "ReadDataTimeMs")]
    pub read_data_time_ms: Option<i32>,
    #[serde(rename = "WriteDataTimeMs")]
    pub write_data_time_ms: Option<i32>,
    #[serde(rename = "CommitAndPublishTimeMs")]
    pub commit_and_publish_time_ms: Option<i32>,
    #[serde(rename = "StreamLoadPlanTimeMs")]
    pub stream_load_plan_time_ms: Option<i32>,
    #[serde(rename = "ExistingJobStatus")]
    pub existing_job_status: Option<String>,
    #[serde(rename = "ErrorURL")]
    pub error_url: Option<String>,
}

pub struct StarrocksClient {
    insert: InserterInner,
}
impl StarrocksClient {
    pub fn new(insert: InserterInner) -> Self {
        Self { insert }
    }

    pub async fn write(&mut self, data: Bytes) -> Result<()> {
        self.insert.write(data).await?;
        Ok(())
    }

    pub async fn finish(self) -> Result<StarrocksInsertResultResponse> {
        let raw = self.insert.finish().await?;
        let res: StarrocksInsertResultResponse = serde_json::from_slice(&raw)
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?;

        if !STARROCKS_SUCCESS_STATUS.contains(&res.status.as_str()) {
            return Err(SinkError::DorisStarrocksConnect(anyhow::anyhow!(
                "Insert error: {}, {}, {:?}",
                res.status,
                res.message,
                res.error_url,
            )));
        };
        Ok(res)
    }
}

pub struct StarrocksTxnClient {
    request_builder: StarrocksTxnRequestBuilder,
}

impl StarrocksTxnClient {
    pub fn new(request_builder: StarrocksTxnRequestBuilder) -> Self {
        Self { request_builder }
    }

    fn check_response_and_extract_label(&self, res: Bytes) -> Result<String> {
        let res: StarrocksInsertResultResponse = serde_json::from_slice(&res)
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?;
        if !STARROCKS_SUCCESS_STATUS.contains(&res.status.as_str()) {
            return Err(SinkError::DorisStarrocksConnect(anyhow::anyhow!(
                "transaction error: {}, {}, {:?}",
                res.status,
                res.message,
                res.error_url,
            )));
        }
        res.label.ok_or_else(|| {
            SinkError::DorisStarrocksConnect(anyhow::anyhow!("Can't get label from response"))
        })
    }

    pub async fn begin(&self, label: String) -> Result<String> {
        let res = self
            .request_builder
            .build_begin_request_sender(label)?
            .send()
            .await?;
        self.check_response_and_extract_label(res)
    }

    pub async fn prepare(&self, label: String) -> Result<String> {
        let res = self
            .request_builder
            .build_prepare_request_sender(label)?
            .send()
            .await?;
        self.check_response_and_extract_label(res)
    }

    pub async fn commit(&self, label: String) -> Result<String> {
        let res = self
            .request_builder
            .build_commit_request_sender(label)?
            .send()
            .await?;
        self.check_response_and_extract_label(res)
    }

    pub async fn rollback(&self, label: String) -> Result<String> {
        let res = self
            .request_builder
            .build_rollback_request_sender(label)?
            .send()
            .await?;
        self.check_response_and_extract_label(res)
    }

    pub async fn load(&self, label: String) -> Result<InserterInner> {
        self.request_builder.build_txn_inserter(label).await
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;

    use super::*;

    fn is_compatible(rw_data_type: DataType, starrocks_data_type: &str) -> bool {
        StarrocksSink::check_and_correct_column_type(&rw_data_type, starrocks_data_type).unwrap()
    }

    #[test]
    fn test_timestamptz_compatible_starrocks_types() {
        for starrocks_data_type in [
            "datetime",
            "datetime(6)",
            "varchar(64)",
            "char(32)",
            "string",
        ] {
            assert!(
                is_compatible(DataType::Timestamptz, starrocks_data_type),
                "{starrocks_data_type} should be compatible with timestamptz"
            );
        }
    }

    #[test]
    fn test_timestamptz_incompatible_starrocks_types() {
        for starrocks_data_type in ["date", "int", "bigint", "json", "boolean"] {
            assert!(
                !is_compatible(DataType::Timestamptz, starrocks_data_type),
                "{starrocks_data_type} should not be compatible with timestamptz"
            );
        }
    }

    fn base_properties() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("starrocks.host".to_owned(), "127.0.0.1".to_owned()),
            ("starrocks.mysqlport".to_owned(), "9030".to_owned()),
            ("starrocks.httpport".to_owned(), "8030".to_owned()),
            ("starrocks.user".to_owned(), "root".to_owned()),
            ("starrocks.password".to_owned(), "".to_owned()),
            ("starrocks.database".to_owned(), "demo".to_owned()),
            ("starrocks.table".to_owned(), "sink_table".to_owned()),
            ("type".to_owned(), SINK_TYPE_APPEND_ONLY.to_owned()),
        ])
    }

    #[test]
    fn starrocks_max_batch_size_bytes_defaults_to_none() {
        let config = StarrocksConfig::from_btreemap(base_properties()).unwrap();

        assert_eq!(config.max_batch_size_bytes, None);
    }

    #[test]
    fn starrocks_max_batch_size_bytes_parses() {
        let mut properties = base_properties();
        properties.insert("starrocks.max_batch_size_bytes".to_owned(), "2".to_owned());

        let config = StarrocksConfig::from_btreemap(properties).unwrap();

        assert_eq!(config.max_batch_size_bytes, Some(2));
    }

    #[test]
    fn starrocks_max_batch_size_bytes_rejects_zero() {
        let mut properties = base_properties();
        properties.insert("starrocks.max_batch_size_bytes".to_owned(), "0".to_owned());

        let err = StarrocksConfig::from_btreemap(properties).unwrap_err();

        assert!(
            err.to_string()
                .contains("`starrocks.max_batch_size_bytes` must be greater than 0")
        );
    }

    #[test]
    fn starrocks_batch_size_allows_exact_limit() {
        assert_eq!(
            decide_load_request_size(3, 2, 5).unwrap(),
            LoadRequestSizeDecision {
                finish_current_load: false,
                next_batch_size_bytes: 5,
            }
        );
    }

    #[test]
    fn starrocks_batch_size_rolls_over_before_exceeding_limit() {
        assert_eq!(
            decide_load_request_size(4, 2, 5).unwrap(),
            LoadRequestSizeDecision {
                finish_current_load: true,
                next_batch_size_bytes: 2,
            }
        );
    }

    #[test]
    fn starrocks_batch_size_rejects_single_oversized_row() {
        let err = decide_load_request_size(0, 6, 5).unwrap_err();

        assert!(err.to_string().contains(
            "single row payload size 6 bytes exceeds `starrocks.max_batch_size_bytes` limit 5 bytes"
        ));
    }

    #[test]
    fn starrocks_batch_size_rolls_over_on_u64_overflow() {
        assert_eq!(
            decide_load_request_size(u64::MAX, 1, u64::MAX).unwrap(),
            LoadRequestSizeDecision {
                finish_current_load: true,
                next_batch_size_bytes: 1,
            }
        );
    }
}
