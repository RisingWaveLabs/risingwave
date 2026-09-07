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

use core::fmt::Debug;
use core::num::NonZeroU64;
use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::anyhow;
use clickhouse::insert::Insert;
use clickhouse::{Client as ClickHouseClient, Row as ClickHouseRow};
use itertools::Itertools;
use phf::{Set, phf_set};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::{FieldLike, Schema};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, Decimal, ScalarRefImpl, Serial};
use risingwave_common::util::iter_util::ZipEqDebug;
use serde::ser::{SerializeSeq, SerializeStruct};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use thiserror_ext::AsReport;
use tonic::async_trait;
use tracing::warn;
use with_options::WithOptions;

use super::decouple_checkpoint_log_sink::{
    DecoupleCheckpointLogSinkerOf, default_commit_checkpoint_interval,
};
use super::writer::SinkWriter;
use super::{SinkWriterMetrics, SinkWriterParam};
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;
use crate::sink::{
    Result, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, Sink, SinkError, SinkParam,
};

const QUERY_ENGINE: &str =
    "select distinct ?fields from system.tables where database = ? and name = ?";
const QUERY_COLUMN: &str =
    "select distinct ?fields from system.columns where database = ? and table = ? order by ?";
pub const CLICKHOUSE_SINK: &str = "clickhouse";

const ALLOW_EXPERIMENTAL_JSON_TYPE: &str = "allow_experimental_json_type";
const INPUT_FORMAT_BINARY_READ_JSON_AS_STRING: &str = "input_format_binary_read_json_as_string";
const OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING: &str = "output_format_binary_write_json_as_string";

/// `ClickHouse` backs `Decimal(P, S)` with `Decimal256` once `P > 38`, which takes 32 bytes on the
/// wire, while [`ClickHouseDecimal`] never writes more than 16.
const MAX_ENCODABLE_DECIMAL_PRECISION: u8 = 38;

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct ClickHouseCommon {
    #[serde(rename = "clickhouse.url")]
    pub url: String,
    #[serde(rename = "clickhouse.user")]
    #[with_option(allow_alter_on_fly)]
    pub user: String,
    #[serde(rename = "clickhouse.password")]
    #[with_option(allow_alter_on_fly)]
    pub password: String,
    #[serde(rename = "clickhouse.database")]
    pub database: String,
    #[serde(rename = "clickhouse.table")]
    pub table: String,
    #[serde(rename = "clickhouse.delete.column")]
    pub delete_column: Option<String>,
    /// Commit every n(>0) checkpoints, default is 10.
    #[serde(default = "default_commit_checkpoint_interval")]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    pub commit_checkpoint_interval: u64,
}

impl EnforceSecret for ClickHouseCommon {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "clickhouse.password", "clickhouse.user"
    };
}

#[derive(Debug)]
enum ClickHouseEngine {
    MergeTree,
    ReplacingMergeTree(Option<String>),
    SummingMergeTree,
    AggregatingMergeTree,
    CollapsingMergeTree(String),
    VersionedCollapsingMergeTree(String),
    GraphiteMergeTree,
    ReplicatedMergeTree,
    ReplicatedReplacingMergeTree(Option<String>),
    ReplicatedSummingMergeTree,
    ReplicatedAggregatingMergeTree,
    ReplicatedCollapsingMergeTree(String),
    ReplicatedVersionedCollapsingMergeTree(String),
    ReplicatedGraphiteMergeTree,
    SharedMergeTree,
    SharedReplacingMergeTree(Option<String>),
    SharedSummingMergeTree,
    SharedAggregatingMergeTree,
    SharedCollapsingMergeTree(String),
    SharedVersionedCollapsingMergeTree(String),
    SharedGraphiteMergeTree,
    Null,
}
impl ClickHouseEngine {
    pub fn is_collapsing_engine(&self) -> bool {
        matches!(
            self,
            ClickHouseEngine::CollapsingMergeTree(_)
                | ClickHouseEngine::VersionedCollapsingMergeTree(_)
                | ClickHouseEngine::ReplicatedCollapsingMergeTree(_)
                | ClickHouseEngine::ReplicatedVersionedCollapsingMergeTree(_)
                | ClickHouseEngine::SharedCollapsingMergeTree(_)
                | ClickHouseEngine::SharedVersionedCollapsingMergeTree(_)
        )
    }

    pub fn is_delete_replacing_engine(&self) -> bool {
        match self {
            ClickHouseEngine::ReplacingMergeTree(delete_col) => delete_col.is_some(),
            ClickHouseEngine::ReplicatedReplacingMergeTree(delete_col) => delete_col.is_some(),
            ClickHouseEngine::SharedReplacingMergeTree(delete_col) => delete_col.is_some(),
            _ => false,
        }
    }

    pub fn get_delete_col(&self) -> Option<String> {
        match self {
            ClickHouseEngine::ReplacingMergeTree(Some(delete_col)) => Some(delete_col.clone()),
            ClickHouseEngine::ReplicatedReplacingMergeTree(Some(delete_col)) => {
                Some(delete_col.clone())
            }
            ClickHouseEngine::SharedReplacingMergeTree(Some(delete_col)) => {
                Some(delete_col.clone())
            }
            _ => None,
        }
    }

    pub fn get_sign_name(&self) -> Option<String> {
        match self {
            ClickHouseEngine::CollapsingMergeTree(sign_name) => Some(sign_name.clone()),
            ClickHouseEngine::VersionedCollapsingMergeTree(sign_name) => Some(sign_name.clone()),
            ClickHouseEngine::ReplicatedCollapsingMergeTree(sign_name) => Some(sign_name.clone()),
            ClickHouseEngine::ReplicatedVersionedCollapsingMergeTree(sign_name) => {
                Some(sign_name.clone())
            }
            ClickHouseEngine::SharedCollapsingMergeTree(sign_name) => Some(sign_name.clone()),
            ClickHouseEngine::SharedVersionedCollapsingMergeTree(sign_name) => {
                Some(sign_name.clone())
            }
            _ => None,
        }
    }

    pub fn is_shared_tree(&self) -> bool {
        matches!(
            self,
            ClickHouseEngine::SharedMergeTree
                | ClickHouseEngine::SharedReplacingMergeTree(_)
                | ClickHouseEngine::SharedSummingMergeTree
                | ClickHouseEngine::SharedAggregatingMergeTree
                | ClickHouseEngine::SharedCollapsingMergeTree(_)
                | ClickHouseEngine::SharedVersionedCollapsingMergeTree(_)
                | ClickHouseEngine::SharedGraphiteMergeTree
        )
    }

    pub fn from_query_engine(
        engine_name: &ClickhouseQueryEngine,
        config: &ClickHouseConfig,
    ) -> Result<Self> {
        match engine_name.engine.as_str() {
            "MergeTree" => Ok(ClickHouseEngine::MergeTree),
            "Null" => Ok(ClickHouseEngine::Null),
            "ReplacingMergeTree" => {
                let delete_column = config.common.delete_column.clone();
                Ok(ClickHouseEngine::ReplacingMergeTree(delete_column))
            }
            "SummingMergeTree" => Ok(ClickHouseEngine::SummingMergeTree),
            "AggregatingMergeTree" => Ok(ClickHouseEngine::AggregatingMergeTree),
            // VersionedCollapsingMergeTree(sign_name,"a")
            "VersionedCollapsingMergeTree" => {
                let sign_name = engine_name
                    .create_table_query
                    .split("VersionedCollapsingMergeTree(")
                    .last()
                    .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                    .split(',')
                    .next()
                    .ok_or_else(|| SinkError::ClickHouse("must have next".to_owned()))?
                    .trim()
                    .to_owned();
                Ok(ClickHouseEngine::VersionedCollapsingMergeTree(sign_name))
            }
            // CollapsingMergeTree(sign_name)
            "CollapsingMergeTree" => {
                let sign_name = engine_name
                    .create_table_query
                    .split("CollapsingMergeTree(")
                    .last()
                    .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                    .split(')')
                    .next()
                    .ok_or_else(|| SinkError::ClickHouse("must have next".to_owned()))?
                    .trim()
                    .to_owned();
                Ok(ClickHouseEngine::CollapsingMergeTree(sign_name))
            }
            "GraphiteMergeTree" => Ok(ClickHouseEngine::GraphiteMergeTree),
            "ReplicatedMergeTree" => Ok(ClickHouseEngine::ReplicatedMergeTree),
            "ReplicatedReplacingMergeTree" => {
                let delete_column = config.common.delete_column.clone();
                Ok(ClickHouseEngine::ReplicatedReplacingMergeTree(
                    delete_column,
                ))
            }
            "ReplicatedSummingMergeTree" => Ok(ClickHouseEngine::ReplicatedSummingMergeTree),
            "ReplicatedAggregatingMergeTree" => {
                Ok(ClickHouseEngine::ReplicatedAggregatingMergeTree)
            }
            // ReplicatedVersionedCollapsingMergeTree("a","b",sign_name,"c")
            "ReplicatedVersionedCollapsingMergeTree" => {
                let sign_name = engine_name
                    .create_table_query
                    .split("ReplicatedVersionedCollapsingMergeTree(")
                    .last()
                    .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                    .split(',')
                    .rev()
                    .nth(1)
                    .ok_or_else(|| SinkError::ClickHouse("must have index 1".to_owned()))?
                    .trim()
                    .to_owned();
                Ok(ClickHouseEngine::ReplicatedVersionedCollapsingMergeTree(
                    sign_name,
                ))
            }
            // ReplicatedCollapsingMergeTree("a","b",sign_name)
            "ReplicatedCollapsingMergeTree" => {
                let sign_name = engine_name
                    .create_table_query
                    .split("ReplicatedCollapsingMergeTree(")
                    .last()
                    .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                    .split(')')
                    .next()
                    .ok_or_else(|| SinkError::ClickHouse("must have next".to_owned()))?
                    .split(',')
                    .next_back()
                    .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                    .trim()
                    .to_owned();
                Ok(ClickHouseEngine::ReplicatedCollapsingMergeTree(sign_name))
            }
            "ReplicatedGraphiteMergeTree" => Ok(ClickHouseEngine::ReplicatedGraphiteMergeTree),
            "SharedMergeTree" => Ok(ClickHouseEngine::SharedMergeTree),
            "SharedReplacingMergeTree" => {
                let delete_column = config.common.delete_column.clone();
                Ok(ClickHouseEngine::SharedReplacingMergeTree(delete_column))
            }
            "SharedSummingMergeTree" => Ok(ClickHouseEngine::SharedSummingMergeTree),
            "SharedAggregatingMergeTree" => Ok(ClickHouseEngine::SharedAggregatingMergeTree),
            // SharedVersionedCollapsingMergeTree("a","b",sign_name,"c")
            "SharedVersionedCollapsingMergeTree" => {
                let sign_name = engine_name
                    .create_table_query
                    .split("SharedVersionedCollapsingMergeTree(")
                    .last()
                    .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                    .split(',')
                    .rev()
                    .nth(1)
                    .ok_or_else(|| SinkError::ClickHouse("must have index 1".to_owned()))?
                    .trim()
                    .to_owned();
                Ok(ClickHouseEngine::SharedVersionedCollapsingMergeTree(
                    sign_name,
                ))
            }
            // SharedCollapsingMergeTree("a","b",sign_name)
            "SharedCollapsingMergeTree" => {
                let sign_name = engine_name
                    .create_table_query
                    .split("SharedCollapsingMergeTree(")
                    .last()
                    .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                    .split(')')
                    .next()
                    .ok_or_else(|| SinkError::ClickHouse("must have next".to_owned()))?
                    .split(',')
                    .next_back()
                    .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                    .trim()
                    .to_owned();
                Ok(ClickHouseEngine::SharedCollapsingMergeTree(sign_name))
            }
            "SharedGraphiteMergeTree" => Ok(ClickHouseEngine::SharedGraphiteMergeTree),
            _ => Err(SinkError::ClickHouse(format!(
                "Cannot find clickhouse engine {:?}",
                engine_name.engine
            ))),
        }
    }
}

impl ClickHouseCommon {
    pub(crate) fn build_client(&self) -> ConnectorResult<ClickHouseClient> {
        let client = ClickHouseClient::default() // hyper(0.14) client inside
            .with_url(&self.url)
            .with_user(&self.user)
            .with_password(&self.password)
            .with_database(&self.database)
            .with_option(ALLOW_EXPERIMENTAL_JSON_TYPE, "1")
            .with_option(INPUT_FORMAT_BINARY_READ_JSON_AS_STRING, "1")
            .with_option(OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING, "1");
        Ok(client)
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct ClickHouseConfig {
    #[serde(flatten)]
    pub common: ClickHouseCommon,

    pub r#type: String, // accept "append-only" or "upsert"

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(ClickHouseConfig);

impl EnforceSecret for ClickHouseConfig {
    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        ClickHouseCommon::enforce_one(prop)
    }

    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            ClickHouseCommon::enforce_one(prop)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ClickHouseSink {
    pub config: ClickHouseConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl EnforceSecret for ClickHouseSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            ClickHouseConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl ClickHouseConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<ClickHouseConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }
}

impl TryFrom<SinkParam> for ClickHouseSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = ClickHouseConfig::from_btreemap(param.properties)?;
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

impl ClickHouseSink {
    /// Check that the column names and types of risingwave and clickhouse are identical
    fn check_column_name_and_type(&self, clickhouse_columns_desc: &[SystemColumn]) -> Result<()> {
        let rw_fields_name = build_fields_name_type_from_schema(&self.schema)?;
        let clickhouse_columns_desc: HashMap<String, SystemColumn> = clickhouse_columns_desc
            .iter()
            .map(|s| (s.name.clone(), s.clone()))
            .collect();

        if rw_fields_name.len().gt(&clickhouse_columns_desc.len()) {
            return Err(SinkError::ClickHouse("The columns of the sink must be equal to or a superset of the target table's columns.".to_owned()));
        }

        for i in rw_fields_name {
            let value = clickhouse_columns_desc.get(&i.0).ok_or_else(|| {
                SinkError::ClickHouse(format!(
                    "Column name don't find in clickhouse, risingwave is {:?} ",
                    i.0
                ))
            })?;

            Self::check_and_correct_column_type(&i.1, value)?;
        }
        Ok(())
    }

    /// Check that the column names and types of risingwave and clickhouse are identical
    fn check_pk_match(&self, clickhouse_columns_desc: &[SystemColumn]) -> Result<()> {
        let mut clickhouse_pks: HashSet<String> = clickhouse_columns_desc
            .iter()
            .filter(|s| s.is_in_primary_key == 1)
            .map(|s| s.name.clone())
            .collect();

        for (_, field) in self
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, _)| self.pk_indices.contains(index))
        {
            if !clickhouse_pks.remove(&field.name) {
                return Err(SinkError::ClickHouse(
                    "Clicklhouse and RisingWave pk is not match".to_owned(),
                ));
            }
        }

        if !clickhouse_pks.is_empty() {
            return Err(SinkError::ClickHouse(
                "Clicklhouse and RisingWave pk is not match".to_owned(),
            ));
        }
        Ok(())
    }

    /// Check that the column types of risingwave and clickhouse are identical
    fn check_and_correct_column_type(
        fields_type: &DataType,
        ck_column: &SystemColumn,
    ) -> Result<()> {
        // FIXME: the "contains" based implementation is wrong
        let is_match = match fields_type {
            risingwave_common::types::DataType::Boolean => Ok(ck_column.r#type.contains("Bool")),
            risingwave_common::types::DataType::Int16 => Ok(ck_column.r#type.contains("UInt16")
                | ck_column.r#type.contains("Int16")
                // Allow Int16 to be pushed to Enum16, they share an encoding and value range
                // No special care is taken to ensure values are valid.
                | ck_column.r#type.contains("Enum16")),
            risingwave_common::types::DataType::Int32 => {
                Ok(ck_column.r#type.contains("UInt32") | ck_column.r#type.contains("Int32"))
            }
            risingwave_common::types::DataType::Int64 => {
                Ok(ck_column.r#type.contains("UInt64") | ck_column.r#type.contains("Int64"))
            }
            risingwave_common::types::DataType::Float32 => Ok(ck_column.r#type.contains("Float32")),
            risingwave_common::types::DataType::Float64 => Ok(ck_column.r#type.contains("Float64")),
            risingwave_common::types::DataType::Decimal => {
                match parse_decimal_accuracy(&ck_column.r#type)? {
                    Some((precision, _)) if precision > MAX_ENCODABLE_DECIMAL_PRECISION => {
                        Err(SinkError::ClickHouse(format!(
                            "column {:?} has ClickHouse type `{}`: precision {} is backed by Decimal256, \
                             but this sink can only encode decimals up to precision {}. Lower the column \
                             precision (recreating the table if it is a key column), or leave the column \
                             out of the sink.",
                            ck_column.name,
                            ck_column.r#type,
                            precision,
                            MAX_ENCODABLE_DECIMAL_PRECISION,
                        )))
                    }
                    accuracy => Ok(accuracy.is_some()),
                }
            }
            risingwave_common::types::DataType::Date => Ok(ck_column.r#type.contains("Date32")),
            risingwave_common::types::DataType::Varchar => Ok(ck_column.r#type.contains("String")),
            risingwave_common::types::DataType::Time => Err(SinkError::ClickHouse(
                "TIME is not supported for ClickHouse sink. Please convert to VARCHAR or other supported types.".to_owned(),
            )),
            risingwave_common::types::DataType::Timestamp => Err(SinkError::ClickHouse(
                "clickhouse does not have a type corresponding to naive timestamp".to_owned(),
            )),
            risingwave_common::types::DataType::Timestamptz => {
                Ok(ck_column.r#type.contains("DateTime64"))
            }
            risingwave_common::types::DataType::Interval => Err(SinkError::ClickHouse(
                "INTERVAL is not supported for ClickHouse sink. Please convert to VARCHAR or other supported types.".to_owned(),
            )),
            risingwave_common::types::DataType::Struct(_) => Err(SinkError::ClickHouse(
                "struct needs to be converted into a list".to_owned(),
            )),
            risingwave_common::types::DataType::List(list) => {
                Self::check_and_correct_column_type(list.elem(), ck_column)?;
                Ok(ck_column.r#type.contains("Array"))
            }
            risingwave_common::types::DataType::Bytea => Err(SinkError::ClickHouse(
                "BYTEA is not supported for ClickHouse sink. Please convert to VARCHAR or other supported types.".to_owned(),
            )),
            risingwave_common::types::DataType::Jsonb => Ok(ck_column.r#type.contains("JSON")),
            risingwave_common::types::DataType::Variant => Err(SinkError::ClickHouse(
                "VARIANT is not supported for ClickHouse sink.".to_owned(),
            )),
            risingwave_common::types::DataType::Serial => {
                Ok(ck_column.r#type.contains("UInt64") | ck_column.r#type.contains("Int64"))
            }
            risingwave_common::types::DataType::Int256 => Err(SinkError::ClickHouse(
                "INT256 is not supported for ClickHouse sink.".to_owned(),
            )),
            risingwave_common::types::DataType::Map(_) => Err(SinkError::ClickHouse(
                "MAP is not supported for ClickHouse sink.".to_owned(),
            )),
            DataType::Vector(_) => Err(SinkError::ClickHouse(
                "VECTOR is not supported for ClickHouse sink.".to_owned(),
            )),
        };
        if !is_match? {
            return Err(SinkError::ClickHouse(format!(
                "Column type mismatch for column {:?}: RisingWave type is {:?}, ClickHouse type is {:?}",
                ck_column.name, fields_type, ck_column.r#type
            )));
        }

        Ok(())
    }
}

impl Sink for ClickHouseSink {
    type LogSinker = DecoupleCheckpointLogSinkerOf<ClickHouseSinkWriter>;

    const SINK_NAME: &'static str = CLICKHOUSE_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn validate(&self) -> Result<()> {
        // For upsert clickhouse sink, the primary key must be defined.
        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Primary key not defined for upsert clickhouse sink (please define in `primary_key` field)"
            )));
        }

        // check reachability
        let client = self.config.common.build_client()?;

        let (clickhouse_column, clickhouse_engine) =
            query_column_engine_from_ck(client, &self.config).await?;
        if clickhouse_engine.is_shared_tree() {
            risingwave_common::license::Feature::ClickHouseSharedEngine
                .check_available()
                .map_err(|e| anyhow::anyhow!(e))?;
        }

        if !self.is_append_only
            && !clickhouse_engine.is_collapsing_engine()
            && !clickhouse_engine.is_delete_replacing_engine()
        {
            return match clickhouse_engine {
                ClickHouseEngine::ReplicatedReplacingMergeTree(None) | ClickHouseEngine::ReplacingMergeTree(None) | ClickHouseEngine::SharedReplacingMergeTree(None) =>  {
                    Err(SinkError::ClickHouse("To enable upsert with a `ReplacingMergeTree`, you must set a `clickhouse.delete.column` to the UInt8 column in ClickHouse used to signify deletes. See https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree#is_deleted for more information".to_owned()))
                }
                _ => Err(SinkError::ClickHouse("If you want to use upsert, please use either `VersionedCollapsingMergeTree`, `CollapsingMergeTree` or the `ReplacingMergeTree` in ClickHouse".to_owned()))
            };
        }

        self.check_column_name_and_type(&clickhouse_column)?;
        if !self.is_append_only {
            self.check_pk_match(&clickhouse_column)?;
        }

        if self.config.common.commit_checkpoint_interval == 0 {
            return Err(SinkError::Config(anyhow!(
                "`commit_checkpoint_interval` must be greater than 0"
            )));
        }
        Ok(())
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        ClickHouseConfig::from_btreemap(config.clone())?;
        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let writer = ClickHouseSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await?;
        let commit_checkpoint_interval =
    NonZeroU64::new(self.config.common.commit_checkpoint_interval).expect(
        "commit_checkpoint_interval should be greater than 0, and it should be checked in config validation",
    );

        Ok(DecoupleCheckpointLogSinkerOf::new(
            writer,
            SinkWriterMetrics::new(&writer_param),
            commit_checkpoint_interval,
        ))
    }
}
pub struct ClickHouseSinkWriter {
    pub config: ClickHouseConfig,
    schema: Schema,
    #[expect(dead_code)]
    pk_indices: Vec<usize>,
    client: ClickHouseClient,
    #[expect(dead_code)]
    is_append_only: bool,
    // Save some features of the clickhouse column type
    column_correct_map: HashMap<String, ClickHouseSchemaFeature>,
    rw_fields_name_after_calibration: Vec<String>,
    clickhouse_engine: ClickHouseEngine,
    inserter: Option<Insert<ClickHouseColumn>>,
}
#[derive(Debug)]
struct ClickHouseSchemaFeature {
    can_null: bool,
    // Time accuracy in clickhouse for rw and ck conversions
    accuracy_time: u8,

    accuracy_decimal: (u8, u8),
}

impl ClickHouseSinkWriter {
    pub async fn new(
        config: ClickHouseConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let client = config.common.build_client()?;

        let (clickhouse_column, clickhouse_engine) =
            query_column_engine_from_ck(client.clone(), &config).await?;

        let column_correct_map: Result<HashMap<String, ClickHouseSchemaFeature>> =
            clickhouse_column
                .iter()
                .map(Self::build_column_correct_map)
                .collect();
        let sink_fields = build_fields_name_type_from_schema(&schema)?;

        // The table may have been altered since `CREATE SINK` validated it, and an under-sized
        // decimal encoding would desync every following column of the same RowBinary row. Only
        // columns the writer encodes as decimals are held to the decimal rule.
        let ck_columns: HashMap<&str, &SystemColumn> = clickhouse_column
            .iter()
            .map(|c| (c.name.as_str(), c))
            .collect();
        for (name, data_type) in sink_fields.iter().filter(|(_, t)| contains_decimal(t)) {
            if let Some(ck_column) = ck_columns.get(name.as_str()) {
                ClickHouseSink::check_and_correct_column_type(data_type, ck_column)?;
            }
        }

        let mut rw_fields_name_after_calibration =
            sink_fields.iter().map(|(a, _)| a.clone()).collect_vec();

        if let Some(sign) = clickhouse_engine.get_sign_name() {
            rw_fields_name_after_calibration.push(sign);
        }
        if let Some(delete_col) = clickhouse_engine.get_delete_col() {
            rw_fields_name_after_calibration.push(delete_col);
        }
        Ok(Self {
            config,
            schema,
            pk_indices,
            client,
            is_append_only,
            column_correct_map: column_correct_map?,
            rw_fields_name_after_calibration,
            clickhouse_engine,
            inserter: None,
        })
    }

    /// Check if clickhouse's column is 'Nullable', valid bits of `DateTime64`. And save it in
    /// `column_correct_map`
    fn build_column_correct_map(
        ck_column: &SystemColumn,
    ) -> Result<(String, ClickHouseSchemaFeature)> {
        let can_null = ck_column.r#type.contains("Nullable");
        // `DateTime64` without precision is already displayed as `DateTime(3)` in `system.columns`.
        let accuracy_time = if ck_column.r#type.contains("DateTime64(") {
            ck_column
                .r#type
                .split("DateTime64(")
                .last()
                .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                .split(')')
                .next()
                .ok_or_else(|| SinkError::ClickHouse("must have next".to_owned()))?
                .split(',')
                .next()
                .ok_or_else(|| SinkError::ClickHouse("must have next".to_owned()))?
                .parse::<u8>()
                .map_err(|e| SinkError::ClickHouse(e.to_report_string()))?
        } else {
            0_u8
        };
        let accuracy_decimal = parse_decimal_accuracy(&ck_column.r#type)?.unwrap_or((0, 0));
        Ok((
            ck_column.name.clone(),
            ClickHouseSchemaFeature {
                can_null,
                accuracy_time,
                accuracy_decimal,
            },
        ))
    }

    async fn write(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.inserter.is_none() {
            self.inserter = Some(self.client.insert_with_fields_name(
                &self.config.common.table,
                self.rw_fields_name_after_calibration.clone(),
            )?);
        }
        for (op, row) in chunk.rows() {
            let mut clickhouse_filed_vec = vec![];
            for (data, field) in row.iter().zip_eq_debug(self.schema.fields()) {
                clickhouse_filed_vec.extend(ClickHouseFieldWithNull::from_scalar_ref(
                    data,
                    &self.column_correct_map,
                    field.name(),
                    &field.data_type(),
                )?);
            }
            match op {
                Op::Insert | Op::UpdateInsert => {
                    if self.clickhouse_engine.is_collapsing_engine() {
                        clickhouse_filed_vec.push(ClickHouseFieldWithNull::WithoutSome(
                            ClickHouseField::Int8(1),
                        ));
                    }
                    if self.clickhouse_engine.is_delete_replacing_engine() {
                        clickhouse_filed_vec.push(ClickHouseFieldWithNull::WithoutSome(
                            ClickHouseField::Int8(0),
                        ))
                    }
                }
                Op::Delete | Op::UpdateDelete => {
                    if !self.clickhouse_engine.is_collapsing_engine()
                        && !self.clickhouse_engine.is_delete_replacing_engine()
                    {
                        return Err(SinkError::ClickHouse(
                            "Clickhouse engine don't support upsert".to_owned(),
                        ));
                    }
                    if self.clickhouse_engine.is_collapsing_engine() {
                        clickhouse_filed_vec.push(ClickHouseFieldWithNull::WithoutSome(
                            ClickHouseField::Int8(-1),
                        ));
                    }
                    if self.clickhouse_engine.is_delete_replacing_engine() {
                        clickhouse_filed_vec.push(ClickHouseFieldWithNull::WithoutSome(
                            ClickHouseField::Int8(1),
                        ))
                    }
                }
            }
            let clickhouse_column = ClickHouseColumn {
                row: clickhouse_filed_vec,
            };
            self.inserter
                .as_mut()
                .unwrap()
                .write(&clickhouse_column)
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl SinkWriter for ClickHouseSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        self.write(chunk).await
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<()> {
        if is_checkpoint && let Some(inserter) = self.inserter.take() {
            inserter.end().await?;
        }
        Ok(())
    }
}

#[derive(ClickHouseRow, Deserialize, Clone)]
struct SystemColumn {
    name: String,
    r#type: String,
    is_in_primary_key: u8,
}

/// Parse `(precision, scale)` out of a decimal type, or `None` if it is not a decimal.
/// `system.columns` renders decimals canonically, so `Decimal128(10)` arrives as `Decimal(38, 10)`.
fn parse_decimal_accuracy(ck_type: &str) -> Result<Option<(u8, u8)>> {
    let Some((_, rest)) = ck_type.split_once("Decimal(") else {
        return Ok(None);
    };
    let (args, _) = rest
        .split_once(')')
        .ok_or_else(|| SinkError::ClickHouse(format!("unterminated decimal type {ck_type:?}")))?;
    let (precision, scale) = args
        .split_once(',')
        .ok_or_else(|| SinkError::ClickHouse(format!("decimal type {ck_type:?} has no scale")))?;
    let parse = |field: &str| {
        field.trim().parse::<u8>().map_err(|e| {
            SinkError::ClickHouse(format!(
                "cannot parse decimal type {:?}: {}",
                ck_type,
                e.to_report_string()
            ))
        })
    };
    Ok(Some((parse(precision)?, parse(scale)?)))
}

fn contains_decimal(data_type: &DataType) -> bool {
    match data_type {
        DataType::Decimal => true,
        DataType::List(list) => contains_decimal(list.elem()),
        _ => false,
    }
}

#[derive(ClickHouseRow, Deserialize)]
struct ClickhouseQueryEngine {
    #[expect(dead_code)]
    name: String,
    engine: String,
    create_table_query: String,
}

async fn query_column_engine_from_ck(
    client: ClickHouseClient,
    config: &ClickHouseConfig,
) -> Result<(Vec<SystemColumn>, ClickHouseEngine)> {
    let query_engine = QUERY_ENGINE;
    let query_column = QUERY_COLUMN;

    let clickhouse_engine = client
        .query(query_engine)
        .bind(config.common.database.clone())
        .bind(config.common.table.clone())
        .fetch_all::<ClickhouseQueryEngine>()
        .await?;
    let mut clickhouse_column = client
        .query(query_column)
        .bind(config.common.database.clone())
        .bind(config.common.table.clone())
        .bind("position")
        .fetch_all::<SystemColumn>()
        .await?;
    if clickhouse_engine.is_empty() || clickhouse_column.is_empty() {
        return Err(SinkError::ClickHouse(format!(
            "table {:?}.{:?} is not find in clickhouse",
            config.common.database, config.common.table
        )));
    }

    let clickhouse_engine =
        ClickHouseEngine::from_query_engine(clickhouse_engine.first().unwrap(), config)?;

    if let Some(sign) = &clickhouse_engine.get_sign_name() {
        clickhouse_column.retain(|a| sign.ne(&a.name))
    }

    if let Some(delete_col) = &clickhouse_engine.get_delete_col() {
        clickhouse_column.retain(|a| delete_col.ne(&a.name))
    }

    Ok((clickhouse_column, clickhouse_engine))
}

/// Serialize this structure to simulate the `struct` call clickhouse interface
#[derive(ClickHouseRow, Debug)]
struct ClickHouseColumn {
    row: Vec<ClickHouseFieldWithNull>,
}

/// Basic data types for use with the clickhouse interface
#[derive(Debug)]
enum ClickHouseField {
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Serial(Serial),
    Float32(f32),
    Float64(f64),
    String(String),
    Bool(bool),
    List(Vec<ClickHouseFieldWithNull>),
    Int8(i8),
    Decimal(ClickHouseDecimal),
}
#[derive(Debug)]
enum ClickHouseDecimal {
    Decimal32(i32),
    Decimal64(i64),
    Decimal128(i128),
}
impl Serialize for ClickHouseDecimal {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ClickHouseDecimal::Decimal32(v) => serializer.serialize_i32(*v),
            ClickHouseDecimal::Decimal64(v) => serializer.serialize_i64(*v),
            ClickHouseDecimal::Decimal128(v) => serializer.serialize_i128(*v),
        }
    }
}

/// Enum that support clickhouse nullable
#[derive(Debug)]
enum ClickHouseFieldWithNull {
    WithSome(ClickHouseField),
    WithoutSome(ClickHouseField),
    None,
}

impl ClickHouseFieldWithNull {
    pub fn from_scalar_ref(
        data: Option<ScalarRefImpl<'_>>,
        clickhouse_schema_feature_map: &HashMap<String, ClickHouseSchemaFeature>,
        rw_name: &str,
        rw_type: &DataType,
    ) -> Result<Vec<ClickHouseFieldWithNull>> {
        let clickhouse_schema_feature = if !matches!(rw_type, DataType::Struct(_)) {
            Some(clickhouse_schema_feature_map.get(rw_name).ok_or_else(|| {
                SinkError::ClickHouse(format!(
                    "No column found from clickhouse table schema, name is {rw_name}"
                ))
            })?)
        } else {
            None
        };

        let Some(data) = data else {
            let Some(clickhouse_schema_feature) = clickhouse_schema_feature else {
                return Err(SinkError::ClickHouse(
                    "clickhouse's nested can not insert null".to_owned(),
                ));
            };
            if !clickhouse_schema_feature.can_null {
                return Err(SinkError::ClickHouse(
                    "Cannot insert null value into non-nullable ClickHouse column".to_owned(),
                ));
            } else {
                return Ok(vec![ClickHouseFieldWithNull::None]);
            }
        };
        let data = match data {
            ScalarRefImpl::Int16(v) => ClickHouseField::Int16(v),
            ScalarRefImpl::Int32(v) => ClickHouseField::Int32(v),
            ScalarRefImpl::Int64(v) => ClickHouseField::Int64(v),
            ScalarRefImpl::Int256(_) => {
                return Err(SinkError::ClickHouse(
                    "INT256 is not supported for ClickHouse sink.".to_owned(),
                ));
            }
            ScalarRefImpl::Serial(v) => ClickHouseField::Serial(v),
            ScalarRefImpl::Float32(v) => ClickHouseField::Float32(v.into_inner()),
            ScalarRefImpl::Float64(v) => ClickHouseField::Float64(v.into_inner()),
            ScalarRefImpl::Utf8(v) => ClickHouseField::String(v.to_owned()),
            ScalarRefImpl::Bool(v) => ClickHouseField::Bool(v),
            ScalarRefImpl::Decimal(d) => {
                let d = if let Decimal::Normalized(d) = d {
                    let scale = clickhouse_schema_feature.unwrap().accuracy_decimal.1 as i32
                        - d.scale() as i32;
                    if scale < 0 {
                        d.mantissa() / 10_i128.pow(scale.unsigned_abs())
                    } else {
                        d.mantissa() * 10_i128.pow(scale as u32)
                    }
                } else if clickhouse_schema_feature.unwrap().can_null {
                    warn!("Inf, -Inf, Nan in RW decimal is converted into clickhouse null!");
                    return Ok(vec![ClickHouseFieldWithNull::None]);
                } else {
                    warn!("Inf, -Inf, Nan in RW decimal is converted into clickhouse 0!");
                    0_i128
                };
                if clickhouse_schema_feature.unwrap().accuracy_decimal.0 <= 9 {
                    ClickHouseField::Decimal(ClickHouseDecimal::Decimal32(d as i32))
                } else if clickhouse_schema_feature.unwrap().accuracy_decimal.0 <= 18 {
                    ClickHouseField::Decimal(ClickHouseDecimal::Decimal64(d as i64))
                } else {
                    ClickHouseField::Decimal(ClickHouseDecimal::Decimal128(d))
                }
            }
            ScalarRefImpl::Interval(_) => {
                return Err(SinkError::ClickHouse(
                    "INTERVAL is not supported for ClickHouse sink. Please convert to VARCHAR or other supported types.".to_owned(),
                ));
            }
            ScalarRefImpl::Date(v) => {
                let days = v.get_nums_days_unix_epoch();
                ClickHouseField::Int32(days)
            }
            ScalarRefImpl::Time(_) => {
                return Err(SinkError::ClickHouse(
                    "TIME is not supported for ClickHouse sink. Please convert to VARCHAR or other supported types.".to_owned(),
                ));
            }
            ScalarRefImpl::Timestamp(_) => {
                return Err(SinkError::ClickHouse(
                    "clickhouse does not have a type corresponding to naive timestamp".to_owned(),
                ));
            }
            ScalarRefImpl::Timestamptz(v) => {
                let micros = v.timestamp_micros();
                let ticks = match clickhouse_schema_feature.unwrap().accuracy_time <= 6 {
                    true => {
                        micros
                            / 10_i64
                                .pow((6 - clickhouse_schema_feature.unwrap().accuracy_time).into())
                    }
                    false => micros
                        .checked_mul(
                            10_i64
                                .pow((clickhouse_schema_feature.unwrap().accuracy_time - 6).into()),
                        )
                        .ok_or_else(|| SinkError::ClickHouse("DateTime64 overflow".to_owned()))?,
                };
                ClickHouseField::Int64(ticks)
            }
            ScalarRefImpl::Jsonb(v) => {
                let json_str = v.to_string();
                ClickHouseField::String(json_str)
            }
            ScalarRefImpl::Variant(_) => {
                return Err(SinkError::ClickHouse(
                    "VARIANT is not supported for ClickHouse sink.".to_owned(),
                ));
            }
            ScalarRefImpl::Struct(v) => {
                let mut struct_vec = vec![];
                for (field, (struct_field_name, struct_field_type)) in
                    v.iter_fields_ref().zip_eq_debug(rw_type.as_struct().iter())
                {
                    let name = format!("{}.{}", rw_name, struct_field_name);
                    let a = Self::from_scalar_ref(
                        field,
                        clickhouse_schema_feature_map,
                        &name,
                        struct_field_type,
                    )?;
                    struct_vec.push(ClickHouseFieldWithNull::WithoutSome(ClickHouseField::List(
                        a,
                    )));
                }
                return Ok(struct_vec);
            }
            ScalarRefImpl::List(v) => {
                let mut vec = vec![];
                for i in v.iter() {
                    vec.extend(Self::from_scalar_ref(
                        i,
                        clickhouse_schema_feature_map,
                        rw_name,
                        rw_type,
                    )?)
                }
                return Ok(vec![ClickHouseFieldWithNull::WithoutSome(
                    ClickHouseField::List(vec),
                )]);
            }
            ScalarRefImpl::Bytea(_) => {
                return Err(SinkError::ClickHouse(
                    "BYTEA is not supported for ClickHouse sink. Please convert to VARCHAR or other supported types.".to_owned(),
                ));
            }
            ScalarRefImpl::Map(_) => {
                return Err(SinkError::ClickHouse(
                    "MAP is not supported for ClickHouse sink.".to_owned(),
                ));
            }
            ScalarRefImpl::Vector(_) => {
                return Err(SinkError::ClickHouse(
                    "VECTOR is not supported for ClickHouse sink.".to_owned(),
                ));
            }
        };
        let data = if let Some(clickhouse_schema_feature) = clickhouse_schema_feature
            && clickhouse_schema_feature.can_null
        {
            vec![ClickHouseFieldWithNull::WithSome(data)]
        } else {
            vec![ClickHouseFieldWithNull::WithoutSome(data)]
        };
        Ok(data)
    }
}

impl Serialize for ClickHouseField {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ClickHouseField::Int16(v) => serializer.serialize_i16(*v),
            ClickHouseField::Int32(v) => serializer.serialize_i32(*v),
            ClickHouseField::Int64(v) => serializer.serialize_i64(*v),
            ClickHouseField::Serial(v) => v.serialize(serializer),
            ClickHouseField::Float32(v) => serializer.serialize_f32(*v),
            ClickHouseField::Float64(v) => serializer.serialize_f64(*v),
            ClickHouseField::String(v) => serializer.serialize_str(v),
            ClickHouseField::Bool(v) => serializer.serialize_bool(*v),
            ClickHouseField::List(v) => {
                let mut s = serializer.serialize_seq(Some(v.len()))?;
                for i in v {
                    s.serialize_element(i)?;
                }
                s.end()
            }
            ClickHouseField::Decimal(v) => v.serialize(serializer),
            ClickHouseField::Int8(v) => serializer.serialize_i8(*v),
        }
    }
}
impl Serialize for ClickHouseFieldWithNull {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ClickHouseFieldWithNull::WithSome(v) => serializer.serialize_some(v),
            ClickHouseFieldWithNull::WithoutSome(v) => v.serialize(serializer),
            ClickHouseFieldWithNull::None => serializer.serialize_none(),
        }
    }
}
impl Serialize for ClickHouseColumn {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_struct("useless", self.row.len())?;
        for data in &self.row {
            s.serialize_field("useless", &data)?
        }
        s.end()
    }
}

/// 'Struct'(clickhouse type name is nested) will be converted into some arrays by clickhouse. So we
/// need to make some conversions
pub fn build_fields_name_type_from_schema(schema: &Schema) -> Result<Vec<(String, DataType)>> {
    let mut vec = vec![];
    for field in schema.fields() {
        if let DataType::Struct(st) = &field.data_type {
            for (name, data_type) in st.iter() {
                if matches!(data_type, DataType::Struct(_)) {
                    return Err(SinkError::ClickHouse(
                        "Only one level of nesting is supported for struct".to_owned(),
                    ));
                } else {
                    vec.push((
                        format!("{}.{}", field.name, name),
                        DataType::list(data_type.clone()),
                    ))
                }
            }
        } else {
            vec.push((field.name.clone(), field.data_type()));
        }
    }
    Ok(vec)
}

impl From<::clickhouse::error::Error> for SinkError {
    fn from(value: ::clickhouse::error::Error) -> Self {
        SinkError::ClickHouse(value.to_report_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn system_column(ck_type: &str) -> SystemColumn {
        SystemColumn {
            name: "amount".to_owned(),
            r#type: ck_type.to_owned(),
            is_in_primary_key: 0,
        }
    }

    #[test]
    fn test_parse_decimal_accuracy() {
        let parse = |ck_type: &str| parse_decimal_accuracy(ck_type).unwrap();

        assert_eq!(parse("Decimal(38, 10)"), Some((38, 10)));
        assert_eq!(parse("Decimal(38,10)"), Some((38, 10)));
        assert_eq!(parse("Nullable(Decimal(76, 4))"), Some((76, 4)));
        assert_eq!(parse("DateTime64(3)"), None);

        assert!(parse_decimal_accuracy("Decimal(38)").is_err());
    }

    #[test]
    fn test_decimal_column_check() {
        let check = |rw_type: &DataType, ck_type: &str| {
            ClickHouseSink::check_and_correct_column_type(rw_type, &system_column(ck_type))
        };
        let decimal_list = DataType::list(DataType::Decimal);

        check(&DataType::Decimal, "Nullable(Decimal(38, 10))").unwrap();
        check(&decimal_list, "Array(Decimal(9, 2))").unwrap();
        // Only columns encoded as decimals are held to the precision rule.
        check(&DataType::Jsonb, "JSON(a Decimal(76, 4), b Decimal(9, 2))").unwrap();

        check(&DataType::Decimal, "String").unwrap_err();
        let err = check(&DataType::Decimal, "Decimal(76, 4)")
            .unwrap_err()
            .to_string();
        assert!(err.contains("amount"), "{err}");
        assert!(err.contains("Decimal(76, 4)"), "{err}");
        assert!(err.contains("precision 76"), "{err}");
    }
}
