// Copyright 2024 RisingWave Labs
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

use anyhow::anyhow;
use opendal::Operator;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::S3;
use risingwave_common::util::env_var::env_var_is_true;
use serde::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use super::opendal_sink::{BatchingStrategy, FileSink};
use crate::connector_common::DISABLE_DEFAULT_CREDENTIAL;
use crate::deserialize_optional_bool_from_string;
use crate::sink::file_sink::opendal_sink::OpendalSinkBackend;
use crate::sink::{Result, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, SinkError};
use crate::source::UnknownFields;
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct S3Common {
    #[serde(rename = "s3.region_name", alias = "snowflake.aws_region")]
    pub region_name: String,
    #[serde(rename = "s3.bucket_name", alias = "snowflake.s3_bucket")]
    pub bucket_name: String,
    /// The directory where the sink file is located.
    #[serde(rename = "s3.path", alias = "snowflake.s3_path", default)]
    pub path: Option<String>,
    /// Enable config load. This parameter set to true will load s3 credentials from the environment. Only allowed to be used in a self-hosted environment.
    #[serde(default, deserialize_with = "deserialize_optional_bool_from_string")]
    pub enable_config_load: Option<bool>,
    #[serde(
        rename = "s3.credentials.access",
        alias = "snowflake.aws_access_key_id",
        default
    )]
    pub access: Option<String>,
    #[serde(
        rename = "s3.credentials.secret",
        alias = "snowflake.aws_secret_access_key",
        default
    )]
    pub secret: Option<String>,
    #[serde(rename = "s3.endpoint_url")]
    pub endpoint_url: Option<String>,
    #[serde(rename = "s3.assume_role", default)]
    pub assume_role: Option<String>,
}

impl S3Common {
    pub fn enable_config_load(&self) -> bool {
        // If the env var is set to true, we disable the default config load. (Cloud environment)
        if env_var_is_true(DISABLE_DEFAULT_CREDENTIAL) {
            return false;
        }
        self.enable_config_load.unwrap_or(false)
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct S3Config {
    #[serde(flatten)]
    pub common: S3Common,

    #[serde(flatten)]
    pub batching_strategy: BatchingStrategy,

    pub r#type: String, // accept "append-only"

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

pub const S3_SINK: &str = "s3";

impl<S: OpendalSinkBackend> FileSink<S> {
    pub fn new_s3_sink(common: &S3Common) -> Result<Operator> {
        // Create s3 builder.
        let mut builder = S3::default()
            .bucket(&common.bucket_name)
            .region(&common.region_name);

        if let Some(endpoint_url) = &common.endpoint_url {
            builder = builder.endpoint(endpoint_url);
        }

        if let Some(access) = &common.access {
            builder = builder.access_key_id(access);
        }

        if let Some(secret) = &common.secret {
            builder = builder.secret_access_key(secret);
        }

        if let Some(assume_role) = &common.assume_role {
            builder = builder.role_arn(assume_role);
        }
        // Default behavior is disable loading config from environment.
        if !common.enable_config_load() {
            builder = builder.disable_config_load();
        }

        let operator: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default());

        Ok(operator)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct S3Sink;

impl UnknownFields for S3Config {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

crate::impl_sink_unknown_fields!(S3Config);

impl OpendalSinkBackend for S3Sink {
    type Properties = S3Config;

    const SINK_NAME: &'static str = S3_SINK;

    fn from_btreemap(btree_map: BTreeMap<String, String>) -> Result<Self::Properties> {
        let config = serde_json::from_value::<S3Config>(serde_json::to_value(btree_map).unwrap())
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

    fn new_operator(properties: S3Config) -> Result<Operator> {
        FileSink::<S3Sink>::new_s3_sink(&properties.common)
    }

    fn get_path(properties: Self::Properties) -> String {
        properties.common.path.unwrap_or_default()
    }

    fn get_engine_type() -> super::opendal_sink::EngineType {
        super::opendal_sink::EngineType::S3
    }

    fn get_batching_strategy(properties: Self::Properties) -> BatchingStrategy {
        BatchingStrategy {
            max_row_count: properties.batching_strategy.max_row_count,
            rollover_seconds: properties.batching_strategy.rollover_seconds,
            path_partition_prefix: properties.batching_strategy.path_partition_prefix,
        }
    }
}

// Wrapper for expanding in the sink config type.
#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct SnowflakeConfig {
    #[serde(flatten)]
    pub inner: S3Config,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnowflakeSink;

pub const SNOWFLAKE_SINK: &str = "snowflake";

impl OpendalSinkBackend for SnowflakeSink {
    type Properties = S3Config;

    const SINK_NAME: &'static str = SNOWFLAKE_SINK;

    fn from_btreemap(btree_map: BTreeMap<String, String>) -> Result<Self::Properties> {
        let config =
            serde_json::from_value::<SnowflakeConfig>(serde_json::to_value(btree_map).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?
                .inner;
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

    fn new_operator(properties: S3Config) -> Result<Operator> {
        FileSink::<SnowflakeSink>::new_s3_sink(&properties.common)
    }

    fn get_path(properties: Self::Properties) -> String {
        properties.common.path.unwrap_or_default()
    }

    fn get_engine_type() -> super::opendal_sink::EngineType {
        super::opendal_sink::EngineType::Snowflake
    }

    fn get_batching_strategy(properties: Self::Properties) -> BatchingStrategy {
        BatchingStrategy {
            max_row_count: properties.batching_strategy.max_row_count,
            rollover_seconds: properties.batching_strategy.rollover_seconds,
            path_partition_prefix: properties.batching_strategy.path_partition_prefix,
        }
    }
}
