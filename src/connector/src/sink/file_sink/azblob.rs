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
use opendal::services::Azblob;
use serde::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use super::opendal_sink::{BatchingStrategy, FileSink};
use crate::sink::file_sink::opendal_sink::OpendalSinkBackend;
use crate::sink::{Result, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, SinkError};
use crate::source::UnknownFields;
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct AzblobCommon {
    #[serde(rename = "azblob.container_name")]
    pub container_name: String,
    /// The directory where the sink file is located.
    #[serde(rename = "azblob.path")]
    pub path: String,
    #[serde(rename = "azblob.credentials.account_name", default)]
    pub account_name: Option<String>,
    #[serde(rename = "azblob.credentials.account_key", default)]
    pub account_key: Option<String>,
    #[serde(rename = "azblob.endpoint_url")]
    pub endpoint_url: String,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct AzblobConfig {
    #[serde(flatten)]
    pub common: AzblobCommon,

    #[serde(flatten)]
    pub batching_strategy: BatchingStrategy,

    pub r#type: String, // accept "append-only"

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

pub const AZBLOB_SINK: &str = "azblob";

impl<S: OpendalSinkBackend> FileSink<S> {
    pub fn new_azblob_sink(config: AzblobConfig) -> Result<Operator> {
        // Create azblob builder.
        let mut builder = Azblob::default();
        builder = builder
            .container(&config.common.container_name)
            .endpoint(&config.common.endpoint_url);

        if let Some(account_name) = config.common.account_name {
            builder = builder.account_name(&account_name);
        } else {
            tracing::warn!(
                "account_name azblob is not set, container  {}",
                config.common.container_name
            );
        }

        if let Some(account_key) = config.common.account_key {
            builder = builder.account_key(&account_key);
        } else {
            tracing::warn!(
                "account_key azblob is not set, container  {}",
                config.common.container_name
            );
        }
        let operator: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default());

        Ok(operator)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AzblobSink;

impl UnknownFields for AzblobConfig {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

crate::impl_sink_unknown_fields!(AzblobConfig);

impl OpendalSinkBackend for AzblobSink {
    type Properties = AzblobConfig;

    const SINK_NAME: &'static str = AZBLOB_SINK;

    fn from_btreemap(btree_map: BTreeMap<String, String>) -> Result<Self::Properties> {
        let config =
            serde_json::from_value::<AzblobConfig>(serde_json::to_value(btree_map).unwrap())
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

    fn new_operator(properties: AzblobConfig) -> Result<Operator> {
        FileSink::<AzblobSink>::new_azblob_sink(properties)
    }

    fn get_path(properties: Self::Properties) -> String {
        properties.common.path
    }

    fn get_engine_type() -> super::opendal_sink::EngineType {
        super::opendal_sink::EngineType::Azblob
    }

    fn get_batching_strategy(properties: Self::Properties) -> BatchingStrategy {
        BatchingStrategy {
            max_row_count: properties.batching_strategy.max_row_count,
            rollover_seconds: properties.batching_strategy.rollover_seconds,
            path_partition_prefix: properties.batching_strategy.path_partition_prefix,
        }
    }
}
