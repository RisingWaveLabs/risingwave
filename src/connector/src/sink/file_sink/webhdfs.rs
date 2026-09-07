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
use opendal::layers::LoggingLayer;
use opendal::services::Webhdfs;
use serde::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use super::opendal_sink::{BatchingStrategy, FileSink};
use crate::sink::file_sink::opendal_sink::OpendalSinkBackend;
use crate::sink::{Result, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, SinkError};
use crate::source::UnknownFields;
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct WebhdfsCommon {
    #[serde(rename = "webhdfs.endpoint")]
    pub endpoint: String,
    /// The directory where the sink file is located.
    #[serde(rename = "webhdfs.path")]
    pub path: String,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct WebhdfsConfig {
    #[serde(flatten)]
    pub common: WebhdfsCommon,

    #[serde(flatten)]
    pub batching_strategy: BatchingStrategy,

    pub r#type: String, // accept "append-only"

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

pub const WEBHDFS_SINK: &str = "webhdfs";

impl<S: OpendalSinkBackend> FileSink<S> {
    pub fn new_webhdfs_sink(config: WebhdfsConfig) -> Result<Operator> {
        // Create webhdfs backend builder.
        let builder = Webhdfs::default()
            .endpoint(&config.common.endpoint)
            .root(&config.common.path);

        let operator: Operator = Operator::new(builder)?.layer(LoggingLayer::default());

        Ok(operator)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WebhdfsSink;

impl UnknownFields for WebhdfsConfig {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

crate::impl_sink_unknown_fields!(WebhdfsConfig);

impl OpendalSinkBackend for WebhdfsSink {
    type Properties = WebhdfsConfig;

    const SINK_NAME: &'static str = WEBHDFS_SINK;

    fn from_btreemap(btree_map: BTreeMap<String, String>) -> Result<Self::Properties> {
        let config =
            serde_json::from_value::<WebhdfsConfig>(serde_json::to_value(btree_map).unwrap())
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

    fn new_operator(properties: WebhdfsConfig) -> Result<Operator> {
        FileSink::<WebhdfsSink>::new_webhdfs_sink(properties)
    }

    fn get_path(properties: Self::Properties) -> String {
        properties.common.path
    }

    fn get_engine_type() -> super::opendal_sink::EngineType {
        super::opendal_sink::EngineType::Webhdfs
    }

    fn get_batching_strategy(properties: Self::Properties) -> BatchingStrategy {
        BatchingStrategy {
            max_row_count: properties.batching_strategy.max_row_count,
            rollover_seconds: properties.batching_strategy.rollover_seconds,
            path_partition_prefix: properties.batching_strategy.path_partition_prefix,
        }
    }
}
