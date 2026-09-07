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

use std::collections::BTreeMap;

use anyhow::anyhow;
use risingwave_common::catalog::Schema;
use risingwave_common::session_config::sink_decouple::SinkDecouple;
use tonic::async_trait;

use super::super::writer::{AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriterExt};
use super::super::{Sink, SinkError, SinkParam, SinkWriterParam};
use super::elasticsearch_opensearch_client::ElasticSearchOpenSearchSinkWriter;
use super::elasticsearch_opensearch_config::{ElasticSearchOpenSearchConfig, OpenSearchConfig};
use crate::enforce_secret::EnforceSecret;
use crate::sink::Result;

pub const OPENSEARCH_SINK: &str = "opensearch";

#[derive(Debug)]
pub struct OpenSearchSink {
    config: ElasticSearchOpenSearchConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl EnforceSecret for OpenSearchSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            ElasticSearchOpenSearchConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}
#[async_trait]
impl TryFrom<SinkParam> for OpenSearchSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = OpenSearchConfig::from_btreemap(param.properties)?.inner;
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

impl Sink for OpenSearchSink {
    type LogSinker = AsyncTruncateLogSinkerOf<ElasticSearchOpenSearchSinkWriter>;

    const SINK_NAME: &'static str = OPENSEARCH_SINK;

    fn validate_unknown_fields(&self) -> Result<()> {
        crate::sink::validate_sink_unknown_fields(&self.config)
    }

    async fn validate(&self) -> Result<()> {
        risingwave_common::license::Feature::OpenSearchSink
            .check_available()
            .map_err(|e| anyhow::anyhow!(e))?;
        self.config.validate_config(&self.schema)?;
        let client = self.config.build_client(Self::SINK_NAME)?;
        client.ping().await?;
        Ok(())
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        OpenSearchConfig::from_btreemap(config.clone())?;
        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(ElasticSearchOpenSearchSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            Self::SINK_NAME,
            self.is_append_only,
        )?
        .into_log_sinker(self.config.concurrent_requests))
    }

    fn set_default_commit_checkpoint_interval(
        desc: &mut crate::sink::catalog::desc::SinkDesc,
        user_specified: &risingwave_common::session_config::sink_decouple::SinkDecouple,
    ) -> Result<()> {
        if crate::sink::is_sink_support_commit_checkpoint_interval(Self::SINK_NAME) {
            match desc
                .properties
                .get(crate::sink::decouple_checkpoint_log_sink::COMMIT_CHECKPOINT_INTERVAL)
            {
                Some(commit_checkpoint_interval) => {
                    let commit_checkpoint_interval = commit_checkpoint_interval
                        .parse::<u64>()
                        .map_err(|e| SinkError::Config(anyhow!(e)))?;
                    if std::matches!(user_specified, SinkDecouple::Disable)
                        && commit_checkpoint_interval > 1
                    {
                        return Err(SinkError::Config(anyhow!(
                            "config conflict: `commit_checkpoint_interval` larger than 1 means that sink decouple must be enabled, but session config sink_decouple is disabled"
                        )));
                    }
                }
                None => match user_specified {
                    risingwave_common::session_config::sink_decouple::SinkDecouple::Default
                    | risingwave_common::session_config::sink_decouple::SinkDecouple::Enable => {
                        desc.properties.insert(
                            crate::sink::decouple_checkpoint_log_sink::COMMIT_CHECKPOINT_INTERVAL.to_owned(),
                            crate::sink::decouple_checkpoint_log_sink::DEFAULT_COMMIT_CHECKPOINT_INTERVAL_WITH_SINK_DECOUPLE.to_string(),
                        );
                    }
                    risingwave_common::session_config::sink_decouple::SinkDecouple::Disable => {
                        desc.properties.insert(
                            crate::sink::decouple_checkpoint_log_sink::COMMIT_CHECKPOINT_INTERVAL.to_owned(),
                            crate::sink::decouple_checkpoint_log_sink::DEFAULT_COMMIT_CHECKPOINT_INTERVAL_WITHOUT_SINK_DECOUPLE.to_string(),
                        );
                    }
                },
            }
        }
        Ok(())
    }

    fn is_sink_decouple(
        user_specified: &risingwave_common::session_config::sink_decouple::SinkDecouple,
    ) -> Result<bool> {
        match user_specified {
            risingwave_common::session_config::sink_decouple::SinkDecouple::Default
            | risingwave_common::session_config::sink_decouple::SinkDecouple::Enable => Ok(true),
            risingwave_common::session_config::sink_decouple::SinkDecouple::Disable => Ok(false),
        }
    }
}
