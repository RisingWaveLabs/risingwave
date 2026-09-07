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

use risingwave_common::catalog::Schema;
use tonic::async_trait;

use super::super::writer::{AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriterExt};
use super::super::{Sink, SinkError, SinkParam, SinkWriterParam};
use super::elasticsearch_opensearch_client::ElasticSearchOpenSearchSinkWriter;
use super::elasticsearch_opensearch_config::{ElasticSearchConfig, ElasticSearchOpenSearchConfig};
use crate::enforce_secret::EnforceSecret;
use crate::sink::Result;

pub const ES_SINK: &str = "elasticsearch";

#[derive(Debug)]
pub struct ElasticSearchSink {
    config: ElasticSearchOpenSearchConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl EnforceSecret for ElasticSearchSink {
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
impl TryFrom<SinkParam> for ElasticSearchSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = ElasticSearchConfig::from_btreemap(param.properties)?.inner;
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

impl Sink for ElasticSearchSink {
    type LogSinker = AsyncTruncateLogSinkerOf<ElasticSearchOpenSearchSinkWriter>;

    const SINK_NAME: &'static str = ES_SINK;

    fn validate_unknown_fields(&self) -> Result<()> {
        crate::sink::validate_sink_unknown_fields(&self.config)
    }

    fn support_schema_change() -> bool {
        true
    }

    async fn validate(&self) -> Result<()> {
        self.config.validate_config(&self.schema)?;
        let client = self.config.build_client(Self::SINK_NAME)?;
        client.ping().await?;
        Ok(())
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        ElasticSearchConfig::from_btreemap(config.clone())?;
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
}
