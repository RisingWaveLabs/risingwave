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

use std::collections::{BTreeMap, HashSet};

use anyhow::anyhow;
use maplit::hashset;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use url::Url;
use with_options::WithOptions;

use super::super::SinkError;
use super::elasticsearch::ES_SINK;
use super::elasticsearch_opensearch_client::ElasticSearchOpenSearchClient;
use super::opensearch::OPENSEARCH_SINK;
use crate::connector_common::ElasticsearchConnection;
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorError;
use crate::sink::Result;

pub const ES_OPTION_DELIMITER: &str = "delimiter";
pub const ES_OPTION_INDEX_COLUMN: &str = "index_column";
pub const ES_OPTION_INDEX: &str = "index";
pub const ES_OPTION_ROUTING_COLUMN: &str = "routing_column";

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct ElasticSearchConfig {
    #[serde(flatten)]
    pub inner: ElasticSearchOpenSearchConfig,
}

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct OpenSearchConfig {
    #[serde(flatten)]
    pub inner: ElasticSearchOpenSearchConfig,
}

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct ElasticSearchOpenSearchConfig {
    #[serde(rename = "url")]
    pub url: String,
    /// The index's name of elasticsearch or openserach
    #[serde(rename = "index")]
    pub index: Option<String>,
    /// If pk is set, then "pk1+delimiter+pk2+delimiter..." will be used as the key, if pk is not set, we will just use the first column as the key.
    #[serde(rename = "delimiter")]
    pub delimiter: Option<String>,
    /// The username of elasticsearch or openserach
    #[serde(rename = "username")]
    pub username: Option<String>,
    /// The username of elasticsearch or openserach
    #[serde(rename = "password")]
    pub password: Option<String>,
    /// It is used for dynamic index, if it is be set, the value of this column will be used as the index. It and `index` can only set one
    #[serde(rename = "index_column")]
    pub index_column: Option<String>,

    /// It is used for dynamic route, if it is be set, the value of this column will be used as the route
    #[serde(rename = "routing_column")]
    pub routing_column: Option<String>,

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,

    #[serde(rename = "retry_on_conflict")]
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_retry_on_conflict")]
    pub retry_on_conflict: i32,

    #[serde(rename = "batch_num_messages")]
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_batch_num_messages")]
    #[with_option(allow_alter_on_fly)]
    pub batch_num_messages: usize,

    #[serde(rename = "batch_size_kb")]
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_batch_size_kb")]
    #[with_option(allow_alter_on_fly)]
    pub batch_size_kb: usize,

    #[serde(rename = "concurrent_requests")]
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_concurrent_requests")]
    #[with_option(allow_alter_on_fly)]
    pub concurrent_requests: usize,

    #[serde(default = "default_type")]
    pub r#type: String,
}

crate::impl_sink_unknown_fields!(ElasticSearchOpenSearchConfig);

impl EnforceSecret for ElasticSearchOpenSearchConfig {
    const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf::phf_set! {
        "username",
        "password",
    };
}

fn default_type() -> String {
    "upsert".to_owned()
}

fn default_retry_on_conflict() -> i32 {
    3
}

fn default_batch_num_messages() -> usize {
    512
}

fn default_batch_size_kb() -> usize {
    5 * 1024
}

fn default_concurrent_requests() -> usize {
    1024
}

impl TryFrom<&ElasticsearchConnection> for ElasticSearchOpenSearchConfig {
    type Error = ConnectorError;

    fn try_from(value: &ElasticsearchConnection) -> std::result::Result<Self, Self::Error> {
        let allowed_fields: HashSet<&str> = hashset!["url", "username", "password"]; // from ElasticsearchOpenSearchConfig

        for k in value.0.keys() {
            if !allowed_fields.contains(k.as_str()) {
                return Err(ConnectorError::from(anyhow!(
                    "Invalid field: {}, allowed fields: {:?}",
                    k,
                    allowed_fields
                )));
            }
        }

        let config = serde_json::from_value::<ElasticSearchOpenSearchConfig>(
            serde_json::to_value(value.0.clone()).unwrap(),
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }
}

impl ElasticSearchConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<ElasticSearchConfig>(
            serde_json::to_value(properties).unwrap(),
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }
}

impl OpenSearchConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<OpenSearchConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }
}

impl ElasticSearchOpenSearchConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<ElasticSearchOpenSearchConfig>(
            serde_json::to_value(properties).unwrap(),
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }

    pub fn build_client(&self, connector: &str) -> Result<ElasticSearchOpenSearchClient> {
        let check_username_password = || -> Result<()> {
            if self.username.is_some() && self.password.is_none() {
                return Err(SinkError::Config(anyhow!(
                    "please set the password when the username is set."
                )));
            }
            if self.username.is_none() && self.password.is_some() {
                return Err(SinkError::Config(anyhow!(
                    "please set the username when the password is set."
                )));
            }
            Ok(())
        };
        let url =
            Url::parse(&self.url).map_err(|e| SinkError::ElasticSearchOpenSearch(anyhow!(e)))?;
        if connector.eq(ES_SINK) {
            let mut transport_builder = elasticsearch::http::transport::TransportBuilder::new(
                elasticsearch::http::transport::SingleNodeConnectionPool::new(url),
            );
            if let Some(username) = &self.username
                && let Some(password) = &self.password
            {
                transport_builder = transport_builder.auth(
                    elasticsearch::auth::Credentials::Basic(username.clone(), password.clone()),
                );
            }
            check_username_password()?;
            let transport = transport_builder
                .build()
                .map_err(|e| SinkError::ElasticSearchOpenSearch(anyhow!(e)))?;
            let client = elasticsearch::Elasticsearch::new(transport);
            Ok(ElasticSearchOpenSearchClient::ElasticSearch(client))
        } else if connector.eq(OPENSEARCH_SINK) {
            let mut transport_builder = opensearch::http::transport::TransportBuilder::new(
                opensearch::http::transport::SingleNodeConnectionPool::new(url),
            );
            if let Some(username) = &self.username
                && let Some(password) = &self.password
            {
                transport_builder = transport_builder.auth(opensearch::auth::Credentials::Basic(
                    username.clone(),
                    password.clone(),
                ));
            }
            check_username_password()?;
            let transport = transport_builder
                .build()
                .map_err(|e| SinkError::ElasticSearchOpenSearch(anyhow!(e)))?;
            let client = opensearch::OpenSearch::new(transport);
            Ok(ElasticSearchOpenSearchClient::OpenSearch(client))
        } else {
            panic!(
                "connector type must be {} or {}, but get {}",
                ES_SINK, OPENSEARCH_SINK, connector
            );
        }
    }

    pub fn validate_config(&self, schema: &Schema) -> Result<()> {
        if self.index_column.is_some() && self.index.is_some()
            || self.index_column.is_none() && self.index.is_none()
        {
            return Err(SinkError::Config(anyhow!(
                "please set only one of the 'index_column' or 'index' properties."
            )));
        }

        if let Some(index_column) = &self.index_column {
            let filed = schema
                .fields()
                .iter()
                .find(|f| &f.name == index_column)
                .unwrap();
            if filed.data_type() != DataType::Varchar {
                return Err(SinkError::Config(anyhow!(
                    "please ensure the data type of {} is varchar.",
                    index_column
                )));
            }
        }

        if let Some(routing_column) = &self.routing_column {
            let filed = schema
                .fields()
                .iter()
                .find(|f| &f.name == routing_column)
                .unwrap();
            if filed.data_type() != DataType::Varchar {
                return Err(SinkError::Config(anyhow!(
                    "please ensure the data type of {} is varchar.",
                    routing_column
                )));
            }
        }
        Ok(())
    }

    pub fn get_index_column_index(&self, schema: &Schema) -> Result<Option<usize>> {
        let index_column_idx = self
            .index_column
            .as_ref()
            .map(|n| {
                schema
                    .fields()
                    .iter()
                    .position(|s| &s.name == n)
                    .ok_or_else(|| anyhow!("Cannot find {}", ES_OPTION_INDEX_COLUMN))
            })
            .transpose()?;
        Ok(index_column_idx)
    }

    pub fn get_routing_column_index(&self, schema: &Schema) -> Result<Option<usize>> {
        let routing_column_idx = self
            .routing_column
            .as_ref()
            .map(|n| {
                schema
                    .fields()
                    .iter()
                    .position(|s| &s.name == n)
                    .ok_or_else(|| anyhow!("Cannot find {}", ES_OPTION_ROUTING_COLUMN))
            })
            .transpose()?;
        Ok(routing_column_idx)
    }
}
