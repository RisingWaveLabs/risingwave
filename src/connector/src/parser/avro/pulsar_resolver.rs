// Copyright 2026 RisingWave Labs
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

use std::sync::Arc;

use anyhow::Context;
use apache_avro::Schema;
use moka::future::Cache;
use risingwave_common::bail;

use crate::error::ConnectorResult;
use crate::schema::pulsar_schema::{Client, PulsarSchemaConfig, PulsarSchemaInfo};

/// Fetch schemas from Pulsar and cache writer schemas by version.
#[derive(Debug)]
pub struct PulsarSchemaCache {
    writer_schemas: Cache<i64, Arc<Schema>>,
    pulsar_client: Client,
    topic: String,
}

impl PulsarSchemaCache {
    pub fn new(config: PulsarSchemaConfig, topic: String) -> ConnectorResult<Self> {
        Ok(Self {
            writer_schemas: Cache::new(u64::MAX),
            pulsar_client: Client::new(&config)?,
            topic,
        })
    }

    async fn parse_and_cache_schema(
        &self,
        raw_schema: PulsarSchemaInfo,
    ) -> ConnectorResult<Arc<Schema>> {
        if !raw_schema.r#type.eq_ignore_ascii_case("AVRO") {
            bail!("expected Pulsar AVRO schema, got {}", raw_schema.r#type);
        }
        let schema =
            Schema::parse_str(&raw_schema.data).context("failed to parse Pulsar Avro schema")?;
        let schema = Arc::new(schema);
        self.writer_schemas
            .insert(raw_schema.version, Arc::clone(&schema))
            .await;
        Ok(schema)
    }

    /// Gets the latest schema, which is used as the reader schema.
    pub async fn get_latest(&self) -> ConnectorResult<Arc<Schema>> {
        self.parse_and_cache_schema(self.pulsar_client.get_schema(&self.topic, None).await?)
            .await
    }

    /// Gets a specific schema version, which is used as the writer schema.
    pub async fn get_by_version(&self, version: i64) -> ConnectorResult<Arc<Schema>> {
        if let Some(schema) = self.writer_schemas.get(&version).await {
            return Ok(schema);
        }
        self.parse_and_cache_schema(
            self.pulsar_client
                .get_schema(&self.topic, Some(version))
                .await?,
        )
        .await
    }
}
