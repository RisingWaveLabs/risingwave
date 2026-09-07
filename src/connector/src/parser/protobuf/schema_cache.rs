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

use anyhow::Context;
use moka::future::Cache;
use prost_reflect::FileDescriptor;

use crate::error::{ConnectorError, ConnectorResult};
use crate::schema::ConfluentSchemaLoader;

const DEFAULT_PROTOBUF_SCHEMA_CACHE_CAPACITY: u64 = 128;

#[derive(Debug)]
pub(super) struct ProtobufSchemaCache {
    loader: ConfluentSchemaLoader,
    schemas: Cache<i32, FileDescriptor>,
}

impl ProtobufSchemaCache {
    pub fn new(loader: ConfluentSchemaLoader) -> Self {
        let schemas = Cache::builder()
            .max_capacity(DEFAULT_PROTOBUF_SCHEMA_CACHE_CAPACITY)
            .build();

        Self { loader, schemas }
    }

    /// Inserts an already loaded schema into the cache without contacting Schema Registry.
    pub async fn load(&self, schema_id: i32, file_descriptor: FileDescriptor) {
        self.schemas.insert(schema_id, file_descriptor).await;
    }

    pub async fn get(&self, schema_id: i32) -> ConnectorResult<FileDescriptor> {
        self.schemas
            .try_get_with(schema_id, async {
                self.loader
                    .load_val_schema_by_id(schema_id)
                    .await
                    .with_context(|| {
                        format!("failed to load protobuf writer schema ID {schema_id}")
                    })
                    .map_err(ConnectorError::from)
            })
            .await
            .map_err(ConnectorError::from)
    }
}

#[cfg(test)]
mod tests {
    use prost_reflect::DescriptorPool;
    use prost_types::{FileDescriptorProto, FileDescriptorSet};

    use super::*;
    use crate::schema::schema_registry::{Client, SchemaRegistryConfig};

    #[tokio::test]
    async fn test_schema_is_cached() {
        let pool = DescriptorPool::from_file_descriptor_set(FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("initial.proto".into()),
                syntax: Some("proto3".into()),
                ..Default::default()
            }],
        })
        .unwrap();
        let file_descriptor = pool.get_file_by_name("initial.proto").unwrap();
        let loader = ConfluentSchemaLoader {
            client: Client::new(
                vec!["http://127.0.0.1:1".parse().unwrap()],
                &SchemaRegistryConfig::default(),
            )
            .unwrap(),
            name_strategy: Default::default(),
            topic: "unused".into(),
            key_record_name: None,
            val_record_name: Some("test.Event".into()),
        };

        let cache = ProtobufSchemaCache::new(loader);
        cache.load(42, file_descriptor.clone()).await;
        let cached = cache.get(42).await.unwrap();

        assert_eq!(cached, file_descriptor);
    }
}
