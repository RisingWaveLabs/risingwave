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

use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Context;
use apache_avro::types::Value;
use apache_avro::{Reader, Schema, from_avro_datum};
use risingwave_common::catalog::Field;
use risingwave_common::{bail, try_match_expand};
use risingwave_connector_codec::decoder::avro::{
    AvroAccess, AvroParseOptions, ResolvedAvroSchema, avro_schema_to_fields,
};

use super::{ConfluentSchemaCache, GlueSchemaCache as _, GlueSchemaCacheImpl, PulsarSchemaCache};
use crate::error::ConnectorResult;
use crate::parser::unified::AccessImpl;
use crate::parser::utils::bytes_from_url;
use crate::parser::{
    AccessBuilder, AvroProperties, EncodingProperties, MapHandling, SchemaLocation,
};
use crate::schema::pulsar_schema::PulsarSchemaVersion;
use crate::schema::schema_registry::{
    Client, extract_schema_id, get_subject_by_strategy, handle_sr_list,
};
use crate::source::SourceMeta;

// Default avro access builder
#[derive(Debug)]
pub struct AvroAccessBuilder {
    schema: Arc<ResolvedAvroSchema>,
    /// Refer to [`AvroParserConfig::writer_schema_cache`].
    writer_schema_cache: WriterSchemaCache,
    value: Option<Value>,
}

impl AccessBuilder for AvroAccessBuilder {
    async fn generate_accessor(
        &mut self,
        payload: Vec<u8>,
        source_meta: &SourceMeta,
    ) -> ConnectorResult<AccessImpl<'_>> {
        self.value = self.parse_avro_value(&payload, source_meta).await?;
        Ok(AccessImpl::Avro(AvroAccess::new(
            self.value.as_ref().unwrap(),
            AvroParseOptions::create(&self.schema.original_schema),
        )))
    }
}

impl AvroAccessBuilder {
    pub fn new(config: AvroParserConfig) -> ConnectorResult<Self> {
        let AvroParserConfig {
            schema,
            writer_schema_cache,
            ..
        } = config;
        Ok(Self {
            schema,
            writer_schema_cache,
            value: None,
        })
    }

    /// Note: we should use unresolved schema to parsing bytes into avro value.
    /// Otherwise it's an invalid schema and parsing will fail. (Avro error: Two named schema defined for same fullname)
    ///
    /// # Notes about how Avro data looks like
    ///
    /// First, it has two [serialization encodings: binary and JSON](https://avro.apache.org/docs/1.11.1/specification/#encodings).
    /// They don't have magic bytes and cannot be distinguished on their own.
    ///
    /// But in different cases, it starts with different headers, or magic bytes, which can be confusing.
    ///
    /// ## `apache_avro` API and headers
    ///
    /// - `apache_avro::Reader`: [Object Container Files](https://avro.apache.org/docs/1.11.1/specification/#object-container-files): contains file header, starting with 4 bytes `Obj1`. This is a batch file encoding. We don't use it.
    /// - `apache_avro::GenericSingleObjectReader`: [Single-object encoding](https://avro.apache.org/docs/1.11.1/specification/#single-object-encoding): starts with 2 bytes `0xC301`. This is designed to be used in places like Kafka, but Confluent schema registry doesn't use it.
    /// - `apache_avro::from_avro_datum`: no header, binary encoding. This is what we should use.
    ///
    /// ## Confluent schema registry
    ///
    /// - In Kafka ([Confluent schema registry wire format](https://docs.confluent.io/platform/7.6/schema-registry/fundamentals/serdes-develop/index.html#wire-format)):
    ///   starts with 5 bytes`0x00{schema_id:08x}` followed by Avro binary encoding.
    ///
    /// ## Pulsar schema
    ///
    /// - In Pulsar, the payload is raw Avro binary encoding without a schema-registry header.
    ///   The writer schema version is carried in Pulsar message metadata.
    async fn parse_avro_value(
        &self,
        payload: &[u8],
        source_meta: &SourceMeta,
    ) -> ConnectorResult<Option<Value>> {
        // Each schema backend has its own wire-format contract, so keep payload framing close to
        // writer-schema lookup.
        match &self.writer_schema_cache {
            WriterSchemaCache::Confluent(resolver) => {
                let (schema_id, mut raw_payload) = extract_schema_id(payload)?;
                let writer_schema = resolver.get_by_id(schema_id).await?;
                Ok(Some(from_avro_datum(
                    writer_schema.as_ref(),
                    &mut raw_payload,
                    Some(&self.schema.original_schema),
                )?))
            }
            WriterSchemaCache::File => {
                // FIXME: we should not use `Reader` (file header) here. See comment above and https://github.com/risingwavelabs/risingwave/issues/12871
                let mut reader = Reader::with_schema(&self.schema.original_schema, payload)?;
                match reader.next() {
                    Some(Ok(v)) => Ok(Some(v)),
                    Some(Err(e)) => Err(e)?,
                    None => bail!("avro parse unexpected eof"),
                }
            }
            WriterSchemaCache::Glue(resolver) => {
                // <https://github.com/awslabs/aws-glue-schema-registry/blob/v1.1.20/common/src/main/java/com/amazonaws/services/schemaregistry/utils/AWSSchemaRegistryConstants.java#L59-L61>
                // byte 0:      header version = 3
                // byte 1:      compression: 0 = no compression; 5 = zlib (unsupported)
                // byte 2..=17: 16-byte UUID as schema version id
                // byte 18..:   raw avro payload
                if payload.len() < 18 {
                    bail!("payload shorter than 18-byte glue header");
                }
                if payload[0] != 3 {
                    bail!(
                        "Only support glue header version 3 but found {}",
                        payload[0]
                    );
                }
                if payload[1] != 0 {
                    bail!("Non-zero compression {} not supported", payload[1]);
                }
                let schema_version_id = uuid::Uuid::from_slice(&payload[2..18]).unwrap();
                let writer_schema = resolver.get_by_id(schema_version_id).await?;
                let mut raw_payload = &payload[18..];
                Ok(Some(from_avro_datum(
                    writer_schema.as_ref(),
                    &mut raw_payload,
                    Some(&self.schema.original_schema),
                )?))
            }
            WriterSchemaCache::Pulsar(resolver) => {
                let SourceMeta::Pulsar(meta) = source_meta else {
                    bail!("Pulsar Avro parser received non-Pulsar source metadata");
                };
                let writer_schema = match meta
                    .schema_version
                    .as_deref()
                    .map(PulsarSchemaVersion::try_from)
                    .transpose()?
                {
                    Some(version) => resolver.get_by_version(version.0).await?,
                    None => Arc::clone(&self.schema.original_schema),
                };
                let mut raw_payload = payload;
                Ok(Some(from_avro_datum(
                    writer_schema.as_ref(),
                    &mut raw_payload,
                    Some(&self.schema.original_schema),
                )?))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct AvroParserConfig {
    schema: Arc<ResolvedAvroSchema>,
    /// Writer schema is the schema used to write the data. When parsing Avro data, the exactly same schema
    /// must be used to decode the message, and then convert it with the reader schema.
    writer_schema_cache: WriterSchemaCache,

    map_handling: Option<MapHandling>,
}

#[derive(Debug, Clone)]
enum WriterSchemaCache {
    Confluent(Arc<ConfluentSchemaCache>),
    Glue(Arc<GlueSchemaCacheImpl>),
    Pulsar(Arc<PulsarSchemaCache>),
    File,
}

impl AvroParserConfig {
    pub async fn new(encoding_properties: EncodingProperties) -> ConnectorResult<Self> {
        let AvroProperties {
            schema_location,
            record_name,
            key_record_name,
            map_handling,
        } = try_match_expand!(encoding_properties, EncodingProperties::Avro)?;
        match schema_location {
            SchemaLocation::Confluent {
                urls: schema_location,
                client_config,
                name_strategy,
                topic,
            } => {
                let url = handle_sr_list(schema_location.as_str())?;
                let client = Client::new(url, &client_config)?;
                let resolver = ConfluentSchemaCache::new(client);

                if let Some(name) = &key_record_name {
                    bail!("unused FORMAT ENCODE option: key.message='{name}'");
                }
                let subject_value = get_subject_by_strategy(
                    &name_strategy,
                    topic.as_str(),
                    record_name.as_deref(),
                    false,
                )?;
                tracing::debug!("value subject {subject_value}");

                Ok(Self {
                    schema: Arc::new(ResolvedAvroSchema::create(
                        resolver.get_by_subject(&subject_value).await?,
                    )?),
                    writer_schema_cache: WriterSchemaCache::Confluent(Arc::new(resolver)),
                    map_handling,
                })
            }
            SchemaLocation::File {
                url: schema_location,
                aws_auth_props,
            } => {
                let url = handle_sr_list(schema_location.as_str())?;
                let url = url.first().unwrap();
                let schema_content = bytes_from_url(url, aws_auth_props.as_ref()).await?;
                let schema = Schema::parse_reader(&mut schema_content.as_slice())
                    .context("failed to parse avro schema")?;
                Ok(Self {
                    schema: Arc::new(ResolvedAvroSchema::create(Arc::new(schema))?),
                    writer_schema_cache: WriterSchemaCache::File,
                    map_handling,
                })
            }
            SchemaLocation::Glue {
                schema_arn,
                aws_auth_props,
                mock_config,
            } => {
                let resolver =
                    GlueSchemaCacheImpl::new(&aws_auth_props, mock_config.as_deref()).await?;
                let schema = resolver.get_by_name(&schema_arn).await?;
                Ok(Self {
                    schema: Arc::new(ResolvedAvroSchema::create(schema)?),
                    writer_schema_cache: WriterSchemaCache::Glue(Arc::new(resolver)),
                    map_handling,
                })
            }
            SchemaLocation::Pulsar {
                client_config,
                topic,
            } => {
                let resolver = PulsarSchemaCache::new(client_config, topic)?;
                let schema = resolver.get_latest().await?;
                Ok(Self {
                    schema: Arc::new(ResolvedAvroSchema::create(schema)?),
                    writer_schema_cache: WriterSchemaCache::Pulsar(Arc::new(resolver)),
                    map_handling,
                })
            }
        }
    }

    pub fn map_to_columns(&self) -> ConnectorResult<Vec<Field>> {
        avro_schema_to_fields(&self.schema.original_schema, self.map_handling).map_err(Into::into)
    }
}

#[cfg(test)]
mod test {
    use std::env;

    use url::Url;

    use super::*;
    use crate::connector_common::AwsAuthProps;

    fn test_data_path(file_name: &str) -> String {
        let curr_dir = env::current_dir().unwrap().into_os_string();
        curr_dir.into_string().unwrap() + "/src/test_data/" + file_name
    }

    #[tokio::test]
    #[ignore]
    async fn test_load_schema_from_s3() {
        let schema_location = "s3://mingchao-schemas/complex-schema.avsc".to_owned();
        let url = Url::parse(&schema_location).unwrap();
        let aws_auth_config: AwsAuthProps =
            serde_json::from_str(r#"region":"ap-southeast-1"#).unwrap();
        let schema_content = bytes_from_url(&url, Some(&aws_auth_config)).await;
        assert!(schema_content.is_ok());
        let schema = Schema::parse_reader(&mut schema_content.unwrap().as_slice());
        assert!(schema.is_ok());
        println!("schema = {:?}", schema.unwrap());
    }

    #[tokio::test]
    async fn test_load_schema_from_local() {
        let schema_location = Url::from_file_path(test_data_path("complex-schema.avsc")).unwrap();
        let schema_content = bytes_from_url(&schema_location, None).await;
        assert!(schema_content.is_ok());
        let schema = Schema::parse_reader(&mut schema_content.unwrap().as_slice());
        assert!(schema.is_ok());
        println!("schema = {:?}", schema.unwrap());
    }

    #[tokio::test]
    #[ignore]
    async fn test_load_schema_from_https() {
        let schema_location =
            "https://mingchao-schemas.s3.ap-southeast-1.amazonaws.com/complex-schema.avsc";
        let url = Url::parse(schema_location).unwrap();
        let schema_content = bytes_from_url(&url, None).await;
        assert!(schema_content.is_ok());
        let schema = Schema::parse_reader(&mut schema_content.unwrap().as_slice());
        assert!(schema.is_ok());
        println!("schema = {:?}", schema.unwrap());
    }
}
