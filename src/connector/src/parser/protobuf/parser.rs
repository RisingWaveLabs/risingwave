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

use std::collections::HashSet;

use anyhow::Context;
use prost_reflect::{DescriptorPool, FileDescriptor, MessageDescriptor};
use risingwave_common::catalog::Field;
use risingwave_common::{bail, try_match_expand};
use risingwave_connector_codec::decoder::protobuf::ProtobufAccess;
pub use risingwave_connector_codec::decoder::protobuf::parser::{PROTOBUF_MESSAGES_AS_JSONB, *};

use super::decoder::ProtobufDecoder;
use crate::error::ConnectorResult;
use crate::parser::unified::AccessImpl;
use crate::parser::utils::bytes_from_url;
use crate::parser::{AccessBuilder, EncodingProperties, SchemaLocation};
use crate::schema::schema_registry::{Client, handle_sr_list};
use crate::schema::{
    ConfluentSchemaLoader, InvalidOptionError, SchemaVersion, bail_invalid_option_error,
};

#[derive(Debug)]
pub struct ProtobufAccessBuilder {
    decoder: ProtobufDecoder,
    reader_descriptor: MessageDescriptor,

    // A HashSet containing protobuf message type full names (e.g. "google.protobuf.Any")
    // that should be mapped to JSONB type when storing in RisingWave
    messages_as_jsonb: HashSet<String>,
}

impl AccessBuilder for ProtobufAccessBuilder {
    async fn generate_accessor(
        &mut self,
        payload: Vec<u8>,
        _: &crate::source::SourceMeta,
    ) -> ConnectorResult<AccessImpl<'_>> {
        let message = self.decoder.decode(&payload).await?;

        Ok(AccessImpl::Protobuf(ProtobufAccess::new(
            message,
            self.reader_descriptor.clone(),
            &self.messages_as_jsonb,
        )))
    }
}

impl ProtobufAccessBuilder {
    pub fn new(config: ProtobufParserConfig) -> ConnectorResult<Self> {
        let ProtobufParserConfig {
            decoder,
            message_descriptor,
            messages_as_jsonb,
        } = config;

        Ok(Self {
            decoder,
            reader_descriptor: message_descriptor,
            messages_as_jsonb,
        })
    }
}

#[derive(Debug)]
pub struct ProtobufParserConfig {
    decoder: ProtobufDecoder,
    pub(crate) message_descriptor: MessageDescriptor,
    messages_as_jsonb: HashSet<String>,
}

impl ProtobufParserConfig {
    pub async fn new(encoding_properties: EncodingProperties) -> ConnectorResult<Self> {
        let protobuf_config = try_match_expand!(encoding_properties, EncodingProperties::Protobuf)?;
        if protobuf_config.key_message_name.is_some() {
            // https://docs.confluent.io/platform/7.5/control-center/topics/schema.html#c3-schemas-best-practices-key-value-pairs
            bail!("protobuf key is not supported");
        }
        let message_name = protobuf_config.message_name;
        let (message_descriptor, decoder) = match protobuf_config.schema_location {
            SchemaLocation::Confluent {
                urls,
                client_config,
                name_strategy,
                topic,
            } => {
                let url = handle_sr_list(urls.as_str())?;
                let client = Client::new(url, &client_config)?;
                let loader = ConfluentSchemaLoader {
                    client,
                    name_strategy,
                    topic,
                    key_record_name: None,
                    val_record_name: Some(message_name.clone()),
                };
                let (schema_version, root_file_descriptor) = loader
                    .load_val_schema::<FileDescriptor>()
                    .await
                    .context("load schema failed")?;
                let SchemaVersion::Confluent(schema_id) = schema_version else {
                    unreachable!("ConfluentSchemaLoader must return a Confluent schema version")
                };
                let message_descriptor = root_file_descriptor
                    .parent_pool()
                    .get_message_by_name(&message_name)
                    .with_context(|| format!("cannot find message `{message_name}` in schema"))?;
                let decoder = ProtobufDecoder::confluent(
                    message_name.clone(),
                    loader,
                    (schema_id, root_file_descriptor),
                )
                .await;

                (message_descriptor, decoder)
            }
            SchemaLocation::File {
                url,
                aws_auth_props,
            } => {
                let url = handle_sr_list(url.as_str())?;
                let url = url.first().unwrap();
                let schema_bytes = bytes_from_url(url, aws_auth_props.as_ref()).await?;
                let pool = DescriptorPool::decode(schema_bytes.as_slice())
                    .with_context(|| format!("cannot build descriptor pool from schema `{url}`"))?;
                let message_descriptor = pool
                    .get_message_by_name(&message_name)
                    .with_context(|| format!("cannot find message `{message_name}` in schema"))?;
                (
                    message_descriptor.clone(),
                    ProtobufDecoder::Static(message_descriptor),
                )
            }
            SchemaLocation::Glue { .. } => bail_invalid_option_error!(
                "encode protobuf from aws glue schema registry not supported yet"
            ),
        };

        Ok(Self {
            message_descriptor,
            decoder,
            messages_as_jsonb: protobuf_config.messages_as_jsonb,
        })
    }

    /// Maps the protobuf schema to relational schema.
    pub fn map_to_columns(&self) -> ConnectorResult<Vec<Field>> {
        pb_schema_to_fields(&self.message_descriptor, &self.messages_as_jsonb).map_err(|e| e.into())
    }
}
