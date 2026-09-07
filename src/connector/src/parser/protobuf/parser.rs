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
use prost_reflect::{DescriptorPool, DynamicMessage, FileDescriptor, MessageDescriptor};
use risingwave_common::catalog::Field;
use risingwave_common::{bail, try_match_expand};
use risingwave_connector_codec::decoder::protobuf::ProtobufAccess;
pub use risingwave_connector_codec::decoder::protobuf::parser::{PROTOBUF_MESSAGES_AS_JSONB, *};

use crate::error::ConnectorResult;
use crate::parser::unified::AccessImpl;
use crate::parser::utils::bytes_from_url;
use crate::parser::{AccessBuilder, EncodingProperties, SchemaLocation};
use crate::schema::schema_registry::{Client, WireFormatError, extract_schema_id, handle_sr_list};
use crate::schema::{
    ConfluentSchemaLoader, InvalidOptionError, SchemaLoader, bail_invalid_option_error,
};

#[derive(Debug)]
pub struct ProtobufAccessBuilder {
    wire_type: WireType,
    message_descriptor: MessageDescriptor,

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
        let payload = match self.wire_type {
            WireType::Confluent => resolve_pb_header(&payload)?,
            WireType::None => &payload,
        };

        let message = DynamicMessage::decode(self.message_descriptor.clone(), payload)
            .context("failed to parse message")?;

        Ok(AccessImpl::Protobuf(ProtobufAccess::new(
            message,
            &self.messages_as_jsonb,
        )))
    }
}

impl ProtobufAccessBuilder {
    pub fn new(config: ProtobufParserConfig) -> ConnectorResult<Self> {
        let ProtobufParserConfig {
            wire_type,
            message_descriptor,
            messages_as_jsonb,
        } = config;

        Ok(Self {
            wire_type,
            message_descriptor,
            messages_as_jsonb,
        })
    }
}

#[derive(Debug, Clone)]
enum WireType {
    None,
    Confluent,
    // Glue,
    // Pulsar,
}

impl TryFrom<&SchemaLocation> for WireType {
    type Error = InvalidOptionError;

    fn try_from(value: &SchemaLocation) -> Result<Self, Self::Error> {
        match value {
            SchemaLocation::File { .. } => Ok(Self::None),
            SchemaLocation::Confluent { .. } => Ok(Self::Confluent),
            SchemaLocation::Glue { .. } => bail_invalid_option_error!(
                "encode protobuf from aws glue schema registry not supported yet"
            ),
            SchemaLocation::Pulsar { .. } => bail_invalid_option_error!(
                "encode protobuf from Pulsar schema registry not supported yet"
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProtobufParserConfig {
    wire_type: WireType,
    pub(crate) message_descriptor: MessageDescriptor,
    messages_as_jsonb: HashSet<String>,
}

impl ProtobufParserConfig {
    pub async fn new(encoding_properties: EncodingProperties) -> ConnectorResult<Self> {
        let protobuf_config = try_match_expand!(encoding_properties, EncodingProperties::Protobuf)?;
        let message_name = &protobuf_config.message_name;

        let wire_type = (&protobuf_config.schema_location).try_into()?;
        if protobuf_config.key_message_name.is_some() {
            // https://docs.confluent.io/platform/7.5/control-center/topics/schema.html#c3-schemas-best-practices-key-value-pairs
            bail!("protobuf key is not supported");
        }
        let pool = match protobuf_config.schema_location {
            SchemaLocation::Confluent {
                urls,
                client_config,
                name_strategy,
                topic,
            } => {
                let url = handle_sr_list(urls.as_str())?;
                let client = Client::new(url, &client_config)?;
                let loader = SchemaLoader::Confluent(ConfluentSchemaLoader {
                    client,
                    name_strategy,
                    topic,
                    key_record_name: None,
                    val_record_name: Some(message_name.clone()),
                });
                let (_schema_id, root_file_descriptor) = loader
                    .load_val_schema::<FileDescriptor>()
                    .await
                    .context("load schema failed")?;
                root_file_descriptor.parent_pool().clone()
            }
            SchemaLocation::File {
                url,
                aws_auth_props,
            } => {
                let url = handle_sr_list(url.as_str())?;
                let url = url.first().unwrap();
                let schema_bytes = bytes_from_url(url, aws_auth_props.as_ref()).await?;
                DescriptorPool::decode(schema_bytes.as_slice())
                    .with_context(|| format!("cannot build descriptor pool from schema `{url}`"))?
            }
            SchemaLocation::Glue { .. } => bail_invalid_option_error!(
                "encode protobuf from aws glue schema registry not supported yet"
            ),
            SchemaLocation::Pulsar { .. } => bail_invalid_option_error!(
                "encode protobuf from Pulsar schema registry not supported yet"
            ),
        };

        let message_descriptor = pool
            .get_message_by_name(message_name)
            .with_context(|| format!("cannot find message `{message_name}` in schema"))?;

        Ok(Self {
            message_descriptor,
            wire_type,
            messages_as_jsonb: protobuf_config.messages_as_jsonb,
        })
    }

    /// Maps the protobuf schema to relational schema.
    pub fn map_to_columns(&self) -> ConnectorResult<Vec<Field>> {
        pb_schema_to_fields(&self.message_descriptor, &self.messages_as_jsonb).map_err(|e| e.into())
    }
}

/// A port from the implementation of confluent's Varint Zig-zag deserialization.
/// See `ReadVarint` in <https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/utils/ByteUtils.java>
fn decode_varint_zigzag(buffer: &[u8]) -> ConnectorResult<(i32, usize)> {
    // We expect the decoded number to be 4 bytes.
    let mut value = 0u32;
    let mut shift = 0;
    let mut len = 0usize;

    for &byte in buffer {
        len += 1;
        // The Varint encoding is limited to 5 bytes.
        if len > 5 {
            break;
        }
        // The byte is cast to u32 to avoid shifting overflow.
        let byte_ext = byte as u32;
        // In Varint encoding, the lowest 7 bits are used to represent number,
        // while the highest zero bit indicates the end of the number with Varint encoding.
        value |= (byte_ext & 0x7F) << shift;
        if byte_ext & 0x80 == 0 {
            return Ok((((value >> 1) as i32) ^ -((value & 1) as i32), len));
        }

        shift += 7;
    }

    Err(WireFormatError::ParseMessageIndexes.into())
}

/// Reference: <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>
/// Wire format for Confluent pb header is:
/// | 0          | 1-4        | 5-x             | x+1-end
/// | magic-byte | schema-id  | message-indexes | protobuf-payload
pub(crate) fn resolve_pb_header(payload: &[u8]) -> ConnectorResult<&[u8]> {
    // there's a message index array at the front of payload
    // if it is the first message in proto def, the array is just and `0`
    let (_, remained) = extract_schema_id(payload)?;
    // The message indexes are encoded as int using variable-length zig-zag encoding,
    // prefixed by the length of the array.
    // Note that if the first byte is 0, it is equivalent to (1, 0) as an optimization.
    match remained.first() {
        Some(0) => Ok(&remained[1..]),
        Some(_) => {
            let (index_len, mut offset) = decode_varint_zigzag(remained)?;
            for _ in 0..index_len {
                offset += decode_varint_zigzag(&remained[offset..])?.1;
            }
            Ok(&remained[offset..])
        }
        None => bail!("The proto payload is empty"),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_decode_varint_zigzag() {
        // 1. Positive number
        let buffer = vec![0x02];
        let (value, len) = decode_varint_zigzag(&buffer).unwrap();
        assert_eq!(value, 1);
        assert_eq!(len, 1);

        // 2. Negative number
        let buffer = vec![0x01];
        let (value, len) = decode_varint_zigzag(&buffer).unwrap();
        assert_eq!(value, -1);
        assert_eq!(len, 1);

        // 3. Larger positive number
        let buffer = vec![0x9E, 0x03];
        let (value, len) = decode_varint_zigzag(&buffer).unwrap();
        assert_eq!(value, 207);
        assert_eq!(len, 2);

        // 4. Larger negative number
        let buffer = vec![0xBF, 0x07];
        let (value, len) = decode_varint_zigzag(&buffer).unwrap();
        assert_eq!(value, -480);
        assert_eq!(len, 2);

        // 5. Maximum positive number
        let buffer = vec![0xFE, 0xFF, 0xFF, 0xFF, 0x0F];
        let (value, len) = decode_varint_zigzag(&buffer).unwrap();
        assert_eq!(value, i32::MAX);
        assert_eq!(len, 5);

        // 6. Maximum negative number
        let buffer = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x0F];
        let (value, len) = decode_varint_zigzag(&buffer).unwrap();
        assert_eq!(value, i32::MIN);
        assert_eq!(len, 5);

        // 7. More than 32 bits
        let buffer = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x7F];
        let (value, len) = decode_varint_zigzag(&buffer).unwrap();
        assert_eq!(value, i32::MIN);
        assert_eq!(len, 5);

        // 8. Invalid input (more than 5 bytes)
        let buffer = vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        let result = decode_varint_zigzag(&buffer);
        assert!(result.is_err());
    }
}
