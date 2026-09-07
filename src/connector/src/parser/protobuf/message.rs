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

use crate::schema::schema_registry::{WireFormatError, extract_schema_id};

#[derive(Debug, thiserror::Error)]
pub(super) enum ConfluentProtobufMessageError {
    #[error(transparent)]
    WireFormat(#[from] WireFormatError),
    #[error("protobuf schema ID must be non-negative, got {0}")]
    NegativeSchemaId(i32),
    #[error("protobuf message indexes are missing")]
    MissingMessageIndexes,
    #[error("protobuf message index count must be positive, got {0}")]
    NonPositiveMessageIndexCount(i32),
    #[error("protobuf message index must be non-negative, got {0}")]
    NegativeMessageIndex(i32),
}

/// A Protobuf message split into its Confluent metadata and encoded payload.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ConfluentProtobufMessage<'a> {
    pub schema_id: i32,
    pub message_indexes: Vec<u32>,
    pub payload: &'a [u8],
}

/// A port of Confluent's signed varint (zig-zag) decoding.
fn decode_varint_zigzag(buffer: &[u8]) -> Result<(i32, usize), ConfluentProtobufMessageError> {
    let mut value = 0u32;

    for (offset, &byte) in buffer.iter().take(5).enumerate() {
        if offset == 4 && byte & 0xf0 != 0 {
            return Err(WireFormatError::ParseMessageIndexes.into());
        }
        value |= ((byte & 0x7f) as u32) << (offset * 7);
        if byte & 0x80 == 0 {
            let decoded = ((value >> 1) as i32) ^ -((value & 1) as i32);
            return Ok((decoded, offset + 1));
        }
    }

    Err(WireFormatError::ParseMessageIndexes.into())
}

/// Parses the Protobuf-specific part of the Confluent wire format.
///
/// The Confluent Protobuf wire format is:
///
/// ```text
/// | byte 0     | bytes 1-4              | bytes 5..N      | remaining bytes  |
/// | magic byte | schema ID (big-endian) | message indexes | protobuf payload |
/// ```
pub(super) fn parse_confluent_protobuf_message(
    payload: &[u8],
) -> Result<ConfluentProtobufMessage<'_>, ConfluentProtobufMessageError> {
    let (schema_id, remained) = extract_schema_id(payload)?;
    if schema_id < 0 {
        return Err(ConfluentProtobufMessageError::NegativeSchemaId(schema_id));
    }
    let Some(&first) = remained.first() else {
        return Err(ConfluentProtobufMessageError::MissingMessageIndexes);
    };

    // Confluent encodes the common message-index array `[0]` as a single zero byte,
    // without an explicit array length, so the Protobuf payload starts after this byte.
    if first == 0 {
        return Ok(ConfluentProtobufMessage {
            schema_id,
            message_indexes: vec![0],
            payload: &remained[1..],
        });
    }

    let (index_count, mut offset) = decode_varint_zigzag(remained)?;
    if index_count <= 0 {
        return Err(ConfluentProtobufMessageError::NonPositiveMessageIndexCount(
            index_count,
        ));
    }
    if index_count as usize > remained.len().saturating_sub(offset) {
        return Err(WireFormatError::ParseMessageIndexes.into());
    }

    let mut message_indexes = Vec::with_capacity(index_count as usize);
    for _ in 0..index_count {
        let Some(index_bytes) = remained.get(offset..) else {
            return Err(WireFormatError::ParseMessageIndexes.into());
        };
        let (index, encoded_len) = decode_varint_zigzag(index_bytes)?;
        if index < 0 {
            return Err(ConfluentProtobufMessageError::NegativeMessageIndex(index));
        }
        message_indexes.push(index as u32);
        offset += encoded_len;
    }

    Ok(ConfluentProtobufMessage {
        schema_id,
        message_indexes,
        payload: &remained[offset..],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn framed(indexes: &[u8], payload: &[u8]) -> Vec<u8> {
        [&[0, 0, 0, 0, 7][..], indexes, payload].concat()
    }

    #[test]
    fn test_decode_varint_zigzag() {
        assert_eq!(decode_varint_zigzag(&[0x02]).unwrap(), (1, 1));
        assert_eq!(decode_varint_zigzag(&[0x01]).unwrap(), (-1, 1));
        assert_eq!(decode_varint_zigzag(&[0x9e, 0x03]).unwrap(), (207, 2));
        assert_eq!(decode_varint_zigzag(&[0xbf, 0x07]).unwrap(), (-480, 2));
        assert_eq!(
            decode_varint_zigzag(&[0xfe, 0xff, 0xff, 0xff, 0x0f]).unwrap(),
            (i32::MAX, 5)
        );
        assert_eq!(
            decode_varint_zigzag(&[0xff, 0xff, 0xff, 0xff, 0x0f]).unwrap(),
            (i32::MIN, 5)
        );
        assert!(decode_varint_zigzag(&[0xff, 0xff, 0xff, 0xff, 0x7f]).is_err());
        assert!(decode_varint_zigzag(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff]).is_err());
    }

    #[test]
    fn test_optimized_first_message_index() {
        let bytes = framed(&[0], &[8, 1]);
        let message = parse_confluent_protobuf_message(&bytes).unwrap();
        assert_eq!(message.schema_id, 7);
        assert_eq!(message.message_indexes, [0]);
        assert_eq!(message.payload, [8, 1]);
    }

    #[test]
    fn test_top_level_and_nested_message_indexes() {
        // Zig-zag encoded [count = 2, indexes = 1, 0].
        let bytes = framed(&[4, 2, 0], &[8, 1]);
        let message = parse_confluent_protobuf_message(&bytes).unwrap();
        assert_eq!(message.message_indexes, [1, 0]);
        assert_eq!(message.payload, [8, 1]);
    }

    #[test]
    fn test_invalid_message_indexes() {
        assert!(matches!(
            parse_confluent_protobuf_message(&framed(&[], &[])),
            Err(ConfluentProtobufMessageError::MissingMessageIndexes)
        ));
        assert!(matches!(
            parse_confluent_protobuf_message(&framed(&[1], &[])),
            Err(ConfluentProtobufMessageError::NonPositiveMessageIndexCount(
                -1
            ))
        ));
        assert!(matches!(
            parse_confluent_protobuf_message(&framed(&[2, 1], &[])),
            Err(ConfluentProtobufMessageError::NegativeMessageIndex(-1))
        ));
        assert!(matches!(
            parse_confluent_protobuf_message(&framed(&[2, 0x80], &[])),
            Err(ConfluentProtobufMessageError::WireFormat(
                WireFormatError::ParseMessageIndexes
            ))
        ));
        assert!(
            parse_confluent_protobuf_message(&framed(&[0xfe, 0xff, 0xff, 0xff, 0x7f], &[]))
                .is_err()
        );
    }

    #[test]
    fn test_negative_schema_id() {
        let bytes = [0, 0xff, 0xff, 0xff, 0xff, 0];
        assert!(matches!(
            parse_confluent_protobuf_message(&bytes),
            Err(ConfluentProtobufMessageError::NegativeSchemaId(-1))
        ));
    }
}
