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

use std::borrow::Cow;
use std::collections::HashSet;

use anyhow::Context;
use itertools::Itertools;
use prost_reflect::{
    Cardinality, DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, ReflectMessage, Value,
};
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::catalog::Field;
use risingwave_common::types::{
    DataType, DatumCow, Decimal, F32, F64, JsonbVal, MapType, MapValue, ScalarImpl, StructType,
    ToOwnedDatum,
};
use thiserror::Error;
use thiserror_ext::Macro;

use crate::decoder::{AccessError, AccessResult, uncategorized};

pub const PROTOBUF_MESSAGES_AS_JSONB: &str = "messages_as_jsonb";

pub fn pb_schema_to_fields(
    message_descriptor: &MessageDescriptor,
    messages_as_jsonb: &HashSet<String>,
) -> anyhow::Result<Vec<Field>> {
    let mut parse_trace: Vec<String> = vec![];
    message_descriptor
        .fields()
        .map(|field| {
            let field_type = protobuf_type_mapping(&field, &mut parse_trace, messages_as_jsonb)
                .context("failed to map protobuf type")?;
            let column = Field::new(field.name(), field_type);
            Ok(column)
        })
        .collect()
}

#[derive(Error, Debug, Macro)]
#[error("{0}")]
struct ProtobufTypeError(#[message] String);

fn detect_loop_and_push(
    trace: &mut Vec<String>,
    fd: &FieldDescriptor,
) -> std::result::Result<(), ProtobufTypeError> {
    let identifier = format!("{}({})", fd.name(), fd.full_name());
    if trace.iter().any(|s| s == identifier.as_str()) {
        bail_protobuf_type_error!(
            "circular reference detected: {}, conflict with {}, kind {:?}. Adding {:?} to {:?} may help.",
            trace.iter().format("->"),
            identifier,
            fd.kind(),
            fd.kind(),
            PROTOBUF_MESSAGES_AS_JSONB,
        );
    }
    trace.push(identifier);
    Ok(())
}

/// Converts a protobuf message field to a datum.
///
/// We will get the protobuf value from the message by checking the field descriptor and correctly
/// handling presence, then call [`from_protobuf_value`].
pub fn from_protobuf_message_field<'a>(
    field_desc: &FieldDescriptor,
    reader_field_desc: Option<&FieldDescriptor>,
    message: &'a DynamicMessage,
    type_expected: &DataType,
    messages_as_jsonb: &'a HashSet<String>,
) -> AccessResult<DatumCow<'a>> {
    let value = if field_desc.supports_presence() && !message.has_field(field_desc) {
        // The field supports presence and it's absent in the message. Treat it as NULL.
        // This is the case for `optional` fields, message fields, and fields contained in `oneof`.
        return Ok(DatumCow::NULL);
    } else {
        // Otherwise, directly call `get_field`, which will return the default value if absent.
        message.get_field(field_desc)
    };

    match value {
        Cow::Borrowed(value) => from_protobuf_value(
            field_desc,
            reader_field_desc,
            value,
            type_expected,
            messages_as_jsonb,
        ),
        Cow::Owned(value) => from_protobuf_value(
            field_desc,
            reader_field_desc,
            &value,
            type_expected,
            messages_as_jsonb,
        )
        .map(|d| d.to_owned_datum().into()),
    }
}

/// Converts a protobuf value to a datum.
fn from_protobuf_value<'a>(
    field_desc: &FieldDescriptor,
    reader_field_desc: Option<&FieldDescriptor>,
    value: &'a Value,
    type_expected: &DataType,
    messages_as_jsonb: &'a HashSet<String>,
) -> AccessResult<DatumCow<'a>> {
    let kind = field_desc.kind();

    macro_rules! borrowed {
        ($v:expr) => {
            return Ok(DatumCow::Borrowed(Some($v.into())))
        };
    }

    let v: ScalarImpl = match value {
        Value::Bool(v) => ScalarImpl::Bool(*v),
        Value::I32(i) => ScalarImpl::Int32(*i),
        Value::U32(i) => ScalarImpl::Int64(*i as i64),
        Value::I64(i) => ScalarImpl::Int64(*i),
        Value::U64(i) => ScalarImpl::Decimal(Decimal::from(*i)),
        Value::F32(f) => ScalarImpl::Float32(F32::from(*f)),
        Value::F64(f) => ScalarImpl::Float64(F64::from(*f)),
        Value::String(s) => borrowed!(s.as_str()),
        Value::EnumNumber(idx) => {
            let enum_desc = kind.as_enum().ok_or_else(|| AccessError::TypeError {
                expected: "enum".to_owned(),
                got: format!("{kind:?}"),
                value: value.to_string(),
            })?;
            let enum_symbol = enum_desc.get_value(*idx).ok_or_else(|| {
                uncategorized!("unknown enum index {} of enum {:?}", idx, enum_desc)
            })?;
            ScalarImpl::Utf8(enum_symbol.name().into())
        }
        Value::Message(dyn_msg) => {
            if messages_as_jsonb.contains(dyn_msg.descriptor().full_name()) {
                ScalarImpl::Jsonb(JsonbVal::from(
                    serde_json::to_value(dyn_msg).map_err(AccessError::ProtobufAnyToJson)?,
                ))
            } else {
                let desc = dyn_msg.descriptor();
                let reader_desc = reader_field_desc.and_then(|field| match field.kind() {
                    Kind::Message(message) => Some(message),
                    _ => None,
                });
                let DataType::Struct(st) = type_expected else {
                    return Err(AccessError::TypeError {
                        expected: type_expected.to_string(),
                        got: desc.full_name().to_owned(),
                        value: value.to_string(), // Protobuf TEXT
                    });
                };

                let mut datums = Vec::with_capacity(st.len());
                for (name, expected_field_type) in st.iter() {
                    let reader_field = reader_desc
                        .as_ref()
                        .and_then(|descriptor| descriptor.get_field_by_name(name));
                    let writer_field = match &reader_field {
                        Some(reader_field) => desc.get_field(reader_field.number()),
                        None => desc.get_field_by_name(name),
                    };
                    let Some(writer_field) = writer_field else {
                        // The field does not exist in the writer schema. Use SQL NULL instead of a
                        // default from the reader schema.
                        datums.push(None);
                        continue;
                    };
                    let datum = from_protobuf_message_field(
                        &writer_field,
                        reader_field.as_ref(),
                        dyn_msg,
                        expected_field_type,
                        messages_as_jsonb,
                    )?;
                    datums.push(datum.to_owned_datum());
                }
                ScalarImpl::Struct(StructValue::new(datums))
            }
        }
        Value::List(values) => {
            let DataType::List(list_type) = type_expected else {
                return Err(AccessError::TypeError {
                    expected: type_expected.to_string(),
                    got: format!("repeated {:?}", kind),
                    value: value.to_string(), // Protobuf TEXT
                });
            };
            let elem_type = list_type.elem();
            let mut builder = elem_type.create_array_builder(values.len());
            for value in values {
                builder.append(from_protobuf_value(
                    field_desc,
                    reader_field_desc,
                    value,
                    elem_type,
                    messages_as_jsonb,
                )?);
            }
            ScalarImpl::List(ListValue::new(builder.finish()))
        }
        Value::Bytes(value) => borrowed!(&**value),
        Value::Map(map) => {
            let err = || {
                AccessError::TypeError {
                    expected: type_expected.to_string(),
                    got: format!("{:?}", kind),
                    value: value.to_string(), // Protobuf TEXT
                }
            };

            let DataType::Map(map_type) = type_expected else {
                return Err(err());
            };
            if !field_desc.is_map() {
                return Err(err());
            }
            let map_desc = kind.as_message().ok_or_else(err)?;
            let reader_map_desc = reader_field_desc.and_then(|field| match field.kind() {
                Kind::Message(message) if message.is_map_entry() => Some(message),
                _ => None,
            });
            let reader_key_field = reader_map_desc
                .as_ref()
                .map(MessageDescriptor::map_entry_key_field);
            let reader_value_field = reader_map_desc
                .as_ref()
                .map(MessageDescriptor::map_entry_value_field);

            let mut key_builder = map_type.key().create_array_builder(map.len());
            let mut value_builder = map_type.value().create_array_builder(map.len());
            // NOTE: HashMap's iter order is non-deterministic, but MapValue's
            // order matters. We sort by key here to have deterministic order
            // in tests. We might consider removing this, or make all MapValue sorted
            // in the future.
            for (key, value) in map.iter().sorted_by_key(|(k, _v)| *k) {
                key_builder.append(from_protobuf_value(
                    &map_desc.map_entry_key_field(),
                    reader_key_field.as_ref(),
                    &key.clone().into(),
                    map_type.key(),
                    messages_as_jsonb,
                )?);
                value_builder.append(from_protobuf_value(
                    &map_desc.map_entry_value_field(),
                    reader_value_field.as_ref(),
                    value,
                    map_type.value(),
                    messages_as_jsonb,
                )?);
            }
            let keys = key_builder.finish();
            let values = value_builder.finish();
            ScalarImpl::Map(
                MapValue::try_from_kv(ListValue::new(keys), ListValue::new(values))
                    .map_err(|e| uncategorized!("failed to convert protobuf map: {e}"))?,
            )
        }
    };
    Ok(Some(v).into())
}

/// Maps protobuf type to RW type.
fn protobuf_type_mapping(
    field_descriptor: &FieldDescriptor,
    parse_trace: &mut Vec<String>,
    messages_as_jsonb: &HashSet<String>,
) -> std::result::Result<DataType, ProtobufTypeError> {
    detect_loop_and_push(parse_trace, field_descriptor)?;
    let mut t = match field_descriptor.kind() {
        Kind::Bool => DataType::Boolean,
        Kind::Double => DataType::Float64,
        Kind::Float => DataType::Float32,
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => DataType::Int32,
        // Fixed32 represents [0, 2^32 - 1]. It's equal to u32.
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 | Kind::Uint32 | Kind::Fixed32 => {
            DataType::Int64
        }
        Kind::Uint64 | Kind::Fixed64 => DataType::Decimal,
        Kind::String => DataType::Varchar,
        Kind::Message(m) => {
            if messages_as_jsonb.contains(m.full_name()) {
                // Well-Known Types are identified by their full name
                DataType::Jsonb
            } else if m.is_map_entry() {
                // Map is equivalent to `repeated MapFieldEntry map_field = N;`
                debug_assert!(field_descriptor.is_map());
                let key = protobuf_type_mapping(
                    &m.map_entry_key_field(),
                    parse_trace,
                    messages_as_jsonb,
                )?;
                let value = protobuf_type_mapping(
                    &m.map_entry_value_field(),
                    parse_trace,
                    messages_as_jsonb,
                )?;
                _ = parse_trace.pop();
                return Ok(DataType::Map(MapType::from_kv(key, value)));
            } else {
                let fields = m
                    .fields()
                    .map(|f| {
                        Ok((
                            f.name().to_owned(),
                            protobuf_type_mapping(&f, parse_trace, messages_as_jsonb)?,
                        ))
                    })
                    .try_collect::<_, Vec<_>, _>()?;
                StructType::new(fields).into()
            }
        }
        Kind::Enum(_) => DataType::Varchar,
        Kind::Bytes => DataType::Bytea,
    };
    if field_descriptor.cardinality() == Cardinality::Repeated {
        debug_assert!(!field_descriptor.is_map());
        t = DataType::list(t)
    }
    _ = parse_trace.pop();
    Ok(t)
}
