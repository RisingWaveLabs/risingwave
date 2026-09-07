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

use bytes::{BufMut, Bytes};
use prost::Message;
use prost_reflect::{
    DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, ReflectMessage, Value,
};
use risingwave_common::array::VECTOR_ITEM_TYPE;
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, DatumRef, MapType, ScalarRefImpl, StructType};
use risingwave_common::util::iter_util::ZipEqDebug;

use super::{FieldEncodeError, Result as SinkResult, RowEncoder, SerTo};

type Result<T> = std::result::Result<T, FieldEncodeError>;

pub struct ProtoEncoder {
    schema: Schema,
    col_indices: Option<Vec<usize>>,
    descriptor: MessageDescriptor,
    header: ProtoHeader,
}

#[derive(Debug, Clone, Copy)]
pub enum ProtoHeader {
    None,
    /// <https://docs.confluent.io/platform/7.5/schema-registry/fundamentals/serdes-develop/index.html#messages-wire-format>
    ///
    /// * 00
    /// * 4-byte big-endian schema ID
    ConfluentSchemaRegistry(i32),
}

impl ProtoEncoder {
    pub fn new(
        schema: Schema,
        col_indices: Option<Vec<usize>>,
        descriptor: MessageDescriptor,
        header: ProtoHeader,
    ) -> SinkResult<Self> {
        match &col_indices {
            Some(col_indices) => validate_fields(
                col_indices.iter().map(|idx| {
                    let f = &schema[*idx];
                    (f.name.as_str(), &f.data_type)
                }),
                &descriptor,
            )?,
            None => validate_fields(
                schema
                    .fields
                    .iter()
                    .map(|f| (f.name.as_str(), &f.data_type)),
                &descriptor,
            )?,
        };

        Ok(Self {
            schema,
            col_indices,
            descriptor,
            header,
        })
    }
}

pub struct ProtoEncoded {
    pub message: DynamicMessage,
    header: ProtoHeader,
}

impl RowEncoder for ProtoEncoder {
    type Output = ProtoEncoded;

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn col_indices(&self) -> Option<&[usize]> {
        self.col_indices.as_deref()
    }

    fn encode_cols(
        &self,
        row: impl Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> SinkResult<Self::Output> {
        encode_fields(
            col_indices.map(|idx| {
                let f = &self.schema[idx];
                ((f.name.as_str(), &f.data_type), row.datum_at(idx))
            }),
            &self.descriptor,
        )
        .map_err(Into::into)
        .map(|m| ProtoEncoded {
            message: m,
            header: self.header,
        })
    }
}

impl SerTo<Vec<u8>> for ProtoEncoded {
    fn ser_to(self) -> SinkResult<Vec<u8>> {
        let mut buf = Vec::new();
        match self.header {
            ProtoHeader::None => { /* noop */ }
            ProtoHeader::ConfluentSchemaRegistry(schema_id) => {
                buf.reserve(1 + 4);
                buf.put_u8(0);
                buf.put_i32(schema_id);
                MessageIndexes::from(self.message.descriptor()).encode(&mut buf);
            }
        }
        self.message.encode(&mut buf).unwrap();
        Ok(buf)
    }
}

struct MessageIndexes(Vec<i32>);

impl MessageIndexes {
    fn from(desc: MessageDescriptor) -> Self {
        // https://github.com/protocolbuffers/protobuf/blob/v25.1/src/google/protobuf/descriptor.proto
        // https://docs.rs/prost-reflect/0.12.0/src/prost_reflect/descriptor/tag.rs.html
        // https://docs.rs/prost-reflect/0.12.0/src/prost_reflect/descriptor/build/visit.rs.html#125
        // `FileDescriptorProto` field #4 is `repeated DescriptorProto message_type`
        const TAG_FILE_MESSAGE: i32 = 4;
        // `DescriptorProto` field #3 is `repeated DescriptorProto nested_type`
        const TAG_MESSAGE_NESTED: i32 = 3;

        let mut indexes = vec![];
        let mut path = desc.path().iter().copied().array_chunks();
        let [tag, idx] = path.next().unwrap();
        assert_eq!(tag, TAG_FILE_MESSAGE);
        indexes.push(idx);
        for [tag, idx] in path {
            assert_eq!(tag, TAG_MESSAGE_NESTED);
            indexes.push(idx);
        }
        Self(indexes)
    }

    fn zig_i32(value: i32, buf: &mut impl BufMut) {
        let unsigned = ((value << 1) ^ (value >> 31)) as u32 as u64;
        prost::encoding::encode_varint(unsigned, buf);
    }

    fn encode(&self, buf: &mut impl BufMut) {
        if self.0 == [0] {
            buf.put_u8(0);
            return;
        }
        Self::zig_i32(self.0.len().try_into().unwrap(), buf);
        for &idx in &self.0 {
            Self::zig_i32(idx, buf);
        }
    }
}

/// A trait that assists code reuse between `validate` and `encode`.
/// * For `validate`, the inputs are (RisingWave type, ProtoBuf type).
/// * For `encode`, the inputs are (RisingWave type, RisingWave data, ProtoBuf type).
///
/// Thus we impl [`MaybeData`] for both `()` and [`ScalarRefImpl`].
trait MaybeData: std::fmt::Debug {
    type Out;

    fn on_base(self, f: impl FnOnce(ScalarRefImpl<'_>) -> Result<Value>) -> Result<Self::Out>;

    fn on_struct(self, st: &StructType, pb: &MessageDescriptor) -> Result<Self::Out>;

    fn on_list(self, elem: &DataType, pb: &FieldDescriptor) -> Result<Self::Out>;

    fn on_map(self, m: &MapType, pb: &MessageDescriptor) -> Result<Self::Out>;
}

impl MaybeData for () {
    type Out = ();

    fn on_base(self, _: impl FnOnce(ScalarRefImpl<'_>) -> Result<Value>) -> Result<Self::Out> {
        Ok(self)
    }

    fn on_struct(self, st: &StructType, pb: &MessageDescriptor) -> Result<Self::Out> {
        validate_fields(st.iter(), pb)
    }

    fn on_list(self, elem: &DataType, pb: &FieldDescriptor) -> Result<Self::Out> {
        on_field(elem, (), pb, true)
    }

    fn on_map(self, elem: &MapType, pb: &MessageDescriptor) -> Result<Self::Out> {
        debug_assert!(pb.is_map_entry());
        on_field(elem.key(), (), &pb.map_entry_key_field(), false)?;
        on_field(elem.value(), (), &pb.map_entry_value_field(), false)?;
        Ok(())
    }
}

/// Nullability is not part of type system in proto.
/// * Top level is always a message.
/// * All message fields can be omitted in proto3.
/// * All repeated elements must have a value.
///
/// So we handle [`ScalarRefImpl`] rather than [`DatumRef`] here.
impl MaybeData for ScalarRefImpl<'_> {
    type Out = Value;

    fn on_base(self, f: impl FnOnce(ScalarRefImpl<'_>) -> Result<Value>) -> Result<Self::Out> {
        f(self)
    }

    fn on_struct(self, st: &StructType, pb: &MessageDescriptor) -> Result<Self::Out> {
        let d = self.into_struct();
        let message = encode_fields(st.iter().zip_eq_debug(d.iter_fields_ref()), pb)?;
        Ok(Value::Message(message))
    }

    fn on_list(self, elem: &DataType, pb: &FieldDescriptor) -> Result<Self::Out> {
        let d = self.into_list();
        let vs = d
            .iter()
            .map(|d| {
                on_field(
                    elem,
                    d.ok_or_else(|| {
                        FieldEncodeError::new("array containing null not allowed as repeated field")
                    })?,
                    pb,
                    true,
                )
            })
            .try_collect()?;
        Ok(Value::List(vs))
    }

    fn on_map(self, m: &MapType, pb: &MessageDescriptor) -> Result<Self::Out> {
        debug_assert!(pb.is_map_entry());
        let vs = self
            .into_map()
            .iter()
            .map(|(k, v)| {
                let v =
                    v.ok_or_else(|| FieldEncodeError::new("map containing null not allowed"))?;
                let k = on_field(m.key(), k, &pb.map_entry_key_field(), false)?;
                let v = on_field(m.value(), v, &pb.map_entry_value_field(), false)?;
                Ok((
                    k.into_map_key().ok_or_else(|| {
                        FieldEncodeError::new("failed to convert map key to proto")
                    })?,
                    v,
                ))
            })
            .try_collect()?;
        Ok(Value::Map(vs))
    }
}

fn validate_fields<'a>(
    fields: impl Iterator<Item = (&'a str, &'a DataType)>,
    descriptor: &MessageDescriptor,
) -> Result<()> {
    for (name, t) in fields {
        let Some(proto_field) = descriptor.get_field_by_name(name) else {
            return Err(FieldEncodeError::new("field not in proto").with_name(name));
        };
        if proto_field.cardinality() == prost_reflect::Cardinality::Required {
            return Err(FieldEncodeError::new("`required` not supported").with_name(name));
        }
        on_field(t, (), &proto_field, false).map_err(|e| e.with_name(name))?;
    }
    Ok(())
}

fn encode_fields<'a>(
    fields_with_datums: impl Iterator<Item = ((&'a str, &'a DataType), DatumRef<'a>)>,
    descriptor: &MessageDescriptor,
) -> Result<DynamicMessage> {
    let mut message = DynamicMessage::new(descriptor.clone());
    for ((name, t), d) in fields_with_datums {
        let proto_field = descriptor.get_field_by_name(name).unwrap();
        // On `null`, simply skip setting the field.
        if let Some(scalar) = d {
            let value = on_field(t, scalar, &proto_field, false).map_err(|e| e.with_name(name))?;
            message
                .try_set_field(&proto_field, value)
                .map_err(|e| FieldEncodeError::new(e).with_name(name))?;
        }
    }
    Ok(message)
}

// Full name of Well-Known Types
const WKT_TIMESTAMP: &str = "google.protobuf.Timestamp";
#[expect(dead_code)]
const WKT_BOOL_VALUE: &str = "google.protobuf.BoolValue";

/// Handles both `validate` (without actual data) and `encode`.
/// See [`MaybeData`] for more info.
fn on_field<D: MaybeData>(
    data_type: &DataType,
    maybe: D,
    proto_field: &FieldDescriptor,
    in_repeated: bool,
) -> Result<D::Out> {
    // Regarding (proto_field.is_list, in_repeated):
    // (F, T) => impossible
    // (F, F) => encoding to a non-repeated field
    // (T, F) => encoding to a repeated field
    // (T, T) => encoding to an element of a repeated field
    // In the bottom 2 cases, we need to distinguish the same `proto_field` with the help of `in_repeated`.
    assert!(proto_field.is_list() || !in_repeated);
    let expect_list = proto_field.is_list() && !in_repeated;
    if proto_field.is_group() {
        return Err(FieldEncodeError::new("proto group not supported yet"));
    }

    let no_match_err = || {
        Err(FieldEncodeError::new(format!(
            "cannot encode {} column as {}{:?} field",
            data_type,
            if expect_list { "repeated " } else { "" },
            proto_field.kind()
        )))
    };

    if expect_list && !matches!(data_type, DataType::List(_)) {
        return no_match_err();
    }

    let value = match &data_type {
        // Group A: perfect match between RisingWave types and ProtoBuf types
        DataType::Boolean => match proto_field.kind() {
            Kind::Bool => maybe.on_base(|s| Ok(Value::Bool(s.into_bool())))?,
            _ => return no_match_err(),
        },
        DataType::Varchar => match proto_field.kind() {
            Kind::String => maybe.on_base(|s| Ok(Value::String(s.into_utf8().into())))?,
            Kind::Enum(enum_desc) => maybe.on_base(|s| {
                let name = s.into_utf8();
                let enum_value_desc = enum_desc.get_value_by_name(name).ok_or_else(|| {
                    FieldEncodeError::new(format!("'{name}' not in enum {}", enum_desc.name()))
                })?;
                Ok(Value::EnumNumber(enum_value_desc.number()))
            })?,
            _ => return no_match_err(),
        },
        DataType::Bytea => match proto_field.kind() {
            Kind::Bytes => {
                maybe.on_base(|s| Ok(Value::Bytes(Bytes::copy_from_slice(s.into_bytea()))))?
            }
            _ => return no_match_err(),
        },
        DataType::Float32 => match proto_field.kind() {
            Kind::Float => maybe.on_base(|s| Ok(Value::F32(s.into_float32().into())))?,
            _ => return no_match_err(),
        },
        DataType::Float64 => match proto_field.kind() {
            Kind::Double => maybe.on_base(|s| Ok(Value::F64(s.into_float64().into())))?,
            _ => return no_match_err(),
        },
        DataType::Int32 => match proto_field.kind() {
            Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
                maybe.on_base(|s| Ok(Value::I32(s.into_int32())))?
            }
            _ => return no_match_err(),
        },
        DataType::Int64 => match proto_field.kind() {
            Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
                maybe.on_base(|s| Ok(Value::I64(s.into_int64())))?
            }
            _ => return no_match_err(),
        },
        DataType::Struct(st) => match proto_field.kind() {
            Kind::Message(pb) => maybe.on_struct(st, &pb)?,
            _ => return no_match_err(),
        },
        DataType::List(lt) => match expect_list {
            true => maybe.on_list(lt.elem(), proto_field)?,
            false => return no_match_err(),
        },
        // Group B: match between RisingWave types and ProtoBuf Well-Known types
        DataType::Timestamptz => match proto_field.kind() {
            Kind::Message(pb) if pb.full_name() == WKT_TIMESTAMP => maybe.on_base(|s| {
                let d = s.into_timestamptz();
                let message = prost_types::Timestamp {
                    seconds: d.timestamp(),
                    nanos: d.timestamp_subsec_nanos().try_into().unwrap(),
                };
                Ok(Value::Message(message.transcode_to_dynamic()))
            })?,
            Kind::String => {
                maybe.on_base(|s| Ok(Value::String(s.into_timestamptz().to_string())))?
            }
            _ => return no_match_err(),
        },
        DataType::Jsonb => match proto_field.kind() {
            Kind::String => maybe.on_base(|s| Ok(Value::String(s.into_jsonb().to_string())))?,
            _ => return no_match_err(), /* Value, NullValue, Struct (map), ListValue
                                         * Group C: experimental */
        },
        DataType::Variant => {
            return no_match_err();
        }
        DataType::Int16 => match proto_field.kind() {
            Kind::Int64 => maybe.on_base(|s| Ok(Value::I64(s.into_int16() as i64)))?,
            _ => return no_match_err(),
        },
        DataType::Date => match proto_field.kind() {
            Kind::Int32 => {
                maybe.on_base(|s| Ok(Value::I32(s.into_date().get_nums_days_unix_epoch())))?
            }
            _ => return no_match_err(), // google.type.Date
        },
        DataType::Time => match proto_field.kind() {
            Kind::String => maybe.on_base(|s| Ok(Value::String(s.into_time().to_string())))?,
            _ => return no_match_err(), // google.type.TimeOfDay
        },
        DataType::Timestamp => match proto_field.kind() {
            Kind::String => maybe.on_base(|s| Ok(Value::String(s.into_timestamp().to_string())))?,
            _ => return no_match_err(), // google.type.DateTime
        },
        DataType::Decimal => match proto_field.kind() {
            Kind::String => maybe.on_base(|s| Ok(Value::String(s.into_decimal().to_string())))?,
            _ => return no_match_err(), // google.type.Decimal
        },
        DataType::Interval => match proto_field.kind() {
            Kind::String => {
                maybe.on_base(|s| Ok(Value::String(s.into_interval().as_iso_8601())))?
            }
            _ => return no_match_err(), // Group D: unsupported
        },
        DataType::Serial => match proto_field.kind() {
            Kind::Int64 => maybe.on_base(|s| Ok(Value::I64(s.into_serial().as_row_id())))?,
            _ => return no_match_err(), // Group D: unsupported
        },
        DataType::Int256 => {
            return no_match_err();
        }
        DataType::Map(map_type) => {
            if proto_field.is_map() {
                let msg = match proto_field.kind() {
                    Kind::Message(m) => m,
                    _ => return no_match_err(), // unreachable actually
                };
                return maybe.on_map(map_type, &msg);
            } else {
                return no_match_err();
            }
        }
        DataType::Vector(_) => match expect_list {
            true => maybe.on_list(&VECTOR_ITEM_TYPE, proto_field)?,
            false => return no_match_err(),
        },
    };

    Ok(value)
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::{ArrayBuilder, StructArrayBuilder};
    use risingwave_common::catalog::Field;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{
        ListValue, MapType, MapValue, Scalar, ScalarImpl, StructValue, Timestamptz,
    };

    use super::*;

    #[test]
    fn test_encode_proto_ok() {
        let pool_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("codec/tests/test_data/all-types.pb");
        let pool_bytes = std::fs::read(pool_path).unwrap();
        let pool = prost_reflect::DescriptorPool::decode(pool_bytes.as_ref()).unwrap();
        let descriptor = pool.get_message_by_name("all_types.AllTypes").unwrap();
        let schema = Schema::new(vec![
            Field::with_name(DataType::Boolean, "bool_field"),
            Field::with_name(DataType::Varchar, "string_field"),
            Field::with_name(DataType::Bytea, "bytes_field"),
            Field::with_name(DataType::Float32, "float_field"),
            Field::with_name(DataType::Float64, "double_field"),
            Field::with_name(DataType::Int32, "int32_field"),
            Field::with_name(DataType::Int64, "int64_field"),
            Field::with_name(DataType::Int32, "sint32_field"),
            Field::with_name(DataType::Int64, "sint64_field"),
            Field::with_name(DataType::Int32, "sfixed32_field"),
            Field::with_name(DataType::Int64, "sfixed64_field"),
            Field::with_name(
                DataType::Struct(StructType::new(vec![
                    ("id", DataType::Int32),
                    ("name", DataType::Varchar),
                ])),
                "nested_message_field",
            ),
            Field::with_name(DataType::Int32.list(), "repeated_int_field"),
            Field::with_name(DataType::Timestamptz, "timestamp_field"),
            Field::with_name(
                DataType::Map(MapType::from_kv(DataType::Varchar, DataType::Int32)),
                "map_field",
            ),
            Field::with_name(
                DataType::Map(MapType::from_kv(
                    DataType::Varchar,
                    DataType::Struct(StructType::new(vec![
                        ("id", DataType::Int32),
                        ("name", DataType::Varchar),
                    ])),
                )),
                "map_struct_field",
            ),
        ]);
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Bool(true)),
            Some(ScalarImpl::Utf8("RisingWave".into())),
            Some(ScalarImpl::Bytea([0xbe, 0xef].into())),
            Some(ScalarImpl::Float32(3.5f32.into())),
            Some(ScalarImpl::Float64(4.25f64.into())),
            Some(ScalarImpl::Int32(22)),
            Some(ScalarImpl::Int64(23)),
            Some(ScalarImpl::Int32(24)),
            None,
            Some(ScalarImpl::Int32(26)),
            Some(ScalarImpl::Int64(27)),
            Some(ScalarImpl::Struct(StructValue::new(vec![
                Some(ScalarImpl::Int32(1)),
                Some(ScalarImpl::Utf8("".into())),
            ]))),
            Some(ScalarImpl::List(ListValue::from_iter([4, 0, 4]))),
            Some(ScalarImpl::Timestamptz(
                Timestamptz::from_micros(3).unwrap(),
            )),
            Some(ScalarImpl::Map(
                MapValue::try_from_kv(
                    ListValue::from_iter(["a", "b"]),
                    ListValue::from_iter([1, 2]),
                )
                .unwrap(),
            )),
            {
                let mut struct_array_builder = StructArrayBuilder::with_type(
                    2,
                    DataType::Struct(StructType::new(vec![
                        ("id", DataType::Int32),
                        ("name", DataType::Varchar),
                    ])),
                );
                struct_array_builder.append(Some(
                    StructValue::new(vec![
                        Some(ScalarImpl::Int32(1)),
                        Some(ScalarImpl::Utf8("x".into())),
                    ])
                    .as_scalar_ref(),
                ));
                struct_array_builder.append(Some(
                    StructValue::new(vec![
                        Some(ScalarImpl::Int32(2)),
                        Some(ScalarImpl::Utf8("y".into())),
                    ])
                    .as_scalar_ref(),
                ));
                Some(ScalarImpl::Map(
                    MapValue::try_from_kv(
                        ListValue::from_iter(["a", "b"]),
                        ListValue::new(struct_array_builder.finish().into()),
                    )
                    .unwrap(),
                ))
            },
        ]);

        let encoder = ProtoEncoder::new(schema, None, descriptor, ProtoHeader::None).unwrap();
        let m = encoder.encode(row).unwrap();
        expect_test::expect![[r#"
            field: FieldDescriptor {
                name: "double_field",
                full_name: "all_types.AllTypes.double_field",
                json_name: "doubleField",
                number: 1,
                kind: double,
                cardinality: Optional,
                containing_oneof: None,
                default_value: F64(
                    0.0,
                ),
                is_group: false,
                is_list: false,
                is_map: false,
                is_packed: false,
                supports_presence: false,
            }

            value: F64(4.25)

            ==============================
            field: FieldDescriptor {
                name: "float_field",
                full_name: "all_types.AllTypes.float_field",
                json_name: "floatField",
                number: 2,
                kind: float,
                cardinality: Optional,
                containing_oneof: None,
                default_value: F32(
                    0.0,
                ),
                is_group: false,
                is_list: false,
                is_map: false,
                is_packed: false,
                supports_presence: false,
            }

            value: F32(3.5)

            ==============================
            field: FieldDescriptor {
                name: "int32_field",
                full_name: "all_types.AllTypes.int32_field",
                json_name: "int32Field",
                number: 3,
                kind: int32,
                cardinality: Optional,
                containing_oneof: None,
                default_value: I32(
                    0,
                ),
                is_group: false,
                is_list: false,
                is_map: false,
                is_packed: false,
                supports_presence: false,
            }

            value: I32(22)

            ==============================
            field: FieldDescriptor {
                name: "int64_field",
                full_name: "all_types.AllTypes.int64_field",
                json_name: "int64Field",
                number: 4,
                kind: int64,
                cardinality: Optional,
                containing_oneof: None,
                default_value: I64(
                    0,
                ),
                is_group: false,
                is_list: false,
                is_map: false,
                is_packed: false,
                supports_presence: false,
            }

            value: I64(23)

            ==============================
            field: FieldDescriptor {
                name: "sint32_field",
                full_name: "all_types.AllTypes.sint32_field",
                json_name: "sint32Field",
                number: 7,
                kind: sint32,
                cardinality: Optional,
                containing_oneof: None,
                default_value: I32(
                    0,
                ),
                is_group: false,
                is_list: false,
                is_map: false,
                is_packed: false,
                supports_presence: false,
            }

            value: I32(24)

            ==============================
            field: FieldDescriptor {
                name: "sfixed32_field",
                full_name: "all_types.AllTypes.sfixed32_field",
                json_name: "sfixed32Field",
                number: 11,
                kind: sfixed32,
                cardinality: Optional,
                containing_oneof: None,
                default_value: I32(
                    0,
                ),
                is_group: false,
                is_list: false,
                is_map: false,
                is_packed: false,
                supports_presence: false,
            }

            value: I32(26)

            ==============================
            field: FieldDescriptor {
                name: "sfixed64_field",
                full_name: "all_types.AllTypes.sfixed64_field",
                json_name: "sfixed64Field",
                number: 12,
                kind: sfixed64,
                cardinality: Optional,
                containing_oneof: None,
                default_value: I64(
                    0,
                ),
                is_group: false,
                is_list: false,
                is_map: false,
                is_packed: false,
                supports_presence: false,
            }

            value: I64(27)

            ==============================
            field: FieldDescriptor {
                name: "bool_field",
                full_name: "all_types.AllTypes.bool_field",
                json_name: "boolField",
                number: 13,
                kind: bool,
                cardinality: Optional,
                containing_oneof: None,
                default_value: Bool(
                    false,
                ),
                is_group: false,
                is_list: false,
                is_map: false,
                is_packed: false,
                supports_presence: false,
            }

            value: Bool(true)

            ==============================
            field: FieldDescriptor {
                name: "string_field",
                full_name: "all_types.AllTypes.string_field",
                json_name: "stringField",
                number: 14,
                kind: string,
                cardinality: Optional,
                containing_oneof: None,
                default_value: String(
                    "",
                ),
                is_group: false,
                is_list: false,
                is_map: false,
                is_packed: false,
                supports_presence: false,
            }

            value: String("RisingWave")

            ==============================
            field: FieldDescriptor {
                name: "bytes_field",
                full_name: "all_types.AllTypes.bytes_field",
                json_name: "bytesField",
                number: 15,
                kind: bytes,
                cardinality: Optional,
                containing_oneof: None,
                default_value: Bytes(
                    b"",
                ),
                is_group: false,
                is_list: false,
                is_map: false,
                is_packed: false,
                supports_presence: false,
            }

            value: Bytes(b"\xbe\xef")

            ==============================
            field: FieldDescriptor {
                name: "nested_message_field",
                full_name: "all_types.AllTypes.nested_message_field",
                json_name: "nestedMessageField",
                number: 17,
                kind: all_types.AllTypes.NestedMessage,
                cardinality: Optional,
                containing_oneof: None,
                default_value: Message(
                    DynamicMessage {
                        desc: MessageDescriptor {
                            name: "NestedMessage",
                            full_name: "all_types.AllTypes.NestedMessage",
                            is_map_entry: false,
                            fields: [
                                FieldDescriptor {
                                    name: "id",
                                    full_name: "all_types.AllTypes.NestedMessage.id",
                                    json_name: "id",
                                    number: 1,
                                    kind: int32,
                                    cardinality: Optional,
                                    containing_oneof: None,
                                    default_value: I32(
                                        0,
                                    ),
                                    is_group: false,
                                    is_list: false,
                                    is_map: false,
                                    is_packed: false,
                                    supports_presence: false,
                                },
                                FieldDescriptor {
                                    name: "name",
                                    full_name: "all_types.AllTypes.NestedMessage.name",
                                    json_name: "name",
                                    number: 2,
                                    kind: string,
                                    cardinality: Optional,
                                    containing_oneof: None,
                                    default_value: String(
                                        "",
                                    ),
                                    is_group: false,
                                    is_list: false,
                                    is_map: false,
                                    is_packed: false,
                                    supports_presence: false,
                                },
                            ],
                            oneofs: [],
                        },
                        fields: DynamicMessageFieldSet {
                            fields: {},
                        },
                    },
                ),
                is_group: false,
                is_list: false,
                is_map: false,
                is_packed: false,
                supports_presence: true,
            }

            value: Message(DynamicMessage { desc: MessageDescriptor { name: "NestedMessage", full_name: "all_types.AllTypes.NestedMessage", is_map_entry: false, fields: [FieldDescriptor { name: "id", full_name: "all_types.AllTypes.NestedMessage.id", json_name: "id", number: 1, kind: int32, cardinality: Optional, containing_oneof: None, default_value: I32(0), is_group: false, is_list: false, is_map: false, is_packed: false, supports_presence: false }, FieldDescriptor { name: "name", full_name: "all_types.AllTypes.NestedMessage.name", json_name: "name", number: 2, kind: string, cardinality: Optional, containing_oneof: None, default_value: String(""), is_group: false, is_list: false, is_map: false, is_packed: false, supports_presence: false }], oneofs: [] }, fields: DynamicMessageFieldSet { fields: {1: Value(I32(1)), 2: Value(String(""))} } })

            ==============================
            field: FieldDescriptor {
                name: "repeated_int_field",
                full_name: "all_types.AllTypes.repeated_int_field",
                json_name: "repeatedIntField",
                number: 18,
                kind: int32,
                cardinality: Repeated,
                containing_oneof: None,
                default_value: List(
                    [],
                ),
                is_group: false,
                is_list: true,
                is_map: false,
                is_packed: true,
                supports_presence: false,
            }

            value: List([I32(4), I32(0), I32(4)])

            ==============================
            field: FieldDescriptor {
                name: "map_field",
                full_name: "all_types.AllTypes.map_field",
                json_name: "mapField",
                number: 22,
                kind: all_types.AllTypes.MapFieldEntry,
                cardinality: Repeated,
                containing_oneof: None,
                default_value: Map(
                    {},
                ),
                is_group: false,
                is_list: false,
                is_map: true,
                is_packed: false,
                supports_presence: false,
            }

            value: Map({
                String("a"): I32(1),
                String("b"): I32(2),
            })

            ==============================
            field: FieldDescriptor {
                name: "timestamp_field",
                full_name: "all_types.AllTypes.timestamp_field",
                json_name: "timestampField",
                number: 23,
                kind: google.protobuf.Timestamp,
                cardinality: Optional,
                containing_oneof: None,
                default_value: Message(
                    DynamicMessage {
                        desc: MessageDescriptor {
                            name: "Timestamp",
                            full_name: "google.protobuf.Timestamp",
                            is_map_entry: false,
                            fields: [
                                FieldDescriptor {
                                    name: "seconds",
                                    full_name: "google.protobuf.Timestamp.seconds",
                                    json_name: "seconds",
                                    number: 1,
                                    kind: int64,
                                    cardinality: Optional,
                                    containing_oneof: None,
                                    default_value: I64(
                                        0,
                                    ),
                                    is_group: false,
                                    is_list: false,
                                    is_map: false,
                                    is_packed: false,
                                    supports_presence: false,
                                },
                                FieldDescriptor {
                                    name: "nanos",
                                    full_name: "google.protobuf.Timestamp.nanos",
                                    json_name: "nanos",
                                    number: 2,
                                    kind: int32,
                                    cardinality: Optional,
                                    containing_oneof: None,
                                    default_value: I32(
                                        0,
                                    ),
                                    is_group: false,
                                    is_list: false,
                                    is_map: false,
                                    is_packed: false,
                                    supports_presence: false,
                                },
                            ],
                            oneofs: [],
                        },
                        fields: DynamicMessageFieldSet {
                            fields: {},
                        },
                    },
                ),
                is_group: false,
                is_list: false,
                is_map: false,
                is_packed: false,
                supports_presence: true,
            }

            value: Message(DynamicMessage { desc: MessageDescriptor { name: "Timestamp", full_name: "google.protobuf.Timestamp", is_map_entry: false, fields: [FieldDescriptor { name: "seconds", full_name: "google.protobuf.Timestamp.seconds", json_name: "seconds", number: 1, kind: int64, cardinality: Optional, containing_oneof: None, default_value: I64(0), is_group: false, is_list: false, is_map: false, is_packed: false, supports_presence: false }, FieldDescriptor { name: "nanos", full_name: "google.protobuf.Timestamp.nanos", json_name: "nanos", number: 2, kind: int32, cardinality: Optional, containing_oneof: None, default_value: I32(0), is_group: false, is_list: false, is_map: false, is_packed: false, supports_presence: false }], oneofs: [] }, fields: DynamicMessageFieldSet { fields: {2: Value(I32(3000))} } })

            ==============================
            field: FieldDescriptor {
                name: "map_struct_field",
                full_name: "all_types.AllTypes.map_struct_field",
                json_name: "mapStructField",
                number: 29,
                kind: all_types.AllTypes.MapStructFieldEntry,
                cardinality: Repeated,
                containing_oneof: None,
                default_value: Map(
                    {},
                ),
                is_group: false,
                is_list: false,
                is_map: true,
                is_packed: false,
                supports_presence: false,
            }

            value: Map({
                String("a"): Message(DynamicMessage { desc: MessageDescriptor { name: "NestedMessage", full_name: "all_types.AllTypes.NestedMessage", is_map_entry: false, fields: [FieldDescriptor { name: "id", full_name: "all_types.AllTypes.NestedMessage.id", json_name: "id", number: 1, kind: int32, cardinality: Optional, containing_oneof: None, default_value: I32(0), is_group: false, is_list: false, is_map: false, is_packed: false, supports_presence: false }, FieldDescriptor { name: "name", full_name: "all_types.AllTypes.NestedMessage.name", json_name: "name", number: 2, kind: string, cardinality: Optional, containing_oneof: None, default_value: String(""), is_group: false, is_list: false, is_map: false, is_packed: false, supports_presence: false }], oneofs: [] }, fields: DynamicMessageFieldSet { fields: {1: Value(I32(1)), 2: Value(String("x"))} } }),
                String("b"): Message(DynamicMessage { desc: MessageDescriptor { name: "NestedMessage", full_name: "all_types.AllTypes.NestedMessage", is_map_entry: false, fields: [FieldDescriptor { name: "id", full_name: "all_types.AllTypes.NestedMessage.id", json_name: "id", number: 1, kind: int32, cardinality: Optional, containing_oneof: None, default_value: I32(0), is_group: false, is_list: false, is_map: false, is_packed: false, supports_presence: false }, FieldDescriptor { name: "name", full_name: "all_types.AllTypes.NestedMessage.name", json_name: "name", number: 2, kind: string, cardinality: Optional, containing_oneof: None, default_value: String(""), is_group: false, is_list: false, is_map: false, is_packed: false, supports_presence: false }], oneofs: [] }, fields: DynamicMessageFieldSet { fields: {1: Value(I32(2)), 2: Value(String("y"))} } }),
            })"#]].assert_eq(&format!("{}",
            m.message.fields().format_with("\n\n==============================\n", |(field,value),f| {
            f(&format!("field: {:#?}\n\nvalue: {}", field, print_proto(value)))
        })));
    }

    fn print_proto(value: &Value) -> String {
        match value {
            Value::Map(m) => {
                let mut res = String::new();
                res.push_str("Map({\n");
                for (k, v) in m.iter().sorted_by_key(|(k, _v)| *k) {
                    res.push_str(&format!(
                        "    {}: {},\n",
                        print_proto(&k.clone().into()),
                        print_proto(v)
                    ));
                }
                res.push_str("})");
                res
            }
            _ => format!("{:?}", value),
        }
    }

    #[test]
    fn test_encode_proto_repeated() {
        let pool_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("codec/tests/test_data/all-types.pb");
        let pool_bytes = fs_err::read(pool_path).unwrap();
        let pool = prost_reflect::DescriptorPool::decode(pool_bytes.as_ref()).unwrap();
        let message_descriptor = pool.get_message_by_name("all_types.AllTypes").unwrap();

        let schema = Schema::new(vec![Field::with_name(
            DataType::Int32.list().list(),
            "repeated_int_field",
        )]);

        let err = validate_fields(
            schema
                .fields
                .iter()
                .map(|f| (f.name.as_str(), &f.data_type)),
            &message_descriptor,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "encode 'repeated_int_field' error: cannot encode integer[] column as int32 field"
        );

        let schema = Schema::new(vec![Field::with_name(
            DataType::Int32.list(),
            "repeated_int_field",
        )]);
        let row = OwnedRow::new(vec![Some(ScalarImpl::List(ListValue::from_iter([
            Some(0),
            None,
            Some(2),
            Some(3),
        ])))]);

        let err = encode_fields(
            schema
                .fields
                .iter()
                .map(|f| (f.name.as_str(), &f.data_type))
                .zip_eq_debug(row.iter()),
            &message_descriptor,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "encode 'repeated_int_field' error: array containing null not allowed as repeated field"
        );
    }

    #[test]
    fn test_encode_proto_err() {
        let pool_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("codec/tests/test_data/all-types.pb");
        let pool_bytes = std::fs::read(pool_path).unwrap();
        let pool = prost_reflect::DescriptorPool::decode(pool_bytes.as_ref()).unwrap();
        let message_descriptor = pool.get_message_by_name("all_types.AllTypes").unwrap();

        let err = validate_fields(
            std::iter::once(("not_exists", &DataType::Int16)),
            &message_descriptor,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "encode 'not_exists' error: field not in proto"
        );

        let err = validate_fields(
            std::iter::once(("map_field", &DataType::Jsonb)),
            &message_descriptor,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "encode 'map_field' error: cannot encode jsonb column as all_types.AllTypes.MapFieldEntry field"
        );
    }
}
