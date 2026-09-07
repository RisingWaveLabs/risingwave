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

use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::schema::{Name, RecordSchema, Schema as AvroSchema};
use apache_avro::types::{Record, Value};
use risingwave_common::array::VECTOR_AS_LIST_TYPE;
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, DatumRef, ListType, ScalarRefImpl, StructType};
use risingwave_common::util::iter_util::{ZipEqDebug, ZipEqFast};
use risingwave_connector_codec::decoder::utils::rust_decimal_to_scaled_bigint;
use thiserror_ext::AsReport;

use super::{FieldEncodeError, Result as SinkResult, RowEncoder, SerTo};

type Result<T> = std::result::Result<T, FieldEncodeError>;
struct NamesRef(HashMap<Name, AvroSchema>);

pub struct AvroEncoder {
    schema: Schema,
    col_indices: Option<Vec<usize>>,
    avro_schema: Arc<AvroSchema>,
    refs: NamesRef,
    header: AvroHeader,
}

#[derive(Debug, Clone, Copy)]
pub enum AvroHeader {
    None,
    /// <https://avro.apache.org/docs/1.11.1/specification/#single-object-encoding>
    ///
    /// * C3 01
    /// * 8-byte little-endian CRC-64-AVRO fingerprint
    SingleObject,
    /// <https://avro.apache.org/docs/1.11.1/specification/#object-container-files>
    ///
    /// * 4F 62 6A 01
    /// * schema
    /// * 16-byte random sync marker
    ContainerFile,
    /// <https://docs.confluent.io/platform/7.5/schema-registry/fundamentals/serdes-develop/index.html#messages-wire-format>
    ///
    /// * 00
    /// * 4-byte big-endian schema ID
    ConfluentSchemaRegistry(i32),
    /// <https://github.com/awslabs/aws-glue-schema-registry/blob/v1.1.20/common/src/main/java/com/amazonaws/services/schemaregistry/utils/AWSSchemaRegistryConstants.java#L59-L61>
    ///
    /// * 03
    /// * 00
    /// * 16-byte UUID identifying a specific schema version
    GlueSchemaRegistry(uuid::Uuid),
}

impl AvroEncoder {
    pub fn new(
        schema: Schema,
        col_indices: Option<Vec<usize>>,
        avro_schema: Arc<AvroSchema>,
        header: AvroHeader,
    ) -> SinkResult<Self> {
        let refs = NamesRef::new(&avro_schema)?;
        match &col_indices {
            Some(col_indices) => validate_fields(
                col_indices.iter().map(|idx| {
                    let f = &schema[*idx];
                    (f.name.as_str(), &f.data_type)
                }),
                &avro_schema,
                &refs,
            )?,
            None => validate_fields(
                schema
                    .fields
                    .iter()
                    .map(|f| (f.name.as_str(), &f.data_type)),
                &avro_schema,
                &refs,
            )?,
        };

        Ok(Self {
            schema,
            col_indices,
            avro_schema,
            refs,
            header,
        })
    }
}

impl NamesRef {
    fn new(root: &AvroSchema) -> std::result::Result<Self, apache_avro::Error> {
        let resolved = apache_avro::schema::ResolvedSchema::try_from(root)?;
        let refs = resolved
            .get_names()
            .iter()
            .map(|(k, v)| (k.to_owned(), (*v).to_owned()))
            .collect();
        Ok(Self(refs))
    }

    fn lookup<'a>(&'a self, avro: &'a AvroSchema) -> &'a AvroSchema {
        match avro {
            AvroSchema::Ref { name } => &self.0[name],
            _ => avro,
        }
    }
}

pub struct AvroEncoded {
    value: Value,
    schema: Arc<AvroSchema>,
    header: AvroHeader,
}

impl RowEncoder for AvroEncoder {
    type Output = AvroEncoded;

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
        let record = encode_fields(
            col_indices.map(|idx| {
                let f = &self.schema[idx];
                ((f.name.as_str(), &f.data_type), row.datum_at(idx))
            }),
            &self.avro_schema,
            &self.refs,
        )?;
        Ok(AvroEncoded {
            value: record.into(),
            schema: self.avro_schema.clone(),
            header: self.header,
        })
    }
}

impl SerTo<Vec<u8>> for AvroEncoded {
    fn ser_to(self) -> SinkResult<Vec<u8>> {
        use bytes::BufMut as _;

        let header = match self.header {
            AvroHeader::ConfluentSchemaRegistry(schema_id) => {
                let mut buf = Vec::with_capacity(1 + 4);
                buf.put_u8(0);
                buf.put_i32(schema_id);
                buf
            }
            AvroHeader::GlueSchemaRegistry(schema_version_id) => {
                let mut buf = Vec::with_capacity(1 + 1 + 16);
                buf.put_u8(3);
                buf.put_u8(0);
                buf.put_slice(schema_version_id.as_bytes());
                buf
            }
            AvroHeader::None | AvroHeader::SingleObject | AvroHeader::ContainerFile => {
                return Err(crate::sink::SinkError::Encode(format!(
                    "{:?} unsupported yet",
                    self.header
                )));
            }
        };

        let raw = apache_avro::to_avro_datum(&self.schema, self.value)
            .map_err(|e| crate::sink::SinkError::Encode(e.to_report_string()))?;
        let mut buf = Vec::with_capacity(header.len() + raw.len());
        buf.put_slice(&header);
        buf.put_slice(&raw);

        Ok(buf)
    }
}

enum OptIdx {
    /// `T`
    NotUnion,
    /// `[T]`
    Single,
    /// `[null, T]`
    NullLeft,
    /// `[T, null]`
    NullRight,
}

/// A trait that assists code reuse between `validate` and `encode`.
/// * For `validate`, the inputs are (RisingWave type, ProtoBuf type).
/// * For `encode`, the inputs are (RisingWave type, RisingWave data, ProtoBuf type).
///
/// Thus we impl [`MaybeData`] for both `()` and [`DatumRef`].
trait MaybeData: std::fmt::Debug {
    type Out;

    fn on_base(self, f: impl FnOnce(ScalarRefImpl<'_>) -> Result<Value>) -> Result<Self::Out>;

    /// Switch to `RecordSchema` after #12562
    fn on_struct(self, st: &StructType, avro: &AvroSchema, refs: &NamesRef) -> Result<Self::Out>;

    fn on_list(self, lt: &ListType, avro: &AvroSchema, refs: &NamesRef) -> Result<Self::Out>;

    fn on_map(
        self,
        value_type: &DataType,
        avro_value_schema: &AvroSchema,
        refs: &NamesRef,
    ) -> Result<Self::Out>;

    fn handle_nullable_union(out: Self::Out, opt_idx: OptIdx) -> Result<Self::Out>;
}

impl MaybeData for () {
    type Out = ();

    fn on_base(self, _: impl FnOnce(ScalarRefImpl<'_>) -> Result<Value>) -> Result<Self::Out> {
        Ok(self)
    }

    fn on_struct(self, st: &StructType, avro: &AvroSchema, refs: &NamesRef) -> Result<Self::Out> {
        validate_fields(st.iter(), avro, refs)
    }

    fn on_list(self, lt: &ListType, avro: &AvroSchema, refs: &NamesRef) -> Result<Self::Out> {
        on_field(lt.elem(), (), avro, refs)
    }

    fn on_map(self, elem: &DataType, avro: &AvroSchema, refs: &NamesRef) -> Result<Self::Out> {
        on_field(elem, (), avro, refs)
    }

    fn handle_nullable_union(out: Self::Out, _: OptIdx) -> Result<Self::Out> {
        Ok(out)
    }
}

impl MaybeData for DatumRef<'_> {
    type Out = Value;

    fn on_base(self, f: impl FnOnce(ScalarRefImpl<'_>) -> Result<Value>) -> Result<Self::Out> {
        match self {
            Some(s) => f(s),
            None => Ok(Value::Null),
        }
    }

    fn on_struct(self, st: &StructType, avro: &AvroSchema, refs: &NamesRef) -> Result<Self::Out> {
        let d = match self {
            Some(s) => s.into_struct(),
            None => return Ok(Value::Null),
        };
        let record = encode_fields(st.iter().zip_eq_debug(d.iter_fields_ref()), avro, refs)?;
        Ok(record.into())
    }

    fn on_list(self, lt: &ListType, avro: &AvroSchema, refs: &NamesRef) -> Result<Self::Out> {
        let d = match self {
            Some(s) => s.into_list(),
            None => return Ok(Value::Null),
        };
        let vs = d
            .iter()
            .map(|d| on_field(lt.elem(), d, avro, refs))
            .try_collect()?;
        Ok(Value::Array(vs))
    }

    fn on_map(self, elem: &DataType, avro: &AvroSchema, refs: &NamesRef) -> Result<Self::Out> {
        let d = match self {
            Some(s) => s.into_map(),
            None => return Ok(Value::Null),
        };
        let vs = d
            .iter()
            .map(|(k, v)| {
                let k = k.into_utf8().to_owned();
                let v = on_field(elem, v, avro, refs)?;
                Ok((k, v))
            })
            .try_collect()?;
        Ok(Value::Map(vs))
    }

    fn handle_nullable_union(out: Self::Out, opt_idx: OptIdx) -> Result<Self::Out> {
        use OptIdx::*;

        match out == Value::Null {
            true => {
                let ni = match opt_idx {
                    NotUnion | Single => {
                        return Err(FieldEncodeError::new("found null but required"));
                    }
                    NullLeft => 0,
                    NullRight => 1,
                };
                Ok(Value::Union(ni, out.into()))
            }
            false => {
                let vi = match opt_idx {
                    NotUnion => return Ok(out),
                    NullLeft => 1,
                    Single | NullRight => 0,
                };
                Ok(Value::Union(vi, out.into()))
            }
        }
    }
}

fn validate_fields<'rw>(
    rw_fields: impl Iterator<Item = (&'rw str, &'rw DataType)>,
    avro: &AvroSchema,
    refs: &NamesRef,
) -> Result<()> {
    let avro = refs.lookup(avro);
    let AvroSchema::Record(RecordSchema { fields, lookup, .. }) = avro else {
        return Err(FieldEncodeError::new(format!(
            "expect avro record but got {}",
            avro.canonical_form(),
        )));
    };
    let mut present = vec![false; fields.len()];
    for (name, t) in rw_fields {
        let Some(&idx) = lookup.get(name) else {
            return Err(FieldEncodeError::new("field not in avro").with_name(name));
        };
        present[idx] = true;
        let avro_field = &fields[idx];
        on_field(t, (), &avro_field.schema, refs).map_err(|e| e.with_name(name))?;
    }
    for (p, avro_field) in present.into_iter().zip_eq_fast(fields) {
        if p {
            continue;
        }
        if !avro_field.is_nullable() {
            return Err(
                FieldEncodeError::new("field not present but required").with_name(&avro_field.name)
            );
        }
    }
    Ok(())
}

fn encode_fields<'avro, 'rw>(
    fields_with_datums: impl Iterator<Item = ((&'rw str, &'rw DataType), DatumRef<'rw>)>,
    schema: &'avro AvroSchema,
    refs: &'avro NamesRef,
) -> Result<Record<'avro>> {
    let schema = refs.lookup(schema);
    let mut record = Record::new(schema).unwrap();
    let AvroSchema::Record(RecordSchema { fields, lookup, .. }) = schema else {
        unreachable!()
    };
    let mut present = vec![false; fields.len()];
    for ((name, t), d) in fields_with_datums {
        let idx = lookup[name];
        present[idx] = true;
        let avro_field = &fields[idx];
        let value = on_field(t, d, &avro_field.schema, refs).map_err(|e| e.with_name(name))?;
        record.put(name, value);
    }
    // Unfortunately, the upstream `apache_avro` does not handle missing fields as nullable correctly.
    // The correct encoding is `Value::Union(null_index, Value::Null)` but it simply writes `Value::Null`.
    // See [`tests::test_encode_avro_lib_bug`].
    for (p, avro_field) in present.into_iter().zip_eq_fast(fields) {
        if p {
            continue;
        }
        let AvroSchema::Union(u) = &avro_field.schema else {
            unreachable!()
        };
        // We could have saved null index of each field during [`validate_fields`] to avoid repeated lookup.
        // But in most cases it is the 0th.
        // Alternatively, we can simplify by enforcing the best practice of `null at 0th`.
        let ni = u
            .variants()
            .iter()
            .position(|a| a == &AvroSchema::Null)
            .unwrap();
        record.put(
            &avro_field.name,
            Value::Union(ni.try_into().unwrap(), Value::Null.into()),
        );
    }
    Ok(record)
}

/// Handles both `validate` (without actual data) and `encode`.
/// See [`MaybeData`] for more info.
fn on_field<D: MaybeData>(
    data_type: &DataType,
    maybe: D,
    expected: &AvroSchema,
    refs: &NamesRef,
) -> Result<D::Out> {
    use risingwave_common::types::Interval;

    let no_match_err = || {
        Err(FieldEncodeError::new(format!(
            "cannot encode {} column as {} field",
            data_type,
            expected.canonical_form()
        )))
    };

    // For now, we only support optional single type, rather than general union.
    // For example, how do we encode int16 into avro `["int", "long"]`?
    let (inner, opt_idx) = match expected {
        AvroSchema::Union(union) => match union.variants() {
            [] => return no_match_err(),
            [one] => (one, OptIdx::Single),
            [AvroSchema::Null, r] => (r, OptIdx::NullLeft),
            [l, AvroSchema::Null] => (l, OptIdx::NullRight),
            _ => return no_match_err(),
        },
        _ => (expected, OptIdx::NotUnion),
    };

    let inner = refs.lookup(inner);

    let value = match &data_type {
        // Group A: perfect match between RisingWave types and Avro types
        DataType::Boolean => match inner {
            AvroSchema::Boolean => maybe.on_base(|s| Ok(Value::Boolean(s.into_bool())))?,
            _ => return no_match_err(),
        },
        DataType::Varchar => match inner {
            AvroSchema::String => maybe.on_base(|s| Ok(Value::String(s.into_utf8().into())))?,

            // Add enum support
            AvroSchema::Enum(enum_schema) => maybe.on_base(|s| {
                let str_value = s.into_utf8();

                if let Some(position) = enum_schema
                    .symbols
                    .iter()
                    .position(|symbol| symbol == str_value)
                {
                    Ok(Value::Enum(position as u32, str_value.to_owned()))
                } else {
                    Err(FieldEncodeError::new(format!(
                        "Value '{}' is not a valid enum symbol. Valid symbols are: {:?}",
                        str_value, enum_schema.symbols
                    )))
                }
            })?,

            _ => return no_match_err(),
        },
        DataType::Bytea => match inner {
            AvroSchema::Bytes => maybe.on_base(|s| Ok(Value::Bytes(s.into_bytea().into())))?,
            _ => return no_match_err(),
        },
        DataType::Float32 => match inner {
            AvroSchema::Float => maybe.on_base(|s| Ok(Value::Float(s.into_float32().into())))?,
            _ => return no_match_err(),
        },
        DataType::Float64 => match inner {
            AvroSchema::Double => maybe.on_base(|s| Ok(Value::Double(s.into_float64().into())))?,
            _ => return no_match_err(),
        },
        DataType::Int32 => match inner {
            AvroSchema::Int => maybe.on_base(|s| Ok(Value::Int(s.into_int32())))?,
            _ => return no_match_err(),
        },
        DataType::Int64 => match inner {
            AvroSchema::Long => maybe.on_base(|s| Ok(Value::Long(s.into_int64())))?,
            _ => return no_match_err(),
        },
        DataType::Serial => match inner {
            AvroSchema::Long => maybe.on_base(|s| Ok(Value::Long(s.into_serial().into_inner())))?,
            _ => return no_match_err(),
        },
        DataType::Struct(st) => match inner {
            AvroSchema::Record { .. } => maybe.on_struct(st, inner, refs)?,
            _ => return no_match_err(),
        },
        DataType::List(lt) => match inner {
            AvroSchema::Array(avro_arr) => maybe.on_list(lt, &avro_arr.items, refs)?,
            _ => return no_match_err(),
        },
        DataType::Map(m) => {
            if *m.key() != DataType::Varchar {
                return no_match_err();
            }
            match inner {
                AvroSchema::Map(avro_map) => maybe.on_map(m.value(), &avro_map.types, refs)?,
                _ => return no_match_err(),
            }
        }

        // Group B: match between RisingWave types and Avro logical types
        DataType::Timestamptz => match inner {
            AvroSchema::TimestampMicros => maybe.on_base(|s| {
                Ok(Value::TimestampMicros(
                    s.into_timestamptz().timestamp_micros(),
                ))
            })?,
            AvroSchema::TimestampMillis => maybe.on_base(|s| {
                Ok(Value::TimestampMillis(
                    s.into_timestamptz().timestamp_millis(),
                ))
            })?,
            _ => return no_match_err(),
        },
        DataType::Timestamp => return no_match_err(),
        DataType::Date => match inner {
            AvroSchema::Date => {
                maybe.on_base(|s| Ok(Value::Date(s.into_date().get_nums_days_unix_epoch())))?
            }
            _ => return no_match_err(),
        },
        DataType::Time => match inner {
            AvroSchema::TimeMicros => {
                maybe.on_base(|s| Ok(Value::TimeMicros(Interval::from(s.into_time()).usecs())))?
            }
            AvroSchema::TimeMillis => maybe.on_base(|s| {
                Ok(Value::TimeMillis(
                    (Interval::from(s.into_time()).usecs() / 1000)
                        .try_into()
                        .unwrap(),
                ))
            })?,
            _ => return no_match_err(),
        },
        DataType::Interval => match inner {
            AvroSchema::Duration => maybe.on_base(|s| {
                use apache_avro::{Days, Duration, Millis, Months};
                let iv = s.into_interval();

                let overflow = |_| FieldEncodeError::new(format!("{iv} overflows avro duration"));

                Ok(Value::Duration(Duration::new(
                    Months::new(iv.months().try_into().map_err(overflow)?),
                    Days::new(iv.days().try_into().map_err(overflow)?),
                    Millis::new((iv.usecs() / 1000).try_into().map_err(overflow)?),
                )))
            })?,
            _ => return no_match_err(),
        },
        // Group C: experimental
        DataType::Int16 => match inner {
            AvroSchema::Int => maybe.on_base(|s| Ok(Value::Int(s.into_int16() as i32)))?,
            _ => return no_match_err(),
        },
        DataType::Decimal => match inner {
            AvroSchema::Decimal(decimal_schema) => {
                maybe.on_base(|s| {
                    match s.into_decimal() {
                        risingwave_common::types::Decimal::Normalized(decimal) => {
                            // convert to bigint with scale
                            // rescale the rust_decimal to the scale of the avro decimal
                            //
                            // From bigdecimal::BigDecimal::with_scale:
                            // If the new_scale is lower than the current value (indicating a larger
                            // power of 10), digits will be dropped (as precision is lower)
                            let signed_bigint_bytes =
                                rust_decimal_to_scaled_bigint(decimal, decimal_schema.scale)
                                    .map_err(FieldEncodeError::new)?;
                            Ok(Value::Decimal(apache_avro::Decimal::from(
                                &signed_bigint_bytes,
                            )))
                        }
                        d @ risingwave_common::types::Decimal::NaN
                        | d @ risingwave_common::types::Decimal::NegativeInf
                        | d @ risingwave_common::types::Decimal::PositiveInf => {
                            Err(FieldEncodeError::new(format!(
                                "Avro Decimal does not support NaN or Inf, but got {}",
                                d
                            )))
                        }
                    }
                })?
            }
            _ => return no_match_err(),
        },
        DataType::Jsonb => match inner {
            AvroSchema::String => {
                maybe.on_base(|s| Ok(Value::String(s.into_jsonb().to_string())))?
            }
            _ => return no_match_err(),
        },
        DataType::Variant => {
            return no_match_err();
        }
        DataType::Vector(_) => match inner {
            AvroSchema::Array(avro_arr) => {
                maybe.on_list(&VECTOR_AS_LIST_TYPE, &avro_arr.items, refs)?
            }
            _ => return no_match_err(),
        },
        // Group D: unsupported
        DataType::Int256 => {
            return no_match_err();
        }
    };

    D::handle_nullable_union(value, opt_idx)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::str::FromStr;

    use expect_test::expect;
    use itertools::Itertools;
    use risingwave_common::array::{ArrayBuilder, MapArrayBuilder};
    use risingwave_common::catalog::Field;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{
        Date, Datum, Interval, JsonbVal, ListValue, MapType, MapValue, Scalar, ScalarImpl,
        StructValue, Time, Timestamptz, ToDatumRef,
    };

    use super::*;

    #[track_caller]
    fn test_ok(rw_type: &DataType, rw_datum: Datum, avro_type: &str, expected: Value) {
        let avro_schema = AvroSchema::parse_str(avro_type).unwrap();
        let refs = NamesRef::new(&avro_schema).unwrap();
        let actual = on_field(rw_type, rw_datum.to_datum_ref(), &avro_schema, &refs).unwrap();
        assert_eq!(actual, expected);
    }

    #[track_caller]
    fn test_err<D: MaybeData>(t: &DataType, d: D, avro: &str, expected: &str)
    where
        D::Out: std::fmt::Debug,
    {
        let avro_schema = AvroSchema::parse_str(avro).unwrap();
        let refs = NamesRef::new(&avro_schema).unwrap();
        let err = on_field(t, d, &avro_schema, &refs).unwrap_err();
        assert_eq!(err.to_string(), expected);
    }

    #[track_caller]
    fn test_v2(rw_type: &str, rw_scalar: &str, avro_type: &str, expected: expect_test::Expect) {
        let avro_schema = AvroSchema::parse_str(avro_type).unwrap();
        let refs = NamesRef::new(&avro_schema).unwrap();
        let rw_type = DataType::from_str(rw_type).unwrap();
        let rw_datum = ScalarImpl::from_text_for_test(rw_scalar, &rw_type).unwrap();

        if let Err(validate_err) = on_field(&rw_type, (), &avro_schema, &refs) {
            expected.assert_debug_eq(&validate_err);
            return;
        }
        let actual = on_field(&rw_type, Some(rw_datum).to_datum_ref(), &avro_schema, &refs);
        match actual {
            Ok(v) => expected.assert_eq(&print_avro_value(&v)),
            Err(e) => expected.assert_debug_eq(&e),
        }
    }

    fn print_avro_value(v: &Value) -> String {
        match v {
            Value::Map(m) => {
                let mut res = "Map({".to_owned();
                for (k, v) in m.iter().sorted_by_key(|x| x.0) {
                    res.push_str(&format!("{}: {}, ", k, print_avro_value(v)));
                }
                res.push_str("})");
                res
            }
            _ => format!("{v:?}"),
        }
    }

    #[test]
    fn test_encode_v2() {
        test_v2(
            "boolean",
            "false",
            r#""int""#,
            expect![[r#"
                FieldEncodeError {
                    message: "cannot encode boolean column as \"int\" field",
                    rev_path: [],
                }
            "#]],
        );
        test_v2("boolean", "true", r#""boolean""#, expect!["Boolean(true)"]);

        test_v2(
            "map(varchar,varchar)",
            "{1:1,2:2,3:3}",
            r#"{"type": "map","values": "string"}"#,
            expect![[r#"Map({1: String("1"), 2: String("2"), 3: String("3"), })"#]],
        );

        test_v2(
            "map(varchar,varchar)",
            "{1:1,2:NULL,3:3}",
            r#"{"type": "map","values": "string"}"#,
            expect![[r#"
                FieldEncodeError {
                    message: "found null but required",
                    rev_path: [],
                }
            "#]],
        );

        test_v2(
            "map(varchar,varchar)",
            "{1:1,2:NULL,3:3}",
            r#"{"type": "map","values": ["null", "string"]}"#,
            expect![[
                r#"Map({1: Union(1, String("1")), 2: Union(0, Null), 3: Union(1, String("3")), })"#
            ]],
        );

        test_v2(
            "map(int,varchar)",
            "{1:1,2:NULL,3:3}",
            r#"{"type": "map","values": ["null", "string"]}"#,
            expect![[r#"
                FieldEncodeError {
                    message: "cannot encode map(integer,character varying) column as {\"type\":\"map\",\"values\":[\"null\",\"string\"]} field",
                    rev_path: [],
                }
            "#]],
        );
    }

    #[test]
    fn test_encode_avro_ok() {
        test_ok(
            &DataType::Boolean,
            Some(ScalarImpl::Bool(false)),
            r#""boolean""#,
            Value::Boolean(false),
        );

        test_ok(
            &DataType::Varchar,
            Some(ScalarImpl::Utf8("RisingWave".into())),
            r#""string""#,
            Value::String("RisingWave".into()),
        );

        test_ok(
            &DataType::Bytea,
            Some(ScalarImpl::Bytea([0xbe, 0xef].into())),
            r#""bytes""#,
            Value::Bytes([0xbe, 0xef].into()),
        );

        test_ok(
            &DataType::Float32,
            Some(ScalarImpl::Float32(3.5f32.into())),
            r#""float""#,
            Value::Float(3.5f32),
        );

        test_ok(
            &DataType::Float64,
            Some(ScalarImpl::Float64(4.25f64.into())),
            r#""double""#,
            Value::Double(4.25f64),
        );

        test_ok(
            &DataType::Int32,
            Some(ScalarImpl::Int32(16)),
            r#""int""#,
            Value::Int(16),
        );

        test_ok(
            &DataType::Int64,
            Some(ScalarImpl::Int64(i64::MAX)),
            r#""long""#,
            Value::Long(i64::MAX),
        );

        test_ok(
            &DataType::Serial,
            Some(ScalarImpl::Serial(i64::MAX.into())),
            r#""long""#,
            Value::Long(i64::MAX),
        );

        let tstz = "2018-01-26T18:30:09.453Z".parse().unwrap();
        test_ok(
            &DataType::Timestamptz,
            Some(ScalarImpl::Timestamptz(tstz)),
            r#"{"type": "long", "logicalType": "timestamp-micros"}"#,
            Value::TimestampMicros(tstz.timestamp_micros()),
        );
        test_ok(
            &DataType::Timestamptz,
            Some(ScalarImpl::Timestamptz(tstz)),
            r#"{"type": "long", "logicalType": "timestamp-millis"}"#,
            Value::TimestampMillis(tstz.timestamp_millis()),
        );

        test_ok(
            &DataType::Date,
            Some(ScalarImpl::Date(Date::from_ymd_uncheck(1970, 1, 2))),
            r#"{"type": "int", "logicalType": "date"}"#,
            Value::Date(1),
        );

        let tm = Time::from_num_seconds_from_midnight_uncheck(1000, 0);
        test_ok(
            &DataType::Time,
            Some(ScalarImpl::Time(tm)),
            r#"{"type": "long", "logicalType": "time-micros"}"#,
            Value::TimeMicros(1000 * 1_000_000),
        );
        test_ok(
            &DataType::Time,
            Some(ScalarImpl::Time(tm)),
            r#"{"type": "int", "logicalType": "time-millis"}"#,
            Value::TimeMillis(1000 * 1000),
        );

        test_ok(
            &DataType::Int16,
            Some(ScalarImpl::Int16(i16::MAX)),
            r#""int""#,
            Value::Int(i16::MAX as i32),
        );

        test_ok(
            &DataType::Int16,
            Some(ScalarImpl::Int16(i16::MIN)),
            r#""int""#,
            Value::Int(i16::MIN as i32),
        );

        test_ok(
            &DataType::Jsonb,
            Some(ScalarImpl::Jsonb(
                JsonbVal::from_str(r#"{"a": 1}"#).unwrap(),
            )),
            r#""string""#,
            Value::String(r#"{"a": 1}"#.into()),
        );

        test_ok(
            &DataType::Interval,
            Some(ScalarImpl::Interval(Interval::from_month_day_usec(
                13, 2, 1000000,
            ))),
            r#"{"type": "fixed", "name": "Duration", "size": 12, "logicalType": "duration"}"#,
            Value::Duration(apache_avro::Duration::new(
                apache_avro::Months::new(13),
                apache_avro::Days::new(2),
                apache_avro::Millis::new(1000),
            )),
        );

        let mut inner_map_array_builder = MapArrayBuilder::with_type(
            2,
            DataType::Map(MapType::from_kv(DataType::Varchar, DataType::Int32)),
        );
        inner_map_array_builder.append(Some(
            MapValue::try_from_kv(
                ListValue::from_iter(["a", "b"]),
                ListValue::from_iter([1, 2]),
            )
            .unwrap()
            .as_scalar_ref(),
        ));
        inner_map_array_builder.append(Some(
            MapValue::try_from_kv(
                ListValue::from_iter(["c", "d"]),
                ListValue::from_iter([3, 4]),
            )
            .unwrap()
            .as_scalar_ref(),
        ));
        let inner_map_array = inner_map_array_builder.finish();
        test_ok(
            &DataType::Map(MapType::from_kv(
                DataType::Varchar,
                DataType::Map(MapType::from_kv(DataType::Varchar, DataType::Int32)),
            )),
            Some(ScalarImpl::Map(
                MapValue::try_from_kv(
                    ListValue::from_iter(["k1", "k2"]),
                    ListValue::new(inner_map_array.into()),
                )
                .unwrap(),
            )),
            r#"{"type": "map","values": {"type": "map","values": "int"}}"#,
            Value::Map(HashMap::from_iter([
                (
                    "k1".into(),
                    Value::Map(HashMap::from_iter([
                        ("a".into(), Value::Int(1)),
                        ("b".into(), Value::Int(2)),
                    ])),
                ),
                (
                    "k2".into(),
                    Value::Map(HashMap::from_iter([
                        ("c".into(), Value::Int(3)),
                        ("d".into(), Value::Int(4)),
                    ])),
                ),
            ])),
        );

        test_ok(
            &DataType::Struct(StructType::new(vec![
                (
                    "p",
                    DataType::Struct(StructType::new(vec![
                        ("x", DataType::Int32),
                        ("y", DataType::Int32),
                    ])),
                ),
                (
                    "q",
                    DataType::Struct(StructType::new(vec![
                        ("x", DataType::Int32),
                        ("y", DataType::Int32),
                    ])),
                ),
            ])),
            Some(ScalarImpl::Struct(StructValue::new(vec![
                Some(ScalarImpl::Struct(StructValue::new(vec![
                    Some(ScalarImpl::Int32(-2)),
                    Some(ScalarImpl::Int32(-1)),
                ]))),
                Some(ScalarImpl::Struct(StructValue::new(vec![
                    Some(ScalarImpl::Int32(2)),
                    Some(ScalarImpl::Int32(1)),
                ]))),
            ]))),
            r#"{
                "type": "record",
                "name": "Segment",
                "fields": [
                    {
                        "name": "p",
                        "type": {
                            "type": "record",
                            "name": "Point",
                            "fields": [
                                {
                                    "name": "x",
                                    "type": "int"
                                },
                                {
                                    "name": "y",
                                    "type": "int"
                                }
                            ]
                        }
                    },
                    {
                        "name": "q",
                        "type": "Point"
                    }
                ]
            }"#,
            Value::Record(vec![
                (
                    "p".to_owned(),
                    Value::Record(vec![
                        ("x".to_owned(), Value::Int(-2)),
                        ("y".to_owned(), Value::Int(-1)),
                    ]),
                ),
                (
                    "q".to_owned(),
                    Value::Record(vec![
                        ("x".to_owned(), Value::Int(2)),
                        ("y".to_owned(), Value::Int(1)),
                    ]),
                ),
            ]),
        );

        // NEW: Varchar to Enum tests
        test_ok(
            &DataType::Varchar,
            Some(ScalarImpl::Utf8("RED".into())),
            r#"{"type": "enum", "name": "Color", "symbols": ["RED", "GREEN", "BLUE"]}"#,
            Value::Enum(0, "RED".to_owned()),
        );

        test_ok(
            &DataType::Varchar,
            Some(ScalarImpl::Utf8("BLUE".into())),
            r#"{"type": "enum", "name": "Color", "symbols": ["RED", "GREEN", "BLUE"]}"#,
            Value::Enum(2, "BLUE".to_owned()),
        );

        test_ok(
            &DataType::Varchar,
            Some(ScalarImpl::Utf8("ACTIVE".into())),
            r#"{"type": "enum", "name": "Status", "symbols": ["ACTIVE", "INACTIVE"]}"#,
            Value::Enum(0, "ACTIVE".to_owned()),
        );

        // Test complex JSON with nested structures - using serde_json::Value comparison
        let complex_json = r#"{
            "person": {
                "name": "John Doe",
                "age": 30,
                "address": {
                    "street": "123 Main St.",
                    "city": "New York",
                    "coordinates": [40.7128, -74.0060]
                },
                "contacts": [
                    {"type": "email", "value": "john@example.com"},
                    {"type": "phone", "value": "+1-555-123-4567"}
                ],
                "active": true,
                "preferences": {
                    "notifications": true,
                    "theme": "dark",
                    "languages": ["en", "es"],
                    "lastLogin": null
                },
                "tags": ["premium", "verified"],
                "unicode_test": "Hello, 世界! 🌍"
            }
        }"#;

        let input_json = JsonbVal::from_str(complex_json).unwrap();
        let result = on_field(
            &DataType::Jsonb,
            Some(ScalarImpl::Jsonb(input_json)).to_datum_ref(),
            &AvroSchema::parse_str(r#""string""#).unwrap(),
            &NamesRef::new(&AvroSchema::parse_str(r#""string""#).unwrap()).unwrap(),
        )
        .unwrap();

        // Compare as parsed JSON values to handle key order randomness
        if let Value::String(result_str) = result {
            let expected_json: serde_json::Value = serde_json::from_str(complex_json).unwrap();
            let actual_json: serde_json::Value = serde_json::from_str(&result_str).unwrap();
            assert_eq!(
                expected_json, actual_json,
                "JSON values should be equivalent regardless of key order"
            );
        } else {
            panic!("Expected String value");
        };
    }

    #[test]
    fn test_encode_avro_err() {
        test_err(
            &DataType::Interval,
            Some(ScalarRefImpl::Interval(Interval::from_month_day_usec(
                -1,
                -1,
                i64::MAX,
            ))),
            r#"{"type": "fixed", "name": "Duration", "size": 12, "logicalType": "duration"}"#,
            "encode '' error: -1 mons -1 days +2562047788:00:54.775807 overflows avro duration",
        );

        let avro_schema = AvroSchema::parse_str(
            r#"{"type": "record", "name": "Root", "fields": [
                {"name": "f0", "type": "int"}
            ]}"#,
        )
        .unwrap();
        let mut record = Record::new(&avro_schema).unwrap();
        record.put("f0", Value::String("2".into()));
        let res: SinkResult<Vec<u8>> = AvroEncoded {
            value: Value::from(record),
            schema: Arc::new(avro_schema),
            header: AvroHeader::ConfluentSchemaRegistry(42),
        }
        .ser_to();
        assert_eq!(
            res.unwrap_err().to_string(),
            "Encode error: Value does not match schema"
        );
    }

    #[test]
    fn test_encode_avro_record() {
        let avro_schema = AvroSchema::parse_str(
            r#"{
                "type": "record",
                "name": "Root",
                "fields": [
                    {"name": "req", "type": "int"},
                    {"name": "opt", "type": ["null", "long"]}
                ]
            }"#,
        )
        .unwrap();
        let avro_schema = Arc::new(avro_schema);
        let header = AvroHeader::None;

        let schema = Schema::new(vec![
            Field::with_name(DataType::Int64, "opt"),
            Field::with_name(DataType::Int32, "req"),
        ]);
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Int64(31)),
            Some(ScalarImpl::Int32(15)),
        ]);
        let encoder = AvroEncoder::new(schema, None, avro_schema.clone(), header).unwrap();
        let actual = encoder.encode(row).unwrap();
        assert_eq!(
            actual.value,
            Value::Record(vec![
                ("req".into(), Value::Int(15)),
                ("opt".into(), Value::Union(1, Value::Long(31).into())),
            ])
        );

        let schema = Schema::new(vec![Field::with_name(DataType::Int32, "req")]);
        let row = OwnedRow::new(vec![Some(ScalarImpl::Int32(15))]);
        let encoder = AvroEncoder::new(schema, None, avro_schema.clone(), header).unwrap();
        let actual = encoder.encode(row).unwrap();
        assert_eq!(
            actual.value,
            Value::Record(vec![
                ("req".into(), Value::Int(15)),
                ("opt".into(), Value::Union(0, Value::Null.into())),
            ])
        );

        let schema = Schema::new(vec![Field::with_name(DataType::Int64, "opt")]);
        let Err(err) = AvroEncoder::new(schema, None, avro_schema.clone(), header) else {
            panic!()
        };
        assert_eq!(
            err.to_string(),
            "Encode error: encode 'req' error: field not present but required"
        );

        let schema = Schema::new(vec![
            Field::with_name(DataType::Int64, "opt"),
            Field::with_name(DataType::Int32, "req"),
            Field::with_name(DataType::Varchar, "extra"),
        ]);
        let Err(err) = AvroEncoder::new(schema, None, avro_schema, header) else {
            panic!()
        };
        assert_eq!(
            err.to_string(),
            "Encode error: encode 'extra' error: field not in avro"
        );

        let avro_schema = AvroSchema::parse_str(r#"["null", "long"]"#).unwrap();
        let schema = Schema::new(vec![Field::with_name(DataType::Int64, "opt")]);
        let Err(err) = AvroEncoder::new(schema, None, avro_schema.into(), header) else {
            panic!()
        };
        assert_eq!(
            err.to_string(),
            r#"Encode error: encode '' error: expect avro record but got ["null","long"]"#
        );

        test_err(
            &DataType::Struct(StructType::new(vec![("f0", DataType::Boolean)])),
            (),
            r#"{"type": "record", "name": "T", "fields": [{"name": "f0", "type": "int"}]}"#,
            "encode 'f0' error: cannot encode boolean column as \"int\" field",
        );
    }

    #[test]
    fn test_encode_avro_array() {
        let avro_schema = r#"{
            "type": "array",
            "items": "int"
        }"#;

        test_ok(
            &DataType::Int32.list(),
            Some(ScalarImpl::List(ListValue::from_iter([4, 5]))),
            avro_schema,
            Value::Array(vec![Value::Int(4), Value::Int(5)]),
        );

        test_err(
            &DataType::Int32.list(),
            Some(ScalarImpl::List(ListValue::from_iter([Some(4), None]))).to_datum_ref(),
            avro_schema,
            "encode '' error: found null but required",
        );

        test_ok(
            &DataType::Int32.list(),
            Some(ScalarImpl::List(ListValue::from_iter([Some(4), None]))),
            r#"{
                "type": "array",
                "items": ["null", "int"]
            }"#,
            Value::Array(vec![
                Value::Union(1, Value::Int(4).into()),
                Value::Union(0, Value::Null.into()),
            ]),
        );

        test_ok(
            &DataType::Int32.list().list(),
            Some(ScalarImpl::List(ListValue::from_iter([
                ListValue::from_iter([26, 29]),
                ListValue::from_iter([46, 49]),
            ]))),
            r#"{
                "type": "array",
                "items": {
                    "type": "array",
                    "items": "int"
                }
            }"#,
            Value::Array(vec![
                Value::Array(vec![Value::Int(26), Value::Int(29)]),
                Value::Array(vec![Value::Int(46), Value::Int(49)]),
            ]),
        );

        test_err(
            &DataType::Boolean.list(),
            (),
            r#"{"type": "array", "items": "int"}"#,
            "encode '' error: cannot encode boolean column as \"int\" field",
        );
    }

    #[test]
    fn test_encode_avro_union() {
        let t = &DataType::Timestamptz;
        let datum = Some(ScalarImpl::Timestamptz(
            Timestamptz::from_micros(1500).unwrap(),
        ));
        let opt_micros = r#"["null", {"type": "long", "logicalType": "timestamp-micros"}]"#;
        let opt_millis = r#"["null", {"type": "long", "logicalType": "timestamp-millis"}]"#;
        let both = r#"[{"type": "long", "logicalType": "timestamp-millis"}, {"type": "long", "logicalType": "timestamp-micros"}]"#;
        let empty = "[]";
        let one = r#"[{"type": "long", "logicalType": "timestamp-millis"}]"#;
        let right = r#"[{"type": "long", "logicalType": "timestamp-millis"}, "null"]"#;

        test_ok(
            t,
            datum.clone(),
            opt_micros,
            Value::Union(1, Value::TimestampMicros(1500).into()),
        );
        test_ok(t, None, opt_micros, Value::Union(0, Value::Null.into()));
        test_ok(
            t,
            datum.clone(),
            opt_millis,
            Value::Union(1, Value::TimestampMillis(1).into()),
        );
        test_ok(t, None, opt_millis, Value::Union(0, Value::Null.into()));

        test_err(
            t,
            datum.to_datum_ref(),
            both,
            r#"encode '' error: cannot encode timestamp with time zone column as [{"type":"long"},{"type":"long"}] field"#,
        );

        test_err(
            t,
            datum.to_datum_ref(),
            empty,
            "encode '' error: cannot encode timestamp with time zone column as [] field",
        );

        test_ok(
            t,
            datum.clone(),
            one,
            Value::Union(0, Value::TimestampMillis(1).into()),
        );
        test_err(t, None, one, "encode '' error: found null but required");

        test_ok(
            t,
            datum,
            right,
            Value::Union(0, Value::TimestampMillis(1).into()),
        );
        test_ok(t, None, right, Value::Union(1, Value::Null.into()));
    }

    /// This just demonstrates bugs of the upstream [`apache_avro`], rather than our encoder.
    /// The encoder is not using these buggy calls and is already tested above.
    #[test]
    fn test_encode_avro_lib_bug() {
        use apache_avro::{Reader, Writer};

        // a record with 2 optional int fields
        let avro_schema = AvroSchema::parse_str(
            r#"{
                "type": "record",
                "name": "Root",
                "fields": [
                    {
                        "name": "f0",
                        "type": ["null", "int"]
                    },
                    {
                        "name": "f1",
                        "type": ["null", "int"]
                    }
                ]
            }"#,
        )
        .unwrap();

        let mut writer = Writer::new(&avro_schema, Vec::new());
        let mut record = Record::new(writer.schema()).unwrap();
        // f0 omitted, f1 = Int(3)
        record.put("f1", Value::Int(3));
        writer.append(record).unwrap();
        let encoded = writer.into_inner().unwrap();
        // writing produced no error, but read fails
        let reader = Reader::new(encoded.as_slice()).unwrap();
        for value in reader {
            assert_eq!(
                value.unwrap_err().to_string(),
                "Union index 3 out of bounds: 2"
            );
        }
    }
}
