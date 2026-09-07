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

mod schema;

use apache_avro::Schema;
use apache_avro::schema::{DecimalSchema, NamesRef, UnionSchema};
use apache_avro::types::{Value, ValueKind};
use chrono::Datelike;
use itertools::Itertools;
use num_bigint::BigInt;
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::types::{
    DataType, Date, DatumCow, Interval, JsonbVal, MapValue, ScalarImpl, Time, Timestamp,
    Timestamptz, ToOwnedDatum,
};

pub use self::schema::{MapHandling, ResolvedAvroSchema, avro_schema_to_fields};
use super::utils::scaled_bigint_to_rust_decimal;
use super::{Access, AccessError, AccessResult, bail_uncategorized, uncategorized};
use crate::decoder::avro::schema::avro_schema_to_struct_field_name;

#[derive(Clone)]
/// Options for parsing an `AvroValue` into Datum, with a root avro schema.
pub struct AvroParseOptions<'a> {
    /// The avro schema at root level
    root_schema: &'a Schema,
    /// The immutable "global" context during recursive parsing
    inner: AvroParseOptionsInner<'a>,
}

#[derive(Clone)]
/// Options for parsing an `AvroValue` into Datum, with names resolved from root schema.
struct AvroParseOptionsInner<'a> {
    /// Mapping from type names to actual schema
    refs: NamesRef<'a>,
    /// Strict Mode
    /// If strict mode is disabled, an int64 can be parsed from an `AvroInt` (int32) value.
    relax_numeric: bool,
}

impl<'a> AvroParseOptions<'a> {
    pub fn create(root_schema: &'a Schema) -> Self {
        let resolved = apache_avro::schema::ResolvedSchema::try_from(root_schema)
            .expect("avro schema is self contained");
        Self {
            root_schema,
            inner: AvroParseOptionsInner {
                refs: resolved.get_names().clone(),
                relax_numeric: true,
            },
        }
    }
}

impl<'a> AvroParseOptionsInner<'a> {
    fn lookup_ref(&self, schema: &'a Schema) -> &'a Schema {
        match schema {
            Schema::Ref { name } => self.refs[name],
            _ => schema,
        }
    }

    /// Parse an avro value into expected type.
    ///
    /// 3 kinds of type info are used to parsing:
    /// - `type_expected`. The type that we expect the value is.
    /// - value type. The type info together with the value argument.
    /// - schema. The `AvroSchema` provided in option.
    ///
    /// Cases: (FIXME: Is this precise?)
    /// - If both `type_expected` and schema are provided, it will check both strictly.
    /// - If only `type_expected` is provided, it will try to match the value type
    ///   and the `type_expected`, converting the value if possible.
    /// - If only value is provided (without schema and `type_expected`),
    ///   the `DataType` will be inferred.
    fn convert_to_datum<'b>(
        &self,
        unresolved_schema: &'a Schema,
        value: &'b Value,
        type_expected: &DataType,
    ) -> AccessResult<DatumCow<'b>>
    where
        'b: 'a,
    {
        let create_error = || AccessError::TypeError {
            expected: format!("{:?}", type_expected),
            got: format!("{:?}", value),
            value: String::new(),
        };

        macro_rules! borrowed {
            ($v:expr) => {
                return Ok(DatumCow::Borrowed(Some($v.into())))
            };
        }

        let v: ScalarImpl = match (type_expected, value) {
            (_, Value::Null) => return Ok(DatumCow::NULL),
            // ---- Union (with >=2 non null variants), and nullable Union ([null, record]) -----
            (DataType::Struct(struct_type_info), Value::Union(variant, v)) => {
                let Schema::Union(u) = self.lookup_ref(unresolved_schema) else {
                    // XXX: Is this branch actually unreachable? (if self.schema is correctly used)
                    return Err(create_error());
                };

                if let Some(inner) = get_nullable_union_inner(u) {
                    // nullable Union ([null, record])
                    return self.convert_to_datum(inner, v, type_expected);
                }
                let variant_schema = &u.variants()[*variant as usize];

                if matches!(variant_schema, &Schema::Null) {
                    return Ok(DatumCow::NULL);
                }

                // Here we compare the field name, instead of using the variant idx to find the field idx.
                // The latter approach might also work, but might be more error-prone.
                // We will need to get the index of the "null" variant, and then re-map the variant index to the field index.
                // XXX: probably we can unwrap here (if self.schema is correctly used)
                let expected_field_name = avro_schema_to_struct_field_name(variant_schema)?;

                let mut fields = Vec::with_capacity(struct_type_info.len());
                for (field_name, field_type) in struct_type_info.iter() {
                    if field_name == expected_field_name {
                        let datum = self
                            .convert_to_datum(variant_schema, v, field_type)?
                            .to_owned_datum();

                        fields.push(datum)
                    } else {
                        fields.push(None)
                    }
                }
                StructValue::new(fields).into()
            }
            // nullable Union ([null, T])
            (_, Value::Union(_, v)) => {
                let Schema::Union(u) = self.lookup_ref(unresolved_schema) else {
                    return Err(create_error());
                };
                let Some(schema) = get_nullable_union_inner(u) else {
                    return Err(create_error());
                };
                return self.convert_to_datum(schema, v, type_expected);
            }
            // ---- Boolean -----
            (DataType::Boolean, Value::Boolean(b)) => (*b).into(),
            // ---- Int16 -----
            (DataType::Int16, Value::Int(i)) if self.relax_numeric => (*i as i16).into(),
            (DataType::Int16, Value::Long(i)) if self.relax_numeric => (*i as i16).into(),

            // ---- Int32 -----
            (DataType::Int32, Value::Int(i)) => (*i).into(),
            (DataType::Int32, Value::Long(i)) if self.relax_numeric => (*i as i32).into(),
            // ---- Int64 -----
            (DataType::Int64, Value::Long(i)) => (*i).into(),
            (DataType::Int64, Value::Int(i)) if self.relax_numeric => (*i as i64).into(),
            // ---- Float32 -----
            (DataType::Float32, Value::Float(i)) => (*i).into(),
            (DataType::Float32, Value::Double(i)) => (*i as f32).into(),
            // ---- Float64 -----
            (DataType::Float64, Value::Double(i)) => (*i).into(),
            (DataType::Float64, Value::Float(i)) => (*i as f64).into(),
            // ---- Decimal -----
            (DataType::Decimal, Value::Decimal(avro_decimal)) => {
                let (_precision, scale) = match self.lookup_ref(unresolved_schema) {
                    Schema::Decimal(DecimalSchema {
                        precision, scale, ..
                    }) => (*precision, *scale),
                    _ => Err(create_error())?,
                };
                let decimal = scaled_bigint_to_rust_decimal(avro_decimal.clone().into(), scale)
                    .map_err(|_| create_error())?;
                ScalarImpl::Decimal(risingwave_common::types::Decimal::Normalized(decimal))
            }
            (DataType::Decimal, Value::Record(fields)) => {
                // VariableScaleDecimal has fixed fields, scale(int) and value(bytes)
                let find_in_records = |field_name: &str| {
                    fields
                        .iter()
                        .find(|field| field.0 == field_name)
                        .map(|field| &field.1)
                        .ok_or_else(|| {
                            uncategorized!("`{field_name}` field not found in VariableScaleDecimal")
                        })
                };
                let scale = match find_in_records("scale")? {
                    Value::Int(scale) => *scale,
                    avro_value => bail_uncategorized!(
                        "scale field in VariableScaleDecimal is not int, got {:?}",
                        avro_value
                    ),
                };

                let value: BigInt = match find_in_records("value")? {
                    Value::Bytes(bytes) => BigInt::from_signed_bytes_be(bytes),
                    avro_value => bail_uncategorized!(
                        "value field in VariableScaleDecimal is not bytes, got {:?}",
                        avro_value
                    ),
                };

                let decimal = scaled_bigint_to_rust_decimal(value, scale as _)?;
                ScalarImpl::Decimal(risingwave_common::types::Decimal::Normalized(decimal))
            }
            // ---- Time -----
            (DataType::Time, Value::TimeMillis(ms)) => Time::with_milli(*ms as u32)
                .map_err(|_| create_error())?
                .into(),
            (DataType::Time, Value::TimeMicros(us)) => Time::with_micro(*us as u64)
                .map_err(|_| create_error())?
                .into(),
            // ---- Date -----
            (DataType::Date, Value::Date(days)) => {
                Date::with_days_since_ce(days + unix_epoch_days())
                    .map_err(|_| create_error())?
                    .into()
            }
            // ---- Varchar -----
            (DataType::Varchar, Value::Enum(_, symbol)) => borrowed!(symbol.as_str()),
            (DataType::Varchar, Value::String(s)) => borrowed!(s.as_str()),
            // ---- Timestamp -----
            (DataType::Timestamp, Value::LocalTimestampMillis(ms)) => Timestamp::with_millis(*ms)
                .map_err(|_| create_error())?
                .into(),
            (DataType::Timestamp, Value::LocalTimestampMicros(us)) => Timestamp::with_micros(*us)
                .map_err(|_| create_error())?
                .into(),

            // ---- TimestampTz -----
            (DataType::Timestamptz, Value::TimestampMillis(ms)) => Timestamptz::from_millis(*ms)
                .ok_or_else(|| {
                    uncategorized!("timestamptz with milliseconds {ms} * 1000 is out of range")
                })?
                .into(),
            (DataType::Timestamptz, Value::TimestampMicros(us)) => Timestamptz::from_micros(*us)
                .ok_or_else(|| {
                    uncategorized!("timestamptz with microseconds {us} is out of range")
                })?
                .into(),

            // ---- Interval -----
            (DataType::Interval, Value::Duration(duration)) => {
                let months = u32::from(duration.months()) as i32;
                let days = u32::from(duration.days()) as i32;
                let usecs = (u32::from(duration.millis()) as i64) * 1000; // never overflows
                ScalarImpl::Interval(Interval::from_month_day_usec(months, days, usecs))
            }
            // ---- Struct -----
            (DataType::Struct(struct_type_info), Value::Record(descs)) => StructValue::new({
                let Schema::Record(record_schema) = self.lookup_ref(unresolved_schema) else {
                    return Err(create_error());
                };
                struct_type_info
                    .iter()
                    .map(|(field_name, field_type)| {
                        if let Some(idx) = record_schema.lookup.get(field_name) {
                            let value = &descs[*idx].1;
                            let schema = &record_schema.fields[*idx].schema;
                            Ok(self
                                .convert_to_datum(schema, value, field_type)?
                                .to_owned_datum())
                        } else {
                            Ok(None)
                        }
                    })
                    .collect::<Result<_, AccessError>>()?
            })
            .into(),
            // ---- List -----
            (DataType::List(list_type), Value::Array(array)) => ListValue::new({
                let Schema::Array(array_schema) = self.lookup_ref(unresolved_schema) else {
                    return Err(create_error());
                };
                let schema = &array_schema.items;
                let elem_type = list_type.elem();
                let mut builder = elem_type.create_array_builder(array.len());
                for v in array {
                    let value = self.convert_to_datum(schema, v, elem_type)?;
                    builder.append(value);
                }
                builder.finish()
            })
            .into(),
            // ---- Bytea -----
            (DataType::Bytea, Value::Bytes(value)) => borrowed!(value.as_slice()),
            // ---- Jsonb -----
            (DataType::Jsonb, v @ Value::Map(_)) => {
                let mut builder = jsonbb::Builder::default();
                avro_to_jsonb(v, &mut builder)?;
                let jsonb = builder.finish();
                debug_assert!(jsonb.as_ref().is_object());
                JsonbVal::from(jsonb).into()
            }
            (DataType::Varchar, Value::Uuid(uuid)) => {
                uuid.as_hyphenated().to_string().into_boxed_str().into()
            }
            (DataType::Map(map_type), Value::Map(map)) => {
                let Schema::Map(map_schema) = self.lookup_ref(unresolved_schema) else {
                    return Err(create_error());
                };
                let schema = &map_schema.types;
                let mut builder = map_type
                    .clone()
                    .into_struct()
                    .create_array_builder(map.len());
                // Since the map is HashMap, we can ensure
                // key is non-null and unique, keys and values have the same length.

                // NOTE: HashMap's iter order is non-deterministic, but MapValue's
                // order matters. We sort by key here to have deterministic order
                // in tests. We might consider removing this, or make all MapValue sorted
                // in the future.
                for (k, v) in map.iter().sorted_by_key(|(k, _v)| *k) {
                    let value_datum = self
                        .convert_to_datum(schema, v, map_type.value())?
                        .to_owned_datum();
                    builder.append(
                        StructValue::new(vec![Some(k.as_str().into()), value_datum])
                            .to_owned_datum(),
                    );
                }
                let list = ListValue::new(builder.finish());
                MapValue::from_entries(list).into()
            }

            (_expected, _got) => Err(create_error())?,
        };
        Ok(DatumCow::Owned(Some(v)))
    }
}

pub struct AvroAccess<'a> {
    value: &'a Value,
    options: AvroParseOptions<'a>,
}

impl<'a> AvroAccess<'a> {
    pub fn new(root_value: &'a Value, options: AvroParseOptions<'a>) -> Self {
        Self {
            value: root_value,
            options,
        }
    }
}

impl Access for AvroAccess<'_> {
    fn access<'a>(&'a self, path: &[&str], type_expected: &DataType) -> AccessResult<DatumCow<'a>> {
        let mut value = self.value;
        let mut unresolved_schema = self.options.root_schema;

        debug_assert!(
            path.len() == 1
                || (path.len() == 2 && matches!(path[0], "before" | "after" | "source")),
            "unexpected path access: {:?}",
            path
        );
        let mut i = 0;
        while i < path.len() {
            let key = path[i];
            let create_error = || AccessError::Undefined {
                name: key.to_owned(),
                path: path.iter().take(i).join("."),
            };
            match value {
                Value::Union(_, v) => {
                    // The debezium "before" field is a nullable union.
                    // "fields": [
                    // {
                    //     "name": "before",
                    //     "type": [
                    //         "null",
                    //         {
                    //             "type": "record",
                    //             "name": "Value",
                    //             "fields": [...],
                    //         }
                    //     ],
                    //     "default": null
                    // },
                    // {
                    //     "name": "after",
                    //     "type": [
                    //         "null",
                    //         "Value"
                    //     ],
                    //     "default": null
                    // },
                    // ...]
                    value = v;
                    let Schema::Union(u) = self.options.inner.lookup_ref(unresolved_schema) else {
                        return Err(create_error());
                    };
                    let Some(schema) = get_nullable_union_inner(u) else {
                        return Err(create_error());
                    };
                    unresolved_schema = schema;
                    continue;
                }
                Value::Record(fields) => {
                    let Schema::Record(record_schema) =
                        self.options.inner.lookup_ref(unresolved_schema)
                    else {
                        return Err(create_error());
                    };
                    if let Some(idx) = record_schema.lookup.get(key) {
                        value = &fields[*idx].1;
                        unresolved_schema = &record_schema.fields[*idx].schema;
                        i += 1;
                        continue;
                    }
                }
                _ => (),
            }
            Err(create_error())?;
        }

        self.options
            .inner
            .convert_to_datum(unresolved_schema, value, type_expected)
    }
}

/// If the union schema is `[null, T]` or `[T, null]`, returns `Some(T)`; otherwise returns `None`.
pub fn get_nullable_union_inner(union_schema: &UnionSchema) -> Option<&'_ Schema> {
    let variants = union_schema.variants();
    // Note: `[null, null] is invalid`, we don't need to worry about that.
    if variants.len() == 2 && variants.contains(&Schema::Null) {
        let inner_schema = variants
            .iter()
            .find(|s| !matches!(s, &&Schema::Null))
            .unwrap();
        Some(inner_schema)
    } else {
        None
    }
}

pub(crate) fn unix_epoch_days() -> i32 {
    Date::from_ymd_uncheck(1970, 1, 1).0.num_days_from_ce()
}

pub(crate) fn avro_to_jsonb(avro: &Value, builder: &mut jsonbb::Builder) -> AccessResult<()> {
    match avro {
        Value::Null => builder.add_null(),
        Value::Boolean(b) => builder.add_bool(*b),
        Value::Int(i) => builder.add_i64(*i as i64),
        Value::String(s) => builder.add_string(s),
        Value::Map(m) => {
            builder.begin_object();
            for (k, v) in m {
                builder.add_string(k);
                avro_to_jsonb(v, builder)?;
            }
            builder.end_object()
        }
        // same representation as map
        Value::Record(r) => {
            builder.begin_object();
            for (k, v) in r {
                builder.add_string(k);
                avro_to_jsonb(v, builder)?;
            }
            builder.end_object()
        }
        Value::Array(a) => {
            builder.begin_array();
            for v in a {
                avro_to_jsonb(v, builder)?;
            }
            builder.end_array()
        }

        // TODO: figure out where the following encoding is reasonable before enabling them.
        // See discussions: https://github.com/risingwavelabs/risingwave/pull/16948

        // jsonbb supports int64, but JSON spec does not allow it. How should we handle it?
        // BTW, protobuf canonical JSON converts int64 to string.
        // Value::Long(l) => builder.add_i64(*l),
        // Value::Float(f) => {
        //     if f.is_nan() || f.is_infinite() {
        //         // XXX: pad null or return err here?
        //         builder.add_null()
        //     } else {
        //         builder.add_f64(*f as f64)
        //     }
        // }
        // Value::Double(f) => {
        //     if f.is_nan() || f.is_infinite() {
        //         // XXX: pad null or return err here?
        //         builder.add_null()
        //     } else {
        //         builder.add_f64(*f)
        //     }
        // }
        // // XXX: What encoding to use?
        // // ToText is \x plus hex string.
        // Value::Bytes(b) => builder.add_string(&ToText::to_text(&b.as_slice())),
        // Value::Enum(_, symbol) => {
        //     builder.add_string(&symbol);
        // }
        // Value::Uuid(id) => builder.add_string(&id.as_hyphenated().to_string()),
        // // For Union, one concern is that the avro union is tagged (like rust enum) but json union is untagged (like c union).
        // // When the union consists of multiple records, it is possible to distinguish which variant is active in avro, but in json they will all become jsonb objects and indistinguishable.
        // Value::Union(_, v) => avro_to_jsonb(v, builder)?
        // XXX: pad null or return err here?
        v @ (Value::Long(_)
        | Value::Float(_)
        | Value::Double(_)
        | Value::Bytes(_)
        | Value::Enum(_, _)
        | Value::Fixed(_, _)
        | Value::Date(_)
        | Value::Decimal(_)
        | Value::BigDecimal(_)
        | Value::TimeMillis(_)
        | Value::TimeMicros(_)
        | Value::TimestampMillis(_)
        | Value::TimestampMicros(_)
        | Value::TimestampNanos(_)
        | Value::LocalTimestampMillis(_)
        | Value::LocalTimestampMicros(_)
        | Value::LocalTimestampNanos(_)
        | Value::Duration(_)
        | Value::Uuid(_)
        | Value::Union(_, _)) => {
            bail_uncategorized!(
                "unimplemented conversion from avro to jsonb: {:?}",
                ValueKind::from(v)
            )
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use apache_avro::{Decimal as AvroDecimal, from_avro_datum};
    use expect_test::expect;
    use risingwave_common::types::{Datum, Decimal};

    use super::*;

    /// Test the behavior of the Rust Avro lib for handling union with logical type.
    #[test]
    fn test_avro_lib_union() {
        // duplicate types
        let s = Schema::parse_str(r#"["null", "null"]"#);
        expect![[r#"
            Err(
                Error {
                    details: Unions cannot contain duplicate types,
                },
            )
        "#]]
        .assert_debug_eq(&s);
        let s = Schema::parse_str(r#"["int", "int"]"#);
        expect![[r#"
            Err(
                Error {
                    details: Unions cannot contain duplicate types,
                },
            )
        "#]]
        .assert_debug_eq(&s);
        // multiple map/array are considered as the same type, regardless of the element type!
        let s = Schema::parse_str(
            r#"[
"null",
{
    "type": "map",
    "values" : "long",
    "default": {}
},
{
    "type": "map",
    "values" : "int",
    "default": {}
}
]
"#,
        );
        expect![[r#"
            Err(
                Error {
                    details: Unions cannot contain duplicate types,
                },
            )
        "#]]
        .assert_debug_eq(&s);
        let s = Schema::parse_str(
            r#"[
"null",
{
    "type": "array",
    "items" : "long",
    "default": {}
},
{
    "type": "array",
    "items" : "int",
    "default": {}
}
]
"#,
        );
        expect![[r#"
            Err(
                Error {
                    details: Unions cannot contain duplicate types,
                },
            )
        "#]]
        .assert_debug_eq(&s);
        // multiple named types
        let s = Schema::parse_str(
            r#"[
"null",
{"type":"fixed","name":"a","size":16},
{"type":"fixed","name":"b","size":32}
]
"#,
        );
        expect![[r#"
            Ok(
                Union(
                    UnionSchema {
                        schemas: [
                            Null,
                            Fixed(
                                FixedSchema {
                                    name: Name {
                                        name: "a",
                                        namespace: None,
                                    },
                                    aliases: None,
                                    doc: None,
                                    size: 16,
                                    default: None,
                                    attributes: {},
                                },
                            ),
                            Fixed(
                                FixedSchema {
                                    name: Name {
                                        name: "b",
                                        namespace: None,
                                    },
                                    aliases: None,
                                    doc: None,
                                    size: 32,
                                    default: None,
                                    attributes: {},
                                },
                            ),
                        ],
                        variant_index: {
                            Null: 0,
                        },
                    },
                ),
            )
        "#]]
        .assert_debug_eq(&s);

        // union in union
        let s = Schema::parse_str(r#"["int", ["null", "int"]]"#);
        expect![[r#"
            Err(
                Error {
                    details: Unions may not directly contain a union,
                },
            )
        "#]]
        .assert_debug_eq(&s);

        // logical type
        let s = Schema::parse_str(r#"["null", {"type":"string","logicalType":"uuid"}]"#).unwrap();
        expect![[r#"
            Union(
                UnionSchema {
                    schemas: [
                        Null,
                        Uuid,
                    ],
                    variant_index: {
                        Null: 0,
                        Uuid: 1,
                    },
                },
            )
        "#]]
        .assert_debug_eq(&s);
        // Note: Java Avro lib rejects this (logical type unions with its physical type)
        let s = Schema::parse_str(r#"["string", {"type":"string","logicalType":"uuid"}]"#).unwrap();
        expect![[r#"
            Union(
                UnionSchema {
                    schemas: [
                        String,
                        Uuid,
                    ],
                    variant_index: {
                        String: 0,
                        Uuid: 1,
                    },
                },
            )
        "#]]
        .assert_debug_eq(&s);
        // Note: Java Avro lib rejects this (logical type unions with its physical type)
        let s = Schema::parse_str(r#"["int", {"type":"int", "logicalType": "date"}]"#).unwrap();
        expect![[r#"
            Union(
                UnionSchema {
                    schemas: [
                        Int,
                        Date,
                    ],
                    variant_index: {
                        Int: 0,
                        Date: 1,
                    },
                },
            )
        "#]]
        .assert_debug_eq(&s);
        // Note: Java Avro lib allows this (2 decimal with different "name")
        let s = Schema::parse_str(
            r#"[
{"type":"fixed","name":"Decimal128","size":16,"logicalType":"decimal","precision":38,"scale":2},
{"type":"fixed","name":"Decimal256","size":32,"logicalType":"decimal","precision":50,"scale":2}
]"#,
        );
        expect![[r#"
            Err(
                Error {
                    details: Unions cannot contain duplicate types,
                },
            )
        "#]]
        .assert_debug_eq(&s);
    }

    #[test]
    fn test_avro_lib_union_record_bug() {
        // multiple named types (record)
        let s = Schema::parse_str(
            r#"
    {
      "type": "record",
      "name": "Root",
      "fields": [
        {
          "name": "unionTypeComplex",
          "type": [
            "null",
            {"type": "record", "name": "Email","fields": [{"name":"inner","type":"string"}]},
            {"type": "record", "name": "Fax","fields": [{"name":"inner","type":"int"}]},
            {"type": "record", "name": "Sms","fields": [{"name":"inner","type":"int"}]}
          ]
        }
      ]
    }
        "#,
        )
        .unwrap();

        let bytes = hex::decode("060c").unwrap();
        // Correct should be variant 3 (Sms)
        let correct_value = from_avro_datum(&s, &mut bytes.as_slice(), None);
        expect![[r#"
                Ok(
                    Record(
                        [
                            (
                                "unionTypeComplex",
                                Union(
                                    3,
                                    Record(
                                        [
                                            (
                                                "inner",
                                                Int(
                                                    6,
                                                ),
                                            ),
                                        ],
                                    ),
                                ),
                            ),
                        ],
                    ),
                )
            "#]]
        .assert_debug_eq(&correct_value);
        // Bug: We got variant 2 (Fax) here, if we pass the reader schema.
        let wrong_value = from_avro_datum(&s, &mut bytes.as_slice(), Some(&s));
        expect![[r#"
                Ok(
                    Record(
                        [
                            (
                                "unionTypeComplex",
                                Union(
                                    2,
                                    Record(
                                        [
                                            (
                                                "inner",
                                                Int(
                                                    6,
                                                ),
                                            ),
                                        ],
                                    ),
                                ),
                            ),
                        ],
                    ),
                )
            "#]]
        .assert_debug_eq(&wrong_value);

        // The bug below can explain what happened.
        // The two records below are actually incompatible: https://avro.apache.org/docs/1.11.1/specification/_print/#schema-resolution
        // > both schemas are records with the _same (unqualified) name_
        // In from_avro_datum, it first reads the value with the writer schema, and then
        // it just uses the reader schema to interpret the value.
        // The value doesn't have record "name" information. So it wrongly passed the conversion.
        // The correct way is that we need to use both the writer and reader schema in the second step to interpret the value.

        let s = Schema::parse_str(
            r#"
    {
      "type": "record",
      "name": "Root",
      "fields": [
        {
          "name": "a",
          "type": "int"
        }
      ]
    }
        "#,
        )
        .unwrap();
        let s2 = Schema::parse_str(
            r#"
{
  "type": "record",
  "name": "Root222",
  "fields": [
    {
      "name": "a",
      "type": "int"
    }
  ]
}
    "#,
        )
        .unwrap();

        let bytes = hex::decode("0c").unwrap();
        let value = from_avro_datum(&s, &mut bytes.as_slice(), Some(&s2));
        expect![[r#"
            Ok(
                Record(
                    [
                        (
                            "a",
                            Int(
                                6,
                            ),
                        ),
                    ],
                ),
            )
        "#]]
        .assert_debug_eq(&value);
    }

    #[test]
    fn test_convert_decimal() {
        // 280
        let v = vec![1, 24];
        let avro_decimal = AvroDecimal::from(v);
        let rust_decimal = scaled_bigint_to_rust_decimal(avro_decimal.into(), 0).unwrap();
        assert_eq!(rust_decimal, rust_decimal::Decimal::from(280));

        // 28.1
        let v = vec![1, 25];
        let avro_decimal = AvroDecimal::from(v);
        let rust_decimal = scaled_bigint_to_rust_decimal(avro_decimal.into(), 1).unwrap();
        assert_eq!(rust_decimal, rust_decimal::Decimal::try_from(28.1).unwrap());

        // 1.1234567891
        let value = BigInt::from(11234567891_i64);
        let decimal = scaled_bigint_to_rust_decimal(value, 10).unwrap();
        assert_eq!(
            decimal,
            rust_decimal::Decimal::try_from(1.1234567891).unwrap()
        );

        // 1.123456789123456789123456789
        let v = vec![3, 161, 77, 58, 146, 180, 49, 220, 100, 4, 95, 21];
        let avro_decimal = AvroDecimal::from(v);
        let rust_decimal = scaled_bigint_to_rust_decimal(avro_decimal.into(), 27).unwrap();
        assert_eq!(
            rust_decimal,
            rust_decimal::Decimal::from_str("1.123456789123456789123456789").unwrap()
        );
    }

    /// Convert Avro value to datum.For now, support the following [Avro type](https://avro.apache.org/docs/current/spec.html).
    ///  - boolean
    ///  - int : i32
    ///  - long: i64
    ///  - float: f32
    ///  - double: f64
    ///  - string: String
    ///  - Date (the number of days from the unix epoch, 1970-1-1 UTC)
    ///  - Timestamp (the number of milliseconds from the unix epoch,  1970-1-1 00:00:00.000 UTC)
    fn from_avro_value(
        value: Value,
        value_schema: &Schema,
        shape: &DataType,
    ) -> anyhow::Result<Datum> {
        Ok(AvroParseOptions::create(value_schema)
            .inner
            .convert_to_datum(value_schema, &value, shape)?
            .to_owned_datum())
    }

    #[test]
    fn test_avro_timestamptz_micros() {
        let v1 = Value::TimestampMicros(1620000000000000);
        let v2 = Value::TimestampMillis(1620000000000);
        let value_schema1 = Schema::TimestampMicros;
        let value_schema2 = Schema::TimestampMillis;
        let datum1 = from_avro_value(v1, &value_schema1, &DataType::Timestamptz).unwrap();
        let datum2 = from_avro_value(v2, &value_schema2, &DataType::Timestamptz).unwrap();
        assert_eq!(
            datum1,
            Some(ScalarImpl::Timestamptz(
                Timestamptz::from_str("2021-05-03T00:00:00Z").unwrap()
            ))
        );
        assert_eq!(
            datum2,
            Some(ScalarImpl::Timestamptz(
                Timestamptz::from_str("2021-05-03T00:00:00Z").unwrap()
            ))
        );
    }

    #[test]
    fn test_decimal_truncate() {
        let schema = Schema::parse_str(
            r#"
            {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 38,
                "scale": 18
            }
            "#,
        )
        .unwrap();
        let bytes = vec![0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f];
        let value = Value::Decimal(AvroDecimal::from(bytes));
        let resp = from_avro_value(value, &schema, &DataType::Decimal).unwrap();
        assert_eq!(
            resp,
            Some(ScalarImpl::Decimal(Decimal::Normalized(
                rust_decimal::Decimal::from_str("0.017802464409370431").unwrap()
            )))
        );
    }

    #[test]
    fn test_variable_scale_decimal() {
        let schema = Schema::parse_str(
            r#"
            {
                "type": "record",
                "name": "VariableScaleDecimal",
                "namespace": "io.debezium.data",
                "fields": [
                    {
                        "name": "scale",
                        "type": "int"
                    },
                    {
                        "name": "value",
                        "type": "bytes"
                    }
                ]
            }
            "#,
        )
        .unwrap();
        let value = Value::Record(vec![
            ("scale".to_owned(), Value::Int(0)),
            ("value".to_owned(), Value::Bytes(vec![0x01, 0x02, 0x03])),
        ]);

        let resp = from_avro_value(value, &schema, &DataType::Decimal).unwrap();
        assert_eq!(resp, Some(ScalarImpl::Decimal(Decimal::from(66051))));
    }
}
