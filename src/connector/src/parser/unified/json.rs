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

use std::str::FromStr;
use std::sync::LazyLock;

use base64::Engine;
use itertools::Itertools;
use num_bigint::BigInt;
use risingwave_common::array::{Finite32, ListValue, StructValue};
use risingwave_common::cast::{i64_to_timestamp, i64_to_timestamptz, str_to_bytea};
use risingwave_common::log::LogSuppressor;
use risingwave_common::types::{
    DEBEZIUM_UNAVAILABLE_VALUE, DataType, Date, Decimal, Int256, Interval, JsonbVal, ScalarImpl,
    Time, Timestamp, Timestamptz, ToOwnedDatum, VectorVal, debezium_unavailable_vector,
};
use risingwave_connector_codec::decoder::utils::scaled_bigint_to_rust_decimal;
use simd_json::base::ValueAsObject;
use simd_json::prelude::{
    TypedValue, ValueAsArray, ValueAsScalar, ValueObjectAccess, ValueTryAsScalar,
};
use simd_json::{BorrowedValue, ValueType};
use thiserror_ext::AsReport;

use super::{Access, AccessError, AccessResult};
use crate::parser::DatumCow;
use crate::schema::{InvalidOptionError, bail_invalid_option_error};

/// Try to parse a Debezium `PostGIS` spatial object.
///
/// Debezium represents `PostGIS` `geometry` and `geography` values as an object:
/// `{"srid": <int>, "wkb": <base64_string>}`.
/// For our current Postgres CDC ingestion, the `wkb` field is expected to be EWKB bytes (base64-encoded),
/// and `srid` is redundant. We decode `wkb` into raw bytes and store it as `bytea`.
///
/// Note: Debezium provides an SMT to convert between `WKB` and `EWKB`, which may be useful for future
/// unification across connectors (e.g., MySQL): see `GeometryFormatTransformer`.
///
/// Return semantics:
/// - `Ok(Some(bytes))`: The input matches the Debezium spatial shape (`srid` is numeric AND `wkb` is string),
///   and we successfully decoded `wkb` into bytes.
/// - `Ok(None)`: The input does NOT look like a Debezium spatial object. This allows the caller to keep the
///   match arm focused on dispatching, and avoids misclassifying other JSON objects that might map to `bytea`
///   in the future.
/// - `Err(...)`: The input looks like a Debezium spatial object, but decoding/parsing failed (e.g. invalid
///   base64). This indicates a real data/format error and should not be silently ignored.
fn try_parse_debezium_postgis_spatial_as_bytea(
    value: &BorrowedValue<'_>,
    create_error: impl Fn() -> AccessError,
) -> AccessResult<Option<Box<[u8]>>> {
    let obj = match value.as_object() {
        Some(obj) => obj,
        None => return Ok(None),
    };

    // Strictly identify the spatial object by checking both fields and their types.
    // There may be other objects that map to bytea in the future.
    let srid = obj.get("srid").and_then(|v| v.as_i64());
    let wkb = obj.get("wkb").and_then(|v| v.as_str());

    let (Some(_srid), Some(wkb)) = (srid, wkb) else {
        return Ok(None);
    };

    let bytes = base64::engine::general_purpose::STANDARD
        .decode(wkb)
        .map_err(|_| create_error())?
        .into_boxed_slice();

    Ok(Some(bytes))
}

#[derive(Clone, Debug)]
pub enum ByteaHandling {
    Standard,
    // debezium converts postgres bytea to base64 format
    Base64,
}
#[derive(Clone, Debug)]
pub enum TimeHandling {
    Milli,
    Micro,
}

#[derive(Clone, Copy, Debug)]
pub enum BigintUnsignedHandlingMode {
    /// Convert unsigned bigint to signed bigint (default)
    Long,
    /// Use base64-encoded decimal for unsigned bigint (Debezium precise mode)
    Precise,
}

#[derive(Clone, Debug)]
pub enum TimestamptzHandling {
    /// `"2024-04-11T02:00:00.123456Z"`
    UtcString,
    /// `"2024-04-11 02:00:00.123456"`
    UtcWithoutSuffix,
    /// `1712800800123`
    Milli,
    /// `1712800800123456`
    Micro,
    /// Both `1712800800123` (ms) and `1712800800123456` (us) maps to `2024-04-11`.
    ///
    /// Only works for `[1973-03-03 09:46:40, 5138-11-16 09:46:40)`.
    ///
    /// This option is backward compatible.
    GuessNumberUnit,
}

impl TimestamptzHandling {
    pub const OPTION_KEY: &'static str = "timestamptz.handling.mode";

    pub fn from_options(value: &str) -> Result<Self, InvalidOptionError> {
        let mode = match value {
            "utc_string" => Self::UtcString,
            "utc_without_suffix" => Self::UtcWithoutSuffix,
            "micro" => Self::Micro,
            "milli" => Self::Milli,
            "guess_number_unit" => Self::GuessNumberUnit,
            v => bail_invalid_option_error!("unrecognized {} value {}", Self::OPTION_KEY, v),
        };
        Ok(mode)
    }
}

#[derive(Clone, Debug)]
pub enum TimestampHandling {
    Milli,
    Micro,
    GuessNumberUnit,
}

#[derive(Clone, Debug)]
pub enum JsonValueHandling {
    AsValue,
    AsString,
}
#[derive(Clone, Debug)]
pub enum NumericHandling {
    Strict,
    // should integer be parsed to float
    Relax {
        // should "3.14" be parsed to 3.14 in float
        string_parsing: bool,
    },
}
#[derive(Clone, Debug)]
pub enum BooleanHandling {
    Strict,
    // should integer 1,0 be parsed to boolean (debezium)
    Relax {
        // should "True" "False" be parsed to true or false in boolean
        string_parsing: bool,
        // should string "1" "0" be paesed to boolean (cannal + mysql)
        string_integer_parsing: bool,
    },
}

#[derive(Clone, Debug)]
pub enum VarcharHandling {
    // do not allow other types cast to varchar
    Strict,
    // allow Json Value (Null, Bool, I64, I128, U64, U128, F64) cast to varchar
    OnlyPrimaryTypes,
    // allow all type cast to varchar (inc. Array, Object)
    AllTypes,
}

#[derive(Clone, Debug)]
pub enum StructHandling {
    // only allow object parsed to struct
    Strict,
    // allow string containing a serialized json object (like "{\"a\": 1, \"b\": 2}") parsed to
    // struct
    AllowJsonString,
}

#[derive(Clone, Debug)]
pub struct JsonParseOptions {
    pub bytea_handling: ByteaHandling,
    pub time_handling: TimeHandling,
    pub timestamp_handling: TimestampHandling,
    pub timestamptz_handling: TimestamptzHandling,
    pub json_value_handling: JsonValueHandling,
    pub numeric_handling: NumericHandling,
    pub boolean_handling: BooleanHandling,
    pub varchar_handling: VarcharHandling,
    pub struct_handling: StructHandling,
    pub bigint_unsigned_handling: BigintUnsignedHandlingMode,
    pub ignoring_keycase: bool,
    pub handle_toast_columns: bool,
}

impl Default for JsonParseOptions {
    fn default() -> Self {
        Self::DEFAULT.clone()
    }
}

impl JsonParseOptions {
    pub const CANAL: JsonParseOptions = JsonParseOptions {
        bytea_handling: ByteaHandling::Standard,
        time_handling: TimeHandling::Micro,
        timestamp_handling: TimestampHandling::GuessNumberUnit, // backward-compatible
        timestamptz_handling: TimestamptzHandling::GuessNumberUnit, // backward-compatible
        json_value_handling: JsonValueHandling::AsValue,
        numeric_handling: NumericHandling::Relax {
            string_parsing: true,
        },
        boolean_handling: BooleanHandling::Relax {
            string_parsing: true,
            string_integer_parsing: true,
        },
        varchar_handling: VarcharHandling::Strict,
        struct_handling: StructHandling::Strict,
        bigint_unsigned_handling: BigintUnsignedHandlingMode::Long, // default to long mode
        ignoring_keycase: true,
        handle_toast_columns: false,
    };
    pub const DEFAULT: JsonParseOptions = JsonParseOptions {
        bytea_handling: ByteaHandling::Standard,
        time_handling: TimeHandling::Micro,
        timestamp_handling: TimestampHandling::GuessNumberUnit, // backward-compatible
        timestamptz_handling: TimestamptzHandling::GuessNumberUnit, // backward-compatible
        json_value_handling: JsonValueHandling::AsValue,
        numeric_handling: NumericHandling::Relax {
            string_parsing: true,
        },
        boolean_handling: BooleanHandling::Strict,
        varchar_handling: VarcharHandling::OnlyPrimaryTypes,
        struct_handling: StructHandling::AllowJsonString,
        bigint_unsigned_handling: BigintUnsignedHandlingMode::Long, // default to long mode
        ignoring_keycase: true,
        handle_toast_columns: false,
    };

    pub fn new_for_debezium(
        timestamptz_handling: TimestamptzHandling,
        timestamp_handling: TimestampHandling,
        time_handling: TimeHandling,
        bigint_unsigned_handling: BigintUnsignedHandlingMode,
        handle_toast_columns: bool,
    ) -> Self {
        Self {
            bytea_handling: ByteaHandling::Base64,
            time_handling,
            timestamp_handling,
            timestamptz_handling,
            json_value_handling: JsonValueHandling::AsString,
            numeric_handling: NumericHandling::Relax {
                string_parsing: false,
            },
            boolean_handling: BooleanHandling::Relax {
                string_parsing: false,
                string_integer_parsing: false,
            },
            varchar_handling: VarcharHandling::Strict,
            struct_handling: StructHandling::Strict,
            bigint_unsigned_handling,
            ignoring_keycase: true,
            handle_toast_columns,
        }
    }

    pub fn parse<'a>(
        &self,
        value: &'a BorrowedValue<'a>,
        type_expected: &DataType,
    ) -> AccessResult<DatumCow<'a>> {
        let create_error = || AccessError::TypeError {
            expected: format!("{:?}", type_expected),
            got: value.value_type().to_string(),
            value: value.to_string(),
        };
        let v: ScalarImpl = match (type_expected, value.value_type()) {
            (_, ValueType::Null) => return Ok(DatumCow::NULL),
            // ---- Boolean -----
            (DataType::Boolean, ValueType::Bool) => value.as_bool().unwrap().into(),

            (
                DataType::Boolean,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) if matches!(self.boolean_handling, BooleanHandling::Relax { .. })
                && matches!(value.as_i64(), Some(0i64) | Some(1i64)) =>
            {
                (value.as_i64() == Some(1i64)).into()
            }

            (DataType::Boolean, ValueType::String)
                if matches!(
                    self.boolean_handling,
                    BooleanHandling::Relax {
                        string_parsing: true,
                        ..
                    }
                ) =>
            {
                match value.as_str().unwrap().to_lowercase().as_str() {
                    "true" => true.into(),
                    "false" => false.into(),
                    c @ ("1" | "0")
                        if matches!(
                            self.boolean_handling,
                            BooleanHandling::Relax {
                                string_parsing: true,
                                string_integer_parsing: true
                            }
                        ) =>
                    {
                        if c == "1" {
                            true.into()
                        } else {
                            false.into()
                        }
                    }
                    _ => Err(create_error())?,
                }
            }
            // ---- Int16 -----
            (
                DataType::Int16,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => value.try_as_i16().map_err(|_| create_error())?.into(),

            (DataType::Int16, ValueType::String)
                if matches!(
                    self.numeric_handling,
                    NumericHandling::Relax {
                        string_parsing: true
                    }
                ) =>
            {
                value
                    .as_str()
                    .unwrap()
                    .parse::<i16>()
                    .map_err(|_| create_error())?
                    .into()
            }
            // ---- Int32 -----
            (
                DataType::Int32,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => value.try_as_i32().map_err(|_| create_error())?.into(),

            (DataType::Int32, ValueType::String)
                if matches!(
                    self.numeric_handling,
                    NumericHandling::Relax {
                        string_parsing: true
                    }
                ) =>
            {
                value
                    .as_str()
                    .unwrap()
                    .parse::<i32>()
                    .map_err(|_| create_error())?
                    .into()
            }
            // ---- Int64 -----
            (
                DataType::Int64,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => value.try_as_i64().map_err(|_| create_error())?.into(),

            (DataType::Int64, ValueType::String)
                if matches!(
                    self.numeric_handling,
                    NumericHandling::Relax {
                        string_parsing: true
                    }
                ) =>
            {
                value
                    .as_str()
                    .unwrap()
                    .parse::<i64>()
                    .map_err(|_| create_error())?
                    .into()
            }
            // ---- Float32 -----
            (
                DataType::Float32,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) if matches!(self.numeric_handling, NumericHandling::Relax { .. }) => {
                (value.try_as_i64().map_err(|_| create_error())? as f32).into()
            }
            (DataType::Float32, ValueType::String)
                if matches!(
                    self.numeric_handling,
                    NumericHandling::Relax {
                        string_parsing: true
                    }
                ) =>
            {
                value
                    .as_str()
                    .unwrap()
                    .parse::<f32>()
                    .map_err(|_| create_error())?
                    .into()
            }
            (DataType::Float32, ValueType::F64) => {
                value.try_as_f32().map_err(|_| create_error())?.into()
            }
            // ---- Float64 -----
            (
                DataType::Float64,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) if matches!(self.numeric_handling, NumericHandling::Relax { .. }) => {
                (value.try_as_i64().map_err(|_| create_error())? as f64).into()
            }
            (DataType::Float64, ValueType::String)
                if matches!(
                    self.numeric_handling,
                    NumericHandling::Relax {
                        string_parsing: true
                    }
                ) =>
            {
                value
                    .as_str()
                    .unwrap()
                    .parse::<f64>()
                    .map_err(|_| create_error())?
                    .into()
            }
            (DataType::Float64, ValueType::F64) => {
                value.try_as_f64().map_err(|_| create_error())?.into()
            }
            // ---- Decimal -----
            (DataType::Decimal, ValueType::I128 | ValueType::U128) => {
                Decimal::from_str(&value.try_as_i128().map_err(|_| create_error())?.to_string())
                    .map_err(|_| create_error())?
                    .into()
            }
            (DataType::Decimal, ValueType::I64 | ValueType::U64) => {
                let i64_val = value.try_as_i64().map_err(|_| create_error())?;
                Decimal::from(i64_val).into()
            }
            (DataType::Decimal, ValueType::String) => {
                let str_val = value.as_str().unwrap();
                // the following values are special string generated by Debezium and should be handled separately
                match str_val {
                    "NAN" => return Ok(DatumCow::Owned(Some(ScalarImpl::Decimal(Decimal::NaN)))),
                    "POSITIVE_INFINITY" => {
                        return Ok(DatumCow::Owned(Some(ScalarImpl::Decimal(
                            Decimal::PositiveInf,
                        ))));
                    }
                    "NEGATIVE_INFINITY" => {
                        return Ok(DatumCow::Owned(Some(ScalarImpl::Decimal(
                            Decimal::NegativeInf,
                        ))));
                    }
                    _ => {}
                }

                Decimal::from_str(str_val)
                    .or_else(|_err| {
                        try_base64_decode_decimal(
                            str_val,
                            self.bigint_unsigned_handling,
                            create_error,
                        )
                    })?
                    .into()
            }

            (DataType::Decimal, ValueType::F64) => {
                Decimal::try_from(value.try_as_f64().map_err(|_| create_error())?)
                    .map_err(|_| create_error())?
                    .into()
            }
            (DataType::Decimal, ValueType::Object) => {
                // ref https://github.com/risingwavelabs/risingwave/issues/10628
                // handle debezium json (variable scale): {"scale": int, "value": bytes}
                let scale = value
                    .get("scale")
                    .ok_or_else(create_error)?
                    .as_i32()
                    .unwrap();
                let value = value
                    .get("value")
                    .ok_or_else(create_error)?
                    .as_str()
                    .unwrap()
                    .as_bytes();
                let unscaled = BigInt::from_signed_bytes_be(value);
                let decimal = scaled_bigint_to_rust_decimal(unscaled, scale as _)?;
                ScalarImpl::Decimal(Decimal::Normalized(decimal))
            }
            // ---- Date -----
            (
                DataType::Date,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => Date::with_days_since_unix_epoch(value.try_as_i32().map_err(|_| create_error())?)
                .map_err(|_| create_error())?
                .into(),
            (DataType::Date, ValueType::String) => value
                .as_str()
                .unwrap()
                .parse::<Date>()
                .map_err(|_| create_error())?
                .into(),
            // ---- Varchar -----
            (DataType::Varchar, ValueType::String) => {
                return Ok(DatumCow::Borrowed(Some(value.as_str().unwrap().into())));
            }
            (
                DataType::Varchar,
                ValueType::Bool
                | ValueType::I64
                | ValueType::I128
                | ValueType::U64
                | ValueType::U128
                | ValueType::F64,
            ) if matches!(self.varchar_handling, VarcharHandling::OnlyPrimaryTypes) => {
                value.to_string().into()
            }
            (
                DataType::Varchar,
                ValueType::Bool
                | ValueType::I64
                | ValueType::I128
                | ValueType::U64
                | ValueType::U128
                | ValueType::F64
                | ValueType::Array
                | ValueType::Object,
            ) if matches!(self.varchar_handling, VarcharHandling::AllTypes) => {
                value.to_string().into()
            }
            // ---- Time -----
            (DataType::Time, ValueType::String) => value
                .as_str()
                .unwrap()
                .parse::<Time>()
                .map_err(|_| create_error())?
                .into(),
            (
                DataType::Time,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => value
                .as_i64()
                .map(|i| match self.time_handling {
                    TimeHandling::Milli => Time::with_milli(i as u32),
                    TimeHandling::Micro => Time::with_micro(i as u64),
                })
                .unwrap()
                .map_err(|_| create_error())?
                .into(),
            // ---- Timestamp -----
            (DataType::Timestamp, ValueType::String) => value
                .as_str()
                .unwrap()
                .parse::<Timestamp>()
                .map_err(|_| create_error())?
                .into(),
            (
                DataType::Timestamp,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => value
                .as_i64()
                .map(|num| match self.timestamp_handling {
                    // Only when user configures debezium.time.precision.mode = 'connect',
                    // the Milli branch will be executed
                    TimestampHandling::Milli => {
                        Timestamp::with_millis(num).map_err(|_| create_error())
                    }
                    TimestampHandling::Micro => {
                        Timestamp::with_micros(num).map_err(|_| create_error())
                    }
                    TimestampHandling::GuessNumberUnit => {
                        i64_to_timestamp(num).map_err(|_| create_error())
                    }
                })
                .ok_or_else(create_error)??
                .into(),
            // ---- Timestamptz -----
            (DataType::Timestamptz, ValueType::String) => match self.timestamptz_handling {
                TimestamptzHandling::UtcWithoutSuffix => value
                    .as_str()
                    .unwrap()
                    .parse::<Timestamp>()
                    .map(|naive_utc| {
                        Timestamptz::from_micros_uncheck(naive_utc.0.and_utc().timestamp_micros())
                    })
                    .map_err(|_| create_error())?
                    .into(),
                // Unless explicitly requested `utc_without_utc`, we parse string with `YYYY-MM-DDTHH:MM:SSZ`.
                _ => value
                    .as_str()
                    .unwrap()
                    .parse::<Timestamptz>()
                    .map_err(|_| create_error())?
                    .into(),
            },
            (
                DataType::Timestamptz,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => value
                .as_i64()
                .and_then(|num| match self.timestamptz_handling {
                    TimestamptzHandling::GuessNumberUnit => i64_to_timestamptz(num).ok(),
                    TimestamptzHandling::Micro => Timestamptz::from_micros(num),
                    TimestamptzHandling::Milli => Timestamptz::from_millis(num),
                    // When explicitly requested string format, number without units are rejected.
                    TimestamptzHandling::UtcString | TimestamptzHandling::UtcWithoutSuffix => None,
                })
                .ok_or_else(create_error)?
                .into(),
            // ---- Interval -----
            (DataType::Interval, ValueType::String) => value
                .as_str()
                .unwrap()
                .parse::<Interval>()
                .map_err(|_| create_error())?
                .into(),
            // ---- Struct -----
            (DataType::Struct(struct_type_info), ValueType::Object) => {
                // Collecting into a Result<Vec<_>> doesn't reserve the capacity in advance, so we `Vec::with_capacity` instead.
                // https://github.com/rust-lang/rust/issues/48994
                let mut fields = Vec::with_capacity(struct_type_info.len());
                for (field_name, field_type) in struct_type_info.iter() {
                    let field_value = json_object_get_case_insensitive(value, field_name)
                            .unwrap_or_else(|| {
                                let error = AccessError::Undefined {
                                    name: field_name.to_owned(),
                                    path: struct_type_info.to_string(), // TODO: this is not good, we should maintain a path stack
                                };
                                // TODO: is it possible to unify the logging with the one in `do_action`?
                                static LOG_SUPPRESSOR: LazyLock<LogSuppressor> =  LazyLock::new(LogSuppressor::default);
                                if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                                    tracing::warn!(error = %error.as_report(), suppressed_count, "undefined nested field, padding with `NULL`");
                                }
                                &BorrowedValue::Static(simd_json::StaticNode::Null)
                            });
                    fields.push(
                        self.parse(field_value, field_type)
                            .map(|d| d.to_owned_datum())?,
                    );
                }
                StructValue::new(fields).into()
            }

            // String containing json object, e.g. "{\"a\": 1, \"b\": 2}"
            // Try to parse it as json object.
            (DataType::Struct(_), ValueType::String)
                if matches!(self.struct_handling, StructHandling::AllowJsonString) =>
            {
                // TODO: avoid copy by accepting `&mut BorrowedValue` in `parse` method.
                let mut value = value.as_str().unwrap().as_bytes().to_vec();
                let value =
                    simd_json::to_borrowed_value(&mut value[..]).map_err(|_| create_error())?;
                return self
                    .parse(&value, type_expected)
                    .map(|d| d.to_owned_datum().into());
            }

            // ---- List -----
            (DataType::List(list_type), ValueType::Array) => ListValue::new({
                let item_type = list_type.elem();
                let array = value.as_array().unwrap();
                let mut builder = item_type.create_array_builder(array.len());
                for v in array {
                    let value = self.parse(v, item_type)?;
                    builder.append(value);
                }
                builder.finish()
            })
            .into(),
            // ---- Vector -----
            (DataType::Vector(size), ValueType::Array) => {
                let array = value.as_array().unwrap();
                if array.len() != *size {
                    Err(create_error())?
                }
                let mut elems = Vec::with_capacity(array.len());
                for v in array {
                    let value = match v.value_type() {
                        ValueType::I64 | ValueType::I128 => {
                            let i128_value = v.try_as_i128().map_err(|_| create_error())?;
                            i128_value
                                .to_string()
                                .parse::<f32>()
                                .map_err(|_| create_error())?
                        }
                        ValueType::U64 | ValueType::U128 => {
                            let u128_value = v.try_as_u128().map_err(|_| create_error())?;
                            u128_value
                                .to_string()
                                .parse::<f32>()
                                .map_err(|_| create_error())?
                        }
                        ValueType::F64 => {
                            let f64_value = v.try_as_f64().map_err(|_| create_error())?;
                            if !f64_value.is_finite() {
                                Err(create_error())?
                            }
                            f64_value
                                .to_string()
                                .parse::<f32>()
                                .map_err(|_| create_error())?
                        }
                        _ => Err(create_error())?,
                    };
                    let finite = Finite32::try_from(value).map_err(|_| create_error())?;
                    elems.push(finite);
                }
                VectorVal::from(elems).into()
            }
            // Vector emitted as a string. Reached via the Java `PgVectorToStringConverter`,
            // which downgrades the pgvector column from `DoubleVector` (ARRAY) to STRING so
            // that the Debezium unchanged-TOAST placeholder can flow through without tripping
            // Connect's schema validation. The value is either the pgvector text form
            // `"[a,b,...]"` or the placeholder; the latter is mapped to a sentinel `VectorVal`
            // that the materialize executor recognises and replaces with the old row value.
            (DataType::Vector(size), ValueType::String) => {
                let s = value.as_str().unwrap();
                if self.handle_toast_columns
                    && s.len() == DEBEZIUM_UNAVAILABLE_VALUE.len()
                    && s == DEBEZIUM_UNAVAILABLE_VALUE
                {
                    // Build the sentinel at the declared dimension so it passes
                    // `check_datum_type` in the chunk builder; the materialize
                    // executor recognises it by checking that every element
                    // equals `DEBEZIUM_UNAVAILABLE_VECTOR_ELEM`.
                    debezium_unavailable_vector(*size).into()
                } else {
                    VectorVal::from_text(s, *size)
                        .map_err(|_| create_error())?
                        .into()
                }
            }

            // ---- Bytea -----
            (DataType::Bytea, ValueType::String) => {
                let value_str = value.as_str().unwrap();

                match self.bytea_handling {
                    ByteaHandling::Standard => {
                        let mut buf = Vec::new();
                        str_to_bytea(value_str, &mut buf).map_err(|_| create_error())?;
                        buf.into()
                    }
                    ByteaHandling::Base64 => base64::engine::general_purpose::STANDARD
                        .decode(value_str)
                        .map_err(|_| create_error())?
                        .into_boxed_slice()
                        .into(),
                }
            }
            // Handle Debezium PostGIS geometry and geography types:
            // {"srid": <int>, "wkb": <base64_string>}. We extract the wkb field and
            // decode it as EWKB bytes.
            (DataType::Bytea, ValueType::Object) => {
                match try_parse_debezium_postgis_spatial_as_bytea(value, create_error)? {
                    Some(bytes) => bytes.into(),
                    None => Err(create_error())?,
                }
            }
            // ---- Jsonb -----
            (DataType::Jsonb, ValueType::String)
                if matches!(self.json_value_handling, JsonValueHandling::AsString) =>
            {
                // Check if this value is the Debezium unavailable value (TOAST handling for postgres-cdc).
                // Debezium will base64 encode the bytea type placeholder.
                // When a placeholder is encountered, it is converted into a jsonb format placeholder to match the original type.
                match self.handle_toast_columns {
                    true => JsonbVal::from_debezium_unavailable_value(value.as_str().unwrap())
                        .map_err(|_| create_error())?
                        .into(),
                    false => JsonbVal::from_str(value.as_str().unwrap())
                        .map_err(|_| create_error())?
                        .into(),
                }
            }
            (DataType::Jsonb, _)
                if matches!(self.json_value_handling, JsonValueHandling::AsValue) =>
            {
                let value: serde_json::Value =
                    value.clone().try_into().map_err(|_| create_error())?;
                JsonbVal::from(value).into()
            }
            // ---- Int256 -----
            (
                DataType::Int256,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => Int256::from(value.try_as_i64().map_err(|_| create_error())?).into(),

            (DataType::Int256, ValueType::String) => Int256::from_str(value.as_str().unwrap())
                .map_err(|_| create_error())?
                .into(),

            (_expected, _got) => Err(create_error())?,
        };
        Ok(DatumCow::Owned(Some(v)))
    }
}

/// Try to decode a base64-encoded decimal string for unsigned bigint handling in Precise mode.
///
/// This is used when processing CDC data from upstream systems with unsigned bigint (e.g., MySQL CDC).
/// When users configure `debezium.bigint.unsigned.handling.mode='precise'`, Debezium converts
/// unsigned bigint to base64-encoded decimal.
///
/// Reference: <https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-property-bigint-unsigned-handling-mode>.
fn try_base64_decode_decimal(
    str_val: &str,
    bigint_unsigned_handling: BigintUnsignedHandlingMode,
    create_error: impl Fn() -> AccessError,
) -> Result<Decimal, AccessError> {
    match bigint_unsigned_handling {
        BigintUnsignedHandlingMode::Precise => {
            // A better approach would be to get bytes + org.apache.kafka.connect.data.Decimal from schema
            // instead of string, as described in <https://github.com/risingwavelabs/risingwave/issues/16852>.
            // However, Rust doesn't have a library to parse Kafka Connect metadata, so we'll refactor this
            // after implementing that functionality.
            let value = base64::engine::general_purpose::STANDARD
                .decode(str_val)
                .map_err(|_| create_error())?;
            let unscaled = num_bigint::BigInt::from_signed_bytes_be(&value);
            Decimal::from_str(&unscaled.to_string()).map_err(|_| create_error())
        }
        BigintUnsignedHandlingMode::Long => {
            // In Long mode, don't attempt base64 decoding
            Err(create_error())
        }
    }
}

pub struct JsonAccess<'a> {
    value: BorrowedValue<'a>,
    options: &'a JsonParseOptions,
}

impl<'a> JsonAccess<'a> {
    pub fn new_with_options(value: BorrowedValue<'a>, options: &'a JsonParseOptions) -> Self {
        Self { value, options }
    }

    pub fn new(value: BorrowedValue<'a>) -> Self {
        Self::new_with_options(value, &JsonParseOptions::DEFAULT)
    }
}

impl Access for JsonAccess<'_> {
    fn access<'a>(&'a self, path: &[&str], type_expected: &DataType) -> AccessResult<DatumCow<'a>> {
        let mut value = &self.value;

        for (idx, &key) in path.iter().enumerate() {
            if let Some(sub_value) = if self.options.ignoring_keycase {
                json_object_get_case_insensitive(value, key)
            } else {
                value.get(key)
            } {
                value = sub_value;
            } else {
                Err(AccessError::Undefined {
                    name: key.to_owned(),
                    path: path.iter().take(idx).join("."),
                })?;
            }
        }

        self.options.parse(value, type_expected)
    }
}

/// Get a value from a json object by key, case insensitive.
///
/// Returns `None` if the given json value is not an object, or the key is not found.
fn json_object_get_case_insensitive<'b>(
    v: &'b simd_json::BorrowedValue<'b>,
    key: &str,
) -> Option<&'b simd_json::BorrowedValue<'b>> {
    let obj = v.as_object()?;
    let value = obj.get(key);
    if value.is_some() {
        return value; // fast path
    }
    for (k, v) in obj {
        if k.eq_ignore_ascii_case(key) {
            return Some(v);
        }
    }
    None
}
