// Copyright 2022 RisingWave Labs
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

//! Value encoding is an encoding format which converts the data into a binary form (not
//! memcomparable, i.e., Key encoding).

use std::sync::LazyLock;

use bytes::{Buf, BufMut};
use chrono::{Datelike, Timelike};
use either::{Either, for_both};
use enum_as_inner::EnumAsInner;
use risingwave_pb::data::PbDatum;

use crate::array::ArrayImpl;
use crate::log::LogSuppressor;
use crate::row::Row;
use crate::types::*;

pub mod error;
use error::ValueEncodingError;

use self::column_aware_row_encoding::ColumnAwareSerde;
pub mod column_aware_row_encoding;

pub use crate::row::RowDeserializer as BasicDeserializer;
use crate::vector::{decode_vector_payload, encode_vector_payload};

pub type Result<T> = std::result::Result<T, ValueEncodingError>;

/// The kind of all possible `ValueRowSerde`.
#[derive(EnumAsInner)]
pub enum ValueRowSerdeKind {
    /// For `BasicSerde`, the value is encoded with value-encoding.
    Basic,
    /// For `ColumnAwareSerde`, the value is encoded with column-aware row encoding.
    ColumnAware,
}

/// Part of `ValueRowSerde` that implements `serialize` a `Row` into bytes
pub trait ValueRowSerializer: Clone {
    fn serialize(&self, row: impl Row) -> Vec<u8>;
}

/// Part of `ValueRowSerde` that implements `deserialize` bytes into a `Row`
pub trait ValueRowDeserializer: Clone {
    fn deserialize(&self, encoded_bytes: &[u8]) -> Result<Vec<Datum>>;
}

/// The type-erased `ValueRowSerde`, used for simplifying the code.
#[derive(Clone)]
pub struct EitherSerde(pub Either<BasicSerde, ColumnAwareSerde>);

impl From<BasicSerde> for EitherSerde {
    fn from(value: BasicSerde) -> Self {
        Self(Either::Left(value))
    }
}
impl From<ColumnAwareSerde> for EitherSerde {
    fn from(value: ColumnAwareSerde) -> Self {
        Self(Either::Right(value))
    }
}

impl ValueRowSerializer for EitherSerde {
    fn serialize(&self, row: impl Row) -> Vec<u8> {
        for_both!(&self.0, s => s.serialize(row))
    }
}

impl ValueRowDeserializer for EitherSerde {
    fn deserialize(&self, encoded_bytes: &[u8]) -> Result<Vec<Datum>> {
        for_both!(&self.0, s => s.deserialize(encoded_bytes))
    }
}

/// Wrap of the original `Row` serializing function
#[derive(Clone)]
pub struct BasicSerializer;

impl ValueRowSerializer for BasicSerializer {
    fn serialize(&self, row: impl Row) -> Vec<u8> {
        let mut buf = vec![];
        for datum in row.iter() {
            serialize_datum_into(datum, &mut buf);
        }
        buf
    }
}

impl ValueRowDeserializer for BasicDeserializer {
    fn deserialize(&self, encoded_bytes: &[u8]) -> Result<Vec<Datum>> {
        Ok(self.deserialize(encoded_bytes)?.into_inner().into())
    }
}

/// Wrap of the original `Row` serializing and deserializing function
#[derive(Clone)]
pub struct BasicSerde {
    pub serializer: BasicSerializer,
    pub deserializer: BasicDeserializer,
}

impl ValueRowSerializer for BasicSerde {
    fn serialize(&self, row: impl Row) -> Vec<u8> {
        self.serializer.serialize(row)
    }
}

impl ValueRowDeserializer for BasicSerde {
    fn deserialize(&self, encoded_bytes: &[u8]) -> Result<Vec<Datum>> {
        Ok(self
            .deserializer
            .deserialize(encoded_bytes)?
            .into_inner()
            .into())
    }
}

pub fn try_get_exact_serialize_datum_size(arr: &ArrayImpl) -> Option<usize> {
    match arr {
        ArrayImpl::Int16(_) => Some(2),
        ArrayImpl::Int32(_) => Some(4),
        ArrayImpl::Int64(_) => Some(8),
        ArrayImpl::Serial(_) => Some(8),
        ArrayImpl::Float32(_) => Some(4),
        ArrayImpl::Float64(_) => Some(8),
        ArrayImpl::Bool(_) => Some(1),
        ArrayImpl::Decimal(_) => Some(estimate_serialize_decimal_size()),
        ArrayImpl::Interval(_) => Some(estimate_serialize_interval_size()),
        ArrayImpl::Date(_) => Some(estimate_serialize_date_size()),
        ArrayImpl::Timestamp(_) => Some(estimate_serialize_timestamp_size()),
        ArrayImpl::Time(_) => Some(estimate_serialize_time_size()),
        _ => None,
    }
    .map(|x| x + 1)
}

/// Serialize a datum into bytes and return (Not order guarantee, used in value encoding).
pub fn serialize_datum(cell: impl ToDatumRef) -> Vec<u8> {
    let mut buf: Vec<u8> = vec![];
    serialize_datum_into(cell, &mut buf);
    buf
}

/// Serialize a datum into bytes (Not order guarantee, used in value encoding).
pub fn serialize_datum_into(datum_ref: impl ToDatumRef, buf: &mut impl BufMut) {
    if let Some(d) = datum_ref.to_datum_ref() {
        buf.put_u8(1);
        serialize_scalar(d, buf)
    } else {
        buf.put_u8(0);
    }
}

pub fn estimate_serialize_datum_size(datum_ref: impl ToDatumRef) -> usize {
    if let Some(d) = datum_ref.to_datum_ref() {
        1 + estimate_serialize_scalar_size(d)
    } else {
        1
    }
}

#[easy_ext::ext(DatumFromProtoExt)]
impl Datum {
    /// Create a datum from the protobuf representation with the given data type.
    pub fn from_protobuf(proto: &PbDatum, data_type: &DataType) -> Result<Datum> {
        deserialize_datum(proto.body.as_slice(), data_type)
    }
}

#[easy_ext::ext(DatumToProtoExt)]
impl<D: ToDatumRef> D {
    /// Convert the datum to the protobuf representation.
    pub fn to_protobuf(&self) -> PbDatum {
        PbDatum {
            body: serialize_datum(self),
        }
    }
}

/// Deserialize bytes into a datum (Not order guarantee, used in value encoding).
pub fn deserialize_datum(mut data: impl Buf, ty: &DataType) -> Result<Datum> {
    inner_deserialize_datum(&mut data, ty)
}

// prevent recursive use of &mut
#[inline(always)]
fn inner_deserialize_datum(data: &mut impl Buf, ty: &DataType) -> Result<Datum> {
    let null_tag = data.get_u8();
    match null_tag {
        0 => Ok(None),
        1 => Some(deserialize_value(ty, data)).transpose(),
        _ => Err(ValueEncodingError::InvalidTagEncoding(null_tag)),
    }
}

fn serialize_scalar(value: ScalarRefImpl<'_>, buf: &mut impl BufMut) {
    match value {
        ScalarRefImpl::Int16(v) => buf.put_i16_le(v),
        ScalarRefImpl::Int32(v) => buf.put_i32_le(v),
        ScalarRefImpl::Int64(v) => buf.put_i64_le(v),
        ScalarRefImpl::Int256(v) => buf.put_slice(&v.to_le_bytes()),
        ScalarRefImpl::Serial(v) => buf.put_i64_le(v.into_inner()),
        ScalarRefImpl::Float32(v) => buf.put_f32_le(v.into_inner()),
        ScalarRefImpl::Float64(v) => buf.put_f64_le(v.into_inner()),
        ScalarRefImpl::Utf8(v) => serialize_str(v.as_bytes(), buf),
        ScalarRefImpl::Bytea(v) => serialize_str(v, buf),
        ScalarRefImpl::Bool(v) => buf.put_u8(v as u8),
        ScalarRefImpl::Decimal(v) => serialize_decimal(&v, buf),
        ScalarRefImpl::Interval(v) => serialize_interval(&v, buf),
        ScalarRefImpl::Date(v) => serialize_date(v.0.num_days_from_ce(), buf),
        ScalarRefImpl::Timestamp(v) => serialize_timestamp(
            v.0.and_utc().timestamp(),
            v.0.and_utc().timestamp_subsec_nanos(),
            buf,
        ),
        ScalarRefImpl::Timestamptz(v) => buf.put_i64_le(v.timestamp_micros()),
        ScalarRefImpl::Time(v) => {
            serialize_time(v.0.num_seconds_from_midnight(), v.0.nanosecond(), buf)
        }
        ScalarRefImpl::Jsonb(v) => serialize_str(&v.value_serialize(), buf),
        ScalarRefImpl::Variant(v) => serialize_str(v.as_bytes(), buf),
        ScalarRefImpl::Struct(s) => serialize_struct(s, buf),
        ScalarRefImpl::List(v) => serialize_list(v, buf),
        ScalarRefImpl::Map(m) => serialize_list(m.into_inner(), buf),
        ScalarRefImpl::Vector(v) => serialize_vector(v, buf),
    }
}

fn estimate_serialize_scalar_size(value: ScalarRefImpl<'_>) -> usize {
    match value {
        ScalarRefImpl::Int16(_) => 2,
        ScalarRefImpl::Int32(_) => 4,
        ScalarRefImpl::Int64(_) => 8,
        ScalarRefImpl::Int256(_) => 32,
        ScalarRefImpl::Serial(_) => 8,
        ScalarRefImpl::Float32(_) => 4,
        ScalarRefImpl::Float64(_) => 8,
        ScalarRefImpl::Utf8(v) => estimate_serialize_str_size(v.as_bytes()),
        ScalarRefImpl::Bytea(v) => estimate_serialize_str_size(v),
        ScalarRefImpl::Bool(_) => 1,
        ScalarRefImpl::Decimal(_) => estimate_serialize_decimal_size(),
        ScalarRefImpl::Interval(_) => estimate_serialize_interval_size(),
        ScalarRefImpl::Date(_) => estimate_serialize_date_size(),
        ScalarRefImpl::Timestamp(_) => estimate_serialize_timestamp_size(),
        ScalarRefImpl::Timestamptz(_) => 8,
        ScalarRefImpl::Time(_) => estimate_serialize_time_size(),
        // not exact as we use internal encoding size to estimate the json string size
        ScalarRefImpl::Jsonb(v) => v.capacity(),
        ScalarRefImpl::Variant(v) => estimate_serialize_str_size(v.as_bytes()),
        ScalarRefImpl::Struct(s) => estimate_serialize_struct_size(s),
        ScalarRefImpl::List(v) => estimate_serialize_list_size(v),
        ScalarRefImpl::Map(v) => estimate_serialize_list_size(v.into_inner()),
        ScalarRefImpl::Vector(v) => estimate_serialize_vector_size(v),
    }
}

fn serialize_struct(value: StructRef<'_>, buf: &mut impl BufMut) {
    value.iter_fields_ref().for_each(|field_value| {
        serialize_datum_into(field_value, buf);
    });
}

fn estimate_serialize_struct_size(s: StructRef<'_>) -> usize {
    s.estimate_serialize_size_inner()
}
fn serialize_list(value: ListRef<'_>, buf: &mut impl BufMut) {
    let elems = value.iter();
    buf.put_u32_le(elems.len() as u32);

    elems.for_each(|field_value| {
        serialize_datum_into(field_value, buf);
    });
}
fn estimate_serialize_list_size(list: ListRef<'_>) -> usize {
    4 + list.estimate_serialize_size_inner()
}

fn serialize_vector(value: VectorRef<'_>, buf: &mut impl BufMut) {
    let elems = value.as_slice();
    encode_vector_payload(elems, buf);
}
fn estimate_serialize_vector_size(v: VectorRef<'_>) -> usize {
    size_of_val(v.as_slice())
}

fn serialize_str(bytes: &[u8], buf: &mut impl BufMut) {
    buf.put_u32_le(bytes.len() as u32);
    buf.put_slice(bytes);
}

fn estimate_serialize_str_size(bytes: &[u8]) -> usize {
    4 + bytes.len()
}

fn serialize_interval(interval: &Interval, buf: &mut impl BufMut) {
    buf.put_i32_le(interval.months());
    buf.put_i32_le(interval.days());
    buf.put_i64_le(interval.usecs());
}

fn estimate_serialize_interval_size() -> usize {
    4 + 4 + 8
}

fn serialize_date(days: i32, buf: &mut impl BufMut) {
    buf.put_i32_le(days);
}

fn estimate_serialize_date_size() -> usize {
    4
}

fn serialize_timestamp(secs: i64, nsecs: u32, buf: &mut impl BufMut) {
    buf.put_i64_le(secs);
    buf.put_u32_le(nsecs);
}

fn estimate_serialize_timestamp_size() -> usize {
    8 + 4
}

fn serialize_time(secs: u32, nano: u32, buf: &mut impl BufMut) {
    buf.put_u32_le(secs);
    buf.put_u32_le(nano);
}

fn estimate_serialize_time_size() -> usize {
    4 + 4
}

fn serialize_decimal(decimal: &Decimal, buf: &mut impl BufMut) {
    buf.put_slice(&decimal.unordered_serialize());
}

fn estimate_serialize_decimal_size() -> usize {
    16
}

fn deserialize_value(ty: &DataType, data: &mut impl Buf) -> Result<ScalarImpl> {
    Ok(match ty {
        DataType::Int16 => ScalarImpl::Int16(data.get_i16_le()),
        DataType::Int32 => ScalarImpl::Int32(data.get_i32_le()),
        DataType::Int64 => ScalarImpl::Int64(data.get_i64_le()),
        DataType::Int256 => ScalarImpl::Int256(deserialize_int256(data)),
        DataType::Serial => ScalarImpl::Serial(Serial::from(data.get_i64_le())),
        DataType::Float32 => ScalarImpl::Float32(F32::from(data.get_f32_le())),
        DataType::Float64 => ScalarImpl::Float64(F64::from(data.get_f64_le())),
        DataType::Varchar => ScalarImpl::Utf8(deserialize_str(data)?),
        DataType::Boolean => ScalarImpl::Bool(deserialize_bool(data)?),
        DataType::Decimal => ScalarImpl::Decimal(deserialize_decimal(data)?),
        DataType::Interval => ScalarImpl::Interval(deserialize_interval(data)?),
        DataType::Time => ScalarImpl::Time(deserialize_time(data)?),
        DataType::Timestamp => ScalarImpl::Timestamp(deserialize_timestamp(data)?),
        DataType::Timestamptz => {
            let micros = data.get_i64_le();
            let tz = Timestamptz::from_micros(micros).unwrap_or_else(|| {
                // Values written before validation existed may be out of range (#26397). Keep
                // them readable and report, so the row can still be read and deleted.
                static LOG_SUPPRESSOR: LazyLock<LogSuppressor> =
                    LazyLock::new(LogSuppressor::default);
                if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                    tracing::error!(suppressed_count, micros, "decoded out-of-range timestamptz");
                }
                Timestamptz::from_micros_uncheck(micros)
            });
            ScalarImpl::Timestamptz(tz)
        }
        DataType::Date => ScalarImpl::Date(deserialize_date(data)?),
        DataType::Jsonb => ScalarImpl::Jsonb(
            JsonbVal::value_deserialize(&deserialize_bytea(data))
                .ok_or(ValueEncodingError::InvalidJsonbEncoding)?,
        ),
        DataType::Variant => ScalarImpl::Variant(
            VariantVal::value_deserialize(&deserialize_bytea(data))
                .ok_or(ValueEncodingError::InvalidVariantEncoding)?,
        ),
        DataType::Struct(struct_def) => deserialize_struct(struct_def, data)?,
        DataType::Bytea => ScalarImpl::Bytea(deserialize_bytea(data).into()),
        DataType::Vector(dimension) => deserialize_vector(*dimension, data),
        DataType::List(list_type) => deserialize_list(list_type, data)?,
        DataType::Map(map_type) => deserialize_map(map_type, data)?,
    })
}

fn deserialize_struct(struct_def: &StructType, data: &mut impl Buf) -> Result<ScalarImpl> {
    let mut field_values = Vec::with_capacity(struct_def.len());
    for field_type in struct_def.types() {
        field_values.push(inner_deserialize_datum(data, field_type)?);
    }

    Ok(ScalarImpl::Struct(StructValue::new(field_values)))
}

fn deserialize_list(list_type: &ListType, data: &mut impl Buf) -> Result<ScalarImpl> {
    let elem_type = list_type.elem();
    let len = data.get_u32_le();
    let mut builder = elem_type.create_array_builder(len as usize);
    for _ in 0..len {
        builder.append(inner_deserialize_datum(data, elem_type)?);
    }
    Ok(ScalarImpl::List(ListValue::new(builder.finish())))
}

fn deserialize_map(map_type: &MapType, data: &mut impl Buf) -> Result<ScalarImpl> {
    // FIXME: clone type everytime here is inefficient
    let list = deserialize_list(&map_type.clone().into_list_type(), data)?.into_list();
    Ok(ScalarImpl::Map(MapValue::from_entries(list)))
}

fn deserialize_vector(dimension: usize, data: &mut impl Buf) -> ScalarImpl {
    VectorVal {
        inner: decode_vector_payload(dimension, data).into_boxed_slice(),
    }
    .to_scalar_value()
}

fn deserialize_str(data: &mut impl Buf) -> Result<Box<str>> {
    let len = data.get_u32_le();
    let mut bytes = vec![0; len as usize];
    data.copy_to_slice(&mut bytes);
    String::from_utf8(bytes)
        .map(String::into_boxed_str)
        .map_err(ValueEncodingError::InvalidUtf8)
}

fn deserialize_bytea(data: &mut impl Buf) -> Vec<u8> {
    let len = data.get_u32_le();
    let mut bytes = vec![0; len as usize];
    data.copy_to_slice(&mut bytes);
    bytes
}

fn deserialize_int256(data: &mut impl Buf) -> Int256 {
    let mut bytes = [0; Int256::size()];
    data.copy_to_slice(&mut bytes);
    Int256::from_le_bytes(bytes)
}

fn deserialize_bool(data: &mut impl Buf) -> Result<bool> {
    match data.get_u8() {
        1 => Ok(true),
        0 => Ok(false),
        value => Err(ValueEncodingError::InvalidBoolEncoding(value)),
    }
}

fn deserialize_interval(data: &mut impl Buf) -> Result<Interval> {
    let months = data.get_i32_le();
    let days = data.get_i32_le();
    let usecs = data.get_i64_le();
    Ok(Interval::from_month_day_usec(months, days, usecs))
}

fn deserialize_time(data: &mut impl Buf) -> Result<Time> {
    let secs = data.get_u32_le();
    let nano = data.get_u32_le();
    Time::with_secs_nano(secs, nano)
        .map_err(|_e| ValueEncodingError::InvalidTimeEncoding(secs, nano))
}

fn deserialize_timestamp(data: &mut impl Buf) -> Result<Timestamp> {
    let secs = data.get_i64_le();
    let nsecs = data.get_u32_le();
    Timestamp::with_secs_nsecs(secs, nsecs)
        .map_err(|_e| ValueEncodingError::InvalidTimestampEncoding(secs, nsecs))
}

fn deserialize_date(data: &mut impl Buf) -> Result<Date> {
    let days = data.get_i32_le();
    Date::with_days_since_ce(days).map_err(|_e| ValueEncodingError::InvalidDateEncoding(days))
}

fn deserialize_decimal(data: &mut impl Buf) -> Result<Decimal> {
    let mut bytes = [0; 16];
    data.copy_to_slice(&mut bytes);
    Ok(Decimal::unordered_deserialize(bytes))
}

#[cfg(test)]
mod tests {
    use crate::array::{ArrayImpl, ListValue, StructValue};
    use crate::test_utils::rand_chunk;
    use crate::types::{
        DataType, Date, Datum, Decimal, Interval, ScalarImpl, Serial, Time, Timestamp,
    };
    use crate::util::value_encoding::{
        estimate_serialize_datum_size, serialize_datum, try_get_exact_serialize_datum_size,
    };

    fn test_estimate_serialize_scalar_size(s: ScalarImpl) {
        let d = Datum::from(s);
        assert_eq!(estimate_serialize_datum_size(&d), serialize_datum(&d).len());
    }

    fn test_try_get_exact_serialize_datum_size(s: &ArrayImpl) {
        let d = s.to_datum();
        if let Some(ret) = try_get_exact_serialize_datum_size(s) {
            assert_eq!(ret, serialize_datum(&d).len());
        }
    }

    #[test]
    fn test_estimate_size() {
        let d: Datum = None;
        assert_eq!(estimate_serialize_datum_size(&d), serialize_datum(&d).len());

        test_estimate_serialize_scalar_size(ScalarImpl::Bool(true));
        test_estimate_serialize_scalar_size(ScalarImpl::Int16(1));
        test_estimate_serialize_scalar_size(ScalarImpl::Int32(1));
        test_estimate_serialize_scalar_size(ScalarImpl::Int64(1));
        test_estimate_serialize_scalar_size(ScalarImpl::Float32(1.0.into()));
        test_estimate_serialize_scalar_size(ScalarImpl::Float64(1.0.into()));
        test_estimate_serialize_scalar_size(ScalarImpl::Serial(Serial::from(i64::MIN)));

        test_estimate_serialize_scalar_size(ScalarImpl::Utf8("abc".into()));
        test_estimate_serialize_scalar_size(ScalarImpl::Utf8("".into()));
        test_estimate_serialize_scalar_size(ScalarImpl::Decimal(Decimal::NegativeInf));
        test_estimate_serialize_scalar_size(ScalarImpl::Decimal(Decimal::PositiveInf));
        test_estimate_serialize_scalar_size(ScalarImpl::Decimal(Decimal::NaN));
        test_estimate_serialize_scalar_size(ScalarImpl::Decimal(123123.into()));
        test_estimate_serialize_scalar_size(ScalarImpl::Interval(Interval::from_month_day_usec(
            7, 8, 9,
        )));
        test_estimate_serialize_scalar_size(ScalarImpl::Date(Date::from_ymd_uncheck(2333, 3, 3)));
        test_estimate_serialize_scalar_size(ScalarImpl::Bytea("\\x233".as_bytes().into()));
        test_estimate_serialize_scalar_size(ScalarImpl::Time(Time::from_hms_uncheck(2, 3, 3)));
        test_estimate_serialize_scalar_size(ScalarImpl::Timestamp(
            Timestamp::from_timestamp_uncheck(23333333, 2333),
        ));
        test_estimate_serialize_scalar_size(ScalarImpl::Interval(Interval::from_month_day_usec(
            2, 3, 3333,
        )));
        test_estimate_serialize_scalar_size(ScalarImpl::Struct(StructValue::new(vec![
            ScalarImpl::Int64(233).into(),
            ScalarImpl::Float64(23.33.into()).into(),
        ])));
        test_estimate_serialize_scalar_size(ScalarImpl::List(ListValue::from_iter([233i64, 2333])));
    }

    #[test]
    fn test_try_estimate_size() {
        let chunk = rand_chunk::gen_chunk(
            &[
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
                DataType::Serial,
                DataType::Float32,
                DataType::Float64,
                DataType::Boolean,
                DataType::Decimal,
                DataType::Interval,
                DataType::Time,
                DataType::Timestamp,
                DataType::Date,
            ],
            1,
            0,
            0.0,
        );
        for column in chunk.columns() {
            test_try_get_exact_serialize_datum_size(column);
        }
    }
}
