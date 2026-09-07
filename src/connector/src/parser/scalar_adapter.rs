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

use std::str::FromStr;

use anyhow::anyhow;
use bytes::{Buf, BufMut, BytesMut};
use pg_bigdecimal::PgNumeric;
use risingwave_common::types::{
    DataType, Decimal, Int256, ListValue, ScalarImpl, ScalarRefImpl, StructValue,
};
use thiserror_ext::AsReport;
use tokio_postgres::types::{FromSql, IsNull, Kind, ToSql, Type, to_sql_checked};

use crate::connector_common::postgres::postgres_point_type;
use crate::error::ConnectorResult;

#[derive(Clone, Debug)]
pub struct EnumString(pub String);

impl<'a> FromSql<'a> for EnumString {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Sync + Send>> {
        Ok(EnumString(String::from_utf8_lossy(raw).into_owned()))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.kind(), Kind::Enum(_))
    }
}

impl ToSql for EnumString {
    to_sql_checked!();

    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match ty.kind() {
            Kind::Enum(e) => {
                if e.contains(&self.0) {
                    out.extend_from_slice(self.0.as_bytes());
                    Ok(IsNull::No)
                } else {
                    Err(format!(
                        "EnumString value {} is not in the enum type {:?}",
                        self.0, e
                    )
                    .into())
                }
            }
            _ => Err("EnumString can only be used with ENUM types".into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.kind(), Kind::Enum(_))
    }
}

#[derive(Debug)]
pub(crate) struct PgPoint {
    x: f64,
    y: f64,
}

impl<'a> FromSql<'a> for PgPoint {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() != 2 * std::mem::size_of::<f64>() {
            return Err("invalid point binary payload length".into());
        }

        let mut buf = raw;

        Ok(Self {
            x: buf.get_f64(),
            y: buf.get_f64(),
        })
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::POINT
    }
}

impl ToSql for PgPoint {
    to_sql_checked!();

    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.put_f64(self.x);
        out.put_f64(self.y);

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::POINT
    }
}

/// Adapter for `ScalarImpl` to Postgres data type,
/// which can be used to encode/decode to/from Postgres value.
#[derive(Debug)]
pub(crate) enum ScalarAdapter {
    Builtin(ScalarImpl),
    Uuid(uuid::Uuid),
    Point(PgPoint),
    // Currently in order to handle the decimal beyond RustDecimal,
    // we use the PgNumeric type to convert the decimal to a string/decimal/rw_int256.
    Numeric(PgNumeric),
    Enum(EnumString),
    NumericList(Vec<Option<PgNumeric>>),
    EnumList(Vec<Option<EnumString>>),
    // UuidList is covered by List, while NumericList and EnumList are special cases.
    // Note: The IntervalList is not supported.
    List(Vec<Option<ScalarAdapter>>),
}

impl ToSql for ScalarAdapter {
    to_sql_checked!();

    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            ScalarAdapter::Builtin(v) => v.to_sql(ty, out),
            ScalarAdapter::Uuid(v) => v.to_sql(ty, out),
            ScalarAdapter::Point(v) => v.to_sql(ty, out),
            ScalarAdapter::Numeric(v) => v.to_sql(ty, out),
            ScalarAdapter::Enum(v) => v.to_sql(ty, out),
            ScalarAdapter::NumericList(v) => v.to_sql(ty, out),
            ScalarAdapter::EnumList(v) => v.to_sql(ty, out),
            ScalarAdapter::List(v) => v.to_sql(ty, out),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

/// convert from Postgres uuid, numeric and enum to `ScalarAdapter`
impl<'a> FromSql<'a> for ScalarAdapter {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        match ty.kind() {
            Kind::Simple => match *ty {
                Type::UUID => Ok(ScalarAdapter::Uuid(uuid::Uuid::from_sql(ty, raw)?)),
                Type::POINT => Ok(ScalarAdapter::Point(PgPoint::from_sql(ty, raw)?)),
                // In order to cover the decimal beyond RustDecimal(only 28 digits are supported),
                // we use the PgNumeric to handle decimal from postgres.
                Type::NUMERIC => Ok(ScalarAdapter::Numeric(PgNumeric::from_sql(ty, raw)?)),
                _ => Ok(ScalarAdapter::Builtin(ScalarImpl::from_sql(ty, raw)?)),
            },
            Kind::Enum(_) => Ok(ScalarAdapter::Enum(EnumString::from_sql(ty, raw)?)),
            Kind::Array(Type::NUMERIC) => {
                Ok(ScalarAdapter::NumericList(FromSql::from_sql(ty, raw)?))
            }
            Kind::Array(inner_type) if let Kind::Enum(_) = inner_type.kind() => {
                Ok(ScalarAdapter::EnumList(FromSql::from_sql(ty, raw)?))
            }
            Kind::Array(_) => Ok(ScalarAdapter::List(FromSql::from_sql(ty, raw)?)),
            _ => Err(anyhow!("failed to convert type {:?} to ScalarAdapter", ty).into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        match ty.kind() {
            Kind::Simple => {
                matches!(ty, &Type::UUID | &Type::NUMERIC | &Type::POINT)
                    || <ScalarImpl as FromSql>::accepts(ty)
            }
            Kind::Enum(_) => true,
            Kind::Array(inner_type) => <ScalarAdapter as FromSql>::accepts(inner_type),
            _ => false,
        }
    }
}

impl ScalarAdapter {
    pub fn name(&self) -> &'static str {
        match self {
            ScalarAdapter::Builtin(_) => "Builtin",
            ScalarAdapter::Uuid(_) => "Uuid",
            ScalarAdapter::Point(_) => "Point",
            ScalarAdapter::Numeric(_) => "Numeric",
            ScalarAdapter::Enum(_) => "Enum",
            ScalarAdapter::EnumList(_) => "EnumList",
            ScalarAdapter::NumericList(_) => "NumericList",
            ScalarAdapter::List(_) => "List",
        }
    }

    /// convert `ScalarRefImpl` to `ScalarAdapter` so that we can correctly encode to postgres value
    pub(crate) fn from_scalar(
        scalar: ScalarRefImpl<'_>,
        ty: &Type,
    ) -> ConnectorResult<ScalarAdapter> {
        Ok(match (scalar, ty, ty.kind()) {
            (ScalarRefImpl::Utf8(s), &Type::UUID, _) => ScalarAdapter::Uuid(s.parse()?),
            (ScalarRefImpl::Struct(point), &Type::POINT, _) => {
                let mut fields = point.iter_fields_ref();

                let (
                    Some(Some(ScalarRefImpl::Float64(x))),
                    Some(Some(ScalarRefImpl::Float64(y))),
                    None,
                ) = (fields.next(), fields.next(), fields.next())
                else {
                    return Err(anyhow!(
                        "failed to convert struct to PostgreSQL point: expected exactly two non-null float64 fields"
                    )
                    .into());
                };

                ScalarAdapter::Point(PgPoint {
                    x: x.into_inner(),
                    y: y.into_inner(),
                })
            }
            (ScalarRefImpl::Utf8(s), &Type::NUMERIC, _) => {
                ScalarAdapter::Numeric(string_to_pg_numeric(s))
            }
            (ScalarRefImpl::Int256(s), &Type::NUMERIC, _) => {
                ScalarAdapter::Numeric(string_to_pg_numeric(&s.to_string()))
            }
            (ScalarRefImpl::Utf8(s), _, Kind::Enum(_)) => {
                ScalarAdapter::Enum(EnumString(s.to_owned()))
            }
            (ScalarRefImpl::List(list), &Type::NUMERIC_ARRAY, _) => {
                let mut vec = vec![];
                for datum in list.iter() {
                    vec.push(match datum {
                        Some(ScalarRefImpl::Int256(s)) => Some(string_to_pg_numeric(&s.to_string())),
                        Some(ScalarRefImpl::Decimal(s)) => Some(rw_numeric_to_pg_numeric(s)),
                        Some(ScalarRefImpl::Utf8(s)) => Some(string_to_pg_numeric(s)),
                        None => None,
                        _ => {
                            unreachable!("Only rw-numeric[], rw_int256[] and varchar[] are supported to convert to pg-numeric[]");
                        }
                    })
                }
                ScalarAdapter::NumericList(vec)
            }
            (ScalarRefImpl::List(list), _, Kind::Array(inner_type)) => match inner_type.kind() {
                Kind::Enum(_) => {
                    let mut vec = vec![];
                    for datum in list.iter() {
                        vec.push(match datum {
                            Some(ScalarRefImpl::Utf8(s)) => Some(EnumString(s.to_owned())),
                            _ => unreachable!(
                                "Only non-null varchar[] is supported to convert to enum[]"
                            ),
                        })
                    }
                    ScalarAdapter::EnumList(vec)
                }
                _ => {
                    let mut vec = vec![];
                    for datum in list.iter() {
                        vec.push(
                            datum
                                .map(|s| ScalarAdapter::from_scalar(s, inner_type))
                                .transpose()?,
                        );
                    }
                    ScalarAdapter::List(vec)
                }
            },
            _ => ScalarAdapter::Builtin(scalar.into_scalar_impl()),
        })
    }

    pub fn into_scalar(self, ty: &DataType) -> Option<ScalarImpl> {
        match (self, &ty) {
            (ScalarAdapter::Builtin(scalar), _) => Some(scalar),
            (ScalarAdapter::Uuid(uuid), &DataType::Varchar) => {
                Some(ScalarImpl::from(uuid.to_string()))
            }
            (ScalarAdapter::Point(PgPoint { x, y }), &DataType::Struct(_)) => {
                assert_eq!(
                    ty,
                    &postgres_point_type(),
                    "PostgreSQL point must map to struct<x float64, y float64>"
                );

                Some(StructValue::new(vec![Some(x.into()), Some(y.into())]).into())
            }
            (ScalarAdapter::Numeric(numeric), &DataType::Varchar) => {
                Some(ScalarImpl::from(pg_numeric_to_string(&numeric)))
            }
            (ScalarAdapter::Numeric(numeric), &DataType::Int256) => {
                pg_numeric_to_rw_int256(&numeric)
            }
            (ScalarAdapter::Numeric(numeric), &DataType::Decimal) => {
                pg_numeric_to_rw_numeric(&numeric)
            }
            (ScalarAdapter::Enum(EnumString(s)), &DataType::Varchar) => Some(ScalarImpl::from(s)),
            (ScalarAdapter::NumericList(vec), &DataType::List(list)) => {
                let elem = list.elem();
                let mut builder = elem.create_array_builder(0);
                for val in vec {
                    let scalar = match (val, &elem) {
                        // A numeric array contains special values like NaN, Inf, -Inf, which are not supported in Debezium,
                        // when we encounter these special values, we fallback the array to NULL, returning None directly.
                        (Some(numeric), DataType::Varchar) => {
                            if pg_numeric_is_special(&numeric) {
                                return None;
                            } else {
                                ScalarAdapter::Numeric(numeric).into_scalar(elem)
                            }
                        }
                        (Some(numeric), DataType::Int256 | DataType::Decimal) => {
                            if pg_numeric_is_special(&numeric) {
                                return None;
                            } else {
                                // A PgNumeric can sometimes exceeds the range of Int256 and RwNumeric.
                                // In our json parsing, we fallback the array to NULL in this case.
                                // Here we keep the behavior consistent and return None directly.
                                match ScalarAdapter::Numeric(numeric).into_scalar(elem) {
                                    Some(scalar) => Some(scalar),
                                    None => {
                                        return None;
                                    }
                                }
                            }
                        }
                        (Some(_), _) => unreachable!(
                            "Only rw-numeric[], rw_int256[] and varchar[] are supported to convert to pg-numeric[]"
                        ),
                        // This item is NULL, continue to handle next item.
                        (None, _) => None,
                    };
                    builder.append(scalar);
                }
                Some(ScalarImpl::from(ListValue::new(builder.finish())))
            }
            (ScalarAdapter::EnumList(vec), &DataType::List(list)) => {
                let mut builder = list.elem().create_array_builder(0);
                for val in vec {
                    match val {
                        Some(EnumString(s)) => {
                            builder.append(Some(ScalarImpl::from(s)));
                        }
                        None => {
                            return None;
                        }
                    }
                }
                Some(ScalarImpl::from(ListValue::new(builder.finish())))
            }
            (ScalarAdapter::List(vec), &DataType::List(list)) => {
                let elem = list.elem();
                // Due to https://github.com/risingwavelabs/risingwave/issues/16882, INTERVAL_ARRAY is not supported in Debezium, so we keep backfilling and CDC consistent.
                if matches!(elem, DataType::Interval) {
                    return None;
                }
                let mut builder = elem.create_array_builder(0);
                for val in vec {
                    builder.append(val.and_then(|v| v.into_scalar(elem)));
                }
                Some(ScalarImpl::from(ListValue::new(builder.finish())))
            }
            (scaler, ty) => {
                tracing::error!(
                    adapter = scaler.name(),
                    rw_type = ty.pg_name(),
                    "failed to convert from ScalarAdapter: invalid conversion"
                );
                None
            }
        }
    }
}

pub fn validate_pg_type_to_rw_type(pg_type: &DataType, rw_type: &DataType) -> bool {
    if pg_type == rw_type {
        return true;
    }
    match rw_type {
        DataType::Varchar => matches!(pg_type, DataType::Decimal | DataType::Int256),
        DataType::List(list) if list.elem() == &DataType::Varchar => {
            matches!(
                pg_type,
                DataType::List(list) if matches!(list.elem(), DataType::Decimal | DataType::Int256)
            )
        }
        _ => false,
    }
}

fn pg_numeric_is_special(val: &PgNumeric) -> bool {
    matches!(
        val,
        PgNumeric::NegativeInf | PgNumeric::PositiveInf | PgNumeric::NaN
    )
}

fn pg_numeric_to_rw_int256(val: &PgNumeric) -> Option<ScalarImpl> {
    match Int256::from_str(pg_numeric_to_string(val).as_str()) {
        Ok(num) => Some(ScalarImpl::from(num)),
        Err(err) => {
            tracing::error!(error = %err.as_report(), "failed to convert PgNumeric to Int256");
            None
        }
    }
}

fn pg_numeric_to_rw_numeric(val: &PgNumeric) -> Option<ScalarImpl> {
    match val {
        PgNumeric::NegativeInf => Some(ScalarImpl::from(Decimal::NegativeInf)),
        PgNumeric::Normalized(big_decimal) => {
            match Decimal::from_str(big_decimal.to_string().as_str()) {
                Ok(num) => Some(ScalarImpl::from(num)),
                Err(err) => {
                    tracing::error!(error = %err.as_report(), "parse pg-numeric as rw-numeric failed (likely out-of-range");
                    None
                }
            }
        }
        PgNumeric::PositiveInf => Some(ScalarImpl::from(Decimal::PositiveInf)),
        PgNumeric::NaN => Some(ScalarImpl::from(Decimal::NaN)),
    }
}

fn pg_numeric_to_string(val: &PgNumeric) -> String {
    // TODO(kexiang): NEGATIVE_INFINITY -> -Infinity, POSITIVE_INFINITY -> Infinity, NAN -> NaN
    // The current implementation is to ensure consistency with the behavior of cdc event parsor.
    match val {
        PgNumeric::NegativeInf => String::from("NEGATIVE_INFINITY"),
        PgNumeric::Normalized(big_decimal) => big_decimal.to_string(),
        PgNumeric::PositiveInf => String::from("POSITIVE_INFINITY"),
        PgNumeric::NaN => String::from("NAN"),
    }
}

fn string_to_pg_numeric(s: &str) -> PgNumeric {
    match s {
        "NEGATIVE_INFINITY" => PgNumeric::NegativeInf,
        "POSITIVE_INFINITY" => PgNumeric::PositiveInf,
        "NAN" => PgNumeric::NaN,
        _ => PgNumeric::Normalized(s.parse().unwrap()),
    }
}

fn rw_numeric_to_pg_numeric(val: Decimal) -> PgNumeric {
    match val {
        Decimal::NegativeInf => PgNumeric::NegativeInf,
        Decimal::Normalized(inner) => PgNumeric::Normalized(inner.to_string().parse().unwrap()),
        Decimal::PositiveInf => PgNumeric::PositiveInf,
        Decimal::NaN => PgNumeric::NaN,
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::{ScalarImpl, StructValue};
    use tokio_postgres::types::{FromSql, Type};

    use super::ScalarAdapter;
    use crate::connector_common::postgres::postgres_point_type;

    #[test]
    fn test_postgres_point_into_scalar() {
        let mut raw = vec![];
        raw.extend_from_slice(&16777217.25f64.to_be_bytes());
        raw.extend_from_slice(&(-0.987654321098765f64).to_be_bytes());

        let adapter = ScalarAdapter::from_sql(&Type::POINT, &raw).unwrap();
        assert_eq!(
            adapter.into_scalar(&postgres_point_type()),
            Some(ScalarImpl::Struct(StructValue::new(vec![
                Some(ScalarImpl::Float64(16777217.25.into())),
                Some(ScalarImpl::Float64((-0.987654321098765).into())),
            ])))
        );
    }
}
