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

use postgres_types::{FromSql, Type};
use risingwave_common::types::{
    Date, Interval, JsonbVal, ScalarImpl, Time, Timestamp, Timestamptz,
};

impl<'a> FromSql<'a> for ScalarImpl {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(match *ty {
            Type::BOOL => ScalarImpl::from(bool::from_sql(ty, raw)?),
            Type::INT2 => ScalarImpl::from(i16::from_sql(ty, raw)?),
            Type::INT4 => ScalarImpl::from(i32::from_sql(ty, raw)?),
            Type::INT8 => ScalarImpl::from(i64::from_sql(ty, raw)?),
            Type::FLOAT4 => ScalarImpl::from(f32::from_sql(ty, raw)?),
            Type::FLOAT8 => ScalarImpl::from(f64::from_sql(ty, raw)?),
            Type::DATE => ScalarImpl::from(Date::from_sql(ty, raw)?),
            Type::TIME => ScalarImpl::from(Time::from_sql(ty, raw)?),
            Type::TIMESTAMP => ScalarImpl::from(Timestamp::from_sql(ty, raw)?),
            Type::TIMESTAMPTZ => ScalarImpl::from(Timestamptz::from_sql(ty, raw)?),
            Type::JSON | Type::JSONB => ScalarImpl::from(JsonbVal::from_sql(ty, raw)?),
            Type::INTERVAL => ScalarImpl::from(Interval::from_sql(ty, raw)?),
            Type::BYTEA => ScalarImpl::from(Vec::<u8>::from_sql(ty, raw)?.into_boxed_slice()),
            Type::VARCHAR | Type::TEXT | Type::BPCHAR => {
                ScalarImpl::from(String::from_sql(ty, raw)?)
            }
            ref ty
                if (ty.name() == "citext"
                    || ty.name() == "ltree"
                    || ty.name() == "lquery"
                    || ty.name() == "ltxtquery") =>
            {
                ScalarImpl::from(String::from_sql(ty, raw)?)
            }
            // PostGIS spatial types: read as EWKB bytes.
            ref ty if matches!(ty.name(), "geometry" | "geography") => {
                ScalarImpl::from(Vec::<u8>::from_sql(ty, raw)?.into_boxed_slice())
            }
            // Serial, Int256, Struct, List and Decimal are not supported here
            // Note: The Decimal type is specially handled in the `ScalarAdapter`.
            _ => {
                bail_not_implemented!("the postgres decoding for {ty} is unsupported")
            }
        })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(
            *ty,
            Type::BOOL
                | Type::INT2
                | Type::INT4
                | Type::INT8
                | Type::FLOAT4
                | Type::FLOAT8
                | Type::DATE
                | Type::TIME
                | Type::TIMESTAMP
                | Type::TIMESTAMPTZ
                | Type::JSON
                | Type::JSONB
                | Type::INTERVAL
                | Type::BYTEA
                | Type::VARCHAR
                | Type::TEXT
                | Type::BPCHAR
        ) || (ty.name() == "citext"
            || ty.name() == "ltree"
            || ty.name() == "lquery"
            || ty.name() == "ltxtquery"
            || ty.name() == "geography"
            || ty.name() == "geometry")
    }
}
