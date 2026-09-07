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

use bytes::{BufMut, Bytes, BytesMut};
use postgres_types::{ToSql, Type};
use rw_iter_util::ZipEqFast;

use super::{
    DataType, Date, Decimal, F32, F64, Interval, ScalarRefImpl, Serial, Time, Timestamp,
    Timestamptz,
};
use crate::array::{ListRef, StructRef};
use crate::error::NotImplemented;

/// Error type for [`ToBinary`] trait.
#[derive(thiserror::Error, Debug)]
pub enum ToBinaryError {
    #[error(transparent)]
    ToSql(Box<dyn std::error::Error + Send + Sync>),

    #[error(transparent)]
    NotImplemented(#[from] NotImplemented),
}

pub type Result<T> = std::result::Result<T, ToBinaryError>;

/// Converts `ScalarRef` to pgwire "BINARY" format.
///
/// [`postgres_types::ToSql`] has similar functionality, and most of our types implement
/// that trait and forward `ToBinary` to it directly.
pub trait ToBinary {
    fn to_binary_with_type(&self, ty: &DataType) -> Result<Bytes>;
}
macro_rules! implement_using_to_sql {
    ($({ $scalar_type:ty, $data_type:ident, $accessor:expr } ),* $(,)?) => {
        $(
            impl ToBinary for $scalar_type {
                fn to_binary_with_type(&self, ty: &DataType) -> Result<Bytes> {
                    match ty {
                        DataType::$data_type => {
                            let mut output = BytesMut::new();

                            $accessor(self).to_sql(&Type::ANY, &mut output).map_err(ToBinaryError::ToSql)?;
                            Ok(output.freeze())
                        },
                        _ => unreachable!(),
                    }
                }
            }
        )*
    };
}

implement_using_to_sql! {
    { i16, Int16, |x| x },
    { i32, Int32, |x| x },
    { i64, Int64, |x| x },
    { &str, Varchar, |x| x },
    { F32, Float32, |x: &F32| x.0 },
    { F64, Float64, |x: &F64| x.0 },
    { bool, Boolean, |x| x },
    { &[u8], Bytea, |x| x },
    { Time, Time, |x: &Time| x.0 },
    { Date, Date, |x: &Date| x.0 },
    { Timestamp, Timestamp, |x: &Timestamp| x.0 },
    { Decimal, Decimal, |x| x },
    { Interval, Interval, |x| x },
    { Serial, Serial, |x: &Serial| x.0 },
    { Timestamptz, Timestamptz, |x: &Timestamptz| x.to_datetime_utc() }
}

fn list_to_binary_with_type(
    iter: impl ExactSizeIterator<Item = Option<impl ToBinary>>,
    ty: &DataType,
) -> Result<Bytes> {
    {
        // Reference: Postgres code `src/backend/utils/adt/arrayfuncs.c`
        // https://github.com/postgres/postgres/blob/c1c09007e219ae68d1f8428a54baf68ccc1f8683/src/backend/utils/adt/arrayfuncs.c#L1548
        let element_ty = ty.as_list_elem();
        if matches!(element_ty, DataType::List(_)) {
            bail_not_implemented!(
                issue = 7949,
                "list with 2 or more dimensions is not supported"
            )
        }
        let mut buf = BytesMut::new();
        buf.put_i32(1); // Number of dimensions (must be 1)
        buf.put_i32(1); // Has nulls?
        buf.put_i32(element_ty.to_oid()); // Element type
        buf.put_i32(iter.len() as i32); // Length of 1st dimension
        buf.put_i32(1); // Offset of 1st dimension, starting from 1
        for element in iter {
            match element {
                None => {
                    buf.put_i32(-1); // -1 length means a NULL
                }
                Some(value) => {
                    let data = value.to_binary_with_type(element_ty)?;
                    buf.put_i32(data.len() as i32); // Length of element
                    buf.put(data);
                }
            }
        }
        Ok(buf.into())
    }
}

impl ToBinary for StructRef<'_> {
    fn to_binary_with_type(&self, ty: &DataType) -> Result<Bytes> {
        // Reference: Postgres code `src/backend/utils/adt/rowtypes.c`
        // https://github.com/postgres/postgres/blob/a3699daea2026de324ed7cc7115c36d3499010d3/src/backend/utils/adt/rowtypes.c#L687
        let mut buf = BytesMut::new();
        buf.put_i32(ty.as_struct().len() as i32); // number of columns
        for (datum, field_ty) in self.iter_fields_ref().zip_eq_fast(ty.as_struct().types()) {
            buf.put_i32(field_ty.to_oid()); // column type
            match datum {
                None => {
                    buf.put_i32(-1); // -1 length means a NULL
                }
                Some(value) => {
                    let data = value.to_binary_with_type(field_ty)?;
                    buf.put_i32(data.len() as i32); // Length of element
                    buf.put(data);
                }
            }
        }
        Ok(buf.into())
    }
}

impl ToBinary for ListRef<'_> {
    fn to_binary_with_type(&self, ty: &DataType) -> Result<Bytes> {
        list_to_binary_with_type(self.iter(), ty)
    }
}

impl ToBinary for ScalarRefImpl<'_> {
    fn to_binary_with_type(&self, ty: &DataType) -> Result<Bytes> {
        match self {
            ScalarRefImpl::Int16(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Int32(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Int64(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Int256(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Serial(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Float32(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Float64(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Utf8(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Bool(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Decimal(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Interval(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Date(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Timestamp(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Timestamptz(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Time(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Bytea(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Jsonb(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Variant(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Vector(v) => {
                assert_eq!(&DataType::Vector(v.dimension()), ty);
                list_to_binary_with_type(
                    v.as_slice().iter().cloned().map(Some),
                    &DataType::Float32.list(),
                )
            }
            ScalarRefImpl::List(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Struct(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Map(_) => {
                bail_not_implemented!(
                    issue = 7949,
                    "the pgwire extended-mode encoding for {ty} is unsupported"
                )
            }
        }
    }
}
