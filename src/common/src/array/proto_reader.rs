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

use std::io::{Cursor, Read};

use anyhow::Context;
use byteorder::{BigEndian, ReadBytesExt};
use risingwave_pb::data::PbArrayType;

use super::*;

impl ArrayImpl {
    pub fn from_protobuf(array: &PbArray, cardinality: usize) -> ArrayResult<Self> {
        let array = match array.array_type() {
            PbArrayType::Unspecified => unreachable!(),
            PbArrayType::Int16 => read_primitive_array::<i16>(array, cardinality)?,
            PbArrayType::Int32 => read_primitive_array::<i32>(array, cardinality)?,
            PbArrayType::Int64 => read_primitive_array::<i64>(array, cardinality)?,
            PbArrayType::Serial => read_primitive_array::<Serial>(array, cardinality)?,
            PbArrayType::Float32 => read_primitive_array::<F32>(array, cardinality)?,
            PbArrayType::Float64 => read_primitive_array::<F64>(array, cardinality)?,
            PbArrayType::Bool => read_bool_array(array, cardinality)?,
            PbArrayType::Utf8 => read_var_sized_array::<Utf8ValueReader>(array, cardinality)?,
            PbArrayType::Decimal => read_primitive_array::<Decimal>(array, cardinality)?,
            PbArrayType::Date => read_primitive_array::<Date>(array, cardinality)?,
            PbArrayType::Time => read_primitive_array::<Time>(array, cardinality)?,
            PbArrayType::Timestamp => read_primitive_array::<Timestamp>(array, cardinality)?,
            PbArrayType::Timestamptz => read_primitive_array::<Timestamptz>(array, cardinality)?,
            PbArrayType::Interval => read_primitive_array::<Interval>(array, cardinality)?,
            PbArrayType::Jsonb => JsonbArray::from_protobuf(array)?,
            PbArrayType::Variant => read_var_sized_array::<VariantValueReader>(array, cardinality)?,
            PbArrayType::Struct => StructArray::from_protobuf(array)?,
            PbArrayType::List => ListArray::from_protobuf(array)?,
            PbArrayType::Bytea => read_var_sized_array::<BytesValueReader>(array, cardinality)?,
            PbArrayType::Int256 => Int256Array::from_protobuf(array, cardinality)?,
            PbArrayType::Map => MapArray::from_protobuf(array)?,
            PbArrayType::Vector => VectorArray::from_protobuf(array)?,
        };
        Ok(array)
    }
}

// TODO: Use techniques like apache arrow flight RPC to eliminate deserialization.
// https://arrow.apache.org/docs/format/Flight.html

fn read_primitive_array<T: PrimitiveArrayItemType>(
    array: &PbArray,
    cardinality: usize,
) -> ArrayResult<ArrayImpl> {
    ensure!(
        array.get_values().len() == 1,
        "Must have only 1 buffer in a numeric array"
    );

    let buf = array.get_values()[0].get_body().as_slice();

    let mut builder = PrimitiveArrayBuilder::<T>::new(cardinality);
    let bitmap: Bitmap = array.get_null_bitmap()?.into();
    let mut cursor = Cursor::new(buf);
    for not_null in bitmap.iter() {
        if not_null {
            let v = T::from_protobuf(&mut cursor)?;
            builder.append(Some(v));
        } else {
            builder.append(None);
        }
    }
    let arr = builder.finish();
    ensure_eq!(arr.len(), cardinality);

    Ok(arr.into())
}

fn read_bool_array(array: &PbArray, cardinality: usize) -> ArrayResult<ArrayImpl> {
    ensure!(
        array.get_values().len() == 1,
        "Must have only 1 buffer in a bool array"
    );

    let data = (&array.get_values()[0]).into();
    let bitmap: Bitmap = array.get_null_bitmap()?.into();

    let arr = BoolArray::new(data, bitmap);
    ensure_eq!(arr.len(), cardinality);

    Ok(arr.into())
}

fn read_offset(offset_cursor: &mut Cursor<&[u8]>) -> ArrayResult<i64> {
    let offset = offset_cursor
        .read_i64::<BigEndian>()
        .context("failed to read i64 from offset buffer")?;
    // `read_var_sized_array` uses a negative `prev_offset` as its "not yet read" sentinel, so a
    // negative offset here would silently lengthen the first value instead of being rejected.
    ensure!(
        offset >= 0,
        "invalid offset buffer: offsets must not be negative"
    );

    Ok(offset)
}

trait VarSizedValueReader {
    type AB: ArrayBuilder;
    fn new_builder(capacity: usize) -> Self::AB;
    fn read(buf: &[u8], builder: &mut Self::AB) -> ArrayResult<()>;
}

struct Utf8ValueReader;

impl VarSizedValueReader for Utf8ValueReader {
    type AB = Utf8ArrayBuilder;

    fn new_builder(capacity: usize) -> Self::AB {
        Utf8ArrayBuilder::new(capacity)
    }

    fn read(buf: &[u8], builder: &mut Utf8ArrayBuilder) -> ArrayResult<()> {
        let s = std::str::from_utf8(buf).context("failed to read utf8 string from bytes")?;
        builder.append(Some(s));
        Ok(())
    }
}

struct BytesValueReader;

impl VarSizedValueReader for BytesValueReader {
    type AB = BytesArrayBuilder;

    fn new_builder(capacity: usize) -> Self::AB {
        BytesArrayBuilder::new(capacity)
    }

    fn read(buf: &[u8], builder: &mut BytesArrayBuilder) -> ArrayResult<()> {
        builder.append(Some(buf));
        Ok(())
    }
}

struct VariantValueReader;

impl VarSizedValueReader for VariantValueReader {
    type AB = VariantArrayBuilder;

    fn new_builder(capacity: usize) -> Self::AB {
        VariantArrayBuilder::new(capacity)
    }

    fn read(buf: &[u8], builder: &mut VariantArrayBuilder) -> ArrayResult<()> {
        let variant =
            VariantRef::from_serialized(buf).context("failed to read variant from bytes")?;
        builder.append(Some(variant));
        Ok(())
    }
}

fn read_var_sized_array<R: VarSizedValueReader>(
    array: &PbArray,
    cardinality: usize,
) -> ArrayResult<ArrayImpl> {
    ensure!(
        array.get_values().len() == 2,
        "Must have exactly 2 buffers in a variable-sized array"
    );
    let offset_buff = array.get_values()[0].get_body().as_slice();
    let data_buf = array.get_values()[1].get_body().as_slice();

    let mut builder = R::new_builder(cardinality);
    let bitmap: Bitmap = array.get_null_bitmap()?.into();
    let mut offset_cursor = Cursor::new(offset_buff);
    let mut data_cursor = Cursor::new(data_buf);
    let mut prev_offset: i64 = -1;

    let mut buf = Vec::new();
    for not_null in bitmap.iter() {
        if not_null {
            if prev_offset < 0 {
                prev_offset = read_offset(&mut offset_cursor)?;
            }
            let offset = read_offset(&mut offset_cursor)?;
            // A decreasing offset would wrap to a huge length and drive the `resize` below.
            let length = offset
                .checked_sub(prev_offset)
                .and_then(|length| usize::try_from(length).ok())
                .context("invalid offset buffer: offsets must be non-decreasing")?;
            prev_offset = offset;
            buf.resize(length, Default::default());
            data_cursor
                .read_exact(buf.as_mut_slice())
                .with_context(|| {
                    format!(
                        "failed to read value from data buffer [length={}, offset={}]",
                        length, offset
                    )
                })?;
            R::read(buf.as_slice(), &mut builder)?;
        } else {
            builder.append(None);
        }
    }
    let arr = builder.finish();
    ensure_eq!(arr.len(), cardinality);

    Ok(arr.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Convert a column to protobuf, then convert it back to column, and ensures the two are
    // identical.
    #[test]
    fn test_column_protobuf_conversion() {
        let cardinality = 2048;
        let mut builder = I32ArrayBuilder::new(cardinality);
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder.append(Some(i as i32));
            } else {
                builder.append(None);
            }
        }
        let col: ArrayImpl = builder.finish().into();
        let new_col = ArrayImpl::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        assert_eq!(new_col.len(), cardinality);
        let arr: &I32Array = new_col.as_int32();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!(i as i32, x.unwrap());
            } else {
                assert!(x.is_none());
            }
        });
    }

    #[test]
    fn test_bool_column_protobuf_conversion() {
        let cardinality = 2048;
        let mut builder = BoolArrayBuilder::new(cardinality);
        for i in 0..cardinality {
            match i % 3 {
                0 => builder.append(Some(false)),
                1 => builder.append(Some(true)),
                _ => builder.append(None),
            }
        }
        let col: ArrayImpl = builder.finish().into();
        let new_col = ArrayImpl::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        assert_eq!(new_col.len(), cardinality);
        let arr: &BoolArray = new_col.as_bool();
        arr.iter().enumerate().for_each(|(i, x)| match i % 3 {
            0 => assert_eq!(Some(false), x),
            1 => assert_eq!(Some(true), x),
            _ => assert_eq!(None, x),
        });
    }

    #[test]
    fn test_utf8_column_conversion() {
        let cardinality = 2048;
        let mut builder = Utf8ArrayBuilder::new(cardinality);
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder.append(Some("abc"));
            } else {
                builder.append(None);
            }
        }
        let col: ArrayImpl = builder.finish().into();
        let new_col = ArrayImpl::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        let arr: &Utf8Array = new_col.as_utf8();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!("abc", x.unwrap());
            } else {
                assert!(x.is_none());
            }
        });
    }

    #[test]
    fn test_decimal_protobuf_conversion() {
        let cardinality = 2048;
        let mut builder = DecimalArrayBuilder::new(cardinality);
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder.append(Some(Decimal::from(i)));
            } else {
                builder.append(None);
            }
        }
        let col: ArrayImpl = builder.finish().into();
        let new_col = ArrayImpl::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        assert_eq!(new_col.len(), cardinality);
        let arr: &DecimalArray = new_col.as_decimal();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!(Decimal::from(i), x.unwrap());
            } else {
                assert!(x.is_none());
            }
        });
    }

    #[test]
    fn test_date_protobuf_conversion() {
        let cardinality = 2048;
        let mut builder = DateArrayBuilder::new(cardinality);
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder.append(Date::with_days_since_ce(i as i32).ok());
            } else {
                builder.append(None);
            }
        }
        let col: ArrayImpl = builder.finish().into();
        let new_col = ArrayImpl::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        assert_eq!(new_col.len(), cardinality);
        let arr: &DateArray = new_col.as_date();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!(Date::with_days_since_ce(i as i32).ok().unwrap(), x.unwrap());
            } else {
                assert!(x.is_none());
            }
        });
    }

    #[test]
    fn test_time_protobuf_conversion() {
        let cardinality = 2048;
        let mut builder = TimeArrayBuilder::new(cardinality);
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder.append(Time::with_secs_nano(i as u32, i as u32 * 1000).ok());
            } else {
                builder.append(None);
            }
        }
        let col: ArrayImpl = builder.finish().into();
        let new_col = ArrayImpl::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        assert_eq!(new_col.len(), cardinality);
        let arr: &TimeArray = new_col.as_time();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!(
                    Time::with_secs_nano(i as u32, i as u32 * 1000)
                        .ok()
                        .unwrap(),
                    x.unwrap()
                );
            } else {
                assert!(x.is_none());
            }
        });
    }

    #[test]
    fn test_timestamp_protobuf_conversion() {
        let cardinality = 2048;
        let mut builder = TimestampArrayBuilder::new(cardinality);
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder.append(Timestamp::with_secs_nsecs(i as i64, i as u32 * 1000).ok());
            } else {
                builder.append(None);
            }
        }
        let col: ArrayImpl = builder.finish().into();
        let new_col = ArrayImpl::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        assert_eq!(new_col.len(), cardinality);
        let arr: &TimestampArray = new_col.as_timestamp();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!(
                    Timestamp::with_secs_nsecs(i as i64, i as u32 * 1000)
                        .ok()
                        .unwrap(),
                    x.unwrap()
                );
            } else {
                assert!(x.is_none());
            }
        });
    }
}
