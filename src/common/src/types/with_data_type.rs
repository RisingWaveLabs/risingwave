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

use std::rc::Rc;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_pb::id::TypedId;

use super::{
    DataType, Date, Decimal, F32, F64, Fields, Int256, Interval, JsonbRef, JsonbVal, Serial,
    StructType, Time, Timestamp, Timestamptz, VariantRef, VariantVal,
};

/// A trait for all physical types that can be associated with a [`DataType`].
///
/// This is also a helper for [`Fields`](derive@crate::types::Fields) derive macro.
pub trait WithDataType {
    /// Returns the most obvious [`DataType`] for the rust type.
    fn default_data_type() -> DataType;
}

impl<T> WithDataType for Option<T>
where
    T: WithDataType,
{
    fn default_data_type() -> DataType {
        T::default_data_type()
    }
}

macro_rules! impl_with_data_type {
    ($t:ty, $val:expr) => {
        impl WithDataType for $t {
            fn default_data_type() -> DataType {
                $val
            }
        }

        impl<'a> WithDataType for &'a $t {
            fn default_data_type() -> DataType {
                $val
            }
        }

        impl<'a> WithDataType for &'a mut $t {
            fn default_data_type() -> DataType {
                $val
            }
        }

        impl WithDataType for Box<$t> {
            fn default_data_type() -> DataType {
                $val
            }
        }

        impl WithDataType for Rc<$t> {
            fn default_data_type() -> DataType {
                $val
            }
        }

        impl WithDataType for Arc<$t> {
            fn default_data_type() -> DataType {
                $val
            }
        }
    };
}

impl_with_data_type!(bool, DataType::Boolean);
impl_with_data_type!(i16, DataType::Int16);
impl_with_data_type!(i32, DataType::Int32);
impl_with_data_type!(i64, DataType::Int64);
impl_with_data_type!(Int256, DataType::Int256);
impl_with_data_type!(f32, DataType::Float32);
impl_with_data_type!(F32, DataType::Float32);
impl_with_data_type!(f64, DataType::Float64);
impl_with_data_type!(F64, DataType::Float64);
impl_with_data_type!(rust_decimal::Decimal, DataType::Decimal);
impl_with_data_type!(Decimal, DataType::Decimal);
impl_with_data_type!(Serial, DataType::Serial);

impl WithDataType for &str {
    fn default_data_type() -> DataType {
        DataType::Varchar
    }
}

impl_with_data_type!(char, DataType::Varchar);
impl_with_data_type!(String, DataType::Varchar);
impl_with_data_type!(Date, DataType::Date);
impl_with_data_type!(Time, DataType::Time);
impl_with_data_type!(Timestamp, DataType::Timestamp);
impl_with_data_type!(Timestamptz, DataType::Timestamptz);
impl_with_data_type!(Interval, DataType::Interval);
impl_with_data_type!(Vec<u8>, DataType::Bytea);
impl_with_data_type!(Bytes, DataType::Bytea);
impl_with_data_type!(JsonbVal, DataType::Jsonb);
impl_with_data_type!(VariantVal, DataType::Variant);

impl WithDataType for JsonbRef<'_> {
    fn default_data_type() -> DataType {
        DataType::Jsonb
    }
}

impl WithDataType for VariantRef<'_> {
    fn default_data_type() -> DataType {
        DataType::Variant
    }
}

impl<T> WithDataType for Vec<T>
where
    T: WithDataType,
{
    fn default_data_type() -> DataType {
        DataType::list(T::default_data_type())
    }
}

impl<T> WithDataType for [T]
where
    T: WithDataType,
{
    fn default_data_type() -> DataType {
        DataType::list(T::default_data_type())
    }
}

impl<T> WithDataType for T
where
    T: Fields,
{
    fn default_data_type() -> DataType {
        DataType::Struct(StructType::new(T::fields()))
    }
}

impl<const N: usize> WithDataType for TypedId<N, u32> {
    fn default_data_type() -> DataType {
        DataType::Int32
    }
}

impl<const N: usize> WithDataType for TypedId<N, u64> {
    fn default_data_type() -> DataType {
        DataType::Int64
    }
}
