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

//! Data types in RisingWave.

// NOTE: When adding or modifying data types, remember to update the type matrix in
// src/expr/macro/src/types.rs

use std::fmt::Debug;
use std::hash::Hash;
use std::str::FromStr;

use bytes::{Buf, BufMut, Bytes};
use chrono::{Datelike, Timelike};
use itertools::Itertools;
use parse_display::{Display, FromStr};
use paste::paste;
use postgres_types::{FromSql, IsNull, ToSql, Type};
use risingwave_common_estimate_size::{EstimateSize, ZeroHeapSize};
use risingwave_pb::data::PbDataType;
use risingwave_pb::data::data_type::PbTypeName;
use rw_iter_util::ZipEqFast as _;
use serde::{Deserialize, Serialize, Serializer};
use strum_macros::EnumDiscriminants;
use thiserror_ext::AsReport;

use crate::array::{
    ArrayBuilderImpl, ArrayError, ArrayResult, NULL_VAL_FOR_HASH, PrimitiveArrayItemType,
};
// Complex type's value is based on the array
pub use crate::array::{
    ListRef, ListValue, MapRef, MapValue, StructRef, StructValue, VectorRef, VectorVal,
};
use crate::cast::{str_to_bool, str_to_bytea};
use crate::catalog::ColumnId;
use crate::error::BoxedError;
use crate::{
    dispatch_data_types, dispatch_scalar_ref_variants, dispatch_scalar_variants, for_all_variants,
};

mod cow;
mod datetime;
mod decimal;
mod fields;
mod from_sql;
mod interval;
mod jsonb;
mod list_type;
mod macros;
mod map_type;
mod native_type;
mod num256;
mod ops;
mod ordered;
mod ordered_float;
pub mod postgres_type;
mod scalar_impl;
mod sentinel;
mod serial;
mod struct_type;
mod successor;
mod timestamptz;
mod to_binary;
mod to_sql;
mod to_text;
mod variant;
mod with_data_type;

pub use fields::Fields;
pub use risingwave_fields_derive::Fields;
use risingwave_pb::id::TypedId;

pub use self::cow::DatumCow;
pub use self::datetime::{Date, Time, Timestamp};
pub use self::decimal::{Decimal, PowError as DecimalPowError};
pub use self::interval::{DateTimeField, Interval, IntervalDisplay, test_utils};
pub use self::jsonb::{JsonbRef, JsonbVal};
pub use self::list_type::ListType;
pub use self::map_type::MapType;
pub use self::native_type::*;
pub use self::num256::{Int256, Int256Ref};
pub use self::ops::{CheckedAdd, IsNegative};
pub use self::ordered::*;
pub use self::ordered_float::{FloatExt, IntoOrdered};
pub use self::scalar_impl::*;
pub use self::sentinel::Sentinelled;
pub use self::serial::Serial;
pub use self::struct_type::StructType;
pub use self::successor::Successor;
pub use self::timestamptz::*;
pub use self::to_text::ToText;
pub use self::variant::{VariantPath, VariantRef, VariantVal};
pub use self::with_data_type::WithDataType;

/// A 32-bit floating point type with total order.
pub type F32 = ordered_float::OrderedFloat<f32>;

/// A 64-bit floating point type with total order.
pub type F64 = ordered_float::OrderedFloat<f64>;

pub const DEBEZIUM_UNAVAILABLE_VALUE: &str = "__debezium_unavailable_value";

// Pre-built JSON value for Debezium unavailable value to avoid rebuilding it every time
pub static DEBEZIUM_UNAVAILABLE_JSON: std::sync::LazyLock<JsonbVal> =
    std::sync::LazyLock::new(|| {
        let mut builder = jsonbb::Builder::default();
        builder.add_string(DEBEZIUM_UNAVAILABLE_VALUE);
        JsonbVal(builder.finish())
    });

/// Magic per-element value used to build the Debezium unchanged-TOAST sentinel for
/// pgvector columns. Picked because normal embeddings sit in a normalised range and
/// having all elements simultaneously equal to `f32::MAX` is effectively impossible.
pub const DEBEZIUM_UNAVAILABLE_VECTOR_ELEM: f32 = f32::MAX;

/// Build a sentinel `VectorVal` of the given dimension to represent Debezium's
/// unchanged-TOAST placeholder. The dimension must match the column's declared
/// `vector(N)` size so it passes `check_datum_type` on the way through the
/// `SourceStreamChunkBuilder`; the materialize executor recognises this sentinel
/// by checking that every element equals `DEBEZIUM_UNAVAILABLE_VECTOR_ELEM`.
pub fn debezium_unavailable_vector(size: usize) -> VectorVal {
    VectorVal::from(
        (0..size)
            .map(|_| {
                crate::array::Finite32::try_from(DEBEZIUM_UNAVAILABLE_VECTOR_ELEM)
                    .expect("f32::MAX is finite")
            })
            .collect::<Vec<_>>(),
    )
}

/// The set of datatypes that are supported in RisingWave.
///
/// # Trait implementations
///
/// - `EnumDiscriminants` generates [`DataTypeName`] enum with the same variants,
///   but without data fields.
/// - `FromStr` is only used internally for tests.
///   The generated implementation isn't efficient, and doesn't handle whitespaces, etc.
#[derive(Debug, Display, Clone, PartialEq, Eq, Hash, EnumDiscriminants, FromStr)]
#[strum_discriminants(derive(Hash, Ord, PartialOrd))]
#[strum_discriminants(name(DataTypeName))]
#[strum_discriminants(vis(pub))]
#[cfg_attr(test, strum_discriminants(derive(strum_macros::EnumIter)))]
pub enum DataType {
    #[display("boolean")]
    #[from_str(regex = "(?i)^bool$|^boolean$")]
    Boolean,
    #[display("smallint")]
    #[from_str(regex = "(?i)^smallint$|^int2$")]
    Int16,
    #[display("integer")]
    #[from_str(regex = "(?i)^integer$|^int$|^int4$")]
    Int32,
    #[display("bigint")]
    #[from_str(regex = "(?i)^bigint$|^int8$")]
    Int64,
    #[display("real")]
    #[from_str(regex = "(?i)^real$|^float4$")]
    Float32,
    #[display("double precision")]
    #[from_str(regex = "(?i)^double precision$|^float8$")]
    Float64,
    #[display("numeric")]
    #[from_str(regex = "(?i)^numeric$|^decimal$")]
    Decimal,
    #[display("date")]
    #[from_str(regex = "(?i)^date$")]
    Date,
    #[display("character varying")]
    #[from_str(regex = "(?i)^character varying$|^varchar$")]
    Varchar,
    #[display("time without time zone")]
    #[from_str(regex = "(?i)^time$|^time without time zone$")]
    Time,
    #[display("timestamp without time zone")]
    #[from_str(regex = "(?i)^timestamp$|^timestamp without time zone$")]
    Timestamp,
    #[display("timestamp with time zone")]
    #[from_str(regex = "(?i)^timestamptz$|^timestamp with time zone$")]
    Timestamptz,
    #[display("interval")]
    #[from_str(regex = "(?i)^interval$")]
    Interval,
    #[display("{0}")]
    #[from_str(regex = "(?i)^(?P<0>.+)$")]
    Struct(StructType),
    #[display("{0}")]
    #[from_str(regex = "(?i)^(?P<0>.+)$")]
    List(ListType),
    #[display("bytea")]
    #[from_str(regex = "(?i)^bytea$")]
    Bytea,
    #[display("jsonb")]
    #[from_str(regex = "(?i)^jsonb$")]
    Jsonb,
    #[display("serial")]
    #[from_str(regex = "(?i)^serial$")]
    Serial,
    #[display("rw_int256")]
    #[from_str(regex = "(?i)^rw_int256$")]
    Int256,
    #[display("{0}")]
    #[from_str(regex = "(?i)^(?P<0>.+)$")]
    Map(MapType),
    #[display("vector({0})")]
    #[from_str(regex = "(?i)^vector\\((?P<0>.+)\\)$")]
    Vector(usize),
    #[display("variant")]
    #[from_str(regex = "(?i)^variant$")]
    Variant,
}

impl !PartialOrd for DataType {}

impl ZeroHeapSize for DataType {}

impl TryFrom<DataTypeName> for DataType {
    type Error = &'static str;

    fn try_from(type_name: DataTypeName) -> Result<Self, Self::Error> {
        match type_name {
            DataTypeName::Boolean => Ok(DataType::Boolean),
            DataTypeName::Int16 => Ok(DataType::Int16),
            DataTypeName::Int32 => Ok(DataType::Int32),
            DataTypeName::Int64 => Ok(DataType::Int64),
            DataTypeName::Int256 => Ok(DataType::Int256),
            DataTypeName::Serial => Ok(DataType::Serial),
            DataTypeName::Decimal => Ok(DataType::Decimal),
            DataTypeName::Float32 => Ok(DataType::Float32),
            DataTypeName::Float64 => Ok(DataType::Float64),
            DataTypeName::Varchar => Ok(DataType::Varchar),
            DataTypeName::Bytea => Ok(DataType::Bytea),
            DataTypeName::Date => Ok(DataType::Date),
            DataTypeName::Timestamp => Ok(DataType::Timestamp),
            DataTypeName::Timestamptz => Ok(DataType::Timestamptz),
            DataTypeName::Time => Ok(DataType::Time),
            DataTypeName::Interval => Ok(DataType::Interval),
            DataTypeName::Jsonb => Ok(DataType::Jsonb),
            DataTypeName::Variant => Ok(DataType::Variant),
            DataTypeName::Struct
            | DataTypeName::List
            | DataTypeName::Map
            | DataTypeName::Vector => Err(
                "Functions returning parameterized types cannot be inferred. Please use `FunctionCall::new_unchecked`.",
            ),
        }
    }
}

impl From<&PbDataType> for DataType {
    fn from(proto: &PbDataType) -> DataType {
        match proto.get_type_name().expect("missing type field") {
            PbTypeName::TypeUnspecified => unreachable!(),
            PbTypeName::Int16 => DataType::Int16,
            PbTypeName::Int32 => DataType::Int32,
            PbTypeName::Int64 => DataType::Int64,
            PbTypeName::Serial => DataType::Serial,
            PbTypeName::Float => DataType::Float32,
            PbTypeName::Double => DataType::Float64,
            PbTypeName::Boolean => DataType::Boolean,
            PbTypeName::Varchar => DataType::Varchar,
            PbTypeName::Date => DataType::Date,
            PbTypeName::Time => DataType::Time,
            PbTypeName::Timestamp => DataType::Timestamp,
            PbTypeName::Timestamptz => DataType::Timestamptz,
            PbTypeName::Decimal => DataType::Decimal,
            PbTypeName::Interval => DataType::Interval,
            PbTypeName::Bytea => DataType::Bytea,
            PbTypeName::Jsonb => DataType::Jsonb,
            PbTypeName::Variant => DataType::Variant,
            PbTypeName::Struct => {
                let fields: Vec<DataType> = proto.field_type.iter().map(|f| f.into()).collect_vec();
                let field_names: Vec<String> = proto.field_names.iter().cloned().collect_vec();
                let field_ids = (proto.field_ids.iter().copied())
                    .map(ColumnId::new)
                    .collect_vec();

                let mut struct_type = if proto.field_names.is_empty() {
                    StructType::unnamed(fields)
                } else {
                    StructType::new(field_names.into_iter().zip_eq_fast(fields))
                };
                // `field_ids` is used for nested-schema evolution. Cases when `field_ids` is empty:
                //
                // 1. The data type is not associated with a table column, so we don't need to set it.
                // 2. The column is created before nested-schema evolution is supported, thus is using
                //    the old serialization format and does not have field ids.
                // 3. This is an empty struct, which is always considered alterable, and setting ids
                //    is a no-op.
                if !field_ids.is_empty() {
                    struct_type = struct_type.with_ids(field_ids);
                }
                struct_type.into()
            }
            PbTypeName::List => DataType::list(
                // The first (and only) item is the list element type.
                proto.field_type[0].clone().into(),
            ),
            PbTypeName::Map => {
                // Map is physically the same as a list.
                // So the first (and only) item is the list element type.
                let list_entries_type: DataType = (&proto.field_type[0]).into();
                DataType::Map(MapType::from_entries(list_entries_type))
            }
            PbTypeName::Vector => DataType::Vector(proto.precision as _),
            PbTypeName::Int256 => DataType::Int256,
        }
    }
}

impl From<PbDataType> for DataType {
    fn from(proto: PbDataType) -> DataType {
        DataType::from(&proto)
    }
}

impl From<DataTypeName> for PbTypeName {
    fn from(type_name: DataTypeName) -> Self {
        match type_name {
            DataTypeName::Boolean => PbTypeName::Boolean,
            DataTypeName::Int16 => PbTypeName::Int16,
            DataTypeName::Int32 => PbTypeName::Int32,
            DataTypeName::Int64 => PbTypeName::Int64,
            DataTypeName::Serial => PbTypeName::Serial,
            DataTypeName::Float32 => PbTypeName::Float,
            DataTypeName::Float64 => PbTypeName::Double,
            DataTypeName::Varchar => PbTypeName::Varchar,
            DataTypeName::Date => PbTypeName::Date,
            DataTypeName::Timestamp => PbTypeName::Timestamp,
            DataTypeName::Timestamptz => PbTypeName::Timestamptz,
            DataTypeName::Time => PbTypeName::Time,
            DataTypeName::Interval => PbTypeName::Interval,
            DataTypeName::Decimal => PbTypeName::Decimal,
            DataTypeName::Bytea => PbTypeName::Bytea,
            DataTypeName::Jsonb => PbTypeName::Jsonb,
            DataTypeName::Variant => PbTypeName::Variant,
            DataTypeName::Struct => PbTypeName::Struct,
            DataTypeName::List => PbTypeName::List,
            DataTypeName::Int256 => PbTypeName::Int256,
            DataTypeName::Map => PbTypeName::Map,
            DataTypeName::Vector => PbTypeName::Vector,
        }
    }
}

/// Convenient macros to generate match arms for [`DataType`].
pub mod data_types {
    use super::DataType;

    /// Numeric [`DataType`]s supported to be `offset` of `RANGE` frame.
    #[macro_export]
    macro_rules! _range_frame_numeric_data_types {
        () => {
            DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal
        };
    }
    pub use _range_frame_numeric_data_types as range_frame_numeric;

    /// Date/time [`DataType`]s supported to be `offset` of `RANGE` frame.
    #[macro_export]
    macro_rules! _range_frame_datetime_data_types {
        () => {
            DataType::Date
                | DataType::Time
                | DataType::Timestamp
                | DataType::Timestamptz
                | DataType::Interval
        };
    }
    pub use _range_frame_datetime_data_types as range_frame_datetime;

    /// Data types that do not have inner fields.
    #[macro_export]
    macro_rules! _simple_data_types {
        () => {
            DataType::Boolean
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal
                | DataType::Date
                | DataType::Varchar
                | DataType::Time
                | DataType::Timestamp
                | DataType::Timestamptz
                | DataType::Interval
                | DataType::Bytea
                | DataType::Jsonb
                | DataType::Variant
                | DataType::Serial
                | DataType::Int256
                | DataType::Vector(_)
        };
    }
    pub use _simple_data_types as simple;

    /// Data types that have inner fields.
    #[macro_export]
    macro_rules! _composite_data_types {
        () => {
            DataType::Struct { .. } | DataType::List { .. } | DataType::Map { .. }
        };
    }
    pub use _composite_data_types as composite;

    /// Test that all data types are covered either by `simple!()` or `composite!()`.
    fn _simple_composite_data_types_exhausted(dt: DataType) {
        match dt {
            simple!() => {}
            composite!() => {}
        }
    }
}

impl DataType {
    /// Same as pgvector; unsure how it was chosen there
    /// <https://github.com/pgvector/pgvector/blob/v0.8.0/README.md#vector-type>
    pub const VEC_MAX_SIZE: usize = 16000;

    pub fn create_array_builder(&self, capacity: usize) -> ArrayBuilderImpl {
        use crate::array::*;

        dispatch_data_types!(self, [B = ArrayBuilder], {
            B::with_type(capacity, self.clone()).into()
        })
    }

    pub fn type_name(&self) -> DataTypeName {
        DataTypeName::from(self)
    }

    pub fn prost_type_name(&self) -> PbTypeName {
        self.type_name().into()
    }

    pub fn to_protobuf(&self) -> PbDataType {
        let mut pb = PbDataType {
            type_name: self.prost_type_name() as i32,
            is_nullable: true,
            ..Default::default()
        };
        match self {
            DataType::Struct(t) => {
                if !t.is_unnamed() {
                    // To be consistent with `From<&PbDataType>`,
                    // we only set field names when it's a named struct.
                    pb.field_names = t.names().map(|s| s.into()).collect();
                }
                pb.field_type = t.types().map(|f| f.to_protobuf()).collect();
                if let Some(ids) = t.ids() {
                    pb.field_ids = ids.map(|id| id.get_id()).collect();
                }
            }
            DataType::List(list) => {
                pb.field_type = vec![list.elem().to_protobuf()];
            }
            DataType::Map(map) => {
                // Same as List<Struct<K,V>>
                pb.field_type = vec![map.clone().into_struct().to_protobuf()];
            }
            DataType::Vector(size) => {
                pb.precision = *size as _;
            }
            DataType::Boolean
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal
            | DataType::Date
            | DataType::Varchar
            | DataType::Time
            | DataType::Timestamp
            | DataType::Timestamptz
            | DataType::Interval
            | DataType::Bytea
            | DataType::Jsonb
            | DataType::Variant
            | DataType::Serial
            | DataType::Int256 => (),
        }
        pb
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Serial
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal
        )
    }

    /// Returns whether the data type does not have inner fields.
    pub fn is_simple(&self) -> bool {
        matches!(self, data_types::simple!())
    }

    /// Returns whether the data type has inner fields.
    pub fn is_composite(&self) -> bool {
        matches!(self, data_types::composite!())
    }

    pub fn is_array(&self) -> bool {
        matches!(self, DataType::List(_))
    }

    pub fn is_struct(&self) -> bool {
        matches!(self, DataType::Struct(_))
    }

    pub fn is_map(&self) -> bool {
        matches!(self, DataType::Map(_))
    }

    pub fn is_int(&self) -> bool {
        matches!(self, DataType::Int16 | DataType::Int32 | DataType::Int64)
    }

    /// Returns the output type of time window function on a given input type.
    pub fn window_of(input: &DataType) -> Option<DataType> {
        match input {
            DataType::Timestamptz => Some(DataType::Timestamptz),
            DataType::Timestamp | DataType::Date => Some(DataType::Timestamp),
            _ => None,
        }
    }

    pub fn as_struct(&self) -> &StructType {
        match self {
            DataType::Struct(t) => t,
            t => panic!("expect struct type, got {t}"),
        }
    }

    pub fn into_struct(self) -> StructType {
        match self {
            DataType::Struct(t) => t,
            t => panic!("expect struct type, got {t}"),
        }
    }

    pub fn as_map(&self) -> &MapType {
        match self {
            DataType::Map(t) => t,
            t => panic!("expect map type, got {t}"),
        }
    }

    pub fn into_map(self) -> MapType {
        match self {
            DataType::Map(t) => t,
            t => panic!("expect map type, got {t}"),
        }
    }

    pub fn as_list(&self) -> &ListType {
        match self {
            DataType::List(t) => t,
            t => panic!("expect list type, got {t}"),
        }
    }

    pub fn into_list(self) -> ListType {
        match self {
            DataType::List(t) => t,
            t => panic!("expect list type, got {t}"),
        }
    }

    /// Returns the inner element's type if `self` is a list type.
    /// Equivalent to `self.as_list().elem()`.
    pub fn as_list_elem(&self) -> &DataType {
        self.as_list().elem()
    }

    /// Returns the inner element's type if `self` is a list type.
    /// Equivalent to `self.into_list().into_elem()`.
    pub fn into_list_elem(self) -> DataType {
        self.into_list().into_elem()
    }

    /// Return a new type that removes the outer list, and get the innermost element type.
    ///
    /// Use [`DataType::as_list_elem`] if you only want the element type of a list.
    ///
    /// ```
    /// use risingwave_common::types::DataType::*;
    /// assert_eq!(Int32.list().unnest_list(), &Int32);
    /// assert_eq!(Int32.list().list().unnest_list(), &Int32);
    /// ```
    pub fn unnest_list(&self) -> &Self {
        match self {
            DataType::List(list) => list.elem().unnest_list(),
            _ => self,
        }
    }

    /// Return the number of dimensions of this array/list type. Return `0` when this type is not an
    /// array/list.
    pub fn array_ndims(&self) -> usize {
        let mut d = 0;
        let mut t = self;
        while let Self::List(list) = t {
            d += 1;
            t = list.elem();
        }
        d
    }

    /// Compares the datatype with another, ignoring nested field names and ids.
    pub fn equals_datatype(&self, other: &DataType) -> bool {
        match (self, other) {
            (Self::Struct(s1), Self::Struct(s2)) => s1.equals_datatype(s2),
            (Self::List(d1), Self::List(d2)) => d1.elem().equals_datatype(d2.elem()),
            (Self::Map(m1), Self::Map(m2)) => {
                m1.key().equals_datatype(m2.key()) && m1.value().equals_datatype(m2.value())
            }
            _ => self == other,
        }
    }

    /// Whether a column with this data type can be altered to a new data type. This determines
    /// the encoding of the column data.
    ///
    /// Returns...
    /// - `None`, if the data type is simple or does not contain a struct type.
    /// - `Some(true)`, if the data type contains a struct type with field ids ([`StructType::has_ids`]).
    /// - `Some(false)`, if the data type contains a struct type without field ids.
    pub fn can_alter(&self) -> Option<bool> {
        match self {
            data_types::simple!() => None,
            DataType::Struct(struct_type) => {
                // As long as we meet a struct type, we can check its `ids` field to determine if
                // it can be altered.
                let struct_can_alter = struct_type.has_ids();
                // In debug build, we assert that once a struct type does (or does not) have ids,
                // all its composite fields should have the same property.
                if cfg!(debug_assertions) {
                    for field in struct_type.types() {
                        if let Some(field_can_alter) = field.can_alter() {
                            assert_eq!(struct_can_alter, field_can_alter);
                        }
                    }
                }
                Some(struct_can_alter)
            }

            DataType::List(list_type) => list_type.elem().can_alter(),
            DataType::Map(map_type) => {
                debug_assert!(
                    map_type.key().is_simple(),
                    "unexpected key type of map {map_type:?}"
                );
                map_type.value().can_alter()
            }
        }
    }

    /// Whether this type is `VARIANT` or contains a nested `VARIANT`.
    pub fn contains_variant(&self) -> bool {
        matches!(self, DataType::Variant)
            || match self {
                DataType::List(list_type) => list_type.elem().contains_variant(),
                DataType::Struct(struct_type) => {
                    struct_type.types().any(DataType::contains_variant)
                }
                DataType::Map(map_type) => {
                    map_type.key().contains_variant() || map_type.value().contains_variant()
                }
                // Listed rather than `_`: a new composite type answering `false` here would slip
                // past every VARIANT-as-key gate.
                data_types::simple!() => false,
            }
    }
}

impl From<StructType> for DataType {
    fn from(value: StructType) -> Self {
        Self::Struct(value)
    }
}

impl From<DataType> for PbDataType {
    fn from(data_type: DataType) -> Self {
        data_type.to_protobuf()
    }
}

mod private {
    use super::*;

    // Note: put pub trait inside a private mod just makes the name private,
    // The trait methods will still be publicly available...
    // a.k.a. ["Voldemort type"](https://rust-lang.github.io/rfcs/2145-type-privacy.html#lint-3-voldemort-types-its-reachable-but-i-cant-name-it)

    /// Common trait bounds of scalar and scalar reference types.
    ///
    /// NOTE(rc): `Hash` is not in the trait bound list, it's implemented as [`ScalarRef::hash_scalar`].
    pub trait ScalarBounds<Impl> = Debug
        + Send
        + Sync
        + Clone
        + PartialEq
        + Eq
        // in default ascending order
        + PartialOrd
        + Ord
        + TryFrom<Impl, Error = ArrayError>
        // `ScalarImpl`/`ScalarRefImpl`
        + Into<Impl>;
}

/// `Scalar` is a trait over all possible owned types in the evaluation
/// framework.
///
/// `Scalar` is reciprocal to `ScalarRef`. Use `as_scalar_ref` to get a
/// reference which has the same lifetime as `self`.
pub trait Scalar: private::ScalarBounds<ScalarImpl> + 'static {
    /// Type for reference of `Scalar`
    type ScalarRefType<'a>: ScalarRef<'a, ScalarType = Self> + 'a
    where
        Self: 'a;

    /// Get a reference to current scalar.
    fn as_scalar_ref(&self) -> Self::ScalarRefType<'_>;

    fn to_scalar_value(self) -> ScalarImpl {
        self.into()
    }
}

/// `ScalarRef` is a trait over all possible references in the evaluation
/// framework.
///
/// `ScalarRef` is reciprocal to `Scalar`. Use `to_owned_scalar` to get an
/// owned scalar.
pub trait ScalarRef<'a>: private::ScalarBounds<ScalarRefImpl<'a>> + 'a + Copy {
    /// `ScalarType` is the owned type of current `ScalarRef`.
    type ScalarType: Scalar<ScalarRefType<'a> = Self>;

    /// Convert `ScalarRef` to an owned scalar.
    fn to_owned_scalar(&self) -> Self::ScalarType;

    /// A wrapped hash function to get the hash value for this scaler.
    fn hash_scalar<H: std::hash::Hasher>(&self, state: &mut H);
}

/// Define `ScalarImpl` and `ScalarRefImpl` with macro.
macro_rules! scalar_impl_enum {
    ($( { $data_type:ident, $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty, $array:ty, $builder:ty } ),*) => {
        /// `ScalarImpl` embeds all possible scalars in the evaluation framework.
        ///
        /// Note: `ScalarImpl` doesn't contain all information of its `DataType`,
        /// so sometimes they need to be used together.
        /// e.g., for `Struct`, we don't have the field names in the value.
        ///
        /// See `for_all_variants` for the definition.
        #[derive(Debug, Clone, PartialEq, Eq, EstimateSize)]
        pub enum ScalarImpl {
            $( $variant_name($scalar) ),*
        }

        /// `ScalarRefImpl` embeds all possible scalar references in the evaluation
        /// framework.
        ///
        /// Note: `ScalarRefImpl` doesn't contain all information of its `DataType`,
        /// so sometimes they need to be used together.
        /// e.g., for `Struct`, we don't have the field names in the value.
        ///
        /// See `for_all_variants` for the definition.
        #[derive(Debug, Copy, Clone, PartialEq, Eq)]
        pub enum ScalarRefImpl<'scalar> {
            $( $variant_name($scalar_ref) ),*
        }
    };
}

for_all_variants! { scalar_impl_enum }

// We MUST NOT implement `Ord` for `ScalarImpl` because that will make `Datum` derive an incorrect
// default `Ord`. To get a default-ordered `ScalarImpl`/`ScalarRefImpl`/`Datum`/`DatumRef`, you can
// use `DefaultOrdered<T>`. If non-default order is needed, please refer to `sort_util`.
impl !PartialOrd for ScalarImpl {}
impl !PartialOrd for ScalarRefImpl<'_> {}

pub type Datum = Option<ScalarImpl>;
pub type DatumRef<'a> = Option<ScalarRefImpl<'a>>;

/// This trait is to implement `to_owned_datum` for `Option<ScalarImpl>`
pub trait ToOwnedDatum {
    /// Convert the datum to an owned [`Datum`].
    fn to_owned_datum(self) -> Datum;
}

impl ToOwnedDatum for &Datum {
    #[inline(always)]
    fn to_owned_datum(self) -> Datum {
        self.clone()
    }
}

impl<T: Into<ScalarImpl>> ToOwnedDatum for T {
    #[inline(always)]
    fn to_owned_datum(self) -> Datum {
        Some(self.into())
    }
}

impl<T: Into<ScalarImpl>> ToOwnedDatum for Option<T> {
    #[inline(always)]
    fn to_owned_datum(self) -> Datum {
        self.map(Into::into)
    }
}

impl<const N: usize> From<TypedId<N, u32>> for ScalarImpl {
    fn from(value: TypedId<N, u32>) -> Self {
        value.as_i32_id().into()
    }
}

impl<const N: usize> From<TypedId<N, u64>> for ScalarImpl {
    fn from(value: TypedId<N, u64>) -> Self {
        value.as_i64_id().into()
    }
}

#[auto_impl::auto_impl(&)]
pub trait ToDatumRef: PartialEq + Eq + Debug + Send + Sync {
    /// Convert the datum to [`DatumRef`].
    fn to_datum_ref(&self) -> DatumRef<'_>;
}

impl ToDatumRef for Datum {
    #[inline(always)]
    fn to_datum_ref(&self) -> DatumRef<'_> {
        self.as_ref().map(|d| d.as_scalar_ref_impl())
    }
}
impl ToDatumRef for Option<&ScalarImpl> {
    #[inline(always)]
    fn to_datum_ref(&self) -> DatumRef<'_> {
        self.map(|d| d.as_scalar_ref_impl())
    }
}
impl ToDatumRef for DatumRef<'_> {
    #[inline(always)]
    fn to_datum_ref(&self) -> DatumRef<'_> {
        *self
    }
}

/// To make sure there is `as_scalar_ref` for all scalar ref types.
/// See <https://github.com/risingwavelabs/risingwave/pull/9977/files#r1208972881>
///
/// This is used by the expr macro.
pub trait SelfAsScalarRef {
    fn as_scalar_ref(&self) -> Self;
}
macro_rules! impl_self_as_scalar_ref {
    ($($t:ty),*) => {
        $(
            impl SelfAsScalarRef for $t {
                fn as_scalar_ref(&self) -> Self {
                    *self
                }
            }
        )*
    };
}
impl_self_as_scalar_ref! { &str, &[u8], Int256Ref<'_>, JsonbRef<'_>, VariantRef<'_>, ListRef<'_>, StructRef<'_>, ScalarRefImpl<'_>, MapRef<'_> }

/// `for_all_native_types` includes all native variants of our scalar types.
///
/// Specifically, it doesn't support u8/u16/u32/u64.
#[macro_export]
macro_rules! for_all_native_types {
    ($macro:ident) => {
        $macro! {
            { i16, Int16, read_i16 },
            { i32, Int32, read_i32 },
            { i64, Int64, read_i64 },
            { Serial, Serial, read_i64 },
            { $crate::types::F32, Float32, read_f32 },
            { $crate::types::F64, Float64, read_f64 }
        }
    };
}

/// `impl_convert` implements several conversions for `Scalar`.
/// * `Scalar <-> ScalarImpl` with `From` and `TryFrom` trait.
/// * `ScalarRef <-> ScalarRefImpl` with `From` and `TryFrom` trait.
/// * `&ScalarImpl -> &Scalar` with `impl.as_int16()`.
/// * `ScalarImpl -> Scalar` with `impl.into_int16()`.
macro_rules! impl_convert {
    ($( { $data_type:ident, $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty, $array:ty, $builder:ty } ),*) => {
        $(
            impl From<$scalar> for ScalarImpl {
                fn from(val: $scalar) -> Self {
                    ScalarImpl::$variant_name(val)
                }
            }

            impl TryFrom<ScalarImpl> for $scalar {
                type Error = ArrayError;

                fn try_from(val: ScalarImpl) -> ArrayResult<Self> {
                    match val {
                        ScalarImpl::$variant_name(scalar) => Ok(scalar),
                        other_scalar => bail!("cannot convert ScalarImpl::{} to concrete type", other_scalar.get_ident()),
                    }
                }
            }

            impl <'scalar> From<$scalar_ref> for ScalarRefImpl<'scalar> {
                fn from(val: $scalar_ref) -> Self {
                    ScalarRefImpl::$variant_name(val)
                }
            }

            impl <'scalar> TryFrom<ScalarRefImpl<'scalar>> for $scalar_ref {
                type Error = ArrayError;

                fn try_from(val: ScalarRefImpl<'scalar>) -> ArrayResult<Self> {
                    match val {
                        ScalarRefImpl::$variant_name(scalar_ref) => Ok(scalar_ref),
                        other_scalar => bail!("cannot convert ScalarRefImpl::{} to concrete type {}", other_scalar.get_ident(), stringify!($variant_name)),
                    }
                }
            }

            paste! {
                impl ScalarImpl {
                    /// # Panics
                    /// If the scalar is not of the expected type.
                    pub fn [<as_ $suffix_name>](&self) -> &$scalar {
                        match self {
                            Self::$variant_name(scalar) => scalar,
                            other_scalar => panic!("cannot convert ScalarImpl::{} to concrete type {}", other_scalar.get_ident(), stringify!($variant_name))
                        }
                    }

                    /// # Panics
                    /// If the scalar is not of the expected type.
                    pub fn [<into_ $suffix_name>](self) -> $scalar {
                        match self {
                            Self::$variant_name(scalar) => scalar,
                            other_scalar =>  panic!("cannot convert ScalarImpl::{} to concrete type {}", other_scalar.get_ident(), stringify!($variant_name))
                        }
                    }
                }

                impl <'scalar> ScalarRefImpl<'scalar> {
                    /// # Panics
                    /// If the scalar is not of the expected type.
                    pub fn [<into_ $suffix_name>](self) -> $scalar_ref {
                        match self {
                            Self::$variant_name(inner) => inner,
                            other_scalar => panic!("cannot convert ScalarRefImpl::{} to concrete type {}", other_scalar.get_ident(), stringify!($variant_name))
                        }
                    }
                }
            }
        )*
    };
}

for_all_variants! { impl_convert }

// Implement `From<raw float>` for `ScalarImpl::Float` as a sugar.
impl From<f32> for ScalarImpl {
    fn from(f: f32) -> Self {
        Self::Float32(f.into())
    }
}
impl From<f64> for ScalarImpl {
    fn from(f: f64) -> Self {
        Self::Float64(f.into())
    }
}

// Implement `From<string like>` for `ScalarImpl::Utf8` as a sugar.
impl From<String> for ScalarImpl {
    fn from(s: String) -> Self {
        Self::Utf8(s.into_boxed_str())
    }
}
impl From<&str> for ScalarImpl {
    fn from(s: &str) -> Self {
        Self::Utf8(s.into())
    }
}
impl From<&String> for ScalarImpl {
    fn from(s: &String) -> Self {
        Self::Utf8(s.as_str().into())
    }
}
impl TryFrom<ScalarImpl> for String {
    type Error = ArrayError;

    fn try_from(val: ScalarImpl) -> ArrayResult<Self> {
        match val {
            ScalarImpl::Utf8(s) => Ok(s.into()),
            other_scalar => bail!(
                "cannot convert ScalarImpl::{} to concrete type",
                other_scalar.get_ident()
            ),
        }
    }
}

impl From<char> for ScalarImpl {
    fn from(c: char) -> Self {
        Self::Utf8(c.to_string().into())
    }
}

impl From<&[u8]> for ScalarImpl {
    fn from(s: &[u8]) -> Self {
        Self::Bytea(s.into())
    }
}

impl From<JsonbRef<'_>> for ScalarImpl {
    fn from(jsonb: JsonbRef<'_>) -> Self {
        Self::Jsonb(jsonb.to_owned_scalar())
    }
}

impl From<VariantRef<'_>> for ScalarImpl {
    fn from(variant: VariantRef<'_>) -> Self {
        Self::Variant(variant.to_owned_scalar())
    }
}

impl<T: PrimitiveArrayItemType> From<Vec<T>> for ScalarImpl {
    fn from(v: Vec<T>) -> Self {
        Self::List(v.into_iter().collect())
    }
}

impl<T: PrimitiveArrayItemType> From<Vec<Option<T>>> for ScalarImpl {
    fn from(v: Vec<Option<T>>) -> Self {
        Self::List(v.into_iter().collect())
    }
}

impl From<Vec<String>> for ScalarImpl {
    fn from(v: Vec<String>) -> Self {
        Self::List(v.iter().map(|s| s.as_str()).collect())
    }
}

impl From<Vec<u8>> for ScalarImpl {
    fn from(v: Vec<u8>) -> Self {
        Self::Bytea(v.into())
    }
}

impl From<Bytes> for ScalarImpl {
    fn from(v: Bytes) -> Self {
        Self::Bytea(v.as_ref().into())
    }
}

impl From<ListRef<'_>> for ScalarImpl {
    fn from(list: ListRef<'_>) -> Self {
        Self::List(list.to_owned_scalar())
    }
}

impl ScalarImpl {
    /// Creates a scalar from pgwire "BINARY" format.
    ///
    /// The counterpart of [`to_binary::ToBinary`].
    pub fn from_binary(bytes: &Bytes, data_type: &DataType) -> Result<Self, BoxedError> {
        let res = match data_type {
            DataType::Varchar => Self::Utf8(String::from_sql(&Type::VARCHAR, bytes)?.into()),
            DataType::Bytea => Self::Bytea(Vec::<u8>::from_sql(&Type::BYTEA, bytes)?.into()),
            DataType::Boolean => Self::Bool(bool::from_sql(&Type::BOOL, bytes)?),
            DataType::Int16 => Self::Int16(i16::from_sql(&Type::INT2, bytes)?),
            DataType::Int32 => Self::Int32(i32::from_sql(&Type::INT4, bytes)?),
            DataType::Int64 => Self::Int64(i64::from_sql(&Type::INT8, bytes)?),
            DataType::Serial => Self::Serial(Serial::from(i64::from_sql(&Type::INT8, bytes)?)),
            DataType::Float32 => Self::Float32(f32::from_sql(&Type::FLOAT4, bytes)?.into()),
            DataType::Float64 => Self::Float64(f64::from_sql(&Type::FLOAT8, bytes)?.into()),
            DataType::Decimal => {
                Self::Decimal(rust_decimal::Decimal::from_sql(&Type::NUMERIC, bytes)?.into())
            }
            DataType::Date => Self::Date(chrono::NaiveDate::from_sql(&Type::DATE, bytes)?.into()),
            DataType::Time => Self::Time(chrono::NaiveTime::from_sql(&Type::TIME, bytes)?.into()),
            DataType::Timestamp => {
                Self::Timestamp(chrono::NaiveDateTime::from_sql(&Type::TIMESTAMP, bytes)?.into())
            }
            DataType::Timestamptz => Self::Timestamptz(
                chrono::DateTime::<chrono::Utc>::from_sql(&Type::TIMESTAMPTZ, bytes)?.into(),
            ),
            DataType::Interval => Self::Interval(Interval::from_sql(&Type::INTERVAL, bytes)?),
            DataType::Jsonb => Self::Jsonb(
                JsonbVal::value_deserialize(bytes)
                    .ok_or_else(|| "invalid value of Jsonb".to_owned())?,
            ),
            // pgwire binary parameters are untrusted and must be re-canonicalized.
            DataType::Variant => Self::Variant(VariantVal::from_serialized_untrusted(bytes)?),
            DataType::Int256 => Self::Int256(Int256::from_binary(bytes)?),
            DataType::Vector(_) | DataType::Struct(_) | DataType::List(_) | DataType::Map(_) => {
                return Err(format!("unsupported data type: {}", data_type).into());
            }
        };
        Ok(res)
    }

    /// Creates a scalar from pgwire "TEXT" format.
    ///
    /// The counterpart of [`ToText`].
    pub fn from_text(s: &str, data_type: &DataType) -> Result<Self, BoxedError> {
        Ok(match data_type {
            DataType::Boolean => str_to_bool(s)?.into(),
            DataType::Int16 => i16::from_str(s)?.into(),
            DataType::Int32 => i32::from_str(s)?.into(),
            DataType::Int64 => i64::from_str(s)?.into(),
            DataType::Int256 => Int256::from_str(s)?.into(),
            DataType::Serial => Serial::from(i64::from_str(s)?).into(),
            DataType::Decimal => Decimal::from_str(s)?.into(),
            DataType::Float32 => F32::from_str(s)?.into(),
            DataType::Float64 => F64::from_str(s)?.into(),
            DataType::Varchar => s.into(),
            DataType::Date => Date::from_str(s)?.into(),
            DataType::Timestamp => Timestamp::from_str(s)?.into(),
            // We only handle the case with timezone here, and leave the implicit session timezone case
            // for later phase.
            DataType::Timestamptz => Timestamptz::from_str(s)?.into(),
            DataType::Time => Time::from_str(s)?.into(),
            DataType::Interval => Interval::from_str(s)?.into(),
            DataType::List(_) => ListValue::from_str(s, data_type)?.into(),
            DataType::Struct(st) => StructValue::from_str(s, st)?.into(),
            DataType::Jsonb => JsonbVal::from_str(s)?.into(),
            DataType::Variant => VariantVal::from_str(s)?.into(),
            DataType::Bytea => {
                let mut buf = Vec::new();
                str_to_bytea(s, &mut buf)?;
                buf.into()
            }
            DataType::Vector(size) => VectorVal::from_text(s, *size)?.into(),
            DataType::Map(_m) => return Err("map from text is not supported".into()),
        })
    }

    pub fn from_text_for_test(s: &str, data_type: &DataType) -> Result<Self, BoxedError> {
        Ok(match data_type {
            DataType::Map(map_type) => MapValue::from_str_for_test(s, map_type)?.into(),
            _ => ScalarImpl::from_text(s, data_type)?,
        })
    }
}

impl From<ScalarRefImpl<'_>> for ScalarImpl {
    fn from(scalar_ref: ScalarRefImpl<'_>) -> Self {
        scalar_ref.into_scalar_impl()
    }
}

impl<'a> From<&'a ScalarImpl> for ScalarRefImpl<'a> {
    fn from(scalar: &'a ScalarImpl) -> Self {
        scalar.as_scalar_ref_impl()
    }
}

impl ScalarImpl {
    /// Converts [`ScalarImpl`] to [`ScalarRefImpl`]
    pub fn as_scalar_ref_impl(&self) -> ScalarRefImpl<'_> {
        dispatch_scalar_variants!(self, inner, { inner.as_scalar_ref().into() })
    }
}

impl ScalarRefImpl<'_> {
    /// Converts [`ScalarRefImpl`] to [`ScalarImpl`]
    pub fn into_scalar_impl(self) -> ScalarImpl {
        dispatch_scalar_ref_variants!(self, inner, { inner.to_owned_scalar().into() })
    }
}

impl Hash for ScalarImpl {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        dispatch_scalar_variants!(self, inner, { inner.as_scalar_ref().hash_scalar(state) })
    }
}

impl Hash for ScalarRefImpl<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        dispatch_scalar_ref_variants!(self, inner, { inner.hash_scalar(state) })
    }
}

/// Feeds the raw scalar reference of `datum` to the given `state`, which should behave the same
/// as [`crate::array::Array::hash_at`], where NULL value will be carefully handled.
///
/// **FIXME**: the result of this function might be different from [`std::hash::Hash`] due to the
/// type alias of `DatumRef = Option<_>`, we should manually implement [`std::hash::Hash`] for
/// [`DatumRef`] in the future when it becomes a newtype. (#477)
#[inline(always)]
pub fn hash_datum(datum: impl ToDatumRef, state: &mut impl std::hash::Hasher) {
    match datum.to_datum_ref() {
        Some(scalar_ref) => scalar_ref.hash(state),
        None => NULL_VAL_FOR_HASH.hash(state),
    }
}

impl ScalarRefImpl<'_> {
    pub fn binary_format(&self, data_type: &DataType) -> to_binary::Result<Bytes> {
        use self::to_binary::ToBinary;
        self.to_binary_with_type(data_type)
    }

    pub fn text_format(&self, data_type: &DataType) -> String {
        self.to_text_with_type(data_type)
    }

    /// Serialize the scalar into the `memcomparable` format.
    pub fn serialize(
        &self,
        ser: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        match self {
            Self::Int16(v) => v.serialize(ser)?,
            Self::Int32(v) => v.serialize(ser)?,
            Self::Int64(v) => v.serialize(ser)?,
            Self::Serial(v) => v.serialize(ser)?,
            Self::Float32(v) => v.serialize(ser)?,
            Self::Float64(v) => v.serialize(ser)?,
            Self::Utf8(v) => v.serialize(ser)?,
            Self::Bytea(v) => ser.serialize_bytes(v)?,
            Self::Bool(v) => v.serialize(ser)?,
            Self::Decimal(v) => ser.serialize_decimal((*v).into())?,
            Self::Interval(v) => v.serialize(ser)?,
            Self::Date(v) => v.0.num_days_from_ce().serialize(ser)?,
            Self::Timestamp(v) => {
                v.0.and_utc().timestamp().serialize(&mut *ser)?;
                v.0.and_utc().timestamp_subsec_nanos().serialize(ser)?;
            }
            Self::Timestamptz(v) => v.serialize(ser)?,
            Self::Time(v) => {
                v.0.num_seconds_from_midnight().serialize(&mut *ser)?;
                v.0.nanosecond().serialize(ser)?;
            }
            Self::Int256(v) => v.memcmp_serialize(ser)?,
            Self::Jsonb(v) => v.memcmp_serialize(ser)?,
            Self::Variant(v) => v.memcmp_serialize(ser)?,
            Self::Struct(v) => v.memcmp_serialize(ser)?,
            Self::List(v) => v.memcmp_serialize(ser)?,
            Self::Map(v) => v.memcmp_serialize(ser)?,
            Self::Vector(v) => v.memcmp_serialize(ser)?,
        };
        Ok(())
    }
}

impl ScalarImpl {
    /// Serialize the scalar into the `memcomparable` format.
    pub fn serialize(
        &self,
        ser: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        self.as_scalar_ref_impl().serialize(ser)
    }

    /// Deserialize the scalar from the `memcomparable` format.
    pub fn deserialize(
        ty: &DataType,
        de: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        use DataType as Ty;
        Ok(match ty {
            Ty::Int16 => Self::Int16(i16::deserialize(de)?),
            Ty::Int32 => Self::Int32(i32::deserialize(de)?),
            Ty::Int64 => Self::Int64(i64::deserialize(de)?),
            Ty::Int256 => Self::Int256(Int256::memcmp_deserialize(de)?),
            Ty::Serial => Self::Serial(Serial::from(i64::deserialize(de)?)),
            Ty::Float32 => Self::Float32(f32::deserialize(de)?.into()),
            Ty::Float64 => Self::Float64(f64::deserialize(de)?.into()),
            Ty::Varchar => Self::Utf8(Box::<str>::deserialize(de)?),
            Ty::Bytea => Self::Bytea(serde_bytes::ByteBuf::deserialize(de)?.into_vec().into()),
            Ty::Boolean => Self::Bool(bool::deserialize(de)?),
            Ty::Decimal => Self::Decimal(de.deserialize_decimal()?.into()),
            Ty::Interval => Self::Interval(Interval::deserialize(de)?),
            Ty::Time => Self::Time({
                let secs = u32::deserialize(&mut *de)?;
                let nano = u32::deserialize(de)?;
                Time::with_secs_nano(secs, nano)
                    .map_err(|e| memcomparable::Error::Message(e.to_report_string()))?
            }),
            Ty::Timestamp => Self::Timestamp({
                let secs = i64::deserialize(&mut *de)?;
                let nsecs = u32::deserialize(de)?;
                Timestamp::with_secs_nsecs(secs, nsecs)
                    .map_err(|e| memcomparable::Error::Message(e.to_report_string()))?
            }),
            Ty::Timestamptz => Self::Timestamptz(Timestamptz::deserialize(de)?),
            Ty::Date => Self::Date({
                let days = i32::deserialize(de)?;
                Date::with_days_since_ce(days)
                    .map_err(|e| memcomparable::Error::Message(e.to_report_string()))?
            }),
            Ty::Jsonb => Self::Jsonb(JsonbVal::memcmp_deserialize(de)?),
            Ty::Variant => Self::Variant(VariantVal::memcmp_deserialize(de)?),
            Ty::Struct(t) => StructValue::memcmp_deserialize(t.types(), de)?.to_scalar_value(),
            Ty::List(t) => ListValue::memcmp_deserialize(t, de)?.to_scalar_value(),
            Ty::Map(t) => MapValue::memcmp_deserialize(t, de)?.to_scalar_value(),
            Ty::Vector(dimension) => {
                VectorVal::memcmp_deserialize(*dimension, de)?.to_scalar_value()
            }
        })
    }

    pub fn as_integral(&self) -> i64 {
        match self {
            Self::Int16(v) => *v as i64,
            Self::Int32(v) => *v as i64,
            Self::Int64(v) => *v,
            _ => panic!(
                "Can't convert ScalarImpl::{} to a integral",
                self.get_ident()
            ),
        }
    }
}

/// Returns whether the `literal` matches the `data_type`.
pub fn literal_type_match(data_type: &DataType, literal: Option<&ScalarImpl>) -> bool {
    match literal {
        None => true,
        Some(scalar) => scalar_ref_type_match(data_type, scalar.as_scalar_ref_impl()),
    }
}

/// Returns whether the scalar ref matches the `data_type`.
///
/// This is a lightweight "shape check" intended for callers that need to avoid panics on
/// malformed input. For nested types, it checks element/field types recursively.
pub fn scalar_ref_type_match(data_type: &DataType, scalar: ScalarRefImpl<'_>) -> bool {
    match (data_type, scalar) {
        (DataType::List(list_type), ScalarRefImpl::List(v)) => {
            v.elem_type().equals_datatype(list_type.elem())
        }
        (DataType::Map(map_type), ScalarRefImpl::Map(v)) => v
            .inner()
            .elem_type()
            .equals_datatype(&map_type.clone().into_struct()),
        (DataType::Vector(size), ScalarRefImpl::Vector(v)) => v.dimension() == *size,
        (DataType::Struct(struct_type), ScalarRefImpl::Struct(v)) => {
            struct_ref_type_match(struct_type, v)
        }

        _ => {
            macro_rules! matches {
                ($( { $data_type:ident, $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty, $array:ty, $builder:ty }),*) => {
                    match (data_type, scalar) {
                        $(
                            (DataType::$data_type { .. }, ScalarRefImpl::$variant_name(_)) => true,
                            (DataType::$data_type { .. }, _) => false, // keep exhaustive over DataType variants
                        )*
                    }
                }
            }
            for_all_variants! { matches }
        }
    }
}

/// Returns whether the `datum` matches the `data_type`.
#[inline(always)]
pub fn datum_ref_type_match(data_type: &DataType, datum: DatumRef<'_>) -> bool {
    match datum {
        None => true,
        Some(scalar) => scalar_ref_type_match(data_type, scalar),
    }
}

fn struct_ref_type_match(expected: &StructType, value: StructRef<'_>) -> bool {
    match value {
        StructRef::Indexed { arr, .. } => {
            // `StructRef::Indexed` comes with a `StructArray`, whose type can be compared directly.
            crate::array::Array::data_type(arr).equals_datatype(&DataType::Struct(expected.clone()))
        }
        StructRef::ValueRef { val } => {
            let fields = val.fields();
            if fields.len() != expected.len() {
                return false;
            }
            expected
                .types()
                .zip_eq_fast(fields.iter())
                .all(|(ty, datum)| datum_ref_type_match(ty, datum.to_datum_ref()))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::hash::{BuildHasher, Hasher};

    use strum::IntoEnumIterator;

    use super::*;
    use crate::util::hash_util::Crc32FastBuilder;

    #[test]
    fn test_size() {
        use static_assertions::const_assert_eq;

        use crate::array::*;

        macro_rules! assert_item_size_eq {
            ($array:ty, $size:literal) => {
                const_assert_eq!(std::mem::size_of::<<$array as Array>::OwnedItem>(), $size);
            };
        }

        assert_item_size_eq!(StructArray, 16); // Box<[Datum]>
        assert_item_size_eq!(ListArray, 8); // Box<ArrayImpl>
        assert_item_size_eq!(Utf8Array, 16); // Box<str>
        assert_item_size_eq!(IntervalArray, 16);
        assert_item_size_eq!(TimestampArray, 12);

        // TODO: try to reduce the memory usage of `Decimal`, `ScalarImpl` and `Datum`.
        assert_item_size_eq!(DecimalArray, 20);

        const_assert_eq!(std::mem::size_of::<ScalarImpl>(), 24);
        const_assert_eq!(std::mem::size_of::<ScalarRefImpl<'_>>(), 24);
        const_assert_eq!(std::mem::size_of::<Datum>(), 24);
        const_assert_eq!(std::mem::size_of::<StructType>(), 8);
        const_assert_eq!(std::mem::size_of::<DataType>(), 16);
    }

    #[test]
    fn test_data_type_display() {
        let d: DataType =
            StructType::new(vec![("i", DataType::Int32), ("j", DataType::Varchar)]).into();
        assert_eq!(
            format!("{}", d),
            "struct<i integer, j character varying>".to_owned()
        );
    }

    #[test]
    fn test_hash_implementation() {
        fn test(datum: Datum, data_type: DataType) {
            assert!(literal_type_match(&data_type, datum.as_ref()));

            let mut builder = data_type.create_array_builder(6);
            for _ in 0..3 {
                builder.append_null();
                builder.append(&datum);
            }
            let array = builder.finish();

            let hash_from_array = {
                let mut state = Crc32FastBuilder.build_hasher();
                array.hash_at(3, &mut state);
                state.finish()
            };

            let hash_from_datum = {
                let mut state = Crc32FastBuilder.build_hasher();
                hash_datum(&datum, &mut state);
                state.finish()
            };

            let hash_from_datum_ref = {
                let mut state = Crc32FastBuilder.build_hasher();
                hash_datum(datum.to_datum_ref(), &mut state);
                state.finish()
            };

            assert_eq!(hash_from_array, hash_from_datum);
            assert_eq!(hash_from_datum, hash_from_datum_ref);
        }

        for name in DataTypeName::iter() {
            let (scalar, data_type) = match name {
                DataTypeName::Boolean => (ScalarImpl::Bool(true), DataType::Boolean),
                DataTypeName::Int16 => (ScalarImpl::Int16(233), DataType::Int16),
                DataTypeName::Int32 => (ScalarImpl::Int32(233333), DataType::Int32),
                DataTypeName::Int64 => (ScalarImpl::Int64(233333333333), DataType::Int64),
                DataTypeName::Int256 => (
                    ScalarImpl::Int256(233333333333_i64.into()),
                    DataType::Int256,
                ),
                DataTypeName::Serial => (ScalarImpl::Serial(233333333333.into()), DataType::Serial),
                DataTypeName::Float32 => (ScalarImpl::Float32(23.33.into()), DataType::Float32),
                DataTypeName::Float64 => (
                    ScalarImpl::Float64(23.333333333333.into()),
                    DataType::Float64,
                ),
                DataTypeName::Decimal => (
                    ScalarImpl::Decimal("233.33".parse().unwrap()),
                    DataType::Decimal,
                ),
                DataTypeName::Date => (
                    ScalarImpl::Date(Date::from_ymd_uncheck(2333, 3, 3)),
                    DataType::Date,
                ),
                DataTypeName::Varchar => (ScalarImpl::Utf8("233".into()), DataType::Varchar),
                DataTypeName::Bytea => (
                    ScalarImpl::Bytea("\\x233".as_bytes().into()),
                    DataType::Bytea,
                ),
                DataTypeName::Time => (
                    ScalarImpl::Time(Time::from_hms_uncheck(2, 3, 3)),
                    DataType::Time,
                ),
                DataTypeName::Timestamp => (
                    ScalarImpl::Timestamp(Timestamp::from_timestamp_uncheck(23333333, 2333)),
                    DataType::Timestamp,
                ),
                DataTypeName::Timestamptz => (
                    ScalarImpl::Timestamptz(Timestamptz::from_micros(233333333).unwrap()),
                    DataType::Timestamptz,
                ),
                DataTypeName::Interval => (
                    ScalarImpl::Interval(Interval::from_month_day_usec(2, 3, 3333)),
                    DataType::Interval,
                ),
                DataTypeName::Jsonb => (ScalarImpl::Jsonb(JsonbVal::null()), DataType::Jsonb),
                DataTypeName::Variant => {
                    (ScalarImpl::Variant(VariantVal::null()), DataType::Variant)
                }
                DataTypeName::Struct => (
                    ScalarImpl::Struct(StructValue::new(vec![
                        ScalarImpl::Int64(233).into(),
                        ScalarImpl::Float64(23.33.into()).into(),
                    ])),
                    DataType::Struct(StructType::new(vec![
                        ("a", DataType::Int64),
                        ("b", DataType::Float64),
                    ])),
                ),
                DataTypeName::List => (
                    ScalarImpl::List(ListValue::from_iter([233i64, 2333])),
                    DataType::Int64.list(),
                ),
                DataTypeName::Vector => (
                    ScalarImpl::Vector(VectorVal::from_iter(
                        (0..VectorVal::TEST_VECTOR_DIMENSION)
                            .map(|i| ((i + 1) as f32).try_into().unwrap()),
                    )),
                    DataType::Vector(VectorVal::TEST_VECTOR_DIMENSION),
                ),
                DataTypeName::Map => {
                    // map is not hashable
                    continue;
                }
            };

            test(Some(scalar), data_type.clone());
            test(None, data_type);
        }
    }

    #[test]
    fn test_data_type_from_str() {
        assert_eq!(DataType::from_str("bool").unwrap(), DataType::Boolean);
        assert_eq!(DataType::from_str("boolean").unwrap(), DataType::Boolean);
        assert_eq!(DataType::from_str("BOOL").unwrap(), DataType::Boolean);
        assert_eq!(DataType::from_str("BOOLEAN").unwrap(), DataType::Boolean);

        assert_eq!(DataType::from_str("int2").unwrap(), DataType::Int16);
        assert_eq!(DataType::from_str("smallint").unwrap(), DataType::Int16);
        assert_eq!(DataType::from_str("INT2").unwrap(), DataType::Int16);
        assert_eq!(DataType::from_str("SMALLINT").unwrap(), DataType::Int16);

        assert_eq!(DataType::from_str("int4").unwrap(), DataType::Int32);
        assert_eq!(DataType::from_str("integer").unwrap(), DataType::Int32);
        assert_eq!(DataType::from_str("int4").unwrap(), DataType::Int32);
        assert_eq!(DataType::from_str("INT4").unwrap(), DataType::Int32);
        assert_eq!(DataType::from_str("INTEGER").unwrap(), DataType::Int32);
        assert_eq!(DataType::from_str("INT").unwrap(), DataType::Int32);

        assert_eq!(DataType::from_str("int8").unwrap(), DataType::Int64);
        assert_eq!(DataType::from_str("bigint").unwrap(), DataType::Int64);
        assert_eq!(DataType::from_str("INT8").unwrap(), DataType::Int64);
        assert_eq!(DataType::from_str("BIGINT").unwrap(), DataType::Int64);

        assert_eq!(DataType::from_str("rw_int256").unwrap(), DataType::Int256);
        assert_eq!(DataType::from_str("RW_INT256").unwrap(), DataType::Int256);

        assert_eq!(DataType::from_str("float4").unwrap(), DataType::Float32);
        assert_eq!(DataType::from_str("real").unwrap(), DataType::Float32);
        assert_eq!(DataType::from_str("FLOAT4").unwrap(), DataType::Float32);
        assert_eq!(DataType::from_str("REAL").unwrap(), DataType::Float32);

        assert_eq!(DataType::from_str("float8").unwrap(), DataType::Float64);
        assert_eq!(
            DataType::from_str("double precision").unwrap(),
            DataType::Float64
        );
        assert_eq!(DataType::from_str("FLOAT8").unwrap(), DataType::Float64);
        assert_eq!(
            DataType::from_str("DOUBLE PRECISION").unwrap(),
            DataType::Float64
        );

        assert_eq!(DataType::from_str("decimal").unwrap(), DataType::Decimal);
        assert_eq!(DataType::from_str("DECIMAL").unwrap(), DataType::Decimal);
        assert_eq!(DataType::from_str("numeric").unwrap(), DataType::Decimal);
        assert_eq!(DataType::from_str("NUMERIC").unwrap(), DataType::Decimal);

        assert_eq!(DataType::from_str("date").unwrap(), DataType::Date);
        assert_eq!(DataType::from_str("DATE").unwrap(), DataType::Date);

        assert_eq!(DataType::from_str("varchar").unwrap(), DataType::Varchar);
        assert_eq!(DataType::from_str("VARCHAR").unwrap(), DataType::Varchar);

        assert_eq!(DataType::from_str("time").unwrap(), DataType::Time);
        assert_eq!(
            DataType::from_str("time without time zone").unwrap(),
            DataType::Time
        );
        assert_eq!(DataType::from_str("TIME").unwrap(), DataType::Time);
        assert_eq!(
            DataType::from_str("TIME WITHOUT TIME ZONE").unwrap(),
            DataType::Time
        );

        assert_eq!(
            DataType::from_str("timestamp").unwrap(),
            DataType::Timestamp
        );
        assert_eq!(
            DataType::from_str("timestamp without time zone").unwrap(),
            DataType::Timestamp
        );
        assert_eq!(
            DataType::from_str("TIMESTAMP").unwrap(),
            DataType::Timestamp
        );
        assert_eq!(
            DataType::from_str("TIMESTAMP WITHOUT TIME ZONE").unwrap(),
            DataType::Timestamp
        );

        assert_eq!(
            DataType::from_str("timestamptz").unwrap(),
            DataType::Timestamptz
        );
        assert_eq!(
            DataType::from_str("timestamp with time zone").unwrap(),
            DataType::Timestamptz
        );
        assert_eq!(
            DataType::from_str("TIMESTAMPTZ").unwrap(),
            DataType::Timestamptz
        );
        assert_eq!(
            DataType::from_str("TIMESTAMP WITH TIME ZONE").unwrap(),
            DataType::Timestamptz
        );

        assert_eq!(DataType::from_str("interval").unwrap(), DataType::Interval);
        assert_eq!(DataType::from_str("INTERVAL").unwrap(), DataType::Interval);

        assert_eq!(
            DataType::from_str("int2[]").unwrap(),
            DataType::Int16.list()
        );
        assert_eq!(DataType::from_str("int[]").unwrap(), DataType::Int32.list());
        assert_eq!(
            DataType::from_str("int8[]").unwrap(),
            DataType::Int64.list()
        );
        assert_eq!(
            DataType::from_str("float4[]").unwrap(),
            DataType::Float32.list()
        );
        assert_eq!(
            DataType::from_str("float8[]").unwrap(),
            DataType::Float64.list()
        );
        assert_eq!(
            DataType::from_str("decimal[]").unwrap(),
            DataType::Decimal.list()
        );
        assert_eq!(
            DataType::from_str("varchar[]").unwrap(),
            DataType::Varchar.list()
        );
        assert_eq!(DataType::from_str("variant").unwrap(), DataType::Variant);
        assert_eq!(DataType::from_str("VARIANT").unwrap(), DataType::Variant);
        assert_eq!(
            DataType::from_str("variant[]").unwrap(),
            DataType::Variant.list()
        );

        assert_eq!(DataType::from_str("date[]").unwrap(), DataType::Date.list());
        assert_eq!(DataType::from_str("time[]").unwrap(), DataType::Time.list());
        assert_eq!(
            DataType::from_str("timestamp[]").unwrap(),
            DataType::Timestamp.list()
        );
        assert_eq!(
            DataType::from_str("timestamptz[]").unwrap(),
            DataType::Timestamptz.list()
        );
        assert_eq!(
            DataType::from_str("interval[]").unwrap(),
            DataType::Interval.list()
        );

        assert_eq!(
            DataType::from_str("record").unwrap(),
            DataType::Struct(StructType::unnamed(vec![]))
        );
        assert_eq!(
            DataType::from_str("struct<a int4, b varchar>").unwrap(),
            DataType::Struct(StructType::new(vec![
                ("a", DataType::Int32),
                ("b", DataType::Varchar)
            ]))
        );
    }

    #[test]
    fn test_can_alter() {
        let cannots = [
            (DataType::Int32, None),
            (DataType::Int32.list(), None),
            (
                MapType::from_kv(DataType::Varchar, DataType::Int32.list()).into(),
                None,
            ),
            (
                StructType::new([("a", DataType::Int32)]).into(),
                Some(false),
            ),
            (
                MapType::from_kv(
                    DataType::Varchar,
                    StructType::new([("a", DataType::Int32)]).into(),
                )
                .into(),
                Some(false),
            ),
        ];
        for (cannot, why) in cannots {
            assert_eq!(cannot.can_alter(), why, "{cannot:?}");
        }

        let cans = [
            StructType::new([("a", DataType::Int32), ("b", DataType::Int32.list())])
                .with_ids([ColumnId::new(1), ColumnId::new(2)])
                .into(),
            DataType::list(DataType::Struct(
                StructType::new([("a", DataType::Int32)]).with_ids([ColumnId::new(1)]),
            )),
            MapType::from_kv(
                DataType::Varchar,
                StructType::new([("a", DataType::Int32)])
                    .with_ids([ColumnId::new(1)])
                    .into(),
            )
            .into(),
        ];
        for can in cans {
            assert_eq!(can.can_alter(), Some(true), "{can:?}");
        }
    }
}
