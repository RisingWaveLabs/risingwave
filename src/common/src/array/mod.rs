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

//! `Array` defines all in-memory representations of vectorized execution framework.

pub mod arrow;
mod bool_array;
pub mod bytes_array;
mod chrono_array;
mod data_chunk;
pub mod data_chunk_iter;
mod decimal_array;
pub mod error;
pub mod interval_array;
mod iterator;
mod jsonb_array;
pub mod list_array;
mod map_array;
mod num256_array;
mod primitive_array;
mod proto_reader;
pub mod stream_chunk;
pub mod stream_chunk_builder;
mod stream_chunk_iter;
pub mod stream_record;
pub mod struct_array;
mod utf8_array;
mod variant_array;
mod vector_array;

use std::convert::From;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub use bool_array::{BoolArray, BoolArrayBuilder};
pub use bytes_array::*;
pub use chrono_array::{
    DateArray, DateArrayBuilder, TimeArray, TimeArrayBuilder, TimestampArray,
    TimestampArrayBuilder, TimestamptzArray, TimestamptzArrayBuilder,
};
pub use data_chunk::{DataChunk, DataChunkTestExt};
pub use data_chunk_iter::RowRef;
pub use decimal_array::{DecimalArray, DecimalArrayBuilder};
pub use interval_array::{IntervalArray, IntervalArrayBuilder};
pub use iterator::ArrayIterator;
pub use jsonb_array::{JsonbArray, JsonbArrayBuilder};
pub use list_array::{ListArray, ListArrayBuilder, ListRef, ListValue, ListWrite, ListWriter};
pub use map_array::{MapArray, MapArrayBuilder, MapRef, MapValue};
use paste::paste;
pub use primitive_array::{PrimitiveArray, PrimitiveArrayBuilder, PrimitiveArrayItemType};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::data::PbArray;
pub use stream_chunk::{Op, StreamChunk, StreamChunkTestExt};
pub use stream_chunk_builder::StreamChunkBuilder;
pub use struct_array::{StructArray, StructArrayBuilder, StructRef, StructValue};
pub use utf8_array::*;
pub use variant_array::{VariantArray, VariantArrayBuilder};
pub use vector_array::{
    Finite32, VECTOR_AS_LIST_TYPE, VECTOR_DISTANCE_TYPE, VECTOR_ITEM_TYPE, VectorArray,
    VectorArrayBuilder, VectorDistanceType, VectorItemType, VectorRef, VectorVal,
};

pub use self::error::ArrayError;
pub use crate::array::num256_array::{Int256Array, Int256ArrayBuilder};
use crate::bitmap::Bitmap;
use crate::types::*;
use crate::{dispatch_array_builder_variants, dispatch_array_variants, for_all_variants};
pub type ArrayResult<T> = Result<T, ArrayError>;

pub type I64Array = PrimitiveArray<i64>;
pub type I32Array = PrimitiveArray<i32>;
pub type I16Array = PrimitiveArray<i16>;
pub type F64Array = PrimitiveArray<F64>;
pub type F32Array = PrimitiveArray<F32>;
pub type SerialArray = PrimitiveArray<Serial>;

pub type I64ArrayBuilder = PrimitiveArrayBuilder<i64>;
pub type I32ArrayBuilder = PrimitiveArrayBuilder<i32>;
pub type I16ArrayBuilder = PrimitiveArrayBuilder<i16>;
pub type F64ArrayBuilder = PrimitiveArrayBuilder<F64>;
pub type F32ArrayBuilder = PrimitiveArrayBuilder<F32>;
pub type SerialArrayBuilder = PrimitiveArrayBuilder<Serial>;

// alias for expr macros
pub type ArrayImplBuilder = ArrayBuilderImpl;

/// The hash source for `None` values when hashing an item.
pub(crate) const NULL_VAL_FOR_HASH: u32 = 0xfffffff0;

/// A trait over all array builders.
///
/// `ArrayBuilder` is a trait over all builders. You could build an array with
/// `append` with the help of `ArrayBuilder` trait. The `append` function always
/// accepts reference to an element if it is not primitive. e.g. for `PrimitiveArray`,
/// you could do `builder.append(Some(1))`. For `Utf8Array`, you must do
/// `builder.append(Some("xxx"))`. Note that you don't need to construct a `String`.
///
/// The associated type `ArrayType` is the type of the corresponding array. It is the
/// return type of `finish`.
pub trait ArrayBuilder: Send + Sync + Sized + 'static {
    /// Corresponding `Array` of this builder, which is reciprocal to `ArrayBuilder`.
    type ArrayType: Array<Builder = Self>;

    /// Create a new builder with `capacity`.
    /// TODO: remove this function from the trait. Let it be methods of each concrete builders.
    fn new(capacity: usize) -> Self;

    /// # Panics
    /// Panics if `meta`'s type mismatches with the array type.
    fn with_type(capacity: usize, ty: DataType) -> Self;

    /// Append a value multiple times.
    ///
    /// This should be more efficient than calling `append` multiple times.
    fn append_n(&mut self, n: usize, value: Option<<Self::ArrayType as Array>::RefItem<'_>>);

    /// Append a value to builder.
    fn append(&mut self, value: Option<<Self::ArrayType as Array>::RefItem<'_>>) {
        self.append_n(1, value);
    }

    /// Append an owned value to builder.
    fn append_owned(&mut self, value: Option<<Self::ArrayType as Array>::OwnedItem>) {
        let value = value.as_ref().map(|s| s.as_scalar_ref());
        self.append(value)
    }

    fn append_null(&mut self) {
        self.append(None)
    }

    /// Append an array to builder.
    fn append_array(&mut self, other: &Self::ArrayType);

    /// Pop an element from the builder.
    ///
    /// It's used in `rollback` in source parser.
    ///
    /// # Returns
    ///
    /// Returns `None` if there is no elements in the builder.
    fn pop(&mut self) -> Option<()>;

    /// Append an element in another array into builder.
    fn append_array_element(&mut self, other: &Self::ArrayType, idx: usize) {
        self.append(other.value_at(idx));
    }

    /// Return the number of elements in the builder.
    fn len(&self) -> usize;

    /// Return `true` if the array has a length of 0.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Finish build and return a new array.
    fn finish(self) -> Self::ArrayType;
}

/// A trait over all array.
///
/// `Array` must be built with an `ArrayBuilder`. The array trait provides several
/// unified interface on an array, like `len`, `value_at` and `iter`.
///
/// The `Builder` associated type is the builder for this array.
///
/// The `Iter` associated type is the iterator of this array. And the `RefItem` is
/// the item you could retrieve from this array.
/// For example, `PrimitiveArray` could return an `Option<u32>`, and `Utf8Array` will
/// return an `Option<&str>`.
///
/// In some cases, we will need to store owned data. For example, when aggregating min
/// and max, we need to store current maximum in the aggregator. In this case, we
/// could use `A::OwnedItem` in aggregator struct.
pub trait Array:
    std::fmt::Debug + Send + Sync + Sized + 'static + Into<ArrayImpl> + EstimateSize
{
    /// A reference to item in array, as well as return type of `value_at`, which is
    /// reciprocal to `Self::OwnedItem`.
    type RefItem<'a>: ScalarRef<'a, ScalarType = Self::OwnedItem>
    where
        Self: 'a;

    /// Owned type of item in array, which is reciprocal to `Self::RefItem`.
    type OwnedItem: Clone
        + std::fmt::Debug
        + EstimateSize
        + for<'a> Scalar<ScalarRefType<'a> = Self::RefItem<'a>>;

    /// Corresponding builder of this array, which is reciprocal to `Array`.
    type Builder: ArrayBuilder<ArrayType = Self>;

    /// Retrieve a reference to value regardless of whether it is null
    /// without checking the index boundary.
    ///
    /// The returned value for NULL values is the default value.
    ///
    /// # Safety
    ///
    /// Index must be within the bounds.
    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_>;

    /// Retrieve a reference to value.
    #[inline]
    fn value_at(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        if !self.is_null(idx) {
            // Safety: the above `is_null` check ensures that the index is valid.
            Some(unsafe { self.raw_value_at_unchecked(idx) })
        } else {
            None
        }
    }

    /// # Safety
    ///
    /// Retrieve a reference to value without checking the index boundary.
    #[inline]
    unsafe fn value_at_unchecked(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        unsafe {
            if !self.is_null_unchecked(idx) {
                Some(self.raw_value_at_unchecked(idx))
            } else {
                None
            }
        }
    }

    /// Number of items of array.
    fn len(&self) -> usize;

    /// Get iterator of current array.
    fn iter(&self) -> ArrayIterator<'_, Self> {
        ArrayIterator::new(self)
    }

    /// Get raw iterator of current array.
    ///
    /// The raw iterator simply iterates values without checking the null bitmap.
    /// The returned value for NULL values is undefined.
    fn raw_iter(&self) -> impl ExactSizeIterator<Item = Self::RefItem<'_>> {
        (0..self.len()).map(|i| unsafe { self.raw_value_at_unchecked(i) })
    }

    /// Serialize to protobuf
    fn to_protobuf(&self) -> PbArray;

    /// Get the null `Bitmap` from `Array`.
    fn null_bitmap(&self) -> &Bitmap;

    /// Get the owned null `Bitmap` from `Array`.
    fn into_null_bitmap(self) -> Bitmap;

    /// Check if an element is `null` or not.
    fn is_null(&self, idx: usize) -> bool {
        !self.null_bitmap().is_set(idx)
    }

    /// # Safety
    ///
    /// The unchecked version of `is_null`, ignore index out of bound check. It is
    /// the caller's responsibility to ensure the index is valid.
    unsafe fn is_null_unchecked(&self, idx: usize) -> bool {
        unsafe { !self.null_bitmap().is_set_unchecked(idx) }
    }

    fn set_bitmap(&mut self, bitmap: Bitmap);

    /// Feed the value at `idx` into the given [`Hasher`].
    #[inline(always)]
    fn hash_at<H: Hasher>(&self, idx: usize, state: &mut H) {
        // We use a default implementation for all arrays for now, as retrieving the reference
        // should be lightweight.
        if let Some(value) = self.value_at(idx) {
            value.hash_scalar(state);
        } else {
            NULL_VAL_FOR_HASH.hash(state);
        }
    }

    fn hash_vec<H: Hasher>(&self, hashers: &mut [H], vis: &Bitmap) {
        assert_eq!(hashers.len(), self.len());
        for idx in vis.iter_ones() {
            self.hash_at(idx, &mut hashers[idx]);
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn create_builder(&self, capacity: usize) -> Self::Builder {
        Self::Builder::with_type(capacity, self.data_type())
    }

    fn data_type(&self) -> DataType;

    /// Converts the array into an [`ArrayRef`].
    fn into_ref(self) -> ArrayRef {
        Arc::new(self.into())
    }
}

/// Implement `compact_vis` on array, which removes element according to `visibility`.
#[easy_ext::ext(ArrayCompactVisExt)]
impl<A: Array> A {
    /// Select some elements from `Array` based on `visibility` bitmap.
    /// `cardinality` is only used to decide capacity of the new `Array`.
    pub fn compact_vis(&self, visibility: &Bitmap, cardinality: usize) -> Self {
        let mut builder = A::Builder::with_type(cardinality, self.data_type());
        for idx in visibility.iter_ones() {
            // SAFETY(value_at_unchecked): the idx is always in bound.
            unsafe {
                builder.append(self.value_at_unchecked(idx));
            }
        }
        builder.finish()
    }
}

/// Define `ArrayImpl` with macro.
macro_rules! array_impl_enum {
    ( $( { $data_type:ident, $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty, $array:ty, $builder:ty } ),*) => {
        /// `ArrayImpl` embeds all possible array in `array` module.
        #[derive(Debug, Clone, EstimateSize)]
        pub enum ArrayImpl {
            $( $variant_name($array) ),*
        }
    };
}

for_all_variants! { array_impl_enum }

// We cannot put the From implementations in impl_convert,
// because then we can't prove for all `T: PrimitiveArrayItemType`,
// it's implemented.

impl<T: PrimitiveArrayItemType> From<PrimitiveArray<T>> for ArrayImpl {
    fn from(arr: PrimitiveArray<T>) -> Self {
        T::erase_array_type(arr)
    }
}

impl From<Int256Array> for ArrayImpl {
    fn from(arr: Int256Array) -> Self {
        Self::Int256(arr)
    }
}

impl From<BoolArray> for ArrayImpl {
    fn from(arr: BoolArray) -> Self {
        Self::Bool(arr)
    }
}

impl From<Utf8Array> for ArrayImpl {
    fn from(arr: Utf8Array) -> Self {
        Self::Utf8(arr)
    }
}

impl From<JsonbArray> for ArrayImpl {
    fn from(arr: JsonbArray) -> Self {
        Self::Jsonb(arr)
    }
}

impl From<VariantArray> for ArrayImpl {
    fn from(arr: VariantArray) -> Self {
        Self::Variant(arr)
    }
}

impl From<StructArray> for ArrayImpl {
    fn from(arr: StructArray) -> Self {
        Self::Struct(arr)
    }
}

impl From<ListArray> for ArrayImpl {
    fn from(arr: ListArray) -> Self {
        Self::List(arr)
    }
}

impl From<VectorArray> for ArrayImpl {
    fn from(arr: VectorArray) -> Self {
        Self::Vector(arr)
    }
}

impl From<BytesArray> for ArrayImpl {
    fn from(arr: BytesArray) -> Self {
        Self::Bytea(arr)
    }
}

impl From<MapArray> for ArrayImpl {
    fn from(arr: MapArray) -> Self {
        Self::Map(arr)
    }
}

/// `impl_convert` implements several conversions for `Array` and `ArrayBuilder`.
/// * `ArrayImpl -> &Array` with `impl.as_int16()`.
/// * `ArrayImpl -> Array` with `impl.into_int16()`.
/// * `&ArrayImpl -> &Array` with `From` trait.
/// * `ArrayImpl -> Array` with `From` trait.
/// * `ArrayBuilder -> ArrayBuilderImpl` with `From` trait.
macro_rules! impl_convert {
    ($( { $data_type:ident, $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty, $array:ty, $builder:ty } ),*) => {
        $(
            paste! {
                impl ArrayImpl {
                    /// # Panics
                    ///
                    /// Panics if type mismatches.
                    pub fn [<as_ $suffix_name>](&self) -> &$array {
                        match self {
                            Self::$variant_name(array) => array,
                            other_array => panic!("cannot convert ArrayImpl::{} to concrete type {}", other_array.get_ident(), stringify!($variant_name))
                        }
                    }

                    /// # Panics
                    ///
                    /// Panics if type mismatches.
                    pub fn [<into_ $suffix_name>](self) -> $array {
                        match self {
                            Self::$variant_name(array) => array,
                            other_array => panic!("cannot convert ArrayImpl::{} to concrete type {}", other_array.get_ident(), stringify!($variant_name))
                        }
                    }
                }

                // FIXME: panic in From here is not proper.
                impl <'a> From<&'a ArrayImpl> for &'a $array {
                    fn from(array: &'a ArrayImpl) -> Self {
                        match array {
                            ArrayImpl::$variant_name(inner) => inner,
                            other_array => panic!("cannot convert ArrayImpl::{} to concrete type {}", other_array.get_ident(), stringify!($variant_name))
                        }
                    }
                }

                impl From<ArrayImpl> for $array {
                    fn from(array: ArrayImpl) -> Self {
                        match array {
                            ArrayImpl::$variant_name(inner) => inner,
                            other_array => panic!("cannot convert ArrayImpl::{} to concrete type {}", other_array.get_ident(), stringify!($variant_name))
                        }
                    }
                }

                impl From<$builder> for ArrayBuilderImpl {
                    fn from(builder: $builder) -> Self {
                        Self::$variant_name(builder)
                    }
                }
            }
        )*
    };
}

for_all_variants! { impl_convert }

/// Define `ArrayImplBuilder` with macro.
macro_rules! array_builder_impl_enum {
    ($( { $data_type:ident, $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty, $array:ty, $builder:ty } ),*) => {
        /// `ArrayBuilderImpl` embeds all possible array in `array` module.
        #[derive(Debug, Clone, EstimateSize)]
        pub enum ArrayBuilderImpl {
            $( $variant_name($builder) ),*
        }
    };
}

for_all_variants! { array_builder_impl_enum }

/// Implements all `ArrayBuilder` functions with `for_all_variant`.
impl ArrayBuilderImpl {
    pub fn with_type(capacity: usize, ty: DataType) -> Self {
        ty.create_array_builder(capacity)
    }

    pub fn append_array(&mut self, other: &ArrayImpl) {
        dispatch_array_builder_variants!(self, inner, { inner.append_array(other.into()) })
    }

    pub fn append_null(&mut self) {
        dispatch_array_builder_variants!(self, inner, { inner.append(None) })
    }

    pub fn append_n_null(&mut self, n: usize) {
        dispatch_array_builder_variants!(self, inner, { inner.append_n(n, None) })
    }

    /// Append a [`Datum`] or [`DatumRef`] multiple times,
    /// panicking if the datum's type does not match the array builder's type.
    pub fn append_n(&mut self, n: usize, datum: impl ToDatumRef) {
        match datum.to_datum_ref() {
            None => dispatch_array_builder_variants!(self, inner, { inner.append_n(n, None) }),

            Some(scalar_ref) => {
                dispatch_array_builder_variants!(self, inner, [I = VARIANT_NAME], {
                    inner.append_n(
                        n,
                        Some(scalar_ref.try_into().unwrap_or_else(|_| {
                            panic!(
                                "type mismatch, array builder type: {}, scalar type: {}",
                                I,
                                scalar_ref.get_ident()
                            )
                        })),
                    )
                })
            }
        }
    }

    /// Append a [`Datum`] or [`DatumRef`], return error while type not match.
    pub fn append(&mut self, datum: impl ToDatumRef) {
        self.append_n(1, datum);
    }

    pub fn append_array_element(&mut self, other: &ArrayImpl, idx: usize) {
        dispatch_array_builder_variants!(self, inner, {
            inner.append_array_element(other.into(), idx)
        })
    }

    pub fn pop(&mut self) -> Option<()> {
        dispatch_array_builder_variants!(self, inner, { inner.pop() })
    }

    pub fn finish(self) -> ArrayImpl {
        dispatch_array_builder_variants!(self, inner, { inner.finish().into() })
    }

    pub fn get_ident(&self) -> &'static str {
        dispatch_array_builder_variants!(self, [I = VARIANT_NAME], { I })
    }

    pub fn len(&self) -> usize {
        dispatch_array_builder_variants!(self, inner, { inner.len() })
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl ArrayImpl {
    /// Number of items in array.
    pub fn len(&self) -> usize {
        dispatch_array_variants!(self, inner, { inner.len() })
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the null `Bitmap` of the array.
    pub fn null_bitmap(&self) -> &Bitmap {
        dispatch_array_variants!(self, inner, { inner.null_bitmap() })
    }

    pub fn into_null_bitmap(self) -> Bitmap {
        dispatch_array_variants!(self, inner, { inner.into_null_bitmap() })
    }

    pub fn to_protobuf(&self) -> PbArray {
        dispatch_array_variants!(self, inner, { inner.to_protobuf() })
    }

    pub fn hash_at<H: Hasher>(&self, idx: usize, state: &mut H) {
        dispatch_array_variants!(self, inner, { inner.hash_at(idx, state) })
    }

    pub fn hash_vec<H: Hasher>(&self, hashers: &mut [H], vis: &Bitmap) {
        dispatch_array_variants!(self, inner, { inner.hash_vec(hashers, vis) })
    }

    /// Select some elements from `Array` based on `visibility` bitmap.
    pub fn compact_vis(&self, visibility: &Bitmap, cardinality: usize) -> Self {
        dispatch_array_variants!(self, inner, {
            inner.compact_vis(visibility, cardinality).into()
        })
    }

    pub fn get_ident(&self) -> &'static str {
        dispatch_array_variants!(self, [I = VARIANT_NAME], { I })
    }

    /// Get the enum-wrapped `Datum` out of the `Array`.
    pub fn datum_at(&self, idx: usize) -> Datum {
        self.value_at(idx).to_owned_datum()
    }

    /// If the array only have one single element, convert it to `Datum`.
    pub fn to_datum(&self) -> Datum {
        assert_eq!(self.len(), 1);
        self.datum_at(0)
    }

    /// Get the enum-wrapped `ScalarRefImpl` out of the `Array`.
    pub fn value_at(&self, idx: usize) -> DatumRef<'_> {
        dispatch_array_variants!(self, inner, {
            inner.value_at(idx).map(ScalarRefImpl::from)
        })
    }

    /// # Safety
    ///
    /// This function is unsafe because it does not check the validity of `idx`. It is caller's
    /// responsibility to ensure the validity of `idx`.
    ///
    /// Unsafe version of getting the enum-wrapped `ScalarRefImpl` out of the `Array`.
    pub unsafe fn value_at_unchecked(&self, idx: usize) -> DatumRef<'_> {
        unsafe {
            dispatch_array_variants!(self, inner, {
                inner.value_at_unchecked(idx).map(ScalarRefImpl::from)
            })
        }
    }

    pub fn set_bitmap(&mut self, bitmap: Bitmap) {
        dispatch_array_variants!(self, inner, { inner.set_bitmap(bitmap) })
    }

    pub fn create_builder(&self, capacity: usize) -> ArrayBuilderImpl {
        dispatch_array_variants!(self, inner, { inner.create_builder(capacity).into() })
    }

    /// Returns the `DataType` of this array.
    pub fn data_type(&self) -> DataType {
        dispatch_array_variants!(self, inner, { inner.data_type() })
    }

    pub fn into_ref(self) -> ArrayRef {
        Arc::new(self)
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = DatumRef<'_>> + ExactSizeIterator {
        (0..self.len()).map(|i| self.value_at(i))
    }
}

pub type ArrayRef = Arc<ArrayImpl>;

impl PartialEq for ArrayImpl {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl Eq for ArrayImpl {}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::util::iter_util::ZipEqFast;

    fn filter<'a, A, F>(data: &'a A, pred: F) -> ArrayResult<A>
    where
        A: Array + 'a,
        F: Fn(Option<A::RefItem<'a>>) -> bool,
    {
        let mut builder = A::Builder::with_type(data.len(), data.data_type());
        for i in 0..data.len() {
            if pred(data.value_at(i)) {
                builder.append(data.value_at(i));
            }
        }
        Ok(builder.finish())
    }

    #[test]
    fn test_filter() {
        let mut builder = PrimitiveArrayBuilder::<i32>::new(0);
        for i in 0..=60 {
            builder.append(Some(i));
        }
        let array = filter(&builder.finish(), |x| x.unwrap_or(0) >= 60).unwrap();
        assert_eq!(array.iter().collect::<Vec<Option<i32>>>(), vec![Some(60)]);
    }

    use num_traits::ops::checked::CheckedAdd;

    fn vec_add<T1, T2, T3>(
        a: &PrimitiveArray<T1>,
        b: &PrimitiveArray<T2>,
    ) -> ArrayResult<PrimitiveArray<T3>>
    where
        T1: PrimitiveArrayItemType,
        T2: PrimitiveArrayItemType,
        T3: PrimitiveArrayItemType + CheckedAdd + From<T1> + From<T2>,
    {
        let mut builder = PrimitiveArrayBuilder::<T3>::new(a.len());
        for (a, b) in a.iter().zip_eq_fast(b.iter()) {
            let item = match (a, b) {
                (Some(a), Some(b)) => Some(T3::from(a) + T3::from(b)),
                _ => None,
            };
            builder.append(item);
        }
        Ok(builder.finish())
    }

    #[test]
    fn test_vectorized_add() {
        let mut builder = PrimitiveArrayBuilder::<i32>::new(0);
        for i in 0..=60 {
            builder.append(Some(i));
        }
        let array1 = builder.finish();

        let mut builder = PrimitiveArrayBuilder::<i16>::new(0);
        for i in 0..=60 {
            builder.append(Some(i as i16));
        }
        let array2 = builder.finish();

        let final_array = vec_add(&array1, &array2).unwrap() as PrimitiveArray<i64>;

        assert_eq!(final_array.len(), array1.len());
        for (idx, data) in final_array.iter().enumerate() {
            assert_eq!(data, Some(idx as i64 * 2));
        }
    }
}

#[cfg(test)]
mod test_util {
    use std::hash::{BuildHasher, Hasher};

    use super::Array;
    use crate::bitmap::Bitmap;
    use crate::util::iter_util::ZipEqFast;

    pub fn hash_finish<H: Hasher>(hashers: &[H]) -> Vec<u64> {
        hashers
            .iter()
            .map(|hasher| hasher.finish())
            .collect::<Vec<u64>>()
    }

    pub fn test_hash<H: BuildHasher, A: Array>(arrs: Vec<A>, expects: Vec<u64>, hasher_builder: H) {
        let len = expects.len();
        let mut states_scalar = Vec::with_capacity(len);
        states_scalar.resize_with(len, || hasher_builder.build_hasher());
        let mut states_vec = Vec::with_capacity(len);
        states_vec.resize_with(len, || hasher_builder.build_hasher());

        arrs.iter().for_each(|arr| {
            for (i, state) in states_scalar.iter_mut().enumerate() {
                arr.hash_at(i, state)
            }
        });
        let vis = Bitmap::ones(len);
        arrs.iter()
            .for_each(|arr| arr.hash_vec(&mut states_vec[..], &vis));
        itertools::cons_tuples(
            expects
                .iter()
                .zip_eq_fast(hash_finish(&states_scalar[..]))
                .zip_eq_fast(hash_finish(&states_vec[..])),
        )
        .all(|(a, b, c)| *a == b && b == c);
    }
}
