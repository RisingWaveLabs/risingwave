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

//! This module contains implementation for hash key serialization for
//! hash-agg, hash-join, and perhaps other hash-based operators.
//!
//! There may be multiple columns in one row being combined and encoded into
//! one single hash key.
//! For example, `SELECT sum(t.a) FROM t GROUP BY t.b, t.c`, the hash keys
//! are encoded from both `t.b` and `t.c`. If `t.b="abc"` and `t.c=1`, the hashkey may be
//! encoded in certain format of `("abc", 1)`.

use std::default::Default;
use std::fmt::Debug;
use std::hash::{BuildHasher, Hasher};
use std::marker::PhantomData;

use bytes::{Buf, BufMut};
use chrono::{Datelike, Timelike};
use fixedbitset::FixedBitSet;
use risingwave_common_estimate_size::EstimateSize;
use smallbitset::Set64;
use static_assertions::const_assert_eq;

use crate::array::{ListValue, MapValue, StructValue, VectorVal};
use crate::types::{
    DataType, Date, Decimal, F32, F64, Int256, Int256Ref, JsonbVal, Scalar, ScalarRef,
    ScalarRefImpl, Serial, Time, Timestamp, Timestamptz, VariantVal,
};
use crate::util::hash_util::{Crc32FastBuilder, XxHash64Builder};
use crate::util::sort_util::OrderType;
use crate::util::{memcmp_encoding, value_encoding};

/// This is determined by the stack based data structure we use,
/// `StackNullBitmap`, which can store 64 bits at most.
pub static MAX_GROUP_KEYS_ON_STACK: usize = 64;

/// Null bitmap on heap.
/// We use this for the **edge case** where group key sizes are larger than 64.
/// This is because group key null bits cannot fit into a u64 on the stack
/// if they exceed 64 bits.
/// NOTE(kwannoel): This is not really optimized as it is an edge case.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq)]
pub struct HeapNullBitmap {
    inner: FixedBitSet,
}

impl HeapNullBitmap {
    fn with_capacity(n: usize) -> Self {
        HeapNullBitmap {
            inner: FixedBitSet::with_capacity(n),
        }
    }
}

/// Null Bitmap on stack.
/// This is specialized for the common case where group keys (<= 64).
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq)]
pub struct StackNullBitmap {
    inner: Set64,
}

const_assert_eq!(
    std::mem::size_of::<StackNullBitmap>(),
    std::mem::size_of::<u64>()
);

const_assert_eq!(
    std::mem::size_of::<HeapNullBitmap>(),
    std::mem::size_of::<usize>() * 3,
);

/// We use a trait for `NullBitmap` so we can parameterize structs on it.
/// This is because `NullBitmap` is used often, and we want it to occupy
/// the minimal stack space.
///
/// ### Example
/// ```rust
/// use risingwave_common::hash::{NullBitmap, StackNullBitmap};
/// struct A<B: NullBitmap> {
///     null_bitmap: B,
/// }
/// ```
///
/// Then `A<StackNullBitmap>` occupies 64 bytes,
/// and in cases which require it,
/// `A<HeapNullBitmap>` will occupy 4 * usize bytes (on 64 bit arch that would be 256 bytes).
pub trait NullBitmap: EstimateSize + Clone + PartialEq + Debug + Send + Sync + 'static {
    fn empty() -> Self;

    fn is_empty(&self) -> bool;

    fn set_true(&mut self, idx: usize);

    fn contains(&self, x: usize) -> bool;

    fn is_subset(&self, other: &Self) -> bool;

    fn from_bool_vec<T: AsRef<[bool]> + IntoIterator<Item = bool>>(value: T) -> Self;
}

impl NullBitmap for StackNullBitmap {
    fn empty() -> Self {
        StackNullBitmap {
            inner: Set64::empty(),
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn set_true(&mut self, idx: usize) {
        self.inner.add_inplace(idx);
    }

    fn contains(&self, x: usize) -> bool {
        self.inner.contains(x)
    }

    fn is_subset(&self, other: &Self) -> bool {
        other.inner.contains_all(self.inner)
    }

    fn from_bool_vec<T: AsRef<[bool]> + IntoIterator<Item = bool>>(value: T) -> Self {
        value.into()
    }
}

impl NullBitmap for HeapNullBitmap {
    fn empty() -> Self {
        HeapNullBitmap {
            inner: FixedBitSet::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn set_true(&mut self, idx: usize) {
        self.inner.grow(idx + 1);
        self.inner.insert(idx)
    }

    fn contains(&self, x: usize) -> bool {
        self.inner.contains(x)
    }

    fn is_subset(&self, other: &Self) -> bool {
        self.inner.is_subset(&other.inner)
    }

    fn from_bool_vec<T: AsRef<[bool]> + IntoIterator<Item = bool>>(value: T) -> Self {
        value.into()
    }
}

impl EstimateSize for StackNullBitmap {
    fn estimated_heap_size(&self) -> usize {
        0
    }
}

impl EstimateSize for HeapNullBitmap {
    fn estimated_heap_size(&self) -> usize {
        self.inner.estimated_heap_size()
    }
}

impl<T: AsRef<[bool]> + IntoIterator<Item = bool>> From<T> for StackNullBitmap {
    fn from(value: T) -> Self {
        let mut bitmap = StackNullBitmap::empty();
        for (idx, is_true) in value.into_iter().enumerate() {
            if is_true {
                bitmap.set_true(idx);
            }
        }
        bitmap
    }
}

impl<T: AsRef<[bool]> + IntoIterator<Item = bool>> From<T> for HeapNullBitmap {
    fn from(value: T) -> Self {
        let mut bitmap = HeapNullBitmap::with_capacity(value.as_ref().len());
        for (idx, is_true) in value.into_iter().enumerate() {
            if is_true {
                bitmap.set_true(idx);
            }
        }
        bitmap
    }
}

/// A wrapper for u64 hash result. Generic over the hasher.
#[derive(Default, Clone, Copy)]
pub struct HashCode<T: 'static + BuildHasher> {
    value: u64,
    _phantom: PhantomData<&'static T>,
}

impl<T: BuildHasher> Debug for HashCode<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashCode")
            .field("value", &self.value)
            .finish()
    }
}

impl<T: BuildHasher> PartialEq for HashCode<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T: BuildHasher> From<u64> for HashCode<T> {
    fn from(hash_code: u64) -> Self {
        Self {
            value: hash_code,
            _phantom: PhantomData,
        }
    }
}

impl<T: BuildHasher> HashCode<T> {
    pub fn value(&self) -> u64 {
        self.value
    }
}

/// Hash code from the `Crc32` hasher. Used for hash-shuffle exchange.
pub type Crc32HashCode = HashCode<Crc32FastBuilder>;
/// Hash code from the `XxHash64` hasher. Used for in-memory hash map cache.
pub type XxHash64HashCode = HashCode<XxHash64Builder>;

/// A special hasher designed for [`HashKey`], which stores a hash key from `HashKey::hash()` and
/// outputs it on `finish()`.
///
/// We need this because we compute hash keys in vectorized fashion, and we store them in this
/// hasher.
///
/// WARN: This should ONLY be used along with [`HashKey`].
///
/// [`HashKey`]: crate::hash::HashKey
#[derive(Default)]
pub struct PrecomputedHasher {
    hash_code: u64,
}

impl Hasher for PrecomputedHasher {
    fn finish(&self) -> u64 {
        self.hash_code
    }

    fn write_u64(&mut self, i: u64) {
        assert_eq!(self.hash_code, 0);
        self.hash_code = i;
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("must writes from HashKey with write_u64")
    }
}

#[derive(Default, Clone)]
pub struct PrecomputedBuildHasher;

impl BuildHasher for PrecomputedBuildHasher {
    type Hasher = PrecomputedHasher;

    fn build_hasher(&self) -> Self::Hasher {
        PrecomputedHasher::default()
    }
}

/// Extension of scalars to be serialized into hash keys.
///
/// The `exact_size` and `estimated_size` methods are used to estimate the size of the serialized
/// hash key, so that we can pre-allocate the buffer for it.
/// - override `exact_size` if the serialized size is known for this scalar type;
/// - override `estimated_size` if the serialized size varies for different values of this scalar
///   type, but we can estimate it.
///
/// NOTE: The hash key encoding algorithm needs to respect the implementation of `Hash` and `Eq` on
/// scalar types, which is exactly the same behavior of the data types under `GROUP BY` or
/// `PARTITION BY` in PostgreSQL. For example, `Decimal(1.0)` vs `Decimal(1.00)`, or `Interval(24
/// hour)` vs `Interval(1 day)` are considered equal here.
///
/// This means that...
/// - delegating to the value encoding can be **incorrect** for some types, as they reflect the
///   in-memory representation faithfully;
/// - delegating to the memcmp encoding should be always safe, but they put extra effort to also
///   preserve the property of `Ord`.
///
/// So for some scalar types we have to maintain a separate implementation here. For those that can
/// be delegated to other encoding algorithms, we can use macros of
/// `impl_memcmp_encoding_hash_key_serde!` and `impl_value_encoding_hash_key_serde!` here.
pub trait HashKeySer<'a>: ScalarRef<'a> {
    /// Serialize the scalar into the given buffer.
    fn serialize_into(self, buf: impl BufMut);

    /// Returns `Some` if the serialized size is known for this scalar type.
    fn exact_size() -> Option<usize> {
        None
    }

    /// Returns the estimated serialized size for this scalar.
    fn estimated_size(self) -> usize {
        Self::exact_size().unwrap_or(1) // use a default size of 1 if not known
        // this should never happen in practice as we always
        // implement one of these two methods
    }
}

/// The deserialization counterpart of [`HashKeySer`].
pub trait HashKeyDe: Scalar {
    fn deserialize(data_type: &DataType, buf: impl Buf) -> Self;
}

macro_rules! impl_value_encoding_hash_key_serde {
    ($owned_ty:ty) => {
        // TODO: extra boxing to `ScalarRefImpl` and encoding for `NonNull` tag is
        // unnecessary here. After we resolve them, we can make more types directly delegate
        // to this implementation.
        impl<'a> HashKeySer<'a> for <$owned_ty as Scalar>::ScalarRefType<'a> {
            fn serialize_into(self, mut buf: impl BufMut) {
                value_encoding::serialize_datum_into(Some(ScalarRefImpl::from(self)), &mut buf);
            }

            fn estimated_size(self) -> usize {
                value_encoding::estimate_serialize_datum_size(Some(ScalarRefImpl::from(self)))
            }
        }

        impl HashKeyDe for $owned_ty {
            fn deserialize(data_type: &DataType, buf: impl Buf) -> Self {
                let scalar = value_encoding::deserialize_datum(buf, data_type)
                    .expect("in-memory deserialize should never fail")
                    .expect("datum should never be NULL");

                // TODO: extra unboxing from `ScalarRefImpl` is unnecessary here.
                scalar.try_into().unwrap()
            }
        }
    };
}

macro_rules! impl_memcmp_encoding_hash_key_serde {
    ($owned_ty:ty) => {
        impl<'a> HashKeySer<'a> for <$owned_ty as Scalar>::ScalarRefType<'a> {
            fn serialize_into(self, buf: impl BufMut) {
                let mut serializer = memcomparable::Serializer::new(buf);
                // TODO: extra boxing to `ScalarRefImpl` and encoding for `NonNull` tag is
                // unnecessary here.
                memcmp_encoding::serialize_datum(
                    Some(ScalarRefImpl::from(self)),
                    OrderType::ascending(),
                    &mut serializer,
                )
                .expect("serialize should never fail");
            }

            // TODO: estimate size for memcmp encoding.
            fn estimated_size(self) -> usize {
                1
            }
        }

        impl HashKeyDe for $owned_ty {
            fn deserialize(data_type: &DataType, buf: impl Buf) -> Self {
                let mut deserializer = memcomparable::Deserializer::new(buf);
                let scalar = memcmp_encoding::deserialize_datum(
                    data_type,
                    OrderType::ascending(),
                    &mut deserializer,
                )
                .expect("in-memory deserialize should never fail")
                .expect("datum should never be NULL");

                // TODO: extra unboxing from `ScalarRefImpl` is unnecessary here.
                scalar.try_into().unwrap()
            }
        }
    };
}

impl HashKeySer<'_> for bool {
    fn serialize_into(self, mut buf: impl BufMut) {
        buf.put_u8(if self { 1 } else { 0 });
    }

    fn exact_size() -> Option<usize> {
        Some(1)
    }
}

impl HashKeyDe for bool {
    fn deserialize(_data_type: &DataType, mut buf: impl Buf) -> Self {
        buf.get_u8() == 1
    }
}

impl HashKeySer<'_> for i16 {
    fn serialize_into(self, mut buf: impl BufMut) {
        buf.put_i16_ne(self);
    }

    fn exact_size() -> Option<usize> {
        Some(2)
    }
}

impl HashKeyDe for i16 {
    fn deserialize(_data_type: &DataType, mut buf: impl Buf) -> Self {
        buf.get_i16_ne()
    }
}

impl HashKeySer<'_> for i32 {
    fn serialize_into(self, mut buf: impl BufMut) {
        buf.put_i32_ne(self);
    }

    fn exact_size() -> Option<usize> {
        Some(4)
    }
}

impl HashKeyDe for i32 {
    fn deserialize(_data_type: &DataType, mut buf: impl Buf) -> Self {
        buf.get_i32_ne()
    }
}

impl HashKeySer<'_> for i64 {
    fn serialize_into(self, mut buf: impl BufMut) {
        buf.put_i64_ne(self);
    }

    fn exact_size() -> Option<usize> {
        Some(8)
    }
}

impl HashKeyDe for i64 {
    fn deserialize(_data_type: &DataType, mut buf: impl Buf) -> Self {
        buf.get_i64_ne()
    }
}

impl<'a> HashKeySer<'a> for Int256Ref<'a> {
    fn serialize_into(self, mut buf: impl BufMut) {
        let b = self.to_ne_bytes();
        buf.put_slice(b.as_ref());
    }

    fn exact_size() -> Option<usize> {
        Some(32)
    }
}

impl HashKeyDe for Int256 {
    fn deserialize(_data_type: &DataType, mut buf: impl Buf) -> Self {
        let mut value = [0; 32];
        buf.copy_to_slice(&mut value);
        Self::from_ne_bytes(value)
    }
}

impl HashKeySer<'_> for Serial {
    fn serialize_into(self, mut buf: impl BufMut) {
        buf.put_i64_ne(self.as_row_id());
    }

    fn exact_size() -> Option<usize> {
        Some(8)
    }
}

impl HashKeyDe for Serial {
    fn deserialize(_data_type: &DataType, mut buf: impl Buf) -> Self {
        buf.get_i64_ne().into()
    }
}

impl HashKeySer<'_> for F32 {
    fn serialize_into(self, mut buf: impl BufMut) {
        buf.put_f32_ne(self.normalized().0);
    }

    fn exact_size() -> Option<usize> {
        Some(4)
    }
}

impl HashKeyDe for F32 {
    fn deserialize(_data_type: &DataType, mut buf: impl Buf) -> Self {
        buf.get_f32_ne().into()
    }
}

impl HashKeySer<'_> for F64 {
    fn serialize_into(self, mut buf: impl BufMut) {
        buf.put_f64_ne(self.normalized().0);
    }

    fn exact_size() -> Option<usize> {
        Some(8)
    }
}

impl HashKeyDe for F64 {
    fn deserialize(_data_type: &DataType, mut buf: impl Buf) -> Self {
        buf.get_f64_ne().into()
    }
}

impl HashKeySer<'_> for Decimal {
    fn serialize_into(self, mut buf: impl BufMut) {
        let b = Decimal::unordered_serialize(&self.normalize());
        buf.put_slice(b.as_ref());
    }

    fn exact_size() -> Option<usize> {
        Some(16)
    }
}

impl HashKeyDe for Decimal {
    fn deserialize(_data_type: &DataType, mut buf: impl Buf) -> Self {
        let mut value = [0; 16];
        buf.copy_to_slice(&mut value);
        Self::unordered_deserialize(value)
    }
}

impl HashKeySer<'_> for Date {
    fn serialize_into(self, mut buf: impl BufMut) {
        let b = self.0.num_days_from_ce().to_ne_bytes();
        buf.put_slice(b.as_ref());
    }

    fn exact_size() -> Option<usize> {
        Some(4)
    }
}

impl HashKeyDe for Date {
    fn deserialize(_data_type: &DataType, mut buf: impl Buf) -> Self {
        let days = buf.get_i32_ne();
        Date::with_days_since_ce(days).unwrap()
    }
}

impl HashKeySer<'_> for Timestamp {
    fn serialize_into(self, mut buf: impl BufMut) {
        buf.put_i64_ne(self.0.and_utc().timestamp());
        buf.put_u32_ne(self.0.and_utc().timestamp_subsec_nanos());
    }

    fn exact_size() -> Option<usize> {
        Some(12)
    }
}

impl HashKeyDe for Timestamp {
    fn deserialize(_data_type: &DataType, mut buf: impl Buf) -> Self {
        let secs = buf.get_i64_ne();
        let nsecs = buf.get_u32_ne();
        Timestamp::with_secs_nsecs(secs, nsecs).unwrap()
    }
}

impl HashKeySer<'_> for Time {
    fn serialize_into(self, mut buf: impl BufMut) {
        buf.put_u32_ne(self.0.num_seconds_from_midnight());
        buf.put_u32_ne(self.0.nanosecond());
    }

    fn exact_size() -> Option<usize> {
        Some(8)
    }
}

impl HashKeyDe for Time {
    fn deserialize(_data_type: &DataType, mut buf: impl Buf) -> Self {
        let secs = buf.get_u32_ne();
        let nano = buf.get_u32_ne();
        Time::with_secs_nano(secs, nano).unwrap()
    }
}

impl HashKeySer<'_> for Timestamptz {
    fn serialize_into(self, mut buf: impl BufMut) {
        buf.put_i64_ne(self.timestamp_micros());
    }

    fn exact_size() -> Option<usize> {
        Some(8)
    }
}

impl HashKeyDe for Timestamptz {
    fn deserialize(_data_type: &DataType, mut buf: impl Buf) -> Self {
        Timestamptz::from_micros_uncheck(buf.get_i64_ne())
    }
}

impl_value_encoding_hash_key_serde!(Box<str>);
impl_value_encoding_hash_key_serde!(Box<[u8]>);
impl_value_encoding_hash_key_serde!(JsonbVal);
impl_value_encoding_hash_key_serde!(VariantVal);

// It's possible there's `Decimal` or `Interval` in these composite types, so we currently always
// use the memcmp encoding for safety.
impl_memcmp_encoding_hash_key_serde!(StructValue);
impl_memcmp_encoding_hash_key_serde!(ListValue);
impl_memcmp_encoding_hash_key_serde!(VectorVal);
impl_memcmp_encoding_hash_key_serde!(MapValue);

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Arc;

    use itertools::Itertools;

    use super::*;
    use crate::array::{
        ArrayBuilder, ArrayBuilderImpl, ArrayImpl, BoolArray, DataChunk, DataChunkTestExt,
        DateArray, DecimalArray, F32Array, F64Array, I16Array, I32Array, I32ArrayBuilder, I64Array,
        TimeArray, TimestampArray, Utf8Array,
    };
    use crate::hash::{HashKey, Key16, Key32, Key64, Key128, Key256, KeySerialized};
    use crate::test_utils::rand_array::seed_rand_array_ref;
    use crate::types::Datum;

    #[derive(Hash, PartialEq, Eq)]
    struct Row(Vec<Datum>);

    fn generate_random_data_chunk() -> (DataChunk, Vec<DataType>) {
        let capacity = 128;
        let seed = 10244021u64;
        let columns = vec![
            seed_rand_array_ref::<BoolArray>(capacity, seed, 0.5),
            seed_rand_array_ref::<I16Array>(capacity, seed + 1, 0.5),
            seed_rand_array_ref::<I32Array>(capacity, seed + 2, 0.5),
            seed_rand_array_ref::<I64Array>(capacity, seed + 3, 0.5),
            seed_rand_array_ref::<F32Array>(capacity, seed + 4, 0.5),
            seed_rand_array_ref::<F64Array>(capacity, seed + 5, 0.5),
            seed_rand_array_ref::<DecimalArray>(capacity, seed + 6, 0.5),
            seed_rand_array_ref::<Utf8Array>(capacity, seed + 7, 0.5),
            seed_rand_array_ref::<DateArray>(capacity, seed + 8, 0.5),
            seed_rand_array_ref::<TimeArray>(capacity, seed + 9, 0.5),
            seed_rand_array_ref::<TimestampArray>(capacity, seed + 10, 0.5),
        ];
        let types = vec![
            DataType::Boolean,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal,
            DataType::Varchar,
            DataType::Date,
            DataType::Time,
            DataType::Timestamp,
        ];

        (DataChunk::new(columns, capacity), types)
    }

    fn do_test_serialize<K: HashKey, F>(column_indexes: Vec<usize>, data_gen: F)
    where
        F: FnOnce() -> DataChunk,
    {
        let data = data_gen();

        let mut actual_row_id_mapping = HashMap::<usize, Vec<usize>>::with_capacity(100);
        {
            let mut fast_hash_map =
                HashMap::<K, Vec<usize>, PrecomputedBuildHasher>::with_capacity_and_hasher(
                    100,
                    PrecomputedBuildHasher,
                );
            let keys = K::build_many(column_indexes.as_slice(), &data);

            for (row_id, key) in keys.into_iter().enumerate() {
                let row_ids = fast_hash_map.entry(key).or_default();
                row_ids.push(row_id);
            }

            for row_ids in fast_hash_map.values() {
                for row_id in row_ids {
                    actual_row_id_mapping.insert(*row_id, row_ids.clone());
                }
            }
        }

        let mut expected_row_id_mapping = HashMap::<usize, Vec<usize>>::with_capacity(100);
        {
            let mut normal_hash_map: HashMap<Row, Vec<usize>> = HashMap::with_capacity(100);
            for row_idx in 0..data.capacity() {
                let row = column_indexes
                    .iter()
                    .map(|col_idx| data.column_at(*col_idx))
                    .map(|col| col.datum_at(row_idx))
                    .collect::<Vec<Datum>>();

                normal_hash_map.entry(Row(row)).or_default().push(row_idx);
            }

            for row_ids in normal_hash_map.values() {
                for row_id in row_ids {
                    expected_row_id_mapping.insert(*row_id, row_ids.clone());
                }
            }
        }

        assert_eq!(expected_row_id_mapping, actual_row_id_mapping);
    }

    fn do_test_deserialize<K: HashKey, F>(column_indexes: Vec<usize>, data_gen: F)
    where
        F: FnOnce() -> (DataChunk, Vec<DataType>),
    {
        let (data, types) = data_gen();
        let keys = K::build_many(column_indexes.as_slice(), &data);

        let mut array_builders = column_indexes
            .iter()
            .map(|idx| data.columns()[*idx].create_builder(1024))
            .collect::<Vec<ArrayBuilderImpl>>();
        let key_types: Vec<_> = column_indexes
            .iter()
            .map(|idx| types[*idx].clone())
            .collect();

        keys.into_iter()
            .try_for_each(|k| k.deserialize_to_builders(&mut array_builders[..], &key_types))
            .expect("Failed to deserialize!");

        let result_arrays = array_builders
            .into_iter()
            .map(|array_builder| array_builder.finish())
            .collect::<Vec<ArrayImpl>>();

        for (ret_idx, col_idx) in column_indexes.iter().enumerate() {
            assert_eq!(&*data.columns()[*col_idx], &result_arrays[ret_idx]);
        }
    }

    fn do_test<K: HashKey, F>(column_indexes: Vec<usize>, data_gen: F)
    where
        F: FnOnce() -> (DataChunk, Vec<DataType>),
    {
        let (data, types) = data_gen();

        let data1 = data.clone();
        do_test_serialize::<K, _>(column_indexes.clone(), move || data1);
        do_test_deserialize::<K, _>(column_indexes, move || (data, types));
    }

    #[test]
    fn test_two_bytes_hash_key() {
        do_test::<Key16, _>(vec![1], generate_random_data_chunk);
    }

    #[test]
    fn test_four_bytes_hash_key() {
        do_test::<Key32, _>(vec![0, 1], generate_random_data_chunk);
        do_test::<Key32, _>(vec![2], generate_random_data_chunk);
        do_test::<Key32, _>(vec![4], generate_random_data_chunk);
    }

    #[test]
    fn test_eight_bytes_hash_key() {
        do_test::<Key64, _>(vec![1, 2], generate_random_data_chunk);
        do_test::<Key64, _>(vec![0, 1, 2], generate_random_data_chunk);
        do_test::<Key64, _>(vec![3], generate_random_data_chunk);
        do_test::<Key64, _>(vec![5], generate_random_data_chunk);
    }

    #[test]
    fn test_128_bits_hash_key() {
        do_test::<Key128, _>(vec![3, 5], generate_random_data_chunk);
        do_test::<Key128, _>(vec![6], generate_random_data_chunk);
    }

    #[test]
    fn test_256_bits_hash_key() {
        do_test::<Key256, _>(vec![3, 5, 6], generate_random_data_chunk);
        do_test::<Key256, _>(vec![3, 6], generate_random_data_chunk);
    }

    #[test]
    fn test_var_length_hash_key() {
        do_test::<KeySerialized, _>(vec![0, 7], generate_random_data_chunk);
    }

    fn generate_decimal_test_data() -> (DataChunk, Vec<DataType>) {
        let columns = vec![Arc::new(
            DecimalArray::from_iter([
                Some(Decimal::from_str("1.2").unwrap()),
                None,
                Some(Decimal::from_str("1.200").unwrap()),
                Some(Decimal::from_str("0.00").unwrap()),
                Some(Decimal::from_str("0.0").unwrap()),
            ])
            .into(),
        )];
        let types = vec![DataType::Decimal];

        (DataChunk::new(columns, 5), types)
    }

    #[test]
    fn test_decimal_hash_key_serialization() {
        do_test::<Key128, _>(vec![0], generate_decimal_test_data);
    }

    // Simple test to ensure a row <None, Some(2)> will be serialized and restored
    // losslessly.
    #[test]
    fn test_simple_hash_key_nullable_serde() {
        let keys = Key64::build_many(
            &[0, 1],
            &DataChunk::from_pretty(
                "i i
                 1 .
                 . 2",
            ),
        );

        let mut array_builders = [0, 1]
            .iter()
            .map(|_| ArrayBuilderImpl::Int32(I32ArrayBuilder::new(2)))
            .collect::<Vec<_>>();

        keys.into_iter().for_each(|k: Key64| {
            k.deserialize_to_builders(&mut array_builders[..], &[DataType::Int32, DataType::Int32])
                .unwrap()
        });

        let array = array_builders.pop().unwrap().finish();
        let i32_vec = array
            .iter()
            .map(|opt| opt.map(|s| s.into_int32()))
            .collect_vec();
        assert_eq!(i32_vec, vec![None, Some(2)]);
    }
}
