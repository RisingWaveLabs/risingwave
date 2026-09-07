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

use std::fmt;
use std::fmt::Display;
use std::hash::BuildHasher;
use std::sync::Arc;

use bytes::Bytes;
use either::Either;
use itertools::Itertools;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::data::PbDataChunk;

use super::{Array, ArrayImpl, ArrayRef, ArrayResult, StructArray};
use crate::array::ArrayBuilderImpl;
use crate::array::data_chunk_iter::RowRef;
use crate::bitmap::{Bitmap, BitmapBuilder};
use crate::field_generator::{FieldGeneratorImpl, VarcharProperty};
use crate::hash::HashCode;
use crate::row::Row;
use crate::types::{DataType, DatumRef, MapType, StructType, ToOwnedDatum, ToText};
use crate::util::chunk_coalesce::DataChunkBuilder;
use crate::util::hash_util::finalize_hashers;
use crate::util::iter_util::ZipEqFast;
use crate::util::value_encoding::{
    ValueRowSerializer, estimate_serialize_datum_size, serialize_datum_into,
    try_get_exact_serialize_datum_size,
};

/// [`DataChunk`] is a collection of Columns,
/// with a visibility mask for each row.
/// For instance, we could have a [`DataChunk`] of this format.
///
/// | v1 | v2 | v3 |
/// |----|----|----|
/// | 1  | a  | t  |
/// | 2  | b  | f  |
/// | 3  | c  | t  |
/// | 4  | d  | f  |
///
/// Our columns are v1, v2, v3.
/// Then, if the Visibility Mask hides rows 2 and 4,
/// We will only have these rows visible:
///
/// | v1 | v2 | v3 |
/// |----|----|----|
/// | 1  | a  | t  |
/// | 3  | c  | t  |
#[derive(Clone, PartialEq)]
#[must_use]
pub struct DataChunk {
    columns: Arc<[ArrayRef]>,
    visibility: Bitmap,
}

impl DataChunk {
    pub(crate) const PRETTY_TABLE_PRESET: &'static str = "||--+-++|    ++++++";

    /// Create a `DataChunk` with `columns` and visibility.
    ///
    /// The visibility can either be a `Bitmap` or a simple cardinality number.
    pub fn new(columns: Vec<ArrayRef>, visibility: impl Into<Bitmap>) -> Self {
        let visibility = visibility.into();
        let capacity = visibility.len();
        for column in &columns {
            assert_eq!(capacity, column.len());
        }

        DataChunk {
            columns: columns.into(),
            visibility,
        }
    }

    /// `new_dummy` creates a data chunk without columns but only a cardinality.
    pub fn new_dummy(cardinality: usize) -> Self {
        DataChunk {
            columns: Arc::new([]),
            visibility: Bitmap::ones(cardinality),
        }
    }

    /// Build a `DataChunk` with rows.
    ///
    /// Panics if the `rows` is empty.
    ///
    /// Should prefer using [`DataChunkBuilder`] instead to avoid unnecessary allocation
    /// of rows.
    pub fn from_rows(rows: &[impl Row], data_types: &[DataType]) -> Self {
        // `append_one_row` will cause the builder to finish immediately once capacity is met.
        // Hence, we allocate an extra row here, to avoid the builder finishing prematurely.
        // This just makes the code cleaner, since we can loop through all rows, and consume it finally.
        // TODO: introduce `new_unlimited` to decouple memory reservation from builder capacity.
        let mut builder = DataChunkBuilder::new(data_types.to_vec(), rows.len() + 1);

        for row in rows {
            let none = builder.append_one_row(row);
            debug_assert!(none.is_none());
        }

        builder.consume_all().expect("chunk should not be empty")
    }

    /// Return the next visible row index on or after `row_idx`.
    pub fn next_visible_row_idx(&self, row_idx: usize) -> Option<usize> {
        self.visibility.next_set_bit(row_idx)
    }

    pub fn into_parts(self) -> (Vec<ArrayRef>, Bitmap) {
        (self.columns.to_vec(), self.visibility)
    }

    pub fn into_parts_v2(self) -> (Arc<[ArrayRef]>, Bitmap) {
        (self.columns, self.visibility)
    }

    pub fn from_parts(columns: Arc<[ArrayRef]>, visibilities: Bitmap) -> Self {
        Self {
            columns,
            visibility: visibilities,
        }
    }

    pub fn dimension(&self) -> usize {
        self.columns.len()
    }

    // TODO(rc): shall we rename this to `visible_size`? I sometimes find this confused with `capacity`.
    /// `cardinality` returns the number of visible tuples
    pub fn cardinality(&self) -> usize {
        self.visibility.count_ones()
    }

    /// Returns whether this chunk contains at least one visible row.
    pub fn has_visible_rows(&self) -> bool {
        self.visibility.any()
    }

    // Compute the required permits of this chunk for rate limiting.
    pub fn rate_limit_permits(&self) -> u64 {
        self.cardinality() as _
    }

    // TODO(rc): shall we rename this to `size`?
    /// `capacity` returns physical length of any chunk column
    pub fn capacity(&self) -> usize {
        self.visibility.len()
    }

    pub fn selectivity(&self) -> f64 {
        if self.visibility.is_empty() {
            0.0
        } else if self.visibility.all() {
            1.0
        } else {
            self.visibility.count_ones() as f64 / self.visibility.len() as f64
        }
    }

    pub fn with_visibility(&self, visibility: impl Into<Bitmap>) -> Self {
        DataChunk {
            columns: self.columns.clone(),
            visibility: visibility.into(),
        }
    }

    pub fn visibility(&self) -> &Bitmap {
        &self.visibility
    }

    pub fn set_visibility(&mut self, visibility: Bitmap) {
        assert_eq!(visibility.len(), self.capacity());
        self.visibility = visibility;
    }

    /// Returns whether all rows in the chunk are visible, i.e., the chunk is compacted
    /// in terms of row visibility.
    pub fn is_vis_compacted(&self) -> bool {
        self.visibility.all()
    }

    pub fn column_at(&self, idx: usize) -> &ArrayRef {
        &self.columns[idx]
    }

    pub fn columns(&self) -> &[ArrayRef] {
        &self.columns
    }

    /// Returns the data types of all columns.
    pub fn data_types(&self) -> Vec<DataType> {
        self.columns.iter().map(|col| col.data_type()).collect()
    }

    /// Divides one chunk into two at an column index.
    ///
    /// # Panics
    ///
    /// Panics if `idx > columns.len()`.
    pub fn split_column_at(&self, idx: usize) -> (Self, Self) {
        let (left, right) = self.columns.split_at(idx);
        let left = DataChunk::new(left.to_vec(), self.visibility.clone());
        let right = DataChunk::new(right.to_vec(), self.visibility.clone());
        (left, right)
    }

    pub fn to_protobuf(&self) -> PbDataChunk {
        assert!(self.visibility.all(), "must be compacted before transfer");
        let mut proto = PbDataChunk {
            cardinality: self.cardinality() as u32,
            columns: Default::default(),
        };
        let column_ref = &mut proto.columns;
        for array in &*self.columns {
            column_ref.push(array.to_protobuf());
        }
        proto
    }

    /// Removes the invisible rows based on `visibility`. Returns a new compacted chunk
    /// with all rows visible.
    ///
    /// `compact_vis` has trade-offs:
    ///
    /// Cost:
    /// It has to rebuild the each column, meaning it will incur cost
    /// of copying over bytes from the original column array to the new one.
    ///
    /// Benefit:
    /// The main benefit is that the data chunk is smaller, taking up less memory.
    /// We can also save the cost of iterating over many hidden rows.
    pub fn compact_vis(self) -> Self {
        if self.visibility.all() {
            return self;
        }
        let cardinality = self.visibility.count_ones();
        let columns = self
            .columns
            .iter()
            .map(|col| {
                let array = col;
                array.compact_vis(&self.visibility, cardinality).into()
            })
            .collect::<Vec<_>>();
        Self::new(columns, Bitmap::ones(cardinality))
    }

    /// Scatter a compacted chunk to a new chunk with the given visibility.
    pub fn expand_vis(self, vis: Bitmap) -> Self {
        let mut uncompact_builders: Vec<_> = self
            .columns
            .iter()
            .map(|c| c.create_builder(vis.len()))
            .collect();
        let mut last_u = None;

        for (idx, u) in vis.iter_ones().enumerate() {
            // pad invisible rows with NULL
            let zeros = if let Some(last_u) = last_u {
                u - last_u - 1
            } else {
                u
            };
            for _ in 0..zeros {
                uncompact_builders
                    .iter_mut()
                    .for_each(|builder| builder.append_null());
            }
            uncompact_builders
                .iter_mut()
                .zip_eq_fast(self.columns.iter())
                .for_each(|(builder, c)| builder.append(c.datum_at(idx)));
            last_u = Some(u);
        }
        let zeros = if let Some(last_u) = last_u {
            vis.len() - last_u - 1
        } else {
            vis.len()
        };
        for _ in 0..zeros {
            uncompact_builders
                .iter_mut()
                .for_each(|builder| builder.append_null());
        }
        let array: Vec<_> = uncompact_builders
            .into_iter()
            .map(|builder| Arc::new(builder.finish()))
            .collect();

        Self::new(array, vis)
    }

    pub fn from_protobuf(proto: &PbDataChunk) -> ArrayResult<Self> {
        let mut columns = vec![];
        for any_col in proto.get_columns() {
            let cardinality = proto.get_cardinality() as usize;
            columns.push(ArrayImpl::from_protobuf(any_col, cardinality)?.into());
        }

        let chunk = DataChunk::new(columns, proto.cardinality as usize);
        Ok(chunk)
    }

    /// `rechunk` creates a new vector of data chunk whose size is `each_size_limit`.
    /// When the total cardinality of all the chunks is not evenly divided by the `each_size_limit`,
    /// the last new chunk will be the remainder.
    pub fn rechunk(chunks: &[DataChunk], each_size_limit: usize) -> ArrayResult<Vec<DataChunk>> {
        let Some(data_types) = chunks.first().map(|c| c.data_types()) else {
            return Ok(Vec::new());
        };

        let mut builder = DataChunkBuilder::new(data_types, each_size_limit);
        let mut outputs = Vec::new();

        for chunk in chunks {
            for output in builder.append_chunk(chunk.clone()) {
                outputs.push(output);
            }
        }
        if let Some(output) = builder.consume_all() {
            outputs.push(output);
        }

        Ok(outputs)
    }

    /// Compute hash values for each row. The number of the returning `HashCodes` is `self.capacity()`.
    /// When `skip_invisible_row` is true, the `HashCode` for the invisible rows is arbitrary.
    pub fn get_hash_values<H: BuildHasher>(
        &self,
        column_idxes: &[usize],
        hasher_builder: H,
    ) -> Vec<HashCode<H>> {
        let len = self.capacity();
        let mut states = Vec::with_capacity(len);
        states.resize_with(len, || hasher_builder.build_hasher());
        // Compute hash for the specified columns.
        for column_idx in column_idxes {
            let array = self.column_at(*column_idx);
            array.hash_vec(&mut states[..], self.visibility());
        }
        finalize_hashers(&states[..])
            .into_iter()
            .map(|hash_code| hash_code.into())
            .collect_vec()
    }

    /// Random access a tuple in a data chunk. Return in a row format.
    /// # Arguments
    /// * `pos` - Index of look up tuple
    /// * `RowRef` - Reference of data tuple
    /// * bool - whether this tuple is visible
    pub fn row_at(&self, pos: usize) -> (RowRef<'_>, bool) {
        let row = self.row_at_unchecked_vis(pos);
        let vis = self.visibility.is_set(pos);
        (row, vis)
    }

    /// Random access a tuple in a data chunk. Return in a row format.
    /// Note that this function do not return whether the row is visible.
    /// # Arguments
    /// * `pos` - Index of look up tuple
    pub fn row_at_unchecked_vis(&self, pos: usize) -> RowRef<'_> {
        RowRef::new(self, pos)
    }

    /// Returns a table-like text representation of the `DataChunk`.
    pub fn to_pretty(&self) -> impl Display + use<> {
        use comfy_table::Table;

        if self.cardinality() == 0 {
            return Either::Left("(empty)");
        }

        let mut table = Table::new();
        table.load_preset(Self::PRETTY_TABLE_PRESET);

        for row in self.rows() {
            let cells: Vec<_> = row
                .iter()
                .map(|v| {
                    match v {
                        None => "".to_owned(), // NULL
                        Some(scalar) => scalar.to_text(),
                    }
                })
                .collect();
            table.add_row(cells);
        }

        Either::Right(table)
    }

    /// Keep the specified columns and set the rest elements to null.
    ///
    /// # Example
    ///
    /// ```text
    /// i i i                            i i i
    /// 1 2 3  --> keep_columns([1]) --> . 2 .
    /// 4 5 6                            . 5 .
    /// ```
    pub fn keep_columns(&self, column_indices: &[usize]) -> Self {
        let capacity: usize = self.capacity();
        let columns = (self.columns.iter().enumerate())
            .map(|(i, column)| {
                if column_indices.contains(&i) {
                    column.clone()
                } else {
                    let mut builder = column.create_builder(capacity);
                    builder.append_n(capacity, None as DatumRef<'_>);
                    builder.finish().into()
                }
            })
            .collect();
        DataChunk {
            columns,
            visibility: self.visibility.clone(),
        }
    }

    /// Reorder (and possibly remove) columns.
    ///
    /// e.g. if `indices` is `[2, 1, 0]`, and the chunk contains column `[a, b, c]`, then the output
    /// will be `[c, b, a]`. If `indices` is [2, 0], then the output will be `[c, a]`.
    /// If the input mapping is identity mapping, no reorder will be performed.
    pub fn project(&self, indices: &[usize]) -> Self {
        Self {
            columns: indices.iter().map(|i| self.columns[*i].clone()).collect(),
            visibility: self.visibility.clone(),
        }
    }

    /// Reorder columns and set visibility.
    pub fn project_with_vis(&self, indices: &[usize], visibility: Bitmap) -> Self {
        assert_eq!(visibility.len(), self.capacity());
        Self {
            columns: indices.iter().map(|i| self.columns[*i].clone()).collect(),
            visibility,
        }
    }

    /// Reorder rows by indexes.
    pub fn reorder_rows(&self, indexes: &[usize]) -> Self {
        let mut array_builders: Vec<ArrayBuilderImpl> = self
            .columns
            .iter()
            .map(|col| col.create_builder(indexes.len()))
            .collect();
        for &i in indexes {
            for (builder, col) in array_builders.iter_mut().zip_eq_fast(self.columns.iter()) {
                builder.append(col.value_at(i));
            }
        }
        let columns = array_builders
            .into_iter()
            .map(|builder| builder.finish().into())
            .collect();
        DataChunk::new(columns, indexes.len())
    }

    /// Partition fixed size datums and variable length ones.
    /// ---
    /// In some cases, we have fixed size for the entire column,
    /// when the datatypes are fixed size or the datums are constants.
    /// As such we can compute the size for it just once for the column.
    ///
    /// Otherwise, for variable sized datatypes, such as `varchar`,
    /// we have to individually compute their sizes per row.
    fn partition_sizes(&self) -> (usize, Vec<&ArrayRef>) {
        let mut col_variable: Vec<&ArrayRef> = vec![];
        let mut row_len_fixed: usize = 0;
        for c in &*self.columns {
            if let Some(field_len) = try_get_exact_serialize_datum_size(c) {
                row_len_fixed += field_len;
            } else {
                col_variable.push(c);
            }
        }
        (row_len_fixed, col_variable)
    }

    unsafe fn compute_size_of_variable_cols_in_row(
        variable_cols: &[&ArrayRef],
        row_idx: usize,
    ) -> usize {
        unsafe {
            variable_cols
                .iter()
                .map(|col| estimate_serialize_datum_size(col.value_at_unchecked(row_idx)))
                .sum::<usize>()
        }
    }

    unsafe fn init_buffer(
        row_len_fixed: usize,
        variable_cols: &[&ArrayRef],
        row_idx: usize,
    ) -> Vec<u8> {
        unsafe {
            Vec::with_capacity(
                row_len_fixed + Self::compute_size_of_variable_cols_in_row(variable_cols, row_idx),
            )
        }
    }

    /// Serialize each row into value encoding bytes.
    ///
    /// The returned vector's size is `self.capacity()` and for the invisible row will give a empty
    /// bytes.
    // Note(bugen): should we exclude the invisible rows in the output so that the caller won't need
    // to handle visibility again?
    pub fn serialize(&self) -> Vec<Bytes> {
        let buffers = if !self.visibility.all() {
            let rows_num = self.visibility.len();
            let mut buffers: Vec<Vec<u8>> = vec![];
            let (row_len_fixed, col_variable) = self.partition_sizes();

            // First initialize buffer with the right size to avoid re-allocations
            for i in 0..rows_num {
                // SAFETY(value_at_unchecked): the idx is always in bound.
                unsafe {
                    if self.visibility.is_set_unchecked(i) {
                        buffers.push(Self::init_buffer(row_len_fixed, &col_variable, i));
                    } else {
                        buffers.push(vec![]);
                    }
                }
            }

            // Then do the actual serialization
            for c in &*self.columns {
                assert_eq!(c.len(), rows_num);
                for (i, buffer) in buffers.iter_mut().enumerate() {
                    // SAFETY(value_at_unchecked): the idx is always in bound.
                    unsafe {
                        if self.visibility.is_set_unchecked(i) {
                            serialize_datum_into(c.value_at_unchecked(i), buffer);
                        }
                    }
                }
            }
            buffers
        } else {
            let mut buffers: Vec<Vec<u8>> = vec![];
            let (row_len_fixed, col_variable) = self.partition_sizes();
            for i in 0..self.visibility.len() {
                unsafe {
                    buffers.push(Self::init_buffer(row_len_fixed, &col_variable, i));
                }
            }
            for c in &*self.columns {
                assert_eq!(c.len(), self.visibility.len());
                for (i, buffer) in buffers.iter_mut().enumerate() {
                    // SAFETY(value_at_unchecked): the idx is always in bound.
                    unsafe {
                        serialize_datum_into(c.value_at_unchecked(i), buffer);
                    }
                }
            }
            buffers
        };

        buffers.into_iter().map(|item| item.into()).collect_vec()
    }

    /// Serialize each row into bytes with given serializer.
    ///
    /// This is similar to `serialize` but it uses a custom serializer. Prefer `serialize` if
    /// possible since it might be more efficient due to columnar operations.
    pub fn serialize_with(&self, serializer: &impl ValueRowSerializer) -> Vec<Bytes> {
        let mut results = Vec::with_capacity(self.capacity());
        for row in self.rows_with_holes() {
            results.push(if let Some(row) = row {
                serializer.serialize(row).into()
            } else {
                Bytes::new()
            });
        }
        results
    }

    /// Estimate size of hash keys. Their indices in a row are indicated by `column_indices`.
    /// Size here refers to the number of u8s required to store the serialized datum.
    pub fn estimate_value_encoding_size(&self, column_indices: &[usize]) -> usize {
        if self.capacity() == 0 {
            0
        } else {
            column_indices
                .iter()
                .map(|idx| {
                    let datum = self.column_at(*idx).datum_at(0);
                    estimate_serialize_datum_size(datum)
                })
                .sum()
        }
    }
}

impl fmt::Debug for DataChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DataChunk {{ cardinality = {}, capacity = {}, data = \n{} }}",
            self.cardinality(),
            self.capacity(),
            self.to_pretty()
        )
    }
}

impl<'a> From<&'a StructArray> for DataChunk {
    fn from(array: &'a StructArray) -> Self {
        Self {
            columns: array.fields().cloned().collect(),
            visibility: Bitmap::ones(array.len()),
        }
    }
}

impl EstimateSize for DataChunk {
    fn estimated_heap_size(&self) -> usize {
        self.columns
            .iter()
            .map(|a| a.estimated_heap_size())
            .sum::<usize>()
            + self.visibility.estimated_heap_size()
    }
}

/// Test utilities for [`DataChunk`].
pub trait DataChunkTestExt {
    /// SEED for generating data chunk.
    const SEED: u64 = 0xFF67FEABBAEF76FF;

    /// Parse a chunk from string.
    ///
    /// # Format
    ///
    /// The first line is a header indicating the column types.
    /// The following lines indicate rows within the chunk.
    /// Each line starts with an operation followed by values.
    /// NULL values are represented as `.`.
    ///
    /// # Example
    /// ```
    /// use risingwave_common::array::{DataChunk, DataChunkTestExt};
    /// let chunk = DataChunk::from_pretty(
    ///     "I I I I      // type chars
    ///      2 5 . .      // '.' means NULL
    ///      2 5 2 6 D    // 'D' means deleted in visibility
    ///      . . 4 8      // ^ comments are ignored
    ///      . . 3 4",
    /// );
    ///
    /// // type chars:
    /// //     B: bool
    /// //     I: i64
    /// //     i: i32
    /// //     F: f64
    /// //     f: f32
    /// //     T: str
    /// //    TS: Timestamp
    /// //   SRL: Serial
    /// // <i,f>: struct
    /// ```
    fn from_pretty(s: &str) -> Self;

    /// Insert one invisible hole after every record.
    fn with_invisible_holes(self) -> Self
    where
        Self: Sized;

    /// Panic if the chunk is invalid.
    fn assert_valid(&self);

    /// Generate data chunk when supplied with `chunk_size` and column data types.
    fn gen_data_chunk(
        chunk_offset: usize,
        chunk_size: usize,
        data_types: &[DataType],
        string_properties: &VarcharProperty,
        visibility_ratio: f64,
    ) -> Self;

    /// Generate data chunks when supplied with `chunk_size` and column data types.
    fn gen_data_chunks(
        num_of_chunks: usize,
        chunk_size: usize,
        data_types: &[DataType],
        string_properties: &VarcharProperty,
        visibility_ratio: f64,
    ) -> Vec<Self>
    where
        Self: Sized;
}

impl DataChunkTestExt for DataChunk {
    fn from_pretty(s: &str) -> Self {
        use crate::types::ScalarImpl;
        fn parse_type(s: &str) -> DataType {
            if let Some(s) = s.strip_suffix("[]") {
                return DataType::list(parse_type(s));
            }

            // Special logic to support Map type in `DataChunk::from_pretty`.
            // Please refer to `src/expr/impl/src/scalar/map_filter.rs`.
            if let Some(inner) = s.strip_prefix("map<").and_then(|s| s.strip_suffix('>')) {
                let mut parts = inner.split(',');
                let key_type = parts.next().expect("Key type expected");
                let value_type = parts.next().expect("Value type expected");
                return DataType::Map(MapType::from_kv(
                    parse_type(key_type),
                    parse_type(value_type),
                ));
            }

            match s {
                "B" => DataType::Boolean,
                "I" => DataType::Int64,
                "i" => DataType::Int32,
                "F" => DataType::Float64,
                "f" => DataType::Float32,
                "TS" => DataType::Timestamp,
                "TZ" => DataType::Timestamptz,
                "T" => DataType::Varchar,
                "SRL" => DataType::Serial,
                "D" => DataType::Date,
                array if array.starts_with('<') && array.ends_with('>') => DataType::Struct(
                    StructType::unnamed(array[1..array.len() - 1].split(',').map(parse_type)),
                ),
                _ => todo!("unsupported type: {s:?}"),
            }
        }

        let mut lines = s.split('\n').filter(|l| !l.trim().is_empty());
        // initialize array builders from the first line
        let header = lines.next().unwrap().trim();
        let datatypes = header
            .split_ascii_whitespace()
            .take_while(|c| *c != "//")
            .map(parse_type)
            .collect::<Vec<_>>();
        let mut array_builders = datatypes
            .iter()
            .map(|ty| ty.create_array_builder(1))
            .collect::<Vec<_>>();
        let mut visibility = vec![];
        for line in lines {
            let mut token = line.trim().split_ascii_whitespace();
            // allow `zip` since `token` may longer than `array_builders`
            #[expect(clippy::disallowed_methods)]
            for ((builder, ty), val_str) in
                array_builders.iter_mut().zip(&datatypes).zip(&mut token)
            {
                let datum = match val_str {
                    "." => None,
                    "(empty)" => Some("".into()),
                    // `from_text_for_test` has support for Map.
                    _ => Some(ScalarImpl::from_text_for_test(val_str, ty).unwrap()),
                };
                builder.append(datum);
            }
            let visible = match token.next() {
                None | Some("//") => true,
                Some("D") => false,
                Some(t) => panic!("invalid token: {t:?}"),
            };
            visibility.push(visible);
        }
        let columns = array_builders
            .into_iter()
            .map(|builder| builder.finish().into())
            .collect();
        let vis = Bitmap::from_iter(visibility);
        let chunk = DataChunk::new(columns, vis);
        chunk.assert_valid();
        chunk
    }

    fn with_invisible_holes(self) -> Self
    where
        Self: Sized,
    {
        let (cols, vis) = self.into_parts();
        let n = vis.len();
        let mut new_vis = BitmapBuilder::with_capacity(n * 2);
        for b in vis.iter() {
            new_vis.append(b);
            new_vis.append(false);
        }
        let new_cols = cols
            .into_iter()
            .map(|col| {
                let arr = col;
                let mut builder = arr.create_builder(n * 2);
                for v in arr.iter() {
                    builder.append(v.to_owned_datum());
                    builder.append_null();
                }

                builder.finish().into()
            })
            .collect();
        let chunk = DataChunk::new(new_cols, new_vis.finish());
        chunk.assert_valid();
        chunk
    }

    fn assert_valid(&self) {
        let cols = self.columns();
        let vis = &self.visibility;
        let n = vis.len();
        for col in cols {
            assert_eq!(col.len(), n);
        }
    }

    fn gen_data_chunk(
        chunk_offset: usize,
        chunk_size: usize,
        data_types: &[DataType],
        varchar_properties: &VarcharProperty,
        visibility_percent: f64,
    ) -> Self {
        let vis = if visibility_percent == 0.0 {
            Bitmap::zeros(chunk_size)
        } else if visibility_percent == 1.0 {
            Bitmap::ones(chunk_size)
        } else {
            let mut rng = SmallRng::from_seed([0; 32]);
            let mut vis_builder = BitmapBuilder::with_capacity(chunk_size);
            for _i in 0..chunk_size {
                vis_builder.append(rng.random_bool(visibility_percent));
            }
            vis_builder.finish()
        };

        let mut columns = Vec::new();
        // Generate columns of this chunk.
        for data_type in data_types {
            let mut array_builder = data_type.create_array_builder(chunk_size);
            for j in 0..chunk_size {
                let offset = ((chunk_offset + 1) * (j + 1)) as u64;
                match data_type {
                    DataType::Varchar => {
                        let datum =
                            FieldGeneratorImpl::with_varchar(varchar_properties, Self::SEED)
                                .generate_datum(offset);
                        array_builder.append(&datum);
                    }
                    DataType::Timestamp => {
                        let datum =
                            FieldGeneratorImpl::with_timestamp(None, None, None, Self::SEED)
                                .expect("create timestamp generator should succeed")
                                .generate_datum(offset);
                        array_builder.append(datum);
                    }
                    DataType::Timestamptz => {
                        let datum =
                            FieldGeneratorImpl::with_timestamptz(None, None, None, Self::SEED)
                                .expect("create timestamptz generator should succeed")
                                .generate_datum(offset);
                        array_builder.append(datum);
                    }
                    _ if data_type.is_numeric() => {
                        let mut data_gen = FieldGeneratorImpl::with_number_random(
                            data_type.clone(),
                            None,
                            None,
                            Self::SEED,
                        )
                        .unwrap();
                        let datum = data_gen.generate_datum(offset);
                        array_builder.append(datum);
                    }
                    _ => todo!("unsupported type: {data_type:?}"),
                }
            }
            columns.push(array_builder.finish().into());
        }
        DataChunk::new(columns, vis)
    }

    fn gen_data_chunks(
        num_of_chunks: usize,
        chunk_size: usize,
        data_types: &[DataType],
        varchar_properties: &VarcharProperty,
        visibility_percent: f64,
    ) -> Vec<Self> {
        (0..num_of_chunks)
            .map(|i| {
                Self::gen_data_chunk(
                    i,
                    chunk_size,
                    data_types,
                    varchar_properties,
                    visibility_percent,
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::array::*;
    use crate::row::Row;

    #[test]
    fn test_rechunk() {
        let test_case = |num_chunks: usize, chunk_size: usize, new_chunk_size: usize| {
            let mut chunks = vec![];
            for chunk_idx in 0..num_chunks {
                let mut builder = PrimitiveArrayBuilder::<i32>::new(0);
                for i in chunk_size * chunk_idx..chunk_size * (chunk_idx + 1) {
                    builder.append(Some(i as i32));
                }
                let chunk = DataChunk::new(vec![Arc::new(builder.finish().into())], chunk_size);
                chunks.push(chunk);
            }

            let total_size = num_chunks * chunk_size;
            let num_full_new_chunk = total_size / new_chunk_size;
            let mut chunk_sizes = vec![new_chunk_size; num_full_new_chunk];
            let remainder = total_size % new_chunk_size;
            if remainder != 0 {
                chunk_sizes.push(remainder);
            }

            let new_chunks = DataChunk::rechunk(&chunks, new_chunk_size).unwrap();
            assert_eq!(new_chunks.len(), chunk_sizes.len());
            // check cardinality
            for (idx, chunk_size) in chunk_sizes.iter().enumerate() {
                assert_eq!(*chunk_size, new_chunks[idx].capacity());
            }

            let mut chunk_idx = 0;
            let mut cur_idx = 0;
            for val in 0..total_size {
                if cur_idx >= chunk_sizes[chunk_idx] {
                    cur_idx = 0;
                    chunk_idx += 1;
                }
                assert_eq!(
                    new_chunks[chunk_idx]
                        .column_at(0)
                        .as_int32()
                        .value_at(cur_idx)
                        .unwrap(),
                    val as i32
                );
                cur_idx += 1;
            }
        };

        test_case(0, 0, 1);
        test_case(0, 10, 1);
        test_case(10, 0, 1);
        test_case(1, 1, 6);
        test_case(1, 10, 11);
        test_case(2, 3, 6);
        test_case(5, 5, 6);
        test_case(10, 10, 7);
    }

    #[test]
    fn test_chunk_iter() {
        let num_of_columns: usize = 2;
        let length = 5;
        let mut columns = vec![];
        for i in 0..num_of_columns {
            let mut builder = PrimitiveArrayBuilder::<i32>::new(length);
            for _ in 0..length {
                builder.append(Some(i as i32));
            }
            let arr = builder.finish();
            columns.push(Arc::new(arr.into()))
        }
        let chunk: DataChunk = DataChunk::new(columns, length);
        for row in chunk.rows() {
            for i in 0..num_of_columns {
                let val = row.datum_at(i).unwrap();
                assert_eq!(val.into_int32(), i as i32);
            }
        }
    }

    #[test]
    fn test_to_pretty_string() {
        let chunk = DataChunk::new(
            vec![
                Arc::new(I64Array::from_iter([1, 2, 3, 4]).into()),
                Arc::new(I64Array::from_iter([Some(6), None, Some(7), None]).into()),
            ],
            4,
        );
        assert_eq!(
            chunk.to_pretty().to_string(),
            "\
+---+---+
| 1 | 6 |
| 2 |   |
| 3 | 7 |
| 4 |   |
+---+---+"
        );
    }

    #[test]
    fn test_no_column_chunk() {
        let chunk = DataChunk::new_dummy(10);
        assert_eq!(chunk.rows().count(), 10);

        let chunk_after_serde = DataChunk::from_protobuf(&chunk.to_protobuf()).unwrap();
        assert_eq!(chunk_after_serde.rows().count(), 10);
        assert_eq!(chunk_after_serde.cardinality(), 10);
    }

    #[test]
    fn reorder_columns() {
        let chunk = DataChunk::from_pretty(
            "I I I
             2 5 1
             4 9 2
             6 9 3",
        );
        assert_eq!(
            chunk.project(&[2, 1, 0]),
            DataChunk::from_pretty(
                "I I I
                 1 5 2
                 2 9 4
                 3 9 6",
            )
        );
        assert_eq!(
            chunk.project(&[2, 0]),
            DataChunk::from_pretty(
                "I I
                 1 2
                 2 4
                 3 6",
            )
        );
        assert_eq!(chunk.project(&[0, 1, 2]), chunk);
        assert_eq!(chunk.project(&[]).cardinality(), 3);
    }

    #[test]
    fn test_chunk_estimated_size() {
        assert_eq!(
            72,
            DataChunk::from_pretty(
                "I I I
                 1 5 2
                 2 9 4
                 3 9 6",
            )
            .estimated_heap_size()
        );
        assert_eq!(
            48,
            DataChunk::from_pretty(
                "I I
                 1 2
                 2 4
                 3 6",
            )
            .estimated_heap_size()
        );
    }
}
