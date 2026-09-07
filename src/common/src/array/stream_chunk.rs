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

use std::fmt::Display;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::{fmt, mem};

use either::Either;
use enum_as_inner::EnumAsInner;
use itertools::Itertools;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::data::{PbOp, PbStreamChunk};

use super::stream_chunk_builder::StreamChunkBuilder;
use super::{ArrayImpl, ArrayRef, ArrayResult, DataChunkTestExt, RowRef};
use crate::array::DataChunk;
use crate::bitmap::{Bitmap, BitmapBuilder};
use crate::catalog::Schema;
use crate::field_generator::VarcharProperty;
use crate::row::Row;
use crate::types::{DataType, DefaultOrdered, ToText};

/// `Op` represents three operations in `StreamChunk`.
///
/// `UpdateDelete` and `UpdateInsert` are semantically equivalent to `Delete` and `Insert`
/// but always appear in pairs to represent an update operation. It's guaranteed that
/// they are adjacent to each other in the same `StreamChunk`, and the stream key of the two
/// rows are the same.
#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, EnumAsInner)]
pub enum Op {
    Insert,
    Delete,
    UpdateDelete,
    UpdateInsert,
}

impl Op {
    pub fn to_protobuf(self) -> PbOp {
        match self {
            Op::Insert => PbOp::Insert,
            Op::Delete => PbOp::Delete,
            Op::UpdateInsert => PbOp::UpdateInsert,
            Op::UpdateDelete => PbOp::UpdateDelete,
        }
    }

    pub fn from_protobuf(prost: &i32) -> ArrayResult<Op> {
        let op = match PbOp::try_from(*prost) {
            Ok(PbOp::Insert) => Op::Insert,
            Ok(PbOp::Delete) => Op::Delete,
            Ok(PbOp::UpdateInsert) => Op::UpdateInsert,
            Ok(PbOp::UpdateDelete) => Op::UpdateDelete,
            Ok(PbOp::Unspecified) => unreachable!(),
            Err(_) => bail!("No such op type"),
        };
        Ok(op)
    }

    /// convert `UpdateDelete` to `Delete` and `UpdateInsert` to Insert
    pub fn normalize_update(self) -> Op {
        match self {
            Op::Insert => Op::Insert,
            Op::Delete => Op::Delete,
            Op::UpdateDelete => Op::Delete,
            Op::UpdateInsert => Op::Insert,
        }
    }

    pub fn to_i16(self) -> i16 {
        match self {
            Op::Insert => 1,
            Op::Delete => 2,
            Op::UpdateInsert => 3,
            Op::UpdateDelete => 4,
        }
    }

    pub fn to_varchar(self) -> String {
        match self {
            Op::Insert => "Insert",
            Op::Delete => "Delete",
            Op::UpdateInsert => "UpdateInsert",
            Op::UpdateDelete => "UpdateDelete",
        }
        .to_owned()
    }
}

/// `StreamChunk` is used to pass data over the streaming pathway.
#[derive(Clone, PartialEq)]
pub struct StreamChunk {
    // TODO: Optimize using bitmap
    ops: Arc<[Op]>,
    data: DataChunk,
}

impl Default for StreamChunk {
    /// Create a 0-row-0-col `StreamChunk`. Only used in some existing tests.
    /// This is NOT the same as an **empty** chunk, which has 0 rows but with
    /// columns aligned with executor schema.
    fn default() -> Self {
        Self {
            ops: Arc::new([]),
            data: DataChunk::new(vec![], 0),
        }
    }
}

impl StreamChunk {
    /// Create a new `StreamChunk` with given ops and columns.
    pub fn new(ops: impl Into<Arc<[Op]>>, columns: Vec<ArrayRef>) -> Self {
        let ops = ops.into();
        let visibility = Bitmap::ones(ops.len());
        Self::with_visibility(ops, columns, visibility)
    }

    /// Create a new `StreamChunk` with given ops, columns and visibility.
    pub fn with_visibility(
        ops: impl Into<Arc<[Op]>>,
        columns: Vec<ArrayRef>,
        visibility: Bitmap,
    ) -> Self {
        let ops = ops.into();
        for col in &columns {
            assert_eq!(col.len(), ops.len());
        }
        let data = DataChunk::new(columns, visibility);
        StreamChunk { ops, data }
    }

    /// Build a `StreamChunk` from rows.
    ///
    /// Panics if the `rows` is empty.
    ///
    /// Should prefer using [`StreamChunkBuilder`] instead to avoid unnecessary
    /// allocation of rows.
    pub fn from_rows(rows: &[(Op, impl Row)], data_types: &[DataType]) -> Self {
        let mut builder = StreamChunkBuilder::unlimited(data_types.to_vec(), Some(rows.len()));

        for (op, row) in rows {
            let none = builder.append_row(*op, row);
            debug_assert!(none.is_none());
        }

        builder.take().expect("chunk should not be empty")
    }

    pub fn empty(data_types: &[DataType]) -> Self {
        StreamChunkBuilder::build_empty(data_types.to_vec())
    }

    /// Get the reference of the underlying data chunk.
    pub fn data_chunk(&self) -> &DataChunk {
        &self.data
    }

    /// Removes the invisible rows based on `visibility`. Returns a new compacted chunk
    /// with all rows visible.
    ///
    /// This does not change the visible content of the chunk. Not to be confused with
    /// `StreamChunkCompactor`, which removes unnecessary changes based on the key.
    ///
    /// See [`DataChunk::compact_vis`] for more details.
    pub fn compact_vis(self) -> Self {
        if self.is_vis_compacted() {
            return self;
        }

        let (ops, columns, visibility) = self.into_inner();

        let cardinality = visibility
            .iter()
            .fold(0, |vis_cnt, vis| vis_cnt + vis as usize);
        let columns: Vec<_> = columns
            .into_iter()
            .map(|col| col.compact_vis(&visibility, cardinality).into())
            .collect();
        let mut new_ops = Vec::with_capacity(cardinality);
        for idx in visibility.iter_ones() {
            new_ops.push(ops[idx]);
        }
        StreamChunk::new(new_ops, columns)
    }

    /// Split the `StreamChunk` into multiple chunks with the given size at most.
    ///
    /// When the total cardinality of all the chunks is not evenly divided by the `size`,
    /// the last new chunk will be the remainder.
    ///
    /// For consecutive `UpdateDelete` and `UpdateInsert`, they will be kept in one chunk.
    /// As a result, some chunks may have `size + 1` rows.
    pub fn split(&self, size: usize) -> Vec<Self> {
        let mut builder = StreamChunkBuilder::new(size, self.data_types());
        let mut outputs = Vec::new();

        // TODO: directly append the chunk.
        for (op, row) in self.rows() {
            if let Some(chunk) = builder.append_row(op, row) {
                outputs.push(chunk);
            }
        }
        if let Some(output) = builder.take() {
            outputs.push(output);
        }

        outputs
    }

    pub fn into_parts(self) -> (DataChunk, Arc<[Op]>) {
        (self.data, self.ops)
    }

    pub fn from_parts(ops: impl Into<Arc<[Op]>>, data_chunk: DataChunk) -> Self {
        let (columns, vis) = data_chunk.into_parts();
        Self::with_visibility(ops, columns, vis)
    }

    pub fn into_inner(self) -> (Arc<[Op]>, Vec<ArrayRef>, Bitmap) {
        let (columns, vis) = self.data.into_parts();
        (self.ops, columns, vis)
    }

    pub fn to_protobuf(&self) -> PbStreamChunk {
        if !self.is_vis_compacted() {
            return self.clone().compact_vis().to_protobuf();
        }
        PbStreamChunk {
            cardinality: self.cardinality() as u32,
            ops: self.ops.iter().map(|op| op.to_protobuf() as i32).collect(),
            columns: self.columns().iter().map(|col| col.to_protobuf()).collect(),
        }
    }

    pub fn from_protobuf(prost: &PbStreamChunk) -> ArrayResult<Self> {
        let cardinality = prost.get_cardinality() as usize;
        let mut ops = Vec::with_capacity(cardinality);
        for op in prost.get_ops() {
            ops.push(Op::from_protobuf(op)?);
        }
        let mut columns = vec![];
        for column in prost.get_columns() {
            columns.push(ArrayImpl::from_protobuf(column, cardinality)?.into());
        }
        Ok(StreamChunk::new(ops, columns))
    }

    pub fn ops(&self) -> &[Op] {
        &self.ops
    }

    /// Returns a table-like text representation of the `StreamChunk`.
    pub fn to_pretty(&self) -> impl Display + use<> {
        self.to_pretty_inner(None)
    }

    /// Returns a table-like text representation of the `StreamChunk` with a header of column names
    /// from the given `schema`.
    pub fn to_pretty_with_schema(&self, schema: &Schema) -> impl Display + use<> {
        self.to_pretty_inner(Some(schema))
    }

    fn to_pretty_inner(&self, schema: Option<&Schema>) -> impl Display + use<> {
        use comfy_table::{Cell, CellAlignment, Table};

        if self.cardinality() == 0 {
            return Either::Left("(empty)");
        }

        let mut table = Table::new();
        table.load_preset(DataChunk::PRETTY_TABLE_PRESET);

        if let Some(schema) = schema {
            assert_eq!(self.dimension(), schema.len());
            let cells = std::iter::once(String::new())
                .chain(schema.fields().iter().map(|f| f.name.clone()));
            table.set_header(cells);
        }

        for (op, row_ref) in self.rows() {
            let mut cells = Vec::with_capacity(row_ref.len() + 1);
            cells.push(
                Cell::new(match op {
                    Op::Insert => "+",
                    Op::Delete => "-",
                    Op::UpdateDelete => "U-",
                    Op::UpdateInsert => "U+",
                })
                .set_alignment(CellAlignment::Right),
            );
            for datum in row_ref.iter() {
                let str = match datum {
                    None => "".to_owned(), // NULL
                    Some(scalar) => scalar.to_text(),
                };
                cells.push(Cell::new(str));
            }
            table.add_row(cells);
        }

        Either::Right(table)
    }

    /// Reorder (and possibly remove) columns.
    ///
    /// e.g. if `indices` is `[2, 1, 0]`, and the chunk contains column `[a, b, c]`, then the output
    /// will be `[c, b, a]`. If `indices` is [2, 0], then the output will be `[c, a]`.
    /// If the input mapping is identity mapping, no reorder will be performed.
    pub fn project(&self, indices: &[usize]) -> Self {
        Self {
            ops: self.ops.clone(),
            data: self.data.project(indices),
        }
    }

    /// Remove the adjacent delete-insert and insert-deletes if their row value are the same.
    pub fn eliminate_adjacent_noop_update(self) -> Self {
        let len = self.data_chunk().capacity();
        let mut c: StreamChunkMut = self.into();
        let mut prev_r = None;
        for curr in 0..len {
            if !c.vis(curr) {
                continue;
            }
            if let Some(prev) = prev_r
                && (
                    // 1. Delete then Insert
                    (matches!(c.op(prev), Op::UpdateDelete | Op::Delete)
                    && matches!(c.op(curr), Op::UpdateInsert | Op::Insert))
                    ||
                    // 2. Insert then Delete
                    //
                    // Note that after eliminating `U+` and `U-` here, we will get a new
                    // pair of `U-` and `U+` consisting of `prev.prev` and the `next` row.
                    // `prev.prev` and `prev`, `curr` and `next` share the same stream key
                    // because they are `U-` and `U+` pairs, while `prev` and `curr` share
                    // the same stream key because they are equal, so `prev-prev` and `next`
                    // must also share the same stream key. Therefore, it's okay to leave
                    // their `Update` ops unchanged.
                    //
                    // Also, they can't be the same, otherwise these 4 rows are the same,
                    // which should already be eliminated by the first branch.
                    (matches!(c.op(prev), Op::UpdateInsert | Op::Insert)
                    && matches!(c.op(curr), Op::UpdateDelete | Op::Delete))
                )
                && c.row_ref(prev) == c.row_ref(curr)
            {
                c.set_vis(prev, false);
                c.set_vis(curr, false);
                prev_r = None;
            } else {
                prev_r = Some(curr);
            }
        }

        // Normalize update pairs that became partially invisible.
        // If only U- is visible, turn it into Delete; if only U+ is visible, turn it into Insert.
        for idx in 0..len.saturating_sub(1) {
            if c.op(idx) == Op::UpdateDelete && c.op(idx + 1) == Op::UpdateInsert {
                let delete_vis = c.vis(idx);
                let insert_vis = c.vis(idx + 1);
                if delete_vis && !insert_vis {
                    c.set_op(idx, Op::Delete);
                } else if !delete_vis && insert_vis {
                    c.set_op(idx + 1, Op::Insert);
                }
            }
        }
        c.into()
    }

    /// Reorder columns and set visibility.
    pub fn project_with_vis(&self, indices: &[usize], vis: Bitmap) -> Self {
        Self {
            ops: self.ops.clone(),
            data: self.data.project_with_vis(indices, vis),
        }
    }

    /// Clone the `StreamChunk` with a new visibility.
    pub fn clone_with_vis(&self, vis: Bitmap) -> Self {
        Self {
            ops: self.ops.clone(),
            data: self.data.with_visibility(vis),
        }
    }
}

impl Deref for StreamChunk {
    type Target = DataChunk;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for StreamChunk {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

/// `StreamChunk` can be created from `DataChunk` with all operations set to `Insert`.
impl From<DataChunk> for StreamChunk {
    fn from(data: DataChunk) -> Self {
        Self::from_parts(vec![Op::Insert; data.capacity()], data)
    }
}

impl fmt::Debug for StreamChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(
                f,
                "StreamChunk {{ cardinality: {}, capacity: {}, data:\n{}\n }}",
                self.cardinality(),
                self.capacity(),
                self.to_pretty()
            )
        } else {
            f.debug_struct("StreamChunk")
                .field("cardinality", &self.cardinality())
                .field("capacity", &self.capacity())
                .finish_non_exhaustive()
        }
    }
}

impl EstimateSize for StreamChunk {
    fn estimated_heap_size(&self) -> usize {
        self.data.estimated_heap_size() + self.ops.len() * size_of::<Op>()
    }
}

enum OpsMutState {
    ArcRef(Arc<[Op]>),
    Mut(Vec<Op>),
}

impl OpsMutState {
    const UNDEFINED: Self = Self::Mut(Vec::new());
}

pub struct OpsMut {
    state: OpsMutState,
}

impl OpsMut {
    pub fn new(ops: Arc<[Op]>) -> Self {
        Self {
            state: OpsMutState::ArcRef(ops),
        }
    }

    pub fn len(&self) -> usize {
        match &self.state {
            OpsMutState::ArcRef(v) => v.len(),
            OpsMutState::Mut(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn set(&mut self, n: usize, val: Op) {
        debug_assert!(n < self.len());
        if let OpsMutState::Mut(v) = &mut self.state {
            v[n] = val;
        } else {
            let state = mem::replace(&mut self.state, OpsMutState::UNDEFINED); // intermediate state
            let mut v = match state {
                OpsMutState::ArcRef(v) => v.to_vec(),
                OpsMutState::Mut(_) => unreachable!(),
            };
            v[n] = val;
            self.state = OpsMutState::Mut(v);
        }
    }

    pub fn get(&self, n: usize) -> Op {
        debug_assert!(n < self.len());
        match &self.state {
            OpsMutState::ArcRef(v) => v[n],
            OpsMutState::Mut(v) => v[n],
        }
    }
}
impl From<OpsMut> for Arc<[Op]> {
    fn from(v: OpsMut) -> Self {
        match v.state {
            OpsMutState::ArcRef(a) => a,
            OpsMutState::Mut(v) => v.into(),
        }
    }
}

/// A mutable wrapper for `StreamChunk`. can only set the visibilities and ops in place, can not
/// change the length.
pub struct StreamChunkMut {
    columns: Arc<[ArrayRef]>,
    ops: OpsMut,
    vis: BitmapBuilder,
}

impl From<StreamChunk> for StreamChunkMut {
    fn from(c: StreamChunk) -> Self {
        let (c, ops) = c.into_parts();
        let (columns, vis) = c.into_parts_v2();
        Self {
            columns,
            ops: OpsMut::new(ops),
            vis: vis.into(),
        }
    }
}

impl From<StreamChunkMut> for StreamChunk {
    fn from(c: StreamChunkMut) -> Self {
        StreamChunk::from_parts(c.ops, DataChunk::from_parts(c.columns, c.vis.finish()))
    }
}

/// A handle to one row of a [`StreamChunkMut`] that can update the row's op and
/// visibility in place. It holds a raw pointer instead of `&mut` since multiple
/// handles to the same chunk coexist (see [`StreamChunkMut::to_rows_mut`]).
pub struct OpRowMutRef<'a> {
    c: *mut StreamChunkMut,
    i: usize,
    _phantom: PhantomData<&'a mut StreamChunkMut>,
}

impl PartialEq for OpRowMutRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.row_ref() == other.row_ref()
    }
}
impl Eq for OpRowMutRef<'_> {}

impl<'a> OpRowMutRef<'a> {
    pub fn index(&self) -> usize {
        self.i
    }

    // SAFETY of derefs below: `self.c` is valid for `'a`, and each access reborrows
    // a single disjoint field only.

    pub fn vis(&self) -> bool {
        let vis = unsafe { &(*self.c).vis };
        vis.is_set(self.i)
    }

    pub fn op(&self) -> Op {
        let ops = unsafe { &(*self.c).ops };
        ops.get(self.i)
    }

    pub fn set_vis(&mut self, val: bool) {
        let vis = unsafe { &mut (*self.c).vis };
        vis.set(self.i, val);
    }

    pub fn set_op(&mut self, val: Op) {
        let ops = unsafe { &mut (*self.c).ops };
        ops.set(self.i, val);
    }

    pub fn row_ref(&self) -> RowRef<'_> {
        RowRef::with_columns(unsafe { &(*self.c).columns }, self.i)
    }

    /// return if the two row ref is in the same chunk
    pub fn same_chunk(&self, other: &Self) -> bool {
        std::ptr::eq(self.c, other.c)
    }
}

impl StreamChunkMut {
    pub fn capacity(&self) -> usize {
        self.vis.len()
    }

    pub fn vis(&self, i: usize) -> bool {
        self.vis.is_set(i)
    }

    pub fn op(&self, i: usize) -> Op {
        self.ops.get(i)
    }

    pub fn row_ref(&self, i: usize) -> RowRef<'_> {
        RowRef::with_columns(self.columns(), i)
    }

    pub fn set_vis(&mut self, n: usize, val: bool) {
        self.vis.set(n, val);
    }

    pub fn set_op(&mut self, n: usize, val: Op) {
        self.ops.set(n, val);
    }

    pub fn columns(&self) -> &[ArrayRef] {
        &self.columns
    }

    /// get the mut reference of the stream chunk.
    pub fn to_rows_mut(&mut self) -> impl Iterator<Item = (RowRef<'_>, OpRowMutRef<'_>)> {
        // SAFETY: the pointer is derived from `&mut self`, which stays exclusively
        // borrowed by the returned iterator.
        unsafe { Self::rows_mut_ptr(self) }
    }

    /// # Safety
    ///
    /// `p` must be derived from an exclusive reference valid for `'a`, and the chunk must
    /// not be accessed in other ways while the iterator or any yielded item is alive.
    unsafe fn rows_mut_ptr<'a>(
        p: *mut Self,
    ) -> impl Iterator<Item = (RowRef<'a>, OpRowMutRef<'a>)> {
        let len = {
            let vis = unsafe { &(*p).vis };
            vis.len()
        };
        (0..len)
            .filter(move |i| {
                let vis = unsafe { &(*p).vis };
                vis.is_set(*i)
            })
            .map(move |i| {
                (
                    RowRef::with_columns(unsafe { &(*p).columns }, i),
                    OpRowMutRef {
                        c: p,
                        i,
                        _phantom: PhantomData,
                    },
                )
            })
    }
}

/// Test utilities for [`StreamChunk`].
#[easy_ext::ext(StreamChunkTestExt)]
impl StreamChunk {
    /// Parse a chunk from string.
    ///
    /// See also [`DataChunkTestExt::from_pretty`].
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
    /// use risingwave_common::array::StreamChunk;
    /// use risingwave_common::array::stream_chunk::StreamChunkTestExt as _;
    /// let chunk = StreamChunk::from_pretty(
    ///     "  I I I I      // type chars
    ///     U- 2 5 . .      // '.' means NULL
    ///     U+ 2 5 2 6 D    // 'D' means deleted in visibility
    ///     +  . . 4 8      // ^ comments are ignored
    ///     -  . . 3 4",
    /// );
    /// //  ^ operations:
    /// //     +: Insert
    /// //     -: Delete
    /// //    U+: UpdateInsert
    /// //    U-: UpdateDelete
    ///
    /// // type chars:
    /// //     I: i64
    /// //     i: i32
    /// //     F: f64
    /// //     f: f32
    /// //     T: str
    /// //    TS: Timestamp
    /// //    TZ: Timestamptz
    /// //   SRL: Serial
    /// //   x[]: array of x
    /// // <i,f>: struct
    /// ```
    pub fn from_pretty(s: &str) -> Self {
        let mut chunk_str = String::new();
        let mut ops = vec![];

        let (header, body) = match s.split_once('\n') {
            Some(pair) => pair,
            None => {
                // empty chunk
                return StreamChunk {
                    ops: Arc::new([]),
                    data: DataChunk::from_pretty(s),
                };
            }
        };
        chunk_str.push_str(header);
        chunk_str.push('\n');

        for line in body.split_inclusive('\n') {
            if line.trim_start().is_empty() {
                continue;
            }
            let (op, row) = line
                .trim_start()
                .split_once(|c: char| c.is_ascii_whitespace())
                .ok_or_else(|| panic!("missing operation: {line:?}"))
                .unwrap();
            ops.push(match op {
                "+" => Op::Insert,
                "-" => Op::Delete,
                "U+" => Op::UpdateInsert,
                "U-" => Op::UpdateDelete,
                t => panic!("invalid op: {t:?}"),
            });
            chunk_str.push_str(row);
        }
        StreamChunk {
            ops: ops.into(),
            data: DataChunk::from_pretty(&chunk_str),
        }
    }

    /// Validate the `StreamChunk` layout.
    pub fn valid(&self) -> bool {
        let len = self.ops.len();
        let data = &self.data;
        data.visibility().len() == len && data.columns().iter().all(|col| col.len() == len)
    }

    /// Concatenate multiple `StreamChunk` into one.
    ///
    /// Panics if `chunks` is empty.
    pub fn concat(chunks: Vec<StreamChunk>) -> StreamChunk {
        let data_types = chunks[0].data_types();
        let size = chunks.iter().map(|c| c.cardinality()).sum::<usize>();

        let mut builder = StreamChunkBuilder::unlimited(data_types, Some(size));

        for chunk in chunks {
            // TODO: directly append chunks.
            for (op, row) in chunk.rows() {
                let none = builder.append_row(op, row);
                debug_assert!(none.is_none());
            }
        }

        builder.take().expect("chunk should not be empty")
    }

    /// Sort rows.
    pub fn sort_rows(self) -> Self {
        if self.capacity() == 0 {
            return self;
        }
        let rows = self.rows().collect_vec();
        let mut idx = (0..self.capacity()).collect_vec();
        idx.sort_by_key(|&i| {
            let (op, row_ref) = rows[i];
            (op, DefaultOrdered(row_ref))
        });
        StreamChunk {
            ops: idx.iter().map(|&i| self.ops[i]).collect(),
            data: self.data.reorder_rows(&idx),
        }
    }

    /// Generate `num_of_chunks` data chunks with type `data_types`,
    /// where each data chunk has cardinality of `chunk_size`.
    /// TODO(kwannoel): Generate different types of op, different vis.
    pub fn gen_stream_chunks(
        num_of_chunks: usize,
        chunk_size: usize,
        data_types: &[DataType],
        varchar_properties: &VarcharProperty,
    ) -> Vec<StreamChunk> {
        Self::gen_stream_chunks_inner(
            num_of_chunks,
            chunk_size,
            data_types,
            varchar_properties,
            1.0,
            1.0,
        )
    }

    pub fn gen_stream_chunks_inner(
        num_of_chunks: usize,
        chunk_size: usize,
        data_types: &[DataType],
        varchar_properties: &VarcharProperty,
        visibility_percent: f64, // % of rows that are visible
        inserts_percent: f64,    // Rest will be deletes.
    ) -> Vec<StreamChunk> {
        let ops = if inserts_percent == 0.0 {
            vec![Op::Delete; chunk_size]
        } else if inserts_percent == 1.0 {
            vec![Op::Insert; chunk_size]
        } else {
            let mut rng = SmallRng::from_seed([0; 32]);
            let mut ops = vec![];
            for _ in 0..chunk_size {
                ops.push(if rng.random_bool(inserts_percent) {
                    Op::Insert
                } else {
                    Op::Delete
                });
            }
            ops
        };
        DataChunk::gen_data_chunks(
            num_of_chunks,
            chunk_size,
            data_types,
            varchar_properties,
            visibility_percent,
        )
        .into_iter()
        .map(|chunk| StreamChunk::from_parts(ops.clone(), chunk))
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_pretty_string() {
        let chunk = StreamChunk::from_pretty(
            "  I I
             + 1 6
             - 2 .
            U- 3 7
            U+ 4 .",
        );
        assert_eq!(
            chunk.to_pretty().to_string(),
            "\
+----+---+---+
|  + | 1 | 6 |
|  - | 2 |   |
| U- | 3 | 7 |
| U+ | 4 |   |
+----+---+---+"
        );
    }

    #[test]
    fn test_split_1() {
        let chunk = StreamChunk::from_pretty(
            "  I I
             + 1 6
             - 2 .
            U- 3 7
            U+ 4 .",
        );
        let results = chunk.split(2);
        assert_eq!(2, results.len());
        assert_eq!(
            results[0].to_pretty().to_string(),
            "\
+---+---+---+
| + | 1 | 6 |
| - | 2 |   |
+---+---+---+"
        );
        assert_eq!(
            results[1].to_pretty().to_string(),
            "\
+----+---+---+
| U- | 3 | 7 |
| U+ | 4 |   |
+----+---+---+"
        );
    }

    #[test]
    fn test_split_2() {
        let chunk = StreamChunk::from_pretty(
            "  I I
             + 1 6
            U- 3 7
            U+ 4 .
             - 2 .",
        );
        let results = chunk.split(2);
        assert_eq!(2, results.len());
        assert_eq!(
            results[0].to_pretty().to_string(),
            "\
+----+---+---+
|  + | 1 | 6 |
| U- | 3 | 7 |
| U+ | 4 |   |
+----+---+---+"
        );
        assert_eq!(
            results[1].to_pretty().to_string(),
            "\
+---+---+---+
| - | 2 |   |
+---+---+---+"
        );
    }

    #[test]
    fn test_eliminate_adjacent_noop_update() {
        let c = StreamChunk::from_pretty(
            "  I I
            - 1 6 D
            - 2 2
            + 2 3
            - 2 3
            + 1 6
            - 1 7
            + 1 10 D
            + 1 7
            U- 3 7
            U+ 3 7
            + 2 3",
        );
        let c = c.eliminate_adjacent_noop_update();
        assert_eq!(
            c.to_pretty().to_string(),
            "\
+---+---+---+
| - | 2 | 2 |
| + | 1 | 6 |
| + | 2 | 3 |
+---+---+---+"
        );
    }

    #[test]
    fn test_eliminate_adjacent_noop_update_normalize_update_pair() {
        let c = StreamChunk::from_pretty(
            "  I I
            + 1 10
            U- 1 10
            U+ 1 20",
        );
        let c = c.eliminate_adjacent_noop_update();
        assert_eq!(
            c.to_pretty().to_string(),
            "\
+---+---+----+
| + | 1 | 20 |
+---+---+----+"
        );
    }
}
