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

use itertools::Itertools;
use parse_display::Display;

use crate::array::{Array, ArrayImpl, DataChunk};
use crate::hash::Crc32HashCode;
use crate::row::{Row, RowExt};
use crate::types::{DataType, Datum, DatumRef, ScalarImpl, ScalarRefImpl};
use crate::util::hash_util::Crc32FastBuilder;
use crate::util::row_id::compute_vnode_from_row_id;

/// `VirtualNode` (a.k.a. Vnode) is a minimal partition that a set of keys belong to. It is used for
/// consistent hashing.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display)]
#[display("{0}")]
pub struct VirtualNode(VirtualNodeInner);

/// The internal representation of a virtual node id.
///
/// Note: not all bits of the inner representation might be used.
type VirtualNodeInner = u16;

/// `vnode_count` must be provided to convert a hash code to a virtual node.
///
/// Use [`Crc32HashCodeToVnodeExt::to_vnode`] instead.
impl !From<Crc32HashCode> for VirtualNode {}

#[easy_ext::ext(Crc32HashCodeToVnodeExt)]
impl Crc32HashCode {
    /// Converts the hash code to a virtual node, based on the given total count of vnodes.
    fn to_vnode(self, vnode_count: usize) -> VirtualNode {
        // Take the least significant bits of the hash code.
        // TODO: should we use the most significant bits?
        let inner = (self.value() % vnode_count as u64) as VirtualNodeInner;
        VirtualNode(inner)
    }
}

impl VirtualNode {
    /// The total count of virtual nodes, for compatibility purposes **ONLY**.
    ///
    /// Typical use cases:
    ///
    /// - As the default value for the session configuration.
    /// - As the vnode count for all streaming jobs, fragments, and tables that were created before
    ///   the variable vnode count support was introduced.
    /// - As the vnode count for singletons.
    pub const COUNT_FOR_COMPAT: usize = 1 << 8;
}

impl VirtualNode {
    /// The total count of virtual nodes, for testing purposes.
    pub const COUNT_FOR_TEST: usize = Self::COUNT_FOR_COMPAT;
    /// The maximum value of the virtual node, for testing purposes.
    pub const MAX_FOR_TEST: VirtualNode = VirtualNode::from_index(Self::COUNT_FOR_TEST - 1);
}

impl VirtualNode {
    /// The maximum count of virtual nodes that fits in [`VirtualNodeInner`].
    ///
    /// Note that the most significant bit is not used. This is because we use signed integers (`i16`)
    /// for the scalar representation, where overflow can be confusing in terms of ordering.
    // TODO(var-vnode): the only usage is in log-store, shall we update it by storing the vnode as
    // bytea to enable 2^16 vnodes?
    pub const MAX_COUNT: usize = 1 << (VirtualNodeInner::BITS - 1);
    /// The maximum value of the virtual node that can be represented.
    ///
    /// Note that this is **NOT** the maximum value of the virtual node, which depends on the configuration.
    pub const MAX_REPRESENTABLE: VirtualNode = VirtualNode::from_index(Self::MAX_COUNT - 1);
    /// The size of a virtual node in bytes, in memory or serialized representation.
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

/// An iterator over all virtual nodes.
pub type AllVirtualNodeIter = std::iter::Map<std::ops::Range<usize>, fn(usize) -> VirtualNode>;

impl VirtualNode {
    /// We may use `VirtualNode` as a datum in a stream, or store it as a column.
    /// Hence this reifies it as a RW datatype.
    pub const RW_TYPE: DataType = DataType::Int16;
    /// The minimum (zero) value of the virtual node.
    pub const ZERO: VirtualNode = VirtualNode::from_index(0);

    /// Creates a virtual node from the `usize` index.
    pub const fn from_index(index: usize) -> Self {
        debug_assert!(index < Self::MAX_COUNT);
        Self(index as _)
    }

    /// Returns the `usize` the virtual node used for indexing.
    pub const fn to_index(self) -> usize {
        self.0 as _
    }

    /// Creates a virtual node from the given scalar representation. Used by `VNODE` expression.
    pub const fn from_scalar(scalar: i16) -> Self {
        debug_assert!(scalar >= 0);
        Self(scalar as _)
    }

    pub fn from_datum_ref(datum: DatumRef<'_>) -> Self {
        Self::from_scalar(datum.expect("should not be none").into_int16())
    }

    /// Returns the scalar representation of the virtual node. Used by `VNODE` expression.
    pub const fn to_scalar(self) -> i16 {
        self.0 as _
    }

    pub const fn to_datum(self) -> Datum {
        Some(ScalarImpl::Int16(self.to_scalar()))
    }

    /// Creates a virtual node from the given big-endian bytes representation.
    pub const fn from_be_bytes(bytes: [u8; Self::SIZE]) -> Self {
        let inner = VirtualNodeInner::from_be_bytes(bytes);
        debug_assert!((inner as usize) < Self::MAX_COUNT);
        Self(inner)
    }

    /// Returns the big-endian bytes representation of the virtual node.
    pub const fn to_be_bytes(self) -> [u8; Self::SIZE] {
        self.0.to_be_bytes()
    }

    /// Iterates over all virtual nodes.
    pub fn all(vnode_count: usize) -> AllVirtualNodeIter {
        (0..vnode_count).map(Self::from_index)
    }
}

impl VirtualNode {
    // `compute_chunk` is used to calculate the `VirtualNode` for the columns in the
    // chunk. When only one column is provided and its type is `Serial`, we consider the column to
    // be the one that contains RowId, and use a special method to skip the calculation of Hash
    // and directly extract the `VirtualNode` from `RowId`.
    pub fn compute_chunk(
        data_chunk: &DataChunk,
        keys: &[usize],
        vnode_count: usize,
    ) -> Vec<VirtualNode> {
        if let Ok(idx) = Itertools::exactly_one(keys.iter())
            && let ArrayImpl::Serial(serial_array) = &**data_chunk.column_at(*idx)
        {
            return serial_array
                .iter()
                .enumerate()
                .map(|(idx, serial)| {
                    if let Some(serial) = serial {
                        compute_vnode_from_row_id(serial.as_row_id(), vnode_count)
                    } else {
                        // NOTE: here it will hash the entire row when the `_row_id` is missing,
                        // which could result in rows from the same chunk being allocated to different chunks.
                        // This process doesn’t guarantee the order of rows, producing indeterminate results in some cases,
                        // such as when `distinct on` is used without an `order by`.
                        let (row, _) = data_chunk.row_at(idx);
                        row.hash(Crc32FastBuilder).to_vnode(vnode_count)
                    }
                })
                .collect();
        }

        data_chunk
            .get_hash_values(keys, Crc32FastBuilder)
            .into_iter()
            .map(|hash| hash.to_vnode(vnode_count))
            .collect()
    }

    /// Equivalent to [`Self::compute_chunk`] with [`VirtualNode::COUNT_FOR_TEST`] as the vnode count.
    pub fn compute_chunk_for_test(data_chunk: &DataChunk, keys: &[usize]) -> Vec<VirtualNode> {
        Self::compute_chunk(data_chunk, keys, Self::COUNT_FOR_TEST)
    }

    // `compute_row` is used to calculate the `VirtualNode` for the corresponding columns in a
    // `Row`. Similar to `compute_chunk`, it also contains special handling for serial columns.
    pub fn compute_row(row: impl Row, indices: &[usize], vnode_count: usize) -> VirtualNode {
        let project = row.project(indices);
        if let Ok(Some(ScalarRefImpl::Serial(s))) = Itertools::exactly_one(project.iter()).as_ref()
        {
            return compute_vnode_from_row_id(s.as_row_id(), vnode_count);
        }

        project.hash(Crc32FastBuilder).to_vnode(vnode_count)
    }

    /// Equivalent to [`Self::compute_row`] with [`VirtualNode::COUNT_FOR_TEST`] as the vnode count.
    pub fn compute_row_for_test(row: impl Row, indices: &[usize]) -> VirtualNode {
        Self::compute_row(row, indices, Self::COUNT_FOR_TEST)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::DataChunkTestExt;
    use crate::row::OwnedRow;
    use crate::util::row_id::RowIdGenerator;

    #[test]
    fn test_serial_key_chunk() {
        let mut r#gen =
            RowIdGenerator::new([VirtualNode::from_index(100)], VirtualNode::COUNT_FOR_TEST);
        let chunk = format!(
            "SRL I
             {} 1
             {} 2",
            r#gen.next(),
            r#gen.next(),
        );

        let chunk = DataChunk::from_pretty(chunk.as_str());
        let vnodes = VirtualNode::compute_chunk_for_test(&chunk, &[0]);

        assert_eq!(
            vnodes.as_slice(),
            &[VirtualNode::from_index(100), VirtualNode::from_index(100)]
        );
    }

    #[test]
    fn test_serial_key_row() {
        let mut r#gen =
            RowIdGenerator::new([VirtualNode::from_index(100)], VirtualNode::COUNT_FOR_TEST);
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Serial(r#gen.next().into())),
            Some(ScalarImpl::Int64(12345)),
        ]);

        let vnode = VirtualNode::compute_row_for_test(&row, &[0]);

        assert_eq!(vnode, VirtualNode::from_index(100));
    }

    #[test]
    fn test_serial_key_chunk_multiple_vnodes() {
        let mut r#gen = RowIdGenerator::new(
            [100, 200].map(VirtualNode::from_index),
            VirtualNode::COUNT_FOR_TEST,
        );
        let chunk = format!(
            "SRL I
             {} 1
             {} 2
             {} 3
             {} 4",
            r#gen.next(),
            r#gen.next(),
            r#gen.next(),
            r#gen.next(),
        );

        let chunk = DataChunk::from_pretty(chunk.as_str());
        let vnodes = VirtualNode::compute_chunk_for_test(&chunk, &[0]);

        assert_eq!(
            vnodes.as_slice(),
            &[
                VirtualNode::from_index(100),
                VirtualNode::from_index(200),
                VirtualNode::from_index(100),
                VirtualNode::from_index(200),
            ]
        );
    }

    #[track_caller]
    fn assert_vnode(t: &DataType, s: &str, vnode: expect_test::Expect) {
        let scalar = ScalarImpl::from_text(s, t).unwrap();
        let row = OwnedRow::new(vec![Some(scalar)]);
        let actual = VirtualNode::compute_row_for_test(row, &[0]);
        vnode.assert_eq(&actual.to_string());
    }

    #[test]
    fn test_row_hash_backward_compat() {
        use DataType as T;
        // See #25336
        // The expected value is pre-computed by earlier versions of RisingWave and is persisted by storage.
        // Hash implementations that output different values are thus backward incompatible.
        assert_vnode(&T::Boolean, "true", expect_test::expect!["27"]);
        assert_vnode(&T::Boolean, "false", expect_test::expect!["141"]);
        assert_vnode(&T::Int16, "1", expect_test::expect!["190"]);
        assert_vnode(&T::Int32, "1", expect_test::expect!["121"]);
        assert_vnode(&T::Int64, "1", expect_test::expect!["247"]);
        assert_vnode(&T::Varchar, "abc", expect_test::expect!["221"]);
        assert_vnode(&T::Bytea, r#"\x616263"#, expect_test::expect!["61"]);
        assert_vnode(&T::Date, "2006-01-02", expect_test::expect!["136"]);
        assert_vnode(&T::Time, "15:04:05", expect_test::expect!["180"]);
        assert_vnode(&T::Timestamp, "2006-01-02 15:04:05", expect_test::expect!["132"]);
        assert_vnode(&T::Timestamptz, "2006-01-02 15:04:05+00:00", expect_test::expect!["255"]);

        assert_vnode(&T::Jsonb, r#"{"a":"b"}"#, expect_test::expect!["201"]);
        assert_vnode(&T::Interval, "12:34:56", expect_test::expect!["207"]);
        assert_vnode(&T::Decimal, "1.2", expect_test::expect!["142"]);
        assert_vnode(&T::Float32, "NaN", expect_test::expect!["113"]);
        assert_vnode(&T::Float64, "Infinity", expect_test::expect!["128"]);
        assert_vnode(&T::Vector(3), "[1,2,3]", expect_test::expect!["52"]);
        // assert_vnode(&T::Struct(), "1", expect_test::expect!["27"]);
        // assert_vnode(&T::List(), "1", expect_test::expect!["27"]);
        // assert_vnode(&T::Map(), "1", expect_test::expect!["27"]);
        assert_vnode(&T::Serial, "1", expect_test::expect!["0"]);
        assert_vnode(&T::Int256, "1", expect_test::expect!["212"]);
    }
}
