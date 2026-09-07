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

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::iter::once;
use std::ops::Bound::*;
use std::ops::{Bound, Deref, DerefMut, RangeBounds};
use std::ptr;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_common_estimate_size::EstimateSize;

use crate::{EpochWithGap, HummockEpoch};

pub const EPOCH_LEN: usize = std::mem::size_of::<HummockEpoch>();
pub const TABLE_PREFIX_LEN: usize = std::mem::size_of::<u32>();
// Max length for key overlap and diff length. See KeyPrefix::encode.
pub const MAX_KEY_LEN: usize = u16::MAX as usize;

pub type KeyPayloadType = Bytes;
pub type TableKeyRange = (
    Bound<TableKey<KeyPayloadType>>,
    Bound<TableKey<KeyPayloadType>>,
);
pub type UserKeyRange = (
    Bound<UserKey<KeyPayloadType>>,
    Bound<UserKey<KeyPayloadType>>,
);
pub type UserKeyRangeRef<'a> = (Bound<UserKey<&'a [u8]>>, Bound<UserKey<&'a [u8]>>);
pub type FullKeyRange = (
    Bound<FullKey<KeyPayloadType>>,
    Bound<FullKey<KeyPayloadType>>,
);

pub fn is_empty_key_range(key_range: &TableKeyRange) -> bool {
    match key_range {
        (Included(start), Excluded(end)) => start == end,
        _ => false,
    }
}

/// Returns left inclusive and right exclusive vnode index of the given range.
///
/// # Vnode count unawareness
///
/// Note that this function is not aware of the vnode count that is actually used in this table.
/// For example, if the total vnode count is 256, `Unbounded` can be a correct end bound for vnode 255,
/// but this function will still return `Excluded(256)`.
///
/// See also [`vnode`] and [`end_bound_of_vnode`] which hold such invariant.
pub fn vnode_range<T: AsRef<[u8]>>(range: &impl RangeBounds<TableKey<T>>) -> (usize, usize) {
    let left = match range.start_bound() {
        Included(key) | Excluded(key) => key.vnode_part().to_index(),
        Unbounded => 0,
    };
    let right = match range.end_bound() {
        Included(key) => key.vnode_part().to_index() + 1,
        Excluded(key) => {
            let (vnode, inner_key) = key.split_vnode();
            if inner_key.is_empty() {
                // When the exclusive end key range contains only a vnode,
                // the whole vnode is excluded.
                vnode.to_index()
            } else {
                vnode.to_index() + 1
            }
        }
        Unbounded => VirtualNode::MAX_REPRESENTABLE.to_index() + 1,
    };
    (left, right)
}

/// Ensure there is only one vnode involved in table key range and return the vnode.
///
/// # Vnode count unawareness
///
/// Note that this function is not aware of the vnode count that is actually used in this table.
/// For example, if the total vnode count is 256, `Unbounded` can be a correct end bound for vnode 255,
/// but this function will still require `Excluded(256)`.
///
/// See also [`vnode_range`] and [`end_bound_of_vnode`] which hold such invariant.
pub fn vnode(range: &TableKeyRange) -> VirtualNode {
    let (l, r_exclusive) = vnode_range(range);
    assert_eq!(r_exclusive - l, 1);
    VirtualNode::from_index(l)
}

/// Converts user key to full key by appending `epoch` to the user key.
pub fn key_with_epoch(mut user_key: Vec<u8>, epoch: HummockEpoch) -> Vec<u8> {
    let res = epoch.to_be();
    user_key.reserve(EPOCH_LEN);
    let buf = user_key.chunk_mut();

    // TODO: check whether this hack improves performance
    unsafe {
        ptr::copy_nonoverlapping(
            &res as *const _ as *const u8,
            buf.as_mut_ptr() as *mut _,
            EPOCH_LEN,
        );
        user_key.advance_mut(EPOCH_LEN);
    }

    user_key
}

/// Splits a full key into its user key part and epoch part.
#[inline]
pub fn split_key_epoch(full_key: &[u8]) -> (&[u8], &[u8]) {
    let pos = full_key
        .len()
        .checked_sub(EPOCH_LEN)
        .unwrap_or_else(|| panic!("bad full key format: {:?}", full_key));
    full_key.split_at(pos)
}

/// Extract encoded [`UserKey`] from encoded [`FullKey`] without epoch part
pub fn user_key(full_key: &[u8]) -> &[u8] {
    split_key_epoch(full_key).0
}

/// Extract table key from encoded [`UserKey`] without table id part
pub fn table_key(user_key: &[u8]) -> &[u8] {
    &user_key[TABLE_PREFIX_LEN..]
}

#[inline(always)]
/// Extract encoded [`UserKey`] from encoded [`FullKey`] but allow empty slice
pub fn get_user_key(full_key: &[u8]) -> Vec<u8> {
    if full_key.is_empty() {
        vec![]
    } else {
        user_key(full_key).to_vec()
    }
}

/// Extract table id from encoded [`FullKey`]
#[inline(always)]
pub fn get_table_id(full_key: &[u8]) -> u32 {
    let mut buf = full_key;
    buf.get_u32()
}

// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

/// Computes the next key of the given key.
///
/// If the key has no successor key (e.g. the input is "\xff\xff"), the result
/// would be an empty vector.
///
/// # Examples
///
/// ```rust
/// use risingwave_hummock_sdk::key::next_key;
/// assert_eq!(next_key(b"123"), b"124");
/// assert_eq!(next_key(b"12\xff"), b"13");
/// assert_eq!(next_key(b"\xff\xff"), b"");
/// assert_eq!(next_key(b"\xff\xfe"), b"\xff\xff");
/// assert_eq!(next_key(b"T"), b"U");
/// assert_eq!(next_key(b""), b"");
/// ```
pub fn next_key(key: &[u8]) -> Vec<u8> {
    if let Some((s, e)) = next_key_no_alloc(key) {
        let mut res = Vec::with_capacity(s.len() + 1);
        res.extend_from_slice(s);
        res.push(e);
        res
    } else {
        Vec::new()
    }
}

/// Computes the previous key of the given key.
///
/// If the key has no predecessor key (e.g. the input is "\x00\x00"), the result
/// would be a "\xff\xff" vector.
///
/// # Examples
///
/// ```rust
/// use risingwave_hummock_sdk::key::prev_key;
/// assert_eq!(prev_key(b"123"), b"122");
/// assert_eq!(prev_key(b"12\x00"), b"11\xff");
/// assert_eq!(prev_key(b"\x00\x00"), b"\xff\xff");
/// assert_eq!(prev_key(b"\x00\x01"), b"\x00\x00");
/// assert_eq!(prev_key(b"T"), b"S");
/// assert_eq!(prev_key(b""), b"");
/// ```
pub fn prev_key(key: &[u8]) -> Vec<u8> {
    let pos = key.iter().rposition(|b| *b != 0x00);
    match pos {
        Some(pos) => {
            let mut res = Vec::with_capacity(key.len());
            res.extend_from_slice(&key[0..pos]);
            res.push(key[pos] - 1);
            if pos + 1 < key.len() {
                res.push(b"\xff".to_owned()[0]);
            }
            res
        }
        None => {
            vec![0xff; key.len()]
        }
    }
}

fn next_key_no_alloc(key: &[u8]) -> Option<(&[u8], u8)> {
    let pos = key.iter().rposition(|b| *b != 0xff)?;
    Some((&key[..pos], key[pos] + 1))
}

// End Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

/// compute the next epoch, and don't change the bytes of the u8 slice.
/// # Examples
///
/// ```rust
/// use risingwave_hummock_sdk::key::next_epoch;
/// assert_eq!(next_epoch(b"123"), b"124");
/// assert_eq!(next_epoch(b"\xff\x00\xff"), b"\xff\x01\x00");
/// assert_eq!(next_epoch(b"\xff\xff"), b"\x00\x00");
/// assert_eq!(next_epoch(b"\x00\x00"), b"\x00\x01");
/// assert_eq!(next_epoch(b"S"), b"T");
/// assert_eq!(next_epoch(b""), b"");
/// ```
pub fn next_epoch(epoch: &[u8]) -> Vec<u8> {
    let pos = epoch.iter().rposition(|b| *b != 0xff);
    match pos {
        Some(mut pos) => {
            let mut res = Vec::with_capacity(epoch.len());
            res.extend_from_slice(&epoch[0..pos]);
            res.push(epoch[pos] + 1);
            while pos + 1 < epoch.len() {
                res.push(0x00);
                pos += 1;
            }
            res
        }
        None => {
            vec![0x00; epoch.len()]
        }
    }
}

/// compute the prev epoch, and don't change the bytes of the u8 slice.
/// # Examples
///
/// ```rust
/// use risingwave_hummock_sdk::key::prev_epoch;
/// assert_eq!(prev_epoch(b"124"), b"123");
/// assert_eq!(prev_epoch(b"\xff\x01\x00"), b"\xff\x00\xff");
/// assert_eq!(prev_epoch(b"\x00\x00"), b"\xff\xff");
/// assert_eq!(prev_epoch(b"\x00\x01"), b"\x00\x00");
/// assert_eq!(prev_epoch(b"T"), b"S");
/// assert_eq!(prev_epoch(b""), b"");
/// ```
pub fn prev_epoch(epoch: &[u8]) -> Vec<u8> {
    let pos = epoch.iter().rposition(|b| *b != 0x00);
    match pos {
        Some(mut pos) => {
            let mut res = Vec::with_capacity(epoch.len());
            res.extend_from_slice(&epoch[0..pos]);
            res.push(epoch[pos] - 1);
            while pos + 1 < epoch.len() {
                res.push(0xff);
                pos += 1;
            }
            res
        }
        None => {
            vec![0xff; epoch.len()]
        }
    }
}

/// compute the next full key of the given full key
///
/// if the `user_key` has no successor key, the result will be a empty vec
pub fn next_full_key(full_key: &[u8]) -> Vec<u8> {
    let (user_key, epoch) = split_key_epoch(full_key);
    let prev_epoch = prev_epoch(epoch);
    let mut res = Vec::with_capacity(full_key.len());
    if prev_epoch.cmp(&vec![0xff; prev_epoch.len()]) == Ordering::Equal {
        let next_user_key = next_key(user_key);
        if next_user_key.is_empty() {
            return Vec::new();
        }
        res.extend_from_slice(next_user_key.as_slice());
        res.extend_from_slice(prev_epoch.as_slice());
        res
    } else {
        res.extend_from_slice(user_key);
        res.extend_from_slice(prev_epoch.as_slice());
        res
    }
}

/// compute the prev full key of the given full key
///
/// if the `user_key` has no predecessor key, the result will be a empty vec
pub fn prev_full_key(full_key: &[u8]) -> Vec<u8> {
    let (user_key, epoch) = split_key_epoch(full_key);
    let next_epoch = next_epoch(epoch);
    let mut res = Vec::with_capacity(full_key.len());
    if next_epoch.cmp(&vec![0x00; next_epoch.len()]) == Ordering::Equal {
        let prev_user_key = prev_key(user_key);
        if prev_user_key.cmp(&vec![0xff; prev_user_key.len()]) == Ordering::Equal {
            return Vec::new();
        }
        res.extend_from_slice(prev_user_key.as_slice());
        res.extend_from_slice(next_epoch.as_slice());
        res
    } else {
        res.extend_from_slice(user_key);
        res.extend_from_slice(next_epoch.as_slice());
        res
    }
}

/// [`Unbounded`] if the vnode is the maximum representable value (i.e. [`VirtualNode::MAX_REPRESENTABLE`]),
/// otherwise [`Excluded`] the next vnode.
///
/// Note that this function is not aware of the vnode count that is actually used in this table.
/// For example, if the total vnode count is 256, `Unbounded` can be a correct end bound for vnode 255,
/// but this function will still return `Excluded(256)`. See also [`vnode`] and [`vnode_range`] which
/// rely on such invariant.
pub fn end_bound_of_vnode(vnode: VirtualNode) -> Bound<Bytes> {
    if vnode == VirtualNode::MAX_REPRESENTABLE {
        Unbounded
    } else {
        let end_bound_index = vnode.to_index() + 1;
        Excluded(Bytes::copy_from_slice(
            &VirtualNode::from_index(end_bound_index).to_be_bytes(),
        ))
    }
}

/// Get the end bound of the given `prefix` when transforming it to a key range.
pub fn end_bound_of_prefix(prefix: &[u8]) -> Bound<Bytes> {
    if let Some((s, e)) = next_key_no_alloc(prefix) {
        let mut buf = BytesMut::with_capacity(s.len() + 1);
        buf.extend_from_slice(s);
        buf.put_u8(e);
        Excluded(buf.freeze())
    } else {
        Unbounded
    }
}

/// Get the start bound of the given `prefix` when it is excluded from the range.
pub fn start_bound_of_excluded_prefix(prefix: &[u8]) -> Bound<Bytes> {
    if let Some((s, e)) = next_key_no_alloc(prefix) {
        let mut buf = BytesMut::with_capacity(s.len() + 1);
        buf.extend_from_slice(s);
        buf.put_u8(e);
        Included(buf.freeze())
    } else {
        panic!("the prefix is the maximum value")
    }
}

/// Transform the given `prefix` to a key range.
pub fn range_of_prefix(prefix: &[u8]) -> (Bound<Bytes>, Bound<Bytes>) {
    if prefix.is_empty() {
        (Unbounded, Unbounded)
    } else {
        (
            Included(Bytes::copy_from_slice(prefix)),
            end_bound_of_prefix(prefix),
        )
    }
}

pub fn prefix_slice_with_vnode(vnode: VirtualNode, slice: &[u8]) -> Bytes {
    let prefix = vnode.to_be_bytes();
    let mut buf = BytesMut::with_capacity(prefix.len() + slice.len());
    buf.extend_from_slice(&prefix);
    buf.extend_from_slice(slice);
    buf.freeze()
}

/// Prepend the `prefix` to the given key `range`.
pub fn prefixed_range_with_vnode<B: AsRef<[u8]>>(
    range: impl RangeBounds<B>,
    vnode: VirtualNode,
) -> TableKeyRange {
    let prefixed = |b: &B| -> Bytes { prefix_slice_with_vnode(vnode, b.as_ref()) };

    let start: Bound<Bytes> = match range.start_bound() {
        Included(b) => Included(prefixed(b)),
        Excluded(b) => {
            assert!(!b.as_ref().is_empty());
            Excluded(prefixed(b))
        }
        Unbounded => Included(Bytes::copy_from_slice(&vnode.to_be_bytes())),
    };

    let end = match range.end_bound() {
        Included(b) => Included(prefixed(b)),
        Excluded(b) => {
            assert!(!b.as_ref().is_empty());
            Excluded(prefixed(b))
        }
        Unbounded => end_bound_of_vnode(vnode),
    };

    map_table_key_range((start, end))
}

pub trait SetSlice<S: AsRef<[u8]> + ?Sized> {
    fn set(&mut self, value: &S);
}

impl<S: AsRef<[u8]> + ?Sized> SetSlice<S> for Vec<u8> {
    fn set(&mut self, value: &S) {
        self.clear();
        self.extend_from_slice(value.as_ref());
    }
}

impl SetSlice<Bytes> for Bytes {
    fn set(&mut self, value: &Bytes) {
        *self = value.clone()
    }
}

pub trait CopyFromSlice: Send + 'static {
    fn copy_from_slice(slice: &[u8]) -> Self;
}

impl CopyFromSlice for Vec<u8> {
    fn copy_from_slice(slice: &[u8]) -> Self {
        Vec::from(slice)
    }
}

impl CopyFromSlice for Bytes {
    fn copy_from_slice(slice: &[u8]) -> Self {
        Bytes::copy_from_slice(slice)
    }
}

impl CopyFromSlice for () {
    fn copy_from_slice(_: &[u8]) -> Self {}
}

/// [`TableKey`] is an internal concept in storage. It's a wrapper around the key directly from the
/// user, to make the code clearer and avoid confusion with encoded [`UserKey`] and [`FullKey`].
///
/// Its name come from the assumption that Hummock is always accessed by a table-like structure
/// identified by a [`TableId`].
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TableKey<T: AsRef<[u8]>>(pub T);

impl<T: AsRef<[u8]>> Debug for TableKey<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableKey {{ {} }}", hex::encode(self.0.as_ref()))
    }
}

impl<T: AsRef<[u8]>> Deref for TableKey<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: AsRef<[u8]>> DerefMut for TableKey<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: AsRef<[u8]>> AsRef<[u8]> for TableKey<T> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl TableKey<Bytes> {
    pub fn split_vnode_bytes(&self) -> (VirtualNode, Bytes) {
        debug_assert!(
            self.0.len() >= VirtualNode::SIZE,
            "too short table key: {:?}",
            self.0.as_ref()
        );
        let (vnode, _) = self.0.split_first_chunk::<{ VirtualNode::SIZE }>().unwrap();
        (
            VirtualNode::from_be_bytes(*vnode),
            self.0.slice(VirtualNode::SIZE..),
        )
    }
}

impl<T: AsRef<[u8]>> TableKey<T> {
    pub fn split_vnode(&self) -> (VirtualNode, &[u8]) {
        debug_assert!(
            self.0.as_ref().len() >= VirtualNode::SIZE,
            "too short table key: {:?}",
            self.0.as_ref()
        );
        let (vnode, inner_key) = self
            .0
            .as_ref()
            .split_first_chunk::<{ VirtualNode::SIZE }>()
            .unwrap();
        (VirtualNode::from_be_bytes(*vnode), inner_key)
    }

    pub fn vnode_part(&self) -> VirtualNode {
        self.split_vnode().0
    }

    pub fn key_part(&self) -> &[u8] {
        self.split_vnode().1
    }

    pub fn to_ref(&self) -> TableKey<&[u8]> {
        TableKey(self.0.as_ref())
    }
}

impl<T: AsRef<[u8]>> Borrow<[u8]> for TableKey<T> {
    fn borrow(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl EstimateSize for TableKey<Bytes> {
    fn estimated_heap_size(&self) -> usize {
        self.0.estimated_heap_size()
    }
}

impl TableKey<&[u8]> {
    pub fn copy_into<T: CopyFromSlice + AsRef<[u8]>>(&self) -> TableKey<T> {
        TableKey(T::copy_from_slice(self.as_ref()))
    }
}

#[inline]
pub fn map_table_key_range(range: (Bound<KeyPayloadType>, Bound<KeyPayloadType>)) -> TableKeyRange {
    (range.0.map(TableKey), range.1.map(TableKey))
}

pub fn gen_key_from_bytes(vnode: VirtualNode, payload: &[u8]) -> TableKey<Bytes> {
    TableKey(Bytes::from(
        [vnode.to_be_bytes().as_slice(), payload].concat(),
    ))
}

pub fn gen_key_from_str(vnode: VirtualNode, payload: &str) -> TableKey<Bytes> {
    gen_key_from_bytes(vnode, payload.as_bytes())
}

/// [`UserKey`] is is an internal concept in storage. In the storage interface, user specifies
/// `table_key` and `table_id` (in `ReadOptions` or `WriteOptions`) as the input. The storage
/// will group these two values into one struct for convenient filtering.
///
/// The encoded format is | `table_id` | `table_key` |.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct UserKey<T: AsRef<[u8]>> {
    // When comparing `UserKey`, we first compare `table_id`, then `table_key`. So the order of
    // declaration matters.
    pub table_id: TableId,
    pub table_key: TableKey<T>,
}

impl<T: AsRef<[u8]>> Debug for UserKey<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UserKey {{ {}, {:?} }}", self.table_id, self.table_key)
    }
}

impl<T: AsRef<[u8]>> UserKey<T> {
    pub fn new(table_id: TableId, table_key: TableKey<T>) -> Self {
        Self {
            table_id,
            table_key,
        }
    }

    /// Pass the inner type of `table_key` to make the code less verbose.
    pub fn for_test(table_id: TableId, table_key: T) -> Self {
        Self {
            table_id,
            table_key: TableKey(table_key),
        }
    }

    /// Encode in to a buffer.
    pub fn encode_into(&self, buf: &mut impl BufMut) {
        buf.put_u32(self.table_id.as_raw_id());
        buf.put_slice(self.table_key.as_ref());
    }

    pub fn encode_table_key_into(&self, buf: &mut impl BufMut) {
        buf.put_slice(self.table_key.as_ref());
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut ret = Vec::with_capacity(TABLE_PREFIX_LEN + self.table_key.as_ref().len());
        self.encode_into(&mut ret);
        ret
    }

    pub fn is_empty(&self) -> bool {
        self.table_key.as_ref().is_empty()
    }

    /// Get the length of the encoded format.
    pub fn encoded_len(&self) -> usize {
        self.table_key.as_ref().len() + TABLE_PREFIX_LEN
    }

    pub fn get_vnode_id(&self) -> usize {
        self.table_key.vnode_part().to_index()
    }
}

impl<'a> UserKey<&'a [u8]> {
    /// Construct a [`UserKey`] from a byte slice. Its `table_key` will be a part of the input
    /// `slice`.
    pub fn decode(slice: &'a [u8]) -> Self {
        let table_id: u32 = (&slice[..]).get_u32();

        Self {
            table_id: TableId::new(table_id),
            table_key: TableKey(&slice[TABLE_PREFIX_LEN..]),
        }
    }

    pub fn to_vec(self) -> UserKey<Vec<u8>> {
        self.copy_into()
    }

    pub fn copy_into<T: CopyFromSlice + AsRef<[u8]>>(self) -> UserKey<T> {
        UserKey {
            table_id: self.table_id,
            table_key: TableKey(T::copy_from_slice(self.table_key.0)),
        }
    }
}

impl<T: AsRef<[u8]> + Clone> UserKey<&T> {
    pub fn cloned(self) -> UserKey<T> {
        UserKey {
            table_id: self.table_id,
            table_key: TableKey(self.table_key.0.clone()),
        }
    }
}

impl<T: AsRef<[u8]>> UserKey<T> {
    pub fn as_ref(&self) -> UserKey<&[u8]> {
        UserKey::new(self.table_id, TableKey(self.table_key.as_ref()))
    }
}

impl<T: AsRef<[u8]>> UserKey<T> {
    /// Use this method to override an old `UserKey<Vec<u8>>` with a `UserKey<&[u8]>` to own the
    /// table key without reallocating a new `UserKey` object.
    pub fn set<F>(&mut self, other: UserKey<F>)
    where
        T: SetSlice<F>,
        F: AsRef<[u8]>,
    {
        self.table_id = other.table_id;
        self.table_key.0.set(&other.table_key.0);
    }
}

impl UserKey<Vec<u8>> {
    pub fn into_bytes(self) -> UserKey<Bytes> {
        UserKey {
            table_id: self.table_id,
            table_key: TableKey(Bytes::from(self.table_key.0)),
        }
    }
}

/// [`FullKey`] is an internal concept in storage. It associates [`UserKey`] with an epoch.
///
/// The encoded format is | `user_key` | `epoch` |.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct FullKey<T: AsRef<[u8]>> {
    pub user_key: UserKey<T>,
    pub epoch_with_gap: EpochWithGap,
}

impl<T: AsRef<[u8]>> Debug for FullKey<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FullKey {{ {:?}, epoch: {}, epoch_with_gap: {}, spill_offset: {}}}",
            self.user_key,
            self.epoch_with_gap.pure_epoch(),
            self.epoch_with_gap.as_u64(),
            self.epoch_with_gap.as_u64() - self.epoch_with_gap.pure_epoch(),
        )
    }
}

impl<T: AsRef<[u8]>> FullKey<T> {
    pub fn new(table_id: TableId, table_key: TableKey<T>, epoch: HummockEpoch) -> Self {
        Self {
            user_key: UserKey::new(table_id, table_key),
            epoch_with_gap: EpochWithGap::new(epoch, 0),
        }
    }

    pub fn new_with_gap_epoch(
        table_id: TableId,
        table_key: TableKey<T>,
        epoch_with_gap: EpochWithGap,
    ) -> Self {
        Self {
            user_key: UserKey::new(table_id, table_key),
            epoch_with_gap,
        }
    }

    pub fn from_user_key(user_key: UserKey<T>, epoch: HummockEpoch) -> Self {
        Self {
            user_key,
            epoch_with_gap: EpochWithGap::new_from_epoch(epoch),
        }
    }

    /// Pass the inner type of `table_key` to make the code less verbose.
    pub fn for_test(table_id: TableId, table_key: T, epoch: HummockEpoch) -> Self {
        Self {
            user_key: UserKey::for_test(table_id, table_key),
            epoch_with_gap: EpochWithGap::new(epoch, 0),
        }
    }

    /// Encode in to a buffer.
    pub fn encode_into(&self, buf: &mut impl BufMut) {
        self.user_key.encode_into(buf);
        buf.put_u64(self.epoch_with_gap.as_u64());
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            TABLE_PREFIX_LEN + self.user_key.table_key.as_ref().len() + EPOCH_LEN,
        );
        self.encode_into(&mut buf);
        buf
    }

    pub fn encode_reverse_epoch(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            TABLE_PREFIX_LEN + self.user_key.table_key.as_ref().len() + EPOCH_LEN,
        );
        self.user_key.encode_into(&mut buf);
        buf.put_u64(u64::MAX - self.epoch_with_gap.as_u64());
        buf
    }

    pub fn is_empty(&self) -> bool {
        self.user_key.is_empty()
    }

    /// Get the length of the encoded format.
    pub fn encoded_len(&self) -> usize {
        self.user_key.encoded_len() + EPOCH_LEN
    }
}

impl<'a> FullKey<&'a [u8]> {
    /// Construct a [`FullKey`] from a byte slice.
    pub fn decode(slice: &'a [u8]) -> Self {
        let epoch_pos = slice.len() - EPOCH_LEN;
        let epoch = (&slice[epoch_pos..]).get_u64();

        Self {
            user_key: UserKey::decode(&slice[..epoch_pos]),
            epoch_with_gap: EpochWithGap::from_u64(epoch),
        }
    }

    /// Construct a [`FullKey`] from a byte slice without `table_id` encoded.
    pub fn from_slice_without_table_id(
        table_id: TableId,
        slice_without_table_id: &'a [u8],
    ) -> Self {
        let epoch_pos = slice_without_table_id.len() - EPOCH_LEN;
        let epoch = (&slice_without_table_id[epoch_pos..]).get_u64();

        Self {
            user_key: UserKey::new(table_id, TableKey(&slice_without_table_id[..epoch_pos])),
            epoch_with_gap: EpochWithGap::from_u64(epoch),
        }
    }

    /// Construct a [`FullKey`] from a byte slice.
    pub fn decode_reverse_epoch(slice: &'a [u8]) -> Self {
        let epoch_pos = slice.len() - EPOCH_LEN;
        let epoch = (&slice[epoch_pos..]).get_u64();

        Self {
            user_key: UserKey::decode(&slice[..epoch_pos]),
            epoch_with_gap: EpochWithGap::from_u64(u64::MAX - epoch),
        }
    }

    pub fn to_vec(self) -> FullKey<Vec<u8>> {
        self.copy_into()
    }

    pub fn copy_into<T: CopyFromSlice + AsRef<[u8]>>(self) -> FullKey<T> {
        FullKey {
            user_key: self.user_key.copy_into(),
            epoch_with_gap: self.epoch_with_gap,
        }
    }
}

impl FullKey<Vec<u8>> {
    /// Calling this method may accidentally cause memory allocation when converting `Vec` into
    /// `Bytes`
    pub fn into_bytes(self) -> FullKey<Bytes> {
        FullKey {
            epoch_with_gap: self.epoch_with_gap,
            user_key: self.user_key.into_bytes(),
        }
    }
}

impl<T: AsRef<[u8]>> FullKey<T> {
    pub fn to_ref(&self) -> FullKey<&[u8]> {
        FullKey {
            user_key: self.user_key.as_ref(),
            epoch_with_gap: self.epoch_with_gap,
        }
    }
}

impl<T: AsRef<[u8]>> FullKey<T> {
    /// Use this method to override an old `FullKey<Vec<u8>>` with a `FullKey<&[u8]>` to own the
    /// table key without reallocating a new `FullKey` object.
    pub fn set<F>(&mut self, other: FullKey<F>)
    where
        T: SetSlice<F>,
        F: AsRef<[u8]>,
    {
        self.user_key.set(other.user_key);
        self.epoch_with_gap = other.epoch_with_gap;
    }
}

impl<T: AsRef<[u8]> + Ord + Eq> Ord for FullKey<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // When `user_key` is the same, greater epoch comes first.
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.epoch_with_gap.cmp(&self.epoch_with_gap))
    }
}

impl<T: AsRef<[u8]> + Ord + Eq> PartialOrd for FullKey<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub mod range_delete_backward_compatibility_serde_struct {
    use bytes::{Buf, BufMut};
    use risingwave_common::catalog::TableId;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
    pub struct TableKey(Vec<u8>);

    #[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
    #[serde(from = "UserKeySerde", into = "UserKeySerde")]
    pub struct UserKey {
        // When comparing `UserKey`, we first compare `table_id`, then `table_key`. So the order of
        // declaration matters.
        pub table_id: TableId,
        pub table_key: TableKey,
    }

    #[derive(Deserialize, Serialize)]
    pub struct TableIdSerde {
        table_id: u32,
    }

    #[derive(Deserialize, Serialize)]
    struct UserKeySerde {
        table_id: TableIdSerde,
        table_key: TableKey,
    }

    impl From<UserKeySerde> for UserKey {
        fn from(value: UserKeySerde) -> Self {
            Self {
                table_id: TableId::new(value.table_id.table_id),
                table_key: value.table_key,
            }
        }
    }

    impl From<UserKey> for UserKeySerde {
        fn from(value: UserKey) -> Self {
            Self {
                table_id: TableIdSerde {
                    table_id: value.table_id.as_raw_id(),
                },
                table_key: value.table_key,
            }
        }
    }

    impl UserKey {
        pub fn decode_length_prefixed(buf: &mut &[u8]) -> Self {
            let table_id = buf.get_u32();
            let len = buf.get_u32() as usize;
            let data = buf[..len].to_vec();
            buf.advance(len);
            UserKey {
                table_id: TableId::new(table_id),
                table_key: TableKey(data),
            }
        }

        pub fn encode_length_prefixed(&self, mut buf: impl BufMut) {
            buf.put_u32(self.table_id.as_raw_id());
            buf.put_u32(self.table_key.0.as_slice().len() as u32);
            buf.put_slice(self.table_key.0.as_slice());
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
    pub struct PointRange {
        // When comparing `PointRange`, we first compare `left_user_key`, then
        // `is_exclude_left_key`. Therefore the order of declaration matters.
        pub left_user_key: UserKey,
        /// `PointRange` represents the left user key itself if `is_exclude_left_key==false`
        /// while represents the right δ Neighborhood of the left user key if
        /// `is_exclude_left_key==true`.
        pub is_exclude_left_key: bool,
    }
}

pub trait EmptySliceRef {
    fn empty_slice_ref<'a>() -> &'a Self;
}

static EMPTY_BYTES: Bytes = Bytes::new();
impl EmptySliceRef for Bytes {
    fn empty_slice_ref<'a>() -> &'a Self {
        &EMPTY_BYTES
    }
}

static EMPTY_VEC: Vec<u8> = Vec::new();
impl EmptySliceRef for Vec<u8> {
    fn empty_slice_ref<'a>() -> &'a Self {
        &EMPTY_VEC
    }
}

const EMPTY_SLICE: &[u8] = b"";
impl EmptySliceRef for &[u8] {
    fn empty_slice_ref<'b>() -> &'b Self {
        &EMPTY_SLICE
    }
}

/// Bound table key range with table id to generate a new user key range.
pub fn bound_table_key_range<T: AsRef<[u8]> + EmptySliceRef>(
    table_id: TableId,
    table_key_range: &impl RangeBounds<TableKey<T>>,
) -> (Bound<UserKey<&T>>, Bound<UserKey<&T>>) {
    let start = match table_key_range.start_bound() {
        Included(b) => Included(UserKey::new(table_id, TableKey(&b.0))),
        Excluded(b) => Excluded(UserKey::new(table_id, TableKey(&b.0))),
        Unbounded => Included(UserKey::new(table_id, TableKey(T::empty_slice_ref()))),
    };

    let end = match table_key_range.end_bound() {
        Included(b) => Included(UserKey::new(table_id, TableKey(&b.0))),
        Excluded(b) => Excluded(UserKey::new(table_id, TableKey(&b.0))),
        Unbounded => {
            if let Some(next_table_id) = table_id.as_raw_id().checked_add(1) {
                Excluded(UserKey::new(
                    next_table_id.into(),
                    TableKey(T::empty_slice_ref()),
                ))
            } else {
                Unbounded
            }
        }
    };

    (start, end)
}

/// TODO: Temporary bypass full key check. Remove this field after #15099 is resolved.
pub struct FullKeyTracker<T: AsRef<[u8]> + Ord + Eq, const SKIP_DEDUP: bool = false> {
    pub latest_full_key: FullKey<T>,
    last_observed_epoch_with_gap: EpochWithGap,
}

impl<T: AsRef<[u8]> + Ord + Eq, const SKIP_DEDUP: bool> FullKeyTracker<T, SKIP_DEDUP> {
    pub fn new(init_full_key: FullKey<T>) -> Self {
        let epoch_with_gap = init_full_key.epoch_with_gap;
        Self {
            latest_full_key: init_full_key,
            last_observed_epoch_with_gap: epoch_with_gap,
        }
    }

    /// Check and observe a new full key during iteration
    ///
    /// # Examples:
    /// ```
    /// use bytes::Bytes;
    /// use risingwave_common::catalog::TableId;
    /// use risingwave_common::util::epoch::EPOCH_AVAILABLE_BITS;
    /// use risingwave_hummock_sdk::EpochWithGap;
    /// use risingwave_hummock_sdk::key::{FullKey, FullKeyTracker, TableKey};
    ///
    /// let table_id = TableId::new(1);
    /// let full_key1 = FullKey::new(table_id, TableKey(Bytes::from("c")), 5 << EPOCH_AVAILABLE_BITS);
    /// let mut a: FullKeyTracker<_> = FullKeyTracker::<Bytes>::new(full_key1.clone());
    ///
    /// // Panic on non-decreasing epoch observed for the same user key.
    /// // let full_key_with_larger_epoch = FullKey::new(table_id, TableKey(Bytes::from("c")), 6 << EPOCH_AVAILABLE_BITS);
    /// // a.observe(full_key_with_larger_epoch);
    ///
    /// // Panic on non-increasing user key observed.
    /// // let full_key_with_smaller_user_key = FullKey::new(table_id, TableKey(Bytes::from("b")), 3 << EPOCH_AVAILABLE_BITS);
    /// // a.observe(full_key_with_smaller_user_key);
    ///
    /// let full_key2 = FullKey::new(table_id, TableKey(Bytes::from("c")), 3 << EPOCH_AVAILABLE_BITS);
    /// assert_eq!(a.observe(full_key2.clone()), false);
    /// assert_eq!(a.latest_user_key(), &full_key2.user_key);
    ///
    /// let full_key3 = FullKey::new(table_id, TableKey(Bytes::from("f")), 4 << EPOCH_AVAILABLE_BITS);
    /// assert_eq!(a.observe(full_key3.clone()), true);
    /// assert_eq!(a.latest_user_key(), &full_key3.user_key);
    /// ```
    ///
    /// Return:
    /// - If the provided `key` contains a new user key, return true.
    /// - Otherwise: return false
    pub fn observe<F>(&mut self, key: FullKey<F>) -> bool
    where
        T: SetSlice<F>,
        F: AsRef<[u8]>,
    {
        self.observe_multi_version(key.user_key, once(key.epoch_with_gap))
    }

    /// `epochs` comes from greater to smaller
    pub fn observe_multi_version<F>(
        &mut self,
        user_key: UserKey<F>,
        mut epochs: impl Iterator<Item = EpochWithGap>,
    ) -> bool
    where
        T: SetSlice<F>,
        F: AsRef<[u8]>,
    {
        let max_epoch_with_gap = epochs.next().expect("non-empty");
        let min_epoch_with_gap = epochs.fold(
            max_epoch_with_gap,
            |prev_epoch_with_gap, curr_epoch_with_gap| {
                assert!(
                    prev_epoch_with_gap > curr_epoch_with_gap,
                    "epoch list not sorted. prev: {:?}, curr: {:?}, user_key: {:?}",
                    prev_epoch_with_gap,
                    curr_epoch_with_gap,
                    user_key
                );
                curr_epoch_with_gap
            },
        );
        match self
            .latest_full_key
            .user_key
            .as_ref()
            .cmp(&user_key.as_ref())
        {
            Ordering::Less => {
                // Observe a new user key

                // Reset epochs
                self.last_observed_epoch_with_gap = min_epoch_with_gap;

                // Take the previous key and set latest key
                self.latest_full_key.set(FullKey {
                    user_key,
                    epoch_with_gap: min_epoch_with_gap,
                });
                true
            }
            Ordering::Equal => {
                if max_epoch_with_gap > self.last_observed_epoch_with_gap
                    || (!SKIP_DEDUP && max_epoch_with_gap == self.last_observed_epoch_with_gap)
                {
                    // Epoch from the same user key should be monotonically decreasing
                    panic!(
                        "key {:?} epoch {:?} >= prev epoch {:?}",
                        user_key, max_epoch_with_gap, self.last_observed_epoch_with_gap
                    );
                }
                self.last_observed_epoch_with_gap = min_epoch_with_gap;
                false
            }
            Ordering::Greater => {
                // User key should be monotonically increasing
                panic!(
                    "key {:?} <= prev key {:?}",
                    user_key,
                    FullKey {
                        user_key: self.latest_full_key.user_key.as_ref(),
                        epoch_with_gap: self.last_observed_epoch_with_gap
                    }
                );
            }
        }
    }

    pub fn latest_user_key(&self) -> &UserKey<T> {
        &self.latest_full_key.user_key
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::util::epoch::test_epoch;

    use super::*;

    #[test]
    fn test_encode_decode() {
        let epoch = test_epoch(1);
        let table_key = b"abc".to_vec();
        let key = FullKey::for_test(TableId::new(0), &table_key[..], 0);
        let buf = key.encode();
        assert_eq!(FullKey::decode(&buf), key);
        let key = FullKey::for_test(TableId::new(1), &table_key[..], epoch);
        let buf = key.encode();
        assert_eq!(FullKey::decode(&buf), key);
        let mut table_key = vec![1];
        let a = FullKey::for_test(TableId::new(1), table_key.clone(), epoch);
        table_key[0] = 2;
        let b = FullKey::for_test(TableId::new(1), table_key.clone(), epoch);
        table_key[0] = 129;
        let c = FullKey::for_test(TableId::new(1), table_key, epoch);
        assert!(a.lt(&b));
        assert!(b.lt(&c));
    }

    #[test]
    fn test_key_cmp() {
        let epoch = test_epoch(1);
        let epoch2 = test_epoch(2);
        // 1 compared with 256 under little-endian encoding would return wrong result.
        let key1 = FullKey::for_test(TableId::new(0), b"0".to_vec(), epoch);
        let key2 = FullKey::for_test(TableId::new(1), b"0".to_vec(), epoch);
        let key3 = FullKey::for_test(TableId::new(1), b"1".to_vec(), epoch2);
        let key4 = FullKey::for_test(TableId::new(1), b"1".to_vec(), epoch);

        assert_eq!(key1.cmp(&key1), Ordering::Equal);
        assert_eq!(key1.cmp(&key2), Ordering::Less);
        assert_eq!(key2.cmp(&key3), Ordering::Less);
        assert_eq!(key3.cmp(&key4), Ordering::Less);
    }

    #[test]
    fn test_prev_key() {
        assert_eq!(prev_key(b"123"), b"122");
        assert_eq!(prev_key(b"12\x00"), b"11\xff");
        assert_eq!(prev_key(b"\x00\x00"), b"\xff\xff");
        assert_eq!(prev_key(b"\x00\x01"), b"\x00\x00");
        assert_eq!(prev_key(b"T"), b"S");
        assert_eq!(prev_key(b""), b"");
    }

    #[test]
    fn test_bound_table_key_range() {
        assert_eq!(
            bound_table_key_range(
                TableId::default(),
                &(
                    Included(TableKey(b"a".to_vec())),
                    Included(TableKey(b"b".to_vec()))
                )
            ),
            (
                Included(UserKey::for_test(TableId::default(), &b"a".to_vec())),
                Included(UserKey::for_test(TableId::default(), &b"b".to_vec()),)
            )
        );
        assert_eq!(
            bound_table_key_range(
                TableId::from(1),
                &(Included(TableKey(b"a".to_vec())), Unbounded)
            ),
            (
                Included(UserKey::for_test(TableId::from(1), &b"a".to_vec())),
                Excluded(UserKey::for_test(TableId::from(2), &b"".to_vec()),)
            )
        );
        assert_eq!(
            bound_table_key_range(
                TableId::from(u32::MAX),
                &(Included(TableKey(b"a".to_vec())), Unbounded)
            ),
            (
                Included(UserKey::for_test(TableId::from(u32::MAX), &b"a".to_vec())),
                Unbounded,
            )
        );
    }

    #[test]
    fn test_next_full_key() {
        let user_key = b"aaa".to_vec();
        let epoch: HummockEpoch = 3;
        let mut full_key = key_with_epoch(user_key, epoch);
        full_key = next_full_key(full_key.as_slice());
        assert_eq!(full_key, key_with_epoch(b"aaa".to_vec(), 2));
        full_key = next_full_key(full_key.as_slice());
        assert_eq!(full_key, key_with_epoch(b"aaa".to_vec(), 1));
        full_key = next_full_key(full_key.as_slice());
        assert_eq!(full_key, key_with_epoch(b"aaa".to_vec(), 0));
        full_key = next_full_key(full_key.as_slice());
        assert_eq!(
            full_key,
            key_with_epoch("aab".as_bytes().to_vec(), HummockEpoch::MAX)
        );
        assert_eq!(
            next_full_key(&key_with_epoch(b"\xff".to_vec(), 0)),
            Vec::<u8>::new()
        );
    }

    #[test]
    fn test_prev_full_key() {
        let user_key = b"aab";
        let epoch: HummockEpoch = HummockEpoch::MAX - 3;
        let mut full_key = key_with_epoch(user_key.to_vec(), epoch);
        full_key = prev_full_key(full_key.as_slice());
        assert_eq!(
            full_key,
            key_with_epoch(b"aab".to_vec(), HummockEpoch::MAX - 2)
        );
        full_key = prev_full_key(full_key.as_slice());
        assert_eq!(
            full_key,
            key_with_epoch(b"aab".to_vec(), HummockEpoch::MAX - 1)
        );
        full_key = prev_full_key(full_key.as_slice());
        assert_eq!(full_key, key_with_epoch(b"aab".to_vec(), HummockEpoch::MAX));
        full_key = prev_full_key(full_key.as_slice());
        assert_eq!(full_key, key_with_epoch(b"aaa".to_vec(), 0));

        assert_eq!(
            prev_full_key(&key_with_epoch(b"\x00".to_vec(), HummockEpoch::MAX)),
            Vec::<u8>::new()
        );
    }

    #[test]
    fn test_user_key_order() {
        let a = UserKey::new(TableId::new(1), TableKey(b"aaa".to_vec()));
        let b = UserKey::new(TableId::new(2), TableKey(b"aaa".to_vec()));
        let c = UserKey::new(TableId::new(2), TableKey(b"bbb".to_vec()));
        assert!(a.lt(&b));
        assert!(b.lt(&c));
        let a = a.encode();
        let b = b.encode();
        let c = c.encode();
        assert!(a.lt(&b));
        assert!(b.lt(&c));
    }

    #[test]
    fn test_prefixed_range_with_vnode() {
        let concat = |vnode: usize, b: &[u8]| -> Bytes {
            prefix_slice_with_vnode(VirtualNode::from_index(vnode), b)
        };
        assert_eq!(
            prefixed_range_with_vnode(
                (Included(Bytes::from("1")), Included(Bytes::from("2"))),
                VirtualNode::from_index(233),
            ),
            (
                Included(TableKey(concat(233, b"1"))),
                Included(TableKey(concat(233, b"2")))
            )
        );
        assert_eq!(
            prefixed_range_with_vnode(
                (Excluded(Bytes::from("1")), Excluded(Bytes::from("2"))),
                VirtualNode::from_index(233),
            ),
            (
                Excluded(TableKey(concat(233, b"1"))),
                Excluded(TableKey(concat(233, b"2")))
            )
        );
        assert_eq!(
            prefixed_range_with_vnode(
                (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                VirtualNode::from_index(233),
            ),
            (
                Included(TableKey(concat(233, b""))),
                Excluded(TableKey(concat(234, b"")))
            )
        );
        let max_vnode = VirtualNode::MAX_REPRESENTABLE.to_index();
        assert_eq!(
            prefixed_range_with_vnode(
                (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                VirtualNode::from_index(max_vnode),
            ),
            (Included(TableKey(concat(max_vnode, b""))), Unbounded)
        );
        let second_max_vnode = max_vnode - 1;
        assert_eq!(
            prefixed_range_with_vnode(
                (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                VirtualNode::from_index(second_max_vnode),
            ),
            (
                Included(TableKey(concat(second_max_vnode, b""))),
                Excluded(TableKey(concat(max_vnode, b"")))
            )
        );
    }

    #[test]
    fn test_single_vnode_range() {
        let left_bound = vec![
            Included(b"0".as_slice()),
            Excluded(b"0".as_slice()),
            Unbounded,
        ];
        let right_bound = vec![
            Included(b"1".as_slice()),
            Excluded(b"1".as_slice()),
            Unbounded,
        ];
        for vnode in 0..VirtualNode::MAX_COUNT {
            for left in &left_bound {
                for right in &right_bound {
                    assert_eq!(
                        (vnode, vnode + 1),
                        vnode_range(&prefixed_range_with_vnode::<&[u8]>(
                            (*left, *right),
                            VirtualNode::from_index(vnode)
                        ))
                    )
                }
            }
        }
    }
}
