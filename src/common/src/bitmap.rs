// Copyright 2024 RisingWave Labs
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

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines a bitmap, which is used to track which values in an Arrow array are null.
//! This is called a "validity bitmap" in the Arrow documentation.
//! This file is adapted from [arrow-rs](https://github.com/apache/arrow-rs)

// allow `zip` for performance reasons
#![allow(clippy::disallowed_methods)]

use std::borrow::Borrow;
use std::iter::{self, TrustedLen};
use std::ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, Not, Range, RangeInclusive};

use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::common::PbBuffer;
use risingwave_pb::common::buffer::CompressionType;
use rw_iter_util::ZipEqFast;

#[derive(Default, Debug, Clone, EstimateSize)]
pub struct BitmapBuilder {
    len: usize,
    data: Vec<usize>,
}

const BITS: usize = usize::BITS as usize;

impl From<Bitmap> for BitmapBuilder {
    fn from(bitmap: Bitmap) -> Self {
        let len = bitmap.len();
        if let Some(bits) = bitmap.bits {
            BitmapBuilder {
                len,
                data: bits.into_vec(),
            }
        } else if bitmap.count_ones == 0 {
            Self::zeroed(bitmap.len())
        } else {
            debug_assert!(bitmap.len() == bitmap.count_ones());
            Self::filled(bitmap.len())
        }
    }
}

impl BitmapBuilder {
    /// Creates a new empty bitmap with at least the specified capacity.
    pub fn with_capacity(capacity: usize) -> BitmapBuilder {
        BitmapBuilder {
            len: 0,
            data: Vec::with_capacity(Bitmap::vec_len(capacity)),
        }
    }

    /// Creates a new bitmap with all bits set to 0.
    pub fn zeroed(len: usize) -> BitmapBuilder {
        BitmapBuilder {
            len,
            data: vec![0; Bitmap::vec_len(len)],
        }
    }

    /// Creates a new bitmap with all bits set to 1.
    pub fn filled(len: usize) -> BitmapBuilder {
        let vec_len = Bitmap::vec_len(len);
        let mut data = vec![usize::MAX; vec_len];
        if vec_len >= 1 && !len.is_multiple_of(BITS) {
            data[vec_len - 1] = (1 << (len % BITS)) - 1;
        }
        BitmapBuilder { len, data }
    }

    /// Writes a new value into a single bit.
    pub fn set(&mut self, n: usize, val: bool) {
        assert!(n < self.len);

        let byte = &mut self.data[n / BITS];
        let mask = 1 << (n % BITS);
        if val {
            *byte |= mask;
        } else {
            *byte &= !mask;
        }
    }

    /// Tests a single bit.
    pub fn is_set(&self, n: usize) -> bool {
        assert!(n < self.len);
        let byte = &self.data[n / BITS];
        let mask = 1 << (n % BITS);
        *byte & mask != 0
    }

    /// Appends a single bit to the back.
    pub fn append(&mut self, bit_set: bool) -> &mut Self {
        if self.len.is_multiple_of(BITS) {
            self.data.push(0);
        }
        self.data[self.len / BITS] |= (bit_set as usize) << (self.len % BITS);
        self.len += 1;
        self
    }

    /// Appends `n` bits to the back.
    pub fn append_n(&mut self, mut n: usize, bit_set: bool) -> &mut Self {
        while n != 0 && !self.len.is_multiple_of(BITS) {
            self.append(bit_set);
            n -= 1;
        }
        self.len += n;
        self.data.resize(
            Bitmap::vec_len(self.len),
            if bit_set { usize::MAX } else { 0 },
        );
        if bit_set && !self.len.is_multiple_of(BITS) {
            // remove tailing 1s
            *self.data.last_mut().unwrap() &= (1 << (self.len % BITS)) - 1;
        }
        self
    }

    /// Removes the last bit.
    pub fn pop(&mut self) -> Option<()> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        self.data.truncate(Bitmap::vec_len(self.len));
        if !self.len.is_multiple_of(BITS) {
            *self.data.last_mut().unwrap() &= (1 << (self.len % BITS)) - 1;
        }
        Some(())
    }

    /// Appends a bitmap to the back.
    pub fn append_bitmap(&mut self, other: &Bitmap) -> &mut Self {
        if self.len.is_multiple_of(BITS) {
            // fast path: self is aligned
            self.len += other.len();
            if let Some(bits) = &other.bits {
                // append the bytes
                self.data.extend_from_slice(bits);
            } else if other.count_ones == 0 {
                // append 0s
                self.data.resize(Bitmap::vec_len(self.len), 0);
            } else {
                // append 1s
                self.data.resize(Bitmap::vec_len(self.len), usize::MAX);
                if !self.len.is_multiple_of(BITS) {
                    // remove tailing 1s
                    *self.data.last_mut().unwrap() = (1 << (self.len % BITS)) - 1;
                }
            }
        } else {
            // slow path: append bits one by one
            for bit in other.iter() {
                self.append(bit);
            }
        }
        self
    }

    /// Finishes building and returns the bitmap.
    pub fn finish(self) -> Bitmap {
        let count_ones = self.data.iter().map(|&x| x.count_ones()).sum::<u32>() as usize;
        Bitmap {
            num_bits: self.len(),
            count_ones,
            bits: (count_ones != 0 && count_ones != self.len).then(|| self.data.into()),
        }
    }

    /// Returns the number of bits in the bitmap.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the bitmap has a length of 0.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// An immutable bitmap. Use [`BitmapBuilder`] to build it.
#[derive(Clone, PartialEq, Eq)]
pub struct Bitmap {
    /// The useful bits in the bitmap. The total number of bits will usually
    /// be larger than the useful bits due to byte-padding.
    num_bits: usize,

    /// The number of high bits in the bitmap.
    count_ones: usize,

    /// Bits are stored in a compact form.
    /// They are packed into `usize`s.
    ///
    /// Optimization: If all bits are set to 0 or 1, this field MUST be `None`.
    bits: Option<Box<[usize]>>,
}

impl EstimateSize for Bitmap {
    fn estimated_heap_size(&self) -> usize {
        match &self.bits {
            Some(bits) => std::mem::size_of_val(bits.as_ref()),
            None => 0,
        }
    }
}

impl std::fmt::Debug for Bitmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for data in self.iter() {
            write!(f, "{}", data as u8)?;
        }
        Ok(())
    }
}

impl Bitmap {
    /// Creates a new bitmap with all bits set to 0.
    pub fn zeros(num_bits: usize) -> Self {
        Self {
            bits: None,
            num_bits,
            count_ones: 0,
        }
    }

    /// Creates a new bitmap with all bits set to 1.
    pub fn ones(num_bits: usize) -> Self {
        Self {
            bits: None,
            num_bits,
            count_ones: num_bits,
        }
    }

    /// Creates a new bitmap from vector.
    fn from_vec_with_len(buf: Vec<usize>, num_bits: usize) -> Self {
        debug_assert_eq!(buf.len(), Self::vec_len(num_bits));
        let count_ones = buf.iter().map(|&x| x.count_ones()).sum::<u32>() as usize;
        debug_assert!(count_ones <= num_bits);
        Self {
            bits: (count_ones != 0 && count_ones != num_bits).then(|| buf.into()),
            num_bits,
            count_ones,
        }
    }

    /// Creates a new bitmap from bytes.
    pub fn from_bytes(buf: &[u8]) -> Self {
        Self::from_bytes_with_len(buf, buf.len() * 8)
    }

    /// Creates a new bitmap from the indices of bits set to 1.
    pub fn from_indices<I, T>(num_bits: usize, ones: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Borrow<usize>,
    {
        let mut builder = BitmapBuilder::zeroed(num_bits);

        for idx in ones {
            let idx = *idx.borrow();
            debug_assert!(idx < num_bits);
            builder.set(idx, true);
        }

        builder.finish()
    }

    /// Creates a new bitmap from bytes and length.
    fn from_bytes_with_len(buf: &[u8], num_bits: usize) -> Self {
        let mut bits = Vec::with_capacity(Self::vec_len(num_bits));
        let slice = unsafe {
            bits.set_len(bits.capacity());
            std::slice::from_raw_parts_mut(bits.as_ptr() as *mut u8, bits.len() * (BITS / 8))
        };
        slice[..buf.len()].copy_from_slice(buf);
        slice[buf.len()..].fill(0);
        Self::from_vec_with_len(bits, num_bits)
    }

    /// Creates a new bitmap from a slice of `bool`.
    pub fn from_bool_slice(bools: &[bool]) -> Self {
        // use SIMD to speed up
        let mut iter = bools.iter().copied().array_chunks::<BITS>();
        let mut bits = Vec::with_capacity(Self::vec_len(bools.len()));
        for chunk in iter.by_ref() {
            let bitmask = std::simd::Mask::<i8, BITS>::from_array(chunk).to_bitmask() as usize;
            bits.push(bitmask);
        }
        // `ArrayChunks::into_remainder` now returns the remainder iterator directly
        // (previously `Option<_>`); the full chunks above are already drained via
        // `by_ref`, so the remainder is simply empty when the length divides evenly.
        let remainder_iter = iter.into_remainder();
        if !remainder_iter.is_empty() {
            let mut bitmask = 0;
            for (i, b) in remainder_iter.enumerate() {
                bitmask |= (b as usize) << i;
            }
            bits.push(bitmask);
        }
        Self::from_vec_with_len(bits, bools.len())
    }

    /// Return the next set bit index on or after `bit_idx`.
    pub fn next_set_bit(&self, bit_idx: usize) -> Option<usize> {
        (bit_idx..self.len()).find(|&idx| unsafe { self.is_set_unchecked(idx) })
    }

    /// Counts the number of bits set to 1.
    pub fn count_ones(&self) -> usize {
        self.count_ones
    }

    /// Returns true if any bit is set to 1.
    pub fn any(&self) -> bool {
        self.count_ones != 0
    }

    /// Returns the length of vector to store `num_bits` bits.
    fn vec_len(num_bits: usize) -> usize {
        num_bits.div_ceil(BITS)
    }

    /// Returns the number of valid bits in the bitmap,
    /// also referred to its 'length'.
    #[inline]
    pub fn len(&self) -> usize {
        self.num_bits
    }

    /// Returns true if the `Bitmap` has a length of 0.
    pub fn is_empty(&self) -> bool {
        self.num_bits == 0
    }

    /// Returns true if the bit at `idx` is set, without doing bounds checking.
    ///
    /// # Safety
    ///
    /// Index must be in range.
    pub unsafe fn is_set_unchecked(&self, idx: usize) -> bool {
        unsafe {
            match &self.bits {
                None => self.count_ones != 0,
                Some(bits) => bits.get_unchecked(idx / BITS) & (1 << (idx % BITS)) != 0,
            }
        }
    }

    /// Returns true if the bit at `idx` is set.
    pub fn is_set(&self, idx: usize) -> bool {
        assert!(idx < self.len());
        unsafe { self.is_set_unchecked(idx) }
    }

    /// Tests if every bit is set to 1.
    pub fn all(&self) -> bool {
        self.count_ones == self.len()
    }

    /// Produces an iterator over each bit.
    pub fn iter(&self) -> BitmapIter<'_> {
        BitmapIter {
            bits: self.bits.as_ref().map(|s| &s[..]),
            idx: 0,
            num_bits: self.num_bits,
            current_usize: 0,
            all_ones: self.count_ones == self.num_bits,
        }
    }

    /// Enumerates the index of each bit set to 1.
    pub fn iter_ones(&self) -> BitmapOnesIter<'_> {
        if let Some(bits) = &self.bits {
            BitmapOnesIter::Buffer {
                bits: &bits[..],
                cur_idx: 0,
                cur_bits: bits.first().cloned(),
            }
        } else {
            // all zeros or all ones
            BitmapOnesIter::Range {
                start: 0,
                end: self.count_ones,
            }
        }
    }

    /// Returns an iterator which yields the position ranges of continuous high bits.
    pub fn high_ranges(&self) -> impl Iterator<Item = RangeInclusive<usize>> + '_ {
        let mut start = None;

        self.iter()
            .chain(iter::once(false))
            .enumerate()
            .filter_map(move |(i, bit)| match (bit, start) {
                // A new high range starts.
                (true, None) => {
                    start = Some(i);
                    None
                }
                // The current high range ends.
                (false, Some(s)) => {
                    start = None;
                    Some(s..=(i - 1))
                }
                _ => None,
            })
    }

    #[cfg(test)]
    fn assert_valid(&self) {
        assert_eq!(
            self.iter().map(|x| x as usize).sum::<usize>(),
            self.count_ones
        )
    }

    /// Creates a new bitmap with all bits in range set to 1.
    ///
    /// # Example
    /// ```
    /// use risingwave_common::bitmap::Bitmap;
    /// let bitmap = Bitmap::from_range(200, 100..180);
    /// assert_eq!(bitmap.count_ones(), 80);
    /// for i in 0..200 {
    ///     assert_eq!(bitmap.is_set(i), i >= 100 && i < 180);
    /// }
    /// ```
    pub fn from_range(num_bits: usize, range: Range<usize>) -> Self {
        assert!(range.start <= range.end);
        assert!(range.end <= num_bits);
        if range.start == range.end {
            return Self::zeros(num_bits);
        } else if range == (0..num_bits) {
            return Self::ones(num_bits);
        }
        let mut bits = vec![0; Self::vec_len(num_bits)];
        let start = range.start / BITS;
        let end = range.end / BITS;
        let start_offset = range.start % BITS;
        let end_offset = range.end % BITS;
        if start == end {
            bits[start] = ((1 << (end_offset - start_offset)) - 1) << start_offset;
        } else {
            bits[start] = !0 << start_offset;
            bits[start + 1..end].fill(!0);
            if end_offset != 0 {
                bits[end] = (1 << end_offset) - 1;
            }
        }
        Self {
            bits: Some(bits.into()),
            num_bits,
            count_ones: range.len(),
        }
    }
}

impl From<usize> for Bitmap {
    fn from(val: usize) -> Self {
        Self::ones(val)
    }
}

impl<'b> BitAnd<&'b Bitmap> for &Bitmap {
    type Output = Bitmap;

    fn bitand(self, rhs: &'b Bitmap) -> Bitmap {
        assert_eq!(self.num_bits, rhs.num_bits);
        let (lbits, rbits) = match (&self.bits, &rhs.bits) {
            _ if self.count_ones == 0 || rhs.count_ones == 0 => {
                return Bitmap::zeros(self.num_bits);
            }
            (_, None) => return self.clone(),
            (None, _) => return rhs.clone(),
            (Some(lbits), Some(rbits)) => (lbits, rbits),
        };
        let bits = (lbits.iter().zip(rbits.iter()))
            .map(|(&a, &b)| a & b)
            .collect();
        Bitmap::from_vec_with_len(bits, self.num_bits)
    }
}

impl BitAnd<Bitmap> for &Bitmap {
    type Output = Bitmap;

    fn bitand(self, rhs: Bitmap) -> Self::Output {
        self.bitand(&rhs)
    }
}

impl<'b> BitAnd<&'b Bitmap> for Bitmap {
    type Output = Bitmap;

    fn bitand(self, rhs: &'b Bitmap) -> Self::Output {
        rhs.bitand(self)
    }
}

impl BitAnd for Bitmap {
    type Output = Bitmap;

    fn bitand(self, rhs: Bitmap) -> Self::Output {
        (&self).bitand(&rhs)
    }
}

impl BitAndAssign<&Bitmap> for Bitmap {
    fn bitand_assign(&mut self, rhs: &Bitmap) {
        *self = &*self & rhs;
    }
}

impl BitAndAssign<Bitmap> for Bitmap {
    fn bitand_assign(&mut self, rhs: Bitmap) {
        *self = &*self & rhs;
    }
}

impl<'b> BitOr<&'b Bitmap> for &Bitmap {
    type Output = Bitmap;

    fn bitor(self, rhs: &'b Bitmap) -> Bitmap {
        assert_eq!(self.num_bits, rhs.num_bits);
        let (lbits, rbits) = match (&self.bits, &rhs.bits) {
            _ if self.count_ones == self.num_bits || rhs.count_ones == self.num_bits => {
                return Bitmap::ones(self.num_bits);
            }
            (_, None) => return self.clone(),
            (None, _) => return rhs.clone(),
            (Some(lbits), Some(rbits)) => (lbits, rbits),
        };
        let bits = (lbits.iter().zip(rbits.iter()))
            .map(|(&a, &b)| a | b)
            .collect();
        Bitmap::from_vec_with_len(bits, self.num_bits)
    }
}

impl BitOr<Bitmap> for &Bitmap {
    type Output = Bitmap;

    fn bitor(self, rhs: Bitmap) -> Self::Output {
        self.bitor(&rhs)
    }
}

impl<'b> BitOr<&'b Bitmap> for Bitmap {
    type Output = Bitmap;

    fn bitor(self, rhs: &'b Bitmap) -> Self::Output {
        rhs.bitor(self)
    }
}

impl BitOr for Bitmap {
    type Output = Bitmap;

    fn bitor(self, rhs: Bitmap) -> Self::Output {
        (&self).bitor(&rhs)
    }
}

impl BitOrAssign<&Bitmap> for Bitmap {
    fn bitor_assign(&mut self, rhs: &Bitmap) {
        *self = &*self | rhs;
    }
}

impl BitOrAssign<Bitmap> for Bitmap {
    fn bitor_assign(&mut self, rhs: Bitmap) {
        *self |= &rhs;
    }
}

impl BitXor for &Bitmap {
    type Output = Bitmap;

    fn bitxor(self, rhs: &Bitmap) -> Self::Output {
        assert_eq!(self.num_bits, rhs.num_bits);
        let (lbits, rbits) = match (&self.bits, &rhs.bits) {
            (_, None) if rhs.count_ones == 0 => return self.clone(),
            (_, None) => return !self,
            (None, _) if self.count_ones == 0 => return rhs.clone(),
            (None, _) => return !rhs,
            (Some(lbits), Some(rbits)) => (lbits, rbits),
        };
        let bits = (lbits.iter().zip(rbits.iter()))
            .map(|(&a, &b)| a ^ b)
            .collect();
        Bitmap::from_vec_with_len(bits, self.num_bits)
    }
}

impl Not for &Bitmap {
    type Output = Bitmap;

    fn not(self) -> Self::Output {
        let bits = match &self.bits {
            None if self.count_ones == 0 => return Bitmap::ones(self.num_bits),
            None => return Bitmap::zeros(self.num_bits),
            Some(bits) => bits,
        };
        let mut bits: Box<[usize]> = bits.iter().map(|b| !b).collect();
        if !self.num_bits.is_multiple_of(BITS) {
            bits[self.num_bits / BITS] &= (1 << (self.num_bits % BITS)) - 1;
        }
        Bitmap {
            num_bits: self.num_bits,
            count_ones: self.num_bits - self.count_ones,
            bits: Some(bits),
        }
    }
}

impl Not for Bitmap {
    type Output = Bitmap;

    fn not(mut self) -> Self::Output {
        let bits = match &mut self.bits {
            None if self.count_ones == 0 => return Bitmap::ones(self.num_bits),
            None => return Bitmap::zeros(self.num_bits),
            Some(bits) => bits,
        };
        bits.iter_mut().for_each(|x| *x = !*x);
        if !self.num_bits.is_multiple_of(BITS) {
            bits[self.num_bits / BITS] &= (1 << (self.num_bits % BITS)) - 1;
        }
        self.count_ones = self.num_bits - self.count_ones;
        self
    }
}

impl FromIterator<bool> for Bitmap {
    fn from_iter<T: IntoIterator<Item = bool>>(iter: T) -> Self {
        let vec = iter.into_iter().collect::<Vec<_>>();
        Self::from_bool_slice(&vec)
    }
}

impl FromIterator<Option<bool>> for Bitmap {
    fn from_iter<T: IntoIterator<Item = Option<bool>>>(iter: T) -> Self {
        iter.into_iter().map(|b| b.unwrap_or(false)).collect()
    }
}

impl Bitmap {
    pub fn to_protobuf(&self) -> PbBuffer {
        let body_len = self.num_bits.div_ceil(8) + 1;
        let mut body = Vec::with_capacity(body_len);
        body.push((self.num_bits % 8) as u8);
        match &self.bits {
            None if self.count_ones == 0 => body.resize(body_len, 0),
            None => {
                body.resize(body_len, u8::MAX);
                if !self.num_bits.is_multiple_of(8) {
                    body[body_len - 1] = (1 << (self.num_bits % 8)) - 1;
                }
            }
            Some(bits) => {
                body.extend_from_slice(unsafe {
                    std::slice::from_raw_parts(
                        bits.as_ptr() as *const u8,
                        self.num_bits.div_ceil(8),
                    )
                });
            }
        }
        PbBuffer {
            body,
            compression: CompressionType::None as i32,
        }
    }
}

impl From<&PbBuffer> for Bitmap {
    fn from(buf: &PbBuffer) -> Self {
        let last_byte_num_bits = buf.body[0];
        let num_bits = ((buf.body.len() - 1) * 8) - ((8 - last_byte_num_bits) % 8) as usize;
        Self::from_bytes_with_len(&buf.body[1..], num_bits)
    }
}

impl From<PbBuffer> for Bitmap {
    fn from(buf: PbBuffer) -> Self {
        Self::from(&buf)
    }
}

/// Bitmap iterator.
pub struct BitmapIter<'a> {
    bits: Option<&'a [usize]>,
    idx: usize,
    num_bits: usize,
    current_usize: usize,
    all_ones: bool,
}

impl BitmapIter<'_> {
    fn next_always_load_usize(&mut self) -> Option<bool> {
        if self.idx >= self.num_bits {
            return None;
        }
        let Some(bits) = &self.bits else {
            self.idx += 1;
            return Some(self.all_ones);
        };

        // Offset of the bit within the usize.
        let usize_offset = self.idx % BITS;

        // Get the index of usize which the bit is located in
        let usize_index = self.idx / BITS;
        self.current_usize = unsafe { *bits.get_unchecked(usize_index) };

        let bit_mask = 1 << usize_offset;
        let bit_flag = self.current_usize & bit_mask != 0;
        self.idx += 1;
        Some(bit_flag)
    }
}

impl iter::Iterator for BitmapIter<'_> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.num_bits {
            return None;
        }
        let Some(bits) = &self.bits else {
            self.idx += 1;
            return Some(self.all_ones);
        };

        // Offset of the bit within the usize.
        let usize_offset = self.idx % BITS;

        if usize_offset == 0 {
            // Get the index of usize which the bit is located in
            let usize_index = self.idx / BITS;
            self.current_usize = unsafe { *bits.get_unchecked(usize_index) };
        }

        let bit_mask = 1 << usize_offset;

        let bit_flag = self.current_usize & bit_mask != 0;
        self.idx += 1;
        Some(bit_flag)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.num_bits - self.idx;
        (remaining, Some(remaining))
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.idx += n;
        self.next_always_load_usize()
    }
}

impl ExactSizeIterator for BitmapIter<'_> {}
unsafe impl TrustedLen for BitmapIter<'_> {}

pub enum BitmapOnesIter<'a> {
    Range {
        start: usize,
        end: usize,
    },
    Buffer {
        bits: &'a [usize],
        cur_idx: usize,
        cur_bits: Option<usize>,
    },
}

impl iter::Iterator for BitmapOnesIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BitmapOnesIter::Range { start, end } => {
                let i = *start;
                if i >= *end {
                    None
                } else {
                    *start += 1;
                    Some(i)
                }
            }
            BitmapOnesIter::Buffer {
                bits,
                cur_idx,
                cur_bits,
            } => {
                while *cur_bits == Some(0) {
                    *cur_idx += 1;
                    *cur_bits = if *cur_idx >= bits.len() {
                        None
                    } else {
                        Some(bits[*cur_idx])
                    };
                }
                cur_bits.map(|bits| {
                    let low_bit = bits.isolate_lowest_one();
                    let low_bit_idx = bits.trailing_zeros();
                    *cur_bits = Some(bits ^ low_bit);
                    *cur_idx * BITS + low_bit_idx as usize
                })
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            BitmapOnesIter::Range { start, end } => {
                let remaining = end - start;
                (remaining, Some(remaining))
            }
            BitmapOnesIter::Buffer { bits, .. } => (0, Some(bits.len() * usize::BITS as usize)),
        }
    }
}

pub trait FilterByBitmap: ExactSizeIterator + Sized {
    fn filter_by_bitmap(self, bitmap: &Bitmap) -> impl Iterator<Item = Self::Item> {
        self.zip_eq_fast(bitmap.iter())
            .filter_map(|(item, bit)| bit.then_some(item))
    }
}

impl<T> FilterByBitmap for T where T: ExactSizeIterator {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_builder() {
        let bitmap1 = {
            let mut builder = BitmapBuilder::default();
            let bits = [
                false, true, true, false, true, false, true, false, true, false, true, true, false,
                true, false, true,
            ];
            for bit in bits {
                builder.append(bit);
            }
            for (idx, bit) in bits.iter().enumerate() {
                assert_eq!(builder.is_set(idx), *bit);
            }
            builder.finish()
        };
        let byte1 = 0b0101_0110_u8;
        let byte2 = 0b1010_1101_u8;
        let expected = Bitmap::from_bytes(&[byte1, byte2]);
        assert_eq!(bitmap1, expected);
        assert_eq!(
            bitmap1.count_ones(),
            (byte1.count_ones() + byte2.count_ones()) as usize
        );

        let bitmap2 = {
            let mut builder = BitmapBuilder::default();
            let bits = [false, true, true, false, true, false, true, false];
            for bit in bits {
                builder.append(bit);
            }
            for (idx, bit) in bits.iter().enumerate() {
                assert_eq!(builder.is_set(idx), *bit);
            }
            builder.finish()
        };
        let byte1 = 0b0101_0110_u8;
        let expected = Bitmap::from_bytes(&[byte1]);
        assert_eq!(bitmap2, expected);
    }

    #[test]
    fn test_builder_append_bitmap() {
        let mut builder = BitmapBuilder::default();
        builder.append_bitmap(&Bitmap::zeros(64));
        builder.append_bitmap(&Bitmap::ones(64));
        builder.append_bitmap(&Bitmap::from_bytes(&[0b1010_1010]));
        builder.append_bitmap(&Bitmap::zeros(8));
        builder.append_bitmap(&Bitmap::ones(8));
        let bitmap = builder.finish();
        assert_eq!(
            bitmap,
            Bitmap::from_vec_with_len(
                vec![0, usize::MAX, 0b1111_1111_0000_0000_1010_1010],
                64 + 64 + 8 + 8 + 8
            ),
        );
    }

    #[test]
    fn test_bitmap_get_size() {
        let bitmap1 = {
            let mut builder = BitmapBuilder::default();
            let bits = [
                false, true, true, false, true, false, true, false, true, false, true, true, false,
                true, false, true,
            ];
            for bit in bits {
                builder.append(bit);
            }
            for (idx, bit) in bits.iter().enumerate() {
                assert_eq!(builder.is_set(idx), *bit);
            }
            builder.finish()
        };
        assert_eq!(8, bitmap1.estimated_heap_size());
        assert_eq!(40, bitmap1.estimated_size());
    }

    #[test]
    fn test_bitmap_all_high() {
        let num_bits = 3;
        let bitmap = Bitmap::ones(num_bits);
        assert_eq!(bitmap.len(), num_bits);
        assert!(bitmap.all());
        for i in 0..num_bits {
            assert!(bitmap.is_set(i));
        }
        // Test to and from protobuf is OK.
        assert_eq!(bitmap, Bitmap::from(&bitmap.to_protobuf()));
    }

    #[test]
    fn test_bitwise_and() {
        #[rustfmt::skip]
        let cases = [(
            vec![],
            vec![],
            vec![],
        ), (
            vec![0, 1, 1, 0, 1, 0, 1, 0],
            vec![0, 1, 0, 0, 1, 1, 1, 0],
            vec![0, 1, 0, 0, 1, 0, 1, 0],
        ), (
            vec![0, 1, 0, 1, 0],
            vec![1, 1, 1, 1, 0],
            vec![0, 1, 0, 1, 0],
        )];

        for (input1, input2, expected) in cases {
            let bitmap1: Bitmap = input1.into_iter().map(|x| x != 0).collect();
            let bitmap2: Bitmap = input2.into_iter().map(|x| x != 0).collect();
            let res = bitmap1 & bitmap2;
            res.assert_valid();
            assert_eq!(res.iter().map(|x| x as i32).collect::<Vec<_>>(), expected,);
        }
    }

    #[test]
    fn test_bitwise_not() {
        #[rustfmt::skip]
        let cases = [(
            vec![1, 0, 1, 0, 1],
            vec![0, 1, 0, 1, 0],
        ), (
            vec![],
            vec![],
        ), (
            vec![1, 0, 1, 1, 0, 0, 1, 1],
            vec![0, 1, 0, 0, 1, 1, 0, 0],
        ), (
            vec![1, 0, 0, 1, 1, 1, 0, 0, 1, 0],
            vec![0, 1, 1, 0, 0, 0, 1, 1, 0, 1],
        )];

        for (input, expected) in cases {
            let bitmap: Bitmap = input.into_iter().map(|x| x != 0).collect();
            let res = !bitmap;
            res.assert_valid();
            assert_eq!(res.iter().map(|x| x as i32).collect::<Vec<_>>(), expected);
        }
    }

    #[test]
    fn test_bitwise_or() {
        let bitmap1 = Bitmap::from_bytes(&[0b01101010]);
        let bitmap2 = Bitmap::from_bytes(&[0b01001110]);
        assert_eq!(Bitmap::from_bytes(&[0b01101110]), (bitmap1 | bitmap2));
    }

    #[test]
    fn test_bitmap_is_set() {
        let bitmap = Bitmap::from_bytes(&[0b01001010]);
        assert!(!bitmap.is_set(0));
        assert!(bitmap.is_set(1));
        assert!(!bitmap.is_set(2));
        assert!(bitmap.is_set(3));
        assert!(!bitmap.is_set(4));
        assert!(!bitmap.is_set(5));
        assert!(bitmap.is_set(6));
        assert!(!bitmap.is_set(7));
    }

    #[test]
    fn test_bitmap_iter() {
        {
            let bitmap = Bitmap::from_bytes(&[0b01001010]);
            let mut booleans = vec![];
            for b in bitmap.iter() {
                booleans.push(b as u8);
            }
            assert_eq!(booleans, vec![0u8, 1, 0, 1, 0, 0, 1, 0]);
        }
        {
            let bitmap: Bitmap = vec![true; 5].into_iter().collect();
            for b in bitmap.iter() {
                assert!(b);
            }
        }
    }

    #[test]
    fn test_bitmap_high_ranges_iter() {
        fn test(bits: impl IntoIterator<Item = bool>, expected: Vec<RangeInclusive<usize>>) {
            let bitmap = Bitmap::from_iter(bits);
            let high_ranges = bitmap.high_ranges().collect::<Vec<_>>();
            assert_eq!(high_ranges, expected);
        }

        test(
            vec![
                true, true, true, false, false, true, true, true, false, true,
            ],
            vec![0..=2, 5..=7, 9..=9],
        );
        test(vec![true, true, true], std::iter::once(0..=2).collect());
        test(vec![false, false, false], vec![]);
    }

    #[test]
    fn test_bitmap_from_protobuf() {
        let bitmap_bytes = vec![3u8 /* len % BITS */, 0b0101_0010, 0b110];
        let buf = PbBuffer {
            body: bitmap_bytes,
            compression: CompressionType::None as _,
        };
        let bitmap: Bitmap = (&buf).into();
        let actual_bytes: Vec<u8> = bitmap.iter().map(|b| b as u8).collect();

        assert_eq!(actual_bytes, vec![0, 1, 0, 0, 1, 0, 1, 0, /*  */ 0, 1, 1]); // in reverse order
        assert_eq!(bitmap.count_ones(), 5);
    }

    #[test]
    fn test_bitmap_from_buffer() {
        let byte1 = 0b0110_1010_u8;
        let byte2 = 0b1011_0101_u8;
        let bitmap = Bitmap::from_bytes(&[0b0110_1010, 0b1011_0101]);
        let expected = Bitmap::from_iter(vec![
            false, true, false, true, false, true, true, false, true, false, true, false, true,
            true, false, true,
        ]);
        let count_ones = (byte1.count_ones() + byte2.count_ones()) as usize;
        assert_eq!(expected, bitmap);
        assert_eq!(bitmap.count_ones(), count_ones);
        assert_eq!(expected.count_ones(), count_ones);
    }

    #[test]
    fn test_bitmap_eq() {
        let b1: Bitmap = Bitmap::zeros(3);
        let b2: Bitmap = Bitmap::zeros(5);
        assert_ne!(b1, b2);

        let b1: Bitmap = [true, false].iter().cycle().cloned().take(10000).collect();
        let b2: Bitmap = [true, false].iter().cycle().cloned().take(10000).collect();
        assert_eq!(b1, b2);
    }

    #[test]
    fn test_bitmap_set() {
        let mut b = BitmapBuilder::zeroed(10);

        b.set(0, true);
        b.set(7, true);
        b.set(8, true);
        b.set(9, true);

        b.set(7, false);
        b.set(8, false);

        b.append(true);
        assert_eq!(b.len(), 11);

        let b = b.finish();
        assert_eq!(b.count_ones(), 3);
        assert_eq!(&b.bits.unwrap()[..], &[0b0110_0000_0001]);
    }

    #[test]
    fn test_bitmap_pop() {
        let mut b = BitmapBuilder::zeroed(7);

        {
            b.append(true);
            assert!(b.is_set(b.len() - 1));
            b.pop();
            assert!(!b.is_set(b.len() - 1));
        }

        {
            b.append(false);
            assert!(!b.is_set(b.len() - 1));
            b.pop();
            assert!(!b.is_set(b.len() - 1));
        }

        {
            b.append(true);
            b.append(false);
            assert!(!b.is_set(b.len() - 1));
            b.pop();
            assert!(b.is_set(b.len() - 1));
            b.pop();
            assert!(!b.is_set(b.len() - 1));
        }
    }

    #[test]
    fn test_bitmap_iter_ones() {
        let mut builder = BitmapBuilder::zeroed(1000);
        builder.append_n(1000, true);
        let bitmap = builder.finish();
        let mut iter = bitmap.iter_ones();
        for i in 0..1000 {
            let item = iter.next();
            assert!(item == Some(i + 1000));
        }
        for _ in 0..5 {
            let item = iter.next();
            assert!(item.is_none());
        }
    }

    #[test]
    fn from_indices_creates_correct_bitmap() {
        let bitmap = Bitmap::from_indices(10, [1, 3, 5, 7, 9]);
        assert_eq!(bitmap.len(), 10);
        for i in [1, 3, 5, 7, 9] {
            assert!(bitmap.is_set(i));
        }

        for i in [0, 2, 4, 6, 8] {
            assert!(!bitmap.is_set(i));
        }
    }

    #[test]
    fn from_indices_empty_indices_creates_zeroed_bitmap() {
        let bitmap = Bitmap::from_indices(10, [] as [usize; 0]);
        assert_eq!(bitmap.len(), 10);
        assert!(!bitmap.any());
    }

    #[test]
    fn from_indices_out_of_bounds_panics() {
        let result = std::panic::catch_unwind(|| {
            Bitmap::from_indices(5, [0, 6]);
        });
        assert!(result.is_err());
    }

    #[test]
    fn from_indices_all_indices_set_creates_filled_bitmap() {
        let bitmap = Bitmap::from_indices(5, [0, 1, 2, 3, 4]);
        assert_eq!(bitmap.len(), 5);
        assert!(bitmap.all());
    }
}
