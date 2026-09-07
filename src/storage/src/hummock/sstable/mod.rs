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

//! Hummock state store's SST builder, format and iterator

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
mod block;

use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::ops::{BitXor, Bound, Range, RangeInclusive};

pub use block::*;
mod block_iterator;
pub use block_iterator::*;
mod xor_filter;
use serde::{Deserialize, Serialize};
pub use xor_filter::{
    BlockedXor8FilterBuilder, BlockedXor16FilterBuilder, Xor8FilterBuilder, Xor16FilterBuilder,
    XorFilterReader,
};
pub mod builder;
pub use builder::*;
pub mod writer;
use risingwave_common::catalog::TableId;
pub use writer::*;
mod forward_sstable_iterator;
pub mod multi_builder;
use bytes::{Buf, BufMut};
pub use forward_sstable_iterator::*;
use tracing::warn;
mod backward_sstable_iterator;
pub use backward_sstable_iterator::*;
use risingwave_hummock_sdk::key::{FullKey, KeyPayloadType, UserKey, UserKeyRangeRef};
use risingwave_hummock_sdk::{HummockEpoch, HummockSstableObjectId};

mod filter;
pub(crate) mod meta;
mod utils;

pub use filter::{
    DEFAULT_FILTER_HASH_PREALLOC_KEY_COUNT_CAP, FilterBuilder, FilterBuilderOptions,
    NoneFilterBuilder,
};
pub use utils::{CompressionAlgorithm, xxhash64_checksum, xxhash64_verify};
use utils::{get_length_prefixed_slice, put_length_prefixed_slice};
use xxhash_rust::xxh64;

use super::{HummockError, HummockResult};
use crate::hummock::CachePolicy;
use crate::store::ReadOptions;

const MAGIC: u32 = 0x5785ab73;
const OLD_VERSION: u32 = 1;
const VERSION: u32 = 2;

/// Assume that watermark1 is 5, watermark2 is 7, watermark3 is 11, delete ranges
/// `{ [0, wmk1) in epoch1, [wmk1, wmk2) in epoch2, [wmk2, wmk3) in epoch3 }`
/// can be transformed into events below:
/// `{ <0, +epoch1> <wmk1, -epoch1> <wmk1, +epoch2> <wmk2, -epoch2> <wmk2, +epoch3> <wmk3,
/// -epoch3> }`
/// Then we can get monotonic events (they are in order by user key) as below:
/// `{ <0, epoch1>, <wmk1, epoch2>, <wmk2, epoch3>, <wmk3, +inf> }`
/// which means that delete range of [0, wmk1) is epoch1, delete range of [wmk1, wmk2) if epoch2,
/// etc. In this example, at the event key wmk1 (5), delete range changes from epoch1 to epoch2,
/// thus the `new epoch` is epoch2. epoch2 will be used from the event key wmk1 (5) and till the
/// next event key wmk2 (7) (not inclusive).
/// If there is no range deletes between current event key and next event key, `new_epoch` will be
/// `HummockEpoch::MAX`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct MonotonicDeleteEvent {
    pub event_key:
        risingwave_hummock_sdk::key::range_delete_backward_compatibility_serde_struct::PointRange,
    pub new_epoch: HummockEpoch,
}

impl MonotonicDeleteEvent {
    pub fn encode(&self, mut buf: impl BufMut) {
        self.event_key
            .left_user_key
            .encode_length_prefixed(&mut buf);
        buf.put_u8(if self.event_key.is_exclude_left_key {
            1
        } else {
            0
        });
        buf.put_u64_le(self.new_epoch);
    }

    pub fn decode(buf: &mut &[u8]) -> Self {
        use risingwave_hummock_sdk::key::range_delete_backward_compatibility_serde_struct::*;
        let user_key = UserKey::decode_length_prefixed(buf);
        let exclude_left_key_flag = buf.get_u8();
        let is_exclude_left_key = match exclude_left_key_flag {
            0 => false,
            1 => true,
            _ => panic!("exclusive flag should be either 0 or 1"),
        };
        let new_epoch = buf.get_u64_le();
        Self {
            event_key: PointRange {
                left_user_key: user_key,
                is_exclude_left_key,
            },
            new_epoch,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct SerdeSstable {
    id: HummockSstableObjectId,
    meta: SstableMeta,
}

impl From<SerdeSstable> for Sstable {
    fn from(SerdeSstable { id, meta }: SerdeSstable) -> Self {
        // Set skip_bloom_filter_in_serde to false because the behavior
        // is determined by the serializer
        Sstable::new(id, meta, false)
    }
}

/// [`Sstable`] is a handle for accessing SST.
#[derive(Clone, Deserialize)]
#[serde(from = "SerdeSstable")]
pub struct Sstable {
    pub id: HummockSstableObjectId,
    pub meta: SstableMeta,
    #[serde(skip)]
    pub filter_reader: XorFilterReader,
    /// SST serde happens when an SST meta is written to meta disk cache.
    /// Excluding the SST filter from serde can reduce the meta disk cache entry size
    /// and reduce disk IO throughput at the cost of making the SST filter useless.
    #[serde(skip)]
    skip_bloom_filter_in_serde: bool,
}

impl Serialize for Sstable {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut serde_sstable = SerdeSstable {
            id: self.id,
            meta: self.meta.clone(),
        };
        if !self.skip_bloom_filter_in_serde {
            serde_sstable.meta.bloom_filter = self.filter_reader.encode_to_bytes();
        }
        serde_sstable.serialize(serializer)
    }
}

impl Debug for Sstable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sstable")
            .field("id", &self.id)
            .field("meta", &self.meta)
            .finish()
    }
}

impl Sstable {
    pub fn new(
        id: HummockSstableObjectId,
        mut meta: SstableMeta,
        skip_bloom_filter_in_serde: bool,
    ) -> Self {
        let filter_data = std::mem::take(&mut meta.bloom_filter);
        let filter_reader = XorFilterReader::new(&filter_data, &meta.block_metas);
        Self {
            id,
            meta,
            filter_reader,
            skip_bloom_filter_in_serde,
        }
    }

    #[inline(always)]
    pub fn has_filter(&self) -> bool {
        !self.filter_reader.is_empty()
    }

    #[inline(always)]
    pub fn hash_for_filter(dist_key: &[u8], table_id: u32) -> u64 {
        let dist_key_hash = xxh64::xxh64(dist_key, 0);
        // congyi adds this because he aims to dedup keys in different tables
        (table_id as u64).bitxor(dist_key_hash)
    }

    #[inline(always)]
    pub fn may_match_hash(&self, user_key_range: &UserKeyRangeRef<'_>, hash: u64) -> bool {
        self.filter_reader
            .may_match(&self.meta.block_metas, user_key_range, hash)
    }

    #[inline(always)]
    pub fn block_count(&self) -> usize {
        self.meta.block_metas.len()
    }

    #[inline(always)]
    pub fn estimated_meta_cache_memory_weight(&self) -> usize {
        // This is for foyer's in-memory cache weighter. The disk tier uses foyer `Code`
        // serialization and `estimated_size` instead.
        std::mem::size_of::<Self>()
            + self.meta.estimated_heap_size()
            + self.filter_reader.estimated_heap_size()
    }
}

/// Per-SST metadata capability handle.
///
/// This is not a public wrapper around full [`SstableMeta`]. Callers should ask
/// for operations such as single-block metadata or block ranges. The V2
/// implementation still uses the existing full meta internally.
#[derive(Clone, Copy, Debug)]
pub enum SstableMetaHandle<'a> {
    V2(&'a Sstable),
}

impl<'a> SstableMetaHandle<'a> {
    pub fn v2(sst: &'a Sstable) -> Self {
        Self::V2(sst)
    }

    pub fn object_id(&self) -> HummockSstableObjectId {
        match self {
            Self::V2(sst) => sst.id,
        }
    }

    pub fn block_count(&self) -> usize {
        match self {
            Self::V2(sst) => sst.block_count(),
        }
    }

    pub fn estimated_size(&self) -> u32 {
        match self {
            Self::V2(sst) => sst.meta.estimated_size,
        }
    }

    pub fn smallest_key(&self) -> &'a Vec<u8> {
        match self {
            Self::V2(sst) => &sst.meta.smallest_key,
        }
    }

    pub fn block_meta(&self, global_block_idx: usize) -> &'a BlockMeta {
        match self {
            Self::V2(sst) => &sst.meta.block_metas[global_block_idx],
        }
    }

    pub fn largest_key(&self) -> &'a Vec<u8> {
        match self {
            Self::V2(sst) => &sst.meta.largest_key,
        }
    }

    /// Returns the exclusive upper key boundary for a block range ending at
    /// `block_end_idx`.
    ///
    /// When the range reaches the last block, the SST largest key is used as
    /// the boundary. Otherwise, the smallest key of the next block is used.
    pub fn block_upper_bound_key(&self, block_end_idx: usize) -> &'a Vec<u8> {
        if block_end_idx == self.block_count() {
            self.largest_key()
        } else {
            &self.block_meta(block_end_idx).smallest_key
        }
    }

    pub fn block_metas_partition_point(
        &self,
        range: RangeInclusive<usize>,
        pred: impl FnMut(&BlockMeta) -> bool,
    ) -> usize {
        let start = *range.start();
        let end = *range.end();
        match self {
            Self::V2(sst) => start + sst.meta.block_metas[start..=end].partition_point(pred),
        }
    }

    pub fn block_metas_vec(&self, range: Range<usize>) -> Vec<BlockMeta> {
        match self {
            Self::V2(sst) => sst.meta.block_metas[range].to_vec(),
        }
    }

    pub fn block_range(&self, global_block_idx: usize) -> (Range<usize>, usize) {
        let block_meta = self.block_meta(global_block_idx);
        let range =
            block_meta.offset as usize..block_meta.offset as usize + block_meta.len as usize;
        let uncompressed_capacity = block_meta.uncompressed_size as usize;
        (range, uncompressed_capacity)
    }
}

#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockMeta {
    pub smallest_key: Vec<u8>,
    pub offset: u32,
    pub len: u32,
    pub uncompressed_size: u32,
    pub total_key_count: u32,
    pub stale_key_count: u32,
}

impl BlockMeta {
    /// Format:
    ///
    /// ```plain
    /// | offset (4B) | len (4B) | uncompressed size (4B) | smallest key len (4B) | smallest key |
    /// ```
    pub fn encode(&self, mut buf: impl BufMut) {
        buf.put_u32_le(self.offset);
        buf.put_u32_le(self.len);
        buf.put_u32_le(self.uncompressed_size);
        buf.put_u32_le(self.total_key_count);
        buf.put_u32_le(self.stale_key_count);
        put_length_prefixed_slice(buf, &self.smallest_key);
    }

    pub fn decode(buf: &mut &[u8]) -> Self {
        let offset = buf.get_u32_le();
        let len = buf.get_u32_le();
        let uncompressed_size = buf.get_u32_le();

        let total_key_count = buf.get_u32_le();
        let stale_key_count = buf.get_u32_le();
        let smallest_key = get_length_prefixed_slice(buf);
        Self {
            smallest_key,
            offset,
            len,
            uncompressed_size,
            total_key_count,
            stale_key_count,
        }
    }

    pub fn decode_from_v1(buf: &mut &[u8]) -> Self {
        let offset = buf.get_u32_le();
        let len = buf.get_u32_le();
        let uncompressed_size = buf.get_u32_le();
        let total_key_count = 0;
        let stale_key_count = 0;
        let smallest_key = get_length_prefixed_slice(buf);
        Self {
            smallest_key,
            offset,
            len,
            uncompressed_size,
            total_key_count,
            stale_key_count,
        }
    }

    #[inline]
    pub fn encoded_size(&self) -> usize {
        24 /* offset + len + key len + uncompressed size + total key count + stale key count */ + self.smallest_key.len()
    }

    pub fn table_id(&self) -> TableId {
        FullKey::decode(&self.smallest_key).user_key.table_id
    }

    fn estimated_heap_size(&self) -> usize {
        self.smallest_key.capacity()
    }
}

#[derive(Default, Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct SstableMeta {
    pub block_metas: Vec<BlockMeta>,
    pub bloom_filter: Vec<u8>,
    pub estimated_size: u32,
    pub key_count: u32,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub meta_offset: u64,
    /// Assume that watermark1 is 5, watermark2 is 7, watermark3 is 11, delete ranges
    /// `{ [0, wmk1) in epoch1, [wmk1, wmk2) in epoch2, [wmk2, wmk3) in epoch3 }`
    /// can be transformed into events below:
    /// `{ <0, +epoch1> <wmk1, -epoch1> <wmk1, +epoch2> <wmk2, -epoch2> <wmk2, +epoch3> <wmk3,
    /// -epoch3> }`
    /// Then we can get monotonic events (they are in order by user key) as below:
    /// `{ <0, epoch1>, <wmk1, epoch2>, <wmk2, epoch3>, <wmk3, +inf> }`
    /// which means that delete range of [0, wmk1) is epoch1, delete range of [wmk1, wmk2) if
    /// epoch2, etc. In this example, at the event key wmk1 (5), delete range changes from
    /// epoch1 to epoch2, thus the `new epoch` is epoch2. epoch2 will be used from the event
    /// key wmk1 (5) and till the next event key wmk2 (7) (not inclusive).
    /// If there is no range deletes between current event key and next event key, `new_epoch` will
    /// be `HummockEpoch::MAX`.
    #[deprecated]
    pub monotonic_tombstone_events: Vec<MonotonicDeleteEvent>,
    /// Format version, for further compatibility.
    pub version: u32,
}

impl SstableMeta {
    /// Format:
    ///
    /// ```plain
    /// | N (4B) |
    /// | block meta 0 | ... | block meta N-1 |
    /// | SST filter len (4B) | SST filter |
    /// | estimated size (4B) | key count (4B) |
    /// | smallest key len (4B) | smallest key |
    /// | largest key len (4B) | largest key |
    /// | K (4B) |
    /// | tombstone-event 0 | ... | tombstone-event K-1 |
    /// | file offset of this meta block (8B) |
    /// | checksum (8B) | version (4B) | magic (4B) |
    /// ```
    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let encoded_size = self.encoded_size();
        let mut buf = Vec::with_capacity(encoded_size);
        self.encode_to(&mut buf);
        buf
    }

    pub fn encode_to(&self, mut buf: impl BufMut + AsRef<[u8]>) {
        let start = buf.as_ref().len();

        buf.put_u32_le(
            utils::checked_into_u32(self.block_metas.len()).unwrap_or_else(|_| {
                let tmp_full_key = FullKey::decode(&self.smallest_key);
                panic!(
                    "WARN overflow can't convert block_metas_len {} into u32 table {}",
                    self.block_metas.len(),
                    tmp_full_key.user_key.table_id,
                )
            }),
        );
        for block_meta in &self.block_metas {
            block_meta.encode(&mut buf);
        }
        put_length_prefixed_slice(&mut buf, &self.bloom_filter);
        buf.put_u32_le(self.estimated_size);
        buf.put_u32_le(self.key_count);
        put_length_prefixed_slice(&mut buf, &self.smallest_key);
        put_length_prefixed_slice(&mut buf, &self.largest_key);
        #[expect(deprecated)]
        buf.put_u32_le(
            utils::checked_into_u32(self.monotonic_tombstone_events.len()).unwrap_or_else(|_| {
                let tmp_full_key = FullKey::decode(&self.smallest_key);
                panic!(
                    "WARN overflow can't convert monotonic_tombstone_events_len {} into u32 table {}",
                    self.monotonic_tombstone_events.len(),
                    tmp_full_key.user_key.table_id,
                )
            }),
        );
        #[expect(deprecated)]
        for monotonic_tombstone_event in &self.monotonic_tombstone_events {
            monotonic_tombstone_event.encode(&mut buf);
        }
        buf.put_u64_le(self.meta_offset);

        let end = buf.as_ref().len();

        let checksum = xxhash64_checksum(&buf.as_ref()[start..end]);
        buf.put_u64_le(checksum);
        buf.put_u32_le(VERSION);
        buf.put_u32_le(MAGIC);
    }

    pub fn decode(buf: &[u8]) -> HummockResult<Self> {
        let mut cursor = buf.len();

        cursor -= 4;
        let magic = (&buf[cursor..]).get_u32_le();
        if magic != MAGIC {
            return Err(HummockError::magic_mismatch(MAGIC, magic));
        }

        cursor -= 4;
        let version = (&buf[cursor..cursor + 4]).get_u32_le();
        if version != VERSION && version != OLD_VERSION {
            return Err(HummockError::invalid_format_version(version));
        }

        cursor -= 8;
        let checksum = (&buf[cursor..cursor + 8]).get_u64_le();
        let buf = &mut &buf[..cursor];
        xxhash64_verify(buf, checksum)?;

        let block_meta_count = buf.get_u32_le() as usize;
        let mut block_metas = Vec::with_capacity(block_meta_count);
        if version == OLD_VERSION {
            for _ in 0..block_meta_count {
                block_metas.push(BlockMeta::decode_from_v1(buf));
            }
        } else {
            for _ in 0..block_meta_count {
                block_metas.push(BlockMeta::decode(buf));
            }
        }

        let bloom_filter = get_length_prefixed_slice(buf);
        let estimated_size = buf.get_u32_le();
        let key_count = buf.get_u32_le();
        let smallest_key = get_length_prefixed_slice(buf);
        let largest_key = get_length_prefixed_slice(buf);
        let tomb_event_count = buf.get_u32_le() as usize;
        let mut monotonic_tombstone_events = Vec::with_capacity(tomb_event_count);
        for _ in 0..tomb_event_count {
            let monotonic_tombstone_event = MonotonicDeleteEvent::decode(buf);
            monotonic_tombstone_events.push(monotonic_tombstone_event);
        }
        let meta_offset = buf.get_u64_le();

        if !monotonic_tombstone_events.is_empty() {
            warn!(
                count = monotonic_tombstone_events.len(),
                tables = ?monotonic_tombstone_events
                    .iter()
                    .map(|event| event.event_key.left_user_key.table_id)
                    .collect::<HashSet<_>>(),
                "read non-empty range tombstones");
        }

        #[expect(deprecated)]
        Ok(Self {
            block_metas,
            bloom_filter,
            estimated_size,
            key_count,
            smallest_key,
            largest_key,
            meta_offset,
            monotonic_tombstone_events,
            version,
        })
    }

    #[inline]
    pub fn encoded_size(&self) -> usize {
        4 // block meta count
            + self
            .block_metas
            .iter()
            .map(|block_meta| block_meta.encoded_size())
            .sum::<usize>()
            + 4 // monotonic tombstone events len
            + 4 // SST filter len
            + self.bloom_filter.len()
            + 4 // estimated size
            + 4 // key count
            + 4 // key len
            + self.smallest_key.len()
            + 4 // key len
            + self.largest_key.len()
            + 8 // footer
            + 8 // checksum
            + 4 // version
            + 4 // magic
    }

    #[expect(
        deprecated,
        reason = "monotonic_tombstone_events is deprecated but still contributes to decoded meta heap size"
    )]
    fn estimated_heap_size(&self) -> usize {
        self.block_metas.capacity() * std::mem::size_of::<BlockMeta>()
            + self
                .block_metas
                .iter()
                .map(BlockMeta::estimated_heap_size)
                .sum::<usize>()
            + self.bloom_filter.capacity()
            + self.smallest_key.capacity()
            + self.largest_key.capacity()
            + self.monotonic_tombstone_events.capacity()
                * std::mem::size_of::<MonotonicDeleteEvent>()
    }
}

#[derive(Default)]
pub struct SstableIteratorReadOptions {
    pub cache_policy: CachePolicy,
    /// The only hard upper bound of this scan. It limits the iterator's block window.
    pub scan_end_user_key: Option<Bound<UserKey<KeyPayloadType>>>,
    /// Whether to prefetch blocks within the already-bounded scan window.
    pub prefetch: bool,
    pub max_preload_retry_times: usize,
}

impl SstableIteratorReadOptions {
    /// Builds options with prefetch disabled. Range scans enable it explicitly after setting the
    /// scan bound, while point gets retain the default to avoid creating a prefetch stream.
    pub fn from_read_options(read_options: &ReadOptions) -> Self {
        Self {
            cache_policy: read_options.cache_policy,
            scan_end_user_key: None,
            prefetch: false,
            max_preload_retry_times: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hummock::HummockValue;
    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, iterator_test_key_of,
    };
    use crate::hummock::test_utils::gen_test_sstable_data;
    use crate::store::PrefetchOptions;

    #[test]
    fn test_iterator_read_options_do_not_enable_prefetch_for_point_get() {
        let read_options = ReadOptions {
            prefetch_options: PrefetchOptions::prefetch_for_large_range_scan(),
            ..Default::default()
        };

        let iterator_options = SstableIteratorReadOptions::from_read_options(&read_options);
        assert!(!iterator_options.prefetch);
        assert!(iterator_options.scan_end_user_key.is_none());
    }

    #[test]
    fn test_sstable_meta_enc_dec() {
        #[expect(deprecated)]
        let meta = SstableMeta {
            block_metas: vec![
                BlockMeta {
                    smallest_key: b"0-smallest-key".to_vec(),
                    len: 100,
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: b"5-some-key".to_vec(),
                    offset: 100,
                    len: 100,
                    ..Default::default()
                },
            ],
            bloom_filter: b"0123456789".to_vec(),
            estimated_size: 123,
            key_count: 123,
            smallest_key: b"0-smallest-key".to_vec(),
            largest_key: b"9-largest-key".to_vec(),
            meta_offset: 123,
            monotonic_tombstone_events: vec![],
            version: VERSION,
        };
        let sz = meta.encoded_size();
        let buf = meta.encode_to_bytes();
        assert_eq!(sz, buf.len());
        let decoded_meta = SstableMeta::decode(&buf[..]).unwrap();
        assert_eq!(decoded_meta, meta);

        println!("buf: {}", buf.len());
    }

    #[tokio::test]
    async fn test_sstable_serde() {
        let (_, meta) = gen_test_sstable_data(
            default_builder_opt_for_test(),
            (0..100).clone().map(|x| {
                (
                    iterator_test_key_of(x),
                    HummockValue::put(format!("overlapped_new_{}", x).as_bytes().to_vec()),
                )
            }),
        )
        .await;

        // skip sst serde
        let sstable = Sstable::new(42.into(), meta.clone(), true);

        let buffer = bincode::serialize(&sstable).unwrap();

        let s: Sstable = bincode::deserialize(&buffer).unwrap();

        assert_eq!(s.id, sstable.id);
        assert_eq!(s.meta, sstable.meta);
        assert!(!sstable.filter_reader.is_empty());
        // The table filter reader is empty because the SST filter is skipped in serde.
        assert!(s.filter_reader.is_empty());

        // enable sst serde
        let sstable = Sstable::new(42.into(), meta, false);

        let buffer = bincode::serialize(&sstable).unwrap();

        let s: Sstable = bincode::deserialize(&buffer).unwrap();

        assert_eq!(s.id, sstable.id);
        assert_eq!(s.meta, sstable.meta);
        assert_eq!(
            s.filter_reader.encode_to_bytes(),
            sstable.filter_reader.encode_to_bytes()
        );
    }
}
