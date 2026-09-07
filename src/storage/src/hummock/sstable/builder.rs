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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::mem;
use std::sync::Arc;
use std::time::SystemTime;

use await_tree::{InstrumentAwait, SpanExt};
use bytes::{Bytes, BytesMut};
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_hummock_sdk::key::{FullKey, MAX_KEY_LEN, TABLE_PREFIX_LEN, UserKey, user_key};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner, VnodeStatistics};
use risingwave_hummock_sdk::table_stats::{TableStats, TableStatsMap};
use risingwave_hummock_sdk::{HummockEpoch, HummockSstableObjectId, LocalSstableInfo};
use risingwave_pb::hummock::{PbSstableFilterLayout, PbSstableFilterType};

use super::utils::CompressionAlgorithm;
use super::{
    BlockBuilder, BlockBuilderOptions, BlockMeta, DEFAULT_BLOCK_SIZE, DEFAULT_ENTRY_SIZE,
    DEFAULT_RESTART_INTERVAL, SstableMeta, SstableWriter, VERSION,
};
use crate::compaction_catalog_manager::{
    CompactionCatalogAgent, CompactionCatalogAgentRef, FilterKeyExtractorImpl,
    FullKeyFilterKeyExtractor,
};
use crate::hummock::sstable::{
    DEFAULT_FILTER_HASH_PREALLOC_KEY_COUNT_CAP, FilterBuilder, FilterBuilderOptions, utils,
};
use crate::hummock::value::HummockValue;
use crate::hummock::{
    Block, BlockHolder, BlockIterator, HummockResult, MemoryLimiter, Xor16FilterBuilder,
    try_shorten_block_smallest_key,
};
use crate::monitor::CompactorMetrics;
use crate::opts::StorageOpts;

pub const DEFAULT_SSTABLE_SIZE: usize = 4 * 1024 * 1024;
pub const DEFAULT_BLOOM_FALSE_POSITIVE: f64 = 0.001;
pub const DEFAULT_MAX_SST_SIZE: u64 = 512 * 1024 * 1024;
pub const MIN_BLOCK_SIZE: usize = 8 * 1024;

#[derive(Clone, Debug)]
pub struct SstableBuilderOptions {
    /// Approximate sstable capacity.
    pub capacity: usize,
    /// Approximate block capacity.
    pub block_capacity: usize,
    /// Restart point interval.
    pub restart_interval: usize,
    /// Deprecated and ignored by SST filter builders; kept for backward compatibility.
    pub bloom_false_positive: f64,
    /// Compression algorithm.
    pub compression_algorithm: CompressionAlgorithm,
    pub max_sst_size: u64,
    /// If set, block metadata keys will be shortened when their length exceeds this threshold.
    pub shorten_block_meta_key_threshold: Option<usize>,
    /// Max bytes for vnode key-range hints in SST metadata. None disables collection.
    pub max_vnode_key_range_bytes: Option<usize>,
    /// Estimated key count for one output SST. Used only as a filter-builder capacity hint.
    pub estimated_output_key_count: Option<usize>,
    /// Upper bound for the initial key-hash buffer allocation in plain filter builders.
    pub filter_hash_prealloc_key_count_cap: usize,
}

impl From<&StorageOpts> for SstableBuilderOptions {
    fn from(options: &StorageOpts) -> SstableBuilderOptions {
        let capacity: usize = (options.sstable_size_mb as usize) * (1 << 20);
        SstableBuilderOptions {
            capacity,
            block_capacity: (options.block_size_kb as usize) * (1 << 10),
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: options.bloom_false_positive,
            compression_algorithm: CompressionAlgorithm::None,
            max_sst_size: options.compactor_max_sst_size,
            shorten_block_meta_key_threshold: options.shorten_block_meta_key_threshold,
            max_vnode_key_range_bytes: None,
            estimated_output_key_count: None,
            filter_hash_prealloc_key_count_cap: DEFAULT_FILTER_HASH_PREALLOC_KEY_COUNT_CAP,
        }
    }
}

impl Default for SstableBuilderOptions {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_SSTABLE_SIZE,
            block_capacity: DEFAULT_BLOCK_SIZE,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: DEFAULT_BLOOM_FALSE_POSITIVE,
            compression_algorithm: CompressionAlgorithm::None,
            max_sst_size: DEFAULT_MAX_SST_SIZE,
            shorten_block_meta_key_threshold: None,
            max_vnode_key_range_bytes: None,
            estimated_output_key_count: None,
            filter_hash_prealloc_key_count_cap: DEFAULT_FILTER_HASH_PREALLOC_KEY_COUNT_CAP,
        }
    }
}

impl SstableBuilderOptions {
    pub fn estimated_output_key_count(&self) -> usize {
        self.estimated_output_key_count
            .unwrap_or(self.capacity / DEFAULT_ENTRY_SIZE + 1)
    }

    pub fn filter_builder_options(&self) -> FilterBuilderOptions {
        FilterBuilderOptions {
            estimated_key_count: self.estimated_output_key_count(),
            estimated_block_count: self.capacity / self.block_capacity + 1,
            hash_prealloc_key_count_cap: self.filter_hash_prealloc_key_count_cap,
        }
    }
}

pub struct SstableBuilderOutput<WO> {
    pub sst_info: LocalSstableInfo,
    pub writer_output: WO,
    pub stats: SstableBuilderOutputStats,
}

pub struct SstableBuilder<W: SstableWriter, F: FilterBuilder> {
    /// Options.
    options: SstableBuilderOptions,
    /// Data writer.
    writer: W,
    /// Current block builder.
    block_builder: BlockBuilder,

    compaction_catalog_agent_ref: CompactionCatalogAgentRef,
    /// Block metadata vec.
    block_metas: Vec<BlockMeta>,

    /// `table_id` of added keys.
    table_ids: BTreeSet<TableId>,
    last_full_key: Vec<u8>,
    /// Buffer for encoded key and value to avoid allocation.
    raw_key: BytesMut,
    raw_value: BytesMut,
    last_table_id: Option<TableId>,
    sst_object_id: HummockSstableObjectId,

    /// Per table stats.
    table_stats: TableStatsMap,
    /// `last_table_stats` accumulates stats for `last_table_id` and finalizes it in `table_stats`
    /// by `finalize_last_table_stats`
    last_table_stats: TableStats,

    filter_builder: F,

    epoch_set: BTreeSet<u64>,
    memory_limiter: Option<Arc<MemoryLimiter>>,

    block_size_vec: Vec<usize>, // for statistics
    vnode_range_collector: Option<VnodeUserKeyRangeCollector>,
}

impl<W: SstableWriter> SstableBuilder<W, Xor16FilterBuilder> {
    pub fn for_test(
        sstable_id: u64,
        writer: W,
        options: SstableBuilderOptions,
        table_id_to_vnode: HashMap<impl Into<TableId>, usize>,
        table_id_to_watermark_serde: HashMap<
            impl Into<TableId>,
            Option<(OrderedRowSerde, OrderedRowSerde, usize)>,
        >,
    ) -> Self {
        let compaction_catalog_agent_ref = Arc::new(CompactionCatalogAgent::new(
            FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor),
            table_id_to_vnode
                .into_iter()
                .map(|(table_id, v)| (table_id.into(), v))
                .collect(),
            table_id_to_watermark_serde
                .into_iter()
                .map(|(table_id, v)| (table_id.into(), v))
                .collect(),
            HashMap::default(),
        ));

        Self::new(
            sstable_id,
            writer,
            Xor16FilterBuilder::create(options.filter_builder_options()),
            options,
            compaction_catalog_agent_ref,
            None,
        )
    }
}

impl<W: SstableWriter, F: FilterBuilder> SstableBuilder<W, F> {
    pub fn new(
        sst_object_id: impl Into<HummockSstableObjectId>,
        writer: W,
        filter_builder: F,
        options: SstableBuilderOptions,
        compaction_catalog_agent_ref: CompactionCatalogAgentRef,
        memory_limiter: Option<Arc<MemoryLimiter>>,
    ) -> Self {
        let sst_object_id = sst_object_id.into();
        Self {
            vnode_range_collector: VnodeUserKeyRangeCollector::with_limit(
                options.max_vnode_key_range_bytes,
            ),
            options: options.clone(),
            writer,
            block_builder: BlockBuilder::new(BlockBuilderOptions {
                capacity: options.block_capacity,
                restart_interval: options.restart_interval,
                compression_algorithm: options.compression_algorithm,
            }),
            filter_builder,
            block_metas: Vec::with_capacity(options.capacity / options.block_capacity + 1),
            table_ids: BTreeSet::new(),
            last_table_id: None,
            raw_key: BytesMut::new(),
            raw_value: BytesMut::new(),
            last_full_key: vec![],
            sst_object_id,
            compaction_catalog_agent_ref,
            table_stats: Default::default(),
            last_table_stats: Default::default(),
            epoch_set: BTreeSet::default(),
            memory_limiter,
            block_size_vec: Vec::new(),
        }
    }

    /// Add kv pair to sstable.
    pub async fn add_for_test(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
    ) -> HummockResult<()> {
        self.add(full_key, value).await
    }

    pub fn current_block_size(&self) -> usize {
        self.block_builder.approximate_len()
    }

    /// Add raw data of block to sstable. return false means fallback
    pub async fn add_raw_block(
        &mut self,
        buf: Bytes,
        filter_data: Vec<u8>,
        smallest_key: FullKey<Vec<u8>>,
        largest_key: Vec<u8>,
        mut meta: BlockMeta,
    ) -> HummockResult<bool> {
        let table_id = smallest_key.user_key.table_id;
        if self.last_table_id.is_none() || self.last_table_id.unwrap() != table_id {
            if !self.block_builder.is_empty() {
                // Try to finish the previous `Block`` when the `table_id` is switched, making sure that the data in the `Block` doesn't span two `table_ids`.
                self.build_block()
                    .instrument_await("sstable_build_block_on_raw_table_switch".verbose())
                    .await?;
            }

            self.table_ids.insert(table_id);
            self.finalize_last_table_stats();
            self.last_table_id = Some(table_id);
        }

        if !self.block_builder.is_empty() {
            let min_block_size = std::cmp::min(MIN_BLOCK_SIZE, self.options.block_capacity / 4);

            // If the previous block is too small, we should merge it into the previous block.
            if self.block_builder.approximate_len() < min_block_size {
                let block = Block::decode(buf, meta.uncompressed_size as usize)?;
                let mut iter = BlockIterator::new(BlockHolder::from_owned_block(Box::new(block)));
                iter.seek_to_first();
                while iter.is_valid() {
                    let value = HummockValue::from_slice(iter.value()).unwrap_or_else(|_| {
                        panic!(
                            "decode failed for fast compact sst_id {} block_idx {} last_table_id {:?}",
                            self.sst_object_id, self.block_metas.len(), self.last_table_id
                        )
                    });
                    self.add_impl(iter.key(), value, false)
                        .instrument_await("sstable_add_raw_block_fallback_add".verbose())
                        .await?;
                    iter.next();
                }
                return Ok(false);
            }

            self.build_block()
                .instrument_await("sstable_build_block_before_raw_block".verbose())
                .await?;
        }
        self.last_full_key = largest_key;
        assert_eq!(
            meta.len as usize,
            buf.len(),
            "meta {} buf {} last_table_id {:?}",
            meta.len,
            buf.len(),
            self.last_table_id
        );
        meta.offset = self.writer.data_len() as u32;
        self.block_metas.push(meta);
        self.filter_builder.add_raw_data(filter_data);
        let block_meta = self.block_metas.last_mut().unwrap();
        self.writer
            .write_block_bytes(buf, block_meta)
            .instrument_await("sstable_write_raw_block_bytes".verbose())
            .await?;

        Ok(true)
    }

    /// Add kv pair to sstable.
    pub async fn add(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
    ) -> HummockResult<()> {
        self.add_impl(full_key, value, true).await
    }

    /// Add kv pair to sstable.
    async fn add_impl(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        could_switch_block: bool,
    ) -> HummockResult<()> {
        const LARGE_KEY_LEN: usize = MAX_KEY_LEN >> 1;

        let table_key_len = full_key.user_key.table_key.as_ref().len();
        let table_value_len = match &value {
            HummockValue::Put(t) => t.len(),
            HummockValue::Delete => 0,
        };
        let large_value_len = self.options.max_sst_size as usize / 10;
        let large_key_value_len = self.options.max_sst_size as usize / 2;
        if table_key_len >= LARGE_KEY_LEN
            || table_value_len > large_value_len
            || table_key_len + table_value_len > large_key_value_len
        {
            let table_id = full_key.user_key.table_id;
            tracing::warn!(
                "A large key/value (table_id={}, key len={}, value len={}, epoch={}, spill offset={}) is added to block",
                table_id,
                table_key_len,
                table_value_len,
                full_key.epoch_with_gap.pure_epoch(),
                full_key.epoch_with_gap.offset(),
            );
        }

        // TODO: refine me
        full_key.encode_into(&mut self.raw_key);
        value.encode(&mut self.raw_value);
        let is_new_user_key = self.last_full_key.is_empty()
            || !user_key(&self.raw_key).eq(user_key(self.last_full_key.as_slice()));
        let table_id = full_key.user_key.table_id;
        let is_new_table = self.last_table_id != Some(table_id);
        let current_block_size = self.current_block_size();
        let is_block_full = current_block_size >= self.options.block_capacity
            || (current_block_size > self.options.block_capacity / 4 * 3
                && current_block_size + self.raw_value.len() + self.raw_key.len()
                    > self.options.block_capacity);

        if is_new_table {
            assert!(
                could_switch_block,
                "is_new_user_key {} sst_id {} block_idx {} table_id {} last_table_id {:?} full_key {:?}",
                is_new_user_key,
                self.sst_object_id,
                self.block_metas.len(),
                table_id,
                self.last_table_id,
                full_key
            );
            self.table_ids.insert(table_id);
            self.finalize_last_table_stats();
            self.last_table_id = Some(table_id);
            if !self.block_builder.is_empty() {
                self.build_block()
                    .instrument_await("sstable_build_block_on_table_switch".verbose())
                    .await?;
            }
        } else if is_block_full && could_switch_block {
            self.build_block()
                .instrument_await("sstable_build_block_on_block_full".verbose())
                .await?;
        }
        self.last_table_stats.total_key_count += 1;
        self.epoch_set.insert(full_key.epoch_with_gap.pure_epoch());

        // Rotate block builder if the previous one has been built.
        if self.block_builder.is_empty() {
            let smallest_key = if let Some(threshold) =
                self.options.shorten_block_meta_key_threshold
                && !self.last_full_key.is_empty()
                && full_key.encoded_len() >= threshold
            {
                let prev = FullKey::decode(&self.last_full_key);
                if let Some(shortened) = try_shorten_block_smallest_key(&prev, &full_key) {
                    shortened.encode()
                } else {
                    full_key.encode()
                }
            } else {
                full_key.encode()
            };

            self.block_metas.push(BlockMeta {
                offset: utils::checked_into_u32(self.writer.data_len()).unwrap_or_else(|_| {
                    panic!(
                        "WARN overflow can't convert writer_data_len {} into u32 sst_id {} block_idx {} tables {:?}",
                        self.writer.data_len(),
                        self.sst_object_id,
                        self.block_metas.len(),
                        self.table_ids,
                    )
                }),
                len: 0,
                smallest_key,
                uncompressed_size: 0,
                total_key_count: 0,
                stale_key_count: 0,
            });
        }

        let filter_key = self
            .compaction_catalog_agent_ref
            .extract(user_key(&self.raw_key));
        // Add SST filter check.
        if !filter_key.is_empty() {
            self.filter_builder
                .add_key(filter_key, table_id.as_raw_id());
        }
        // Use pre-encoded key to avoid redundant encoding
        self.block_builder.add(
            table_id,
            &self.raw_key[TABLE_PREFIX_LEN..],
            self.raw_value.as_ref(),
        );
        self.block_metas.last_mut().unwrap().total_key_count += 1;
        if !is_new_user_key || value.is_delete() {
            self.block_metas.last_mut().unwrap().stale_key_count += 1;
        }
        self.last_table_stats.total_key_size += full_key.encoded_len() as i64;
        self.last_table_stats.total_value_size += value.encoded_len() as i64;

        if let Some(collector) = self.vnode_range_collector.as_mut() {
            collector.observe_key(
                VirtualNode::from_index(full_key.user_key.get_vnode_id()),
                &self.raw_key,
                &self.last_full_key,
            );
        }

        self.last_full_key.clear();
        self.last_full_key.extend_from_slice(&self.raw_key);

        self.raw_key.clear();
        self.raw_value.clear();
        Ok(())
    }

    /// Finish building sst.
    ///
    /// # Format
    ///
    /// data:
    ///
    /// ```plain
    /// | Block 0 | ... | Block N-1 | N (4B) |
    /// ```
    pub async fn finish(mut self) -> HummockResult<SstableBuilderOutput<W::Output>> {
        let smallest_key = if self.block_metas.is_empty() {
            vec![]
        } else {
            self.block_metas[0].smallest_key.clone()
        };
        let largest_key = self.last_full_key.clone();
        self.finalize_last_table_stats();

        // Vnode key-range hints are only supported for single-table SSTs.
        // Multi-table SST scenarios should not enable max_vnode_key_range_bytes in config.
        assert!(
            self.table_ids.len() <= 1 || self.vnode_range_collector.is_none(),
            "vnode key-range hints are only supported for single-table SSTs, found {} tables",
            self.table_ids.len()
        );

        self.build_block()
            .instrument_await("sstable_finish_build_block".verbose())
            .await?;
        let right_exclusive = false;
        let meta_offset = self.writer.data_len() as u64;

        let filter_data = self.filter_builder.finish(self.memory_limiter.clone());
        let (filter_type, filter_layout) = if filter_data.is_none() {
            (
                PbSstableFilterType::SstableFilterNone,
                PbSstableFilterLayout::Unspecified,
            )
        } else if self.filter_builder.support_blocked_raw_data() {
            (
                self.filter_builder.filter_type(),
                PbSstableFilterLayout::Blocked,
            )
        } else {
            (
                self.filter_builder.filter_type(),
                PbSstableFilterLayout::Plain,
            )
        };

        let total_key_count = self
            .block_metas
            .iter()
            .map(|block_meta| block_meta.total_key_count as u64)
            .sum::<u64>();
        let stale_key_count = self
            .block_metas
            .iter()
            .map(|block_meta| block_meta.stale_key_count as u64)
            .sum::<u64>();
        let uncompressed_file_size = self
            .block_metas
            .iter()
            .map(|block_meta| block_meta.uncompressed_size as u64)
            .sum::<u64>();

        #[expect(deprecated)]
        let mut meta = SstableMeta {
            block_metas: self.block_metas,
            bloom_filter: filter_data.unwrap_or_default(),
            estimated_size: 0,
            key_count: utils::checked_into_u32(total_key_count).unwrap_or_else(|_| {
                panic!(
                    "WARN overflow can't convert total_key_count {} into u32 tables {:?}",
                    total_key_count, self.table_ids,
                )
            }),
            smallest_key,
            largest_key,
            version: VERSION,
            meta_offset,
            monotonic_tombstone_events: vec![],
        };

        let meta_encode_size = meta.encoded_size();
        let encoded_size_u32 = utils::checked_into_u32(meta_encode_size).unwrap_or_else(|_| {
            panic!(
                "WARN overflow can't convert meta_encoded_size {} into u32 tables {:?}",
                meta_encode_size, self.table_ids,
            )
        });
        let meta_offset_u32 = utils::checked_into_u32(meta_offset).unwrap_or_else(|_| {
            panic!(
                "WARN overflow can't convert meta_offset {} into u32 tables {:?}",
                meta_offset, self.table_ids,
            )
        });
        meta.estimated_size = encoded_size_u32
            .checked_add(meta_offset_u32)
            .unwrap_or_else(|| {
                panic!(
                    "WARN overflow encoded_size_u32 {} meta_offset_u32 {} table_id {:?} table_ids {:?}",
                    encoded_size_u32, meta_offset_u32, self.last_table_id, self.table_ids
                )
            });

        let (avg_key_size, avg_value_size) = if self.table_stats.is_empty() {
            (0, 0)
        } else {
            let total_key_count: usize = self
                .table_stats
                .values()
                .map(|s| s.total_key_count as usize)
                .sum();

            let total_key_size: usize = self
                .table_stats
                .values()
                .map(|s| s.total_key_size as usize)
                .sum();

            let total_value_size: usize = self
                .table_stats
                .values()
                .map(|s| s.total_value_size as usize)
                .sum();

            (
                total_key_size.checked_div(total_key_count).unwrap_or(0),
                total_value_size.checked_div(total_key_count).unwrap_or(0),
            )
        };

        let (min_epoch, max_epoch) = {
            if self.epoch_set.is_empty() {
                (HummockEpoch::MAX, u64::MIN)
            } else {
                (
                    *self.epoch_set.first().unwrap(),
                    *self.epoch_set.last().unwrap(),
                )
            }
        };

        let vnode_user_key_ranges = self
            .vnode_range_collector
            .take()
            .and_then(|collector| collector.finish(&self.last_full_key));

        let sst_info: SstableInfo = SstableInfoInner {
            object_id: self.sst_object_id,
            // use the same sst_id as object_id for initial sst
            sst_id: self.sst_object_id.as_raw_id().into(),
            key_range: KeyRange {
                left: Bytes::from(meta.smallest_key.clone()),
                right: Bytes::from(meta.largest_key.clone()),
                right_exclusive,
            },
            file_size: meta.estimated_size as u64,
            table_ids: self.table_ids.into_iter().collect(),
            meta_offset: meta.meta_offset,
            stale_key_count,
            total_key_count,
            uncompressed_file_size: uncompressed_file_size + meta.encoded_size() as u64,
            min_epoch,
            max_epoch,
            range_tombstone_count: 0,
            sst_size: meta.estimated_size as u64,
            filter_type,
            filter_layout,
            vnode_statistics: vnode_user_key_ranges,
        }
        .into();

        tracing::trace!(
            "meta_size {} filter_size {} add_key_counts {} stale_key_count {} min_epoch {} max_epoch {} epoch_count {}",
            meta.encoded_size(),
            meta.bloom_filter.len(),
            total_key_count,
            stale_key_count,
            min_epoch,
            max_epoch,
            self.epoch_set.len()
        );
        let filter_size = meta.bloom_filter.len();
        let sstable_file_size = sst_info.file_size as usize;

        if !meta.block_metas.is_empty() {
            // fill total_compressed_size
            let mut last_table_id = meta.block_metas[0].table_id();
            let mut last_table_stats = self.table_stats.get_mut(&last_table_id).unwrap();
            for block_meta in &meta.block_metas {
                let block_table_id = block_meta.table_id();
                if last_table_id != block_table_id {
                    last_table_id = block_table_id;
                    last_table_stats = self.table_stats.get_mut(&last_table_id).unwrap();
                }

                last_table_stats.total_compressed_size += block_meta.len as u64;
            }
        }

        let writer_output = self
            .writer
            .finish(meta)
            .instrument_await("sstable_writer_finish".verbose())
            .await?;
        // The timestamp is only used during full GC.
        //
        // Ideally object store object's last_modified should be used.
        // However, it'll incur additional IO overhead since S3 lacks an interface to retrieve the last_modified timestamp after the PUT operation on an object.
        //
        // The local timestamp below is expected to precede the last_modified of object store object, given that the object store object is created afterward.
        // It should help alleviate the clock drift issue.

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        Ok(SstableBuilderOutput::<W::Output> {
            sst_info: LocalSstableInfo::new(sst_info, self.table_stats, now),
            writer_output,
            stats: SstableBuilderOutputStats {
                filter_size,
                avg_key_size,
                avg_value_size,
                epoch_count: self.epoch_set.len(),
                block_size_vec: self.block_size_vec,
                sstable_file_size,
            },
        })
    }

    pub fn approximate_len(&self) -> usize {
        self.writer.data_len()
            + self.block_builder.approximate_len()
            + self.filter_builder.approximate_len()
    }

    pub async fn build_block(&mut self) -> HummockResult<()> {
        // Skip empty block.
        if self.block_builder.is_empty() {
            return Ok(());
        }

        let block_meta = self.block_metas.last_mut().unwrap();
        let uncompressed_block_size = self.block_builder.uncompressed_block_size();
        block_meta.uncompressed_size = utils::checked_into_u32(uncompressed_block_size)
            .unwrap_or_else(|_| {
                panic!(
                    "WARN overflow can't convert uncompressed_block_size {} into u32 table {:?}",
                    uncompressed_block_size,
                    self.block_builder.table_id(),
                )
            });
        let block = self.block_builder.build();
        self.writer
            .write_block(block, block_meta)
            .instrument_await("sstable_write_block".verbose())
            .await?;
        self.block_size_vec.push(block.len());
        let data_len = utils::checked_into_u32(self.writer.data_len()).unwrap_or_else(|_| {
            panic!(
                "WARN overflow can't convert writer_data_len {} into u32 table {:?}",
                self.writer.data_len(),
                self.block_builder.table_id(),
            )
        });
        block_meta.len = data_len.checked_sub(block_meta.offset).unwrap_or_else(|| {
            panic!(
                "data_len should >= meta_offset, found data_len={}, meta_offset={}",
                data_len, block_meta.offset
            )
        });

        self.filter_builder
            .switch_block(self.memory_limiter.clone());

        if data_len as usize > self.options.capacity * 2 {
            tracing::warn!(
                "WARN unexpected block size {} table {:?}",
                data_len,
                self.block_builder.table_id()
            );
        }

        self.block_builder.clear();
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.writer.data_len() > 0
    }

    /// Returns true if we roughly reached capacity
    pub fn reach_capacity(&self) -> bool {
        self.approximate_len() >= self.options.capacity
    }

    fn finalize_last_table_stats(&mut self) {
        if self.table_ids.is_empty() || self.last_table_id.is_none() {
            return;
        }
        self.table_stats.insert(
            self.last_table_id.unwrap(),
            std::mem::take(&mut self.last_table_stats),
        );
    }
}

/// Collects vnode key-range hints during SST building.
struct VnodeUserKeyRangeCollector {
    max_bytes: usize,
    current_size: usize,
    ranges: BTreeMap<VirtualNode, (UserKey<Bytes>, UserKey<Bytes>)>,
    current_vnode: VirtualNode,
    range_start_key: Vec<u8>,
}

impl VnodeUserKeyRangeCollector {
    fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            current_size: 0,
            ranges: BTreeMap::new(),
            current_vnode: VirtualNode::ZERO,
            range_start_key: Vec::new(),
        }
    }

    fn with_limit(max_bytes: Option<usize>) -> Option<Self> {
        max_bytes.filter(|&n| n > 0).map(Self::new)
    }

    /// Track vnode boundaries. On vnode switch, seals previous range with `prev_key` as right bound.
    /// Range: `[first_key_of_vnode, last_key_of_vnode]` (inclusive).
    fn observe_key(&mut self, vnode: VirtualNode, key: &[u8], prev_key: &[u8]) {
        if self.current_size >= self.max_bytes {
            return;
        }

        // First key
        if self.range_start_key.is_empty() {
            self.current_vnode = vnode;
            self.range_start_key = key.to_vec();
            return;
        }

        // Same vnode, nothing to do
        if vnode == self.current_vnode {
            return;
        }

        // Vnode changed: seal previous range
        self.seal_range(prev_key);

        // Check if budget exhausted after sealing
        if self.current_size >= self.max_bytes {
            return;
        }

        // Start new vnode
        self.current_vnode = vnode;
        self.range_start_key = key.to_vec();
    }

    /// Seal current range. Asserts `vnode/table_id` consistency between left and right keys.
    fn seal_range(&mut self, right_key: &[u8]) {
        let left_key = mem::take(&mut self.range_start_key);
        self.current_size += mem::size_of::<VirtualNode>() + left_key.len() + right_key.len();

        let left_full_key = FullKey::decode(&left_key);
        let right_full_key = FullKey::decode(right_key);
        let left_user_key = left_full_key.user_key.copy_into();
        let right_user_key = right_full_key.user_key.copy_into();

        // Sanity checks for data correctness:
        // 1. left and right keys have same `vnode`
        // 2. vnode matches `current_vnode` being tracked
        // 3. left and right keys have same `table_id`
        assert_eq!(
            left_user_key.get_vnode_id(),
            right_user_key.get_vnode_id(),
            "vnode changed within range: left_user {:?}, right_user {:?}",
            left_user_key,
            right_user_key
        );
        assert_eq!(
            left_user_key.get_vnode_id(),
            self.current_vnode.to_index(),
            "vnode mismatch: left {:?}, right {:?}, expected vnode {}",
            left_user_key,
            right_user_key,
            self.current_vnode.to_index()
        );
        assert_eq!(
            left_user_key.table_id, right_user_key.table_id,
            "table_id changed within range: left {:?}, right {:?}",
            left_user_key, right_user_key
        );

        self.ranges
            .insert(self.current_vnode, (left_user_key, right_user_key));
    }

    /// Returns `Some` if >1 vnodes collected, `None` otherwise (single-vnode needs no hints).
    fn finish(mut self, last_key: &[u8]) -> Option<VnodeStatistics> {
        if !self.range_start_key.is_empty() {
            self.seal_range(last_key);
        }

        if self.ranges.len() > 1 {
            // Validate all ranges belong to the same table_id before building VnodeStatistics
            let mut table_ids = self
                .ranges
                .values()
                .flat_map(|(left, right)| [left.table_id, right.table_id]);
            if let Some(first_table_id) = table_ids.next() {
                for table_id in table_ids {
                    assert_eq!(
                        table_id, first_table_id,
                        "all vnode ranges must belong to the same table_id, found {:?} and {:?}",
                        table_id, first_table_id
                    );
                }
            }

            Some(VnodeStatistics::from_map(self.ranges))
        } else {
            None
        }
    }
}

pub struct SstableBuilderOutputStats {
    filter_size: usize,
    avg_key_size: usize,
    avg_value_size: usize,
    epoch_count: usize,
    block_size_vec: Vec<usize>, // for statistics
    sstable_file_size: usize,
}

impl SstableBuilderOutputStats {
    pub fn report_stats(&self, metrics: &Arc<CompactorMetrics>) {
        if self.filter_size != 0 {
            metrics
                .sstable_bloom_filter_size
                .observe(self.filter_size as _);
        }

        if self.sstable_file_size != 0 {
            metrics
                .sstable_file_size
                .observe(self.sstable_file_size as _);
        }

        if self.avg_key_size != 0 {
            metrics.sstable_avg_key_size.observe(self.avg_key_size as _);
        }

        if self.avg_value_size != 0 {
            metrics
                .sstable_avg_value_size
                .observe(self.avg_value_size as _);
        }

        if self.epoch_count != 0 {
            metrics
                .sstable_distinct_epoch_count
                .observe(self.epoch_count as _);
        }

        if !self.block_size_vec.is_empty() {
            for block_size in &self.block_size_vec {
                metrics.sstable_block_size.observe(*block_size as _);
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::{Bound, HashMap};

    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::key::UserKey;

    use super::*;
    use crate::assert_bytes_eq;
    use crate::compaction_catalog_manager::{
        CompactionCatalogAgent, DummyFilterKeyExtractor, MultiFilterKeyExtractor,
    };
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::sstable::xor_filter::BlockedXor16FilterBuilder;
    use crate::hummock::test_utils::{
        TEST_KEYS_COUNT, default_builder_opt_for_test, gen_test_sstable_impl, mock_sst_writer,
        test_key_of, test_value_of,
    };
    use crate::hummock::{CachePolicy, Sstable, SstableWriterOptions, Xor8FilterBuilder};
    use crate::monitor::StoreLocalStatistic;

    #[tokio::test]
    async fn test_empty() {
        let opt = SstableBuilderOptions {
            capacity: 0,
            block_capacity: 4096,
            restart_interval: 16,
            bloom_false_positive: 0.001,
            ..Default::default()
        };

        let table_id_to_vnode = HashMap::from_iter(vec![(0, VirtualNode::COUNT_FOR_TEST)]);
        let table_id_to_watermark_serde = HashMap::from_iter(vec![(0, None)]);
        let b = SstableBuilder::for_test(
            0,
            mock_sst_writer(&opt),
            opt,
            table_id_to_vnode,
            table_id_to_watermark_serde,
        );

        b.finish().await.unwrap();
    }

    fn encode_full_key(vnode: VirtualNode, table_key_suffix: &[u8]) -> Vec<u8> {
        let mut table_key = vnode.to_be_bytes().to_vec();
        table_key.extend_from_slice(table_key_suffix);
        FullKey::for_test(TableId::default(), table_key, 0).encode()
    }

    fn table_key_of(vnode: VirtualNode, suffix: &[u8]) -> Vec<u8> {
        let mut key = vnode.to_be_bytes().to_vec();
        key.extend_from_slice(suffix);
        key
    }

    #[test]
    fn test_vnode_user_key_range_basic_collection() {
        // Test basic multi-vnode collection with boundary semantics verification.
        // Validates: vnode switching triggers range sealing, boundaries are inclusive (right_exclusive=false).
        let mut collector = VnodeUserKeyRangeCollector::with_limit(Some(1024)).unwrap();
        let vnode_1 = VirtualNode::from_index(1);
        let vnode_2 = VirtualNode::from_index(2);

        let k1 = encode_full_key(vnode_1, b"k1");
        let k2 = encode_full_key(vnode_1, b"k2");
        let k3 = encode_full_key(vnode_2, b"k3");
        let k4 = encode_full_key(vnode_2, b"k4");

        collector.observe_key(vnode_1, &k1, &[]);
        collector.observe_key(vnode_1, &k2, &k1);
        collector.observe_key(vnode_2, &k3, &k2);
        collector.observe_key(vnode_2, &k4, &k3);

        let info = collector.finish(&k4).unwrap();
        assert_eq!(info.vnode_user_key_ranges().len(), 2);

        // Verify vnode_1: left = first key, right = last key before switch
        let (range1_left, range1_right) = info.get_vnode_user_key_range(vnode_1).unwrap();
        assert_eq!(range1_left.table_key.as_ref(), table_key_of(vnode_1, b"k1"));
        assert_eq!(
            range1_right.table_key.as_ref(),
            table_key_of(vnode_1, b"k2")
        );

        // Verify vnode_2: left = first key, right = SST's last key
        let (range2_left, range2_right) = info.get_vnode_user_key_range(vnode_2).unwrap();
        assert_eq!(range2_left.table_key.as_ref(), table_key_of(vnode_2, b"k3"));
        assert_eq!(
            range2_right.table_key.as_ref(),
            table_key_of(vnode_2, b"k4")
        );
    }

    #[test]
    fn test_vnode_user_key_range_capacity_limit() {
        // Test "allow over-limit write" semantics: inserting a range may exceed capacity,
        // but stops before starting the next range if it would exceed the limit.
        //
        // Calculation based on encoded key sizes:
        // Each range = sizeof(VirtualNode) + left.len() + right.len().
        // Use a limit that:
        //   - allows writing vnode_1 (range_size),
        //   - allows over-limit write of vnode_2 (range_size + estimated_next),
        //   - stops before vnode_3 (2 * range_size + estimated_next > limit).
        let vnode_1 = VirtualNode::from_index(1);
        let vnode_2 = VirtualNode::from_index(2);
        let vnode_3 = VirtualNode::from_index(3);
        let vnode_4 = VirtualNode::from_index(4);

        let k1 = encode_full_key(vnode_1, b"k1");
        let k2 = encode_full_key(vnode_1, b"k2");
        let k3 = encode_full_key(vnode_2, b"k3");
        let k4 = encode_full_key(vnode_2, b"k4");
        let k5 = encode_full_key(vnode_3, b"k5");
        let k6 = encode_full_key(vnode_3, b"k6");
        let k7 = encode_full_key(vnode_4, b"k7");

        let range_size = mem::size_of::<VirtualNode>() + k1.len() + k2.len();
        let estimated_next_size = mem::size_of::<VirtualNode>() + k3.len();
        let limit = range_size + estimated_next_size; // allow second range, stop before third
        let mut collector = VnodeUserKeyRangeCollector::with_limit(Some(limit)).unwrap();

        collector.observe_key(vnode_1, &k1, &[]);
        collector.observe_key(vnode_1, &k2, &k1);
        collector.observe_key(vnode_2, &k3, &k2); // Seals vnode_1 (6 bytes)
        collector.observe_key(vnode_2, &k4, &k3);
        collector.observe_key(vnode_3, &k5, &k4); // Seals vnode_2 (12 bytes total), stops before vnode_3
        collector.observe_key(vnode_3, &k6, &k5); // Ignored
        collector.observe_key(vnode_4, &k7, &k6); // Ignored

        let info = collector.finish(&k7).unwrap();
        assert_eq!(
            info.vnode_user_key_ranges().len(),
            2,
            "Should collect exactly 2 vnodes"
        );

        // Verify collected vnodes have correct boundaries
        let (range1_left, range1_right) = info.get_vnode_user_key_range(vnode_1).unwrap();
        assert_eq!(range1_left.table_key.as_ref(), table_key_of(vnode_1, b"k1"));
        assert_eq!(
            range1_right.table_key.as_ref(),
            table_key_of(vnode_1, b"k2")
        );

        let (range2_left, range2_right) = info.get_vnode_user_key_range(vnode_2).unwrap();
        assert_eq!(range2_left.table_key.as_ref(), table_key_of(vnode_2, b"k3"));
        assert_eq!(
            range2_right.table_key.as_ref(),
            table_key_of(vnode_2, b"k4")
        );

        // Verify stopped vnodes are not collected
        assert!(info.get_vnode_user_key_range(vnode_3).is_none());
        assert!(info.get_vnode_user_key_range(vnode_4).is_none());
    }

    #[test]
    fn test_vnode_user_key_range_sparse_distribution() {
        // Test non-consecutive vnodes (production scenario: hash-based data distribution).
        // Validates: collector handles sparse vnode indices correctly, gaps are not filled.
        let mut collector = VnodeUserKeyRangeCollector::with_limit(Some(1024)).unwrap();
        let vnode_5 = VirtualNode::from_index(5);
        let vnode_10 = VirtualNode::from_index(10);
        let vnode_100 = VirtualNode::from_index(100);

        let keys = vec![
            (vnode_5, encode_full_key(vnode_5, b"key_005_001")),
            (vnode_5, encode_full_key(vnode_5, b"key_005_002")),
            (vnode_5, encode_full_key(vnode_5, b"key_005_999")),
            (vnode_10, encode_full_key(vnode_10, b"key_010_001")),
            (vnode_10, encode_full_key(vnode_10, b"key_010_100")),
            (vnode_100, encode_full_key(vnode_100, b"key_100_001")),
            (vnode_100, encode_full_key(vnode_100, b"key_100_999")),
        ];

        let mut prev_key = Vec::new();
        for (vnode, key) in &keys {
            collector.observe_key(*vnode, key, &prev_key);
            prev_key = key.clone();
        }

        let info = collector.finish(&prev_key).unwrap();
        assert_eq!(info.vnode_user_key_ranges().len(), 3);

        // Verify each collected vnode has correct boundaries
        let (range5_left, range5_right) = info.get_vnode_user_key_range(vnode_5).unwrap();
        assert_eq!(
            range5_left.table_key.as_ref(),
            table_key_of(vnode_5, b"key_005_001")
        );
        assert_eq!(
            range5_right.table_key.as_ref(),
            table_key_of(vnode_5, b"key_005_999")
        );

        let (range10_left, range10_right) = info.get_vnode_user_key_range(vnode_10).unwrap();
        assert_eq!(
            range10_left.table_key.as_ref(),
            table_key_of(vnode_10, b"key_010_001")
        );
        assert_eq!(
            range10_right.table_key.as_ref(),
            table_key_of(vnode_10, b"key_010_100")
        );

        let (range100_left, range100_right) = info.get_vnode_user_key_range(vnode_100).unwrap();
        assert_eq!(
            range100_left.table_key.as_ref(),
            table_key_of(vnode_100, b"key_100_001")
        );
        assert_eq!(
            range100_right.table_key.as_ref(),
            table_key_of(vnode_100, b"key_100_999")
        );

        // Verify gaps are not filled (no data for vnodes 0, 7, 50)
        assert!(
            info.get_vnode_user_key_range(VirtualNode::from_index(0))
                .is_none()
        );
        assert!(
            info.get_vnode_user_key_range(VirtualNode::from_index(7))
                .is_none()
        );
        assert!(
            info.get_vnode_user_key_range(VirtualNode::from_index(50))
                .is_none()
        );
    }

    #[test]
    fn test_vnode_user_key_range_edge_cases() {
        // Test 1: Configuration disabled (None or 0) should return None
        assert!(VnodeUserKeyRangeCollector::with_limit(None).is_none());
        assert!(VnodeUserKeyRangeCollector::with_limit(Some(0)).is_none());

        // Test 2: Empty collector (no keys) should return None
        let collector = VnodeUserKeyRangeCollector::with_limit(Some(1024)).unwrap();
        assert!(
            collector
                .finish(&encode_full_key(VirtualNode::ZERO, b"any"))
                .is_none()
        );

        // Test 3: Single vnode (optimization: don't emit hints for single-vnode SSTs)
        let vnode = VirtualNode::from_index(5);
        let mut collector = VnodeUserKeyRangeCollector::with_limit(Some(1024)).unwrap();
        let a = encode_full_key(vnode, b"a");
        let b = encode_full_key(vnode, b"b");
        let c = encode_full_key(vnode, b"c");
        collector.observe_key(vnode, &a, &[]);
        collector.observe_key(vnode, &b, &a);
        collector.observe_key(vnode, &c, &b);
        assert!(
            collector.finish(&c).is_none(),
            "Single-vnode SST should not emit hints"
        );

        // Test 4: Extreme limit (1 byte) - first vnode exceeds, becomes single-vnode
        // Range size = sizeof(VirtualNode=2) + encoded_key_len * 2
        // With 1-byte limit: vnode_1 range exceeds but allowed (over-limit write)
        // vnode_2 check: current_size >= max_bytes, stop. Result: only vnode_1, returns None (single-vnode)
        let mut collector = VnodeUserKeyRangeCollector::with_limit(Some(1)).unwrap();
        let vnode_1 = VirtualNode::from_index(1);
        let vnode_2 = VirtualNode::from_index(2);
        let key1 = encode_full_key(vnode_1, b"key1");
        let key2 = encode_full_key(vnode_1, b"key2");
        let key3 = encode_full_key(vnode_2, b"key3");
        collector.observe_key(vnode_1, &key1, &[]);
        collector.observe_key(vnode_1, &key2, &key1);
        collector.observe_key(vnode_2, &key3, &key2); // Seals vnode_1, stops before vnode_2
        assert!(
            collector.finish(&key3).is_none(),
            "Only 1 vnode collected, should return None"
        );
    }

    #[tokio::test]
    async fn test_basic() {
        let opt = default_builder_opt_for_test();

        let table_id_to_vnode = HashMap::from_iter(vec![(0, VirtualNode::COUNT_FOR_TEST)]);
        let table_id_to_watermark_serde = HashMap::from_iter(vec![(0, None)]);
        let mut b = SstableBuilder::for_test(
            0,
            mock_sst_writer(&opt),
            opt,
            table_id_to_vnode,
            table_id_to_watermark_serde,
        );

        for i in 0..TEST_KEYS_COUNT {
            b.add_for_test(
                test_key_of(i).to_ref(),
                HummockValue::put(&test_value_of(i)),
            )
            .await
            .unwrap();
        }

        let output = b.finish().await.unwrap();
        let info = output.sst_info.sst_info;

        assert_bytes_eq!(test_key_of(0).encode(), info.key_range.left);
        assert_bytes_eq!(
            test_key_of(TEST_KEYS_COUNT - 1).encode(),
            info.key_range.right
        );
        let (data, meta) = output.writer_output;
        assert_eq!(info.file_size, meta.estimated_size as u64);
        let offset = info.meta_offset as usize;
        let meta2 = SstableMeta::decode(&data[offset..]).unwrap();
        assert_eq!(meta2, meta);
    }

    async fn test_with_xor_filter_builder<F: FilterBuilder>(
        bloom_false_positive: f64,
        expected_filter_type: PbSstableFilterType,
        expected_filter_layout: PbSstableFilterLayout,
    ) {
        let key_count = 1000;

        let opts = SstableBuilderOptions {
            capacity: 0,
            block_capacity: 4096,
            restart_interval: 16,
            bloom_false_positive,
            ..Default::default()
        };

        // build remote table
        let sstable_store = mock_sstable_store().await;
        let table_id_to_vnode = HashMap::from_iter(vec![(0, VirtualNode::COUNT_FOR_TEST)]);
        let table_id_to_watermark_serde = HashMap::from_iter(vec![(0, None)]);
        let sst_info = gen_test_sstable_impl::<Vec<u8>, F>(
            opts,
            0,
            (0..TEST_KEYS_COUNT).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i)))),
            sstable_store.clone(),
            CachePolicy::NotFill,
            table_id_to_vnode,
            table_id_to_watermark_serde,
        )
        .await;
        assert_eq!(sst_info.filter_type, expected_filter_type);
        assert_eq!(sst_info.filter_layout, expected_filter_layout);
        let table = sstable_store
            .sstable(&sst_info, &mut StoreLocalStatistic::default())
            .await
            .unwrap();

        assert!(table.has_filter());
        for i in 0..key_count {
            let full_key = test_key_of(i);
            let hash = Sstable::hash_for_filter(full_key.user_key.encode().as_slice(), 0);
            let key_ref = full_key.user_key.as_ref();
            assert!(
                table.may_match_hash(&(Bound::Included(key_ref), Bound::Included(key_ref)), hash),
                "failed at {}",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_xor_filter_builder_output() {
        test_with_xor_filter_builder::<Xor16FilterBuilder>(
            0.0,
            PbSstableFilterType::SstableFilterXor16,
            PbSstableFilterLayout::Plain,
        )
        .await;
        test_with_xor_filter_builder::<Xor16FilterBuilder>(
            0.01,
            PbSstableFilterType::SstableFilterXor16,
            PbSstableFilterLayout::Plain,
        )
        .await;
        test_with_xor_filter_builder::<Xor8FilterBuilder>(
            0.01,
            PbSstableFilterType::SstableFilterXor8,
            PbSstableFilterLayout::Plain,
        )
        .await;
        test_with_xor_filter_builder::<BlockedXor16FilterBuilder>(
            0.01,
            PbSstableFilterType::SstableFilterXor16,
            PbSstableFilterLayout::Blocked,
        )
        .await;
    }

    #[tokio::test]
    async fn test_no_xor_filter_block() {
        let opts = SstableBuilderOptions::default();
        // build remote table
        let sstable_store = mock_sstable_store().await;
        let writer_opts = SstableWriterOptions::default();
        let object_id = 1;
        let writer = sstable_store
            .clone()
            .create_sst_writer(object_id, writer_opts);
        let mut filter = MultiFilterKeyExtractor::default();
        filter.register(
            1.into(),
            FilterKeyExtractorImpl::Dummy(DummyFilterKeyExtractor),
        );
        filter.register(
            2.into(),
            FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor),
        );
        filter.register(
            3.into(),
            FilterKeyExtractorImpl::Dummy(DummyFilterKeyExtractor),
        );

        let table_id_to_vnode = HashMap::from_iter(vec![
            (1.into(), VirtualNode::COUNT_FOR_TEST),
            (2.into(), VirtualNode::COUNT_FOR_TEST),
            (3.into(), VirtualNode::COUNT_FOR_TEST),
        ]);
        let table_id_to_watermark_serde =
            HashMap::from_iter(vec![(1.into(), None), (2.into(), None), (3.into(), None)]);

        let compaction_catalog_agent_ref = Arc::new(CompactionCatalogAgent::new(
            FilterKeyExtractorImpl::Multi(filter),
            table_id_to_vnode,
            table_id_to_watermark_serde,
            HashMap::default(),
        ));

        let mut builder = SstableBuilder::new(
            object_id,
            writer,
            BlockedXor16FilterBuilder::create(opts.filter_builder_options()),
            opts,
            compaction_catalog_agent_ref,
            None,
        );

        let key_count: usize = 10000;
        for table_id in 1..4 {
            let mut table_key = VirtualNode::ZERO.to_be_bytes().to_vec();
            for idx in 0..key_count {
                table_key.resize(VirtualNode::SIZE, 0);
                table_key.extend_from_slice(format!("key_test_{:05}", idx * 2).as_bytes());
                let k = UserKey::for_test(TableId::new(table_id), table_key.as_ref());
                let v = test_value_of(idx);
                builder
                    .add(
                        FullKey::from_user_key(k, test_epoch(1)),
                        HummockValue::put(v.as_ref()),
                    )
                    .await
                    .unwrap();
            }
        }
        let ret = builder.finish().await.unwrap();
        let sst_info = ret.sst_info.sst_info.clone();
        ret.writer_output.await.unwrap().unwrap();
        let table = sstable_store
            .sstable(&sst_info, &mut StoreLocalStatistic::default())
            .await
            .unwrap();
        let mut table_key = VirtualNode::ZERO.to_be_bytes().to_vec();
        for idx in 0..key_count {
            table_key.resize(VirtualNode::SIZE, 0);
            table_key.extend_from_slice(format!("key_test_{:05}", idx * 2).as_bytes());
            let k = UserKey::for_test(TableId::new(2), table_key.as_slice());
            let hash = Sstable::hash_for_filter(&k.encode(), 2);
            let key_ref = k.as_ref();
            assert!(
                table.may_match_hash(&(Bound::Included(key_ref), Bound::Included(key_ref)), hash)
            );
        }
    }
}
