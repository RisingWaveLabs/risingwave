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

use std::cmp::Ordering;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, atomic};
use std::time::Instant;

use await_tree::{InstrumentAwait, SpanExt};
use bytes::Bytes;
use fail::fail_point;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::TableStats;
use risingwave_hummock_sdk::{EpochWithGap, LocalSstableInfo, can_concat, compact_task_to_string};
use risingwave_pb::hummock::PbSstableFilterLayout;

use crate::compaction_catalog_manager::CompactionCatalogAgentRef;
use crate::hummock::block_stream::BlockDataStream;
use crate::hummock::compactor::compaction_utils::{
    blocked_xor_filter_key_count_threshold, estimate_output_key_count_for_task,
};
use crate::hummock::compactor::task_progress::TaskProgress;
use crate::hummock::compactor::{
    CompactionFilter, CompactionStatistics, Compactor, CompactorContext, MultiCompactionFilter,
    RemoteBuilderFactory, TaskConfig,
};
use crate::hummock::iterator::{
    NonPkPrefixSkipWatermarkState, PkPrefixSkipWatermarkState, SkipWatermarkState,
    ValueSkipWatermarkState,
};
use crate::hummock::multi_builder::{CapacitySplitTableBuilder, TableBuilderFactory};
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::value::HummockValue;
use crate::hummock::{
    Block, BlockBuilder, BlockHolder, BlockIterator, BlockMeta, BlockedXor16FilterBuilder,
    CachePolicy, CompressionAlgorithm, FilterBuilder, GetObjectId, HummockResult,
    SstableBuilderOptions, StreamingSstableWriterFactory, TableHolder,
};
use crate::monitor::{CompactorMetrics, StoreLocalStatistic};

/// Iterates over the KV-pairs of an SST while downloading it.
pub struct BlockStreamIterator {
    /// The downloading stream.
    block_stream: Option<BlockDataStream>,

    next_block_index: usize,

    /// For key sanity check of divided SST and debugging
    sstable: TableHolder,
    iter: Option<BlockIterator>,
    task_progress: Arc<TaskProgress>,

    // For block stream recreate
    sstable_store: SstableStoreRef,
    sstable_info: SstableInfo,
    io_retry_times: usize,
    max_io_retry_times: usize,
    stats_ptr: Arc<AtomicU64>,
}

impl BlockStreamIterator {
    // We have to handle two internal iterators.
    //   `block_stream`: iterates over the blocks of the table.
    //     `block_iter`: iterates over the KV-pairs of the current block.
    // These iterators work in different ways.

    // BlockIterator works as follows: After new(), we call seek(). That brings us
    // to the first element. Calling next() then brings us to the second element and does not
    // return anything.

    // BlockStream follows a different approach. After new(), we do not seek, instead next()
    // returns the first value.

    /// Initialises a new [`BlockStreamIterator`] which iterates over the given [`BlockDataStream`].
    /// The iterator reads at most `max_block_count` from the stream.
    pub fn new(
        sstable: TableHolder,
        task_progress: Arc<TaskProgress>,
        sstable_store: SstableStoreRef,
        sstable_info: SstableInfo,
        max_io_retry_times: usize,
        stats_ptr: Arc<AtomicU64>,
    ) -> Self {
        Self {
            block_stream: None,
            next_block_index: 0,
            sstable,
            iter: None,
            task_progress,
            sstable_store,
            sstable_info,
            io_retry_times: 0,
            max_io_retry_times,
            stats_ptr,
        }
    }

    async fn create_stream(&mut self) -> HummockResult<()> {
        // Fast compaction streams the physical SST blocks directly. Table-id pruning is handled
        // later by `CompactTaskExecutor` before raw block copy or decoded block compaction.
        let block_stream = self
            .sstable_store
            .get_stream_for_blocks(
                self.sstable_info.object_id,
                &self.sstable.meta.block_metas[self.next_block_index..],
            )
            .instrument_await("stream_iter_get_stream".verbose())
            .await?;
        self.block_stream = Some(block_stream);
        Ok(())
    }

    /// Wrapper function for `self.block_stream.next()` which allows us to measure the time needed.
    pub(crate) async fn download_next_block(
        &mut self,
    ) -> HummockResult<Option<(Bytes, Vec<u8>, BlockMeta)>> {
        let now = Instant::now();
        let _time_stat = scopeguard::guard(self.stats_ptr.clone(), |stats_ptr: Arc<AtomicU64>| {
            let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
            stats_ptr.fetch_add(add as u64, atomic::Ordering::Relaxed);
        });
        loop {
            let ret = match &mut self.block_stream {
                Some(block_stream) => block_stream.next_block_impl().await,
                None => {
                    self.create_stream().await?;
                    continue;
                }
            };
            match ret {
                Ok(Some((data, _))) => {
                    let meta = self.sstable.meta.block_metas[self.next_block_index].clone();
                    let filter_block = self
                        .sstable
                        .filter_reader
                        .get_block_raw_filter(self.next_block_index);
                    self.next_block_index += 1;
                    return Ok(Some((data, filter_block, meta)));
                }

                Ok(None) => break,

                Err(e) => {
                    if !e.is_object_error() || self.io_retry_times >= self.max_io_retry_times {
                        return Err(e);
                    }

                    self.block_stream.take();
                    self.io_retry_times += 1;
                    fail_point!("create_stream_err");

                    tracing::warn!(
                        "fast compact retry create stream for sstable {} times, sstinfo={}",
                        self.io_retry_times,
                        format!(
                            "object_id={}, sst_id={}, meta_offset={}, table_ids={:?}",
                            self.sstable_info.object_id,
                            self.sstable_info.sst_id,
                            self.sstable_info.meta_offset,
                            self.sstable_info.table_ids
                        )
                    );
                }
            }
        }

        self.next_block_index = self.sstable.meta.block_metas.len();
        self.iter.take();
        Ok(None)
    }

    pub(crate) fn init_block_iter(
        &mut self,
        buf: Bytes,
        uncompressed_capacity: usize,
    ) -> HummockResult<()> {
        let block = Block::decode(buf, uncompressed_capacity)?;
        let mut iter = BlockIterator::new(BlockHolder::from_owned_block(Box::new(block)));
        iter.seek_to_first();
        self.iter = Some(iter);
        Ok(())
    }

    fn next_block_smallest(&self) -> &[u8] {
        self.sstable.meta.block_metas[self.next_block_index]
            .smallest_key
            .as_ref()
    }

    fn next_block_largest(&self) -> &[u8] {
        if self.next_block_index + 1 < self.sstable.meta.block_metas.len() {
            self.sstable.meta.block_metas[self.next_block_index + 1]
                .smallest_key
                .as_ref()
        } else {
            self.sstable.meta.largest_key.as_ref()
        }
    }

    fn current_block_largest(&self) -> Vec<u8> {
        if self.next_block_index < self.sstable.meta.block_metas.len() {
            let mut largest_key = FullKey::decode(
                self.sstable.meta.block_metas[self.next_block_index]
                    .smallest_key
                    .as_ref(),
            );
            // do not include this key because it is the smallest key of next block.
            largest_key.epoch_with_gap = EpochWithGap::new_max_epoch();
            largest_key.encode()
        } else {
            self.sstable.meta.largest_key.clone()
        }
    }

    fn key(&self) -> FullKey<&[u8]> {
        match self.iter.as_ref() {
            Some(iter) => iter.key(),
            None => FullKey::decode(
                self.sstable.meta.block_metas[self.next_block_index]
                    .smallest_key
                    .as_ref(),
            ),
        }
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.iter.is_some() || self.next_block_index < self.sstable.meta.block_metas.len()
    }

    #[cfg(test)]
    #[cfg(feature = "failpoints")]
    pub(crate) fn iter_mut(&mut self) -> &mut BlockIterator {
        self.iter.as_mut().unwrap()
    }
}

impl Drop for BlockStreamIterator {
    fn drop(&mut self) {
        self.task_progress.dec_num_pending_read_io();
    }
}

/// Iterates over the KV-pairs of a given list of SSTs. The key-ranges of these SSTs are assumed to
/// be consecutive and non-overlapping.
pub struct ConcatSstableIterator {
    /// The iterator of the current table.
    sstable_iter: Option<BlockStreamIterator>,

    /// Current table index.
    cur_idx: usize,

    /// All non-overlapping tables.
    sstables: Vec<SstableInfo>,

    sstable_store: SstableStoreRef,

    stats: StoreLocalStatistic,
    task_progress: Arc<TaskProgress>,

    max_io_retry_times: usize,
}

impl ConcatSstableIterator {
    /// Caller should make sure that `tables` are non-overlapping,
    /// arranged in ascending order when it serves as a forward iterator,
    /// and arranged in descending order when it serves as a backward iterator.
    pub fn new(
        sst_infos: Vec<SstableInfo>,
        sstable_store: SstableStoreRef,
        task_progress: Arc<TaskProgress>,
        max_io_retry_times: usize,
    ) -> Self {
        Self {
            sstable_iter: None,
            cur_idx: 0,
            sstables: sst_infos,
            sstable_store,
            task_progress,
            stats: StoreLocalStatistic::default(),
            max_io_retry_times,
        }
    }

    pub async fn rewind(&mut self) -> HummockResult<()> {
        self.seek_idx(0).await
    }

    pub async fn next_sstable(&mut self) -> HummockResult<()> {
        self.seek_idx(self.cur_idx + 1).await
    }

    pub fn current_sstable(&mut self) -> &mut BlockStreamIterator {
        self.sstable_iter.as_mut().unwrap()
    }

    pub async fn init_block_iter(&mut self) -> HummockResult<()> {
        if let Some(sstable) = self.sstable_iter.as_mut() {
            if sstable.iter.is_some() {
                return Ok(());
            }
            let (buf, _, meta) = sstable.download_next_block().await?.unwrap();
            sstable.init_block_iter(buf, meta.uncompressed_size as usize)?;
        }
        Ok(())
    }

    pub fn is_valid(&self) -> bool {
        self.cur_idx < self.sstables.len()
    }

    /// Resets the iterator, loads the specified SST, and seeks in that SST to `seek_key` if given.
    async fn seek_idx(&mut self, idx: usize) -> HummockResult<()> {
        self.sstable_iter.take();
        self.cur_idx = idx;
        if self.cur_idx < self.sstables.len() {
            let sstable_info = &self.sstables[self.cur_idx];
            let sstable = self
                .sstable_store
                .sstable(sstable_info, &mut self.stats)
                .instrument_await("stream_iter_sstable".verbose())
                .await?;
            self.task_progress.inc_num_pending_read_io();

            let sstable_iter = BlockStreamIterator::new(
                sstable,
                self.task_progress.clone(),
                self.sstable_store.clone(),
                sstable_info.clone(),
                self.max_io_retry_times,
                self.stats.remote_io_time.clone(),
            );
            self.sstable_iter = Some(sstable_iter);
        }
        Ok(())
    }
}

pub struct CompactorRunner<
    B: FilterBuilder = BlockedXor16FilterBuilder,
    C: CompactionFilter = MultiCompactionFilter,
> {
    left: Box<ConcatSstableIterator>,
    right: Box<ConcatSstableIterator>,
    task_id: u64,
    executor: CompactTaskExecutor<RemoteBuilderFactory<StreamingSstableWriterFactory, B>, C>,
    compression_algorithm: CompressionAlgorithm,
    metrics: Arc<CompactorMetrics>,
}

impl<B: FilterBuilder, C: CompactionFilter> CompactorRunner<B, C> {
    pub fn new(
        context: CompactorContext,
        task: CompactTask,
        compaction_catalog_agent_ref: CompactionCatalogAgentRef,
        object_id_getter: Arc<dyn GetObjectId>,
        task_progress: Arc<TaskProgress>,
        compaction_filter: C,
    ) -> Self {
        let mut options: SstableBuilderOptions = context.storage_opts.as_ref().into();
        let compression_algorithm: CompressionAlgorithm = task.compression_algorithm.into();
        options.compression_algorithm = compression_algorithm;
        options.capacity = task.target_file_size as usize;
        let estimated_output_key_count =
            estimate_output_key_count_for_task(&task, options.capacity);
        options.estimated_output_key_count = Some(estimated_output_key_count);
        options.filter_hash_prealloc_key_count_cap =
            blocked_xor_filter_key_count_threshold(task.blocked_xor_filter_kv_count_threshold);
        // Disable vnode key-range hints for fast compaction path by default.
        options.max_vnode_key_range_bytes = None;
        let get_id_time = Arc::new(AtomicU64::new(0));

        let key_range = KeyRange::inf();
        let read_table_ids = HashSet::from_iter(task.get_table_ids_from_input_ssts());

        let task_config = TaskConfig {
            key_range,
            cache_policy: CachePolicy::NotFill,
            gc_delete_keys: task.gc_delete_keys,
            retain_multiple_version: false,
            table_vnode_partition: task.table_vnode_partition.clone(),
            sstable_filter_layout: PbSstableFilterLayout::Blocked,
            sstable_filter_type: task.sstable_filter_type,
            table_schemas: Default::default(),
            disable_drop_column_optimization: false,
        };
        let factory = StreamingSstableWriterFactory::new(context.sstable_store.clone());

        let builder_factory = RemoteBuilderFactory::<_, B> {
            object_id_getter,
            limiter: context.memory_limiter.clone(),
            options,
            policy: task_config.cache_policy,
            remote_rpc_cost: get_id_time,
            compaction_catalog_agent_ref: compaction_catalog_agent_ref.clone(),
            sstable_writer_factory: factory,
            _phantom: PhantomData,
        };
        let sst_builder = CapacitySplitTableBuilder::new(
            builder_factory,
            context.compactor_metrics.clone(),
            Some(task_progress.clone()),
            task_config.table_vnode_partition.clone(),
            context
                .storage_opts
                .compactor_concurrent_uploading_sst_count,
            compaction_catalog_agent_ref.clone(),
        );
        assert_eq!(
            task.input_ssts.len(),
            2,
            "TaskId {} target_level {:?} task {:?}",
            task.task_id,
            task.target_level,
            compact_task_to_string(&task)
        );
        let left_ssts = task.input_ssts[0]
            .read_sstable_infos()
            .cloned()
            .collect_vec();
        let right_ssts = task.input_ssts[1]
            .read_sstable_infos()
            .cloned()
            .collect_vec();
        assert!(
            left_ssts
                .iter()
                .chain(right_ssts.iter())
                .all(|sst| sst.filter_layout == PbSstableFilterLayout::Blocked),
            "fast compaction requires blocked-filter SSTs: {}",
            compact_task_to_string(&task)
        );
        let left = Box::new(ConcatSstableIterator::new(
            left_ssts,
            context.sstable_store.clone(),
            task_progress.clone(),
            context.storage_opts.compactor_iter_max_io_retry_times,
        ));
        let right = Box::new(ConcatSstableIterator::new(
            right_ssts,
            context.sstable_store,
            task_progress.clone(),
            context.storage_opts.compactor_iter_max_io_retry_times,
        ));

        // Can not consume the watermarks because the watermarks may be used by `check_compact_result`.
        let pk_prefix_state = PkPrefixSkipWatermarkState::from_safe_epoch_watermarks(
            task.pk_prefix_table_watermarks.clone(),
        );
        let non_pk_prefix_state = NonPkPrefixSkipWatermarkState::from_safe_epoch_watermarks(
            task.non_pk_prefix_table_watermarks.clone(),
            compaction_catalog_agent_ref.clone(),
        );
        let value_skip_watermark_state = ValueSkipWatermarkState::from_safe_epoch_watermarks(
            task.value_table_watermarks.clone(),
            compaction_catalog_agent_ref,
        );

        Self {
            executor: CompactTaskExecutor::new(
                sst_builder,
                task_config,
                task_progress,
                pk_prefix_state,
                non_pk_prefix_state,
                value_skip_watermark_state,
                compaction_filter,
                read_table_ids,
            ),
            left,
            right,
            task_id: task.task_id,
            metrics: context.compactor_metrics,
            compression_algorithm,
        }
    }

    pub async fn run(mut self) -> HummockResult<(Vec<LocalSstableInfo>, CompactionStatistics)> {
        self.left.rewind().await?;
        self.right.rewind().await?;
        let mut skip_raw_block_count = 0;
        let mut skip_raw_block_size = 0;
        while self.left.is_valid() && self.right.is_valid() {
            let ret = self
                .left
                .current_sstable()
                .key()
                .cmp(&self.right.current_sstable().key());
            let (first, second) = if ret == Ordering::Less {
                (&mut self.left, &mut self.right)
            } else {
                (&mut self.right, &mut self.left)
            };
            assert!(
                ret != Ordering::Equal,
                "sst range overlap equal_key {:?}",
                self.left.current_sstable().key()
            );
            if first.current_sstable().iter.is_none() {
                let right_key = second.current_sstable().key();
                while first.current_sstable().is_valid() && !self.executor.builder.need_flush() {
                    let full_key = FullKey::decode(first.current_sstable().next_block_largest());
                    // the full key may be either Excluded key or Included key, so we do not allow
                    // they equals.
                    if full_key.user_key.ge(&right_key.user_key) {
                        break;
                    }
                    let smallest_key =
                        FullKey::decode(first.current_sstable().next_block_smallest());
                    if !self.executor.shall_copy_raw_block(&smallest_key) {
                        break;
                    }
                    let smallest_key = smallest_key.to_vec();

                    let (mut block, filter_data, mut meta) = first
                        .current_sstable()
                        .download_next_block()
                        .await?
                        .unwrap();
                    let algorithm = Block::get_algorithm(&block)?;
                    if algorithm == CompressionAlgorithm::None
                        && algorithm != self.compression_algorithm
                    {
                        block = BlockBuilder::compress_block(block, self.compression_algorithm)?;
                        meta.len = block.len() as u32;
                    }

                    let largest_key = first.current_sstable().current_block_largest();
                    let block_len = block.len() as u64;
                    let block_key_count = meta.total_key_count;

                    if self
                        .executor
                        .builder
                        .add_raw_block(block, filter_data, smallest_key, largest_key, meta)
                        .await?
                    {
                        skip_raw_block_size += block_len;
                        skip_raw_block_count += 1;
                    }
                    self.executor.may_report_process_key(block_key_count);
                    self.executor.clear();
                }
                if !first.current_sstable().is_valid() {
                    first.next_sstable().await?;
                    continue;
                }
                first.init_block_iter().await?;
            }

            let target_key = second.current_sstable().key();
            let iter = first.sstable_iter.as_mut().unwrap().iter.as_mut().unwrap();
            self.executor.reset_watermark();
            self.executor.run(iter, target_key).await?;
            if !iter.is_valid() {
                first.sstable_iter.as_mut().unwrap().iter.take();
                if !first.current_sstable().is_valid() {
                    first.next_sstable().await?;
                }
            }
        }
        let rest_data = if !self.left.is_valid() {
            &mut self.right
        } else {
            &mut self.left
        };
        if rest_data.is_valid() {
            // compact rest keys of the current block.
            let sstable_iter = rest_data.sstable_iter.as_mut().unwrap();
            let target_key = FullKey::decode(&sstable_iter.sstable.meta.largest_key);
            if let Some(iter) = sstable_iter.iter.as_mut() {
                self.executor.reset_watermark();
                self.executor.run(iter, target_key).await?;
                assert!(
                    !iter.is_valid(),
                    "iter should not be valid key {:?}",
                    iter.key()
                );
            }
            sstable_iter.iter.take();
        }

        while rest_data.is_valid() {
            let mut sstable_iter = rest_data.sstable_iter.take().unwrap();
            while sstable_iter.is_valid() {
                let smallest_key = FullKey::decode(sstable_iter.next_block_smallest()).to_vec();
                let (block, filter_data, block_meta) =
                    sstable_iter.download_next_block().await?.unwrap();
                // If the last key is tombstone and it was deleted, the first key of this block must be deleted. So we can not move this block directly.
                let need_deleted = self.executor.last_key.user_key.eq(&smallest_key.user_key)
                    && self.executor.last_key_is_delete;
                if self.executor.builder.need_flush()
                    || need_deleted
                    || !self.executor.shall_copy_raw_block(&smallest_key.to_ref())
                {
                    let largest_key = sstable_iter.sstable.meta.largest_key.clone();
                    let target_key = FullKey::decode(&largest_key);
                    sstable_iter.init_block_iter(block, block_meta.uncompressed_size as usize)?;
                    let mut iter = sstable_iter.iter.take().unwrap();
                    self.executor.reset_watermark();
                    self.executor.run(&mut iter, target_key).await?;
                } else {
                    let largest_key = sstable_iter.current_block_largest();
                    let block_len = block.len() as u64;
                    let block_key_count = block_meta.total_key_count;
                    if self
                        .executor
                        .builder
                        .add_raw_block(block, filter_data, smallest_key, largest_key, block_meta)
                        .await?
                    {
                        skip_raw_block_count += 1;
                        skip_raw_block_size += block_len;
                    }
                    self.executor.may_report_process_key(block_key_count);
                    self.executor.clear();
                }
            }
            rest_data.next_sstable().await?;
        }
        let mut total_read_bytes = 0;
        for sst in &self.left.sstables {
            total_read_bytes += sst.sst_size;
        }
        for sst in &self.right.sstables {
            total_read_bytes += sst.sst_size;
        }
        self.metrics
            .compact_fast_runner_bytes
            .inc_by(skip_raw_block_size);
        tracing::info!(
            "OPTIMIZATION: skip {} blocks for task-{}, optimize {}% data compression",
            skip_raw_block_count,
            self.task_id,
            skip_raw_block_size * 100 / total_read_bytes,
        );

        let statistic = self.executor.take_statistics();
        let output_ssts = self.executor.builder.finish().await?;
        Compactor::report_progress(
            self.metrics.clone(),
            Some(self.executor.task_progress.clone()),
            &output_ssts,
            false,
        );
        let sst_infos = output_ssts
            .iter()
            .map(|sst| sst.sst_info.clone())
            .collect_vec();
        assert!(can_concat(&sst_infos));
        Ok((output_ssts, statistic))
    }
}

pub struct CompactTaskExecutor<F: TableBuilderFactory, C: CompactionFilter> {
    last_key: FullKey<Vec<u8>>,
    compaction_statistics: CompactionStatistics,
    last_table_id: Option<TableId>,
    last_table_stats: TableStats,
    builder: CapacitySplitTableBuilder<F>,
    task_config: TaskConfig,
    task_progress: Arc<TaskProgress>,
    pk_prefix_skip_watermark_state: PkPrefixSkipWatermarkState,
    last_key_is_delete: bool,
    progress_key_num: u32,
    non_pk_prefix_skip_watermark_state: NonPkPrefixSkipWatermarkState,
    value_skip_watermark_state: ValueSkipWatermarkState,
    compaction_filter: C,
    read_table_ids: HashSet<TableId>,
}

impl<F: TableBuilderFactory, C: CompactionFilter> CompactTaskExecutor<F, C> {
    pub fn new(
        builder: CapacitySplitTableBuilder<F>,
        task_config: TaskConfig,
        task_progress: Arc<TaskProgress>,
        pk_prefix_skip_watermark_state: PkPrefixSkipWatermarkState,
        non_pk_prefix_skip_watermark_state: NonPkPrefixSkipWatermarkState,
        value_skip_watermark_state: ValueSkipWatermarkState,
        compaction_filter: C,
        read_table_ids: HashSet<TableId>,
    ) -> Self {
        Self {
            builder,
            task_config,
            last_key: FullKey::default(),
            last_key_is_delete: false,
            compaction_statistics: CompactionStatistics::default(),
            last_table_id: None,
            last_table_stats: TableStats::default(),
            task_progress,
            pk_prefix_skip_watermark_state,
            progress_key_num: 0,
            non_pk_prefix_skip_watermark_state,
            value_skip_watermark_state,
            compaction_filter,
            read_table_ids,
        }
    }

    fn take_statistics(&mut self) -> CompactionStatistics {
        if let Some(last_table_id) = self.last_table_id.take() {
            self.compaction_statistics
                .delta_drop_stat
                .insert(last_table_id, std::mem::take(&mut self.last_table_stats));
        }
        std::mem::take(&mut self.compaction_statistics)
    }

    fn clear(&mut self) {
        if !self.last_key.is_empty() {
            self.last_key = FullKey::default();
        }
        self.last_key_is_delete = false;
    }

    fn reset_watermark(&mut self) {
        self.pk_prefix_skip_watermark_state.reset_watermark();
        self.non_pk_prefix_skip_watermark_state.reset_watermark();
        self.value_skip_watermark_state.reset_watermark();
    }

    #[inline(always)]
    fn should_skip_block(&self, table_id: TableId) -> bool {
        !self.read_table_ids.contains(&table_id)
    }

    #[inline(always)]
    fn may_report_process_key(&mut self, key_count: u32) {
        const PROGRESS_KEY_INTERVAL: u32 = 100;
        self.progress_key_num += key_count;
        if self.progress_key_num > PROGRESS_KEY_INTERVAL {
            self.task_progress
                .inc_progress_key(self.progress_key_num as u64);
            self.progress_key_num = 0;
        }
    }

    pub async fn run(
        &mut self,
        iter: &mut BlockIterator,
        target_key: FullKey<&[u8]>,
    ) -> HummockResult<()> {
        if self.should_skip_block(iter.table_id()) {
            iter.finish_block();
            return Ok(());
        }

        while iter.is_valid() && iter.key().le(&target_key) {
            let is_new_user_key =
                !self.last_key.is_empty() && iter.key().user_key != self.last_key.user_key.as_ref();
            self.compaction_statistics.iter_total_key_counts += 1;
            self.may_report_process_key(1);

            let mut drop = false;
            let value = HummockValue::from_slice(iter.value()).unwrap();
            let is_first_or_new_user_key = is_new_user_key || self.last_key.is_empty();
            if is_first_or_new_user_key {
                self.last_key.set(iter.key());
                self.last_key_is_delete = false;
            }

            // See note in `compactor_runner.rs`.
            if !self.task_config.retain_multiple_version
                && self.task_config.gc_delete_keys
                && value.is_delete()
            {
                drop = true;
                self.last_key_is_delete = true;
            } else if !self.task_config.retain_multiple_version && !is_first_or_new_user_key {
                drop = true;
            }

            if !drop && self.compaction_filter.should_delete(iter.key()) {
                drop = true;
            }

            if !drop && self.watermark_should_delete(&iter.key(), value) {
                drop = true;
                self.last_key_is_delete = true;
            }

            if self.last_table_id != Some(self.last_key.user_key.table_id) {
                if let Some(last_table_id) = self.last_table_id.take() {
                    self.compaction_statistics
                        .delta_drop_stat
                        .insert(last_table_id, std::mem::take(&mut self.last_table_stats));
                }
                self.last_table_id = Some(self.last_key.user_key.table_id);
            }

            if drop {
                self.compaction_statistics.iter_drop_key_counts += 1;

                self.last_table_stats.total_key_count -= 1;
                self.last_table_stats.total_key_size -= self.last_key.encoded_len() as i64;
                self.last_table_stats.total_value_size -= value.encoded_len() as i64;
                iter.next();
                continue;
            }
            self.builder
                .add_full_key(iter.key(), value, is_new_user_key)
                .instrument_await("fast_add_full_key".verbose())
                .await?;
            iter.next();
        }
        Ok(())
    }

    pub fn shall_copy_raw_block(&mut self, smallest_key: &FullKey<&[u8]>) -> bool {
        if self.should_skip_block(smallest_key.user_key.table_id) {
            // If the table id of smallest key is not in read_table_ids, we can not copy the raw block.
            return false;
        }

        if self.last_key_is_delete && self.last_key.user_key.as_ref().eq(&smallest_key.user_key) {
            // If the last key is delete tombstone, we can not append the origin block
            // because it would cause a deleted key could be see by user again.
            return false;
        }

        if self.watermark_may_delete(smallest_key) {
            return false;
        }

        // Check compaction filter
        if self.compaction_filter.should_delete(*smallest_key) {
            return false;
        }

        true
    }

    fn watermark_may_delete(&mut self, key: &FullKey<&[u8]>) -> bool {
        // Correctness requires the assumption that these PkPrefixSkipWatermarkState and NonPkPrefixSkipWatermarkState never use the `unused_put`.
        let pk_prefix_has_watermark = self.pk_prefix_skip_watermark_state.has_watermark();
        let non_pk_prefix_has_watermark = self.non_pk_prefix_skip_watermark_state.has_watermark();
        if pk_prefix_has_watermark || non_pk_prefix_has_watermark {
            let unused = vec![];
            let unused_put = HummockValue::Put(unused.as_slice());
            if (pk_prefix_has_watermark
                && self
                    .pk_prefix_skip_watermark_state
                    .should_delete(key, unused_put))
                || (non_pk_prefix_has_watermark
                    && self
                        .non_pk_prefix_skip_watermark_state
                        .should_delete(key, unused_put))
            {
                return true;
            }
        }
        self.value_skip_watermark_state.has_watermark()
            && self.value_skip_watermark_state.may_delete(key)
    }

    fn watermark_should_delete(
        &mut self,
        key: &FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
    ) -> bool {
        (self.pk_prefix_skip_watermark_state.has_watermark()
            && self
                .pk_prefix_skip_watermark_state
                .should_delete(key, value))
            || (self.non_pk_prefix_skip_watermark_state.has_watermark()
                && self
                    .non_pk_prefix_skip_watermark_state
                    .should_delete(key, value))
            || (self.value_skip_watermark_state.has_watermark()
                && self.value_skip_watermark_state.should_delete(key, value))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::Arc;

    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::compact_task::CompactTask;
    use risingwave_hummock_sdk::key::FullKey;
    use risingwave_hummock_sdk::level::InputLevel;
    use risingwave_pb::hummock::compact_task::TaskType;
    use risingwave_pb::hummock::{LevelType, PbSstableFilterLayout, PbSstableFilterType};

    use super::CompactorRunner;
    use crate::compaction_catalog_manager::CompactionCatalogAgent;
    use crate::hummock::compactor::compaction_utils::optimize_by_copy_block;
    use crate::hummock::compactor::task_progress::TaskProgress;
    use crate::hummock::compactor::{CompactorContext, MultiCompactionFilter};
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, default_opts_for_test, gen_test_sstable_impl, test_value_of,
    };
    use crate::hummock::value::HummockValue;
    use crate::hummock::{
        BlockedXor16FilterBuilder, CachePolicy, SharedComapctorObjectIdManager, Xor16FilterBuilder,
    };
    use crate::monitor::CompactorMetrics;

    fn test_key(table_id: u32, idx: usize) -> FullKey<Vec<u8>> {
        let mut table_key = VirtualNode::ZERO.to_be_bytes().to_vec();
        table_key.extend_from_slice(format!("key_test_{idx:05}").as_bytes());
        FullKey::for_test(TableId::new(table_id), table_key, test_epoch(1))
    }

    #[tokio::test]
    async fn test_fast_compact_skips_empty_table_id_sst() {
        let sstable_store = mock_sstable_store().await;
        let table_id_to_vnode = HashMap::from([
            (1, VirtualNode::COUNT_FOR_TEST),
            (2, VirtualNode::COUNT_FOR_TEST),
        ]);
        let table_id_to_watermark_serde = HashMap::from([(1, None), (2, None)]);

        let mut dropped_only_sst = gen_test_sstable_impl::<_, Xor16FilterBuilder>(
            default_builder_opt_for_test(),
            1,
            (0..2).map(|idx| (test_key(1, idx), HummockValue::put(test_value_of(idx)))),
            sstable_store.clone(),
            CachePolicy::NotFill,
            table_id_to_vnode.clone(),
            table_id_to_watermark_serde.clone(),
        )
        .await;
        assert_eq!(dropped_only_sst.filter_layout, PbSstableFilterLayout::Plain);
        let mut inner = dropped_only_sst.get_inner();
        inner.table_ids.clear();
        dropped_only_sst.set_inner(inner);

        let live_left_sst = gen_test_sstable_impl::<_, BlockedXor16FilterBuilder>(
            default_builder_opt_for_test(),
            2,
            (0..2).map(|idx| (test_key(2, idx), HummockValue::put(test_value_of(idx)))),
            sstable_store.clone(),
            CachePolicy::NotFill,
            table_id_to_vnode.clone(),
            table_id_to_watermark_serde.clone(),
        )
        .await;
        let live_right_sst = gen_test_sstable_impl::<_, BlockedXor16FilterBuilder>(
            default_builder_opt_for_test(),
            3,
            (2..4).map(|idx| (test_key(2, idx), HummockValue::put(test_value_of(idx)))),
            sstable_store.clone(),
            CachePolicy::NotFill,
            table_id_to_vnode,
            table_id_to_watermark_serde,
        )
        .await;

        let mut storage_opts = default_opts_for_test();
        storage_opts.enable_fast_compaction = true;
        storage_opts.compactor_fast_max_compact_task_size = u64::MAX;
        storage_opts.compactor_fast_max_compact_delete_ratio = 100;
        let context = CompactorContext::new_local_compact_context(
            Arc::new(storage_opts),
            sstable_store,
            Arc::new(CompactorMetrics::unused()),
            None,
        );

        let task = CompactTask {
            input_ssts: vec![
                InputLevel {
                    level_idx: 1,
                    level_type: LevelType::Nonoverlapping,
                    table_infos: vec![dropped_only_sst, live_left_sst],
                },
                InputLevel {
                    level_idx: 2,
                    level_type: LevelType::Nonoverlapping,
                    table_infos: vec![live_right_sst],
                },
            ],
            task_id: 42,
            target_level: 2,
            existing_table_ids: vec![TableId::new(2)],
            target_file_size: 1 << 20,
            task_type: TaskType::Dynamic,
            blocked_xor_filter_kv_count_threshold: Some(0),
            sstable_filter_type: PbSstableFilterType::SstableFilterXor16,
            ..Default::default()
        };

        assert_eq!(task.input_ssts[0].read_sstable_infos().count(), 1);
        assert!(optimize_by_copy_block(&task, &context));

        let runner = CompactorRunner::<BlockedXor16FilterBuilder, _>::new(
            context,
            task,
            CompactionCatalogAgent::for_test(vec![1, 2]),
            SharedComapctorObjectIdManager::for_test(VecDeque::from([100])),
            Arc::new(TaskProgress::default()),
            MultiCompactionFilter::default(),
        );
        runner.run().await.unwrap();
    }
}
