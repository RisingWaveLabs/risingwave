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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;

use await_tree::{InstrumentAwait, SpanExt};
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use num_integer::Integer;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::LocalSstableInfo;
use risingwave_hummock_sdk::key::{FullKey, UserKey};
use tokio::task::JoinHandle;

use crate::compaction_catalog_manager::CompactionCatalogAgentRef;
use crate::hummock::compactor::task_progress::TaskProgress;
use crate::hummock::sstable::filter::FilterBuilder;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::value::HummockValue;
use crate::hummock::{
    BatchUploadWriter, BlockMeta, CachePolicy, HummockError, HummockResult, MemoryLimiter,
    SstableBuilder, SstableBuilderOptions, SstableWriter, SstableWriterOptions, Xor16FilterBuilder,
};
use crate::monitor::CompactorMetrics;

pub type UploadJoinHandle = JoinHandle<HummockResult<()>>;

#[async_trait::async_trait]
pub trait TableBuilderFactory {
    type Writer: SstableWriter<Output = UploadJoinHandle>;
    type Filter: FilterBuilder;
    async fn open_builder(&mut self) -> HummockResult<SstableBuilder<Self::Writer, Self::Filter>>;
}

/// A wrapper for [`SstableBuilder`] which automatically split key-value pairs into multiple tables,
/// based on their target capacity set in options.
///
/// When building is finished, one may call `finish` to get the results of zero, one or more tables.
pub struct CapacitySplitTableBuilder<F>
where
    F: TableBuilderFactory,
{
    /// When creating a new [`SstableBuilder`], caller use this factory to generate it.
    builder_factory: F,

    sst_outputs: Vec<LocalSstableInfo>,

    current_builder: Option<SstableBuilder<F::Writer, F::Filter>>,

    /// Statistics.
    pub compactor_metrics: Arc<CompactorMetrics>,

    /// Update the number of sealed Sstables.
    task_progress: Option<Arc<TaskProgress>>,

    last_table_id: TableId,

    vnode_count: usize,
    table_vnode_partition: BTreeMap<TableId, u32>,
    split_weight_by_vnode: u32,
    /// When vnode of the coming key is greater than `largest_vnode_in_current_partition`, we will
    /// switch SST.
    largest_vnode_in_current_partition: usize,

    concurrent_upload_join_handle: FuturesUnordered<UploadJoinHandle>,

    concurrent_uploading_sst_count: Option<usize>,

    compaction_catalog_agent_ref: CompactionCatalogAgentRef,
}

impl<F> CapacitySplitTableBuilder<F>
where
    F: TableBuilderFactory,
{
    /// Creates a new [`CapacitySplitTableBuilder`] using given configuration generator.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        builder_factory: F,
        compactor_metrics: Arc<CompactorMetrics>,
        task_progress: Option<Arc<TaskProgress>>,
        table_vnode_partition: BTreeMap<TableId, u32>,
        concurrent_uploading_sst_count: Option<usize>,
        compaction_catalog_agent_ref: CompactionCatalogAgentRef,
    ) -> Self {
        // TODO(var-vnode): should use value from caller
        let vnode_count = VirtualNode::COUNT_FOR_COMPAT;

        Self {
            builder_factory,
            sst_outputs: Vec::new(),
            current_builder: None,
            compactor_metrics,
            task_progress,
            last_table_id: 0.into(),
            table_vnode_partition,
            vnode_count,
            split_weight_by_vnode: 0,
            largest_vnode_in_current_partition: vnode_count - 1,
            concurrent_upload_join_handle: FuturesUnordered::new(),
            concurrent_uploading_sst_count,
            compaction_catalog_agent_ref,
        }
    }

    pub fn for_test(
        builder_factory: F,
        compaction_catalog_agent_ref: CompactionCatalogAgentRef,
    ) -> Self {
        Self {
            builder_factory,
            sst_outputs: Vec::new(),
            current_builder: None,
            compactor_metrics: Arc::new(CompactorMetrics::unused()),
            task_progress: None,
            last_table_id: 0.into(),
            table_vnode_partition: BTreeMap::default(),
            vnode_count: VirtualNode::COUNT_FOR_TEST,
            split_weight_by_vnode: 0,
            largest_vnode_in_current_partition: VirtualNode::MAX_FOR_TEST.to_index(),
            concurrent_upload_join_handle: FuturesUnordered::new(),
            concurrent_uploading_sst_count: None,
            compaction_catalog_agent_ref,
        }
    }

    /// Returns the number of [`SstableBuilder`]s.
    pub fn len(&self) -> usize {
        self.sst_outputs.len() + self.current_builder.is_some() as usize
    }

    /// Returns true if no builder is created.
    pub fn is_empty(&self) -> bool {
        self.sst_outputs.is_empty() && self.current_builder.is_none()
    }

    pub async fn add_full_key_for_test(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        is_new_user_key: bool,
    ) -> HummockResult<()> {
        self.add_full_key(full_key, value, is_new_user_key).await
    }

    pub async fn add_raw_block(
        &mut self,
        buf: Bytes,
        filter_data: Vec<u8>,
        smallest_key: FullKey<Vec<u8>>,
        largest_key: Vec<u8>,
        block_meta: BlockMeta,
    ) -> HummockResult<bool> {
        if self.current_builder.is_none() {
            if let Some(progress) = &self.task_progress {
                progress.inc_num_pending_write_io()
            }
            let builder = self
                .builder_factory
                .open_builder()
                .instrument_await("multi_builder_open_builder_for_raw_block".verbose())
                .await?;
            self.current_builder = Some(builder);
        }

        let builder = self.current_builder.as_mut().unwrap();
        builder
            .add_raw_block(buf, filter_data, smallest_key, largest_key, block_meta)
            .await
    }

    /// Adds a key-value pair to the underlying builders.
    ///
    /// If `allow_split` and the current builder reaches its capacity, this function will create a
    /// new one with the configuration generated by the closure provided earlier.
    ///
    /// Note that in some cases like compaction of the same user key, automatic splitting is not
    /// allowed, where `allow_split` should be `false`.
    pub async fn add_full_key(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        is_new_user_key: bool,
    ) -> HummockResult<()> {
        let switch_builder = self.check_switch_builder(&full_key.user_key);

        // We use this `need_seal_current` flag to store whether we need to call `seal_current` and
        // then call `seal_current` later outside the `if let` instead of calling
        // `seal_current` at where we set `need_seal_current = true`. This is because
        // `seal_current` is an async method, and if we call `seal_current` within the `if let`,
        // this temporary reference to `current_builder` will be captured in the future generated
        // from the current method. Since this generated future is usually required to be `Send`,
        // the captured reference to `current_builder` is also required to be `Send`, and then
        // `current_builder` itself is required to be `Sync`, which is unnecessary.
        let mut need_seal_current = false;
        if let Some(builder) = self.current_builder.as_mut()
            && is_new_user_key
        {
            need_seal_current = switch_builder || builder.reach_capacity();
        }

        if need_seal_current {
            self.seal_current()
                .instrument_await("multi_builder_seal_current".verbose())
                .await?;
        }

        if self.current_builder.is_none() {
            if let Some(progress) = &self.task_progress {
                progress.inc_num_pending_write_io();
            }
            let builder = self
                .builder_factory
                .open_builder()
                .instrument_await("multi_builder_open_builder".verbose())
                .await?;
            self.current_builder = Some(builder);
        }

        let builder = self.current_builder.as_mut().unwrap();
        builder
            .add(full_key, value)
            .instrument_await("sstable_builder_add".verbose())
            .await
    }

    pub fn check_switch_builder(&mut self, user_key: &UserKey<&[u8]>) -> bool {
        let mut switch_builder = false;
        if user_key.table_id != self.last_table_id {
            let new_vnode_partition_count = self.table_vnode_partition.get(&user_key.table_id);

            self.vnode_count = self
                .compaction_catalog_agent_ref
                .vnode_count(user_key.table_id);
            self.largest_vnode_in_current_partition = self.vnode_count - 1;

            if new_vnode_partition_count.is_some()
                || self.table_vnode_partition.contains_key(&self.last_table_id)
            {
                if let Some(new_vnode_partition_count) = new_vnode_partition_count {
                    if (*new_vnode_partition_count as usize) > self.vnode_count {
                        tracing::warn!(
                            "vnode partition count {} is larger than vnode count {}",
                            new_vnode_partition_count,
                            self.vnode_count
                        );

                        self.split_weight_by_vnode = 0;
                    } else {
                        self.split_weight_by_vnode = *new_vnode_partition_count;
                    };
                } else {
                    self.split_weight_by_vnode = 0;
                }

                // table_id change
                self.last_table_id = user_key.table_id;
                switch_builder = true;
                if self.split_weight_by_vnode > 1 {
                    self.largest_vnode_in_current_partition =
                        self.vnode_count / (self.split_weight_by_vnode as usize) - 1;
                } else {
                    // default
                    self.largest_vnode_in_current_partition = self.vnode_count - 1;
                }
            }
        }
        if self.largest_vnode_in_current_partition != self.vnode_count - 1 {
            let key_vnode = user_key.get_vnode_id();
            if key_vnode > self.largest_vnode_in_current_partition {
                // vnode partition change
                switch_builder = true;

                // SAFETY: `self.split_weight_by_vnode > 1` here.
                let (basic, remainder) = self
                    .vnode_count
                    .div_rem(&(self.split_weight_by_vnode as usize));
                let small_segments_area = basic * (self.split_weight_by_vnode as usize - remainder);
                self.largest_vnode_in_current_partition = (if key_vnode < small_segments_area {
                    (key_vnode / basic + 1) * basic
                } else {
                    ((key_vnode - small_segments_area) / (basic + 1) + 1) * (basic + 1)
                        + small_segments_area
                }) - 1;
                debug_assert!(key_vnode <= self.largest_vnode_in_current_partition);
            }
        }
        switch_builder
    }

    pub fn need_flush(&self) -> bool {
        self.current_builder
            .as_ref()
            .map(|builder| builder.reach_capacity())
            .unwrap_or(false)
    }

    /// Marks the current builder as sealed. Next call of `add` will always create a new table.
    ///
    /// If there's no builder created, or current one is already sealed before, then this function
    /// will be no-op.
    pub async fn seal_current(&mut self) -> HummockResult<()> {
        if let Some(builder) = self.current_builder.take() {
            let builder_output = builder
                .finish()
                .instrument_await("sstable_builder_finish".verbose())
                .await?;
            {
                // report
                if let Some(progress) = &self.task_progress {
                    progress.inc_ssts_sealed();
                }
                builder_output.stats.report_stats(&self.compactor_metrics);
            }

            self.concurrent_upload_join_handle
                .push(builder_output.writer_output);

            self.sst_outputs.push(builder_output.sst_info);

            if let Some(concurrent_uploading_sst_count) = self.concurrent_uploading_sst_count
                && self.concurrent_upload_join_handle.len() >= concurrent_uploading_sst_count
            {
                self.concurrent_upload_join_handle
                    .next()
                    .instrument_await("upload".verbose())
                    .await
                    .unwrap()
                    .map_err(HummockError::sstable_upload_error)??;
            }
        }
        Ok(())
    }

    /// Finalizes all the tables to be ids, blocks and metadata.
    pub async fn finish(mut self) -> HummockResult<Vec<LocalSstableInfo>> {
        use futures::future::try_join_all;
        self.seal_current()
            .instrument_await("multi_builder_finish_seal_current".verbose())
            .await?;
        try_join_all(self.concurrent_upload_join_handle)
            .instrument_await("multi_builder_wait_all_uploads".verbose())
            .await
            .map_err(HummockError::sstable_upload_error)?
            .into_iter()
            .collect::<HummockResult<Vec<()>>>()?;

        Ok(self.sst_outputs)
    }
}

/// Used for unit tests and benchmarks.
pub struct LocalTableBuilderFactory {
    next_id: AtomicU64,
    sstable_store: SstableStoreRef,
    options: SstableBuilderOptions,
    policy: CachePolicy,
    limiter: MemoryLimiter,
}

impl LocalTableBuilderFactory {
    pub fn new(
        next_id: u64,
        sstable_store: SstableStoreRef,
        options: SstableBuilderOptions,
    ) -> Self {
        Self {
            next_id: AtomicU64::new(next_id),
            sstable_store,
            options,
            policy: CachePolicy::NotFill,
            limiter: MemoryLimiter::new(1000000),
        }
    }
}

#[async_trait::async_trait]
impl TableBuilderFactory for LocalTableBuilderFactory {
    type Filter = Xor16FilterBuilder;
    type Writer = BatchUploadWriter;

    async fn open_builder(
        &mut self,
    ) -> HummockResult<SstableBuilder<BatchUploadWriter, Xor16FilterBuilder>> {
        let id = self.next_id.fetch_add(1, SeqCst);
        let tracker = self
            .limiter
            .require_memory(1)
            .instrument_await("local_builder_require_memory".verbose())
            .await;
        let writer_options = SstableWriterOptions {
            capacity_hint: Some(self.options.capacity),
            tracker: Some(tracker),
            policy: self.policy,
        };
        let writer = self
            .sstable_store
            .clone()
            .create_sst_writer(id, writer_options);
        let table_id_to_vnode = HashMap::from_iter(vec![(
            TableId::default().as_raw_id(),
            VirtualNode::COUNT_FOR_TEST,
        )]);
        let table_id_to_watermark_serde =
            HashMap::from_iter(vec![(TableId::default().as_raw_id(), None)]);
        let builder = SstableBuilder::for_test(
            id,
            writer,
            self.options.clone(),
            table_id_to_vnode,
            table_id_to_watermark_serde,
        );

        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::TableId;
    use risingwave_common::util::epoch::{EpochExt, test_epoch};

    use super::*;
    use crate::compaction_catalog_manager::{
        CompactionCatalogAgent, FilterKeyExtractorImpl, FullKeyFilterKeyExtractor,
    };
    use crate::hummock::DEFAULT_RESTART_INTERVAL;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{default_builder_opt_for_test, test_key_of, test_user_key_of};

    #[tokio::test]
    async fn test_empty() {
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let opts = SstableBuilderOptions {
            capacity: table_capacity,
            block_capacity: block_size,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: 0.1,
            ..Default::default()
        };
        let builder_factory = LocalTableBuilderFactory::new(1001, mock_sstable_store().await, opts);
        let compaction_catalog_agent_ref = Arc::new(CompactionCatalogAgent::dummy());
        let builder =
            CapacitySplitTableBuilder::for_test(builder_factory, compaction_catalog_agent_ref);
        let results = builder.finish().await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_lots_of_tables() {
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let opts = SstableBuilderOptions {
            capacity: table_capacity,
            block_capacity: block_size,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: 0.1,
            ..Default::default()
        };
        let compaction_catalog_agent_ref = CompactionCatalogAgent::for_test(vec![0]);

        let builder_factory = LocalTableBuilderFactory::new(1001, mock_sstable_store().await, opts);
        let mut builder =
            CapacitySplitTableBuilder::for_test(builder_factory, compaction_catalog_agent_ref);

        for i in 0..table_capacity {
            builder
                .add_full_key_for_test(
                    FullKey::from_user_key(
                        test_user_key_of(i).as_ref(),
                        test_epoch((table_capacity - i) as u64),
                    ),
                    HummockValue::put(b"value"),
                    true,
                )
                .await
                .unwrap();
        }

        let results = builder.finish().await.unwrap();
        assert!(results.len() > 1);
    }

    #[tokio::test]
    async fn test_table_seal() {
        let opts = default_builder_opt_for_test();
        let compaction_catalog_agent_ref = CompactionCatalogAgent::for_test(vec![0]);
        let mut builder = CapacitySplitTableBuilder::for_test(
            LocalTableBuilderFactory::new(1001, mock_sstable_store().await, opts),
            compaction_catalog_agent_ref,
        );
        let mut epoch = test_epoch(100);

        macro_rules! add {
            () => {
                epoch.dec_epoch();
                builder
                    .add_full_key_for_test(
                        FullKey::from_user_key(test_user_key_of(1).as_ref(), epoch),
                        HummockValue::put(b"v"),
                        true,
                    )
                    .await
                    .unwrap();
            };
        }

        assert_eq!(builder.len(), 0);
        builder.seal_current().await.unwrap();
        assert_eq!(builder.len(), 0);
        add!();
        assert_eq!(builder.len(), 1);
        add!();
        assert_eq!(builder.len(), 1);
        builder.seal_current().await.unwrap();
        assert_eq!(builder.len(), 1);
        add!();
        assert_eq!(builder.len(), 2);
        builder.seal_current().await.unwrap();
        assert_eq!(builder.len(), 2);
        builder.seal_current().await.unwrap();
        assert_eq!(builder.len(), 2);

        let results = builder.finish().await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_initial_not_allowed_split() {
        let opts = default_builder_opt_for_test();
        let compaction_catalog_agent_ref = CompactionCatalogAgent::for_test(vec![0]);
        let mut builder = CapacitySplitTableBuilder::for_test(
            LocalTableBuilderFactory::new(1001, mock_sstable_store().await, opts),
            compaction_catalog_agent_ref,
        );
        builder
            .add_full_key_for_test(test_key_of(0).to_ref(), HummockValue::put(b"v"), false)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_check_table_and_vnode_change() {
        let block_size = 256;
        let table_capacity = 2 * block_size;
        let opts = SstableBuilderOptions {
            capacity: table_capacity,
            block_capacity: block_size,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: 0.1,
            ..Default::default()
        };

        {
            let table_partition_vnode = BTreeMap::from([
                (1_u32.into(), 4_u32),
                (2_u32.into(), 4_u32),
                (3_u32.into(), 4_u32),
            ]);

            let compaction_catalog_agent_ref =
                CompactionCatalogAgent::for_test(vec![0, 1, 2, 3, 4, 5]);
            let mut builder = CapacitySplitTableBuilder::new(
                LocalTableBuilderFactory::new(1001, mock_sstable_store().await, opts.clone()),
                Arc::new(CompactorMetrics::unused()),
                None,
                table_partition_vnode,
                None,
                compaction_catalog_agent_ref,
            );

            let mut table_key = VirtualNode::from_index(0).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());

            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(1), &table_key));
            assert!(switch_builder);

            let mut table_key = VirtualNode::from_index(62).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(1), &table_key));
            assert!(!switch_builder);

            let mut table_key = VirtualNode::from_index(63).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(1), &table_key));
            assert!(!switch_builder);

            let mut table_key = VirtualNode::from_index(64).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(1), &table_key));
            assert!(switch_builder);

            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(2), &table_key));
            assert!(switch_builder);
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(3), &table_key));
            assert!(switch_builder);
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(4), &table_key));
            assert!(switch_builder);
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(5), &table_key));
            assert!(!switch_builder);
        }

        {
            // Test different table vnode count
            let table_partition_vnode = BTreeMap::from([
                (1_u32.into(), 4_u32),
                (2_u32.into(), 4_u32),
                (3_u32.into(), 4_u32),
            ]);

            let table_id_to_vnode =
                HashMap::from_iter(vec![(1.into(), 64), (2.into(), 128), (3.into(), 256)]);
            let table_id_to_watermark_serde =
                HashMap::from_iter(vec![(1.into(), None), (2.into(), None), (3.into(), None)]);
            let compaction_catalog_agent_ref = Arc::new(CompactionCatalogAgent::new(
                FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor),
                table_id_to_vnode,
                table_id_to_watermark_serde,
                HashMap::default(),
            ));

            let mut builder = CapacitySplitTableBuilder::new(
                LocalTableBuilderFactory::new(1001, mock_sstable_store().await, opts),
                Arc::new(CompactorMetrics::unused()),
                None,
                table_partition_vnode,
                None,
                compaction_catalog_agent_ref,
            );

            let mut table_key = VirtualNode::from_index(0).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());

            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(1), &table_key));
            assert!(switch_builder);

            let mut table_key = VirtualNode::from_index(15).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(1), &table_key));
            assert!(!switch_builder);

            let mut table_key = VirtualNode::from_index(16).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(1), &table_key));
            assert!(switch_builder);

            let mut table_key = VirtualNode::from_index(0).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(2), &table_key));
            assert!(switch_builder);

            let mut table_key = VirtualNode::from_index(16).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(2), &table_key));
            assert!(!switch_builder);

            let mut table_key = VirtualNode::from_index(31).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(2), &table_key));
            assert!(!switch_builder);

            let mut table_key = VirtualNode::from_index(32).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(2), &table_key));
            assert!(switch_builder);

            let mut table_key = VirtualNode::from_index(64).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(2), &table_key));
            assert!(switch_builder);

            let mut table_key = VirtualNode::from_index(0).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(3), &table_key));
            assert!(switch_builder);

            let mut table_key = VirtualNode::from_index(16).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(3), &table_key));
            assert!(!switch_builder);

            let mut table_key = VirtualNode::from_index(32).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(3), &table_key));
            assert!(!switch_builder);

            let mut table_key = VirtualNode::from_index(63).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(3), &table_key));
            assert!(!switch_builder);

            let mut table_key = VirtualNode::from_index(64).to_be_bytes().to_vec();
            table_key.extend_from_slice("a".as_bytes());
            let switch_builder =
                builder.check_switch_builder(&UserKey::for_test(TableId::from(3), &table_key));
            assert!(switch_builder);
        }
    }
}
