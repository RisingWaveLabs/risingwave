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

use std::future::Future;
use std::iter::once;
use std::ops::Bound;
use std::sync::Arc;

use await_tree::{InstrumentAwait, SpanExt};
use bytes::Bytes;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::{EpochPair, MAX_EPOCH, MAX_SPILL_TIMES};
use risingwave_hummock_sdk::key::{
    FullKey, TableKey, TableKeyRange, UserKey, is_empty_key_range, vnode_range,
};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::{EpochWithGap, HummockEpoch};
use tracing::{Instrument, warn};

use super::version::VersionUpdate;
use crate::error::StorageResult;
use crate::hummock::event_handler::hummock_event_handler::HummockEventSender;
use crate::hummock::event_handler::{HummockEvent, HummockReadVersionRef, LocalInstanceGuard};
use crate::hummock::iterator::{
    Backward, BackwardUserIterator, ConcatIteratorInner, Forward, HummockIteratorUnion,
    IteratorFactory, MergeIterator, UserIterator,
};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::shared_buffer::TableMemoryMetrics;
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferBatchIterator, SharedBufferBatchOldValues, SharedBufferItem,
    SharedBufferValue,
};
use crate::hummock::store::version::{HummockVersionReader, read_filter_for_version};
use crate::hummock::utils::{
    do_delete_sanity_check, do_insert_sanity_check, do_update_sanity_check, sanity_check_enabled,
    wait_for_epoch,
};
use crate::hummock::write_limiter::WriteLimiterRef;
use crate::hummock::{
    BackwardSstableIterator, HummockError, MemoryLimiter, SstableIterator,
    SstableIteratorReadOptions, SstableStoreRef,
};
use crate::mem_table::{KeyOp, MemTable, MemTableHummockIterator, MemTableHummockRevIterator};
use crate::monitor::{HummockStateStoreMetrics, IterLocalMetricsGuard, StoreLocalStatistic};
use crate::store::*;

/// `LocalHummockStorage` is a handle for a state table shard to access data from and write data to
/// the hummock state backend. It is created via `HummockStorage::new_local`.
pub struct LocalHummockStorage {
    mem_table: MemTable,

    spill_offset: u16,
    epoch: Option<EpochPair>,

    table_id: TableId,
    op_consistency_level: OpConsistencyLevel,
    table_option: TableOption,

    instance_guard: LocalInstanceGuard,

    /// Read handle.
    read_version: HummockReadVersionRef,

    /// This indicates that this `LocalHummockStorage` replicates another `LocalHummockStorage`.
    /// It's used by executors in different CNs to synchronize states.
    ///
    /// Within `LocalHummockStorage` we use this flag to avoid uploading local state to be
    /// persisted, so we won't have duplicate data.
    ///
    /// This also handles a corner case where an executor doing replication
    /// is scheduled to the same CN as its Upstream executor.
    /// In that case, we use this flag to avoid reading the same data twice,
    /// by ignoring the replicated `ReadVersion`.
    is_replicated: bool,

    /// Whether or not send imm to uploader on every flush.
    upload_on_flush: bool,

    /// Event sender.
    event_sender: HummockEventSender,

    memory_limiter: Arc<MemoryLimiter>,

    hummock_version_reader: HummockVersionReader,

    table_memory_metrics: Arc<TableMemoryMetrics>,

    write_limiter: WriteLimiterRef,

    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<PinnedVersion>>,

    mem_table_spill_threshold: usize,
}

impl LocalHummockFlushedSnapshotReader {
    async fn get_flushed<'a, O>(
        hummock_version_reader: &'a HummockVersionReader,
        read_version: &HummockReadVersionRef,
        user_key: UserKey<Bytes>,
        table_option: TableOption,
        read_options: ReadOptions,
        on_key_value_fn: impl KeyValueFn<'a, O>,
        epoch: HummockEpoch,
    ) -> StorageResult<Option<O>> {
        let table_key_range = (
            Bound::Included(user_key.table_key.clone()),
            Bound::Included(user_key.table_key.clone()),
        );

        let (table_key_range, read_snapshot) =
            read_filter_for_version(epoch, user_key.table_id, table_key_range, read_version)?;

        if is_empty_key_range(&table_key_range) {
            return Ok(None);
        }

        hummock_version_reader
            .get(
                user_key.table_key,
                epoch,
                user_key.table_id,
                table_option,
                read_options,
                read_snapshot,
                on_key_value_fn,
            )
            .await
    }

    async fn iter_flushed(
        &self,
        table_key_range: TableKeyRange,
        read_options: ReadOptions,
        epoch: HummockEpoch,
    ) -> StorageResult<HummockStorageIterator> {
        let (table_key_range, read_snapshot) =
            read_filter_for_version(epoch, self.table_id, table_key_range, &self.read_version)?;

        let table_key_range = table_key_range;

        self.hummock_version_reader
            .iter(
                table_key_range,
                epoch,
                self.table_id,
                self.table_option,
                read_options,
                read_snapshot,
            )
            .await
    }

    async fn rev_iter_flushed(
        &self,
        table_key_range: TableKeyRange,
        read_options: ReadOptions,
        epoch: HummockEpoch,
    ) -> StorageResult<HummockStorageRevIterator> {
        let (table_key_range, read_snapshot) =
            read_filter_for_version(epoch, self.table_id, table_key_range, &self.read_version)?;

        let table_key_range = table_key_range;

        self.hummock_version_reader
            .rev_iter(
                table_key_range,
                epoch,
                self.table_id,
                self.table_option,
                read_options,
                read_snapshot,
                None,
            )
            .await
    }
}

impl LocalHummockStorage {
    fn epoch(&self) -> u64 {
        self.epoch.expect("should have set the epoch").curr
    }

    fn current_epoch_with_gap(&self) -> EpochWithGap {
        EpochWithGap::new(self.epoch(), self.spill_offset)
    }

    fn mem_table_iter(&self) -> MemTableHummockIterator<'_> {
        MemTableHummockIterator::new(
            &self.mem_table.buffer,
            self.current_epoch_with_gap(),
            self.table_id,
        )
    }

    fn mem_table_rev_iter(&self) -> MemTableHummockRevIterator<'_> {
        MemTableHummockRevIterator::new(
            &self.mem_table.buffer,
            self.current_epoch_with_gap(),
            self.table_id,
        )
    }

    async fn iter_all(
        &self,
        table_key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<LocalHummockStorageIterator<'_>> {
        let (table_key_range, read_snapshot) =
            read_filter_for_version(epoch, self.table_id, table_key_range, &self.read_version)?;

        self.hummock_version_reader
            .iter_with_memtable(
                table_key_range,
                epoch,
                self.table_id,
                self.table_option,
                read_options,
                read_snapshot,
                Some(self.mem_table_iter()),
            )
            .await
    }

    async fn rev_iter_all(
        &self,
        table_key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<LocalHummockStorageRevIterator<'_>> {
        let (table_key_range, read_snapshot) =
            read_filter_for_version(epoch, self.table_id, table_key_range, &self.read_version)?;

        self.hummock_version_reader
            .rev_iter(
                table_key_range,
                epoch,
                self.table_id,
                self.table_option,
                read_options,
                read_snapshot,
                Some(self.mem_table_rev_iter()),
            )
            .await
    }
}

#[derive(Clone)]
pub struct LocalHummockFlushedSnapshotReader {
    table_id: TableId,
    table_option: TableOption,
    read_version: HummockReadVersionRef,
    hummock_version_reader: HummockVersionReader,
    epoch: HummockEpoch,
}

impl StateStoreGet for LocalHummockFlushedSnapshotReader {
    async fn on_key_value<'a, O: Send + 'a>(
        &'a self,
        key: TableKey<Bytes>,
        read_options: ReadOptions,
        on_key_value_fn: impl KeyValueFn<'a, O>,
    ) -> StorageResult<Option<O>> {
        let key = UserKey::new(self.table_id, key);
        Self::get_flushed(
            &self.hummock_version_reader,
            &self.read_version,
            key,
            self.table_option,
            read_options,
            on_key_value_fn,
            self.epoch,
        )
        .await
    }
}

impl StateStoreRead for LocalHummockFlushedSnapshotReader {
    type Iter = HummockStorageIterator;
    type RevIter = HummockStorageRevIterator;

    fn iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::Iter>> + '_ {
        self.iter_flushed(key_range, read_options, self.epoch)
            .instrument(tracing::trace_span!("hummock_iter"))
    }

    fn rev_iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::RevIter>> + '_ {
        self.rev_iter_flushed(key_range, read_options, self.epoch)
            .instrument(tracing::trace_span!("hummock_rev_iter"))
    }
}

impl StateStoreGet for LocalHummockStorage {
    async fn on_key_value<'a, O: Send + 'a>(
        &'a self,
        key: TableKey<Bytes>,
        read_options: ReadOptions,
        on_key_value_fn: impl KeyValueFn<'a, O>,
    ) -> StorageResult<Option<O>> {
        let key = UserKey::new(self.table_id, key);
        match self.mem_table.buffer.get(&key.table_key) {
            None => {
                LocalHummockFlushedSnapshotReader::get_flushed(
                    &self.hummock_version_reader,
                    &self.read_version,
                    key,
                    self.table_option,
                    read_options,
                    on_key_value_fn,
                    self.epoch(),
                )
                .await
            }
            Some(op) => match op {
                KeyOp::Insert(value) | KeyOp::Update((_, value)) => Ok({
                    Some(on_key_value_fn(
                        FullKey::new_with_gap_epoch(
                            self.table_id,
                            key.table_key.to_ref(),
                            self.current_epoch_with_gap(),
                        ),
                        value.as_ref(),
                    )?)
                }),
                KeyOp::Delete(_) => Ok(None),
            },
        }
    }
}

impl LocalStateStore for LocalHummockStorage {
    type FlushedSnapshotReader = LocalHummockFlushedSnapshotReader;
    type Iter<'a> = LocalHummockStorageIterator<'a>;
    type RevIter<'a> = LocalHummockStorageRevIterator<'a>;

    async fn iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> StorageResult<Self::Iter<'_>> {
        let (l_vnode_inclusive, r_vnode_exclusive) = vnode_range(&key_range);
        assert_eq!(
            r_vnode_exclusive - l_vnode_inclusive,
            1,
            "read range {:?} for table {} iter contains more than one vnode",
            key_range,
            self.table_id
        );
        self.iter_all(key_range.clone(), self.epoch(), read_options)
            .await
    }

    async fn rev_iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> StorageResult<Self::RevIter<'_>> {
        let (l_vnode_inclusive, r_vnode_exclusive) = vnode_range(&key_range);
        assert_eq!(
            r_vnode_exclusive - l_vnode_inclusive,
            1,
            "read range {:?} for table {} iter contains more than one vnode",
            key_range,
            self.table_id
        );
        self.rev_iter_all(key_range.clone(), self.epoch(), read_options)
            .await
    }

    fn new_flushed_snapshot_reader(&self) -> Self::FlushedSnapshotReader {
        assert_eq!(
            self.table_option.retention_seconds, None,
            "flushed snapshot reader should not work with table {} with ttl",
            self.table_id
        );
        self.new_flushed_snapshot_reader_inner(MAX_EPOCH)
    }

    fn get_table_watermark(&self, vnode: VirtualNode) -> Option<Bytes> {
        self.read_version.read().latest_watermark(vnode)
    }

    fn insert(
        &mut self,
        key: TableKey<Bytes>,
        new_val: Bytes,
        old_val: Option<Bytes>,
    ) -> StorageResult<()> {
        match old_val {
            None => self.mem_table.insert(key, new_val)?,
            Some(old_val) => self.mem_table.update(key, old_val, new_val)?,
        };

        Ok(())
    }

    fn delete(&mut self, key: TableKey<Bytes>, old_val: Bytes) -> StorageResult<()> {
        self.mem_table.delete(key, old_val)?;

        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) -> StorageResult<Arc<Bitmap>> {
        self.update_vnode_bitmap_impl(vnodes).await
    }
}

impl StateStoreWriteEpochControl for LocalHummockStorage {
    async fn flush(&mut self) -> StorageResult<usize> {
        let buffer = self.mem_table.drain().into_parts();
        let mut kv_pairs = Vec::with_capacity(buffer.len());
        let mut old_values = if self.is_flush_old_value() {
            Some(Vec::with_capacity(buffer.len()))
        } else {
            None
        };
        let sanity_check_flushed_snapshot_reader = if sanity_check_enabled() {
            Some(self.new_flushed_snapshot_reader_inner(self.epoch()))
        } else {
            None
        };
        for (key, key_op) in buffer {
            match key_op {
                // Currently, some executors do not strictly comply with these semantics. As
                // a workaround you may call disable the check by initializing the
                // state store with `is_consistent_op=false`.
                KeyOp::Insert(value) => {
                    if let Some(sanity_check_reader) = &sanity_check_flushed_snapshot_reader {
                        do_insert_sanity_check(
                            self.table_id,
                            &key,
                            &value,
                            sanity_check_reader,
                            &self.op_consistency_level,
                        )
                        .await?;
                    }
                    kv_pairs.push((key, SharedBufferValue::Insert(value)));
                    if let Some(old_values) = &mut old_values {
                        old_values.push(Bytes::new());
                    }
                }
                KeyOp::Delete(old_value) => {
                    if let Some(sanity_check_reader) = &sanity_check_flushed_snapshot_reader {
                        do_delete_sanity_check(
                            self.table_id,
                            &key,
                            &old_value,
                            sanity_check_reader,
                            &self.op_consistency_level,
                        )
                        .await?;
                    }
                    kv_pairs.push((key, SharedBufferValue::Delete));
                    if let Some(old_values) = &mut old_values {
                        old_values.push(old_value);
                    }
                }
                KeyOp::Update((old_value, new_value)) => {
                    if let Some(sanity_check_reader) = &sanity_check_flushed_snapshot_reader {
                        do_update_sanity_check(
                            self.table_id,
                            &key,
                            &old_value,
                            &new_value,
                            sanity_check_reader,
                            &self.op_consistency_level,
                        )
                        .await?;
                    }
                    kv_pairs.push((key, SharedBufferValue::Update(new_value)));
                    if let Some(old_values) = &mut old_values {
                        old_values.push(old_value);
                    }
                }
            }
        }
        self.flush_inner(kv_pairs, old_values).await
    }

    async fn try_flush(&mut self) -> StorageResult<()> {
        if self.mem_table_spill_threshold != 0
            && self.mem_table.kv_size.size() > self.mem_table_spill_threshold
        {
            if self.spill_offset < MAX_SPILL_TIMES {
                self.flush().await?;
                self.table_memory_metrics.mem_table_spill_counts.inc();
            } else {
                tracing::warn!("No mem table spill occurs, the gap epoch exceeds available range.");
            }
        }

        Ok(())
    }

    async fn init(&mut self, options: InitOptions) -> StorageResult<()> {
        let epoch = options.epoch;
        wait_for_epoch(&self.version_update_notifier_tx, epoch.prev, self.table_id).await?;
        assert_eq!(
            self.epoch.replace(epoch),
            None,
            "local state store of table id {:?} is init for more than once",
            self.table_id
        );
        self.read_version.write().init();
        if !self.is_replicated {
            self.event_sender
                .send(HummockEvent::InitEpoch {
                    instance_id: self.instance_id(),
                    init_epoch: options.epoch.curr,
                })
                .map_err(|_| {
                    HummockError::other("failed to send InitEpoch. maybe shutting down")
                })?;
        }
        Ok(())
    }

    fn seal_current_epoch(&mut self, next_epoch: u64, mut opts: SealCurrentEpochOptions) {
        assert!(!self.mem_table.is_dirty());
        if !self.is_replicated {
            if self.upload_on_flush {
                debug_assert_eq!(self.read_version.write().pending_imm_size(), 0);
            } else {
                let pending_imms = self.read_version.write().start_upload_pending_imms();
                if !pending_imms.is_empty()
                    && self
                        .event_sender
                        .send(HummockEvent::ImmToUploader {
                            instance_id: self.instance_id(),
                            imms: pending_imms,
                        })
                        .is_err()
                {
                    warn!("failed to send ImmToUploader during seal. maybe shutting down");
                }
            }
        }

        if let Some(new_level) = &opts.switch_op_consistency_level {
            self.mem_table.op_consistency_level.update(new_level);
            self.op_consistency_level.update(new_level);
        }
        let epoch = self
            .epoch
            .as_mut()
            .expect("should have init epoch before seal the first epoch");
        let prev_epoch = epoch.curr;
        epoch.prev = prev_epoch;
        epoch.curr = next_epoch;
        self.spill_offset = 0;
        assert!(
            next_epoch > prev_epoch,
            "new epoch {} should be greater than current epoch: {}",
            next_epoch,
            prev_epoch
        );

        if let Some((direction, watermarks, watermark_type)) = &mut opts.table_watermarks {
            let mut read_version = self.read_version.write();
            read_version.filter_regress_watermarks(watermarks);
            if !watermarks.is_empty() {
                read_version.update(VersionUpdate::NewTableWatermark {
                    direction: *direction,
                    epoch: prev_epoch,
                    vnode_watermarks: watermarks.clone(),
                    watermark_type: *watermark_type,
                });
            }
        }

        if !self.is_replicated
            && self
                .event_sender
                .send(HummockEvent::LocalSealEpoch {
                    instance_id: self.instance_id(),
                    next_epoch,
                    opts,
                })
                .is_err()
        {
            warn!("failed to send LocalSealEpoch. maybe shutting down");
        }
    }
}

impl LocalHummockStorage {
    async fn update_vnode_bitmap_impl(
        &mut self,
        vnodes: Arc<Bitmap>,
    ) -> StorageResult<Arc<Bitmap>> {
        wait_for_epoch(
            &self.version_update_notifier_tx,
            self.epoch.expect("should have init").prev,
            self.table_id,
        )
        .await?;
        assert!(!self.mem_table.is_dirty());
        let mut read_version = self.read_version.write();
        assert!(
            read_version.staging().is_empty(),
            "There is uncommitted staging data in read version table_id {:?} instance_id {:?} on vnode bitmap update",
            self.table_id(),
            self.instance_id()
        );
        Ok(read_version.update_vnode_bitmap(vnodes))
    }

    fn new_flushed_snapshot_reader_inner(
        &self,
        epoch: HummockEpoch,
    ) -> LocalHummockFlushedSnapshotReader {
        LocalHummockFlushedSnapshotReader {
            table_id: self.table_id,
            table_option: self.table_option,
            read_version: self.read_version.clone(),
            hummock_version_reader: self.hummock_version_reader.clone(),
            epoch,
        }
    }

    async fn flush_inner(
        &mut self,
        sorted_items: Vec<SharedBufferItem>,
        old_values: Option<Vec<Bytes>>,
    ) -> StorageResult<usize> {
        let epoch = self.epoch();
        let table_id = self.table_id;

        self.table_memory_metrics
            .write_batch_tuple_counts
            .inc_by(sorted_items.len() as _);
        let timer = self.table_memory_metrics.write_batch_duration.start_timer();

        let imm_size = if !sorted_items.is_empty() {
            let (size, old_value_size) =
                SharedBufferBatch::measure_batch_size(&sorted_items, old_values.as_deref());

            let old_values = old_values.map(|old_values| {
                SharedBufferBatchOldValues::new(
                    old_values,
                    old_value_size,
                    self.table_memory_metrics.old_value_size.clone(),
                )
            });

            let instance_id = self.instance_guard.instance_id;
            let imm = SharedBufferBatch::build_shared_buffer_batch(
                epoch,
                self.spill_offset,
                sorted_items,
                old_values,
                size,
                table_id,
                self.table_memory_metrics.clone(),
            );
            self.spill_offset += 1;

            if self.is_replicated {
                // A replicated imm cannot be uploaded independently: it is discarded when the
                // source table's committed epoch catches up. It therefore bypasses the regular
                // write and memory limiters. Keep this memory visible through
                // `state_store_replicated_imm_size`, especially for operators that maintain one
                // full replica per actor.
                self.read_version.write().add_replicated_imm(imm);
            } else {
                self.write_limiter.wait_permission(self.table_id).await;
                let limiter = &self.memory_limiter;
                let (tracker, fast_required_memory) = match limiter.try_require_memory(size as u64)
                {
                    Some(tracker) => (tracker, true),
                    None => {
                        warn!(
                            "blocked at requiring memory: {}, current {}",
                            size,
                            limiter.get_memory_usage()
                        );
                        self.event_sender
                            .send(HummockEvent::BufferMayFlush)
                            .expect("should be able to send");
                        let tracker = limiter
                            .require_memory(size as u64)
                            .instrument_await("hummock_require_memory".verbose())
                            .await;
                        warn!(
                            "successfully requiring memory: {}, current {}",
                            size,
                            limiter.get_memory_usage()
                        );
                        (tracker, false)
                    }
                };
                let mut read_version = self.read_version.write();
                read_version.add_pending_imm(imm, tracker);
                if self.upload_on_flush
                    || read_version.pending_imm_size() >= self.mem_table_spill_threshold
                    || !fast_required_memory
                {
                    let imms = read_version.start_upload_pending_imms();
                    self.event_sender
                        .send(HummockEvent::ImmToUploader { instance_id, imms })
                        .map_err(|_| {
                            HummockError::other(
                                "failed to send imm to uploader. maybe shutting down",
                            )
                        })?;
                }
            }

            size
        } else {
            0
        };

        timer.observe_duration();

        self.table_memory_metrics
            .write_batch_size
            .observe(imm_size as _);
        Ok(imm_size)
    }
}

impl LocalHummockStorage {
    pub fn new(
        instance_guard: LocalInstanceGuard,
        read_version: HummockReadVersionRef,
        hummock_version_reader: HummockVersionReader,
        event_sender: HummockEventSender,
        memory_limiter: Arc<MemoryLimiter>,
        write_limiter: WriteLimiterRef,
        option: NewLocalOptions,
        version_update_notifier_tx: Arc<tokio::sync::watch::Sender<PinnedVersion>>,
        mem_table_spill_threshold: usize,
    ) -> Self {
        let table_memory_metrics = Arc::new(TableMemoryMetrics::new(
            hummock_version_reader.stats(),
            option.table_id,
            option.fragment_id,
            option.is_replicated,
        ));
        Self {
            mem_table: MemTable::new(option.table_id, option.op_consistency_level.clone()),
            spill_offset: 0,
            epoch: None,
            table_id: option.table_id,
            op_consistency_level: option.op_consistency_level,
            table_option: option.table_option,
            is_replicated: option.is_replicated,
            instance_guard,
            read_version,
            event_sender,
            memory_limiter,
            hummock_version_reader,
            table_memory_metrics,
            write_limiter,
            version_update_notifier_tx,
            mem_table_spill_threshold,
            upload_on_flush: option.upload_on_flush,
        }
    }

    /// See `HummockReadVersion::update` for more details.
    pub fn read_version(&self) -> HummockReadVersionRef {
        self.read_version.clone()
    }

    pub fn table_id(&self) -> TableId {
        self.instance_guard.table_id
    }

    pub fn instance_id(&self) -> u64 {
        self.instance_guard.instance_id
    }

    fn is_flush_old_value(&self) -> bool {
        matches!(
            &self.op_consistency_level,
            OpConsistencyLevel::ConsistentOldValue {
                is_log_store: true,
                ..
            }
        )
    }
}

pub type StagingDataIterator = MergeIterator<
    HummockIteratorUnion<Forward, SharedBufferBatchIterator<Forward>, SstableIterator>,
>;
pub type StagingDataRevIterator = MergeIterator<
    HummockIteratorUnion<Backward, SharedBufferBatchIterator<Backward>, BackwardSstableIterator>,
>;
pub type HummockStorageIteratorPayloadInner<'a> = MergeIterator<
    HummockIteratorUnion<
        Forward,
        StagingDataIterator,
        SstableIterator,
        ConcatIteratorInner<SstableIterator>,
        MemTableHummockIterator<'a>,
    >,
>;

pub type StorageRevIteratorPayloadInner<'a> = MergeIterator<
    HummockIteratorUnion<
        Backward,
        StagingDataRevIterator,
        BackwardSstableIterator,
        ConcatIteratorInner<BackwardSstableIterator>,
        MemTableHummockRevIterator<'a>,
    >,
>;

pub type HummockStorageIterator = HummockStorageIteratorInner<'static>;
pub type HummockStorageRevIterator = HummockStorageRevIteratorInner<'static>;
pub type LocalHummockStorageIterator<'a> = HummockStorageIteratorInner<'a>;
pub type LocalHummockStorageRevIterator<'a> = HummockStorageRevIteratorInner<'a>;

pub struct HummockStorageIteratorInner<'a> {
    inner: UserIterator<HummockStorageIteratorPayloadInner<'a>>,
    initial_read: bool,
    stats_guard: IterLocalMetricsGuard,
}

impl StateStoreIter for HummockStorageIteratorInner<'_> {
    async fn try_next(&mut self) -> StorageResult<Option<StateStoreKeyedRowRef<'_>>> {
        let iter = &mut self.inner;
        if !self.initial_read {
            self.initial_read = true;
        } else {
            iter.next().await?;
        }

        if iter.is_valid() {
            Ok(Some((iter.key(), iter.value())))
        } else {
            Ok(None)
        }
    }
}

impl<'a> HummockStorageIteratorInner<'a> {
    pub fn new(
        inner: UserIterator<HummockStorageIteratorPayloadInner<'a>>,
        metrics: Arc<HummockStateStoreMetrics>,
        table_id: TableId,
        mut local_stats: StoreLocalStatistic,
    ) -> Self {
        local_stats.found_key = inner.is_valid();
        local_stats.sub_iter_count = local_stats.staging_imm_iter_count
            + local_stats.staging_sst_iter_count
            + local_stats.overlapping_iter_count
            + local_stats.non_overlapping_iter_count;
        Self {
            inner,
            initial_read: false,
            stats_guard: IterLocalMetricsGuard::new(metrics, table_id, local_stats),
        }
    }
}

impl Drop for HummockStorageIteratorInner<'_> {
    fn drop(&mut self) {
        self.inner
            .collect_local_statistic(&mut self.stats_guard.local_stats);
    }
}

#[derive(Default)]
pub struct ForwardIteratorFactory {
    non_overlapping_iters: Vec<ConcatIteratorInner<SstableIterator>>,
    overlapping_iters: Vec<SstableIterator>,
    staging_iters:
        Vec<HummockIteratorUnion<Forward, SharedBufferBatchIterator<Forward>, SstableIterator>>,
}

impl ForwardIteratorFactory {
    pub fn build(
        self,
        mem_table: Option<MemTableHummockIterator<'_>>,
    ) -> HummockStorageIteratorPayloadInner<'_> {
        // 3. build user_iterator
        let staging_iter = StagingDataIterator::new(self.staging_iters);
        MergeIterator::new(
            once(HummockIteratorUnion::First(staging_iter))
                .chain(
                    self.overlapping_iters
                        .into_iter()
                        .map(HummockIteratorUnion::Second),
                )
                .chain(
                    self.non_overlapping_iters
                        .into_iter()
                        .map(HummockIteratorUnion::Third),
                )
                .chain(mem_table.into_iter().map(HummockIteratorUnion::Fourth)),
        )
    }
}

pub struct HummockStorageRevIteratorInner<'a> {
    inner: BackwardUserIterator<StorageRevIteratorPayloadInner<'a>>,
    initial_read: bool,
    stats_guard: IterLocalMetricsGuard,
}

impl StateStoreIter for HummockStorageRevIteratorInner<'_> {
    async fn try_next(&mut self) -> StorageResult<Option<StateStoreKeyedRowRef<'_>>> {
        let iter = &mut self.inner;
        if !self.initial_read {
            self.initial_read = true;
        } else {
            iter.next().await?;
        }

        if iter.is_valid() {
            Ok(Some((iter.key(), iter.value())))
        } else {
            Ok(None)
        }
    }
}

impl<'a> HummockStorageRevIteratorInner<'a> {
    pub fn new(
        inner: BackwardUserIterator<StorageRevIteratorPayloadInner<'a>>,
        metrics: Arc<HummockStateStoreMetrics>,
        table_id: TableId,
        mut local_stats: StoreLocalStatistic,
    ) -> Self {
        local_stats.found_key = inner.is_valid();
        local_stats.sub_iter_count = local_stats.staging_imm_iter_count
            + local_stats.staging_sst_iter_count
            + local_stats.overlapping_iter_count
            + local_stats.non_overlapping_iter_count;
        Self {
            inner,
            initial_read: false,
            stats_guard: IterLocalMetricsGuard::new(metrics, table_id, local_stats),
        }
    }
}

impl Drop for HummockStorageRevIteratorInner<'_> {
    fn drop(&mut self) {
        self.inner
            .collect_local_statistic(&mut self.stats_guard.local_stats);
    }
}

impl IteratorFactory for ForwardIteratorFactory {
    type Direction = Forward;
    type SstableIteratorType = SstableIterator;

    fn add_batch_iter(&mut self, batch: SharedBufferBatch) {
        self.staging_iters
            .push(HummockIteratorUnion::First(batch.into_forward_iter()));
    }

    fn add_staging_sst_iter(&mut self, iter: Self::SstableIteratorType) {
        self.staging_iters.push(HummockIteratorUnion::Second(iter));
    }

    fn add_overlapping_sst_iter(&mut self, iter: Self::SstableIteratorType) {
        self.overlapping_iters.push(iter);
    }

    fn add_concat_sst_iter(
        &mut self,
        tables: Vec<SstableInfo>,
        sstable_store: SstableStoreRef,
        read_options: Arc<SstableIteratorReadOptions>,
    ) {
        self.non_overlapping_iters
            .push(ConcatIteratorInner::<Self::SstableIteratorType>::new(
                tables,
                sstable_store,
                read_options,
            ));
    }
}

#[derive(Default)]
pub struct BackwardIteratorFactory {
    non_overlapping_iters: Vec<ConcatIteratorInner<BackwardSstableIterator>>,
    overlapping_iters: Vec<BackwardSstableIterator>,
    staging_iters: Vec<
        HummockIteratorUnion<
            Backward,
            SharedBufferBatchIterator<Backward>,
            BackwardSstableIterator,
        >,
    >,
}

impl BackwardIteratorFactory {
    pub fn build(
        self,
        mem_table: Option<MemTableHummockRevIterator<'_>>,
    ) -> StorageRevIteratorPayloadInner<'_> {
        // 3. build user_iterator
        let staging_iter = StagingDataRevIterator::new(self.staging_iters);
        MergeIterator::new(
            once(HummockIteratorUnion::First(staging_iter))
                .chain(
                    self.overlapping_iters
                        .into_iter()
                        .map(HummockIteratorUnion::Second),
                )
                .chain(
                    self.non_overlapping_iters
                        .into_iter()
                        .map(HummockIteratorUnion::Third),
                )
                .chain(mem_table.into_iter().map(HummockIteratorUnion::Fourth)),
        )
    }
}

impl IteratorFactory for BackwardIteratorFactory {
    type Direction = Backward;
    type SstableIteratorType = BackwardSstableIterator;

    fn add_batch_iter(&mut self, batch: SharedBufferBatch) {
        self.staging_iters
            .push(HummockIteratorUnion::First(batch.into_backward_iter()));
    }

    fn add_staging_sst_iter(&mut self, iter: Self::SstableIteratorType) {
        self.staging_iters.push(HummockIteratorUnion::Second(iter));
    }

    fn add_overlapping_sst_iter(&mut self, iter: Self::SstableIteratorType) {
        self.overlapping_iters.push(iter);
    }

    fn add_concat_sst_iter(
        &mut self,
        mut tables: Vec<SstableInfo>,
        sstable_store: SstableStoreRef,
        read_options: Arc<SstableIteratorReadOptions>,
    ) {
        tables.reverse();
        self.non_overlapping_iters
            .push(ConcatIteratorInner::<Self::SstableIteratorType>::new(
                tables,
                sstable_store,
                read_options,
            ));
    }
}
