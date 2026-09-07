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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::vec_deque::VecDeque;
use std::ops::Bound::{self};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::future::try_join_all;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::array::VectorRef;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::MAX_SPILL_TIMES;
use risingwave_hummock_sdk::key::{
    FullKey, TableKey, TableKeyRange, UserKey, bound_table_key_range,
};
use risingwave_hummock_sdk::key_range::KeyRangeCommon;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_watermark::{
    TableWatermarksIndex, VnodeWatermark, WatermarkDirection, WatermarkSerdeType,
};
use risingwave_hummock_sdk::vector_index::VectorIndexImpl;
use risingwave_hummock_sdk::{EpochWithGap, HummockEpoch, LocalSstableInfo};
use risingwave_pb::hummock::LevelType;
use sync_point::sync_point;
use tracing::warn;

use crate::error::StorageResult;
use crate::hummock::event_handler::LocalInstanceId;
use crate::hummock::iterator::change_log::ChangeLogIterator;
use crate::hummock::iterator::{
    BackwardUserIterator, HummockIterator, IteratorFactory, MergeIterator, UserIterator,
};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::sstable::{SstableIteratorReadOptions, SstableIteratorType};
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::table_change_log_manager::TableChangeLogManager;
use crate::hummock::utils::{
    MemoryTracker, filter_single_sst, prune_nonoverlapping_ssts, prune_overlapping_ssts,
    range_overlap, search_sst_idx,
};
use crate::hummock::vector::file::{FileVectorStore, FileVectorStoreCtx};
use crate::hummock::vector::monitor::{VectorStoreCacheStats, report_hnsw_stat};
use crate::hummock::{
    BackwardIteratorFactory, ForwardIteratorFactory, HummockError, HummockResult,
    HummockStorageIterator, HummockStorageIteratorInner, HummockStorageRevIteratorInner,
    ReadVersionTuple, Sstable, SstableIterator, get_from_batch, get_from_sstable_info,
    hit_sstable_filter,
};
use crate::mem_table::{
    ImmId, ImmutableMemtable, MemTableHummockIterator, MemTableHummockRevIterator,
};
use crate::monitor::{
    GetLocalMetricsGuard, HummockStateStoreMetrics, IterLocalMetricsGuard, StoreLocalStatistic,
};
use crate::store::{
    OnNearestItemFn, ReadLogOptions, ReadOptions, VectorNearestOptions, gen_min_epoch,
};
use crate::vector::hnsw::nearest;
use crate::vector::{MeasureDistanceBuilder, NearestBuilder};

pub type CommittedVersion = PinnedVersion;

/// Data not committed to Hummock. There are two types of staging data:
/// - Immutable memtable: data that has been written into local state store but not persisted.
/// - Uncommitted SST: data that has been uploaded to persistent storage but not committed to
///   hummock version.

#[derive(Clone, Debug, PartialEq)]
pub struct StagingSstableInfo {
    // newer data comes first
    sstable_infos: Vec<LocalSstableInfo>,
    old_value_sstable_infos: Vec<LocalSstableInfo>,
    /// Epochs whose data are included in the Sstable. The newer epoch comes first.
    /// The field must not be empty.
    epochs: Vec<HummockEpoch>,
    // newer data at the front
    imm_ids: HashMap<LocalInstanceId, Vec<ImmId>>,
    imm_size: usize,
}

impl StagingSstableInfo {
    pub fn new(
        sstable_infos: Vec<LocalSstableInfo>,
        old_value_sstable_infos: Vec<LocalSstableInfo>,
        epochs: Vec<HummockEpoch>,
        imm_ids: HashMap<LocalInstanceId, Vec<ImmId>>,
        imm_size: usize,
    ) -> Self {
        // the epochs are sorted from higher epoch to lower epoch
        assert!(epochs.is_sorted_by(|epoch1, epoch2| epoch2 <= epoch1));
        Self {
            sstable_infos,
            old_value_sstable_infos,
            epochs,
            imm_ids,
            imm_size,
        }
    }

    pub fn sstable_infos(&self) -> &Vec<LocalSstableInfo> {
        &self.sstable_infos
    }

    pub fn old_value_sstable_infos(&self) -> &Vec<LocalSstableInfo> {
        &self.old_value_sstable_infos
    }

    pub fn imm_size(&self) -> usize {
        self.imm_size
    }

    pub fn epochs(&self) -> &Vec<HummockEpoch> {
        &self.epochs
    }

    pub fn imm_ids(&self) -> &HashMap<LocalInstanceId, Vec<ImmId>> {
        &self.imm_ids
    }
}

pub enum VersionUpdate {
    Sst(Arc<StagingSstableInfo>),
    CommittedSnapshot(CommittedVersion),
    NewTableWatermark {
        direction: WatermarkDirection,
        epoch: HummockEpoch,
        vnode_watermarks: Vec<VnodeWatermark>,
        watermark_type: WatermarkSerdeType,
    },
}

pub struct StagingVersion {
    pending_imm_size: usize,
    /// It contains the imms added but not sent to the uploader of hummock event handler.
    /// It is non-empty only when `upload_on_flush` is false.
    ///
    /// It will be sent to the uploader when `pending_imm_size` exceed threshold or on `seal_current_epoch`.
    ///
    /// newer data comes last
    pub pending_imms: Vec<(ImmutableMemtable, MemoryTracker)>,
    /// It contains the imms already sent to uploader of hummock event handler.
    /// Note: Currently, building imm and writing to staging version is not atomic, and therefore
    /// imm of smaller batch id may be added later than one with greater batch id
    ///
    /// Newer data comes first.
    pub uploading_imms: VecDeque<ImmutableMemtable>,

    // newer data comes first
    pub sst: VecDeque<Arc<StagingSstableInfo>>,
}

impl StagingVersion {
    /// Get the overlapping `imm`s and `sst`s that overlap respectively with `table_key_range` and
    /// the user key range derived from `table_id`, `epoch` and `table_key_range`.
    pub fn prune_overlap<'a>(
        &'a self,
        max_epoch_inclusive: HummockEpoch,
        table_id: TableId,
        table_key_range: &'a TableKeyRange,
    ) -> (
        impl Iterator<Item = &'a ImmutableMemtable> + 'a,
        impl Iterator<Item = &'a SstableInfo> + 'a,
    ) {
        let (left, right) = table_key_range;
        let left = left.as_ref().map(|key| TableKey(key.0.as_ref()));
        let right = right.as_ref().map(|key| TableKey(key.0.as_ref()));
        let overlapped_imms = self
            .pending_imms
            .iter()
            .map(|(imm, _)| imm)
            .rev() // rev to let newer imm come first
            .chain(self.uploading_imms.iter())
            .filter(move |imm| {
                // retain imm which is overlapped with (min_epoch_exclusive, max_epoch_inclusive]
                imm.epoch() <= max_epoch_inclusive
                    && imm.table_id == table_id
                    && range_overlap(
                        &(left, right),
                        &imm.start_table_key(),
                        Bound::Included(&imm.end_table_key()),
                    )
            });

        // TODO: Remove duplicate sst based on sst id
        let overlapped_ssts = self
            .sst
            .iter()
            .filter(move |staging_sst| {
                let sst_max_epoch = *staging_sst.epochs.last().expect("epochs not empty");
                sst_max_epoch <= max_epoch_inclusive
            })
            .flat_map(move |staging_sst| {
                // TODO: sstable info should be concat-able after each streaming table owns a read
                // version. May use concat sstable iter instead in some cases.
                staging_sst
                    .sstable_infos
                    .iter()
                    .map(|sstable| &sstable.sst_info)
                    .filter(move |sstable: &&SstableInfo| {
                        filter_single_sst(sstable, table_id, table_key_range)
                    })
            });
        (overlapped_imms, overlapped_ssts)
    }

    pub fn is_empty(&self) -> bool {
        self.pending_imms.is_empty() && self.uploading_imms.is_empty() && self.sst.is_empty()
    }
}

/// A container of information required for reading from hummock.
pub struct HummockReadVersion {
    table_id: TableId,
    instance_id: LocalInstanceId,

    is_initialized: bool,

    /// Local version for staging data.
    staging: StagingVersion,

    /// Remote version for committed data.
    committed: CommittedVersion,

    /// Indicate if this is replicated. If it is, we should ignore it during
    /// global state store read, to avoid duplicated results.
    /// Otherwise for local state store, it is fine, see we will see the
    /// `ReadVersion` just for that local state store.
    is_replicated: bool,

    table_watermarks: Option<TableWatermarksIndex>,

    // Vnode bitmap corresponding to the read version
    // It will be initialized after local state store init
    vnodes: Arc<Bitmap>,
}

impl HummockReadVersion {
    pub fn new_with_replication_option(
        table_id: TableId,
        instance_id: LocalInstanceId,
        committed_version: CommittedVersion,
        is_replicated: bool,
        vnodes: Arc<Bitmap>,
    ) -> Self {
        // before build `HummockReadVersion`, we need to get the a initial version which obtained
        // from meta. want this initialization after version is initialized (now with
        // notification), so add a assert condition to guarantee correct initialization order
        assert!(committed_version.is_valid());
        Self {
            table_id,
            instance_id,
            table_watermarks: {
                match committed_version.table_watermarks.get(&table_id) {
                    Some(table_watermarks) => Some(TableWatermarksIndex::new_committed(
                        table_watermarks.clone(),
                        committed_version
                            .state_table_info
                            .info()
                            .get(&table_id)
                            .expect("should exist")
                            .committed_epoch,
                        table_watermarks.watermark_type,
                    )),
                    None => None,
                }
            },
            staging: StagingVersion {
                pending_imm_size: 0,
                pending_imms: Vec::default(),
                uploading_imms: VecDeque::default(),
                sst: VecDeque::default(),
            },

            committed: committed_version,

            is_replicated,
            vnodes,
            is_initialized: false,
        }
    }

    pub fn new(
        table_id: TableId,
        instance_id: LocalInstanceId,
        committed_version: CommittedVersion,
        vnodes: Arc<Bitmap>,
    ) -> Self {
        Self::new_with_replication_option(table_id, instance_id, committed_version, false, vnodes)
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn init(&mut self) {
        assert!(!self.is_initialized);
        self.is_initialized = true;
    }

    pub fn add_pending_imm(&mut self, imm: ImmutableMemtable, tracker: MemoryTracker) {
        assert!(self.is_initialized);
        assert!(!self.is_replicated);
        if let Some(item) = self
            .staging
            .pending_imms
            .last()
            .map(|(imm, _)| imm)
            .or_else(|| self.staging.uploading_imms.front())
        {
            // check batch_id order from newest to old
            assert!(item.batch_id() < imm.batch_id());
        }

        self.staging.pending_imm_size += imm.size();
        self.staging.pending_imms.push((imm, tracker));
    }

    pub fn add_replicated_imm(&mut self, imm: ImmutableMemtable) {
        assert!(self.is_initialized);
        assert!(self.is_replicated);
        assert!(self.staging.pending_imms.is_empty());
        if let Some(item) = self.staging.uploading_imms.front() {
            // check batch_id order from newest to old
            assert!(item.batch_id() < imm.batch_id());
        }
        self.staging.uploading_imms.push_front(imm);
    }

    pub fn pending_imm_size(&self) -> usize {
        self.staging.pending_imm_size
    }

    pub fn start_upload_pending_imms(&mut self) -> Vec<(ImmutableMemtable, MemoryTracker)> {
        assert!(self.is_initialized);
        assert!(!self.is_replicated);
        let pending_imms = std::mem::take(&mut self.staging.pending_imms);
        for (imm, _) in &pending_imms {
            self.staging.uploading_imms.push_front(imm.clone());
        }
        self.staging.pending_imm_size = 0;
        pending_imms
    }

    /// Updates the read version with `VersionUpdate`.
    /// There will be three data types to be processed
    /// `VersionUpdate::Staging`
    ///     - `StagingData::ImmMem` -> Insert into memory's `staging_imm`
    ///     - `StagingData::Sst` -> Update the sst to memory's `staging_sst` and remove the
    ///       corresponding `staging_imms` according to the `batch_id`
    /// `VersionUpdate::CommittedDelta` -> Unimplemented yet
    /// `VersionUpdate::CommittedSnapshot` -> Update `committed_version` , and clean up related
    /// `staging_sst` and `staging_imm` in memory according to epoch
    pub fn update(&mut self, info: VersionUpdate) {
        match info {
            VersionUpdate::Sst(staging_sst_ref) => {
                {
                    assert!(!self.is_replicated);
                    let Some(imms) = staging_sst_ref.imm_ids.get(&self.instance_id) else {
                        warn!(
                            instance_id = self.instance_id,
                            "no related imm in sst input"
                        );
                        return;
                    };

                    // old data comes first
                    for imm_id in imms.iter().rev() {
                        let check_err = match self.staging.uploading_imms.pop_back() {
                            None => Some("empty".to_owned()),
                            Some(prev_imm_id) => {
                                if prev_imm_id.batch_id() == *imm_id {
                                    None
                                } else {
                                    Some(format!(
                                        "miss match id {} {}",
                                        prev_imm_id.batch_id(),
                                        *imm_id
                                    ))
                                }
                            }
                        };
                        assert!(
                            check_err.is_none(),
                            "should be valid staging_sst.size {},
                                    staging_sst.imm_ids {:?},
                                    staging_sst.epochs {:?},
                                    local_pending_imm_ids {:?},
                                    local_uploading_imm_ids {:?},
                                    instance_id {}
                                    check_err {:?}",
                            staging_sst_ref.imm_size,
                            staging_sst_ref.imm_ids,
                            staging_sst_ref.epochs,
                            self.staging
                                .pending_imms
                                .iter()
                                .map(|(imm, _)| imm.batch_id())
                                .collect_vec(),
                            self.staging
                                .uploading_imms
                                .iter()
                                .map(|imm| imm.batch_id())
                                .collect_vec(),
                            self.instance_id,
                            check_err
                        );
                    }

                    self.staging.sst.push_front(staging_sst_ref);
                }
            }

            VersionUpdate::CommittedSnapshot(committed_version) => {
                if let Some(info) = committed_version
                    .state_table_info
                    .info()
                    .get(&self.table_id)
                {
                    let committed_epoch = info.committed_epoch;
                    if self.is_replicated {
                        self.staging
                            .uploading_imms
                            .retain(|imm| imm.epoch() > committed_epoch);
                        self.staging
                            .pending_imms
                            .retain(|(imm, _)| imm.epoch() > committed_epoch);
                    } else {
                        self.staging
                            .pending_imms
                            .iter()
                            .map(|(imm, _)| imm)
                            .chain(self.staging.uploading_imms.iter())
                            .for_each(|imm| {
                                assert!(
                                    imm.epoch() > committed_epoch,
                                    "imm of table {} min_epoch {} should be greater than committed_epoch {}",
                                    imm.table_id,
                                    imm.epoch(),
                                    committed_epoch
                                )
                            });
                    }

                    self.staging.sst.retain(|sst| {
                        sst.epochs.first().expect("epochs not empty") > &committed_epoch
                    });

                    // check epochs.last() > MCE
                    assert!(self.staging.sst.iter().all(|sst| {
                        sst.epochs.last().expect("epochs not empty") > &committed_epoch
                    }));

                    if let Some(committed_watermarks) =
                        committed_version.table_watermarks.get(&self.table_id)
                    {
                        if let Some(watermark_index) = &mut self.table_watermarks {
                            watermark_index.apply_committed_watermarks(
                                committed_watermarks.clone(),
                                committed_epoch,
                            );
                        } else {
                            self.table_watermarks = Some(TableWatermarksIndex::new_committed(
                                committed_watermarks.clone(),
                                committed_epoch,
                                committed_watermarks.watermark_type,
                            ));
                        }
                    }
                }

                self.committed = committed_version;
            }
            VersionUpdate::NewTableWatermark {
                direction,
                epoch,
                vnode_watermarks,
                watermark_type,
            } => {
                if let Some(watermark_index) = &mut self.table_watermarks {
                    watermark_index.add_epoch_watermark(
                        epoch,
                        Arc::from(vnode_watermarks),
                        direction,
                    );
                } else {
                    self.table_watermarks = Some(TableWatermarksIndex::new(
                        direction,
                        epoch,
                        vnode_watermarks,
                        self.committed.table_committed_epoch(self.table_id),
                        watermark_type,
                    ));
                }
            }
        }
    }

    pub fn staging(&self) -> &StagingVersion {
        &self.staging
    }

    pub fn committed(&self) -> &CommittedVersion {
        &self.committed
    }

    /// We have assumption that the watermark is increasing monotonically. Therefore,
    /// here if the upper layer usage has passed an regressed watermark, we should
    /// filter out the regressed watermark. Currently the kv log store may write
    /// regressed watermark
    pub fn filter_regress_watermarks(&self, watermarks: &mut Vec<VnodeWatermark>) {
        if let Some(watermark_index) = &self.table_watermarks {
            watermark_index.filter_regress_watermarks(watermarks)
        }
    }

    pub fn latest_watermark(&self, vnode: VirtualNode) -> Option<Bytes> {
        self.table_watermarks
            .as_ref()
            .and_then(|watermark_index| watermark_index.latest_watermark(vnode))
    }

    pub fn is_initialized(&self) -> bool {
        self.is_initialized
    }

    pub fn is_replicated(&self) -> bool {
        self.is_replicated
    }

    pub fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) -> Arc<Bitmap> {
        std::mem::replace(&mut self.vnodes, vnodes)
    }

    pub fn contains(&self, vnode: VirtualNode) -> bool {
        self.vnodes.is_set(vnode.to_index())
    }

    pub fn vnodes(&self) -> Arc<Bitmap> {
        self.vnodes.clone()
    }
}

pub fn read_filter_for_version(
    epoch: HummockEpoch,
    table_id: TableId,
    mut table_key_range: TableKeyRange,
    read_version: &RwLock<HummockReadVersion>,
) -> StorageResult<(TableKeyRange, ReadVersionTuple)> {
    let read_version_guard = read_version.read();

    let committed_version = read_version_guard.committed().clone();

    if let Some(watermark) = read_version_guard.table_watermarks.as_ref() {
        watermark.rewrite_range_with_table_watermark(epoch, &mut table_key_range)
    }

    let (imm_iter, sst_iter) =
        read_version_guard
            .staging()
            .prune_overlap(epoch, table_id, &table_key_range);

    let imms = imm_iter.cloned().collect();
    let ssts = sst_iter.cloned().collect();

    Ok((table_key_range, (imms, ssts, committed_version)))
}

#[derive(Clone)]
pub struct HummockVersionReader {
    sstable_store: SstableStoreRef,

    /// Statistics
    state_store_metrics: Arc<HummockStateStoreMetrics>,
    preload_retry_times: usize,
}

/// use `HummockVersionReader` to reuse `get` and `iter` implement for both `batch_query` and
/// `streaming_query`
impl HummockVersionReader {
    pub fn new(
        sstable_store: SstableStoreRef,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
        preload_retry_times: usize,
    ) -> Self {
        Self {
            sstable_store,
            state_store_metrics,
            preload_retry_times,
        }
    }

    pub fn stats(&self) -> &Arc<HummockStateStoreMetrics> {
        &self.state_store_metrics
    }
}

const SLOW_ITER_FETCH_META_DURATION_SECOND: f64 = 5.0;

impl HummockVersionReader {
    fn skip_get_by_vnode_user_key_range(
        sstable_info: &SstableInfo,
        vnode: VirtualNode,
        user_key: UserKey<&[u8]>,
        local_stats: &mut StoreLocalStatistic,
    ) -> bool {
        if let Some(vnode_statistics) = &sstable_info.vnode_statistics {
            // Only skip if vnode-statistics exists and key is out of range.
            // If vnode-statistics not found, it may be due to incomplete stats (reached limit).
            if let Some((vnode_min, vnode_max)) = vnode_statistics.get_vnode_user_key_range(vnode) {
                local_stats.vnode_checked_get_count += 1;
                if user_key < vnode_min.as_ref() || user_key > vnode_max.as_ref() {
                    local_stats.vnode_pruned_get_count += 1;
                    return true;
                }
            }
        }
        false
    }

    pub async fn get<'a, O>(
        &'a self,
        table_key: TableKey<Bytes>,
        epoch: u64,
        table_id: TableId,
        table_option: TableOption,
        read_options: ReadOptions,
        read_version_tuple: ReadVersionTuple,
        on_key_value_fn: impl crate::store::KeyValueFn<'a, O>,
    ) -> StorageResult<Option<O>> {
        let (imms, uncommitted_ssts, committed_version) = read_version_tuple;

        let min_epoch = gen_min_epoch(epoch, table_option.retention_seconds);
        let mut stats_guard = GetLocalMetricsGuard::new(self.state_store_metrics.clone(), table_id);
        let local_stats = &mut stats_guard.local_stats;
        local_stats.found_key = true;

        // 1. read staging data
        for imm in &imms {
            // skip imm that only holding out-of-date data
            if imm.epoch() < min_epoch {
                continue;
            }

            local_stats.staging_imm_get_count += 1;

            if let Some((data, data_epoch)) = get_from_batch(
                imm,
                TableKey(table_key.as_ref()),
                epoch,
                &read_options,
                local_stats,
            ) {
                return Ok(if data_epoch.pure_epoch() < min_epoch {
                    None
                } else {
                    data.into_user_value()
                        .map(|v| {
                            on_key_value_fn(
                                FullKey::new_with_gap_epoch(
                                    table_id,
                                    table_key.to_ref(),
                                    data_epoch,
                                ),
                                v.as_ref(),
                            )
                        })
                        .transpose()?
                });
            }
        }

        // 2. order guarantee: imm -> sst
        let dist_key_hash = read_options
            .prefix_hint
            .as_ref()
            .map(|dist_key| Sstable::hash_for_filter(dist_key.as_ref(), table_id.as_raw_id()));

        // Here epoch passed in is pure epoch, and we will seek the constructed `full_key` later.
        // Therefore, it is necessary to construct the `full_key` with `MAX_SPILL_TIMES`, otherwise, the iterator might skip keys with spill offset greater than 0.
        let full_key = FullKey::new_with_gap_epoch(
            table_id,
            TableKey(table_key.clone()),
            EpochWithGap::new(epoch, MAX_SPILL_TIMES),
        );
        let single_table_key_range = table_key.clone()..=table_key.clone();

        // prune uncommitted ssts with the keyrange
        let pruned_uncommitted_ssts =
            prune_overlapping_ssts(&uncommitted_ssts, table_id, &single_table_key_range);
        for local_sst in pruned_uncommitted_ssts {
            local_stats.staging_sst_get_count += 1;
            if let Some(iter) = get_from_sstable_info(
                self.sstable_store.clone(),
                local_sst,
                full_key.to_ref(),
                &read_options,
                dist_key_hash,
                local_stats,
            )
            .await?
            {
                debug_assert!(iter.is_valid());
                let data_epoch = iter.key().epoch_with_gap;
                return Ok(if data_epoch.pure_epoch() < min_epoch {
                    None
                } else {
                    iter.value()
                        .into_user_value()
                        .map(|v| {
                            on_key_value_fn(
                                FullKey::new_with_gap_epoch(
                                    table_id,
                                    table_key.to_ref(),
                                    data_epoch,
                                ),
                                v,
                            )
                        })
                        .transpose()?
                });
            }
        }
        // 3. read from committed_version sst file
        // Because SST meta records encoded key range,
        // the filter key needs to be encoded as well.
        assert!(committed_version.is_valid());
        for level in committed_version.levels(table_id) {
            if level.table_infos.is_empty() {
                continue;
            }

            match level.level_type {
                LevelType::Overlapping | LevelType::Unspecified => {
                    let sstable_infos = prune_overlapping_ssts(
                        &level.table_infos,
                        table_id,
                        &single_table_key_range,
                    );
                    for sstable_info in sstable_infos {
                        // filter vnode-key range that is definitely not containing the key
                        if Self::skip_get_by_vnode_user_key_range(
                            sstable_info,
                            VirtualNode::from_index(full_key.user_key.get_vnode_id()),
                            full_key.user_key.as_ref(),
                            local_stats,
                        ) {
                            continue;
                        }

                        local_stats.overlapping_get_count += 1;
                        if let Some(iter) = get_from_sstable_info(
                            self.sstable_store.clone(),
                            sstable_info,
                            full_key.to_ref(),
                            &read_options,
                            dist_key_hash,
                            local_stats,
                        )
                        .await?
                        {
                            debug_assert!(iter.is_valid());
                            let data_epoch = iter.key().epoch_with_gap;
                            return Ok(if data_epoch.pure_epoch() < min_epoch {
                                None
                            } else {
                                iter.value()
                                    .into_user_value()
                                    .map(|v| {
                                        on_key_value_fn(
                                            FullKey::new_with_gap_epoch(
                                                table_id,
                                                table_key.to_ref(),
                                                data_epoch,
                                            ),
                                            v,
                                        )
                                    })
                                    .transpose()?
                            });
                        }
                    }
                }
                LevelType::Nonoverlapping => {
                    let mut table_info_idx =
                        search_sst_idx(&level.table_infos, full_key.user_key.as_ref());
                    if table_info_idx == 0 {
                        continue;
                    }
                    table_info_idx = table_info_idx.saturating_sub(1);
                    let sstable_info = &level.table_infos[table_info_idx];

                    if sstable_info.table_ids.binary_search(&table_id).is_err() {
                        continue;
                    }

                    // Filter SSTs that definitely cannot contain the key.
                    let ord = sstable_info
                        .key_range
                        .compare_right_with_user_key(full_key.user_key.as_ref());
                    // the case that the key falls into the gap between two ssts
                    if ord == Ordering::Less {
                        sync_point!("HUMMOCK_V2::GET::SKIP_BY_NO_FILE");
                        continue;
                    }

                    // Filter vnode-key range that is definitely not containing the key.
                    if Self::skip_get_by_vnode_user_key_range(
                        sstable_info,
                        VirtualNode::from_index(full_key.user_key.get_vnode_id()),
                        full_key.user_key.as_ref(),
                        local_stats,
                    ) {
                        continue;
                    }

                    local_stats.non_overlapping_get_count += 1;
                    if let Some(iter) = get_from_sstable_info(
                        self.sstable_store.clone(),
                        sstable_info,
                        full_key.to_ref(),
                        &read_options,
                        dist_key_hash,
                        local_stats,
                    )
                    .await?
                    {
                        debug_assert!(iter.is_valid());
                        let data_epoch = iter.key().epoch_with_gap;
                        return Ok(if data_epoch.pure_epoch() < min_epoch {
                            None
                        } else {
                            iter.value()
                                .into_user_value()
                                .map(|v| {
                                    on_key_value_fn(
                                        FullKey::new_with_gap_epoch(
                                            table_id,
                                            table_key.to_ref(),
                                            data_epoch,
                                        ),
                                        v,
                                    )
                                })
                                .transpose()?
                        });
                    }
                }
            }
        }
        stats_guard.local_stats.found_key = false;
        Ok(None)
    }

    pub async fn iter(
        &self,
        table_key_range: TableKeyRange,
        epoch: u64,
        table_id: TableId,
        table_option: TableOption,
        read_options: ReadOptions,
        read_version_tuple: (Vec<ImmutableMemtable>, Vec<SstableInfo>, CommittedVersion),
    ) -> StorageResult<HummockStorageIterator> {
        self.iter_with_memtable(
            table_key_range,
            epoch,
            table_id,
            table_option,
            read_options,
            read_version_tuple,
            None,
        )
        .await
    }

    pub async fn iter_with_memtable<'b>(
        &self,
        table_key_range: TableKeyRange,
        epoch: u64,
        table_id: TableId,
        table_option: TableOption,
        read_options: ReadOptions,
        read_version_tuple: (Vec<ImmutableMemtable>, Vec<SstableInfo>, CommittedVersion),
        memtable_iter: Option<MemTableHummockIterator<'b>>,
    ) -> StorageResult<HummockStorageIteratorInner<'b>> {
        let user_key_range_ref = bound_table_key_range(table_id, &table_key_range);
        let user_key_range = (
            user_key_range_ref.0.map(|key| key.cloned()),
            user_key_range_ref.1.map(|key| key.cloned()),
        );
        let mut factory = ForwardIteratorFactory::default();
        let mut local_stats = StoreLocalStatistic::default();
        let (imms, uncommitted_ssts, committed) = read_version_tuple;
        let min_epoch = gen_min_epoch(epoch, table_option.retention_seconds);
        self.iter_inner(
            table_key_range,
            epoch,
            table_id,
            read_options,
            imms,
            uncommitted_ssts,
            &committed,
            &mut local_stats,
            &mut factory,
        )
        .await?;
        let merge_iter = factory.build(memtable_iter);
        // the epoch_range left bound for iterator read
        let mut user_iter = UserIterator::new(
            merge_iter,
            user_key_range,
            epoch,
            min_epoch,
            Some(committed),
        );
        user_iter.rewind().await?;
        Ok(HummockStorageIteratorInner::new(
            user_iter,
            self.state_store_metrics.clone(),
            table_id,
            local_stats,
        ))
    }

    pub async fn rev_iter<'b>(
        &self,
        table_key_range: TableKeyRange,
        epoch: u64,
        table_id: TableId,
        table_option: TableOption,
        read_options: ReadOptions,
        read_version_tuple: (Vec<ImmutableMemtable>, Vec<SstableInfo>, CommittedVersion),
        memtable_iter: Option<MemTableHummockRevIterator<'b>>,
    ) -> StorageResult<HummockStorageRevIteratorInner<'b>> {
        let user_key_range_ref = bound_table_key_range(table_id, &table_key_range);
        let user_key_range = (
            user_key_range_ref.0.map(|key| key.cloned()),
            user_key_range_ref.1.map(|key| key.cloned()),
        );
        let mut factory = BackwardIteratorFactory::default();
        let mut local_stats = StoreLocalStatistic::default();
        let (imms, uncommitted_ssts, committed) = read_version_tuple;
        let min_epoch = gen_min_epoch(epoch, table_option.retention_seconds);
        self.iter_inner(
            table_key_range,
            epoch,
            table_id,
            read_options,
            imms,
            uncommitted_ssts,
            &committed,
            &mut local_stats,
            &mut factory,
        )
        .await?;
        let merge_iter = factory.build(memtable_iter);
        // the epoch_range left bound for iterator read
        let mut user_iter = BackwardUserIterator::new(
            merge_iter,
            user_key_range,
            epoch,
            min_epoch,
            Some(committed),
        );
        user_iter.rewind().await?;
        Ok(HummockStorageRevIteratorInner::new(
            user_iter,
            self.state_store_metrics.clone(),
            table_id,
            local_stats,
        ))
    }

    async fn iter_inner<F: IteratorFactory>(
        &self,
        table_key_range: TableKeyRange,
        epoch: u64,
        table_id: TableId,
        read_options: ReadOptions,
        imms: Vec<ImmutableMemtable>,
        uncommitted_ssts: Vec<SstableInfo>,
        committed: &CommittedVersion,
        local_stats: &mut StoreLocalStatistic,
        factory: &mut F,
    ) -> StorageResult<()> {
        {
            fn bound_inner<T>(bound: &Bound<T>) -> Option<&T> {
                match bound {
                    Bound::Included(bound) | Bound::Excluded(bound) => Some(bound),
                    Bound::Unbounded => None,
                }
            }
            let (left, right) = &table_key_range;
            if let (Some(left), Some(right)) = (bound_inner(left), bound_inner(right))
                && right < left
            {
                if cfg!(debug_assertions) {
                    panic!("invalid iter key range: {table_id} {left:?} {right:?}")
                } else {
                    return Err(HummockError::other(format!(
                        "invalid iter key range: {table_id} {left:?} {right:?}"
                    ))
                    .into());
                }
            }
        }

        local_stats.staging_imm_iter_count = imms.len() as u64;
        for imm in imms {
            factory.add_batch_iter(imm);
        }

        // 2. build iterator from committed
        // Because SST meta records encoded key range,
        // the filter key range needs to be encoded as well.
        let user_key_range = bound_table_key_range(table_id, &table_key_range);
        let user_key_range_ref = (
            user_key_range.0.as_ref().map(UserKey::as_ref),
            user_key_range.1.as_ref().map(UserKey::as_ref),
        );
        let mut staging_sst_iter_count = 0;
        // encode once
        let filter_prefix_hash = read_options
            .prefix_hint
            .as_ref()
            .map(|hint| Sstable::hash_for_filter(hint, table_id.as_raw_id()));
        let mut sst_read_options = SstableIteratorReadOptions::from_read_options(&read_options);
        sst_read_options.scan_end_user_key = Some(user_key_range.1.map(|key| key.cloned()));
        sst_read_options.prefetch = read_options.prefetch_options.prefetch;
        if sst_read_options.prefetch {
            sst_read_options.max_preload_retry_times = self.preload_retry_times;
        }
        let sst_read_options = Arc::new(sst_read_options);
        for sstable_info in &uncommitted_ssts {
            let table_holder = self
                .sstable_store
                .sstable(sstable_info, local_stats)
                .await?;

            if let Some(prefix_hash) = filter_prefix_hash.as_ref()
                && !hit_sstable_filter(
                    &table_holder,
                    &user_key_range_ref,
                    *prefix_hash,
                    local_stats,
                )
            {
                continue;
            }

            staging_sst_iter_count += 1;
            factory.add_staging_sst_iter(F::SstableIteratorType::create(
                table_holder,
                self.sstable_store.clone(),
                sst_read_options.clone(),
                sstable_info,
            ));
        }
        local_stats.staging_sst_iter_count = staging_sst_iter_count;

        let timer = Instant::now();

        for level in committed.levels(table_id) {
            if level.table_infos.is_empty() {
                continue;
            }

            if level.level_type == LevelType::Nonoverlapping {
                let mut table_infos =
                    prune_nonoverlapping_ssts(&level.table_infos, user_key_range_ref, table_id)
                        .peekable();

                if table_infos.peek().is_none() {
                    continue;
                }
                let sstable_infos = table_infos.cloned().collect_vec();
                if sstable_infos.len() > 1 {
                    factory.add_concat_sst_iter(
                        sstable_infos,
                        self.sstable_store.clone(),
                        sst_read_options.clone(),
                    );
                    local_stats.non_overlapping_iter_count += 1;
                } else {
                    let sstable_info = &sstable_infos[0];

                    let sstable = self
                        .sstable_store
                        .sstable(sstable_info, local_stats)
                        .await?;

                    if let Some(dist_hash) = filter_prefix_hash.as_ref()
                        && !hit_sstable_filter(
                            &sstable,
                            &user_key_range_ref,
                            *dist_hash,
                            local_stats,
                        )
                    {
                        continue;
                    }
                    // Since there is only one sst to be included for the current non-overlapping
                    // level, there is no need to create a ConcatIterator on it.
                    // We put the SstableIterator in `overlapping_iters` just for convenience since
                    // it overlaps with SSTs in other levels. In metrics reporting, we still count
                    // it in `non_overlapping_iter_count`.
                    factory.add_overlapping_sst_iter(F::SstableIteratorType::create(
                        sstable,
                        self.sstable_store.clone(),
                        sst_read_options.clone(),
                        sstable_info,
                    ));
                    local_stats.non_overlapping_iter_count += 1;
                }
            } else {
                let table_infos =
                    prune_overlapping_ssts(&level.table_infos, table_id, &table_key_range);
                // Overlapping
                let fetch_meta_req = table_infos.rev().collect_vec();
                if fetch_meta_req.is_empty() {
                    continue;
                }
                for sstable_info in fetch_meta_req {
                    let sstable = self
                        .sstable_store
                        .sstable(sstable_info, local_stats)
                        .await?;
                    assert_eq!(sstable_info.object_id, sstable.id);
                    if let Some(dist_hash) = filter_prefix_hash.as_ref()
                        && !hit_sstable_filter(
                            &sstable,
                            &user_key_range_ref,
                            *dist_hash,
                            local_stats,
                        )
                    {
                        continue;
                    }
                    factory.add_overlapping_sst_iter(F::SstableIteratorType::create(
                        sstable,
                        self.sstable_store.clone(),
                        sst_read_options.clone(),
                        sstable_info,
                    ));
                    local_stats.overlapping_iter_count += 1;
                }
            }
        }
        let fetch_meta_duration_sec = timer.elapsed().as_secs_f64();
        if fetch_meta_duration_sec > SLOW_ITER_FETCH_META_DURATION_SECOND {
            let table_id_string = table_id.to_string();
            tracing::warn!(
                "Fetching meta while creating an iter to read table_id {:?} at epoch {:?} is slow: duration = {:?}s, cache unhits = {:?}.",
                table_id_string,
                epoch,
                fetch_meta_duration_sec,
                local_stats.cache_meta_block_miss
            );
            self.state_store_metrics
                .iter_slow_fetch_meta_cache_unhits
                .set(local_stats.cache_meta_block_miss as i64);
        }
        Ok(())
    }

    pub async fn iter_log(
        &self,
        epoch_range: (u64, u64),
        key_range: TableKeyRange,
        options: ReadLogOptions,
        table_change_log_manager: Arc<TableChangeLogManager>,
    ) -> HummockResult<ChangeLogIterator> {
        // The end value of `epoch_range` is not greater than max committed epoch, guaranteed by the caller `BatchTableInnerIterLogInner`.
        let change_log: Vec<_> = {
            let table_change_logs = table_change_log_manager
                .fetch_table_change_logs(options.table_id, epoch_range, false, None)
                .await?;
            if let Some(change_log) = table_change_logs.get(&options.table_id) {
                change_log.filter_epoch(epoch_range).cloned().collect_vec()
            } else {
                Vec::new()
            }
        };

        if let Some(max_epoch_change_log) = change_log.last() {
            let (_, max_epoch) = epoch_range;
            if !max_epoch_change_log.epochs().contains(&max_epoch) {
                warn!(
                    max_epoch,
                    change_log_epochs = ?change_log.iter().flat_map(|epoch_log| epoch_log.epochs()).collect_vec(),
                    table_id = %options.table_id,
                    "max_epoch does not exist"
                );
            }
        }
        let read_options = Arc::new(SstableIteratorReadOptions {
            cache_policy: Default::default(),
            scan_end_user_key: None,
            prefetch: false,
            max_preload_retry_times: 0,
        });

        async fn make_iter(
            sstable_infos: impl Iterator<Item = &SstableInfo>,
            sstable_store: &SstableStoreRef,
            read_options: Arc<SstableIteratorReadOptions>,
            local_stat: &mut StoreLocalStatistic,
        ) -> HummockResult<MergeIterator<SstableIterator>> {
            let iters = try_join_all(sstable_infos.map(|sstable_info| {
                let sstable_store = sstable_store.clone();
                let read_options = read_options.clone();
                async move {
                    let mut local_stat = StoreLocalStatistic::default();
                    let table_holder = sstable_store.sstable(sstable_info, &mut local_stat).await?;
                    Ok::<_, HummockError>((
                        SstableIterator::new(
                            table_holder,
                            sstable_store,
                            read_options,
                            sstable_info,
                        ),
                        local_stat,
                    ))
                }
            }))
            .await?;
            Ok::<_, HummockError>(MergeIterator::new(iters.into_iter().map(
                |(iter, stats)| {
                    local_stat.add(&stats);
                    iter
                },
            )))
        }

        let mut local_stat = StoreLocalStatistic::default();

        let new_value_iter = make_iter(
            change_log
                .iter()
                .flat_map(|log| log.new_value.iter())
                .filter(|sst| filter_single_sst(sst, options.table_id, &key_range)),
            &self.sstable_store,
            read_options.clone(),
            &mut local_stat,
        )
        .await?;
        let old_value_iter = make_iter(
            change_log
                .iter()
                .flat_map(|log| log.old_value.iter())
                .filter(|sst| filter_single_sst(sst, options.table_id, &key_range)),
            &self.sstable_store,
            read_options.clone(),
            &mut local_stat,
        )
        .await?;
        ChangeLogIterator::new(
            epoch_range,
            key_range,
            new_value_iter,
            old_value_iter,
            options.table_id,
            IterLocalMetricsGuard::new(
                self.state_store_metrics.clone(),
                options.table_id,
                local_stat,
            ),
        )
        .await
    }

    pub async fn nearest<'a, M: MeasureDistanceBuilder, O: Send>(
        &'a self,
        version: PinnedVersion,
        table_id: TableId,
        target: VectorRef<'a>,
        options: VectorNearestOptions,
        on_nearest_item_fn: impl OnNearestItemFn<'a, O>,
    ) -> HummockResult<Vec<O>> {
        let Some(index) = version.vector_indexes.get(&table_id) else {
            return Ok(vec![]);
        };
        if target.dimension() != index.dimension {
            return Err(HummockError::other(format!(
                "target dimension {} not match index dimension {}",
                target.dimension(),
                index.dimension
            )));
        }
        match &index.inner {
            VectorIndexImpl::Flat(flat) => {
                let mut builder = NearestBuilder::<'_, O, M>::new(target, options.top_n);
                let mut cache_stat = VectorStoreCacheStats::default();
                for vector_file in &flat.vector_store_info.vector_files {
                    let meta = self
                        .sstable_store
                        .get_vector_file_meta(vector_file, &mut cache_stat)
                        .await?;
                    for (i, block_meta) in meta.block_metas.iter().enumerate() {
                        let block = self
                            .sstable_store
                            .get_vector_block(vector_file, i, block_meta, &mut cache_stat)
                            .await?;
                        builder.add(&**block, &on_nearest_item_fn);
                    }
                }
                cache_stat.report(table_id, "flat", self.stats());
                Ok(builder.finish())
            }
            VectorIndexImpl::HnswFlat(hnsw_flat) => {
                let Some(graph_file) = &hnsw_flat.graph_file else {
                    return Ok(vec![]);
                };

                let mut ctx = FileVectorStoreCtx::default();

                let graph = self
                    .sstable_store
                    .get_hnsw_graph(graph_file, &mut ctx.stats)
                    .await?;

                let vector_store =
                    FileVectorStore::new_for_reader(hnsw_flat, self.sstable_store.clone());
                let (items, stats) = nearest::<O, M, _>(
                    &vector_store,
                    &mut ctx,
                    &*graph,
                    target,
                    on_nearest_item_fn,
                    options.hnsw_ef_search,
                    options.top_n,
                )
                .await?;
                ctx.stats.report(table_id, "hnsw_read", self.stats());
                report_hnsw_stat(
                    self.stats(),
                    table_id,
                    "hnsw_read",
                    options.top_n,
                    options.hnsw_ef_search,
                    [stats],
                );
                Ok(items)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::Arc;

    use bytes::Bytes;
    use prometheus::Registry;
    use risingwave_common::catalog::{TableId, TableOption};
    use risingwave_common::config::MetricLevel;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKey, gen_key_from_bytes};
    use risingwave_hummock_sdk::key_range::KeyRange;
    use risingwave_hummock_sdk::level::{Level, Levels, OverlappingLevel};
    use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner, VnodeStatistics};
    use risingwave_hummock_sdk::version::HummockVersion;
    use risingwave_hummock_sdk::{EpochWithGap, HummockSstableObjectId};
    use risingwave_pb::hummock::hummock_version::PbLevels;
    use risingwave_pb::hummock::{
        LevelType as PbLevelType, PbHummockVersion, PbLevel, PbOverlappingLevel,
        PbSstableFilterLayout, PbSstableFilterType, PbStateTableInfo, StateTableInfoDelta,
    };
    use tokio::sync::mpsc::unbounded_channel;

    use crate::hummock::HummockValue;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::local_version::pinned_version::{PinVersionAction, PinnedVersion};
    use crate::hummock::store::version::{CommittedVersion, HummockVersionReader};
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_test_sstable_with_table_ids,
    };
    use crate::monitor::{HummockStateStoreMetrics, flush_local_metrics_for_test};
    use crate::store::ReadOptions;

    /// In a nonoverlapping level, `search_sst_idx` may locate an SST whose key range covers
    /// the query user key but whose `table_ids` does not contain the queried table. The get
    /// path must skip such SSTs instead of reading them.
    #[tokio::test]
    async fn test_get_skips_sst_by_table_id_filter() {
        let query_table_id = TableId::new(100);
        let epoch: u64 = (31 * 1000) << 16;
        let compaction_group_id = StaticCompactionGroupId::StateDefault;

        // SST key range: table_id 50..150, but table_ids = [50, 150] (no 100).
        let sst_info = SstableInfoInner {
            sst_id: 1.into(),
            object_id: 1.into(),
            key_range: KeyRange {
                left: Bytes::from(
                    FullKey::for_test(TableId::new(50), b"aaa".to_vec(), epoch).encode(),
                ),
                right: Bytes::from(
                    FullKey::for_test(TableId::new(150), b"zzz".to_vec(), epoch).encode(),
                ),
                right_exclusive: false,
            },
            table_ids: vec![TableId::new(50), TableId::new(150)],
            file_size: 1024,
            ..Default::default()
        }
        .into();

        let level = Level {
            level_idx: 1,
            level_type: PbLevelType::Nonoverlapping,
            table_infos: vec![sst_info],
            total_file_size: 0,
            sub_level_id: 0,
            uncompressed_file_size: 0,
            vnode_partition_count: 0,
        };

        #[allow(deprecated)]
        let levels = Levels {
            levels: vec![level],
            l0: OverlappingLevel::default(),
            group_id: compaction_group_id,
            parent_group_id: compaction_group_id,
            member_table_ids: vec![],
            compaction_group_version_id: 0,
        };

        let mut version = HummockVersion::from_persisted_protobuf_owned(PbHummockVersion {
            id: 1u64.into(),
            ..Default::default()
        });
        version.levels.insert(compaction_group_id, levels);
        version.state_table_info.apply_delta(
            &HashMap::from([(
                query_table_id,
                StateTableInfoDelta {
                    committed_epoch: epoch,
                    compaction_group_id,
                },
            )]),
            &HashSet::new(),
        );

        let pinned_version = PinnedVersion::new(version, unbounded_channel().0);
        let reader = HummockVersionReader::new(
            mock_sstable_store().await,
            Arc::new(HummockStateStoreMetrics::unused()),
            0,
        );

        let result = reader
            .get(
                TableKey(Bytes::from("test_key")),
                epoch,
                query_table_id,
                TableOption::default(),
                ReadOptions::default(),
                (vec![], vec![], pinned_version),
                |_key, _value| Ok(()),
            )
            .await
            .unwrap();

        assert!(result.is_none());
    }

    /// Build a committed version containing a single SST with custom vnode stats.
    #[allow(deprecated)]
    fn build_version_with_vnode_stats(
        table_id: TableId,
        vnode_stats: VnodeStatistics,
        key_range: (Vec<u8>, Vec<u8>),
        level_type: PbLevelType,
    ) -> (SstableInfo, CommittedVersion) {
        let object_id = HummockSstableObjectId::new(1);
        let left_full_key = FullKey::new_with_gap_epoch(
            table_id,
            TableKey(Bytes::from(key_range.0)),
            EpochWithGap::new_from_epoch(test_epoch(0)),
        )
        .encode();
        let right_full_key = FullKey::new_with_gap_epoch(
            table_id,
            TableKey(Bytes::from(key_range.1)),
            EpochWithGap::new_from_epoch(test_epoch(0)),
        )
        .encode();

        let sstable_info: SstableInfo = SstableInfoInner {
            object_id,
            sst_id: object_id.as_raw_id().into(),
            key_range: KeyRange {
                left: Bytes::from(left_full_key),
                right: Bytes::from(right_full_key),
                right_exclusive: false,
            },
            file_size: 1,
            table_ids: vec![table_id],
            meta_offset: 0,
            stale_key_count: 0,
            total_key_count: 0,
            min_epoch: 0,
            max_epoch: 0,
            uncompressed_file_size: 0,
            range_tombstone_count: 0,
            filter_type: PbSstableFilterType::SstableFilterXor16,
            filter_layout: PbSstableFilterLayout::Plain,
            sst_size: 1,
            vnode_statistics: Some(vnode_stats),
        }
        .into();
        let pb_level = PbLevel {
            level_idx: if level_type == PbLevelType::Overlapping {
                0
            } else {
                1
            },
            level_type: level_type as i32,
            table_infos: vec![sstable_info.clone().into()],
            total_file_size: 1,
            sub_level_id: 0,
            uncompressed_file_size: 1,
            vnode_partition_count: 0,
        };

        let (levels, l0) = if level_type == PbLevelType::Overlapping {
            (
                vec![],
                Some(PbOverlappingLevel {
                    sub_levels: vec![pb_level],
                    total_file_size: 1,
                    uncompressed_file_size: 1,
                }),
            )
        } else {
            (vec![pb_level], Some(PbOverlappingLevel::default()))
        };

        let pb_levels = PbLevels {
            levels,
            l0,
            group_id: StaticCompactionGroupId::NewCompactionGroup,
            parent_group_id: 0.into(),
            member_table_ids: vec![],
            compaction_group_version_id: 0,
        };

        let pb_version = PbHummockVersion {
            id: 1.into(),
            levels: HashMap::from_iter([(StaticCompactionGroupId::NewCompactionGroup, pb_levels)]),
            max_committed_epoch: 0,
            table_watermarks: HashMap::new(),
            table_change_logs: HashMap::new(),
            state_table_info: HashMap::from_iter([(
                table_id,
                PbStateTableInfo {
                    committed_epoch: 0,
                    compaction_group_id: StaticCompactionGroupId::NewCompactionGroup,
                },
            )]),
            vector_indexes: HashMap::new(),
        };

        let version = HummockVersion::from(&pb_version);
        let (tx, _rx) = unbounded_channel::<PinVersionAction>();
        let pinned = PinnedVersion::new(version, tx);
        (sstable_info, pinned)
    }

    /// Build a committed version from an existing SST (with real object in the store).
    #[allow(deprecated)]
    fn build_version_from_sstables(
        table_id: TableId,
        sstable_infos: Vec<SstableInfo>,
        level_type: PbLevelType,
    ) -> CommittedVersion {
        let total_file_size = sstable_infos.iter().map(|sst| sst.file_size).sum::<u64>();
        let uncompressed_file_size = sstable_infos
            .iter()
            .map(|sst| sst.uncompressed_file_size)
            .sum::<u64>();
        let pb_level = PbLevel {
            level_idx: if level_type == PbLevelType::Overlapping {
                0
            } else {
                1
            },
            level_type: level_type as i32,
            table_infos: sstable_infos.into_iter().map(Into::into).collect(),
            total_file_size,
            sub_level_id: 0,
            uncompressed_file_size,
            vnode_partition_count: 0,
        };

        let (levels, l0) = if level_type == PbLevelType::Overlapping {
            (
                vec![],
                Some(PbOverlappingLevel {
                    sub_levels: vec![pb_level],
                    total_file_size,
                    uncompressed_file_size,
                }),
            )
        } else {
            (vec![pb_level], Some(PbOverlappingLevel::default()))
        };

        let pb_levels = PbLevels {
            levels,
            l0,
            group_id: StaticCompactionGroupId::NewCompactionGroup,
            parent_group_id: 0.into(),
            member_table_ids: vec![],
            compaction_group_version_id: 0,
        };

        let pb_version = PbHummockVersion {
            id: 1.into(),
            levels: HashMap::from_iter([(StaticCompactionGroupId::NewCompactionGroup, pb_levels)]),
            max_committed_epoch: 0,
            table_watermarks: HashMap::new(),
            table_change_logs: HashMap::new(),
            state_table_info: HashMap::from_iter([(
                table_id,
                PbStateTableInfo {
                    committed_epoch: 0,
                    compaction_group_id: StaticCompactionGroupId::NewCompactionGroup,
                },
            )]),
            vector_indexes: HashMap::new(),
        };

        let version = HummockVersion::from(&pb_version);
        let (tx, _rx) = unbounded_channel::<PinVersionAction>();
        PinnedVersion::new(version, tx)
    }

    /// Build a committed version from one existing non-overlapping SST.
    #[allow(deprecated)]
    fn build_version_from_sstable(
        table_id: TableId,
        sstable_info: SstableInfo,
    ) -> CommittedVersion {
        build_version_from_sstables(table_id, vec![sstable_info], PbLevelType::Nonoverlapping)
    }

    fn vnode_prune_counts(
        metrics: &HummockStateStoreMetrics,
        table_id: TableId,
        operation: &str,
    ) -> (u64, u64) {
        let table_label = table_id.to_string();
        let checked = metrics
            .vnode_pruning_counts
            .with_guarded_label_values(&[
                table_label.clone(),
                operation.to_owned(),
                "checked".to_owned(),
            ])
            .get();
        let pruned = metrics
            .vnode_pruning_counts
            .with_guarded_label_values(&[table_label, operation.to_owned(), "pruned".to_owned()])
            .get();
        (checked, pruned)
    }

    async fn assert_vnode_prune_get_skips_out_of_range_key(
        table_id: TableId,
        epoch: u64,
        level_type: PbLevelType,
    ) {
        let sstable_store = mock_sstable_store().await;
        let registry = Registry::new();
        let metrics = Arc::new(HummockStateStoreMetrics::new(&registry, MetricLevel::Debug));
        let reader = HummockVersionReader::new(sstable_store, metrics.clone(), 0);
        let (checked_before, pruned_before) = vnode_prune_counts(&metrics, table_id, "get");

        let make_user_key = |vnode: VirtualNode, suffix: &str| {
            let mut raw = vnode.to_be_bytes().to_vec();
            raw.extend_from_slice(suffix.as_bytes());
            UserKey::new(table_id, TableKey(raw.into()))
        };

        // Stats cover vnode 1 only up to "bb".
        let vnode_stats = VnodeStatistics::from_map(BTreeMap::from_iter([(
            VirtualNode::from_index(1),
            (
                make_user_key(VirtualNode::from_index(1), "aa"),
                make_user_key(VirtualNode::from_index(1), "bb"),
            ),
        )]));

        // SST key range is wide enough to include the queried key, but vnode stats should prune it.
        let key_range = {
            let mut left = VirtualNode::from_index(0).to_be_bytes().to_vec();
            left.extend_from_slice(b"aa");
            let mut right = VirtualNode::from_index(1).to_be_bytes().to_vec();
            right.extend_from_slice(b"zzzz");
            (left, right)
        };

        let (_sst, committed) =
            build_version_with_vnode_stats(table_id, vnode_stats, key_range, level_type);

        // Query vnode 1 but with suffix beyond the recorded max -> should be pruned.
        let mut raw = VirtualNode::from_index(1).to_be_bytes().to_vec();
        raw.extend_from_slice(b"zz");
        let table_key = TableKey(Bytes::from(raw.clone()));

        let result = reader
            .get(
                table_key,
                epoch,
                table_id,
                TableOption::default(),
                ReadOptions::default(),
                (vec![], vec![], committed),
                |_k, v| Ok(Bytes::copy_from_slice(v)),
            )
            .await
            .unwrap();
        flush_local_metrics_for_test();

        assert!(
            result.is_none(),
            "vnode pruning should skip SST without reading data"
        );
        let (checked_after, pruned_after) = vnode_prune_counts(&metrics, table_id, "get");
        assert_eq!(checked_before + 1, checked_after);
        assert_eq!(pruned_before + 1, pruned_after);
    }

    async fn assert_vnode_prune_get_not_pruned_nonoverlapping() {
        let table_id = TableId::new(42);
        let epoch = test_epoch(3);
        let sstable_store = mock_sstable_store().await;
        let registry = Registry::new();
        let metrics = Arc::new(HummockStateStoreMetrics::new(&registry, MetricLevel::Debug));
        let reader = HummockVersionReader::new(sstable_store.clone(), metrics.clone(), 0);
        let (checked_before, pruned_before) = vnode_prune_counts(&metrics, table_id, "get");

        let mut opts = default_builder_opt_for_test();
        opts.max_vnode_key_range_bytes = None;
        let mut kvs = vec![
            (
                FullKey::new_with_gap_epoch(
                    table_id,
                    gen_key_from_bytes(VirtualNode::from_index(1), b"aa"),
                    EpochWithGap::new_from_epoch(epoch),
                ),
                HummockValue::put(Bytes::from_static(b"v1")),
            ),
            (
                FullKey::new_with_gap_epoch(
                    table_id,
                    gen_key_from_bytes(VirtualNode::ZERO, b"cc"),
                    EpochWithGap::new_from_epoch(epoch),
                ),
                HummockValue::put(Bytes::from_static(b"v0")),
            ),
        ];
        kvs.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        let (_, mut sstable_info): (crate::hummock::sstable_store::TableHolder, SstableInfo) =
            gen_test_sstable_with_table_ids(
                opts,
                10,
                kvs.into_iter(),
                sstable_store.clone(),
                vec![table_id.as_raw_id()],
            )
            .await;
        // Override vnode stats to ensure the queried key falls inside the recorded range.
        let mut inner = sstable_info.get_inner();
        inner.vnode_statistics = Some(VnodeStatistics::from_map(BTreeMap::from_iter([(
            VirtualNode::from_index(1),
            (
                UserKey::new(
                    table_id,
                    gen_key_from_bytes(VirtualNode::from_index(1), b"aa"),
                ),
                UserKey::new(
                    table_id,
                    gen_key_from_bytes(VirtualNode::from_index(1), b"zz"),
                ),
            ),
        )])));
        sstable_info = inner.into();
        let committed = build_version_from_sstable(table_id, sstable_info.clone());

        // Key is within vnode range, should not be pruned.
        let mut raw = VirtualNode::from_index(1).to_be_bytes().to_vec();
        raw.extend_from_slice(b"aa");
        let table_key = TableKey(Bytes::from(raw.clone()));

        let result = reader
            .get(
                table_key,
                epoch,
                table_id,
                TableOption::default(),
                ReadOptions::default(),
                (vec![], vec![], committed),
                |_k, v| Ok(Bytes::copy_from_slice(v)),
            )
            .await
            .unwrap();
        flush_local_metrics_for_test();
        assert!(result.is_some(), "key should be read when not pruned");
        let (checked_after, pruned_after) = vnode_prune_counts(&metrics, table_id, "get");
        assert_eq!(checked_before + 1, checked_after);
        assert_eq!(pruned_before, pruned_after);
    }

    #[tokio::test]
    async fn test_vnode_prune_get_single_sst_cases() {
        assert_vnode_prune_get_skips_out_of_range_key(
            TableId::default(),
            test_epoch(1),
            PbLevelType::Nonoverlapping,
        )
        .await;
        assert_vnode_prune_get_skips_out_of_range_key(
            TableId::new(7),
            test_epoch(2),
            PbLevelType::Overlapping,
        )
        .await;
        assert_vnode_prune_get_not_pruned_nonoverlapping().await;
    }

    #[tokio::test]
    async fn test_vnode_prune_get_overlapping_distribution_prunes_only_out_of_range_sst() {
        let table_id = TableId::new(77);
        let epoch = test_epoch(4);
        let vnode = VirtualNode::from_index(1);
        let sstable_store = mock_sstable_store().await;
        let registry = Registry::new();
        let metrics = Arc::new(HummockStateStoreMetrics::new(&registry, MetricLevel::Debug));
        let reader = HummockVersionReader::new(sstable_store.clone(), metrics.clone(), 0);
        let (checked_before, pruned_before) = vnode_prune_counts(&metrics, table_id, "get");

        let mut opts = default_builder_opt_for_test();
        opts.max_vnode_key_range_bytes = None;

        let mut kvs1 = vec![
            (
                FullKey::new_with_gap_epoch(
                    table_id,
                    gen_key_from_bytes(vnode, b"aa"),
                    EpochWithGap::new_from_epoch(epoch),
                ),
                HummockValue::put(Bytes::from_static(b"s1_aa")),
            ),
            (
                FullKey::new_with_gap_epoch(
                    table_id,
                    gen_key_from_bytes(vnode, b"zz"),
                    EpochWithGap::new_from_epoch(epoch),
                ),
                HummockValue::put(Bytes::from_static(b"s1_zz")),
            ),
        ];
        kvs1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        let (_, mut sst1): (crate::hummock::sstable_store::TableHolder, SstableInfo) =
            gen_test_sstable_with_table_ids(
                opts.clone(),
                11,
                kvs1.into_iter(),
                sstable_store.clone(),
                vec![table_id.as_raw_id()],
            )
            .await;
        let mut sst1_inner = sst1.get_inner();
        sst1_inner.vnode_statistics = Some(VnodeStatistics::from_map(BTreeMap::from_iter([(
            vnode,
            (
                UserKey::new(table_id, gen_key_from_bytes(vnode, b"aa")),
                UserKey::new(table_id, gen_key_from_bytes(vnode, b"bb")),
            ),
        )])));
        sst1 = sst1_inner.into();

        let mut kvs2 = vec![(
            FullKey::new_with_gap_epoch(
                table_id,
                gen_key_from_bytes(vnode, b"mm"),
                EpochWithGap::new_from_epoch(epoch),
            ),
            HummockValue::put(Bytes::from_static(b"hit")),
        )];
        kvs2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        let (_, mut sst2): (crate::hummock::sstable_store::TableHolder, SstableInfo) =
            gen_test_sstable_with_table_ids(
                opts,
                12,
                kvs2.into_iter(),
                sstable_store.clone(),
                vec![table_id.as_raw_id()],
            )
            .await;
        let mut sst2_inner = sst2.get_inner();
        sst2_inner.vnode_statistics = Some(VnodeStatistics::from_map(BTreeMap::from_iter([(
            vnode,
            (
                UserKey::new(table_id, gen_key_from_bytes(vnode, b"aa")),
                UserKey::new(table_id, gen_key_from_bytes(vnode, b"zz")),
            ),
        )])));
        sst2 = sst2_inner.into();

        let committed =
            build_version_from_sstables(table_id, vec![sst1, sst2], PbLevelType::Overlapping);

        let table_key = gen_key_from_bytes(vnode, b"mm");
        let result = reader
            .get(
                table_key,
                epoch,
                table_id,
                TableOption::default(),
                ReadOptions::default(),
                (vec![], vec![], committed),
                |_k, v| Ok(Bytes::copy_from_slice(v)),
            )
            .await
            .unwrap();
        flush_local_metrics_for_test();

        assert_eq!(result, Some(Bytes::from_static(b"hit")));
        let (checked_after, pruned_after) = vnode_prune_counts(&metrics, table_id, "get");
        // First SST is pruned by vnode stats; second SST is checked and read.
        assert_eq!(checked_before + 2, checked_after);
        assert_eq!(pruned_before + 1, pruned_after);
    }
}
