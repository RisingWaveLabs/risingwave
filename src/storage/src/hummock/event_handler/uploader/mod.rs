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

mod spiller;
mod task_manager;
pub(crate) mod test_utils;

use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque, hash_map};
use std::fmt::{Debug, Display, Formatter};
use std::future::{Future, poll_fn};
use std::mem::{replace, swap, take};
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use futures::FutureExt;
use itertools::Itertools;
use more_asserts::assert_gt;
use prometheus::{Histogram, HistogramTimer, IntGauge};
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::catalog::TableId;
use risingwave_common::metrics::UintGauge;
use risingwave_common::must_match;
use risingwave_hummock_sdk::table_watermark::{
    TableWatermarks, VnodeWatermark, WatermarkDirection, WatermarkSerdeType,
};
use risingwave_hummock_sdk::vector_index::VectorIndexAdd;
use risingwave_hummock_sdk::{HummockEpoch, HummockRawObjectId, LocalSstableInfo};
use task_manager::{TaskManager, UploadingTaskStatus};
use thiserror_ext::AsReport;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::hummock::event_handler::LocalInstanceId;
use crate::hummock::event_handler::hummock_event_handler::{BufferTracker, send_sync_result};
use crate::hummock::event_handler::uploader::spiller::Spiller;
use crate::hummock::event_handler::uploader::uploader_imm::UploaderImm;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatchId;
use crate::hummock::store::version::StagingSstableInfo;
use crate::hummock::utils::MemoryTracker;
use crate::hummock::{HummockError, HummockResult, ImmutableMemtable};
use crate::mem_table::ImmId;
use crate::monitor::HummockStateStoreMetrics;
use crate::store::SealCurrentEpochOptions;

/// Take epoch data inclusively before `epoch` out from `data`
fn take_before_epoch<T>(
    data: &mut BTreeMap<HummockEpoch, T>,
    epoch: HummockEpoch,
) -> BTreeMap<HummockEpoch, T> {
    let mut before_epoch_data = data.split_off(&(epoch + 1));
    swap(&mut before_epoch_data, data);
    before_epoch_data
}

type UploadTaskInput = HashMap<LocalInstanceId, Vec<UploaderImm>>;
pub type UploadTaskPayload = HashMap<LocalInstanceId, Vec<ImmutableMemtable>>;

#[derive(Debug)]
pub struct UploadTaskOutput {
    pub new_value_ssts: Vec<LocalSstableInfo>,
    pub old_value_ssts: Vec<LocalSstableInfo>,
    pub wait_poll_timer: Option<HistogramTimer>,
}
pub type SpawnUploadTask = Arc<
    dyn Fn(UploadTaskPayload, UploadTaskInfo) -> JoinHandle<HummockResult<UploadTaskOutput>>
        + Send
        + Sync
        + 'static,
>;

#[derive(Clone)]
pub struct UploadTaskInfo {
    pub task_size: usize,
    pub epochs: Vec<HummockEpoch>,
    pub table_ids: HashSet<TableId>,
    pub imm_ids: HashMap<LocalInstanceId, Vec<ImmId>>,
}

impl Display for UploadTaskInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadTaskInfo")
            .field("task_size", &self.task_size)
            .field("epochs", &self.epochs)
            .field("len(imm_ids)", &self.imm_ids.len())
            .finish()
    }
}

impl Debug for UploadTaskInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadTaskInfo")
            .field("task_size", &self.task_size)
            .field("epochs", &self.epochs)
            .field("imm_ids", &self.imm_ids)
            .finish()
    }
}

mod uploader_imm {
    use std::fmt::Formatter;
    use std::ops::Deref;

    use risingwave_common::metrics::{LabelGuardedIntGauge, UintGauge};

    #[cfg(test)]
    use crate::hummock::MemoryLimiter;
    use crate::hummock::event_handler::uploader::UploaderContext;
    use crate::hummock::utils::MemoryTracker;
    use crate::mem_table::ImmutableMemtable;

    pub(super) struct UploaderImm {
        inner: ImmutableMemtable,
        size_guard: UintGauge,
        per_table_size_guard: LabelGuardedIntGauge,
        per_table_count_guard: LabelGuardedIntGauge,
        _tracker: MemoryTracker,
    }

    impl UploaderImm {
        pub(super) fn new(
            imm: ImmutableMemtable,
            context: &UploaderContext,
            tracker: MemoryTracker,
        ) -> Self {
            let size = imm.size();
            let table_id_str = imm.table_id().to_string();
            let size_guard = context.stats.uploader_imm_size.clone();
            let per_table_size_guard = context
                .stats
                .uploader_per_table_imm_size
                .with_guarded_label_values(&[&table_id_str]);
            let per_table_count_guard = context
                .stats
                .uploader_per_table_imm_count
                .with_guarded_label_values(&[&table_id_str]);
            size_guard.add(size as _);
            per_table_size_guard.add(size as _);
            per_table_count_guard.add(1 as _);
            Self {
                inner: imm,
                size_guard,
                per_table_size_guard,
                per_table_count_guard,
                _tracker: tracker,
            }
        }

        #[cfg(test)]
        pub(super) fn for_test(imm: ImmutableMemtable) -> Self {
            Self {
                inner: imm,
                size_guard: UintGauge::new("test", "test").unwrap(),
                per_table_size_guard: LabelGuardedIntGauge::test_int_gauge::<1>(),
                per_table_count_guard: LabelGuardedIntGauge::test_int_gauge::<1>(),
                _tracker: MemoryLimiter::unlimit().try_require_memory(1).unwrap(),
            }
        }
    }

    impl std::fmt::Debug for UploaderImm {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            self.inner.fmt(f)
        }
    }

    impl Deref for UploaderImm {
        type Target = ImmutableMemtable;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    impl Drop for UploaderImm {
        fn drop(&mut self) {
            self.size_guard.sub(self.inner.size() as _);
            self.per_table_size_guard.sub(self.inner.size() as _);
            self.per_table_count_guard.dec();
        }
    }
}

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord, Copy, Clone, Debug)]
struct UploadingTaskId(usize);

/// A wrapper for a uploading task that compacts and uploads the imm payload. Task context are
/// stored so that when the task fails, it can be re-tried.
struct UploadingTask {
    task_id: UploadingTaskId,
    // newer data at the front
    input: UploadTaskInput,
    join_handle: JoinHandle<HummockResult<UploadTaskOutput>>,
    task_info: UploadTaskInfo,
    spawn_upload_task: SpawnUploadTask,
    task_size_guard: UintGauge,
    task_count_guard: IntGauge,
}

impl Drop for UploadingTask {
    fn drop(&mut self) {
        self.task_size_guard.sub(self.task_info.task_size as u64);
        self.task_count_guard.dec();
    }
}

impl Debug for UploadingTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadingTask")
            .field("input", &self.input)
            .field("task_info", &self.task_info)
            .finish()
    }
}

fn get_payload_imm_ids(
    payload: &UploadTaskPayload,
) -> HashMap<LocalInstanceId, Vec<SharedBufferBatchId>> {
    payload
        .iter()
        .map(|(instance_id, imms)| {
            (
                *instance_id,
                imms.iter().map(|imm| imm.batch_id()).collect_vec(),
            )
        })
        .collect()
}

impl UploadingTask {
    // INFO logs will be enabled for task with size exceeding 50MB.
    const LOG_THRESHOLD_FOR_UPLOAD_TASK_SIZE: usize = 50 * (1 << 20);

    fn input_to_payload(input: &UploadTaskInput) -> UploadTaskPayload {
        input
            .iter()
            .map(|(instance_id, imms)| {
                (
                    *instance_id,
                    imms.iter().map(|imm| (**imm).clone()).collect(),
                )
            })
            .collect()
    }

    fn new(task_id: UploadingTaskId, input: UploadTaskInput, context: &UploaderContext) -> Self {
        assert!(!input.is_empty());
        let mut epochs = input
            .values()
            .flat_map(|imms| imms.iter().map(|imm| imm.epoch()))
            .sorted()
            .dedup()
            .collect_vec();

        // reverse to make newer epochs comes first
        epochs.reverse();
        let payload = Self::input_to_payload(&input);
        let imm_ids = get_payload_imm_ids(&payload);
        let task_size = input
            .values()
            .map(|imms| imms.iter().map(|imm| imm.size()).sum::<usize>())
            .sum();
        let task_info = UploadTaskInfo {
            task_size,
            epochs,
            table_ids: payload
                .values()
                .flat_map(|imms| imms.iter().map(|imm| imm.table_id()))
                .collect(),
            imm_ids,
        };
        context
            .buffer_tracker
            .global_upload_task_size()
            .add(task_size as u64);
        if task_info.task_size > Self::LOG_THRESHOLD_FOR_UPLOAD_TASK_SIZE {
            info!("start upload task: {:?}", task_info);
        } else {
            debug!("start upload task: {:?}", task_info);
        }
        let join_handle = (context.spawn_upload_task)(payload, task_info.clone());
        context.stats.uploader_uploading_task_count.inc();
        Self {
            task_id,
            input,
            join_handle,
            task_info,
            spawn_upload_task: context.spawn_upload_task.clone(),
            task_size_guard: context.buffer_tracker.global_upload_task_size().clone(),
            task_count_guard: context.stats.uploader_uploading_task_count.clone(),
        }
    }

    /// Poll the result of the uploading task
    fn poll_result(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<HummockResult<Arc<StagingSstableInfo>>> {
        Poll::Ready(match ready!(self.join_handle.poll_unpin(cx)) {
            Ok(task_result) => task_result
                .inspect(|_| {
                    if self.task_info.task_size > Self::LOG_THRESHOLD_FOR_UPLOAD_TASK_SIZE {
                        info!(task_info = ?self.task_info, "upload task finish");
                    } else {
                        debug!(task_info = ?self.task_info, "upload task finish");
                    }
                })
                .inspect_err(|e| error!(task_info = ?self.task_info, err = ?e.as_report(), "upload task failed"))
                .map(|output| {
                    Arc::new(StagingSstableInfo::new(
                        output.new_value_ssts,
                        output.old_value_ssts,
                        self.task_info.epochs.clone(),
                        self.task_info.imm_ids.clone(),
                        self.task_info.task_size,
                    ))
                }),

            Err(err) => Err(HummockError::other(format!(
                "failed to join the upload task handle: {}",
                err.as_report()
            ))),
        })
    }

    /// Poll the uploading task until it succeeds. If it fails, we will retry it.
    fn poll_ok_with_retry(&mut self, cx: &mut Context<'_>) -> Poll<Arc<StagingSstableInfo>> {
        loop {
            let result = ready!(self.poll_result(cx));
            match result {
                Ok(sstables) => return Poll::Ready(sstables),
                Err(e) => {
                    error!(
                        error = %e.as_report(),
                        task_info = ?self.task_info,
                        "a flush task failed, start retry",
                    );
                    self.join_handle = (self.spawn_upload_task)(
                        Self::input_to_payload(&self.input),
                        self.task_info.clone(),
                    );
                    // It is important not to return Poll::pending here immediately, because the new
                    // join_handle is not polled yet, and will not awake the current task when
                    // succeed. It will be polled in the next loop iteration.
                }
            }
        }
    }

    pub fn get_task_info(&self) -> &UploadTaskInfo {
        &self.task_info
    }
}

impl TableUnsyncData {
    fn add_table_watermarks(
        &mut self,
        epoch: HummockEpoch,
        table_watermarks: Vec<VnodeWatermark>,
        direction: WatermarkDirection,
        watermark_type: WatermarkSerdeType,
    ) {
        if table_watermarks.is_empty() {
            return;
        }
        let vnode_count = table_watermarks[0].vnode_count();
        for watermark in &table_watermarks {
            assert_eq!(vnode_count, watermark.vnode_count());
        }

        fn apply_new_vnodes(
            vnode_bitmap: &mut BitmapBuilder,
            vnode_watermarks: &Vec<VnodeWatermark>,
        ) {
            for vnode_watermark in vnode_watermarks {
                for vnode in vnode_watermark.vnode_bitmap().iter_ones() {
                    assert!(
                        !vnode_bitmap.is_set(vnode),
                        "vnode {} write multiple table watermarks",
                        vnode
                    );
                    vnode_bitmap.set(vnode, true);
                }
            }
        }
        match &mut self.table_watermarks {
            Some((prev_direction, prev_watermarks, prev_watermark_type)) => {
                assert_eq!(
                    *prev_direction, direction,
                    "table id {} new watermark direction not match with previous",
                    self.table_id
                );
                assert_eq!(
                    *prev_watermark_type, watermark_type,
                    "table id {} new watermark watermark_type not match with previous",
                    self.table_id
                );
                match prev_watermarks.entry(epoch) {
                    Entry::Occupied(mut entry) => {
                        let (prev_watermarks, vnode_bitmap) = entry.get_mut();
                        apply_new_vnodes(vnode_bitmap, &table_watermarks);
                        prev_watermarks.extend(table_watermarks);
                    }
                    Entry::Vacant(entry) => {
                        let mut vnode_bitmap = BitmapBuilder::zeroed(vnode_count);
                        apply_new_vnodes(&mut vnode_bitmap, &table_watermarks);
                        entry.insert((table_watermarks, vnode_bitmap));
                    }
                }
            }
            None => {
                let mut vnode_bitmap = BitmapBuilder::zeroed(vnode_count);
                apply_new_vnodes(&mut vnode_bitmap, &table_watermarks);
                self.table_watermarks = Some((
                    direction,
                    BTreeMap::from_iter([(epoch, (table_watermarks, vnode_bitmap))]),
                    watermark_type,
                ));
            }
        }
    }
}

impl UploaderData {
    fn add_table_watermarks(
        all_table_watermarks: &mut HashMap<TableId, TableWatermarks>,
        table_id: TableId,
        direction: WatermarkDirection,
        watermarks: impl Iterator<Item = (HummockEpoch, Vec<VnodeWatermark>)>,
        watermark_type: WatermarkSerdeType,
    ) {
        let mut table_watermarks: Option<TableWatermarks> = None;
        for (epoch, watermarks) in watermarks {
            match &mut table_watermarks {
                Some(prev_watermarks) => {
                    assert_eq!(prev_watermarks.direction, direction);
                    assert_eq!(prev_watermarks.watermark_type, watermark_type);
                    prev_watermarks.add_new_epoch_watermarks(
                        epoch,
                        Arc::from(watermarks),
                        direction,
                        watermark_type,
                    );
                }
                None => {
                    table_watermarks = Some(TableWatermarks::single_epoch(
                        epoch,
                        watermarks,
                        direction,
                        watermark_type,
                    ));
                }
            }
        }
        if let Some(table_watermarks) = table_watermarks {
            assert!(
                all_table_watermarks
                    .insert(table_id, table_watermarks)
                    .is_none()
            );
        }
    }
}

struct LocalInstanceEpochData {
    epoch: HummockEpoch,
    // newer data comes first.
    imms: VecDeque<UploaderImm>,
    has_spilled: bool,
}

impl LocalInstanceEpochData {
    fn new(epoch: HummockEpoch) -> Self {
        Self {
            epoch,
            imms: VecDeque::new(),
            has_spilled: false,
        }
    }

    fn epoch(&self) -> HummockEpoch {
        self.epoch
    }

    fn add_imm(&mut self, imm: UploaderImm) {
        assert_eq!(self.epoch, imm.epoch());
        if let Some(prev_imm) = self.imms.front() {
            assert_gt!(imm.batch_id(), prev_imm.batch_id());
        }
        self.imms.push_front(imm);
    }

    fn is_empty(&self) -> bool {
        self.imms.is_empty()
    }
}

struct LocalInstanceUnsyncData {
    table_id: TableId,
    instance_id: LocalInstanceId,
    // None means that the current instance should have stopped advancing
    current_epoch_data: Option<LocalInstanceEpochData>,
    // newer data comes first.
    sealed_data: VecDeque<LocalInstanceEpochData>,
    // newer data comes first
    flushing_imms: VecDeque<SharedBufferBatchId>,
    is_destroyed: bool,
    is_stopped: bool,
}

impl LocalInstanceUnsyncData {
    fn new(table_id: TableId, instance_id: LocalInstanceId, init_epoch: HummockEpoch) -> Self {
        Self {
            table_id,
            instance_id,
            current_epoch_data: Some(LocalInstanceEpochData::new(init_epoch)),
            sealed_data: VecDeque::new(),
            flushing_imms: Default::default(),
            is_destroyed: false,
            is_stopped: false,
        }
    }

    fn add_imm(&mut self, imm: UploaderImm) {
        assert!(!self.is_destroyed);
        assert!(!self.is_stopped);
        assert_eq!(self.table_id, imm.table_id);
        self.current_epoch_data
            .as_mut()
            .expect("should be Some when adding new imm")
            .add_imm(imm);
    }

    fn local_seal_epoch(&mut self, next_epoch: HummockEpoch, stopped: bool) -> HummockEpoch {
        let data = self
            .current_epoch_data
            .as_mut()
            .expect("should be Some when seal new epoch");
        let current_epoch = data.epoch;
        debug!(
            instance_id = self.instance_id,
            next_epoch, current_epoch, stopped, "local seal epoch"
        );
        assert_gt!(next_epoch, current_epoch);
        assert!(!self.is_destroyed);
        assert!(!self.is_stopped);
        self.is_stopped = stopped;
        let epoch_data = replace(data, LocalInstanceEpochData::new(next_epoch));
        if !epoch_data.is_empty() {
            self.sealed_data.push_front(epoch_data);
        }
        current_epoch
    }

    // imm_ids from old to new, which means in ascending order
    fn ack_flushed(&mut self, imm_ids: impl Iterator<Item = SharedBufferBatchId>) {
        for imm_id in imm_ids {
            assert_eq!(self.flushing_imms.pop_back().expect("should exist"), imm_id);
        }
    }

    fn spill(&mut self, epoch: HummockEpoch) -> Vec<UploaderImm> {
        let imms = if let Some(oldest_sealed_epoch) = self.sealed_data.back() {
            match oldest_sealed_epoch.epoch.cmp(&epoch) {
                Ordering::Less => {
                    unreachable!(
                        "should not spill at this epoch because there \
                    is unspilled data in previous epoch: prev epoch {}, spill epoch {}",
                        oldest_sealed_epoch.epoch, epoch
                    );
                }
                Ordering::Equal => {
                    let epoch_data = self.sealed_data.pop_back().unwrap();
                    assert_eq!(epoch, epoch_data.epoch);
                    epoch_data.imms
                }
                Ordering::Greater => VecDeque::new(),
            }
        } else {
            let Some(current_epoch_data) = &mut self.current_epoch_data else {
                return Vec::new();
            };
            match current_epoch_data.epoch.cmp(&epoch) {
                Ordering::Less => {
                    assert!(
                        current_epoch_data.imms.is_empty(),
                        "should not spill at this epoch because there \
                    is unspilled data in current epoch epoch {}, spill epoch {}",
                        current_epoch_data.epoch,
                        epoch
                    );
                    VecDeque::new()
                }
                Ordering::Equal => {
                    if !current_epoch_data.imms.is_empty() {
                        current_epoch_data.has_spilled = true;
                        take(&mut current_epoch_data.imms)
                    } else {
                        VecDeque::new()
                    }
                }
                Ordering::Greater => VecDeque::new(),
            }
        };
        self.add_flushing_imm(imms.iter().rev().map(|imm| imm.batch_id()));
        imms.into_iter().collect()
    }

    fn add_flushing_imm(&mut self, imm_ids: impl Iterator<Item = SharedBufferBatchId>) {
        for imm_id in imm_ids {
            if let Some(prev_imm_id) = self.flushing_imms.front() {
                assert_gt!(imm_id, *prev_imm_id);
            }
            self.flushing_imms.push_front(imm_id);
        }
    }

    // start syncing the imm inclusively before the `epoch`
    // returning data with newer data coming first
    fn sync(&mut self, epoch: HummockEpoch) -> Vec<UploaderImm> {
        // firstly added from old to new
        let mut ret = Vec::new();
        while let Some(epoch_data) = self.sealed_data.back()
            && epoch_data.epoch() <= epoch
        {
            let imms = self.sealed_data.pop_back().expect("checked exist").imms;
            self.add_flushing_imm(imms.iter().rev().map(|imm| imm.batch_id()));
            ret.extend(imms.into_iter().rev());
        }
        // reverse so that newer data comes first
        ret.reverse();
        if let Some(latest_epoch_data) = &self.current_epoch_data
            && latest_epoch_data.epoch <= epoch
        {
            assert!(self.sealed_data.is_empty());
            assert!(latest_epoch_data.is_empty());
            assert!(!latest_epoch_data.has_spilled);
            if cfg!(debug_assertions) {
                panic!(
                    "sync epoch exceeds latest epoch, and the current instance should have been archived, table_id = {}, latest_epoch_data = {}, epoch = {}",
                    self.table_id,
                    latest_epoch_data.epoch(),
                    epoch
                );
            }
            warn!(
                instance_id = self.instance_id,
                table_id = %self.table_id,
                "sync epoch exceeds latest epoch, and the current instance should have be archived"
            );
            self.current_epoch_data = None;
        }
        ret
    }

    fn assert_after_epoch(&self, epoch: HummockEpoch) {
        if let Some(oldest_sealed_data) = self.sealed_data.back() {
            assert!(!oldest_sealed_data.imms.is_empty());
            assert_gt!(oldest_sealed_data.epoch, epoch);
        } else if let Some(current_data) = &self.current_epoch_data
            && current_data.epoch <= epoch
        {
            assert!(current_data.imms.is_empty() && !current_data.has_spilled);
        }
    }

    fn is_finished(&self) -> bool {
        self.is_destroyed && self.sealed_data.is_empty()
    }
}

struct TableUnsyncData {
    table_id: TableId,
    instance_data: HashMap<LocalInstanceId, LocalInstanceUnsyncData>,
    #[expect(clippy::type_complexity)]
    table_watermarks: Option<(
        WatermarkDirection,
        BTreeMap<HummockEpoch, (Vec<VnodeWatermark>, BitmapBuilder)>,
        WatermarkSerdeType,
    )>,
    spill_tasks: BTreeMap<HummockEpoch, VecDeque<UploadingTaskId>>,
    unsync_epochs: BTreeMap<HummockEpoch, ()>,
    // newer epoch at the front
    syncing_epochs: VecDeque<HummockEpoch>,
    max_synced_epoch: Option<HummockEpoch>,
}

impl TableUnsyncData {
    fn new(table_id: TableId, committed_epoch: Option<HummockEpoch>) -> Self {
        Self {
            table_id,
            instance_data: Default::default(),
            table_watermarks: None,
            spill_tasks: Default::default(),
            unsync_epochs: Default::default(),
            syncing_epochs: Default::default(),
            max_synced_epoch: committed_epoch,
        }
    }

    fn new_epoch(&mut self, epoch: HummockEpoch) {
        debug!(table_id = ?self.table_id, epoch, "table new epoch");
        if let Some(latest_epoch) = self.max_epoch() {
            assert_gt!(epoch, latest_epoch);
        }
        self.unsync_epochs.insert(epoch, ());
    }

    #[expect(clippy::type_complexity)]
    fn sync(
        &mut self,
        epoch: HummockEpoch,
    ) -> (
        impl Iterator<Item = (LocalInstanceId, Vec<UploaderImm>)> + '_,
        Option<(
            WatermarkDirection,
            impl Iterator<Item = (HummockEpoch, Vec<VnodeWatermark>)> + use<>,
            WatermarkSerdeType,
        )>,
        impl Iterator<Item = UploadingTaskId> + use<>,
        BTreeMap<HummockEpoch, ()>,
    ) {
        if let Some(prev_epoch) = self.max_sync_epoch() {
            assert_gt!(epoch, prev_epoch)
        }
        let epochs = take_before_epoch(&mut self.unsync_epochs, epoch);
        assert_eq!(
            *epochs.last_key_value().expect("non-empty").0,
            epoch,
            "{epochs:?} {epoch} {:?}",
            self.table_id
        );
        self.syncing_epochs.push_front(epoch);
        (
            self.instance_data
                .iter_mut()
                .map(move |(instance_id, data)| (*instance_id, data.sync(epoch))),
            self.table_watermarks
                .as_mut()
                .map(|(direction, watermarks, watermark_type)| {
                    let watermarks = take_before_epoch(watermarks, epoch)
                        .into_iter()
                        .map(|(epoch, (watermarks, _))| (epoch, watermarks));
                    (*direction, watermarks, *watermark_type)
                }),
            take_before_epoch(&mut self.spill_tasks, epoch)
                .into_values()
                .flat_map(|tasks| tasks.into_iter()),
            epochs,
        )
    }

    fn ack_synced(&mut self, sync_epoch: HummockEpoch) {
        let min_sync_epoch = self.syncing_epochs.pop_back().expect("should exist");
        assert_eq!(sync_epoch, min_sync_epoch);
        self.max_synced_epoch = Some(sync_epoch);
    }

    fn ack_committed(&mut self, committed_epoch: HummockEpoch) {
        let synced_epoch_advanced = {
            if let Some(max_synced_epoch) = self.max_synced_epoch
                && max_synced_epoch >= committed_epoch
            {
                false
            } else {
                true
            }
        };
        if synced_epoch_advanced {
            self.max_synced_epoch = Some(committed_epoch);
            if let Some(min_syncing_epoch) = self.syncing_epochs.back() {
                assert_gt!(*min_syncing_epoch, committed_epoch);
            }
            self.assert_after_epoch(committed_epoch);
        }
    }

    fn assert_after_epoch(&self, epoch: HummockEpoch) {
        self.instance_data
            .values()
            .for_each(|instance_data| instance_data.assert_after_epoch(epoch));
        if let Some((_, watermarks, _)) = &self.table_watermarks
            && let Some((oldest_epoch, _)) = watermarks.first_key_value()
        {
            assert_gt!(*oldest_epoch, epoch);
        }
    }

    fn max_sync_epoch(&self) -> Option<HummockEpoch> {
        self.syncing_epochs
            .front()
            .cloned()
            .or(self.max_synced_epoch)
    }

    fn max_epoch(&self) -> Option<HummockEpoch> {
        self.unsync_epochs
            .last_key_value()
            .map(|(epoch, _)| *epoch)
            .or_else(|| self.max_sync_epoch())
    }

    fn is_empty(&self) -> bool {
        self.instance_data.is_empty()
            && self.syncing_epochs.is_empty()
            && self.unsync_epochs.is_empty()
    }
}

#[derive(Eq, Hash, PartialEq, Copy, Clone)]
struct UnsyncEpochId(HummockEpoch, TableId);

impl UnsyncEpochId {
    fn epoch(&self) -> HummockEpoch {
        self.0
    }
}

fn get_unsync_epoch_id(epoch: HummockEpoch, table_ids: &HashSet<TableId>) -> Option<UnsyncEpochId> {
    table_ids
        .iter()
        .min()
        .map(|table_id| UnsyncEpochId(epoch, *table_id))
}

#[derive(Default)]
/// Unsync data, can be either imm or spilled sst, and some aggregated epoch information.
///
/// `instance_data` holds the imm of each individual local instance, and data are first added here.
/// The aggregated epoch information (table watermarks, etc.) and the spilled sst will be added to `epoch_data`.
struct UnsyncData {
    table_data: HashMap<TableId, TableUnsyncData>,
    // An index as a mapping from instance id to its table id
    instance_table_id: HashMap<LocalInstanceId, TableId>,
    unsync_epochs: HashMap<UnsyncEpochId, HashSet<TableId>>,
    spilled_data: HashMap<UploadingTaskId, (Arc<StagingSstableInfo>, HashSet<TableId>)>,
}

impl UnsyncData {
    fn init_instance(
        &mut self,
        table_id: TableId,
        instance_id: LocalInstanceId,
        init_epoch: HummockEpoch,
    ) {
        debug!(
            table_id = %table_id,
            instance_id, init_epoch, "init epoch"
        );
        let table_data = self
            .table_data
            .get_mut(&table_id)
            .unwrap_or_else(|| panic!("should exist. {table_id:?}"));
        assert!(
            table_data
                .instance_data
                .insert(
                    instance_id,
                    LocalInstanceUnsyncData::new(table_id, instance_id, init_epoch)
                )
                .is_none()
        );
        assert!(
            self.instance_table_id
                .insert(instance_id, table_id)
                .is_none()
        );
        assert!(table_data.unsync_epochs.contains_key(&init_epoch));
    }

    fn instance_data(
        &mut self,
        instance_id: LocalInstanceId,
    ) -> Option<&mut LocalInstanceUnsyncData> {
        self.instance_table_id
            .get_mut(&instance_id)
            .cloned()
            .map(move |table_id| {
                self.table_data
                    .get_mut(&table_id)
                    .expect("should exist")
                    .instance_data
                    .get_mut(&instance_id)
                    .expect("should exist")
            })
    }

    fn add_imm(&mut self, instance_id: LocalInstanceId, imm: UploaderImm) {
        self.instance_data(instance_id)
            .expect("should exist")
            .add_imm(imm);
    }

    fn local_seal_epoch(
        &mut self,
        instance_id: LocalInstanceId,
        next_epoch: HummockEpoch,
        opts: SealCurrentEpochOptions,
    ) {
        let table_id = self.instance_table_id[&instance_id];
        let table_data = self.table_data.get_mut(&table_id).expect("should exist");
        let instance_data = table_data
            .instance_data
            .get_mut(&instance_id)
            .expect("should exist");
        // When drop/cancel a streaming job, for the barrier to stop actor, the
        // local instance will call `local_seal_epoch`, but the `next_epoch` won't be
        // called `start_epoch` because we have stopped writing on it.
        let stopped = !table_data.unsync_epochs.contains_key(&next_epoch);
        let epoch = instance_data.local_seal_epoch(next_epoch, stopped);
        if let Some((direction, table_watermarks, watermark_type)) = opts.table_watermarks {
            table_data.add_table_watermarks(epoch, table_watermarks, direction, watermark_type);
        }
    }

    fn may_destroy_instance(&mut self, instance_id: LocalInstanceId) {
        if let Some(table_id) = self.instance_table_id.get(&instance_id) {
            debug!(instance_id, "destroy instance");
            let table_data = self.table_data.get_mut(table_id).expect("should exist");
            let instance_data = table_data
                .instance_data
                .get_mut(&instance_id)
                .expect("should exist");
            assert!(
                !instance_data.is_destroyed,
                "cannot destroy an instance for twice"
            );
            instance_data.is_destroyed = true;
        }
    }

    fn clear_tables(&mut self, table_ids: &HashSet<TableId>, task_manager: &mut TaskManager) {
        for table_id in table_ids {
            if let Some(table_unsync_data) = self.table_data.remove(table_id) {
                for task_id in table_unsync_data.spill_tasks.into_values().flatten() {
                    if let Some(task_status) = task_manager.abort_task(task_id) {
                        must_match!(task_status, UploadingTaskStatus::Spilling(spill_table_ids) => {
                            assert!(spill_table_ids.is_subset(table_ids));
                        });
                    }
                    if let Some((_, spill_table_ids)) = self.spilled_data.remove(&task_id) {
                        assert!(spill_table_ids.is_subset(table_ids));
                    }
                }
                assert!(
                    table_unsync_data
                        .instance_data
                        .values()
                        .all(|instance| instance.is_destroyed),
                    "should be clear when dropping the read version instance"
                );
                for instance_id in table_unsync_data.instance_data.keys() {
                    assert_eq!(
                        *table_id,
                        self.instance_table_id
                            .remove(instance_id)
                            .expect("should exist")
                    );
                }
            }
        }
        debug_assert!(
            self.spilled_data
                .values()
                .all(|(_, spill_table_ids)| spill_table_ids.is_disjoint(table_ids))
        );
        self.unsync_epochs.retain(|_, unsync_epoch_table_ids| {
            if !unsync_epoch_table_ids.is_disjoint(table_ids) {
                assert!(unsync_epoch_table_ids.is_subset(table_ids));
                false
            } else {
                true
            }
        });
        assert!(
            self.instance_table_id
                .values()
                .all(|table_id| !table_ids.contains(table_id))
        );
    }
}

impl UploaderData {
    fn sync(
        &mut self,
        context: &UploaderContext,
        sync_result_sender: oneshot::Sender<HummockResult<SyncedData>>,
        sync_table_epochs: Vec<(HummockEpoch, HashSet<TableId>)>,
    ) {
        let mut all_table_watermarks = HashMap::new();
        let mut uploading_tasks = HashSet::new();
        let mut spilled_tasks = BTreeSet::new();
        let mut all_table_ids = HashSet::new();
        let mut vector_index_adds = HashMap::new();

        let mut flush_payload = HashMap::new();

        for (epoch, table_ids) in &sync_table_epochs {
            let epoch = *epoch;
            for table_id in table_ids {
                assert!(
                    all_table_ids.insert(*table_id),
                    "duplicate sync table epoch: {:?} {:?}",
                    all_table_ids,
                    sync_table_epochs
                );
            }
            if let Some(UnsyncEpochId(_, min_table_id)) = get_unsync_epoch_id(epoch, table_ids) {
                let min_table_id_data = self
                    .unsync_data
                    .table_data
                    .get_mut(&min_table_id)
                    .expect("should exist");
                let epochs = take_before_epoch(&mut min_table_id_data.unsync_epochs.clone(), epoch);
                for epoch in epochs.keys() {
                    assert_eq!(
                        &self
                            .unsync_data
                            .unsync_epochs
                            .remove(&UnsyncEpochId(*epoch, min_table_id))
                            .expect("should exist"),
                        table_ids
                    );
                }
                for table_id in table_ids {
                    let table_data = self
                        .unsync_data
                        .table_data
                        .get_mut(table_id)
                        .expect("should exist");
                    let (unflushed_payload, table_watermarks, task_ids, table_unsync_epochs) =
                        table_data.sync(epoch);
                    assert_eq!(table_unsync_epochs, epochs, "invalid table_id {table_id}");
                    for (instance_id, payload) in unflushed_payload {
                        if !payload.is_empty() {
                            flush_payload.insert(instance_id, payload);
                        }
                    }
                    table_data.instance_data.retain(|instance_id, data| {
                        // remove the finished instances
                        if data.is_finished() {
                            assert_eq!(
                                self.unsync_data.instance_table_id.remove(instance_id),
                                Some(*table_id)
                            );
                            false
                        } else {
                            true
                        }
                    });
                    if let Some((direction, watermarks, watermark_type)) = table_watermarks {
                        Self::add_table_watermarks(
                            &mut all_table_watermarks,
                            *table_id,
                            direction,
                            watermarks,
                            watermark_type,
                        );
                    }
                    for task_id in task_ids {
                        if self.unsync_data.spilled_data.contains_key(&task_id) {
                            spilled_tasks.insert(task_id);
                        } else {
                            uploading_tasks.insert(task_id);
                        }
                    }

                    if let hash_map::Entry::Occupied(mut entry) =
                        self.unsync_vector_index_data.entry(*table_id)
                    {
                        let data = entry.get_mut();
                        let adds = take_before_epoch(&mut data.sealed_epoch_data, epoch)
                            .into_values()
                            .flatten()
                            .collect_vec();
                        if data.is_dropped && data.sealed_epoch_data.is_empty() {
                            entry.remove();
                        }
                        if !adds.is_empty() {
                            vector_index_adds
                                .try_insert(*table_id, adds)
                                .expect("non-duplicate");
                        }
                    }
                }
            }
        }

        let sync_id = {
            let sync_id = self.next_sync_id;
            self.next_sync_id += 1;
            SyncId(sync_id)
        };

        if let Some(extra_flush_task_id) = self.task_manager.sync(
            context,
            sync_id,
            flush_payload,
            uploading_tasks.iter().cloned(),
            &all_table_ids,
        ) {
            uploading_tasks.insert(extra_flush_task_id);
        }

        // iter from large task_id to small one so that newer data at the front
        let uploaded = spilled_tasks
            .iter()
            .rev()
            .map(|task_id| {
                let (sst, spill_table_ids) = self
                    .unsync_data
                    .spilled_data
                    .remove(task_id)
                    .expect("should exist");
                assert!(
                    spill_table_ids.is_subset(&all_table_ids),
                    "spilled tabled ids {:?} not a subset of sync table id {:?}",
                    spill_table_ids,
                    all_table_ids
                );
                sst
            })
            .collect();

        self.syncing_data.insert(
            sync_id,
            SyncingData {
                sync_table_epochs,
                remaining_uploading_tasks: uploading_tasks,
                uploaded,
                table_watermarks: all_table_watermarks,
                vector_index_adds,
                sync_result_sender,
            },
        );

        self.check_upload_task_consistency();
    }
}

impl UnsyncData {
    fn ack_flushed(&mut self, sstable_info: &StagingSstableInfo) {
        for (instance_id, imm_ids) in sstable_info.imm_ids() {
            if let Some(instance_data) = self.instance_data(*instance_id) {
                // take `rev` to let old imm id goes first
                instance_data.ack_flushed(imm_ids.iter().rev().cloned());
            }
        }
    }
}

struct SyncingData {
    sync_table_epochs: Vec<(HummockEpoch, HashSet<TableId>)>,
    remaining_uploading_tasks: HashSet<UploadingTaskId>,
    // newer data at the front
    uploaded: VecDeque<Arc<StagingSstableInfo>>,
    table_watermarks: HashMap<TableId, TableWatermarks>,
    vector_index_adds: HashMap<TableId, Vec<VectorIndexAdd>>,
    sync_result_sender: oneshot::Sender<HummockResult<SyncedData>>,
}

#[derive(Debug)]
pub struct SyncedData {
    pub uploaded_ssts: VecDeque<Arc<StagingSstableInfo>>,
    pub table_watermarks: HashMap<TableId, TableWatermarks>,
    pub vector_index_adds: HashMap<TableId, Vec<VectorIndexAdd>>,
}

struct UploaderContext {
    pinned_version: PinnedVersion,
    /// When called, it will spawn a task to flush the imm into sst and return the join handle.
    spawn_upload_task: SpawnUploadTask,
    buffer_tracker: BufferTracker,

    stats: Arc<HummockStateStoreMetrics>,
}

impl UploaderContext {
    fn new(
        pinned_version: PinnedVersion,
        spawn_upload_task: SpawnUploadTask,
        buffer_tracker: BufferTracker,
        stats: Arc<HummockStateStoreMetrics>,
    ) -> Self {
        UploaderContext {
            pinned_version,
            spawn_upload_task,
            buffer_tracker,
            stats,
        }
    }
}

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord, Copy, Clone, Debug)]
struct SyncId(usize);

struct UnsyncVectorIndexData {
    sealed_epoch_data: BTreeMap<HummockEpoch, Option<VectorIndexAdd>>,
    curr_epoch: u64,
    is_dropped: bool,
}

#[derive(Default)]
struct UploaderData {
    unsync_data: UnsyncData,
    unsync_vector_index_data: HashMap<TableId, UnsyncVectorIndexData>,

    syncing_data: BTreeMap<SyncId, SyncingData>,

    task_manager: TaskManager,
    next_sync_id: usize,
}

impl UploaderData {
    fn abort(self, err: impl Fn() -> HummockError) {
        self.task_manager.abort_all_tasks();
        for syncing_data in self.syncing_data.into_values() {
            send_sync_result(syncing_data.sync_result_sender, Err(err()));
        }
    }

    fn clear_tables(&mut self, table_ids: HashSet<TableId>) {
        if table_ids.is_empty() {
            return;
        }
        self.unsync_data
            .clear_tables(&table_ids, &mut self.task_manager);
        self.syncing_data.retain(|sync_id, syncing_data| {
            if syncing_data
                .sync_table_epochs
                .iter()
                .any(|(_, sync_table_ids)| !sync_table_ids.is_disjoint(&table_ids))
            {
                assert!(
                    syncing_data
                        .sync_table_epochs
                        .iter()
                        .all(|(_, sync_table_ids)| sync_table_ids.is_subset(&table_ids))
                );
                for task_id in &syncing_data.remaining_uploading_tasks {
                    match self
                        .task_manager
                        .abort_task(*task_id)
                        .expect("should exist")
                    {
                        UploadingTaskStatus::Spilling(spill_table_ids) => {
                            assert!(spill_table_ids.is_subset(&table_ids));
                        }
                        UploadingTaskStatus::Sync(task_sync_id) => {
                            assert_eq!(sync_id, &task_sync_id);
                        }
                    }
                }
                false
            } else {
                true
            }
        });

        self.check_upload_task_consistency();
    }

    fn min_uncommitted_object_id(&self) -> Option<HummockRawObjectId> {
        self.unsync_data
            .spilled_data
            .values()
            .map(|(s, _)| s)
            .chain(self.syncing_data.values().flat_map(|s| s.uploaded.iter()))
            .filter_map(|s| {
                s.sstable_infos()
                    .iter()
                    .chain(s.old_value_sstable_infos())
                    .map(|s| s.sst_info.object_id)
                    .min()
            })
            .min()
            .map(|object_id| object_id.as_raw())
    }
}

struct ErrState {
    failed_sync_table_epochs: Vec<(HummockEpoch, HashSet<TableId>)>,
    reason: String,
}

enum UploaderState {
    Working(UploaderData),
    Err(ErrState),
}

/// An uploader for hummock data.
///
/// Data have 3 sequential stages: unsync (inside each local instance, data can be unsealed, sealed), syncing, synced.
///
/// The 3 stages are divided by 2 marginal epochs: `max_syncing_epoch`,
/// `max_synced_epoch` in each `TableUnSyncData`. Epochs satisfy the following inequality.
///
/// (epochs of `synced_data`) <= `max_synced_epoch` < (epochs of `syncing_data`) <=
/// `max_syncing_epoch` < (epochs of `unsync_data`)
///
/// Data are mostly stored in `VecDeque`, and the order stored in the `VecDeque` indicates the data
/// order. Data at the front represents ***newer*** data.
pub struct HummockUploader {
    state: UploaderState,

    context: UploaderContext,
}

impl HummockUploader {
    pub(super) fn new(
        state_store_metrics: Arc<HummockStateStoreMetrics>,
        pinned_version: PinnedVersion,
        spawn_upload_task: SpawnUploadTask,
        buffer_tracker: BufferTracker,
    ) -> Self {
        Self {
            state: UploaderState::Working(UploaderData::default()),
            context: UploaderContext::new(
                pinned_version,
                spawn_upload_task,
                buffer_tracker,
                state_store_metrics,
            ),
        }
    }

    pub(super) fn buffer_tracker(&self) -> &BufferTracker {
        &self.context.buffer_tracker
    }

    pub(super) fn hummock_version(&self) -> &PinnedVersion {
        &self.context.pinned_version
    }

    pub(super) fn add_imms(
        &mut self,
        instance_id: LocalInstanceId,
        imms: Vec<(ImmutableMemtable, MemoryTracker)>,
    ) {
        let UploaderState::Working(data) = &mut self.state else {
            return;
        };
        for (imm, tracker) in imms {
            let imm = UploaderImm::new(imm, &self.context, tracker);
            data.unsync_data.add_imm(instance_id, imm);
        }
    }

    pub(super) fn init_instance(
        &mut self,
        instance_id: LocalInstanceId,
        table_id: TableId,
        init_epoch: HummockEpoch,
    ) {
        let UploaderState::Working(data) = &mut self.state else {
            return;
        };
        data.unsync_data
            .init_instance(table_id, instance_id, init_epoch);
    }

    pub(super) fn local_seal_epoch(
        &mut self,
        instance_id: LocalInstanceId,
        next_epoch: HummockEpoch,
        opts: SealCurrentEpochOptions,
    ) {
        let UploaderState::Working(data) = &mut self.state else {
            return;
        };
        data.unsync_data
            .local_seal_epoch(instance_id, next_epoch, opts);
    }

    pub(super) fn register_vector_writer(&mut self, table_id: TableId, init_epoch: HummockEpoch) {
        let UploaderState::Working(data) = &mut self.state else {
            return;
        };
        assert!(
            data.unsync_vector_index_data
                .try_insert(
                    table_id,
                    UnsyncVectorIndexData {
                        sealed_epoch_data: Default::default(),
                        curr_epoch: init_epoch,
                        is_dropped: false,
                    }
                )
                .is_ok(),
            "duplicate vector writer on {}",
            table_id
        )
    }

    pub(super) fn vector_writer_seal_epoch(
        &mut self,
        table_id: TableId,
        next_epoch: HummockEpoch,
        add: Option<VectorIndexAdd>,
    ) {
        let UploaderState::Working(data) = &mut self.state else {
            return;
        };
        let data = data
            .unsync_vector_index_data
            .get_mut(&table_id)
            .expect("should exist");
        assert!(!data.is_dropped);
        assert!(
            data.curr_epoch < next_epoch,
            "next epoch {} should be greater than current epoch {}",
            next_epoch,
            data.curr_epoch
        );
        data.sealed_epoch_data
            .try_insert(data.curr_epoch, add)
            .expect("non-duplicate");
        data.curr_epoch = next_epoch;
    }

    pub(super) fn drop_vector_writer(&mut self, table_id: TableId) {
        let UploaderState::Working(data) = &mut self.state else {
            return;
        };
        let hash_map::Entry::Occupied(mut entry) = data.unsync_vector_index_data.entry(table_id)
        else {
            panic!("vector writer {} should exist", table_id);
        };
        let data = entry.get_mut();
        data.is_dropped = true;
        if data.sealed_epoch_data.is_empty() {
            entry.remove();
        }
    }

    pub(super) fn start_epoch(&mut self, epoch: HummockEpoch, table_ids: HashSet<TableId>) {
        let UploaderState::Working(data) = &mut self.state else {
            return;
        };
        debug!(epoch, ?table_ids, "start epoch");
        for table_id in &table_ids {
            let table_data = data
                .unsync_data
                .table_data
                .entry(*table_id)
                .or_insert_with(|| {
                    TableUnsyncData::new(
                        *table_id,
                        self.context.pinned_version.table_committed_epoch(*table_id),
                    )
                });
            table_data.new_epoch(epoch);
        }
        if let Some(unsync_epoch_id) = get_unsync_epoch_id(epoch, &table_ids) {
            assert!(
                data.unsync_data
                    .unsync_epochs
                    .insert(unsync_epoch_id, table_ids)
                    .is_none()
            );
        }
    }

    pub(super) fn start_sync_epoch(
        &mut self,
        sync_result_sender: oneshot::Sender<HummockResult<SyncedData>>,
        sync_table_epochs: Vec<(HummockEpoch, HashSet<TableId>)>,
    ) {
        let data = match &mut self.state {
            UploaderState::Working(data) => data,
            UploaderState::Err(ErrState {
                failed_sync_table_epochs,
                reason,
            }) => {
                let result = Err(HummockError::other(format!(
                    "previous sync epoch {:?} failed due to [{}]",
                    failed_sync_table_epochs, reason
                )));
                send_sync_result(sync_result_sender, result);
                return;
            }
        };
        debug!(?sync_table_epochs, "start sync epoch");

        data.sync(&self.context, sync_result_sender, sync_table_epochs);

        data.may_notify_sync_task(&self.context);

        self.context
            .stats
            .uploader_syncing_epoch_count
            .set(data.syncing_data.len() as _);
    }

    pub(crate) fn update_pinned_version(&mut self, pinned_version: PinnedVersion) {
        if let UploaderState::Working(data) = &mut self.state {
            // TODO: may only `ack_committed` on table whose `committed_epoch` is changed.
            for (table_id, info) in pinned_version.state_table_info.info() {
                if let Some(table_data) = data.unsync_data.table_data.get_mut(table_id) {
                    table_data.ack_committed(info.committed_epoch);
                }
            }
        }
        self.context.pinned_version = pinned_version;
    }

    pub(crate) fn may_flush(&mut self, spiller_latency: &Histogram) -> bool {
        let UploaderState::Working(data) = &mut self.state else {
            return false;
        };
        if self.context.buffer_tracker.need_flush() {
            let timer = spiller_latency.start_timer();
            let mut spiller = Spiller::new(&mut data.unsync_data);
            let mut curr_batch_flush_size = 0;
            // iterate from older epoch to newer epoch
            while self
                .context
                .buffer_tracker
                .need_more_flush(curr_batch_flush_size)
                && let Some((epoch, payload, spilled_table_ids)) = spiller.next_spilled_payload()
            {
                assert!(!payload.is_empty());
                {
                    let (task_id, task_size, spilled_table_ids) =
                        data.task_manager
                            .spill(&self.context, spilled_table_ids, payload);
                    for table_id in spilled_table_ids {
                        spiller
                            .unsync_data()
                            .table_data
                            .get_mut(table_id)
                            .expect("should exist")
                            .spill_tasks
                            .entry(epoch)
                            .or_default()
                            .push_front(task_id);
                    }
                    curr_batch_flush_size += task_size;
                }
            }
            timer.observe_duration();
            data.check_upload_task_consistency();
            curr_batch_flush_size > 0
        } else {
            false
        }
    }

    pub(crate) fn clear(&mut self, table_ids: Option<HashSet<TableId>>) {
        if let Some(table_ids) = table_ids {
            if let UploaderState::Working(data) = &mut self.state {
                data.clear_tables(table_ids);
            }
        } else {
            if let UploaderState::Working(data) = replace(
                &mut self.state,
                UploaderState::Working(UploaderData::default()),
            ) {
                data.abort(|| HummockError::other("uploader is reset"));
            }

            self.context.stats.uploader_syncing_epoch_count.set(0);
        }
    }

    pub(crate) fn may_destroy_instance(&mut self, instance_id: LocalInstanceId) {
        let UploaderState::Working(data) = &mut self.state else {
            return;
        };
        data.unsync_data.may_destroy_instance(instance_id);
    }

    pub(crate) fn min_uncommitted_object_id(&self) -> Option<HummockRawObjectId> {
        if let UploaderState::Working(ref u) = self.state {
            u.min_uncommitted_object_id()
        } else {
            None
        }
    }
}

impl UploaderData {
    fn may_notify_sync_task(&mut self, context: &UploaderContext) {
        while let Some((_, syncing_data)) = self.syncing_data.first_key_value()
            && syncing_data.remaining_uploading_tasks.is_empty()
        {
            let (_, syncing_data) = self.syncing_data.pop_first().expect("non-empty");
            let SyncingData {
                sync_table_epochs,
                remaining_uploading_tasks: _,
                uploaded,
                table_watermarks,
                vector_index_adds,
                sync_result_sender,
            } = syncing_data;
            context
                .stats
                .uploader_syncing_epoch_count
                .set(self.syncing_data.len() as _);

            for (sync_epoch, table_ids) in sync_table_epochs {
                for table_id in table_ids {
                    if let Some(table_data) = self.unsync_data.table_data.get_mut(&table_id) {
                        table_data.ack_synced(sync_epoch);
                        if table_data.is_empty() {
                            self.unsync_data.table_data.remove(&table_id);
                        }
                    }
                }
            }

            send_sync_result(
                sync_result_sender,
                Ok(SyncedData {
                    uploaded_ssts: uploaded,
                    table_watermarks,
                    vector_index_adds,
                }),
            )
        }
    }

    fn check_upload_task_consistency(&self) {
        #[cfg(debug_assertions)]
        {
            let mut spill_task_table_id_from_data: HashMap<_, HashSet<_>> = HashMap::new();
            for table_data in self.unsync_data.table_data.values() {
                for task_id in table_data
                    .spill_tasks
                    .values()
                    .flat_map(|tasks| tasks.iter())
                {
                    assert!(
                        spill_task_table_id_from_data
                            .entry(*task_id)
                            .or_default()
                            .insert(table_data.table_id)
                    );
                }
            }
            let syncing_task_id_from_data: HashMap<_, HashSet<_>> = self
                .syncing_data
                .iter()
                .filter_map(|(sync_id, data)| {
                    if data.remaining_uploading_tasks.is_empty() {
                        None
                    } else {
                        Some((*sync_id, data.remaining_uploading_tasks.clone()))
                    }
                })
                .collect();

            let mut spill_task_table_id_from_manager: HashMap<_, HashSet<_>> = HashMap::new();
            for (task_id, (_, table_ids)) in &self.unsync_data.spilled_data {
                spill_task_table_id_from_manager.insert(*task_id, table_ids.clone());
            }
            let mut syncing_task_from_manager: HashMap<_, HashSet<_>> = HashMap::new();
            for (task_id, status) in self.task_manager.tasks() {
                match status {
                    UploadingTaskStatus::Spilling(table_ids) => {
                        assert!(
                            spill_task_table_id_from_manager
                                .insert(task_id, table_ids.clone())
                                .is_none()
                        );
                    }
                    UploadingTaskStatus::Sync(sync_id) => {
                        assert!(
                            syncing_task_from_manager
                                .entry(*sync_id)
                                .or_default()
                                .insert(task_id)
                        );
                    }
                }
            }
            assert_eq!(
                spill_task_table_id_from_data,
                spill_task_table_id_from_manager
            );
            assert_eq!(syncing_task_id_from_data, syncing_task_from_manager);
        }
    }
}

impl HummockUploader {
    pub(super) fn next_uploaded_ssts(
        &mut self,
    ) -> impl Future<Output = Vec<Arc<StagingSstableInfo>>> + '_ {
        poll_fn(|cx| {
            let UploaderState::Working(data) = &mut self.state else {
                return Poll::Pending;
            };

            const MAX_BATCH_SIZE: usize = 16;

            let mut ssts = None;
            while let Poll::Ready(Some((task_id, status, result))) =
                data.task_manager.poll_task_result(cx)
            {
                match result {
                    Ok(sst) => {
                        data.unsync_data.ack_flushed(&sst);
                        match status {
                            UploadingTaskStatus::Sync(sync_id) => {
                                let syncing_data =
                                    data.syncing_data.get_mut(&sync_id).expect("should exist");
                                syncing_data.uploaded.push_front(sst.clone());
                                assert!(syncing_data.remaining_uploading_tasks.remove(&task_id));
                                data.may_notify_sync_task(&self.context);
                            }
                            UploadingTaskStatus::Spilling(table_ids) => {
                                data.unsync_data
                                    .spilled_data
                                    .insert(task_id, (sst.clone(), table_ids));
                            }
                        }
                        data.check_upload_task_consistency();
                        let ssts = ssts.get_or_insert_with(|| Vec::with_capacity(MAX_BATCH_SIZE));
                        ssts.push(sst);
                        if ssts.len() >= MAX_BATCH_SIZE {
                            break;
                        }
                    }
                    Err((sync_id, e)) => {
                        let syncing_data =
                            data.syncing_data.remove(&sync_id).expect("should exist");
                        let failed_epochs = syncing_data.sync_table_epochs.clone();
                        let data = must_match!(replace(
                            &mut self.state,
                            UploaderState::Err(ErrState {
                                failed_sync_table_epochs: syncing_data.sync_table_epochs,
                                reason: e.as_report().to_string(),
                            }),
                        ), UploaderState::Working(data) => data);

                        let _ = syncing_data
                            .sync_result_sender
                            .send(Err(HummockError::other(format!(
                                "failed to sync: {:?}",
                                e.as_report()
                            ))));

                        data.abort(|| {
                            HummockError::other(format!(
                                "previous epoch {:?} failed to sync",
                                failed_epochs
                            ))
                        });
                        return Poll::Pending;
                    }
                }
            }
            if let Some(ssts) = ssts {
                Poll::Ready(ssts)
            } else {
                Poll::Pending
            }
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{HashMap, HashSet};
    use std::future::{Future, poll_fn};
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use std::task::Poll;

    use futures::FutureExt;
    use risingwave_common::catalog::TableId;
    use risingwave_common::metrics::LabelGuardedHistogram;
    use risingwave_common::util::epoch::EpochExt;
    use risingwave_hummock_sdk::HummockEpoch;
    use risingwave_hummock_sdk::vector_index::{
        FlatIndexAdd, VectorFileInfo, VectorIndexAdd, VectorStoreInfoDelta,
    };
    use tokio::sync::oneshot;

    use super::test_utils::*;
    use crate::hummock::event_handler::uploader::{
        HummockUploader, SyncedData, UploadingTask, get_payload_imm_ids,
    };
    use crate::hummock::event_handler::{LocalInstanceId, TEST_LOCAL_INSTANCE_ID};
    use crate::hummock::utils::MemoryTracker;
    use crate::hummock::{HummockError, HummockResult};
    use crate::mem_table::ImmutableMemtable;
    use crate::opts::StorageOpts;

    impl HummockUploader {
        pub(super) fn add_imm(
            &mut self,
            instance_id: LocalInstanceId,
            imm: (ImmutableMemtable, MemoryTracker),
        ) {
            self.add_imms(instance_id, vec![imm]);
        }

        pub(super) fn start_single_epoch_sync(
            &mut self,
            epoch: HummockEpoch,
            sync_result_sender: oneshot::Sender<HummockResult<SyncedData>>,
            table_ids: HashSet<TableId>,
        ) {
            self.start_sync_epoch(sync_result_sender, vec![(epoch, table_ids)]);
        }

        pub(super) fn may_flush_for_test(&mut self) -> bool {
            self.may_flush(&LabelGuardedHistogram::test_histogram::<1>())
        }
    }

    #[tokio::test]
    pub async fn test_uploading_task_future() {
        let uploader_context = test_uploader_context(dummy_success_upload_future);

        let imm = gen_imm(INITIAL_EPOCH);
        let imm_size = imm.size();
        let imm_ids = get_imm_ids(vec![&imm]);
        let mut task = UploadingTask::from_vec(vec![imm], &uploader_context);
        assert_eq!(imm_size, task.task_info.task_size);
        assert_eq!(imm_ids, task.task_info.imm_ids);
        assert_eq!(vec![INITIAL_EPOCH], task.task_info.epochs);
        let output = poll_fn(|cx| task.poll_result(cx)).await.unwrap();
        assert_eq!(
            output.sstable_infos(),
            &dummy_success_upload_output().new_value_ssts
        );
        assert_eq!(imm_size, output.imm_size());
        assert_eq!(&imm_ids, output.imm_ids());
        assert_eq!(&vec![INITIAL_EPOCH], output.epochs());

        let uploader_context = test_uploader_context(dummy_fail_upload_future);
        let imm = gen_imm(INITIAL_EPOCH);
        let mut task = UploadingTask::from_vec(vec![imm], &uploader_context);
        let _ = poll_fn(|cx| task.poll_result(cx)).await.unwrap_err();
    }

    #[tokio::test]
    pub async fn test_uploading_task_poll_result() {
        let uploader_context = test_uploader_context(dummy_success_upload_future);
        let mut task = UploadingTask::from_vec(vec![gen_imm(INITIAL_EPOCH)], &uploader_context);
        let output = poll_fn(|cx| task.poll_result(cx)).await.unwrap();
        assert_eq!(
            output.sstable_infos(),
            &dummy_success_upload_output().new_value_ssts
        );

        let uploader_context = test_uploader_context(dummy_fail_upload_future);
        let mut task = UploadingTask::from_vec(vec![gen_imm(INITIAL_EPOCH)], &uploader_context);
        let _ = poll_fn(|cx| task.poll_result(cx)).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_uploading_task_poll_ok_with_retry() {
        let run_count = Arc::new(AtomicUsize::new(0));
        let fail_num = 10;
        let run_count_clone = run_count.clone();
        let uploader_context = test_uploader_context(move |payload, info| {
            let run_count = run_count.clone();
            async move {
                // fail in the first `fail_num` run, and success at the end
                let ret = if run_count.load(SeqCst) < fail_num {
                    Err(HummockError::other("fail"))
                } else {
                    dummy_success_upload_future(payload, info).await
                };
                run_count.fetch_add(1, SeqCst);
                ret
            }
        });
        let mut task = UploadingTask::from_vec(vec![gen_imm(INITIAL_EPOCH)], &uploader_context);
        let output = poll_fn(|cx| task.poll_ok_with_retry(cx)).await;
        assert_eq!(fail_num + 1, run_count_clone.load(SeqCst));
        assert_eq!(
            output.sstable_infos(),
            &dummy_success_upload_output().new_value_ssts
        );
    }

    #[tokio::test]
    async fn test_uploader_basic() {
        let mut uploader = test_uploader(dummy_success_upload_future);
        let mut sst_collector = UploadedSstCollector::default();
        let epoch1 = INITIAL_EPOCH.next_epoch();
        const VECTOR_INDEX_TABLE_ID: TableId = TableId::new(234);
        uploader.start_epoch(
            epoch1,
            HashSet::from_iter([TEST_TABLE_ID, VECTOR_INDEX_TABLE_ID]),
        );
        let (imm, tracker) = gen_imm_with_unlimit(epoch1);
        uploader.init_instance(TEST_LOCAL_INSTANCE_ID, TEST_TABLE_ID, epoch1);
        uploader.add_imm(TEST_LOCAL_INSTANCE_ID, (imm.clone(), tracker));
        uploader.local_seal_epoch_for_test(TEST_LOCAL_INSTANCE_ID, epoch1);

        uploader.register_vector_writer(VECTOR_INDEX_TABLE_ID, epoch1);
        let vector_info_file = VectorFileInfo {
            object_id: 1.into(),
            vector_count: 1,
            file_size: 0,
            start_vector_id: 0,
            meta_offset: 20,
        };
        let vector_index_add = VectorIndexAdd::Flat(FlatIndexAdd {
            vector_store_info_delta: VectorStoreInfoDelta {
                next_vector_id: 1,
                added_vector_files: vec![vector_info_file.clone()],
            },
        });
        uploader.vector_writer_seal_epoch(
            VECTOR_INDEX_TABLE_ID,
            epoch1.next_epoch(),
            Some(vector_index_add.clone()),
        );

        let (sync_tx, sync_rx) = oneshot::channel();
        uploader.start_single_epoch_sync(
            epoch1,
            sync_tx,
            HashSet::from_iter([TEST_TABLE_ID, VECTOR_INDEX_TABLE_ID]),
        );
        assert_eq!(epoch1 as HummockEpoch, uploader.test_max_syncing_epoch());
        assert_eq!(1, uploader.data().syncing_data.len());
        let (_, syncing_data) = uploader.data().syncing_data.first_key_value().unwrap();
        assert_eq!(epoch1 as HummockEpoch, syncing_data.sync_table_epochs[0].0);
        assert!(syncing_data.uploaded.is_empty());
        assert!(!syncing_data.remaining_uploading_tasks.is_empty());

        let staging_sst = sst_collector.next(&mut uploader).await;
        assert_eq!(&vec![epoch1], staging_sst.epochs());
        assert_eq!(
            &HashMap::from_iter([(TEST_LOCAL_INSTANCE_ID, vec![imm.batch_id()])]),
            staging_sst.imm_ids()
        );
        assert_eq!(
            &dummy_success_upload_output().new_value_ssts,
            staging_sst.sstable_infos()
        );

        match sync_rx.await {
            Ok(Ok(data)) => {
                let SyncedData {
                    uploaded_ssts,
                    table_watermarks,
                    vector_index_adds,
                } = data;
                assert_eq!(1, uploaded_ssts.len());
                let staging_sst = &uploaded_ssts[0];
                assert_eq!(&vec![epoch1], staging_sst.epochs());
                assert_eq!(
                    &HashMap::from_iter([(TEST_LOCAL_INSTANCE_ID, vec![imm.batch_id()])]),
                    staging_sst.imm_ids()
                );
                assert_eq!(
                    &dummy_success_upload_output().new_value_ssts,
                    staging_sst.sstable_infos()
                );
                assert!(table_watermarks.is_empty());
                assert_eq!(vector_index_adds.len(), 1);
                let (table_id, vector_index_adds) = vector_index_adds.into_iter().next().unwrap();
                assert_eq!(table_id, VECTOR_INDEX_TABLE_ID);
                assert_eq!(vector_index_adds.len(), 1);
                let synced_vector_index_add = vector_index_adds[0].clone();
                assert_eq!(vector_index_add, synced_vector_index_add);
            }
            _ => unreachable!(),
        };
        assert_eq!(epoch1, uploader.test_max_synced_epoch());

        let new_pinned_version = uploader
            .context
            .pinned_version
            .new_pin_version(test_hummock_version(epoch1))
            .unwrap();
        uploader.update_pinned_version(new_pinned_version);
        assert_eq!(
            epoch1,
            uploader
                .context
                .pinned_version
                .table_committed_epoch(TEST_TABLE_ID)
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_uploader_destroy_instance_before_sync() {
        let mut uploader = test_uploader(dummy_success_upload_future);
        let mut sst_collector = UploadedSstCollector::default();
        let epoch1 = INITIAL_EPOCH.next_epoch();
        uploader.start_epochs_for_test([epoch1]);
        let (imm, tracker) = gen_imm_with_unlimit(epoch1);
        uploader.init_instance(TEST_LOCAL_INSTANCE_ID, TEST_TABLE_ID, epoch1);
        uploader.add_imm(TEST_LOCAL_INSTANCE_ID, (imm.clone(), tracker));
        uploader.local_seal_epoch_for_test(TEST_LOCAL_INSTANCE_ID, epoch1);
        uploader.may_destroy_instance(TEST_LOCAL_INSTANCE_ID);

        let (sync_tx, sync_rx) = oneshot::channel();
        uploader.start_single_epoch_sync(epoch1, sync_tx, HashSet::from_iter([TEST_TABLE_ID]));
        assert_eq!(epoch1 as HummockEpoch, uploader.test_max_syncing_epoch());
        assert_eq!(1, uploader.data().syncing_data.len());
        let (_, syncing_data) = uploader.data().syncing_data.first_key_value().unwrap();
        assert_eq!(epoch1 as HummockEpoch, syncing_data.sync_table_epochs[0].0);
        assert!(syncing_data.uploaded.is_empty());
        assert!(!syncing_data.remaining_uploading_tasks.is_empty());

        let staging_sst = sst_collector.next(&mut uploader).await;
        assert_eq!(&vec![epoch1], staging_sst.epochs());
        assert_eq!(
            &HashMap::from_iter([(TEST_LOCAL_INSTANCE_ID, vec![imm.batch_id()])]),
            staging_sst.imm_ids()
        );
        assert_eq!(
            &dummy_success_upload_output().new_value_ssts,
            staging_sst.sstable_infos()
        );

        match sync_rx.await {
            Ok(Ok(data)) => {
                let SyncedData {
                    uploaded_ssts,
                    table_watermarks,
                    ..
                } = data;
                assert_eq!(1, uploaded_ssts.len());
                let staging_sst = &uploaded_ssts[0];
                assert_eq!(&vec![epoch1], staging_sst.epochs());
                assert_eq!(
                    &HashMap::from_iter([(TEST_LOCAL_INSTANCE_ID, vec![imm.batch_id()])]),
                    staging_sst.imm_ids()
                );
                assert_eq!(
                    &dummy_success_upload_output().new_value_ssts,
                    staging_sst.sstable_infos()
                );
                assert!(table_watermarks.is_empty());
            }
            _ => unreachable!(),
        };
        assert!(
            !uploader
                .data()
                .unsync_data
                .table_data
                .contains_key(&TEST_TABLE_ID)
        );
    }

    #[tokio::test]
    async fn test_empty_uploader_sync() {
        let mut uploader = test_uploader(dummy_success_upload_future);
        let epoch1 = INITIAL_EPOCH.next_epoch();

        let (sync_tx, sync_rx) = oneshot::channel();
        uploader.start_epochs_for_test([epoch1]);
        uploader.init_instance(TEST_LOCAL_INSTANCE_ID, TEST_TABLE_ID, epoch1);
        uploader.local_seal_epoch_for_test(TEST_LOCAL_INSTANCE_ID, epoch1);
        uploader.start_single_epoch_sync(epoch1, sync_tx, HashSet::from_iter([TEST_TABLE_ID]));
        assert_eq!(epoch1, uploader.test_max_syncing_epoch());

        assert_uploader_pending(&mut uploader).await;

        match sync_rx.await {
            Ok(Ok(data)) => {
                assert!(data.uploaded_ssts.is_empty());
            }
            _ => unreachable!(),
        };
        assert_eq!(epoch1, uploader.test_max_synced_epoch());
        let new_pinned_version = uploader
            .context
            .pinned_version
            .new_pin_version(test_hummock_version(epoch1))
            .unwrap();
        uploader.update_pinned_version(new_pinned_version);
        assert!(uploader.data().syncing_data.is_empty());
        assert_eq!(
            epoch1,
            uploader
                .context
                .pinned_version
                .table_committed_epoch(TEST_TABLE_ID)
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_uploader_empty_epoch() {
        let mut uploader = test_uploader(dummy_success_upload_future);
        let epoch1 = INITIAL_EPOCH.next_epoch();
        let epoch2 = epoch1.next_epoch();
        uploader.start_epochs_for_test([epoch1, epoch2]);
        let (imm, tracker) = gen_imm_with_unlimit(epoch2);
        // epoch1 is empty while epoch2 is not. Going to seal empty epoch1.
        uploader.init_instance(TEST_LOCAL_INSTANCE_ID, TEST_TABLE_ID, epoch1);
        uploader.local_seal_epoch_for_test(TEST_LOCAL_INSTANCE_ID, epoch1);
        uploader.add_imm(TEST_LOCAL_INSTANCE_ID, (imm, tracker));

        let (sync_tx, sync_rx) = oneshot::channel();
        uploader.start_single_epoch_sync(epoch1, sync_tx, HashSet::from_iter([TEST_TABLE_ID]));
        assert_eq!(epoch1, uploader.test_max_syncing_epoch());

        assert_uploader_pending(&mut uploader).await;

        match sync_rx.await {
            Ok(Ok(data)) => {
                assert!(data.uploaded_ssts.is_empty());
            }
            _ => unreachable!(),
        };
        assert_eq!(epoch1, uploader.test_max_synced_epoch());
        let new_pinned_version = uploader
            .context
            .pinned_version
            .new_pin_version(test_hummock_version(epoch1))
            .unwrap();
        uploader.update_pinned_version(new_pinned_version);
        assert!(uploader.data().syncing_data.is_empty());
        assert_eq!(
            epoch1,
            uploader
                .context
                .pinned_version
                .table_committed_epoch(TEST_TABLE_ID)
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_uploader_poll_empty() {
        let mut uploader = test_uploader(dummy_success_upload_future);
        let fut = uploader.next_uploaded_ssts();
        let mut fut = pin!(fut);
        assert!(poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx).is_pending())).await);
    }

    #[tokio::test]
    async fn test_uploader_empty_advance_mce() {
        let mut uploader = test_uploader(dummy_success_upload_future);
        let mut sst_collector = UploadedSstCollector::default();
        let initial_pinned_version = uploader.context.pinned_version.clone();
        let epoch1 = INITIAL_EPOCH.next_epoch();
        let epoch2 = epoch1.next_epoch();
        let epoch3 = epoch2.next_epoch();
        let epoch4 = epoch3.next_epoch();
        let epoch5 = epoch4.next_epoch();
        let epoch6 = epoch5.next_epoch();
        let version1 = initial_pinned_version
            .new_pin_version(test_hummock_version(epoch1))
            .unwrap();
        let version2 = initial_pinned_version
            .new_pin_version(test_hummock_version(epoch2))
            .unwrap();
        let version3 = initial_pinned_version
            .new_pin_version(test_hummock_version(epoch3))
            .unwrap();
        let version4 = initial_pinned_version
            .new_pin_version(test_hummock_version(epoch4))
            .unwrap();
        let version5 = initial_pinned_version
            .new_pin_version(test_hummock_version(epoch5))
            .unwrap();

        uploader.start_epochs_for_test([epoch6]);
        uploader.init_instance(TEST_LOCAL_INSTANCE_ID, TEST_TABLE_ID, epoch6);

        uploader.update_pinned_version(version1);
        assert_eq!(epoch1, uploader.test_max_synced_epoch());
        assert_eq!(epoch1, uploader.test_max_syncing_epoch());

        let (imm, tracker) = gen_imm_with_unlimit(epoch6);
        uploader.add_imm(TEST_LOCAL_INSTANCE_ID, (imm.clone(), tracker));
        uploader.update_pinned_version(version2);
        assert_eq!(epoch2, uploader.test_max_synced_epoch());
        assert_eq!(epoch2, uploader.test_max_syncing_epoch());

        uploader.local_seal_epoch_for_test(TEST_LOCAL_INSTANCE_ID, epoch6);
        uploader.update_pinned_version(version3);
        assert_eq!(epoch3, uploader.test_max_synced_epoch());
        assert_eq!(epoch3, uploader.test_max_syncing_epoch());

        let (sync_tx, sync_rx) = oneshot::channel();
        uploader.start_single_epoch_sync(epoch6, sync_tx, HashSet::from_iter([TEST_TABLE_ID]));
        assert_eq!(epoch6, uploader.test_max_syncing_epoch());
        uploader.update_pinned_version(version4);
        assert_eq!(epoch4, uploader.test_max_synced_epoch());
        assert_eq!(epoch6, uploader.test_max_syncing_epoch());

        let sst = sst_collector.next(&mut uploader).await;
        assert_eq!(&get_imm_ids([&imm]), sst.imm_ids());

        match sync_rx.await {
            Ok(Ok(data)) => {
                assert!(data.table_watermarks.is_empty());
                assert_eq!(1, data.uploaded_ssts.len());
                assert_eq!(&get_imm_ids([&imm]), data.uploaded_ssts[0].imm_ids());
            }
            _ => unreachable!(),
        }

        uploader.update_pinned_version(version5);
        assert_eq!(epoch6, uploader.test_max_synced_epoch());
        assert_eq!(epoch6, uploader.test_max_syncing_epoch());
    }

    #[tokio::test]
    async fn test_uploader_finish_in_order() {
        let config = StorageOpts {
            shared_buffer_capacity_mb: 1024 * 1024,
            shared_buffer_flush_ratio: 0.0,
            ..Default::default()
        };
        let (buffer_tracker, mut uploader, new_task_notifier) =
            prepare_uploader_order_test(&config, false);
        let mut sst_collector = UploadedSstCollector::default();

        let epoch1 = INITIAL_EPOCH.next_epoch();
        let epoch2 = epoch1.next_epoch();
        let epoch3 = epoch2.next_epoch();
        let epoch4 = epoch3.next_epoch();
        uploader.start_epochs_for_test([epoch1, epoch2, epoch3, epoch4]);
        let memory_limiter = buffer_tracker.get_memory_limiter().clone();
        let memory_limiter = memory_limiter.as_ref();

        let instance_id1 = 1;
        let instance_id2 = 2;

        uploader.init_instance(instance_id1, TEST_TABLE_ID, epoch1);
        uploader.init_instance(instance_id2, TEST_TABLE_ID, epoch2);

        // imm2 contains data in newer epoch, but added first
        let (imm2, tracker2) = gen_imm_with_limiter(epoch2, memory_limiter).await;
        uploader.add_imm(instance_id2, (imm2.clone(), tracker2));
        let (imm1_1, tracker1_1) = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(instance_id1, (imm1_1.clone(), tracker1_1));
        let (imm1_2, tracker1_2) = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(instance_id1, (imm1_2.clone(), tracker1_2));

        // imm1 will be spilled first
        let epoch1_spill_payload12 =
            HashMap::from_iter([(instance_id1, vec![imm1_2.clone(), imm1_1.clone()])]);
        let epoch2_spill_payload = HashMap::from_iter([(instance_id2, vec![imm2.clone()])]);
        let (await_start1, finish_tx1) =
            new_task_notifier(get_payload_imm_ids(&epoch1_spill_payload12));
        let (await_start2, finish_tx2) =
            new_task_notifier(get_payload_imm_ids(&epoch2_spill_payload));
        uploader.may_flush_for_test();
        await_start1.await;
        await_start2.await;

        assert_uploader_pending(&mut uploader).await;

        finish_tx2.send(()).unwrap();
        assert_uploader_pending(&mut uploader).await;

        finish_tx1.send(()).unwrap();
        let sst = sst_collector.next(&mut uploader).await;
        assert_eq!(&get_payload_imm_ids(&epoch1_spill_payload12), sst.imm_ids());
        assert_eq!(&vec![epoch1], sst.epochs());

        let sst = sst_collector.next(&mut uploader).await;
        assert_eq!(&get_payload_imm_ids(&epoch2_spill_payload), sst.imm_ids());
        assert_eq!(&vec![epoch2], sst.epochs());

        let (imm1_3, tracker1_3) = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(instance_id1, (imm1_3.clone(), tracker1_3));
        let epoch1_spill_payload3 = HashMap::from_iter([(instance_id1, vec![imm1_3.clone()])]);
        let (await_start1_3, finish_tx1_3) =
            new_task_notifier(get_payload_imm_ids(&epoch1_spill_payload3));
        uploader.may_flush_for_test();
        await_start1_3.await;
        let (imm1_4, tracker1_4) = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(instance_id1, (imm1_4.clone(), tracker1_4));
        let epoch1_sync_payload = HashMap::from_iter([(instance_id1, vec![imm1_4.clone()])]);
        let (await_start1_4, finish_tx1_4) =
            new_task_notifier(get_payload_imm_ids(&epoch1_sync_payload));
        uploader.local_seal_epoch_for_test(instance_id1, epoch1);
        let (sync_tx1, sync_rx1) = oneshot::channel();
        uploader.start_single_epoch_sync(epoch1, sync_tx1, HashSet::from_iter([TEST_TABLE_ID]));
        await_start1_4.await;

        uploader.local_seal_epoch_for_test(instance_id1, epoch2);
        uploader.local_seal_epoch_for_test(instance_id2, epoch2);

        // current uploader state:
        // unsealed: empty
        // sealed: epoch2: uploaded sst([imm2])
        // syncing: epoch1: uploading: [imm1_4], [imm1_3], uploaded: sst([imm1_2, imm1_1])

        let (imm3_1, tracker3_1) = gen_imm_with_limiter(epoch3, memory_limiter).await;
        let epoch3_spill_payload1 = HashMap::from_iter([(instance_id1, vec![imm3_1.clone()])]);
        uploader.add_imm(instance_id1, (imm3_1.clone(), tracker3_1));
        let (await_start3_1, finish_tx3_1) =
            new_task_notifier(get_payload_imm_ids(&epoch3_spill_payload1));
        uploader.may_flush_for_test();
        await_start3_1.await;
        let (imm3_2, tracker3_2) = gen_imm_with_limiter(epoch3, memory_limiter).await;
        let epoch3_spill_payload2 = HashMap::from_iter([(instance_id2, vec![imm3_2.clone()])]);
        uploader.add_imm(instance_id2, (imm3_2.clone(), tracker3_2));
        let (await_start3_2, finish_tx3_2) =
            new_task_notifier(get_payload_imm_ids(&epoch3_spill_payload2));
        uploader.may_flush_for_test();
        await_start3_2.await;
        let (imm3_3, tracker3_3) = gen_imm_with_limiter(epoch3, memory_limiter).await;
        uploader.add_imm(instance_id1, (imm3_3.clone(), tracker3_3));

        // current uploader state:
        // unsealed: epoch3: imm: imm3_3, uploading: [imm3_2], [imm3_1]
        // sealed: uploaded sst([imm2])
        // syncing: epoch1: uploading: [imm1_4], [imm1_3], uploaded: sst([imm1_2, imm1_1])

        uploader.local_seal_epoch_for_test(instance_id1, epoch3);
        let (imm4, tracker4) = gen_imm_with_limiter(epoch4, memory_limiter).await;
        uploader.add_imm(instance_id1, (imm4.clone(), tracker4));
        assert_uploader_pending(&mut uploader).await;

        // current uploader state:
        // unsealed: epoch3: imm: imm3_3, uploading: [imm3_2], [imm3_1]
        //           epoch4: imm: imm4
        // sealed: uploaded sst([imm2])
        // syncing: epoch1: uploading: [imm1_4], [imm1_3], uploaded: sst([imm1_2, imm1_1])

        finish_tx3_1.send(()).unwrap();
        assert_uploader_pending(&mut uploader).await;
        finish_tx1_4.send(()).unwrap();
        assert_uploader_pending(&mut uploader).await;
        finish_tx1_3.send(()).unwrap();

        let sst = sst_collector.next(&mut uploader).await;
        assert_eq!(&get_payload_imm_ids(&epoch1_spill_payload3), sst.imm_ids());

        let sst = sst_collector.next(&mut uploader).await;
        assert_eq!(&get_payload_imm_ids(&epoch1_sync_payload), sst.imm_ids());

        match sync_rx1.await {
            Ok(Ok(data)) => {
                assert_eq!(3, data.uploaded_ssts.len());
                assert_eq!(
                    &get_payload_imm_ids(&epoch1_sync_payload),
                    data.uploaded_ssts[0].imm_ids()
                );
                assert_eq!(
                    &get_payload_imm_ids(&epoch1_spill_payload3),
                    data.uploaded_ssts[1].imm_ids()
                );
                assert_eq!(
                    &get_payload_imm_ids(&epoch1_spill_payload12),
                    data.uploaded_ssts[2].imm_ids()
                );
            }
            _ => {
                unreachable!()
            }
        }

        // current uploader state:
        // unsealed: epoch3: imm: imm3_3, uploading: [imm3_2], [imm3_1]
        //           epoch4: imm: imm4
        // sealed: uploaded sst([imm2])
        // syncing: empty
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])

        let (sync_tx2, sync_rx2) = oneshot::channel();
        uploader.start_single_epoch_sync(epoch2, sync_tx2, HashSet::from_iter([TEST_TABLE_ID]));
        uploader.local_seal_epoch_for_test(instance_id2, epoch3);
        let sst = sst_collector.next(&mut uploader).await;
        assert_eq!(&get_payload_imm_ids(&epoch3_spill_payload1), sst.imm_ids());

        match sync_rx2.await {
            Ok(Ok(data)) => {
                assert_eq!(data.uploaded_ssts.len(), 1);
                assert_eq!(
                    &get_payload_imm_ids(&epoch2_spill_payload),
                    data.uploaded_ssts[0].imm_ids()
                );
            }
            _ => {
                unreachable!("should be sync finish");
            }
        }
        assert_eq!(epoch2, uploader.test_max_synced_epoch());

        // current uploader state:
        // unsealed: epoch4: imm: imm4
        // sealed: imm: imm3_3, uploading: [imm3_2], uploaded: sst([imm3_1])
        // syncing: empty
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])
        //         epoch2: sst([imm2])

        uploader.local_seal_epoch_for_test(instance_id1, epoch4);
        uploader.local_seal_epoch_for_test(instance_id2, epoch4);
        let epoch4_sync_payload = HashMap::from_iter([(instance_id1, vec![imm4, imm3_3])]);
        let (await_start4_with_3_3, finish_tx4_with_3_3) =
            new_task_notifier(get_payload_imm_ids(&epoch4_sync_payload));
        let (sync_tx4, mut sync_rx4) = oneshot::channel();
        uploader.start_single_epoch_sync(epoch4, sync_tx4, HashSet::from_iter([TEST_TABLE_ID]));
        await_start4_with_3_3.await;

        // current uploader state:
        // unsealed: empty
        // sealed: empty
        // syncing: epoch4: uploading: [imm4, imm3_3], [imm3_2], uploaded: sst([imm3_1])
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])
        //         epoch2: sst([imm2])

        assert_uploader_pending(&mut uploader).await;

        finish_tx3_2.send(()).unwrap();
        let sst = sst_collector.next(&mut uploader).await;
        assert_eq!(&get_payload_imm_ids(&epoch3_spill_payload2), sst.imm_ids());

        finish_tx4_with_3_3.send(()).unwrap();
        assert!(poll_fn(|cx| Poll::Ready(sync_rx4.poll_unpin(cx).is_pending())).await);

        let sst = sst_collector.next(&mut uploader).await;
        assert_eq!(&get_payload_imm_ids(&epoch4_sync_payload), sst.imm_ids());

        match sync_rx4.await {
            Ok(Ok(data)) => {
                assert_eq!(3, data.uploaded_ssts.len());
                assert_eq!(
                    &get_payload_imm_ids(&epoch4_sync_payload),
                    data.uploaded_ssts[0].imm_ids()
                );
                assert_eq!(
                    &get_payload_imm_ids(&epoch3_spill_payload2),
                    data.uploaded_ssts[1].imm_ids()
                );
                assert_eq!(
                    &get_payload_imm_ids(&epoch3_spill_payload1),
                    data.uploaded_ssts[2].imm_ids(),
                )
            }
            _ => {
                unreachable!("should be sync finish");
            }
        }
        assert_eq!(epoch4, uploader.test_max_synced_epoch());

        // current uploader state:
        // unsealed: empty
        // sealed: empty
        // syncing: empty
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])
        //         epoch2: sst([imm2])
        //         epoch4: sst([imm4, imm3_3]), sst([imm3_2]), sst([imm3_1])
    }

    #[tokio::test]
    async fn test_uploader_frequently_flush() {
        let config = StorageOpts {
            shared_buffer_capacity_mb: 10,
            shared_buffer_flush_ratio: 0.12,
            // This test will fail when we set it to 0
            shared_buffer_min_batch_flush_size_mb: 1,
            ..Default::default()
        };
        let (buffer_tracker, mut uploader, _new_task_notifier) =
            prepare_uploader_order_test(&config, true);

        let epoch1 = INITIAL_EPOCH.next_epoch();
        let epoch2 = epoch1.next_epoch();
        uploader.start_epochs_for_test([epoch1, epoch2]);
        let instance_id1 = 1;
        let instance_id2 = 2;
        let flush_threshold = buffer_tracker.flush_threshold();
        let memory_limiter = buffer_tracker.get_memory_limiter().clone();
        let memory_limiter = memory_limiter.as_ref();

        uploader.init_instance(instance_id1, TEST_TABLE_ID, epoch1);
        uploader.init_instance(instance_id2, TEST_TABLE_ID, epoch2);

        // imm2 contains data in newer epoch, but added first
        let mut total_memory = 0;
        while total_memory < flush_threshold {
            let (imm, tracker) = gen_imm_with_limiter(epoch2, memory_limiter).await;
            total_memory += imm.size();
            if total_memory > flush_threshold {
                break;
            }
            uploader.add_imm(instance_id2, (imm, tracker));
        }
        let (imm, tracker) = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(instance_id1, (imm, tracker));
        assert!(uploader.may_flush_for_test());

        for _ in 0..10 {
            let (imm, tracker) = gen_imm_with_limiter(epoch1, memory_limiter).await;
            uploader.add_imm(instance_id1, (imm, tracker));
            assert!(!uploader.may_flush_for_test());
        }
    }
}
