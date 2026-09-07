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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::mem::replace;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RawRwLock;
use parking_lot::lock_api::RwLockReadGuard;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{DatabaseId, FragmentTypeFlag, FragmentTypeMask, TableId};
use risingwave_common::id::JobId;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_mut;
use risingwave_connector::source::{SplitImpl, SplitMetaData};
use risingwave_meta_model::WorkerId;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_pb::ddl_service::PbBackfillType;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::id::SubscriberId;
use risingwave_pb::meta::PbFragmentWorkerSlotMapping;
use risingwave_pb::meta::subscribe_response::Operation;
use risingwave_pb::source::PbCdcTableSnapshotSplits;
use risingwave_pb::stream_plan::PbUpstreamSinkInfo;
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use tracing::{info, warn};

use crate::barrier::cdc_progress::{CdcProgress, CdcTableBackfillTracker};
use crate::barrier::command::{
    CreateStreamingJobCommandInfo, PostCollectCommand, ReplaceStreamJobPlan, ThrottleConfigMap,
    extract_throttle_config,
};
use crate::barrier::edge_builder::{
    EdgeBuilderFragmentInfo, FragmentEdgeBuildResult, FragmentEdgeBuilder,
};
use crate::barrier::progress::{CreateMviewProgressTracker, StagingCommitInfo};
use crate::barrier::rpc::{ControlStreamManager, to_partial_graph_id};
use crate::barrier::{
    BackfillProgress, BarrierKind, CreateStreamingJobType, FragmentBackfillProgress, TracedEpoch,
};
use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::controller::utils::rebuild_fragment_mapping;
use crate::manager::NotificationManagerRef;
use crate::model::{ActorId, BackfillUpstreamType, FragmentId, StreamActor, StreamJobFragments};
use crate::stream::UpstreamSinkInfo;
use crate::{MetaError, MetaResult};

#[derive(Debug, Clone)]
pub struct SharedActorInfo {
    pub worker_id: WorkerId,
    pub vnode_bitmap: Option<Bitmap>,
    pub splits: Vec<SplitImpl>,
}

impl From<&InflightActorInfo> for SharedActorInfo {
    fn from(value: &InflightActorInfo) -> Self {
        Self {
            worker_id: value.worker_id,
            vnode_bitmap: value.vnode_bitmap.clone(),
            splits: value.splits.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SharedFragmentInfo {
    pub fragment_id: FragmentId,
    pub job_id: JobId,
    pub distribution_type: DistributionType,
    pub actors: HashMap<ActorId, SharedActorInfo>,
    pub vnode_count: usize,
    pub fragment_type_mask: FragmentTypeMask,
    pub state_table_ids: HashSet<TableId>,
}

impl From<(&InflightFragmentInfo, JobId)> for SharedFragmentInfo {
    fn from(pair: (&InflightFragmentInfo, JobId)) -> Self {
        let (info, job_id) = pair;

        let InflightFragmentInfo {
            fragment_id,
            distribution_type,
            fragment_type_mask,
            actors,
            vnode_count,
            state_table_ids,
            ..
        } = info;

        Self {
            fragment_id: *fragment_id,
            job_id,
            distribution_type: *distribution_type,
            fragment_type_mask: *fragment_type_mask,
            actors: actors
                .iter()
                .map(|(actor_id, actor)| (*actor_id, actor.into()))
                .collect(),
            vnode_count: *vnode_count,
            state_table_ids: state_table_ids.clone(),
        }
    }
}

#[derive(Default, Debug)]
pub struct SharedActorInfosInner {
    info: HashMap<DatabaseId, HashMap<FragmentId, SharedFragmentInfo>>,
}

impl SharedActorInfosInner {
    pub fn get_fragment(&self, fragment_id: FragmentId) -> Option<&SharedFragmentInfo> {
        self.info
            .values()
            .find_map(|database| database.get(&fragment_id))
    }

    pub fn iter_over_fragments(&self) -> impl Iterator<Item = (&FragmentId, &SharedFragmentInfo)> {
        self.info.values().flatten()
    }
}

#[derive(Clone, educe::Educe)]
#[educe(Debug)]
pub struct SharedActorInfos {
    inner: Arc<parking_lot::RwLock<SharedActorInfosInner>>,
    #[educe(Debug(ignore))]
    notification_manager: NotificationManagerRef,
}

impl SharedActorInfos {
    pub fn read_guard(&self) -> RwLockReadGuard<'_, RawRwLock, SharedActorInfosInner> {
        self.inner.read()
    }

    pub fn list_assignments(&self) -> HashMap<ActorId, Vec<SplitImpl>> {
        let core = self.inner.read();
        core.iter_over_fragments()
            .flat_map(|(_, fragment)| {
                fragment
                    .actors
                    .iter()
                    .map(|(actor_id, info)| (*actor_id, info.splits.clone()))
            })
            .collect()
    }

    /// Migrates splits from previous actors to the new actors for a rescheduled fragment.
    ///
    /// Very occasionally split removal may happen during scaling, in which case we need to
    /// use the old splits for reallocation instead of the latest splits (which may be missing),
    /// so that we can resolve the split removal in the next command.
    pub fn migrate_splits_for_source_actors(
        &self,
        fragment_id: FragmentId,
        prev_actor_ids: &[ActorId],
        curr_actor_ids: &[ActorId],
    ) -> MetaResult<HashMap<ActorId, Vec<SplitImpl>>> {
        let guard = self.read_guard();

        let prev_splits = prev_actor_ids
            .iter()
            .flat_map(|actor_id| {
                // Note: File Source / Iceberg Source doesn't have splits assigned by meta.
                guard
                    .get_fragment(fragment_id)
                    .and_then(|info| info.actors.get(actor_id))
                    .map(|actor| actor.splits.clone())
                    .unwrap_or_default()
            })
            .map(|split| (split.id(), split))
            .collect();

        let empty_actor_splits = curr_actor_ids
            .iter()
            .map(|actor_id| (*actor_id, vec![]))
            .collect();

        let diff = crate::stream::source_manager::reassign_splits(
            fragment_id,
            empty_actor_splits,
            &prev_splits,
            // pre-allocate splits is the first time getting splits, and it does not have scale-in scene
            std::default::Default::default(),
        )
        .unwrap_or_default();

        Ok(diff)
    }
}

impl SharedActorInfos {
    pub(crate) fn new(notification_manager: NotificationManagerRef) -> Self {
        Self {
            inner: Arc::new(Default::default()),
            notification_manager,
        }
    }

    pub(super) fn remove_database(&self, database_id: DatabaseId) {
        if let Some(database) = self.inner.write().info.remove(&database_id) {
            let mapping = database
                .into_values()
                .map(|fragment| rebuild_fragment_mapping(&fragment))
                .collect_vec();
            if !mapping.is_empty() {
                self.notification_manager
                    .notify_streaming_fragment_mapping(Operation::Delete, mapping);
            }
        }
    }

    pub(super) fn retain_databases(&self, database_ids: impl IntoIterator<Item = DatabaseId>) {
        let database_ids: HashSet<_> = database_ids.into_iter().collect();

        let mut mapping = Vec::new();
        for fragment in self
            .inner
            .write()
            .info
            .extract_if(|database_id, _| !database_ids.contains(database_id))
            .flat_map(|(_, fragments)| fragments.into_values())
        {
            mapping.push(rebuild_fragment_mapping(&fragment));
        }
        if !mapping.is_empty() {
            self.notification_manager
                .notify_streaming_fragment_mapping(Operation::Delete, mapping);
        }
    }

    pub(super) fn recover_database(
        &self,
        database_id: DatabaseId,
        fragments: impl Iterator<Item = (&InflightFragmentInfo, JobId)>,
    ) {
        let mut remaining_fragments: HashMap<_, _> = fragments
            .map(|info @ (fragment, _)| (fragment.fragment_id, info))
            .collect();
        // delete the fragments that exist previously, but not included in the recovered fragments
        let mut writer = self.start_writer(database_id);
        let database = writer.write_guard.info.entry(database_id).or_default();
        for (_, fragment) in database.extract_if(|fragment_id, fragment_infos| {
            if let Some(info) = remaining_fragments.remove(fragment_id) {
                let info = info.into();
                writer
                    .updated_fragment_mapping
                    .get_or_insert_default()
                    .push(rebuild_fragment_mapping(&info));
                *fragment_infos = info;
                false
            } else {
                true
            }
        }) {
            writer
                .deleted_fragment_mapping
                .get_or_insert_default()
                .push(rebuild_fragment_mapping(&fragment));
        }
        for (fragment_id, info) in remaining_fragments {
            let info = info.into();
            writer
                .added_fragment_mapping
                .get_or_insert_default()
                .push(rebuild_fragment_mapping(&info));
            database.insert(fragment_id, info);
        }
        writer.finish();
    }

    pub(super) fn upsert(
        &self,
        database_id: DatabaseId,
        infos: impl IntoIterator<Item = (&InflightFragmentInfo, JobId)>,
    ) {
        let mut writer = self.start_writer(database_id);
        writer.upsert(infos);
        writer.finish();
    }

    pub(super) fn start_writer(&self, database_id: DatabaseId) -> SharedActorInfoWriter<'_> {
        SharedActorInfoWriter {
            database_id,
            write_guard: self.inner.write(),
            notification_manager: &self.notification_manager,
            added_fragment_mapping: None,
            updated_fragment_mapping: None,
            deleted_fragment_mapping: None,
        }
    }
}

pub(super) struct SharedActorInfoWriter<'a> {
    database_id: DatabaseId,
    write_guard: parking_lot::RwLockWriteGuard<'a, SharedActorInfosInner>,
    notification_manager: &'a NotificationManagerRef,
    added_fragment_mapping: Option<Vec<PbFragmentWorkerSlotMapping>>,
    updated_fragment_mapping: Option<Vec<PbFragmentWorkerSlotMapping>>,
    deleted_fragment_mapping: Option<Vec<PbFragmentWorkerSlotMapping>>,
}

impl SharedActorInfoWriter<'_> {
    pub(super) fn upsert(
        &mut self,
        infos: impl IntoIterator<Item = (&InflightFragmentInfo, JobId)>,
    ) {
        let database = self.write_guard.info.entry(self.database_id).or_default();
        for info @ (fragment, _) in infos {
            match database.entry(fragment.fragment_id) {
                Entry::Occupied(mut entry) => {
                    let info = info.into();
                    self.updated_fragment_mapping
                        .get_or_insert_default()
                        .push(rebuild_fragment_mapping(&info));
                    entry.insert(info);
                }
                Entry::Vacant(entry) => {
                    let info = info.into();
                    self.added_fragment_mapping
                        .get_or_insert_default()
                        .push(rebuild_fragment_mapping(&info));
                    entry.insert(info);
                }
            }
        }
    }

    pub(super) fn remove(&mut self, info: &InflightFragmentInfo) {
        if let Some(database) = self.write_guard.info.get_mut(&self.database_id)
            && let Some(fragment) = database.remove(&info.fragment_id)
        {
            self.deleted_fragment_mapping
                .get_or_insert_default()
                .push(rebuild_fragment_mapping(&fragment));
        }
    }

    pub(super) fn finish(self) {
        if let Some(mapping) = self.added_fragment_mapping {
            self.notification_manager
                .notify_streaming_fragment_mapping(Operation::Add, mapping);
        }
        if let Some(mapping) = self.updated_fragment_mapping {
            self.notification_manager
                .notify_streaming_fragment_mapping(Operation::Update, mapping);
        }
        if let Some(mapping) = self.deleted_fragment_mapping {
            self.notification_manager
                .notify_streaming_fragment_mapping(Operation::Delete, mapping);
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct BarrierInfo {
    pub prev_epoch: TracedEpoch,
    pub curr_epoch: TracedEpoch,
    pub kind: BarrierKind,
}

impl BarrierInfo {
    pub(super) fn prev_epoch(&self) -> u64 {
        self.prev_epoch.value().0
    }

    pub(super) fn curr_epoch(&self) -> u64 {
        self.curr_epoch.value().0
    }

    pub(super) fn epoch(&self) -> EpochPair {
        EpochPair {
            curr: self.curr_epoch(),
            prev: self.prev_epoch(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum SubscriberType {
    Subscription(u64),
    SnapshotBackfill,
}

#[derive(Debug)]
pub(super) enum CreateStreamingJobStatus {
    Init,
    Creating { tracker: CreateMviewProgressTracker },
    Created,
}

#[derive(Debug)]
pub(super) struct InflightStreamingJobInfo {
    pub job_id: JobId,
    pub fragment_infos: HashMap<FragmentId, InflightFragmentInfo>,
    pub subscribers: HashMap<SubscriberId, SubscriberType>,
    pub status: CreateStreamingJobStatus,
    pub cdc_table_backfill_tracker: Option<CdcTableBackfillTracker>,
}

impl InflightStreamingJobInfo {
    pub fn fragment_infos(&self) -> impl Iterator<Item = &InflightFragmentInfo> + '_ {
        self.fragment_infos.values()
    }

    pub fn snapshot_backfill_actor_ids(
        fragment_infos: &HashMap<FragmentId, InflightFragmentInfo>,
    ) -> impl Iterator<Item = ActorId> + '_ {
        fragment_infos
            .values()
            .filter(|fragment| {
                fragment
                    .fragment_type_mask
                    .contains(FragmentTypeFlag::SnapshotBackfillStreamScan)
            })
            .flat_map(|fragment| fragment.actors.keys().copied())
    }

    pub fn tracking_progress_actor_ids(
        fragment_infos: &HashMap<FragmentId, InflightFragmentInfo>,
    ) -> Vec<(ActorId, BackfillUpstreamType)> {
        StreamJobFragments::tracking_progress_actor_ids_impl(
            fragment_infos
                .values()
                .map(|fragment| (fragment.fragment_type_mask, fragment.actors.keys().copied())),
        )
    }
}

impl<'a> IntoIterator for &'a InflightStreamingJobInfo {
    type Item = &'a InflightFragmentInfo;

    type IntoIter = impl Iterator<Item = &'a InflightFragmentInfo> + 'a;

    fn into_iter(self) -> Self::IntoIter {
        self.fragment_infos()
    }
}

#[derive(Debug)]
pub struct InflightDatabaseInfo {
    pub(super) database_id: DatabaseId,
    jobs: HashMap<JobId, InflightStreamingJobInfo>,
    fragment_location: HashMap<FragmentId, JobId>,
    pub(super) shared_actor_infos: SharedActorInfos,
}

impl InflightDatabaseInfo {
    pub fn fragment_infos(&self) -> impl Iterator<Item = &InflightFragmentInfo> + '_ {
        self.jobs.values().flat_map(|job| job.fragment_infos())
    }

    pub fn contains_job(&self, job_id: JobId) -> bool {
        self.jobs.contains_key(&job_id)
    }

    pub(super) fn job_id_by_fragment(&self, fragment_id: FragmentId) -> Option<JobId> {
        self.fragment_location.get(&fragment_id).copied()
    }

    pub fn fragment(&self, fragment_id: FragmentId) -> &InflightFragmentInfo {
        let job_id = self.fragment_location[&fragment_id];
        self.jobs
            .get(&job_id)
            .expect("should exist")
            .fragment_infos
            .get(&fragment_id)
            .expect("should exist")
    }

    pub(super) fn backfill_fragment_ids_for_job(
        &self,
        job_id: JobId,
    ) -> MetaResult<HashSet<FragmentId>> {
        let job = self
            .jobs
            .get(&job_id)
            .ok_or_else(|| MetaError::invalid_parameter(format!("job {} not found", job_id)))?;
        Ok(job
            .fragment_infos
            .iter()
            .filter_map(|(fragment_id, fragment)| {
                fragment
                    .fragment_type_mask
                    .contains_any([
                        FragmentTypeFlag::StreamScan,
                        FragmentTypeFlag::SourceScan,
                        FragmentTypeFlag::LocalityProvider,
                    ])
                    .then_some(*fragment_id)
            })
            .collect())
    }

    pub(super) fn is_backfill_fragment(&self, fragment_id: FragmentId) -> MetaResult<bool> {
        let job_id = self.fragment_location.get(&fragment_id).ok_or_else(|| {
            MetaError::invalid_parameter(format!("fragment {} not found", fragment_id))
        })?;
        let fragment = self
            .jobs
            .get(job_id)
            .expect("should exist")
            .fragment_infos
            .get(&fragment_id)
            .expect("should exist");
        Ok(fragment.fragment_type_mask.contains_any([
            FragmentTypeFlag::StreamScan,
            FragmentTypeFlag::SourceScan,
            FragmentTypeFlag::LocalityProvider,
        ]))
    }

    pub fn gen_backfill_progress(&self) -> impl Iterator<Item = (JobId, BackfillProgress)> + '_ {
        self.jobs
            .iter()
            .filter_map(|(job_id, job)| match &job.status {
                CreateStreamingJobStatus::Init => None,
                CreateStreamingJobStatus::Creating { tracker } => {
                    let progress = tracker.gen_backfill_progress();
                    Some((
                        *job_id,
                        BackfillProgress {
                            progress,
                            backfill_type: PbBackfillType::NormalBackfill,
                        },
                    ))
                }
                CreateStreamingJobStatus::Created => None,
            })
    }

    pub fn gen_cdc_progress(&self) -> impl Iterator<Item = (JobId, CdcProgress)> + '_ {
        self.jobs.iter().filter_map(|(job_id, job)| {
            job.cdc_table_backfill_tracker
                .as_ref()
                .map(|tracker| (*job_id, tracker.gen_cdc_progress()))
        })
    }

    pub fn gen_fragment_backfill_progress(&self) -> Vec<FragmentBackfillProgress> {
        let mut result = Vec::new();
        for job in self.jobs.values() {
            let CreateStreamingJobStatus::Creating { tracker } = &job.status else {
                continue;
            };
            let fragment_progress = tracker.collect_fragment_progress(&job.fragment_infos, true);
            result.extend(fragment_progress);
        }
        result
    }

    pub(super) fn may_assign_fragment_cdc_backfill_splits(
        &mut self,
        fragment_id: FragmentId,
    ) -> MetaResult<Option<HashMap<ActorId, PbCdcTableSnapshotSplits>>> {
        let job_id = self.fragment_location[&fragment_id];
        let job = self.jobs.get_mut(&job_id).expect("should exist");
        if let Some(tracker) = &mut job.cdc_table_backfill_tracker {
            if !tracker.is_parallelized() {
                return Ok(None);
            }
            let cdc_scan_fragment_id = tracker.cdc_scan_fragment_id();
            if cdc_scan_fragment_id != fragment_id {
                return Ok(None);
            }
            let actors = job.fragment_infos[&cdc_scan_fragment_id]
                .actors
                .keys()
                .copied()
                .collect();
            tracker.reassign_splits(actors).map(Some)
        } else {
            Ok(None)
        }
    }

    pub(super) fn assign_cdc_backfill_splits(
        &mut self,
        job_id: JobId,
    ) -> MetaResult<Option<HashMap<ActorId, PbCdcTableSnapshotSplits>>> {
        let job = self.jobs.get_mut(&job_id).expect("should exist");
        if let Some(tracker) = &mut job.cdc_table_backfill_tracker {
            if !tracker.is_parallelized() {
                return Ok(None);
            }
            let cdc_scan_fragment_id = tracker.cdc_scan_fragment_id();
            let actors = job.fragment_infos[&cdc_scan_fragment_id]
                .actors
                .keys()
                .copied()
                .collect();
            tracker.reassign_splits(actors).map(Some)
        } else {
            Ok(None)
        }
    }

    pub(super) fn apply_collected_command(
        &mut self,
        command: &PostCollectCommand,
        resps: &HashMap<WorkerId, BarrierCompleteResponse>,
        version_stats: &HummockVersionStats,
    ) {
        if let PostCollectCommand::CreateStreamingJob { info, job_type, .. } = command {
            match job_type {
                CreateStreamingJobType::Normal | CreateStreamingJobType::SinkIntoTable(_) => {
                    let job_id = info.streaming_job.id();
                    if let Some(job_info) = self.jobs.get_mut(&job_id) {
                        let CreateStreamingJobStatus::Init = replace(
                            &mut job_info.status,
                            CreateStreamingJobStatus::Creating {
                                tracker: CreateMviewProgressTracker::new(
                                    info,
                                    version_stats,
                                    &job_info.fragment_infos,
                                ),
                            },
                        ) else {
                            unreachable!("should be init before collect the first barrier")
                        };
                    } else {
                        info!(%job_id, "newly create job get cancelled before first barrier is collected")
                    }
                }
                CreateStreamingJobType::SnapshotBackfill { .. }
                | CreateStreamingJobType::BatchRefresh(_) => {
                    // The progress of SnapshotBackfill/BatchRefresh won't be tracked here
                }
            }
        }
        if let PostCollectCommand::Reschedule { reschedules, .. } = command {
            // During reschedule we expect fragments to be rebuilt with new actors and no vnode bitmap update.
            debug_assert!(
                reschedules
                    .values()
                    .all(|reschedule| reschedule.vnode_bitmap_updates.is_empty()),
                "Reschedule should not carry vnode bitmap updates when actors are rebuilt"
            );

            // Collect jobs that own the rescheduled fragments; de-duplicate via HashSet.
            let related_job_ids = reschedules
                .keys()
                .filter_map(|fragment_id| self.fragment_location.get(fragment_id))
                .cloned()
                .collect::<HashSet<_>>();
            for job_id in related_job_ids {
                if let Some(job) = self.jobs.get_mut(&job_id)
                    && let CreateStreamingJobStatus::Creating { tracker, .. } = &mut job.status
                {
                    tracker.refresh_after_reschedule(&job.fragment_infos, version_stats);
                }
            }
        }
        for progress in resps.values().flat_map(|resp| &resp.create_mview_progress) {
            let Some(job_id) = self.fragment_location.get(&progress.fragment_id) else {
                warn!(
                    "update the progress of an non-existent creating streaming job: {progress:?}, which could be cancelled"
                );
                continue;
            };
            let tracker = match &mut self.jobs.get_mut(job_id).expect("should exist").status {
                CreateStreamingJobStatus::Init => {
                    continue;
                }
                CreateStreamingJobStatus::Creating { tracker, .. } => tracker,
                CreateStreamingJobStatus::Created => {
                    if !progress.done {
                        warn!("update the progress of an created streaming job: {progress:?}");
                    }
                    continue;
                }
            };
            tracker.apply_progress(progress, version_stats);
        }
        for progress in resps
            .values()
            .flat_map(|resp| &resp.cdc_table_backfill_progress)
        {
            let Some(job_id) = self.fragment_location.get(&progress.fragment_id) else {
                warn!(
                    "update the cdc progress of an non-existent creating streaming job: {progress:?}, which could be cancelled"
                );
                continue;
            };
            let job = self.jobs.get_mut(job_id).expect("should exist");
            if job.cdc_table_backfill_tracker.is_none() && progress.backfilled_row_count.is_some() {
                job.cdc_table_backfill_tracker = Some(
                    CdcTableBackfillTracker::new_non_parallelized(progress.fragment_id),
                );
            }
            let Some(tracker) = &mut job.cdc_table_backfill_tracker else {
                warn!("update the cdc progress of an created streaming job: {progress:?}");
                continue;
            };
            if progress.backfilled_row_count.is_some() {
                if tracker.is_parallelized() {
                    warn!("update row progress on a parallelized CDC backfill: {progress:?}");
                    continue;
                }
                tracker.update_row_progress(progress);
            } else {
                if !tracker.is_parallelized() {
                    warn!("update split progress on a non-parallelized CDC backfill: {progress:?}");
                    continue;
                }
                tracker.update_split_progress(progress);
            }
        }
        // Handle CDC source offset updated events
        for cdc_offset_updated in resps
            .values()
            .flat_map(|resp| &resp.cdc_source_offset_updated)
        {
            let source_id = cdc_offset_updated.source_id;
            let job_id = source_id.as_share_source_job_id();
            if let Some(job) = self.jobs.get_mut(&job_id) {
                if let CreateStreamingJobStatus::Creating { tracker, .. } = &mut job.status {
                    tracker.mark_cdc_source_finished();
                }
            } else {
                warn!(
                    "update cdc source offset for non-existent creating streaming job: source_id={}, job_id={}",
                    cdc_offset_updated.source_id, job_id
                );
            }
        }
    }

    fn iter_creating_job_tracker(&self) -> impl Iterator<Item = &CreateMviewProgressTracker> {
        self.jobs.values().filter_map(|job| match &job.status {
            CreateStreamingJobStatus::Init => None,
            CreateStreamingJobStatus::Creating { tracker, .. } => Some(tracker),
            CreateStreamingJobStatus::Created => None,
        })
    }

    fn iter_mut_creating_job_tracker(
        &mut self,
    ) -> impl Iterator<Item = &mut CreateMviewProgressTracker> {
        self.jobs
            .values_mut()
            .filter_map(|job| match &mut job.status {
                CreateStreamingJobStatus::Init => None,
                CreateStreamingJobStatus::Creating { tracker, .. } => Some(tracker),
                CreateStreamingJobStatus::Created => None,
            })
    }

    pub(super) fn has_pending_finished_jobs(&self) -> bool {
        self.iter_creating_job_tracker()
            .any(|tracker| tracker.is_finished())
    }

    pub(super) fn take_pending_backfill_nodes(&mut self) -> Vec<FragmentId> {
        self.iter_mut_creating_job_tracker()
            .flat_map(|tracker| tracker.take_pending_backfill_nodes())
            .collect()
    }

    pub(super) fn take_staging_commit_info(&mut self) -> StagingCommitInfo {
        let mut finished_jobs = vec![];
        let mut table_ids_to_truncate = vec![];
        let mut finished_cdc_table_backfill = vec![];
        for (job_id, job) in &mut self.jobs {
            if let CreateStreamingJobStatus::Creating { tracker, .. } = &mut job.status {
                let (is_finished, truncate_table_ids) = tracker.collect_staging_commit_info();
                table_ids_to_truncate.extend(truncate_table_ids);
                if is_finished {
                    let CreateStreamingJobStatus::Creating { tracker, .. } =
                        replace(&mut job.status, CreateStreamingJobStatus::Created)
                    else {
                        unreachable!()
                    };
                    finished_jobs.push(tracker.into_tracking_job());
                }
            }
            if let Some(tracker) = &mut job.cdc_table_backfill_tracker
                && tracker.take_pre_completed()
            {
                finished_cdc_table_backfill.push(*job_id);
            }
        }
        StagingCommitInfo {
            finished_jobs,
            table_ids_to_truncate,
            finished_cdc_table_backfill,
        }
    }

    pub fn fragment_subscribers(
        &self,
        fragment_id: FragmentId,
    ) -> impl Iterator<Item = SubscriberId> + '_ {
        let job_id = self.fragment_location[&fragment_id];
        self.jobs[&job_id].subscribers.keys().copied()
    }

    pub fn job_subscribers(&self, job_id: JobId) -> impl Iterator<Item = SubscriberId> + '_ {
        self.jobs[&job_id].subscribers.keys().copied()
    }

    pub fn subscribed_tables(&self) -> impl Iterator<Item = TableId> + '_ {
        self.jobs.iter().filter_map(|(job_id, info)| {
            info.subscribers
                .values()
                .any(|subscriber| matches!(subscriber, SubscriberType::Subscription(_)))
                .then_some(job_id.as_mv_table_id())
        })
    }

    pub fn register_subscriber(
        &mut self,
        job_id: JobId,
        subscriber_id: SubscriberId,
        subscriber: SubscriberType,
    ) {
        self.jobs
            .get_mut(&job_id)
            .expect("should exist")
            .subscribers
            .try_insert(subscriber_id, subscriber)
            .expect("non duplicate");
    }

    pub fn unregister_subscriber(
        &mut self,
        job_id: JobId,
        subscriber_id: SubscriberId,
    ) -> Option<SubscriberType> {
        self.jobs
            .get_mut(&job_id)
            .expect("should exist")
            .subscribers
            .remove(&subscriber_id)
    }

    pub fn update_subscription_retention(
        &mut self,
        job_id: JobId,
        subscriber_id: SubscriberId,
        retention_second: u64,
    ) {
        let job = self.jobs.get_mut(&job_id).expect("should exist");
        match job.subscribers.get_mut(&subscriber_id) {
            Some(SubscriberType::Subscription(current_retention)) => {
                *current_retention = retention_second;
            }
            Some(SubscriberType::SnapshotBackfill) => {
                warn!(
                    %job_id,
                    %subscriber_id,
                    "cannot update retention for snapshot backfill subscriber"
                );
            }
            None => {
                warn!(%job_id, %subscriber_id, "subscription subscriber not found");
            }
        }
    }

    fn fragment_mut(&mut self, fragment_id: FragmentId) -> (&mut InflightFragmentInfo, JobId) {
        let job_id = self.fragment_location[&fragment_id];
        let fragment = self
            .jobs
            .get_mut(&job_id)
            .expect("should exist")
            .fragment_infos
            .get_mut(&fragment_id)
            .expect("should exist");
        (fragment, job_id)
    }

    fn empty_inner(database_id: DatabaseId, shared_actor_infos: SharedActorInfos) -> Self {
        Self {
            database_id,
            jobs: Default::default(),
            fragment_location: Default::default(),
            shared_actor_infos,
        }
    }

    pub fn empty(database_id: DatabaseId, shared_actor_infos: SharedActorInfos) -> Self {
        // remove the database because it's empty.
        shared_actor_infos.remove_database(database_id);
        Self::empty_inner(database_id, shared_actor_infos)
    }

    pub fn recover(
        database_id: DatabaseId,
        jobs: impl Iterator<Item = InflightStreamingJobInfo>,
        shared_actor_infos: SharedActorInfos,
    ) -> Self {
        let mut info = Self::empty_inner(database_id, shared_actor_infos);
        for job in jobs {
            info.add_existing(job);
        }
        info
    }

    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    pub fn add_existing(&mut self, job: InflightStreamingJobInfo) {
        let InflightStreamingJobInfo {
            job_id,
            fragment_infos,
            subscribers,
            status,
            cdc_table_backfill_tracker,
        } = job;
        self.jobs
            .try_insert(
                job_id,
                InflightStreamingJobInfo {
                    job_id,
                    subscribers,
                    fragment_infos: Default::default(), // fill in later in pre_apply_new_fragments
                    status,
                    cdc_table_backfill_tracker,
                },
            )
            .expect("non-duplicate");
        self.pre_apply_new_fragments(
            fragment_infos
                .into_iter()
                .map(|(fragment_id, info)| (fragment_id, job_id, info)),
        );
    }

    /// Register a new streaming job entry (with empty `fragment_infos`).
    pub(crate) fn pre_apply_new_job(
        &mut self,
        job_id: JobId,
        cdc_table_backfill_tracker: Option<CdcTableBackfillTracker>,
    ) {
        {
            self.jobs
                .try_insert(
                    job_id,
                    InflightStreamingJobInfo {
                        job_id,
                        fragment_infos: Default::default(),
                        subscribers: Default::default(), // no subscriber for newly create job
                        status: CreateStreamingJobStatus::Init,
                        cdc_table_backfill_tracker,
                    },
                )
                .expect("non-duplicate");
        }
    }

    /// Add new fragment infos and update shared actor infos.
    pub(crate) fn pre_apply_new_fragments(
        &mut self,
        fragments: impl IntoIterator<Item = (FragmentId, JobId, InflightFragmentInfo)>,
    ) {
        {
            let shared_infos = self.shared_actor_infos.clone();
            let mut shared_actor_writer = shared_infos.start_writer(self.database_id);
            for (fragment_id, job_id, info) in fragments {
                {
                    {
                        let fragment_infos = self.jobs.get_mut(&job_id).expect("should exist");
                        shared_actor_writer.upsert([(&info, job_id)]);
                        fragment_infos
                            .fragment_infos
                            .try_insert(fragment_id, info)
                            .expect("non duplicate");
                        self.fragment_location
                            .try_insert(fragment_id, job_id)
                            .expect("non duplicate");
                    }
                }
            }
            shared_actor_writer.finish();
        }
    }

    /// Pre-apply reschedule: update actors, vnode bitmaps, and splits.
    /// The actual removal of old actors happens in `post_apply_reschedules`.
    pub(crate) fn pre_apply_reschedule(
        &mut self,
        fragment_id: FragmentId,
        new_actors: HashMap<ActorId, InflightActorInfo>,
        actor_update_vnode_bitmap: HashMap<ActorId, Bitmap>,
        actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
    ) {
        {
            {
                {
                    {
                        let (info, _) = self.fragment_mut(fragment_id);
                        let actors = &mut info.actors;
                        for (actor_id, new_vnodes) in actor_update_vnode_bitmap {
                            actors
                                .get_mut(&actor_id)
                                .expect("should exist")
                                .vnode_bitmap = Some(new_vnodes);
                        }
                        for (actor_id, actor) in new_actors {
                            actors
                                .try_insert(actor_id as _, actor)
                                .expect("non-duplicate");
                        }
                        for (actor_id, splits) in actor_splits {
                            actors.get_mut(&actor_id).expect("should exist").splits = splits;
                        }
                        // info will be upserted into shared_actor_infos in post_apply stage
                    }
                }
            }
        }
    }

    /// Replace upstream fragment IDs in merge nodes of a fragment's stream graph.
    pub(crate) fn pre_apply_replace_node_upstream(
        &mut self,
        fragment_id: FragmentId,
        replace_map: &HashMap<FragmentId, FragmentId>,
    ) {
        {
            {
                {
                    {
                        let mut remaining_fragment_ids: HashSet<_> =
                            replace_map.keys().cloned().collect();
                        let (info, _) = self.fragment_mut(fragment_id);
                        visit_stream_node_mut(&mut info.nodes, |node| {
                            if let NodeBody::Merge(m) = node
                                && let Some(new_upstream_fragment_id) =
                                    replace_map.get(&m.upstream_fragment_id)
                            {
                                if !remaining_fragment_ids.remove(&m.upstream_fragment_id) {
                                    if cfg!(debug_assertions) {
                                        panic!(
                                            "duplicate upstream fragment: {:?} {:?}",
                                            m, replace_map
                                        );
                                    } else {
                                        warn!(?m, ?replace_map, "duplicate upstream fragment");
                                    }
                                }
                                m.upstream_fragment_id = *new_upstream_fragment_id;
                            }
                        });
                        if cfg!(debug_assertions) {
                            assert!(
                                remaining_fragment_ids.is_empty(),
                                "non-existing fragment to replace: {:?} {:?} {:?}",
                                remaining_fragment_ids,
                                info.nodes,
                                replace_map
                            );
                        } else {
                            warn!(?remaining_fragment_ids, node = ?info.nodes, ?replace_map, "non-existing fragment to replace");
                        }
                    }
                }
            }
        }
    }

    /// Add a new upstream sink node to a fragment's `UpstreamSinkUnion`.
    pub(crate) fn pre_apply_add_node_upstream(
        &mut self,
        fragment_id: FragmentId,
        new_upstream_info: &PbUpstreamSinkInfo,
    ) {
        {
            {
                {
                    {
                        let (info, _) = self.fragment_mut(fragment_id);
                        let mut injected = false;
                        visit_stream_node_mut(&mut info.nodes, |node| {
                            if let NodeBody::UpstreamSinkUnion(u) = node {
                                if cfg!(debug_assertions) {
                                    let current_upstream_fragment_ids = u
                                        .init_upstreams
                                        .iter()
                                        .map(|upstream| upstream.upstream_fragment_id)
                                        .collect::<HashSet<_>>();
                                    if current_upstream_fragment_ids
                                        .contains(&new_upstream_info.upstream_fragment_id)
                                    {
                                        panic!(
                                            "duplicate upstream fragment: {:?} {:?}",
                                            u, new_upstream_info
                                        );
                                    }
                                }
                                u.init_upstreams.push(new_upstream_info.clone());
                                injected = true;
                            }
                        });
                        assert!(injected, "should inject upstream into UpstreamSinkUnion");
                    }
                }
            }
        }
    }

    /// Remove upstream sink nodes from a fragment's `UpstreamSinkUnion`.
    pub(crate) fn pre_apply_drop_node_upstream(
        &mut self,
        fragment_id: FragmentId,
        drop_upstream_fragment_ids: &[FragmentId],
    ) {
        if !self.fragment_location.contains_key(&fragment_id) {
            warn!(
                target_fragment_id = %fragment_id,
                drop_upstream_fragment_ids = ?drop_upstream_fragment_ids,
                "skip dropping upstream sink fragments for non-existing target fragment"
            );
            return;
        }
        {
            {
                {
                    {
                        let (info, _) = self.fragment_mut(fragment_id);
                        let mut removed = false;
                        visit_stream_node_mut(&mut info.nodes, |node| {
                            if let NodeBody::UpstreamSinkUnion(u) = node {
                                if cfg!(debug_assertions) {
                                    let current_upstream_fragment_ids = u
                                        .init_upstreams
                                        .iter()
                                        .map(|upstream| upstream.upstream_fragment_id)
                                        .collect::<HashSet<FragmentId>>();
                                    for drop_fragment_id in drop_upstream_fragment_ids {
                                        if !current_upstream_fragment_ids.contains(drop_fragment_id)
                                        {
                                            panic!(
                                                "non-existing upstream fragment to drop: {:?} {:?} {:?}",
                                                u, drop_upstream_fragment_ids, drop_fragment_id
                                            );
                                        }
                                    }
                                }
                                u.init_upstreams.retain(|upstream| {
                                    !drop_upstream_fragment_ids
                                        .contains(&upstream.upstream_fragment_id)
                                });
                                removed = true;
                            }
                        });
                        assert!(removed, "should remove upstream from UpstreamSinkUnion");
                    }
                }
            }
        }
    }

    /// Sync inflight `nodes` so a later reschedule won't materialize new actors from stale data.
    pub(crate) fn pre_apply_throttle(
        &mut self,
        config: &mut ThrottleConfigMap,
    ) -> Option<Mutation> {
        extract_throttle_config(config, |fragment_id, stream_node| {
            if !self.fragment_location.contains_key(&fragment_id) {
                return false;
            }
            self.fragment_mut(fragment_id).0.nodes = stream_node.clone();
            true
        })
    }

    /// Update split assignments for actors in fragments.
    pub(crate) fn pre_apply_split_assignments(
        &mut self,
        assignments: impl IntoIterator<Item = (FragmentId, HashMap<ActorId, Vec<SplitImpl>>)>,
    ) {
        {
            let shared_infos = self.shared_actor_infos.clone();
            let mut shared_actor_writer = shared_infos.start_writer(self.database_id);
            {
                {
                    for (fragment_id, actor_splits) in assignments {
                        let (info, job_id) = self.fragment_mut(fragment_id);
                        let actors = &mut info.actors;
                        for (actor_id, splits) in actor_splits {
                            actors.get_mut(&actor_id).expect("should exist").splits = splits;
                        }
                        shared_actor_writer.upsert([(&*info, job_id)]);
                    }
                }
            }
            shared_actor_writer.finish();
        }
    }

    pub(super) fn build_edge(
        &self,
        info: Option<(&CreateStreamingJobCommandInfo, bool)>,
        replace_job: Option<&ReplaceStreamJobPlan>,
        new_upstream_sink: Option<&UpstreamSinkInfo>,
        control_stream_manager: &ControlStreamManager,
        stream_actors: &HashMap<FragmentId, Vec<StreamActor>>,
        actor_location: &HashMap<ActorId, WorkerId>,
    ) -> FragmentEdgeBuildResult {
        // `existing_fragment_ids` consists of
        //  - keys of `info.upstream_fragment_downstreams`, which are the `fragment_id` the upstream fragment of the newly created job
        //  - keys of `replace_job.upstream_fragment_downstreams`, which are the `fragment_id` of upstream fragment of replace_job,
        // if the upstream fragment previously exists
        //  - keys of `replace_upstream`, which are the `fragment_id` of downstream fragments that will update their upstream fragments,
        // if creating a new sink-into-table
        //  - should contain the `fragment_id` of the downstream table.
        let existing_fragment_ids = info
            .into_iter()
            .flat_map(|(info, _)| info.upstream_fragment_downstreams.keys())
            .chain(replace_job.into_iter().flat_map(|replace_job| {
                replace_job
                    .upstream_fragment_downstreams
                    .keys()
                    .filter(|fragment_id| {
                        info.map(|(info, _)| {
                            !info
                                .stream_job_fragments
                                .fragments
                                .contains_key(*fragment_id)
                        })
                        .unwrap_or(true)
                    })
                    .chain(replace_job.replace_upstream.keys())
            }))
            .chain(
                new_upstream_sink
                    .into_iter()
                    .map(|ctx| &ctx.new_sink_downstream.downstream_fragment_id),
            )
            .cloned();
        // Collect new fragments with their partial graph IDs
        let new_fragments = info
            .into_iter()
            .flat_map(|(info, is_snapshot_backfill)| {
                let partial_graph_id = to_partial_graph_id(
                    self.database_id,
                    is_snapshot_backfill.then_some(info.streaming_job.id()),
                );
                info.stream_job_fragments
                    .fragments
                    .values()
                    .map(move |fragment| (partial_graph_id, fragment))
            })
            .chain(replace_job.into_iter().flat_map(|replace_job| {
                replace_job
                    .new_fragments
                    .fragments
                    .values()
                    .chain(
                        replace_job
                            .auto_refresh_schema_sinks
                            .as_ref()
                            .into_iter()
                            .flat_map(move |sinks| sinks.iter().map(|sink| &sink.new_fragment)),
                    )
                    .map(|fragment| {
                        (
                            // we assume that replace job only happens in database partial graph
                            to_partial_graph_id(self.database_id, None),
                            fragment,
                        )
                    })
            }));

        let mut builder = FragmentEdgeBuilder::new(
            // Existing fragments
            existing_fragment_ids
                .map(|fragment_id| {
                    (
                        fragment_id,
                        EdgeBuilderFragmentInfo::from_inflight(
                            self.fragment(fragment_id),
                            to_partial_graph_id(self.database_id, None),
                            control_stream_manager,
                        ),
                    )
                })
                // New fragments from create/replace jobs
                .chain(new_fragments.map(|(partial_graph_id, fragment)| {
                    (
                        fragment.fragment_id,
                        EdgeBuilderFragmentInfo::from_fragment(
                            fragment,
                            stream_actors,
                            actor_location,
                            partial_graph_id,
                            control_stream_manager,
                        ),
                    )
                })),
        );
        if let Some((info, _)) = info {
            builder.add_relations(&info.upstream_fragment_downstreams);
            builder.add_relations(&info.stream_job_fragments.downstreams);
        }
        if let Some(replace_job) = replace_job {
            builder.add_relations(&replace_job.upstream_fragment_downstreams);
            builder.add_relations(&replace_job.new_fragments.downstreams);
        }
        if let Some(new_upstream_sink) = new_upstream_sink {
            let sink_fragment_id = new_upstream_sink.sink_fragment_id;
            let new_sink_downstream = &new_upstream_sink.new_sink_downstream;
            builder.add_edge(sink_fragment_id, new_sink_downstream);
        }
        if let Some(replace_job) = replace_job {
            for (fragment_id, fragment_replacement) in &replace_job.replace_upstream {
                for (original_upstream_fragment_id, new_upstream_fragment_id) in
                    fragment_replacement
                {
                    builder.replace_upstream(
                        *fragment_id,
                        *original_upstream_fragment_id,
                        *new_upstream_fragment_id,
                    );
                }
            }
        }
        builder.build()
    }

    /// Post-apply reschedule: remove actors that were marked for removal.
    pub(crate) fn post_apply_reschedules(
        &mut self,
        reschedules: impl IntoIterator<Item = (FragmentId, HashSet<ActorId>)>,
    ) {
        let inner = self.shared_actor_infos.clone();
        let mut shared_actor_writer = inner.start_writer(self.database_id);
        {
            {
                {
                    for (fragment_id, to_remove) in reschedules {
                        let job_id = self.fragment_location[&fragment_id];
                        let info = self
                            .jobs
                            .get_mut(&job_id)
                            .expect("should exist")
                            .fragment_infos
                            .get_mut(&fragment_id)
                            .expect("should exist");
                        for actor_id in to_remove {
                            assert!(info.actors.remove(&actor_id).is_some());
                        }
                        shared_actor_writer.upsert([(&*info, job_id)]);
                    }
                }
            }
        }
        shared_actor_writer.finish();
    }

    /// Post-apply fragment removal: remove fragments and their jobs if empty.
    pub(crate) fn post_apply_remove_fragments(
        &mut self,
        fragment_ids: impl IntoIterator<Item = FragmentId>,
    ) {
        let inner = self.shared_actor_infos.clone();
        let mut shared_actor_writer = inner.start_writer(self.database_id);
        {
            {
                {
                    for fragment_id in fragment_ids {
                        let job_id = self
                            .fragment_location
                            .remove(&fragment_id)
                            .expect("should exist");
                        let job = self.jobs.get_mut(&job_id).expect("should exist");
                        let fragment = job
                            .fragment_infos
                            .remove(&fragment_id)
                            .expect("should exist");
                        shared_actor_writer.remove(&fragment);
                        if job.fragment_infos.is_empty() {
                            self.jobs.remove(&job_id).expect("should exist");
                        }
                    }
                }
            }
        }
        shared_actor_writer.finish();
    }

    pub(crate) fn post_apply_remove_job(
        &mut self,
        job_id: JobId,
    ) -> Option<InflightStreamingJobInfo> {
        let job = self.jobs.remove(&job_id)?;
        let inner = self.shared_actor_infos.clone();
        let mut shared_actor_writer = inner.start_writer(self.database_id);
        for (fragment_id, fragment) in &job.fragment_infos {
            self.fragment_location
                .remove(fragment_id)
                .expect("should exist");
            shared_actor_writer.remove(fragment);
        }
        shared_actor_writer.finish();
        Some(job)
    }
}

impl InflightFragmentInfo {
    /// Returns actor list to collect in the target worker node.
    pub(crate) fn actor_ids_to_collect(
        infos: impl IntoIterator<Item = &Self>,
    ) -> HashMap<WorkerId, HashSet<ActorId>> {
        let mut ret: HashMap<_, HashSet<_>> = HashMap::new();
        for (actor_id, actor) in infos.into_iter().flat_map(|info| info.actors.iter()) {
            assert!(
                ret.entry(actor.worker_id)
                    .or_default()
                    .insert(*actor_id as _)
            )
        }
        ret
    }

    pub fn existing_table_ids<'a>(
        infos: impl IntoIterator<Item = &'a Self> + 'a,
    ) -> impl Iterator<Item = TableId> + 'a {
        infos
            .into_iter()
            .flat_map(|info| info.state_table_ids.iter().cloned())
    }

    pub fn workers<'a>(
        infos: impl IntoIterator<Item = &'a Self> + 'a,
    ) -> impl Iterator<Item = WorkerId> + 'a {
        infos
            .into_iter()
            .flat_map(|fragment| fragment.actors.values().map(|actor| actor.worker_id))
    }

    pub fn contains_worker<'a>(
        infos: impl IntoIterator<Item = &'a Self> + 'a,
        worker_id: WorkerId,
    ) -> bool {
        Self::workers(infos).any(|existing_worker_id| existing_worker_id == worker_id)
    }
}

impl InflightDatabaseInfo {
    pub fn contains_worker(&self, worker_id: WorkerId) -> bool {
        InflightFragmentInfo::contains_worker(self.fragment_infos(), worker_id)
    }

    pub fn existing_table_ids(&self) -> impl Iterator<Item = TableId> + '_ {
        InflightFragmentInfo::existing_table_ids(self.fragment_infos())
    }
}
