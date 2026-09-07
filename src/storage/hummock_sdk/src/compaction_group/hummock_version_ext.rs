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
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::iter::once;
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::log::LogSuppressor;
use risingwave_pb::hummock::{
    CompactionConfig, CompatibilityVersion, PbLevelType, StateTableInfo, StateTableInfoDelta,
};
use tracing::warn;

use super::group_split::split_sst_with_table_ids;
use super::{StateTableId, group_split};
use crate::change_log::{EpochNewChangeLogCommon, TableChangeLogCommon};
use crate::compact_task::is_compaction_task_expired;
use crate::compaction_group::StaticCompactionGroupId;
use crate::key_range::KeyRangeCommon;
use crate::level::{Level, LevelCommon, Levels, OverlappingLevel};
use crate::sstable_info::SstableInfo;
use crate::table_watermark::{ReadTableWatermark, TableWatermarks};
use crate::vector_index::apply_vector_index_delta;
use crate::version::{
    GroupDelta, GroupDeltaCommon, HummockVersion, HummockVersionCommon, HummockVersionDeltaCommon,
    IntraLevelDelta, IntraLevelDeltaCommon, ObjectIdReader, SstableIdReader,
};
use crate::{
    CompactionGroupId, HummockObjectId, HummockSstableId, HummockSstableObjectId, can_concat,
};

#[derive(Debug, Clone, Default)]
pub struct SstDeltaInfo {
    pub insert_sst_level: u32,
    pub insert_sst_infos: Vec<SstableInfo>,
    pub delete_sst_object_ids: Vec<HummockSstableObjectId>,
}

pub type BranchedSstInfo = HashMap<CompactionGroupId, Vec<HummockSstableId>>;

impl HummockVersionCommon<SstableInfo> {
    pub fn get_compaction_group_levels(&self, compaction_group_id: CompactionGroupId) -> &Levels {
        self.levels
            .get(&compaction_group_id)
            .unwrap_or_else(|| panic!("compaction group {} does not exist", compaction_group_id))
    }

    pub fn get_compaction_group_levels_mut(
        &mut self,
        compaction_group_id: CompactionGroupId,
    ) -> &mut Levels {
        self.levels
            .get_mut(&compaction_group_id)
            .unwrap_or_else(|| panic!("compaction group {} does not exist", compaction_group_id))
    }

    // Only scan SST infos from levels in the specified compaction group.
    pub fn get_sst_ids_by_group_id(
        &self,
        compaction_group_id: CompactionGroupId,
    ) -> impl Iterator<Item = HummockSstableId> + '_ {
        self.levels
            .iter()
            .filter_map(move |(cg_id, level)| {
                if *cg_id == compaction_group_id {
                    Some(level)
                } else {
                    None
                }
            })
            .flat_map(|level| level.l0.sub_levels.iter().rev().chain(level.levels.iter()))
            .flat_map(|level| level.table_infos.iter())
            .map(|s| s.sst_id)
    }

    /// Prune stale table ids that no longer exist in `state_table_info` from SST metadata.
    ///
    /// This is used to normalize recovered versions from old clusters where dropped table ids
    /// may still exist in persisted SST metadata.
    pub fn prune_stale_table_ids_from_ssts(&mut self) -> usize {
        let live_table_ids: HashSet<_> = self.state_table_info.info().keys().copied().collect();
        // Older checkpoints may rely on deprecated `member_table_ids` before `state_table_info`
        // is backfilled.
        if live_table_ids.is_empty()
            && self.levels.values().any(|levels| {
                #[expect(deprecated)]
                {
                    !levels.member_table_ids.is_empty()
                }
            })
        {
            return 0;
        }

        let mut pruned_table_ids = HashSet::new();

        for levels in self.levels.values_mut() {
            let stale_table_ids = levels
                .l0
                .sub_levels
                .iter()
                .chain(levels.levels.iter())
                .flat_map(|level| level.table_infos.iter())
                .flat_map(|sst| sst.table_ids.iter().copied())
                .filter(|table_id| !live_table_ids.contains(table_id))
                .collect::<HashSet<_>>();

            if stale_table_ids.is_empty() {
                continue;
            }

            pruned_table_ids.extend(stale_table_ids.iter().copied());
            levels.prune_table_ids_from_ssts(&stale_table_ids);
        }

        pruned_table_ids.len()
    }

    pub fn level_iter<F: FnMut(&Level) -> bool>(
        &self,
        compaction_group_id: CompactionGroupId,
        mut f: F,
    ) {
        if let Some(levels) = self.levels.get(&compaction_group_id) {
            for sub_level in &levels.l0.sub_levels {
                if !f(sub_level) {
                    return;
                }
            }
            for level in &levels.levels {
                if !f(level) {
                    return;
                }
            }
        }
    }

    pub fn num_levels(&self, compaction_group_id: CompactionGroupId) -> usize {
        // l0 is currently separated from all levels
        self.levels
            .get(&compaction_group_id)
            .map(|group| group.levels.len() + 1)
            .unwrap_or(0)
    }

    pub fn safe_epoch_table_watermarks(
        &self,
        existing_table_ids: &[TableId],
    ) -> BTreeMap<TableId, TableWatermarks> {
        safe_epoch_table_watermarks_impl(&self.table_watermarks, existing_table_ids)
    }
}

pub fn safe_epoch_table_watermarks_impl(
    table_watermarks: &HashMap<TableId, Arc<TableWatermarks>>,
    existing_table_ids: &[TableId],
) -> BTreeMap<TableId, TableWatermarks> {
    fn extract_single_table_watermark(
        table_watermarks: &TableWatermarks,
    ) -> Option<TableWatermarks> {
        if let Some((first_epoch, first_epoch_watermark)) = table_watermarks.watermarks.first() {
            Some(TableWatermarks {
                watermarks: vec![(*first_epoch, first_epoch_watermark.clone())],
                direction: table_watermarks.direction,
                watermark_type: table_watermarks.watermark_type,
            })
        } else {
            None
        }
    }
    table_watermarks
        .iter()
        .filter_map(|(table_id, table_watermarks)| {
            if !existing_table_ids.contains(table_id) {
                None
            } else {
                extract_single_table_watermark(table_watermarks)
                    .map(|table_watermarks| (*table_id, table_watermarks))
            }
        })
        .collect()
}

pub fn safe_epoch_read_table_watermarks_impl(
    safe_epoch_watermarks: BTreeMap<TableId, TableWatermarks>,
) -> BTreeMap<TableId, ReadTableWatermark> {
    safe_epoch_watermarks
        .into_iter()
        .map(|(table_id, watermarks)| {
            assert_eq!(watermarks.watermarks.len(), 1);
            let vnode_watermarks = &watermarks.watermarks.first().expect("should exist").1;
            let mut vnode_watermark_map = BTreeMap::new();
            for vnode_watermark in vnode_watermarks.iter() {
                let watermark = Bytes::copy_from_slice(vnode_watermark.watermark());
                for vnode in vnode_watermark.vnode_bitmap().iter_vnodes() {
                    assert!(
                        vnode_watermark_map
                            .insert(vnode, watermark.clone())
                            .is_none(),
                        "duplicate table watermark on vnode {}",
                        vnode.to_index()
                    );
                }
            }
            (
                table_id,
                ReadTableWatermark {
                    direction: watermarks.direction,
                    vnode_watermarks: vnode_watermark_map,
                },
            )
        })
        .collect()
}

impl HummockVersionCommon<SstableInfo> {
    pub fn count_new_ssts_in_group_split(
        &self,
        parent_group_id: CompactionGroupId,
        split_key: Bytes,
    ) -> u64 {
        self.levels
            .get(&parent_group_id)
            .map_or(0, |parent_levels| {
                let l0 = &parent_levels.l0;
                let mut split_count = 0;
                for sub_level in &l0.sub_levels {
                    assert!(!sub_level.table_infos.is_empty());

                    if sub_level.level_type == PbLevelType::Overlapping {
                        // TODO: use table_id / vnode / key_range filter
                        split_count += sub_level
                            .table_infos
                            .iter()
                            .map(|sst| {
                                if let group_split::SstSplitType::Both =
                                    group_split::need_to_split(sst, split_key.clone())
                                {
                                    2
                                } else {
                                    0
                                }
                            })
                            .sum::<u64>();
                        continue;
                    }

                    let pos = group_split::get_split_pos(&sub_level.table_infos, split_key.clone());
                    let sst = sub_level.table_infos.get(pos).unwrap();

                    if let group_split::SstSplitType::Both =
                        group_split::need_to_split(sst, split_key.clone())
                    {
                        split_count += 2;
                    }
                }

                for level in &parent_levels.levels {
                    if level.table_infos.is_empty() {
                        continue;
                    }
                    let pos = group_split::get_split_pos(&level.table_infos, split_key.clone());
                    let sst = level.table_infos.get(pos).unwrap();
                    if let group_split::SstSplitType::Both =
                        group_split::need_to_split(sst, split_key.clone())
                    {
                        split_count += 2;
                    }
                }

                split_count
            })
    }

    pub fn init_with_parent_group(
        &mut self,
        parent_group_id: CompactionGroupId,
        group_id: CompactionGroupId,
        member_table_ids: BTreeSet<StateTableId>,
        new_sst_start_id: HummockSstableId,
    ) {
        let mut new_sst_id = new_sst_start_id;
        if parent_group_id == StaticCompactionGroupId::NewCompactionGroup {
            if new_sst_start_id != 0 {
                if cfg!(debug_assertions) {
                    panic!(
                        "non-zero sst start id {} for NewCompactionGroup",
                        new_sst_start_id
                    );
                } else {
                    warn!(
                        %new_sst_start_id,
                        "non-zero sst start id for NewCompactionGroup"
                    );
                }
            }
            return;
        } else if !self.levels.contains_key(&parent_group_id) {
            unreachable!(
                "non-existing parent group id {} to init from",
                parent_group_id
            );
        }
        let [parent_levels, cur_levels] = self
            .levels
            .get_disjoint_mut([&parent_group_id, &group_id])
            .map(|res| res.unwrap());
        // After certain compaction group operation, e.g. split, any ongoing compaction tasks created prior to that should be rejected due to expiration.
        // By incrementing the compaction_group_version_id of the compaction group, and comparing it with the one recorded in compaction task, expired compaction tasks can be identified.
        parent_levels.compaction_group_version_id += 1;
        cur_levels.compaction_group_version_id += 1;
        let l0 = &mut parent_levels.l0;
        {
            for sub_level in &mut l0.sub_levels {
                let target_l0 = &mut cur_levels.l0;
                // Remove SST from sub level may result in empty sub level. It will be purged
                // whenever another compaction task is finished.
                let insert_table_infos =
                    split_sst_info_for_level(&member_table_ids, sub_level, &mut new_sst_id);
                sub_level.normalize();
                if insert_table_infos.is_empty() {
                    continue;
                }
                match group_split::get_sub_level_insert_hint(&target_l0.sub_levels, sub_level) {
                    Ok(idx) => {
                        add_ssts_to_sub_level(target_l0, idx, insert_table_infos);
                    }
                    Err(idx) => {
                        insert_new_sub_level(
                            target_l0,
                            sub_level.sub_level_id,
                            sub_level.level_type,
                            insert_table_infos,
                            Some(idx),
                        );
                    }
                }
            }
            l0.normalize();
        }
        for (idx, level) in parent_levels.levels.iter_mut().enumerate() {
            let insert_table_infos =
                split_sst_info_for_level(&member_table_ids, level, &mut new_sst_id);
            cur_levels.levels[idx].total_file_size += insert_table_infos
                .iter()
                .map(|sst| sst.sst_size)
                .sum::<u64>();
            cur_levels.levels[idx].uncompressed_file_size += insert_table_infos
                .iter()
                .map(|sst| sst.uncompressed_file_size)
                .sum::<u64>();
            cur_levels.levels[idx]
                .table_infos
                .extend(insert_table_infos);
            cur_levels.levels[idx]
                .table_infos
                .sort_by(|sst1, sst2| sst1.key_range.cmp(&sst2.key_range));
            assert!(can_concat(&cur_levels.levels[idx].table_infos));
            level.normalize();
        }

        assert!(
            parent_levels
                .l0
                .sub_levels
                .iter()
                .all(|level| !level.table_infos.is_empty())
        );
        assert!(
            cur_levels
                .l0
                .sub_levels
                .iter()
                .all(|level| !level.table_infos.is_empty())
        );
    }

    pub fn build_sst_delta_infos(
        &self,
        version_delta: &HummockVersionDeltaCommon<SstableInfo>,
    ) -> Vec<SstDeltaInfo> {
        let mut infos = vec![];

        // Skip trivial move delta for refiller
        // The trivial move task only changes the position of the sst in the lsm, it does not modify the object information corresponding to the sst, and does not need to re-execute the refill.
        if version_delta.trivial_move {
            return infos;
        }

        for (group_id, group_deltas) in &version_delta.group_deltas {
            let mut info = SstDeltaInfo::default();

            let mut removed_l0_ssts: BTreeSet<HummockSstableId> = BTreeSet::new();
            let mut removed_ssts: BTreeMap<u32, BTreeSet<HummockSstableId>> = BTreeMap::new();

            // Build only if all deltas are intra level deltas.
            if !group_deltas.group_deltas.iter().all(|delta| {
                matches!(
                    delta,
                    GroupDelta::IntraLevel(_) | GroupDelta::NewL0SubLevel(_)
                )
            }) {
                continue;
            }

            // TODO(MrCroxx): At most one insert delta is allowed here. It's okay for now with the
            // current `hummock::manager::gen_version_delta` implementation. Better refactor the
            // struct to reduce conventions.
            for group_delta in &group_deltas.group_deltas {
                match group_delta {
                    GroupDeltaCommon::NewL0SubLevel(inserted_table_infos) => {
                        if !inserted_table_infos.is_empty() {
                            info.insert_sst_level = 0;
                            info.insert_sst_infos
                                .extend(inserted_table_infos.iter().cloned());
                        }
                    }
                    GroupDeltaCommon::IntraLevel(intra_level) => {
                        if !intra_level.inserted_table_infos.is_empty() {
                            info.insert_sst_level = intra_level.level_idx;
                            info.insert_sst_infos
                                .extend(intra_level.inserted_table_infos.iter().cloned());
                        }
                        if !intra_level.removed_table_ids.is_empty() {
                            for id in &intra_level.removed_table_ids {
                                if intra_level.level_idx == 0 {
                                    removed_l0_ssts.insert(*id);
                                } else {
                                    removed_ssts
                                        .entry(intra_level.level_idx)
                                        .or_default()
                                        .insert(*id);
                                }
                            }
                        }
                    }
                    GroupDeltaCommon::GroupConstruct(_)
                    | GroupDeltaCommon::GroupDestroy(_)
                    | GroupDeltaCommon::GroupMerge(_)
                    | GroupDeltaCommon::PruneTableIdsFromSsts(_) => {}
                }
            }

            let group = self.levels.get(group_id).unwrap();
            for l0_sub_level in &group.level0().sub_levels {
                for sst_info in &l0_sub_level.table_infos {
                    if removed_l0_ssts.remove(&sst_info.sst_id) {
                        info.delete_sst_object_ids.push(sst_info.object_id);
                    }
                }
            }
            for level in &group.levels {
                if let Some(mut removed_level_ssts) = removed_ssts.remove(&level.level_idx) {
                    for sst_info in &level.table_infos {
                        if removed_level_ssts.remove(&sst_info.sst_id) {
                            info.delete_sst_object_ids.push(sst_info.object_id);
                        }
                    }
                    if !removed_level_ssts.is_empty() {
                        tracing::error!(
                            "removed_level_ssts is not empty: {:?}",
                            removed_level_ssts,
                        );
                    }
                    debug_assert!(removed_level_ssts.is_empty());
                }
            }

            if !removed_l0_ssts.is_empty() || !removed_ssts.is_empty() {
                tracing::error!(
                    "not empty removed_l0_ssts: {:?}, removed_ssts: {:?}",
                    removed_l0_ssts,
                    removed_ssts
                );
            }
            debug_assert!(removed_l0_ssts.is_empty());
            debug_assert!(removed_ssts.is_empty());

            infos.push(info);
        }

        infos
    }

    pub fn apply_version_delta(
        &mut self,
        version_delta: &HummockVersionDeltaCommon<SstableInfo>,
    ) -> HashMap<TableId, Option<StateTableInfo>> {
        assert_eq!(self.id, version_delta.prev_id);

        let (changed_table_info, mut is_commit_epoch) = self.state_table_info.apply_delta(
            &version_delta.state_table_info_delta,
            &version_delta.removed_table_ids,
        );

        #[expect(deprecated)]
        {
            if !is_commit_epoch && self.max_committed_epoch < version_delta.max_committed_epoch {
                is_commit_epoch = true;
                tracing::trace!(
                    "max committed epoch bumped but no table committed epoch is changed"
                );
            }
        }

        // apply to `levels`, which is different compaction groups
        for (compaction_group_id, group_deltas) in &version_delta.group_deltas {
            let mut is_applied_l0_compact = false;
            for group_delta in &group_deltas.group_deltas {
                match group_delta {
                    GroupDeltaCommon::GroupConstruct(group_construct) => {
                        let mut new_levels = build_initial_compaction_group_levels(
                            *compaction_group_id,
                            group_construct.get_group_config().unwrap(),
                        );
                        let parent_group_id = group_construct.parent_group_id;
                        new_levels.parent_group_id = parent_group_id;
                        #[expect(deprecated)]
                        // for backward-compatibility of previous hummock version delta
                        new_levels
                            .member_table_ids
                            .clone_from(&group_construct.table_ids);
                        self.levels.insert(*compaction_group_id, new_levels);
                        let member_table_ids = if group_construct.version()
                            >= CompatibilityVersion::NoMemberTableIds
                        {
                            self.state_table_info
                                .compaction_group_member_table_ids(*compaction_group_id)
                                .iter()
                                .copied()
                                .collect()
                        } else {
                            #[expect(deprecated)]
                            // for backward-compatibility of previous hummock version delta
                            BTreeSet::from_iter(
                                group_construct.table_ids.iter().copied().map(Into::into),
                            )
                        };

                        if group_construct.version() >= CompatibilityVersion::SplitGroupByTableId {
                            let split_key = if group_construct.split_key.is_some() {
                                Some(Bytes::from(group_construct.split_key.clone().unwrap()))
                            } else {
                                None
                            };
                            self.init_with_parent_group_v2(
                                parent_group_id,
                                *compaction_group_id,
                                group_construct.new_sst_start_id,
                                split_key.clone(),
                            );
                        } else {
                            // for backward-compatibility of previous hummock version delta
                            self.init_with_parent_group(
                                parent_group_id,
                                *compaction_group_id,
                                member_table_ids,
                                group_construct.new_sst_start_id,
                            );
                        }
                    }
                    GroupDeltaCommon::GroupMerge(group_merge) => {
                        tracing::info!(
                            "group_merge left {:?} right {:?}",
                            group_merge.left_group_id,
                            group_merge.right_group_id
                        );
                        self.merge_compaction_group(
                            group_merge.left_group_id,
                            group_merge.right_group_id,
                        )
                    }
                    GroupDeltaCommon::IntraLevel(level_delta) => {
                        let levels =
                            self.levels.get_mut(compaction_group_id).unwrap_or_else(|| {
                                panic!("compaction group {} does not exist", compaction_group_id)
                            });
                        if is_commit_epoch {
                            assert!(
                                level_delta.removed_table_ids.is_empty(),
                                "no sst should be deleted when committing an epoch"
                            );

                            let IntraLevelDelta {
                                level_idx,
                                l0_sub_level_id,
                                inserted_table_infos,
                                ..
                            } = level_delta;
                            {
                                assert_eq!(
                                    *level_idx, 0,
                                    "we should only add to L0 when we commit an epoch."
                                );
                                if !inserted_table_infos.is_empty() {
                                    insert_new_sub_level(
                                        &mut levels.l0,
                                        *l0_sub_level_id,
                                        PbLevelType::Overlapping,
                                        inserted_table_infos.clone(),
                                        None,
                                    );
                                }
                            }
                        } else {
                            // The delta is caused by compaction.
                            levels.apply_compact_ssts(
                                level_delta,
                                self.state_table_info
                                    .compaction_group_member_table_ids(*compaction_group_id),
                            );
                            if level_delta.level_idx == 0 {
                                is_applied_l0_compact = true;
                            }
                        }
                    }
                    GroupDeltaCommon::NewL0SubLevel(inserted_table_infos) => {
                        let levels =
                            self.levels.get_mut(compaction_group_id).unwrap_or_else(|| {
                                panic!("compaction group {} does not exist", compaction_group_id)
                            });
                        assert!(is_commit_epoch);

                        if !inserted_table_infos.is_empty() {
                            let next_l0_sub_level_id = levels
                                .l0
                                .sub_levels
                                .last()
                                .map(|level| level.sub_level_id + 1)
                                .unwrap_or(1);

                            insert_new_sub_level(
                                &mut levels.l0,
                                next_l0_sub_level_id,
                                PbLevelType::Overlapping,
                                inserted_table_infos.clone(),
                                None,
                            );
                        }
                    }
                    GroupDeltaCommon::GroupDestroy(_) => {
                        self.levels.remove(compaction_group_id);
                    }

                    GroupDeltaCommon::PruneTableIdsFromSsts(table_ids) => {
                        self.levels
                            .get_mut(compaction_group_id)
                            .unwrap_or_else(|| {
                                panic!("compaction group {} does not exist", compaction_group_id)
                            })
                            .prune_table_ids_from_ssts(table_ids);
                    }
                }
            }

            if is_applied_l0_compact && let Some(levels) = self.levels.get_mut(compaction_group_id)
            {
                levels.l0.normalize();
            }
        }
        self.id = version_delta.id;
        #[expect(deprecated)]
        {
            self.max_committed_epoch = version_delta.max_committed_epoch;
        }

        // apply to table watermark

        // Store the table watermarks that needs to be updated. None means to remove the table watermark of the table id
        let mut modified_table_watermarks: HashMap<TableId, Option<TableWatermarks>> =
            HashMap::new();

        // apply to table watermark
        for (table_id, table_watermarks) in &version_delta.new_table_watermarks {
            if let Some(current_table_watermarks) = self.table_watermarks.get(table_id) {
                if version_delta.removed_table_ids.contains(table_id) {
                    modified_table_watermarks.insert(*table_id, None);
                } else {
                    let mut current_table_watermarks = (**current_table_watermarks).clone();
                    current_table_watermarks.apply_new_table_watermarks(table_watermarks);
                    modified_table_watermarks.insert(*table_id, Some(current_table_watermarks));
                }
            } else {
                modified_table_watermarks.insert(*table_id, Some(table_watermarks.clone()));
            }
        }
        for table_id in &version_delta.removed_table_ids {
            modified_table_watermarks.insert(*table_id, None);
        }
        for (table_id, table_watermarks) in &self.table_watermarks {
            let safe_epoch = if let Some(state_table_info) =
                self.state_table_info.info().get(table_id)
                && let Some((oldest_epoch, _)) = table_watermarks.watermarks.first()
                && state_table_info.committed_epoch > *oldest_epoch
            {
                // safe epoch has progressed, need further clear.
                state_table_info.committed_epoch
            } else {
                // safe epoch not progressed or the table has been removed. No need to truncate
                continue;
            };
            let table_watermarks = modified_table_watermarks
                .entry(*table_id)
                .or_insert_with(|| Some((**table_watermarks).clone()));
            if let Some(table_watermarks) = table_watermarks {
                table_watermarks.clear_stale_epoch_watermark(safe_epoch);
            }
        }
        // apply the staging table watermark to hummock version
        for (table_id, table_watermarks) in modified_table_watermarks {
            if let Some(table_watermarks) = table_watermarks {
                self.table_watermarks
                    .insert(table_id, Arc::new(table_watermarks));
            } else {
                self.table_watermarks.remove(&table_id);
            }
        }
        // apply to vector index
        apply_vector_index_delta(
            &mut self.vector_indexes,
            &version_delta.vector_index_delta,
            &version_delta.removed_table_ids,
        );

        changed_table_info
    }

    pub fn apply_change_log_delta<T: Clone>(
        table_change_log: &mut HashMap<TableId, TableChangeLogCommon<T>>,
        change_log_delta: &HashMap<TableId, EpochNewChangeLogCommon<T>>,
    ) {
        for (table_id, new_change_log) in change_log_delta {
            match table_change_log.entry(*table_id) {
                Entry::Occupied(entry) => {
                    let change_log = entry.into_mut();
                    change_log.add_change_log(new_change_log.clone());
                }
                Entry::Vacant(entry) => {
                    entry.insert(TableChangeLogCommon::new(once(new_change_log.clone())));
                }
            };
        }
    }

    /// Returns the log deltas required to truncate the entire change log for a table.
    pub fn collect_gc_change_log_delta<'a, T: Clone>(
        current_change_log_table_ids: impl Iterator<Item = &'a TableId>,
        change_log_delta: &HashMap<TableId, EpochNewChangeLogCommon<T>>,
        removed_table_ids: &HashSet<TableId>,
        state_table_info_delta: &HashMap<TableId, StateTableInfoDelta>,
        changed_table_info: &HashMap<TableId, Option<StateTableInfo>>,
    ) -> HashSet<TableId> {
        let mut gc_change_log_delta = HashSet::new();
        // If a table has no new change log entry (even an empty one), it means we have stopped maintained
        // the change log for the table, and then we will remove the table change log.
        // The table change log will also be removed when the table id is removed.
        for table_id in current_change_log_table_ids {
            if removed_table_ids.contains(table_id) {
                gc_change_log_delta.insert(*table_id);
                continue;
            }
            if let Some(table_info_delta) = state_table_info_delta.get(table_id)
                && let Some(Some(prev_table_info)) = changed_table_info.get(table_id)
                && table_info_delta.committed_epoch > prev_table_info.committed_epoch
            {
                // the table exists previously, and its committed epoch has progressed.
            } else {
                // otherwise, the table change log should be kept anyway
                continue;
            }
            let contains = change_log_delta.contains_key(table_id);
            if !contains {
                gc_change_log_delta.insert(*table_id);
                static LOG_SUPPRESSOR: LazyLock<LogSuppressor> =
                    LazyLock::new(|| LogSuppressor::per_second(1));
                if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                    warn!(
                        suppressed_count,
                        %table_id,
                        "table change log dropped due to no further change log at newly committed epoch"
                    );
                }
            }
        }
        gc_change_log_delta
    }

    pub fn build_branched_sst_info(&self) -> BTreeMap<HummockSstableObjectId, BranchedSstInfo> {
        let mut ret: BTreeMap<_, _> = BTreeMap::new();
        for (compaction_group_id, group) in &self.levels {
            let mut levels = vec![];
            levels.extend(group.l0.sub_levels.iter());
            levels.extend(group.levels.iter());
            for level in levels {
                for table_info in &level.table_infos {
                    if table_info.sst_id.as_raw_id() == table_info.object_id.as_raw_id() {
                        continue;
                    }
                    let object_id = table_info.object_id;
                    let entry: &mut BranchedSstInfo = ret.entry(object_id).or_default();
                    entry
                        .entry(*compaction_group_id)
                        .or_default()
                        .push(table_info.sst_id)
                }
            }
        }
        ret
    }

    pub fn merge_compaction_group(
        &mut self,
        left_group_id: CompactionGroupId,
        right_group_id: CompactionGroupId,
    ) {
        // Double check
        let left_group_id_table_ids = self
            .state_table_info
            .compaction_group_member_table_ids(left_group_id)
            .iter();
        let right_group_id_table_ids = self
            .state_table_info
            .compaction_group_member_table_ids(right_group_id)
            .iter();

        assert!(
            left_group_id_table_ids
                .chain(right_group_id_table_ids)
                .is_sorted()
        );

        let total_cg = self.levels.keys().cloned().collect::<Vec<_>>();
        let right_levels = self.levels.remove(&right_group_id).unwrap_or_else(|| {
            panic!(
                "compaction group should exist right {} all {:?}",
                right_group_id, total_cg
            )
        });

        let left_levels = self.levels.get_mut(&left_group_id).unwrap_or_else(|| {
            panic!(
                "compaction group should exist left {} all {:?}",
                left_group_id, total_cg
            )
        });

        group_split::merge_levels(left_levels, right_levels);
    }

    pub fn init_with_parent_group_v2(
        &mut self,
        parent_group_id: CompactionGroupId,
        group_id: CompactionGroupId,
        new_sst_start_id: HummockSstableId,
        split_key: Option<Bytes>,
    ) {
        let mut new_sst_id = new_sst_start_id;
        if parent_group_id == StaticCompactionGroupId::NewCompactionGroup {
            if new_sst_start_id != 0 {
                if cfg!(debug_assertions) {
                    panic!(
                        "non-zero sst start id {} for NewCompactionGroup",
                        new_sst_start_id
                    );
                } else {
                    warn!(
                        %new_sst_start_id,
                        "non-zero sst start id for NewCompactionGroup"
                    );
                }
            }
            return;
        } else if !self.levels.contains_key(&parent_group_id) {
            unreachable!(
                "non-existing parent group id {} to init from (V2)",
                parent_group_id
            );
        }

        let [parent_levels, cur_levels] = self
            .levels
            .get_disjoint_mut([&parent_group_id, &group_id])
            .map(|res| res.unwrap());
        // After certain compaction group operation, e.g. split, any ongoing compaction tasks created prior to that should be rejected due to expiration.
        // By incrementing the compaction_group_version_id of the compaction group, and comparing it with the one recorded in compaction task, expired compaction tasks can be identified.
        parent_levels.compaction_group_version_id += 1;
        cur_levels.compaction_group_version_id += 1;

        let l0 = &mut parent_levels.l0;
        {
            for sub_level in &mut l0.sub_levels {
                let target_l0 = &mut cur_levels.l0;
                // Remove SST from sub level may result in empty sub level. It will be purged
                // whenever another compaction task is finished.
                let insert_table_infos = if let Some(split_key) = &split_key {
                    group_split::split_sst_info_for_level_v2(
                        sub_level,
                        &mut new_sst_id,
                        split_key.clone(),
                    )
                } else {
                    vec![]
                };

                if insert_table_infos.is_empty() {
                    continue;
                }

                sub_level.normalize();
                match group_split::get_sub_level_insert_hint(&target_l0.sub_levels, sub_level) {
                    Ok(idx) => {
                        add_ssts_to_sub_level(target_l0, idx, insert_table_infos);
                    }
                    Err(idx) => {
                        insert_new_sub_level(
                            target_l0,
                            sub_level.sub_level_id,
                            sub_level.level_type,
                            insert_table_infos,
                            Some(idx),
                        );
                    }
                }
            }
            l0.normalize();
        }

        for (idx, level) in parent_levels.levels.iter_mut().enumerate() {
            let insert_table_infos = if let Some(split_key) = &split_key {
                group_split::split_sst_info_for_level_v2(level, &mut new_sst_id, split_key.clone())
            } else {
                vec![]
            };

            if insert_table_infos.is_empty() {
                continue;
            }

            cur_levels.levels[idx].total_file_size += insert_table_infos
                .iter()
                .map(|sst| sst.sst_size)
                .sum::<u64>();
            cur_levels.levels[idx].uncompressed_file_size += insert_table_infos
                .iter()
                .map(|sst| sst.uncompressed_file_size)
                .sum::<u64>();
            cur_levels.levels[idx]
                .table_infos
                .extend(insert_table_infos);
            cur_levels.levels[idx]
                .table_infos
                .sort_by(|sst1, sst2| sst1.key_range.cmp(&sst2.key_range));
            assert!(can_concat(&cur_levels.levels[idx].table_infos));
            level.normalize();
        }

        assert!(
            parent_levels
                .l0
                .sub_levels
                .iter()
                .all(|level| !level.table_infos.is_empty())
        );
        assert!(
            cur_levels
                .l0
                .sub_levels
                .iter()
                .all(|level| !level.table_infos.is_empty())
        );
    }
}

impl<T> HummockVersionCommon<T>
where
    T: SstableIdReader + ObjectIdReader,
{
    pub fn get_object_ids(&self) -> impl Iterator<Item = HummockObjectId> + '_ {
        // DO NOT REMOVE THIS LINE
        // This is to ensure that when adding new variant to `HummockObjectId`,
        // the compiler will warn us if we forget to handle it here.
        match HummockObjectId::Sstable(0.into()) {
            HummockObjectId::Sstable(_) => {}
            HummockObjectId::VectorFile(_) => {}
            HummockObjectId::HnswGraphFile(_) => {}
        };
        self.get_sst_infos()
            .map(|s| HummockObjectId::Sstable(s.object_id()))
            .chain(
                self.vector_indexes
                    .values()
                    .flat_map(|index| index.get_objects().map(|(object_id, _)| object_id)),
            )
    }

    pub fn get_sst_ids(&self) -> HashSet<HummockSstableId> {
        self.get_sst_infos().map(|s| s.sst_id()).collect()
    }

    pub fn get_sst_infos(&self) -> impl Iterator<Item = &T> {
        self.get_combined_levels()
            .flat_map(|level| level.table_infos.iter())
    }
}

impl Levels {
    pub(crate) fn apply_compact_ssts(
        &mut self,
        level_delta: &IntraLevelDeltaCommon<SstableInfo>,
        member_table_ids: &BTreeSet<TableId>,
    ) {
        let IntraLevelDeltaCommon {
            level_idx,
            l0_sub_level_id,
            inserted_table_infos: insert_table_infos,
            vnode_partition_count,
            removed_table_ids: delete_sst_ids_set,
            compaction_group_version_id,
        } = level_delta;
        let new_vnode_partition_count = *vnode_partition_count;

        if is_compaction_task_expired(
            self.compaction_group_version_id,
            *compaction_group_version_id,
        ) {
            warn!(
                current_compaction_group_version_id = self.compaction_group_version_id,
                delta_compaction_group_version_id = compaction_group_version_id,
                level_idx,
                l0_sub_level_id,
                insert_table_infos = ?insert_table_infos
                    .iter()
                    .map(|sst| (sst.sst_id, sst.object_id))
                    .collect_vec(),
                ?delete_sst_ids_set,
                "This VersionDelta may be committed by an expired compact task. Please check it."
            );
            return;
        }
        if !delete_sst_ids_set.is_empty() {
            if *level_idx == 0 {
                for level in &mut self.l0.sub_levels {
                    level.delete_ssts(delete_sst_ids_set);
                }
            } else {
                let idx = *level_idx as usize - 1;
                self.levels[idx].delete_ssts(delete_sst_ids_set);
            }
        }

        if !insert_table_infos.is_empty() {
            let insert_sst_level_id = *level_idx;
            let insert_sub_level_id = *l0_sub_level_id;
            if insert_sst_level_id == 0 {
                let l0 = &mut self.l0;
                let index = l0
                    .sub_levels
                    .partition_point(|level| level.sub_level_id < insert_sub_level_id);
                assert!(
                    index < l0.sub_levels.len()
                        && l0.sub_levels[index].sub_level_id == insert_sub_level_id,
                    "should find the level to insert into when applying compaction generated delta. sub level idx: {},  removed sst ids: {:?}, sub levels: {:?},",
                    insert_sub_level_id,
                    delete_sst_ids_set,
                    l0.sub_levels
                        .iter()
                        .map(|level| level.sub_level_id)
                        .collect_vec()
                );
                if l0.sub_levels[index].table_infos.is_empty()
                    && member_table_ids.len() == 1
                    && insert_table_infos.iter().all(|sst| {
                        sst.table_ids.len() == 1
                            && sst.table_ids[0]
                                == *member_table_ids.iter().next().expect("non-empty")
                    })
                {
                    // Only change vnode_partition_count for group which has only one state-table.
                    // Only change vnode_partition_count for level which update all sst files in this compact task.
                    l0.sub_levels[index].vnode_partition_count = new_vnode_partition_count;
                }
                level_insert_ssts(&mut l0.sub_levels[index], insert_table_infos);
            } else {
                let idx = insert_sst_level_id as usize - 1;
                if self.levels[idx].table_infos.is_empty()
                    && insert_table_infos
                        .iter()
                        .all(|sst| sst.table_ids.len() == 1)
                {
                    self.levels[idx].vnode_partition_count = new_vnode_partition_count;
                } else if self.levels[idx].vnode_partition_count != 0
                    && new_vnode_partition_count == 0
                    && member_table_ids.len() > 1
                {
                    self.levels[idx].vnode_partition_count = 0;
                }
                level_insert_ssts(&mut self.levels[idx], insert_table_infos);
            }
        }
    }

    /// Prune specified table ids from all SST metadata, remove emptied SSTs and sub-levels,
    /// then bump `compaction_group_version_id`.
    pub(crate) fn prune_table_ids_from_ssts(&mut self, table_ids: &HashSet<TableId>) {
        for level in self.l0.sub_levels.iter_mut().chain(self.levels.iter_mut()) {
            level.prune_table_ids_from_ssts(table_ids);
        }
        self.l0.normalize();
        self.compaction_group_version_id += 1;
    }
}

impl<T> HummockVersionCommon<T> {
    pub fn get_combined_levels(&self) -> impl Iterator<Item = &'_ LevelCommon<T>> + '_ {
        self.levels
            .values()
            .flat_map(|level| level.l0.sub_levels.iter().rev().chain(level.levels.iter()))
    }
}

pub fn build_initial_compaction_group_levels(
    group_id: impl Into<CompactionGroupId>,
    compaction_config: &CompactionConfig,
) -> Levels {
    let mut levels = vec![];
    for l in 0..compaction_config.get_max_level() {
        levels.push(Level {
            level_idx: (l + 1) as u32,
            level_type: PbLevelType::Nonoverlapping,
            table_infos: vec![],
            total_file_size: 0,
            sub_level_id: 0,
            uncompressed_file_size: 0,
            vnode_partition_count: 0,
        });
    }
    #[expect(deprecated)] // for backward-compatibility of previous hummock version delta
    Levels {
        levels,
        l0: OverlappingLevel {
            sub_levels: vec![],
            total_file_size: 0,
            uncompressed_file_size: 0,
        },
        group_id: group_id.into(),
        parent_group_id: 0.into(),
        member_table_ids: vec![],
        compaction_group_version_id: 0,
    }
}

fn split_sst_info_for_level(
    member_table_ids: &BTreeSet<TableId>,
    level: &mut Level,
    new_sst_id: &mut HummockSstableId,
) -> Vec<SstableInfo> {
    // Remove SST from sub level may result in empty sub level. It will be purged
    // whenever another compaction task is finished.
    let mut insert_table_infos = vec![];
    for sst_info in &mut level.table_infos {
        let removed_table_ids = sst_info
            .table_ids
            .iter()
            .filter(|table_id| member_table_ids.contains(*table_id))
            .cloned()
            .collect_vec();
        let sst_size = sst_info.sst_size;
        if sst_size / 2 == 0 {
            tracing::warn!(
                id = %sst_info.sst_id,
                object_id = %sst_info.object_id,
                sst_size = sst_info.sst_size,
                file_size = sst_info.file_size,
                "Sstable sst_size is under expected",
            );
        };
        if !removed_table_ids.is_empty() {
            let (modified_sst, branch_sst) = split_sst_with_table_ids(
                sst_info,
                new_sst_id,
                sst_size / 2,
                sst_size / 2,
                member_table_ids.iter().cloned().collect_vec(),
            );
            *sst_info = modified_sst;
            insert_table_infos.push(branch_sst);
        }
    }
    insert_table_infos
}

/// Gets all compaction group ids.
pub fn get_compaction_group_ids(
    version: &HummockVersion,
) -> impl Iterator<Item = CompactionGroupId> + '_ {
    version.levels.keys().cloned()
}

pub fn get_table_compaction_group_id_mapping(
    version: &HummockVersion,
) -> HashMap<StateTableId, CompactionGroupId> {
    version
        .state_table_info
        .info()
        .iter()
        .map(|(table_id, info)| (*table_id, info.compaction_group_id))
        .collect()
}

/// Gets all SSTs in `group_id`
pub fn get_compaction_group_ssts(
    version: &HummockVersion,
    group_id: CompactionGroupId,
) -> impl Iterator<Item = (HummockSstableObjectId, HummockSstableId)> + '_ {
    let group_levels = version.get_compaction_group_levels(group_id);
    group_levels
        .l0
        .sub_levels
        .iter()
        .rev()
        .chain(group_levels.levels.iter())
        .flat_map(|level| {
            level
                .table_infos
                .iter()
                .map(|table_info| (table_info.object_id, table_info.sst_id))
        })
}

pub fn new_sub_level(
    sub_level_id: u64,
    level_type: PbLevelType,
    table_infos: Vec<SstableInfo>,
) -> Level {
    if level_type == PbLevelType::Nonoverlapping {
        debug_assert!(
            can_concat(&table_infos),
            "sst of non-overlapping level is not concat-able: {:?}",
            table_infos
        );
    }
    let total_file_size = table_infos.iter().map(|table| table.sst_size).sum();
    let uncompressed_file_size = table_infos
        .iter()
        .map(|table| table.uncompressed_file_size)
        .sum();
    Level {
        level_idx: 0,
        level_type,
        table_infos,
        total_file_size,
        sub_level_id,
        uncompressed_file_size,
        vnode_partition_count: 0,
    }
}

pub fn add_ssts_to_sub_level(
    l0: &mut OverlappingLevel,
    sub_level_idx: usize,
    insert_table_infos: Vec<SstableInfo>,
) {
    insert_table_infos.iter().for_each(|sst| {
        l0.sub_levels[sub_level_idx].total_file_size += sst.sst_size;
        l0.sub_levels[sub_level_idx].uncompressed_file_size += sst.uncompressed_file_size;
        l0.total_file_size += sst.sst_size;
        l0.uncompressed_file_size += sst.uncompressed_file_size;
    });
    l0.sub_levels[sub_level_idx]
        .table_infos
        .extend(insert_table_infos);
    if l0.sub_levels[sub_level_idx].level_type == PbLevelType::Nonoverlapping {
        l0.sub_levels[sub_level_idx]
            .table_infos
            .sort_by(|sst1, sst2| sst1.key_range.cmp(&sst2.key_range));
        assert!(
            can_concat(&l0.sub_levels[sub_level_idx].table_infos),
            "sstable ids: {:?}",
            l0.sub_levels[sub_level_idx]
                .table_infos
                .iter()
                .map(|sst| sst.sst_id)
                .collect_vec()
        );
    }
}

/// `None` value of `sub_level_insert_hint` means append.
pub fn insert_new_sub_level(
    l0: &mut OverlappingLevel,
    insert_sub_level_id: u64,
    level_type: PbLevelType,
    insert_table_infos: Vec<SstableInfo>,
    sub_level_insert_hint: Option<usize>,
) {
    if insert_sub_level_id == u64::MAX {
        return;
    }
    let insert_pos = if let Some(insert_pos) = sub_level_insert_hint {
        insert_pos
    } else {
        if let Some(newest_level) = l0.sub_levels.last() {
            assert!(
                newest_level.sub_level_id < insert_sub_level_id,
                "inserted new level is not the newest: prev newest: {}, insert: {}. L0: {:?}",
                newest_level.sub_level_id,
                insert_sub_level_id,
                l0,
            );
        }
        l0.sub_levels.len()
    };
    #[cfg(debug_assertions)]
    {
        if insert_pos > 0
            && let Some(smaller_level) = l0.sub_levels.get(insert_pos - 1)
        {
            debug_assert!(smaller_level.sub_level_id < insert_sub_level_id);
        }
        if let Some(larger_level) = l0.sub_levels.get(insert_pos) {
            debug_assert!(larger_level.sub_level_id > insert_sub_level_id);
        }
    }
    // All files will be committed in one new Overlapping sub-level and become
    // Nonoverlapping  after at least one compaction.
    let level = new_sub_level(insert_sub_level_id, level_type, insert_table_infos);
    l0.total_file_size += level.total_file_size;
    l0.uncompressed_file_size += level.uncompressed_file_size;
    l0.sub_levels.insert(insert_pos, level);
}

impl Level {
    fn recompute_size(&mut self) {
        self.total_file_size = self
            .table_infos
            .iter()
            .map(|table| table.sst_size)
            .sum::<u64>();
        self.uncompressed_file_size = self
            .table_infos
            .iter()
            .map(|table| table.uncompressed_file_size)
            .sum::<u64>();
    }

    /// Remove SSTs with empty `table_ids`, then recompute sizes.
    fn normalize(&mut self) {
        self.table_infos
            .retain(|sst_info| !sst_info.table_ids.is_empty());
        self.recompute_size();
    }

    /// Return `true` if any SST was actually removed.
    fn delete_ssts(&mut self, ids: &HashSet<HummockSstableId>) -> bool {
        let original_len = self.table_infos.len();
        self.table_infos
            .retain(|table| !ids.contains(&table.sst_id));
        self.recompute_size();
        original_len != self.table_infos.len()
    }

    /// Prune specified `table_ids` from each SST's metadata,
    /// remove SSTs that become empty, then recompute sizes.
    fn prune_table_ids_from_ssts(&mut self, table_ids: &HashSet<TableId>) {
        for sstable_info in &mut self.table_infos {
            if !sstable_info
                .table_ids
                .iter()
                .any(|table_id| table_ids.contains(table_id))
            {
                continue;
            }

            let mut inner = sstable_info.get_inner();
            inner.table_ids.retain(|id| !table_ids.contains(id));
            sstable_info.set_inner(inner);
        }
        self.normalize();
    }
}

impl OverlappingLevel {
    /// Remove empty sub-levels, then recompute aggregated sizes.
    fn normalize(&mut self) {
        self.sub_levels
            .retain(|level| !level.table_infos.is_empty());
        self.total_file_size = self
            .sub_levels
            .iter()
            .map(|level| level.total_file_size)
            .sum::<u64>();
        self.uncompressed_file_size = self
            .sub_levels
            .iter()
            .map(|level| level.uncompressed_file_size)
            .sum::<u64>();
    }
}

fn level_insert_ssts(operand: &mut Level, insert_table_infos: &Vec<SstableInfo>) {
    fn display_sstable_infos(ssts: &[impl Borrow<SstableInfo>]) -> String {
        format!(
            "sstable ids: {:?}",
            ssts.iter().map(|s| s.borrow().sst_id).collect_vec()
        )
    }
    operand.total_file_size += insert_table_infos
        .iter()
        .map(|sst| sst.sst_size)
        .sum::<u64>();
    operand.uncompressed_file_size += insert_table_infos
        .iter()
        .map(|sst| sst.uncompressed_file_size)
        .sum::<u64>();
    if operand.level_type == PbLevelType::Overlapping {
        operand.level_type = PbLevelType::Nonoverlapping;
        operand
            .table_infos
            .extend(insert_table_infos.iter().cloned());
        operand
            .table_infos
            .sort_by(|sst1, sst2| sst1.key_range.cmp(&sst2.key_range));
        assert!(
            can_concat(&operand.table_infos),
            "{}",
            display_sstable_infos(&operand.table_infos)
        );
    } else if !insert_table_infos.is_empty() {
        let sorted_insert: Vec<_> = insert_table_infos
            .iter()
            .sorted_by(|sst1, sst2| sst1.key_range.cmp(&sst2.key_range))
            .cloned()
            .collect();
        let first = &sorted_insert[0];
        let last = &sorted_insert[sorted_insert.len() - 1];
        let pos = operand
            .table_infos
            .partition_point(|b| b.key_range.cmp(&first.key_range) == Ordering::Less);
        if pos >= operand.table_infos.len()
            || last.key_range.cmp(&operand.table_infos[pos].key_range) == Ordering::Less
        {
            operand.table_infos.splice(pos..pos, sorted_insert);
            // Validate the inserted SST batch along with the two SSTs that precede and follow it.
            let validate_range = operand
                .table_infos
                .iter()
                .skip(pos.saturating_sub(1))
                .take(insert_table_infos.len() + 2)
                .collect_vec();
            assert!(
                can_concat(&validate_range),
                "{}",
                display_sstable_infos(&validate_range),
            );
        } else {
            // If this branch is reached, it indicates some unexpected behavior in compaction.
            // Here we issue a warning and fall back to insert one by one.
            warn!(insert = ?insert_table_infos, level = ?operand.table_infos, "unexpected overlap");
            for i in insert_table_infos {
                let pos = operand
                    .table_infos
                    .partition_point(|b| b.key_range.cmp(&i.key_range) == Ordering::Less);
                operand.table_infos.insert(pos, i.clone());
            }
            assert!(
                can_concat(&operand.table_infos),
                "{}",
                display_sstable_infos(&operand.table_infos)
            );
        }
    }
}

pub fn version_object_size_map(version: &HummockVersion) -> HashMap<HummockObjectId, u64> {
    // DO NOT REMOVE THIS LINE
    // This is to ensure that when adding new variant to `HummockObjectId`,
    // the compiler will warn us if we forget to handle it here.
    match HummockObjectId::Sstable(0.into()) {
        HummockObjectId::Sstable(_) => {}
        HummockObjectId::VectorFile(_) => {}
        HummockObjectId::HnswGraphFile(_) => {}
    };
    version
        .levels
        .values()
        .flat_map(|cg| {
            cg.level0()
                .sub_levels
                .iter()
                .chain(cg.levels.iter())
                .flat_map(|level| level.table_infos.iter().map(|t| (t.object_id, t.file_size)))
        })
        .map(|(object_id, size)| (HummockObjectId::Sstable(object_id), size))
        .chain(
            version
                .vector_indexes
                .values()
                .flat_map(|index| index.get_objects()),
        )
        .collect()
}

/// Verify the validity of a `HummockVersion` and return a list of violations if any.
/// Currently this method is only used by risectl validate-version.
pub fn validate_version(version: &HummockVersion) -> Vec<String> {
    let mut res = Vec::new();
    // Ensure each table maps to only one compaction group
    for (group_id, levels) in &version.levels {
        // Ensure compaction group id matches
        if levels.group_id != *group_id {
            res.push(format!(
                "GROUP {}: inconsistent group id {} in Levels",
                group_id, levels.group_id
            ));
        }

        let validate_level = |group: CompactionGroupId,
                              expected_level_idx: u32,
                              level: &Level,
                              res: &mut Vec<String>| {
            let mut level_identifier = format!("GROUP {} LEVEL {}", group, level.level_idx);
            if level.level_idx == 0 {
                level_identifier.push_str(format!("SUBLEVEL {}", level.sub_level_id).as_str());
                // Ensure sub-level is not empty
                if level.table_infos.is_empty() {
                    res.push(format!("{}: empty level", level_identifier));
                }
            } else if level.level_type != PbLevelType::Nonoverlapping {
                // Ensure non-L0 level is non-overlapping level
                res.push(format!(
                    "{}: level type {:?} is not non-overlapping",
                    level_identifier, level.level_type
                ));
            }

            // Ensure level idx matches
            if level.level_idx != expected_level_idx {
                res.push(format!(
                    "{}: mismatched level idx {}",
                    level_identifier, expected_level_idx
                ));
            }

            let mut prev_table_info: Option<&SstableInfo> = None;
            for table_info in &level.table_infos {
                // Ensure table_ids are sorted and unique
                if !table_info.table_ids.is_sorted_by(|a, b| a < b) {
                    res.push(format!(
                        "{} SST {}: table_ids not sorted",
                        level_identifier, table_info.object_id
                    ));
                }

                // Ensure SSTs in non-overlapping level have non-overlapping key range
                if level.level_type == PbLevelType::Nonoverlapping {
                    if let Some(prev) = prev_table_info.take()
                        && prev
                            .key_range
                            .compare_right_with(&table_info.key_range.left)
                            != Ordering::Less
                    {
                        res.push(format!(
                            "{} SST {}: key range should not overlap. prev={:?}, cur={:?}",
                            level_identifier, table_info.object_id, prev, table_info
                        ));
                    }
                    let _ = prev_table_info.insert(table_info);
                }
            }
        };

        let l0 = &levels.l0;
        let mut prev_sub_level_id = u64::MAX;
        for sub_level in &l0.sub_levels {
            // Ensure sub_level_id is sorted and unique
            if sub_level.sub_level_id >= prev_sub_level_id {
                res.push(format!(
                    "GROUP {} LEVEL 0: sub_level_id {} >= prev_sub_level {}",
                    group_id, sub_level.level_idx, prev_sub_level_id
                ));
            }
            prev_sub_level_id = sub_level.sub_level_id;

            validate_level(*group_id, 0, sub_level, &mut res);
        }

        for idx in 1..=levels.levels.len() {
            validate_level(*group_id, idx as u32, levels.get_level(idx), &mut res);
        }
    }
    res
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use bytes::Bytes;
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_pb::hummock::{
        CompactionConfig, GroupConstruct, GroupDestroy, LevelType, StateTableInfo,
    };

    use super::group_split;
    use crate::HummockVersionId;
    use crate::compaction_group::group_split::*;
    use crate::compaction_group::hummock_version_ext::build_initial_compaction_group_levels;
    use crate::key::{FullKey, gen_key_from_str};
    use crate::key_range::KeyRange;
    use crate::level::{Level, Levels, OverlappingLevel};
    use crate::sstable_info::{SstableInfo, SstableInfoInner};
    use crate::table_watermark::{
        TableWatermarks, VnodeWatermark, WatermarkDirection, WatermarkSerdeType,
    };
    use crate::version::{
        GroupDelta, GroupDeltas, HummockVersion, HummockVersionDelta, HummockVersionStateTableInfo,
        IntraLevelDelta,
    };

    fn gen_sstable_info(sst_id: u64, table_ids: Vec<u32>, epoch: u64) -> SstableInfo {
        gen_sstable_info_impl(sst_id, table_ids, epoch).into()
    }

    fn gen_sstable_info_impl(sst_id: u64, table_ids: Vec<u32>, epoch: u64) -> SstableInfoInner {
        let table_key_l = gen_key_from_str(VirtualNode::ZERO, "1");
        let table_key_r = gen_key_from_str(VirtualNode::MAX_FOR_TEST, "1");
        let full_key_l = FullKey::for_test(
            TableId::new(*table_ids.first().unwrap()),
            table_key_l,
            epoch,
        )
        .encode();
        let full_key_r =
            FullKey::for_test(TableId::new(*table_ids.last().unwrap()), table_key_r, epoch)
                .encode();

        SstableInfoInner {
            sst_id: sst_id.into(),
            key_range: KeyRange {
                left: full_key_l.into(),
                right: full_key_r.into(),
                right_exclusive: false,
            },
            table_ids: table_ids.into_iter().map(Into::into).collect(),
            object_id: sst_id.into(),
            min_epoch: 20,
            max_epoch: 20,
            file_size: 100,
            sst_size: 100,
            ..Default::default()
        }
    }

    #[test]
    fn test_get_sst_object_ids() {
        let mut version = HummockVersion {
            id: HummockVersionId::new(0),
            levels: HashMap::from_iter([(
                0.into(),
                Levels {
                    levels: vec![],
                    l0: OverlappingLevel {
                        sub_levels: vec![],
                        total_file_size: 0,
                        uncompressed_file_size: 0,
                    },
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        assert_eq!(version.get_object_ids().count(), 0);

        // Add to sub level
        version
            .levels
            .get_mut(&0)
            .unwrap()
            .l0
            .sub_levels
            .push(Level {
                table_infos: vec![
                    SstableInfoInner {
                        object_id: 11.into(),
                        sst_id: 11.into(),
                        ..Default::default()
                    }
                    .into(),
                ],
                ..Default::default()
            });
        assert_eq!(version.get_object_ids().count(), 1);

        // Add to non sub level
        version.levels.get_mut(&0).unwrap().levels.push(Level {
            table_infos: vec![
                SstableInfoInner {
                    object_id: 22.into(),
                    sst_id: 22.into(),
                    ..Default::default()
                }
                .into(),
            ],
            ..Default::default()
        });
        assert_eq!(version.get_object_ids().count(), 2);
    }

    #[test]
    fn test_apply_version_delta() {
        let mut version = HummockVersion {
            id: HummockVersionId::new(0),
            levels: HashMap::from_iter([
                (
                    0.into(),
                    build_initial_compaction_group_levels(
                        0,
                        &CompactionConfig {
                            max_level: 6,
                            ..Default::default()
                        },
                    ),
                ),
                (
                    1.into(),
                    build_initial_compaction_group_levels(
                        1,
                        &CompactionConfig {
                            max_level: 6,
                            ..Default::default()
                        },
                    ),
                ),
            ]),
            ..Default::default()
        };
        let version_delta = HummockVersionDelta {
            id: HummockVersionId::new(1),
            group_deltas: HashMap::from_iter([
                (
                    2.into(),
                    GroupDeltas {
                        group_deltas: vec![GroupDelta::GroupConstruct(Box::new(GroupConstruct {
                            group_config: Some(CompactionConfig {
                                max_level: 6,
                                ..Default::default()
                            }),
                            ..Default::default()
                        }))],
                    },
                ),
                (
                    0.into(),
                    GroupDeltas {
                        group_deltas: vec![GroupDelta::GroupDestroy(GroupDestroy {})],
                    },
                ),
                (
                    1.into(),
                    GroupDeltas {
                        group_deltas: vec![GroupDelta::IntraLevel(IntraLevelDelta::new(
                            1,
                            0,
                            HashSet::new(),
                            vec![
                                SstableInfoInner {
                                    object_id: 1.into(),
                                    sst_id: 1.into(),
                                    ..Default::default()
                                }
                                .into(),
                            ],
                            0,
                            version
                                .levels
                                .get(&1)
                                .as_ref()
                                .unwrap()
                                .compaction_group_version_id,
                        ))],
                    },
                ),
            ]),
            ..Default::default()
        };
        let version_delta = version_delta;

        version.apply_version_delta(&version_delta);
        let mut cg1 = build_initial_compaction_group_levels(
            1,
            &CompactionConfig {
                max_level: 6,
                ..Default::default()
            },
        );
        cg1.levels[0] = Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping,
            table_infos: vec![
                SstableInfoInner {
                    object_id: 1.into(),
                    sst_id: 1.into(),
                    ..Default::default()
                }
                .into(),
            ],
            ..Default::default()
        };
        assert_eq!(
            version,
            HummockVersion {
                id: HummockVersionId::new(1),
                levels: HashMap::from_iter([
                    (
                        2.into(),
                        build_initial_compaction_group_levels(
                            2,
                            &CompactionConfig {
                                max_level: 6,
                                ..Default::default()
                            },
                        ),
                    ),
                    (1.into(), cg1),
                ]),
                ..Default::default()
            }
        );
    }

    fn gen_sst_info(object_id: u64, table_ids: Vec<u32>, left: Bytes, right: Bytes) -> SstableInfo {
        gen_sst_info_impl(object_id, table_ids, left, right).into()
    }

    fn gen_sst_info_impl(
        object_id: u64,
        table_ids: Vec<u32>,
        left: Bytes,
        right: Bytes,
    ) -> SstableInfoInner {
        SstableInfoInner {
            object_id: object_id.into(),
            sst_id: object_id.into(),
            key_range: KeyRange {
                left,
                right,
                right_exclusive: false,
            },
            table_ids: table_ids.into_iter().map(Into::into).collect(),
            file_size: 100,
            sst_size: 100,
            uncompressed_file_size: 100,
            ..Default::default()
        }
    }

    #[test]
    fn test_merge_levels() {
        let mut left_levels = build_initial_compaction_group_levels(
            1,
            &CompactionConfig {
                max_level: 6,
                ..Default::default()
            },
        );

        let mut right_levels = build_initial_compaction_group_levels(
            2,
            &CompactionConfig {
                max_level: 6,
                ..Default::default()
            },
        );

        left_levels.levels[0] = Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping,
            table_infos: vec![
                gen_sst_info(
                    1,
                    vec![3],
                    FullKey::for_test(
                        TableId::new(3),
                        gen_key_from_str(VirtualNode::from_index(1), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                    FullKey::for_test(
                        TableId::new(3),
                        gen_key_from_str(VirtualNode::from_index(200), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                ),
                gen_sst_info(
                    10,
                    vec![3, 4],
                    FullKey::for_test(
                        TableId::new(3),
                        gen_key_from_str(VirtualNode::from_index(201), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                    FullKey::for_test(
                        TableId::new(4),
                        gen_key_from_str(VirtualNode::from_index(10), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                ),
                gen_sst_info(
                    11,
                    vec![4],
                    FullKey::for_test(
                        TableId::new(4),
                        gen_key_from_str(VirtualNode::from_index(11), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                    FullKey::for_test(
                        TableId::new(4),
                        gen_key_from_str(VirtualNode::from_index(200), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                ),
            ],
            total_file_size: 300,
            ..Default::default()
        };

        left_levels.l0.sub_levels.push(Level {
            level_idx: 0,
            table_infos: vec![gen_sst_info(
                3,
                vec![3],
                FullKey::for_test(
                    TableId::new(3),
                    gen_key_from_str(VirtualNode::from_index(1), "1"),
                    0,
                )
                .encode()
                .into(),
                FullKey::for_test(
                    TableId::new(3),
                    gen_key_from_str(VirtualNode::from_index(200), "1"),
                    0,
                )
                .encode()
                .into(),
            )],
            sub_level_id: 101,
            level_type: LevelType::Overlapping,
            total_file_size: 100,
            ..Default::default()
        });

        left_levels.l0.sub_levels.push(Level {
            level_idx: 0,
            table_infos: vec![gen_sst_info(
                3,
                vec![3],
                FullKey::for_test(
                    TableId::new(3),
                    gen_key_from_str(VirtualNode::from_index(1), "1"),
                    0,
                )
                .encode()
                .into(),
                FullKey::for_test(
                    TableId::new(3),
                    gen_key_from_str(VirtualNode::from_index(200), "1"),
                    0,
                )
                .encode()
                .into(),
            )],
            sub_level_id: 103,
            level_type: LevelType::Overlapping,
            total_file_size: 100,
            ..Default::default()
        });

        left_levels.l0.sub_levels.push(Level {
            level_idx: 0,
            table_infos: vec![gen_sst_info(
                3,
                vec![3],
                FullKey::for_test(
                    TableId::new(3),
                    gen_key_from_str(VirtualNode::from_index(1), "1"),
                    0,
                )
                .encode()
                .into(),
                FullKey::for_test(
                    TableId::new(3),
                    gen_key_from_str(VirtualNode::from_index(200), "1"),
                    0,
                )
                .encode()
                .into(),
            )],
            sub_level_id: 105,
            level_type: LevelType::Nonoverlapping,
            total_file_size: 100,
            ..Default::default()
        });

        right_levels.levels[0] = Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping,
            table_infos: vec![
                gen_sst_info(
                    1,
                    vec![5],
                    FullKey::for_test(
                        TableId::new(5),
                        gen_key_from_str(VirtualNode::from_index(1), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                    FullKey::for_test(
                        TableId::new(5),
                        gen_key_from_str(VirtualNode::from_index(200), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                ),
                gen_sst_info(
                    10,
                    vec![5, 6],
                    FullKey::for_test(
                        TableId::new(5),
                        gen_key_from_str(VirtualNode::from_index(201), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                    FullKey::for_test(
                        TableId::new(6),
                        gen_key_from_str(VirtualNode::from_index(10), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                ),
                gen_sst_info(
                    11,
                    vec![6],
                    FullKey::for_test(
                        TableId::new(6),
                        gen_key_from_str(VirtualNode::from_index(11), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                    FullKey::for_test(
                        TableId::new(6),
                        gen_key_from_str(VirtualNode::from_index(200), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                ),
            ],
            total_file_size: 300,
            ..Default::default()
        };

        right_levels.l0.sub_levels.push(Level {
            level_idx: 0,
            table_infos: vec![gen_sst_info(
                3,
                vec![5],
                FullKey::for_test(
                    TableId::new(5),
                    gen_key_from_str(VirtualNode::from_index(1), "1"),
                    0,
                )
                .encode()
                .into(),
                FullKey::for_test(
                    TableId::new(5),
                    gen_key_from_str(VirtualNode::from_index(200), "1"),
                    0,
                )
                .encode()
                .into(),
            )],
            sub_level_id: 101,
            level_type: LevelType::Overlapping,
            total_file_size: 100,
            ..Default::default()
        });

        right_levels.l0.sub_levels.push(Level {
            level_idx: 0,
            table_infos: vec![gen_sst_info(
                5,
                vec![5],
                FullKey::for_test(
                    TableId::new(5),
                    gen_key_from_str(VirtualNode::from_index(1), "1"),
                    0,
                )
                .encode()
                .into(),
                FullKey::for_test(
                    TableId::new(5),
                    gen_key_from_str(VirtualNode::from_index(200), "1"),
                    0,
                )
                .encode()
                .into(),
            )],
            sub_level_id: 102,
            level_type: LevelType::Overlapping,
            total_file_size: 100,
            ..Default::default()
        });

        right_levels.l0.sub_levels.push(Level {
            level_idx: 0,
            table_infos: vec![gen_sst_info(
                3,
                vec![5],
                FullKey::for_test(
                    TableId::new(5),
                    gen_key_from_str(VirtualNode::from_index(1), "1"),
                    0,
                )
                .encode()
                .into(),
                FullKey::for_test(
                    TableId::new(5),
                    gen_key_from_str(VirtualNode::from_index(200), "1"),
                    0,
                )
                .encode()
                .into(),
            )],
            sub_level_id: 103,
            level_type: LevelType::Nonoverlapping,
            total_file_size: 100,
            ..Default::default()
        });

        {
            // test empty
            let mut left_levels = Levels::default();
            let right_levels = Levels::default();

            group_split::merge_levels(&mut left_levels, right_levels);
        }

        {
            // test empty left
            let mut left_levels = build_initial_compaction_group_levels(
                1,
                &CompactionConfig {
                    max_level: 6,
                    ..Default::default()
                },
            );
            let right_levels = right_levels.clone();

            group_split::merge_levels(&mut left_levels, right_levels);

            assert!(left_levels.l0.sub_levels.len() == 3);
            assert!(left_levels.l0.sub_levels[0].sub_level_id == 101);
            assert_eq!(100, left_levels.l0.sub_levels[0].total_file_size);
            assert!(left_levels.l0.sub_levels[1].sub_level_id == 102);
            assert_eq!(100, left_levels.l0.sub_levels[1].total_file_size);
            assert!(left_levels.l0.sub_levels[2].sub_level_id == 103);
            assert_eq!(100, left_levels.l0.sub_levels[2].total_file_size);

            assert!(left_levels.levels[0].level_idx == 1);
            assert_eq!(300, left_levels.levels[0].total_file_size);
        }

        {
            // test empty right
            let mut left_levels = left_levels.clone();
            let right_levels = build_initial_compaction_group_levels(
                2,
                &CompactionConfig {
                    max_level: 6,
                    ..Default::default()
                },
            );

            group_split::merge_levels(&mut left_levels, right_levels);

            assert!(left_levels.l0.sub_levels.len() == 3);
            assert!(left_levels.l0.sub_levels[0].sub_level_id == 101);
            assert_eq!(100, left_levels.l0.sub_levels[0].total_file_size);
            assert!(left_levels.l0.sub_levels[1].sub_level_id == 103);
            assert_eq!(100, left_levels.l0.sub_levels[1].total_file_size);
            assert!(left_levels.l0.sub_levels[2].sub_level_id == 105);
            assert_eq!(100, left_levels.l0.sub_levels[2].total_file_size);

            assert!(left_levels.levels[0].level_idx == 1);
            assert_eq!(300, left_levels.levels[0].total_file_size);
        }

        {
            let mut left_levels = left_levels.clone();
            let right_levels = right_levels.clone();

            group_split::merge_levels(&mut left_levels, right_levels);

            assert!(left_levels.l0.sub_levels.len() == 6);
            assert!(left_levels.l0.sub_levels[0].sub_level_id == 101);
            assert_eq!(100, left_levels.l0.sub_levels[0].total_file_size);
            assert!(left_levels.l0.sub_levels[1].sub_level_id == 103);
            assert_eq!(100, left_levels.l0.sub_levels[1].total_file_size);
            assert!(left_levels.l0.sub_levels[2].sub_level_id == 105);
            assert_eq!(100, left_levels.l0.sub_levels[2].total_file_size);
            assert!(left_levels.l0.sub_levels[3].sub_level_id == 106);
            assert_eq!(100, left_levels.l0.sub_levels[3].total_file_size);
            assert!(left_levels.l0.sub_levels[4].sub_level_id == 107);
            assert_eq!(100, left_levels.l0.sub_levels[4].total_file_size);
            assert!(left_levels.l0.sub_levels[5].sub_level_id == 108);
            assert_eq!(100, left_levels.l0.sub_levels[5].total_file_size);

            assert!(left_levels.levels[0].level_idx == 1);
            assert_eq!(600, left_levels.levels[0].total_file_size);
        }
    }

    #[test]
    fn test_get_split_pos() {
        let epoch = test_epoch(1);
        let s1 = gen_sstable_info(1, vec![1, 2], epoch);
        let s2 = gen_sstable_info(2, vec![3, 4, 5], epoch);
        let s3 = gen_sstable_info(3, vec![6, 7], epoch);

        let ssts = vec![s1, s2, s3];
        let split_key = group_split::build_split_key(4.into(), VirtualNode::ZERO);

        let pos = group_split::get_split_pos(&ssts, split_key.clone());
        assert_eq!(1, pos);

        let pos = group_split::get_split_pos(&vec![], split_key);
        assert_eq!(0, pos);
    }

    #[test]
    fn test_split_sst() {
        let epoch = test_epoch(1);
        let sst = gen_sstable_info(1, vec![1, 2, 3, 5], epoch);

        {
            let split_key = group_split::build_split_key(3.into(), VirtualNode::ZERO);
            let origin_sst = sst.clone();
            let sst_size = origin_sst.sst_size;
            let split_type = group_split::need_to_split(&origin_sst, split_key.clone());
            assert_eq!(SstSplitType::Both, split_type);

            let mut new_sst_id = 10.into();
            let (origin_sst, branched_sst) = group_split::split_sst(
                origin_sst,
                &mut new_sst_id,
                split_key,
                sst_size / 2,
                sst_size / 2,
            );

            let origin_sst = origin_sst.unwrap();
            let branched_sst = branched_sst.unwrap();

            assert!(origin_sst.key_range.right_exclusive);
            assert!(
                origin_sst
                    .key_range
                    .right
                    .cmp(&branched_sst.key_range.left)
                    .is_le()
            );
            assert!(origin_sst.table_ids.is_sorted());
            assert!(branched_sst.table_ids.is_sorted());
            assert!(origin_sst.table_ids.last().unwrap() < branched_sst.table_ids.first().unwrap());
            assert!(branched_sst.sst_size < origin_sst.file_size);
            assert_eq!(10, branched_sst.sst_id);
            assert_eq!(11, origin_sst.sst_id);
            assert_eq!(3, branched_sst.table_ids.first().unwrap().as_raw_id()); // split table_id to right
        }

        {
            // test un-exist table_id
            let split_key = group_split::build_split_key(4.into(), VirtualNode::ZERO);
            let origin_sst = sst.clone();
            let sst_size = origin_sst.sst_size;
            let split_type = group_split::need_to_split(&origin_sst, split_key.clone());
            assert_eq!(SstSplitType::Both, split_type);

            let mut new_sst_id = 10.into();
            let (origin_sst, branched_sst) = group_split::split_sst(
                origin_sst,
                &mut new_sst_id,
                split_key,
                sst_size / 2,
                sst_size / 2,
            );

            let origin_sst = origin_sst.unwrap();
            let branched_sst = branched_sst.unwrap();

            assert!(origin_sst.key_range.right_exclusive);
            assert!(origin_sst.key_range.right.le(&branched_sst.key_range.left));
            assert!(origin_sst.table_ids.is_sorted());
            assert!(branched_sst.table_ids.is_sorted());
            assert!(origin_sst.table_ids.last().unwrap() < branched_sst.table_ids.first().unwrap());
            assert!(branched_sst.sst_size < origin_sst.file_size);
            assert_eq!(10, branched_sst.sst_id);
            assert_eq!(11, origin_sst.sst_id);
            assert_eq!(5, branched_sst.table_ids.first().unwrap().as_raw_id()); // split table_id to right
        }

        {
            let split_key = group_split::build_split_key(6.into(), VirtualNode::ZERO);
            let split_type = group_split::need_to_split(&sst, split_key);
            assert_eq!(SstSplitType::Left, split_type);
        }

        {
            let split_key = group_split::build_split_key(4.into(), VirtualNode::ZERO);
            let origin_sst = sst.clone();
            let split_type = group_split::need_to_split(&origin_sst, split_key);
            assert_eq!(SstSplitType::Both, split_type);

            let split_key = group_split::build_split_key(1.into(), VirtualNode::ZERO);
            let origin_sst = sst;
            let split_type = group_split::need_to_split(&origin_sst, split_key);
            assert_eq!(SstSplitType::Right, split_type);
        }

        {
            // test key_range left = right
            let mut sst = gen_sstable_info_impl(1, vec![1], epoch);
            sst.key_range.right = sst.key_range.left.clone();
            let sst: SstableInfo = sst.into();
            let split_key = group_split::build_split_key(1.into(), VirtualNode::ZERO);
            let origin_sst = sst;
            let sst_size = origin_sst.sst_size;

            let mut new_sst_id = 10.into();
            let (origin_sst, branched_sst) = group_split::split_sst(
                origin_sst,
                &mut new_sst_id,
                split_key,
                sst_size / 2,
                sst_size / 2,
            );

            assert!(origin_sst.is_none());
            assert!(branched_sst.is_some());
        }
    }

    #[test]
    fn test_split_sst_info_for_level() {
        let mut version = HummockVersion {
            id: HummockVersionId::new(0),
            levels: HashMap::from_iter([(
                1.into(),
                build_initial_compaction_group_levels(
                    1,
                    &CompactionConfig {
                        max_level: 6,
                        ..Default::default()
                    },
                ),
            )]),
            ..Default::default()
        };

        let cg1 = version.levels.get_mut(&1).unwrap();

        cg1.levels[0] = Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping,
            table_infos: vec![
                gen_sst_info(
                    1,
                    vec![3],
                    FullKey::for_test(
                        TableId::new(3),
                        gen_key_from_str(VirtualNode::from_index(1), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                    FullKey::for_test(
                        TableId::new(3),
                        gen_key_from_str(VirtualNode::from_index(200), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                ),
                gen_sst_info(
                    10,
                    vec![3, 4],
                    FullKey::for_test(
                        TableId::new(3),
                        gen_key_from_str(VirtualNode::from_index(201), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                    FullKey::for_test(
                        TableId::new(4),
                        gen_key_from_str(VirtualNode::from_index(10), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                ),
                gen_sst_info(
                    11,
                    vec![4],
                    FullKey::for_test(
                        TableId::new(4),
                        gen_key_from_str(VirtualNode::from_index(11), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                    FullKey::for_test(
                        TableId::new(4),
                        gen_key_from_str(VirtualNode::from_index(200), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                ),
            ],
            total_file_size: 300,
            ..Default::default()
        };

        cg1.l0.sub_levels.push(Level {
            level_idx: 0,
            table_infos: vec![
                gen_sst_info(
                    2,
                    vec![2],
                    FullKey::for_test(
                        TableId::new(0),
                        gen_key_from_str(VirtualNode::from_index(1), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                    FullKey::for_test(
                        TableId::new(2),
                        gen_key_from_str(VirtualNode::from_index(200), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                ),
                gen_sst_info(
                    22,
                    vec![2],
                    FullKey::for_test(
                        TableId::new(0),
                        gen_key_from_str(VirtualNode::from_index(1), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                    FullKey::for_test(
                        TableId::new(2),
                        gen_key_from_str(VirtualNode::from_index(200), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                ),
                gen_sst_info(
                    23,
                    vec![2],
                    FullKey::for_test(
                        TableId::new(0),
                        gen_key_from_str(VirtualNode::from_index(1), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                    FullKey::for_test(
                        TableId::new(2),
                        gen_key_from_str(VirtualNode::from_index(200), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                ),
                gen_sst_info(
                    24,
                    vec![2],
                    FullKey::for_test(
                        TableId::new(2),
                        gen_key_from_str(VirtualNode::from_index(1), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                    FullKey::for_test(
                        TableId::new(2),
                        gen_key_from_str(VirtualNode::from_index(200), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                ),
                gen_sst_info(
                    25,
                    vec![2],
                    FullKey::for_test(
                        TableId::new(0),
                        gen_key_from_str(VirtualNode::from_index(1), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                    FullKey::for_test(
                        TableId::new(0),
                        gen_key_from_str(VirtualNode::from_index(200), "1"),
                        0,
                    )
                    .encode()
                    .into(),
                ),
            ],
            sub_level_id: 101,
            level_type: LevelType::Overlapping,
            total_file_size: 300,
            ..Default::default()
        });

        {
            // split Overlapping level
            let split_key = group_split::build_split_key(1.into(), VirtualNode::ZERO);

            let mut new_sst_id = 100.into();
            let x = group_split::split_sst_info_for_level_v2(
                &mut cg1.l0.sub_levels[0],
                &mut new_sst_id,
                split_key,
            );
            // assert_eq!(3, x.len());
            // assert_eq!(100, x[0].sst_id);
            // assert_eq!(100, x[0].sst_size);
            // assert_eq!(101, x[1].sst_id);
            // assert_eq!(100, x[1].sst_size);
            // assert_eq!(102, x[2].sst_id);
            // assert_eq!(100, x[2].sst_size);

            let mut right_l0 = OverlappingLevel {
                sub_levels: vec![],
                total_file_size: 0,
                uncompressed_file_size: 0,
            };

            right_l0.sub_levels.push(Level {
                level_idx: 0,
                table_infos: x,
                sub_level_id: 101,
                total_file_size: 100,
                level_type: LevelType::Overlapping,
                ..Default::default()
            });

            let right_levels = Levels {
                levels: vec![],
                l0: right_l0,
                ..Default::default()
            };

            merge_levels(cg1, right_levels);
        }

        {
            // test split empty level
            let mut new_sst_id = 100.into();
            let split_key = group_split::build_split_key(1.into(), VirtualNode::ZERO);
            let x = group_split::split_sst_info_for_level_v2(
                &mut cg1.levels[2],
                &mut new_sst_id,
                split_key,
            );

            assert!(x.is_empty());
        }

        {
            // test split to right Nonoverlapping level
            let mut cg1 = cg1.clone();
            let split_key = group_split::build_split_key(1.into(), VirtualNode::ZERO);

            let mut new_sst_id = 100.into();
            let x = group_split::split_sst_info_for_level_v2(
                &mut cg1.levels[0],
                &mut new_sst_id,
                split_key,
            );

            assert_eq!(3, x.len());
            assert_eq!(1, x[0].sst_id);
            assert_eq!(100, x[0].sst_size);
            assert_eq!(10, x[1].sst_id);
            assert_eq!(100, x[1].sst_size);
            assert_eq!(11, x[2].sst_id);
            assert_eq!(100, x[2].sst_size);

            assert_eq!(0, cg1.levels[0].table_infos.len());
        }

        {
            // test split to left Nonoverlapping level
            let mut cg1 = cg1.clone();
            let split_key = group_split::build_split_key(5.into(), VirtualNode::ZERO);

            let mut new_sst_id = 100.into();
            let x = group_split::split_sst_info_for_level_v2(
                &mut cg1.levels[0],
                &mut new_sst_id,
                split_key,
            );

            assert_eq!(0, x.len());
            assert_eq!(3, cg1.levels[0].table_infos.len());
        }

        // {
        //     // test split to both Nonoverlapping level
        //     let mut cg1 = cg1.clone();
        //     let split_key = build_split_key(3, VirtualNode::MAX);

        //     let mut new_sst_id = 100;
        //     let x = group_split::split_sst_info_for_level_v2(
        //         &mut cg1.levels[0],
        //         &mut new_sst_id,
        //         split_key,
        //     );

        //     assert_eq!(2, x.len());
        //     assert_eq!(100, x[0].sst_id);
        //     assert_eq!(100 / 2, x[0].sst_size);
        //     assert_eq!(11, x[1].sst_id);
        //     assert_eq!(100, x[1].sst_size);
        //     assert_eq!(vec![3, 4], x[0].table_ids);

        //     assert_eq!(2, cg1.levels[0].table_infos.len());
        //     assert_eq!(101, cg1.levels[0].table_infos[1].sst_id);
        //     assert_eq!(100 / 2, cg1.levels[0].table_infos[1].sst_size);
        //     assert_eq!(vec![3], cg1.levels[0].table_infos[1].table_ids);
        // }

        {
            // test split to both Nonoverlapping level
            let mut cg1 = cg1.clone();
            let split_key = group_split::build_split_key(4.into(), VirtualNode::ZERO);

            let mut new_sst_id = 100.into();
            let x = group_split::split_sst_info_for_level_v2(
                &mut cg1.levels[0],
                &mut new_sst_id,
                split_key,
            );

            assert_eq!(2, x.len());
            assert_eq!(100, x[0].sst_id);
            assert_eq!(100 / 2, x[0].sst_size);
            assert_eq!(11, x[1].sst_id);
            assert_eq!(100, x[1].sst_size);
            assert_eq!(vec![TableId::new(4)], x[1].table_ids);

            assert_eq!(2, cg1.levels[0].table_infos.len());
            assert_eq!(101, cg1.levels[0].table_infos[1].sst_id);
            assert_eq!(100 / 2, cg1.levels[0].table_infos[1].sst_size);
            assert_eq!(
                vec![TableId::new(3)],
                cg1.levels[0].table_infos[1].table_ids
            );
        }
    }

    fn make_sst(sst_id: u64, table_ids: Vec<u32>, sst_size: u64) -> SstableInfo {
        SstableInfoInner {
            sst_id: sst_id.into(),
            object_id: sst_id.into(),
            table_ids: table_ids.into_iter().map(TableId::new).collect(),
            file_size: sst_size,
            sst_size,
            uncompressed_file_size: sst_size * 2,
            ..Default::default()
        }
        .into()
    }

    #[test]
    fn test_level_normalize() {
        // Mixed: some SSTs empty, some not.
        let mut level = Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping,
            table_infos: vec![
                make_sst(1, vec![1, 2], 100),
                make_sst(2, vec![], 200), // empty → removed
                make_sst(3, vec![3], 300),
                make_sst(4, vec![], 400), // empty → removed
            ],
            total_file_size: 9999, // intentionally wrong
            uncompressed_file_size: 9999,
            ..Default::default()
        };

        level.normalize();

        assert_eq!(level.table_infos.len(), 2);
        assert_eq!(1, level.table_infos[0].sst_id);
        assert_eq!(3, level.table_infos[1].sst_id);
        assert_eq!(level.total_file_size, 400);
        assert_eq!(level.uncompressed_file_size, 800);

        // No empty SSTs: just recompute sizes.
        level.total_file_size = 0;
        level.uncompressed_file_size = 0;
        level.normalize();
        assert_eq!(level.table_infos.len(), 2);
        assert_eq!(level.total_file_size, 400);
        assert_eq!(level.uncompressed_file_size, 800);

        // All empty → level becomes empty.
        level.table_infos = vec![make_sst(10, vec![], 100), make_sst(11, vec![], 200)];
        level.normalize();
        assert!(level.table_infos.is_empty());
        assert_eq!(level.total_file_size, 0);
        assert_eq!(level.uncompressed_file_size, 0);
    }

    #[test]
    fn test_level_delete_ssts() {
        let mut level = Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping,
            table_infos: vec![
                make_sst(1, vec![1], 100),
                make_sst(2, vec![2], 200),
                make_sst(3, vec![3], 300),
            ],
            total_file_size: 600,
            uncompressed_file_size: 1200,
            ..Default::default()
        };

        let delete_ids: HashSet<crate::HummockSstableId> = HashSet::from([2.into()]);
        let changed = level.delete_ssts(&delete_ids);

        assert!(changed);
        assert_eq!(level.table_infos.len(), 2);
        assert_eq!(1, level.table_infos[0].sst_id);
        assert_eq!(3, level.table_infos[1].sst_id);
        assert_eq!(level.total_file_size, 400);
        assert_eq!(level.uncompressed_file_size, 800);

        // Delete non-existent id → no change, returns false.
        let delete_ids: HashSet<crate::HummockSstableId> = HashSet::from([999.into()]);
        let changed = level.delete_ssts(&delete_ids);
        assert!(!changed);
        assert_eq!(level.table_infos.len(), 2);
    }

    #[test]
    fn test_level_prune_table_ids_from_ssts() {
        let mut level = Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping,
            table_infos: vec![
                make_sst(1, vec![1, 2], 100),
                make_sst(2, vec![2, 3], 200),
                make_sst(3, vec![2], 300),
            ],
            total_file_size: 600,
            uncompressed_file_size: 1200,
            ..Default::default()
        };

        let pruned_table_ids = HashSet::from([TableId::new(2)]);
        level.prune_table_ids_from_ssts(&pruned_table_ids);

        // SST 3 should be removed (was only table_id=2)
        assert_eq!(level.table_infos.len(), 2);
        assert_eq!(level.table_infos[0].table_ids, vec![TableId::new(1)]);
        assert_eq!(level.table_infos[1].table_ids, vec![TableId::new(3)]);
        assert_eq!(level.total_file_size, 100 + 200);
        assert_eq!(level.uncompressed_file_size, 200 + 400);

        // Prune remaining tables, so all SSTs are removed.
        let pruned_table_ids = HashSet::from([TableId::new(1), TableId::new(3)]);
        level.prune_table_ids_from_ssts(&pruned_table_ids);
        assert!(level.table_infos.is_empty());
        assert_eq!(level.total_file_size, 0);
        assert_eq!(level.uncompressed_file_size, 0);
    }

    #[test]
    fn test_overlapping_level_normalize() {
        let mut l0 = OverlappingLevel {
            sub_levels: vec![
                Level {
                    level_idx: 0,
                    table_infos: vec![make_sst(1, vec![1], 100)],
                    total_file_size: 100,
                    uncompressed_file_size: 200,
                    sub_level_id: 1,
                    ..Default::default()
                },
                Level {
                    level_idx: 0,
                    table_infos: vec![], // empty → should be removed
                    total_file_size: 0,
                    uncompressed_file_size: 0,
                    sub_level_id: 2,
                    ..Default::default()
                },
                Level {
                    level_idx: 0,
                    table_infos: vec![make_sst(3, vec![3], 300)],
                    total_file_size: 300,
                    uncompressed_file_size: 600,
                    sub_level_id: 3,
                    ..Default::default()
                },
            ],
            total_file_size: 9999, // intentionally wrong
            uncompressed_file_size: 9999,
        };

        l0.normalize();

        assert_eq!(l0.sub_levels.len(), 2);
        assert_eq!(l0.sub_levels[0].sub_level_id, 1);
        assert_eq!(l0.sub_levels[1].sub_level_id, 3);
        assert_eq!(l0.total_file_size, 100 + 300);
        assert_eq!(l0.uncompressed_file_size, 200 + 600);

        // All sub-levels empty → normalize clears everything.
        l0.sub_levels = vec![Level {
            level_idx: 0,
            table_infos: vec![],
            ..Default::default()
        }];
        l0.normalize();
        assert!(l0.sub_levels.is_empty());
        assert_eq!(l0.total_file_size, 0);
        assert_eq!(l0.uncompressed_file_size, 0);
    }

    #[test]
    fn test_levels_prune_table_ids_from_ssts() {
        #[expect(deprecated)]
        let mut levels = Levels {
            l0: OverlappingLevel {
                sub_levels: vec![
                    Level {
                        level_idx: 0,
                        table_infos: vec![
                            make_sst(1, vec![10], 100), // table 10
                            make_sst(2, vec![20], 200), // table 20
                        ],
                        total_file_size: 300,
                        uncompressed_file_size: 600,
                        sub_level_id: 1,
                        ..Default::default()
                    },
                    Level {
                        level_idx: 0,
                        table_infos: vec![
                            make_sst(3, vec![10], 150), // table 10
                        ],
                        total_file_size: 150,
                        uncompressed_file_size: 300,
                        sub_level_id: 2,
                        ..Default::default()
                    },
                ],
                total_file_size: 450,
                uncompressed_file_size: 900,
            },
            levels: vec![Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![
                    make_sst(4, vec![10, 20], 400), // shared SST
                    make_sst(5, vec![10], 500),     // table 10 only
                ],
                total_file_size: 900,
                uncompressed_file_size: 1800,
                ..Default::default()
            }],
            group_id: 1.into(),
            parent_group_id: 0.into(),
            member_table_ids: vec![],
            compaction_group_version_id: 0,
        };

        // Prune table 10 from SST metadata.
        levels.prune_table_ids_from_ssts(&HashSet::from([TableId::new(10)]));

        assert_eq!(levels.l0.sub_levels.len(), 1);
        assert_eq!(levels.l0.sub_levels[0].sub_level_id, 1);
        assert_eq!(levels.l0.sub_levels[0].table_infos.len(), 1);
        assert_eq!(2, levels.l0.sub_levels[0].table_infos[0].sst_id);
        assert_eq!(levels.l0.sub_levels[0].total_file_size, 200);
        assert_eq!(levels.l0.sub_levels[0].uncompressed_file_size, 400);

        assert_eq!(levels.l0.total_file_size, 200);
        assert_eq!(levels.l0.uncompressed_file_size, 400);

        assert_eq!(levels.levels[0].table_infos.len(), 1);
        assert_eq!(4, levels.levels[0].table_infos[0].sst_id);
        assert_eq!(
            levels.levels[0].table_infos[0].table_ids,
            vec![TableId::new(20)]
        );
        assert_eq!(levels.levels[0].total_file_size, 400);
        assert_eq!(levels.levels[0].uncompressed_file_size, 800);

        assert_eq!(levels.compaction_group_version_id, 1);
    }

    #[test]
    fn test_apply_version_delta_prune_table_ids_from_ssts() {
        let mut version = HummockVersion {
            id: HummockVersionId::new(0),
            levels: HashMap::from_iter([(1.into(), {
                #[expect(deprecated)]
                let levels = Levels {
                    l0: OverlappingLevel {
                        sub_levels: vec![
                            Level {
                                level_idx: 0,
                                level_type: LevelType::Overlapping,
                                table_infos: vec![
                                    make_sst(1, vec![100], 50), // only table 100
                                    make_sst(2, vec![200], 60), // only table 200
                                ],
                                total_file_size: 110,
                                uncompressed_file_size: 220,
                                sub_level_id: 1,
                                ..Default::default()
                            },
                            Level {
                                level_idx: 0,
                                level_type: LevelType::Overlapping,
                                table_infos: vec![
                                    make_sst(3, vec![100], 70), // only table 100
                                ],
                                total_file_size: 70,
                                uncompressed_file_size: 140,
                                sub_level_id: 2,
                                ..Default::default()
                            },
                        ],
                        total_file_size: 180,
                        uncompressed_file_size: 360,
                    },
                    levels: vec![Level {
                        level_idx: 1,
                        level_type: LevelType::Nonoverlapping,
                        table_infos: vec![
                            make_sst(4, vec![100, 200], 80), // shared
                            make_sst(5, vec![100], 90),      // only table 100
                        ],
                        total_file_size: 170,
                        uncompressed_file_size: 340,
                        ..Default::default()
                    }],
                    group_id: 1.into(),
                    parent_group_id: 0.into(),
                    member_table_ids: vec![],
                    compaction_group_version_id: 0,
                };
                levels
            })]),
            ..Default::default()
        };

        let version_delta = HummockVersionDelta {
            id: HummockVersionId::new(1),
            group_deltas: HashMap::from_iter([(
                1.into(),
                GroupDeltas {
                    group_deltas: vec![GroupDelta::PruneTableIdsFromSsts(HashSet::from([
                        TableId::new(100),
                    ]))],
                },
            )]),
            ..Default::default()
        };

        version.apply_version_delta(&version_delta);

        let cg = version.get_compaction_group_levels(1.into());

        assert_eq!(
            cg.l0.sub_levels.len(),
            1,
            "empty sub-level should be removed"
        );
        assert_eq!(cg.l0.sub_levels[0].sub_level_id, 1);
        assert_eq!(cg.l0.sub_levels[0].table_infos.len(), 1);
        assert_eq!(2, cg.l0.sub_levels[0].table_infos[0].sst_id);
        assert_eq!(
            cg.l0.sub_levels[0].table_infos[0].table_ids,
            vec![TableId::new(200)]
        );

        assert_eq!(cg.l0.total_file_size, 60);
        assert_eq!(cg.l0.uncompressed_file_size, 120);

        assert_eq!(cg.levels[0].table_infos.len(), 1);
        assert_eq!(4, cg.levels[0].table_infos[0].sst_id);
        assert_eq!(
            cg.levels[0].table_infos[0].table_ids,
            vec![TableId::new(200)]
        );
        assert_eq!(cg.levels[0].total_file_size, 80);
        assert_eq!(cg.levels[0].uncompressed_file_size, 160);

        assert_eq!(cg.compaction_group_version_id, 1);
    }

    #[test]
    fn test_apply_version_delta_removes_dropped_table_watermark() {
        let table_id = TableId::new(100);
        let mut version = HummockVersion {
            id: HummockVersionId::new(0),
            table_watermarks: HashMap::from([(
                table_id,
                Arc::new(TableWatermarks::single_epoch(
                    test_epoch(1),
                    vec![VnodeWatermark::new(
                        Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                        Bytes::from_static(b"watermark"),
                    )],
                    WatermarkDirection::Ascending,
                    WatermarkSerdeType::PkPrefix,
                )),
            )]),
            ..Default::default()
        };

        let version_delta = HummockVersionDelta {
            id: HummockVersionId::new(1),
            prev_id: HummockVersionId::new(0),
            removed_table_ids: HashSet::from([table_id]),
            ..Default::default()
        };

        version.apply_version_delta(&version_delta);

        assert!(!version.table_watermarks.contains_key(&table_id));
    }

    #[test]
    fn test_prune_stale_table_ids_from_ssts() {
        let live_table_id = TableId::new(100);
        let stale_table_id = TableId::new(200);
        let mut version = HummockVersion {
            id: HummockVersionId::new(0),
            levels: HashMap::from_iter([(1.into(), {
                #[expect(deprecated)]
                let levels = Levels {
                    l0: OverlappingLevel {
                        sub_levels: vec![Level {
                            level_idx: 0,
                            level_type: LevelType::Overlapping,
                            table_infos: vec![make_sst(
                                1,
                                vec![live_table_id.as_raw_id(), stale_table_id.as_raw_id()],
                                50,
                            )],
                            total_file_size: 50,
                            uncompressed_file_size: 100,
                            sub_level_id: 1,
                            ..Default::default()
                        }],
                        total_file_size: 50,
                        uncompressed_file_size: 100,
                    },
                    levels: vec![Level {
                        level_idx: 1,
                        level_type: LevelType::Nonoverlapping,
                        table_infos: vec![make_sst(2, vec![stale_table_id.as_raw_id()], 60)],
                        total_file_size: 60,
                        uncompressed_file_size: 120,
                        ..Default::default()
                    }],
                    group_id: 1.into(),
                    parent_group_id: 0.into(),
                    member_table_ids: vec![],
                    compaction_group_version_id: 0,
                };
                levels
            })]),
            state_table_info: HummockVersionStateTableInfo::from_protobuf_owned(
                HashMap::from_iter([(
                    live_table_id,
                    StateTableInfo {
                        committed_epoch: 1,
                        compaction_group_id: 1.into(),
                    },
                )]),
            ),
            ..Default::default()
        };

        assert_eq!(version.prune_stale_table_ids_from_ssts(), 1);

        let cg = version.get_compaction_group_levels(1.into());
        assert_eq!(cg.l0.sub_levels.len(), 1);
        assert_eq!(cg.l0.sub_levels[0].table_infos.len(), 1);
        assert_eq!(cg.l0.sub_levels[0].table_infos[0].sst_id, 1);
        assert_eq!(
            cg.l0.sub_levels[0].table_infos[0].table_ids,
            vec![live_table_id]
        );
        assert!(cg.levels[0].table_infos.is_empty());
        assert_eq!(cg.compaction_group_version_id, 1);
    }

    #[test]
    fn test_prune_stale_table_ids_from_ssts_skips_legacy_member_table_ids() {
        let mut version = HummockVersion {
            id: HummockVersionId::new(0),
            levels: HashMap::from_iter([(1.into(), {
                #[expect(deprecated)]
                let levels = Levels {
                    l0: OverlappingLevel {
                        sub_levels: vec![Level {
                            level_idx: 0,
                            level_type: LevelType::Overlapping,
                            table_infos: vec![make_sst(1, vec![100, 200], 50)],
                            total_file_size: 50,
                            uncompressed_file_size: 100,
                            sub_level_id: 1,
                            ..Default::default()
                        }],
                        total_file_size: 50,
                        uncompressed_file_size: 100,
                    },
                    group_id: 1.into(),
                    parent_group_id: 0.into(),
                    member_table_ids: vec![100, 200],
                    compaction_group_version_id: 0,
                    ..Default::default()
                };
                levels
            })]),
            ..Default::default()
        };

        assert_eq!(version.prune_stale_table_ids_from_ssts(), 0);

        let cg = version.get_compaction_group_levels(1.into());
        assert_eq!(
            cg.l0.sub_levels[0].table_infos[0].table_ids,
            vec![TableId::new(100), TableId::new(200)]
        );
        assert_eq!(cg.compaction_group_version_id, 0);
    }

    #[test]
    fn test_apply_version_delta_compact_l0() {
        let mut version = HummockVersion {
            id: HummockVersionId::new(0),
            levels: HashMap::from_iter([(1.into(), {
                #[expect(deprecated)]
                let levels = Levels {
                    l0: OverlappingLevel {
                        sub_levels: vec![
                            Level {
                                level_idx: 0,
                                level_type: LevelType::Nonoverlapping,
                                table_infos: vec![
                                    make_sst(1, vec![1], 100),
                                    make_sst(2, vec![2], 200),
                                ],
                                total_file_size: 300,
                                uncompressed_file_size: 600,
                                sub_level_id: 1,
                                ..Default::default()
                            },
                            Level {
                                level_idx: 0,
                                level_type: LevelType::Nonoverlapping,
                                table_infos: vec![make_sst(3, vec![3], 300)],
                                total_file_size: 300,
                                uncompressed_file_size: 600,
                                sub_level_id: 2,
                                ..Default::default()
                            },
                        ],
                        total_file_size: 600,
                        uncompressed_file_size: 1200,
                    },
                    levels: vec![Level {
                        level_idx: 1,
                        level_type: LevelType::Nonoverlapping,
                        table_infos: vec![],
                        total_file_size: 0,
                        uncompressed_file_size: 0,
                        ..Default::default()
                    }],
                    group_id: 1.into(),
                    parent_group_id: 0.into(),
                    member_table_ids: vec![],
                    compaction_group_version_id: 0,
                };
                levels
            })]),
            ..Default::default()
        };

        let version_delta = HummockVersionDelta {
            id: HummockVersionId::new(1),
            group_deltas: HashMap::from_iter([(
                1.into(),
                GroupDeltas {
                    group_deltas: vec![
                        GroupDelta::IntraLevel(IntraLevelDelta::new(
                            0, // L0
                            0,
                            HashSet::from([1.into(), 2.into(), 3.into()]),
                            vec![],
                            0,
                            0,
                        )),
                        GroupDelta::IntraLevel(IntraLevelDelta::new(
                            1, // L1
                            0,
                            HashSet::new(),
                            vec![make_sst(10, vec![1, 2, 3], 500)],
                            0,
                            0,
                        )),
                    ],
                },
            )]),
            ..Default::default()
        };

        version.apply_version_delta(&version_delta);

        let cg = version.get_compaction_group_levels(1.into());

        assert!(cg.l0.sub_levels.is_empty());
        assert_eq!(cg.l0.total_file_size, 0);
        assert_eq!(cg.l0.uncompressed_file_size, 0);

        assert_eq!(cg.levels[0].table_infos.len(), 1);
        assert_eq!(10, cg.levels[0].table_infos[0].sst_id);
        assert_eq!(cg.levels[0].total_file_size, 500);
    }
}
