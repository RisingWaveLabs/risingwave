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

use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::mem::{replace, size_of};
use std::ops::Deref;
use std::sync::{Arc, LazyLock};

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_pb::hummock::group_delta::{DeltaType, PbDeltaType};
use risingwave_pb::hummock::hummock_version_delta::PbGroupDeltas;
use risingwave_pb::hummock::*;
use tracing::warn;

use crate::compaction_group::StaticCompactionGroupId;
use crate::compaction_group::hummock_version_ext::build_initial_compaction_group_levels;
use crate::level::LevelsCommon;
use crate::sstable_info::SstableInfo;
use crate::table_watermark::TableWatermarks;
use crate::vector_index::{VectorIndex, VectorIndexDelta};
use crate::{
    CompactionGroupId, FIRST_VERSION_ID, HummockEpoch, HummockObjectId, HummockSstableId,
    HummockSstableObjectId, HummockVersionId,
};

pub const MAX_HUMMOCK_VERSION_ID: HummockVersionId = HummockVersionId::new(i64::MAX as _);

#[derive(Debug, Clone, PartialEq)]
pub struct HummockVersionStateTableInfo {
    state_table_info: HashMap<TableId, PbStateTableInfo>,

    // in memory index
    compaction_group_member_tables: HashMap<CompactionGroupId, BTreeSet<TableId>>,
}

impl HummockVersionStateTableInfo {
    pub fn empty() -> Self {
        Self {
            state_table_info: HashMap::new(),
            compaction_group_member_tables: HashMap::new(),
        }
    }

    fn build_compaction_group_member_tables(
        state_table_info: &HashMap<TableId, PbStateTableInfo>,
    ) -> HashMap<CompactionGroupId, BTreeSet<TableId>> {
        let mut ret: HashMap<_, BTreeSet<_>> = HashMap::new();
        for (table_id, info) in state_table_info {
            assert!(
                ret.entry(info.compaction_group_id)
                    .or_default()
                    .insert(*table_id)
            );
        }
        ret
    }

    pub fn build_table_compaction_group_id(&self) -> HashMap<TableId, CompactionGroupId> {
        self.state_table_info
            .iter()
            .map(|(table_id, info)| (*table_id, info.compaction_group_id))
            .collect()
    }

    pub fn from_protobuf(state_table_info: &HashMap<TableId, PbStateTableInfo>) -> Self {
        let state_table_info = state_table_info
            .iter()
            .map(|(table_id, info)| (*table_id, *info))
            .collect();
        let compaction_group_member_tables =
            Self::build_compaction_group_member_tables(&state_table_info);
        Self {
            state_table_info,
            compaction_group_member_tables,
        }
    }

    pub fn from_protobuf_owned(state_table_info: HashMap<TableId, PbStateTableInfo>) -> Self {
        let compaction_group_member_tables =
            Self::build_compaction_group_member_tables(&state_table_info);
        Self {
            state_table_info,
            compaction_group_member_tables,
        }
    }

    pub fn apply_delta(
        &mut self,
        delta: &HashMap<TableId, StateTableInfoDelta>,
        removed_table_id: &HashSet<TableId>,
    ) -> (HashMap<TableId, Option<StateTableInfo>>, bool) {
        let mut changed_table = HashMap::new();
        let mut has_bumped_committed_epoch = false;
        fn remove_table_from_compaction_group(
            compaction_group_member_tables: &mut HashMap<CompactionGroupId, BTreeSet<TableId>>,
            compaction_group_id: CompactionGroupId,
            table_id: TableId,
        ) {
            let member_tables = compaction_group_member_tables
                .get_mut(&compaction_group_id)
                .expect("should exist");
            assert!(member_tables.remove(&table_id));
            if member_tables.is_empty() {
                assert!(
                    compaction_group_member_tables
                        .remove(&compaction_group_id)
                        .is_some()
                );
            }
        }
        for table_id in removed_table_id {
            if let Some(prev_info) = self.state_table_info.remove(table_id) {
                remove_table_from_compaction_group(
                    &mut self.compaction_group_member_tables,
                    prev_info.compaction_group_id,
                    *table_id,
                );
                assert!(changed_table.insert(*table_id, Some(prev_info)).is_none());
            } else {
                warn!(
                    %table_id,
                    "table to remove does not exist"
                );
            }
        }
        for (table_id, delta) in delta {
            if removed_table_id.contains(table_id) {
                continue;
            }
            let new_info = StateTableInfo {
                committed_epoch: delta.committed_epoch,
                compaction_group_id: delta.compaction_group_id,
            };
            match self.state_table_info.entry(*table_id) {
                Entry::Occupied(mut entry) => {
                    let prev_info = entry.get_mut();
                    assert!(
                        new_info.committed_epoch >= prev_info.committed_epoch,
                        "state table info regress. table id: {}, prev_info: {:?}, new_info: {:?}",
                        table_id,
                        prev_info,
                        new_info
                    );
                    if new_info.committed_epoch > prev_info.committed_epoch {
                        has_bumped_committed_epoch = true;
                    }
                    if prev_info.compaction_group_id != new_info.compaction_group_id {
                        // table moved to another compaction group
                        remove_table_from_compaction_group(
                            &mut self.compaction_group_member_tables,
                            prev_info.compaction_group_id,
                            *table_id,
                        );
                        assert!(
                            self.compaction_group_member_tables
                                .entry(new_info.compaction_group_id)
                                .or_default()
                                .insert(*table_id)
                        );
                    }
                    let prev_info = replace(prev_info, new_info);
                    changed_table.insert(*table_id, Some(prev_info));
                }
                Entry::Vacant(entry) => {
                    assert!(
                        self.compaction_group_member_tables
                            .entry(new_info.compaction_group_id)
                            .or_default()
                            .insert(*table_id)
                    );
                    has_bumped_committed_epoch = true;
                    entry.insert(new_info);
                    changed_table.insert(*table_id, None);
                }
            }
        }
        debug_assert_eq!(
            self.compaction_group_member_tables,
            Self::build_compaction_group_member_tables(&self.state_table_info)
        );
        (changed_table, has_bumped_committed_epoch)
    }

    pub fn info(&self) -> &HashMap<TableId, StateTableInfo> {
        &self.state_table_info
    }

    pub fn compaction_group_member_table_ids(
        &self,
        compaction_group_id: CompactionGroupId,
    ) -> &BTreeSet<TableId> {
        static EMPTY_SET: LazyLock<BTreeSet<TableId>> = LazyLock::new(BTreeSet::new);
        self.compaction_group_member_tables
            .get(&compaction_group_id)
            .unwrap_or_else(|| EMPTY_SET.deref())
    }

    pub fn compaction_group_member_tables(&self) -> &HashMap<CompactionGroupId, BTreeSet<TableId>> {
        &self.compaction_group_member_tables
    }

    pub fn max_table_committed_epoch(&self) -> Option<HummockEpoch> {
        self.state_table_info
            .values()
            .map(|info| info.committed_epoch)
            .max()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HummockVersionCommon<T> {
    pub id: HummockVersionId,
    pub levels: HashMap<CompactionGroupId, LevelsCommon<T>>,
    #[deprecated]
    pub(crate) max_committed_epoch: u64,
    pub table_watermarks: HashMap<TableId, Arc<TableWatermarks>>,
    pub state_table_info: HummockVersionStateTableInfo,
    pub vector_indexes: HashMap<TableId, VectorIndex>,
}

pub type HummockVersion = HummockVersionCommon<SstableInfo>;

impl Default for HummockVersion {
    fn default() -> Self {
        HummockVersion::from(&PbHummockVersion::default())
    }
}

impl<T> HummockVersionCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
    PbSstableInfo: for<'a> From<&'a T>,
{
    /// Convert the `PbHummockVersion` received from rpc to `HummockVersion`. No need to
    /// maintain backward compatibility.
    pub fn from_rpc_protobuf(pb_version: &PbHummockVersion) -> Self {
        pb_version.into()
    }

    /// Convert the `PbHummockVersion` deserialized from persisted state to `HummockVersion`.
    /// We should maintain backward compatibility.
    pub fn from_persisted_protobuf(pb_version: &PbHummockVersion) -> Self {
        pb_version.into()
    }

    pub fn to_protobuf(&self) -> PbHummockVersion {
        self.into()
    }
}

impl<T> HummockVersionCommon<T>
where
    T: From<PbSstableInfo>,
    PbSstableInfo: for<'a> From<&'a T>,
{
    /// Convert an owned `PbHummockVersion` deserialized from persisted state to `HummockVersion`,
    /// moving data instead of cloning for better performance on large checkpoints.
    pub fn from_persisted_protobuf_owned(pb_version: PbHummockVersion) -> Self {
        pb_version.into()
    }
}

impl HummockVersion {
    pub fn estimated_encode_len(&self) -> usize {
        self.levels.len() * size_of::<CompactionGroupId>()
            + self
                .levels
                .values()
                .map(|level| level.estimated_encode_len())
                .sum::<usize>()
            + self.table_watermarks.len() * size_of::<u32>()
            + self
                .table_watermarks
                .values()
                .map(|table_watermark| table_watermark.estimated_encode_len())
                .sum::<usize>()
    }
}

impl<T> From<&PbHummockVersion> for HummockVersionCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    fn from(pb_version: &PbHummockVersion) -> Self {
        #[expect(deprecated)]
        Self {
            id: pb_version.id,
            levels: pb_version
                .levels
                .iter()
                .map(|(group_id, levels)| (*group_id, LevelsCommon::from(levels)))
                .collect(),
            max_committed_epoch: pb_version.max_committed_epoch,
            table_watermarks: pb_version
                .table_watermarks
                .iter()
                .map(|(table_id, table_watermark)| {
                    (*table_id, Arc::new(TableWatermarks::from(table_watermark)))
                })
                .collect(),
            state_table_info: HummockVersionStateTableInfo::from_protobuf(
                &pb_version.state_table_info,
            ),
            vector_indexes: pb_version
                .vector_indexes
                .iter()
                .map(|(table_id, index)| (*table_id, index.clone().into()))
                .collect(),
        }
    }
}

impl<T> From<PbHummockVersion> for HummockVersionCommon<T>
where
    T: From<PbSstableInfo>,
{
    fn from(pb_version: PbHummockVersion) -> Self {
        #[expect(deprecated)]
        Self {
            id: pb_version.id,
            levels: pb_version
                .levels
                .into_iter()
                .map(|(group_id, levels)| (group_id, LevelsCommon::from(levels)))
                .collect(),
            max_committed_epoch: pb_version.max_committed_epoch,
            table_watermarks: pb_version
                .table_watermarks
                .into_iter()
                .map(|(table_id, table_watermark)| {
                    (table_id, Arc::new(TableWatermarks::from(table_watermark)))
                })
                .collect(),
            state_table_info: HummockVersionStateTableInfo::from_protobuf_owned(
                pb_version.state_table_info,
            ),
            vector_indexes: pb_version
                .vector_indexes
                .into_iter()
                .map(|(table_id, index)| (table_id, index.into()))
                .collect(),
        }
    }
}

impl<T> From<&HummockVersionCommon<T>> for PbHummockVersion
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(version: &HummockVersionCommon<T>) -> Self {
        #[expect(deprecated)]
        Self {
            id: version.id,
            levels: version
                .levels
                .iter()
                .map(|(group_id, levels)| (*group_id, levels.into()))
                .collect(),
            max_committed_epoch: version.max_committed_epoch,
            table_watermarks: version
                .table_watermarks
                .iter()
                .map(|(table_id, watermark)| (*table_id, watermark.as_ref().into()))
                .collect(),
            table_change_logs: Default::default(),
            state_table_info: version.state_table_info.state_table_info.clone(),
            vector_indexes: version
                .vector_indexes
                .iter()
                .map(|(table_id, index)| (*table_id, index.clone().into()))
                .collect(),
        }
    }
}

impl<T> From<HummockVersionCommon<T>> for PbHummockVersion
where
    PbSstableInfo: From<T>,
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(version: HummockVersionCommon<T>) -> Self {
        #[expect(deprecated)]
        Self {
            id: version.id,
            levels: version
                .levels
                .into_iter()
                .map(|(group_id, levels)| (group_id, levels.into()))
                .collect(),
            max_committed_epoch: version.max_committed_epoch,
            table_watermarks: version
                .table_watermarks
                .into_iter()
                .map(|(table_id, watermark)| (table_id, watermark.as_ref().into()))
                .collect(),
            table_change_logs: Default::default(),
            state_table_info: version.state_table_info.state_table_info.clone(),
            vector_indexes: version
                .vector_indexes
                .into_iter()
                .map(|(table_id, index)| (table_id, index.into()))
                .collect(),
        }
    }
}

impl HummockVersion {
    pub fn next_version_id(&self) -> HummockVersionId {
        self.id + 1
    }

    pub fn need_fill_backward_compatible_state_table_info_delta(&self) -> bool {
        // for backward-compatibility of previous hummock version delta
        self.state_table_info.state_table_info.is_empty()
            && self.levels.values().any(|group| {
                // state_table_info is not previously filled, but there previously exists some tables
                #[expect(deprecated)]
                !group.member_table_ids.is_empty()
            })
    }

    pub fn may_fill_backward_compatible_state_table_info_delta(
        &self,
        delta: &mut HummockVersionDelta,
    ) {
        #[expect(deprecated)]
        // for backward-compatibility of previous hummock version delta
        for (cg_id, group) in &self.levels {
            for table_id in &group.member_table_ids {
                assert!(
                    delta
                        .state_table_info_delta
                        .insert(
                            (*table_id).into(),
                            StateTableInfoDelta {
                                committed_epoch: self.max_committed_epoch,
                                compaction_group_id: *cg_id,
                            }
                        )
                        .is_none(),
                    "duplicate table id {} in cg {}",
                    table_id,
                    cg_id
                );
            }
        }
    }

    pub fn create_init_version(default_compaction_config: Arc<CompactionConfig>) -> HummockVersion {
        #[expect(deprecated)]
        let mut init_version = HummockVersion {
            id: FIRST_VERSION_ID,
            levels: Default::default(),
            max_committed_epoch: INVALID_EPOCH,
            table_watermarks: HashMap::new(),
            state_table_info: HummockVersionStateTableInfo::empty(),
            vector_indexes: Default::default(),
        };
        for group_id in [
            StaticCompactionGroupId::StateDefault as CompactionGroupId,
            StaticCompactionGroupId::MaterializedView as CompactionGroupId,
        ] {
            init_version.levels.insert(
                group_id,
                build_initial_compaction_group_levels(group_id, default_compaction_config.as_ref()),
            );
        }
        init_version
    }

    pub fn version_delta_after(&self) -> HummockVersionDelta {
        #[expect(deprecated)]
        HummockVersionDelta {
            id: self.next_version_id(),
            prev_id: self.id,
            trivial_move: false,
            max_committed_epoch: self.max_committed_epoch,
            group_deltas: Default::default(),
            new_table_watermarks: HashMap::new(),
            removed_table_ids: HashSet::new(),
            state_table_info_delta: Default::default(),
            vector_index_delta: Default::default(),
        }
    }
}

impl<T> HummockVersionCommon<T> {
    pub fn table_committed_epoch(&self, table_id: TableId) -> Option<u64> {
        self.state_table_info
            .info()
            .get(&table_id)
            .map(|info| info.committed_epoch)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct HummockVersionDeltaCommon<T> {
    pub id: HummockVersionId,
    pub prev_id: HummockVersionId,
    pub group_deltas: HashMap<CompactionGroupId, GroupDeltasCommon<T>>,
    #[deprecated]
    pub(crate) max_committed_epoch: u64,
    pub trivial_move: bool,
    pub new_table_watermarks: HashMap<TableId, TableWatermarks>,
    pub removed_table_ids: HashSet<TableId>,
    pub state_table_info_delta: HashMap<TableId, StateTableInfoDelta>,
    pub vector_index_delta: HashMap<TableId, VectorIndexDelta>,
}

pub type HummockVersionDelta = HummockVersionDeltaCommon<SstableInfo>;

impl Default for HummockVersionDelta {
    fn default() -> Self {
        HummockVersionDelta::from(&PbHummockVersionDelta::default())
    }
}

impl<T> HummockVersionDeltaCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
    PbSstableInfo: for<'a> From<&'a T>,
{
    /// Convert the `PbHummockVersionDelta` deserialized from persisted state to `HummockVersionDelta`.
    /// We should maintain backward compatibility.
    pub fn from_persisted_protobuf(delta: &PbHummockVersionDelta) -> Self {
        warn_if_legacy_change_log_delta_is_present(delta);
        delta.into()
    }

    /// Convert the `PbHummockVersionDelta` received from rpc to `HummockVersionDelta`. No need to
    /// maintain backward compatibility.
    pub fn from_rpc_protobuf(delta: &PbHummockVersionDelta) -> Self {
        delta.into()
    }

    pub fn to_protobuf(&self) -> PbHummockVersionDelta {
        self.into()
    }
}

impl<T> HummockVersionDeltaCommon<T>
where
    T: From<PbSstableInfo>,
    PbSstableInfo: for<'a> From<&'a T>,
{
    /// Convert an owned `PbHummockVersionDelta` deserialized from persisted state to
    /// `HummockVersionDelta`, moving data instead of cloning.
    pub fn from_persisted_protobuf_owned(delta: PbHummockVersionDelta) -> Self {
        warn_if_legacy_change_log_delta_is_present(&delta);
        delta.into()
    }
}

fn warn_if_legacy_change_log_delta_is_present(delta: &PbHummockVersionDelta) {
    if !delta.change_log_delta.is_empty() {
        warn!(
            version_delta_id = ?delta.id,
            table_count = delta.change_log_delta.len(),
            "deprecated table change log delta found in persisted hummock version delta; ignoring it"
        );
    }
}

pub trait SstableIdReader {
    fn sst_id(&self) -> HummockSstableId;
}

pub trait ObjectIdReader {
    fn object_id(&self) -> HummockSstableObjectId;
}

impl<T> HummockVersionDeltaCommon<T>
where
    T: SstableIdReader + ObjectIdReader,
{
    /// Get the newly added object ids from the version delta.
    ///
    /// Note: the result can be false positive because we only collect the set of sst object ids in the `inserted_table_infos`,
    /// but it is possible that the object is moved or split from other compaction groups or levels.
    pub fn newly_added_object_ids(&self) -> HashSet<HummockObjectId> {
        // DO NOT REMOVE THIS LINE
        // This is to ensure that when adding new variant to `HummockObjectId`,
        // the compiler will warn us if we forget to handle it here.
        match HummockObjectId::Sstable(0.into()) {
            HummockObjectId::Sstable(_) => {}
            HummockObjectId::VectorFile(_) => {}
            HummockObjectId::HnswGraphFile(_) => {}
        };
        self.newly_added_sst_infos()
            .map(|sst| HummockObjectId::Sstable(sst.object_id()))
            .chain(
                self.vector_index_delta
                    .values()
                    .flat_map(|vector_index_delta| {
                        vector_index_delta
                            .newly_added_objects()
                            .map(|(object_id, _)| object_id)
                    }),
            )
            .collect()
    }

    pub fn newly_added_sst_ids(&self) -> HashSet<HummockSstableId> {
        self.newly_added_sst_infos()
            .map(|sst| sst.sst_id())
            .collect()
    }
}

impl<T> HummockVersionDeltaCommon<T> {
    pub fn newly_added_sst_infos(&self) -> impl Iterator<Item = &'_ T> {
        self.group_deltas.values().flat_map(|group_deltas| {
            group_deltas.group_deltas.iter().flat_map(|group_delta| {
                let sst_slice = match &group_delta {
                    GroupDeltaCommon::NewL0SubLevel(inserted_table_infos)
                    | GroupDeltaCommon::IntraLevel(IntraLevelDeltaCommon {
                        inserted_table_infos,
                        ..
                    }) => Some(inserted_table_infos.iter()),
                    GroupDeltaCommon::GroupConstruct(_)
                    | GroupDeltaCommon::GroupDestroy(_)
                    | GroupDeltaCommon::GroupMerge(_)
                    | GroupDeltaCommon::PruneTableIdsFromSsts(_) => None,
                };
                sst_slice.into_iter().flatten()
            })
        })
    }
}

impl HummockVersionDelta {
    #[expect(deprecated)]
    pub fn max_committed_epoch_for_migration(&self) -> HummockEpoch {
        self.max_committed_epoch
    }
}

impl<T> From<&PbHummockVersionDelta> for HummockVersionDeltaCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    fn from(pb_version_delta: &PbHummockVersionDelta) -> Self {
        #[expect(deprecated)]
        Self {
            id: pb_version_delta.id,
            prev_id: pb_version_delta.prev_id,
            group_deltas: pb_version_delta
                .group_deltas
                .iter()
                .map(|(group_id, deltas)| (*group_id, GroupDeltasCommon::from(deltas)))
                .collect(),
            max_committed_epoch: pb_version_delta.max_committed_epoch,
            trivial_move: pb_version_delta.trivial_move,
            new_table_watermarks: pb_version_delta
                .new_table_watermarks
                .iter()
                .map(|(table_id, watermarks)| (*table_id, TableWatermarks::from(watermarks)))
                .collect(),
            removed_table_ids: pb_version_delta.removed_table_ids.iter().copied().collect(),
            state_table_info_delta: pb_version_delta
                .state_table_info_delta
                .iter()
                .map(|(table_id, delta)| (*table_id, *delta))
                .collect(),
            vector_index_delta: pb_version_delta
                .vector_index_delta
                .iter()
                .map(|(table_id, delta)| (*table_id, delta.clone().into()))
                .collect(),
        }
    }
}

impl<T> From<&HummockVersionDeltaCommon<T>> for PbHummockVersionDelta
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(version_delta: &HummockVersionDeltaCommon<T>) -> Self {
        #[expect(deprecated)]
        Self {
            id: version_delta.id,
            prev_id: version_delta.prev_id,
            group_deltas: version_delta
                .group_deltas
                .iter()
                .map(|(group_id, deltas)| (*group_id, deltas.into()))
                .collect(),
            max_committed_epoch: version_delta.max_committed_epoch,
            trivial_move: version_delta.trivial_move,
            new_table_watermarks: version_delta
                .new_table_watermarks
                .iter()
                .map(|(table_id, watermarks)| (*table_id, watermarks.into()))
                .collect(),
            removed_table_ids: version_delta.removed_table_ids.iter().copied().collect(),
            change_log_delta: Default::default(),
            state_table_info_delta: version_delta.state_table_info_delta.clone(),
            vector_index_delta: version_delta
                .vector_index_delta
                .iter()
                .map(|(table_id, delta)| (*table_id, delta.clone().into()))
                .collect(),
        }
    }
}

impl<T> From<HummockVersionDeltaCommon<T>> for PbHummockVersionDelta
where
    PbSstableInfo: From<T>,
{
    fn from(version_delta: HummockVersionDeltaCommon<T>) -> Self {
        #[expect(deprecated)]
        Self {
            id: version_delta.id,
            prev_id: version_delta.prev_id,
            group_deltas: version_delta
                .group_deltas
                .into_iter()
                .map(|(group_id, deltas)| (group_id, deltas.into()))
                .collect(),
            max_committed_epoch: version_delta.max_committed_epoch,
            trivial_move: version_delta.trivial_move,
            new_table_watermarks: version_delta
                .new_table_watermarks
                .into_iter()
                .map(|(table_id, watermarks)| (table_id, watermarks.into()))
                .collect(),
            removed_table_ids: version_delta.removed_table_ids.into_iter().collect(),
            change_log_delta: Default::default(),
            state_table_info_delta: version_delta.state_table_info_delta,
            vector_index_delta: version_delta
                .vector_index_delta
                .into_iter()
                .map(|(table_id, delta)| (table_id, delta.into()))
                .collect(),
        }
    }
}

impl<T> From<PbHummockVersionDelta> for HummockVersionDeltaCommon<T>
where
    T: From<PbSstableInfo>,
{
    fn from(pb_version_delta: PbHummockVersionDelta) -> Self {
        #[expect(deprecated)]
        Self {
            id: pb_version_delta.id,
            prev_id: pb_version_delta.prev_id,
            group_deltas: pb_version_delta
                .group_deltas
                .into_iter()
                .map(|(group_id, deltas)| (group_id, deltas.into()))
                .collect(),
            max_committed_epoch: pb_version_delta.max_committed_epoch,
            trivial_move: pb_version_delta.trivial_move,
            new_table_watermarks: pb_version_delta
                .new_table_watermarks
                .into_iter()
                .map(|(table_id, watermarks)| (table_id, watermarks.into()))
                .collect(),
            removed_table_ids: pb_version_delta.removed_table_ids.into_iter().collect(),
            state_table_info_delta: pb_version_delta.state_table_info_delta,
            vector_index_delta: pb_version_delta
                .vector_index_delta
                .into_iter()
                .map(|(table_id, delta)| (table_id, delta.into()))
                .collect(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct IntraLevelDeltaCommon<T> {
    pub level_idx: u32,
    pub l0_sub_level_id: u64,
    pub removed_table_ids: HashSet<HummockSstableId>,
    pub inserted_table_infos: Vec<T>,
    pub vnode_partition_count: u32,
    pub compaction_group_version_id: u64,
}

pub type IntraLevelDelta = IntraLevelDeltaCommon<SstableInfo>;

impl IntraLevelDelta {
    pub fn estimated_encode_len(&self) -> usize {
        size_of::<u32>()
            + size_of::<u64>()
            + self.removed_table_ids.len() * size_of::<u32>()
            + self
                .inserted_table_infos
                .iter()
                .map(|sst| sst.estimated_encode_len())
                .sum::<usize>()
            + size_of::<u32>()
    }
}

impl<T> From<PbIntraLevelDelta> for IntraLevelDeltaCommon<T>
where
    T: From<PbSstableInfo>,
{
    fn from(pb_intra_level_delta: PbIntraLevelDelta) -> Self {
        Self {
            level_idx: pb_intra_level_delta.level_idx,
            l0_sub_level_id: pb_intra_level_delta.l0_sub_level_id,
            removed_table_ids: HashSet::from_iter(
                pb_intra_level_delta.removed_table_ids.iter().copied(),
            ),
            inserted_table_infos: pb_intra_level_delta
                .inserted_table_infos
                .into_iter()
                .map(Into::into)
                .collect_vec(),
            vnode_partition_count: pb_intra_level_delta.vnode_partition_count,
            compaction_group_version_id: pb_intra_level_delta.compaction_group_version_id,
        }
    }
}

impl<T> From<IntraLevelDeltaCommon<T>> for PbIntraLevelDelta
where
    PbSstableInfo: From<T>,
{
    fn from(intra_level_delta: IntraLevelDeltaCommon<T>) -> Self {
        Self {
            level_idx: intra_level_delta.level_idx,
            l0_sub_level_id: intra_level_delta.l0_sub_level_id,
            removed_table_ids: intra_level_delta.removed_table_ids.into_iter().collect(),
            inserted_table_infos: intra_level_delta
                .inserted_table_infos
                .into_iter()
                .map(Into::into)
                .collect_vec(),
            vnode_partition_count: intra_level_delta.vnode_partition_count,
            compaction_group_version_id: intra_level_delta.compaction_group_version_id,
        }
    }
}

impl<T> From<&IntraLevelDeltaCommon<T>> for PbIntraLevelDelta
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(intra_level_delta: &IntraLevelDeltaCommon<T>) -> Self {
        Self {
            level_idx: intra_level_delta.level_idx,
            l0_sub_level_id: intra_level_delta.l0_sub_level_id,
            removed_table_ids: intra_level_delta
                .removed_table_ids
                .iter()
                .copied()
                .collect(),
            inserted_table_infos: intra_level_delta
                .inserted_table_infos
                .iter()
                .map(Into::into)
                .collect_vec(),
            vnode_partition_count: intra_level_delta.vnode_partition_count,
            compaction_group_version_id: intra_level_delta.compaction_group_version_id,
        }
    }
}

impl<T> From<&PbIntraLevelDelta> for IntraLevelDeltaCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    fn from(pb_intra_level_delta: &PbIntraLevelDelta) -> Self {
        Self {
            level_idx: pb_intra_level_delta.level_idx,
            l0_sub_level_id: pb_intra_level_delta.l0_sub_level_id,
            removed_table_ids: HashSet::from_iter(
                pb_intra_level_delta.removed_table_ids.iter().copied(),
            ),
            inserted_table_infos: pb_intra_level_delta
                .inserted_table_infos
                .iter()
                .map(Into::into)
                .collect_vec(),
            vnode_partition_count: pb_intra_level_delta.vnode_partition_count,
            compaction_group_version_id: pb_intra_level_delta.compaction_group_version_id,
        }
    }
}

impl IntraLevelDelta {
    pub fn new(
        level_idx: u32,
        l0_sub_level_id: u64,
        removed_table_ids: HashSet<HummockSstableId>,
        inserted_table_infos: Vec<SstableInfo>,
        vnode_partition_count: u32,
        compaction_group_version_id: u64,
    ) -> Self {
        Self {
            level_idx,
            l0_sub_level_id,
            removed_table_ids,
            inserted_table_infos,
            vnode_partition_count,
            compaction_group_version_id,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum GroupDeltaCommon<T> {
    NewL0SubLevel(Vec<T>),
    IntraLevel(IntraLevelDeltaCommon<T>),
    GroupConstruct(Box<PbGroupConstruct>),
    GroupDestroy(PbGroupDestroy),
    GroupMerge(PbGroupMerge),
    /// Prunes table ids from SST metadata.
    ///
    /// Serialized as protobuf `truncate_tables` for compatibility.
    PruneTableIdsFromSsts(HashSet<TableId>),
}

pub type GroupDelta = GroupDeltaCommon<SstableInfo>;

impl<T> From<PbGroupDelta> for GroupDeltaCommon<T>
where
    T: From<PbSstableInfo>,
{
    fn from(pb_group_delta: PbGroupDelta) -> Self {
        match pb_group_delta.delta_type {
            Some(PbDeltaType::IntraLevel(pb_intra_level_delta)) => {
                GroupDeltaCommon::IntraLevel(IntraLevelDeltaCommon::from(pb_intra_level_delta))
            }
            Some(PbDeltaType::GroupConstruct(pb_group_construct)) => {
                GroupDeltaCommon::GroupConstruct(Box::new(pb_group_construct))
            }
            Some(PbDeltaType::GroupDestroy(pb_group_destroy)) => {
                GroupDeltaCommon::GroupDestroy(pb_group_destroy)
            }
            Some(PbDeltaType::GroupMerge(pb_group_merge)) => {
                GroupDeltaCommon::GroupMerge(pb_group_merge)
            }
            Some(DeltaType::NewL0SubLevel(pb_new_sub_level)) => GroupDeltaCommon::NewL0SubLevel(
                pb_new_sub_level
                    .inserted_table_infos
                    .into_iter()
                    .map(T::from)
                    .collect(),
            ),
            Some(PbDeltaType::TruncateTables(pb_truncate_tables)) => {
                GroupDeltaCommon::PruneTableIdsFromSsts(
                    pb_truncate_tables.table_ids.into_iter().collect(),
                )
            }

            None => panic!("delta_type is not set"),
        }
    }
}

impl<T> From<GroupDeltaCommon<T>> for PbGroupDelta
where
    PbSstableInfo: From<T>,
{
    fn from(group_delta: GroupDeltaCommon<T>) -> Self {
        match group_delta {
            GroupDeltaCommon::IntraLevel(intra_level_delta) => PbGroupDelta {
                delta_type: Some(PbDeltaType::IntraLevel(intra_level_delta.into())),
            },
            GroupDeltaCommon::GroupConstruct(pb_group_construct) => PbGroupDelta {
                delta_type: Some(PbDeltaType::GroupConstruct(*pb_group_construct)),
            },
            GroupDeltaCommon::GroupDestroy(pb_group_destroy) => PbGroupDelta {
                delta_type: Some(PbDeltaType::GroupDestroy(pb_group_destroy)),
            },
            GroupDeltaCommon::GroupMerge(pb_group_merge) => PbGroupDelta {
                delta_type: Some(PbDeltaType::GroupMerge(pb_group_merge)),
            },
            GroupDeltaCommon::NewL0SubLevel(new_sub_level) => PbGroupDelta {
                delta_type: Some(PbDeltaType::NewL0SubLevel(PbNewL0SubLevel {
                    inserted_table_infos: new_sub_level
                        .into_iter()
                        .map(PbSstableInfo::from)
                        .collect(),
                })),
            },
            GroupDeltaCommon::PruneTableIdsFromSsts(table_ids) => PbGroupDelta {
                delta_type: Some(PbDeltaType::TruncateTables(PbTruncateTables {
                    table_ids: table_ids.iter().copied().collect(),
                })),
            },
        }
    }
}

impl<T> From<&GroupDeltaCommon<T>> for PbGroupDelta
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(group_delta: &GroupDeltaCommon<T>) -> Self {
        match group_delta {
            GroupDeltaCommon::IntraLevel(intra_level_delta) => PbGroupDelta {
                delta_type: Some(PbDeltaType::IntraLevel(intra_level_delta.into())),
            },
            GroupDeltaCommon::GroupConstruct(pb_group_construct) => PbGroupDelta {
                delta_type: Some(PbDeltaType::GroupConstruct(*pb_group_construct.clone())),
            },
            GroupDeltaCommon::GroupDestroy(pb_group_destroy) => PbGroupDelta {
                delta_type: Some(PbDeltaType::GroupDestroy(*pb_group_destroy)),
            },
            GroupDeltaCommon::GroupMerge(pb_group_merge) => PbGroupDelta {
                delta_type: Some(PbDeltaType::GroupMerge(*pb_group_merge)),
            },
            GroupDeltaCommon::NewL0SubLevel(new_sub_level) => PbGroupDelta {
                delta_type: Some(PbDeltaType::NewL0SubLevel(PbNewL0SubLevel {
                    inserted_table_infos: new_sub_level.iter().map(PbSstableInfo::from).collect(),
                })),
            },
            GroupDeltaCommon::PruneTableIdsFromSsts(table_ids) => PbGroupDelta {
                delta_type: Some(PbDeltaType::TruncateTables(PbTruncateTables {
                    table_ids: table_ids.iter().copied().collect(),
                })),
            },
        }
    }
}

impl<T> From<&PbGroupDelta> for GroupDeltaCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    fn from(pb_group_delta: &PbGroupDelta) -> Self {
        match &pb_group_delta.delta_type {
            Some(PbDeltaType::IntraLevel(pb_intra_level_delta)) => {
                GroupDeltaCommon::IntraLevel(IntraLevelDeltaCommon::from(pb_intra_level_delta))
            }
            Some(PbDeltaType::GroupConstruct(pb_group_construct)) => {
                GroupDeltaCommon::GroupConstruct(Box::new(pb_group_construct.clone()))
            }
            Some(PbDeltaType::GroupDestroy(pb_group_destroy)) => {
                GroupDeltaCommon::GroupDestroy(*pb_group_destroy)
            }
            Some(PbDeltaType::GroupMerge(pb_group_merge)) => {
                GroupDeltaCommon::GroupMerge(*pb_group_merge)
            }
            Some(DeltaType::NewL0SubLevel(pb_new_sub_level)) => GroupDeltaCommon::NewL0SubLevel(
                pb_new_sub_level
                    .inserted_table_infos
                    .iter()
                    .map(T::from)
                    .collect(),
            ),
            Some(PbDeltaType::TruncateTables(pb_truncate_tables)) => {
                GroupDeltaCommon::PruneTableIdsFromSsts(
                    pb_truncate_tables.table_ids.iter().copied().collect(),
                )
            }
            None => panic!("delta_type is not set"),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct GroupDeltasCommon<T> {
    pub group_deltas: Vec<GroupDeltaCommon<T>>,
}

impl<T> Default for GroupDeltasCommon<T> {
    fn default() -> Self {
        Self {
            group_deltas: vec![],
        }
    }
}

pub type GroupDeltas = GroupDeltasCommon<SstableInfo>;

impl<T> From<PbGroupDeltas> for GroupDeltasCommon<T>
where
    T: From<PbSstableInfo>,
{
    fn from(pb_group_deltas: PbGroupDeltas) -> Self {
        Self {
            group_deltas: pb_group_deltas
                .group_deltas
                .into_iter()
                .map(GroupDeltaCommon::from)
                .collect_vec(),
        }
    }
}

impl<T> From<GroupDeltasCommon<T>> for PbGroupDeltas
where
    PbSstableInfo: From<T>,
{
    fn from(group_deltas: GroupDeltasCommon<T>) -> Self {
        Self {
            group_deltas: group_deltas
                .group_deltas
                .into_iter()
                .map(|group_delta| group_delta.into())
                .collect_vec(),
        }
    }
}

impl<T> From<&GroupDeltasCommon<T>> for PbGroupDeltas
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(group_deltas: &GroupDeltasCommon<T>) -> Self {
        Self {
            group_deltas: group_deltas
                .group_deltas
                .iter()
                .map(|group_delta| group_delta.into())
                .collect_vec(),
        }
    }
}

impl<T> From<&PbGroupDeltas> for GroupDeltasCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    fn from(pb_group_deltas: &PbGroupDeltas) -> Self {
        Self {
            group_deltas: pb_group_deltas
                .group_deltas
                .iter()
                .map(GroupDeltaCommon::from)
                .collect_vec(),
        }
    }
}

impl<T> GroupDeltasCommon<T>
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    pub fn to_protobuf(&self) -> PbGroupDeltas {
        self.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deprecated_change_log_delta_is_ignored() {
        let mut pb_delta = PbHummockVersionDelta {
            id: 1.into(),
            ..Default::default()
        };
        pb_delta
            .change_log_delta
            .insert(1.into(), Default::default());

        let delta = HummockVersionDelta::from_persisted_protobuf_owned(pb_delta);
        assert!(delta.to_protobuf().change_log_delta.is_empty());
    }
}
