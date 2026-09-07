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

use std::cmp;
use std::collections::Bound::{Excluded, Included};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_hummock_sdk::change_log::{EpochNewChangeLog, TableChangeLog, TableChangeLogs};
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    BranchedSstInfo, get_compaction_group_ids, get_table_compaction_group_id_mapping,
};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::{PbTableStatsMap, add_prost_table_stats_map};
use risingwave_hummock_sdk::version::{
    HummockVersion, HummockVersionDelta, MAX_HUMMOCK_VERSION_ID,
};
use risingwave_hummock_sdk::{
    CompactionGroupId, HummockContextId, HummockObjectId, HummockSstableId, HummockSstableObjectId,
    HummockVersionId, get_stale_object_ids,
};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::hummock::write_limits::WriteLimit;
use risingwave_pb::hummock::{HummockPinnedVersion, HummockVersionStats};
use risingwave_pb::id::TableId;
use risingwave_pb::meta::subscribe_response::{Info, Operation};

use super::GroupStateValidator;
use crate::MetaResult;
use crate::hummock::HummockManager;
use crate::hummock::error::{Error, Result};
use crate::hummock::manager::checkpoint::HummockVersionCheckpoint;
use crate::hummock::manager::commit_multi_var;
use crate::hummock::manager::context::ContextInfo;
use crate::hummock::manager::transaction::HummockVersionTransaction;
use crate::hummock::metrics_utils::{LocalTableMetrics, trigger_write_stop_stats};
use crate::hummock::model::CompactionGroup;
use crate::model::VarTransaction;

#[derive(Default)]
pub struct Versioning {
    // Volatile states below
    /// Avoid commit epoch epochs
    /// Don't persist compaction version delta to meta store
    pub disable_commit_epochs: bool,
    /// Latest hummock version
    pub current_version: Arc<HummockVersion>,
    pub local_metrics: HashMap<TableId, LocalTableMetrics>,
    pub time_travel_snapshot_interval_counter: u64,
    /// Used to avoid the attempts to rewrite the same SST to meta store
    pub last_time_travel_snapshot_sst_ids: HashSet<HummockSstableId>,

    // Persistent states below
    pub hummock_version_deltas: BTreeMap<HummockVersionId, HummockVersionDelta>,
    /// Stats for latest hummock version.
    pub version_stats: HummockVersionStats,
    pub checkpoint: HummockVersionCheckpoint,
    pub table_change_log: HashMap<TableId, TableChangeLog>,
}

impl ContextInfo {
    pub fn min_pinned_version_id(&self) -> HummockVersionId {
        let mut min_pinned_version_id = MAX_HUMMOCK_VERSION_ID;
        for id in self
            .pinned_versions
            .values()
            .map(|v| v.min_pinned_id)
            .chain(self.version_safe_points.iter().cloned())
        {
            min_pinned_version_id = cmp::min(id, min_pinned_version_id);
        }
        min_pinned_version_id
    }
}

impl Versioning {
    pub(super) fn mark_next_time_travel_version_snapshot(&mut self) {
        self.time_travel_snapshot_interval_counter = u64::MAX;
    }

    pub fn get_tracked_object_ids(
        &self,
        min_pinned_version_id: HummockVersionId,
    ) -> HashSet<HummockObjectId> {
        // object ids in checkpoint version
        let mut tracked_object_ids = self
            .checkpoint
            .version
            .get_object_ids()
            .chain(
                self.table_change_log
                    .values()
                    .flat_map(|c| c.get_object_ids()),
            )
            .collect::<HashSet<_>>();
        // add object ids added between checkpoint version and current version
        for (_, delta) in self.hummock_version_deltas.range((
            Excluded(self.checkpoint.version.id),
            Included(self.current_version.id),
        )) {
            tracked_object_ids.extend(delta.newly_added_object_ids());
        }
        // add stale object ids before the checkpoint version
        tracked_object_ids.extend(
            self.checkpoint
                .stale_objects
                .iter()
                .filter(|(version_id, _)| **version_id >= min_pinned_version_id)
                .flat_map(|(_, objects)| get_stale_object_ids(objects)),
        );
        tracked_object_ids
    }
}

impl HummockManager {
    pub async fn list_pinned_version(&self) -> Vec<HummockPinnedVersion> {
        self.context_info
            .read_with_process_name("list_pinned_version")
            .await
            .pinned_versions
            .values()
            .cloned()
            .collect_vec()
    }

    pub async fn list_workers(
        &self,
        context_ids: &[HummockContextId],
    ) -> MetaResult<HashMap<HummockContextId, WorkerNode>> {
        let mut workers = HashMap::new();
        for context_id in context_ids {
            if let Some(worker_node) = self
                .metadata_manager()
                .get_worker_by_id(*context_id as _)
                .await?
            {
                workers.insert(*context_id, worker_node);
            }
        }
        Ok(workers)
    }

    /// Gets current version without pinning it.
    /// Should not be called inside [`HummockManager`], because it requests locks internally.
    ///
    /// Note: this method can hurt performance because it will clone a large object.
    #[cfg(any(test, feature = "test"))]
    pub async fn get_current_version(&self) -> HummockVersion {
        self.on_current_version(|version| version.clone()).await
    }

    pub async fn on_current_version<T>(&self, mut f: impl FnMut(&HummockVersion) -> T) -> T {
        f(self
            .versioning
            .read_with_process_name("on_current_version")
            .await
            .current_version
            .as_ref())
    }

    pub async fn on_current_version_and_table_change_log<T>(
        &self,
        mut f: impl FnMut(&HummockVersion, &TableChangeLogs) -> T,
    ) -> T {
        let guard = self
            .versioning
            .read_with_process_name("on_current_version_and_table_change_log")
            .await;
        f(&guard.current_version, &guard.table_change_log)
    }

    pub async fn get_version_id(&self) -> HummockVersionId {
        self.on_current_version(|version| version.id).await
    }

    /// Gets the mapping from table id to compaction group id
    pub async fn get_table_compaction_group_id_mapping(
        &self,
    ) -> HashMap<StateTableId, CompactionGroupId> {
        get_table_compaction_group_id_mapping(
            &self
                .versioning
                .read_with_process_name("get_table_compaction_group_id_mapping")
                .await
                .current_version,
        )
    }

    /// Get version deltas from meta store
    pub async fn list_version_deltas(
        &self,
        start_id: HummockVersionId,
        num_limit: u32,
    ) -> Result<Vec<HummockVersionDelta>> {
        let versioning = self
            .versioning
            .read_with_process_name("list_version_deltas")
            .await;
        let version_deltas = versioning
            .hummock_version_deltas
            .range(start_id..)
            .map(|(_id, delta)| delta)
            .take(num_limit as _)
            .cloned()
            .collect();
        Ok(version_deltas)
    }

    pub async fn get_version_stats(&self) -> HummockVersionStats {
        self.versioning
            .read_with_process_name("get_version_stats")
            .await
            .version_stats
            .clone()
    }

    /// Updates write limits for `target_groups` and sends notification.
    /// Returns true if `write_limit` has been modified.
    /// The implementation acquires `versioning` lock and `compaction_group_manager` lock.
    pub(super) async fn try_update_write_limits(
        &self,
        target_group_ids: &[CompactionGroupId],
    ) -> bool {
        let versioning = self
            .versioning
            .read_with_process_name("try_update_write_limits")
            .await;
        let mut cg_manager = self
            .compaction_group_manager
            .write_with_process_name("try_update_write_limits")
            .await;
        let target_group_configs = target_group_ids
            .iter()
            .filter_map(|id| {
                cg_manager
                    .try_get_compaction_group_config(*id)
                    .map(|config| (*id, config))
            })
            .collect();
        let mut new_write_limits = calc_new_write_limits(
            target_group_configs,
            cg_manager.write_limit.clone(),
            &versioning.current_version,
        );
        let all_group_ids: HashSet<_> =
            HashSet::from_iter(get_compaction_group_ids(&versioning.current_version));
        new_write_limits.retain(|group_id, _| all_group_ids.contains(group_id));
        if new_write_limits == cg_manager.write_limit {
            return false;
        }
        tracing::debug!("Hummock stopped write is updated: {:#?}", new_write_limits);
        trigger_write_stop_stats(&self.metrics, &new_write_limits);
        cg_manager.write_limit = new_write_limits;
        self.env
            .notification_manager()
            .notify_hummock_without_version(
                Operation::Add,
                Info::HummockWriteLimits(risingwave_pb::hummock::WriteLimits {
                    write_limits: cg_manager.write_limit.clone(),
                }),
            );
        true
    }

    /// Gets write limits.
    /// The implementation acquires `versioning` lock.
    pub async fn write_limits(&self) -> HashMap<CompactionGroupId, WriteLimit> {
        let guard = self
            .compaction_group_manager
            .read_with_process_name("write_limits")
            .await;
        guard.write_limit.clone()
    }

    pub async fn list_branched_objects(&self) -> BTreeMap<HummockSstableObjectId, BranchedSstInfo> {
        let guard = self
            .versioning
            .read_with_process_name("list_branched_objects")
            .await;
        guard.current_version.build_branched_sst_info()
    }

    pub async fn rebuild_table_stats(&self) -> Result<()> {
        let mut versioning = self
            .versioning
            .write_with_process_name("rebuild_table_stats")
            .await;
        let new_stats = rebuild_table_stats(&versioning.current_version);
        let mut version_stats = VarTransaction::new(&mut versioning.version_stats);
        // version_stats.hummock_version_id is always 0 in meta store.
        version_stats.table_stats = new_stats.table_stats;
        commit_multi_var!(self.meta_store_ref(), version_stats)?;
        Ok(())
    }

    pub async fn may_fill_backward_state_table_info(&self) -> Result<()> {
        let mut versioning = self
            .versioning
            .write_with_process_name("may_fill_backward_state_table_info")
            .await;
        if versioning
            .current_version
            .need_fill_backward_compatible_state_table_info_delta()
        {
            let versioning: &mut Versioning = &mut versioning;
            let mut version = HummockVersionTransaction::new(
                &mut versioning.current_version,
                &mut versioning.hummock_version_deltas,
                &mut versioning.table_change_log,
                self.env.notification_manager(),
                None,
                &self.metrics,
                &self.env.opts,
                &self.version_stat_tx,
            );
            let mut new_version_delta = version.new_delta();
            new_version_delta.with_latest_version(|version, delta| {
                version.may_fill_backward_compatible_state_table_info_delta(delta)
            });
            new_version_delta.pre_apply();
            commit_multi_var!(self.meta_store_ref(), version)?;
        }
        Ok(())
    }

    pub async fn get_table_change_logs(
        &self,
        epoch_only: bool,
        start_epoch_inclusive: Option<u64>,
        end_epoch_inclusive: Option<u64>,
        table_ids: Option<HashSet<TableId>>,
        exclude_empty: bool,
        limit: Option<u32>,
    ) -> Result<TableChangeLogs> {
        let _timer = self.metrics.table_change_log_get_latency.start_timer();
        let start_epoch = start_epoch_inclusive.unwrap_or(0);
        let end_epoch = end_epoch_inclusive.unwrap_or(u64::MAX);
        if start_epoch > end_epoch {
            return Err(Error::InvalidEpochRange {
                start_epoch,
                end_epoch,
            });
        }
        let table_change_logs = self
            .on_current_version_and_table_change_log(|_, table_change_logs| {
                table_change_logs
                    .iter()
                    .filter_map(|(id, change_log)| {
                        if let Some(table_filter) = &table_ids
                            && !table_filter.contains(id)
                        {
                            return None;
                        }
                        let filtered_change_logs = change_log
                            .filter_epoch((start_epoch, end_epoch))
                            .filter(|change_log| {
                                if exclude_empty
                                    && change_log.new_value.is_empty()
                                    && change_log.old_value.is_empty()
                                {
                                    return false;
                                }
                                true
                            })
                            .take(limit.map(|l| l as usize).unwrap_or(usize::MAX))
                            .map(|change_log| {
                                if epoch_only {
                                    EpochNewChangeLog {
                                        new_value: vec![],
                                        old_value: vec![],
                                        non_checkpoint_epochs: change_log
                                            .non_checkpoint_epochs
                                            .clone(),
                                        checkpoint_epoch: change_log.checkpoint_epoch,
                                    }
                                } else {
                                    change_log.clone()
                                }
                            });
                        Some((id.to_owned(), TableChangeLog::new(filtered_change_logs)))
                    })
                    .collect()
            })
            .await;
        Ok(table_change_logs)
    }
}

/// Calculates write limits for `target_groups`.
/// Returns a new complete write limits snapshot based on `origin_snapshot` and `version`.
pub(super) fn calc_new_write_limits(
    target_groups: HashMap<CompactionGroupId, CompactionGroup>,
    origin_snapshot: HashMap<CompactionGroupId, WriteLimit>,
    version: &HummockVersion,
) -> HashMap<CompactionGroupId, WriteLimit> {
    let mut new_write_limits = origin_snapshot;
    for (id, config) in &target_groups {
        let levels = match version.levels.get(id) {
            None => {
                new_write_limits.remove(id);
                continue;
            }
            Some(levels) => levels,
        };

        let group_state = GroupStateValidator::check_single_group_write_stop(
            levels,
            config.compaction_config.as_ref(),
        );

        if group_state.is_write_stop() {
            new_write_limits.insert(
                *id,
                WriteLimit {
                    table_ids: version
                        .state_table_info
                        .compaction_group_member_table_ids(*id)
                        .iter()
                        .copied()
                        .collect(),
                    reason: group_state.reason().unwrap().to_owned(),
                },
            );
            continue;
        }
        // No condition is met.
        new_write_limits.remove(id);
    }
    new_write_limits
}

/// Rebuilds table stats from the given version.
/// Note that the result is approximate value. See `estimate_table_stats`.
fn rebuild_table_stats(version: &HummockVersion) -> HummockVersionStats {
    let mut stats = HummockVersionStats {
        hummock_version_id: version.id,
        table_stats: Default::default(),
    };
    for level in version.get_combined_levels() {
        for sst in &level.table_infos {
            let changes = estimate_table_stats(sst);
            add_prost_table_stats_map(&mut stats.table_stats, &changes);
        }
    }
    stats
}

/// Estimates table stats change from the given file.
/// - The file stats is evenly distributed among multiple tables within the file.
/// - The total key size and total value size are estimated based on key range and file size.
/// - Branched files may lead to an overestimation.
fn estimate_table_stats(sst: &SstableInfo) -> PbTableStatsMap {
    let mut changes: PbTableStatsMap = HashMap::default();
    let weighted_value =
        |value: i64| -> i64 { (value as f64 / sst.table_ids.len() as f64).ceil() as i64 };
    let key_range = &sst.key_range;
    let estimated_key_size: u64 = (key_range.left.len() + key_range.right.len()) as u64 / 2;
    let mut estimated_total_key_size = estimated_key_size * sst.total_key_count;
    if estimated_total_key_size > sst.uncompressed_file_size {
        estimated_total_key_size = sst.uncompressed_file_size / 2;
        tracing::warn!(
            %sst.sst_id,
            "Calculated estimated_total_key_size {} > uncompressed_file_size {}. Use uncompressed_file_size/2 as estimated_total_key_size instead.",
            estimated_total_key_size,
            sst.uncompressed_file_size
        );
    }
    let estimated_total_value_size = sst.uncompressed_file_size - estimated_total_key_size;
    for table_id in &sst.table_ids {
        let e = changes.entry(*table_id).or_default();
        e.total_key_count += weighted_value(sst.total_key_count as i64);
        e.total_key_size += weighted_value(estimated_total_key_size as i64);
        e.total_value_size += weighted_value(estimated_total_value_size as i64);
    }
    changes
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use itertools::Itertools;
    use risingwave_hummock_sdk::key_range::KeyRange;
    use risingwave_hummock_sdk::level::{Level, Levels};
    use risingwave_hummock_sdk::sstable_info::SstableInfoInner;
    use risingwave_hummock_sdk::version::{HummockVersion, MAX_HUMMOCK_VERSION_ID};
    use risingwave_hummock_sdk::{CompactionGroupId, HummockVersionId};
    use risingwave_pb::hummock::write_limits::WriteLimit;
    use risingwave_pb::hummock::{HummockPinnedVersion, HummockVersionStats};

    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::manager::context::ContextInfo;
    use crate::hummock::manager::versioning::{
        calc_new_write_limits, estimate_table_stats, rebuild_table_stats,
    };
    use crate::hummock::model::CompactionGroup;

    #[test]
    fn test_min_pinned_version_id() {
        let mut context_info = ContextInfo::default();
        assert_eq!(context_info.min_pinned_version_id(), MAX_HUMMOCK_VERSION_ID);
        context_info.pinned_versions.insert(
            1.into(),
            HummockPinnedVersion {
                context_id: 1.into(),
                min_pinned_id: 10.into(),
            },
        );
        assert_eq!(context_info.min_pinned_version_id(), 10);
        context_info
            .version_safe_points
            .push(HummockVersionId::new(5));
        assert_eq!(context_info.min_pinned_version_id(), 5);
        context_info.version_safe_points.clear();
        assert_eq!(context_info.min_pinned_version_id(), 10);
        context_info.pinned_versions.clear();
        assert_eq!(context_info.min_pinned_version_id(), MAX_HUMMOCK_VERSION_ID);
    }

    #[test]
    fn test_calc_new_write_limits() {
        let add_level_to_l0 = |levels: &mut Levels| {
            levels.l0.sub_levels.push(Level::default());
        };
        let set_sub_level_number_threshold_for_group_1 =
            |target_groups: &mut HashMap<CompactionGroupId, CompactionGroup>,
             sub_level_number_threshold: u64| {
                target_groups.insert(
                    1.into(),
                    CompactionGroup {
                        group_id: 1.into(),
                        compaction_config: Arc::new(
                            CompactionConfigBuilder::new()
                                .level0_stop_write_threshold_sub_level_number(
                                    sub_level_number_threshold,
                                )
                                .build(),
                        ),
                    },
                );
            };

        let set_level_0_max_sst_count_threshold_for_group_1 =
            |target_groups: &mut HashMap<CompactionGroupId, CompactionGroup>,
             max_sst_count_threshold: u32| {
                target_groups.insert(
                    1.into(),
                    CompactionGroup {
                        group_id: 1.into(),
                        compaction_config: Arc::new(
                            CompactionConfigBuilder::new()
                                .level0_stop_write_threshold_max_sst_count(Some(
                                    max_sst_count_threshold,
                                ))
                                .build(),
                        ),
                    },
                );
            };

        let set_level_0_max_size_threshold_for_group_1 =
            |target_groups: &mut HashMap<CompactionGroupId, CompactionGroup>,
             max_size_threshold: u64| {
                target_groups.insert(
                    1.into(),
                    CompactionGroup {
                        group_id: 1.into(),
                        compaction_config: Arc::new(
                            CompactionConfigBuilder::new()
                                .level0_stop_write_threshold_max_size(Some(max_size_threshold))
                                .build(),
                        ),
                    },
                );
            };

        let mut target_groups: HashMap<CompactionGroupId, CompactionGroup> = Default::default();
        set_sub_level_number_threshold_for_group_1(&mut target_groups, 10);
        let origin_snapshot: HashMap<CompactionGroupId, WriteLimit> = [(
            2.into(),
            WriteLimit {
                table_ids: [1, 2, 3].into_iter().map_into().collect(),
                reason: "for test".to_owned(),
            },
        )]
        .into_iter()
        .collect();
        let mut version: HummockVersion = Default::default();
        for group_id in 1..=3 {
            version.levels.insert(group_id.into(), Levels::default());
        }
        let new_write_limits =
            calc_new_write_limits(target_groups.clone(), origin_snapshot.clone(), &version);
        assert_eq!(
            new_write_limits, origin_snapshot,
            "write limit should not be triggered for group 1"
        );
        assert_eq!(new_write_limits.len(), 1);
        for _ in 1..=10 {
            add_level_to_l0(version.levels.get_mut(&1).unwrap());
            let new_write_limits =
                calc_new_write_limits(target_groups.clone(), origin_snapshot.clone(), &version);
            assert_eq!(
                new_write_limits, origin_snapshot,
                "write limit should not be triggered for group 1"
            );
        }
        add_level_to_l0(version.levels.get_mut(&1).unwrap());
        let new_write_limits =
            calc_new_write_limits(target_groups.clone(), origin_snapshot.clone(), &version);
        assert_ne!(
            new_write_limits, origin_snapshot,
            "write limit should be triggered for group 1"
        );
        assert_eq!(
            new_write_limits.get(&1).as_ref().unwrap().reason,
            "WriteStop(l0_level_count: 11, threshold: 10) too many L0 sub levels"
        );
        assert_eq!(new_write_limits.len(), 2);

        set_sub_level_number_threshold_for_group_1(&mut target_groups, 100);
        let new_write_limits =
            calc_new_write_limits(target_groups.clone(), origin_snapshot.clone(), &version);
        assert_eq!(
            new_write_limits, origin_snapshot,
            "write limit should not be triggered for group 1"
        );

        set_sub_level_number_threshold_for_group_1(&mut target_groups, 5);
        let new_write_limits =
            calc_new_write_limits(target_groups.clone(), origin_snapshot.clone(), &version);
        assert_ne!(
            new_write_limits, origin_snapshot,
            "write limit should be triggered for group 1"
        );
        assert_eq!(
            new_write_limits.get(&1).as_ref().unwrap().reason,
            "WriteStop(l0_level_count: 11, threshold: 5) too many L0 sub levels"
        );

        set_sub_level_number_threshold_for_group_1(&mut target_groups, 100);
        let last_level = version
            .levels
            .get_mut(&1)
            .unwrap()
            .l0
            .sub_levels
            .last_mut()
            .unwrap();
        last_level.table_infos.extend(vec![
            SstableInfoInner {
                key_range: KeyRange::default(),
                table_ids: vec![1.into(), 2.into(), 3.into()],
                total_key_count: 100,
                sst_size: 100,
                uncompressed_file_size: 100,
                ..Default::default()
            }
            .into(),
            SstableInfoInner {
                key_range: KeyRange::default(),
                table_ids: vec![1.into(), 2.into(), 3.into()],
                total_key_count: 100,
                sst_size: 100,
                uncompressed_file_size: 100,
                ..Default::default()
            }
            .into(),
        ]);
        version.levels.get_mut(&1).unwrap().l0.total_file_size += 200;
        let new_write_limits =
            calc_new_write_limits(target_groups.clone(), origin_snapshot.clone(), &version);
        assert_eq!(
            new_write_limits, origin_snapshot,
            "write limit should not be triggered for group 1"
        );

        set_level_0_max_size_threshold_for_group_1(&mut target_groups, 10);
        let new_write_limits =
            calc_new_write_limits(target_groups.clone(), origin_snapshot.clone(), &version);
        assert_ne!(
            new_write_limits, origin_snapshot,
            "write limit should be triggered for group 1"
        );
        assert_eq!(
            new_write_limits.get(&1).as_ref().unwrap().reason,
            "WriteStop(l0_size: 200, threshold: 10) too large L0 size"
        );

        set_level_0_max_size_threshold_for_group_1(&mut target_groups, 10000);
        let new_write_limits =
            calc_new_write_limits(target_groups.clone(), origin_snapshot.clone(), &version);
        assert_eq!(
            new_write_limits, origin_snapshot,
            "write limit should not be triggered for group 1"
        );

        set_level_0_max_sst_count_threshold_for_group_1(&mut target_groups, 1);
        let new_write_limits =
            calc_new_write_limits(target_groups.clone(), origin_snapshot.clone(), &version);
        assert_ne!(
            new_write_limits, origin_snapshot,
            "write limit should be triggered for group 1"
        );
        assert_eq!(
            new_write_limits.get(&1).as_ref().unwrap().reason,
            "WriteStop(l0_sst_count: 2, threshold: 1) too many L0 sst files"
        );

        set_level_0_max_sst_count_threshold_for_group_1(&mut target_groups, 100);
        let new_write_limits =
            calc_new_write_limits(target_groups, origin_snapshot.clone(), &version);

        assert_eq!(
            new_write_limits, origin_snapshot,
            "write limit should not be triggered for group 1"
        );
    }

    #[test]
    fn test_estimate_table_stats() {
        let sst = SstableInfoInner {
            key_range: KeyRange {
                left: vec![1; 10].into(),
                right: vec![1; 20].into(),
                ..Default::default()
            },
            table_ids: vec![1.into(), 2.into(), 3.into()],
            total_key_count: 6000,
            uncompressed_file_size: 6_000_000,
            ..Default::default()
        }
        .into();
        let changes = estimate_table_stats(&sst);
        assert_eq!(changes.len(), 3);
        for stats in changes.values() {
            assert_eq!(stats.total_key_count, 6000 / 3);
            assert_eq!(stats.total_key_size, (10 + 20) / 2 * 6000 / 3);
            assert_eq!(
                stats.total_value_size,
                (6_000_000 - (10 + 20) / 2 * 6000) / 3
            );
        }

        let mut version = HummockVersion::default();
        version.id = HummockVersionId::new(123);

        for cg in 1..3 {
            version.levels.insert(
                cg.into(),
                Levels {
                    levels: vec![Level {
                        table_infos: vec![sst.clone()],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            );
        }
        let HummockVersionStats {
            hummock_version_id,
            table_stats,
        } = rebuild_table_stats(&version);
        assert_eq!(hummock_version_id, version.id);
        assert_eq!(table_stats.len(), 3);
        for (tid, stats) in table_stats {
            assert_eq!(
                stats.total_key_count,
                changes.get(&tid).unwrap().total_key_count * 2
            );
            assert_eq!(
                stats.total_key_size,
                changes.get(&tid).unwrap().total_key_size * 2
            );
            assert_eq!(
                stats.total_value_size,
                changes.get(&tid).unwrap().total_value_size * 2
            );
        }
    }

    #[test]
    fn test_estimate_table_stats_large_key_range() {
        let sst = SstableInfoInner {
            key_range: KeyRange {
                left: vec![1; 1000].into(),
                right: vec![1; 2000].into(),
                ..Default::default()
            },
            table_ids: vec![1.into(), 2.into(), 3.into()],
            total_key_count: 6000,
            uncompressed_file_size: 60_000,
            ..Default::default()
        }
        .into();
        let changes = estimate_table_stats(&sst);
        assert_eq!(changes.len(), 3);
        for t in &sst.table_ids {
            let stats = changes.get(t).unwrap();
            assert_eq!(stats.total_key_count, 6000 / 3);
            assert_eq!(stats.total_key_size, 60_000 / 2 / 3);
            assert_eq!(stats.total_value_size, (60_000 - 60_000 / 2) / 3);
        }
    }
}
