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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_common::monitor::MonitoredRwLock;
use risingwave_hummock_sdk::compact_task::{ReportTask, is_compaction_task_expired};
use risingwave_hummock_sdk::compaction_group::{
    StateTableId, StaticCompactionGroupId, group_split,
};
use risingwave_hummock_sdk::version::{GroupDelta, GroupDeltas, HummockVersion};
use risingwave_hummock_sdk::{CompactionGroupId, can_concat};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::mutable_config::MutableConfig;
use risingwave_pb::hummock::{
    CompatibilityVersion, PbGroupConstruct, PbGroupMerge, PbStateTableInfoDelta,
};
use thiserror_ext::AsReport;

use super::compaction_group_manager::CompactionGroupManager;
use super::{CompactionGroupStatistic, GroupStateValidator};
use crate::hummock::error::{Error, Result};
use crate::hummock::manager::transaction::HummockVersionTransaction;
use crate::hummock::manager::versioning::Versioning;
use crate::hummock::manager::{HummockManager, commit_multi_var};
use crate::hummock::metrics_utils::remove_compaction_group_metrics;
use crate::hummock::sequence::{next_compaction_group_id, next_sstable_id};
use crate::hummock::table_write_throughput_statistic::{
    TableWriteThroughputStatistic, TableWriteThroughputStatisticManager,
};
use crate::manager::MetaOpts;

#[derive(Debug, PartialEq, Eq)]
struct NormalizePlan {
    parent_group_id: CompactionGroupId,
    parent_table_ids: Vec<StateTableId>,
    boundary_table_id: StateTableId,
}

impl NormalizePlan {
    fn split_key(&self) -> Bytes {
        group_split::build_split_full_key(self.boundary_table_id, VirtualNode::ZERO)
            .encode()
            .into()
    }

    fn split_table_ids(&self) -> (Vec<StateTableId>, Vec<StateTableId>) {
        let split_full_key =
            group_split::build_split_full_key(self.boundary_table_id, VirtualNode::ZERO);
        let (table_ids_left, table_ids_right) =
            group_split::split_table_ids_with_table_id_and_vnode(
                &self.parent_table_ids,
                split_full_key.user_key.table_id,
                split_full_key.user_key.get_vnode_id(),
            );
        assert!(!table_ids_left.is_empty() && !table_ids_right.is_empty());
        (table_ids_left, table_ids_right)
    }
}

fn gen_normalize_plan(
    left: &CompactionGroupStatistic,
    right: &CompactionGroupStatistic,
) -> Option<NormalizePlan> {
    let left_table_ids = left.table_statistic.keys().copied().collect_vec();

    if left_table_ids.len() <= 1 {
        return None;
    }

    let left_max = *left_table_ids.last().unwrap();
    let right_min = *right.table_statistic.keys().next().unwrap();
    if left_max < right_min {
        return None;
    }

    let boundary_index = left_table_ids.partition_point(|&table_id| table_id < right_min);
    if boundary_index == 0 || boundary_index >= left_table_ids.len() {
        return None;
    }
    let boundary_table_id = left_table_ids[boundary_index];

    Some(NormalizePlan {
        parent_group_id: left.group_id,
        parent_table_ids: left_table_ids,
        boundary_table_id,
    })
}

fn build_normalize_plan_from_group_statistics(
    groups: &[CompactionGroupStatistic],
) -> Option<NormalizePlan> {
    // `calculate_compaction_group_statistic()` iterates all version levels, so newly created or
    // transiently empty groups can appear here without any member tables.
    let mut groups = groups
        .iter()
        .filter(|group| !group.table_statistic.is_empty())
        .collect_vec();
    groups.sort_by_key(|group| *group.table_statistic.keys().next().unwrap());

    groups
        .split(|group| {
            group
                .compaction_group_config
                .compaction_config
                .disable_auto_group_scheduling
                .unwrap_or(false)
        })
        .find_map(|segment| {
            segment
                .windows(2)
                .find_map(|pair| gen_normalize_plan(pair[0], pair[1]))
        })
}

fn collect_normalize_group_statistics(
    version: &HummockVersion,
    compaction_group_manager: &CompactionGroupManager,
) -> Result<Vec<CompactionGroupStatistic>> {
    let mut groups = vec![];
    for group_id in version.levels.keys() {
        let table_ids = version
            .state_table_info
            .compaction_group_member_table_ids(*group_id)
            .iter()
            .copied()
            .collect_vec();
        if table_ids.is_empty() {
            continue;
        }

        let group_config = compaction_group_manager
            .try_get_compaction_group_config(*group_id)
            .ok_or_else(|| {
                Error::CompactionGroup(format!(
                    "group {} config not found during normalize",
                    group_id
                ))
            })?;
        groups.push(CompactionGroupStatistic {
            group_id: *group_id,
            group_size: 0,
            table_statistic: table_ids
                .into_iter()
                .map(|table_id| (table_id, 0))
                .collect(),
            compaction_group_config: group_config,
        });
    }
    Ok(groups)
}

impl HummockManager {
    pub async fn merge_compaction_group(
        &self,
        group_1: CompactionGroupId,
        group_2: CompactionGroupId,
    ) -> Result<()> {
        self.merge_compaction_group_impl(group_1, group_2, None)
            .await
    }

    pub async fn merge_compaction_group_for_test(
        &self,
        group_1: CompactionGroupId,
        group_2: CompactionGroupId,
        created_tables: HashSet<TableId>,
    ) -> Result<()> {
        self.merge_compaction_group_impl(group_1, group_2, Some(created_tables))
            .await
    }

    pub async fn merge_compaction_group_impl(
        &self,
        group_1: CompactionGroupId,
        group_2: CompactionGroupId,
        created_tables: Option<HashSet<TableId>>,
    ) -> Result<()> {
        let compaction_guard = self
            .compaction
            .write_with_process_name("merge_compaction_group_impl")
            .await;
        let mut versioning_guard = self
            .versioning
            .write_with_process_name("merge_compaction_group_impl")
            .await;
        let versioning = versioning_guard.deref_mut();
        // Validate parameters.
        if !versioning.current_version.levels.contains_key(&group_1) {
            return Err(Error::CompactionGroup(format!("invalid group {}", group_1)));
        }

        if !versioning.current_version.levels.contains_key(&group_2) {
            return Err(Error::CompactionGroup(format!("invalid group {}", group_2)));
        }

        let state_table_info = versioning.current_version.state_table_info.clone();
        let mut member_table_ids_1 = state_table_info
            .compaction_group_member_table_ids(group_1)
            .iter()
            .cloned()
            .collect_vec();

        if member_table_ids_1.is_empty() {
            return Err(Error::CompactionGroup(format!(
                "group_1 {} is empty",
                group_1
            )));
        }

        let mut member_table_ids_2 = state_table_info
            .compaction_group_member_table_ids(group_2)
            .iter()
            .cloned()
            .collect_vec();

        if member_table_ids_2.is_empty() {
            return Err(Error::CompactionGroup(format!(
                "group_2 {} is empty",
                group_2
            )));
        }

        debug_assert!(!member_table_ids_1.is_empty());
        debug_assert!(!member_table_ids_2.is_empty());
        assert!(member_table_ids_1.is_sorted());
        assert!(member_table_ids_2.is_sorted());

        let created_tables = if let Some(created_tables) = created_tables {
            // if the created_tables is provided, use it directly, most for test
            #[expect(clippy::assertions_on_constants)]
            {
                assert!(cfg!(debug_assertions));
            }
            created_tables
        } else {
            match self.metadata_manager.get_created_table_ids().await {
                Ok(created_tables) => HashSet::from_iter(created_tables),
                Err(err) => {
                    tracing::warn!(error = %err.as_report(), "failed to fetch created table ids");
                    return Err(Error::CompactionGroup(format!(
                        "merge group_1 {} group_2 {} failed to fetch created table ids",
                        group_1, group_2
                    )));
                }
            }
        };

        fn contains_creating_table(
            table_ids: &Vec<TableId>,
            created_tables: &HashSet<TableId>,
        ) -> bool {
            table_ids
                .iter()
                .any(|table_id| !created_tables.contains(table_id))
        }

        // do not merge the compaction group which is creating
        if contains_creating_table(&member_table_ids_1, &created_tables)
            || contains_creating_table(&member_table_ids_2, &created_tables)
        {
            return Err(Error::CompactionGroup(format!(
                "Cannot merge creating group {} next_group {} member_table_ids_1 {:?} member_table_ids_2 {:?}",
                group_1, group_2, member_table_ids_1, member_table_ids_2
            )));
        }

        // Make sure `member_table_ids_1` is smaller than `member_table_ids_2`
        let (left_group_id, right_group_id) =
            if member_table_ids_1.first().unwrap() < member_table_ids_2.first().unwrap() {
                (group_1, group_2)
            } else {
                std::mem::swap(&mut member_table_ids_1, &mut member_table_ids_2);
                (group_2, group_1)
            };

        // We can only merge two groups with non-overlapping member table ids.
        // After the swap above, member_table_ids_1 has the smaller first element.
        // If the last element of member_table_ids_1 >= the first element of member_table_ids_2,
        // the two groups' table id ranges overlap and cannot be merged.
        if member_table_ids_1.last().unwrap() >= member_table_ids_2.first().unwrap() {
            return Err(Error::CompactionGroup(format!(
                "invalid merge group_1 {} group_2 {}: table id ranges overlap",
                left_group_id, right_group_id
            )));
        }

        let combined_member_table_ids = member_table_ids_1
            .iter()
            .chain(member_table_ids_2.iter())
            .collect_vec();
        assert!(combined_member_table_ids.is_sorted());

        // check duplicated sst_id
        let mut sst_id_set = HashSet::new();
        for sst_id in versioning
            .current_version
            .get_sst_ids_by_group_id(left_group_id)
            .chain(
                versioning
                    .current_version
                    .get_sst_ids_by_group_id(right_group_id),
            )
        {
            if !sst_id_set.insert(sst_id) {
                return Err(Error::CompactionGroup(format!(
                    "invalid merge group_1 {} group_2 {} duplicated sst_id {}",
                    left_group_id, right_group_id, sst_id
                )));
            }
        }

        // check branched sst on non-overlap level
        {
            let left_levels = versioning
                .current_version
                .get_compaction_group_levels(group_1);

            let right_levels = versioning
                .current_version
                .get_compaction_group_levels(group_2);

            // we can not check the l0 sub level, because the sub level id will be rewritten when merge
            // This check will ensure that other non-overlapping level ssts can be concat and that the key_range is correct.
            let max_level = std::cmp::max(left_levels.levels.len(), right_levels.levels.len());
            for level_idx in 1..=max_level {
                let left_level = left_levels.get_level(level_idx);
                let right_level = right_levels.get_level(level_idx);
                if left_level.table_infos.is_empty() || right_level.table_infos.is_empty() {
                    continue;
                }

                let left_last_sst = left_level.table_infos.last().unwrap().clone();
                let right_first_sst = right_level.table_infos.first().unwrap().clone();
                let left_sst_id = left_last_sst.sst_id;
                let right_sst_id = right_first_sst.sst_id;
                let left_obj_id = left_last_sst.object_id;
                let right_obj_id = right_first_sst.object_id;

                // Since the sst key_range within a group is legal, we only need to check the ssts adjacent to the two groups.
                if !can_concat(&[left_last_sst, right_first_sst]) {
                    return Err(Error::CompactionGroup(format!(
                        "invalid merge group_1 {} group_2 {} level_idx {} left_last_sst_id {} right_first_sst_id {} left_obj_id {} right_obj_id {}",
                        left_group_id,
                        right_group_id,
                        level_idx,
                        left_sst_id,
                        right_sst_id,
                        left_obj_id,
                        right_obj_id
                    )));
                }
            }
        }

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

        let target_compaction_group_id = {
            // merge right_group_id to left_group_id and remove right_group_id
            new_version_delta.group_deltas.insert(
                left_group_id,
                GroupDeltas {
                    group_deltas: vec![GroupDelta::GroupMerge(PbGroupMerge {
                        left_group_id,
                        right_group_id,
                    })],
                },
            );
            left_group_id
        };

        // TODO: remove compaciton group_id from state_table_info
        // rewrite compaction_group_id for all tables
        new_version_delta.with_latest_version(|version, new_version_delta| {
            for &table_id in combined_member_table_ids {
                let info = version
                    .state_table_info
                    .info()
                    .get(&table_id)
                    .expect("have check exist previously");
                assert!(
                    new_version_delta
                        .state_table_info_delta
                        .insert(
                            table_id,
                            PbStateTableInfoDelta {
                                committed_epoch: info.committed_epoch,
                                compaction_group_id: target_compaction_group_id,
                            }
                        )
                        .is_none()
                );
            }
        });

        {
            let mut compaction_group_manager = self
                .compaction_group_manager
                .write_with_process_name("merge_compaction_group_impl")
                .await;
            let mut compaction_groups_txn = compaction_group_manager.start_compaction_groups_txn();

            // for metrics reclaim
            {
                let right_group_max_level = new_version_delta
                    .latest_version()
                    .get_compaction_group_levels(right_group_id)
                    .levels
                    .len();

                remove_compaction_group_metrics(
                    &self.metrics,
                    right_group_id,
                    right_group_max_level,
                );
            }

            // clean up compaction schedule state for the merged group
            self.compaction_state
                .remove_compaction_group(right_group_id);

            // clear `partition_vnode_count` for the hybrid group
            {
                if let Err(err) = compaction_groups_txn.update_compaction_config(
                    &[left_group_id],
                    &[MutableConfig::SplitWeightByVnode(0)], // default
                ) {
                    tracing::error!(
                        error = %err.as_report(),
                        "failed to update compaction config for group-{}",
                        left_group_id
                    );
                }
            }

            new_version_delta.pre_apply();

            // remove right_group_id
            compaction_groups_txn.remove(right_group_id);
            commit_multi_var!(self.meta_store_ref(), version, compaction_groups_txn)?;
        }

        // Instead of handling DeltaType::GroupConstruct for time travel, simply enforce a version snapshot.
        versioning.mark_next_time_travel_version_snapshot();

        // cancel tasks
        let mut canceled_tasks = vec![];
        // after merge, all tasks in right_group_id should be canceled
        // Failure of cancel does not cause correctness problems, the report task will have better interception, and the operation here is designed to free up compactor compute resources more quickly.
        let compact_task_assignments =
            compaction_guard.get_compact_task_assignments_by_group_id(right_group_id);
        compact_task_assignments
            .into_iter()
            .for_each(|task_assignment| {
                let task = &task_assignment.compact_task;
                assert_eq!(task.compaction_group_id, right_group_id);
                canceled_tasks.push(ReportTask {
                    task_id: task.task_id,
                    task_status: TaskStatus::ManualCanceled,
                    table_stats_change: HashMap::default(),
                    sorted_output_ssts: vec![],
                    object_timestamps: HashMap::default(),
                });
            });

        if !canceled_tasks.is_empty() {
            self.report_compact_tasks_impl(canceled_tasks, compaction_guard, versioning_guard)
                .await?;
        } else {
            drop(versioning_guard);
            drop(compaction_guard);
        }

        self.try_update_write_limits(&[left_group_id, right_group_id])
            .await;

        self.metrics
            .merge_compaction_group_count
            .with_label_values(&[&left_group_id.to_string()])
            .inc();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use risingwave_hummock_sdk::CompactionGroupId;
    use risingwave_pb::hummock::CompactionConfig;

    use super::{
        CompactionGroupStatistic, NormalizePlan, build_normalize_plan_from_group_statistics,
        gen_normalize_plan,
    };
    use crate::hummock::model::CompactionGroup;

    fn group(
        group_id: CompactionGroupId,
        table_ids: &[u32],
        disable_auto_group_scheduling: bool,
    ) -> CompactionGroupStatistic {
        let config = CompactionConfig {
            disable_auto_group_scheduling: Some(disable_auto_group_scheduling),
            ..Default::default()
        };
        CompactionGroupStatistic {
            group_id,
            group_size: 0,
            table_statistic: table_ids
                .iter()
                .copied()
                .map(|table_id| (table_id.into(), 0_u64))
                .collect::<BTreeMap<_, _>>(),
            compaction_group_config: CompactionGroup::new(group_id, config),
        }
    }

    #[test]
    fn test_gen_normalize_plan_returns_none_for_single_table_group() {
        let left = group(1.into(), &[10], false);
        let right = group(2.into(), &[5, 20], false);

        assert_eq!(None, gen_normalize_plan(&left, &right));
    }

    #[test]
    fn test_gen_normalize_plan_returns_none_for_non_overlapping_groups() {
        let left = group(1.into(), &[1, 2, 3], false);
        let right = group(2.into(), &[4, 5, 6], false);

        assert_eq!(None, gen_normalize_plan(&left, &right));
    }

    #[test]
    fn test_gen_normalize_plan_returns_none_when_boundary_cannot_split_parent() {
        let left = group(1.into(), &[5, 6, 7], false);
        let right = group(2.into(), &[4, 8], false);

        assert_eq!(None, gen_normalize_plan(&left, &right));
    }

    #[test]
    fn test_gen_normalize_plan_generates_expected_boundary() {
        let left = group(1.into(), &[1, 4, 7], false);
        let right = group(2.into(), &[2, 5, 8], false);

        assert_eq!(
            Some(NormalizePlan {
                parent_group_id: 1.into(),
                parent_table_ids: vec![1.into(), 4.into(), 7.into()],
                boundary_table_id: 4.into(),
            }),
            gen_normalize_plan(&left, &right)
        );
    }

    #[test]
    fn test_build_normalize_plan_skips_disabled_boundary_and_continues_later_segment() {
        let groups = vec![
            group(1.into(), &[1, 4, 7], false),
            group(2.into(), &[2, 5, 8], true),
            group(3.into(), &[10, 13, 16], false),
            group(4.into(), &[11, 14, 17], false),
        ];

        assert_eq!(
            Some(NormalizePlan {
                parent_group_id: 3.into(),
                parent_table_ids: vec![10.into(), 13.into(), 16.into()],
                boundary_table_id: 13.into(),
            }),
            build_normalize_plan_from_group_statistics(&groups)
        );
    }
}

impl HummockManager {
    /// Split `table_ids` to a dedicated compaction group.(will be split by the `table_id` and `vnode`.)
    /// Returns the compaction group id containing the `table_ids` and the mapping of compaction group id to table ids.
    /// The split will follow the following rules
    /// 1. ssts with `key_range.left` greater than `split_key` will be split to the right group
    /// 2. the sst containing `split_key` will be split into two separate ssts and their `key_range` will be changed `sst_1`: [`sst.key_range.left`, `split_key`) `sst_2`: [`split_key`, `sst.key_range.right`]
    /// 3. currently only `vnode` 0 and `vnode` max is supported. (Due to the above rule, vnode max will be rewritten as `table_id` + 1, `vnode` 0)
    ///   - `parent_group_id`: the `group_id` to split
    ///   - `split_table_ids`: the `table_ids` to split, now we still support to split multiple tables to one group at once, pass `split_table_ids` for per `split` operation for checking
    ///   - `table_id_to_split`: the `table_id` to split
    ///   - `vnode_to_split`: the `vnode` to split
    ///   - `partition_vnode_count`: the partition count for the single table group if need
    async fn split_compaction_group_impl(
        &self,
        parent_group_id: CompactionGroupId,
        split_table_ids: &[StateTableId],
        table_id_to_split: StateTableId,
        vnode_to_split: VirtualNode,
        partition_vnode_count: Option<u32>,
    ) -> Result<Vec<(CompactionGroupId, Vec<StateTableId>)>> {
        let mut result = vec![];
        let compaction_guard = self
            .compaction
            .write_with_process_name("split_compaction_group_impl")
            .await;
        let mut versioning_guard = self
            .versioning
            .write_with_process_name("split_compaction_group_impl")
            .await;
        let versioning = versioning_guard.deref_mut();
        // Validate parameters.
        if !versioning
            .current_version
            .levels
            .contains_key(&parent_group_id)
        {
            return Err(Error::CompactionGroup(format!(
                "invalid group {}",
                parent_group_id
            )));
        }

        let member_table_ids = versioning
            .current_version
            .state_table_info
            .compaction_group_member_table_ids(parent_group_id)
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();

        if !member_table_ids.contains(&table_id_to_split) {
            return Err(Error::CompactionGroup(format!(
                "table {} doesn't in group {}",
                table_id_to_split, parent_group_id
            )));
        }

        let split_full_key = group_split::build_split_full_key(table_id_to_split, vnode_to_split);

        // change to vec for partition
        let table_ids = member_table_ids.into_iter().collect_vec();
        if table_ids == split_table_ids {
            return Err(Error::CompactionGroup(format!(
                "invalid split attempt for group {}: all member tables are moved",
                parent_group_id
            )));
        }
        // avoid decode split_key when caller is aware of the table_id and vnode
        let (table_ids_left, table_ids_right) =
            group_split::split_table_ids_with_table_id_and_vnode(
                &table_ids,
                split_full_key.user_key.table_id,
                split_full_key.user_key.get_vnode_id(),
            );
        if table_ids_left.is_empty() || table_ids_right.is_empty() {
            // not need to split group if all tables are in the same side
            if !table_ids_left.is_empty() {
                result.push((parent_group_id, table_ids_left));
            }

            if !table_ids_right.is_empty() {
                result.push((parent_group_id, table_ids_right));
            }
            return Ok(result);
        }

        result.push((parent_group_id, table_ids_left));

        let split_key: Bytes = split_full_key.encode().into();

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

        let split_sst_count = new_version_delta
            .latest_version()
            .count_new_ssts_in_group_split(parent_group_id, split_key.clone());

        let new_sst_start_id = next_sstable_id(&self.env, split_sst_count).await?;
        let (new_compaction_group_id, config) = {
            // All NewCompactionGroup pairs are mapped to one new compaction group.
            let new_compaction_group_id = next_compaction_group_id(&self.env).await?;
            // Inherit config from parent group
            let config = self
                .compaction_group_manager
                .read_with_process_name("split_compaction_group_impl")
                .await
                .try_get_compaction_group_config(parent_group_id)
                .ok_or_else(|| {
                    Error::CompactionGroup(format!(
                        "parent group {} config not found",
                        parent_group_id
                    ))
                })?
                .compaction_config()
                .as_ref()
                .clone();

            #[expect(deprecated)]
            // fill the deprecated field with default value
            new_version_delta.group_deltas.insert(
                new_compaction_group_id,
                GroupDeltas {
                    group_deltas: vec![GroupDelta::GroupConstruct(Box::new(PbGroupConstruct {
                        group_config: Some(config.clone()),
                        group_id: new_compaction_group_id,
                        parent_group_id,
                        new_sst_start_id,
                        table_ids: vec![],
                        version: CompatibilityVersion::LATEST as _, // for compatibility
                        split_key: Some(split_key.into()),
                    }))],
                },
            );
            (new_compaction_group_id, config)
        };

        new_version_delta.with_latest_version(|version, new_version_delta| {
            for &table_id in &table_ids_right {
                let info = version
                    .state_table_info
                    .info()
                    .get(&table_id)
                    .expect("have check exist previously");
                assert!(
                    new_version_delta
                        .state_table_info_delta
                        .insert(
                            table_id,
                            PbStateTableInfoDelta {
                                committed_epoch: info.committed_epoch,
                                compaction_group_id: new_compaction_group_id,
                            }
                        )
                        .is_none()
                );
            }
        });

        result.push((new_compaction_group_id, table_ids_right));

        {
            let mut compaction_group_manager = self
                .compaction_group_manager
                .write_with_process_name("split_compaction_group_impl")
                .await;
            let mut compaction_groups_txn = compaction_group_manager.start_compaction_groups_txn();
            compaction_groups_txn
                .create_compaction_groups(new_compaction_group_id, Arc::new(config));

            // check if need to update the compaction config for the single table group and guarantee the operation atomicity
            // `partition_vnode_count` only works inside a table, to avoid a lot of slicing sst, we only enable it in groups with high throughput and only one table.
            // The target `table_ids` might be split to an existing group, so we need to try to update its config
            for (cg_id, table_ids) in &result {
                // check the split_tables had been place to the dedicated compaction group
                if let Some(partition_vnode_count) = partition_vnode_count
                    && table_ids.len() == 1
                    && table_ids == split_table_ids
                    && let Err(err) = compaction_groups_txn.update_compaction_config(
                        &[*cg_id],
                        &[MutableConfig::SplitWeightByVnode(partition_vnode_count)],
                    )
                {
                    tracing::error!(
                        error = %err.as_report(),
                        "failed to update compaction config for group-{}",
                        cg_id
                    );
                }
            }

            new_version_delta.pre_apply();
            commit_multi_var!(self.meta_store_ref(), version, compaction_groups_txn)?;
        }
        // Instead of handling DeltaType::GroupConstruct for time travel, simply enforce a version snapshot.
        versioning.mark_next_time_travel_version_snapshot();

        // The expired compact tasks will be canceled.
        // Failure of cancel does not cause correctness problems, the report task will have better interception, and the operation here is designed to free up compactor compute resources more quickly.
        let mut canceled_tasks = vec![];
        let compact_task_assignments =
            compaction_guard.get_compact_task_assignments_by_group_id(parent_group_id);
        let levels = versioning
            .current_version
            .get_compaction_group_levels(parent_group_id);
        compact_task_assignments
            .into_iter()
            .for_each(|task_assignment| {
                let task = &task_assignment.compact_task;
                let is_expired = is_compaction_task_expired(
                    task.compaction_group_version_id,
                    levels.compaction_group_version_id,
                );
                if is_expired {
                    canceled_tasks.push(ReportTask {
                        task_id: task.task_id,
                        task_status: TaskStatus::ManualCanceled,
                        table_stats_change: HashMap::default(),
                        sorted_output_ssts: vec![],
                        object_timestamps: HashMap::default(),
                    });
                }
            });

        if !canceled_tasks.is_empty() {
            self.report_compact_tasks_impl(canceled_tasks, compaction_guard, versioning_guard)
                .await?;
        } else {
            drop(versioning_guard);
            drop(compaction_guard);
        }

        let affected_group_ids = result.iter().map(|(cg_id, _)| *cg_id).collect_vec();
        self.try_update_write_limits(&affected_group_ids).await;

        self.metrics
            .split_compaction_group_count
            .with_label_values(&[&parent_group_id.to_string()])
            .inc();

        Ok(result)
    }

    /// Split `table_ids` to a dedicated compaction group.
    /// Returns the compaction group id containing the `table_ids` and the mapping of compaction group id to table ids.
    pub async fn move_state_tables_to_dedicated_compaction_group(
        &self,
        parent_group_id: CompactionGroupId,
        table_ids: &[StateTableId],
        partition_vnode_count: Option<u32>,
    ) -> Result<(
        CompactionGroupId,
        BTreeMap<CompactionGroupId, Vec<StateTableId>>,
    )> {
        if table_ids.is_empty() {
            return Err(Error::CompactionGroup(
                "table_ids must not be empty".to_owned(),
            ));
        }

        if !table_ids.is_sorted() {
            return Err(Error::CompactionGroup(
                "table_ids must be sorted".to_owned(),
            ));
        }

        let parent_table_ids = {
            let versioning_guard = self
                .versioning
                .read_with_process_name("move_state_tables_to_dedicated_compaction_group")
                .await;
            versioning_guard
                .current_version
                .state_table_info
                .compaction_group_member_table_ids(parent_group_id)
                .iter()
                .copied()
                .collect_vec()
        };

        if parent_table_ids == table_ids {
            return Err(Error::CompactionGroup(format!(
                "invalid split attempt for group {}: all member tables are moved",
                parent_group_id
            )));
        }

        fn check_table_ids_valid(cg_id_to_table_ids: &BTreeMap<CompactionGroupId, Vec<TableId>>) {
            // 1. table_ids in different cg are sorted.
            {
                cg_id_to_table_ids
                    .iter()
                    .for_each(|(_cg_id, table_ids)| assert!(table_ids.is_sorted()));
            }

            // 2.table_ids in different cg are non-overlapping
            {
                let mut table_table_ids_vec = cg_id_to_table_ids.values().cloned().collect_vec();
                table_table_ids_vec.sort_by(|a, b| a[0].cmp(&b[0]));
                assert!(table_table_ids_vec.concat().is_sorted());
            }

            // 3.table_ids belong to one and only one cg.
            {
                let mut all_table_ids = HashSet::new();
                for table_ids in cg_id_to_table_ids.values() {
                    for table_id in table_ids {
                        assert!(all_table_ids.insert(*table_id));
                    }
                }
            }
        }

        // move [3,4,5,6]
        // [1,2,3,4,5,6,7,8,9,10] -> [1,2] [3,4,5,6] [7,8,9,10]
        // split key
        // 1. table_id = 3, vnode = 0, epoch = MAX
        // 2. table_id = 7, vnode = 0, epoch = MAX

        // The new compaction group id is always generate on the right side
        // Hence, we return the first compaction group id as the result
        // split 1
        let mut cg_id_to_table_ids: BTreeMap<CompactionGroupId, Vec<TableId>> = BTreeMap::new();
        let table_id_to_split = *table_ids.first().unwrap();
        let mut target_compaction_group_id: CompactionGroupId = 0.into();
        let result_vec = self
            .split_compaction_group_impl(
                parent_group_id,
                table_ids,
                table_id_to_split,
                VirtualNode::ZERO,
                partition_vnode_count,
            )
            .await?;
        assert!(result_vec.len() <= 2);

        let mut finish_move = false;
        for (cg_id, table_ids_after_split) in result_vec {
            if table_ids_after_split.contains(&table_id_to_split) {
                target_compaction_group_id = cg_id;
            }

            if table_ids_after_split == table_ids {
                finish_move = true;
            }

            cg_id_to_table_ids.insert(cg_id, table_ids_after_split);
        }
        check_table_ids_valid(&cg_id_to_table_ids);

        if finish_move {
            return Ok((target_compaction_group_id, cg_id_to_table_ids));
        }

        // split 2
        // See the example above and the split rule in `split_compaction_group_impl`.
        let table_id_to_split = *table_ids.last().unwrap();
        let result_vec = self
            .split_compaction_group_impl(
                target_compaction_group_id,
                table_ids,
                table_id_to_split,
                VirtualNode::MAX_REPRESENTABLE,
                partition_vnode_count,
            )
            .await?;
        assert!(result_vec.len() <= 2);
        for (cg_id, table_ids_after_split) in result_vec {
            if table_ids_after_split.contains(&table_id_to_split) {
                target_compaction_group_id = cg_id;
            }
            cg_id_to_table_ids.insert(cg_id, table_ids_after_split);
        }
        check_table_ids_valid(&cg_id_to_table_ids);

        Ok((target_compaction_group_id, cg_id_to_table_ids))
    }
}

impl HummockManager {
    async fn build_normalize_plan(&self) -> Option<NormalizePlan> {
        let groups = self.calculate_compaction_group_statistic().await;
        build_normalize_plan_from_group_statistics(&groups)
    }

    async fn apply_normalize_plan(&self, plan: &NormalizePlan) -> Result<bool> {
        let (table_ids_right, boundary_table_id, new_compaction_group_id) = {
            let mut versioning_guard = self
                .versioning
                .write_with_process_name("apply_normalize_plan")
                .await;
            let versioning = versioning_guard.deref_mut();
            let mut compaction_group_manager = self
                .compaction_group_manager
                .write_with_process_name("apply_normalize_plan")
                .await;

            let groups = collect_normalize_group_statistics(
                &versioning.current_version,
                &compaction_group_manager,
            )?;
            let Some(current_plan) = build_normalize_plan_from_group_statistics(&groups) else {
                return Ok(false);
            };

            if &current_plan != plan {
                return Ok(false);
            }

            let (_table_ids_left, table_ids_right) = plan.split_table_ids();

            let config = compaction_group_manager
                .try_get_compaction_group_config(plan.parent_group_id)
                .ok_or_else(|| {
                    Error::CompactionGroup(format!(
                        "parent group {} config not found",
                        plan.parent_group_id
                    ))
                })?
                .compaction_config()
                .as_ref()
                .clone();

            let mut compaction_groups_txn = compaction_group_manager.start_compaction_groups_txn();
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
            let split_key = plan.split_key();
            let split_sst_count = new_version_delta
                .latest_version()
                .count_new_ssts_in_group_split(plan.parent_group_id, split_key.clone());
            let new_sst_start_id = next_sstable_id(&self.env, split_sst_count).await?;
            let new_compaction_group_id = next_compaction_group_id(&self.env).await?;

            #[expect(deprecated)]
            new_version_delta.group_deltas.insert(
                new_compaction_group_id,
                GroupDeltas {
                    group_deltas: vec![GroupDelta::GroupConstruct(Box::new(PbGroupConstruct {
                        group_config: Some(config.clone()),
                        group_id: new_compaction_group_id,
                        parent_group_id: plan.parent_group_id,
                        new_sst_start_id,
                        table_ids: vec![],
                        version: CompatibilityVersion::LATEST as _,
                        split_key: Some(split_key.into()),
                    }))],
                },
            );

            new_version_delta.with_latest_version(|version, new_version_delta| {
                for &table_id in &table_ids_right {
                    let info = version
                        .state_table_info
                        .info()
                        .get(&table_id)
                        .expect("table should exist before normalize split");
                    assert!(
                        new_version_delta
                            .state_table_info_delta
                            .insert(
                                table_id,
                                PbStateTableInfoDelta {
                                    committed_epoch: info.committed_epoch,
                                    compaction_group_id: new_compaction_group_id,
                                }
                            )
                            .is_none()
                    );
                }
            });
            new_version_delta.pre_apply();
            compaction_groups_txn
                .create_compaction_groups(new_compaction_group_id, Arc::new(config));

            commit_multi_var!(self.meta_store_ref(), version, compaction_groups_txn)?;
            versioning.mark_next_time_travel_version_snapshot();

            (
                table_ids_right,
                plan.boundary_table_id,
                new_compaction_group_id,
            )
        };

        self.cancel_expired_normalize_split_tasks(plan.parent_group_id)
            .await?;
        self.try_update_write_limits(&[plan.parent_group_id, new_compaction_group_id])
            .await;
        self.metrics
            .split_compaction_group_count
            .with_label_values(&[&plan.parent_group_id.to_string()])
            .inc();
        tracing::info!(
            "normalize split success: parent_group={} boundary_table_id={} moved_tables={:?} new_group_id={}",
            plan.parent_group_id,
            boundary_table_id,
            table_ids_right,
            new_compaction_group_id
        );

        Ok(true)
    }

    async fn cancel_expired_normalize_split_tasks(
        &self,
        parent_group_id: CompactionGroupId,
    ) -> Result<()> {
        let mut canceled_tasks = vec![];
        let compaction_guard = self
            .compaction
            .write_with_process_name("cancel_expired_normalize_split_tasks")
            .await;
        let mut versioning_guard = self
            .versioning
            .write_with_process_name("cancel_expired_normalize_split_tasks")
            .await;
        let versioning = versioning_guard.deref_mut();
        let compact_task_assignments =
            compaction_guard.get_compact_task_assignments_by_group_id(parent_group_id);
        let levels = versioning
            .current_version
            .get_compaction_group_levels(parent_group_id);
        compact_task_assignments
            .into_iter()
            .for_each(|task_assignment| {
                let task = &task_assignment.compact_task;
                if is_compaction_task_expired(
                    task.compaction_group_version_id,
                    levels.compaction_group_version_id,
                ) {
                    canceled_tasks.push(ReportTask {
                        task_id: task.task_id,
                        task_status: TaskStatus::ManualCanceled,
                        table_stats_change: HashMap::default(),
                        sorted_output_ssts: vec![],
                        object_timestamps: HashMap::default(),
                    });
                }
            });
        canceled_tasks.sort_by_key(|task| task.task_id);
        canceled_tasks.dedup_by_key(|task| task.task_id);

        if !canceled_tasks.is_empty() {
            self.report_compact_tasks_impl(canceled_tasks, compaction_guard, versioning_guard)
                .await?;
        }

        Ok(())
    }

    /// Normalize overlapping adjacent compaction groups by split only.
    ///
    /// The algorithm repeatedly scans adjacent groups by `min(table_id)` and if
    /// `max(left) >= min(right)`, it splits `left` at the first table id `>= min(right)`.
    /// Each step is planned from a read snapshot, then revalidated and applied with a short write
    /// transaction.
    pub async fn normalize_overlapping_compaction_groups(&self) -> Result<usize> {
        self.normalize_overlapping_compaction_groups_with_limit(usize::MAX)
            .await
    }

    pub async fn normalize_overlapping_compaction_groups_with_limit(
        &self,
        max_splits: usize,
    ) -> Result<usize> {
        let mut split_count = 0usize;
        while split_count < max_splits {
            let Some(plan) = self.build_normalize_plan().await else {
                break;
            };

            if !self.apply_normalize_plan(&plan).await? {
                tracing::debug!(
                    parent_group_id = %plan.parent_group_id,
                    boundary_table_id = %plan.boundary_table_id,
                    "normalize plan became stale before apply"
                );
                break;
            }
            split_count += 1;
        }

        Ok(split_count)
    }

    /// Split the compaction group if the group is too large or contains high throughput tables.
    pub async fn try_split_compaction_group(
        &self,
        table_write_throughput_statistic_manager: &TableWriteThroughputStatisticManager,
        group: CompactionGroupStatistic,
    ) {
        if group
            .compaction_group_config
            .compaction_config
            .disable_auto_group_scheduling
            .unwrap_or(false)
        {
            return;
        }
        // split high throughput table to dedicated compaction group
        for (table_id, table_size) in &group.table_statistic {
            self.try_move_high_throughput_table_to_dedicated_cg(
                table_write_throughput_statistic_manager,
                *table_id,
                table_size,
                group.group_id,
            )
            .await;
        }

        // split the huge group to multiple groups
        self.try_split_huge_compaction_group(group).await;
    }

    /// Try to move the high throughput table to a dedicated compaction group.
    pub async fn try_move_high_throughput_table_to_dedicated_cg(
        &self,
        table_write_throughput_statistic_manager: &TableWriteThroughputStatisticManager,
        table_id: TableId,
        _table_size: &u64,
        parent_group_id: CompactionGroupId,
    ) {
        let mut table_throughput = table_write_throughput_statistic_manager
            .get_table_throughput_descending(
                table_id,
                self.env.opts.table_stat_throuput_window_seconds_for_split as i64,
            )
            .peekable();

        if table_throughput.peek().is_none() {
            return;
        }

        let is_high_write_throughput = GroupMergeValidator::is_table_high_write_throughput(
            table_throughput,
            self.env.opts.table_high_write_throughput_threshold,
            self.env
                .opts
                .table_stat_high_write_throughput_ratio_for_split,
        );

        // do not split a table to dedicated compaction group if it is not high write throughput
        if !is_high_write_throughput {
            return;
        }

        let ret = self
            .move_state_tables_to_dedicated_compaction_group(
                parent_group_id,
                &[table_id],
                Some(self.env.opts.partition_vnode_count),
            )
            .await;
        match ret {
            Ok(split_result) => {
                tracing::info!(
                    "split state table [{}] from group-{} success table_vnode_partition_count {:?} split result {:?}",
                    table_id,
                    parent_group_id,
                    self.env.opts.partition_vnode_count,
                    split_result
                );
            }
            Err(e) => {
                tracing::info!(
                    error = %e.as_report(),
                    "failed to split state table [{}] from group-{}",
                    table_id,
                    parent_group_id,
                )
            }
        }
    }

    pub async fn try_split_huge_compaction_group(&self, group: CompactionGroupStatistic) {
        let group_max_size = (group.compaction_group_config.max_estimated_group_size() as f64
            * self.env.opts.split_group_size_ratio) as u64;
        let is_huge_hybrid_group =
            group.group_size > group_max_size && group.table_statistic.len() > 1; // avoid split single table group
        if is_huge_hybrid_group {
            let mut accumulated_size = 0;
            let mut table_ids = Vec::default();
            for (table_id, table_size) in &group.table_statistic {
                accumulated_size += table_size;
                table_ids.push(*table_id);
                // split if the accumulated size is greater than half of the group size
                // avoid split a small table to dedicated compaction group and trigger multiple merge
                assert!(table_ids.is_sorted());
                let remaining_size = group.group_size.saturating_sub(accumulated_size);
                if accumulated_size > group_max_size / 2
                    && remaining_size > 0
                    && table_ids.len() < group.table_statistic.len()
                {
                    let ret = self
                        .move_state_tables_to_dedicated_compaction_group(
                            group.group_id,
                            &table_ids,
                            None,
                        )
                        .await;
                    match ret {
                        Ok(split_result) => {
                            tracing::info!(
                                "split_huge_compaction_group success {:?}",
                                split_result
                            );
                            self.metrics
                                .split_compaction_group_count
                                .with_label_values(&[&group.group_id.to_string()])
                                .inc();
                            return;
                        }
                        Err(e) => {
                            tracing::error!(
                                error = %e.as_report(),
                                "failed to split_huge_compaction_group table {:?} from group-{}",
                                table_ids,
                                group.group_id
                            );

                            return;
                        }
                    }
                }
            }
        }
    }

    pub async fn try_merge_compaction_group(
        &self,
        table_write_throughput_statistic_manager: &TableWriteThroughputStatisticManager,
        group: &CompactionGroupStatistic,
        next_group: &CompactionGroupStatistic,
        created_tables: &HashSet<TableId>,
    ) -> Result<()> {
        GroupMergeValidator::validate_group_merge(
            group,
            next_group,
            created_tables,
            table_write_throughput_statistic_manager,
            &self.env.opts,
            &self.versioning,
        )
        .await?;

        let result = self
            .merge_compaction_group(group.group_id, next_group.group_id)
            .await;

        match &result {
            Ok(()) => {
                tracing::info!(
                    "merge group-{} to group-{}",
                    next_group.group_id,
                    group.group_id,
                );

                self.metrics
                    .merge_compaction_group_count
                    .with_label_values(&[&group.group_id.to_string()])
                    .inc();
            }
            Err(e) => {
                tracing::info!(
                    error = %e.as_report(),
                    "failed to merge group-{} group-{}",
                    next_group.group_id,
                    group.group_id,
                );
            }
        }

        result
    }
}

#[derive(Debug, Default)]
struct GroupMergeValidator {}

impl GroupMergeValidator {
    /// Check if two groups have compatible compaction configs for merging.
    /// Ignores `split_weight_by_vnode` since it's per-table and will be reset after merge.
    fn is_merge_compatible_by_semantics(
        group: &CompactionGroupStatistic,
        next_group: &CompactionGroupStatistic,
    ) -> bool {
        let (mut left, mut right) = (
            group
                .compaction_group_config
                .compaction_config
                .as_ref()
                .clone(),
            next_group
                .compaction_group_config
                .compaction_config
                .as_ref()
                .clone(),
        );
        left.split_weight_by_vnode = 0;
        right.split_weight_by_vnode = 0;
        left == right
    }

    /// Check if the table is high write throughput with the given threshold and ratio.
    pub fn is_table_high_write_throughput(
        table_throughput: impl Iterator<Item = &TableWriteThroughputStatistic>,
        threshold: u64,
        high_write_throughput_ratio: f64,
    ) -> bool {
        let mut sample_size = 0;
        let mut high_write_throughput_count = 0;
        for statistic in table_throughput {
            sample_size += 1;
            if statistic.throughput > threshold {
                high_write_throughput_count += 1;
            }
        }

        high_write_throughput_count as f64 > sample_size as f64 * high_write_throughput_ratio
    }

    pub fn is_table_low_write_throughput(
        table_throughput: impl Iterator<Item = &TableWriteThroughputStatistic>,
        threshold: u64,
        low_write_throughput_ratio: f64,
    ) -> bool {
        let mut sample_size = 0;
        let mut low_write_throughput_count = 0;
        for statistic in table_throughput {
            sample_size += 1;
            if statistic.throughput <= threshold {
                low_write_throughput_count += 1;
            }
        }

        low_write_throughput_count as f64 > sample_size as f64 * low_write_throughput_ratio
    }

    fn check_is_low_write_throughput_compaction_group(
        table_write_throughput_statistic_manager: &TableWriteThroughputStatisticManager,
        group: &CompactionGroupStatistic,
        opts: &Arc<MetaOpts>,
    ) -> bool {
        let mut table_with_statistic = Vec::with_capacity(group.table_statistic.len());
        for table_id in group.table_statistic.keys() {
            let mut table_throughput = table_write_throughput_statistic_manager
                .get_table_throughput_descending(
                    *table_id,
                    opts.table_stat_throuput_window_seconds_for_merge as i64,
                )
                .peekable();
            if table_throughput.peek().is_none() {
                continue;
            }

            table_with_statistic.push(table_throughput);
        }

        // if all tables in the group do not have enough statistics, return true
        if table_with_statistic.is_empty() {
            return true;
        }

        // check if all tables in the group are low write throughput with enough statistics
        table_with_statistic.into_iter().all(|table_throughput| {
            Self::is_table_low_write_throughput(
                table_throughput,
                opts.table_low_write_throughput_threshold,
                opts.table_stat_low_write_throughput_ratio_for_merge,
            )
        })
    }

    fn check_is_creating_compaction_group(
        group: &CompactionGroupStatistic,
        created_tables: &HashSet<TableId>,
    ) -> bool {
        group
            .table_statistic
            .keys()
            .any(|table_id| !created_tables.contains(table_id))
    }

    async fn validate_group_merge(
        group: &CompactionGroupStatistic,
        next_group: &CompactionGroupStatistic,
        created_tables: &HashSet<TableId>,
        table_write_throughput_statistic_manager: &TableWriteThroughputStatisticManager,
        opts: &Arc<MetaOpts>,
        versioning: &MonitoredRwLock<Versioning>,
    ) -> Result<()> {
        // TODO: remove this check after refactor group id
        if (group.group_id == StaticCompactionGroupId::StateDefault
            && next_group.group_id == StaticCompactionGroupId::MaterializedView)
            || (group.group_id == StaticCompactionGroupId::MaterializedView
                && next_group.group_id == StaticCompactionGroupId::StateDefault)
        {
            return Err(Error::CompactionGroup(format!(
                "group-{} and group-{} are both StaticCompactionGroupId",
                group.group_id, next_group.group_id
            )));
        }

        if group.table_statistic.is_empty() || next_group.table_statistic.is_empty() {
            return Err(Error::CompactionGroup(format!(
                "group-{} or group-{} is empty",
                group.group_id, next_group.group_id
            )));
        }

        // Check non-overlapping table ids early to avoid acquiring heavyweight write locks
        // in merge_compaction_group_impl only to fail at the overlap check.
        // Sort both sides and ensure table_ids_1 has the smaller first element,
        // then reject if table_ids_1's last element >= table_ids_2's first element (overlap).
        {
            let mut table_ids_1: Vec<TableId> = group.table_statistic.keys().cloned().collect_vec();
            let mut table_ids_2: Vec<TableId> =
                next_group.table_statistic.keys().cloned().collect_vec();
            table_ids_1.sort();
            table_ids_2.sort();
            if table_ids_1.first().unwrap() > table_ids_2.first().unwrap() {
                std::mem::swap(&mut table_ids_1, &mut table_ids_2);
            }
            if table_ids_1.last().unwrap() >= table_ids_2.first().unwrap() {
                return Err(Error::CompactionGroup(format!(
                    "group-{} and group-{} have overlapping table id ranges, not mergeable",
                    group.group_id, next_group.group_id
                )));
            }
        }

        if group
            .compaction_group_config
            .compaction_config
            .disable_auto_group_scheduling
            .unwrap_or(false)
            || next_group
                .compaction_group_config
                .compaction_config
                .disable_auto_group_scheduling
                .unwrap_or(false)
        {
            return Err(Error::CompactionGroup(format!(
                "group-{} or group-{} disable_auto_group_scheduling",
                group.group_id, next_group.group_id
            )));
        }

        // Keep merge compatibility as a feature, but ignore split_weight_by_vnode, because it is
        // only used for per-table split behavior and will be reset after merge.
        if !Self::is_merge_compatible_by_semantics(group, next_group) {
            let left_config = group.compaction_group_config.compaction_config.as_ref();
            let right_config = next_group
                .compaction_group_config
                .compaction_config
                .as_ref();

            tracing::warn!(
                group_id = %group.group_id,
                next_group_id = %next_group.group_id,
                left_config = ?left_config,
                right_config = ?right_config,
                "compaction config semantic mismatch detected while merging compaction groups"
            );

            return Err(Error::CompactionGroup(format!(
                "Cannot merge group {} and next_group {} with different compaction config (split_weight_by_vnode is excluded from comparison). left_config: {:?}, right_config: {:?}",
                group.group_id, next_group.group_id, left_config, right_config
            )));
        }

        // do not merge the compaction group which is creating
        if Self::check_is_creating_compaction_group(group, created_tables) {
            return Err(Error::CompactionGroup(format!(
                "Cannot merge creating group {} next_group {}",
                group.group_id, next_group.group_id
            )));
        }

        // do not merge high throughput group
        if !Self::check_is_low_write_throughput_compaction_group(
            table_write_throughput_statistic_manager,
            group,
            opts,
        ) {
            return Err(Error::CompactionGroup(format!(
                "Cannot merge high throughput group {} next_group {}",
                group.group_id, next_group.group_id
            )));
        }

        let size_limit = (group.compaction_group_config.max_estimated_group_size() as f64
            * opts.split_group_size_ratio) as u64;

        if (group.group_size + next_group.group_size) > size_limit {
            return Err(Error::CompactionGroup(format!(
                "Cannot merge huge group {} group_size {} next_group {} next_group_size {} size_limit {}",
                group.group_id,
                group.group_size,
                next_group.group_id,
                next_group.group_size,
                size_limit
            )));
        }

        if Self::check_is_creating_compaction_group(next_group, created_tables) {
            return Err(Error::CompactionGroup(format!(
                "Cannot merge creating group {} next group {}",
                group.group_id, next_group.group_id
            )));
        }

        if !Self::check_is_low_write_throughput_compaction_group(
            table_write_throughput_statistic_manager,
            next_group,
            opts,
        ) {
            return Err(Error::CompactionGroup(format!(
                "Cannot merge high throughput group {} next group {}",
                group.group_id, next_group.group_id
            )));
        }

        {
            // Avoid merge when the group is in emergency state
            let versioning_guard = versioning
                .read_with_process_name("validate_group_merge")
                .await;
            let levels = &versioning_guard.current_version.levels;
            if !levels.contains_key(&group.group_id) {
                return Err(Error::CompactionGroup(format!(
                    "cannot merge compaction group {} because it does not exist",
                    group.group_id
                )));
            }

            if !levels.contains_key(&next_group.group_id) {
                return Err(Error::CompactionGroup(format!(
                    "cannot merge next compaction group {} because it does not exist",
                    next_group.group_id
                )));
            }

            let group_levels = versioning_guard
                .current_version
                .get_compaction_group_levels(group.group_id);

            let next_group_levels = versioning_guard
                .current_version
                .get_compaction_group_levels(next_group.group_id);

            let group_state = GroupStateValidator::group_state(
                group_levels,
                group.compaction_group_config.compaction_config().deref(),
            );

            if group_state.is_write_stop() || group_state.is_emergency() {
                return Err(Error::CompactionGroup(format!(
                    "Cannot merge write limit group {} next group {}",
                    group.group_id, next_group.group_id
                )));
            }

            let next_group_state = GroupStateValidator::group_state(
                next_group_levels,
                next_group
                    .compaction_group_config
                    .compaction_config()
                    .deref(),
            );

            if next_group_state.is_write_stop() || next_group_state.is_emergency() {
                return Err(Error::CompactionGroup(format!(
                    "Cannot merge write limit next group {} group {}",
                    next_group.group_id, group.group_id
                )));
            }

            // check whether the group is in the write stop state after merge
            let l0_sub_level_count_after_merge =
                group_levels.l0.sub_levels.len() + next_group_levels.l0.sub_levels.len();
            if GroupStateValidator::write_stop_sub_level_count(
                (l0_sub_level_count_after_merge as f64
                    * opts.compaction_group_merge_dimension_threshold) as usize,
                group.compaction_group_config.compaction_config().deref(),
            ) {
                return Err(Error::CompactionGroup(format!(
                    "Cannot merge write limit group {} next group {}, will trigger write stop after merge",
                    group.group_id, next_group.group_id
                )));
            }

            let l0_file_count_after_merge = group_levels
                .l0
                .sub_levels
                .iter()
                .chain(next_group_levels.l0.sub_levels.iter())
                .map(|level| level.table_infos.len())
                .sum::<usize>();
            if GroupStateValidator::write_stop_l0_file_count(
                (l0_file_count_after_merge as f64 * opts.compaction_group_merge_dimension_threshold)
                    as usize,
                group.compaction_group_config.compaction_config().deref(),
            ) {
                return Err(Error::CompactionGroup(format!(
                    "Cannot merge write limit next group {} group {}, will trigger write stop after merge",
                    next_group.group_id, group.group_id
                )));
            }

            let l0_size_after_merge =
                group_levels.l0.total_file_size + next_group_levels.l0.total_file_size;

            if GroupStateValidator::write_stop_l0_size(
                (l0_size_after_merge as f64 * opts.compaction_group_merge_dimension_threshold)
                    as u64,
                group.compaction_group_config.compaction_config().deref(),
            ) {
                return Err(Error::CompactionGroup(format!(
                    "Cannot merge write limit next group {} group {}, will trigger write stop after merge",
                    next_group.group_id, group.group_id
                )));
            }

            // check whether the group is in the emergency state after merge
            if GroupStateValidator::emergency_l0_file_count(
                (l0_file_count_after_merge as f64 * opts.compaction_group_merge_dimension_threshold)
                    as usize,
                group.compaction_group_config.compaction_config().deref(),
            ) {
                return Err(Error::CompactionGroup(format!(
                    "Cannot merge emergency group {} next group {}, will trigger emergency after merge",
                    group.group_id, next_group.group_id
                )));
            }
        }

        Ok(())
    }
}
