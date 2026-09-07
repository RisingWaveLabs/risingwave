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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::TableId;
use risingwave_common::config::meta::default::compaction_config;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_hummock_sdk::change_log::EpochNewChangeLog;
use risingwave_hummock_sdk::compaction_group::group_split::split_sst_with_table_ids;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::{
    PbTableStatsMap, add_prost_table_stats_map, purge_prost_table_stats, to_prost_table_stats_map,
};
use risingwave_hummock_sdk::table_watermark::TableWatermarks;
use risingwave_hummock_sdk::vector_index::VectorIndexDelta;
use risingwave_hummock_sdk::version::HummockVersionStateTableInfo;
use risingwave_hummock_sdk::{
    CompactionGroupId, HummockContextId, HummockSstableObjectId, LocalSstableInfo,
};
use risingwave_pb::hummock::{CompactionConfig, compact_task};
use sea_orm::TransactionTrait;

use crate::hummock::error::{Error, Result};
use crate::hummock::manager::compaction_group_manager::CompactionGroupManager;
use crate::hummock::manager::transaction::{
    HummockVersionStatsTransaction, HummockVersionTransaction,
};
use crate::hummock::manager::versioning::Versioning;
use crate::hummock::metrics_utils::{
    get_or_create_local_table_stat, trigger_epoch_stat, trigger_local_table_stat, trigger_sst_stat,
};
use crate::hummock::model::CompactionGroup;
use crate::hummock::sequence::{next_compaction_group_id, next_sstable_id};
use crate::hummock::time_travel::should_mark_next_time_travel_version_snapshot;
use crate::hummock::{HummockManager, commit_multi_var_with_provided_txn};

pub struct NewTableFragmentInfo {
    pub table_ids: HashSet<TableId>,
}

#[derive(Default)]
pub struct CommitEpochInfo {
    pub sstables: Vec<LocalSstableInfo>,
    pub new_table_watermarks: HashMap<TableId, TableWatermarks>,
    pub sst_to_context: HashMap<HummockSstableObjectId, HummockContextId>,
    pub new_table_fragment_infos: Vec<NewTableFragmentInfo>,
    pub change_log_delta: HashMap<TableId, EpochNewChangeLog>,
    pub vector_index_delta: HashMap<TableId, VectorIndexDelta>,
    /// `table_id` -> `committed_epoch`
    pub tables_to_commit: HashMap<TableId, u64>,

    pub truncate_tables: HashSet<TableId>,
}

impl HummockManager {
    /// Caller should ensure `epoch` > `committed_epoch` of `tables_to_commit`
    /// if tables are not newly added via `new_table_fragment_info`
    pub async fn commit_epoch(&self, commit_info: CommitEpochInfo) -> Result<()> {
        let CommitEpochInfo {
            mut sstables,
            new_table_watermarks,
            sst_to_context,
            new_table_fragment_infos,
            change_log_delta,
            vector_index_delta,
            tables_to_commit,
            truncate_tables,
        } = commit_info;
        let mut versioning_guard = self
            .versioning
            .write_with_process_name("commit_epoch")
            .await;
        // Prevent commit new epochs if this flag is set
        if versioning_guard.disable_commit_epochs {
            return Ok(());
        }

        assert!(!tables_to_commit.is_empty());

        let versioning: &mut Versioning = &mut versioning_guard;
        self.commit_epoch_sanity_check(
            &tables_to_commit,
            &sstables,
            &sst_to_context,
            &versioning.current_version,
        )
        .await?;

        // Consume and aggregate table stats.
        let mut table_stats_change = PbTableStatsMap::default();
        for s in &mut sstables {
            add_prost_table_stats_map(
                &mut table_stats_change,
                &to_prost_table_stats_map(s.table_stats.clone()),
            );
        }

        let table_change_log_object_ids_before_commit = versioning
            .table_change_log
            .values()
            .flat_map(|l| l.get_object_ids())
            .collect::<HashSet<_>>();

        let mut version = HummockVersionTransaction::new(
            &mut versioning.current_version,
            &mut versioning.hummock_version_deltas,
            &mut versioning.table_change_log,
            self.env.notification_manager(),
            Some(&self.table_committed_epoch_notifiers),
            &self.metrics,
            &self.env.opts,
            &self.version_stat_tx,
        );

        let state_table_info = &version.latest_version().state_table_info;
        let mut table_compaction_group_mapping = state_table_info.build_table_compaction_group_id();
        let mut new_table_ids = HashMap::new();
        let mut new_compaction_groups = Vec::new();
        let mut compaction_group_manager_txn = None;
        let mut compaction_group_config: Option<Arc<CompactionConfig>> = None;

        // Add new table
        for NewTableFragmentInfo { table_ids } in new_table_fragment_infos {
            let (compaction_group_manager, compaction_group_config) =
                if let Some(compaction_group_manager) = &mut compaction_group_manager_txn {
                    (
                        compaction_group_manager,
                        (*compaction_group_config
                            .as_ref()
                            .expect("must be set with compaction_group_manager_txn"))
                        .clone(),
                    )
                } else {
                    let compaction_group_manager_guard = self
                        .compaction_group_manager
                        .write_with_process_name("commit_epoch")
                        .await;
                    let new_compaction_group_config =
                        compaction_group_manager_guard.default_compaction_config();
                    compaction_group_config = Some(new_compaction_group_config.clone());
                    (
                        compaction_group_manager_txn.insert(
                            CompactionGroupManager::start_owned_compaction_groups_txn(
                                compaction_group_manager_guard,
                            ),
                        ),
                        new_compaction_group_config,
                    )
                };
            let new_compaction_group_id = next_compaction_group_id(&self.env).await?;
            let new_compaction_group = CompactionGroup {
                group_id: new_compaction_group_id,
                compaction_config: compaction_group_config.clone(),
            };

            new_compaction_groups.push(new_compaction_group.clone());
            compaction_group_manager.insert(new_compaction_group_id, new_compaction_group);

            on_handle_add_new_table(
                state_table_info,
                &table_ids,
                new_compaction_group_id,
                &mut table_compaction_group_mapping,
                &mut new_table_ids,
            )?;
        }

        let commit_sstables = self
            .correct_commit_ssts(sstables, &table_compaction_group_mapping)
            .await?;

        let modified_compaction_groups = commit_sstables.keys().cloned().collect_vec();
        // fill compaction_groups
        let mut group_id_to_config = HashMap::new();
        if let Some(compaction_group_manager) = compaction_group_manager_txn.as_ref() {
            for cg_id in &modified_compaction_groups {
                let compaction_group = compaction_group_manager
                    .get(cg_id)
                    .unwrap_or_else(|| panic!("compaction group {} should be created", cg_id))
                    .compaction_config();
                group_id_to_config.insert(*cg_id, compaction_group);
            }
        } else {
            let compaction_group_manager = self
                .compaction_group_manager
                .read_with_process_name("commit_epoch")
                .await;
            for cg_id in &modified_compaction_groups {
                let compaction_group = compaction_group_manager
                    .try_get_compaction_group_config(*cg_id)
                    .unwrap_or_else(|| panic!("compaction group {} should be created", cg_id))
                    .compaction_config();
                group_id_to_config.insert(*cg_id, compaction_group);
            }
        }

        let group_id_to_sub_levels =
            rewrite_commit_sstables_to_sub_level(commit_sstables, &group_id_to_config);

        // build group_id to truncate tables
        let mut group_id_to_truncate_tables: HashMap<CompactionGroupId, HashSet<TableId>> =
            HashMap::new();
        for table_id in &truncate_tables {
            if let Some(compaction_group_id) = table_compaction_group_mapping.get(table_id) {
                group_id_to_truncate_tables
                    .entry(*compaction_group_id)
                    .or_default()
                    .insert(*table_id);
            } else {
                bail!(
                    "table {} doesn't belong to any compaction group, skip truncating",
                    table_id
                );
            }
        }

        let time_travel_delta = version.pre_commit_epoch(
            &tables_to_commit,
            new_compaction_groups,
            group_id_to_sub_levels,
            &new_table_ids,
            new_table_watermarks,
            change_log_delta,
            vector_index_delta,
            group_id_to_truncate_tables,
        );

        if should_mark_next_time_travel_version_snapshot(&time_travel_delta) {
            // Unable to invoke mark_next_time_travel_version_snapshot because versioning is already mutable borrowed.
            versioning.time_travel_snapshot_interval_counter = u64::MAX;
        }

        // Apply stats changes.
        let mut version_stats = HummockVersionStatsTransaction::new(
            &mut versioning.version_stats,
            self.env.notification_manager(),
        );
        add_prost_table_stats_map(&mut version_stats.table_stats, &table_stats_change);
        if purge_prost_table_stats(
            &mut version_stats.table_stats,
            version.latest_version(),
            &truncate_tables,
        ) {
            self.metrics.version_stats.reset();
            versioning.local_metrics.clear();
        }

        trigger_local_table_stat(
            &self.metrics,
            &mut versioning.local_metrics,
            &version_stats,
            &table_stats_change,
        );
        for (table_id, stats) in &table_stats_change {
            if stats.total_key_size == 0
                && stats.total_value_size == 0
                && stats.total_key_count == 0
            {
                continue;
            }
            let stats_value = std::cmp::max(0, stats.total_key_size + stats.total_value_size);
            let table_metrics = get_or_create_local_table_stat(
                &self.metrics,
                *table_id,
                &mut versioning.local_metrics,
            );
            table_metrics.inc_write_throughput(stats_value as u64);
        }
        let mut time_travel_version = None;
        if versioning.time_travel_snapshot_interval_counter
            >= self.env.opts.hummock_time_travel_snapshot_interval
        {
            versioning.time_travel_snapshot_interval_counter = 0;
            time_travel_version = Some(version.latest_version());
        } else {
            versioning.time_travel_snapshot_interval_counter = versioning
                .time_travel_snapshot_interval_counter
                .saturating_add(1);
        }
        let time_travel_tables_to_commit =
            table_compaction_group_mapping
                .iter()
                .filter_map(|(table_id, cg_id)| {
                    tables_to_commit
                        .get(table_id)
                        .map(|committed_epoch| (table_id, cg_id, *committed_epoch))
                });
        let time_travel_table_ids: HashSet<_> = self
            .metadata_manager
            .catalog_controller
            .list_time_travel_table_ids()
            .await
            .map_err(|e| Error::Internal(e.into()))?
            .into_iter()
            .collect();
        let mut txn = self.env.meta_store_ref().conn.begin().await?;
        let version_snapshot_sst_ids = self
            .write_time_travel_metadata(
                &txn,
                time_travel_version,
                time_travel_delta,
                time_travel_table_ids,
                &versioning.last_time_travel_snapshot_sst_ids,
                time_travel_tables_to_commit,
            )
            .await?;
        commit_multi_var_with_provided_txn!(
            txn,
            version,
            version_stats,
            compaction_group_manager_txn
        )?;
        if let Some(version_snapshot_sst_ids) = version_snapshot_sst_ids {
            versioning.last_time_travel_snapshot_sst_ids = version_snapshot_sst_ids;
        }

        for compaction_group_id in &modified_compaction_groups {
            trigger_sst_stat(
                &self.metrics,
                None,
                &versioning.current_version,
                *compaction_group_id,
            );
        }
        trigger_epoch_stat(&self.metrics, &versioning.current_version);
        let table_change_log_object_ids_after_commit = versioning
            .table_change_log
            .values()
            .flat_map(|l| l.get_object_ids())
            .collect::<HashSet<_>>();
        drop(versioning_guard);
        let may_delete_object_ids =
            &table_change_log_object_ids_before_commit - &table_change_log_object_ids_after_commit;
        self.gc_manager
            .add_may_delete_object_ids(may_delete_object_ids.into_iter());

        // Don't trigger compactions if we enable deterministic compaction
        if !self.env.opts.compaction_deterministic_test {
            // commit_epoch may contains SSTs from any compaction group
            for id in &modified_compaction_groups {
                self.try_send_compaction_request(*id, compact_task::TaskType::Dynamic);
            }
            if !table_stats_change.is_empty() {
                self.collect_table_write_throughput(table_stats_change)
                    .await;
            }
        }
        if !modified_compaction_groups.is_empty() {
            self.try_update_write_limits(&modified_compaction_groups)
                .await;
        }
        #[cfg(test)]
        {
            self.check_state_consistency().await;
        }
        Ok(())
    }

    async fn collect_table_write_throughput(&self, table_stats: PbTableStatsMap) {
        let params = self.env.system_params_reader().await;
        let barrier_interval_ms = params.barrier_interval_ms() as u64;
        let checkpoint_secs = std::cmp::max(
            1,
            params.checkpoint_frequency() * barrier_interval_ms / 1000,
        );

        let mut table_throughput_statistic_manager =
            self.table_write_throughput_statistic_manager.write();
        let timestamp = chrono::Utc::now().timestamp();

        for (table_id, stat) in table_stats {
            let throughput = ((stat.total_value_size + stat.total_key_size) as f64
                / checkpoint_secs as f64) as u64;
            table_throughput_statistic_manager
                .add_table_throughput_with_ts(table_id, throughput, timestamp);
        }
    }

    async fn correct_commit_ssts(
        &self,
        sstables: Vec<LocalSstableInfo>,
        table_compaction_group_mapping: &HashMap<TableId, CompactionGroupId>,
    ) -> Result<BTreeMap<CompactionGroupId, Vec<SstableInfo>>> {
        let mut new_sst_id_number = 0;
        let mut sst_to_cg_vec = Vec::with_capacity(sstables.len());
        let commit_object_id_vec = sstables.iter().map(|s| s.sst_info.object_id).collect_vec();
        for commit_sst in sstables {
            let mut group_table_ids: BTreeMap<CompactionGroupId, Vec<TableId>> = BTreeMap::new();
            for table_id in &commit_sst.sst_info.table_ids {
                match table_compaction_group_mapping.get(table_id) {
                    Some(cg_id_from_meta) => {
                        group_table_ids
                            .entry(*cg_id_from_meta)
                            .or_default()
                            .push(*table_id);
                    }
                    None => {
                        tracing::warn!(
                            %table_id,
                            object_id = %commit_sst.sst_info.object_id,
                            "table doesn't belong to any compaction group",
                        );
                    }
                }
            }

            new_sst_id_number += group_table_ids.len() * 2; // `split_sst` will split the SST into two parts and consumer 2 SST IDs
            sst_to_cg_vec.push((commit_sst, group_table_ids));
        }

        // Generate new SST IDs for each compaction group
        // `next_sstable_id` will update the global SST ID and reserve the new SST IDs
        // So we need to get the new SST ID first and then split the SSTs
        let mut new_sst_id = next_sstable_id(&self.env, new_sst_id_number).await?;
        let mut commit_sstables: BTreeMap<CompactionGroupId, Vec<SstableInfo>> = BTreeMap::new();

        for (mut sst, group_table_ids) in sst_to_cg_vec {
            let len = group_table_ids.len();
            for (index, (group_id, match_ids)) in group_table_ids.into_iter().enumerate() {
                if sst.sst_info.table_ids == match_ids {
                    // The SST contains all the tables in the group should be last key
                    assert!(
                        index == len - 1,
                        "SST should be the last key in the group {} index {} len {}",
                        group_id,
                        index,
                        len
                    );
                    commit_sstables
                        .entry(group_id)
                        .or_default()
                        .push(sst.sst_info);
                    break;
                }

                let origin_sst_size = sst.sst_info.sst_size;
                let new_sst_size = match_ids
                    .iter()
                    .map(|id| {
                        let stat = sst.table_stats.get(id).unwrap();
                        stat.total_compressed_size
                    })
                    .sum();

                if new_sst_size == 0 {
                    tracing::warn!(
                        id = %sst.sst_info.sst_id,
                        object_id = %sst.sst_info.object_id,
                        match_ids = ?match_ids,
                        "Sstable doesn't contain any data for tables",
                    );
                }

                let old_sst_size = origin_sst_size.saturating_sub(new_sst_size);
                if old_sst_size == 0 {
                    tracing::warn!(
                        id = %sst.sst_info.sst_id,
                        object_id = %sst.sst_info.object_id,
                        match_ids = ?match_ids,
                        origin_sst_size = origin_sst_size,
                        new_sst_size = new_sst_size,
                        "Sstable doesn't contain any data for tables",
                    );
                }
                let (modified_sst_info, branch_sst) = split_sst_with_table_ids(
                    &sst.sst_info,
                    &mut new_sst_id,
                    old_sst_size,
                    new_sst_size,
                    match_ids,
                );
                sst.sst_info = modified_sst_info;

                commit_sstables
                    .entry(group_id)
                    .or_default()
                    .push(branch_sst);
            }
        }

        // order check
        for ssts in commit_sstables.values() {
            let object_ids = ssts.iter().map(|s| s.object_id).collect_vec();
            assert!(is_ordered_subset(&commit_object_id_vec, &object_ids));
        }

        Ok(commit_sstables)
    }
}

fn on_handle_add_new_table(
    state_table_info: &HummockVersionStateTableInfo,
    table_ids: impl IntoIterator<Item = &TableId>,
    compaction_group_id: CompactionGroupId,
    table_compaction_group_mapping: &mut HashMap<TableId, CompactionGroupId>,
    new_table_ids: &mut HashMap<TableId, CompactionGroupId>,
) -> Result<()> {
    for table_id in table_ids {
        if let Some(info) = state_table_info.info().get(table_id) {
            return Err(Error::CompactionGroup(format!(
                "table {} already exist {:?}",
                table_id, info,
            )));
        }
        table_compaction_group_mapping.insert(*table_id, compaction_group_id);
        new_table_ids.insert(*table_id, compaction_group_id);
    }

    Ok(())
}

/// Rewrite the commit sstables to sub-levels based on the compaction group config.
/// The type of `compaction_group_manager_txn` is too complex to be used in the function signature. So we use `HashMap` instead.
fn rewrite_commit_sstables_to_sub_level(
    commit_sstables: BTreeMap<CompactionGroupId, Vec<SstableInfo>>,
    group_id_to_config: &HashMap<CompactionGroupId, Arc<CompactionConfig>>,
) -> BTreeMap<CompactionGroupId, Vec<Vec<SstableInfo>>> {
    let mut overlapping_sstables: BTreeMap<CompactionGroupId, Vec<Vec<SstableInfo>>> =
        BTreeMap::new();
    for (group_id, inserted_table_infos) in commit_sstables {
        let config = group_id_to_config
            .get(&group_id)
            .expect("compaction group should exist");

        let mut accumulated_size = 0;
        let mut ssts = vec![];
        let sub_level_size_limit = config
            .max_overlapping_level_size
            .unwrap_or(compaction_config::max_overlapping_level_size());

        let level = overlapping_sstables.entry(group_id).or_default();

        for sst in inserted_table_infos {
            accumulated_size += sst.sst_size;
            ssts.push(sst);
            if accumulated_size > sub_level_size_limit {
                level.push(ssts);

                // reset the accumulated size and ssts
                accumulated_size = 0;
                ssts = vec![];
            }
        }

        if !ssts.is_empty() {
            level.push(ssts);
        }

        // The uploader organizes the ssts in decreasing epoch order, so the level needs to be reversed to ensure that the latest epoch is at the top.
        level.reverse();
    }

    overlapping_sstables
}

fn is_ordered_subset<T: PartialEq>(vec_1: &Vec<T>, vec_2: &Vec<T>) -> bool {
    let mut vec_2_iter = vec_2.iter().peekable();
    for item in vec_1 {
        if vec_2_iter.peek() == Some(&item) {
            vec_2_iter.next();
        }
    }

    vec_2_iter.peek().is_none()
}
