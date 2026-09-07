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
use std::ops::DerefMut;
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use anyhow::Context;
use compaction_event_loop::{
    HummockCompactionEventDispatcher, HummockCompactionEventHandler, HummockCompactionEventLoop,
    HummockCompactorDedicatedEventLoop,
};
use fail::fail_point;
use itertools::Itertools;
use parking_lot::Mutex;
use rand::rng as thread_rng;
use rand::seq::SliceRandom;
use risingwave_common::catalog::TableId;
use risingwave_common::config::meta::default::compaction_config;
use risingwave_common::util::epoch::Epoch;
use risingwave_hummock_sdk::compact_task::{CompactTask, CompactTaskAssignment, ReportTask};
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::safe_epoch_table_watermarks_impl;
use risingwave_hummock_sdk::level::Levels;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::{
    PbTableStatsMap, add_prost_table_stats_map, purge_prost_table_stats,
};
use risingwave_hummock_sdk::table_watermark::TableWatermarks;
use risingwave_hummock_sdk::version::{GroupDelta, IntraLevelDelta};
use risingwave_hummock_sdk::{
    CompactionGroupId, HummockCompactionTaskId, HummockContextId, HummockSstableId,
    HummockSstableObjectId, HummockVersionId, compact_task_to_string, statistics_compact_task,
};
use risingwave_meta_model::hummock_sequence::COMPACTION_TASK_ID;
use risingwave_pb::hummock::compact_task::{TaskStatus, TaskType};
use risingwave_pb::hummock::subscribe_compaction_event_response::Event as ResponseEvent;
use risingwave_pb::hummock::{
    CompactTaskAssignment as PbCompactTaskAssignment, CompactionConfig, PbCompactStatus,
    SubscribeCompactionEventRequest, TableOption, compact_task,
};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::task::JoinHandle;
use tonic::Streaming;
use tracing::warn;

use crate::hummock::compaction::in_progress_compaction::InProgressCompactionView;
use crate::hummock::compaction::selector::level_selector::PickerInfo;
use crate::hummock::compaction::selector::{
    DynamicLevelSelector, DynamicLevelSelectorCore, LocalSelectorStatistic, ManualCompactionOption,
    ManualCompactionSelector, SpaceReclaimCompactionSelector, TombstoneCompactionSelector,
    TtlCompactionSelector, VnodeWatermarkCompactionSelector,
};
use crate::hummock::compaction::{
    CompactStatus, CompactionDeveloperConfig, CompactionSelector,
    CompactionTask as PickedCompactionTask,
};
use crate::hummock::error::{Error, Result};
use crate::hummock::manager::CompactionTaskReportResult;
use crate::hummock::manager::compaction::compact_task_builder::{
    CompactTaskBuildContext, attach_compact_task_table_metadata, build_base_compact_task,
};
use crate::hummock::manager::transaction::{
    HummockVersionStatsTransaction, HummockVersionTransaction,
};
use crate::hummock::manager::versioning::Versioning;
use crate::hummock::metrics_utils::{
    build_compact_task_level_type_metrics_label, trigger_compact_tasks_stat,
    trigger_local_table_stat,
};
use crate::hummock::model::CompactionGroup;
use crate::hummock::{HummockManager, commit_multi_var};
use crate::manager::META_NODE_ID;
use crate::model::BTreeMapTransaction;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManualCompactionTriggerResult {
    Submitted,
    Retry,
}

mod compact_task_builder;
pub mod compaction_event_loop;
pub mod compaction_group_manager;
pub mod compaction_group_schedule;

static CANCEL_STATUS_SET: LazyLock<HashSet<TaskStatus>> = LazyLock::new(|| {
    [
        TaskStatus::ManualCanceled,
        TaskStatus::SendFailCanceled,
        TaskStatus::AssignFailCanceled,
        TaskStatus::HeartbeatCanceled,
        TaskStatus::InvalidGroupCanceled,
        TaskStatus::NoAvailMemoryResourceCanceled,
        TaskStatus::NoAvailCpuResourceCanceled,
        TaskStatus::HeartbeatProgressCanceled,
    ]
    .into_iter()
    .collect()
});

fn init_selectors() -> HashMap<compact_task::TaskType, Box<dyn CompactionSelector>> {
    let mut compaction_selectors: HashMap<compact_task::TaskType, Box<dyn CompactionSelector>> =
        HashMap::default();
    compaction_selectors.insert(
        compact_task::TaskType::Dynamic,
        Box::<DynamicLevelSelector>::default(),
    );
    compaction_selectors.insert(
        compact_task::TaskType::SpaceReclaim,
        Box::<SpaceReclaimCompactionSelector>::default(),
    );
    compaction_selectors.insert(
        compact_task::TaskType::Ttl,
        Box::<TtlCompactionSelector>::default(),
    );
    compaction_selectors.insert(
        compact_task::TaskType::Tombstone,
        Box::<TombstoneCompactionSelector>::default(),
    );
    compaction_selectors.insert(
        compact_task::TaskType::VnodeWatermark,
        Box::<VnodeWatermarkCompactionSelector>::default(),
    );
    compaction_selectors
}

enum BuiltCompactTask {
    MetaFinished(CompactTask),
    PendingAssignment(CompactTask),
}

impl HummockVersionTransaction<'_> {
    fn apply_compact_task(&mut self, compact_task: &CompactTask) {
        let mut version_delta = self.new_delta();
        let trivial_move = compact_task.is_trivial_move_task();
        version_delta.trivial_move = trivial_move;

        let group_deltas = &mut version_delta
            .group_deltas
            .entry(compact_task.compaction_group_id)
            .or_default()
            .group_deltas;
        let mut removed_table_ids_map: BTreeMap<u32, HashSet<HummockSstableId>> =
            BTreeMap::default();

        for level in &compact_task.input_ssts {
            let level_idx = level.level_idx;

            removed_table_ids_map
                .entry(level_idx)
                .or_default()
                .extend(level.table_infos.iter().map(|sst| sst.sst_id));
        }

        for (level_idx, removed_table_ids) in removed_table_ids_map {
            let group_delta = GroupDelta::IntraLevel(IntraLevelDelta::new(
                level_idx,
                0, // default
                removed_table_ids,
                vec![], // default
                0,      // default
                compact_task.compaction_group_version_id,
            ));

            group_deltas.push(group_delta);
        }

        let group_delta = GroupDelta::IntraLevel(IntraLevelDelta::new(
            compact_task.target_level,
            compact_task.target_sub_level_id,
            HashSet::new(), // default
            compact_task.sorted_output_ssts.clone(),
            compact_task.split_weight_by_vnode,
            compact_task.compaction_group_version_id,
        ));

        group_deltas.push(group_delta);
        version_delta.pre_apply();
    }
}

#[derive(Default)]
pub struct Compaction {
    /// Compaction task that is already assigned to a compactor
    pub compact_task_assignment: BTreeMap<HummockCompactionTaskId, CompactTaskAssignment>,
    /// `CompactStatus` of each compaction group
    pub compaction_statuses: BTreeMap<CompactionGroupId, CompactStatus>,

    pub _deterministic_mode: bool,
}

impl HummockManager {
    pub async fn get_assigned_compact_task_num(&self) -> u64 {
        self.compaction
            .read_with_process_name("get_assigned_compact_task_num")
            .await
            .compact_task_assignment
            .len() as u64
    }

    pub async fn list_compaction_status(
        &self,
    ) -> (Vec<PbCompactStatus>, Vec<PbCompactTaskAssignment>) {
        let (compaction_statuses, compact_task_assignments) = {
            let compaction = self
                .compaction
                .read_with_process_name("list_compaction_status")
                .await;
            (
                compaction
                    .compaction_statuses
                    .values()
                    .map_into()
                    .collect_vec(),
                compaction
                    .compact_task_assignment
                    .values()
                    .cloned()
                    .collect_vec(),
            )
        };

        (
            compaction_statuses,
            compact_task_assignments
                .into_iter()
                .map(PbCompactTaskAssignment::from)
                .collect(),
        )
    }

    pub async fn get_compaction_scores(
        &self,
        compaction_group_id: CompactionGroupId,
    ) -> Vec<PickerInfo> {
        let (status, levels, group) = {
            let compaction = self
                .compaction
                .read_with_process_name("get_compaction_scores")
                .await;
            let versioning = self
                .versioning
                .read_with_process_name("get_compaction_scores")
                .await;
            let config_manager = self
                .compaction_group_manager
                .read_with_process_name("get_compaction_scores")
                .await;
            match (
                compaction.compaction_statuses.get(&compaction_group_id),
                versioning.current_version.levels.get(&compaction_group_id),
                config_manager.try_get_compaction_group_config(compaction_group_id),
            ) {
                (Some(cs), Some(v), Some(cf)) => (cs.to_owned(), v.to_owned(), cf),
                _ => {
                    return vec![];
                }
            }
        };
        let dynamic_level_core = DynamicLevelSelectorCore::new(
            group.compaction_config,
            Arc::new(CompactionDeveloperConfig::default()),
        );
        let ctx = dynamic_level_core.get_priority_levels(&levels, &status.level_handlers);
        ctx.score_levels
    }
}

impl HummockManager {
    pub fn compaction_event_loop(
        hummock_manager: Arc<Self>,
        compactor_streams_change_rx: UnboundedReceiver<(
            HummockContextId,
            Streaming<SubscribeCompactionEventRequest>,
        )>,
    ) -> Vec<(JoinHandle<()>, Sender<()>)> {
        let mut join_handle_vec = Vec::default();

        let hummock_compaction_event_handler =
            HummockCompactionEventHandler::new(hummock_manager.clone());

        let dedicated_event_loop = HummockCompactorDedicatedEventLoop::new(
            hummock_manager.clone(),
            hummock_compaction_event_handler.clone(),
        );

        let (dedicated_event_loop_join_handle, event_tx, shutdown_tx) = dedicated_event_loop.run();
        join_handle_vec.push((dedicated_event_loop_join_handle, shutdown_tx));

        let hummock_compaction_event_dispatcher = HummockCompactionEventDispatcher::new(
            hummock_manager.env.opts.clone(),
            hummock_compaction_event_handler,
            Some(event_tx),
        );

        let event_loop = HummockCompactionEventLoop::new(
            hummock_compaction_event_dispatcher,
            hummock_manager.metrics.clone(),
            compactor_streams_change_rx,
        );

        let (event_loop_join_handle, event_loop_shutdown_tx) = event_loop.run();
        join_handle_vec.push((event_loop_join_handle, event_loop_shutdown_tx));

        join_handle_vec
    }

    pub fn add_compactor_stream(
        &self,
        context_id: HummockContextId,
        req_stream: Streaming<SubscribeCompactionEventRequest>,
    ) {
        self.compactor_streams_change_tx
            .send((context_id, req_stream))
            .unwrap();
    }
}

impl HummockManager {
    /// Gets one compaction task id with best-effort batching while ensuring concurrent callers
    /// share the same refill instead of wasting an allocated range.
    async fn next_compaction_task_id_with_prefetch(&self, refill_capacity: u32) -> Result<u64> {
        self.prefetched_compaction_task_ids
            .next(refill_capacity, |count| async move {
                self.env
                    .hummock_seq
                    .next_interval(COMPACTION_TASK_ID, count)
                    .await
            })
            .await
    }

    pub async fn get_compact_tasks_impl(
        &self,
        compaction_groups: Vec<CompactionGroupId>,
        max_select_count: usize,
        selector: &mut dyn CompactionSelector,
    ) -> Result<(Vec<CompactTask>, Vec<CompactionGroupId>)> {
        let deterministic_mode = self.env.opts.compaction_deterministic_test;

        let mut compaction_guard = self
            .compaction
            .write_with_process_name("get_compact_tasks_impl")
            .await;
        let mut versioning_guard = self
            .versioning
            .write_with_process_name("get_compact_tasks_impl")
            .await;
        let compaction: &mut Compaction = &mut compaction_guard;
        let versioning: &mut Versioning = &mut versioning_guard;

        let start_time = Instant::now();
        let mut compaction_statuses = BTreeMapTransaction::new(&mut compaction.compaction_statuses);

        let mut compact_task_assignment =
            BTreeMapTransaction::new(&mut compaction.compact_task_assignment);

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
        // Apply stats changes.
        let mut version_stats = HummockVersionStatsTransaction::new(
            &mut versioning.version_stats,
            self.env.notification_manager(),
        );

        if deterministic_mode {
            version.disable_apply_to_txn();
        }
        let all_versioned_table_schemas = if self.env.opts.enable_dropped_column_reclaim {
            self.metadata_manager
                .catalog_controller
                .get_versioned_table_schemas()
                .await
                .map_err(|e| Error::Internal(e.into()))?
        } else {
            HashMap::default()
        };
        let mut unschedule_groups = vec![];
        let mut trivial_tasks = vec![];
        let mut pick_tasks = vec![];
        let developer_config = Arc::new(CompactionDeveloperConfig::new_from_meta_opts(
            &self.env.opts,
        ));
        // Reuse prefetched task ids from previous loops.
        // Each group consumes at most one task_id (trivial tasks share the same id with normal
        // task). When prefetched ids are exhausted, refill in fixed-size chunks to avoid
        // per-group SQL transactions while keeping the in-memory cache small.
        'outside: for compaction_group_id in compaction_groups {
            if pick_tasks.len() >= max_select_count {
                break;
            }

            if !version
                .latest_version()
                .levels
                .contains_key(&compaction_group_id)
            {
                continue;
            }

            // When the last table of a compaction group is deleted, the compaction group (and its
            // config) is destroyed as well. Then a compaction task for this group may come later and
            // cannot find its config.
            let group_config = {
                let config_manager = self
                    .compaction_group_manager
                    .read_with_process_name("get_compact_tasks_impl")
                    .await;

                match config_manager.try_get_compaction_group_config(compaction_group_id) {
                    Some(config) => config,
                    None => continue,
                }
            };

            // Use prefetched task id if available; when exhausted, refill in chunks first.
            let task_id = self
                .next_compaction_task_id_with_prefetch(
                    self.env.opts.compaction_task_id_refill_capacity,
                )
                .await?;

            if !compaction_statuses.contains_key(&compaction_group_id) {
                // lazy initialize.
                compaction_statuses.insert(
                    compaction_group_id,
                    CompactStatus::new(
                        compaction_group_id,
                        group_config.compaction_config.max_level,
                    ),
                );
            }
            let mut compact_status = compaction_statuses.get_mut(compaction_group_id).unwrap();

            let mut stats = LocalSelectorStatistic::default();
            let member_table_ids: Vec<_> = version
                .latest_version()
                .state_table_info
                .compaction_group_member_table_ids(compaction_group_id)
                .iter()
                .copied()
                .collect();

            let mut table_id_to_option: HashMap<TableId, _> = HashMap::default();

            {
                let guard = self.table_id_to_table_option.read();
                for table_id in &member_table_ids {
                    if let Some(opts) = guard.get(table_id) {
                        table_id_to_option.insert(*table_id, *opts);
                    }
                }
            }

            let in_progress_compactions = InProgressCompactionView::for_group(
                compact_task_assignment.tree_ref().values(),
                compaction_group_id,
            );

            while let Some(picked_task) = compact_status.get_compact_task(
                version
                    .latest_version()
                    .get_compaction_group_levels(compaction_group_id),
                version
                    .latest_version()
                    .state_table_info
                    .compaction_group_member_table_ids(compaction_group_id),
                task_id as HummockCompactionTaskId,
                &group_config,
                &mut stats,
                selector,
                &table_id_to_option,
                developer_config.clone(),
                &version.latest_version().table_watermarks,
                &version.latest_version().state_table_info,
                &in_progress_compactions,
            ) {
                let compaction_group_levels = version
                    .latest_version()
                    .get_compaction_group_levels(compaction_group_id);
                let target_level_id = picked_task.input.target_level as u32;
                let is_target_level_last = compaction_group_levels.is_last_level(target_level_id);
                let table_options = table_id_to_option
                    .iter()
                    .map(|(table_id, table_option)| (*table_id, TableOption::from(table_option)))
                    .collect();
                let built_compact_task = self.build_ready_compact_task(
                    picked_task,
                    CompactTaskBuildContext {
                        task_id,
                        compaction_group_id: group_config.group_id,
                        compaction_group_version_id: compaction_group_levels
                            .compaction_group_version_id,
                        existing_table_ids: member_table_ids.clone(),
                        table_options,
                        is_target_level_last,
                        compaction_config: group_config.compaction_config.clone(),
                        current_epoch_time: Epoch::now().0,
                    },
                    &version.latest_version().table_watermarks,
                    &all_versioned_table_schemas,
                );

                match built_compact_task {
                    BuiltCompactTask::MetaFinished(compact_task) => {
                        let label = compact_task.task_label();
                        tracing::debug!(
                            "{} for compaction group {}: input: {:?}, cost time: {:?}",
                            label,
                            compact_task.compaction_group_id,
                            compact_task.input_ssts,
                            start_time.elapsed()
                        );
                        compact_status.report_compact_task(&compact_task);
                        update_table_stats_for_vnode_watermark_trivial_reclaim(
                            &mut version_stats.table_stats,
                            &compact_task,
                        );
                        self.metrics
                            .compact_frequency
                            .with_label_values(&[
                                label,
                                &compact_task.compaction_group_id.to_string(),
                                selector.task_type().as_str_name(),
                                "SUCCESS",
                            ])
                            .inc();

                        version.apply_compact_task(&compact_task);
                        trivial_tasks.push(compact_task);
                        if trivial_tasks.len() >= self.env.opts.max_trivial_move_task_count_per_loop
                        {
                            break 'outside;
                        }
                    }
                    BuiltCompactTask::PendingAssignment(compact_task) => {
                        compact_task_assignment.insert(
                            compact_task.task_id,
                            CompactTaskAssignment {
                                compact_task: compact_task.clone(),
                                context_id: META_NODE_ID, // deprecated
                            },
                        );

                        pick_tasks.push(compact_task);
                        break;
                    }
                }

                stats.report_to_metrics(compaction_group_id, self.metrics.as_ref());
                stats = LocalSelectorStatistic::default();
            }
            if pick_tasks
                .last()
                .map(|task| task.compaction_group_id != compaction_group_id)
                .unwrap_or(true)
            {
                unschedule_groups.push(compaction_group_id);
            }
            stats.report_to_metrics(compaction_group_id, self.metrics.as_ref());
        }

        if !trivial_tasks.is_empty() {
            commit_multi_var!(
                self.meta_store_ref(),
                compaction_statuses,
                compact_task_assignment,
                version,
                version_stats
            )?;
            self.metrics
                .compact_task_batch_count
                .with_label_values(&["batch_trivial_move"])
                .observe(trivial_tasks.len() as f64);

            for trivial_task in &trivial_tasks {
                self.metrics
                    .compact_task_trivial_move_sst_count
                    .with_label_values(&[&trivial_task.compaction_group_id.to_string()])
                    .observe(trivial_task.input_ssts[0].table_infos.len() as _);
            }

            drop(versioning_guard);
        } else {
            // We are using a single transaction to ensure that each task has progress when it is
            // created.
            drop(versioning_guard);
            commit_multi_var!(
                self.meta_store_ref(),
                compaction_statuses,
                compact_task_assignment
            )?;
        }
        drop(compaction_guard);
        if !pick_tasks.is_empty() {
            self.metrics
                .compact_task_batch_count
                .with_label_values(&["batch_get_compact_task"])
                .observe(pick_tasks.len() as f64);
        }

        for compact_task in &mut pick_tasks {
            let compaction_group_id = compact_task.compaction_group_id;

            // Initiate heartbeat for the task to track its progress.
            self.compactor_manager
                .initiate_task_heartbeat(compact_task.clone());

            // this task has been finished.
            compact_task.task_status = TaskStatus::Pending;
            let compact_task_statistics = statistics_compact_task(compact_task);

            let level_type_label = build_compact_task_level_type_metrics_label(
                compact_task.input_ssts[0].level_idx as usize,
                compact_task.input_ssts.last().unwrap().level_idx as usize,
            );

            let level_count = compact_task.input_ssts.len();
            if compact_task.input_ssts[0].level_idx == 0 {
                self.metrics
                    .l0_compact_level_count
                    .with_label_values(&[&compaction_group_id.to_string(), &level_type_label])
                    .observe(level_count as _);
            }

            self.metrics
                .compact_task_size
                .with_label_values(&[&compaction_group_id.to_string(), &level_type_label])
                .observe(compact_task_statistics.total_file_size as _);

            self.metrics
                .compact_task_size
                .with_label_values(&[
                    &compaction_group_id.to_string(),
                    &format!("{} uncompressed", level_type_label),
                ])
                .observe(compact_task_statistics.total_uncompressed_file_size as _);

            self.metrics
                .compact_task_file_count
                .with_label_values(&[&compaction_group_id.to_string(), &level_type_label])
                .observe(compact_task_statistics.total_file_count as _);

            tracing::trace!(
                "For compaction group {}: pick up {} {} sub_level in level {} to compact to target {}. cost time: {:?} compact_task_statistics {:?}",
                compaction_group_id,
                level_count,
                compact_task.input_ssts[0].level_type.as_str_name(),
                compact_task.input_ssts[0].level_idx,
                compact_task.target_level,
                start_time.elapsed(),
                compact_task_statistics
            );
        }

        #[cfg(test)]
        {
            self.check_state_consistency().await;
        }
        pick_tasks.extend(trivial_tasks);
        Ok((pick_tasks, unschedule_groups))
    }

    /// Cancels a compaction task no matter it's assigned or unassigned.
    pub async fn cancel_compact_task(&self, task_id: u64, task_status: TaskStatus) -> Result<bool> {
        fail_point!("fp_cancel_compact_task", |_| Err(Error::MetaStore(
            anyhow::anyhow!("failpoint metastore err")
        )));
        let ret = self
            .cancel_compact_task_impl(vec![task_id], task_status)
            .await?;
        Ok(ret[0])
    }

    pub async fn cancel_compact_tasks(
        &self,
        tasks: Vec<u64>,
        task_status: TaskStatus,
    ) -> Result<Vec<bool>> {
        self.cancel_compact_task_impl(tasks, task_status).await
    }

    async fn cancel_compact_task_impl(
        &self,
        task_ids: Vec<u64>,
        task_status: TaskStatus,
    ) -> Result<Vec<bool>> {
        assert!(CANCEL_STATUS_SET.contains(&task_status));
        let tasks = task_ids
            .into_iter()
            .map(|task_id| ReportTask {
                task_id,
                task_status,
                sorted_output_ssts: vec![],
                table_stats_change: HashMap::default(),
                object_timestamps: HashMap::default(),
            })
            .collect_vec();
        let rets = self.report_compact_tasks(tasks).await?;
        #[cfg(test)]
        {
            self.check_state_consistency().await;
        }
        Ok(rets)
    }

    async fn get_compact_tasks(
        &self,
        mut compaction_groups: Vec<CompactionGroupId>,
        max_select_count: usize,
        selector: &mut dyn CompactionSelector,
    ) -> Result<(Vec<CompactTask>, Vec<CompactionGroupId>)> {
        fail_point!("fp_get_compact_task", |_| Err(Error::MetaStore(
            anyhow::anyhow!("failpoint metastore error")
        )));
        compaction_groups.shuffle(&mut thread_rng());
        let (mut tasks, groups) = self
            .get_compact_tasks_impl(compaction_groups, max_select_count, selector)
            .await?;
        tasks.retain(|task| {
            if task.task_status == TaskStatus::Success {
                debug_assert!(task.is_trivial_reclaim() || task.is_trivial_move_task());
                false
            } else {
                true
            }
        });
        Ok((tasks, groups))
    }

    pub async fn get_compact_task(
        &self,
        compaction_group_id: CompactionGroupId,
        selector: &mut dyn CompactionSelector,
    ) -> Result<Option<CompactTask>> {
        fail_point!("fp_get_compact_task", |_| Err(Error::MetaStore(
            anyhow::anyhow!("failpoint metastore error")
        )));

        let (normal_tasks, _) = self
            .get_compact_tasks_impl(vec![compaction_group_id], 1, selector)
            .await?;
        for task in normal_tasks {
            if task.task_status != TaskStatus::Success {
                return Ok(Some(task));
            }
            debug_assert!(task.is_trivial_reclaim() || task.is_trivial_move_task());
        }
        Ok(None)
    }

    pub async fn manual_get_compact_task(
        &self,
        compaction_group_id: CompactionGroupId,
        manual_compaction_option: ManualCompactionOption,
    ) -> Result<Option<CompactTask>> {
        let (task, _) = self
            .manual_get_compact_task_with_info(compaction_group_id, manual_compaction_option)
            .await?;
        Ok(task)
    }

    pub async fn manual_get_compact_task_with_info(
        &self,
        compaction_group_id: CompactionGroupId,
        manual_compaction_option: ManualCompactionOption,
    ) -> Result<(Option<CompactTask>, bool)> {
        let mut selector = ManualCompactionSelector::new(manual_compaction_option);
        let task = self
            .get_compact_task(compaction_group_id, &mut selector)
            .await?;
        if let Some(err) = selector.validation_error() {
            return Err(Error::InvalidManualCompactionOption(err.to_owned()));
        }
        Ok((task, selector.blocked_by_pending()))
    }

    pub async fn report_compact_task(
        &self,
        task_id: u64,
        task_status: TaskStatus,
        sorted_output_ssts: Vec<SstableInfo>,
        table_stats_change: Option<PbTableStatsMap>,
        object_timestamps: HashMap<HummockSstableObjectId, u64>,
    ) -> Result<bool> {
        let rets = self
            .report_compact_tasks(vec![ReportTask {
                task_id,
                task_status,
                sorted_output_ssts,
                table_stats_change: table_stats_change.unwrap_or_default(),
                object_timestamps,
            }])
            .await?;
        Ok(rets[0])
    }

    pub async fn report_compact_tasks(&self, report_tasks: Vec<ReportTask>) -> Result<Vec<bool>> {
        let compaction_guard = self
            .compaction
            .write_with_process_name("report_compact_tasks")
            .await;
        let versioning_guard = self
            .versioning
            .write_with_process_name("report_compact_tasks")
            .await;

        self.report_compact_tasks_impl(report_tasks, compaction_guard, versioning_guard)
            .await
    }

    /// Finishes or cancels a compaction task, according to `task_status`.
    ///
    /// If `context_id` is not None, its validity will be checked when writing meta store.
    /// Its ownership of the task is checked as well.
    ///
    /// Return Ok(false) indicates either the task is not found,
    /// or the task is not owned by `context_id` when `context_id` is not None.
    pub async fn report_compact_tasks_impl(
        &self,
        report_tasks: Vec<ReportTask>,
        mut compaction_guard: impl DerefMut<Target = Compaction>,
        mut versioning_guard: impl DerefMut<Target = Versioning>,
    ) -> Result<Vec<bool>> {
        let deterministic_mode = self.env.opts.compaction_deterministic_test;
        let compaction: &mut Compaction = &mut compaction_guard;
        let start_time = Instant::now();
        let original_keys = compaction.compaction_statuses.keys().cloned().collect_vec();
        let mut compact_statuses = BTreeMapTransaction::new(&mut compaction.compaction_statuses);
        let mut rets = vec![false; report_tasks.len()];
        let mut compact_task_assignment =
            BTreeMapTransaction::new(&mut compaction.compact_task_assignment);
        // The compaction task is finished.
        let versioning: &mut Versioning = &mut versioning_guard;

        // purge stale compact_status
        for group_id in original_keys {
            if !versioning.current_version.levels.contains_key(&group_id) {
                compact_statuses.remove(group_id);
            }
        }
        let mut tasks = vec![];

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

        if deterministic_mode {
            version.disable_apply_to_txn();
        }

        let mut version_stats = HummockVersionStatsTransaction::new(
            &mut versioning.version_stats,
            self.env.notification_manager(),
        );
        let mut success_count = 0;
        let mut report_results = Vec::with_capacity(rets.len());
        for (idx, task) in report_tasks.into_iter().enumerate() {
            rets[idx] = true;
            let task_id = task.task_id;
            let mut task_status = task.task_status;
            let mut compact_task = match compact_task_assignment.remove(task.task_id) {
                Some(compact_task_assignment) => compact_task_assignment.compact_task,
                None => {
                    tracing::warn!("{}", format!("compact task {} not found", task.task_id));
                    rets[idx] = false;
                    report_results.push(CompactionTaskReportResult {
                        task_id,
                        task_status,
                        reported: false,
                    });
                    continue;
                }
            };

            {
                // apply result
                compact_task.task_status = task.task_status;
                compact_task.sorted_output_ssts = task.sorted_output_ssts;
            }

            match compact_statuses.get_mut(compact_task.compaction_group_id) {
                Some(mut compact_status) => {
                    compact_status.report_compact_task(&compact_task);
                }
                None => {
                    // When the group_id is not found in the compaction_statuses, it means the group has been removed.
                    // The task is invalid and should be canceled.
                    // e.g.
                    // 1. The group is removed by the user unregistering the tables
                    // 2. The group is removed by the group scheduling algorithm
                    compact_task.task_status = TaskStatus::InvalidGroupCanceled;
                }
            }

            let is_success = if let TaskStatus::Success = compact_task.task_status {
                match self
                    .report_compaction_sanity_check(&task.object_timestamps)
                    .await
                {
                    Err(e) => {
                        warn!(
                            "failed to commit compaction task {} {}",
                            compact_task.task_id,
                            e.as_report()
                        );
                        compact_task.task_status = TaskStatus::RetentionTimeRejected;
                        false
                    }
                    _ => {
                        let group = version
                            .latest_version()
                            .levels
                            .get(&compact_task.compaction_group_id)
                            .unwrap();
                        let is_expired = compact_task.is_expired(group.compaction_group_version_id);
                        if is_expired {
                            compact_task.task_status = TaskStatus::InputOutdatedCanceled;
                            warn!(
                                "The task may be expired because of group split, task:\n {:?}",
                                compact_task_to_string(&compact_task)
                            );
                        }
                        !is_expired
                    }
                }
            } else {
                false
            };
            if is_success {
                success_count += 1;
                version.apply_compact_task(&compact_task);
                if purge_prost_table_stats(
                    &mut version_stats.table_stats,
                    version.latest_version(),
                    &HashSet::default(),
                ) {
                    self.metrics.version_stats.reset();
                    versioning.local_metrics.clear();
                }
                add_prost_table_stats_map(&mut version_stats.table_stats, &task.table_stats_change);
                trigger_local_table_stat(
                    &self.metrics,
                    &mut versioning.local_metrics,
                    &version_stats,
                    &task.table_stats_change,
                );
            }
            task_status = compact_task.task_status;
            report_results.push(CompactionTaskReportResult {
                task_id,
                task_status,
                reported: rets[idx],
            });
            tasks.push(compact_task);
        }
        if success_count > 0 {
            commit_multi_var!(
                self.meta_store_ref(),
                compact_statuses,
                compact_task_assignment,
                version,
                version_stats
            )?;

            self.metrics
                .compact_task_batch_count
                .with_label_values(&["batch_report_task"])
                .observe(success_count as f64);
        } else {
            // The compaction task is cancelled or failed.
            commit_multi_var!(
                self.meta_store_ref(),
                compact_statuses,
                compact_task_assignment
            )?;
        }

        self.notify_compaction_task_report_waiters(report_results);

        let mut success_groups = vec![];
        for compact_task in &tasks {
            self.compactor_manager
                .remove_task_heartbeat(compact_task.task_id);
            tracing::trace!(
                "Reported compaction task. {}. cost time: {:?}",
                compact_task_to_string(compact_task),
                start_time.elapsed(),
            );

            if !deterministic_mode
                && (matches!(compact_task.task_type, compact_task::TaskType::Dynamic)
                    || matches!(compact_task.task_type, compact_task::TaskType::Emergency))
            {
                // only try send Dynamic compaction
                self.try_send_compaction_request(
                    compact_task.compaction_group_id,
                    compact_task::TaskType::Dynamic,
                );
            }

            if compact_task.task_status == TaskStatus::Success {
                success_groups.push(compact_task.compaction_group_id);
            }
        }

        trigger_compact_tasks_stat(
            &self.metrics,
            &tasks,
            &compaction.compaction_statuses,
            &versioning_guard.current_version,
        );
        drop(versioning_guard);
        if !success_groups.is_empty() {
            self.try_update_write_limits(&success_groups).await;
        }
        Ok(rets)
    }

    /// Triggers compacitons to specified compaction groups.
    /// Don't wait for compaction finish
    pub async fn trigger_compaction_deterministic(
        &self,
        _base_version_id: HummockVersionId,
        compaction_groups: Vec<CompactionGroupId>,
    ) -> Result<()> {
        self.on_current_version(|old_version| {
            tracing::info!(
                "Trigger compaction for version {}, groups {:?}",
                old_version.id,
                compaction_groups
            );
        })
        .await;

        if compaction_groups.is_empty() {
            return Ok(());
        }
        for compaction_group in compaction_groups {
            self.try_send_compaction_request(compaction_group, compact_task::TaskType::Dynamic);
        }
        Ok(())
    }

    pub async fn trigger_manual_compaction(
        &self,
        compaction_group: CompactionGroupId,
        manual_compaction_option: ManualCompactionOption,
    ) -> Result<ManualCompactionTriggerResult> {
        let start_time = Instant::now();
        let exclusive = manual_compaction_option.exclusive;

        // 1. Get idle compactor.
        let compactor = match self.compactor_manager.next_compactor() {
            Some(compactor) => compactor,
            None => {
                tracing::warn!("trigger_manual_compaction No compactor is available.");
                return Err(anyhow::anyhow!(
                    "trigger_manual_compaction No compactor is available. compaction_group {}",
                    compaction_group
                )
                .into());
            }
        };

        // 2. Get manual compaction task.
        let compact_task = self
            .manual_get_compact_task_with_info(compaction_group, manual_compaction_option)
            .await;
        let (compact_task, blocked_by_pending) = match compact_task {
            Ok((compact_task, blocked_by_pending)) => (compact_task, blocked_by_pending),
            Err(err) => {
                tracing::warn!(error = %err.as_report(), "Failed to get compaction task");
                if matches!(err, Error::InvalidManualCompactionOption(_)) {
                    return Err(err);
                }

                return Err(anyhow::anyhow!(err)
                    .context(format!(
                        "Failed to get compaction task for compaction_group {}",
                        compaction_group,
                    ))
                    .into());
            }
        };
        let compact_task = match compact_task {
            Some(compact_task) => compact_task,
            None => {
                if exclusive && blocked_by_pending {
                    return Ok(ManualCompactionTriggerResult::Retry);
                }
                // No compaction task available.
                return Err(anyhow::anyhow!(
                    "trigger_manual_compaction No compaction_task is available. compaction_group {}",
                    compaction_group
                )
                .into());
            }
        };

        // 3. send task to compactor
        let task_id = compact_task.task_id;
        let compact_task_string = compact_task_to_string(&compact_task);
        tracing::info!(
            compact_task_string,
            duration = ?start_time.elapsed(),
            "Triggered manual compaction task."
        );

        let report_rx = self.register_compaction_task_report_waiter(task_id);
        if let Err(err) = compactor
            .send_event(ResponseEvent::CompactTask(compact_task.into()))
            .with_context(|| {
                format!(
                    "Failed to trigger compaction task for compaction_group {}",
                    compaction_group,
                )
            })
        {
            self.remove_compaction_task_report_waiter(task_id);
            return Err(err.into());
        }

        let report_result = match report_rx.await {
            Ok(result) => result,
            Err(_) => {
                self.remove_compaction_task_report_waiter(task_id);
                return Err(anyhow::anyhow!(
                    "trigger_manual_compaction wait report failed. compaction_group {}",
                    compaction_group
                )
                .into());
            }
        };
        if !report_result.reported {
            return Err(anyhow::anyhow!(
                "trigger_manual_compaction report not accepted. task_id {}",
                report_result.task_id
            )
            .into());
        }

        if report_result.task_status == TaskStatus::NoAvailCpuResourceCanceled
            || report_result.task_status == TaskStatus::NoAvailMemoryResourceCanceled
        {
            return Ok(ManualCompactionTriggerResult::Retry);
        }

        tracing::info!(
            ?report_result,
            duration = ?start_time.elapsed(),
            "Completed manual compaction task."
        );

        Ok(ManualCompactionTriggerResult::Submitted)
    }

    /// Sends a compaction request for new data (clears cooldown).
    pub fn try_send_compaction_request(
        &self,
        compaction_group: CompactionGroupId,
        task_type: compact_task::TaskType,
    ) -> bool {
        self.compaction_state.try_sched_compaction(
            compaction_group,
            task_type,
            ScheduleTrigger::NewData,
        )
    }

    /// Apply `split_weight_by_vnode` based partition strategy.
    /// This handles dynamic partitioning based on table size and write throughput.
    fn apply_split_weight_by_vnode_partition(
        &self,
        compact_task: &mut CompactTask,
        compaction_config: &CompactionConfig,
        compact_table_ids: &[TableId],
    ) {
        if compaction_config.split_weight_by_vnode > 0 {
            for table_id in compact_table_ids {
                compact_task
                    .table_vnode_partition
                    .insert(*table_id, compact_task.split_weight_by_vnode);
            }

            return;
        }

        // Calculate per-table size from normalized input SSTs.
        let mut table_size_info: HashMap<TableId, u64> = HashMap::default();
        for input_ssts in &compact_task.input_ssts {
            for sst in &input_ssts.table_infos {
                for table_id in &sst.table_ids {
                    *table_size_info.entry(*table_id).or_default() +=
                        sst.sst_size / (sst.table_ids.len() as u64);
                }
            }
        }

        let hybrid_vnode_count = self.env.opts.hybrid_partition_node_count;
        let default_partition_count = self.env.opts.partition_vnode_count;
        let compact_task_table_size_partition_threshold_low = self
            .env
            .opts
            .compact_task_table_size_partition_threshold_low;
        let compact_task_table_size_partition_threshold_high = self
            .env
            .opts
            .compact_task_table_size_partition_threshold_high;

        // Check latest write throughput
        let table_write_throughput_statistic_manager =
            self.table_write_throughput_statistic_manager.read();
        let timestamp = chrono::Utc::now().timestamp();

        for (table_id, compact_table_size) in table_size_info {
            let write_throughput = table_write_throughput_statistic_manager
                .get_table_throughput_descending(table_id, timestamp)
                .peekable()
                .peek()
                .map(|item| item.throughput)
                .unwrap_or(0);

            if compact_table_size > compact_task_table_size_partition_threshold_high
                && default_partition_count > 0
            {
                compact_task
                    .table_vnode_partition
                    .insert(table_id, default_partition_count);
            } else if (compact_table_size > compact_task_table_size_partition_threshold_low
                || (write_throughput > self.env.opts.table_high_write_throughput_threshold
                    && compact_table_size > compaction_config.target_file_size_base))
                && hybrid_vnode_count > 0
            {
                compact_task
                    .table_vnode_partition
                    .insert(table_id, hybrid_vnode_count);
            } else if compact_table_size > compaction_config.target_file_size_base {
                compact_task.table_vnode_partition.insert(table_id, 1);
            }
        }

        compact_task
            .table_vnode_partition
            .retain(|table_id, _| compact_table_ids.contains(table_id));
    }

    pub(crate) fn calculate_vnode_partition(
        &self,
        compact_task: &mut CompactTask,
        compaction_config: &CompactionConfig,
        compact_table_ids: &[TableId],
    ) {
        // Do not split sst by vnode partition when target_level > base_level
        // The purpose of data alignment is mainly to improve the parallelism of base level compaction
        // and reduce write amplification. However, at high level, the size of the sst file is often
        // larger and only contains the data of a single table_id, so there is no need to cut it.
        if compact_task.target_level > compact_task.base_level {
            return;
        }

        // Apply split_weight_by_vnode based partition strategy
        self.apply_split_weight_by_vnode_partition(
            compact_task,
            compaction_config,
            compact_table_ids,
        );
    }

    fn build_ready_compact_task(
        &self,
        picked_task: PickedCompactionTask,
        context: CompactTaskBuildContext,
        table_watermarks: &HashMap<TableId, Arc<TableWatermarks>>,
        all_versioned_table_schemas: &HashMap<TableId, Vec<i32>>,
    ) -> BuiltCompactTask {
        let compaction_config = context.compaction_config.clone();
        let (mut compact_task, compact_table_ids) = build_base_compact_task(picked_task, context);

        if compact_task.is_trivial_reclaim() {
            compact_task.task_status = TaskStatus::Success;
            compact_task.sorted_output_ssts.clear();
            return BuiltCompactTask::MetaFinished(compact_task);
        }

        if compact_task.is_trivial_move_task() {
            compact_task.task_status = TaskStatus::Success;
            compact_task.sorted_output_ssts = compact_task.input_ssts[0]
                .read_sstable_infos()
                .cloned()
                .collect();
            return BuiltCompactTask::MetaFinished(compact_task);
        }

        self.prepare_compact_task_for_assignment(
            &mut compact_task,
            compaction_config.as_ref(),
            &compact_table_ids,
            safe_epoch_table_watermarks_impl(table_watermarks, &compact_table_ids),
            all_versioned_table_schemas,
        );

        BuiltCompactTask::PendingAssignment(compact_task)
    }

    fn prepare_compact_task_for_assignment(
        &self,
        compact_task: &mut CompactTask,
        compaction_config: &CompactionConfig,
        compact_table_ids: &[TableId],
        table_watermarks: BTreeMap<TableId, TableWatermarks>,
        all_versioned_table_schemas: &HashMap<TableId, Vec<i32>>,
    ) {
        self.calculate_vnode_partition(compact_task, compaction_config, compact_table_ids);
        attach_compact_task_table_metadata(
            compact_task,
            compact_table_ids,
            table_watermarks,
            all_versioned_table_schemas,
        );
    }

    pub fn compactor_manager_ref(&self) -> crate::hummock::CompactorManagerRef {
        self.compactor_manager.clone()
    }

    fn register_compaction_task_report_waiter(
        &self,
        task_id: HummockCompactionTaskId,
    ) -> Receiver<CompactionTaskReportResult> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.compaction_task_report_notifiers
            .lock()
            .register(task_id, tx);
        rx
    }

    fn remove_compaction_task_report_waiter(&self, task_id: HummockCompactionTaskId) {
        self.compaction_task_report_notifiers.lock().remove(task_id);
    }

    fn notify_compaction_task_report_waiters(&self, results: Vec<CompactionTaskReportResult>) {
        let mut guard = self.compaction_task_report_notifiers.lock();
        for result in results {
            guard.notify(result);
        }
    }
}

#[cfg(any(test, feature = "test"))]
impl HummockManager {
    pub async fn compaction_task_from_assignment_for_test(
        &self,
        task_id: u64,
    ) -> Option<CompactTaskAssignment> {
        let compaction_guard = self
            .compaction
            .read_with_process_name("compaction_task_from_assignment_for_test")
            .await;
        let assignment_ref = &compaction_guard.compact_task_assignment;
        assignment_ref.get(&task_id).cloned()
    }

    pub async fn report_compact_task_for_test(
        &self,
        task_id: u64,
        compact_task: Option<CompactTask>,
        task_status: TaskStatus,
        sorted_output_ssts: Vec<SstableInfo>,
        table_stats_change: Option<PbTableStatsMap>,
    ) -> Result<()> {
        if let Some(task) = compact_task {
            let mut guard = self
                .compaction
                .write_with_process_name("report_compact_task_for_test")
                .await;
            guard.compact_task_assignment.insert(
                task_id,
                CompactTaskAssignment {
                    compact_task: task,
                    context_id: 0.into(),
                },
            );
        }

        // In the test, the contents of the compact task may have been modified directly, while the contents of compact_task_assignment were not modified.
        // So we pass the modified compact_task directly into the `report_compact_task_impl`
        self.report_compact_tasks(vec![ReportTask {
            task_id,
            task_status,
            sorted_output_ssts,
            table_stats_change: table_stats_change.unwrap_or_default(),
            object_timestamps: HashMap::default(),
        }])
        .await?;
        Ok(())
    }
}

/// What triggered the compaction schedule request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleTrigger {
    /// New data arrived (e.g., `commit_epoch`). Clears cooldown.
    NewData,
    /// Periodic timer. Respects cooldown for Dynamic type.
    Periodic,
}

/// A point-in-time snapshot of the compaction schedule state.
///
/// `snapshot_time` is used by `unschedule()` to detect whether new data arrived
/// after the snapshot was taken, preventing incorrect cooldown.
pub struct CompactionScheduleSnapshot {
    scheduled: HashSet<(CompactionGroupId, compact_task::TaskType)>,
    snapshot_time: Instant,
}

impl CompactionScheduleSnapshot {
    /// Task type priority order for scheduling (checked first = higher priority).
    const TASK_TYPE_PRIORITY: &[TaskType] = &[
        TaskType::Dynamic,
        TaskType::SpaceReclaim,
        TaskType::Ttl,
        TaskType::Tombstone,
        TaskType::VnodeWatermark,
    ];

    pub fn snapshot_time(&self) -> Instant {
        self.snapshot_time
    }

    /// Pick compaction groups and task type from this snapshot.
    ///
    /// Returns groups in shuffled order. Non-Dynamic types have higher priority
    /// and return a single group; Dynamic groups are batched together.
    pub fn pick_compaction_groups_and_type(&self) -> Option<(Vec<CompactionGroupId>, TaskType)> {
        let group_ids = self.group_ids_shuffled();
        let mut normal_groups = vec![];
        for cg_id in group_ids {
            if let Some(pick_type) = self.pick_type(cg_id) {
                if pick_type == TaskType::Dynamic {
                    normal_groups.push(cg_id);
                } else if normal_groups.is_empty() {
                    return Some((vec![cg_id], pick_type));
                }
            }
        }
        if normal_groups.is_empty() {
            None
        } else {
            Some((normal_groups, TaskType::Dynamic))
        }
    }

    fn group_ids_shuffled(&self) -> Vec<CompactionGroupId> {
        let mut group_ids: Vec<_> = self.scheduled.iter().map(|(g, _)| *g).unique().collect();
        group_ids.shuffle(&mut thread_rng());
        group_ids
    }

    fn pick_type(&self, group: CompactionGroupId) -> Option<TaskType> {
        Self::TASK_TYPE_PRIORITY
            .iter()
            .find(|t| self.scheduled.contains(&(group, **t)))
            .copied()
    }
}

/// Tracks which (`compaction_group`, `task_type`) pairs are scheduled for compaction.
///
/// For `Dynamic` type, includes a cooldown mechanism: groups with no compaction work
/// are skipped by periodic triggers until new data arrives via `commit_epoch`.
#[derive(Debug, Default)]
pub struct CompactionState {
    inner: Mutex<CompactionStateInner>,
}

#[derive(Debug, Default)]
struct CompactionStateInner {
    scheduled: HashSet<(CompactionGroupId, compact_task::TaskType)>,
    /// Groups skipped by periodic Dynamic trigger until new data arrives.
    dynamic_cooldown: HashSet<CompactionGroupId>,
    /// Tracks new-data arrival time per group for cooldown race detection.
    last_new_data_time: HashMap<CompactionGroupId, Instant>,
}

impl CompactionState {
    pub fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }

    /// Enqueues a compaction request. Returns `true` if newly scheduled.
    ///
    /// `trigger` only affects `Dynamic` type — see [`ScheduleTrigger`].
    pub fn try_sched_compaction(
        &self,
        compaction_group: CompactionGroupId,
        task_type: TaskType,
        trigger: ScheduleTrigger,
    ) -> bool {
        let mut guard = self.inner.lock();
        if task_type == TaskType::Dynamic {
            match trigger {
                ScheduleTrigger::NewData => {
                    guard.dynamic_cooldown.remove(&compaction_group);
                    guard
                        .last_new_data_time
                        .insert(compaction_group, Instant::now());
                }
                ScheduleTrigger::Periodic => {
                    if guard.dynamic_cooldown.contains(&compaction_group) {
                        return false;
                    }
                }
            }
        }
        guard.scheduled.insert((compaction_group, task_type))
    }

    /// Removes a scheduled entry. For Dynamic type, adds to cooldown unless
    /// new data arrived after `snapshot_time`.
    pub fn unschedule(
        &self,
        compaction_group: CompactionGroupId,
        task_type: compact_task::TaskType,
        snapshot_time: Instant,
    ) {
        let mut guard = self.inner.lock();
        guard.scheduled.remove(&(compaction_group, task_type));
        if task_type == TaskType::Dynamic {
            let has_new_data = guard
                .last_new_data_time
                .get(&compaction_group)
                .is_some_and(|t| *t > snapshot_time);
            if !has_new_data {
                guard.dynamic_cooldown.insert(compaction_group);
            }
        }
    }

    /// Takes a snapshot of the current schedule state.
    pub fn snapshot(&self) -> CompactionScheduleSnapshot {
        let guard = self.inner.lock();
        // Record time after lock to ensure accurate ordering vs. try_sched_compaction
        let snapshot_time = Instant::now();
        CompactionScheduleSnapshot {
            scheduled: guard.scheduled.clone(),
            snapshot_time,
        }
    }

    /// Removes all schedule state for a deleted or merged group.
    pub fn remove_compaction_group(&self, compaction_group: CompactionGroupId) {
        let mut guard = self.inner.lock();
        guard
            .scheduled
            .retain(|(group, _)| *group != compaction_group);
        guard.dynamic_cooldown.remove(&compaction_group);
        guard.last_new_data_time.remove(&compaction_group);
    }
}

impl Compaction {
    pub fn get_compact_task_assignments_by_group_id(
        &self,
        compaction_group_id: CompactionGroupId,
    ) -> Vec<CompactTaskAssignment> {
        self.compact_task_assignment
            .values()
            .filter_map(|assignment| {
                if assignment.compact_task.compaction_group_id == compaction_group_id {
                    Some(assignment.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

#[derive(Clone, Default)]
pub struct CompactionGroupStatistic {
    pub group_id: CompactionGroupId,
    pub group_size: u64,
    pub table_statistic: BTreeMap<StateTableId, u64>,
    pub compaction_group_config: CompactionGroup,
}

/// Updates table stats caused by vnode watermark trivial reclaim compaction.
fn update_table_stats_for_vnode_watermark_trivial_reclaim(
    table_stats: &mut PbTableStatsMap,
    task: &CompactTask,
) {
    if task.task_type != TaskType::VnodeWatermark {
        return;
    }
    let mut deleted_table_keys: HashMap<TableId, u64> = HashMap::default();
    for s in task.input_ssts.iter().flat_map(|l| l.table_infos.iter()) {
        assert_eq!(s.table_ids.len(), 1);
        let e = deleted_table_keys.entry(s.table_ids[0]).or_insert(0);
        *e += s.total_key_count;
    }
    for (table_id, delete_count) in deleted_table_keys {
        let Some(stats) = table_stats.get_mut(&table_id) else {
            continue;
        };
        if stats.total_key_count == 0 {
            continue;
        }
        let new_total_key_count = stats.total_key_count.saturating_sub(delete_count as i64);
        let ratio = new_total_key_count as f64 / stats.total_key_count as f64;
        // total_key_count is updated accurately.
        stats.total_key_count = new_total_key_count;
        // others are updated approximately.
        stats.total_key_size = (stats.total_key_size as f64 * ratio).ceil() as i64;
        stats.total_value_size = (stats.total_value_size as f64 * ratio).ceil() as i64;
    }
}

#[derive(Debug, Clone)]
pub enum GroupState {
    /// The compaction group is not in emergency state.
    Normal,

    /// The compaction group is in emergency state.
    Emergency(String), // reason

    /// The compaction group is in write stop state.
    WriteStop(String), // reason
}

impl GroupState {
    pub fn is_write_stop(&self) -> bool {
        matches!(self, Self::WriteStop(_))
    }

    pub fn is_emergency(&self) -> bool {
        matches!(self, Self::Emergency(_))
    }

    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Emergency(reason) | Self::WriteStop(reason) => Some(reason),
            _ => None,
        }
    }
}

#[derive(Clone, Default)]
pub struct GroupStateValidator;

impl GroupStateValidator {
    pub fn write_stop_sub_level_count(
        level_count: usize,
        compaction_config: &CompactionConfig,
    ) -> bool {
        let threshold = compaction_config.level0_stop_write_threshold_sub_level_number as usize;
        level_count > threshold
    }

    pub fn write_stop_l0_size(l0_size: u64, compaction_config: &CompactionConfig) -> bool {
        l0_size
            > compaction_config
                .level0_stop_write_threshold_max_size
                .unwrap_or(compaction_config::level0_stop_write_threshold_max_size())
    }

    pub fn write_stop_l0_file_count(
        l0_file_count: usize,
        compaction_config: &CompactionConfig,
    ) -> bool {
        l0_file_count
            > compaction_config
                .level0_stop_write_threshold_max_sst_count
                .unwrap_or(compaction_config::level0_stop_write_threshold_max_sst_count())
                as usize
    }

    pub fn emergency_l0_file_count(
        l0_file_count: usize,
        compaction_config: &CompactionConfig,
    ) -> bool {
        l0_file_count
            > compaction_config
                .emergency_level0_sst_file_count
                .unwrap_or(compaction_config::emergency_level0_sst_file_count())
                as usize
    }

    pub fn emergency_l0_partition_count(
        last_l0_sub_level_partition_count: usize,
        compaction_config: &CompactionConfig,
    ) -> bool {
        last_l0_sub_level_partition_count
            > compaction_config
                .emergency_level0_sub_level_partition
                .unwrap_or(compaction_config::emergency_level0_sub_level_partition())
                as usize
    }

    pub fn check_single_group_write_stop(
        levels: &Levels,
        compaction_config: &CompactionConfig,
    ) -> GroupState {
        if Self::write_stop_sub_level_count(levels.l0.sub_levels.len(), compaction_config) {
            return GroupState::WriteStop(format!(
                "WriteStop(l0_level_count: {}, threshold: {}) too many L0 sub levels",
                levels.l0.sub_levels.len(),
                compaction_config.level0_stop_write_threshold_sub_level_number
            ));
        }

        if Self::write_stop_l0_file_count(
            levels
                .l0
                .sub_levels
                .iter()
                .map(|l| l.table_infos.len())
                .sum(),
            compaction_config,
        ) {
            return GroupState::WriteStop(format!(
                "WriteStop(l0_sst_count: {}, threshold: {}) too many L0 sst files",
                levels
                    .l0
                    .sub_levels
                    .iter()
                    .map(|l| l.table_infos.len())
                    .sum::<usize>(),
                compaction_config
                    .level0_stop_write_threshold_max_sst_count
                    .unwrap_or(compaction_config::level0_stop_write_threshold_max_sst_count())
            ));
        }

        if Self::write_stop_l0_size(levels.l0.total_file_size, compaction_config) {
            return GroupState::WriteStop(format!(
                "WriteStop(l0_size: {}, threshold: {}) too large L0 size",
                levels.l0.total_file_size,
                compaction_config
                    .level0_stop_write_threshold_max_size
                    .unwrap_or(compaction_config::level0_stop_write_threshold_max_size())
            ));
        }

        GroupState::Normal
    }

    pub fn check_single_group_emergency(
        levels: &Levels,
        compaction_config: &CompactionConfig,
    ) -> GroupState {
        if Self::emergency_l0_file_count(
            levels
                .l0
                .sub_levels
                .iter()
                .map(|l| l.table_infos.len())
                .sum(),
            compaction_config,
        ) {
            return GroupState::Emergency(format!(
                "Emergency(l0_sst_count: {}, threshold: {}) too many L0 sst files",
                levels
                    .l0
                    .sub_levels
                    .iter()
                    .map(|l| l.table_infos.len())
                    .sum::<usize>(),
                compaction_config
                    .emergency_level0_sst_file_count
                    .unwrap_or(compaction_config::emergency_level0_sst_file_count())
            ));
        }

        if Self::emergency_l0_partition_count(
            levels
                .l0
                .sub_levels
                .first()
                .map(|l| l.table_infos.len())
                .unwrap_or(0),
            compaction_config,
        ) {
            return GroupState::Emergency(format!(
                "Emergency(l0_partition_count: {}, threshold: {}) too many L0 partitions",
                levels
                    .l0
                    .sub_levels
                    .first()
                    .map(|l| l.table_infos.len())
                    .unwrap_or(0),
                compaction_config
                    .emergency_level0_sub_level_partition
                    .unwrap_or(compaction_config::emergency_level0_sub_level_partition())
            ));
        }

        GroupState::Normal
    }

    pub fn group_state(levels: &Levels, compaction_config: &CompactionConfig) -> GroupState {
        let state = Self::check_single_group_write_stop(levels, compaction_config);
        if state.is_write_stop() {
            return state;
        }

        Self::check_single_group_emergency(levels, compaction_config)
    }
}

#[cfg(test)]
mod compaction_state_tests {
    use risingwave_pb::hummock::compact_task::TaskType;

    use super::*;

    #[test]
    fn test_basic_schedule_and_unschedule() {
        let state = CompactionState::new();
        let group_id: CompactionGroupId = 1.into();

        // First schedule should succeed
        assert!(state.try_sched_compaction(group_id, TaskType::Dynamic, ScheduleTrigger::NewData));
        // Duplicate schedule should fail
        assert!(!state.try_sched_compaction(group_id, TaskType::Dynamic, ScheduleTrigger::NewData));
        // Different task type should succeed
        assert!(state.try_sched_compaction(group_id, TaskType::Ttl, ScheduleTrigger::Periodic));

        // Snapshot should contain both
        let snapshot = state.snapshot();
        assert!(snapshot.scheduled.contains(&(group_id, TaskType::Dynamic)));
        assert!(snapshot.scheduled.contains(&(group_id, TaskType::Ttl)));

        // Unschedule removes from scheduled set
        state.unschedule(group_id, TaskType::Dynamic, snapshot.snapshot_time());
        let snapshot2 = state.snapshot();
        assert!(!snapshot2.scheduled.contains(&(group_id, TaskType::Dynamic)));
        assert!(snapshot2.scheduled.contains(&(group_id, TaskType::Ttl)));
    }

    #[test]
    fn test_cooldown_blocks_periodic_trigger() {
        let state = CompactionState::new();
        let group_id: CompactionGroupId = 1.into();

        // Schedule then unschedule - should add to cooldown
        assert!(state.try_sched_compaction(group_id, TaskType::Dynamic, ScheduleTrigger::NewData));
        let snapshot = state.snapshot();
        state.unschedule(group_id, TaskType::Dynamic, snapshot.snapshot_time());

        // Verify in cooldown
        assert!(state.inner.lock().dynamic_cooldown.contains(&group_id));

        // Periodic trigger should be blocked
        assert!(!state.try_sched_compaction(
            group_id,
            TaskType::Dynamic,
            ScheduleTrigger::Periodic
        ));
    }

    #[test]
    fn test_new_data_clears_cooldown() {
        let state = CompactionState::new();
        let group_id: CompactionGroupId = 1.into();

        // Put group in cooldown
        assert!(state.try_sched_compaction(group_id, TaskType::Dynamic, ScheduleTrigger::NewData));
        let snapshot = state.snapshot();
        state.unschedule(group_id, TaskType::Dynamic, snapshot.snapshot_time());
        assert!(state.inner.lock().dynamic_cooldown.contains(&group_id));

        // NewData trigger should clear cooldown and schedule
        assert!(state.try_sched_compaction(group_id, TaskType::Dynamic, ScheduleTrigger::NewData));
        assert!(!state.inner.lock().dynamic_cooldown.contains(&group_id));
    }

    #[test]
    fn test_cooldown_only_affects_dynamic_type() {
        let state = CompactionState::new();
        let group_id: CompactionGroupId = 1.into();

        // Put group in cooldown for Dynamic
        assert!(state.try_sched_compaction(group_id, TaskType::Dynamic, ScheduleTrigger::NewData));
        let snapshot = state.snapshot();
        state.unschedule(group_id, TaskType::Dynamic, snapshot.snapshot_time());

        // Ttl unschedule should NOT add to cooldown
        let group_id_2: CompactionGroupId = 2.into();
        assert!(state.try_sched_compaction(group_id_2, TaskType::Ttl, ScheduleTrigger::Periodic));
        let snapshot2 = state.snapshot();
        state.unschedule(group_id_2, TaskType::Ttl, snapshot2.snapshot_time());
        assert!(!state.inner.lock().dynamic_cooldown.contains(&group_id_2));

        // Other task types should work regardless of cooldown
        assert!(state.try_sched_compaction(group_id, TaskType::Ttl, ScheduleTrigger::Periodic));
        assert!(state.try_sched_compaction(
            group_id,
            TaskType::SpaceReclaim,
            ScheduleTrigger::Periodic
        ));
    }

    #[test]
    fn test_race_condition_new_data_after_snapshot() {
        let state = CompactionState::new();
        let group_id: CompactionGroupId = 1.into();

        assert!(state.try_sched_compaction(group_id, TaskType::Dynamic, ScheduleTrigger::NewData));
        let snapshot = state.snapshot();

        // Simulate new data arriving AFTER snapshot
        {
            let mut guard = state.inner.lock();
            guard.last_new_data_time.insert(group_id, Instant::now());
        }

        // Unschedule should NOT add to cooldown (new data arrived after snapshot)
        state.unschedule(group_id, TaskType::Dynamic, snapshot.snapshot_time());
        assert!(
            !state.inner.lock().dynamic_cooldown.contains(&group_id),
            "Should skip cooldown when new data arrived after snapshot"
        );
    }

    #[test]
    fn test_remove_compaction_group_cleans_all_state() {
        let state = CompactionState::new();
        let group_id: CompactionGroupId = 1.into();

        // Set up state
        assert!(state.try_sched_compaction(group_id, TaskType::Dynamic, ScheduleTrigger::NewData));
        assert!(state.try_sched_compaction(group_id, TaskType::Ttl, ScheduleTrigger::Periodic));
        state.inner.lock().dynamic_cooldown.insert(group_id);

        // Remove group
        state.remove_compaction_group(group_id);

        // Verify all state cleaned up
        let guard = state.inner.lock();
        assert!(!guard.scheduled.contains(&(group_id, TaskType::Dynamic)));
        assert!(!guard.scheduled.contains(&(group_id, TaskType::Ttl)));
        assert!(!guard.dynamic_cooldown.contains(&group_id));
        assert!(!guard.last_new_data_time.contains_key(&group_id));
    }

    #[test]
    fn test_snapshot_pick_type_priority() {
        let state = CompactionState::new();
        let group_id: CompactionGroupId = 1.into();

        // Empty group returns None
        assert_eq!(state.snapshot().pick_type(group_id), None);

        // Priority order: Dynamic > SpaceReclaim > Ttl > Tombstone > VnodeWatermark
        state.try_sched_compaction(
            group_id,
            TaskType::VnodeWatermark,
            ScheduleTrigger::Periodic,
        );
        assert_eq!(
            state.snapshot().pick_type(group_id),
            Some(TaskType::VnodeWatermark)
        );

        state.try_sched_compaction(group_id, TaskType::Tombstone, ScheduleTrigger::Periodic);
        assert_eq!(
            state.snapshot().pick_type(group_id),
            Some(TaskType::Tombstone)
        );

        state.try_sched_compaction(group_id, TaskType::Ttl, ScheduleTrigger::Periodic);
        assert_eq!(state.snapshot().pick_type(group_id), Some(TaskType::Ttl));

        state.try_sched_compaction(group_id, TaskType::SpaceReclaim, ScheduleTrigger::Periodic);
        assert_eq!(
            state.snapshot().pick_type(group_id),
            Some(TaskType::SpaceReclaim)
        );

        state.try_sched_compaction(group_id, TaskType::Dynamic, ScheduleTrigger::NewData);
        assert_eq!(
            state.snapshot().pick_type(group_id),
            Some(TaskType::Dynamic)
        );
    }

    #[test]
    fn test_multiple_groups_independent_cooldown() {
        let state = CompactionState::new();
        let g1: CompactionGroupId = 1.into();
        let g2: CompactionGroupId = 2.into();

        state.try_sched_compaction(g1, TaskType::Dynamic, ScheduleTrigger::NewData);
        state.try_sched_compaction(g2, TaskType::Dynamic, ScheduleTrigger::NewData);
        let snapshot = state.snapshot();

        // Only unschedule g1
        state.unschedule(g1, TaskType::Dynamic, snapshot.snapshot_time());

        let guard = state.inner.lock();
        assert!(guard.dynamic_cooldown.contains(&g1));
        assert!(!guard.dynamic_cooldown.contains(&g2));
    }

    #[test]
    fn test_pick_compaction_groups_empty() {
        let state = CompactionState::new();
        let snapshot = state.snapshot();
        // No scheduled groups → returns None
        assert!(snapshot.pick_compaction_groups_and_type().is_none());
    }

    #[test]
    fn test_pick_compaction_groups_mixed_types() {
        let state = CompactionState::new();
        let g1: CompactionGroupId = 1.into();
        let g2: CompactionGroupId = 2.into();
        let g3: CompactionGroupId = 3.into();

        // g1: Dynamic, g2: Ttl, g3: Dynamic
        state.try_sched_compaction(g1, TaskType::Dynamic, ScheduleTrigger::NewData);
        state.try_sched_compaction(g2, TaskType::Ttl, ScheduleTrigger::Periodic);
        state.try_sched_compaction(g3, TaskType::Dynamic, ScheduleTrigger::NewData);

        let snapshot = state.snapshot();
        let (groups, task_type) = snapshot.pick_compaction_groups_and_type().unwrap();

        // Due to shuffle, either:
        // - Ttl group is encountered first → returns (vec![g2], Ttl)
        // - Dynamic group is encountered first → collects all Dynamic, skips Ttl
        //   → returns ([g1, g3] in some order, Dynamic)
        if task_type == TaskType::Dynamic {
            assert!(groups.contains(&g1));
            assert!(groups.contains(&g3));
            assert!(!groups.contains(&g2)); // Ttl group excluded from Dynamic result
        } else {
            assert_eq!(task_type, TaskType::Ttl);
            assert_eq!(groups, vec![g2]);
        }
    }

    #[test]
    fn test_pick_compaction_groups_all_dynamic() {
        let state = CompactionState::new();
        let g1: CompactionGroupId = 1.into();
        let g2: CompactionGroupId = 2.into();

        state.try_sched_compaction(g1, TaskType::Dynamic, ScheduleTrigger::NewData);
        state.try_sched_compaction(g2, TaskType::Dynamic, ScheduleTrigger::NewData);

        let snapshot = state.snapshot();
        let (groups, task_type) = snapshot.pick_compaction_groups_and_type().unwrap();
        assert_eq!(task_type, TaskType::Dynamic);
        assert!(groups.contains(&g1));
        assert!(groups.contains(&g2));
    }

    #[test]
    fn test_pick_compaction_groups_single_non_dynamic() {
        let state = CompactionState::new();
        let g1: CompactionGroupId = 1.into();

        state.try_sched_compaction(g1, TaskType::SpaceReclaim, ScheduleTrigger::Periodic);

        let snapshot = state.snapshot();
        let (groups, task_type) = snapshot.pick_compaction_groups_and_type().unwrap();
        assert_eq!(task_type, TaskType::SpaceReclaim);
        assert_eq!(groups, vec![g1]);
    }
}
