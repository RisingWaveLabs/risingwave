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

#![cfg(test)]

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use prometheus::Registry;
use prometheus::core::Collector;
use prometheus::proto::MetricFamily;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_common::id::JobId;
use risingwave_common::util::epoch::{EpochExt, test_epoch};
use risingwave_hummock_sdk::change_log::{EpochNewChangeLog, TableChangeLog};
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::get_compaction_group_ssts;
use risingwave_hummock_sdk::key::{FullKey, gen_key_from_str};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::level::Level;
use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
use risingwave_hummock_sdk::table_stats::{TableStats, TableStatsMap, to_prost_table_stats_map};
use risingwave_hummock_sdk::version::{
    HummockVersion, HummockVersionStateTableInfo, MAX_HUMMOCK_VERSION_ID,
};
use risingwave_hummock_sdk::{
    CompactionGroupId, FIRST_VERSION_ID, HummockEpoch, HummockObjectId, HummockSstableId,
    HummockSstableObjectId, LocalSstableInfo, SyncResult,
};
use risingwave_pb::common::worker_node::Property;
use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::mutable_config::MutableConfig;
use risingwave_pb::hummock::{HummockPinnedVersion, PbLevelType, StateTableInfo};
use risingwave_rpc_client::HummockMetaClient;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
use thiserror_ext::AsReport;

use crate::controller::catalog::CatalogController;
use crate::controller::cluster::ClusterController;
use crate::controller::streaming_job::{IndependentJobChangeLogInfo, TableChangeLogTruncateInfo};
use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
use crate::hummock::compaction::selector::{ManualCompactionOption, default_compaction_selector};
use crate::hummock::error::Error;
use crate::hummock::manager::checkpoint::HummockVersionCheckpoint;
use crate::hummock::model::ext::to_table_change_log_meta_store_model;
use crate::hummock::table_write_throughput_statistic::TableWriteThroughputStatisticManager;
use crate::hummock::test_utils::*;
use crate::hummock::{CompactorManager, HummockManager, HummockManagerRef, MockHummockMetaClient};
use crate::manager::{MetaOpts, MetaSrvEnv};
use crate::rpc::metrics::MetaMetrics;

pub fn version_max_committed_epoch(version: &HummockVersion) -> u64 {
    let committed_epoch = version
        .state_table_info
        .info()
        .values()
        .next()
        .unwrap()
        .committed_epoch;
    assert!(
        version
            .state_table_info
            .info()
            .values()
            .all(|info| info.committed_epoch == committed_epoch),
        "info: {:?}",
        version.state_table_info.info()
    );
    committed_epoch
}

fn pin_versions_sum(pin_versions: &[HummockPinnedVersion]) -> usize {
    pin_versions.iter().len()
}

fn gen_sstable_info(sst_id: u64, table_ids: Vec<u32>, epoch: u64) -> SstableInfo {
    gen_sstable_info_impl(sst_id, table_ids, epoch).into()
}

fn metric_has_label_value(
    metric_families: &[MetricFamily],
    label_name: &str,
    label_value: &str,
) -> bool {
    metric_families.iter().any(|metric_family| {
        metric_family.get_metric().iter().any(|metric| {
            metric
                .get_label()
                .iter()
                .any(|label| label.name() == label_name && label.value() == label_value)
        })
    })
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
        FullKey::for_test(TableId::new(*table_ids.last().unwrap()), table_key_r, epoch).encode();

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

fn gen_local_sstable_info(sst_id: u64, table_ids: Vec<u32>, epoch: u64) -> LocalSstableInfo {
    LocalSstableInfo {
        sst_info: gen_sstable_info(sst_id, table_ids, epoch),
        table_stats: Default::default(),
        created_at: u64::MAX,
    }
}

#[tokio::test]
async fn test_get_table_change_logs_with_inverted_epoch_range() {
    let (_env, hummock_manager, _, _) = setup_compute_env(80).await;
    let table_id = TableId::new(1);
    hummock_manager
        .versioning
        .write_with_process_name("test_get_table_change_logs_with_inverted_epoch_range")
        .await
        .table_change_log
        .insert(
            table_id,
            TableChangeLog::new([
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    non_checkpoint_epochs: vec![3],
                    checkpoint_epoch: 4,
                },
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    non_checkpoint_epochs: vec![],
                    checkpoint_epoch: 6,
                },
            ]),
        );

    let error = hummock_manager
        .get_table_change_logs(
            true,
            Some(7),
            Some(5),
            Some(HashSet::from([table_id])),
            false,
            None,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        Error::InvalidEpochRange {
            start_epoch: 7,
            end_epoch: 5
        }
    ));
}

#[tokio::test]
async fn test_truncate_table_change_log_persisted_and_in_memory() {
    let (env, hummock_manager, _, _) = setup_compute_env(80).await;
    let table_id = TableId::new(1);
    let state_table_id = TableId::new(2);
    let old_epoch = test_epoch(10_000);
    let truncate_epoch = test_epoch(20_000);
    let committed_epoch = test_epoch(30_000);
    let change_logs = [
        (old_epoch, vec![1], vec![2]),
        (truncate_epoch, vec![3], vec![4]),
        (committed_epoch, vec![5], vec![6]),
    ]
    .map(
        |(checkpoint_epoch, new_object_ids, old_object_ids)| EpochNewChangeLog {
            new_value: new_object_ids
                .into_iter()
                .map(|object_id| {
                    gen_sstable_info(object_id, vec![table_id.as_raw_id()], checkpoint_epoch)
                })
                .collect(),
            old_value: old_object_ids
                .into_iter()
                .map(|object_id| {
                    gen_sstable_info(object_id, vec![table_id.as_raw_id()], checkpoint_epoch)
                })
                .collect(),
            non_checkpoint_epochs: vec![],
            checkpoint_epoch,
        },
    );

    {
        let mut versioning = hummock_manager.versioning.write().await;
        let mut version = versioning.current_version.as_ref().clone();
        let mut delta = version.version_delta_after();
        delta.state_table_info_delta.insert(
            table_id,
            risingwave_pb::hummock::StateTableInfoDelta {
                committed_epoch,
                compaction_group_id: 1.into(),
            },
        );
        delta.state_table_info_delta.insert(
            state_table_id,
            risingwave_pb::hummock::StateTableInfoDelta {
                committed_epoch: truncate_epoch,
                compaction_group_id: 1.into(),
            },
        );
        version.apply_version_delta(&delta);
        versioning.current_version = Arc::new(version);
        versioning
            .table_change_log
            .insert(table_id, TableChangeLog::new(change_logs.clone()));
    }
    risingwave_meta_model::hummock_table_change_log::Entity::insert_many(
        change_logs
            .iter()
            .map(|change_log| to_table_change_log_meta_store_model(table_id, change_log)),
    )
    .exec(&env.meta_store_ref().conn)
    .await
    .unwrap();

    hummock_manager
        .truncate_table_change_log(TableChangeLogTruncateInfo {
            subscription_retention_seconds: HashMap::new(),
            independent_jobs: vec![IndependentJobChangeLogInfo {
                job_id: JobId::new(3),
                state_table_ids: HashSet::from([state_table_id]),
                upstream_table_snapshot_epochs: HashMap::from([(table_id, Some(truncate_epoch))]),
            }],
        })
        .await
        .unwrap();

    let persisted_epochs: Vec<risingwave_meta_model::Epoch> =
        risingwave_meta_model::hummock_table_change_log::Entity::find()
            .filter(risingwave_meta_model::hummock_table_change_log::Column::TableId.eq(table_id))
            .select_only()
            .column(risingwave_meta_model::hummock_table_change_log::Column::CheckpointEpoch)
            .order_by_asc(risingwave_meta_model::hummock_table_change_log::Column::CheckpointEpoch)
            .into_tuple()
            .all(&env.meta_store_ref().conn)
            .await
            .unwrap();
    assert_eq!(
        persisted_epochs,
        vec![
            truncate_epoch as risingwave_meta_model::Epoch,
            committed_epoch as risingwave_meta_model::Epoch,
        ]
    );
    let in_memory_epochs = hummock_manager.versioning.read().await.table_change_log[&table_id]
        .epochs()
        .collect_vec();
    assert_eq!(in_memory_epochs, vec![truncate_epoch, committed_epoch]);
    assert_eq!(
        hummock_manager
            .gc_manager
            .try_take_may_delete_object_ids(1)
            .unwrap(),
        HashSet::from([
            HummockObjectId::Sstable(1.into()),
            HummockObjectId::Sstable(2.into()),
        ])
    );
}
fn get_compaction_group_object_ids(
    version: &HummockVersion,
    group_id: impl Into<CompactionGroupId>,
) -> Vec<HummockSstableObjectId> {
    get_compaction_group_ssts(version, group_id.into())
        .map(|(object_id, _)| object_id)
        .collect_vec()
}

async fn list_pinned_version_from_meta_store(env: &MetaSrvEnv) -> Vec<HummockPinnedVersion> {
    use risingwave_meta_model::hummock_pinned_version;
    use sea_orm::EntityTrait;
    hummock_pinned_version::Entity::find()
        .all(&env.meta_store_ref().conn)
        .await
        .unwrap()
        .into_iter()
        .map(Into::into)
        .collect()
}

async fn setup_compute_env_with_meta_opts(
    port: i32,
    opts: MetaOpts,
) -> (
    MetaSrvEnv,
    HummockManagerRef,
    crate::controller::cluster::ClusterControllerRef,
    risingwave_meta_model::WorkerId,
) {
    let config = CompactionConfigBuilder::new()
        .level0_tier_compact_file_number(1)
        .level0_max_compact_file_number(130)
        .level0_sub_level_compact_level_count(1)
        .level0_overlapping_sub_level_compact_level_count(1)
        .build();
    let env = MetaSrvEnv::for_test_opts(opts, |_| ()).await;
    let cluster_ctl = Arc::new(
        ClusterController::new(env.clone(), Duration::from_secs(1))
            .await
            .unwrap(),
    );
    let catalog_ctl = Arc::new(CatalogController::new(env.clone()).await.unwrap());
    let compactor_manager = Arc::new(CompactorManager::for_test());
    let (compactor_streams_change_tx, _compactor_streams_change_rx) =
        tokio::sync::mpsc::unbounded_channel();
    let hummock_manager = HummockManager::with_config(
        env.clone(),
        cluster_ctl.clone(),
        catalog_ctl,
        Arc::new(MetaMetrics::default()),
        compactor_manager,
        config,
        compactor_streams_change_tx,
    )
    .await;

    let worker_id = cluster_ctl
        .add_worker(
            WorkerType::ComputeNode,
            HostAddress {
                host: "127.0.0.1".to_owned(),
                port,
            },
            Property {
                is_streaming: true,
                is_serving: true,
                parallelism: 4,
                ..Default::default()
            },
            Default::default(),
        )
        .await
        .unwrap();
    (env, hummock_manager, cluster_ctl, worker_id)
}

async fn build_hummock_manager_for_test_env(env: MetaSrvEnv) -> HummockManagerRef {
    let config = CompactionConfigBuilder::new()
        .level0_tier_compact_file_number(1)
        .level0_max_compact_file_number(130)
        .level0_sub_level_compact_level_count(2)
        .level0_overlapping_sub_level_compact_level_count(1)
        .build();
    let cluster_ctl = Arc::new(
        ClusterController::new(env.clone(), Duration::from_secs(1))
            .await
            .unwrap(),
    );
    let catalog_ctl = Arc::new(CatalogController::new(env.clone()).await.unwrap());
    let compactor_manager = Arc::new(CompactorManager::for_test());
    let (compactor_streams_change_tx, _compactor_streams_change_rx) =
        tokio::sync::mpsc::unbounded_channel();
    HummockManager::with_config(
        env,
        cluster_ctl,
        catalog_ctl,
        Arc::new(MetaMetrics::default()),
        compactor_manager,
        config,
        compactor_streams_change_tx,
    )
    .await
}

fn compaction_group_ranges(version: &HummockVersion) -> Vec<(u32, u32)> {
    let mut ranges = version
        .levels
        .keys()
        .filter_map(|group_id| {
            let members = version
                .state_table_info
                .compaction_group_member_table_ids(*group_id)
                .iter()
                .map(|table_id| table_id.as_raw_id())
                .collect_vec();
            (!members.is_empty()).then(|| (*members.first().unwrap(), *members.last().unwrap()))
        })
        .collect_vec();
    ranges.sort_by_key(|(min_table_id, _)| *min_table_id);
    ranges
}

fn assert_compaction_groups_non_overlapping(version: &HummockVersion) {
    let ranges = compaction_group_ranges(version);
    assert!(ranges.windows(2).all(|window| window[0].1 < window[1].0));
}

fn has_overlapping_compaction_groups(version: &HummockVersion) -> bool {
    let ranges = compaction_group_ranges(version);
    ranges.windows(2).any(|window| window[0].1 >= window[1].0)
}

fn all_group_member_table_ids(version: &HummockVersion) -> Vec<u32> {
    version
        .levels
        .keys()
        .flat_map(|group_id| {
            version
                .state_table_info
                .compaction_group_member_table_ids(*group_id)
                .iter()
                .map(|table_id| table_id.as_raw_id())
                .collect_vec()
        })
        .sorted()
        .collect_vec()
}

fn non_empty_compaction_group_count(version: &HummockVersion) -> usize {
    version
        .levels
        .keys()
        .filter(|group_id| {
            !version
                .state_table_info
                .compaction_group_member_table_ids(**group_id)
                .is_empty()
        })
        .count()
}

#[tokio::test]
async fn test_hummock_compaction_task() {
    let (_, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let sst_num = 2;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    // No compaction task available.
    assert!(
        hummock_manager
            .get_compact_task(
                StaticCompactionGroupId::StateDefault,
                &mut *default_compaction_selector(),
            )
            .await
            .unwrap()
            .is_none()
    );

    // Add some sstables and commit.
    let epoch = test_epoch(1);
    let table_id = 1;
    let original_tables = generate_test_sstables_with_table_id(
        epoch,
        table_id,
        get_sst_ids(&hummock_manager, sst_num).await,
    );
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &original_tables,
        StaticCompactionGroupId::StateDefault,
    )
    .await;
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&original_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Get a compaction task.
    let compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), table_id).await;
    let compact_task = hummock_manager
        .get_compact_task(compaction_group_id, &mut *default_compaction_selector())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(compact_task.input_ssts.first().unwrap().level_idx, 0);
    assert_eq!(compact_task.task_id, 2);

    // Cancel the task and succeed.
    assert!(
        hummock_manager
            .cancel_compact_task(compact_task.task_id, TaskStatus::ManualCanceled)
            .await
            .unwrap()
    );

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(compaction_group_id, &mut *default_compaction_selector())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(compact_task.task_id, 3);
    // Finish the task and succeed.

    assert!(
        hummock_manager
            .report_compact_task(
                compact_task.task_id,
                TaskStatus::Success,
                vec![],
                None,
                HashMap::default()
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_hummock_table() {
    let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    let epoch = test_epoch(1);
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &original_tables,
        StaticCompactionGroupId::StateDefault,
    )
    .await;
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&original_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let compaction_group_id = StaticCompactionGroupId::StateDefault;
    let pinned_version = hummock_manager.get_current_version().await;
    let levels = pinned_version.get_compaction_group_levels(compaction_group_id);
    assert_eq!(
        Ordering::Equal,
        levels
            .l0
            .sub_levels
            .iter()
            .chain(levels.levels.iter())
            .flat_map(|level| level.table_infos.iter())
            .map(|info| info.object_id)
            .sorted()
            .cmp(original_tables.iter().map(|ot| ot.object_id).sorted())
    );

    // Confirm tables got are equal to original tables
    assert_eq!(
        get_sorted_object_ids(&original_tables),
        get_sorted_committed_object_ids(&pinned_version, compaction_group_id)
    );
}

#[tokio::test]
async fn test_hummock_transaction() {
    let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let mut committed_tables = vec![];
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    // Add and commit tables in epoch1.
    // BEFORE:  committed_epochs = []
    // AFTER:   committed_epochs = [epoch1]
    let epoch1 = test_epoch(1);
    {
        // Add tables in epoch1
        let tables_in_epoch1 = generate_test_tables(epoch1, get_sst_ids(&hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            &hummock_manager,
            &tables_in_epoch1,
            StaticCompactionGroupId::StateDefault,
        )
        .await;
        // Get tables before committing epoch1. No tables should be returned.
        let current_version = hummock_manager.get_current_version().await;
        let compaction_group_id = StaticCompactionGroupId::StateDefault;
        assert!(get_sorted_committed_object_ids(&current_version, compaction_group_id).is_empty());

        // Commit epoch1
        hummock_meta_client
            .commit_epoch(
                epoch1,
                SyncResult {
                    uncommitted_ssts: to_local_sstable_info(&tables_in_epoch1),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        committed_tables.extend(tables_in_epoch1.clone());

        // Get tables after committing epoch1. All tables committed in epoch1 should be returned
        let current_version = hummock_manager.get_current_version().await;
        let compaction_group_id = StaticCompactionGroupId::StateDefault;
        assert_eq!(version_max_committed_epoch(&current_version), epoch1);
        assert_eq!(
            get_sorted_object_ids(&committed_tables),
            get_sorted_committed_object_ids(&current_version, compaction_group_id)
        );
    }

    // Add and commit tables in epoch2.
    // BEFORE:  committed_epochs = [epoch1]
    // AFTER:   committed_epochs = [epoch1, epoch2]
    let epoch2 = epoch1.next_epoch();
    {
        // Add tables in epoch2
        let tables_in_epoch2 = generate_test_tables(epoch2, get_sst_ids(&hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            &hummock_manager,
            &tables_in_epoch2,
            StaticCompactionGroupId::StateDefault,
        )
        .await;
        // Get tables before committing epoch2. tables_in_epoch1 should be returned and
        // tables_in_epoch2 should be invisible.
        let current_version = hummock_manager.get_current_version().await;
        let compaction_group_id = StaticCompactionGroupId::StateDefault;
        assert_eq!(version_max_committed_epoch(&current_version), epoch1);
        assert_eq!(
            get_sorted_object_ids(&committed_tables),
            get_sorted_committed_object_ids(&current_version, compaction_group_id)
        );

        // Commit epoch2
        hummock_meta_client
            .commit_epoch(
                epoch2,
                SyncResult {
                    uncommitted_ssts: to_local_sstable_info(&tables_in_epoch2),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        committed_tables.extend(tables_in_epoch2);

        // Get tables after committing epoch2. tables_in_epoch1 and tables_in_epoch2 should be
        // returned
        let current_version = hummock_manager.get_current_version().await;
        let compaction_group_id = StaticCompactionGroupId::StateDefault;
        assert_eq!(version_max_committed_epoch(&current_version), epoch2);
        assert_eq!(
            get_sorted_object_ids(&committed_tables),
            get_sorted_committed_object_ids(&current_version, compaction_group_id)
        );
    }
}

#[tokio::test]
async fn test_release_context_resource() {
    let (env, hummock_manager, cluster_ctl, worker_id) = setup_compute_env(1).await;
    let context_id_1 = worker_id as _;

    let fake_host_address_2 = HostAddress {
        host: "127.0.0.1".to_owned(),
        port: 2,
    };
    let fake_parallelism = 4;
    let worker_node_2 = cluster_ctl
        .add_worker(
            WorkerType::ComputeNode,
            fake_host_address_2,
            Property {
                parallelism: fake_parallelism,
                is_streaming: true,
                is_serving: true,
                ..Default::default()
            },
            Default::default(),
        )
        .await
        .unwrap();
    let context_id_2 = worker_node_2 as _;

    assert_eq!(
        pin_versions_sum(&list_pinned_version_from_meta_store(&env).await),
        0
    );
    hummock_manager.pin_version(context_id_1).await.unwrap();
    hummock_manager.pin_version(context_id_2).await.unwrap();
    assert_eq!(
        pin_versions_sum(&list_pinned_version_from_meta_store(&env).await),
        2
    );
    assert_eq!(list_pinned_version_from_meta_store(&env).await.len(), 2);
    hummock_manager
        .release_contexts(&vec![context_id_1])
        .await
        .unwrap();
    let pinned_versions = list_pinned_version_from_meta_store(&env).await;
    assert_eq!(pin_versions_sum(&pinned_versions), 1);
    assert_eq!(pinned_versions[0].context_id, context_id_2);
    // it's OK to call again
    hummock_manager
        .release_contexts(&vec![context_id_1])
        .await
        .unwrap();
    hummock_manager
        .release_contexts(&vec![context_id_2])
        .await
        .unwrap();
    assert_eq!(
        pin_versions_sum(&list_pinned_version_from_meta_store(&env).await),
        0
    );
}

#[tokio::test]
async fn test_context_id_validation() {
    let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let invalid_context_id = 233.into();
    let context_id = worker_id as _;

    // Invalid context id is rejected.
    let error = hummock_manager
        .pin_version(invalid_context_id)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidContext(_)));

    // Valid context id is accepted.
    hummock_manager.pin_version(context_id).await.unwrap();
    // Pin multiple times is OK.
    hummock_manager.pin_version(context_id).await.unwrap();
}

#[tokio::test]
async fn test_hummock_manager_basic() {
    let (_env, hummock_manager, cluster_ctl, worker_id) = setup_compute_env(1).await;
    let context_id_1 = worker_id as _;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        context_id_1,
    ));

    let fake_host_address_2 = HostAddress {
        host: "127.0.0.1".to_owned(),
        port: 2,
    };
    let fake_parallelism = 4;
    let worker_node_2 = cluster_ctl
        .add_worker(
            WorkerType::ComputeNode,
            fake_host_address_2,
            Property {
                parallelism: fake_parallelism,
                is_streaming: true,
                is_serving: true,
                ..Default::default()
            },
            Default::default(),
        )
        .await
        .unwrap();
    let context_id_2 = worker_node_2 as _;

    // initial version id
    assert_eq!(
        hummock_manager.get_current_version().await.id,
        FIRST_VERSION_ID
    );

    let mut epoch = test_epoch(1);
    let mut register_log_count = 0;
    let mut commit_log_count = 0;
    let commit_one = |epoch: HummockEpoch,
                      hummock_manager: HummockManagerRef,
                      hummock_meta_client: Arc<dyn HummockMetaClient>| async move {
        let original_tables =
            generate_test_tables(test_epoch(epoch), get_sst_ids(&hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            &hummock_manager,
            &original_tables,
            StaticCompactionGroupId::StateDefault,
        )
        .await;

        hummock_meta_client
            .commit_epoch(
                epoch,
                SyncResult {
                    uncommitted_ssts: to_local_sstable_info(&original_tables),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    };

    commit_one(epoch, hummock_manager.clone(), hummock_meta_client.clone()).await;
    register_log_count += 1;
    commit_log_count += 1;
    epoch.inc_epoch();

    let init_version_id = FIRST_VERSION_ID;

    // increased version id
    assert_eq!(
        hummock_manager.get_current_version().await.id,
        init_version_id + commit_log_count + register_log_count
    );

    // min pinned version id if no clients
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        MAX_HUMMOCK_VERSION_ID
    );
    for _ in 0..2 {
        hummock_manager
            .unpin_version_before(context_id_1, MAX_HUMMOCK_VERSION_ID)
            .await
            .unwrap();
        assert_eq!(
            hummock_manager.get_min_pinned_version_id().await,
            MAX_HUMMOCK_VERSION_ID
        );

        // should pin latest because u64::MAX
        let version = hummock_manager.pin_version(context_id_1).await.unwrap();
        assert_eq!(
            version.id,
            init_version_id + commit_log_count + register_log_count
        );
        assert_eq!(
            hummock_manager.get_min_pinned_version_id().await,
            init_version_id + commit_log_count + register_log_count
        );
    }

    commit_one(epoch, hummock_manager.clone(), hummock_meta_client.clone()).await;
    commit_log_count += 1;
    register_log_count += 1;

    for _ in 0..2 {
        // should pin latest because deltas cannot contain INVALID_EPOCH
        let version = hummock_manager.pin_version(context_id_2).await.unwrap();
        assert_eq!(
            version.id,
            init_version_id + commit_log_count + register_log_count
        );
        // pinned by context_id_1
        assert_eq!(
            hummock_manager.get_min_pinned_version_id().await + 2,
            init_version_id + commit_log_count + register_log_count,
        );
    }
    assert_eq!(hummock_manager.delete_version_deltas().await.unwrap(), 0);
    assert_eq!(
        hummock_manager.create_version_checkpoint(1).await.unwrap(),
        commit_log_count + register_log_count
    );
    assert_eq!(
        hummock_manager.delete_version_deltas().await.unwrap(),
        (commit_log_count + register_log_count) as usize
    );
    hummock_manager
        .unpin_version_before(context_id_1, MAX_HUMMOCK_VERSION_ID)
        .await
        .unwrap();
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        init_version_id + commit_log_count + register_log_count
    );
    hummock_manager
        .unpin_version_before(context_id_2, MAX_HUMMOCK_VERSION_ID)
        .await
        .unwrap();
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        MAX_HUMMOCK_VERSION_ID
    );
}

#[tokio::test]
async fn test_pin_snapshot_response_lost() {
    let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    let mut epoch = test_epoch(1);
    let test_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &test_tables,
        StaticCompactionGroupId::StateDefault,
    )
    .await;
    // [ ] -> [ e0 ]
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&test_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    epoch.inc_epoch();

    // Pin a snapshot with smallest last_pin
    // [ e0 ] -> [ e0:pinned ]
    let mut epoch_recorded_in_frontend = hummock_manager
        .on_current_version(version_max_committed_epoch)
        .await;
    let prev_epoch = epoch.prev_epoch();
    assert_eq!(epoch_recorded_in_frontend, prev_epoch);

    let test_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &test_tables,
        StaticCompactionGroupId::StateDefault,
    )
    .await;
    // [ e0:pinned ] -> [ e0:pinned, e1 ]
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&test_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    epoch.inc_epoch();

    // Assume the response of the previous rpc is lost.
    // [ e0:pinned, e1 ] -> [ e0, e1:pinned ]
    epoch_recorded_in_frontend = hummock_manager
        .on_current_version(version_max_committed_epoch)
        .await;
    let prev_epoch = epoch.prev_epoch();
    assert_eq!(epoch_recorded_in_frontend, prev_epoch);

    // Assume the response of the previous rpc is lost.
    // [ e0, e1:pinned ] -> [ e0, e1:pinned ]
    epoch_recorded_in_frontend = hummock_manager
        .on_current_version(version_max_committed_epoch)
        .await;
    assert_eq!(epoch_recorded_in_frontend, epoch.prev_epoch());

    let test_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &test_tables,
        StaticCompactionGroupId::StateDefault,
    )
    .await;
    // [ e0, e1:pinned ] -> [ e0, e1:pinned, e2 ]
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&test_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    epoch.inc_epoch();

    // Use correct snapshot id.
    // [ e0, e1:pinned, e2 ] -> [ e0, e1:pinned, e2:pinned ]
    epoch_recorded_in_frontend = hummock_manager
        .on_current_version(version_max_committed_epoch)
        .await;
    assert_eq!(epoch_recorded_in_frontend, epoch.prev_epoch());

    let test_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &test_tables,
        StaticCompactionGroupId::StateDefault,
    )
    .await;
    // [ e0, e1:pinned, e2:pinned ] -> [ e0, e1:pinned, e2:pinned, e3 ]
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&test_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    epoch.inc_epoch();

    // Use u64::MAX as epoch to pin greatest snapshot
    // [ e0, e1:pinned, e2:pinned, e3 ] -> [ e0, e1:pinned, e2:pinned, e3::pinned ]
    epoch_recorded_in_frontend = hummock_manager
        .on_current_version(version_max_committed_epoch)
        .await;
    assert_eq!(epoch_recorded_in_frontend, epoch.prev_epoch());
}

#[tokio::test]
async fn test_print_compact_task() {
    let (_, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    // Add some sstables and commit.
    let epoch = test_epoch(1);
    let original_tables =
        generate_test_sstables_with_table_id(epoch, 1, get_sst_ids(&hummock_manager, 2).await);
    let compaction_group_id = StaticCompactionGroupId::StateDefault;
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &original_tables,
        compaction_group_id,
    )
    .await;
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&original_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(compaction_group_id, &mut *default_compaction_selector())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(compact_task.input_ssts.first().unwrap().level_idx, 0);

    let s = compact_task_to_string(&compact_task);
    assert!(s.contains("Compaction task id: 1, group-id: 2, type: Dynamic, target level: 0"));
}

#[tokio::test]
async fn test_invalid_sst_id() {
    let (_, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let epoch = test_epoch(1);
    let ssts = generate_test_tables(epoch, vec![1]);
    let compaction_group_id = StaticCompactionGroupId::StateDefault;
    register_sstable_infos_to_compaction_group(&hummock_manager, &ssts, compaction_group_id).await;
    let ssts = to_local_sstable_info(&ssts);
    // reject due to invalid context id
    {
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager.clone(),
            23333.into(),
        ));
        let error = hummock_meta_client
            .commit_epoch(
                epoch,
                SyncResult {
                    uncommitted_ssts: ssts.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.as_report().to_string(),
            "mock error: SST 1 is invalid"
        );
    }

    // reject due to SST's timestamp is below watermark
    let ssts_below_watermerk = ssts
        .iter()
        .map(|s| LocalSstableInfo {
            sst_info: s.sst_info.clone(),
            table_stats: s.table_stats.clone(),
            created_at: 0,
        })
        .collect();
    let error = hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: ssts_below_watermerk,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        error
            .as_report()
            .to_string()
            .contains("is rejected from being committed since it's below watermark")
    );

    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: ssts.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_trigger_manual_compaction() {
    let (_, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let context_id = worker_id as _;

    {
        let option = ManualCompactionOption::default();
        // to check no compaction task
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault, option)
            .await;

        assert_eq!(
            "trigger_manual_compaction No compactor is available. compaction_group 2",
            result.err().unwrap().to_string()
        );
    }

    // No compaction task available.
    let compactor_manager_ref = hummock_manager.compactor_manager.clone();
    let receiver = compactor_manager_ref.add_compactor(context_id);
    {
        let option = ManualCompactionOption::default();
        // to check no compaction task
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault, option)
            .await;

        assert_eq!(
            "trigger_manual_compaction No compaction_task is available. compaction_group 2",
            result.err().unwrap().to_string()
        );
    }

    // Generate data for compaction task
    let compaction_group_id = StaticCompactionGroupId::StateDefault;
    let _ = add_test_tables(
        &hummock_manager,
        hummock_meta_client.clone(),
        compaction_group_id,
    )
    .await;
    {
        let option = ManualCompactionOption {
            level: 0,
            target_level: Some(0),
            ..Default::default()
        };
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault, option)
            .await;

        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("invalid manual compaction option: target_level for L0 must be")
        );
    }
    {
        // to check compactor send task fail
        drop(receiver);
        {
            let option = ManualCompactionOption::default();
            let result = hummock_manager
                .trigger_manual_compaction(StaticCompactionGroupId::StateDefault, option)
                .await;
            assert!(result.is_err());
        }
    }

    compactor_manager_ref.remove_compactor(context_id);
    let mut receiver = compactor_manager_ref.add_compactor(context_id);
    let hummock_manager_clone = hummock_manager.clone();
    let report_handle = tokio::spawn(async move {
        if let Some(Ok(resp)) = receiver.recv().await
            && let Some(
                risingwave_pb::hummock::subscribe_compaction_event_response::Event::CompactTask(
                    task,
                ),
            ) = resp.event
        {
            let compact_task = CompactTask::from(task);
            let _ = hummock_manager_clone
                .report_compact_task(
                    compact_task.task_id,
                    TaskStatus::ExecuteFailed,
                    vec![],
                    None,
                    HashMap::default(),
                )
                .await;
        }
    });

    {
        let option = ManualCompactionOption {
            level: 6,
            key_range: KeyRange::default(),
            ..Default::default()
        };

        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault, option)
            .await;
        assert!(result.is_ok());
    }
    report_handle.await.unwrap();

    {
        let option = ManualCompactionOption::default();
        // all sst pending , test no compaction avail
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault, option)
            .await;
        assert!(result.is_err());
    }
}

// This is a non-deterministic test
#[cfg(madsim)]
#[tokio::test]
async fn test_hummock_compaction_task_heartbeat() {
    use risingwave_pb::hummock::CompactTaskProgress;

    use crate::hummock::HummockManager;

    let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let context_id = worker_id as _;
    let sst_num = 2;

    let compactor_manager = hummock_manager.compactor_manager.clone();
    let _tx = compactor_manager.add_compactor(context_id);

    let (join_handle, shutdown_tx) =
        HummockManager::hummock_timer_task(hummock_manager.clone(), None);

    // No compaction task available.
    assert!(
        hummock_manager
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut *default_compaction_selector(),
            )
            .await
            .unwrap()
            .is_none()
    );

    // Add some sstables and commit.
    let epoch = test_epoch(1);
    let original_tables = generate_test_sstables_with_table_id(
        epoch,
        1,
        get_sst_ids(&hummock_manager, sst_num).await,
    );
    register_table_ids_to_compaction_group(
        &hummock_manager,
        &[1],
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&original_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut *default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(compact_task.input_ssts.first().unwrap().level_idx, 0);
    assert_eq!(compact_task.task_id, 2);

    for i in 0..10 {
        // send heartbeats to the task over 2.5 seconds
        let req = CompactTaskProgress {
            task_id: compact_task.task_id,
            num_ssts_sealed: i + 1,
            ..Default::default()
        };
        compactor_manager.update_task_heartbeats(&vec![req]);
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    // Cancel the task immediately and succeed.
    assert!(
        hummock_manager
            .report_compact_task(
                compact_task.task_id,
                TaskStatus::ExecuteFailed,
                vec![],
                None,
                HashMap::default(),
            )
            .await
            .unwrap()
    );

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut *default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(compact_task.task_id, 3);

    // Cancel the task after heartbeat has triggered and fail.

    // do not send heartbeats to the task for 30s seconds (ttl = 1s, heartbeat check freq. = 1s)
    // default_interval = 30s
    tokio::time::sleep(std::time::Duration::from_secs(32)).await;

    assert!(
        !hummock_manager
            .report_compact_task(
                compact_task.task_id,
                TaskStatus::ExecuteFailed,
                vec![],
                None,
                HashMap::default(),
            )
            .await
            .unwrap()
    );
    shutdown_tx.send(()).unwrap();
    join_handle.await.unwrap();
}

// This is a non-deterministic test
#[cfg(madsim)]
#[tokio::test]
async fn test_hummock_compaction_task_heartbeat_removal_on_node_removal() {
    use risingwave_pb::hummock::CompactTaskProgress;

    use crate::hummock::HummockManager;
    let (_env, hummock_manager, cluster_ctl, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let context_id = worker_id as _;
    let sst_num = 2;

    let worker_node = cluster_ctl
        .get_worker_by_id(worker_id)
        .await
        .unwrap()
        .unwrap();

    let compactor_manager = hummock_manager.compactor_manager.clone();
    let _tx = compactor_manager.add_compactor(context_id);

    let (join_handle, shutdown_tx) =
        HummockManager::hummock_timer_task(hummock_manager.clone(), None);

    // No compaction task available.
    assert!(
        hummock_manager
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut *default_compaction_selector(),
            )
            .await
            .unwrap()
            .is_none()
    );

    // Add some sstables and commit.
    let epoch = test_epoch(1);
    let original_tables = generate_test_sstables_with_table_id(
        epoch,
        1,
        get_sst_ids(&hummock_manager, sst_num).await,
    );
    register_table_ids_to_compaction_group(
        &hummock_manager,
        &[1],
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&original_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut *default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(compact_task.input_ssts.first().unwrap().level_idx, 0);
    assert_eq!(compact_task.task_id, 2);

    // send heartbeats to the task immediately
    let req = CompactTaskProgress {
        task_id: compact_task.task_id,
        num_ssts_sealed: 1,
        num_ssts_uploaded: 1,
        ..Default::default()
    };
    compactor_manager.update_task_heartbeats(&vec![req.clone()]);

    // Removing the node from cluster will invalidate context id.
    cluster_ctl
        .delete_worker(worker_node.host.unwrap())
        .await
        .unwrap();
    hummock_manager
        .release_contexts([context_id])
        .await
        .unwrap();

    shutdown_tx.send(()).unwrap();
    join_handle.await.unwrap();
}

#[tokio::test]
async fn test_extend_objects_to_delete() {
    let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let context_id = worker_id as _;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        context_id,
    ));
    let _pinned_version1 = hummock_manager.pin_version(context_id).await.unwrap();
    let compaction_group_id = StaticCompactionGroupId::StateDefault;
    let sst_infos = add_test_tables(
        hummock_manager.as_ref(),
        hummock_meta_client.clone(),
        compaction_group_id,
    )
    .await;
    let max_committed_object_id = sst_infos
        .iter()
        .map(|ssts| ssts.iter().map(|s| s.object_id).max().unwrap())
        .max()
        .unwrap();
    let orphan_sst_num: usize = 10;
    let all_object_ids = sst_infos
        .iter()
        .flatten()
        .map(|s| HummockObjectId::Sstable(s.object_id))
        .chain(
            (max_committed_object_id + 1..=max_committed_object_id + orphan_sst_num as u64)
                .map(HummockObjectId::Sstable),
        )
        .collect_vec();
    assert_eq!(
        hummock_manager
            .finalize_objects_to_delete(all_object_ids.clone().into_iter())
            .await
            .unwrap()
            .len(),
        orphan_sst_num
    );

    // Checkpoint
    assert_eq!(
        hummock_manager.create_version_checkpoint(1).await.unwrap(),
        6
    );
    {
        let versioning = hummock_manager
            .versioning
            .read_with_process_name("test_extend_objects_to_delete")
            .await;
        let checkpoint = &versioning.checkpoint;
        let expected_version_object_ids: HashSet<_> = checkpoint.version.get_object_ids().collect();
        assert_eq!(checkpoint.object_ids, expected_version_object_ids);

        let restored_checkpoint =
            HummockVersionCheckpoint::from_protobuf(&checkpoint.to_protobuf());
        assert_eq!(restored_checkpoint.object_ids, expected_version_object_ids);
    }

    // A table-change-log-only object is protected while the log is live, but must not be retained
    // by the immutable checkpoint index after the log is removed.
    let change_log_object_id = 10_000;
    let change_log_object = HummockObjectId::Sstable(change_log_object_id.into());
    let change_log_table_id = TableId::new(u32::MAX);
    {
        let mut versioning = hummock_manager
            .versioning
            .write_with_process_name("test_extend_objects_to_delete")
            .await;
        let checkpoint_version = versioning.checkpoint.version.clone();
        let stale_objects = versioning.checkpoint.stale_objects.clone();
        versioning.table_change_log.insert(
            change_log_table_id,
            TableChangeLog::new([EpochNewChangeLog {
                new_value: vec![gen_sstable_info(
                    change_log_object_id,
                    vec![change_log_table_id.as_raw_id()],
                    test_epoch(1),
                )],
                old_value: vec![],
                non_checkpoint_epochs: vec![],
                checkpoint_epoch: test_epoch(1),
            }]),
        );
        versioning.checkpoint = HummockVersionCheckpoint::new(checkpoint_version, stale_objects);
        assert!(
            !versioning
                .checkpoint
                .object_ids
                .contains(&change_log_object)
        );
    }
    assert!(
        hummock_manager
            .finalize_objects_to_delete([change_log_object].into_iter())
            .await
            .unwrap()
            .is_empty()
    );
    hummock_manager
        .versioning
        .write_with_process_name("test_extend_objects_to_delete")
        .await
        .table_change_log
        .remove(&change_log_table_id);
    assert_eq!(
        hummock_manager
            .finalize_objects_to_delete([change_log_object].into_iter())
            .await
            .unwrap(),
        vec![change_log_object]
    );
    // since version1 is still pinned, the sst removed in compaction can not be reclaimed.
    assert_eq!(
        hummock_manager
            .finalize_objects_to_delete(all_object_ids.clone().into_iter())
            .await
            .unwrap()
            .len(),
        orphan_sst_num
    );
    let pinned_version2 = hummock_manager.pin_version(context_id).await.unwrap();
    hummock_manager
        .unpin_version_before(context_id, pinned_version2.id)
        .await
        .unwrap();
    // version1 is unpin, but version2 is pinned, and version2 is the checkpoint version.
    // stale objects are combined in the checkpoint of version2, so no sst to reclaim
    assert_eq!(
        hummock_manager
            .finalize_objects_to_delete(all_object_ids.clone().into_iter())
            .await
            .unwrap()
            .len(),
        orphan_sst_num
    );
    let new_epoch = version_max_committed_epoch(&pinned_version2).next_epoch();
    hummock_meta_client
        .commit_epoch(
            new_epoch,
            SyncResult {
                uncommitted_ssts: vec![],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let pinned_version3 = hummock_manager.pin_version(context_id).await.unwrap();
    assert_eq!(new_epoch, version_max_committed_epoch(&pinned_version3));
    hummock_manager
        .unpin_version_before(context_id, pinned_version3.id)
        .await
        .unwrap();
    // version3 is the min pinned, and sst removed in compaction can be reclaimed, because they were tracked
    // in the stale objects of version2 checkpoint
    assert_eq!(
        hummock_manager
            .finalize_objects_to_delete(all_object_ids.clone().into_iter())
            .await
            .unwrap()
            .len(),
        orphan_sst_num + 3
    );
}

#[tokio::test]
async fn test_version_stats() {
    let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    let init_stats = hummock_manager.get_version_stats().await;
    assert!(init_stats.table_stats.is_empty());

    // Commit epoch
    let epoch = test_epoch(1);
    register_table_ids_to_compaction_group(
        &hummock_manager,
        &[1, 2, 3],
        StaticCompactionGroupId::StateDefault,
    )
    .await;
    let table_stats_change = TableStats {
        total_key_size: 1000,
        total_value_size: 100,
        total_key_count: 10,

        total_compressed_size: 1024 * 1024,
    };
    let ssts_with_table_ids = vec![vec![1.into(), 2.into()], vec![2.into(), 3.into()]];
    let sst_ids = get_sst_ids(&hummock_manager, ssts_with_table_ids.len() as _).await;
    let ssts = ssts_with_table_ids
        .into_iter()
        .enumerate()
        .map(|(idx, table_ids)| LocalSstableInfo {
            sst_info: SstableInfoInner {
                object_id: sst_ids[idx].into(),
                sst_id: sst_ids[idx].into(),
                key_range: KeyRange {
                    left: iterator_test_key_of_epoch(1, 1, 1).into(),
                    right: iterator_test_key_of_epoch(1, 1, 1).into(),
                    right_exclusive: false,
                },
                file_size: 1024 * 1024 * 1024,
                table_ids: table_ids.clone(),
                sst_size: 1024 * 1024 * 1024,
                ..Default::default()
            }
            .into(),
            table_stats: table_ids
                .iter()
                .map(|table_id| (*table_id, table_stats_change.clone()))
                .collect(),
            created_at: u64::MAX,
        })
        .collect_vec();
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: ssts,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let stats_after_commit = hummock_manager.get_version_stats().await;
    assert_eq!(stats_after_commit.table_stats.len(), 3);
    let table1_stats = stats_after_commit.table_stats.get(&1).unwrap();
    let table2_stats = stats_after_commit.table_stats.get(&2).unwrap();
    let table3_stats = stats_after_commit.table_stats.get(&3).unwrap();
    assert_eq!(table1_stats.total_key_count, 10);
    assert_eq!(table1_stats.total_value_size, 100);
    assert_eq!(table1_stats.total_key_size, 1000);
    assert_eq!(table2_stats.total_key_count, 20);
    assert_eq!(table2_stats.total_value_size, 200);
    assert_eq!(table2_stats.total_key_size, 2000);
    assert_eq!(table3_stats.total_key_count, 10);
    assert_eq!(table3_stats.total_value_size, 100);
    assert_eq!(table3_stats.total_key_size, 1000);

    let compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault,
            &mut *default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();
    // compact_task.task_status = TaskStatus::Success as _;
    let compact_table_stats_change = TableStatsMap::from([
        (
            2.into(),
            TableStats {
                total_key_size: -1000,
                total_value_size: -100,
                total_key_count: -10,
                total_compressed_size: 0, // unused
            },
        ),
        (
            3.into(),
            TableStats {
                total_key_size: -1000,
                total_value_size: -100,
                total_key_count: -10,
                total_compressed_size: 0, // unused
            },
        ),
    ]);
    hummock_manager
        .report_compact_task(
            compact_task.task_id,
            TaskStatus::Success,
            vec![],
            Some(to_prost_table_stats_map(compact_table_stats_change)),
            HashMap::default(),
        )
        .await
        .unwrap();
    let stats_after_compact = hummock_manager.get_version_stats().await;
    let compact_table1_stats = stats_after_compact.table_stats.get(&1).unwrap();
    let compact_table2_stats = stats_after_compact.table_stats.get(&2).unwrap();
    let compact_table3_stats = stats_after_compact.table_stats.get(&3).unwrap();
    assert_eq!(compact_table1_stats, table1_stats);
    assert_eq!(compact_table2_stats.total_key_count, 10);
    assert_eq!(compact_table2_stats.total_value_size, 100);
    assert_eq!(compact_table2_stats.total_key_size, 1000);
    assert_eq!(compact_table3_stats.total_key_count, 0);
    assert_eq!(compact_table3_stats.total_value_size, 0);
    assert_eq!(compact_table3_stats.total_key_size, 0);
}

#[tokio::test]
async fn test_move_state_tables_to_dedicated_compaction_group_on_commit() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    hummock_manager
        .register_table_ids_for_test(&[(100, 2.into()), (101, 3.into())])
        .await
        .unwrap();
    let sst_1 = LocalSstableInfo {
        sst_info: gen_sstable_info(10, vec![100, 101], test_epoch(20)),
        table_stats: HashMap::from([
            (
                100.into(),
                TableStats {
                    total_compressed_size: 50,
                    ..Default::default()
                },
            ),
            (
                101.into(),
                TableStats {
                    total_compressed_size: 50,
                    ..Default::default()
                },
            ),
        ]),
        created_at: u64::MAX,
    };
    hummock_meta_client
        .commit_epoch(
            test_epoch(30),
            SyncResult {
                uncommitted_ssts: vec![sst_1],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let current_version = hummock_manager.get_current_version().await;
    assert_eq!(current_version.levels.len(), 2);
    assert_eq!(
        get_compaction_group_object_ids(&current_version, 2),
        vec![10]
    );
    assert_eq!(
        get_compaction_group_object_ids(&current_version, 3),
        vec![10]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(2.into())
            .iter()
            .map(|table_id| table_id.as_raw_id())
            .collect_vec(),
        vec![100]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(3.into())
            .iter()
            .map(|table_id| table_id.as_raw_id())
            .collect_vec(),
        vec![101]
    );
}

#[tokio::test]
async fn test_move_state_tables_to_dedicated_compaction_group_on_demand_basic() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let original_groups = hummock_manager
        .get_current_version()
        .await
        .levels
        .keys()
        .cloned()
        .sorted()
        .collect_vec();
    assert_eq!(original_groups, vec![2, 3]);

    let err = hummock_manager
        .move_state_tables_to_dedicated_compaction_group(100.into(), &[0.into()], None)
        .await
        .unwrap_err();
    assert_eq!("compaction group error: invalid group 100", err.to_string());

    let err = hummock_manager
        .move_state_tables_to_dedicated_compaction_group(2.into(), &[100.into()], None)
        .await
        .unwrap_err();
    assert_eq!(
        "compaction group error: table 100 doesn't in group 2",
        err.to_string()
    );

    hummock_manager
        .register_table_ids_for_test(&[(100, 2.into()), (101, 2.into())])
        .await
        .unwrap();
    let sst_1 = LocalSstableInfo {
        sst_info: gen_sstable_info(10, vec![100], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };
    let sst_2 = LocalSstableInfo {
        sst_info: gen_sstable_info(11, vec![100, 101], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };
    hummock_meta_client
        .commit_epoch(
            test_epoch(30),
            SyncResult {
                uncommitted_ssts: vec![sst_1, sst_2],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let compaction_group_id = StaticCompactionGroupId::StateDefault;
    let err = hummock_manager
        .move_state_tables_to_dedicated_compaction_group(
            compaction_group_id,
            &[100.into(), 101.into()],
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(
        "compaction group error: invalid split attempt for group 2: all member tables are moved",
        err.to_string()
    );

    // Now group 2 has member tables [100,101,102], so split [100, 101] can succeed even though
    // there is no data of 102.
    hummock_manager
        .register_table_ids_for_test(&[(102, 2.into())])
        .await
        .unwrap();

    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(
            compaction_group_id,
            &[100.into(), 101.into()],
            None,
        )
        .await
        .unwrap();
    let current_version = hummock_manager.get_current_version().await;
    assert_eq!(current_version.levels.len(), 3);
    let new_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;

    let old_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 102).await;
    assert_eq!(
        get_compaction_group_object_ids(&current_version, old_compaction_group_id),
        Vec::<u64>::new()
    );
    assert_eq!(
        get_compaction_group_object_ids(&current_version, new_compaction_group_id),
        vec![10, 11]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(old_compaction_group_id)
            .iter()
            .map(|table_id| table_id.as_raw_id())
            .collect_vec(),
        vec![102]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(new_compaction_group_id)
            .iter()
            .map(|table_id| table_id.as_raw_id())
            .sorted()
            .collect_vec(),
        vec![100, 101]
    );
}

#[tokio::test]
async fn test_merge_compaction_group_removes_split_group_metrics() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    let compaction_group_id = StaticCompactionGroupId::StateDefault;
    hummock_manager
        .register_table_ids_for_test(&[
            (100, compaction_group_id),
            (101, compaction_group_id),
            (102, compaction_group_id),
        ])
        .await
        .unwrap();

    let epoch = test_epoch(30);
    let ssts = [100, 101, 102]
        .into_iter()
        .enumerate()
        .map(|(idx, table_id)| gen_local_sstable_info(10 + idx as u64, vec![table_id], epoch))
        .collect_vec();
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: ssts,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let (right_group_id, _) = hummock_manager
        .move_state_tables_to_dedicated_compaction_group(
            compaction_group_id,
            &[101.into(), 102.into()],
            None,
        )
        .await
        .unwrap();
    let left_group_id = get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    assert_ne!(left_group_id, right_group_id);

    // Exercise production metric paths before merging: checkpoint updates split stats, and compact
    // task picking records compact task metrics for the split group.
    hummock_manager.create_version_checkpoint(0).await.unwrap();
    let compact_task = hummock_manager
        .get_compact_task(right_group_id, &mut *default_compaction_selector())
        .await
        .unwrap();
    assert!(compact_task.is_some());

    let right_group_label = right_group_id.to_string();
    let collect_group_metrics = || {
        [
            (
                "state_table_count",
                hummock_manager.metrics.state_table_count.collect(),
            ),
            (
                "compact_task_size",
                hummock_manager.metrics.compact_task_size.collect(),
            ),
            (
                "compact_task_file_count",
                hummock_manager.metrics.compact_task_file_count.collect(),
            ),
        ]
    };
    for (metric_name, metric_families) in collect_group_metrics() {
        assert!(
            metric_has_label_value(&metric_families, "group", &right_group_label),
            "{metric_name} should have right group metric before merge"
        );
    }

    hummock_manager
        .merge_compaction_group_for_test(
            left_group_id,
            right_group_id,
            HashSet::from_iter([100.into(), 101.into(), 102.into()]),
        )
        .await
        .unwrap();

    for (metric_name, metric_families) in collect_group_metrics() {
        assert!(
            !metric_has_label_value(&metric_families, "group", &right_group_label),
            "{metric_name} should not have right group metric after merge"
        );
    }
}

#[tokio::test]
async fn test_move_state_tables_to_dedicated_compaction_group_on_demand_non_trivial() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let sst_1 = LocalSstableInfo {
        sst_info: gen_sstable_info(10, vec![100, 101], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };
    hummock_manager
        .register_table_ids_for_test(&[(100, 2.into()), (101, 2.into())])
        .await
        .unwrap();
    hummock_meta_client
        .commit_epoch(
            30,
            SyncResult {
                uncommitted_ssts: vec![sst_1],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let compaction_group_id = StaticCompactionGroupId::StateDefault;
    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(compaction_group_id, &[100.into()], None)
        .await
        .unwrap();

    let current_version = hummock_manager.get_current_version().await;
    assert_eq!(current_version.levels.len(), 3);
    let new_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    let old_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 101).await;
    assert_eq!(
        get_compaction_group_object_ids(&current_version, old_compaction_group_id),
        vec![10]
    );
    assert_eq!(
        get_compaction_group_object_ids(&current_version, new_compaction_group_id),
        vec![10]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(old_compaction_group_id)
            .iter()
            .map(|table_id| table_id.as_raw_id())
            .collect_vec(),
        vec![101]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(new_compaction_group_id)
            .iter()
            .map(|table_id| table_id.as_raw_id())
            .collect_vec(),
        vec![100]
    );
}

#[tokio::test]
async fn test_move_state_tables_to_dedicated_compaction_group_trivial_expired() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let original_groups = hummock_manager
        .get_current_version()
        .await
        .levels
        .keys()
        .cloned()
        .sorted()
        .collect_vec();
    assert_eq!(original_groups, vec![2, 3]);

    hummock_manager
        .register_table_ids_for_test(&[(100, 2.into()), (101, 2.into())])
        .await
        .unwrap();
    let sst_1 = LocalSstableInfo {
        sst_info: gen_sstable_info(10, vec![100], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };

    let sst_2 = LocalSstableInfo {
        sst_info: gen_sstable_info(11, vec![100, 101], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };
    let sst_3 = LocalSstableInfo {
        sst_info: SstableInfoInner {
            sst_id: 8.into(),
            object_id: 8.into(),
            ..sst_2.sst_info.get_inner()
        }
        .into(),
        ..sst_2.clone()
    };
    let sst_4 = LocalSstableInfo {
        sst_info: SstableInfoInner {
            sst_id: 9.into(),
            object_id: 9.into(),
            ..sst_1.sst_info.get_inner()
        }
        .into(),
        ..sst_1.clone()
    };
    hummock_meta_client
        .commit_epoch(
            30,
            SyncResult {
                uncommitted_ssts: vec![sst_1, sst_2, sst_3, sst_4],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Now group 2 has member tables [100,101,102], so split [100, 101] can succeed even though
    // there is no data of 102.
    hummock_manager
        .register_table_ids_for_test(&[(102, 2.into())])
        .await
        .unwrap();
    let compaction_group_id = StaticCompactionGroupId::StateDefault;
    let task = hummock_manager
        .get_compact_task(compaction_group_id, &mut *default_compaction_selector())
        .await
        .unwrap()
        .unwrap();

    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(compaction_group_id, &[100.into()], None)
        .await
        .unwrap();

    let current_version = hummock_manager.get_current_version().await;
    let left_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    let right_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 101).await;
    assert_eq!(current_version.levels.len(), 3);
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(right_compaction_group_id)
            .iter()
            .map(|table_id| table_id.as_raw_id())
            .sorted()
            .collect_vec(),
        vec![101, 102]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(left_compaction_group_id)
            .iter()
            .map(|table_id| table_id.as_raw_id())
            .collect_vec(),
        vec![100]
    );

    let task2 = hummock_manager
        .get_compact_task(
            left_compaction_group_id,
            &mut *default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();

    let ret = hummock_manager
        .report_compact_task(
            task2.task_id,
            TaskStatus::Success,
            vec![
                SstableInfoInner {
                    object_id: 12.into(),
                    sst_id: 12.into(),
                    key_range: KeyRange::default(),
                    table_ids: vec![100.into()],
                    min_epoch: 20,
                    max_epoch: 20,
                    file_size: 100,
                    sst_size: 100,
                    ..Default::default()
                }
                .into(),
            ],
            None,
            HashMap::default(),
        )
        .await
        .unwrap();
    assert!(ret);
    let ret = hummock_manager
        .report_compact_task(
            task.task_id,
            TaskStatus::Success,
            vec![],
            None,
            HashMap::default(),
        )
        .await
        .unwrap();
    // the task has been canceled
    assert!(!ret);
}

async fn get_manual_compact_task(
    hummock_manager_ref: HummockManagerRef,
    compaction_group_id: CompactionGroupId,
    level: usize,
) -> CompactTask {
    let manual_compcation_option = ManualCompactionOption {
        level,
        ..Default::default()
    };

    hummock_manager_ref
        .manual_get_compact_task(compaction_group_id, manual_compcation_option)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn test_move_state_tables_to_dedicated_compaction_group_on_demand_bottom_levels() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    hummock_manager
        .register_table_ids_for_test(&[
            (100, 2.into()),
            (101, 2.into()),
            (102, 2.into()),
            (103, 2.into()),
        ])
        .await
        .unwrap();

    let epoch = test_epoch(1);
    let sst_1 = LocalSstableInfo {
        sst_info: gen_sstable_info(10, vec![100], epoch),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };

    let sst_2 = LocalSstableInfo {
        sst_info: gen_sstable_info(11, vec![101, 102], epoch),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };

    let sst_3 = LocalSstableInfo {
        sst_info: gen_sstable_info(12, vec![103], epoch),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };

    hummock_meta_client
        .commit_epoch(
            30,
            SyncResult {
                uncommitted_ssts: vec![sst_1, sst_2, sst_3],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    let manual_compcation_option = ManualCompactionOption {
        level: 0,
        ..Default::default()
    };
    // 2. get compact task
    let compact_task = hummock_manager
        .manual_get_compact_task(compaction_group_id, manual_compcation_option)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        compact_task
            .input_ssts
            .iter()
            .map(|level| level.table_infos.len())
            .sum::<usize>(),
        3,
    );

    hummock_manager
        .report_compact_task(
            compact_task.task_id,
            TaskStatus::Success,
            compact_task
                .input_ssts
                .iter()
                .flat_map(|level| level.table_infos.clone())
                .collect(),
            None,
            HashMap::default(),
        )
        .await
        .unwrap();

    let base_level: usize = 6;

    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(2.into(), &[100.into()], None)
        .await
        .unwrap();

    let current_version = hummock_manager.get_current_version().await;
    let left_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    let right_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 101).await;
    assert_eq!(
        current_version
            .get_compaction_group_levels(left_compaction_group_id)
            .levels[base_level - 1]
            .table_infos
            .len(),
        1
    );

    assert_eq!(
        current_version
            .get_compaction_group_levels(right_compaction_group_id)
            .levels[base_level - 1]
            .table_infos[0]
            .table_ids,
        [101, 102]
            .into_iter()
            .map(Into::<TableId>::into)
            .collect_vec()
    );
    assert_eq!(
        current_version
            .get_compaction_group_levels(left_compaction_group_id)
            .levels[base_level - 1]
            .table_infos
            .len(),
        1
    );
    assert_eq!(
        current_version
            .get_compaction_group_levels(right_compaction_group_id)
            .levels[base_level - 1]
            .table_infos
            .len(),
        2
    );
    assert_eq!(
        current_version
            .get_compaction_group_levels(left_compaction_group_id)
            .levels[base_level - 1]
            .table_infos[0]
            .table_ids,
        [100].into_iter().map(Into::<TableId>::into).collect_vec()
    );
    assert_eq!(
        current_version
            .get_compaction_group_levels(right_compaction_group_id)
            .levels[base_level - 1]
            .table_infos[1]
            .table_ids,
        [103].into_iter().map(Into::<TableId>::into).collect_vec()
    );
}

#[tokio::test]
async fn test_compaction_task_expiration_due_to_split_group() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    let compaction_group_id = StaticCompactionGroupId::StateDefault;
    hummock_manager
        .register_table_ids_for_test(&[(100, compaction_group_id), (101, compaction_group_id)])
        .await
        .unwrap();
    let sst_1 = LocalSstableInfo {
        sst_info: gen_sstable_info(10, vec![100, 101], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };
    let sst_2 = LocalSstableInfo {
        sst_info: gen_sstable_info(11, vec![101], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };

    hummock_meta_client
        .commit_epoch(
            30,
            SyncResult {
                uncommitted_ssts: vec![sst_1, sst_2],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let compaction_group_id: CompactionGroupId = StaticCompactionGroupId::StateDefault;
    let compaction_task =
        get_manual_compact_task(hummock_manager.clone(), compaction_group_id, 0).await;
    assert_eq!(compaction_task.input_ssts[0].table_infos.len(), 2);
    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(compaction_group_id, &[100.into()], None)
        .await
        .unwrap();

    let version_1 = hummock_manager.get_current_version().await;
    assert!(
        !hummock_manager
            .report_compact_task(
                compaction_task.task_id,
                TaskStatus::Success,
                vec![],
                None,
                HashMap::default()
            )
            .await
            .unwrap()
    );
    let version_2 = hummock_manager.get_current_version().await;
    assert_eq!(
        version_1, version_2,
        "version should not change because compaction task has been cancelled"
    );

    let compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 101).await;
    let compaction_task =
        get_manual_compact_task(hummock_manager.clone(), compaction_group_id, 0).await;
    assert_eq!(compaction_task.input_ssts[0].table_infos.len(), 2);
    hummock_manager
        .report_compact_task(
            compaction_task.task_id,
            TaskStatus::Success,
            vec![],
            None,
            HashMap::default(),
        )
        .await
        .unwrap();

    let version_3 = hummock_manager.get_current_version().await;
    assert_ne!(
        version_2, version_3,
        "version should change because compaction task has succeeded"
    );
}

#[tokio::test]
async fn test_move_tables_between_compaction_group() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    hummock_manager
        .register_table_ids_for_test(&[(100, 2.into()), (101, 2.into()), (102, 2.into())])
        .await
        .unwrap();
    let sst_1 = gen_local_sstable_info(10, vec![100, 101, 102], test_epoch(1));

    hummock_meta_client
        .commit_epoch(
            30,
            SyncResult {
                uncommitted_ssts: vec![sst_1.clone()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let sst_2 = gen_local_sstable_info(14, vec![101, 102], test_epoch(2));

    hummock_meta_client
        .commit_epoch(
            31,
            SyncResult {
                uncommitted_ssts: vec![sst_2.clone()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let current_version = hummock_manager.get_current_version().await;
    let compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    let sst_ids = current_version
        .get_sst_ids_by_group_id(compaction_group_id)
        .collect_vec();
    assert_eq!(2, sst_ids.len());
    assert!(sst_ids.contains(&10.into()));
    assert!(sst_ids.contains(&14.into()));

    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(2.into(), &[100.into()], None)
        .await
        .unwrap();
    let compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 101).await;
    let current_version = hummock_manager.get_current_version().await;
    let sst_ids = current_version
        .get_sst_ids_by_group_id(compaction_group_id)
        .collect_vec();
    assert_eq!(2, sst_ids.len());
    assert!(!sst_ids.contains(&10.into()));

    let compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    let sst_ids = current_version
        .get_sst_ids_by_group_id(compaction_group_id)
        .collect_vec();
    assert_eq!(1, sst_ids.len());
    assert!(!sst_ids.contains(&10.into()));
}

#[tokio::test]
async fn test_gc_stats() {
    let config = CompactionConfigBuilder::new()
        .level0_tier_compact_file_number(1)
        .level0_max_compact_file_number(130)
        .level0_sub_level_compact_level_count(1)
        .level0_overlapping_sub_level_compact_level_count(1)
        .build();
    let registry = Registry::new();
    let (_env, hummock_manager, _, worker_id) =
        setup_compute_env_with_metric(80, config, Some(MetaMetrics::for_test(&registry))).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let context_id = worker_id as _;
    let assert_eq_gc_stats = |stale_object_size,
                              stale_object_count,
                              old_version_object_size,
                              old_version_object_count,
                              current_version_object_count,
                              current_version_object_size| {
        assert_eq!(
            hummock_manager.metrics.stale_object_size.get(),
            stale_object_size
        );
        assert_eq!(
            hummock_manager.metrics.stale_object_count.get(),
            stale_object_count
        );
        assert_eq!(
            hummock_manager.metrics.old_version_object_size.get(),
            old_version_object_size
        );
        assert_eq!(
            hummock_manager.metrics.old_version_object_count.get(),
            old_version_object_count
        );
        assert_eq!(
            hummock_manager.metrics.current_version_object_count.get(),
            current_version_object_count
        );
        assert_eq!(
            hummock_manager.metrics.current_version_object_size.get(),
            current_version_object_size
        );
    };
    assert_eq_gc_stats(0, 0, 0, 0, 0, 0);
    assert_eq!(
        hummock_manager.create_version_checkpoint(0).await.unwrap(),
        0
    );

    let compaction_group_id = StaticCompactionGroupId::StateDefault;
    hummock_manager.pin_version(context_id).await.unwrap();
    let _ = add_test_tables(
        &hummock_manager,
        hummock_meta_client.clone(),
        compaction_group_id,
    )
    .await;
    assert_eq_gc_stats(0, 0, 0, 0, 0, 0);
    assert_ne!(
        hummock_manager.create_version_checkpoint(0).await.unwrap(),
        0
    );

    assert_eq_gc_stats(0, 0, 6, 3, 2, 4);
    hummock_manager
        .unpin_version_before(context_id, MAX_HUMMOCK_VERSION_ID)
        .await
        .unwrap();

    assert_eq_gc_stats(0, 0, 6, 3, 2, 4);
    assert_eq!(
        hummock_manager.create_version_checkpoint(0).await.unwrap(),
        0
    );
}

#[tokio::test]
async fn test_partition_level() {
    let config = CompactionConfigBuilder::new()
        .level0_tier_compact_file_number(3)
        .level0_sub_level_compact_level_count(3)
        .level0_overlapping_sub_level_compact_level_count(3)
        .build();
    let registry = Registry::new();
    let (env, hummock_manager, _, worker_id) =
        setup_compute_env_with_metric(80, config.clone(), Some(MetaMetrics::for_test(&registry)))
            .await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    hummock_manager
        .register_table_ids_for_test(&[(100, 2.into()), (101, 2.into())])
        .await
        .unwrap();
    let sst_1 = gen_local_sstable_info(10, vec![100, 101], test_epoch(1));

    hummock_meta_client
        .commit_epoch(
            30,
            SyncResult {
                uncommitted_ssts: vec![sst_1],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let compaction_group_id = StaticCompactionGroupId::StateDefault;
    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(
            compaction_group_id,
            &[100.into()],
            Some(env.opts.partition_vnode_count),
        )
        .await
        .unwrap();
    let new_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    let mut global_sst_id = 13;
    const MB: u64 = 1024 * 1024;
    let mut selector = default_compaction_selector();
    for epoch in 31..100 {
        let mut sst = gen_sstable_info_impl(global_sst_id, vec![100], test_epoch(epoch));
        sst.file_size = 10 * MB;
        sst.sst_size = 10 * MB;
        sst.uncompressed_file_size = 10 * MB;
        let sst = LocalSstableInfo {
            sst_info: sst.into(),
            table_stats: Default::default(),
            created_at: u64::MAX,
        };

        hummock_meta_client
            .commit_epoch(
                epoch,
                SyncResult {
                    uncommitted_ssts: vec![sst],
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        global_sst_id += 1;
        if let Some(task) = hummock_manager
            .get_compact_task(new_compaction_group_id, selector.as_mut())
            .await
            .unwrap()
        {
            let mut sst = gen_sstable_info_impl(global_sst_id, vec![100], test_epoch(epoch));
            sst.file_size = task
                .input_ssts
                .iter()
                .map(|level| {
                    level
                        .table_infos
                        .iter()
                        .map(|sst| sst.sst_size)
                        .sum::<u64>()
                })
                .sum::<u64>();
            sst.sst_size = sst.file_size;
            global_sst_id += 1;
            let ret = hummock_manager
                .report_compact_task(
                    task.task_id,
                    TaskStatus::Success,
                    vec![sst.into()],
                    None,
                    HashMap::default(),
                )
                .await
                .unwrap();
            assert!(ret);
        }
    }
    let current_version = hummock_manager.get_current_version().await;
    let group = current_version.get_compaction_group_levels(new_compaction_group_id);
    for sub_level in &group.l0.sub_levels {
        if sub_level.total_file_size > config.sub_level_max_compaction_bytes {
            assert!(sub_level.vnode_partition_count > 0);
        }
    }
}

#[tokio::test]
async fn test_unregister_table_prunes_sst_table_ids() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let compaction_group_id = StaticCompactionGroupId::StateDefault;
    hummock_manager
        .register_table_ids_for_test(&[(100, compaction_group_id), (101, compaction_group_id)])
        .await
        .unwrap();
    hummock_meta_client
        .commit_epoch(
            test_epoch(30),
            SyncResult {
                uncommitted_ssts: vec![
                    gen_local_sstable_info(10, vec![100, 101], test_epoch(20)),
                    gen_local_sstable_info(11, vec![101], test_epoch(20)),
                ],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    hummock_manager
        .unregister_table_ids([TableId::new(101)])
        .await
        .unwrap();

    let current_version = hummock_manager.get_current_version().await;
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(compaction_group_id)
            .iter()
            .copied()
            .collect_vec(),
        vec![TableId::new(100)]
    );
    assert_eq!(
        get_compaction_group_object_ids(&current_version, compaction_group_id),
        vec![10]
    );
    let levels = current_version.get_compaction_group_levels(compaction_group_id);
    let table_ids = levels
        .l0
        .sub_levels
        .iter()
        .chain(levels.levels.iter())
        .flat_map(|level| level.table_infos.iter())
        .map(|sst| sst.table_ids.clone())
        .collect_vec();
    assert_eq!(table_ids, vec![vec![TableId::new(100)]]);
}

#[tokio::test]
async fn test_unregister_moved_table() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let original_groups = hummock_manager
        .get_current_version()
        .await
        .levels
        .keys()
        .cloned()
        .sorted()
        .collect_vec();
    assert_eq!(
        original_groups,
        vec![
            StaticCompactionGroupId::StateDefault,
            StaticCompactionGroupId::MaterializedView
        ]
    );

    hummock_manager
        .register_table_ids_for_test(&[(100, 2.into()), (101, 2.into())])
        .await
        .unwrap();
    let sst_1 = LocalSstableInfo {
        sst_info: gen_sstable_info(10, vec![100], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };
    let sst_2 = LocalSstableInfo {
        sst_info: gen_sstable_info(11, vec![100, 101], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };

    hummock_meta_client
        .commit_epoch(
            30,
            SyncResult {
                uncommitted_ssts: vec![sst_1, sst_2],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let compaction_group_id = StaticCompactionGroupId::StateDefault;
    let new_compaction_group_id = hummock_manager
        .move_state_tables_to_dedicated_compaction_group(compaction_group_id, &[100.into()], None)
        .await
        .unwrap()
        .0;

    let current_version = hummock_manager.get_current_version().await;
    let left_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    assert_eq!(new_compaction_group_id, left_compaction_group_id);
    assert_eq!(current_version.levels.len(), 3);

    let right_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 101).await;
    assert_eq!(
        get_compaction_group_object_ids(&current_version, right_compaction_group_id),
        vec![11]
    );
    assert_eq!(
        get_compaction_group_object_ids(&current_version, left_compaction_group_id),
        vec![10, 11]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(right_compaction_group_id)
            .iter()
            .map(|table_id| table_id.as_raw_id())
            .collect_vec(),
        vec![101]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(left_compaction_group_id)
            .iter()
            .map(|table_id| table_id.as_raw_id())
            .collect_vec(),
        vec![100]
    );

    hummock_manager
        .unregister_table_ids([TableId::new(101)])
        .await
        .unwrap();
    let current_version = hummock_manager.get_current_version().await;
    assert_eq!(current_version.levels.len(), 2);
    assert_eq!(
        get_compaction_group_object_ids(&current_version, left_compaction_group_id),
        vec![10, 11]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(left_compaction_group_id)
            .iter()
            .map(|table_id| table_id.as_raw_id())
            .collect_vec(),
        vec![100]
    );
}

#[tokio::test]
async fn test_merge_compaction_group_task_expired() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let original_groups = hummock_manager
        .get_current_version()
        .await
        .levels
        .keys()
        .cloned()
        .sorted()
        .collect_vec();
    assert_eq!(original_groups, vec![2, 3]);

    hummock_manager
        .register_table_ids_for_test(&[(100, 2.into()), (101, 2.into())])
        .await
        .unwrap();
    let sst_1 = LocalSstableInfo {
        sst_info: gen_sstable_info(1, vec![100], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };

    let sst_2 = LocalSstableInfo {
        sst_info: gen_sstable_info(2, vec![100, 101], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };
    let sst_3 = LocalSstableInfo {
        sst_info: SstableInfoInner {
            sst_id: 3.into(),
            object_id: 3.into(),
            ..sst_2.sst_info.get_inner()
        }
        .into(),
        ..sst_2.clone()
    };
    let sst_4 = LocalSstableInfo {
        sst_info: SstableInfoInner {
            sst_id: 4.into(),
            object_id: 4.into(),
            ..sst_1.sst_info.get_inner()
        }
        .into(),
        ..sst_1.clone()
    };
    hummock_meta_client
        .commit_epoch(
            30,
            SyncResult {
                uncommitted_ssts: vec![sst_1, sst_2, sst_3, sst_4],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Now group 2 has member tables [100,101,102], so split [100, 101] can succeed even though
    // there is no data of 102.
    hummock_manager
        .register_table_ids_for_test(&[(102, 2.into())])
        .await
        .unwrap();
    let compaction_group_id = StaticCompactionGroupId::StateDefault;

    // for task pending
    let _task = hummock_manager
        .get_compact_task(compaction_group_id, &mut *default_compaction_selector())
        .await
        .unwrap()
        .unwrap();

    // will cancel the task
    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(compaction_group_id, &[100.into()], None)
        .await
        .unwrap();

    let current_version = hummock_manager.get_current_version().await;
    let left_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    let right_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 101).await;
    assert_eq!(current_version.levels.len(), 3);
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(right_compaction_group_id)
            .iter()
            .map(|table_id| table_id.as_raw_id())
            .sorted()
            .collect_vec(),
        vec![101, 102]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(left_compaction_group_id)
            .iter()
            .map(|table_id| table_id.as_raw_id())
            .collect_vec(),
        vec![100]
    );

    let current_version_2 = hummock_manager.get_current_version().await;
    // The compaction task is cancelled or failed.
    assert_eq!(current_version_2, current_version);

    let task2 = hummock_manager
        .get_compact_task(
            right_compaction_group_id,
            &mut *default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();

    let created_tables = HashSet::from_iter(vec![100.into(), 101.into(), 102.into()]);

    // will cancel the task2
    hummock_manager
        .merge_compaction_group_for_test(
            left_compaction_group_id,
            right_compaction_group_id,
            created_tables,
        )
        .await
        .unwrap();

    let report_sst_id: HummockSstableId = 100.into();
    let ret = hummock_manager
        .report_compact_task(
            task2.task_id,
            TaskStatus::Success,
            vec![
                SstableInfoInner {
                    object_id: report_sst_id.as_raw_id().into(),
                    sst_id: report_sst_id,
                    key_range: KeyRange::default(),
                    table_ids: vec![100.into()],
                    min_epoch: 20,
                    max_epoch: 20,
                    file_size: 100,
                    sst_size: 100,
                    ..Default::default()
                }
                .into(),
            ],
            None,
            HashMap::default(),
        )
        .await
        .unwrap();
    // has been cancelled by merge
    assert!(!ret);

    let current_version_3 = hummock_manager.get_current_version().await;
    let sst_ids = current_version_3
        .get_sst_ids_by_group_id(left_compaction_group_id)
        .collect::<HashSet<_>>();
    // task 2 report failed due to the compaction group is merged
    assert!(!sst_ids.contains(&report_sst_id));
}

#[tokio::test]
async fn test_vacuum() {
    let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let context_id = worker_id;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        context_id,
    ));
    assert_eq!(hummock_manager.delete_version_deltas().await.unwrap(), 0);
    hummock_manager.pin_version(context_id).await.unwrap();
    let compaction_group_id = StaticCompactionGroupId::StateDefault;
    let sst_infos = add_test_tables(
        hummock_manager.as_ref(),
        hummock_meta_client.clone(),
        compaction_group_id,
    )
    .await;
    assert_eq!(hummock_manager.delete_version_deltas().await.unwrap(), 0);
    hummock_manager.create_version_checkpoint(1).await.unwrap();
    assert_eq!(hummock_manager.delete_version_deltas().await.unwrap(), 6);
    assert_eq!(hummock_manager.delete_version_deltas().await.unwrap(), 0);

    hummock_manager
        .unpin_version_before(context_id, MAX_HUMMOCK_VERSION_ID)
        .await
        .unwrap();
    hummock_manager.create_version_checkpoint(0).await.unwrap();
    let deleted = hummock_manager
        .complete_gc_batch(
            sst_infos
                .iter()
                .flat_map(|ssts| ssts.iter().map(|s| HummockObjectId::Sstable(s.object_id)))
                .collect(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(deleted, 3);
}

#[tokio::test]
async fn test_old_version_dropped_table_sst_does_not_make_new_compaction_fail() {
    let data_directory = format!(
        "hummock_recover_prune_stale_table_ids_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let env = MetaSrvEnv::for_test_opts(MetaOpts::test(false), |params| {
        // Use shared memory so the second manager can read the dirty checkpoint written by the
        // first manager, while a unique data directory keeps this test isolated.
        params.state_store = Some("hummock+memory".to_owned());
        params.data_directory = Some(data_directory);
    })
    .await;
    let initial_manager = build_hummock_manager_for_test_env(env.clone()).await;

    let compaction_group_id = StaticCompactionGroupId::StateDefault;
    let live_table_id = TableId::new(100);
    let stale_table_id = TableId::new(200);

    let mut dirty_version = HummockVersion::create_init_version(Arc::new(
        CompactionConfigBuilder::new()
            .level0_sub_level_compact_level_count(2)
            .build(),
    ));
    dirty_version.state_table_info =
        HummockVersionStateTableInfo::from_protobuf_owned(HashMap::from_iter([(
            live_table_id,
            StateTableInfo {
                committed_epoch: test_epoch(1),
                compaction_group_id,
            },
        )]));

    let live_and_stale_sst = gen_sstable_info(
        10,
        vec![live_table_id.as_raw_id(), stale_table_id.as_raw_id()],
        test_epoch(1),
    );
    let stale_only_sst = gen_sstable_info(11, vec![stale_table_id.as_raw_id()], test_epoch(1));
    let live_only_sst = gen_sstable_info(12, vec![live_table_id.as_raw_id()], test_epoch(1));
    let another_live_only_sst =
        gen_sstable_info(13, vec![live_table_id.as_raw_id()], test_epoch(1));
    let dirty_l0_sub_levels = vec![
        live_and_stale_sst,
        stale_only_sst,
        live_only_sst,
        another_live_only_sst,
    ]
    .into_iter()
    .enumerate()
    .map(|(idx, sst)| Level {
        level_idx: 0,
        level_type: PbLevelType::Nonoverlapping,
        total_file_size: sst.sst_size,
        uncompressed_file_size: sst.uncompressed_file_size,
        sub_level_id: idx as u64,
        table_infos: vec![sst],
        vnode_partition_count: 0,
    })
    .collect_vec();
    let group = dirty_version.levels.get_mut(&compaction_group_id).unwrap();
    group.l0.total_file_size = dirty_l0_sub_levels
        .iter()
        .map(|level| level.total_file_size)
        .sum();
    group.l0.uncompressed_file_size = dirty_l0_sub_levels
        .iter()
        .map(|level| level.uncompressed_file_size)
        .sum();
    group.l0.sub_levels = dirty_l0_sub_levels;

    // Simulate the upgrade path:
    // 1. an old version wrote SST metadata containing table ids [live, dropped];
    // 2. the old version dropped one table, but the checkpoint still had stale SST table ids;
    // 3. the new version recovers this checkpoint and picks a compaction task.
    //
    // After checkpoint and delta replay, recovery should normalize the recovered current version
    // before publishing it. The normalization should retain only live `state_table_info` table ids
    // in SST metadata and remove SSTs whose `table_ids` become empty. Otherwise the new compactor
    // will pass the stale read set to catalog acquire and fail before compaction can run.
    initial_manager
        .write_checkpoint(&HummockVersionCheckpoint::new(
            Arc::new(dirty_version),
            Default::default(),
        ))
        .await
        .unwrap();

    let recovered_manager = build_hummock_manager_for_test_env(env).await;
    let compact_task = recovered_manager
        .get_compact_task(compaction_group_id, &mut *default_compaction_selector())
        .await
        .unwrap()
        .expect("dirty old-version SST should be picked for compaction");
    let compact_table_ids = compact_task.get_table_ids_from_input_ssts().collect_vec();

    assert!(
        !compact_table_ids.contains(&stale_table_id),
        "new-version compaction task must not read dropped table id; otherwise compactor catalog acquire will fail. read_table_ids: {:?}, task: {}",
        compact_table_ids,
        compact_task_to_string(&compact_task),
    );
    let recovered_version = recovered_manager.get_current_version().await;
    let recovered_group_levels = recovered_version.get_compaction_group_levels(compaction_group_id);
    let recovered_ssts = recovered_group_levels
        .l0
        .sub_levels
        .iter()
        .chain(recovered_group_levels.levels.iter())
        .flat_map(|level| level.table_infos.iter())
        .collect_vec();
    let stale_table_sst_ids = recovered_ssts
        .iter()
        .filter(|sst| sst.table_ids.contains(&stale_table_id))
        .map(|sst| sst.sst_id)
        .collect_vec();
    assert!(
        stale_table_sst_ids.is_empty(),
        "recovered version should prune stale table ids from SST metadata: {:?}",
        stale_table_sst_ids
    );
    let recovered_sst_ids = recovered_ssts.iter().map(|sst| sst.sst_id).collect_vec();
    assert!(
        !recovered_sst_ids.contains(&HummockSstableId::new(11)),
        "SSTs whose table_ids become empty after pruning should be removed from the recovered version: {:?}",
        recovered_sst_ids
    );
}

/// Regression test: `try_merge_compaction_group` must return `Err` for groups
/// with overlapping table ID ranges. Before the fix, errors were swallowed
/// (always returned `Ok(())`), causing the merge scheduling loop to retry
/// the same unmergeable pair infinitely (167K+ error log spam).
#[tokio::test]
async fn test_try_merge_compaction_group_error_propagation() {
    let config = CompactionConfigBuilder::new()
        .level0_tier_compact_file_number(1)
        .level0_max_compact_file_number(130)
        .level0_sub_level_compact_level_count(1)
        .level0_overlapping_sub_level_compact_level_count(1)
        .level0_stop_write_threshold_sub_level_number(10)
        .level0_stop_write_threshold_max_sst_count(Some(6))
        .emergency_level0_sst_file_count(Some(100))
        .build();
    let (_env, hummock_manager, _, worker_id) = setup_compute_env_with_config(80, config).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    // StateDefault(2): [200, 202], MaterializedView(3): [100, 201]
    hummock_manager
        .register_table_ids_for_test(&[(200, 2.into()), (202, 2.into())])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(100, 3.into()), (201, 3.into())])
        .await
        .unwrap();

    // Commit SSTs so that split can proceed
    let epoch = test_epoch(20);
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: vec![
                    gen_local_sstable_info(1, vec![200, 202], epoch),
                    gen_local_sstable_info(2, vec![100, 201], epoch),
                    gen_local_sstable_info(3, vec![100, 201], epoch),
                    gen_local_sstable_info(4, vec![100, 201], epoch),
                    gen_local_sstable_info(5, vec![100, 201], epoch),
                ],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Split table 201 out of MV(3) → Group_X = [201].
    // Now StateDefault(2)=[200,202] and Group_X=[201] have overlapping table ID ranges.
    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(
            StaticCompactionGroupId::MaterializedView,
            &[201.into()],
            None,
        )
        .await
        .unwrap();

    let state_default_id: CompactionGroupId = StaticCompactionGroupId::StateDefault;
    let mv_id: CompactionGroupId = StaticCompactionGroupId::MaterializedView;
    let group_x_id = get_compaction_group_id_by_table_id(hummock_manager.clone(), 201).await;
    assert_ne!(state_default_id, group_x_id);
    assert_ne!(mv_id, group_x_id);

    let group_infos = hummock_manager.calculate_compaction_group_statistic().await;
    let sd_stat = group_infos
        .iter()
        .find(|g| g.group_id == state_default_id)
        .unwrap();
    let gx_stat = group_infos
        .iter()
        .find(|g| g.group_id == group_x_id)
        .unwrap();
    let mv_stat = group_infos.iter().find(|g| g.group_id == mv_id).unwrap();

    let throughput_manager = TableWriteThroughputStatisticManager::new(100);
    let created_tables: HashSet<TableId> =
        HashSet::from_iter(vec![100.into(), 200.into(), 201.into(), 202.into()]);

    // Group_X and MaterializedView are each below the L0 SST count write-stop threshold, but the
    // merged group should be rejected by the post-merge L0 SST count check.
    let result = hummock_manager
        .try_merge_compaction_group(&throughput_manager, gx_stat, mv_stat, &created_tables)
        .await;
    let err =
        result.expect_err("try_merge_compaction_group must reject merged L0 SST count write stop");
    assert!(
        err.as_report()
            .to_string()
            .contains("will trigger write stop after merge"),
        "unexpected merge rejection: {}",
        err.as_report()
    );

    // Must return Err for overlapping groups — guards against the swallowed-error regression.
    let result = hummock_manager
        .try_merge_compaction_group(&throughput_manager, sd_stat, gx_stat, &created_tables)
        .await;
    assert!(
        result.is_err(),
        "try_merge_compaction_group must return Err for overlapping groups, got Ok",
    );
}

#[tokio::test]
async fn test_normalize_overlapping_compaction_groups_cascading() {
    let (_env, hummock_manager, _, _worker_id) = setup_compute_env(80).await;

    // StateDefault(2): [64,80], MaterializedView(3): [65,81,83]
    hummock_manager
        .register_table_ids_for_test(&[(64, 2.into()), (80, 2.into())])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(65, 3.into()), (81, 3.into()), (83, 3.into())])
        .await
        .unwrap();

    let cg_64 = get_compaction_group_id_by_table_id(hummock_manager.clone(), 64).await;
    let cg_65 = get_compaction_group_id_by_table_id(hummock_manager.clone(), 65).await;

    let split_count = hummock_manager
        .normalize_overlapping_compaction_groups()
        .await
        .unwrap();
    assert_eq!(split_count, 2);

    let current_version = hummock_manager.get_current_version().await;

    let members_64 = current_version
        .state_table_info
        .compaction_group_member_table_ids(cg_64)
        .iter()
        .map(|table_id| table_id.as_raw_id())
        .collect_vec();
    let members_65 = current_version
        .state_table_info
        .compaction_group_member_table_ids(cg_65)
        .iter()
        .map(|table_id| table_id.as_raw_id())
        .collect_vec();
    assert_eq!(members_64, vec![64]);
    assert_eq!(members_65, vec![65]);
    assert_eq!(
        all_group_member_table_ids(&current_version),
        vec![64, 65, 80, 81, 83]
    );
    assert_eq!(
        current_version
            .levels
            .keys()
            .filter(|group_id| {
                !current_version
                    .state_table_info
                    .compaction_group_member_table_ids(**group_id)
                    .is_empty()
            })
            .count(),
        4,
        "normalize should create exactly two new non-empty groups"
    );
    assert_compaction_groups_non_overlapping(&current_version);
}

#[tokio::test]
async fn test_normalize_overlapping_compaction_groups_with_limit() {
    let (_env, hummock_manager, _, _worker_id) = setup_compute_env(80).await;

    hummock_manager
        .register_table_ids_for_test(&[(64, 2.into()), (80, 2.into())])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(65, 3.into()), (81, 3.into()), (83, 3.into())])
        .await
        .unwrap();

    let split_count = hummock_manager
        .normalize_overlapping_compaction_groups_with_limit(1)
        .await
        .unwrap();
    assert_eq!(split_count, 1);

    let current_version = hummock_manager.get_current_version().await;
    assert!(has_overlapping_compaction_groups(&current_version));

    let split_count = hummock_manager
        .normalize_overlapping_compaction_groups_with_limit(1)
        .await
        .unwrap();
    assert_eq!(split_count, 1);

    let current_version = hummock_manager.get_current_version().await;
    assert_compaction_groups_non_overlapping(&current_version);
}

#[tokio::test]
async fn test_normalize_overlapping_compaction_groups_respects_disable_auto_group_scheduling() {
    let (_env, hummock_manager, _, _worker_id) = setup_compute_env(80).await;

    hummock_manager
        .register_table_ids_for_test(&[(64, 2.into()), (80, 2.into())])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(65, 3.into()), (81, 3.into())])
        .await
        .unwrap();

    let cg_64 = get_compaction_group_id_by_table_id(hummock_manager.clone(), 64).await;
    hummock_manager
        .update_compaction_config(&[cg_64], &[MutableConfig::DisableAutoGroupScheduling(true)])
        .await
        .unwrap();

    let split_count = hummock_manager
        .normalize_overlapping_compaction_groups()
        .await
        .unwrap();
    assert_eq!(split_count, 0);

    let current_version = hummock_manager.get_current_version().await;
    let members = current_version
        .state_table_info
        .compaction_group_member_table_ids(cg_64)
        .iter()
        .map(|table_id| table_id.as_raw_id())
        .collect_vec();
    assert_eq!(members, vec![64, 80]);
}

#[tokio::test]
async fn test_normalize_overlapping_compaction_groups_respects_disable_auto_group_scheduling_on_right()
 {
    let (_env, hummock_manager, _, _worker_id) = setup_compute_env(80).await;

    hummock_manager
        .register_table_ids_for_test(&[(10, 2.into()), (30, 2.into()), (40, 2.into())])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(20, 3.into()), (50, 3.into())])
        .await
        .unwrap();

    let cg_20 = get_compaction_group_id_by_table_id(hummock_manager.clone(), 20).await;
    hummock_manager
        .update_compaction_config(&[cg_20], &[MutableConfig::DisableAutoGroupScheduling(true)])
        .await
        .unwrap();

    let split_count = hummock_manager
        .normalize_overlapping_compaction_groups()
        .await
        .unwrap();
    assert_eq!(split_count, 0);

    let current_version = hummock_manager.get_current_version().await;
    let ranges = compaction_group_ranges(&current_version);
    assert_eq!(ranges, vec![(10, 40), (20, 50)]);
}

#[tokio::test]
async fn test_normalize_overlapping_compaction_groups_skips_disabled_boundary_but_continues_later_segment()
 {
    let (_env, hummock_manager, _, _worker_id) = setup_compute_env(80).await;

    hummock_manager
        .register_table_ids_for_test(&[
            (10, 2.into()),
            (15, 2.into()),
            (35, 2.into()),
            (20, 3.into()),
            (40, 3.into()),
        ])
        .await
        .unwrap();

    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(
            StaticCompactionGroupId::StateDefault,
            &[35.into()],
            None,
        )
        .await
        .unwrap();

    let cg_10 = get_compaction_group_id_by_table_id(hummock_manager.clone(), 10).await;
    hummock_manager
        .update_compaction_config(&[cg_10], &[MutableConfig::DisableAutoGroupScheduling(true)])
        .await
        .unwrap();

    let split_count = hummock_manager
        .normalize_overlapping_compaction_groups()
        .await
        .unwrap();
    assert_eq!(split_count, 1);

    let current_version = hummock_manager.get_current_version().await;
    let members = current_version
        .state_table_info
        .compaction_group_member_table_ids(cg_10)
        .iter()
        .map(|table_id| table_id.as_raw_id())
        .collect_vec();
    assert_eq!(members, vec![10, 15]);
    assert_compaction_groups_non_overlapping(&current_version);
}

#[tokio::test]
async fn test_normalize_overlapping_compaction_groups_noop_on_non_overlapping_input() {
    let (_env, hummock_manager, _, _worker_id) = setup_compute_env(80).await;

    hummock_manager
        .register_table_ids_for_test(&[(10, 2.into()), (11, 2.into())])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(20, 3.into()), (21, 3.into())])
        .await
        .unwrap();

    let split_count = hummock_manager
        .normalize_overlapping_compaction_groups()
        .await
        .unwrap();
    assert_eq!(split_count, 0);
}

#[tokio::test]
async fn test_normalize_overlapping_compaction_groups_cancels_expired_compact_tasks() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    hummock_manager
        .register_table_ids_for_test(&[(64, 2.into()), (80, 2.into())])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(65, 3.into()), (81, 3.into())])
        .await
        .unwrap();

    let cg_64 = get_compaction_group_id_by_table_id(hummock_manager.clone(), 64).await;
    let epoch = test_epoch(1);
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: vec![gen_local_sstable_info(1, vec![64, 80], epoch)],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let task = hummock_manager
        .get_compact_task(cg_64, &mut *default_compaction_selector())
        .await
        .unwrap()
        .expect("should produce a compaction task");
    let task_id = task.task_id;

    assert!(
        !hummock_manager
            .compaction
            .read_with_process_name(
                "test_normalize_overlapping_compaction_groups_cancels_expired_compact_tasks",
            )
            .await
            .get_compact_task_assignments_by_group_id(cg_64)
            .is_empty()
    );

    let split_count = hummock_manager
        .normalize_overlapping_compaction_groups()
        .await
        .unwrap();
    assert!(split_count > 0);
    assert!(
        hummock_manager
            .compaction
            .read_with_process_name(
                "test_normalize_overlapping_compaction_groups_cancels_expired_compact_tasks",
            )
            .await
            .get_compact_task_assignments_by_group_id(cg_64)
            .is_empty(),
        "normalize should eagerly cancel parent-group task assignments"
    );

    let report_ok = hummock_manager
        .report_compact_task(
            task_id,
            TaskStatus::Success,
            vec![],
            None,
            HashMap::default(),
        )
        .await
        .unwrap();
    assert!(!report_ok);
}

#[tokio::test]
async fn test_normalize_overlapping_compaction_groups_refreshes_write_limits() {
    let config = CompactionConfigBuilder::new()
        .level0_tier_compact_file_number(1)
        .level0_max_compact_file_number(130)
        .level0_sub_level_compact_level_count(1)
        .level0_overlapping_sub_level_compact_level_count(1)
        .level0_stop_write_threshold_sub_level_number(1)
        .build();
    let (_env, hummock_manager, _, worker_id) =
        setup_compute_env_with_metric(80, config, None).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    hummock_manager
        .register_table_ids_for_test(&[(64, 2.into()), (80, 2.into())])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(65, 3.into())])
        .await
        .unwrap();

    let cg_64 = get_compaction_group_id_by_table_id(hummock_manager.clone(), 64).await;

    hummock_meta_client
        .commit_epoch(
            test_epoch(1),
            SyncResult {
                uncommitted_ssts: vec![gen_local_sstable_info(1, vec![64], test_epoch(1))],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    hummock_meta_client
        .commit_epoch(
            test_epoch(2),
            SyncResult {
                uncommitted_ssts: vec![gen_local_sstable_info(2, vec![80], test_epoch(2))],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert!(
        hummock_manager.write_limits().await.contains_key(&cg_64),
        "the parent group should enter write stop before normalize"
    );

    let split_count = hummock_manager
        .normalize_overlapping_compaction_groups()
        .await
        .unwrap();
    assert!(split_count > 0);

    let write_limits = hummock_manager.write_limits().await;
    assert!(
        write_limits.is_empty(),
        "normalize split should refresh write limits after rewriting L0 sub-levels"
    );
}

#[tokio::test]
async fn test_schedule_group_split_does_not_normalize_overlap_when_enabled() {
    let mut opts = MetaOpts::test(false);
    opts.enable_compaction_group_normalize = true;
    let (_env, hummock_manager, _, worker_id) =
        setup_compute_env_with_meta_opts(80, opts.clone()).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    hummock_manager
        .register_table_ids_for_test(&[
            (64, 2.into()),
            (80, 2.into()),
            (200, 2.into()),
            (201, 2.into()),
        ])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(65, 3.into()), (81, 3.into())])
        .await
        .unwrap();
    hummock_meta_client
        .commit_epoch(
            test_epoch(1),
            SyncResult {
                uncommitted_ssts: vec![gen_local_sstable_info(1, vec![200, 201], test_epoch(1))],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(2.into(), &[200.into(), 201.into()], None)
        .await
        .unwrap();

    hummock_manager
        .table_write_throughput_statistic_manager
        .write()
        .add_table_throughput_with_ts(
            200.into(),
            opts.table_high_write_throughput_threshold + 1,
            chrono::Utc::now().timestamp(),
        );

    assert_eq!(
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 200).await,
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 201).await
    );

    hummock_manager.schedule_group_split_for_test().await;

    let current_version = hummock_manager.get_current_version().await;
    assert!(has_overlapping_compaction_groups(&current_version));
    assert_eq!(
        all_group_member_table_ids(&current_version),
        vec![64, 65, 80, 81, 200, 201]
    );
    assert_eq!(non_empty_compaction_group_count(&current_version), 4);
    assert_ne!(
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 200).await,
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 201).await
    );
}

#[tokio::test]
async fn test_schedule_group_merge_with_normalize_disabled() {
    let (_env, hummock_manager, _, _worker_id) = setup_compute_env(80).await;

    hummock_manager
        .register_table_ids_for_test(&[(64, 2.into()), (80, 2.into())])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(65, 3.into()), (81, 3.into())])
        .await
        .unwrap();

    hummock_manager.schedule_group_merge_for_test().await;

    let current_version = hummock_manager.get_current_version().await;
    let ranges = compaction_group_ranges(&current_version);
    assert_eq!(ranges, vec![(64, 80), (65, 81)]);
}

#[tokio::test]
async fn test_schedule_group_split_with_normalize_disabled() {
    let (_env, hummock_manager, _, _worker_id) = setup_compute_env(80).await;

    hummock_manager
        .register_table_ids_for_test(&[(64, 2.into()), (80, 2.into())])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(65, 3.into()), (81, 3.into())])
        .await
        .unwrap();

    hummock_manager.schedule_group_split_for_test().await;

    let current_version = hummock_manager.get_current_version().await;
    let ranges = compaction_group_ranges(&current_version);
    assert_eq!(ranges, vec![(64, 80), (65, 81)]);
}

#[tokio::test]
async fn test_time_travel_vacuum_with_cross_database_epoch_order() {
    use risingwave_meta_model::{hummock_epoch_to_version, hummock_time_travel_version};
    use sea_orm::ActiveValue::Set;
    use sea_orm::{EntityTrait, PaginatorTrait};

    let (env, hummock_manager, _, _) = setup_compute_env(80).await;
    let table_a = TableId::new(1);
    let table_b = TableId::new(2);

    let mut version_10 = HummockVersion::default();
    version_10.id = 10.into();
    version_10.state_table_info =
        HummockVersionStateTableInfo::from_protobuf_owned(HashMap::from([
            (
                table_a,
                StateTableInfo {
                    committed_epoch: 300,
                    compaction_group_id: 1.into(),
                },
            ),
            (
                table_b,
                StateTableInfo {
                    committed_epoch: 100,
                    compaction_group_id: 1.into(),
                },
            ),
        ]));
    let mut version_11 = version_10.clone();
    version_11.id = 11.into();
    version_11.state_table_info =
        HummockVersionStateTableInfo::from_protobuf_owned(HashMap::from([
            (
                table_a,
                StateTableInfo {
                    committed_epoch: 300,
                    compaction_group_id: 1.into(),
                },
            ),
            (
                table_b,
                StateTableInfo {
                    committed_epoch: 200,
                    compaction_group_id: 1.into(),
                },
            ),
        ]));
    for version in [&version_10, &version_11] {
        hummock_time_travel_version::Entity::insert(hummock_time_travel_version::ActiveModel {
            version_id: Set(version.id),
            version: Set((&version.to_protobuf()).into()),
        })
        .exec(&env.meta_store_ref().conn)
        .await
        .unwrap();
    }
    for (table_id, epoch, version_id) in [
        (table_b, 100_u64, 10_u64),
        (table_b, 200, 11),
        (table_a, 300, 10),
    ] {
        hummock_epoch_to_version::Entity::insert(hummock_epoch_to_version::ActiveModel {
            epoch: Set(epoch.try_into().unwrap()),
            table_id: Set(i64::from(table_id.as_raw_id())),
            version_id: Set(version_id.into()),
        })
        .exec(&env.meta_store_ref().conn)
        .await
        .unwrap();
    }

    hummock_manager
        .truncate_time_travel_metadata(50, HashMap::new())
        .await
        .unwrap();
    assert_eq!(
        hummock_epoch_to_version::Entity::find()
            .count(&env.meta_store_ref().conn)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        hummock_time_travel_version::Entity::find()
            .count(&env.meta_store_ref().conn)
            .await
            .unwrap(),
        2
    );

    hummock_manager
        .truncate_time_travel_metadata(250, HashMap::new())
        .await
        .unwrap();

    assert_eq!(
        hummock_time_travel_version::Entity::find()
            .count(&env.meta_store_ref().conn)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        hummock_manager
            .epoch_to_version(300, table_a)
            .await
            .unwrap()
            .id,
        version_10.id
    );

    hummock_manager
        .truncate_time_travel_metadata(350, HashMap::new())
        .await
        .unwrap();
    assert_eq!(
        hummock_time_travel_version::Entity::find()
            .count(&env.meta_store_ref().conn)
            .await
            .unwrap(),
        1
    );
    assert!(
        hummock_time_travel_version::Entity::find_by_id(version_11.id)
            .one(&env.meta_store_ref().conn)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn test_time_travel_epoch_mapping_excludes_internal_tables() {
    use risingwave_hummock_sdk::version::HummockVersionDelta;
    use risingwave_meta_model::hummock_epoch_to_version;
    use sea_orm::{EntityTrait, TransactionTrait};

    let (env, hummock_manager, _, _) = setup_compute_env(80).await;
    env.system_params_manager_impl_ref()
        .set_param(
            "time_travel_retention_ms",
            Some((10 * 60 * 1000).to_string()),
        )
        .await
        .unwrap();

    let user_table_id = TableId::new(1);
    let internal_table_id = TableId::new(2);
    let compaction_group_id: CompactionGroupId = 1.into();
    let tables_to_commit = [
        (user_table_id, compaction_group_id, 100),
        (internal_table_id, compaction_group_id, 100),
    ];
    let mut delta = HummockVersionDelta::default();
    delta.id = 2.into();

    let txn = env.meta_store_ref().conn.begin().await.unwrap();
    hummock_manager
        .write_time_travel_metadata(
            &txn,
            None,
            delta,
            HashSet::from([user_table_id]),
            &HashSet::new(),
            tables_to_commit
                .iter()
                .map(|(table_id, compaction_group_id, epoch)| {
                    (table_id, compaction_group_id, *epoch)
                }),
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let epoch_mappings = hummock_epoch_to_version::Entity::find()
        .all(&env.meta_store_ref().conn)
        .await
        .unwrap();
    assert_eq!(epoch_mappings.len(), 1);
    assert_eq!(
        epoch_mappings[0].table_id,
        i64::from(user_table_id.as_raw_id())
    );
}

#[tokio::test]
async fn test_time_travel_vacuum_pins_snapshot_epoch() {
    use risingwave_hummock_sdk::level::Levels;
    use risingwave_meta_model::hummock_sstable_info::SstableInfoV2Backend;
    use risingwave_meta_model::{
        hummock_epoch_to_version, hummock_sstable_info, hummock_time_travel_delta,
        hummock_time_travel_version,
    };
    use sea_orm::ActiveValue::Set;
    use sea_orm::{EntityTrait, PaginatorTrait, QueryOrder, QuerySelect};

    async fn insert_version(env: &MetaSrvEnv, id: u64, sst: Option<SstableInfo>) {
        let mut version = HummockVersion::default();
        version.id = id.into();
        if let Some(sst) = sst {
            version.levels.insert(
                1.into(),
                Levels {
                    levels: vec![Level {
                        table_infos: vec![sst],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            );
        }
        hummock_time_travel_version::Entity::insert(hummock_time_travel_version::ActiveModel {
            version_id: Set(version.id),
            version: Set((&version.to_protobuf()).into()),
        })
        .exec(&env.meta_store_ref().conn)
        .await
        .unwrap();
    }

    async fn insert_sstable_info(env: &MetaSrvEnv, sst: &SstableInfo) {
        hummock_sstable_info::Entity::insert(hummock_sstable_info::ActiveModel {
            sst_id: Set(sst.sst_id),
            object_id: Set(sst.object_id),
            sstable_info: Set(SstableInfoV2Backend::from(&sst.to_protobuf())),
        })
        .exec(&env.meta_store_ref().conn)
        .await
        .unwrap();
    }

    async fn insert_delta(env: &MetaSrvEnv, id: u64, prev_id: u64) {
        let mut delta = risingwave_hummock_sdk::version::HummockVersionDelta::default();
        delta.id = id.into();
        delta.prev_id = prev_id.into();
        hummock_time_travel_delta::Entity::insert(hummock_time_travel_delta::ActiveModel {
            version_id: Set(delta.id),
            version_delta: Set((&delta.to_protobuf()).into()),
        })
        .exec(&env.meta_store_ref().conn)
        .await
        .unwrap();
    }

    async fn insert_epoch_mapping(env: &MetaSrvEnv, table_id: TableId, epoch: u64, version: u64) {
        hummock_epoch_to_version::Entity::insert(hummock_epoch_to_version::ActiveModel {
            epoch: Set(epoch.try_into().unwrap()),
            table_id: Set(i64::from(table_id.as_raw_id())),
            version_id: Set(version.into()),
        })
        .exec(&env.meta_store_ref().conn)
        .await
        .unwrap();
    }

    let mut opts = MetaOpts::test(false);
    opts.time_travel_vacuum_max_version_count = Some(1);
    let (env, hummock_manager, _, _) = setup_compute_env_with_meta_opts(80, opts).await;
    let table_id = TableId::new(1);
    let pinned_sst = gen_sstable_info(10, vec![table_id.as_raw_id()], test_epoch(1));
    insert_sstable_info(&env, &pinned_sst).await;
    insert_version(&env, 1, Some(pinned_sst)).await;
    insert_delta(&env, 2, 1).await;
    insert_delta(&env, 3, 2).await;
    insert_version(&env, 4, None).await;
    insert_version(&env, 5, None).await;
    insert_epoch_mapping(&env, table_id, 100, 2).await;
    insert_epoch_mapping(&env, table_id, 150, 3).await;
    insert_epoch_mapping(&env, table_id, 175, 3).await;
    insert_epoch_mapping(&env, table_id, 200, 4).await;
    insert_epoch_mapping(&env, table_id, 300, 5).await;
    insert_epoch_mapping(&env, TableId::new(2), 50, 6).await;

    hummock_manager
        .truncate_time_travel_metadata(
            250,
            HashMap::from([
                (table_id, HashSet::from([100, 125, 150])),
                (TableId::new(2), HashSet::from([50])),
                (TableId::new(999), HashSet::from([100, 250, 300])),
            ]),
        )
        .await
        .unwrap();

    let epoch_rows = hummock_epoch_to_version::Entity::find()
        .order_by_asc(hummock_epoch_to_version::Column::Epoch)
        .all(&env.meta_store_ref().conn)
        .await
        .unwrap();
    assert_eq!(
        epoch_rows
            .iter()
            .map(|row| u64::try_from(row.epoch).unwrap())
            .collect_vec(),
        vec![50, 100, 150, 300]
    );
    let version_ids: Vec<risingwave_meta_model::HummockVersionId> =
        hummock_time_travel_version::Entity::find()
            .select_only()
            .column(hummock_time_travel_version::Column::VersionId)
            .order_by_asc(hummock_time_travel_version::Column::VersionId)
            .into_tuple()
            .all(&env.meta_store_ref().conn)
            .await
            .unwrap();
    assert_eq!(
        version_ids.iter().map(|id| id.as_raw_id()).collect_vec(),
        vec![1, 5]
    );
    let delta_ids: Vec<risingwave_meta_model::HummockVersionId> =
        hummock_time_travel_delta::Entity::find()
            .select_only()
            .column(hummock_time_travel_delta::Column::VersionId)
            .order_by_asc(hummock_time_travel_delta::Column::VersionId)
            .into_tuple()
            .all(&env.meta_store_ref().conn)
            .await
            .unwrap();
    assert_eq!(
        delta_ids.iter().map(|id| id.as_raw_id()).collect_vec(),
        vec![2, 3]
    );
    assert_eq!(
        hummock_sstable_info::Entity::find()
            .count(&env.meta_store_ref().conn)
            .await
            .unwrap(),
        1
    );
    assert!(
        hummock_manager
            .gc_manager
            .try_take_may_delete_object_ids(1)
            .is_none()
    );
    assert_eq!(
        hummock_manager
            .epoch_to_version(100, table_id)
            .await
            .unwrap()
            .id
            .as_raw_id(),
        2
    );
    assert_eq!(
        hummock_manager
            .epoch_to_version(150, table_id)
            .await
            .unwrap()
            .id
            .as_raw_id(),
        3
    );
    assert_eq!(
        hummock_manager
            .epoch_to_version(300, table_id)
            .await
            .unwrap()
            .id
            .as_raw_id(),
        5
    );

    hummock_manager
        .truncate_time_travel_metadata(350, HashMap::new())
        .await
        .unwrap();
    assert!(
        hummock_manager
            .epoch_to_version(100, table_id)
            .await
            .is_err()
    );
    assert_eq!(
        hummock_time_travel_version::Entity::find()
            .count(&env.meta_store_ref().conn)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        hummock_time_travel_delta::Entity::find()
            .count(&env.meta_store_ref().conn)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        hummock_sstable_info::Entity::find()
            .count(&env.meta_store_ref().conn)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        hummock_manager
            .gc_manager
            .try_take_may_delete_object_ids(1)
            .unwrap()
            .into_iter()
            .map(|object_id| object_id.as_raw().as_raw_id())
            .collect_vec(),
        vec![10]
    );
}
