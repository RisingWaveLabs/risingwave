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
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use parking_lot::Mutex;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::change_log::{EpochNewChangeLog, TableChangeLog};
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_watermark::TableWatermarks;
use risingwave_hummock_sdk::vector_index::VectorIndexDelta;
use risingwave_hummock_sdk::version::{GroupDelta, HummockVersion, HummockVersionDelta};
use risingwave_hummock_sdk::{
    CompactionGroupId, FrontendHummockVersionDelta, HummockSstableId, HummockVersionId,
};
use risingwave_pb::hummock::{
    CompatibilityVersion, GroupConstruct, HummockVersionDeltas, HummockVersionStats,
    StateTableInfoDelta,
};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use sea_orm::{ConnectionTrait, EntityTrait};

use super::TableCommittedEpochNotifiers;
use crate::hummock::model::CompactionGroup;
use crate::hummock::model::ext::to_table_change_log_meta_store_model;
use crate::manager::{MetaOpts, NotificationManager};
use crate::model::{
    InMemValTransaction, MetadataModelResult, Transactional, ValTransaction, VarTransaction,
};
use crate::rpc::metrics::MetaMetrics;

fn trigger_delta_log_stats(metrics: &MetaMetrics, total_number: usize) {
    metrics.delta_log_count.set(total_number as _);
}

#[derive(Default)]
struct TableChangeLogTransactionDelta {
    updates: Vec<HashMap<TableId, EpochNewChangeLog>>,
    delete_all: HashSet<TableId>,
}

pub(super) fn trigger_version_stat(metrics: &MetaMetrics, current_version: &HummockVersion) {
    metrics
        .version_size
        .set(current_version.estimated_encode_len() as i64);
    metrics
        .current_version_id
        .set(current_version.id.as_i64_id());
}

pub(super) struct HummockVersionTransaction<'a> {
    orig_version: &'a mut Arc<HummockVersion>,
    orig_deltas: &'a mut BTreeMap<HummockVersionId, HummockVersionDelta>,
    orig_table_change_log: &'a mut HashMap<TableId, TableChangeLog>,
    notification_manager: &'a NotificationManager,
    table_committed_epoch_notifiers: Option<&'a Mutex<TableCommittedEpochNotifiers>>,
    meta_metrics: &'a MetaMetrics,
    version_stat_tx: &'a tokio::sync::mpsc::UnboundedSender<Arc<HummockVersion>>,

    pre_applied_version: Option<(
        HummockVersion,
        Vec<HummockVersionDelta>,
        TableChangeLogTransactionDelta,
    )>,
    disable_apply_to_txn: bool,
    opts: &'a MetaOpts,
}

impl<'a> HummockVersionTransaction<'a> {
    pub(super) fn new(
        version: &'a mut Arc<HummockVersion>,
        deltas: &'a mut BTreeMap<HummockVersionId, HummockVersionDelta>,
        table_change_log: &'a mut HashMap<TableId, TableChangeLog>,
        notification_manager: &'a NotificationManager,
        table_committed_epoch_notifiers: Option<&'a Mutex<TableCommittedEpochNotifiers>>,
        meta_metrics: &'a MetaMetrics,
        opts: &'a MetaOpts,
        version_stat_tx: &'a tokio::sync::mpsc::UnboundedSender<Arc<HummockVersion>>,
    ) -> Self {
        Self {
            orig_version: version,
            orig_deltas: deltas,
            orig_table_change_log: table_change_log,
            pre_applied_version: None,
            disable_apply_to_txn: false,
            notification_manager,
            table_committed_epoch_notifiers,
            meta_metrics,
            opts,
            version_stat_tx,
        }
    }

    pub(super) fn disable_apply_to_txn(&mut self) {
        assert!(
            self.pre_applied_version.is_none(),
            "should only call disable at the beginning of txn"
        );
        self.disable_apply_to_txn = true;
    }

    pub(super) fn latest_version(&self) -> &HummockVersion {
        if let Some((version, _, _)) = &self.pre_applied_version {
            version
        } else {
            self.orig_version.as_ref()
        }
    }

    pub(super) fn new_delta<'b>(&'b mut self) -> SingleDeltaTransaction<'a, 'b> {
        let delta = self.latest_version().version_delta_after();
        SingleDeltaTransaction {
            version_txn: self,
            delta: Some((delta, HashMap::new())),
        }
    }

    fn pre_apply(
        &mut self,
        delta: HummockVersionDelta,
        change_log_updates: HashMap<TableId, EpochNewChangeLog>,
    ) {
        let (version, deltas, table_change_log_delta) =
            self.pre_applied_version.get_or_insert_with(|| {
                (
                    self.orig_version.as_ref().clone(),
                    Vec::with_capacity(1),
                    TableChangeLogTransactionDelta::default(),
                )
            });
        let changed_table_info = version.apply_version_delta(&delta);
        // The in-memory change logs are only updated after the metastore transaction succeeds, so
        // complete deletion is derived from the original state. A deletion that becomes eligible
        // after multiple deltas in one transaction may be deferred to the next transaction.
        let delete_all = HummockVersion::collect_gc_change_log_delta(
            self.orig_table_change_log.keys(),
            &change_log_updates,
            &delta.removed_table_ids,
            &delta.state_table_info_delta,
            &changed_table_info,
        );
        table_change_log_delta.delete_all.extend(delete_all);
        if !change_log_updates.is_empty() {
            table_change_log_delta.updates.push(change_log_updates);
        }
        deltas.push(delta);
    }

    /// Returns a duplicate delta, used by time travel.
    pub(super) fn pre_commit_epoch(
        &mut self,
        tables_to_commit: &HashMap<TableId, u64>,
        new_compaction_groups: Vec<CompactionGroup>,
        group_id_to_sub_levels: BTreeMap<CompactionGroupId, Vec<Vec<SstableInfo>>>,
        new_table_ids: &HashMap<TableId, CompactionGroupId>,
        new_table_watermarks: HashMap<TableId, TableWatermarks>,
        change_log_delta: HashMap<TableId, EpochNewChangeLog>,
        vector_index_delta: HashMap<TableId, VectorIndexDelta>,
        group_id_to_truncate_tables: HashMap<CompactionGroupId, HashSet<TableId>>,
    ) -> HummockVersionDelta {
        let mut new_version_delta = self.new_delta();
        new_version_delta.new_table_watermarks = new_table_watermarks;
        new_version_delta.set_change_log_delta(change_log_delta);
        new_version_delta.vector_index_delta = vector_index_delta;

        for compaction_group in &new_compaction_groups {
            let group_deltas = &mut new_version_delta
                .group_deltas
                .entry(compaction_group.group_id())
                .or_default()
                .group_deltas;

            #[expect(deprecated)]
            group_deltas.push(GroupDelta::GroupConstruct(Box::new(GroupConstruct {
                group_config: Some(compaction_group.compaction_config().as_ref().clone()),
                group_id: compaction_group.group_id(),
                parent_group_id: StaticCompactionGroupId::NewCompactionGroup as CompactionGroupId,
                new_sst_start_id: HummockSstableId::default(), // No need to set it when `NewCompactionGroup`
                table_ids: vec![],
                version: CompatibilityVersion::LATEST as _,
                split_key: None,
            })));
        }

        // Append SSTs to a new version.
        for (compaction_group_id, sub_levels) in group_id_to_sub_levels {
            let group_deltas = &mut new_version_delta
                .group_deltas
                .entry(compaction_group_id)
                .or_default()
                .group_deltas;

            for sub_level in sub_levels {
                group_deltas.push(GroupDelta::NewL0SubLevel(sub_level));
            }
        }

        for (compaction_group_id, table_ids) in group_id_to_truncate_tables {
            let group_deltas = &mut new_version_delta
                .group_deltas
                .entry(compaction_group_id)
                .or_default()
                .group_deltas;

            group_deltas.push(GroupDelta::PruneTableIdsFromSsts(
                table_ids.into_iter().collect(),
            ));
        }

        // update state table info
        new_version_delta.with_latest_version(|version, delta| {
            for (table_id, cg_id) in new_table_ids {
                assert!(
                    !version.state_table_info.info().contains_key(table_id),
                    "newly added table exists previously: {:?}",
                    table_id
                );
                let committed_epoch = *tables_to_commit.get(table_id).expect("newly added table must exist in tables_to_commit");
                delta.state_table_info_delta.insert(
                    *table_id,
                    StateTableInfoDelta {
                        committed_epoch,
                        compaction_group_id: *cg_id,
                    },
                );
            }

            for (table_id, committed_epoch) in tables_to_commit {
                if new_table_ids.contains_key(table_id) {
                    continue;
                }
                let info = version.state_table_info.info().get(table_id).unwrap_or_else(|| {
                    panic!("tables_to_commit {:?} contains table_id {} that is not newly added but not exists previously", tables_to_commit, table_id);
                });
                assert!(delta
                    .state_table_info_delta
                    .insert(
                        *table_id,
                        StateTableInfoDelta {
                            committed_epoch: *committed_epoch,
                            compaction_group_id: info.compaction_group_id,
                        }
                    )
                    .is_none());
            }
        });

        let time_travel_delta = (*new_version_delta).clone();
        new_version_delta.pre_apply();
        time_travel_delta
    }
}

impl InMemValTransaction for HummockVersionTransaction<'_> {
    fn commit(self) {
        if let Some((version, deltas, table_change_log_delta)) = self.pre_applied_version {
            *self.orig_version = Arc::new(version);
            for change_log_delta in table_change_log_delta.updates {
                HummockVersion::apply_change_log_delta(
                    self.orig_table_change_log,
                    &change_log_delta,
                );
            }
            self.orig_table_change_log
                .retain(|table_id, _| !table_change_log_delta.delete_all.contains(table_id));

            if !self.disable_apply_to_txn {
                let pb_deltas = deltas.iter().map(|delta| delta.to_protobuf()).collect();
                self.notification_manager.notify_hummock_without_version(
                    Operation::Add,
                    Info::HummockVersionDeltas(risingwave_pb::hummock::HummockVersionDeltas {
                        version_deltas: pb_deltas,
                    }),
                );
                self.notification_manager.notify_frontend_without_version(
                    Operation::Update,
                    Info::HummockVersionDeltas(HummockVersionDeltas {
                        version_deltas: deltas
                            .iter()
                            .map(|delta| {
                                FrontendHummockVersionDelta::from_delta(delta).to_protobuf()
                            })
                            .collect(),
                    }),
                );
                if let Some(table_committed_epoch_notifiers) = self.table_committed_epoch_notifiers
                {
                    table_committed_epoch_notifiers
                        .lock()
                        .notify_deltas(&deltas);
                }
            }

            for delta in deltas {
                assert!(self.orig_deltas.insert(delta.id, delta.clone()).is_none());
            }

            trigger_delta_log_stats(self.meta_metrics, self.orig_deltas.len());
            let _ = self.version_stat_tx.send(self.orig_version.clone());
        }
    }
}

impl<TXN> ValTransaction<TXN> for HummockVersionTransaction<'_>
where
    TXN: ConnectionTrait,
    HummockVersionDelta: Transactional<TXN>,
    HummockVersionStats: Transactional<TXN>,
{
    async fn apply_to_txn(&self, txn: &mut TXN) -> MetadataModelResult<()> {
        if self.disable_apply_to_txn {
            return Ok(());
        }
        if let Some((_, deltas, table_change_log_delta)) = &self.pre_applied_version {
            // These upsert_in_transaction can be batched. However, we know len(deltas) is always 1 currently.
            for delta in deltas {
                delta.upsert_in_transaction(txn).await?;
            }

            let insert_batch_size = self.opts.table_change_log_insert_batch_size as usize;
            use futures::stream::{self, StreamExt};
            use sea_orm::{ColumnTrait, Condition, QueryFilter};
            let insert_iter = table_change_log_delta
                .updates
                .iter()
                .flat_map(|updates| updates.iter())
                .map(|(table_id, new_log)| (*table_id, new_log));
            let mut stream = stream::iter(insert_iter).chunks(insert_batch_size);
            while let Some(change_log_batch) = stream.next().await {
                let insert_many = change_log_batch
                    .into_iter()
                    .map(|(table_id, change_log)| {
                        to_table_change_log_meta_store_model(table_id, change_log)
                    })
                    .collect::<Vec<_>>();
                risingwave_meta_model::hummock_table_change_log::Entity::insert_many(insert_many)
                    .on_empty_do_nothing()
                    .exec(txn)
                    .await?;
            }

            let delete_batch_size = self.opts.table_change_log_delete_batch_size as usize;
            let delete_iter = table_change_log_delta.delete_all.iter().copied();

            let mut stream = stream::iter(delete_iter).chunks(delete_batch_size);
            while let Some(change_log_batch) = stream.next().await {
                let mut condition = Condition::any();
                for table_id in change_log_batch {
                    let table_condition = Condition::all().add(
                        risingwave_meta_model::hummock_table_change_log::Column::TableId
                            .eq(table_id),
                    );
                    condition = condition.add(table_condition);
                }
                risingwave_meta_model::hummock_table_change_log::Entity::delete_many()
                    .filter(condition)
                    .exec(txn)
                    .await?;
            }
        }
        Ok(())
    }
}

pub(super) struct SingleDeltaTransaction<'a, 'b> {
    version_txn: &'b mut HummockVersionTransaction<'a>,
    delta: Option<(HummockVersionDelta, HashMap<TableId, EpochNewChangeLog>)>,
}

impl SingleDeltaTransaction<'_, '_> {
    pub(super) fn latest_version(&self) -> &HummockVersion {
        self.version_txn.latest_version()
    }

    fn set_change_log_delta(&mut self, change_log_delta: HashMap<TableId, EpochNewChangeLog>) {
        self.delta.as_mut().expect("should exist").1 = change_log_delta;
    }

    pub(super) fn pre_apply(mut self) {
        let (delta, change_log_delta) = self.delta.take().unwrap();
        self.version_txn.pre_apply(delta, change_log_delta);
    }

    pub(super) fn with_latest_version(
        &mut self,
        f: impl FnOnce(&HummockVersion, &mut HummockVersionDelta),
    ) {
        f(
            self.version_txn.latest_version(),
            &mut self.delta.as_mut().expect("should exist").0,
        )
    }
}

impl Deref for SingleDeltaTransaction<'_, '_> {
    type Target = HummockVersionDelta;

    fn deref(&self) -> &Self::Target {
        &self.delta.as_ref().expect("should exist").0
    }
}

impl DerefMut for SingleDeltaTransaction<'_, '_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.delta.as_mut().expect("should exist").0
    }
}

impl Drop for SingleDeltaTransaction<'_, '_> {
    fn drop(&mut self) {
        if let Some((delta, change_log_delta)) = self.delta.take() {
            self.version_txn.pre_apply(delta, change_log_delta);
        }
    }
}

pub(super) struct HummockVersionStatsTransaction<'a> {
    stats: VarTransaction<'a, HummockVersionStats>,
    notification_manager: &'a NotificationManager,
}

impl<'a> HummockVersionStatsTransaction<'a> {
    pub(super) fn new(
        stats: &'a mut HummockVersionStats,
        notification_manager: &'a NotificationManager,
    ) -> Self {
        Self {
            stats: VarTransaction::new(stats),
            notification_manager,
        }
    }
}

impl InMemValTransaction for HummockVersionStatsTransaction<'_> {
    fn commit(self) {
        if self.stats.has_new_value() {
            let stats = self.stats.clone();
            self.stats.commit();
            self.notification_manager
                .notify_frontend_without_version(Operation::Update, Info::HummockStats(stats));
        }
    }
}

impl<TXN> ValTransaction<TXN> for HummockVersionStatsTransaction<'_>
where
    TXN: ConnectionTrait,
    HummockVersionStats: Transactional<TXN>,
{
    async fn apply_to_txn(&self, txn: &mut TXN) -> MetadataModelResult<()> {
        self.stats.apply_to_txn(txn).await
    }
}

impl Deref for HummockVersionStatsTransaction<'_> {
    type Target = HummockVersionStats;

    fn deref(&self) -> &Self::Target {
        self.stats.deref()
    }
}

impl DerefMut for HummockVersionStatsTransaction<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.stats.deref_mut()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_hummock_sdk::change_log::EpochNewChangeLog;
    use risingwave_pb::hummock::StateTableInfo;

    use super::*;

    fn new_change_log(checkpoint_epoch: u64) -> EpochNewChangeLog {
        EpochNewChangeLog {
            new_value: vec![],
            old_value: vec![],
            non_checkpoint_epochs: vec![],
            checkpoint_epoch,
        }
    }

    #[test]
    fn test_apply_change_log_delta() {
        let table_id = TableId::new(1);
        let mut table_change_logs = HashMap::from([(
            table_id,
            TableChangeLog::new([new_change_log(1), new_change_log(2)]),
        )]);
        HummockVersion::apply_change_log_delta(
            &mut table_change_logs,
            &HashMap::from([(table_id, new_change_log(3))]),
        );

        assert_eq!(
            table_change_logs[&table_id].epochs().collect_vec(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn test_collect_gc_change_log_delta() {
        let table_id = TableId::new(1);
        let removed_table_id = TableId::new(2);
        let current_table_ids = HashSet::from([table_id, removed_table_id]);
        let state_table_info_delta = HashMap::from([(
            table_id,
            StateTableInfoDelta {
                committed_epoch: 2,
                compaction_group_id: 1.into(),
            },
        )]);
        let changed_table_info = HashMap::from([(
            table_id,
            Some(StateTableInfo {
                committed_epoch: 1,
                compaction_group_id: 1.into(),
            }),
        )]);

        assert_eq!(
            HummockVersion::collect_gc_change_log_delta(
                current_table_ids.iter(),
                &HashMap::<TableId, EpochNewChangeLog>::new(),
                &HashSet::from([removed_table_id]),
                &state_table_info_delta,
                &changed_table_info,
            ),
            HashSet::from([table_id, removed_table_id])
        );
        assert_eq!(
            HummockVersion::collect_gc_change_log_delta(
                current_table_ids.iter(),
                &HashMap::from([(table_id, new_change_log(2))]),
                &HashSet::new(),
                &state_table_info_delta,
                &changed_table_info,
            ),
            HashSet::new()
        );
    }
}
