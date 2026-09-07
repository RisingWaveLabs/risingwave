// Copyright 2026 RisingWave Labs
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

use std::collections::{HashMap, HashSet};

use anyhow::{Context, anyhow};
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::Epoch;
use risingwave_hummock_sdk::change_log::TableChangeLog;
use risingwave_hummock_sdk::version::HummockVersion;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DbBackend, EntityTrait, FromQueryResult, QueryFilter,
    QueryTrait, TransactionTrait,
};

use crate::controller::streaming_job::TableChangeLogTruncateInfo;
use crate::hummock::HummockManager;
use crate::hummock::error::{Error, Result};
use crate::hummock::model::ext::to_table_change_log;

fn update_truncate_epoch(
    truncate_epochs: &mut HashMap<TableId, u64>,
    table_id: TableId,
    truncate_epoch: u64,
) {
    truncate_epochs
        .entry(table_id)
        .and_modify(|epoch| *epoch = (*epoch).min(truncate_epoch))
        .or_insert(truncate_epoch);
}

fn resolve_table_change_log_truncate_epochs(
    info: &TableChangeLogTruncateInfo,
    version: &HummockVersion,
    current_time_epoch: Epoch,
) -> anyhow::Result<HashMap<TableId, u64>> {
    let mut truncate_epochs = HashMap::new();
    let mut untruncatable_table_ids = HashSet::new();
    for (table_id, retention_seconds) in &info.subscription_retention_seconds {
        if version.table_committed_epoch(*table_id).is_none() {
            // A concurrently dropped table is cleaned up by its commit-epoch transaction.
            tracing::warn!(
                %table_id,
                "cannot get committed epoch for subscribed table, skip table change log truncation"
            );
            continue;
        }
        let truncate_epoch = current_time_epoch
            .subtract_ms(retention_seconds.saturating_mul(1000))
            .0;
        update_truncate_epoch(&mut truncate_epochs, *table_id, truncate_epoch);
    }

    for job in &info.independent_jobs {
        let mut all_snapshot_epochs_none = true;
        for (upstream_table_id, snapshot_epoch) in &job.upstream_table_snapshot_epochs {
            match snapshot_epoch {
                Some(_) => all_snapshot_epochs_none = false,
                None => {
                    // The independent job has not fixed a safe snapshot epoch yet. This vetoes
                    // truncation even when another consumer provides a concrete cutoff.
                    untruncatable_table_ids.insert(*upstream_table_id);
                }
            }
        }
        if all_snapshot_epochs_none {
            continue;
        }

        let mut state_table_ids = job.state_table_ids.iter();
        let first_table_id = state_table_ids
            .next()
            .ok_or_else(|| anyhow!("independent job {} has no state table", job.job_id))?;
        let committed_epoch = version
            .table_committed_epoch(*first_table_id)
            .ok_or_else(|| {
                anyhow!(
                    "cannot get committed epoch of state table {} in independent job {}",
                    first_table_id,
                    job.job_id
                )
            })?;
        for table_id in state_table_ids {
            let table_committed_epoch =
                version.table_committed_epoch(*table_id).ok_or_else(|| {
                    anyhow!(
                        "cannot get committed epoch of state table {} in independent job {}",
                        table_id,
                        job.job_id
                    )
                })?;
            if table_committed_epoch != committed_epoch {
                return Err(anyhow!(
                    "state tables {} and {} in independent job {} have different committed epochs {} and {}",
                    first_table_id,
                    table_id,
                    job.job_id,
                    committed_epoch,
                    table_committed_epoch
                ));
            }
        }

        for (upstream_table_id, snapshot_epoch) in &job.upstream_table_snapshot_epochs {
            if let Some(snapshot_epoch) = snapshot_epoch {
                let pinned_epoch = committed_epoch.max(*snapshot_epoch);
                update_truncate_epoch(&mut truncate_epochs, *upstream_table_id, pinned_epoch);
            }
        }
    }
    truncate_epochs.retain(|table_id, _| !untruncatable_table_ids.contains(table_id));
    Ok(truncate_epochs)
}

impl HummockManager {
    pub async fn truncate_table_change_log(&self, info: TableChangeLogTruncateInfo) -> Result<()> {
        let _timer = self.metrics.table_change_log_truncate_latency.start_timer();
        let mut versioning = self
            .versioning
            .write_with_process_name("truncate_table_change_log")
            .await;
        let current_time_epoch = Epoch::now();
        let truncate_epochs = resolve_table_change_log_truncate_epochs(
            &info,
            versioning.current_version.as_ref(),
            current_time_epoch,
        )
        .map_err(Error::Internal)?;
        let truncate_epochs: Vec<_> = truncate_epochs
            .into_iter()
            .filter(|(table_id, _)| versioning.table_change_log.contains_key(table_id))
            .collect();
        if truncate_epochs.is_empty() {
            return Ok(());
        }

        let sql_store = self.env.meta_store_ref();
        let txn = sql_store.conn.begin().await?;
        let batch_size = self.env.opts.table_change_log_delete_batch_size as usize;
        let mut rows_affected = 0;
        let mut may_delete_object_ids = HashSet::new();
        for batch in truncate_epochs.chunks(batch_size) {
            let mut condition = Condition::any();
            for (table_id, truncate_epoch) in batch {
                let truncate_epoch = risingwave_meta_model::Epoch::try_from(*truncate_epoch)
                    .context("table change log truncate epoch exceeds meta store range")
                    .map_err(Error::Internal)?;
                condition = condition.add(
                    Condition::all()
                        .add(
                            risingwave_meta_model::hummock_table_change_log::Column::TableId
                                .eq(*table_id),
                        )
                        .add(
                            risingwave_meta_model::hummock_table_change_log::Column::CheckpointEpoch
                                .lt(truncate_epoch),
                        ),
                );
            }
            let (change_logs_to_delete, deleted_count) = match txn.get_database_backend() {
                DbBackend::Postgres => {
                    let mut delete =
                        risingwave_meta_model::hummock_table_change_log::Entity::delete_many()
                            .filter(condition)
                            .into_query();
                    delete.returning_all();
                    let statement = DbBackend::Postgres.build(&delete);
                    let change_logs_to_delete = txn
                        .query_all(statement)
                        .await?
                        .iter()
                        .map(|row| {
                            risingwave_meta_model::hummock_table_change_log::Model::from_query_result(
                                row, "",
                            )
                        })
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    let deleted_count = change_logs_to_delete.len() as u64;
                    (change_logs_to_delete, deleted_count)
                }
                DbBackend::MySql | DbBackend::Sqlite => {
                    // MySQL does not support DELETE RETURNING, and SQLite returning support is not
                    // enabled in SeaORM, so select the rows in the same transaction before deleting.
                    let change_logs_to_delete =
                        risingwave_meta_model::hummock_table_change_log::Entity::find()
                            .filter(condition.clone())
                            .all(&txn)
                            .await?;
                    let deleted_count =
                        risingwave_meta_model::hummock_table_change_log::Entity::delete_many()
                            .filter(condition)
                            .exec(&txn)
                            .await?
                            .rows_affected;
                    (change_logs_to_delete, deleted_count)
                }
            };
            for change_log_to_delete in change_logs_to_delete {
                let deleted_change_log =
                    TableChangeLog::new([to_table_change_log(change_log_to_delete)]);
                may_delete_object_ids.extend(deleted_change_log.get_object_ids());
            }
            rows_affected += deleted_count;
        }
        txn.commit().await?;

        for (table_id, truncate_epoch) in truncate_epochs {
            if let Some(change_log) = versioning.table_change_log.get_mut(&table_id) {
                change_log.truncate(truncate_epoch);
            }
        }
        drop(versioning);
        let may_delete_object_count = may_delete_object_ids.len();
        self.gc_manager
            .add_may_delete_object_ids(may_delete_object_ids.into_iter());
        tracing::info!(
            rows_affected,
            may_delete_object_count,
            "truncated table change logs"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use risingwave_common::id::JobId;
    use risingwave_pb::hummock::StateTableInfoDelta;

    use super::*;
    use crate::controller::streaming_job::IndependentJobChangeLogInfo;

    fn version_with_committed_epochs(
        committed_epochs: impl IntoIterator<Item = (TableId, u64)>,
    ) -> HummockVersion {
        let mut version = HummockVersion::default();
        let mut delta = version.version_delta_after();
        for (table_id, committed_epoch) in committed_epochs {
            delta.state_table_info_delta.insert(
                table_id,
                StateTableInfoDelta {
                    committed_epoch,
                    compaction_group_id: 1.into(),
                },
            );
        }
        version.apply_version_delta(&delta);
        version
    }

    #[test]
    fn test_resolve_table_change_log_truncate_epochs() {
        let upstream_table_id = TableId::new(1);
        let job_state_table_id = TableId::new(2);
        let current_time_epoch = Epoch::from_physical_time(100_000);
        let subscription_epoch = Epoch::from_physical_time(70_000).0;
        let job_committed_epoch = Epoch::from_physical_time(80_000).0;
        let snapshot_epoch = Epoch::from_physical_time(85_000).0;
        let version = version_with_committed_epochs([
            (upstream_table_id, subscription_epoch),
            (job_state_table_id, job_committed_epoch),
        ]);
        let info = TableChangeLogTruncateInfo {
            subscription_retention_seconds: HashMap::from([(upstream_table_id, 10)]),
            independent_jobs: vec![IndependentJobChangeLogInfo {
                job_id: JobId::new(3),
                state_table_ids: HashSet::from([job_state_table_id]),
                upstream_table_snapshot_epochs: HashMap::from([(
                    upstream_table_id,
                    Some(snapshot_epoch),
                )]),
            }],
        };

        let truncate_epochs =
            resolve_table_change_log_truncate_epochs(&info, &version, current_time_epoch).unwrap();
        assert_eq!(truncate_epochs[&upstream_table_id], snapshot_epoch);
    }

    #[test]
    fn test_missing_snapshot_epoch_prevents_truncation() {
        let upstream_table_id = TableId::new(1);
        let job_state_table_id = TableId::new(2);
        let upstream_committed_epoch = Epoch::from_physical_time(100_000).0;
        let job_committed_epoch = Epoch::from_physical_time(80_000).0;
        let version = version_with_committed_epochs([
            (upstream_table_id, upstream_committed_epoch),
            (job_state_table_id, job_committed_epoch),
        ]);
        let info = TableChangeLogTruncateInfo {
            subscription_retention_seconds: HashMap::from([(upstream_table_id, 10)]),
            independent_jobs: vec![IndependentJobChangeLogInfo {
                job_id: JobId::new(3),
                state_table_ids: HashSet::from([job_state_table_id]),
                upstream_table_snapshot_epochs: HashMap::from([(upstream_table_id, None)]),
            }],
        };

        let truncate_epochs = resolve_table_change_log_truncate_epochs(
            &info,
            &version,
            Epoch::from_physical_time(100_000),
        )
        .unwrap();
        assert!(!truncate_epochs.contains_key(&upstream_table_id));
    }

    #[test]
    fn test_inconsistent_job_committed_epoch_fails() {
        let state_table_id_1 = TableId::new(1);
        let state_table_id_2 = TableId::new(2);
        let version = version_with_committed_epochs([
            (state_table_id_1, Epoch::from_physical_time(1).0),
            (state_table_id_2, Epoch::from_physical_time(2).0),
        ]);
        let info = TableChangeLogTruncateInfo {
            subscription_retention_seconds: HashMap::new(),
            independent_jobs: vec![IndependentJobChangeLogInfo {
                job_id: JobId::new(3),
                state_table_ids: HashSet::from([state_table_id_1, state_table_id_2]),
                upstream_table_snapshot_epochs: HashMap::from([(
                    TableId::new(4),
                    Some(Epoch::from_physical_time(1).0),
                )]),
            }],
        };

        assert!(
            resolve_table_change_log_truncate_epochs(
                &info,
                &version,
                Epoch::from_physical_time(100_000),
            )
            .is_err()
        );
    }
}
