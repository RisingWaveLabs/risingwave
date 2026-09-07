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

use std::collections::{HashMap, HashSet, VecDeque};

use anyhow::anyhow;
use futures::TryStreamExt;
use risingwave_common::catalog::TableId;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::util::epoch::Epoch;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::time_travel::{
    IncompleteHummockVersion, IncompleteHummockVersionDelta, refill_version,
};
use risingwave_hummock_sdk::version::{GroupDeltaCommon, HummockVersion, HummockVersionDelta};
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch, HummockObjectId, HummockSstableId};
use risingwave_meta_model::hummock_sstable_info::SstableInfoV2Backend;
use risingwave_meta_model::{
    HummockVersionId, hummock_epoch_to_version, hummock_sstable_info, hummock_time_travel_delta,
    hummock_time_travel_version,
};
use risingwave_pb::hummock::{PbHummockVersion, PbHummockVersionDelta};
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseTransaction, EntityTrait, PaginatorTrait,
    QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
};
use tracing::info;

use crate::hummock::HummockManager;
use crate::hummock::error::{Error, Result};

/// Time travel.
impl HummockManager {
    pub(crate) async fn init_time_travel_state(&self) -> Result<()> {
        let sql_store = self.env.meta_store_ref();
        let mut guard = self
            .versioning
            .write_with_process_name("init_time_travel_state")
            .await;
        guard.mark_next_time_travel_version_snapshot();

        guard.last_time_travel_snapshot_sst_ids = HashSet::new();
        let Some(version) = hummock_time_travel_version::Entity::find()
            .order_by_desc(hummock_time_travel_version::Column::VersionId)
            .one(&sql_store.conn)
            .await?
            .map(|v| {
                IncompleteHummockVersion::from_persisted_protobuf_owned(v.version.to_protobuf())
            })
        else {
            return Ok(());
        };
        guard.last_time_travel_snapshot_sst_ids = version.get_sst_ids();
        Ok(())
    }

    pub(crate) async fn truncate_time_travel_metadata(
        &self,
        epoch_watermark: HummockEpoch,
        pinned_snapshot_epochs: HashMap<TableId, HashSet<HummockEpoch>>,
    ) -> Result<()> {
        let _timer = self
            .metrics
            .time_travel_vacuum_metadata_latency
            .start_timer();
        let min_pinned_version_id = self
            .context_info
            .read_with_process_name("truncate_time_travel_metadata")
            .await
            .min_pinned_version_id();
        let sql_store = self.env.meta_store_ref();
        let txn = sql_store.conn.begin().await?;

        let epoch_watermark_model =
            risingwave_meta_model::Epoch::try_from(epoch_watermark).unwrap();
        let has_expired_epoch = hummock_epoch_to_version::Entity::find()
            .filter(hummock_epoch_to_version::Column::Epoch.lt(epoch_watermark_model))
            .select_only()
            .column(hummock_epoch_to_version::Column::VersionId)
            .into_tuple::<HummockVersionId>()
            .one(&txn)
            .await?
            .is_some();
        if !has_expired_epoch {
            txn.commit().await?;
            return Ok(());
        }

        let mut pinned_epoch_rows = HashSet::new();
        let mut pinned_snapshot_version_ids = HashSet::new();
        for (table_id, pinned_epochs) in pinned_snapshot_epochs {
            for pinned_epoch in pinned_epochs {
                if pinned_epoch >= epoch_watermark {
                    continue;
                }
                let pinned_epoch_model =
                    risingwave_meta_model::Epoch::try_from(pinned_epoch).unwrap();
                let epoch_to_version = hummock_epoch_to_version::Entity::find_by_id((
                    pinned_epoch_model,
                    i64::from(table_id.as_raw_id()),
                ))
                .one(&txn)
                .await?;
                let Some(epoch_to_version) = epoch_to_version else {
                    tracing::warn!(
                        %table_id,
                        pinned_epoch,
                        "pinned snapshot epoch mapping not found, skip pinning"
                    );
                    continue;
                };
                pinned_epoch_rows.insert((epoch_to_version.epoch, epoch_to_version.table_id));
                pinned_snapshot_version_ids.insert(epoch_to_version.version_id);
            }
        }

        let mut pinned_replay_version_ids = HashSet::new();
        let mut pinned_delta_ids = HashSet::new();
        let mut pinned_snapshot_sst_ids = HashSet::new();
        let mut pinned_snapshot_object_ids = HashSet::new();
        for pinned_snapshot_version_id in pinned_snapshot_version_ids {
            let Some(resolved) =
                resolve_time_travel_version(&txn, pinned_snapshot_version_id).await?
            else {
                tracing::warn!(
                    %pinned_snapshot_version_id,
                    "time travel version before pinned version not found, skip pinning"
                );
                continue;
            };
            pinned_replay_version_ids.insert(resolved.replay_version.id);
            pinned_snapshot_sst_ids.extend(resolved.replay_version.get_sst_ids());
            pinned_snapshot_object_ids.extend(resolved.replay_version.get_object_ids());
            for delta in resolved.deltas {
                pinned_delta_ids.insert(delta.id);
                pinned_snapshot_sst_ids.extend(delta.newly_added_sst_ids());
                pinned_snapshot_object_ids.extend(delta.newly_added_object_ids());
            }
        }

        // Version ids follow global commit order, while epochs are only monotonic per table.
        // Use the earliest version needed by any retained mapping as the watermark so its replay
        // metadata cannot be truncated. If no mapping is retained, only version pins and vacuum
        // throttling need to constrain the watermark.
        let version_watermark = hummock_epoch_to_version::Entity::find()
            .filter(hummock_epoch_to_version::Column::Epoch.gte(epoch_watermark_model))
            .select_only()
            .column(hummock_epoch_to_version::Column::VersionId)
            .order_by_asc(hummock_epoch_to_version::Column::VersionId)
            .into_tuple::<HummockVersionId>()
            .one(&txn)
            .await?;
        // metadata BELOW watermark_version_id will be truncated.
        let mut watermark_version_id = version_watermark.map_or(min_pinned_version_id, |id| {
            std::cmp::min(id, min_pinned_version_id)
        });
        if let Some(max_version_count) = self.env.opts.time_travel_vacuum_max_version_count {
            let mut query = hummock_time_travel_version::Entity::find()
                .select_only()
                .column(hummock_time_travel_version::Column::VersionId)
                .order_by_asc(hummock_time_travel_version::Column::VersionId)
                .limit(2);
            // A replay version can be pinned for arbitrarily long. Counting it would keep selecting
            // the same two earliest versions and prevent incremental vacuum from advancing.
            if !pinned_replay_version_ids.is_empty() {
                query = query.filter(
                    hummock_time_travel_version::Column::VersionId
                        .is_not_in(pinned_replay_version_ids.iter().copied()),
                );
            }
            let earliest2_version_ids = query.into_tuple::<HummockVersionId>().all(&txn).await?;
            // Ensure at least 1 version BELOW watermark_version_id if applying time_travel_vacuum_max_version_count.
            if earliest2_version_ids.len() == 2 {
                watermark_version_id = std::cmp::min(
                    watermark_version_id,
                    HummockVersionId::new(std::cmp::max(
                        earliest2_version_ids[0]
                            .as_raw_id()
                            .saturating_add(max_version_count.into()),
                        earliest2_version_ids[1].as_raw_id(),
                    )),
                );
            }
        }
        let mut delete_epoch_rows = hummock_epoch_to_version::Entity::delete_many()
            .filter(hummock_epoch_to_version::Column::Epoch.lt(epoch_watermark_model));
        for (epoch, table_id) in &pinned_epoch_rows {
            delete_epoch_rows = delete_epoch_rows.filter(
                Condition::any()
                    .add(hummock_epoch_to_version::Column::Epoch.ne(*epoch))
                    .add(hummock_epoch_to_version::Column::TableId.ne(*table_id)),
            );
        }
        let res = delete_epoch_rows.exec(&txn).await?;
        tracing::info!(
            epoch_watermark,
            "Delete {} rows from hummock_epoch_to_version.",
            res.rows_affected
        );
        let latest_valid_version = hummock_time_travel_version::Entity::find()
            .filter(hummock_time_travel_version::Column::VersionId.lte(watermark_version_id))
            .order_by_desc(hummock_time_travel_version::Column::VersionId)
            .one(&txn)
            .await?
            .map(|m| {
                IncompleteHummockVersion::from_persisted_protobuf_owned(m.version.to_protobuf())
            });
        let Some(latest_valid_version) = latest_valid_version else {
            txn.commit().await?;
            return Ok(());
        };
        let latest_valid_version_id = latest_valid_version.id;
        let mut retained_snapshot_sst_ids = latest_valid_version.get_sst_ids();
        let mut retained_snapshot_object_ids = latest_valid_version
            .get_object_ids()
            .collect::<HashSet<_>>();
        retained_snapshot_sst_ids.extend(pinned_snapshot_sst_ids);
        retained_snapshot_object_ids.extend(pinned_snapshot_object_ids);
        let mut object_ids_to_delete: HashSet<_> = HashSet::default();
        let mut version_delete_condition = Condition::all()
            .add(hummock_time_travel_version::Column::VersionId.lt(latest_valid_version_id));
        if !pinned_replay_version_ids.is_empty() {
            version_delete_condition = version_delete_condition.add(
                hummock_time_travel_version::Column::VersionId
                    .is_not_in(pinned_replay_version_ids.iter().copied()),
            );
        }
        let version_ids_to_delete: Vec<risingwave_meta_model::HummockVersionId> =
            hummock_time_travel_version::Entity::find()
                .select_only()
                .column(hummock_time_travel_version::Column::VersionId)
                .filter(version_delete_condition.clone())
                .order_by_desc(hummock_time_travel_version::Column::VersionId)
                .into_tuple()
                .all(&txn)
                .await?;
        let mut delta_delete_condition = Condition::all()
            .add(hummock_time_travel_delta::Column::VersionId.lt(latest_valid_version_id));
        if !pinned_delta_ids.is_empty() {
            delta_delete_condition = delta_delete_condition.add(
                hummock_time_travel_delta::Column::VersionId
                    .is_not_in(pinned_delta_ids.iter().copied()),
            );
        }
        let delta_ids_to_delete: Vec<risingwave_meta_model::HummockVersionId> =
            hummock_time_travel_delta::Entity::find()
                .select_only()
                .column(hummock_time_travel_delta::Column::VersionId)
                .filter(delta_delete_condition.clone())
                .into_tuple()
                .all(&txn)
                .await?;
        let delete_sst_batch_size = self
            .env
            .opts
            .hummock_time_travel_epoch_version_insert_batch_size;
        let delta_fetch_batch_size = self
            .env
            .opts
            .hummock_time_travel_delta_fetch_batch_size
            .max(1);
        let mut sst_ids_to_delete: HashSet<_> = HashSet::default();
        async fn delete_sst_in_batch(
            txn: &DatabaseTransaction,
            sst_ids_to_delete: HashSet<HummockSstableId>,
            delete_sst_batch_size: usize,
        ) -> Result<()> {
            for start_idx in 0..=(sst_ids_to_delete.len().saturating_sub(1) / delete_sst_batch_size)
            {
                hummock_sstable_info::Entity::delete_many()
                    .filter(
                        hummock_sstable_info::Column::SstId.is_in(
                            sst_ids_to_delete
                                .iter()
                                .skip(start_idx * delete_sst_batch_size)
                                .take(delete_sst_batch_size)
                                .copied(),
                        ),
                    )
                    .exec(txn)
                    .await?;
            }
            Ok(())
        }
        for delta_id_batch in delta_ids_to_delete.chunks(delta_fetch_batch_size) {
            let mut delta_to_delete_by_id: HashMap<_, _> =
                hummock_time_travel_delta::Entity::find()
                    .filter(
                        hummock_time_travel_delta::Column::VersionId
                            .is_in(delta_id_batch.iter().copied()),
                    )
                    .all(&txn)
                    .await?
                    .into_iter()
                    .map(|delta| (delta.version_id, delta))
                    .collect();
            for &delta_id_to_delete in delta_id_batch {
                let delta_to_delete = delta_to_delete_by_id
                    .remove(&delta_id_to_delete)
                    .ok_or_else(|| {
                        Error::TimeTravel(anyhow!(format!(
                            "version delta {} not found",
                            delta_id_to_delete
                        )))
                    })?;
                let delta_to_delete = IncompleteHummockVersionDelta::from_persisted_protobuf_owned(
                    delta_to_delete.version_delta.to_protobuf(),
                );
                let new_sst_ids = delta_to_delete.newly_added_sst_ids();
                // The SST ids added and then deleted by compaction between the 2 versions.
                sst_ids_to_delete.extend(&new_sst_ids - &retained_snapshot_sst_ids);
                if sst_ids_to_delete.len() >= delete_sst_batch_size {
                    delete_sst_in_batch(
                        &txn,
                        std::mem::take(&mut sst_ids_to_delete),
                        delete_sst_batch_size,
                    )
                    .await?;
                }
                let new_object_ids = delta_to_delete.newly_added_object_ids();
                object_ids_to_delete.extend(&new_object_ids - &retained_snapshot_object_ids);
            }
        }
        for prev_version_id in version_ids_to_delete {
            let prev_version = {
                let prev_version = hummock_time_travel_version::Entity::find_by_id(prev_version_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| {
                        Error::TimeTravel(anyhow!(format!(
                            "prev_version {} not found",
                            prev_version_id
                        )))
                    })?;
                IncompleteHummockVersion::from_persisted_protobuf_owned(
                    prev_version.version.to_protobuf(),
                )
            };
            let sst_ids = prev_version.get_sst_ids();
            sst_ids_to_delete.extend(&sst_ids - &retained_snapshot_sst_ids);
            if sst_ids_to_delete.len() >= delete_sst_batch_size {
                delete_sst_in_batch(
                    &txn,
                    std::mem::take(&mut sst_ids_to_delete),
                    delete_sst_batch_size,
                )
                .await?;
            }
            let new_object_ids: HashSet<_> = prev_version.get_object_ids().collect();
            object_ids_to_delete.extend(&new_object_ids - &retained_snapshot_object_ids);
        }
        if !sst_ids_to_delete.is_empty() {
            delete_sst_in_batch(&txn, sst_ids_to_delete, delete_sst_batch_size).await?;
        }

        if !object_ids_to_delete.is_empty() {
            // IMPORTANT: object_ids_to_delete may include objects that are still being used by SSTs not included in time travel metadata.
            // So it's crucial to filter out those objects before actually deleting them, i.e. when using `try_take_may_delete_object_ids`.
            self.gc_manager
                .add_may_delete_object_ids(object_ids_to_delete.into_iter());
        }

        let res = hummock_time_travel_version::Entity::delete_many()
            .filter(version_delete_condition)
            .exec(&txn)
            .await?;
        tracing::info!(
            %watermark_version_id,
            %latest_valid_version_id,
            "Deleted {} rows from hummock_time_travel_version.",
            res.rows_affected
        );

        let res = hummock_time_travel_delta::Entity::delete_many()
            .filter(delta_delete_condition)
            .exec(&txn)
            .await?;
        tracing::info!(
            %watermark_version_id,
            %latest_valid_version_id,
            "Deleted {} rows from hummock_time_travel_delta.",
            res.rows_affected
        );

        txn.commit().await?;
        Ok(())
    }

    pub(crate) async fn filter_out_objects_by_time_travel_v1(
        &self,
        objects: impl Iterator<Item = HummockObjectId>,
    ) -> Result<HashSet<HummockObjectId>> {
        let batch_size = self
            .env
            .opts
            .hummock_time_travel_filter_out_objects_batch_size;
        info!("filter out objects by time travel v1, only sst will remain in the result set");
        // The input object count is much smaller than time travel pinned object count in meta store.
        // So search input object in meta store.
        let mut result: HashSet<_> = objects
            .filter(|object_id| match object_id {
                HummockObjectId::Sstable(_) => true,
                HummockObjectId::VectorFile(_) | HummockObjectId::HnswGraphFile(_) => false,
            })
            .collect();
        let mut remain_sst: VecDeque<_> = result.iter().copied().collect();
        while !remain_sst.is_empty() {
            let batch = remain_sst
                .drain(..std::cmp::min(remain_sst.len(), batch_size))
                .map(|object_id| object_id.as_raw());
            let reject_object_ids: Vec<risingwave_meta_model::HummockSstableObjectId> =
                hummock_sstable_info::Entity::find()
                    .filter(hummock_sstable_info::Column::ObjectId.is_in(batch))
                    .select_only()
                    .column(hummock_sstable_info::Column::ObjectId)
                    .into_tuple()
                    .all(&self.env.meta_store_ref().conn)
                    .await?;
            for reject in reject_object_ids {
                let object_id = HummockObjectId::Sstable(reject);
                result.remove(&object_id);
            }
        }
        Ok(result)
    }

    pub(crate) async fn filter_out_objects_by_time_travel(
        &self,
        objects: impl Iterator<Item = HummockObjectId>,
    ) -> Result<HashSet<HummockObjectId>> {
        if self.env.opts.hummock_time_travel_filter_out_objects_v1 {
            return self.filter_out_objects_by_time_travel_v1(objects).await;
        }
        let mut result: HashSet<_> = objects.collect();

        // filtered out object id pinned by time travel hummock version
        {
            let mut prev_version_id: Option<HummockVersionId> = None;
            loop {
                let query = hummock_time_travel_version::Entity::find();
                let query = if let Some(prev_version_id) = prev_version_id {
                    query.filter(hummock_time_travel_version::Column::VersionId.gt(prev_version_id))
                } else {
                    query
                };
                let mut version_stream = query
                    .order_by_asc(hummock_time_travel_version::Column::VersionId)
                    .limit(
                        self.env
                            .opts
                            .hummock_time_travel_filter_out_objects_list_version_batch_size
                            as u64,
                    )
                    .stream(&self.env.meta_store_ref().conn)
                    .await?;
                let mut next_prev_version_id = None;
                while let Some(model) = version_stream.try_next().await? {
                    let version =
                        HummockVersion::from_persisted_protobuf_owned(model.version.to_protobuf());
                    for object_id in version.get_object_ids() {
                        result.remove(&object_id);
                    }
                    next_prev_version_id = Some(model.version_id);
                }
                if let Some(next_prev_version_id) = next_prev_version_id {
                    prev_version_id = Some(next_prev_version_id);
                } else {
                    break;
                }
            }
        }

        // filtered out object ids pinned by time travel hummock version delta
        {
            let mut prev_version_id: Option<HummockVersionId> = None;
            loop {
                let query = hummock_time_travel_delta::Entity::find();
                let query = if let Some(prev_version_id) = prev_version_id {
                    query.filter(hummock_time_travel_delta::Column::VersionId.gt(prev_version_id))
                } else {
                    query
                };
                let mut version_stream = query
                    .order_by_asc(hummock_time_travel_delta::Column::VersionId)
                    .limit(
                        self.env
                            .opts
                            .hummock_time_travel_filter_out_objects_list_delta_batch_size
                            as u64,
                    )
                    .stream(&self.env.meta_store_ref().conn)
                    .await?;
                let mut next_prev_version_id = None;
                while let Some(model) = version_stream.try_next().await? {
                    let version_delta = HummockVersionDelta::from_persisted_protobuf_owned(
                        model.version_delta.to_protobuf(),
                    );
                    for object_id in version_delta.newly_added_object_ids() {
                        result.remove(&object_id);
                    }
                    next_prev_version_id = Some(model.version_id);
                }
                if let Some(next_prev_version_id) = next_prev_version_id {
                    prev_version_id = Some(next_prev_version_id);
                } else {
                    break;
                }
            }
        }

        Ok(result)
    }

    pub(crate) async fn time_travel_pinned_object_count(&self) -> Result<u64> {
        let count = hummock_sstable_info::Entity::find()
            .count(&self.env.meta_store_ref().conn)
            .await?;
        Ok(count)
    }

    /// Attempt to locate the version corresponding to `query_epoch`.
    ///
    /// The version is retrieved from `hummock_epoch_to_version`, selecting the entry with the largest epoch that's lte `query_epoch`.
    ///
    /// The resulted version is complete, i.e. with correct `SstableInfo`.
    pub async fn epoch_to_version(
        &self,
        query_epoch: HummockEpoch,
        table_id: TableId,
    ) -> Result<HummockVersion> {
        let sql_store = self.env.meta_store_ref();
        let _permit = self.inflight_time_travel_query.try_acquire().map_err(|_| {
            anyhow!(format!(
                "too many inflight time travel queries, max_inflight_time_travel_query={}",
                self.env.opts.max_inflight_time_travel_query
            ))
        })?;
        let epoch_to_version = hummock_epoch_to_version::Entity::find()
            .filter(
                Condition::any()
                    .add(
                        hummock_epoch_to_version::Column::TableId
                            .eq(i64::from(table_id.as_raw_id())),
                    )
                    // for backward compatibility
                    .add(hummock_epoch_to_version::Column::TableId.eq(0)),
            )
            .filter(
                hummock_epoch_to_version::Column::Epoch
                    .lte(risingwave_meta_model::Epoch::try_from(query_epoch).unwrap()),
            )
            .order_by_desc(hummock_epoch_to_version::Column::Epoch)
            .one(&sql_store.conn)
            .await?
            .ok_or_else(|| Error::TimeTravelVersionExpired {
                table_id,
                epoch: query_epoch,
            })?;
        let timer = self
            .metrics
            .time_travel_version_replay_latency
            .start_timer();
        let actual_version_id = epoch_to_version.version_id;
        tracing::debug!(
            query_epoch,
            query_tz = ?(Epoch(query_epoch).as_timestamptz()),
            actual_epoch = epoch_to_version.epoch,
            actual_tz = ?(Epoch(u64::try_from(epoch_to_version.epoch).unwrap()).as_timestamptz()),
            %actual_version_id,
            "convert query epoch"
        );

        let Some(resolved) =
            resolve_time_travel_version(&sql_store.conn, actual_version_id).await?
        else {
            return Err(Error::TimeTravelVersionExpired {
                table_id,
                epoch: query_epoch,
            });
        };
        let mut actual_version = replay_archive(
            resolved.replay_version.to_protobuf(),
            resolved.deltas.into_iter().map(|delta| delta.to_protobuf()),
        )?;
        if actual_version.id != actual_version_id {
            return Err(Error::TimeTravelVersionExpired {
                table_id,
                epoch: query_epoch,
            });
        }

        // SstableInfo in actual_version is incomplete before refill_version.
        let mut sst_ids = actual_version
            .get_sst_ids()
            .into_iter()
            .collect::<VecDeque<_>>();
        let sst_count = sst_ids.len();
        let mut sst_id_to_info = HashMap::with_capacity(sst_count);
        let sst_info_fetch_batch_size = self.env.opts.hummock_time_travel_sst_info_fetch_batch_size;
        while !sst_ids.is_empty() {
            let sst_infos = hummock_sstable_info::Entity::find()
                .filter(hummock_sstable_info::Column::SstId.is_in(
                    sst_ids.drain(..std::cmp::min(sst_info_fetch_batch_size, sst_ids.len())),
                ))
                .all(&sql_store.conn)
                .await?;
            for sst_info in sst_infos {
                let sst_info: SstableInfo = sst_info.sstable_info.to_protobuf().into();
                sst_id_to_info.insert(sst_info.sst_id, sst_info);
            }
        }
        if sst_count != sst_id_to_info.len() {
            return Err(Error::TimeTravelVersionExpired {
                table_id,
                epoch: query_epoch,
            });
        }
        refill_version(&mut actual_version, &sst_id_to_info, table_id);
        timer.observe_duration();
        Ok(actual_version)
    }

    pub(crate) async fn write_time_travel_metadata(
        &self,
        txn: &DatabaseTransaction,
        version: Option<&HummockVersion>,
        delta: HummockVersionDelta,
        time_travel_table_ids: HashSet<StateTableId>,
        skip_sst_ids: &HashSet<HummockSstableId>,
        tables_to_commit: impl Iterator<Item = (&TableId, &CompactionGroupId, u64)>,
    ) -> Result<Option<HashSet<HummockSstableId>>> {
        let _timer = self
            .metrics
            .time_travel_write_metadata_latency
            .start_timer();
        if self
            .env
            .system_params_reader()
            .await
            .time_travel_retention_ms()
            == 0
        {
            return Ok(None);
        }
        async fn write_sstable_infos(
            mut sst_infos: impl Iterator<Item = &SstableInfo>,
            txn: &DatabaseTransaction,
            batch_size: usize,
        ) -> Result<usize> {
            let mut count = 0;
            let mut is_finished = false;
            while !is_finished {
                let mut remain = batch_size;
                let mut batch = vec![];
                while remain > 0 {
                    let Some(sst_info) = sst_infos.next() else {
                        is_finished = true;
                        break;
                    };
                    batch.push(hummock_sstable_info::ActiveModel {
                        sst_id: Set(sst_info.sst_id),
                        object_id: Set(sst_info.object_id),
                        sstable_info: Set(SstableInfoV2Backend::from(&sst_info.to_protobuf())),
                    });
                    remain -= 1;
                    count += 1;
                }
                if batch.is_empty() {
                    break;
                }
                hummock_sstable_info::Entity::insert_many(batch)
                    .on_conflict_do_nothing()
                    .exec(txn)
                    .await?;
            }
            Ok(count)
        }

        let mut batch = vec![];
        for (table_id, _cg_id, committed_epoch) in tables_to_commit {
            if !time_travel_table_ids.contains(table_id) {
                continue;
            }
            let m = hummock_epoch_to_version::ActiveModel {
                epoch: Set(committed_epoch.try_into().unwrap()),
                table_id: Set(i64::from(table_id.as_raw_id())),
                version_id: Set(delta.id),
            };
            batch.push(m);
            if batch.len()
                >= self
                    .env
                    .opts
                    .hummock_time_travel_epoch_version_insert_batch_size
            {
                // There should be no conflict rows.
                hummock_epoch_to_version::Entity::insert_many(std::mem::take(&mut batch))
                    .do_nothing()
                    .exec(txn)
                    .await?;
            }
        }
        if !batch.is_empty() {
            // There should be no conflict rows.
            hummock_epoch_to_version::Entity::insert_many(batch)
                .do_nothing()
                .exec(txn)
                .await?;
        }

        let mut version_sst_ids = None;
        if let Some(version) = version {
            // `version_sst_ids` is used to update `last_time_travel_snapshot_sst_ids`.
            version_sst_ids = Some(
                version
                    .get_sst_infos()
                    .filter_map(|s| {
                        if s.table_ids
                            .iter()
                            .any(|tid| time_travel_table_ids.contains(tid))
                        {
                            return Some(s.sst_id);
                        }
                        None
                    })
                    .collect(),
            );
            write_sstable_infos(
                version.get_sst_infos().filter(|s| {
                    !skip_sst_ids.contains(&s.sst_id)
                        && s.table_ids
                            .iter()
                            .any(|tid| time_travel_table_ids.contains(tid))
                }),
                txn,
                self.env.opts.hummock_time_travel_sst_info_insert_batch_size,
            )
            .await?;
            let m = hummock_time_travel_version::ActiveModel {
                version_id: Set(version.id),
                version: Set(
                    (&IncompleteHummockVersion::from((version, &time_travel_table_ids))
                        .to_protobuf())
                        .into(),
                ),
            };
            hummock_time_travel_version::Entity::insert(m)
                .on_conflict_do_nothing()
                .exec(txn)
                .await?;
            // Return early to skip persisting delta.
            return Ok(version_sst_ids);
        }
        let written = write_sstable_infos(
            delta.newly_added_sst_infos().filter(|s| {
                !skip_sst_ids.contains(&s.sst_id)
                    && s.table_ids
                        .iter()
                        .any(|tid| time_travel_table_ids.contains(tid))
            }),
            txn,
            self.env.opts.hummock_time_travel_sst_info_insert_batch_size,
        )
        .await?;
        let has_state_table_info_delta = delta
            .state_table_info_delta
            .keys()
            .any(|table_id| time_travel_table_ids.contains(table_id));
        if written > 0 || has_state_table_info_delta {
            let m = hummock_time_travel_delta::ActiveModel {
                version_id: Set(delta.id),
                version_delta: Set((&IncompleteHummockVersionDelta::from((
                    &delta,
                    &time_travel_table_ids,
                ))
                .to_protobuf())
                    .into()),
            };
            hummock_time_travel_delta::Entity::insert(m)
                .on_conflict_do_nothing()
                .exec(txn)
                .await?;
        }

        Ok(version_sst_ids)
    }
}

struct ResolvedTimeTravelVersion {
    replay_version: IncompleteHummockVersion,
    deltas: Vec<IncompleteHummockVersionDelta>,
}

async fn resolve_time_travel_version(
    conn: &impl ConnectionTrait,
    actual_version_id: HummockVersionId,
) -> Result<Option<ResolvedTimeTravelVersion>> {
    let Some(replay_version) = hummock_time_travel_version::Entity::find()
        .filter(hummock_time_travel_version::Column::VersionId.lte(actual_version_id))
        .order_by_desc(hummock_time_travel_version::Column::VersionId)
        .one(conn)
        .await?
    else {
        return Ok(None);
    };
    let deltas = hummock_time_travel_delta::Entity::find()
        .filter(hummock_time_travel_delta::Column::VersionId.gt(replay_version.version_id))
        .filter(hummock_time_travel_delta::Column::VersionId.lte(actual_version_id))
        .order_by_asc(hummock_time_travel_delta::Column::VersionId)
        .all(conn)
        .await?;
    Ok(Some(ResolvedTimeTravelVersion {
        replay_version: IncompleteHummockVersion::from_persisted_protobuf_owned(
            replay_version.version.to_protobuf(),
        ),
        deltas: deltas
            .into_iter()
            .map(|delta| {
                IncompleteHummockVersionDelta::from_persisted_protobuf_owned(
                    delta.version_delta.to_protobuf(),
                )
            })
            .collect(),
    }))
}

/// The `HummockVersion` is actually `InHummockVersion`. It requires `refill_version`.
fn replay_archive(
    version: PbHummockVersion,
    deltas: impl Iterator<Item = PbHummockVersionDelta>,
) -> Result<HummockVersion> {
    // The pb version ann pb version delta are actually written by InHummockVersion and InHummockVersionDelta, respectively.
    // Using HummockVersion make it easier for `refill_version` later.
    let mut last_version = HummockVersion::from_persisted_protobuf_owned(version);
    for d in deltas {
        let d = HummockVersionDelta::from_persisted_protobuf_owned(d);
        debug_assert!(
            !should_mark_next_time_travel_version_snapshot(&d),
            "unexpected time travel delta {:?}",
            d
        );
        if d.prev_id < last_version.id {
            return Err(Error::TimeTravel(anyhow!(format!(
                "invalid time travel delta chain: delta {} has prev version {}, but replay has reached {}",
                d.id, d.prev_id, last_version.id
            ))));
        }
        // Compaction deltas are not included in the time travel archive, so there may be gaps
        // between the last replayed version and this delta's previous version.
        last_version.id = d.prev_id;
        last_version.apply_version_delta(&d);
    }
    Ok(last_version)
}

pub fn require_sql_meta_store_err() -> Error {
    Error::TimeTravel(anyhow!("require SQL meta store"))
}

/// Time travel delta replay only expect `NewL0SubLevel`. In all other cases, a new version snapshot should be created.
pub fn should_mark_next_time_travel_version_snapshot(delta: &HummockVersionDelta) -> bool {
    delta.group_deltas.iter().any(|(_, deltas)| {
        deltas
            .group_deltas
            .iter()
            .any(|d| !matches!(d, GroupDeltaCommon::NewL0SubLevel(_)))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(id: u64) -> PbHummockVersion {
        let mut version = HummockVersion::default();
        version.id = id.into();
        version.to_protobuf()
    }

    fn delta(id: u64, prev_id: u64) -> PbHummockVersionDelta {
        let mut delta = HummockVersionDelta::default();
        delta.id = id.into();
        delta.prev_id = prev_id.into();
        delta.to_protobuf()
    }

    #[test]
    fn test_replay_archive_delta_chain() {
        let replayed = replay_archive(version(1), [delta(4, 3), delta(5, 4)].into_iter()).unwrap();
        assert_eq!(replayed.id, HummockVersionId::new(5));

        assert!(replay_archive(version(3), [delta(4, 2)].into_iter()).is_err());
    }
}
