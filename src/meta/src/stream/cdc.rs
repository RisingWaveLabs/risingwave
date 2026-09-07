// Copyright 2025 RisingWave Labs
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
use std::iter;
use std::time::Duration;

use anyhow::Context;
use futures::pin_mut;
use futures_async_stream::for_await;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{FragmentTypeFlag, FragmentTypeMask, Schema};
use risingwave_common::id::{ActorId, JobId};
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont;
use risingwave_connector::source::cdc::external::{
    CdcTableSnapshotSplitOption, ExternalCdcTableType, ExternalTableConfig, ExternalTableReader,
    SchemaTableName,
};
use risingwave_connector::source::cdc::{CdcScanOptions, build_cdc_table_snapshot_split};
use risingwave_connector::source::{CdcTableSnapshotSplit, CdcTableSnapshotSplitRaw};
use risingwave_meta_model::cdc_table_snapshot_split::Relation::Object;
use risingwave_meta_model::{cdc_table_snapshot_split, object};
use risingwave_meta_model_migration::JoinType;
use risingwave_pb::id::{DatabaseId, TableId};
use risingwave_pb::plan_common::ExternalTableDesc;
use risingwave_pb::source::PbCdcTableSnapshotSplits;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{StreamCdcScanNode, StreamCdcScanOptions, StreamNode};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, RelationTrait, Set,
    TransactionTrait,
};

use crate::MetaResult;
use crate::controller::SqlMetaStore;
use crate::model::Fragment;

/// A CDC table snapshot splits can only be successfully initialized once.
/// Subsequent attempts to write to the metastore with the same primary key will be rejected.
pub(crate) async fn try_init_parallel_cdc_table_snapshot_splits(
    table_id: TableId,
    table_desc: &ExternalTableDesc,
    meta_store: &SqlMetaStore,
    per_table_options: &StreamCdcScanOptions,
    insert_batch_size: u64,
    sleep_split_interval: u64,
    sleep_duration_millis: u64,
) -> MetaResult<Vec<CdcTableSnapshotSplitRaw>> {
    let split_options = CdcTableSnapshotSplitOption {
        backfill_num_rows_per_split: per_table_options.backfill_num_rows_per_split,
        backfill_as_even_splits: per_table_options.backfill_as_even_splits,
        backfill_split_pk_column_index: per_table_options.backfill_split_pk_column_index,
        backfill_split_column_name: per_table_options.backfill_split_column_name.clone(),
    };
    let table_type = ExternalCdcTableType::from_properties(&table_desc.connect_properties);
    // Filter out additional columns to construct the external table schema
    let table_schema: Schema = table_desc
        .columns
        .iter()
        .filter(|col| {
            col.additional_column
                .as_ref()
                .is_none_or(|a_col| a_col.column_type.is_none())
        })
        .map(Into::into)
        .collect();
    let table_pk_indices = table_desc
        .pk
        .iter()
        .map(|k| k.column_index as usize)
        .collect_vec();
    let table_config = ExternalTableConfig::try_from_btreemap(
        table_desc.connect_properties.clone(),
        table_desc.secret_refs.clone(),
    )
    .context("failed to parse external table config")?;
    let schema_table_name = SchemaTableName::from_properties(&table_desc.connect_properties);
    let reader = table_type
        .create_table_reader(
            table_config,
            table_schema,
            table_pk_indices,
            schema_table_name,
        )
        .await?;
    let stream = reader.get_parallel_cdc_splits(split_options);
    let mut insert_batch = vec![];
    let mut splits_num = 0;
    let mut splits = vec![];
    let txn = meta_store.conn.begin().await?;
    pin_mut!(stream);
    #[for_await]
    for split in stream {
        let split: CdcTableSnapshotSplit = split?;
        splits_num += 1;
        let left = split.left_bound_inclusive.value_serialize();
        let right = split.right_bound_exclusive.value_serialize();
        insert_batch.push(cdc_table_snapshot_split::ActiveModel {
            table_id: Set(table_id.as_job_id()),
            split_id: Set(split.split_id.to_owned()),
            left: Set(left.clone()),
            right: Set(right.clone()),
            is_backfill_finished: Set(0),
        });
        splits.push(CdcTableSnapshotSplitRaw {
            split_id: split.split_id,
            left_bound_inclusive: left,
            right_bound_exclusive: right,
        });
        if insert_batch.len() >= insert_batch_size as usize {
            cdc_table_snapshot_split::Entity::insert_many(std::mem::take(&mut insert_batch))
                .exec(&txn)
                .await?;
        }
        if splits_num % sleep_split_interval == 0 {
            tokio::time::sleep(Duration::from_millis(sleep_duration_millis)).await;
        }
    }
    if !insert_batch.is_empty() {
        cdc_table_snapshot_split::Entity::insert_many(std::mem::take(&mut insert_batch))
            .exec(&txn)
            .await?;
    }
    txn.commit().await?;
    Ok(splits)
}

/// Returns some cdc scan node if the fragment is CDC scan and has parallelized backfill enabled.
pub(crate) fn is_parallelized_backfill_enabled_cdc_scan_fragment(
    fragment_type_mask: FragmentTypeMask,
    nodes: &StreamNode,
) -> Option<&StreamCdcScanNode> {
    if !fragment_type_mask.contains(FragmentTypeFlag::StreamCdcScan) {
        return None;
    }
    let mut stream_cdc_scan = None;
    visit_stream_node_cont(nodes, |node| {
        if let Some(NodeBody::StreamCdcScan(node)) = &node.node_body {
            if is_parallelized_backfill_enabled(node) {
                stream_cdc_scan = Some(&**node);
            }
            false
        } else {
            true
        }
    });
    stream_cdc_scan
}

fn is_parallelized_backfill_enabled(node: &StreamCdcScanNode) -> bool {
    if let Some(options) = &node.options
        && CdcScanOptions::from_proto(options).is_parallelized_backfill()
    {
        return true;
    }
    false
}

pub(crate) fn parallel_cdc_table_backfill_fragment<'a>(
    fragments: impl Iterator<Item = &'a Fragment>,
) -> Option<(&'a Fragment, &'a StreamCdcScanNode)> {
    let mut stream_scan_fragments = fragments.filter_map(|f| {
        is_parallelized_backfill_enabled_cdc_scan_fragment(f.fragment_type_mask, &f.nodes)
            .map(|cdc_scan| (f, cdc_scan))
    });
    let fragment = stream_scan_fragments.next()?;
    assert_eq!(
        stream_scan_fragments.count(),
        0,
        "Expect no remaining scan fragment",
    );
    Some(fragment)
}

pub(crate) fn assign_cdc_table_snapshot_splits(
    actor_ids: HashSet<ActorId>,
    splits: &[CdcTableSnapshotSplitRaw],
    generation: u64,
) -> MetaResult<HashMap<ActorId, PbCdcTableSnapshotSplits>> {
    if actor_ids.is_empty() {
        return Err(anyhow::anyhow!("Expect at least 1 actor, 0 was found.").into());
    }
    if splits.is_empty() {
        return Err(
            anyhow::anyhow!("Expect at least 1 CDC table snapshot splits, 0 was found.").into(),
        );
    }
    let splits_per_actor = splits.len().div_ceil(actor_ids.len());
    let mut assignments = HashMap::new();
    for (actor_id, splits) in actor_ids.iter().copied().zip_eq_debug(
        splits
            .iter()
            .map(build_cdc_table_snapshot_split)
            .chunks(splits_per_actor)
            .into_iter()
            .map(|c| c.collect_vec())
            .chain(iter::repeat(Vec::default()))
            .take(actor_ids.len()),
    ) {
        assignments.insert(actor_id, PbCdcTableSnapshotSplits { splits, generation });
    }
    Ok(assignments)
}

#[derive(Debug)]
pub enum CdcTableSnapshotSplits {
    Backfilling(Vec<CdcTableSnapshotSplitRaw>),
    Finished,
}

pub async fn reload_cdc_table_snapshot_splits(
    txn: &impl ConnectionTrait,
    database_id: Option<DatabaseId>,
) -> MetaResult<HashMap<JobId, CdcTableSnapshotSplits>> {
    let columns = [
        cdc_table_snapshot_split::Column::TableId,
        cdc_table_snapshot_split::Column::SplitId,
        cdc_table_snapshot_split::Column::Left,
        cdc_table_snapshot_split::Column::Right,
        cdc_table_snapshot_split::Column::IsBackfillFinished,
    ];
    #[expect(clippy::type_complexity)]
    let all_splits: Vec<(JobId, i64, Vec<u8>, Vec<u8>, i16)> =
        if let Some(database_id) = database_id {
            cdc_table_snapshot_split::Entity::find()
                .join(JoinType::LeftJoin, Object.def())
                .select_only()
                .columns(columns)
                .filter(object::Column::DatabaseId.eq(database_id))
                .into_tuple()
                .all(txn)
                .await?
        } else {
            cdc_table_snapshot_split::Entity::find()
                .select_only()
                .columns(columns)
                .into_tuple()
                .all(txn)
                .await?
        };

    let mut job_splits = HashMap::<_, Vec<_>>::new();
    for (job_id, split_id, left, right, is_backfill_finished) in all_splits {
        job_splits
            .entry(job_id)
            .or_default()
            .push((split_id, left, right, is_backfill_finished));
    }

    job_splits
        .into_iter()
        .map(|(job_id, splits)| {
            let splits = compose_job_splits(job_id, splits)?;
            Ok((job_id, splits))
        })
        .try_collect()
}

pub fn compose_job_splits(
    job_id: JobId,
    splits: Vec<(i64, Vec<u8>, Vec<u8>, i16)>,
) -> MetaResult<CdcTableSnapshotSplits> {
    let split_completed_count = splits
        .iter()
        .filter(|(_, _, _, is_backfill_finished)| *is_backfill_finished == 1)
        .count();
    assert!(
        split_completed_count <= 1,
        "split_completed_count = {}",
        split_completed_count
    );
    let is_backfill_finished = split_completed_count == 1;
    let splits = if is_backfill_finished {
        if splits.len() != 1 {
            // CdcTableBackfillTracker::complete_job rewrites splits in a transaction.
            // This error should only happen when the meta store reads uncommitted data.
            tracing::error!(%job_id, ?splits, "unexpected split count");
            bail!(
                "unexpected split count: job_id={job_id}, split_total_count={}, split_completed_count={split_completed_count}",
                splits.len()
            );
        }
        CdcTableSnapshotSplits::Finished
    } else {
        let splits: Vec<_> = splits
            .into_iter()
            // The try_init_parallel_cdc_table_snapshot_splits ensures that split with a larger split_id will always have a larger left bound.
            // Assigning consecutive splits to the same actor enables potential optimization in CDC backfill executor.
            .sorted_by_key(|(split_id, _, _, _)| *split_id)
            .map(
                |(split_id, left_bound_inclusive, right_bound_exclusive, _)| {
                    CdcTableSnapshotSplitRaw {
                        split_id,
                        left_bound_inclusive,
                        right_bound_exclusive,
                    }
                },
            )
            .collect();
        CdcTableSnapshotSplits::Backfilling(splits)
    };
    Ok(splits)
}

pub fn single_merged_split() -> CdcTableSnapshotSplitRaw {
    CdcTableSnapshotSplitRaw {
        // TODO(zw): remove magic number
        split_id: 1,
        left_bound_inclusive: OwnedRow::new(vec![None]).value_serialize(),
        right_bound_exclusive: OwnedRow::new(vec![None]).value_serialize(),
    }
}
