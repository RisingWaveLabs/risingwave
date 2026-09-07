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

use std::collections::HashMap;

use risingwave_common::catalog::FieldDisplay;
use risingwave_pb::stream_plan::StreamScanType;

use super::*;
use crate::optimizer::ShareId;
use crate::optimizer::property::RequiredDist;

/// `ToStream` converts a logical plan node to streaming physical node
/// with an optional required distribution.
///
/// when implement this trait you can choose the two ways
/// - Implement `to_stream` and use the default implementation of `to_stream_with_dist_required`
/// - Or, if the required distribution is given, there will be a better plan. For example a hash
///   join with hash-key(a,b) and the plan is required hash-distributed by (a,b,c). you can
///   implement `to_stream_with_dist_required`, and implement `to_stream` with
///   `to_stream_with_dist_required(RequiredDist::Any)`. you can see [`LogicalProject`] as an
///   example.
pub trait ToStream {
    /// `logical_rewrite_for_stream` will rewrite the logical node, and return (`new_plan_node`,
    /// `col_mapping`), the `col_mapping` is for original columns have been changed into some other
    /// position.
    ///
    /// Now it is used to:
    /// 1. ensure every plan node's output having pk column
    /// 2. add `row_count`() in every Agg
    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(LogicalPlanRef, ColIndexMapping)>;

    /// `to_stream` is equivalent to `to_stream_with_dist_required(RequiredDist::Any)`
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<StreamPlanRef>;

    /// convert the plan to streaming physical plan and satisfy the required distribution
    fn to_stream_with_dist_required(
        &self,
        required_dist: &RequiredDist,
        ctx: &mut ToStreamContext,
    ) -> Result<StreamPlanRef> {
        let ret = self.to_stream(ctx)?;
        required_dist.streaming_enforce_if_not_satisfies(ret)
    }

    fn try_better_locality(&self, _columns: &[usize]) -> Option<LogicalPlanRef> {
        None
    }
}

/// Try to enforce the locality requirement on the given columns.
/// If a better plan can be found, return the better plan.
/// If no better plan can be found, and locality backfill is enabled, wrap the plan
/// with `LogicalLocalityProvider`.
/// Otherwise, return the plan as is.
pub fn try_enforce_locality_requirement(
    plan: LogicalPlanRef,
    columns: &[usize],
    locality_backfill_enabled: bool,
) -> LogicalPlanRef {
    assert!(!columns.is_empty());
    if let Some(better_plan) = plan.try_better_locality(columns) {
        better_plan
    } else if locality_backfill_enabled {
        LogicalLocalityProvider::new(plan, columns.to_owned()).into()
    } else {
        plan
    }
}

pub fn stream_enforce_eowc_requirement(
    ctx: OptimizerContextRef,
    plan: StreamPlanRef,
    emit_on_window_close: bool,
) -> Result<StreamPlanRef> {
    if emit_on_window_close && !plan.emit_on_window_close() {
        let watermark_groups = plan.watermark_columns().grouped();
        let n_watermark_groups = watermark_groups.len();
        if n_watermark_groups == 0 {
            Err(ErrorCode::NotSupported(
                "The query cannot be executed in Emit-On-Window-Close mode.".to_owned(),
                "Try define a watermark column in the source, or avoid aggregation without GROUP BY".to_owned(),
            )
            .into())
        } else {
            let first_watermark_group = watermark_groups.values().next().unwrap();
            let watermark_col_idx = first_watermark_group.indices().next().unwrap();
            if n_watermark_groups > 1 {
                ctx.warn_to_user(format!(
                    "There are multiple unrelated watermark columns in the query, the first one `{}` is used.",
                    FieldDisplay(&plan.schema()[watermark_col_idx])
                ));
            }
            Ok(StreamWatermarkSort::new(plan, watermark_col_idx).into())
        }
    } else {
        Ok(plan)
    }
}

#[derive(Debug, Clone)]
pub struct RewriteStreamContext {
    share_rewrite_map: HashMap<ShareId, (LogicalPlanRef, ColIndexMapping)>,
    // Snapshot backfill needs upstream table primary-key semantics during logical rewrite
    // so operators above `LogicalScan` can preserve hidden primary-key columns before
    // `StreamTableScan` is built. Other backfill types keep logical stream-key semantics.
    backfill_type: BackfillType,
    locality_backfill_enabled: bool,
}

impl RewriteStreamContext {
    pub fn new_with_backfill_type(
        backfill_type: BackfillType,
        locality_backfill_enabled: bool,
    ) -> Self {
        Self {
            share_rewrite_map: HashMap::new(),
            backfill_type,
            locality_backfill_enabled,
        }
    }

    pub fn backfill_type(&self) -> BackfillType {
        self.backfill_type
    }

    pub fn locality_backfill_enabled(&self) -> bool {
        self.locality_backfill_enabled
    }

    pub fn add_rewrite_result(
        &mut self,
        share_id: ShareId,
        plan_ref: LogicalPlanRef,
        col_change: ColIndexMapping,
    ) {
        let prev = self
            .share_rewrite_map
            .insert(share_id, (plan_ref, col_change));
        assert!(prev.is_none());
    }

    pub fn get_rewrite_result(
        &self,
        share_id: ShareId,
    ) -> Option<&(LogicalPlanRef, ColIndexMapping)> {
        self.share_rewrite_map.get(&share_id)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum BackfillType {
    Replicated,
    /// Frontend-only variant for snapshot-free sinks. It is serialized as
    /// `StreamScanType::UpstreamOnly`, but derives an upsert stream kind.
    UpstreamOnlySink,
    ArrangementBackfill,
    SnapshotBackfill,
    /// Frontend-only variant for sinks created with `since_timestamp`.
    /// It is serialized as `StreamScanType::SnapshotBackfill`, but derives
    /// the same upsert stream kind as upstream-only sinks.
    SnapshotBackfillSinceTimestamp,
}

impl BackfillType {
    pub fn without_snapshot(self) -> bool {
        matches!(
            self,
            BackfillType::UpstreamOnlySink | BackfillType::SnapshotBackfillSinceTimestamp
        )
    }

    pub fn is_snapshot_backfill(self) -> bool {
        matches!(
            self,
            BackfillType::SnapshotBackfill | BackfillType::SnapshotBackfillSinceTimestamp
        )
    }

    pub fn to_stream_scan_type(self, is_cross_db: bool) -> StreamScanType {
        if is_cross_db {
            return StreamScanType::CrossDbSnapshotBackfill;
        }

        match self {
            BackfillType::Replicated | BackfillType::UpstreamOnlySink => {
                StreamScanType::UpstreamOnly
            }
            BackfillType::ArrangementBackfill => StreamScanType::ArrangementBackfill,
            BackfillType::SnapshotBackfill | BackfillType::SnapshotBackfillSinceTimestamp => {
                StreamScanType::SnapshotBackfill
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ToStreamContext {
    share_to_stream_map: HashMap<ShareId, StreamPlanRef>,
    emit_on_window_close: bool,
    backfill_type: BackfillType,
}

impl ToStreamContext {
    pub fn new_with_backfill_type(emit_on_window_close: bool, backfill_type: BackfillType) -> Self {
        Self {
            share_to_stream_map: HashMap::new(),
            emit_on_window_close,
            backfill_type,
        }
    }

    pub fn backfill_type(&self) -> BackfillType {
        self.backfill_type
    }

    pub fn add_to_stream_result(&mut self, share_id: ShareId, plan_ref: StreamPlanRef) {
        self.share_to_stream_map
            .try_insert(share_id, plan_ref)
            .unwrap();
    }

    pub fn get_to_stream_result(&self, share_id: ShareId) -> Option<&StreamPlanRef> {
        self.share_to_stream_map.get(&share_id)
    }

    pub fn emit_on_window_close(&self) -> bool {
        self.emit_on_window_close
    }
}

/// `ToBatch` allows to convert a logical plan node to batch physical node
/// with an optional required order.
///
/// The generated plan has single distribution and doesn't have any exchange nodes inserted.
/// Use either [`ToLocalBatch`] or [`ToDistributedBatch`] after `ToBatch` to get a distributed plan.
///
/// To implement this trait you can choose one of the two ways:
/// - Implement `to_batch` and use the default implementation of `to_batch_with_order_required`
/// - Or, if a better plan can be generated when a required order is given, you can implement
///   `to_batch_with_order_required`, and implement `to_batch` with
///   `to_batch_with_order_required(&Order::any())`.
pub trait ToBatch {
    /// `to_batch` is equivalent to `to_batch_with_order_required(&Order::any())`
    fn to_batch(&self) -> Result<BatchPlanRef>;
    /// convert the plan to batch physical plan and satisfy the required Order
    fn to_batch_with_order_required(&self, required_order: &Order) -> Result<BatchPlanRef> {
        let ret = self.to_batch()?;
        required_order.enforce_if_not_satisfies(ret)
    }
}

/// Converts a batch physical plan to local plan for local execution.
///
/// This is quite similar to `ToBatch`, but different in several ways. For example it converts
/// scan to exchange + scan.
pub trait ToLocalBatch {
    fn to_local(&self) -> Result<BatchPlanRef>;

    /// Convert the plan to batch local physical plan and satisfy the required Order
    fn to_local_with_order_required(&self, required_order: &Order) -> Result<BatchPlanRef> {
        let ret = self.to_local()?;
        required_order.enforce_if_not_satisfies(ret)
    }
}

/// `ToDistributedBatch` allows to convert a batch physical plan to distributed batch plan, by
/// insert exchange node, with an optional required order and distributed.
///
/// To implement this trait you can choose one of the two ways:
/// - Implement `to_distributed` and use the default implementation of
///   `to_distributed_with_required`
/// - Or, if a better plan can be generated when a required order is given, you can implement
///   `to_distributed_with_required`, and implement `to_distributed` with
///   `to_distributed_with_required(&Order::any(), &RequiredDist::Any)`
pub trait ToDistributedBatch {
    /// `to_distributed` is equivalent to `to_distributed_with_required(&Order::any(),
    /// &RequiredDist::Any)`
    fn to_distributed(&self) -> Result<BatchPlanRef>;
    /// insert the exchange in batch physical plan to satisfy the required Distribution and Order.
    fn to_distributed_with_required(
        &self,
        required_order: &Order,
        required_dist: &RequiredDist,
    ) -> Result<BatchPlanRef> {
        let ret = self.to_distributed()?;
        let ret = required_order.enforce_if_not_satisfies(ret)?;
        required_dist.batch_enforce_if_not_satisfies(ret, required_order)
    }
}
