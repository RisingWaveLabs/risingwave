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

use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::stream_node::NodeBody;

use super::generic::GenericPlanNode;
use super::utils::TableCatalogBuilder;
use super::{
    ExprRewritable, ExprVisitable, PlanBase, PlanRef, PlanTreeNodeUnary, Stream, TryToStreamPb,
    generic,
};
use crate::TableCatalog;
use crate::binder::BoundFillStrategy;
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprVisitor, InputRef};
use crate::optimizer::plan_node::stream::StreamPlanNodeMetadata;
use crate::optimizer::plan_node::utils::impl_distill_by_unit;
use crate::optimizer::property::{Distribution, MonotonicityMap, WatermarkColumns};
use crate::scheduler::SchedulerResult;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamEowcGapFill` implements [`super::Stream`] to represent a gap-filling operation on a time
/// series in streaming mode.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamEowcGapFill {
    pub base: PlanBase<super::Stream>,
    core: generic::GapFill<PlanRef<Stream>>,
}

impl StreamEowcGapFill {
    pub fn new(core: generic::GapFill<PlanRef<Stream>>) -> Self {
        let input = &core.input;

        let dist = if core.partition_by_cols.is_empty() {
            Distribution::Single
        } else {
            let partition_indices: Vec<usize> =
                core.partition_by_cols.iter().map(|c| c.index()).collect();
            Distribution::HashShard(partition_indices)
        };

        // A late anchor makes gap fill back-fill a partition below the already-forwarded time
        // watermark, so the time watermark is never preserved. Only `PARTITION BY` columns are
        // copied unchanged into generated rows, so only their watermark can be propagated.
        let mut watermark_columns = WatermarkColumns::new();
        for partition_col in &core.partition_by_cols {
            let idx = partition_col.index();
            if let Some(group) = input.watermark_columns().get_group(idx) {
                watermark_columns.insert(idx, group);
            }
        }

        // Without partitioning the output stays time-sorted, so the time column is monotonic.
        // With partitioning, cross-partition interleaving breaks that; only the copied
        // `PARTITION BY` columns keep their monotonicity.
        let mut columns_monotonicity = MonotonicityMap::new();
        if core.partition_by_cols.is_empty() {
            let idx = core.time_col.index();
            columns_monotonicity.insert(idx, input.columns_monotonicity()[idx]);
        } else {
            for partition_col in &core.partition_by_cols {
                let idx = partition_col.index();
                columns_monotonicity.insert(idx, input.columns_monotonicity()[idx]);
            }
        }

        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            input.stream_kind(),
            true, // provides EOWC semantics
            watermark_columns,
            columns_monotonicity,
        );
        Self { base, core }
    }

    pub fn time_col(&self) -> &InputRef {
        &self.core.time_col
    }

    pub fn interval(&self) -> &ExprImpl {
        &self.core.interval
    }

    pub fn fill_strategies(&self) -> &[BoundFillStrategy] {
        &self.core.fill_strategies
    }

    fn infer_prev_row_table(&self) -> TableCatalog {
        let mut tbl_builder = TableCatalogBuilder::default();

        for field in self.core.schema().fields() {
            tbl_builder.add_column(field);
        }

        if self.core.partition_by_cols.is_empty() {
            // No partition: single-row table, use first column as PK
            if !self.core.schema().fields().is_empty() {
                tbl_builder.add_order_column(0, OrderType::ascending());
            }
            tbl_builder.build(vec![], 0)
        } else {
            // With partition: PK = partition_cols (one prev_row per partition)
            for pc in &self.core.partition_by_cols {
                tbl_builder.add_order_column(pc.index(), OrderType::ascending());
            }
            let dist_key_indices: Vec<usize> = self
                .core
                .partition_by_cols
                .iter()
                .map(|c| c.index())
                .collect();
            tbl_builder.build(dist_key_indices, 0)
        }
    }
}

impl PlanTreeNodeUnary<Stream> for StreamEowcGapFill {
    fn input(&self) -> PlanRef<Stream> {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef<Stream>) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! { Stream, StreamEowcGapFill }
impl_distill_by_unit!(StreamEowcGapFill, core, "StreamEowcGapFill");

impl TryToStreamPb for StreamEowcGapFill {
    fn try_to_stream_prost_body(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<NodeBody> {
        use risingwave_pb::stream_plan::*;

        let fill_strategies: Vec<String> = self
            .fill_strategies()
            .iter()
            .map(|strategy| match strategy.strategy {
                crate::binder::FillStrategy::Locf => "locf".to_owned(),
                crate::binder::FillStrategy::Interpolate => "interpolate".to_owned(),
                crate::binder::FillStrategy::Null => "null".to_owned(),
            })
            .collect();

        let prev_row_table = self
            .infer_prev_row_table()
            .with_id(state.gen_table_id_wrapped())
            .to_internal_table_prost();

        Ok(NodeBody::EowcGapFill(Box::new(EowcGapFillNode {
            time_column_index: self.time_col().index() as u32,
            interval: Some(self.interval().to_expr_proto_checked_pure(
                self.stream_kind().is_retract(),
                "gap filling interval",
            )?),
            fill_columns: self
                .fill_strategies()
                .iter()
                .map(|strategy| strategy.target_col.index() as u32)
                .collect(),
            fill_strategies,
            prev_row_table: Some(prev_row_table),
            partition_by_indices: self
                .core
                .partition_by_cols
                .iter()
                .map(|c| c.index() as u32)
                .collect(),
        })))
    }
}

impl ExprRewritable<Stream> for StreamEowcGapFill {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef<Stream> {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self {
            base: self.base.clone_with_new_plan_id(),
            core,
        }
        .into()
    }
}

impl ExprVisitable for StreamEowcGapFill {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v)
    }
}
