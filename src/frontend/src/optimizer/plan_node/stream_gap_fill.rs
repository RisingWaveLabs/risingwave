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
use crate::binder::BoundFillStrategy;
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprVisitor, InputRef};
use crate::optimizer::plan_node::stream::StreamPlanNodeMetadata;
use crate::optimizer::plan_node::utils::impl_distill_by_unit;
use crate::optimizer::property::{Distribution, MonotonicityMap, WatermarkColumns};
use crate::scheduler::SchedulerResult;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamGapFill` implements [`super::Stream`] to represent a gap-filling operation on a time
/// series in normal streaming mode (without EOWC semantics).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamGapFill {
    pub base: PlanBase<super::Stream>,
    core: generic::GapFill<PlanRef<Stream>>,
}

impl StreamGapFill {
    pub fn new(core: generic::GapFill<PlanRef<Stream>>) -> Self {
        let input = &core.input;
        let partition_indices = core.partition_key_indices();
        let distinct_partition_key_count = partition_indices
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert_eq!(
            partition_indices.len(),
            distinct_partition_key_count,
            "stream gap fill expects canonicalized partition_by columns",
        );
        assert!(
            !core
                .pointer_key_indices()
                .expect("stream gap fill input should have stream key")
                .is_empty(),
            "stream gap fill pointer key should not be empty",
        );

        let dist = if core.partition_by_cols.is_empty() {
            Distribution::Single
        } else {
            Distribution::HashShard(partition_indices)
        };

        // Gap fill back-fills below the current watermark and emits non-monotonic fill values,
        // so it preserves neither watermark nor monotonicity on its output.
        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            input.stream_kind(),
            false, // does NOT provide EOWC semantics
            WatermarkColumns::new(),
            MonotonicityMap::new(),
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

    fn pointer_key_indices(&self) -> Vec<usize> {
        self.core
            .pointer_key_indices()
            .expect("stream gap fill input should have stream key")
    }

    fn infer_state_table(&self) -> crate::TableCatalog {
        let mut tbl_builder = TableCatalogBuilder::default();

        let out_schema = self.core.schema();
        for field in out_schema.fields() {
            tbl_builder.add_column(field);
        }

        let state_key_indices = self
            .core
            .stream_key_indices()
            .expect("stream gap fill input should have stream key");

        // PK: deduplicated (partition_cols..., time_col, upstream stream key...).
        for key_idx in state_key_indices {
            tbl_builder.add_order_column(key_idx, OrderType::ascending());
        }

        let partition_key_indices = self.core.partition_key_indices();
        let read_prefix_len_hint = partition_key_indices.len();
        tbl_builder.build(partition_key_indices, read_prefix_len_hint)
    }
}

impl PlanTreeNodeUnary<Stream> for StreamGapFill {
    fn input(&self) -> PlanRef<Stream> {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef<Stream>) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! { Stream, StreamGapFill }
impl_distill_by_unit!(StreamGapFill, core, "StreamGapFill");

impl TryToStreamPb for StreamGapFill {
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

        let state_table = self
            .infer_state_table()
            .with_id(state.gen_table_id_wrapped())
            .to_internal_table_prost();

        Ok(NodeBody::GapFill(Box::new(GapFillNode {
            pointer_key_indices: self
                .pointer_key_indices()
                .into_iter()
                .map(|idx| idx as u32)
                .collect(),
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
            state_table: Some(state_table),
            partition_by_indices: self
                .core
                .partition_by_cols
                .iter()
                .map(|c| c.index() as u32)
                .collect(),
        })))
    }
}

impl ExprRewritable<Stream> for StreamGapFill {
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

impl ExprVisitable for StreamGapFill {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v)
    }
}
