// Copyright 2023 RisingWave Labs
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

use std::collections::HashSet;

use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_expr::window_function::FrameBounds;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::generic::{GenericPlanNode, PlanWindowFunction};
use super::stream::prelude::*;
use super::utils::{TableCatalogBuilder, impl_distill_by_unit};
use super::{
    ExprRewritable, PlanBase, PlanTreeNodeUnary, StreamNode, StreamPlanRef as PlanRef, generic,
};
use crate::TableCatalog;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{MonotonicityMap, WatermarkColumns};
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamOverWindow {
    pub base: PlanBase<Stream>,
    core: generic::OverWindow<PlanRef>,
}

impl StreamOverWindow {
    pub fn new(core: generic::OverWindow<PlanRef>) -> Result<Self> {
        assert!(core.funcs_have_same_partition_and_order());
        reject_upsert_input!(core.input);

        let input = &core.input;
        let watermark_columns = WatermarkColumns::new();

        let base = PlanBase::new_stream_with_core(
            &core,
            input.distribution().clone(),
            StreamKind::Retract, // general over window cannot be append-only
            false,
            watermark_columns,
            MonotonicityMap::new(), // TODO: derive monotonicity
        );

        Ok(StreamOverWindow { base, core })
    }

    fn infer_state_table(&self) -> TableCatalog {
        let mut tbl_builder = TableCatalogBuilder::default();

        let out_schema = self.core.schema();
        for field in out_schema.fields() {
            tbl_builder.add_column(field);
        }

        let mut order_cols = HashSet::new();
        for idx in self.core.partition_key_indices() {
            if order_cols.insert(idx) {
                tbl_builder.add_order_column(idx, OrderType::ascending());
            }
        }
        let read_prefix_len_hint = tbl_builder.get_current_pk_len();
        for o in self.core.order_key() {
            if order_cols.insert(o.column_index) {
                tbl_builder.add_order_column(o.column_index, o.order_type);
            }
        }
        for &idx in self.core.input.expect_stream_key() {
            if order_cols.insert(idx) {
                tbl_builder.add_order_column(idx, OrderType::ascending());
            }
        }

        let in_dist_key = self.core.input.distribution().dist_column_indices();
        tbl_builder.build(in_dist_key.to_vec(), read_prefix_len_hint)
    }

    /// Whether the executor is allowed to clean up state rows below the watermark of the first
    /// `ORDER BY` column. See `OverWindowExecutor` for the cleaning strategy.
    ///
    /// The cleaning is only correct when:
    /// - the input is append-only, so no existing row will ever be updated or deleted;
    /// - all window frames are bounded `ROWS` frames, so a row can only affect (and be affected
    ///   by) a bounded number of neighboring rows;
    /// - the first `ORDER BY` column is a watermark column with NULLs ordered as the largest
    ///   values, so that once a watermark is received, rows below it can never get new neighbors
    ///   on the "smaller" side, and all new rows (including NULLs) land on the "larger" side.
    fn state_cleaning_enabled(&self) -> bool {
        let input = &self.core.input;
        let Some(first_order_key) = self.core.order_key().first() else {
            return false;
        };
        input.append_only()
            && self.core.window_functions().iter().all(|func| {
                matches!(&func.frame.bounds, FrameBounds::Rows(bounds)
                    if !bounds.start.is_unbounded_preceding() && !bounds.end.is_unbounded_following())
            })
            && first_order_key.order_type.nulls_are_largest()
            && input.watermark_columns().contains(first_order_key.column_index)
            // the first order key column must be part of the state table sub-PK following the
            // partition key, so that the executor can scan stale rows with it
            && !self
                .core
                .partition_key_indices()
                .contains(&first_order_key.column_index)
    }
}

impl_distill_by_unit!(StreamOverWindow, core, "StreamOverWindow");

impl PlanTreeNodeUnary<Stream> for StreamOverWindow {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core).unwrap()
    }
}
impl_plan_tree_node_for_unary! { Stream, StreamOverWindow }

impl StreamNode for StreamOverWindow {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;

        let calls = self
            .core
            .window_functions()
            .iter()
            .map(PlanWindowFunction::to_protobuf)
            .collect();
        let partition_by = self
            .core
            .partition_key_indices()
            .into_iter()
            .map(|idx| idx as _)
            .collect();
        let order_by = self
            .core
            .order_key()
            .iter()
            .copied()
            .map(ColumnOrder::to_protobuf)
            .collect();
        let state_table = self
            .infer_state_table()
            .with_id(state.gen_table_id_wrapped())
            .to_internal_table_prost();

        PbNodeBody::OverWindow(Box::new(OverWindowNode {
            calls,
            partition_by,
            order_by,
            state_table: Some(state_table),

            // Cache policy should now be read from per-job config override.
            #[expect(deprecated)]
            cache_policy: PbOverWindowCachePolicy::Unspecified as _,

            enable_state_cleaning: self.state_cleaning_enabled(),
        }))
    }
}

impl ExprRewritable<Stream> for StreamOverWindow {}

impl ExprVisitable for StreamOverWindow {}
