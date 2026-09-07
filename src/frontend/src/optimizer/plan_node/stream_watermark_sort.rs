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

use std::collections::HashSet;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::FieldDisplay;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::stream::prelude::*;
use super::utils::{Distill, TableCatalogBuilder, childless_record};
use super::{ExprRewritable, PlanBase, PlanTreeNodeUnary, StreamNode, StreamPlanRef as PlanRef};
use crate::TableCatalog;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Monotonicity, MonotonicityMap, WatermarkColumns};
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamWatermarkSort {
    pub base: PlanBase<Stream>,

    input: PlanRef,
    sort_column_index: usize,
    /// See [`Self::with_secondary_order`]. Empty for plain watermark sorting.
    secondary_order_columns: Vec<usize>,
}

impl Distill for StreamWatermarkSort {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let mut fields = vec![(
            "sort_column",
            Pretty::display(&FieldDisplay(&self.input.schema()[self.sort_column_index])),
        )];
        // EXPLAIN must show the full order the sort actually enforces, not just the leading
        // watermark column.
        if !self.secondary_order_columns.is_empty() {
            fields.push((
                "secondary_order_columns",
                Pretty::Array(
                    self.secondary_order_columns
                        .iter()
                        .map(|&i| Pretty::display(&FieldDisplay(&self.input.schema()[i])))
                        .collect(),
                ),
            ));
        }
        childless_record("StreamWatermarkSort", fields)
    }
}

impl StreamWatermarkSort {
    pub fn new(input: PlanRef, sort_column_index: usize) -> Self {
        Self::with_secondary_order(input, sort_column_index, vec![])
    }

    /// Like [`Self::new`], with additional order columns appended to the buffer table's key right
    /// after the sort column. The executor emits rows in `(sort_column, buffer-table key)` order,
    /// so this makes the emission order `(sort_column, secondary columns, ...)` — what an
    /// order-sensitive consumer with a multi-column ORDER BY (e.g. `MATCH_RECOGNIZE`) requires.
    /// No executor or proto change: the order is carried entirely by the inferred table key.
    pub fn with_secondary_order(
        input: PlanRef,
        sort_column_index: usize,
        secondary_order_columns: Vec<usize>,
    ) -> Self {
        assert!(input.watermark_columns().contains(sort_column_index));

        let schema = input.schema().clone();
        let stream_key = input.stream_key().map(|v| v.to_vec());
        let fd_set = input.functional_dependency().clone();
        let dist = input.distribution().clone();

        let mut watermark_columns = WatermarkColumns::new();
        watermark_columns.insert(
            sort_column_index,
            // `StreamSort` operator will propagate input watermark as it is,
            // so we can assign the same watermark group.
            input
                .watermark_columns()
                .get_group(sort_column_index)
                .unwrap(),
        );

        // StreamWatermarkSort makes the sorting watermark column non-decreasing
        let mut columns_monotonicity = MonotonicityMap::new();
        columns_monotonicity.insert(sort_column_index, Monotonicity::NonDecreasing);

        let base = PlanBase::new_stream(
            input.ctx(),
            schema,
            stream_key,
            fd_set,
            dist,
            StreamKind::AppendOnly,
            true,
            watermark_columns,
            columns_monotonicity,
        );
        Self {
            base,
            input,
            sort_column_index,
            secondary_order_columns,
        }
    }

    fn infer_state_table(&self) -> TableCatalog {
        // The sort state table has the same schema as the input.

        let in_fields = self.input.schema().fields();
        let mut tbl_builder = TableCatalogBuilder::default();
        for field in in_fields {
            tbl_builder.add_column(field);
        }

        let mut order_cols = HashSet::new();
        tbl_builder.add_order_column(self.sort_column_index, OrderType::ascending());
        order_cols.insert(self.sort_column_index);

        // Secondary order columns go right after the sort column and before the distribution and
        // stream keys: the executor emits in `(sort_column, table key)` order, so this is what
        // makes the emission order the caller's full ORDER BY (see `with_secondary_order`).
        for idx in &self.secondary_order_columns {
            if !order_cols.contains(idx) {
                tbl_builder.add_order_column(*idx, OrderType::ascending());
                order_cols.insert(*idx);
            }
        }

        let dist_key = self.base.distribution().dist_column_indices().to_vec();
        for idx in &dist_key {
            if !order_cols.contains(idx) {
                tbl_builder.add_order_column(*idx, OrderType::ascending());
                order_cols.insert(*idx);
            }
        }

        for idx in self.input.expect_stream_key() {
            if !order_cols.contains(idx) {
                tbl_builder.add_order_column(*idx, OrderType::ascending());
                order_cols.insert(*idx);
            }
        }

        let read_prefix_len_hint = 0;
        tbl_builder.build(dist_key, read_prefix_len_hint)
    }
}

impl PlanTreeNodeUnary<Stream> for StreamWatermarkSort {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::with_secondary_order(
            input,
            self.sort_column_index,
            self.secondary_order_columns.clone(),
        )
    }
}

impl_plan_tree_node_for_unary! { Stream, StreamWatermarkSort }

impl StreamNode for StreamWatermarkSort {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;
        PbNodeBody::Sort(Box::new(SortNode {
            state_table: Some(
                self.infer_state_table()
                    .with_id(state.gen_table_id_wrapped())
                    .to_internal_table_prost(),
            ),
            sort_column_index: self.sort_column_index as _,
        }))
    }
}

impl ExprRewritable<Stream> for StreamWatermarkSort {}

impl ExprVisitable for StreamWatermarkSort {}
