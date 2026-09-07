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

use itertools::Itertools;
use pretty_xmlish::XmlNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::generic::{self, PlanAggCall};
use super::stream::prelude::*;
use super::utils::{Distill, childless_record, plan_node_name};
use super::{ExprRewritable, PlanBase, PlanTreeNodeUnary, StreamPlanRef as PlanRef, TryToStreamPb};
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, MonotonicityMap, WatermarkColumns};
use crate::scheduler::SchedulerResult;
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamSimpleAgg {
    pub base: PlanBase<Stream>,
    core: generic::Agg<PlanRef>,

    /// The index of `count(*)` in `agg_calls`.
    row_count_idx: usize,

    // Required by the downstream `RowMerge`,
    // currently only used by the `approx_percentile`'s two phase plan
    must_output_per_barrier: bool,
}

impl StreamSimpleAgg {
    pub fn new(
        core: generic::Agg<PlanRef>,
        row_count_idx: usize,
        must_output_per_barrier: bool,
    ) -> Result<Self> {
        assert_eq!(core.agg_calls[row_count_idx], PlanAggCall::count_star());
        reject_upsert_input!(core.input);

        let input = core.input.clone();
        let input_dist = input.distribution();
        let dist = match input_dist {
            Distribution::Single => Distribution::Single,
            _ => panic!(),
        };

        // Empty because watermark column(s) must be in group key and simple agg have no group key.
        let watermark_columns = WatermarkColumns::new();

        // Simple agg executor might change the append-only behavior of the stream.
        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            StreamKind::Retract,
            false,
            watermark_columns,
            MonotonicityMap::new(),
        );

        Ok(StreamSimpleAgg {
            base,
            core,
            row_count_idx,
            must_output_per_barrier,
        })
    }

    pub fn agg_calls(&self) -> &[PlanAggCall] {
        &self.core.agg_calls
    }
}

impl Distill for StreamSimpleAgg {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let name = plan_node_name!("StreamSimpleAgg",
            { "append_only", self.input().append_only() },
        );
        let mut vec = self.core.fields_pretty();
        if self.must_output_per_barrier {
            vec.push(("must_output_per_barrier", "true".into()));
        }
        childless_record(name, vec)
    }
}

impl PlanTreeNodeUnary<Stream> for StreamSimpleAgg {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let logical = generic::Agg {
            input,
            ..self.core.clone()
        };
        Self::new(logical, self.row_count_idx, self.must_output_per_barrier).unwrap()
    }
}
impl_plan_tree_node_for_unary! { Stream, StreamSimpleAgg }

impl TryToStreamPb for StreamSimpleAgg {
    fn try_to_stream_prost_body(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<PbNodeBody> {
        use risingwave_pb::stream_plan::*;
        let (intermediate_state_table, agg_states, distinct_dedup_tables) =
            self.core.infer_tables(&self.base, None, None);

        let append_only = self.input().append_only();
        let agg_calls = self
            .agg_calls()
            .iter()
            .map(|call| call.to_protobuf_checked_pure(self.input().stream_kind().is_retract()))
            .collect::<crate::error::Result<Vec<_>>>()?;

        Ok(PbNodeBody::SimpleAgg(Box::new(SimpleAggNode {
            agg_calls,
            is_append_only: append_only,
            agg_call_states: agg_states
                .into_iter()
                .map(|s| s.into_prost(state))
                .collect(),
            intermediate_state_table: Some(
                intermediate_state_table
                    .with_id(state.gen_table_id_wrapped())
                    .to_internal_table_prost(),
            ),
            distinct_dedup_tables: distinct_dedup_tables
                .into_iter()
                .sorted_by_key(|(i, _)| *i)
                .map(|(key_idx, table)| {
                    (
                        key_idx as u32,
                        table
                            .with_id(state.gen_table_id_wrapped())
                            .to_internal_table_prost(),
                    )
                })
                .collect(),
            row_count_index: self.row_count_idx as u32,
            version: PbAggNodeVersion::LATEST as _,
            must_output_per_barrier: self.must_output_per_barrier,
        })))
    }
}

impl ExprRewritable<Stream> for StreamSimpleAgg {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core, self.row_count_idx, self.must_output_per_barrier)
            .unwrap()
            .into()
    }
}

impl ExprVisitable for StreamSimpleAgg {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
