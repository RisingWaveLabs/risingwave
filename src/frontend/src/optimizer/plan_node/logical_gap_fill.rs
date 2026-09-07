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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_expr::bail;

use super::stream::StreamPlanNodeMetadata;
use super::{
    ColPrunable, ColumnPruningContext, ExprRewritable, ExprVisitable, Logical,
    LogicalPlanRef as PlanRef, LogicalProject, PlanBase, PlanTreeNodeUnary, PredicatePushdown,
    PredicatePushdownContext, ToBatch, ToStream, ToStreamContext, gen_filter_and_pushdown, generic,
};
use crate::binder::BoundFillStrategy;
use crate::error::Result;
use crate::expr::{ExprImpl, InputRef};
use crate::optimizer::plan_node::utils::impl_distill_by_unit;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalGapFill` implements [`super::Logical`] to represent a gap-filling operation on a time
/// series.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalGapFill {
    pub base: PlanBase<super::Logical>,
    core: generic::GapFill<PlanRef>,
}

impl LogicalGapFill {
    pub fn new(
        input: PlanRef,
        time_col: InputRef,
        interval: ExprImpl,
        fill_strategies: Vec<BoundFillStrategy>,
        partition_by_cols: Vec<InputRef>,
    ) -> Self {
        let core = generic::GapFill {
            input,
            time_col,
            interval,
            fill_strategies,
            partition_by_cols,
        };
        let base = PlanBase::new_logical_with_core(&core);
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

    pub fn partition_by_cols(&self) -> &[InputRef] {
        &self.core.partition_by_cols
    }
}

impl PlanTreeNodeUnary<Logical> for LogicalGapFill {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(
            input,
            self.time_col().clone(),
            self.interval().clone(),
            self.fill_strategies().to_vec(),
            self.partition_by_cols().to_vec(),
        )
    }
}

impl_plan_tree_node_for_unary! { Logical, LogicalGapFill }
impl_distill_by_unit!(LogicalGapFill, core, "LogicalGapFill");

impl ColPrunable for LogicalGapFill {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let input_col_num = self.input().schema().len();
        let mut input_required = FixedBitSet::with_capacity(input_col_num);
        for &required_col in required_cols {
            input_required.insert(required_col);
        }
        input_required.insert(self.time_col().index());
        input_required.union_with(&self.interval().collect_input_refs(input_col_num));
        self.fill_strategies()
            .iter()
            .for_each(|s| input_required.insert(s.target_col.index()));
        self.partition_by_cols()
            .iter()
            .for_each(|c| input_required.insert(c.index()));

        let input_required_cols: Vec<_> = input_required.ones().collect();
        let mut col_index_mapping =
            ColIndexMapping::with_remaining_columns(&input_required_cols, input_col_num);

        let mut new_core = self.core.clone();
        new_core.input = self.input().prune_col(&input_required_cols, ctx);
        new_core.rewrite_with_col_index_mapping(&mut col_index_mapping);

        let logical_gap_fill = Self {
            base: PlanBase::new_logical_with_core(&new_core),
            core: new_core,
        }
        .into();

        if input_required_cols == required_cols {
            logical_gap_fill
        } else {
            let output_required_cols = required_cols
                .iter()
                .map(|&idx| col_index_mapping.map(idx))
                .collect_vec();
            let src_size = logical_gap_fill.schema().len();
            LogicalProject::with_mapping(
                logical_gap_fill,
                ColIndexMapping::with_remaining_columns(&output_required_cols, src_size),
            )
            .into()
        }
    }
}

impl ExprRewritable<Logical> for LogicalGapFill {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn crate::expr::ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self {
            base: self.base.clone(),
            core,
        }
        .into()
    }
}

impl ExprVisitable for LogicalGapFill {
    fn visit_exprs(&self, v: &mut dyn crate::expr::ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

impl PredicatePushdown for LogicalGapFill {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // No pushdown, but keep recursing with a `true` condition so that shares below
        // still receive a contribution from this parent (see `PredicatePushdownContext`).
        gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx)
    }
}

impl ToBatch for LogicalGapFill {
    fn to_batch(&self) -> Result<super::BatchPlanRef> {
        bail!("BatchGapFill is not implemented yet")
    }
}

impl ToStream for LogicalGapFill {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<super::StreamPlanRef> {
        use super::{StreamEowcGapFill, StreamGapFill, StreamWatermarkSort};
        use crate::error::ErrorCode;
        use crate::optimizer::property::RequiredDist;

        let stream_input = self.input().to_stream(ctx)?;
        let partition_cols = &self.core.partition_by_cols;

        // Choose distribution based on partition_by_cols
        let new_input = if partition_cols.is_empty() {
            // No partition: singleton distribution (backward compatible)
            RequiredDist::single().streaming_enforce_if_not_satisfies(stream_input)?
        } else {
            // With partition: hash distribute by partition columns
            let partition_indices: Vec<usize> = partition_cols.iter().map(|c| c.index()).collect();
            RequiredDist::shard_by_key(stream_input.schema().len(), &partition_indices)
                .streaming_enforce_if_not_satisfies(stream_input)?
        };

        // No stream key validation - gap fill manages its own state internally
        // using (partition_cols, time_col, upstream_stream_key) as state table PK.

        if ctx.emit_on_window_close() {
            // EOWC mode requires watermark on time column
            let time_col_idx = self.core.time_col.index();
            let input_watermark_cols = new_input.watermark_columns();
            if !input_watermark_cols.contains(time_col_idx) {
                return Err(ErrorCode::NotSupported(
                    "GAP_FILL with EMIT ON WINDOW CLOSE requires a watermark on the time column."
                        .to_owned(),
                    format!(
                        "Please define a watermark on the time column (column index {}).",
                        time_col_idx
                    ),
                )
                .into());
            }

            let sorted_input = StreamWatermarkSort::new(new_input, time_col_idx).into();
            let core = generic::GapFill {
                input: sorted_input,
                time_col: self.core.time_col.clone(),
                interval: self.core.interval.clone(),
                fill_strategies: self.core.fill_strategies.clone(),
                partition_by_cols: self.core.partition_by_cols.clone(),
            };
            Ok(StreamEowcGapFill::new(core).into())
        } else {
            let core = generic::GapFill {
                input: new_input,
                time_col: self.core.time_col.clone(),
                interval: self.core.interval.clone(),
                fill_strategies: self.core.fill_strategies.clone(),
                partition_by_cols: self.core.partition_by_cols.clone(),
            };
            Ok(StreamGapFill::new(core).into())
        }
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut super::convert::RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, mut col_index_mapping) = self.input().logical_rewrite_for_stream(_ctx)?;
        let mut new_core = self.core.clone();
        new_core.input = input;

        if col_index_mapping.is_identity() {
            return Ok((
                Self {
                    base: PlanBase::new_logical_with_core(&new_core),
                    core: new_core,
                }
                .into(),
                col_index_mapping,
            ));
        }

        new_core.rewrite_with_col_index_mapping(&mut col_index_mapping);

        Ok((
            Self {
                base: PlanBase::new_logical_with_core(&new_core),
                core: new_core,
            }
            .into(),
            col_index_mapping,
        ))
    }
}
