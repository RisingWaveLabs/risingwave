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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_expr::bail;
use risingwave_sqlparser::ast::{AfterMatchSkip, MatchRecognizePattern, RowsPerMatch};

use super::generic::GenericPlanRef;
use super::stream::StreamPlanNodeMetadata;
use super::{
    ColPrunable, ColumnPruningContext, ExprRewritable, ExprVisitable, Logical,
    LogicalPlanRef as PlanRef, LogicalProject, PlanBase, PlanTreeNodeUnary, PredicatePushdown,
    PredicatePushdownContext, ToBatch, ToStream, ToStreamContext, gen_filter_and_pushdown, generic,
};
use crate::binder::{BoundMeasure, BoundSymbolDefinition, MeasureSlotKind};
use crate::error::Result;
use crate::expr::ExprImpl;
use crate::optimizer::plan_node::utils::impl_distill_by_unit;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalMatchRecognize` implements [`super::Logical`] for a SQL `MATCH_RECOGNIZE` (row pattern
/// recognition) operation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalMatchRecognize {
    pub base: PlanBase<super::Logical>,
    core: generic::MatchRecognize<PlanRef>,
}

impl LogicalMatchRecognize {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: PlanRef,
        partition_by: Vec<ExprImpl>,
        order_by: Vec<ExprImpl>,
        measures: Vec<BoundMeasure>,
        rows_per_match: Option<RowsPerMatch>,
        after_match_skip: Option<AfterMatchSkip>,
        pattern: MatchRecognizePattern,
        defines: Vec<BoundSymbolDefinition>,
        within: Option<ExprImpl>,
        within_deadline: Option<ExprImpl>,
    ) -> Self {
        let core = generic::MatchRecognize {
            input,
            partition_by,
            order_by,
            measures,
            rows_per_match,
            after_match_skip,
            pattern,
            defines,
            within,
            within_deadline,
        };
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }

    /// The set of input columns referenced by any expression in the clause.
    fn input_required_cols(&self) -> FixedBitSet {
        let input_col_num = self.core.input.schema().len();
        let mut required = FixedBitSet::with_capacity(input_col_num);
        for e in &self.core.partition_by {
            required.union_with(&e.collect_input_refs(input_col_num));
        }
        for e in &self.core.order_by {
            required.union_with(&e.collect_input_refs(input_col_num));
        }
        // Measure expressions are over the synthetic per-match row; the real input columns they read
        // are recorded in the slots.
        for m in &self.core.measures {
            for slot in &m.slots {
                if !matches!(slot.kind, MeasureSlotKind::Classifier) {
                    required.insert(slot.col_idx);
                }
            }
        }
        // DEFINE predicates are over synthetic per-candidate rows; the real input columns they read
        // are recorded in the slots.
        for d in &self.core.defines {
            for slot in &d.slots {
                required.insert(slot.col_idx);
            }
        }
        required
    }
}

impl PlanTreeNodeUnary<Logical> for LogicalMatchRecognize {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(
            input,
            self.core.partition_by.clone(),
            self.core.order_by.clone(),
            self.core.measures.clone(),
            self.core.rows_per_match.clone(),
            self.core.after_match_skip.clone(),
            self.core.pattern.clone(),
            self.core.defines.clone(),
            self.core.within.clone(),
            self.core.within_deadline.clone(),
        )
    }
}

impl_plan_tree_node_for_unary! { Logical, LogicalMatchRecognize }
impl_distill_by_unit!(LogicalMatchRecognize, core, "LogicalMatchRecognize");

impl ColPrunable for LogicalMatchRecognize {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        // Prune the input down to the columns the clause's expressions actually reference.
        let input_col_num = self.core.input.schema().len();
        let input_required = self.input_required_cols();
        let input_required_cols: Vec<_> = input_required.ones().collect();

        let mut col_index_mapping =
            ColIndexMapping::with_remaining_columns(&input_required_cols, input_col_num);

        let mut new_core = self.core.clone();
        new_core.input = self.core.input.prune_col(&input_required_cols, ctx);
        new_core.rewrite_with_col_index_mapping(&mut col_index_mapping);

        let node: PlanRef = Self {
            base: PlanBase::new_logical_with_core(&new_core),
            core: new_core,
        }
        .into();

        // The node's own output is (partition cols + measures); project if the caller wants a subset.
        let output_col_num = self.schema().len();
        if required_cols == (0..output_col_num).collect_vec() {
            node
        } else {
            LogicalProject::with_mapping(
                node,
                ColIndexMapping::with_remaining_columns(required_cols, output_col_num),
            )
            .into()
        }
    }
}

impl ExprRewritable<Logical> for LogicalMatchRecognize {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn crate::expr::ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self {
            base: PlanBase::new_logical_with_core(&core),
            core,
        }
        .into()
    }
}

impl ExprVisitable for LogicalMatchRecognize {
    fn visit_exprs(&self, v: &mut dyn crate::expr::ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

impl PredicatePushdown for LogicalMatchRecognize {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // Output columns are computed (partition/measures), so do not push predicates through, but
        // keep recursing so a share below this node receives a contribution from every parent.
        gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx)
    }
}

impl ToBatch for LogicalMatchRecognize {
    fn to_batch(&self) -> Result<super::BatchPlanRef> {
        bail!("BatchMatchRecognize is not implemented yet")
    }
}

impl ToStream for LogicalMatchRecognize {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<super::StreamPlanRef> {
        use super::{StreamFilter, StreamMatchRecognize, StreamWatermarkSort};
        use crate::error::ErrorCode;
        use crate::expr::{ExprType, FunctionCall, InputRef};
        use crate::optimizer::property::RequiredDist;
        use crate::utils::Condition;
        // v1 restrictions: PARTITION BY / ORDER BY must be plain columns, PARTITION BY non-empty.
        // `NotSupported(cause, hint)` throughout, matching this feature's binder-side validation.
        if self.core.partition_key_indices().is_none() || self.core.order_key_indices().is_none() {
            return Err(ErrorCode::NotSupported(
                "MATCH_RECOGNIZE with an expression in PARTITION BY or ORDER BY".to_owned(),
                "use plain column references; compute the expression in a view below and \
                 partition/order by the resulting column"
                    .to_owned(),
            )
            .into());
        }
        if self
            .core
            .partition_key_indices()
            .expect("checked above")
            .is_empty()
        {
            return Err(ErrorCode::NotSupported(
                "MATCH_RECOGNIZE without a PARTITION BY".to_owned(),
                "add PARTITION BY; for a global pattern, partition by a constant column computed \
                 in a view below (all rows then match within one partition)"
                    .to_owned(),
            )
            .into());
        }
        let order_indices = self.core.order_key_indices().expect("checked above");
        let Some(&time_col) = order_indices.first() else {
            bail!("MATCH_RECOGNIZE requires an ORDER BY clause");
        };
        let partition_key_indices = self.core.partition_key_indices().expect("checked above");

        let stream_input = self.input().to_stream(ctx)?;
        // The executor matches over an append-only sequence and emits insert-only results; it has no
        // semantics for retracting or revising an already-emitted match, and the stream plan node
        // declares append-only output. Reject a non-append-only input during planning so the user
        // gets an error at `CREATE`, rather than the executor crashing on the first update/delete.
        if !stream_input.append_only() {
            return Err(ErrorCode::NotSupported(
                "MATCH_RECOGNIZE over a non-append-only input (updates or deletes could revise \
                 an already-emitted match)"
                    .to_owned(),
                "use an append-only source or table (e.g. CREATE TABLE ... APPEND ONLY)".to_owned(),
            )
            .into());
        }
        // Event-time contract: the executor buffers rows and finalises matches as the watermark on
        // the leading ORDER BY column advances, so that column must carry a watermark. This mirrors
        // Flink requiring a rowtime attribute on ORDER BY.
        if !stream_input.watermark_columns().contains(time_col) {
            return Err(ErrorCode::NotSupported(
                "MATCH_RECOGNIZE without a watermark on the leading ORDER BY column".to_owned(),
                "declare one on the source or table, e.g. WATERMARK FOR ts AS ts - INTERVAL '5' \
                 SECOND"
                    .to_owned(),
            )
            .into());
        }
        // A NULL leading order key has no event time: no watermark can ever release it from the
        // sort (whose cache key requires it non-null), and it cannot be ordered against other
        // rows. Filter such rows out below the sort, exactly as event-time processing drops
        // NULL-rowtime rows.
        let ts_type = stream_input.schema().fields()[time_col].data_type();
        let not_null: ExprImpl = FunctionCall::new(
            ExprType::IsNotNull,
            vec![InputRef::new(time_col, ts_type).into()],
        )?
        .into();
        let filter_core = generic::Filter {
            predicate: Condition::with_expr(not_null),
            input: stream_input,
        };
        let stream_input: super::StreamPlanRef = StreamFilter::new(filter_core).into();

        // Ordered-input planning: hash-shard by the PARTITION BY key, then a WatermarkSort over
        // the full ORDER BY (leading watermark column plus secondary order columns) so the matcher
        // receives rows already in ORDER BY order, strictly below each forwarded watermark. Sort
        // and matcher stay in the same fragment -- no exchange between them, which would destroy
        // the ordering the sort just established. The matcher itself then owns only NFA state and
        // match finalization.
        //
        // The requirement is the EXACT hash distribution over the partition columns in PARTITION
        // BY order -- not `shard_by_key`. The matcher's state table hashes its distribution key in
        // PARTITION BY order, so the rows must be physically routed by that same column order;
        // `shard_by_key` would accept any subset in any order (and its enforcing exchange hashes
        // in ascending column-index order), letting the row's routed vnode disagree with the vnode
        // its state-table key computes -- a "vnode should not be accessed" panic on the first
        // insert for `PARTITION BY (b, a)` or a pre-sharded input.
        let stream_input = RequiredDist::hash_shard(&partition_key_indices)
            .streaming_enforce_if_not_satisfies(stream_input)?;
        let secondary_order: Vec<usize> = order_indices[1..].to_vec();
        let sorted_input: super::StreamPlanRef =
            StreamWatermarkSort::with_secondary_order(stream_input, time_col, secondary_order)
                .into();
        let core = generic::MatchRecognize {
            input: sorted_input,
            partition_by: self.core.partition_by.clone(),
            order_by: self.core.order_by.clone(),
            measures: self.core.measures.clone(),
            rows_per_match: self.core.rows_per_match.clone(),
            after_match_skip: self.core.after_match_skip.clone(),
            pattern: self.core.pattern.clone(),
            defines: self.core.defines.clone(),
            within: self.core.within.clone(),
            within_deadline: self.core.within_deadline.clone(),
        };
        Ok(StreamMatchRecognize::new(core).into())
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut super::convert::RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.core.input.logical_rewrite_for_stream(ctx)?;
        let mut new_core = self.core.clone();
        new_core.input = input;
        let mut mapping = input_col_change;
        new_core.rewrite_with_col_index_mapping(&mut mapping);
        let node = Self {
            base: PlanBase::new_logical_with_core(&new_core),
            core: new_core,
        };
        // Output columns (partition + measures) are produced fresh by this node, so downstream sees
        // an identity mapping over this node's own output schema.
        let out_col_change = ColIndexMapping::identity(node.schema().len());
        Ok((node.into(), out_col_change))
    }
}
