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

use pretty_xmlish::XmlNode;
use risingwave_pb::stream_plan::ProjectNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::stream::prelude::*;
use super::utils::{Distill, childless_record, watermark_pretty};
use super::{
    ExprRewritable, PlanBase, PlanTreeNodeUnary, StreamPlanRef as PlanRef, TryToStreamPb, generic,
};
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::GenericPlanNode;
use crate::optimizer::property::{
    MonotonicityMap, WatermarkColumns, analyze_monotonicity, monotonicity_variants,
};
use crate::scheduler::SchedulerResult;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::ColIndexMappingRewriteExt;

/// `StreamProject` implements [`super::LogicalProject`] to evaluate specified expressions on input
/// rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamProject {
    pub base: PlanBase<Stream>,
    core: generic::Project<PlanRef>,
    /// A stream key known from semantics outside of a regular projection, for example a generated
    /// column declared as the primary key of a source. A regular [`generic::Project`] can only
    /// propagate keys through `InputRef` expressions.
    explicit_stream_key: Option<Vec<usize>>,
    /// All the watermark derivations, (`input_column_index`, `output_column_index`). And the
    /// derivation expression is the project's expression itself.
    watermark_derivations: Vec<(usize, usize)>,
    /// Nondecreasing expression indices. `Project` can produce watermarks for these expressions.
    nondecreasing_exprs: Vec<usize>,
    /// Whether there are likely no-op updates in the output chunks, so that eliminating them with
    /// `StreamChunk::eliminate_adjacent_noop_update` could be beneficial.
    noop_update_hint: bool,
}

impl Distill for StreamProject {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();

        let schema = self.schema();
        let mut vec = self.core.fields_pretty(schema);
        if let Some(display_output_watermarks) =
            watermark_pretty(self.base.watermark_columns(), schema)
        {
            vec.push(("output_watermarks", display_output_watermarks));
        }
        if verbose && self.noop_update_hint {
            vec.push(("noop_update_hint", "true".into()));
        }
        childless_record("StreamProject", vec)
    }
}

impl StreamProject {
    pub fn new(core: generic::Project<PlanRef>) -> Self {
        let noop_update_hint = core.likely_produces_noop_updates();
        Self::new_inner(core, noop_update_hint, None)
    }

    /// Creates a project with an output stream key guaranteed by semantics outside of the
    /// projection expressions.
    ///
    /// This must not be used to infer uniqueness from an arbitrary expression. It is intended for
    /// planner-generated projections whose output key is declared explicitly in a catalog.
    pub fn new_with_stream_key(core: generic::Project<PlanRef>, stream_key: Vec<usize>) -> Self {
        assert!(stream_key.iter().all(|&idx| idx < core.schema().len()));
        let noop_update_hint = core.likely_produces_noop_updates();
        Self::new_inner(core, noop_update_hint, Some(stream_key))
    }

    /// Set the `noop_update_hint` flag to the given value.
    pub fn with_noop_update_hint(self, noop_update_hint: bool) -> Self {
        Self {
            noop_update_hint,
            ..self
        }
    }

    fn new_inner(
        core: generic::Project<PlanRef>,
        noop_update_hint: bool,
        explicit_stream_key: Option<Vec<usize>>,
    ) -> Self {
        let ctx = core.ctx();
        let input = core.input.clone();
        let distribution = core
            .i2o_col_mapping()
            .rewrite_provided_distribution(input.distribution());

        let mut watermark_derivations = vec![];
        let mut nondecreasing_exprs = vec![];
        let mut out_watermark_columns = WatermarkColumns::new();
        let mut out_monotonicity_map = MonotonicityMap::new();
        for (expr_idx, expr) in core.exprs.iter().enumerate() {
            use monotonicity_variants::*;
            match analyze_monotonicity(expr) {
                Inherent(monotonicity) => {
                    out_monotonicity_map.insert(expr_idx, monotonicity);
                    if monotonicity.is_non_decreasing() && !monotonicity.is_constant() {
                        // TODO(rc): may be we should also derive watermark for constant later
                        // to produce watermarks
                        nondecreasing_exprs.push(expr_idx);
                        // each inherently non-decreasing expr creates a new watermark group
                        out_watermark_columns.insert(expr_idx, ctx.next_watermark_group_id());
                    }
                }
                FollowingInput(input_idx) => {
                    let in_monotonicity = input.columns_monotonicity()[input_idx];
                    out_monotonicity_map.insert(expr_idx, in_monotonicity);
                    if let Some(wtmk_group) = input.watermark_columns().get_group(input_idx) {
                        // to propagate watermarks
                        watermark_derivations.push((input_idx, expr_idx));
                        // join an existing watermark group
                        out_watermark_columns.insert(expr_idx, wtmk_group);
                    }
                }
                _FollowingInputInversely(_) => {}
            }
        }
        // Project executor won't change the append-only behavior of the stream, so it depends on
        // input's `append_only`.
        let base = if let Some(stream_key) = &explicit_stream_key {
            let mut functional_dependency = core.functional_dependency();
            functional_dependency.add_key(stream_key);
            PlanBase::new_stream(
                ctx,
                core.schema(),
                Some(stream_key.clone()),
                functional_dependency,
                distribution,
                input.stream_kind(),
                input.emit_on_window_close(),
                out_watermark_columns,
                out_monotonicity_map,
            )
        } else {
            PlanBase::new_stream_with_core(
                &core,
                distribution,
                input.stream_kind(),
                input.emit_on_window_close(),
                out_watermark_columns,
                out_monotonicity_map,
            )
        };

        StreamProject {
            base,
            core,
            explicit_stream_key,
            watermark_derivations,
            nondecreasing_exprs,
            noop_update_hint,
        }
    }

    pub fn core(&self) -> &generic::Project<PlanRef> {
        &self.core
    }

    pub fn exprs(&self) -> &Vec<ExprImpl> {
        &self.core.exprs
    }

    pub fn noop_update_hint(&self) -> bool {
        self.noop_update_hint
    }

    pub fn explicit_stream_key(&self) -> Option<&[usize]> {
        self.explicit_stream_key.as_deref()
    }
}

impl PlanTreeNodeUnary<Stream> for StreamProject {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new_inner(
            core,
            self.noop_update_hint,
            self.explicit_stream_key.clone(),
        )
    }
}
impl_plan_tree_node_for_unary! { Stream, StreamProject}

impl TryToStreamPb for StreamProject {
    fn try_to_stream_prost_body(
        &self,
        _state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<PbNodeBody> {
        let (watermark_input_cols, watermark_output_cols) = self
            .watermark_derivations
            .iter()
            .map(|(i, o)| (*i as u32, *o as u32))
            .unzip();
        let select_list = self
            .core
            .exprs
            .iter()
            .map(|expr| {
                expr.to_expr_proto_checked_pure(
                    self.input().stream_kind().is_retract(),
                    "SELECT list",
                )
            })
            .collect::<crate::error::Result<Vec<_>>>()?;
        Ok(PbNodeBody::Project(Box::new(ProjectNode {
            select_list,
            watermark_input_cols,
            watermark_output_cols,
            nondecreasing_exprs: self.nondecreasing_exprs.iter().map(|i| *i as _).collect(),
            noop_update_hint: self.noop_update_hint,
        })))
    }
}

impl ExprRewritable<Stream> for StreamProject {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        let noop_update_hint = self.noop_update_hint || core.likely_produces_noop_updates();
        Self::new_inner(core, noop_update_hint, self.explicit_stream_key.clone()).into()
    }
}

impl ExprVisitable for StreamProject {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
