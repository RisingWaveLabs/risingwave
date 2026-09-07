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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_common::{bail, bail_not_implemented, not_implemented};
use risingwave_expr::aggregate::{AggType, PbAggKind, agg_types};

use super::generic::{self, Agg, GenericPlanRef, PlanAggCall, ProjectBuilder};
use super::utils::impl_distill_by_unit;
use super::{
    BatchHashAgg, BatchSimpleAgg, ColPrunable, ExprRewritable, Logical, LogicalPlanRef as PlanRef,
    PlanBase, PlanTreeNodeUnary, PredicatePushdown, StreamHashAgg, StreamPlanRef, StreamProject,
    StreamShare, StreamSimpleAgg, StreamStatelessSimpleAgg, ToBatch, ToStream,
    try_enforce_locality_requirement,
};
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{
    AggCall, Expr, ExprImpl, ExprRewriter, ExprType, ExprVisitor, FunctionCall, InputRef, Literal,
    OrderBy, OrderByExpr, WindowFunction,
};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::GenericPlanNode;
use crate::optimizer::plan_node::stream_global_approx_percentile::StreamGlobalApproxPercentile;
use crate::optimizer::plan_node::stream_local_approx_percentile::StreamLocalApproxPercentile;
use crate::optimizer::plan_node::stream_row_merge::StreamRowMerge;
use crate::optimizer::plan_node::{
    BatchSortAgg, ColumnPruningContext, LogicalDedup, LogicalProject, PredicatePushdownContext,
    RewriteStreamContext, ToStreamContext, gen_filter_and_pushdown,
};
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::utils::{
    ColIndexMapping, ColIndexMappingRewriteExt, Condition, GroupBy, IndexSet, Substitute,
};

pub struct AggInfo {
    pub calls: Vec<PlanAggCall>,
    pub col_mapping: ColIndexMapping,
}

/// `SeparatedAggInfo` is used to separate normal and approx percentile aggs.
pub struct SeparatedAggInfo {
    normal: AggInfo,
    approx: AggInfo,
}

/// `LogicalAgg` groups input data by their group key and computes aggregation functions.
///
/// It corresponds to the `GROUP BY` operator in a SQL query statement together with the aggregate
/// functions in the `SELECT` clause.
///
/// The output schema will first include the group key and then the aggregation calls.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalAgg {
    pub base: PlanBase<Logical>,
    core: Agg<PlanRef>,
}

impl LogicalAgg {
    /// Generate plan for stateless 2-phase streaming agg.
    /// Should only be used iff input is distributed. Input must be converted to stream form.
    fn gen_stateless_two_phase_streaming_agg_plan(
        &self,
        stream_input: StreamPlanRef,
    ) -> Result<StreamPlanRef> {
        debug_assert!(self.group_key().is_empty());

        // ====== Handle approx percentile aggs
        let (
            non_approx_percentile_col_mapping,
            approx_percentile_col_mapping,
            approx_percentile,
            core,
        ) = self.prepare_approx_percentile(stream_input)?;

        if core.agg_calls.is_empty() {
            if let Some(approx_percentile) = approx_percentile {
                return Ok(approx_percentile);
            };
            bail!("expected at least one agg call");
        }

        let need_row_merge: bool = Self::need_row_merge(&approx_percentile);

        // ====== Handle normal aggs
        let total_agg_calls = core
            .agg_calls
            .iter()
            .enumerate()
            .map(|(partial_output_idx, agg_call)| {
                agg_call.partial_to_total_agg_call(partial_output_idx)
            })
            .collect_vec();
        let local_agg = StreamStatelessSimpleAgg::new(core)?;
        let exchange =
            RequiredDist::single().streaming_enforce_if_not_satisfies(local_agg.into())?;

        let must_output_per_barrier = need_row_merge;
        let global_agg = new_stream_simple_agg(
            Agg::new(total_agg_calls, IndexSet::empty(), exchange),
            must_output_per_barrier,
        )?;

        // ====== Merge approx percentile and normal aggs
        Self::add_row_merge_if_needed(
            approx_percentile,
            global_agg.into(),
            approx_percentile_col_mapping,
            non_approx_percentile_col_mapping,
        )
    }

    /// Generate plan for stateless/stateful 2-phase streaming agg.
    /// Should only be used iff input is distributed.
    /// Input must be converted to stream form.
    fn gen_vnode_two_phase_streaming_agg_plan(
        &self,
        stream_input: StreamPlanRef,
        dist_key: &[usize],
    ) -> Result<StreamPlanRef> {
        let (
            non_approx_percentile_col_mapping,
            approx_percentile_col_mapping,
            approx_percentile,
            core,
        ) = self.prepare_approx_percentile(stream_input.clone())?;

        if core.agg_calls.is_empty() {
            if let Some(approx_percentile) = approx_percentile {
                return Ok(approx_percentile);
            };
            bail!("expected at least one agg call");
        }
        let need_row_merge = Self::need_row_merge(&approx_percentile);

        // Generate vnode via project
        // TODO(kwannoel): We should apply Project optimization rules here.
        let input_col_num = stream_input.schema().len(); // get schema len before moving `stream_input`.
        let project = StreamProject::new(generic::Project::with_vnode_col(stream_input, dist_key));
        let vnode_col_idx = project.base.schema().len() - 1;

        // Generate local agg step
        let mut local_group_key = self.group_key().clone();
        local_group_key.insert(vnode_col_idx);
        let n_local_group_key = local_group_key.len();
        let local_agg = new_stream_hash_agg(
            Agg::new(core.agg_calls.clone(), local_group_key, project.into()),
            Some(vnode_col_idx),
        )?;
        // Global group key excludes vnode.
        let local_agg_group_key_cardinality = local_agg.group_key().len();
        let local_group_key_without_vnode =
            &local_agg.group_key().to_vec()[..local_agg_group_key_cardinality - 1];
        let global_group_key = local_agg
            .i2o_col_mapping()
            .rewrite_dist_key(local_group_key_without_vnode)
            .expect("some input group key could not be mapped");

        // Generate global agg step
        let global_agg = if self.group_key().is_empty() {
            let exchange =
                RequiredDist::single().streaming_enforce_if_not_satisfies(local_agg.into())?;
            let must_output_per_barrier = need_row_merge;
            let global_agg = new_stream_simple_agg(
                Agg::new(
                    core.agg_calls
                        .iter()
                        .enumerate()
                        .map(|(partial_output_idx, agg_call)| {
                            agg_call
                                .partial_to_total_agg_call(n_local_group_key + partial_output_idx)
                        })
                        .collect(),
                    global_group_key.into_iter().collect(),
                    exchange,
                ),
                must_output_per_barrier,
            )?;
            global_agg.into()
        } else {
            // the `RowMergeExec` has not supported keyed merge
            assert!(!need_row_merge);
            let exchange = RequiredDist::shard_by_key(input_col_num, &global_group_key)
                .streaming_enforce_if_not_satisfies(local_agg.into())?;
            // Local phase should have reordered the group keys into their required order.
            // we can just follow it.
            let global_agg = new_stream_hash_agg(
                Agg::new(
                    core.agg_calls
                        .iter()
                        .enumerate()
                        .map(|(partial_output_idx, agg_call)| {
                            agg_call
                                .partial_to_total_agg_call(n_local_group_key + partial_output_idx)
                        })
                        .collect(),
                    global_group_key.into_iter().collect(),
                    exchange,
                ),
                None,
            )?;
            global_agg.into()
        };
        Self::add_row_merge_if_needed(
            approx_percentile,
            global_agg,
            approx_percentile_col_mapping,
            non_approx_percentile_col_mapping,
        )
    }

    fn gen_single_plan(&self, stream_input: StreamPlanRef) -> Result<StreamPlanRef> {
        let input = RequiredDist::single().streaming_enforce_if_not_satisfies(stream_input)?;
        let core = self.core.clone_with_input(input);
        Ok(new_stream_simple_agg(core, false)?.into())
    }

    fn gen_shuffle_plan(&self, stream_input: StreamPlanRef) -> Result<StreamPlanRef> {
        let input =
            RequiredDist::shard_by_key(stream_input.schema().len(), &self.group_key().to_vec())
                .streaming_enforce_if_not_satisfies(stream_input)?;
        let core = self.core.clone_with_input(input);
        Ok(new_stream_hash_agg(core, None)?.into())
    }

    /// Generates distributed stream plan.
    fn gen_dist_stream_agg_plan(&self, stream_input: StreamPlanRef) -> Result<StreamPlanRef> {
        use super::stream::prelude::*;

        let input_dist = stream_input.distribution();
        debug_assert!(*input_dist != Distribution::Broadcast);

        // Shuffle agg
        // If we have group key, and we won't try two phase agg optimization at all,
        // we will always choose shuffle agg over single agg.
        if !self.group_key().is_empty() && !self.core.must_try_two_phase_agg() {
            return self.gen_shuffle_plan(stream_input);
        }

        // Standalone agg
        // If no group key, and cannot two phase agg, we have to use single plan.
        if self.group_key().is_empty() && !self.core.can_two_phase_agg() {
            return self.gen_single_plan(stream_input);
        }

        debug_assert!(if !self.group_key().is_empty() {
            self.core.must_try_two_phase_agg()
        } else {
            self.core.can_two_phase_agg()
        });

        // Stateless 2-phase simple agg
        // can be applied on stateless simple agg calls,
        // with input distributed by [`Distribution::AnyShard`]
        if self.group_key().is_empty()
            && self
                .core
                .all_local_aggs_are_stateless(stream_input.append_only())
            && input_dist.satisfies(&RequiredDist::AnyShard)
        {
            return self.gen_stateless_two_phase_streaming_agg_plan(stream_input);
        }

        // If input is [`Distribution::SomeShard`] and we must try to use two phase agg,
        // The only remaining strategy is Vnode-based 2-phase agg.
        // We shall first distribute it by PK,
        // so it obeys consistent hash strategy via [`Distribution::HashShard`].
        let stream_input =
            if *input_dist == Distribution::SomeShard && self.core.must_try_two_phase_agg() {
                RequiredDist::shard_by_key(
                    stream_input.schema().len(),
                    stream_input.expect_stream_key(),
                )
                .streaming_enforce_if_not_satisfies(stream_input)?
            } else {
                stream_input
            };
        let input_dist = stream_input.distribution();

        // Vnode-based 2-phase agg
        // can be applied on agg calls not affected by order,
        // with input distributed by dist_key.
        match input_dist {
            Distribution::HashShard(dist_key) | Distribution::UpstreamHashShard(dist_key, _)
                if (self.group_key().is_empty()
                    || !self.core.hash_agg_dist_satisfied_by_input_dist(input_dist)) =>
            {
                let dist_key = dist_key.clone();
                return self.gen_vnode_two_phase_streaming_agg_plan(stream_input, &dist_key);
            }
            _ => {}
        }

        // Fallback to shuffle or single, if we can't generate any 2-phase plans.
        if !self.group_key().is_empty() {
            self.gen_shuffle_plan(stream_input)
        } else {
            self.gen_single_plan(stream_input)
        }
    }

    /// Prepares metadata and the `approx_percentile` plan, if there's one present.
    /// It may modify `core.agg_calls` to separate normal agg and approx percentile agg,
    /// and `core.input` to share the input via `StreamShare`,
    /// to both approx percentile agg and normal agg.
    fn prepare_approx_percentile(
        &self,
        stream_input: StreamPlanRef,
    ) -> Result<(
        ColIndexMapping,
        ColIndexMapping,
        Option<StreamPlanRef>,
        Agg<StreamPlanRef>,
    )> {
        let SeparatedAggInfo { normal, approx } = self.separate_normal_and_special_agg();

        let AggInfo {
            calls: non_approx_percentile_agg_calls,
            col_mapping: non_approx_percentile_col_mapping,
        } = normal;
        let AggInfo {
            calls: approx_percentile_agg_calls,
            col_mapping: approx_percentile_col_mapping,
        } = approx;
        if !self.group_key().is_empty() && !approx_percentile_agg_calls.is_empty() {
            bail_not_implemented!(
                "two-phase streaming approx percentile aggregation with group key, \
             please use single phase aggregation instead"
            );
        }

        // Either we have approx percentile aggs and non_approx percentile aggs,
        // or we have at least 2 approx percentile aggs.
        let needs_row_merge = (!non_approx_percentile_agg_calls.is_empty()
            && !approx_percentile_agg_calls.is_empty())
            || approx_percentile_agg_calls.len() >= 2;
        let input = if needs_row_merge {
            // If there's row merge, we need to share the input.
            StreamShare::new_from_input(stream_input).into()
        } else {
            stream_input
        };
        let mut core = self.core.clone_with_input(input);
        core.agg_calls = non_approx_percentile_agg_calls;

        let approx_percentile =
            self.build_approx_percentile_aggs(core.input.clone(), &approx_percentile_agg_calls)?;
        Ok((
            non_approx_percentile_col_mapping,
            approx_percentile_col_mapping,
            approx_percentile,
            core,
        ))
    }

    fn need_row_merge(approx_percentile: &Option<StreamPlanRef>) -> bool {
        approx_percentile.is_some()
    }

    /// Add `RowMerge` if needed
    fn add_row_merge_if_needed(
        approx_percentile: Option<StreamPlanRef>,
        global_agg: StreamPlanRef,
        approx_percentile_col_mapping: ColIndexMapping,
        non_approx_percentile_col_mapping: ColIndexMapping,
    ) -> Result<StreamPlanRef> {
        // just for assert
        let need_row_merge = Self::need_row_merge(&approx_percentile);

        if let Some(approx_percentile) = approx_percentile {
            assert!(need_row_merge);
            let row_merge = StreamRowMerge::new(
                approx_percentile,
                global_agg,
                approx_percentile_col_mapping,
                non_approx_percentile_col_mapping,
            )?;
            Ok(row_merge.into())
        } else {
            assert!(!need_row_merge);
            Ok(global_agg)
        }
    }

    fn separate_normal_and_special_agg(&self) -> SeparatedAggInfo {
        let estimated_len = self.agg_calls().len() - 1;
        let mut approx_percentile_agg_calls = Vec::with_capacity(estimated_len);
        let mut non_approx_percentile_agg_calls = Vec::with_capacity(estimated_len);
        let mut approx_percentile_col_mapping = Vec::with_capacity(estimated_len);
        let mut non_approx_percentile_col_mapping = Vec::with_capacity(estimated_len);
        for (output_idx, agg_call) in self.agg_calls().iter().enumerate() {
            if agg_call.agg_type == AggType::Builtin(PbAggKind::ApproxPercentile) {
                approx_percentile_agg_calls.push(agg_call.clone());
                approx_percentile_col_mapping.push(Some(output_idx));
            } else {
                non_approx_percentile_agg_calls.push(agg_call.clone());
                non_approx_percentile_col_mapping.push(Some(output_idx));
            }
        }
        let normal = AggInfo {
            calls: non_approx_percentile_agg_calls,
            col_mapping: ColIndexMapping::new(
                non_approx_percentile_col_mapping,
                self.agg_calls().len(),
            ),
        };
        let approx = AggInfo {
            calls: approx_percentile_agg_calls,
            col_mapping: ColIndexMapping::new(
                approx_percentile_col_mapping,
                self.agg_calls().len(),
            ),
        };
        SeparatedAggInfo { normal, approx }
    }

    fn build_approx_percentile_agg(
        &self,
        input: StreamPlanRef,
        approx_percentile_agg_call: &PlanAggCall,
    ) -> Result<StreamPlanRef> {
        let local_approx_percentile =
            StreamLocalApproxPercentile::new(input, approx_percentile_agg_call)?;
        let exchange = RequiredDist::single()
            .streaming_enforce_if_not_satisfies(local_approx_percentile.into())?;
        let global_approx_percentile =
            StreamGlobalApproxPercentile::new(exchange, approx_percentile_agg_call);
        Ok(global_approx_percentile.into())
    }

    /// If only 1 approx percentile, just return it.
    /// Otherwise build a tree of approx percentile with `MergeProject`.
    /// e.g.
    /// ApproxPercentile(col1, 0.5) as x,
    /// ApproxPercentile(col2, 0.5) as y,
    /// ApproxPercentile(col3, 0.5) as z
    /// will be built as
    ///        `MergeProject`
    ///       /          \
    ///  `MergeProject`       z
    ///  /        \
    /// x          y
    fn build_approx_percentile_aggs(
        &self,
        input: StreamPlanRef,
        approx_percentile_agg_call: &[PlanAggCall],
    ) -> Result<Option<StreamPlanRef>> {
        if approx_percentile_agg_call.is_empty() {
            return Ok(None);
        }
        let approx_percentile_plans: Vec<_> = approx_percentile_agg_call
            .iter()
            .map(|agg_call| self.build_approx_percentile_agg(input.clone(), agg_call))
            .try_collect()?;
        assert!(!approx_percentile_plans.is_empty());
        let mut iter = approx_percentile_plans.into_iter();
        let mut acc = iter.next().unwrap();
        for (current_size, plan) in iter.enumerate().map(|(i, p)| (i + 1, p)) {
            let new_size = current_size + 1;
            let row_merge = StreamRowMerge::new(
                acc,
                plan,
                ColIndexMapping::identity_or_none(current_size, new_size),
                ColIndexMapping::new(vec![Some(current_size)], new_size),
            )?;
            acc = row_merge.into();
        }
        Ok(Some(acc))
    }

    pub fn core(&self) -> &Agg<PlanRef> {
        &self.core
    }
}

/// `LogicalAggBuilder` extracts agg calls and references to group columns from select list and
/// build the plan like `LogicalAgg - LogicalProject`.
/// it is constructed by `group_exprs` and collect and rewrite the expression in selection and
/// having clause.
pub struct LogicalAggBuilder {
    /// the builder of the input Project
    input_proj_builder: ProjectBuilder,
    /// the group key column indices in the project's output
    group_key: IndexSet,
    /// the grouping sets
    grouping_sets: Vec<IndexSet>,
    /// the agg calls
    agg_calls: Vec<PlanAggCall>,
    /// the error during the expression rewriting
    error: Option<RwError>,
    /// If `is_in_filter_clause` is true, it means that
    /// we are processing filter clause.
    /// This field is needed because input refs in these clauses
    /// are allowed to refer to any columns, while those not in filter
    /// clause are only allowed to refer to group keys.
    is_in_filter_clause: bool,
}

impl LogicalAggBuilder {
    fn new(group_by: GroupBy, input_schema_len: usize) -> Result<Self> {
        let mut input_proj_builder = ProjectBuilder::default();

        let mut gen_group_key_and_grouping_sets =
            |grouping_sets: Vec<Vec<ExprImpl>>| -> Result<(IndexSet, Vec<IndexSet>)> {
                let grouping_sets: Vec<IndexSet> = grouping_sets
                    .into_iter()
                    .map(|set| {
                        set.into_iter()
                            .map(|expr| input_proj_builder.add_expr(&expr))
                            .try_collect()
                            .map_err(|err| not_implemented!("{err} inside GROUP BY"))
                    })
                    .try_collect()?;

                // Construct group key based on grouping sets.
                let group_key = grouping_sets
                    .iter()
                    .fold(FixedBitSet::with_capacity(input_schema_len), |acc, x| {
                        acc.union(&x.to_bitset()).collect()
                    });

                Ok((IndexSet::from_iter(group_key.ones()), grouping_sets))
            };

        let (group_key, grouping_sets) = match group_by {
            GroupBy::GroupKey(group_key) => {
                let group_key = group_key
                    .into_iter()
                    .map(|expr| input_proj_builder.add_expr(&expr))
                    .try_collect()
                    .map_err(|err| not_implemented!("{err} inside GROUP BY"))?;
                (group_key, vec![])
            }
            GroupBy::GroupingSets(grouping_sets) => gen_group_key_and_grouping_sets(grouping_sets)?,
            GroupBy::Rollup(rollup) => {
                // Convert rollup to grouping sets.
                let grouping_sets = (0..=rollup.len())
                    .map(|n| {
                        rollup
                            .iter()
                            .take(n)
                            .flat_map(|x| x.iter().cloned())
                            .collect_vec()
                    })
                    .collect_vec();
                gen_group_key_and_grouping_sets(grouping_sets)?
            }
            GroupBy::Cube(cube) => {
                // Convert cube to grouping sets.
                let grouping_sets = cube
                    .into_iter()
                    .powerset()
                    .map(|x| x.into_iter().flatten().collect_vec())
                    .collect_vec();
                gen_group_key_and_grouping_sets(grouping_sets)?
            }
        };

        Ok(LogicalAggBuilder {
            group_key,
            grouping_sets,
            agg_calls: vec![],
            error: None,
            input_proj_builder,
            is_in_filter_clause: false,
        })
    }

    pub fn build(self, input: PlanRef) -> LogicalAgg {
        // This LogicalProject focuses on the exprs in aggregates and GROUP BY clause.
        let logical_project = LogicalProject::with_core(self.input_proj_builder.build(input));

        // This LogicalAgg focuses on calculating the aggregates and grouping.
        Agg::new(self.agg_calls, self.group_key, logical_project.into())
            .with_grouping_sets(self.grouping_sets)
            .into()
    }

    fn rewrite_with_error(&mut self, expr: ExprImpl) -> Result<ExprImpl> {
        let rewritten_expr = self.rewrite_expr(expr);
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        Ok(rewritten_expr)
    }

    /// check if the expression is a group by key, and try to return the group key
    pub fn try_as_group_expr(&self, expr: &ExprImpl) -> Option<usize> {
        if let Some(input_index) = self.input_proj_builder.expr_index(expr)
            && let Some(index) = self
                .group_key
                .indices()
                .position(|group_key| group_key == input_index)
        {
            return Some(index);
        }
        None
    }

    fn schema_agg_start_offset(&self) -> usize {
        self.group_key.len()
    }

    /// Rewrite [`AggCall`] if needed, and push it into the builder using `push_agg_call`.
    /// This is shared by [`LogicalAggBuilder`] and `LogicalOverWindowBuilder`.
    pub(crate) fn general_rewrite_agg_call(
        agg_call: AggCall,
        mut push_agg_call: impl FnMut(AggCall) -> Result<InputRef>,
    ) -> Result<ExprImpl> {
        match agg_call.agg_type {
            // Rewrite avg to cast(sum as avg_return_type) / count.
            AggType::Builtin(PbAggKind::Avg) => {
                assert_eq!(agg_call.args.len(), 1);
                let return_type = agg_call.return_type();

                let sum = ExprImpl::from(push_agg_call(AggCall::new(
                    PbAggKind::Sum.into(),
                    agg_call.args.clone(),
                    agg_call.distinct,
                    agg_call.order_by.clone(),
                    agg_call.filter.clone(),
                    agg_call.direct_args.clone(),
                )?)?)
                .cast_explicit(&agg_call.return_type())?;

                let count = ExprImpl::from(push_agg_call(AggCall::new(
                    PbAggKind::Count.into(),
                    agg_call.args.clone(),
                    agg_call.distinct,
                    agg_call.order_by.clone(),
                    agg_call.filter.clone(),
                    agg_call.direct_args,
                )?)?);

                let target = ExprImpl::from(FunctionCall::new(
                    ExprType::Divide,
                    Vec::from([sum, count.clone()]),
                )?);
                let null = ExprImpl::from(Literal::new(None, return_type));
                let zero = ExprImpl::literal_int(0);
                let case_cond =
                    ExprImpl::from(FunctionCall::new(ExprType::Equal, vec![count, zero])?);
                Ok(ExprImpl::from(FunctionCall::new(
                    ExprType::Case,
                    vec![case_cond, null, target],
                )?))
            }
            // We compute `var_samp` as
            // (sum(sq) - sum * sum / count) / (count - 1)
            // and `var_pop` as
            // (sum(sq) - sum * sum / count) / count
            // Since we don't have the square function, we use the plain Multiply for squaring,
            // which is in a sense more general than the pow function, especially when calculating
            // covariances in the future. Also we don't have the sqrt function for rooting, so we
            // use pow(x, 0.5) to simulate
            AggType::Builtin(
                kind @ (PbAggKind::StddevPop
                | PbAggKind::StddevSamp
                | PbAggKind::VarPop
                | PbAggKind::VarSamp),
            ) => {
                let arg = Itertools::exactly_one(agg_call.args().iter()).unwrap();
                let squared_arg = ExprImpl::from(FunctionCall::new(
                    ExprType::Multiply,
                    vec![arg.clone(), arg.clone()],
                )?);

                let sum_of_sq = ExprImpl::from(push_agg_call(AggCall::new(
                    PbAggKind::Sum.into(),
                    vec![squared_arg],
                    agg_call.distinct,
                    agg_call.order_by.clone(),
                    agg_call.filter.clone(),
                    agg_call.direct_args.clone(),
                )?)?)
                .cast_explicit(&agg_call.return_type())?;

                let sum = ExprImpl::from(push_agg_call(AggCall::new(
                    PbAggKind::Sum.into(),
                    agg_call.args.clone(),
                    agg_call.distinct,
                    agg_call.order_by.clone(),
                    agg_call.filter.clone(),
                    agg_call.direct_args.clone(),
                )?)?)
                .cast_explicit(&agg_call.return_type())?;

                let count = ExprImpl::from(push_agg_call(AggCall::new(
                    PbAggKind::Count.into(),
                    agg_call.args.clone(),
                    agg_call.distinct,
                    agg_call.order_by.clone(),
                    agg_call.filter.clone(),
                    agg_call.direct_args.clone(),
                )?)?);

                let zero = ExprImpl::literal_int(0);
                let one = ExprImpl::literal_int(1);

                let squared_sum = ExprImpl::from(FunctionCall::new(
                    ExprType::Multiply,
                    vec![sum.clone(), sum],
                )?);

                let raw_numerator = ExprImpl::from(FunctionCall::new(
                    ExprType::Subtract,
                    vec![
                        sum_of_sq,
                        ExprImpl::from(FunctionCall::new(
                            ExprType::Divide,
                            vec![squared_sum, count.clone()],
                        )?),
                    ],
                )?);

                // We need to check for potential accuracy issues that may occasionally lead to results less than 0.
                let numerator_type = raw_numerator.return_type();
                let numerator = ExprImpl::from(FunctionCall::new(
                    ExprType::Greatest,
                    vec![raw_numerator, zero.clone().cast_explicit(&numerator_type)?],
                )?);

                let denominator = match kind {
                    PbAggKind::VarPop | PbAggKind::StddevPop => count.clone(),
                    PbAggKind::VarSamp | PbAggKind::StddevSamp => ExprImpl::from(
                        FunctionCall::new(ExprType::Subtract, vec![count.clone(), one.clone()])?,
                    ),
                    _ => unreachable!(),
                };

                let mut target = ExprImpl::from(FunctionCall::new(
                    ExprType::Divide,
                    vec![numerator, denominator],
                )?);

                if matches!(kind, PbAggKind::StddevPop | PbAggKind::StddevSamp) {
                    target = ExprImpl::from(FunctionCall::new(ExprType::Sqrt, vec![target])?);
                }

                let null = ExprImpl::from(Literal::new(None, agg_call.return_type()));
                let case_cond = match kind {
                    PbAggKind::VarPop | PbAggKind::StddevPop => {
                        ExprImpl::from(FunctionCall::new(ExprType::Equal, vec![count, zero])?)
                    }
                    PbAggKind::VarSamp | PbAggKind::StddevSamp => ExprImpl::from(
                        FunctionCall::new(ExprType::LessThanOrEqual, vec![count, one])?,
                    ),
                    _ => unreachable!(),
                };

                Ok(ExprImpl::from(FunctionCall::new(
                    ExprType::Case,
                    vec![case_cond, null, target],
                )?))
            }
            AggType::Builtin(PbAggKind::ApproxPercentile) => {
                if agg_call.order_by.sort_exprs[0].order_type == OrderType::descending() {
                    // Rewrite DESC into 1.0-percentile for approx_percentile.
                    let prev_percentile = agg_call.direct_args[0].clone();
                    let new_percentile = 1.0
                        - prev_percentile
                            .get_data()
                            .as_ref()
                            .unwrap()
                            .as_float64()
                            .into_inner();
                    let new_percentile = Some(ScalarImpl::Float64(new_percentile.into()));
                    let new_percentile = Literal::new(new_percentile, DataType::Float64);
                    let new_direct_args = vec![new_percentile, agg_call.direct_args[1].clone()];

                    let new_agg_call = AggCall {
                        order_by: OrderBy::any(),
                        direct_args: new_direct_args,
                        ..agg_call
                    };
                    Ok(push_agg_call(new_agg_call)?.into())
                } else {
                    let new_agg_call = AggCall {
                        order_by: OrderBy::any(),
                        ..agg_call
                    };
                    Ok(push_agg_call(new_agg_call)?.into())
                }
            }
            AggType::Builtin(PbAggKind::ArgMin | PbAggKind::ArgMax) => {
                let mut agg_call = agg_call;

                let comparison_arg_type = agg_call.args[1].return_type();
                match comparison_arg_type {
                    DataType::Struct(_)
                    | DataType::List(_)
                    | DataType::Map(_)
                    | DataType::Vector(_)
                    | DataType::Jsonb => {
                        bail!(format!(
                            "{} does not support struct, array, map, vector, jsonb for comparison argument, got {}",
                            agg_call.agg_type.to_string(),
                            comparison_arg_type
                        ));
                    }
                    _ => {}
                }

                let not_null_exprs: Vec<ExprImpl> = agg_call
                    .args
                    .iter()
                    .map(|arg| -> Result<ExprImpl> {
                        Ok(FunctionCall::new(ExprType::IsNotNull, vec![arg.clone()])?.into())
                    })
                    .try_collect()?;

                let comparison_expr = agg_call.args[1].clone();
                let mut order_exprs = vec![OrderByExpr {
                    expr: comparison_expr,
                    order_type: if agg_call.agg_type == AggType::Builtin(PbAggKind::ArgMin) {
                        OrderType::ascending()
                    } else {
                        OrderType::descending()
                    },
                }];

                order_exprs.extend(agg_call.order_by.sort_exprs);

                let order_by = OrderBy::new(order_exprs);

                let filter = agg_call.filter.clone().and(Condition {
                    conjunctions: not_null_exprs,
                });

                agg_call.args.truncate(1);

                let new_agg_call = AggCall {
                    agg_type: AggType::Builtin(PbAggKind::FirstValue),
                    order_by,
                    filter,
                    ..agg_call
                };
                Ok(push_agg_call(new_agg_call)?.into())
            }
            _ => Ok(push_agg_call(agg_call)?.into()),
        }
    }

    /// Push a new agg call into the builder.
    /// Return an `InputRef` to that agg call.
    /// For existing agg calls, return an `InputRef` to the existing one.
    fn push_agg_call(&mut self, agg_call: AggCall) -> Result<InputRef> {
        let AggCall {
            agg_type,
            return_type,
            args,
            distinct,
            order_by,
            filter,
            direct_args,
        } = agg_call;

        self.is_in_filter_clause = true;
        // filter expr is not added to `input_proj_builder` as a whole. Special exprs incl
        // subquery/agg/table are rejected in `bind_agg`.
        let filter = filter.rewrite_expr(self);
        self.is_in_filter_clause = false;

        let args: Vec<_> = args
            .iter()
            .map(|expr| {
                let index = self.input_proj_builder.add_expr(expr)?;
                Ok(InputRef::new(index, expr.return_type()))
            })
            .try_collect()
            .map_err(|err: &'static str| not_implemented!("{err} inside aggregation calls"))?;

        let order_by: Vec<_> = order_by
            .sort_exprs
            .iter()
            .map(|e| {
                let index = self.input_proj_builder.add_expr(&e.expr)?;
                Ok(ColumnOrder::new(index, e.order_type))
            })
            .try_collect()
            .map_err(|err: &'static str| {
                not_implemented!("{err} inside aggregation calls order by")
            })?;

        let plan_agg_call = PlanAggCall {
            agg_type,
            return_type: return_type.clone(),
            inputs: args,
            distinct,
            order_by,
            filter,
            direct_args,
        };

        if let Some((pos, existing)) = self
            .agg_calls
            .iter()
            .find_position(|&c| c == &plan_agg_call)
        {
            return Ok(InputRef::new(
                self.schema_agg_start_offset() + pos,
                existing.return_type.clone(),
            ));
        }
        let index = self.schema_agg_start_offset() + self.agg_calls.len();
        self.agg_calls.push(plan_agg_call);
        Ok(InputRef::new(index, return_type))
    }

    /// When there is an agg call, there are 3 things to do:
    /// 1. Rewrite `avg`, `var_samp`, etc. into a combination of `sum`, `count`, etc.;
    /// 2. Add exprs in arguments to input `Project`;
    /// 2. Add the agg call to current `Agg`, and return an `InputRef` to it.
    ///
    /// Note that the rewriter does not traverse into inputs of agg calls.
    fn try_rewrite_agg_call(&mut self, mut agg_call: AggCall) -> Result<ExprImpl> {
        if matches!(agg_call.agg_type, agg_types::must_have_order_by!())
            && agg_call.order_by.sort_exprs.is_empty()
        {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "Aggregation function {} requires ORDER BY clause",
                agg_call.agg_type
            ))
            .into());
        }

        // try ignore ORDER BY if it doesn't affect the result
        if matches!(
            agg_call.agg_type,
            agg_types::result_unaffected_by_order_by!()
        ) {
            agg_call.order_by = OrderBy::any();
        }
        // try ignore DISTINCT if it doesn't affect the result
        if matches!(
            agg_call.agg_type,
            agg_types::result_unaffected_by_distinct!()
        ) {
            agg_call.distinct = false;
        }

        if matches!(agg_call.agg_type, AggType::Builtin(PbAggKind::Grouping)) {
            if self.grouping_sets.is_empty() {
                return Err(ErrorCode::NotSupported(
                    "GROUPING must be used in a query with grouping sets".into(),
                    "try to use grouping sets instead".into(),
                )
                .into());
            }
            if agg_call.args.len() >= 32 {
                return Err(ErrorCode::InvalidInputSyntax(
                    "GROUPING must have fewer than 32 arguments".into(),
                )
                .into());
            }
            if agg_call
                .args
                .iter()
                .any(|x| self.try_as_group_expr(x).is_none())
            {
                return Err(ErrorCode::InvalidInputSyntax(
                    "arguments to GROUPING must be grouping expressions of the associated query level"
                        .into(),
                ).into());
            }
        }

        Self::general_rewrite_agg_call(agg_call, |agg_call| self.push_agg_call(agg_call))
    }
}

impl ExprRewriter for LogicalAggBuilder {
    fn rewrite_agg_call(&mut self, agg_call: AggCall) -> ExprImpl {
        let dummy = Literal::new(None, agg_call.return_type()).into();
        match self.try_rewrite_agg_call(agg_call) {
            Ok(expr) => expr,
            Err(err) => {
                self.error = Some(err);
                dummy
            }
        }
    }

    /// When there is an `FunctionCall` (outside of agg call), it must refers to a group column.
    /// Or all `InputRef`s appears in it must refer to a group column.
    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
        let expr = func_call.into();
        if let Some(group_key) = self.try_as_group_expr(&expr) {
            InputRef::new(group_key, expr.return_type()).into()
        } else {
            let (func_type, inputs, ret) = expr.into_function_call().unwrap().decompose();
            let inputs = inputs
                .into_iter()
                .map(|expr| self.rewrite_expr(expr))
                .collect();
            FunctionCall::new_unchecked(func_type, inputs, ret).into()
        }
    }

    /// When there is an `WindowFunction` (outside of agg call), it must refers to a group column.
    /// Or all `InputRef`s appears in it must refer to a group column.
    fn rewrite_window_function(&mut self, window_func: WindowFunction) -> ExprImpl {
        let WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } = window_func;
        let args = args
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        let partition_by = partition_by
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        let order_by = order_by.rewrite_expr(self);
        WindowFunction {
            args,
            partition_by,
            order_by,
            ..window_func
        }
        .into()
    }

    /// When there is an `InputRef` (outside of agg call), it must refers to a group column.
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        let expr = input_ref.into();
        if let Some(group_key) = self.try_as_group_expr(&expr) {
            InputRef::new(group_key, expr.return_type()).into()
        } else if self.is_in_filter_clause {
            InputRef::new(
                self.input_proj_builder.add_expr(&expr).unwrap(),
                expr.return_type(),
            )
            .into()
        } else {
            self.error = Some(
                ErrorCode::InvalidInputSyntax(
                    "column must appear in the GROUP BY clause or be used in an aggregate function"
                        .into(),
                )
                .into(),
            );
            expr
        }
    }

    fn rewrite_subquery(&mut self, subquery: crate::expr::Subquery) -> ExprImpl {
        if subquery.is_correlated_by_depth(0) {
            self.error = Some(
                not_implemented!(
                    issue = 2275,
                    "correlated subquery in HAVING or SELECT with agg",
                )
                .into(),
            );
        }
        subquery.into()
    }
}

impl From<Agg<PlanRef>> for LogicalAgg {
    fn from(core: Agg<PlanRef>) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }
}

/// Because `From`/`Into` are not transitive
impl From<Agg<PlanRef>> for PlanRef {
    fn from(core: Agg<PlanRef>) -> Self {
        LogicalAgg::from(core).into()
    }
}

impl LogicalAgg {
    /// get the Mapping of columnIndex from input column index to out column index
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        self.core.i2o_col_mapping()
    }

    /// `create` will analyze select exprs, group exprs and having, and construct a plan like
    ///
    /// ```text
    /// LogicalAgg -> LogicalProject -> input
    /// ```
    ///
    /// It also returns the rewritten select exprs and having that reference into the aggregated
    /// results.
    pub fn create(
        select_exprs: Vec<ExprImpl>,
        group_by: GroupBy,
        having: Option<ExprImpl>,
        input: PlanRef,
    ) -> Result<(PlanRef, Vec<ExprImpl>, Option<ExprImpl>)> {
        let mut agg_builder = LogicalAggBuilder::new(group_by, input.schema().len())?;

        let rewritten_select_exprs = select_exprs
            .into_iter()
            .map(|expr| agg_builder.rewrite_with_error(expr))
            .collect::<Result<_>>()?;
        let rewritten_having = having
            .map(|expr| agg_builder.rewrite_with_error(expr))
            .transpose()?;

        Ok((
            agg_builder.build(input).into(),
            rewritten_select_exprs,
            rewritten_having,
        ))
    }

    /// Get a reference to the logical agg's agg calls.
    pub fn agg_calls(&self) -> &Vec<PlanAggCall> {
        &self.core.agg_calls
    }

    /// Get a reference to the logical agg's group key.
    pub fn group_key(&self) -> &IndexSet {
        &self.core.group_key
    }

    pub fn grouping_sets(&self) -> &Vec<IndexSet> {
        &self.core.grouping_sets
    }

    pub fn decompose(self) -> (Vec<PlanAggCall>, IndexSet, Vec<IndexSet>, PlanRef, bool) {
        self.core.decompose()
    }

    #[must_use]
    pub fn rewrite_with_input_agg(
        &self,
        input: PlanRef,
        agg_calls: &[PlanAggCall],
        mut input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let agg_calls = agg_calls
            .iter()
            .cloned()
            .map(|mut agg_call| {
                agg_call.inputs.iter_mut().for_each(|i| {
                    *i = InputRef::new(input_col_change.map(i.index()), i.return_type())
                });
                agg_call.order_by.iter_mut().for_each(|o| {
                    o.column_index = input_col_change.map(o.column_index);
                });
                agg_call.filter = agg_call.filter.rewrite_expr(&mut input_col_change);
                agg_call
            })
            .collect();
        // This is the group key order should be after rewriting.
        let group_key_in_vec: Vec<usize> = self
            .group_key()
            .indices()
            .map(|key| input_col_change.map(key))
            .collect();
        // This is the group key order we get after rewriting.
        let group_key: IndexSet = group_key_in_vec.iter().cloned().collect();
        let grouping_sets = self
            .grouping_sets()
            .iter()
            .map(|set| set.indices().map(|key| input_col_change.map(key)).collect())
            .collect();

        let new_agg = Agg::new(agg_calls, group_key.clone(), input)
            .with_grouping_sets(grouping_sets)
            .with_enable_two_phase(self.core().enable_two_phase);

        // group_key remapping might cause an output column change, since group key actually is a
        // `FixedBitSet`.
        let mut out_col_change = vec![];
        for idx in group_key_in_vec {
            let pos = group_key.indices().position(|x| x == idx).unwrap();
            out_col_change.push(pos);
        }
        for i in (group_key.len())..new_agg.schema().len() {
            out_col_change.push(i);
        }
        let out_col_change =
            ColIndexMapping::with_remaining_columns(&out_col_change, new_agg.schema().len());

        (new_agg.into(), out_col_change)
    }
}

impl PlanTreeNodeUnary<Logical> for LogicalAgg {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Agg::new(self.agg_calls().clone(), self.group_key().clone(), input)
            .with_grouping_sets(self.grouping_sets().clone())
            .with_enable_two_phase(self.core().enable_two_phase)
            .into()
    }

    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        self.rewrite_with_input_agg(input, self.agg_calls(), input_col_change)
    }
}

impl_plan_tree_node_for_unary! { Logical, LogicalAgg }
impl_distill_by_unit!(LogicalAgg, core, "LogicalAgg");

impl ExprRewritable<Logical> for LogicalAgg {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self {
            base: self.base.clone_with_new_plan_id(),
            core,
        }
        .into()
    }
}

impl ExprVisitable for LogicalAgg {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

impl ColPrunable for LogicalAgg {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let group_key_required_cols = self.group_key().to_bitset();

        let (agg_call_required_cols, agg_calls) = {
            let input_cnt = self.input().schema().len();
            let mut tmp = FixedBitSet::with_capacity(input_cnt);
            let group_key_cardinality = self.group_key().len();
            let new_agg_calls = required_cols
                .iter()
                .filter(|&&index| index >= group_key_cardinality)
                .map(|&index| {
                    let index = index - group_key_cardinality;
                    let agg_call = self.agg_calls()[index].clone();
                    tmp.extend(agg_call.inputs.iter().map(|x| x.index()));
                    tmp.extend(agg_call.order_by.iter().map(|x| x.column_index));
                    // collect columns used in aggregate filter expressions
                    for i in &agg_call.filter.conjunctions {
                        tmp.union_with(&i.collect_input_refs(input_cnt));
                    }
                    agg_call
                })
                .collect_vec();
            (tmp, new_agg_calls)
        };

        let input_required_cols = {
            let mut tmp = FixedBitSet::with_capacity(self.input().schema().len());
            tmp.union_with(&group_key_required_cols);
            tmp.union_with(&agg_call_required_cols);
            tmp.ones().collect_vec()
        };
        let input_col_change = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );
        let agg = {
            let input = self.input().prune_col(&input_required_cols, ctx);
            let (agg, output_col_change) =
                self.rewrite_with_input_agg(input, &agg_calls, input_col_change);
            assert!(output_col_change.is_identity());
            agg
        };
        let new_output_cols = {
            // group key were never pruned or even re-ordered in current impl
            let group_key_cardinality = agg.group_key().len();
            let mut tmp = (0..group_key_cardinality).collect_vec();
            tmp.extend(
                required_cols
                    .iter()
                    .filter(|&&index| index >= group_key_cardinality),
            );
            tmp
        };
        if new_output_cols == required_cols {
            // current schema perfectly fit the required columns
            agg.into()
        } else {
            // some columns are not needed, or the order need to be adjusted.
            // so we did a projection to remove/reorder the columns.
            let mapping =
                &ColIndexMapping::with_remaining_columns(&new_output_cols, self.schema().len());
            let output_required_cols = required_cols
                .iter()
                .map(|&idx| mapping.map(idx))
                .collect_vec();
            let src_size = agg.schema().len();
            LogicalProject::with_mapping(
                agg.into(),
                ColIndexMapping::with_remaining_columns(&output_required_cols, src_size),
            )
            .into()
        }
    }
}

impl PredicatePushdown for LogicalAgg {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        let num_group_key = self.group_key().len();
        let num_agg_calls = self.agg_calls().len();
        assert!(num_group_key + num_agg_calls == self.schema().len());

        // SimpleAgg should be skipped because the predicate either references agg_calls
        // or is const.
        // If the filter references agg_calls, we can not push it.
        // When it is constantly true, pushing is useless and may actually cause more evaluation
        // cost of the predicate.
        // When it is constantly false, pushing is wrong - the old plan returns 0 rows but new one
        // returns 1 row.
        if num_group_key == 0 {
            return gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx);
        }

        // If the filter references agg_calls, we can not push it. An impure predicate must also
        // remain above the aggregation because pushing it down changes its evaluation cardinality
        // from once per group to once per input row.
        let mut agg_call_columns = FixedBitSet::with_capacity(num_group_key + num_agg_calls);
        agg_call_columns.insert_range(num_group_key..num_group_key + num_agg_calls);
        let [agg_call_pred, pushed_predicate] = predicate.group_by::<_, 2>(|expr| {
            (expr.is_pure()
                && expr
                    .collect_input_refs(self.schema().len())
                    .is_disjoint(&agg_call_columns)) as usize
        });

        // convert the predicate to one that references the child of the agg
        let mut subst = Substitute {
            mapping: self
                .group_key()
                .indices()
                .enumerate()
                .map(|(i, group_key)| {
                    InputRef::new(group_key, self.schema().fields()[i].data_type()).into()
                })
                .collect(),
        };
        let pushed_predicate = pushed_predicate.rewrite_expr(&mut subst);

        gen_filter_and_pushdown(self, agg_call_pred, pushed_predicate, ctx)
    }
}

impl ToBatch for LogicalAgg {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        self.to_batch_with_order_required(&Order::any())
    }

    // TODO(rc): `to_batch_with_order_required` seems to be useless after we decide to use
    // `BatchSortAgg` only when input is already sorted
    fn to_batch_with_order_required(
        &self,
        required_order: &Order,
    ) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        let input = self.input().to_batch()?;
        let new_logical = self.core.clone_with_input(input);
        let agg_plan = if self.group_key().is_empty() {
            BatchSimpleAgg::new(new_logical).into()
        } else if self.ctx().session_ctx().config().batch_enable_sort_agg()
            && new_logical.input_provides_order_on_group_keys()
        {
            BatchSortAgg::new(new_logical).into()
        } else {
            BatchHashAgg::new(new_logical).into()
        };
        required_order.enforce_if_not_satisfies(agg_plan)
    }
}

fn find_or_append_row_count(mut logical: Agg<StreamPlanRef>) -> (Agg<StreamPlanRef>, usize) {
    // `HashAgg`/`SimpleAgg` executors require a `count(*)` to correctly build changes, so
    // append a `count(*)` if not exists.
    let count_star = PlanAggCall::count_star();
    let row_count_idx = if let Some((idx, _)) = logical
        .agg_calls
        .iter()
        .find_position(|&c| c == &count_star)
    {
        idx
    } else {
        let idx = logical.agg_calls.len();
        logical.agg_calls.push(count_star);
        idx
    };
    (logical, row_count_idx)
}

fn new_stream_simple_agg(
    core: Agg<StreamPlanRef>,
    must_output_per_barrier: bool,
) -> Result<StreamSimpleAgg> {
    let (logical, row_count_idx) = find_or_append_row_count(core);
    StreamSimpleAgg::new(logical, row_count_idx, must_output_per_barrier)
}

fn new_stream_hash_agg(
    core: Agg<StreamPlanRef>,
    vnode_col_idx: Option<usize>,
) -> Result<StreamHashAgg> {
    let (logical, row_count_idx) = find_or_append_row_count(core);
    StreamHashAgg::new(logical, vnode_col_idx, row_count_idx)
}

impl ToStream for LogicalAgg {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<StreamPlanRef> {
        use super::stream::prelude::*;

        let eowc = ctx.emit_on_window_close();
        let input = self.input();

        let stream_input = input.to_stream(ctx)?;

        // Use Dedup operator, if possible.
        if stream_input.append_only() && self.agg_calls().is_empty() && !self.group_key().is_empty()
        {
            let group_key = self.group_key().to_vec();
            let input_schema_len = input.schema().len();
            let dedup: PlanRef = LogicalDedup::new(input, group_key.clone()).into();
            let project = LogicalProject::with_mapping(
                dedup,
                ColIndexMapping::with_remaining_columns(&group_key, input_schema_len),
            );
            return project.to_stream(ctx);
        }

        if self.agg_calls().iter().any(|call| {
            matches!(
                call.agg_type,
                AggType::Builtin(PbAggKind::ApproxCountDistinct)
            )
        }) {
            if stream_input.append_only() {
                self.core.ctx().session_ctx().notice_to_user(
                    "Streaming `APPROX_COUNT_DISTINCT` is still a preview feature and subject to change. Please do not use it in production environment.",
                );
            } else {
                bail_not_implemented!(
                    "Streaming `APPROX_COUNT_DISTINCT` is only supported in append-only stream"
                );
            }
        }

        let plan = self.gen_dist_stream_agg_plan(stream_input)?;

        let (plan, n_final_agg_calls) = if let Some(final_agg) = plan.as_stream_simple_agg() {
            if eowc {
                return Err(ErrorCode::InvalidInputSyntax(
                    "`EMIT ON WINDOW CLOSE` cannot be used for aggregation without `GROUP BY`"
                        .to_owned(),
                )
                .into());
            }
            (plan.clone(), final_agg.agg_calls().len())
        } else if let Some(final_agg) = plan.as_stream_hash_agg() {
            (
                if eowc {
                    final_agg.to_eowc_version()?
                } else {
                    plan.clone()
                },
                final_agg.agg_calls().len(),
            )
        } else if let Some(_approx_percentile_agg) = plan.as_stream_global_approx_percentile() {
            if eowc {
                return Err(ErrorCode::InvalidInputSyntax(
                    "`EMIT ON WINDOW CLOSE` cannot be used for aggregation without `GROUP BY`"
                        .to_owned(),
                )
                .into());
            }
            (plan.clone(), 1)
        } else if let Some(stream_row_merge) = plan.as_stream_row_merge() {
            if eowc {
                return Err(ErrorCode::InvalidInputSyntax(
                    "`EMIT ON WINDOW CLOSE` cannot be used for aggregation without `GROUP BY`"
                        .to_owned(),
                )
                .into());
            }
            (plan.clone(), stream_row_merge.base.schema().len())
        } else {
            panic!(
                "the root PlanNode must be StreamHashAgg, StreamSimpleAgg, StreamGlobalApproxPercentile, or StreamRowMerge"
            );
        };

        if self.agg_calls().len() == n_final_agg_calls {
            // an existing `count(*)` is used as row count column in `StreamXxxAgg`
            Ok(plan)
        } else {
            // a `count(*)` is appended, should project the output
            assert_eq!(self.agg_calls().len() + 1, n_final_agg_calls);

            let mut project = StreamProject::new(generic::Project::with_out_col_idx(
                plan,
                0..self.schema().len(),
            ));
            // If there's no agg call, then `count(*)` will be the only column in the output besides keys.
            // Since it'll be pruned immediately in `StreamProject`, the update records are likely to be
            // no-op. So we set the hint to instruct the executor to eliminate them.
            // See https://github.com/risingwavelabs/risingwave/issues/17030.
            if self.agg_calls().is_empty() {
                project = project.with_noop_update_hint(true);
            }
            Ok(project.into())
        }
    }

    fn try_better_locality(&self, columns: &[usize]) -> Option<PlanRef> {
        if columns.is_empty() {
            return None;
        }

        // Check if the given columns are a prefix of group keys.
        let group_key = self.group_key().to_vec();
        if columns.len() > group_key.len() || columns != &group_key[..columns.len()] {
            return None;
        }

        // Return the same plan directly without calling `try_better_locality` on input.
        // Because in `logical_rewrite_for_stream`, we will enforce the locality requirement on the group keys anyway.
        // If we call `try_better_locality` on input, it would miss the chance to utilize the locality of the current agg,
        // since the agg's input doesn't have the locality yet at that moment.
        Some(self.clone_with_input(self.input()).into())
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let logical_input = if self.group_key().is_empty() {
            self.input()
        } else {
            try_enforce_locality_requirement(
                self.input(),
                &self.group_key().to_vec(),
                ctx.locality_backfill_enabled(),
            )
        };
        let (input, input_col_change) = logical_input.logical_rewrite_for_stream(ctx)?;
        let (agg, out_col_change) = self.rewrite_with_input(input, input_col_change);
        let (map, _) = out_col_change.into_parts();
        let out_col_change = ColIndexMapping::new(map, agg.schema().len());
        Ok((agg.into(), out_col_change))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{Field, Schema};

    use super::*;
    use crate::expr::{assert_eq_input_ref, input_ref_to_column_indices};
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::LogicalValues;

    #[tokio::test]
    async fn test_create() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock();
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let values = LogicalValues::new(vec![], Schema { fields }, ctx);
        let input = PlanRef::from(values);
        let input_ref_1 = InputRef::new(0, ty.clone());
        let input_ref_2 = InputRef::new(1, ty.clone());
        let input_ref_3 = InputRef::new(2, ty);

        let gen_internal_value = |select_exprs: Vec<ExprImpl>,
                                  group_exprs|
         -> (Vec<ExprImpl>, Vec<PlanAggCall>, IndexSet) {
            let (plan, exprs, _) = LogicalAgg::create(
                select_exprs,
                GroupBy::GroupKey(group_exprs),
                None,
                input.clone(),
            )
            .unwrap();

            let logical_agg = plan.as_logical_agg().unwrap();
            let agg_calls = logical_agg.agg_calls().clone();
            let group_key = logical_agg.group_key().clone();

            (exprs, agg_calls, group_key)
        };

        // Test case: select v1 from test group by v1;
        {
            let select_exprs = vec![input_ref_1.clone().into()];
            let group_exprs = vec![input_ref_1.clone().into()];

            let (exprs, agg_calls, group_key) = gen_internal_value(select_exprs, group_exprs);

            assert_eq!(exprs.len(), 1);
            assert_eq_input_ref!(&exprs[0], 0);

            assert_eq!(agg_calls.len(), 0);
            assert_eq!(group_key, vec![0].into());
        }

        // Test case: select v1, min(v2) from test group by v1;
        {
            let min_v2 = AggCall::new(
                PbAggKind::Min.into(),
                vec![input_ref_2.clone().into()],
                false,
                OrderBy::any(),
                Condition::true_cond(),
                vec![],
            )
            .unwrap();
            let select_exprs = vec![input_ref_1.clone().into(), min_v2.into()];
            let group_exprs = vec![input_ref_1.clone().into()];

            let (exprs, agg_calls, group_key) = gen_internal_value(select_exprs, group_exprs);

            assert_eq!(exprs.len(), 2);
            assert_eq_input_ref!(&exprs[0], 0);
            assert_eq_input_ref!(&exprs[1], 1);

            assert_eq!(agg_calls.len(), 1);
            assert_eq!(agg_calls[0].agg_type, PbAggKind::Min.into());
            assert_eq!(input_ref_to_column_indices(&agg_calls[0].inputs), vec![1]);
            assert_eq!(group_key, vec![0].into());
        }

        // Test case: select v1, min(v2) + max(v3) from t group by v1;
        {
            let min_v2 = AggCall::new(
                PbAggKind::Min.into(),
                vec![input_ref_2.clone().into()],
                false,
                OrderBy::any(),
                Condition::true_cond(),
                vec![],
            )
            .unwrap();
            let max_v3 = AggCall::new(
                PbAggKind::Max.into(),
                vec![input_ref_3.clone().into()],
                false,
                OrderBy::any(),
                Condition::true_cond(),
                vec![],
            )
            .unwrap();
            let func_call =
                FunctionCall::new(ExprType::Add, vec![min_v2.into(), max_v3.into()]).unwrap();
            let select_exprs = vec![input_ref_1.clone().into(), ExprImpl::from(func_call)];
            let group_exprs = vec![input_ref_1.clone().into()];

            let (exprs, agg_calls, group_key) = gen_internal_value(select_exprs, group_exprs);

            assert_eq_input_ref!(&exprs[0], 0);
            if let ExprImpl::FunctionCall(func_call) = &exprs[1] {
                assert_eq!(func_call.func_type(), ExprType::Add);
                let inputs = func_call.inputs();
                assert_eq_input_ref!(&inputs[0], 1);
                assert_eq_input_ref!(&inputs[1], 2);
            } else {
                panic!("Wrong expression type!");
            }

            assert_eq!(agg_calls.len(), 2);
            assert_eq!(agg_calls[0].agg_type, PbAggKind::Min.into());
            assert_eq!(input_ref_to_column_indices(&agg_calls[0].inputs), vec![1]);
            assert_eq!(agg_calls[1].agg_type, PbAggKind::Max.into());
            assert_eq!(input_ref_to_column_indices(&agg_calls[1].inputs), vec![2]);
            assert_eq!(group_key, vec![0].into());
        }

        // Test case: select v2, min(v1 * v3) from test group by v2;
        {
            let v1_mult_v3 = FunctionCall::new(
                ExprType::Multiply,
                vec![input_ref_1.into(), input_ref_3.into()],
            )
            .unwrap();
            let agg_call = AggCall::new(
                PbAggKind::Min.into(),
                vec![v1_mult_v3.into()],
                false,
                OrderBy::any(),
                Condition::true_cond(),
                vec![],
            )
            .unwrap();
            let select_exprs = vec![input_ref_2.clone().into(), agg_call.into()];
            let group_exprs = vec![input_ref_2.into()];

            let (exprs, agg_calls, group_key) = gen_internal_value(select_exprs, group_exprs);

            assert_eq_input_ref!(&exprs[0], 0);
            assert_eq_input_ref!(&exprs[1], 1);

            assert_eq!(agg_calls.len(), 1);
            assert_eq!(agg_calls[0].agg_type, PbAggKind::Min.into());
            assert_eq!(input_ref_to_column_indices(&agg_calls[0].inputs), vec![1]);
            assert_eq!(group_key, vec![0].into());
        }
    }

    /// Generate a agg call node with given [`DataType`] and fields.
    /// For example, `generate_agg_call(Int32, [v1, v2, v3])` will result in:
    /// ```text
    /// Agg(min(input_ref(2))) group by (input_ref(1))
    ///   TableScan(v1, v2, v3)
    /// ```
    fn generate_agg_call(ty: DataType, fields: Vec<Field>) -> LogicalAgg {
        let ctx = OptimizerContext::mock();

        let values = LogicalValues::new(vec![], Schema { fields }, ctx);
        let agg_call = PlanAggCall {
            agg_type: PbAggKind::Min.into(),
            return_type: ty.clone(),
            inputs: vec![InputRef::new(2, ty)],
            distinct: false,
            order_by: vec![],
            filter: Condition::true_cond(),
            direct_args: vec![],
        };
        Agg::new(vec![agg_call], vec![1].into(), values.into()).into()
    }

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Agg(min(input_ref(2))) group by (input_ref(1))
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [0,1] (all columns) will result in
    /// ```text
    /// Agg(min(input_ref(1))) group by (input_ref(0))
    ///  TableScan(v2, v3)
    /// ```
    async fn test_prune_all() {
        let ty = DataType::Int32;
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let agg: PlanRef = generate_agg_call(ty.clone(), fields.clone()).into();
        // Perform the prune
        let required_cols = vec![0, 1];
        let plan = agg.prune_col(&required_cols, &mut ColumnPruningContext::new(agg.clone()));

        // Check the result
        let agg_new = plan.as_logical_agg().unwrap();
        assert_eq!(agg_new.group_key(), &vec![0].into());

        assert_eq!(agg_new.agg_calls().len(), 1);
        let agg_call_new = agg_new.agg_calls()[0].clone();
        assert_eq!(agg_call_new.agg_type, PbAggKind::Min.into());
        assert_eq!(input_ref_to_column_indices(&agg_call_new.inputs), vec![1]);
        assert_eq!(agg_call_new.return_type, ty);

        let values = agg_new.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields(), &fields[1..]);
    }

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Agg(min(input_ref(2))) group by (input_ref(1))
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [1,0] (all columns, with reversed order) will result in
    /// ```text
    /// Project [input_ref(1), input_ref(0)]
    ///   Agg(min(input_ref(1))) group by (input_ref(0))
    ///     TableScan(v2, v3)
    /// ```
    async fn test_prune_all_with_order_required() {
        let ty = DataType::Int32;
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let agg: PlanRef = generate_agg_call(ty.clone(), fields.clone()).into();
        // Perform the prune
        let required_cols = vec![1, 0];
        let plan = agg.prune_col(&required_cols, &mut ColumnPruningContext::new(agg.clone()));
        // Check the result
        let proj = plan.as_logical_project().unwrap();
        assert_eq!(proj.exprs().len(), 2);
        assert_eq!(proj.exprs()[0].as_input_ref().unwrap().index(), 1);
        assert_eq!(proj.exprs()[1].as_input_ref().unwrap().index(), 0);
        let proj_input = proj.input();
        let agg_new = proj_input.as_logical_agg().unwrap();
        assert_eq!(agg_new.group_key(), &vec![0].into());

        assert_eq!(agg_new.agg_calls().len(), 1);
        let agg_call_new = agg_new.agg_calls()[0].clone();
        assert_eq!(agg_call_new.agg_type, PbAggKind::Min.into());
        assert_eq!(input_ref_to_column_indices(&agg_call_new.inputs), vec![1]);
        assert_eq!(agg_call_new.return_type, ty);

        let values = agg_new.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields(), &fields[1..]);
    }

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Agg(min(input_ref(2))) group by (input_ref(1))
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [1] (group key removed) will result in
    /// ```text
    /// Project(input_ref(1))
    ///   Agg(min(input_ref(1))) group by (input_ref(0))
    ///     TableScan(v2, v3)
    /// ```
    async fn test_prune_group_key() {
        let ctx = OptimizerContext::mock();
        let ty = DataType::Int32;
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let values: LogicalValues = LogicalValues::new(
            vec![],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );
        let agg_call = PlanAggCall {
            agg_type: PbAggKind::Min.into(),
            return_type: ty.clone(),
            inputs: vec![InputRef::new(2, ty.clone())],
            distinct: false,
            order_by: vec![],
            filter: Condition::true_cond(),
            direct_args: vec![],
        };
        let agg: PlanRef = Agg::new(vec![agg_call], vec![1].into(), values.into()).into();

        // Perform the prune
        let required_cols = vec![1];
        let plan = agg.prune_col(&required_cols, &mut ColumnPruningContext::new(agg.clone()));

        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 1);
        assert_eq_input_ref!(&project.exprs()[0], 1);

        let agg_new = project.input();
        let agg_new = agg_new.as_logical_agg().unwrap();
        assert_eq!(agg_new.group_key(), &vec![0].into());

        assert_eq!(agg_new.agg_calls().len(), 1);
        let agg_call_new = agg_new.agg_calls()[0].clone();
        assert_eq!(agg_call_new.agg_type, PbAggKind::Min.into());
        assert_eq!(input_ref_to_column_indices(&agg_call_new.inputs), vec![1]);
        assert_eq!(agg_call_new.return_type, ty);

        let values = agg_new.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields(), &fields[1..]);
    }

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Agg(min(input_ref(2)), max(input_ref(1))) group by (input_ref(1), input_ref(2))
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [0,3] will result in
    /// ```text
    /// Project(input_ref(0), input_ref(2))
    ///   Agg(max(input_ref(0))) group by (input_ref(0), input_ref(1))
    ///     TableScan(v2, v3)
    /// ```
    async fn test_prune_agg() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock();
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let values = LogicalValues::new(
            vec![],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );

        let agg_calls = vec![
            PlanAggCall {
                agg_type: PbAggKind::Min.into(),
                return_type: ty.clone(),
                inputs: vec![InputRef::new(2, ty.clone())],
                distinct: false,
                order_by: vec![],
                filter: Condition::true_cond(),
                direct_args: vec![],
            },
            PlanAggCall {
                agg_type: PbAggKind::Max.into(),
                return_type: ty.clone(),
                inputs: vec![InputRef::new(1, ty.clone())],
                distinct: false,
                order_by: vec![],
                filter: Condition::true_cond(),
                direct_args: vec![],
            },
        ];
        let agg: PlanRef = Agg::new(agg_calls, vec![1, 2].into(), values.into()).into();

        // Perform the prune
        let required_cols = vec![0, 3];
        let plan = agg.prune_col(&required_cols, &mut ColumnPruningContext::new(agg.clone()));
        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 2);
        assert_eq_input_ref!(&project.exprs()[0], 0);
        assert_eq_input_ref!(&project.exprs()[1], 2);

        let agg_new = project.input();
        let agg_new = agg_new.as_logical_agg().unwrap();
        assert_eq!(agg_new.group_key(), &vec![0, 1].into());

        assert_eq!(agg_new.agg_calls().len(), 1);
        let agg_call_new = agg_new.agg_calls()[0].clone();
        assert_eq!(agg_call_new.agg_type, PbAggKind::Max.into());
        assert_eq!(input_ref_to_column_indices(&agg_call_new.inputs), vec![0]);
        assert_eq!(agg_call_new.return_type, ty);

        let values = agg_new.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields(), &fields[1..]);
    }
}
