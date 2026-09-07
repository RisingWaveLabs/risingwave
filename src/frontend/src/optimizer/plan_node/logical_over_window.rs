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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_common::{bail_not_implemented, not_implemented};
use risingwave_expr::aggregate::{AggType, PbAggKind, agg_types};
use risingwave_expr::window_function::{Frame, FrameBound, WindowFuncKind};

use super::generic::{GenericPlanRef, OverWindow, PlanWindowFunction, ProjectBuilder};
use super::utils::impl_distill_by_unit;
use super::{
    BatchOverWindow, ColPrunable, ExprRewritable, Logical, LogicalPlanRef as PlanRef,
    LogicalProject, PlanBase, PlanTreeNodeUnary, PredicatePushdown, StreamEowcOverWindow,
    StreamOverWindow, StreamWatermarkSort, ToBatch, ToStream, gen_filter_and_pushdown,
    try_enforce_locality_requirement,
};
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{
    AggCall, Expr, ExprImpl, ExprRewriter, ExprType, ExprVisitor, FunctionCall, InputRef,
    WindowFunction,
};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::logical_agg::LogicalAggBuilder;
use crate::optimizer::plan_node::{
    ColumnPruningContext, Literal, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::RequiredDist;
use crate::utils::{ColIndexMapping, Condition, IndexSet};

struct LogicalOverWindowBuilder<'a> {
    /// the builder of the input Project
    input_proj_builder: &'a ProjectBuilder,
    /// the window functions
    window_functions: &'a mut Vec<WindowFunction>,
    /// the error during the expression rewriting
    error: Option<RwError>,
}

impl<'a> LogicalOverWindowBuilder<'a> {
    fn new(
        input_proj_builder: &'a ProjectBuilder,
        window_functions: &'a mut Vec<WindowFunction>,
    ) -> Result<Self> {
        Ok(Self {
            input_proj_builder,
            window_functions,
            error: None,
        })
    }

    fn rewrite_selected_items(&mut self, selected_items: Vec<ExprImpl>) -> Result<Vec<ExprImpl>> {
        let mut rewritten_items = vec![];
        for expr in selected_items {
            let rewritten_expr = self.rewrite_expr(expr);
            if let Some(error) = self.error.take() {
                return Err(error);
            } else {
                rewritten_items.push(rewritten_expr);
            }
        }
        Ok(rewritten_items)
    }

    fn schema_over_window_start_offset(&self) -> usize {
        self.input_proj_builder.exprs_len()
    }

    fn push_window_func(&mut self, window_func: WindowFunction) -> InputRef {
        if let Some((pos, existing)) = self
            .window_functions
            .iter()
            .find_position(|&w| w == &window_func)
        {
            return InputRef::new(
                self.schema_over_window_start_offset() + pos,
                existing.return_type.clone(),
            );
        }
        let index = self.schema_over_window_start_offset() + self.window_functions.len();
        let data_type = window_func.return_type.clone();
        self.window_functions.push(window_func);
        InputRef::new(index, data_type)
    }

    fn try_rewrite_window_function(&mut self, window_func: WindowFunction) -> Result<ExprImpl> {
        let WindowFunction {
            kind,
            args,
            return_type,
            partition_by,
            order_by,
            ignore_nulls,
            frame,
        } = window_func;

        let new_expr = if let WindowFuncKind::Aggregate(agg_type) = &kind
            && matches!(agg_type, agg_types::rewritten!())
        {
            let agg_call = AggCall::new(
                agg_type.clone(),
                args,
                false,
                order_by,
                Condition::true_cond(),
                vec![],
            )?;
            LogicalAggBuilder::general_rewrite_agg_call(agg_call, |agg_call| {
                Ok(self.push_window_func(
                    // AggCall -> WindowFunction
                    WindowFunction::new(
                        WindowFuncKind::Aggregate(agg_call.agg_type),
                        agg_call.args.clone(),
                        false, // we don't support `IGNORE NULLS` for these functions now
                        partition_by.clone(),
                        agg_call.order_by,
                        frame.clone(),
                    )?,
                ))
            })?
        } else {
            ExprImpl::from(self.push_window_func(WindowFunction::new(
                kind,
                args,
                ignore_nulls,
                partition_by,
                order_by,
                frame,
            )?))
        };

        assert_eq!(new_expr.return_type(), return_type);
        Ok(new_expr)
    }
}

impl ExprRewriter for LogicalOverWindowBuilder<'_> {
    fn rewrite_window_function(&mut self, window_func: WindowFunction) -> ExprImpl {
        let dummy = Literal::new(None, window_func.return_type()).into();
        match self.try_rewrite_window_function(window_func) {
            Ok(expr) => expr,
            Err(err) => {
                self.error = Some(err);
                dummy
            }
        }
    }

    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        let input_expr = input_ref.into();
        let index = self.input_proj_builder.expr_index(&input_expr).unwrap();
        ExprImpl::from(InputRef::new(index, input_expr.return_type()))
    }
}

/// Build columns from window function `args` / `partition_by` / `order_by`
struct OverWindowProjectBuilder<'a> {
    builder: &'a mut ProjectBuilder,
    error: Option<ErrorCode>,
}

impl<'a> OverWindowProjectBuilder<'a> {
    fn new(builder: &'a mut ProjectBuilder) -> Self {
        Self {
            builder,
            error: None,
        }
    }

    fn try_visit_window_function(
        &mut self,
        window_function: &WindowFunction,
    ) -> std::result::Result<(), ErrorCode> {
        if let WindowFuncKind::Aggregate(agg_type) = &window_function.kind
            && matches!(
                agg_type,
                AggType::Builtin(
                    PbAggKind::StddevPop
                        | PbAggKind::StddevSamp
                        | PbAggKind::VarPop
                        | PbAggKind::VarSamp
                )
            )
        {
            let input = Itertools::exactly_one(window_function.args.iter()).unwrap();
            let squared_input_expr = ExprImpl::from(
                FunctionCall::new(ExprType::Multiply, vec![input.clone(), input.clone()]).unwrap(),
            );
            self.builder
                .add_expr(&squared_input_expr)
                .map_err(|err| not_implemented!("{err} inside args"))?;
        }
        for arg in &window_function.args {
            self.builder
                .add_expr(arg)
                .map_err(|err| not_implemented!("{err} inside args"))?;
        }
        for partition_by in &window_function.partition_by {
            self.builder
                .add_expr(partition_by)
                .map_err(|err| not_implemented!("{err} inside partition_by"))?;
        }
        for order_by in window_function.order_by.sort_exprs.iter().map(|e| &e.expr) {
            self.builder
                .add_expr(order_by)
                .map_err(|err| not_implemented!("{err} inside order_by"))?;
        }
        Ok(())
    }
}

impl ExprVisitor for OverWindowProjectBuilder<'_> {
    fn visit_window_function(&mut self, window_function: &WindowFunction) {
        if let Err(e) = self.try_visit_window_function(window_function) {
            self.error = Some(e);
        }
    }
}

/// `LogicalOverWindow` performs `OVER` window functions to its input.
///
/// The output schema is the input schema plus the window functions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalOverWindow {
    pub base: PlanBase<Logical>,
    core: OverWindow<PlanRef>,
}

impl LogicalOverWindow {
    pub fn new(calls: Vec<PlanWindowFunction>, input: PlanRef) -> Self {
        let core = OverWindow::new(calls, input);
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }

    fn build_input_proj(input: PlanRef, select_exprs: &[ExprImpl]) -> Result<ProjectBuilder> {
        let mut input_proj_builder = ProjectBuilder::default();
        // Add and check input columns
        for (idx, field) in input.schema().fields().iter().enumerate() {
            input_proj_builder
                .add_expr(&InputRef::new(idx, field.data_type()).into())
                .map_err(|err| not_implemented!("{err} inside input"))?;
        }
        let mut build_input_proj_visitor = OverWindowProjectBuilder::new(&mut input_proj_builder);
        for expr in select_exprs {
            build_input_proj_visitor.visit_expr(expr);
            if let Some(error) = build_input_proj_visitor.error.take() {
                return Err(error.into());
            }
        }
        Ok(input_proj_builder)
    }

    pub fn create(input: PlanRef, select_exprs: Vec<ExprImpl>) -> Result<(PlanRef, Vec<ExprImpl>)> {
        let input_proj_builder = Self::build_input_proj(input.clone(), &select_exprs)?;

        let mut window_functions = vec![];
        let mut over_window_builder =
            LogicalOverWindowBuilder::new(&input_proj_builder, &mut window_functions)?;

        let rewritten_selected_items = over_window_builder.rewrite_selected_items(select_exprs)?;

        for window_func in &window_functions {
            if window_func.kind.is_numbering() && window_func.order_by.sort_exprs.is_empty() {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "window rank function without order by: {:?}",
                    window_func
                ))
                .into());
            }
        }

        let plan_window_funcs = window_functions
            .into_iter()
            .map(|x| Self::convert_window_function(x, &input_proj_builder))
            .try_collect()?;

        Ok((
            Self::new(
                plan_window_funcs,
                LogicalProject::with_core(input_proj_builder.build(input)).into(),
            )
            .into(),
            rewritten_selected_items,
        ))
    }

    fn convert_window_function(
        window_function: WindowFunction,
        input_proj_builder: &ProjectBuilder,
    ) -> Result<PlanWindowFunction> {
        let order_by = window_function
            .order_by
            .sort_exprs
            .into_iter()
            .map(|e| {
                ColumnOrder::new(
                    input_proj_builder.expr_index(&e.expr).unwrap(),
                    e.order_type,
                )
            })
            .collect_vec();
        let partition_by = window_function
            .partition_by
            .into_iter()
            .map(|e| InputRef::new(input_proj_builder.expr_index(&e).unwrap(), e.return_type()))
            .collect_vec();

        let mut args = window_function.args;
        let (kind, frame) = match window_function.kind {
            WindowFuncKind::RowNumber | WindowFuncKind::Rank | WindowFuncKind::DenseRank => {
                // ignore user-defined frame for rank functions, also, rank functions only care
                // about the rows before current row
                (
                    window_function.kind,
                    Frame::rows(FrameBound::UnboundedPreceding, FrameBound::CurrentRow),
                )
            }
            WindowFuncKind::Lag | WindowFuncKind::Lead => {
                // `lag(x, const offset N) over ()`
                //     == `first_value(x) over (rows between N preceding and N preceding)`
                // `lead(x, const offset N) over ()`
                //     == `first_value(x) over (rows between N following and N following)`
                assert!(!window_function.ignore_nulls); // the conversion is not applicable to `LAG`/`LEAD` with `IGNORE NULLS`

                let offset = if args.len() > 1 {
                    let offset_expr = args.remove(1);
                    if !offset_expr.return_type().is_int() {
                        return Err(ErrorCode::InvalidInputSyntax(format!(
                            "the `offset` of `{}` function should be integer",
                            window_function.kind
                        ))
                        .into());
                    }
                    let const_offset = offset_expr
                        .cast_implicit(&DataType::Int64)?
                        .try_fold_const();
                    if const_offset.is_none() {
                        // should already be checked in `WindowFunction::infer_return_type`,
                        // but just in case
                        bail_not_implemented!(
                            "non-const `offset` of `lag`/`lead` is not supported yet"
                        );
                    }
                    const_offset.unwrap()?.map(|v| *v.as_int64()).unwrap_or(1)
                } else {
                    1
                };
                let sign = if window_function.kind == WindowFuncKind::Lag {
                    -1
                } else {
                    1
                };
                let abs_offset = offset.unsigned_abs() as usize;
                let frame = if sign * offset <= 0 {
                    Frame::rows(
                        FrameBound::Preceding(abs_offset),
                        FrameBound::Preceding(abs_offset),
                    )
                } else {
                    Frame::rows(
                        FrameBound::Following(abs_offset),
                        FrameBound::Following(abs_offset),
                    )
                };

                (
                    WindowFuncKind::Aggregate(AggType::Builtin(PbAggKind::FirstValue)),
                    frame,
                )
            }
            WindowFuncKind::Aggregate(_) => {
                let frame = window_function.frame.unwrap_or({
                    // FIXME(rc): The following 2 cases should both be `Frame::Range(Unbounded,
                    // CurrentRow)` but we don't support yet.
                    if order_by.is_empty() {
                        Frame::rows(
                            FrameBound::UnboundedPreceding,
                            FrameBound::UnboundedFollowing,
                        )
                    } else {
                        Frame::rows(FrameBound::UnboundedPreceding, FrameBound::CurrentRow)
                    }
                });
                (window_function.kind, frame)
            }
        };

        let args = args
            .into_iter()
            .map(|e| InputRef::new(input_proj_builder.expr_index(&e).unwrap(), e.return_type()))
            .collect_vec();

        Ok(PlanWindowFunction {
            kind,
            return_type: window_function.return_type,
            args,
            ignore_nulls: window_function.ignore_nulls,
            partition_by,
            order_by,
            frame,
        })
    }

    pub fn window_functions(&self) -> &[PlanWindowFunction] {
        &self.core.window_functions
    }

    pub fn partition_key_indices(&self) -> Vec<usize> {
        self.core.partition_key_indices()
    }

    pub fn order_key(&self) -> &[ColumnOrder] {
        self.core.order_key()
    }

    #[must_use]
    fn rewrite_with_input_and_window(
        &self,
        input: PlanRef,
        window_functions: &[PlanWindowFunction],
        input_col_change: ColIndexMapping,
    ) -> Self {
        let window_functions = window_functions
            .iter()
            .cloned()
            .map(|mut window_function| {
                window_function.args.iter_mut().for_each(|i| {
                    *i = InputRef::new(input_col_change.map(i.index()), i.return_type())
                });
                window_function.order_by.iter_mut().for_each(|o| {
                    o.column_index = input_col_change.map(o.column_index);
                });
                window_function.partition_by.iter_mut().for_each(|i| {
                    *i = InputRef::new(input_col_change.map(i.index()), i.return_type())
                });
                window_function
            })
            .collect();
        Self::new(window_functions, input)
    }

    pub fn split_with_rule(&self, groups: Vec<Vec<usize>>) -> PlanRef {
        assert!(groups.iter().flatten().all_unique());
        assert!(
            groups
                .iter()
                .flatten()
                .all(|&idx| idx < self.window_functions().len())
        );

        let input_len = self.input().schema().len();
        let original_out_fields = (0..input_len + self.window_functions().len()).collect_vec();
        let mut out_fields = original_out_fields.clone();
        let mut cur_input = self.input();
        let mut cur_node = self.clone();
        let mut cur_win_func_pos = input_len;
        for func_indices in &groups {
            cur_node = Self::new(
                func_indices
                    .iter()
                    .map(|&idx| {
                        let func = &self.window_functions()[idx];
                        out_fields[input_len + idx] = cur_win_func_pos;
                        cur_win_func_pos += 1;
                        func.clone()
                    })
                    .collect_vec(),
                cur_input.clone(),
            );
            cur_input = cur_node.clone().into();
        }
        if out_fields == original_out_fields {
            cur_node.into()
        } else {
            LogicalProject::with_out_col_idx(cur_node.into(), out_fields.into_iter()).into()
        }
    }

    pub fn decompose(self) -> (PlanRef, Vec<PlanWindowFunction>) {
        self.core.decompose()
    }
}

impl PlanTreeNodeUnary<Logical> for LogicalOverWindow {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.core.window_functions.clone(), input)
    }

    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let input_len = self.core.input_len();
        let new_input_len = input.schema().len();
        let output_len = self.core.output_len();
        let new_output_len = new_input_len + self.window_functions().len();
        let output_col_change = {
            let mut mapping = ColIndexMapping::empty(output_len, new_output_len);
            for win_func_idx in 0..self.window_functions().len() {
                mapping.put(input_len + win_func_idx, Some(new_input_len + win_func_idx));
            }
            mapping.union(&input_col_change)
        };
        let new_self =
            self.rewrite_with_input_and_window(input, self.window_functions(), input_col_change);
        (new_self, output_col_change)
    }
}

impl_plan_tree_node_for_unary! { Logical, LogicalOverWindow }
impl_distill_by_unit!(LogicalOverWindow, core, "LogicalOverWindow");

impl ColPrunable for LogicalOverWindow {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let input_len = self.input().schema().len();

        let (req_cols_input_part, req_cols_win_func_part) = {
            let mut in_input = required_cols.to_vec();
            let in_win_funcs: IndexSet = in_input.extract_if(.., |i| *i >= input_len).collect();
            (IndexSet::from(in_input), in_win_funcs)
        };

        if req_cols_win_func_part.is_empty() {
            // no window function is needed
            return self.input().prune_col(&req_cols_input_part.to_vec(), ctx);
        }

        let (input_cols_required_by_this, window_functions) = {
            let mut tmp = IndexSet::empty();
            let new_window_functions = req_cols_win_func_part
                .indices()
                .map(|idx| self.window_functions()[idx - input_len].clone())
                .inspect(|func| {
                    tmp.extend(func.args.iter().map(|x| x.index()));
                    tmp.extend(func.partition_by.iter().map(|x| x.index()));
                    tmp.extend(func.order_by.iter().map(|x| x.column_index));
                })
                .collect_vec();
            (tmp, new_window_functions)
        };

        let input_required_cols = (req_cols_input_part | input_cols_required_by_this).to_vec();
        let input_col_change =
            ColIndexMapping::with_remaining_columns(&input_required_cols, input_len);
        let new_self = {
            let input = self.input().prune_col(&input_required_cols, ctx);
            self.rewrite_with_input_and_window(input, &window_functions, input_col_change)
        };
        if new_self.schema().len() == required_cols.len() {
            // current schema perfectly fit the required columns
            new_self.into()
        } else {
            // some columns are not needed so we did a projection to remove the columns.
            let mut new_output_cols = input_required_cols.clone();
            new_output_cols.extend(required_cols.iter().filter(|&&x| x >= input_len));
            let mapping =
                &ColIndexMapping::with_remaining_columns(&new_output_cols, self.schema().len());
            let output_required_cols = required_cols
                .iter()
                .map(|&idx| mapping.map(idx))
                .collect_vec();
            let src_size = new_self.schema().len();
            LogicalProject::with_mapping(
                new_self.into(),
                ColIndexMapping::with_remaining_columns(&output_required_cols, src_size),
            )
            .into()
        }
    }
}

impl ExprRewritable<Logical> for LogicalOverWindow {}

impl ExprVisitable for LogicalOverWindow {}

impl PredicatePushdown for LogicalOverWindow {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        if !self.core.funcs_have_same_partition_and_order() {
            // Window function calls with different PARTITION BY and ORDER BY clauses are not split yet.
            // No pushdown, but keep recursing with a `true` condition so that shares below
            // still receive a contribution from this parent (see `PredicatePushdownContext`).
            return gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx);
        }

        let all_out_cols: FixedBitSet = (0..self.schema().len()).collect();
        let mut remain_cols: FixedBitSet = all_out_cols
            .difference(&self.partition_key_indices().into_iter().collect())
            .collect();
        remain_cols.grow(self.schema().len());

        let (remain_pred, pushed_pred) = predicate.split_disjoint(&remain_cols);
        gen_filter_and_pushdown(self, remain_pred, pushed_pred, ctx)
    }
}

macro_rules! empty_partition_by_not_implemented {
    () => {
        bail_not_implemented!(
            issue = 11505,
            "Window function with empty PARTITION BY is not supported because of potential bad performance. \
            If you really need this, please workaround with something like `PARTITION BY 1::int`."
        )
    };
}

impl ToBatch for LogicalOverWindow {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        assert!(
            self.core.funcs_have_same_partition_and_order(),
            "must apply OverWindowSplitRule before generating physical plan"
        );

        let input = self.input().to_batch()?;
        let core = self.core.clone_with_input(input);
        Ok(BatchOverWindow::new(core).into())
    }
}

impl ToStream for LogicalOverWindow {
    fn to_stream(
        &self,
        ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        use super::stream::prelude::*;

        assert!(
            self.core.funcs_have_same_partition_and_order(),
            "must apply OverWindowSplitRule before generating physical plan"
        );

        let partition_key_indices = self.window_functions()[0]
            .partition_by
            .iter()
            .map(|e| e.index())
            .collect_vec();
        // TODO(rc): Let's not introduce too many cases at once. Later we may decide to support
        // empty PARTITION BY by simply removing the following check.
        if partition_key_indices.is_empty() {
            empty_partition_by_not_implemented!();
        }

        let stream_input = self.input().to_stream(ctx)?;

        if ctx.emit_on_window_close() {
            // Emit-On-Window-Close case

            let order_by = &self.window_functions()[0].order_by;
            if order_by.len() != 1 || order_by[0].order_type != OrderType::ascending() {
                return Err(ErrorCode::InvalidInputSyntax(
                    "Only support window functions order by single column and in ascending order"
                        .to_owned(),
                )
                .into());
            }
            if !stream_input
                .watermark_columns()
                .contains(order_by[0].column_index)
            {
                let order_by_col = self.input().schema().fields()[order_by[0].column_index]
                    .name
                    .clone();
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "The ORDER BY column `{}` must be a watermark column",
                    order_by_col
                ))
                .into());
            }
            let order_key_index = order_by[0].column_index;

            let sort_input =
                RequiredDist::shard_by_key(stream_input.schema().len(), &partition_key_indices)
                    .streaming_enforce_if_not_satisfies(stream_input)?;
            // After sharding by partition key, `StreamWatermarkSort` gives rows in the same partition
            // and `ORDER BY` value a deterministic tie-break based on the preserved input stream
            // key. This matches `EowcOverWindow`'s persisted order
            // (`partition key | order key | input pk`), so numbering functions recover with the
            // same peer ordering as the live path.
            let sort = StreamWatermarkSort::new(sort_input, order_key_index);

            let core = self.core.clone_with_input(sort.into());
            Ok(StreamEowcOverWindow::new(core).into())
        } else {
            // General (Emit-On-Update) case

            if self
                .window_functions()
                .iter()
                .any(|f| f.frame.bounds.is_session())
            {
                bail_not_implemented!(
                    "Session frame is not yet supported in general streaming mode. \
                    Please consider using Emit-On-Window-Close mode."
                );
            }

            let new_input =
                RequiredDist::shard_by_key(stream_input.schema().len(), &partition_key_indices)
                    .streaming_enforce_if_not_satisfies(stream_input)?;
            let core = self.core.clone_with_input(new_input);

            Ok(StreamOverWindow::new(core)?.into())
        }
    }

    fn try_better_locality(&self, columns: &[usize]) -> Option<PlanRef> {
        if columns.is_empty() {
            return None;
        }

        let partition_key_indices = self.partition_key_indices();
        if columns.len() > partition_key_indices.len()
            || columns != &partition_key_indices[..columns.len()]
        {
            return None;
        }

        // Similar to agg/topn, keep the current over-window node so the locality can be provided
        // by its own state table after `to_stream`, instead of trying to enforce it on input
        // during logical rewrite.
        Some(self.clone_with_input(self.input()).into())
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let partition_key_indices = self.window_functions()[0]
            .partition_by
            .iter()
            .map(|e| e.index())
            .collect_vec();
        let logical_input = if partition_key_indices.is_empty() {
            self.input()
        } else {
            try_enforce_locality_requirement(
                self.input(),
                &partition_key_indices,
                ctx.locality_backfill_enabled(),
            )
        };
        let (input, input_col_change) = logical_input.logical_rewrite_for_stream(ctx)?;
        let (new_self, output_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((new_self.into(), output_col_change))
    }
}
