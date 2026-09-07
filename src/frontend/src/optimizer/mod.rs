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

use std::num::NonZeroU32;
use std::ops::DerefMut;
use std::sync::Arc;

use risingwave_pb::catalog::PbVectorIndexInfo;

pub mod plan_node;

use plan_node::StreamFilter;
pub use plan_node::{Explain, LogicalPlanRef, PlanRef};

pub mod property;

mod delta_join_solver;
mod heuristic_optimizer;
mod plan_rewriter;

mod plan_visitor;

#[cfg(feature = "datafusion")]
pub use plan_visitor::DataFusionExecuteCheckerExt;
pub use plan_visitor::{
    ExecutionModeDecider, PlanVisitor, RelationCollectorVisitor, SysTableVisitor,
};
use risingwave_pb::plan_common::source_refresh_mode::RefreshMode;

pub mod backfill_order_strategy;
mod logical_optimization;
mod optimizer_context;
pub mod plan_expr_rewriter;
mod plan_expr_visitor;
mod rule;
pub mod variant_key;

use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;

use educe::Educe;
use fixedbitset::FixedBitSet;
use itertools::Itertools;
pub use logical_optimization::*;
pub use optimizer_context::*;
use plan_expr_rewriter::ConstEvalRewriter;
use property::Order;
use risingwave_common::bail;
use risingwave_common::catalog::{
    ColumnCatalog, ColumnDesc, ConflictBehavior, Field, FieldDisplay, Schema,
};
use risingwave_common::session_config::LocalityBackfillMode;
use risingwave_common::types::DataType;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_connector::WithPropertiesExt;
use risingwave_connector::sink::catalog::SinkFormatDesc;

use self::heuristic_optimizer::ApplyOrder;
use self::plan_node::generic::{self, PhysicalPlanRef};
use self::plan_node::{
    BatchProject, LogicalProject, LogicalSource, PartitionComputeInfo, StreamDml,
    StreamMaterialize, StreamProject, StreamRowIdGen, StreamSink, StreamWatermarkFilter,
    ToStreamContext, stream_enforce_eowc_requirement,
};
#[cfg(debug_assertions)]
use self::plan_visitor::InputRefValidator;
use self::plan_visitor::{CardinalityVisitor, StreamKeyChecker, has_batch_exchange};
use self::property::{Cardinality, RequiredDist};
use self::rule::*;
use self::variant_key::variant_key_error;
use crate::TableCatalog;
use crate::catalog::table_catalog::TableType;
use crate::catalog::{DatabaseId, SchemaId};
use crate::error::{ErrorCode, Result};
use crate::expr::TimestamptzExprFinder;
use crate::handler::create_table::{CreateTableInfo, CreateTableProps};
use crate::optimizer::plan_node::generic::{GenericPlanRef, SourceNodeKind, Union};
use crate::optimizer::plan_node::{
    BackfillType, Batch, BatchExchange, BatchPlanNodeType, BatchPlanRef, ConventionMarker,
    PlanTreeNode, RewriteStreamContext, Stream, StreamExchange, StreamPlanRef, StreamUnion,
    StreamUpstreamSinkUnion, StreamVectorIndexWrite, ToStream, VisitExprsRecursive,
};
use crate::optimizer::plan_visitor::{
    LocalityBackfillScanEstimator, LocalityProviderCounter, RwTimestampValidator,
    TemporalJoinValidator,
};
use crate::optimizer::property::Distribution;
use crate::utils::{
    ColIndexMappingRewriteExt, MV_REFRESH_INTERVAL_SEC_KEY, WithOptionsSecResolved,
};

/// `PlanRoot` is used to describe a plan. planner will construct a `PlanRoot` with `LogicalNode`.
/// and required distribution and order. And `PlanRoot` can generate corresponding streaming or
/// batch plan with optimization. the required Order and Distribution columns might be more than the
/// output columns. for example:
/// ```sql
///    select v1 from t order by id;
/// ```
/// the plan will return two columns (id, v1), and the required order column is id. the id
/// column is required in optimization, but the final generated plan will remove the unnecessary
/// column in the result.
#[derive(Educe)]
#[educe(Debug, Clone)]
pub struct PlanRoot<P: PlanPhase> {
    // The current plan node.
    pub plan: PlanRef<P::Convention>,
    // The phase of the plan.
    #[educe(Debug(ignore), Clone(method(PhantomData::clone)))]
    _phase: PhantomData<P>,
    required_dist: RequiredDist,
    required_order: Order,
    out_fields: FixedBitSet,
    out_names: Vec<String>,
}

/// `PlanPhase` is used to track the phase of the `PlanRoot`.
/// Usually, it begins from `Logical` and ends with `Batch` or `Stream`, unless we want to construct a `PlanRoot` from an intermediate phase.
/// Typical phase transformation are:
/// - `Logical` -> `OptimizedLogicalForBatch` -> `Batch`
/// - `Logical` -> `OptimizedLogicalForStream` -> `Stream`
pub trait PlanPhase {
    type Convention: ConventionMarker;
}

macro_rules! for_all_phase {
    () => {
        for_all_phase! {
            { Logical, $crate::optimizer::plan_node::Logical },
            { BatchOptimizedLogical, $crate::optimizer::plan_node::Logical },
            { StreamOptimizedLogical, $crate::optimizer::plan_node::Stream },
            { Batch, $crate::optimizer::plan_node::Batch },
            { Stream, $crate::optimizer::plan_node::Stream }
        }
    };
    ($({$phase:ident, $convention:ty}),+ $(,)?) => {
        $(
            paste::paste! {
                pub struct [< PlanPhase$phase >];
                impl PlanPhase for [< PlanPhase$phase >] {
                    type Convention = $convention;
                }
                pub type [< $phase PlanRoot >] = PlanRoot<[< PlanPhase$phase >]>;
            }
        )+
    }
}

for_all_phase!();

impl LogicalPlanRoot {
    pub fn new_with_logical_plan(
        plan: LogicalPlanRef,
        required_dist: RequiredDist,
        required_order: Order,
        out_fields: FixedBitSet,
        out_names: Vec<String>,
    ) -> Self {
        Self::new_inner(plan, required_dist, required_order, out_fields, out_names)
    }
}

impl BatchPlanRoot {
    pub fn new_with_batch_plan(
        plan: BatchPlanRef,
        required_dist: RequiredDist,
        required_order: Order,
        out_fields: FixedBitSet,
        out_names: Vec<String>,
    ) -> Self {
        Self::new_inner(plan, required_dist, required_order, out_fields, out_names)
    }
}

impl<P: PlanPhase> PlanRoot<P> {
    fn new_inner(
        plan: PlanRef<P::Convention>,
        required_dist: RequiredDist,
        required_order: Order,
        out_fields: FixedBitSet,
        out_names: Vec<String>,
    ) -> Self {
        let input_schema = plan.schema();
        assert_eq!(input_schema.fields().len(), out_fields.len());
        assert_eq!(out_fields.count_ones(..), out_names.len());

        Self {
            plan,
            _phase: PhantomData,
            required_dist,
            required_order,
            out_fields,
            out_names,
        }
    }

    fn into_phase<P2: PlanPhase>(self, plan: PlanRef<P2::Convention>) -> PlanRoot<P2> {
        PlanRoot {
            plan,
            _phase: PhantomData,
            required_dist: self.required_dist,
            required_order: self.required_order,
            out_fields: self.out_fields,
            out_names: self.out_names,
        }
    }

    /// Set customized names of the output fields, used for `CREATE [MATERIALIZED VIEW | SINK] r(a,
    /// b, ..)`.
    ///
    /// If the number of names does not match the number of output fields, an error is returned.
    pub fn set_out_names(&mut self, out_names: Vec<String>) -> Result<()> {
        if out_names.len() != self.out_fields.count_ones(..) {
            Err(ErrorCode::InvalidInputSyntax(
                "number of column names does not match number of columns".to_owned(),
            ))?
        }
        self.out_names = out_names;
        Ok(())
    }

    /// Get the plan root's schema, only including the fields to be output.
    pub fn schema(&self) -> Schema {
        // The schema can be derived from the `out_fields` and `out_names`, so we don't maintain it
        // as a field and always construct one on demand here to keep it in sync.
        Schema {
            fields: self
                .out_fields
                .ones()
                .map(|i| self.plan.schema().fields()[i].clone())
                .zip_eq_debug(&self.out_names)
                .map(|(field, name)| Field {
                    name: name.clone(),
                    ..field
                })
                .collect(),
        }
    }
}

fn resolve_locality_backfill(
    ctx: &OptimizerContext,
    plan: LogicalPlanRef,
    backfill_type: BackfillType,
) -> bool {
    let config = ctx.session_ctx().config();
    let enabled = config.enable_locality_backfill();
    let mode = config.locality_backfill_mode();
    let min_size = config.auto_locality_backfill_min_size();
    drop(config);

    if !enabled {
        return false;
    }

    match mode {
        LocalityBackfillMode::Always => true,
        LocalityBackfillMode::Auto => {
            if backfill_type.without_snapshot()
                || risingwave_common::license::Feature::LocalityBackfill
                    .check_available()
                    .is_err()
            {
                false
            } else {
                let catalog_reader = ctx.session_ctx().env().catalog_reader().read_guard();
                let estimated_backfill_size =
                    LocalityBackfillScanEstimator::estimate(plan, catalog_reader.table_stats());
                estimated_backfill_size >= min_size
            }
        }
    }
}

const UNLICENSED_LOCALITY_PROVIDER_LIMIT: usize = 5;

fn locality_backfill_requires_license(
    mode: LocalityBackfillMode,
    locality_provider_count: usize,
) -> bool {
    locality_provider_count > 0
        && (mode == LocalityBackfillMode::Auto
            || locality_provider_count > UNLICENSED_LOCALITY_PROVIDER_LIMIT)
}

fn rewrite_logical_plan_for_stream(
    plan: &LogicalPlanRef,
    backfill_type: BackfillType,
    locality_backfill_enabled: bool,
) -> Result<(LogicalPlanRef, ColIndexMapping)> {
    let (plan, out_col_change) = plan.logical_rewrite_for_stream(
        &mut RewriteStreamContext::new_with_backfill_type(backfill_type, locality_backfill_enabled),
    )?;
    if out_col_change.is_injective() {
        Ok((plan, out_col_change))
    } else {
        let mut output_indices = (0..plan.schema().len()).collect_vec();
        #[expect(unused_assignments)]
        let (mut map, mut target_size) = out_col_change.into_parts();

        // TODO(st1page): https://github.com/risingwavelabs/risingwave/issues/7234
        // assert_eq!(target_size, output_indices.len());
        target_size = plan.schema().len();
        let mut tar_exists = vec![false; target_size];
        for i in map.iter_mut().flatten() {
            if tar_exists[*i] {
                output_indices.push(*i);
                *i = target_size;
                target_size += 1;
            } else {
                tar_exists[*i] = true;
            }
        }
        let plan = LogicalProject::with_out_col_idx(plan, output_indices.into_iter());
        let out_col_change = ColIndexMapping::new(map, target_size);
        Ok((plan.into(), out_col_change))
    }
}

impl LogicalPlanRoot {
    /// Transform the [`PlanRoot`] back to a [`PlanRef`] suitable to be used as a subplan, for
    /// example as insert source or subquery. This ignores Order but retains post-Order pruning
    /// (`out_fields`).
    pub fn into_unordered_subplan(self) -> LogicalPlanRef {
        if self.out_fields.count_ones(..) == self.out_fields.len() {
            return self.plan;
        }
        LogicalProject::with_out_fields(self.plan, &self.out_fields).into()
    }

    /// Transform the [`PlanRoot`] wrapped in an array-construction subquery to a [`PlanRef`]
    /// supported by `ARRAY_AGG`. Similar to the unordered version, this abstracts away internal
    /// `self.plan` which is further modified by `self.required_order` then `self.out_fields`.
    pub fn into_array_agg(self) -> Result<LogicalPlanRef> {
        use generic::Agg;
        use plan_node::PlanAggCall;
        use risingwave_common::types::ListValue;
        use risingwave_expr::aggregate::PbAggKind;

        use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};
        use crate::utils::{Condition, IndexSet};

        let Ok(select_idx) = Itertools::exactly_one(self.out_fields.ones()) else {
            bail!("subquery must return only one column");
        };
        let input_column_type = self.plan.schema().fields()[select_idx].data_type();
        let return_type = DataType::list(input_column_type.clone());
        let agg = Agg::new(
            vec![PlanAggCall {
                agg_type: PbAggKind::ArrayAgg.into(),
                return_type: return_type.clone(),
                inputs: vec![InputRef::new(select_idx, input_column_type.clone())],
                distinct: false,
                order_by: self.required_order.column_orders,
                filter: Condition::true_cond(),
                direct_args: vec![],
            }],
            IndexSet::empty(),
            self.plan,
        );
        Ok(LogicalProject::create(
            agg.into(),
            vec![
                FunctionCall::new(
                    ExprType::Coalesce,
                    vec![
                        InputRef::new(0, return_type).into(),
                        ExprImpl::literal_list(
                            ListValue::empty(&input_column_type),
                            input_column_type,
                        ),
                    ],
                )
                .unwrap()
                .into(),
            ],
        ))
    }

    /// Apply logical optimization to the plan for stream.
    pub fn gen_optimized_logical_plan_for_stream(mut self) -> Result<LogicalPlanRoot> {
        self.plan = LogicalOptimizer::gen_optimized_logical_plan_for_stream(self.plan.clone())?;
        Ok(self)
    }

    /// Apply logical optimization to the plan for batch.
    pub fn gen_optimized_logical_plan_for_batch(self) -> Result<BatchOptimizedLogicalPlanRoot> {
        let plan = LogicalOptimizer::gen_optimized_logical_plan_for_batch(self.plan.clone())?;
        Ok(self.into_phase(plan))
    }

    pub fn gen_batch_plan(self) -> Result<BatchPlanRoot> {
        self.gen_optimized_logical_plan_for_batch()?
            .gen_batch_plan()
    }
}

impl BatchOptimizedLogicalPlanRoot {
    /// Rejects `VARIANT` wherever a batch plan would group, deduplicate or order by it. See
    /// [`crate::optimizer::variant_key`] for why this is batch's last line of defense.
    fn reject_variant_keys(&self) -> Result<()> {
        if let Some(err) = StreamKeyChecker::Variant.visit(self.plan.clone()) {
            return Err(variant_key_error(err));
        }
        let schema = self.plan.schema();
        for order in &self.required_order.column_orders {
            let field = &schema[order.column_index];
            if field.data_type().contains_variant() {
                return Err(variant_key_error(format!(
                    "VARIANT column \"{}\" should not be in the ORDER BY.",
                    FieldDisplay(field)
                )));
            }
        }
        Ok(())
    }

    /// Optimize and generate a singleton batch physical plan without exchange nodes.
    pub fn gen_batch_plan(self) -> Result<BatchPlanRoot> {
        self.reject_variant_keys()?;
        if TemporalJoinValidator::exist_dangling_temporal_scan(self.plan.clone()) {
            return Err(ErrorCode::NotSupported(
                "do not support temporal join for batch queries".to_owned(),
                "please use temporal join in streaming queries".to_owned(),
            )
            .into());
        }

        let ctx = self.plan.ctx();
        // Inline session timezone mainly for rewriting now()
        let mut plan = inline_session_timezone_in_exprs(ctx.clone(), self.plan.clone())?;

        // Const eval of exprs at the last minute, but before `to_batch` to make functional index selection happy.
        plan = const_eval_exprs(plan)?;

        if ctx.is_explain_trace() {
            ctx.trace("Const eval exprs:");
            ctx.trace(plan.explain_to_string());
        }

        // Convert to physical plan node
        let mut plan = plan.to_batch_with_order_required(&self.required_order)?;
        if ctx.is_explain_trace() {
            ctx.trace("To Batch Plan:");
            ctx.trace(plan.explain_to_string());
        }

        plan = plan.optimize_by_rules(&OptimizationStage::<Batch>::new(
            "Merge BatchProject",
            vec![BatchProjectMergeRule::create()],
            ApplyOrder::BottomUp,
        ))?;

        // Inline session timezone
        plan = inline_session_timezone_in_exprs(ctx.clone(), plan)?;

        if ctx.is_explain_trace() {
            ctx.trace("Inline Session Timezone:");
            ctx.trace(plan.explain_to_string());
        }

        #[cfg(debug_assertions)]
        InputRefValidator.validate(plan.clone());
        assert_eq!(
            *plan.distribution(),
            Distribution::Single,
            "{}",
            plan.explain_to_string()
        );
        assert!(
            !has_batch_exchange(plan.clone()),
            "{}",
            plan.explain_to_string()
        );

        let ctx = plan.ctx();
        if ctx.is_explain_trace() {
            ctx.trace("To Batch Physical Plan:");
            ctx.trace(plan.explain_to_string());
        }

        Ok(self.into_phase(plan))
    }

    #[cfg(feature = "datafusion")]
    pub fn gen_datafusion_logical_plan(
        &self,
    ) -> Result<Arc<datafusion::logical_expr::LogicalPlan>> {
        use datafusion::logical_expr::{Expr as DFExpr, LogicalPlan, Projection, Sort};
        use datafusion_common::Column;
        use plan_visitor::LogicalPlanToDataFusionExt;

        use crate::datafusion::{InputColumns, convert_column_order};

        tracing::debug!(
            "Converting RisingWave logical plan to DataFusion plan:\nRisingWave Plan: {:?}",
            self.plan
        );

        let ctx = self.plan.ctx();
        // Inline session timezone mainly for rewriting now()
        let mut plan = inline_session_timezone_in_exprs(ctx, self.plan.clone())?;
        plan = const_eval_exprs(plan)?;

        let mut df_plan = plan.to_datafusion_logical_plan()?;

        if !self.required_order.is_any() {
            let input_columns = InputColumns::new(df_plan.schema().as_ref(), plan.schema());
            let expr = self
                .required_order
                .column_orders
                .iter()
                .map(|column_order| convert_column_order(column_order, &input_columns))
                .collect_vec();
            df_plan = Arc::new(LogicalPlan::Sort(Sort {
                expr,
                input: df_plan,
                fetch: None,
            }));
        }

        if self.out_names.len() < df_plan.schema().fields().len() {
            let df_schema = df_plan.schema().as_ref();
            let projection_exprs = self
                .out_fields
                .ones()
                .zip_eq_debug(self.out_names.iter())
                .map(|(i, name)| {
                    DFExpr::Column(Column::from(df_schema.qualified_field(i))).alias(name)
                })
                .collect_vec();
            df_plan = Arc::new(LogicalPlan::Projection(Projection::try_new(
                projection_exprs,
                df_plan,
            )?));
        }

        tracing::debug!("Converted DataFusion plan:\nDataFusion Plan: {:?}", df_plan);

        Ok(df_plan)
    }
}

impl BatchPlanRoot {
    /// Optimize and generate a batch query plan for distributed execution.
    pub fn gen_batch_distributed_plan(mut self) -> Result<BatchPlanRef> {
        self.required_dist = RequiredDist::single();
        let mut plan = self.plan;

        // Convert to distributed plan
        plan = plan.to_distributed_with_required(&self.required_order, &self.required_dist)?;

        let ctx = plan.ctx();
        if ctx.is_explain_trace() {
            ctx.trace("To Batch Distributed Plan:");
            ctx.trace(plan.explain_to_string());
        }
        if require_additional_exchange_on_root_in_distributed_mode(plan.clone()) {
            plan =
                BatchExchange::new(plan, self.required_order.clone(), Distribution::Single).into();
        }

        // Add Project if the any position of `self.out_fields` is set to zero.
        if self.out_fields.count_ones(..) != self.out_fields.len() {
            plan =
                BatchProject::new(generic::Project::with_out_fields(plan, &self.out_fields)).into();
        }

        // Both two phase limit and topn could generate limit on top of the scan, so we push limit here.
        let plan = plan.optimize_by_rules(&OptimizationStage::new(
            "Push Limit To Scan",
            vec![BatchPushLimitToScanRule::create()],
            ApplyOrder::BottomUp,
        ))?;

        Ok(plan)
    }

    /// Optimize and generate a batch query plan for local execution.
    pub fn gen_batch_local_plan(self) -> Result<BatchPlanRef> {
        let mut plan = self.plan;

        // Convert to local plan node
        plan = plan.to_local_with_order_required(&self.required_order)?;

        // We remark that since the `to_local_with_order_required` does not enforce single
        // distribution, we enforce at the root if needed.
        let insert_exchange = match plan.distribution() {
            Distribution::Single => require_additional_exchange_on_root_in_local_mode(plan.clone()),
            _ => true,
        };
        if insert_exchange {
            plan =
                BatchExchange::new(plan, self.required_order.clone(), Distribution::Single).into()
        }

        // Add Project if the any position of `self.out_fields` is set to zero.
        if self.out_fields.count_ones(..) != self.out_fields.len() {
            plan =
                BatchProject::new(generic::Project::with_out_fields(plan, &self.out_fields)).into();
        }

        let ctx = plan.ctx();
        if ctx.is_explain_trace() {
            ctx.trace("To Batch Local Plan:");
            ctx.trace(plan.explain_to_string());
        }

        // Both two phase limit and topn could generate limit on top of the scan, so we push limit here.
        let plan = plan.optimize_by_rules(&OptimizationStage::new(
            "Push Limit To Scan",
            vec![BatchPushLimitToScanRule::create()],
            ApplyOrder::BottomUp,
        ))?;

        Ok(plan)
    }
}

impl LogicalPlanRoot {
    /// Generate optimized stream plan
    pub(crate) fn derive_backfill_type(&self, allow_snapshot_backfill: bool) -> BackfillType {
        if allow_snapshot_backfill && self.should_use_snapshot_backfill() {
            BackfillType::SnapshotBackfill
        } else {
            BackfillType::ArrangementBackfill
        }
    }

    fn gen_optimized_stream_plan(
        self,
        emit_on_window_close: bool,
        backfill_type: BackfillType,
    ) -> Result<StreamOptimizedLogicalPlanRoot> {
        let ctx = self.plan.ctx();
        let _explain_trace = ctx.is_explain_trace();

        let optimized_plan = self.gen_stream_plan(emit_on_window_close, backfill_type)?;

        let mut plan = optimized_plan
            .plan
            .clone()
            .optimize_by_rules(&OptimizationStage::new(
                "Merge StreamProject",
                vec![StreamProjectMergeRule::create()],
                ApplyOrder::BottomUp,
            ))?;

        if ctx
            .session_ctx()
            .config()
            .streaming_separate_consecutive_join()
        {
            plan = plan.optimize_by_rules(&OptimizationStage::new(
                "Separate consecutive StreamHashJoin by no-shuffle StreamExchange",
                vec![SeparateConsecutiveJoinRule::create()],
                ApplyOrder::BottomUp,
            ))?;
        }

        // Add Logstore for Unaligned join
        // Apply this BEFORE delta join rule, because delta join removes
        // the join
        if ctx.session_ctx().config().streaming_enable_unaligned_join() {
            plan = plan.optimize_by_rules(&OptimizationStage::new(
                "Add Logstore for Unaligned join",
                vec![AddLogstoreRule::create()],
                ApplyOrder::BottomUp,
            ))?;
        }

        if ctx.session_ctx().config().streaming_enable_delta_join()
            && ctx.session_ctx().config().enable_index_selection()
        {
            // TODO: make it a logical optimization.
            // Rewrite joins with index to delta join
            plan = plan.optimize_by_rules(&OptimizationStage::new(
                "To IndexDeltaJoin",
                vec![IndexDeltaJoinRule::create()],
                ApplyOrder::BottomUp,
            ))?;
        }
        // Inline session timezone
        plan = inline_session_timezone_in_exprs(ctx.clone(), plan)?;

        if ctx.is_explain_trace() {
            ctx.trace("Inline session timezone:");
            ctx.trace(plan.explain_to_string());
        }

        // Const eval of exprs at the last minute
        plan = const_eval_exprs(plan)?;

        if ctx.is_explain_trace() {
            ctx.trace("Const eval exprs:");
            ctx.trace(plan.explain_to_string());
        }

        #[cfg(debug_assertions)]
        InputRefValidator.validate(plan.clone());

        if TemporalJoinValidator::exist_dangling_temporal_scan(plan.clone()) {
            return Err(ErrorCode::NotSupported(
                "exist dangling temporal scan".to_owned(),
                "please check your temporal join syntax e.g. consider removing the right outer join if it is being used.".to_owned(),
            ).into());
        }

        if RwTimestampValidator::select_rw_timestamp_in_stream_query(plan.clone()) {
            return Err(ErrorCode::NotSupported(
                "selecting `_rw_timestamp` in a streaming query is not allowed".to_owned(),
                "please run the sql in batch mode or remove the column `_rw_timestamp` from the streaming query".to_owned(),
            ).into());
        }

        Ok(optimized_plan.into_phase(plan))
    }

    pub(crate) fn require_snapshot_backfill_for_batch_refresh(&self) -> Result<()> {
        let ctx = self.plan.ctx();
        let session_ctx = ctx.session_ctx();
        let snapshot_backfill_enabled = session_ctx
            .env()
            .streaming_config()
            .developer
            .enable_snapshot_backfill
            && session_ctx.config().streaming_use_snapshot_backfill();
        if !snapshot_backfill_enabled {
            return Err(ErrorCode::NotSupported(
                "Batch refresh materialized view requires snapshot backfill".to_owned(),
                format!(
                    "Please enable snapshot backfill or remove `{}` from the WITH clause.",
                    MV_REFRESH_INTERVAL_SEC_KEY
                ),
            )
            .into());
        }
        if let Some(reason) = self.plan.forbid_snapshot_backfill() {
            return Err(ErrorCode::NotSupported(
                format!("Batch refresh materialized view requires snapshot backfill, but {reason}"),
                "Please rewrite the query to avoid operators that forbid snapshot backfill."
                    .to_owned(),
            )
            .into());
        }
        Ok(())
    }

    /// Generate create index or create materialize view plan.
    fn gen_stream_plan(
        self,
        emit_on_window_close: bool,
        backfill_type: BackfillType,
    ) -> Result<StreamOptimizedLogicalPlanRoot> {
        let ctx = self.plan.ctx();
        let explain_trace = ctx.is_explain_trace();

        let plan = {
            {
                if let Some(err) = StreamKeyChecker::Variant.visit(self.plan.clone()) {
                    return Err(variant_key_error(err));
                }
                if !ctx
                    .session_ctx()
                    .config()
                    .streaming_allow_jsonb_in_stream_key()
                    && let Some(err) = StreamKeyChecker::Jsonb.visit(self.plan.clone())
                {
                    return Err(ErrorCode::NotSupported(
                        err,
                        "Using JSONB columns as part of the join or aggregation keys can severely impair performance. \
                        If you intend to proceed, force to enable it with: `set rw_streaming_allow_jsonb_in_stream_key to true`".to_owned(),
                    ).into());
                }
                let mut optimized_plan = self.gen_optimized_logical_plan_for_stream()?;
                let locality_backfill_enabled =
                    resolve_locality_backfill(&ctx, optimized_plan.plan.clone(), backfill_type);
                let (mut plan, mut out_col_change) = rewrite_logical_plan_for_stream(
                    &optimized_plan.plan,
                    backfill_type,
                    locality_backfill_enabled,
                )?;

                let locality_provider_count = LocalityProviderCounter::count(plan.clone());
                let locality_backfill_mode = ctx.session_ctx().config().locality_backfill_mode();
                if locality_backfill_enabled
                    && locality_backfill_requires_license(
                        locality_backfill_mode,
                        locality_provider_count,
                    )
                    && risingwave_common::license::Feature::LocalityBackfill
                        .check_available()
                        .is_err()
                {
                    ctx.warn_to_user(format!(
                        "The streaming job would use {locality_provider_count} locality providers, \
                         which are unavailable under the current license. Falling back to regular backfill."
                    ));
                    (plan, out_col_change) = rewrite_logical_plan_for_stream(
                        &optimized_plan.plan,
                        backfill_type,
                        false,
                    )?;
                }
                if explain_trace {
                    ctx.trace("Logical Rewrite For Stream:");
                    ctx.trace(plan.explain_to_string());
                }

                optimized_plan.required_dist =
                    out_col_change.rewrite_required_distribution(&optimized_plan.required_dist);
                optimized_plan.required_order = out_col_change
                    .rewrite_required_order(&optimized_plan.required_order)
                    .unwrap();
                optimized_plan.out_fields =
                    out_col_change.rewrite_bitset(&optimized_plan.out_fields);
                let mut plan = plan.to_stream_with_dist_required(
                    &optimized_plan.required_dist,
                    &mut ToStreamContext::new_with_backfill_type(
                        emit_on_window_close,
                        backfill_type,
                    ),
                )?;
                plan = stream_enforce_eowc_requirement(ctx.clone(), plan, emit_on_window_close)?;
                optimized_plan.into_phase(plan)
            }
        };

        if explain_trace {
            ctx.trace("To Stream Plan:");
            // TODO: can be `plan.plan.explain_to_string()`, but should explicitly specify the type due to some limitation of rust compiler
            ctx.trace(<PlanRef<Stream> as Explain>::explain_to_string(&plan.plan));
        }
        Ok(plan)
    }

    /// Visit the plan root and compute the cardinality.
    ///
    /// Panics if not called on a logical plan.
    fn compute_cardinality(&self) -> Cardinality {
        CardinalityVisitor.visit(self.plan.clone())
    }

    /// Optimize and generate a create table plan.
    pub fn gen_table_plan(
        self,
        context: OptimizerContextRef,
        table_name: String,
        database_id: DatabaseId,
        schema_id: SchemaId,
        CreateTableInfo {
            columns,
            pk_column_ids,
            row_id_index,
            watermark_descs,
            source_catalog,
            version,
        }: CreateTableInfo,
        CreateTableProps {
            definition,
            append_only,
            on_conflict,
            with_version_columns,
            webhook_info,
            engine,
        }: CreateTableProps,
    ) -> Result<StreamMaterialize> {
        let backfill_type = self.derive_backfill_type(false);
        // Snapshot backfill is not allowed for create table
        let stream_plan = self.gen_optimized_stream_plan(false, backfill_type)?;

        assert!(!pk_column_ids.is_empty() || row_id_index.is_some());

        let pk_column_indices = {
            let mut id_to_idx = HashMap::new();

            columns.iter().enumerate().for_each(|(idx, c)| {
                id_to_idx.insert(c.column_id(), idx);
            });
            pk_column_ids
                .iter()
                .map(|c| id_to_idx.get(c).copied().unwrap()) // pk column id must exist in table columns.
                .collect_vec()
        };

        fn inject_project_for_generated_column_if_needed(
            columns: &[ColumnCatalog],
            node: StreamPlanRef,
        ) -> Result<StreamPlanRef> {
            let exprs = LogicalSource::derive_output_exprs_from_generated_columns(columns)?;
            if let Some(exprs) = exprs {
                let logical_project = generic::Project::new(exprs, node);
                return Ok(StreamProject::new(logical_project).into());
            }
            Ok(node)
        }

        #[derive(PartialEq, Debug, Copy, Clone)]
        enum PrimaryKeyKind {
            UserDefinedPrimaryKey,
            NonAppendOnlyRowIdPk,
            AppendOnlyRowIdPk,
        }

        fn inject_dml_node(
            columns: &[ColumnCatalog],
            append_only: bool,
            stream_plan: StreamPlanRef,
            pk_column_indices: &[usize],
            kind: PrimaryKeyKind,
            column_descs: Vec<ColumnDesc>,
        ) -> Result<StreamPlanRef> {
            let mut dml_node = StreamDml::new(stream_plan, append_only, column_descs).into();

            // Add generated columns.
            dml_node = inject_project_for_generated_column_if_needed(columns, dml_node)?;

            dml_node = match kind {
                PrimaryKeyKind::UserDefinedPrimaryKey | PrimaryKeyKind::NonAppendOnlyRowIdPk => {
                    RequiredDist::hash_shard(pk_column_indices)
                        .streaming_enforce_if_not_satisfies(dml_node)?
                }
                PrimaryKeyKind::AppendOnlyRowIdPk => {
                    StreamExchange::new_no_shuffle(dml_node).into()
                }
            };

            Ok(dml_node)
        }

        let kind = if let Some(row_id_index) = row_id_index {
            assert_eq!(
                Itertools::exactly_one(pk_column_indices.iter())
                    .copied()
                    .unwrap(),
                row_id_index
            );
            if append_only {
                PrimaryKeyKind::AppendOnlyRowIdPk
            } else {
                PrimaryKeyKind::NonAppendOnlyRowIdPk
            }
        } else {
            PrimaryKeyKind::UserDefinedPrimaryKey
        };

        let column_descs: Vec<ColumnDesc> = columns
            .iter()
            .filter(|&c| c.can_dml())
            .map(|c| c.column_desc.clone())
            .collect();

        let mut not_null_idxs = vec![];
        for (idx, column) in column_descs.iter().enumerate() {
            if !column.nullable {
                not_null_idxs.push(idx);
            }
        }

        let version_column_indices = if !with_version_columns.is_empty() {
            find_version_column_indices(&columns, with_version_columns)?
        } else {
            vec![]
        };

        let with_external_source = source_catalog.is_some();
        let (dml_source_node, external_source_node) = if with_external_source {
            let dummy_source_node = LogicalSource::new(
                None,
                columns.clone(),
                row_id_index,
                SourceNodeKind::CreateTable,
                context.clone(),
                None,
            )
            .and_then(|s| {
                s.to_stream(&mut ToStreamContext::new_with_backfill_type(
                    false,
                    // Dummy DML source planning does not create stream table scans, so this
                    // required context value is only a placeholder and is not used for backfill
                    // selection.
                    BackfillType::ArrangementBackfill,
                ))
            })?;
            let mut external_source_node = stream_plan.plan;
            external_source_node =
                inject_project_for_generated_column_if_needed(&columns, external_source_node)?;
            external_source_node = match kind {
                PrimaryKeyKind::UserDefinedPrimaryKey => {
                    RequiredDist::hash_shard(&pk_column_indices)
                        .streaming_enforce_if_not_satisfies(external_source_node)?
                }

                PrimaryKeyKind::NonAppendOnlyRowIdPk | PrimaryKeyKind::AppendOnlyRowIdPk => {
                    StreamExchange::new_no_shuffle(external_source_node).into()
                }
            };
            (dummy_source_node, Some(external_source_node))
        } else {
            (stream_plan.plan, None)
        };

        let dml_node = inject_dml_node(
            &columns,
            append_only,
            dml_source_node,
            &pk_column_indices,
            kind,
            column_descs,
        )?;

        let dists = external_source_node
            .iter()
            .map(|input| input.distribution())
            .chain([dml_node.distribution()])
            .unique()
            .collect_vec();

        let dist = match &dists[..] {
            &[Distribution::SomeShard, Distribution::HashShard(_)]
            | &[Distribution::HashShard(_), Distribution::SomeShard] => Distribution::SomeShard,
            &[dist @ Distribution::SomeShard] | &[dist @ Distribution::HashShard(_)] => {
                dist.clone()
            }
            _ => {
                unreachable!()
            }
        };

        let generated_column_exprs =
            LogicalSource::derive_output_exprs_from_generated_columns(&columns)?;
        let upstream_sink_union = StreamUpstreamSinkUnion::new(
            context.clone(),
            dml_node.schema(),
            dml_node.stream_key(),
            dist.clone(), // should always be the same as dist of `Union`
            append_only,
            row_id_index.is_none(),
            generated_column_exprs,
        );

        let union_inputs = external_source_node
            .into_iter()
            .chain([dml_node, upstream_sink_union.into()])
            .collect_vec();

        let mut stream_plan: StreamPlanRef = StreamUnion::new_with_dist(
            Union {
                all: true,
                inputs: union_inputs,
                source_col: None,
            },
            dist,
        )
        .into();

        let ttl_watermark_indices = watermark_descs
            .iter()
            .filter(|d| d.with_ttl)
            .map(|d| d.watermark_idx as usize)
            .collect_vec();

        let add_row_id_gen = |stream_plan: StreamPlanRef, row_id_index| match kind {
            PrimaryKeyKind::UserDefinedPrimaryKey => {
                unreachable!()
            }
            PrimaryKeyKind::NonAppendOnlyRowIdPk | PrimaryKeyKind::AppendOnlyRowIdPk => {
                StreamRowIdGen::new_with_dist(
                    stream_plan,
                    row_id_index,
                    Distribution::HashShard(vec![row_id_index]),
                )
                .into()
            }
        };

        // Add RowIDGen before WatermarkFilter, so filtering always sees a valid row-id key.
        if let Some(row_id_index) = row_id_index {
            stream_plan = add_row_id_gen(stream_plan, row_id_index);
        }

        // Add WatermarkFilter node.
        if !watermark_descs.is_empty() {
            stream_plan = StreamWatermarkFilter::new(stream_plan, watermark_descs).into();
        }

        let conflict_behavior = on_conflict.to_behavior(append_only, row_id_index.is_some())?;

        if let ConflictBehavior::IgnoreConflict = conflict_behavior
            && !version_column_indices.is_empty()
        {
            Err(ErrorCode::InvalidParameterValue(
                "The with version column syntax cannot be used with the ignore behavior of on conflict".to_owned(),
            ))?
        }

        let retention_seconds = context.with_options().retention_seconds();

        let table_required_dist = {
            let mut bitset = FixedBitSet::with_capacity(columns.len());
            for idx in &pk_column_indices {
                bitset.insert(*idx);
            }
            RequiredDist::ShardByKey(bitset)
        };

        let mut stream_plan = inline_session_timezone_in_exprs(context, stream_plan)?;

        if !not_null_idxs.is_empty() {
            stream_plan =
                StreamFilter::filter_out_any_null_rows(stream_plan.clone(), &not_null_idxs);
        }

        // Determine if the table should be refreshable based on the connector type
        let refreshable = source_catalog
            .as_ref()
            .map(|catalog| {
                catalog.with_properties.supports_full_reload_refresh()
                    && matches!(
                        catalog
                            .refresh_mode
                            .as_ref()
                            .map(|refresh_mode| refresh_mode.refresh_mode),
                        Some(Some(RefreshMode::FullReload(_)))
                    )
            })
            .unwrap_or(false);

        // Validate that refreshable tables have a user-defined primary key (i.e., does not have rowid)
        if refreshable && row_id_index.is_some() {
            return Err(crate::error::ErrorCode::BindError(
                "Refreshable tables must have a PRIMARY KEY. Please define a primary key for the table."
                    .to_owned(),
            )
            .into());
        }

        StreamMaterialize::create_for_table(
            stream_plan,
            table_name,
            database_id,
            schema_id,
            table_required_dist,
            Order::any(),
            columns,
            definition,
            conflict_behavior,
            version_column_indices,
            pk_column_indices,
            ttl_watermark_indices,
            row_id_index,
            version,
            retention_seconds,
            webhook_info,
            engine,
            refreshable,
        )
    }

    /// Optimize and generate a create materialized view plan.
    pub fn gen_materialize_plan(
        self,
        database_id: DatabaseId,
        schema_id: SchemaId,
        mv_name: String,
        definition: String,
        emit_on_window_close: bool,
        backfill_type: BackfillType,
    ) -> Result<StreamMaterialize> {
        let cardinality = self.compute_cardinality();
        let stream_plan = self.gen_optimized_stream_plan(emit_on_window_close, backfill_type)?;
        StreamMaterialize::create(
            stream_plan,
            mv_name,
            database_id,
            schema_id,
            definition,
            TableType::MaterializedView,
            cardinality,
            None,
        )
    }

    /// Optimize and generate a create index plan.
    pub fn gen_index_plan(
        self,
        index_name: String,
        database_id: DatabaseId,
        schema_id: SchemaId,
        definition: String,
        retention_seconds: Option<NonZeroU32>,
    ) -> Result<StreamMaterialize> {
        let cardinality = self.compute_cardinality();
        let backfill_type = self.derive_backfill_type(false);
        let stream_plan = self.gen_optimized_stream_plan(false, backfill_type)?;

        StreamMaterialize::create(
            stream_plan,
            index_name,
            database_id,
            schema_id,
            definition,
            TableType::Index,
            cardinality,
            retention_seconds,
        )
    }

    pub fn gen_vector_index_plan(
        self,
        index_name: String,
        database_id: DatabaseId,
        schema_id: SchemaId,
        definition: String,
        retention_seconds: Option<NonZeroU32>,
        vector_index_info: PbVectorIndexInfo,
    ) -> Result<StreamVectorIndexWrite> {
        let cardinality = self.compute_cardinality();
        let backfill_type = self.derive_backfill_type(false);
        let stream_plan = self.gen_optimized_stream_plan(false, backfill_type)?;

        StreamVectorIndexWrite::create(
            stream_plan,
            index_name,
            database_id,
            schema_id,
            definition,
            cardinality,
            retention_seconds,
            vector_index_info,
        )
    }

    /// Optimize and generate a create sink plan.
    #[expect(clippy::too_many_arguments)]
    pub fn gen_sink_plan(
        self,
        sink_name: String,
        definition: String,
        properties: WithOptionsSecResolved,
        emit_on_window_close: bool,
        db_name: String,
        sink_from_table_name: String,
        format_desc: Option<SinkFormatDesc>,
        without_snapshot: bool,
        since_timestamp: bool,
        is_iceberg_engine_internal: bool,
        target_table: Option<Arc<TableCatalog>>,
        partition_info: Option<PartitionComputeInfo>,
        user_specified_columns: bool,
        auto_refresh_schema_from_table: Option<Arc<TableCatalog>>,
    ) -> Result<StreamSink> {
        let backfill_type = if since_timestamp {
            assert!(
                target_table.is_none(),
                "should not allow since_timestamp for sink-into-table"
            );
            if is_iceberg_engine_internal {
                return Err(ErrorCode::InvalidInputSyntax(
                    "since_timestamp is not allowed for this sink".to_owned(),
                )
                .into());
            }
            BackfillType::SnapshotBackfillSinceTimestamp
        } else if without_snapshot {
            BackfillType::UpstreamOnlySink
        } else if target_table.is_none()
            && !is_iceberg_engine_internal
            && self.should_use_snapshot_backfill()
            && {
                if auto_refresh_schema_from_table.is_some() {
                    self.plan.ctx().session_ctx().notice_to_user("Auto schema change only support for ArrangementBackfill. Switched to use ArrangementBackfill");
                    false
                } else {
                    true
                }
            }
        {
            assert!(
                target_table.is_none(),
                "should not allow snapshot backfill for sink-into-table"
            );
            // Snapshot backfill on sink-into-table is not allowed
            BackfillType::SnapshotBackfill
        } else {
            BackfillType::ArrangementBackfill
        };
        if auto_refresh_schema_from_table.is_some()
            && backfill_type != BackfillType::ArrangementBackfill
        {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "auto schema change only support for ArrangementBackfill, but got: {:?}",
                backfill_type
            ))
            .into());
        }
        let stream_plan = self.gen_optimized_stream_plan(emit_on_window_close, backfill_type)?;
        let target_columns_to_plan_mapping = target_table.as_ref().map(|t| {
            let columns = t.columns_without_rw_timestamp();
            stream_plan.target_columns_to_plan_mapping(&columns, user_specified_columns)
        });

        StreamSink::create(
            stream_plan,
            sink_name,
            db_name,
            sink_from_table_name,
            target_table,
            target_columns_to_plan_mapping,
            definition,
            properties,
            format_desc,
            partition_info,
            auto_refresh_schema_from_table,
        )
    }

    pub fn should_use_snapshot_backfill(&self) -> bool {
        let ctx = self.plan.ctx();
        let session_ctx = ctx.session_ctx();
        let use_snapshot_backfill = session_ctx
            .env()
            .streaming_config()
            .developer
            .enable_snapshot_backfill
            && session_ctx.config().streaming_use_snapshot_backfill();
        if use_snapshot_backfill {
            if let Some(warning_msg) = self.plan.forbid_snapshot_backfill() {
                self.plan.ctx().session_ctx().notice_to_user(warning_msg);
                false
            } else {
                true
            }
        } else {
            false
        }
    }
}

impl<P: PlanPhase> PlanRoot<P> {
    /// used when the plan has a target relation such as DML and sink into table, return the mapping from table's columns to the plan's schema
    pub fn target_columns_to_plan_mapping(
        &self,
        tar_cols: &[ColumnCatalog],
        user_specified_columns: bool,
    ) -> Vec<Option<usize>> {
        #[expect(clippy::disallowed_methods)]
        let visible_cols: Vec<(usize, String)> = self
            .out_fields
            .ones()
            .zip_eq(self.out_names.iter().cloned())
            .collect_vec();

        let visible_col_idxes = visible_cols.iter().map(|(i, _)| *i).collect_vec();
        let visible_col_idxes_by_name = visible_cols
            .iter()
            .map(|(i, name)| (name.as_ref(), *i))
            .collect::<BTreeMap<_, _>>();

        tar_cols
            .iter()
            .enumerate()
            .filter(|(_, tar_col)| tar_col.can_dml())
            .map(|(tar_i, tar_col)| {
                if user_specified_columns {
                    visible_col_idxes_by_name.get(tar_col.name()).cloned()
                } else {
                    (tar_i < visible_col_idxes.len()).then(|| visible_cols[tar_i].0)
                }
            })
            .collect()
    }
}

fn find_version_column_indices(
    column_catalog: &Vec<ColumnCatalog>,
    version_column_names: Vec<String>,
) -> Result<Vec<usize>> {
    let mut indices = Vec::new();
    for version_column_name in version_column_names {
        let mut found = false;
        for (index, column) in column_catalog.iter().enumerate() {
            if column.column_desc.name == version_column_name {
                if let &DataType::Jsonb
                | &DataType::Variant
                | &DataType::List(_)
                | &DataType::Struct(_)
                | &DataType::Bytea
                | &DataType::Boolean = column.data_type()
                {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "Version column {} must be of a comparable data type",
                        version_column_name
                    ))
                    .into());
                }
                indices.push(index);
                found = true;
                break;
            }
        }
        if !found {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "Version column {} not found",
                version_column_name
            ))
            .into());
        }
    }
    Ok(indices)
}

fn const_eval_exprs<C: ConventionMarker>(plan: PlanRef<C>) -> Result<PlanRef<C>> {
    let mut const_eval_rewriter = ConstEvalRewriter { error: None };

    let plan = plan.rewrite_exprs_recursive(&mut const_eval_rewriter);
    if let Some(error) = const_eval_rewriter.error {
        return Err(error);
    }
    Ok(plan)
}

fn inline_session_timezone_in_exprs<C: ConventionMarker>(
    ctx: OptimizerContextRef,
    plan: PlanRef<C>,
) -> Result<PlanRef<C>> {
    let mut v = TimestamptzExprFinder::default();
    plan.visit_exprs_recursive(&mut v);
    if v.has() {
        Ok(plan.rewrite_exprs_recursive(ctx.session_timezone().deref_mut()))
    } else {
        Ok(plan)
    }
}

fn exist_and_no_exchange_before(
    plan: &BatchPlanRef,
    is_candidate: fn(&BatchPlanRef) -> bool,
) -> bool {
    if plan.node_type() == BatchPlanNodeType::BatchExchange {
        return false;
    }
    is_candidate(plan)
        || plan
            .inputs()
            .iter()
            .any(|input| exist_and_no_exchange_before(input, is_candidate))
}

impl BatchPlanRef {
    fn is_user_table_scan(&self) -> bool {
        self.node_type() == BatchPlanNodeType::BatchSeqScan
            || self.node_type() == BatchPlanNodeType::BatchLogSeqScan
            || self.node_type() == BatchPlanNodeType::BatchVectorSearch
    }

    fn is_source_scan(&self) -> bool {
        self.node_type() == BatchPlanNodeType::BatchSource
            || self.node_type() == BatchPlanNodeType::BatchKafkaScan
            || self.node_type() == BatchPlanNodeType::BatchIcebergScan
    }

    fn is_lookup_join(&self) -> bool {
        self.node_type() == BatchPlanNodeType::BatchLookupJoin
    }

    fn is_insert(&self) -> bool {
        self.node_type() == BatchPlanNodeType::BatchInsert
    }

    fn is_update(&self) -> bool {
        self.node_type() == BatchPlanNodeType::BatchUpdate
    }

    fn is_delete(&self) -> bool {
        self.node_type() == BatchPlanNodeType::BatchDelete
    }
}

/// As we always run the root stage locally, for some plan in root stage which need to execute in
/// compute node we insert an additional exhchange before it to avoid to include it in the root
/// stage.
///
/// Returns `true` if we must insert an additional exchange to ensure this.
fn require_additional_exchange_on_root_in_distributed_mode(plan: BatchPlanRef) -> bool {
    assert_eq!(plan.distribution(), &Distribution::Single);
    exist_and_no_exchange_before(&plan, |plan| {
        plan.is_user_table_scan()
            || plan.is_source_scan()
            || plan.is_insert()
            || plan.is_update()
            || plan.is_delete()
            // A lookup join on a singleton lookup table may be already in `Single`
            // distribution and reads from the state store, so it must not be executed
            // in the root stage on the frontend.
            || plan.is_lookup_join()
    })
}

/// The purpose is same as `require_additional_exchange_on_root_in_distributed_mode`. We separate
/// them for the different requirement of plan node in different execute mode.
fn require_additional_exchange_on_root_in_local_mode(plan: BatchPlanRef) -> bool {
    assert_eq!(plan.distribution(), &Distribution::Single);
    exist_and_no_exchange_before(&plan, |plan| {
        plan.is_user_table_scan() || plan.is_source_scan() || plan.is_insert()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::plan_node::LogicalValues;

    #[tokio::test]
    async fn test_as_subplan() {
        let ctx = OptimizerContext::mock();
        let values = LogicalValues::new(
            vec![],
            Schema::new(vec![
                Field::with_name(DataType::Int32, "v1"),
                Field::with_name(DataType::Varchar, "v2"),
            ]),
            ctx,
        )
        .into();
        let out_fields = FixedBitSet::with_capacity_and_blocks(2, [1]);
        let out_names = vec!["v1".into()];
        let root = PlanRoot::new_with_logical_plan(
            values,
            RequiredDist::Any,
            Order::any(),
            out_fields,
            out_names,
        );
        let subplan = root.into_unordered_subplan();
        assert_eq!(
            subplan.schema(),
            &Schema::new(vec![Field::with_name(DataType::Int32, "v1")])
        );
    }
}
