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
use risingwave_common::bail;
use thiserror_ext::AsReport as _;

use super::plan_node::{ConventionMarker, Logical, LogicalPlanRef};
use super::plan_visitor::has_logical_max_one_row;
use crate::error::Result;
use crate::expr::NowProcTimeFinder;
use crate::optimizer::PlanRef;
use crate::optimizer::heuristic_optimizer::{ApplyOrder, HeuristicOptimizer};
use crate::optimizer::plan_node::{
    ColPrunable, ColumnPruningContext, PredicatePushdown, PredicatePushdownContext,
    VisitExprsRecursive,
};
use crate::optimizer::plan_rewriter::ShareSourceRewriter;
#[cfg(debug_assertions)]
use crate::optimizer::plan_visitor::InputRefValidator;
use crate::optimizer::plan_visitor::{
    HasMaxOneRowApply, PlanCheckApplyEliminationExt, PlanVisitor, has_logical_apply,
};
use crate::optimizer::rule::*;
use crate::utils::Condition;
use crate::{Binder, Explain, OptimizerContextRef, Planner};

impl<C: ConventionMarker> PlanRef<C> {
    fn optimize_by_rules_inner(
        self,
        heuristic_optimizer: &mut HeuristicOptimizer<'_, C>,
        stage_name: &str,
    ) -> Result<PlanRef<C>> {
        let ctx = self.ctx();

        let result = heuristic_optimizer.optimize(self);
        let stats = heuristic_optimizer.get_stats();

        if ctx.is_explain_trace() && stats.has_applied_rule() {
            ctx.trace(format!("{}:", stage_name));
            ctx.trace(format!("{}", stats));
            ctx.trace(match &result {
                Ok(plan) => plan.explain_to_string(),
                Err(error) => format!("Optimization failed: {}", error.as_report()),
            });
        }
        ctx.add_rule_applied(stats.total_applied());

        result
    }

    pub(crate) fn optimize_by_rules(
        self,
        OptimizationStage {
            stage_name,
            rules,
            apply_order,
        }: &OptimizationStage<C>,
    ) -> Result<PlanRef<C>> {
        self.optimize_by_rules_inner(&mut HeuristicOptimizer::new(apply_order, rules), stage_name)
    }

    pub(crate) fn optimize_by_rules_until_fix_point(
        mut self,
        OptimizationStage {
            stage_name,
            rules,
            apply_order,
        }: &OptimizationStage<C>,
    ) -> Result<PlanRef<C>> {
        loop {
            let mut heuristic_optimizer = HeuristicOptimizer::new(apply_order, rules);
            self = self.optimize_by_rules_inner(&mut heuristic_optimizer, stage_name)?;
            if !heuristic_optimizer.get_stats().has_applied_rule() {
                return Ok(self);
            }
        }
    }
}

pub struct OptimizationStage<C: ConventionMarker = Logical> {
    stage_name: String,
    rules: Vec<BoxedRule<C>>,
    apply_order: ApplyOrder,
}

impl<C: ConventionMarker> OptimizationStage<C> {
    pub fn new<S>(name: S, rules: Vec<BoxedRule<C>>, apply_order: ApplyOrder) -> Self
    where
        S: Into<String>,
    {
        OptimizationStage {
            stage_name: name.into(),
            rules,
            apply_order,
        }
    }
}

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use risingwave_common::id::ObjectId;
use risingwave_pb::common::PbObjectType;
use risingwave_sqlparser::ast::Statement;

use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_visitor::RelationCollectorVisitor;
use crate::session::SessionImpl;

pub struct LogicalOptimizer {}

static DAG_TO_TREE: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "DAG To Tree",
        vec![DagToTreeRule::create()],
        ApplyOrder::TopDown,
    )
});

static STREAM_GENERATE_SERIES_WITH_NOW: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Convert GENERATE_SERIES Ends With NOW",
        vec![GenerateSeriesWithNowRule::create()],
        ApplyOrder::TopDown,
    )
});

static TABLE_FUNCTION_CONVERT: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Table Function Convert",
        vec![
            // Apply file scan rule first
            TableFunctionToFileScanRule::create(),
            // Apply internal backfill progress rule first
            TableFunctionToInternalBackfillProgressRule::create(),
            // Apply internal source backfill progress rule next
            TableFunctionToInternalSourceBackfillProgressRule::create(),
            // Apply internal get channel delta stats rule next
            TableFunctionToInternalGetChannelDeltaStatsRule::create(),
            // Apply postgres query rule next
            TableFunctionToPostgresQueryRule::create(),
            // Apply mysql query rule next
            TableFunctionToMySqlQueryRule::create(),
            // Apply mssql query rule next
            TableFunctionToMssqlQueryRule::create(),
            // Apply project set rule last
            TableFunctionToProjectSetRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static TABLE_FUNCTION_TO_FILE_SCAN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Table Function To FileScan",
        vec![TableFunctionToFileScanRule::create()],
        ApplyOrder::TopDown,
    )
});

static TABLE_FUNCTION_TO_POSTGRES_QUERY: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Table Function To PostgresQuery",
        vec![TableFunctionToPostgresQueryRule::create()],
        ApplyOrder::TopDown,
    )
});

static TABLE_FUNCTION_TO_MYSQL_QUERY: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Table Function To MySQL",
        vec![TableFunctionToMySqlQueryRule::create()],
        ApplyOrder::TopDown,
    )
});

static TABLE_FUNCTION_TO_MSSQL_QUERY: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "TableFunctionToMssqlQueryRule",
        vec![TableFunctionToMssqlQueryRule::create()],
        ApplyOrder::TopDown,
    )
});

static TABLE_FUNCTION_TO_INTERNAL_BACKFILL_PROGRESS: LazyLock<OptimizationStage> =
    LazyLock::new(|| {
        OptimizationStage::new(
            "Table Function To Internal Backfill Progress",
            vec![TableFunctionToInternalBackfillProgressRule::create()],
            ApplyOrder::TopDown,
        )
    });

static TABLE_FUNCTION_TO_INTERNAL_SOURCE_BACKFILL_PROGRESS: LazyLock<OptimizationStage> =
    LazyLock::new(|| {
        OptimizationStage::new(
            "Table Function To Internal Source Backfill Progress",
            vec![TableFunctionToInternalSourceBackfillProgressRule::create()],
            ApplyOrder::TopDown,
        )
    });

static TABLE_FUNCTION_TO_INTERNAL_GET_CHANNEL_DELTA_STATS: LazyLock<OptimizationStage> =
    LazyLock::new(|| {
        OptimizationStage::new(
            "Table Function To Internal Get Channel Delta Stats",
            vec![TableFunctionToInternalGetChannelDeltaStatsRule::create()],
            ApplyOrder::TopDown,
        )
    });

static VALUES_EXTRACT_PROJECT: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Values Extract Project",
        vec![ValuesExtractProjectRule::create()],
        ApplyOrder::TopDown,
    )
});

static SIMPLE_UNNESTING: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Simple Unnesting",
        vec![
            // Pull correlated predicates up the algebra tree to unnest simple subquery.
            PullUpCorrelatedPredicateRule::create(),
            // Pull correlated project expressions with values to inline scalar subqueries.
            PullUpCorrelatedProjectValueRule::create(),
            PullUpCorrelatedPredicateAggRule::create(),
            // Eliminate max one row
            MaxOneRowEliminateRule::create(),
            // Eliminate lateral table-function apply into a unary ProjectSet.
            ApplyTableFunctionToProjectSetRule::create(),
            // Convert apply to join.
            ApplyToJoinRule::create(),
        ],
        ApplyOrder::BottomUp,
    )
});

static SET_OPERATION_MERGE: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Set Operation Merge",
        vec![
            UnionMergeRule::create(),
            IntersectMergeRule::create(),
            ExceptMergeRule::create(),
        ],
        ApplyOrder::BottomUp,
    )
});

static GENERAL_UNNESTING_TRANS_APPLY_WITH_SHARE: LazyLock<OptimizationStage> =
    LazyLock::new(|| {
        OptimizationStage::new(
            "General Unnesting(Translate Apply)",
            vec![TranslateApplyRule::create(true)],
            ApplyOrder::TopDown,
        )
    });

static GENERAL_UNNESTING_TRANS_APPLY_WITHOUT_SHARE: LazyLock<OptimizationStage> =
    LazyLock::new(|| {
        OptimizationStage::new(
            "General Unnesting(Translate Apply)",
            vec![TranslateApplyRule::create(false)],
            ApplyOrder::TopDown,
        )
    });

static GENERAL_UNNESTING_PUSH_DOWN_APPLY: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "General Unnesting(Push Down Apply)",
        vec![
            ApplyEliminateRule::create(),
            ApplyAggTransposeRule::create(),
            ApplyDedupTransposeRule::create(),
            ApplyFilterTransposeRule::create(),
            ApplyProjectTransposeRule::create(),
            ApplyProjectSetTransposeRule::create(),
            ApplyTopNTransposeRule::create(),
            ApplyLimitTransposeRule::create(),
            ApplyJoinTransposeRule::create(),
            ApplyUnionTransposeRule::create(),
            ApplyOverWindowTransposeRule::create(),
            ApplyExpandTransposeRule::create(),
            ApplyHopWindowTransposeRule::create(),
            CrossJoinEliminateRule::create(),
            ApplyShareEliminateRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static TO_MULTI_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "To MultiJoin",
        vec![MergeMultiJoinRule::create()],
        ApplyOrder::TopDown,
    )
});

static LEFT_DEEP_JOIN_ORDERING: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Join Ordering".to_owned(),
        vec![LeftDeepTreeJoinOrderingRule::create()],
        ApplyOrder::TopDown,
    )
});

static BUSHY_TREE_JOIN_ORDERING: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Join Ordering".to_owned(),
        vec![BushyTreeJoinOrderingRule::create()],
        ApplyOrder::TopDown,
    )
});

static FILTER_WITH_NOW_TO_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Push down filter with now into a left semijoin",
        vec![
            SplitNowAndRule::create(),
            SplitNowOrRule::create(),
            FilterWithNowToJoinRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static PUSH_CALC_OF_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Push down the calculation of inputs of join's condition",
        vec![PushCalculationOfJoinRule::create()],
        ApplyOrder::TopDown,
    )
});

static CONVERT_DISTINCT_AGG_FOR_STREAM: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Convert Distinct Aggregation",
        vec![UnionToDistinctRule::create(), DistinctAggRule::create(true)],
        ApplyOrder::TopDown,
    )
});

static CONVERT_DISTINCT_AGG_FOR_BATCH: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Convert Distinct Aggregation",
        vec![
            UnionToDistinctRule::create(),
            DistinctAggRule::create(false),
        ],
        ApplyOrder::TopDown,
    )
});

static SIMPLIFY_AGG: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Simplify Aggregation",
        vec![
            AggGroupBySimplifyRule::create(),
            AggCallMergeRule::create(),
            UnifyFirstLastValueRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static JOIN_COMMUTE: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Join Commute".to_owned(),
        vec![JoinCommuteRule::create()],
        ApplyOrder::TopDown,
    )
});

static CONSTANT_OUTPUT_REMOVE: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Constant Output Operator Remove",
        vec![EmptyAggRemoveRule::create()],
        ApplyOrder::TopDown,
    )
});

static PROJECT_REMOVE: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Project Remove",
        vec![
            // merge should be applied before eliminate
            ProjectMergeRule::create(),
            ProjectEliminateRule::create(),
            TrivialProjectToValuesRule::create(),
            UnionInputValuesMergeRule::create(),
            JoinProjectTransposeRule::create(),
            // project-join merge should be applied after merge
            // eliminate and to values
            ProjectJoinMergeRule::create(),
            AggProjectMergeRule::create(),
        ],
        ApplyOrder::BottomUp,
    )
});

static SPLIT_OVER_WINDOW: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Split Over Window",
        vec![OverWindowSplitRule::create()],
        ApplyOrder::TopDown,
    )
});

// the `OverWindowToTopNRule` need to match the pattern of Proj-Filter-OverWindow so it is
// 1. conflict with `ProjectJoinMergeRule`, `AggProjectMergeRule` or other rules
// 2. should be after merge the multiple projects
static CONVERT_OVER_WINDOW: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Convert Over Window",
        vec![
            ProjectMergeRule::create(),
            ProjectEliminateRule::create(),
            TrivialProjectToValuesRule::create(),
            UnionInputValuesMergeRule::create(),
            OverWindowToAggAndJoinRule::create(),
            OverWindowToTopNRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

// DataFusion cannot apply `OverWindowToTopNRule`
static CONVERT_OVER_WINDOW_FOR_BATCH: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Convert Over Window",
        vec![
            ProjectMergeRule::create(),
            ProjectEliminateRule::create(),
            TrivialProjectToValuesRule::create(),
            UnionInputValuesMergeRule::create(),
            OverWindowToAggAndJoinRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static MERGE_OVER_WINDOW: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Merge Over Window",
        vec![OverWindowMergeRule::create()],
        ApplyOrder::TopDown,
    )
});

static REWRITE_LIKE_EXPR: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Rewrite Like Expr",
        vec![RewriteLikeExprRule::create()],
        ApplyOrder::TopDown,
    )
});

static TOP_N_AGG_ON_INDEX: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "TopN/SimpleAgg on Index",
        vec![
            TopNProjectTransposeRule::create(),
            TopNOnIndexRule::create(),
            MinMaxOnIndexRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static PROJECT_TOP_N_TRANSPOSE: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Project TopN Transpose",
        vec![ProjectTopNTransposeRule::create()],
        ApplyOrder::TopDown,
    )
});

static ALWAYS_FALSE_FILTER: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Void always-false filter's downstream",
        vec![AlwaysFalseFilterRule::create()],
        ApplyOrder::TopDown,
    )
});

static LIMIT_PUSH_DOWN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Push Down Limit",
        vec![LimitPushDownRule::create()],
        ApplyOrder::TopDown,
    )
});

static PULL_UP_HOP: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Pull Up Hop",
        vec![PullUpHopRule::create()],
        ApplyOrder::BottomUp,
    )
});

static SET_OPERATION_TO_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Set Operation To Join",
        vec![
            IntersectToSemiJoinRule::create(),
            ExceptToAntiJoinRule::create(),
        ],
        ApplyOrder::BottomUp,
    )
});

static GROUPING_SETS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Grouping Sets",
        vec![
            GroupingSetsToExpandRule::create(),
            ExpandToProjectRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static COMMON_SUB_EXPR_EXTRACT: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Common Sub Expression Extract",
        vec![CommonSubExprExtractRule::create()],
        ApplyOrder::TopDown,
    )
});

static LOGICAL_FILTER_EXPRESSION_SIMPLIFY: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Logical Filter Expression Simplify",
        vec![LogicalFilterExpressionSimplifyRule::create()],
        ApplyOrder::TopDown,
    )
});

static REWRITE_SOURCE_FOR_BATCH: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Rewrite Source For Batch",
        vec![SourceToKafkaScanRule::create()],
        ApplyOrder::TopDown,
    )
});

static MATERIALIZE_ICEBERG_SCAN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Materialize Iceberg Scan",
        vec![
            // When storage mode is auto, may rewrite Iceberg intermediate scan to Hummock scan based on statistics.
            IcebergEngineStorageSelectionRule::create(),
            // This converts LogicalIcebergIntermediateScan to LogicalIcebergScan with anti-joins
            // for delete files.
            IcebergIntermediateScanRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static ICEBERG_COUNT_STAR: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Iceberg Count Star Optimization",
        vec![IcebergCountStarRule::create()],
        ApplyOrder::BottomUp,
    )
});

static TOP_N_TO_VECTOR_SEARCH: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "TopN to Vector Search",
        vec![TopNToVectorSearchRule::create()],
        ApplyOrder::BottomUp,
    )
});

static CORRELATED_TOP_N_TO_VECTOR_SEARCH_FOR_BATCH: LazyLock<OptimizationStage> =
    LazyLock::new(|| {
        OptimizationStage::new(
            "Correlated TopN to Vector Search",
            vec![CorrelatedTopNToVectorSearchRule::create(true)],
            ApplyOrder::BottomUp,
        )
    });

static CORRELATED_TOP_N_TO_VECTOR_SEARCH_FOR_STREAM: LazyLock<OptimizationStage> =
    LazyLock::new(|| {
        OptimizationStage::new(
            "Correlated TopN to Vector Search",
            vec![CorrelatedTopNToVectorSearchRule::create(false)],
            ApplyOrder::BottomUp,
        )
    });

static BATCH_MV_SELECTION: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Batch Mv Selection",
        vec![MvSelectionRule::create()],
        ApplyOrder::TopDown,
    )
});

impl LogicalOptimizer {
    pub fn predicate_pushdown(
        plan: LogicalPlanRef,
        explain_trace: bool,
        ctx: &OptimizerContextRef,
    ) -> LogicalPlanRef {
        // Go through the trait method instead of `PredicatePushdownContext::run` so that the
        // top-level `check_equivalent_plan` debug check covers the whole pass.
        let mut pushdown_ctx = PredicatePushdownContext::new(plan.clone());
        let plan = plan.predicate_pushdown(Condition::true_cond(), &mut pushdown_ctx);
        if explain_trace {
            ctx.trace("Predicate Push Down:");
            ctx.trace(plan.explain_to_string());
        }
        plan
    }

    pub fn subquery_unnesting(
        mut plan: LogicalPlanRef,
        enable_share_plan: bool,
        explain_trace: bool,
        ctx: &OptimizerContextRef,
    ) -> Result<LogicalPlanRef> {
        // Bail our if no apply operators.
        if !has_logical_apply(plan.clone()) {
            return Ok(plan);
        }
        // Simple Unnesting.
        plan = plan.optimize_by_rules(&SIMPLE_UNNESTING)?;
        debug_assert!(!HasMaxOneRowApply().visit(plan.clone()));
        // Predicate push down before translate apply, because we need to calculate the domain
        // and predicate push down can reduce the size of domain.
        plan = Self::predicate_pushdown(plan, explain_trace, ctx);
        // In order to unnest values with correlated input ref, we need to extract project first.
        plan = plan.optimize_by_rules(&VALUES_EXTRACT_PROJECT)?;
        // General Unnesting.
        // Translate Apply, push Apply down the plan and finally replace Apply with regular inner
        // join.
        plan = if enable_share_plan {
            plan.optimize_by_rules(&GENERAL_UNNESTING_TRANS_APPLY_WITH_SHARE)?
        } else {
            plan.optimize_by_rules(&GENERAL_UNNESTING_TRANS_APPLY_WITHOUT_SHARE)?
        };
        plan = plan.optimize_by_rules_until_fix_point(&GENERAL_UNNESTING_PUSH_DOWN_APPLY)?;

        // Check if all `Apply`s are eliminated and the subquery is unnested.
        plan.check_apply_elimination()?;

        Ok(plan)
    }

    pub fn column_pruning(
        mut plan: LogicalPlanRef,
        explain_trace: bool,
        ctx: &OptimizerContextRef,
    ) -> LogicalPlanRef {
        let required_cols = (0..plan.schema().len()).collect_vec();
        // Go through the trait method instead of `ColumnPruningContext::run` so that the
        // top-level `check_equivalent_plan` debug check covers the whole pass.
        let mut column_pruning_ctx = ColumnPruningContext::new(plan.clone());
        plan = plan.prune_col(&required_cols, &mut column_pruning_ctx);
        // Column pruning may introduce additional projects, and filter can be pushed again.
        if explain_trace {
            ctx.trace("Prune Columns:");
            ctx.trace(plan.explain_to_string());
        }
        plan
    }

    pub fn inline_now_proc_time(plan: LogicalPlanRef, ctx: &OptimizerContextRef) -> LogicalPlanRef {
        // If now() and proctime() are not found, bail out.
        let mut v = NowProcTimeFinder::default();
        plan.visit_exprs_recursive(&mut v);
        if !v.has() {
            return plan;
        }

        let mut v = ctx.session_ctx().pinned_snapshot().inline_now_proc_time();

        let plan = plan.rewrite_exprs_recursive(&mut v);

        if ctx.is_explain_trace() {
            ctx.trace("Inline Now and ProcTime:");
            ctx.trace(plan.explain_to_string());
        }
        plan
    }

    pub fn gen_optimized_logical_plan_for_stream(
        mut plan: LogicalPlanRef,
    ) -> Result<LogicalPlanRef> {
        let ctx = plan.ctx();
        let explain_trace = ctx.is_explain_trace();

        if explain_trace {
            ctx.trace("Begin:");
            ctx.trace(plan.explain_to_string());
        }

        // Convert grouping sets at first because other agg rule can't handle grouping sets.
        plan = plan.optimize_by_rules(&GROUPING_SETS)?;
        // Remove nodes with constant output.
        plan = plan.optimize_by_rules(&CONSTANT_OUTPUT_REMOVE)?;
        // Remove project to make common sub-plan sharing easier.
        plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;

        // If share plan is disable, we need to remove all the share operator generated by the
        // binder, e.g. CTE and View. However, we still need to share source to ensure self
        // source join can return correct result.
        let enable_share_plan = ctx.session_ctx().config().enable_share_plan();
        if enable_share_plan {
            // Common sub-plan sharing.
            plan = plan.common_subplan_sharing();
            plan = plan.prune_share();
            if explain_trace {
                ctx.trace("Common Sub-plan Sharing:");
                ctx.trace(plan.explain_to_string());
            }
        } else {
            plan = plan.optimize_by_rules(&DAG_TO_TREE)?;

            // Replace source to share source.
            // Perform share source at the beginning so that we can benefit from predicate pushdown
            // and column pruning for the share operator.
            plan = ShareSourceRewriter::share_source(plan);
            if explain_trace {
                ctx.trace("Share Source:");
                ctx.trace(plan.explain_to_string());
            }
        }
        plan = plan.optimize_by_rules(&SET_OPERATION_MERGE)?;
        plan = plan.optimize_by_rules(&SET_OPERATION_TO_JOIN)?;
        // Convert `generate_series` ends with `now()` to a `Now` source. Only for streaming mode.
        // Should be applied before converting table function to project set.
        plan = plan.optimize_by_rules(&STREAM_GENERATE_SERIES_WITH_NOW)?;
        // In order to unnest a table function, we need to convert it into a `project_set` first.
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_CONVERT)?;

        plan = plan.optimize_by_rules(&CORRELATED_TOP_N_TO_VECTOR_SEARCH_FOR_STREAM)?;

        plan = Self::subquery_unnesting(plan, enable_share_plan, explain_trace, &ctx)?;
        if has_logical_max_one_row(plan.clone()) {
            // `MaxOneRow` is currently only used for the runtime check of
            // scalar subqueries, while it's not supported in streaming mode, so
            // we raise a precise error here.
            bail!("Scalar subquery might produce more than one row.");
        }

        // Same to batch plan optimization, this rule shall be applied before
        // predicate push down
        plan = plan.optimize_by_rules(&LOGICAL_FILTER_EXPRESSION_SIMPLIFY)?;

        // Predicate Push-down
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        if plan.ctx().session_ctx().config().enable_join_ordering() {
            // Merge inner joins and intermediate filters into multijoin
            // This rule assumes that filters have already been pushed down near to
            // their relevant joins.
            plan = plan.optimize_by_rules(&TO_MULTI_JOIN)?;

            // Reorder multijoin into join tree.
            if plan
                .ctx()
                .session_ctx()
                .config()
                .streaming_enable_bushy_join()
            {
                plan = plan.optimize_by_rules(&BUSHY_TREE_JOIN_ORDERING)?;
            } else {
                plan = plan.optimize_by_rules(&LEFT_DEEP_JOIN_ORDERING)?;
            }
        }

        // Predicate Push-down: apply filter pushdown rules again since we pullup all join
        // conditions into a filter above the multijoin.
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        // For stream, push down predicates with now into a left-semi join
        plan = plan.optimize_by_rules(&FILTER_WITH_NOW_TO_JOIN)?;

        // Push down the calculation of inputs of join's condition.
        plan = plan.optimize_by_rules(&PUSH_CALC_OF_JOIN)?;

        plan = plan.optimize_by_rules(&SPLIT_OVER_WINDOW)?;
        // Must push down predicates again after split over window so that OverWindow can be
        // optimized to TopN.
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
        plan = plan.optimize_by_rules(&CONVERT_OVER_WINDOW)?;
        plan = plan.optimize_by_rules(&MERGE_OVER_WINDOW)?;

        let force_split_distinct_agg = ctx.session_ctx().config().force_split_distinct_agg();
        // TODO: better naming of the OptimizationStage
        // Convert distinct aggregates.
        plan = if force_split_distinct_agg {
            plan.optimize_by_rules(&CONVERT_DISTINCT_AGG_FOR_BATCH)?
        } else {
            plan.optimize_by_rules(&CONVERT_DISTINCT_AGG_FOR_STREAM)?
        };

        plan = plan.optimize_by_rules(&SIMPLIFY_AGG)?;

        plan = plan.optimize_by_rules(&JOIN_COMMUTE)?;

        // Do a final column pruning and predicate pushing down to clean up the plan.
        plan = Self::column_pruning(plan, explain_trace, &ctx);
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        plan = plan.optimize_by_rules(&CONSTANT_OUTPUT_REMOVE)?;
        plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;

        plan = plan.optimize_by_rules(&COMMON_SUB_EXPR_EXTRACT)?;

        #[cfg(debug_assertions)]
        InputRefValidator.validate(plan.clone());

        ctx.may_store_explain_logical(&plan);

        Ok(plan)
    }

    pub fn gen_optimized_logical_plan_for_batch(
        mut plan: LogicalPlanRef,
    ) -> Result<LogicalPlanRef> {
        let ctx = plan.ctx();
        let explain_trace = ctx.is_explain_trace();

        if explain_trace {
            ctx.trace("Begin:");
            ctx.trace(plan.explain_to_string());
        }

        if ctx.session_ctx().config().enable_mv_selection() {
            let query_relations =
                RelationCollectorVisitor::collect_with(HashSet::new(), plan.clone());
            Self::register_batch_mview_candidates(ctx.session_ctx(), &ctx, &query_relations);
            plan = plan.optimize_by_rules(&BATCH_MV_SELECTION)?;
        }

        // Inline `NOW()` and `PROCTIME()`, only for batch queries.
        plan = Self::inline_now_proc_time(plan, &ctx);

        // Convert the dag back to the tree, because we don't support DAG plan for batch.
        plan = plan.optimize_by_rules(&DAG_TO_TREE)?;

        plan = plan.optimize_by_rules(&REWRITE_SOURCE_FOR_BATCH)?;
        plan = plan.optimize_by_rules(&GROUPING_SETS)?;
        plan = plan.optimize_by_rules(&REWRITE_LIKE_EXPR)?;
        plan = plan.optimize_by_rules(&SET_OPERATION_MERGE)?;
        plan = plan.optimize_by_rules(&SET_OPERATION_TO_JOIN)?;
        plan = plan.optimize_by_rules(&ALWAYS_FALSE_FILTER)?;
        // Table function should be converted into `file_scan` before `project_set`.
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_TO_FILE_SCAN)?;
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_TO_POSTGRES_QUERY)?;
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_TO_MYSQL_QUERY)?;
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_TO_MSSQL_QUERY)?;
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_TO_INTERNAL_BACKFILL_PROGRESS)?;
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_TO_INTERNAL_GET_CHANNEL_DELTA_STATS)?;
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_TO_INTERNAL_SOURCE_BACKFILL_PROGRESS)?;
        // In order to unnest a table function, we need to convert it into a `project_set` first.
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_CONVERT)?;

        plan = plan.optimize_by_rules(&CORRELATED_TOP_N_TO_VECTOR_SEARCH_FOR_BATCH)?;

        plan = Self::subquery_unnesting(plan, false, explain_trace, &ctx)?;

        // Filter simplification must be applied before predicate push-down
        // otherwise the filter for some nodes (e.g., `LogicalScan`)
        // may not be properly applied.
        plan = plan.optimize_by_rules(&LOGICAL_FILTER_EXPRESSION_SIMPLIFY)?;

        // Predicate Push-down
        let mut last_total_rule_applied_before_predicate_pushdown = ctx.total_rule_applied();
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        if plan.ctx().session_ctx().config().enable_join_ordering() {
            // Merge inner joins and intermediate filters into multijoin
            // This rule assumes that filters have already been pushed down near to
            // their relevant joins.
            plan = plan.optimize_by_rules(&TO_MULTI_JOIN)?;

            // Reorder multijoin into left-deep join tree.
            plan = plan.optimize_by_rules(&LEFT_DEEP_JOIN_ORDERING)?;
        }

        // Predicate Push-down: apply filter pushdown rules again since we pullup all join
        // conditions into a filter above the multijoin.
        if last_total_rule_applied_before_predicate_pushdown != ctx.total_rule_applied() {
            last_total_rule_applied_before_predicate_pushdown = ctx.total_rule_applied();
            plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
        }

        // Push down the calculation of inputs of join's condition.
        plan = plan.optimize_by_rules(&PUSH_CALC_OF_JOIN)?;

        plan = plan.optimize_by_rules(&SPLIT_OVER_WINDOW)?;
        // Must push down predicates again after split over window so that OverWindow can be
        // optimized to TopN.
        if last_total_rule_applied_before_predicate_pushdown != ctx.total_rule_applied() {
            last_total_rule_applied_before_predicate_pushdown = ctx.total_rule_applied();
            plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
        }
        plan = plan.optimize_by_rules(&CONVERT_OVER_WINDOW_FOR_BATCH)?;
        plan = plan.optimize_by_rules(&MERGE_OVER_WINDOW)?;

        // Convert distinct aggregates.
        plan = plan.optimize_by_rules(&CONVERT_DISTINCT_AGG_FOR_BATCH)?;

        plan = plan.optimize_by_rules(&SIMPLIFY_AGG)?;

        plan = plan.optimize_by_rules(&JOIN_COMMUTE)?;

        plan = plan.optimize_by_rules(&TOP_N_TO_VECTOR_SEARCH)?;

        // Do a final column pruning and predicate pushing down to clean up the plan.
        plan = Self::column_pruning(plan, explain_trace, &ctx);
        if last_total_rule_applied_before_predicate_pushdown != ctx.total_rule_applied() {
            let _ = ctx.total_rule_applied();
            plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
        }

        // Materialize Iceberg intermediate scans after predicate pushdown and column pruning.
        plan = plan.optimize_by_rules(&MATERIALIZE_ICEBERG_SCAN)?;

        plan = plan.optimize_by_rules(&CONSTANT_OUTPUT_REMOVE)?;
        plan = plan.optimize_by_rules(&PROJECT_REMOVE)?;

        plan = plan.optimize_by_rules(&COMMON_SUB_EXPR_EXTRACT)?;

        // This need to be apply after PROJECT_REMOVE to ensure there is no projection between agg and iceberg scan.
        plan = plan.optimize_by_rules(&ICEBERG_COUNT_STAR)?;

        plan = plan.optimize_by_rules(&PULL_UP_HOP)?;

        plan = plan.optimize_by_rules(&TOP_N_AGG_ON_INDEX)?;

        plan = plan.optimize_by_rules(&PROJECT_TOP_N_TRANSPOSE)?;

        plan = plan.optimize_by_rules(&LIMIT_PUSH_DOWN)?;

        plan = plan.optimize_by_rules(&DAG_TO_TREE)?;

        #[cfg(debug_assertions)]
        InputRefValidator.validate(plan.clone());

        ctx.may_store_explain_logical(&plan);

        Ok(plan)
    }

    fn register_batch_mview_candidates(
        session: &SessionImpl,
        context: &OptimizerContextRef,
        query_relations: &HashSet<ObjectId>,
    ) {
        let catalog_reader = session.env().catalog_reader().read_guard();
        let user_reader = session.env().user_info_reader().read_guard();
        let Some(current_user) = user_reader.get_user_by_name(&session.user_name()) else {
            return;
        };
        let mut mv_dependencies: HashMap<ObjectId, HashSet<ObjectId>> = HashMap::new();
        let mut mviews_with_source_dependency: HashSet<ObjectId> = HashSet::new();
        for dep in catalog_reader.iter_object_dependencies() {
            mv_dependencies
                .entry(dep.object_id)
                .or_default()
                .insert(dep.referenced_object_id);
            if dep.referenced_object_type == PbObjectType::Source {
                mviews_with_source_dependency.insert(dep.object_id);
            }
        }
        let db_name = session.database();
        let Ok(schemas) = catalog_reader.iter_schemas(&db_name) else {
            return;
        };

        for schema in schemas {
            for mv in schema.iter_created_mvs_with_acl(current_user) {
                let mv_object_id = mv.id().as_object_id();
                if mviews_with_source_dependency.contains(&mv_object_id) {
                    continue;
                }
                let is_subset = mv_dependencies
                    .get(&mv_object_id)
                    .is_some_and(|deps| deps.is_subset(query_relations));
                if !is_subset {
                    continue;
                }
                let Ok(stmt) = mv.create_sql_ast() else {
                    continue;
                };
                let Statement::CreateView {
                    materialized: true,
                    query,
                    ..
                } = stmt
                else {
                    continue;
                };
                let mut binder = Binder::new_for_batch(session);
                let Ok(bound_query) = binder.bind_query(&query) else {
                    continue;
                };
                let mut planner = Planner::new_for_batch_dql(context.clone());
                let Ok(plan_root) = planner.plan_query(bound_query) else {
                    continue;
                };
                context.add_batch_mview_candidate(mv.clone(), plan_root.plan.clone());
            }
        }
    }
}
