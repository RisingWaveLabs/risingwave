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

//! Define all [`Rule`]

use std::convert::Infallible;
use std::ops::FromResidual;

use thiserror_ext::AsReport;

use super::PlanRef;
use crate::error::RwError;

/// Result when applying a [`Rule`] to a `PlanNode`
pub enum ApplyResult<T> {
    /// Successfully applied the rule and returned a new plan.
    Ok(T),
    /// The current rule is not applicable to the input.
    /// The optimizer may try another rule.
    NotApplicable,
    /// An unrecoverable error occurred while applying the rule.
    /// The optimizer should stop applying other rules and report the error to the user.
    Err(RwError),
}

impl<T> ApplyResult<T> {
    /// Unwrap the result, panicking if it's not `Ok`.
    pub fn unwrap(self) -> T {
        match self {
            ApplyResult::Ok(plan) => plan,
            ApplyResult::NotApplicable => panic!("unwrap ApplyResult::NotApplicable"),
            ApplyResult::Err(e) => panic!("unwrap ApplyResult::Err, error: {:?}", e.as_report()),
        }
    }
}

/// Allow calling `?` on an `Option` in a function returning `ApplyResult`.
impl<T> FromResidual<Option<Infallible>> for ApplyResult<T> {
    fn from_residual(residual: Option<Infallible>) -> Self {
        match residual {
            Some(i) => match i {},
            None => Self::NotApplicable,
        }
    }
}

/// Allow calling `?` on a `Result` in a function returning `ApplyResult`.
impl<T, E> FromResidual<Result<Infallible, E>> for ApplyResult<T>
where
    E: Into<RwError>,
{
    fn from_residual(residual: Result<Infallible, E>) -> Self {
        match residual {
            Ok(i) => match i {},
            Err(e) => Self::Err(e.into()),
        }
    }
}

/// An one-to-one transform for the `PlanNode`.
///
/// It's a convenient trait to implement [`FallibleRule`], thus made available only within this module.
trait InfallibleRule<C: ConventionMarker>: Send + Sync + Description {
    /// Apply the rule to the plan node.
    ///
    /// - Returns `Some` if the apply is successful.
    /// - Returns `None` if it's not applicable. The optimizer may try other rules.
    fn apply(&self, plan: PlanRef<C>) -> Option<PlanRef<C>>;
}

use InfallibleRule as Rule;

/// An one-to-one transform for the `PlanNode` that may return an
/// unrecoverable error that stops further optimization.
///
/// An [`InfallibleRule`] is always a [`FallibleRule`].
pub trait FallibleRule<C: ConventionMarker>: Send + Sync + Description {
    /// Apply the rule to the plan node, which may return an unrecoverable error.
    ///
    /// - Returns `ApplyResult::Ok` if the apply is successful.
    /// - Returns `ApplyResult::NotApplicable` if it's not applicable. The optimizer may try other rules.
    /// - Returns `ApplyResult::Err` if an unrecoverable error occurred. The optimizer should stop applying
    ///   other rules and report the error to the user.
    fn apply(&self, plan: PlanRef<C>) -> ApplyResult<PlanRef<C>>;
}

impl<C: ConventionMarker, R> FallibleRule<C> for R
where
    R: InfallibleRule<C>,
{
    fn apply(&self, plan: PlanRef<C>) -> ApplyResult<PlanRef<C>> {
        match InfallibleRule::apply(self, plan) {
            Some(plan) => ApplyResult::Ok(plan),
            None => ApplyResult::NotApplicable,
        }
    }
}

pub trait Description {
    fn description(&self) -> &str;
}

pub(super) type BoxedRule<C> = Box<dyn FallibleRule<C>>;

mod correlated_expr_rewriter;
mod logical_filter_expression_simplify_rule;
pub use logical_filter_expression_simplify_rule::*;
mod mv_selection_rule;
pub use mv_selection_rule::*;
mod over_window_merge_rule;
pub use over_window_merge_rule::*;
mod project_join_merge_rule;
pub use project_join_merge_rule::*;
mod project_eliminate_rule;
pub use project_eliminate_rule::*;
mod project_merge_rule;
pub use project_merge_rule::*;
mod project_top_n_transpose_rule;
pub use project_top_n_transpose_rule::*;
mod top_n_project_transpose_rule;
pub use top_n_project_transpose_rule::*;
mod pull_up_correlated_predicate_rule;
pub use pull_up_correlated_predicate_rule::*;
mod pull_up_correlated_project_value_rule;
pub use pull_up_correlated_project_value_rule::*;
mod index_delta_join_rule;
pub use index_delta_join_rule::*;
mod left_deep_tree_join_ordering_rule;
pub use left_deep_tree_join_ordering_rule::*;
mod apply_agg_transpose_rule;
pub use apply_agg_transpose_rule::*;
mod apply_filter_transpose_rule;
pub use apply_filter_transpose_rule::*;
mod apply_project_transpose_rule;
pub use apply_project_transpose_rule::*;
mod apply_eliminate_rule;
pub use apply_eliminate_rule::*;
mod translate_apply_rule;
pub use translate_apply_rule::*;
mod merge_multijoin_rule;
pub use merge_multijoin_rule::*;
mod max_one_row_eliminate_rule;
pub use max_one_row_eliminate_rule::*;
mod apply_join_transpose_rule;
pub use apply_join_transpose_rule::*;
mod apply_to_join_rule;
pub use apply_to_join_rule::*;
mod distinct_agg_rule;
pub use distinct_agg_rule::*;
mod index_selection_rule;
pub use index_selection_rule::*;
mod streaming_index_selection_rule;
pub use streaming_index_selection_rule::StreamingIndexSelectionRule;
mod push_calculation_of_join_rule;
pub use push_calculation_of_join_rule::*;
mod join_commute_rule;
mod over_window_to_agg_and_join_rule;
pub use over_window_to_agg_and_join_rule::*;
mod over_window_split_rule;
pub use over_window_split_rule::*;
mod over_window_to_topn_rule;
pub use join_commute_rule::*;
pub use over_window_to_topn_rule::*;
mod union_to_distinct_rule;
pub use union_to_distinct_rule::*;
mod agg_project_merge_rule;
pub use agg_project_merge_rule::*;
mod union_merge_rule;
pub use union_merge_rule::*;
mod dag_to_tree_rule;
pub use dag_to_tree_rule::*;
mod apply_share_eliminate_rule;
pub use apply_share_eliminate_rule::*;
mod top_n_on_index_rule;
pub use top_n_on_index_rule::*;
mod stream;
pub use stream::bushy_tree_join_ordering_rule::*;
pub use stream::filter_with_now_to_join_rule::*;
pub use stream::generate_series_with_now_rule::*;
pub use stream::separate_consecutive_join_rule::*;
pub use stream::split_now_and_rule::*;
pub use stream::split_now_or_rule::*;
pub use stream::stream_project_merge_rule::*;
mod trivial_project_to_values_rule;
pub use trivial_project_to_values_rule::*;
mod union_input_values_merge_rule;
pub use union_input_values_merge_rule::*;
mod rewrite_like_expr_rule;
pub use rewrite_like_expr_rule::*;
mod min_max_on_index_rule;
pub use min_max_on_index_rule::*;
mod always_false_filter_rule;
pub use always_false_filter_rule::*;
mod join_project_transpose_rule;
pub use join_project_transpose_rule::*;
mod limit_push_down_rule;
pub use limit_push_down_rule::*;
mod pull_up_hop_rule;
pub use pull_up_hop_rule::*;
mod apply_offset_rewriter;
use apply_offset_rewriter::ApplyOffsetRewriter;
mod intersect_to_semi_join_rule;
pub use intersect_to_semi_join_rule::*;
mod except_to_anti_join_rule;
pub use except_to_anti_join_rule::*;
mod intersect_merge_rule;
pub use intersect_merge_rule::*;
mod except_merge_rule;
pub use except_merge_rule::*;
mod apply_union_transpose_rule;
pub use apply_union_transpose_rule::*;
mod apply_dedup_transpose_rule;
pub use apply_dedup_transpose_rule::*;
mod project_join_separate_rule;
pub use project_join_separate_rule::*;
mod grouping_sets_to_expand_rule;
pub use grouping_sets_to_expand_rule::*;
mod apply_project_set_transpose_rule;
pub use apply_project_set_transpose_rule::*;
mod apply_table_function_to_project_set_rule;
pub use apply_table_function_to_project_set_rule::*;
mod cross_join_eliminate_rule;
pub use cross_join_eliminate_rule::*;
mod table_function_to_project_set_rule;

pub use table_function_to_project_set_rule::*;
mod apply_topn_transpose_rule;
pub use apply_topn_transpose_rule::*;
mod apply_limit_transpose_rule;
pub use apply_limit_transpose_rule::*;
mod batch;
pub use batch::batch_project_merge_rule::*;
mod common_sub_expr_extract_rule;
pub use common_sub_expr_extract_rule::*;
mod apply_over_window_transpose_rule;
pub use apply_over_window_transpose_rule::*;
mod apply_expand_transpose_rule;
pub use apply_expand_transpose_rule::*;
mod expand_to_project_rule;
pub use expand_to_project_rule::*;
mod agg_group_by_simplify_rule;
pub use agg_group_by_simplify_rule::*;
mod apply_hop_window_transpose_rule;
pub use apply_hop_window_transpose_rule::*;
mod agg_call_merge_rule;
pub use agg_call_merge_rule::*;
mod unify_first_last_value_rule;
pub use unify_first_last_value_rule::*;
mod empty_agg_remove_rule;
pub use empty_agg_remove_rule::*;
mod add_logstore_rule;
mod correlated_topn_to_vector_search;
mod iceberg_count_star_rule;
mod iceberg_engine_storage_selection_rule;
mod iceberg_intermediate_scan_rule;
mod pull_up_correlated_predicate_agg_rule;
mod source_to_kafka_scan_rule;
mod table_function_to_file_scan_rule;
mod table_function_to_internal_backfill_progress;
mod table_function_to_internal_get_channel_delta_stats;
mod table_function_to_internal_source_backfill_progress;
mod table_function_to_mssql_query_rule;
mod table_function_to_mysql_query_rule;
mod table_function_to_postgres_query_rule;
mod top_n_to_vector_search_rule;
mod values_extract_project_rule;
pub use add_logstore_rule::*;
pub use batch::batch_push_limit_to_scan_rule::*;
pub use correlated_topn_to_vector_search::*;
pub use iceberg_count_star_rule::IcebergCountStarRule;
pub use iceberg_engine_storage_selection_rule::*;
pub use iceberg_intermediate_scan_rule::*;
pub use pull_up_correlated_predicate_agg_rule::*;
pub use source_to_kafka_scan_rule::*;
pub use table_function_to_file_scan_rule::*;
pub use table_function_to_internal_backfill_progress::*;
pub use table_function_to_internal_get_channel_delta_stats::*;
pub use table_function_to_internal_source_backfill_progress::*;
pub use table_function_to_mssql_query_rule::*;
pub use table_function_to_mysql_query_rule::*;
pub use table_function_to_postgres_query_rule::*;
pub use top_n_to_vector_search_rule::*;
pub use values_extract_project_rule::*;

use crate::optimizer::plan_node::ConventionMarker;

#[macro_export]
macro_rules! for_all_rules {
    ($macro:ident) => {
        $macro! {
              { ApplyAggTransposeRule }
            , { ApplyFilterTransposeRule }
            , { ApplyProjectTransposeRule }
            , { ApplyProjectSetTransposeRule }
            , { ApplyTableFunctionToProjectSetRule }
            , { ApplyEliminateRule }
            , { ApplyJoinTransposeRule }
            , { ApplyShareEliminateRule }
            , { ApplyToJoinRule }
            , { MaxOneRowEliminateRule }
            , { DistinctAggRule }
            , { IndexDeltaJoinRule }
            , { MergeMultiJoinRule }
            , { ProjectEliminateRule }
            , { ProjectJoinMergeRule }
            , { ProjectMergeRule }
            , { PullUpCorrelatedPredicateRule }
            , { PullUpCorrelatedProjectValueRule }
            , { LeftDeepTreeJoinOrderingRule }
            , { TranslateApplyRule }
            , { PushCalculationOfJoinRule }
            , { IndexSelectionRule }
            , { OverWindowToTopNRule }
            , { OverWindowToAggAndJoinRule }
            , { OverWindowSplitRule }
            , { OverWindowMergeRule }
            , { JoinCommuteRule }
            , { UnionToDistinctRule }
            , { AggProjectMergeRule }
            , { UnionMergeRule }
            , { DagToTreeRule }
            , { SplitNowAndRule }
            , { SplitNowOrRule }
            , { FilterWithNowToJoinRule }
            , { GenerateSeriesWithNowRule }
            , { ProjectTopNTransposeRule }
            , { TopNProjectTransposeRule }
            , { TopNOnIndexRule }
            , { TrivialProjectToValuesRule }
            , { UnionInputValuesMergeRule }
            , { RewriteLikeExprRule }
            , { MinMaxOnIndexRule }
            , { AlwaysFalseFilterRule }
            , { BushyTreeJoinOrderingRule }
            , { StreamProjectMergeRule }
            , { SeparateConsecutiveJoinRule }
            , { LogicalFilterExpressionSimplifyRule }
            , { JoinProjectTransposeRule }
            , { LimitPushDownRule }
            , { PullUpHopRule }
            , { IntersectToSemiJoinRule }
            , { ExceptToAntiJoinRule }
            , { IntersectMergeRule }
            , { ExceptMergeRule }
            , { ApplyUnionTransposeRule }
            , { ApplyDedupTransposeRule }
            , { ProjectJoinSeparateRule }
            , { GroupingSetsToExpandRule }
            , { CrossJoinEliminateRule }
            , { ApplyTopNTransposeRule }
            , { TableFunctionToProjectSetRule }
            , { TableFunctionToFileScanRule }
            , { TableFunctionToPostgresQueryRule }
            , { TableFunctionToMySqlQueryRule }
            , { TableFunctionToMssqlQueryRule }
            , { TableFunctionToInternalBackfillProgressRule }
            , { TableFunctionToInternalGetChannelDeltaStatsRule }
            , { TableFunctionToInternalSourceBackfillProgressRule }
            , { ApplyLimitTransposeRule }
            , { CommonSubExprExtractRule }
            , { BatchProjectMergeRule }
            , { ApplyOverWindowTransposeRule }
            , { ApplyExpandTransposeRule }
            , { ExpandToProjectRule }
            , { AggGroupBySimplifyRule }
            , { ApplyHopWindowTransposeRule }
            , { AggCallMergeRule }
            , { UnifyFirstLastValueRule }
            , { ValuesExtractProjectRule }
            , { BatchPushLimitToScanRule }
            , { PullUpCorrelatedPredicateAggRule }
            , { SourceToKafkaScanRule }
            , { IcebergEngineStorageSelectionRule }
            , { IcebergCountStarRule}
            , { IcebergIntermediateScanRule }
            , { AddLogstoreRule }
            , { EmptyAggRemoveRule }
            , { TopNToVectorSearchRule }
            , { CorrelatedTopNToVectorSearchRule }
            , { MvSelectionRule }
        }
    };
}

macro_rules! impl_description {
    ($( { $name:ident }),*) => {
        paste::paste!{
            $(impl Description for [<$name>] {
                fn description(&self) -> &str {
                    stringify!([<$name>])
                }
            })*
        }
    }
}

for_all_rules! {impl_description}

mod prelude {
    pub(super) use crate::optimizer::plan_node::{Logical, LogicalPlanRef as PlanRef};
    pub(super) use crate::optimizer::rule::Rule;

    pub(super) type BoxedRule = crate::optimizer::rule::BoxedRule<Logical>;
}
