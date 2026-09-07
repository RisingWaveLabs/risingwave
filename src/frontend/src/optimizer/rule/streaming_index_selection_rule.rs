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

//! Streaming index selection rule for backfill.
//!
//! Unlike the batch [`IndexSelectionRule`](IndexSelectionRule), this rule:
//! - Only considers **covering** indexes (no lookup join in streaming backfill)
//! - Expands multi-range predicates (IN, OR, range splits) into a `LogicalUnion`
//!   of `LogicalScan`s, each with a branch-local predicate reconstructed from the
//!   scan range for correct post-backfill routing

use std::ops::Bound;

use super::index_selection_rule::TableScanIoEstimator;
use super::prelude::PlanRef;
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef, Literal};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalScan, LogicalUnion};
use crate::optimizer::rule::IndexSelectionRule;
use crate::utils::Condition;

pub struct StreamingIndexSelectionRule;

impl StreamingIndexSelectionRule {
    /// Rewrites a `LogicalScan` for backfill.
    ///
    /// Applies two optimizations in order:
    /// 1. **Covering index selection**: picks the lowest-cost covering index
    /// 2. **Multi-range expansion**: splits predicates that produce multiple scan ranges
    ///    into a `LogicalUnion` of `LogicalScan`s, each with a complete branch-local
    ///    predicate reconstructed from the scan range
    ///
    /// Returns `Some(PlanRef)` which is either a `LogicalScan` (index only) or a
    /// `LogicalUnion` of `LogicalScan`s (multi-range expansion, possibly on an index).
    pub fn rewrite(logical_scan: &LogicalScan) -> Option<PlanRef> {
        // Step 1: Try covering index selection.
        let best_scan =
            Self::select_covering_index(logical_scan).unwrap_or_else(|| logical_scan.clone());

        // Step 2: Try multi-range expansion on the (possibly index-backed) scan.
        if let Some(union) = Self::try_expand_multi_range(&best_scan) {
            return Some(union);
        }

        // If we selected a better index (different from original), return it.
        if best_scan.table().id != logical_scan.table().id {
            return Some(best_scan.into());
        }

        None
    }

    /// Picks the lowest-cost covering index for a streaming scan.
    fn select_covering_index(logical_scan: &LogicalScan) -> Option<LogicalScan> {
        let indexes = logical_scan.table_indexes();
        if indexes.is_empty() {
            return None;
        }

        let rule = IndexSelectionRule {};
        let primary_table_row_size = TableScanIoEstimator::estimate_row_size(logical_scan);
        let primary_cost = std::cmp::min(
            rule.estimate_table_scan_cost(logical_scan, primary_table_row_size),
            rule.estimate_full_table_scan_cost(logical_scan, primary_table_row_size),
        );

        if primary_cost.primary_lookup {
            return None;
        }

        let mut best_scan: Option<LogicalScan> = None;
        let mut min_cost = primary_cost;

        for index in indexes {
            if let Some(index_scan) = logical_scan.to_index_scan_if_index_covered(index) {
                let index_cost = rule.estimate_table_scan_cost(
                    &index_scan,
                    TableScanIoEstimator::estimate_row_size(&index_scan),
                );
                if index_cost.le(&min_cost) {
                    min_cost = index_cost;
                    best_scan = Some(index_scan);
                }
            }
            // Skip non-covering indexes — no lookup join for backfill.
        }

        best_scan
    }

    /// Expands a predicate with multiple scan ranges into a `LogicalUnion` of `LogicalScan`s.
    ///
    /// Each branch gets a complete predicate reconstructed from the scan range's `eq_conds`
    /// and range bounds, plus residual conditions. This handles:
    /// - IN predicates: `a IN (1, 2, 3)` → 3 branches with `a = 1`, `a = 2`, `a = 3`
    /// - Multi-range: `a >= 1 AND a < 5 OR a >= 10 AND a < 20` → 2 branches with range bounds
    /// - Mixed: `eq_conds` prefix + range bound on next column
    ///
    /// Only applies when all scan ranges are non-trivial (selective).
    /// `split_to_scan_ranges` guarantees the ranges are non-overlapping.
    fn try_expand_multi_range(logical_scan: &LogicalScan) -> Option<PlanRef> {
        let (scan_ranges, residual) = logical_scan
            .predicate()
            .clone()
            .split_to_scan_ranges(
                logical_scan.table(),
                logical_scan
                    .base
                    .ctx()
                    .session_ctx()
                    .config()
                    .max_split_range_gap() as u64,
            )
            .ok()?;

        if scan_ranges.len() <= 1 || !scan_ranges.iter().all(|r| !r.is_full_table_scan()) {
            return None;
        }

        let pk = &logical_scan.table().pk;
        let table_cols = &logical_scan.table().columns;

        let branches: Vec<PlanRef> = scan_ranges
            .into_iter()
            .map(|range| -> Option<PlanRef> {
                let mut conjunctions = Vec::new();

                // 1. Build equality predicates from eq_conds (PK prefix).
                for (i, datum) in range.eq_conds.iter().enumerate() {
                    let table_col_idx = pk[i].column_index;
                    let col_type = table_cols[table_col_idx].data_type().clone();
                    let input_ref: ExprImpl = InputRef::new(table_col_idx, col_type.clone()).into();
                    let literal: ExprImpl = Literal::new(datum.clone(), col_type).into();
                    let eq = FunctionCall::new(ExprType::Equal, vec![input_ref, literal])
                        .ok()
                        .map(ExprImpl::from)?;
                    conjunctions.push(eq);
                }

                // 2. Build range bound predicates on the next PK column(s) after eq_conds.
                let range_col_start = range.eq_conds.len();
                Self::build_bound_expr(
                    &range.range.0,
                    range_col_start,
                    pk,
                    table_cols,
                    true, // is_lower
                    &mut conjunctions,
                )?;
                Self::build_bound_expr(
                    &range.range.1,
                    range_col_start,
                    pk,
                    table_cols,
                    false, // is_lower
                    &mut conjunctions,
                )?;

                // 3. Add residual conditions so each branch is self-contained.
                conjunctions.extend(residual.conjunctions.iter().cloned());
                let branch_predicate = Condition { conjunctions };
                Some(logical_scan.clone_with_predicate(branch_predicate).into())
            })
            .collect::<Option<_>>()?;

        Some(LogicalUnion::create(true, branches))
    }

    /// Builds comparison expression(s) from a scan range bound.
    ///
    /// For `Included(v)` lower bound → `col >= v`
    /// For `Excluded(v)` lower bound → `col > v`
    /// For `Included(v)` upper bound → `col <= v`
    /// For `Excluded(v)` upper bound → `col < v`
    /// For `Unbounded` → no expression added
    ///
    /// Returns `Some(())` on success, `None` if expression construction fails.
    fn build_bound_expr(
        bound: &Bound<Vec<risingwave_common::types::Datum>>,
        range_col_start: usize,
        pk: &[risingwave_common::util::sort_util::ColumnOrder],
        table_cols: &[risingwave_common::catalog::ColumnCatalog],
        is_lower: bool,
        conjunctions: &mut Vec<ExprImpl>,
    ) -> Option<()> {
        let datums = match bound {
            Bound::Unbounded => return Some(()),
            Bound::Included(d) => d,
            Bound::Excluded(d) => d,
        };
        let is_inclusive = matches!(bound, Bound::Included(_));

        for (j, datum) in datums.iter().enumerate() {
            let pk_idx = range_col_start + j;
            if pk_idx >= pk.len() {
                break;
            }
            let table_col_idx = pk[pk_idx].column_index;
            let col_type = table_cols[table_col_idx].data_type().clone();
            let input_ref: ExprImpl = InputRef::new(table_col_idx, col_type.clone()).into();
            let literal: ExprImpl = Literal::new(datum.clone(), col_type).into();

            // For multi-datum bounds, only the last datum gets the actual bound operator;
            // preceding datums are equalities (they form part of the composite prefix).
            let expr_type = if j < datums.len() - 1 {
                ExprType::Equal
            } else if is_lower {
                if is_inclusive {
                    ExprType::GreaterThanOrEqual
                } else {
                    ExprType::GreaterThan
                }
            } else if is_inclusive {
                ExprType::LessThanOrEqual
            } else {
                ExprType::LessThan
            };

            let expr = FunctionCall::new(expr_type, vec![input_ref, literal])
                .ok()
                .map(ExprImpl::from)?;
            conjunctions.push(expr);
        }
        Some(())
    }
}
