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

use std::collections::{HashMap, HashSet};

use super::*;
use crate::optimizer::plan_visitor::{ExprCorrelatedIdFinder, ShareParentCounter};
use crate::optimizer::{
    ExpressionSimplifyRewriter, LogicalPlanRef as PlanRef, PlanVisitor, ShareId,
};

/// The trait for predicate pushdown, only logical plan node will use it, though all plan node impl
/// it.
pub trait PredicatePushdown {
    /// Push predicate down for every logical plan node.
    ///
    /// There are three kinds of predicates:
    ///
    /// 1. those can't be pushed down. We just create a `LogicalFilter` for them above the current
    ///    `PlanNode`. i.e.,
    ///
    ///     ```ignore
    ///     LogicalFilter::create(self.clone().into(), predicate)
    ///     ```
    ///
    /// 2. those can be merged with current `PlanNode` (e.g., `LogicalJoin`). We just merge
    ///    the predicates with the `Condition` of it.
    ///
    /// 3. those can be pushed down. We pass them to current `PlanNode`'s input.
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef;
}

#[inline]
pub fn gen_filter_and_pushdown<T: PlanTreeNodeUnary<Logical> + LogicalPlanNode>(
    node: &T,
    filter_predicate: Condition,
    pushed_predicate: Condition,
    ctx: &mut PredicatePushdownContext,
) -> PlanRef {
    let new_input = node.input().predicate_pushdown(pushed_predicate, ctx);
    let new_node = node.clone_with_input(new_input);
    LogicalFilter::create(new_node.into(), filter_predicate)
}

fn merge_share_predicates(requirements: Vec<Condition>) -> Condition {
    let merged = requirements
        .into_iter()
        .map(|mut condition| Condition {
            conjunctions: condition
                .conjunctions
                .extract_if(.., |expr| {
                    // Temporal, impure and correlated predicates must remain above each
                    // parent and cannot participate in an OR below a share.
                    let mut finder = ExprCorrelatedIdFinder::default();
                    finder.visit_expr(expr);
                    expr.count_nows() == 0 && expr.is_pure() && !finder.has_correlated_input_ref()
                })
                .collect(),
        })
        .reduce(Condition::or)
        .expect("a shared plan must have at least one parent predicate");

    let mut rewriter = ExpressionSimplifyRewriter {};
    merged
        .conjunctions
        .into_iter()
        .fold(Condition::true_cond(), |condition, expr| {
            condition.and(Condition::with_expr(rewriter.rewrite_cond(expr)))
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PredicatePushdownPhase {
    Idle,
    Collect,
    Rewrite,
}

#[derive(Debug, Clone)]
struct SharePredicatePushdown {
    original_input: PlanRef,
    predicate: Condition,
}

#[derive(Debug, Clone)]
pub struct PredicatePushdownContext {
    pending_predicates: HashMap<ShareId, Vec<Condition>>,
    collected_shares: HashMap<ShareId, SharePredicatePushdown>,
    share_parent_counter: ShareParentCounter,
    rebuilt_shares: HashSet<ShareId>,
    skipped_shares: HashSet<ShareId>,
    phase: PredicatePushdownPhase,
}

impl PredicatePushdownContext {
    pub fn new(root: PlanRef) -> Self {
        let mut share_parent_counter = ShareParentCounter::default();
        share_parent_counter.visit(root);
        Self {
            pending_predicates: HashMap::new(),
            collected_shares: HashMap::new(),
            share_parent_counter,
            rebuilt_shares: HashSet::new(),
            skipped_shares: HashSet::new(),
            phase: PredicatePushdownPhase::Idle,
        }
    }

    pub(in crate::optimizer) fn is_running(&self) -> bool {
        self.phase != PredicatePushdownPhase::Idle
    }

    pub(in crate::optimizer) fn is_collecting(&self) -> bool {
        self.phase == PredicatePushdownPhase::Collect
    }

    pub(in crate::optimizer) fn get_parent_num(&self, share: &LogicalShare) -> usize {
        self.share_parent_counter.get_parent_num(share)
    }

    pub(in crate::optimizer) fn add_predicate(
        &mut self,
        share: &LogicalShare,
        predicate: Condition,
    ) -> Option<Condition> {
        let share_id = share.share_id();
        let parent_num = self.share_parent_counter.get_parent_num_by_id(share_id);
        let pending = self.pending_predicates.entry(share_id).or_default();
        pending.push(predicate);
        assert!(
            pending.len() <= parent_num,
            "share {share_id:?} received more predicates than parents"
        );
        if pending.len() != parent_num {
            return None;
        }

        let merged_predicate = merge_share_predicates(
            self.pending_predicates
                .remove(&share_id)
                .expect("share predicates must exist"),
        );
        self.collected_shares
            .try_insert(
                share_id,
                SharePredicatePushdown {
                    original_input: share.input(),
                    predicate: merged_predicate.clone(),
                },
            )
            .expect("predicates must be merged once per share");
        Some(merged_predicate)
    }

    /// Rebuilds one shared definition on first use. Nested shares recursively rebuild first, so
    /// the call stack provides the child-before-parent order without a separate dependency graph.
    pub(in crate::optimizer) fn ensure_share_rebuilt(&mut self, share: &LogicalShare) {
        let share_id = share.share_id();
        if self.rebuilt_shares.contains(&share_id) {
            return;
        }
        assert_eq!(self.phase, PredicatePushdownPhase::Rewrite);

        let Some(SharePredicatePushdown {
            original_input,
            predicate,
        }) = self.collected_shares.remove(&share_id)
        else {
            // The share was skipped during collection (see `run`): it never received a
            // predicate from every parent, so it must keep its original definition.
            // Parents re-create their filters above the share during the rewrite walk.
            assert!(
                self.skipped_shares.contains(&share_id),
                "share {share_id:?} has no collected predicates"
            );
            return;
        };
        let rebuilt_input = original_input.predicate_pushdown(predicate, self);
        share.ctx().update_logical_share(share_id, rebuilt_input);
        assert!(self.rebuilt_shares.insert(share_id));
    }

    pub(in crate::optimizer) fn run(&mut self, root: PlanRef, predicate: Condition) -> PlanRef {
        self.phase = PredicatePushdownPhase::Collect;
        let collected = root.predicate_pushdown_inner(predicate.clone(), self);
        // `ShareParentCounter` counts parents via the visitor walk, but the transformation
        // walk is not guaranteed to reach a share from every parent: some `PredicatePushdown`
        // impls (e.g. `LogicalOverWindow`, `LogicalGapFill`) legitimately stop recursing into
        // their inputs. A predicate merged from a subset of parents must not be pushed below
        // the share — it would drop rows required by the parents that never contributed — so
        // such shares are skipped and keep their original definition, like on the base plan.
        //
        // Every non-leaf `PredicatePushdown` impl is expected to recurse into all of its
        // inputs, pushing `Condition::true_cond()` when nothing can be pushed (like
        // `LogicalExpand`), so this should never happen: fail loudly in debug builds and
        // degrade to skipping the share in release builds.
        debug_assert!(
            self.pending_predicates.is_empty(),
            "shares {:?} did not receive predicates from every parent; some \
             `PredicatePushdown` impl likely failed to recurse into an input — push \
             `Condition::true_cond()` instead of returning early",
            self.pending_predicates.keys().collect::<Vec<_>>(),
        );
        self.skipped_shares.extend(
            self.pending_predicates
                .drain()
                .map(|(share_id, _)| share_id),
        );
        if self.collected_shares.is_empty() {
            self.phase = PredicatePushdownPhase::Idle;
            return collected;
        }

        self.phase = PredicatePushdownPhase::Rewrite;
        let result = root.predicate_pushdown_inner(predicate, self);
        self.phase = PredicatePushdownPhase::Idle;
        result
    }
}
