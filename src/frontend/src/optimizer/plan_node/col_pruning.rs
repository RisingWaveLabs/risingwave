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
use crate::optimizer::plan_visitor::ShareParentCounter;
use crate::optimizer::{LogicalPlanRef as PlanRef, PlanVisitor};

/// The trait for column pruning, only logical plan node will use it, though all plan node impl it.
pub trait ColPrunable {
    /// Transform the plan node to only output the required columns ordered by index number.
    ///
    /// `required_cols` must be a subset of the range `0..self.schema().len()`.
    ///
    /// After calling `prune_col` on the children, their output schema may change, so
    /// the caller may need to transform its [`InputRef`](crate::expr::InputRef) using
    /// [`ColIndexMapping`](crate::utils::ColIndexMapping).
    ///
    /// When implementing this method for a node, it may require its children to produce additional
    /// columns besides `required_cols`. In this case, it may need to insert a
    /// [`LogicalProject`](super::LogicalProject) above to have a correct schema.
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnPruningPhase {
    Idle,
    Collect,
    Rewrite,
}

#[derive(Debug, Clone)]
struct ShareColumnPruning {
    original_input: PlanRef,
    required_cols: Vec<usize>,
}

#[derive(Debug, Clone)]
pub struct ColumnPruningContext {
    pending_required_cols: HashMap<ShareId, Vec<Vec<usize>>>,
    collected_shares: HashMap<ShareId, ShareColumnPruning>,
    share_parent_counter: ShareParentCounter,
    share_mappings: HashMap<ShareId, ColIndexMapping>,
    skipped_shares: HashSet<ShareId>,
    phase: ColumnPruningPhase,
}

impl ColumnPruningContext {
    pub fn new(root: PlanRef) -> Self {
        let mut share_parent_counter = ShareParentCounter::default();
        share_parent_counter.visit(root);
        Self {
            pending_required_cols: HashMap::new(),
            collected_shares: HashMap::new(),
            share_parent_counter,
            share_mappings: HashMap::new(),
            skipped_shares: HashSet::new(),
            phase: ColumnPruningPhase::Idle,
        }
    }

    pub(in crate::optimizer) fn is_running(&self) -> bool {
        self.phase != ColumnPruningPhase::Idle
    }

    pub(in crate::optimizer) fn is_collecting(&self) -> bool {
        self.phase == ColumnPruningPhase::Collect
    }

    pub(in crate::optimizer) fn get_parent_num(&self, share: &LogicalShare) -> usize {
        self.share_parent_counter.get_parent_num(share)
    }

    pub(in crate::optimizer) fn add_required_cols(
        &mut self,
        share: &LogicalShare,
        required_cols: Vec<usize>,
    ) -> Option<Vec<usize>> {
        let share_id = share.share_id();
        let parent_num = self.share_parent_counter.get_parent_num_by_id(share_id);
        let pending = self.pending_required_cols.entry(share_id).or_default();
        pending.push(required_cols);
        assert!(
            pending.len() <= parent_num,
            "share {share_id:?} received more column requirements than parents"
        );
        if pending.len() != parent_num {
            return None;
        }

        let merged_required_cols = self
            .pending_required_cols
            .remove(&share_id)
            .expect("column requirements must exist")
            .into_iter()
            .flatten()
            .sorted()
            .dedup()
            .collect_vec();
        self.collected_shares
            .try_insert(
                share_id,
                ShareColumnPruning {
                    original_input: share.input(),
                    required_cols: merged_required_cols.clone(),
                },
            )
            .expect("column requirements must be merged once per share");
        Some(merged_required_cols)
    }

    pub(in crate::optimizer) fn share_mapping(&self, share: &LogicalShare) -> ColIndexMapping {
        self.share_mappings
            .get(&share.share_id())
            .unwrap_or_else(|| {
                panic!(
                    "logical share {:?} has no column-pruning mapping",
                    share.share_id()
                )
            })
            .clone()
    }

    /// Rebuilds one shared definition on first use. Nested shares recursively rebuild first, so
    /// the call stack provides the child-before-parent order without a separate dependency graph.
    pub(in crate::optimizer) fn ensure_share_rebuilt(&mut self, share: &LogicalShare) {
        let share_id = share.share_id();
        if self.share_mappings.contains_key(&share_id) {
            return;
        }
        assert_eq!(self.phase, ColumnPruningPhase::Rewrite);

        let Some(ShareColumnPruning {
            original_input,
            required_cols,
        }) = self.collected_shares.remove(&share_id)
        else {
            // The share was skipped during collection (see `run`): it never received a
            // requirement from every parent, so it must keep its original definition and
            // full schema. Parents prune with their own projections above the share.
            assert!(
                self.skipped_shares.contains(&share_id),
                "share {share_id:?} has no collected column requirements"
            );
            self.share_mappings
                .try_insert(share_id, ColIndexMapping::identity(share.schema().len()))
                .expect("a logical share must be rebuilt once");
            return;
        };
        let old_schema_len = original_input.schema().len();
        let rebuilt_input = original_input.prune_col(&required_cols, self);
        let mapping = ColIndexMapping::with_remaining_columns(&required_cols, old_schema_len);
        debug_assert_eq!(mapping.target_size(), rebuilt_input.schema().len());

        share.ctx().update_logical_share(share_id, rebuilt_input);
        self.share_mappings
            .try_insert(share_id, mapping)
            .expect("a logical share must be rebuilt once");
    }

    pub(in crate::optimizer) fn run(&mut self, root: PlanRef, required_cols: &[usize]) -> PlanRef {
        self.phase = ColumnPruningPhase::Collect;
        let collected = root.prune_col_inner(required_cols, self);
        // `ShareParentCounter` counts parents via the visitor walk, but the transformation
        // walk is not guaranteed to reach a share from every parent: some `ColPrunable`
        // impls (e.g. `LogicalVectorSearchLookupJoin`, which never prunes its lookup side)
        // legitimately stop recursing into an input. Pruning with a subset of the parents'
        // requirements would drop columns still referenced by the parents that never
        // contributed, so such shares are skipped and keep their original definition.
        self.skipped_shares.extend(
            self.pending_required_cols
                .drain()
                .map(|(share_id, _)| share_id),
        );
        if self.collected_shares.is_empty() {
            self.phase = ColumnPruningPhase::Idle;
            return collected;
        }

        self.phase = ColumnPruningPhase::Rewrite;
        let result = root.prune_col_inner(required_cols, self);
        self.phase = ColumnPruningPhase::Idle;
        result
    }
}
