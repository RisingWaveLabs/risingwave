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

use std::fmt;
use std::hash::{Hash, Hasher};

use risingwave_common::catalog::Schema;

use super::{GenericPlanNode, GenericPlanRef};
use crate::optimizer::plan_node::PlanNodeId;
use crate::optimizer::property::FunctionalDependencySet;
use crate::optimizer::{OptimizerContextRef, ShareEntryRef, ShareId};

/// Immutable handle to a registered shared subplan.
#[derive(Clone)]
pub struct Share<PlanRef> {
    share_id: ShareId,
    input: PlanRef,
    /// Keeps the weak context registry entry alive without making the context own a plan that
    /// points back to itself.
    entry: ShareEntryRef<PlanRef>,
}

impl<PlanRef> fmt::Debug for Share<PlanRef> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Share")
            .field("share_id", &self.share_id)
            .finish_non_exhaustive()
    }
}

/// Compare and hash the shared subplan *structurally* instead of by `share_id`: ids are
/// allocated per registration, so two plans built independently (e.g. an MV selection
/// candidate vs. the query plan) never agree on ids even when they are semantically
/// identical. Structural comparison keeps `Eq`/`Hash` consistent with the behavior
/// before the share input was moved into the context side table, which structural
/// matchers like `MvSelectionRule` and common sub-plan sharing rely on.
impl<PlanRef: PartialEq> PartialEq for Share<PlanRef> {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
    }
}

impl<PlanRef: Eq> Eq for Share<PlanRef> {}

impl<PlanRef: Hash> Hash for Share<PlanRef> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
    }
}

impl<PlanRef: Clone> Share<PlanRef> {
    pub(in crate::optimizer) fn new(
        share_id: ShareId,
        input: PlanRef,
        entry: ShareEntryRef<PlanRef>,
    ) -> Self {
        Self {
            share_id,
            input,
            entry,
        }
    }

    pub(in crate::optimizer) fn with_input(&self, input: PlanRef) -> Self {
        Self {
            share_id: self.share_id,
            input,
            entry: self.entry.clone(),
        }
    }

    pub fn share_id(&self) -> ShareId {
        self.share_id
    }

    pub fn plan_node_id(&self) -> PlanNodeId {
        self.entry.plan_node_id()
    }

    pub fn input(&self) -> PlanRef {
        self.input.clone()
    }
}

impl<PlanRef: GenericPlanRef + Clone> GenericPlanNode for Share<PlanRef> {
    fn schema(&self) -> Schema {
        self.input.schema().clone()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        Some(self.input.stream_key()?.to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.input.functional_dependency().clone()
    }
}
