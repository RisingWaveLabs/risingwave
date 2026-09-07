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

use std::collections::HashMap;

use super::{DefaultBehavior, DefaultValue, LogicalPlanVisitor};
use crate::optimizer::ShareId;
use crate::optimizer::plan_node::{LogicalShare, PlanTreeNodeUnary, ShareNode};
use crate::optimizer::plan_visitor::PlanVisitor;

#[derive(Debug, Clone, Default)]
pub struct ShareParentCounter {
    /// Share identity to parent number mapping.
    parent_counter: HashMap<ShareId, usize>,
}

impl ShareParentCounter {
    pub fn get_parent_num(&self, share: &LogicalShare) -> usize {
        self.get_parent_num_by_id(share.share_id())
    }

    pub fn get_parent_num_by_id(&self, share_id: ShareId) -> usize {
        *self
            .parent_counter
            .get(&share_id)
            .expect("share must exist")
    }
}

impl LogicalPlanVisitor for ShareParentCounter {
    type Result = ();

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        DefaultValue
    }

    fn visit_logical_share(&mut self, share: &LogicalShare) {
        let v = self
            .parent_counter
            .entry(share.share_id())
            .and_modify(|counter| *counter += 1)
            .or_insert(1);
        if *v == 1 {
            self.visit(share.input())
        }
    }
}
