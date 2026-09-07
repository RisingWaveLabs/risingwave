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

use super::{DefaultBehavior, LogicalPlanVisitor, Merge};
use crate::error::{ErrorCode, RwError};
use crate::optimizer::plan_node::{LogicalApply, LogicalPlanRef as PlanRef, PlanTreeNodeBinary};
use crate::optimizer::plan_visitor::PlanVisitor;

pub struct HasMaxOneRowApply();

impl LogicalPlanVisitor for HasMaxOneRowApply {
    type Result = bool;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a, b| a | b)
    }

    fn visit_logical_apply(&mut self, plan: &LogicalApply) -> bool {
        plan.max_one_row() | self.visit(plan.left()) | self.visit(plan.right())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Default)]
enum CheckResult {
    #[default]
    Ok,
    CannotBeUnnested,
    MoreThanOneRow,
}

impl From<CheckResult> for Result<(), RwError> {
    fn from(val: CheckResult) -> Self {
        let msg = match val {
            CheckResult::Ok => return Ok(()),
            CheckResult::CannotBeUnnested => "Subquery cannot be unnested.",
            CheckResult::MoreThanOneRow => "Scalar subquery might produce more than one row.",
        };

        Err(ErrorCode::InternalError(msg.to_owned()).into())
    }
}

#[derive(Default)]
pub struct CheckApplyElimination {
    result: CheckResult,
}

impl LogicalPlanVisitor for CheckApplyElimination {
    type Result = ();

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(std::cmp::max)
    }

    fn visit_logical_apply(&mut self, plan: &LogicalApply) {
        // If there's a runtime max-one-row check on the right side, it's likely to be the
        // reason for the failed unnesting. Report users with a more precise error message.
        if plan.right().as_logical_max_one_row().is_some() {
            self.result = CheckResult::MoreThanOneRow;
        } else {
            self.result = CheckResult::CannotBeUnnested;
        }
    }
}

#[easy_ext::ext(PlanCheckApplyEliminationExt)]
impl PlanRef {
    /// Checks if all `LogicalApply` nodes in the plan have been eliminated, that is,
    /// subqueries are successfully unnested.
    pub fn check_apply_elimination(&self) -> Result<(), RwError> {
        let mut visitor = CheckApplyElimination::default();
        visitor.visit(self.clone());
        visitor.result.into()
    }
}
