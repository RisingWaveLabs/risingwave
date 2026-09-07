// Copyright 2025 RisingWave Labs
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
use crate::optimizer::plan_node::{LogicalLocalityProvider, LogicalPlanRef, PlanTreeNodeUnary};
use crate::optimizer::plan_visitor::PlanVisitor;

#[derive(Debug, Clone, Default)]
pub struct LocalityProviderCounter {}

impl LocalityProviderCounter {
    pub fn count(plan: LogicalPlanRef) -> usize {
        let mut locality_provider_counter = Self::default();
        locality_provider_counter.visit(plan)
    }
}

impl LogicalPlanVisitor for LocalityProviderCounter {
    type Result = usize;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a: usize, b| a + b)
    }

    fn visit_logical_locality_provider(
        &mut self,
        locality_provider: &LogicalLocalityProvider,
    ) -> Self::Result {
        1 + self.visit(locality_provider.input())
    }
}
