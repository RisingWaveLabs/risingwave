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
use std::collections::hash_map::Entry;
use std::fmt;

use itertools::Itertools;

use super::ApplyResult;
#[cfg(debug_assertions)]
use crate::Explain;
use crate::error::Result;
use crate::optimizer::plan_node::{ConventionMarker, ShareNode};
use crate::optimizer::rule::BoxedRule;
use crate::optimizer::{PlanRef, PlanTreeNode, ShareId};

/// Traverse order of [`HeuristicOptimizer`]
pub enum ApplyOrder {
    TopDown,
    BottomUp,
}

// TODO: we should have a builder of HeuristicOptimizer here
/// A rule-based heuristic optimizer, which traverses every plan nodes and tries to
/// apply each rule on them.
pub struct HeuristicOptimizer<'a, C: ConventionMarker> {
    apply_order: &'a ApplyOrder,
    rules: &'a [BoxedRule<C>],
    stats: Stats,
    share_cache: HashMap<ShareId, (PlanRef<C>, bool)>,
}

impl<'a, C: ConventionMarker> HeuristicOptimizer<'a, C> {
    pub fn new(apply_order: &'a ApplyOrder, rules: &'a [BoxedRule<C>]) -> Self {
        Self {
            apply_order,
            rules,
            stats: Stats::new(),
            share_cache: HashMap::new(),
        }
    }

    fn optimize_node(&mut self, mut plan: PlanRef<C>) -> Result<(PlanRef<C>, bool)> {
        let mut changed = false;
        for rule in self.rules {
            match rule.apply(plan.clone()) {
                ApplyResult::Ok(applied) => {
                    #[cfg(debug_assertions)]
                    Self::check_equivalent_plan(rule.description(), &plan, &applied);

                    plan = applied;
                    changed = true;
                    self.stats.count_rule(rule);
                }
                ApplyResult::NotApplicable => {}
                ApplyResult::Err(error) => return Err(error),
            }
        }
        Ok((plan, changed))
    }

    fn optimize_inputs(&mut self, plan: PlanRef<C>) -> Result<(PlanRef<C>, bool)> {
        let optimized_inputs: Vec<_> = plan
            .inputs()
            .into_iter()
            .map(|sub_tree| self.optimize_recursively(sub_tree))
            .try_collect()?;
        let changed = optimized_inputs.iter().any(|(_, changed)| *changed);

        if changed {
            let inputs = optimized_inputs
                .into_iter()
                .map(|(input, _)| input)
                .collect_vec();
            Ok((plan.clone_root_with_inputs(&inputs), true))
        } else {
            Ok((plan, false))
        }
    }

    fn optimize_recursively(&mut self, plan: PlanRef<C>) -> Result<(PlanRef<C>, bool)> {
        if let Some(share_id) = plan.as_share_node().map(ShareNode::share_id) {
            if let Some((cached, changed)) = self.share_cache.get(&share_id) {
                return Ok((cached.clone(), *changed));
            }
            let (optimized, changed) = self.optimize_uncached(plan)?;
            self.share_cache
                .insert(share_id, (optimized.clone(), changed));
            return Ok((optimized, changed));
        }

        self.optimize_uncached(plan)
    }

    fn optimize_uncached(&mut self, plan: PlanRef<C>) -> Result<(PlanRef<C>, bool)> {
        match self.apply_order {
            ApplyOrder::TopDown => {
                let (plan, node_changed) = self.optimize_node(plan)?;
                let (plan, inputs_changed) = self.optimize_inputs(plan)?;
                Ok((plan, node_changed || inputs_changed))
            }
            ApplyOrder::BottomUp => {
                let (plan, inputs_changed) = self.optimize_inputs(plan)?;
                let (plan, node_changed) = self.optimize_node(plan)?;
                Ok((plan, inputs_changed || node_changed))
            }
        }
    }

    pub fn optimize(&mut self, plan: PlanRef<C>) -> Result<PlanRef<C>> {
        self.share_cache.clear();
        self.optimize_recursively(plan).map(|(plan, _)| plan)
    }

    pub fn get_stats(&self) -> &Stats {
        &self.stats
    }

    #[cfg(debug_assertions)]
    pub fn check_equivalent_plan(
        rule_desc: &str,
        input_plan: &PlanRef<C>,
        output_plan: &PlanRef<C>,
    ) {
        use crate::optimizer::plan_node::generic::GenericPlanRef;
        if !input_plan.schema().type_eq(output_plan.schema()) {
            panic!(
                "{} fails to generate equivalent plan.\nInput schema: {:?}\nInput plan: \n{}\nOutput schema: {:?}\nOutput plan: \n{}\nSQL: {}",
                rule_desc,
                input_plan.schema(),
                input_plan.explain_to_string(),
                output_plan.schema(),
                output_plan.explain_to_string(),
                output_plan.ctx().sql()
            );
        }
    }
}

pub struct Stats {
    total_applied: usize,
    rule_counter: HashMap<String, u32>,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            rule_counter: HashMap::new(),
            total_applied: 0,
        }
    }

    pub fn count_rule(&mut self, rule: &BoxedRule<impl ConventionMarker>) {
        self.total_applied += 1;
        match self.rule_counter.entry(rule.description().to_owned()) {
            Entry::Occupied(mut entry) => {
                *entry.get_mut() += 1;
            }
            Entry::Vacant(entry) => {
                entry.insert(1);
            }
        }
    }

    pub fn has_applied_rule(&self) -> bool {
        self.total_applied != 0
    }

    pub fn total_applied(&self) -> usize {
        self.total_applied
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (rule, count) in &self.rule_counter {
            writeln!(f, "apply {} {} time(s)", rule, count)?;
        }
        Ok(())
    }
}
