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
use std::hash::Hash;

use super::{
    EndoPlan, LogicalPlanRef as PlanRef, LogicalShare, PlanTreeNodeUnary, ShareNode, VisitPlan,
};
use crate::optimizer::{ShareId, plan_visitor};
use crate::utils::{Endo, Visit};

pub trait Semantics<V: Hash + Eq> {
    fn semantics(&self) -> V;
}

impl Semantics<PlanRef> for PlanRef {
    fn semantics(&self) -> PlanRef {
        self.clone()
    }
}

impl PlanRef {
    pub fn common_subplan_sharing<V: Hash + Eq>(self) -> PlanRef
    where
        PlanRef: Semantics<V>,
    {
        Merger::default().apply(self)
    }
}

struct Merger<V: Hash + Eq> {
    cache: HashMap<V, LogicalShare>,
}

impl<V: Hash + Eq> Default for Merger<V> {
    fn default() -> Self {
        Merger {
            cache: Default::default(),
        }
    }
}

impl<V: Hash + Eq> Endo<PlanRef> for Merger<V>
where
    PlanRef: Semantics<V>,
{
    fn apply(&mut self, t: PlanRef) -> PlanRef {
        let semantics = t.semantics();
        let share = self.cache.get(&semantics).cloned().unwrap_or_else(|| {
            let share = LogicalShare::new(self.tree_apply(t));
            self.cache.entry(semantics).or_insert(share).clone()
        });
        share.into()
    }
}

impl PlanRef {
    pub fn prune_share(&self) -> PlanRef {
        let mut counter = Counter::default();
        counter.visit(self);
        counter.to_pruner().apply(self.clone())
    }
}

#[derive(Default)]
struct Counter {
    counts: HashMap<ShareId, u64>,
}

impl Counter {
    fn to_pruner(&self) -> Pruner<'_> {
        Pruner {
            counts: &self.counts,
            cache: HashMap::new(),
        }
    }
}

impl VisitPlan for Counter {
    fn visited<F>(&mut self, plan: &PlanRef, mut f: F)
    where
        F: FnMut(&mut Self),
    {
        let share_id = plan
            .as_logical_share()
            .expect("dag cache is only used for shares")
            .share_id();
        if self.counts.get(&share_id).is_none_or(|c| *c <= 1) {
            f(self);
        }
    }
}

impl Visit<PlanRef> for Counter {
    fn visit(&mut self, t: &PlanRef) {
        if let Some(s) = t.as_logical_share() {
            self.counts
                .entry(s.share_id())
                .and_modify(|c| *c += 1)
                .or_insert(1);
        }
        self.dag_visit(t);
    }
}

struct Pruner<'a> {
    counts: &'a HashMap<ShareId, u64>,
    cache: HashMap<ShareId, PlanRef>,
}

impl EndoPlan for Pruner<'_> {
    fn cached<F>(&mut self, plan: PlanRef, mut f: F) -> PlanRef
    where
        F: FnMut(&mut Self) -> PlanRef,
    {
        let share_id = plan
            .as_logical_share()
            .expect("dag cache is only used for shares")
            .share_id();
        self.cache.get(&share_id).cloned().unwrap_or_else(|| {
            let res = f(self);
            self.cache.entry(share_id).or_insert(res).clone()
        })
    }
}

impl Endo<PlanRef> for Pruner<'_> {
    fn pre(&mut self, t: PlanRef) -> PlanRef {
        let prunable = |s: &&LogicalShare| {
            // Prune if share node has only one parent
            // or it just shares a scan
            // or it doesn't share any scan or source.
            *self
                .counts
                .get(&s.share_id())
                .expect("Unprocessed shared node.")
                == 1
                || s.input().as_logical_scan().is_some()
                || !(plan_visitor::has_logical_scan(s.input())
                    || plan_visitor::has_logical_source(s.input()))
        };
        t.as_logical_share()
            .filter(prunable)
            .map_or(t.clone(), |s| self.pre(s.input()))
    }

    fn apply(&mut self, t: PlanRef) -> PlanRef {
        self.dag_apply(t)
    }
}
