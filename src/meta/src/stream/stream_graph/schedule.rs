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

use std::collections::{BTreeMap, HashMap};

use anyhow::Context;
use enum_as_inner::EnumAsInner;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::hash::{ActorAlignmentId, VnodeCountCompat};
use risingwave_common::util::stream_graph_visitor::visit_fragment;
use risingwave_connector::source::cdc::{CDC_BACKFILL_MAX_PARALLELISM, CdcScanOptions};
use risingwave_meta_model::WorkerId;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::table_fragments::fragment::{
    FragmentDistributionType, PbFragmentDistributionType,
};
use risingwave_pb::stream_plan::DispatcherType::{self, *};

use crate::MetaResult;
use crate::model::{ActorId, Fragment};
use crate::stream::stream_graph::fragment::CompleteStreamFragmentGraph;
use crate::stream::stream_graph::id::GlobalFragmentId as Id;

type HashMappingId = usize;

/// The internal structure for processing scheduling requirements in the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Req {
    /// The fragment must be singleton and is scheduled to the given worker id.
    Singleton,
    /// The fragment must be hash-distributed and is scheduled by the given hash mapping.
    Hash(HashMappingId),
    /// The fragment must have the given vnode count, but can be scheduled anywhere.
    /// When the vnode count is 1, it means the fragment must be singleton.
    AnyVnodeCount(usize),
}

impl Req {
    /// Equivalent to `Req::AnyVnodeCount(1)`.
    #[expect(non_upper_case_globals)]
    const AnySingleton: Self = Self::AnyVnodeCount(1);

    /// Merge two requirements. Returns an error if the requirements are incompatible.
    ///
    /// The `mapping_len` function is used to get the vnode count of a hash mapping by its id.
    fn merge(a: Self, b: Self, mapping_len: impl Fn(HashMappingId) -> usize) -> MetaResult<Self> {
        // Note that a and b are always different, as they come from a set.
        let merge = |a, b| match (a, b) {
            (Self::AnySingleton, Self::Singleton) => Some(Self::Singleton),
            (Self::AnyVnodeCount(count), Self::Hash(id)) if mapping_len(id) == count => {
                Some(Self::Hash(id))
            }
            _ => None,
        };

        match merge(a, b).or_else(|| merge(b, a)) {
            Some(req) => Ok(req),
            None => bail!("incompatible requirements `{a:?}` and `{b:?}`"),
        }
    }
}

/// Facts as the input of the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Fact {
    /// An edge in the fragment graph.
    Edge {
        from: Id,
        to: Id,
        dt: DispatcherType,
    },
    /// A scheduling requirement for a fragment.
    Req { id: Id, req: Req },
}

crepe::crepe! {
    @input
    struct Input(Fact);

    struct Edge(Id, Id, DispatcherType);
    struct ExternalReq(Id, Req);

    @output
    struct Requirement(Id, Req);

    // Extract facts.
    Edge(from, to, dt) <- Input(f), let Fact::Edge { from, to, dt } = f;
    Requirement(id, req) <- Input(f), let Fact::Req { id, req } = f;

    // The downstream fragment of a `Simple` edge must be singleton.
    Requirement(y, Req::AnySingleton) <- Edge(_, y, Simple);
    // Requirements propagate through `NoShuffle` edges.
    Requirement(x, d) <- Edge(x, y, NoShuffle), Requirement(y, d);
    Requirement(y, d) <- Edge(x, y, NoShuffle), Requirement(x, d);
}

/// The distribution (scheduling result) of a fragment.
#[derive(Debug, Clone, EnumAsInner)]
pub(super) enum Distribution {
    /// The fragment is singleton and is scheduled to the given worker slot.
    Singleton,

    /// The fragment is hash-distributed and is scheduled by the given hash mapping.
    Hash(usize),
}

impl Distribution {
    /// Get the vnode count of the distribution.
    pub fn vnode_count(&self) -> usize {
        match self {
            Distribution::Singleton => 1, // only `SINGLETON_VNODE`
            Distribution::Hash(vnode_count) => *vnode_count,
        }
    }

    /// Create a distribution from a persisted protobuf `Fragment`.
    pub fn from_fragment(fragment: &Fragment) -> Self {
        match fragment.distribution_type {
            FragmentDistributionType::Single => Distribution::Singleton,
            FragmentDistributionType::Hash => Distribution::Hash(fragment.vnode_count()),
            PbFragmentDistributionType::Unspecified => {
                unreachable!()
            }
        }
    }

    /// Convert the distribution to [`PbFragmentDistributionType`].
    pub fn to_distribution_type(&self) -> PbFragmentDistributionType {
        match self {
            Distribution::Singleton => PbFragmentDistributionType::Single,
            Distribution::Hash(_) => PbFragmentDistributionType::Hash,
        }
    }
}

/// [`Scheduler`] schedules the distribution of fragments in a stream graph.
pub(super) struct Scheduler {
    /// The default hash mapping for hash-distributed fragments, if there's no requirement derived.
    default_vnode_count: usize,
}

impl Scheduler {
    /// Create a new [`Scheduler`] with the expected vnode count of the streaming job.
    pub fn new(expected_vnode_count: usize) -> MetaResult<Self> {
        Ok(Self {
            default_vnode_count: expected_vnode_count,
        })
    }

    /// Schedule the given complete graph and returns the distribution of each **building
    /// fragment**.
    pub fn schedule(
        &self,
        graph: &CompleteStreamFragmentGraph,
    ) -> MetaResult<HashMap<Id, Distribution>> {
        let existing_distribution = graph.existing_distribution();

        // Build an index map for all hash mappings.
        let all_hash_mappings = existing_distribution
            .values()
            .flat_map(|dist| dist.as_hash())
            .cloned()
            .unique()
            .collect_vec();
        let hash_mapping_id: HashMap<_, _> = all_hash_mappings
            .iter()
            .enumerate()
            .map(|(i, m)| (*m, i))
            .collect();

        let mut facts = Vec::new();

        // Singletons.
        for (&id, fragment) in graph.building_fragments() {
            if fragment.requires_singleton {
                facts.push(Fact::Req {
                    id,
                    req: Req::AnySingleton,
                });
            }
        }
        let mut force_parallelism_fragment_ids: HashMap<_, _> = HashMap::default();
        // Vnode count requirements: if a fragment is going to look up an existing table,
        // it must have the same vnode count as that table.
        for (&id, fragment) in graph.building_fragments() {
            visit_fragment(fragment, |node| {
                use risingwave_pb::stream_plan::stream_node::NodeBody;
                let vnode_count = match node {
                    NodeBody::StreamScan(node) => {
                        if let Some(table) = &node.arrangement_table {
                            table.vnode_count()
                        } else if let Some(table) = &node.table_desc {
                            table.vnode_count()
                        } else {
                            return;
                        }
                    }
                    NodeBody::TemporalJoin(node) => {
                        if node.is_broadcast {
                            // Every actor owns a full replicated view of the lookup table, so the
                            // join fragment follows the left input rather than the lookup table's
                            // vnode count.
                            return;
                        }
                        node.get_table_desc().unwrap().vnode_count()
                    }
                    NodeBody::BatchPlan(node) => node.get_table_desc().unwrap().vnode_count(),
                    NodeBody::Lookup(node) => node
                        .get_arrangement_table_info()
                        .unwrap()
                        .get_table_desc()
                        .unwrap()
                        .vnode_count(),
                    NodeBody::StreamCdcScan(node) => {
                        let Some(ref options) = node.options else {
                            return;
                        };
                        let options = CdcScanOptions::from_proto(options);
                        if options.is_parallelized_backfill() {
                            force_parallelism_fragment_ids
                                .insert(id, options.backfill_parallelism as usize);
                            CDC_BACKFILL_MAX_PARALLELISM as usize
                        } else {
                            return;
                        }
                    }
                    _ => return,
                };
                facts.push(Fact::Req {
                    id,
                    req: Req::AnyVnodeCount(vnode_count),
                });
            });
        }
        // Distributions of existing fragments.
        for (id, dist) in existing_distribution {
            let req = match dist {
                Distribution::Singleton => Req::Singleton,
                Distribution::Hash(mapping) => Req::Hash(hash_mapping_id[&mapping]),
            };
            facts.push(Fact::Req { id, req });
        }
        // Edges.
        for (from, to, edge) in graph.all_edges() {
            facts.push(Fact::Edge {
                from,
                to,
                dt: edge.dispatch_strategy.r#type(),
            });
        }

        // Run the algorithm to propagate requirements.
        let mut crepe = Crepe::new();
        crepe.extend(facts.into_iter().map(Input));
        let (reqs,) = crepe.run();
        let reqs = reqs
            .into_iter()
            .map(|Requirement(id, req)| (id, req))
            .into_group_map();

        // Derive scheduling result from requirements.
        let mut distributions = HashMap::new();
        for &id in graph.building_fragments().keys() {
            let dist = match reqs.get(&id) {
                // Merge all requirements.
                Some(reqs) => {
                    let req = (reqs.iter().copied())
                        .try_reduce(|a, b| Req::merge(a, b, |id| all_hash_mappings[id]))
                        .with_context(|| {
                            format!("cannot fulfill scheduling requirements for fragment {id:?}")
                        })?
                        .unwrap();

                    // Derive distribution from the merged requirement.
                    match req {
                        Req::Singleton => Distribution::Singleton,
                        Req::Hash(mapping) => Distribution::Hash(all_hash_mappings[mapping]),
                        Req::AnySingleton => Distribution::Singleton,
                        Req::AnyVnodeCount(vnode_count) => Distribution::Hash(vnode_count),
                    }
                }
                // No requirement, use the default.
                None => Distribution::Hash(self.default_vnode_count),
            };

            distributions.insert(id, dist);
        }

        tracing::debug!(?distributions, "schedule fragments");

        Ok(distributions)
    }
}

/// [`Locations`] represents the locations of the actors.
#[cfg_attr(test, derive(Default))]
pub struct Locations {
    /// actor location map.
    pub actor_locations: BTreeMap<ActorId, ActorAlignmentId>,
    /// worker location map.
    pub worker_locations: HashMap<WorkerId, WorkerNode>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    enum Result {
        DefaultHash,
        Required(Req),
    }

    impl Result {
        #[expect(non_upper_case_globals)]
        const DefaultSingleton: Self = Self::Required(Req::AnySingleton);
    }

    fn run_and_merge(
        facts: impl IntoIterator<Item = Fact>,
        mapping_len: impl Fn(HashMappingId) -> usize,
    ) -> MetaResult<HashMap<Id, Req>> {
        let mut crepe = Crepe::new();
        crepe.extend(facts.into_iter().map(Input));
        let (reqs,) = crepe.run();

        let reqs = reqs
            .into_iter()
            .map(|Requirement(id, req)| (id, req))
            .into_group_map();

        let mut merged = HashMap::new();
        for (id, reqs) in reqs {
            let req = (reqs.iter().copied())
                .try_reduce(|a, b| Req::merge(a, b, &mapping_len))
                .with_context(|| {
                    format!("cannot fulfill scheduling requirements for fragment {id:?}")
                })?
                .unwrap();
            merged.insert(id, req);
        }

        Ok(merged)
    }

    fn test_success(facts: impl IntoIterator<Item = Fact>, expected: HashMap<Id, Result>) {
        test_success_with_mapping_len(facts, expected, |_| 0);
    }

    fn test_success_with_mapping_len(
        facts: impl IntoIterator<Item = Fact>,
        expected: HashMap<Id, Result>,
        mapping_len: impl Fn(HashMappingId) -> usize,
    ) {
        let reqs = run_and_merge(facts, mapping_len).unwrap();

        for (id, expected) in expected {
            match (reqs.get(&id), expected) {
                (None, Result::DefaultHash) => {}
                (Some(actual), Result::Required(expected)) if *actual == expected => {}
                (actual, expected) => panic!(
                    "unexpected result for fragment {id:?}\nactual: {actual:?}\nexpected: {expected:?}"
                ),
            }
        }
    }

    fn test_failed(facts: impl IntoIterator<Item = Fact>) {
        run_and_merge(facts, |_| 0).unwrap_err();
    }

    // 101
    #[test]
    fn test_single_fragment_hash() {
        #[rustfmt::skip]
        let facts = [];

        let expected = maplit::hashmap! {
            101.into() => Result::DefaultHash,
        };

        test_success(facts, expected);
    }

    // 101
    #[test]
    fn test_single_fragment_singleton() {
        #[rustfmt::skip]
        let facts = [
            Fact::Req { id: 101.into(), req: Req::AnySingleton },
        ];

        let expected = maplit::hashmap! {
            101.into() => Result::DefaultSingleton,
        };

        test_success(facts, expected);
    }

    // 1 -|-> 101 -->
    //                103 --> 104
    // 2 -|-> 102 -->
    #[test]
    fn test_scheduling_mv_on_mv() {
        #[rustfmt::skip]
        let facts = [
            Fact::Req { id: 1.into(), req: Req::Hash(1) },
            Fact::Req { id: 2.into(), req: Req::Singleton },
            Fact::Edge { from: 1.into(), to: 101.into(), dt: NoShuffle },
            Fact::Edge { from: 2.into(), to: 102.into(), dt: NoShuffle },
            Fact::Edge { from: 101.into(), to: 103.into(), dt: Hash },
            Fact::Edge { from: 102.into(), to: 103.into(), dt: Hash },
            Fact::Edge { from: 103.into(), to: 104.into(), dt: Simple },
        ];

        let expected = maplit::hashmap! {
            101.into() => Result::Required(Req::Hash(1)),
            102.into() => Result::Required(Req::Singleton),
            103.into() => Result::DefaultHash,
            104.into() => Result::DefaultSingleton,
        };

        test_success(facts, expected);
    }

    // 1 -|-> 101 --> 103 -->
    //             X          105
    // 2 -|-> 102 --> 104 -->
    #[test]
    fn test_delta_join() {
        #[rustfmt::skip]
        let facts = [
            Fact::Req { id: 1.into(), req: Req::Hash(1) },
            Fact::Req { id: 2.into(), req: Req::Hash(2) },
            Fact::Edge { from: 1.into(), to: 101.into(), dt: NoShuffle },
            Fact::Edge { from: 2.into(), to: 102.into(), dt: NoShuffle },
            Fact::Edge { from: 101.into(), to: 103.into(), dt: NoShuffle },
            Fact::Edge { from: 102.into(), to: 104.into(), dt: NoShuffle },
            Fact::Edge { from: 101.into(), to: 104.into(), dt: Hash },
            Fact::Edge { from: 102.into(), to: 103.into(), dt: Hash },
            Fact::Edge { from: 103.into(), to: 105.into(), dt: Hash },
            Fact::Edge { from: 104.into(), to: 105.into(), dt: Hash },
        ];

        let expected = maplit::hashmap! {
            101.into() => Result::Required(Req::Hash(1)),
            102.into() => Result::Required(Req::Hash(2)),
            103.into() => Result::Required(Req::Hash(1)),
            104.into() => Result::Required(Req::Hash(2)),
            105.into() => Result::DefaultHash,
        };

        test_success(facts, expected);
    }

    // 1 -|-> 101 -->
    //                103
    //        102 -->
    #[test]
    fn test_singleton_leaf() {
        #[rustfmt::skip]
        let facts = [
            Fact::Req { id: 1.into(), req: Req::Hash(1) },
            Fact::Edge { from: 1.into(), to: 101.into(), dt: NoShuffle },
            Fact::Req { id: 102.into(), req: Req::AnySingleton }, // like `Now`
            Fact::Edge { from: 101.into(), to: 103.into(), dt: Hash },
            Fact::Edge { from: 102.into(), to: 103.into(), dt: Broadcast },
        ];

        let expected = maplit::hashmap! {
            101.into() => Result::Required(Req::Hash(1)),
            102.into() => Result::DefaultSingleton,
            103.into() => Result::DefaultHash,
        };

        test_success(facts, expected);
    }

    // 1 -|->
    //        101
    // 2 -|->
    #[test]
    fn test_upstream_hash_shard_failed() {
        #[rustfmt::skip]
        let facts = [
            Fact::Req { id: 1.into(), req: Req::Hash(1) },
            Fact::Req { id: 2.into(), req: Req::Hash(2) },
            Fact::Edge { from: 1.into(), to: 101.into(), dt: NoShuffle },
            Fact::Edge { from: 2.into(), to: 101.into(), dt: NoShuffle },
        ];

        test_failed(facts);
    }

    // 1 -|~> 101
    #[test]
    fn test_arrangement_backfill_vnode_count() {
        #[rustfmt::skip]
        let facts = [
            Fact::Req { id: 1.into(), req: Req::Hash(1) },
            Fact::Req { id: 101.into(), req: Req::AnyVnodeCount(128) },
            Fact::Edge { from: 1.into(), to: 101.into(), dt: Hash },
        ];

        let expected = maplit::hashmap! {
            101.into() => Result::Required(Req::AnyVnodeCount(128)),
        };

        test_success(facts, expected);
    }

    // 1 -|~> 101
    #[test]
    fn test_no_shuffle_backfill_vnode_count() {
        #[rustfmt::skip]
        let facts = [
            Fact::Req { id: 1.into(), req: Req::Hash(1) },
            Fact::Req { id: 101.into(), req: Req::AnyVnodeCount(128) },
            Fact::Edge { from: 1.into(), to: 101.into(), dt: NoShuffle },
        ];

        let expected = maplit::hashmap! {
            101.into() => Result::Required(Req::Hash(1)),
        };

        test_success_with_mapping_len(facts, expected, |id| {
            assert_eq!(id, 1);
            128
        });
    }

    // 1 -|~> 101
    #[test]
    fn test_no_shuffle_backfill_mismatched_vnode_count() {
        #[rustfmt::skip]
        let facts = [
            Fact::Req { id: 1.into(), req: Req::Hash(1) },
            Fact::Req { id: 101.into(), req: Req::AnyVnodeCount(128) },
            Fact::Edge { from: 1.into(), to: 101.into(), dt: NoShuffle },
        ];

        // Not specifying `mapping_len` should fail.
        test_failed(facts);
    }

    // 1 -|~> 101
    #[test]
    fn test_backfill_singleton_vnode_count() {
        #[rustfmt::skip]
        let facts = [
            Fact::Req { id: 1.into(), req: Req::Singleton },
            Fact::Req { id: 101.into(), req: Req::AnySingleton },
            Fact::Edge { from: 1.into(), to: 101.into(), dt: NoShuffle }, // or `Simple`
        ];

        let expected = maplit::hashmap! {
            101.into() => Result::Required(Req::Singleton),
        };

        test_success(facts, expected);
    }
}
