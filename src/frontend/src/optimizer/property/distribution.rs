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

//!   "A -> B" represent A satisfies B
//!                                 x
//!  only as a required property    x  can used as both required
//!                                 x  and provided property
//!                                 x
//!            ┌───┐                x┌──────┐
//!            │Any◄─────────────────┤single│
//!            └─▲─┘                x└──────┘
//!              │                  x
//!              │                  x
//!              │                  x
//!          ┌───┴────┐             x┌──────────┐
//!          │AnyShard◄──────────────┤SomeShard │
//!          └───▲────┘             x└──────────┘
//!              │                  x
//!          ┌───┴───────────┐      x┌──────────────┐ ┌──────────────┐
//!          │ShardByKey(a,b)◄───┬───┤HashShard(a,b)│ │HashShard(b,a)│
//!          └───▲──▲────────┘   │  x└──────────────┘ └┬─────────────┘
//!              │  │            │  x                  │
//!              │  │            └─────────────────────┘
//!              │  │               x
//!              │ ┌┴────────────┐  x┌────────────┐
//!              │ │ShardByKey(a)◄───┤HashShard(a)│
//!              │ └─────────────┘  x└────────────┘
//!              │                  x
//!             ┌┴────────────┐     x┌────────────┐
//!             │ShardByKey(b)◄──────┤HashShard(b)│
//!             └─────────────┘     x└────────────┘
//!                                 x
//!                                 x
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;

use fixedbitset::FixedBitSet;
use generic::PhysicalPlanRef;
use itertools::Itertools;
use risingwave_batch::worker_manager::worker_node_manager::WorkerNodeSelector;
use risingwave_common::catalog::{FieldDisplay, Schema, TableId};
use risingwave_common::hash::WorkerSlotId;
use risingwave_pb::batch_plan::ExchangeInfo;
use risingwave_pb::batch_plan::exchange_info::{
    ConsistentHashInfo, Distribution as PbDistribution, DistributionMode, HashInfo,
};

use super::super::plan_node::*;
use crate::catalog::FragmentId;
use crate::catalog::catalog_service::CatalogReader;
use crate::error::Result;
use crate::optimizer::property::Order;

/// the distribution property provided by a operator.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Distribution {
    /// There is only one partition. All records are placed on it.
    ///
    /// Note: singleton will not be enforced automatically.
    /// It's set in `crate::stream_fragmenter::build_fragment`,
    /// by setting `requires_singleton` manually.
    Single,
    /// Records are sharded into partitions, and satisfy the `AnyShard` but without any guarantee
    /// about their placement rules.
    SomeShard,
    /// Records are sharded into partitions based on the hash value of some columns, which means
    /// the records with the same hash values must be on the same partition.
    /// `usize` is the index of column used as the distribution key.
    HashShard(Vec<usize>),
    /// A special kind of provided distribution which is almost the same as
    /// [`Distribution::HashShard`], but may have different vnode mapping.
    ///
    /// It exists because the upstream MV can be scaled independently. So we use
    /// `UpstreamHashShard` to **force an exchange to be inserted**.
    ///
    /// Alternatively, [`Distribution::SomeShard`] can also be used to insert an exchange, but
    /// `UpstreamHashShard` contains distribution keys, which might be useful in some cases, e.g.,
    /// two-phase Agg. It also satisfies [`RequiredDist::ShardByKey`].
    ///
    /// `TableId` is used to represent the data distribution(`vnode_mapping`) of this
    /// `UpstreamHashShard`. The scheduler can fetch `TableId`'s corresponding `vnode_mapping` to do
    /// shuffle.
    UpstreamHashShard(Vec<usize>, TableId),
    /// Records are available on all downstream shards.
    Broadcast,
}

/// the distribution property requirement.
#[derive(Debug, Clone, PartialEq)]
pub enum RequiredDist {
    /// with any distribution
    Any,
    /// records are shard on partitions, which means every record should belong to a partition
    AnyShard,
    /// records are shard on partitions based on some keys(order-irrelevance, ShardByKey({a,b}) is
    /// equivalent with ShardByKey({b,a})), which means the records with same keys must be on
    /// the same partition, as required property only. Any distribution sharded by a subset of this
    /// key set satisfies the requirement.
    ShardByKey(FixedBitSet),
    /// records are shard on partitions based on an exact set of keys (order-irrelevance).
    /// Only distribution sharded by the same key set satisfies this requirement.
    ShardByExactKey(FixedBitSet),
    /// must be the same with the physical distribution
    PhysicalDist(Distribution),
}

impl Distribution {
    pub fn to_prost(
        &self,
        output_count: u32,
        catalog_reader: &CatalogReader,
        worker_node_manager: &WorkerNodeSelector,
        batch_parallelism: usize,
    ) -> Result<ExchangeInfo> {
        let exchange_info = ExchangeInfo {
            mode: match self {
                Distribution::Single => DistributionMode::Single,
                Distribution::HashShard(_) => DistributionMode::Hash,
                // TODO: add round robin DistributionMode
                Distribution::SomeShard => DistributionMode::Single,
                Distribution::Broadcast => DistributionMode::Broadcast,
                Distribution::UpstreamHashShard(_, _) => DistributionMode::ConsistentHash,
            } as i32,
            distribution: match self {
                Distribution::Single => None,
                Distribution::HashShard(key) => {
                    assert!(
                        !key.is_empty(),
                        "hash key should not be empty, use `Single` instead"
                    );
                    Some(PbDistribution::HashInfo(HashInfo {
                        output_count,
                        key: key.iter().map(|num| *num as u32).collect(),
                    }))
                }
                // TODO: add round robin distribution
                Distribution::SomeShard => None,
                Distribution::Broadcast => None,
                Distribution::UpstreamHashShard(key, table_id) => {
                    assert!(
                        !key.is_empty(),
                        "hash key should not be empty, use `Single` instead"
                    );

                    let vnode_mapping = worker_node_manager.fragment_mapping(
                        Self::get_fragment_id(catalog_reader, *table_id)?,
                        batch_parallelism,
                    )?;

                    let worker_slot_to_id_map: HashMap<WorkerSlotId, u32> = vnode_mapping
                        .iter_unique()
                        .enumerate()
                        .map(|(i, worker_slot_id)| (worker_slot_id, i as u32))
                        .collect();

                    Some(PbDistribution::ConsistentHashInfo(ConsistentHashInfo {
                        vmap: vnode_mapping
                            .iter()
                            .map(|id| worker_slot_to_id_map[&id])
                            .collect_vec(),
                        key: key.iter().map(|num| *num as u32).collect(),
                    }))
                }
            },
        };
        Ok(exchange_info)
    }

    /// check if the distribution satisfies other required distribution
    pub fn satisfies(&self, required: &RequiredDist) -> bool {
        match required {
            RequiredDist::Any => true,
            RequiredDist::AnyShard => {
                matches!(
                    self,
                    Distribution::SomeShard
                        | Distribution::HashShard(_)
                        | Distribution::UpstreamHashShard(_, _)
                        | Distribution::Broadcast
                )
            }
            RequiredDist::ShardByKey(required_key) => match self {
                Distribution::HashShard(hash_key)
                | Distribution::UpstreamHashShard(hash_key, _) => {
                    hash_key.iter().all(|idx| required_key.contains(*idx))
                }
                _ => false,
            },
            RequiredDist::ShardByExactKey(required_key) => match self {
                Distribution::HashShard(hash_key)
                | Distribution::UpstreamHashShard(hash_key, _) => {
                    hash_key.len() == required_key.count_ones(..)
                        && hash_key.iter().all(|idx| required_key.contains(*idx))
                }
                _ => false,
            },
            RequiredDist::PhysicalDist(other) => self == other,
        }
    }

    /// Get distribution column indices. Panics if the distribution is `SomeShard` or `Broadcast`.
    pub fn dist_column_indices(&self) -> &[usize] {
        self.dist_column_indices_opt()
            .unwrap_or_else(|| panic!("cannot obtain distribution columns for {self:?}"))
    }

    /// Get distribution column indices. Returns `None` if the distribution is `SomeShard` or `Broadcast`.
    pub fn dist_column_indices_opt(&self) -> Option<&[usize]> {
        match self {
            Distribution::Single => Some(&[]),
            Distribution::HashShard(dists) | Distribution::UpstreamHashShard(dists, _) => {
                Some(dists)
            }
            Distribution::SomeShard | Distribution::Broadcast => None,
        }
    }

    #[inline(always)]
    fn get_fragment_id(catalog_reader: &CatalogReader, table_id: TableId) -> Result<FragmentId> {
        catalog_reader
            .read_guard()
            .get_any_table_by_id(table_id)
            .map(|table| table.fragment_id)
            .map_err(Into::into)
    }
}

impl fmt::Display for Distribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        match self {
            Self::Single => f.write_str("Single")?,
            Self::SomeShard => f.write_str("SomeShard")?,
            Self::Broadcast => f.write_str("Broadcast")?,
            Self::HashShard(vec) | Self::UpstreamHashShard(vec, _) => {
                for key in vec {
                    std::fmt::Debug::fmt(&key, f)?;
                }
            }
        }
        f.write_str("]")
    }
}

pub struct DistributionDisplay<'a> {
    pub distribution: &'a Distribution,
    pub input_schema: &'a Schema,
}

impl DistributionDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let that = self.distribution;
        match that {
            Distribution::Single => f.write_str("Single"),
            Distribution::SomeShard => f.write_str("SomeShard"),
            Distribution::Broadcast => f.write_str("Broadcast"),
            Distribution::HashShard(vec) | Distribution::UpstreamHashShard(vec, _) => {
                if let Distribution::HashShard(_) = that {
                    f.write_str("HashShard(")?;
                } else {
                    f.write_str("UpstreamHashShard(")?;
                }
                for (pos, key) in vec.iter().copied().with_position() {
                    std::fmt::Debug::fmt(
                        &FieldDisplay(self.input_schema.fields.get(key).unwrap()),
                        f,
                    )?;
                    match pos {
                        itertools::Position::First | itertools::Position::Middle => {
                            f.write_str(", ")?;
                        }
                        _ => {}
                    }
                }
                f.write_str(")")
            }
        }
    }
}

impl fmt::Debug for DistributionDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}

impl fmt::Display for DistributionDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}

impl RequiredDist {
    pub fn single() -> Self {
        Self::PhysicalDist(Distribution::Single)
    }

    pub fn shard_by_key(tot_col_num: usize, key: &[usize]) -> Self {
        let mut cols = FixedBitSet::with_capacity(tot_col_num);
        for i in key {
            cols.insert(*i);
        }
        assert!(!cols.is_clear());
        Self::ShardByKey(cols)
    }

    pub fn shard_by_exact_key(tot_col_num: usize, key: &[usize]) -> Self {
        let mut cols = FixedBitSet::with_capacity(tot_col_num);
        for i in key {
            cols.insert(*i);
        }
        assert!(!cols.is_clear());
        Self::ShardByExactKey(cols)
    }

    pub fn hash_shard(key: &[usize]) -> Self {
        assert!(!key.is_empty());
        Self::PhysicalDist(Distribution::HashShard(key.to_vec()))
    }

    pub fn batch_enforce_if_not_satisfies(
        &self,
        mut plan: BatchPlanRef,
        required_order: &Order,
    ) -> Result<BatchPlanRef> {
        plan = required_order.enforce_if_not_satisfies(plan)?;
        if !plan.distribution().satisfies(self) {
            Ok(self.batch_enforce(plan, required_order))
        } else {
            Ok(plan)
        }
    }

    pub fn streaming_enforce_if_not_satisfies(&self, plan: StreamPlanRef) -> Result<StreamPlanRef> {
        if !plan.distribution().satisfies(self) {
            Ok(self.stream_enforce(plan))
        } else {
            Ok(plan)
        }
    }

    pub fn no_shuffle(plan: StreamPlanRef) -> StreamPlanRef {
        StreamExchange::new_no_shuffle(plan).into()
    }

    /// check if the distribution satisfies other required distribution
    pub fn satisfies(&self, required: &RequiredDist) -> bool {
        match self {
            RequiredDist::Any => matches!(required, RequiredDist::Any),
            RequiredDist::AnyShard => {
                matches!(required, RequiredDist::Any | RequiredDist::AnyShard)
            }
            RequiredDist::ShardByKey(key) => match required {
                RequiredDist::Any | RequiredDist::AnyShard => true,
                RequiredDist::ShardByKey(required_key) => key.is_subset(required_key),
                RequiredDist::ShardByExactKey(required_key) => {
                    key == required_key && key.count_ones(..) == 1
                }
                _ => false,
            },
            RequiredDist::ShardByExactKey(key) => match required {
                RequiredDist::Any | RequiredDist::AnyShard => true,
                RequiredDist::ShardByKey(required_key) => key.is_subset(required_key),
                RequiredDist::ShardByExactKey(required_key) => key == required_key,
                _ => false,
            },
            RequiredDist::PhysicalDist(dist) => dist.satisfies(required),
        }
    }

    pub fn batch_enforce(&self, plan: BatchPlanRef, required_order: &Order) -> BatchPlanRef {
        let dist = self.to_dist();
        BatchExchange::new(plan, required_order.clone(), dist).into()
    }

    pub fn stream_enforce(&self, plan: StreamPlanRef) -> StreamPlanRef {
        let dist = self.to_dist();
        StreamExchange::new(plan, dist).into()
    }

    fn to_dist(&self) -> Distribution {
        match self {
            // all the distribution satisfy the Any, and the function can be only called by
            // `enforce_if_not_satisfies`
            RequiredDist::Any => unreachable!(),
            // TODO: add round robin distributed type
            RequiredDist::AnyShard => todo!(),
            RequiredDist::ShardByKey(required_keys) => {
                Distribution::HashShard(required_keys.ones().collect())
            }
            RequiredDist::ShardByExactKey(required_keys) => {
                Distribution::HashShard(required_keys.ones().collect())
            }
            RequiredDist::PhysicalDist(dist) => dist.clone(),
        }
    }
}

impl StreamPlanRef {
    /// Eliminate `SomeShard` distribution by using the stream key as the distribution key to
    /// enforce the current plan to have a known distribution key.
    pub fn enforce_concrete_distribution(self) -> Self {
        match self.distribution() {
            Distribution::SomeShard => {
                RequiredDist::shard_by_key(self.schema().len(), self.expect_stream_key())
                    .stream_enforce(self)
            }
            _ => self,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Distribution, RequiredDist};

    #[test]
    fn hash_shard_satisfy() {
        let d1 = Distribution::HashShard(vec![0, 1]);
        let d2 = Distribution::HashShard(vec![1, 0]);
        let d3 = Distribution::HashShard(vec![0]);
        let d4 = Distribution::HashShard(vec![1]);

        let r1 = RequiredDist::shard_by_key(2, &[0, 1]);
        let r3 = RequiredDist::shard_by_key(2, &[0]);
        let r4 = RequiredDist::shard_by_key(2, &[1]);
        let r_exact = RequiredDist::shard_by_exact_key(2, &[0, 1]);
        let r_exact_single = RequiredDist::shard_by_exact_key(2, &[0]);
        assert!(d1.satisfies(&RequiredDist::PhysicalDist(d1.clone())));
        assert!(d2.satisfies(&RequiredDist::PhysicalDist(d2.clone())));
        assert!(d3.satisfies(&RequiredDist::PhysicalDist(d3.clone())));
        assert!(d4.satisfies(&RequiredDist::PhysicalDist(d4.clone())));

        assert!(!d2.satisfies(&RequiredDist::PhysicalDist(d1.clone())));
        assert!(!d3.satisfies(&RequiredDist::PhysicalDist(d1.clone())));
        assert!(!d4.satisfies(&RequiredDist::PhysicalDist(d1.clone())));

        assert!(!d1.satisfies(&RequiredDist::PhysicalDist(d3.clone())));
        assert!(!d2.satisfies(&RequiredDist::PhysicalDist(d3.clone())));
        assert!(!d1.satisfies(&RequiredDist::PhysicalDist(d4.clone())));
        assert!(!d2.satisfies(&RequiredDist::PhysicalDist(d4.clone())));

        assert!(d1.satisfies(&r1));
        assert!(d2.satisfies(&r1));
        assert!(d3.satisfies(&r1));
        assert!(d4.satisfies(&r1));

        assert!(!d1.satisfies(&r3));
        assert!(!d2.satisfies(&r3));
        assert!(d3.satisfies(&r3));
        assert!(!d4.satisfies(&r3));

        assert!(!d1.satisfies(&r4));
        assert!(!d2.satisfies(&r4));
        assert!(!d3.satisfies(&r4));
        assert!(d4.satisfies(&r4));

        assert!(d1.satisfies(&r_exact));
        assert!(d2.satisfies(&r_exact));
        assert!(!d3.satisfies(&r_exact));
        assert!(!d4.satisfies(&r_exact));

        assert!(r3.satisfies(&r1));
        assert!(r4.satisfies(&r1));
        assert!(!r1.satisfies(&r3));
        assert!(!r1.satisfies(&r4));
        assert!(!r3.satisfies(&r4));
        assert!(!r4.satisfies(&r3));

        assert!(r_exact.satisfies(&r1));
        assert!(!r1.satisfies(&r_exact));
        assert!(!r3.satisfies(&r_exact));
        assert!(!r_exact.satisfies(&r3));

        assert!(r3.satisfies(&r_exact_single));
        assert!(r_exact_single.satisfies(&r3));
    }
}
