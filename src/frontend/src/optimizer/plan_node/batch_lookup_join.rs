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

use std::sync::Arc;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::ColumnId;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{DistributedLookupJoinNode, LocalLookupJoinNode};
use risingwave_pb::plan_common::AsOfJoinDesc;
use risingwave_sqlparser::ast::AsOf;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record, to_batch_query_epoch};
use super::{BatchPlanRef as PlanRef, BatchSeqScan, ExprRewritable, generic};
use crate::TableCatalog;
use crate::error::Result;
use crate::expr::{Expr, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{
    EqJoinPredicate, EqJoinPredicateDisplay, PlanBase, PlanTreeNodeUnary, ToDistributedBatch,
    ToLocalBatch, TryToBatchPb,
};
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::scheduler::SchedulerResult;
use crate::utils::ColIndexMappingRewriteExt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchLookupJoin {
    pub base: PlanBase<Batch>,
    core: generic::Join<PlanRef>,

    /// Table description of the right side table
    right_table: Arc<TableCatalog>,

    /// Output column ids of the right side table
    right_output_column_ids: Vec<ColumnId>,

    /// The prefix length of the order key of right side table.
    lookup_prefix_len: usize,

    /// If `distributed_lookup` is true, it will generate `DistributedLookupJoinNode` for
    /// `ToBatchPb`. Otherwise, it will generate `LookupJoinNode`.
    distributed_lookup: bool,

    as_of: Option<AsOf>,
    // `AsOf` join description
    asof_desc: Option<AsOfJoinDesc>,
}

impl BatchLookupJoin {
    pub fn new(
        core: generic::Join<PlanRef>,
        right_table: Arc<TableCatalog>,
        right_output_column_ids: Vec<ColumnId>,
        lookup_prefix_len: usize,
        distributed_lookup: bool,
        as_of: Option<AsOf>,
        asof_desc: Option<AsOfJoinDesc>,
    ) -> Self {
        // We cannot create a `BatchLookupJoin` without any eq keys. We require eq keys to do the
        // lookup.
        let eq_join_predicate = core
            .on
            .as_eq_predicate_ref()
            .expect("BatchLookupJoin requires JoinOn::EqPredicate in core");
        assert!(eq_join_predicate.has_eq());
        assert!(eq_join_predicate.eq_keys_are_type_aligned());
        let dist = Self::derive_dist(core.left.distribution(), &core);
        let base = PlanBase::new_batch_with_core(&core, dist, Order::any());
        Self {
            base,
            core,
            right_table,
            right_output_column_ids,
            lookup_prefix_len,
            distributed_lookup,
            as_of,
            asof_desc,
        }
    }

    fn derive_dist(left: &Distribution, core: &generic::Join<PlanRef>) -> Distribution {
        match left {
            Distribution::Single => Distribution::Single,
            Distribution::HashShard(_) | Distribution::UpstreamHashShard(_, _) => {
                let l2o = core.l2i_col_mapping().composite(&core.i2o_col_mapping());
                l2o.rewrite_provided_distribution(left)
            }
            _ => unreachable!(),
        }
    }

    fn eq_join_predicate(&self) -> &EqJoinPredicate {
        self.core
            .on
            .as_eq_predicate_ref()
            .expect("BatchLookupJoin should store predicate as EqJoinPredicate")
    }

    pub fn right_table(&self) -> &TableCatalog {
        &self.right_table
    }

    fn clone_with_distributed_lookup(&self, input: PlanRef, distributed_lookup: bool) -> Self {
        let mut batch_lookup_join = self.clone_with_input(input);
        batch_lookup_join.distributed_lookup = distributed_lookup;
        batch_lookup_join
    }

    pub fn lookup_prefix_len(&self) -> usize {
        self.lookup_prefix_len
    }
}

impl Distill for BatchLookupJoin {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(if verbose { 3 } else { 2 });
        vec.push(("type", Pretty::debug(&self.core.join_type)));

        let concat_schema = self.core.concat_schema();
        vec.push((
            "predicate",
            Pretty::debug(&EqJoinPredicateDisplay {
                eq_join_predicate: self.eq_join_predicate(),
                input_schema: &concat_schema,
            }),
        ));

        if verbose {
            let data = IndicesDisplay::from_join(&self.core, &concat_schema);
            vec.push(("output", data));
        }

        let scan: &BatchSeqScan = self.core.right.as_batch_seq_scan().unwrap();

        vec.push(("lookup table", Pretty::display(&scan.core().table_name())));

        if let Some(as_of) = &self.as_of {
            vec.push(("as_of", Pretty::debug(as_of)));
        }

        childless_record("BatchLookupJoin", vec)
    }
}

impl PlanTreeNodeUnary<Batch> for BatchLookupJoin {
    fn input(&self) -> PlanRef {
        self.core.left.clone()
    }

    // Only change left side
    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.left = input;
        Self::new(
            core,
            self.right_table.clone(),
            self.right_output_column_ids.clone(),
            self.lookup_prefix_len,
            self.distributed_lookup,
            self.as_of.clone(),
            self.asof_desc,
        )
    }
}

impl_plan_tree_node_for_unary! { Batch, BatchLookupJoin }

impl ToDistributedBatch for BatchLookupJoin {
    fn to_distributed(&self) -> Result<PlanRef> {
        let right_table = &self.right_table;

        // The lookup table has a singleton distribution, so there's no way to align the
        // left side with the distribution key of the right table. Instead, gather the left
        // side into a single task and perform all lookups from there. Note that this is
        // still correct because the lookup executor scans the table with a full vnode
        // bitmap, regardless of which worker the task is scheduled to.
        if right_table.distribution_key.is_empty() {
            let input = self
                .input()
                .to_distributed_with_required(&Order::any(), &RequiredDist::single())?;
            return Ok(self.clone_with_distributed_lookup(input, true).into());
        }

        // Align left distribution keys with the right table.
        let mut exchange_dist_keys = vec![];
        let left_eq_indexes = self.eq_join_predicate().left_eq_indexes();
        for dist_col_index in &right_table.distribution_key {
            let dist_col_id = right_table.columns[*dist_col_index].column_desc.column_id;
            let output_pos = self
                .right_output_column_ids
                .iter()
                .position(|p| *p == dist_col_id)
                .unwrap();
            let dist_in_eq_indexes = self
                .eq_join_predicate()
                .right_eq_indexes()
                .iter()
                .position(|col| *col == output_pos)
                .unwrap();
            assert!(dist_in_eq_indexes < self.lookup_prefix_len);
            exchange_dist_keys.push(left_eq_indexes[dist_in_eq_indexes]);
        }

        assert!(!exchange_dist_keys.is_empty());

        let input = self.input().to_distributed_with_required(
            &Order::any(),
            &RequiredDist::PhysicalDist(Distribution::UpstreamHashShard(
                exchange_dist_keys,
                self.right_table.id,
            )),
        )?;

        Ok(self.clone_with_distributed_lookup(input, true).into())
    }
}

impl TryToBatchPb for BatchLookupJoin {
    fn try_to_batch_prost_body(&self) -> SchedulerResult<NodeBody> {
        let eq_join_predicate = self.eq_join_predicate();
        Ok(if self.distributed_lookup {
            NodeBody::DistributedLookupJoin(DistributedLookupJoinNode {
                join_type: self.core.join_type as i32,
                condition: self
                    .eq_join_predicate()
                    .other_cond()
                    .as_expr_unless_true()
                    .map(|x| x.to_expr_proto()),
                outer_side_key: self
                    .eq_join_predicate()
                    .left_eq_indexes()
                    .into_iter()
                    .map(|a| a as _)
                    .collect(),
                inner_side_key: self
                    .eq_join_predicate()
                    .right_eq_indexes()
                    .into_iter()
                    .map(|a| a as _)
                    .collect(),
                inner_side_table_desc: Some(self.right_table.table_desc().try_to_protobuf()?),
                inner_side_column_ids: self
                    .right_output_column_ids
                    .iter()
                    .map(ColumnId::get_id)
                    .collect(),
                output_indices: self.core.output_indices.iter().map(|&x| x as u32).collect(),
                null_safe: eq_join_predicate.null_safes(),
                lookup_prefix_len: self.lookup_prefix_len as u32,
                query_epoch: to_batch_query_epoch(&self.as_of)?,
                asof_desc: self.asof_desc,
            })
        } else {
            NodeBody::LocalLookupJoin(LocalLookupJoinNode {
                join_type: self.core.join_type as i32,
                condition: self
                    .eq_join_predicate()
                    .other_cond()
                    .as_expr_unless_true()
                    .map(|x| x.to_expr_proto()),
                outer_side_key: self
                    .eq_join_predicate()
                    .left_eq_indexes()
                    .into_iter()
                    .map(|a| a as _)
                    .collect(),
                inner_side_key: self
                    .eq_join_predicate()
                    .right_eq_indexes()
                    .into_iter()
                    .map(|a| a as _)
                    .collect(),
                inner_side_table_desc: Some(self.right_table.table_desc().try_to_protobuf()?),
                inner_side_vnode_mapping: vec![], // To be filled in at local.rs
                inner_side_column_ids: self
                    .right_output_column_ids
                    .iter()
                    .map(ColumnId::get_id)
                    .collect(),
                output_indices: self.core.output_indices.iter().map(|&x| x as u32).collect(),
                worker_nodes: vec![], // To be filled in at local.rs
                null_safe: eq_join_predicate.null_safes(),
                lookup_prefix_len: self.lookup_prefix_len as u32,
                query_epoch: to_batch_query_epoch(&self.as_of)?,
                asof_desc: self.asof_desc,
            })
        })
    }
}

impl ToLocalBatch for BatchLookupJoin {
    fn to_local(&self) -> Result<PlanRef> {
        let input = RequiredDist::single()
            .batch_enforce_if_not_satisfies(self.input().to_local()?, &Order::any())?;

        Ok(self.clone_with_distributed_lookup(input, false).into())
    }
}

impl ExprRewritable<Batch> for BatchLookupJoin {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let base = self.base.clone_with_new_plan_id();
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        let mut new = self.clone();
        new.base = base;
        new.core = core;
        new.into()
    }
}

impl ExprVisitable for BatchLookupJoin {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
