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

//! Defines all kinds of node in the plan tree, each node represent a relational expression.
//!
//! We use a immutable style tree structure, every Node are immutable and cannot be modified after
//! it has been created. If you want to modify the node, such as rewriting the expression in a
//! `ProjectNode` or changing a node's input node, you need to create a new node. We use Rc as the
//! node's reference, and a node just storage its inputs' reference, so change a node just need
//! create one new node but not the entire sub-tree.
//!
//! So when you want to add a new node, make sure:
//! - each field in the node struct are private
//! - recommend to implement the construction of Node in a unified `new()` function, if have multi
//!   methods to construct, make they have a consistent behavior
//! - all field should be valued in construction, so the properties' derivation should be finished
//!   in the `new()` function.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Deref;
use std::rc::Rc;

use downcast_rs::{Downcast, impl_downcast};
use dyn_clone::DynClone;
use itertools::Itertools;
use paste::paste;
use petgraph::dot::{Config, Dot};
use petgraph::graph::Graph;
use pretty_xmlish::{Pretty, PrettyConfig};
use risingwave_common::catalog::Schema;
use risingwave_common::util::recursive::{self, Recurse};
use risingwave_pb::batch_plan::PlanNode as PbBatchPlan;
use risingwave_pb::stream_plan::StreamNode as PbStreamPlan;
use serde::Serialize;

use self::batch::BatchPlanNodeMetadata;
use self::generic::{GenericPlanRef, PhysicalPlanRef};
use self::stream::StreamPlanNodeMetadata;
use self::utils::Distill;
use super::property::{
    Distribution, FunctionalDependencySet, MonotonicityMap, Order, WatermarkColumns,
};
use crate::error::{ErrorCode, Result};
use crate::optimizer::property::StreamKind;
use crate::optimizer::{PlanVisitor, ShareId};
use crate::session::current::notice_to_user;
use crate::utils::{PrettySerde, build_graph_from_pretty};

/// A marker trait for different conventions, used for enforcing type safety.
///
/// Implementors are [`Logical`], [`Batch`], and [`Stream`].
pub trait ConventionMarker: 'static + Sized + Clone + Debug + Eq + PartialEq + Hash {
    /// The extra fields in the [`PlanBase`] of this convention.
    type Extra: 'static + Eq + Hash + Clone + Debug;
    type ShareNode: ShareNode<Self>;
    type PlanRefDyn: PlanNodeCommon<Self> + Eq + Hash + ?Sized;
    type PlanNodeType;

    fn as_share(plan: &Self::PlanRefDyn) -> Option<&Self::ShareNode>;
}

pub trait ShareNode<C: ConventionMarker>:
    AnyPlanNodeMeta<C> + PlanTreeNodeUnary<C> + 'static
{
    fn share_id(&self) -> ShareId;
    fn new_share(share: generic::Share<PlanRef<C>>) -> PlanRef<C>;
    fn replace_input(&self, plan: PlanRef<C>) -> PlanRef<C>;
    fn fork_with_input(&self, plan: PlanRef<C>) -> PlanRef<C>;
}

pub struct NoShareNode<C: ConventionMarker>(!, PhantomData<C>);

impl<C: ConventionMarker> ShareNode<C> for NoShareNode<C> {
    fn share_id(&self) -> ShareId {
        unreachable!()
    }

    fn new_share(_plan: generic::Share<PlanRef<C>>) -> PlanRef<C> {
        unreachable!()
    }

    fn replace_input(&self, _plan: PlanRef<C>) -> PlanRef<C> {
        unreachable!()
    }

    fn fork_with_input(&self, _plan: PlanRef<C>) -> PlanRef<C> {
        unreachable!()
    }
}

impl<C: ConventionMarker> PlanTreeNodeUnary<C> for NoShareNode<C> {
    fn input(&self) -> PlanRef<C> {
        unreachable!()
    }

    fn clone_with_input(&self, _input: PlanRef<C>) -> Self {
        unreachable!()
    }
}

impl<C: ConventionMarker> AnyPlanNodeMeta<C> for NoShareNode<C> {
    fn node_type(&self) -> C::PlanNodeType {
        unreachable!()
    }

    fn plan_base(&self) -> &PlanBase<C> {
        unreachable!()
    }
}

/// The marker for logical convention.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Logical;
impl ConventionMarker for Logical {
    type Extra = plan_base::NoExtra;
    type PlanNodeType = LogicalPlanNodeType;
    type PlanRefDyn = dyn LogicalPlanNode;
    type ShareNode = LogicalShare;

    fn as_share(plan: &Self::PlanRefDyn) -> Option<&Self::ShareNode> {
        plan.as_logical_share()
    }
}

/// The marker for batch convention.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Batch;
impl ConventionMarker for Batch {
    type Extra = plan_base::BatchExtra;
    type PlanNodeType = BatchPlanNodeType;
    type PlanRefDyn = dyn BatchPlanNode;
    type ShareNode = NoShareNode<Batch>;

    fn as_share(_plan: &Self::PlanRefDyn) -> Option<&Self::ShareNode> {
        None
    }
}

/// The marker for stream convention.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Stream;
impl ConventionMarker for Stream {
    type Extra = plan_base::StreamExtra;
    type PlanNodeType = StreamPlanNodeType;
    type PlanRefDyn = dyn StreamPlanNode;
    type ShareNode = StreamShare;

    fn as_share(plan: &Self::PlanRefDyn) -> Option<&Self::ShareNode> {
        plan.as_stream_share()
    }
}

/// The trait for accessing the meta data and [`PlanBase`] for plan nodes.
pub trait PlanNodeMeta {
    type Convention: ConventionMarker;
    const NODE_TYPE: <Self::Convention as ConventionMarker>::PlanNodeType;
    /// Get the reference to the [`PlanBase`] with corresponding convention.
    fn plan_base(&self) -> &PlanBase<Self::Convention>;
}

// Intentionally made private.
mod plan_node_meta {
    use super::*;

    /// The object-safe version of [`PlanNodeMeta`], used as a super trait of `PlanNode`.
    ///
    /// Check [`PlanNodeMeta`] for more details.
    pub trait AnyPlanNodeMeta<C: ConventionMarker> {
        fn node_type(&self) -> C::PlanNodeType;
        fn plan_base(&self) -> &PlanBase<C>;
    }

    /// Implement [`AnyPlanNodeMeta`] for all [`PlanNodeMeta`].
    impl<P> AnyPlanNodeMeta<P::Convention> for P
    where
        P: PlanNodeMeta,
    {
        fn node_type(&self) -> <P::Convention as ConventionMarker>::PlanNodeType {
            P::NODE_TYPE
        }

        fn plan_base(&self) -> &PlanBase<P::Convention> {
            <Self as PlanNodeMeta>::plan_base(self)
        }
    }
}
use plan_node_meta::AnyPlanNodeMeta;

pub trait PlanNodeCommon<C: ConventionMarker> = PlanTreeNode<C>
    + DynClone
    + DynEq
    + DynHash
    + Distill
    + Debug
    + Downcast
    + ExprRewritable<C>
    + ExprVisitable
    + AnyPlanNodeMeta<C>;

/// The common trait over all plan nodes. Used by optimizer framework which will treat all node as
/// `dyn PlanNode`
///
/// We split the trait into lots of sub-trait so that we can easily use macro to impl them.
pub trait StreamPlanNode: PlanNodeCommon<Stream> + TryToStreamPb {}
pub trait BatchPlanNode:
    PlanNodeCommon<Batch> + ToDistributedBatch + ToLocalBatch + TryToBatchPb
{
}
pub trait LogicalPlanNode:
    PlanNodeCommon<Logical> + ColPrunable + PredicatePushdown + ToBatch + ToStream
{
}

macro_rules! impl_trait {
    ($($convention:ident),+) => {
        paste! {
            $(
                impl Hash for dyn [<$convention  PlanNode>] {
                    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                        self.dyn_hash(state);
                    }
                }

                impl PartialEq for dyn [<$convention  PlanNode>] {
                    fn eq(&self, other: &Self) -> bool {
                        self.dyn_eq(other.as_dyn_eq())
                    }
                }

                impl Eq for dyn [<$convention  PlanNode>] {}
            )+
        }
    };
}

impl_trait!(Batch, Stream, Logical);
impl_downcast!(BatchPlanNode);
impl_downcast!(LogicalPlanNode);
impl_downcast!(StreamPlanNode);

// Using a new type wrapper allows direct function implementation on `PlanRef`,
// and we currently need a manual implementation of `PartialEq` for `PlanRef`.
#[expect(clippy::derived_hash_with_manual_eq)]
#[derive(Debug, Eq, Hash)]
pub struct PlanRef<C: ConventionMarker>(Rc<C::PlanRefDyn>);

impl<C: ConventionMarker> Clone for PlanRef<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

pub type LogicalPlanRef = PlanRef<Logical>;
pub type StreamPlanRef = PlanRef<Stream>;
pub type BatchPlanRef = PlanRef<Batch>;

// Cannot use the derived implementation for now.
// See https://github.com/rust-lang/rust/issues/31740
#[expect(clippy::op_ref)]
impl<C: ConventionMarker> PartialEq for PlanRef<C> {
    fn eq(&self, other: &Self) -> bool {
        &self.0 == &other.0
    }
}

impl<C: ConventionMarker> Deref for PlanRef<C> {
    type Target = C::PlanRefDyn;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<T: LogicalPlanNode> From<T> for PlanRef<Logical> {
    fn from(value: T) -> Self {
        PlanRef(Rc::new(value) as _)
    }
}

impl<T: StreamPlanNode> From<T> for PlanRef<Stream> {
    fn from(value: T) -> Self {
        PlanRef(Rc::new(value) as _)
    }
}

impl<T: BatchPlanNode> From<T> for PlanRef<Batch> {
    fn from(value: T) -> Self {
        PlanRef(Rc::new(value) as _)
    }
}

impl<C: ConventionMarker> Layer for PlanRef<C> {
    type Sub = Self;

    fn map<F>(self, f: F) -> Self
    where
        F: FnMut(Self::Sub) -> Self::Sub,
    {
        self.clone_root_with_inputs(&self.inputs().into_iter().map(f).collect_vec())
    }

    fn descent<F>(&self, f: F)
    where
        F: FnMut(&Self::Sub),
    {
        self.inputs().iter().for_each(f);
    }
}

#[derive(Clone, Debug, Copy, Serialize, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct PlanNodeId(pub i32);

impl PlanNodeId {
    pub fn to_stream_node_operator_id(self) -> StreamNodeLocalOperatorId {
        StreamNodeLocalOperatorId::new(self.0 as _)
    }
}

/// A more sophisticated `Endo` taking into account of the DAG structure of `PlanRef`.
/// In addition to `Endo`, one have to specify the `cached` function
/// to persist transformed `LogicalShare` and their results,
/// and the `dag_apply` function will take care to only transform every `LogicalShare` nodes once.
///
/// Note: Due to the way super trait is designed in rust,
/// one need to have separate implementation blocks of `Endo<PlanRef>` and `EndoPlan`.
/// And conventionally the real transformation `apply` is under `Endo<PlanRef>`,
/// although one can refer to `dag_apply` in the implementation of `apply`.
pub trait EndoPlan: Endo<LogicalPlanRef> {
    // Return the cached result of `plan` if present,
    // otherwise store and return the value provided by `f`.
    // Notice that to allow mutable access of `self` in `f`,
    // we let `f` to take `&mut Self` as its first argument.
    fn cached<F>(&mut self, plan: LogicalPlanRef, f: F) -> LogicalPlanRef
    where
        F: FnMut(&mut Self) -> LogicalPlanRef;

    fn dag_apply(&mut self, plan: LogicalPlanRef) -> LogicalPlanRef {
        match plan.as_logical_share() {
            Some(_) => self.cached(plan.clone(), |this| this.tree_apply(plan.clone())),
            None => self.tree_apply(plan),
        }
    }
}

/// A more sophisticated `Visit` taking into account of the DAG structure of `PlanRef`.
/// In addition to `Visit`, one have to specify `visited`
/// to store and report visited `LogicalShare` nodes,
/// and the `dag_visit` function will take care to only visit every `LogicalShare` nodes once.
/// See also `EndoPlan`.
pub trait VisitPlan: Visit<LogicalPlanRef> {
    // Skip visiting `plan` if visited, otherwise run the traversal provided by `f`.
    // Notice that to allow mutable access of `self` in `f`,
    // we let `f` to take `&mut Self` as its first argument.
    fn visited<F>(&mut self, plan: &LogicalPlanRef, f: F)
    where
        F: FnMut(&mut Self);

    fn dag_visit(&mut self, plan: &LogicalPlanRef) {
        match plan.as_logical_share() {
            Some(_) => self.visited(plan, |this| this.tree_visit(plan)),
            None => self.tree_visit(plan),
        }
    }
}

impl<C: ConventionMarker> PlanRef<C> {
    pub fn rewrite_exprs_recursive(&self, r: &mut impl ExprRewriter) -> PlanRef<C> {
        self.rewrite_exprs_recursive_inner(r, &mut HashMap::new())
    }

    fn rewrite_exprs_recursive_inner(
        &self,
        r: &mut impl ExprRewriter,
        rewritten_shares: &mut HashMap<ShareId, PlanRef<C>>,
    ) -> PlanRef<C> {
        if let Some(share) = self.as_share_node() {
            let share_id = share.share_id();
            if let Some(rewritten) = rewritten_shares.get(&share_id) {
                return rewritten.clone();
            }

            let rewritten = self.rewrite_exprs(r);
            let rewritten_share = rewritten
                .as_share_node()
                .expect("rewriting expressions must preserve a share node");
            let input = rewritten_share
                .input()
                .rewrite_exprs_recursive_inner(r, rewritten_shares);
            let rewritten = rewritten_share.replace_input(input);
            rewritten_shares.insert(share_id, rewritten.clone());
            return rewritten;
        }

        let new = self.rewrite_exprs(r);
        let inputs: Vec<PlanRef<C>> = new
            .inputs()
            .iter()
            .map(|plan_ref| plan_ref.rewrite_exprs_recursive_inner(r, rewritten_shares))
            .collect();
        new.clone_root_with_inputs(&inputs[..])
    }
}

pub(crate) trait VisitExprsRecursive {
    fn visit_exprs_recursive(&self, r: &mut impl ExprVisitor);
}

impl<C: ConventionMarker> VisitExprsRecursive for PlanRef<C> {
    fn visit_exprs_recursive(&self, r: &mut impl ExprVisitor) {
        self.visit_exprs_recursive_inner(r, &mut HashSet::new());
    }
}

impl<C: ConventionMarker> PlanRef<C> {
    fn visit_exprs_recursive_inner(
        &self,
        r: &mut impl ExprVisitor,
        visited_shares: &mut HashSet<ShareId>,
    ) {
        if let Some(share) = self.as_share_node()
            && !visited_shares.insert(share.share_id())
        {
            return;
        }
        self.visit_exprs(r);
        self.inputs()
            .iter()
            .for_each(|plan_ref| plan_ref.visit_exprs_recursive_inner(r, visited_shares));
    }
}

impl<C: ConventionMarker> PlanRef<C> {
    pub fn expect_stream_key(&self) -> &[usize] {
        self.stream_key().unwrap_or_else(|| {
            panic!(
                "a stream key is expected but not exist, plan:\n{}",
                self.explain_to_string()
            )
        })
    }
}

impl LogicalPlanRef {
    fn prune_col_inner(
        &self,
        required_cols: &[usize],
        ctx: &mut ColumnPruningContext,
    ) -> LogicalPlanRef {
        if let Some(logical_share) = self.as_logical_share() {
            // A single-parent share is not a DAG boundary and can be removed immediately.
            if ctx.get_parent_num(logical_share) == 1 {
                return logical_share.input().prune_col(required_cols, ctx);
            }

            if ctx.is_collecting() {
                if let Some(merged_required_cols) =
                    ctx.add_required_cols(logical_share, required_cols.to_vec())
                {
                    // Continue collection below the share only after every parent has
                    // contributed its requirement. Rewriting happens in the next phase.
                    let _ = logical_share.input().prune_col(&merged_required_cols, ctx);
                }

                let mapping =
                    ColIndexMapping::with_remaining_columns(required_cols, self.schema().len());
                LogicalProject::with_mapping(self.clone(), mapping).into()
            } else {
                ctx.ensure_share_rebuilt(logical_share);
                let share_mapping = ctx.share_mapping(logical_share);
                let new_required_cols = required_cols
                    .iter()
                    .map(|&column| {
                        share_mapping.try_map(column).unwrap_or_else(|| {
                            panic!(
                                "column {column} is absent from the collected mapping for share {:?}",
                                logical_share.share_id()
                            )
                        })
                    })
                    .collect_vec();
                let refreshed_share: LogicalPlanRef =
                    LogicalShare::from_share_id(self.ctx(), logical_share.share_id()).into();
                let mapping = ColIndexMapping::with_remaining_columns(
                    &new_required_cols,
                    refreshed_share.schema().len(),
                );
                LogicalProject::with_mapping(refreshed_share, mapping).into()
            }
        } else {
            // Dispatch to dyn PlanNode instead of PlanRef.
            let dyn_t = self.deref();
            dyn_t.prune_col(required_cols, ctx)
        }
    }

    fn predicate_pushdown_inner(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> LogicalPlanRef {
        if let Some(logical_share) = self.as_logical_share() {
            // A single-parent share is not a DAG boundary and can be removed immediately.
            if ctx.get_parent_num(logical_share) == 1 {
                return logical_share.input().predicate_pushdown(predicate, ctx);
            }

            if ctx.is_collecting() {
                if let Some(merged_predicate) = ctx.add_predicate(logical_share, predicate.clone())
                {
                    // Continue collection below the share only after every parent has
                    // contributed. Rewriting happens in the next phase.
                    let _ = logical_share
                        .input()
                        .predicate_pushdown(merged_predicate, ctx);
                }
                LogicalFilter::create(self.clone(), predicate)
            } else {
                ctx.ensure_share_rebuilt(logical_share);
                let refreshed_share: LogicalPlanRef =
                    LogicalShare::from_share_id(self.ctx(), logical_share.share_id()).into();
                LogicalFilter::create(refreshed_share, predicate)
            }
        } else {
            // Dispatch to dyn PlanNode instead of PlanRef.
            let dyn_t = self.deref();
            dyn_t.predicate_pushdown(predicate, ctx)
        }
    }

    pub fn forbid_snapshot_backfill(&self) -> Option<String> {
        struct ForbidSnapshotBackfill {
            warning_msg: Option<String>,
        }
        impl LogicalPlanVisitor for ForbidSnapshotBackfill {
            type Result = ();

            type DefaultBehavior = impl DefaultBehavior<Self::Result>;

            fn default_behavior() -> Self::DefaultBehavior {
                DefaultValue
            }

            fn visit_logical_join(&mut self, plan: &LogicalJoin) -> Self::Result {
                self.visit(plan.left());
                self.visit(plan.right());
                if self.warning_msg.is_none() && plan.should_be_temporal_join() {
                    self.warning_msg =
                        Some("snapshot backfill disabled due to temporal join".to_owned());
                }
            }

            fn visit_logical_source(&mut self, plan: &LogicalSource) -> Self::Result {
                if self.warning_msg.is_none() && plan.is_shared_source() {
                    self.warning_msg = Some(format!(
                        "snapshot backfill disabled due to using shared source {:?}",
                        plan.core.catalog.as_ref().map(|c| &c.name)
                    ));
                }
            }
        }
        let mut forbid_snapshot = ForbidSnapshotBackfill { warning_msg: None };
        forbid_snapshot.visit(self.clone());
        forbid_snapshot.warning_msg
    }
}

impl ColPrunable for LogicalPlanRef {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> LogicalPlanRef {
        let res = if ctx.is_running() {
            self.prune_col_inner(required_cols, ctx)
        } else {
            ctx.run(self.clone(), required_cols)
        };
        #[cfg(debug_assertions)]
        super::heuristic_optimizer::HeuristicOptimizer::check_equivalent_plan(
            "column pruning",
            &LogicalProject::with_out_col_idx(self.clone(), required_cols.iter().cloned()).into(),
            &res,
        );
        res
    }
}

impl PredicatePushdown for LogicalPlanRef {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> LogicalPlanRef {
        #[cfg(debug_assertions)]
        let predicate_clone = predicate.clone();

        let res = if ctx.is_running() {
            self.predicate_pushdown_inner(predicate, ctx)
        } else {
            ctx.run(self.clone(), predicate)
        };

        #[cfg(debug_assertions)]
        super::heuristic_optimizer::HeuristicOptimizer::check_equivalent_plan(
            "predicate push down",
            &LogicalFilter::new(self.clone(), predicate_clone).into(),
            &res,
        );

        res
    }
}

impl<C: ConventionMarker> PlanRef<C> {
    pub fn clone_root_with_inputs(&self, inputs: &[PlanRef<C>]) -> PlanRef<C> {
        if let Some(share) = self.as_share_node() {
            assert_eq!(inputs.len(), 1);
            share.replace_input(inputs[0].clone())
        } else {
            // Dispatch to dyn PlanNode instead of PlanRef.
            let dyn_t = self.deref();
            dyn_t.clone_with_inputs(inputs)
        }
    }
}

/// Implement again for the `dyn` newtype wrapper.
impl<C: ConventionMarker> PlanRef<C> {
    pub fn node_type(&self) -> C::PlanNodeType {
        self.0.node_type()
    }

    pub fn plan_base(&self) -> &PlanBase<C> {
        self.0.plan_base()
    }
}

/// Allow access to all fields defined in [`GenericPlanRef`] for the type-erased plan node.
// TODO: may also implement on `dyn PlanNode` directly.
impl<C: ConventionMarker> GenericPlanRef for PlanRef<C> {
    fn id(&self) -> PlanNodeId {
        self.plan_base().id()
    }

    fn schema(&self) -> &Schema {
        self.plan_base().schema()
    }

    fn stream_key(&self) -> Option<&[usize]> {
        self.plan_base().stream_key()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.plan_base().ctx()
    }

    fn functional_dependency(&self) -> &FunctionalDependencySet {
        self.plan_base().functional_dependency()
    }
}

/// Allow access to all fields defined in [`PhysicalPlanRef`] for the type-erased plan node.
// TODO: may also implement on `dyn PlanNode` directly.
impl PhysicalPlanRef for BatchPlanRef {
    fn distribution(&self) -> &Distribution {
        self.plan_base().distribution()
    }
}

impl PhysicalPlanRef for StreamPlanRef {
    fn distribution(&self) -> &Distribution {
        self.plan_base().distribution()
    }
}

/// Allow access to all fields defined in [`StreamPlanNodeMetadata`] for the type-erased plan node.
// TODO: may also implement on `dyn PlanNode` directly.
impl StreamPlanNodeMetadata for StreamPlanRef {
    fn stream_kind(&self) -> StreamKind {
        self.plan_base().stream_kind()
    }

    fn emit_on_window_close(&self) -> bool {
        self.plan_base().emit_on_window_close()
    }

    fn watermark_columns(&self) -> &WatermarkColumns {
        self.plan_base().watermark_columns()
    }

    fn columns_monotonicity(&self) -> &MonotonicityMap {
        self.plan_base().columns_monotonicity()
    }
}

/// Allow access to all fields defined in [`BatchPlanNodeMetadata`] for the type-erased plan node.
// TODO: may also implement on `dyn PlanNode` directly.
impl BatchPlanNodeMetadata for BatchPlanRef {
    fn order(&self) -> &Order {
        self.plan_base().order()
    }

    fn orders(&self) -> Vec<Order> {
        self.plan_base().orders()
    }
}

/// In order to let expression display id started from 1 for explaining, hidden column names and
/// other places. We will reset expression display id to 0 and clone the whole plan to reset the
/// schema.
pub fn reorganize_elements_id<C: ConventionMarker>(plan: PlanRef<C>) -> PlanRef<C> {
    let backup = plan.ctx().backup_elem_ids();
    plan.ctx().reset_elem_ids();
    let plan = PlanCloner::clone_whole_plan(plan);
    plan.ctx().restore_elem_ids(backup);
    plan
}

pub trait Explain {
    /// Write explain the whole plan tree.
    fn explain<'a>(&self) -> Pretty<'a>;

    /// Write explain the whole plan tree with node id.
    fn explain_with_id<'a>(&self) -> Pretty<'a>;

    /// Explain the plan node and return a string.
    fn explain_to_string(&self) -> String;

    /// Explain the plan node and return a json string.
    fn explain_to_json(&self) -> String;

    /// Explain the plan node and return a xml string.
    fn explain_to_xml(&self) -> String;

    /// Explain the plan node and return a yaml string.
    fn explain_to_yaml(&self) -> String;

    /// Explain the plan node and return a dot format string.
    fn explain_to_dot(&self) -> String;
}

impl<C: ConventionMarker> Explain for PlanRef<C> {
    /// Write explain the whole plan tree.
    fn explain<'a>(&self) -> Pretty<'a> {
        let mut node = self.distill();
        let inputs = self.inputs();
        for input in inputs.iter().peekable() {
            node.children.push(input.explain());
        }
        Pretty::Record(node)
    }

    /// Write explain the whole plan tree with node id.
    fn explain_with_id<'a>(&self) -> Pretty<'a> {
        let node_id = self.id();
        let mut node = self.distill();
        // NOTE(kwannoel): Can lead to poor performance if plan is very large,
        // but we want to show the id first.
        node.fields
            .insert(0, ("id".into(), Pretty::display(&node_id.0)));
        let inputs = self.inputs();
        for input in inputs.iter().peekable() {
            node.children.push(input.explain_with_id());
        }
        Pretty::Record(node)
    }

    /// Explain the plan node and return a string.
    fn explain_to_string(&self) -> String {
        let plan = reorganize_elements_id(self.clone());

        let mut output = String::with_capacity(2048);
        let mut config = pretty_config();
        config.unicode(&mut output, &plan.explain());
        output
    }

    /// Explain the plan node and return a json string.
    fn explain_to_json(&self) -> String {
        let plan = reorganize_elements_id(self.clone());
        let explain_ir = plan.explain();
        serde_json::to_string_pretty(&PrettySerde(explain_ir, true))
            .expect("failed to serialize plan to json")
    }

    /// Explain the plan node and return a xml string.
    fn explain_to_xml(&self) -> String {
        let plan = reorganize_elements_id(self.clone());
        let explain_ir = plan.explain();
        quick_xml::se::to_string(&PrettySerde(explain_ir, true))
            .expect("failed to serialize plan to xml")
    }

    /// Explain the plan node and return a yaml string.
    fn explain_to_yaml(&self) -> String {
        let plan = reorganize_elements_id(self.clone());
        let explain_ir = plan.explain();
        serde_yaml::to_string(&PrettySerde(explain_ir, true))
            .expect("failed to serialize plan to yaml")
    }

    /// Explain the plan node and return a dot format string.
    fn explain_to_dot(&self) -> String {
        let plan = reorganize_elements_id(self.clone());
        let explain_ir = plan.explain_with_id();
        let mut graph = Graph::<String, String>::new();
        let mut nodes = HashMap::new();
        build_graph_from_pretty(&explain_ir, &mut graph, &mut nodes, None);
        let dot = Dot::with_config(&graph, &[Config::EdgeNoLabel]);
        dot.to_string()
    }
}

impl<C: ConventionMarker> PlanRef<C> {
    pub fn as_share_node(&self) -> Option<&C::ShareNode> {
        C::as_share(self)
    }
}

pub(crate) fn pretty_config() -> PrettyConfig {
    PrettyConfig {
        indent: 3,
        need_boundaries: false,
        width: 2048,
        reduced_spaces: true,
    }
}

macro_rules! impl_generic_plan_ref_method {
    ($($convention:ident),+) => {
        paste! {
            $(
                /// Directly implement methods for `PlanNode` to access the fields defined in [`GenericPlanRef`].
                impl dyn [<$convention PlanNode>] {
                    pub fn id(&self) -> PlanNodeId {
                        self.plan_base().id()
                    }

                    pub fn ctx(&self) -> OptimizerContextRef {
                        self.plan_base().ctx().clone()
                    }

                    pub fn schema(&self) -> &Schema {
                        self.plan_base().schema()
                    }

                    pub fn stream_key(&self) -> Option<&[usize]> {
                        self.plan_base().stream_key()
                    }

                    pub fn functional_dependency(&self) -> &FunctionalDependencySet {
                        self.plan_base().functional_dependency()
                    }

                    pub fn explain_myself_to_string(&self) -> String {
                        self.distill_to_string()
                    }
                }
            )+
        }
    };
}

impl_generic_plan_ref_method!(Batch, Stream, Logical);

/// Recursion depth threshold for plan node visitor to send notice to user.
pub const PLAN_DEPTH_THRESHOLD: usize = 30;
/// Notice message for plan node visitor to send to user when the depth threshold is reached.
pub const PLAN_TOO_DEEP_NOTICE: &str = "The plan is too deep. \
Consider simplifying or splitting the query if you encounter any issues.";

impl dyn StreamPlanNode {
    /// Serialize the plan node and its children to a stream plan proto.
    ///
    /// Note that some operators has their own implementation of `to_stream_prost`. We have a
    /// hook inside to do some ad-hoc things.
    pub fn to_stream_prost(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<PbStreamPlan> {
        recursive::tracker!().recurse(|t| {
            if t.depth_reaches(PLAN_DEPTH_THRESHOLD) {
                notice_to_user(PLAN_TOO_DEEP_NOTICE);
            }

            use stream::prelude::*;

            if let Some(stream_table_scan) = self.as_stream_table_scan() {
                return stream_table_scan.adhoc_to_stream_prost(state);
            }
            if let Some(stream_cdc_table_scan) = self.as_stream_cdc_table_scan() {
                return stream_cdc_table_scan.adhoc_to_stream_prost(state);
            }
            if let Some(stream_source_scan) = self.as_stream_source_scan() {
                return stream_source_scan.adhoc_to_stream_prost(state);
            }
            if let Some(stream_share) = self.as_stream_share() {
                return stream_share.adhoc_to_stream_prost(state);
            }
            if let Some(writer) = self.as_stream_iceberg_with_pk_index_writer() {
                return writer.adhoc_to_stream_prost(state);
            }

            let node = Some(self.try_to_stream_prost_body(state)?);
            let input = self
                .inputs()
                .into_iter()
                .map(|plan| plan.to_stream_prost(state))
                .try_collect()?;
            // TODO: support pk_indices and operator_id
            Ok(PbStreamPlan {
                input,
                identity: self.explain_myself_to_string(),
                node_body: node,
                operator_id: self.id().to_stream_node_operator_id(),
                stream_key: self
                    .stream_key()
                    .unwrap_or_default()
                    .iter()
                    .map(|x| *x as u32)
                    .collect(),
                fields: self.schema().to_prost(),
                stream_kind: self.plan_base().stream_kind().to_protobuf() as i32,
            })
        })
    }
}

impl dyn BatchPlanNode {
    /// Serialize the plan node and its children to a batch plan proto.
    pub fn to_batch_prost(&self) -> SchedulerResult<PbBatchPlan> {
        self.to_batch_prost_identity(true)
    }

    /// Serialize the plan node and its children to a batch plan proto without the identity field
    /// (for testing).
    pub fn to_batch_prost_identity(&self, identity: bool) -> SchedulerResult<PbBatchPlan> {
        recursive::tracker!().recurse(|t| {
            if t.depth_reaches(PLAN_DEPTH_THRESHOLD) {
                notice_to_user(PLAN_TOO_DEEP_NOTICE);
            }

            let node_body = Some(self.try_to_batch_prost_body()?);
            let children = self
                .inputs()
                .into_iter()
                .map(|plan| plan.to_batch_prost_identity(identity))
                .try_collect()?;
            Ok(PbBatchPlan {
                children,
                identity: if identity {
                    self.explain_myself_to_string()
                } else {
                    "".into()
                },
                node_body,
            })
        })
    }
}

mod plan_base;
pub use plan_base::*;
#[macro_use]
mod plan_tree_node;
pub use plan_tree_node::*;
mod col_pruning;
pub use col_pruning::*;
mod expr_rewritable;
pub use expr_rewritable::*;
mod expr_visitable;

mod convert;
pub use convert::*;
mod eq_join_predicate;
pub use eq_join_predicate::*;
mod to_prost;
pub use to_prost::*;
mod predicate_pushdown;
pub use predicate_pushdown::*;
mod merge_eq_nodes;
pub use merge_eq_nodes::*;

pub mod batch;
pub mod generic;
pub mod stream;

pub use generic::{PlanAggCall, PlanAggCallDisplay};

mod batch_delete;
mod batch_exchange;
mod batch_expand;
mod batch_filter;
mod batch_get_channel_delta_stats;
mod batch_group_topn;
mod batch_hash_agg;
mod batch_hash_join;
mod batch_hop_window;
mod batch_insert;
mod batch_limit;
mod batch_log_seq_scan;
mod batch_lookup_join;
mod batch_max_one_row;
mod batch_nested_loop_join;
mod batch_over_window;
mod batch_project;
mod batch_project_set;
mod batch_seq_scan;
mod batch_simple_agg;
mod batch_sort;
mod batch_sort_agg;
mod batch_source;
mod batch_sys_seq_scan;
mod batch_table_function;
mod batch_topn;
mod batch_union;
mod batch_update;
mod batch_values;
mod logical_agg;
mod logical_apply;
mod logical_cdc_scan;
mod logical_changelog;
mod logical_dedup;
mod logical_delete;
mod logical_except;
mod logical_expand;
mod logical_filter;
mod logical_gap_fill;
mod logical_get_channel_delta_stats;
mod logical_hop_window;
mod logical_insert;
mod logical_intersect;
mod logical_join;
mod logical_kafka_scan;
mod logical_limit;
mod logical_locality_provider;
mod logical_match_recognize;
mod logical_max_one_row;
mod logical_multi_join;
mod logical_now;
mod logical_over_window;
mod logical_project;
mod logical_project_set;
mod logical_scan;
mod logical_share;
mod logical_source;
mod logical_sys_scan;
mod logical_table_function;
mod logical_topn;
mod logical_union;
mod logical_update;
mod logical_values;
mod stream_asof_join;
mod stream_changelog;
mod stream_dedup;
mod stream_delta_join;
mod stream_dml;
mod stream_dynamic_filter;
mod stream_eowc_gap_fill;
mod stream_eowc_over_window;
mod stream_exchange;
mod stream_expand;
mod stream_filter;
mod stream_fs_fetch;
mod stream_gap_fill;
mod stream_global_approx_percentile;
mod stream_group_topn;
mod stream_hash_agg;
mod stream_hash_join;
mod stream_hop_window;
mod stream_iceberg_with_pk_index_position_delete_merger;
mod stream_iceberg_with_pk_index_writer;
mod stream_join_common;
mod stream_local_approx_percentile;
mod stream_locality_provider;
mod stream_match_recognize;
mod stream_materialize;
mod stream_materialized_exprs;
mod stream_now;
mod stream_over_window;
mod stream_project;
mod stream_project_set;
mod stream_row_id_gen;
mod stream_row_merge;
mod stream_simple_agg;
mod stream_sink;
mod stream_source;
mod stream_source_scan;
mod stream_stateless_simple_agg;
mod stream_sync_log_store;
mod stream_table_scan;
mod stream_topn;
mod stream_union;
mod stream_values;
mod stream_watermark_filter;
mod stream_watermark_sort;

mod batch_file_scan;
mod batch_iceberg_metadata_scan;
mod batch_iceberg_scan;
mod batch_kafka_scan;
mod batch_postgres_query;

mod batch_mysql_query;
mod derive;
mod logical_file_scan;
mod logical_iceberg_intermediate_scan;
mod logical_iceberg_metadata_scan;
mod logical_iceberg_scan;
mod logical_postgres_query;

mod batch_vector_search;
mod logical_mysql_query;
mod logical_vector_search;
mod logical_vector_search_lookup_join;
mod stream_cdc_table_scan;
mod stream_share;
mod stream_temporal_join;
mod stream_upstream_sink_union;
mod stream_vector_index_lookup_join;
mod stream_vector_index_write;
pub mod utils;

pub use batch_delete::BatchDelete;
pub use batch_exchange::BatchExchange;
pub use batch_expand::BatchExpand;
pub use batch_file_scan::BatchFileScan;
pub use batch_filter::BatchFilter;
pub use batch_get_channel_delta_stats::BatchGetChannelDeltaStats;
pub use batch_group_topn::BatchGroupTopN;
pub use batch_hash_agg::BatchHashAgg;
pub use batch_hash_join::BatchHashJoin;
pub use batch_hop_window::BatchHopWindow;
pub use batch_iceberg_metadata_scan::BatchIcebergMetadataScan;
pub use batch_iceberg_scan::BatchIcebergScan;
pub use batch_insert::BatchInsert;
pub use batch_kafka_scan::BatchKafkaScan;
pub use batch_limit::BatchLimit;
pub use batch_log_seq_scan::BatchLogSeqScan;
pub use batch_lookup_join::BatchLookupJoin;
pub use batch_max_one_row::BatchMaxOneRow;
pub use batch_mysql_query::BatchMySqlQuery;
pub use batch_nested_loop_join::BatchNestedLoopJoin;
pub use batch_over_window::BatchOverWindow;
pub use batch_postgres_query::BatchPostgresQuery;
pub use batch_project::BatchProject;
pub use batch_project_set::BatchProjectSet;
pub use batch_seq_scan::BatchSeqScan;
pub use batch_simple_agg::BatchSimpleAgg;
pub use batch_sort::BatchSort;
pub use batch_sort_agg::BatchSortAgg;
pub use batch_source::BatchSource;
pub use batch_sys_seq_scan::BatchSysSeqScan;
pub use batch_table_function::BatchTableFunction;
pub use batch_topn::BatchTopN;
pub use batch_union::BatchUnion;
pub use batch_update::BatchUpdate;
pub use batch_values::BatchValues;
pub use batch_vector_search::BatchVectorSearch;
pub use logical_agg::LogicalAgg;
pub use logical_apply::LogicalApply;
pub use logical_cdc_scan::LogicalCdcScan;
pub use logical_changelog::LogicalChangeLog;
pub use logical_dedup::LogicalDedup;
pub use logical_delete::LogicalDelete;
pub use logical_except::LogicalExcept;
pub use logical_expand::LogicalExpand;
pub use logical_file_scan::LogicalFileScan;
pub use logical_filter::LogicalFilter;
pub use logical_gap_fill::LogicalGapFill;
pub use logical_get_channel_delta_stats::LogicalGetChannelDeltaStats;
pub use logical_hop_window::LogicalHopWindow;
pub use logical_iceberg_intermediate_scan::{HummockRewriteInfo, LogicalIcebergIntermediateScan};
pub use logical_iceberg_metadata_scan::LogicalIcebergMetadataScan;
pub use logical_iceberg_scan::LogicalIcebergScan;
pub use logical_insert::LogicalInsert;
pub use logical_intersect::LogicalIntersect;
pub use logical_join::LogicalJoin;
pub use logical_kafka_scan::LogicalKafkaScan;
pub use logical_limit::LogicalLimit;
pub use logical_locality_provider::LogicalLocalityProvider;
pub use logical_match_recognize::LogicalMatchRecognize;
pub use logical_max_one_row::LogicalMaxOneRow;
pub use logical_multi_join::{LogicalMultiJoin, LogicalMultiJoinBuilder};
pub use logical_mysql_query::LogicalMySqlQuery;
pub use logical_now::LogicalNow;
pub use logical_over_window::LogicalOverWindow;
pub use logical_postgres_query::LogicalPostgresQuery;
pub use logical_project::LogicalProject;
pub use logical_project_set::LogicalProjectSet;
pub use logical_scan::LogicalScan;
pub use logical_share::LogicalShare;
pub use logical_source::LogicalSource;
pub use logical_sys_scan::LogicalSysScan;
pub use logical_table_function::LogicalTableFunction;
pub use logical_topn::LogicalTopN;
pub use logical_union::LogicalUnion;
pub use logical_update::LogicalUpdate;
pub use logical_values::LogicalValues;
pub use logical_vector_search::LogicalVectorSearch;
pub use logical_vector_search_lookup_join::LogicalVectorSearchLookupJoin;
use risingwave_pb::id::StreamNodeLocalOperatorId;
pub use stream_asof_join::StreamAsOfJoin;
pub use stream_cdc_table_scan::StreamCdcTableScan;
pub use stream_changelog::StreamChangeLog;
pub use stream_dedup::StreamDedup;
pub use stream_delta_join::StreamDeltaJoin;
pub use stream_dml::StreamDml;
pub use stream_dynamic_filter::StreamDynamicFilter;
pub use stream_eowc_gap_fill::StreamEowcGapFill;
pub use stream_eowc_over_window::StreamEowcOverWindow;
pub use stream_exchange::StreamExchange;
pub use stream_expand::StreamExpand;
pub use stream_filter::StreamFilter;
pub use stream_fs_fetch::StreamFsFetch;
pub use stream_gap_fill::StreamGapFill;
pub use stream_global_approx_percentile::StreamGlobalApproxPercentile;
pub use stream_group_topn::StreamGroupTopN;
pub use stream_hash_agg::StreamHashAgg;
pub use stream_hash_join::StreamHashJoin;
pub use stream_hop_window::StreamHopWindow;
pub use stream_iceberg_with_pk_index_position_delete_merger::StreamIcebergWithPkIndexPositionDeleteMerger;
pub use stream_iceberg_with_pk_index_writer::StreamIcebergWithPkIndexWriter;
use stream_join_common::StreamJoinCommon;
pub use stream_local_approx_percentile::StreamLocalApproxPercentile;
pub use stream_locality_provider::StreamLocalityProvider;
pub use stream_match_recognize::StreamMatchRecognize;
pub use stream_materialize::StreamMaterialize;
pub use stream_materialized_exprs::StreamMaterializedExprs;
pub use stream_now::StreamNow;
pub use stream_over_window::StreamOverWindow;
pub use stream_project::StreamProject;
pub use stream_project_set::StreamProjectSet;
pub use stream_row_id_gen::StreamRowIdGen;
pub use stream_row_merge::StreamRowMerge;
pub use stream_share::StreamShare;
pub use stream_simple_agg::StreamSimpleAgg;
pub use stream_sink::{IcebergPartitionInfo, PartitionComputeInfo, StreamSink};
pub use stream_source::StreamSource;
pub use stream_source_scan::StreamSourceScan;
pub use stream_stateless_simple_agg::StreamStatelessSimpleAgg;
pub use stream_sync_log_store::StreamSyncLogStore;
pub(crate) use stream_sync_log_store::ensure_sync_log_store_fragment_root;
pub use stream_table_scan::StreamTableScan;
pub use stream_temporal_join::StreamTemporalJoin;
pub use stream_topn::StreamTopN;
pub use stream_union::StreamUnion;
pub use stream_upstream_sink_union::StreamUpstreamSinkUnion;
pub use stream_values::StreamValues;
pub use stream_vector_index_lookup_join::StreamVectorIndexLookupJoin;
pub use stream_vector_index_write::StreamVectorIndexWrite;
pub use stream_watermark_filter::StreamWatermarkFilter;
pub use stream_watermark_sort::StreamWatermarkSort;

use crate::expr::{ExprRewriter, ExprVisitor, Literal};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_rewriter::PlanCloner;
use crate::optimizer::plan_visitor::{DefaultBehavior, DefaultValue, LogicalPlanVisitor};
use crate::scheduler::SchedulerResult;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::{ColIndexMapping, Condition, DynEq, DynHash, Endo, Layer, Visit};

/// `for_all_plan_nodes` includes all plan nodes. If you added a new plan node
/// inside the project, be sure to add here and in its conventions like `for_logical_plan_nodes`
///
/// Every tuple has two elements, where `{ convention, name }`
/// You can use it as follows
/// ```rust
/// macro_rules! use_plan {
///     ($({ $convention:ident, $name:ident }),*) => {};
/// }
/// risingwave_frontend::for_all_plan_nodes! { use_plan }
/// ```
/// See the following implementations for example.
#[macro_export]
macro_rules! for_all_plan_nodes {
    ($macro:path $(,$rest:tt)*) => {
        $macro! {
              { Logical, Agg }
            , { Logical, Apply }
            , { Logical, Filter }
            , { Logical, Project }
            , { Logical, Scan }
            , { Logical, CdcScan }
            , { Logical, SysScan }
            , { Logical, Source }
            , { Logical, Insert }
            , { Logical, Delete }
            , { Logical, Update }
            , { Logical, Join }
            , { Logical, Values }
            , { Logical, Limit }
            , { Logical, TopN }
            , { Logical, HopWindow }
            , { Logical, TableFunction }
            , { Logical, MultiJoin }
            , { Logical, Expand }
            , { Logical, ProjectSet }
            , { Logical, Union }
            , { Logical, OverWindow }
            , { Logical, Share }
            , { Logical, Now }
            , { Logical, Dedup }
            , { Logical, Intersect }
            , { Logical, Except }
            , { Logical, MatchRecognize }
            , { Logical, MaxOneRow }
            , { Logical, KafkaScan }
            , { Logical, IcebergScan }
            , { Logical, IcebergMetadataScan }
            , { Logical, IcebergIntermediateScan }
            , { Logical, ChangeLog }
            , { Logical, FileScan }
            , { Logical, PostgresQuery }
            , { Logical, MySqlQuery }
            , { Logical, GapFill }
            , { Logical, VectorSearch }
            , { Logical, GetChannelDeltaStats }
            , { Logical, LocalityProvider }
            , { Logical, VectorSearchLookupJoin }
            , { Batch, SimpleAgg }
            , { Batch, HashAgg }
            , { Batch, SortAgg }
            , { Batch, Project }
            , { Batch, Filter }
            , { Batch, Insert }
            , { Batch, Delete }
            , { Batch, Update }
            , { Batch, SeqScan }
            , { Batch, SysSeqScan }
            , { Batch, LogSeqScan }
            , { Batch, HashJoin }
            , { Batch, NestedLoopJoin }
            , { Batch, Values }
            , { Batch, Sort }
            , { Batch, Exchange }
            , { Batch, Limit }
            , { Batch, TopN }
            , { Batch, HopWindow }
            , { Batch, TableFunction }
            , { Batch, Expand }
            , { Batch, LookupJoin }
            , { Batch, ProjectSet }
            , { Batch, Union }
            , { Batch, GroupTopN }
            , { Batch, Source }
            , { Batch, OverWindow }
            , { Batch, MaxOneRow }
            , { Batch, KafkaScan }
            , { Batch, IcebergScan }
            , { Batch, IcebergMetadataScan }
            , { Batch, FileScan }
            , { Batch, PostgresQuery }
            , { Batch, MySqlQuery }
            , { Batch, GetChannelDeltaStats }
            , { Batch, VectorSearch }
            , { Stream, Project }
            , { Stream, Filter }
            , { Stream, TableScan }
            , { Stream, CdcTableScan }
            , { Stream, Sink }
            , { Stream, Source }
            , { Stream, SourceScan }
            , { Stream, HashJoin }
            , { Stream, Exchange }
            , { Stream, HashAgg }
            , { Stream, SimpleAgg }
            , { Stream, StatelessSimpleAgg }
            , { Stream, Materialize }
            , { Stream, TopN }
            , { Stream, HopWindow }
            , { Stream, DeltaJoin }
            , { Stream, Expand }
            , { Stream, DynamicFilter }
            , { Stream, ProjectSet }
            , { Stream, GroupTopN }
            , { Stream, Union }
            , { Stream, RowIdGen }
            , { Stream, Dml }
            , { Stream, Now }
            , { Stream, Share }
            , { Stream, WatermarkFilter }
            , { Stream, TemporalJoin }
            , { Stream, Values }
            , { Stream, Dedup }
            , { Stream, EowcOverWindow }
            , { Stream, WatermarkSort }
            , { Stream, MatchRecognize }
            , { Stream, OverWindow }
            , { Stream, FsFetch }
            , { Stream, ChangeLog }
            , { Stream, GlobalApproxPercentile }
            , { Stream, LocalApproxPercentile }
            , { Stream, RowMerge }
            , { Stream, AsOfJoin }
            , { Stream, SyncLogStore }
            , { Stream, MaterializedExprs }
            , { Stream, VectorIndexWrite }
            , { Stream, VectorIndexLookupJoin }
            , { Stream, UpstreamSinkUnion }
            , { Stream, LocalityProvider }
            , { Stream, EowcGapFill }
            , { Stream, GapFill }
            , { Stream, IcebergWithPkIndexWriter }
            , { Stream, IcebergWithPkIndexPositionDeleteMerger }
            $(,$rest)*
        }
    };
}

#[macro_export]
macro_rules! for_each_convention_all_plan_nodes {
    ($macro:path $(,$rest:tt)*) => {
        $crate::for_all_plan_nodes! {
            $crate::for_each_convention_all_plan_nodes
            , $macro
            $(,$rest)*
        }
    };
    (
        $( { Logical, $logical_name:ident } ),*
        , $( { Batch, $batch_name:ident } ),*
        , $( { Stream, $stream_name:ident } ),*
        , $macro:path $(,$rest:tt)*
    ) => {
        $macro! {
            {
                Logical, { $( $logical_name ),* },
                Batch, { $( $batch_name ),* },
                Stream, { $( $stream_name ),* }
            }
            $(,$rest)*
        }
    }
}

/// impl `PlanNodeType` fn for each node.
macro_rules! impl_plan_node_meta {
    ({
        $( $convention:ident, { $( $name:ident ),* }),*
    }) => {
        paste!{
            $(
                /// each enum value represent a `PlanNode` struct type, help us to dispatch and downcast
                #[derive(Copy, Clone, PartialEq, Debug, Hash, Eq, Serialize)]
                pub enum [<$convention PlanNodeType>] {
                    $( [<$convention $name>] ),*
                }
            )*
            $(
                $(impl PlanNodeMeta for [<$convention $name>] {
                    type Convention = $convention;
                    const NODE_TYPE: [<$convention PlanNodeType>] = [<$convention PlanNodeType>]::[<$convention $name>];

                    fn plan_base(&self) -> &PlanBase<$convention> {
                        &self.base
                    }
                }

                impl Deref for [<$convention $name>] {
                    type Target = PlanBase<$convention>;

                    fn deref(&self) -> &Self::Target {
                        &self.base
                    }
                })*
            )*
        }
    }
}

for_each_convention_all_plan_nodes! { impl_plan_node_meta }

macro_rules! impl_plan_node {
    ($({ $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl [<$convention PlanNode>] for [<$convention $name>] { })*
        }
    }
}

for_all_plan_nodes! { impl_plan_node }

/// impl plan node downcast fn for each node.
macro_rules! impl_down_cast_fn {
    ({
        $( $convention:ident, { $( $name:ident ),* }),*
    }) => {
        paste!{
            $(
                impl dyn [<$convention PlanNode>] {
                    $( pub fn [< as_ $convention:snake _ $name:snake>](&self) -> Option<&[<$convention $name>]> {
                        self.downcast_ref::<[<$convention $name>]>()
                    } )*
                }
            )*
        }
    }
}

for_each_convention_all_plan_nodes! { impl_down_cast_fn }
