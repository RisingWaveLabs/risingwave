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

use itertools::{EitherOrBoth, Itertools};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::plan_common::JoinType;

use super::{EqJoinPredicate, GenericPlanNode, GenericPlanRef};
use crate::TableCatalog;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::StreamPlanRef;
use crate::optimizer::plan_node::stream::StreamPlanNodeMetadata as _;
use crate::optimizer::plan_node::stream::prelude::*;
use crate::optimizer::plan_node::utils::TableCatalogBuilder;
use crate::optimizer::property::{FunctionalDependencySet, StreamKind};
use crate::utils::{ColIndexMapping, ColIndexMappingRewriteExt, Condition};

/// Join predicate stored in the join core.
///
/// - Logical joins keep the original [`Condition`] for optimizer rules.
/// - Physical joins keep a fixed [`EqJoinPredicate`] (eq keys are already extracted and must be
///   preserved even if condition simplification would otherwise drop them).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinOn {
    Condition(Condition),
    EqPredicate(EqJoinPredicate),
}

impl JoinOn {
    /// Get the join predicate as a [`Condition`].
    ///
    /// - For [`JoinOn::Condition`], this returns the stored condition.
    /// - For [`JoinOn::EqPredicate`], this converts the stored [`EqJoinPredicate`] back to a
    ///   condition.
    ///
    /// Prefer [`JoinOn::as_condition_ref`] if you only want the original logical condition and
    /// don't want any conversion.
    pub fn as_condition(&self) -> Condition {
        match self {
            JoinOn::Condition(cond) => cond.clone(),
            JoinOn::EqPredicate(pred) => pred.all_cond(),
        }
    }

    /// Borrow the original logical join condition, if any.
    ///
    /// Returns `None` for [`JoinOn::EqPredicate`], because physical plans store a fixed
    /// [`EqJoinPredicate`] instead of the original condition.
    pub fn as_condition_ref(&self) -> Option<&Condition> {
        match self {
            JoinOn::Condition(cond) => Some(cond),
            JoinOn::EqPredicate(_) => None,
        }
    }

    /// Borrow the fixed [`EqJoinPredicate`], if any.
    ///
    /// Returns `None` for [`JoinOn::Condition`]. If the caller needs eq keys, it should extract
    /// them via [`EqJoinPredicate::create`] (potentially after rewriting/simplifying the
    /// condition).
    pub fn as_eq_predicate_ref(&self) -> Option<&EqJoinPredicate> {
        match self {
            JoinOn::Condition(_) => None,
            JoinOn::EqPredicate(pred) => Some(pred),
        }
    }

    /// Rewrite expressions inside the predicate.
    ///
    /// For [`JoinOn::EqPredicate`], eq keys are treated as fixed (they are expected to be plain
    /// input refs), so only the "other" condition is rewritten.
    pub fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        match self {
            JoinOn::Condition(cond) => {
                *cond = cond.clone().rewrite_expr(r);
            }
            JoinOn::EqPredicate(pred) => {
                *pred = pred.rewrite_exprs(r);
            }
        }
    }

    /// Visit expressions inside the predicate.
    ///
    /// For [`JoinOn::EqPredicate`], only non-eq conditions are visited.
    pub fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        match self {
            JoinOn::Condition(cond) => cond.visit_expr(v),
            // eq keys are fixed and contain only input refs; only visit non-eq conditions.
            JoinOn::EqPredicate(pred) => pred.visit_exprs(v),
        }
    }
}

/// [`Join`] combines two relations according to some condition.
///
/// Each output row has fields from the left and right inputs. The set of output rows is a subset
/// of the cartesian product of the two inputs; precisely which subset depends on the join
/// condition. In addition, the output columns are a subset of the columns of the left and
/// right columns, dependent on the output indices provided. A repeat output index is illegal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Join<PlanRef> {
    pub left: PlanRef,
    pub right: PlanRef,
    pub on: JoinOn,
    pub join_type: JoinType,
    pub output_indices: Vec<usize>,
}

pub(crate) fn has_repeated_element(slice: &[usize]) -> bool {
    (1..slice.len()).any(|i| slice[i..].contains(&slice[i - 1]))
}

impl<PlanRef: GenericPlanRef> Join<PlanRef> {
    pub(crate) fn clone_with_inputs<OtherPlanRef>(
        &self,
        left: OtherPlanRef,
        right: OtherPlanRef,
    ) -> Join<OtherPlanRef> {
        Join {
            left,
            right,
            on: self.on.clone(),
            join_type: self.join_type,
            output_indices: self.output_indices.clone(),
        }
    }

    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.on.rewrite_exprs(r);
    }

    pub(crate) fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.on.visit_exprs(v);
    }

    pub fn eq_indexes(&self) -> Vec<(usize, usize)> {
        let left_len = self.left.schema().len();
        let right_len = self.right.schema().len();
        match &self.on {
            JoinOn::Condition(on) => {
                EqJoinPredicate::create(left_len, right_len, on.clone()).eq_indexes()
            }
            JoinOn::EqPredicate(pred) => pred.eq_indexes(),
        }
    }

    pub fn new(
        left: PlanRef,
        right: PlanRef,
        on: Condition,
        join_type: JoinType,
        output_indices: Vec<usize>,
    ) -> Self {
        // We cannot deal with repeated output indices in join
        debug_assert!(!has_repeated_element(&output_indices));
        Self {
            left,
            right,
            on: JoinOn::Condition(on),
            join_type,
            output_indices,
        }
    }

    pub fn new_with_eq_predicate(
        left: PlanRef,
        right: PlanRef,
        eq_join_predicate: EqJoinPredicate,
        join_type: JoinType,
        output_indices: Vec<usize>,
    ) -> Self {
        debug_assert!(!has_repeated_element(&output_indices));
        Self {
            left,
            right,
            on: JoinOn::EqPredicate(eq_join_predicate),
            join_type,
            output_indices,
        }
    }
}

impl Join<StreamPlanRef> {
    pub fn stream_kind(&self) -> Result<StreamKind> {
        let left_kind = reject_upsert_input!(self.left, "Join");
        let right_kind = reject_upsert_input!(self.right, "Join");

        // Inner join won't change the append-only behavior of the stream. The rest might.
        if let JoinType::Inner | JoinType::AsofInner = self.join_type
            && let StreamKind::AppendOnly = left_kind
            && let StreamKind::AppendOnly = right_kind
        {
            Ok(StreamKind::AppendOnly)
        } else {
            Ok(StreamKind::Retract)
        }
    }

    /// Return stream hash join internal table catalog and degree table catalog.
    pub fn infer_internal_and_degree_table_catalog(
        input: StreamPlanRef,
        join_key_indices: Vec<usize>,
        dk_indices_in_jk: Vec<usize>,
    ) -> (TableCatalog, TableCatalog, Vec<usize>) {
        let schema = input.schema();

        let internal_table_dist_keys = dk_indices_in_jk
            .iter()
            .map(|idx| join_key_indices[*idx])
            .collect_vec();

        let degree_table_dist_keys = dk_indices_in_jk.clone();

        // The pk of hash join internal and degree table should be join_key + input_pk.
        let join_key_len = join_key_indices.len();
        let mut pk_indices = join_key_indices;

        // dedup the pk in dist key..
        let mut deduped_input_pk_indices = vec![];
        for input_pk_idx in input.stream_key().unwrap() {
            if !pk_indices.contains(input_pk_idx)
                && !deduped_input_pk_indices.contains(input_pk_idx)
            {
                deduped_input_pk_indices.push(*input_pk_idx);
            }
        }

        pk_indices.extend(deduped_input_pk_indices.clone());

        // Build internal table
        let mut internal_table_catalog_builder = TableCatalogBuilder::default();
        let internal_columns_fields = schema.fields().to_vec();

        internal_columns_fields.iter().for_each(|field| {
            internal_table_catalog_builder.add_column(field);
        });
        pk_indices.iter().for_each(|idx| {
            internal_table_catalog_builder.add_order_column(*idx, OrderType::ascending())
        });

        // Build degree table.
        let mut degree_table_catalog_builder = TableCatalogBuilder::default();

        let degree_column_field = Field::with_name(DataType::Int64, "_degree");

        pk_indices.iter().enumerate().for_each(|(order_idx, idx)| {
            degree_table_catalog_builder.add_column(&internal_columns_fields[*idx]);
            degree_table_catalog_builder.add_order_column(order_idx, OrderType::ascending());
        });
        degree_table_catalog_builder.add_column(&degree_column_field);
        degree_table_catalog_builder
            .set_value_indices(vec![degree_table_catalog_builder.columns().len() - 1]);

        internal_table_catalog_builder.set_dist_key_in_pk(dk_indices_in_jk.clone());
        degree_table_catalog_builder.set_dist_key_in_pk(dk_indices_in_jk);

        (
            internal_table_catalog_builder.build(internal_table_dist_keys, join_key_len),
            degree_table_catalog_builder.build(degree_table_dist_keys, join_key_len),
            deduped_input_pk_indices,
        )
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Join<PlanRef> {
    fn schema(&self) -> Schema {
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();
        let i2l = self.i2l_col_mapping();
        let i2r = self.i2r_col_mapping();
        let fields = self
            .output_indices
            .iter()
            .map(|&i| match (i2l.try_map(i), i2r.try_map(i)) {
                (Some(l_i), None) => left_schema.fields()[l_i].clone(),
                (None, Some(r_i)) => right_schema.fields()[r_i].clone(),
                _ => panic!(
                    "left len {}, right len {}, i {}, lmap {:?}, rmap {:?}",
                    left_schema.len(),
                    right_schema.len(),
                    i,
                    i2l,
                    i2r
                ),
            })
            .collect();
        Schema { fields }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        let eq_indexes = self.eq_indexes();
        let left_pk = self.left.stream_key()?;
        let right_pk = self.right.stream_key()?;
        let l2i = self.l2i_col_mapping();
        let r2i = self.r2i_col_mapping();
        let full_out_col_num = self.internal_column_num();
        let i2o = ColIndexMapping::with_remaining_columns(&self.output_indices, full_out_col_num);

        // Collect PKs in internal column space (without applying i2o mapping yet)
        let mut pk_indices_internal = left_pk
            .iter()
            .map(|index| l2i.try_map(*index))
            .chain(right_pk.iter().map(|index| r2i.try_map(*index)))
            .flatten()
            .collect::<Vec<_>>();

        let either_or_both = self.add_which_join_key_to_pk();

        for (lk, rk) in eq_indexes {
            match either_or_both {
                EitherOrBoth::Left(_) => {
                    // Remove right-side join-key column from pk_indices_internal.
                    // This may happen when right-side join-key is included in right-side PK.
                    // e.g. select a, b where a.bid = b.id
                    // Here the pk_indices should be [a.id, a.bid] instead of [a.id, b.id, a.bid],
                    // because b.id = a.bid, so either of them would be enough.
                    if let Some(rk_internal) = r2i.try_map(rk) {
                        pk_indices_internal.retain(|&x| x != rk_internal);
                    }
                    // Add left-side join-key column in pk_indices_internal
                    if let Some(lk_internal) = l2i.try_map(lk)
                        && !pk_indices_internal.contains(&lk_internal)
                    {
                        pk_indices_internal.push(lk_internal);
                    }
                }
                EitherOrBoth::Right(_) => {
                    // Remove left-side join-key column from pk_indices_internal
                    // See the example above
                    if let Some(lk_internal) = l2i.try_map(lk) {
                        pk_indices_internal.retain(|&x| x != lk_internal);
                    }
                    // Add right-side join-key column in pk_indices_internal
                    if let Some(rk_internal) = r2i.try_map(rk)
                        && !pk_indices_internal.contains(&rk_internal)
                    {
                        pk_indices_internal.push(rk_internal);
                    }
                }
                EitherOrBoth::Both(_, _) => {
                    if let Some(lk_internal) = l2i.try_map(lk)
                        && !pk_indices_internal.contains(&lk_internal)
                    {
                        pk_indices_internal.push(lk_internal);
                    }
                    if let Some(rk_internal) = r2i.try_map(rk)
                        && !pk_indices_internal.contains(&rk_internal)
                    {
                        pk_indices_internal.push(rk_internal);
                    }
                }
            };
        }

        // Now apply i2o mapping to get output indices
        let pk_indices = pk_indices_internal
            .iter()
            .map(|&index| i2o.try_map(index))
            .collect::<Option<Vec<_>>>()?;

        Some(pk_indices)
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.left.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let left_len = self.left.schema().len();
        let right_len = self.right.schema().len();
        let left_fd_set = self.left.functional_dependency().clone();
        let right_fd_set = self.right.functional_dependency().clone();

        let full_out_col_num = self.internal_column_num();

        let get_new_left_fd_set = |left_fd_set: FunctionalDependencySet| {
            ColIndexMapping::with_shift_offset(left_len, 0)
                .composite(&ColIndexMapping::identity(full_out_col_num))
                .rewrite_functional_dependency_set(left_fd_set)
        };
        let get_new_right_fd_set = |right_fd_set: FunctionalDependencySet| {
            ColIndexMapping::with_shift_offset(right_len, left_len.try_into().unwrap())
                .rewrite_functional_dependency_set(right_fd_set)
        };
        let fd_set: FunctionalDependencySet = match self.join_type {
            JoinType::Inner | JoinType::AsofInner => {
                let mut fd_set = FunctionalDependencySet::new(full_out_col_num);
                for i in &self.on.as_condition().conjunctions {
                    if let Some((col, _)) = i.as_eq_const() {
                        fd_set.add_constant_columns(&[col.index()])
                    } else if let Some((left, right)) = i.as_eq_cond() {
                        fd_set.add_functional_dependency_by_column_indices(
                            &[left.index()],
                            &[right.index()],
                        );
                        fd_set.add_functional_dependency_by_column_indices(
                            &[right.index()],
                            &[left.index()],
                        );
                    }
                }
                get_new_left_fd_set(left_fd_set)
                    .into_dependencies()
                    .into_iter()
                    .chain(get_new_right_fd_set(right_fd_set).into_dependencies())
                    .for_each(|fd| fd_set.add_functional_dependency(fd));
                fd_set
            }
            JoinType::LeftOuter | JoinType::AsofLeftOuter => get_new_left_fd_set(left_fd_set),
            JoinType::RightOuter => get_new_right_fd_set(right_fd_set),
            JoinType::FullOuter => FunctionalDependencySet::new(full_out_col_num),
            JoinType::LeftSemi | JoinType::LeftAnti => left_fd_set,
            JoinType::RightSemi | JoinType::RightAnti => right_fd_set,
            JoinType::Unspecified => unreachable!(),
        };
        ColIndexMapping::with_remaining_columns(&self.output_indices, full_out_col_num)
            .rewrite_functional_dependency_set(fd_set)
    }
}

impl<PlanRef> Join<PlanRef> {
    pub fn decompose(self) -> (PlanRef, PlanRef, Condition, JoinType, Vec<usize>) {
        (
            self.left,
            self.right,
            self.on.as_condition(),
            self.join_type,
            self.output_indices,
        )
    }
}

impl<PlanRef: GenericPlanRef> Join<PlanRef> {
    pub fn full_out_col_num(left_len: usize, right_len: usize, join_type: JoinType) -> usize {
        match join_type {
            JoinType::Inner
            | JoinType::LeftOuter
            | JoinType::RightOuter
            | JoinType::FullOuter
            | JoinType::AsofInner
            | JoinType::AsofLeftOuter => left_len + right_len,
            JoinType::LeftSemi | JoinType::LeftAnti => left_len,
            JoinType::RightSemi | JoinType::RightAnti => right_len,
            JoinType::Unspecified => unreachable!(),
        }
    }

    pub fn with_full_output(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on: Condition,
    ) -> Self {
        let out_column_num =
            Self::full_out_col_num(left.schema().len(), right.schema().len(), join_type);
        Self {
            left,
            right,
            join_type,
            on: JoinOn::Condition(on),
            output_indices: (0..out_column_num).collect(),
        }
    }

    pub fn with_full_output_eq_predicate(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        eq_join_predicate: EqJoinPredicate,
    ) -> Self {
        let out_column_num =
            Self::full_out_col_num(left.schema().len(), right.schema().len(), join_type);
        Self {
            left,
            right,
            join_type,
            on: JoinOn::EqPredicate(eq_join_predicate),
            output_indices: (0..out_column_num).collect(),
        }
    }

    pub fn internal_column_num(&self) -> usize {
        Self::full_out_col_num(
            self.left.schema().len(),
            self.right.schema().len(),
            self.join_type,
        )
    }

    pub fn is_full_out(&self) -> bool {
        self.output_indices.len() == self.internal_column_num()
    }

    /// Get the Mapping of columnIndex from internal column index to left column index.
    pub fn i2l_col_mapping(&self) -> ColIndexMapping {
        let left_len = self.left.schema().len();
        let right_len = self.right.schema().len();

        match self.join_type {
            JoinType::Inner
            | JoinType::LeftOuter
            | JoinType::RightOuter
            | JoinType::FullOuter
            | JoinType::AsofInner
            | JoinType::AsofLeftOuter => {
                ColIndexMapping::identity_or_none(left_len + right_len, left_len)
            }

            JoinType::LeftSemi | JoinType::LeftAnti => ColIndexMapping::identity(left_len),
            JoinType::RightSemi | JoinType::RightAnti => {
                ColIndexMapping::empty(right_len, left_len)
            }
            JoinType::Unspecified => unreachable!(),
        }
    }

    /// Get the Mapping of columnIndex from internal column index to right column index.
    pub fn i2r_col_mapping(&self) -> ColIndexMapping {
        let left_len = self.left.schema().len();
        let right_len = self.right.schema().len();

        match self.join_type {
            JoinType::Inner
            | JoinType::LeftOuter
            | JoinType::RightOuter
            | JoinType::FullOuter
            | JoinType::AsofInner
            | JoinType::AsofLeftOuter => {
                ColIndexMapping::with_shift_offset(left_len + right_len, -(left_len as isize))
            }
            JoinType::LeftSemi | JoinType::LeftAnti => ColIndexMapping::empty(left_len, right_len),
            JoinType::RightSemi | JoinType::RightAnti => ColIndexMapping::identity(right_len),
            JoinType::Unspecified => unreachable!(),
        }
    }

    /// TODO: This function may can be merged with `i2l_col_mapping` in future.
    pub fn i2l_col_mapping_ignore_join_type(&self) -> ColIndexMapping {
        let left_len = self.left.schema().len();
        let right_len = self.right.schema().len();

        ColIndexMapping::identity_or_none(left_len + right_len, left_len)
    }

    /// TODO: This function may can be merged with `i2r_col_mapping` in future.
    pub fn i2r_col_mapping_ignore_join_type(&self) -> ColIndexMapping {
        let left_len = self.left.schema().len();
        let right_len = self.right.schema().len();

        ColIndexMapping::with_shift_offset(left_len + right_len, -(left_len as isize))
    }

    /// Get the Mapping of columnIndex from left column index to internal column index.
    pub fn l2i_col_mapping(&self) -> ColIndexMapping {
        self.i2l_col_mapping()
            .inverse()
            .expect("must be invertible")
    }

    /// Get the Mapping of columnIndex from right column index to internal column index.
    pub fn r2i_col_mapping(&self) -> ColIndexMapping {
        self.i2r_col_mapping()
            .inverse()
            .expect("must be invertible")
    }

    /// Get the Mapping of columnIndex from internal column index to output column index
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::with_remaining_columns(&self.output_indices, self.internal_column_num())
    }

    /// Get the Mapping of columnIndex from output column index to internal column index
    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        // If output_indices = [0, 0, 1], we should use it as `o2i_col_mapping` directly.
        // If we use `self.i2o_col_mapping().inverse()`, we will lose the first 0.
        ColIndexMapping::new(
            self.output_indices.iter().map(|x| Some(*x)).collect(),
            self.internal_column_num(),
        )
    }

    pub fn add_which_join_key_to_pk(&self) -> EitherOrBoth<(), ()> {
        match self.join_type {
            JoinType::Inner | JoinType::AsofInner => {
                // Theoretically adding either side is ok, but the distribution key of the inner
                // join derived based on the left side by default, so we choose the left side here
                // to ensure the pk comprises the distribution key.
                EitherOrBoth::Left(())
            }
            JoinType::LeftOuter
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::AsofLeftOuter => EitherOrBoth::Left(()),
            JoinType::RightSemi | JoinType::RightAnti | JoinType::RightOuter => {
                EitherOrBoth::Right(())
            }
            JoinType::FullOuter => EitherOrBoth::Both((), ()),
            JoinType::Unspecified => unreachable!(),
        }
    }

    pub fn concat_schema(&self) -> Schema {
        Schema::new(
            [
                self.left.schema().fields.clone(),
                self.right.schema().fields.clone(),
            ]
            .concat(),
        )
    }
}

/// Try to split and pushdown `predicate` into a into a join condition and into the inputs of the
/// join. Returns the pushed predicates. The pushed part will be removed from the original
/// predicate.
///
/// `InputRef`s in the right pushed condition are indexed by the right child's output schema.
pub fn push_down_into_join(
    predicate: &mut Condition,
    left_col_num: usize,
    right_col_num: usize,
    ty: JoinType,
    push_temporal_predicate: bool,
) -> (Condition, Condition, Condition) {
    let (left, right) = push_down_to_inputs(
        predicate,
        left_col_num,
        right_col_num,
        can_push_left_from_filter(ty),
        can_push_right_from_filter(ty),
        push_temporal_predicate,
    );

    let on = if can_push_on_from_filter(ty) {
        let mut conjunctions = std::mem::take(&mut predicate.conjunctions);

        if push_temporal_predicate {
            Condition { conjunctions }
        } else {
            // Do not push now on to the on, it will be pulled up into a filter instead.
            let on = Condition {
                conjunctions: conjunctions
                    .extract_if(.., |expr| expr.count_nows() == 0)
                    .collect(),
            };
            predicate.conjunctions = conjunctions;
            on
        }
    } else {
        Condition::true_cond()
    };
    (left, right, on)
}

/// Try to pushes parts of the join condition to its inputs. Returns the pushed predicates. The
/// pushed part will be removed from the original join predicate.
///
/// `InputRef`s in the right pushed condition are indexed by the right child's output schema.
pub fn push_down_join_condition(
    on_condition: &mut Condition,
    left_col_num: usize,
    right_col_num: usize,
    ty: JoinType,
    push_temporal_predicate: bool,
) -> (Condition, Condition) {
    push_down_to_inputs(
        on_condition,
        left_col_num,
        right_col_num,
        can_push_left_from_on(ty),
        can_push_right_from_on(ty),
        push_temporal_predicate,
    )
}

/// Try to split and pushdown `predicate` into a join's left/right child.
/// Returns the pushed predicates. The pushed part will be removed from the original predicate.
///
/// `InputRef`s in the right `Condition` are shifted by `-left_col_num`.
fn push_down_to_inputs(
    predicate: &mut Condition,
    left_col_num: usize,
    right_col_num: usize,
    push_left: bool,
    push_right: bool,
    push_temporal_predicate: bool,
) -> (Condition, Condition) {
    let mut conjunctions = std::mem::take(&mut predicate.conjunctions);
    let (mut left, right, mut others) = if push_temporal_predicate {
        Condition { conjunctions }.split(left_col_num, right_col_num)
    } else {
        let temporal_filter_cons = conjunctions
            .extract_if(.., |e| e.count_nows() != 0)
            .collect_vec();
        let (left, right, mut others) =
            Condition { conjunctions }.split(left_col_num, right_col_num);

        others.conjunctions.extend(temporal_filter_cons);
        (left, right, others)
    };

    if !push_left {
        others.conjunctions.extend(left);
        left = Condition::true_cond();
    };

    let right = if push_right {
        let mut mapping = ColIndexMapping::with_shift_offset(
            left_col_num + right_col_num,
            -(left_col_num as isize),
        );
        right.rewrite_expr(&mut mapping)
    } else {
        others.conjunctions.extend(right);
        Condition::true_cond()
    };

    predicate.conjunctions = others.conjunctions;

    (left, right)
}

pub fn can_push_left_from_filter(ty: JoinType) -> bool {
    matches!(
        ty,
        JoinType::Inner
            | JoinType::LeftOuter
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::AsofInner
            | JoinType::AsofLeftOuter
    )
}

pub fn can_push_right_from_filter(ty: JoinType) -> bool {
    matches!(
        ty,
        JoinType::Inner
            | JoinType::RightOuter
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::AsofInner
    )
}

pub fn can_push_on_from_filter(ty: JoinType) -> bool {
    matches!(
        ty,
        JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
    )
}

pub fn can_push_left_from_on(ty: JoinType) -> bool {
    matches!(
        ty,
        JoinType::Inner | JoinType::RightOuter | JoinType::LeftSemi | JoinType::AsofInner
    )
}

pub fn can_push_right_from_on(ty: JoinType) -> bool {
    matches!(
        ty,
        JoinType::Inner
            | JoinType::LeftOuter
            | JoinType::RightSemi
            | JoinType::AsofInner
            | JoinType::AsofLeftOuter
    )
}
