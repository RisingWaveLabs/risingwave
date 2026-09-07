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

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::fmt::{self, Debug};
use std::ops::Bound;
use std::sync::LazyLock;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_common::types::{DataType, DefaultOrd, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::scan_range::{ScanRange, is_full_range};
use risingwave_common::util::sort_util::{OrderType, cmp_rows};

use crate::TableCatalog;
use crate::error::Result;
use crate::expr::{
    ExprDisplay, ExprImpl, ExprMutator, ExprRewriter, ExprType, ExprVisitor, FunctionCall,
    InequalityInputPair, InputRef, collect_input_refs, column_self_eq_eliminate,
    factorization_expr, fold_boolean_constant, push_down_not, to_conjunctions,
    try_get_bool_constant,
};
use crate::utils::condition::cast_compare::{ResultForCmp, ResultForEq};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Condition {
    /// Condition expressions in conjunction form (combined with `AND`)
    pub conjunctions: Vec<ExprImpl>,
}

impl IntoIterator for Condition {
    type IntoIter = std::vec::IntoIter<ExprImpl>;
    type Item = ExprImpl;

    fn into_iter(self) -> Self::IntoIter {
        self.conjunctions.into_iter()
    }
}

impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut conjunctions = self.conjunctions.iter();
        if let Some(expr) = conjunctions.next() {
            write!(f, "{:?}", expr)?;
        }
        if self.always_true() {
            write!(f, "true")?;
        } else {
            for expr in conjunctions {
                write!(f, " AND {:?}", expr)?;
            }
        }
        Ok(())
    }
}

impl Condition {
    pub fn with_expr(expr: ExprImpl) -> Self {
        let conjunctions = to_conjunctions(expr);

        Self { conjunctions }.simplify()
    }

    pub fn true_cond() -> Self {
        Self {
            conjunctions: vec![],
        }
    }

    pub fn false_cond() -> Self {
        Self {
            conjunctions: vec![ExprImpl::literal_bool(false)],
        }
    }

    pub fn always_true(&self) -> bool {
        self.conjunctions.is_empty()
    }

    pub fn always_false(&self) -> bool {
        static FALSE: LazyLock<ExprImpl> = LazyLock::new(|| ExprImpl::literal_bool(false));
        // There is at least one conjunction that is false.
        !self.conjunctions.is_empty() && self.conjunctions.contains(&*FALSE)
    }

    /// Convert condition to an expression. If always true, return `None`.
    pub fn as_expr_unless_true(&self) -> Option<ExprImpl> {
        if self.always_true() {
            None
        } else {
            Some(self.clone().into())
        }
    }

    #[must_use]
    pub fn and(self, other: Self) -> Self {
        let mut ret = self;
        ret.conjunctions.extend(other.conjunctions);
        ret.simplify()
    }

    #[must_use]
    pub fn or(self, other: Self) -> Self {
        let or_expr = ExprImpl::FunctionCall(
            FunctionCall::new_unchecked(
                ExprType::Or,
                vec![self.into(), other.into()],
                DataType::Boolean,
            )
            .into(),
        );
        let ret = Self::with_expr(or_expr);
        ret.simplify()
    }

    /// Split the condition expressions into 3 groups: left, right and others
    #[must_use]
    pub fn split(self, left_col_num: usize, right_col_num: usize) -> (Self, Self, Self) {
        let left_bit_map = FixedBitSet::from_iter(0..left_col_num);
        let right_bit_map = FixedBitSet::from_iter(left_col_num..left_col_num + right_col_num);

        self.group_by::<_, 3>(|expr| {
            let input_bits = expr.collect_input_refs(left_col_num + right_col_num);
            if input_bits.is_subset(&left_bit_map) {
                0
            } else if input_bits.is_subset(&right_bit_map) {
                1
            } else {
                2
            }
        })
        .into_iter()
        .next_tuple()
        .unwrap()
    }

    /// Collect all `InputRef`s' indexes in the expressions.
    ///
    /// # Panics
    /// Panics if `input_ref >= input_col_num`.
    pub fn collect_input_refs(&self, input_col_num: usize) -> FixedBitSet {
        collect_input_refs(input_col_num, &self.conjunctions)
    }

    /// Split the condition expressions into (N choose 2) + 1 groups: those containing two columns
    /// from different buckets (and optionally, needing an equal condition between them), and
    /// others.
    ///
    /// `input_num_cols` are the number of columns in each of the input buckets. For instance, with
    /// bucket0: col0, col1, col2 | bucket1: col3, col4 | bucket2: col5
    /// `input_num_cols` = [3, 2, 1]
    ///
    /// Returns hashmap with keys of the form (col1, col2) where col1 < col2 in terms of their col
    /// index.
    ///
    /// `only_eq`: whether to only split those conditions with an eq condition predicate between two
    /// buckets.
    #[must_use]
    pub fn split_by_input_col_nums(
        self,
        input_col_nums: &[usize],
        only_eq: bool,
    ) -> (BTreeMap<(usize, usize), Self>, Self) {
        let mut bitmaps = Vec::with_capacity(input_col_nums.len());
        let mut cols_seen = 0;
        for cols in input_col_nums {
            bitmaps.push(FixedBitSet::from_iter(cols_seen..cols_seen + cols));
            cols_seen += cols;
        }

        let mut pairwise_conditions = BTreeMap::new();
        let mut non_eq_join = vec![];

        for expr in self.conjunctions {
            let input_bits = expr.collect_input_refs(cols_seen);
            let mut subset_indices = Vec::with_capacity(input_col_nums.len());
            for (idx, bitmap) in bitmaps.iter().enumerate() {
                if !input_bits.is_disjoint(bitmap) {
                    subset_indices.push(idx);
                }
            }
            if subset_indices.len() != 2 || (only_eq && expr.as_eq_cond().is_none()) {
                non_eq_join.push(expr);
            } else {
                // The key has the canonical ordering (lower, higher)
                let key = if subset_indices[0] < subset_indices[1] {
                    (subset_indices[0], subset_indices[1])
                } else {
                    (subset_indices[1], subset_indices[0])
                };
                let e = pairwise_conditions
                    .entry(key)
                    .or_insert_with(Condition::true_cond);
                e.conjunctions.push(expr);
            }
        }
        (
            pairwise_conditions,
            Condition {
                conjunctions: non_eq_join,
            },
        )
    }

    #[must_use]
    /// For [`EqJoinPredicate`], separate equality conditions which connect left columns and right
    /// columns from other conditions.
    ///
    /// The equality conditions are transformed into `(left_col_id, right_col_id, null_eq_null)` tuples.
    ///
    /// [`EqJoinPredicate`]: crate::optimizer::plan_node::EqJoinPredicate
    pub fn split_eq_keys(
        self,
        left_col_num: usize,
        right_col_num: usize,
    ) -> (Vec<(InputRef, InputRef, bool)>, Self) {
        let left_bit_map = FixedBitSet::from_iter(0..left_col_num);
        let right_bit_map = FixedBitSet::from_iter(left_col_num..left_col_num + right_col_num);

        let (mut eq_keys, mut others) = (vec![], vec![]);
        self.conjunctions.into_iter().for_each(|expr| {
            let input_bits = expr.collect_input_refs(left_col_num + right_col_num);
            if input_bits.is_disjoint(&left_bit_map) || input_bits.is_disjoint(&right_bit_map) {
                others.push(expr)
            } else if let Some(columns) = expr.as_eq_cond() {
                eq_keys.push((columns.0, columns.1, false));
            } else if let Some(columns) = expr.as_is_not_distinct_from_cond() {
                eq_keys.push((columns.0, columns.1, true));
            } else {
                others.push(expr)
            }
        });

        (
            eq_keys,
            Condition {
                conjunctions: others,
            },
        )
    }

    /// For [`EqJoinPredicate`], extract inequality conditions which connect left columns and right
    /// columns from other conditions.
    ///
    /// Returns a list of `(conjunction_index, InequalityInputPair)` where the pair contains
    /// the left column index, right column index (NOT offset by `left_col_num`), and the comparison
    /// operator.
    ///
    /// Only pure `InputRef <op> InputRef` conditions are extracted (no offsets like `+ INTERVAL`).
    ///
    /// [`EqJoinPredicate`]: crate::optimizer::plan_node::EqJoinPredicate
    pub(crate) fn extract_inequality_keys(
        &self,
        left_col_num: usize,
        right_col_num: usize,
    ) -> Vec<(usize, InequalityInputPair)> {
        let left_bit_map = FixedBitSet::from_iter(0..left_col_num);
        let right_bit_map = FixedBitSet::from_iter(left_col_num..left_col_num + right_col_num);

        self.conjunctions
            .iter()
            .enumerate()
            .filter_map(|(conjunction_idx, expr)| {
                let input_bits = expr.collect_input_refs(left_col_num + right_col_num);
                if input_bits.is_disjoint(&left_bit_map) || input_bits.is_disjoint(&right_bit_map) {
                    return None;
                }

                // Use as_comparison_cond which only matches pure InputRef <op> InputRef
                let (left_input, op, right_input) = expr.as_comparison_cond()?;

                // Ensure left is from left input and right is from right input
                // as_comparison_cond normalizes to left.index < right.index
                if left_input.index() < left_col_num
                    && right_input.index() >= left_col_num
                    && right_input.index() < left_col_num + right_col_num
                {
                    Some((
                        conjunction_idx,
                        InequalityInputPair::new(
                            left_input.index(),
                            right_input.index() - left_col_num, // Convert to right input index
                            op,
                        ),
                    ))
                } else {
                    None
                }
            })
            .collect_vec()
    }

    /// Split the condition expressions into 2 groups: those referencing `columns` and others which
    /// are disjoint with columns.
    #[must_use]
    pub fn split_disjoint(self, columns: &FixedBitSet) -> (Self, Self) {
        self.group_by::<_, 2>(|expr| {
            let input_bits = expr.collect_input_refs(columns.len());
            input_bits.is_disjoint(columns) as usize
        })
        .into_iter()
        .next_tuple()
        .unwrap()
    }

    /// Generate range scans from each arm of `OR` clause and merge them.
    /// Currently, only support equal type range scans.
    /// Keep in mind that range scans can not overlap, otherwise duplicate rows will occur.
    fn disjunctions_to_scan_ranges(
        table: &TableCatalog,
        max_split_range_gap: u64,
        disjunctions: Vec<ExprImpl>,
    ) -> Result<Option<(Vec<ScanRange>, bool)>> {
        let disjunctions_result: Result<Vec<(Vec<ScanRange>, Self)>> = disjunctions
            .into_iter()
            .map(|x| {
                Condition {
                    conjunctions: to_conjunctions(x),
                }
                .split_to_scan_ranges(table, max_split_range_gap)
            })
            .collect();

        // If any arm of `OR` clause fails, bail out.
        let disjunctions_result = disjunctions_result?;

        // If all arms of `OR` clause scan ranges are simply equal condition type, merge all
        // of them.
        let all_equal = disjunctions_result
            .iter()
            .all(|(scan_ranges, other_condition)| {
                other_condition.always_true()
                    && scan_ranges
                        .iter()
                        .all(|x| !x.eq_conds.is_empty() && is_full_range(&x.range))
            });

        if all_equal {
            // Think about the case (a = 1) or (a = 1 and b = 2).
            // We should only keep the large one range scan a = 1, because a = 1 overlaps with
            // (a = 1 and b = 2).
            let scan_ranges = disjunctions_result
                .into_iter()
                .flat_map(|(scan_ranges, _)| scan_ranges)
                // sort, large one first
                .sorted_by(|a, b| a.eq_conds.len().cmp(&b.eq_conds.len()))
                .collect_vec();
            // Make sure each range never overlaps with others, that's what scan range mean.
            let mut non_overlap_scan_ranges: Vec<ScanRange> = vec![];
            for s1 in &scan_ranges {
                let overlap = non_overlap_scan_ranges.iter().any(|s2| {
                    #[expect(clippy::disallowed_methods)]
                    s1.eq_conds
                        .iter()
                        .zip(s2.eq_conds.iter())
                        .all(|(a, b)| a == b)
                });
                // If overlap happens, keep the large one and large one always in
                // `non_overlap_scan_ranges`.
                // Otherwise, put s1 into `non_overlap_scan_ranges`.
                if !overlap {
                    non_overlap_scan_ranges.push(s1.clone());
                }
            }

            Ok(Some((non_overlap_scan_ranges, false)))
        } else {
            let mut scan_ranges = vec![];
            for (scan_ranges_chunk, _) in disjunctions_result {
                if scan_ranges_chunk.is_empty() {
                    // full scan range
                    return Ok(None);
                }

                scan_ranges.extend(scan_ranges_chunk);
            }

            let order_types = table
                .pk
                .iter()
                .cloned()
                .map(|x| {
                    if x.order_type.is_descending() {
                        x.order_type.reverse()
                    } else {
                        x.order_type
                    }
                })
                .collect_vec();
            scan_ranges.sort_by(|left, right| {
                let (left_start, _left_end) = &left.convert_to_range();
                let (right_start, _right_end) = &right.convert_to_range();

                let left_start_vec = match &left_start {
                    Bound::Included(vec) | Bound::Excluded(vec) => vec,
                    _ => &vec![],
                };
                let right_start_vec = match &right_start {
                    Bound::Included(vec) | Bound::Excluded(vec) => vec,
                    _ => &vec![],
                };

                if left_start_vec.is_empty() && right_start_vec.is_empty() {
                    return Ordering::Less;
                }

                if left_start_vec.is_empty() {
                    return Ordering::Less;
                }

                if right_start_vec.is_empty() {
                    return Ordering::Greater;
                }

                let cmp_column_len = left_start_vec.len().min(right_start_vec.len());
                cmp_rows(
                    &left_start_vec[0..cmp_column_len],
                    &right_start_vec[0..cmp_column_len],
                    &order_types[0..cmp_column_len],
                )
            });

            if scan_ranges.is_empty() {
                return Ok(None);
            }

            if scan_ranges.len() == 1 {
                return Ok(Some((scan_ranges, true)));
            }

            let mut output_scan_ranges: Vec<ScanRange> = vec![];
            output_scan_ranges.push(scan_ranges[0].clone());
            let mut idx = 1;
            loop {
                if idx >= scan_ranges.len() {
                    break;
                }

                let scan_range_left = output_scan_ranges.last_mut().unwrap();
                let scan_range_right = &scan_ranges[idx];

                if scan_range_left.eq_conds == scan_range_right.eq_conds {
                    // range merge

                    if !ScanRange::is_overlap(scan_range_left, scan_range_right, &order_types) {
                        // not merge
                        output_scan_ranges.push(scan_range_right.clone());
                        idx += 1;
                        continue;
                    }

                    // merge range
                    fn merge_bound(
                        left_scan_range: &Bound<Vec<Option<ScalarImpl>>>,
                        right_scan_range: &Bound<Vec<Option<ScalarImpl>>>,
                        order_types: &[OrderType],
                        left_bound: bool,
                    ) -> Bound<Vec<Option<ScalarImpl>>> {
                        let left_scan_range = match left_scan_range {
                            Bound::Included(vec) | Bound::Excluded(vec) => vec,
                            Bound::Unbounded => return Bound::Unbounded,
                        };

                        let right_scan_range = match right_scan_range {
                            Bound::Included(vec) | Bound::Excluded(vec) => vec,
                            Bound::Unbounded => return Bound::Unbounded,
                        };

                        let cmp_len = left_scan_range.len().min(right_scan_range.len());

                        let cmp = cmp_rows(
                            &left_scan_range[..cmp_len],
                            &right_scan_range[..cmp_len],
                            &order_types[..cmp_len],
                        );

                        let bound = {
                            if (cmp.is_le() && left_bound) || (cmp.is_ge() && !left_bound) {
                                left_scan_range.clone()
                            } else {
                                right_scan_range.clone()
                            }
                        };

                        // Included Bound just for convenience, the correctness will be guaranteed by the upper level filter.
                        Bound::Included(bound)
                    }

                    scan_range_left.range.0 = merge_bound(
                        &scan_range_left.range.0,
                        &scan_range_right.range.0,
                        &order_types,
                        true,
                    );

                    scan_range_left.range.1 = merge_bound(
                        &scan_range_left.range.1,
                        &scan_range_right.range.1,
                        &order_types,
                        false,
                    );

                    if scan_range_left.is_full_table_scan() {
                        return Ok(None);
                    }
                } else {
                    output_scan_ranges.push(scan_range_right.clone());
                }

                idx += 1;
            }

            Ok(Some((output_scan_ranges, true)))
        }
    }

    fn split_row_cmp_to_scan_ranges(
        &self,
        table: &TableCatalog,
    ) -> Result<Option<(Vec<ScanRange>, Self)>> {
        let (mut row_conjunctions, row_conjunctions_without_struct): (Vec<_>, Vec<_>) =
            self.conjunctions.clone().into_iter().partition(|expr| {
                if let Some(f) = expr.as_function_call() {
                    if let Some(left_input) = f.inputs().get(0)
                        && let Some(left_input) = left_input.as_function_call()
                        && matches!(left_input.func_type(), ExprType::Row)
                        && left_input.inputs().iter().all(|x| x.is_input_ref())
                        && let Some(right_input) = f.inputs().get(1)
                        && right_input.is_literal()
                    {
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            });
        // optimize for single row conjunctions. More optimisations may come later
        // For example, (v1,v2,v3) > (1, 2, 3) means all data from (1, 2, 3).
        // Suppose v1 v2 v3 are both pk, we can push (v1,v2,v3）> (1,2,3) down to scan
        // Suppose v1 v2 are both pk, we can push (v1,v2）> (1,2) down to scan and add (v1,v2,v3) > (1,2,3) in filter, it is still possible to reduce the value of scan
        if row_conjunctions.len() == 1 {
            let row_conjunction = row_conjunctions.pop().unwrap();
            let row_left_inputs = row_conjunction
                .as_function_call()
                .unwrap()
                .inputs()
                .get(0)
                .unwrap()
                .as_function_call()
                .unwrap()
                .inputs();
            let row_right_literal = row_conjunction
                .as_function_call()
                .unwrap()
                .inputs()
                .get(1)
                .unwrap()
                .as_literal()
                .unwrap();
            if !matches!(row_right_literal.get_data(), Some(ScalarImpl::Struct(_))) {
                return Ok(None);
            }
            let row_right_literal_data = row_right_literal.get_data().clone().unwrap();
            let right_iter = row_right_literal_data.as_struct().fields();
            let func_type = row_conjunction.as_function_call().unwrap().func_type();
            if row_left_inputs.len() > 1
                && (matches!(func_type, ExprType::LessThan)
                    || matches!(func_type, ExprType::GreaterThan))
            {
                let mut pk_struct = vec![];
                let mut order_type = None;
                let mut all_added = true;
                let mut iter = row_left_inputs.iter().zip_eq_fast(right_iter);
                for column_order in &table.pk {
                    if let Some((left_expr, right_expr)) = iter.next() {
                        if left_expr.as_input_ref().unwrap().index != column_order.column_index {
                            all_added = false;
                            break;
                        }
                        match order_type {
                            Some(o) => {
                                if o != column_order.order_type {
                                    all_added = false;
                                    break;
                                }
                            }
                            None => order_type = Some(column_order.order_type),
                        }
                        pk_struct.push(right_expr.clone());
                    }
                }

                // Here it is necessary to determine whether all of row is included in the `ScanRanges`, if so, the data for eq is not needed
                if !pk_struct.is_empty() {
                    if !all_added {
                        let scan_range = ScanRange {
                            eq_conds: vec![],
                            range: match func_type {
                                ExprType::GreaterThan => {
                                    (Bound::Included(pk_struct), Bound::Unbounded)
                                }
                                ExprType::LessThan => {
                                    (Bound::Unbounded, Bound::Included(pk_struct))
                                }
                                _ => unreachable!(),
                            },
                        };
                        return Ok(Some((
                            vec![scan_range],
                            Condition {
                                conjunctions: self.conjunctions.clone(),
                            },
                        )));
                    } else {
                        let scan_range = ScanRange {
                            eq_conds: vec![],
                            range: match func_type {
                                ExprType::GreaterThan => {
                                    (Bound::Excluded(pk_struct), Bound::Unbounded)
                                }
                                ExprType::LessThan => {
                                    (Bound::Unbounded, Bound::Excluded(pk_struct))
                                }
                                _ => unreachable!(),
                            },
                        };
                        return Ok(Some((
                            vec![scan_range],
                            Condition {
                                conjunctions: row_conjunctions_without_struct,
                            },
                        )));
                    }
                }
            }
        }
        Ok(None)
    }

    /// x = 1 AND y = 2 AND z = 3 => [x, y, z]
    pub fn get_eq_const_input_refs(&self) -> Vec<InputRef> {
        self.conjunctions
            .iter()
            .filter_map(|expr| expr.as_eq_const().map(|(input_ref, _)| input_ref))
            .collect()
    }

    /// See also [`ScanRange`](risingwave_pb::batch_plan::ScanRange).
    pub fn split_to_scan_ranges(
        self,
        table: &TableCatalog,
        max_split_range_gap: u64,
    ) -> Result<(Vec<ScanRange>, Self)> {
        fn false_cond() -> (Vec<ScanRange>, Condition) {
            (vec![], Condition::false_cond())
        }

        // It's an OR.
        if self.conjunctions.len() == 1
            && let Some(disjunctions) = self.conjunctions[0].as_or_disjunctions()
        {
            if let Some((scan_ranges, maintaining_condition)) =
                Self::disjunctions_to_scan_ranges(table, max_split_range_gap, disjunctions)?
            {
                if maintaining_condition {
                    return Ok((scan_ranges, self));
                } else {
                    return Ok((scan_ranges, Condition::true_cond()));
                }
            } else {
                return Ok((vec![], self));
            }
        }
        if let Some((scan_ranges, other_condition)) = self.split_row_cmp_to_scan_ranges(table)? {
            return Ok((scan_ranges, other_condition));
        }

        let mut groups = Self::classify_conjunctions_by_pk(self.conjunctions, table);
        let mut other_conds = groups.pop().unwrap();

        // Analyze each group and use result to update scan range.
        let mut scan_range = ScanRange::full_table_scan();
        for i in 0..table.pk.len() {
            let group = std::mem::take(&mut groups[i]);
            if group.is_empty() {
                groups.push(other_conds);
                return Ok((
                    if scan_range.is_full_table_scan() {
                        vec![]
                    } else {
                        vec![scan_range]
                    },
                    Self {
                        conjunctions: groups[i + 1..].concat(),
                    },
                ));
            }

            let Some((
                lower_bound_conjunctions,
                upper_bound_conjunctions,
                eq_conds,
                part_of_other_conds,
            )) = Self::analyze_group(group)?
            else {
                return Ok(false_cond());
            };
            other_conds.extend(part_of_other_conds);

            let lower_bound = Self::merge_lower_bound_conjunctions(lower_bound_conjunctions);
            let upper_bound = Self::merge_upper_bound_conjunctions(upper_bound_conjunctions);

            if Self::is_invalid_range(&lower_bound, &upper_bound) {
                return Ok(false_cond());
            }

            // update scan_range
            match eq_conds.len() {
                1 => {
                    let eq_conds =
                        Self::extract_eq_conds_within_range(eq_conds, &upper_bound, &lower_bound);
                    if eq_conds.is_empty() {
                        return Ok(false_cond());
                    }
                    scan_range.eq_conds.extend(eq_conds);
                }
                0 => {
                    let convert = |bound| match bound {
                        Bound::Included(l) => Bound::Included(vec![Some(l)]),
                        Bound::Excluded(l) => Bound::Excluded(vec![Some(l)]),
                        Bound::Unbounded => Bound::Unbounded,
                    };
                    scan_range.range = (convert(lower_bound), convert(upper_bound));
                    other_conds.extend(groups[i + 1..].iter().flatten().cloned());
                    break;
                }
                _ => {
                    // currently we will split IN list to multiple scan ranges immediately
                    // i.e., a = 1 AND b in (1,2) is handled
                    // TODO:
                    // a in (1,2) AND b = 1
                    // a in (1,2) AND b in (1,2)
                    // a in (1,2) AND b > 1
                    let eq_conds =
                        Self::extract_eq_conds_within_range(eq_conds, &upper_bound, &lower_bound);
                    if eq_conds.is_empty() {
                        return Ok(false_cond());
                    }
                    other_conds.extend(groups[i + 1..].iter().flatten().cloned());
                    let scan_ranges = eq_conds
                        .into_iter()
                        .map(|lit| {
                            let mut scan_range = scan_range.clone();
                            scan_range.eq_conds.push(lit);
                            scan_range
                        })
                        .collect();
                    return Ok((
                        scan_ranges,
                        Self {
                            conjunctions: other_conds,
                        },
                    ));
                }
            }
        }

        Ok((
            if scan_range.is_full_table_scan() {
                vec![]
            } else if table.columns[table.pk[0].column_index].data_type.is_int() {
                match scan_range.split_small_range(max_split_range_gap) {
                    Some(scan_ranges) => scan_ranges,
                    None => vec![scan_range],
                }
            } else {
                vec![scan_range]
            },
            Self {
                conjunctions: other_conds,
            },
        ))
    }

    /// classify conjunctions into groups:
    /// The i-th group has exprs that only reference the i-th PK column.
    /// The last group contains all the other exprs.
    fn classify_conjunctions_by_pk(
        conjunctions: Vec<ExprImpl>,
        table: &TableCatalog,
    ) -> Vec<Vec<ExprImpl>> {
        let pk_cols_num = table.pk.len();
        let cols_num = table.columns.len();

        let mut col_idx_to_pk_idx = vec![None; cols_num];
        table
            .order_column_indices()
            .enumerate()
            .for_each(|(idx, pk_idx)| {
                col_idx_to_pk_idx[pk_idx] = Some(idx);
            });

        let mut groups = vec![vec![]; pk_cols_num + 1];
        for (key, group) in &conjunctions.into_iter().chunk_by(|expr| {
            let input_bits = expr.collect_input_refs(cols_num);
            if input_bits.count_ones(..) == 1 {
                let col_idx = input_bits.ones().next().unwrap();
                col_idx_to_pk_idx[col_idx].unwrap_or(pk_cols_num)
            } else {
                pk_cols_num
            }
        }) {
            groups[key].extend(group);
        }

        groups
    }

    /// Extract the following information in a group of conjunctions:
    /// 1. lower bound conjunctions
    /// 2. upper bound conjunctions
    /// 3. eq conditions
    /// 4. other conditions
    ///
    /// return None indicates that this conjunctions is always false
    #[expect(clippy::type_complexity)]
    fn analyze_group(
        group: Vec<ExprImpl>,
    ) -> Result<
        Option<(
            Vec<Bound<ScalarImpl>>,
            Vec<Bound<ScalarImpl>>,
            Vec<Option<ScalarImpl>>,
            Vec<ExprImpl>,
        )>,
    > {
        let mut lower_bound_conjunctions = vec![];
        let mut upper_bound_conjunctions = vec![];
        // values in eq_cond are OR'ed
        let mut eq_conds = vec![];
        let mut other_conds = vec![];

        // analyze exprs in the group. scan_range is not updated
        'group_loop: for expr in group {
            if let Some((input_ref, const_expr)) = expr.as_eq_const() {
                let new_expr = if let Ok(expr) =
                    const_expr.clone().cast_implicit(&input_ref.data_type)
                {
                    expr
                } else {
                    match self::cast_compare::cast_compare_for_eq(const_expr, input_ref.data_type) {
                        Ok(ResultForEq::Success(expr)) => expr,
                        Ok(ResultForEq::NeverEqual) => {
                            return Ok(None);
                        }
                        Err(_) => {
                            other_conds.push(expr);
                            continue;
                        }
                    }
                };

                let Some(new_cond) = new_expr.fold_const()? else {
                    // column = NULL, the result is always NULL.
                    return Ok(None);
                };
                if Self::mutual_exclusive_with_eq_conds(&new_cond, &eq_conds) {
                    return Ok(None);
                }
                eq_conds = vec![Some(new_cond)];
            } else if expr.as_is_null().is_some() {
                if !eq_conds.is_empty() && eq_conds.into_iter().all(|l| l.is_some()) {
                    return Ok(None);
                }
                eq_conds = vec![None];
            } else if let Some((input_ref, in_const_list)) = expr
                .as_in_const_list()
                .or_else(|| expr.as_some_eq_const_list())
            {
                let mut scalars = HashSet::new();
                for const_expr in in_const_list {
                    let new_expr =
                        if let Ok(expr) = const_expr.clone().cast_implicit(&input_ref.data_type) {
                            expr
                        } else {
                            match self::cast_compare::cast_compare_for_eq(
                                const_expr,
                                input_ref.data_type.clone(),
                            ) {
                                Ok(ResultForEq::Success(expr)) => expr,
                                Ok(ResultForEq::NeverEqual) => {
                                    continue;
                                }
                                Err(_) => {
                                    other_conds.push(expr);
                                    continue 'group_loop;
                                }
                            }
                        };

                    let value = new_expr.fold_const()?;
                    let Some(value) = value else {
                        continue;
                    };
                    scalars.insert(Some(value));
                }
                if scalars.is_empty() {
                    // There're only NULLs in the in-list
                    return Ok(None);
                }
                if !eq_conds.is_empty() {
                    scalars = scalars
                        .intersection(&HashSet::from_iter(eq_conds))
                        .cloned()
                        .collect();
                    if scalars.is_empty() {
                        return Ok(None);
                    }
                }
                // Sort to ensure a deterministic result for planner test.
                eq_conds = scalars
                    .into_iter()
                    .sorted_by(DefaultOrd::default_cmp)
                    .collect();
            } else if let Some((input_ref, op, const_expr)) = expr.as_comparison_const() {
                let new_expr =
                    if let Ok(expr) = const_expr.clone().cast_implicit(&input_ref.data_type) {
                        expr
                    } else {
                        match self::cast_compare::cast_compare_for_cmp(
                            const_expr,
                            input_ref.data_type,
                            op,
                        ) {
                            Ok(ResultForCmp::Success(expr)) => expr,
                            _ => {
                                other_conds.push(expr);
                                continue;
                            }
                        }
                    };
                let Some(value) = new_expr.fold_const()? else {
                    // column compare with NULL, the result is always  NULL.
                    return Ok(None);
                };
                match op {
                    ExprType::LessThan => {
                        upper_bound_conjunctions.push(Bound::Excluded(value));
                    }
                    ExprType::LessThanOrEqual => {
                        upper_bound_conjunctions.push(Bound::Included(value));
                    }
                    ExprType::GreaterThan => {
                        lower_bound_conjunctions.push(Bound::Excluded(value));
                    }
                    ExprType::GreaterThanOrEqual => {
                        lower_bound_conjunctions.push(Bound::Included(value));
                    }
                    _ => unreachable!(),
                }
            } else {
                other_conds.push(expr);
            }
        }
        Ok(Some((
            lower_bound_conjunctions,
            upper_bound_conjunctions,
            eq_conds,
            other_conds,
        )))
    }

    fn mutual_exclusive_with_eq_conds(
        new_conds: &ScalarImpl,
        eq_conds: &[Option<ScalarImpl>],
    ) -> bool {
        !eq_conds.is_empty()
            && eq_conds.iter().all(|l| {
                if let Some(l) = l {
                    l != new_conds
                } else {
                    true
                }
            })
    }

    fn merge_lower_bound_conjunctions(lb: Vec<Bound<ScalarImpl>>) -> Bound<ScalarImpl> {
        lb.into_iter()
            .max_by(|a, b| {
                // For lower bound, Unbounded means -inf
                match (a, b) {
                    (Bound::Included(_), Bound::Unbounded) => std::cmp::Ordering::Greater,
                    (Bound::Excluded(_), Bound::Unbounded) => std::cmp::Ordering::Greater,
                    (Bound::Unbounded, Bound::Included(_)) => std::cmp::Ordering::Less,
                    (Bound::Unbounded, Bound::Excluded(_)) => std::cmp::Ordering::Less,
                    (Bound::Unbounded, Bound::Unbounded) => std::cmp::Ordering::Equal,
                    (Bound::Included(a), Bound::Included(b)) => a.default_cmp(b),
                    (Bound::Excluded(a), Bound::Excluded(b)) => a.default_cmp(b),
                    // excluded bound is strict than included bound so we assume it more greater.
                    (Bound::Included(a), Bound::Excluded(b)) => match a.default_cmp(b) {
                        std::cmp::Ordering::Equal => std::cmp::Ordering::Less,
                        other => other,
                    },
                    (Bound::Excluded(a), Bound::Included(b)) => match a.default_cmp(b) {
                        std::cmp::Ordering::Equal => std::cmp::Ordering::Greater,
                        other => other,
                    },
                }
            })
            .unwrap_or(Bound::Unbounded)
    }

    fn merge_upper_bound_conjunctions(ub: Vec<Bound<ScalarImpl>>) -> Bound<ScalarImpl> {
        ub.into_iter()
            .min_by(|a, b| {
                // For upper bound, Unbounded means +inf
                match (a, b) {
                    (Bound::Included(_), Bound::Unbounded) => std::cmp::Ordering::Less,
                    (Bound::Excluded(_), Bound::Unbounded) => std::cmp::Ordering::Less,
                    (Bound::Unbounded, Bound::Included(_)) => std::cmp::Ordering::Greater,
                    (Bound::Unbounded, Bound::Excluded(_)) => std::cmp::Ordering::Greater,
                    (Bound::Unbounded, Bound::Unbounded) => std::cmp::Ordering::Equal,
                    (Bound::Included(a), Bound::Included(b)) => a.default_cmp(b),
                    (Bound::Excluded(a), Bound::Excluded(b)) => a.default_cmp(b),
                    // excluded bound is strict than included bound so we assume it more greater.
                    (Bound::Included(a), Bound::Excluded(b)) => match a.default_cmp(b) {
                        std::cmp::Ordering::Equal => std::cmp::Ordering::Greater,
                        other => other,
                    },
                    (Bound::Excluded(a), Bound::Included(b)) => match a.default_cmp(b) {
                        std::cmp::Ordering::Equal => std::cmp::Ordering::Less,
                        other => other,
                    },
                }
            })
            .unwrap_or(Bound::Unbounded)
    }

    fn is_invalid_range(lower_bound: &Bound<ScalarImpl>, upper_bound: &Bound<ScalarImpl>) -> bool {
        match (lower_bound, upper_bound) {
            (Bound::Included(l), Bound::Included(u)) => l.default_cmp(u).is_gt(), // l > u
            (Bound::Included(l), Bound::Excluded(u)) => l.default_cmp(u).is_ge(), // l >= u
            (Bound::Excluded(l), Bound::Included(u)) => l.default_cmp(u).is_ge(), // l >= u
            (Bound::Excluded(l), Bound::Excluded(u)) => l.default_cmp(u).is_ge(), // l >= u
            _ => false,
        }
    }

    fn extract_eq_conds_within_range(
        eq_conds: Vec<Option<ScalarImpl>>,
        upper_bound: &Bound<ScalarImpl>,
        lower_bound: &Bound<ScalarImpl>,
    ) -> Vec<Option<ScalarImpl>> {
        // defensive programming: for now we will guarantee that the range is valid before calling
        // this function
        if Self::is_invalid_range(lower_bound, upper_bound) {
            return vec![];
        }

        let is_extract_null = upper_bound == &Bound::Unbounded && lower_bound == &Bound::Unbounded;

        eq_conds
            .into_iter()
            .filter(|cond| {
                if let Some(cond) = cond {
                    match lower_bound {
                        Bound::Included(val) => {
                            if cond.default_cmp(val).is_lt() {
                                // cond < val
                                return false;
                            }
                        }
                        Bound::Excluded(val) => {
                            if cond.default_cmp(val).is_le() {
                                // cond <= val
                                return false;
                            }
                        }
                        Bound::Unbounded => {}
                    }
                    match upper_bound {
                        Bound::Included(val) => {
                            if cond.default_cmp(val).is_gt() {
                                // cond > val
                                return false;
                            }
                        }
                        Bound::Excluded(val) => {
                            if cond.default_cmp(val).is_ge() {
                                // cond >= val
                                return false;
                            }
                        }
                        Bound::Unbounded => {}
                    }
                    true
                } else {
                    is_extract_null
                }
            })
            .collect()
    }

    /// Split the condition expressions into `N` groups.
    /// An expression `expr` is in the `i`-th group if `f(expr)==i`.
    ///
    /// # Panics
    /// Panics if `f(expr)>=N`.
    #[must_use]
    pub fn group_by<F, const N: usize>(self, f: F) -> [Self; N]
    where
        F: Fn(&ExprImpl) -> usize,
    {
        const EMPTY: Vec<ExprImpl> = vec![];
        let mut groups = [EMPTY; N];
        for (key, group) in &self.conjunctions.into_iter().chunk_by(|expr| {
            // i-th group
            let i = f(expr);
            assert!(i < N);
            i
        }) {
            groups[key].extend(group);
        }

        groups.map(|group| Condition {
            conjunctions: group,
        })
    }

    #[must_use]
    pub fn rewrite_expr(self, rewriter: &mut (impl ExprRewriter + ?Sized)) -> Self {
        Self {
            conjunctions: self
                .conjunctions
                .into_iter()
                .map(|expr| rewriter.rewrite_expr(expr))
                .collect(),
        }
        .simplify()
    }

    pub fn visit_expr<V: ExprVisitor + ?Sized>(&self, visitor: &mut V) {
        self.conjunctions
            .iter()
            .for_each(|expr| visitor.visit_expr(expr));
    }

    pub fn visit_expr_mut(&mut self, mutator: &mut (impl ExprMutator + ?Sized)) {
        self.conjunctions
            .iter_mut()
            .for_each(|expr| mutator.visit_expr(expr))
    }

    /// Simplify conditions
    /// It simplify conditions by applying constant folding and removing unnecessary conjunctions
    fn simplify(self) -> Self {
        // boolean constant folding
        let conjunctions: Vec<_> = self
            .conjunctions
            .into_iter()
            .map(push_down_not)
            .map(fold_boolean_constant)
            .map(column_self_eq_eliminate)
            .flat_map(to_conjunctions)
            .collect();
        let mut res: Vec<ExprImpl> = Vec::new();
        let mut visited: HashSet<ExprImpl> = HashSet::new();
        for expr in conjunctions {
            // factorization_expr requires hash-able ExprImpl
            if !expr.has_subquery() {
                let results_of_factorization = factorization_expr(expr);
                res.extend(
                    results_of_factorization
                        .clone()
                        .into_iter()
                        .filter(|expr| !visited.contains(expr)),
                );
                visited.extend(results_of_factorization);
            } else {
                // for subquery, simply give up factorization
                res.push(expr);
            }
        }
        // remove all constant boolean `true`
        res.retain(|expr| {
            if let Some(v) = try_get_bool_constant(expr)
                && v
            {
                false
            } else {
                true
            }
        });
        // if there is a `false` in conjunctions, the whole condition will be `false`
        for expr in &mut res {
            if let Some(v) = try_get_bool_constant(expr)
                && !v
            {
                res.clear();
                res.push(ExprImpl::literal_bool(false));
                break;
            }
        }
        Self { conjunctions: res }
    }
}

pub struct ConditionDisplay<'a> {
    pub condition: &'a Condition,
    pub input_schema: &'a Schema,
}

impl ConditionDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.condition.always_true() {
            write!(f, "true")
        } else {
            write!(
                f,
                "{}",
                self.condition
                    .conjunctions
                    .iter()
                    .format_with(" AND ", |expr, f| {
                        f(&ExprDisplay {
                            expr,
                            input_schema: self.input_schema,
                        })
                    })
            )
        }
    }
}

impl fmt::Display for ConditionDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}

impl fmt::Debug for ConditionDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt(f)
    }
}

/// `cast_compare` can be summarized as casting to target type which can be compared but can't be
/// cast implicitly to, like:
/// 1. bigger range -> smaller range in same type, e.g. int64 -> int32
/// 2. different type, e.g. float type -> integral type
mod cast_compare {
    use risingwave_common::types::DataType;

    use crate::expr::{Expr, ExprImpl, ExprType};

    enum ShrinkResult {
        OutUpperBound,
        OutLowerBound,
        InRange(ExprImpl),
    }

    pub enum ResultForEq {
        Success(ExprImpl),
        NeverEqual,
    }

    pub enum ResultForCmp {
        Success(ExprImpl),
        OutUpperBound,
        OutLowerBound,
    }

    pub fn cast_compare_for_eq(const_expr: ExprImpl, target: DataType) -> Result<ResultForEq, ()> {
        match (const_expr.return_type(), &target) {
            (DataType::Int64, DataType::Int32)
            | (DataType::Int64, DataType::Int16)
            | (DataType::Int32, DataType::Int16) => match shrink_integral(const_expr, target)? {
                ShrinkResult::InRange(expr) => Ok(ResultForEq::Success(expr)),
                ShrinkResult::OutUpperBound | ShrinkResult::OutLowerBound => {
                    Ok(ResultForEq::NeverEqual)
                }
            },
            _ => Err(()),
        }
    }

    pub fn cast_compare_for_cmp(
        const_expr: ExprImpl,
        target: DataType,
        _op: ExprType,
    ) -> Result<ResultForCmp, ()> {
        match (const_expr.return_type(), &target) {
            (DataType::Int64, DataType::Int32)
            | (DataType::Int64, DataType::Int16)
            | (DataType::Int32, DataType::Int16) => match shrink_integral(const_expr, target)? {
                ShrinkResult::InRange(expr) => Ok(ResultForCmp::Success(expr)),
                ShrinkResult::OutUpperBound => Ok(ResultForCmp::OutUpperBound),
                ShrinkResult::OutLowerBound => Ok(ResultForCmp::OutLowerBound),
            },
            _ => Err(()),
        }
    }

    fn shrink_integral(const_expr: ExprImpl, target: DataType) -> Result<ShrinkResult, ()> {
        let (upper_bound, lower_bound) = match (const_expr.return_type(), &target) {
            (DataType::Int64, DataType::Int32) => (i32::MAX as i64, i32::MIN as i64),
            (DataType::Int64, DataType::Int16) | (DataType::Int32, DataType::Int16) => {
                (i16::MAX as i64, i16::MIN as i64)
            }
            _ => unreachable!(),
        };
        match const_expr.fold_const().map_err(|_| ())? {
            Some(scalar) => {
                let value = scalar.as_integral();
                if value > upper_bound {
                    Ok(ShrinkResult::OutUpperBound)
                } else if value < lower_bound {
                    Ok(ShrinkResult::OutLowerBound)
                } else {
                    Ok(ShrinkResult::InRange(
                        const_expr.cast_explicit(&target).unwrap(),
                    ))
                }
            }
            None => Ok(ShrinkResult::InRange(
                const_expr.cast_explicit(&target).unwrap(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    #[test]
    fn test_split() {
        let left_col_num = 3;
        let right_col_num = 2;

        let ty = DataType::Int32;

        let mut rng = rand::rng();

        let left: ExprImpl = FunctionCall::new(
            ExprType::LessThanOrEqual,
            vec![
                InputRef::new(rng.random_range(0..left_col_num), ty.clone()).into(),
                InputRef::new(rng.random_range(0..left_col_num), ty.clone()).into(),
            ],
        )
        .unwrap()
        .into();

        let right: ExprImpl = FunctionCall::new(
            ExprType::LessThan,
            vec![
                InputRef::new(
                    rng.random_range(left_col_num..left_col_num + right_col_num),
                    ty.clone(),
                )
                .into(),
                InputRef::new(
                    rng.random_range(left_col_num..left_col_num + right_col_num),
                    ty.clone(),
                )
                .into(),
            ],
        )
        .unwrap()
        .into();

        let other: ExprImpl = FunctionCall::new(
            ExprType::GreaterThan,
            vec![
                InputRef::new(rng.random_range(0..left_col_num), ty.clone()).into(),
                InputRef::new(
                    rng.random_range(left_col_num..left_col_num + right_col_num),
                    ty,
                )
                .into(),
            ],
        )
        .unwrap()
        .into();

        let cond = Condition::with_expr(other.clone())
            .and(Condition::with_expr(right.clone()))
            .and(Condition::with_expr(left.clone()));

        let res = cond.split(left_col_num, right_col_num);

        assert_eq!(res.0.conjunctions, vec![left]);
        assert_eq!(res.1.conjunctions, vec![right]);
        assert_eq!(res.2.conjunctions, vec![other]);
    }

    #[test]
    fn test_self_eq_eliminate() {
        let left_col_num = 3;
        let right_col_num = 2;

        let ty = DataType::Int32;

        let mut rng = rand::rng();

        let x: ExprImpl = InputRef::new(rng.random_range(0..left_col_num), ty.clone()).into();

        let left: ExprImpl = FunctionCall::new(ExprType::Equal, vec![x.clone(), x.clone()])
            .unwrap()
            .into();

        let right: ExprImpl = FunctionCall::new(
            ExprType::LessThan,
            vec![
                InputRef::new(
                    rng.random_range(left_col_num..left_col_num + right_col_num),
                    ty.clone(),
                )
                .into(),
                InputRef::new(
                    rng.random_range(left_col_num..left_col_num + right_col_num),
                    ty.clone(),
                )
                .into(),
            ],
        )
        .unwrap()
        .into();

        let other: ExprImpl = FunctionCall::new(
            ExprType::GreaterThan,
            vec![
                InputRef::new(rng.random_range(0..left_col_num), ty.clone()).into(),
                InputRef::new(
                    rng.random_range(left_col_num..left_col_num + right_col_num),
                    ty,
                )
                .into(),
            ],
        )
        .unwrap()
        .into();

        let cond = Condition::with_expr(other.clone())
            .and(Condition::with_expr(right.clone()))
            .and(Condition::with_expr(left));

        let res = cond.split(left_col_num, right_col_num);

        let left_res = FunctionCall::new(ExprType::IsNotNull, vec![x])
            .unwrap()
            .into();

        assert_eq!(res.0.conjunctions, vec![left_res]);
        assert_eq!(res.1.conjunctions, vec![right]);
        assert_eq!(res.2.conjunctions, vec![other]);
    }
}
