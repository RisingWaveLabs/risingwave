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

use risingwave_pb::plan_common::JoinType;
use risingwave_sqlparser::ast::{
    BinaryOperator, Expr, Ident, JoinConstraint, JoinOperator, TableFactor, TableWithJoins, Value,
};

use crate::binder::bind_context::BindContext;
use crate::binder::statement::RewriteExprsRecursive;
use crate::binder::{Binder, COLUMN_GROUP_PREFIX, Clause, Relation};
use crate::error::{ErrorCode, Result};
use crate::expr::ExprImpl;

#[derive(Debug, Clone)]
pub struct BoundJoin {
    pub join_type: JoinType,
    pub left: Relation,
    pub right: Relation,
    pub cond: ExprImpl,
}

impl RewriteExprsRecursive for BoundJoin {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        self.left.rewrite_exprs_recursive(rewriter);
        self.right.rewrite_exprs_recursive(rewriter);
        self.cond = rewriter.rewrite_expr(self.cond.take());
    }
}

impl Binder {
    pub(crate) fn bind_vec_table_with_joins(
        &mut self,
        from: &[TableWithJoins],
    ) -> Result<Option<Relation>> {
        let mut from_iter = from.iter();
        let first = match from_iter.next() {
            Some(t) => t,
            None => return Ok(None),
        };
        self.push_lateral_context();
        let mut root = self.bind_table_with_joins(first)?;
        self.pop_and_merge_lateral_context()?;
        for t in from_iter {
            self.push_lateral_context();
            let right = self.bind_table_with_joins(t)?;
            self.pop_and_merge_lateral_context()?;

            root = if Self::requires_apply(&right) {
                Relation::Apply(Box::new(BoundJoin {
                    join_type: JoinType::Inner,
                    left: root,
                    right,
                    cond: ExprImpl::literal_bool(true),
                }))
            } else {
                Relation::Join(Box::new(BoundJoin {
                    join_type: JoinType::Inner,
                    left: root,
                    right,
                    cond: ExprImpl::literal_bool(true),
                }))
            }
        }
        Ok(Some(root))
    }

    fn requires_apply(relation: &Relation) -> bool {
        matches!(relation, Relation::Subquery(subquery) if subquery.lateral)
            || matches!(relation, Relation::TableFunction { .. })
            || relation.is_correlated_by_depth(0)
    }

    pub(crate) fn bind_table_with_joins(&mut self, table: &TableWithJoins) -> Result<Relation> {
        let mut root = self.bind_table_factor(&table.relation)?;
        for join in &table.joins {
            let (constraint, join_type) = match &join.join_operator {
                JoinOperator::Inner(constraint) => (constraint, JoinType::Inner),
                JoinOperator::LeftOuter(constraint) => (constraint, JoinType::LeftOuter),
                JoinOperator::RightOuter(constraint) => (constraint, JoinType::RightOuter),
                JoinOperator::FullOuter(constraint) => (constraint, JoinType::FullOuter),
                // Cross join equals to inner join with with no constraint.
                JoinOperator::CrossJoin => (&JoinConstraint::None, JoinType::Inner),
                JoinOperator::AsOfInner(constraint) => (constraint, JoinType::AsofInner),
                JoinOperator::AsOfLeft(constraint) => (constraint, JoinType::AsofLeftOuter),
            };
            let right: Relation;
            let cond: ExprImpl;
            if matches!(
                constraint.clone(),
                JoinConstraint::Using(_) | JoinConstraint::Natural
            ) {
                let option_rel: Option<Relation>;
                (cond, option_rel) =
                    self.bind_join_constraint(constraint, Some(&join.relation), join_type)?;
                right = option_rel.unwrap();
            } else {
                right = self.bind_table_factor(&join.relation)?;
                (cond, _) = self.bind_join_constraint(constraint, None, join_type)?;
            }

            root = if Self::requires_apply(&right) {
                match join_type {
                    JoinType::Inner | JoinType::LeftOuter => {}
                    _ => {
                        return Err(ErrorCode::InvalidInputSyntax("The combining JOIN type must be INNER or LEFT for a LATERAL reference.".to_owned())
                            .into());
                    }
                }

                Relation::Apply(Box::new(BoundJoin {
                    join_type,
                    left: root,
                    right,
                    cond,
                }))
            } else {
                Relation::Join(Box::new(BoundJoin {
                    join_type,
                    left: root,
                    right,
                    cond,
                }))
            };
        }

        Ok(root)
    }

    fn bind_join_constraint(
        &mut self,
        constraint: &JoinConstraint,
        table_factor: Option<&TableFactor>,
        join_type: JoinType,
    ) -> Result<(ExprImpl, Option<Relation>)> {
        Ok(match constraint {
            JoinConstraint::None => (ExprImpl::literal_bool(true), None),
            c @ JoinConstraint::Natural | c @ JoinConstraint::Using(_) => {
                // First, we identify columns with the same name.
                let old_context = self.context.clone();
                let l_len = old_context.columns.len();
                // Bind this table factor to an empty context
                self.push_lateral_context();
                let table_factor = table_factor.unwrap();
                let relation = self.bind_table_factor(table_factor)?;

                let using_columns = match c {
                    JoinConstraint::Natural => None,
                    JoinConstraint::Using(cols) => {
                        // sanity check
                        for col in cols {
                            if !old_context.indices_of.contains_key(&col.real_value()) {
                                return Err(ErrorCode::ItemNotFound(format!("column \"{}\" specified in USING clause does not exist in left table", col.real_value())).into());
                            }
                            if !self.context.indices_of.contains_key(&col.real_value()) {
                                return Err(ErrorCode::ItemNotFound(format!("column \"{}\" specified in USING clause does not exist in right table", col.real_value())).into());
                            }
                        }
                        Some(cols)
                    }
                    _ => unreachable!(),
                };

                let mut columns = self
                    .context
                    .indices_of
                    .iter()
                    .filter(|(_, idxs)| idxs.iter().all(|i| !self.context.columns[*i].is_hidden))
                    .map(|(s, idxes)| (Ident::from_real_value(s), idxes))
                    .collect::<Vec<_>>();
                columns.sort_by_key(|a| a.0.real_value());

                let mut col_indices = Vec::new();
                let mut binary_expr = Expr::Value(Value::Boolean(true));

                // Walk the RHS cols, checking to see if any share a name with any LHS cols
                for (column, indices_r) in columns {
                    // TODO: is it ok to ignore quote style?
                    // If we have a `USING` constraint, we only bind the columns appearing in the
                    // constraint.
                    if let Some(cols) = &using_columns
                        && !cols.contains(&column)
                    {
                        continue;
                    }
                    let indices_l = match old_context.get_unqualified_indices(&column.real_value())
                    {
                        Err(e) => {
                            if let ErrorCode::ItemNotFound(_) = e {
                                continue;
                            } else {
                                return Err(e.into());
                            }
                        }
                        Ok(idxs) => idxs,
                    };
                    // Select at most one column from each natural column group from left and right
                    col_indices.push((indices_l[0], indices_r[0] + l_len));
                    let left_expr = Self::get_identifier_from_indices(
                        &old_context,
                        &indices_l,
                        column.clone(),
                    )?;
                    let right_expr = Self::get_identifier_from_indices(
                        &self.context,
                        indices_r,
                        column.clone(),
                    )?;
                    binary_expr = Expr::BinaryOp {
                        left: Box::new(binary_expr),
                        op: BinaryOperator::And,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(left_expr),
                            op: BinaryOperator::Eq,
                            right: Box::new(right_expr),
                        }),
                    }
                }
                self.pop_and_merge_lateral_context()?;
                // Bind the expression first, before allowing disambiguation of the columns involved
                // in the join
                let expr = self.bind_expr(&binary_expr)?;
                for (l, r) in col_indices {
                    let non_nullable = match join_type {
                        JoinType::LeftOuter | JoinType::Inner => Some(l),
                        JoinType::RightOuter => Some(r),
                        JoinType::FullOuter => None,
                        _ => unreachable!(),
                    };
                    self.context.add_natural_columns(l, r, non_nullable);
                }
                (expr, Some(relation))
            }
            JoinConstraint::On(expr) => {
                let clause = self.context.clause;
                self.context.clause = Some(Clause::JoinOn);
                let bound_expr: ExprImpl = self
                    .bind_expr(expr)
                    .and_then(|expr| expr.enforce_bool_clause("JOIN ON"))?;
                self.context.clause = clause;
                (bound_expr, None)
            }
        })
    }

    fn get_identifier_from_indices(
        context: &BindContext,
        indices: &[usize],
        column: Ident,
    ) -> Result<Expr> {
        if indices.len() == 1 {
            let right_table = context.columns[indices[0]].table_name.as_ref();
            Ok(Expr::CompoundIdentifier(vec![
                Ident::from_real_value(right_table),
                column,
            ]))
        } else if let Some(group_id) = context.column_group_context.mapping.get(&indices[0]) {
            Ok(Expr::CompoundIdentifier(vec![
                Ident::from_real_value(&format!("{COLUMN_GROUP_PREFIX}{}", group_id)),
                column,
            ]))
        } else {
            Err(
                ErrorCode::InternalError(format!("Ambiguous column name: {}", column.real_value()))
                    .into(),
            )
        }
    }
}
