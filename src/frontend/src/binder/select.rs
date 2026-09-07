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

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::ScalarImpl;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_sqlparser::ast::{
    DataType as AstDataType, Distinct, Expr, Select, SelectItem, Value, WindowSpec,
};

use super::bind_context::{Clause, ColumnBinding};
use super::statement::RewriteExprsRecursive;
use super::{BoundShareInput, UNNAMED_COLUMN};
use crate::binder::{Binder, Relation};
use crate::catalog::check_column_name_not_reserved;
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{CorrelatedId, Depth, Expr as _, ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::CHANGELOG_OP;
use crate::utils::group_by::GroupBy;

#[derive(Debug, Clone)]
pub struct BoundSelect {
    pub distinct: BoundDistinct,
    pub select_items: Vec<ExprImpl>,
    pub aliases: Vec<Option<String>>,
    pub from: Option<Relation>,
    pub where_clause: Option<ExprImpl>,
    pub group_by: GroupBy,
    pub having: Option<ExprImpl>,
    pub window: HashMap<String, WindowSpec>,
    pub schema: Schema,
}

impl RewriteExprsRecursive for BoundSelect {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        self.distinct.rewrite_exprs_recursive(rewriter);

        let new_select_items = std::mem::take(&mut self.select_items)
            .into_iter()
            .map(|expr| rewriter.rewrite_expr(expr))
            .collect::<Vec<_>>();
        self.select_items = new_select_items;

        if let Some(from) = &mut self.from {
            from.rewrite_exprs_recursive(rewriter);
        }

        self.where_clause =
            std::mem::take(&mut self.where_clause).map(|expr| rewriter.rewrite_expr(expr));

        let new_group_by = match &mut self.group_by {
            GroupBy::GroupKey(group_key) => GroupBy::GroupKey(
                std::mem::take(group_key)
                    .into_iter()
                    .map(|expr| rewriter.rewrite_expr(expr))
                    .collect::<Vec<_>>(),
            ),
            GroupBy::GroupingSets(grouping_sets) => GroupBy::GroupingSets(
                std::mem::take(grouping_sets)
                    .into_iter()
                    .map(|set| {
                        set.into_iter()
                            .map(|expr| rewriter.rewrite_expr(expr))
                            .collect()
                    })
                    .collect::<Vec<_>>(),
            ),
            GroupBy::Rollup(rollup) => GroupBy::Rollup(
                std::mem::take(rollup)
                    .into_iter()
                    .map(|set| {
                        set.into_iter()
                            .map(|expr| rewriter.rewrite_expr(expr))
                            .collect()
                    })
                    .collect::<Vec<_>>(),
            ),
            GroupBy::Cube(cube) => GroupBy::Cube(
                std::mem::take(cube)
                    .into_iter()
                    .map(|set| {
                        set.into_iter()
                            .map(|expr| rewriter.rewrite_expr(expr))
                            .collect()
                    })
                    .collect::<Vec<_>>(),
            ),
        };
        self.group_by = new_group_by;

        self.having = std::mem::take(&mut self.having).map(|expr| rewriter.rewrite_expr(expr));
    }
}

impl BoundSelect {
    /// The schema returned by this [`BoundSelect`].
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn exprs(&self) -> impl Iterator<Item = &ExprImpl> {
        self.select_items
            .iter()
            .chain(self.group_by.iter())
            .chain(self.where_clause.iter())
            .chain(self.having.iter())
    }

    pub fn exprs_mut(&mut self) -> impl Iterator<Item = &mut ExprImpl> {
        self.select_items
            .iter_mut()
            .chain(self.group_by.iter_mut())
            .chain(self.where_clause.iter_mut())
            .chain(self.having.iter_mut())
    }

    pub fn is_correlated_by_depth(&self, depth: Depth) -> bool {
        self.exprs()
            .any(|expr| expr.has_correlated_input_ref_by_depth(depth))
            || match self.from.as_ref() {
                Some(relation) => relation.is_correlated_by_depth(depth),
                None => false,
            }
    }

    pub fn is_correlated_by_correlated_id(&self, correlated_id: CorrelatedId) -> bool {
        self.exprs()
            .any(|expr| expr.has_correlated_input_ref_by_correlated_id(correlated_id))
            || match self.from.as_ref() {
                Some(relation) => relation.is_correlated_by_correlated_id(correlated_id),
                None => false,
            }
    }

    pub fn collect_correlated_indices_by_depth_and_assign_id(
        &mut self,
        depth: Depth,
        correlated_id: CorrelatedId,
    ) -> Vec<usize> {
        let mut correlated_indices = self
            .exprs_mut()
            .flat_map(|expr| {
                expr.collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id)
            })
            .collect_vec();

        if let Some(relation) = self.from.as_mut() {
            correlated_indices.extend(
                relation.collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id),
            );
        }

        correlated_indices
    }
}

#[derive(Debug, Clone)]
pub enum BoundDistinct {
    All,
    Distinct,
    DistinctOn(Vec<ExprImpl>),
}

impl RewriteExprsRecursive for BoundDistinct {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        if let Self::DistinctOn(exprs) = self {
            let new_exprs = std::mem::take(exprs)
                .into_iter()
                .map(|expr| rewriter.rewrite_expr(expr))
                .collect::<Vec<_>>();
            exprs.extend(new_exprs);
        }
    }
}

impl BoundDistinct {
    pub const fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }

    pub const fn is_distinct(&self) -> bool {
        matches!(self, Self::Distinct)
    }
}

impl Binder {
    pub(super) fn bind_select(&mut self, select: &Select) -> Result<BoundSelect> {
        // Bind FROM clause.
        let from = self.bind_vec_table_with_joins(&select.from)?;

        // Bind WINDOW clause early - store named window definitions for window function resolution
        let mut named_windows = HashMap::new();
        for named_window in &select.window {
            let window_name = named_window.name.real_value();
            if named_windows.contains_key(&window_name) {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "window \"{}\" is already defined",
                    window_name
                ))
                .into());
            }
            named_windows.insert(window_name, named_window.window_spec.clone());
        }

        // Store window definitions in bind context for window function resolution
        self.context.named_windows = named_windows.clone();

        // Bind SELECT clause.
        let (select_items, aliases) = self.bind_select_list(&select.projection)?;
        let out_name_to_index = Self::build_name_to_index(aliases.iter().filter_map(Clone::clone));

        // Bind DISTINCT ON.
        let distinct =
            self.bind_distinct_on(&select.distinct, &out_name_to_index, &select_items)?;

        // Bind WHERE clause.
        self.context.clause = Some(Clause::Where);
        let selection = select
            .selection
            .as_ref()
            .map(|expr| {
                self.bind_expr(expr)
                    .and_then(|expr| expr.enforce_bool_clause("WHERE"))
            })
            .transpose()?;
        self.context.clause = None;

        // Bind GROUP BY clause.
        self.context.clause = Some(Clause::GroupBy);

        // Only support one grouping item in group by clause
        let group_by = if select.group_by.len() == 1
            && let Expr::GroupingSets(grouping_sets) = &select.group_by[0]
        {
            GroupBy::GroupingSets(self.bind_grouping_items_expr_in_select(
                grouping_sets.clone(),
                &out_name_to_index,
                &select_items,
            )?)
        } else if select.group_by.len() == 1
            && let Expr::Rollup(rollup) = &select.group_by[0]
        {
            GroupBy::Rollup(self.bind_grouping_items_expr_in_select(
                rollup.clone(),
                &out_name_to_index,
                &select_items,
            )?)
        } else if select.group_by.len() == 1
            && let Expr::Cube(cube) = &select.group_by[0]
        {
            GroupBy::Cube(self.bind_grouping_items_expr_in_select(
                cube.clone(),
                &out_name_to_index,
                &select_items,
            )?)
        } else {
            if select.group_by.iter().any(|expr| {
                matches!(expr, Expr::GroupingSets(_))
                    || matches!(expr, Expr::Rollup(_))
                    || matches!(expr, Expr::Cube(_))
            }) {
                return Err(ErrorCode::BindError(
                    "Only support one grouping item in group by clause".to_owned(),
                )
                .into());
            }
            GroupBy::GroupKey(
                select
                    .group_by
                    .iter()
                    .map(|expr| {
                        self.bind_group_by_expr_in_select(expr, &out_name_to_index, &select_items)
                    })
                    .try_collect()?,
            )
        };
        self.context.clause = None;

        // Bind HAVING clause.
        self.context.clause = Some(Clause::Having);
        let having = select
            .having
            .as_ref()
            .map(|expr| {
                self.bind_expr(expr)
                    .and_then(|expr| expr.enforce_bool_clause("HAVING"))
            })
            .transpose()?;
        self.context.clause = None;

        // Store field from `ExprImpl` to support binding `field_desc` in `subquery`.
        let fields = select_items
            .iter()
            .zip_eq_fast(aliases.iter())
            .map(|(s, a)| {
                let name = a.clone().unwrap_or_else(|| UNNAMED_COLUMN.to_owned());
                Ok(Field::with_name(s.return_type(), name))
            })
            .collect::<Result<Vec<Field>>>()?;

        if let Some(Relation::Share(bound)) = &from
            && matches!(bound.input, BoundShareInput::ChangeLog(_))
            && fields.iter().filter(|&x| x.name.eq(CHANGELOG_OP)).count() > 1
        {
            return Err(ErrorCode::BindError(
                "The source table of changelog cannot have `changelog_op`, please rename it first"
                    .to_owned(),
            )
            .into());
        }

        Ok(BoundSelect {
            distinct,
            select_items,
            aliases,
            from,
            where_clause: selection,
            group_by,
            having,
            window: named_windows,
            schema: Schema { fields },
        })
    }

    pub fn bind_select_list(
        &mut self,
        select_items: &[SelectItem],
    ) -> Result<(Vec<ExprImpl>, Vec<Option<String>>)> {
        let mut select_list = vec![];
        let mut aliases = vec![];
        for item in select_items {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let alias = derive_alias(expr);
                    let bound = self.bind_expr(expr)?;
                    select_list.push(bound);
                    aliases.push(alias);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    check_column_name_not_reserved(&alias.real_value())?;

                    let expr = self.bind_expr(expr)?;
                    select_list.push(expr);
                    aliases.push(Some(alias.real_value()));
                }
                SelectItem::QualifiedWildcard(obj_name, except) => {
                    let (schema_name, table_name) =
                        Binder::resolve_schema_qualified_name(&self.db_name, obj_name)?;
                    let except_indices = self.generate_except_indices(except.as_deref())?;
                    let (begin, end) = self
                        .context
                        .resolve_relation_range(&table_name, &schema_name)?;
                    let (exprs, names) = Self::iter_bound_columns(
                        self.context.columns[begin..end]
                            .iter()
                            .filter(|c| !c.is_hidden && !except_indices.contains(&c.index)),
                    );
                    select_list.extend(exprs);
                    aliases.extend(names);
                }
                SelectItem::ExprQualifiedWildcard(expr, prefix) => {
                    let (exprs, names) = self.bind_wildcard_field_column(expr, prefix)?;
                    select_list.extend(exprs);
                    aliases.extend(names);
                }
                SelectItem::Wildcard(except) => {
                    if self.context.range_of.is_empty() {
                        return Err(ErrorCode::BindError(
                            "SELECT * with no tables specified is not valid".into(),
                        )
                        .into());
                    }

                    // Bind the column groups
                    // In psql, the USING and NATURAL columns come before the rest of the
                    // columns in a SELECT * statement
                    let (exprs, names) = self.iter_column_groups();
                    select_list.extend(exprs);
                    aliases.extend(names);

                    let except_indices = self.generate_except_indices(except.as_deref())?;

                    // Bind columns that are not in groups
                    let (exprs, names) =
                        Self::iter_bound_columns(self.context.columns[..].iter().filter(|c| {
                            !c.is_hidden
                                && !self
                                    .context
                                    .column_group_context
                                    .mapping
                                    .contains_key(&c.index)
                                && !except_indices.contains(&c.index)
                        }));

                    select_list.extend(exprs);
                    aliases.extend(names);
                    // TODO: we will need to be able to handle wildcard expressions bound to
                    // aliases in the future. We'd then need a
                    // `NaturalGroupContext` bound to each alias
                    // to correctly disambiguate column
                    // references
                    //
                    // We may need to refactor `NaturalGroupContext` to become span aware in
                    // that case.
                }
            }
        }
        assert_eq!(select_list.len(), aliases.len());
        Ok((select_list, aliases))
    }

    /// Bind an `GROUP BY` expression in a [`Select`], which can be either:
    /// * index of an output column
    /// * an arbitrary expression on input columns
    /// * an output-column name
    ///
    /// Note the differences from `bind_order_by_expr_in_query`:
    /// * When a name matches both an input column and an output column, `group by` interprets it as
    ///   input column while `order by` interprets it as output column.
    /// * As the name suggests, `group by` is part of `select` while `order by` is part of `query`.
    ///   A `query` may consist unions of multiple `select`s (each with their own `group by`) but
    ///   only one `order by`.
    /// * Logically / semantically, `group by` evaluates before `select items`, which evaluates
    ///   before `order by`. This means, `group by` can evaluate arbitrary expressions itself, or
    ///   take expressions from `select items` (we `clone` here and `logical_agg` will rewrite those
    ///   `select items` to `InputRef`). However, `order by` can only refer to `select items`, or
    ///   append its extra arbitrary expressions as hidden `select items` for evaluation.
    ///
    /// # Arguments
    ///
    /// * `name_to_index` - output column name -> index. Ambiguous (duplicate) output names are
    ///   marked with `usize::MAX`.
    fn bind_group_by_expr_in_select(
        &mut self,
        expr: &Expr,
        name_to_index: &HashMap<String, usize>,
        select_items: &[ExprImpl],
    ) -> Result<ExprImpl> {
        let name = match &expr {
            Expr::Identifier(ident) => Some(ident.real_value()),
            _ => None,
        };
        match self.bind_expr(expr) {
            Ok(ExprImpl::Literal(lit)) => match lit.get_data() {
                Some(ScalarImpl::Int32(idx)) => idx
                    .saturating_sub(1)
                    .try_into()
                    .ok()
                    .and_then(|i: usize| select_items.get(i).cloned())
                    .ok_or_else(|| {
                        ErrorCode::BindError(format!(
                            "GROUP BY position {idx} is not in select list"
                        ))
                        .into()
                    }),
                _ => Err(ErrorCode::BindError("non-integer constant in GROUP BY".into()).into()),
            },
            Ok(e) => Ok(e),
            Err(e) => match name {
                None => Err(e),
                Some(name) => match name_to_index.get(&name) {
                    None => Err(e),
                    Some(&usize::MAX) => Err(ErrorCode::BindError(format!(
                        "GROUP BY \"{name}\" is ambiguous"
                    ))
                    .into()),
                    Some(out_idx) => Ok(select_items[*out_idx].clone()),
                },
            },
        }
    }

    fn bind_grouping_items_expr_in_select(
        &mut self,
        grouping_items: Vec<Vec<Expr>>,
        name_to_index: &HashMap<String, usize>,
        select_items: &[ExprImpl],
    ) -> Result<Vec<Vec<ExprImpl>>> {
        let mut result = vec![];
        for set in grouping_items {
            let mut set_exprs = vec![];
            for expr in set {
                let name = match &expr {
                    Expr::Identifier(ident) => Some(ident.real_value()),
                    _ => None,
                };
                let expr_impl = match self.bind_expr(&expr) {
                    Ok(ExprImpl::Literal(lit)) => match lit.get_data() {
                        Some(ScalarImpl::Int32(idx)) => idx
                            .saturating_sub(1)
                            .try_into()
                            .ok()
                            .and_then(|i: usize| select_items.get(i).cloned())
                            .ok_or_else(|| {
                                ErrorCode::BindError(format!(
                                    "GROUP BY position {idx} is not in select list"
                                ))
                                .into()
                            }),
                        _ => Err(
                            ErrorCode::BindError("non-integer constant in GROUP BY".into()).into(),
                        ),
                    },
                    Ok(e) => Ok(e),
                    Err(e) => match name {
                        None => Err(e),
                        Some(name) => match name_to_index.get(&name) {
                            None => Err(e),
                            Some(&usize::MAX) => Err(ErrorCode::BindError(format!(
                                "GROUP BY \"{name}\" is ambiguous"
                            ))
                            .into()),
                            Some(out_idx) => Ok(select_items[*out_idx].clone()),
                        },
                    },
                };

                set_exprs.push(expr_impl?);
            }
            result.push(set_exprs);
        }
        Ok(result)
    }

    pub fn bind_returning_list(
        &mut self,
        returning_items: Vec<SelectItem>,
    ) -> Result<(Vec<ExprImpl>, Vec<Field>)> {
        let (returning_list, aliases) = self.bind_select_list(&returning_items)?;
        if returning_list
            .iter()
            .any(|expr| expr.has_agg_call() || expr.has_window_function())
        {
            return Err(RwError::from(ErrorCode::BindError(
                "should not have agg/window in the `RETURNING` list".to_owned(),
            )));
        }

        let fields = returning_list
            .iter()
            .zip_eq_fast(aliases.iter())
            .map(|(s, a)| {
                let name = a.clone().unwrap_or_else(|| UNNAMED_COLUMN.to_owned());
                Ok::<Field, RwError>(Field::with_name(s.return_type(), name))
            })
            .try_collect()?;
        Ok((returning_list, fields))
    }

    pub fn iter_bound_columns<'a>(
        column_binding: impl Iterator<Item = &'a ColumnBinding>,
    ) -> (Vec<ExprImpl>, Vec<Option<String>>) {
        column_binding
            .map(|c| {
                (
                    InputRef::new(c.index, c.field.data_type.clone()).into(),
                    Some(c.field.name.clone()),
                )
            })
            .unzip()
    }

    pub fn iter_column_groups(&self) -> (Vec<ExprImpl>, Vec<Option<String>>) {
        self.context
            .column_group_context
            .groups
            .values()
            .rev() // ensure that the outermost col group gets put first in the list
            .map(|g| {
                if let Some(col) = &g.non_nullable_column {
                    let c = &self.context.columns[*col];
                    (
                        InputRef::new(c.index, c.field.data_type.clone()).into(),
                        Some(c.field.name.clone()),
                    )
                } else {
                    let mut input_idxes = g.indices.iter().collect::<Vec<_>>();
                    input_idxes.sort();
                    let inputs = input_idxes
                        .into_iter()
                        .map(|index| {
                            let column = &self.context.columns[*index];
                            InputRef::new(column.index, column.field.data_type.clone()).into()
                        })
                        .collect::<Vec<_>>();
                    let c = &self.context.columns[*g.indices.iter().next().unwrap()];
                    (
                        FunctionCall::new(ExprType::Coalesce, inputs)
                            .expect("Failure binding COALESCE function call")
                            .into(),
                        Some(c.field.name.clone()),
                    )
                }
            })
            .unzip()
    }

    /// Bind `DISTINCT` clause in a [`Select`].
    /// Note that for `DISTINCT ON`, each expression is interpreted in the same way as `ORDER BY`
    /// expression, which means it will be bound in the following order:
    ///
    /// * as an output-column name (can use aliases)
    /// * as an index (from 1) of an output column
    /// * as an arbitrary expression (cannot use aliases)
    ///
    /// See also the `bind_order_by_expr_in_query` method.
    ///
    /// # Arguments
    ///
    /// * `name_to_index` - output column name -> index. Ambiguous (duplicate) output names are
    ///   marked with `usize::MAX`.
    fn bind_distinct_on(
        &mut self,
        distinct: &Distinct,
        name_to_index: &HashMap<String, usize>,
        select_items: &[ExprImpl],
    ) -> Result<BoundDistinct> {
        Ok(match distinct {
            Distinct::All => BoundDistinct::All,
            Distinct::Distinct => BoundDistinct::Distinct,
            Distinct::DistinctOn(exprs) => {
                let mut bound_exprs = vec![];
                for expr in exprs {
                    let expr_impl = match expr {
                        Expr::Identifier(name)
                            if let Some(index) = name_to_index.get(&name.real_value()) =>
                        {
                            match *index {
                                usize::MAX => {
                                    return Err(ErrorCode::BindError(format!(
                                        "DISTINCT ON \"{}\" is ambiguous",
                                        name.real_value()
                                    ))
                                    .into());
                                }
                                _ => select_items[*index].clone(),
                            }
                        }
                        Expr::Value(Value::Number(number)) => match number.parse::<usize>() {
                            Ok(index) if 1 <= index && index <= select_items.len() => {
                                let idx_from_0 = index - 1;
                                select_items[idx_from_0].clone()
                            }
                            _ => {
                                return Err(ErrorCode::InvalidInputSyntax(format!(
                                    "Invalid ordinal number in DISTINCT ON: {}",
                                    number
                                ))
                                .into());
                            }
                        },
                        expr => self.bind_expr(expr)?,
                    };
                    bound_exprs.push(expr_impl);
                }
                BoundDistinct::DistinctOn(bound_exprs)
            }
        })
    }

    pub(super) fn generate_except_indices(
        &mut self,
        except: Option<&[Expr]>,
    ) -> Result<HashSet<usize>> {
        let mut except_indices: HashSet<usize> = HashSet::new();
        if let Some(exprs) = except {
            for expr in exprs {
                let bound = self.bind_expr(expr)?;
                match bound {
                    ExprImpl::InputRef(inner) => {
                        if !except_indices.insert(inner.index) {
                            return Err(ErrorCode::BindError(
                                "Duplicate entry in except list".into(),
                            )
                            .into());
                        }
                    }
                    _ => {
                        return Err(ErrorCode::BindError(
                            "Only support column name in except list".into(),
                        )
                        .into());
                    }
                }
            }
        }
        Ok(except_indices)
    }
}

fn derive_alias(expr: &Expr) -> Option<String> {
    match expr.clone() {
        Expr::Identifier(ident) => Some(ident.real_value()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.real_value()),
        Expr::FieldIdentifier(_, idents) => idents.last().map(|ident| ident.real_value()),
        Expr::Function(func) => Some(func.name.real_value()),
        Expr::Extract { .. } => Some("extract".to_owned()),
        Expr::Case { .. } => Some("case".to_owned()),
        Expr::Cast { expr, data_type } => {
            derive_alias(&expr).or_else(|| data_type_to_alias(&data_type))
        }
        Expr::TypedString { data_type, .. } => data_type_to_alias(&data_type),
        Expr::Value(Value::Interval { .. }) => Some("interval".to_owned()),
        Expr::Row(_) => Some("row".to_owned()),
        Expr::Array(_) => Some("array".to_owned()),
        Expr::Index { obj, index: _ } => derive_alias(&obj),
        _ => None,
    }
}

fn data_type_to_alias(data_type: &AstDataType) -> Option<String> {
    let alias = match data_type {
        AstDataType::Char(_) => "bpchar".to_owned(),
        AstDataType::Varchar => "varchar".to_owned(),
        AstDataType::Uuid => "uuid".to_owned(),
        AstDataType::Decimal(_, _) => "numeric".to_owned(),
        AstDataType::Real | AstDataType::Float(Some(1..=24)) => "float4".to_owned(),
        AstDataType::Double | AstDataType::Float(Some(25..=53) | None) => "float8".to_owned(),
        AstDataType::Float(Some(0 | 54..)) => unreachable!(),
        AstDataType::SmallInt => "int2".to_owned(),
        AstDataType::Int => "int4".to_owned(),
        AstDataType::BigInt => "int8".to_owned(),
        AstDataType::Boolean => "bool".to_owned(),
        AstDataType::Date => "date".to_owned(),
        AstDataType::Time(tz) => format!("time{}", if *tz { "z" } else { "" }),
        AstDataType::Timestamp(tz) => {
            format!("timestamp{}", if *tz { "tz" } else { "" })
        }
        AstDataType::Interval => "interval".to_owned(),
        AstDataType::Regclass => "regclass".to_owned(),
        AstDataType::Regproc => "regproc".to_owned(),
        AstDataType::Text => "text".to_owned(),
        AstDataType::Bytea => "bytea".to_owned(),
        AstDataType::Jsonb => "jsonb".to_owned(),
        AstDataType::Variant => "variant".to_owned(),
        AstDataType::Array(ty) => return data_type_to_alias(ty),
        AstDataType::Custom(ty) => format!("{}", ty),
        AstDataType::Vector(_) => "vector".to_owned(),
        AstDataType::Struct(_) | AstDataType::Map(_) => {
            // It doesn't bother to derive aliases for these types.
            return None;
        }
    };

    Some(alias)
}
