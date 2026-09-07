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

use itertools::Itertools;
use risingwave_pb::expr::expr_node::{self, RexNode};
use risingwave_pb::expr::{ExprNode, FunctionCall, UserDefinedFunction};
use risingwave_pb::plan_common::PbColumnDesc;
use risingwave_sqlparser::ast::{
    Array, CdcTableInfo, CreateSink, CreateSinkStatement, CreateSourceStatement,
    CreateSubscriptionStatement, Distinct, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgList, Ident, ObjectName, Query, SelectItem, SetExpr, Statement, TableAlias,
    TableFactor, TableWithJoins, Window,
};
use risingwave_sqlparser::parser::Parser;

/// `alter_relation_rename` renames a relation to a new name in its `Create` statement, and returns
/// the updated definition raw sql. Note that the `definition` must be a `Create` statement and the
/// `new_name` must be a valid identifier, it should be validated before calling this function. To
/// update all relations that depend on the renamed one, use `alter_relation_rename_refs`.
pub fn alter_relation_rename(definition: &str, new_name: &str) -> String {
    // This happens when we try to rename a table that's created by `CREATE TABLE AS`. Remove it
    // when we support `SHOW CREATE TABLE` for `CREATE TABLE AS`.
    if definition.is_empty() {
        tracing::warn!("found empty definition when renaming relation, ignored.");
        return definition.into();
    }
    let ast = Parser::parse_sql(definition).expect("failed to parse relation definition");
    let mut stmt =
        Itertools::exactly_one(ast.into_iter()).expect("should contains only one statement");

    match &mut stmt {
        Statement::CreateTable { name, .. }
        | Statement::CreateView { name, .. }
        | Statement::CreateIndex { name, .. }
        | Statement::CreateSource {
            stmt: CreateSourceStatement {
                source_name: name, ..
            },
        }
        | Statement::CreateSubscription {
            stmt:
                CreateSubscriptionStatement {
                    subscription_name: name,
                    ..
                },
        }
        | Statement::CreateSink {
            stmt: CreateSinkStatement {
                sink_name: name, ..
            },
        } => replace_table_name(name, new_name),
        _ => unreachable!(),
    };

    stmt.to_string()
}

/// `alter_relation_rename_refs` updates all references of renamed-relation in the definition of
/// target relation's `Create` statement.
pub fn alter_relation_rename_refs(definition: &str, from: &str, to: &str) -> String {
    let ast = Parser::parse_sql(definition).expect("failed to parse relation definition");
    let mut stmt =
        Itertools::exactly_one(ast.into_iter()).expect("should contains only one statement");

    match &mut stmt {
        Statement::CreateTable {
            query: Some(query), ..
        }
        | Statement::CreateView { query, .. }
        | Statement::Query(query) // Used by view, actually we store a query as the definition of view.
        | Statement::CreateSink {
            stmt:
            CreateSinkStatement {
                sink_from: CreateSink::AsQuery(query),
                into_table_name: None,
                ..
            },
        } => {
            QueryRewriter::rewrite_query(query, from, to);
        }
        Statement::CreateIndex { table_name, .. }
        | Statement::CreateSink {
            stmt:
            CreateSinkStatement {
                sink_from: CreateSink::From(table_name),
                into_table_name: None,
                ..
            },
        }| Statement::CreateSubscription {
            stmt:
            CreateSubscriptionStatement {
                subscription_from: table_name,
                ..
            },
        } | Statement::CreateTable {
            cdc_table_info:
            Some(CdcTableInfo {
                source_name: table_name,
                ..
            }),
            ..
        } => replace_table_name(table_name, to),
        Statement::CreateSink {
            stmt: CreateSinkStatement {
                sink_from,
                into_table_name: Some(table_name),
                ..
            }
        } => {
            let idx = table_name.0.len() - 1;
            if table_name.0[idx].real_value() == from {
                table_name.0[idx] = Ident::from_real_value(to);
            } else {
                match sink_from {
                    CreateSink::From(table_name) => replace_table_name(table_name, to),
                    CreateSink::AsQuery(query) => QueryRewriter::rewrite_query(query, from, to),
                }
            }
        }
        _ => unreachable!(),
    };
    stmt.to_string()
}

/// Replace the last ident in the `table_name` with the given name, the object name is ensured to be
/// non-empty. e.g. `schema.table` or `database.schema.table`.
fn replace_table_name(table_name: &mut ObjectName, to: &str) {
    let idx = table_name.0.len() - 1;
    table_name.0[idx] = Ident::from_real_value(to);
}

/// `QueryRewriter` is a visitor that updates all references of relation named `from` to `to` in the
/// given query, which is the part of create statement of `relation`.
struct QueryRewriter<'a> {
    from: &'a str,
    to: &'a str,
}

impl QueryRewriter<'_> {
    fn rewrite_query(query: &mut Query, from: &str, to: &str) {
        let rewriter = QueryRewriter { from, to };
        rewriter.visit_query(query)
    }

    /// Visit the query and update all references of relation named `from` to `to`.
    fn visit_query(&self, query: &mut Query) {
        if let Some(with) = &mut query.with {
            for cte_table in &mut with.cte_tables {
                match &mut cte_table.cte_inner {
                    risingwave_sqlparser::ast::CteInner::Query(query) => self.visit_query(query),
                    risingwave_sqlparser::ast::CteInner::ChangeLog(name) => {
                        let idx = name.0.len() - 1;
                        if name.0[idx].real_value() == self.from {
                            replace_table_name(name, self.to);
                        }
                    }
                }
            }
        }
        self.visit_set_expr(&mut query.body);
        for expr in &mut query.order_by {
            self.visit_expr(&mut expr.expr);
        }
    }

    /// Visit table factor and update all references of relation named `from` to `to`.
    /// Rewrite idents(i.e. `schema.table`, `table`) that contains the old name in the
    /// following pattern:
    /// 1. `FROM a` to `FROM new_a AS a`
    /// 2. `FROM a AS b` to `FROM new_a AS b`
    ///
    /// So that we DON'T have to:
    /// 1. rewrite the select and expr part like `schema.table.column`, `table.column`,
    ///    `alias.column` etc.
    /// 2. handle the case that the old name is used as alias.
    /// 3. handle the case that the new name is used as alias.
    fn visit_table_factor(&self, table_factor: &mut TableFactor) {
        match table_factor {
            TableFactor::Table { name, alias, .. } => {
                let idx = name.0.len() - 1;
                if name.0[idx].real_value() == self.from {
                    if alias.is_none() {
                        *alias = Some(TableAlias {
                            name: Ident::from_real_value(self.from),
                            columns: vec![],
                        });
                    }
                    name.0[idx] = Ident::from_real_value(self.to);
                }
            }
            TableFactor::Derived { subquery, .. } => self.visit_query(subquery),
            TableFactor::TableFunction { args, .. } => {
                for arg in args {
                    self.visit_function_arg(arg);
                }
            }
            TableFactor::NestedJoin(table_with_joins) => {
                self.visit_table_with_joins(table_with_joins);
            }
            TableFactor::MatchRecognize { table, .. } => {
                // Only the input table can reference a relation: the binder rejects subqueries in
                // both DEFINE and MEASURES (they have no representation in the executor's scalar
                // expressions), so the remaining clauses contain no rename targets.
                self.visit_table_factor(table);
            }
        }
    }

    /// Visit table with joins and update all references of relation named `from` to `to`.
    fn visit_table_with_joins(&self, table_with_joins: &mut TableWithJoins) {
        self.visit_table_factor(&mut table_with_joins.relation);
        for join in &mut table_with_joins.joins {
            self.visit_table_factor(&mut join.relation);
        }
    }

    /// Visit query body expression and update all references.
    fn visit_set_expr(&self, set_expr: &mut SetExpr) {
        match set_expr {
            SetExpr::Select(select) => {
                if let Distinct::DistinctOn(exprs) = &mut select.distinct {
                    for expr in exprs {
                        self.visit_expr(expr);
                    }
                }
                for select_item in &mut select.projection {
                    self.visit_select_item(select_item);
                }
                for from_item in &mut select.from {
                    self.visit_table_with_joins(from_item);
                }
                if let Some(where_clause) = &mut select.selection {
                    self.visit_expr(where_clause);
                }
                for expr in &mut select.group_by {
                    self.visit_expr(expr);
                }
                if let Some(having) = &mut select.having {
                    self.visit_expr(having);
                }
                for named_window in &mut select.window {
                    for expr in &mut named_window.window_spec.partition_by {
                        self.visit_expr(expr);
                    }
                    for expr in &mut named_window.window_spec.order_by {
                        self.visit_expr(&mut expr.expr);
                    }
                }
            }
            SetExpr::Query(query) => self.visit_query(query),
            SetExpr::SetOperation { left, right, .. } => {
                self.visit_set_expr(left);
                self.visit_set_expr(right);
            }
            SetExpr::Values(_) => {}
        }
    }

    /// Visit function arguments and update all references.
    fn visit_function_arg(&self, function_arg: &mut FunctionArg) {
        match function_arg {
            FunctionArg::Unnamed(arg) | FunctionArg::Named { arg, .. } => match arg {
                FunctionArgExpr::Expr(expr) | FunctionArgExpr::ExprQualifiedWildcard(expr, _) => {
                    self.visit_expr(expr)
                }
                FunctionArgExpr::QualifiedWildcard(_, None) | FunctionArgExpr::Wildcard(None) => {}
                FunctionArgExpr::QualifiedWildcard(_, Some(exprs))
                | FunctionArgExpr::Wildcard(Some(exprs)) => {
                    for expr in exprs {
                        self.visit_expr(expr);
                    }
                }
                FunctionArgExpr::SecretRef(_) => {}
            },
        }
    }

    fn visit_function_arg_list(&self, arg_list: &mut FunctionArgList) {
        for arg in &mut arg_list.args {
            self.visit_function_arg(arg);
        }
        for expr in &mut arg_list.order_by {
            self.visit_expr(&mut expr.expr)
        }
    }

    /// Visit function and update all references.
    fn visit_function(&self, function: &mut Function) {
        self.visit_function_arg_list(&mut function.arg_list);
        if let Some(over) = &mut function.over {
            match over {
                Window::Spec(window) => {
                    for expr in &mut window.partition_by {
                        self.visit_expr(expr);
                    }
                    for expr in &mut window.order_by {
                        self.visit_expr(&mut expr.expr);
                    }
                }
                Window::Name(_) => {
                    // Named window references don't contain expressions to rewrite
                }
            }
        }
    }

    /// Visit expression and update all references.
    fn visit_expr(&self, expr: &mut Expr) {
        match expr {
            Expr::FieldIdentifier(expr, ..)
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::IsJson { expr, .. }
            | Expr::InList { expr, .. }
            | Expr::SomeOp(expr)
            | Expr::AllOp(expr)
            | Expr::UnaryOp { expr, .. }
            | Expr::Cast { expr, .. }
            | Expr::TryCast { expr, .. }
            | Expr::AtTimeZone {
                timestamp: expr, ..
            }
            | Expr::Extract { expr, .. }
            | Expr::Substring { expr, .. }
            | Expr::Overlay { expr, .. }
            | Expr::Trim { expr, .. }
            | Expr::Nested(expr)
            | Expr::Index { obj: expr, .. }
            | Expr::ArrayRangeIndex { obj: expr, .. } => self.visit_expr(expr),

            Expr::Position { substring, string } => {
                self.visit_expr(substring);
                self.visit_expr(string);
            }

            Expr::InSubquery { expr, subquery, .. } => {
                self.visit_expr(expr);
                self.visit_query(subquery);
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.visit_expr(expr);
                self.visit_expr(low);
                self.visit_expr(high);
            }
            Expr::Like {
                expr, pattern: pat, ..
            } => {
                self.visit_expr(expr);
                self.visit_expr(pat);
            }
            Expr::ILike {
                expr, pattern: pat, ..
            } => {
                self.visit_expr(expr);
                self.visit_expr(pat);
            }
            Expr::SimilarTo {
                expr, pattern: pat, ..
            } => {
                self.visit_expr(expr);
                self.visit_expr(pat);
            }

            Expr::IsDistinctFrom(expr1, expr2)
            | Expr::IsNotDistinctFrom(expr1, expr2)
            | Expr::BinaryOp {
                left: expr1,
                right: expr2,
                ..
            } => {
                self.visit_expr(expr1);
                self.visit_expr(expr2);
            }
            Expr::Function(function) => self.visit_function(function),
            Expr::Exists(query) | Expr::Subquery(query) | Expr::ArraySubquery(query) => {
                self.visit_query(query)
            }

            Expr::GroupingSets(exprs_vec) | Expr::Cube(exprs_vec) | Expr::Rollup(exprs_vec) => {
                for exprs in exprs_vec {
                    for expr in exprs {
                        self.visit_expr(expr);
                    }
                }
            }

            Expr::Row(exprs) | Expr::Array(Array { elem: exprs, .. }) => {
                for expr in exprs {
                    self.visit_expr(expr);
                }
            }
            Expr::Map { entries } => {
                for (key, value) in entries {
                    self.visit_expr(key);
                    self.visit_expr(value);
                }
            }

            Expr::LambdaFunction { body, args: _ } => self.visit_expr(body),

            // No need to visit.
            Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::Collate { .. }
            | Expr::Value(_)
            | Expr::Parameter { .. }
            | Expr::TypedString { .. }
            | Expr::Case { .. } => {}
        }
    }

    /// Visit select item and update all references.
    fn visit_select_item(&self, select_item: &mut SelectItem) {
        match select_item {
            SelectItem::UnnamedExpr(expr)
            | SelectItem::ExprQualifiedWildcard(expr, _)
            | SelectItem::ExprWithAlias { expr, .. } => self.visit_expr(expr),
            SelectItem::QualifiedWildcard(_, None) | SelectItem::Wildcard(None) => {}
            SelectItem::QualifiedWildcard(_, Some(exprs)) | SelectItem::Wildcard(Some(exprs)) => {
                for expr in exprs {
                    self.visit_expr(expr);
                }
            }
        }
    }
}

/// Rewrite the expression in index item after there's a schema change on the primary table.
// TODO: move this out of `rename.rs`, this has nothing to do with renaming.
pub struct IndexItemRewriter {
    pub original_columns: Vec<PbColumnDesc>,
    pub new_columns: Vec<PbColumnDesc>,
}

impl IndexItemRewriter {
    pub fn rewrite_expr(&self, expr: &mut ExprNode) {
        let rex_node = expr.rex_node.as_mut().unwrap();
        match rex_node {
            RexNode::InputRef(idx) => {
                let old_idx = *idx as usize;
                let original_column = &self.original_columns[old_idx];
                let (new_idx, new_column) = self
                    .new_columns
                    .iter()
                    .find_position(|c| c.column_id == original_column.column_id)
                    .expect("should already checked index referencing column still exists");
                *idx = new_idx as u32;

                // If there's a type change, we need to wrap it with an internal `CompositeCast` to
                // maintain the correct return type. It cannot execute and will be eliminated in
                // the frontend when rebuilding the index items.
                if new_column.column_type != original_column.column_type {
                    let old_type = original_column.column_type.clone().unwrap();
                    let new_type = new_column.column_type.clone().unwrap();

                    assert_eq!(&old_type, expr.return_type.as_ref().unwrap());
                    expr.return_type = Some(new_type); // update return type of `InputRef`

                    let new_expr_node = ExprNode {
                        function_type: expr_node::Type::CompositeCast as _,
                        return_type: Some(old_type),
                        rex_node: RexNode::FuncCall(FunctionCall {
                            children: vec![expr.clone()],
                        })
                        .into(),
                    };

                    *expr = new_expr_node;
                }
            }
            RexNode::Constant(_) => {}
            RexNode::Udf(udf) => self.rewrite_udf(udf),
            RexNode::FuncCall(function_call) => self.rewrite_function_call(function_call),
            RexNode::Now(_) | RexNode::SecretRef(_) => {}
        }
    }

    fn rewrite_udf(&self, udf: &mut UserDefinedFunction) {
        udf.children
            .iter_mut()
            .for_each(|expr| self.rewrite_expr(expr));
    }

    fn rewrite_function_call(&self, function_call: &mut FunctionCall) {
        function_call
            .children
            .iter_mut()
            .for_each(|expr| self.rewrite_expr(expr));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alter_table_rename() {
        let definition = "CREATE TABLE foo (a int, b int)";
        let new_name = "bar";
        let expected = "CREATE TABLE bar (a INT, b INT)";
        let actual = alter_relation_rename(definition, new_name);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_rename_index_refs() {
        let definition = "CREATE INDEX idx1 ON foo(v1 DESC, v2)";
        let from = "foo";
        let to = "bar";
        let expected = "CREATE INDEX idx1 ON bar(v1 DESC, v2)";
        let actual = alter_relation_rename_refs(definition, from, to);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_rename_sink_refs() {
        let definition =
            "CREATE SINK sink_t FROM foo WITH (connector = 'kafka', format = 'append_only')";
        let from = "foo";
        let to = "bar";
        let expected =
            "CREATE SINK sink_t FROM bar WITH (connector = 'kafka', format = 'append_only')";
        let actual = alter_relation_rename_refs(definition, from, to);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_rename_with_alias_refs() {
        let definition =
            "CREATE MATERIALIZED VIEW mv1 AS SELECT foo.v1 AS m1v, foo.v2 AS m2v FROM foo";
        let from = "foo";
        let to = "bar";
        let expected =
            "CREATE MATERIALIZED VIEW mv1 AS SELECT foo.v1 AS m1v, foo.v2 AS m2v FROM bar AS foo";
        let actual = alter_relation_rename_refs(definition, from, to);
        assert_eq!(expected, actual);

        let definition = "CREATE MATERIALIZED VIEW mv1 AS SELECT foo.v1 AS m1v, (foo.v2).v3 AS m2v FROM foo WHERE foo.v1 = 1 AND (foo.v2).v3 IS TRUE";
        let expected = "CREATE MATERIALIZED VIEW mv1 AS SELECT foo.v1 AS m1v, (foo.v2).v3 AS m2v FROM bar AS foo WHERE foo.v1 = 1 AND (foo.v2).v3 IS TRUE";
        let actual = alter_relation_rename_refs(definition, from, to);
        assert_eq!(expected, actual);

        let definition = "CREATE MATERIALIZED VIEW mv1 AS SELECT bar.v1 AS m1v, (bar.v2).v3 AS m2v FROM foo AS bar WHERE bar.v1 = 1";
        let expected = "CREATE MATERIALIZED VIEW mv1 AS SELECT bar.v1 AS m1v, (bar.v2).v3 AS m2v FROM bar AS bar WHERE bar.v1 = 1";
        let actual = alter_relation_rename_refs(definition, from, to);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_rename_with_complex_funcs() {
        let definition = "CREATE MATERIALIZED VIEW mv1 AS SELECT \
                            agg1(\
                              foo.v1, func2(foo.v2) \
                              ORDER BY \
                              (SELECT foo.v3 FROM foo), \
                              (SELECT first_value(foo.v4) OVER (PARTITION BY (SELECT foo.v5 FROM foo) ORDER BY (SELECT foo.v6 FROM foo)) FROM foo)\
                            ) \
                          FROM foo";
        let from = "foo";
        let to = "bar";
        let expected = "CREATE MATERIALIZED VIEW mv1 AS SELECT \
                          agg1(\
                            foo.v1, func2(foo.v2) \
                            ORDER BY \
                            (SELECT foo.v3 FROM bar AS foo), \
                            (SELECT first_value(foo.v4) OVER (PARTITION BY (SELECT foo.v5 FROM bar AS foo) ORDER BY (SELECT foo.v6 FROM bar AS foo)) FROM bar AS foo)\
                          ) \
                        FROM bar AS foo";
        let actual = alter_relation_rename_refs(definition, from, to);
        assert_eq!(expected, actual);
    }
}
