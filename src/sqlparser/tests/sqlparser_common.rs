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

//! Test SQL syntax, which all sqlparser dialects must parse in the same way.
//!
//! Note that it does not mean all SQL here is valid in all the dialects, only
//! that 1) it's either standard or widely supported and 2) it can be parsed by
//! sqlparser regardless of the chosen dialect (i.e. it doesn't conflict with
//! dialect-specific parsing rules).

#[macro_use]
mod test_utils;
use matches::assert_matches;
use risingwave_sqlparser::ast::JoinOperator::Inner;
use risingwave_sqlparser::ast::*;
use risingwave_sqlparser::keywords::ALL_KEYWORDS;
use risingwave_sqlparser::parser::ParserError;
use risingwave_sqlparser::test_utils::*;

#[test]
fn parse_recover() {
    assert_matches!(
        verified_stmt("RECOVER"),
        Statement::Recover {
            database_name: None
        }
    );
    assert_matches!(
        verified_stmt("RECOVER my_database"),
        Statement::Recover {
            database_name: Some(name)
        } if name.to_string() == "my_database"
    );
    assert_matches!(
        verified_stmt("RECOVER \"Database.Name\""),
        Statement::Recover {
            database_name: Some(name)
        } if name.to_string() == "\"Database.Name\""
    );
}

#[test]
fn parse_insert_values() {
    let row = vec![
        Expr::Value(number("1")),
        Expr::Value(number("2")),
        Expr::Value(number("3")),
    ];
    let rows1 = vec![row.clone()];
    let rows2 = vec![row.clone(), row];

    let sql = "INSERT INTO customer VALUES (1, 2, 3)";
    check_one(sql, "customer", &[], &rows1);

    let sql = "INSERT INTO customer VALUES (1, 2, 3), (1, 2, 3)";
    check_one(sql, "customer", &[], &rows2);

    let sql = "INSERT INTO public.customer VALUES (1, 2, 3)";
    check_one(sql, "public.customer", &[], &rows1);

    let sql = "INSERT INTO db.public.customer VALUES (1, 2, 3)";
    check_one(sql, "db.public.customer", &[], &rows1);

    let sql = "INSERT INTO public.customer (id, name, active) VALUES (1, 2, 3)";
    check_one(
        sql,
        "public.customer",
        &["id".to_owned(), "name".to_owned(), "active".to_owned()],
        &rows1,
    );

    fn check_one(
        sql: &str,
        expected_table_name: &str,
        expected_columns: &[String],
        expected_rows: &[Vec<Expr>],
    ) {
        match verified_stmt(sql) {
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                assert_eq!(table_name.to_string(), expected_table_name);
                assert_eq!(columns.len(), expected_columns.len());
                for (index, column) in columns.iter().enumerate() {
                    assert_eq!(
                        column,
                        &Ident::new_unchecked(expected_columns[index].clone())
                    );
                }
                match &source.body {
                    SetExpr::Values(Values(values)) => assert_eq!(values.as_slice(), expected_rows),
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    verified_stmt("INSERT INTO customer WITH foo AS (SELECT 1) SELECT * FROM foo UNION VALUES (1)");
}

#[test]
fn parse_update() {
    let sql = "UPDATE t SET a = 1, b = 2, c = 3, d = DEFAULT WHERE e";
    match verified_stmt(sql) {
        Statement::Update {
            table_name,
            assignments,
            selection,
            ..
        } => {
            assert_eq!(table_name.to_string(), "t".to_owned());
            assert_eq!(
                assignments,
                vec![
                    Assignment {
                        id: vec!["a".into()],
                        value: AssignmentValue::Expr(Expr::Value(number("1"))),
                    },
                    Assignment {
                        id: vec!["b".into()],
                        value: AssignmentValue::Expr(Expr::Value(number("2"))),
                    },
                    Assignment {
                        id: vec!["c".into()],
                        value: AssignmentValue::Expr(Expr::Value(number("3"))),
                    },
                    Assignment {
                        id: vec!["d".into()],
                        value: AssignmentValue::Default,
                    }
                ]
            );
            assert_eq!(selection.unwrap(), Expr::Identifier("e".into()));
        }
        _ => unreachable!(),
    }

    verified_stmt("UPDATE t SET a = 1, a = 2, a = 3");

    let sql = "UPDATE t WHERE 1";
    let res = parse_sql_statements(sql);
    assert!(format!("{}", res.unwrap_err()).contains("expected SET, found: WHERE"));

    let sql = "UPDATE t SET a = 1 extrabadstuff";
    let res = parse_sql_statements(sql);
    assert!(
        format!("{}", res.unwrap_err()).contains("expected end of statement, found: extrabadstuff")
    );
}

#[test]
fn parse_invalid_table_name() {
    let ast = run_parser_method("db.public..customer", |parser| parser.parse_object_name());
    assert!(ast.is_err());
}

#[test]
fn parse_no_table_name() {
    let ast = run_parser_method("", |parser| parser.parse_object_name());
    assert!(ast.is_err());
}

#[test]
fn parse_delete_statement() {
    let sql = "DELETE FROM \"table\"";
    match verified_stmt(sql) {
        Statement::Delete { table_name, .. } => {
            assert_eq!(
                ObjectName(vec![Ident::with_quote_unchecked('"', "table")]),
                table_name
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_where_delete_statement() {
    use self::BinaryOperator::*;

    let sql = "DELETE FROM foo WHERE name = 5";
    match verified_stmt(sql) {
        Statement::Delete {
            table_name,
            selection,
            ..
        } => {
            assert_eq!(ObjectName(vec![Ident::new_unchecked("foo")]), table_name);

            assert_eq!(
                Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new_unchecked("name"))),
                    op: Eq,
                    right: Box::new(Expr::Value(number("5"))),
                },
                selection.unwrap(),
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_top_level() {
    verified_stmt("SELECT 1");
    verified_stmt("(SELECT 1)");
    verified_stmt("((SELECT 1))");
    verified_stmt("VALUES (1)");
}

#[test]
fn parse_simple_select() {
    let sql = "SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT 5";
    let select = verified_only_select(sql);
    assert_eq!(select.distinct, Distinct::All);
    assert_eq!(3, select.projection.len());
    let select = verified_query(sql);
    assert_eq!(
        Some(Expr::Value(Value::Number("5".to_owned()))),
        select.limit
    );
}

#[test]
fn parse_limit_is_not_an_alias() {
    // In dialects supporting LIMIT it shouldn't be parsed as a table alias
    let ast = verified_query("SELECT id FROM customer LIMIT 1");
    assert_eq!(Some(Expr::Value(Value::Number("1".to_owned()))), ast.limit);

    let ast = verified_query("SELECT 1 LIMIT 5");
    assert_eq!(Some(Expr::Value(Value::Number("5".to_owned()))), ast.limit);
}

#[test]
fn parse_select_distinct() {
    let sql = "SELECT DISTINCT name FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(select.distinct, Distinct::Distinct);
    assert_eq!(
        &SelectItem::UnnamedExpr(Expr::Identifier(Ident::new_unchecked("name"))),
        only(&select.projection)
    );
}

#[test]
fn parse_select_distinct_on() {
    let sql = "SELECT DISTINCT ON (id) name FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        select.distinct,
        Distinct::DistinctOn(vec![Expr::Identifier(Ident::new_unchecked("id"))])
    );
    assert_eq!(
        &SelectItem::UnnamedExpr(Expr::Identifier(Ident::new_unchecked("name"))),
        only(&select.projection)
    );
}

#[test]
fn parse_select_all() {
    one_statement_parses_to("SELECT ALL name FROM customer", "SELECT name FROM customer");
}

#[test]
fn parse_select_all_distinct() {
    let result = parse_sql_statements("SELECT ALL DISTINCT name FROM customer");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("syntax error at or near DISTINCT at line 1, column 12")
    );
}

#[test]
fn parse_select_wildcard() {
    let sql = "SELECT * FROM foo";
    let select = verified_only_select(sql);
    assert_eq!(&SelectItem::Wildcard(None), only(&select.projection));

    let sql = "SELECT foo.* FROM foo";
    let select = verified_only_select(sql);
    assert_eq!(
        &SelectItem::QualifiedWildcard(ObjectName(vec![Ident::new_unchecked("foo")]), None),
        only(&select.projection)
    );

    let sql = "SELECT myschema.mytable.* FROM myschema.mytable";
    let select = verified_only_select(sql);
    assert_eq!(
        &SelectItem::QualifiedWildcard(
            ObjectName(vec![
                Ident::new_unchecked("myschema"),
                Ident::new_unchecked("mytable"),
            ]),
            None
        ),
        only(&select.projection)
    );

    let sql = "SELECT * + * FROM foo;";
    let result = parse_sql_statements(sql);
    assert!(format!("{}", result.unwrap_err()).contains("expected end of statement, found: +"));
}

#[test]
fn parse_select_except() {
    let sql = "SELECT * EXCEPT (v1) FROM foo";
    let select = verified_only_select(sql);
    assert_eq!(
        &SelectItem::Wildcard(Some(vec![Expr::Identifier(Ident::new_unchecked("v1"))])),
        only(&select.projection)
    );
}

#[test]
fn parse_count_wildcard() {
    verified_only_select("SELECT COUNT(*) FROM Orders WHERE id = 10");

    verified_only_select(
        "SELECT COUNT(Employee.*) FROM Orders JOIN Employee ON Orders.employee = Employee.id",
    );
}

#[test]
fn parse_column_aliases() {
    let sql = "SELECT a.col + 1 AS newname FROM foo AS a";
    let select = verified_only_select(sql);
    if let SelectItem::ExprWithAlias {
        expr: Expr::BinaryOp { op, right, .. },
        alias,
    } = only(&select.projection)
    {
        assert_eq!(&BinaryOperator::Plus, op);
        assert_eq!(&Expr::Value(number("1")), right.as_ref());
        assert_eq!(&Ident::new_unchecked("newname"), alias);
    } else {
        panic!("expected ExprWithAlias")
    }

    // alias without AS is parsed correctly:
    one_statement_parses_to("SELECT a.col + 1 newname FROM foo AS a", sql);
}

#[test]
fn test_eof_after_as() {
    let res = parse_sql_statements("SELECT foo AS");
    assert!(
        format!("{}", res.unwrap_err()).contains("expected an identifier after AS, found: EOF")
    );

    let res = parse_sql_statements("SELECT 1 FROM foo AS");
    assert!(
        format!("{}", res.unwrap_err()).contains("expected an identifier after AS, found: EOF")
    );
}

#[test]
fn parse_select_count_wildcard() {
    let sql = "SELECT COUNT(*) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Function(Function {
            scalar_as_agg: false,
            name: ObjectName(vec![Ident::new_unchecked("COUNT")]),
            arg_list: FunctionArgList::args_only(vec![FunctionArg::Unnamed(
                FunctionArgExpr::Wildcard(None)
            )]),
            within_group: None,
            filter: None,
            over: None,
        }),
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_select_count_distinct() {
    let sql = "SELECT COUNT(DISTINCT + x) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Function(Function {
            scalar_as_agg: false,
            name: ObjectName(vec![Ident::new_unchecked("COUNT")]),
            arg_list: FunctionArgList::for_agg(
                true,
                vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::UnaryOp {
                    op: UnaryOperator::Plus,
                    expr: Box::new(Expr::Identifier(Ident::new_unchecked("x"))),
                }))],
                vec![]
            ),
            within_group: None,
            filter: None,
            over: None,
        }),
        expr_from_projection(only(&select.projection))
    );

    one_statement_parses_to(
        "SELECT COUNT(ALL + x) FROM customer",
        "SELECT COUNT(+ x) FROM customer",
    );

    let sql = "SELECT COUNT(ALL DISTINCT + x) FROM customer";
    let res = parse_sql_statements(sql);
    assert!(format!("{}", res.unwrap_err()).contains("Cannot specify both ALL and DISTINCT"));
}

#[test]
fn parse_invalid_infix_not() {
    let res = parse_sql_statements("SELECT c FROM t WHERE c NOT (");
    assert!(format!("{}", res.unwrap_err(),).contains("expected end of statement, found: NOT"));
}

#[test]
fn parse_collate() {
    let sql = "SELECT name COLLATE \"de_DE\" FROM customer";
    assert_matches!(
        only(&verified_only_select(sql).projection),
        SelectItem::UnnamedExpr(Expr::Collate { .. })
    );
}

#[test]
fn parse_projection_nested_type() {
    let sql = "SELECT customer.address.state FROM foo";
    let _ast = verified_only_select(sql);
    // TODO: add assertions
}

#[test]
fn parse_null_in_select() {
    let sql = "SELECT NULL";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Value(Value::Null),
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_select_with_date_column_name() {
    let sql = "SELECT date";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Identifier(Ident::new_unchecked("date")),
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_escaped_single_quote_string_predicate() {
    use self::BinaryOperator::*;
    let sql = "SELECT id, fname, lname FROM customer \
               WHERE salary <> 'Jim''s salary'";
    let ast = verified_only_select(sql);
    assert_eq!(
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new_unchecked("salary"))),
            op: NotEq,
            right: Box::new(Expr::Value(Value::SingleQuotedString(
                "Jim's salary".to_owned()
            )))
        }),
        ast.selection,
    );
}

#[test]
fn parse_number() {
    let expr = verified_expr("1.0");

    assert_eq!(expr, Expr::Value(Value::Number("1.0".into())));
}

#[test]
fn parse_compound_expr_1() {
    use self::BinaryOperator::*;
    use self::Expr::*;
    let sql = "a + b * c";
    let ast = run_parser_method(sql, |parser| parser.parse_expr()).unwrap();
    assert_eq!("a + b * c", &ast.to_string());
    assert_eq!(
        BinaryOp {
            left: Box::new(Identifier(Ident::new_unchecked("a"))),
            op: Plus,
            right: Box::new(BinaryOp {
                left: Box::new(Identifier(Ident::new_unchecked("b"))),
                op: Multiply,
                right: Box::new(Identifier(Ident::new_unchecked("c")))
            })
        },
        ast
    );
}

#[test]
fn parse_compound_expr_2() {
    use self::BinaryOperator::*;
    use self::Expr::*;
    let sql = "a * b + c";
    let ast = run_parser_method(sql, |parser| parser.parse_expr()).unwrap();
    assert_eq!("a * b + c", &ast.to_string());
    assert_eq!(
        BinaryOp {
            left: Box::new(BinaryOp {
                left: Box::new(Identifier(Ident::new_unchecked("a"))),
                op: Multiply,
                right: Box::new(Identifier(Ident::new_unchecked("b")))
            }),
            op: Plus,
            right: Box::new(Identifier(Ident::new_unchecked("c")))
        },
        ast
    );
}

#[test]
fn parse_unary_math() {
    use self::Expr::*;
    let sql = "- a + - b";
    let ast = run_parser_method(sql, |parser| parser.parse_expr()).unwrap();
    assert_eq!("- a + - b", &ast.to_string());
    assert_eq!(
        BinaryOp {
            left: Box::new(UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Identifier(Ident::new_unchecked("a"))),
            }),
            op: BinaryOperator::Plus,
            right: Box::new(UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Identifier(Ident::new_unchecked("b"))),
            }),
        },
        ast
    );
}

#[test]
fn parse_is_null() {
    use self::Expr::*;
    let sql = "a IS NULL";
    assert_eq!(
        IsNull(Box::new(Identifier(Ident::new_unchecked("a")))),
        verified_expr(sql)
    );
}

#[test]
fn parse_is_not_null() {
    use self::Expr::*;
    let sql = "a IS NOT NULL";
    assert_eq!(
        IsNotNull(Box::new(Identifier(Ident::new_unchecked("a")))),
        verified_expr(sql)
    );
}

#[test]
fn parse_is_distinct_from() {
    use self::Expr::*;
    let sql = "a IS DISTINCT FROM b";
    assert_eq!(
        IsDistinctFrom(
            Box::new(Identifier(Ident::new_unchecked("a"))),
            Box::new(Identifier(Ident::new_unchecked("b")))
        ),
        verified_expr(sql)
    );
}
#[test]
fn parse_is_not_distinct_from() {
    use self::Expr::*;
    let sql = "a IS NOT DISTINCT FROM b";
    assert_eq!(
        IsNotDistinctFrom(
            Box::new(Identifier(Ident::new_unchecked("a"))),
            Box::new(Identifier(Ident::new_unchecked("b")))
        ),
        verified_expr(sql)
    );
}

#[test]
fn parse_not_precedence() {
    // NOT has higher precedence than OR/AND, so the following must parse as (NOT true) OR true
    let sql = "NOT true OR true";
    let ast = run_parser_method(sql, |parser| parser.parse_expr()).unwrap();
    assert_eq!("NOT true OR true", &ast.to_string());
    assert_matches!(
        ast,
        Expr::BinaryOp {
            op: BinaryOperator::Or,
            ..
        }
    );

    // But NOT has lower precedence than comparison operators, so the following parses as NOT (a IS
    // NULL)
    let sql = "NOT a IS NULL";
    let ast = run_parser_method(sql, |parser| parser.parse_expr()).unwrap();
    assert_eq!("NOT a IS NULL", &ast.to_string());
    assert_matches!(
        ast,
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            ..
        }
    );

    // NOT has lower precedence than BETWEEN, so the following parses as NOT (1 NOT BETWEEN 1 AND 2)
    let sql = "NOT 1 NOT BETWEEN 1 AND 2";
    assert_eq!(
        verified_expr(sql),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Between {
                expr: Box::new(Expr::Value(number("1"))),
                low: Box::new(Expr::Value(number("1"))),
                high: Box::new(Expr::Value(number("2"))),
                negated: true,
            }),
        },
    );

    // NOT has lower precedence than LIKE, so the following parses as NOT ('a' NOT LIKE 'b')
    let sql = "NOT 'a' NOT LIKE 'b'";
    let ast = run_parser_method(sql, |parser| parser.parse_expr()).unwrap();
    assert_eq!("NOT 'a' NOT LIKE 'b'", &ast.to_string());
    assert_eq!(
        ast,
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Like {
                expr: Box::new(Expr::Value(Value::SingleQuotedString("a".into()))),
                negated: true,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("b".into()))),
                escape_char: None,
            }),
        },
    );

    // NOT has lower precedence than IN, so the following parses as NOT (a NOT IN 'a')
    let sql = "NOT a NOT IN ('a')";
    assert_eq!(
        verified_expr(sql),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::InList {
                expr: Box::new(Expr::Identifier("a".into())),
                list: vec![Expr::Value(Value::SingleQuotedString("a".into()))],
                negated: true,
            }),
        },
    );
}

#[test]
fn parse_like() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::Like {
                expr: Box::new(Expr::Identifier(Ident::new_unchecked("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_owned()))),
                escape_char: None,
            },
            select.selection.unwrap()
        );

        // Test with escape char
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a' ESCAPE '\\'",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::Like {
                expr: Box::new(Expr::Identifier(Ident::new_unchecked("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_owned()))),
                escape_char: Some(EscapeChar::escape('\\')),
            },
            select.selection.unwrap()
        );

        // This statement tests that LIKE and NOT LIKE have the same precedence.
        // This was previously mishandled (#81).
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a' IS NULL",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::IsNull(Box::new(Expr::Like {
                expr: Box::new(Expr::Identifier(Ident::new_unchecked("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_owned()))),
                escape_char: None,
            })),
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_ilike() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}ILIKE '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::ILike {
                expr: Box::new(Expr::Identifier(Ident::new_unchecked("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_owned()))),
                escape_char: None,
            },
            select.selection.unwrap()
        );

        // Test with escape char
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}ILIKE '%a' ESCAPE '^'",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::ILike {
                expr: Box::new(Expr::Identifier(Ident::new_unchecked("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_owned()))),
                escape_char: Some(EscapeChar::escape('^')),
            },
            select.selection.unwrap()
        );

        // This statement tests that ILIKE and NOT ILIKE have the same precedence.
        // This was previously mishandled (#81).
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}ILIKE '%a' IS NULL",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::IsNull(Box::new(Expr::ILike {
                expr: Box::new(Expr::Identifier(Ident::new_unchecked("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_owned()))),
                escape_char: None,
            })),
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_similar_to() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new_unchecked("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_owned()))),
                escape_char: None,
            },
            select.selection.unwrap()
        );

        // Test with escape char
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a' ESCAPE '\\'",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new_unchecked("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_owned()))),
                escape_char: Some(EscapeChar::escape('\\')),
            },
            select.selection.unwrap()
        );

        // This statement tests that SIMILAR TO and NOT SIMILAR TO have the same precedence.
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a' ESCAPE '\\' IS NULL",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::IsNull(Box::new(Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new_unchecked("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_owned()))),
                escape_char: Some(EscapeChar::escape('\\')),
            })),
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_in_list() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE segment {}IN ('HIGH', 'MED')",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::InList {
                expr: Box::new(Expr::Identifier(Ident::new_unchecked("segment"))),
                list: vec![
                    Expr::Value(Value::SingleQuotedString("HIGH".to_owned())),
                    Expr::Value(Value::SingleQuotedString("MED".to_owned())),
                ],
                negated,
            },
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_in_subquery() {
    let sql = "SELECT * FROM customers WHERE segment IN (SELECT segm FROM bar)";
    let select = verified_only_select(sql);
    assert_eq!(
        Expr::InSubquery {
            expr: Box::new(Expr::Identifier(Ident::new_unchecked("segment"))),
            subquery: Box::new(verified_query("SELECT segm FROM bar")),
            negated: false,
        },
        select.selection.unwrap()
    );
}

#[test]
fn parse_string_concat() {
    let sql = "SELECT a || b";

    let select = verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new_unchecked("a"))),
            op: BinaryOperator::Custom("||".to_owned()),
            right: Box::new(Expr::Identifier(Ident::new_unchecked("b"))),
        }),
        select.projection[0]
    );
}

#[test]
fn parse_bitwise_ops() {
    let bitwise_ops = &[
        ("|", BinaryOperator::Custom("|".to_owned())),
        ("&", BinaryOperator::Custom("&".to_owned())),
    ];

    for (str_op, op) in bitwise_ops {
        let select = verified_only_select(&format!("SELECT a {} b", str_op));
        assert_eq!(
            SelectItem::UnnamedExpr(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new_unchecked("a"))),
                op: op.clone(),
                right: Box::new(Expr::Identifier(Ident::new_unchecked("b"))),
            }),
            select.projection[0]
        );
    }
}

#[test]
fn parse_binary_some() {
    let select = verified_only_select("SELECT a = SOME(b)");
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new_unchecked("a"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::SomeOp(Box::new(Expr::Identifier(
                Ident::new_unchecked("b")
            )))),
        }),
        select.projection[0]
    );
}

#[test]
fn parse_binary_all() {
    let select = verified_only_select("SELECT a = ALL(b)");
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new_unchecked("a"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::AllOp(Box::new(Expr::Identifier(
                Ident::new_unchecked("b")
            )))),
        }),
        select.projection[0]
    );
}

#[test]
fn parse_logical_xor() {
    let sql = "SELECT true XOR true, false XOR false, true XOR false, false XOR true";
    let select = verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Value(Value::Boolean(true))),
            op: BinaryOperator::Xor,
            right: Box::new(Expr::Value(Value::Boolean(true))),
        }),
        select.projection[0]
    );
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Value(Value::Boolean(false))),
            op: BinaryOperator::Xor,
            right: Box::new(Expr::Value(Value::Boolean(false))),
        }),
        select.projection[1]
    );
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Value(Value::Boolean(true))),
            op: BinaryOperator::Xor,
            right: Box::new(Expr::Value(Value::Boolean(false))),
        }),
        select.projection[2]
    );
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Value(Value::Boolean(false))),
            op: BinaryOperator::Xor,
            right: Box::new(Expr::Value(Value::Boolean(true))),
        }),
        select.projection[3]
    );
}

#[test]
fn parse_between() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE age {}BETWEEN 25 AND 32",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::Between {
                expr: Box::new(Expr::Identifier(Ident::new_unchecked("age"))),
                low: Box::new(Expr::Value(number("25"))),
                high: Box::new(Expr::Value(number("32"))),
                negated,
            },
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_between_with_expr() {
    use self::BinaryOperator::*;
    let sql = "SELECT * FROM t WHERE 1 BETWEEN 1 + 2 AND 3 + 4 IS NULL";
    let select = verified_only_select(sql);
    assert_eq!(
        Expr::IsNull(Box::new(Expr::Between {
            expr: Box::new(Expr::Value(number("1"))),
            low: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(number("1"))),
                op: Plus,
                right: Box::new(Expr::Value(number("2"))),
            }),
            high: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(number("3"))),
                op: Plus,
                right: Box::new(Expr::Value(number("4"))),
            }),
            negated: false,
        })),
        select.selection.unwrap()
    );

    let sql = "SELECT * FROM t WHERE (1 = 1) AND 1 + x BETWEEN 1 AND 2";
    let select = verified_only_select(sql);
    assert_eq!(
        Expr::BinaryOp {
            left: Box::new(Expr::Nested(Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(number("1"))),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(number("1"))),
            }))),
            op: BinaryOperator::And,
            right: Box::new(Expr::Between {
                expr: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Value(number("1"))),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expr::Identifier(Ident::new_unchecked("x"))),
                }),
                low: Box::new(Expr::Value(number("1"))),
                high: Box::new(Expr::Value(number("2"))),
                negated: false,
            }),
        },
        select.selection.unwrap(),
    )
}

#[test]
fn parse_select_order_by() {
    fn chk(sql: &str) {
        let select = verified_query(sql);
        assert_eq!(
            vec![
                OrderByExpr {
                    expr: Expr::Identifier(Ident::new_unchecked("lname")),
                    asc: Some(true),
                    nulls_first: None,
                },
                OrderByExpr {
                    expr: Expr::Identifier(Ident::new_unchecked("fname")),
                    asc: Some(false),
                    nulls_first: None,
                },
                OrderByExpr {
                    expr: Expr::Identifier(Ident::new_unchecked("id")),
                    asc: None,
                    nulls_first: None,
                },
            ],
            select.order_by
        );
    }
    chk("SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY lname ASC, fname DESC, id");
    // make sure ORDER is not treated as an alias
    chk("SELECT id, fname, lname FROM customer ORDER BY lname ASC, fname DESC, id");
    chk("SELECT 1 AS lname, 2 AS fname, 3 AS id, 4 ORDER BY lname ASC, fname DESC, id");
}

#[test]
fn parse_select_order_by_limit() {
    let sql = "SELECT id, fname, lname FROM customer WHERE id < 5 \
               ORDER BY lname ASC, fname DESC LIMIT 2";
    let select = verified_query(sql);
    assert_eq!(
        vec![
            OrderByExpr {
                expr: Expr::Identifier(Ident::new_unchecked("lname")),
                asc: Some(true),
                nulls_first: None,
            },
            OrderByExpr {
                expr: Expr::Identifier(Ident::new_unchecked("fname")),
                asc: Some(false),
                nulls_first: None,
            },
        ],
        select.order_by
    );
    assert_eq!(
        Some(Expr::Value(Value::Number("2".to_owned()))),
        select.limit
    );
}

#[test]
fn parse_select_order_by_nulls_order() {
    let sql = "SELECT id, fname, lname FROM customer WHERE id < 5 \
               ORDER BY lname ASC NULLS FIRST, fname DESC NULLS LAST LIMIT 2";
    let select = verified_query(sql);
    assert_eq!(
        vec![
            OrderByExpr {
                expr: Expr::Identifier(Ident::new_unchecked("lname")),
                asc: Some(true),
                nulls_first: Some(true),
            },
            OrderByExpr {
                expr: Expr::Identifier(Ident::new_unchecked("fname")),
                asc: Some(false),
                nulls_first: Some(false),
            },
        ],
        select.order_by
    );
    assert_eq!(
        Some(Expr::Value(Value::Number("2".to_owned()))),
        select.limit
    );
}

#[test]
fn parse_select_group_by() {
    let sql = "SELECT id, fname, lname FROM customer GROUP BY lname, fname";
    let select = verified_only_select(sql);
    assert_eq!(
        vec![
            Expr::Identifier(Ident::new_unchecked("lname")),
            Expr::Identifier(Ident::new_unchecked("fname")),
        ],
        select.group_by
    );
}

#[test]
fn parse_select_group_by_grouping_sets() {
    let sql = "SELECT brand, size, sum(sales) FROM items_sold GROUP BY size, GROUPING SETS ((brand), (size), ())";
    let select = verified_only_select(sql);
    assert_eq!(
        vec![
            Expr::Identifier(Ident::new_unchecked("size")),
            Expr::GroupingSets(vec![
                vec![Expr::Identifier(Ident::new_unchecked("brand"))],
                vec![Expr::Identifier(Ident::new_unchecked("size"))],
                vec![],
            ])
        ],
        select.group_by
    );
}

#[test]
fn parse_select_group_by_rollup() {
    let sql = "SELECT brand, size, sum(sales) FROM items_sold GROUP BY size, ROLLUP (brand, size)";
    let select = verified_only_select(sql);
    assert_eq!(
        vec![
            Expr::Identifier(Ident::new_unchecked("size")),
            Expr::Rollup(vec![
                vec![Expr::Identifier(Ident::new_unchecked("brand"))],
                vec![Expr::Identifier(Ident::new_unchecked("size"))],
            ])
        ],
        select.group_by
    );
}

#[test]
fn parse_select_group_by_cube() {
    let sql = "SELECT brand, size, sum(sales) FROM items_sold GROUP BY size, CUBE (brand, size)";
    let select = verified_only_select(sql);
    assert_eq!(
        vec![
            Expr::Identifier(Ident::new_unchecked("size")),
            Expr::Cube(vec![
                vec![Expr::Identifier(Ident::new_unchecked("brand"))],
                vec![Expr::Identifier(Ident::new_unchecked("size"))],
            ])
        ],
        select.group_by
    );
}

#[test]
fn parse_select_having() {
    let sql = "SELECT foo FROM bar GROUP BY foo HAVING COUNT(*) > 1";
    let select = verified_only_select(sql);
    assert_eq!(
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Function(Function {
                scalar_as_agg: false,
                name: ObjectName(vec![Ident::new_unchecked("COUNT")]),
                arg_list: FunctionArgList::args_only(vec![FunctionArg::Unnamed(
                    FunctionArgExpr::Wildcard(None)
                )]),
                within_group: None,
                filter: None,
                over: None,
            })),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Value(number("1"))),
        }),
        select.having
    );

    let sql = "SELECT 'foo' HAVING 1 = 1";
    let select = verified_only_select(sql);
    assert!(select.having.is_some());
}

#[test]
fn parse_limit_accepts_all() {
    one_statement_parses_to(
        "SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT ALL",
        "SELECT id, fname, lname FROM customer WHERE id = 1",
    );
}

#[test]
fn parse_cast() {
    let sql = "SELECT CAST(id AS BIGINT) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            expr: Box::new(Expr::Identifier(Ident::new_unchecked("id"))),
            data_type: DataType::BigInt
        },
        expr_from_projection(only(&select.projection))
    );

    one_statement_parses_to(
        "SELECT CAST(id AS BIGINT) FROM customer",
        "SELECT CAST(id AS BIGINT) FROM customer",
    );

    verified_stmt("SELECT CAST(id AS NUMERIC) FROM customer");

    one_statement_parses_to(
        "SELECT CAST(id AS DEC) FROM customer",
        "SELECT CAST(id AS NUMERIC) FROM customer",
    );

    one_statement_parses_to(
        "SELECT CAST(id AS DECIMAL) FROM customer",
        "SELECT CAST(id AS NUMERIC) FROM customer",
    );
}

#[test]
fn parse_try_cast() {
    let sql = "SELECT TRY_CAST(id AS BIGINT) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TryCast {
            expr: Box::new(Expr::Identifier(Ident::new_unchecked("id"))),
            data_type: DataType::BigInt
        },
        expr_from_projection(only(&select.projection))
    );
    one_statement_parses_to(
        "SELECT TRY_CAST(id AS BIGINT) FROM customer",
        "SELECT TRY_CAST(id AS BIGINT) FROM customer",
    );

    verified_stmt("SELECT TRY_CAST(id AS NUMERIC) FROM customer");

    one_statement_parses_to(
        "SELECT TRY_CAST(id AS DEC) FROM customer",
        "SELECT TRY_CAST(id AS NUMERIC) FROM customer",
    );

    one_statement_parses_to(
        "SELECT TRY_CAST(id AS DECIMAL) FROM customer",
        "SELECT TRY_CAST(id AS NUMERIC) FROM customer",
    );
}

#[test]
fn parse_extract() {
    let sql = "SELECT EXTRACT(YEAR FROM d)";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Extract {
            field: "YEAR".to_owned(),
            expr: Box::new(Expr::Identifier(Ident::new_unchecked("d"))),
        },
        expr_from_projection(only(&select.projection)),
    );

    one_statement_parses_to("SELECT EXTRACT(year from d)", "SELECT EXTRACT(YEAR FROM d)");

    verified_stmt("SELECT EXTRACT(MONTH FROM d)");
    verified_stmt("SELECT EXTRACT(DAY FROM d)");
    verified_stmt("SELECT EXTRACT(HOUR FROM d)");
    verified_stmt("SELECT EXTRACT(MINUTE FROM d)");
    verified_stmt("SELECT EXTRACT(SECOND FROM d)");
}

#[test]
fn parse_create_table() {
    let sql = "CREATE TABLE uk_cities (\
               name VARCHAR NOT NULL,\
               lat DOUBLE NULL,\
               lng DOUBLE,
               constrained INT NULL CONSTRAINT pkey PRIMARY KEY NOT NULL UNIQUE CHECK (constrained > 0),
               ref INT REFERENCES othertable (a, b),\
               ref2 INT references othertable2 on delete cascade on update no action,\
               constraint fkey foreign key (lat) references othertable3 (lat) on delete restrict,\
               constraint fkey2 foreign key (lat) references othertable4(lat) on delete no action on update restrict, \
               foreign key (lat) references othertable4(lat) on update set default on delete cascade, \
               FOREIGN KEY (lng) REFERENCES othertable4 (longitude) ON UPDATE SET NULL
               )";
    let ast = one_statement_parses_to(
        sql,
        "CREATE TABLE uk_cities (\
         name CHARACTER VARYING NOT NULL, \
         lat DOUBLE NULL, \
         lng DOUBLE, \
         constrained INT NULL CONSTRAINT pkey PRIMARY KEY NOT NULL UNIQUE CHECK (constrained > 0), \
         ref INT REFERENCES othertable (a, b), \
         ref2 INT REFERENCES othertable2 ON DELETE CASCADE ON UPDATE NO ACTION, \
         CONSTRAINT fkey FOREIGN KEY (lat) REFERENCES othertable3(lat) ON DELETE RESTRICT, \
         CONSTRAINT fkey2 FOREIGN KEY (lat) REFERENCES othertable4(lat) ON DELETE NO ACTION ON UPDATE RESTRICT, \
         FOREIGN KEY (lat) REFERENCES othertable4(lat) ON DELETE CASCADE ON UPDATE SET DEFAULT, \
         FOREIGN KEY (lng) REFERENCES othertable4(longitude) ON UPDATE SET NULL)",
    );
    match ast {
        Statement::CreateTable {
            name,
            columns,
            constraints,
            with_options,
            if_not_exists: false,
            ..
        } => {
            assert_eq!("uk_cities", name.to_string());
            assert_eq!(
                columns,
                vec![
                    ColumnDef::new(
                        "name".into(),
                        DataType::Varchar,
                        None,
                        vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull
                        }],
                    ),
                    ColumnDef::new(
                        "lat".into(),
                        DataType::Double,
                        None,
                        vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Null
                        }],
                    ),
                    ColumnDef::new("lng".into(), DataType::Double, None, vec![],),
                    ColumnDef::new(
                        "constrained".into(),
                        DataType::Int,
                        None,
                        vec![
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Null
                            },
                            ColumnOptionDef {
                                name: Some("pkey".into()),
                                option: ColumnOption::Unique { is_primary: true }
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Unique { is_primary: false },
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Check(verified_expr("constrained > 0")),
                            }
                        ],
                    ),
                    ColumnDef::new(
                        Ident::new_unchecked("ref"),
                        DataType::Int,
                        None,
                        vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::ForeignKey {
                                foreign_table: ObjectName(vec!["othertable".into()]),
                                referred_columns: vec!["a".into(), "b".into(),],
                                on_delete: None,
                                on_update: None,
                            }
                        }],
                    ),
                    ColumnDef::new(
                        "ref2".into(),
                        DataType::Int,
                        None,
                        vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::ForeignKey {
                                foreign_table: ObjectName(vec!["othertable2".into()]),
                                referred_columns: vec![],
                                on_delete: Some(ReferentialAction::Cascade),
                                on_update: Some(ReferentialAction::NoAction),
                            }
                        },],
                    ),
                ]
            );
            assert_eq!(
                constraints,
                vec![
                    TableConstraint::ForeignKey {
                        name: Some("fkey".into()),
                        columns: vec!["lat".into()],
                        foreign_table: ObjectName(vec!["othertable3".into()]),
                        referred_columns: vec!["lat".into()],
                        on_delete: Some(ReferentialAction::Restrict),
                        on_update: None
                    },
                    TableConstraint::ForeignKey {
                        name: Some("fkey2".into()),
                        columns: vec!["lat".into()],
                        foreign_table: ObjectName(vec!["othertable4".into()]),
                        referred_columns: vec!["lat".into()],
                        on_delete: Some(ReferentialAction::NoAction),
                        on_update: Some(ReferentialAction::Restrict)
                    },
                    TableConstraint::ForeignKey {
                        name: None,
                        columns: vec!["lat".into()],
                        foreign_table: ObjectName(vec!["othertable4".into()]),
                        referred_columns: vec!["lat".into()],
                        on_delete: Some(ReferentialAction::Cascade),
                        on_update: Some(ReferentialAction::SetDefault)
                    },
                    TableConstraint::ForeignKey {
                        name: None,
                        columns: vec!["lng".into()],
                        foreign_table: ObjectName(vec!["othertable4".into()]),
                        referred_columns: vec!["longitude".into()],
                        on_delete: None,
                        on_update: Some(ReferentialAction::SetNull)
                    },
                ]
            );
            assert_eq!(with_options, vec![]);
        }
        _ => unreachable!(),
    }

    let res = parse_sql_statements("CREATE TABLE t (a int NOT NULL GARBAGE)");
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("expected \',\' or \')\' after column definition, found: GARBAGE")
    );

    let res = parse_sql_statements("CREATE TABLE t (a int NOT NULL CONSTRAINT foo)");
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("expected constraint details after CONSTRAINT <name>")
    );
}

#[test]
fn parse_create_table_with_multiple_on_delete_in_constraint_fails() {
    parse_sql_statements(
        "\
        create table X (\
            y_id int, \
            foreign key (y_id) references Y (id) on delete cascade on update cascade on delete no action\
        )",
    )
        .expect_err("should have failed");
}

#[test]
fn parse_create_table_with_multiple_on_delete_fails() {
    parse_sql_statements(
        "\
        create table X (\
            y_id int references Y (id) \
            on delete cascade on update cascade on delete no action\
        )",
    )
    .expect_err("should have failed");
}

#[test]
fn parse_create_schema() {
    let sql = "CREATE SCHEMA X";

    match verified_stmt(sql) {
        Statement::CreateSchema { schema_name, .. } => {
            assert_eq!(schema_name.to_string(), "X".to_owned())
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_schema() {
    let sql = "DROP SCHEMA X";

    match verified_stmt(sql) {
        Statement::Drop(stmt) => assert_eq!(stmt.object_type, ObjectType::Schema),
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_as() {
    let sql = "CREATE TABLE t AS SELECT * FROM a";

    match verified_stmt(sql) {
        Statement::CreateTable { name, query, .. } => {
            assert_eq!(name.to_string(), "t".to_owned());
            assert_eq!(query, Some(Box::new(verified_query("SELECT * FROM a"))));
        }
        _ => unreachable!(),
    }

    // BigQuery allows specifying table schema in CTAS
    // ANSI SQL and PostgreSQL let you only specify the list of columns
    // (without data types) in a CTAS, but we have yet to support that.
    let sql = "CREATE TABLE t (a INT, b INT) AS SELECT 1 AS b, 2 AS a";
    match verified_stmt(sql) {
        Statement::CreateTable { columns, query, .. } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].to_string(), "a INT".to_owned());
            assert_eq!(columns[1].to_string(), "b INT".to_owned());
            assert_eq!(
                query,
                Some(Box::new(verified_query("SELECT 1 AS b, 2 AS a")))
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_with_on_delete_on_update_2in_any_order() -> Result<(), ParserError> {
    let sql = |options: &str| -> String {
        format!("create table X (y_id int references Y (id) {})", options)
    };

    parse_sql_statements(&sql("on update cascade on delete no action"))?;
    parse_sql_statements(&sql("on delete cascade on update cascade"))?;
    parse_sql_statements(&sql("on update no action"))?;
    parse_sql_statements(&sql("on delete restrict"))?;

    Ok(())
}

#[test]
fn parse_create_table_with_options() {
    let sql = "CREATE TABLE t (c INT) WITH (foo = 'bar', a = 123)";
    match verified_stmt(sql) {
        Statement::CreateTable { with_options, .. } => {
            assert_eq!(
                vec![
                    SqlOption {
                        name: vec!["foo".into()].into(),
                        value: Value::SingleQuotedString("bar".into()).into(),
                    },
                    SqlOption {
                        name: vec!["a".into()].into(),
                        value: number("123").into(),
                    },
                ],
                with_options
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_trailing_comma() {
    let sql = "CREATE TABLE foo (bar int,)";
    one_statement_parses_to(sql, "CREATE TABLE foo (bar INT)");
}

#[test]
fn parse_alter_table() {
    let add_column = "ALTER TABLE tab ADD COLUMN foo TEXT;";
    match one_statement_parses_to(add_column, "ALTER TABLE tab ADD COLUMN foo TEXT") {
        Statement::AlterTable {
            name,
            operation: AlterTableOperation::AddColumn { column_def },
        } => {
            assert_eq!("tab", name.to_string());
            assert_eq!("foo", column_def.name.to_string());
            assert_eq!("TEXT", column_def.data_type.unwrap().to_string());
        }
        _ => unreachable!(),
    };

    let rename_table = "ALTER TABLE tab RENAME TO new_tab";
    match verified_stmt(rename_table) {
        Statement::AlterTable {
            name,
            operation: AlterTableOperation::RenameTable { table_name },
        } => {
            assert_eq!("tab", name.to_string());
            assert_eq!("new_tab", table_name.to_string())
        }
        _ => unreachable!(),
    };

    let rename_column = "ALTER TABLE tab RENAME COLUMN foo TO new_foo";
    match verified_stmt(rename_column) {
        Statement::AlterTable {
            name,
            operation:
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                },
        } => {
            assert_eq!("tab", name.to_string());
            assert_eq!(old_column_name.to_string(), "foo");
            assert_eq!(new_column_name.to_string(), "new_foo");
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_table_constraints() {
    check_one("CONSTRAINT address_pkey PRIMARY KEY (address_id)");
    check_one("CONSTRAINT uk_task UNIQUE (report_date, task_id)");
    check_one(
        "CONSTRAINT customer_address_id_fkey FOREIGN KEY (address_id) \
         REFERENCES public.address(address_id)",
    );
    check_one("CONSTRAINT ck CHECK (rtrim(ltrim(REF_CODE)) <> '')");

    check_one("PRIMARY KEY (foo, bar)");
    check_one("UNIQUE (id)");
    check_one("FOREIGN KEY (foo, bar) REFERENCES AnotherTable(foo, bar)");
    check_one("CHECK ((end_date > start_date) OR (end_date IS NULL))");

    fn check_one(constraint_text: &str) {
        match verified_stmt(&format!("ALTER TABLE tab ADD {}", constraint_text)) {
            Statement::AlterTable {
                name,
                operation: AlterTableOperation::AddConstraint(constraint),
            } => {
                assert_eq!("tab", name.to_string());
                assert_eq!(constraint_text, constraint.to_string());
            }
            _ => unreachable!(),
        }
        verified_stmt(&format!("CREATE TABLE foo (id INT, {})", constraint_text));
    }
}

#[test]
fn parse_alter_table_drop_column() {
    check_one("DROP COLUMN IF EXISTS is_active CASCADE");
    one_statement_parses_to(
        "ALTER TABLE tab DROP IF EXISTS is_active CASCADE",
        "ALTER TABLE tab DROP COLUMN IF EXISTS is_active CASCADE",
    );
    one_statement_parses_to(
        "ALTER TABLE tab DROP is_active CASCADE",
        "ALTER TABLE tab DROP COLUMN is_active CASCADE",
    );

    fn check_one(constraint_text: &str) {
        match verified_stmt(&format!("ALTER TABLE tab {}", constraint_text)) {
            Statement::AlterTable {
                name,
                operation:
                    AlterTableOperation::DropColumn {
                        column_name,
                        if_exists,
                        cascade,
                    },
            } => {
                assert_eq!("tab", name.to_string());
                assert_eq!("is_active", column_name.to_string());
                assert!(if_exists);
                assert!(cascade);
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn parse_alter_table_alter_watermark() {
    match verified_stmt("ALTER TABLE tab ALTER WATERMARK FOR ts AS ts - INTERVAL '1' MINUTE") {
        Statement::AlterTable {
            name,
            operation:
                AlterTableOperation::AlterWatermark {
                    column_name,
                    expr: _,
                    with_ttl,
                },
        } => {
            assert_eq!("tab", name.to_string());
            assert_eq!("ts", column_name.to_string());
            assert!(!with_ttl);
        }
        _ => unreachable!(),
    }

    match verified_stmt("ALTER TABLE tab ALTER WATERMARK FOR ts AS ts - 10 WITH TTL") {
        Statement::AlterTable {
            operation:
                AlterTableOperation::AlterWatermark {
                    column_name,
                    with_ttl,
                    ..
                },
            ..
        } => {
            assert_eq!("ts", column_name.to_string());
            assert!(with_ttl);
        }
        _ => unreachable!(),
    }

    // `WATERMARK` is non-reserved; the new branch must not steal `ALTER <col>`
    // when `<col>` happens to be named `watermark` (only commit on `WATERMARK FOR`).
    match one_statement_parses_to(
        "ALTER TABLE tab ALTER watermark TYPE BIGINT",
        "ALTER TABLE tab ALTER COLUMN watermark SET DATA TYPE BIGINT",
    ) {
        Statement::AlterTable {
            operation: AlterTableOperation::AlterColumn { column_name, .. },
            ..
        } => {
            assert_eq!("watermark", column_name.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_table_alter_column() {
    let alter_stmt = "ALTER TABLE tab";
    match verified_stmt(&format!(
        "{} ALTER COLUMN is_active SET NOT NULL",
        alter_stmt
    )) {
        Statement::AlterTable {
            name,
            operation: AlterTableOperation::AlterColumn { column_name, op },
        } => {
            assert_eq!("tab", name.to_string());
            assert_eq!("is_active", column_name.to_string());
            assert_eq!(op, AlterColumnOperation::SetNotNull {});
        }
        _ => unreachable!(),
    }

    one_statement_parses_to(
        "ALTER TABLE tab ALTER is_active DROP NOT NULL",
        "ALTER TABLE tab ALTER COLUMN is_active DROP NOT NULL",
    );

    match verified_stmt(&format!(
        "{} ALTER COLUMN is_active SET DEFAULT false",
        alter_stmt
    )) {
        Statement::AlterTable {
            name,
            operation: AlterTableOperation::AlterColumn { column_name, op },
        } => {
            assert_eq!("tab", name.to_string());
            assert_eq!("is_active", column_name.to_string());
            assert_eq!(
                op,
                AlterColumnOperation::SetDefault {
                    value: Expr::Value(Value::Boolean(false))
                }
            );
        }
        _ => unreachable!(),
    }

    match verified_stmt(&format!(
        "{} ALTER COLUMN is_active DROP DEFAULT",
        alter_stmt
    )) {
        Statement::AlterTable {
            name,
            operation: AlterTableOperation::AlterColumn { column_name, op },
        } => {
            assert_eq!("tab", name.to_string());
            assert_eq!("is_active", column_name.to_string());
            assert_eq!(op, AlterColumnOperation::DropDefault {});
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_table_alter_column_type() {
    [
        "ALTER TABLE tab ALTER COLUMN is_active SET DATA TYPE TEXT",
        "ALTER TABLE tab ALTER COLUMN is_active TYPE TEXT",
    ]
    .iter()
    .for_each(|query| {
        match one_statement_parses_to(
            query,
            "ALTER TABLE tab ALTER COLUMN is_active SET DATA TYPE TEXT",
        ) {
            Statement::AlterTable {
                name,
                operation: AlterTableOperation::AlterColumn { column_name, op },
            } => {
                assert_eq!("tab", name.to_string());
                assert_eq!("is_active", column_name.to_string());
                assert_eq!(
                    op,
                    AlterColumnOperation::SetDataType {
                        data_type: DataType::Text,
                        using: None,
                    }
                );
            }
            _ => unreachable!(),
        }
    });

    match verified_stmt("ALTER TABLE tab ALTER COLUMN is_active SET DATA TYPE TEXT USING 'text'") {
        Statement::AlterTable {
            name,
            operation: AlterTableOperation::AlterColumn { column_name, op },
        } => {
            assert_eq!("tab", name.to_string());
            assert_eq!("is_active", column_name.to_string());
            assert_eq!(
                op,
                AlterColumnOperation::SetDataType {
                    data_type: DataType::Text,
                    using: Some(Expr::Value(Value::SingleQuotedString("text".to_owned()))),
                }
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_bad_constraint() {
    let res = parse_sql_statements("ALTER TABLE tab ADD");
    assert!(format!("{}", res.unwrap_err()).contains("expected identifier, found: EOF"));

    let res = parse_sql_statements("CREATE TABLE tab (foo int,");

    assert!(
        format!("{}", res.unwrap_err())
            .contains("expected column name or constraint definition, found: EOF")
    );
}

fn run_explain_analyze(query: &str, expected_analyze: bool, expected_options: ExplainOptions) {
    match one_statement_parses_to(query, "") {
        Statement::Explain {
            analyze,
            statement,
            options,
        } => {
            assert_eq!(analyze, expected_analyze);
            assert_eq!(options, expected_options);
            assert_eq!("SELECT sqrt(id) FROM foo", statement.to_string());
        }
        _ => panic!("Unexpected Statement, must be Explain"),
    }
}

#[test]
fn parse_explain_analyze_with_simple_select() {
    run_explain_analyze(
        "EXPLAIN SELECT sqrt(id) FROM foo",
        false,
        ExplainOptions::default(),
    );
    run_explain_analyze(
        "EXPLAIN (VERBOSE) SELECT sqrt(id) FROM foo",
        false,
        ExplainOptions {
            verbose: true,
            ..Default::default()
        },
    );
    run_explain_analyze(
        "EXPLAIN ANALYZE SELECT sqrt(id) FROM foo",
        true,
        ExplainOptions::default(),
    );
    run_explain_analyze(
        "EXPLAIN (TRACE) SELECT sqrt(id) FROM foo",
        false,
        ExplainOptions {
            trace: true,
            ..Default::default()
        },
    );
    run_explain_analyze(
        "EXPLAIN ANALYZE (VERBOSE) SELECT sqrt(id) FROM foo",
        true,
        ExplainOptions {
            verbose: true,
            ..Default::default()
        },
    );

    run_explain_analyze(
        "EXPLAIN (VERBOSE  , TRACE) SELECT sqrt(id) FROM foo",
        false,
        ExplainOptions {
            verbose: true,
            trace: true,
            ..Default::default()
        },
    );
    run_explain_analyze(
        "EXPLAIN (DISTSQL, TRACE ,VERBOSE) SELECT sqrt(id) FROM foo",
        false,
        ExplainOptions {
            trace: true,
            verbose: true,
            explain_type: ExplainType::DistSql,
            explain_format: ExplainFormat::Text,
            ..Default::default()
        },
    );
    run_explain_analyze(
        "EXPLAIN (DISTSQL, TRACE false ,VERBOSE true) SELECT sqrt(id) FROM foo",
        false,
        ExplainOptions {
            trace: false,
            verbose: true,
            explain_type: ExplainType::DistSql,
            explain_format: ExplainFormat::Text,
            ..Default::default()
        },
    );
    run_explain_analyze(
        "EXPLAIN (TYPE DISTSQL, TRACE false ,VERBOSE true) SELECT sqrt(id) FROM foo",
        false,
        ExplainOptions {
            trace: false,
            verbose: true,
            explain_type: ExplainType::DistSql,
            explain_format: ExplainFormat::Text,
            ..Default::default()
        },
    );
    run_explain_analyze(
        "EXPLAIN (LOGICAL, FORMAT JSON) SELECT sqrt(id) FROM foo",
        false,
        ExplainOptions {
            trace: false,
            verbose: false,
            explain_type: ExplainType::Logical,
            explain_format: ExplainFormat::Json,
            ..Default::default()
        },
    );
}

#[test]
fn parse_explain_with_invalid_options() {
    let res = parse_sql_statements("EXPLAIN (V) SELECT sqrt(id) FROM foo");
    assert!(res.is_err());

    let res = parse_sql_statements("EXPLAIN (VERBOSE TRACE) SELECT sqrt(id) FROM foo");
    assert!(format!("{}", res.unwrap_err()).contains("expected ), found: TRACE"));

    let res = parse_sql_statements("EXPLAIN () SELECT sqrt(id) FROM foo");
    assert!(res.is_err());

    let res = parse_sql_statements("EXPLAIN (VERBOSE, ) SELECT sqrt(id) FROM foo");

    let expected = "sql parser error: expected identifier, found: )";
    let actual = res.unwrap_err().to_string();
    assert!(
        actual.contains(expected),
        "expected: {:?}\nactual: {:?}",
        expected,
        actual
    );
}

#[test]
fn parse_named_argument_function() {
    let sql = "SELECT FUN(a => '1', b => '2') FROM foo";
    let select = verified_only_select(sql);

    assert_eq!(
        &Expr::Function(Function {
            scalar_as_agg: false,
            name: ObjectName(vec![Ident::new_unchecked("FUN")]),
            arg_list: FunctionArgList::args_only(vec![
                FunctionArg::Named {
                    name: Ident::new_unchecked("a"),
                    arg: FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(
                        "1".to_owned()
                    ))),
                },
                FunctionArg::Named {
                    name: Ident::new_unchecked("b"),
                    arg: FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(
                        "2".to_owned()
                    ))),
                },
            ]),
            within_group: None,
            filter: None,
            over: None,
        }),
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_window_functions() {
    let sql = "\
    SELECT \
        row_number() OVER (ORDER BY dt DESC), \
        sum(foo) OVER (\
            PARTITION BY a, b \
            ORDER BY c, d \
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        ), \
        avg(bar) OVER (\
            ORDER BY a \
            RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING\
        ), \
        max(baz) OVER (\
            ORDER BY a \
            ROWS UNBOUNDED PRECEDING\
        ), \
        sum(qux) OVER (\
            ORDER BY a \
            GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING\
        ), \
        AGGREGATE:my_func(abc) OVER (), \
        rank(foo IGNORE NULLS) FILTER (WHERE bar > 0) OVER (ORDER BY a), \
        min(foo) OVER w1, \
        min(foo) OVER w2 \
    FROM foo \
    WINDOW w1 AS (PARTITION BY a ORDER BY b), w2 AS (PARTITION BY c ORDER BY d)\
    ";
    let select = verified_only_select(sql);
    assert_eq!(9, select.projection.len());
    assert_eq!(
        &Expr::Function(Function {
            scalar_as_agg: false,
            name: ObjectName(vec![Ident::new_unchecked("row_number")]),
            arg_list: FunctionArgList::empty(),
            within_group: None,
            filter: None,
            over: Some(Window::Spec(WindowSpec {
                partition_by: vec![],
                order_by: vec![OrderByExpr {
                    expr: Expr::Identifier(Ident::new_unchecked("dt")),
                    asc: Some(false),
                    nulls_first: None,
                }],
                window_frame: None,
            })),
        }),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Function(Function {
            scalar_as_agg: true,
            name: ObjectName(vec![Ident::new_unchecked("my_func")]),
            arg_list: FunctionArgList {
                distinct: false,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                    Expr::Identifier(Ident::new_unchecked("abc"))
                ))],
                variadic: false,
                order_by: vec![],
                ignore_nulls: false,
            },
            within_group: None,
            filter: None,
            over: Some(Window::Spec(WindowSpec {
                partition_by: vec![],
                order_by: vec![],
                window_frame: None,
            })),
        }),
        expr_from_projection(&select.projection[5])
    );
    assert_eq!(
        &Expr::Function(Function {
            scalar_as_agg: false,
            name: ObjectName(vec![Ident::new_unchecked("rank")]),
            arg_list: FunctionArgList {
                distinct: false,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                    Expr::Identifier(Ident::new_unchecked("foo"))
                ))],
                variadic: false,
                order_by: vec![],
                ignore_nulls: true,
            },
            within_group: None,
            filter: Some(Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new_unchecked("bar"))),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Value(Value::Number("0".to_owned()))),
            })),
            over: Some(Window::Spec(WindowSpec {
                partition_by: vec![],
                order_by: vec![OrderByExpr {
                    expr: Expr::Identifier(Ident::new_unchecked("a")),
                    asc: None,
                    nulls_first: None,
                }],
                window_frame: None,
            })),
        }),
        expr_from_projection(&select.projection[6])
    );
    assert_eq!(
        &Expr::Function(Function {
            scalar_as_agg: false,
            name: ObjectName(vec![Ident::new_unchecked("min")]),
            arg_list: FunctionArgList::args_only(vec![FunctionArg::Unnamed(
                FunctionArgExpr::Expr(Expr::Identifier(Ident::new_unchecked("foo")))
            )]),
            within_group: None,
            filter: None,
            over: Some(Window::Name(Ident::new_unchecked("w1"))),
        }),
        expr_from_projection(&select.projection[7])
    );
}

#[test]
fn parse_aggregate_with_group_by() {
    let sql = "SELECT a, COUNT(1), MIN(b), MAX(b) FROM foo GROUP BY a";
    let _ast = verified_only_select(sql);
    // TODO: assertions
}

#[test]
fn parse_aggregate_with_order_by() {
    let sql = "SELECT STRING_AGG(a, b ORDER BY b ASC, a DESC) FROM foo";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Function(Function {
            scalar_as_agg: false,
            name: ObjectName(vec![Ident::new_unchecked("STRING_AGG")]),
            arg_list: FunctionArgList::for_agg(
                false,
                vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(
                        Ident::new_unchecked("a")
                    ))),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(
                        Ident::new_unchecked("b")
                    ))),
                ],
                vec![
                    OrderByExpr {
                        expr: Expr::Identifier(Ident::new_unchecked("b")),
                        asc: Some(true),
                        nulls_first: None,
                    },
                    OrderByExpr {
                        expr: Expr::Identifier(Ident::new_unchecked("a")),
                        asc: Some(false),
                        nulls_first: None,
                    }
                ]
            ),
            within_group: None,
            filter: None,
            over: None,
        }),
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_aggregate_with_filter() {
    let sql = "SELECT sum(a) FILTER (WHERE (a > 0) AND (a IS NOT NULL)) FROM foo";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Function(Function {
            scalar_as_agg: false,
            name: ObjectName(vec![Ident::new_unchecked("sum")]),
            arg_list: FunctionArgList::args_only(vec![FunctionArg::Unnamed(
                FunctionArgExpr::Expr(Expr::Identifier(Ident::new_unchecked("a")))
            )]),
            within_group: None,
            filter: Some(Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Nested(Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new_unchecked("a"))),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Value(Value::Number("0".to_owned())))
                }))),
                op: BinaryOperator::And,
                right: Box::new(Expr::Nested(Box::new(Expr::IsNotNull(Box::new(
                    Expr::Identifier(Ident::new_unchecked("a"))
                )))))
            })),
            over: None,
        }),
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_aggregate_with_within_group() {
    let sql = "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY a DESC NULLS FIRST) FILTER (WHERE b > 0) FROM foo";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Function(Function {
            scalar_as_agg: false,
            name: ObjectName(vec![Ident::new_unchecked("percentile_cont")]),
            arg_list: FunctionArgList::args_only(vec![FunctionArg::Unnamed(
                FunctionArgExpr::Expr(Expr::Value(Value::Number("0.5".to_owned())))
            )]),
            within_group: Some(Box::new(OrderByExpr {
                expr: Expr::Identifier(Ident::new_unchecked("a")),
                asc: Some(false),
                nulls_first: Some(true),
            },)),
            filter: Some(Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new_unchecked("b"))),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Value(Value::Number("0".to_owned())))
            })),
            over: None,
        }),
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_literal_decimal() {
    // These numbers were explicitly chosen to not roundtrip if represented as
    // f64s (i.e., as 64-bit binary floating point numbers).
    let sql = "SELECT 0.300000000000000004, 9007199254740993.0";
    let select = verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Value(number("0.300000000000000004")),
        expr_from_projection(&select.projection[0]),
    );
    assert_eq!(
        &Expr::Value(number("9007199254740993.0")),
        expr_from_projection(&select.projection[1]),
    )
}

#[test]
fn parse_literal_string() {
    let sql = r"SELECT 'one', N'national string', X'deadBEEF', E'c style escape string \x3f'";
    let select = verified_only_select(sql);
    assert_eq!(4, select.projection.len());
    assert_eq!(
        &Expr::Value(Value::SingleQuotedString("one".to_owned())),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Value(Value::NationalStringLiteral("national string".to_owned())),
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Value(Value::HexStringLiteral("deadBEEF".to_owned())),
        expr_from_projection(&select.projection[2])
    );
    assert_eq!(
        &Expr::Value(Value::CstyleEscapedString(CstyleEscapedString {
            value: "c style escape string \x3f".to_owned(),
            raw: r"c style escape string \x3f".to_owned(),
        })),
        expr_from_projection(&select.projection[3])
    );

    one_statement_parses_to("SELECT x'deadBEEF'", "SELECT X'deadBEEF'");
}

#[test]
fn parse_literal_date() {
    let sql = "SELECT DATE '1999-01-01'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString {
            data_type: DataType::Date,
            value: "1999-01-01".into()
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_literal_string_with_dollar_quoted_string() {
    let stmts = parse_sql_statements("SELECT DATE $$1999-01-01$$").unwrap();

    let projection = match &stmts[0] {
        Statement::Query(query) => match &query.body {
            SetExpr::Select(select) => &select.projection,
            _ => unreachable!(),
        },
        _ => unreachable!(),
    };

    assert_eq!(
        &Expr::TypedString {
            data_type: DataType::Date,
            value: "1999-01-01".into()
        },
        expr_from_projection(only(projection)),
    );
}

#[test]
fn parse_escaped_double_quote_in_delimited_identifier() {
    {
        let select = verified_only_select(r#"SELECT 'STRI"NG' AS "'STRI""NG""#);

        match only(&select.projection) {
            SelectItem::ExprWithAlias { expr, alias } => {
                assert_eq!(
                    &Expr::Value(Value::SingleQuotedString("STRI\"NG".to_owned())),
                    expr
                );
                assert_eq!(&Ident::with_quote_unchecked('"', "'STRI\"NG"), alias);
            }
            _ => panic!("expected ExprWithAlias"),
        }
    }

    {
        let select = verified_only_select(r#"SELECT "'STRI""NG""#);
        assert_eq!(
            &Expr::Identifier(Ident::with_quote_unchecked('"', "'STRI\"NG")),
            expr_from_projection(only(&select.projection)),
        );
    }
}

#[test]
fn parse_literal_time() {
    let sql = "SELECT TIME '01:23:34'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString {
            data_type: DataType::Time(false),
            value: "01:23:34".into()
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_literal_timestamp() {
    let sql = "SELECT TIMESTAMP '1999-01-01 01:23:34'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString {
            data_type: DataType::Timestamp(false),
            value: "1999-01-01 01:23:34".into()
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_literal_interval() {
    let sql = "SELECT INTERVAL '1-1' YEAR TO MONTH";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Value(Value::Interval {
            value: "1-1".into(),
            leading_field: Some(DateTimeField::Year),
            leading_precision: None,
            last_field: Some(DateTimeField::Month),
            fractional_seconds_precision: None,
        }),
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT INTERVAL '01:01.01' MINUTE (5) TO SECOND (5)";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Value(Value::Interval {
            value: "01:01.01".into(),
            leading_field: Some(DateTimeField::Minute),
            leading_precision: Some(5),
            last_field: Some(DateTimeField::Second),
            fractional_seconds_precision: Some(5),
        }),
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT INTERVAL '1' SECOND (5, 4)";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Value(Value::Interval {
            value: "1".into(),
            leading_field: Some(DateTimeField::Second),
            leading_precision: Some(5),
            last_field: None,
            fractional_seconds_precision: Some(4),
        }),
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT INTERVAL '10' HOUR";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Value(Value::Interval {
            value: "10".into(),
            leading_field: Some(DateTimeField::Hour),
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        }),
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT INTERVAL '10' HOUR (1)";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Value(Value::Interval {
            value: "10".into(),
            leading_field: Some(DateTimeField::Hour),
            leading_precision: Some(1),
            last_field: None,
            fractional_seconds_precision: None,
        }),
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT INTERVAL '1 DAY'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Value(Value::Interval {
            value: "1 DAY".into(),
            leading_field: None,
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        }),
        expr_from_projection(only(&select.projection)),
    );

    let result = parse_sql_statements("SELECT INTERVAL '1' SECOND TO SECOND");
    assert!(
        format!("{}", result.unwrap_err()).contains("expected end of statement, found: SECOND")
    );

    let result = parse_sql_statements("SELECT INTERVAL '10' HOUR (1) TO HOUR (2)");
    assert!(format!("{}", result.unwrap_err()).contains("expected end of statement, found: ("));

    verified_only_select("SELECT INTERVAL '1' YEAR");
    verified_only_select("SELECT INTERVAL '1' MONTH");
    verified_only_select("SELECT INTERVAL '1' DAY");
    verified_only_select("SELECT INTERVAL '1' HOUR");
    verified_only_select("SELECT INTERVAL '1' MINUTE");
    verified_only_select("SELECT INTERVAL '1' SECOND");
    verified_only_select("SELECT INTERVAL '1' YEAR TO MONTH");
    verified_only_select("SELECT INTERVAL '1' DAY TO HOUR");
    verified_only_select("SELECT INTERVAL '1' DAY TO MINUTE");
    verified_only_select("SELECT INTERVAL '1' DAY TO SECOND");
    verified_only_select("SELECT INTERVAL '1' HOUR TO MINUTE");
    verified_only_select("SELECT INTERVAL '1' HOUR TO SECOND");
    verified_only_select("SELECT INTERVAL '1' MINUTE TO SECOND");
    verified_only_select("SELECT INTERVAL '1 YEAR'");
    verified_only_select("SELECT INTERVAL '1 YEAR' AS one_year");
    one_statement_parses_to(
        "SELECT INTERVAL '1 YEAR' one_year",
        "SELECT INTERVAL '1 YEAR' AS one_year",
    );
}

#[test]
fn parse_simple_math_expr_plus() {
    let sql = "SELECT a + b, 2 + a, 2.5 + a, a_f + b_f, 2 + a_f, 2.5 + a_f FROM c";
    verified_only_select(sql);
}

#[test]
fn parse_simple_math_expr_minus() {
    let sql = "SELECT a - b, 2 - a, 2.5 - a, a_f - b_f, 2 - a_f, 2.5 - a_f FROM c";
    verified_only_select(sql);
}

#[test]
fn parse_delimited_identifiers() {
    // check that quoted identifiers in any position remain quoted after serialization
    let select = verified_only_select(
        r#"SELECT "alias"."bar baz", "myfun"(), "simple id" AS "column alias" FROM "a table" AS "alias""#,
    );
    // check FROM
    match only(select.from).relation {
        TableFactor::Table { name, alias, as_of } => {
            assert_eq!(vec![Ident::with_quote_unchecked('"', "a table")], name.0);
            assert_eq!(
                Ident::with_quote_unchecked('"', "alias"),
                alias.unwrap().name
            );
            assert!(as_of.is_none());
        }
        _ => panic!("Expecting TableFactor::Table"),
    }
    // check SELECT
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::CompoundIdentifier(vec![
            Ident::with_quote_unchecked('"', "alias"),
            Ident::with_quote_unchecked('"', "bar baz")
        ]),
        expr_from_projection(&select.projection[0]),
    );
    assert_eq!(
        &Expr::Function(Function {
            scalar_as_agg: false,
            name: ObjectName(vec![Ident::with_quote_unchecked('"', "myfun")]),
            arg_list: FunctionArgList::empty(),
            within_group: None,
            filter: None,
            over: None,
        }),
        expr_from_projection(&select.projection[1]),
    );
    match &select.projection[2] {
        SelectItem::ExprWithAlias { expr, alias } => {
            assert_eq!(
                &Expr::Identifier(Ident::with_quote_unchecked('"', "simple id")),
                expr
            );
            assert_eq!(&Ident::with_quote_unchecked('"', "column alias"), alias);
        }
        _ => panic!("expected ExprWithAlias"),
    }

    verified_stmt(r#"CREATE TABLE "foo" ("bar" "int")"#);
    verified_stmt(r#"ALTER TABLE foo ADD CONSTRAINT "bar" PRIMARY KEY (baz)"#);
    // TODO verified_stmt(r#"UPDATE foo SET "bar" = 5"#);
}

#[test]
fn parse_parens() {
    use self::BinaryOperator::*;
    use self::Expr::*;
    let sql = "(a + b) - (c + d)";
    assert_eq!(
        BinaryOp {
            left: Box::new(Nested(Box::new(BinaryOp {
                left: Box::new(Identifier(Ident::new_unchecked("a"))),
                op: Plus,
                right: Box::new(Identifier(Ident::new_unchecked("b")))
            }))),
            op: Minus,
            right: Box::new(Nested(Box::new(BinaryOp {
                left: Box::new(Identifier(Ident::new_unchecked("c"))),
                op: Plus,
                right: Box::new(Identifier(Ident::new_unchecked("d")))
            })))
        },
        verified_expr(sql)
    );
}

#[test]
fn parse_searched_case_expr() {
    let sql = "SELECT CASE WHEN bar IS NULL THEN 'null' WHEN bar = 0 THEN '=0' WHEN bar >= 0 THEN '>=0' ELSE '<0' END FROM foo";
    use self::BinaryOperator::*;
    use self::Expr::{BinaryOp, Case, Identifier, IsNull};
    let select = verified_only_select(sql);
    assert_eq!(
        &Case {
            operand: None,
            conditions: vec![
                IsNull(Box::new(Identifier(Ident::new_unchecked("bar")))),
                BinaryOp {
                    left: Box::new(Identifier(Ident::new_unchecked("bar"))),
                    op: Eq,
                    right: Box::new(Expr::Value(number("0")))
                },
                BinaryOp {
                    left: Box::new(Identifier(Ident::new_unchecked("bar"))),
                    op: GtEq,
                    right: Box::new(Expr::Value(number("0")))
                }
            ],
            results: vec![
                Expr::Value(Value::SingleQuotedString("null".to_owned())),
                Expr::Value(Value::SingleQuotedString("=0".to_owned())),
                Expr::Value(Value::SingleQuotedString(">=0".to_owned()))
            ],
            else_result: Some(Box::new(Expr::Value(Value::SingleQuotedString(
                "<0".to_owned()
            ))))
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_simple_case_expr() {
    // ANSI calls a CASE expression with an operand "<simple case>"
    let sql = "SELECT CASE foo WHEN 1 THEN 'Y' ELSE 'N' END";
    let select = verified_only_select(sql);
    use self::Expr::{Case, Identifier};
    assert_eq!(
        &Case {
            operand: Some(Box::new(Identifier(Ident::new_unchecked("foo")))),
            conditions: vec![Expr::Value(number("1"))],
            results: vec![Expr::Value(Value::SingleQuotedString("Y".to_owned())),],
            else_result: Some(Box::new(Expr::Value(Value::SingleQuotedString(
                "N".to_owned()
            ))))
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_implicit_join() {
    let sql = "SELECT * FROM t1, t2";
    let select = verified_only_select(sql);
    assert_eq!(
        vec![
            TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec!["t1".into()]),
                    alias: None,
                    as_of: None,
                },
                joins: vec![],
            },
            TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec!["t2".into()]),
                    alias: None,
                    as_of: None,
                },
                joins: vec![],
            }
        ],
        select.from,
    );

    let sql = "SELECT * FROM t1a NATURAL JOIN t1b, t2a NATURAL JOIN t2b";
    let select = verified_only_select(sql);
    assert_eq!(
        vec![
            TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec!["t1a".into()]),
                    alias: None,
                    as_of: None,
                },
                joins: vec![Join {
                    relation: TableFactor::Table {
                        name: ObjectName(vec!["t1b".into()]),
                        alias: None,
                        as_of: None,
                    },
                    join_operator: JoinOperator::Inner(JoinConstraint::Natural),
                }]
            },
            TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec!["t2a".into()]),
                    alias: None,
                    as_of: None,
                },
                joins: vec![Join {
                    relation: TableFactor::Table {
                        name: ObjectName(vec!["t2b".into()]),
                        alias: None,
                        as_of: None,
                    },
                    join_operator: JoinOperator::Inner(JoinConstraint::Natural),
                }]
            }
        ],
        select.from,
    );
}

#[test]
fn parse_cross_join() {
    let sql = "SELECT * FROM t1 CROSS JOIN t2";
    let select = verified_only_select(sql);
    assert_eq!(
        Join {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident::new_unchecked("t2")]),
                alias: None,
                as_of: None,
            },
            join_operator: JoinOperator::CrossJoin
        },
        only(only(select.from).joins),
    );
}

#[test]
fn parse_temporal_join() {
    let sql = "SELECT * FROM t1 JOIN t2 FOR SYSTEM_TIME AS OF PROCTIME() ON c1 = c2";
    let select = verified_only_select(sql);
    assert_eq!(
        Join {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident::new_unchecked("t2")]),
                alias: None,
                as_of: Some(AsOf::ProcessTime),
            },
            join_operator: Inner(JoinConstraint::On(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("c1".into())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Identifier("c2".into())),
            }))
        },
        only(only(select.from).joins),
    );

    let sql = "SELECT * FROM t1 JOIN t2 FOR SYSTEM_TIME AS OF NOW() - '1' SECOND ON c1 = c2";
    verified_only_select(sql);
}

#[test]
fn parse_joins_on() {
    fn join_with_constraint(
        relation: impl Into<String>,
        alias: Option<TableAlias>,
        f: impl Fn(JoinConstraint) -> JoinOperator,
    ) -> Join {
        Join {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident::new_unchecked(relation.into())]),
                alias,
                as_of: None,
            },
            join_operator: f(JoinConstraint::On(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("c1".into())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Identifier("c2".into())),
            })),
        }
    }
    // Test parsing of aliases
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 JOIN t2 AS foo ON c1 = c2").from).joins,
        vec![join_with_constraint(
            "t2",
            table_alias("foo"),
            JoinOperator::Inner
        )]
    );
    one_statement_parses_to(
        "SELECT * FROM t1 JOIN t2 foo ON c1 = c2",
        "SELECT * FROM t1 JOIN t2 AS foo ON c1 = c2",
    );
    // Test parsing of different join operators
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::Inner)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 LEFT JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::LeftOuter)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 RIGHT JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::RightOuter)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 FULL JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::FullOuter)]
    );
}

#[test]
fn parse_joins_using() {
    fn join_with_constraint(
        relation: impl Into<String>,
        alias: Option<TableAlias>,
        f: impl Fn(JoinConstraint) -> JoinOperator,
    ) -> Join {
        Join {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident::new_unchecked(relation.into())]),
                alias,
                as_of: None,
            },
            join_operator: f(JoinConstraint::Using(vec!["c1".into()])),
        }
    }
    // Test parsing of aliases
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 JOIN t2 AS foo USING(c1)").from).joins,
        vec![join_with_constraint(
            "t2",
            table_alias("foo"),
            JoinOperator::Inner
        )]
    );
    one_statement_parses_to(
        "SELECT * FROM t1 JOIN t2 foo USING(c1)",
        "SELECT * FROM t1 JOIN t2 AS foo USING(c1)",
    );
    // Test parsing of different join operators
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::Inner)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 LEFT JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::LeftOuter)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 RIGHT JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::RightOuter)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 FULL JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::FullOuter)]
    );
}

#[test]
fn parse_natural_join() {
    fn natural_join(f: impl Fn(JoinConstraint) -> JoinOperator) -> Join {
        Join {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident::new_unchecked("t2")]),
                alias: None,
                as_of: None,
            },
            join_operator: f(JoinConstraint::Natural),
        }
    }
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL JOIN t2").from).joins,
        vec![natural_join(JoinOperator::Inner)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL LEFT JOIN t2").from).joins,
        vec![natural_join(JoinOperator::LeftOuter)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL RIGHT JOIN t2").from).joins,
        vec![natural_join(JoinOperator::RightOuter)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL FULL JOIN t2").from).joins,
        vec![natural_join(JoinOperator::FullOuter)]
    );

    let sql = "SELECT * FROM t1 natural";
    assert!(
        format!("{}", parse_sql_statements(sql).unwrap_err(),)
            .contains("expected a join type after NATURAL, found: EOF")
    );
}

#[test]
fn parse_complex_join() {
    let sql = "SELECT c1, c2 FROM t1, t4 JOIN t2 ON t2.c = t1.c LEFT JOIN t3 USING(q, c) WHERE t4.c = t1.c";
    verified_only_select(sql);
}

#[test]
fn parse_join_nesting() {
    let sql = "SELECT * FROM a NATURAL JOIN (b NATURAL JOIN (c NATURAL JOIN d NATURAL JOIN e)) \
               NATURAL JOIN (f NATURAL JOIN (g NATURAL JOIN h))";
    assert_eq!(
        only(&verified_only_select(sql).from).joins,
        vec![
            join(nest!(table("b"), nest!(table("c"), table("d"), table("e")))),
            join(nest!(table("f"), nest!(table("g"), table("h"))))
        ],
    );

    let sql = "SELECT * FROM (a NATURAL JOIN b) NATURAL JOIN c";
    let select = verified_only_select(sql);
    let from = only(select.from);
    assert_eq!(from.relation, nest!(table("a"), table("b")));
    assert_eq!(from.joins, vec![join(table("c"))]);

    let sql = "SELECT * FROM (((a NATURAL JOIN b)))";
    let select = verified_only_select(sql);
    let from = only(select.from);
    assert_eq!(from.relation, nest!(nest!(nest!(table("a"), table("b")))));
    assert_eq!(from.joins, vec![]);

    let sql = "SELECT * FROM a NATURAL JOIN (((b NATURAL JOIN c)))";
    let select = verified_only_select(sql);
    let from = only(select.from);
    assert_eq!(from.relation, table("a"));
    assert_eq!(
        from.joins,
        vec![join(nest!(nest!(nest!(table("b"), table("c")))))]
    );
}

#[test]
fn parse_join_syntax_variants() {
    one_statement_parses_to(
        "SELECT c1 FROM t1 INNER JOIN t2 USING(c1)",
        "SELECT c1 FROM t1 JOIN t2 USING(c1)",
    );
    one_statement_parses_to(
        "SELECT c1 FROM t1 LEFT OUTER JOIN t2 USING(c1)",
        "SELECT c1 FROM t1 LEFT JOIN t2 USING(c1)",
    );
    one_statement_parses_to(
        "SELECT c1 FROM t1 RIGHT OUTER JOIN t2 USING(c1)",
        "SELECT c1 FROM t1 RIGHT JOIN t2 USING(c1)",
    );
    one_statement_parses_to(
        "SELECT c1 FROM t1 FULL OUTER JOIN t2 USING(c1)",
        "SELECT c1 FROM t1 FULL JOIN t2 USING(c1)",
    );

    let res = parse_sql_statements("SELECT * FROM a OUTER JOIN b ON 1");
    assert!(
        format!("{}", res.unwrap_err()).contains("expected LEFT, RIGHT, or FULL, found: OUTER")
    );
}

#[test]
fn parse_ctes() {
    let cte_sqls = vec!["SELECT 1 AS foo", "SELECT 2 AS bar"];
    let with = &format!(
        "WITH a AS ({}), b AS ({}) SELECT foo + bar FROM a, b",
        cte_sqls[0], cte_sqls[1]
    );

    fn assert_ctes_in_select(expected: &[&str], sel: &Query) {
        for (i, exp) in expected.iter().enumerate() {
            let Cte {
                alias, cte_inner, ..
            } = &sel.with.as_ref().unwrap().cte_tables[i];
            if let CteInner::Query(query) = cte_inner {
                assert_eq!(*exp, query.to_string());
                assert_eq!(
                    if i == 0 {
                        Ident::new_unchecked("a")
                    } else {
                        Ident::new_unchecked("b")
                    },
                    alias.name
                );
                assert!(alias.columns.is_empty());
            } else {
                panic!("expected CteInner::Query")
            }
        }
    }

    // Top-level CTE
    assert_ctes_in_select(&cte_sqls, &verified_query(with));
    // CTE in a subquery
    let sql = &format!("SELECT ({})", with);
    let select = verified_only_select(sql);
    match expr_from_projection(only(&select.projection)) {
        Expr::Subquery(subquery) => {
            assert_ctes_in_select(&cte_sqls, subquery.as_ref());
        }
        _ => panic!("expected subquery"),
    }
    // CTE in a derived table
    let sql = &format!("SELECT * FROM ({})", with);
    let select = verified_only_select(sql);
    match only(select.from).relation {
        TableFactor::Derived { subquery, .. } => {
            assert_ctes_in_select(&cte_sqls, subquery.as_ref())
        }
        _ => panic!("expected derived table"),
    }
    // CTE in a view
    let sql = &format!("CREATE VIEW v AS {}", with);
    match verified_stmt(sql) {
        Statement::CreateView { query, .. } => assert_ctes_in_select(&cte_sqls, &query),
        _ => panic!("expected CREATE VIEW"),
    }
    // CTE in a CTE...
    let sql = &format!("WITH outer_cte AS ({}) SELECT * FROM outer_cte", with);
    let select = verified_query(sql);
    if let CteInner::Query(query) = &only(&select.with.unwrap().cte_tables).cte_inner {
        assert_ctes_in_select(&cte_sqls, query);
    } else {
        panic!("expected CteInner::Query")
    }
}

#[test]
fn parse_cte_renamed_columns() {
    let sql = "WITH cte (col1, col2) AS (SELECT foo, bar FROM baz) SELECT * FROM cte";
    let query = verified_query(sql);
    assert_eq!(
        vec![Ident::new_unchecked("col1"), Ident::new_unchecked("col2")],
        query
            .with
            .unwrap()
            .cte_tables
            .first()
            .unwrap()
            .alias
            .columns
    );

    let sql_changelog = "WITH cte (col1, col2) AS changelog from baz SELECT * FROM cte";

    let query_changelog = verified_query(sql_changelog);
    assert_eq!(
        vec![Ident::new_unchecked("col1"), Ident::new_unchecked("col2")],
        query_changelog
            .with
            .unwrap()
            .cte_tables
            .first()
            .unwrap()
            .alias
            .columns
    );
}

#[test]
fn parse_recursive_cte() {
    let cte_query = "SELECT 1 UNION ALL SELECT val + 1 FROM nums WHERE val < 10".to_owned();
    let sql = &format!(
        "WITH RECURSIVE nums (val) AS ({}) SELECT * FROM nums",
        cte_query
    );

    let cte_query = verified_query(&cte_query);
    let query = verified_query(sql);

    let with = query.with.as_ref().unwrap();
    assert!(with.recursive);
    assert_eq!(with.cte_tables.len(), 1);
    let expected = Cte {
        alias: TableAlias {
            name: Ident::new_unchecked("nums"),
            columns: vec![Ident::new_unchecked("val")],
        },
        cte_inner: CteInner::Query(Box::new(cte_query)),
    };
    assert_eq!(with.cte_tables.first().unwrap(), &expected);
}

#[test]
fn parse_derived_tables() {
    let sql = "SELECT a.x, b.y FROM (SELECT x FROM foo) AS a CROSS JOIN (SELECT y FROM bar) AS b";
    let _ = verified_only_select(sql);
    // TODO: add assertions

    let sql = "SELECT a.x, b.y \
               FROM (SELECT x FROM foo) AS a (x) \
               CROSS JOIN (SELECT y FROM bar) AS b (y)";
    let _ = verified_only_select(sql);
    // TODO: add assertions

    let sql = "SELECT * FROM (SELECT 1)";
    let _ = verified_only_select(sql);
    // TODO: add assertions

    let sql = "SELECT * FROM t NATURAL JOIN (SELECT 1)";
    let _ = verified_only_select(sql);
    // TODO: add assertions

    let sql = "SELECT * FROM (((SELECT 1) UNION (SELECT 2)) AS t1 NATURAL JOIN t2)";
    let select = verified_only_select(sql);
    let from = only(select.from);
    assert_eq!(
        from.relation,
        TableFactor::NestedJoin(Box::new(TableWithJoins {
            relation: TableFactor::Derived {
                lateral: false,
                subquery: Box::new(verified_query("(SELECT 1) UNION (SELECT 2)")),
                alias: Some(TableAlias {
                    name: "t1".into(),
                    columns: vec![],
                })
            },
            joins: vec![Join {
                relation: TableFactor::Table {
                    name: ObjectName(vec!["t2".into()]),
                    alias: None,
                    as_of: None,
                },
                join_operator: JoinOperator::Inner(JoinConstraint::Natural),
            }],
        }))
    );
}

#[test]
fn parse_union() {
    // TODO: add assertions
    verified_stmt("SELECT 1 UNION SELECT 2");
    verified_stmt("SELECT 1 UNION ALL SELECT 2");
    verified_stmt("SELECT 1 EXCEPT SELECT 2");
    verified_stmt("SELECT 1 EXCEPT ALL SELECT 2");
    verified_stmt("SELECT 1 INTERSECT SELECT 2");
    verified_stmt("SELECT 1 INTERSECT ALL SELECT 2");
    verified_stmt("SELECT 1 UNION SELECT 2 UNION SELECT 3");
    verified_stmt("SELECT 1 EXCEPT SELECT 2 UNION SELECT 3"); // Union[Except[1,2], 3]
    verified_stmt("SELECT 1 INTERSECT (SELECT 2 EXCEPT SELECT 3)");
    verified_stmt("WITH cte AS (SELECT 1 AS foo) (SELECT foo FROM cte ORDER BY 1 LIMIT 1)");
    verified_stmt("SELECT 1 UNION (SELECT 2 ORDER BY 1 LIMIT 1)");
    verified_stmt("SELECT 1 UNION SELECT 2 INTERSECT SELECT 3"); // Union[1, Intersect[2,3]]
    verified_stmt("SELECT foo FROM tab UNION SELECT bar FROM TAB");
    verified_stmt(
        "(SELECT * FROM new EXCEPT SELECT * FROM old) UNION ALL (SELECT * FROM old EXCEPT SELECT * FROM new) ORDER BY 1",
    );
}

#[test]
fn parse_values() {
    verified_stmt("SELECT * FROM (VALUES (1), (2), (3))");
    verified_stmt("SELECT * FROM (VALUES (1), (2), (3)), (VALUES (1, 2, 3))");
    verified_stmt("SELECT * FROM (VALUES (1)) UNION VALUES (1)");
}

#[test]
fn parse_multiple_statements() {
    fn test_with(sql1: &str, sql2_kw: &str, sql2_rest: &str) {
        // Check that a string consisting of two statements delimited by a semicolon
        // parses the same as both statements individually:
        let res = parse_sql_statements(&(sql1.to_owned() + ";" + sql2_kw + sql2_rest));
        assert_eq!(
            vec![
                one_statement_parses_to(sql1, ""),
                one_statement_parses_to(&(sql2_kw.to_owned() + sql2_rest), ""),
            ],
            res.unwrap()
        );
        // Check that extra semicolon at the end is stripped by normalization:
        one_statement_parses_to(&(sql1.to_owned() + ";"), sql1);
        // Check that forgetting the semicolon results in an error:
        let res = parse_sql_statements(&(sql1.to_owned() + " " + sql2_kw + sql2_rest));
        let err_msg = "expected end of statement, found: ";
        assert!(format!("{}", res.unwrap_err()).contains(err_msg));
    }
    test_with("SELECT foo", "SELECT", " bar");
    // ensure that SELECT/WITH is not parsed as a table or column alias if ';'
    // separating the statements is omitted:
    test_with("SELECT foo FROM baz", "SELECT", " bar");
    test_with("SELECT foo", "WITH", " cte AS (SELECT 1 AS s) SELECT bar");
    test_with(
        "SELECT foo FROM baz",
        "WITH",
        " cte AS (SELECT 1 AS s) SELECT bar",
    );
    test_with("DELETE FROM foo", "SELECT", " bar");
    test_with("INSERT INTO foo VALUES (1)", "SELECT", " bar");
    test_with("CREATE TABLE foo (baz INT)", "SELECT", " bar");
    // Make sure that empty statements do not cause an error:
    let res = parse_sql_statements(";;");
    assert_eq!(0, res.unwrap().len());
}

#[test]
fn parse_scalar_subqueries() {
    let sql = "(SELECT 1) + (SELECT 2)";
    assert_matches!(
        verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::Plus,
            .. /* left: box Subquery { .. },
                * right: box Subquery { .. }, */
        }
    );
}

#[test]
fn parse_substring() {
    one_statement_parses_to("SELECT SUBSTRING('1')", "SELECT SUBSTRING('1')");

    one_statement_parses_to(
        "SELECT SUBSTRING('1' FROM 1)",
        "SELECT SUBSTRING('1' FROM 1)",
    );

    one_statement_parses_to(
        "SELECT SUBSTRING('1' FROM 1 FOR 3)",
        "SELECT SUBSTRING('1' FROM 1 FOR 3)",
    );

    one_statement_parses_to("SELECT SUBSTRING('1' FOR 3)", "SELECT SUBSTRING('1' FOR 3)");
}

#[test]
fn parse_trim() {
    one_statement_parses_to(
        "SELECT TRIM(BOTH 'xyz' FROM 'xyzfooxyz')",
        "SELECT TRIM(BOTH 'xyz' FROM 'xyzfooxyz')",
    );

    one_statement_parses_to(
        "SELECT TRIM(LEADING 'xyz' FROM 'xyzfooxyz')",
        "SELECT TRIM(LEADING 'xyz' FROM 'xyzfooxyz')",
    );

    one_statement_parses_to(
        "SELECT TRIM(TRAILING 'xyz' FROM 'xyzfooxyz')",
        "SELECT TRIM(TRAILING 'xyz' FROM 'xyzfooxyz')",
    );

    one_statement_parses_to("SELECT TRIM('   foo   ')", "SELECT TRIM(FROM '   foo   ')");

    let res = parse_sql_statements("SELECT TRIM(FOO 'xyz' FROM 'xyzfooxyz')");

    let err_msg = "expected ), found: 'xyz'";
    assert!(format!("{}", res.unwrap_err()).contains(err_msg));
}

#[test]
fn parse_exists_subquery() {
    let expected_inner = verified_query("SELECT 1");
    let sql = "SELECT * FROM t WHERE EXISTS (SELECT 1)";
    let select = verified_only_select(sql);
    assert_eq!(
        Expr::Exists(Box::new(expected_inner.clone())),
        select.selection.unwrap(),
    );

    let sql = "SELECT * FROM t WHERE NOT EXISTS (SELECT 1)";
    let select = verified_only_select(sql);
    assert_eq!(
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Exists(Box::new(expected_inner))),
        },
        select.selection.unwrap(),
    );

    verified_stmt("SELECT * FROM t WHERE EXISTS (WITH u AS (SELECT 1) SELECT * FROM u)");
    verified_stmt("SELECT EXISTS (SELECT 1)");

    let res = parse_sql_statements("SELECT EXISTS (");
    let err_msg = "expected SELECT, VALUES, or a subquery in the query body, found: EOF";
    assert!(format!("{}", res.unwrap_err()).contains(err_msg));

    let res = parse_sql_statements("SELECT EXISTS (NULL)");

    let err_msg = "expected SELECT, VALUES, or a subquery in the query body, found: NULL";
    assert!(format!("{}", res.unwrap_err()).contains(err_msg));
}

#[test]
fn parse_create_view() {
    let sql = "CREATE VIEW IF NOT EXISTS myschema.myview AS SELECT foo FROM bar";
    match verified_stmt(sql) {
        Statement::CreateView {
            name,
            if_not_exists,
            columns,
            query,
            or_replace,
            materialized,
            with_options,
            emit_mode,
        } => {
            assert_eq!("myschema.myview", name.to_string());
            assert!(if_not_exists);
            assert_eq!(Vec::<Ident>::new(), columns);
            assert_eq!("SELECT foo FROM bar", query.to_string());
            assert!(!materialized);
            assert!(!or_replace);
            assert_eq!(with_options, vec![]);
            assert_eq!(emit_mode, None);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_view_with_options() {
    let sql = "CREATE VIEW v WITH (foo = 'bar', a = 123) AS SELECT 1";
    match verified_stmt(sql) {
        Statement::CreateView { with_options, .. } => {
            assert_eq!(
                vec![
                    SqlOption {
                        name: vec!["foo".into()].into(),
                        value: Value::SingleQuotedString("bar".into()).into(),
                    },
                    SqlOption {
                        name: vec!["a".into()].into(),
                        value: number("123").into(),
                    },
                ],
                with_options
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_view_with_columns() {
    let sql = "CREATE VIEW v (has, cols) AS SELECT 1, 2";
    match verified_stmt(sql) {
        Statement::CreateView {
            if_not_exists,
            name,
            columns,
            or_replace,
            with_options,
            query,
            materialized,
            emit_mode,
        } => {
            assert!(!if_not_exists);
            assert_eq!("v", name.to_string());
            assert_eq!(
                columns,
                vec![Ident::new_unchecked("has"), Ident::new_unchecked("cols")]
            );
            assert_eq!(with_options, vec![]);
            assert_eq!("SELECT 1, 2", query.to_string());
            assert!(!materialized);
            assert!(!or_replace);
            assert_eq!(emit_mode, None);
        }
        _ => unreachable!(),
    }
}
#[test]
fn parse_create_or_replace_view() {
    let sql = "CREATE OR REPLACE VIEW v AS SELECT 1";
    match verified_stmt(sql) {
        Statement::CreateView {
            if_not_exists,
            name,
            columns,
            or_replace,
            with_options,
            query,
            materialized,
            emit_mode,
        } => {
            assert!(!if_not_exists);
            assert_eq!("v", name.to_string());
            assert_eq!(columns, vec![]);
            assert_eq!(with_options, vec![]);
            assert_eq!("SELECT 1", query.to_string());
            assert!(!materialized);
            assert!(or_replace);
            assert_eq!(emit_mode, None);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_or_replace_materialized_view() {
    // Supported in BigQuery (Beta)
    // https://cloud.google.com/bigquery/docs/materialized-views-intro
    // and Snowflake:
    // https://docs.snowflake.com/en/sql-reference/sql/create-materialized-view.html
    let sql = "CREATE OR REPLACE MATERIALIZED VIEW v AS SELECT 1";
    match verified_stmt(sql) {
        Statement::CreateView {
            if_not_exists,
            name,
            columns,
            or_replace,
            with_options,
            query,
            materialized,
            emit_mode,
        } => {
            assert!(!if_not_exists);
            assert_eq!("v", name.to_string());
            assert!(!if_not_exists);
            assert_eq!(columns, vec![]);
            assert_eq!(with_options, vec![]);
            assert_eq!("SELECT 1", query.to_string());
            assert!(materialized);
            assert!(or_replace);
            assert_eq!(emit_mode, None);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_materialized_view() {
    let sql = "CREATE MATERIALIZED VIEW myschema.myview AS SELECT foo FROM bar";
    match verified_stmt(sql) {
        Statement::CreateView {
            if_not_exists,
            name,
            or_replace,
            columns,
            query,
            materialized,
            with_options,
            emit_mode,
        } => {
            assert!(!if_not_exists);
            assert_eq!("myschema.myview", name.to_string());
            assert_eq!(Vec::<Ident>::new(), columns);
            assert_eq!("SELECT foo FROM bar", query.to_string());
            assert!(materialized);
            assert_eq!(with_options, vec![]);
            assert!(!or_replace);
            assert_eq!(emit_mode, None);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_materialized_view_emit_immediately() {
    let sql = "CREATE MATERIALIZED VIEW myschema.myview AS SELECT foo FROM bar EMIT IMMEDIATELY";
    match verified_stmt(sql) {
        Statement::CreateView {
            if_not_exists,
            name,
            or_replace,
            columns,
            query,
            materialized,
            with_options,
            emit_mode,
        } => {
            assert!(!if_not_exists);
            assert_eq!("myschema.myview", name.to_string());
            assert_eq!(Vec::<Ident>::new(), columns);
            assert_eq!("SELECT foo FROM bar", query.to_string());
            assert!(materialized);
            assert_eq!(with_options, vec![]);
            assert!(!or_replace);
            assert_eq!(emit_mode, Some(EmitMode::Immediately));
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_on_conflict() {
    let sql = "CREATE TABLE myschema.myview ON CONFLICT DO UPDATE FULL AS SELECT foo FROM bar";
    match verified_stmt(sql) {
        Statement::CreateTable {
            name,
            query,
            on_conflict,
            ..
        } => {
            assert_eq!("myschema.myview", name.to_string());
            assert_eq!(query, Some(Box::new(verified_query("SELECT foo FROM bar"))));
            assert_eq!(on_conflict, Some(OnConflict::UpdateFull));
        }
        _ => unreachable!(),
    }

    let sql = "CREATE TABLE myschema.myview ON CONFLICT DO NOTHING AS SELECT foo FROM bar";
    match verified_stmt(sql) {
        Statement::CreateTable {
            name,
            query,
            on_conflict,
            ..
        } => {
            assert_eq!("myschema.myview", name.to_string());
            assert_eq!(query, Some(Box::new(verified_query("SELECT foo FROM bar"))));
            assert_eq!(on_conflict, Some(OnConflict::Nothing));
        }
        _ => unreachable!(),
    }

    let sql = "CREATE TABLE myschema.myview AS SELECT foo FROM bar";
    match verified_stmt(sql) {
        Statement::CreateTable {
            name,
            query,
            on_conflict,
            ..
        } => {
            assert_eq!("myschema.myview", name.to_string());
            assert_eq!(query, Some(Box::new(verified_query("SELECT foo FROM bar"))));
            assert_eq!(on_conflict, None);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_on_conflict_with_version_column() {
    let sql1 = "CREATE TABLE t (v1 INT, v2 INT, v3 INT, PRIMARY KEY (v1)) ON CONFLICT DO UPDATE FULL WITH VERSION COLUMN(v2)";
    match verified_stmt(sql1) {
        Statement::CreateTable {
            name,
            on_conflict,
            with_version_columns,
            ..
        } => {
            assert_eq!("t", name.to_string());
            assert_eq!(on_conflict, Some(OnConflict::UpdateFull));
            assert_eq!(with_version_columns, vec![Ident::new_unchecked("v2")]);
        }
        _ => unreachable!(),
    }

    let invalid_sql1 = "CREATE TABLE t (v1 INT, v2 INT, v3 INT, PRIMARY KEY (v1)) ON CONFLICT DO UPDATE FULL WITH VERSION COLUMN(v2";
    let res = parse_sql_statements(invalid_sql1);
    assert!(res.is_err());

    let invalid_sql2 = "CREATE TABLE t (v1 INT, v2 INT, v3 INT, PRIMARY KEY (v1)) ON CONFLICT DO UPDATE FULL WITH VERSION COLUMN";
    let res = parse_sql_statements(invalid_sql2);
    assert!(res.is_err());

    let invalid_sql3 = "CREATE TABLE t (v1 INT, v2 INT, v3 INT, PRIMARY KEY (v1)) ON CONFLICT DO UPDATE FULL WITH COLUMN";
    let res = parse_sql_statements(invalid_sql3);
    assert!(res.is_err());
}

#[test]
fn parse_create_materialized_view_emit_on_window_close() {
    let sql =
        "CREATE MATERIALIZED VIEW myschema.myview AS SELECT foo FROM bar EMIT ON WINDOW CLOSE";
    match verified_stmt(sql) {
        Statement::CreateView {
            if_not_exists,
            name,
            or_replace,
            columns,
            query,
            materialized,
            with_options,
            emit_mode,
        } => {
            assert!(!if_not_exists);
            assert_eq!("myschema.myview", name.to_string());
            assert_eq!(Vec::<Ident>::new(), columns);
            assert_eq!("SELECT foo FROM bar", query.to_string());
            assert!(materialized);
            assert_eq!(with_options, vec![]);
            assert!(!or_replace);
            assert_eq!(emit_mode, Some(EmitMode::OnWindowClose));
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_table() {
    let sql = "DROP TABLE foo";
    match verified_stmt(sql) {
        Statement::Drop(stmt) => {
            assert!(!stmt.if_exists);
            assert_eq!(ObjectType::Table, stmt.object_type);
            assert_eq!(
                ObjectName(vec![Ident::new_unchecked("foo")]),
                stmt.object_name
            );
            assert_eq!(stmt.drop_mode, AstOption::None);
        }
        _ => unreachable!(),
    }

    let sql = "DROP TABLE IF EXISTS foo CASCADE";
    match verified_stmt(sql) {
        Statement::Drop(stmt) => {
            assert!(stmt.if_exists);
            assert_eq!(ObjectType::Table, stmt.object_type);
            assert_eq!(
                ObjectName(vec![Ident::new_unchecked("foo")]),
                stmt.object_name
            );
            assert_eq!(stmt.drop_mode, AstOption::Some(DropMode::Cascade));
        }
        _ => unreachable!(),
    };

    let sql = "DROP TABLE";
    assert!(
        format!("{}", parse_sql_statements(sql).unwrap_err(),)
            .contains("expected identifier, found: EOF")
    );

    let sql = "DROP TABLE IF EXISTS foo CASCADE RESTRICT";
    assert!(
        format!("{}", parse_sql_statements(sql).unwrap_err(),)
            .contains("expected end of statement, found: RESTRICT")
    );
}

#[test]
fn parse_drop_view() {
    let sql = "DROP VIEW myview";
    match verified_stmt(sql) {
        Statement::Drop(stmt) => {
            assert_eq!(
                ObjectName(vec![Ident::new_unchecked("myview")]),
                stmt.object_name
            );
            assert_eq!(ObjectType::View, stmt.object_type);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_materialized_drop_view() {
    let sql = "DROP MATERIALIZED VIEW mymview";
    match verified_stmt(sql) {
        Statement::Drop(stmt) => {
            assert_eq!(
                ObjectName(vec![Ident::new_unchecked("mymview")]),
                stmt.object_name
            );
            assert_eq!(ObjectType::MaterializedView, stmt.object_type);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_user() {
    let sql = "CREATE USER foo WITH NOSUPERUSER CREATEDB LOGIN PASSWORD 'md5827ccb0eea8a706c4c34a16891f84e7b'";
    match verified_stmt(sql) {
        Statement::CreateUser(stmt) => {
            assert_eq!(
                ObjectName(vec![Ident::new_unchecked("foo")]),
                stmt.user_name
            );
            assert_eq!(
                stmt.with_options.0,
                vec![
                    UserOption::NoSuperUser,
                    UserOption::CreateDB,
                    UserOption::Login,
                    UserOption::Password(Some(AstString(
                        "md5827ccb0eea8a706c4c34a16891f84e7b".into()
                    ))),
                ]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_user_without_with_prefix() {
    one_statement_parses_to(
        "ALTER USER WITH PASSWORD 'rw_password_2'",
        "ALTER USER WITH PASSWORD 'rw_password_2'",
    );
}

#[test]
fn parse_invalid_subquery_without_parens() {
    let res = parse_sql_statements("SELECT SELECT 1 FROM bar WHERE 1=1 FROM baz");
    assert!(format!("{}", res.unwrap_err()).contains("expected end of statement, found: 1"));
}

#[test]
fn parse_offset() {
    let expect = Some("2".to_owned());
    let ast = verified_query("SELECT foo FROM bar OFFSET 2");
    assert_eq!(ast.offset, expect);
    let ast = verified_query("SELECT foo FROM bar WHERE foo = 4 OFFSET 2");
    assert_eq!(ast.offset, expect);
    let ast = verified_query("SELECT foo FROM bar ORDER BY baz OFFSET 2");
    assert_eq!(ast.offset, expect);
    let ast = verified_query("SELECT foo FROM bar WHERE foo = 4 ORDER BY baz OFFSET 2");
    assert_eq!(ast.offset, expect);
    let ast = verified_query("SELECT foo FROM (SELECT * FROM bar OFFSET 2) OFFSET 2");
    assert_eq!(ast.offset, expect);
    match ast.body {
        SetExpr::Select(s) => match only(s.from).relation {
            TableFactor::Derived { subquery, .. } => {
                assert_eq!(subquery.offset, expect);
            }
            _ => panic!("Test broke"),
        },
        _ => panic!("Test broke"),
    }
    let ast = query("SELECT 'foo' OFFSET 0 ROWS", "SELECT 'foo' OFFSET 0");
    assert_eq!(ast.offset, Some("0".to_owned()));
    let ast = query("SELECT 'foo' OFFSET 1 ROW", "SELECT 'foo' OFFSET 1");
    assert_eq!(ast.offset, Some("1".to_owned()));
    let ast = verified_query("SELECT 'foo' OFFSET 1");
    assert_eq!(ast.offset, Some("1".to_owned()));
}

#[test]
fn parse_fetch() {
    let fetch_first_two_rows_only = Some(Fetch {
        with_ties: false,

        quantity: Some("2".to_owned()),
    });
    let ast = verified_query("SELECT foo FROM bar FETCH FIRST 2 ROWS ONLY");
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    let ast = verified_query("SELECT 'foo' FETCH FIRST 2 ROWS ONLY");
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    let ast = verified_query("SELECT foo FROM bar FETCH FIRST ROWS ONLY");
    assert_eq!(
        ast.fetch,
        Some(Fetch {
            with_ties: false,
            quantity: None,
        })
    );
    let ast = verified_query("SELECT foo FROM bar WHERE foo = 4 FETCH FIRST 2 ROWS ONLY");
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    let ast = verified_query("SELECT foo FROM bar ORDER BY baz FETCH FIRST 2 ROWS ONLY");
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    let ast = verified_query(
        "SELECT foo FROM bar WHERE foo = 4 ORDER BY baz FETCH FIRST 2 ROWS WITH TIES",
    );
    assert_eq!(
        ast.fetch,
        Some(Fetch {
            with_ties: true,
            quantity: Some("2".to_owned()),
        })
    );
    let ast = verified_query(
        "SELECT foo FROM bar WHERE foo = 4 ORDER BY baz OFFSET 2 FETCH FIRST 2 ROWS ONLY",
    );
    assert_eq!(ast.offset, Some("2".to_owned()));
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    let ast = verified_query(
        "SELECT foo FROM (SELECT * FROM bar FETCH FIRST 2 ROWS ONLY) FETCH FIRST 2 ROWS ONLY",
    );
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    match ast.body {
        SetExpr::Select(s) => match only(s.from).relation {
            TableFactor::Derived { subquery, .. } => {
                assert_eq!(subquery.fetch, fetch_first_two_rows_only);
            }
            _ => panic!("Test broke"),
        },
        _ => panic!("Test broke"),
    }
    let ast = verified_query(
        "SELECT foo FROM (SELECT * FROM bar OFFSET 2 FETCH FIRST 2 ROWS ONLY) OFFSET 2 FETCH FIRST 2 ROWS ONLY",
    );
    assert_eq!(ast.offset, Some("2".to_owned()));
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    match ast.body {
        SetExpr::Select(s) => match only(s.from).relation {
            TableFactor::Derived { subquery, .. } => {
                assert_eq!(subquery.offset, Some("2".to_owned()));
                assert_eq!(subquery.fetch, fetch_first_two_rows_only);
            }
            _ => panic!("Test broke"),
        },
        _ => panic!("Test broke"),
    }
}

#[test]
fn parse_fetch_variations() {
    one_statement_parses_to(
        "SELECT foo FROM bar FETCH FIRST 10 ROW ONLY",
        "SELECT foo FROM bar FETCH FIRST 10 ROWS ONLY",
    );
    one_statement_parses_to(
        "SELECT foo FROM bar FETCH NEXT 10 ROW ONLY",
        "SELECT foo FROM bar FETCH FIRST 10 ROWS ONLY",
    );
    one_statement_parses_to(
        "SELECT foo FROM bar ORDER BY baz FETCH NEXT 10 ROWS WITH TIES",
        "SELECT foo FROM bar ORDER BY baz FETCH FIRST 10 ROWS WITH TIES",
    );
    one_statement_parses_to(
        "SELECT foo FROM bar ORDER BY baz FETCH NEXT ROWS WITH TIES",
        "SELECT foo FROM bar ORDER BY baz FETCH FIRST ROWS WITH TIES",
    );
    one_statement_parses_to(
        "SELECT foo FROM bar FETCH FIRST ROWS ONLY",
        "SELECT foo FROM bar FETCH FIRST ROWS ONLY",
    );
}

#[test]
fn lateral_derived() {
    fn chk(lateral_in: bool) {
        let lateral_str = if lateral_in { "LATERAL " } else { "" };
        let sql = format!(
            "SELECT * FROM customer LEFT JOIN {}\
             (SELECT * FROM orders WHERE orders.customer = customer.id LIMIT 3) AS orders ON true",
            lateral_str
        );
        let select = verified_only_select(&sql);
        let from = only(select.from);
        assert_eq!(from.joins.len(), 1);
        let join = &from.joins[0];
        assert_eq!(
            join.join_operator,
            JoinOperator::LeftOuter(JoinConstraint::On(Expr::Value(Value::Boolean(true))))
        );
        if let TableFactor::Derived {
            lateral,
            ref subquery,
            alias: Some(ref alias),
        } = join.relation
        {
            assert_eq!(lateral_in, lateral);
            assert_eq!(Ident::new_unchecked("orders"), alias.name);
            assert_eq!(
                subquery.to_string(),
                "SELECT * FROM orders WHERE orders.customer = customer.id LIMIT 3"
            );
        } else {
            unreachable!()
        }
    }
    chk(false);
    chk(true);

    let sql = "SELECT * FROM customer LEFT JOIN LATERAL generate_series(1, customer.id)";
    let res = parse_sql_statements(sql);
    assert!(
        format!("{}", res.unwrap_err())
            .contains("expected subquery after LATERAL, found: generate_series")
    );

    let sql = "SELECT * FROM a LEFT JOIN LATERAL (b CROSS JOIN c)";
    let res = parse_sql_statements(sql);
    assert!(
        format!("{}", res.unwrap_err())
            .contains("expected SELECT, VALUES, or a subquery in the query body, found: b")
    );
}

#[test]
fn parse_start_transaction() {
    match verified_stmt("START TRANSACTION READ ONLY, READ WRITE, ISOLATION LEVEL SERIALIZABLE") {
        Statement::StartTransaction { modes } => assert_eq!(
            modes,
            vec![
                TransactionMode::AccessMode(TransactionAccessMode::ReadOnly),
                TransactionMode::AccessMode(TransactionAccessMode::ReadWrite),
                TransactionMode::IsolationLevel(TransactionIsolationLevel::Serializable),
            ]
        ),
        _ => unreachable!(),
    }

    // For historical reasons, PostgreSQL allows the commas between the modes to
    // be omitted.
    match one_statement_parses_to(
        "START TRANSACTION READ ONLY READ WRITE ISOLATION LEVEL SERIALIZABLE",
        "START TRANSACTION READ ONLY, READ WRITE, ISOLATION LEVEL SERIALIZABLE",
    ) {
        Statement::StartTransaction { modes } => assert_eq!(
            modes,
            vec![
                TransactionMode::AccessMode(TransactionAccessMode::ReadOnly),
                TransactionMode::AccessMode(TransactionAccessMode::ReadWrite),
                TransactionMode::IsolationLevel(TransactionIsolationLevel::Serializable),
            ]
        ),
        _ => unreachable!(),
    }

    verified_stmt("START TRANSACTION");

    verified_stmt("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
    verified_stmt("START TRANSACTION ISOLATION LEVEL READ COMMITTED");
    verified_stmt("START TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    verified_stmt("START TRANSACTION ISOLATION LEVEL SERIALIZABLE");

    // Regression test for https://github.com/sqlparser-rs/sqlparser-rs/pull/139,
    // in which START TRANSACTION would fail to parse if followed by a statement
    // terminator.
    assert_eq!(
        parse_sql_statements("START TRANSACTION; SELECT 1"),
        Ok(vec![
            verified_stmt("START TRANSACTION"),
            verified_stmt("SELECT 1"),
        ])
    );

    let res = parse_sql_statements("START TRANSACTION ISOLATION LEVEL BAD");
    assert!(format!("{}", res.unwrap_err()).contains("expected isolation level, found: BAD"));

    let res = parse_sql_statements("START TRANSACTION BAD");
    assert!(format!("{}", res.unwrap_err()).contains("expected end of statement, found: BAD"));

    let res = parse_sql_statements("START TRANSACTION READ ONLY,");
    assert!(format!("{}", res.unwrap_err()).contains("expected transaction mode, found: EOF"));
}

#[test]
fn parse_begin() {
    one_statement_parses_to("BEGIN", "BEGIN");
    one_statement_parses_to("BEGIN WORK", "BEGIN");
    one_statement_parses_to("BEGIN TRANSACTION", "BEGIN");
}

#[test]
fn parse_alter_system_clear_file_cache() {
    one_statement_parses_to(
        "ALTER SYSTEM CLEAR FILE CACHE META",
        "ALTER SYSTEM CLEAR FILE CACHE META",
    );
    one_statement_parses_to(
        "ALTER SYSTEM CLEAR FILE CACHE DATA",
        "ALTER SYSTEM CLEAR FILE CACHE DATA",
    );
    one_statement_parses_to(
        "ALTER SYSTEM CLEAR FILE CACHE ALL",
        "ALTER SYSTEM CLEAR FILE CACHE ALL",
    );
}

#[test]
fn parse_set_transaction() {
    // SET TRANSACTION shares transaction mode parsing code with START
    // TRANSACTION, so no need to duplicate the tests here. We just do a quick
    // sanity check.
    match verified_stmt("SET TRANSACTION READ ONLY, READ WRITE, ISOLATION LEVEL SERIALIZABLE") {
        Statement::SetTransaction {
            modes,
            session,
            snapshot,
        } => {
            assert_eq!(
                modes,
                vec![
                    TransactionMode::AccessMode(TransactionAccessMode::ReadOnly),
                    TransactionMode::AccessMode(TransactionAccessMode::ReadWrite),
                    TransactionMode::IsolationLevel(TransactionIsolationLevel::Serializable),
                ]
            );
            assert!(!session);
            assert_eq!(snapshot, None);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_commit() {
    match verified_stmt("COMMIT") {
        Statement::Commit { chain: false } => (),
        _ => unreachable!(),
    }

    match verified_stmt("COMMIT AND CHAIN") {
        Statement::Commit { chain: true } => (),
        _ => unreachable!(),
    }

    one_statement_parses_to("COMMIT AND NO CHAIN", "COMMIT");
    one_statement_parses_to("COMMIT WORK AND NO CHAIN", "COMMIT");
    one_statement_parses_to("COMMIT TRANSACTION AND NO CHAIN", "COMMIT");
    one_statement_parses_to("COMMIT WORK AND CHAIN", "COMMIT AND CHAIN");
    one_statement_parses_to("COMMIT TRANSACTION AND CHAIN", "COMMIT AND CHAIN");
    one_statement_parses_to("COMMIT WORK", "COMMIT");
    one_statement_parses_to("COMMIT TRANSACTION", "COMMIT");
}

#[test]
fn parse_rollback() {
    match verified_stmt("ROLLBACK") {
        Statement::Rollback { chain: false } => (),
        _ => unreachable!(),
    }

    match verified_stmt("ROLLBACK AND CHAIN") {
        Statement::Rollback { chain: true } => (),
        _ => unreachable!(),
    }

    one_statement_parses_to("ROLLBACK AND NO CHAIN", "ROLLBACK");
    one_statement_parses_to("ROLLBACK WORK AND NO CHAIN", "ROLLBACK");
    one_statement_parses_to("ROLLBACK TRANSACTION AND NO CHAIN", "ROLLBACK");
    one_statement_parses_to("ROLLBACK WORK AND CHAIN", "ROLLBACK AND CHAIN");
    one_statement_parses_to("ROLLBACK TRANSACTION AND CHAIN", "ROLLBACK AND CHAIN");
    one_statement_parses_to("ROLLBACK WORK", "ROLLBACK");
    one_statement_parses_to("ROLLBACK TRANSACTION", "ROLLBACK");
}

#[test]
fn parse_create_index() {
    let sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_name ON test USING hnsw (name, age DESC) INCLUDE(other) DISTRIBUTED BY(name) WITH (m = 16)";
    let indexed_columns = vec![
        OrderByExpr {
            expr: Expr::Identifier(Ident::new_unchecked("name")),
            asc: None,
            nulls_first: None,
        },
        OrderByExpr {
            expr: Expr::Identifier(Ident::new_unchecked("age")),
            asc: Some(false),
            nulls_first: None,
        },
    ];

    let include_columns = vec![Ident::new_unchecked("other")];
    let distributed_columns = vec![Expr::Identifier(Ident::new_unchecked("name"))];
    match verified_stmt(sql) {
        Statement::CreateIndex {
            name,
            table_name,
            columns,
            method,
            include,
            distributed_by,
            unique,
            if_not_exists,
            with_properties,
        } => {
            assert_eq!("idx_name", name.to_string());
            assert_eq!("test", table_name.to_string());
            assert_eq!(indexed_columns, columns);
            assert_eq!(method, Some(Ident::new_unchecked("hnsw")));
            assert_eq!(include_columns, include);
            assert_eq!(distributed_columns, distributed_by);
            assert!(unique);
            assert!(if_not_exists);
            assert_eq!(
                with_properties.0,
                vec![SqlOption {
                    name: ObjectName::from_test_str("m"),
                    value: SqlOptionValue::Value(Value::Number("16".to_owned())),
                }]
            )
        }
        _ => unreachable!(),
    }
    verified_stmt(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_name ON test(name, age DESC) INCLUDE(other) DISTRIBUTED BY(name)",
    );
    verified_stmt(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_name ON test(name, age DESC) INCLUDE(other) DISTRIBUTED BY(name) WITH (m = 16)",
    );
}

#[test]
fn parse_grant() {
    let sql = "GRANT SELECT, INSERT, UPDATE (shape, size), EXECUTE, TEMPORARY, USAGE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON abc, def TO xyz, m WITH GRANT OPTION GRANTED BY jj";
    match verified_stmt(sql) {
        Statement::Grant {
            privileges,
            objects,
            grantees,
            with_grant_option,
            granted_by,
            ..
        } => match (privileges, objects) {
            (Privileges::Actions(actions), GrantObjects::Tables(objects)) => {
                assert_eq!(
                    vec![
                        Action::Select { columns: None },
                        Action::Insert { columns: None },
                        Action::Update {
                            columns: Some(vec![
                                Ident::new_unchecked("shape"),
                                Ident::new_unchecked("size")
                            ])
                        },
                        Action::Execute,
                        Action::Temporary,
                        Action::Usage,
                        Action::Delete,
                        Action::Truncate,
                        Action::References { columns: None },
                        Action::Trigger,
                    ],
                    actions
                );
                assert_eq!(
                    vec!["abc", "def"],
                    objects.iter().map(ToString::to_string).collect::<Vec<_>>()
                );
                assert_eq!(
                    vec!["xyz", "m"],
                    grantees.iter().map(ToString::to_string).collect::<Vec<_>>()
                );
                assert!(with_grant_option);
                assert_eq!("jj", granted_by.unwrap().to_string());
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }

    let sql2 = "GRANT INSERT ON ALL TABLES IN SCHEMA public TO browser";
    match verified_stmt(sql2) {
        Statement::Grant {
            privileges,
            objects,
            grantees,
            with_grant_option,
            ..
        } => match (privileges, objects) {
            (Privileges::Actions(actions), GrantObjects::AllTablesInSchema { schemas }) => {
                assert_eq!(vec![Action::Insert { columns: None }], actions);
                assert_eq!(
                    vec!["public"],
                    schemas.iter().map(ToString::to_string).collect::<Vec<_>>()
                );
                assert_eq!(
                    vec!["browser"],
                    grantees.iter().map(ToString::to_string).collect::<Vec<_>>()
                );
                assert!(!with_grant_option);
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }

    let sql3 = "GRANT USAGE, SELECT ON SEQUENCE p TO u";
    match verified_stmt(sql3) {
        Statement::Grant {
            privileges,
            objects,
            grantees,
            granted_by,
            ..
        } => match (privileges, objects, granted_by) {
            (Privileges::Actions(actions), GrantObjects::Sequences(objects), None) => {
                assert_eq!(
                    vec![Action::Usage, Action::Select { columns: None }],
                    actions
                );
                assert_eq!(
                    vec!["p"],
                    objects.iter().map(ToString::to_string).collect::<Vec<_>>()
                );
                assert_eq!(
                    vec!["u"],
                    grantees.iter().map(ToString::to_string).collect::<Vec<_>>()
                );
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }

    let sql4 = "GRANT ALL PRIVILEGES ON aa, b TO z";
    match verified_stmt(sql4) {
        Statement::Grant { privileges, .. } => {
            assert_eq!(
                Privileges::All {
                    with_privileges_keyword: true
                },
                privileges
            );
        }
        _ => unreachable!(),
    }

    let sql5 = "GRANT ALL ON SCHEMA aa, b TO z";
    match verified_stmt(sql5) {
        Statement::Grant {
            privileges,
            objects,
            ..
        } => match (privileges, objects) {
            (
                Privileges::All {
                    with_privileges_keyword,
                },
                GrantObjects::Schemas(schemas),
            ) => {
                assert!(!with_privileges_keyword);
                assert_eq!(
                    vec!["aa", "b"],
                    schemas.iter().map(ToString::to_string).collect::<Vec<_>>()
                );
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }

    let sql6 = "GRANT USAGE ON ALL SEQUENCES IN SCHEMA bus TO a, beta WITH GRANT OPTION";
    match verified_stmt(sql6) {
        Statement::Grant {
            privileges,
            objects,
            ..
        } => match (privileges, objects) {
            (Privileges::Actions(actions), GrantObjects::AllSequencesInSchema { schemas }) => {
                assert_eq!(vec![Action::Usage], actions);
                assert_eq!(
                    vec!["bus"],
                    schemas.iter().map(ToString::to_string).collect::<Vec<_>>()
                );
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

#[test]
fn test_revoke() {
    let sql = "REVOKE ALL PRIVILEGES ON users, auth FROM analyst CASCADE";
    match verified_stmt(sql) {
        Statement::Revoke {
            privileges,
            objects: GrantObjects::Tables(tables),
            grantees,
            cascade,
            revoke_grant_option,
            granted_by,
        } => {
            assert_eq!(
                Privileges::All {
                    with_privileges_keyword: true
                },
                privileges
            );
            assert_eq!(
                vec!["users", "auth"],
                tables.iter().map(ToString::to_string).collect::<Vec<_>>()
            );
            assert_eq!(
                vec!["analyst"],
                grantees.iter().map(ToString::to_string).collect::<Vec<_>>()
            );
            assert!(cascade);
            assert!(!revoke_grant_option);
            assert_eq!(None, granted_by);
        }
        _ => unreachable!(),
    }
}

#[test]
fn all_keywords_sorted() {
    // assert!(ALL_KEYWORDS.is_sorted())
    let mut copy = Vec::from(ALL_KEYWORDS);
    copy.sort_unstable();
    assert_eq!(copy, ALL_KEYWORDS)
}

#[test]
fn parse_window_clause() {
    // Test that WINDOW clause can be parsed
    let sql = "SELECT sum(foo) OVER w FROM t WINDOW w AS (PARTITION BY col)";
    let ast = parse_sql_statements(sql).unwrap();
    // For now just verify it parses without errors
    assert_eq!(ast.len(), 1);

    // Test multiple named windows
    let sql = "SELECT sum(foo) OVER w1, avg(bar) OVER w2 FROM t WINDOW w1 AS (PARTITION BY col1), w2 AS (ORDER BY col2)";
    let ast = parse_sql_statements(sql).unwrap();
    assert_eq!(ast.len(), 1);

    // Test inline window specification (should still work)
    let sql = "SELECT sum(foo) OVER (PARTITION BY col) FROM t";
    let ast = parse_sql_statements(sql).unwrap();
    assert_eq!(ast.len(), 1);
}

#[test]
fn parse_alter_fragment_set_parallelism() {
    match verified_stmt("ALTER FRAGMENT 1 SET PARALLELISM TO 4") {
        Statement::AlterFragment {
            fragment_ids,
            operation,
        } => {
            assert_eq!(fragment_ids, vec![1]);
            match operation {
                AlterFragmentOperation::SetParallelism { parallelism } => {
                    assert_eq!(
                        parallelism,
                        SetVariableValue::Single(SetVariableValueSingle::Literal(Value::Number(
                            "4".into()
                        )))
                    );
                }
                _ => panic!("unexpected alter fragment operation"),
            }
        }
        _ => panic!("unexpected statement kind"),
    }

    match verified_stmt("ALTER FRAGMENT 2 SET PARALLELISM TO DEFAULT") {
        Statement::AlterFragment {
            fragment_ids,
            operation,
        } => {
            assert_eq!(fragment_ids, vec![2]);
            match operation {
                AlterFragmentOperation::SetParallelism { parallelism } => {
                    assert_eq!(parallelism, SetVariableValue::Default);
                }
                _ => panic!("unexpected alter fragment operation"),
            }
        }
        _ => panic!("unexpected statement kind"),
    }

    match verified_stmt("ALTER FRAGMENT 1, 2, 3 SET PARALLELISM TO 8") {
        Statement::AlterFragment {
            fragment_ids,
            operation,
        } => {
            assert_eq!(fragment_ids, vec![1, 2, 3]);
            match operation {
                AlterFragmentOperation::SetParallelism { parallelism } => {
                    assert_eq!(
                        parallelism,
                        SetVariableValue::Single(SetVariableValueSingle::Literal(Value::Number(
                            "8".into()
                        )))
                    );
                }
                _ => panic!("unexpected alter fragment operation"),
            }
        }
        _ => panic!("unexpected statement kind"),
    }
}

#[test]
fn parse_wait_target() {
    one_statement_parses_to("WAIT", "WAIT");
    one_statement_parses_to("WAIT TABLE t", "WAIT TABLE t");
    one_statement_parses_to("WAIT MATERIALIZED VIEW mv1", "WAIT MATERIALIZED VIEW mv1");
    one_statement_parses_to("WAIT SINK snk1", "WAIT SINK snk1");
    one_statement_parses_to("WAIT INDEX idx1", "WAIT INDEX idx1");
}

#[test]
fn parse_alter_compaction_group() {
    use risingwave_sqlparser::ast::AlterCompactionGroupOperation;

    // Single group with single config
    match verified_stmt("ALTER COMPACTION GROUP 2 SET max_bytes_for_level_base = 1073741824") {
        Statement::AlterCompactionGroup {
            group_ids,
            operation,
        } => {
            assert_eq!(group_ids, vec![2]);
            match operation {
                AlterCompactionGroupOperation::Set { configs } => {
                    assert_eq!(configs.len(), 1);
                    assert_eq!(configs[0].param.real_value(), "max_bytes_for_level_base");
                    assert_eq!(
                        configs[0].value,
                        SetVariableValue::Single(SetVariableValueSingle::Literal(Value::Number(
                            "1073741824".into()
                        )))
                    );
                }
            }
        }
        _ => panic!("unexpected statement kind"),
    }

    // Single group with multiple configs
    match verified_stmt(
        "ALTER COMPACTION GROUP 2 SET max_bytes_for_level_base = 1073741824, target_file_size_base = 67108864",
    ) {
        Statement::AlterCompactionGroup {
            group_ids,
            operation,
        } => {
            assert_eq!(group_ids, vec![2]);
            match operation {
                AlterCompactionGroupOperation::Set { configs } => {
                    assert_eq!(configs.len(), 2);
                    assert_eq!(configs[0].param.real_value(), "max_bytes_for_level_base");
                    assert_eq!(configs[1].param.real_value(), "target_file_size_base");
                }
            }
        }
        _ => panic!("unexpected statement kind"),
    }

    // Multiple groups
    match verified_stmt("ALTER COMPACTION GROUP 1, 2, 3 SET max_bytes_for_level_base = 1073741824")
    {
        Statement::AlterCompactionGroup {
            group_ids,
            operation,
        } => {
            assert_eq!(group_ids, vec![1, 2, 3]);
            match operation {
                AlterCompactionGroupOperation::Set { configs } => {
                    assert_eq!(configs.len(), 1);
                }
            }
        }
        _ => panic!("unexpected statement kind"),
    }

    // Boolean config
    match verified_stmt("ALTER COMPACTION GROUP 2 SET enable_emergency_picker = true") {
        Statement::AlterCompactionGroup {
            group_ids,
            operation,
        } => {
            assert_eq!(group_ids, vec![2]);
            match operation {
                AlterCompactionGroupOperation::Set { configs } => {
                    assert_eq!(configs.len(), 1);
                    assert_eq!(configs[0].param.real_value(), "enable_emergency_picker");
                    assert_eq!(
                        configs[0].value,
                        SetVariableValue::Single(SetVariableValueSingle::Literal(Value::Boolean(
                            true
                        )))
                    );
                }
            }
        }
        _ => panic!("unexpected statement kind"),
    }

    // String config
    match verified_stmt("ALTER COMPACTION GROUP 2 SET compression_algorithm = '6:lz4'") {
        Statement::AlterCompactionGroup {
            group_ids,
            operation,
        } => {
            assert_eq!(group_ids, vec![2]);
            match operation {
                AlterCompactionGroupOperation::Set { configs } => {
                    assert_eq!(configs.len(), 1);
                    assert_eq!(configs[0].param.real_value(), "compression_algorithm");
                    assert_eq!(
                        configs[0].value,
                        SetVariableValue::Single(SetVariableValueSingle::Literal(
                            Value::SingleQuotedString("6:lz4".into())
                        ))
                    );
                }
            }
        }
        _ => panic!("unexpected statement kind"),
    }

    // Boolean config using unquoted identifier literal
    match verified_stmt("ALTER COMPACTION GROUP 2 SET enable_emergency_picker = on") {
        Statement::AlterCompactionGroup {
            group_ids,
            operation,
        } => {
            assert_eq!(group_ids, vec![2]);
            match operation {
                AlterCompactionGroupOperation::Set { configs } => {
                    assert_eq!(configs.len(), 1);
                    assert_eq!(configs[0].param.real_value(), "enable_emergency_picker");
                    assert!(matches!(
                        configs[0].value,
                        SetVariableValue::Single(SetVariableValueSingle::Ident(ref ident))
                            if ident.real_value() == "on"
                    ));
                }
            }
        }
        _ => panic!("unexpected statement kind"),
    }
}
