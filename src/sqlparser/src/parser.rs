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

//! SQL Parser

use std::fmt;

use ddl::{AlterRateLimit, AlterRateLimitType, WebhookSourceInfo};
use itertools::Itertools;
use tracing::{debug, instrument};
use winnow::combinator::{
    alt, cut_err, dispatch, fail, opt, peek, preceded, repeat, separated, separated_pair,
};
use winnow::{ModalResult, Parser as _};

use crate::ast::*;
use crate::keywords::{self, Keyword};
use crate::parser_v2::{
    ParserExt as _, dollar_quoted_string, keyword, literal_i64, literal_u32, literal_u64,
    single_quoted_string,
};
use crate::tokenizer::*;
use crate::{impl_parse_to, parser_v2};

pub(crate) const UPSTREAM_SOURCE_KEY: &str = "connector";
pub(crate) const WEBHOOK_CONNECTOR: &str = "webhook";

const WEBHOOK_WAIT_FOR_PERSISTENCE: &str = "webhook.wait_for_persistence";
const WEBHOOK_IS_BATCHED: &str = "is_batched";

#[derive(Debug, Clone, PartialEq)]
pub enum ParserError {
    TokenizerError(String),
    ParserError(String),
}

impl ParserError {
    pub fn inner_msg(self) -> String {
        match self {
            ParserError::TokenizerError(s) | ParserError::ParserError(s) => s,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct StrError(pub String);

// Use `Parser::expected` instead, if possible
#[macro_export]
macro_rules! parser_err {
    ($($arg:tt)*) => {
        return Err(winnow::error::ErrMode::Backtrack(<winnow::error::ContextError as winnow::error::FromExternalError<_, _>>::from_external_error(
            &Parser::default(),
            $crate::parser::StrError(format!($($arg)*)),
        )))
    };
}

impl From<StrError> for winnow::error::ErrMode<winnow::error::ContextError> {
    fn from(e: StrError) -> Self {
        winnow::error::ErrMode::Backtrack(<winnow::error::ContextError as winnow::error::FromExternalError<_, _>>::from_external_error(
            &Parser::default(),
            e,
        ))
    }
}

// Returns a successful result if the optional expression is some
macro_rules! return_ok_if_some {
    ($e:expr) => {{
        if let Some(v) = $e {
            return Ok(v);
        }
    }};
}

#[derive(PartialEq)]
pub enum IsOptional {
    Optional,
    Mandatory,
}

use IsOptional::*;

pub enum IsLateral {
    Lateral,
    NotLateral,
}

use IsLateral::*;

use crate::ast::ddl::{AlterCompactionGroupOperation, AlterFragmentOperation};

pub type IncludeOption = Vec<IncludeOptionItem>;

#[derive(Eq, Clone, Debug, PartialEq, Hash)]
pub struct IncludeOptionItem {
    pub column_type: Ident,
    pub column_alias: Option<Ident>,
    pub inner_field: Option<String>,
    pub header_inner_expect_type: Option<DataType>,
}

#[derive(Debug)]
pub enum WildcardOrExpr {
    Expr(Expr),
    /// Expr is an arbitrary expression, returning either a table or a column.
    /// Idents are the prefix of `*`, which are consecutive field accesses.
    /// e.g. `(table.v1).*` or `(table).v1.*`
    ///
    /// See also [`Expr::FieldIdentifier`] for behaviors of parentheses.
    ExprQualifiedWildcard(Expr, Vec<Ident>),
    /// `QualifiedWildcard` and `Wildcard` can be followed by EXCEPT (columns)
    QualifiedWildcard(ObjectName, Option<Vec<Expr>>),
    Wildcard(Option<Vec<Expr>>),
}

impl From<WildcardOrExpr> for FunctionArgExpr {
    fn from(wildcard_expr: WildcardOrExpr) -> Self {
        match wildcard_expr {
            WildcardOrExpr::Expr(expr) => Self::Expr(expr),
            WildcardOrExpr::ExprQualifiedWildcard(expr, prefix) => {
                Self::ExprQualifiedWildcard(expr, prefix)
            }
            WildcardOrExpr::QualifiedWildcard(prefix, except) => {
                Self::QualifiedWildcard(prefix, except)
            }
            WildcardOrExpr::Wildcard(except) => Self::Wildcard(except),
        }
    }
}

impl From<TokenizerError> for ParserError {
    fn from(e: TokenizerError) -> Self {
        ParserError::TokenizerError(e.to_string())
    }
}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "sql parser error: {}",
            match self {
                ParserError::TokenizerError(s) => s,
                ParserError::ParserError(s) => s,
            }
        )
    }
}

impl std::error::Error for ParserError {}

type ColumnsDefTuple = (
    Vec<ColumnDef>,
    Vec<TableConstraint>,
    Vec<SourceWatermark>,
    Option<usize>,
);

/// Reference:
/// <https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-PRECEDENCE>
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Precedence {
    Zero = 0,
    LogicalOr, // 5 in upstream
    LogicalXor,
    LogicalAnd, // 10 in upstream
    UnaryNot,   // 15 in upstream
    Is,         // 17 in upstream
    Cmp,
    Like,    // 19 in upstream
    Between, // 20 in upstream
    Other,
    PlusMinus, // 30 in upstream
    MulDiv,    // 40 in upstream
    Exp,
    At,
    Collate,
    UnaryPosNeg,
    Array,
    DoubleColon, // 50 in upstream
}

#[derive(Clone, Copy, Default)]
pub struct Parser<'a>(pub(crate) &'a [TokenWithLocation]);

impl Parser<'_> {
    /// Parse a SQL statement and produce an Abstract Syntax Tree (AST)
    #[instrument(level = "debug")]
    pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParserError> {
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer.tokenize_with_location()?;
        let parser = Parser(&tokens);
        let stmts = Parser::parse_statements.parse(parser).map_err(|e| {
            // append SQL context to the error message, e.g.:
            // LINE 1: SELECT 1::int(2);
            let loc = match tokens.get(e.offset()) {
                Some(token) => token.location.clone(),
                None => {
                    // get location of EOF
                    Location {
                        line: sql.lines().count() as u64,
                        column: sql.lines().last().map_or(0, |l| l.len() as u64) + 1,
                    }
                }
            };
            let prefix = format!("LINE {}: ", loc.line);
            let sql_line = sql.split('\n').nth(loc.line as usize - 1).unwrap();
            let cursor = " ".repeat(prefix.len() + loc.column as usize - 1);
            ParserError::ParserError(format!(
                "{}\n{}{}\n{}^",
                e.inner().to_string().replace('\n', ": "),
                prefix,
                sql_line,
                cursor
            ))
        })?;
        Ok(stmts)
    }

    /// Parse exactly one statement from a string.
    pub fn parse_exactly_one(sql: &str) -> Result<Statement, ParserError> {
        Itertools::exactly_one(
            Parser::parse_sql(sql)
                .map_err(|e| {
                    ParserError::ParserError(format!("failed to parse definition sql: {}", e))
                })?
                .into_iter(),
        )
        .map_err(|e| {
            ParserError::ParserError(format!(
                "expecting exactly one statement in definition: {}",
                e
            ))
        })
    }

    /// Parse object name from a string.
    pub fn parse_object_name_str(s: &str) -> Result<ObjectName, ParserError> {
        let mut tokenizer = Tokenizer::new(s);
        let tokens = tokenizer.tokenize_with_location()?;
        let parser = Parser(&tokens);
        Parser::parse_object_name
            .parse(parser)
            .map_err(|e| ParserError::ParserError(e.inner().to_string()))
    }

    /// Parse function description from a string.
    pub fn parse_function_desc_str(func: &str) -> Result<FunctionDesc, ParserError> {
        let mut tokenizer = Tokenizer::new(func);
        let tokens = tokenizer.tokenize_with_location()?;
        let parser = Parser(&tokens);
        Parser::parse_function_desc
            .parse(parser)
            .map_err(|e| ParserError::ParserError(e.inner().to_string()))
    }

    /// Parse a list of semicolon-separated statements.
    fn parse_statements(&mut self) -> ModalResult<Vec<Statement>> {
        let mut stmts = Vec::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while self.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if self.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return self.expected("end of statement");
            }

            let statement = self.parse_statement()?;
            stmts.push(statement);
            expecting_statement_delimiter = true;
        }
        debug!("parsed statements:\n{:#?}", stmts);
        Ok(stmts)
    }

    /// Parse a single top-level statement (such as SELECT, INSERT, CREATE, etc.),
    /// stopping before the statement separator, if any.
    pub fn parse_statement(&mut self) -> ModalResult<Statement> {
        let checkpoint = *self;
        let token = self.next_token();
        match token.token {
            Token::Word(w) => match w.keyword {
                Keyword::EXPLAIN => Ok(self.parse_explain()?),
                Keyword::ANALYZE => Ok(self.parse_analyze()?),
                Keyword::SELECT | Keyword::WITH | Keyword::VALUES => {
                    *self = checkpoint;
                    Ok(Statement::Query(Box::new(self.parse_query()?)))
                }
                Keyword::DECLARE => Ok(self.parse_declare()?),
                Keyword::FETCH => Ok(self.parse_fetch_cursor()?),
                Keyword::CLOSE => Ok(self.parse_close_cursor()?),
                Keyword::TRUNCATE => Ok(self.parse_truncate()?),
                Keyword::REFRESH => Ok(self.parse_refresh()?),
                Keyword::CREATE => Ok(self.parse_create()?),
                Keyword::REPLACE => Ok(self.parse_replace()?),
                Keyword::DISCARD => Ok(self.parse_discard()?),
                Keyword::DROP => Ok(self.parse_drop()?),
                Keyword::DELETE => Ok(self.parse_delete()?),
                Keyword::INSERT => Ok(self.parse_insert()?),
                Keyword::UPDATE => Ok(self.parse_update()?),
                Keyword::ALTER => Ok(self.parse_alter()?),
                Keyword::COPY => Ok(self.parse_copy()?),
                Keyword::SET => Ok(self.parse_set()?),
                Keyword::SHOW => {
                    if self.parse_keyword(Keyword::CREATE) {
                        Ok(self.parse_show_create()?)
                    } else {
                        Ok(self.parse_show()?)
                    }
                }
                Keyword::CANCEL => Ok(self.parse_cancel_job()?),
                Keyword::KILL => Ok(self.parse_kill_process()?),
                Keyword::DESCRIBE => Ok(self.parse_describe()?),
                Keyword::GRANT => Ok(self.parse_grant()?),
                Keyword::REVOKE => Ok(self.parse_revoke()?),
                Keyword::START => Ok(self.parse_start_transaction()?),
                Keyword::ABORT => Ok(Statement::Abort),
                // `BEGIN` is a nonstandard but common alias for the
                // standard `START TRANSACTION` statement. It is supported
                // by at least PostgreSQL and MySQL.
                Keyword::BEGIN => Ok(self.parse_begin()?),
                Keyword::COMMIT => Ok(self.parse_commit()?),
                Keyword::ROLLBACK => Ok(self.parse_rollback()?),
                // `PREPARE`, `EXECUTE` and `DEALLOCATE` are Postgres-specific
                // syntaxes. They are used for Postgres prepared statement.
                Keyword::DEALLOCATE => Ok(self.parse_deallocate()?),
                Keyword::EXECUTE => Ok(self.parse_execute()?),
                Keyword::PREPARE => Ok(self.parse_prepare()?),
                Keyword::COMMENT => Ok(self.parse_comment()?),
                Keyword::FLUSH => Ok(Statement::Flush),
                Keyword::WAIT => Ok(self.parse_wait()?),
                Keyword::BACKUP => Ok(Statement::Backup),
                Keyword::RECOVER => Ok(Statement::Recover),
                Keyword::USE => Ok(self.parse_use()?),
                Keyword::VACUUM => Ok(self.parse_vacuum()?),
                _ => self.expected_at(checkpoint, "statement"),
            },
            Token::LParen => {
                *self = checkpoint;
                Ok(Statement::Query(Box::new(self.parse_query()?)))
            }
            _ => self.expected_at(checkpoint, "statement"),
        }
    }

    pub fn parse_truncate(&mut self) -> ModalResult<Statement> {
        let _ = self.parse_keyword(Keyword::TABLE);
        let table_name = self.parse_object_name()?;
        Ok(Statement::Truncate { table_name })
    }

    pub fn parse_refresh(&mut self) -> ModalResult<Statement> {
        self.expect_keyword(Keyword::TABLE)?;
        let table_name = self.parse_object_name()?;
        Ok(Statement::Refresh { table_name })
    }

    pub fn parse_analyze(&mut self) -> ModalResult<Statement> {
        let table_name = self.parse_object_name()?;

        Ok(Statement::Analyze { table_name })
    }

    pub fn parse_vacuum(&mut self) -> ModalResult<Statement> {
        let full = self.parse_keyword(Keyword::FULL);
        let object_name = self.parse_object_name()?;

        Ok(Statement::Vacuum { object_name, full })
    }

    /// Tries to parse a wildcard expression. If it is not a wildcard, parses an expression.
    ///
    /// A wildcard expression either means:
    /// - Selecting all fields from a struct. In this case, it is a
    ///   [`WildcardOrExpr::ExprQualifiedWildcard`]. Similar to [`Expr::FieldIdentifier`], It must
    ///   contain parentheses.
    /// - Selecting all columns from a table. In this case, it is a
    ///   [`WildcardOrExpr::QualifiedWildcard`] or a [`WildcardOrExpr::Wildcard`].
    pub fn parse_wildcard_or_expr(&mut self) -> ModalResult<WildcardOrExpr> {
        let checkpoint = *self;

        match self.next_token().token {
            Token::Word(w) if self.peek_token() == Token::Period => {
                // Since there's no parenthesis, `w` must be a column or a table
                // So what follows must be dot-delimited identifiers, e.g. `a.b.c.*`
                let wildcard_expr = self.parse_simple_wildcard_expr(checkpoint)?;
                return self.word_concat_wildcard_expr(w.to_ident()?, wildcard_expr);
            }
            Token::Mul => {
                return Ok(WildcardOrExpr::Wildcard(self.parse_except()?));
            }
            // parses wildcard field selection expression.
            // Code is similar to `parse_struct_selection`
            Token::LParen => {
                let mut expr = self.parse_expr()?;
                if self.consume_token(&Token::RParen) {
                    // Unwrap parentheses
                    while let Expr::Nested(inner) = expr {
                        expr = *inner;
                    }
                    // Now that we have an expr, what follows must be
                    // dot-delimited identifiers, e.g. `b.c.*` in `(a).b.c.*`
                    let wildcard_expr = self.parse_simple_wildcard_expr(checkpoint)?;
                    return self.expr_concat_wildcard_expr(expr, wildcard_expr);
                }
            }
            _ => (),
        };

        *self = checkpoint;
        self.parse_expr().map(WildcardOrExpr::Expr)
    }

    /// Concats `ident` and `wildcard_expr` in `ident.wildcard_expr`
    pub fn word_concat_wildcard_expr(
        &mut self,
        ident: Ident,
        simple_wildcard_expr: WildcardOrExpr,
    ) -> ModalResult<WildcardOrExpr> {
        let mut idents = vec![ident];
        let mut except_cols = vec![];
        match simple_wildcard_expr {
            WildcardOrExpr::QualifiedWildcard(ids, except) => {
                idents.extend(ids.0);
                if let Some(cols) = except {
                    except_cols = cols;
                }
            }
            WildcardOrExpr::Wildcard(except) => {
                if let Some(cols) = except {
                    except_cols = cols;
                }
            }
            WildcardOrExpr::ExprQualifiedWildcard(_, _) => unreachable!(),
            WildcardOrExpr::Expr(e) => return Ok(WildcardOrExpr::Expr(e)),
        }
        Ok(WildcardOrExpr::QualifiedWildcard(
            ObjectName(idents),
            if except_cols.is_empty() {
                None
            } else {
                Some(except_cols)
            },
        ))
    }

    /// Concats `expr` and `wildcard_expr` in `(expr).wildcard_expr`.
    pub fn expr_concat_wildcard_expr(
        &mut self,
        expr: Expr,
        simple_wildcard_expr: WildcardOrExpr,
    ) -> ModalResult<WildcardOrExpr> {
        if let WildcardOrExpr::Expr(e) = simple_wildcard_expr {
            return Ok(WildcardOrExpr::Expr(e));
        }

        // similar to `parse_struct_selection`
        let mut idents = vec![];
        let expr = match expr {
            // expr is `(foo)`
            Expr::Identifier(_) => expr,
            // expr is `(foo.v1)`
            Expr::CompoundIdentifier(_) => expr,
            // expr is `((1,2,3)::foo)`
            Expr::Cast { .. } => expr,
            // expr is `(func())`
            Expr::Function(_) => expr,
            // expr is `((foo.v1).v2)`
            Expr::FieldIdentifier(expr, ids) => {
                // Put `ids` to the latter part!
                idents.extend(ids);
                *expr
            }
            // expr is other things, e.g., `(1+2)`. It will become an unexpected period error at
            // upper level.
            _ => return Ok(WildcardOrExpr::Expr(expr)),
        };

        match simple_wildcard_expr {
            WildcardOrExpr::QualifiedWildcard(ids, except) => {
                if except.is_some() {
                    return self.expected("Expr quantified wildcard does not support except");
                }
                idents.extend(ids.0);
            }
            WildcardOrExpr::Wildcard(except) => {
                if except.is_some() {
                    return self.expected("Expr quantified wildcard does not support except");
                }
            }
            WildcardOrExpr::ExprQualifiedWildcard(_, _) => unreachable!(),
            WildcardOrExpr::Expr(_) => unreachable!(),
        }
        Ok(WildcardOrExpr::ExprQualifiedWildcard(expr, idents))
    }

    /// Tries to parses a wildcard expression without any parentheses.
    ///
    /// If wildcard is not found, go back to `index` and parse an expression.
    pub fn parse_simple_wildcard_expr(&mut self, checkpoint: Self) -> ModalResult<WildcardOrExpr> {
        let mut id_parts = vec![];
        while self.consume_token(&Token::Period) {
            let ckpt = *self;
            let token = self.next_token();
            match token.token {
                Token::Word(w) => id_parts.push(w.to_ident()?),
                Token::Mul => {
                    return if id_parts.is_empty() {
                        Ok(WildcardOrExpr::Wildcard(self.parse_except()?))
                    } else {
                        Ok(WildcardOrExpr::QualifiedWildcard(
                            ObjectName(id_parts),
                            self.parse_except()?,
                        ))
                    };
                }
                _ => {
                    *self = ckpt;
                    return self.expected("an identifier or a '*' after '.'");
                }
            }
        }
        *self = checkpoint;
        self.parse_expr().map(WildcardOrExpr::Expr)
    }

    pub fn parse_except(&mut self) -> ModalResult<Option<Vec<Expr>>> {
        if !self.parse_keyword(Keyword::EXCEPT) {
            return Ok(None);
        }
        if !self.consume_token(&Token::LParen) {
            return self.expected("EXCEPT should be followed by (");
        }
        let exprs = self.parse_comma_separated(Parser::parse_expr)?;
        if self.consume_token(&Token::RParen) {
            Ok(Some(exprs))
        } else {
            self.expected("( should be followed by ) after column names")
        }
    }

    /// Parse a new expression
    pub fn parse_expr(&mut self) -> ModalResult<Expr> {
        self.parse_subexpr(Precedence::Zero)
    }

    /// Parse tokens until the precedence changes
    pub fn parse_subexpr(&mut self, precedence: Precedence) -> ModalResult<Expr> {
        debug!("parsing expr, current token: {:?}", self.peek_token().token);
        let mut expr = self.parse_prefix()?;
        debug!("prefix: {:?}", expr);
        loop {
            let next_precedence = self.get_next_precedence()?;
            debug!("precedence: {precedence:?}, next precedence: {next_precedence:?}");

            if precedence >= next_precedence {
                break;
            }

            expr = self.parse_infix(expr, next_precedence)?;
        }
        Ok(expr)
    }

    /// Parse an expression prefix
    pub fn parse_prefix(&mut self) -> ModalResult<Expr> {
        // PostgreSQL allows any string literal to be preceded by a type name, indicating that the
        // string literal represents a literal of that type. Some examples:
        //
        //      DATE '2020-05-20'
        //      TIMESTAMP WITH TIME ZONE '2020-05-20 7:43:54'
        //      BOOL 'true'
        //
        // The first two are standard SQL, while the latter is a PostgreSQL extension. Complicating
        // matters is the fact that INTERVAL string literals may optionally be followed by special
        // keywords, e.g.:
        //
        //      INTERVAL '7' DAY
        //
        // Note also that naively `SELECT date` looks like a syntax error because the `date` type
        // name is not followed by a string literal, but in fact in PostgreSQL it is a valid
        // expression that should parse as the column name "date".
        return_ok_if_some!(self.maybe_parse(|parser| {
            match parser.parse_data_type()? {
                DataType::Interval => parser.parse_literal_interval(),
                // PostgreSQL allows almost any identifier to be used as custom data type name,
                // and we support that in `parse_data_type()`. But unlike Postgres we don't
                // have a list of globally reserved keywords (since they vary across dialects),
                // so given `NOT 'a' LIKE 'b'`, we'd accept `NOT` as a possible custom data type
                // name, resulting in `NOT 'a'` being recognized as a `TypedString` instead of
                // an unary negation `NOT ('a' LIKE 'b')`. To solve this, we don't accept the
                // `type 'string'` syntax for the custom data types at all.
                DataType::Custom(..) => parser_err!("dummy"),
                data_type => Ok(Expr::TypedString {
                    data_type,
                    value: parser.parse_literal_string()?,
                }),
            }
        }));

        let checkpoint = *self;
        let token = self.next_token();
        let expr = match token.token.clone() {
            Token::Word(w) => match w.keyword {
                Keyword::TRUE | Keyword::FALSE | Keyword::NULL => {
                    *self = checkpoint;
                    Ok(Expr::Value(self.ensure_parse_value()?))
                }
                Keyword::CASE => self.parse_case_expr(),
                Keyword::CAST => self.parse_cast_expr(),
                Keyword::TRY_CAST => self.parse_try_cast_expr(),
                Keyword::EXISTS => self.parse_exists_expr(),
                Keyword::EXTRACT => self.parse_extract_expr(),
                Keyword::SUBSTRING => self.parse_substring_expr(),
                Keyword::POSITION => self.parse_position_expr(),
                Keyword::OVERLAY => self.parse_overlay_expr(),
                Keyword::TRIM => self.parse_trim_expr(),
                Keyword::INTERVAL => self.parse_literal_interval(),
                Keyword::NOT => Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(self.parse_subexpr(Precedence::UnaryNot)?),
                }),
                Keyword::ROW => self.parse_row_expr(),
                Keyword::ARRAY if self.peek_token() == Token::LParen => {
                    // similar to `exists(subquery)`
                    self.expect_token(&Token::LParen)?;
                    let exists_node = Expr::ArraySubquery(Box::new(self.parse_query()?));
                    self.expect_token(&Token::RParen)?;
                    Ok(exists_node)
                }
                Keyword::ARRAY if self.peek_token() == Token::LBracket => self.parse_array_expr(),
                Keyword::MAP if self.peek_token() == Token::LBrace => self.parse_map_expr(),
                // `LEFT` and `RIGHT` are reserved as identifier but okay as function
                Keyword::LEFT | Keyword::RIGHT => {
                    *self = checkpoint;
                    self.parse_function()
                }
                Keyword::OPERATOR if self.peek_token().token == Token::LParen => {
                    let op = UnaryOperator::PGQualified(Box::new(self.parse_qualified_operator()?));
                    Ok(Expr::UnaryOp {
                        op,
                        expr: Box::new(self.parse_subexpr(Precedence::Other)?),
                    })
                }
                keyword @ (Keyword::ALL | Keyword::ANY | Keyword::SOME) => {
                    self.expect_token(&Token::LParen)?;
                    // In upstream's PR of parser-rs, there is `self.parser_subexpr(precedence)` here.
                    // But it will fail to parse `select 1 = any(null and true);`.
                    let sub = self.parse_expr()?;
                    self.expect_token(&Token::RParen)?;

                    // TODO: support `all/any/some(subquery)`.
                    if let Expr::Subquery(_) = &sub {
                        parser_err!("ANY/SOME/ALL(Subquery) is not implemented");
                    }

                    Ok(match keyword {
                        Keyword::ALL => Expr::AllOp(Box::new(sub)),
                        // `SOME` is a synonym for `ANY`.
                        Keyword::ANY | Keyword::SOME => Expr::SomeOp(Box::new(sub)),
                        _ => unreachable!(),
                    })
                }
                k if keywords::RESERVED_FOR_COLUMN_OR_TABLE_NAME.contains(&k) => {
                    parser_err!("syntax error at or near {token}")
                }
                Keyword::AGGREGATE => {
                    *self = checkpoint;
                    self.parse_function()
                }
                // Here `w` is a word, check if it's a part of a multi-part
                // identifier, a function call, or a simple identifier:
                _ => match self.peek_token().token {
                    Token::LParen | Token::Period => {
                        *self = checkpoint;
                        if let Ok(object_name) = self.parse_object_name()
                            && !matches!(self.peek_token().token, Token::LParen)
                        {
                            Ok(Expr::CompoundIdentifier(object_name.0))
                        } else {
                            *self = checkpoint;
                            self.parse_function()
                        }
                    }
                    _ => Ok(Expr::Identifier(w.to_ident()?)),
                },
            }, // End of Token::Word

            tok @ Token::Minus | tok @ Token::Plus => {
                let op = if tok == Token::Plus {
                    UnaryOperator::Plus
                } else {
                    UnaryOperator::Minus
                };
                let mut sub_expr = self.parse_subexpr(Precedence::UnaryPosNeg)?;
                if let Expr::Value(Value::Number(ref mut s)) = sub_expr {
                    if tok == Token::Minus {
                        *s = format!("-{}", s);
                    }
                    return Ok(sub_expr);
                }
                Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(sub_expr),
                })
            }
            Token::Op(name) => {
                let op = UnaryOperator::Custom(name);
                // Counter-intuitively, `|/ 4 + 12` means `|/ (4+12)` rather than `(|/4) + 12` in
                // PostgreSQL.
                Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(self.parse_subexpr(Precedence::Other)?),
                })
            }
            Token::Number(_)
            | Token::SingleQuotedString(_)
            | Token::DollarQuotedString(_)
            | Token::NationalStringLiteral(_)
            | Token::HexStringLiteral(_)
            | Token::CstyleEscapesString(_) => {
                *self = checkpoint;
                Ok(Expr::Value(self.ensure_parse_value()?))
            }
            Token::Parameter(number) => self.parse_param(number),
            Token::Pipe => {
                let args = self.parse_comma_separated(Parser::parse_identifier)?;
                self.expect_token(&Token::Pipe)?;
                let body = self.parse_expr()?;
                Ok(Expr::LambdaFunction {
                    args,
                    body: Box::new(body),
                })
            }
            Token::LParen => {
                let expr = if matches!(self.peek_token().token, Token::Word(w) if w.keyword == Keyword::SELECT || w.keyword == Keyword::WITH)
                {
                    Expr::Subquery(Box::new(self.parse_query()?))
                } else {
                    let mut exprs = self.parse_comma_separated(Parser::parse_expr)?;
                    if exprs.len() == 1 {
                        Expr::Nested(Box::new(exprs.pop().unwrap()))
                    } else {
                        Expr::Row(exprs)
                    }
                };
                self.expect_token(&Token::RParen)?;
                if self.peek_token() == Token::Period && matches!(expr, Expr::Nested(_)) {
                    self.parse_struct_selection(expr)
                } else {
                    Ok(expr)
                }
            }
            _ => self.expected_at(checkpoint, "an expression"),
        }?;

        if self.parse_keyword(Keyword::COLLATE) {
            Ok(Expr::Collate {
                expr: Box::new(expr),
                collation: self.parse_object_name()?,
            })
        } else {
            Ok(expr)
        }
    }

    fn parse_param(&mut self, param: String) -> ModalResult<Expr> {
        let Ok(index) = param.parse() else {
            parser_err!("Parameter symbol has a invalid index {}.", param);
        };
        Ok(Expr::Parameter { index })
    }

    /// Parses a field selection expression. See also [`Expr::FieldIdentifier`].
    pub fn parse_struct_selection(&mut self, expr: Expr) -> ModalResult<Expr> {
        let mut nested_expr = expr;
        // Unwrap parentheses
        while let Expr::Nested(inner) = nested_expr {
            nested_expr = *inner;
        }
        let fields = self.parse_fields()?;
        Ok(Expr::FieldIdentifier(Box::new(nested_expr), fields))
    }

    /// Parses consecutive field identifiers after a period. i.e., `.foo.bar.baz`
    pub fn parse_fields(&mut self) -> ModalResult<Vec<Ident>> {
        repeat(.., preceded(Token::Period, cut_err(Self::parse_identifier))).parse_next(self)
    }

    pub fn parse_qualified_operator(&mut self) -> ModalResult<QualifiedOperator> {
        self.expect_token(&Token::LParen)?;

        let checkpoint = *self;
        let schema = match self.parse_identifier_non_reserved() {
            Ok(ident) => {
                self.expect_token(&Token::Period)?;
                Some(ident)
            }
            Err(_) => {
                *self = checkpoint;
                None
            }
        };

        // https://www.postgresql.org/docs/15/sql-syntax-lexical.html#SQL-SYNTAX-OPERATORS
        const OP_CHARS: &[char] = &[
            '+', '-', '*', '/', '<', '>', '=', '~', '!', '@', '#', '%', '^', '&', '|', '`', '?',
        ];
        let name = {
            // Unlike PostgreSQL, we only take 1 token here rather than any sequence of `OP_CHARS`.
            // This is enough because we do not support custom operators like `x *@ y` anyways,
            // and all builtin sequences are already single tokens.
            //
            // To support custom operators and be fully compatible with PostgreSQL later, the
            // tokenizer should also be updated.
            let checkpoint = *self;
            let token = self.next_token();
            let name = token.token.to_string();
            if !name.trim_matches(OP_CHARS).is_empty() {
                return self
                    .expected_at(checkpoint, &format!("one of {}", OP_CHARS.iter().join(" ")));
            }
            name
        };

        self.expect_token(&Token::RParen)?;
        Ok(QualifiedOperator { schema, name })
    }

    /// Parse a function call.
    pub fn parse_function(&mut self) -> ModalResult<Expr> {
        // [aggregate:]
        let scalar_as_agg = if self.parse_keyword(Keyword::AGGREGATE) {
            self.expect_token(&Token::Colon)?;
            true
        } else {
            false
        };
        let name = self.parse_object_name()?;
        let arg_list = self.parse_argument_list()?;

        let within_group = if self.parse_keywords(&[Keyword::WITHIN, Keyword::GROUP]) {
            self.expect_token(&Token::LParen)?;
            self.expect_keywords(&[Keyword::ORDER, Keyword::BY])?;
            let order_by = self.parse_order_by_expr()?;
            self.expect_token(&Token::RParen)?;
            Some(Box::new(order_by))
        } else {
            None
        };

        let filter = if self.parse_keyword(Keyword::FILTER) {
            self.expect_token(&Token::LParen)?;
            self.expect_keyword(Keyword::WHERE)?;
            let filter_expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Some(Box::new(filter_expr))
        } else {
            None
        };

        let over = if self.parse_keyword(Keyword::OVER) {
            if self.peek_token() == Token::LParen {
                // Inline window specification: OVER (...)
                self.expect_token(&Token::LParen)?;
                let window_spec = self.parse_window_spec()?;
                self.expect_token(&Token::RParen)?;
                Some(Window::Spec(window_spec))
            } else {
                // Named window: OVER window_name
                let window_name = self.parse_identifier()?;
                Some(Window::Name(window_name))
            }
        } else {
            None
        };

        Ok(Expr::Function(Function {
            scalar_as_agg,
            name,
            arg_list,
            within_group,
            filter,
            over,
        }))
    }

    pub fn parse_window_frame_units(&mut self) -> ModalResult<WindowFrameUnits> {
        dispatch! { peek(keyword);
            Keyword::ROWS => keyword.value(WindowFrameUnits::Rows),
            Keyword::RANGE => keyword.value(WindowFrameUnits::Range),
            Keyword::GROUPS => keyword.value(WindowFrameUnits::Groups),
            Keyword::SESSION => keyword.value(WindowFrameUnits::Session),
            _ => fail,
        }
        .expect("ROWS, RANGE, or GROUPS")
        .parse_next(self)
    }

    pub fn parse_window_frame(&mut self) -> ModalResult<WindowFrame> {
        let units = self.parse_window_frame_units()?;
        let bounds = if self.parse_keyword(Keyword::BETWEEN) {
            // `BETWEEN <frame_start> AND <frame_end>`
            let start = self.parse_window_frame_bound()?;
            self.expect_keyword(Keyword::AND)?;
            let end = Some(self.parse_window_frame_bound()?);
            WindowFrameBounds::Bounds { start, end }
        } else if self.parse_keywords(&[Keyword::WITH, Keyword::GAP]) {
            // `WITH GAP <gap>`, only for session frames
            WindowFrameBounds::Gap(Box::new(self.parse_expr()?))
        } else {
            // `<frame_start>`
            WindowFrameBounds::Bounds {
                start: self.parse_window_frame_bound()?,
                end: None,
            }
        };
        let exclusion = if self.parse_keyword(Keyword::EXCLUDE) {
            Some(self.parse_window_frame_exclusion()?)
        } else {
            None
        };
        Ok(WindowFrame {
            units,
            bounds,
            exclusion,
        })
    }

    /// Parse `CURRENT ROW` or `{ <non-negative numeric | datetime | interval> | UNBOUNDED } { PRECEDING | FOLLOWING }`
    pub fn parse_window_frame_bound(&mut self) -> ModalResult<WindowFrameBound> {
        if self.parse_keywords(&[Keyword::CURRENT, Keyword::ROW]) {
            Ok(WindowFrameBound::CurrentRow)
        } else {
            let rows = if self.parse_keyword(Keyword::UNBOUNDED) {
                None
            } else {
                Some(Box::new(self.parse_expr()?))
            };
            if self.parse_keyword(Keyword::PRECEDING) {
                Ok(WindowFrameBound::Preceding(rows))
            } else if self.parse_keyword(Keyword::FOLLOWING) {
                Ok(WindowFrameBound::Following(rows))
            } else {
                self.expected("PRECEDING or FOLLOWING")
            }
        }
    }

    pub fn parse_window_frame_exclusion(&mut self) -> ModalResult<WindowFrameExclusion> {
        if self.parse_keywords(&[Keyword::CURRENT, Keyword::ROW]) {
            Ok(WindowFrameExclusion::CurrentRow)
        } else if self.parse_keyword(Keyword::GROUP) {
            Ok(WindowFrameExclusion::Group)
        } else if self.parse_keyword(Keyword::TIES) {
            Ok(WindowFrameExclusion::Ties)
        } else if self.parse_keywords(&[Keyword::NO, Keyword::OTHERS]) {
            Ok(WindowFrameExclusion::NoOthers)
        } else {
            self.expected("CURRENT ROW, GROUP, TIES, or NO OTHERS")
        }
    }

    /// parse a group by expr. a group by expr can be one of group sets, roll up, cube, or simple
    /// expr.
    fn parse_group_by_expr(&mut self) -> ModalResult<Expr> {
        if self.parse_keywords(&[Keyword::GROUPING, Keyword::SETS]) {
            self.expect_token(&Token::LParen)?;
            let result = self.parse_comma_separated(|p| p.parse_tuple(true, true))?;
            self.expect_token(&Token::RParen)?;
            Ok(Expr::GroupingSets(result))
        } else if self.parse_keyword(Keyword::CUBE) {
            self.expect_token(&Token::LParen)?;
            let result = self.parse_comma_separated(|p| p.parse_tuple(true, false))?;
            self.expect_token(&Token::RParen)?;
            Ok(Expr::Cube(result))
        } else if self.parse_keyword(Keyword::ROLLUP) {
            self.expect_token(&Token::LParen)?;
            let result = self.parse_comma_separated(|p| p.parse_tuple(true, false))?;
            self.expect_token(&Token::RParen)?;
            Ok(Expr::Rollup(result))
        } else {
            self.parse_expr()
        }
    }

    /// parse a tuple with `(` and `)`.
    /// If `lift_singleton` is true, then a singleton tuple is lifted to a tuple of length 1,
    /// otherwise it will fail. If `allow_empty` is true, then an empty tuple is allowed.
    fn parse_tuple(&mut self, lift_singleton: bool, allow_empty: bool) -> ModalResult<Vec<Expr>> {
        if lift_singleton {
            if self.consume_token(&Token::LParen) {
                let result = if allow_empty && self.consume_token(&Token::RParen) {
                    vec![]
                } else {
                    let result = self.parse_comma_separated(Parser::parse_expr)?;
                    self.expect_token(&Token::RParen)?;
                    result
                };
                Ok(result)
            } else {
                Ok(vec![self.parse_expr()?])
            }
        } else {
            self.expect_token(&Token::LParen)?;
            let result = if allow_empty && self.consume_token(&Token::RParen) {
                vec![]
            } else {
                let result = self.parse_comma_separated(Parser::parse_expr)?;
                self.expect_token(&Token::RParen)?;
                result
            };
            Ok(result)
        }
    }

    pub fn parse_case_expr(&mut self) -> ModalResult<Expr> {
        parser_v2::expr_case(self)
    }

    /// Parse a SQL CAST function e.g. `CAST(expr AS FLOAT)`
    pub fn parse_cast_expr(&mut self) -> ModalResult<Expr> {
        parser_v2::expr_cast(self)
    }

    /// Parse a SQL TRY_CAST function e.g. `TRY_CAST(expr AS FLOAT)`
    pub fn parse_try_cast_expr(&mut self) -> ModalResult<Expr> {
        parser_v2::expr_try_cast(self)
    }

    /// Parse a SQL EXISTS expression e.g. `WHERE EXISTS(SELECT ...)`.
    pub fn parse_exists_expr(&mut self) -> ModalResult<Expr> {
        self.expect_token(&Token::LParen)?;
        let exists_node = Expr::Exists(Box::new(self.parse_query()?));
        self.expect_token(&Token::RParen)?;
        Ok(exists_node)
    }

    pub fn parse_extract_expr(&mut self) -> ModalResult<Expr> {
        parser_v2::expr_extract(self)
    }

    pub fn parse_substring_expr(&mut self) -> ModalResult<Expr> {
        parser_v2::expr_substring(self)
    }

    /// `POSITION(<expr> IN <expr>)`
    pub fn parse_position_expr(&mut self) -> ModalResult<Expr> {
        parser_v2::expr_position(self)
    }

    /// `OVERLAY(<expr> PLACING <expr> FROM <expr> [ FOR <expr> ])`
    pub fn parse_overlay_expr(&mut self) -> ModalResult<Expr> {
        parser_v2::expr_overlay(self)
    }

    /// `TRIM ([WHERE] ['text'] FROM 'text')`\
    /// `TRIM ([WHERE] [FROM] 'text' [, 'text'])`
    pub fn parse_trim_expr(&mut self) -> ModalResult<Expr> {
        self.expect_token(&Token::LParen)?;
        let mut trim_where = None;
        if let Token::Word(word) = self.peek_token().token
            && [Keyword::BOTH, Keyword::LEADING, Keyword::TRAILING].contains(&word.keyword)
        {
            trim_where = Some(self.parse_trim_where()?);
        }

        let (mut trim_what, expr) = if self.parse_keyword(Keyword::FROM) {
            (None, self.parse_expr()?)
        } else {
            let mut expr = self.parse_expr()?;
            if self.parse_keyword(Keyword::FROM) {
                let trim_what = std::mem::replace(&mut expr, self.parse_expr()?);
                (Some(Box::new(trim_what)), expr)
            } else {
                (None, expr)
            }
        };

        if trim_what.is_none() && self.consume_token(&Token::Comma) {
            trim_what = Some(Box::new(self.parse_expr()?));
        }
        self.expect_token(&Token::RParen)?;

        Ok(Expr::Trim {
            expr: Box::new(expr),
            trim_where,
            trim_what,
        })
    }

    pub fn parse_trim_where(&mut self) -> ModalResult<TrimWhereField> {
        dispatch! { peek(keyword);
            Keyword::BOTH => keyword.value(TrimWhereField::Both),
            Keyword::LEADING => keyword.value(TrimWhereField::Leading),
            Keyword::TRAILING => keyword.value(TrimWhereField::Trailing),
            _ => fail
        }
        .expect("BOTH, LEADING, or TRAILING")
        .parse_next(self)
    }

    /// Parses an array expression `[ex1, ex2, ..]`
    pub fn parse_array_expr(&mut self) -> ModalResult<Expr> {
        let mut expected_depth = None;
        let exprs = self.parse_array_inner(0, &mut expected_depth)?;
        Ok(Expr::Array(Array {
            elem: exprs,
            // Top-level array is named.
            named: true,
        }))
    }

    fn parse_array_inner(
        &mut self,
        depth: usize,
        expected_depth: &mut Option<usize>,
    ) -> ModalResult<Vec<Expr>> {
        self.expect_token(&Token::LBracket)?;
        if let Some(expected_depth) = *expected_depth
            && depth > expected_depth
        {
            return self.expected("]");
        }
        let exprs = if self.peek_token() == Token::LBracket {
            self.parse_comma_separated(|parser| {
                let exprs = parser.parse_array_inner(depth + 1, expected_depth)?;
                Ok(Expr::Array(Array {
                    elem: exprs,
                    named: false,
                }))
            })?
        } else {
            if let Some(expected_depth) = *expected_depth {
                if depth < expected_depth {
                    return self.expected("[");
                }
            } else {
                *expected_depth = Some(depth);
            }
            if self.consume_token(&Token::RBracket) {
                return Ok(vec![]);
            }
            self.parse_comma_separated(Self::parse_expr)?
        };
        self.expect_token(&Token::RBracket)?;
        Ok(exprs)
    }

    /// Parses a map expression `MAP {k1:v1, k2:v2, ..}`
    pub fn parse_map_expr(&mut self) -> ModalResult<Expr> {
        self.expect_token(&Token::LBrace)?;
        if self.consume_token(&Token::RBrace) {
            return Ok(Expr::Map { entries: vec![] });
        }
        let entries = self.parse_comma_separated(|parser| {
            let key = parser.parse_expr()?;
            parser.expect_token(&Token::Colon)?;
            let value = parser.parse_expr()?;
            Ok((key, value))
        })?;
        self.expect_token(&Token::RBrace)?;
        Ok(Expr::Map { entries })
    }

    // This function parses date/time fields for interval qualifiers.
    pub fn parse_date_time_field(&mut self) -> ModalResult<DateTimeField> {
        dispatch! { peek(keyword);
            Keyword::YEAR => keyword.value(DateTimeField::Year),
            Keyword::MONTH => keyword.value(DateTimeField::Month),
            Keyword::DAY => keyword.value(DateTimeField::Day),
            Keyword::HOUR => keyword.value(DateTimeField::Hour),
            Keyword::MINUTE => keyword.value(DateTimeField::Minute),
            Keyword::SECOND => keyword.value(DateTimeField::Second),
            _ => fail,
        }
        .expect("date/time field")
        .parse_next(self)
    }

    // This function parses date/time fields for the EXTRACT function-like operator. PostgreSQL
    // allows arbitrary inputs including invalid ones.
    //
    // ```
    //   select extract(day from null::date);
    //   select extract(invalid from null::date);
    //   select extract("invaLId" from null::date);
    //   select extract('invaLId' from null::date);
    // ```
    pub fn parse_date_time_field_in_extract(&mut self) -> ModalResult<String> {
        let checkpoint = *self;
        let token = self.next_token();
        match token.token {
            Token::Word(w) => Ok(w.value.to_uppercase()),
            Token::SingleQuotedString(s) => Ok(s.to_uppercase()),
            _ => {
                *self = checkpoint;
                self.expected("date/time field")
            }
        }
    }

    /// Parse an INTERVAL literal.
    ///
    /// Some syntactically valid intervals:
    ///
    ///   1. `INTERVAL '1' DAY`
    ///   2. `INTERVAL '1-1' YEAR TO MONTH`
    ///   3. `INTERVAL '1' SECOND`
    ///   4. `INTERVAL '1:1:1.1' HOUR (5) TO SECOND (5)`
    ///   5. `INTERVAL '1.1' SECOND (2, 2)`
    ///   6. `INTERVAL '1:1' HOUR (5) TO MINUTE (5)`
    ///
    /// Note that we do not currently attempt to parse the quoted value.
    pub fn parse_literal_interval(&mut self) -> ModalResult<Expr> {
        // The SQL standard allows an optional sign before the value string, but
        // it is not clear if any implementations support that syntax, so we
        // don't currently try to parse it. (The sign can instead be included
        // inside the value string.)

        // The first token in an interval is a string literal which specifies
        // the duration of the interval.
        let value = self.parse_literal_string()?;

        // Following the string literal is a qualifier which indicates the units
        // of the duration specified in the string literal.
        //
        // Note that PostgreSQL allows omitting the qualifier, so we provide
        // this more general implementation.
        let leading_field = match self.peek_token().token {
            Token::Word(kw)
                if [
                    Keyword::YEAR,
                    Keyword::MONTH,
                    Keyword::DAY,
                    Keyword::HOUR,
                    Keyword::MINUTE,
                    Keyword::SECOND,
                ]
                .contains(&kw.keyword) =>
            {
                Some(self.parse_date_time_field()?)
            }
            _ => None,
        };

        let (leading_precision, last_field, fsec_precision) =
            if leading_field == Some(DateTimeField::Second) {
                // SQL mandates special syntax for `SECOND TO SECOND` literals.
                // Instead of
                //     `SECOND [(<leading precision>)] TO SECOND[(<fractional seconds precision>)]`
                // one must use the special format:
                //     `SECOND [( <leading precision> [ , <fractional seconds precision>] )]`
                let last_field = None;
                let (leading_precision, fsec_precision) = self.parse_optional_precision_scale()?;
                (leading_precision, last_field, fsec_precision)
            } else {
                let leading_precision = self.parse_optional_precision()?;
                if self.parse_keyword(Keyword::TO) {
                    let last_field = Some(self.parse_date_time_field()?);
                    let fsec_precision = if last_field == Some(DateTimeField::Second) {
                        self.parse_optional_precision()?
                    } else {
                        None
                    };
                    (leading_precision, last_field, fsec_precision)
                } else {
                    (leading_precision, None, None)
                }
            };

        Ok(Expr::Value(Value::Interval {
            value,
            leading_field,
            leading_precision,
            last_field,
            fractional_seconds_precision: fsec_precision,
        }))
    }

    /// Parse an operator following an expression
    pub fn parse_infix(&mut self, expr: Expr, precedence: Precedence) -> ModalResult<Expr> {
        let checkpoint = *self;
        let tok = self.next_token();
        debug!("parsing infix {:?}", tok.token);
        let regular_binary_operator = match &tok.token {
            Token::Eq => Some(BinaryOperator::Eq),
            Token::Neq => Some(BinaryOperator::NotEq),
            Token::Gt => Some(BinaryOperator::Gt),
            Token::GtEq => Some(BinaryOperator::GtEq),
            Token::Lt => Some(BinaryOperator::Lt),
            Token::LtEq => Some(BinaryOperator::LtEq),
            Token::Plus => Some(BinaryOperator::Plus),
            Token::Minus => Some(BinaryOperator::Minus),
            Token::Mul => Some(BinaryOperator::Multiply),
            Token::Mod => Some(BinaryOperator::Modulo),
            Token::Pipe => Some(BinaryOperator::Custom("|".to_owned())),
            Token::Caret => Some(BinaryOperator::Pow),
            Token::Div => Some(BinaryOperator::Divide),
            Token::Op(name) => Some(BinaryOperator::Custom(name.clone())),
            Token::Word(w) => match w.keyword {
                Keyword::AND => Some(BinaryOperator::And),
                Keyword::OR => Some(BinaryOperator::Or),
                Keyword::XOR => Some(BinaryOperator::Xor),
                Keyword::OPERATOR if self.peek_token() == Token::LParen => Some(
                    BinaryOperator::PGQualified(Box::new(self.parse_qualified_operator()?)),
                ),
                _ => None,
            },
            _ => None,
        };

        if let Some(op) = regular_binary_operator {
            // // `all/any/some` only appears to the right of the binary op.
            // if let Some(keyword) =
            //     self.parse_one_of_keywords(&[Keyword::ANY, Keyword::ALL, Keyword::SOME])
            // {
            //     self.expect_token(&Token::LParen)?;
            //     // In upstream's PR of parser-rs, there is `self.parser_subexpr(precedence)` here.
            //     // But it will fail to parse `select 1 = any(null and true);`.
            //     let right = self.parse_expr()?;
            //     self.expect_token(&Token::RParen)?;

            //     // TODO: support `all/any/some(subquery)`.
            //     if let Expr::Subquery(_) = &right {
            //         parser_err!("ANY/SOME/ALL(Subquery) is not implemented");
            //     }

            //     let right = match keyword {
            //         Keyword::ALL => Box::new(Expr::AllOp(Box::new(right))),
            //         // `SOME` is a synonym for `ANY`.
            //         Keyword::ANY | Keyword::SOME => Box::new(Expr::SomeOp(Box::new(right))),
            //         _ => unreachable!(),
            //     };

            //     Ok(Expr::BinaryOp {
            //         left: Box::new(expr),
            //         op,
            //         right,
            //     })
            // } else {
            Ok(Expr::BinaryOp {
                left: Box::new(expr),
                op,
                right: Box::new(self.parse_subexpr(precedence)?),
            })
            // }
        } else if let Token::Word(w) = &tok.token {
            match w.keyword {
                Keyword::IS => {
                    if self.parse_keyword(Keyword::TRUE) {
                        Ok(Expr::IsTrue(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::TRUE]) {
                        Ok(Expr::IsNotTrue(Box::new(expr)))
                    } else if self.parse_keyword(Keyword::FALSE) {
                        Ok(Expr::IsFalse(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::FALSE]) {
                        Ok(Expr::IsNotFalse(Box::new(expr)))
                    } else if self.parse_keyword(Keyword::UNKNOWN) {
                        Ok(Expr::IsUnknown(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::UNKNOWN]) {
                        Ok(Expr::IsNotUnknown(Box::new(expr)))
                    } else if self.parse_keyword(Keyword::NULL) {
                        Ok(Expr::IsNull(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
                        Ok(Expr::IsNotNull(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::DISTINCT, Keyword::FROM]) {
                        let expr2 = self.parse_expr()?;
                        Ok(Expr::IsDistinctFrom(Box::new(expr), Box::new(expr2)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::DISTINCT, Keyword::FROM])
                    {
                        let expr2 = self.parse_expr()?;
                        Ok(Expr::IsNotDistinctFrom(Box::new(expr), Box::new(expr2)))
                    } else {
                        let negated = self.parse_keyword(Keyword::NOT);

                        if self.parse_keyword(Keyword::JSON) {
                            self.parse_is_json(expr, negated)
                        } else {
                            self.expected(
                                "[NOT] { TRUE | FALSE | UNKNOWN | NULL | DISTINCT FROM | JSON } after IS",
                            )
                        }
                    }
                }
                Keyword::AT => {
                    assert_eq!(precedence, Precedence::At);
                    let time_zone = Box::new(
                        preceded(
                            (Keyword::TIME, Keyword::ZONE),
                            cut_err(|p: &mut Self| p.parse_subexpr(precedence)),
                        )
                        .parse_next(self)?,
                    );
                    Ok(Expr::AtTimeZone {
                        timestamp: Box::new(expr),
                        time_zone,
                    })
                }
                keyword @ (Keyword::ALL | Keyword::ANY | Keyword::SOME) => {
                    self.expect_token(&Token::LParen)?;
                    // In upstream's PR of parser-rs, there is `self.parser_subexpr(precedence)` here.
                    // But it will fail to parse `select 1 = any(null and true);`.
                    let sub = self.parse_expr()?;
                    self.expect_token(&Token::RParen)?;

                    // TODO: support `all/any/some(subquery)`.
                    if let Expr::Subquery(_) = &sub {
                        parser_err!("ANY/SOME/ALL(Subquery) is not implemented");
                    }

                    Ok(match keyword {
                        Keyword::ALL => Expr::AllOp(Box::new(sub)),
                        // `SOME` is a synonym for `ANY`.
                        Keyword::ANY | Keyword::SOME => Expr::SomeOp(Box::new(sub)),
                        _ => unreachable!(),
                    })
                }
                Keyword::NOT
                | Keyword::IN
                | Keyword::BETWEEN
                | Keyword::LIKE
                | Keyword::ILIKE
                | Keyword::SIMILAR => {
                    *self = checkpoint;
                    let negated = self.parse_keyword(Keyword::NOT);
                    if self.parse_keyword(Keyword::IN) {
                        self.parse_in(expr, negated)
                    } else if self.parse_keyword(Keyword::BETWEEN) {
                        self.parse_between(expr, negated)
                    } else if self.parse_keyword(Keyword::LIKE) {
                        Ok(Expr::Like {
                            negated,
                            expr: Box::new(expr),
                            pattern: Box::new(self.parse_subexpr(Precedence::Like)?),
                            escape_char: self.parse_escape()?,
                        })
                    } else if self.parse_keyword(Keyword::ILIKE) {
                        Ok(Expr::ILike {
                            negated,
                            expr: Box::new(expr),
                            pattern: Box::new(self.parse_subexpr(Precedence::Like)?),
                            escape_char: self.parse_escape()?,
                        })
                    } else if self.parse_keywords(&[Keyword::SIMILAR, Keyword::TO]) {
                        Ok(Expr::SimilarTo {
                            negated,
                            expr: Box::new(expr),
                            pattern: Box::new(self.parse_subexpr(Precedence::Like)?),
                            escape_char: self.parse_escape()?,
                        })
                    } else {
                        self.expected("IN, BETWEEN or SIMILAR TO after NOT")
                    }
                }
                // Can only happen if `get_next_precedence` got out of sync with this function
                _ => parser_err!("No infix parser for token {:?}", tok),
            }
        } else if Token::DoubleColon == tok {
            self.parse_pg_cast(expr)
        } else if Token::LBracket == tok {
            self.parse_array_index(expr)
        } else {
            // Can only happen if `get_next_precedence` got out of sync with this function
            parser_err!("No infix parser for token {:?}", tok)
        }
    }

    /// parse the ESCAPE CHAR portion of LIKE, ILIKE, and SIMILAR TO
    pub fn parse_escape(&mut self) -> ModalResult<Option<EscapeChar>> {
        if self.parse_keyword(Keyword::ESCAPE) {
            let s = self.parse_literal_string()?;
            let mut chs = s.chars();
            if let Some(ch) = chs.next() {
                if chs.next().is_some() {
                    parser_err!("Escape string must be empty or one character, found {s:?}")
                } else {
                    Ok(Some(EscapeChar::escape(ch)))
                }
            } else {
                Ok(Some(EscapeChar::empty()))
            }
        } else {
            Ok(None)
        }
    }

    /// We parse both `array[1,9][1]`, `array[1,9][1:2]`, `array[1,9][:2]`, `array[1,9][1:]` and
    /// `array[1,9][:]` in this function.
    pub fn parse_array_index(&mut self, expr: Expr) -> ModalResult<Expr> {
        let new_expr = match self.peek_token().token {
            Token::Colon => {
                // [:] or [:N]
                assert!(self.consume_token(&Token::Colon));
                let end = match self.peek_token().token {
                    Token::RBracket => None,
                    _ => {
                        let end_index = Box::new(self.parse_expr()?);
                        Some(end_index)
                    }
                };
                Expr::ArrayRangeIndex {
                    obj: Box::new(expr),
                    start: None,
                    end,
                }
            }
            _ => {
                // [N], [N:], [N:M]
                let index = Box::new(self.parse_expr()?);
                match self.peek_token().token {
                    Token::Colon => {
                        // [N:], [N:M]
                        assert!(self.consume_token(&Token::Colon));
                        match self.peek_token().token {
                            Token::RBracket => {
                                // [N:]
                                Expr::ArrayRangeIndex {
                                    obj: Box::new(expr),
                                    start: Some(index),
                                    end: None,
                                }
                            }
                            _ => {
                                // [N:M]
                                let end = Some(Box::new(self.parse_expr()?));
                                Expr::ArrayRangeIndex {
                                    obj: Box::new(expr),
                                    start: Some(index),
                                    end,
                                }
                            }
                        }
                    }
                    _ => {
                        // [N]
                        Expr::Index {
                            obj: Box::new(expr),
                            index,
                        }
                    }
                }
            }
        };
        self.expect_token(&Token::RBracket)?;
        // recursively checking for more indices
        if self.consume_token(&Token::LBracket) {
            self.parse_array_index(new_expr)
        } else {
            Ok(new_expr)
        }
    }

    /// Parses the optional constraints following the `IS [NOT] JSON` predicate
    pub fn parse_is_json(&mut self, expr: Expr, negated: bool) -> ModalResult<Expr> {
        let item_type = match self.peek_token().token {
            Token::Word(w) => match w.keyword {
                Keyword::VALUE => Some(JsonPredicateType::Value),
                Keyword::ARRAY => Some(JsonPredicateType::Array),
                Keyword::OBJECT => Some(JsonPredicateType::Object),
                Keyword::SCALAR => Some(JsonPredicateType::Scalar),
                _ => None,
            },
            _ => None,
        };
        if item_type.is_some() {
            self.next_token();
        }
        let item_type = item_type.unwrap_or_default();

        let unique_keys = self.parse_one_of_keywords(&[Keyword::WITH, Keyword::WITHOUT]);
        if unique_keys.is_some() {
            self.expect_keyword(Keyword::UNIQUE)?;
            _ = self.parse_keyword(Keyword::KEYS);
        }
        let unique_keys = unique_keys.is_some_and(|w| w == Keyword::WITH);

        Ok(Expr::IsJson {
            expr: Box::new(expr),
            negated,
            item_type,
            unique_keys,
        })
    }

    /// Parses the parens following the `[ NOT ] IN` operator
    pub fn parse_in(&mut self, expr: Expr, negated: bool) -> ModalResult<Expr> {
        self.expect_token(&Token::LParen)?;
        let in_op = if matches!(self.peek_token().token, Token::Word(w) if w.keyword == Keyword::SELECT || w.keyword == Keyword::WITH)
        {
            Expr::InSubquery {
                expr: Box::new(expr),
                subquery: Box::new(self.parse_query()?),
                negated,
            }
        } else {
            Expr::InList {
                expr: Box::new(expr),
                list: self.parse_comma_separated(Parser::parse_expr)?,
                negated,
            }
        };
        self.expect_token(&Token::RParen)?;
        Ok(in_op)
    }

    /// Parses `BETWEEN <low> AND <high>`, assuming the `BETWEEN` keyword was already consumed
    pub fn parse_between(&mut self, expr: Expr, negated: bool) -> ModalResult<Expr> {
        // Stop parsing subexpressions for <low> and <high> on tokens with
        // precedence lower than that of `BETWEEN`, such as `AND`, `IS`, etc.
        let low = self.parse_subexpr(Precedence::Between)?;
        self.expect_keyword(Keyword::AND)?;
        let high = self.parse_subexpr(Precedence::Between)?;
        Ok(Expr::Between {
            expr: Box::new(expr),
            negated,
            low: Box::new(low),
            high: Box::new(high),
        })
    }

    /// Parse a postgresql casting style which is in the form of `expr::datatype`
    pub fn parse_pg_cast(&mut self, expr: Expr) -> ModalResult<Expr> {
        Ok(Expr::Cast {
            expr: Box::new(expr),
            data_type: self.parse_data_type()?,
        })
    }

    /// Get the precedence of the next token
    pub fn get_next_precedence(&self) -> ModalResult<Precedence> {
        use Precedence as P;

        let token = self.peek_token();
        debug!("get_next_precedence() {:?}", token);
        match token.token {
            Token::Word(w) if w.keyword == Keyword::OR => Ok(P::LogicalOr),
            Token::Word(w) if w.keyword == Keyword::XOR => Ok(P::LogicalXor),
            Token::Word(w) if w.keyword == Keyword::AND => Ok(P::LogicalAnd),
            Token::Word(w) if w.keyword == Keyword::AT => {
                match (self.peek_nth_token(1).token, self.peek_nth_token(2).token) {
                    (Token::Word(w), Token::Word(w2))
                        if w.keyword == Keyword::TIME && w2.keyword == Keyword::ZONE =>
                    {
                        Ok(P::At)
                    }
                    _ => Ok(P::Zero),
                }
            }

            Token::Word(w) if w.keyword == Keyword::NOT => match self.peek_nth_token(1).token {
                // The precedence of NOT varies depending on keyword that
                // follows it. If it is followed by IN, BETWEEN, or LIKE,
                // it takes on the precedence of those tokens. Otherwise it
                // is not an infix operator, and therefore has zero
                // precedence.
                Token::Word(w) if w.keyword == Keyword::BETWEEN => Ok(P::Between),
                Token::Word(w) if w.keyword == Keyword::IN => Ok(P::Between),
                Token::Word(w) if w.keyword == Keyword::LIKE => Ok(P::Like),
                Token::Word(w) if w.keyword == Keyword::ILIKE => Ok(P::Like),
                Token::Word(w) if w.keyword == Keyword::SIMILAR => Ok(P::Like),
                _ => Ok(P::Zero),
            },

            Token::Word(w) if w.keyword == Keyword::IS => Ok(P::Is),
            Token::Word(w) if w.keyword == Keyword::ISNULL => Ok(P::Is),
            Token::Word(w) if w.keyword == Keyword::NOTNULL => Ok(P::Is),
            Token::Eq | Token::Lt | Token::LtEq | Token::Neq | Token::Gt | Token::GtEq => {
                Ok(P::Cmp)
            }
            Token::Word(w) if w.keyword == Keyword::IN => Ok(P::Between),
            Token::Word(w) if w.keyword == Keyword::BETWEEN => Ok(P::Between),
            Token::Word(w) if w.keyword == Keyword::LIKE => Ok(P::Like),
            Token::Word(w) if w.keyword == Keyword::ILIKE => Ok(P::Like),
            Token::Word(w) if w.keyword == Keyword::SIMILAR => Ok(P::Like),
            Token::Word(w) if w.keyword == Keyword::ALL => Ok(P::Other),
            Token::Word(w) if w.keyword == Keyword::ANY => Ok(P::Other),
            Token::Word(w) if w.keyword == Keyword::SOME => Ok(P::Other),
            Token::Op(_) => Ok(P::Other),
            Token::Word(w)
                if w.keyword == Keyword::OPERATOR && self.peek_nth_token(1) == Token::LParen =>
            {
                Ok(P::Other)
            }
            // In some languages (incl. rust, c), bitwise operators have precedence:
            //   or < xor < and < shift
            // But in PostgreSQL, they are just left to right. So `2 | 3 & 4` is 0.
            Token::Pipe => Ok(P::Other),
            Token::Plus | Token::Minus => Ok(P::PlusMinus),
            Token::Mul | Token::Div | Token::Mod => Ok(P::MulDiv),
            Token::Caret => Ok(P::Exp),
            Token::LBracket => Ok(P::Array),
            Token::DoubleColon => Ok(P::DoubleColon),
            _ => Ok(P::Zero),
        }
    }

    /// Return the first non-whitespace token that has not yet been processed
    /// (or None if reached end-of-file)
    pub fn peek_token(&self) -> TokenWithLocation {
        self.peek_nth_token(0)
    }

    /// Return nth non-whitespace token that has not yet been processed
    pub fn peek_nth_token(&self, mut n: usize) -> TokenWithLocation {
        let mut index = 0;
        loop {
            let token = self.0.get(index);
            index += 1;
            match token.map(|x| &x.token) {
                Some(Token::Whitespace(_)) => continue,
                _ => {
                    if n == 0 {
                        return token.cloned().unwrap_or(TokenWithLocation::eof());
                    }
                    n -= 1;
                }
            }
        }
    }

    /// Return the first non-whitespace token that has not yet been processed
    /// (or None if reached end-of-file) and mark it as processed. OK to call
    /// repeatedly after reaching EOF.
    pub fn next_token(&mut self) -> TokenWithLocation {
        loop {
            let Some(token) = self.0.first() else {
                return TokenWithLocation::eof();
            };
            self.0 = &self.0[1..];
            match token.token {
                Token::Whitespace(_) => continue,
                _ => return token.clone(),
            }
        }
    }

    /// Return the first unprocessed token, possibly whitespace.
    pub fn next_token_no_skip(&mut self) -> Option<&TokenWithLocation> {
        if self.0.is_empty() {
            None
        } else {
            let (first, rest) = self.0.split_at(1);
            self.0 = rest;
            Some(&first[0])
        }
    }

    /// Report an expected error at the current position.
    pub fn expected<T>(&self, expected: &str) -> ModalResult<T> {
        parser_err!("expected {}, found: {}", expected, self.peek_token().token)
    }

    /// Revert the parser to a previous position and report an expected error.
    pub fn expected_at<T>(&mut self, checkpoint: Self, expected: &str) -> ModalResult<T> {
        *self = checkpoint;
        self.expected(expected)
    }

    /// Check if the expected match is the next token.
    /// The equality check is case-insensitive.
    pub fn parse_word(&mut self, expected: &str) -> bool {
        match self.peek_token().token {
            Token::Word(w) if w.value.to_uppercase() == expected => {
                self.next_token();
                true
            }
            _ => false,
        }
    }

    pub fn expect_word(&mut self, expected: &str) -> ModalResult<()> {
        if self.parse_word(expected) {
            Ok(())
        } else {
            self.expected(expected)
        }
    }

    /// Look for an expected keyword and consume it if it exists
    #[must_use]
    pub fn parse_keyword(&mut self, expected: Keyword) -> bool {
        match self.peek_token().token {
            Token::Word(w) if expected == w.keyword => {
                self.next_token();
                true
            }
            _ => false,
        }
    }

    /// Look for an expected sequence of keywords and consume them if they exist
    #[must_use]
    pub fn parse_keywords(&mut self, keywords: &[Keyword]) -> bool {
        let checkpoint = *self;
        for &keyword in keywords {
            if !self.parse_keyword(keyword) {
                // println!("parse_keywords aborting .. did not find {:?}", keyword);
                // reset index and return immediately
                *self = checkpoint;
                return false;
            }
        }
        true
    }

    /// Look for one of the given keywords and return the one that matches.
    #[must_use]
    pub fn parse_one_of_keywords(&mut self, keywords: &[Keyword]) -> Option<Keyword> {
        match self.peek_token().token {
            Token::Word(w) => {
                keywords
                    .iter()
                    .find(|keyword| **keyword == w.keyword)
                    .map(|keyword| {
                        self.next_token();
                        *keyword
                    })
            }
            _ => None,
        }
    }

    pub fn peek_nth_any_of_keywords(&mut self, n: usize, keywords: &[Keyword]) -> bool {
        match self.peek_nth_token(n).token {
            Token::Word(w) => keywords.contains(&w.keyword),
            _ => false,
        }
    }

    /// Bail out if the current token is not one of the expected keywords, or consume it if it is
    pub fn expect_one_of_keywords(&mut self, keywords: &[Keyword]) -> ModalResult<Keyword> {
        if let Some(keyword) = self.parse_one_of_keywords(keywords) {
            Ok(keyword)
        } else {
            let keywords: Vec<String> = keywords.iter().map(|x| format!("{:?}", x)).collect();
            self.expected(&format!("one of {}", keywords.join(" or ")))
        }
    }

    /// Bail out if the current token is not an expected keyword, or consume it if it is
    pub fn expect_keyword(&mut self, expected: Keyword) -> ModalResult<()> {
        if self.parse_keyword(expected) {
            Ok(())
        } else {
            self.expected(format!("{:?}", expected).as_str())
        }
    }

    /// Bail out if the following tokens are not the expected sequence of
    /// keywords, or consume them if they are.
    pub fn expect_keywords(&mut self, expected: &[Keyword]) -> ModalResult<()> {
        for &kw in expected {
            self.expect_keyword(kw)?;
        }
        Ok(())
    }

    /// Consume the next token if it matches the expected token, otherwise return false
    #[must_use]
    pub fn consume_token(&mut self, expected: &Token) -> bool {
        if self.peek_token() == *expected {
            self.next_token();
            true
        } else {
            false
        }
    }

    /// Bail out if the current token is not an expected keyword, or consume it if it is
    pub fn expect_token(&mut self, expected: &Token) -> ModalResult<()> {
        if self.consume_token(expected) {
            Ok(())
        } else {
            self.expected(&expected.to_string())
        }
    }

    /// Parse a comma-separated list of 1+ items accepted by `F`
    pub fn parse_comma_separated<T, F>(&mut self, mut f: F) -> ModalResult<Vec<T>>
    where
        F: FnMut(&mut Self) -> ModalResult<T>,
    {
        let mut values = vec![];
        loop {
            values.push(f(self)?);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(values)
    }

    /// Run a parser method `f`, reverting back to the current position
    /// if unsuccessful.
    #[must_use]
    fn maybe_parse<T, F>(&mut self, mut f: F) -> Option<T>
    where
        F: FnMut(&mut Self) -> ModalResult<T>,
    {
        let checkpoint = *self;
        match f(self) {
            Ok(t) => Some(t),
            _ => {
                *self = checkpoint;
                None
            }
        }
    }

    /// Parse either `ALL` or `DISTINCT`. Returns `true` if `DISTINCT` is parsed and results in a
    /// `ParserError` if both `ALL` and `DISTINCT` are fround.
    pub fn parse_all_or_distinct(&mut self) -> ModalResult<bool> {
        let all = self.parse_keyword(Keyword::ALL);
        let distinct = self.parse_keyword(Keyword::DISTINCT);
        if all && distinct {
            parser_err!("Cannot specify both ALL and DISTINCT")
        } else {
            Ok(distinct)
        }
    }

    /// Parse either `ALL` or `DISTINCT` or `DISTINCT ON (<expr>)`.
    pub fn parse_all_or_distinct_on(&mut self) -> ModalResult<Distinct> {
        if self.parse_keywords(&[Keyword::DISTINCT, Keyword::ON]) {
            self.expect_token(&Token::LParen)?;
            let exprs = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
            return Ok(Distinct::DistinctOn(exprs));
        } else if self.parse_keyword(Keyword::DISTINCT) {
            return Ok(Distinct::Distinct);
        };
        _ = self.parse_keyword(Keyword::ALL);
        Ok(Distinct::All)
    }

    /// Parse a SQL CREATE statement
    pub fn parse_create(&mut self) -> ModalResult<Statement> {
        let or_replace = self.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
        let temporary = self
            .parse_one_of_keywords(&[Keyword::TEMP, Keyword::TEMPORARY])
            .is_some();
        if self.parse_keyword(Keyword::TABLE) {
            self.parse_create_table(or_replace, temporary)
        } else if self.parse_keyword(Keyword::VIEW) {
            self.parse_create_view(false, or_replace)
        } else if self.parse_keywords(&[Keyword::MATERIALIZED, Keyword::VIEW]) {
            self.parse_create_view(true, or_replace)
        } else if self.parse_keywords(&[Keyword::MATERIALIZED, Keyword::SOURCE]) {
            parser_err!("CREATE MATERIALIZED SOURCE has been deprecated, use CREATE TABLE instead")
        } else if self.parse_keyword(Keyword::SOURCE) {
            self.parse_create_source(or_replace, temporary)
        } else if self.parse_keyword(Keyword::SINK) {
            if or_replace {
                parser_err!("REPLACE SINK should be used instead of CREATE OR REPLACE SINK");
            }
            self.parse_create_sink(or_replace)
        } else if self.parse_keyword(Keyword::SUBSCRIPTION) {
            self.parse_create_subscription(or_replace)
        } else if self.parse_keyword(Keyword::CONNECTION) {
            self.parse_create_connection()
        } else if self.parse_keyword(Keyword::FUNCTION) {
            self.parse_create_function(or_replace, temporary)
        } else if self.parse_keyword(Keyword::AGGREGATE) {
            self.parse_create_aggregate(or_replace)
        } else if or_replace {
            self.expected(
                "[EXTERNAL] TABLE or [MATERIALIZED] VIEW or [MATERIALIZED] SOURCE or SINK or FUNCTION after CREATE OR REPLACE",
            )
        } else if self.parse_keyword(Keyword::INDEX) {
            self.parse_create_index(false)
        } else if self.parse_keywords(&[Keyword::UNIQUE, Keyword::INDEX]) {
            self.parse_create_index(true)
        } else if self.parse_keyword(Keyword::SCHEMA) {
            self.parse_create_schema()
        } else if self.parse_keyword(Keyword::DATABASE) {
            self.parse_create_database()
        } else if self.parse_keyword(Keyword::USER) {
            self.parse_create_user()
        } else if self.parse_keyword(Keyword::SECRET) {
            self.parse_create_secret()
        } else {
            self.expected("an object type after CREATE")
        }
    }

    pub fn parse_create_schema(&mut self) -> ModalResult<Statement> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let (schema_name, owner) = if self.parse_keyword(Keyword::AUTHORIZATION) {
            let owner = self.parse_object_name()?;
            (owner.clone(), Some(owner))
        } else {
            let schema_name = self.parse_object_name()?;
            let owner = if self.parse_keyword(Keyword::AUTHORIZATION) {
                Some(self.parse_object_name()?)
            } else {
                None
            };
            (schema_name, owner)
        };
        Ok(Statement::CreateSchema {
            schema_name,
            if_not_exists,
            owner,
        })
    }

    pub fn parse_create_database(&mut self) -> ModalResult<Statement> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let db_name = self.parse_object_name()?;
        let _ = self.parse_keyword(Keyword::WITH);

        let mut owner = None;
        let mut resource_group = None;
        let mut barrier_interval_ms = None;
        let mut checkpoint_frequency = None;

        loop {
            if let Some(keyword) =
                self.parse_one_of_keywords(&[Keyword::OWNER, Keyword::RESOURCE_GROUP])
            {
                match keyword {
                    Keyword::OWNER => {
                        if owner.is_some() {
                            parser_err!("duplicate OWNER clause in CREATE DATABASE");
                        }

                        let _ = self.consume_token(&Token::Eq);
                        owner = Some(self.parse_object_name()?);
                    }
                    Keyword::RESOURCE_GROUP => {
                        if resource_group.is_some() {
                            parser_err!("duplicate RESOURCE_GROUP clause in CREATE DATABASE");
                        }

                        let _ = self.consume_token(&Token::Eq);
                        resource_group = Some(self.parse_set_variable()?);
                    }
                    _ => unreachable!(),
                }
            } else if self.parse_word("BARRIER_INTERVAL_MS") {
                if barrier_interval_ms.is_some() {
                    parser_err!("duplicate BARRIER_INTERVAL_MS clause in CREATE DATABASE");
                }

                let _ = self.consume_token(&Token::Eq);
                barrier_interval_ms = Some(self.parse_literal_u32()?);
            } else if self.parse_word("CHECKPOINT_FREQUENCY") {
                if checkpoint_frequency.is_some() {
                    parser_err!("duplicate CHECKPOINT_FREQUENCY clause in CREATE DATABASE");
                }

                let _ = self.consume_token(&Token::Eq);
                checkpoint_frequency = Some(self.parse_literal_u64()?);
            } else {
                break;
            }
        }

        Ok(Statement::CreateDatabase {
            db_name,
            if_not_exists,
            owner,
            resource_group,
            barrier_interval_ms,
            checkpoint_frequency,
        })
    }

    pub fn parse_create_view(
        &mut self,
        materialized: bool,
        or_replace: bool,
    ) -> ModalResult<Statement> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        // Many dialects support `OR ALTER` right after `CREATE`, but we don't (yet).
        // ANSI SQL and Postgres support RECURSIVE here, but we don't support it either.
        let name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        let with_options = self.parse_options_with_preceding_keyword(Keyword::WITH)?;
        self.expect_keyword(Keyword::AS)?;
        let query = Box::new(self.parse_query()?);
        let emit_mode = if materialized {
            self.parse_emit_mode()?
        } else {
            None
        };
        // Optional `WITH [ CASCADED | LOCAL ] CHECK OPTION` is widely supported here.
        Ok(Statement::CreateView {
            if_not_exists,
            name,
            columns,
            query,
            materialized,
            or_replace,
            with_options,
            emit_mode,
        })
    }

    // CREATE [OR REPLACE]?
    // [TEMPORARY] SOURCE
    // [IF NOT EXISTS]?
    // <source_name: Ident>
    // [COLUMNS]?
    // [WITH (properties)]?
    // ROW FORMAT <row_format: Ident>
    // [ROW SCHEMA LOCATION <row_schema_location: String>]?
    pub fn parse_create_source(
        &mut self,
        _or_replace: bool,
        temporary: bool,
    ) -> ModalResult<Statement> {
        impl_parse_to!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], self);
        impl_parse_to!(source_name: ObjectName, self);

        // parse columns
        let (columns, constraints, source_watermarks, wildcard_idx) =
            self.parse_columns_with_watermark()?;
        let include_options = self.parse_include_options()?;

        let with_options = self.parse_with_properties()?;
        let option = with_options
            .iter()
            .find(|&opt| opt.name.real_value() == UPSTREAM_SOURCE_KEY);
        let connector: String = option.map(|opt| opt.value.to_string()).unwrap_or_default();
        let cdc_source_job = connector.contains("-cdc");
        if cdc_source_job && (!columns.is_empty() || !constraints.is_empty()) {
            parser_err!("CDC source cannot define columns and constraints");
        }

        // row format for nexmark source must be native
        // default row format for datagen source is native
        let format_encode = self.parse_format_encode_with_connector(&connector, cdc_source_job)?;

        let stmt = CreateSourceStatement {
            temporary,
            if_not_exists,
            columns,
            wildcard_idx,
            constraints,
            source_name,
            with_properties: WithProperties(with_options),
            format_encode,
            source_watermarks,
            include_column_options: include_options,
        };

        Ok(Statement::CreateSource { stmt })
    }

    /// Parse a SQL REPLACE statement.
    pub fn parse_replace(&mut self) -> ModalResult<Statement> {
        if self.parse_keyword(Keyword::SINK) {
            self.parse_create_sink(true)
        } else {
            self.expected("SINK after REPLACE")
        }
    }

    // CREATE SINK / REPLACE SINK
    // [IF NOT EXISTS]?
    // <sink_name: Ident>
    // FROM
    // <materialized_view: Ident>
    // [WITH (properties)]?
    pub fn parse_create_sink(&mut self, or_replace: bool) -> ModalResult<Statement> {
        Ok(Statement::CreateSink {
            stmt: CreateSinkStatement::parse_to_with_or_replace(self, or_replace)?,
        })
    }

    // CREATE
    // SUBSCRIPTION
    // [IF NOT EXISTS]?
    // <subscription_name: Ident>
    // FROM
    // <materialized_view: Ident>
    // [WITH (properties)]?
    pub fn parse_create_subscription(&mut self, _or_replace: bool) -> ModalResult<Statement> {
        Ok(Statement::CreateSubscription {
            stmt: CreateSubscriptionStatement::parse_to(self)?,
        })
    }

    // CREATE
    // CONNECTION
    // [IF NOT EXISTS]?
    // <connection_name: Ident>
    // [WITH (properties)]?
    pub fn parse_create_connection(&mut self) -> ModalResult<Statement> {
        Ok(Statement::CreateConnection {
            stmt: CreateConnectionStatement::parse_to(self)?,
        })
    }

    pub fn parse_create_function(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> ModalResult<Statement> {
        impl_parse_to!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], self);

        let FunctionDesc { name, args } = self.parse_function_desc()?;

        let return_type = if self.parse_keyword(Keyword::RETURNS) {
            if self.parse_keyword(Keyword::TABLE) {
                self.expect_token(&Token::LParen)?;
                let mut values = vec![];
                loop {
                    values.push(self.parse_table_column_def()?);
                    let comma = self.consume_token(&Token::Comma);
                    if self.consume_token(&Token::RParen) {
                        // allow a trailing comma, even though it's not in standard
                        break;
                    } else if !comma {
                        return self.expected("',' or ')'");
                    }
                }
                Some(CreateFunctionReturns::Table(values))
            } else {
                Some(CreateFunctionReturns::Value(self.parse_data_type()?))
            }
        } else {
            None
        };

        let params = self.parse_create_function_body()?;
        let with_options = self.parse_options_with_preceding_keyword(Keyword::WITH)?;
        let with_options = with_options.try_into()?;
        Ok(Statement::CreateFunction {
            or_replace,
            temporary,
            if_not_exists,
            name,
            args,
            returns: return_type,
            params,
            with_options,
        })
    }

    fn parse_create_aggregate(&mut self, or_replace: bool) -> ModalResult<Statement> {
        impl_parse_to!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], self);

        let name = self.parse_object_name()?;
        self.expect_token(&Token::LParen)?;
        let args = self.parse_comma_separated(Parser::parse_function_arg)?;
        self.expect_token(&Token::RParen)?;

        self.expect_keyword(Keyword::RETURNS)?;
        let returns = self.parse_data_type()?;

        let append_only = self.parse_keywords(&[Keyword::APPEND, Keyword::ONLY]);
        let params = self.parse_create_function_body()?;

        Ok(Statement::CreateAggregate {
            or_replace,
            if_not_exists,
            name,
            args,
            returns,
            append_only,
            params,
        })
    }

    pub fn parse_declare(&mut self) -> ModalResult<Statement> {
        Ok(Statement::DeclareCursor {
            stmt: DeclareCursorStatement::parse_to(self)?,
        })
    }

    pub fn parse_fetch_cursor(&mut self) -> ModalResult<Statement> {
        Ok(Statement::FetchCursor {
            stmt: FetchCursorStatement::parse_to(self)?,
        })
    }

    pub fn parse_close_cursor(&mut self) -> ModalResult<Statement> {
        Ok(Statement::CloseCursor {
            stmt: CloseCursorStatement::parse_to(self)?,
        })
    }

    fn parse_table_column_def(&mut self) -> ModalResult<TableColumnDef> {
        Ok(TableColumnDef {
            name: self.parse_identifier_non_reserved()?,
            data_type: self.parse_data_type()?,
        })
    }

    fn parse_function_arg(&mut self) -> ModalResult<OperateFunctionArg> {
        let mode = if self.parse_keyword(Keyword::IN) {
            Some(ArgMode::In)
        } else if self.parse_keyword(Keyword::OUT) {
            Some(ArgMode::Out)
        } else if self.parse_keyword(Keyword::INOUT) {
            Some(ArgMode::InOut)
        } else {
            None
        };

        // parse: [ argname ] argtype
        let mut name = None;
        let mut data_type = self.parse_data_type()?;
        if let DataType::Custom(n) = &data_type
            && !matches!(self.peek_token().token, Token::Comma | Token::RParen)
        {
            // the first token is actually a name
            name = Some(n.0[0].clone());
            data_type = self.parse_data_type()?;
        }

        let default_expr = if self.parse_keyword(Keyword::DEFAULT) || self.consume_token(&Token::Eq)
        {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(OperateFunctionArg {
            mode,
            name,
            data_type,
            default_expr,
        })
    }

    fn parse_create_function_body(&mut self) -> ModalResult<CreateFunctionBody> {
        let mut body = CreateFunctionBody::default();
        loop {
            fn ensure_not_set<T>(field: &Option<T>, name: &str) -> ModalResult<()> {
                if field.is_some() {
                    parser_err!("{name} specified more than once");
                }
                Ok(())
            }
            if self.parse_keyword(Keyword::AS) {
                ensure_not_set(&body.as_, "AS")?;
                body.as_ = Some(self.parse_function_definition()?);
            } else if self.parse_keyword(Keyword::LANGUAGE) {
                ensure_not_set(&body.language, "LANGUAGE")?;
                body.language = Some(self.parse_identifier()?);
            } else if self.parse_keyword(Keyword::RUNTIME) {
                ensure_not_set(&body.runtime, "RUNTIME")?;
                body.runtime = Some(self.parse_identifier()?);
            } else if self.parse_keyword(Keyword::IMMUTABLE) {
                ensure_not_set(&body.behavior, "IMMUTABLE | STABLE | VOLATILE")?;
                body.behavior = Some(FunctionBehavior::Immutable);
            } else if self.parse_keyword(Keyword::STABLE) {
                ensure_not_set(&body.behavior, "IMMUTABLE | STABLE | VOLATILE")?;
                body.behavior = Some(FunctionBehavior::Stable);
            } else if self.parse_keyword(Keyword::VOLATILE) {
                ensure_not_set(&body.behavior, "IMMUTABLE | STABLE | VOLATILE")?;
                body.behavior = Some(FunctionBehavior::Volatile);
            } else if self.parse_keyword(Keyword::RETURN) {
                ensure_not_set(&body.return_, "RETURN")?;
                body.return_ = Some(self.parse_expr()?);
            } else if self.parse_keyword(Keyword::USING) {
                ensure_not_set(&body.using, "USING")?;
                body.using = Some(self.parse_create_function_using()?);
            } else {
                return Ok(body);
            }
        }
    }

    fn parse_create_function_using(&mut self) -> ModalResult<CreateFunctionUsing> {
        let keyword = self.expect_one_of_keywords(&[Keyword::LINK, Keyword::BASE64])?;

        match keyword {
            Keyword::LINK => {
                let uri = self.parse_literal_string()?;
                Ok(CreateFunctionUsing::Link(uri))
            }
            Keyword::BASE64 => {
                let base64 = self.parse_literal_string()?;
                Ok(CreateFunctionUsing::Base64(base64))
            }
            _ => unreachable!("{}", keyword),
        }
    }

    // CREATE USER name [ [ WITH ] option [ ... ] ]
    // where option can be:
    //       SUPERUSER | NOSUPERUSER
    //     | CREATEDB | NOCREATEDB
    //     | CREATEUSER | NOCREATEUSER
    //     | LOGIN | NOLOGIN
    //     | [ ENCRYPTED ] PASSWORD 'password' | PASSWORD NULL | OAUTH
    fn parse_create_user(&mut self) -> ModalResult<Statement> {
        Ok(Statement::CreateUser(CreateUserStatement::parse_to(self)?))
    }

    fn parse_create_secret(&mut self) -> ModalResult<Statement> {
        Ok(Statement::CreateSecret {
            stmt: CreateSecretStatement::parse_to(self)?,
        })
    }

    pub fn parse_with_properties(&mut self) -> ModalResult<Vec<SqlOption>> {
        self.parse_options_with_preceding_keyword(Keyword::WITH)
    }

    pub fn parse_discard(&mut self) -> ModalResult<Statement> {
        self.expect_keyword(Keyword::ALL)?;
        Ok(Statement::Discard(DiscardType::All))
    }

    pub fn parse_drop(&mut self) -> ModalResult<Statement> {
        if self.parse_keyword(Keyword::FUNCTION) {
            return self.parse_drop_function();
        } else if self.parse_keyword(Keyword::AGGREGATE) {
            return self.parse_drop_aggregate();
        }
        Ok(Statement::Drop(DropStatement::parse_to(self)?))
    }

    /// ```sql
    /// DROP FUNCTION [ IF EXISTS ] name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] [, ...]
    /// [ CASCADE | RESTRICT ]
    /// ```
    fn parse_drop_function(&mut self) -> ModalResult<Statement> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let func_desc = self.parse_comma_separated(Parser::parse_function_desc)?;
        let option = match self.parse_one_of_keywords(&[Keyword::CASCADE, Keyword::RESTRICT]) {
            Some(Keyword::CASCADE) => Some(ReferentialAction::Cascade),
            Some(Keyword::RESTRICT) => Some(ReferentialAction::Restrict),
            _ => None,
        };
        Ok(Statement::DropFunction {
            if_exists,
            func_desc,
            option,
        })
    }

    /// ```sql
    /// DROP AGGREGATE [ IF EXISTS ] name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] [, ...]
    /// [ CASCADE | RESTRICT ]
    /// ```
    fn parse_drop_aggregate(&mut self) -> ModalResult<Statement> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let func_desc = self.parse_comma_separated(Parser::parse_function_desc)?;
        let option = match self.parse_one_of_keywords(&[Keyword::CASCADE, Keyword::RESTRICT]) {
            Some(Keyword::CASCADE) => Some(ReferentialAction::Cascade),
            Some(Keyword::RESTRICT) => Some(ReferentialAction::Restrict),
            _ => None,
        };
        Ok(Statement::DropAggregate {
            if_exists,
            func_desc,
            option,
        })
    }

    fn parse_function_desc(&mut self) -> ModalResult<FunctionDesc> {
        let name = self.parse_object_name()?;

        let args = if self.consume_token(&Token::LParen) {
            if self.consume_token(&Token::RParen) {
                Some(vec![])
            } else {
                let args = self.parse_comma_separated(Parser::parse_function_arg)?;
                self.expect_token(&Token::RParen)?;
                Some(args)
            }
        } else {
            None
        };

        Ok(FunctionDesc { name, args })
    }

    pub fn parse_create_index(&mut self, unique: bool) -> ModalResult<Statement> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let index_name = self.parse_object_name()?;
        self.expect_keyword(Keyword::ON)?;
        let table_name = self.parse_object_name()?;
        let method = if self.parse_keyword(Keyword::USING) {
            let method = self.parse_identifier()?;
            Some(method)
        } else {
            None
        };
        self.expect_token(&Token::LParen)?;
        let columns = self.parse_comma_separated(Parser::parse_order_by_expr)?;
        self.expect_token(&Token::RParen)?;
        let mut include = vec![];
        if self.parse_keyword(Keyword::INCLUDE) {
            self.expect_token(&Token::LParen)?;
            include = self.parse_comma_separated(Parser::parse_identifier_non_reserved)?;
            self.expect_token(&Token::RParen)?;
        }
        let mut distributed_by = vec![];
        if self.parse_keywords(&[Keyword::DISTRIBUTED, Keyword::BY]) {
            self.expect_token(&Token::LParen)?;
            distributed_by = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
        }
        let with_properties = WithProperties(self.parse_with_properties()?);

        Ok(Statement::CreateIndex {
            name: index_name,
            table_name,
            method,
            columns,
            include,
            distributed_by,
            unique,
            if_not_exists,
            with_properties,
        })
    }

    pub fn parse_with_version_columns(&mut self) -> ModalResult<Vec<Ident>> {
        if self.parse_keywords(&[Keyword::WITH, Keyword::VERSION, Keyword::COLUMN]) {
            self.expect_token(&Token::LParen)?;
            let columns =
                self.parse_comma_separated(|parser| parser.parse_identifier_non_reserved())?;
            self.expect_token(&Token::RParen)?;
            Ok(columns)
        } else {
            Ok(Vec::new())
        }
    }

    pub fn parse_on_conflict(&mut self) -> ModalResult<Option<OnConflict>> {
        if self.parse_keywords(&[Keyword::ON, Keyword::CONFLICT]) {
            self.parse_handle_conflict_behavior()
        } else {
            Ok(None)
        }
    }

    pub fn parse_create_table(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> ModalResult<Statement> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name()?;
        // parse optional column list (schema) and watermarks on source.
        let (columns, constraints, source_watermarks, wildcard_idx) =
            self.parse_columns_with_watermark()?;

        let append_only = if self.parse_keyword(Keyword::APPEND) {
            self.expect_keyword(Keyword::ONLY)?;
            true
        } else {
            false
        };

        let on_conflict = self.parse_on_conflict()?;

        let with_version_columns = self.parse_with_version_columns()?;
        let include_options = self.parse_include_options()?;

        // PostgreSQL supports `WITH ( options )`, before `AS`
        let with_options = self.parse_with_properties()?;

        let option = with_options
            .iter()
            .find(|&opt| opt.name.real_value() == UPSTREAM_SOURCE_KEY);
        let connector = option.map(|opt| opt.value.to_string());
        let contain_webhook =
            connector.is_some() && connector.as_ref().unwrap().contains(WEBHOOK_CONNECTOR);

        // webhook connector does not require row format
        let format_encode = if let Some(connector) = connector
            && !contain_webhook
        {
            Some(self.parse_format_encode_with_connector(&connector, false)?)
        } else {
            None // Table is NOT created with an external connector.
        };
        // Parse optional `AS ( query )`
        let query = if self.parse_keyword(Keyword::AS) {
            if !source_watermarks.is_empty() {
                parser_err!("Watermarks can't be defined on table created by CREATE TABLE AS");
            }
            Some(Box::new(self.parse_query()?))
        } else {
            None
        };

        let cdc_table_info = if self.parse_keyword(Keyword::FROM) {
            let source_name = self.parse_object_name()?;
            self.expect_keyword(Keyword::TABLE)?;
            let external_table_name = self.parse_literal_string()?;
            Some(CdcTableInfo {
                source_name,
                external_table_name,
            })
        } else {
            None
        };

        let webhook_wait_for_persistence = with_options
            .iter()
            .find(|&opt| opt.name.real_value() == WEBHOOK_WAIT_FOR_PERSISTENCE)
            .map(|opt| opt.value.to_string().eq_ignore_ascii_case("true"))
            .unwrap_or(true);
        let webhook_is_batched = with_options
            .iter()
            .find(|&opt| opt.name.real_value() == WEBHOOK_IS_BATCHED)
            .map(|opt| opt.value.to_string().eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let webhook_info = if self.parse_keyword(Keyword::VALIDATE) {
            if !contain_webhook {
                parser_err!("VALIDATE is only supported for tables created with webhook source");
            }

            let secret_ref = if self.parse_keyword(Keyword::SECRET) {
                let secret_ref = self.parse_secret_ref()?;
                if secret_ref.ref_as == SecretRefAsType::File {
                    parser_err!("Secret for SECURE_COMPARE() does not support AS FILE");
                };
                Some(secret_ref)
            } else {
                None
            };

            self.expect_keyword(Keyword::AS)?;
            let signature_expr = self.parse_function()?;

            Some(WebhookSourceInfo {
                secret_ref,
                signature_expr: Some(signature_expr),
                wait_for_persistence: webhook_wait_for_persistence,
                is_batched: webhook_is_batched,
            })
        } else if contain_webhook {
            Some(WebhookSourceInfo {
                secret_ref: None,
                signature_expr: None,
                wait_for_persistence: webhook_wait_for_persistence,
                is_batched: webhook_is_batched,
            })
        } else {
            None
        };

        let engine = if self.parse_keyword(Keyword::ENGINE) {
            self.expect_token(&Token::Eq)?;
            let engine_name = self.parse_object_name()?;
            if "iceberg".eq_ignore_ascii_case(&engine_name.real_value()) {
                Engine::Iceberg
            } else if "hummock".eq_ignore_ascii_case(&engine_name.real_value()) {
                Engine::Hummock
            } else {
                parser_err!("Unsupported engine: {}", engine_name);
            }
        } else {
            Engine::Hummock
        };

        Ok(Statement::CreateTable {
            name: table_name,
            temporary,
            columns,
            wildcard_idx,
            constraints,
            with_options,
            or_replace,
            if_not_exists,
            format_encode,
            source_watermarks,
            append_only,
            on_conflict,
            with_version_columns,
            query,
            cdc_table_info,
            include_column_options: include_options,
            webhook_info,
            engine,
        })
    }

    pub fn parse_include_options(&mut self) -> ModalResult<IncludeOption> {
        let mut options = vec![];
        while self.parse_keyword(Keyword::INCLUDE) {
            let column_type = self.parse_identifier()?;

            let mut column_inner_field = None;
            let mut header_inner_expect_type = None;
            if let Token::SingleQuotedString(inner_field) = self.peek_token().token {
                self.next_token();
                column_inner_field = Some(inner_field);

                // `verify` rejects `DataType::Custom` so that a following `INCLUDE` (or even `WITH`)
                // will not be misrecognized as a DataType.
                //
                // For example, the following look structurally the same because `INCLUDE` is not a
                // reserved keyword. (`AS` is reserved.)
                // * `INCLUDE header 'foo' varchar`
                // * `INCLUDE header 'foo' INCLUDE`
                //
                // To be honest `bytea` shall be a `DataType::Custom` rather than a keyword, and the
                // logic here shall be:
                // ```
                // match dt {
                //     DataType::Custom(name) => allowed.contains(name.real_value()),
                //     _ => true,
                // }
                // ```
                // An allowlist is better than a denylist, as the following token may be other than
                // `INCLUDE` or `WITH` in the future.
                //
                // If this sounds too complicated - it means we should have designed this extension
                // syntax differently to make ambiguity handling easier.
                header_inner_expect_type =
                    opt(parser_v2::data_type.verify(|dt| !matches!(dt, DataType::Custom(_))))
                        .parse_next(self)?;
            }

            let mut column_alias = None;
            if self.parse_keyword(Keyword::AS) {
                column_alias = Some(self.parse_identifier()?);
            }

            options.push(IncludeOptionItem {
                column_type,
                inner_field: column_inner_field,
                column_alias,
                header_inner_expect_type,
            });

            // tolerate previous bug #18800 of displaying with comma separation
            let _ = self.consume_token(&Token::Comma);
        }
        Ok(options)
    }

    pub fn parse_columns_with_watermark(&mut self) -> ModalResult<ColumnsDefTuple> {
        let mut columns = vec![];
        let mut constraints = vec![];
        let mut watermarks = vec![];
        let mut wildcard_idx = None;
        if !self.consume_token(&Token::LParen) || self.consume_token(&Token::RParen) {
            return Ok((columns, constraints, watermarks, wildcard_idx));
        }

        loop {
            if self.consume_token(&Token::Mul) {
                if wildcard_idx.is_none() {
                    wildcard_idx = Some(columns.len());
                } else {
                    parser_err!("At most 1 wildcard is allowed in source definition");
                }
            } else if let Some(constraint) = self.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Some(watermark) = self.parse_optional_watermark()? {
                watermarks.push(watermark);
                if watermarks.len() > 1 {
                    // TODO(yuhao): allow multiple watermark on source.
                    parser_err!("Only 1 watermark is allowed to be defined on source.");
                }
            } else if let Token::Word(_) = self.peek_token().token {
                columns.push(self.parse_column_def()?);
            } else {
                return self.expected("column name or constraint definition");
            }
            let comma = self.consume_token(&Token::Comma);
            if self.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected("',' or ')' after column definition");
            }
        }

        Ok((columns, constraints, watermarks, wildcard_idx))
    }

    fn parse_column_def(&mut self) -> ModalResult<ColumnDef> {
        let name = self.parse_identifier_non_reserved()?;
        let data_type = if let Token::Word(_) = self.peek_token().token {
            Some(self.parse_data_type()?)
        } else {
            None
        };

        let collation = if self.parse_keyword(Keyword::COLLATE) {
            Some(self.parse_object_name()?)
        } else {
            None
        };
        let mut options = vec![];
        loop {
            if self.parse_keyword(Keyword::CONSTRAINT) {
                let name = Some(self.parse_identifier_non_reserved()?);
                if let Some(option) = self.parse_optional_column_option()? {
                    options.push(ColumnOptionDef { name, option });
                } else {
                    return self.expected("constraint details after CONSTRAINT <name>");
                }
            } else if let Some(option) = self.parse_optional_column_option()? {
                options.push(ColumnOptionDef { name: None, option });
            } else {
                break;
            };
        }
        Ok(ColumnDef {
            name,
            data_type,
            collation,
            options,
        })
    }

    pub fn parse_optional_column_option(&mut self) -> ModalResult<Option<ColumnOption>> {
        if self.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            Ok(Some(ColumnOption::NotNull))
        } else if self.parse_keyword(Keyword::NULL) {
            Ok(Some(ColumnOption::Null))
        } else if self.parse_keyword(Keyword::DEFAULT) {
            if self.parse_keyword(Keyword::INTERNAL) {
                Ok(Some(ColumnOption::DefaultValueInternal {
                    // Placeholder. Will fill during definition purification for schema change.
                    persisted: Default::default(),
                    expr: None,
                }))
            } else {
                Ok(Some(ColumnOption::DefaultValue(self.parse_expr()?)))
            }
        } else if self.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY]) {
            Ok(Some(ColumnOption::Unique { is_primary: true }))
        } else if self.parse_keyword(Keyword::UNIQUE) {
            Ok(Some(ColumnOption::Unique { is_primary: false }))
        } else if self.parse_keyword(Keyword::REFERENCES) {
            let foreign_table = self.parse_object_name()?;
            // PostgreSQL allows omitting the column list and
            // uses the primary key column of the foreign table by default
            let referred_columns = self.parse_parenthesized_column_list(Optional)?;
            let mut on_delete = None;
            let mut on_update = None;
            loop {
                if on_delete.is_none() && self.parse_keywords(&[Keyword::ON, Keyword::DELETE]) {
                    on_delete = Some(self.parse_referential_action()?);
                } else if on_update.is_none()
                    && self.parse_keywords(&[Keyword::ON, Keyword::UPDATE])
                {
                    on_update = Some(self.parse_referential_action()?);
                } else {
                    break;
                }
            }
            Ok(Some(ColumnOption::ForeignKey {
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
            }))
        } else if self.parse_keyword(Keyword::CHECK) {
            self.expect_token(&Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Ok(Some(ColumnOption::Check(expr)))
        } else if self.parse_keyword(Keyword::AS) {
            Ok(Some(ColumnOption::GeneratedColumns(self.parse_expr()?)))
        } else {
            Ok(None)
        }
    }

    pub fn parse_handle_conflict_behavior(&mut self) -> ModalResult<Option<OnConflict>> {
        if self.parse_keyword(Keyword::OVERWRITE) {
            // compatible with v1.9 - v2.0
            Ok(Some(OnConflict::UpdateFull))
        } else if self.parse_keyword(Keyword::IGNORE) {
            // compatible with v1.9 - v2.0
            Ok(Some(OnConflict::Nothing))
        } else if self.parse_keywords(&[
            Keyword::DO,
            Keyword::UPDATE,
            Keyword::IF,
            Keyword::NOT,
            Keyword::NULL,
        ]) {
            Ok(Some(OnConflict::UpdateIfNotNull))
        } else if self.parse_keywords(&[Keyword::DO, Keyword::UPDATE, Keyword::FULL]) {
            Ok(Some(OnConflict::UpdateFull))
        } else if self.parse_keywords(&[Keyword::DO, Keyword::NOTHING]) {
            Ok(Some(OnConflict::Nothing))
        } else {
            Ok(None)
        }
    }

    pub fn parse_referential_action(&mut self) -> ModalResult<ReferentialAction> {
        if self.parse_keyword(Keyword::RESTRICT) {
            Ok(ReferentialAction::Restrict)
        } else if self.parse_keyword(Keyword::CASCADE) {
            Ok(ReferentialAction::Cascade)
        } else if self.parse_keywords(&[Keyword::SET, Keyword::NULL]) {
            Ok(ReferentialAction::SetNull)
        } else if self.parse_keywords(&[Keyword::NO, Keyword::ACTION]) {
            Ok(ReferentialAction::NoAction)
        } else if self.parse_keywords(&[Keyword::SET, Keyword::DEFAULT]) {
            Ok(ReferentialAction::SetDefault)
        } else {
            self.expected("one of RESTRICT, CASCADE, SET NULL, NO ACTION or SET DEFAULT")
        }
    }

    pub fn parse_optional_watermark(&mut self) -> ModalResult<Option<SourceWatermark>> {
        if self.parse_keyword(Keyword::WATERMARK) {
            self.expect_keyword(Keyword::FOR)?;
            let column = self.parse_identifier_non_reserved()?;
            self.expect_keyword(Keyword::AS)?;
            let expr = self.parse_expr()?;
            let with_ttl = self.parse_keywords(&[Keyword::WITH, Keyword::TTL]);
            Ok(Some(SourceWatermark {
                column,
                expr,
                with_ttl,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn parse_optional_table_constraint(&mut self) -> ModalResult<Option<TableConstraint>> {
        let name = if self.parse_keyword(Keyword::CONSTRAINT) {
            Some(self.parse_identifier_non_reserved()?)
        } else {
            None
        };
        let checkpoint = *self;
        let token = self.next_token();
        match token.token {
            Token::Word(w) if w.keyword == Keyword::PRIMARY || w.keyword == Keyword::UNIQUE => {
                let is_primary = w.keyword == Keyword::PRIMARY;
                if is_primary {
                    self.expect_keyword(Keyword::KEY)?;
                }
                let columns = self.parse_parenthesized_column_list(Mandatory)?;
                Ok(Some(TableConstraint::Unique {
                    name,
                    columns,
                    is_primary,
                }))
            }
            Token::Word(w) if w.keyword == Keyword::FOREIGN => {
                self.expect_keyword(Keyword::KEY)?;
                let columns = self.parse_parenthesized_column_list(Mandatory)?;
                self.expect_keyword(Keyword::REFERENCES)?;
                let foreign_table = self.parse_object_name()?;
                let referred_columns = self.parse_parenthesized_column_list(Mandatory)?;
                let mut on_delete = None;
                let mut on_update = None;
                loop {
                    if on_delete.is_none() && self.parse_keywords(&[Keyword::ON, Keyword::DELETE]) {
                        on_delete = Some(self.parse_referential_action()?);
                    } else if on_update.is_none()
                        && self.parse_keywords(&[Keyword::ON, Keyword::UPDATE])
                    {
                        on_update = Some(self.parse_referential_action()?);
                    } else {
                        break;
                    }
                }
                Ok(Some(TableConstraint::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                }))
            }
            Token::Word(w) if w.keyword == Keyword::CHECK => {
                self.expect_token(&Token::LParen)?;
                let expr = Box::new(self.parse_expr()?);
                self.expect_token(&Token::RParen)?;
                Ok(Some(TableConstraint::Check { name, expr }))
            }
            _ => {
                *self = checkpoint;
                if name.is_some() {
                    self.expected("PRIMARY, UNIQUE, FOREIGN, or CHECK")
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub fn parse_options_with_preceding_keyword(
        &mut self,
        keyword: Keyword,
    ) -> ModalResult<Vec<SqlOption>> {
        if self.parse_keyword(keyword) {
            self.expect_token(&Token::LParen)?;
            self.parse_options_inner()
        } else {
            Ok(vec![])
        }
    }

    pub fn parse_options(&mut self) -> ModalResult<Vec<SqlOption>> {
        if self.peek_token() == Token::LParen {
            self.next_token();
            self.parse_options_inner()
        } else {
            Ok(vec![])
        }
    }

    // has parsed a LParen
    pub fn parse_options_inner(&mut self) -> ModalResult<Vec<SqlOption>> {
        let mut values = vec![];
        loop {
            values.push(Parser::parse_sql_option(self)?);
            let comma = self.consume_token(&Token::Comma);
            if self.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected("',' or ')' after option definition");
            }
        }
        Ok(values)
    }

    pub fn parse_sql_option(&mut self) -> ModalResult<SqlOption> {
        const CONNECTION_REF_KEY: &str = "connection";
        const BACKFILL_ORDER: &str = "backfill_order";

        let name = self.parse_object_name()?;
        self.expect_token(&Token::Eq)?;
        let value = {
            if name.real_value().eq_ignore_ascii_case(CONNECTION_REF_KEY) {
                let connection_name = self.parse_object_name()?;
                // tolerate previous buggy Display that outputs `connection = connection foo`
                let connection_name = match connection_name.0.as_slice() {
                    [ident] if ident.real_value() == CONNECTION_REF_KEY => {
                        self.parse_object_name()?
                    }
                    _ => connection_name,
                };
                SqlOptionValue::ConnectionRef(ConnectionRefValue { connection_name })
            } else if name.real_value().eq_ignore_ascii_case(BACKFILL_ORDER) {
                let order = self.parse_backfill_order_strategy()?;
                SqlOptionValue::BackfillOrder(order)
            } else {
                self.parse_value_and_obj_ref::<false>()?
            }
        };
        Ok(SqlOption { name, value })
    }

    // <config_param> { TO | = } { <value> | DEFAULT }
    // <config_param> is not a keyword, but an identifier
    pub fn parse_config_param(&mut self) -> ModalResult<ConfigParam> {
        self.parse_config_param_inner(Self::parse_set_variable)
    }

    fn parse_config_param_inner(
        &mut self,
        parse_value: fn(&mut Self) -> ModalResult<SetVariableValue>,
    ) -> ModalResult<ConfigParam> {
        let param = self.parse_identifier()?;
        if !self.consume_token(&Token::Eq) && !self.parse_keyword(Keyword::TO) {
            return self.expected("'=' or 'TO' after config parameter");
        }
        let value = parse_value(self)?;
        Ok(ConfigParam { param, value })
    }

    /// Parse a single-value config param.
    ///
    /// This differs from [`Self::parse_config_param`] in that it does **not** allow a comma-separated
    /// list on the RHS, so it can be safely used in constructs where comma separates multiple
    /// assignments (e.g. `... SET a = 1, b = 2`).
    fn parse_config_param_no_list(&mut self) -> ModalResult<ConfigParam> {
        self.parse_config_param_inner(Self::parse_set_variable_no_list)
    }

    fn parse_set_variable_no_list(&mut self) -> ModalResult<SetVariableValue> {
        alt((
            Keyword::DEFAULT.value(SetVariableValue::Default),
            alt((
                Self::ensure_parse_value.map(SetVariableValueSingle::Literal),
                |parser: &mut Self| {
                    let checkpoint = *parser;
                    let ident = parser.parse_identifier()?;
                    if ident.value == "default" {
                        *parser = checkpoint;
                        return parser.expected("parameter list value").map_err(|e| e.cut());
                    }
                    Ok(SetVariableValueSingle::Ident(ident))
                },
                fail.expect("parameter value"),
            ))
            .map(|single: SetVariableValueSingle| SetVariableValue::Single(single)),
        ))
        .parse_next(self)
    }

    pub fn parse_since(&mut self) -> ModalResult<Since> {
        if self.parse_keyword(Keyword::SINCE) {
            let checkpoint = *self;
            let token = self.next_token();
            match token.token {
                Token::Word(w) => {
                    let ident = w.to_ident()?;
                    // Backward compatibility for now.
                    if ident.real_value() == "proctime" || ident.real_value() == "now" {
                        self.expect_token(&Token::LParen)?;
                        self.expect_token(&Token::RParen)?;
                        Ok(Since::ProcessTime)
                    } else if ident.real_value() == "begin" {
                        self.expect_token(&Token::LParen)?;
                        self.expect_token(&Token::RParen)?;
                        Ok(Since::Begin)
                    } else {
                        parser_err!(
                            "Expected proctime(), begin() or now(), found: {}",
                            ident.real_value()
                        )
                    }
                }
                Token::Number(s) => {
                    let num = s
                        .parse::<u64>()
                        .map_err(|e| StrError(format!("Could not parse '{}' as u64: {}", s, e)))?;
                    Ok(Since::TimestampMsNum(num))
                }
                _ => self.expected_at(checkpoint, "proctime(), begin() , now(), Number"),
            }
        } else if self.parse_word("FULL") {
            Ok(Since::Full)
        } else {
            Ok(Since::ProcessTime)
        }
    }

    pub fn parse_emit_mode(&mut self) -> ModalResult<Option<EmitMode>> {
        if self.parse_keyword(Keyword::EMIT) {
            match self.parse_one_of_keywords(&[Keyword::IMMEDIATELY, Keyword::ON]) {
                Some(Keyword::IMMEDIATELY) => Ok(Some(EmitMode::Immediately)),
                Some(Keyword::ON) => {
                    self.expect_keywords(&[Keyword::WINDOW, Keyword::CLOSE])?;
                    Ok(Some(EmitMode::OnWindowClose))
                }
                Some(_) => unreachable!(),
                None => self.expected("IMMEDIATELY or ON WINDOW CLOSE after EMIT"),
            }
        } else {
            Ok(None)
        }
    }

    pub fn parse_alter(&mut self) -> ModalResult<Statement> {
        if self.parse_keyword(Keyword::DATABASE) {
            self.parse_alter_database()
        } else if self.parse_keyword(Keyword::SCHEMA) {
            self.parse_alter_schema()
        } else if self.parse_keyword(Keyword::TABLE) {
            self.parse_alter_table()
        } else if self.parse_keyword(Keyword::INDEX) {
            self.parse_alter_index()
        } else if self.parse_keyword(Keyword::VIEW) {
            self.parse_alter_view(false)
        } else if self.parse_keywords(&[Keyword::MATERIALIZED, Keyword::VIEW]) {
            self.parse_alter_view(true)
        } else if self.parse_keyword(Keyword::SINK) {
            self.parse_alter_sink()
        } else if self.parse_keyword(Keyword::SOURCE) {
            self.parse_alter_source()
        } else if self.parse_keyword(Keyword::FUNCTION) {
            self.parse_alter_function()
        } else if self.parse_keyword(Keyword::CONNECTION) {
            self.parse_alter_connection()
        } else if self.parse_keyword(Keyword::USER) {
            self.parse_alter_user()
        } else if self.parse_keyword(Keyword::SYSTEM) {
            self.parse_alter_system()
        } else if self.parse_keyword(Keyword::SUBSCRIPTION) {
            self.parse_alter_subscription()
        } else if self.parse_keyword(Keyword::SECRET) {
            self.parse_alter_secret()
        } else if self.parse_word("FRAGMENT") {
            self.parse_alter_fragment()
        } else if self.parse_keyword(Keyword::COMPACTION) {
            self.parse_alter_compaction_group()
        } else if self.parse_keywords(&[Keyword::DEFAULT, Keyword::PRIVILEGES]) {
            self.parse_alter_default_privileges()
        } else {
            self.expected(
                "COMPACTION, DATABASE, FRAGMENT, SCHEMA, TABLE, INDEX, MATERIALIZED, VIEW, SINK, SUBSCRIPTION, SOURCE, FUNCTION, USER, SECRET or SYSTEM after ALTER"
            )
        }
    }

    pub fn parse_alter_database(&mut self) -> ModalResult<Statement> {
        let database_name = self.parse_object_name()?;
        let operation = if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            let owner_name: Ident = self.parse_identifier()?;
            AlterDatabaseOperation::ChangeOwner {
                new_owner_name: owner_name,
            }
        } else if self.parse_keyword(Keyword::RENAME) {
            if self.parse_keyword(Keyword::TO) {
                let database_name = self.parse_object_name()?;
                AlterDatabaseOperation::RenameDatabase { database_name }
            } else {
                return self.expected("TO after RENAME");
            }
        } else if self.parse_keyword(Keyword::SET) {
            if self.parse_keyword(Keyword::RESOURCE_GROUP) {
                if self.expect_keyword(Keyword::TO).is_err()
                    && self.expect_token(&Token::Eq).is_err()
                {
                    return self.expected("TO or = after ALTER DATABASE SET RESOURCE_GROUP");
                }
                let value = self.parse_set_variable()?;
                if !self.parse_keyword(Keyword::DEFERRED) {
                    return self.expected("DEFERRED after ALTER DATABASE SET RESOURCE_GROUP");
                }

                AlterDatabaseOperation::SetResourceGroup {
                    resource_group: Some(value),
                    deferred: true,
                }
            } else {
                // check will be delayed to frontend
                AlterDatabaseOperation::SetParam(self.parse_config_param()?)
            }
        } else if self.parse_keyword(Keyword::RESET) {
            if self.parse_keyword(Keyword::RESOURCE_GROUP) {
                if !self.parse_keyword(Keyword::DEFERRED) {
                    return self.expected("DEFERRED after ALTER DATABASE RESET RESOURCE_GROUP");
                }

                AlterDatabaseOperation::SetResourceGroup {
                    resource_group: None,
                    deferred: true,
                }
            } else {
                return self.expected("RESOURCE_GROUP after RESET");
            }
        } else {
            return self.expected("RENAME, OWNER TO, SET, OR RESET after ALTER DATABASE");
        };

        Ok(Statement::AlterDatabase {
            name: database_name,
            operation,
        })
    }

    pub fn parse_alter_schema(&mut self) -> ModalResult<Statement> {
        let schema_name = self.parse_object_name()?;
        let operation = if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            let owner_name: Ident = self.parse_identifier()?;
            AlterSchemaOperation::ChangeOwner {
                new_owner_name: owner_name,
            }
        } else if self.parse_keyword(Keyword::RENAME) {
            self.expect_keyword(Keyword::TO)?;
            let schema_name = self.parse_object_name()?;
            AlterSchemaOperation::RenameSchema { schema_name }
        } else if self.parse_keywords(&[Keyword::SWAP, Keyword::WITH]) {
            let target_schema = self.parse_object_name()?;
            AlterSchemaOperation::SwapRenameSchema { target_schema }
        } else {
            return self.expected("RENAME, OWNER TO, OR SWAP WITH after ALTER SCHEMA");
        };

        Ok(Statement::AlterSchema {
            name: schema_name,
            operation,
        })
    }

    pub fn parse_alter_user(&mut self) -> ModalResult<Statement> {
        Ok(Statement::AlterUser(AlterUserStatement::parse_to(self)?))
    }

    pub fn parse_alter_table(&mut self) -> ModalResult<Statement> {
        let _ = self.parse_keyword(Keyword::ONLY);
        let table_name = self.parse_object_name()?;
        let operation = if self.parse_keyword(Keyword::ADD) {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                AlterTableOperation::AddConstraint(constraint)
            } else {
                let _ = self.parse_keyword(Keyword::COLUMN);
                let _if_not_exists =
                    self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
                let column_def = self.parse_column_def()?;
                AlterTableOperation::AddColumn { column_def }
            }
        } else if self.parse_keywords(&[Keyword::DROP, Keyword::CONNECTOR]) {
            AlterTableOperation::DropConnector
        } else if self.parse_keyword(Keyword::RENAME) {
            if self.parse_keyword(Keyword::CONSTRAINT) {
                let old_name = self.parse_identifier_non_reserved()?;
                self.expect_keyword(Keyword::TO)?;
                let new_name = self.parse_identifier_non_reserved()?;
                AlterTableOperation::RenameConstraint { old_name, new_name }
            } else if self.parse_keyword(Keyword::TO) {
                let table_name = self.parse_object_name()?;
                AlterTableOperation::RenameTable { table_name }
            } else {
                let _ = self.parse_keyword(Keyword::COLUMN);
                let old_column_name = self.parse_identifier_non_reserved()?;
                self.expect_keyword(Keyword::TO)?;
                let new_column_name = self.parse_identifier_non_reserved()?;
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                }
            }
        } else if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            let owner_name: Ident = self.parse_identifier()?;
            AlterTableOperation::ChangeOwner {
                new_owner_name: owner_name,
            }
        } else if self.parse_keyword(Keyword::SET) {
            if self.parse_keyword(Keyword::SCHEMA) {
                let schema_name = self.parse_object_name()?;
                AlterTableOperation::SetSchema {
                    new_schema_name: schema_name,
                }
            } else if self.parse_keyword(Keyword::PARALLELISM) {
                if self.expect_keyword(Keyword::TO).is_err()
                    && self.expect_token(&Token::Eq).is_err()
                {
                    return self.expected("TO or = after ALTER TABLE SET PARALLELISM");
                }

                let value = self.parse_set_variable()?;

                let deferred = self.parse_keyword(Keyword::DEFERRED);

                AlterTableOperation::SetParallelism {
                    parallelism: value,
                    deferred,
                }
            } else if self.parse_keyword(Keyword::BACKFILL_PARALLELISM) {
                if self.expect_keyword(Keyword::TO).is_err()
                    && self.expect_token(&Token::Eq).is_err()
                {
                    return self.expected("TO or = after ALTER TABLE SET BACKFILL_PARALLELISM");
                }

                let value = self.parse_set_variable()?;

                let deferred = self.parse_keyword(Keyword::DEFERRED);

                AlterTableOperation::SetBackfillParallelism {
                    parallelism: value,
                    deferred,
                }
            } else if let Some(rate_limit) = self.parse_alter_rate_limit()? {
                AlterTableOperation::AlterRateLimit(rate_limit)
            } else if self.parse_keyword(Keyword::CONFIG) {
                let entries = self.parse_options()?;
                AlterTableOperation::SetConfig { entries }
            } else {
                return self.expected(
                    "SCHEMA/PARALLELISM/BACKFILL_PARALLELISM/SOURCE_RATE_LIMIT/DML_RATE_LIMIT/CONFIG after SET",
                );
            }
        } else if self.parse_keyword(Keyword::RESET) {
            if self.parse_keyword(Keyword::CONFIG) {
                let keys = self.parse_parenthesized_object_name_list()?;
                AlterTableOperation::ResetConfig { keys }
            } else {
                return self.expected("CONFIG after RESET");
            }
        } else if self.parse_keyword(Keyword::DROP) {
            let _ = self.parse_keyword(Keyword::COLUMN);
            let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let column_name = self.parse_identifier_non_reserved()?;
            let cascade = self.parse_keyword(Keyword::CASCADE);
            AlterTableOperation::DropColumn {
                column_name,
                if_exists,
                cascade,
            }
        } else if self.parse_keyword(Keyword::ALTER) {
            // `WATERMARK` is non-reserved; require `FOR` so `ALTER <col>` on a
            // column named `watermark` still falls through to ALTER COLUMN.
            if self.parse_keywords(&[Keyword::WATERMARK, Keyword::FOR]) {
                let column_name = self.parse_identifier_non_reserved()?;
                self.expect_keyword(Keyword::AS)?;
                let expr = self.parse_expr()?;
                let with_ttl = self.parse_keywords(&[Keyword::WITH, Keyword::TTL]);
                return Ok(Statement::AlterTable {
                    name: table_name,
                    operation: AlterTableOperation::AlterWatermark {
                        column_name,
                        expr,
                        with_ttl,
                    },
                });
            }
            let _ = self.parse_keyword(Keyword::COLUMN);
            let column_name = self.parse_identifier_non_reserved()?;

            let op = if self.parse_keywords(&[Keyword::SET, Keyword::NOT, Keyword::NULL]) {
                AlterColumnOperation::SetNotNull {}
            } else if self.parse_keywords(&[Keyword::DROP, Keyword::NOT, Keyword::NULL]) {
                AlterColumnOperation::DropNotNull {}
            } else if self.parse_keywords(&[Keyword::SET, Keyword::DEFAULT]) {
                AlterColumnOperation::SetDefault {
                    value: self.parse_expr()?,
                }
            } else if self.parse_keywords(&[Keyword::DROP, Keyword::DEFAULT]) {
                AlterColumnOperation::DropDefault {}
            } else if self.parse_keywords(&[Keyword::SET, Keyword::DATA, Keyword::TYPE])
                || (self.parse_keyword(Keyword::TYPE))
            {
                let data_type = self.parse_data_type()?;
                let using = if self.parse_keyword(Keyword::USING) {
                    Some(self.parse_expr()?)
                } else {
                    None
                };
                AlterColumnOperation::SetDataType { data_type, using }
            } else {
                return self
                    .expected("SET/DROP NOT NULL, SET DEFAULT, SET DATA TYPE after ALTER COLUMN");
            };
            AlterTableOperation::AlterColumn { column_name, op }
        } else if self.parse_keywords(&[Keyword::REFRESH, Keyword::SCHEMA]) {
            AlterTableOperation::RefreshSchema
        } else if self.parse_keywords(&[Keyword::SWAP, Keyword::WITH]) {
            let target_table = self.parse_object_name()?;
            AlterTableOperation::SwapRenameTable { target_table }
        } else if self.parse_keyword(Keyword::CONNECTOR) {
            let with_options = self.parse_with_properties()?;
            AlterTableOperation::AlterConnectorProps {
                alter_props: with_options,
            }
        } else {
            return self.expected(
                "ADD or RENAME or OWNER TO or SET or RESET or DROP or SWAP or CONNECTOR after ALTER TABLE",
            );
        };
        Ok(Statement::AlterTable {
            name: table_name,
            operation,
        })
    }

    fn parse_rate_limit_value(&mut self) -> ModalResult<i32> {
        if self.expect_keyword(Keyword::TO).is_err() && self.expect_token(&Token::Eq).is_err() {
            return self.expected("TO or = after rate limit");
        }
        if self.parse_keyword(Keyword::DEFAULT) {
            return Ok(-1);
        }
        let s = self.parse_number_value()?;
        if let Ok(n) = s.parse::<i32>() {
            Ok(n)
        } else {
            self.expected("number or DEFAULT")
        }
    }

    pub fn parse_alter_rate_limit(&mut self) -> ModalResult<Option<AlterRateLimit>> {
        for rate_limit_type in [
            AlterRateLimitType::Source,
            AlterRateLimitType::Backfill,
            AlterRateLimitType::Dml,
            AlterRateLimitType::Sink,
        ] {
            if self.parse_word(rate_limit_type.as_str()) {
                let rate_limit = self.parse_rate_limit_value()?;
                return Ok(Some(AlterRateLimit {
                    rate_limit_type,
                    rate_limit,
                }));
            }
        }
        Ok(None)
    }

    pub fn parse_alter_index(&mut self) -> ModalResult<Statement> {
        let index_name = self.parse_object_name()?;
        let operation = if self.parse_keyword(Keyword::RENAME) {
            if self.parse_keyword(Keyword::TO) {
                let index_name = self.parse_object_name()?;
                AlterIndexOperation::RenameIndex { index_name }
            } else {
                return self.expected("TO after RENAME");
            }
        } else if self.parse_keyword(Keyword::SET) {
            if self.parse_keyword(Keyword::PARALLELISM) {
                if self.expect_keyword(Keyword::TO).is_err()
                    && self.expect_token(&Token::Eq).is_err()
                {
                    return self.expected("TO or = after ALTER INDEX SET PARALLELISM");
                }

                let value = self.parse_set_variable()?;

                let deferred = self.parse_keyword(Keyword::DEFERRED);

                AlterIndexOperation::SetParallelism {
                    parallelism: value,
                    deferred,
                }
            } else if self.parse_keyword(Keyword::BACKFILL_PARALLELISM) {
                if self.expect_keyword(Keyword::TO).is_err()
                    && self.expect_token(&Token::Eq).is_err()
                {
                    return self.expected("TO or = after ALTER INDEX SET BACKFILL_PARALLELISM");
                }

                let value = self.parse_set_variable()?;

                let deferred = self.parse_keyword(Keyword::DEFERRED);

                AlterIndexOperation::SetBackfillParallelism {
                    parallelism: value,
                    deferred,
                }
            } else if self.parse_keyword(Keyword::RESOURCE_GROUP) {
                if self.expect_keyword(Keyword::TO).is_err()
                    && self.expect_token(&Token::Eq).is_err()
                {
                    return self.expected("TO or = after ALTER INDEX SET RESOURCE_GROUP");
                }
                let value = self.parse_set_variable()?;
                let deferred = self.parse_keyword(Keyword::DEFERRED);

                AlterIndexOperation::SetResourceGroup {
                    resource_group: Some(value),
                    deferred,
                }
            } else if self.parse_keyword(Keyword::CONFIG) {
                let entries = self.parse_options()?;
                AlterIndexOperation::SetConfig { entries }
            } else {
                return self.expected(
                    "PARALLELISM/BACKFILL_PARALLELISM/RESOURCE_GROUP or CONFIG after SET",
                );
            }
        } else if self.parse_keyword(Keyword::RESET) {
            if self.parse_keyword(Keyword::RESOURCE_GROUP) {
                let deferred = self.parse_keyword(Keyword::DEFERRED);

                AlterIndexOperation::SetResourceGroup {
                    resource_group: None,
                    deferred,
                }
            } else if self.parse_keyword(Keyword::CONFIG) {
                let keys = self.parse_parenthesized_object_name_list()?;
                AlterIndexOperation::ResetConfig { keys }
            } else {
                return self.expected("RESOURCE_GROUP or CONFIG after RESET");
            }
        } else {
            return self.expected("RENAME, SET, or RESET after ALTER INDEX");
        };

        Ok(Statement::AlterIndex {
            name: index_name,
            operation,
        })
    }

    pub fn parse_alter_view(&mut self, materialized: bool) -> ModalResult<Statement> {
        let view_name = self.parse_object_name()?;
        let operation = if self.parse_keyword(Keyword::AS) {
            let query = Box::new(self.parse_query()?);
            AlterViewOperation::AsQuery { query }
        } else if self.parse_keyword(Keyword::RENAME) {
            if self.parse_keyword(Keyword::TO) {
                let view_name = self.parse_object_name()?;
                AlterViewOperation::RenameView { view_name }
            } else {
                return self.expected("TO after RENAME");
            }
        } else if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            let owner_name: Ident = self.parse_identifier()?;
            AlterViewOperation::ChangeOwner {
                new_owner_name: owner_name,
            }
        } else if self.parse_keywords(&[Keyword::SWAP, Keyword::WITH]) {
            let target_view = self.parse_object_name()?;
            AlterViewOperation::SwapRenameView { target_view }
        } else if self.parse_keyword(Keyword::SET) {
            if self.parse_keyword(Keyword::SCHEMA) {
                let schema_name = self.parse_object_name()?;
                AlterViewOperation::SetSchema {
                    new_schema_name: schema_name,
                }
            } else if self.parse_word("STREAMING_ENABLE_UNALIGNED_JOIN") {
                if self.expect_keyword(Keyword::TO).is_err()
                    && self.expect_token(&Token::Eq).is_err()
                {
                    return self
                        .expected("TO or = after ALTER TABLE SET STREAMING_ENABLE_UNALIGNED_JOIN");
                }
                let value = self.parse_boolean()?;
                AlterViewOperation::SetStreamingEnableUnalignedJoin { enable: value }
            } else if self.parse_keyword(Keyword::PARALLELISM) && materialized {
                if self.expect_keyword(Keyword::TO).is_err()
                    && self.expect_token(&Token::Eq).is_err()
                {
                    return self.expected("TO or = after ALTER MATERIALIZED VIEW SET PARALLELISM");
                }

                let value = self.parse_set_variable()?;

                let deferred = self.parse_keyword(Keyword::DEFERRED);

                AlterViewOperation::SetParallelism {
                    parallelism: value,
                    deferred,
                }
            } else if self.parse_keyword(Keyword::BACKFILL_PARALLELISM) && materialized {
                if self.expect_keyword(Keyword::TO).is_err()
                    && self.expect_token(&Token::Eq).is_err()
                {
                    return self.expected(
                        "TO or = after ALTER MATERIALIZED VIEW SET BACKFILL_PARALLELISM",
                    );
                }

                let value = self.parse_set_variable()?;

                let deferred = self.parse_keyword(Keyword::DEFERRED);

                AlterViewOperation::SetBackfillParallelism {
                    parallelism: value,
                    deferred,
                }
            } else if self.parse_keyword(Keyword::RESOURCE_GROUP) && materialized {
                if self.expect_keyword(Keyword::TO).is_err()
                    && self.expect_token(&Token::Eq).is_err()
                {
                    return self
                        .expected("TO or = after ALTER MATERIALIZED VIEW SET RESOURCE_GROUP");
                }
                let value = self.parse_set_variable()?;
                let deferred = self.parse_keyword(Keyword::DEFERRED);

                AlterViewOperation::SetResourceGroup {
                    resource_group: Some(value),
                    deferred,
                }
            } else if let Some(rate_limit) = self.parse_alter_rate_limit()? {
                AlterViewOperation::AlterRateLimit(rate_limit)
            } else if self.parse_keyword(Keyword::CONFIG) && materialized {
                let entries = self.parse_options()?;
                AlterViewOperation::SetConfig { entries }
            } else {
                return self.expected(
                    "SCHEMA/PARALLELISM/BACKFILL_PARALLELISM/BACKFILL_RATE_LIMIT/CONFIG after SET",
                );
            }
        } else if self.parse_keyword(Keyword::RESET) {
            if self.parse_keyword(Keyword::RESOURCE_GROUP) && materialized {
                let deferred = self.parse_keyword(Keyword::DEFERRED);

                AlterViewOperation::SetResourceGroup {
                    resource_group: None,
                    deferred,
                }
            } else if self.parse_keyword(Keyword::CONFIG) && materialized {
                let keys = self.parse_parenthesized_object_name_list()?;
                AlterViewOperation::ResetConfig { keys }
            } else {
                return self.expected("RESOURCE_GROUP or CONFIG after RESET");
            }
        } else {
            return self.expected(&format!(
                "AS, RENAME, OWNER TO, SET, or SWAP after ALTER {}VIEW",
                if materialized { "MATERIALIZED " } else { "" }
            ));
        };

        Ok(Statement::AlterView {
            materialized,
            name: view_name,
            operation,
        })
    }

    pub fn parse_alter_sink(&mut self) -> ModalResult<Statement> {
        let sink_name = self.parse_object_name()?;
        let operation = if self.parse_keyword(Keyword::RENAME) {
            if self.parse_keyword(Keyword::TO) {
                let sink_name = self.parse_object_name()?;
                AlterSinkOperation::RenameSink { sink_name }
            } else {
                return self.expected("TO after RENAME");
            }
        } else if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            let owner_name: Ident = self.parse_identifier()?;
            AlterSinkOperation::ChangeOwner {
                new_owner_name: owner_name,
            }
        } else if self.parse_keyword(Keyword::SET) {
            if self.parse_keyword(Keyword::SCHEMA) {
                let schema_name = self.parse_object_name()?;
                AlterSinkOperation::SetSchema {
                    new_schema_name: schema_name,
                }
            } else if self.parse_word("STREAMING_ENABLE_UNALIGNED_JOIN") {
                self.expect_keyword(Keyword::TO)?;
                let value = self.parse_boolean()?;
                AlterSinkOperation::SetStreamingEnableUnalignedJoin { enable: value }
            } else if self.parse_keyword(Keyword::PARALLELISM) {
                if self.expect_keyword(Keyword::TO).is_err()
                    && self.expect_token(&Token::Eq).is_err()
                {
                    return self.expected("TO or = after ALTER SINK SET PARALLELISM");
                }

                let value = self.parse_set_variable()?;
                let deferred = self.parse_keyword(Keyword::DEFERRED);

                AlterSinkOperation::SetParallelism {
                    parallelism: value,
                    deferred,
                }
            } else if self.parse_keyword(Keyword::BACKFILL_PARALLELISM) {
                if self.expect_keyword(Keyword::TO).is_err()
                    && self.expect_token(&Token::Eq).is_err()
                {
                    return self.expected("TO or = after ALTER SINK SET BACKFILL_PARALLELISM");
                }

                let value = self.parse_set_variable()?;
                let deferred = self.parse_keyword(Keyword::DEFERRED);

                AlterSinkOperation::SetBackfillParallelism {
                    parallelism: value,
                    deferred,
                }
            } else if self.parse_keyword(Keyword::RESOURCE_GROUP) {
                if self.expect_keyword(Keyword::TO).is_err()
                    && self.expect_token(&Token::Eq).is_err()
                {
                    return self.expected("TO or = after ALTER SINK SET RESOURCE_GROUP");
                }
                let value = self.parse_set_variable()?;
                let deferred = self.parse_keyword(Keyword::DEFERRED);

                AlterSinkOperation::SetResourceGroup {
                    resource_group: Some(value),
                    deferred,
                }
            } else if let Some(rate_limit) = self.parse_alter_rate_limit()? {
                AlterSinkOperation::AlterRateLimit(rate_limit)
            } else if self.parse_keyword(Keyword::CONFIG) {
                let entries = self.parse_options()?;
                AlterSinkOperation::SetConfig { entries }
            } else {
                return self.expected(
                    "SCHEMA/PARALLELISM/BACKFILL_PARALLELISM/RESOURCE_GROUP/SINK_RATE_LIMIT/BACKFILL_RATE_LIMIT/STREAMING_ENABLE_UNALIGNED_JOIN/CONFIG after SET",
                );
            }
        } else if self.parse_keyword(Keyword::RESET) {
            if self.parse_keyword(Keyword::RESOURCE_GROUP) {
                let deferred = self.parse_keyword(Keyword::DEFERRED);

                AlterSinkOperation::SetResourceGroup {
                    resource_group: None,
                    deferred,
                }
            } else if self.parse_keyword(Keyword::CONFIG) {
                let keys = self.parse_parenthesized_object_name_list()?;
                AlterSinkOperation::ResetConfig { keys }
            } else {
                return self.expected("RESOURCE_GROUP or CONFIG after RESET");
            }
        } else if self.parse_keywords(&[Keyword::SWAP, Keyword::WITH]) {
            let target_sink = self.parse_object_name()?;
            AlterSinkOperation::SwapRenameSink { target_sink }
        } else if self.parse_keyword(Keyword::CONNECTOR) {
            let changed_props = self.parse_with_properties()?;
            AlterSinkOperation::AlterConnectorProps {
                alter_props: changed_props,
            }
        } else {
            return self
                .expected("RENAME or OWNER TO or SET or RESET or CONNECTOR WITH after ALTER SINK");
        };

        Ok(Statement::AlterSink {
            name: sink_name,
            operation,
        })
    }

    pub fn parse_alter_subscription(&mut self) -> ModalResult<Statement> {
        let subscription_name = self.parse_object_name()?;
        let operation = if self.parse_keyword(Keyword::RENAME) {
            if self.parse_keyword(Keyword::TO) {
                let subscription_name = self.parse_object_name()?;
                AlterSubscriptionOperation::RenameSubscription { subscription_name }
            } else {
                return self.expected("TO after RENAME");
            }
        } else if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            let owner_name: Ident = self.parse_identifier()?;
            AlterSubscriptionOperation::ChangeOwner {
                new_owner_name: owner_name,
            }
        } else if self.parse_keyword(Keyword::SET) {
            if self.parse_keyword(Keyword::SCHEMA) {
                let schema_name = self.parse_object_name()?;
                AlterSubscriptionOperation::SetSchema {
                    new_schema_name: schema_name,
                }
            } else if self.parse_keyword(Keyword::RETENTION) {
                if self.expect_keyword(Keyword::TO).is_err()
                    && self.expect_token(&Token::Eq).is_err()
                {
                    return self.expected("TO or = after ALTER SUBSCRIPTION SET RETENTION");
                }
                let retention = self.ensure_parse_value()?;
                AlterSubscriptionOperation::SetRetention { retention }
            } else {
                return self.expected("SCHEMA or RETENTION after SET");
            }
        } else if self.parse_keywords(&[Keyword::SWAP, Keyword::WITH]) {
            let target_subscription = self.parse_object_name()?;
            AlterSubscriptionOperation::SwapRenameSubscription {
                target_subscription,
            }
        } else {
            return self.expected("RENAME or OWNER TO or SET or SWAP after ALTER SUBSCRIPTION");
        };

        Ok(Statement::AlterSubscription {
            name: subscription_name,
            operation,
        })
    }

    pub fn parse_alter_source(&mut self) -> ModalResult<Statement> {
        let source_name = self.parse_object_name()?;
        let operation = if self.parse_keyword(Keyword::RENAME) {
            if self.parse_keyword(Keyword::TO) {
                let source_name = self.parse_object_name()?;
                AlterSourceOperation::RenameSource { source_name }
            } else {
                return self.expected("TO after RENAME");
            }
        } else if self.parse_keyword(Keyword::ADD) {
            let _ = self.parse_keyword(Keyword::COLUMN);
            let _if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
            let column_def = self.parse_column_def()?;
            AlterSourceOperation::AddColumn { column_def }
        } else if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            let owner_name: Ident = self.parse_identifier()?;
            AlterSourceOperation::ChangeOwner {
                new_owner_name: owner_name,
            }
        } else if self.parse_keyword(Keyword::SET) {
            if self.parse_keyword(Keyword::SCHEMA) {
                let schema_name = self.parse_object_name()?;
                AlterSourceOperation::SetSchema {
                    new_schema_name: schema_name,
                }
            } else if let Some(rate_limit) = self.parse_alter_rate_limit()? {
                AlterSourceOperation::AlterRateLimit(rate_limit)
            } else if self.parse_keyword(Keyword::PARALLELISM) {
                if self.expect_keyword(Keyword::TO).is_err()
                    && self.expect_token(&Token::Eq).is_err()
                {
                    return self.expected("TO or = after ALTER SOURCE SET PARALLELISM");
                }

                let value = self.parse_set_variable()?;
                let deferred = self.parse_keyword(Keyword::DEFERRED);

                AlterSourceOperation::SetParallelism {
                    parallelism: value,
                    deferred,
                }
            } else if self.parse_keyword(Keyword::BACKFILL_PARALLELISM) {
                if self.expect_keyword(Keyword::TO).is_err()
                    && self.expect_token(&Token::Eq).is_err()
                {
                    return self.expected("TO or = after ALTER SOURCE SET BACKFILL_PARALLELISM");
                }

                let value = self.parse_set_variable()?;
                let deferred = self.parse_keyword(Keyword::DEFERRED);

                AlterSourceOperation::SetBackfillParallelism {
                    parallelism: value,
                    deferred,
                }
            } else if self.parse_keyword(Keyword::CONFIG) {
                let entries = self.parse_options()?;
                AlterSourceOperation::SetConfig { entries }
            } else {
                return self.expected(
                    "SCHEMA, SOURCE_RATE_LIMIT, PARALLELISM, BACKFILL_PARALLELISM or CONFIG after SET",
                );
            }
        } else if self.parse_keyword(Keyword::RESET) {
            if self.parse_keyword(Keyword::CONFIG) {
                let keys = self.parse_parenthesized_object_name_list()?;
                AlterSourceOperation::ResetConfig { keys }
            } else {
                // RESET without CONFIG means reset CDC source offset to latest
                AlterSourceOperation::ResetSource
            }
        } else if self.peek_nth_any_of_keywords(0, &[Keyword::FORMAT]) {
            let format_encode = self.parse_schema()?.unwrap();
            if format_encode.key_encode.is_some() {
                parser_err!("key encode clause is not supported in source schema");
            }
            AlterSourceOperation::FormatEncode { format_encode }
        } else if self.parse_keywords(&[Keyword::REFRESH, Keyword::SCHEMA]) {
            AlterSourceOperation::RefreshSchema
        } else if self.parse_keywords(&[Keyword::SWAP, Keyword::WITH]) {
            let target_source = self.parse_object_name()?;
            AlterSourceOperation::SwapRenameSource { target_source }
        } else if self.parse_keyword(Keyword::CONNECTOR) {
            let with_options = self.parse_with_properties()?;
            AlterSourceOperation::AlterConnectorProps {
                alter_props: with_options,
            }
        } else {
            return self.expected(
                "RENAME, ADD COLUMN, OWNER TO, CONNECTOR, SET or RESET after ALTER SOURCE",
            );
        };

        Ok(Statement::AlterSource {
            name: source_name,
            operation,
        })
    }

    pub fn parse_alter_function(&mut self) -> ModalResult<Statement> {
        let FunctionDesc { name, args } = self.parse_function_desc()?;

        let operation = if self.parse_keyword(Keyword::SET) {
            if self.parse_keyword(Keyword::SCHEMA) {
                let schema_name = self.parse_object_name()?;
                AlterFunctionOperation::SetSchema {
                    new_schema_name: schema_name,
                }
            } else {
                return self.expected("SCHEMA after SET");
            }
        } else if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            let owner_name: Ident = self.parse_identifier()?;
            AlterFunctionOperation::ChangeOwner {
                new_owner_name: owner_name,
            }
        } else {
            return self.expected("SET or OWNER TO after ALTER FUNCTION");
        };

        Ok(Statement::AlterFunction {
            name,
            args,
            operation,
        })
    }

    pub fn parse_alter_connection(&mut self) -> ModalResult<Statement> {
        let connection_name = self.parse_object_name()?;
        let operation = if self.parse_keyword(Keyword::SET) {
            if self.parse_keyword(Keyword::SCHEMA) {
                let schema_name = self.parse_object_name()?;
                AlterConnectionOperation::SetSchema {
                    new_schema_name: schema_name,
                }
            } else {
                return self.expected("SCHEMA after SET");
            }
        } else if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            let owner_name: Ident = self.parse_identifier()?;
            AlterConnectionOperation::ChangeOwner {
                new_owner_name: owner_name,
            }
        } else if self.parse_keyword(Keyword::CONNECTOR) {
            let with_options = self.parse_with_properties()?;
            AlterConnectionOperation::AlterConnectorProps {
                alter_props: with_options,
            }
        } else {
            return self.expected("SET, OWNER TO, or CONNECTOR WITH after ALTER CONNECTION");
        };

        Ok(Statement::AlterConnection {
            name: connection_name,
            operation,
        })
    }

    pub fn parse_alter_system(&mut self) -> ModalResult<Statement> {
        if self.parse_word("CLEAR") {
            self.expect_keywords(&[Keyword::FILE, Keyword::CACHE])?;
            let cache_type = if self.parse_keyword(Keyword::META) {
                FileCacheType::Meta
            } else if self.parse_keyword(Keyword::DATA) {
                FileCacheType::Data
            } else if self.parse_keyword(Keyword::ALL) {
                FileCacheType::All
            } else {
                return self.expected("META, DATA, or ALL after ALTER SYSTEM CLEAR FILE CACHE");
            };
            return Ok(Statement::AlterSystemClearFileCache { cache_type });
        }

        self.expect_keyword(Keyword::SET)?;
        let param = self.parse_identifier()?;
        if self.expect_keyword(Keyword::TO).is_err() && self.expect_token(&Token::Eq).is_err() {
            return self.expected("TO or = after ALTER SYSTEM SET");
        }
        let value = self.parse_set_variable()?;
        Ok(Statement::AlterSystem { param, value })
    }

    pub fn parse_alter_secret(&mut self) -> ModalResult<Statement> {
        let secret_name = self.parse_object_name()?;
        let operation = if self.parse_keyword(Keyword::WITH) {
            let with_options = self.parse_options()?;
            if self.parse_keyword(Keyword::AS) {
                let new_credential = self.ensure_parse_value()?;
                AlterSecretOperation::ChangeCredential {
                    with_options,
                    new_credential,
                }
            } else {
                return self.expected("Keyword AS after Options");
            }
        } else if self.parse_keyword(Keyword::AS) {
            let new_credential = self.ensure_parse_value()?;
            AlterSecretOperation::ChangeCredential {
                with_options: vec![],
                new_credential,
            }
        } else if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            let owner_name: Ident = self.parse_identifier()?;
            AlterSecretOperation::ChangeOwner {
                new_owner_name: owner_name,
            }
        } else {
            return self.expected("WITH, AS or OWNER TO after ALTER SECRET");
        };
        Ok(Statement::AlterSecret {
            name: secret_name,
            operation,
        })
    }

    pub fn parse_alter_fragment(&mut self) -> ModalResult<Statement> {
        let mut fragment_ids = vec![self.parse_literal_u32()?];
        while self.consume_token(&Token::Comma) {
            fragment_ids.push(self.parse_literal_u32()?);
        }
        if !self.parse_keyword(Keyword::SET) {
            return self.expected("SET after ALTER FRAGMENT");
        }
        let operation = if self.parse_keyword(Keyword::PARALLELISM) {
            if self.expect_keyword(Keyword::TO).is_err() && self.expect_token(&Token::Eq).is_err() {
                return self.expected("TO or = after ALTER FRAGMENT SET PARALLELISM");
            }
            let parallelism = self.parse_set_variable()?;
            AlterFragmentOperation::SetParallelism { parallelism }
        } else {
            let rate_limit = self.parse_alter_fragment_rate_limit()?;
            AlterFragmentOperation::AlterRateLimit(rate_limit)
        };
        Ok(Statement::AlterFragment {
            fragment_ids,
            operation,
        })
    }

    pub fn parse_alter_compaction_group(&mut self) -> ModalResult<Statement> {
        if !self.parse_keyword(Keyword::GROUP) {
            return self.expected("GROUP after ALTER COMPACTION");
        }
        let mut group_ids = vec![self.parse_literal_u64()?];
        while self.consume_token(&Token::Comma) {
            group_ids.push(self.parse_literal_u64()?);
        }
        if !self.parse_keyword(Keyword::SET) {
            return self.expected("SET after ALTER COMPACTION GROUP <id>");
        }
        // NOTE: use the `no_list` variant here, because `parse_set_variable` allows comma-separated
        // lists (e.g., `SET foo = 1,2,3`), which would conflict with our use of comma to separate
        // multiple config assignments.
        let configs = self.parse_comma_separated(Parser::parse_config_param_no_list)?;
        let operation = AlterCompactionGroupOperation::Set { configs };
        Ok(Statement::AlterCompactionGroup {
            group_ids,
            operation,
        })
    }

    fn parse_alter_fragment_rate_limit(&mut self) -> ModalResult<AlterRateLimit> {
        if self.parse_word("RATE_LIMIT") {
            let rate_limit = self.parse_rate_limit_value()?;
            return Ok(AlterRateLimit {
                rate_limit_type: AlterRateLimitType::Backfill,
                rate_limit,
            });
        }
        if let Some(rate_limit) = self.parse_alter_rate_limit()? {
            Ok(rate_limit)
        } else {
            self.expected("expected rate limit after SET")
        }
    }

    /// Parse a copy statement
    pub fn parse_copy(&mut self) -> ModalResult<Statement> {
        let entity = if self.consume_token(&Token::LParen) {
            let query = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            CopyEntity::Query(query.into())
        } else {
            let table_name = self.parse_object_name()?;
            let columns = self.parse_parenthesized_column_list(Optional)?;
            CopyEntity::Table {
                table_name,
                columns,
            }
        };

        let target = if self.parse_keywords(&[Keyword::FROM, Keyword::STDIN]) {
            self.expect_token(&Token::SemiColon)?;
            let values = self.parse_tsv();
            CopyTarget::Stdin { values }
        } else if self.parse_keywords(&[Keyword::TO, Keyword::STDOUT]) {
            CopyTarget::Stdout
        } else {
            return self.expected("FROM STDIN or TO STDOUT");
        };

        Ok(Statement::Copy { entity, target })
    }

    /// Parse a tab separated values in
    /// COPY payload
    fn parse_tsv(&mut self) -> Vec<Option<String>> {
        self.parse_tab_value()
    }

    fn parse_tab_value(&mut self) -> Vec<Option<String>> {
        let mut values = vec![];
        let mut content = String::from("");
        while let Some(t) = self.next_token_no_skip() {
            match t.token {
                Token::Whitespace(Whitespace::Tab) => {
                    values.push(Some(content.clone()));
                    content.clear();
                }
                Token::Whitespace(Whitespace::Newline) => {
                    values.push(Some(content.clone()));
                    content.clear();
                }
                Token::Backslash => {
                    if self.consume_token(&Token::Period) {
                        return values;
                    }
                    if let Token::Word(w) = self.next_token().token
                        && w.value == "N"
                    {
                        values.push(None);
                    }
                }
                _ => {
                    content.push_str(&t.to_string());
                }
            }
        }
        values
    }

    pub fn ensure_parse_value(&mut self) -> ModalResult<Value> {
        match self.parse_value_and_obj_ref::<true>()? {
            SqlOptionValue::Value(value) => Ok(value),
            SqlOptionValue::SecretRef(_)
            | SqlOptionValue::ConnectionRef(_)
            | SqlOptionValue::BackfillOrder(_) => unreachable!(),
        }
    }

    /// Parse a literal value (numbers, strings, date/time, booleans)
    pub fn parse_value_and_obj_ref<const FORBID_OBJ_REF: bool>(
        &mut self,
    ) -> ModalResult<SqlOptionValue> {
        let checkpoint = *self;
        let token = self.next_token();
        match token.token {
            Token::Word(w) => match w.keyword {
                Keyword::TRUE => Ok(Value::Boolean(true).into()),
                Keyword::FALSE => Ok(Value::Boolean(false).into()),
                Keyword::NULL => Ok(Value::Null.into()),
                Keyword::NoKeyword if w.quote_style.is_some() => match w.quote_style {
                    Some('"') => Ok(Value::DoubleQuotedString(w.value).into()),
                    Some('\'') => Ok(Value::SingleQuotedString(w.value).into()),
                    _ => self.expected_at(checkpoint, "A value")?,
                },
                Keyword::SECRET => {
                    if FORBID_OBJ_REF {
                        return self.expected_at(
                            checkpoint,
                            "a concrete value rather than a secret reference",
                        );
                    }
                    let secret = self.parse_secret_ref()?;
                    Ok(SqlOptionValue::SecretRef(secret))
                }
                _ => self.expected_at(checkpoint, "a concrete value"),
            },
            Token::Number(ref n) => Ok(Value::Number(n.clone()).into()),
            Token::SingleQuotedString(ref s) => Ok(Value::SingleQuotedString(s.clone()).into()),
            Token::DollarQuotedString(ref s) => Ok(Value::DollarQuotedString(s.clone()).into()),
            Token::CstyleEscapesString(ref s) => Ok(Value::CstyleEscapedString(s.clone()).into()),
            Token::NationalStringLiteral(ref s) => {
                Ok(Value::NationalStringLiteral(s.clone()).into())
            }
            Token::HexStringLiteral(ref s) => Ok(Value::HexStringLiteral(s.clone()).into()),
            _ => self.expected_at(checkpoint, "a value"),
        }
    }

    fn parse_secret_ref(&mut self) -> ModalResult<SecretRefValue> {
        let secret_name = self.parse_object_name()?;
        let ref_as = if self.parse_keywords(&[Keyword::AS, Keyword::FILE]) {
            SecretRefAsType::File
        } else {
            SecretRefAsType::Text
        };
        Ok(SecretRefValue {
            secret_name,
            ref_as,
        })
    }

    fn parse_set_variable(&mut self) -> ModalResult<SetVariableValue> {
        alt((
            Keyword::DEFAULT.value(SetVariableValue::Default),
            separated(
                1..,
                alt((
                    Self::ensure_parse_value.map(SetVariableValueSingle::Literal),
                    |parser: &mut Self| {
                        let checkpoint = *parser;
                        let ident = parser.parse_identifier()?;
                        if parser.consume_token(&Token::LParen) {
                            let args = parser.parse_comma_separated(Parser::ensure_parse_value)?;
                            parser.expect_token(&Token::RParen)?;
                            let raw = format!(
                                "{}({})",
                                ident,
                                args.iter().map(ToString::to_string).join(", ")
                            );
                            return Ok(SetVariableValueSingle::Raw(raw));
                        }
                        if ident.value == "default" {
                            *parser = checkpoint;
                            return parser.expected("parameter list value").map_err(|e| e.cut());
                        }
                        Ok(SetVariableValueSingle::Ident(ident))
                    },
                    fail.expect("parameter value"),
                )),
                Token::Comma,
            )
            .map(|list: Vec<SetVariableValueSingle>| {
                if list.len() == 1 {
                    SetVariableValue::Single(list[0].clone())
                } else {
                    SetVariableValue::List(list)
                }
            }),
        ))
        .parse_next(self)
    }

    fn parse_backfill_order_strategy(&mut self) -> ModalResult<BackfillOrderStrategy> {
        alt((
            Keyword::DEFAULT.value(BackfillOrderStrategy::Default),
            Keyword::NONE.value(BackfillOrderStrategy::None),
            Keyword::AUTO.value(BackfillOrderStrategy::Auto),
            Self::parse_fixed_backfill_order.map(BackfillOrderStrategy::Fixed),
            fail.expect("backfill order strategy"),
        ))
        .parse_next(self)
    }

    fn parse_fixed_backfill_order(&mut self) -> ModalResult<Vec<(ObjectName, ObjectName)>> {
        self.expect_word("FIXED")?;
        self.expect_token(&Token::LParen)?;
        let edges = separated(
            0..,
            separated_pair(
                Self::parse_object_name,
                Token::Op("->".to_owned()),
                Self::parse_object_name,
            ),
            Token::Comma,
        )
        .parse_next(self)?;
        self.expect_token(&Token::RParen)?;
        Ok(edges)
    }

    pub fn parse_number_value(&mut self) -> ModalResult<String> {
        let checkpoint = *self;
        match self.ensure_parse_value()? {
            Value::Number(v) => Ok(v),
            _ => self.expected_at(checkpoint, "literal number"),
        }
    }

    pub fn parse_literal_u32(&mut self) -> ModalResult<u32> {
        literal_u32(self)
    }

    pub fn parse_literal_u64(&mut self) -> ModalResult<u64> {
        literal_u64(self)
    }

    pub fn parse_function_definition(&mut self) -> ModalResult<FunctionDefinition> {
        alt((
            single_quoted_string.map(FunctionDefinition::SingleQuotedDef),
            dollar_quoted_string.map(FunctionDefinition::DoubleDollarDef),
            Self::parse_identifier.map(|i| FunctionDefinition::Identifier(i.value)),
            fail.expect("function definition"),
        ))
        .parse_next(self)
    }

    /// Parse a literal string
    pub fn parse_literal_string(&mut self) -> ModalResult<String> {
        let checkpoint = *self;
        let token = self.next_token();
        match token.token {
            Token::SingleQuotedString(s) => Ok(s),
            Token::DollarQuotedString(s) => Ok(s.value),
            _ => self.expected_at(checkpoint, "literal string"),
        }
    }

    /// Parse a SQL datatype (in the context of a CREATE TABLE statement for example)
    pub fn parse_data_type(&mut self) -> ModalResult<DataType> {
        parser_v2::data_type(self)
    }

    /// Parse `AS identifier` (or simply `identifier` if it's not a reserved keyword)
    /// Some examples with aliases: `SELECT 1 foo`, `SELECT COUNT(*) AS cnt`,
    /// `SELECT ... FROM t1 foo, t2 bar`, `SELECT ... FROM (...) AS bar`
    pub fn parse_optional_alias(
        &mut self,
        reserved_kwds: &[Keyword],
    ) -> ModalResult<Option<Ident>> {
        let after_as = self.parse_keyword(Keyword::AS);
        let checkpoint = *self;
        let token = self.next_token();
        match token.token {
            // Accept any identifier after `AS` (though many dialects have restrictions on
            // keywords that may appear here). If there's no `AS`: don't parse keywords,
            // which may start a construct allowed in this position, to be parsed as aliases.
            // (For example, in `FROM t1 JOIN` the `JOIN` will always be parsed as a keyword,
            // not an alias.)
            Token::Word(w) if after_as || (!reserved_kwds.contains(&w.keyword)) => {
                Ok(Some(w.to_ident()?))
            }
            _ => {
                *self = checkpoint;
                if after_as {
                    return self.expected("an identifier after AS");
                }
                Ok(None) // no alias found
            }
        }
    }

    /// Parse `AS identifier` when the AS is describing a table-valued object,
    /// like in `... FROM generate_series(1, 10) AS t (col)`. In this case
    /// the alias is allowed to optionally name the columns in the table, in
    /// addition to the table itself.
    pub fn parse_optional_table_alias(
        &mut self,
        reserved_kwds: &[Keyword],
    ) -> ModalResult<Option<TableAlias>> {
        if self.peek_broadcast_join() {
            return Ok(None);
        }
        match self.parse_optional_alias(reserved_kwds)? {
            Some(name) => {
                let columns = self.parse_parenthesized_column_list(Optional)?;
                Ok(Some(TableAlias { name, columns }))
            }
            None => Ok(None),
        }
    }

    /// syntax `FOR SYSTEM_TIME AS OF PROCTIME()` is used for temporal join.
    pub fn parse_as_of(&mut self) -> ModalResult<AsOf> {
        Keyword::FOR.parse_next(self)?;
        alt((
            preceded(
                (Keyword::SYSTEM_TIME, Keyword::AS, Keyword::OF),
                cut_err(
                    alt((
                        preceded(
                            (
                                Self::parse_identifier.verify(|ident| ident.real_value() == "now"),
                                cut_err(Token::LParen),
                                cut_err(Token::RParen),
                                Token::Minus,
                            ),
                            Self::parse_literal_interval.try_map(|e| match e {
                                Expr::Value(v) => match v {
                                    Value::Interval {
                                        value,
                                        leading_field,
                                        ..
                                    } => {
                                        let Some(leading_field) = leading_field else {
                                            return Err(StrError("expect duration unit".into()));
                                        };
                                        Ok(AsOf::ProcessTimeWithInterval((value, leading_field)))
                                    }
                                    _ => Err(StrError("expect Value::Interval".into())),
                                },
                                _ => Err(StrError("expect Expr::Value".into())),
                            }),
                        ),
                        (
                            Self::parse_identifier.verify(|ident| ident.real_value() == "now"),
                            cut_err(Token::LParen),
                            cut_err(Token::RParen),
                        )
                            .value(AsOf::ProcessTimeWithInterval((
                                "0".to_owned(),
                                DateTimeField::Second,
                            ))),
                        (
                            Self::parse_identifier.verify(|ident| ident.real_value() == "proctime"),
                            cut_err(Token::LParen),
                            cut_err(Token::RParen),
                        )
                            .value(AsOf::ProcessTime),
                        literal_i64.map(AsOf::TimestampNum),
                        single_quoted_string.map(AsOf::TimestampString),
                    ))
                    .expect("proctime(), now(), number or string"),
                ),
            ),
            preceded(
                (Keyword::SYSTEM_VERSION, Keyword::AS, Keyword::OF),
                cut_err(
                    alt((
                        literal_i64.map(AsOf::VersionNum),
                        single_quoted_string.map(AsOf::VersionString),
                    ))
                    .expect("number or string"),
                ),
            ),
        ))
        .parse_next(self)
    }

    /// Parse a possibly qualified, possibly quoted identifier, e.g.
    /// `foo` or `myschema."table"
    pub fn parse_object_name(&mut self) -> ModalResult<ObjectName> {
        let mut idents = vec![];
        loop {
            idents.push(self.parse_identifier()?);
            if !self.consume_token(&Token::Period) {
                break;
            }
        }
        Ok(ObjectName(idents))
    }

    /// Parse a parenthesized comma-separated list of object names
    pub fn parse_parenthesized_object_name_list(&mut self) -> ModalResult<Vec<ObjectName>> {
        if self.consume_token(&Token::LParen) {
            let names = self.parse_comma_separated(Parser::parse_object_name)?;
            self.expect_token(&Token::RParen)?;
            Ok(names)
        } else {
            self.expected("a list of object names in parentheses")
        }
    }

    /// Parse identifiers strictly i.e. don't parse keywords
    pub fn parse_identifiers_non_keywords(&mut self) -> ModalResult<Vec<Ident>> {
        let mut idents = vec![];
        loop {
            match self.peek_token().token {
                Token::Word(w) => {
                    if w.keyword != Keyword::NoKeyword {
                        break;
                    }

                    idents.push(w.to_ident()?);
                }
                Token::EOF | Token::Eq => break,
                _ => {}
            }

            self.next_token();
        }

        Ok(idents)
    }

    /// Parse identifiers
    pub fn parse_identifiers(&mut self) -> ModalResult<Vec<Ident>> {
        let mut idents = vec![];
        loop {
            let token = self.next_token();
            match token.token {
                Token::Word(w) => {
                    idents.push(w.to_ident()?);
                }
                Token::EOF => break,
                _ => {}
            }
        }

        Ok(idents)
    }

    /// Parse a simple one-word identifier (possibly quoted, possibly a keyword)
    pub fn parse_identifier(&mut self) -> ModalResult<Ident> {
        let checkpoint = *self;
        let token = self.next_token();
        match token.token {
            Token::Word(w) => Ok(w.to_ident()?),
            _ => self.expected_at(checkpoint, "identifier"),
        }
    }

    /// Parse a simple one-word identifier (possibly quoted, possibly a non-reserved keyword)
    pub fn parse_identifier_non_reserved(&mut self) -> ModalResult<Ident> {
        let checkpoint = *self;
        let token = self.next_token();
        match token.token {
            Token::Word(w) => {
                match keywords::RESERVED_FOR_COLUMN_OR_TABLE_NAME.contains(&w.keyword) {
                    true => parser_err!("syntax error at or near {w}"),
                    false => Ok(w.to_ident()?),
                }
            }
            _ => self.expected_at(checkpoint, "identifier"),
        }
    }

    /// Parse a parenthesized comma-separated list of unqualified, possibly quoted identifiers
    pub fn parse_parenthesized_column_list(
        &mut self,
        optional: IsOptional,
    ) -> ModalResult<Vec<Ident>> {
        if self.consume_token(&Token::LParen) {
            let cols = self.parse_comma_separated(Parser::parse_identifier_non_reserved)?;
            self.expect_token(&Token::RParen)?;
            Ok(cols)
        } else if optional == Optional {
            Ok(vec![])
        } else {
            self.expected("a list of columns in parentheses")
        }
    }

    pub fn parse_returning(&mut self, optional: IsOptional) -> ModalResult<Vec<SelectItem>> {
        if self.parse_keyword(Keyword::RETURNING) {
            let cols = self.parse_comma_separated(Parser::parse_select_item)?;
            Ok(cols)
        } else if optional == Optional {
            Ok(vec![])
        } else {
            self.expected("a list of columns or * after returning")
        }
    }

    pub fn parse_row_expr(&mut self) -> ModalResult<Expr> {
        Ok(Expr::Row(self.parse_token_wrapped_exprs(
            &Token::LParen,
            &Token::RParen,
        )?))
    }

    /// Parse a comma-separated list (maybe empty) from a wrapped expression
    pub fn parse_token_wrapped_exprs(
        &mut self,
        left: &Token,
        right: &Token,
    ) -> ModalResult<Vec<Expr>> {
        if self.consume_token(left) {
            let exprs = if self.consume_token(right) {
                vec![]
            } else {
                let exprs = self.parse_comma_separated(Parser::parse_expr)?;
                self.expect_token(right)?;
                exprs
            };
            Ok(exprs)
        } else {
            self.expected(left.to_string().as_str())
        }
    }

    pub fn parse_optional_precision(&mut self) -> ModalResult<Option<u64>> {
        if self.consume_token(&Token::LParen) {
            let n = self.parse_literal_u64()?;
            self.expect_token(&Token::RParen)?;
            Ok(Some(n))
        } else {
            Ok(None)
        }
    }

    pub fn parse_optional_precision_scale(&mut self) -> ModalResult<(Option<u64>, Option<u64>)> {
        if self.consume_token(&Token::LParen) {
            let n = self.parse_literal_u64()?;
            let scale = if self.consume_token(&Token::Comma) {
                Some(self.parse_literal_u64()?)
            } else {
                None
            };
            self.expect_token(&Token::RParen)?;
            Ok((Some(n), scale))
        } else {
            Ok((None, None))
        }
    }

    pub fn parse_delete(&mut self) -> ModalResult<Statement> {
        if self.parse_keyword(Keyword::META) {
            let Some(_) = self.parse_one_of_keywords(&[Keyword::SNAPSHOT, Keyword::SNAPSHOTS])
            else {
                return self.expected("SNAPSHOT or SNAPSHOTS");
            };
            let snapshot_ids = self.parse_comma_separated(Parser::parse_literal_u64)?;
            return Ok(Statement::DeleteMetaSnapshots { snapshot_ids });
        }

        self.expect_keyword(Keyword::FROM)?;
        let table_name = self.parse_object_name()?;
        let selection = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let returning = self.parse_returning(Optional)?;

        Ok(Statement::Delete {
            table_name,
            selection,
            returning,
        })
    }

    pub fn parse_boolean(&mut self) -> ModalResult<bool> {
        if let Some(keyword) = self.parse_one_of_keywords(&[Keyword::TRUE, Keyword::FALSE]) {
            match keyword {
                Keyword::TRUE => Ok(true),
                Keyword::FALSE => Ok(false),
                _ => unreachable!(),
            }
        } else {
            self.expected("TRUE or FALSE")
        }
    }

    pub fn parse_optional_boolean(&mut self, default: bool) -> bool {
        self.parse_boolean().unwrap_or(default)
    }

    fn parse_explain_options(&mut self) -> ModalResult<(ExplainOptions, Option<u64>)> {
        let mut options = ExplainOptions::default();
        let mut analyze_duration = None;

        const BACKFILL: &str = "backfill";
        const VERBOSE: &str = "verbose";
        const TRACE: &str = "trace";
        const TYPE: &str = "type";
        const LOGICAL: &str = "logical";
        const PHYSICAL: &str = "physical";
        const DISTSQL: &str = "distsql";
        const FORMAT: &str = "format";
        const DURATION_SECS: &str = "duration_secs";

        let explain_options_identifiers = [
            BACKFILL,
            VERBOSE,
            TRACE,
            TYPE,
            LOGICAL,
            PHYSICAL,
            DISTSQL,
            FORMAT,
            DURATION_SECS,
        ];

        let parse_explain_option = |parser: &mut Parser<'_>| -> ModalResult<()> {
            match parser.parse_identifier()?.real_value().as_str() {
                VERBOSE => options.verbose = parser.parse_optional_boolean(true),
                TRACE => options.trace = parser.parse_optional_boolean(true),
                BACKFILL => options.backfill = parser.parse_optional_boolean(true),
                TYPE => {
                    let explain_type = parser.parse_identifier()?.real_value();
                    match explain_type.as_str() {
                        LOGICAL => options.explain_type = ExplainType::Logical,
                        PHYSICAL => options.explain_type = ExplainType::Physical,
                        DISTSQL => options.explain_type = ExplainType::DistSql,
                        unexpected => {
                            parser_err!("unexpected explain type: [{unexpected}]")
                        }
                    }
                }
                LOGICAL => options.explain_type = ExplainType::Logical,
                PHYSICAL => options.explain_type = ExplainType::Physical,
                DISTSQL => options.explain_type = ExplainType::DistSql,
                FORMAT => {
                    options.explain_format = {
                        let format = parser.parse_identifier()?.real_value();
                        match format.as_str() {
                            "text" => ExplainFormat::Text,
                            "json" => ExplainFormat::Json,
                            "xml" => ExplainFormat::Xml,
                            "yaml" => ExplainFormat::Yaml,
                            "dot" => ExplainFormat::Dot,
                            unexpected => {
                                parser_err!("unexpected explain format [{unexpected}]")
                            }
                        }
                    }
                }
                DURATION_SECS => {
                    analyze_duration = Some(parser.parse_literal_u64()?);
                }
                unexpected => {
                    parser_err!("unexpected explain options: [{unexpected}]")
                }
            };
            Ok(())
        };

        // In order to support following statement, we need to peek before consume.
        // explain (select 1) union (select 1)
        if self.peek_token() == Token::LParen
            && let Token::Word(word) = self.peek_nth_token(1).token
            && let Ok(ident) = word.to_ident()
            && explain_options_identifiers.contains(&ident.real_value().as_str())
        {
            assert!(self.consume_token(&Token::LParen));
            self.parse_comma_separated(parse_explain_option)?;
            self.expect_token(&Token::RParen)?;
        }

        Ok((options, analyze_duration))
    }

    pub fn parse_explain(&mut self) -> ModalResult<Statement> {
        let analyze = self.parse_keyword(Keyword::ANALYZE);
        let (options, analyze_duration) = self.parse_explain_options()?;

        if analyze {
            fn parse_analyze_target(parser: &mut Parser<'_>) -> ModalResult<Option<AnalyzeTarget>> {
                if parser.parse_keyword(Keyword::TABLE) {
                    let table_name = parser.parse_object_name()?;
                    Ok(Some(AnalyzeTarget::Table(table_name)))
                } else if parser.parse_keyword(Keyword::INDEX) {
                    let index_name = parser.parse_object_name()?;
                    Ok(Some(AnalyzeTarget::Index(index_name)))
                } else if parser.parse_keywords(&[Keyword::MATERIALIZED, Keyword::VIEW]) {
                    let view_name = parser.parse_object_name()?;
                    Ok(Some(AnalyzeTarget::MaterializedView(view_name)))
                } else if parser.parse_keyword(Keyword::INDEX) {
                    let index_name = parser.parse_object_name()?;
                    Ok(Some(AnalyzeTarget::Index(index_name)))
                } else if parser.parse_keyword(Keyword::SINK) {
                    let sink_name = parser.parse_object_name()?;
                    Ok(Some(AnalyzeTarget::Sink(sink_name)))
                } else if parser.parse_word("ID") {
                    let job_id = parser.parse_literal_u32()?;
                    Ok(Some(AnalyzeTarget::Id(job_id)))
                } else {
                    Ok(None)
                }
            }
            if let Some(target) = parse_analyze_target(self)? {
                let statement = Statement::ExplainAnalyzeStreamJob {
                    target,
                    duration_secs: analyze_duration,
                };
                return Ok(statement);
            }
        }

        let statement = match self.parse_statement() {
            Ok(statement) => statement,
            error @ Err(_) => {
                return if analyze {
                    self.expected_at(
                        *self,
                        "SINK, TABLE, MATERIALIZED VIEW, INDEX or a statement after ANALYZE",
                    )
                } else {
                    error
                };
            }
        };
        Ok(Statement::Explain {
            analyze,
            statement: Box::new(statement),
            options,
        })
    }

    pub fn parse_describe(&mut self) -> ModalResult<Statement> {
        let kind = match self.parse_one_of_keywords(&[Keyword::FRAGMENT, Keyword::FRAGMENTS]) {
            Some(Keyword::FRAGMENT) => {
                let fragment_id = self.parse_literal_u32()?;
                return Ok(Statement::DescribeFragment { fragment_id });
            }
            Some(Keyword::FRAGMENTS) => DescribeKind::Fragments,
            None => DescribeKind::Plain,
            Some(_) => unreachable!(),
        };
        let name = self.parse_object_name()?;
        Ok(Statement::Describe { name, kind })
    }

    /// Parse a query expression, i.e. a `SELECT` statement optionally
    /// preceded with some `WITH` CTE declarations and optionally followed
    /// by `ORDER BY`. Unlike some other parse_... methods, this one doesn't
    /// expect the initial keyword to be already consumed
    pub fn parse_query(&mut self) -> ModalResult<Query> {
        let with = if self.parse_keyword(Keyword::WITH) {
            Some(With {
                recursive: self.parse_keyword(Keyword::RECURSIVE),
                cte_tables: self.parse_comma_separated(Parser::parse_cte)?,
            })
        } else {
            None
        };

        let body = self.parse_query_body(0)?;

        let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_order_by_expr)?
        } else {
            vec![]
        };

        let mut limit = None;
        let mut offset = None;
        for _x in 0..2 {
            if limit.is_none() && self.parse_keyword(Keyword::LIMIT) {
                limit = self.parse_limit()?
            }

            if offset.is_none() && self.parse_keyword(Keyword::OFFSET) {
                offset = Some(self.parse_offset()?)
            }
        }

        let fetch = if self.parse_keyword(Keyword::FETCH) {
            if limit.is_some() {
                parser_err!("Cannot specify both LIMIT and FETCH");
            }
            let fetch = self.parse_fetch()?;
            if fetch.with_ties && order_by.is_empty() {
                parser_err!("WITH TIES cannot be specified without ORDER BY clause");
            }
            Some(fetch)
        } else {
            None
        };

        Ok(Query {
            with,
            body,
            order_by,
            limit,
            offset,
            fetch,
        })
    }

    /// Parse a CTE (`alias [( col1, col2, ... )] AS (subquery)`)
    fn parse_cte(&mut self) -> ModalResult<Cte> {
        let name = self.parse_identifier_non_reserved()?;
        let cte = if self.parse_keyword(Keyword::AS) {
            let cte_inner = self.parse_cte_inner()?;
            let alias = TableAlias {
                name,
                columns: vec![],
            };
            Cte { alias, cte_inner }
        } else {
            let columns = self.parse_parenthesized_column_list(Optional)?;
            self.expect_keyword(Keyword::AS)?;
            let cte_inner = self.parse_cte_inner()?;
            let alias = TableAlias { name, columns };
            Cte { alias, cte_inner }
        };
        Ok(cte)
    }

    fn parse_cte_inner(&mut self) -> ModalResult<CteInner> {
        match self.expect_token(&Token::LParen) {
            Ok(()) => {
                let query = self.parse_query()?;
                self.expect_token(&Token::RParen)?;
                Ok(CteInner::Query(Box::new(query)))
            }
            _ => {
                let changelog = self.parse_identifier_non_reserved()?;
                if changelog.to_string().to_lowercase() != "changelog" {
                    parser_err!("Expected 'changelog' but found '{}'", changelog);
                }
                self.expect_keyword(Keyword::FROM)?;
                Ok(CteInner::ChangeLog(self.parse_object_name()?))
            }
        }
    }

    /// Parse a "query body", which is an expression with roughly the
    /// following grammar:
    /// ```text
    ///   query_body ::= restricted_select | '(' subquery ')' | set_operation
    ///   restricted_select ::= 'SELECT' [expr_list] [ from ] [ where ] [ groupby_having ]
    ///   subquery ::= query_body [ order_by_limit ]
    ///   set_operation ::= query_body { 'UNION' | 'EXCEPT' | 'INTERSECT' } [ 'ALL' ] query_body
    /// ```
    fn parse_query_body(&mut self, precedence: u8) -> ModalResult<SetExpr> {
        // We parse the expression using a Pratt parser, as in `parse_expr()`.
        // Start by parsing a restricted SELECT or a `(subquery)`:
        let mut expr = if self.parse_keyword(Keyword::SELECT) {
            SetExpr::Select(Box::new(self.parse_select()?))
        } else if self.consume_token(&Token::LParen) {
            // CTEs are not allowed here, but the parser currently accepts them
            let subquery = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            SetExpr::Query(Box::new(subquery))
        } else if self.parse_keyword(Keyword::VALUES) {
            SetExpr::Values(self.parse_values()?)
        } else {
            return self.expected("SELECT, VALUES, or a subquery in the query body");
        };

        loop {
            // The query can be optionally followed by a set operator:
            let op = self.parse_set_operator(&self.peek_token().token);
            let next_precedence = match op {
                // UNION and EXCEPT have the same binding power and evaluate left-to-right
                Some(SetOperator::Union) | Some(SetOperator::Except) => 10,
                // INTERSECT has higher precedence than UNION/EXCEPT
                Some(SetOperator::Intersect) => 20,
                // Unexpected token or EOF => stop parsing the query body
                None => break,
            };
            if precedence >= next_precedence {
                break;
            }
            self.next_token(); // skip past the set operator

            let all = self.parse_keyword(Keyword::ALL);
            let corresponding = self.parse_corresponding()?;

            expr = SetExpr::SetOperation {
                left: Box::new(expr),
                op: op.unwrap(),
                corresponding,
                all,
                right: Box::new(self.parse_query_body(next_precedence)?),
            };
        }

        Ok(expr)
    }

    fn parse_set_operator(&mut self, token: &Token) -> Option<SetOperator> {
        match token {
            Token::Word(w) if w.keyword == Keyword::UNION => Some(SetOperator::Union),
            Token::Word(w) if w.keyword == Keyword::EXCEPT => Some(SetOperator::Except),
            Token::Word(w) if w.keyword == Keyword::INTERSECT => Some(SetOperator::Intersect),
            _ => None,
        }
    }

    fn parse_corresponding(&mut self) -> ModalResult<Corresponding> {
        let corresponding = if self.parse_keyword(Keyword::CORRESPONDING) {
            let column_list = if self.parse_keyword(Keyword::BY) {
                Some(self.parse_parenthesized_column_list(IsOptional::Mandatory)?)
            } else {
                None
            };
            Corresponding::with_column_list(column_list)
        } else {
            Corresponding::none()
        };
        Ok(corresponding)
    }

    /// Parse a restricted `SELECT` statement (no CTEs / `UNION` / `ORDER BY`),
    /// assuming the initial `SELECT` was already consumed
    pub fn parse_select(&mut self) -> ModalResult<Select> {
        let distinct = self.parse_all_or_distinct_on()?;

        let projection = self.parse_comma_separated(Parser::parse_select_item)?;

        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_COLUMN_ALIAS` / `RESERVED_FOR_TABLE_ALIAS`,
        // otherwise they may be parsed as an alias as part of the `projection`
        // or `from`.

        let from = if self.parse_keyword(Keyword::FROM) {
            self.parse_comma_separated(Parser::parse_table_and_joins)?
        } else {
            vec![]
        };
        let mut lateral_views = vec![];
        loop {
            if self.parse_keywords(&[Keyword::LATERAL, Keyword::VIEW]) {
                let outer = self.parse_keyword(Keyword::OUTER);
                let lateral_view = self.parse_expr()?;
                let lateral_view_name = self.parse_object_name()?;
                let lateral_col_alias = self
                    .parse_comma_separated(|parser| {
                        parser.parse_optional_alias(&[
                            Keyword::WHERE,
                            Keyword::GROUP,
                            Keyword::CLUSTER,
                            Keyword::HAVING,
                            Keyword::LATERAL,
                        ]) // This couldn't possibly be a bad idea
                    })?
                    .into_iter()
                    .flatten()
                    .collect();

                lateral_views.push(LateralView {
                    lateral_view,
                    lateral_view_name,
                    lateral_col_alias,
                    outer,
                });
            } else {
                break;
            }
        }

        let selection = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.parse_keywords(&[Keyword::GROUP, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_group_by_expr)?
        } else {
            vec![]
        };

        let having = if self.parse_keyword(Keyword::HAVING) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let window = if self.parse_keyword(Keyword::WINDOW) {
            self.parse_comma_separated(Parser::parse_named_window)?
        } else {
            vec![]
        };

        Ok(Select {
            distinct,
            projection,
            from,
            lateral_views,
            selection,
            group_by,
            having,
            window,
        })
    }

    pub fn parse_set(&mut self) -> ModalResult<Statement> {
        let modifier = self.parse_one_of_keywords(&[Keyword::SESSION, Keyword::LOCAL]);
        if self.parse_keywords(&[Keyword::TIME, Keyword::ZONE]) {
            let value = alt((
                Keyword::DEFAULT.value(SetTimeZoneValue::Default),
                Keyword::LOCAL.value(SetTimeZoneValue::Local),
                preceded(
                    Keyword::INTERVAL,
                    cut_err(Self::parse_literal_interval.try_map(|e| match e {
                        // support a special case for clients which would send when initializing the connection
                        // like: SET TIME ZONE INTERVAL '+00:00' HOUR TO MINUTE;
                        Expr::Value(v) => match v {
                            Value::Interval { value, .. } => {
                                if value != "+00:00" {
                                    return Err(StrError("only support \"+00:00\" ".into()));
                                }
                                Ok(SetTimeZoneValue::Ident(Ident::with_quote_unchecked(
                                    '\'',
                                    "UTC".to_owned(),
                                )))
                            }
                            _ => Err(StrError("expect Value::Interval".into())),
                        },
                        _ => Err(StrError("expect Expr::Value".into())),
                    })),
                ),
                Self::parse_identifier.map(SetTimeZoneValue::Ident),
                Self::ensure_parse_value.map(SetTimeZoneValue::Literal),
            ))
            .expect("variable")
            .parse_next(self)?;

            Ok(Statement::SetTimeZone {
                local: modifier == Some(Keyword::LOCAL),
                value,
            })
        } else if self.parse_keyword(Keyword::CHARACTERISTICS) && modifier == Some(Keyword::SESSION)
        {
            self.expect_keywords(&[Keyword::AS, Keyword::TRANSACTION])?;
            Ok(Statement::SetTransaction {
                modes: self.parse_transaction_modes()?,
                snapshot: None,
                session: true,
            })
        } else if self.parse_keyword(Keyword::TRANSACTION) && modifier.is_none() {
            if self.parse_keyword(Keyword::SNAPSHOT) {
                let snapshot_id = self.ensure_parse_value()?;
                return Ok(Statement::SetTransaction {
                    modes: vec![],
                    snapshot: Some(snapshot_id),
                    session: false,
                });
            }
            Ok(Statement::SetTransaction {
                modes: self.parse_transaction_modes()?,
                snapshot: None,
                session: false,
            })
        } else {
            let config_param = self.parse_config_param()?;
            Ok(Statement::SetVariable {
                local: modifier == Some(Keyword::LOCAL),
                variable: config_param.param,
                value: config_param.value,
            })
        }
    }

    /// If have `databases`,`tables`,`columns`,`schemas` and `materialized views` after show,
    /// return `Statement::ShowCommand` or `Statement::ShowColumn`,
    /// otherwise, return `Statement::ShowVariable`.
    pub fn parse_show(&mut self) -> ModalResult<Statement> {
        let checkpoint = *self;
        if let Token::Word(w) = self.next_token().token {
            match w.keyword {
                Keyword::TABLES => {
                    return Ok(Statement::ShowObjects {
                        object: ShowObject::Table {
                            schema: self.parse_from_and_identifier()?,
                        },
                        filter: self.parse_show_statement_filter()?,
                    });
                }
                Keyword::INTERNAL => {
                    self.expect_keyword(Keyword::TABLES)?;
                    return Ok(Statement::ShowObjects {
                        object: ShowObject::InternalTable {
                            schema: self.parse_from_and_identifier()?,
                        },
                        filter: self.parse_show_statement_filter()?,
                    });
                }
                Keyword::SOURCES => {
                    return Ok(Statement::ShowObjects {
                        object: ShowObject::Source {
                            schema: self.parse_from_and_identifier()?,
                        },
                        filter: self.parse_show_statement_filter()?,
                    });
                }
                Keyword::SINKS => {
                    return Ok(Statement::ShowObjects {
                        object: ShowObject::Sink {
                            schema: self.parse_from_and_identifier()?,
                        },
                        filter: self.parse_show_statement_filter()?,
                    });
                }
                Keyword::SUBSCRIPTIONS => {
                    return Ok(Statement::ShowObjects {
                        object: ShowObject::Subscription {
                            schema: self.parse_from_and_identifier()?,
                        },
                        filter: self.parse_show_statement_filter()?,
                    });
                }
                Keyword::DATABASES => {
                    return Ok(Statement::ShowObjects {
                        object: ShowObject::Database,
                        filter: self.parse_show_statement_filter()?,
                    });
                }
                Keyword::SCHEMAS => {
                    return Ok(Statement::ShowObjects {
                        object: ShowObject::Schema,
                        filter: self.parse_show_statement_filter()?,
                    });
                }
                Keyword::VIEWS => {
                    return Ok(Statement::ShowObjects {
                        object: ShowObject::View {
                            schema: self.parse_from_and_identifier()?,
                        },
                        filter: self.parse_show_statement_filter()?,
                    });
                }
                Keyword::MATERIALIZED => {
                    if self.parse_keyword(Keyword::VIEWS) {
                        return Ok(Statement::ShowObjects {
                            object: ShowObject::MaterializedView {
                                schema: self.parse_from_and_identifier()?,
                            },
                            filter: self.parse_show_statement_filter()?,
                        });
                    } else {
                        return self.expected("VIEWS after MATERIALIZED");
                    }
                }
                Keyword::COLUMNS => {
                    if self.parse_keyword(Keyword::FROM) {
                        return Ok(Statement::ShowObjects {
                            object: ShowObject::Columns {
                                table: self.parse_object_name()?,
                            },
                            filter: self.parse_show_statement_filter()?,
                        });
                    } else {
                        return self.expected("from after columns");
                    }
                }
                Keyword::SECRETS => {
                    return Ok(Statement::ShowObjects {
                        object: ShowObject::Secret {
                            schema: self.parse_from_and_identifier()?,
                        },
                        filter: self.parse_show_statement_filter()?,
                    });
                }
                Keyword::CONNECTIONS => {
                    return Ok(Statement::ShowObjects {
                        object: ShowObject::Connection {
                            schema: self.parse_from_and_identifier()?,
                        },
                        filter: self.parse_show_statement_filter()?,
                    });
                }
                Keyword::FUNCTIONS => {
                    return Ok(Statement::ShowObjects {
                        object: ShowObject::Function {
                            schema: self.parse_from_and_identifier()?,
                        },
                        filter: self.parse_show_statement_filter()?,
                    });
                }
                Keyword::INDEXES => {
                    if self.parse_keyword(Keyword::FROM) {
                        return Ok(Statement::ShowObjects {
                            object: ShowObject::Indexes {
                                table: self.parse_object_name()?,
                            },
                            filter: self.parse_show_statement_filter()?,
                        });
                    } else {
                        return self.expected("from after indexes");
                    }
                }
                Keyword::CLUSTER => {
                    return Ok(Statement::ShowObjects {
                        object: ShowObject::Cluster,
                        filter: self.parse_show_statement_filter()?,
                    });
                }
                Keyword::JOBS => {
                    return Ok(Statement::ShowObjects {
                        object: ShowObject::Jobs,
                        filter: self.parse_show_statement_filter()?,
                    });
                }
                Keyword::PROCESSLIST => {
                    return Ok(Statement::ShowObjects {
                        object: ShowObject::ProcessList,
                        filter: self.parse_show_statement_filter()?,
                    });
                }
                Keyword::TRANSACTION => {
                    self.expect_keywords(&[Keyword::ISOLATION, Keyword::LEVEL])?;
                    return Ok(Statement::ShowTransactionIsolationLevel);
                }
                Keyword::CURSORS => {
                    return Ok(Statement::ShowObjects {
                        object: ShowObject::Cursor,
                        filter: None,
                    });
                }
                Keyword::SUBSCRIPTION => {
                    self.expect_keyword(Keyword::CURSORS)?;
                    return Ok(Statement::ShowObjects {
                        object: ShowObject::SubscriptionCursor,
                        filter: None,
                    });
                }
                _ => {}
            }
        }
        *self = checkpoint;
        Ok(Statement::ShowVariable {
            variable: self.parse_identifiers()?,
        })
    }

    pub fn parse_cancel_job(&mut self) -> ModalResult<Statement> {
        // CANCEL [JOBS|JOB] job_ids
        match self.peek_token().token {
            Token::Word(w) if Keyword::JOBS == w.keyword || Keyword::JOB == w.keyword => {
                self.next_token();
            }
            _ => return self.expected("JOBS or JOB after CANCEL"),
        }

        let mut job_ids = vec![];
        loop {
            job_ids.push(self.parse_literal_u32()?);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(Statement::CancelJobs(JobIdents(job_ids)))
    }

    pub fn parse_kill_process(&mut self) -> ModalResult<Statement> {
        let worker_process_id = self.parse_literal_string()?;
        Ok(Statement::Kill(worker_process_id))
    }

    /// Parser `from schema` after `show tables` and `show materialized views`, if not conclude
    /// `from` then use default schema name.
    pub fn parse_from_and_identifier(&mut self) -> ModalResult<Option<Ident>> {
        if self.parse_keyword(Keyword::FROM) {
            Ok(Some(self.parse_identifier_non_reserved()?))
        } else {
            Ok(None)
        }
    }

    /// Parse object type and name after `show create`.
    pub fn parse_show_create(&mut self) -> ModalResult<Statement> {
        if let Token::Word(w) = self.next_token().token {
            let show_type = match w.keyword {
                Keyword::TABLE => ShowCreateType::Table,
                Keyword::MATERIALIZED => {
                    if self.parse_keyword(Keyword::VIEW) {
                        ShowCreateType::MaterializedView
                    } else {
                        return self.expected("VIEW after MATERIALIZED");
                    }
                }
                Keyword::VIEW => ShowCreateType::View,
                Keyword::INDEX => ShowCreateType::Index,
                Keyword::SOURCE => ShowCreateType::Source,
                Keyword::SINK => ShowCreateType::Sink,
                Keyword::SUBSCRIPTION => ShowCreateType::Subscription,
                Keyword::FUNCTION => ShowCreateType::Function,
                _ => return self.expected(
                    "TABLE, MATERIALIZED VIEW, VIEW, INDEX, FUNCTION, SOURCE, SUBSCRIPTION or SINK",
                ),
            };
            return Ok(Statement::ShowCreateObject {
                create_type: show_type,
                name: self.parse_object_name()?,
            });
        }
        self.expected(
            "TABLE, MATERIALIZED VIEW, VIEW, INDEX, FUNCTION, SOURCE, SUBSCRIPTION or SINK",
        )
    }

    pub fn parse_show_statement_filter(&mut self) -> ModalResult<Option<ShowStatementFilter>> {
        if self.parse_keyword(Keyword::LIKE) {
            Ok(Some(ShowStatementFilter::Like(
                self.parse_literal_string()?,
            )))
        } else if self.parse_keyword(Keyword::ILIKE) {
            Ok(Some(ShowStatementFilter::ILike(
                self.parse_literal_string()?,
            )))
        } else if self.parse_keyword(Keyword::WHERE) {
            Ok(Some(ShowStatementFilter::Where(self.parse_expr()?)))
        } else {
            Ok(None)
        }
    }

    pub fn parse_table_and_joins(&mut self) -> ModalResult<TableWithJoins> {
        let relation = self.parse_table_factor()?;

        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_TABLE_ALIAS`, otherwise they may be parsed as
        // a table alias.
        let mut joins = vec![];
        loop {
            let join_checkpoint = *self;
            let join = if self.parse_keyword(Keyword::CROSS) {
                let join_operator = if self.parse_keyword(Keyword::JOIN) {
                    JoinOperator::CrossJoin
                } else {
                    return self.expected("JOIN after CROSS");
                };
                Join {
                    relation: self.parse_table_factor()?,
                    join_operator,
                }
            } else {
                let broadcast = self.peek_broadcast_join();
                if broadcast {
                    let _ = self.next_token();
                }
                let (natural, asof) =
                    match self.parse_one_of_keywords(&[Keyword::NATURAL, Keyword::ASOF]) {
                        Some(Keyword::NATURAL) => (true, false),
                        Some(Keyword::ASOF) => (false, true),
                        Some(_) => unreachable!(),
                        None => (false, false),
                    };
                let peek_keyword = if let Token::Word(w) = self.peek_token().token {
                    w.keyword
                } else {
                    Keyword::NoKeyword
                };

                let join_operator_type = match peek_keyword {
                    Keyword::INNER | Keyword::JOIN => {
                        let _ = self.parse_keyword(Keyword::INNER);
                        self.expect_keyword(Keyword::JOIN)?;
                        if asof {
                            JoinOperator::AsOfInner
                        } else {
                            JoinOperator::Inner
                        }
                    }
                    kw @ Keyword::LEFT | kw @ Keyword::RIGHT | kw @ Keyword::FULL => {
                        let checkpoint = *self;
                        let _ = self.next_token();
                        let _ = self.parse_keyword(Keyword::OUTER);
                        self.expect_keyword(Keyword::JOIN)?;
                        if asof {
                            if Keyword::LEFT == kw {
                                JoinOperator::AsOfLeft
                            } else {
                                return self.expected_at(
                                    checkpoint,
                                    "LEFT after ASOF. RIGHT or FULL are not supported",
                                );
                            }
                        } else {
                            match kw {
                                Keyword::LEFT => JoinOperator::LeftOuter,
                                Keyword::RIGHT => JoinOperator::RightOuter,
                                Keyword::FULL => JoinOperator::FullOuter,
                                _ => unreachable!(),
                            }
                        }
                    }
                    Keyword::OUTER => {
                        return self.expected("LEFT, RIGHT, or FULL");
                    }
                    _ if natural => {
                        return self.expected("a join type after NATURAL");
                    }
                    _ if asof => {
                        return self.expected("a join type after ASOF");
                    }
                    _ if broadcast => {
                        return self.expected_at(join_checkpoint, "a join type after BROADCAST");
                    }
                    _ => break,
                };
                let mut relation = self.parse_table_factor()?;
                let join_constraint = self.parse_join_constraint(natural)?;
                let join_operator = join_operator_type(join_constraint);
                if broadcast {
                    if !matches!(
                        join_operator,
                        JoinOperator::Inner(_) | JoinOperator::LeftOuter(_)
                    ) {
                        return self.expected_at(
                            join_checkpoint,
                            "INNER or LEFT temporal join after BROADCAST",
                        );
                    }
                    match &mut relation {
                        TableFactor::Table {
                            as_of: Some(as_of), ..
                        } if matches!(as_of, AsOf::ProcessTime) => {
                            *as_of = AsOf::ProcessTimeBroadcast;
                        }
                        _ => {
                            return self.expected_at(
                                join_checkpoint,
                                "a table with FOR SYSTEM_TIME AS OF PROCTIME() after BROADCAST JOIN",
                            );
                        }
                    }
                }
                let need_constraint = match join_operator {
                    JoinOperator::Inner(JoinConstraint::None) => Some("INNER JOIN"),
                    JoinOperator::AsOfInner(JoinConstraint::None) => Some("ASOF INNER JOIN"),
                    JoinOperator::AsOfLeft(JoinConstraint::None) => Some("ASOF LEFT JOIN"),
                    _ => None,
                };
                if let Some(join_type) = need_constraint {
                    return self.expected(&format!("join constraint after {join_type}"));
                }

                Join {
                    relation,
                    join_operator,
                }
            };
            joins.push(join);
        }
        Ok(TableWithJoins { relation, joins })
    }

    /// Whether the next tokens start a supported broadcast join.
    ///
    /// `BROADCAST` remains a regular identifier outside this exact position so that existing
    /// table aliases and identifier formatting remain compatible.
    fn peek_broadcast_join(&self) -> bool {
        matches!(
            self.peek_nth_token(0).token,
            Token::Word(word)
                if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("BROADCAST")
        ) && matches!(
            self.peek_nth_token(1).token,
            Token::Word(word)
                if matches!(word.keyword, Keyword::INNER | Keyword::JOIN | Keyword::LEFT)
        )
    }

    /// A table name or a parenthesized subquery, followed by optional `[AS] alias`
    pub fn parse_table_factor(&mut self) -> ModalResult<TableFactor> {
        if self.parse_keyword(Keyword::LATERAL) {
            // LATERAL must always be followed by a subquery.
            if !self.consume_token(&Token::LParen) {
                self.expected("subquery after LATERAL")?;
            }
            self.parse_derived_table_factor(Lateral)
        } else if self.consume_token(&Token::LParen) {
            // A left paren introduces either a derived table (i.e., a subquery)
            // or a nested join. It's nearly impossible to determine ahead of
            // time which it is... so we just try to parse both.
            //
            // Here's an example that demonstrates the complexity:
            //                     /-------------------------------------------------------\
            //                     | /-----------------------------------\                 |
            //     SELECT * FROM ( ( ( (SELECT 1) UNION (SELECT 2) ) AS t1 NATURAL JOIN t2 ) )
            //                   ^ ^ ^ ^
            //                   | | | |
            //                   | | | |
            //                   | | | (4) belongs to a SetExpr::Query inside the subquery
            //                   | | (3) starts a derived table (subquery)
            //                   | (2) starts a nested join
            //                   (1) an additional set of parens around a nested join
            //

            // It can only be a subquery. We don't use `maybe_parse` so that a meaningful error can
            // be returned.
            match self.peek_token().token {
                Token::Word(w)
                    if [Keyword::SELECT, Keyword::WITH, Keyword::VALUES].contains(&w.keyword) =>
                {
                    return self.parse_derived_table_factor(NotLateral);
                }
                _ => {}
            };
            // It can still be a subquery, e.g., the case (3) in the example above:
            // (SELECT 1) UNION (SELECT 2)
            // TODO: how to produce a good error message here?
            if self.peek_token() == Token::LParen {
                return_ok_if_some!(
                    self.maybe_parse(|parser| parser.parse_derived_table_factor(NotLateral))
                );
            }

            // A parsing error from `parse_derived_table_factor` indicates that the '(' we've
            // recently consumed does not start a derived table (cases 1, 2, or 4).
            // `maybe_parse` will ignore such an error and rewind to be after the opening '('.

            // Inside the parentheses we expect to find an (A) table factor
            // followed by some joins or (B) another level of nesting.
            let table_and_joins = self.parse_table_and_joins()?;

            if !table_and_joins.joins.is_empty() {
                self.expect_token(&Token::RParen)?;
                Ok(TableFactor::NestedJoin(Box::new(table_and_joins))) // (A)
            } else if let TableFactor::NestedJoin(_) = &table_and_joins.relation {
                // (B): `table_and_joins` (what we found inside the parentheses)
                // is a nested join `(foo JOIN bar)`, not followed by other joins.
                self.expect_token(&Token::RParen)?;
                Ok(TableFactor::NestedJoin(Box::new(table_and_joins)))
            } else {
                // The SQL spec prohibits derived tables and bare tables from
                // appearing alone in parentheses (e.g. `FROM (mytable)`)
                parser_err!(
                    "Expected joined table, found: {table_and_joins}, next_token: {}",
                    self.peek_token()
                );
            }
        } else {
            let name = self.parse_object_name()?;
            if self.peek_token() == Token::LParen {
                // table-valued function

                let arg_list = self.parse_argument_list()?;
                if arg_list.distinct {
                    parser_err!("DISTINCT is not supported in table-valued function calls");
                }
                if !arg_list.order_by.is_empty() {
                    parser_err!("ORDER BY is not supported in table-valued function calls");
                }
                if arg_list.ignore_nulls {
                    parser_err!("IGNORE NULLS is not supported in table-valued function calls");
                }

                let args = arg_list.args;
                let with_ordinality = self.parse_keywords(&[Keyword::WITH, Keyword::ORDINALITY]);
                let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;

                Ok(TableFactor::TableFunction {
                    name,
                    alias,
                    args,
                    with_ordinality,
                })
            } else {
                let as_of = opt(Self::parse_as_of).parse_next(self)?;
                let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
                Ok(TableFactor::Table { name, alias, as_of })
            }
        }
    }

    pub fn parse_derived_table_factor(&mut self, lateral: IsLateral) -> ModalResult<TableFactor> {
        let subquery = Box::new(self.parse_query()?);
        self.expect_token(&Token::RParen)?;
        let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
        Ok(TableFactor::Derived {
            lateral: match lateral {
                Lateral => true,
                NotLateral => false,
            },
            subquery,
            alias,
        })
    }

    fn parse_join_constraint(&mut self, natural: bool) -> ModalResult<JoinConstraint> {
        if natural {
            Ok(JoinConstraint::Natural)
        } else if self.parse_keyword(Keyword::ON) {
            let constraint = self.parse_expr()?;
            Ok(JoinConstraint::On(constraint))
        } else if self.parse_keyword(Keyword::USING) {
            let columns = self.parse_parenthesized_column_list(Mandatory)?;
            Ok(JoinConstraint::Using(columns))
        } else {
            Ok(JoinConstraint::None)
            // self.expected("ON, or USING after JOIN")
        }
    }

    /// Parse a GRANT statement.
    pub fn parse_grant(&mut self) -> ModalResult<Statement> {
        let (privileges, objects) = self.parse_grant_revoke_privileges_objects()?;

        self.expect_keyword(Keyword::TO)?;
        let grantees = self.parse_comma_separated(Parser::parse_identifier)?;

        let with_grant_option =
            self.parse_keywords(&[Keyword::WITH, Keyword::GRANT, Keyword::OPTION]);

        let granted_by = self
            .parse_keywords(&[Keyword::GRANTED, Keyword::BY])
            .then(|| self.parse_identifier().unwrap());

        Ok(Statement::Grant {
            privileges,
            objects,
            grantees,
            with_grant_option,
            granted_by,
        })
    }

    fn parse_privileges(&mut self) -> ModalResult<Privileges> {
        let privileges = if self.parse_keyword(Keyword::ALL) {
            Privileges::All {
                with_privileges_keyword: self.parse_keyword(Keyword::PRIVILEGES),
            }
        } else {
            Privileges::Actions(
                self.parse_comma_separated(Parser::parse_grant_permission)?
                    .into_iter()
                    .map(|(kw, columns)| match kw {
                        Keyword::CONNECT => Action::Connect,
                        Keyword::CREATE => Action::Create,
                        Keyword::DELETE => Action::Delete,
                        Keyword::EXECUTE => Action::Execute,
                        Keyword::INSERT => Action::Insert { columns },
                        Keyword::REFERENCES => Action::References { columns },
                        Keyword::SELECT => Action::Select { columns },
                        Keyword::TEMPORARY => Action::Temporary,
                        Keyword::TRIGGER => Action::Trigger,
                        Keyword::TRUNCATE => Action::Truncate,
                        Keyword::UPDATE => Action::Update { columns },
                        Keyword::USAGE => Action::Usage,
                        _ => unreachable!(),
                    })
                    .collect(),
            )
        };

        Ok(privileges)
    }

    fn parse_grant_revoke_privileges_objects(&mut self) -> ModalResult<(Privileges, GrantObjects)> {
        let privileges = self.parse_privileges()?;

        self.expect_keyword(Keyword::ON)?;

        let objects = if self.parse_keywords(&[
            Keyword::ALL,
            Keyword::TABLES,
            Keyword::IN,
            Keyword::SCHEMA,
        ]) {
            GrantObjects::AllTablesInSchema {
                schemas: self.parse_comma_separated(Parser::parse_object_name)?,
            }
        } else if self.parse_keywords(&[
            Keyword::ALL,
            Keyword::SEQUENCES,
            Keyword::IN,
            Keyword::SCHEMA,
        ]) {
            GrantObjects::AllSequencesInSchema {
                schemas: self.parse_comma_separated(Parser::parse_object_name)?,
            }
        } else if self.parse_keywords(&[
            Keyword::ALL,
            Keyword::SOURCES,
            Keyword::IN,
            Keyword::SCHEMA,
        ]) {
            GrantObjects::AllSourcesInSchema {
                schemas: self.parse_comma_separated(Parser::parse_object_name)?,
            }
        } else if self.parse_keywords(&[Keyword::ALL, Keyword::SINKS, Keyword::IN, Keyword::SCHEMA])
        {
            GrantObjects::AllSinksInSchema {
                schemas: self.parse_comma_separated(Parser::parse_object_name)?,
            }
        } else if self.parse_keywords(&[
            Keyword::ALL,
            Keyword::MATERIALIZED,
            Keyword::VIEWS,
            Keyword::IN,
            Keyword::SCHEMA,
        ]) {
            GrantObjects::AllMviewsInSchema {
                schemas: self.parse_comma_separated(Parser::parse_object_name)?,
            }
        } else if self.parse_keywords(&[Keyword::ALL, Keyword::VIEWS, Keyword::IN, Keyword::SCHEMA])
        {
            GrantObjects::AllViewsInSchema {
                schemas: self.parse_comma_separated(Parser::parse_object_name)?,
            }
        } else if self.parse_keywords(&[
            Keyword::ALL,
            Keyword::FUNCTIONS,
            Keyword::IN,
            Keyword::SCHEMA,
        ]) {
            GrantObjects::AllFunctionsInSchema {
                schemas: self.parse_comma_separated(Parser::parse_object_name)?,
            }
        } else if self.parse_keywords(&[
            Keyword::ALL,
            Keyword::SECRETS,
            Keyword::IN,
            Keyword::SCHEMA,
        ]) {
            GrantObjects::AllSecretsInSchema {
                schemas: self.parse_comma_separated(Parser::parse_object_name)?,
            }
        } else if self.parse_keywords(&[
            Keyword::ALL,
            Keyword::CONNECTIONS,
            Keyword::IN,
            Keyword::SCHEMA,
        ]) {
            GrantObjects::AllConnectionsInSchema {
                schemas: self.parse_comma_separated(Parser::parse_object_name)?,
            }
        } else if self.parse_keywords(&[
            Keyword::ALL,
            Keyword::SUBSCRIPTIONS,
            Keyword::IN,
            Keyword::SCHEMA,
        ]) {
            GrantObjects::AllSubscriptionsInSchema {
                schemas: self.parse_comma_separated(Parser::parse_object_name)?,
            }
        } else if self.parse_keywords(&[Keyword::MATERIALIZED, Keyword::VIEW]) {
            GrantObjects::Mviews(self.parse_comma_separated(Parser::parse_object_name)?)
        } else {
            let object_type = self.parse_one_of_keywords(&[
                Keyword::SEQUENCE,
                Keyword::DATABASE,
                Keyword::SCHEMA,
                Keyword::TABLE,
                Keyword::SOURCE,
                Keyword::SINK,
                Keyword::VIEW,
                Keyword::SUBSCRIPTION,
                Keyword::FUNCTION,
                Keyword::CONNECTION,
                Keyword::SECRET,
            ]);
            if let Some(Keyword::FUNCTION) = object_type {
                let func_descs = self.parse_comma_separated(Parser::parse_function_desc)?;
                GrantObjects::Functions(func_descs)
            } else {
                let objects = self.parse_comma_separated(Parser::parse_object_name);
                match object_type {
                    Some(Keyword::DATABASE) => GrantObjects::Databases(objects?),
                    Some(Keyword::SCHEMA) => GrantObjects::Schemas(objects?),
                    Some(Keyword::SEQUENCE) => GrantObjects::Sequences(objects?),
                    Some(Keyword::SOURCE) => GrantObjects::Sources(objects?),
                    Some(Keyword::SINK) => GrantObjects::Sinks(objects?),
                    Some(Keyword::VIEW) => GrantObjects::Views(objects?),
                    Some(Keyword::SUBSCRIPTION) => GrantObjects::Subscriptions(objects?),
                    Some(Keyword::CONNECTION) => GrantObjects::Connections(objects?),
                    Some(Keyword::SECRET) => GrantObjects::Secrets(objects?),
                    Some(Keyword::TABLE) | None => GrantObjects::Tables(objects?),
                    _ => unreachable!(),
                }
            }
        };

        Ok((privileges, objects))
    }

    fn parse_grant_permission(&mut self) -> ModalResult<(Keyword, Option<Vec<Ident>>)> {
        let kw = self.expect_one_of_keywords(&[
            Keyword::CONNECT,
            Keyword::CREATE,
            Keyword::DELETE,
            Keyword::EXECUTE,
            Keyword::INSERT,
            Keyword::REFERENCES,
            Keyword::SELECT,
            Keyword::TEMPORARY,
            Keyword::TRIGGER,
            Keyword::TRUNCATE,
            Keyword::UPDATE,
            Keyword::USAGE,
        ])?;
        let columns = match kw {
            Keyword::INSERT | Keyword::REFERENCES | Keyword::SELECT | Keyword::UPDATE => {
                let columns = self.parse_parenthesized_column_list(Optional)?;
                if columns.is_empty() {
                    None
                } else {
                    Some(columns)
                }
            }
            _ => None,
        };
        Ok((kw, columns))
    }

    /// Parse a REVOKE statement
    pub fn parse_revoke(&mut self) -> ModalResult<Statement> {
        let revoke_grant_option =
            self.parse_keywords(&[Keyword::GRANT, Keyword::OPTION, Keyword::FOR]);
        let (privileges, objects) = self.parse_grant_revoke_privileges_objects()?;

        self.expect_keyword(Keyword::FROM)?;
        let grantees = self.parse_comma_separated(Parser::parse_identifier)?;

        let granted_by = self
            .parse_keywords(&[Keyword::GRANTED, Keyword::BY])
            .then(|| self.parse_identifier().unwrap());

        let cascade = self.parse_keyword(Keyword::CASCADE);
        let restrict = self.parse_keyword(Keyword::RESTRICT);
        if cascade && restrict {
            parser_err!("Cannot specify both CASCADE and RESTRICT in REVOKE");
        }

        Ok(Statement::Revoke {
            privileges,
            objects,
            grantees,
            granted_by,
            revoke_grant_option,
            cascade,
        })
    }

    fn parse_privilege_object_types(&mut self) -> ModalResult<PrivilegeObjectType> {
        let object_type = if self.parse_keyword(Keyword::TABLES) {
            PrivilegeObjectType::Tables
        } else if self.parse_keyword(Keyword::SOURCES) {
            PrivilegeObjectType::Sources
        } else if self.parse_keyword(Keyword::SINKS) {
            PrivilegeObjectType::Sinks
        } else if self.parse_keywords(&[Keyword::MATERIALIZED, Keyword::VIEWS]) {
            PrivilegeObjectType::Mviews
        } else if self.parse_keyword(Keyword::VIEWS) {
            PrivilegeObjectType::Views
        } else if self.parse_keyword(Keyword::FUNCTIONS) {
            PrivilegeObjectType::Functions
        } else if self.parse_keyword(Keyword::SECRETS) {
            PrivilegeObjectType::Secrets
        } else if self.parse_keyword(Keyword::CONNECTIONS) {
            PrivilegeObjectType::Connections
        } else if self.parse_keyword(Keyword::SUBSCRIPTIONS) {
            PrivilegeObjectType::Subscriptions
        } else if self.parse_keyword(Keyword::SCHEMAS) {
            PrivilegeObjectType::Schemas
        } else {
            return self.expected("TABLES, SOURCES, SINKS, MATERIALIZED VIEWS, VIEWS, FUNCTIONS, SECRETS, CONNECTIONS, SUBSCRIPTIONS or SCHEMAS");
        };

        Ok(object_type)
    }

    pub fn parse_alter_default_privileges(&mut self) -> ModalResult<Statement> {
        // [ FOR USER target_user [, ...] ]
        let target_users = if self.parse_keyword(Keyword::FOR) {
            self.expect_keyword(Keyword::USER)?;
            Some(self.parse_comma_separated(Parser::parse_identifier)?)
        } else {
            None
        };

        // [ IN SCHEMA schema_name [, ...] ]
        let schema_names = if self.parse_keywords(&[Keyword::IN, Keyword::SCHEMA]) {
            Some(self.parse_comma_separated(Parser::parse_object_name)?)
        } else {
            None
        };
        let keyword = self.expect_one_of_keywords(&[Keyword::GRANT, Keyword::REVOKE])?;
        let for_grant = keyword == Keyword::GRANT;
        if for_grant {
            let privileges = self.parse_privileges()?;
            self.expect_keyword(Keyword::ON)?;
            let object_type = self.parse_privilege_object_types()?;
            if schema_names.is_some() && object_type == PrivilegeObjectType::Schemas {
                parser_err!("cannot use IN SCHEMA clause when using GRANT/REVOKE ON SCHEMAS");
            }
            self.expect_keyword(Keyword::TO)?;
            let grantees = self.parse_comma_separated(Parser::parse_identifier)?;

            let with_grant_option =
                self.parse_keywords(&[Keyword::WITH, Keyword::GRANT, Keyword::OPTION]);

            Ok(Statement::AlterDefaultPrivileges {
                target_users,
                schema_names,
                operation: DefaultPrivilegeOperation::Grant {
                    privileges,
                    object_type,
                    grantees,
                    with_grant_option,
                },
            })
        } else {
            let revoke_grant_option =
                self.parse_keywords(&[Keyword::GRANT, Keyword::OPTION, Keyword::FOR]);
            let privileges = self.parse_privileges()?;
            self.expect_keyword(Keyword::ON)?;
            let object_type = self.parse_privilege_object_types()?;
            if schema_names.is_some() && object_type == PrivilegeObjectType::Schemas {
                parser_err!("cannot use IN SCHEMA clause when using GRANT/REVOKE ON SCHEMAS");
            }
            self.expect_keyword(Keyword::FROM)?;
            let grantees = self.parse_comma_separated(Parser::parse_identifier)?;
            let cascade = self.parse_keyword(Keyword::CASCADE);
            let restrict = self.parse_keyword(Keyword::RESTRICT);
            if cascade && restrict {
                parser_err!("Cannot specify both CASCADE and RESTRICT in REVOKE");
            }

            Ok(Statement::AlterDefaultPrivileges {
                target_users,
                schema_names,
                operation: DefaultPrivilegeOperation::Revoke {
                    privileges,
                    object_type,
                    grantees,
                    revoke_grant_option,
                    cascade,
                },
            })
        }
    }

    /// Parse an INSERT statement
    pub fn parse_insert(&mut self) -> ModalResult<Statement> {
        self.expect_keyword(Keyword::INTO)?;

        let table_name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;

        let source = Box::new(self.parse_query()?);
        let returning = self.parse_returning(Optional)?;
        Ok(Statement::Insert {
            table_name,
            columns,
            source,
            returning,
        })
    }

    pub fn parse_update(&mut self) -> ModalResult<Statement> {
        let table_name = self.parse_object_name()?;

        self.expect_keyword(Keyword::SET)?;
        let assignments = self.parse_comma_separated(Parser::parse_assignment)?;
        let selection = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let returning = self.parse_returning(Optional)?;
        Ok(Statement::Update {
            table_name,
            assignments,
            selection,
            returning,
        })
    }

    /// Parse a `var = expr` assignment, used in an UPDATE statement
    pub fn parse_assignment(&mut self) -> ModalResult<Assignment> {
        let id = self.parse_identifiers_non_keywords()?;
        self.expect_token(&Token::Eq)?;

        let value = if self.parse_keyword(Keyword::DEFAULT) {
            AssignmentValue::Default
        } else {
            AssignmentValue::Expr(self.parse_expr()?)
        };

        Ok(Assignment { id, value })
    }

    /// Parse a `[VARIADIC] name => expr`.
    fn parse_function_args(&mut self) -> ModalResult<(bool, FunctionArg)> {
        let variadic = self.parse_keyword(Keyword::VARIADIC);
        let arg = if self.peek_nth_token(1) == Token::RArrow {
            let name = self.parse_identifier()?;

            self.expect_token(&Token::RArrow)?;
            let arg = if self.parse_keyword(Keyword::SECRET) {
                FunctionArgExpr::SecretRef(self.parse_secret_ref()?)
            } else {
                self.parse_wildcard_or_expr()?.into()
            };

            FunctionArg::Named { name, arg }
        } else if self.parse_keyword(Keyword::SECRET) {
            FunctionArg::Unnamed(FunctionArgExpr::SecretRef(self.parse_secret_ref()?))
        } else {
            FunctionArg::Unnamed(self.parse_wildcard_or_expr()?.into())
        };
        Ok((variadic, arg))
    }

    pub fn parse_argument_list(&mut self) -> ModalResult<FunctionArgList> {
        self.expect_token(&Token::LParen)?;
        if self.consume_token(&Token::RParen) {
            Ok(FunctionArgList::empty())
        } else {
            let distinct = self.parse_all_or_distinct()?;
            let args = self.parse_comma_separated(Parser::parse_function_args)?;
            if args
                .iter()
                .take(args.len() - 1)
                .any(|(variadic, _)| *variadic)
            {
                parser_err!("VARIADIC argument must be the last");
            }
            let variadic = args.last().map(|(variadic, _)| *variadic).unwrap_or(false);
            let args = args.into_iter().map(|(_, arg)| arg).collect();

            let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
                self.parse_comma_separated(Parser::parse_order_by_expr)?
            } else {
                vec![]
            };

            let ignore_nulls = self.parse_keywords(&[Keyword::IGNORE, Keyword::NULLS]);

            let arg_list = FunctionArgList {
                distinct,
                args,
                variadic,
                order_by,
                ignore_nulls,
            };

            self.expect_token(&Token::RParen)?;
            Ok(arg_list)
        }
    }

    /// Parse a comma-delimited list of projections after SELECT
    pub fn parse_select_item(&mut self) -> ModalResult<SelectItem> {
        match self.parse_wildcard_or_expr()? {
            WildcardOrExpr::Expr(expr) => self
                .parse_optional_alias(keywords::RESERVED_FOR_COLUMN_ALIAS)
                .map(|alias| match alias {
                    Some(alias) => SelectItem::ExprWithAlias { expr, alias },
                    None => SelectItem::UnnamedExpr(expr),
                }),
            WildcardOrExpr::QualifiedWildcard(prefix, except) => {
                Ok(SelectItem::QualifiedWildcard(prefix, except))
            }
            WildcardOrExpr::ExprQualifiedWildcard(expr, prefix) => {
                Ok(SelectItem::ExprQualifiedWildcard(expr, prefix))
            }
            WildcardOrExpr::Wildcard(except) => Ok(SelectItem::Wildcard(except)),
        }
    }

    /// Parse an expression, optionally followed by ASC or DESC (used in ORDER BY)
    pub fn parse_order_by_expr(&mut self) -> ModalResult<OrderByExpr> {
        let expr = self.parse_expr()?;

        let asc = if self.parse_keyword(Keyword::ASC) {
            Some(true)
        } else if self.parse_keyword(Keyword::DESC) {
            Some(false)
        } else {
            None
        };

        let nulls_first = if self.parse_keywords(&[Keyword::NULLS, Keyword::FIRST]) {
            Some(true)
        } else if self.parse_keywords(&[Keyword::NULLS, Keyword::LAST]) {
            Some(false)
        } else {
            None
        };

        Ok(OrderByExpr {
            expr,
            asc,
            nulls_first,
        })
    }

    /// Parse a LIMIT clause
    pub fn parse_limit(&mut self) -> ModalResult<Option<Expr>> {
        if self.parse_keyword(Keyword::ALL) {
            Ok(None)
        } else {
            let expr = self.parse_expr()?;
            Ok(Some(expr))
        }
    }

    /// Parse an OFFSET clause
    pub fn parse_offset(&mut self) -> ModalResult<String> {
        let value = self.parse_number_value()?;
        // TODO(Kexiang): support LIMIT expr
        if self.consume_token(&Token::DoubleColon) {
            self.expect_keyword(Keyword::BIGINT)?;
        }
        _ = self.parse_one_of_keywords(&[Keyword::ROW, Keyword::ROWS]);
        Ok(value)
    }

    /// Parse a FETCH clause
    pub fn parse_fetch(&mut self) -> ModalResult<Fetch> {
        self.expect_one_of_keywords(&[Keyword::FIRST, Keyword::NEXT])?;
        let quantity = if self
            .parse_one_of_keywords(&[Keyword::ROW, Keyword::ROWS])
            .is_some()
        {
            None
        } else {
            let quantity = self.parse_number_value()?;
            self.expect_one_of_keywords(&[Keyword::ROW, Keyword::ROWS])?;
            Some(quantity)
        };
        let with_ties = if self.parse_keyword(Keyword::ONLY) {
            false
        } else if self.parse_keywords(&[Keyword::WITH, Keyword::TIES]) {
            true
        } else {
            return self.expected("one of ONLY or WITH TIES");
        };
        Ok(Fetch {
            with_ties,
            quantity,
        })
    }

    pub fn parse_values(&mut self) -> ModalResult<Values> {
        let values = self.parse_comma_separated(|parser| {
            parser.expect_token(&Token::LParen)?;
            let exprs = parser.parse_comma_separated(Parser::parse_expr)?;
            parser.expect_token(&Token::RParen)?;
            Ok(exprs)
        })?;
        Ok(Values(values))
    }

    pub fn parse_start_transaction(&mut self) -> ModalResult<Statement> {
        self.expect_keyword(Keyword::TRANSACTION)?;
        Ok(Statement::StartTransaction {
            modes: self.parse_transaction_modes()?,
        })
    }

    pub fn parse_begin(&mut self) -> ModalResult<Statement> {
        let _ = self.parse_one_of_keywords(&[Keyword::TRANSACTION, Keyword::WORK]);
        Ok(Statement::Begin {
            modes: self.parse_transaction_modes()?,
        })
    }

    pub fn parse_transaction_modes(&mut self) -> ModalResult<Vec<TransactionMode>> {
        let mut modes = vec![];
        let mut required = false;
        loop {
            let mode = if self.parse_keywords(&[Keyword::ISOLATION, Keyword::LEVEL]) {
                let iso_level = if self.parse_keywords(&[Keyword::READ, Keyword::UNCOMMITTED]) {
                    TransactionIsolationLevel::ReadUncommitted
                } else if self.parse_keywords(&[Keyword::READ, Keyword::COMMITTED]) {
                    TransactionIsolationLevel::ReadCommitted
                } else if self.parse_keywords(&[Keyword::REPEATABLE, Keyword::READ]) {
                    TransactionIsolationLevel::RepeatableRead
                } else if self.parse_keyword(Keyword::SERIALIZABLE) {
                    TransactionIsolationLevel::Serializable
                } else {
                    self.expected("isolation level")?
                };
                TransactionMode::IsolationLevel(iso_level)
            } else if self.parse_keywords(&[Keyword::READ, Keyword::ONLY]) {
                TransactionMode::AccessMode(TransactionAccessMode::ReadOnly)
            } else if self.parse_keywords(&[Keyword::READ, Keyword::WRITE]) {
                TransactionMode::AccessMode(TransactionAccessMode::ReadWrite)
            } else if required {
                self.expected("transaction mode")?
            } else {
                break;
            };
            modes.push(mode);
            // ANSI requires a comma after each transaction mode, but
            // PostgreSQL, for historical reasons, does not. We follow
            // PostgreSQL in making the comma optional, since that is strictly
            // more general.
            required = self.consume_token(&Token::Comma);
        }
        Ok(modes)
    }

    pub fn parse_commit(&mut self) -> ModalResult<Statement> {
        Ok(Statement::Commit {
            chain: self.parse_commit_rollback_chain()?,
        })
    }

    pub fn parse_rollback(&mut self) -> ModalResult<Statement> {
        Ok(Statement::Rollback {
            chain: self.parse_commit_rollback_chain()?,
        })
    }

    pub fn parse_commit_rollback_chain(&mut self) -> ModalResult<bool> {
        let _ = self.parse_one_of_keywords(&[Keyword::TRANSACTION, Keyword::WORK]);
        if self.parse_keyword(Keyword::AND) {
            let chain = !self.parse_keyword(Keyword::NO);
            self.expect_keyword(Keyword::CHAIN)?;
            Ok(chain)
        } else {
            Ok(false)
        }
    }

    fn parse_deallocate(&mut self) -> ModalResult<Statement> {
        let prepare = self.parse_keyword(Keyword::PREPARE);
        let name = if self.parse_keyword(Keyword::ALL) {
            None
        } else {
            Some(self.parse_identifier()?)
        };
        Ok(Statement::Deallocate { name, prepare })
    }

    fn parse_execute(&mut self) -> ModalResult<Statement> {
        let name = self.parse_identifier()?;

        let mut parameters = vec![];
        if self.consume_token(&Token::LParen) {
            parameters = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
        }

        Ok(Statement::Execute { name, parameters })
    }

    fn parse_prepare(&mut self) -> ModalResult<Statement> {
        let name = self.parse_identifier()?;

        let mut data_types = vec![];
        if self.consume_token(&Token::LParen) {
            data_types = self.parse_comma_separated(Parser::parse_data_type)?;
            self.expect_token(&Token::RParen)?;
        }

        self.expect_keyword(Keyword::AS)?;
        let statement = Box::new(self.parse_statement()?);
        Ok(Statement::Prepare {
            name,
            data_types,
            statement,
        })
    }

    fn parse_comment(&mut self) -> ModalResult<Statement> {
        self.expect_keyword(Keyword::ON)?;
        let checkpoint = *self;
        let token = self.next_token();

        let (object_type, object_name) = match token.token {
            Token::Word(w) if w.keyword == Keyword::COLUMN => {
                let object_name = self.parse_object_name()?;
                (CommentObject::Column, object_name)
            }
            Token::Word(w) if w.keyword == Keyword::TABLE => {
                let object_name = self.parse_object_name()?;
                (CommentObject::Table, object_name)
            }
            _ => self.expected_at(checkpoint, "comment object_type")?,
        };

        self.expect_keyword(Keyword::IS)?;
        let comment = if self.parse_keyword(Keyword::NULL) {
            None
        } else {
            Some(self.parse_literal_string()?)
        };
        Ok(Statement::Comment {
            object_type,
            object_name,
            comment,
        })
    }

    fn parse_use(&mut self) -> ModalResult<Statement> {
        let db_name = self.parse_object_name()?;
        Ok(Statement::Use { db_name })
    }

    /// Parse a named window definition for the WINDOW clause
    pub fn parse_named_window(&mut self) -> ModalResult<NamedWindow> {
        let name = self.parse_identifier()?;
        self.expect_keywords(&[Keyword::AS])?;
        self.expect_token(&Token::LParen)?;
        let window_spec = self.parse_window_spec()?;
        self.expect_token(&Token::RParen)?;
        Ok(NamedWindow { name, window_spec })
    }

    /// Parse a window specification (contents of OVER clause or WINDOW clause)
    pub fn parse_window_spec(&mut self) -> ModalResult<WindowSpec> {
        let partition_by = if self.parse_keywords(&[Keyword::PARTITION, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_expr)?
        } else {
            vec![]
        };
        let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_order_by_expr)?
        } else {
            vec![]
        };
        let window_frame = if !self.peek_token().eq(&Token::RParen) {
            Some(self.parse_window_frame()?)
        } else {
            None
        };
        Ok(WindowSpec {
            partition_by,
            order_by,
            window_frame,
        })
    }

    pub fn parse_wait(&mut self) -> ModalResult<Statement> {
        let target = if self.parse_keyword(Keyword::TABLE) {
            WaitTarget::Table(self.parse_object_name()?)
        } else if self.parse_keyword(Keyword::MATERIALIZED) {
            self.expect_keyword(Keyword::VIEW)?;
            WaitTarget::MaterializedView(self.parse_object_name()?)
        } else if self.parse_keyword(Keyword::SINK) {
            WaitTarget::Sink(self.parse_object_name()?)
        } else if self.parse_keyword(Keyword::INDEX) {
            WaitTarget::Index(self.parse_object_name()?)
        } else {
            WaitTarget::All
        };

        Ok(Statement::Wait(target))
    }
}

impl Word {
    /// Convert a Word to a Identifier, return ParserError when the Word's value is a empty string.
    pub fn to_ident(&self) -> ModalResult<Ident> {
        if self.value.is_empty() {
            parser_err!("zero-length delimited identifier at or near \"{self}\"")
        } else {
            Ok(Ident {
                value: self.value.clone(),
                quote_style: self.quote_style,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::run_parser_method;

    #[test]
    fn test_parse_integer_min() {
        let min_bigint = "-9223372036854775808";
        run_parser_method(min_bigint, |parser| {
            assert_eq!(
                parser.parse_expr().unwrap(),
                Expr::Value(Value::Number("-9223372036854775808".to_owned()))
            )
        });
    }

    #[test]
    fn test_parse_function_arg_secret_ref() {
        use crate::ast::{FunctionArg, FunctionArgExpr, SecretRefAsType, SecretRefValue};

        // Unnamed secret argument
        run_parser_method("SECRET my_secret", |parser| {
            let (_variadic, arg) = parser.parse_function_args().unwrap();
            assert_eq!(
                arg,
                FunctionArg::Unnamed(FunctionArgExpr::SecretRef(SecretRefValue {
                    secret_name: ObjectName(vec![Ident::new_unchecked("my_secret")]),
                    ref_as: SecretRefAsType::Text,
                }))
            );
        });

        // Unnamed secret argument with AS FILE
        run_parser_method("SECRET my_secret AS FILE", |parser| {
            let (_variadic, arg) = parser.parse_function_args().unwrap();
            assert_eq!(
                arg,
                FunctionArg::Unnamed(FunctionArgExpr::SecretRef(SecretRefValue {
                    secret_name: ObjectName(vec![Ident::new_unchecked("my_secret")]),
                    ref_as: SecretRefAsType::File,
                }))
            );
        });

        // Named secret argument
        run_parser_method("header => SECRET my_secret", |parser| {
            let (_variadic, arg) = parser.parse_function_args().unwrap();
            assert_eq!(
                arg,
                FunctionArg::Named {
                    name: Ident::new_unchecked("header"),
                    arg: FunctionArgExpr::SecretRef(SecretRefValue {
                        secret_name: ObjectName(vec![Ident::new_unchecked("my_secret")]),
                        ref_as: SecretRefAsType::Text,
                    }),
                }
            );
        });
    }
}
