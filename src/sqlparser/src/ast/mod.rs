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

//! SQL Abstract Syntax Tree (AST) types
mod analyze;
mod data_type;
pub(crate) mod ddl;
mod legacy_source;
mod operator;
mod query;
mod statement;
mod value;

use std::collections::HashSet;
use std::fmt::{self, Display};
use std::sync::Arc;

use itertools::Itertools;
use winnow::ModalResult;

pub use self::data_type::{DataType, StructField};
pub use self::ddl::{
    AlterColumnOperation, AlterCompactionGroupOperation, AlterConnectionOperation,
    AlterDatabaseOperation, AlterFragmentOperation, AlterFunctionOperation, AlterRateLimit,
    AlterRateLimitType, AlterSchemaOperation, AlterSecretOperation, AlterTableOperation, ColumnDef,
    ColumnOption, ColumnOptionDef, ReferentialAction, SourceWatermark, TableConstraint,
    WebhookSourceInfo,
};
pub use self::legacy_source::{CompatibleFormatEncode, get_delimiter};
pub use self::operator::{BinaryOperator, QualifiedOperator, UnaryOperator};
pub use self::query::{
    Corresponding, Cte, CteInner, Distinct, Fetch, Join, JoinConstraint, JoinOperator, LateralView,
    NamedWindow, OrderByExpr, Query, Select, SelectItem, SetExpr, SetOperator, TableAlias,
    TableFactor, TableWithJoins, Top, Values, With,
};
pub use self::statement::*;
pub use self::value::{
    ConnectionRefValue, CstyleEscapedString, DateTimeField, DollarQuotedString, JsonPredicateType,
    SecretRefAsType, SecretRefValue, TrimWhereField, Value,
};
pub use crate::ast::analyze::AnalyzeTarget;
pub use crate::ast::ddl::{
    AlterIndexOperation, AlterSinkOperation, AlterSourceOperation, AlterSubscriptionOperation,
    AlterViewOperation,
};
use crate::keywords::Keyword;
use crate::parser::{IncludeOption, IncludeOptionItem, Parser, ParserError, StrError};
pub use crate::quote_ident::QuoteIdent;
use crate::tokenizer::Tokenizer;

pub type RedactSqlOptionKeywordsRef = Arc<HashSet<String>>;

task_local::task_local! {
    pub static REDACT_SQL_OPTION_KEYWORDS: RedactSqlOptionKeywordsRef;
}

pub struct DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    slice: &'a [T],
    sep: &'static str,
}

impl<T> fmt::Display for DisplaySeparated<'_, T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut delim = "";
        for t in self.slice {
            write!(f, "{}", delim)?;
            delim = self.sep;
            write!(f, "{}", t)?;
        }
        Ok(())
    }
}

pub fn display_separated<'a, T>(slice: &'a [T], sep: &'static str) -> DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    DisplaySeparated { slice, sep }
}

pub fn display_comma_separated<T>(slice: &[T]) -> DisplaySeparated<'_, T>
where
    T: fmt::Display,
{
    DisplaySeparated { slice, sep: ", " }
}

/// An identifier, decomposed into its value or character data and the quote style.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ident {
    /// The value of the identifier without quotes.
    pub(crate) value: String,
    /// The starting quote if any. Valid quote characters are the single quote,
    /// double quote, backtick, and opening square bracket.
    pub(crate) quote_style: Option<char>,
}

impl Ident {
    /// Create a new identifier with the given value and no quotes.
    /// the given value must not be a empty string.
    // FIXME: should avoid using this function unless it's a literal or for testing.
    pub fn new_unchecked<S>(value: S) -> Self
    where
        S: Into<String>,
    {
        Ident {
            value: value.into(),
            quote_style: None,
        }
    }

    /// Create a new quoted identifier with the given quote and value.
    /// the given value must not be a empty string and the given quote must be in ['\'', '"', '`',
    /// '['].
    pub fn with_quote_unchecked<S>(quote: char, value: S) -> Self
    where
        S: Into<String>,
    {
        Ident {
            value: value.into(),
            quote_style: Some(quote),
        }
    }

    /// Create a new quoted identifier with the given quote and value.
    /// returns ParserError when the given string is empty or the given quote is illegal.
    pub fn with_quote_check<S>(quote: char, value: S) -> Result<Ident, ParserError>
    where
        S: Into<String>,
    {
        let value_str = value.into();
        if value_str.is_empty() {
            return Err(ParserError::ParserError(format!(
                "zero-length delimited identifier at or near \"{value_str}\""
            )));
        }

        if !(quote == '\'' || quote == '"' || quote == '`' || quote == '[') {
            return Err(ParserError::ParserError(
                "unexpected quote style".to_owned(),
            ));
        }

        Ok(Ident {
            value: value_str,
            quote_style: Some(quote),
        })
    }

    /// Returns the identifier value used for name lookup and comparison.
    ///
    /// Unquoted identifiers are folded to lowercase, while double-quoted identifiers preserve
    /// their case and unescaped contents. For example, `Foo` becomes `foo`, while `"Foo"` remains
    /// `Foo` and `"a""b"` becomes `a"b`.
    pub fn real_value(&self) -> String {
        match self.quote_style {
            Some('"') => self.value.clone(),
            _ => self.value.to_lowercase(),
        }
    }

    /// Creates an identifier from a name used internally by the database.
    ///
    /// The input is stored as its unescaped value. Double quotes are selected when needed to
    /// preserve the name in SQL; escaping embedded quotes is left to the [`Display`] implementation.
    /// This behaves like the SQL `quote_ident` function and the [`QuoteIdent`] wrapper.
    pub fn from_real_value(value: &str) -> Self {
        let needs_quotes = QuoteIdent::needs_quotes(value);

        if needs_quotes {
            Self::with_quote_unchecked('"', value)
        } else {
            Self::new_unchecked(value)
        }
    }

    pub fn quote_style(&self) -> Option<char> {
        self.quote_style
    }
}

impl From<&str> for Ident {
    fn from(value: &str) -> Self {
        Self::from_real_value(value)
    }
}

impl ParseTo for Ident {
    fn parse_to(parser: &mut Parser<'_>) -> ModalResult<Self> {
        parser.parse_identifier()
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.quote_style {
            Some(q) if q == '\'' || q == '`' => write!(f, "{}{}{}", q, self.value, q),
            Some('"') => write!(f, "\"{}\"", self.value.replace('"', "\"\"")),
            Some('[') => write!(f, "[{}]", self.value),
            None => f.write_str(&self.value),
            _ => panic!("unexpected quote style"),
        }
    }
}

/// A name of a table, view, custom type, etc., possibly multi-part, i.e. db.schema.obj
///
/// Is is ensured to be non-empty.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectName(pub Vec<Ident>);

impl ObjectName {
    pub fn real_value(&self) -> String {
        self.0
            .iter()
            .map(|ident| ident.real_value())
            .collect::<Vec<_>>()
            .join(".")
    }

    pub fn from_test_str(s: &str) -> Self {
        ObjectName::from(vec![s.into()])
    }

    pub fn base_name(&self) -> String {
        self.0
            .iter()
            .last()
            .expect("should have base name")
            .real_value()
    }
}

impl fmt::Display for ObjectName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", display_separated(&self.0, "."))
    }
}

impl ParseTo for ObjectName {
    fn parse_to(p: &mut Parser<'_>) -> ModalResult<Self> {
        p.parse_object_name()
    }
}

impl From<Vec<Ident>> for ObjectName {
    fn from(value: Vec<Ident>) -> Self {
        Self(value)
    }
}

/// For array type `ARRAY[..]` or `[..]`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Array {
    /// The list of expressions between brackets
    pub elem: Vec<Expr>,

    /// `true` for  `ARRAY[..]`, `false` for `[..]`
    pub named: bool,
}

impl fmt::Display for Array {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}[{}]",
            if self.named { "ARRAY" } else { "" },
            display_comma_separated(&self.elem)
        )
    }
}

/// An escape character, to represent '' or a single character.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct EscapeChar(Option<char>);

impl EscapeChar {
    pub fn escape(ch: char) -> Self {
        Self(Some(ch))
    }

    pub fn empty() -> Self {
        Self(None)
    }
}

impl fmt::Display for EscapeChar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(ch) => write!(f, "{}", ch),
            None => f.write_str(""),
        }
    }
}

/// An SQL expression of any type.
///
/// The parser does not distinguish between expressions of different types
/// (e.g. boolean vs string), so the caller must handle expressions of
/// inappropriate type, like `WHERE 1` or `SELECT 1=1`, as necessary.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expr {
    /// Identifier e.g. table name or column name
    Identifier(Ident),
    /// Multi-part identifier, e.g. `table_alias.column` or `schema.table.col`
    CompoundIdentifier(Vec<Ident>),
    /// Struct-field identifier.
    /// Expr is an arbitrary expression, returning either a table or a column.
    /// Idents are consecutive field accesses.
    /// e.g. `(table.v1).v2` or `(table).v1.v2`
    ///
    /// It must contain parentheses to be distinguished from a [`Expr::CompoundIdentifier`].
    /// See also <https://www.postgresql.org/docs/current/rowtypes.html#ROWTYPES-ACCESSING>
    ///
    /// The left parentheses must be put at the beginning of the expression.
    /// The first parenthesized part is the `expr` part, and the rest are flattened into `idents`.
    /// e.g., `((v1).v2.v3).v4` is equivalent to `(v1).v2.v3.v4`.
    FieldIdentifier(Box<Expr>, Vec<Ident>),
    /// `IS NULL` operator
    IsNull(Box<Expr>),
    /// `IS NOT NULL` operator
    IsNotNull(Box<Expr>),
    /// `IS TRUE` operator
    IsTrue(Box<Expr>),
    /// `IS NOT TRUE` operator
    IsNotTrue(Box<Expr>),
    /// `IS FALSE` operator
    IsFalse(Box<Expr>),
    /// `IS NOT FALSE` operator
    IsNotFalse(Box<Expr>),
    /// `IS UNKNOWN` operator
    IsUnknown(Box<Expr>),
    /// `IS NOT UNKNOWN` operator
    IsNotUnknown(Box<Expr>),
    /// `IS DISTINCT FROM` operator
    IsDistinctFrom(Box<Expr>, Box<Expr>),
    /// `IS NOT DISTINCT FROM` operator
    IsNotDistinctFrom(Box<Expr>, Box<Expr>),
    /// ```text
    /// IS [ NOT ] JSON [ VALUE | ARRAY | OBJECT | SCALAR ]
    /// [ { WITH | WITHOUT } UNIQUE [ KEYS ] ]
    /// ```
    IsJson {
        expr: Box<Expr>,
        negated: bool,
        item_type: JsonPredicateType,
        unique_keys: bool,
    },
    /// `[ NOT ] IN (val1, val2, ...)`
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Query>,
        negated: bool,
    },
    /// `<expr> [ NOT ] BETWEEN <low> AND <high>`
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    /// LIKE
    Like {
        negated: bool,
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape_char: Option<EscapeChar>,
    },
    /// ILIKE (case-insensitive LIKE)
    ILike {
        negated: bool,
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape_char: Option<EscapeChar>,
    },
    /// `<expr> [ NOT ] SIMILAR TO <pat> ESCAPE <esc_text>`
    SimilarTo {
        negated: bool,
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape_char: Option<EscapeChar>,
    },
    /// Binary operation e.g. `1 + 1` or `foo > bar`
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// Some operation e.g. `foo > Some(bar)`, It will be wrapped in the right side of BinaryExpr
    SomeOp(Box<Expr>),
    /// ALL operation e.g. `foo > ALL(bar)`, It will be wrapped in the right side of BinaryExpr
    AllOp(Box<Expr>),
    /// Unary operation e.g. `NOT foo`
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    /// CAST an expression to a different data type e.g. `CAST(foo AS VARCHAR)`
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    /// TRY_CAST an expression to a different data type e.g. `TRY_CAST(foo AS VARCHAR)`
    //  this differs from CAST in the choice of how to implement invalid conversions
    TryCast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    /// AT TIME ZONE converts `timestamp without time zone` to/from `timestamp with time zone` with
    /// explicitly specified zone
    AtTimeZone {
        timestamp: Box<Expr>,
        time_zone: Box<Expr>,
    },
    /// `EXTRACT(DateTimeField FROM <expr>)`
    Extract {
        field: String,
        expr: Box<Expr>,
    },
    /// `SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])`
    Substring {
        expr: Box<Expr>,
        substring_from: Option<Box<Expr>>,
        substring_for: Option<Box<Expr>>,
    },
    /// `POSITION(<expr> IN <expr>)`
    Position {
        substring: Box<Expr>,
        string: Box<Expr>,
    },
    /// `OVERLAY(<expr> PLACING <expr> FROM <expr> [ FOR <expr> ])`
    Overlay {
        expr: Box<Expr>,
        new_substring: Box<Expr>,
        start: Box<Expr>,
        count: Option<Box<Expr>>,
    },
    /// `TRIM([BOTH | LEADING | TRAILING] [<expr>] FROM <expr>)`\
    /// Or\
    /// `TRIM([BOTH | LEADING | TRAILING] [FROM] <expr> [, <expr>])`
    Trim {
        expr: Box<Expr>,
        // ([BOTH | LEADING | TRAILING], <expr>)
        trim_where: Option<TrimWhereField>,
        trim_what: Option<Box<Expr>>,
    },
    /// `expr COLLATE collation`
    Collate {
        expr: Box<Expr>,
        collation: ObjectName,
    },
    /// Nested expression e.g. `(foo > bar)` or `(1)`
    Nested(Box<Expr>),
    /// A literal value, such as string, number, date or NULL
    Value(Value),
    /// Parameter Symbol e.g. `$1`, `$1::int`
    Parameter {
        index: u64,
    },
    /// A constant of form `<data_type> 'value'`.
    /// This can represent ANSI SQL `DATE`, `TIME`, and `TIMESTAMP` literals (such as `DATE
    /// '2020-01-01'`), as well as constants of other types (a non-standard PostgreSQL extension).
    TypedString {
        data_type: DataType,
        value: String,
    },
    /// Scalar function call e.g. `LEFT(foo, 5)`
    Function(Function),
    /// `CASE [<operand>] WHEN <condition> THEN <result> ... [ELSE <result>] END`
    ///
    /// Note we only recognize a complete single expression as `<condition>`,
    /// not `< 0` nor `1, 2, 3` as allowed in a `<simple when clause>` per
    /// <https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html#simple-when-clause>
    Case {
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    },
    /// An exists expression `EXISTS(SELECT ...)`, used in expressions like
    /// `WHERE EXISTS (SELECT ...)`.
    Exists(Box<Query>),
    /// A parenthesized subquery `(SELECT ...)`, used in expression like
    /// `SELECT (subquery) AS x` or `WHERE (subquery) = x`
    Subquery(Box<Query>),
    /// The `GROUPING SETS` expr.
    GroupingSets(Vec<Vec<Expr>>),
    /// The `CUBE` expr.
    Cube(Vec<Vec<Expr>>),
    /// The `ROLLUP` expr.
    Rollup(Vec<Vec<Expr>>),
    /// The `ROW` expr. The `ROW` keyword can be omitted,
    Row(Vec<Expr>),
    /// An array constructor `ARRAY[[2,3,4],[5,6,7]]`
    Array(Array),
    /// An array constructing subquery `ARRAY(SELECT 2 UNION SELECT 3)`
    ArraySubquery(Box<Query>),
    /// A subscript expression `arr[1]` or `map['a']`
    Index {
        obj: Box<Expr>,
        index: Box<Expr>,
    },
    /// A slice expression `arr[1:3]`
    ArrayRangeIndex {
        obj: Box<Expr>,
        start: Option<Box<Expr>>,
        end: Option<Box<Expr>>,
    },
    LambdaFunction {
        args: Vec<Ident>,
        body: Box<Expr>,
    },
    Map {
        entries: Vec<(Expr, Expr)>,
    },
}

impl fmt::Display for Expr {
    #[expect(clippy::disallowed_methods, reason = "use zip_eq")]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Identifier(s) => write!(f, "{}", s),
            Expr::CompoundIdentifier(s) => write!(f, "{}", display_separated(s, ".")),
            Expr::FieldIdentifier(ast, s) => write!(f, "({}).{}", ast, display_separated(s, ".")),
            Expr::IsNull(ast) => write!(f, "{} IS NULL", ast),
            Expr::IsNotNull(ast) => write!(f, "{} IS NOT NULL", ast),
            Expr::IsTrue(ast) => write!(f, "{} IS TRUE", ast),
            Expr::IsNotTrue(ast) => write!(f, "{} IS NOT TRUE", ast),
            Expr::IsFalse(ast) => write!(f, "{} IS FALSE", ast),
            Expr::IsNotFalse(ast) => write!(f, "{} IS NOT FALSE", ast),
            Expr::IsUnknown(ast) => write!(f, "{} IS UNKNOWN", ast),
            Expr::IsNotUnknown(ast) => write!(f, "{} IS NOT UNKNOWN", ast),
            Expr::IsJson {
                expr,
                negated,
                item_type,
                unique_keys,
            } => write!(
                f,
                "{} IS {}JSON{}{}",
                expr,
                if *negated { "NOT " } else { "" },
                item_type,
                if *unique_keys {
                    " WITH UNIQUE KEYS"
                } else {
                    ""
                },
            ),
            Expr::InList {
                expr,
                list,
                negated,
            } => write!(
                f,
                "{} {}IN ({})",
                expr,
                if *negated { "NOT " } else { "" },
                display_comma_separated(list)
            ),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => write!(
                f,
                "{} {}IN ({})",
                expr,
                if *negated { "NOT " } else { "" },
                subquery
            ),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => write!(
                f,
                "{} {}BETWEEN {} AND {}",
                expr,
                if *negated { "NOT " } else { "" },
                low,
                high
            ),
            Expr::Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => match escape_char {
                Some(ch) => write!(
                    f,
                    "{} {}LIKE {} ESCAPE '{}'",
                    expr,
                    if *negated { "NOT " } else { "" },
                    pattern,
                    ch
                ),
                _ => write!(
                    f,
                    "{} {}LIKE {}",
                    expr,
                    if *negated { "NOT " } else { "" },
                    pattern
                ),
            },
            Expr::ILike {
                negated,
                expr,
                pattern,
                escape_char,
            } => match escape_char {
                Some(ch) => write!(
                    f,
                    "{} {}ILIKE {} ESCAPE '{}'",
                    expr,
                    if *negated { "NOT " } else { "" },
                    pattern,
                    ch
                ),
                _ => write!(
                    f,
                    "{} {}ILIKE {}",
                    expr,
                    if *negated { "NOT " } else { "" },
                    pattern
                ),
            },
            Expr::SimilarTo {
                negated,
                expr,
                pattern,
                escape_char,
            } => match escape_char {
                Some(ch) => write!(
                    f,
                    "{} {}SIMILAR TO {} ESCAPE '{}'",
                    expr,
                    if *negated { "NOT " } else { "" },
                    pattern,
                    ch
                ),
                _ => write!(
                    f,
                    "{} {}SIMILAR TO {}",
                    expr,
                    if *negated { "NOT " } else { "" },
                    pattern
                ),
            },
            Expr::BinaryOp { left, op, right } => write!(f, "{} {} {}", left, op, right),
            Expr::SomeOp(expr) => write!(f, "SOME({})", expr),
            Expr::AllOp(expr) => write!(f, "ALL({})", expr),
            Expr::UnaryOp { op, expr } => {
                write!(f, "{} {}", op, expr)
            }
            Expr::Cast { expr, data_type } => write!(f, "CAST({} AS {})", expr, data_type),
            Expr::TryCast { expr, data_type } => write!(f, "TRY_CAST({} AS {})", expr, data_type),
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => write!(f, "{} AT TIME ZONE {}", timestamp, time_zone),
            Expr::Extract { field, expr } => write!(f, "EXTRACT({} FROM {})", field, expr),
            Expr::Collate { expr, collation } => write!(f, "{} COLLATE {}", expr, collation),
            Expr::Nested(ast) => write!(f, "({})", ast),
            Expr::Value(v) => write!(f, "{}", v),
            Expr::Parameter { index } => write!(f, "${}", index),
            Expr::TypedString { data_type, value } => {
                write!(f, "{}", data_type)?;
                write!(f, " '{}'", value::escape_single_quote_string(value))
            }
            Expr::Function(fun) => write!(f, "{}", fun),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                write!(f, "CASE")?;
                if let Some(operand) = operand {
                    write!(f, " {}", operand)?;
                }
                for (c, r) in conditions.iter().zip_eq(results) {
                    write!(f, " WHEN {} THEN {}", c, r)?;
                }

                if let Some(else_result) = else_result {
                    write!(f, " ELSE {}", else_result)?;
                }
                write!(f, " END")
            }
            Expr::Exists(s) => write!(f, "EXISTS ({})", s),
            Expr::Subquery(s) => write!(f, "({})", s),
            Expr::GroupingSets(sets) => {
                write!(f, "GROUPING SETS (")?;
                let mut sep = "";
                for set in sets {
                    write!(f, "{}", sep)?;
                    sep = ", ";
                    write!(f, "({})", display_comma_separated(set))?;
                }
                write!(f, ")")
            }
            Expr::Cube(sets) => {
                write!(f, "CUBE (")?;
                let mut sep = "";
                for set in sets {
                    write!(f, "{}", sep)?;
                    sep = ", ";
                    if set.len() == 1 {
                        write!(f, "{}", set[0])?;
                    } else {
                        write!(f, "({})", display_comma_separated(set))?;
                    }
                }
                write!(f, ")")
            }
            Expr::Rollup(sets) => {
                write!(f, "ROLLUP (")?;
                let mut sep = "";
                for set in sets {
                    write!(f, "{}", sep)?;
                    sep = ", ";
                    if set.len() == 1 {
                        write!(f, "{}", set[0])?;
                    } else {
                        write!(f, "({})", display_comma_separated(set))?;
                    }
                }
                write!(f, ")")
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
            } => {
                write!(f, "SUBSTRING({}", expr)?;
                if let Some(from_part) = substring_from {
                    write!(f, " FROM {}", from_part)?;
                }
                if let Some(from_part) = substring_for {
                    write!(f, " FOR {}", from_part)?;
                }

                write!(f, ")")
            }
            Expr::Position { substring, string } => {
                write!(f, "POSITION({} IN {})", substring, string)
            }
            Expr::Overlay {
                expr,
                new_substring,
                start,
                count,
            } => {
                write!(f, "OVERLAY({}", expr)?;
                write!(f, " PLACING {}", new_substring)?;
                write!(f, " FROM {}", start)?;

                if let Some(count_expr) = count {
                    write!(f, " FOR {}", count_expr)?;
                }

                write!(f, ")")
            }
            Expr::IsDistinctFrom(a, b) => write!(f, "{} IS DISTINCT FROM {}", a, b),
            Expr::IsNotDistinctFrom(a, b) => write!(f, "{} IS NOT DISTINCT FROM {}", a, b),
            Expr::Trim {
                expr,
                trim_where,
                trim_what,
            } => {
                write!(f, "TRIM(")?;
                if let Some(ident) = trim_where {
                    write!(f, "{} ", ident)?;
                }
                if let Some(trim_char) = trim_what {
                    write!(f, "{} ", trim_char)?;
                }
                write!(f, "FROM {})", expr)
            }
            Expr::Row(exprs) => write!(
                f,
                "ROW({})",
                exprs
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<String>>()
                    .as_slice()
                    .join(", ")
            ),
            Expr::Index { obj, index } => {
                write!(f, "{}[{}]", obj, index)?;
                Ok(())
            }
            Expr::ArrayRangeIndex { obj, start, end } => {
                let start_str = match start {
                    None => "".to_owned(),
                    Some(start) => format!("{}", start),
                };
                let end_str = match end {
                    None => "".to_owned(),
                    Some(end) => format!("{}", end),
                };
                write!(f, "{}[{}:{}]", obj, start_str, end_str)?;
                Ok(())
            }
            Expr::Array(exprs) => write!(f, "{}", exprs),
            Expr::ArraySubquery(s) => write!(f, "ARRAY ({})", s),
            Expr::LambdaFunction { args, body } => {
                write!(
                    f,
                    "|{}| {}",
                    args.iter().map(ToString::to_string).join(", "),
                    body
                )
            }
            Expr::Map { entries } => {
                write!(
                    f,
                    "MAP {{{}}}",
                    entries
                        .iter()
                        .map(|(k, v)| format!("{}: {}", k, v))
                        .join(", ")
                )
            }
        }
    }
}

/// A window specification (i.e. `OVER (PARTITION BY .. ORDER BY .. etc.)`).
/// This is used both for named window definitions and inline window specifications.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub window_frame: Option<WindowFrame>,
}

/// A window definition that can appear in the OVER clause of a window function.
/// This can be either an inline window specification or a reference to a named window.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Window {
    /// Inline window specification: `OVER (PARTITION BY ... ORDER BY ...)`
    Spec(WindowSpec),
    /// Named window reference: `OVER window_name`
    Name(Ident),
}

impl fmt::Display for WindowSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut delim = "";
        if !self.partition_by.is_empty() {
            delim = " ";
            write!(
                f,
                "PARTITION BY {}",
                display_comma_separated(&self.partition_by)
            )?;
        }
        if !self.order_by.is_empty() {
            f.write_str(delim)?;
            delim = " ";
            write!(f, "ORDER BY {}", display_comma_separated(&self.order_by))?;
        }
        if let Some(window_frame) = &self.window_frame {
            f.write_str(delim)?;
            window_frame.fmt(f)?;
        }
        Ok(())
    }
}

impl fmt::Display for Window {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Window::Spec(spec) => write!(f, "({})", spec),
            Window::Name(name) => write!(f, "{}", name),
        }
    }
}

/// Specifies the data processed by a window function, e.g.
/// `RANGE UNBOUNDED PRECEDING` or `ROWS BETWEEN 5 PRECEDING AND CURRENT ROW`.
///
/// Note: The parser does not validate the specified bounds; the caller should
/// reject invalid bounds like `ROWS UNBOUNDED FOLLOWING` before execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub bounds: WindowFrameBounds,
    pub exclusion: Option<WindowFrameExclusion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
    Session,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WindowFrameBounds {
    Bounds {
        start: WindowFrameBound,
        /// The right bound of the `BETWEEN .. AND` clause. The end bound of `None`
        /// indicates the shorthand form (e.g. `ROWS 1 PRECEDING`), which must
        /// behave the same as `end_bound = WindowFrameBound::CurrentRow`.
        end: Option<WindowFrameBound>,
    },
    Gap(Box<Expr>),
}

impl fmt::Display for WindowFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ", self.units)?;
        match &self.bounds {
            WindowFrameBounds::Bounds { start, end } => {
                if let Some(end) = end {
                    write!(f, "BETWEEN {} AND {}", start, end)
                } else {
                    write!(f, "{}", start)
                }
            }
            WindowFrameBounds::Gap(gap) => {
                write!(f, "WITH GAP {}", gap)
            }
        }
    }
}

impl fmt::Display for WindowFrameUnits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            WindowFrameUnits::Rows => "ROWS",
            WindowFrameUnits::Range => "RANGE",
            WindowFrameUnits::Groups => "GROUPS",
            WindowFrameUnits::Session => "SESSION",
        })
    }
}

/// Specifies [WindowFrame]'s `start_bound` and `end_bound`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WindowFrameBound {
    /// `CURRENT ROW`
    CurrentRow,
    /// `<offset> PRECEDING` or `UNBOUNDED PRECEDING`
    Preceding(Option<Box<Expr>>),
    /// `<offset> FOLLOWING` or `UNBOUNDED FOLLOWING`.
    Following(Option<Box<Expr>>),
}

impl fmt::Display for WindowFrameBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WindowFrameBound::CurrentRow => f.write_str("CURRENT ROW"),
            WindowFrameBound::Preceding(None) => f.write_str("UNBOUNDED PRECEDING"),
            WindowFrameBound::Following(None) => f.write_str("UNBOUNDED FOLLOWING"),
            WindowFrameBound::Preceding(Some(n)) => write!(f, "{} PRECEDING", n),
            WindowFrameBound::Following(Some(n)) => write!(f, "{} FOLLOWING", n),
        }
    }
}

/// Frame exclusion option of [WindowFrame].
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WindowFrameExclusion {
    CurrentRow,
    Group,
    Ties,
    NoOthers,
}

impl fmt::Display for WindowFrameExclusion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WindowFrameExclusion::CurrentRow => f.write_str("EXCLUDE CURRENT ROW"),
            WindowFrameExclusion::Group => f.write_str("EXCLUDE GROUP"),
            WindowFrameExclusion::Ties => f.write_str("EXCLUDE TIES"),
            WindowFrameExclusion::NoOthers => f.write_str("EXCLUDE NO OTHERS"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AddDropSync {
    ADD,
    DROP,
    SYNC,
}

impl fmt::Display for AddDropSync {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AddDropSync::SYNC => f.write_str("SYNC PARTITIONS"),
            AddDropSync::DROP => f.write_str("DROP PARTITIONS"),
            AddDropSync::ADD => f.write_str("ADD PARTITIONS"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ShowObject {
    Table { schema: Option<Ident> },
    InternalTable { schema: Option<Ident> },
    Database,
    Schema,
    View { schema: Option<Ident> },
    MaterializedView { schema: Option<Ident> },
    Source { schema: Option<Ident> },
    Sink { schema: Option<Ident> },
    Subscription { schema: Option<Ident> },
    Columns { table: ObjectName },
    Connection { schema: Option<Ident> },
    Secret { schema: Option<Ident> },
    Function { schema: Option<Ident> },
    Indexes { table: ObjectName },
    Cluster,
    Jobs,
    ProcessList,
    Cursor,
    SubscriptionCursor,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JobIdents(pub Vec<u32>);

impl fmt::Display for ShowObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn fmt_schema(schema: &Option<Ident>) -> String {
            if let Some(schema) = schema {
                format!(" FROM {}", schema.value)
            } else {
                "".to_owned()
            }
        }

        match self {
            ShowObject::Database => f.write_str("DATABASES"),
            ShowObject::Schema => f.write_str("SCHEMAS"),
            ShowObject::Table { schema } => {
                write!(f, "TABLES{}", fmt_schema(schema))
            }
            ShowObject::InternalTable { schema } => {
                write!(f, "INTERNAL TABLES{}", fmt_schema(schema))
            }
            ShowObject::View { schema } => {
                write!(f, "VIEWS{}", fmt_schema(schema))
            }
            ShowObject::MaterializedView { schema } => {
                write!(f, "MATERIALIZED VIEWS{}", fmt_schema(schema))
            }
            ShowObject::Source { schema } => write!(f, "SOURCES{}", fmt_schema(schema)),
            ShowObject::Sink { schema } => write!(f, "SINKS{}", fmt_schema(schema)),
            ShowObject::Columns { table } => write!(f, "COLUMNS FROM {}", table),
            ShowObject::Connection { schema } => write!(f, "CONNECTIONS{}", fmt_schema(schema)),
            ShowObject::Function { schema } => write!(f, "FUNCTIONS{}", fmt_schema(schema)),
            ShowObject::Indexes { table } => write!(f, "INDEXES FROM {}", table),
            ShowObject::Cluster => {
                write!(f, "CLUSTER")
            }
            ShowObject::Jobs => write!(f, "JOBS"),
            ShowObject::ProcessList => write!(f, "PROCESSLIST"),
            ShowObject::Subscription { schema } => write!(f, "SUBSCRIPTIONS{}", fmt_schema(schema)),
            ShowObject::Secret { schema } => write!(f, "SECRETS{}", fmt_schema(schema)),
            ShowObject::Cursor => write!(f, "CURSORS"),
            ShowObject::SubscriptionCursor => write!(f, "SUBSCRIPTION CURSORS"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ShowCreateType {
    Table,
    MaterializedView,
    View,
    Index,
    Source,
    Sink,
    Function,
    Subscription,
}

impl fmt::Display for ShowCreateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShowCreateType::Table => f.write_str("TABLE"),
            ShowCreateType::MaterializedView => f.write_str("MATERIALIZED VIEW"),
            ShowCreateType::View => f.write_str("VIEW"),
            ShowCreateType::Index => f.write_str("INDEX"),
            ShowCreateType::Source => f.write_str("SOURCE"),
            ShowCreateType::Sink => f.write_str("SINK"),
            ShowCreateType::Function => f.write_str("FUNCTION"),
            ShowCreateType::Subscription => f.write_str("SUBSCRIPTION"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CommentObject {
    Column,
    Table,
}

impl fmt::Display for CommentObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommentObject::Column => f.write_str("COLUMN"),
            CommentObject::Table => f.write_str("TABLE"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExplainType {
    Logical,
    Physical,
    DistSql,
}

impl fmt::Display for ExplainType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExplainType::Logical => f.write_str("Logical"),
            ExplainType::Physical => f.write_str("Physical"),
            ExplainType::DistSql => f.write_str("DistSQL"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExplainFormat {
    Text,
    Json,
    Xml,
    Yaml,
    Dot,
}

impl fmt::Display for ExplainFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExplainFormat::Text => f.write_str("TEXT"),
            ExplainFormat::Json => f.write_str("JSON"),
            ExplainFormat::Xml => f.write_str("XML"),
            ExplainFormat::Yaml => f.write_str("YAML"),
            ExplainFormat::Dot => f.write_str("DOT"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExplainOptions {
    /// Display additional information regarding the plan.
    pub verbose: bool,
    // Trace plan transformation of the optimizer step by step
    pub trace: bool,
    // Display backfill order
    pub backfill: bool,
    // explain's plan type
    pub explain_type: ExplainType,
    // explain's plan format
    pub explain_format: ExplainFormat,
}

impl Default for ExplainOptions {
    fn default() -> Self {
        Self {
            verbose: false,
            trace: false,
            backfill: false,
            explain_type: ExplainType::Physical,
            explain_format: ExplainFormat::Text,
        }
    }
}

impl fmt::Display for ExplainOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let default = Self::default();
        if *self == default {
            Ok(())
        } else {
            let mut option_strs = vec![];
            if self.verbose {
                option_strs.push("VERBOSE".to_owned());
            }
            if self.trace {
                option_strs.push("TRACE".to_owned());
            }
            if self.backfill {
                option_strs.push("BACKFILL".to_owned());
            }
            if self.explain_type == default.explain_type {
                option_strs.push(self.explain_type.to_string());
            }
            if self.explain_format == default.explain_format {
                option_strs.push(self.explain_format.to_string());
            }
            write!(f, "{}", option_strs.iter().format(","))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CdcTableInfo {
    pub source_name: ObjectName,
    pub external_table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CopyEntity {
    Query(Box<Query>),
    Table {
        /// TABLE
        table_name: ObjectName,
        /// COLUMNS
        columns: Vec<Ident>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CopyTarget {
    Stdin {
        /// VALUES a vector of values to be copied
        values: Vec<Option<String>>,
    },
    Stdout,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WaitTarget {
    All,
    Table(ObjectName),
    MaterializedView(ObjectName),
    Sink(ObjectName),
    Index(ObjectName),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileCacheType {
    Meta,
    Data,
    All,
}

/// A top-level statement (SELECT, INSERT, CREATE, etc.)

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Statement {
    /// Analyze (Hive)
    Analyze {
        table_name: ObjectName,
    },
    /// Truncate (Hive)
    Truncate {
        table_name: ObjectName,
    },
    /// Refresh table
    Refresh {
        table_name: ObjectName,
    },
    /// SELECT
    Query(Box<Query>),
    /// INSERT
    Insert {
        /// TABLE
        table_name: ObjectName,
        /// COLUMNS
        columns: Vec<Ident>,
        /// A SQL query that specifies what to insert
        source: Box<Query>,
        /// Define output of this insert statement
        returning: Vec<SelectItem>,
    },
    Copy {
        entity: CopyEntity,
        target: CopyTarget,
    },
    /// UPDATE
    Update {
        /// TABLE
        table_name: ObjectName,
        /// Column assignments
        assignments: Vec<Assignment>,
        /// WHERE
        selection: Option<Expr>,
        /// RETURNING
        returning: Vec<SelectItem>,
    },
    /// DELETE
    Delete {
        /// FROM
        table_name: ObjectName,
        /// WHERE
        selection: Option<Expr>,
        /// RETURNING
        returning: Vec<SelectItem>,
    },
    /// DELETE META SNAPSHOT(S)
    DeleteMetaSnapshots {
        snapshot_ids: Vec<u64>,
    },
    /// DISCARD
    Discard(DiscardType),
    /// CREATE VIEW
    CreateView {
        or_replace: bool,
        materialized: bool,
        if_not_exists: bool,
        /// View name
        name: ObjectName,
        columns: Vec<Ident>,
        query: Box<Query>,
        emit_mode: Option<EmitMode>,
        with_options: Vec<SqlOption>,
    },
    /// CREATE TABLE
    CreateTable {
        or_replace: bool,
        temporary: bool,
        if_not_exists: bool,
        /// Table name
        name: ObjectName,
        /// Optional schema
        columns: Vec<ColumnDef>,
        // The wildchar position in columns defined in sql. Only exist when using external schema.
        wildcard_idx: Option<usize>,
        constraints: Vec<TableConstraint>,
        with_options: Vec<SqlOption>,
        /// `FORMAT ... ENCODE ...` for table with connector
        format_encode: Option<CompatibleFormatEncode>,
        /// The watermark defined on source.
        source_watermarks: Vec<SourceWatermark>,
        /// Append only table.
        append_only: bool,
        /// On conflict behavior
        on_conflict: Option<OnConflict>,
        /// with_version_columns behind on conflict - supports multiple version columns
        with_version_columns: Vec<Ident>,
        /// `AS ( query )`
        query: Option<Box<Query>>,
        /// `FROM cdc_source TABLE database_name.table_name`
        cdc_table_info: Option<CdcTableInfo>,
        /// `INCLUDE a AS b INCLUDE c`
        include_column_options: IncludeOption,
        /// `VALIDATE SECRET secure_secret_name AS secure_compare ()`
        webhook_info: Option<WebhookSourceInfo>,
        /// `Engine = [hummock | iceberg]`
        engine: Engine,
    },
    /// CREATE INDEX
    CreateIndex {
        /// index name
        name: ObjectName,
        table_name: ObjectName,
        columns: Vec<OrderByExpr>,
        method: Option<Ident>,
        include: Vec<Ident>,
        distributed_by: Vec<Expr>,
        unique: bool,
        if_not_exists: bool,
        with_properties: WithProperties,
    },
    /// CREATE SOURCE
    CreateSource {
        stmt: CreateSourceStatement,
    },
    /// CREATE SINK
    CreateSink {
        stmt: CreateSinkStatement,
    },
    /// CREATE SUBSCRIPTION
    CreateSubscription {
        stmt: CreateSubscriptionStatement,
    },
    /// CREATE CONNECTION
    CreateConnection {
        stmt: CreateConnectionStatement,
    },
    CreateSecret {
        stmt: CreateSecretStatement,
    },
    /// CREATE FUNCTION
    ///
    /// Postgres: <https://www.postgresql.org/docs/15/sql-createfunction.html>
    CreateFunction {
        or_replace: bool,
        temporary: bool,
        if_not_exists: bool,
        name: ObjectName,
        args: Option<Vec<OperateFunctionArg>>,
        returns: Option<CreateFunctionReturns>,
        /// Optional parameters.
        params: CreateFunctionBody,
        with_options: CreateFunctionWithOptions, // FIXME(eric): use Option<>
    },
    /// CREATE AGGREGATE
    ///
    /// Postgres: <https://www.postgresql.org/docs/15/sql-createaggregate.html>
    CreateAggregate {
        or_replace: bool,
        if_not_exists: bool,
        name: ObjectName,
        args: Vec<OperateFunctionArg>,
        returns: DataType,
        /// Optional parameters.
        append_only: bool,
        params: CreateFunctionBody,
    },

    /// DECLARE CURSOR
    DeclareCursor {
        stmt: DeclareCursorStatement,
    },

    // FETCH CURSOR
    FetchCursor {
        stmt: FetchCursorStatement,
    },

    // CLOSE CURSOR
    CloseCursor {
        stmt: CloseCursorStatement,
    },

    /// ALTER DATABASE
    AlterDatabase {
        name: ObjectName,
        operation: AlterDatabaseOperation,
    },
    /// ALTER SCHEMA
    AlterSchema {
        name: ObjectName,
        operation: AlterSchemaOperation,
    },
    /// ALTER TABLE
    AlterTable {
        /// Table name
        name: ObjectName,
        operation: AlterTableOperation,
    },
    /// ALTER INDEX
    AlterIndex {
        /// Index name
        name: ObjectName,
        operation: AlterIndexOperation,
    },
    /// ALTER VIEW
    AlterView {
        /// View name
        name: ObjectName,
        materialized: bool,
        operation: AlterViewOperation,
    },
    /// ALTER SINK
    AlterSink {
        /// Sink name
        name: ObjectName,
        operation: AlterSinkOperation,
    },
    AlterSubscription {
        name: ObjectName,
        operation: AlterSubscriptionOperation,
    },
    /// ALTER SOURCE
    AlterSource {
        /// Source name
        name: ObjectName,
        operation: AlterSourceOperation,
    },
    /// ALTER FUNCTION
    AlterFunction {
        /// Function name
        name: ObjectName,
        args: Option<Vec<OperateFunctionArg>>,
        operation: AlterFunctionOperation,
    },
    /// ALTER CONNECTION
    AlterConnection {
        /// Connection name
        name: ObjectName,
        operation: AlterConnectionOperation,
    },
    /// ALTER SECRET
    AlterSecret {
        /// Secret name
        name: ObjectName,
        operation: AlterSecretOperation,
    },
    /// ALTER FRAGMENT
    AlterFragment {
        fragment_ids: Vec<u32>,
        operation: AlterFragmentOperation,
    },
    /// ALTER COMPACTION GROUP
    AlterCompactionGroup {
        group_ids: Vec<u64>,
        operation: AlterCompactionGroupOperation,
    },
    /// DESCRIBE relation
    /// ALTER DEFAULT PRIVILEGES
    AlterDefaultPrivileges {
        target_users: Option<Vec<Ident>>,
        schema_names: Option<Vec<ObjectName>>,
        operation: DefaultPrivilegeOperation,
    },
    /// DESCRIBE relation
    Describe {
        /// relation name
        name: ObjectName,
        kind: DescribeKind,
    },
    /// DESCRIBE FRAGMENT <fragment_id>
    DescribeFragment {
        fragment_id: u32,
    },
    /// SHOW OBJECT COMMAND
    ShowObjects {
        object: ShowObject,
        filter: Option<ShowStatementFilter>,
    },
    /// SHOW CREATE COMMAND
    ShowCreateObject {
        /// Show create object type
        create_type: ShowCreateType,
        /// Show create object name
        name: ObjectName,
    },
    ShowTransactionIsolationLevel,
    /// CANCEL JOBS COMMAND
    CancelJobs(JobIdents),
    /// KILL COMMAND
    /// Kill process in the show processlist.
    Kill(String),
    /// DROP
    Drop(DropStatement),
    /// DROP FUNCTION
    DropFunction {
        if_exists: bool,
        /// One or more function to drop
        func_desc: Vec<FunctionDesc>,
        /// `CASCADE` or `RESTRICT`
        option: Option<ReferentialAction>,
    },
    /// DROP AGGREGATE
    DropAggregate {
        if_exists: bool,
        /// One or more function to drop
        func_desc: Vec<FunctionDesc>,
        /// `CASCADE` or `RESTRICT`
        option: Option<ReferentialAction>,
    },
    /// `SET <variable>`
    ///
    /// Note: this is not a standard SQL statement, but it is supported by at
    /// least MySQL and PostgreSQL. Not all MySQL-specific syntactic forms are
    /// supported yet.
    SetVariable {
        local: bool,
        variable: Ident,
        value: SetVariableValue,
    },
    /// `SHOW <variable>`
    ///
    /// Note: this is a PostgreSQL-specific statement.
    ShowVariable {
        variable: Vec<Ident>,
    },
    /// `START TRANSACTION ...`
    StartTransaction {
        modes: Vec<TransactionMode>,
    },
    /// `BEGIN [ TRANSACTION | WORK ]`
    Begin {
        modes: Vec<TransactionMode>,
    },
    /// ABORT
    Abort,
    /// `SET TRANSACTION ...`
    SetTransaction {
        modes: Vec<TransactionMode>,
        snapshot: Option<Value>,
        session: bool,
    },
    /// `SET [ SESSION | LOCAL ] TIME ZONE { value | 'value' | LOCAL | DEFAULT }`
    SetTimeZone {
        local: bool,
        value: SetTimeZoneValue,
    },
    /// `COMMENT ON ...`
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Comment {
        object_type: CommentObject,
        object_name: ObjectName,
        comment: Option<String>,
    },
    /// `COMMIT [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ]`
    Commit {
        chain: bool,
    },
    /// `ROLLBACK [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ]`
    Rollback {
        chain: bool,
    },
    /// CREATE SCHEMA
    CreateSchema {
        schema_name: ObjectName,
        if_not_exists: bool,
        owner: Option<ObjectName>,
    },
    /// CREATE DATABASE
    CreateDatabase {
        db_name: ObjectName,
        if_not_exists: bool,
        owner: Option<ObjectName>,
        resource_group: Option<SetVariableValue>,
        barrier_interval_ms: Option<u32>,
        checkpoint_frequency: Option<u64>,
    },
    /// GRANT privileges ON objects TO grantees
    Grant {
        privileges: Privileges,
        objects: GrantObjects,
        grantees: Vec<Ident>,
        with_grant_option: bool,
        granted_by: Option<Ident>,
    },
    /// REVOKE privileges ON objects FROM grantees
    Revoke {
        privileges: Privileges,
        objects: GrantObjects,
        grantees: Vec<Ident>,
        granted_by: Option<Ident>,
        revoke_grant_option: bool,
        cascade: bool,
    },
    /// `DEALLOCATE [ PREPARE ] { name | ALL }`
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Deallocate {
        name: Option<Ident>,
        prepare: bool,
    },
    /// `EXECUTE name [ ( parameter [, ...] ) ]`
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Execute {
        name: Ident,
        parameters: Vec<Expr>,
    },
    /// `PREPARE name [ ( data_type [, ...] ) ] AS statement`
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Prepare {
        name: Ident,
        data_types: Vec<DataType>,
        statement: Box<Statement>,
    },
    /// EXPLAIN / DESCRIBE for select_statement
    Explain {
        /// Carry out the command and show actual run times and other statistics.
        analyze: bool,
        /// A SQL query that specifies what to explain
        statement: Box<Statement>,
        /// options of the explain statement
        options: ExplainOptions,
    },
    /// EXPLAIN ANALYZE for stream job
    /// We introduce a new statement rather than reuse `EXPLAIN` because
    /// the body of the statement is not an SQL query.
    /// TODO(kwannoel): Make profiling duration configurable: EXPLAIN ANALYZE (DURATION 1s) ...
    ExplainAnalyzeStreamJob {
        target: AnalyzeTarget,
        duration_secs: Option<u64>,
    },
    /// CREATE USER
    CreateUser(CreateUserStatement),
    /// ALTER USER
    AlterUser(AlterUserStatement),
    /// ALTER SYSTEM SET configuration_parameter { TO | = } { value | 'value' | DEFAULT }
    AlterSystem {
        param: Ident,
        value: SetVariableValue,
    },
    /// ALTER SYSTEM CLEAR FILE CACHE [META | DATA | ALL]
    AlterSystemClearFileCache {
        cache_type: FileCacheType,
    },
    /// FLUSH the current barrier.
    ///
    /// Note: RisingWave specific statement.
    Flush,
    /// WAIT for background stream jobs to finish.
    /// It will block the current session until the condition is met.
    Wait(WaitTarget),
    /// Trigger meta backup.
    Backup,
    /// Trigger stream job recover
    Recover,
    /// `USE <db_name>`
    ///
    /// Note: this is a RisingWave specific statement and used to switch the current database.
    Use {
        db_name: ObjectName,
    },
    /// `VACUUM [FULL] [database_name][schema_name][object_name]`
    ///
    /// Note: this is a RisingWave specific statement for iceberg table/sink compaction.
    Vacuum {
        object_name: ObjectName,
        full: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DescribeKind {
    /// `DESCRIBE <name>`
    Plain,

    /// `DESCRIBE FRAGMENTS <name>`
    Fragments,
}

impl fmt::Display for Statement {
    /// Converts(unparses) the statement to a SQL string.
    ///
    /// If the resulting SQL is not valid, this function will panic. Use
    /// [`Statement::try_to_string`] to get a `Result` instead.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Note: we ignore formatting options here.
        let sql = self
            .try_to_string()
            .expect("normalized SQL should be parsable");
        f.write_str(&sql)
    }
}

impl Statement {
    /// Converts(unparses) the statement to a SQL string.
    ///
    /// If the resulting SQL is not valid, returns an error.
    pub fn try_to_string(&self) -> Result<String, ParserError> {
        let sql = self.to_string_unchecked();

        // TODO(#20713): expand this check to all statements
        if matches!(
            self,
            Statement::CreateTable { .. } | Statement::CreateSource { .. }
        ) {
            let _ = Parser::parse_sql(&sql)?;
        }
        Ok(sql)
    }

    /// Converts(unparses) the statement to a SQL string.
    ///
    /// The result may not be valid SQL if there's an implementation bug in the `Display`
    /// trait of any AST node. To avoid this, always prefer [`Statement::try_to_string`]
    /// to get a `Result`, or `to_string` which panics if the SQL is invalid.
    pub fn to_string_unchecked(&self) -> String {
        let mut buf = String::new();
        self.fmt_unchecked(&mut buf).unwrap();
        buf
    }

    // NOTE: This function should not check the validity of the unparsed SQL (and panic).
    //       Thus, do not directly format a statement with `write!` or `format!`. Recursively
    //       call `fmt_unchecked` on the inner statements instead.
    //
    // Clippy thinks this function is too complicated, but it is painful to
    // split up without extracting structs for each `Statement` variant.
    #[expect(clippy::cognitive_complexity)]
    fn fmt_unchecked(&self, mut f: impl std::fmt::Write) -> fmt::Result {
        match self {
            Statement::Explain {
                analyze,
                statement,
                options,
            } => {
                write!(f, "EXPLAIN ")?;

                if *analyze {
                    write!(f, "ANALYZE ")?;
                }
                write!(f, "{}", options)?;

                statement.fmt_unchecked(f)
            }
            Statement::ExplainAnalyzeStreamJob {
                target,
                duration_secs,
            } => {
                write!(f, "EXPLAIN ANALYZE {}", target)?;
                if let Some(duration_secs) = duration_secs {
                    write!(f, " (DURATION_SECS {})", duration_secs)?;
                }
                Ok(())
            }
            Statement::Query(s) => write!(f, "{}", s),
            Statement::Truncate { table_name } => {
                write!(f, "TRUNCATE TABLE {}", table_name)?;
                Ok(())
            }
            Statement::Refresh { table_name } => {
                write!(f, "REFRESH TABLE {}", table_name)?;
                Ok(())
            }
            Statement::Analyze { table_name } => {
                write!(f, "ANALYZE TABLE {}", table_name)?;
                Ok(())
            }
            Statement::Describe { name, kind } => {
                write!(f, "DESCRIBE {}", name)?;
                match kind {
                    DescribeKind::Plain => {}

                    DescribeKind::Fragments => {
                        write!(f, " FRAGMENTS")?;
                    }
                }
                Ok(())
            }
            Statement::DescribeFragment { fragment_id } => {
                write!(f, "DESCRIBE FRAGMENT {}", fragment_id)?;
                Ok(())
            }
            Statement::ShowObjects {
                object: show_object,
                filter,
            } => {
                write!(f, "SHOW {}", show_object)?;
                if let Some(filter) = filter {
                    write!(f, " {}", filter)?;
                }
                Ok(())
            }
            Statement::ShowCreateObject {
                create_type: show_type,
                name,
            } => {
                write!(f, "SHOW CREATE {} {}", show_type, name)?;
                Ok(())
            }
            Statement::ShowTransactionIsolationLevel => {
                write!(f, "SHOW TRANSACTION ISOLATION LEVEL")?;
                Ok(())
            }
            Statement::Insert {
                table_name,
                columns,
                source,
                returning,
            } => {
                write!(f, "INSERT INTO {table_name} ", table_name = table_name,)?;
                if !columns.is_empty() {
                    write!(f, "({}) ", display_comma_separated(columns))?;
                }
                write!(f, "{}", source)?;
                if !returning.is_empty() {
                    write!(f, " RETURNING ({})", display_comma_separated(returning))?;
                }
                Ok(())
            }
            Statement::Copy { entity, target } => {
                write!(f, "COPY ",)?;
                match entity {
                    CopyEntity::Query(query) => {
                        write!(f, "({})", query)?;
                    }
                    CopyEntity::Table {
                        table_name,
                        columns,
                    } => {
                        write!(f, "{}", table_name)?;
                        if !columns.is_empty() {
                            write!(f, " ({})", display_comma_separated(columns))?;
                        }
                    }
                }

                match target {
                    CopyTarget::Stdin { values } => {
                        write!(f, " FROM STDIN; ")?;
                        if !values.is_empty() {
                            writeln!(f)?;
                            let mut delim = "";
                            for v in values {
                                write!(f, "{}", delim)?;
                                delim = "\t";
                                if let Some(v) = v {
                                    write!(f, "{}", v)?;
                                } else {
                                    write!(f, "\\N")?;
                                }
                            }
                        }
                        write!(f, "\n\\.")
                    }
                    CopyTarget::Stdout => {
                        write!(f, " TO STDOUT")
                    }
                }
            }
            Statement::Update {
                table_name,
                assignments,
                selection,
                returning,
            } => {
                write!(f, "UPDATE {}", table_name)?;
                if !assignments.is_empty() {
                    write!(f, " SET {}", display_comma_separated(assignments))?;
                }
                if let Some(selection) = selection {
                    write!(f, " WHERE {}", selection)?;
                }
                if !returning.is_empty() {
                    write!(f, " RETURNING ({})", display_comma_separated(returning))?;
                }
                Ok(())
            }
            Statement::Delete {
                table_name,
                selection,
                returning,
            } => {
                write!(f, "DELETE FROM {}", table_name)?;
                if let Some(selection) = selection {
                    write!(f, " WHERE {}", selection)?;
                }
                if !returning.is_empty() {
                    write!(f, " RETURNING {}", display_comma_separated(returning))?;
                }
                Ok(())
            }
            Statement::DeleteMetaSnapshots { snapshot_ids } => {
                write!(
                    f,
                    "DELETE META SNAPSHOTS {}",
                    display_comma_separated(snapshot_ids)
                )?;
                Ok(())
            }
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                owner,
                resource_group,
                barrier_interval_ms,
                checkpoint_frequency,
            } => {
                write!(f, "CREATE DATABASE")?;
                if *if_not_exists {
                    write!(f, " IF NOT EXISTS")?;
                }
                write!(f, " {}", db_name)?;
                if let Some(owner) = owner {
                    write!(f, " WITH OWNER = {}", owner)?;
                }
                if let Some(resource_group) = resource_group {
                    write!(f, " RESOURCE_GROUP = {}", resource_group)?;
                }
                if let Some(barrier_interval_ms) = barrier_interval_ms {
                    write!(f, " BARRIER_INTERVAL_MS = {}", barrier_interval_ms)?;
                }
                if let Some(checkpoint_frequency) = checkpoint_frequency {
                    write!(f, " CHECKPOINT_FREQUENCY = {}", checkpoint_frequency)?;
                }

                Ok(())
            }
            Statement::CreateFunction {
                or_replace,
                temporary,
                if_not_exists,
                name,
                args,
                returns,
                params,
                with_options,
            } => {
                write!(
                    f,
                    "CREATE {or_replace}{temp}FUNCTION {if_not_exists}{name}",
                    temp = if *temporary { "TEMPORARY " } else { "" },
                    or_replace = if *or_replace { "OR REPLACE " } else { "" },
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                )?;
                if let Some(args) = args {
                    write!(f, "({})", display_comma_separated(args))?;
                }
                if let Some(return_type) = returns {
                    write!(f, " {}", return_type)?;
                }
                write!(f, "{params}")?;
                write!(f, "{with_options}")?;
                Ok(())
            }
            Statement::CreateAggregate {
                or_replace,
                if_not_exists,
                name,
                args,
                returns,
                append_only,
                params,
            } => {
                write!(
                    f,
                    "CREATE {or_replace}AGGREGATE {if_not_exists}{name}",
                    or_replace = if *or_replace { "OR REPLACE " } else { "" },
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                )?;
                write!(f, "({})", display_comma_separated(args))?;
                write!(f, " RETURNS {}", returns)?;
                if *append_only {
                    write!(f, " APPEND ONLY")?;
                }
                write!(f, "{params}")?;
                Ok(())
            }
            Statement::CreateView {
                name,
                or_replace,
                if_not_exists,
                columns,
                query,
                materialized,
                with_options,
                emit_mode,
            } => {
                write!(
                    f,
                    "CREATE {or_replace}{materialized}VIEW {if_not_exists}{name}",
                    or_replace = if *or_replace { "OR REPLACE " } else { "" },
                    materialized = if *materialized { "MATERIALIZED " } else { "" },
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    name = name
                )?;
                if !with_options.is_empty() {
                    write!(f, " WITH ({})", display_comma_separated(with_options))?;
                }
                if !columns.is_empty() {
                    write!(f, " ({})", display_comma_separated(columns))?;
                }
                write!(f, " AS {}", query)?;
                if let Some(emit_mode) = emit_mode {
                    write!(f, " EMIT {}", emit_mode)?;
                }
                Ok(())
            }
            Statement::CreateTable {
                name,
                columns,
                wildcard_idx,
                constraints,
                with_options,
                or_replace,
                if_not_exists,
                temporary,
                format_encode,
                source_watermarks,
                append_only,
                on_conflict,
                with_version_columns,
                query,
                cdc_table_info,
                include_column_options,
                webhook_info,
                engine,
            } => {
                // We want to allow the following options
                // Empty column list, allowed by PostgreSQL:
                //   `CREATE TABLE t ()`
                // No columns provided for CREATE TABLE AS:
                //   `CREATE TABLE t AS SELECT a from t2`
                // Columns provided for CREATE TABLE AS:
                //   `CREATE TABLE t (a INT) AS SELECT a from t2`
                write!(
                    f,
                    "CREATE {or_replace}{temporary}TABLE {if_not_exists}{name}",
                    or_replace = if *or_replace { "OR REPLACE " } else { "" },
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    temporary = if *temporary { "TEMPORARY " } else { "" },
                    name = name,
                )?;
                if !columns.is_empty() || !constraints.is_empty() {
                    write!(
                        f,
                        " {}",
                        fmt_create_items(columns, constraints, source_watermarks, *wildcard_idx)?
                    )?;
                } else if query.is_none() {
                    // PostgreSQL allows `CREATE TABLE t ();`, but requires empty parens
                    write!(f, " ()")?;
                }
                if *append_only {
                    write!(f, " APPEND ONLY")?;
                }

                if let Some(on_conflict_behavior) = on_conflict {
                    write!(f, " ON CONFLICT {}", on_conflict_behavior)?;
                }
                if !with_version_columns.is_empty() {
                    write!(
                        f,
                        " WITH VERSION COLUMN({})",
                        display_comma_separated(with_version_columns)
                    )?;
                }
                if !include_column_options.is_empty() {
                    write!(f, " {}", display_separated(include_column_options, " "))?;
                }
                if !with_options.is_empty() {
                    write!(f, " WITH ({})", display_comma_separated(with_options))?;
                }
                if let Some(format_encode) = format_encode {
                    write!(f, " {}", format_encode)?;
                }
                if let Some(query) = query {
                    write!(f, " AS {}", query)?;
                }
                if let Some(info) = cdc_table_info {
                    write!(f, " FROM {}", info.source_name)?;
                    write!(f, " TABLE '{}'", info.external_table_name)?;
                }
                if let Some(info) = webhook_info
                    && let Some(signature_expr) = &info.signature_expr
                {
                    if let Some(secret) = &info.secret_ref {
                        write!(f, " VALIDATE SECRET {}", secret.secret_name)?;
                    } else {
                        write!(f, " VALIDATE")?;
                    }
                    write!(f, " AS {}", signature_expr)?;
                }
                match engine {
                    Engine::Hummock => {}
                    Engine::Iceberg => {
                        write!(f, " ENGINE = {}", engine)?;
                    }
                }
                Ok(())
            }
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
            } => write!(
                f,
                "CREATE {unique}INDEX {if_not_exists}{name} ON {table_name}{method}({columns}){include}{distributed_by}{with_properties}",
                unique = if *unique { "UNIQUE " } else { "" },
                if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                name = name,
                table_name = table_name,
                method = if let Some(method) = method {
                    format!(" USING {} ", method)
                } else {
                    "".to_owned()
                },
                columns = display_comma_separated(columns),
                include = if include.is_empty() {
                    "".to_owned()
                } else {
                    format!(" INCLUDE({})", display_separated(include, ","))
                },
                distributed_by = if distributed_by.is_empty() {
                    "".to_owned()
                } else {
                    format!(
                        " DISTRIBUTED BY({})",
                        display_separated(distributed_by, ",")
                    )
                },
                with_properties = if !with_properties.0.is_empty() {
                    format!(" {}", with_properties)
                } else {
                    "".to_owned()
                },
            ),
            Statement::CreateSource { stmt } => write!(f, "CREATE SOURCE {}", stmt,),
            Statement::CreateSink { stmt } => {
                if stmt.or_replace {
                    write!(f, "REPLACE SINK {}", stmt)
                } else {
                    write!(f, "CREATE SINK {}", stmt)
                }
            }
            Statement::CreateSubscription { stmt } => write!(f, "CREATE SUBSCRIPTION {}", stmt,),
            Statement::CreateConnection { stmt } => write!(f, "CREATE CONNECTION {}", stmt,),
            Statement::DeclareCursor { stmt } => write!(f, "DECLARE {}", stmt,),
            Statement::FetchCursor { stmt } => write!(f, "FETCH {}", stmt),
            Statement::CloseCursor { stmt } => write!(f, "CLOSE {}", stmt),
            Statement::CreateSecret { stmt } => write!(f, "CREATE SECRET {}", stmt),
            Statement::AlterDatabase { name, operation } => {
                write!(f, "ALTER DATABASE {} {}", name, operation)
            }
            Statement::AlterSchema { name, operation } => {
                write!(f, "ALTER SCHEMA {} {}", name, operation)
            }
            Statement::AlterTable { name, operation } => {
                write!(f, "ALTER TABLE {} {}", name, operation)
            }
            Statement::AlterIndex { name, operation } => {
                write!(f, "ALTER INDEX {} {}", name, operation)
            }
            Statement::AlterView {
                materialized,
                name,
                operation,
            } => {
                write!(
                    f,
                    "ALTER {}VIEW {} {}",
                    if *materialized { "MATERIALIZED " } else { "" },
                    name,
                    operation
                )
            }
            Statement::AlterSink { name, operation } => {
                write!(f, "ALTER SINK {} {}", name, operation)
            }
            Statement::AlterSubscription { name, operation } => {
                write!(f, "ALTER SUBSCRIPTION {} {}", name, operation)
            }
            Statement::AlterSource { name, operation } => {
                write!(f, "ALTER SOURCE {} {}", name, operation)
            }
            Statement::AlterFunction {
                name,
                args,
                operation,
            } => {
                write!(f, "ALTER FUNCTION {}", name)?;
                if let Some(args) = args {
                    write!(f, "({})", display_comma_separated(args))?;
                }
                write!(f, " {}", operation)
            }
            Statement::AlterConnection { name, operation } => {
                write!(f, "ALTER CONNECTION {} {}", name, operation)
            }
            Statement::AlterSecret { name, operation } => {
                write!(f, "ALTER SECRET {}", name)?;
                write!(f, "{}", operation)
            }
            Statement::Discard(t) => write!(f, "DISCARD {}", t),
            Statement::Drop(stmt) => write!(f, "DROP {}", stmt),
            Statement::DropFunction {
                if_exists,
                func_desc,
                option,
            } => {
                write!(
                    f,
                    "DROP FUNCTION{} {}",
                    if *if_exists { " IF EXISTS" } else { "" },
                    display_comma_separated(func_desc),
                )?;
                if let Some(op) = option {
                    write!(f, " {}", op)?;
                }
                Ok(())
            }
            Statement::DropAggregate {
                if_exists,
                func_desc,
                option,
            } => {
                write!(
                    f,
                    "DROP AGGREGATE{} {}",
                    if *if_exists { " IF EXISTS" } else { "" },
                    display_comma_separated(func_desc),
                )?;
                if let Some(op) = option {
                    write!(f, " {}", op)?;
                }
                Ok(())
            }
            Statement::SetVariable {
                local,
                variable,
                value,
            } => {
                f.write_str("SET ")?;
                if *local {
                    f.write_str("LOCAL ")?;
                }
                write!(f, "{name} = {value}", name = variable,)
            }
            Statement::ShowVariable { variable } => {
                write!(f, "SHOW")?;
                if !variable.is_empty() {
                    write!(f, " {}", display_separated(variable, " "))?;
                }
                Ok(())
            }
            Statement::StartTransaction { modes } => {
                write!(f, "START TRANSACTION")?;
                if !modes.is_empty() {
                    write!(f, " {}", display_comma_separated(modes))?;
                }
                Ok(())
            }
            Statement::Abort => {
                write!(f, "ABORT")?;
                Ok(())
            }
            Statement::SetTransaction {
                modes,
                snapshot,
                session,
            } => {
                if *session {
                    write!(f, "SET SESSION CHARACTERISTICS AS TRANSACTION")?;
                } else {
                    write!(f, "SET TRANSACTION")?;
                }
                if !modes.is_empty() {
                    write!(f, " {}", display_comma_separated(modes))?;
                }
                if let Some(snapshot_id) = snapshot {
                    write!(f, " SNAPSHOT {}", snapshot_id)?;
                }
                Ok(())
            }
            Statement::SetTimeZone { local, value } => {
                write!(f, "SET")?;
                if *local {
                    write!(f, " LOCAL")?;
                }
                write!(f, " TIME ZONE {}", value)?;
                Ok(())
            }
            Statement::Commit { chain } => {
                write!(f, "COMMIT{}", if *chain { " AND CHAIN" } else { "" },)
            }
            Statement::Rollback { chain } => {
                write!(f, "ROLLBACK{}", if *chain { " AND CHAIN" } else { "" },)
            }
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
                owner,
            } => {
                write!(
                    f,
                    "CREATE SCHEMA {if_not_exists}{name}",
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    name = schema_name
                )?;
                if let Some(user) = owner {
                    write!(f, " AUTHORIZATION {}", user)?;
                }
                Ok(())
            }
            Statement::Grant {
                privileges,
                objects,
                grantees,
                with_grant_option,
                granted_by,
            } => {
                write!(f, "GRANT {} ", privileges)?;
                write!(f, "ON {} ", objects)?;
                write!(f, "TO {}", display_comma_separated(grantees))?;
                if *with_grant_option {
                    write!(f, " WITH GRANT OPTION")?;
                }
                if let Some(grantor) = granted_by {
                    write!(f, " GRANTED BY {}", grantor)?;
                }
                Ok(())
            }
            Statement::Revoke {
                privileges,
                objects,
                grantees,
                granted_by,
                revoke_grant_option,
                cascade,
            } => {
                write!(
                    f,
                    "REVOKE {}{} ",
                    if *revoke_grant_option {
                        "GRANT OPTION FOR "
                    } else {
                        ""
                    },
                    privileges
                )?;
                write!(f, "ON {} ", objects)?;
                write!(f, "FROM {}", display_comma_separated(grantees))?;
                if let Some(grantor) = granted_by {
                    write!(f, " GRANTED BY {}", grantor)?;
                }
                write!(f, " {}", if *cascade { "CASCADE" } else { "RESTRICT" })?;
                Ok(())
            }
            Statement::Deallocate { name, prepare } => {
                if let Some(name) = name {
                    write!(
                        f,
                        "DEALLOCATE {prepare}{name}",
                        prepare = if *prepare { "PREPARE " } else { "" },
                        name = name,
                    )
                } else {
                    write!(
                        f,
                        "DEALLOCATE {prepare}ALL",
                        prepare = if *prepare { "PREPARE " } else { "" },
                    )
                }
            }
            Statement::Execute { name, parameters } => {
                write!(f, "EXECUTE {}", name)?;
                if !parameters.is_empty() {
                    write!(f, "({})", display_comma_separated(parameters))?;
                }
                Ok(())
            }
            Statement::Prepare {
                name,
                data_types,
                statement,
            } => {
                write!(f, "PREPARE {} ", name)?;
                if !data_types.is_empty() {
                    write!(f, "({}) ", display_comma_separated(data_types))?;
                }
                write!(f, "AS ")?;
                statement.fmt_unchecked(f)
            }
            Statement::Comment {
                object_type,
                object_name,
                comment,
            } => {
                write!(f, "COMMENT ON {} {} IS ", object_type, object_name)?;
                if let Some(c) = comment {
                    write!(f, "'{}'", c)
                } else {
                    write!(f, "NULL")
                }
            }
            Statement::CreateUser(statement) => {
                write!(f, "CREATE USER {}", statement)
            }
            Statement::AlterUser(statement) => {
                write!(f, "ALTER USER {}", statement)
            }
            Statement::AlterSystem { param, value } => {
                f.write_str("ALTER SYSTEM SET ")?;
                write!(f, "{param} = {value}",)
            }
            Statement::AlterSystemClearFileCache { cache_type } => {
                f.write_str("ALTER SYSTEM CLEAR FILE CACHE ")?;
                match cache_type {
                    FileCacheType::Meta => f.write_str("META"),
                    FileCacheType::Data => f.write_str("DATA"),
                    FileCacheType::All => f.write_str("ALL"),
                }
            }
            Statement::Flush => {
                write!(f, "FLUSH")
            }
            Statement::Wait(target) => match target {
                WaitTarget::All => write!(f, "WAIT"),
                WaitTarget::Table(name) => write!(f, "WAIT TABLE {name}"),
                WaitTarget::MaterializedView(name) => {
                    write!(f, "WAIT MATERIALIZED VIEW {name}")
                }
                WaitTarget::Sink(name) => write!(f, "WAIT SINK {name}"),
                WaitTarget::Index(name) => write!(f, "WAIT INDEX {name}"),
            },
            Statement::Backup => {
                write!(f, "BACKUP")?;
                Ok(())
            }
            Statement::Begin { modes } => {
                write!(f, "BEGIN")?;
                if !modes.is_empty() {
                    write!(f, " {}", display_comma_separated(modes))?;
                }
                Ok(())
            }
            Statement::CancelJobs(jobs) => {
                write!(f, "CANCEL JOBS {}", display_comma_separated(&jobs.0))?;
                Ok(())
            }
            Statement::Kill(worker_process_id) => {
                write!(f, "KILL '{}'", worker_process_id)?;
                Ok(())
            }
            Statement::Recover => {
                write!(f, "RECOVER")?;
                Ok(())
            }
            Statement::Use { db_name } => {
                write!(f, "USE {}", db_name)?;
                Ok(())
            }
            Statement::Vacuum { object_name, full } => {
                if *full {
                    write!(f, "VACUUM FULL {}", object_name)?;
                } else {
                    write!(f, "VACUUM {}", object_name)?;
                }
                Ok(())
            }
            Statement::AlterFragment {
                fragment_ids,
                operation,
            } => {
                write!(
                    f,
                    "ALTER FRAGMENT {} {}",
                    display_comma_separated(fragment_ids),
                    operation
                )
            }
            Statement::AlterCompactionGroup {
                group_ids,
                operation,
            } => {
                write!(
                    f,
                    "ALTER COMPACTION GROUP {} {}",
                    display_comma_separated(group_ids),
                    operation
                )
            }
            Statement::AlterDefaultPrivileges {
                target_users,
                schema_names,
                operation,
            } => {
                write!(f, "ALTER DEFAULT PRIVILEGES")?;
                if let Some(target_users) = target_users {
                    write!(f, " FOR {}", display_comma_separated(target_users))?;
                }
                if let Some(schema_names) = schema_names {
                    write!(f, " IN SCHEMA {}", display_comma_separated(schema_names))?;
                }
                write!(f, " {}", operation)
            }
        }
    }

    pub fn is_create(&self) -> bool {
        matches!(
            self,
            Statement::CreateTable { .. }
                | Statement::CreateView { .. }
                | Statement::CreateSource { .. }
                | Statement::CreateSink { .. }
                | Statement::CreateSubscription { .. }
                | Statement::CreateConnection { .. }
                | Statement::CreateSecret { .. }
                | Statement::CreateUser { .. }
                | Statement::CreateDatabase { .. }
                | Statement::CreateFunction { .. }
                | Statement::CreateAggregate { .. }
                | Statement::CreateIndex { .. }
                | Statement::CreateSchema { .. }
        )
    }
}

impl Display for IncludeOptionItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            column_type,
            inner_field,
            header_inner_expect_type,
            column_alias,
        } = self;
        write!(f, "INCLUDE {}", column_type)?;
        if let Some(inner_field) = inner_field {
            write!(f, " '{}'", value::escape_single_quote_string(inner_field))?;
            if let Some(expected_type) = header_inner_expect_type {
                write!(f, " {}", expected_type)?;
            }
        }
        if let Some(alias) = column_alias {
            write!(f, " AS {}", alias)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OnInsert {
    /// ON DUPLICATE KEY UPDATE (MySQL when the key already exists, then execute an update instead)
    DuplicateKeyUpdate(Vec<Assignment>),
}

impl fmt::Display for OnInsert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateKeyUpdate(expr) => write!(
                f,
                " ON DUPLICATE KEY UPDATE {}",
                display_comma_separated(expr)
            ),
        }
    }
}

/// Privileges granted in a GRANT statement or revoked in a REVOKE statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Privileges {
    /// All privileges applicable to the object type
    All {
        /// Optional keyword from the spec, ignored in practice
        with_privileges_keyword: bool,
    },
    /// Specific privileges (e.g. `SELECT`, `INSERT`)
    Actions(Vec<Action>),
}

impl fmt::Display for Privileges {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Privileges::All {
                with_privileges_keyword,
            } => {
                write!(
                    f,
                    "ALL{}",
                    if *with_privileges_keyword {
                        " PRIVILEGES"
                    } else {
                        ""
                    }
                )
            }
            Privileges::Actions(actions) => {
                write!(f, "{}", display_comma_separated(actions))
            }
        }
    }
}

/// A privilege on a database object (table, sequence, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    Connect,
    Create,
    Delete,
    Execute,
    Insert { columns: Option<Vec<Ident>> },
    References { columns: Option<Vec<Ident>> },
    Select { columns: Option<Vec<Ident>> },
    Temporary,
    Trigger,
    Truncate,
    Update { columns: Option<Vec<Ident>> },
    Usage,
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Action::Connect => f.write_str("CONNECT")?,
            Action::Create => f.write_str("CREATE")?,
            Action::Delete => f.write_str("DELETE")?,
            Action::Execute => f.write_str("EXECUTE")?,
            Action::Insert { .. } => f.write_str("INSERT")?,
            Action::References { .. } => f.write_str("REFERENCES")?,
            Action::Select { .. } => f.write_str("SELECT")?,
            Action::Temporary => f.write_str("TEMPORARY")?,
            Action::Trigger => f.write_str("TRIGGER")?,
            Action::Truncate => f.write_str("TRUNCATE")?,
            Action::Update { .. } => f.write_str("UPDATE")?,
            Action::Usage => f.write_str("USAGE")?,
        };
        match self {
            Action::Insert { columns }
            | Action::References { columns }
            | Action::Select { columns }
            | Action::Update { columns } => {
                if let Some(columns) = columns {
                    write!(f, " ({})", display_comma_separated(columns))?;
                }
            }
            _ => (),
        };
        Ok(())
    }
}

/// Objects on which privileges are granted in a GRANT statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GrantObjects {
    /// Grant privileges on `ALL SEQUENCES IN SCHEMA <schema_name> [, ...]`
    AllSequencesInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL TABLES IN SCHEMA <schema_name> [, ...]`
    AllTablesInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL SOURCES IN SCHEMA <schema_name> [, ...]`
    AllSourcesInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL SINKS IN SCHEMA <schema_name> [, ...]`
    AllSinksInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL MATERIALIZED VIEWS IN SCHEMA <schema_name> [, ...]`
    AllMviewsInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL VIEWS IN SCHEMA <schema_name> [, ...]`
    AllViewsInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL FUNCTIONS IN SCHEMA <schema_name> [, ...]`
    AllFunctionsInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL SECRETS IN SCHEMA <schema_name> [, ...]`
    AllSecretsInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL SUBSCRIPTIONS IN SCHEMA <schema_name> [, ...]`
    AllSubscriptionsInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL CONNECTIONS IN SCHEMA <schema_name> [, ...]`
    AllConnectionsInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on specific databases
    Databases(Vec<ObjectName>),
    /// Grant privileges on specific schemas
    Schemas(Vec<ObjectName>),
    /// Grant privileges on specific sources
    Sources(Vec<ObjectName>),
    /// Grant privileges on specific materialized views
    Mviews(Vec<ObjectName>),
    /// Grant privileges on specific sequences
    Sequences(Vec<ObjectName>),
    /// Grant privileges on specific tables
    Tables(Vec<ObjectName>),
    /// Grant privileges on specific sinks
    Sinks(Vec<ObjectName>),
    /// Grant privileges on specific views
    Views(Vec<ObjectName>),
    /// Grant privileges on specific connections
    Connections(Vec<ObjectName>),
    /// Grant privileges on specific subscriptions
    Subscriptions(Vec<ObjectName>),
    /// Grant privileges on specific functions
    Functions(Vec<FunctionDesc>),
    /// Grant privileges on specific secrets
    Secrets(Vec<ObjectName>),
}

impl fmt::Display for GrantObjects {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GrantObjects::Sequences(sequences) => {
                write!(f, "SEQUENCE {}", display_comma_separated(sequences))
            }
            GrantObjects::Schemas(schemas) => {
                write!(f, "SCHEMA {}", display_comma_separated(schemas))
            }
            GrantObjects::Tables(tables) => {
                write!(f, "{}", display_comma_separated(tables))
            }
            GrantObjects::AllSequencesInSchema { schemas } => {
                write!(
                    f,
                    "ALL SEQUENCES IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllTablesInSchema { schemas } => {
                write!(
                    f,
                    "ALL TABLES IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllSourcesInSchema { schemas } => {
                write!(
                    f,
                    "ALL SOURCES IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllMviewsInSchema { schemas } => {
                write!(
                    f,
                    "ALL MATERIALIZED VIEWS IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllSinksInSchema { schemas } => {
                write!(
                    f,
                    "ALL SINKS IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllViewsInSchema { schemas } => {
                write!(
                    f,
                    "ALL VIEWS IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllFunctionsInSchema { schemas } => {
                write!(
                    f,
                    "ALL FUNCTIONS IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllSecretsInSchema { schemas } => {
                write!(
                    f,
                    "ALL SECRETS IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllSubscriptionsInSchema { schemas } => {
                write!(
                    f,
                    "ALL SUBSCRIPTIONS IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllConnectionsInSchema { schemas } => {
                write!(
                    f,
                    "ALL CONNECTIONS IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::Databases(databases) => {
                write!(f, "DATABASE {}", display_comma_separated(databases))
            }
            GrantObjects::Sources(sources) => {
                write!(f, "SOURCE {}", display_comma_separated(sources))
            }
            GrantObjects::Mviews(mviews) => {
                write!(f, "MATERIALIZED VIEW {}", display_comma_separated(mviews))
            }
            GrantObjects::Sinks(sinks) => {
                write!(f, "SINK {}", display_comma_separated(sinks))
            }
            GrantObjects::Views(views) => {
                write!(f, "VIEW {}", display_comma_separated(views))
            }
            GrantObjects::Connections(connections) => {
                write!(f, "CONNECTION {}", display_comma_separated(connections))
            }
            GrantObjects::Subscriptions(subscriptions) => {
                write!(f, "SUBSCRIPTION {}", display_comma_separated(subscriptions))
            }
            GrantObjects::Functions(func_descs) => {
                write!(f, "FUNCTION {}", display_comma_separated(func_descs))
            }
            GrantObjects::Secrets(secrets) => {
                write!(f, "SECRET {}", display_comma_separated(secrets))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PrivilegeObjectType {
    Tables,
    Sources,
    Sinks,
    Mviews,
    Views,
    Functions,
    Connections,
    Secrets,
    Subscriptions,
    Schemas,
}

impl fmt::Display for PrivilegeObjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PrivilegeObjectType::Tables => f.write_str("TABLES")?,
            PrivilegeObjectType::Sources => f.write_str("SOURCES")?,
            PrivilegeObjectType::Sinks => f.write_str("SINKS")?,
            PrivilegeObjectType::Mviews => f.write_str("MATERIALIZED VIEWS")?,
            PrivilegeObjectType::Views => f.write_str("VIEWS")?,
            PrivilegeObjectType::Functions => f.write_str("FUNCTIONS")?,
            PrivilegeObjectType::Connections => f.write_str("CONNECTIONS")?,
            PrivilegeObjectType::Secrets => f.write_str("SECRETS")?,
            PrivilegeObjectType::Subscriptions => f.write_str("SUBSCRIPTIONS")?,
            PrivilegeObjectType::Schemas => f.write_str("SCHEMAS")?,
        };
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DefaultPrivilegeOperation {
    Grant {
        privileges: Privileges,
        object_type: PrivilegeObjectType,
        grantees: Vec<Ident>,
        with_grant_option: bool,
    },
    Revoke {
        privileges: Privileges,
        object_type: PrivilegeObjectType,
        grantees: Vec<Ident>,
        revoke_grant_option: bool,
        cascade: bool,
    },
}

impl fmt::Display for DefaultPrivilegeOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DefaultPrivilegeOperation::Grant {
                privileges,
                object_type,
                grantees,
                with_grant_option,
            } => {
                write!(
                    f,
                    "GRANT {} ON {} TO {}",
                    privileges,
                    object_type,
                    display_comma_separated(grantees)
                )?;
                if *with_grant_option {
                    write!(f, " WITH GRANT OPTION")?;
                }
            }
            DefaultPrivilegeOperation::Revoke {
                privileges,
                object_type,
                grantees,
                revoke_grant_option,
                cascade,
            } => {
                write!(f, "REVOKE")?;
                if *revoke_grant_option {
                    write!(f, " GRANT OPTION FOR")?;
                }
                write!(
                    f,
                    " {} ON {} FROM {}",
                    privileges,
                    object_type,
                    display_comma_separated(grantees)
                )?;
                write!(f, " {}", if *cascade { "CASCADE" } else { "RESTRICT" })?;
            }
        }
        Ok(())
    }
}

impl DefaultPrivilegeOperation {
    pub fn for_schemas(&self) -> bool {
        match &self {
            DefaultPrivilegeOperation::Grant { object_type, .. } => {
                object_type == &PrivilegeObjectType::Schemas
            }
            DefaultPrivilegeOperation::Revoke { object_type, .. } => {
                object_type == &PrivilegeObjectType::Schemas
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AssignmentValue {
    /// An expression, e.g. `foo = 1`
    Expr(Expr),
    /// The `DEFAULT` keyword, e.g. `foo = DEFAULT`
    Default,
}

impl fmt::Display for AssignmentValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AssignmentValue::Expr(expr) => write!(f, "{}", expr),
            AssignmentValue::Default => f.write_str("DEFAULT"),
        }
    }
}

/// SQL assignment `foo = { expr | DEFAULT }` as used in SQLUpdate
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Assignment {
    pub id: Vec<Ident>,
    pub value: AssignmentValue,
}

impl fmt::Display for Assignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", display_separated(&self.id, "."), self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FunctionArgExpr {
    Expr(Expr),
    /// Expr is an arbitrary expression, returning either a table or a column.
    /// Idents are the prefix of `*`, which are consecutive field accesses.
    /// e.g. `(table.v1).*` or `(table).v1.*`
    ExprQualifiedWildcard(Expr, Vec<Ident>),
    /// Qualified wildcard, e.g. `alias.*` or `schema.table.*`, followed by optional
    /// except syntax
    QualifiedWildcard(ObjectName, Option<Vec<Expr>>),
    /// An unqualified `*` or `* except (columns)`
    Wildcard(Option<Vec<Expr>>),
    /// A secret reference, e.g. `SECRET my_secret` or `SECRET my_secret AS FILE`
    SecretRef(SecretRefValue),
}

impl fmt::Display for FunctionArgExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionArgExpr::Expr(expr) => write!(f, "{}", expr),
            FunctionArgExpr::ExprQualifiedWildcard(expr, prefix) => {
                write!(
                    f,
                    "({}){}.*",
                    expr,
                    prefix
                        .iter()
                        .format_with("", |i, f| f(&format_args!(".{i}")))
                )
            }
            FunctionArgExpr::QualifiedWildcard(prefix, except) => match except {
                Some(exprs) => write!(
                    f,
                    "{}.* EXCEPT ({})",
                    prefix,
                    exprs
                        .iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<String>>()
                        .as_slice()
                        .join(", ")
                ),
                None => write!(f, "{}.*", prefix),
            },

            FunctionArgExpr::SecretRef(secret_ref) => write!(f, "SECRET {}", secret_ref),
            FunctionArgExpr::Wildcard(except) => match except {
                Some(exprs) => write!(
                    f,
                    "* EXCEPT ({})",
                    exprs
                        .iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<String>>()
                        .as_slice()
                        .join(", ")
                ),
                None => f.write_str("*"),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FunctionArg {
    Named { name: Ident, arg: FunctionArgExpr },
    Unnamed(FunctionArgExpr),
}

impl FunctionArg {
    pub fn get_expr(&self) -> FunctionArgExpr {
        match self {
            FunctionArg::Named { name: _, arg } => arg.clone(),
            FunctionArg::Unnamed(arg) => arg.clone(),
        }
    }
}

impl fmt::Display for FunctionArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionArg::Named { name, arg } => write!(f, "{} => {}", name, arg),
            FunctionArg::Unnamed(unnamed_arg) => write!(f, "{}", unnamed_arg),
        }
    }
}

/// A list of function arguments, including additional modifiers like `DISTINCT` or `ORDER BY`.
/// This basically holds all the information between the `(` and `)` in a function call.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionArgList {
    /// Aggregate function calls may have a `DISTINCT`, e.g. `count(DISTINCT x)`.
    pub distinct: bool,
    pub args: Vec<FunctionArg>,
    /// Whether the last argument is variadic, e.g. `foo(a, b, VARIADIC c)`.
    pub variadic: bool,
    /// Aggregate function calls may have an `ORDER BY`, e.g. `array_agg(x ORDER BY y)`.
    pub order_by: Vec<OrderByExpr>,
    /// Window function calls may have an `IGNORE NULLS`, e.g. `first_value(x IGNORE NULLS)`.
    pub ignore_nulls: bool,
}

impl fmt::Display for FunctionArgList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        if self.distinct {
            write!(f, "DISTINCT ")?;
        }
        if self.variadic {
            for arg in &self.args[0..self.args.len() - 1] {
                write!(f, "{}, ", arg)?;
            }
            write!(f, "VARIADIC {}", self.args.last().unwrap())?;
        } else {
            write!(f, "{}", display_comma_separated(&self.args))?;
        }
        if !self.order_by.is_empty() {
            write!(f, " ORDER BY {}", display_comma_separated(&self.order_by))?;
        }
        if self.ignore_nulls {
            write!(f, " IGNORE NULLS")?;
        }
        write!(f, ")")?;
        Ok(())
    }
}

impl FunctionArgList {
    pub fn empty() -> Self {
        Self {
            distinct: false,
            args: vec![],
            variadic: false,
            order_by: vec![],
            ignore_nulls: false,
        }
    }

    pub fn args_only(args: Vec<FunctionArg>) -> Self {
        Self {
            distinct: false,
            args,
            variadic: false,
            order_by: vec![],
            ignore_nulls: false,
        }
    }

    pub fn is_args_only(&self) -> bool {
        !self.distinct && !self.variadic && self.order_by.is_empty() && !self.ignore_nulls
    }

    pub fn for_agg(distinct: bool, args: Vec<FunctionArg>, order_by: Vec<OrderByExpr>) -> Self {
        Self {
            distinct,
            args,
            variadic: false,
            order_by,
            ignore_nulls: false,
        }
    }
}

/// A function call
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Function {
    /// Whether the function is prefixed with `AGGREGATE:`
    pub scalar_as_agg: bool,
    /// Function name.
    pub name: ObjectName,
    /// Argument list of the function call, i.e. things in `()`.
    pub arg_list: FunctionArgList,
    /// `WITHIN GROUP` clause of the function call, for ordered-set aggregate functions.
    /// FIXME(rc): why we only support one expression here?
    pub within_group: Option<Box<OrderByExpr>>,
    /// `FILTER` clause of the function call, for aggregate and window (not supported yet) functions.
    pub filter: Option<Box<Expr>>,
    /// `OVER` clause of the function call, for window functions.
    pub over: Option<Window>,
}

impl Function {
    pub fn no_arg(name: ObjectName) -> Self {
        Self {
            scalar_as_agg: false,
            name,
            arg_list: FunctionArgList::empty(),
            within_group: None,
            filter: None,
            over: None,
        }
    }
}

impl fmt::Display for Function {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.scalar_as_agg {
            write!(f, "AGGREGATE:")?;
        }
        write!(f, "{}{}", self.name, self.arg_list)?;
        if let Some(within_group) = &self.within_group {
            write!(f, " WITHIN GROUP (ORDER BY {})", within_group)?;
        }
        if let Some(filter) = &self.filter {
            write!(f, " FILTER (WHERE {})", filter)?;
        }
        if let Some(o) = &self.over {
            write!(f, " OVER {}", o)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectType {
    Table,
    View,
    MaterializedView,
    Index,
    Schema,
    Source,
    Sink,
    Database,
    User,
    Connection,
    Secret,
    Subscription,
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ObjectType::Table => "TABLE",
            ObjectType::View => "VIEW",
            ObjectType::MaterializedView => "MATERIALIZED VIEW",
            ObjectType::Index => "INDEX",
            ObjectType::Schema => "SCHEMA",
            ObjectType::Source => "SOURCE",
            ObjectType::Sink => "SINK",
            ObjectType::Database => "DATABASE",
            ObjectType::User => "USER",
            ObjectType::Secret => "SECRET",
            ObjectType::Connection => "CONNECTION",
            ObjectType::Subscription => "SUBSCRIPTION",
        })
    }
}

impl ParseTo for ObjectType {
    fn parse_to(parser: &mut Parser<'_>) -> ModalResult<Self> {
        let object_type = if parser.parse_keyword(Keyword::TABLE) {
            ObjectType::Table
        } else if parser.parse_keyword(Keyword::VIEW) {
            ObjectType::View
        } else if parser.parse_keywords(&[Keyword::MATERIALIZED, Keyword::VIEW]) {
            ObjectType::MaterializedView
        } else if parser.parse_keyword(Keyword::SOURCE) {
            ObjectType::Source
        } else if parser.parse_keyword(Keyword::SINK) {
            ObjectType::Sink
        } else if parser.parse_keyword(Keyword::INDEX) {
            ObjectType::Index
        } else if parser.parse_keyword(Keyword::SCHEMA) {
            ObjectType::Schema
        } else if parser.parse_keyword(Keyword::DATABASE) {
            ObjectType::Database
        } else if parser.parse_keyword(Keyword::USER) {
            ObjectType::User
        } else if parser.parse_keyword(Keyword::CONNECTION) {
            ObjectType::Connection
        } else if parser.parse_keyword(Keyword::SECRET) {
            ObjectType::Secret
        } else if parser.parse_keyword(Keyword::SUBSCRIPTION) {
            ObjectType::Subscription
        } else {
            return parser.expected(
                "TABLE, VIEW, INDEX, MATERIALIZED VIEW, SOURCE, SINK, SUBSCRIPTION, SCHEMA, DATABASE, USER, SECRET or CONNECTION after DROP",
            );
        };
        Ok(object_type)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqlOption {
    pub name: ObjectName,
    pub value: SqlOptionValue,
}

impl SqlOption {
    /// Creates an option whose value is a typed secret reference.
    pub fn from_secret_ref(name: &str, secret_ref: SecretRefValue) -> Self {
        Self {
            name: ObjectName(name.split('.').map(Ident::from_real_value).collect()),
            value: SqlOptionValue::SecretRef(secret_ref),
        }
    }
}

impl fmt::Display for SqlOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let should_redact = REDACT_SQL_OPTION_KEYWORDS
            .try_with(|keywords| {
                let sql_option_name = self.name.real_value().to_lowercase();
                keywords.iter().any(|k| sql_option_name.contains(k))
            })
            .unwrap_or(false);
        if should_redact {
            write!(f, "{} = [REDACTED]", self.name)
        } else {
            write!(f, "{} = {}", self.name, self.value)
        }
    }
}

impl TryFrom<(&String, &String)> for SqlOption {
    type Error = ParserError;

    fn try_from((name, value): (&String, &String)) -> Result<Self, Self::Error> {
        // Use from_real_value to properly escape the name, which handles cases like
        // "debezium.column.truncate.to.2000000.chars" where "2000000" would be
        // tokenized as a number instead of an identifier if not properly quoted.
        let name_parts: Vec<&str> = name.split('.').collect();
        let object_name = ObjectName(name_parts.into_iter().map(Ident::from_real_value).collect());

        // Wrap the value in single quotes so it is always parsed as a string literal.
        // This prevents values containing special characters (e.g., "jdbc:postgresql://...")
        // from being incorrectly tokenized. Escape any embedded single quotes.
        let escaped_value = value.replace('\'', "''");
        let query = format!("{} = '{}'", object_name, escaped_value);
        let mut tokenizer = Tokenizer::new(query.as_str());
        let tokens = tokenizer.tokenize_with_location()?;
        let mut parser = Parser(&tokens);
        parser
            .parse_sql_option()
            .map_err(|e| ParserError::ParserError(e.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SqlOptionValue {
    Value(Value),
    SecretRef(SecretRefValue),
    ConnectionRef(ConnectionRefValue),
    BackfillOrder(BackfillOrderStrategy),
}

impl SqlOptionValue {
    /// Returns a `NULL` value.
    pub const fn null() -> Self {
        Self::Value(Value::Null)
    }
}

impl fmt::Display for SqlOptionValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlOptionValue::Value(value) => write!(f, "{}", value),
            SqlOptionValue::SecretRef(secret_ref) => write!(f, "secret {}", secret_ref),
            SqlOptionValue::ConnectionRef(connection_ref) => {
                write!(f, "{}", connection_ref)
            }
            SqlOptionValue::BackfillOrder(order) => {
                write!(f, "{}", order)
            }
        }
    }
}

impl From<Value> for SqlOptionValue {
    fn from(value: Value) -> Self {
        SqlOptionValue::Value(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EmitMode {
    Immediately,
    OnWindowClose,
}

impl fmt::Display for EmitMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            EmitMode::Immediately => "IMMEDIATELY",
            EmitMode::OnWindowClose => "ON WINDOW CLOSE",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OnConflict {
    UpdateFull,
    Nothing,
    UpdateIfNotNull,
}

impl fmt::Display for OnConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            OnConflict::UpdateFull => "DO UPDATE FULL",
            OnConflict::Nothing => "DO NOTHING",
            OnConflict::UpdateIfNotNull => "DO UPDATE IF NOT NULL",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Engine {
    Hummock,
    Iceberg,
}

impl fmt::Display for crate::ast::Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            crate::ast::Engine::Hummock => "HUMMOCK",
            crate::ast::Engine::Iceberg => "ICEBERG",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SetTimeZoneValue {
    Ident(Ident),
    Literal(Value),
    Local,
    Default,
}

impl fmt::Display for SetTimeZoneValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SetTimeZoneValue::Ident(ident) => write!(f, "{}", ident),
            SetTimeZoneValue::Literal(value) => write!(f, "{}", value),
            SetTimeZoneValue::Local => f.write_str("LOCAL"),
            SetTimeZoneValue::Default => f.write_str("DEFAULT"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransactionMode {
    AccessMode(TransactionAccessMode),
    IsolationLevel(TransactionIsolationLevel),
}

impl fmt::Display for TransactionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TransactionMode::*;
        match self {
            AccessMode(access_mode) => write!(f, "{}", access_mode),
            IsolationLevel(iso_level) => write!(f, "ISOLATION LEVEL {}", iso_level),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransactionAccessMode {
    ReadOnly,
    ReadWrite,
}

impl fmt::Display for TransactionAccessMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TransactionAccessMode::*;
        f.write_str(match self {
            ReadOnly => "READ ONLY",
            ReadWrite => "READ WRITE",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransactionIsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl fmt::Display for TransactionIsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TransactionIsolationLevel::*;
        f.write_str(match self {
            ReadUncommitted => "READ UNCOMMITTED",
            ReadCommitted => "READ COMMITTED",
            RepeatableRead => "REPEATABLE READ",
            Serializable => "SERIALIZABLE",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ShowStatementFilter {
    Like(String),
    ILike(String),
    Where(Expr),
}

impl fmt::Display for ShowStatementFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ShowStatementFilter::*;
        match self {
            Like(pattern) => write!(f, "LIKE '{}'", value::escape_single_quote_string(pattern)),
            ILike(pattern) => write!(f, "ILIKE {}", value::escape_single_quote_string(pattern)),
            Where(expr) => write!(f, "WHERE {}", expr),
        }
    }
}

/// Function describe in DROP FUNCTION.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum DropFunctionOption {
    Restrict,
    Cascade,
}

impl fmt::Display for DropFunctionOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DropFunctionOption::Restrict => write!(f, "RESTRICT "),
            DropFunctionOption::Cascade => write!(f, "CASCADE  "),
        }
    }
}

/// Function describe in DROP FUNCTION.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionDesc {
    pub name: ObjectName,
    pub args: Option<Vec<OperateFunctionArg>>,
}

impl fmt::Display for FunctionDesc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if let Some(args) = &self.args {
            write!(f, "({})", display_comma_separated(args))?;
        }
        Ok(())
    }
}

/// Function argument in CREATE FUNCTION.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OperateFunctionArg {
    pub mode: Option<ArgMode>,
    pub name: Option<Ident>,
    pub data_type: DataType,
    pub default_expr: Option<Expr>,
}

impl OperateFunctionArg {
    /// Returns an unnamed argument.
    pub fn unnamed(data_type: DataType) -> Self {
        Self {
            mode: None,
            name: None,
            data_type,
            default_expr: None,
        }
    }

    /// Returns an argument with name.
    pub fn with_name(name: &str, data_type: DataType) -> Self {
        Self {
            mode: None,
            name: Some(name.into()),
            data_type,
            default_expr: None,
        }
    }
}

impl fmt::Display for OperateFunctionArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(mode) = &self.mode {
            write!(f, "{} ", mode)?;
        }
        if let Some(name) = &self.name {
            write!(f, "{} ", name)?;
        }
        write!(f, "{}", self.data_type)?;
        if let Some(default_expr) = &self.default_expr {
            write!(f, " = {}", default_expr)?;
        }
        Ok(())
    }
}

/// The mode of an argument in CREATE FUNCTION.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ArgMode {
    In,
    Out,
    InOut,
}

impl fmt::Display for ArgMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArgMode::In => write!(f, "IN"),
            ArgMode::Out => write!(f, "OUT"),
            ArgMode::InOut => write!(f, "INOUT"),
        }
    }
}

/// These attributes inform the query optimizer about the behavior of the function.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FunctionBehavior {
    Immutable,
    Stable,
    Volatile,
}

impl fmt::Display for FunctionBehavior {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionBehavior::Immutable => write!(f, "IMMUTABLE"),
            FunctionBehavior::Stable => write!(f, "STABLE"),
            FunctionBehavior::Volatile => write!(f, "VOLATILE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum FunctionDefinition {
    Identifier(String),
    SingleQuotedDef(String),
    DoubleDollarDef(String),
}

impl fmt::Display for FunctionDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionDefinition::Identifier(s) => write!(f, "{s}")?,
            FunctionDefinition::SingleQuotedDef(s) => write!(f, "'{s}'")?,
            FunctionDefinition::DoubleDollarDef(s) => write!(f, "$${s}$$")?,
        }
        Ok(())
    }
}

impl FunctionDefinition {
    /// Returns the function definition as a string slice.
    pub fn as_str(&self) -> &str {
        match self {
            FunctionDefinition::Identifier(s) => s,
            FunctionDefinition::SingleQuotedDef(s) => s,
            FunctionDefinition::DoubleDollarDef(s) => s,
        }
    }

    /// Returns the function definition as a string.
    pub fn into_string(self) -> String {
        match self {
            FunctionDefinition::Identifier(s) => s,
            FunctionDefinition::SingleQuotedDef(s) => s,
            FunctionDefinition::DoubleDollarDef(s) => s,
        }
    }
}

/// Return types of a function.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CreateFunctionReturns {
    /// RETURNS rettype
    Value(DataType),
    /// RETURNS TABLE ( column_name column_type [, ...] )
    Table(Vec<TableColumnDef>),
}

impl fmt::Display for CreateFunctionReturns {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(data_type) => write!(f, "RETURNS {}", data_type),
            Self::Table(columns) => {
                write!(f, "RETURNS TABLE ({})", display_comma_separated(columns))
            }
        }
    }
}

/// Table column definition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableColumnDef {
    pub name: Ident,
    pub data_type: DataType,
}

impl fmt::Display for TableColumnDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)
    }
}

/// Postgres specific feature.
///
/// See [Postgresdocs](https://www.postgresql.org/docs/15/sql-createfunction.html)
/// for more details
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct CreateFunctionBody {
    /// LANGUAGE lang_name
    pub language: Option<Ident>,
    /// RUNTIME runtime_name
    pub runtime: Option<Ident>,

    /// IMMUTABLE | STABLE | VOLATILE
    pub behavior: Option<FunctionBehavior>,
    /// AS 'definition'
    ///
    /// Note that Hive's `AS class_name` is also parsed here.
    pub as_: Option<FunctionDefinition>,
    /// RETURN expression
    pub return_: Option<Expr>,
    /// USING ...
    pub using: Option<CreateFunctionUsing>,
}

impl fmt::Display for CreateFunctionBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(language) = &self.language {
            write!(f, " LANGUAGE {language}")?;
        }
        if let Some(runtime) = &self.runtime {
            write!(f, " RUNTIME {runtime}")?;
        }
        if let Some(behavior) = &self.behavior {
            write!(f, " {behavior}")?;
        }
        if let Some(definition) = &self.as_ {
            write!(f, " AS {definition}")?;
        }
        if let Some(expr) = &self.return_ {
            write!(f, " RETURN {expr}")?;
        }
        if let Some(using) = &self.using {
            write!(f, " {using}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct CreateFunctionWithOptions {
    /// Always retry on network errors.
    pub always_retry_on_network_error: Option<bool>,
    /// Use async functions (only available for JS UDF)
    pub r#async: Option<bool>,
    /// Call in batch mode (only available for JS UDF)
    pub batch: Option<bool>,
}

/// TODO(kwannoel): Generate from the struct definition instead.
impl TryFrom<Vec<SqlOption>> for CreateFunctionWithOptions {
    type Error = StrError;

    fn try_from(with_options: Vec<SqlOption>) -> Result<Self, Self::Error> {
        let mut options = Self::default();
        for option in with_options {
            match option.name.to_string().to_lowercase().as_str() {
                "always_retry_on_network_error" => {
                    options.always_retry_on_network_error = Some(matches!(
                        option.value,
                        SqlOptionValue::Value(Value::Boolean(true))
                    ));
                }
                "async" => {
                    options.r#async = Some(matches!(
                        option.value,
                        SqlOptionValue::Value(Value::Boolean(true))
                    ))
                }
                "batch" => {
                    options.batch = Some(matches!(
                        option.value,
                        SqlOptionValue::Value(Value::Boolean(true))
                    ))
                }
                _ => {
                    return Err(StrError(format!("unknown option: {}", option.name)));
                }
            }
        }
        Ok(options)
    }
}

impl Display for CreateFunctionWithOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self == &Self::default() {
            return Ok(());
        }
        let mut options = vec![];
        if let Some(v) = self.always_retry_on_network_error {
            options.push(format!("always_retry_on_network_error = {}", v));
        }
        if let Some(v) = self.r#async {
            options.push(format!("async = {}", v));
        }
        if let Some(v) = self.batch {
            options.push(format!("batch = {}", v));
        }
        write!(f, " WITH ( {} )", display_comma_separated(&options))
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum CreateFunctionUsing {
    Link(String),
    Base64(String),
}

impl fmt::Display for CreateFunctionUsing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "USING ")?;
        match self {
            CreateFunctionUsing::Link(uri) => write!(f, "LINK '{uri}'"),
            CreateFunctionUsing::Base64(s) => {
                write!(f, "BASE64 '{s}'")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConfigParam {
    pub param: Ident,
    pub value: SetVariableValue,
}

impl fmt::Display for ConfigParam {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SET {} = {}", self.param, self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SetVariableValue {
    Single(SetVariableValueSingle),
    List(Vec<SetVariableValueSingle>),
    Default,
}

impl From<SetVariableValueSingle> for SetVariableValue {
    fn from(value: SetVariableValueSingle) -> Self {
        SetVariableValue::Single(value)
    }
}

impl fmt::Display for SetVariableValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use SetVariableValue::*;
        match self {
            Single(val) => write!(f, "{}", val),
            List(list) => write!(f, "{}", display_comma_separated(list),),
            Default => write!(f, "DEFAULT"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SetVariableValueSingle {
    Ident(Ident),
    Literal(Value),
    Raw(String),
}

impl SetVariableValueSingle {
    pub fn to_string_unquoted(&self) -> String {
        match self {
            Self::Literal(Value::SingleQuotedString(s))
            | Self::Literal(Value::DoubleQuotedString(s))
            | Self::Raw(s) => s.clone(),
            _ => self.to_string(),
        }
    }
}

impl fmt::Display for SetVariableValueSingle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use SetVariableValueSingle::*;
        match self {
            Ident(ident) => write!(f, "{}", ident),
            Literal(literal) => write!(f, "{}", literal),
            Raw(raw) => write!(f, "{}", raw),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AsOf {
    ProcessTime,
    // used by time travel
    ProcessTimeWithInterval((String, DateTimeField)),
    // the number of seconds that have elapsed since the Unix epoch, which is January 1, 1970 at 00:00:00 Coordinated Universal Time (UTC).
    TimestampNum(i64),
    TimestampString(String),
    VersionNum(i64),
    VersionString(String),
}

impl fmt::Display for AsOf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use AsOf::*;
        match self {
            ProcessTime => write!(f, " FOR SYSTEM_TIME AS OF PROCTIME()"),
            ProcessTimeWithInterval((value, leading_field)) => write!(
                f,
                " FOR SYSTEM_TIME AS OF NOW() - '{}' {}",
                value, leading_field
            ),
            TimestampNum(ts) => write!(f, " FOR SYSTEM_TIME AS OF {}", ts),
            TimestampString(ts) => write!(f, " FOR SYSTEM_TIME AS OF '{}'", ts),
            VersionNum(v) => write!(f, " FOR SYSTEM_VERSION AS OF {}", v),
            VersionString(v) => write!(f, " FOR SYSTEM_VERSION AS OF '{}'", v),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DiscardType {
    All,
}

impl fmt::Display for DiscardType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use DiscardType::*;
        match self {
            All => write!(f, "ALL"),
        }
    }
}

// We decouple "default" from none,
// so we can choose strategies that make the most sense.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub enum BackfillOrderStrategy {
    #[default]
    Default,
    None,
    Auto,
    Fixed(Vec<(ObjectName, ObjectName)>),
}

impl fmt::Display for BackfillOrderStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use BackfillOrderStrategy::*;
        match self {
            Default => write!(f, "DEFAULT"),
            None => write!(f, "NONE"),
            Auto => write!(f, "AUTO"),
            Fixed(map) => {
                let mut parts = vec![];
                for (start, end) in map {
                    parts.push(format!("{} -> {}", start, end));
                }
                write!(f, "FIXED({})", display_comma_separated(&parts))
            }
        }
    }
}

impl Statement {
    pub fn to_redacted_string(&self, keywords: RedactSqlOptionKeywordsRef) -> String {
        REDACT_SQL_OPTION_KEYWORDS.sync_scope(keywords, || self.to_string_unchecked())
    }

    /// Create a new `CREATE TABLE` statement with the given `name` and empty fields.
    pub fn default_create_table(name: ObjectName) -> Self {
        Self::CreateTable {
            name,
            or_replace: false,
            temporary: false,
            if_not_exists: false,
            columns: Vec::new(),
            wildcard_idx: None,
            constraints: Vec::new(),
            with_options: Vec::new(),
            format_encode: None,
            source_watermarks: Vec::new(),
            append_only: false,
            on_conflict: None,
            with_version_columns: Vec::new(),
            query: None,
            cdc_table_info: None,
            include_column_options: Vec::new(),
            webhook_info: None,
            engine: Engine::Hummock,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grouping_sets_display() {
        // a and b in different group
        let grouping_sets = Expr::GroupingSets(vec![
            vec![Expr::Identifier(Ident::new_unchecked("a"))],
            vec![Expr::Identifier(Ident::new_unchecked("b"))],
        ]);
        assert_eq!("GROUPING SETS ((a), (b))", format!("{}", grouping_sets));

        // a and b in the same group
        let grouping_sets = Expr::GroupingSets(vec![vec![
            Expr::Identifier(Ident::new_unchecked("a")),
            Expr::Identifier(Ident::new_unchecked("b")),
        ]]);
        assert_eq!("GROUPING SETS ((a, b))", format!("{}", grouping_sets));

        // (a, b) and (c, d) in different group
        let grouping_sets = Expr::GroupingSets(vec![
            vec![
                Expr::Identifier(Ident::new_unchecked("a")),
                Expr::Identifier(Ident::new_unchecked("b")),
            ],
            vec![
                Expr::Identifier(Ident::new_unchecked("c")),
                Expr::Identifier(Ident::new_unchecked("d")),
            ],
        ]);
        assert_eq!(
            "GROUPING SETS ((a, b), (c, d))",
            format!("{}", grouping_sets)
        );
    }

    #[test]
    fn test_rollup_display() {
        let rollup = Expr::Rollup(vec![vec![Expr::Identifier(Ident::new_unchecked("a"))]]);
        assert_eq!("ROLLUP (a)", format!("{}", rollup));

        let rollup = Expr::Rollup(vec![vec![
            Expr::Identifier(Ident::new_unchecked("a")),
            Expr::Identifier(Ident::new_unchecked("b")),
        ]]);
        assert_eq!("ROLLUP ((a, b))", format!("{}", rollup));

        let rollup = Expr::Rollup(vec![
            vec![Expr::Identifier(Ident::new_unchecked("a"))],
            vec![Expr::Identifier(Ident::new_unchecked("b"))],
        ]);
        assert_eq!("ROLLUP (a, b)", format!("{}", rollup));

        let rollup = Expr::Rollup(vec![
            vec![Expr::Identifier(Ident::new_unchecked("a"))],
            vec![
                Expr::Identifier(Ident::new_unchecked("b")),
                Expr::Identifier(Ident::new_unchecked("c")),
            ],
            vec![Expr::Identifier(Ident::new_unchecked("d"))],
        ]);
        assert_eq!("ROLLUP (a, (b, c), d)", format!("{}", rollup));
    }

    #[test]
    fn test_cube_display() {
        let cube = Expr::Cube(vec![vec![Expr::Identifier(Ident::new_unchecked("a"))]]);
        assert_eq!("CUBE (a)", format!("{}", cube));

        let cube = Expr::Cube(vec![vec![
            Expr::Identifier(Ident::new_unchecked("a")),
            Expr::Identifier(Ident::new_unchecked("b")),
        ]]);
        assert_eq!("CUBE ((a, b))", format!("{}", cube));

        let cube = Expr::Cube(vec![
            vec![Expr::Identifier(Ident::new_unchecked("a"))],
            vec![Expr::Identifier(Ident::new_unchecked("b"))],
        ]);
        assert_eq!("CUBE (a, b)", format!("{}", cube));

        let cube = Expr::Cube(vec![
            vec![Expr::Identifier(Ident::new_unchecked("a"))],
            vec![
                Expr::Identifier(Ident::new_unchecked("b")),
                Expr::Identifier(Ident::new_unchecked("c")),
            ],
            vec![Expr::Identifier(Ident::new_unchecked("d"))],
        ]);
        assert_eq!("CUBE (a, (b, c), d)", format!("{}", cube));
    }

    #[test]
    fn test_array_index_display() {
        let array_index = Expr::Index {
            obj: Box::new(Expr::Identifier(Ident::new_unchecked("v1"))),
            index: Box::new(Expr::Value(Value::Number("1".into()))),
        };
        assert_eq!("v1[1]", format!("{}", array_index));

        let array_index2 = Expr::Index {
            obj: Box::new(array_index),
            index: Box::new(Expr::Value(Value::Number("1".into()))),
        };
        assert_eq!("v1[1][1]", format!("{}", array_index2));
    }

    #[test]
    /// issue: https://github.com/risingwavelabs/risingwave/issues/7635
    fn test_nested_op_display() {
        let binary_op = Expr::BinaryOp {
            left: Box::new(Expr::Value(Value::Boolean(true))),
            op: BinaryOperator::Or,
            right: Box::new(Expr::IsNotFalse(Box::new(Expr::Value(Value::Boolean(
                true,
            ))))),
        };
        assert_eq!("true OR true IS NOT FALSE", format!("{}", binary_op));

        let unary_op = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::IsNotFalse(Box::new(Expr::Value(Value::Boolean(
                true,
            ))))),
        };
        assert_eq!("NOT true IS NOT FALSE", format!("{}", unary_op));
    }

    #[test]
    fn test_create_function_display() {
        let create_function = Statement::CreateFunction {
            or_replace: false,
            temporary: false,
            if_not_exists: false,
            name: ObjectName(vec![Ident::new_unchecked("foo")]),
            args: Some(vec![OperateFunctionArg::unnamed(DataType::Int)]),
            returns: Some(CreateFunctionReturns::Value(DataType::Int)),
            params: CreateFunctionBody {
                language: Some(Ident::new_unchecked("python")),
                runtime: None,
                behavior: Some(FunctionBehavior::Immutable),
                as_: Some(FunctionDefinition::SingleQuotedDef("SELECT 1".to_owned())),
                return_: None,
                using: None,
            },
            with_options: CreateFunctionWithOptions {
                always_retry_on_network_error: None,
                r#async: None,
                batch: None,
            },
        };
        assert_eq!(
            "CREATE FUNCTION foo(INT) RETURNS INT LANGUAGE python IMMUTABLE AS 'SELECT 1'",
            format!("{}", create_function)
        );
        let create_function = Statement::CreateFunction {
            or_replace: false,
            temporary: false,
            if_not_exists: false,
            name: ObjectName(vec![Ident::new_unchecked("foo")]),
            args: Some(vec![OperateFunctionArg::unnamed(DataType::Int)]),
            returns: Some(CreateFunctionReturns::Value(DataType::Int)),
            params: CreateFunctionBody {
                language: Some(Ident::new_unchecked("python")),
                runtime: None,
                behavior: Some(FunctionBehavior::Immutable),
                as_: Some(FunctionDefinition::SingleQuotedDef("SELECT 1".to_owned())),
                return_: None,
                using: None,
            },
            with_options: CreateFunctionWithOptions {
                always_retry_on_network_error: Some(true),
                r#async: None,
                batch: None,
            },
        };
        assert_eq!(
            "CREATE FUNCTION foo(INT) RETURNS INT LANGUAGE python IMMUTABLE AS 'SELECT 1' WITH ( always_retry_on_network_error = true )",
            format!("{}", create_function)
        );
    }

    #[test]
    fn test_sql_option_from_secret_ref() {
        {
            let option = SqlOption::from_secret_ref(
                "password",
                SecretRefValue {
                    secret_name: ObjectName(vec![
                        Ident::from_real_value("public"),
                        Ident::from_real_value("s2"),
                    ]),
                    ref_as: SecretRefAsType::Text,
                },
            );

            assert!(matches!(option.value, SqlOptionValue::SecretRef(_)));
            assert_eq!(option.to_string(), "password = secret public.s2");
        }

        {
            let option = SqlOption::from_secret_ref(
                "certificate",
                SecretRefValue {
                    secret_name: ObjectName(vec![Ident::from_real_value("cert")]),
                    ref_as: SecretRefAsType::File,
                },
            );

            assert_eq!(option.to_string(), "certificate = secret cert AS FILE");
        }
    }
}
