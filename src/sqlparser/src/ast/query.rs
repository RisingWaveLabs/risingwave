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

use crate::ast::*;

/// The most complete variant of a `SELECT` query expression, optionally
/// including `WITH`, `UNION` / other set operations, and `ORDER BY`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Query {
    /// WITH (common table expressions, or CTEs)
    pub with: Option<With>,
    /// SELECT or UNION / EXCEPT / INTERSECT
    pub body: SetExpr,
    /// ORDER BY
    pub order_by: Vec<OrderByExpr>,
    /// `LIMIT { <N> | ALL }`
    pub limit: Option<Expr>,
    /// `OFFSET <N> [ { ROW | ROWS } ]`
    ///
    /// `ROW` and `ROWS` are noise words that don't influence the effect of the clause.
    /// They are provided for ANSI compatibility.
    pub offset: Option<String>,
    /// `FETCH { FIRST | NEXT } <N> [ PERCENT ] { ROW | ROWS } | { ONLY | WITH TIES }`
    ///
    /// `ROW` and `ROWS` as well as `FIRST` and `NEXT` are noise words that don't influence the
    /// effect of the clause. They are provided for ANSI compatibility.
    pub fetch: Option<Fetch>,
}

impl Query {
    /// Simple `VALUES` without other clauses.
    pub fn as_simple_values(&self) -> Option<&Values> {
        match &self {
            Query {
                with: None,
                body: SetExpr::Values(values),
                order_by,
                limit: None,
                offset: None,
                fetch: None,
            } if order_by.is_empty() => Some(values),
            _ => None,
        }
    }

    /// `SELECT <expr>` without other clauses.
    pub fn as_single_select_item(&self) -> Option<&Expr> {
        match &self {
            Query {
                with: None,
                body: SetExpr::Select(select),
                order_by,
                limit: None,
                offset: None,
                fetch: None,
            } if order_by.is_empty() => match select.as_ref() {
                Select {
                    distinct: Distinct::All,
                    projection,
                    from,
                    lateral_views,
                    selection: None,
                    group_by,
                    having: None,
                    window,
                } if projection.len() == 1
                    && from.is_empty()
                    && lateral_views.is_empty()
                    && group_by.is_empty()
                    && window.is_empty() =>
                {
                    match &projection[0] {
                        SelectItem::UnnamedExpr(expr) => Some(expr),
                        SelectItem::ExprWithAlias { expr, .. } => Some(expr),
                        _ => None,
                    }
                }
                _ => None,
            },
            _ => None,
        }
    }
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref with) = self.with {
            write!(f, "{} ", with)?;
        }
        write!(f, "{}", self.body)?;
        if !self.order_by.is_empty() {
            write!(f, " ORDER BY {}", display_comma_separated(&self.order_by))?;
        }
        if let Some(ref limit) = self.limit {
            write!(f, " LIMIT {}", limit)?;
        }
        if let Some(ref offset) = self.offset {
            write!(f, " OFFSET {}", offset)?;
        }
        if let Some(ref fetch) = self.fetch {
            write!(f, " {}", fetch)?;
        }
        Ok(())
    }
}

/// A node in a tree, representing a "query body" expression, roughly:
/// `SELECT ... [ {UNION|EXCEPT|INTERSECT} SELECT ...]`

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SetExpr {
    /// Restricted SELECT .. FROM .. HAVING (no ORDER BY or set operations)
    Select(Box<Select>),
    /// Parenthesized SELECT subquery, which may include more set operations
    /// in its body and an optional ORDER BY / LIMIT.
    Query(Box<Query>),
    /// UNION/EXCEPT/INTERSECT of two queries
    SetOperation {
        op: SetOperator,
        all: bool,
        corresponding: Corresponding,
        left: Box<SetExpr>,
        right: Box<SetExpr>,
    },
    Values(Values),
}

impl fmt::Display for SetExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SetExpr::Select(s) => write!(f, "{}", s),
            SetExpr::Query(q) => write!(f, "({})", q),
            SetExpr::Values(v) => write!(f, "{}", v),
            SetExpr::SetOperation {
                left,
                right,
                op,
                all,
                corresponding,
            } => {
                let all_str = if *all { " ALL" } else { "" };
                write!(f, "{} {}{}{} {}", left, op, all_str, corresponding, right)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SetOperator {
    Union,
    Except,
    Intersect,
}

impl fmt::Display for SetOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SetOperator::Union => "UNION",
            SetOperator::Except => "EXCEPT",
            SetOperator::Intersect => "INTERSECT",
        })
    }
}

/// `CORRESPONDING [ BY <left paren> <corresponding column list> <right paren> ]`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Corresponding {
    pub corresponding: bool,
    pub column_list: Option<Vec<Ident>>,
}

impl Corresponding {
    pub fn with_column_list(column_list: Option<Vec<Ident>>) -> Self {
        Self {
            corresponding: true,
            column_list,
        }
    }

    pub fn none() -> Self {
        Self {
            corresponding: false,
            column_list: None,
        }
    }

    pub fn is_corresponding(&self) -> bool {
        self.corresponding
    }

    pub fn column_list(&self) -> Option<&[Ident]> {
        self.column_list.as_deref()
    }
}

impl fmt::Display for Corresponding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.corresponding {
            write!(f, " CORRESPONDING")?;
            if let Some(column_list) = &self.column_list {
                write!(f, " BY ({})", display_comma_separated(column_list))?;
            }
        }
        Ok(())
    }
}

/// A restricted variant of `SELECT` (without CTEs/`ORDER BY`), which may
/// appear either as the only body item of an `SQLQuery`, or as an operand
/// to a set operation like `UNION`.
#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Select {
    pub distinct: Distinct,
    /// projection expressions
    pub projection: Vec<SelectItem>,
    /// FROM
    pub from: Vec<TableWithJoins>,
    /// LATERAL VIEWs
    pub lateral_views: Vec<LateralView>,
    /// WHERE
    pub selection: Option<Expr>,
    /// GROUP BY
    pub group_by: Vec<Expr>,
    /// HAVING
    pub having: Option<Expr>,
    /// WINDOW
    pub window: Vec<NamedWindow>,
}

impl fmt::Display for Select {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SELECT{}", self.distinct)?;
        write!(f, " {}", display_comma_separated(&self.projection))?;
        if !self.from.is_empty() {
            write!(f, " FROM {}", display_comma_separated(&self.from))?;
        }
        if !self.lateral_views.is_empty() {
            for lv in &self.lateral_views {
                write!(f, "{}", lv)?;
            }
        }
        if let Some(ref selection) = self.selection {
            write!(f, " WHERE {}", selection)?;
        }
        if !self.group_by.is_empty() {
            write!(f, " GROUP BY {}", display_comma_separated(&self.group_by))?;
        }
        if let Some(ref having) = self.having {
            write!(f, " HAVING {}", having)?;
        }
        if !self.window.is_empty() {
            write!(f, " WINDOW {}", display_comma_separated(&self.window))?;
        }
        Ok(())
    }
}

/// An `ALL`, `DISTINCT` or `DISTINCT ON (expr, ...)` after `SELECT`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
#[expect(clippy::enum_variant_names)]
pub enum Distinct {
    /// An optional parameter that returns all matching rows.
    #[default]
    All,
    /// A parameter that removes duplicates from the result-set.
    Distinct,
    /// An optional parameter that eliminates duplicate data based on the expressions.
    DistinctOn(Vec<Expr>),
}

impl Distinct {
    pub const fn is_all(&self) -> bool {
        matches!(self, Distinct::All)
    }

    pub const fn is_distinct(&self) -> bool {
        matches!(self, Distinct::Distinct)
    }
}

impl fmt::Display for Distinct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Distinct::All => write!(f, ""),
            Distinct::Distinct => write!(f, " DISTINCT"),
            Distinct::DistinctOn(exprs) => {
                write!(f, " DISTINCT ON ({})", display_comma_separated(exprs))
            }
        }
    }
}

/// A hive LATERAL VIEW with potential column aliases
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LateralView {
    /// LATERAL VIEW
    pub lateral_view: Expr,
    /// LATERAL VIEW table name
    pub lateral_view_name: ObjectName,
    /// LATERAL VIEW optional column aliases
    pub lateral_col_alias: Vec<Ident>,
    /// LATERAL VIEW OUTER
    pub outer: bool,
}

impl fmt::Display for LateralView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            " LATERAL VIEW{outer} {} {}",
            self.lateral_view,
            self.lateral_view_name,
            outer = if self.outer { " OUTER" } else { "" }
        )?;
        if !self.lateral_col_alias.is_empty() {
            write!(
                f,
                " AS {}",
                display_comma_separated(&self.lateral_col_alias)
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct With {
    pub recursive: bool,
    pub cte_tables: Vec<Cte>,
}

impl fmt::Display for With {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WITH {}{}",
            if self.recursive { "RECURSIVE " } else { "" },
            display_comma_separated(&self.cte_tables)
        )
    }
}

/// A single CTE (used after `WITH`): `alias [(col1, col2, ...)] AS ( query )`
///
/// The names in the column list before `AS`, when specified, replace the names
/// of the columns returned by the query. The parser does not validate that the
/// number of columns in the query matches the number of columns in the query.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Cte {
    pub alias: TableAlias,
    pub cte_inner: CteInner,
}

impl fmt::Display for Cte {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.cte_inner {
            CteInner::Query(query) => write!(f, "{} AS ({})", self.alias, query)?,
            CteInner::ChangeLog(obj_name) => {
                write!(f, "{} AS changelog from {}", self.alias, obj_name)?
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CteInner {
    Query(Box<Query>),
    ChangeLog(ObjectName),
}

/// One item of the comma-separated list following `SELECT`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SelectItem {
    /// Any expression, not followed by `[ AS ] alias`
    UnnamedExpr(Expr),
    /// Expr is an arbitrary expression, returning either a table or a column.
    /// Idents are the prefix of `*`, which are consecutive field accesses.
    /// e.g. `(table.v1).*` or `(table).v1.*`
    ExprQualifiedWildcard(Expr, Vec<Ident>),
    /// An expression, followed by `[ AS ] alias`
    ExprWithAlias { expr: Expr, alias: Ident },
    /// `alias.*` or even `schema.table.*` followed by optional except
    QualifiedWildcard(ObjectName, Option<Vec<Expr>>),
    /// An unqualified `*`, or `* except (exprs)`
    Wildcard(Option<Vec<Expr>>),
}

impl fmt::Display for SelectItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            SelectItem::UnnamedExpr(expr) => write!(f, "{}", expr),
            SelectItem::ExprWithAlias { expr, alias } => write!(f, "{} AS {}", expr, alias),
            SelectItem::ExprQualifiedWildcard(expr, prefix) => write!(
                f,
                "({}){}.*",
                expr,
                prefix
                    .iter()
                    .format_with("", |i, f| f(&format_args!(".{i}")))
            ),
            SelectItem::QualifiedWildcard(prefix, except) => match except {
                Some(cols) => write!(
                    f,
                    "{}.* EXCEPT ({})",
                    prefix,
                    cols.iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<String>>()
                        .as_slice()
                        .join(", ")
                ),
                None => write!(f, "{}.*", prefix),
            },
            SelectItem::Wildcard(except) => match except {
                Some(cols) => write!(
                    f,
                    "* EXCEPT ({})",
                    cols.iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<String>>()
                        .as_slice()
                        .join(", ")
                ),
                None => write!(f, "*"),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableWithJoins {
    pub relation: TableFactor,
    pub joins: Vec<Join>,
}

impl fmt::Display for TableWithJoins {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.relation)?;
        for join in &self.joins {
            write!(f, "{}", join)?;
        }
        Ok(())
    }
}

/// A table name or a parenthesized subquery with an optional alias
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableFactor {
    Table {
        name: ObjectName,
        alias: Option<TableAlias>,
        as_of: Option<AsOf>,
    },
    Derived {
        lateral: bool,
        subquery: Box<Query>,
        alias: Option<TableAlias>,
    },
    /// `<expr>(args)[ AS <alias> ]`
    ///
    /// Note that scalar functions can also be used in this way.
    TableFunction {
        name: ObjectName,
        alias: Option<TableAlias>,
        args: Vec<FunctionArg>,
        with_ordinality: bool,
    },
    /// Represents a parenthesized table factor. The SQL spec only allows a
    /// join expression (`(foo <JOIN> bar [ <JOIN> baz ... ])`) to be nested,
    /// possibly several times.
    ///
    /// The parser may also accept non-standard nesting of bare tables for some
    /// dialects, but the information about such nesting is stripped from AST.
    NestedJoin(Box<TableWithJoins>),
    /// `<table> MATCH_RECOGNIZE (...)`: SQL:2016 row pattern recognition applied
    /// to an input table factor.
    MatchRecognize {
        /// The input the pattern is matched over.
        table: Box<TableFactor>,
        /// `PARTITION BY <expr>, ...` — empty when omitted.
        partition_by: Vec<Expr>,
        /// `ORDER BY <expr> [ASC|DESC], ...` — empty when omitted.
        order_by: Vec<OrderByExpr>,
        /// `MEASURES <expr> AS <alias>, ...` — empty when omitted.
        measures: Vec<Measure>,
        /// `ONE ROW PER MATCH` | `ALL ROWS PER MATCH` — `None` when omitted.
        rows_per_match: Option<RowsPerMatch>,
        /// `AFTER MATCH SKIP ...` — `None` when omitted.
        after_match_skip: Option<AfterMatchSkip>,
        /// `PATTERN ( <pattern> )` — required.
        pattern: MatchRecognizePattern,
        /// `WITHIN <interval>` — bounds a match's time span; `None` when omitted.
        within: Option<Expr>,
        /// `SUBSET <name> = (<vars>), ...` — empty when omitted.
        subsets: Vec<SubsetDefinition>,
        /// `DEFINE <symbol> AS <condition>, ...` — required.
        symbols: Vec<SymbolDefinition>,
        /// Optional alias for the row-pattern output.
        alias: Option<TableAlias>,
    },
}

/// A single `MEASURES` item: `<expr> AS <alias>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Measure {
    pub expr: Expr,
    pub alias: Ident,
}

impl fmt::Display for Measure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} AS {}", self.expr, self.alias)
    }
}

/// The output mode of a `MATCH_RECOGNIZE` clause.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RowsPerMatch {
    /// `ONE ROW PER MATCH`
    OneRow,
    /// `ALL ROWS PER MATCH`
    AllRows,
}

impl fmt::Display for RowsPerMatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RowsPerMatch::OneRow => write!(f, "ONE ROW PER MATCH"),
            RowsPerMatch::AllRows => write!(f, "ALL ROWS PER MATCH"),
        }
    }
}

/// The `AFTER MATCH SKIP` strategy of a `MATCH_RECOGNIZE` clause.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AfterMatchSkip {
    /// `AFTER MATCH SKIP PAST LAST ROW`
    PastLastRow,
    /// `AFTER MATCH SKIP TO NEXT ROW`
    ToNextRow,
    /// `AFTER MATCH SKIP TO FIRST <symbol>`
    ToFirst(Ident),
    /// `AFTER MATCH SKIP TO LAST <symbol>`
    ToLast(Ident),
}

impl fmt::Display for AfterMatchSkip {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AFTER MATCH SKIP ")?;
        match self {
            AfterMatchSkip::PastLastRow => write!(f, "PAST LAST ROW"),
            AfterMatchSkip::ToNextRow => write!(f, "TO NEXT ROW"),
            AfterMatchSkip::ToFirst(symbol) => write!(f, "TO FIRST {}", symbol),
            AfterMatchSkip::ToLast(symbol) => write!(f, "TO LAST {}", symbol),
        }
    }
}

/// A single `DEFINE` item: `<symbol> AS <condition>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SymbolDefinition {
    pub symbol: Ident,
    pub definition: Expr,
}

impl fmt::Display for SymbolDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} AS {}", self.symbol, self.definition)
    }
}

/// A single `SUBSET` item: `<name> = (<pattern variable>, ...)` — a union variable.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubsetDefinition {
    pub name: Ident,
    pub members: Vec<Ident>,
}

impl fmt::Display for SubsetDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} = ({})",
            self.name,
            display_comma_separated(&self.members)
        )
    }
}

/// A row-pattern variable, or one of the row-pattern anchors.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MatchRecognizeSymbol {
    /// A named pattern variable, defined (or referenced) in `DEFINE`.
    Named(Ident),
    /// The start anchor `^`.
    Start,
    /// The end anchor `$`.
    End,
}

impl fmt::Display for MatchRecognizeSymbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MatchRecognizeSymbol::Named(symbol) => write!(f, "{}", symbol),
            MatchRecognizeSymbol::Start => write!(f, "^"),
            MatchRecognizeSymbol::End => write!(f, "$"),
        }
    }
}

/// A row-pattern expression inside `PATTERN ( ... )`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MatchRecognizePattern {
    /// A single symbol, e.g. `A`, `^`, `$`.
    Symbol(MatchRecognizeSymbol),
    /// An exclusion `{- <pattern> -}`.
    Exclude(MatchRecognizeSymbol),
    /// `PERMUTE(<symbol>, ...)`.
    Permute(Vec<MatchRecognizeSymbol>),
    /// Concatenation, e.g. `A B C`.
    Concat(Vec<MatchRecognizePattern>),
    /// A parenthesized sub-pattern `( <pattern> )`.
    Group(Box<MatchRecognizePattern>),
    /// Alternation, e.g. `A | B | C`.
    Alternation(Vec<MatchRecognizePattern>),
    /// A quantified sub-pattern, e.g. `A*`, `A+`, `A?`, `A{1,3}`. The bool is `reluctant` (a trailing
    /// `?`, e.g. `A*?`, which prefers the fewest matches).
    Repetition(Box<MatchRecognizePattern>, RepetitionQuantifier, bool),
}

impl fmt::Display for MatchRecognizePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use MatchRecognizePattern::*;
        match self {
            Symbol(symbol) => write!(f, "{}", symbol),
            Exclude(symbol) => write!(f, "{{- {} -}}", symbol),
            Permute(symbols) => write!(f, "PERMUTE({})", display_comma_separated(symbols)),
            Concat(patterns) => write!(f, "{}", display_separated(patterns, " ")),
            Group(pattern) => write!(f, "({})", pattern),
            Alternation(patterns) => write!(f, "{}", display_separated(patterns, " | ")),
            Repetition(pattern, quantifier, reluctant) => {
                write!(
                    f,
                    "{}{}{}",
                    pattern,
                    quantifier,
                    if *reluctant { "?" } else { "" }
                )
            }
        }
    }
}

/// A quantifier applied to a row-pattern, e.g. `*`, `+`, `?`, `{n,m}`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RepetitionQuantifier {
    /// `*`
    ZeroOrMore,
    /// `+`
    OneOrMore,
    /// `?`
    AtMostOne,
    /// `{n}`
    Exactly(u32),
    /// `{n,}`
    AtLeast(u32),
    /// `{,m}`
    AtMost(u32),
    /// `{n,m}`
    Range(u32, u32),
}

impl fmt::Display for RepetitionQuantifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use RepetitionQuantifier::*;
        match self {
            ZeroOrMore => write!(f, "*"),
            OneOrMore => write!(f, "+"),
            AtMostOne => write!(f, "?"),
            Exactly(n) => write!(f, "{{{}}}", n),
            AtLeast(n) => write!(f, "{{{},}}", n),
            AtMost(m) => write!(f, "{{,{}}}", m),
            Range(n, m) => write!(f, "{{{},{}}}", n, m),
        }
    }
}

impl fmt::Display for TableFactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableFactor::Table { name, alias, as_of } => {
                write!(f, "{}", name)?;
                if let Some(as_of) = as_of {
                    write!(f, "{}", as_of)?
                }
                if let Some(alias) = alias {
                    write!(f, " AS {}", alias)?;
                }
                Ok(())
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                if *lateral {
                    write!(f, "LATERAL ")?;
                }
                write!(f, "({})", subquery)?;
                if let Some(alias) = alias {
                    write!(f, " AS {}", alias)?;
                }
                Ok(())
            }
            TableFactor::TableFunction {
                name,
                alias,
                args,
                with_ordinality,
            } => {
                write!(f, "{}({})", name, display_comma_separated(args))?;
                if *with_ordinality {
                    write!(f, " WITH ORDINALITY")?;
                }
                if let Some(alias) = alias {
                    write!(f, " AS {}", alias)?;
                }
                Ok(())
            }
            TableFactor::NestedJoin(table_reference) => write!(f, "({})", table_reference),
            TableFactor::MatchRecognize {
                table,
                partition_by,
                order_by,
                measures,
                rows_per_match,
                after_match_skip,
                pattern,
                within,
                subsets,
                symbols,
                alias,
            } => {
                write!(f, "{} MATCH_RECOGNIZE (", table)?;
                if !partition_by.is_empty() {
                    write!(f, "PARTITION BY {} ", display_comma_separated(partition_by))?;
                }
                if !order_by.is_empty() {
                    write!(f, "ORDER BY {} ", display_comma_separated(order_by))?;
                }
                if !measures.is_empty() {
                    write!(f, "MEASURES {} ", display_comma_separated(measures))?;
                }
                if let Some(rows_per_match) = rows_per_match {
                    write!(f, "{} ", rows_per_match)?;
                }
                if let Some(after_match_skip) = after_match_skip {
                    write!(f, "{} ", after_match_skip)?;
                }
                write!(f, "PATTERN ({}) ", pattern)?;
                if let Some(within) = within {
                    write!(f, "WITHIN {} ", within)?;
                }
                if !subsets.is_empty() {
                    write!(f, "SUBSET {} ", display_comma_separated(subsets))?;
                }
                write!(f, "DEFINE {})", display_comma_separated(symbols))?;
                if let Some(alias) = alias {
                    write!(f, " AS {}", alias)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableAlias {
    pub name: Ident,
    pub columns: Vec<Ident>,
}

impl fmt::Display for TableAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if !self.columns.is_empty() {
            write!(f, " ({})", display_comma_separated(&self.columns))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Join {
    pub relation: TableFactor,
    pub join_operator: JoinOperator,
}

impl fmt::Display for Join {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn prefix(constraint: &JoinConstraint) -> &'static str {
            match constraint {
                JoinConstraint::Natural => "NATURAL ",
                _ => "",
            }
        }
        fn suffix(constraint: &'_ JoinConstraint) -> impl fmt::Display + '_ {
            struct Suffix<'a>(&'a JoinConstraint);
            impl fmt::Display for Suffix<'_> {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    match self.0 {
                        JoinConstraint::On(expr) => write!(f, " ON {}", expr),
                        JoinConstraint::Using(attrs) => {
                            write!(f, " USING({})", display_comma_separated(attrs))
                        }
                        _ => Ok(()),
                    }
                }
            }
            Suffix(constraint)
        }
        match &self.join_operator {
            JoinOperator::Inner(constraint) => write!(
                f,
                " {}JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::LeftOuter(constraint) => write!(
                f,
                " {}LEFT JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::RightOuter(constraint) => write!(
                f,
                " {}RIGHT JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::FullOuter(constraint) => write!(
                f,
                " {}FULL JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::CrossJoin => write!(f, " CROSS JOIN {}", self.relation),
            JoinOperator::AsOfInner(constraint) => write!(
                f,
                " {}ASOF JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::AsOfLeft(constraint) => write!(
                f,
                " {}ASOF LEFT JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinOperator {
    Inner(JoinConstraint),
    LeftOuter(JoinConstraint),
    RightOuter(JoinConstraint),
    FullOuter(JoinConstraint),
    CrossJoin,
    AsOfInner(JoinConstraint),
    AsOfLeft(JoinConstraint),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<Ident>),
    Natural,
    None,
}

/// An `ORDER BY` expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderByExpr {
    pub expr: Expr,
    /// Optional `ASC` or `DESC`
    pub asc: Option<bool>,
    /// Optional `NULLS FIRST` or `NULLS LAST`
    pub nulls_first: Option<bool>,
}

impl fmt::Display for OrderByExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        match self.asc {
            Some(true) => write!(f, " ASC")?,
            Some(false) => write!(f, " DESC")?,
            None => (),
        }
        match self.nulls_first {
            Some(true) => write!(f, " NULLS FIRST")?,
            Some(false) => write!(f, " NULLS LAST")?,
            None => (),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fetch {
    pub with_ties: bool,
    pub quantity: Option<String>,
}

impl fmt::Display for Fetch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let extension = if self.with_ties { "WITH TIES" } else { "ONLY" };
        if let Some(ref quantity) = self.quantity {
            write!(f, "FETCH FIRST {} ROWS {}", quantity, extension)
        } else {
            write!(f, "FETCH FIRST ROWS {}", extension)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Top {
    /// SQL semantic equivalent of LIMIT but with same structure as FETCH.
    pub with_ties: bool,
    pub percent: bool,
    pub quantity: Option<Expr>,
}

impl fmt::Display for Top {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let extension = if self.with_ties { " WITH TIES" } else { "" };
        if let Some(ref quantity) = self.quantity {
            let percent = if self.percent { " PERCENT" } else { "" };
            write!(f, "TOP ({}){}{}", quantity, percent, extension)
        } else {
            write!(f, "TOP{}", extension)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Values(pub Vec<Vec<Expr>>);

impl fmt::Display for Values {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VALUES ")?;
        let mut delim = "";
        for row in &self.0 {
            write!(f, "{}", delim)?;
            delim = ", ";
            write!(f, "({})", display_comma_separated(row))?;
        }
        Ok(())
    }
}

/// A named window definition in the WINDOW clause
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NamedWindow {
    pub name: Ident,
    pub window_spec: WindowSpec,
}

impl fmt::Display for NamedWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} AS ({})", self.name, self.window_spec)
    }
}
