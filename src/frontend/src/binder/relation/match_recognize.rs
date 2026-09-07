// Copyright 2026 RisingWave Labs
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

use std::collections::{BTreeSet, HashMap};

use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::Field;
use risingwave_common::types::{DataType, Decimal, Interval, ScalarImpl};
use risingwave_expr::aggregate::PbAggKind;
use risingwave_sqlparser::ast::{
    AfterMatchSkip, Expr as AstExpr, Function, FunctionArg, FunctionArgExpr, Ident,
    MatchRecognizePattern, MatchRecognizeSymbol, Measure, OrderByExpr, RowsPerMatch,
    SubsetDefinition, SymbolDefinition, TableAlias, TableFactor, Value as AstValue,
};
use thiserror_ext::AsReport;

use super::{Binder, Relation};
use crate::error::Result as RwResult;
use crate::expr::{
    AggCall, Expr, ExprImpl, ExprRewriter, ExprType, ExprVisitor, FunctionCall, InputRef, Literal,
    OrderBy,
};
use crate::optimizer::plan_node::generic::PlanAggCall;
use crate::utils::Condition;

/// One navigation input that a measure expression reads. A measure is lowered to an expression over
/// a synthetic row whose `i`-th column is produced by `slots[i]`; the executor materializes that row
/// per match (the column values are only knowable once the match and its per-row labels are found)
/// and then evaluates the expression.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MeasureSlot {
    pub kind: MeasureSlotKind,
    /// Pattern variables this slot navigates over: one for a plain variable, several for a `SUBSET`
    /// union variable. A row matches if its label is any of these. Empty for `CLASSIFIER`.
    pub vars: Vec<String>,
    /// Input column index to read. Ignored for [`MeasureSlotKind::Classifier`].
    pub col_idx: usize,
    /// The slot's output type: the input column type for navigation, varchar for classifier.
    pub data_type: DataType,
    /// The aggregate to run for [`MeasureSlotKind::Sum`], over a single input column (the projected
    /// `col_idx`) of the rows whose label is in `vars`. `None` for other kinds.
    pub agg: Option<PlanAggCall>,
}

/// How a [`MeasureSlot`] resolves against the rows of a match. This is the wire enum used
/// directly (the variants are documented in `stream_plan.proto`): a parallel binder-side enum
/// would only add a conversion layer for the plan node to keep in lockstep.
pub use risingwave_pb::stream_plan::match_recognize_measure_slot::Kind as MeasureSlotKind;

/// A bound `MEASURES` item: an expression over the per-match synthetic row, its navigation slots,
/// and the output name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BoundMeasure {
    /// Expression over the synthetic row: `InputRef(i)` reads `slots[i]`.
    pub expr: ExprImpl,
    pub name: String,
    pub slots: Vec<MeasureSlot>,
}

/// How a [`DefineSlot`] resolves against the row being tested for membership in a pattern
/// variable. The wire enum, used directly (documented in `stream_plan.proto`); see
/// [`MeasureSlotKind`] for why there is no parallel binder-side enum.
pub use risingwave_pb::stream_plan::match_recognize_define_slot::Kind as DefineSlotKind;

/// One input a `DEFINE` predicate reads. A predicate is lowered to an expression over a synthetic
/// row whose `i`-th column is produced by `slots[i]`; the executor materializes that row for each
/// candidate row from the sorted partition and the in-progress match's labels.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DefineSlot {
    pub kind: DefineSlotKind,
    /// Pattern variables for `RunningFirst`/`RunningLast` (several for a `SUBSET`); empty otherwise.
    pub vars: Vec<String>,
    /// Input column index to read.
    pub col_idx: usize,
    /// Physical offset for `Prev`/`Next` (>= 1); `0` for the other kinds.
    pub offset: usize,
}

/// A bound `DEFINE` item: a pattern variable, the predicate over its [`DefineSlot`]s, and the slots.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BoundSymbolDefinition {
    pub symbol: String,
    /// Predicate over the synthetic row: `InputRef(i)` reads `slots[i]`.
    pub definition: ExprImpl,
    pub slots: Vec<DefineSlot>,
}

#[derive(Debug, Clone)]
pub struct BoundMatchRecognize {
    pub input: Relation,
    pub partition_by: Vec<ExprImpl>,
    pub order_by: Vec<ExprImpl>,
    pub measures: Vec<BoundMeasure>,
    pub rows_per_match: Option<RowsPerMatch>,
    pub after_match_skip: Option<AfterMatchSkip>,
    pub pattern: MatchRecognizePattern,
    pub defines: Vec<BoundSymbolDefinition>,
    /// `WITHIN` span check, lowered to a predicate over a synthetic `[last_order_key,
    /// first_order_key]` row: `InputRef(0) - InputRef(1) <= <interval>`. `None` when omitted.
    pub within: Option<ExprImpl>,
    /// `WITHIN` deadline expr, `first_order_key + <interval>`, over a synthetic `[first_order_key]`
    /// row (`InputRef(0) + <interval>`): the watermark at which a partial starting at that row
    /// expires. Drives idle-partition eviction. `None` when there is no `WITHIN`.
    pub within_deadline: Option<ExprImpl>,
}

impl BoundMatchRecognize {
    /// Every bound expression this node carries, in one place, so relation traversals
    /// (correlation checks, recursive expression rewrites) cannot silently skip a field. The
    /// measure/define/WITHIN expressions are over synthetic slot rows — their `InputRef`s index
    /// slots, not the input schema — but they can still carry `CorrelatedInputRef`s from an
    /// enclosing query and are subject to the same expression-local rewrites as everything else.
    pub fn exprs(&self) -> impl Iterator<Item = &ExprImpl> {
        self.partition_by
            .iter()
            .chain(self.order_by.iter())
            .chain(self.measures.iter().map(|m| &m.expr))
            .chain(self.defines.iter().map(|d| &d.definition))
            .chain(self.within.iter())
            .chain(self.within_deadline.iter())
    }

    /// See [`BoundMatchRecognize::exprs`].
    pub fn exprs_mut(&mut self) -> impl Iterator<Item = &mut ExprImpl> {
        self.partition_by
            .iter_mut()
            .chain(self.order_by.iter_mut())
            .chain(self.measures.iter_mut().map(|m| &mut m.expr))
            .chain(self.defines.iter_mut().map(|d| &mut d.definition))
            .chain(self.within.iter_mut())
            .chain(self.within_deadline.iter_mut())
    }
}

impl Binder {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn bind_match_recognize(
        &mut self,
        table: &TableFactor,
        partition_by: &[AstExpr],
        order_by: &[OrderByExpr],
        measures: &[Measure],
        rows_per_match: &Option<RowsPerMatch>,
        after_match_skip: &Option<AfterMatchSkip>,
        pattern: &MatchRecognizePattern,
        within: &Option<AstExpr>,
        subsets: &[SubsetDefinition],
        symbols: &[SymbolDefinition],
        alias: Option<&TableAlias>,
    ) -> RwResult<BoundMatchRecognize> {
        // ALL ROWS PER MATCH is not in the v1 subset.
        if matches!(rows_per_match, Some(RowsPerMatch::AllRows)) {
            bail_not_implemented!("ALL ROWS PER MATCH");
        }
        // The pattern is expanded eagerly into NFA states, on the compute node, when the actor is
        // built. Validate the bounds here so an unusable pattern is a rejected statement rather than
        // a committed materialized view whose actors die on creation and on every recovery.
        validate_pattern(pattern)?;
        self.push_context();

        // Bind the input. This registers the input's columns in the current context.
        let input = self.bind_table_factor(table)?;

        // PARTITION BY / ORDER BY are evaluated over the input rows, so bind them while only the
        // input is in scope — unqualified column references resolve unambiguously here.
        let partition_by = partition_by
            .iter()
            .map(|e| self.bind_expr(e))
            .collect::<RwResult<Vec<_>>>()?;
        // v1 supports only the default ordering (ascending, default null placement). The plan, proto
        // and executor carry only the ORDER BY *expressions* — not direction or null placement — so a
        // `DESC` or an explicit `NULLS FIRST|LAST` would be silently dropped and the executor would
        // sort the wrong way. Reject it explicitly rather than compile to incorrect behaviour.
        for o in order_by {
            if o.asc == Some(false) || o.nulls_first.is_some() {
                bail_not_implemented!(
                    "MATCH_RECOGNIZE ORDER BY currently supports only ascending order with default \
                     null placement (no DESC or explicit NULLS FIRST/LAST)"
                );
            }
        }
        let order_by = order_by
            .iter()
            .map(|o| self.bind_expr(&o.expr))
            .collect::<RwResult<Vec<_>>>()?;

        // WITHIN: lower `WITHIN <interval>` to a span check over a synthetic [last, first] row, i.e.
        // `InputRef(0) - InputRef(1) <= <interval>`, against the leading ORDER BY column's type. The
        // expression machinery handles the typed arithmetic (timestamp − timestamp → interval, etc).
        let (within, within_deadline) = match within {
            Some(e) => {
                let bound = self.bind_expr(e)?;
                // The lowered predicate/deadline are evaluated over synthetic order-key rows, where
                // any reference into the original input row is out of bounds. Only a constant bound
                // is meaningful there, so reject everything else at bind time.
                if !bound.is_const() {
                    bail_not_implemented!(
                        "MATCH_RECOGNIZE WITHIN bound must be a constant expression; \
                         input column references are not supported"
                    );
                }
                // The bound is the maximum span of a match, compared as `last - first <= bound`
                // and used as the eviction deadline `first + bound`. A NULL bound makes the span
                // predicate never-true-nor-false — it silently bounds NOTHING while reading as if
                // it did — and a zero or negative bound can never hold a multi-row match, leaving
                // a permanently empty view. All three are certainly not what the author meant, so
                // reject them at bind time. (Positivity is checked for the carrier types the
                // `order_key + bound` arithmetic admits; an exotic type falls through to runtime
                // semantics.)
                let is_positive = match bound.try_fold_const().expect("checked is_const")? {
                    None => Some(false),
                    Some(scalar) => match &scalar {
                        ScalarImpl::Int16(v) => Some(*v > 0),
                        ScalarImpl::Int32(v) => Some(*v > 0),
                        ScalarImpl::Int64(v) => Some(*v > 0),
                        ScalarImpl::Float32(v) => Some(v.into_inner() > 0.0),
                        ScalarImpl::Float64(v) => Some(v.into_inner() > 0.0),
                        ScalarImpl::Decimal(d) => Some(*d > Decimal::from(0)),
                        // Positive in total is not enough for an interval: `timestamp + interval`
                        // adds months, days and microseconds as separate checked steps, so a
                        // mixed-sign bound (`'1 month -29 days'`) can overflow on one component
                        // while its true sum is representable — and the executor reads an
                        // out-of-range deadline as a window that never closes, which would then
                        // admit rows past the real deadline. Requiring every component to be
                        // non-negative makes the deadline monotone in the bound, so "out of range"
                        // means exactly "past every representable order key".
                        ScalarImpl::Interval(iv) => Some(
                            iv.months() >= 0
                                && iv.days() >= 0
                                && iv.usecs() >= 0
                                && *iv != Interval::from_month_day_usec(0, 0, 0),
                        ),
                        _ => None,
                    },
                };
                if is_positive == Some(false) {
                    return Err(crate::error::ErrorCode::NotSupported(
                        "a MATCH_RECOGNIZE WITHIN bound that is NULL, zero or negative, or an \
                         interval with a negative component"
                            .to_owned(),
                        "the bound is the maximum span of a match; a non-positive bound can never \
                         hold a multi-row match (the view would stay empty) and a NULL bound \
                         bounds nothing — use a positive constant interval whose months, days and \
                         seconds are all non-negative"
                            .to_owned(),
                    )
                    .into());
                }
                let Some(order_key) = order_by.first() else {
                    bail_not_implemented!("WITHIN requires an ORDER BY column");
                };
                let (predicate, deadline) = lower_within(order_key.return_type(), bound)?;
                (Some(predicate), Some(deadline))
            }
            None => {
                // No WITHIN: an unmatched partial can be completed by an arbitrarily distant future
                // row, so it is retained until matched. For patterns whose live partial spans a
                // bounded number of rows that bounds state by PARTITION BY key cardinality — but a
                // pattern with an unbounded quantifier that stays satisfiable (`(A+ B)` where rows
                // keep satisfying A and B never arrives) retains EVERY row of the partition, with
                // no bound from key cardinality. Warn the author about both regimes (a client
                // NOTICE, visible in the CREATE output); we do not forbid it, matching SQL
                // semantics and Flink's behaviour.
                crate::session::current::notice_to_user(
                    "MATCH_RECOGNIZE without a WITHIN clause retains unmatched partial matches \
                     indefinitely. If every partial the pattern can hold spans a bounded number \
                     of rows, state is bounded by the number of distinct PARTITION BY keys; but a \
                     pattern with an unbounded quantifier that keeps matching (e.g. (A+ B) on a \
                     partition where A stays true and B never arrives) retains every row of that \
                     partition. Add a WITHIN clause to bound state to a time window.",
                );
                (None, None)
            }
        };

        // Snapshot the input columns so each pattern variable can be registered as an alias over
        // them. After this, `A.col` (a pattern-variable-qualified reference) resolves to the input
        // column `col`. The variable association is preserved in the AST for execution; this step
        // only makes MEASURES/DEFINE type-check.
        let input_columns: Vec<(bool, Field)> = self
            .context
            .columns
            .iter()
            .map(|c| (c.is_hidden, c.field.clone()))
            .collect();

        let input_col_num = input_columns.len();
        let pattern_variables = {
            let mut vars = BTreeSet::new();
            collect_from_pattern(pattern, &mut vars);
            vars
        };
        // A DEFINE for a symbol that never appears in PATTERN is dead: no row can be labeled with
        // it, so its predicate is never evaluated. Accepting it silently turns a typo into a
        // pattern that matches something other than what the author wrote (the standard requires
        // every DEFINE symbol to be a primary pattern variable).
        for s in symbols {
            let symbol = s.symbol.real_value();
            if !pattern_variables.contains(&symbol) {
                return Err(crate::error::ErrorCode::InvalidInputSyntax(format!(
                    "DEFINE names `{symbol}`, which does not appear in PATTERN; its predicate \
                     could never be evaluated"
                ))
                .into());
            }
        }
        let variables = collect_pattern_variables(pattern, symbols);
        for var in &variables {
            // `collect_pattern_variables` deduplicates, so the only way this registration can fail
            // is a collision with a relation name already in scope — in practice the MATCH_RECOGNIZE
            // input itself (`FROM t ... PATTERN (t ...)`). Left unmapped that surfaces as
            // "internal error: Duplicated table name", which blames the engine for a user-visible
            // naming clash in legal-looking SQL.
            self.bind_table_to_context(input_columns.clone(), var.clone(), None, None)
                .map_err(|_| {
                    crate::error::ErrorCode::InvalidInputSyntax(format!(
                        "pattern variable `{var}` collides with the name of a relation in scope \
                         (the MATCH_RECOGNIZE input table, most likely); rename the variable or \
                         give the input a different alias"
                    ))
                })?;
        }

        // SUBSET union variables: each must be made of declared pattern variables, and is registered
        // as a further alias block (after the base variables) so `U.col` type-checks. `alias_names`
        // records the registration order so a measure InputRef can be decoded back to its variable.
        let mut alias_names = variables.clone();
        let mut subset_defs: Vec<(String, Vec<String>)> = Vec::with_capacity(subsets.len());
        for s in subsets {
            let name = s.name.real_value();
            // A SUBSET alias sharing a name with a pattern variable or an earlier SUBSET would
            // silently shadow it in every MEASURES/DEFINE reference — reject at bind time.
            if alias_names.contains(&name) {
                return Err(crate::error::ErrorCode::InvalidInputSyntax(format!(
                    "SUBSET name `{name}` collides with a pattern variable or another SUBSET"
                ))
                .into());
            }
            let members: Vec<String> = s.members.iter().map(|m| m.real_value()).collect();
            for m in &members {
                if !variables.contains(m) {
                    // A plain user error (typo), not a missing feature: SQL:2016 requires SUBSET
                    // members to be declared pattern variables.
                    return Err(crate::error::ErrorCode::InvalidInputSyntax(format!(
                        "SUBSET {name} references unknown pattern variable {m}"
                    ))
                    .into());
                }
            }
            self.bind_table_to_context(input_columns.clone(), name.clone(), None, None)
                .map_err(|_| {
                    crate::error::ErrorCode::InvalidInputSyntax(format!(
                        "SUBSET name `{name}` collides with the name of a relation in scope \
                         (the MATCH_RECOGNIZE input table, most likely); rename it or give the \
                         input a different alias"
                    ))
                })?;
            alias_names.push(name.clone());
            subset_defs.push((name, members));
        }
        let resolver = VarResolver {
            input_col_num,
            alias_names: &alias_names,
            subset_defs: &subset_defs,
        };

        // AFTER MATCH SKIP TO FIRST/LAST <var> must name a variable that appears in PATTERN: the skip
        // target is looked up among the labels of a completed match, so a variable absent from the
        // pattern can never be found there and the executor would silently fall back to skipping
        // past the last row on every match. (A DEFINE-only symbol cannot reach here — it is
        // rejected above — so absence from the pattern means the name is unknown outright.)
        if let Some(AfterMatchSkip::ToFirst(sym) | AfterMatchSkip::ToLast(sym)) = after_match_skip {
            let target = sym.real_value();
            if !pattern_variables.contains(&target) {
                bail_not_implemented!(
                    "AFTER MATCH SKIP TO FIRST/LAST references unknown pattern variable {}",
                    target
                );
            }
        }

        // Each pattern variable was registered as an identical alias block over the input columns,
        // DEFINE predicates: <symbol> AS <condition>. Each is lowered to an expression over a
        // synthetic row of navigation slots (the candidate row's columns, physical PREV/NEXT, and
        // running references to other variables), evaluated per candidate during matching.
        let input_fields: Vec<Field> = input_columns.iter().map(|(_, f)| f.clone()).collect();
        // Reject a variable defined twice. The executor keys DEFINE predicates by symbol, so
        // without this
        // check `DEFINE a AS x > 0, a AS x < 0` silently keeps whichever lowered last — the user's
        // first predicate simply stops existing, with nothing to say so.
        {
            let mut seen = std::collections::HashSet::new();
            for sym in symbols {
                let name = sym.symbol.real_value();
                if !seen.insert(name.clone()) {
                    return Err(crate::error::ErrorCode::InvalidInputSyntax(format!(
                        "pattern variable `{name}` is defined more than once in DEFINE"
                    ))
                    .into());
                }
            }
        }
        let defines = symbols
            .iter()
            .map(|s| self.lower_define(s, &resolver, &input_fields))
            .collect::<RwResult<Vec<_>>>()?;

        // Physical `PREV` in `DEFINE` may only read rows inside the match span. Rows before the
        // match are not retained: eviction deletes consumed rows, and "buffer index 0" is the
        // eligible-start floor, so a read reaching before the match start would see a real row
        // before an eviction and `NULL` after it — the same row flipping its verdict on timing
        // (and `PREV(v) IS NULL`, the idiomatic first-row test, turning a mid-partition row into a
        // spurious match start). Require each variable using `PREV(.., k)` to sit at least `k`
        // rows from the match start (see [`min_start_distances`]); everything the walk can prove
        // stays inside the match is allowed, the rest is rejected until lookbehind retention is
        // designed as its own change. `NEXT` needs no such rule: its reads go forward and are
        // bounded by the decision horizon instead.
        let min_dists = min_start_distances(pattern);
        for def in &defines {
            let max_prev = def
                .slots
                .iter()
                .filter(|s| s.kind == DefineSlotKind::Prev)
                .map(|s| s.offset)
                .max()
                .unwrap_or(0);
            if max_prev == 0 {
                continue;
            }
            // A DEFINE symbol that never appears in PATTERN is never evaluated; skip it.
            let Some(&dist) = min_dists.get(&def.symbol) else {
                continue;
            };
            if dist < max_prev as u64 {
                return Err(crate::error::ErrorCode::NotSupported(
                    format!(
                        "PREV with offset {max_prev} in DEFINE {}: the variable can occur {dist} \
                         row(s) from the match start, so the read could reach rows before the \
                         match, which are not retained",
                        def.symbol
                    ),
                    "ensure the variable is always preceded by at least as many pattern rows as \
                     the PREV offset, e.g. prefix the pattern with an anchor variable (`x` with \
                     `x AS TRUE`)"
                        .to_owned(),
                )
                .into());
            }
        }

        // MEASURES: <expr> AS <alias>.
        let measures = measures
            .iter()
            .map(|m| self.lower_measure(m, &resolver))
            .collect::<RwResult<Vec<_>>>()?;

        self.pop_context()?;

        // Output schema (ONE ROW PER MATCH): the partition-by columns followed by the measures.
        let mut output_columns: Vec<(bool, Field)> = Vec::new();
        for (i, e) in partition_by.iter().enumerate() {
            output_columns.push((
                false,
                Field::with_name(e.return_type(), format!("partition_{i}")),
            ));
        }
        for m in &measures {
            // The hidden per-match id below is addressable by explicit name even though it is
            // excluded from `SELECT *`; a measure with the same alias would make an outer
            // `SELECT _match_id` ambiguous. Reserve the name.
            if m.name == "_match_id" {
                return Err(crate::error::ErrorCode::BindError(
                    "the measure alias `_match_id` collides with the hidden per-match id column \
                     MATCH_RECOGNIZE appends; choose another alias"
                        .to_owned(),
                )
                .into());
            }
            output_columns.push((
                false,
                Field::with_name(m.expr.return_type(), m.name.clone()),
            ));
        }
        // A partition can contain many matches, and two matches may produce byte-identical
        // (partition + measures) output, so those columns are not a unique key. Append a hidden
        // per-match id column (filled by the executor) to serve as the unique stream key. It is
        // hidden, so `SELECT *` still returns only the partition and measure columns.
        output_columns.push((true, Field::with_name(DataType::Int64, "_match_id")));

        let table_name = match alias {
            Some(TableAlias { name, .. }) => name.real_value(),
            None => "match_recognize".to_owned(),
        };
        self.bind_table_to_context(output_columns, table_name, None, alias)?;

        Ok(BoundMatchRecognize {
            input,
            partition_by,
            order_by,
            measures,
            rows_per_match: rows_per_match.clone(),
            after_match_skip: after_match_skip.clone(),
            pattern: pattern.clone(),
            defines,
            within,
            within_deadline,
        })
    }

    /// Lowers one `MEASURES` item to an expression over a synthetic per-match row plus the slots
    /// that produce that row. Pattern-variable column references become navigation slots: bare
    /// `var.col` and arithmetic over such references resolve to `LAST(var.col)` (FINAL semantics
    /// under ONE ROW PER MATCH); top-level `FIRST(var.col)` / `LAST(var.col)` and `CLASSIFIER()` are
    /// supported. Nesting `FIRST`/`LAST`/`CLASSIFIER` inside a larger expression is not yet
    /// supported (it falls through to ordinary binding and is rejected as an unknown function).
    fn lower_measure(&mut self, m: &Measure, resolver: &VarResolver<'_>) -> RwResult<BoundMeasure> {
        let name = m.alias.real_value();

        // CLASSIFIER(): the pattern variable bound to the match's last row.
        if let AstExpr::Function(func) = &m.expr
            && func.name.0.len() == 1
            && func.name.0[0]
                .real_value()
                .eq_ignore_ascii_case("classifier")
        {
            reject_func_modifiers(func, "MEASURES")?;
            if !func.arg_list.args.is_empty() {
                bail_not_implemented!("CLASSIFIER() with arguments in MATCH_RECOGNIZE");
            }
            return Ok(BoundMeasure {
                expr: InputRef::new(0, DataType::Varchar).into(),
                name,
                slots: vec![MeasureSlot {
                    kind: MeasureSlotKind::Classifier,
                    vars: vec![],
                    col_idx: 0,
                    data_type: DataType::Varchar,
                    agg: None,
                }],
            });
        }

        // Top-level aggregates over the matched rows: COUNT(*), COUNT/MIN/MAX/SUM/AVG(var.col).
        if let AstExpr::Function(func) = &m.expr
            && func.name.0.len() == 1
            && matches!(
                func.name.0[0].real_value().to_ascii_lowercase().as_str(),
                "count" | "min" | "max" | "sum" | "avg"
            )
        {
            reject_func_modifiers(func, "MEASURES")?;
            let agg = func.name.0[0].real_value().to_ascii_lowercase();
            if func.arg_list.args.len() != 1 {
                bail_not_implemented!("{}() expects exactly one argument", agg.to_uppercase());
            }
            // COUNT(*): every row of the match.
            if agg == "count"
                && matches!(
                    &func.arg_list.args[0],
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard(_))
                )
            {
                return Ok(BoundMeasure {
                    expr: InputRef::new(0, DataType::Int64).into(),
                    name,
                    slots: vec![MeasureSlot {
                        kind: MeasureSlotKind::CountStar,
                        vars: vec![],
                        col_idx: 0,
                        data_type: DataType::Int64,
                        agg: None,
                    }],
                });
            }
            // agg(var.col): over the rows labeled `var`.
            let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = &func.arg_list.args[0] else {
                bail_not_implemented!(
                    "{}() argument must be a pattern-variable column",
                    agg.to_uppercase()
                );
            };
            let ExprImpl::InputRef(r) = self.bind_expr(inner)? else {
                bail_not_implemented!(
                    "{}() argument must be a pattern-variable column",
                    agg.to_uppercase()
                );
            };
            let (vars, col_idx) = resolver.resolve(r.index())?;
            let col_type = r.data_type.clone();
            // Validate the call through the regular aggregate registry, so what an aggregate
            // accepts here is exactly what it accepts anywhere else in RisingWave SQL (e.g. no
            // `max(boolean)`), and the result type is the registry's — the SQL contract must not
            // depend on where the aggregate appears.
            let infer = |kind: PbAggKind| -> RwResult<DataType> {
                Ok(AggCall::new(
                    kind.into(),
                    vec![InputRef::new(0, col_type.clone()).into()],
                    false,
                    OrderBy::any(),
                    Condition::true_cond(),
                    vec![],
                )?
                .return_type)
            };

            // COUNT / MIN / MAX fold directly over the matched rows in the executor.
            if let Some((kind, data_type)) = match agg.as_str() {
                "count" => Some((MeasureSlotKind::Count, DataType::Int64)),
                "min" => Some((MeasureSlotKind::Min, infer(PbAggKind::Min)?)),
                "max" => Some((MeasureSlotKind::Max, infer(PbAggKind::Max)?)),
                _ => None,
            } {
                // The executor's Min/Max fold raw column datums (no kernel), so the declared slot
                // type must BE the column type. Today every registry min/max signature is
                // `T -> auto`, so validation cannot change the type — but that is the registry's
                // property, not this code's; fail here rather than mislabel the output if a
                // type-changing or cast-matched signature ever appears.
                if matches!(kind, MeasureSlotKind::Min | MeasureSlotKind::Max)
                    && data_type != col_type
                {
                    bail_not_implemented!(
                        "{}() over {} in MATCH_RECOGNIZE (the aggregate registry returns {}, but \
                         the per-match evaluation folds column values directly)",
                        agg.to_uppercase(),
                        col_type,
                        data_type
                    );
                }
                return Ok(BoundMeasure {
                    expr: InputRef::new(0, data_type.clone()).into(),
                    name,
                    slots: vec![MeasureSlot {
                        kind,
                        vars,
                        col_idx,
                        data_type,
                        agg: None,
                    }],
                });
            }

            // SUM reuses RisingWave's aggregate kernel so the numeric return type stays faithful. The
            // runtime feeds the kernel a single-column chunk (the projected col), so the call's
            // argument is an InputRef to column 0. AVG is built on top as cast(sum / count).
            let sum_type = infer(PbAggKind::Sum)?;
            let sum_slot = MeasureSlot {
                kind: MeasureSlotKind::Sum,
                vars: vars.clone(),
                col_idx,
                data_type: sum_type.clone(),
                agg: Some(PlanAggCall {
                    agg_type: PbAggKind::Sum.into(),
                    return_type: sum_type.clone(),
                    inputs: vec![InputRef::new(0, col_type.clone())],
                    distinct: false,
                    order_by: vec![],
                    filter: Condition::true_cond(),
                    direct_args: vec![],
                }),
            };

            if agg == "sum" {
                return Ok(BoundMeasure {
                    expr: InputRef::new(0, sum_type).into(),
                    name,
                    slots: vec![sum_slot],
                });
            }

            // AVG = CASE WHEN count = 0 THEN NULL ELSE cast(sum AS avg_type) / count END, mirroring
            // how RisingWave's planner rewrites avg. Slot 0 is the sum, slot 1 the (non-null) count.
            let avg_type = infer(PbAggKind::Avg)?;
            let count_slot = MeasureSlot {
                kind: MeasureSlotKind::Count,
                vars,
                col_idx,
                data_type: DataType::Int64,
                agg: None,
            };
            let sum_ref: ExprImpl = InputRef::new(0, sum_type).into();
            let count_ref: ExprImpl = InputRef::new(1, DataType::Int64).into();
            let quotient: ExprImpl = FunctionCall::new(
                ExprType::Divide,
                vec![sum_ref.cast_explicit(&avg_type)?, count_ref.clone()],
            )?
            .into();
            let count_is_zero: ExprImpl =
                FunctionCall::new(ExprType::Equal, vec![count_ref, ExprImpl::literal_int(0)])?
                    .into();
            let null: ExprImpl = Literal::new(None, avg_type).into();
            let expr: ExprImpl =
                FunctionCall::new(ExprType::Case, vec![count_is_zero, null, quotient])?.into();
            return Ok(BoundMeasure {
                expr,
                name,
                slots: vec![sum_slot, count_slot],
            });
        }

        // Physical PREV/NEXT is DEFINE-only navigation in v1; in MEASURES it would read rows
        // *outside* the matched rows. Reject it by name — letting it fall through to expression
        // binding would produce a misleading "function prev does not exist".
        if let AstExpr::Function(func) = &m.expr
            && func.name.0.len() == 1
            && matches!(
                func.name.0[0].real_value().to_ascii_lowercase().as_str(),
                "prev" | "next"
            )
        {
            bail_not_implemented!(
                "physical {}() in MATCH_RECOGNIZE MEASURES (it reads rows outside the match; \
                 use FIRST/LAST over a pattern variable, or PREV in DEFINE)",
                func.name.0[0].real_value().to_uppercase()
            );
        }

        // Top-level FIRST(var.col) / LAST(var.col).
        if let AstExpr::Function(func) = &m.expr
            && func.name.0.len() == 1
            && matches!(
                func.name.0[0].real_value().to_ascii_lowercase().as_str(),
                "first" | "last"
            )
        {
            reject_func_modifiers(func, "MEASURES")?;
            let kind = if func.name.0[0].real_value().eq_ignore_ascii_case("first") {
                MeasureSlotKind::First
            } else {
                MeasureSlotKind::Last
            };
            if func.arg_list.args.len() != 1 {
                bail_not_implemented!("FIRST/LAST with an offset argument in MATCH_RECOGNIZE");
            }
            let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = &func.arg_list.args[0] else {
                bail_not_implemented!("FIRST/LAST argument must be a pattern-variable column");
            };
            let ExprImpl::InputRef(r) = self.bind_expr(inner)? else {
                bail_not_implemented!("FIRST/LAST argument must be a pattern-variable column");
            };
            let (vars, col_idx) = resolver.resolve(r.index())?;
            let data_type = r.data_type.clone();
            return Ok(BoundMeasure {
                expr: InputRef::new(0, data_type.clone()).into(),
                name,
                slots: vec![MeasureSlot {
                    kind,
                    vars,
                    col_idx,
                    data_type,
                    agg: None,
                }],
            });
        }

        // General case: bare `var.col` and arithmetic over such references. Binding succeeds via the
        // per-variable alias blocks; each resulting InputRef is then rewritten to a synthetic
        // LAST(var.col) slot.
        let expr = self.bind_expr(&m.expr).map_err(|e| {
            crate::error::ErrorCode::BindError(format!(
                "{}\nwhile binding MEASURES item `{name}`; pattern variables in scope \
                 (case-sensitive as written): {}",
                e.as_report(),
                resolver.alias_names.join(", ")
            ))
        })?;
        // An aggregate call that is not the whole measure expression has no lowering: the slot
        // rewriter below maps variable-qualified columns to LAST slots and nothing else, so an
        // embedded `sum(a.v) + 1` would carry a live AggCall into a scalar-projection plan and fail
        // (or panic) far from here, after the statement looked accepted.
        if expr.has_agg_call() {
            bail_not_implemented!(
                "an aggregate inside a larger MEASURES expression; aggregates are supported only \
                 as the whole measure (count/min/max/sum/avg over one pattern-variable column)"
            );
        }
        // Same reasoning as the DEFINE rejection: a measure is evaluated by the executor from a
        // serialized scalar expression over the slot row; a subquery has no representation there
        // and would otherwise be carried to the plan-to-proto conversion before failing — after
        // the statement looked accepted. Rejecting it here also keeps every MATCH_RECOGNIZE
        // clause expression free of relation references, which `ALTER ... RENAME`'s query
        // rewriter (`src/meta/src/controller/rename.rs`) relies on to visit only the input table.
        if expr.has_subquery() {
            return Err(crate::error::ErrorCode::NotSupported(
                format!("a subquery in the MEASURES item `{name}`"),
                "a measure must be a scalar expression over the pattern variables".to_owned(),
            )
            .into());
        }
        let mut check = InputRefBlockCheck {
            input_col_num: resolver.input_col_num,
            // Everything above the variable/subset alias blocks is internal scaffolding — the
            // `__mr_nav` placeholder relations registered while lowering DEFINE stay in the binder
            // context, so a user measure can name them. Un-checked, such a reference reaches
            // `resolve_unchecked` with a block index past `alias_names` and panics the frontend.
            nav_floor: (1 + resolver.alias_names.len()) * resolver.input_col_num,
            unqualified: false,
            internal: false,
        };
        check.visit_expr(&expr);
        if check.unqualified {
            bail_not_implemented!(
                "unqualified or non-pattern-variable column reference in MATCH_RECOGNIZE MEASURES"
            );
        }
        if check.internal {
            return Err(crate::error::ErrorCode::InvalidInputSyntax(
                "a MATCH_RECOGNIZE MEASURES expression references an internal navigation column \
                 (`__mr_nav*`); only input columns qualified by a pattern variable are addressable"
                    .to_owned(),
            )
            .into());
        }
        let mut rewriter = SlotLoweringRewriter {
            resolver,
            slots: Vec::new(),
        };
        let expr = rewriter.rewrite_expr(expr);
        Ok(BoundMeasure {
            expr,
            name,
            slots: rewriter.slots,
        })
    }

    /// Lowers one `DEFINE` predicate to an expression over a synthetic row of [`DefineSlot`]s.
    /// `PREV`/`NEXT`/`FIRST`/`LAST(...)` navigation functions are extracted into slots first (they do
    /// not bind as ordinary functions); the remaining variable-qualified columns bind via the alias
    /// blocks and are mapped to self slots (the defined variable / unqualified) or running slots.
    fn lower_define(
        &mut self,
        s: &SymbolDefinition,
        resolver: &VarResolver<'_>,
        input_fields: &[Field],
    ) -> RwResult<BoundSymbolDefinition> {
        let symbol = s.symbol.real_value();
        // A per-symbol prefix makes diagnostics for the placeholder relation and columns easier to
        // associate with the DEFINE item that created them.
        let prefix = format!("{NAV_TABLE}_{symbol}");

        let mut cond = s.definition.clone();
        let mut extractor = NavExtractor {
            input_fields,
            resolver,
            symbol: &symbol,
            prefix: &prefix,
            nav_slots: Vec::new(),
            nav_fields: Vec::new(),
        };
        extractor.rewrite(&mut cond)?;
        let NavExtractor {
            nav_slots,
            nav_fields,
            ..
        } = extractor;

        // Bring the navigation placeholders into scope as a synthetic relation so the predicate
        // type-checks. Capture the base index first, then restore the context immediately after
        // binding: another DEFINE must not be able to address this predicate's internal columns.
        //
        // Why a synthetic relation rather than a bespoke binder: after extraction each navigation
        // expression is a fresh column of a known type, and the rest of the predicate is ordinary
        // SQL over the input/variable columns. Registering the placeholders as a relation lets the
        // normal `bind_expr` resolve everything in one pass (name resolution, coercion, operator
        // type-checking) and hand back `InputRef`s we then remap to slots. Building a separate typed
        // binder for the predicate would duplicate that machinery for no behavioural gain. The
        // relation name is internal (`__mr_nav_*`) and never escapes this predicate's binding.
        let nav_base = self.context.columns.len();
        let bind_result = if nav_fields.is_empty() {
            self.bind_expr(&cond)
        } else {
            let context = self.context.clone();
            let cols: Vec<(bool, Field)> = nav_fields.iter().map(|f| (false, f.clone())).collect();
            let result = self
                .bind_table_to_context(cols, prefix.clone(), None, None)
                .and_then(|_| self.bind_expr(&cond));
            self.context = context;
            result
        };

        // On failure, list the variables actually in scope: the classic trap is identifier case
        // folding — `PATTERN ("A" ...)` registers `"A"` while an unquoted `A.v` in the predicate
        // folds to `a.v` and misses it, and the generic bind error gives no way to see that.
        let expr = bind_result.map_err(|e| {
            crate::error::ErrorCode::BindError(format!(
                "{}\nwhile binding the DEFINE predicate of `{symbol}`; pattern variables in scope \
                 (case-sensitive as written): {}",
                e.as_report(),
                resolver.alias_names.join(", ")
            ))
        })?;
        // A DEFINE predicate is evaluated per candidate row by the executor, from a serialized scalar
        // expression; a subquery has no representation there and would otherwise be carried all the
        // way to the plan-to-proto conversion before failing, i.e. after the statement looked fine.
        if expr.has_subquery() {
            return Err(crate::error::ErrorCode::NotSupported(
                format!("a subquery in the DEFINE predicate of {symbol}"),
                "a DEFINE predicate must be a scalar expression over the pattern variables"
                    .to_owned(),
            )
            .into());
        }
        // The executor reads the predicate's result as a boolean, so a non-boolean DEFINE must
        // fail here with a normal binder error — not on the compute node once data arrives. Same
        // rule as WHERE/HAVING (untyped literals cast implicitly); Flink rejects this at
        // validation too ("DEFINE clause must be a condition").
        let clause = format!("the DEFINE predicate of {symbol}");
        let expr = expr.enforce_bool_clause(&clause)?;

        let (definition, slots) = {
            let mut rewriter = DefineSlotRewriter {
                resolver,
                defined_var: &symbol,
                nav_base,
                nav_slots: &nav_slots,
                slots: Vec::new(),
            };
            let definition = rewriter.rewrite_expr(expr);
            (definition, rewriter.slots)
        };
        Ok(BoundSymbolDefinition {
            symbol,
            definition,
            slots,
        })
    }
}

/// Rejects function-call modifiers on the specially-lowered `MATCH_RECOGNIZE` functions: the
/// `MEASURES` aggregates and `FIRST`/`LAST`/`CLASSIFIER`, and the `DEFINE` navigation functions
/// `PREV`/`NEXT`/`FIRST`/`LAST`. Both lowerings match these by name and read only the argument list,
/// so a modifier would be dropped and the plain call evaluated, silently producing wrong results.
/// `clause` names the clause for the error message (`MEASURES` or `DEFINE`).
fn reject_func_modifiers(func: &Function, clause: &str) -> RwResult<()> {
    let offending = if func.arg_list.distinct {
        Some("DISTINCT")
    } else if !func.arg_list.order_by.is_empty() {
        Some("ORDER BY")
    } else if func.arg_list.ignore_nulls {
        Some("IGNORE NULLS")
    } else if func.arg_list.variadic {
        Some("VARIADIC")
    } else if func.filter.is_some() {
        Some("FILTER")
    } else if func.over.is_some() {
        Some("OVER")
    } else if func.within_group.is_some() {
        Some("WITHIN GROUP")
    } else if func.scalar_as_agg {
        Some("AGGREGATE")
    } else {
        None
    };
    if let Some(modifier) = offending {
        bail_not_implemented!(
            "{} on {}() in MATCH_RECOGNIZE {}",
            modifier,
            func.name.0[0].real_value().to_uppercase(),
            clause
        );
    }
    Ok(())
}

/// Largest bound accepted in a `{n}` / `{n,}` / `{n,m}` / `{,m}` range quantifier.
///
/// `Nfa::compile` expands a range quantifier eagerly: `min` mandatory copies of the inner pattern
/// plus `max - min` optional copies, at 2 NFA states per pattern variable and 2 more per optional
/// wrapper. Repeating a single variable therefore costs up to `4` states per repetition, so this cap
/// bounds such a quantifier at `4 * 1000 = 4000` states, while leaving two orders of magnitude of
/// headroom over the bounds real patterns use (single or low double digits). A larger inner pattern
/// costs proportionally more per repetition and is bounded by [`MAX_PATTERN_NFA_STATES`] instead.
const MAX_QUANTIFIER_BOUND: u32 = 1000;

/// Largest estimated NFA state count accepted for a whole pattern.
///
/// [`MAX_QUANTIFIER_BOUND`] alone does not bound the pattern: quantifiers nest, and nesting
/// multiplies, so `((a{900}){900})` would expand to ~810000 copies of `a` while every individual
/// bound is legal. This cap bounds the product. For scale: `PERMUTE` of the maximum
/// [`MAX_PERMUTE_VARS`] variables estimates 8642 states (8.6% of the budget) and `(a b c){1000}`
/// estimates 6000, so realistic patterns are far below it.
///
/// This is a *memory* bound, not a throughput one. The NFA is simulated from every candidate start
/// for every row, so a pattern anywhere near this many states would build successfully and still be
/// far too slow to be useful; the cap exists only to keep an absurd pattern from exhausting the
/// compute node's memory while the actor is being built.
const MAX_PATTERN_NFA_STATES: u64 = 100_000;

/// Operational cap on a physical `PREV` offset in `DEFINE`.
///
/// The offset is not just a wire value: `PREV(col, k)` requires the variable to sit at least `k`
/// mandatory rows from the match start (see [`min_start_distances`]), so it scales the pattern's
/// required prefix. The cap is deliberately small — far above any observed real pattern (offsets
/// of 1–3), far below anything degenerate. (Physical `NEXT` is rejected in `DEFINE` outright; see
/// the navigation lowering.)
const MAX_NAV_OFFSET: usize = 100;

/// Largest number of variables accepted in `PERMUTE(...)`.
///
/// `PERMUTE` expands to the alternation of all `n!` orderings of its variables, so the NFA grows
/// factorially.
const MAX_PERMUTE_VARS: usize = 6;

/// Rejects a `PERMUTE` with too many variables. This is the only enforcement point: the pattern
/// lowering in `optimizer::plan_node::stream_match_recognize` no longer repeats the check, since every
/// pattern that reaches it has passed [`validate_pattern`].
fn reject_oversized_permute(count: usize) -> RwResult<()> {
    if count > MAX_PERMUTE_VARS {
        return Err(crate::error::ErrorCode::NotSupported(
            format!("PERMUTE over {count} variables (expands to {count}! orderings)"),
            format!("PERMUTE supports at most {MAX_PERMUTE_VARS} variables"),
        )
        .into());
    }
    Ok(())
}

/// Validates that a pattern can be expanded into an NFA, at bind time.
///
/// Three hazards, all of which otherwise survive planning: an inverted range (`{5,3}`) expands to an
/// empty optional tail and silently degrades to `{5}`; a large bound (or a product of nested bounds)
/// expands to an NFA that exhausts the compute node's memory; and `PERMUTE` grows factorially in its
/// arity. The expansion happens in `Nfa::compile`, which runs when the actor is built — after the DDL
/// has been committed in meta — so the only place these can be reported to the author is here.
///
/// The arity and per-bound checks run before the whole-pattern budget so that the specific cause is
/// named: a `PERMUTE` of 8 variables is over the budget too, but "PERMUTE supports at most 6
/// variables" is the useful thing to say about it.
fn validate_pattern(pattern: &MatchRecognizePattern) -> RwResult<()> {
    check_pattern_bounds(pattern)?;
    let states = estimate_nfa_states(pattern);
    if states > MAX_PATTERN_NFA_STATES {
        return Err(crate::error::ErrorCode::NotSupported(
            format!("a MATCH_RECOGNIZE pattern that expands to about {states} NFA states"),
            format!(
                "the pattern is expanded eagerly into at most {MAX_PATTERN_NFA_STATES} states; \
                 reduce the quantifier bounds, especially where quantifiers are nested"
            ),
        )
        .into());
    }
    Ok(())
}

/// Per-node bound checks: `PERMUTE` arity, `min <= max`, and each bound within
/// [`MAX_QUANTIFIER_BOUND`].
fn check_pattern_bounds(pattern: &MatchRecognizePattern) -> RwResult<()> {
    use risingwave_sqlparser::ast::RepetitionQuantifier as Q;

    match pattern {
        MatchRecognizePattern::Symbol(_) | MatchRecognizePattern::Exclude(_) => {}
        MatchRecognizePattern::Permute(symbols) => {
            reject_oversized_permute(symbols.len())?;
            // PERMUTE(a, a, b) would expand duplicate orderings into redundant NFA branches and
            // is almost certainly an authoring mistake — reject rather than silently accept.
            let mut seen = std::collections::HashSet::new();
            for s in symbols {
                if let risingwave_sqlparser::ast::MatchRecognizeSymbol::Named(ident) = s {
                    let name = ident.real_value();
                    if !seen.insert(name.clone()) {
                        return Err(crate::error::ErrorCode::InvalidInputSyntax(format!(
                            "duplicate variable `{name}` in PERMUTE"
                        ))
                        .into());
                    }
                }
            }
        }
        MatchRecognizePattern::Concat(patterns) | MatchRecognizePattern::Alternation(patterns) => {
            for p in patterns {
                check_pattern_bounds(p)?;
            }
        }
        MatchRecognizePattern::Group(inner) => check_pattern_bounds(inner)?,
        MatchRecognizePattern::Repetition(inner, quantifier, _) => {
            if let Q::Range(min, max) = quantifier
                && min > max
            {
                return Err(crate::error::ErrorCode::NotSupported(
                    format!("a range quantifier with a lower bound above its upper bound ({{{min},{max}}})"),
                    format!("use {{{max},{min}}} if the bounds were swapped, or {{{min}}} for exactly {min} repetitions"),
                )
                .into());
            }
            let bounds = match quantifier {
                Q::ZeroOrMore | Q::OneOrMore | Q::AtMostOne => vec![],
                Q::Exactly(n) | Q::AtLeast(n) => vec![*n],
                Q::AtMost(m) => vec![*m],
                Q::Range(n, m) => vec![*n, *m],
            };
            for b in bounds {
                if b > MAX_QUANTIFIER_BOUND {
                    return Err(crate::error::ErrorCode::NotSupported(
                        format!("a range quantifier bound of {b}"),
                        format!(
                            "a bound is expanded eagerly into up to 4 NFA states per repetition of \
                             a single variable (more for a larger inner pattern), so it may be at \
                             most {MAX_QUANTIFIER_BOUND}"
                        ),
                    )
                    .into());
                }
            }
            check_pattern_bounds(inner)?;
        }
    }
    Ok(())
}

/// Upper bound on the number of NFA states `Nfa::compile` will allocate for `pattern`.
///
/// Mirrors the construction in `nfa.rs` (`Nfa::build`): a variable is 2 states; an alternation adds a
/// start and an accept state; `*`, `?` and each optional copy of a range add 2; `+` builds the inner
/// pattern twice; `PERMUTE` of `n` variables becomes an alternation of `n!` concatenations. Saturating
/// throughout, so an over-large pattern reports a saturated estimate rather than wrapping.
///
/// **This is a model of code in another crate**, so it cannot be checked by a test: `risingwave_stream`
/// depends on `risingwave_frontend`'s protos, not the reverse, and nothing can observe both. If the
/// state count of any construct in `nfa.rs` changes, this must change with it, or the guard above
/// silently becomes an under-estimate. The construction sites in `nfa.rs` carry a comment pointing
/// back here.
fn estimate_nfa_states(pattern: &MatchRecognizePattern) -> u64 {
    use risingwave_sqlparser::ast::RepetitionQuantifier as Q;

    match pattern {
        // Anchors and exclusions are rejected when the pattern is lowered; 2 is the cost of the
        // variable form.
        MatchRecognizePattern::Symbol(_) | MatchRecognizePattern::Exclude(_) => 2,
        MatchRecognizePattern::Permute(symbols) => {
            let n = symbols.len() as u64;
            let orderings = (1..=n)
                .try_fold(1u64, |acc, i| acc.checked_mul(i))
                .unwrap_or(u64::MAX);
            orderings
                .saturating_mul(n.saturating_mul(2))
                .saturating_add(2)
        }
        MatchRecognizePattern::Concat(patterns) => patterns
            .iter()
            .map(estimate_nfa_states)
            .fold(0u64, u64::saturating_add)
            .max(1),
        MatchRecognizePattern::Alternation(patterns) => patterns
            .iter()
            .map(estimate_nfa_states)
            .fold(2u64, u64::saturating_add),
        MatchRecognizePattern::Group(inner) => estimate_nfa_states(inner),
        MatchRecognizePattern::Repetition(inner, quantifier, _) => {
            let inner = estimate_nfa_states(inner);
            match quantifier {
                Q::ZeroOrMore | Q::AtMostOne => inner.saturating_add(2),
                Q::OneOrMore => inner.saturating_mul(2).saturating_add(2),
                // `min` mandatory copies, then an unbounded `*` tail.
                Q::AtLeast(min) => inner
                    .saturating_mul(*min as u64)
                    .saturating_add(inner)
                    .saturating_add(2),
                // `min` mandatory copies, then `max - min` optional (`?`) copies.
                Q::Exactly(n) => inner.saturating_mul(*n as u64).max(1),
                Q::AtMost(max) => inner.saturating_add(2).saturating_mul(*max as u64).max(1),
                Q::Range(min, max) => inner
                    .saturating_mul(*min as u64)
                    .saturating_add(
                        inner
                            .saturating_add(2)
                            .saturating_mul(max.saturating_sub(*min) as u64),
                    )
                    .max(1),
            }
        }
    }
}

/// Name of the synthetic relation that backs a `DEFINE`'s navigation placeholders.
const NAV_TABLE: &str = "__mr_nav";

/// Extracts row-pattern navigation functions (`PREV`/`NEXT`/`FIRST`/`LAST`) from a `DEFINE`
/// predicate AST, replacing each with a synthetic placeholder column and recording the corresponding
/// [`DefineSlot`]. Functions are handled because they do not bind as ordinary scalar functions;
/// plain variable-qualified columns are left to bind normally and are mapped later.
struct NavExtractor<'a> {
    input_fields: &'a [Field],
    resolver: &'a VarResolver<'a>,
    /// The symbol whose `DEFINE` predicate is being lowered — the only variable qualifier a
    /// physical `PREV` may carry (see [`NavExtractor::physical_col`]).
    symbol: &'a str,
    /// Per-DEFINE prefix for the synthetic placeholder column names (kept unique across DEFINE items).
    prefix: &'a str,
    nav_slots: Vec<DefineSlot>,
    nav_fields: Vec<Field>,
}

impl NavExtractor<'_> {
    fn rewrite(&mut self, node: &mut AstExpr) -> RwResult<()> {
        if let AstExpr::Function(func) = node
            && func.name.0.len() == 1
            && matches!(
                func.name.0[0].real_value().to_ascii_lowercase().as_str(),
                "prev" | "next" | "first" | "last"
            )
        {
            let k = self.nav_slots.len();
            let slot = self.nav_slot(func)?;
            let data_type = self.input_fields[slot.col_idx].data_type();
            let col_name = format!("{}_{k}", self.prefix);
            self.nav_fields
                .push(Field::with_name(data_type, col_name.clone()));
            *node = AstExpr::Identifier(Ident::new_unchecked(col_name));
            self.nav_slots.push(slot);
            return Ok(());
        }
        // Every variant that carries sub-expressions is traversed. The match is deliberately
        // exhaustive (no `_` arm): a navigation call the traversal fails to reach is never extracted,
        // and binding then reports the misleading "function prev(integer) does not exist" instead of
        // anything about navigation. Making the compiler flag new `Expr` variants keeps that from
        // silently regressing.
        match node {
            AstExpr::BinaryOp { left, right, .. }
            | AstExpr::IsDistinctFrom(left, right)
            | AstExpr::IsNotDistinctFrom(left, right) => {
                self.rewrite(left)?;
                self.rewrite(right)?;
            }
            AstExpr::UnaryOp { expr, .. }
            | AstExpr::Nested(expr)
            | AstExpr::IsNull(expr)
            | AstExpr::IsNotNull(expr)
            | AstExpr::IsTrue(expr)
            | AstExpr::IsNotTrue(expr)
            | AstExpr::IsFalse(expr)
            | AstExpr::IsNotFalse(expr)
            | AstExpr::IsUnknown(expr)
            | AstExpr::IsNotUnknown(expr)
            | AstExpr::IsJson { expr, .. }
            | AstExpr::FieldIdentifier(expr, _)
            | AstExpr::SomeOp(expr)
            | AstExpr::AllOp(expr)
            | AstExpr::Extract { expr, .. }
            | AstExpr::Collate { expr, .. }
            | AstExpr::Cast { expr, .. }
            | AstExpr::TryCast { expr, .. } => self.rewrite(expr)?,
            AstExpr::Between {
                expr, low, high, ..
            } => {
                self.rewrite(expr)?;
                self.rewrite(low)?;
                self.rewrite(high)?;
            }
            AstExpr::InList { expr, list, .. } => {
                self.rewrite(expr)?;
                for e in list {
                    self.rewrite(e)?;
                }
            }
            AstExpr::Like { expr, pattern, .. }
            | AstExpr::ILike { expr, pattern, .. }
            | AstExpr::SimilarTo { expr, pattern, .. } => {
                self.rewrite(expr)?;
                self.rewrite(pattern)?;
            }
            AstExpr::AtTimeZone {
                timestamp,
                time_zone,
            } => {
                self.rewrite(timestamp)?;
                self.rewrite(time_zone)?;
            }
            AstExpr::Substring {
                expr,
                substring_from,
                substring_for,
            } => {
                self.rewrite(expr)?;
                for e in substring_from.iter_mut().chain(substring_for.iter_mut()) {
                    self.rewrite(e)?;
                }
            }
            AstExpr::Position { substring, string } => {
                self.rewrite(substring)?;
                self.rewrite(string)?;
            }
            AstExpr::Overlay {
                expr,
                new_substring,
                start,
                count,
            } => {
                self.rewrite(expr)?;
                self.rewrite(new_substring)?;
                self.rewrite(start)?;
                if let Some(e) = count {
                    self.rewrite(e)?;
                }
            }
            AstExpr::Trim {
                expr, trim_what, ..
            } => {
                self.rewrite(expr)?;
                if let Some(e) = trim_what {
                    self.rewrite(e)?;
                }
            }
            AstExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                for e in operand.iter_mut().chain(else_result.iter_mut()) {
                    self.rewrite(e)?;
                }
                for e in conditions.iter_mut().chain(results.iter_mut()) {
                    self.rewrite(e)?;
                }
            }
            AstExpr::GroupingSets(sets) | AstExpr::Cube(sets) | AstExpr::Rollup(sets) => {
                for set in sets {
                    for e in set {
                        self.rewrite(e)?;
                    }
                }
            }
            AstExpr::Row(exprs) => {
                for e in exprs {
                    self.rewrite(e)?;
                }
            }
            AstExpr::Array(array) => {
                for e in &mut array.elem {
                    self.rewrite(e)?;
                }
            }
            AstExpr::Index { obj, index } => {
                self.rewrite(obj)?;
                self.rewrite(index)?;
            }
            AstExpr::ArrayRangeIndex { obj, start, end } => {
                self.rewrite(obj)?;
                for e in start.iter_mut().chain(end.iter_mut()) {
                    self.rewrite(e)?;
                }
            }
            AstExpr::Map { entries } => {
                for (k, v) in entries {
                    self.rewrite(k)?;
                    self.rewrite(v)?;
                }
            }
            // A non-navigation call: only its arguments can contain navigation. The modifiers
            // (`FILTER`, `OVER`, `WITHIN GROUP`, the aggregate `ORDER BY`) are not traversed — they
            // only occur on aggregate and window calls, neither of which is supported in a DEFINE
            // predicate at all.
            AstExpr::Function(func) => {
                for arg in &mut func.arg_list.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(e),
                            ..
                        } => self.rewrite(e)?,
                        _ => {}
                    }
                }
            }
            // Leaves: nothing to traverse.
            AstExpr::Identifier(_)
            | AstExpr::CompoundIdentifier(_)
            | AstExpr::Value(_)
            | AstExpr::Parameter { .. }
            | AstExpr::TypedString { .. } => {}
            // `IN (SELECT ...)`: the left-hand operand is an ordinary expression outside the
            // subquery, so it is traversed; only the subquery itself is not.
            AstExpr::InSubquery { expr, .. } => self.rewrite(expr)?,
            // The remaining subquery-bearing forms carry nothing but a `Query`. Row-pattern
            // navigation is defined over the rows of the match, which a subquery cannot see, so
            // nothing inside one is extracted; a navigation call there is reported by ordinary
            // binding.
            AstExpr::Exists(_) | AstExpr::Subquery(_) | AstExpr::ArraySubquery(_) => {}
            // A lambda body is evaluated per element by the higher-order function that receives it,
            // not once per candidate row, so a navigation placeholder cannot be lifted out of it.
            AstExpr::LambdaFunction { .. } => {}
        }
        Ok(())
    }

    /// Builds the [`DefineSlot`] for a navigation function call.
    fn nav_slot(&self, func: &Function) -> RwResult<DefineSlot> {
        let name = func.name.0[0].real_value().to_ascii_lowercase();
        // Only the name and the argument list are read below, so any modifier would be dropped.
        reject_func_modifiers(func, "DEFINE")?;
        let args = &func.arg_list.args;
        let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))) = args.first() else {
            bail_not_implemented!(
                "{}() argument must be a column in DEFINE",
                name.to_uppercase()
            );
        };
        match name.as_str() {
            "prev" | "next" => {
                // Not `bail_not_implemented!`: this is not a feature gap, the call is simply wrong.
                if args.len() > 2 {
                    return Err(crate::error::ErrorCode::NotSupported(
                        format!(
                            "{}() with {} arguments in DEFINE",
                            name.to_uppercase(),
                            args.len()
                        ),
                        format!(
                            "{}() takes a column and an optional positive integer offset",
                            name.to_uppercase()
                        ),
                    )
                    .into());
                }
                // Physical NEXT in DEFINE is not in the v1 subset. A row's verdict would read rows
                // after it, so it is only final once that lookahead is watermark-safe — and a single
                // global "decision horizon" (defer everything by the max offset) can permanently
                // starve an idle partition whose match is in fact already decidable (the lookahead
                // row is inside the match). Correct support needs per-path decidability: an
                // evaluation that actually reads past the safe prefix must be a wait for exactly
                // that candidate, not a global delay and not a NULL verdict. Until that lands,
                // reject rather than expose either wrong behaviour.
                if name != "prev" {
                    bail_not_implemented!(
                        "physical NEXT() in MATCH_RECOGNIZE DEFINE (a row's verdict would depend \
                         on rows after it; per-candidate decidability is not implemented yet)"
                    );
                }
                let col_idx = self.physical_col(inner)?;
                let offset = match args.get(1) {
                    Some(arg) => self.parse_offset(arg, &name)?,
                    None => 1,
                };
                Ok(DefineSlot {
                    kind: DefineSlotKind::Prev,
                    vars: vec![],
                    col_idx,
                    offset,
                })
            }
            _ => {
                if args.len() != 1 {
                    bail_not_implemented!("{}() with an offset in DEFINE", name.to_uppercase());
                }
                let (vars, col_idx) = self.var_col(inner)?;
                let kind = if name == "first" {
                    DefineSlotKind::RunningFirst
                } else {
                    DefineSlotKind::RunningLast
                };
                Ok(DefineSlot {
                    kind,
                    vars,
                    col_idx,
                    offset: 0,
                })
            }
        }
    }

    /// Resolves a physical-navigation argument (`col` or `var.col`) to its input column index.
    ///
    /// A variable qualifier is accepted only when it is the symbol being defined: under the
    /// standard's running semantics `PREV(B.col)` inside `DEFINE B` reads from the row before the
    /// current candidate — exactly the physical previous row this engine navigates to. A qualifier
    /// naming ANOTHER variable anchors the read to that variable's last mapped row instead
    /// (`PREV(A.col)` ≡ `PREV(LAST(A.col, 0), 1)`), which is different semantics this engine does
    /// not implement — silently treating it as the physical previous row would answer a question
    /// the author did not ask. An undeclared qualifier is plain wrong SQL.
    fn physical_col(&self, expr: &AstExpr) -> RwResult<usize> {
        let col = match expr {
            AstExpr::Identifier(c) => c.real_value(),
            AstExpr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let var = parts[0].real_value();
                if !self.resolver.alias_names.iter().any(|n| n == &var) {
                    return Err(crate::error::ErrorCode::InvalidInputSyntax(format!(
                        "PREV/NEXT in DEFINE references unknown pattern variable `{var}`"
                    ))
                    .into());
                }
                if var != self.symbol {
                    bail_not_implemented!(
                        "PREV/NEXT anchored to another pattern variable's rows in DEFINE \
                         (`{}` inside the definition of `{}`); qualify with the symbol being \
                         defined, or leave the column unqualified, for physical navigation from \
                         the current row",
                        var,
                        self.symbol
                    );
                }
                parts[1].real_value()
            }
            _ => bail_not_implemented!("PREV/NEXT argument must be a column reference in DEFINE"),
        };
        self.col_idx(&col)
    }

    /// Resolves a logical-navigation argument (`var.col`) to its variable(s) and input column index.
    fn var_col(&self, expr: &AstExpr) -> RwResult<(Vec<String>, usize)> {
        let AstExpr::CompoundIdentifier(parts) = expr else {
            bail_not_implemented!(
                "FIRST/LAST argument must be a pattern-variable column in DEFINE"
            );
        };
        if parts.len() != 2 {
            bail_not_implemented!(
                "FIRST/LAST argument must be a pattern-variable column in DEFINE"
            );
        }
        let var = parts[0].real_value();
        if !self.resolver.alias_names.iter().any(|n| n == &var) {
            bail_not_implemented!(
                "FIRST/LAST references unknown pattern variable {} in DEFINE",
                var
            );
        }
        Ok((
            self.resolver.members_of(&var),
            self.col_idx(&parts[1].real_value())?,
        ))
    }

    fn col_idx(&self, name: &str) -> RwResult<usize> {
        // Zero, one and many matches are three different answers: with duplicate input column
        // names (`SELECT v AS x, v + 100 AS x ...`) silently taking the first physical field
        // would bind the navigation to an arbitrary column and change match results.
        let mut hits = self
            .input_fields
            .iter()
            .enumerate()
            .filter(|(_, f)| f.name == name);
        match (hits.next(), hits.next()) {
            (Some((i, _)), None) => Ok(i),
            (None, _) => bail_not_implemented!("navigation over unknown column {} in DEFINE", name),
            (Some(_), Some(_)) => Err(crate::error::ErrorCode::BindError(format!(
                "column reference \"{name}\" in MATCH_RECOGNIZE navigation is ambiguous: the \
                 input has more than one column with that name"
            ))
            .into()),
        }
    }

    fn parse_offset(&self, arg: &FunctionArg, name: &str) -> RwResult<usize> {
        let FunctionArg::Unnamed(FunctionArgExpr::Expr(AstExpr::Value(AstValue::Number(s)))) = arg
        else {
            return Err(Self::offset_not_a_positive_literal(name));
        };
        // A `Number` token can be any numeric literal, so distinguish "not a non-negative integer at
        // all" from "an integer that is simply too large": the latter must report the cap, not claim
        // the literal was not an integer.
        let is_integer_literal = !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
        match s.parse::<u64>() {
            // The offset is an operational knob, not just a wire-format concern: a `PREV` offset
            // demands that many mandatory rows before the variable in the pattern, so the cap is
            // deliberately small.
            Ok(n) if n > MAX_NAV_OFFSET as u64 => Err(Self::offset_above_cap(name, s)),
            // The offset must be positive: `PREV(col, 0)` / `NEXT(col, 0)` would resolve to the
            // current row, which is surprising for physical navigation and not what these mean.
            Ok(0) => Err(Self::offset_not_a_positive_literal(name)),
            Ok(n) => Ok(n as usize),
            // Out of `u64` range, but still a decimal integer literal: over the cap, by a lot.
            Err(_) if is_integer_literal => Err(Self::offset_above_cap(name, s)),
            Err(_) => Err(Self::offset_not_a_positive_literal(name)),
        }
    }

    fn offset_above_cap(name: &str, literal: &str) -> crate::error::RwError {
        crate::error::ErrorCode::NotSupported(
            format!("{}() offset of {}", name.to_uppercase(), literal),
            format!(
                "a PREV() offset requires that many mandatory pattern rows before the variable, \
                 so the offset may be at most {MAX_NAV_OFFSET}"
            ),
        )
        .into()
    }

    fn offset_not_a_positive_literal(name: &str) -> crate::error::RwError {
        crate::error::ErrorCode::NotSupported(
            format!(
                "a non-literal or non-positive {}() offset",
                name.to_uppercase()
            ),
            format!(
                "{}() offset must be a positive integer literal (>= 1)",
                name.to_uppercase()
            ),
        )
        .into()
    }
}

/// Maps each `InputRef` in a bound `DEFINE` predicate to a [`DefineSlot`]: navigation placeholders
/// (index `>= nav_base`) to their pre-resolved slot; variable-qualified columns to a self slot (the
/// defined variable, or an unqualified/raw-input reference) or a running slot (another variable).
struct DefineSlotRewriter<'a> {
    resolver: &'a VarResolver<'a>,
    defined_var: &'a str,
    nav_base: usize,
    nav_slots: &'a [DefineSlot],
    slots: Vec<DefineSlot>,
}

impl ExprRewriter for DefineSlotRewriter<'_> {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        let index = input_ref.index();
        let data_type = input_ref.data_type;
        let slot = if index >= self.nav_base {
            self.nav_slots[index - self.nav_base].clone()
        } else {
            let n = self.resolver.input_col_num;
            let col_idx = index % n;
            let block = index / n;
            let self_slot = DefineSlot {
                kind: DefineSlotKind::SelfCol,
                vars: vec![],
                col_idx,
                offset: 0,
            };
            if block == 0 {
                self_slot
            } else {
                let name = &self.resolver.alias_names[block - 1];
                if name == self.defined_var {
                    self_slot
                } else {
                    DefineSlot {
                        kind: DefineSlotKind::RunningLast,
                        vars: self.resolver.members_of(name),
                        col_idx,
                        offset: 0,
                    }
                }
            }
        };
        let idx = self
            .slots
            .iter()
            .position(|s| *s == slot)
            .unwrap_or_else(|| {
                self.slots.push(slot);
                self.slots.len() - 1
            });
        InputRef::new(idx, data_type).into()
    }
}

/// Collect the distinct pattern-variable names appearing in a pattern, unioned with the `DEFINE`
/// symbols. The union is defensive: `bind_match_recognize` rejects any `DEFINE` symbol absent from
/// the pattern before this runs, so the two sets are equal there.
fn collect_pattern_variables(
    pattern: &MatchRecognizePattern,
    symbols: &[SymbolDefinition],
) -> Vec<String> {
    let mut vars: BTreeSet<String> = BTreeSet::new();
    collect_from_pattern(pattern, &mut vars);
    for s in symbols {
        vars.insert(s.symbol.real_value());
    }
    vars.into_iter().collect()
}

/// Per pattern variable: the minimum number of rows a match has necessarily consumed before a row
/// can be labeled with that variable — its minimum distance from the match start.
///
/// This is what makes a physical `PREV(col, k)` in the variable's `DEFINE` safe without any
/// retention of pre-match rows: if the variable can only ever sit at distance `>= k` from the
/// match start, every `PREV` read lands inside the match span, and rows of a live match are never
/// evicted (the eviction walker keeps everything from the first live start onward). A read that
/// could reach *before* the match start would observe a retained row before eviction and `NULL`
/// after it — the same row flipping its verdict on timing — so those shapes are rejected at bind
/// time (see the check in [`Binder::bind_match_recognize`]).
///
/// The walk is exact for the supported constructs and conservative by construction elsewhere:
/// - concatenation shifts a variable's distance by the *minimum* length of everything before it
///   (a zero-minimum quantifier prefix contributes 0 — `(a* b)` leaves `b` at distance 0);
/// - alternation takes the minimum across branches;
/// - a quantified sub-pattern keeps its inner distances unshifted (the first iteration starts at
///   the node's start; later iterations only sit further from the match start);
/// - `PERMUTE` puts every element at distance 0 (any ordering may put it first).
///
/// A variable occurring several times keeps the smallest distance of any occurrence.
fn min_start_distances(pattern: &MatchRecognizePattern) -> HashMap<String, u64> {
    fn insert_min(map: &mut HashMap<String, u64>, var: String, dist: u64) {
        map.entry(var)
            .and_modify(|d| *d = (*d).min(dist))
            .or_insert(dist);
    }

    /// Returns the minimum number of rows `pattern` consumes, recording each contained variable's
    /// minimum start distance *relative to this node's start* into `map`.
    fn walk(pattern: &MatchRecognizePattern, map: &mut HashMap<String, u64>) -> u64 {
        use risingwave_sqlparser::ast::RepetitionQuantifier as Q;
        match pattern {
            MatchRecognizePattern::Symbol(MatchRecognizeSymbol::Named(ident))
            | MatchRecognizePattern::Exclude(MatchRecognizeSymbol::Named(ident)) => {
                insert_min(map, ident.real_value(), 0);
                1
            }
            // Unnamed symbols (anchors) are not in the v1 subset; count them as consuming no rows,
            // which can only *shrink* distances — conservative for this check.
            MatchRecognizePattern::Symbol(_) | MatchRecognizePattern::Exclude(_) => 0,
            MatchRecognizePattern::Permute(symbols) => {
                for s in symbols {
                    if let MatchRecognizeSymbol::Named(ident) = s {
                        // Any element can be ordered first.
                        insert_min(map, ident.real_value(), 0);
                    }
                }
                symbols.len() as u64
            }
            MatchRecognizePattern::Concat(patterns) => {
                let mut prefix = 0u64;
                for p in patterns {
                    let mut inner = HashMap::new();
                    let len = walk(p, &mut inner);
                    for (v, d) in inner {
                        insert_min(map, v, d.saturating_add(prefix));
                    }
                    prefix = prefix.saturating_add(len);
                }
                prefix
            }
            MatchRecognizePattern::Alternation(patterns) => {
                let mut min_len = u64::MAX;
                for p in patterns {
                    min_len = min_len.min(walk(p, map));
                }
                if patterns.is_empty() { 0 } else { min_len }
            }
            MatchRecognizePattern::Group(inner) => walk(inner, map),
            MatchRecognizePattern::Repetition(inner, quantifier, _) => {
                let len = walk(inner, map);
                let min_reps: u64 = match quantifier {
                    Q::ZeroOrMore | Q::AtMostOne | Q::AtMost(_) => 0,
                    Q::OneOrMore => 1,
                    Q::Exactly(n) | Q::AtLeast(n) | Q::Range(n, _) => u64::from(*n),
                };
                len.saturating_mul(min_reps)
            }
        }
    }

    let mut map = HashMap::new();
    walk(pattern, &mut map);
    map
}

fn collect_from_pattern(pattern: &MatchRecognizePattern, out: &mut BTreeSet<String>) {
    match pattern {
        MatchRecognizePattern::Symbol(MatchRecognizeSymbol::Named(ident))
        | MatchRecognizePattern::Exclude(MatchRecognizeSymbol::Named(ident)) => {
            out.insert(ident.real_value());
        }
        MatchRecognizePattern::Symbol(_) | MatchRecognizePattern::Exclude(_) => {}
        MatchRecognizePattern::Permute(symbols) => {
            for s in symbols {
                if let MatchRecognizeSymbol::Named(ident) = s {
                    out.insert(ident.real_value());
                }
            }
        }
        MatchRecognizePattern::Concat(patterns) | MatchRecognizePattern::Alternation(patterns) => {
            for p in patterns {
                collect_from_pattern(p, out);
            }
        }
        MatchRecognizePattern::Group(inner) => collect_from_pattern(inner, out),
        MatchRecognizePattern::Repetition(inner, _, _) => collect_from_pattern(inner, out),
    }
}

/// Decodes a measure `InputRef` back to the pattern variable(s) and input column it references.
/// Pattern variables and `SUBSET` names are each registered as an alias block of width
/// `input_col_num` after the input columns, in `alias_names` order; so block 0 is the raw input (an
/// unqualified reference, unsupported) and block `k + 1` is `alias_names[k]`. A `SUBSET` name
/// resolves to its member variables; a plain variable resolves to itself.
struct VarResolver<'a> {
    input_col_num: usize,
    alias_names: &'a [String],
    subset_defs: &'a [(String, Vec<String>)],
}

impl VarResolver<'_> {
    fn resolve(&self, index: usize) -> RwResult<(Vec<String>, usize)> {
        if index / self.input_col_num == 0 {
            bail_not_implemented!(
                "unqualified or non-pattern-variable column reference in MATCH_RECOGNIZE MEASURES"
            );
        }
        Ok(self.resolve_unchecked(index))
    }

    /// As [`VarResolver::resolve`] but assumes a pattern-variable-qualified reference (block >= 1),
    /// which [`InputRefBlockCheck`] guarantees before lowering.
    fn resolve_unchecked(&self, index: usize) -> (Vec<String>, usize) {
        let block = index / self.input_col_num;
        let name = self
            .alias_names
            .get(block - 1)
            .expect("alias block within range of registered variables/subsets");
        let vars = self.members_of(name);
        (vars, index % self.input_col_num)
    }

    /// The variables a name resolves to: a `SUBSET`'s members, or the variable itself.
    fn members_of(&self, name: &str) -> Vec<String> {
        self.subset_defs
            .iter()
            .find(|(n, _)| n == name)
            .map_or_else(|| vec![name.to_owned()], |(_, members)| members.clone())
    }
}

/// Checks that every measure `InputRef` is pattern-variable-qualified (alias block >= 1). A
/// reference into block 0 is the raw input — an unqualified or table-qualified column with no
/// pattern-variable navigation meaning.
struct InputRefBlockCheck {
    input_col_num: usize,
    /// First index past the variable/subset alias blocks; anything at or above it is internal
    /// binder scaffolding (`__mr_nav` placeholders) that user SQL must not address.
    nav_floor: usize,
    unqualified: bool,
    internal: bool,
}

impl ExprVisitor for InputRefBlockCheck {
    fn visit_input_ref(&mut self, input_ref: &InputRef) {
        if input_ref.index() < self.input_col_num {
            self.unqualified = true;
        }
        if input_ref.index() >= self.nav_floor {
            self.internal = true;
        }
    }
}

/// Rewrites each pattern-variable-qualified `InputRef` in a measure expression to an `InputRef` into
/// the synthetic per-match row, recording a deduplicated `LAST(var.col)` slot for it.
struct SlotLoweringRewriter<'a, 'b> {
    resolver: &'a VarResolver<'b>,
    slots: Vec<MeasureSlot>,
}

impl ExprRewriter for SlotLoweringRewriter<'_, '_> {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        let (vars, col_idx) = self.resolver.resolve_unchecked(input_ref.index());
        let data_type = input_ref.data_type;
        let slot_idx = self
            .slots
            .iter()
            .position(|s| s.kind == MeasureSlotKind::Last && s.vars == vars && s.col_idx == col_idx)
            .unwrap_or_else(|| {
                self.slots.push(MeasureSlot {
                    kind: MeasureSlotKind::Last,
                    vars,
                    col_idx,
                    data_type: data_type.clone(),
                    agg: None,
                });
                self.slots.len() - 1
            });
        InputRef::new(slot_idx, data_type).into()
    }
}

/// Lower a `WITHIN` bound into the two expressions the executor consumes: the span predicate over a
/// synthetic `[last_order_key, first_order_key]` row, and the per-row deadline over a synthetic
/// `[first_order_key]` row.
///
/// Extracted so their relationship is testable. The executor treats the deadline as interchangeable
/// with the right-hand side of the span predicate — it is what lets a hot-path span check reuse the
/// deadline already cached per row instead of evaluating an expression — and that interchangeability
/// holds only because BOTH are built here from one `first + bound`, and because the check below
/// forces that sum to keep the order key's type. The
/// `within_predicate_right_hand_side_is_the_deadline` test pins the first property; the
/// `within_bound_that_promotes_the_order_key_type_is_rejected` test pins the second.
///
/// Lowering the predicate as `(last - first) <= bound` looks equivalent and is not, for
/// calendar-varying intervals: timestamp subtraction yields a months-free interval compared under
/// 30-day normalization, while `first + INTERVAL '1 month'` is calendar addition. For starts in
/// short months the deadline would then close BEFORE the span window, prematurely finalizing and
/// evicting live partials.
///
/// The sum can still leave the order key's range at runtime (a `smallint` key at `32766` with
/// `WITHIN 2::smallint`). That is not a bind-time concern: every representable order key lies inside
/// such a span, so the executor reads an out-of-range sum as a window that never closes
/// (`Deadline::Never` in the stream crate) rather than as a NULL that the span check would reject.
fn lower_within(ts_type: DataType, bound: ExprImpl) -> crate::error::Result<(ExprImpl, ExprImpl)> {
    let last = ExprImpl::from(InputRef::new(0, ts_type.clone()));
    let first = ExprImpl::from(InputRef::new(1, ts_type.clone()));
    let first_plus_bound = ExprImpl::from(FunctionCall::new(
        ExprType::Add,
        vec![first, bound.clone()],
    )?);
    // `Add` goes through generic type inference, so the sum can be WIDER than the order key: an
    // `int2` key with a bare `2` (which binds as `int4`) yields an `int4` deadline, and a `date` key
    // with an interval bound yields `timestamp`. The executor compares the cached deadline against
    // the order key and against the watermark directly, and `ScalarRefImpl::default_cmp` panics on
    // mismatched variants — an actor panic as soon as rows flow, and a crash loop once recovery
    // replays the same rows. The span predicate alone would survive this (its `FunctionCall::new`
    // inserts an implicit cast on `last`), but the deadline consumers have no such protection.
    //
    // Rejected rather than cast back down: truncating `timestamp -> date` would close the window
    // early and prematurely finalize and evict live partials — the same calendar-correctness trap
    // described above.
    let sum_type = first_plus_bound.return_type();
    if sum_type != ts_type {
        return Err(crate::error::ErrorCode::NotSupported(
            format!(
                "a MATCH_RECOGNIZE WITHIN bound whose addition widens the ORDER BY type \
                 ({ts_type} + bound yields {sum_type})"
            ),
            // Deliberately does not offer `WITHIN <bound>::{ts_type}` unconditionally: for the
            // motivating `date` + interval case there is no such cast (`interval::date` is not a
            // valid cast), and suggesting it sends the reader down a dead end. Widening within one
            // numeric family is castable; crossing families is not.
            if sum_type.is_numeric() && ts_type.is_numeric() {
                format!(
                    "the bound must keep the ORDER BY column's type, since it is also used as a \
                     per-row deadline compared against that column and the watermark — cast the \
                     bound, e.g. `WITHIN <bound>::{ts_type}`"
                )
            } else {
                format!(
                    "the bound must keep the ORDER BY column's type, since it is also used as a \
                     per-row deadline compared against that column and the watermark — use an \
                     ORDER BY column whose type absorbs the bound (a timestamp or timestamptz \
                     column for an interval bound, rather than {ts_type})"
                )
            },
        )
        .into());
    }
    let predicate = ExprImpl::from(FunctionCall::new(
        ExprType::LessThanOrEqual,
        vec![last, first_plus_bound],
    )?);
    // The deadline is the same `first + bound`, but over a one-column synthetic row, so `first` is
    // `InputRef(0)` here rather than `InputRef(1)`.
    let first_only = ExprImpl::from(InputRef::new(0, ts_type.clone()));
    let deadline = ExprImpl::from(FunctionCall::new(ExprType::Add, vec![first_only, bound])?);
    // The check above is on the predicate's right-hand side; the DEADLINE is the expression the
    // executor actually evaluates and compares, so assert it directly rather than inferring that
    // identical operand types must infer identically.
    debug_assert_eq!(
        deadline.return_type(),
        ts_type,
        "the WITHIN deadline must keep the ORDER BY type; it is compared against the order key \
         and the watermark by `default_cmp`, which panics across variants"
    );
    Ok((predicate, deadline))
}

#[cfg(test)]
mod tests {
    use risingwave_sqlparser::ast::RepetitionQuantifier as Q;

    use super::*;

    fn var(name: &str) -> MatchRecognizePattern {
        MatchRecognizePattern::Symbol(MatchRecognizeSymbol::Named(Ident::new_unchecked(name)))
    }

    fn rep(inner: MatchRecognizePattern, q: Q) -> MatchRecognizePattern {
        MatchRecognizePattern::Repetition(Box::new(inner), q, false)
    }

    /// The estimate must mirror `Nfa::build`: 2 states per variable, 2 more per optional copy.
    #[test]
    fn nfa_state_estimate_matches_the_expansion() {
        assert_eq!(estimate_nfa_states(&var("a")), 2);
        assert_eq!(estimate_nfa_states(&rep(var("a"), Q::Exactly(1000))), 2000);
        // 3 mandatory copies (2 each) plus 7 optional ones (2 + 2 each).
        assert_eq!(estimate_nfa_states(&rep(var("a"), Q::Range(3, 10))), 34);
        // `min` mandatory copies plus a `*` tail (inner + 2).
        assert_eq!(estimate_nfa_states(&rep(var("a"), Q::AtLeast(3))), 10);
        assert_eq!(estimate_nfa_states(&rep(var("a"), Q::ZeroOrMore)), 4);
        assert_eq!(estimate_nfa_states(&rep(var("a"), Q::OneOrMore)), 6);
        // `PERMUTE` over the maximum 6 variables: 6! orderings of 6 variables, plus the alternation's
        // own start and accept states. Quoted in `MAX_PATTERN_NFA_STATES`.
        let permute6 = MatchRecognizePattern::Permute(
            ["a", "b", "c", "d", "e", "f"]
                .iter()
                .map(|n| MatchRecognizeSymbol::Named(Ident::new_unchecked(*n)))
                .collect(),
        );
        assert_eq!(estimate_nfa_states(&permute6), 8642);
        assert!(estimate_nfa_states(&permute6) < MAX_PATTERN_NFA_STATES);
    }

    /// The minimum-start-distance walk backing the physical-`PREV` rule: exact for the supported
    /// constructs, and the zero-minimum-prefix / alternation / `PERMUTE` corners each pin the case
    /// that would make the rule unsound if gotten wrong.
    #[test]
    fn min_start_distances_cover_the_pattern_constructs() {
        let concat = |ps: Vec<MatchRecognizePattern>| MatchRecognizePattern::Concat(ps);
        let d = |p: &MatchRecognizePattern, v: &str| min_start_distances(p).get(v).copied();

        // Concatenation shifts by the preceding minimum length.
        let ab = concat(vec![var("a"), var("b")]);
        assert_eq!(d(&ab, "a"), Some(0));
        assert_eq!(d(&ab, "b"), Some(1));
        // A variable absent from the pattern has no distance.
        assert_eq!(d(&ab, "x"), None);

        // A zero-minimum quantifier prefix contributes nothing: `(a* b)` leaves `b` at 0.
        let a_star_b = concat(vec![rep(var("a"), Q::ZeroOrMore), var("b")]);
        assert_eq!(d(&a_star_b, "b"), Some(0));
        // ...while a one-minimum prefix contributes its single row: `(a+ b)` puts `b` at 1.
        let a_plus_b = concat(vec![rep(var("a"), Q::OneOrMore), var("b")]);
        assert_eq!(d(&a_plus_b, "b"), Some(1));
        // `{n,...}` prefixes contribute `n` rows.
        let a3_b = concat(vec![rep(var("a"), Q::AtLeast(3)), var("b")]);
        assert_eq!(d(&a3_b, "b"), Some(3));

        // Inside a quantified node the first iteration starts at the node's start: `b+` itself
        // leaves `b` at 0, even though later iterations sit further away.
        assert_eq!(d(&rep(var("b"), Q::OneOrMore), "b"), Some(0));

        // Alternation takes the minimum branch length as a prefix: `s (a | b c) t`.
        let alt = concat(vec![
            var("s"),
            MatchRecognizePattern::Alternation(vec![var("a"), concat(vec![var("b"), var("c")])]),
            var("t"),
        ]);
        assert_eq!(d(&alt, "a"), Some(1));
        assert_eq!(d(&alt, "c"), Some(2));
        // `t` follows the alternation's *minimum* (1 row via the `a` branch).
        assert_eq!(d(&alt, "t"), Some(2));

        // PERMUTE: any element can be ordered first.
        let permute = MatchRecognizePattern::Permute(vec![
            MatchRecognizeSymbol::Named(Ident::new_unchecked("a")),
            MatchRecognizeSymbol::Named(Ident::new_unchecked("b")),
        ]);
        assert_eq!(d(&permute, "b"), Some(0));
        // ...and a PERMUTE consumes all its elements as a prefix.
        let permute_t = concat(vec![permute, var("t")]);
        assert_eq!(d(&permute_t, "t"), Some(2));

        // A variable occurring twice keeps its smallest distance.
        let twice = concat(vec![var("a"), var("b"), var("a")]);
        assert_eq!(d(&twice, "a"), Some(0));
    }

    /// A bound whose addition PROMOTES the order-key type must be rejected at bind time.
    ///
    /// `smallint` order key with `WITHIN 2`: the literal binds as `int4`, so `first + bound` is
    /// `int4` while the order key and the watermark stay `int2`. Nothing downstream reconciles them,
    /// and the executor compares the deadline against both raw — `ScalarRefImpl::default_cmp`
    /// panics on mismatched variants, which is an actor panic as soon as rows flow and a crash loop
    /// after recovery replays them. `date` key with an interval bound promotes to `timestamp` the
    /// same way.
    ///
    /// Rejecting rather than casting the deadline back down is deliberate: truncating
    /// `timestamp -> date` closes the window early and prematurely evicts live partials, which is
    /// the same calendar-correctness trap documented on `lower_within`.
    #[test]
    fn within_bound_that_promotes_the_order_key_type_is_rejected() {
        let err = lower_within(DataType::Int16, ExprImpl::literal_int(2)).expect_err(
            "int4 bound over an int2 order key promotes the deadline and must not bind",
        );
        let msg = err.to_string();
        assert!(
            msg.contains("smallint") && msg.contains("integer"),
            "the error should name both types so the cast is obvious, got: {msg}"
        );

        // The matching-type case still binds.
        lower_within(DataType::Int32, ExprImpl::literal_int(2))
            .expect("an int4 bound over an int4 order key keeps the type");

        // And the documented way through — casting the bound to the order key's type — must work,
        // since `match_recognize_within.slt` tells users to do exactly that.
        let int2_bound = ExprImpl::from(Literal::new(Some(ScalarImpl::Int16(2)), DataType::Int16));
        lower_within(DataType::Int16, int2_bound)
            .expect("`WITHIN 2::smallint` over an int2 order key must keep the type");
    }

    /// The executor's hot-path span check reuses the per-row deadline instead of evaluating the
    /// span predicate, which is sound only while the deadline IS the predicate's right-hand side.
    /// That relationship is established here, by building both from one `first + bound`, and is
    /// relied on in `DefineMatcher::matches` — so pin it: a future change that lowers the predicate
    /// differently (`(last - first) <= bound`, say, which is wrong for calendar intervals) would
    /// silently change which matches the operator produces, and this fails instead.
    #[test]
    fn within_predicate_right_hand_side_is_the_deadline() {
        let bound = ExprImpl::literal_int(5);
        let (predicate, deadline) = lower_within(DataType::Int32, bound).unwrap();

        let ExprImpl::FunctionCall(pred) = &predicate else {
            panic!("the span predicate must be a function call, got {predicate:?}");
        };
        assert_eq!(pred.func_type(), ExprType::LessThanOrEqual);
        let [last, rhs] = pred.inputs() else {
            panic!("the span predicate must be binary");
        };
        assert_eq!(
            last,
            &ExprImpl::from(InputRef::new(0, DataType::Int32)),
            "`last` is column 0 of the synthetic [last, first] row"
        );

        // The two differ only in where `first` is read from: column 1 of the two-column span row
        // versus column 0 of the one-column deadline row. The operator and the bound must be
        // identical, because that is exactly what makes them interchangeable.
        let ExprImpl::FunctionCall(rhs) = rhs else {
            panic!("the predicate's right-hand side must be `first + bound`, got {rhs:?}");
        };
        let ExprImpl::FunctionCall(dl) = &deadline else {
            panic!("the deadline must be a function call, got {deadline:?}");
        };
        assert_eq!(
            rhs.func_type(),
            ExprType::Add,
            "span rhs must be an addition"
        );
        assert_eq!(
            dl.func_type(),
            ExprType::Add,
            "deadline must be an addition"
        );

        let [span_first, span_bound] = rhs.inputs() else {
            panic!("span rhs must be binary");
        };
        let [dl_first, dl_bound] = dl.inputs() else {
            panic!("deadline must be binary");
        };
        assert_eq!(
            span_first,
            &ExprImpl::from(InputRef::new(1, DataType::Int32)),
            "`first` is column 1 of the two-column span row"
        );
        assert_eq!(
            dl_first,
            &ExprImpl::from(InputRef::new(0, DataType::Int32)),
            "`first` is column 0 of the one-column deadline row"
        );
        assert_eq!(
            span_bound, dl_bound,
            "both must carry the SAME bound expression; if they diverge, the executor's hot-path \
             span check (which reuses the cached deadline) stops agreeing with the span predicate"
        );
    }

    /// Nesting multiplies, so per-quantifier bounds within [`MAX_QUANTIFIER_BOUND`] are not enough.
    #[test]
    fn nested_quantifiers_within_the_per_bound_cap_are_still_rejected() {
        let nested = rep(
            MatchRecognizePattern::Group(Box::new(rep(var("a"), Q::Exactly(900)))),
            Q::Exactly(900),
        );
        assert!(check_pattern_bounds(&nested).is_ok());
        assert!(validate_pattern(&nested).is_err());
    }

    #[test]
    fn inverted_and_oversized_bounds_are_rejected() {
        assert!(validate_pattern(&rep(var("a"), Q::Range(5, 3))).is_err());
        assert!(validate_pattern(&rep(var("a"), Q::Range(3, 5))).is_ok());
        assert!(validate_pattern(&rep(var("a"), Q::Exactly(MAX_QUANTIFIER_BOUND))).is_ok());
        assert!(validate_pattern(&rep(var("a"), Q::Exactly(MAX_QUANTIFIER_BOUND + 1))).is_err());
        assert!(validate_pattern(&rep(var("a"), Q::AtMost(u32::MAX))).is_err());
    }

    fn permute(n: usize) -> MatchRecognizePattern {
        MatchRecognizePattern::Permute(
            (0..n)
                .map(|i| {
                    MatchRecognizeSymbol::Named(Ident::new_unchecked(format!("v{i}").as_str()))
                })
                .collect(),
        )
    }

    /// An oversized `PERMUTE` is over the whole-pattern budget as well, so the arity check has to run
    /// first or the budget's quantifier-shaped message shadows the precise one.
    #[test]
    fn oversized_permute_reports_its_arity_not_the_state_budget() {
        // 7 variables is over the arity cap but *under* the state budget: the arity check is the only
        // thing that rejects it.
        assert_eq!(estimate_nfa_states(&permute(7)), 70562);
        assert!(estimate_nfa_states(&permute(7)) < MAX_PATTERN_NFA_STATES);
        let seven = validate_pattern(&permute(7)).unwrap_err().to_string();
        assert!(
            seven.contains("PERMUTE supports at most 6 variables"),
            "{seven}"
        );

        // 8 variables is over both; the arity message must still be the one reported.
        assert!(estimate_nfa_states(&permute(8)) > MAX_PATTERN_NFA_STATES);
        let eight = validate_pattern(&permute(8)).unwrap_err().to_string();
        assert!(
            eight.contains("PERMUTE supports at most 6 variables"),
            "{eight}"
        );
        assert!(!eight.contains("NFA states"), "{eight}");

        assert!(validate_pattern(&permute(MAX_PERMUTE_VARS)).is_ok());
    }
}
