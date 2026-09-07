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

//! Streaming `MATCH_RECOGNIZE` executor over **ordered input**.
//!
//! Scope: append-only input, `ONE ROW PER MATCH`,
//! `AFTER MATCH SKIP {PAST LAST ROW | TO NEXT ROW | TO {FIRST | LAST} <var>}`,
//! `MEASURES` with per-variable navigation (`FIRST`/`LAST`/bare `var.col`) and `CLASSIFIER()`.
//!
//! The ordered-input model: an upstream `WatermarkSort` (same fragment) delivers rows already in
//! full `ORDER BY` order, strictly below each watermark it then forwards, with partitions
//! interleaved. This executor therefore owns **only NFA state and match finalization**: each
//! partition keeps an [`IncrementalMatcher`] fed on arrival, plus the retained rows its live
//! matches still reference. There is no out-of-order buffer here, and consumed history is never
//! rescanned; the matcher does re-scan the *unfrozen suffix* on arrival, so per-row work is
//! proportional to the rows a still-open partial holds live, not to the new rows alone.
//!
//! Emission is on decidability, gated by [`match_is_final`]: the first provisional match is
//! emitted once no gap position before it is alive at the boundary (an alive gap could still
//! yield an earlier, leftmost-preferred match), and no more-preferred path from its own start can
//! be completed by future rows ([`Nfa::may_extend`] probed at the buffer boundary), or its
//! `WITHIN` deadline has strictly passed the watermark, which decides both questions at once.
//! Consumed rows are deleted; rows are retained only while a live partial or held match references
//! them. `AFTER MATCH SKIP TO FIRST|LAST` degradations and scan-budget exhaustion are *reported*,
//! never silent and never actor-fatal — see [`report_skip_degradation_once`] and
//! [`report_scan_budget_once`].
//!
//! State: one table of the retained rows, keyed `(partition..., order..., seq)`. Recovery rebuilds
//! each partition's matcher by re-feeding its retained rows in key order — deterministic, so
//! replayed emission (the hidden `_match_id` is the match's start-row `seq`) is byte-identical.
//! That guarantee is scoped to recovery and rescale, for matches anchored at committed rows:
//! a match whose start row was uncommitted at a crash re-mints that row's seq on replay (as
//! generated ids do system-wide), and `seq` freezes each tie's *arrival* order, so re-creating
//! the MV or replaying the topic may interleave equal-ORDER-BY rows differently and legitimately
//! produce different matches (the standard leaves tie order implementation-defined).

use std::collections::HashMap;

use futures::{StreamExt, pin_mut};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{OwnedRow, Row, RowExt, once};
use risingwave_common::types::{DataType, Datum, DefaultOrd, ScalarImpl, ToOwnedDatum};
use risingwave_expr::ExprError;
use risingwave_expr::aggregate::{AggCall, BoxedAggregateFunction, build_append_only};
use risingwave_expr::expr::{EvalErrorReport, NonStrictExpression, build_non_strict_from_prost};
use risingwave_pb::stream_plan::{
    MatchRecognizeDefine as PbMatchRecognizeDefine,
    MatchRecognizeMeasure as PbMatchRecognizeMeasure,
};
use risingwave_storage::StateStore;

use super::incremental::{Finalized, IncrementalMatcher, Seq};
use super::nfa::{CandidateMatcher, Nfa, ScanBudget, SkipDegradation, SkipMode};
use crate::common::table::state_table::StateTable;
use crate::executor::monitor::MatchRecognizeMetrics;
use crate::executor::prelude::*;
use crate::task::ActorEvalErrorReport;

/// Report an `AFTER MATCH SKIP` degradation ([`SkipDegradation`]) — unless the same degradation was
/// already reported in this watermark pass, in which case it is dropped.
///
/// The condition is data-dependent and deliberately not fatal (see [`SkipMode::next_pos`] for why an
/// error would turn a committed materialized view into a crash loop), so the only thing left is to
/// make it visible. It goes to the actor's [`EvalErrorReport`], which is the surface every expression
/// evaluation error in this operator already uses: the rate-limited `stream_expr_error` log and the
/// `user_compute_error` metric, labelled `["ExprError", executor_name, fragment_id]`.
///
/// **The carrier is new, the surface is not.** Nothing else in the tree hands `EvalErrorReport` a
/// *synthesized* error — every other reporter passes on an error produced by an actual expression
/// evaluation. [`ExprError`] is nonetheless the only type the trait accepts, and
/// `ExprError::InvalidParam` is the honest fit: the query's `AFTER MATCH SKIP` parameter cannot be
/// honored. (`ExprError::Custom` was rejected — it is the UDF error channel and slated for removal;
/// `Internal`/`InvalidState` would misreport a user-query problem as an engine fault.) Two
/// consequences to expect when reading the output:
///
///  * the log line carries the surface's fixed prefix `failed to evaluate expression`, hardcoded in
///    `ActorContext::on_compute_error`, even though no expression was evaluated here. The actionable
///    content is the `error=` field, which is self-contained;
///  * the metric labels separate this operator from others, but not this operator's own
///    `DEFINE`/`MEASURES`/`WITHIN` evaluation errors, which report through the same labels. So the
///    metric reads as "this `MATCH_RECOGNIZE` query is unhealthy" and the log line is what says why.
///
/// **Volume policy.** The cause is a property of the query, not of one row: a skip target that no
/// match can ever bind degrades on every match, forever, and even a target that only *sometimes*
/// fails to bind (`PATTERN (a? b)` with `SKIP TO FIRST b`, degrading on the matches where `a` did not
/// bind) repeats without bound. The diagnostic names the skip clause, its target variable and the
/// applied fallback — and nothing row-, match- or partition-specific — so every repetition within one
/// watermark pass is a byte-identical duplicate carrying no new information, at the cost of a
/// `format!` on the emit path. `already_reported` therefore holds the kinds already reported in this
/// pass (at most two exist, and `Vec::new()` allocates only if one actually fires); it is reset per
/// pass, so a persisting condition keeps producing one report per kind per watermark — a steady
/// signal, bounded by watermark frequency rather than by match or partition count. The trade-off is
/// deliberate: the metric counts *passes* that degraded, not degradations.
fn report_skip_degradation_once(
    report: &impl EvalErrorReport,
    skip: &SkipMode,
    degradation: SkipDegradation,
    already_reported: &mut Vec<SkipDegradation>,
) {
    if already_reported.contains(&degradation) {
        return;
    }
    already_reported.push(degradation);
    // The mode is named once, by `clause_name`; `describe` names the target variable and the fallback.
    report.report(ExprError::InvalidParam {
        name: skip.clause_name(),
        reason: degradation.describe(skip).into(),
    });
}

/// Walk steps — predicate evaluations plus recursion descents, ε-transitions included — one
/// partition visit may spend across all its NFA walks (matching, eviction liveness, extension
/// probing) before the visit degrades; see [`ScanBudget`]. The
/// matcher's worst case is exponential for pathological patterns whose `DEFINE`s read the running
/// label assignment (path-independent patterns are memoized and never approach this); the budget
/// converts that from a pinned compute node into a bounded, reported degradation. Sized so that
/// realistic patterns stay orders of magnitude below it: a memoized scan costs
/// O(states × rows) per start.
const SCAN_BUDGET_EVALUATIONS: usize = 1 << 20;

/// Report a spent [`ScanBudget`] — once per message pass (the cause is a property of the query
/// and its buffered data, so per-visit repeats add volume, not information).
fn report_scan_budget_once(report: &impl EvalErrorReport, already_reported: &mut bool) {
    if *already_reported {
        return;
    }
    *already_reported = true;
    report.report(ExprError::InvalidParam {
        name: "MATCH_RECOGNIZE",
        reason: format!(
            "pattern-match scan budget ({SCAN_BUDGET_EVALUATIONS} predicate evaluations) \
             exhausted while processing one partition; the partition is left undecided for this \
             visit (nothing emitted or evicted beyond what was already decided) and will be \
             retried. This indicates a pattern with catastrophic backtracking over the buffered \
             data — simplify nested optional/alternation quantifiers, or add/tighten WITHIN"
        )
        .into(),
    });
}

/// How a [`MeasureSlot`] resolves against the rows of a match: the wire enum, used directly (the
/// variants are documented in `stream_plan.proto`) — a parallel executor-side enum was one more
/// thing to keep in lockstep with the planner for no representational gain. `Unspecified` is
/// rejected in [`CompiledMeasure::from_protobuf`], so no constructed slot carries it.
use risingwave_pb::stream_plan::match_recognize_measure_slot::Kind as MeasureSlotKind;

/// A `SUM`/`AVG` aggregate kernel for a slot, plus the input column type used to feed it.
struct AggSlot {
    func: BoxedAggregateFunction,
    col_type: DataType,
}

/// One navigation input that a measure expression reads. The executor materializes one value per
/// slot from a match's rows and labels, forming the synthetic row the measure is evaluated over.
struct MeasureSlot {
    kind: MeasureSlotKind,
    /// Pattern variables this slot navigates over (several for a `SUBSET`). A row matches if its
    /// label is any of these. Empty for [`MeasureSlotKind::Classifier`].
    vars: Vec<String>,
    /// Input column index to read. Unused for [`MeasureSlotKind::Classifier`].
    col_idx: usize,
    /// The aggregate kernel for [`MeasureSlotKind::Sum`] (`AVG` is lowered to `Sum` plus `Count`).
    agg: Option<AggSlot>,
}

/// A `MEASURES` item compiled for execution.
pub struct CompiledMeasure {
    /// Expression over the synthetic per-match row: `InputRef(i)` reads `slots[i]`.
    expr: NonStrictExpression,
    slots: Vec<MeasureSlot>,
}

impl CompiledMeasure {
    /// Builds a compiled measure from its protobuf, building any aggregate kernels its slots need.
    pub fn from_protobuf(
        pb: &PbMatchRecognizeMeasure,
        error_report: impl EvalErrorReport + 'static,
    ) -> StreamExecutorResult<Self> {
        let expr = build_non_strict_from_prost(
            pb.expr
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("MATCH_RECOGNIZE measure missing expression"))?,
            error_report,
        )?;
        let slots = pb
            .slots
            .iter()
            .map(|s| {
                let kind = s.kind();
                // Fail fast rather than silently changing measure semantics under a corrupt plan
                // or version skew. (An out-of-range wire value decodes as UNSPECIFIED via
                // `s.kind()`.) Every later match on the kind relies on this rejection.
                if kind == MeasureSlotKind::Unspecified {
                    return Err(anyhow::anyhow!(
                        "invalid MATCH_RECOGNIZE measure slot kind: {}",
                        s.kind
                    )
                    .into());
                }
                let agg = match kind {
                    MeasureSlotKind::Sum => {
                        let call =
                            AggCall::from_protobuf(s.agg_call.as_ref().ok_or_else(|| {
                                anyhow::anyhow!(
                                    "MATCH_RECOGNIZE SUM/AVG measure slot missing agg_call"
                                )
                            })?)?;
                        let col_type = call.args.arg_types()[0].clone();
                        let func = build_append_only(&call)?;
                        Some(AggSlot { func, col_type })
                    }
                    _ => None,
                };
                Ok(MeasureSlot {
                    kind,
                    vars: s.vars.clone(),
                    col_idx: s.col_idx as usize,
                    agg,
                })
            })
            .collect::<StreamExecutorResult<Vec<_>>>()?;
        Ok(CompiledMeasure { expr, slots })
    }
}

impl MeasureSlot {
    /// Resolves this slot against a match: `rows[start..]` are the matched rows and `labels[i]` is
    /// the pattern variable bound to `rows[start + i]`.
    async fn resolve(
        &self,
        rows: &[BufferedRow],
        start: usize,
        labels: &[String],
        error_report: &impl risingwave_expr::expr::EvalErrorReport,
    ) -> StreamExecutorResult<Datum> {
        // The column value of the row at match-relative index `j`.
        let col_at = |j: usize| rows[start + j].row.datum_at(self.col_idx).to_owned_datum();
        // Whether a row's label is one this slot navigates over (a plain var, or any SUBSET member).
        let matches = |l: &String| self.vars.iter().any(|v| v == l);
        Ok(match self.kind {
            MeasureSlotKind::Classifier => {
                labels.last().map(|s| ScalarImpl::Utf8(s.as_str().into()))
            }
            MeasureSlotKind::First => labels.iter().position(&matches).and_then(col_at),
            MeasureSlotKind::Last => labels.iter().rposition(&matches).and_then(col_at),
            MeasureSlotKind::CountStar => Some(ScalarImpl::Int64(labels.len() as i64)),
            MeasureSlotKind::Count => {
                // Compare by reference: owning every candidate datum just to count non-nulls
                // would clone each one.
                let n = labels
                    .iter()
                    .enumerate()
                    .filter(|(j, l)| {
                        matches(l) && rows[start + *j].row.datum_at(self.col_idx).is_some()
                    })
                    .count();
                Some(ScalarImpl::Int64(n as i64))
            }
            MeasureSlotKind::Min => labels
                .iter()
                .enumerate()
                .filter(|(_, l)| matches(l))
                .filter_map(|(j, _)| rows[start + j].row.datum_at(self.col_idx))
                .min_by(|a, b| a.default_cmp(b))
                .map(|r| r.into_scalar_impl()),
            MeasureSlotKind::Max => labels
                .iter()
                .enumerate()
                .filter(|(_, l)| matches(l))
                .filter_map(|(j, _)| rows[start + j].row.datum_at(self.col_idx))
                .max_by(|a, b| a.default_cmp(b))
                .map(|r| r.into_scalar_impl()),
            // Rejected in `from_protobuf`; a constructed slot never carries it. NULL (the
            // non-strict convention) rather than a panic, should that invariant ever break.
            MeasureSlotKind::Unspecified => None,
            MeasureSlotKind::Sum => {
                let agg = self.agg.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("MATCH_RECOGNIZE SUM measure slot has no kernel")
                })?;
                // Feed the kernel a single-column chunk of the col values over the matching rows.
                let input: Vec<(Op, OwnedRow)> = labels
                    .iter()
                    .enumerate()
                    .filter(|(_, l)| matches(l))
                    .map(|(j, _)| (Op::Insert, OwnedRow::new(vec![col_at(j)])))
                    .collect();
                if input.is_empty() {
                    None
                } else {
                    let chunk = StreamChunk::from_rows(&input, std::slice::from_ref(&agg.col_type));
                    // A kernel error here is a DATA error — numeric overflow in SUM is the
                    // canonical one — on rows that recovery will replay verbatim: propagating it
                    // kills the actor and every restart replays the same rows into the same
                    // overflow, an unrecoverable crash loop from one bad match. Mirror what
                    // `NonStrictExpression` does for every other expression in this operator:
                    // report through the actor's error report and yield NULL for the measure.
                    let evaluated = async {
                        let mut state = agg.func.create_state()?;
                        agg.func.update(&mut state, &chunk).await?;
                        agg.func.get_result(&state).await
                    }
                    .await;
                    match evaluated {
                        Ok(d) => d,
                        Err(e) => {
                            error_report.report(e);
                            None
                        }
                    }
                }
            }
        })
    }
}

/// How a [`DefineSlot`] resolves against the candidate row: the wire enum, used directly (see
/// [`MeasureSlotKind`] for the rationale). `Unspecified` and physical `Next` — which the binder
/// rejects in `DEFINE` — are rejected in [`CompiledDefine::from_protobuf`], so no constructed
/// slot carries them.
use risingwave_pb::stream_plan::match_recognize_define_slot::Kind as DefineSlotKind;

/// One input a `DEFINE` predicate reads (mirrors the planner's [`DefineSlot`]).
struct DefineSlot {
    kind: DefineSlotKind,
    vars: Vec<String>,
    col_idx: usize,
    offset: usize,
}

/// A `DEFINE` predicate compiled for execution: a boolean condition over a synthetic slot row.
pub struct CompiledDefine {
    symbol: String,
    condition: NonStrictExpression,
    slots: Vec<DefineSlot>,
}

impl CompiledDefine {
    pub fn from_protobuf(
        pb: &PbMatchRecognizeDefine,
        error_report: impl EvalErrorReport + 'static,
    ) -> StreamExecutorResult<Self> {
        let condition = build_non_strict_from_prost(
            pb.condition
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("MATCH_RECOGNIZE define missing condition"))?,
            error_report,
        )?;
        let slots = pb
            .slots
            .iter()
            .map(|s| {
                let kind = s.kind();
                // The binder rejects physical NEXT in DEFINE (a verdict depending on rows after
                // the candidate needs per-candidate decidability), so no plan this frontend
                // produces carries it — reject rather than evaluate a watermark-unsafe,
                // arrival-order-dependent read from a skewed plan. UNSPECIFIED (also what an
                // out-of-range wire value decodes to) fails fast rather than silently changing
                // the predicate's meaning. Every later match on the kind relies on this.
                if kind == DefineSlotKind::Next {
                    return Err(StreamExecutorError::from(anyhow::anyhow!(
                        "physical NEXT in a MATCH_RECOGNIZE DEFINE is not supported"
                    )));
                }
                if kind == DefineSlotKind::Unspecified {
                    return Err(StreamExecutorError::from(anyhow::anyhow!(
                        "invalid MATCH_RECOGNIZE define slot kind: {}",
                        s.kind
                    )));
                }
                Ok(DefineSlot {
                    kind,
                    vars: s.vars.clone(),
                    col_idx: s.col_idx as usize,
                    offset: s.offset as usize,
                })
            })
            .collect::<StreamExecutorResult<Vec<_>>>()?;
        Ok(CompiledDefine {
            symbol: pb.symbol.clone(),
            condition,
            slots,
        })
    }
}

/// Evaluates `DEFINE` predicates against the in-progress match, driving the NFA. Holds the
/// retained rows of one partition and the compiled `DEFINE`s; a variable with no `DEFINE` is
/// universally true.
struct DefineMatcher<'a> {
    rows: &'a [BufferedRow],
    defines: &'a HashMap<String, CompiledDefine>,
    /// `WITHIN` span predicate over `[last_order_key, first_order_key]`. Applied as a candidate is
    /// bound so the NFA prunes any extension that would push the match's span past the bound,
    /// yielding the longest match that fits the window rather than rejecting an overshooting greedy
    /// match after the fact.
    within: Option<&'a NonStrictExpression>,
}

impl DefineMatcher<'_> {
    /// The value a slot reads for a candidate at `pos` being tested for pattern variable `var`, where
    /// `match_start` is the match's first row and `labels[k]` is the variable bound to
    /// `rows[match_start + k]`.
    ///
    /// `labels` covers only the rows *already* bound, so for running navigation the candidate is the
    /// implicit trailing label: while its membership is still tentative, the running set a `DEFINE`
    /// predicate sees is `labels ++ [var]`. It therefore participates in `RunningFirst`/`RunningLast`
    /// whenever the slot's variable set contains `var` — including via a `SUBSET` that has `var` as a
    /// member. This is what makes `DEFINE a AS LAST(a.v) = a.v` a tautology, as SQL:2016 requires: a
    /// pattern-variable-qualified column reference *is* `RUNNING LAST` of that column, and the binder
    /// already lowers the bare `a.v` inside `a`'s own `DEFINE` to the candidate row.
    fn slot_value(
        &self,
        slot: &DefineSlot,
        var: &str,
        pos: usize,
        match_start: usize,
        labels: &[String],
    ) -> Datum {
        let col_at = |i: usize| self.rows[i].row.datum_at(slot.col_idx).to_owned_datum();
        let in_var = |l: &str| slot.vars.iter().any(|v| v == l);
        // Whether the candidate row itself belongs to the set this slot navigates over.
        let candidate_in_var = in_var(var);
        match slot.kind {
            DefineSlotKind::SelfCol => col_at(pos),
            DefineSlotKind::Prev => pos.checked_sub(slot.offset).and_then(col_at),
            // The candidate is the running first only when no earlier row of the match is in the set.
            DefineSlotKind::RunningFirst => labels
                .iter()
                .position(|l| in_var(l))
                .map(|k| match_start + k)
                .or_else(|| candidate_in_var.then_some(pos))
                .and_then(col_at),
            // The candidate is the newest row, so it is the running last whenever it is in the set.
            DefineSlotKind::RunningLast => candidate_in_var
                .then_some(pos)
                .or_else(|| {
                    labels
                        .iter()
                        .rposition(|l| in_var(l))
                        .map(|k| match_start + k)
                })
                .and_then(col_at),
            // Both rejected in `from_protobuf`; a constructed slot never carries them. NULL (the
            // non-strict convention) rather than a panic, should that invariant ever break.
            DefineSlotKind::Next | DefineSlotKind::Unspecified => None,
        }
    }
}

impl CandidateMatcher for DefineMatcher<'_> {
    async fn matches(
        &self,
        var: &str,
        pos: usize,
        labels: &[String],
    ) -> StreamExecutorResult<bool> {
        let match_start = pos - labels.len();
        // A pattern variable with no DEFINE matches every row; one with a DEFINE must satisfy it.
        if let Some(def) = self.defines.get(var) {
            let synthetic: Vec<Datum> = def
                .slots
                .iter()
                .map(|slot| self.slot_value(slot, var, pos, match_start, labels))
                .collect();
            let value = def
                .condition
                .eval_row_infallible(&OwnedRow::new(synthetic))
                .await;
            if !value.is_some_and(|s| s.into_bool()) {
                return Ok(false);
            }
        }
        // WITHIN: binding `pos` extends the match to span `[match_start, pos]`. Reject the candidate
        // if that span exceeds the bound, so the NFA backtracks to a shorter match that fits.
        //
        // One comparison, not an expression call. The span predicate is `last <= first + bound` and
        // `BufferedRow::deadline` is that same `first + bound`, already evaluated once per row at
        // ingest — the binder builds both from one expression precisely so its two WITHIN consumers
        // agree on every input, including calendar intervals, and `lower_within`'s
        // `within_predicate_right_hand_side_is_the_deadline` test pins that. Reusing it here removes
        // an allocation, two `Datum` clones and a boxed expression walk from the hottest path in the
        // operator: this runs once per predicate evaluation, up to the whole scan budget per visit,
        // and for a pattern variable with no `DEFINE` it was the entire cost of an evaluation.
        //
        // A deadline past the order key type's range admits every row ([`Deadline::Never`]; see
        // [`eval_deadline`]). `order_key` is never NULL for a buffered row — those are dropped at
        // ingest.
        if self.within.is_some() {
            let fits = match &self.rows[pos].order_key {
                Some(last) => self.rows[match_start].deadline.admits(last),
                None => false,
            };
            if !fits {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

pub struct MatchRecognizeExecutorArgs<S: StateStore> {
    pub ctx: ActorContextRef,
    pub input: Executor,
    /// Output schema: the `PARTITION BY` columns followed by the `MEASURES` columns.
    pub schema: Schema,
    pub chunk_size: usize,
    pub partition_key_indices: Vec<usize>,
    pub order_key_indices: Vec<usize>,
    pub measures: Vec<CompiledMeasure>,
    pub defines: Vec<CompiledDefine>,
    /// `WITHIN` span check over `[last_order_key, first_order_key]`; rejects matches that exceed it.
    pub within: Option<NonStrictExpression>,
    /// `WITHIN` deadline `first_order_key + interval` over a synthetic `[first_order_key]` row; the
    /// watermark at which a partial starting at that row expires. Used to wake idle partitions to
    /// evict timed-out partials. `None` when there is no `WITHIN`.
    ///
    /// Non-strict like every other expression here, but built over a [`DeadlineErrorReport`]: a sum
    /// that leaves the order key's range is a meaningful outcome ([`Deadline::Never`]), not a
    /// failure to report. See [`eval_deadline`].
    pub within_deadline: Option<NonStrictExpression>,
    pub nfa: Nfa,
    pub skip: SkipMode,
    /// Where the actor's compute-error reports go. The compiled `DEFINE`/`MEASURES`/`WITHIN`
    /// expressions already report evaluation errors through it; the executor itself uses it for
    /// `AFTER MATCH SKIP` degradations (see [`report_skip_degradation_once`]).
    pub eval_error_report: ActorEvalErrorReport,
    pub state_table: StateTable<S>,
}

pub struct MatchRecognizeExecutor<S: StateStore> {
    ctx: ActorContextRef,
    input: Executor,
    schema: Schema,
    chunk_size: usize,
    partition_key_indices: Vec<usize>,
    /// Input column index of the leading ORDER BY column (the watermark column). The full ORDER BY
    /// is encoded in the state-table key, so the buffer scans back already ordered; the executor
    /// only needs the leading column here, to find the safe prefix against the watermark.
    time_col: usize,
    measures: Vec<CompiledMeasure>,
    /// Compiled `DEFINE` predicates keyed by their pattern variable.
    defines: HashMap<String, CompiledDefine>,
    within: Option<NonStrictExpression>,
    /// `WITHIN` deadline expr (see [`MatchRecognizeExecutorArgs`]); consulted on every watermark
    /// pass — which visits every partition, so an idle partition's timed-out partial is emitted or
    /// evicted without new input in that partition.
    within_deadline: Option<NonStrictExpression>,
    nfa: Nfa,
    skip: SkipMode,
    /// Where `AFTER MATCH SKIP` degradations are reported (see [`MatchRecognizeExecutorArgs`]).
    eval_error_report: ActorEvalErrorReport,
    state_table: StateTable<S>,
}

/// When a row's `WITHIN` window closes: the watermark at which a partial match starting at that row
/// expires, and the latest order key a row may carry and still extend such a match.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Deadline {
    /// `first + bound`, in the order key's type (`lower_within` guarantees that), so it compares
    /// directly against order keys and watermarks.
    At(ScalarImpl),
    /// The window never closes. Without a `WITHIN` clause that is simply the semantics; with one,
    /// it is what `first + bound` denotes when the sum lies past the order key type's range: every
    /// representable order key is `<= first + bound`, so every candidate row is inside the span,
    /// and no representable watermark can pass the deadline. Folding the overflow into NULL
    /// instead — what non-strict evaluation did — rejected every such match (a `smallint` key at
    /// `32766` with `WITHIN 2::smallint` could not match its own next row) while leaving the
    /// partial unevictable.
    ///
    /// The reading rests on the addition being monotone in the bound, so that "out of range" can
    /// only mean "past the maximum": the binder guarantees a positive bound with, for intervals,
    /// no negative component (`timestamp + interval` adds months, days and microseconds as
    /// separate checked steps, and a mixed-sign interval could overflow on one while its true sum
    /// is representable).
    Never,
}

// `BufferedRow` is the operator's whole retained state; the enum must not cost more than the
// `Datum` it replaced (the `ScalarImpl` niche carries the second variant for free).
const _: () = assert!(std::mem::size_of::<Deadline>() == std::mem::size_of::<Datum>());

impl Deadline {
    /// The finality test: the window has closed at watermark `w`. Strict, because a row with
    /// `order_key == w` may still arrive and would still fall inside the inclusive span bound.
    fn closed_at(&self, w: &ScalarImpl) -> bool {
        match self {
            Deadline::At(d) => d.default_cmp(w).is_lt(),
            Deadline::Never => false,
        }
    }

    /// The span test: a match starting at this row may extend to a row with order key `last`.
    /// This is the lowered span predicate `last <= first + bound`, read off the cached deadline.
    fn admits(&self, last: &ScalarImpl) -> bool {
        match self {
            Deadline::At(d) => last.default_cmp(d).is_le(),
            Deadline::Never => true,
        }
    }
}

/// The error report the `WITHIN` deadline expression is built over: the sum leaving the order key
/// type's range is not a failure but the window that never closes ([`Deadline::Never`]), so it is
/// dropped here instead of being counted and logged as a compute error on every affected row. Every
/// other error still reaches the actor's report.
///
/// The expression is `first + bound` with `bound` a positive constant of the order key's own type
/// (`lower_within` enforces both), so out-of-range is the one error it can raise; anything else
/// would mean the expression is no longer the one the binder emits, and deserves the report.
#[derive(Clone)]
pub struct DeadlineErrorReport<R> {
    inner: R,
}

impl<R: EvalErrorReport> DeadlineErrorReport<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<R: EvalErrorReport> EvalErrorReport for DeadlineErrorReport<R> {
    fn report(&self, error: ExprError) {
        if !is_out_of_range(&error) {
            self.inner.report(error);
        }
    }
}

/// Whether `error` is, or wraps, an out-of-range arithmetic error. A generated function
/// implementation does not return its function's error bare: it wraps it in
/// [`ExprError::Function`] with the call rendered for display (`add('32766', '2')`), so the variant
/// has to be found through that wrapper.
fn is_out_of_range(error: &ExprError) -> bool {
    match error {
        // Not `NumericUnderflow`: the bound is positive at bind time, so the sum can only leave
        // the range upwards. An underflow would mean the expression is not the one the binder
        // emits, and must be reported rather than read as a window that never closes.
        ExprError::NumericOutOfRange | ExprError::NumericOverflow => true,
        ExprError::Function { source, .. } => source
            .downcast_ref::<ExprError>()
            .is_some_and(is_out_of_range),
        _ => false,
    }
}

/// Per-row WITHIN-deadline evaluation, run once when a row enters the buffer; every later
/// consultation reads [`BufferedRow::deadline`].
///
/// A NULL result can only be an evaluation error padded to NULL: a NULL sum needs a NULL operand,
/// and the order key is non-null for every buffered row (NULLs are dropped at ingest) while a NULL
/// bound is rejected at bind time. And the one reachable error is the sum leaving the order key
/// type's range (see [`DeadlineErrorReport`]) — the window that never closes. An unexpected error
/// has already been reported by the wrapper and is read the same way, since the alternative —
/// rejecting every match from that row — is the silent data loss this exists to prevent.
async fn eval_deadline(
    within_deadline: &Option<NonStrictExpression>,
    order_key: &Datum,
) -> Deadline {
    let Some(expr) = within_deadline else {
        return Deadline::Never;
    };
    let synthetic = OwnedRow::new(vec![order_key.clone()]);
    match expr.eval_row_infallible(&synthetic).await {
        Some(deadline) => Deadline::At(deadline),
        None => Deadline::Never,
    }
}

/// A buffered input row, materialized from the state table while processing one partition.
struct BufferedRow {
    /// Per-actor monotonic id; the state-table key tiebreaker (keeps rows with equal ORDER BY keys
    /// distinct and stably ordered).
    seq: i64,
    /// Leading ORDER BY value (a copy of `row[time_col]`), compared against the watermark to find
    /// the safe prefix. The buffer arrives pre-sorted by the full ORDER BY key (state-table PK).
    order_key: Datum,
    /// Precomputed `WITHIN` deadline (`order_key + bound`). A pure function of the row, consulted
    /// on every finality test and every prune pass — evaluating the expression per consultation
    /// would put a boxed expression call on each of those paths for what is one comparison.
    deadline: Deadline,
    /// The raw input row, read by DEFINE and MEASURES navigation slots at match time.
    row: OwnedRow,
}

impl<S: StateStore> MatchRecognizeExecutor<S> {
    pub fn new(args: MatchRecognizeExecutorArgs<S>) -> Self {
        let time_col = args.order_key_indices[0];
        let defines = args
            .defines
            .into_iter()
            .map(|d| (d.symbol.clone(), d))
            .collect();
        Self {
            ctx: args.ctx,
            input: args.input,
            schema: args.schema,
            chunk_size: args.chunk_size,
            partition_key_indices: args.partition_key_indices,
            time_col,
            measures: args.measures,
            defines,
            within: args.within,
            within_deadline: args.within_deadline,
            nfa: args.nfa,
            skip: args.skip,
            eval_error_report: args.eval_error_report,
            state_table: args.state_table,
        }
    }

    /// Emit every match the current state has decided, in scan order, mirroring the batch
    /// executor's guard: a match is final when a fed row follows it, or — ending exactly at the
    /// fed boundary — when its accepting path is terminal ([`Nfa::may_extend`] false) or, given a
    /// watermark, its `WITHIN` deadline has strictly passed. Emitting a match consumes everything
    /// up to its skip-resume position — including any earlier still-live partial, which can no
    /// longer produce a non-overlapping match before the emitted one (the same abandonment the
    /// batch scan performs). Returns the chunks that filled while appending.
    #[allow(clippy::too_many_arguments)]
    async fn emit_ready(
        run: &mut PartitionRun,
        partition_key: &OwnedRow,
        nfa: &Nfa,
        skip: &SkipMode,
        defines: &HashMap<String, CompiledDefine>,
        within: Option<&NonStrictExpression>,
        measures: &[CompiledMeasure],
        watermark: Option<&ScalarImpl>,
        state_table: &mut StateTable<S>,
        builder: &mut StreamChunkBuilder,
        eval_error_report: &ActorEvalErrorReport,
        reported_degradations: &mut Vec<SkipDegradation>,
        budget: &mut ScanBudget,
        memoizable: bool,
        statically_terminal: bool,
        metrics: &MatchRecognizeMetrics,
    ) -> StreamExecutorResult<Vec<StreamChunk>> {
        let mut out = Vec::new();
        // With no watermark there is no `within_final`, so a spent budget can decide nothing at all
        // on this path — bail before the per-match seq lookup below rather than walking the whole
        // buffer once per arriving row only to break. (The data path always passes `None`; only the
        // watermark path can reach the drain.)
        if budget.hit && watermark.is_none() {
            return Ok(out);
        }
        // Only the `Copy` identity fields before the gate: cloning the whole match (its label vector
        // in particular) on every ATTEMPT would copy it once per visit for a held match; the clone
        // happens below, after the gate passes.
        while let Some((start_seq, labels_len)) = run
            .matcher
            .provisional()
            .first()
            .map(|m| (m.start_seq, m.labels.len()))
        {
            // `end_seq` is a synthetic exclusive bound (last row's seq + 1), not a real row's
            // seq; the span length is the label count (one label per matched row). The scan runs
            // from 0, NOT from the matcher's resume position: `provisional()` leads with FROZEN
            // but not-yet-emitted matches, whose starts sit before the resume position (it points
            // past the LAST frozen match).
            let resume_pos = run.matcher.resume_pos().min(run.rows.len());
            // The gap check below walks `[resume_pos, start)`; positions the freeze already proved
            // dead (`dead_prefix_end`, monotone under appends) need no walk, so start it past them.
            let gap_from = resume_pos.max(run.matcher.dead_prefix_end());
            // `seq` is strictly increasing in buffer position — rows are appended in mint order and
            // the recovery rebuild re-feeds them in key order — the same invariant the dead-prefix
            // prune already binary-searches on.
            let Ok(start) = run.rows.binary_search_by_key(&start_seq.0, |r| r.seq) else {
                // A provisional match referencing an unfed seq is a matcher-invariant violation.
                // Fail loud: breaking here instead would re-hit the same match on every visit —
                // the partition would silently never emit or evict again while its state grows.
                return Err(anyhow::anyhow!(
                    "provisional match references seq {:?} not present in the row buffer",
                    start_seq
                )
                .into());
            };
            let end = start + labels_len;
            debug_assert!(end <= run.rows.len());

            let within_final = if let Some(w) = watermark {
                run.rows[start].deadline.closed_at(w)
            } else {
                false
            };
            // A spent budget cannot decide a STRUCTURAL hold: every walk short-circuits to
            // "undecided", which the gate must read as hold. A WITHIN-final match is different —
            // its window has closed, so `match_is_final` returns FINAL for it before spending
            // anything, and it needs no walk at all. Those MUST still be drained: leaving one
            // withheld while `prune_dead_prefix` treats its window-closed start row as dead is how
            // a starved visit loses a match outright. It is also the ONLY way a starved partition
            // sheds anything — `prune_dead_prefix` returns early while the matcher is incomplete —
            // though only about one match per visit: emitting a provisional match rebuilds the
            // matcher under the same spent budget, which empties the tail and ends this loop. That
            // is an improvement on shedding nothing, not convergence; see the design doc.
            //
            // (Nothing between the loop head and here spends budget: the provisional read, the seq
            // lookup and the deadline comparison are all plain reads.)
            if budget.hit && !within_final {
                break;
            }
            // Short-circuit a match the gate already held under identical state: only the
            // watermark-dependent WITHIN test can change the answer.
            if run.held == Some((start_seq, resume_pos, run.rows.len())) && !within_final {
                break;
            }
            let final_now = {
                let matcher = DefineMatcher {
                    rows: &run.rows,
                    defines,
                    within,
                };
                match_is_final(
                    nfa,
                    &matcher,
                    gap_from,
                    start,
                    run.rows.len(),
                    within_final,
                    statically_terminal,
                    budget,
                    memoizable,
                )
                .await?
            };
            if !final_now {
                if !budget.hit {
                    run.held = Some((start_seq, resume_pos, run.rows.len()));
                }
                break;
            }
            let m = run
                .matcher
                .provisional()
                .first()
                .cloned()
                .expect("checked non-empty above; nothing mutated the matcher since");

            // Evaluate each measure over the synthetic row its slots produce from the matched rows
            // and labels. WITHIN is enforced inside the matcher.
            let mut measure_datums: Vec<Datum> = Vec::with_capacity(measures.len());
            for measure in measures {
                let mut synthetic = Vec::with_capacity(measure.slots.len());
                for slot in &measure.slots {
                    synthetic.push(
                        slot.resolve(&run.rows, start, &m.labels, eval_error_report)
                            .await?,
                    );
                }
                let synthetic = OwnedRow::new(synthetic);
                measure_datums.push(measure.expr.eval_row_infallible(&synthetic).await);
            }
            // The match's identity is its start row's `seq`: deterministic across recovery replay,
            // and unique forever — consumption alone does not guarantee that (consumed rows are
            // deleted, so a naively re-seeded counter could re-mint their seqs); the epoch floor
            // on the seq counter (see the seeding comment in `execute_inner`) is what makes reuse
            // impossible.
            let match_id = run.rows[start].seq;
            let measures_row = OwnedRow::new(measure_datums);
            metrics.match_recognize_matches_emitted_count.inc();
            if let Some(c) = builder.append_row(
                Op::Insert,
                partition_key
                    .chain(&measures_row)
                    .chain(once(Some(ScalarImpl::Int64(match_id)))),
            ) {
                out.push(c);
            }

            // Where the scan resumes. A variable-targeted skip whose target row does not exist in
            // this match degrades to a weaker strategy instead of failing the actor; reported, not
            // silent.
            let (resume, degradation) = skip.next_pos(start, end, &m.labels);
            if let Some(degradation) = degradation {
                report_skip_degradation_once(
                    eval_error_report,
                    skip,
                    degradation,
                    reported_degradations,
                );
            }
            Self::consume_prefix(
                run,
                resume,
                defines,
                within,
                state_table,
                budget,
                memoizable,
            )
            .await?;
        }
        Ok(out)
    }

    /// Drop the dead prefix at a watermark: rows before the first position that is still a live
    /// match start — structurally alive at the fed boundary AND, under `WITHIN`, its window still
    /// open (`deadline >= w`, the strict complement of the finality test) — can never join a match
    /// again. On a spent budget everything undecided is retained.
    #[allow(clippy::too_many_arguments)]
    async fn prune_dead_prefix(
        run: &mut PartitionRun,
        nfa: &Nfa,
        defines: &HashMap<String, CompiledDefine>,
        within: Option<&NonStrictExpression>,
        w: &ScalarImpl,
        state_table: &mut StateTable<S>,
        budget: &mut ScanBudget,
        memoizable: bool,
    ) -> StreamExecutorResult<()> {
        // A budget-truncated provisional tail means absence-of-a-match is NOT evidence: the
        // WITHIN-deadline skip below treats window-closed rows as dead on the argument that
        // `emit_ready` already drained every within-final match — which only holds for a COMPLETE
        // tail. The executor re-derives (fresh budget) before this pass; if even that was
        // truncated, retain everything and let the next visit retry.
        if run.matcher.is_incomplete() {
            return Ok(());
        }
        let n = run.rows.len();
        // Positions the matcher's freeze walks already proved dead at the boundary (monotone under
        // appends; see `IncrementalMatcher::dead_prefix_end`) need no walk here. Buffer positions
        // and fed positions coincide (`consume_prefix` keeps them aligned).
        let proven_dead = run.matcher.dead_prefix_end().min(n);
        let mut retain_from = n;
        for p in 0..n {
            // Window closed (deadline < w): `p` is dead, skip it. A window that never closes (no
            // WITHIN, or a deadline past the type's range) fails this test, so `p` is retained.
            if run.rows[p].deadline.closed_at(w) {
                continue;
            }
            if p < proven_dead {
                continue;
            }
            let matcher = DefineMatcher {
                rows: &run.rows,
                defines,
                within,
            };
            let alive = nfa
                .reaches_boundary_alive(p, n, &matcher, budget, memoizable)
                .await?;
            if budget.hit || alive {
                // Spent budget: `p` is undecided — retain it all rather than fabricate "dead".
                retain_from = p;
                break;
            }
        }
        // Never consume the start row of a match the matcher still holds.
        //
        // This is NOT about a spent budget — do not gate it on `budget.hit`. With the budget spent
        // this function has already returned above, and even reaching here the loop stops at the
        // first window-open position, which is at or before any held match's start. The case it
        // guards has budget to spare: the loop skips a window-closed row on `deadline < w` WITHOUT
        // consulting the matcher, while the emission gate holds a match because a *gap* position
        // before its start is still alive at the boundary. The held match's own start row can then
        // be dead at the boundary (a path from it accepts before the buffer end), so the loop marches
        // straight past it and `consume_prefix` deletes the row of a match that was never emitted —
        // losing it, and emitting in its place a match a batch evaluation never produces.
        //
        // `provisional()` is ordered by start position and includes frozen-but-unemitted matches, so
        // its first entry is a lower bound on every undrained match. Skipped entirely when nothing
        // would be consumed, since the scan below is O(n) and cannot change a zero.
        if retain_from > 0
            && let Some(first) = run.matcher.provisional().first()
            // `seq` is strictly increasing in buffer position: rows are appended in mint order, and
            // the recovery rebuild re-feeds them in `(partition.., order.., seq)` key order, which
            // under the ordered-input model is the same order.
            && let Ok(pos) = run.rows.binary_search_by_key(&first.start_seq.0, |r| r.seq)
        {
            retain_from = retain_from.min(pos);
        }
        Self::consume_prefix(
            run,
            retain_from,
            defines,
            within,
            state_table,
            budget,
            memoizable,
        )
        .await
    }

    /// Consume `rows[..upto]`: delete them from the state table, drain the window, and bring the
    /// matcher along — rebasing in place where its finalize contract allows, rebuilding it from the
    /// survivors otherwise (the straddle shapes).
    #[allow(clippy::too_many_arguments)]
    async fn consume_prefix(
        run: &mut PartitionRun,
        upto: usize,
        defines: &HashMap<String, CompiledDefine>,
        within: Option<&NonStrictExpression>,
        state_table: &mut StateTable<S>,
        budget: &mut ScanBudget,
        memoizable: bool,
    ) -> StreamExecutorResult<()> {
        if upto == 0 {
            return Ok(());
        }
        // Invalidate the emission-gate cache: its key is `(start_seq, resume_pos, rows.len())`,
        // which identifies gate state only while `rows` is immutable — and this function rebases
        // the buffer, so both the position and the length can return to a value they held under a
        // DIFFERENT set of rows. A stale hit then skips the gate for a match the gate would now
        // decide FINAL, withholding it until the next row arrives in that partition (and, without
        // WITHIN on an idle partition, indefinitely). Cheaper to drop the cache on every rebase
        // than to carry a generation counter: the cache exists to save repeated walks on a HELD
        // match, and a rebase means the next visit has to re-walk anyway.
        run.held = None;
        for c in &run.rows[..upto] {
            state_table.delete(once(Some(ScalarImpl::Int64(c.seq))).chain(&c.row));
        }
        let rebuild = if upto >= run.rows.len() {
            run.rows.clear();
            true
        } else {
            let boundary = Seq(run.rows[upto].seq);
            let rebased = matches!(
                run.matcher.finalize_evicted_prefix(boundary),
                Finalized::Rebased
            );
            run.rows.drain(..upto);
            !rebased
        };
        if rebuild {
            // Full reset (keeps the shared automaton and the allocations) — reconstructing here
            // would deep-copy the skip mode once per emitted match under PAST LAST ROW.
            run.matcher.reset();
            if !run.rows.is_empty() {
                let seqs: Vec<Seq> = run.rows.iter().map(|r| Seq(r.seq)).collect();
                let matcher = DefineMatcher {
                    rows: &run.rows,
                    defines,
                    within,
                };
                run.matcher
                    .advance(&seqs, &matcher, budget, memoizable)
                    .await?;
            }
        }
        Ok(())
    }

    /// Recovery rebuild (rescale restarts the actor and re-enters through this same path):
    /// re-feed every retained row in key order — `(partition...,
    /// order..., seq)`, so each partition arrives contiguous and ordered. No emission here: an
    /// emittable match is consumed in the same epoch it emits, so retained rows only carry held or
    /// partial matches; anything mid-epoch at a crash is re-delivered by replay and re-triggers.
    ///
    /// Rows are collected first and each partition is fed with ONE `advance` call: feeding
    /// row-by-row would rescan the partition's live suffix per row — O(retained²) predicate
    /// evaluations inside the barrier path, turning a partition legitimately retaining many rows
    /// (long `WITHIN`, no completion) into a recovery stall.
    ///
    /// Returns the largest committed `seq` seen (`-1` if none), so the caller can seed the
    /// per-actor seq counter strictly above every retained row.
    #[allow(clippy::too_many_arguments)]
    async fn rebuild_partitions(
        parts: &mut hashbrown::HashMap<OwnedRow, PartitionRun>,
        state_table: &StateTable<S>,
        partition_key_indices: &[usize],
        time_col: usize,
        nfa: &std::sync::Arc<Nfa>,
        skip: &SkipMode,
        defines: &HashMap<String, CompiledDefine>,
        within: Option<&NonStrictExpression>,
        within_deadline: &Option<NonStrictExpression>,
        memoizable: bool,
        eval_error_report: &ActorEvalErrorReport,
        metrics: &MatchRecognizeMetrics,
    ) -> StreamExecutorResult<i64> {
        parts.clear();
        let mut max_seq: i64 = -1;
        let vnodes: Vec<_> = state_table.vnodes().iter_vnodes().collect();
        for vnode in vnodes {
            let stream = state_table
                .iter_keyed_row_with_vnode(
                    vnode,
                    &(
                        std::ops::Bound::<OwnedRow>::Unbounded,
                        std::ops::Bound::<OwnedRow>::Unbounded,
                    ),
                    Default::default(),
                )
                .await?;
            pin_mut!(stream);
            while let Some(kv) = stream.next().await {
                let kv = kv?;
                let stored = kv.row();
                // Stored layout: `[ seq, <input cols..> ]`. Fail descriptive on a corrupt row —
                // a panic here would crash-loop recovery on state that recovery cannot fix.
                let seq = match stored.datum_at(0) {
                    Some(ScalarRefImpl::Int64(s)) => s,
                    other => {
                        return Err(anyhow::anyhow!(
                            "corrupt MATCH_RECOGNIZE state row: seq column must be a non-null \
                             int64, got {other:?}"
                        )
                        .into());
                    }
                };
                max_seq = max_seq.max(seq);
                let input_row = OwnedRow::new(
                    (1..stored.len())
                        .map(|i| stored.datum_at(i).to_owned_datum())
                        .collect(),
                );
                let pk = (&input_row).project(partition_key_indices).into_owned_row();
                let order_key = input_row.datum_at(time_col).to_owned_datum();
                // Not counted here: the ingest path counted this row once already, and a rebuild
                // re-evaluates every retained row on each recovery.
                let deadline = eval_deadline(within_deadline, &order_key).await;
                let run = parts.entry(pk).or_insert_with(|| PartitionRun {
                    rows: Vec::new(),
                    matcher: IncrementalMatcher::new(nfa.clone(), skip.clone()),
                    held: None,
                });
                // The emit path and the dead-prefix prune binary-search `rows` by `seq`; pin the
                // invariant where it is produced: state-table key order must feed each partition's
                // seqs in strictly increasing order (the ordered-input contract).
                debug_assert!(
                    run.rows.last().is_none_or(|last| last.seq < seq),
                    "state-table iteration fed a non-increasing seq into a partition buffer"
                );
                run.rows.push(BufferedRow {
                    seq,
                    order_key,
                    deadline,
                    row: input_row,
                });
            }
        }
        // One BOUNDED budget per partition, exactly like a steady-state visit: recovery and
        // rescale run inside the barrier path, so an unmetered exponential pattern here would
        // hang the actor instead of degrading. A spent budget is safe for the same reason it is
        // safe on the data path — no emission happens during rebuild, the freeze loop holds on
        // exhaustion, and the next visit rescans the suffix with a fresh budget.
        let mut reported_budget = false;
        for run in parts.values_mut() {
            let mut budget = ScanBudget::new(SCAN_BUDGET_EVALUATIONS);
            let fed: Vec<Seq> = run.rows.iter().map(|r| Seq(r.seq)).collect();
            let matcher = DefineMatcher {
                rows: &run.rows,
                defines,
                within,
            };
            run.matcher
                .advance(&fed, &matcher, &mut budget, memoizable)
                .await?;
            if budget.hit {
                metrics.match_recognize_scan_budget_exhausted_count.inc();
                report_scan_budget_once(eval_error_report, &mut reported_budget);
            }
        }
        Ok(max_seq)
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self: Box<Self>) {
        let Self {
            ctx,
            input,
            schema,
            chunk_size,
            partition_key_indices,
            time_col,
            measures,
            defines,
            within,
            within_deadline,
            nfa,
            skip,
            eval_error_report,
            mut state_table,
        } = *self;

        // Whether the per-start `(state, position)` failure memo is sound for this query: no
        // `DEFINE` slot may read the running label assignment. See `Memo` in the NFA module.
        let memoizable = defines.values().all(|d| {
            d.slots
                .iter()
                .all(|s| matches!(s.kind, DefineSlotKind::SelfCol | DefineSlotKind::Prev))
        });

        // One shared automaton for every per-partition matcher (and every post-consumption
        // reset); the matchers hold it by `Arc`.
        let nfa = std::sync::Arc::new(nfa);
        // Fixed-length linear patterns ((a b) and friends) can skip the per-emit extension probe.
        let statically_terminal = nfa.is_linear();

        let metrics = ctx.streaming_metrics.new_match_recognize_metrics(
            state_table.table_id(),
            ctx.id,
            ctx.fragment_id,
        );

        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        state_table.init_epoch(first_epoch).await?;

        // `hashbrown` rather than std for `entry_ref` (see the ingest path below); std's raw
        // entry API never stabilized.
        let mut parts: hashbrown::HashMap<OwnedRow, PartitionRun> = hashbrown::HashMap::new();

        // Recovery / rescale rebuild: see `rebuild_partitions`.
        let max_seq = Self::rebuild_partitions(
            &mut parts,
            &state_table,
            &partition_key_indices,
            time_col,
            &nfa,
            &skip,
            &defines,
            within.as_ref(),
            &within_deadline,
            memoizable,
            &eval_error_report,
            &metrics,
        )
        .await?;

        // `seq` is the PK tiebreaker for rows with equal ORDER BY keys, so it MUST be monotonic
        // in arrival order — the state-table order is the re-feed order on recovery/rescale, and
        // a tie re-fed in a different order than the live matcher saw silently changes which row
        // a match binds (a snowflake-style generator breaks this: its ids interleave vnode bits
        // above the sequence bits, so they are not monotonic within a millisecond). A plain
        // counter is monotonic by construction; its seed must be strictly above BOTH every
        // retained row's seq AND every seq ever minted before — consumed rows are deleted, so
        // "max retained + 1" alone would re-mint the seqs of fully-consumed matches after a
        // restart or rescale, and a reused seq collides `_match_id` values: the new match's
        // output row silently REPLACES the old one in the materialized view (same stream key).
        // The epoch floor provides the never-look-back bound: barrier epochs carry a physical
        // timestamp that only grows, and 2^20 seqs per millisecond is beyond any actor's mint
        // rate. `_match_id` is NOT minted here — it is the match's start-row `seq` (see
        // `emit_ready`), so replayed emission is deterministic.
        let seq_floor = |epoch: risingwave_common::util::epoch::EpochPair| -> i64 {
            (risingwave_common::util::epoch::Epoch(epoch.curr).physical_time() as i64) << 20
        };
        let mut next_seq: i64 = (max_seq + 1).max(seq_floor(first_epoch));

        // Rows currently retained in memory across this actor's partitions, mirrored into the
        // `retained_rows` gauge at the end of every buffer-mutating message (chunk, watermark,
        // and the recovery rebuild; a plain barrier mutates no buffer). Retention is bounded only by
        // match liveness and `WITHIN`, so without this gauge a partition set growing toward memory
        // exhaustion (a pattern whose closer never arrives keeps its rows forever) is invisible
        // until the OOM. The chunk arm maintains it incrementally (an increment beside the push,
        // a decrement beside its eviction counter) because it never iterates the whole partition
        // map; the watermark arm and the rebuild sites recount exactly, which self-heals any
        // accounting slip within one watermark.
        let mut retained_rows: i64 = parts.values().map(|r| r.rows.len() as i64).sum();
        metrics.match_recognize_retained_rows.set(retained_rows);

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    let chunk = chunk.compact_vis();
                    let mut builder = StreamChunkBuilder::new(chunk_size, schema.data_types());
                    let mut reported_budget = false;
                    let mut reported_degradations: Vec<SkipDegradation> = Vec::new();
                    // One budget per CHUNK, not per row. Per-row was 256 fresh budgets for a default
                    // chunk, so a single degraded partition could spend 2^28 predicate evaluations in
                    // one message — tens of seconds of single-threaded work with the barrier queued
                    // behind it, which stalls checkpointing well beyond this job. Sharing it across
                    // the chunk bounds a message at 2^20 regardless of chunk size.
                    //
                    // Safe because nothing on this path needs budget for correctness: every row is
                    // still buffered and fed, a truncated `advance` only defers match derivation and
                    // latches `incomplete`, and the next watermark re-derives with a fresh budget.
                    // Later rows of a chunk that exhausts the budget emit nothing, which is the same
                    // degraded-latency behaviour the design describes.
                    //
                    // TODO: the watermark arm still grants one budget per partition, so a pass costs
                    // `starved_partitions * 2^20` and a message is bounded only by partition count.
                    // Fixing that needs a pass-level cap plus a rotating start offset — a shared
                    // budget alone would starve whichever partitions sort last, pass after pass, as
                    // the comment on that arm says. Left for the follow-up that also addresses
                    // convergence (scanning the window-bounded expiring region under its own budget).
                    let mut budget = ScanBudget::new(SCAN_BUDGET_EVALUATIONS);
                    for (op, row_ref) in chunk.rows() {
                        // Append-only input is enforced at planning time, so a non-Insert here is
                        // an upstream inconsistency. Follow the operator convention rather than
                        // erroring the actor unconditionally: panic under strict consistency,
                        // report-and-skip the record when the cluster runs with strict consistency
                        // disabled — the escape hatch that lets a job limp past bad data instead of
                        // crash-looping on it.
                        if !matches!(op, Op::Insert) {
                            crate::consistency::consistency_panic!(
                                ?op,
                                "MATCH_RECOGNIZE requires append-only input",
                            );
                            continue;
                        }
                        let order_key = row_ref.datum_at(time_col).to_owned_datum();
                        // A NULL order key has no event time: the sort would never release it in
                        // any defined position. Drop it, as event-time processing does.
                        if order_key.is_none() {
                            continue;
                        }
                        let seq = next_seq;
                        next_seq += 1;
                        let deadline = eval_deadline(&within_deadline, &order_key).await;
                        // A `WITHIN` that silently stopped bounding this row's partial is worth
                        // seeing: an integer order key near its type's maximum is a schema smell.
                        if within_deadline.is_some() && deadline == Deadline::Never {
                            metrics.match_recognize_within_deadline_overflow_count.inc();
                        }
                        state_table.insert(once(Some(ScalarImpl::Int64(seq))).chain(row_ref));
                        let pk = row_ref.project(&partition_key_indices).into_owned_row();
                        // `entry_ref` hashes and probes once, materializing the key only on a
                        // vacant insert — one probe fewer than the contains_key/get_mut pair it
                        // replaced. (The `pk` allocation above is per-row either way.)
                        let run = parts.entry_ref(&pk).or_insert_with(|| PartitionRun {
                            rows: Vec::new(),
                            matcher: IncrementalMatcher::new(nfa.clone(), skip.clone()),
                            held: None,
                        });
                        run.rows.push(BufferedRow {
                            seq,
                            order_key,
                            deadline,
                            row: row_ref.into_owned_row(),
                        });
                        retained_rows += 1;
                        {
                            let fed = [Seq(seq)];
                            let matcher = DefineMatcher {
                                rows: &run.rows,
                                defines: &defines,
                                within: within.as_ref(),
                            };
                            run.matcher
                                .advance(&fed, &matcher, &mut budget, memoizable)
                                .await?;
                        }
                        let rows_before = run.rows.len();
                        let filled = Self::emit_ready(
                            run,
                            &pk,
                            &nfa,
                            &skip,
                            &defines,
                            within.as_ref(),
                            &measures,
                            None,
                            &mut state_table,
                            &mut builder,
                            &eval_error_report,
                            &mut reported_degradations,
                            &mut budget,
                            memoizable,
                            statically_terminal,
                            &metrics,
                        )
                        .await?;
                        let evicted = (rows_before - run.rows.len()) as u64;
                        metrics.match_recognize_evicted_rows_count.inc_by(evicted);
                        retained_rows -= evicted as i64;
                        // Captured while the entry is still borrowed; the removal below needs the
                        // borrow released.
                        let partition_emptied = run.rows.is_empty();
                        let emptied_capacity = run.rows.capacity();
                        for c in filled {
                            yield Message::Chunk(c);
                        }
                        // Only drop a partition whose buffers actually grew. Removing it
                        // unconditionally looked tidier but cost more than the wart it fixed: any
                        // pattern that consumes its whole buffer per match (the common case under
                        // the default PAST LAST ROW — `PATTERN (d w)`, say) empties its partition on
                        // essentially every row, and each removal forces the next row down the slow
                        // branch, paying a partition-key clone, a skip-mode clone and regrowth of
                        // three vectors — exactly the reconstruction `consume_prefix`'s `reset()`
                        // exists to avoid. Above the threshold the retained capacity is worth more
                        // than the reconstruction: a partition that peaked at tens of thousands of
                        // rows holds a large allocation for an entry with nothing in it. Below it,
                        // the next watermark pass sweeps the entry anyway.
                        const DROP_EMPTY_PARTITION_CAPACITY: usize = 1024;
                        if partition_emptied && emptied_capacity >= DROP_EMPTY_PARTITION_CAPACITY {
                            // Safe for the same reason as the watermark arm: `consume_prefix` resets
                            // the matcher when it consumes the whole buffer, so an empty-rows
                            // partition carries no state a later row would need.
                            parts.remove(&pk);
                        }
                    }
                    // Once per chunk, not once per row: the budget is now shared across the chunk, so
                    // a per-row check would count every row seen after exhaustion and the counter
                    // would read as "rows processed while starved" rather than "visits that ran out".
                    if budget.hit {
                        metrics.match_recognize_scan_budget_exhausted_count.inc();
                        report_scan_budget_once(&eval_error_report, &mut reported_budget);
                    }
                    metrics.match_recognize_retained_rows.set(retained_rows);
                    if let Some(c) = builder.take() {
                        yield Message::Chunk(c);
                    }
                    // Bound mem-table growth between barriers, as the sibling EOWC executors do.
                    state_table.try_flush().await?;
                }
                Message::Watermark(w) => {
                    // Only the leading ORDER BY column's watermark drives WITHIN finality and
                    // pruning; the output schema carries no watermark columns, so none is
                    // forwarded downstream.
                    if w.col_idx != time_col {
                        continue;
                    }
                    let mut builder = StreamChunkBuilder::new(chunk_size, schema.data_types());
                    let mut reported_budget = false;
                    let mut reported_degradations: Vec<SkipDegradation> = Vec::new();
                    let mut emptied: Vec<OwnedRow> = Vec::new();
                    for (pk, run) in &mut parts {
                        // One budget per partition VISIT: a shared pass-wide budget would let a
                        // single pathological partition starve emission and eviction for every
                        // partition iterated after it, pass after pass (map order is stable).
                        // The budget is a cap, not a spend — a healthy partition never nears it.
                        let mut budget = ScanBudget::new(SCAN_BUDGET_EVALUATIONS);
                        // A previous visit's rescan may have been budget-truncated, leaving the
                        // provisional tail an under-approximation. Re-derive with this visit's
                        // fresh budget BEFORE deciding anything: the deadline prune below treats
                        // missing matches as decided, which is only sound over a complete tail.
                        // A budget-truncated FREEZE asks for the same: it left proven-dead
                        // progress to resume from, and an idle partition gets no arrival to do it.
                        if run.matcher.needs_refresh() {
                            let matcher = DefineMatcher {
                                rows: &run.rows,
                                defines: &defines,
                                within: within.as_ref(),
                            };
                            run.matcher
                                .refresh(&matcher, &mut budget, memoizable)
                                .await?;
                        }
                        let rows_before = run.rows.len();
                        let filled = Self::emit_ready(
                            run,
                            pk,
                            &nfa,
                            &skip,
                            &defines,
                            within.as_ref(),
                            &measures,
                            Some(&w.val),
                            &mut state_table,
                            &mut builder,
                            &eval_error_report,
                            &mut reported_degradations,
                            &mut budget,
                            memoizable,
                            statically_terminal,
                            &metrics,
                        )
                        .await?;
                        for c in filled {
                            yield Message::Chunk(c);
                        }
                        Self::prune_dead_prefix(
                            run,
                            &nfa,
                            &defines,
                            within.as_ref(),
                            &w.val,
                            &mut state_table,
                            &mut budget,
                            memoizable,
                        )
                        .await?;
                        metrics
                            .match_recognize_evicted_rows_count
                            .inc_by((rows_before - run.rows.len()) as u64);
                        if run.rows.is_empty() {
                            emptied.push(pk.clone());
                        }
                        if budget.hit {
                            metrics.match_recognize_scan_budget_exhausted_count.inc();
                            report_scan_budget_once(&eval_error_report, &mut reported_budget);
                        }
                    }
                    for pk in emptied {
                        parts.remove(&pk);
                    }
                    // Exact recount, not the incremental counter: this arm just iterated every
                    // partition, so the recount costs what the pass already paid — and it bounds
                    // the lifetime of any future accounting slip to one watermark instead of
                    // forever. (The chunk arm keeps the incremental counter: a recount there
                    // would add a whole-map walk per chunk.)
                    retained_rows = parts.values().map(|r| r.rows.len() as i64).sum();
                    metrics.match_recognize_retained_rows.set(retained_rows);
                    if let Some(c) = builder.take() {
                        yield Message::Chunk(c);
                    }
                    // A watermark can expire rows across every partition at once (a WITHIN cliff);
                    // bound the mem-table growth exactly as the chunk arm does.
                    state_table.try_flush().await?;
                }
                Message::Barrier(barrier) => {
                    // In-place vnode-bitmap updates are a deprecated scaling path: rescale
                    // restarts the actor, and the post-first-barrier rebuild above reconstructs
                    // every partition from the re-sharded table (the epoch floor on the seq
                    // counter is what keeps re-minted seqs impossible across that restart — see
                    // the seeding comment above). Assert the assumption instead of carrying a
                    // second, in-place rebuild branch.
                    barrier.assume_no_update_vnode_bitmap(ctx.id)?;
                    state_table
                        .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                        .await?;
                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

/// Per-partition live state: the retained rows (exactly what live partials and held matches still
/// reference) and the incremental matcher fed with them. `rows` and the matcher's fed positions
/// stay aligned: pushed exactly when fed, drained exactly when the matcher finalizes past them.
struct PartitionRun {
    rows: Vec<BufferedRow>,
    matcher: IncrementalMatcher,
    /// Emission-gate short-circuit: `(first match's start seq, resume_pos, rows.len())` for which
    /// the gate last answered "hold" with an UNSPENT budget. The gate's gap-liveness and
    /// extension verdicts are pure functions of that triple over immutable rows, so while it is
    /// unchanged only the watermark-dependent `WITHIN`-finality test can flip the answer — a held
    /// match would otherwise re-pay the full walk set on every visit until decided. Budget-hit
    /// verdicts are never cached (they are not verdicts).
    ///
    /// The triple identifies gate state only over an UNCHANGED buffer, so `consume_prefix` clears
    /// this on every rebase: after a prune, the same `(resume_pos, len)` can recur over a different
    /// set of rows and a stale hit would withhold a now-decidable match.
    held: Option<(Seq, usize, usize)>,
}

impl<S: StateStore> Execute for MatchRecognizeExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

/// Whether the first provisional match `[start, ..)` is FINAL — emitting it now agrees with the
/// batch answer over every possible future input — or must be held.
///
/// "A later row exists" is not finality. Two shapes prove it: a more-preferred branch from the
/// same start can be blocked at the BUFFER boundary rather than the match's end (`(a b c d | a b)`
/// over rows a,b,c: the provisional match (0,2) is followed, but a future `d` makes the preferred
/// branch win), and a *gap* position in `[resume_pos, start)` can still be alive at the boundary
/// (`(x n n | n)` over rows x,n: the match at 1 is terminal from its own start, but position 0
/// plus a future `n` yields the leftmost-preferred match). The matcher's freeze gate proves region
/// deadness before retiring its cursor; this is the same discipline anchored at emission:
///
/// - a closed `WITHIN` window (`within_final`) decides everything: every gap row's window closed
///   no later than this match's (order keys are non-decreasing), a gap match over the existing
///   rows would have been the finder's leftmost result already, and any future row violates the
///   inclusive span bound for every start at or before this one;
/// - otherwise every gap position must be provably dead at the boundary — on a spent budget the
///   answer is HOLD, since a fabricated "dead" is precisely the lost-match bug class — and the
///   finder's preferred result from `start` must be un-improvable by future rows
///   ([`Nfa::may_extend`] probed at the buffer boundary, which itself answers "may extend" on a
///   spent budget).
///
/// Positions strictly inside the match (past `start`) are deliberately NOT checked: once the
/// leftmost match is final, the skip mode consumes through them in batch too — abandoning their
/// partials is the batch semantics, not a divergence.
#[allow(clippy::too_many_arguments)]
async fn match_is_final(
    nfa: &Nfa,
    matcher: &(impl CandidateMatcher + Sync),
    resume_pos: usize,
    start: usize,
    n_rows: usize,
    within_final: bool,
    statically_terminal: bool,
    budget: &mut ScanBudget,
    memoize: bool,
) -> StreamExecutorResult<bool> {
    // All three positions are in the same coordinate system: indices into the partition's
    // retained-row buffer (= the matcher's fed positions; `consume_prefix` keeps them aligned).
    // `resume_pos > start` is a LEGITIMATE state, not a violation: a frozen but not-yet-emitted
    // match starts before the resume position (which points past the LAST frozen match), and for
    // it the gap range below is deliberately empty — its region was already proven dead by the
    // freeze gate.
    debug_assert!(
        start <= n_rows,
        "match_is_final start out of range: start={start} n={n_rows}"
    );
    if within_final {
        return Ok(true);
    }
    for p in resume_pos..start {
        let alive = nfa
            .reaches_boundary_alive(p, n_rows, matcher, budget, memoize)
            .await?;
        if budget.hit || alive {
            return Ok(false);
        }
    }
    // A fixed-length linear pattern has exactly one path: an accepted match can never be
    // superseded from its own start, so the probe is statically decided (see [`Nfa::is_linear`]).
    if statically_terminal {
        return Ok(true);
    }
    let extend = nfa
        .may_extend(start, n_rows, matcher, budget, memoize)
        .await?;
    Ok(!extend)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use risingwave_expr::expr::LogReport;
    use risingwave_pb::expr::expr_node::{RexNode, Type as PbExprType};
    use risingwave_pb::expr::{ExprNode, FunctionCall as PbFunctionCall};
    use risingwave_pb::stream_plan::MatchRecognizeDefineSlot as PbDefineSlot;

    use super::*;
    use crate::executor::match_recognize::nfa::{LabeledMatch, Pattern, Quantifier};

    /// Slot kinds as the planner encodes them (see `MatchRecognizeDefineSlot.kind`).
    const KIND_SELF: i32 = 1;
    const KIND_PREV: i32 = 2;
    const KIND_RUNNING_FIRST: i32 = 4;
    const KIND_RUNNING_LAST: i32 = 5;

    /// One `int` column named `v` at index 0; `order_key` mirrors the physical position (unused
    /// without `WITHIN`, but kept consistent).
    fn buffered(vals: &[i32]) -> Vec<BufferedRow> {
        vals.iter()
            .enumerate()
            .map(|(i, v)| BufferedRow {
                seq: i as i64,
                order_key: Some(ScalarImpl::Int32(i as i32)),
                deadline: Deadline::Never,
                row: OwnedRow::new(vec![Some(ScalarImpl::Int32(*v))]),
            })
            .collect()
    }

    fn input_ref(idx: u32) -> ExprNode {
        ExprNode {
            function_type: PbExprType::Unspecified as i32,
            return_type: Some(DataType::Int32.to_protobuf()),
            rex_node: Some(RexNode::InputRef(idx)),
        }
    }

    /// `slots[0] = slots[1]`, i.e. the navigation slot compared against the candidate's own column.
    fn nav_eq_self_condition() -> ExprNode {
        ExprNode {
            function_type: PbExprType::Equal as i32,
            return_type: Some(DataType::Boolean.to_protobuf()),
            rex_node: Some(RexNode::FuncCall(PbFunctionCall {
                children: vec![input_ref(0), input_ref(1)],
            })),
        }
    }

    /// A navigation slot over column `v`.
    fn nav_slot(kind: i32, vars: &[&str], offset: u32) -> PbDefineSlot {
        PbDefineSlot {
            kind,
            vars: vars.iter().map(|v| (*v).to_owned()).collect(),
            col_idx: 0,
            offset,
        }
    }

    /// `DEFINE <symbol> AS <nav> = <symbol>.v`, compiled through the real proto lowering so the slot
    /// kinds are the planner's.
    fn nav_eq_self(symbol: &str, nav: PbDefineSlot) -> (String, CompiledDefine) {
        let pb = PbMatchRecognizeDefine {
            symbol: symbol.to_owned(),
            condition: Some(nav_eq_self_condition()),
            slots: vec![nav, nav_slot(KIND_SELF, &[], 0)],
        };
        (
            symbol.to_owned(),
            CompiledDefine::from_protobuf(&pb, LogReport).unwrap(),
        )
    }

    fn plus(var: &str) -> Pattern {
        Pattern::Quantified(
            Box::new(Pattern::Var(var.to_owned())),
            Quantifier::Plus,
            false,
        )
    }

    fn labels(s: &str) -> Vec<String> {
        s.chars().map(|c| c.to_string()).collect()
    }

    /// All matches over `vals`, with the whole buffer safe (no watermark boundary in play).
    async fn find_all(
        nfa: &Nfa,
        defines: &HashMap<String, CompiledDefine>,
        vals: &[i32],
    ) -> Vec<LabeledMatch> {
        let rows = buffered(vals);
        let matcher = DefineMatcher {
            rows: &rows,
            defines,
            within: None,
        };
        nfa.find_matches_dynamic(rows.len(), &matcher, &SkipMode::PastLastRow)
            .await
            .unwrap()
    }

    /// `DEFINE a AS LAST(a.v) = a.v` is a tautology: SQL:2016 defines a pattern-variable-qualified
    /// column reference as `RUNNING LAST` of that column, and the binder already resolves the bare
    /// `a.v` inside `a`'s own DEFINE to the candidate row. So the running navigation must see the
    /// candidate too — including on the match's first row, where no earlier `a` exists.
    #[tokio::test]
    async fn define_running_last_of_self_sees_candidate() {
        let defines = HashMap::from([nav_eq_self("a", nav_slot(KIND_RUNNING_LAST, &["a"], 0))]);
        assert_eq!(
            find_all(&Nfa::compile(&plus("a")), &defines, &[1, 2, 3]).await,
            vec![LabeledMatch {
                start: 0,
                end: 3,
                labels: labels("aaa"),
            }]
        );
    }

    /// The eviction walker shares the finder's satisfies-source, so it must reach the same verdict:
    /// a lone row that satisfies `a AS LAST(a.v) = a.v` is a live partial match of `(a b)` and must
    /// be retained. Were the two to disagree, eviction would delete rows the matcher still needs.
    #[tokio::test]
    async fn define_running_last_of_self_keeps_start_alive() {
        let rows = buffered(&[1]);
        let defines = HashMap::from([nav_eq_self("a", nav_slot(KIND_RUNNING_LAST, &["a"], 0))]);
        let matcher = DefineMatcher {
            rows: &rows,
            defines: &defines,
            within: None,
        };
        let nfa = Nfa::compile(&Pattern::Concat(vec![
            Pattern::Var("a".to_owned()),
            Pattern::Var("b".to_owned()),
        ]));
        assert!(
            nfa.reaches_boundary_alive(
                0,
                rows.len(),
                &matcher,
                &mut ScanBudget::unlimited(),
                false,
            )
            .await
            .unwrap()
        );
    }

    /// `DEFINE a AS FIRST(a.v) = a.v`: the candidate is the *first* `a` only while no earlier `a` is
    /// bound, so this holds for the match's first row and then pins later rows to that value.
    #[tokio::test]
    async fn define_running_first_of_self_sees_candidate() {
        let defines = HashMap::from([nav_eq_self("a", nav_slot(KIND_RUNNING_FIRST, &["a"], 0))]);
        assert_eq!(
            find_all(&Nfa::compile(&plus("a")), &defines, &[5, 5, 7]).await,
            vec![
                // 5, 5 share the first value; 7 breaks it and starts its own match.
                LabeledMatch {
                    start: 0,
                    end: 2,
                    labels: labels("aa"),
                },
                LabeledMatch {
                    start: 2,
                    end: 3,
                    labels: labels("a"),
                },
            ]
        );
    }

    /// Running navigation that falls back on `labels` still indexes from the match's start. Here
    /// `x AS PREV(x.v) = x.v` cannot hold at position 0 (there is no previous row), so the match
    /// starts at 1, and `a+` binds two rows, so the third row's `FIRST(a.v)` resolves through
    /// `labels[1]` — pinning the `match_start + k` arithmetic where neither term is 0.
    #[tokio::test]
    async fn define_running_first_indexes_from_match_start() {
        let defines = HashMap::from([
            nav_eq_self("x", nav_slot(KIND_PREV, &[], 1)),
            nav_eq_self("a", nav_slot(KIND_RUNNING_FIRST, &["a"], 0)),
        ]);
        let nfa = Nfa::compile(&Pattern::Concat(vec![
            Pattern::Var("x".to_owned()),
            plus("a"),
        ]));
        assert_eq!(
            // `x` = the second 9 (its physical predecessor is the first 9); the run of 7s is `a+`,
            // whose `FIRST` is `rows[2]`, so the trailing 5 ends the match.
            find_all(&nfa, &defines, &[9, 9, 7, 7, 5]).await,
            vec![LabeledMatch {
                start: 1,
                end: 4,
                labels: labels("xaa"),
            }]
        );
    }

    /// `SUBSET u = (a, b)` + `DEFINE a AS LAST(u.v) = a.v`: the candidate is tentatively an `a`, and
    /// `a ∈ u`, so it counts as the running last of `u`. Both spellings lower to this same slot —
    /// `LAST(u.v)` via the navigation path and the bare `u.v` via the input-ref rewriter (whose
    /// self-reference exemption is name-exact and therefore misses the subset).
    #[tokio::test]
    async fn define_running_last_of_subset_containing_self_sees_candidate() {
        // The slot's `vars` is `members_of(u)`, which preserves the SUBSET's declaration order, so
        // both orders must behave identically: membership is a set test, not a look at `vars[0]`.
        for members in [["a", "b"], ["b", "a"]] {
            let defines =
                HashMap::from([nav_eq_self("a", nav_slot(KIND_RUNNING_LAST, &members, 0))]);
            assert_eq!(
                find_all(&Nfa::compile(&plus("a")), &defines, &[1, 2, 3]).await,
                vec![LabeledMatch {
                    start: 0,
                    end: 3,
                    labels: labels("aaa"),
                }],
                "SUBSET u = ({}, {})",
                members[0],
                members[1]
            );
        }
    }

    /// Navigation over a variable set that does *not* contain the candidate's own variable keeps
    /// resolving to the earlier row: `DEFINE b AS LAST(a.v) = b.v` compares against the `a`, never
    /// against the candidate `b`. This is the shape every existing DEFINE test uses.
    #[tokio::test]
    async fn define_running_last_of_other_var_excludes_candidate() {
        let defines = HashMap::from([nav_eq_self("b", nav_slot(KIND_RUNNING_LAST, &["a"], 0))]);
        let nfa = Nfa::compile(&Pattern::Concat(vec![
            Pattern::Var("a".to_owned()),
            Pattern::Var("b".to_owned()),
        ]));
        // Equal values: the `b` row equals the running `a`, so `(a b)` matches.
        assert_eq!(
            find_all(&nfa, &defines, &[7, 7]).await,
            vec![LabeledMatch {
                start: 0,
                end: 2,
                labels: labels("ab"),
            }]
        );
        // Different values: had the candidate been treated as the running last of `a`, this would
        // become a tautology and match.
        assert_eq!(find_all(&nfa, &defines, &[7, 8]).await, vec![]);
    }

    /// Collects what the executor reports, so the `AFTER MATCH SKIP` diagnostic can be asserted
    /// without an actor (in production the report goes to `ActorEvalErrorReport`).
    #[derive(Clone, Default)]
    struct CollectReport(Arc<Mutex<Vec<String>>>);

    impl EvalErrorReport for CollectReport {
        fn report(&self, error: ExprError) {
            self.0.lock().unwrap().push(error.to_string());
        }
    }

    impl CollectReport {
        fn messages(&self) -> Vec<String> {
            self.0.lock().unwrap().clone()
        }
    }

    /// The reported error must be actionable on its own — it is what lands in the `error=` field of
    /// the `stream_expr_error` log line. Pinned verbatim: it names the skip mode (once), the target
    /// variable, and the strategy the resume position degraded to.
    #[test]
    fn skip_degradation_report_names_clause_and_fallback() {
        let report = CollectReport::default();
        let mut reported = Vec::new();
        report_skip_degradation_once(
            &report,
            &SkipMode::ToLast("c".to_owned()),
            SkipDegradation::TargetAbsent,
            &mut reported,
        );
        report_skip_degradation_once(
            &report,
            &SkipMode::ToFirst("a".to_owned()),
            SkipDegradation::TargetAtMatchStart,
            &mut reported,
        );
        assert_eq!(
            report.messages(),
            vec![
                "Invalid parameter AFTER MATCH SKIP TO LAST: target variable `c` is bound to no row \
                 of the match, so there is no row to resume at; the scan resumed past the match's \
                 last row instead (degraded to SKIP PAST LAST ROW)",
                "Invalid parameter AFTER MATCH SKIP TO FIRST: target variable `a` resolves to the \
                 match's own first row, so resuming there would re-find the same match forever; the \
                 scan resumed at the row after the match's first row instead (degraded to SKIP TO \
                 NEXT ROW)",
            ]
        );
    }

    /// Volume policy: the degradation repeats without bound (on every match, when no match can bind
    /// the target), and the message has no row, match or partition identity, so a per-match report
    /// would be byte-identical chatter. One report per kind per watermark pass.
    #[test]
    fn skip_degradation_report_is_deduplicated_per_pass() {
        let report = CollectReport::default();
        let skip = SkipMode::ToLast("x".to_owned());
        let mut reported = Vec::new();
        for _ in 0..5 {
            report_skip_degradation_once(
                &report,
                &skip,
                SkipDegradation::TargetAbsent,
                &mut reported,
            );
        }
        assert_eq!(report.messages().len(), 1, "{:?}", report.messages());
        // A different degradation is a different diagnostic, so it is reported once too.
        report_skip_degradation_once(
            &report,
            &skip,
            SkipDegradation::TargetAtMatchStart,
            &mut reported,
        );
        assert_eq!(report.messages().len(), 2, "{:?}", report.messages());
        // The next pass starts with a fresh set, so a persisting condition keeps being visible.
        let mut next_pass = Vec::new();
        report_skip_degradation_once(
            &report,
            &skip,
            SkipDegradation::TargetAbsent,
            &mut next_pass,
        );
        assert_eq!(report.messages().len(), 3, "{:?}", report.messages());
    }

    /// The emit-finality gate must agree with batch preference semantics, not with the positional
    /// "a later row exists" proxy. These pin the two supersession shapes that proxy gets wrong.
    /// `WITHIN` deadline evaluation, and the two tests the executor reads off the cached value.
    mod within_deadline {
        use risingwave_common::types::DatumRef;
        use risingwave_expr::expr::{LiteralExpression, build_from_pretty};

        use super::*;

        /// Records every error it is handed, so a test can see what a wrapper let through.
        #[derive(Clone, Default)]
        struct RecordingReport(Arc<Mutex<Vec<String>>>);

        impl EvalErrorReport for RecordingReport {
            fn report(&self, error: ExprError) {
                self.0.lock().unwrap().push(error.to_string());
            }
        }

        /// `first + 2::smallint` over an int2 order key — the deadline `lower_within` emits for
        /// `ORDER BY <smallint> ... WITHIN 2::smallint` — built the way `from_proto` builds it.
        fn int2_plus_two(report: RecordingReport) -> Option<NonStrictExpression> {
            Some(NonStrictExpression::new_topmost(
                build_from_pretty("(add:int2 $0:int2 2:int2)"),
                DeadlineErrorReport::new(report),
            ))
        }

        async fn deadline_of(order_key: i16) -> Deadline {
            eval_deadline(
                &int2_plus_two(RecordingReport::default()),
                &Some(ScalarImpl::Int16(order_key)),
            )
            .await
        }

        #[tokio::test]
        async fn representable_sum_is_the_deadline() {
            assert_eq!(deadline_of(1).await, Deadline::At(ScalarImpl::Int16(3)));
            // Landing exactly on the type's maximum is still representable.
            assert_eq!(
                deadline_of(32765).await,
                Deadline::At(ScalarImpl::Int16(i16::MAX))
            );
        }

        /// `32766 + 2` leaves int2. Non-strict evaluation folded that into NULL, which the span
        /// check read as "outside the window": a valid zero-span match at the top of the key's
        /// range was silently dropped. Past the type's range the window never closes.
        #[tokio::test]
        async fn overflowing_sum_never_closes() {
            assert_eq!(deadline_of(32766).await, Deadline::Never);
            assert_eq!(deadline_of(i16::MAX).await, Deadline::Never);
        }

        #[tokio::test]
        async fn absent_within_never_closes() {
            let d = eval_deadline(&None, &Some(ScalarImpl::Int16(0))).await;
            assert_eq!(d, Deadline::Never);
        }

        /// The overflow is a legitimate outcome, not a compute error: it must not be counted and
        /// logged against the actor for every row at the top of the key's range. Anything else
        /// the expression raises still is — bare or inside the `Function` wrapper a generated
        /// implementation puts around its function's error.
        #[tokio::test]
        async fn overflow_is_not_reported_but_other_errors_are() {
            let report = RecordingReport::default();
            let expr = int2_plus_two(report.clone());
            assert_eq!(
                eval_deadline(&expr, &Some(ScalarImpl::Int16(32766))).await,
                Deadline::Never
            );
            assert!(
                report.0.lock().unwrap().is_empty(),
                "an out-of-range deadline must not reach the actor's error report"
            );

            let wrapper = DeadlineErrorReport::new(report.clone());
            let no_args = || Vec::<DatumRef<'_>>::new();
            wrapper.report(ExprError::NumericOutOfRange);
            wrapper.report(ExprError::function(
                "add",
                no_args(),
                ExprError::NumericOverflow,
            ));
            assert!(report.0.lock().unwrap().is_empty());

            let wrapped = ExprError::function("divide", no_args(), ExprError::DivisionByZero);
            let wrapped_text = wrapped.to_string();
            wrapper.report(ExprError::DivisionByZero);
            wrapper.report(wrapped);
            assert_eq!(
                *report.0.lock().unwrap(),
                vec![ExprError::DivisionByZero.to_string(), wrapped_text],
                "every other error is forwarded untouched"
            );
        }

        /// Against a never-closing window the span test admits every order key and the finality
        /// test never fires — so a match whose deadline overflowed is decided structurally,
        /// exactly like a match without `WITHIN`. A representable deadline keeps the inclusive
        /// span bound and the strict watermark boundary.
        #[test]
        fn never_admits_everything_and_never_closes() {
            let never = Deadline::Never;
            assert!(never.admits(&ScalarImpl::Int16(i16::MAX)));
            assert!(!never.closed_at(&ScalarImpl::Int16(i16::MAX)));

            let at = Deadline::At(ScalarImpl::Int16(10));
            assert!(
                at.admits(&ScalarImpl::Int16(10)),
                "the span bound is inclusive"
            );
            assert!(!at.admits(&ScalarImpl::Int16(11)));
            assert!(
                !at.closed_at(&ScalarImpl::Int16(10)),
                "the watermark boundary is strict"
            );
            assert!(at.closed_at(&ScalarImpl::Int16(11)));
        }

        /// Two rows with int2 order keys, each carrying the deadline `eval_deadline` computes for
        /// it, no `DEFINE` (every row satisfies every variable), matched against `(a b)` with the
        /// span check armed.
        async fn ab_matches(order_keys: [i16; 2]) -> Vec<LabeledMatch> {
            let mut rows = Vec::new();
            let expr = int2_plus_two(RecordingReport::default());
            for (i, k) in order_keys.into_iter().enumerate() {
                let order_key = Some(ScalarImpl::Int16(k));
                let deadline = eval_deadline(&expr, &order_key).await;
                rows.push(BufferedRow {
                    seq: i as i64,
                    order_key,
                    deadline,
                    row: OwnedRow::new(vec![Some(ScalarImpl::Int32(0))]),
                });
            }
            // Only `is_some()` is read on the hot path; the predicate itself is never evaluated.
            let within = NonStrictExpression::for_test(LiteralExpression::new(
                DataType::Boolean,
                Some(ScalarImpl::Bool(true)),
            ));
            let defines = HashMap::new();
            let matcher = DefineMatcher {
                rows: &rows,
                defines: &defines,
                within: Some(&within),
            };
            let nfa = Nfa::compile(&Pattern::Concat(vec![
                Pattern::Var("a".to_owned()),
                Pattern::Var("b".to_owned()),
            ]));
            nfa.find_matches_dynamic(rows.len(), &matcher, &SkipMode::PastLastRow)
                .await
                .unwrap()
        }

        /// The reported case: `a`@32766, `b`@32766, `WITHIN 2::smallint`. The span is 0, and the
        /// start row's deadline overflows. The match must be found.
        #[tokio::test]
        async fn overflowed_deadline_does_not_reject_a_match_inside_the_bound() {
            assert_eq!(
                ab_matches([32766, 32766]).await,
                vec![LabeledMatch {
                    start: 0,
                    end: 2,
                    labels: labels("ab"),
                }]
            );
        }

        /// Control: the span check still bites where the deadline IS representable. `a`@32763 has
        /// deadline 32765, so `b`@32766 is outside the window; `b` cannot start `(a b)` alone.
        #[tokio::test]
        async fn representable_deadline_still_rejects_a_match_past_the_bound() {
            assert!(ab_matches([32763, 32766]).await.is_empty());
        }
    }

    mod finality_gate {
        use std::collections::BTreeSet;

        use super::super::match_is_final;
        use super::*;
        use crate::executor::match_recognize::nfa::{Nfa, ScanBudget, SetMatcher};

        fn sets(seq: &str) -> Vec<BTreeSet<String>> {
            seq.chars()
                .map(|c| BTreeSet::from([c.to_string()]))
                .collect()
        }

        fn var(s: &str) -> Pattern {
            Pattern::Var(s.to_owned())
        }

        fn concat(names: &str) -> Pattern {
            Pattern::Concat(names.chars().map(|c| var(&c.to_string())).collect())
        }

        async fn gate(
            pattern: &Pattern,
            rows: &str,
            resume_pos: usize,
            start: usize,
            within_final: bool,
        ) -> bool {
            let nfa = Nfa::compile(pattern);
            let matcher = SetMatcher::new(sets(rows));
            let mut budget = ScanBudget::unlimited();
            match_is_final(
                &nfa,
                &matcher,
                resume_pos,
                start,
                rows.len(),
                within_final,
                // Always exercise the real probe in these tests, even for linear patterns.
                false,
                &mut budget,
                true,
            )
            .await
            .unwrap()
        }

        /// `PATTERN (a b c d | a b)` over rows a,b,c: the provisional match is (0,2) via the
        /// second branch and a later row exists (`c`), but the preferred first branch is blocked
        /// at the BUFFER boundary (it consumed a,b,c and needs d) — a future `d` row makes the
        /// batch answer (0,4). "Followed" is not "final": the match must be held.
        #[tokio::test]
        async fn blocked_preferred_branch_holds_followed_match() {
            let pattern = Pattern::Alt(vec![concat("abcd"), concat("ab")]);
            assert!(!gate(&pattern, "abc", 0, 0, false).await);
        }

        /// `PATTERN (x n n | n)` over rows x,n: the only provisional match is (1,2) via the second
        /// branch and it is terminal from its own start — but position 0 is still alive at the
        /// boundary (`x n` inside the preferred branch), and a future `n` row makes the batch
        /// answer (0,3). Leftmost preference: the gap position must hold the emission.
        #[tokio::test]
        async fn alive_gap_position_holds_boundary_match() {
            let pattern = Pattern::Alt(vec![concat("xnn"), var("n")]);
            assert!(!gate(&pattern, "xn", 0, 1, false).await);
        }

        /// Positive control: `PATTERN (a b)` over rows a,b,z — the follower `z` kills every
        /// extension path, no gap, so the match is final at arrival.
        #[tokio::test]
        async fn decided_followed_match_is_final() {
            let pattern = concat("ab");
            assert!(gate(&pattern, "abz", 0, 0, false).await);
        }

        /// Positive control: a boundary match whose pattern has no extension path is final
        /// without any follower.
        #[tokio::test]
        async fn terminal_boundary_match_is_final() {
            let pattern = var("b");
            assert!(gate(&pattern, "b", 0, 0, false).await);
        }

        /// A closed WITHIN window decides everything: the gap row's window closed no later than
        /// the match's own (order keys are ordered), and no future row can satisfy the span bound,
        /// so within-finality bypasses both the gap and the extension probe.
        #[tokio::test]
        async fn within_finality_overrides_alive_gap() {
            let pattern = Pattern::Alt(vec![concat("xnn"), var("n")]);
            assert!(gate(&pattern, "xn", 0, 1, true).await);
        }
    }
}
