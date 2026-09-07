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

//! Row-pattern NFA for `MATCH_RECOGNIZE`.
//!
//! A `Pattern` (the supported v1 subset of the SQL `PATTERN` clause) is compiled to a
//! Thompson-construction NFA whose labelled transitions are pattern variables. The simulation
//! consumes a sequence of rows, where each row is summarised by the set of pattern variables whose
//! `DEFINE` predicate it satisfies, and finds the greedy longest match from a start position
//! (`ONE ROW PER MATCH` + `AFTER MATCH SKIP PAST LAST ROW`).
//!
//! Variable→predicate evaluation and the streaming/state layer live elsewhere; this module is pure
//! and deterministic so it can be unit-tested without a cluster.

// `BTreeSet` is used only by the test-only reference matchers and the unit tests; the streaming
// matcher walks transitions directly with [`Visited`] guards (a `u64` bitmask for automata of at
// most 64 states, a `HashSet` fallback beyond that).
#[cfg(test)]
use std::collections::BTreeSet;
use std::collections::HashSet;

use crate::executor::error::StreamExecutorResult;

/// Cursor for [`Nfa::next_match`]: the next start position the scan will try. A fresh scan starts
/// at 0; [`Nfa::next_match`] advances it by the `AFTER MATCH SKIP` mode on every match returned
/// (or by one row past a non-matching start), so pulling repeatedly enumerates exactly the match
/// sequence [`Nfa::find_matches_dynamic`] would collect.
#[derive(Debug, Default)]
pub struct MatchScan {
    next_start: usize,
    /// End of the contiguous run of starts, from where this scan began, proven MATCHLESS FOREVER:
    /// their walks found no accept and never reached the boundary, so every path from them died
    /// on a row below it — and those rows are immutable, so no arrival can revive them. The
    /// incremental matcher reads this back and begins its next rescan past them.
    matchless_upto: usize,
}

impl MatchScan {
    pub fn new() -> Self {
        Self::default()
    }

    /// A scan beginning at `start`, with nothing proven yet.
    pub fn starting_at(start: usize) -> Self {
        Self {
            next_start: start,
            matchless_upto: start,
        }
    }

    /// See the `matchless_upto` field.
    pub fn matchless_upto(&self) -> usize {
        self.matchless_upto
    }

    /// The next start the pull loop would explore.
    ///
    /// This is NOT a "fully scanned prefix" marker, and must not be used as one: on a hit the cursor
    /// jumps to `skip.next_pos`, so under `PAST LAST ROW` every start strictly inside the match it
    /// just returned was never evaluated. What it does guarantee, since the budget fix below, is the
    /// narrower property that a start whose walk the budget ABORTED is not advanced past — so the
    /// cursor never claims a verdict the walk did not reach.
    ///
    /// No production caller reads this today; it exists for the emit-on-update port, which needs to
    /// resume a pull, and for the test that pins the abort behaviour.
    pub fn next_start(&self) -> usize {
        self.next_start
    }
}

/// Per-visit budget on NFA walk steps — predicate evaluations AND edges taken (row consumptions
/// and ε-transitions) — shared by every walk of one partition visit (matching, eviction liveness,
/// extension probing). ε-edges are charged deliberately: metering only predicate evaluations left
/// ε-traversal free, so a large NFA could spend arbitrary CPU per metered evaluation and the budget
/// was not actually a CPU bound.
///
/// The matcher is a backtracking DFS whose worst case is exponential in the pattern for
/// pathological shapes (`(a? a? … a? b)` over a run of `a`-rows — the classic catastrophic
/// regex-backtracking family), and [`MAX_PATTERN_NFA_STATES`-style caps bound *space*, not time.
/// [`Memo`] removes the blowup entirely for path-independent patterns; for the rest, this budget
/// is the hard backstop: when it runs out, the walk STOPS — it never fakes a verdict (a fabricated
/// `false` in the liveness walker would evict rows of a live match, the exact bug class the
/// decision-wait semantics exist to prevent). The caller must treat everything undecided
/// conservatively (emit nothing more, evict nothing more, report once, retry next watermark), so a
/// pathological pattern degrades to bounded CPU per visit and an observable report instead of
/// pinning a compute node.
#[derive(Debug)]
pub struct ScanBudget {
    remaining: usize,
    /// Set once the budget runs out; sticky for the rest of the visit.
    pub hit: bool,
}

impl ScanBudget {
    pub fn new(evaluations: usize) -> Self {
        Self {
            remaining: evaluations,
            hit: false,
        }
    }

    /// Account one edge taken by a walk (an ε-transition or a row consumption). Returns `false` —
    /// and latches `hit` — once the budget is spent.
    #[must_use]
    pub fn step(&mut self) -> bool {
        self.charge()
    }

    /// No practical limit — for callers (tests, the collect wrapper) that need the historical
    /// unbounded behaviour.
    pub fn unlimited() -> Self {
        Self::new(usize::MAX)
    }

    /// Account one predicate evaluation. Returns `false` — and latches `hit` — once exhausted.
    fn charge(&mut self) -> bool {
        if self.remaining == 0 {
            self.hit = true;
            return false;
        }
        self.remaining -= 1;
        true
    }
}

/// Per-start `(state, position)` failure memo for the backtracking walkers.
///
/// Soundness: an entry is recorded ONLY for recursion entered through a *consuming* transition —
/// that recursion always starts with a fresh visited-set, so its outcome is context-free — and
/// only when the pattern's verdicts are path-independent (no `DEFINE` slot reads the running label
/// assignment; see the executor's `memoizable` flag). Within one start, a verdict then depends
/// only on `(var, pos)` (`labels.len()` is `pos - start`, so `WITHIN` and the match-start offset
/// are position-determined), so a failed `(state, pos)` fails identically on every re-entry. That
/// re-entry via different consumption prefixes is exactly the exponential blowup; memoizing it
/// makes a start's scan polynomial. ε-level failures are NOT recorded: they can be artifacts of
/// the cycle-cutting visited-set and are not context-free.
struct Memo {
    /// Failure sets indexed by position offset from the memo's start; one [`Visited`] per position
    /// (bitmask for small automata, set fallback beyond).
    failed: Vec<Visited>,
    n_states: usize,
    base: usize,
}

impl Memo {
    fn new(base: usize, _n_rows: usize, n_states: usize) -> Self {
        Self {
            // `slot()` grows on demand; an eager reservation over the whole suffix would malloc
            // O(suffix) per walk INSTANCE — quadratic traffic per rescan — for walks that mostly
            // die within a few positions.
            failed: Vec::new(),
            n_states,
            base,
        }
    }

    fn slot(&mut self, pos: usize) -> &mut Visited {
        let idx = pos - self.base;
        while self.failed.len() <= idx {
            self.failed.push(Visited::new(self.n_states));
        }
        &mut self.failed[idx]
    }

    fn is_failed(&mut self, state: StateId, pos: usize) -> bool {
        self.slot(pos).contains(state)
    }

    fn record_failure(&mut self, state: StateId, pos: usize) {
        self.slot(pos).insert(state);
    }
}

/// Decides whether the row at a physical position can be bound to a pattern variable, given the
/// variables already bound to the earlier rows of the in-progress match. This is how `DEFINE`
/// predicates are evaluated during matching: a predicate may reference the current row, its physical
/// neighbours (`PREV`/`NEXT`), and the running values of other pattern variables (e.g. `A.price`),
/// so membership cannot be precomputed independently of the match path.
pub trait CandidateMatcher {
    /// `labels[k]` is the variable bound to the match's `k`-th row; the candidate is the row at
    /// `pos = match_start + labels.len()`. The returned future is `Send` so the matcher composes
    /// with the (boxed, `Send`) executor stream.
    ///
    /// **Contract for callers:** membership MUST be queried *before* `var` is appended to `labels`,
    /// so `labels` covers only the already-bound rows and never the candidate. Consequently a matcher
    /// that resolves running navigation over `var` itself must treat `var` as the implicit trailing
    /// label. Every caller MUST follow the same rule: [the finder] and [the eviction walker] share one
    /// matcher, so a caller that pushed first would make the two disagree about which rows satisfy a
    /// variable — and eviction would then delete rows the matcher still needs.
    ///
    /// [the finder]: Nfa::find_matches_dynamic
    /// [the eviction walker]: Nfa::reaches_boundary_alive
    fn matches(
        &self,
        var: &str,
        pos: usize,
        labels: &[String],
    ) -> impl std::future::Future<Output = StreamExecutorResult<bool>> + Send;
}

/// A quantifier applied to a sub-pattern. Greedy semantics only (v1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Quantifier {
    /// `*`
    Star,
    /// `+`
    Plus,
    /// `?`
    Question,
    /// `{n}`, `{n,}`, `{n,m}`, `{,m}`. `min` defaults to 0, `max` is `None` for unbounded.
    Range { min: u32, max: Option<u32> },
}

/// The supported v1 subset of a row pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pattern {
    /// A pattern variable, e.g. `A`.
    Var(String),
    /// Concatenation, e.g. `A B C`.
    Concat(Vec<Pattern>),
    /// Alternation, e.g. `A | B`.
    Alt(Vec<Pattern>),
    /// A quantified sub-pattern, e.g. `A+`. The bool is `reluctant` (`A+?` prefers fewer matches).
    Quantified(Box<Pattern>, Quantifier, bool),
    /// `PERMUTE(a, b, ...)` — expanded to the alternation of all orderings.
    Permute(Vec<String>),
}

type StateId = usize;

#[derive(Debug, Clone)]
enum Transition {
    /// An ε-transition (consumes no row).
    Epsilon(StateId),
    /// Consume a row that satisfies pattern variable `var`, moving to `target`.
    OnVar { var: String, target: StateId },
}

/// A Thompson-construction NFA with a single start and single accept state.
#[derive(Debug, Clone)]
pub struct Nfa {
    states: Vec<Vec<Transition>>,
    start: StateId,
    accept: StateId,
    /// Per state: whether `accept` is reachable from it, with every predicate assumed satisfiable.
    /// This is the static half of [`Nfa::may_extend`]: a consuming transition at the row boundary
    /// whose target cannot reach `accept` can never contribute a longer match, no matter what rows
    /// arrive. Computed once at compile with one reverse BFS.
    reach_accept: Vec<bool>,
    /// Fewest rows any accepting path consumes, with every predicate assumed satisfiable — the
    /// shortest match the pattern admits (`0` when it accepts the empty match). A start with fewer
    /// rows than this before the boundary cannot complete, so the finder skips it without a walk
    /// (see [`Nfa::next_match`]). Computed once at compile with one 0-1 BFS.
    min_match_rows: usize,
    /// Most rows any path from `start` consumes, when the automaton is acyclic (no `*`, `+` or
    /// unbounded range) — `None` when it has a cycle and a path can consume without bound. From a
    /// position with MORE rows than this before the boundary, no path can reach the boundary while
    /// still inside the automaton: the position is dead without a walk (see
    /// [`Nfa::reaches_boundary_alive`]). Computed once at compile with one DFS.
    max_match_rows: Option<usize>,
}

impl Nfa {
    /// Compile a [`Pattern`] into an NFA.
    pub fn compile(pattern: &Pattern) -> Self {
        let mut builder = NfaBuilder { states: Vec::new() };
        let frag = builder.build(pattern);
        let reach_accept = Self::compute_reach_accept(&builder.states, frag.accept);
        let max_match_rows = Self::compute_max_match_rows(&builder.states, frag.start);
        let min_match_rows = Self::compute_min_match_rows(&builder.states, frag.start, frag.accept);
        Nfa {
            states: builder.states,
            start: frag.start,
            accept: frag.accept,
            reach_accept,
            min_match_rows,
            max_match_rows,
        }
    }

    /// See the `max_match_rows` field.
    pub fn max_match_rows(&self) -> Option<usize> {
        self.max_match_rows
    }

    /// See the `max_match_rows` field: the longest path from `start` counting consuming edges,
    /// over ANY path (not only accepting ones — a path that dies still consumed its rows), or
    /// `None` if a cycle is reachable. One iterative DFS with a tri-state mark, so a 100k-state
    /// automaton does not recurse.
    fn compute_max_match_rows(states: &[Vec<Transition>], start: StateId) -> Option<usize> {
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum Mark {
            New,
            Active,
            Done,
        }
        let mut mark = vec![Mark::New; states.len()];
        // Longest consumption from each finished state.
        let mut longest = vec![0usize; states.len()];
        let mut stack: Vec<(StateId, usize)> = vec![(start, 0)];
        mark[start] = Mark::Active;
        while let Some(&(s, edge)) = stack.last() {
            if let Some(t) = states[s].get(edge) {
                stack.last_mut().expect("just peeked").1 += 1;
                let next = match t {
                    Transition::Epsilon(next) => *next,
                    Transition::OnVar { target, .. } => *target,
                };
                match mark[next] {
                    // A state still on the DFS path is reachable from itself: a cycle.
                    Mark::Active => return None,
                    Mark::New => {
                        mark[next] = Mark::Active;
                        stack.push((next, 0));
                    }
                    Mark::Done => {}
                }
            } else {
                stack.pop();
                mark[s] = Mark::Done;
                longest[s] = states[s]
                    .iter()
                    .map(|t| match t {
                        Transition::Epsilon(next) => longest[*next],
                        Transition::OnVar { target, .. } => longest[*target] + 1,
                    })
                    .max()
                    .unwrap_or(0);
            }
        }
        Some(longest[start])
    }

    /// See the `min_match_rows` field.
    pub fn min_match_rows(&self) -> usize {
        self.min_match_rows
    }

    /// See the `min_match_rows` field: shortest path from `start` to `accept` where ε-edges cost
    /// nothing and consuming edges cost one row (0-1 BFS, linear in states + transitions).
    fn compute_min_match_rows(
        states: &[Vec<Transition>],
        start: StateId,
        accept: StateId,
    ) -> usize {
        let mut dist = vec![usize::MAX; states.len()];
        let mut queue = std::collections::VecDeque::from([start]);
        dist[start] = 0;
        while let Some(s) = queue.pop_front() {
            let d = dist[s];
            for t in &states[s] {
                let (next, cost) = match t {
                    Transition::Epsilon(next) => (*next, 0),
                    Transition::OnVar { target, .. } => (*target, 1),
                };
                if d + cost < dist[next] {
                    dist[next] = d + cost;
                    if cost == 0 {
                        queue.push_front(next);
                    } else {
                        queue.push_back(next);
                    }
                }
            }
        }
        debug_assert_ne!(
            dist[accept],
            usize::MAX,
            "accept must be reachable by construction"
        );
        dist[accept]
    }

    /// See the `reach_accept` field. Linear in states + transitions (one reverse BFS).
    fn compute_reach_accept(states: &[Vec<Transition>], accept: StateId) -> Vec<bool> {
        let n = states.len();
        let mut rev: Vec<Vec<StateId>> = vec![Vec::new(); n];
        for (s, ts) in states.iter().enumerate() {
            for t in ts {
                match t {
                    Transition::Epsilon(next) => rev[*next].push(s),
                    Transition::OnVar { target, .. } => rev[*target].push(s),
                }
            }
        }
        let mut reach = vec![false; n];
        let mut stack = vec![accept];
        reach[accept] = true;
        while let Some(s) = stack.pop() {
            for &p in &rev[s] {
                if !reach[p] {
                    reach[p] = true;
                    stack.push(p);
                }
            }
        }
        reach
    }

    /// The set of states reachable from `states` via ε-transitions (inclusive). Only the test-only
    /// reference matchers (e.g. [`Nfa::longest_match`]) use the explicit closure; the streaming
    /// matcher walks transitions directly, so this is gated out of the release binary.
    #[cfg(test)]
    fn epsilon_closure(&self, states: impl IntoIterator<Item = StateId>) -> BTreeSet<StateId> {
        let mut closure: BTreeSet<StateId> = BTreeSet::new();
        let mut stack: Vec<StateId> = states.into_iter().collect();
        while let Some(s) = stack.pop() {
            if !closure.insert(s) {
                continue;
            }
            for t in &self.states[s] {
                if let Transition::Epsilon(next) = t {
                    stack.push(*next);
                }
            }
        }
        closure
    }

    /// Greedy longest match starting at `rows[start]`. `rows[i]` is the set of pattern variables
    /// whose `DEFINE` predicate row `i` satisfies. Returns the exclusive end index of the longest
    /// match (so `start..end` are the matched rows), or `None` if no match starts at `start`.
    ///
    /// An empty match (the pattern accepts zero rows, e.g. `A*`) returns `Some(start)`.
    ///
    /// Test-only: the streaming executor matches via [`Nfa::find_matches_dynamic`]. This precomputed
    /// satisfied-set variant is kept as the simple reference the dynamic matcher is checked against,
    /// and to unit-test NFA construction directly. Gated out of the release binary.
    #[cfg(test)]
    pub fn longest_match(&self, rows: &[BTreeSet<String>], start: usize) -> Option<usize> {
        let mut current = self.epsilon_closure([self.start]);
        let mut longest = current.contains(&self.accept).then_some(start);

        let mut pos = start;
        while pos < rows.len() && !current.is_empty() {
            let row = &rows[pos];
            let mut next: BTreeSet<StateId> = BTreeSet::new();
            for &s in &current {
                for t in &self.states[s] {
                    if let Transition::OnVar { var, target } = t
                        && row.contains(var)
                    {
                        next.insert(*target);
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            current = self.epsilon_closure(next);
            pos += 1;
            if current.contains(&self.accept) {
                longest = Some(pos);
            }
        }
        longest
    }
}

/// A single match span over the row sequence: `start..end` (end exclusive) are the matched rows.
/// Test-only: produced by the reference matcher [`Nfa::find_matches`].
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatchSpan {
    pub start: usize,
    pub end: usize,
}

/// Where the scan resumes after a match (the `AFTER MATCH SKIP` strategy).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipMode {
    /// `AFTER MATCH SKIP PAST LAST ROW`: resume past the match's last row (non-overlapping).
    PastLastRow,
    /// `AFTER MATCH SKIP TO NEXT ROW`: resume at the row after the match's first row (overlapping).
    ToNextRow,
    /// `AFTER MATCH SKIP TO FIRST <var>`: resume at the first row labeled `var`.
    ToFirst(String),
    /// `AFTER MATCH SKIP TO LAST <var>`: resume at the last row labeled `var`.
    ToLast(String),
}

/// Why a variable-targeted `AFTER MATCH SKIP` could not resume where the query asked, and which
/// weaker strategy the resume position fell back to. Returned by [`SkipMode::next_pos`] next to the
/// position instead of being reported here: this module stays pure (no error reporter, no executor
/// types), and the executor — which owns the actor's `EvalErrorReport` — decides what to do with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipDegradation {
    /// The target variable is bound to no row of the match, so there is no row to resume at. The
    /// scan resumed past the match's last row, i.e. as `SKIP PAST LAST ROW`.
    TargetAbsent,
    /// The target resolves to the match's own first row, so resuming there would re-find the same
    /// match forever. The scan resumed one row later, i.e. as `SKIP TO NEXT ROW`.
    TargetAtMatchStart,
}

impl SkipDegradation {
    /// The user-facing diagnostic for this degradation under `skip`: the target variable, what could
    /// not be resolved about it, and the strategy actually applied. The *clause* is not repeated here
    /// — the caller supplies it separately via [`SkipMode::clause_name`], so the rendered message
    /// names the mode once (see `report_skip_degradation_once` in `executor.rs`).
    ///
    /// Deliberately carries no row, match or partition identity: the cause is a property of the
    /// query, not of one row, which is what lets the executor report it once per watermark pass
    /// instead of once per match.
    pub fn describe(&self, skip: &SkipMode) -> String {
        // Unreachable for the variable-less modes: only the targeted arms of `next_pos` can produce a
        // degradation. A placeholder rather than an `unwrap` — a diagnostic path must not panic.
        let target = skip.target_var().unwrap_or("?");
        match self {
            SkipDegradation::TargetAbsent => format!(
                "target variable `{target}` is bound to no row of the match, so there is no row to \
                 resume at; the scan resumed past the match's last row instead (degraded to SKIP \
                 PAST LAST ROW)"
            ),
            SkipDegradation::TargetAtMatchStart => format!(
                "target variable `{target}` resolves to the match's own first row, so resuming there \
                 would re-find the same match forever; the scan resumed at the row after the match's \
                 first row instead (degraded to SKIP TO NEXT ROW)"
            ),
        }
    }
}

impl SkipMode {
    /// The `AFTER MATCH SKIP` clause as SQL spells it, *without* the target variable — the mode alone.
    /// `&'static str` so it can be the `name` of an `ExprError::InvalidParam`; the target variable is
    /// named by [`SkipDegradation::describe`] instead, so a rendered diagnostic states the mode once.
    pub fn clause_name(&self) -> &'static str {
        match self {
            SkipMode::PastLastRow => "AFTER MATCH SKIP PAST LAST ROW",
            SkipMode::ToNextRow => "AFTER MATCH SKIP TO NEXT ROW",
            SkipMode::ToFirst(_) => "AFTER MATCH SKIP TO FIRST",
            SkipMode::ToLast(_) => "AFTER MATCH SKIP TO LAST",
        }
    }

    /// The pattern variable a `SKIP TO FIRST|LAST` resumes at; `None` for the variable-less modes,
    /// which are also the only ones that can never degrade.
    pub fn target_var(&self) -> Option<&str> {
        match self {
            SkipMode::PastLastRow | SkipMode::ToNextRow => None,
            SkipMode::ToFirst(var) | SkipMode::ToLast(var) => Some(var),
        }
    }

    /// The position the scan resumes at after a match spanning `[start, end)` with per-row `labels`
    /// (`labels[i]` is the variable bound to `rows[start + i]`), plus a [`SkipDegradation`] when that
    /// position is not the one the query asked for. Always returns `> start` so the scan makes
    /// progress.
    ///
    /// Two cases have no valid resume row, and both are data-dependent — the same query degrades or
    /// not depending on which rows arrive:
    ///
    ///  * the target variable is bound to no row of this match (`(a b?)` matching only `a`, with
    ///    `SKIP TO LAST b`), so the resume position falls back to the match end — silently becoming
    ///    `SKIP PAST LAST ROW`;
    ///  * the target resolves to the match's own first row (`SKIP TO FIRST` of the pattern's leading
    ///    variable), which would re-find the same match forever, so it is clamped to `start + 1` —
    ///    silently becoming `SKIP TO NEXT ROW`.
    ///
    /// The SQL standard prescribes a runtime error for both (Oracle raises ORA-62511 / ORA-62512;
    /// Flink likewise). This implementation deliberately keeps the degradation and **reports** it
    /// instead of raising it: an error here would abort the actor over a data-dependent condition,
    /// and since the materialized view is already committed, every recovery attempt would replay the
    /// same rows and die again — a recoverable query turned into a crash loop. No RisingWave
    /// streaming operator fails an actor for a data-dependent condition; every hard error in this
    /// operator is a contract or plan violation (non-append-only input, an unknown slot kind), which
    /// recovery cannot fix either way. So the degradation is made *visible* rather than fatal: the
    /// executor routes the returned diagnostic to the actor's `EvalErrorReport`, the same surface
    /// expression evaluation errors already use (the `stream_expr_error` log and the
    /// `user_compute_error` metric). See `report_skip_degradation_once` in `executor.rs` for how the
    /// message is rendered — including the surface's fixed log prefix — and for the volume policy.
    pub fn next_pos(
        &self,
        start: usize,
        end: usize,
        labels: &[String],
    ) -> (usize, Option<SkipDegradation>) {
        // Resolve a variable-targeted skip: `found` is the target's index within `labels`. Only these
        // modes can degrade; the variable-less ones always have a valid resume row.
        let resolve = |found: Option<usize>| match found {
            // Index 0 is the match's own first row, so resuming there makes no progress.
            Some(0) => (start + 1, Some(SkipDegradation::TargetAtMatchStart)),
            Some(j) => (start + j, None),
            None => (end.max(start + 1), Some(SkipDegradation::TargetAbsent)),
        };
        match self {
            SkipMode::PastLastRow => (end.max(start + 1), None),
            SkipMode::ToNextRow => (start + 1, None),
            SkipMode::ToFirst(var) => resolve(labels.iter().position(|l| l == var)),
            SkipMode::ToLast(var) => resolve(labels.iter().rposition(|l| l == var)),
        }
    }
}

impl Nfa {
    /// Find all matches over `rows` under `ONE ROW PER MATCH` with the given `AFTER MATCH SKIP`
    /// strategy: scan left to right; at each position take the greedy longest match; on a non-empty
    /// match, record it and resume per `skip`; otherwise advance by one row.
    ///
    /// Empty matches (a pattern that accepts zero rows, e.g. `A*` on a non-matching row) are not
    /// emitted and advance the scan by one, so the scan always terminates.
    ///
    /// Test-only reference matcher (see [`Nfa::longest_match`]); gated out of the release binary.
    #[cfg(test)]
    pub fn find_matches(&self, rows: &[BTreeSet<String>], skip: &SkipMode) -> Vec<MatchSpan> {
        let mut matches = Vec::new();
        let mut i = 0;
        while i < rows.len() {
            if let Some(end) = self.longest_match(rows, i)
                && end > i
            {
                matches.push(MatchSpan { start: i, end });
                // `find_matches` is label-less; the variable-targeted skips resolve like
                // `PAST LAST ROW` here. `find_matches_labeled` applies them precisely.
                i = match skip {
                    SkipMode::ToNextRow => i + 1,
                    _ => end,
                };
            } else {
                i += 1;
            }
        }
        matches
    }
}

/// A match span together with the pattern variable assigned to each matched row.
/// `labels[i]` is the variable that `rows[start + i]` was matched as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabeledMatch {
    pub start: usize,
    pub end: usize,
    pub labels: Vec<String>,
}

/// Visited-state guard for one traversal position of the dynamic matcher. ε-transitions keep the
/// position, so each consumed row starts a fresh set (see [`Nfa::walk`]); these sets are opened
/// O(rows × branches) times per partition visit, so their allocation cost matters. The
/// common case — an automaton with at most 64 states — is a single `u64` bitmask (no allocation,
/// membership is a bit test); larger automata (deep `PERMUTE` expansions) fall back to a `HashSet`.
enum Visited {
    Small(u64),
    Large(HashSet<StateId>),
}

impl Visited {
    fn new(n_states: usize) -> Self {
        if n_states <= 64 {
            Visited::Small(0)
        } else {
            Visited::Large(HashSet::new())
        }
    }

    /// Marks `s` visited; returns whether it was newly inserted (mirrors `HashSet::insert`).
    fn insert(&mut self, s: StateId) -> bool {
        match self {
            Visited::Small(bits) => {
                let mask = 1u64 << s;
                let newly = *bits & mask == 0;
                *bits |= mask;
                newly
            }
            Visited::Large(set) => set.insert(s),
        }
    }

    fn remove(&mut self, s: StateId) {
        match self {
            Visited::Small(bits) => *bits &= !(1u64 << s),
            Visited::Large(set) => {
                set.remove(&s);
            }
        }
    }

    fn contains(&self, s: StateId) -> bool {
        match self {
            Visited::Small(bits) => *bits & (1u64 << s) != 0,
            Visited::Large(set) => set.contains(&s),
        }
    }

    /// Empties the set for reuse as a fresh scope (keeps the `HashSet` fallback's allocation).
    fn clear(&mut self) {
        match self {
            Visited::Small(bits) => *bits = 0,
            Visited::Large(set) => set.clear(),
        }
    }
}

impl Nfa {
    /// Greedy longest match starting at `rows[start]`, returning the per-row variable assignment
    /// along the chosen accepting path (the variable each consumed row was matched as). This is
    /// what `MEASURES` navigation (`FIRST`/`LAST`), `CLASSIFIER()`, and aggregates over matched
    /// rows consume. Returns `(end, labels)` where `labels.len() == end - start`, or `None`.
    #[cfg(test)]
    pub fn longest_match_labeled(
        &self,
        rows: &[BTreeSet<String>],
        start: usize,
    ) -> Option<(usize, Vec<String>)> {
        let mut visited: HashSet<(StateId, usize)> = HashSet::new();
        self.longest_from(rows, self.start, start, &mut visited)
    }

    /// Recursive longest-accepting-path search. `visited` guards against ε-cycles on the current
    /// path (it tracks `(state, pos)` and is unwound on backtrack). Among continuations the one
    /// reaching the furthest `end` wins; ties keep the first in transition order, making the label
    /// assignment deterministic.
    #[cfg(test)]
    fn longest_from(
        &self,
        rows: &[BTreeSet<String>],
        state: StateId,
        pos: usize,
        visited: &mut HashSet<(StateId, usize)>,
    ) -> Option<(usize, Vec<String>)> {
        if !visited.insert((state, pos)) {
            return None;
        }
        let mut best: Option<(usize, Vec<String>)> =
            (state == self.accept).then(|| (pos, Vec::new()));
        for t in &self.states[state] {
            let candidate = match t {
                Transition::Epsilon(next) => self.longest_from(rows, *next, pos, visited),
                Transition::OnVar { var, target } => {
                    if pos < rows.len() && rows[pos].contains(var) {
                        self.longest_from(rows, *target, pos + 1, visited).map(
                            |(end, mut labels)| {
                                labels.insert(0, var.clone());
                                (end, labels)
                            },
                        )
                    } else {
                        None
                    }
                }
            };
            if let Some((end, labels)) = candidate
                && best.as_ref().is_none_or(|(b, _)| end > *b)
            {
                best = Some((end, labels));
            }
        }
        visited.remove(&(state, pos));
        best
    }

    /// Like [`Nfa::find_matches`] but returns each match with its per-row variable labels.
    /// Test-only reference matcher; the streaming executor uses [`Nfa::find_matches_dynamic`].
    #[cfg(test)]
    pub fn find_matches_labeled(
        &self,
        rows: &[BTreeSet<String>],
        skip: &SkipMode,
    ) -> Vec<LabeledMatch> {
        let mut matches = Vec::new();
        let mut i = 0;
        while i < rows.len() {
            if let Some((end, labels)) = self.longest_match_labeled(rows, i)
                && end > i
            {
                let start = i;
                // Test-only matcher: the diagnostic is dropped (there is no actor to report to).
                (i, _) = skip.next_pos(start, end, &labels);
                matches.push(LabeledMatch { start, end, labels });
            } else {
                i += 1;
            }
        }
        matches
    }

    /// Like `find_matches_labeled`, but membership is decided by an async [`CandidateMatcher`]
    /// instead of precomputed satisfied-sets, so `DEFINE` predicates with row-pattern navigation can
    /// be evaluated against the running match. `n_rows` is the number of (sorted) rows to scan. This
    /// is the only matcher the streaming executor uses.
    pub async fn find_matches_dynamic(
        &self,
        n_rows: usize,
        matcher: &(impl CandidateMatcher + Sync),
        skip: &SkipMode,
    ) -> StreamExecutorResult<Vec<LabeledMatch>> {
        let mut matches = Vec::new();
        let mut scan = MatchScan::new();
        let mut budget = ScanBudget::unlimited();
        while let Some(m) = self
            .next_match(&mut scan, n_rows, matcher, skip, &mut budget, false)
            .await?
        {
            matches.push(m);
        }
        Ok(matches)
    }

    /// Pull the next match at or after `scan`'s cursor, advancing the cursor by the skip mode.
    /// Returns `None` once the cursor passes `n_rows`.
    ///
    /// This is the streaming form of [`Nfa::find_matches_dynamic`], which is a thin collect over
    /// it. The executor's emit loop pulls instead of collecting so that stopping — at the first
    /// boundary match that must be held for maximality — stops the *scan*, not just the emission:
    /// nothing past the held match is computed (it would be recomputed from scratch on the next
    /// watermark anyway) and at most one match is resident at a time. Collecting is worst-case
    /// quadratic in live rows: under an overlapping skip mode a greedy `(a+)` over `n` qualifying
    /// rows yields `n` matches whose label vectors sum to `O(n^2)` strings, all materialized before
    /// the first one is examined.
    pub async fn next_match(
        &self,
        scan: &mut MatchScan,
        n_rows: usize,
        matcher: &(impl CandidateMatcher + Sync),
        skip: &SkipMode,
        budget: &mut ScanBudget,
        memoize: bool,
    ) -> StreamExecutorResult<Option<LabeledMatch>> {
        while scan.next_start < n_rows && !budget.hit {
            let i = scan.next_start;
            // Fewer rows than the shortest match before the boundary: no accepting path exists
            // from `i` within `n_rows`, which is exactly the verdict a walk would reach after
            // consuming its way to the boundary — so skip the walk and take the verdict. This is
            // what keeps a rescan over a long pending run of a chain pattern (`a{600}`) from
            // walking every start to the boundary, Θ(k²) per rescan. (Not a matchless start: the
            // walk it stands in for is blocked at the boundary, and more rows may complete it.)
            if n_rows - i < self.min_match_rows() {
                scan.next_start += 1;
                continue;
            }
            // The memo is per START: within one start a verdict depends only on `(var, pos)`
            // (given path-independence), so it must not leak across starts, where `labels.len()`
            // differs for the same position.
            let mut memo = memoize.then(|| Memo::new(i, n_rows, self.states.len()));
            let mut reached_boundary = false;
            let found = self
                .walk(
                    Goal::Accept {
                        n_rows,
                        reached_boundary: &mut reached_boundary,
                    },
                    i,
                    matcher,
                    budget,
                    memo.as_mut(),
                )
                .await?;
            let found_empty = match found {
                Some((end, labels)) if end > i => {
                    // The diagnostic is dropped here on purpose: the executor recomputes the
                    // resume position for the matches it actually *emits* and reports from there,
                    // so a match that this scan finds but the emit path holds back or skips is not
                    // reported twice (nor reported at all until it is emitted).
                    (scan.next_start, _) = skip.next_pos(i, end, &labels);
                    return Ok(Some(LabeledMatch {
                        start: i,
                        end,
                        labels,
                    }));
                }
                Some(_) => true,
                None => false,
            };
            // Only advance past a start with a real verdict. The walk returns `None` for two
            // different reasons — "no match from here" and "the budget died mid-walk, no
            // verdict" — and advancing on the second leaves the cursor claiming a verdict the walk
            // never reached: `(a b) | a` over two `a` rows with a budget of 1 dies inside start 0
            // before ever trying the second alternative, which matches there.
            //
            // Inert for this operator (the loop condition already stops on `budget.hit`, and nothing
            // here reads the cursor afterwards); it matters to a caller that resumes a pull.
            if budget.hit {
                break;
            }
            // A start whose walk found no accept and never reached the boundary is matchless
            // FOREVER: every path from it died on a row below the boundary, and those rows are
            // immutable, so no arrival can revive it. Record the contiguous run of such starts (see
            // `MatchScan::matchless_upto`) — the finder's cross-visit memory, the counterpart of
            // the freeze's proven-dead prefix: a broken run of `r` rows costs its Θ(r²) once,
            // amortised across visits, instead of on every rescan forever. An empty match proves
            // nothing about the other paths (the walk stopped at its first verdict), so it does
            // not count.
            if !found_empty && !reached_boundary && scan.matchless_upto == i {
                scan.matchless_upto = i + 1;
            }
            scan.next_start += 1;
        }
        Ok(None)
    }

    /// Whether a match starting at `pos` is still *live* at the safe boundary `n_rows`: there exists
    /// a path that consumes the safe rows `pos..n_rows` and reaches the boundary while still inside
    /// the automaton (not yet accepted), so a future row could extend it into a complete match. Used
    /// to evict rows that can no longer be part of any match.
    ///
    /// This is strictly stronger than "can `pos` begin the pattern": for `(a b)` over `[a, x]` where
    /// `x` matches neither, the `a` *can* begin the pattern, but every path dies on `x` before the
    /// boundary, so the start is dead and must be evictable. A lone `[a]` (boundary right after `a`),
    /// in contrast, is kept because a future `b` may still complete it.
    pub async fn reaches_boundary_alive(
        &self,
        pos: usize,
        n_rows: usize,
        matcher: &(impl CandidateMatcher + Sync),
        budget: &mut ScanBudget,
        memoize: bool,
    ) -> StreamExecutorResult<bool> {
        // Acyclic automaton: no path consumes more than `max_match_rows` rows, so with more rows
        // than that before the boundary no path can reach it — dead, without a walk. This is what
        // makes freezing a long chain match (`a{600}`) cheap: every position of its region has the
        // rest of the match, and more, ahead of it. (With EXACTLY that many rows the walk runs: a
        // match ending on the boundary keeps its start alive.)
        if let Some(max) = self.max_match_rows
            && n_rows - pos > max
        {
            return Ok(false);
        }
        let mut memo = memoize.then(|| Memo::new(pos, n_rows, self.states.len()));
        Ok(self
            .walk(
                Goal::Boundary { n_rows },
                pos,
                matcher,
                budget,
                memo.as_mut(),
            )
            .await?
            .is_some())
    }

    /// Whether the automaton is a fixed-length linear chain: every state has at most one
    /// outgoing transition (plain concatenations like `(a b c)` — no alternation, no
    /// quantifiers, no PERMUTE). For such patterns an accepted match consumed the only path there
    /// is, so no more-preferred extension can exist and [`Nfa::may_extend`] is statically `false`
    /// — the emission gate can skip the probe entirely.
    pub fn is_linear(&self) -> bool {
        self.states.iter().all(|ts| ts.len() <= 1)
    }

    /// Whether the finder's *preferred* result for the match starting at `start` could change if
    /// more rows arrived past the boundary `end`.
    ///
    /// The finder ([`Nfa::next_match`]) returns the first accepting path in transition
    /// order: greedy quantifiers try their consume edge before their exit edge, reluctant ones the
    /// reverse, and ordered alternation tries branches as listed. A lower-priority path can
    /// therefore NEVER override an accepting higher-priority one, no matter what rows arrive — for
    /// `PATTERN (A (B | B C))` the first-listed `B` alternative wins even if a `C` shows up later.
    /// So the question is not "could any NFA path consume more" but "could a path the finder
    /// prefers over the current result become accepting".
    ///
    /// This walk mirrors the finder's own traversal exactly and stops at its first accept — the
    /// preferred result. It answers `true` iff, strictly before that accept in preference order,
    /// some consuming transition was blocked by the row boundary while its target can still reach
    /// `accept` ([`Nfa::reach_accept`], unknown rows assumed satisfiable): exactly the paths that
    /// arriving rows could turn into a more-preferred accepting result. Everything explored after
    /// the accept is lower-priority and irrelevant.
    ///
    /// `false` means the preferred result is **terminal**: it cannot change, so a boundary match is
    /// final and must be emitted — holding it would starve an idle partition forever (the frontier
    /// recompute finds neither a future row nor, without `WITHIN`, a deadline, and drops the
    /// partition). `true` means the standard maximality wait applies.
    pub async fn may_extend(
        &self,
        start: usize,
        end: usize,
        matcher: &(impl CandidateMatcher + Sync),
        budget: &mut ScanBudget,
        memoize: bool,
    ) -> StreamExecutorResult<bool> {
        let mut blocked = false;
        let mut memo = memoize.then(|| Memo::new(start, end, self.states.len()));
        let accepted = self
            .walk(
                Goal::AcceptOrBlocked {
                    end,
                    blocked: &mut blocked,
                },
                start,
                matcher,
                budget,
                memo.as_mut(),
            )
            .await?
            .is_some();
        // A spent budget means the walk may have stopped before the preferred accept: `blocked`
        // then under-approximates, and the only safe answer is "may extend" (hold — never emit on
        // partial information).
        if budget.hit {
            return Ok(true);
        }
        // Called for a match the finder produced over these same rows, so an accepting path within
        // `end` exists and `accepted` holds; if a caller ever probes a non-match, every explored
        // path is by definition higher-priority than the (absent) result, so `blocked` is still
        // the right answer.
        debug_assert!(accepted, "may_extend probed a non-accepting span");
        Ok(blocked)
    }

    /// The one traversal behind the finder, the liveness check and the extension probe: a
    /// depth-first search from `self.start` at row `start_pos`, in transition order, with `DEFINE`
    /// predicates evaluated over the running match (`path`, the labels bound so far, is threaded to
    /// the matcher). Returns the row position at which `goal` was met together with the labels of
    /// the path that met it; `None` is "no such path" — or, with `budget.hit` latched, "undecided".
    ///
    /// Iterative, over an explicit heap stack. The recursive walkers this replaced — boxed
    /// `async_recursion` frames, polled through the real thread stack once per consumed row and
    /// once per ε-edge — needed a hard depth cap to avoid overflowing it, and the cap made any
    /// match spanning more than a couple of hundred rows permanently undecidable: unlike the
    /// budget it did not reset between visits, so every refresh died at the same depth. Every push
    /// is charged to the budget, so the stack holds at most `budget` frames — at the executor's
    /// 2^20 steps, about 32 megabytes of 32-byte frames in the worst case — and the budget is the
    /// only bound on a walk.
    ///
    /// The discipline, identical for all three goals:
    /// - transitions are tried in the order the builder emitted them, and the FIRST verdict wins —
    ///   greedy quantifiers list their consume edge before their exit edge, reluctant ones the
    ///   reverse, alternation as written;
    /// - a [`Visited`] scope per row position cuts ε-cycles: ε-edges keep the position and the
    ///   scope, a consumed row opens a fresh one, so distinct label assignments may reach the same
    ///   state at the next row;
    /// - the budget is charged once per predicate evaluation and once per edge taken, and a spent
    ///   budget ends the walk without a verdict and without recording anything;
    /// - failures are memoized only at consumption boundaries — a consumed frame starts with a fresh
    ///   scope, so its outcome is context-free — and never for a budget-aborted walk, which would
    ///   turn a transient abort into a permanent wrong verdict (see [`Memo`]).
    async fn walk(
        &self,
        mut goal: Goal<'_>,
        start_pos: usize,
        matcher: &(impl CandidateMatcher + Sync),
        budget: &mut ScanBudget,
        mut memo: Option<&mut Memo>,
    ) -> StreamExecutorResult<Option<(usize, Vec<String>)>> {
        let n_states = self.states.len();
        let mut path: Vec<String> = Vec::new();
        // One visited scope per consumption level; `scopes[depth]` is the live one. A walk opens a
        // scope per consumed row, so they are cleared and reused rather than reallocated.
        let mut scopes: Vec<Visited> = vec![Visited::new(n_states)];
        let mut depth = 0usize;
        let mut stack = vec![Frame::new(self.start, start_pos, false)];

        loop {
            let Some(top) = stack.last_mut() else {
                return Ok(None);
            };
            if !top.entered {
                top.entered = true;
                // A walk entered with an already-spent budget aborts without deciding anything
                // (the caller must treat everything undecided conservatively; see `ScanBudget`).
                // Exhaustion MID-walk never reaches this point: every `step`/`charge` that
                // latches `hit` returns from `walk` directly below — which is what lets
                // `pop_failed` record a failure unconditionally.
                if budget.hit {
                    return Ok(None);
                }
                let (state, pos) = (top.state, top.pos);
                match goal.enter(self, state, pos) {
                    Enter::Verdict => return Ok(Some((pos, path))),
                    Enter::Dead => {
                        Self::pop_failed(
                            &mut stack,
                            &mut scopes,
                            &mut depth,
                            &mut path,
                            memo.as_deref_mut(),
                            budget,
                        );
                        continue;
                    }
                    Enter::Explore => {}
                }
                if scopes[depth].insert(state) {
                    top.inserted = true;
                } else {
                    Self::pop_failed(
                        &mut stack,
                        &mut scopes,
                        &mut depth,
                        &mut path,
                        memo.as_deref_mut(),
                        budget,
                    );
                    continue;
                }
            }
            let (state, pos) = (top.state, top.pos);
            let Some(t) = self.states[state].get(top.next_edge) else {
                // Every transition tried and none met the goal: this frame fails.
                Self::pop_failed(
                    &mut stack,
                    &mut scopes,
                    &mut depth,
                    &mut path,
                    memo.as_deref_mut(),
                    budget,
                );
                continue;
            };
            top.next_edge += 1;
            match t {
                Transition::Epsilon(next) => {
                    if !budget.step() {
                        return Ok(None);
                    }
                    stack.push(Frame::new(*next, pos, false));
                }
                Transition::OnVar { var, target } => {
                    if !goal.may_consume(self, *target, pos) {
                        continue;
                    }
                    if !budget.charge() {
                        return Ok(None);
                    }
                    if !matcher.matches(var, pos, &path).await? {
                        continue;
                    }
                    // Consumption boundary: the frame pushed below starts with a fresh visited
                    // scope, so its outcome is context-free — the failure memo is checked here and
                    // recorded on its exit, never at ε-level, where a failure can be a cycle-cut
                    // artifact.
                    if memo
                        .as_deref_mut()
                        .is_some_and(|m| m.is_failed(*target, pos + 1))
                    {
                        continue;
                    }
                    if !budget.step() {
                        return Ok(None);
                    }
                    path.push(var.clone());
                    depth += 1;
                    if depth == scopes.len() {
                        scopes.push(Visited::new(n_states));
                    } else {
                        scopes[depth].clear();
                    }
                    stack.push(Frame::new(*target, pos + 1, true));
                }
            }
        }
    }

    /// Pop the top frame as a failure: unmark its state in its scope, and if it was entered by
    /// consuming a row, close that scope, drop its label, and memoize the failure.
    ///
    /// A budget-aborted walk never gets here — every latch site returns from `walk` directly — so
    /// the failure being recorded is always a proven one, by construction rather than by a guard.
    fn pop_failed(
        stack: &mut Vec<Frame>,
        scopes: &mut [Visited],
        depth: &mut usize,
        path: &mut Vec<String>,
        memo: Option<&mut Memo>,
        budget: &ScanBudget,
    ) {
        debug_assert!(
            !budget.hit,
            "a spent budget returns from `walk`; it never pops a frame"
        );
        // Callers hold the top frame, so the stack is non-empty; an empty stack here would be a
        // walk-invariant violation, and letting the loop end on it (no verdict) beats panicking an
        // actor over it.
        let Some(frame) = stack.pop() else {
            return;
        };
        if frame.inserted {
            scopes[*depth].remove(frame.state);
        }
        if frame.consumed {
            debug_assert!(*depth > 0, "a consumed frame always opened a scope");
            *depth = depth.saturating_sub(1);
            path.pop();
            if let Some(m) = memo {
                m.record_failure(frame.state, frame.pos);
            }
        }
    }
}

/// What a [`Nfa::walk`] is looking for. The three questions the executor asks of the automaton share
/// one traversal and differ only in when a frame is a verdict and in what a consuming edge may do at
/// the row boundary.
enum Goal<'a> {
    /// The first accepting path in preference order — the match finder ([`Nfa::next_match`]).
    /// Consuming edges stop at `n_rows`. `reached_boundary` records whether any frame was entered
    /// AT `n_rows`: a walk that finds no accept and never got there died entirely on rows below
    /// the boundary, which no arrival can change — the start is matchless forever.
    Accept {
        n_rows: usize,
        reached_boundary: &'a mut bool,
    },
    /// Whether some path consumes the safe suffix up to `n_rows` while still inside the automaton
    /// ([`Nfa::reaches_boundary_alive`]). Reaching `n_rows` is the verdict; reaching `accept` before
    /// it is a complete (already-finalized) match, not a live partial one, and fails that path.
    Boundary { n_rows: usize },
    /// The first accept, bounded by `end` ([`Nfa::may_extend`]). A consuming edge at `end` cannot
    /// be evaluated — the row does not exist yet; if its target can still reach `accept`
    /// ([`Nfa::reach_accept`]) it is recorded in `blocked`, since a future row could fire it and
    /// produce a more-preferred result than any accept found later in the walk.
    AcceptOrBlocked { end: usize, blocked: &'a mut bool },
}

/// A frame's verdict on entry.
enum Enter {
    /// The goal is met at this frame.
    Verdict,
    /// This path can no longer meet the goal.
    Dead,
    /// Keep walking.
    Explore,
}

impl Goal<'_> {
    fn enter(&mut self, nfa: &Nfa, state: StateId, pos: usize) -> Enter {
        if let Goal::Accept {
            n_rows,
            reached_boundary,
        } = self
            && pos == *n_rows
        {
            **reached_boundary = true;
        }
        match self {
            // The single accept state is terminal: reaching it completes the match here.
            Goal::Accept { .. } | Goal::AcceptOrBlocked { .. } if state == nfa.accept => {
                Enter::Verdict
            }
            Goal::Boundary { n_rows } if pos == *n_rows => Enter::Verdict,
            Goal::Boundary { .. } if state == nfa.accept => Enter::Dead,
            _ => Enter::Explore,
        }
    }

    /// Whether a consuming edge into `target` may be evaluated at `pos`.
    fn may_consume(&mut self, nfa: &Nfa, target: StateId, pos: usize) -> bool {
        match self {
            Goal::Accept { n_rows, .. } | Goal::Boundary { n_rows } => pos < *n_rows,
            Goal::AcceptOrBlocked { end, blocked } => {
                if pos == *end {
                    if nfa.reach_accept[target] {
                        **blocked = true;
                    }
                    false
                } else {
                    true
                }
            }
        }
    }
}

/// One frame of the explicit walk stack: the state under exploration at row `pos`, and how far
/// through its transitions the walk has got.
struct Frame {
    state: StateId,
    pos: usize,
    /// Index of the next transition of `state` to try.
    next_edge: usize,
    /// Entered through a consuming transition: it opened a [`Visited`] scope and pushed a label,
    /// both undone on exit, and its failure is memoizable.
    consumed: bool,
    /// The entry checks have run (goal verdict, budget, visited).
    entered: bool,
    /// The frame marked `state` in its scope, and must unmark it on exit.
    inserted: bool,
}

impl Frame {
    fn new(state: StateId, pos: usize, consumed: bool) -> Self {
        Self {
            state,
            pos,
            next_edge: 0,
            consumed,
            entered: false,
            inserted: false,
        }
    }
}

/// A sub-NFA fragment with one entry and one exit state.
struct Fragment {
    start: StateId,
    accept: StateId,
}

struct NfaBuilder {
    states: Vec<Vec<Transition>>,
}

impl NfaBuilder {
    fn new_state(&mut self) -> StateId {
        self.states.push(Vec::new());
        self.states.len() - 1
    }

    fn add_epsilon(&mut self, from: StateId, to: StateId) {
        self.states[from].push(Transition::Epsilon(to));
    }

    fn add_on_var(&mut self, from: StateId, var: String, to: StateId) {
        self.states[from].push(Transition::OnVar { var, target: to });
    }

    /// LOCKSTEP: the per-construct state counts below are mirrored by `estimate_nfa_states` in
    /// `frontend/src/binder/relation/match_recognize.rs`, which rejects a pattern whose expansion
    /// would exhaust memory here. No test can span the two crates, so changing how many states a
    /// construct allocates requires updating that estimator in the same change.
    fn build(&mut self, pattern: &Pattern) -> Fragment {
        match pattern {
            Pattern::Var(v) => {
                let start = self.new_state();
                let accept = self.new_state();
                self.add_on_var(start, v.clone(), accept);
                Fragment { start, accept }
            }
            Pattern::Concat(parts) => {
                if parts.is_empty() {
                    let s = self.new_state();
                    return Fragment {
                        start: s,
                        accept: s,
                    };
                }
                let first = self.build(&parts[0]);
                let mut accept = first.accept;
                for p in &parts[1..] {
                    let frag = self.build(p);
                    self.add_epsilon(accept, frag.start);
                    accept = frag.accept;
                }
                Fragment {
                    start: first.start,
                    accept,
                }
            }
            Pattern::Alt(alts) => {
                let start = self.new_state();
                let accept = self.new_state();
                for a in alts {
                    let frag = self.build(a);
                    self.add_epsilon(start, frag.start);
                    self.add_epsilon(frag.accept, accept);
                }
                Fragment { start, accept }
            }
            Pattern::Quantified(inner, q, reluctant) => self.build_quantified(inner, q, *reluctant),
            Pattern::Permute(vars) => {
                // PERMUTE expands to the alternation of every ordering of the variables.
                let alts: Vec<Pattern> = permutations(vars)
                    .into_iter()
                    .map(|order| Pattern::Concat(order.into_iter().map(Pattern::Var).collect()))
                    .collect();
                self.build(&Pattern::Alt(alts))
            }
        }
    }

    /// LOCKSTEP with `estimate_nfa_states` — see [`Self::build`].
    fn build_quantified(&mut self, inner: &Pattern, q: &Quantifier, reluctant: bool) -> Fragment {
        match q {
            Quantifier::Star => self.build_star(inner, reluctant),
            Quantifier::Plus => {
                // inner followed by inner* (the repetition carries the reluctant preference)
                let first = self.build(inner);
                let star = self.build_star(inner, reluctant);
                self.add_epsilon(first.accept, star.start);
                Fragment {
                    start: first.start,
                    accept: star.accept,
                }
            }
            Quantifier::Question => {
                let start = self.new_state();
                let accept = self.new_state();
                let frag = self.build(inner);
                // Greedy orders take-the-inner before skip; reluctant orders skip first.
                if reluctant {
                    self.add_epsilon(start, accept); // skip first
                    self.add_epsilon(start, frag.start);
                } else {
                    self.add_epsilon(start, frag.start);
                    self.add_epsilon(start, accept); // skip
                }
                self.add_epsilon(frag.accept, accept);
                Fragment { start, accept }
            }
            Quantifier::Range { min, max } => self.build_range(inner, *min, *max, reluctant),
        }
    }

    /// LOCKSTEP with `estimate_nfa_states` — see [`Self::build`].
    fn build_star(&mut self, inner: &Pattern, reluctant: bool) -> Fragment {
        let start = self.new_state();
        let accept = self.new_state();
        let frag = self.build(inner);
        // The matcher takes the first accepting path in edge order. Greedy emits the consume/loop
        // edge before the exit edge (longest match first); reluctant emits the exit edge first
        // (shortest match first).
        if reluctant {
            self.add_epsilon(start, accept); // zero occurrences first
            self.add_epsilon(start, frag.start);
            self.add_epsilon(frag.accept, accept); // exit before loop
            self.add_epsilon(frag.accept, frag.start);
        } else {
            self.add_epsilon(start, frag.start);
            self.add_epsilon(start, accept); // zero occurrences
            self.add_epsilon(frag.accept, frag.start); // loop
            self.add_epsilon(frag.accept, accept);
        }
        Fragment { start, accept }
    }

    /// LOCKSTEP with `estimate_nfa_states` — see [`Self::build`]. The bounds reaching here are already
    /// capped at bind time precisely because this expansion is eager.
    fn build_range(
        &mut self,
        inner: &Pattern,
        min: u32,
        max: Option<u32>,
        reluctant: bool,
    ) -> Fragment {
        // Expand to `min` mandatory copies followed by either `*` (unbounded) or `max-min`
        // optional copies.
        let mut parts: Vec<Pattern> = Vec::new();
        for _ in 0..min {
            parts.push(inner.clone());
        }
        match max {
            None => parts.push(Pattern::Quantified(
                Box::new(inner.clone()),
                Quantifier::Star,
                reluctant,
            )),
            Some(max) => {
                for _ in min..max {
                    parts.push(Pattern::Quantified(
                        Box::new(inner.clone()),
                        Quantifier::Question,
                        reluctant,
                    ));
                }
            }
        }
        self.build(&Pattern::Concat(parts))
    }
}

/// All orderings of `items`. Only used for `PERMUTE`, which has a small arity in practice.
fn permutations(items: &[String]) -> Vec<Vec<String>> {
    if items.is_empty() {
        return vec![vec![]];
    }
    let mut out = Vec::new();
    for i in 0..items.len() {
        let mut rest = items.to_vec();
        let head = rest.remove(i);
        for mut tail in permutations(&rest) {
            tail.insert(0, head.clone());
            out.push(tail);
        }
    }
    out
}

/// A [`CandidateMatcher`] backed by precomputed satisfied-sets — the dynamic driver should then
/// agree with [`Nfa::find_matches_labeled`]. Test-only, but `pub(crate)` so the sibling
/// `incremental` module's differential-oracle tests can reuse the exact same reference matcher as
/// `nfa`'s own tests.
#[cfg(test)]
pub(crate) struct SetMatcher {
    rows: Vec<BTreeSet<String>>,
}

#[cfg(test)]
impl SetMatcher {
    pub(crate) fn new(rows: Vec<BTreeSet<String>>) -> Self {
        Self { rows }
    }
}

#[cfg(test)]
impl CandidateMatcher for SetMatcher {
    async fn matches(
        &self,
        var: &str,
        pos: usize,
        _labels: &[String],
    ) -> StreamExecutorResult<bool> {
        Ok(self.rows[pos].contains(var))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(s: &str) -> Pattern {
        Pattern::Var(s.to_owned())
    }

    /// Build a row sequence from a string where each char names the single variable that row
    /// satisfies, e.g. "abc" -> [{a}, {b}, {c}].
    fn rows(seq: &str) -> Vec<BTreeSet<String>> {
        seq.chars()
            .map(|c| BTreeSet::from([c.to_string()]))
            .collect()
    }

    #[test]
    fn concat_exact() {
        // A B C
        let p = Pattern::Concat(vec![vars("a"), vars("b"), vars("c")]);
        let nfa = Nfa::compile(&p);
        assert_eq!(nfa.longest_match(&rows("abc"), 0), Some(3));
        assert_eq!(nfa.longest_match(&rows("abx"), 0), None);
        assert_eq!(nfa.longest_match(&rows("ab"), 0), None);
    }

    #[test]
    fn plus_is_greedy() {
        // A B+ C  on  a b b b c
        let p = Pattern::Concat(vec![
            vars("a"),
            Pattern::Quantified(Box::new(vars("b")), Quantifier::Plus, false),
            vars("c"),
        ]);
        let nfa = Nfa::compile(&p);
        assert_eq!(nfa.longest_match(&rows("abbbc"), 0), Some(5));
        // B+ requires at least one b.
        assert_eq!(nfa.longest_match(&rows("ac"), 0), None);
    }

    #[test]
    fn question_optional() {
        // A B? C  matches both "abc" and "ac"
        let p = Pattern::Concat(vec![
            vars("a"),
            Pattern::Quantified(Box::new(vars("b")), Quantifier::Question, false),
            vars("c"),
        ]);
        let nfa = Nfa::compile(&p);
        assert_eq!(nfa.longest_match(&rows("abc"), 0), Some(3));
        assert_eq!(nfa.longest_match(&rows("ac"), 0), Some(2));
    }

    #[test]
    fn star_greedy_longest() {
        // A*  on  a a a  -> greedy longest is 3
        let p = Pattern::Quantified(Box::new(vars("a")), Quantifier::Star, false);
        let nfa = Nfa::compile(&p);
        assert_eq!(nfa.longest_match(&rows("aaa"), 0), Some(3));
        // zero occurrences still matches (empty match).
        assert_eq!(nfa.longest_match(&rows("xyz"), 0), Some(0));
    }

    #[test]
    fn alternation() {
        // (A | B) C
        let p = Pattern::Concat(vec![Pattern::Alt(vec![vars("a"), vars("b")]), vars("c")]);
        let nfa = Nfa::compile(&p);
        assert_eq!(nfa.longest_match(&rows("ac"), 0), Some(2));
        assert_eq!(nfa.longest_match(&rows("bc"), 0), Some(2));
        assert_eq!(nfa.longest_match(&rows("cc"), 0), None);
    }

    #[test]
    fn range_bounds() {
        // A{2,3}
        let p = Pattern::Quantified(
            Box::new(vars("a")),
            Quantifier::Range {
                min: 2,
                max: Some(3),
            },
            false,
        );
        let nfa = Nfa::compile(&p);
        assert_eq!(nfa.longest_match(&rows("a"), 0), None); // need >= 2
        assert_eq!(nfa.longest_match(&rows("aa"), 0), Some(2));
        assert_eq!(nfa.longest_match(&rows("aaa"), 0), Some(3));
        assert_eq!(nfa.longest_match(&rows("aaaa"), 0), Some(3)); // capped at 3
    }

    #[test]
    fn permute_any_order() {
        // PERMUTE(a, b)
        let p = Pattern::Permute(vec!["a".to_owned(), "b".to_owned()]);
        let nfa = Nfa::compile(&p);
        assert_eq!(nfa.longest_match(&rows("ab"), 0), Some(2));
        assert_eq!(nfa.longest_match(&rows("ba"), 0), Some(2));
        assert_eq!(nfa.longest_match(&rows("aa"), 0), None);
    }

    #[test]
    fn match_from_offset() {
        // A B starting at index 1 of  x a b
        let p = Pattern::Concat(vec![vars("a"), vars("b")]);
        let nfa = Nfa::compile(&p);
        assert_eq!(nfa.longest_match(&rows("xab"), 1), Some(3));
        assert_eq!(nfa.longest_match(&rows("xab"), 0), None);
    }

    fn spans(v: &[(usize, usize)]) -> Vec<MatchSpan> {
        v.iter()
            .map(|&(start, end)| MatchSpan { start, end })
            .collect()
    }

    #[test]
    fn find_matches_skip_past_last_row() {
        // A B, repeated, with SKIP PAST LAST ROW -> non-overlapping matches.
        let p = Pattern::Concat(vec![vars("a"), vars("b")]);
        let nfa = Nfa::compile(&p);
        assert_eq!(
            nfa.find_matches(&rows("ababab"), &SkipMode::PastLastRow),
            spans(&[(0, 2), (2, 4), (4, 6)])
        );
    }

    #[test]
    fn find_matches_skip_to_next_row_overlaps() {
        // A+ with SKIP TO NEXT ROW: matches may overlap (resume at start+1).
        let p = Pattern::Quantified(Box::new(vars("a")), Quantifier::Plus, false);
        let nfa = Nfa::compile(&p);
        // "aaa": greedy A+ at 0->(0,3); to-next resumes at 1->(1,3); 2->(2,3).
        assert_eq!(
            nfa.find_matches(&rows("aaa"), &SkipMode::ToNextRow),
            spans(&[(0, 3), (1, 3), (2, 3)])
        );
        // PAST LAST ROW on the same input: single match.
        assert_eq!(
            nfa.find_matches(&rows("aaa"), &SkipMode::PastLastRow),
            spans(&[(0, 3)])
        );
    }

    #[test]
    fn find_matches_greedy_then_resume() {
        // A B+ : greedy consumes all b's, then resumes past the match.
        let p = Pattern::Concat(vec![
            vars("a"),
            Pattern::Quantified(Box::new(vars("b")), Quantifier::Plus, false),
        ]);
        let nfa = Nfa::compile(&p);
        // a b b | a b  -> (0,3) then (3,5)
        assert_eq!(
            nfa.find_matches(&rows("abbab"), &SkipMode::PastLastRow),
            spans(&[(0, 3), (3, 5)])
        );
    }

    #[test]
    fn find_matches_skips_non_matching_rows() {
        // A B with junk rows between matches.
        let p = Pattern::Concat(vec![vars("a"), vars("b")]);
        let nfa = Nfa::compile(&p);
        // x a b x x a b -> (1,3),(5,7)
        assert_eq!(
            nfa.find_matches(&rows("xabxxab"), &SkipMode::PastLastRow),
            spans(&[(1, 3), (5, 7)])
        );
    }

    #[test]
    fn find_matches_empty_pattern_terminates() {
        // A* matches empty everywhere; empty matches are not emitted and the scan terminates.
        let p = Pattern::Quantified(Box::new(vars("a")), Quantifier::Star, false);
        let nfa = Nfa::compile(&p);
        // "aa b aa" -> greedy A* consumes runs of a, emits non-empty ones.
        assert_eq!(
            nfa.find_matches(&rows("aabaa"), &SkipMode::PastLastRow),
            spans(&[(0, 2), (3, 5)])
        );
        // all-non-matching -> no matches, terminates.
        assert_eq!(
            nfa.find_matches(&rows("xxx"), &SkipMode::PastLastRow),
            spans(&[])
        );
    }

    fn lbl(s: &str) -> Vec<String> {
        s.chars().map(|c| c.to_string()).collect()
    }

    #[test]
    fn labeled_concat() {
        // A B -> rows labelled a, b.
        let p = Pattern::Concat(vec![vars("a"), vars("b")]);
        let nfa = Nfa::compile(&p);
        assert_eq!(
            nfa.longest_match_labeled(&rows("ab"), 0),
            Some((2, lbl("ab")))
        );
    }

    #[test]
    fn labeled_plus_greedy() {
        // A B+ on a b b -> labels a, b, b (greedy consumes both b's).
        let p = Pattern::Concat(vec![
            vars("a"),
            Pattern::Quantified(Box::new(vars("b")), Quantifier::Plus, false),
        ]);
        let nfa = Nfa::compile(&p);
        assert_eq!(
            nfa.longest_match_labeled(&rows("abb"), 0),
            Some((3, lbl("abb")))
        );
    }

    #[test]
    fn labeled_alternation() {
        // (A | B) C on b c -> labels b, c.
        let p = Pattern::Concat(vec![Pattern::Alt(vec![vars("a"), vars("b")]), vars("c")]);
        let nfa = Nfa::compile(&p);
        assert_eq!(
            nfa.longest_match_labeled(&rows("bc"), 0),
            Some((2, lbl("bc")))
        );
    }

    #[test]
    fn labeled_permute() {
        // PERMUTE(a, b) on b a -> labels b, a.
        let p = Pattern::Permute(vec!["a".to_owned(), "b".to_owned()]);
        let nfa = Nfa::compile(&p);
        assert_eq!(
            nfa.longest_match_labeled(&rows("ba"), 0),
            Some((2, lbl("ba")))
        );
    }

    #[test]
    fn find_matches_labeled_carries_labels() {
        // A B repeated -> two labelled matches.
        let p = Pattern::Concat(vec![vars("a"), vars("b")]);
        let nfa = Nfa::compile(&p);
        assert_eq!(
            nfa.find_matches_labeled(&rows("abab"), &SkipMode::PastLastRow),
            vec![
                LabeledMatch {
                    start: 0,
                    end: 2,
                    labels: lbl("ab")
                },
                LabeledMatch {
                    start: 2,
                    end: 4,
                    labels: lbl("ab")
                },
            ]
        );
    }

    #[test]
    fn skip_to_first_last_var() {
        // Pattern (a b b) over five rows that each satisfy both `a` and `b`, so matches can overlap.
        // The skip strategy decides where each next match starts:
        //   PAST LAST ROW -> one match [0,3)
        //   SKIP TO LAST b -> [0,3), [2,5)      (resume at the match's last `b`)
        //   SKIP TO FIRST b -> [0,3), [1,4), [2,5)  (resume at the match's first `b`)
        let p = Pattern::Concat(vec![vars("a"), vars("b"), vars("b")]);
        let nfa = Nfa::compile(&p);
        let rows = vec![BTreeSet::from(["a".to_owned(), "b".to_owned()]); 5];

        let starts = |skip: &SkipMode| {
            nfa.find_matches_labeled(&rows, skip)
                .into_iter()
                .map(|m| m.start)
                .collect::<Vec<_>>()
        };
        assert_eq!(starts(&SkipMode::PastLastRow), vec![0]);
        assert_eq!(starts(&SkipMode::ToLast("b".to_owned())), vec![0, 2]);
        assert_eq!(starts(&SkipMode::ToFirst("b".to_owned())), vec![0, 1, 2]);
    }

    /// The two data-dependent cases where a variable-targeted skip has no valid resume row degrade
    /// (deliberately, instead of raising the runtime error the SQL standard prescribes — see
    /// [`SkipMode::next_pos`]). The degradation must be *visible*, so `next_pos` returns a
    /// diagnostic alongside the position, and the position itself must be unchanged: this is a
    /// visibility fix, not a behavior change.
    #[test]
    fn skip_target_degradations_are_reported() {
        // A match over rows 3..5, labelled `a` then `b`.
        let labels = lbl("ab");

        // (a) The target is bound to no row of this match: nothing to resume at, so the scan resumes
        //     past the match's last row — silently becoming SKIP PAST LAST ROW.
        assert_eq!(
            SkipMode::ToLast("c".to_owned()).next_pos(3, 5, &labels),
            (5, Some(SkipDegradation::TargetAbsent))
        );
        assert_eq!(
            SkipMode::ToFirst("c".to_owned()).next_pos(3, 5, &labels),
            (5, Some(SkipDegradation::TargetAbsent))
        );

        // (b) The target resolves to the match's own first row: resuming there would re-find the
        //     same match forever, so it is clamped one row on — silently becoming SKIP TO NEXT ROW.
        assert_eq!(
            SkipMode::ToFirst("a".to_owned()).next_pos(3, 5, &labels),
            (4, Some(SkipDegradation::TargetAtMatchStart))
        );
        assert_eq!(
            SkipMode::ToLast("a".to_owned()).next_pos(3, 5, &labels),
            (4, Some(SkipDegradation::TargetAtMatchStart))
        );

        // A resolvable target reports nothing — note it can land on the same position as the clamped
        // case above, so the diagnostic (not the position) is what distinguishes them.
        assert_eq!(
            SkipMode::ToLast("b".to_owned()).next_pos(3, 5, &labels),
            (4, None)
        );
        // The variable-less modes cannot degrade.
        assert_eq!(SkipMode::PastLastRow.next_pos(3, 5, &labels), (5, None));
        assert_eq!(SkipMode::ToNextRow.next_pos(3, 5, &labels), (4, None));
    }

    /// The diagnostic names the target variable and the strategy the resume position degraded to. The
    /// skip mode itself is named by the caller (`clause_name`), so it must NOT be repeated here — the
    /// rendered message would otherwise say `SKIP` twice.
    #[test]
    fn skip_degradation_describes_target_and_fallback() {
        let absent = SkipDegradation::TargetAbsent.describe(&SkipMode::ToLast("c".to_owned()));
        assert!(absent.contains("target variable `c`"), "{absent}");
        assert!(absent.contains("SKIP PAST LAST ROW"), "{absent}");
        assert!(!absent.contains("SKIP TO LAST"), "{absent}");

        let at_start =
            SkipDegradation::TargetAtMatchStart.describe(&SkipMode::ToFirst("a".to_owned()));
        assert!(at_start.contains("target variable `a`"), "{at_start}");
        assert!(at_start.contains("SKIP TO NEXT ROW"), "{at_start}");
        assert!(!at_start.contains("SKIP TO FIRST"), "{at_start}");
    }

    /// The clause name feeds the `ExprError` parameter name, so it must state the mode without the
    /// target variable (which `describe` names) — one mention of the mode per rendered message.
    #[test]
    fn skip_clause_name_and_target_var() {
        assert_eq!(
            SkipMode::ToLast("c".to_owned()).clause_name(),
            "AFTER MATCH SKIP TO LAST"
        );
        assert_eq!(
            SkipMode::ToFirst("a".to_owned()).clause_name(),
            "AFTER MATCH SKIP TO FIRST"
        );
        assert_eq!(
            SkipMode::PastLastRow.clause_name(),
            "AFTER MATCH SKIP PAST LAST ROW"
        );
        assert_eq!(
            SkipMode::ToNextRow.clause_name(),
            "AFTER MATCH SKIP TO NEXT ROW"
        );

        assert_eq!(SkipMode::ToLast("c".to_owned()).target_var(), Some("c"));
        assert_eq!(SkipMode::ToFirst("a".to_owned()).target_var(), Some("a"));
        // The variable-less modes have no target — and can never degrade.
        assert_eq!(SkipMode::PastLastRow.target_var(), None);
        assert_eq!(SkipMode::ToNextRow.target_var(), None);
    }

    #[test]
    fn row_satisfying_multiple_vars() {
        // Overlapping DEFINE predicates: a row can satisfy several variables.
        let p = Pattern::Concat(vec![vars("a"), vars("b")]);
        let nfa = Nfa::compile(&p);
        let rows = vec![
            BTreeSet::from(["a".to_owned(), "b".to_owned()]),
            BTreeSet::from(["b".to_owned()]),
        ];
        assert_eq!(nfa.longest_match(&rows, 0), Some(2));
    }

    /// [`Nfa::next_match`] is lazy: pulling one match must not evaluate predicates past that
    /// match's scan region, and pulling to exhaustion must enumerate exactly what
    /// [`Nfa::find_matches_dynamic`] collects. The emit loop relies on the first property to stop
    /// scanning at a held boundary match without paying for (or materializing) the rest.
    #[tokio::test]
    async fn next_match_is_lazy_and_equivalent() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingMatcher {
            rows: Vec<BTreeSet<String>>,
            calls: AtomicUsize,
        }
        impl CandidateMatcher for CountingMatcher {
            async fn matches(
                &self,
                var: &str,
                pos: usize,
                _labels: &[String],
            ) -> StreamExecutorResult<bool> {
                self.calls.fetch_add(1, Ordering::Relaxed);
                Ok(self.rows[pos].contains(var))
            }
        }

        let nfa = Nfa::compile(&Pattern::Concat(vec![vars("a"), vars("b")]));
        let r = rows("ababab");

        // Pull ONE match, then compare predicate-evaluation counts against a full collect.
        let first_only = CountingMatcher {
            rows: r.clone(),
            calls: AtomicUsize::new(0),
        };
        let mut scan = MatchScan::new();
        let skip = SkipMode::PastLastRow;
        let first = nfa
            .next_match(
                &mut scan,
                r.len(),
                &first_only,
                &skip,
                &mut ScanBudget::unlimited(),
                false,
            )
            .await
            .unwrap()
            .expect("first match");
        assert_eq!((first.start, first.end), (0, 2));
        let one_pull = first_only.calls.load(Ordering::Relaxed);

        let full = CountingMatcher {
            rows: r.clone(),
            calls: AtomicUsize::new(0),
        };
        let collected = nfa
            .find_matches_dynamic(r.len(), &full, &skip)
            .await
            .unwrap();
        let full_scan = full.calls.load(Ordering::Relaxed);
        assert!(
            one_pull < full_scan,
            "one pull ({one_pull} evaluations) must cost less than the full scan ({full_scan})"
        );

        // Pulling to exhaustion enumerates exactly the collected sequence.
        let m = SetMatcher { rows: r.clone() };
        let mut scan = MatchScan::new();
        let mut pulled = Vec::new();
        while let Some(mm) = nfa
            .next_match(
                &mut scan,
                r.len(),
                &m,
                &skip,
                &mut ScanBudget::unlimited(),
                false,
            )
            .await
            .unwrap()
        {
            pulled.push(mm);
        }
        assert_eq!(pulled, collected);
    }

    /// The catastrophic-backtracking family: `(a? a? … a? b)` over a run of `a`-rows costs
    /// exponentially many predicate evaluations per start unmemoized. The per-start `(state, pos)`
    /// failure memo (sound for path-independent verdicts, recorded only at consumption boundaries)
    /// must collapse that to polynomial — and must not change a single result.
    #[tokio::test]
    async fn memoization_defuses_catastrophic_backtracking() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingMatcher {
            rows: Vec<BTreeSet<String>>,
            calls: AtomicUsize,
        }
        impl CandidateMatcher for CountingMatcher {
            async fn matches(
                &self,
                var: &str,
                pos: usize,
                _labels: &[String],
            ) -> StreamExecutorResult<bool> {
                self.calls.fetch_add(1, Ordering::Relaxed);
                Ok(self.rows[pos].contains(var))
            }
        }

        // (a? ×16  b) over 20 `a`-rows and no `b`: every start fails, each exploring the
        // exponential subset lattice of which optionals consumed which rows.
        let mut parts: Vec<Pattern> = (0..16)
            .map(|_| Pattern::Quantified(Box::new(vars("a")), Quantifier::Question, false))
            .collect();
        parts.push(vars("b"));
        let nfa = Nfa::compile(&Pattern::Concat(parts));
        let r = rows(&"a".repeat(20));

        let run = |memoize: bool| {
            let nfa = &nfa;
            let r = r.clone();
            async move {
                let m = CountingMatcher {
                    rows: r.clone(),
                    calls: AtomicUsize::new(0),
                };
                let mut budget = ScanBudget::unlimited();
                let mut scan = MatchScan::new();
                let mut out = Vec::new();
                while let Some(mm) = nfa
                    .next_match(
                        &mut scan,
                        r.len(),
                        &m,
                        &SkipMode::PastLastRow,
                        &mut budget,
                        memoize,
                    )
                    .await
                    .unwrap()
                {
                    out.push(mm);
                }
                (out, m.calls.load(Ordering::Relaxed))
            }
        };

        let (plain_out, plain_calls) = run(false).await;
        let (memo_out, memo_calls) = run(true).await;
        assert_eq!(plain_out, memo_out);
        assert!(
            memo_calls * 20 < plain_calls,
            "memoized scan ({memo_calls} evaluations) must be far below the backtracking scan \
             ({plain_calls})"
        );
        // And the memoized cost is genuinely polynomial-small for this size.
        assert!(memo_calls < 50_000, "memoized: {memo_calls}");
    }

    /// A spent [`ScanBudget`] stops the walks without verdicts: the finder yields no further
    /// match (never a wrong one), the liveness walker answers "not alive" only alongside the
    /// sticky `hit` flag (which the executor must check before believing it), and the extension
    /// probe answers "may extend" (hold).
    #[tokio::test]
    async fn spent_budget_stops_without_verdicts() {
        let nfa = Nfa::compile(&Pattern::Concat(vec![vars("a"), vars("b")]));
        let r = rows("ab");
        let m = SetMatcher { rows: r.clone() };

        // Unlimited: the match is found.
        let mut budget = ScanBudget::unlimited();
        let mut scan = MatchScan::new();
        let found = nfa
            .next_match(
                &mut scan,
                r.len(),
                &m,
                &SkipMode::PastLastRow,
                &mut budget,
                false,
            )
            .await
            .unwrap();
        assert!(found.is_some());
        assert!(!budget.hit);

        // Zero budget: no match, hit latched.
        let mut budget = ScanBudget::new(0);
        let mut scan = MatchScan::new();
        let found = nfa
            .next_match(
                &mut scan,
                r.len(),
                &m,
                &SkipMode::PastLastRow,
                &mut budget,
                false,
            )
            .await
            .unwrap();
        assert!(found.is_none());
        assert!(budget.hit);

        // Liveness under zero budget: answers false with hit latched — the executor treats that
        // as "undecided, retain", never as "dead".
        let mut budget = ScanBudget::new(0);
        let alive = nfa
            .reaches_boundary_alive(0, 1, &m, &mut budget, false)
            .await
            .unwrap();
        assert!(!alive);
        assert!(budget.hit);

        // Extension probe under zero budget: conservative "may extend".
        let mut budget = ScanBudget::new(0);
        assert!(nfa.may_extend(0, 2, &m, &mut budget, false).await.unwrap());
        assert!(budget.hit);
    }

    #[tokio::test]
    async fn dynamic_matches_static_for_set_predicate() {
        let p = Pattern::Concat(vec![vars("a"), vars("b")]);
        let nfa = Nfa::compile(&p);
        let r = rows("abab");
        let m = SetMatcher { rows: r.clone() };
        let dynamic = nfa
            .find_matches_dynamic(r.len(), &m, &SkipMode::PastLastRow)
            .await
            .unwrap();
        assert_eq!(
            dynamic,
            nfa.find_matches_labeled(&r, &SkipMode::PastLastRow)
        );
    }

    /// [`Nfa::may_extend`]: a boundary match whose PREFERRED result cannot change is final (emit
    /// now); one where a higher-priority path is blocked on the row boundary — an open greedy
    /// quantifier, an earlier-listed longer alternation branch, an unexhausted range, an optional
    /// tail — must be held. The check follows the finder's preference order: lower-priority paths
    /// (a later-listed alternation branch, a reluctant loop's consume edge) can never override an
    /// accepting result and must not hold it.
    #[tokio::test]
    async fn may_extend_follows_the_finder_preference_order() {
        let m = |s: &str| SetMatcher { rows: rows(s) };

        // Fixed (a b): after consuming both, nothing can extend — terminal.
        let fixed = Nfa::compile(&Pattern::Concat(vec![vars("a"), vars("b")]));
        assert!(
            !fixed
                .may_extend(0, 2, &m("ab"), &mut ScanBudget::unlimited(), false)
                .await
                .unwrap()
        );

        // (a b+): the greedy loop's consume edge precedes its exit, so a future `b` would produce
        // a preferred (longer) result — held.
        let open = Nfa::compile(&Pattern::Concat(vec![
            vars("a"),
            Pattern::Quantified(Box::new(vars("b")), Quantifier::Plus, false),
        ]));
        assert!(
            open.may_extend(0, 2, &m("ab"), &mut ScanBudget::unlimited(), false)
                .await
                .unwrap()
        );

        // (a (b | b c)): ordered alternation — the first-listed `b` branch already accepted, and a
        // later `c` cannot override it. Terminal, matching what the finder would return.
        let alt = Nfa::compile(&Pattern::Concat(vec![
            vars("a"),
            Pattern::Alt(vec![vars("b"), Pattern::Concat(vec![vars("b"), vars("c")])]),
        ]));
        assert!(
            !alt.may_extend(0, 2, &m("ab"), &mut ScanBudget::unlimited(), false)
                .await
                .unwrap()
        );

        // (a (b c | b)): the LONGER branch is listed first, so a future `c` would produce a
        // preferred result — held. Preference direction, not structure, decides.
        let alt_rev = Nfa::compile(&Pattern::Concat(vec![
            vars("a"),
            Pattern::Alt(vec![Pattern::Concat(vec![vars("b"), vars("c")]), vars("b")]),
        ]));
        assert!(
            alt_rev
                .may_extend(0, 2, &m("ab"), &mut ScanBudget::unlimited(), false)
                .await
                .unwrap()
        );

        // (a b+?): reluctant — the exit edge precedes the consume edge, so the short result is the
        // preferred one and future `b`s cannot change it. Terminal.
        let reluctant = Nfa::compile(&Pattern::Concat(vec![
            vars("a"),
            Pattern::Quantified(Box::new(vars("b")), Quantifier::Plus, true),
        ]));
        assert!(
            !reluctant
                .may_extend(0, 2, &m("ab"), &mut ScanBudget::unlimited(), false)
                .await
                .unwrap()
        );

        // (a b{1,2}): one `b` leaves the range open; two exhaust it.
        let range = Nfa::compile(&Pattern::Concat(vec![
            vars("a"),
            Pattern::Quantified(
                Box::new(vars("b")),
                Quantifier::Range {
                    min: 1,
                    max: Some(2),
                },
                false,
            ),
        ]));
        assert!(
            range
                .may_extend(0, 2, &m("ab"), &mut ScanBudget::unlimited(), false)
                .await
                .unwrap()
        );
        assert!(
            !range
                .may_extend(0, 3, &m("abb"), &mut ScanBudget::unlimited(), false)
                .await
                .unwrap()
        );

        // (a b?): the optional tail extends a bare [a]; a consumed (a, b) is terminal.
        let opt = Nfa::compile(&Pattern::Concat(vec![
            vars("a"),
            Pattern::Quantified(Box::new(vars("b")), Quantifier::Question, false),
        ]));
        assert!(
            opt.may_extend(0, 1, &m("a"), &mut ScanBudget::unlimited(), false)
                .await
                .unwrap()
        );
        assert!(
            !opt.may_extend(0, 2, &m("ab"), &mut ScanBudget::unlimited(), false)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn reaches_boundary_alive_evicts_dead_prefix() {
        // PATTERN (a b): a start is live only if a match from it can still reach the safe boundary.
        let nfa = Nfa::compile(&Pattern::Concat(vec![vars("a"), vars("b")]));

        // `[a]` with the boundary right after it: the `a` is a live partial match — a future `b` may
        // complete it — so it must be retained.
        let m = SetMatcher { rows: rows("a") };
        assert!(
            nfa.reaches_boundary_alive(0, 1, &m, &mut ScanBudget::unlimited(), false)
                .await
                .unwrap()
        );

        // `[a, x]` and `[a, x, x]` (x satisfies neither `a` nor `b`): the `a` can still *begin* the
        // pattern, but the following safe rows already block it from completing, so it is dead and
        // must be evictable. This is the case the previous `can_begin_at`-based predicate retained
        // forever.
        let m = SetMatcher { rows: rows("ax") };
        assert!(
            !nfa.reaches_boundary_alive(0, 2, &m, &mut ScanBudget::unlimited(), false)
                .await
                .unwrap()
        );
        let m = SetMatcher { rows: rows("axx") };
        assert!(
            !nfa.reaches_boundary_alive(0, 3, &m, &mut ScanBudget::unlimited(), false)
                .await
                .unwrap()
        );

        // A later start can be the live one: in `[x, a]` row 0 is dead but row 1 (the `a`) is live.
        let m = SetMatcher { rows: rows("xa") };
        assert!(
            !nfa.reaches_boundary_alive(0, 2, &m, &mut ScanBudget::unlimited(), false)
                .await
                .unwrap()
        );
        assert!(
            nfa.reaches_boundary_alive(1, 2, &m, &mut ScanBudget::unlimited(), false)
                .await
                .unwrap()
        );

        // A complete match sitting exactly at the boundary is not yet finalized (it needs a following
        // safe row to confirm maximality), so its start is still retained.
        let m = SetMatcher { rows: rows("ab") };
        assert!(
            nfa.reaches_boundary_alive(0, 2, &m, &mut ScanBudget::unlimited(), false)
                .await
                .unwrap()
        );
    }

    /// A path-dependent matcher: `b` only matches once an `a` has been bound earlier in the match.
    /// This exercises threading the running labels into the predicate.
    struct NeedsPrecedingA;
    impl CandidateMatcher for NeedsPrecedingA {
        async fn matches(
            &self,
            var: &str,
            _pos: usize,
            labels: &[String],
        ) -> StreamExecutorResult<bool> {
            Ok(match var {
                "a" => true,
                "b" => labels.iter().any(|l| l == "a"),
                _ => false,
            })
        }
    }

    #[tokio::test]
    async fn reluctant_quantifier_prefers_fewer() {
        // Three rows that each satisfy both `a` and `b`, so `a+ b` can stop early.
        let rows = vec![BTreeSet::from(["a".to_owned(), "b".to_owned()]); 3];
        let m = SetMatcher { rows: rows.clone() };

        // Greedy `a+ b`: consume as many `a` as possible -> [0, 3) (a a b).
        let greedy = Nfa::compile(&Pattern::Concat(vec![
            Pattern::Quantified(Box::new(vars("a")), Quantifier::Plus, false),
            vars("b"),
        ]));
        assert_eq!(
            greedy
                .find_matches_dynamic(rows.len(), &m, &SkipMode::PastLastRow)
                .await
                .unwrap(),
            vec![LabeledMatch {
                start: 0,
                end: 3,
                labels: lbl("aab")
            }]
        );

        // Reluctant `a+? b`: take the fewest `a` -> [0, 2) (a b), then [2, ...) finds nothing more.
        let reluctant = Nfa::compile(&Pattern::Concat(vec![
            Pattern::Quantified(Box::new(vars("a")), Quantifier::Plus, true),
            vars("b"),
        ]));
        assert_eq!(
            reluctant
                .find_matches_dynamic(rows.len(), &m, &SkipMode::PastLastRow)
                .await
                .unwrap(),
            vec![LabeledMatch {
                start: 0,
                end: 2,
                labels: lbl("ab")
            }]
        );
    }

    /// `n` rows that each satisfy both `a` and `b`, so quantifier preference (not the predicates)
    /// decides how a match is split between variables.
    fn ab_rows(n: usize) -> Vec<BTreeSet<String>> {
        vec![BTreeSet::from(["a".to_owned(), "b".to_owned()]); n]
    }

    fn plus(inner: Pattern, reluctant: bool) -> Pattern {
        Pattern::Quantified(Box::new(inner), Quantifier::Plus, reluctant)
    }

    fn star(inner: Pattern, reluctant: bool) -> Pattern {
        Pattern::Quantified(Box::new(inner), Quantifier::Star, reluctant)
    }

    #[tokio::test]
    async fn nested_reluctant_then_greedy_adjacent() {
        // `a*? a*` over three `a` rows. The reluctant first star takes as few as possible (zero) and
        // the greedy second star takes the rest, so the whole run is still consumed: [0, 3). This
        // guards against an empty-match or non-termination bug when two quantifiers over the same
        // variable sit adjacent with opposite preferences.
        let r = rows("aaa");
        let m = SetMatcher { rows: r.clone() };
        let nfa = Nfa::compile(&Pattern::Concat(vec![
            star(vars("a"), true),
            star(vars("a"), false),
        ]));
        assert_eq!(
            nfa.find_matches_dynamic(r.len(), &m, &SkipMode::PastLastRow)
                .await
                .unwrap(),
            vec![LabeledMatch {
                start: 0,
                end: 3,
                labels: lbl("aaa")
            }]
        );
    }

    #[tokio::test]
    async fn nested_quantifier_preference_flips_split() {
        // Four rows that each satisfy both `a` and `b`, matched by `(<a-quant> b+)+`. The inner
        // first-variable quantifier's preference decides the split; the rest is greedy `b+`.
        let r = ab_rows(4);
        let m = SetMatcher { rows: r.clone() };

        // Reluctant `a+?` takes the fewest `a` (one), then greedy `b+` takes the rest -> "abbb".
        let reluctant = Nfa::compile(&plus(
            Pattern::Concat(vec![plus(vars("a"), true), plus(vars("b"), false)]),
            false,
        ));
        assert_eq!(
            reluctant
                .find_matches_dynamic(r.len(), &m, &SkipMode::PastLastRow)
                .await
                .unwrap(),
            vec![LabeledMatch {
                start: 0,
                end: 4,
                labels: lbl("abbb")
            }]
        );

        // Greedy `a+` takes as many `a` as it can while still leaving one row for the mandatory
        // `b+`, so it backtracks from four to three -> "aaab".
        let greedy = Nfa::compile(&plus(
            Pattern::Concat(vec![plus(vars("a"), false), plus(vars("b"), false)]),
            false,
        ));
        assert_eq!(
            greedy
                .find_matches_dynamic(r.len(), &m, &SkipMode::PastLastRow)
                .await
                .unwrap(),
            vec![LabeledMatch {
                start: 0,
                end: 4,
                labels: lbl("aaab")
            }]
        );
    }

    #[tokio::test]
    async fn dynamic_threads_running_labels() {
        // (a b): `b` sees `a` in the running labels -> matches.
        let ab = Nfa::compile(&Pattern::Concat(vec![vars("a"), vars("b")]));
        let m = NeedsPrecedingA;
        assert_eq!(
            ab.find_matches_dynamic(2, &m, &SkipMode::PastLastRow)
                .await
                .unwrap(),
            vec![LabeledMatch {
                start: 0,
                end: 2,
                labels: lbl("ab")
            }]
        );

        // (b a): `b` is first, the running labels are empty, so it cannot match -> no match.
        let ba = Nfa::compile(&Pattern::Concat(vec![vars("b"), vars("a")]));
        assert_eq!(
            ba.find_matches_dynamic(2, &m, &SkipMode::PastLastRow)
                .await
                .unwrap(),
            vec![]
        );
    }

    /// A budget that dies inside a start must NOT leave the scan cursor past it: the cursor would
    /// then claim a verdict for a start whose walk never reached one. (It is not a fully-scanned
    /// marker either way — a hit jumps it past the whole match — but "aborted" and "decided" must
    /// stay distinguishable for a caller that resumes a pull.)
    #[tokio::test]
    async fn an_aborted_start_is_not_reported_as_scanned() {
        // `(a b) | a`: the preferred branch charges `a` then `b`; with a budget of 1 the walk dies
        // inside start 0 having never tried the second alternative, which DOES match there.
        let pat = Pattern::Alt(vec![
            Pattern::Concat(vec![Pattern::Var("a".into()), Pattern::Var("b".into())]),
            Pattern::Var("a".into()),
        ]);
        let nfa = Nfa::compile(&pat);
        let matcher = SetMatcher::new(vec![BTreeSet::from(["a".to_owned()]); 2]);
        let mut scan = MatchScan::new();
        let mut budget = ScanBudget::new(1);
        let found = nfa
            .next_match(
                &mut scan,
                2,
                &matcher,
                &SkipMode::PastLastRow,
                &mut budget,
                false,
            )
            .await
            .unwrap();
        assert!(
            found.is_none() && budget.hit,
            "the test needs the budget to die inside start 0"
        );
        assert_eq!(
            scan.next_start(),
            0,
            "start 0 was aborted with no verdict, so the cursor must still point at it"
        );
    }

    #[test]
    fn max_match_rows_is_the_longest_path_or_none_for_a_cycle() {
        let var = |s: &str| Pattern::Var(s.to_owned());
        let concat = |s: &str| Pattern::Concat(s.split(' ').map(var).collect());
        let q = |p: Pattern, q: Quantifier| Pattern::Quantified(Box::new(p), q, false);
        let range = |min, max| Quantifier::Range { min, max };
        assert_eq!(Nfa::compile(&concat("a b c")).max_match_rows(), Some(3));
        assert_eq!(
            Nfa::compile(&Pattern::Alt(vec![concat("a b"), var("c")])).max_match_rows(),
            Some(2)
        );
        assert_eq!(
            Nfa::compile(&Pattern::Concat(vec![
                q(var("a"), Quantifier::Question),
                var("b")
            ]))
            .max_match_rows(),
            Some(2)
        );
        assert_eq!(
            Nfa::compile(&q(var("a"), range(2, Some(5)))).max_match_rows(),
            Some(5)
        );
        assert_eq!(
            Nfa::compile(&q(var("a"), range(600, Some(600)))).max_match_rows(),
            Some(600)
        );
        assert_eq!(
            Nfa::compile(&q(var("a"), Quantifier::Plus)).max_match_rows(),
            None
        );
        assert_eq!(
            Nfa::compile(&q(var("a"), Quantifier::Star)).max_match_rows(),
            None
        );
        assert_eq!(
            Nfa::compile(&q(var("a"), range(2, None))).max_match_rows(),
            None
        );
    }

    /// With more rows to the boundary than an acyclic automaton can consume, a position is dead
    /// without a walk — a budget of 1 survives untouched. With exactly as many, the walk runs, and
    /// a match that ends on the boundary keeps its start alive.
    #[tokio::test]
    async fn far_from_the_boundary_an_acyclic_pattern_is_dead_without_a_walk() {
        let pat = Pattern::Quantified(
            Box::new(Pattern::Var("a".into())),
            Quantifier::Range {
                min: 600,
                max: Some(600),
            },
            false,
        );
        let nfa = Nfa::compile(&pat);
        let n_rows = 1300;
        let matcher = SetMatcher::new(vec![BTreeSet::from(["a".to_owned()]); n_rows]);
        let mut budget = ScanBudget::new(1);
        assert!(
            !nfa.reaches_boundary_alive(0, n_rows, &matcher, &mut budget, true)
                .await
                .unwrap()
        );
        assert!(!budget.hit, "the verdict must not have cost a walk");
        let mut budget = ScanBudget::unlimited();
        assert!(
            nfa.reaches_boundary_alive(700, n_rows, &matcher, &mut budget, true)
                .await
                .unwrap(),
            "a{{600}} from 700 accepts exactly on the boundary, which is alive"
        );
    }

    /// A start whose walk finds no accept and never reaches the boundary died on rows that will
    /// never change: the scan proves it matchless forever. The proof runs contiguously from where
    /// the scan began, stops at a start that reached the boundary, and is not extended by a start
    /// the finder skipped as too short (that one is blocked at the boundary, not dead).
    #[tokio::test]
    async fn starts_that_die_below_the_boundary_are_proven_matchless() {
        let nfa = Nfa::compile(&Pattern::Concat(vec![
            Pattern::Var("a".into()),
            Pattern::Var("b".into()),
        ]));
        let scan_all = |seq: &str| {
            let nfa = &nfa;
            let matcher = SetMatcher::new(rows(seq));
            let n = seq.len();
            async move {
                let mut scan = MatchScan::new();
                let mut budget = ScanBudget::unlimited();
                let found = nfa
                    .next_match(
                        &mut scan,
                        n,
                        &matcher,
                        &SkipMode::PastLastRow,
                        &mut budget,
                        false,
                    )
                    .await
                    .unwrap();
                (found.map(|m| (m.start, m.end)), scan)
            }
        };
        // Starts 0, 1 and 2 all die below the boundary; start 3 is skipped as too short.
        let (found, scan) = scan_all("axaa").await;
        assert_eq!(found, None);
        assert_eq!(scan.matchless_upto(), 3);
        assert_eq!(scan.next_start(), 4);
        // Start 2 consumes `a` and stands at the boundary waiting for `b`: not matchless.
        let (found, scan) = scan_all("axa").await;
        assert_eq!(found, None);
        assert_eq!(scan.matchless_upto(), 2);
        // A match ends the contiguous run.
        let (found, scan) = scan_all("xab").await;
        assert_eq!(found, Some((1, 3)));
        assert_eq!(scan.matchless_upto(), 1);
    }

    #[test]
    fn min_match_rows_is_the_shortest_accepting_path() {
        let var = |s: &str| Pattern::Var(s.to_owned());
        let concat = |s: &str| Pattern::Concat(s.split(' ').map(var).collect());
        let q = |p: Pattern, q: Quantifier| Pattern::Quantified(Box::new(p), q, false);
        assert_eq!(Nfa::compile(&concat("a b c")).min_match_rows(), 3);
        assert_eq!(
            Nfa::compile(&q(var("a"), Quantifier::Plus)).min_match_rows(),
            1
        );
        assert_eq!(
            Nfa::compile(&q(var("a"), Quantifier::Star)).min_match_rows(),
            0
        );
        assert_eq!(
            Nfa::compile(&Pattern::Concat(vec![
                q(var("a"), Quantifier::Question),
                var("b")
            ]))
            .min_match_rows(),
            1
        );
        assert_eq!(
            Nfa::compile(&Pattern::Alt(vec![concat("a b"), var("c")])).min_match_rows(),
            1
        );
        assert_eq!(
            Nfa::compile(&Pattern::Permute(vec!["a".into(), "b".into(), "c".into()]))
                .min_match_rows(),
            3
        );
        assert_eq!(
            Nfa::compile(&q(
                var("a"),
                Quantifier::Range {
                    min: 600,
                    max: Some(600),
                }
            ))
            .min_match_rows(),
            600
        );
    }

    /// A start with fewer rows before the boundary than the shortest possible match is skipped
    /// without a walk — and the skip IS a verdict ("no match from here within these rows"), so the
    /// cursor advances past it exactly as it would after a failed walk. Here the whole buffer is
    /// shorter than `a{600}`: nothing is walked, and a budget of 1 survives untouched.
    #[tokio::test]
    async fn starts_too_close_to_the_boundary_are_skipped_without_a_walk() {
        let pat = Pattern::Quantified(
            Box::new(Pattern::Var("a".into())),
            Quantifier::Range {
                min: 600,
                max: Some(600),
            },
            false,
        );
        let nfa = Nfa::compile(&pat);
        let n_rows = 10;
        let matcher = SetMatcher::new(vec![BTreeSet::from(["a".to_owned()]); n_rows]);
        let mut scan = MatchScan::new();
        let mut budget = ScanBudget::new(1);
        let found = nfa
            .next_match(
                &mut scan,
                n_rows,
                &matcher,
                &SkipMode::PastLastRow,
                &mut budget,
                true,
            )
            .await
            .unwrap();
        assert!(found.is_none());
        assert!(!budget.hit, "no start was walked, so nothing was charged");
        assert_eq!(
            scan.next_start(),
            n_rows,
            "every start was skipped with a verdict, so the cursor passed them all"
        );
    }

    /// The walkers used to recurse once per consumed row and once per ε-edge under a hard cap of
    /// 512 frames, so any match spanning more than a couple of hundred rows was permanently
    /// undecidable: the cap did not reset with the budget, and every visit died at the same depth.
    /// The walk is iterative now and only the budget bounds it. All three walkers reach this depth.
    #[tokio::test]
    async fn a_match_spanning_thousands_of_rows_is_decided() {
        let pat = Pattern::Quantified(Box::new(Pattern::Var("a".into())), Quantifier::Plus, false);
        let nfa = Nfa::compile(&pat);
        let n_rows = 2000;
        let matcher = SetMatcher::new(vec![BTreeSet::from(["a".to_owned()]); n_rows]);

        let mut scan = MatchScan::new();
        let mut budget = ScanBudget::unlimited();
        let found = nfa
            .next_match(
                &mut scan,
                n_rows,
                &matcher,
                &SkipMode::PastLastRow,
                &mut budget,
                true,
            )
            .await
            .unwrap()
            .expect("a 2000-row greedy match must be found");
        assert_eq!((found.start, found.end), (0, n_rows));
        assert_eq!(found.labels.len(), n_rows);
        assert!(!budget.hit);

        // The greedy `a+` is blocked at the boundary, so the match may still extend...
        let mut budget = ScanBudget::unlimited();
        assert!(
            nfa.may_extend(0, n_rows, &matcher, &mut budget, true)
                .await
                .unwrap()
        );
        assert!(!budget.hit);
        // ...and its start is alive there.
        let mut budget = ScanBudget::unlimited();
        assert!(
            nfa.reaches_boundary_alive(0, n_rows, &matcher, &mut budget, true)
                .await
                .unwrap()
        );
        assert!(!budget.hit);
    }

    /// The reported case: the binder accepts repetition counts up to 1000, and `a{600}` compiles to
    /// a 600-copy chain the old depth cap could never walk to its accept. It must match exactly 600
    /// rows — no fewer, and not greedily more.
    #[tokio::test]
    async fn a_bounded_repetition_beyond_the_old_depth_cap_matches_exactly() {
        let pat = Pattern::Quantified(
            Box::new(Pattern::Var("a".into())),
            Quantifier::Range {
                min: 600,
                max: Some(600),
            },
            false,
        );
        let nfa = Nfa::compile(&pat);
        let spans = |n_rows: usize| {
            let nfa = &nfa;
            async move {
                let matcher = SetMatcher::new(vec![BTreeSet::from(["a".to_owned()]); n_rows]);
                nfa.find_matches_dynamic(n_rows, &matcher, &SkipMode::PastLastRow)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|m| (m.start, m.end))
                    .collect::<Vec<_>>()
            }
        };
        assert_eq!(
            spans(599).await,
            vec![],
            "599 rows cannot complete a{{600}}"
        );
        assert_eq!(spans(600).await, vec![(0, 600)]);
        assert_eq!(
            spans(1300).await,
            vec![(0, 600), (600, 1200)],
            "two exact matches; the 100-row tail cannot complete a third"
        );
    }
}
