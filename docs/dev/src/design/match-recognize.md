# Row Pattern Recognition (`MATCH_RECOGNIZE`)

`MATCH_RECOGNIZE` (SQL:2016 row pattern recognition) finds matches of a regular-expression-like
pattern over the rows of a partition, ordered by a time column, and emits one row per match. It is
the streaming-SQL form of complex event processing (CEP): chains like "a login, then three failed
actions, then a withdrawal within five minutes".

This document covers the streaming implementation. The supported v1 subset is:

- **append-only input only** — a retraction mid-partial-match would invalidate an in-progress or
  completed match, so the semantics over a changelog are ill-defined. The binder/planner rejects
  non-append-only input. (Flink restricts `MATCH_RECOGNIZE` to append-only for the same reason.)
- **`ONE ROW PER MATCH`** — `ALL ROWS PER MATCH` is not yet supported.
- `PARTITION BY` (required, plain columns) and `ORDER BY` (required, leading column must carry a
  watermark).
- `PATTERN`: concatenation, alternation (`|`), grouping, quantifiers (`*`, `+`, `?`, `{n,m}` and
  their reluctant `*?` forms), and `PERMUTE`.
- `DEFINE` predicates with running navigation (`PREV`/`FIRST`/`LAST` and bare `A.col`; physical
  `NEXT` is rejected at bind time — see the comparison table).
  The clause is **required** here — a v1 restriction; SQL:2016 makes it optional (an undefined
  pattern variable defaults to always-true).
- `MEASURES` with `FIRST`/`LAST`/bare `A.col`, `CLASSIFIER()`, `SUBSET`, and the aggregates
  `COUNT(*)`/`COUNT`/`MIN`/`MAX`/`SUM`/`AVG`.
- `AFTER MATCH SKIP PAST LAST ROW` / `TO NEXT ROW` / `TO FIRST|LAST <var>`.
- `WITHIN <interval>` (a streaming time bound on the match span).

## Feature support

The clause is modeled on the two reference implementations RisingWave users come from: Apache Flink
SQL (streaming) and Google BigQuery (batch). The table summarizes RisingWave's v1 support against
them. Flink and BigQuery columns reflect their public documentation as of June 2026 (see Sources);
✅ supported, ❌ not supported, ➖ not applicable.

| Feature | Flink SQL | BigQuery | RisingWave v1 |
| --- | :---: | :---: | :---: |
| Streaming | ✅ | ❌ | ✅ |
| Batch | ✅ | ✅ | ❌ |
| `ONE ROW PER MATCH` | ✅ | ✅ ² | ✅ |
| `ALL ROWS PER MATCH` | ✅ | ❌ | ❌ |
| Concatenation, `*` `+` `?` `{n,m}` | ✅ | ✅ | ✅ |
| Reluctant quantifiers (`*?`) | ✅ ¹ | ✅ | ✅ |
| Alternation (`A \| B`) | ❌ | ✅ | ✅ |
| Grouping + quantifier (`(A B)+`) | ❌ | ✅ | ✅ |
| `PERMUTE` | ❌ | ❌ | ✅ |
| Anchors (`^` `$`) | ❌ | ✅ | ❌ |
| Exclusion (`{- … -}`) | ❌ | ❌ | ❌ |
| Running nav in `DEFINE` (`A.col`, `FIRST`/`LAST`) | ✅ | ✅ | ✅ |
| Physical `PREV` in `DEFINE` | ❌ ³ | ✅ | ✅ |
| Physical `NEXT` in `DEFINE` | ❌ ³ | ✅ | ❌ ⁴ |
| `MEASURES` `FIRST`/`LAST` | ✅ | ✅ | ✅ |
| Aggregates in `MEASURES` (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`) | ✅ | ✅ | ✅ |
| `CLASSIFIER()` | ❌ | ✅ | ✅ |
| `MATCH_NUMBER()` | ❌ | ✅ | ❌ |
| `SUBSET` | ❌ | ❌ | ✅ |
| `AFTER MATCH SKIP PAST LAST ROW` / `TO NEXT ROW` | ✅ | ✅ | ✅ |
| `AFTER MATCH SKIP TO FIRST`/`LAST <var>` | ✅ | ❌ | ✅ |
| `WITHIN` (time bound) | ✅ | ❌ | ✅ |
| Checkpoint / recovery / rescaling | ✅ | ➖ | ✅ |

¹ Flink supports reluctant `+?` / `*?` but not the reluctant optional `??`.
² BigQuery has no `ROWS PER MATCH` keyword; it emits one row per match and requires aggregation in
`MEASURES` (use `ARRAY_AGG` for all-rows-style output).
³ Flink has no physical `PREV`/`NEXT`; its `LAST(expr, n)` takes a *logical* offset (the n-th
last row mapped to the variable), which coincides with a physical offset only when the variable
maps every row.
⁴ Rejected at bind time: a row's verdict would depend on rows after it, which needs per-candidate
decidability (an out-of-range read as a wait for exactly that candidate) — future work.

Sources: [Apache Flink — Pattern Recognition](https://nightlies.apache.org/flink/flink-docs-stable/docs/sql/reference/queries/match_recognize/),
[BigQuery — `MATCH_RECOGNIZE` clause](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#match_recognize_clause).

## Ordering and matching are split

The plan separates the two concerns the clause bundles: establishing `ORDER BY` order over an
out-of-order stream, and matching a pattern over ordered rows.

```text
StreamExchange HASH(partition keys)   -- exact key order (see below)
  └─ StreamWatermarkSort              -- full ORDER BY, releases rows strictly below the watermark
       └─ StreamMatchRecognize        -- same fragment: no exchange in between, order preserved
```

`StreamWatermarkSort` (renamed from `StreamEowcSort` — it is watermark-driven, not
Emit-On-Window-Close-specific) already buffers rows in a state table and, when the watermark on its
sort column advances, emits every buffered row strictly below it in `(sort column, remaining PK)`
order, then forwards the watermark. `MATCH_RECOGNIZE` extends it with **secondary order columns** —
a state-table PK extension right after the sort column — so the release order is the query's full
`ORDER BY`, not just the leading time column. The sort and the matcher sit in the same fragment;
an exchange between them would destroy the order the sort just established.

The matcher is therefore **order-oblivious**: it owns only NFA state and match finalization. All
buffering of unreleased rows, out-of-order arrival, and lateness handling stays in the sort, which
already owned those problems for every other watermark-driven operator.

Two planning details keep the physical layout coherent:

- The enforced input distribution is the **exact** hash over the partition columns in `PARTITION
  BY` order — not "any subset in any order". The matcher's state table hashes its distribution key
  in `PARTITION BY` order, and a row must land on the vnode its state-table key computes; a
  subset-or-reordered exchange would route rows to vnodes the table does not own.
- Rows with a NULL leading order key are filtered out below the sort at plan time: a NULL has no
  event time, no watermark can ever release it, and it cannot be ordered against other rows. This
  mirrors event-time processing dropping NULL-rowtime rows.

## Planning pipeline

The clause flows through the usual layers; each is a thin, conventional addition:

- **Parser** (`src/sqlparser`): `TableFactor::MatchRecognize` plus the `Measure`, `RowsPerMatch`,
  `AfterMatchSkip`, `MatchRecognizePattern`, `RepetitionQuantifier`, and `SubsetDefinition` AST
  nodes.
- **Binder** (`src/frontend/src/binder/relation/match_recognize.rs`): produces `BoundMatchRecognize`
  and registers the output columns (`PARTITION BY` columns, then the measures). The interesting work
  is *lowering* `MEASURES` and `DEFINE` (see below).
- **Logical plan** (`logical_match_recognize.rs`): `LogicalMatchRecognize` with the standard trait
  set. `PredicatePushdown` is a barrier (a predicate over computed output columns must not push below
  the operator); `ColPrunable` prunes the input to the columns the clause's expressions actually
  read. `to_stream` enforces the v1 restrictions and builds the plan above: NULL-order-key filter,
  exact-key exchange, `StreamWatermarkSort` with the secondary order columns, then the matcher.
- **Stream plan** (`stream_match_recognize.rs` + `generic/match_recognize.rs`): `StreamMatchRecognize`
  is append-only and hash-sharded on the partition columns. It declares one internal state table (see
  [State and fault tolerance](#state-and-fault-tolerance)) and carries a `MatchRecognizeInputMode`
  in its proto: `EVENT_TIME` is this plan; `PROCESSING_TIME` (arrival order, no sort) is reserved
  for a follow-up, and the executor rejects any mode it does not implement rather than running
  against input whose ordering guarantee does not hold.

### Lowering `MEASURES` and `DEFINE`

Pattern-variable references (`A.price`, `FIRST(B.ts)`, `PREV(price)`) have no direct analog in the
expression framework, so the binder lowers each measure and define to **an ordinary expression over
a synthetic row**, plus a list of *slots* that describe how to build that synthetic row from a
match:

- A `MeasureSlot` / `DefineSlot` records a navigation kind (`First`, `Last`, `Classifier`, `Prev`,
  `Next`, `RunningFirst`, `RunningLast`, the aggregates, …), the pattern variables it ranges over
  (several, for a `SUBSET`), and the input column it reads.
- The lowered expression is a normal `ExprImpl` whose `InputRef(i)` reads `slots[i]`.

`DEFINE` navigation is **`RUNNING`**, and the candidate row counts as tentatively labeled: while
`A`'s predicate is being tested, `RunningFirst`/`RunningLast` over any variable set containing `A`
(directly or through a `SUBSET`) sees the candidate as the newest such row. That is what makes
`DEFINE A AS LAST(A.v) = A.v` a tautology, matching SQL:2016, where a pattern-variable-qualified
column reference *is* `RUNNING LAST` of that column — the binder already lowers the bare `A.v` inside
`A`'s own `DEFINE` straight to the candidate row, so the two spellings must agree. `MEASURES` is
unaffected: it runs over completed matches whose labels already include the last row.

The corollary is that `DEFINE A AS A.v > LAST(A.v)` is **unsatisfiable** — it compares the candidate
against itself (`x > x`), so `A` never matches and the view stays empty. It does not mean "greater
than the previous `A`". The standard spells that with a logical offset, `LAST(A.v, 1)`, which is not
implemented yet (rejected at bind time). Use the physical `PREV(A.v)` instead, as
`e2e_test/streaming/match_recognize_define_nav.slt` does for its V-shape:
`define down as down.price < prev(down.price)`.

This keeps all type checking, coercion, and constant folding in the existing expression machinery:
the executor materializes the synthetic row per match (or per candidate, for `DEFINE`) and evaluates
the expression over it. `DEFINE` navigation functions are pulled out of the predicate by an AST
pre-walk into a synthetic placeholder relation so the remaining predicate binds normally.

Physical `PREV` needs no retention machinery: the binder admits `PREV(.., k)` only on variables at
least `k` mandatory rows from the match start (an exact minimum-distance walk over the pattern), so
every `PREV` read lands inside the match span, whose rows are retained while the match is live.
Physical `NEXT` in `DEFINE` is rejected at bind time entirely: a verdict depending on rows after
the candidate needs per-candidate decidability, and a global deferral (hold everything by the max
offset) can starve a partition whose match is already decidable.

### The hidden match id

A partition can contain many matches, and two matches may produce byte-identical `PARTITION BY` +
`MEASURES` output, so those columns are not a unique key. The output therefore carries a **hidden
`_match_id` column** (the same mechanism sources use for `_row_id`); the stream key is the partition
columns plus `_match_id`. It is hidden, so `SELECT *` returns only the user columns.

The executor fills `_match_id` with the **match's start row's `seq`** (the matcher-state PK
tiebreaker, assigned when the matcher ingests the already-sorted row — see
[State and fault tolerance](#state-and-fault-tolerance)). This is unique forever — an emitted
match's start row is always consumed, so no later match can share it — and, unlike an id minted at
emission time, it is **deterministic across recovery replay**: re-emitting a match after a rollback
reproduces byte-identical output.

### Emit semantics

The operator emits **only final matches, as soon as they are decidable** — a match is output once
no future row can change it (see [Match finality](#match-finality)), which for many patterns is the
moment its last row arrives, not a watermark later. (Under a spent scan budget this degrades to
watermark latency; see [Degradation under a spent budget](#degradation-under-a-spent-budget).) Every output row is decided once and never
revised or retracted, so the plan node declares Emit-On-Window-Close semantics
(`emit_on_window_close = true`): a query written with an explicit `EMIT ON WINDOW CLOSE` clause is
accepted as naming behavior the operator already has, and the plain form is the normal spelling.

One composition consequence: the operator does not emit a downstream watermark, so stateful
operators that need one under Emit-On-Window-Close (e.g. an aggregation) cannot sit above
`MATCH_RECOGNIZE` **inside the same** `EMIT ON WINDOW CLOSE` view. Compose across views instead:
the `MATCH_RECOGNIZE` view's output is append-only, and a plain (default-emit) view can aggregate
it. Stateless operators (projections, filters) compose freely within the same view.

Empty matches are not emitted: `PATTERN (a*)` produces no summary row over non-matching input.
(SQL:2016 would emit one row per empty match; Flink rejects empty-matchable patterns outright.)

Emission within a partition is in match start order (leftmost priority), so under an overlapping
skip mode an already-decided match can wait behind an earlier, still-undecided one.

## The NFA

`src/stream/src/executor/match_recognize/nfa.rs` is a self-contained, pure module (unit-tested
without a cluster). A `Pattern` (variable / concat / alternation / quantified / permute) is compiled
by Thompson construction into an `Nfa` whose labelled transitions are pattern variables.

Matching is **predicate-driven**: rather than precomputing which variables each row satisfies, the
matcher consults a `CandidateMatcher` as it walks the NFA, so `DEFINE` predicates that depend on the
running match (e.g. `B AS B.price > A.price`) can be evaluated against the rows matched so far. The
matcher returns the *first accepting path in transition order*; greedy quantifiers order the loop
edge first (longest match), reluctant quantifiers order the exit edge first (shortest), and
alternation prefers its first branch. `PERMUTE` expands to the alternation of all orderings (capped
to keep the factorial bounded; the executor-side pattern decoder re-validates the caps so a corrupt
or version-skewed plan fails with an error instead of allocating).

Backtracking over predicates is worst-case exponential, so every walk — the match finder itself,
liveness checks, extension probes — runs under two defenses:

- a **scan budget** — a per-visit cap on walk *steps*: predicate evaluations plus every edge
  taken, ε-transitions included. Metering only predicate evaluations, as it originally did, left
  ε-traversal free, so a large compiled NFA could spend arbitrary CPU per metered evaluation and
  the budget was not actually a CPU bound. The walkers are iterative — one depth-first search over
  an explicit heap stack (`Nfa::walk`) serves the finder, the liveness check and the extension
  probe — so a walk's depth is bounded by memory, not by the thread stack, and the budget is the
  only bound on it. (An earlier recursive implementation needed a hard depth cap to avoid
  overflowing the stack, and the cap made any match spanning more than a couple of hundred rows
  permanently undecidable: unlike the budget, it did not reset between visits, so every refresh
  died at the same depth.) Exhaustion is never converted into a *structural* verdict: the walk stops, the caller treats the position as undecided, nothing is
  frozen, the condition is counted in a metric and reported once per pass, and the next visit retries
  with a fresh budget. The budget is scoped per row on the data path and per partition visit on the
  watermark path, so one pathological partition cannot starve the others.

  Two things a spent budget *does* still decide, both from the watermark rather than from predicate
  evaluation, and neither refinable by spending more: a match whose `WITHIN` window has closed is
  FINAL and is emitted, and a row whose window has closed carries no live match and is evicted. See
  [Degradation under a spent budget](#degradation-under-a-spent-budget) for what that means
  operationally, including the case where it does not help at all.
- a **failure memo** over `(state, position)`, recorded only at row-consumption boundaries. It is
  sound only when no `DEFINE` slot reads the running label assignment (all slots `SelfCol`/`Prev`);
  the executor computes that classification once per query. For such path-independent patterns the
  memo removes the exponential blowup outright; the budget remains as the backstop for the rest.

## The executor

`MatchRecognizeExecutor` consumes rows already in full `ORDER BY` order, with partitions
interleaved. Per partition it keeps a `PartitionRun`: the retained rows (exactly those a live
partial or held match still references) and an `IncrementalMatcher`.

- **Feed on arrival.** Each released row is appended to the state table and to its partition's run,
  and fed to the matcher, which re-derives the provisional match set over the *unfrozen suffix* —
  consumed history is never rescanned, but a partition holding an open partial re-scans its live
  window per arriving row (see [Known costs](#known-costs-and-future-work)). The matcher also
  *freezes* the leading run of matches whose whole scan region is dead at the fed boundary: no
  future row can change them, so they are immutable from then on.
- **Emit on decidability.** After each feed (and on each watermark), the executor emits the first
  provisional match while it passes the finality gate below. Emitting consumes the buffer prefix up
  to the match's `AFTER MATCH SKIP` resume position — including earlier still-live partials, which
  is exactly the abandonment a batch scan performs when it resumes past an emitted match, and what
  keeps `_match_id` unique. Consumed rows are deleted from the state table in the same epoch.
- **Watermarks** drive only two things: `WITHIN` finality (a held match whose deadline the
  watermark has strictly passed can no longer be extended or superseded) and **dead-prefix
  pruning** (rows before the first position that is still a live match start — structurally alive
  at the boundary and, under `WITHIN`, its window still open — can never join a match again and are
  deleted). The watermark pass visits every partition, so an idle partition's timed-out partial is
  emitted or evicted without new input in that partition.

### Match finality

"A later row exists" is **not** finality, and the emission gate does not use it. Two shapes show
why. For `PATTERN (a b c d | a b)` over rows `a,b,c`, the finder's provisional match is `(a b)` and
a row follows it — but the preferred first branch consumed `a,b,c` and is blocked at the *buffer*
boundary, so a future `d` changes the answer to the four-row match. For `PATTERN (x n n | n)` over
rows `x,n`, the lone `(n)` match is terminal from its own start — but position 0 is still alive
inside the preferred branch, and a future `n` makes the batch answer the leftmost `(x n n)`, which
consumes the very row the lone `(n)` was built on.

The gate (`match_is_final`) therefore asks exactly what batch equivalence requires:

1. every *gap position* between the matcher's resume point and the match's start is provably dead
   at the boundary (an alive gap could still yield an earlier, leftmost-preferred match), and
2. no path the finder *prefers* over the current result can be completed by future rows —
   `Nfa::may_extend`, probed at the buffer boundary, walking the finder's own preference order and
   stopping at its first accept; a lower-priority path never holds a match; or
3. the match's `WITHIN` deadline is strictly below the watermark, which decides both questions at
   once: every gap row's window closed no later than this match's (order keys are non-decreasing),
   a gap match over existing rows would already have been the finder's leftmost result, and no
   future row can satisfy the span bound for any start at or before this one.

On a spent scan budget the two *structural* conditions answer "hold". The `WITHIN`-finality
condition still answers "emit", because a closed window is a fact the watermark supplies: no future
row can satisfy the span bound for this start, so no amount of predicate evaluation could refine it.
That is what lets a starved partition shed a match at all — provided the scan found one to shed.

### Degradation under a spent budget

Worth stating plainly, because the two cases differ and only one of them recovers.

**With `WITHIN`.** A starved partition still sheds matches, but only through window closure, and
only matches the truncated scan reached. Each watermark visit re-derives the tail (spending the
whole budget), then emits the head if its window has closed. Emitting a provisional match rebuilds
the matcher under that same spent budget, which empties the tail and ends the drain — so the
practical rate is about **one match per watermark visit**, and the deadline prune contributes
nothing while the matcher is incomplete. Emission latency degrades from decidability to window
closure, and the retained set shrinks only at that rate: if arrivals per watermark interval exceed
it, the partition still grows. This is an improvement on shedding nothing; it is not convergence.
When the starvation is in the rescan itself — a long pending run over which no match has completed,
so the tail is empty — window closure has nothing to drain and this path sheds nothing either.

**Long matches.** Exhaustion is not only a backtracking phenomenon: a *linear* chain pattern
(`a{600}`) reaches it through sheer length. Walking every pending start to the boundary costs Θ(k²)
per rescan, and freezing a match of `L` rows — one liveness walk per position of its region, each
walking up to `L` rows — costs Θ(L²), past the 2^20 budget from `L ≈ 600`. Four things keep such
patterns decidable within the binder's repetition limit of 1000, each a verdict derived or
remembered instead of re-walked:

- the finder skips a start with fewer rows to the boundary than the shortest match the pattern
  admits (`Nfa::min_match_rows`, a compile-time 0-1 BFS) — the verdict a walk would reach by
  consuming its way there, taken without the walk;
- for an acyclic pattern, a position with more rows to the boundary than the automaton can
  consume (`Nfa::max_match_rows`) is dead without a walk — which makes freezing a chain match
  free: every position of its region has the rest of the match, and more, ahead of it;
- the finder remembers **matchless** starts: a start whose walk found no accept and never reached
  the boundary died entirely on rows below it, which are immutable, so it can never match at any
  later boundary. The matcher keeps the contiguous prefix of such starts
  (`IncrementalMatcher::matchless_upto`) and the next rescan begins past it: a run of `r` rows
  broken by one non-matching row costs its Θ(r²) once, amortised across visits, instead of on
  every rescan forever;
- the freeze is **resumable**: deadness at the boundary is monotone under appends (a walk reads
  only rows at or before its position; there is no forward navigation), so the matcher keeps the
  prefix of positions its freeze walks have proven dead (`IncrementalMatcher::dead_prefix_end`)
  and the next rescan continues from there — and a freeze cut short by the budget asks for a
  refresh on the next watermark visit, so an idle partition resumes it too. The executor's own
  liveness walks (the dead-prefix prune, the emission gate's gap check) skip that prefix as well.

Both memories are forgotten wherever the rows a verdict was computed over can change: truncation,
and the eviction rebase. What remains inherently per-visit is a run that stays *alive* — `a{600} b`
over an unbroken run of `a` rows keeps every start alive until a `b` arrives or its `WITHIN` window
closes — where each rescan re-walks the live starts and the budget throttles the partition as
described above; `WITHIN` is what bounds that.

**Without `WITHIN`.** There is no deadline, so `WITHIN`-finality is unreachable and nothing above
applies: the partition emits nothing and evicts nothing, and because per-visit cost grows with the
retained set, each failed visit makes the next one likelier to fail. That state is absorbing — it
does not recover on its own, and the practical remedy is to drop and recreate the view.

So the two remedies the runtime report names are not interchangeable. Simplifying nested
optional/alternation quantifiers addresses the cause. Adding or tightening `WITHIN` is what makes the
degradation bounded rather than absorbing, and is the one that matters if a partition is already
stuck. A `scan_budget_exhausted` count that is nonzero and rising on a query without `WITHIN` should
be read as a stuck partition, not as slow progress.

`e2e_test/streaming/match_recognize_preference_supersession.slt` pins both shapes end to end, with
both endings each (the superseding row arrives; a killing row decides the held match), plus an
idle-partition control (an `(n)` with no `x` before it must emit at arrival — holding it would
starve the partition).

### The watermark boundary is strict

A RisingWave watermark `w` promises only that **no future row will have `order_key < w`** — a row
with `order_key == w` may still arrive, and `WatermarkFilterExecutor` forwards it (it keeps
`event_time >= watermark`). Every finality decision is therefore expressed with a strict `< w`:

- the sort releases rows with `order_key < w` (a row at `w` may still arrive and must sort);
- a `WITHIN` window is closed only when `deadline < w` (at `deadline == w` a completing row at
  `order_key == w` still falls inside the inclusive span bound);
- dead-prefix pruning retains rows whose window is still open under the exact complement,
  `deadline >= w`.

A deadline that overflows the order key's type — `first + bound` past the type's maximum — is a
window that **never closes**: every representable order key lies inside the span, and no
representable watermark can pass the deadline. The deadline expression is non-strict like every
other streaming expression, so the overflow surfaces as NULL — and a NULL here can only be an
evaluation error, since the order key is non-null for every buffered row and a NULL bound is
rejected at bind time. The executor reads that NULL as the never-closing state (`Deadline::Never`),
and the expression is built over an error report that drops the out-of-range error instead of
counting it as a compute error on every affected row. Reading the NULL as an ordinary value instead
made the span check treat it as "outside the window", which silently rejected valid matches at the
top of the key's range while leaving their partials unevictable.

The emit test and the prune test must stay in lockstep: if pruning were laxer than emission,
eviction would delete the rows of a match the emit side is still holding, and the match would be
lost with no trace.

### Invalid `AFTER MATCH SKIP` targets degrade, and say so

`AFTER MATCH SKIP TO FIRST|LAST <var>` has no valid resume row in two data-dependent cases — the same
query hits them or not depending on which rows arrive:

| case | example | resume position | degrades to |
|---|---|---|---|
| the target is bound to no row of the match | `PATTERN (a b?)` matching only `a`, `SKIP TO LAST b` | the match end | `SKIP PAST LAST ROW` |
| the target is the match's own first row | `PATTERN (a b)`, `SKIP TO FIRST a` | `match start + 1` | `SKIP TO NEXT ROW` |

The SQL standard prescribes a runtime error for both (Oracle raises ORA-62511 / ORA-62512; Flink
likewise). This implementation keeps the degradation and **reports** it instead of raising it. The
condition is data-dependent, and the materialized view is already committed by the time any row
arrives: an error would abort the actor, recovery would replay the same rows, and the actor would die
again — a recoverable query turned into a crash loop. No RisingWave streaming operator fails an actor
for a data-dependent condition; every hard error in this operator is a contract or plan violation
(non-append-only input, an unknown slot kind, a provisional match referencing an unknown row) that
recovery cannot fix either way.

So the degradation is made visible rather than fatal. `SkipMode::next_pos` returns the resume position
plus an optional diagnostic (keeping `nfa.rs` pure — it holds no error reporter), and the executor's
emit path routes it to the actor's `EvalErrorReport`: the same *surface* expression evaluation errors in
this operator already use, i.e. the rate-limited `stream_expr_error` log and the `user_compute_error`
metric. The reported error names the skip mode, its target variable and the strategy actually applied:

```text
Invalid parameter AFTER MATCH SKIP TO LAST: target variable `c` is bound to no row of the match,
so there is no row to resume at; the scan resumed past the match's last row instead (degraded to
SKIP PAST LAST ROW)
```

Two things about that surface are worth knowing when reading the output. First, while the surface is
precedented, the *carrier* is not: no other `EvalErrorReport` user synthesizes an error — they all pass
on one produced by an actual expression evaluation. `ExprError` is simply the only type the trait
accepts, and `InvalidParam` is the honest fit (the query's `AFTER MATCH SKIP` parameter cannot be
honored); `ExprError::Custom` was rejected as the UDF error channel and slated for removal. A
consequence is that the log line carries the surface's fixed prefix `failed to evaluate expression`,
hardcoded in `ActorContext::on_compute_error`, even though nothing was evaluated — the actionable
content is the self-contained `error=` field, not the head of the line. Second, the metric labels
(`["ExprError", executor_name, fragment_id]`) separate this operator from others but not from this
operator's own `DEFINE`/`MEASURES`/`WITHIN` evaluation errors, so the metric reads as "this
`MATCH_RECOGNIZE` query is unhealthy" and the log line is the artifact that says why.

Reporting is deduplicated **per kind per message pass**. The cause is a property of the query, not of
one row: a target no match can bind degrades on every match forever, and one that only sometimes fails
to bind (`PATTERN (a? b)` with `SKIP TO FIRST b`) still repeats without bound. Every repetition within a
pass would be a byte-identical duplicate, so one report per kind per pass keeps the signal steady and
bounded by message frequency rather than by match or partition count.

Note the related bind-time check: a `SKIP TO FIRST|LAST <var>` target that is not a *pattern* variable
at all (e.g. a `DEFINE`-only symbol) is rejected when the query is bound, so it never reaches the
executor.

## State and fault tolerance

The matcher declares **one** internal state table of the retained rows, layout
`[ seq (i64), <input columns…> ]`, keyed by `(partition columns, ORDER BY columns, seq)` and
distributed by the partition key. Keying by the order columns keeps the state physically sorted in
feed order, so recovery can re-feed each partition without an in-memory sort. Only the raw rows are
persisted — the NFA is recompiled from the pattern at startup, matcher state is re-derived by
re-feeding, and `DEFINE`/`MEASURES` are evaluated at match time. (The upstream `WatermarkSort` has
its own buffer table for rows not yet released; a row lives in exactly one of the two at every
checkpoint.)

`seq` is the matcher state table's PK tiebreaker for rows with equal `ORDER BY` keys. It does not
choose their order: `StreamWatermarkSort` has already emitted them using its physical table key
before the matcher assigns `seq`. The counter must be **monotonic in matcher ingestion order** so
recovery re-feeds ties in the same order the live matcher saw; changing that order would silently
change which row a match binds. It is a plain per-actor counter whose seed is the maximum retained
seq **or the barrier epoch's physical time (`<< 20`), whichever is larger**. The epoch floor is
load-bearing, not belt-and-braces: consumed rows are deleted, so a retained-max-only seed would
re-mint the seqs of fully-consumed matches after a restart or rescale — and a reused seq collides
`_match_id`s, silently replacing an earlier match's row in the materialized view. Barrier epochs'
physical time is strictly increasing (the meta service restores it from the maximum committed
epoch across failover), so the floor clears every seq ever minted. A partition is owned by one
actor at a time, so per-partition uniqueness survives ownership moves.

- **Recovery.** After the first barrier the executor scans its owned vnodes and re-feeds each
  partition's rows into a fresh matcher — one batched feed per partition. No emission happens
  during rebuild, and none is needed: a match that becomes emittable is always consumed in the same
  epoch it emits, so retained rows can only carry held or partial matches; anything mid-epoch at a
  crash is re-delivered by replay and re-triggers naturally.
- **Rescaling.** On a vnode-bitmap change the same rebuild runs over whatever the actor now owns.
  The skip-resume position and `_match_id` are reconstructed deterministically from the surviving
  rows, so a partition moving between actors emits the same matches it would have without the move.
- **Parallelism.** Matching is independent per partition, so the input is hash-sharded by the
  `PARTITION BY` key and each actor owns its partitions' state. Within a partition, ordered
  semantics preclude parallelism — one hot partition serializes its actor's pass, bounded per visit
  by the scan budget.

### State bound and `WITHIN`

Retained rows are exactly those a live partial or held match still references. What bounds them
depends on whether the pattern carries a `WITHIN` clause:

- **With `WITHIN <interval>`** the span of any match is capped: once the watermark is strictly past
  a row's window, the row can no longer begin or extend a match, and the watermark pass prunes it —
  including in partitions receiving no further input. State per partition is bounded by the rows
  arriving within one `WITHIN` window — a bound in *event time*, not in row count: memory still
  scales with the partition's input rate over that window and with the pattern's retention.
- **Without `WITHIN`** a partial can be completed by an arbitrarily distant future row — `PATTERN
  (A B)` retains an `A` until some later `B` arrives, however long that takes. This is correct SQL
  semantics (a streaming join without a time bound retains its build side the same way), but the
  resulting bound depends on the pattern's shape. A pattern whose live partial spans a bounded
  number of rows (like `(A B)`) retains a bounded suffix per partition, so state is bounded by the
  number of distinct `PARTITION BY` keys. A pattern with an unbounded quantifier that stays
  satisfiable — `(A+ B)` on a partition whose rows keep satisfying `A` while `B` never arrives —
  keeps position 0 boundary-alive forever, so the dead-prefix prune never advances and **every row
  of that partition is retained**: state grows linearly with that partition's input, with no bound
  from key cardinality at all. The binder emits a `NOTICE` naming both regimes.

Unlike the previous EOWC-based design, the retained rows are **resident in executor memory** (each
partition's run holds them alongside the matcher), not merely persisted: the in-process footprint
tracks the same bound. Wiring this into RisingWave's memory accounting is a planned follow-up.

### Observability

Four counters and one gauge are labelled `(table_id, actor_id, fragment_id)`, like the neighbouring
over-window set:

- `stream_match_recognize_matches_emitted_count`: matches emitted.
- `stream_match_recognize_evicted_rows_count`: rows leaving the matcher buffer, whether consumed by
  an emitted match or pruned as a dead prefix.
- `stream_match_recognize_scan_budget_exhausted_count`: the alerting hook for
  catastrophic-backtracking degradation. The log line is deduplicated per pass; the counter counts
  every affected partition visit.
- `stream_match_recognize_within_deadline_overflow_count`: buffered rows for which adding the
  `WITHIN` bound exceeds the `ORDER BY` type's range. Their deadline becomes unbounded, so the
  window never closes through watermark progress.
- `stream_match_recognize_retained_rows` (gauge): rows currently resident in matcher memory, summed
  across the actor's partitions.

## Semantic edges for CEP use

The intro calls this the streaming-SQL form of complex event processing; measured against the full
CEP problem space, v1 is a *sequence* detector — ordered, contiguous, occurring point events within
one partition and one layer. The edges below are properties of SQL:2016 or of watermark-driven
streaming rather than defects, but each is the kind a pattern author discovers in production if the
documentation does not say it first.

### Non-occurrence is not expressible

"Payment with no confirmation within 5 minutes" — detecting the *absence* of an event — has no
spelling here: the pattern language has no negation (SQL:2016 has none either), and a partial match
whose `WITHIN` deadline passes is evicted, not emitted, so the moment the operator knows the
absence occurred is the moment it deletes the evidence. The closest in-language approximation,
`PATTERN (payment nc*? witness)` with a filler excluding confirmations, fires only when a *later
row happens to arrive* in the same partition (a silent partition is never flagged) and cannot be
`WITHIN`-bounded (the span check caps the witness inside the bound being exceeded). A timeout
side-output channel (Flink CEP precedent: timed-out partials as a second stream) is the candidate
future feature; the prune pass already has the expiring rows in hand.

### One layer: matches cannot feed a second `MATCH_RECOGNIZE`

The operator forwards no watermark, and `MATCH_RECOGNIZE` requires a watermarked `ORDER BY` column
— so a second `MATCH_RECOGNIZE` (or any watermark-needing operator) can never consume a
`MATCH_RECOGNIZE` view, directly or across views. The composition note under
[Emit semantics](#emit-semantics) covers plain aggregation only. The interim path for hierarchical
detection is `CREATE SINK ... INTO` an append-only table that declares its own `WATERMARK FOR` on
a time measure — the watermark is re-derived from the match rows — but it carries a lateness
budget: a `WITHIN`-held match emits up to `bound + upstream lateness` after its own time measure,
so the intermediate table's allowance must exceed that or exactly the timeout-decided matches are
silently dropped as late, and each layer's allowance adds to end-to-end finality latency. A derived
output watermark (`w − bound`, valid under `WITHIN` by the same invariant the prune pass enforces)
is the candidate future feature that would make direct layering work.

### Simultaneous events are linearized

Rows tying on the full query `ORDER BY` are ordered by `StreamWatermarkSort`'s remaining physical
table key: distribution-key columns followed by upstream stream-key columns, excluding columns
already present in the order key. For an append-only table with a primary key, this commonly means
that tied rows follow primary-key order rather than arrival order. The matcher assigns `seq` only
after the sort emits a row; `seq` preserves that established order in matcher state and does not
choose it.

The physical tie order is stable across recovery and rescale while those keys stay the same, but it
is not part of the SQL query's ordering contract and can change when the physical keys or row
identities change. Such a change can alter an order-sensitive result — including breaking a
contiguous match when a third same-key row lands between two others. This is standard-conformant (a
non-total `ORDER BY` makes results implementation-dependent), and two supported mechanisms make tie
order intentional instead of physical: add secondary `ORDER BY` columns that make the query order
total, or use `PERMUTE` for order-independent steps (`PERMUTE` is not available in Flink or
BigQuery).

### Without `WITHIN`, watermark progress finalizes nothing

A completed match that a preferred path could still extend — a trailing greedy quantifier
(`login fail+`), a longer-listed alternative — is held until the partition's *next row* decides it
or a `WITHIN` deadline passes. Without `WITHIN`, that hold is unbounded: a quiet partition's match
may emit days later or never, regardless of watermark progress. `WITHIN` is therefore the latency
bound, not merely the state bound the `CREATE`-time notice describes.

### Contiguity: `(A B)` means adjacent rows

SQL row-pattern matching consumes every row in the span: `PATTERN (A B)` requires `B` to be the
*immediately next row* in the partition's order, so in a partition mixing event kinds — the normal
CEP topology — any interleaved row kills the naive pattern silently. The idiom is an explicit
reluctant filler, `PATTERN (A x*? B)` with `x` defined as the don't-care condition (Flink CEP's
`followedBy`, Esper's `->`). Fillers are real matched rows: they count in `COUNT(*)` over the
universal set and are constrained by `WITHIN` like any other row. Per-step gaps are expressible
without `WITHIN` via `DEFINE` predicates over the time column
(`b AS b.ts <= a.ts + INTERVAL '1' MINUTE`).

### Timestamps are not causality

A detected pattern certifies temporal order on the `ORDER BY` column plus predicate
co-occurrence — not that the earlier event caused, or even truly preceded, the later one.
Cross-service clock skew can invert a genuinely causal pair; and a row dropped as late by the
upstream `WatermarkFilter` (whose lateness allowance lives in the *source's* DDL, not in this
clause) does not just miss its own patterns — it changes what `PREV` and filler predicates see for
neighboring rows, which can produce false positives. An idle source stalls the watermark and with
it all watermark-driven finality (the e2e tests advance it with high-timestamp sentinel rows for
exactly this reason). One more Flink-migration note: `WITHIN` here is an inclusive first-to-last
span bound (`last − first <= bound`) — a match completing at exactly the bound matches here and
not on Flink, whose window is exclusive.

## Known costs and future work

- **Per-row rescan of the live window.** A partition holding an open partial re-derives its
  provisional matches over the unfrozen suffix on every arriving row; incrementalizing the
  provisional tail is the main planned performance follow-up, along with per-row predicate caching,
  `WITHIN` deadline precompute, and label interning.
- **Watermark passes visit every partition.** A per-partition wakeup frontier (a deadline index)
  would make the pass proportional to the partitions that actually need attention; the previous
  design carried one, and reintroducing it on this architecture is future work.
- **A row is persisted twice across the fragment** — once in the sort's buffer, once in the
  matcher's retained rows — the storage cost of the ordering/matching split.
- `ALL ROWS PER MATCH`, `MATCH_NUMBER()`, anchors (`^`, `$`), exclusions (`{- … -}`), batch
  execution, and non-append-only input are not supported. Anchors and exclusions are rejected at
  parse time (the grammar does not yet produce them; the AST carries their variants for later).
- `PROCESSING_TIME` input mode (arrival order, no sort; replay non-determinism to be documented)
  is reserved in the proto and not yet implemented.
- Candidate features from the [semantic edges](#semantic-edges-for-cep-use): a timeout side-output
  channel for expired partials, and a derived output watermark (`w − bound` under `WITHIN`) to
  unlock direct `MATCH_RECOGNIZE`-over-`MATCH_RECOGNIZE` composition.
