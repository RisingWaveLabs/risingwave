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

//! Incremental driver over the row-pattern [`Nfa`].
//!
//! The batch matcher [`Nfa::find_matches_dynamic`] rescans the whole buffer from position 0 on every
//! call. Under append-only input (rows arriving in `ORDER BY` order) most of that work is redundant:
//! `AFTER MATCH SKIP` makes the matches before a committed skip-resume point immutable, because no
//! row appended *after* them can change a match that already terminated *before* them. This wrapper
//! keeps that skip-resume point as a scan cursor and, on each [`IncrementalMatcher::advance`], reruns
//! `find_matches_dynamic` **only over the suffix that can still change** — never reimplementing the
//! NFA traversal (and so never bypassing its greedy/reluctant preference, fresh-visited-scope, or
//! `WITHIN` invariants).
//!
//! Matches are anchored by row *seq* rather than buffer *position* so they stay stable when earlier
//! rows are evicted and finalized (see [`IncrementalMatcher::finalize_evicted_prefix`]); positions are an
//! internal detail of the current buffer.
//!
//! Freezing rule (see [`IncrementalMatcher::advance`]): a match — and the scan region behind it up
//! to its skip-resume position — freezes only once *every* position in that region is dead at the
//! current boundary per [`Nfa::reaches_boundary_alive`] (the same liveness predicate row eviction
//! uses). A dead position's scan outcome can never change under appended rows, because no path from
//! it can consume past the old boundary; so the whole region's scan behavior — matches found, gaps
//! skipped, and the resume point — is final. Any live position (a still-open trailing match, or a
//! gap where a longer, higher-preference alternative is still in flight) keeps the region
//! provisional and re-attempted on the next advance.
//!
//! A late (out-of-order) row that sorts *before* rows already fed is handled by
//! `IncrementalMatcher::truncate_from_seq`: it rolls state back to a scan-resume point at or before
//! the insertion, re-verifying the freezing gate against the truncation boundary (freezing is only
//! sound against the boundary it was checked at), after which the caller re-feeds the corrected
//! sorted suffix through `advance`.
//!
//! **Scaffolding note:** under the `EVENT_TIME` plan the upstream `WatermarkSort` makes out-of-order
//! feeds unreachable, so `truncate_from_seq` and the provisional-changelog helpers
//! (`diff_provisional`, `plan_provisional_rows`, `fed_seqs`) have no production caller and are
//! compiled `#[cfg(test)]`. They are kept — proven by the randomized differential oracle in this
//! module — as the invalidation machinery a future input mode that revises already-fed rows (e.g.
//! an emit-on-update or arrival-order mode) would need, rather than shipped as live surface.

#[cfg(test)]
use std::cmp::Ordering;
#[cfg(test)]
use std::collections::HashMap;

#[cfg(test)]
use risingwave_common::array::Op;
#[cfg(test)]
use risingwave_common::row::OwnedRow;

use crate::executor::error::StreamExecutorResult;
use crate::executor::match_recognize::nfa::{
    CandidateMatcher, LabeledMatch, MatchScan, Nfa, ScanBudget, SkipMode,
};

/// A row's stable sequence number: the buffer-table PK tiebreaker minted at ingest, unique for the
/// buffer's lifetime and stable across eviction. A newtype (not a bare `i64`) so a seq can never be
/// confused with a buffer *position* (a bare `usize`) at the incremental-matcher seams — the two are
/// different integer spaces. The raw `i64` is unwrapped (`.0`) only at the two real boundaries: the
/// state-table storage (de)serialization, and where the `_match_id` output datum is built.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Seq(pub i64);

/// A match anchored by row seqs (stable across eviction), not buffer positions. `start_seq` is the
/// seq of the match's first row; `end_seq` is one past the seq of its last row (so `end_seq -
/// start_seq` equals the row count only while seqs are contiguous). `labels[i]` is the pattern
/// variable bound to the match's `i`-th row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeqMatch {
    pub start_seq: Seq,
    pub end_seq: Seq,
    pub labels: Vec<String>,
}

/// Outcome of [`IncrementalMatcher::finalize_evicted_prefix`]: whether the matcher could finalize the
/// evicted prefix in place (staying reusable) or the eviction shape forces the caller to drop and
/// rebuild it.
#[derive(Debug)]
#[must_use = "a dropped MustRebuild leaves a silently stale matcher — match on the result"]
pub enum Finalized {
    /// The evicted prefix was finalized and the matcher rebased onto the surviving buffer; keep it.
    /// The finalized matches themselves are not carried: the executor prunes its diff base by
    /// surviving start seqs, uniformly across this and the drop-and-rebuild path, so a payload here
    /// would be an allocation nobody reads.
    Rebased,
    /// The eviction cannot be rebased across — a boundary never fed, past the frozen prefix, or a
    /// frozen match straddling the boundary strictly *inside* the frozen prefix (`final_pos <
    /// next_pos`, reachable only via a direct API call, never the executor's eviction) — so the
    /// matcher was left untouched and the caller must drop it and let the next visit rebuild from
    /// the surviving buffer.
    MustRebuild,
}

/// Diff two provisional match sets — each a `(match, output row)` list, in **any order** — into the
/// changelog ops that turn `old` into `new`. Match identity is `start_seq` (the emitted
/// `_match_id`):
///
/// * a match in `old` but not `new` (its start vanished) → `Delete` of its old row;
/// * a match in `new` but not `old` (a brand-new start) → `Insert` of its new row;
/// * a match present in both whose extent (`end_seq`), labels, or output row changed →
///   `Delete(old)` then `Insert(new)`;
/// * an unchanged match → no ops.
///
/// Ops come out start-seq ascending, `Delete` before `Insert` for a revised identity — the retract
/// encoding of an update to a `_match_id` key.
///
/// The sort-by-identity the merge needs is enforced *here*, not assumed of the caller: the executor
/// hands over [`IncrementalMatcher::provisional`]-derived lists, which are in the matcher's scan
/// (buffer-position) order — and under out-of-order arrival that is **not** start-seq order, because
/// seqs are minted at arrival, so a late row that sorts earlier carries a *higher* seq at an
/// *earlier* position. A merge over such inputs would pair unrelated identities (net-deleting a
/// match that is still in `new`).
/// Test-only on this executor: the emit-on-update mode that drives it is not part of this
/// operator; the differential oracle exercises it so the contract stays proven.
#[cfg(test)]
pub fn diff_provisional(
    old: &[(SeqMatch, OwnedRow)],
    new: &[(SeqMatch, OwnedRow)],
) -> Vec<(Op, OwnedRow)> {
    // Start seqs are unique within one provisional set (matches start at distinct rows), so this is
    // a total order on each side and the linear merge below is well-defined.
    let mut old_sorted: Vec<&(SeqMatch, OwnedRow)> = old.iter().collect();
    old_sorted.sort_by_key(|(m, _)| m.start_seq);
    let mut new_sorted: Vec<&(SeqMatch, OwnedRow)> = new.iter().collect();
    new_sorted.sort_by_key(|(m, _)| m.start_seq);

    let mut ops = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < old_sorted.len() && j < new_sorted.len() {
        let (om, orow) = old_sorted[i];
        let (nm, nrow) = new_sorted[j];
        match om.start_seq.cmp(&nm.start_seq) {
            // `old`'s start sorts first and has no `new` counterpart: it vanished.
            Ordering::Less => {
                ops.push((Op::Delete, orow.clone()));
                i += 1;
            }
            // `new`'s start sorts first and has no `old` counterpart: it is brand new.
            Ordering::Greater => {
                ops.push((Op::Insert, nrow.clone()));
                j += 1;
            }
            // Same identity: emit a Delete+Insert pair when the extent, labels, or output row
            // changed; nothing when the match is byte-for-byte unchanged.
            Ordering::Equal => {
                if om.end_seq != nm.end_seq || om.labels != nm.labels || orow != nrow {
                    ops.push((Op::Delete, orow.clone()));
                    ops.push((Op::Insert, nrow.clone()));
                }
                i += 1;
                j += 1;
            }
        }
    }
    // Only one side can have leftovers; their start_seqs all exceed everything emitted so far, so
    // appending them (deletes for `old`, inserts for `new`) keeps the run start-seq ascending.
    for (_, orow) in &old_sorted[i..] {
        ops.push((Op::Delete, orow.clone()));
    }
    for (_, nrow) in &new_sorted[j..] {
        ops.push((Op::Insert, nrow.clone()));
    }
    ops
}

/// The row-independent half of the emit-on-update diff: decide, per new provisional match, whether
/// its output row can be reused from the base or must be (re)built — *before* building any row.
///
/// Returned positionally over `new`: `Some(i)` means base entry `old[i]` holds a byte-identical
/// output row to reuse; `None` means a fresh row must be built. Reuse is sound exactly when identity
/// (`start_seq`) and content (`end_seq`, `labels`) all match: the output row is a pure function of the
/// append-only matched rows plus their labels, so an identical `(start_seq, end_seq, labels)` triple
/// pins byte-identical rows. (A late row landing *inside* the span would make the contiguous match
/// cover one more position, lengthening `labels`; a row landing *outside* the span shifts positions
/// but leaves the covered rows' seqs and values untouched — append-only rows never change.)
///
/// This lets the executor call the expensive `build_match_row` only for `None` entries: in the steady
/// state a partition's provisional set is unchanged barrier-to-barrier, so every entry reuses and
/// nothing is rebuilt. The changelog is still assembled by [`diff_provisional`] in the same order;
/// because a reused row equals its base row and a changed match differs in `end_seq`/`labels`,
/// `diff_provisional`'s row-inequality tie-break never independently fires on the executor path (it
/// remains live only for synthetic-row callers such as the unit tests).
/// Test-only on this executor: the emit-on-update mode that drives it is not part of this
/// operator; the differential oracle exercises it so the contract stays proven.
#[cfg(test)]
pub fn plan_provisional_rows(old: &[(SeqMatch, OwnedRow)], new: &[SeqMatch]) -> Vec<Option<usize>> {
    let mut by_seq: HashMap<Seq, usize> = HashMap::with_capacity(old.len());
    for (i, (m, _)) in old.iter().enumerate() {
        by_seq.insert(m.start_seq, i);
    }
    new.iter()
        .map(|m| {
            by_seq
                .get(&m.start_seq)
                .copied()
                .filter(|&i| old[i].0.end_seq == m.end_seq && old[i].0.labels == m.labels)
        })
        .collect()
}

/// Incremental wrapper around [`Nfa::find_matches_dynamic`] for append-only input.
///
/// Rows are fed in `ORDER BY` order via [`IncrementalMatcher::advance`]; their buffer position is
/// implied by feed order and mapped back to a stable seq through `seq_index`. Everything before
/// `next_pos` (the skip-resume point after the last frozen match) is immutable and never rescanned.
pub struct IncrementalMatcher {
    /// Shared compiled pattern: one matcher per partition (and a fresh one per full consumption),
    /// so holding the automaton by `Arc` instead of by value avoids a deep clone per instance —
    /// while still keeping this struct free of a lifetime the executor would have to thread
    /// through.
    nfa: std::sync::Arc<Nfa>,
    /// `AFTER MATCH SKIP` strategy, shared with the batch path.
    skip: SkipMode,
    /// All matches over the rows fed so far. `matched[..frozen_count]` are frozen (immutable under
    /// future appends); the rest is the provisional tail recomputed on every advance.
    matched: Vec<SeqMatch>,
    /// Number of leading entries of `matched` that are frozen.
    frozen_count: usize,
    /// Buffer position where the next rescan begins: the skip-resume point after the last frozen
    /// match (0 while nothing is frozen). The suffix `[next_pos, n_rows)` is the only mutable region.
    next_pos: usize,
    /// `seq_index[pos]` is the seq of the row fed at buffer position `pos`. Its length is the number
    /// of rows fed so far (the batch `n_rows`).
    seq_index: Vec<Seq>,
    /// Whether the last rescan stopped on a spent budget, leaving `matched`'s provisional tail a
    /// (possibly empty) leftmost-PREFIX of the true match list rather than the full list. While
    /// set, absence of a match from `provisional()` is NOT evidence of absence: the executor must
    /// re-derive (fresh budget) before any decision that treats missing matches as decided — the
    /// WITHIN-deadline prune in particular would otherwise delete rows carrying a match the
    /// truncated scan never reached.
    incomplete: bool,
    /// Absolute buffer position where a budget-truncated match scan will resume. Unlike
    /// `matchless_upto`, this may follow successful matches: the corresponding leftmost-prefix of
    /// matches remains in `matched`, and the next refresh appends matches found from this cursor.
    /// Reset whenever rows are appended or invalidated, because those changes can alter previously
    /// provisional matches and their skip-resume positions.
    scan_cursor: usize,
    /// Positions `[next_pos, dead_upto)` proven dead at the boundary by the freeze walks of this
    /// and earlier visits (`next_pos <= matchless_upto <= dead_upto` always). Deadness is monotone
    /// under appends — a walk reads only rows at or before its position (there is no forward
    /// navigation: `NEXT` inside `DEFINE` is rejected at bind and decode time), so a position no
    /// path can carry to the boundary stays that way as rows arrive — which lets a freeze that ran
    /// out of budget resume where it stopped instead of restarting at `next_pos`. Without this,
    /// freezing a match of `L` rows costs Θ(L²) steps in ONE visit (each of its `L` positions
    /// walks up to `L` rows), and once that exceeds the per-visit budget the region never freezes:
    /// the permanent, non-self-healing shape a long chain pattern (`a{600}`) otherwise degrades
    /// into. Reset to `next_pos` whenever the rows a verdict was computed over can change
    /// (truncation, eviction rebase).
    dead_upto: usize,
    /// Starts `[next_pos, matchless_upto)` proven MATCHLESS FOREVER by the finder: their walks found
    /// no accept and never reached the boundary, so they died entirely on immutable rows (see
    /// [`MatchScan::matchless_upto`]). The next rescan begins past them. This is the finder's
    /// counterpart of `dead_upto` — and feeds it, since such a start is dead too: without it, a
    /// long run broken by one non-matching row costs Θ(r²) on EVERY rescan (each start walks to
    /// the break and dies), past the budget from `r ≈ 840`, and a partition in that state never
    /// completes a rescan again. Same resets as `dead_upto`.
    matchless_upto: usize,
    /// Whether the last freeze loop stopped on a spent budget with its region unfinished. The
    /// executor refreshes on this like on `incomplete`, so a truncated freeze resumes on the next
    /// watermark visit rather than only on the next arrival.
    freeze_truncated: bool,
}

/// Adapts a [`CandidateMatcher`] so that a scan over the suffix `[offset, ..)` sees suffix-relative
/// positions `0, 1, ...` while the underlying matcher still resolves absolute buffer positions. This
/// lets us drive [`Nfa::find_matches_dynamic`] over just the mutable suffix using the *same* matcher
/// the batch path uses, without adding a start-offset parameter to the NFA.
struct OffsetMatcher<'a, M> {
    inner: &'a M,
    offset: usize,
}

impl<M: CandidateMatcher + Sync> CandidateMatcher for OffsetMatcher<'_, M> {
    fn matches(
        &self,
        var: &str,
        pos: usize,
        labels: &[String],
    ) -> impl std::future::Future<Output = StreamExecutorResult<bool>> + Send {
        self.inner.matches(var, pos + self.offset, labels)
    }
}

impl IncrementalMatcher {
    pub fn new(nfa: std::sync::Arc<Nfa>, skip: SkipMode) -> Self {
        Self {
            nfa,
            skip,
            matched: Vec::new(),
            frozen_count: 0,
            next_pos: 0,
            seq_index: Vec::new(),
            incomplete: false,
            scan_cursor: 0,
            dead_upto: 0,
            matchless_upto: 0,
            freeze_truncated: false,
        }
    }

    /// Reset to the freshly-constructed state, keeping the (shared) automaton and skip mode and
    /// reusing the collections' allocations. Equivalent to a new matcher: used when a consumed
    /// prefix swallows the whole buffer, where reconstructing would re-clone the skip mode for
    /// nothing.
    pub fn reset(&mut self) {
        self.matched.clear();
        self.frozen_count = 0;
        self.next_pos = 0;
        self.seq_index.clear();
        self.incomplete = false;
        self.scan_cursor = 0;
        self.dead_upto = 0;
        self.matchless_upto = 0;
        self.freeze_truncated = false;
    }

    /// Whether the last rescan was truncated by a spent budget — see the field doc. While true,
    /// `provisional()` is a leftmost-prefix under-approximation.
    pub fn is_incomplete(&self) -> bool {
        self.incomplete
    }

    /// Whether a visit's rescan should be re-run with a fresh budget before deciding anything:
    /// the provisional tail is incomplete ([`IncrementalMatcher::is_incomplete`]), or the freeze
    /// stopped on a spent budget and has proven-dead progress to resume from.
    pub fn needs_refresh(&self) -> bool {
        self.incomplete || self.freeze_truncated
    }

    /// Re-derive the provisional tail with a fresh budget, without feeding rows: the executor's
    /// recovery valve for [`IncrementalMatcher::is_incomplete`]. Delegates to the same rescan
    /// `advance` performs.
    pub async fn refresh(
        &mut self,
        matcher: &(impl CandidateMatcher + Sync),
        budget: &mut ScanBudget,
        memoize: bool,
    ) -> StreamExecutorResult<()> {
        self.rescan(matcher, budget, memoize).await
    }

    /// The skip-resume position after the last frozen match — the left edge of the still-revisable
    /// region, in fed-position (= the executor's buffer-index) terms. The executor's emission gate
    /// re-checks gap liveness from here: a position in `[resume_pos, match start)` that is still
    /// alive at the boundary can produce an earlier, leftmost-preferred match and must hold the
    /// emission.
    pub fn resume_pos(&self) -> usize {
        self.next_pos
    }

    /// End of the prefix of fed positions proven dead at the boundary — every position below it
    /// can never again start a match (see the `dead_upto` field). The executor's own liveness
    /// walks (the dead-prefix prune, the emission gate's gap check) skip these positions instead
    /// of re-deriving a verdict the freeze already paid for.
    pub fn dead_prefix_end(&self) -> usize {
        self.dead_upto
    }

    /// Feed rows appended in `ORDER BY` order. `new_row_seqs` are the seqs of the newly appended rows;
    /// their buffer positions are the next positions after the rows fed so far. An empty call is a
    /// no-op.
    ///
    /// Rescans only the mutable suffix `[next_pos, n_rows)` via the budgeted, memoized pull scan
    /// ([`Nfa::next_match`] through an [`OffsetMatcher`]), replacing the provisional tail of
    /// `matched`. It then freezes the leading
    /// run of suffix matches whose entire scan region `[cursor, skip-resume)` is dead at the boundary
    /// per [`Nfa::reaches_boundary_alive`], advancing `next_pos` to the last frozen match's resume
    /// position. Checking the *whole region* — not just the match's start — matters: a gap position
    /// before the match can be alive (a longer, higher-preference alternative still in flight) and a
    /// future row could then produce a match there that consumes past this one, so nothing behind
    /// that gap may freeze. Liveness is checked with the raw `matcher` at absolute positions (only
    /// the finder needs the offset adapter, because it always scans from 0).
    pub async fn advance(
        &mut self,
        new_row_seqs: &[Seq],
        matcher: &(impl CandidateMatcher + Sync),
        budget: &mut ScanBudget,
        memoize: bool,
    ) -> StreamExecutorResult<()> {
        if new_row_seqs.is_empty() {
            return Ok(());
        }
        // `advance` only ever appends genuinely new rows. Re-feeding an already-fed seq (a late row
        // landing before fed rows, or an over-feed narrowing back) must instead go through
        // `truncate_from_seq` / `refresh_matcher`, which roll `seq_index` back first; appending a
        // duplicate here would corrupt the seq→position mapping.
        debug_assert!(
            new_row_seqs.iter().all(|s| !self.seq_index.contains(s)),
            "re-feeds must go through refresh_matcher/truncate, not advance"
        );
        // Appended rows can extend a greedy match or make a formerly boundary-blocked start match,
        // so a saved scan prefix is no longer reusable. Matchless-forever starts remain valid.
        self.incomplete = false;
        self.scan_cursor = self.matchless_upto;
        self.freeze_truncated = false;
        self.seq_index.extend_from_slice(new_row_seqs);
        self.rescan(matcher, budget, memoize).await
    }

    /// Re-derive the provisional tail over the already-fed mutable suffix `[next_pos, n_rows)`
    /// without feeding new rows — the rescan half of [`IncrementalMatcher::advance`], exposed for
    /// the one caller shape where a rescan must happen with nothing to feed: an **over-feed
    /// rollback**. When rows beyond the caller's current window were fed (e.g. an emit-on-update
    /// whole-buffer feed followed by a watermark visit over just the safe prefix),
    /// `IncrementalMatcher::truncate_from_seq` at the first over-fed row rolls the tail back but
    /// also drops every provisional match over the *retained* fed suffix; those rows are still fed,
    /// so there is nothing to `advance` (re-feeding them would double-enter `seq_index`) and the
    /// dropped matches must be re-derived in place. After this call `provisional()` equals a
    /// from-scratch batch scan of the fed rows.
    async fn rescan(
        &mut self,
        matcher: &(impl CandidateMatcher + Sync),
        budget: &mut ScanBudget,
        memoize: bool,
    ) -> StreamExecutorResult<()> {
        let n_rows = self.seq_index.len();

        // Rescan the mutable suffix only. The offset matcher maps suffix-relative positions produced
        // by the scan back onto absolute buffer positions the real matcher understands.
        let offset = self.next_pos;
        let offset_matcher = OffsetMatcher {
            inner: matcher,
            offset,
        };
        // The finder is the exponential walk the scan budget and the failure memo exist for —
        // drive the pull-based scan directly so both actually meter it. On a spent budget the
        // loop stops early and the tail is INCOMPLETE: the freeze loop below holds (it treats
        // `budget.hit` as "alive"), the executor's emission gate holds, and the next visit
        // rescans with a fresh budget — degraded latency, never a wrong or lost match.
        debug_assert!(
            self.next_pos <= self.matchless_upto && self.matchless_upto <= self.dead_upto,
            "next_pos {} <= matchless_upto {} <= dead_upto {}",
            self.next_pos,
            self.matchless_upto,
            self.dead_upto
        );
        let continuing_scan = self.incomplete;
        let resume_freeze = !continuing_scan && self.freeze_truncated;
        let mut tail_abs: Vec<LabeledMatch> = if continuing_scan || resume_freeze {
            // Preserve the already-scanned leftmost prefix. Matches are seq-anchored in storage, so
            // recover their current buffer positions before appending the resumed scan's suffix.
            let mut search_from = self.next_pos;
            self.matched[self.frozen_count..]
                .iter()
                .map(|m| {
                    let start = search_from
                        + self.seq_index[search_from..]
                            .iter()
                            .position(|&seq| seq == m.start_seq)
                            .expect("provisional match start seq must still be fed");
                    search_from = start + 1;
                    LabeledMatch {
                        start,
                        end: start + m.labels.len(),
                        labels: m.labels.clone(),
                    }
                })
                .collect()
        } else {
            Vec::new()
        };
        let mut scan_truncated = false;
        if !resume_freeze {
            // A truncated refresh resumes after the successful matches already retained above.
            // A fresh scan starts after the starts proven matchless forever.
            let scan_start = if continuing_scan {
                self.scan_cursor
            } else {
                self.matchless_upto
            };
            debug_assert!(self.next_pos <= scan_start && scan_start <= n_rows);
            let mut scan = MatchScan::starting_at(scan_start - offset);
            while let Some(m) = self
                .nfa
                .next_match(
                    &mut scan,
                    n_rows - offset,
                    &offset_matcher,
                    &self.skip,
                    budget,
                    memoize,
                )
                .await?
            {
                tail_abs.push(LabeledMatch {
                    start: m.start + offset,
                    end: m.end + offset,
                    labels: m.labels,
                });
            }
            self.scan_cursor = offset + scan.next_start();
            // Extend the matchless prefix only when this pull began exactly at its end. Once a
            // successful match creates a gap, the later cursor is resumable but not matchless.
            if scan_start == self.matchless_upto {
                self.matchless_upto = offset + scan.matchless_upto();
            }
            self.dead_upto = self.dead_upto.max(self.matchless_upto);
            // Capture this before freeze walks spend more budget. A completed scan followed by a
            // truncated freeze keeps its complete tail and resumes only the freeze next visit.
            scan_truncated = budget.hit;
        }

        // Freeze the leading run of matches whose scan region `[cursor, resume)` is entirely dead at
        // the boundary. A dead position's scan outcome is final — no path from it can consume past
        // `n_rows - 1`, so appended rows can never be reached from it and the greedy attempt there
        // returns the same result over any future buffer. Matches freeze strictly in order (there is
        // a single cursor), so stop at the first region containing a live position. A match ending
        // at the boundary is covered without a special case: its own accepting path reaches the
        // boundary, so its start is alive and the region check fails.
        let mut newly_frozen = 0usize;
        let mut cursor = self.next_pos;
        let mut freeze_truncated = false;
        debug_assert!(
            self.dead_upto >= cursor,
            "dead_upto {} < next_pos {cursor}",
            self.dead_upto
        );
        'freeze: for m in &tail_abs {
            // The skip-degradation diagnostic is dropped here for the same reason
            // `Nfa::find_matches_dynamic` drops it: this is freeze-cursor bookkeeping, not an
            // emission site — the executor recomputes the resume position for the matches it
            // actually emits and reports from there.
            let (resume, _) = self.skip.next_pos(m.start, m.end, &m.labels);
            // `dead_upto >= cursor` throughout: it starts at or past `next_pos`, and a region only
            // freezes once every position in it is proven dead, so `cursor = resume` keeps it. An
            // EMPTY range here is the resumed freeze paying off — the whole region was proven dead
            // by earlier visits, and the match freezes without a walk.
            for p in self.dead_upto..resume {
                let alive = self
                    .nfa
                    .reaches_boundary_alive(p, n_rows, matcher, budget, memoize)
                    .await?;
                // A spent budget is NOT a deadness verdict: the liveness walk returns `false`
                // when it stops early, and freezing on that fabricated answer advances the cursor
                // past positions that are genuinely alive — their matches are then lost forever
                // (the cursor never rewinds) while the prune pass, which guards correctly,
                // retains their rows forever. Exhaustion holds the freeze; the next rescan resumes
                // it from `dead_upto` with a fresh budget, so each one makes progress.
                if budget.hit {
                    freeze_truncated = true;
                    break 'freeze;
                }
                if alive {
                    break 'freeze;
                }
                // Proven dead, and deadness is monotone under appends: never walk `p` again.
                self.dead_upto = p + 1;
            }
            cursor = resume;
            newly_frozen += 1;
        }
        self.next_pos = cursor;
        // Freezing moves the resume point past matches, including past starts the finder never
        // proved anything about; the matchless prefix begins at the resume point by definition.
        self.matchless_upto = self.matchless_upto.max(cursor);
        self.scan_cursor = self.scan_cursor.max(cursor);
        self.freeze_truncated = freeze_truncated;

        // Drop the previous provisional tail and reattach the freshly scanned suffix, moving each
        // match's labels (this runs per arriving row — a clone here would copy every provisional
        // label vector once per row for nothing).
        self.matched.truncate(self.frozen_count);
        for m in tail_abs {
            let sm = SeqMatch {
                start_seq: self.seq_index[m.start],
                end_seq: Seq(self.seq_index[m.end - 1].0 + 1),
                labels: m.labels,
            };
            self.matched.push(sm);
        }
        self.frozen_count += newly_frozen;
        self.incomplete = scan_truncated;

        Ok(())
    }

    /// Invalidate everything at and after the fed row identified by `seq`, so the caller can re-feed
    /// a corrected sorted suffix (an out-of-order row landing before rows already fed). `seq` is the
    /// stable identity of the first buffered row whose sorted position changes; the executor computes
    /// it (the first buffered order key `>=` the late row's), and here we only map it back to a fed
    /// position via `seq_index`.
    ///
    /// A seq never fed (e.g. an order key beyond everything buffered) is a no-op; truncating at the
    /// first fed row (position 0) is a full reset. After this call `provisional()` never returns a
    /// match overlapping the truncated region.
    ///
    /// Why this needs the `matcher` (and so mirrors [`IncrementalMatcher::advance`]'s freezing gate
    /// rather than a purely positional rule): a match froze against a *later* boundary, and freezing
    /// only requires every region position to be dead at *that* boundary — a position may still hold
    /// a path that stays alive *through* the rows now being truncated (a longer, higher-preference
    /// alternative that only died past the truncation point). Such a frozen match is not final once
    /// those rows change, even when its own span ends before the truncation point. So we recompute
    /// the surviving frozen prefix with the exact gate `advance` uses — region-wide
    /// [`Nfa::reaches_boundary_alive`] — but against the truncation boundary. Only a region entirely
    /// dead at that boundary is independent of the truncated/re-fed rows and may be kept; the rest
    /// (and every provisional match) is dropped and re-derived by the following `advance`, which
    /// rescans from the rewound `next_pos`.
    /// Test-only on this executor: the emit-on-update mode that drives it is not part of this
    /// operator; the differential oracle exercises it so the contract stays proven.
    #[cfg(test)]
    pub async fn truncate_from_seq(
        &mut self,
        seq: Seq,
        matcher: &(impl CandidateMatcher + Sync),
        budget: &mut ScanBudget,
        memoize: bool,
    ) -> StreamExecutorResult<()> {
        // Seqs are stable row identities, not sort keys, so `seq_index` is not ordered by value; find
        // the exact entry. A missing seq means nothing buffered at/after it changed — leave state as
        // is.
        let Some(trunc_pos) = self.seq_index.iter().position(|&s| s == seq) else {
            return Ok(());
        };

        let mut kept = 0usize;
        let mut cursor = 0usize;
        'keep: for m in &self.matched[..self.frozen_count] {
            // Frozen matches are stored in scan order, so each start is at or after the cursor; search
            // forward from there to recover its buffer position (seqs are not positions).
            let start_pos = cursor
                + self.seq_index[cursor..]
                    .iter()
                    .position(|&s| s == m.start_seq)
                    .expect("frozen match start seq must still be fed at truncation");
            let end_pos = start_pos + m.labels.len();
            // A match reaching into (or across) the truncated region cannot survive: the re-fed rows
            // may change its greedy extent or its skip-resume point.
            if end_pos > trunc_pos {
                break;
            }
            // `resume <= end_pos <= trunc_pos`, so every checked position is in the retained region.
            // Diagnostic dropped: truncation bookkeeping, not an emission site (see
            // `Nfa::find_matches_dynamic` for the policy).
            let (resume, _) = self.skip.next_pos(start_pos, end_pos, &m.labels);
            for p in cursor..resume {
                let alive = self
                    .nfa
                    .reaches_boundary_alive(p, trunc_pos, matcher, budget, memoize)
                    .await?;
                // As in `rescan`'s freeze loop: a spent budget is not a deadness verdict — keep
                // fewer matches frozen rather than freeze on a fabricated "dead".
                if budget.hit || alive {
                    break 'keep;
                }
            }
            cursor = resume;
            kept += 1;
        }

        self.next_pos = cursor;
        // The re-fed rows may differ, and a dead or matchless verdict for any position could have
        // been decided by a path that died on one of them; forget every verdict beyond the kept
        // frozen prefix.
        self.dead_upto = cursor;
        self.matchless_upto = cursor;
        self.scan_cursor = cursor;
        self.incomplete = false;
        self.freeze_truncated = false;
        self.frozen_count = kept;
        self.matched.truncate(kept);
        self.seq_index.truncate(trunc_pos);
        Ok(())
    }

    /// Finalize the evicted prefix in place, or report that the matcher must be rebuilt. The caller
    /// is evicting `[.., boundary)` rows from its buffer; `boundary` is the first row seq that is
    /// *not* evicted (an exclusive upper bound). On success the matches lying wholly within the
    /// evicted prefix leave the diffable set, the surviving matcher is rebased onto the surviving
    /// buffer, and [`IncrementalMatcher::provisional`] then returns only still-revisable matches.
    /// The finalized matches are not returned (see [`Finalized::Rebased`]); tests derive them by
    /// diffing `provisional()` before/after.
    ///
    /// This owns the finalize-vs-rebuild decision the executor used to make itself. It returns
    /// [`Finalized::MustRebuild`] — leaving the matcher untouched — in the shapes where an in-place
    /// rebase is unsound, so the executor never needs to pre-check (and no debug assertion can trip
    /// nor `next_pos -= final_pos` underflow):
    /// - **Boundary never fed** (only unfed/unsafe rows survive) or **past the frozen prefix**
    ///   (`final_pos > next_pos`): finalization would reach into the open, still-revisable region.
    /// - **A frozen match straddles the boundary strictly inside the frozen prefix** — it starts
    ///   before `final_pos` and ends after, while `final_pos < next_pos`. This arises only under the
    ///   overlapping skip modes (`TO NEXT ROW`/`TO FIRST`/`TO LAST`), whose resume precedes the match
    ///   end so a frozen span can outrun its own resume; see the consume/keep walk below for why it
    ///   is declined. It is not reachable through the executor's eviction (which always lands the
    ///   boundary at `final_pos == next_pos`, see below), only through a direct API call.
    ///
    /// **Why the overlapping skip modes now rebase** (they previously always rebuilt): the executor
    /// evicts from the first row still live at the safe boundary. Every position in `[0, next_pos)`
    /// was liveness-checked dead when its region froze, and deadness is monotone in the boundary, so
    /// at the (same-or-later) eviction boundary `[0, next_pos)` is still dead and the first live row
    /// is `>= next_pos`. The check above bounds `final_pos <= next_pos`, so through the executor
    /// `final_pos == next_pos` exactly. At that boundary every frozen match starts before `next_pos`
    /// and is therefore consumed — none is retained — so `next_pos` rebases to `0` and the entire
    /// surviving suffix is re-derived from scratch as the provisional tail. `provisional()` then
    /// trivially equals a fresh scan over the survivors, regardless of skip mode, and no rebased scan
    /// cursor can skip a start a fresh matcher would find. `PAST LAST ROW` additionally tiles
    /// `[0, next_pos)` with non-overlapping spans (`resume == end`), so *any* boundary within the
    /// frozen prefix retains a suffix of frozen matches soundly.
    ///
    /// On [`Finalized::Rebased`] the evicted rows physically leave the front of the logical buffer:
    /// `seq_index` drains its prefix and `next_pos`/`frozen_count` shift down. Only rows at positions
    /// `>= final_pos` survive, and rebasing is a uniform downward shift of those rows; paths *forward*
    /// from a surviving position consume only surviving (unchanged) rows, so the freezing-soundness
    /// argument (a frozen region is dead at its boundary) is preserved. Match spans are anchored by
    /// seq, so retained and returned [`SeqMatch`]es keep their identities without adjustment.
    pub fn finalize_evicted_prefix(&mut self, boundary: Seq) -> Finalized {
        // Map the boundary seq to a fed position. Absent means the boundary row was never fed — the
        // whole fed prefix is being evicted and only unfed/unsafe rows survive — which cannot be
        // rebased against.
        let Some(final_pos) = self.seq_index.iter().position(|&s| s == boundary) else {
            return Finalized::MustRebuild;
        };
        // The boundary must lie within the frozen prefix; past it would reach the open region.
        if final_pos > self.next_pos {
            return Finalized::MustRebuild;
        }

        // Finalized matches are the leading run of frozen matches whose *start* is being evicted
        // (`start_pos < final_pos`): their first row leaves the buffer, so they are consumed — final,
        // already emitted — and drop from the diffable set. Only frozen matches can start within
        // `[0, next_pos)` (a provisional match starts at `>= next_pos`) and they are stored in scan
        // order, so this is a single leading run; stop at the first match that starts at/after the
        // boundary (it survives). Recover each start position from `seq_index` (seqs are identities,
        // not positions).
        let mut finalized = 0usize;
        let mut cursor = 0usize;
        for m in &self.matched[..self.frozen_count] {
            let start_pos = cursor
                + self.seq_index[cursor..]
                    .iter()
                    .position(|&s| s == m.start_seq)
                    .expect("finalized match start seq must still be fed");
            if start_pos >= final_pos {
                // Starts at/after the boundary: wholly retained (and so is everything after it).
                break;
            }
            let end_pos = start_pos + m.labels.len();
            // A consumed match whose span straddles the boundary (`end_pos > final_pos`, possible
            // only under the overlapping modes) orphans its surviving rows `[final_pos, end_pos)`.
            // Dropping it is sound only when the boundary sits exactly at the scan cursor
            // (`final_pos == next_pos`): then no frozen match is retained, so the whole surviving
            // suffix is re-scanned from scratch and `provisional()` still equals a fresh scan. That
            // is the only boundary the executor produces; decline a mid-frozen straddle (a direct-API
            // shape) rather than corrupt the rebase by skipping a start a fresh scan would revisit.
            if end_pos > final_pos && final_pos != self.next_pos {
                return Finalized::MustRebuild;
            }
            finalized += 1;
            // Later matches start strictly after this one (`resume > start`), so search forward.
            cursor = start_pos + 1;
        }

        // Drop the finalized matches and rebase the buffer.
        self.matched.drain(..finalized);
        self.frozen_count -= finalized;
        self.next_pos -= final_pos;
        // Through the executor `final_pos == next_pos`, so this is `0`: every frozen match was
        // emitted and the surviving suffix is re-derived from scratch. Verdicts beyond the frozen
        // prefix are forgotten rather than shifted: a `PREV` slot at the new buffer start reads
        // past it where it read an evicted row before, so a verdict computed over the old buffer
        // need not hold — conservative, and resumable.
        self.dead_upto = self.next_pos;
        self.matchless_upto = self.next_pos;
        self.scan_cursor = self.next_pos;
        self.incomplete = false;
        self.freeze_truncated = false;
        self.seq_index.drain(..final_pos);
        Finalized::Rebased
    }

    /// Current provisional matches over everything fed so far, as if input ended now.
    pub fn provisional(&self) -> &[SeqMatch] {
        &self.matched
    }

    /// Seqs of the rows fed so far, in feed (buffer-position) order — i.e. `seq_index`. The executor
    /// reads this to align the matcher with the freshly-scanned state-table buffer each visit and to
    /// detect an out-of-order safe row (one whose sorted position precedes an already-fed row).
    /// Test-only on this executor: the emit-on-update mode that drives it is not part of this
    /// operator; the differential oracle exercises it so the contract stays proven.
    #[cfg(test)]
    pub fn fed_seqs(&self) -> &[Seq] {
        &self.seq_index
    }

    /// Number of leading buffer positions that are frozen (immutable under future appends) — i.e.
    /// `next_pos`, the scan-resume point. The production caller was absorbed into
    /// [`IncrementalMatcher::finalize_evicted_prefix`]'s internal boundary checks, so this is
    /// test-only observability (like [`IncrementalMatcher::frozen`]). Distinct from `frozen_count`,
    /// which counts frozen *matches*, not positions.
    #[cfg(test)]
    fn frozen_prefix_len(&self) -> usize {
        self.next_pos
    }

    /// Number of leading `provisional()` entries that are frozen (final under future appends).
    /// Test-only observability for asserting freezing behavior directly.
    #[cfg(test)]
    fn frozen(&self) -> usize {
        self.frozen_count
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};
    use risingwave_common::array::Op;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::ScalarImpl;

    use super::{
        Finalized, IncrementalMatcher, Seq, SeqMatch, diff_provisional, plan_provisional_rows,
    };
    use crate::executor::error::StreamExecutorResult;
    use crate::executor::match_recognize::nfa::{
        CandidateMatcher, Nfa, Pattern, Quantifier, ScanBudget, SetMatcher, SkipMode,
    };

    /// A `SeqMatch` with the given extent and labels (identity is `start`).
    fn dm(start: i64, end: i64, ls: &[&str]) -> SeqMatch {
        SeqMatch {
            start_seq: Seq(start),
            end_seq: Seq(end),
            labels: labels(ls),
        }
    }

    /// Wrap raw seq literals for the matcher feed API (`advance` takes `&[Seq]`); tests use bare ints
    /// (seqs equal final sorted positions in these tests, so the ints read naturally).
    fn ss(xs: &[i64]) -> Vec<Seq> {
        xs.iter().map(|&x| Seq(x)).collect()
    }

    /// Run an in-place finalization and return the matches it removed, derived by diffing
    /// `provisional()` before/after — sound because finalization removes exactly a leading run of
    /// the stored matches, so the removed ones are the vanished prefix (scan order, seqs intact).
    /// Fails the test on the `MustRebuild` outcome (the caller-drops-and-rebuilds path is asserted
    /// separately).
    fn finalize_rebased(inc: &mut IncrementalMatcher, boundary: Seq) -> Vec<SeqMatch> {
        let before = inc.provisional().to_vec();
        match inc.finalize_evicted_prefix(boundary) {
            Finalized::Rebased => before[..before.len() - inc.provisional().len()].to_vec(),
            Finalized::MustRebuild => panic!("expected Finalized::Rebased, got MustRebuild"),
        }
    }

    /// A one-column output row carrying `v`, so distinct rows are easy to assert on.
    fn orow(v: i64) -> OwnedRow {
        OwnedRow::new(vec![Some(ScalarImpl::Int64(v))])
    }

    #[test]
    fn diff_emits_delete_insert_for_revision() {
        // start 5 revised: end 7 (row A) -> end 9 (row B). Delete(A) then Insert(B).
        let old = vec![(dm(5, 7, &["a", "b"]), orow(100))];
        let new = vec![(dm(5, 9, &["a", "b", "b"]), orow(200))];
        assert_eq!(
            diff_provisional(&old, &new),
            vec![(Op::Delete, orow(100)), (Op::Insert, orow(200))]
        );
    }

    #[test]
    fn diff_emits_delete_for_vanished_match() {
        let old = vec![(dm(5, 7, &["a", "b"]), orow(100))];
        let new: Vec<(SeqMatch, OwnedRow)> = vec![];
        assert_eq!(diff_provisional(&old, &new), vec![(Op::Delete, orow(100))]);
    }

    #[test]
    fn diff_emits_insert_for_brand_new_match() {
        let old: Vec<(SeqMatch, OwnedRow)> = vec![];
        let new = vec![(dm(5, 7, &["a", "b"]), orow(200))];
        assert_eq!(diff_provisional(&old, &new), vec![(Op::Insert, orow(200))]);
    }

    #[test]
    fn diff_unchanged_match_emits_nothing() {
        let old = vec![(dm(5, 7, &["a", "b"]), orow(100))];
        let new = vec![(dm(5, 7, &["a", "b"]), orow(100))];
        assert_eq!(diff_provisional(&old, &new), vec![]);
    }

    /// Recovery-rebuild seam (Task 8). On recovery the executor reseeds its emit-on-update diff
    /// base by feeding the recovered buffer to a FRESH matcher (`rebuild_last_emitted` →
    /// `compute_partition_emitted`) and emitting nothing. That silent reseed is sound only if the
    /// from-scratch recomputation reproduces the pre-crash matcher's provisional set exactly — the
    /// set downstream already holds. This test exercises that property at the matcher+diff seam:
    /// the pre-crash side reaches its state through multi-visit incremental advances, a
    /// finalize-eviction (the finalized match left the base without a retract, its rows left the
    /// buffer), and a further advance; the recovery side is one whole-buffer feed of the surviving
    /// rows into a fresh matcher — exactly what the rebuild does. Both must agree on full
    /// `(start, end, labels)` triples, and a diff between output rows synthesized deterministically
    /// from each match (`build_match_row` is deterministic in the same way: pure in buffer content)
    /// must be empty — the reseed leaves nothing to re-emit at the next barrier.
    #[tokio::test]
    async fn recovery_rebuild_reproduces_pre_crash_provisional_set() {
        let pat = Pattern::Concat(vec![
            Pattern::Var("a".into()),
            quant(Pattern::Var("b".into()), Quantifier::Plus, false),
        ]);
        let nfa = Nfa::compile(&pat);
        let skip = SkipMode::PastLastRow;
        // Deterministic stand-in for `build_match_row`: a pure function of the match content.
        let out_row = |m: &SeqMatch| orow(m.start_seq.0 * 1000 + m.end_seq.0);

        // Pre-crash: rows 0:a 1:b 2:a 3:b arrive across two visits; the first `a b` = (0,2)
        // freezes once the `a` at position 2 breaks the greedy `b+`.
        let pre = from_str("abab");
        let m_pre = SetMatcher::new(pre.clone());
        let mut pre_inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
        pre_inc
            .advance(&ss(&[0, 1]), &m_pre, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();
        pre_inc
            .advance(&ss(&[2, 3]), &m_pre, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();
        // A watermark finalizes and evicts the frozen (0,2): its rows leave the buffer and it
        // leaves the diff base without a retraction (a permanent result downstream).
        let removed = finalize_rebased(&mut pre_inc, Seq(2));
        assert_eq!(seq_triples(&removed), vec![(0, 2, labels(&["a", "b"]))]);
        // One more row arrives; the last pre-crash barrier emitted this provisional set, so the
        // MV's provisional portion == this base. Surviving buffer: seqs 2,3,4 = {a},{b},{b}.
        let tail = from_str("abb");
        let m_tail = SetMatcher::new(tail.clone());
        pre_inc
            .advance(&ss(&[4]), &m_tail, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();
        let pre_base: Vec<(SeqMatch, OwnedRow)> = pre_inc
            .provisional()
            .iter()
            .map(|m| (m.clone(), out_row(m)))
            .collect();
        assert!(!pre_base.is_empty());

        // Crash. Recovery: the restored state table holds only the surviving rows; the rebuild
        // feeds them, whole-buffer, into a FRESH matcher.
        let mut rec_inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
        rec_inc
            .advance(
                &ss(&[2, 3, 4]),
                &m_tail,
                &mut ScanBudget::unlimited(),
                false,
            )
            .await
            .unwrap();
        let rec_set: Vec<(SeqMatch, OwnedRow)> = rec_inc
            .provisional()
            .iter()
            .map(|m| (m.clone(), out_row(m)))
            .collect();

        // The from-scratch recomputation reproduces the pre-crash provisional set exactly...
        assert_eq!(provisional_triples(&rec_inc), provisional_triples(&pre_inc));
        // ...so seeding `last_emitted` with it and re-diffing (as the next barrier would over an
        // unchanged buffer) re-emits nothing — the silent rebuild is exact, not approximate.
        assert_eq!(diff_provisional(&pre_base, &rec_set), vec![]);
    }

    /// The row-independent classification the executor runs before building any output row. An
    /// unchanged provisional set must reuse every base row (all `Some`, zero rebuilds — the
    /// steady-state win); a same-`start_seq` match with a changed extent or changed labels, and a
    /// brand-new start, must each rebuild (`None`).
    #[test]
    fn plan_reuses_unchanged_and_rebuilds_changed() {
        let base = vec![
            (dm(5, 7, &["a", "b"]), orow(100)),
            (dm(10, 12, &["a", "b"]), orow(200)),
        ];

        // Identical provisional set (order need not match the base): every row reused, none rebuilt.
        let same = vec![dm(10, 12, &["a", "b"]), dm(5, 7, &["a", "b"])];
        let plan = plan_provisional_rows(&base, &same);
        assert_eq!(plan, vec![Some(1), Some(0)]);
        assert!(
            plan.iter().all(Option::is_some),
            "an unchanged provisional set must rebuild no output rows"
        );

        // Changed extent (same start), changed labels (same start+extent), and a brand-new start:
        // each must be rebuilt.
        let changed = vec![
            dm(5, 9, &["a", "b", "b"]), // start 5: extent 7 -> 9
            dm(10, 12, &["b", "b"]),    // start 10: labels [a,b] -> [b,b]
            dm(20, 22, &["a", "b"]),    // brand-new start
        ];
        assert_eq!(
            plan_provisional_rows(&base, &changed),
            vec![None, None, None]
        );
    }

    /// Same extent and labels, but the evaluated output row changed — still a revision (the diff
    /// compares the output row, not just the span/labels).
    #[test]
    fn diff_same_extent_different_row_is_revision() {
        let old = vec![(dm(5, 7, &["a", "b"]), orow(100))];
        let new = vec![(dm(5, 7, &["a", "b"]), orow(101))];
        assert_eq!(
            diff_provisional(&old, &new),
            vec![(Op::Delete, orow(100)), (Op::Insert, orow(101))]
        );
    }

    /// Regression: the inputs are NOT start-seq sorted. `provisional()` yields matches in the
    /// matcher's scan (buffer-position) order, and seqs are minted at *arrival* — so a late row that
    /// sorts earlier carries a higher seq at an earlier position, and position order diverges from
    /// `start_seq` order. Here `old` holds (start 30, X) at the earlier position before (start 10, K);
    /// `new` still holds the unchanged (start 10, K). A merge that trusted the input order would
    /// emit Insert(K), Delete(X), Delete(K) — net-removing K downstream even though it is still
    /// live. The internal sort must yield exactly Delete(X), keeping K present.
    #[test]
    fn diff_position_ordered_inputs_from_out_of_order_arrival() {
        let old = vec![
            (dm(30, 32, &["a", "b"]), orow(300)), // X: earlier position, later-minted seq
            (dm(10, 12, &["a", "b"]), orow(100)), // K: later position, earlier-minted seq
        ];
        let new = vec![(dm(10, 12, &["a", "b"]), orow(100))];
        assert_eq!(diff_provisional(&old, &new), vec![(Op::Delete, orow(300))]);

        // Same provenance with both sides unsorted and K revised: the pairing must still be by
        // identity, so X vanishes and K is retract-updated — never net-deleted.
        let old = vec![
            (dm(30, 32, &["a", "b"]), orow(300)),
            (dm(10, 12, &["a", "b"]), orow(100)),
        ];
        let new = vec![
            (dm(40, 42, &["a", "b"]), orow(400)),
            (dm(10, 13, &["a", "b", "b"]), orow(101)),
        ];
        assert_eq!(
            diff_provisional(&old, &new),
            vec![
                (Op::Delete, orow(100)),
                (Op::Insert, orow(101)),
                (Op::Delete, orow(300)),
                (Op::Insert, orow(400)),
            ]
        );
    }

    /// Mixed sequence in one pass: start 1 unchanged, start 5 revised, start 9 vanished, start 11
    /// brand new. Ops must come out start-seq ascending, Delete-before-Insert per revised identity.
    #[test]
    fn diff_mixed_sequence_orders_by_start_seq() {
        let old = vec![
            (dm(1, 3, &["a", "b"]), orow(10)),
            (dm(5, 7, &["a", "b"]), orow(50)),
            (dm(9, 10, &["a"]), orow(90)),
        ];
        let new = vec![
            (dm(1, 3, &["a", "b"]), orow(10)),
            (dm(5, 9, &["a", "b", "b"]), orow(59)),
            (dm(11, 12, &["a"]), orow(110)),
        ];
        assert_eq!(
            diff_provisional(&old, &new),
            vec![
                (Op::Delete, orow(50)),
                (Op::Insert, orow(59)),
                (Op::Delete, orow(90)),
                (Op::Insert, orow(110)),
            ]
        );
    }

    /// Oracle: feeding rows incrementally (in any split) must equal one batch
    /// `find_matches_dynamic` over the same rows — compared as full `(start, end, labels)` triples
    /// (like [`assert_equiv_with`]), so a *steal* (a row rebinding to a different variable under an
    /// unchanged span) is caught, not just span changes.
    async fn assert_equiv(
        nfa: &Nfa,
        skip: SkipMode,
        rows: &[BTreeSet<String>],
        split_at: &[usize],
    ) {
        let matcher = SetMatcher::new(rows.to_vec());
        let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
        let mut fed = 0usize;
        for &cut in split_at.iter().chain(std::iter::once(&rows.len())) {
            let seqs: Vec<Seq> = (fed..cut).map(|i| Seq(i as i64)).collect();
            inc.advance(&seqs, &matcher, &mut ScanBudget::unlimited(), false)
                .await
                .unwrap();
            fed = cut;
        }
        assert_eq!(
            provisional_triples(&inc),
            batch_triples(nfa, &skip, rows).await
        );
    }

    fn sets(labels: &[&str]) -> BTreeSet<String> {
        labels.iter().map(|s| s.to_string()).collect()
    }

    /// One row per non-whitespace char, each satisfying the single variable named by that char.
    fn from_str(s: &str) -> Vec<BTreeSet<String>> {
        s.chars()
            .filter(|c| !c.is_whitespace())
            .map(|c| BTreeSet::from([c.to_string()]))
            .collect()
    }

    /// `n` rows that each satisfy both `a` and `b`, so quantifier preference (not the predicate)
    /// decides the split — mirrors `nfa`'s own `ab_rows` helper.
    fn ab_rows(n: usize) -> Vec<BTreeSet<String>> {
        vec![BTreeSet::from(["a".to_owned(), "b".to_owned()]); n]
    }

    fn quant(inner: Pattern, q: Quantifier, reluctant: bool) -> Pattern {
        Pattern::Quantified(Box::new(inner), q, reluctant)
    }

    fn labels(ls: &[&str]) -> Vec<String> {
        ls.iter().map(|s| s.to_string()).collect()
    }

    /// Provisional matches as `(start_pos, end_pos, labels)`. In the truncation tests seqs are
    /// assigned equal to final sorted position, so a seq is directly its position and these triples
    /// line up with the batch oracle's position-anchored spans.
    fn provisional_triples(inc: &IncrementalMatcher) -> Vec<(usize, usize, Vec<String>)> {
        inc.provisional()
            .iter()
            .map(|m| {
                (
                    m.start_seq.0 as usize,
                    m.end_seq.0 as usize,
                    m.labels.clone(),
                )
            })
            .collect()
    }

    /// Batch oracle over `rows` as `(start, end, labels)` triples (labels included so a *steal* —
    /// a row rebinding to a different variable — is caught, not just span changes).
    async fn batch_triples(
        nfa: &Nfa,
        skip: &SkipMode,
        rows: &[BTreeSet<String>],
    ) -> Vec<(usize, usize, Vec<String>)> {
        let matcher = SetMatcher::new(rows.to_vec());
        nfa.find_matches_dynamic(rows.len(), &matcher, skip)
            .await
            .unwrap()
            .iter()
            .map(|m| (m.start, m.end, m.labels.clone()))
            .collect()
    }

    #[tokio::test]
    async fn incremental_equals_batch_in_order() {
        // pattern (a b+) — greedy trailing quantifier exercises the "still-open trailing match must
        // re-scan" rule.
        let pat = Pattern::Concat(vec![
            Pattern::Var("a".into()),
            Pattern::Quantified(Box::new(Pattern::Var("b".into())), Quantifier::Plus, false),
        ]);
        let nfa = Nfa::compile(&pat);
        let rows = vec![
            sets(&["a"]),
            sets(&["b"]),
            sets(&["b"]),
            sets(&["a"]),
            sets(&["b"]),
        ];
        // every split point, including feeding one row at a time
        assert_equiv(&nfa, SkipMode::PastLastRow, &rows, &[1]).await;
        assert_equiv(&nfa, SkipMode::PastLastRow, &rows, &[1, 2, 3, 4]).await;
        assert_equiv(&nfa, SkipMode::ToNextRow, &rows, &[2]).await;
    }

    #[tokio::test]
    async fn alternation_incremental_equals_batch() {
        // (a | b) c — the standard alternation shape from nfa.rs's own tests. The trailing `c`
        // makes a match ending at the buffer boundary re-attempt until the `c` row arrives.
        let pat = Pattern::Concat(vec![
            Pattern::Alt(vec![Pattern::Var("a".into()), Pattern::Var("b".into())]),
            Pattern::Var("c".into()),
        ]);
        let nfa = Nfa::compile(&pat);
        let rows = from_str("acbc");
        for split in [&[1][..], &[2][..], &[1, 2, 3][..], &[3][..]] {
            assert_equiv(&nfa, SkipMode::PastLastRow, &rows, split).await;
        }
    }

    #[tokio::test]
    async fn range_quantifier_incremental_equals_batch() {
        // a{2,3} — a bounded range that greedily takes up to three, with the optional third copy
        // ending at the boundary (open) until the next row disambiguates it.
        let pat = quant(
            Pattern::Var("a".into()),
            Quantifier::Range {
                min: 2,
                max: Some(3),
            },
            false,
        );
        let nfa = Nfa::compile(&pat);
        let rows = from_str("aaxaa");
        for split in [&[1][..], &[2][..], &[1, 2, 3, 4][..], &[3][..]] {
            assert_equiv(&nfa, SkipMode::PastLastRow, &rows, split).await;
        }
    }

    #[tokio::test]
    async fn reluctant_quantifier_incremental_equals_batch() {
        // a+? b over rows that each satisfy both a and b: the reluctant `a+?` takes the fewest `a`,
        // so each match is exactly "ab" and the split between them is preference-, not predicate-,
        // driven.
        let pat = Pattern::Concat(vec![
            quant(Pattern::Var("a".into()), Quantifier::Plus, true),
            Pattern::Var("b".into()),
        ]);
        let nfa = Nfa::compile(&pat);
        let rows = ab_rows(4);
        for split in [&[1][..], &[2][..], &[1, 2, 3][..]] {
            assert_equiv(&nfa, SkipMode::PastLastRow, &rows, split).await;
        }
    }

    #[tokio::test]
    async fn to_next_row_overlap_incremental_equals_batch() {
        // a+ with SKIP TO NEXT ROW: overlapping matches (0,3),(1,3),(2,3) over "aaa" — the exact
        // overlap case from nfa.rs's `find_matches_skip_to_next_row_overlaps`. Every open match
        // ends at the boundary, so none freezes until a non-`a` row (or nothing) follows.
        let pat = quant(Pattern::Var("a".into()), Quantifier::Plus, false);
        let nfa = Nfa::compile(&pat);
        let rows = from_str("aaa");
        for split in [&[1][..], &[2][..], &[1, 2][..]] {
            assert_equiv(&nfa, SkipMode::ToNextRow, &rows, split).await;
        }
        // and with a trailing non-`a` row that closes all three matches strictly before the boundary
        let rows = from_str("aaab");
        assert_equiv(&nfa, SkipMode::ToNextRow, &rows, &[1, 2, 3]).await;
    }

    #[tokio::test]
    async fn empty_advances_are_noops() {
        // A repeated split point feeds an empty `advance(&[])`; a leading `0` feeds one before any
        // real row. Both must be no-ops, so the final matches still equal the batch answer.
        let pat = Pattern::Concat(vec![
            Pattern::Var("a".into()),
            quant(Pattern::Var("b".into()), Quantifier::Plus, false),
        ]);
        let nfa = Nfa::compile(&pat);
        let rows = from_str("abbab");
        assert_equiv(&nfa, SkipMode::PastLastRow, &rows, &[2, 2, 3]).await; // empty in the middle
        assert_equiv(&nfa, SkipMode::PastLastRow, &rows, &[0, 1, 3]).await; // empty at the very start
        assert_equiv(&nfa, SkipMode::PastLastRow, &rows, &[1, 3, 3, 3]).await; // empties at the end
    }

    #[tokio::test]
    async fn single_row_feeds_over_twelve_rows() {
        // Feed 12-row inputs one row at a time (split = [1..=11]) for several patterns.
        let one_at_a_time: Vec<usize> = (1..12).collect();

        let ab_plus = Nfa::compile(&Pattern::Concat(vec![
            Pattern::Var("a".into()),
            quant(Pattern::Var("b".into()), Quantifier::Plus, false),
        ]));
        assert_equiv(
            &ab_plus,
            SkipMode::PastLastRow,
            &from_str("abbabbaabbab"),
            &one_at_a_time,
        )
        .await;

        let alt_c = Nfa::compile(&Pattern::Concat(vec![
            Pattern::Alt(vec![Pattern::Var("a".into()), Pattern::Var("b".into())]),
            Pattern::Var("c".into()),
        ]));
        assert_equiv(
            &alt_c,
            SkipMode::PastLastRow,
            &from_str("acbcacxbcacb"),
            &one_at_a_time,
        )
        .await;
        assert_equiv(
            &alt_c,
            SkipMode::PastLastRow,
            &from_str("acbcbcacacbc"),
            &one_at_a_time,
        )
        .await;

        // overlapping matches under ToNextRow, single-fed
        let a_plus = Nfa::compile(&quant(Pattern::Var("a".into()), Quantifier::Plus, false));
        assert_equiv(
            &a_plus,
            SkipMode::ToNextRow,
            &from_str("aaxaaaxaaaax"),
            &one_at_a_time,
        )
        .await;
    }

    /// The freezing gate must consult boundary liveness, not just "match ended before the boundary".
    /// `(a b c) | a` over `[a, b]` returns the fallback `a` match `(0,1)` while the longer,
    /// higher-preference `a b c` branch is still alive *at* the boundary (waiting for `c`). The
    /// naive `end < n_rows` rule would freeze `(0,1)`; the liveness gate sees position 0 alive and
    /// defers, so when `c` arrives the rescan finds the batch answer `(0,3)` ("abc").
    #[tokio::test]
    async fn alternation_alive_at_boundary_defers_freezing() {
        let pat = Pattern::Alt(vec![
            Pattern::Concat(vec![
                Pattern::Var("a".into()),
                Pattern::Var("b".into()),
                Pattern::Var("c".into()),
            ]),
            Pattern::Var("a".into()),
        ]);
        let nfa = Nfa::compile(&pat);
        let rows = from_str("abc");
        assert_equiv(&nfa, SkipMode::PastLastRow, &rows, &[2]).await;
        assert_equiv(&nfa, SkipMode::PastLastRow, &rows, &[1, 2]).await;
    }

    /// A *gap* position (no match there yet) can be the live one: `(a n n n) | n` over `[a, n, n]`
    /// finds only `n` matches, but position 0's `a n n n` branch is alive at the boundary — one more
    /// `n` turns the batch answer into the single match `(0,4)`. Freezing the early `n` matches
    /// (whose own starts are dead) would lose it, so the gate must check every position in the
    /// would-be-frozen region, not just match starts.
    #[tokio::test]
    async fn live_gap_position_defers_freezing() {
        let pat = Pattern::Alt(vec![
            Pattern::Concat(vec![
                Pattern::Var("a".into()),
                Pattern::Var("n".into()),
                Pattern::Var("n".into()),
                Pattern::Var("n".into()),
            ]),
            Pattern::Var("n".into()),
        ]);
        let nfa = Nfa::compile(&pat);
        let rows = from_str("annn");
        assert_equiv(&nfa, SkipMode::PastLastRow, &rows, &[3]).await;
        assert_equiv(&nfa, SkipMode::PastLastRow, &rows, &[1, 2, 3]).await;
    }

    /// A greedy trailing quantifier keeps the last match alive at the buffer end forever: `(a b+)`
    /// fed one row at a time never freezes its trailing match, and the match keeps extending as
    /// each `b` arrives.
    #[tokio::test]
    async fn trailing_quantified_match_extends_without_freezing() {
        let pat = Pattern::Concat(vec![
            Pattern::Var("a".into()),
            quant(Pattern::Var("b".into()), Quantifier::Plus, false),
        ]);
        let nfa = Nfa::compile(&pat);
        let rows = from_str("abbb");
        let matcher = SetMatcher::new(rows.clone());
        let mut inc =
            IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), SkipMode::PastLastRow);

        // [a]: `a` alone doesn't satisfy `a b+`, but it is alive (a `b` may arrive) — no match yet,
        // nothing frozen.
        inc.advance(&ss(&[0]), &matcher, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();
        assert_eq!(inc.provisional(), &[]);
        assert_eq!(inc.frozen(), 0);

        // Each appended `b` extends the same match by one row; it always ends at the buffer end, so
        // it stays alive and never freezes.
        for (seq, expected_end) in [(1i64, 2i64), (2, 3), (3, 4)] {
            inc.advance(&ss(&[seq]), &matcher, &mut ScanBudget::unlimited(), false)
                .await
                .unwrap();
            assert_eq!(
                inc.provisional(),
                &[SeqMatch {
                    start_seq: Seq(0),
                    end_seq: Seq(expected_end),
                    labels: std::iter::once("a".to_owned())
                        .chain(std::iter::repeat_n(
                            "b".to_owned(),
                            expected_end as usize - 1
                        ))
                        .collect(),
                }]
            );
            assert_eq!(inc.frozen(), 0);
        }
    }

    /// Step 1 out-of-order reinsert with a *steal*. Rows arrive `[r0, r1, r3, r4]`; a late `r2`
    /// lands between `r1` and `r3`. Pattern `a+ b` over the pre-insert rows `[{a}, {a,b}, {x}, {x}]`
    /// freezes the match `a b` = `(0,2)` with `r1` bound as the closing `b`. The late `r2 = {b}`
    /// inserted at position 2 lets the greedy `a+` swallow `r1` as an extra `a` and bind `r2` as the
    /// `b`, so the batch answer over `[{a}, {a,b}, {b}, {x}, {x}]` is the longer `(0,3)` = `a a b`
    /// (r1 stolen from `b` to `a`). Truncating at `r3`'s seq must invalidate the frozen `(0,2)` — its
    /// end reaches the truncation point — and rewind so the re-feed re-derives `(0,3)`.
    #[tokio::test]
    async fn out_of_order_reinsert_equals_batch() {
        let pat = Pattern::Concat(vec![
            quant(Pattern::Var("a".into()), Quantifier::Plus, false),
            Pattern::Var("b".into()),
        ]);
        let nfa = Nfa::compile(&pat);
        let skip = SkipMode::PastLastRow;

        // Buffer before the late arrival (sorted positions 0..4).
        let pre_rows = vec![sets(&["a"]), sets(&["a", "b"]), sets(&["x"]), sets(&["x"])];
        let pre_matcher = SetMatcher::new(pre_rows.clone());

        let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
        inc.advance(
            &ss(&[0, 1, 2, 3]),
            &pre_matcher,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();
        // `a b` = (0,2) freezes with r1 bound `b`.
        assert_eq!(provisional_triples(&inc), vec![(0, 2, labels(&["a", "b"]))]);
        assert_eq!(inc.frozen(), 1);

        // r2 = {b} sorts between r1 (pos 1) and r3 (pos 2). r3 is the first buffered row whose sorted
        // position changes, so the caller truncates at r3's seq (2).
        inc.truncate_from_seq(Seq(2), &pre_matcher, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();
        // The frozen match reached the truncation point, so nothing survives.
        assert_eq!(provisional_triples(&inc), vec![]);
        assert_eq!(inc.frozen(), 0);

        // Re-feed the sorted suffix [r2, r3, r4] with their final positions as seqs.
        let final_rows = vec![
            sets(&["a"]),
            sets(&["a", "b"]),
            sets(&["b"]),
            sets(&["x"]),
            sets(&["x"]),
        ];
        let final_matcher = SetMatcher::new(final_rows.clone());
        inc.advance(
            &ss(&[2, 3, 4]),
            &final_matcher,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();

        assert_eq!(
            provisional_triples(&inc),
            batch_triples(&nfa, &skip, &final_rows).await
        );
        assert_eq!(
            provisional_triples(&inc),
            vec![(0, 3, labels(&["a", "a", "b"]))]
        );
    }

    /// Truncation landing in the *middle* of the frozen region: an earlier frozen match survives
    /// while a later one is dropped, so `next_pos` rewinds to the survivor's resume point (not 0).
    /// Pattern `a b` over `[{a},{b},{x},{a},{b},{x}]` freezes both `(0,2)` and `(3,5)`. A late `{a}`
    /// sorts at position 3 (before the second match): truncating at that row's seq keeps `(0,2)` and
    /// invalidates `(3,5)`, and the re-feed re-derives the shifted second match `(4,6)`.
    #[tokio::test]
    async fn truncate_inside_frozen_region_keeps_earlier_matches() {
        let pat = Pattern::Concat(vec![Pattern::Var("a".into()), Pattern::Var("b".into())]);
        let nfa = Nfa::compile(&pat);
        let skip = SkipMode::PastLastRow;

        let pre_rows = vec![
            sets(&["a"]),
            sets(&["b"]),
            sets(&["x"]),
            sets(&["a"]),
            sets(&["b"]),
            sets(&["x"]),
        ];
        let pre_matcher = SetMatcher::new(pre_rows.clone());

        let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
        inc.advance(
            &ss(&[0, 1, 2, 3, 4, 5]),
            &pre_matcher,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            provisional_triples(&inc),
            vec![(0, 2, labels(&["a", "b"])), (3, 5, labels(&["a", "b"]))]
        );
        assert_eq!(inc.frozen(), 2);

        // A late {a} sorts at position 3; the old row at position 3 is the first whose sorted position
        // changes, so the caller truncates at its seq (3).
        inc.truncate_from_seq(Seq(3), &pre_matcher, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();
        // (0,2) survives (its region is dead at boundary 3); (3,5) reaches past it and is dropped.
        assert_eq!(provisional_triples(&inc), vec![(0, 2, labels(&["a", "b"]))]);
        assert_eq!(inc.frozen(), 1);

        // Re-feed the sorted suffix [late {a}, old rows] from position 3, with final positions as seqs.
        let final_rows = vec![
            sets(&["a"]),
            sets(&["b"]),
            sets(&["x"]),
            sets(&["a"]),
            sets(&["a"]),
            sets(&["b"]),
            sets(&["x"]),
        ];
        let final_matcher = SetMatcher::new(final_rows.clone());
        inc.advance(
            &ss(&[3, 4, 5, 6]),
            &final_matcher,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();

        assert_eq!(
            provisional_triples(&inc),
            batch_triples(&nfa, &skip, &final_rows).await
        );
        assert_eq!(
            provisional_triples(&inc),
            vec![(0, 2, labels(&["a", "b"])), (4, 6, labels(&["a", "b"]))]
        );
    }

    /// Truncating at the first fed row is a full reset. A late `{a}` sorting before everything shifts
    /// all positions, so the caller truncates at seq 0; state must clear entirely, and re-feeding the
    /// whole corrected sequence must equal the batch answer.
    #[tokio::test]
    async fn truncate_to_zero_resets_and_refeeds() {
        let pat = Pattern::Concat(vec![Pattern::Var("a".into()), Pattern::Var("b".into())]);
        let nfa = Nfa::compile(&pat);
        let skip = SkipMode::PastLastRow;

        let pre_rows = vec![sets(&["a"]), sets(&["b"]), sets(&["x"])];
        let pre_matcher = SetMatcher::new(pre_rows.clone());

        let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
        inc.advance(
            &ss(&[0, 1, 2]),
            &pre_matcher,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();
        assert_eq!(provisional_triples(&inc), vec![(0, 2, labels(&["a", "b"]))]);
        assert_eq!(inc.frozen(), 1);

        // A late {a} sorts before r0, so r0 (seq 0) is the first row whose position changes.
        inc.truncate_from_seq(Seq(0), &pre_matcher, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();
        assert_eq!(provisional_triples(&inc), vec![]);
        assert_eq!(inc.frozen(), 0);

        let final_rows = vec![sets(&["a"]), sets(&["a"]), sets(&["b"]), sets(&["x"])];
        let final_matcher = SetMatcher::new(final_rows.clone());
        inc.advance(
            &ss(&[0, 1, 2, 3]),
            &final_matcher,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();

        assert_eq!(
            provisional_triples(&inc),
            batch_triples(&nfa, &skip, &final_rows).await
        );
        assert_eq!(provisional_triples(&inc), vec![(1, 3, labels(&["a", "b"]))]);
    }

    /// THE case a positional (matcher-free) truncation rule gets wrong — do not simplify
    /// `truncate_from_seq` back to "drop frozen matches whose end position >= `trunc_pos`".
    ///
    /// Pattern `(a b c d) | (a b)` (long branch preferred) over `[{a},{b},{c},{x}]`: the short
    /// branch matches `(0,2)`, and it freezes only once the `x` at position 3 kills the long branch
    /// (at boundary 3 the long branch is still alive — `a b c` reaches the boundary inside the
    /// automaton — so no freeze happens there). A late `{d}` then sorts at position 3, displacing
    /// the `x`. Truncating at the x-row's seq re-checks the frozen region against boundary 3, where
    /// position 0 is alive again, so `(0,2)` must be dropped even though its end (2) lies strictly
    /// before the truncation position (3); the re-feed then derives the long match `(0,4)`. The
    /// positional rule keeps `(0,2)` and rewinds to its resume point 2 — no match can start at
    /// `{c}`/`{d}`/`{x}`, so it would wrongly answer `(0,2)` forever.
    #[tokio::test]
    async fn truncation_recheck_drops_frozen_match_alive_at_new_boundary() {
        let pat = Pattern::Alt(vec![
            Pattern::Concat(vec![
                Pattern::Var("a".into()),
                Pattern::Var("b".into()),
                Pattern::Var("c".into()),
                Pattern::Var("d".into()),
            ]),
            Pattern::Concat(vec![Pattern::Var("a".into()), Pattern::Var("b".into())]),
        ]);
        let nfa = Nfa::compile(&pat);
        let skip = SkipMode::PastLastRow;

        let pre_rows = vec![sets(&["a"]), sets(&["b"]), sets(&["c"]), sets(&["x"])];
        let pre_matcher = SetMatcher::new(pre_rows.clone());

        let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
        inc.advance(
            &ss(&[0, 1, 2, 3]),
            &pre_matcher,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();
        // The `x` kills the long branch at position 3, so the short `(0,2)` freezes.
        assert_eq!(provisional_triples(&inc), vec![(0, 2, labels(&["a", "b"]))]);
        assert_eq!(inc.frozen(), 1);

        // The late {d} sorts at position 3; the old {x} row (seq 3) is the first buffered row whose
        // sorted position changes, so the caller truncates at its seq.
        inc.truncate_from_seq(Seq(3), &pre_matcher, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();
        // Discriminator: the frozen (0,2) ends *before* the truncation position, yet position 0 is
        // alive at the new boundary — the liveness re-check must drop it. The positional rule keeps
        // it here, and these two assertions (and the batch check below) fail under that rule.
        assert_eq!(provisional_triples(&inc), vec![]);
        assert_eq!(inc.frozen(), 0);

        // Re-feed the sorted suffix [late {d}, old {x}] with final positions as seqs.
        let final_rows = vec![
            sets(&["a"]),
            sets(&["b"]),
            sets(&["c"]),
            sets(&["d"]),
            sets(&["x"]),
        ];
        let final_matcher = SetMatcher::new(final_rows.clone());
        inc.advance(
            &ss(&[3, 4]),
            &final_matcher,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();

        assert_eq!(
            provisional_triples(&inc),
            batch_triples(&nfa, &skip, &final_rows).await
        );
        assert_eq!(
            provisional_triples(&inc),
            vec![(0, 4, labels(&["a", "b", "c", "d"]))]
        );
    }

    /// Truncating at a seq that was never fed is a no-op: neither a seq past everything buffered nor
    /// one exactly one-past-the-end may touch state, and later appends must still equal the batch.
    #[tokio::test]
    async fn truncate_unknown_seq_is_noop() {
        let pat = Pattern::Concat(vec![
            Pattern::Var("a".into()),
            quant(Pattern::Var("b".into()), Quantifier::Plus, false),
        ]);
        let nfa = Nfa::compile(&pat);
        let skip = SkipMode::PastLastRow;

        let rows = from_str("abbc");
        let matcher = SetMatcher::new(rows.clone());

        let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
        inc.advance(
            &ss(&[0, 1, 2]),
            &matcher,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();
        let before = inc.provisional().to_vec();
        let before_frozen = inc.frozen();

        // A seq far past everything buffered, and the seq exactly one past the last fed row: both are
        // absent from `seq_index`, so both leave state untouched.
        inc.truncate_from_seq(Seq(99), &matcher, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();
        inc.truncate_from_seq(Seq(3), &matcher, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();
        assert_eq!(inc.provisional(), before.as_slice());
        assert_eq!(inc.frozen(), before_frozen);

        // Appending really does append (nothing corrupted): the final answer equals the batch.
        inc.advance(&ss(&[3]), &matcher, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();
        assert_eq!(
            provisional_triples(&inc),
            batch_triples(&nfa, &skip, &rows).await
        );
    }

    /// `SeqMatch`es (e.g. the finalized-and-returned ones) as `(start, end, labels)` triples, so
    /// they line up with the position-anchored batch oracle (seqs equal final sorted positions in
    /// these tests).
    fn seq_triples(ms: &[SeqMatch]) -> Vec<(usize, usize, Vec<String>)> {
        ms.iter()
            .map(|m| {
                (
                    m.start_seq.0 as usize,
                    m.end_seq.0 as usize,
                    m.labels.clone(),
                )
            })
            .collect()
    }

    /// Generic oracle: feeding rows incrementally (in any split) through `matcher` must equal one
    /// batch `find_matches_dynamic` with the *same* `matcher`. Unlike [`assert_equiv`] this takes an
    /// arbitrary [`CandidateMatcher`] (not just [`SetMatcher`]), so a matcher that applies its own
    /// pruning — e.g. the `WITHIN` span prune — can be driven through the incremental path.
    async fn assert_equiv_with<M: CandidateMatcher + Sync>(
        nfa: &Nfa,
        skip: SkipMode,
        n_rows: usize,
        matcher: &M,
        split_at: &[usize],
    ) {
        let batch: Vec<(usize, usize, Vec<String>)> = nfa
            .find_matches_dynamic(n_rows, matcher, &skip)
            .await
            .unwrap()
            .iter()
            .map(|m| (m.start, m.end, m.labels.clone()))
            .collect();
        let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
        let mut fed = 0usize;
        for &cut in split_at.iter().chain(std::iter::once(&n_rows)) {
            let seqs: Vec<Seq> = (fed..cut).map(|i| Seq(i as i64)).collect();
            inc.advance(&seqs, matcher, &mut ScanBudget::unlimited(), false)
                .await
                .unwrap();
            fed = cut;
        }
        assert_eq!(provisional_triples(&inc), batch);
    }

    /// A [`CandidateMatcher`] that models the `WITHIN` span prune the executor applies inside
    /// `DefineMatcher::matches` (see `executor.rs`): binding a candidate at `pos` extends the match
    /// to span `[match_start, pos]`, and the executor rejects the candidate when that span exceeds
    /// the bound, so the NFA backtracks to the longest match that fits the window. `WITHIN` lives
    /// entirely inside the `CandidateMatcher`; this module has no `WITHIN` logic of its own — it
    /// hands the matcher straight to `find_matches_dynamic` and `reaches_boundary_alive` — so
    /// driving a span-pruning matcher through the incremental path and checking equality with the
    /// batch path proves the pass-through. (`nfa.rs`'s `SetMatcher` has no `WITHIN`, and the real
    /// `DefineMatcher` needs the executor's expression/row machinery, so we model the prune here.)
    struct WithinSetMatcher {
        rows: Vec<BTreeSet<String>>,
        /// Max span in order-key units. Seqs equal positions here, so the span of a candidate at
        /// `pos` is `pos - match_start == labels.len()`.
        max_span: usize,
    }

    impl CandidateMatcher for WithinSetMatcher {
        async fn matches(
            &self,
            var: &str,
            pos: usize,
            labels: &[String],
        ) -> StreamExecutorResult<bool> {
            if !self.rows[pos].contains(var) {
                return Ok(false);
            }
            let match_start = pos - labels.len();
            Ok(pos - match_start <= self.max_span)
        }
    }

    /// (a) Finalize mid-stream, then keep feeding: the finalized prefix is removed from and returned
    /// out of the diffable set, `provisional()` keeps only the still-revisable matches, and the
    /// union `provisional() ∪ returned` equals the batch oracle over all rows — with the rebased
    /// bookkeeping proven by advancing further after the finalization and still matching the oracle.
    #[tokio::test]
    async fn finalize_removes_prefix_and_rebases_bookkeeping() {
        let pat = Pattern::Concat(vec![Pattern::Var("a".into()), Pattern::Var("b".into())]);
        let nfa = Nfa::compile(&pat);
        let skip = SkipMode::PastLastRow;

        // 0:a 1:b 2:x 3:a 4:b 5:x 6:a 7:b 8:x  -> batch matches (0,2),(3,5),(6,8).
        let full = from_str("abxabxabx");
        let m_full = SetMatcher::new(full.clone());

        let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
        inc.advance(
            &ss(&[0, 1, 2, 3, 4, 5]),
            &m_full,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            provisional_triples(&inc),
            vec![(0, 2, labels(&["a", "b"])), (3, 5, labels(&["a", "b"]))]
        );
        assert_eq!(inc.frozen(), 2);

        // Finalize everything before seq 3 (evict sorted positions [0,3)): removes the wholly-inside
        // match (0,2); (3,5) starts at the boundary and is kept.
        let removed = finalize_rebased(&mut inc, Seq(3));
        assert_eq!(seq_triples(&removed), vec![(0, 2, labels(&["a", "b"]))]);
        assert_eq!(provisional_triples(&inc), vec![(3, 5, labels(&["a", "b"]))]);
        assert_eq!(inc.frozen(), 1);

        // Keep feeding rows 6,7,8. Their buffer positions are now rebased (row 3 sits at position 0),
        // so the matcher indexes the surviving buffer `full[3..]`.
        let m_tail = SetMatcher::new(full[3..].to_vec());
        inc.advance(
            &ss(&[6, 7, 8]),
            &m_tail,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            provisional_triples(&inc),
            vec![(3, 5, labels(&["a", "b"])), (6, 8, labels(&["a", "b"]))]
        );

        // Union of the returned finalized match and the surviving provisional set equals the batch
        // oracle over the whole run.
        let mut union = seq_triples(&removed);
        union.extend(provisional_triples(&inc));
        assert_eq!(union, batch_triples(&nfa, &skip, &full).await);
    }

    /// (a) Bookkeeping consistency across *all three* operations after a finalization: finalize a
    /// prefix, advance to derive more matches, then take a late (out-of-order) row that reinserts
    /// into the already-rebased tail — `truncate_from_seq` + re-feed — and the union still equals the
    /// batch oracle over the corrected full sequence. Exercises seq→position mapping against the
    /// rebased `seq_index` in both `advance` and `truncate_from_seq`.
    #[tokio::test]
    async fn finalize_then_truncate_and_advance_equals_batch() {
        let pat = Pattern::Concat(vec![Pattern::Var("a".into()), Pattern::Var("b".into())]);
        let nfa = Nfa::compile(&pat);
        let skip = SkipMode::PastLastRow;

        // 0:a 1:b 2:x 3:a 4:b 5:x 6:a 7:b 8:x
        let pre = from_str("abxabxabx");
        let m_pre = SetMatcher::new(pre.clone());

        let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
        inc.advance(
            &ss(&[0, 1, 2, 3, 4, 5, 6, 7, 8]),
            &m_pre,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            provisional_triples(&inc),
            vec![
                (0, 2, labels(&["a", "b"])),
                (3, 5, labels(&["a", "b"])),
                (6, 8, labels(&["a", "b"])),
            ]
        );
        assert_eq!(inc.frozen(), 3);

        // Finalize before seq 3 (evict [0,3)); returns (0,2), rebases so row 3 is now position 0.
        let removed = finalize_rebased(&mut inc, Seq(3));
        assert_eq!(seq_triples(&removed), vec![(0, 2, labels(&["a", "b"]))]);
        assert_eq!(
            provisional_triples(&inc),
            vec![(3, 5, labels(&["a", "b"])), (6, 8, labels(&["a", "b"]))]
        );

        // A late {a} sorts at global position 6 (before old row 6): old row 6 (seq 6) is the first
        // buffered row whose sorted position changes, so the caller truncates at seq 6. The matcher
        // indexes the rebased surviving buffer `pre[3..]`.
        let m_pre_tail = SetMatcher::new(pre[3..].to_vec());
        inc.truncate_from_seq(Seq(6), &m_pre_tail, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();
        // (3,5) survives (its region is dead at the truncation boundary); (6,8) reaches past it and
        // is dropped, to be re-derived by the re-feed.
        assert_eq!(provisional_triples(&inc), vec![(3, 5, labels(&["a", "b"]))]);

        // Corrected full sequence with the late {a} inserted at position 6:
        // 0:a 1:b 2:x 3:a 4:b 5:x 6:a 7:a 8:b 9:x  -> batch (0,2),(3,5),(7,9).
        let corrected = from_str("abxabxaabx");
        // Re-feed the sorted suffix from global position 6 (seqs 6..=9), matcher over the rebased
        // surviving buffer `corrected[3..]`.
        let m_corr_tail = SetMatcher::new(corrected[3..].to_vec());
        inc.advance(
            &ss(&[6, 7, 8, 9]),
            &m_corr_tail,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();

        let mut union = seq_triples(&removed);
        union.extend(provisional_triples(&inc));
        assert_eq!(union, batch_triples(&nfa, &skip, &corrected).await);
    }

    /// (a) Robustness: neither a never-fed boundary nor one at the very first row may touch state,
    /// and later appends still equal the batch. A never-fed boundary reports `MustRebuild` (the
    /// caller drops and rebuilds) without mutating; a boundary at the first row (`final_pos == 0`)
    /// evicts nothing (`Rebased` with an empty removed set).
    #[tokio::test]
    async fn finalize_unknown_or_zero_seq_leaves_state_intact() {
        let pat = Pattern::Concat(vec![Pattern::Var("a".into()), Pattern::Var("b".into())]);
        let nfa = Nfa::compile(&pat);
        let skip = SkipMode::PastLastRow;

        let rows = from_str("abxab");
        let matcher = SetMatcher::new(rows.clone());

        let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
        inc.advance(
            &ss(&[0, 1, 2, 3]),
            &matcher,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();
        let before = inc.provisional().to_vec();
        let before_frozen = inc.frozen();

        // Never fed: decline (the executor drops + rebuilds), leaving state untouched.
        assert!(matches!(
            inc.finalize_evicted_prefix(Seq(99)),
            Finalized::MustRebuild
        ));
        // final_pos == 0: rebased, evicting nothing.
        assert_eq!(finalize_rebased(&mut inc, Seq(0)), vec![]);
        assert_eq!(inc.provisional(), before.as_slice());
        assert_eq!(inc.frozen(), before_frozen);

        inc.advance(&ss(&[4]), &matcher, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();
        assert_eq!(
            provisional_triples(&inc),
            batch_triples(&nfa, &skip, &rows).await
        );
    }

    /// (b) `WITHIN` parity: a matcher that applies the `WITHIN` span prune drives identically through
    /// the incremental path and the batch path. `a b+` with a max span of one row (so a match may
    /// span at most two rows, `a b`) over `abbabb`: without `WITHIN` the greedy `b+` swallows both
    /// `b`s per match; `WITHIN` caps each match at `ab`. The incremental path must track that through
    /// both matching and the freeze-gate liveness check — proving pass-through, since this module has
    /// no `WITHIN` logic of its own.
    #[tokio::test]
    async fn within_span_prune_incremental_equals_batch() {
        let pat = Pattern::Concat(vec![
            Pattern::Var("a".into()),
            quant(Pattern::Var("b".into()), Quantifier::Plus, false),
        ]);
        let nfa = Nfa::compile(&pat);
        let rows = from_str("abbabb");
        let matcher = WithinSetMatcher {
            rows: rows.clone(),
            max_span: 1,
        };
        for split in [
            &[1][..],
            &[2][..],
            &[3][..],
            &[1, 2, 3, 4, 5][..],
            &[2, 4][..],
        ] {
            assert_equiv_with(&nfa, SkipMode::PastLastRow, rows.len(), &matcher, split).await;
        }
    }

    /// (c) Finalization must never reach into the open (non-frozen) trailing region: the boundary
    /// has to lie within the frozen prefix. `a b+` fed one row at a time keeps its trailing greedy
    /// match alive at the buffer end forever, so nothing freezes (`next_pos == 0`). Finalizing before
    /// seq 2 (a position past the frozen prefix) must return `MustRebuild` rather than reach into the
    /// open region (formerly a debug-assert panic; now a matcher-owned decision).
    #[tokio::test]
    async fn finalize_into_open_region_must_rebuild() {
        let pat = Pattern::Concat(vec![
            Pattern::Var("a".into()),
            quant(Pattern::Var("b".into()), Quantifier::Plus, false),
        ]);
        let nfa = Nfa::compile(&pat);
        let rows = from_str("abb");
        let matcher = SetMatcher::new(rows.clone());

        let mut inc =
            IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), SkipMode::PastLastRow);
        inc.advance(
            &ss(&[0, 1, 2]),
            &matcher,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();
        // Trailing greedy match (0,3) stays alive at the boundary: nothing frozen.
        assert_eq!(inc.frozen(), 0);
        // seq 2 sits at position 2, past the frozen prefix (next_pos == 0): decline to rebase.
        assert!(matches!(
            inc.finalize_evicted_prefix(Seq(2)),
            Finalized::MustRebuild
        ));
    }

    /// (Task 7, Step 1) After `finalize_evicted_prefix` evicts a frozen match, the matcher never
    /// revisits its rows: subsequent `advance`s extend only the *surviving* matches, and a
    /// `diff_provisional` against the executor's pruned diff base (the finalized start dropped)
    /// emits no op touching that start.
    ///
    /// The pattern is the greedy `a b+`, whose trailing quantifier *would* keep swallowing later
    /// `b`s if a match were still open — the concrete "later rows would have extended it under
    /// `PastLastRow`" shape from the brief. It cannot re-extend the finalized `a b` here, and that is
    /// the freezing invariant, not luck: a match only freezes once its whole scan region is dead at
    /// the boundary, and a position dead at a boundary stays dead at every larger boundary, so no
    /// appended row can revive it. Finalization then drains the frozen match's rows from
    /// `seq_index` and rebases the scan cursor past them, so the later `b` attaches to the surviving
    /// second match instead. This locks in the property Task 7's watermark emit-before-finalize
    /// relies on: a finalized match is a permanent result the diff base must forget without a
    /// retraction, and later input can neither resurrect nor mutate it.
    #[tokio::test]
    async fn finalize_evicts_then_later_rows_never_revisit_finalized_match() {
        let pat = Pattern::Concat(vec![
            Pattern::Var("a".into()),
            quant(Pattern::Var("b".into()), Quantifier::Plus, false),
        ]);
        let nfa = Nfa::compile(&pat);
        let skip = SkipMode::PastLastRow;

        // 0:a 1:b 2:a 3:b -> the first `a b` = (0,2) freezes (its region goes dead at the boundary
        // once the second match's `a` at position 2 breaks the greedy `b+`); the trailing (2,4)
        // stays open at the boundary and does not freeze.
        let pre = from_str("abab");
        let m_pre = SetMatcher::new(pre.clone());
        let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
        inc.advance(
            &ss(&[0, 1, 2, 3]),
            &m_pre,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            provisional_triples(&inc),
            vec![(0, 2, labels(&["a", "b"])), (2, 4, labels(&["a", "b"]))]
        );
        assert_eq!(inc.frozen(), 1); // only the first match froze; the trailing one is still open

        // Executor-side diff base: everything currently provisional has been emitted (one row per
        // match, keyed by its start seq so an op referencing the finalized start is detectable).
        let base: Vec<(SeqMatch, OwnedRow)> = inc
            .provisional()
            .iter()
            .map(|m| (m.clone(), orow(m.start_seq.0)))
            .collect();

        // Finalize before seq 2: evict [0:a, 1:b], returning the frozen (0,2). The executor prunes
        // the finalized start from its base without a retraction (a finalized match is permanent).
        let removed = finalize_rebased(&mut inc, Seq(2));
        assert_eq!(seq_triples(&removed), vec![(0, 2, labels(&["a", "b"]))]);
        assert_eq!(provisional_triples(&inc), vec![(2, 4, labels(&["a", "b"]))]);
        let mut base_pruned = base.clone();
        base_pruned.retain(|(m, _)| !removed.iter().any(|fm| fm.start_seq == m.start_seq));

        // Feed a later `b` (seq 4). If the matcher revisited the evicted `a b`, the greedy `b+`
        // would extend it to `a b b`; instead its rows are gone and the surviving second match
        // (seq 2) grows to `a b b` = (2,5). The matcher indexes the rebased surviving buffer: old
        // positions 2,3 sit at 0,1 (rows {a},{b}) and seq 4 lands at position 2.
        let tail = from_str("abb");
        let m_tail = SetMatcher::new(tail.clone());
        inc.advance(&ss(&[4]), &m_tail, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();
        assert_eq!(
            provisional_triples(&inc),
            vec![(2, 5, labels(&["a", "b", "b"]))]
        );
        assert!(
            inc.provisional().iter().all(|m| m.start_seq != Seq(0)),
            "finalized match start must never be revisited"
        );

        // The diff against the pruned base is exactly the surviving match's revision — a
        // Delete/Insert pair of its row (orow(2), extent 4 -> 5) — and nothing else; in particular
        // no op references the finalized start (whose base row was orow(0)).
        let new_emitted: Vec<(SeqMatch, OwnedRow)> = inc
            .provisional()
            .iter()
            .map(|m| (m.clone(), orow(m.start_seq.0)))
            .collect();
        let ops = diff_provisional(&base_pruned, &new_emitted);
        assert_eq!(ops, vec![(Op::Delete, orow(2)), (Op::Insert, orow(2))]);
    }

    /// The overlapping skip modes now rebase across a consumed straddling match instead of forcing a
    /// rebuild. With `SKIP TO NEXT ROW` the resume point precedes the match end (`resume == start + 1
    /// < end`), so a FROZEN match's span can extend past the frozen prefix. `pattern (a a)` over
    /// three qualifying rows: (0,2) freezes with a frozen prefix of 1 — position 0 is dead (the
    /// pattern is exactly two rows) — while (1,3) ends at the boundary and stays alive. The
    /// executor's eviction boundary is the first alive position (1, exactly the frozen prefix
    /// `next_pos`), and the frozen (0,2) straddles it: its start row (seq 0) is evicted, so (0,2) is
    /// consumed (final, already emitted) and dropped. Because the boundary sits at `next_pos`, no
    /// frozen match survives and `next_pos` rebases to 0, so the surviving suffix is re-derived from
    /// scratch — `provisional()` then equals a fresh scan over the survivors (checked here).
    #[tokio::test]
    async fn finalize_under_to_next_row_rebases_consuming_straddler() {
        let pat = Pattern::Concat(vec![Pattern::Var("a".into()), Pattern::Var("a".into())]);
        let nfa = Nfa::compile(&pat);
        let skip = SkipMode::ToNextRow;
        let rows = from_str("aaa");
        let matcher = SetMatcher::new(rows.clone());

        let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
        inc.advance(
            &ss(&[0, 1, 2]),
            &matcher,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();
        // Overlapping matches (0,2) and (1,3); only (0,2) froze, and the frozen prefix (its resume
        // point, 1) sits strictly inside its span [0, 2).
        assert_eq!(
            provisional_triples(&inc),
            vec![(0, 2, labels(&["a", "a"])), (1, 3, labels(&["a", "a"]))]
        );
        assert_eq!(inc.frozen(), 1);
        assert_eq!(inc.frozen_prefix_len(), 1);
        // The executor-shaped call: evict before the first alive position (seq 1 == next_pos). The
        // frozen (0,2) straddles the boundary but its start is evicted, so it is consumed and the
        // matcher rebases in place rather than declining.
        let removed = finalize_rebased(&mut inc, Seq(1));
        assert_eq!(seq_triples(&removed), vec![(0, 2, labels(&["a", "a"]))]);
        // Post-finalize: only the surviving match (1,3) remains, and it equals a fresh batch scan
        // over the surviving rows `full[1..]` (positions shifted up by the one evicted row).
        assert_eq!(provisional_triples(&inc), vec![(1, 3, labels(&["a", "a"]))]);
        let fresh: Vec<(usize, usize, Vec<String>)> = batch_triples(&nfa, &skip, &rows[1..])
            .await
            .into_iter()
            .map(|(s, e, ls)| (s + 1, e + 1, ls))
            .collect();
        assert_eq!(provisional_triples(&inc), fresh);
    }

    /// Dropping the matcher and rebuilding from the surviving rows — the [`Finalized::MustRebuild`]
    /// fallback the executor still takes when finalize declines (e.g. a whole-buffer drain, or a
    /// WITHIN-expired boundary past the frozen prefix) — remains oracle-correct: a FRESH matcher fed
    /// the surviving rows equals the batch answer over them, and its union with the evicted
    /// (already-emitted/finalized) match equals the batch answer over the full input. (The specific
    /// `TO NEXT ROW` shape here now *rebases* through the executor — see the test above — but the
    /// rebuild path this exercises is still reached on the declined shapes and must stay sound.)
    #[tokio::test]
    async fn to_next_row_drop_and_rebuild_across_eviction_equals_batch() {
        let pat = Pattern::Concat(vec![Pattern::Var("a".into()), Pattern::Var("a".into())]);
        let nfa = Nfa::compile(&pat);
        let skip = SkipMode::ToNextRow;

        // Matches (0,2) frozen, (1,3) boundary-held.
        let full = from_str("aaa");
        let m_full = SetMatcher::new(full.clone());
        let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
        inc.advance(
            &ss(&[0, 1, 2]),
            &m_full,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            provisional_triples(&inc),
            vec![(0, 2, labels(&["a", "a"])), (1, 3, labels(&["a", "a"]))]
        );
        // Model the drop-and-rebuild fallback directly: take the evicted match, drop the matcher,
        // and rebuild from the surviving rows — exactly what the executor does when finalize returns
        // `MustRebuild`. The evicted (0,2) was already delivered (emitted under EOWC; pruned from the
        // diff base without a retract under EOU).
        let evicted = provisional_triples(&inc)[0].clone();
        drop(inc);

        // Rebuild: a fresh matcher fed the surviving rows (seqs 1, 2), with the matcher indexing
        // the rebased surviving buffer `full[1..]` — exactly what the executor's next visit does.
        let m_tail = SetMatcher::new(full[1..].to_vec());
        let mut rebuilt = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
        rebuilt
            .advance(&ss(&[1, 2]), &m_tail, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();

        // Oracle over the surviving rows (batch positions shifted by the eviction offset to line up
        // with the surviving seqs).
        let shifted_batch: Vec<(usize, usize, Vec<String>)> =
            batch_triples(&nfa, &skip, &full[1..])
                .await
                .into_iter()
                .map(|(s, e, ls)| (s + 1, e + 1, ls))
                .collect();
        assert_eq!(provisional_triples(&rebuilt), shifted_batch);
        assert_eq!(
            provisional_triples(&rebuilt),
            vec![(1, 3, labels(&["a", "a"]))]
        );

        // Union of the evicted match and the rebuilt provisional set equals the batch answer over
        // the full input — nothing lost, nothing duplicated across the eviction.
        let mut union = vec![evicted];
        union.extend(provisional_triples(&rebuilt));
        assert_eq!(union, batch_triples(&nfa, &skip, &full).await);
    }

    /// The `refresh_matcher` over-feed rollback shape (see `executor.rs`): under emit-on-update the
    /// whole buffer is fed at a barrier, then the watermark's eviction visit narrows back to the
    /// safe prefix — `truncate_from_seq` at the first over-fed row rolls the tail back, but it also
    /// drops the provisional matches over the *retained* fed suffix, and with nothing left to
    /// re-feed no `advance` follows to re-derive them. `rescan` must restore the invariant:
    /// `provisional()` equals the batch answer over the safe prefix, labels included.
    #[tokio::test]
    async fn overfeed_rollback_rescan_equals_batch_over_safe_prefix() {
        let pat = Pattern::Concat(vec![
            Pattern::Var("a".into()),
            quant(Pattern::Var("b".into()), Quantifier::Plus, false),
        ]);
        let nfa = Nfa::compile(&pat);
        let skip = SkipMode::PastLastRow;

        // 0:a 1:b 2:a 3:b 4:b — whole-buffer matches (0,2) (frozen: the `a` at 2 breaks the greedy
        // `b+`) and (2,5) (trailing, provisional).
        let full = from_str("ababb");
        let m_full = SetMatcher::new(full.clone());
        let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
        inc.advance(
            &ss(&[0, 1, 2, 3, 4]),
            &m_full,
            &mut ScanBudget::unlimited(),
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            provisional_triples(&inc),
            vec![
                (0, 2, labels(&["a", "b"])),
                (2, 5, labels(&["a", "b", "b"]))
            ]
        );
        assert_eq!(inc.frozen(), 1);

        // Roll back to the safe prefix [0, 4): truncate at the first over-fed row's seq. This drops
        // the provisional (2,5) even though rows 2 and 3 stay fed — the reason a rescan (and not a
        // re-feed, which would double-enter the retained rows in `seq_index`) must follow.
        let m_safe = SetMatcher::new(full[..4].to_vec());
        inc.truncate_from_seq(Seq(4), &m_safe, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();
        assert_eq!(provisional_triples(&inc), vec![(0, 2, labels(&["a", "b"]))]);

        // Rescan re-derives the dropped suffix matches in place: `provisional()` now equals the
        // batch answer over the safe prefix — the invariant the executor's eviction pass reads.
        inc.rescan(&m_safe, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();
        assert_eq!(
            provisional_triples(&inc),
            batch_triples(&nfa, &skip, &full[..4]).await
        );
        assert_eq!(
            provisional_triples(&inc),
            vec![(0, 2, labels(&["a", "b"])), (2, 4, labels(&["a", "b"]))]
        );

        // Re-feeding the rolled-back row (the next barrier's whole-buffer feed) still equals the
        // batch over the whole buffer — the rollback+rescan corrupted nothing.
        inc.advance(&ss(&[4]), &m_full, &mut ScanBudget::unlimited(), false)
            .await
            .unwrap();
        assert_eq!(
            provisional_triples(&inc),
            batch_triples(&nfa, &skip, &full).await
        );
    }

    // ---- Randomized operation-sequence oracle (spec §8.1) ---------------------------------------
    //
    // The targeted tests above pin specific shapes; this property test closes the randomized-oracle
    // gap by driving *arbitrary* interleavings of the matcher's operations against the batch
    // reference. For each seed it draws a random pattern (over a small grammar), a random
    // `AFTER MATCH SKIP` mode (all four), random satisfied-set rows, and a random op sequence
    // (`advance` in in-order chunks incl. empty, `truncate_from_seq` at valid and never-fed seqs,
    // `finalize_evicted_prefix` at executor-reachable boundaries). After *every* op it asserts the
    // core invariant — `provisional()` (full `(start, end, labels)` triples) equals a from-scratch
    // batch `find_matches_dynamic` over the currently-live rows for the same skip mode. Seeds are
    // fixed (`0..N`), so CI is deterministic; everything is in-memory, so the sweep runs in seconds.

    /// Build a random pattern over `vars`, shrinking toward a leaf as `budget` decreases so the
    /// compiled NFA stays small (≤64 states → bitmask visited-set) and the batch rescans stay cheap.
    fn gen_pattern(rng: &mut SmallRng, vars: &[&str], budget: usize) -> Pattern {
        let pick_var =
            |rng: &mut SmallRng| Pattern::Var(vars[rng.random_range(0..vars.len())].into());
        // Out of budget → leaf; otherwise choose a construct.
        let choice = if budget == 0 {
            0
        } else {
            rng.random_range(0..4)
        };
        match choice {
            // Concatenation of 2–3 sub-patterns.
            1 => Pattern::Concat(
                (0..rng.random_range(2..=3))
                    .map(|_| gen_pattern(rng, vars, budget - 1))
                    .collect(),
            ),
            // Alternation of 2–3 sub-patterns.
            2 => Pattern::Alt(
                (0..rng.random_range(2..=3))
                    .map(|_| gen_pattern(rng, vars, budget - 1))
                    .collect(),
            ),
            // A quantified sub-pattern (greedy or reluctant), all four quantifier shapes.
            3 => {
                let inner = gen_pattern(rng, vars, budget - 1);
                let q = match rng.random_range(0..4) {
                    0 => Quantifier::Star,
                    1 => Quantifier::Plus,
                    2 => Quantifier::Question,
                    _ => {
                        let min = rng.random_range(0..=2);
                        let max = rng.random_bool(0.5).then(|| min + rng.random_range(0..=2));
                        Quantifier::Range { min, max }
                    }
                };
                Pattern::Quantified(Box::new(inner), q, rng.random_bool(0.5))
            }
            // Leaf variable.
            _ => pick_var(rng),
        }
    }

    /// A random `AFTER MATCH SKIP` mode; the variable-targeted modes bind a symbol from `vars` (a
    /// valid symbol name — `next_pos` degrades to `PAST LAST ROW` if that symbol is absent from a
    /// given match, which is itself a shape worth exercising).
    fn gen_skip(rng: &mut SmallRng, vars: &[&str]) -> SkipMode {
        match rng.random_range(0..4) {
            0 => SkipMode::PastLastRow,
            1 => SkipMode::ToNextRow,
            2 => SkipMode::ToFirst(vars[rng.random_range(0..vars.len())].into()),
            _ => SkipMode::ToLast(vars[rng.random_range(0..vars.len())].into()),
        }
    }

    /// The core oracle assertion: the incremental matcher's provisional set equals a from-scratch
    /// batch scan over the currently-live rows `full_rows[evicted..fed]`. Seqs equal original
    /// positions, so a live match's seq-anchored triple is its batch (position-anchored) triple
    /// shifted up by the evicted prefix length.
    async fn assert_matches_batch(
        inc: &IncrementalMatcher,
        nfa: &Nfa,
        skip: &SkipMode,
        full_rows: &[BTreeSet<String>],
        evicted: usize,
        fed: usize,
        ctx: &str,
    ) {
        let batch: Vec<(usize, usize, Vec<String>)> =
            batch_triples(nfa, skip, &full_rows[evicted..fed])
                .await
                .into_iter()
                .map(|(s, e, ls)| (s + evicted, e + evicted, ls))
                .collect();
        assert_eq!(provisional_triples(inc), batch, "oracle divergence {ctx}");
    }

    /// The same operation sweep under a STARVED budget. Two properties, neither of which the
    /// unlimited oracle can reach:
    ///
    /// 1. A truncated matcher is INCOMPLETE, never wrong — its provisional set is a leftmost
    ///    *prefix* of the batch answer. The scan pulls matches in preference order and a budget
    ///    abort inside a higher-preference subtree propagates out rather than falling through to a
    ///    lower-preference alternative, so starvation can drop a suffix but can never fabricate a
    ///    match, reorder two, or steal a row into a different variable.
    /// 2. Starvation is always RECOVERABLE: one re-derive with budget restores exact equality.
    ///    This is precisely the contract the executor's watermark arm relies on when it refreshes a
    ///    partition before deciding anything, and nothing else tests it.
    #[tokio::test]
    async fn randomized_operation_sequence_oracle_under_a_starved_budget() {
        const SEEDS: u64 = 200;
        const OPS: usize = 30;
        let var_pool: [&[&str]; 3] = [&["a", "b"], &["a", "b", "c"], &["a", "b", "c", "d"]];
        // Guards against the test passing for the wrong reason: budgets small enough to matter must
        // actually be exhausted, and a truncated state must actually differ from the batch answer at
        // least sometimes. Without these a future change to the budget sizing could silently turn
        // this into the unlimited oracle run twice.
        let mut starved_ops = 0usize;
        let mut strictly_shorter = 0usize;

        for seed in 0..SEEDS {
            let mut rng = SmallRng::seed_from_u64(seed ^ 0x5741_2764_u64);
            let vars = var_pool[rng.random_range(0..var_pool.len())];
            let pattern = gen_pattern(&mut rng, vars, 3);
            let nfa = Nfa::compile(&pattern);
            let skip = gen_skip(&mut rng, vars);

            let n_rows = rng.random_range(3..=7);
            let full_rows: Vec<BTreeSet<String>> = (0..n_rows)
                .map(|_| {
                    vars.iter()
                        .filter(|_| rng.random_bool(0.6))
                        .map(|v| (*v).to_owned())
                        .collect()
                })
                .collect();

            let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
            let mut fed = 0usize;
            let evicted = 0usize;

            for op in 0..OPS {
                let matcher = SetMatcher::new(full_rows[evicted..].to_vec());
                let ctx = format!("seed {seed} op {op} (starved)");
                // 0 is included deliberately: a scan that dies on entry, having decided nothing.
                let mut budget = ScanBudget::new(rng.random_range(0..=8));

                match rng.random_range(0..2) {
                    0 => {
                        let remaining = full_rows.len() - fed;
                        let chunk = if remaining == 0 {
                            0
                        } else {
                            rng.random_range(0..=remaining.min(3))
                        };
                        let seqs: Vec<Seq> = (fed..fed + chunk).map(|i| Seq(i as i64)).collect();
                        inc.advance(&seqs, &matcher, &mut budget, false)
                            .await
                            .unwrap();
                        fed += chunk;
                    }
                    _ => {
                        let k = rng.random_range(0..=fed + 2);
                        inc.truncate_from_seq(Seq(k as i64), &matcher, &mut budget, false)
                            .await
                            .unwrap();
                        if (evicted..fed).contains(&k) {
                            fed = k;
                        }
                        inc.rescan(&matcher, &mut budget, false).await.unwrap();
                    }
                }

                // (1) prefix, not equality.
                let batch: Vec<(usize, usize, Vec<String>)> =
                    batch_triples(&nfa, &skip, &full_rows[evicted..fed]).await;
                let starved = provisional_triples(&inc);
                if budget.hit {
                    starved_ops += 1;
                }
                if starved.len() < batch.len() {
                    strictly_shorter += 1;
                }
                assert!(
                    batch.starts_with(&starved),
                    "a starved matcher must hold a PREFIX of the batch answer {ctx}\n  starved: {starved:?}\n  batch:   {batch:?}"
                );

                // (2) one budgeted re-derive restores the exact invariant.
                inc.refresh(&matcher, &mut ScanBudget::unlimited(), false)
                    .await
                    .unwrap();
                assert_matches_batch(&inc, &nfa, &skip, &full_rows, evicted, fed, &ctx).await;
            }
        }

        assert!(
            starved_ops > 0,
            "no operation exhausted its budget — the sweep never reached the truncation paths it \
             exists to cover"
        );
        assert!(
            strictly_shorter > 0,
            "every starved op still matched the batch answer exactly — truncation never actually \
             withheld a match, so the prefix property was asserted vacuously"
        );
    }

    /// A chain pattern with a cycle behind it (`a{600} b*`), so the acyclic shortcut does not
    /// apply and freezing a match of `L` rows really runs one liveness walk per position of its
    /// region, each walking up to `L` rows — Θ(L²) steps, which exceeds one visit's budget
    /// (600 × ~1800 > 2^20). The freeze must carry its proven-dead prefix across visits so a
    /// bounded number of refreshes converges; restarting it at `next_pos` every visit never would
    /// (the budget died at the same position each time, and the region never froze — the
    /// permanent, non-self-healing degradation the depth cap used to cause by other means).
    #[tokio::test]
    async fn freeze_resumes_across_budget_exhausted_visits() {
        const N: usize = 600;
        const ROWS: usize = 1300;
        const BUDGET: usize = 1 << 20;
        let a_n = Pattern::Quantified(
            Box::new(Pattern::Var("a".into())),
            Quantifier::Range {
                min: N as u32,
                max: Some(N as u32),
            },
            false,
        );
        let b_star =
            Pattern::Quantified(Box::new(Pattern::Var("b".into())), Quantifier::Star, false);
        let nfa = Nfa::compile(&Pattern::Concat(vec![a_n, b_star]));
        assert_eq!(
            nfa.max_match_rows(),
            None,
            "the test needs a cyclic automaton"
        );
        let matcher = SetMatcher::new(vec![BTreeSet::from(["a".to_owned()]); ROWS]);
        let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa), SkipMode::PastLastRow);

        let seqs: Vec<Seq> = (0..ROWS as i64).map(Seq).collect();
        let mut budget = ScanBudget::new(BUDGET);
        inc.advance(&seqs, &matcher, &mut budget, true)
            .await
            .unwrap();
        assert!(
            budget.hit,
            "the test needs a freeze that outruns one visit's budget"
        );
        assert_eq!(inc.frozen(), 0, "the first region did not finish freezing");
        assert!(
            !inc.is_incomplete(),
            "the tail scan itself completed; only the freeze was cut short"
        );
        assert!(inc.needs_refresh(), "a truncated freeze asks for a refresh");
        let proven_after_first_visit = inc.dead_prefix_end();
        assert!(
            proven_after_first_visit > 0,
            "the first visit proved nothing dead"
        );

        let mut visits = 1;
        while inc.needs_refresh() {
            assert!(visits < 5, "the freeze did not converge in {visits} visits");
            budget = ScanBudget::new(BUDGET);
            inc.refresh(&matcher, &mut budget, true).await.unwrap();
            visits += 1;
        }
        assert!(visits >= 2, "convergence must have needed the resume");
        assert!(inc.dead_prefix_end() > proven_after_first_visit);

        // (0,600) froze: every position of its region is dead. (600,1200) is provisional and
        // stays so while every row is an `a`: from 700 on, `a{600}` reaches the boundary before
        // it can accept — alive, not frozen — so the proven-dead prefix ends exactly there.
        assert_eq!(inc.frozen(), 1);
        assert_eq!(inc.resume_pos(), N);
        assert_eq!(inc.dead_prefix_end(), 700);
        let spans: Vec<(i64, i64)> = inc
            .provisional()
            .iter()
            .map(|m| (m.start_seq.0, m.end_seq.0))
            .collect();
        assert_eq!(spans, vec![(0, 600), (600, 1200)]);
    }

    /// A run one row short of a match, then a non-matching row: under `a{1000}` (the binder's
    /// limit) every start in the run walks to the break and dies, Θ(r²) per rescan — for r = 999
    /// that is ~1.5M steps, more than one visit's budget. The finder must remember the starts it
    /// proved matchless so the next rescan begins past them; without that memory every visit
    /// re-walks the same dead starts, exhausts at the same place, and the partition never
    /// completes a rescan again.
    #[tokio::test]
    async fn finder_resumes_past_starts_proven_matchless() {
        const N: usize = 1000;
        const RUN: usize = N - 1;
        const BUDGET: usize = 1 << 20;
        let nfa = Nfa::compile(&Pattern::Quantified(
            Box::new(Pattern::Var("a".into())),
            Quantifier::Range {
                min: N as u32,
                max: Some(N as u32),
            },
            false,
        ));
        let a = BTreeSet::from(["a".to_owned()]);
        let x = BTreeSet::from(["x".to_owned()]);
        let rows: Vec<BTreeSet<String>> = std::iter::repeat_n(a.clone(), RUN)
            .chain(std::iter::once(x))
            .chain(std::iter::repeat_n(a, N))
            .collect();
        let n_rows = rows.len();
        let matcher = SetMatcher::new(rows);
        let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa), SkipMode::PastLastRow);

        let seqs: Vec<Seq> = (0..n_rows as i64).map(Seq).collect();
        let mut budget = ScanBudget::new(BUDGET);
        inc.advance(&seqs, &matcher, &mut budget, true)
            .await
            .unwrap();
        assert!(
            budget.hit,
            "the test needs a rescan that outruns one visit's budget"
        );
        assert!(inc.is_incomplete());
        assert!(
            inc.provisional().is_empty(),
            "the finder never got past the break"
        );
        let proven_after_first_visit = inc.dead_prefix_end();
        assert!(
            proven_after_first_visit > 0,
            "the truncated rescan must still have proved a prefix of starts matchless"
        );

        let mut visits = 1;
        while inc.needs_refresh() {
            assert!(visits < 4, "the rescan did not converge in {visits} visits");
            budget = ScanBudget::new(BUDGET);
            inc.refresh(&matcher, &mut budget, true).await.unwrap();
            visits += 1;
        }
        assert_eq!(visits, 2, "one resume should finish the run");

        // The match after the break is found; everything before it is proven dead (the run and
        // the break row are matchless), and the acyclic shortcut proves the match's own region
        // dead up to the first position that can still reach the boundary.
        let spans: Vec<(i64, i64)> = inc
            .provisional()
            .iter()
            .map(|m| (m.start_seq.0, m.end_seq.0))
            .collect();
        assert_eq!(spans, vec![((RUN + 1) as i64, (RUN + 1 + N) as i64)]);
        assert_eq!(inc.dead_prefix_end(), n_rows - N);
        assert_eq!(
            inc.frozen(),
            0,
            "positions from {} on are alive",
            n_rows - N
        );
        assert!(!inc.needs_refresh());
    }

    /// A successful match advances `MatchScan::next_start` without advancing `matchless_upto`.
    /// With overlapping matches, a visit can therefore spend its whole budget after returning a
    /// long prefix while proving no start matchless. Refreshes must retain that prefix and resume
    /// at the saved scan cursor; restarting at `matchless_upto == 0` repeats the same matches
    /// forever and leaves the partition permanently incomplete.
    #[tokio::test]
    async fn finder_resumes_after_budget_truncated_overlapping_matches() {
        const RUN: usize = 80;
        const BUDGET: usize = 1024;
        let nfa = Nfa::compile(&quant(Pattern::Var("a".into()), Quantifier::Plus, false));
        let rows: Vec<BTreeSet<String>> =
            std::iter::repeat_n(BTreeSet::from(["a".to_owned()]), RUN)
                .chain(std::iter::once(BTreeSet::from(["x".to_owned()])))
                .collect();
        let matcher = SetMatcher::new(rows);
        let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa), SkipMode::ToNextRow);
        let seqs: Vec<Seq> = (0..=RUN as i64).map(Seq).collect();

        let mut budget = ScanBudget::new(BUDGET);
        inc.advance(&seqs, &matcher, &mut budget, true)
            .await
            .unwrap();
        assert!(budget.hit, "the first scan must be truncated");
        assert!(inc.is_incomplete());
        let first_prefix_len = inc.provisional().len();
        assert!(
            first_prefix_len > 0 && first_prefix_len < RUN,
            "the first visit must find a strict non-empty prefix"
        );

        let mut visits = 1;
        while inc.needs_refresh() {
            assert!(visits < 32, "the scan/freeze did not converge");
            budget = ScanBudget::new(BUDGET);
            inc.refresh(&matcher, &mut budget, true).await.unwrap();
            visits += 1;
        }

        assert!(visits > 1, "the test must exercise cursor resumption");
        assert_eq!(inc.provisional().len(), RUN);
        assert_eq!(inc.frozen(), RUN);
        assert_eq!(inc.resume_pos(), RUN);
    }

    #[tokio::test]
    async fn randomized_operation_sequence_oracle() {
        // ~200 seeds × ~30 ops. All in-memory over ≤7-row buffers, so the whole sweep is a few ms.
        const SEEDS: u64 = 200;
        const OPS: usize = 30;
        let var_pool: [&[&str]; 3] = [&["a", "b"], &["a", "b", "c"], &["a", "b", "c", "d"]];

        for seed in 0..SEEDS {
            let mut rng = SmallRng::seed_from_u64(seed);
            let vars = var_pool[rng.random_range(0..var_pool.len())];
            let pattern = gen_pattern(&mut rng, vars, 3);
            let nfa = Nfa::compile(&pattern);
            let skip = gen_skip(&mut rng, vars);

            // Random satisfied-set rows: each var present with prob ~0.6 (empty rows allowed).
            let n_rows = rng.random_range(3..=7);
            let full_rows: Vec<BTreeSet<String>> = (0..n_rows)
                .map(|_| {
                    vars.iter()
                        .filter(|_| rng.random_bool(0.6))
                        .map(|v| (*v).to_owned())
                        .collect()
                })
                .collect();

            let mut inc = IncrementalMatcher::new(std::sync::Arc::new(nfa.clone()), skip.clone());
            let mut fed = 0usize; // rows fed so far (== next seq to mint, since seq == position)
            let mut evicted = 0usize; // rows finalized off the front of the live buffer

            for op in 0..OPS {
                // The candidate matcher over the currently-live rows (positions are 0-based from the
                // evicted boundary, exactly as the executor's post-eviction buffer is).
                let matcher = SetMatcher::new(full_rows[evicted..].to_vec());
                let n_live = fed - evicted;
                let ctx = format!("seed {seed} op {op}");

                match rng.random_range(0..3) {
                    // advance: feed the next in-order chunk (possibly empty).
                    0 => {
                        let remaining = full_rows.len() - fed;
                        let chunk = if remaining == 0 {
                            0
                        } else {
                            rng.random_range(0..=remaining.min(3))
                        };
                        let seqs: Vec<Seq> = (fed..fed + chunk).map(|i| Seq(i as i64)).collect();
                        inc.advance(&seqs, &matcher, &mut ScanBudget::unlimited(), false)
                            .await
                            .unwrap();
                        fed += chunk;
                    }
                    // truncate + rescan: roll back at a random seq (valid fed, already-evicted, or
                    // never-fed), then rescan to re-derive the retained tail in place — the executor's
                    // out-of-order / over-feed rollback shape. A never-fed seq is a no-op.
                    1 => {
                        let lo = evicted.saturating_sub(1);
                        let hi = fed + 2;
                        let k = rng.random_range(lo..=hi);
                        inc.truncate_from_seq(
                            Seq(k as i64),
                            &matcher,
                            &mut ScanBudget::unlimited(),
                            false,
                        )
                        .await
                        .unwrap();
                        if (evicted..fed).contains(&k) {
                            fed = k; // rolled the live buffer back to the truncation point
                        }
                        inc.rescan(&matcher, &mut ScanBudget::unlimited(), false)
                            .await
                            .unwrap();
                    }
                    // finalize: mirror the executor's eviction gate. Retain from the first row that is
                    // still live at the safe boundary; evict the dead prefix before it. Only attempt
                    // when there is a dead prefix and a surviving suffix (`0 < retain_from < n_live`).
                    _ => {
                        if n_live >= 2 {
                            let mut retain_from = n_live;
                            for p in 0..n_live {
                                if nfa
                                    .reaches_boundary_alive(
                                        p,
                                        n_live,
                                        &matcher,
                                        &mut ScanBudget::unlimited(),
                                        false,
                                    )
                                    .await
                                    .unwrap()
                                {
                                    retain_from = p;
                                    break;
                                }
                            }
                            if retain_from > 0 && retain_from < n_live {
                                let boundary = Seq((evicted + retain_from) as i64);
                                match inc.finalize_evicted_prefix(boundary) {
                                    Finalized::Rebased => evicted += retain_from,
                                    // The executor drops the matcher and lets the next visit rebuild
                                    // lazily; model that with a fresh matcher fed the surviving rows,
                                    // then continue the sequence.
                                    Finalized::MustRebuild => {
                                        evicted += retain_from;
                                        inc = IncrementalMatcher::new(
                                            std::sync::Arc::new(nfa.clone()),
                                            skip.clone(),
                                        );
                                        let surv = SetMatcher::new(full_rows[evicted..].to_vec());
                                        let seqs: Vec<Seq> =
                                            (evicted..fed).map(|i| Seq(i as i64)).collect();
                                        inc.advance(
                                            &seqs,
                                            &surv,
                                            &mut ScanBudget::unlimited(),
                                            false,
                                        )
                                        .await
                                        .unwrap();
                                    }
                                }
                            }
                        }
                    }
                }

                // Invariant after EVERY op.
                assert_matches_batch(&inc, &nfa, &skip, &full_rows, evicted, fed, &ctx).await;
            }
        }
    }
}
