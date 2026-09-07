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

//! V3 iceberg sink 2PC fault injection tests.
//!
//! ## Relationship with `sink/exactly_once`
//!
//! `sink/exactly_once` tests whether the *V1/V2-era* RisingWave framework
//! (log store, sink decouple, barrier) can drive a **fake**
//! `TwoPhaseCommitCoordinator`. The coordinator itself is a mock, and iceberg
//! is simulated via a `BTreeMap` inside that mock.
//!
//! V3 introduces a meta-side `IcebergV3CoordinatorWorker` (a real 2PC state
//! machine with `pending_sink_state` persistence and recovery replay), and
//! the tests here exercise that **real state machine**. Therefore:
//!   - The mock boundary drops down to `iceberg::Catalog` (instead of the
//!     coordinator).
//!   - Fault injection points correspond to actual code paths (not the five
//!     conventions internal to the V1/V2 mock).
//!   - Tests must really kill/restart meta to trigger the `recovery()` path
//!     (which `exactly_once` never does).
//!
//! ## Event trace legend
//!
//! Single chars appended to `MockIcebergV3Catalog::err_events`. The mock
//! only sees the `iceberg::Catalog` boundary, so the trace only reflects
//! catalog-level activity plus the kill marker pushed by the test driver.
//! Pre-commit, commit-and-prune, and recovery internal states are **not**
//! currently observable in the trace.
//!
//! | Char | Source                  | Meaning                                            |
//! |------|-------------------------|----------------------------------------------------|
//! | `I`  | mock `update_table`     | iceberg `overwrite_files` succeeded                |
//! | `i`  | mock `update_table`     | iceberg commit failed (F11 mock injection)         |
//! | `r`  | mock `update_table`     | duplicate `snapshot_id` reached the catalog        |
//! | `K`  | test (`push_kill_event`)| meta `kill_nodes_and_restart` triggered            |
//!
//! `'r'` is a **regression alarm only** — under correct V3 behavior the
//! `metadata().snapshots()` guard in `coordinator_worker.rs` (backed by
//! iceberg-rust's `TableMetadataBuilder::add_snapshot` local check) prevents
//! any duplicate `update_table` call from ever reaching the catalog.
//!
//! Fault-injection sites:
//! - `iceberg_v3_persist_pre_commit_fail`  (F5; via `fail::cfg`)
//! - `iceberg_v3_commit_prune_fail`        (F12; via `fail::cfg`)
//! - `iceberg_v3_recovery_fail`            (recovery panic; via `fail::cfg`)
//! - `MockIcebergV3Catalog::set_err_rate_txn_commit` (F11; mock catalog state)
//!
//! ## Test cases
//!
//! Every test runs through `assert_invariants` at the end: no duplicate file
//! paths across snapshots, `r_count <= i_count`, and `pending_sink_state`
//! fully drained for the sink. Per-test notes below only call out additional
//! assertions and what fault is exercised.
//!
//! ### 1. `failpoint_limited_test_v3_basic_no_failure`
//!
//! No injection.
//!
//! - **Expected trace:** `III...` (only `'I'`)
//! - **Asserts:**
//!   - at least 3 commits land (multi-snapshot path exercised)
//!   - no lowercase / `'K'` / `'r'` events appear
//!
//! ### 2. `failpoint_limited_test_v3_persist_failure`
//!
//! F5 (`persist_pre_commit_fail`) at 30% for ~10s.
//!
//! - **Expected trace:** `I...I` plus invisible F5 firings
//! - **Asserts:**
//!   - a fresh commit lands strictly after the fault clears
//!
//! ### 3. `failpoint_limited_test_v3_iceberg_commit_failure`
//!
//! F11 (mock `txn_commit`) held at 100% until one `'i'` is observed, then
//! 30% for ~10s.
//!
//! - **Expected trace:** `I...i...I...I` (at least one `'i'` deterministically)
//! - **Asserts:**
//!   - a fresh commit lands strictly after the fault clears
//!
//! ### 4. `failpoint_limited_test_v3_commit_prune_failure`
//!
//! F12 (`commit_prune_fail`) at 30% for ~10s. This is the iceberg-committed-
//! but-prune-failed race window. V3 must redrive `handle_commit` on recovery,
//! hit the idempotency check, and eventually mark the row Committed once the
//! fault clears.
//!
//! - **Expected trace:** `I...I` plus invisible F12 firings
//! - **Asserts:**
//!   - a fresh commit lands strictly after the fault clears
//!
//! ### 5. `failpoint_limited_test_v3_idempotent_on_retry`
//!
//! F12 at 100% for ~10s. Pinned high so the very first commit attempt has
//! its iceberg snapshot persisted but its DB row left Pending; every retry
//! must short-circuit on V3's `metadata().snapshots()` guard before reaching
//! the catalog.
//!
//! - **Expected trace:** `I` (single new `'I'` from the first attempt, then
//!   silence; no `'r'`)
//! - **Asserts:**
//!   - `count_events('r') == 0` (regression alarm: V3 must not call
//!     `update_table` with a duplicate `snapshot_id`)
//!   - a fresh commit lands once the fault clears
//!
//! ### 6. `failpoint_limited_test_v3_random_failures_corner_case`
//!
//! F5 + F12 at 5%, F11 pre-gated at 100% until one `'i'` lands, then 5% —
//! for ~15s total. The pre-gating makes "every fault type fired at least
//! once" independent of seed.
//!
//! - **Expected trace:** at least one `'i'` and several `'I'`
//! - **Asserts:**
//!   - `count_events('r') == 0` (idempotency guard holds under compound
//!     faults)
//!   - a fresh commit lands after the window
//!
//! ### 7. `failpoint_limited_test_v3_meta_kill_during_pre_commit`
//!
//! F5 at 100% + best-effort `sleep(3)` so the coordinator is plausibly
//! stuck mid-pre-commit, then kill + restart meta.
//!
//! - **Expected trace:** `K...I` (no `'I'` before the kill since F5 blocked
//!   pre-commit; one or more `'I'` after recovery + fault clear)
//! - **Asserts:**
//!   - at least one `'I'` lands strictly after the kill marker
//!
//! ### 8. `failpoint_limited_test_v3_meta_kill_during_commit`
//!
//! Wait for baseline `'I'`, then F12 at 100% + `sleep(3)` + kill so the kill
//! lands while iceberg is committed but the DB row is still Pending.
//!
//! - **Expected trace:** `I...K` while F12 holds; `I` resumes only after F12
//!   clears
//! - **Asserts:**
//!   - `count_iceberg_writes_after_kill() == 0` while F12 holds (recovered
//!     worker took the idempotent fast-path)
//!   - `count_events('r') == 0` (regression alarm)
//!   - a fresh commit lands after F12 clears
//!
//! ### 9. `failpoint_limited_test_v3_meta_kill_repeatedly`
//!
//! Six kill+restart cycles spaced 10s apart.
//!
//! - **Expected trace:** `I...K...K...K...K...K...K...I`
//! - **Asserts:**
//!   - sink commits at least one fresh snapshot after the kills stop
//!
//! ### 10. `failpoint_limited_test_v3_recovery_during_recovery`
//!
//! Arm `iceberg_v3_recovery_fail` with `1*panic` so the first post-restart
//! `recovery()` invocation panics. Kill meta once; the supervisor must
//! restart the affected component and the second recovery attempt must
//! succeed. The final convergence wait also acts as a deadlock canary.
//!
//! - **Expected trace:** `I...K...I`
//! - **Asserts:**
//!   - a fresh commit lands after the second recovery attempt
//!
//! ## Known coverage gaps
//!
//! The current suite is strictly weaker than a "full 2PC fault matrix"
//! spec would imply. Notable gaps, all of which would require new event
//! emission hooks or a row-level data comparison helper to close:
//!
//! - **No source-vs-sink data comparison.** Tests only count commit events;
//!   they do not verify that every row inserted upstream actually landed in
//!   the iceberg snapshots.
//! - **F5 and F12 firings are not directly asserted.** Their effect is
//!   verified only indirectly (recovery progress + invariants).
//! - **No `'P'` / `'C'` / `'R'` events.** Tests that want to "kill during
//!   pre-commit" or "kill exactly between commit and prune" rely on fixed
//!   `sleep` calls; the kill timing is best-effort rather than gated on
//!   an observable V3 state transition.

mod mock_catalog;
mod utils;

// Tests live in `mod failpoint_limited` so their fully-qualified nextest names
// contain the substring `failpoint_limited::`, which the `.config/nextest.toml`
// `test(failpoint_limited::)` filter uses to place them in the single-threaded
// `failpoint-limited` group. This is required because the tests share global
// state: `fail::cfg` is process-wide and `MockCatalogGuard::register` panics
// on double-register.
mod failpoint_limited {
    use std::time::Duration;

    use anyhow::Result;

    use super::utils::{
        WorkloadConfig, assert_invariants, spawn_continuous_workload,
        start_v3_test_cluster_with_sink,
    };

    /// No-fault sanity: continuous workload (mixed insert/upsert/delete)
    /// streams into the TABLE, V3 sink must commit at least one snapshot
    /// and remain consistent.
    #[tokio::test]
    async fn failpoint_limited_test_v3_basic_no_failure() -> Result<()> {
        let mut handle = start_v3_test_cluster_with_sink(4).await?;

        let workload = spawn_continuous_workload(&mut handle.cluster, WorkloadConfig::default());

        // Exercise the multi-snapshot path by waiting for 3 commits.
        tokio::time::timeout(
            Duration::from_secs(30),
            handle.mock.wait_for_event_count('I', 3),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "fewer than 3 commits in 30s under continuous workload; trace = {:?}",
                handle.mock.get_event_trace()
            )
        })??;
        workload.shutdown().await;

        assert_invariants(&mut handle).await?;

        // No-failure path: every recorded event should be 'I' (success).
        // No lowercase events (failures), no 'K' (kill), no 'r' (retry).
        let trace = handle.mock.get_event_trace();
        assert!(
            trace.chars().all(|c| c == 'I'),
            "no-failure path produced unexpected events: {}",
            trace
        );
        Ok(())
    }

    /// `persist_pre_commit_metadata` fails with 30% probability during a
    /// fault window. The V3 sink must recover (via barrier recovery + V3
    /// `recovery()`) and continue committing snapshots once the fault is
    /// cleared. Data must remain consistent (no duplicate file paths).
    #[tokio::test]
    async fn failpoint_limited_test_v3_persist_failure() -> Result<()> {
        let mut handle = start_v3_test_cluster_with_sink(4).await?;

        let workload = spawn_continuous_workload(
            &mut handle.cluster,
            WorkloadConfig {
                seed: 1,
                ..WorkloadConfig::default()
            },
        );

        // Establish a baseline: at least one commit happens before fault injection.
        tokio::time::timeout(Duration::from_secs(30), handle.mock.wait_for_event('I'))
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "baseline 'I' event never observed before fault injection; trace = {:?}",
                    handle.mock.get_event_trace()
                )
            })??;
        let baseline_i_count = handle.mock.count_events('I');

        // Inject persist failure for ~10s.
        fail::cfg("iceberg_v3_persist_pre_commit_fail", "30%return").unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
        fail::remove("iceberg_v3_persist_pre_commit_fail");

        // Give the cluster time to recover and commit again past the fault
        // window. A persist failure fails the barrier and drives a full meta
        // recovery (bootstrap + recovery backoff up to 5s/attempt). Once the
        // fault clears no further commit can fail, so this wait only ever
        // drains the single in-flight recovery cascade; a seed-sweep over the
        // 30 CI seeds measured that cascade at ~30s worst case. Budget 120s
        // (~4x) so the test cannot boundary-flake (30s was provably too tight).
        tokio::time::timeout(
            Duration::from_secs(120),
            handle.mock.wait_for_event_count('I', baseline_i_count + 1),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "no new 'I' commits after fault cleared; baseline={}, current={}, trace = {:?}",
                baseline_i_count,
                handle.mock.count_events('I'),
                handle.mock.get_event_trace()
            )
        })??;

        workload.shutdown().await;

        assert_invariants(&mut handle).await?;
        Ok(())
    }

    /// iceberg `update_table` (i.e. `Transaction::commit`) fails with 30%
    /// probability during a fault window. V3's internal commit-retry path
    /// must reissue the commit; once the fault clears, snapshots resume.
    #[tokio::test]
    async fn failpoint_limited_test_v3_iceberg_commit_failure() -> Result<()> {
        let mut handle = start_v3_test_cluster_with_sink(4).await?;

        let workload = spawn_continuous_workload(
            &mut handle.cluster,
            WorkloadConfig {
                seed: 2,
                ..WorkloadConfig::default()
            },
        );

        // Baseline.
        tokio::time::timeout(Duration::from_secs(30), handle.mock.wait_for_event('I'))
            .await
            .map_err(|_| anyhow::anyhow!("baseline 'I' never observed"))??;

        // Inject iceberg-side commit conflict via the mock catalog. Pin the
        // rate at 100% until one failure has landed so the fault window is
        // independent of barrier scheduling, then run at 30% for ~10s.
        handle.mock.set_err_rate_txn_commit(1.0);
        tokio::time::timeout(
            Duration::from_secs(30),
            handle.mock.wait_for_event_count('i', 1),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "F11 never fired even at 100%; trace = {:?}",
                handle.mock.get_event_trace()
            )
        })??;
        handle.mock.set_err_rate_txn_commit(0.3);
        tokio::time::sleep(Duration::from_secs(10)).await;
        handle.mock.set_err_rate_txn_commit(0.0);
        let post_fault_i_count = handle.mock.count_events('I');

        // Sink resumes committing once the fault clears. F11 pinned at 100%
        // guarantees at least one retry-exhausted commit, which fails the
        // barrier and drives a full meta recovery (bootstrap + recovery backoff
        // up to 5s/attempt). Once the rate is 0 no further commit can fail, so
        // this wait only drains the single in-flight cascade. A seed-sweep over
        // the 30 CI seeds measured that cascade at ~30s worst case (this test,
        // seed 26); seed 4 fails at the old 30s budget as a boundary flake.
        // Budget 120s (~4x) so it cannot recur.
        tokio::time::timeout(
            Duration::from_secs(120),
            handle
                .mock
                .wait_for_event_count('I', post_fault_i_count + 1),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "no new 'I' commits after fault cleared; baseline={}, current={}, trace = {:?}",
                post_fault_i_count,
                handle.mock.count_events('I'),
                handle.mock.get_event_trace()
            )
        })??;

        workload.shutdown().await;
        assert_invariants(&mut handle).await?;
        Ok(())
    }

    /// `commit_and_prune_epoch` fails with 30% probability during a
    /// fault window. This is the most dangerous race window — iceberg has
    /// already accepted the snapshot, but the meta-side DB row stays Pending.
    /// V3 must redrive `handle_commit` on recovery, hit iceberg's idempotency
    /// check, and eventually mark the row Committed once the fault clears.
    #[tokio::test]
    async fn failpoint_limited_test_v3_commit_prune_failure() -> Result<()> {
        let mut handle = start_v3_test_cluster_with_sink(4).await?;

        let workload = spawn_continuous_workload(
            &mut handle.cluster,
            WorkloadConfig {
                seed: 3,
                ..WorkloadConfig::default()
            },
        );

        tokio::time::timeout(Duration::from_secs(30), handle.mock.wait_for_event('I'))
            .await
            .map_err(|_| anyhow::anyhow!("baseline 'I' never observed"))??;
        let baseline_i_count = handle.mock.count_events('I');

        fail::cfg("iceberg_v3_commit_prune_fail", "30%return").unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
        fail::remove("iceberg_v3_commit_prune_fail");

        // Sink should eventually commit fresh snapshots once the fault clears.
        // A prune failure leaves the row Pending and is redriven through a full
        // meta recovery cascade (~30s worst case across the 30 CI seeds). The
        // post-clear wait only drains the single in-flight cascade, so budget
        // 120s (~4x) for ample margin.
        tokio::time::timeout(
            Duration::from_secs(120),
            handle.mock.wait_for_event_count('I', baseline_i_count + 1),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "no new 'I' commits after fault cleared; baseline={}, current={}, trace = {:?}",
                baseline_i_count,
                handle.mock.count_events('I'),
                handle.mock.get_event_trace()
            )
        })??;

        workload.shutdown().await;
        assert_invariants(&mut handle).await?;
        Ok(())
    }

    /// `commit_and_prune_epoch` fails 100% of the time during a fault window.
    /// V3 must NOT re-issue iceberg `update_table` for snapshots it has already
    /// persisted: the `metadata().snapshots()` idempotency check at
    /// `coordinator_worker.rs` short-circuits the retry before any catalog call.
    #[tokio::test]
    async fn failpoint_limited_test_v3_idempotent_on_retry() -> Result<()> {
        let mut handle = start_v3_test_cluster_with_sink(4).await?;

        let workload = spawn_continuous_workload(
            &mut handle.cluster,
            WorkloadConfig {
                seed: 4,
                ..WorkloadConfig::default()
            },
        );

        // Baseline: at least one snapshot lands cleanly.
        tokio::time::timeout(Duration::from_secs(30), handle.mock.wait_for_event('I'))
            .await
            .map_err(|_| anyhow::anyhow!("baseline 'I' never observed"))??;
        let baseline_i_count = handle.mock.count_events('I');

        // First attempt writes the snapshot to iceberg ('I'); the DB row
        // then stays Pending forever while the fault holds. Every retry must
        // short-circuit on V3's `metadata().snapshots()` guard.
        fail::cfg("iceberg_v3_commit_prune_fail", "return").unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;

        // `catalog.update_table` must never see a duplicate snapshot_id: V3's
        // guard returns early, and iceberg-rust's `add_snapshot` would
        // otherwise reject the duplicate locally. A non-zero `'r'` would mean
        // V3 regressed into redundant — and in real catalogs potentially
        // destructive — duplicate commit attempts.
        assert_eq!(
            handle.mock.count_events('r'),
            0,
            "V3 attempted a duplicate update_table call; trace = {:?}",
            handle.mock.get_event_trace()
        );

        fail::remove("iceberg_v3_commit_prune_fail");

        // After clearing, the cluster recovers and commits a new epoch. The
        // sustained prune failure keeps the row Pending until a full meta
        // recovery cascade redrives it (~30s worst case across the 30 CI
        // seeds). The post-clear wait only drains the single in-flight cascade,
        // so budget 120s (~4x) for ample margin.
        tokio::time::timeout(
            Duration::from_secs(120),
            handle.mock.wait_for_event_count('I', baseline_i_count + 1),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "no new commit after fault cleared; trace = {:?}",
                handle.mock.get_event_trace()
            )
        })??;

        workload.shutdown().await;
        assert_invariants(&mut handle).await?;
        Ok(())
    }

    /// F5, F11, and F12 fire concurrently at low rates. V3 must exercise
    /// the dangerous race windows (iceberg-committed-but-prune-failed,
    /// repeated re-commits, etc.) without ever producing a duplicate
    /// snapshot.
    #[tokio::test]
    async fn failpoint_limited_test_v3_random_failures_corner_case() -> Result<()> {
        let mut handle = start_v3_test_cluster_with_sink(4).await?;

        let workload = spawn_continuous_workload(
            &mut handle.cluster,
            WorkloadConfig {
                seed: 5,
                ..WorkloadConfig::default()
            },
        );

        tokio::time::timeout(Duration::from_secs(30), handle.mock.wait_for_event('I'))
            .await
            .map_err(|_| anyhow::anyhow!("baseline 'I' never observed"))??;
        let baseline_i_count = handle.mock.count_events('I');

        // F5 and F12 fire at 5% to keep the recovery path lightly stressed
        // without saturating it. F11 is the only mock-observable fault, so
        // we pin it at 100% until one failure has landed and then drop it
        // to 5% — the test's compound window is then guaranteed to have
        // exercised every fault type at least once, independent of seed.
        fail::cfg("iceberg_v3_persist_pre_commit_fail", "5%return").unwrap();
        fail::cfg("iceberg_v3_commit_prune_fail", "5%return").unwrap();
        handle.mock.set_err_rate_txn_commit(1.0);
        tokio::time::timeout(
            Duration::from_secs(30),
            handle.mock.wait_for_event_count('i', 1),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "F11 never fired even at 100%; trace = {:?}",
                handle.mock.get_event_trace()
            )
        })??;
        handle.mock.set_err_rate_txn_commit(0.05);
        tokio::time::sleep(Duration::from_secs(15)).await;
        fail::remove("iceberg_v3_persist_pre_commit_fail");
        fail::remove("iceberg_v3_commit_prune_fail");
        handle.mock.set_err_rate_txn_commit(0.0);

        // Sink commits at least one fresh snapshot after the fault window. The
        // compound F5/F11/F12 window drives a full meta recovery (F11 pinned at
        // 100% guarantees at least one retry-exhausted commit). Once every rate
        // is 0 the post-clear wait only drains the single in-flight cascade
        // (~30s worst case across the 30 CI seeds), so budget 120s (~4x) for
        // ample margin.
        tokio::time::timeout(
            Duration::from_secs(120),
            handle.mock.wait_for_event_count('I', baseline_i_count + 1),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "no new commit after compound fault window cleared; trace = {:?}",
                handle.mock.get_event_trace()
            )
        })??;

        workload.shutdown().await;

        // V3's idempotency guard must hold under compound faults as well.
        assert_eq!(
            handle.mock.count_events('r'),
            0,
            "V3 attempted a duplicate update_table call under compound faults; trace = {:?}",
            handle.mock.get_event_trace()
        );

        assert_invariants(&mut handle).await?;
        Ok(())
    }

    /// Kill meta while pre-commit is being repeatedly forced to fail, so
    /// the kill lands while the coordinator is stuck mid-pre-commit. After
    /// the kill and fault clear, V3 must re-register the sink, re-load the
    /// catalog, and resume committing.
    #[tokio::test]
    async fn failpoint_limited_test_v3_meta_kill_during_pre_commit() -> Result<()> {
        let mut handle = start_v3_test_cluster_with_sink(4).await?;

        let workload = spawn_continuous_workload(
            &mut handle.cluster,
            WorkloadConfig {
                seed: 6,
                ..WorkloadConfig::default()
            },
        );

        // Inject persist-pre-commit failure *before* any baseline so the
        // coordinator is genuinely stuck mid-pre-commit when the kill lands.
        // There will be no 'I' before the kill, so don't wait for one.
        fail::cfg("iceberg_v3_persist_pre_commit_fail", "return").unwrap();

        // Give the coordinator time to enter pre-commit and fail, driving the
        // worker into the persist-fail state.
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Kill + restart meta deterministically while pre-commit is stuck.
        handle.mock.push_kill_event();
        handle.cluster.kill_nodes_and_restart(["meta-1"], 3).await;

        // Clear the fault so the recovered worker can complete pre-commit.
        fail::remove("iceberg_v3_persist_pre_commit_fail");

        // Recovery must succeed and the sink must commit at least one fresh
        // snapshot strictly after the kill marker.
        tokio::time::timeout(
            Duration::from_secs(60),
            handle.mock.wait_for_iceberg_write_after_kill(),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "no new commit after meta kill+restart; trace = {:?}",
                handle.mock.get_event_trace()
            )
        })??;

        workload.shutdown().await;
        assert_invariants(&mut handle).await?;
        Ok(())
    }

    /// Combine commit-prune failure with meta kill so the kill lands while
    /// the DB holds a Pending row whose iceberg snapshot was already
    /// committed. While F12 stays active the recovered V3 worker must take
    /// the idempotency fast-path — see the snapshot_id already present in
    /// `table.metadata().snapshots()` and skip `update_table` entirely —
    /// so neither a fresh iceberg write nor a duplicate `'r'` may appear
    /// after the kill. Once the fault clears, fresh commits resume.
    #[tokio::test]
    async fn failpoint_limited_test_v3_meta_kill_during_commit() -> Result<()> {
        let mut handle = start_v3_test_cluster_with_sink(4).await?;

        let workload = spawn_continuous_workload(
            &mut handle.cluster,
            WorkloadConfig {
                seed: 7,
                ..WorkloadConfig::default()
            },
        );

        tokio::time::timeout(Duration::from_secs(30), handle.mock.wait_for_event('I'))
            .await
            .map_err(|_| anyhow::anyhow!("baseline 'I' never observed"))??;

        // Force a sustained "iceberg committed but DB Pending" window so the
        // kill below happens while at least one such row exists.
        fail::cfg("iceberg_v3_commit_prune_fail", "return").unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
        handle.mock.push_kill_event();
        handle.cluster.kill_nodes_and_restart(["meta-1"], 3).await;

        // While F12 holds, the recovered worker can only take the idempotent
        // fast-path on the pending row (re-load table, see the snapshot_id
        // already present, skip `update_table`). Give meta a generous window
        // to restart and run a few handle_commit cycles, then verify the
        // negative: no fresh iceberg write appeared after the kill, and no
        // duplicate `update_table` call reached the catalog.
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(
            handle.mock.count_iceberg_writes_after_kill(),
            0,
            "recovery wrote a fresh iceberg snapshot instead of taking the idempotent fast-path; trace = {:?}",
            handle.mock.get_event_trace()
        );
        assert_eq!(
            handle.mock.count_events('r'),
            0,
            "V3 attempted a duplicate update_table call during recovery; trace = {:?}",
            handle.mock.get_event_trace()
        );

        // Clear the fault so the recovered worker can finally mark Committed.
        fail::remove("iceberg_v3_commit_prune_fail");

        // After clearing, the sink should resume committing fresh snapshots.
        tokio::time::timeout(
            Duration::from_secs(60),
            handle.mock.wait_for_iceberg_write_after_kill(),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "no new commit after meta kill + fault clear; trace = {:?}",
                handle.mock.get_event_trace()
            )
        })??;

        workload.shutdown().await;
        assert_invariants(&mut handle).await?;
        Ok(())
    }

    /// Chaos: kill+restart meta several times back-to-back. Workload + sink
    /// must converge in the end without producing duplicate snapshots.
    #[tokio::test]
    async fn failpoint_limited_test_v3_meta_kill_repeatedly() -> Result<()> {
        let mut handle = start_v3_test_cluster_with_sink(4).await?;

        let workload = spawn_continuous_workload(
            &mut handle.cluster,
            WorkloadConfig {
                seed: 8,
                ..WorkloadConfig::default()
            },
        );

        tokio::time::timeout(Duration::from_secs(30), handle.mock.wait_for_event('I'))
            .await
            .map_err(|_| anyhow::anyhow!("baseline 'I' never observed"))??;

        for _ in 0..6 {
            tokio::time::sleep(Duration::from_secs(10)).await;
            handle.mock.push_kill_event();
            handle.cluster.kill_nodes_and_restart(["meta-1"], 3).await;
        }

        // After the kills stop, give the cluster time to settle and commit
        // at least one more snapshot from the resumed workload.
        let i_after_kills = handle.mock.count_events('I');
        tokio::time::timeout(
            Duration::from_secs(60),
            handle.mock.wait_for_event_count('I', i_after_kills + 1),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "sink never committed after repeated kills; trace = {:?}",
                handle.mock.get_event_trace()
            )
        })??;

        workload.shutdown().await;
        assert_invariants(&mut handle).await?;
        Ok(())
    }

    /// Configure `iceberg_v3_recovery_fail` with `1*panic` so the first
    /// post-restart `recovery()` invocation panics. The supervisor must
    /// restart the affected component and the second recovery attempt must
    /// succeed. The final 60s convergence wait is also the canary for the
    /// case where a panic in the recovery task deadlocks meta — if that
    /// happens, this test will hang/fail rather than hide the bug.
    #[tokio::test]
    async fn failpoint_limited_test_v3_recovery_during_recovery() -> Result<()> {
        let mut handle = start_v3_test_cluster_with_sink(4).await?;

        let workload = spawn_continuous_workload(
            &mut handle.cluster,
            WorkloadConfig {
                seed: 9,
                ..WorkloadConfig::default()
            },
        );

        tokio::time::timeout(Duration::from_secs(30), handle.mock.wait_for_event('I'))
            .await
            .map_err(|_| anyhow::anyhow!("baseline 'I' never observed"))??;
        let baseline_i_count = handle.mock.count_events('I');

        // Arm `recovery()` to panic on its next invocation (one-shot). The
        // `fail::fail_point!` macro panics directly on `panic` / `N*panic`
        // actions, producing a real panic in the recovery task.
        fail::cfg("iceberg_v3_recovery_fail", "1*panic").unwrap();

        // Force a recovery via meta kill. The first recovery attempt panics,
        // exhausting the "1*panic" action. The supervisor must restart the
        // affected component and a subsequent recovery attempt must succeed.
        handle.mock.push_kill_event();
        handle.cluster.kill_nodes_and_restart(["meta-1"], 3).await;

        // The action budget is exhausted by now; remove just in case.
        fail::remove("iceberg_v3_recovery_fail");

        tokio::time::timeout(
            Duration::from_secs(60),
            handle.mock.wait_for_event_count('I', baseline_i_count + 1),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "no new commit after recovery-during-recovery scenario; trace = {:?}",
                handle.mock.get_event_trace()
            )
        })??;

        workload.shutdown().await;
        assert_invariants(&mut handle).await?;
        Ok(())
    }
}
