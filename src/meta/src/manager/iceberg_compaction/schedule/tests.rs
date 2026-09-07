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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use prometheus::Registry;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::Event as IcebergResponseEvent;
use risingwave_pb::id::IcebergCompactionTaskId;

use super::*;
use crate::controller::catalog::CatalogController;
use crate::controller::cluster::ClusterController;
use crate::hummock::IcebergCompactorManager;
use crate::manager::MetaOpts;
use crate::rpc::metrics::MetaMetrics;

async fn build_test_manager() -> Arc<IcebergCompactionManager> {
    let mut opts = MetaOpts::test(false);
    opts.iceberg_compaction_report_timeout_sec = 30 * 60;
    let env = MetaSrvEnv::for_test_opts(opts, |_| ()).await;
    let cluster_ctl = Arc::new(
        ClusterController::new(env.clone(), Duration::from_secs(1))
            .await
            .unwrap(),
    );
    let catalog_ctl = Arc::new(CatalogController::new(env.clone()).await.unwrap());
    let metadata_manager = MetadataManager::new(cluster_ctl, catalog_ctl);
    let iceberg_compactor_manager = Arc::new(IcebergCompactorManager::new());
    let metrics = Arc::new(MetaMetrics::for_test(&Registry::new()));
    let (manager, _) =
        IcebergCompactionManager::build(env, metadata_manager, iceberg_compactor_manager, metrics);
    manager
}

/// Builds a single-task track. Tests for sequence-bounded rounds must use
/// `new_round_track` instead.
fn new_track(
    now: Instant,
    trigger_interval_sec: u64,
    trigger_snapshot_count: usize,
    pending_commit_count: usize,
) -> CompactionTrack {
    CompactionTrack {
        configured_task_type: TaskType::Full,
        write_mode: IcebergWriteMode::CopyOnWrite,
        trigger_interval_sec,
        trigger_snapshot_count,
        report_timeout: Duration::from_secs(30 * 60),
        last_config_refresh_at: now,
        pending_commit_count,
        latest_observed_snapshot: None,
        round_max_file_sequence_number: None,
        finish_action: CompactionTrackFinishAction::KeepTrack,
        state: CompactionTrackState::Idle {
            next_compaction_time: now + Duration::from_secs(trigger_interval_sec),
            manual_task_type: None,
        },
    }
}

fn new_round_track(
    now: Instant,
    trigger_interval_sec: u64,
    trigger_snapshot_count: usize,
    pending_commit_count: usize,
) -> CompactionTrack {
    let mut track = new_track(
        now,
        trigger_interval_sec,
        trigger_snapshot_count,
        pending_commit_count,
    );
    track.write_mode = IcebergWriteMode::MergeOnRead;
    track
}

fn start_in_flight_on(
    track: &mut CompactionTrack,
    task_id: impl Into<IcebergCompactionTaskId>,
    compactor_context_id: HummockContextId,
    now: Instant,
) {
    track.start_attempt();
    track.mark_dispatched(task_id.into(), compactor_context_id, now);
}

fn start_in_flight(
    track: &mut CompactionTrack,
    task_id: impl Into<IcebergCompactionTaskId>,
    now: Instant,
) {
    start_in_flight_on(track, task_id, 1.into(), now);
}

fn record_commits(track: &mut CompactionTrack, n: usize) {
    for _ in 0..n {
        track.record_commit();
    }
}

fn committed_snapshot(snapshot_id: i64, timestamp_ms: i64) -> IcebergCommittedSnapshot {
    IcebergCommittedSnapshot {
        branch: "main".to_owned(),
        snapshot_id,
        timestamp_ms,
        max_file_sequence_number: Some(snapshot_id),
    }
}

fn commit_update(snapshot_id: i64, timestamp_ms: i64) -> SinkUpdateKind {
    SinkUpdateKind::Commit {
        observed_snapshot: committed_snapshot(snapshot_id, timestamp_ms),
    }
}

fn force_update(snapshot_id: i64, timestamp_ms: i64) -> SinkUpdateKind {
    SinkUpdateKind::ForceCompaction {
        observed_snapshot: committed_snapshot(snapshot_id, timestamp_ms),
    }
}

fn empty_inner() -> IcebergCompactionManagerInner {
    IcebergCompactionManagerInner {
        sink_schedules: HashMap::new(),
        snapshot_expiration_sink_ids: HashSet::new(),
        manifest_rewrite_sink_ids: HashSet::new(),
        manual_compaction_waiters: HashMap::new(),
    }
}

fn new_test_iceberg_config(
    trigger_interval_sec: u64,
    trigger_snapshot_count: usize,
    compaction_type: CompactionType,
) -> IcebergConfig {
    let mut values = BTreeMap::from([
        ("connector".to_owned(), "iceberg".to_owned()),
        ("type".to_owned(), "append-only".to_owned()),
        ("force_append_only".to_owned(), "true".to_owned()),
        ("catalog.name".to_owned(), "test-catalog".to_owned()),
        ("catalog.type".to_owned(), "storage".to_owned()),
        ("warehouse.path".to_owned(), "s3://iceberg".to_owned()),
        ("database.name".to_owned(), "test_db".to_owned()),
        ("table.name".to_owned(), "test_table".to_owned()),
    ]);
    values.insert(
        "compaction_interval_sec".to_owned(),
        trigger_interval_sec.to_string(),
    );
    values.insert(
        "compaction.trigger_snapshot_count".to_owned(),
        trigger_snapshot_count.to_string(),
    );
    values.insert(
        "compaction.type".to_owned(),
        match compaction_type {
            CompactionType::Auto => "auto",
            CompactionType::Full => "full",
            CompactionType::SmallFiles => "small-files",
            CompactionType::FilesWithDelete => "files-with-delete",
        }
        .to_owned(),
    );
    values.insert("enable_compaction".to_owned(), "true".to_owned());

    IcebergConfig::from_btreemap(values).unwrap()
}

#[tokio::test]
async fn test_schedule_resolves_licensed_compaction_type_policy() {
    let manager = build_test_manager().await;
    let now = Instant::now();
    assert!(
        risingwave_common::license::Feature::IcebergCompaction
            .check_available()
            .is_ok()
    );

    for (write_mode, configured_type, expected_task_type) in [
        (IcebergWriteMode::CopyOnWrite, None, TaskType::Auto),
        (
            IcebergWriteMode::CopyOnWrite,
            Some(CompactionType::Full),
            TaskType::Auto,
        ),
        (IcebergWriteMode::MergeOnRead, None, TaskType::Auto),
        (
            IcebergWriteMode::MergeOnRead,
            Some(CompactionType::Full),
            TaskType::Full,
        ),
    ] {
        let mut config = new_test_iceberg_config(300, 3, CompactionType::Auto);
        config.r#type = "upsert".to_owned();
        config.write_mode = write_mode;
        config.compaction_type = configured_type;

        let mut track = manager.create_compaction_track(&config, now);
        assert_eq!(track.configured_task_type, expected_task_type);
        assert_eq!(track.write_mode, write_mode);

        track.record_observed_snapshot(committed_snapshot(10, 1000));
        track.record_commit();
        let attempt = track.start_attempt();
        let expected_boundary = (write_mode == IcebergWriteMode::MergeOnRead).then_some(10);
        assert_eq!(track.round_max_file_sequence_number, expected_boundary);
        assert_eq!(attempt.max_file_sequence_number, expected_boundary);
    }
}

#[test]
fn test_automatic_compaction_without_file_sequence_bound_stays_unbounded() {
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 0);
    let mut observed_snapshot = committed_snapshot(10, 1000);
    observed_snapshot.max_file_sequence_number = None;
    track.record_observed_snapshot(observed_snapshot);
    track.record_commit();

    let attempt = track.start_attempt();

    assert_eq!(attempt.max_file_sequence_number, None);
    assert_eq!(track.round_max_file_sequence_number, None);
    assert_eq!(track.pending_commit_count, 1);
}

#[test]
fn test_should_trigger_by_pending_commit_threshold() {
    let now = Instant::now();
    let track = new_track(now, 300, 3, 3);

    assert!(track.should_trigger(now));
}

#[test]
fn test_should_trigger_by_interval_only_with_pending_commits() {
    let now = Instant::now();
    let mut track = new_track(now, 60, 10, 1);
    track.state = CompactionTrackState::Idle {
        next_compaction_time: now - Duration::from_secs(1),
        manual_task_type: None,
    };
    assert!(track.should_trigger(now));

    let mut empty_track = new_track(now, 60, 10, 0);
    empty_track.state = CompactionTrackState::Idle {
        next_compaction_time: now - Duration::from_secs(1),
        manual_task_type: None,
    };
    assert!(!empty_track.should_trigger(now));
}

#[test]
fn test_record_force_compaction_bootstraps_or_preserves_backlog() {
    let now = Instant::now();

    for (initial_backlog, expected_backlog) in [(0, 1), (4, 4)] {
        let mut track = new_track(now, 300, 10, initial_backlog);

        track.record_force_compaction(now, None);

        assert_eq!(track.pending_commit_count, expected_backlog);
        assert!(track.should_trigger(now));
    }
}

#[tokio::test]
async fn test_commit_update_freezes_gc_watermark_after_attempt_start() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(404);
    let now = Instant::now();
    let config = new_test_iceberg_config(300, 10, CompactionType::SmallFiles);
    let mut guard = empty_inner();

    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(10, 1000),
            now,
            allow_track_initialization: true,
            loaded_config: Some(config),
        },
    );

    assert!(applied);
    guard
        .sink_schedules
        .get_mut(&sink_id)
        .unwrap()
        .start_attempt();

    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(11, 2000),
            now,
            allow_track_initialization: false,
            loaded_config: None,
        },
    );

    assert!(applied);
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(
        track
            .latest_observed_snapshot
            .as_ref()
            .map(|s| s.snapshot_id),
        Some(11)
    );
    match track.active_attempt_gc_watermark_snapshot() {
        Some(Some(snapshot)) => assert_eq!(snapshot.snapshot_id, 10),
        protection => panic!("unexpected gc watermark: {protection:?}"),
    }

    guard
        .sink_schedules
        .get_mut(&sink_id)
        .unwrap()
        .mark_dispatched(1.into(), 1.into(), now);
    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(12, 3000),
            now,
            allow_track_initialization: false,
            loaded_config: None,
        },
    );
    assert!(applied);
    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: force_update(13, 4000),
            now,
            allow_track_initialization: false,
            loaded_config: None,
        },
    );
    assert!(applied);

    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(
        track
            .latest_observed_snapshot
            .as_ref()
            .map(|s| s.snapshot_id),
        Some(13)
    );
    match track.active_attempt_gc_watermark_snapshot() {
        Some(Some(snapshot)) => assert_eq!(snapshot.snapshot_id, 10),
        protection => panic!("unexpected gc watermark: {protection:?}"),
    }
}

#[tokio::test]
async fn test_force_update_initializes_observed_snapshot() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(405);
    let now = Instant::now();
    let config = new_test_iceberg_config(300, 10, CompactionType::SmallFiles);
    let mut guard = empty_inner();

    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: force_update(10, 1000),
            now,
            allow_track_initialization: true,
            loaded_config: Some(config),
        },
    );

    assert!(applied);
    let track = guard.sink_schedules.get_mut(&sink_id).unwrap();
    track.start_attempt();
    match track.active_attempt_gc_watermark_snapshot() {
        Some(Some(snapshot)) => assert_eq!(snapshot.snapshot_id, 10),
        protection => panic!("unexpected gc watermark: {protection:?}"),
    }
}

#[test]
fn test_manual_task_without_observed_snapshot_has_no_gc_watermark() {
    let now = Instant::now();
    let skip_sink_id = SinkId::new(42);
    let mut skip_track = new_track(now, 300, 10, 0);
    skip_track.record_force_compaction(now, Some(TaskType::Full));
    skip_track.start_attempt();

    let mut inner = empty_inner();
    inner.sink_schedules.insert(skip_sink_id, skip_track);

    assert!(matches!(
        inner
            .sink_schedules
            .get(&skip_sink_id)
            .unwrap()
            .active_attempt_gc_watermark_snapshot(),
        Some(None)
    ));
}

#[test]
fn test_finish_success_clears_dispatched_baseline_and_starts_cooldown() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 12);
    start_in_flight(&mut track, 1, now);

    track.finish_success(now);

    assert_eq!(track.pending_commit_count, 0);
    assert!(!track.should_trigger(now));
    match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
            ..
        } => assert!(next_compaction_time >= now + Duration::from_secs(120)),
        CompactionTrackState::PendingDispatch { .. } | CompactionTrackState::InFlight { .. } => {
            panic!("track should be idle")
        }
    }
}

#[test]
fn test_active_round_keeps_sequence_boundary_until_drained() {
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 1);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 1, now);

    assert_eq!(track.round_max_file_sequence_number, Some(10));
    track.record_observed_snapshot(committed_snapshot(11, 2000));
    track.record_commit();
    track.finish_success(now);

    assert_eq!(track.round_max_file_sequence_number, Some(10));
    assert!(track.should_trigger(now));
    track.start_attempt();
    track.mark_dispatched(2.into(), 1.into(), now);
    track.finish_drained(now);

    assert_eq!(track.round_max_file_sequence_number, None);
    assert!(!track.should_trigger(now));
    assert_eq!(track.pending_commit_count, 1);
}

#[test]
fn test_force_advances_active_round_retry_without_adding_backlog() {
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 1);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 1, now);
    track.finish_failed(now);

    let force_at = now + Duration::from_millis(500);
    assert!(!track.should_trigger(force_at));
    track.record_force_compaction(force_at, None);

    assert_eq!(track.pending_commit_count, 0);
    assert_eq!(track.round_max_file_sequence_number, Some(10));
    assert!(track.should_trigger(force_at));
}

#[test]
fn test_active_round_failure_backoff_ignores_commit_threshold() {
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 3, 1);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 1, now);
    record_commits(&mut track, 3);

    track.finish_failed(now);

    assert_eq!(track.round_max_file_sequence_number, Some(10));
    assert_eq!(track.pending_commit_count, 3);
    assert!(!track.should_trigger(now));
    assert!(track.should_trigger(now + Duration::from_secs(1)));
}

#[test]
fn test_finish_failed_preserves_backlog_and_allows_retry() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 4);
    start_in_flight(&mut track, 1, now);

    track.finish_failed(now);

    assert_eq!(track.pending_commit_count, 4);
    assert!(!track.should_trigger(now));
    assert!(track.should_trigger(now + Duration::from_secs(1)));
    match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
            ..
        } => assert_eq!(next_compaction_time, now + Duration::from_secs(1)),
        CompactionTrackState::PendingDispatch { .. } | CompactionTrackState::InFlight { .. } => {
            panic!("track should be idle")
        }
    }
}

#[test]
fn test_report_timeout_starts_at_dispatch() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 5);
    track.report_timeout = Duration::from_secs(17);
    start_in_flight(&mut track, 1, now);

    match track.state {
        CompactionTrackState::InFlight { .. } => {}
        CompactionTrackState::Idle { .. } => panic!("track should remain pending"),
        CompactionTrackState::PendingDispatch { .. } => {
            panic!("track should have been dispatched")
        }
    }
    assert!(!track.is_report_timed_out(now + track.report_timeout - Duration::from_secs(1)));
    assert!(track.is_report_timed_out(now + track.report_timeout));
}

#[test]
fn test_revert_pre_dispatch_failure_requeues_after_backoff_without_losing_backlog() {
    let now = Instant::now();
    for additional_commits in [0, 3] {
        let mut track = new_track(now, 120, 10, 5);
        track.start_attempt();
        record_commits(&mut track, additional_commits);

        let revert_at = now + Duration::from_secs(30);
        assert_eq!(
            track.revert_pre_dispatch_failure(revert_at),
            CompactionTrackFinishAction::KeepTrack
        );

        assert_eq!(track.pending_commit_count, 5 + additional_commits);
        match track.state {
            CompactionTrackState::Idle {
                next_compaction_time,
                ..
            } => assert_eq!(next_compaction_time, revert_at + Duration::from_secs(1)),
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. } => {
                panic!("track should be restored to idle")
            }
        }
    }
}

#[test]
fn test_mark_dispatched_records_task_id_for_stale_report_filtering() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 5);
    track.start_attempt();
    track.mark_dispatched(42.into(), 1.into(), now);

    assert!(track.is_in_flight_task(42.into()));
    assert!(!track.is_in_flight_task(43.into()));
}

#[test]
fn test_force_compaction_does_not_make_active_attempt_triggerable() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.start_attempt();

    track.record_force_compaction(now, None);

    assert!(!track.should_trigger(now));
}

#[test]
fn test_finish_failed_after_force_keeps_force_backlog() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.record_force_compaction(now, None);
    start_in_flight(&mut track, 1, now);

    track.finish_failed(now);

    assert_eq!(track.pending_commit_count, 1);
    assert!(!track.should_trigger(now));
    assert!(track.should_trigger(now + Duration::from_secs(1)));
}

#[test]
fn test_finish_success_after_force_consumes_force_backlog() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.record_force_compaction(now, None);
    start_in_flight(&mut track, 1, now);

    track.finish_success(now);

    assert_eq!(track.pending_commit_count, 0);
    assert!(!track.should_trigger(now));
}

#[test]
fn test_record_commit_during_active_attempt_is_preserved_after_success() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    start_in_flight(&mut track, 1, now);
    record_commits(&mut track, 5);

    track.finish_success(now);

    assert_eq!(track.pending_commit_count, 5);
}

#[test]
fn test_update_interval_resets_idle_deadline() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 1);

    track.update_interval(300, now);

    assert_eq!(track.trigger_interval_sec, 300);
    match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
            ..
        } => assert_eq!(next_compaction_time, now + Duration::from_secs(300)),
        CompactionTrackState::PendingDispatch { .. } | CompactionTrackState::InFlight { .. } => {
            panic!("track should stay idle")
        }
    }
}

#[test]
fn test_update_interval_same_value_keeps_existing_idle_deadline() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 1);
    let original_deadline = match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
            ..
        } => next_compaction_time,
        CompactionTrackState::PendingDispatch { .. } | CompactionTrackState::InFlight { .. } => {
            panic!("track should start idle")
        }
    };

    track.update_interval(120, now + Duration::from_secs(30));

    match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
            ..
        } => assert_eq!(next_compaction_time, original_deadline),
        CompactionTrackState::PendingDispatch { .. } | CompactionTrackState::InFlight { .. } => {
            panic!("track should stay idle")
        }
    }
}

#[test]
fn test_update_interval_does_not_interrupt_active_attempt() {
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 1);
    track.start_attempt();

    track.update_interval(300, now);

    assert_eq!(track.trigger_interval_sec, 300);
    match track.state {
        CompactionTrackState::PendingDispatch { .. } => {}
        CompactionTrackState::Idle { .. } => panic!("active attempt should be preserved"),
        CompactionTrackState::InFlight { .. } => {
            panic!("track should remain pending dispatch")
        }
    }
}

#[test]
fn test_needs_config_refresh_respects_ttl() {
    let now = Instant::now();
    let track = new_track(now, 120, 10, 1);

    let refresh_interval = Duration::from_secs(60);

    assert!(!track.needs_config_refresh(
        now + refresh_interval - Duration::from_secs(1),
        refresh_interval,
    ));
    assert!(track.needs_config_refresh(now + refresh_interval, refresh_interval));
}

#[tokio::test]
async fn test_commit_update_refreshes_config_while_task_is_in_flight() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(240);
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 1);
    track.last_config_refresh_at = now - manager.config_refresh_interval();
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 1, now);
    manager.inner.write().sink_schedules.insert(sink_id, track);

    // Holding the catalog write lock makes an attempted config refresh observable:
    // the commit update must wait here even though the track is in flight.
    let catalog_write_guard = manager
        .metadata_manager
        .catalog_controller
        .get_inner_write_guard()
        .await;
    let join_handle = {
        let manager = manager.clone();
        tokio::spawn(async move {
            manager
                .update_iceberg_commit_info(IcebergSinkCompactionUpdate {
                    sink_id,
                    force_compaction: false,
                    observed_snapshot: committed_snapshot(11, 2000),
                })
                .await;
        })
    };
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    assert!(!join_handle.is_finished());

    join_handle.abort();
    assert!(join_handle.await.unwrap_err().is_cancelled());
    drop(catalog_write_guard);
}

#[tokio::test]
async fn test_apply_sink_update_refreshes_existing_idle_track() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(41);
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 1);
    track.last_config_refresh_at = now - manager.config_refresh_interval();
    manager.inner.write().sink_schedules.insert(sink_id, track);

    let refresh_at = now + Duration::from_secs(30);
    let config = new_test_iceberg_config(300, 3, CompactionType::Auto);
    let mut guard = manager.inner.write();

    manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(1, 1),
            now: refresh_at,
            allow_track_initialization: false,
            loaded_config: Some(config),
        },
    );

    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.configured_task_type, TaskType::Auto);
    assert_eq!(track.trigger_interval_sec, 300);
    assert_eq!(track.trigger_snapshot_count, 3);
    assert_eq!(track.last_config_refresh_at, refresh_at);
    assert_eq!(track.pending_commit_count, 2);
    match track.state {
        CompactionTrackState::Idle {
            next_compaction_time,
            ..
        } => assert_eq!(next_compaction_time, refresh_at + Duration::from_secs(300)),
        CompactionTrackState::PendingDispatch { .. } | CompactionTrackState::InFlight { .. } => {
            panic!("track should stay idle")
        }
    }
}

#[tokio::test]
async fn test_update_iceberg_commit_info_skips_missing_track_on_load_failure() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(42);

    manager
        .update_iceberg_commit_info(IcebergSinkCompactionUpdate {
            sink_id,
            force_compaction: false,
            observed_snapshot: committed_snapshot(1, 1),
        })
        .await;

    let guard = manager.inner.read();
    assert!(!guard.sink_schedules.contains_key(&sink_id));
}

#[tokio::test]
async fn test_update_iceberg_commit_info_refresh_failure_keeps_refresh_deadline_stale() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(43);
    let now = Instant::now();
    let stale_refresh_at = now - manager.config_refresh_interval();
    let mut track = new_track(now, 120, 7, 3);
    track.last_config_refresh_at = stale_refresh_at;
    manager.inner.write().sink_schedules.insert(sink_id, track);

    manager
        .update_iceberg_commit_info(IcebergSinkCompactionUpdate {
            sink_id,
            force_compaction: false,
            observed_snapshot: committed_snapshot(1, 1),
        })
        .await;

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.trigger_interval_sec, 120);
    assert_eq!(track.trigger_snapshot_count, 7);
    assert_eq!(track.pending_commit_count, 4);
    assert_eq!(track.last_config_refresh_at, stale_refresh_at);
}

#[tokio::test]
async fn test_apply_sink_update_creates_missing_track() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(44);
    let now = Instant::now();
    let config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    let mut guard = empty_inner();

    manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(1, 1),
            now,
            allow_track_initialization: true,
            loaded_config: Some(config),
        },
    );

    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.configured_task_type, TaskType::SmallFiles);
    assert_eq!(track.trigger_interval_sec, 300);
    assert_eq!(track.trigger_snapshot_count, 3);
    assert_eq!(track.pending_commit_count, 1);
    assert_eq!(track.last_config_refresh_at, now);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_apply_sink_update_tracks_metadata_maintenance_without_compaction() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(144);
    let now = Instant::now();
    let mut config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    config.enable_compaction = false;
    config.enable_snapshot_expiration = true;
    config.enable_manifest_rewrite = true;
    let mut guard = empty_inner();

    manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(1, 1),
            now,
            allow_track_initialization: true,
            loaded_config: Some(config),
        },
    );

    assert!(!guard.sink_schedules.contains_key(&sink_id));
    assert!(guard.snapshot_expiration_sink_ids.contains(&sink_id));
    assert!(guard.manifest_rewrite_sink_ids.contains(&sink_id));
}

#[tokio::test]
async fn test_apply_manual_force_update_allows_disabled_compaction() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(146);
    let now = Instant::now();
    let mut config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    config.enable_compaction = false;
    let mut guard = empty_inner();

    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: SinkUpdateKind::ManualForceCompaction {
                task_type: TaskType::Full,
            },
            now,
            allow_track_initialization: true,
            loaded_config: Some(config),
        },
    );

    assert!(applied);
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 1);
    assert!(track.should_trigger(now));
    assert_eq!(
        track.finish_action,
        CompactionTrackFinishAction::RemoveTrack
    );
}

#[tokio::test]
async fn test_start_manual_compaction_rejects_active_automatic_round() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(407);
    let now = Instant::now();
    let mut track = new_round_track(now, 300, 10, 1);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 1, now);
    track.finish_success(now);

    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
    assert_eq!(track.round_max_file_sequence_number, Some(10));

    manager.inner.write().sink_schedules.insert(sink_id, track);

    let error = manager.start_manual_compaction(sink_id).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("manual Full compaction is rejected")
    );

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.round_max_file_sequence_number, Some(10));
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_refresh_schedule_config_updates_next_attempt_without_changing_active_attempt() {
    let manager = build_test_manager().await;
    let now = Instant::now();
    let mut track = new_round_track(now, 300, 3, 1);
    track.configured_task_type = TaskType::SmallFiles;
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 1, now);

    let refresh_at = now + Duration::from_secs(5);
    let refreshed_config = new_test_iceberg_config(42, 7, CompactionType::FilesWithDelete);
    manager.refresh_schedule_config(&mut track, &refreshed_config, refresh_at);

    assert_eq!(track.configured_task_type, TaskType::FilesWithDelete);
    assert_eq!(track.effective_task_type(), TaskType::SmallFiles);
    assert_eq!(track.trigger_interval_sec, 42);
    assert_eq!(track.trigger_snapshot_count, 7);
    assert_eq!(track.round_max_file_sequence_number, Some(10));

    track.finish_success(refresh_at);
    assert!(track.should_trigger(refresh_at));
    let next_attempt = track.start_attempt();
    assert_eq!(next_attempt.task_type, TaskType::FilesWithDelete);
    assert_eq!(next_attempt.max_file_sequence_number, Some(10));
}

#[tokio::test]
async fn test_manual_force_update_uses_selected_task_type_once() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(147);
    let now = Instant::now();
    let config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    {
        let mut guard = manager.inner.write();
        let applied = manager.apply_sink_update(
            &mut guard,
            PreparedSinkUpdate {
                sink_id,
                kind: SinkUpdateKind::ManualForceCompaction {
                    task_type: TaskType::FilesWithDelete,
                },
                now,
                allow_track_initialization: true,
                loaded_config: Some(config),
            },
        );

        assert!(applied);
        let track = guard.sink_schedules.get(&sink_id).unwrap();
        assert_eq!(track.configured_task_type, TaskType::SmallFiles);

        let applied = manager.apply_sink_update(
            &mut guard,
            PreparedSinkUpdate {
                sink_id,
                kind: force_update(1, 1),
                now,
                allow_track_initialization: false,
                loaded_config: None,
            },
        );
        assert!(applied);
    }

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);
    assert_eq!(handles.len(), 1);
    assert_eq!(handles[0].attempt.task_type, TaskType::FilesWithDelete);
    assert_eq!(handles[0].attempt.max_file_sequence_number, None);
    drop(handles);

    manager
        .inner
        .write()
        .sink_schedules
        .get_mut(&sink_id)
        .unwrap()
        .record_force_compaction(Instant::now(), None);

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);
    assert_eq!(handles.len(), 1);
    assert_eq!(handles[0].attempt.task_type, TaskType::SmallFiles);
    drop(handles);
}

#[tokio::test]
async fn test_apply_sink_update_keeps_in_flight_attempt_when_compaction_is_disabled() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(145);
    let task_id = 8;
    let now = Instant::now();
    let mut config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    config.enable_compaction = false;
    let mut track = new_round_track(now, 300, 3, 1);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, task_id, now);
    let mut guard = empty_inner();
    guard.sink_schedules.insert(sink_id, track);

    manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(1, 1),
            now,
            allow_track_initialization: false,
            loaded_config: Some(config),
        },
    );

    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert!(matches!(
        track.state,
        CompactionTrackState::InFlight { task_id, .. } if task_id.as_raw_id() == 8
    ));
    assert_eq!(track.round_max_file_sequence_number, Some(10));
    assert_eq!(
        track.finish_action,
        CompactionTrackFinishAction::RemoveTrack
    );
}

#[tokio::test]
async fn test_apply_sink_update_keeps_queued_manual_task_when_compaction_is_disabled() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(248);
    let now = Instant::now();
    let mut config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    config.enable_compaction = false;
    let mut track = new_track(now, 300, 3, 1);
    track.record_force_compaction(now, Some(TaskType::Full));
    let (tx, _rx) = tokio::sync::oneshot::channel();
    let mut guard = empty_inner();
    guard.sink_schedules.insert(sink_id, track);
    guard.manual_compaction_waiters.insert(sink_id, tx);

    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(1, 1),
            now,
            allow_track_initialization: false,
            loaded_config: Some(config),
        },
    );

    assert!(!applied);
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert!(matches!(
        track.state,
        CompactionTrackState::Idle {
            manual_task_type: Some(TaskType::Full),
            ..
        }
    ));
    assert_eq!(
        track.finish_action,
        CompactionTrackFinishAction::RemoveTrack
    );
    assert!(guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_apply_sink_update_rejects_temporary_manual_in_flight_attempt_without_config() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(149);
    let task_id = 8;
    let now = Instant::now();
    let mut track = new_track(now, 300, 3, 1);
    track.finish_action = CompactionTrackFinishAction::RemoveTrack;
    start_in_flight(&mut track, task_id, now);
    let mut guard = empty_inner();
    guard.sink_schedules.insert(sink_id, track);

    let applied = manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(1, 1),
            now,
            allow_track_initialization: false,
            loaded_config: None,
        },
    );

    assert!(!applied);
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 1);
    assert!(matches!(
        track.state,
        CompactionTrackState::InFlight { task_id, .. } if task_id.as_raw_id() == 8
    ));
}

#[tokio::test]
async fn test_apply_sink_update_promotes_temporary_manual_track_when_compaction_enabled() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(150);
    let task_id = 9;
    let now = Instant::now();
    let mut track = new_track(now, 300, 3, 1);
    track.finish_action = CompactionTrackFinishAction::RemoveTrack;
    start_in_flight(&mut track, task_id, now);
    let config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);

        let applied = manager.apply_sink_update(
            &mut guard,
            PreparedSinkUpdate {
                sink_id,
                kind: commit_update(1, 1),
                now,
                allow_track_initialization: false,
                loaded_config: Some(config),
            },
        );

        assert!(applied);
        let track = guard.sink_schedules.get(&sink_id).unwrap();
        assert_eq!(track.pending_commit_count, 2);
        assert_eq!(track.finish_action, CompactionTrackFinishAction::KeepTrack);
    }

    manager.handle_report_task(IcebergReportTask {
        task_id: task_id.into(),
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
        pk_index_result: None,
    });

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 1);
    assert_eq!(track.finish_action, CompactionTrackFinishAction::KeepTrack);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_apply_sink_update_does_not_resurrect_disappeared_track() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(45);
    let now = Instant::now();
    let config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
    let mut guard = empty_inner();

    manager.apply_sink_update(
        &mut guard,
        PreparedSinkUpdate {
            sink_id,
            kind: commit_update(1, 1),
            now,
            allow_track_initialization: false,
            loaded_config: Some(config),
        },
    );

    assert!(!guard.sink_schedules.contains_key(&sink_id));
}

#[tokio::test]
async fn test_cleared_pending_handle_does_not_dispatch_or_restore_track() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(246);
    let now = Instant::now();
    let mut receiver = manager.iceberg_compactor_manager.add_compactor(1.into());
    let compactor = manager
        .iceberg_compactor_manager
        .get_compactor(1.into())
        .unwrap();
    manager
        .inner
        .write()
        .sink_schedules
        .insert(sink_id, new_track(now, 120, 3, 3));

    let mut handle = manager.get_top_n_iceberg_commit_sink_ids(1).pop().unwrap();
    manager.clear_iceberg_maintenance_by_sink_id(sink_id);

    handle
        .try_dispatch_task(
            &compactor,
            IcebergCompactionTask {
                task_id: 99.into(),
                ..Default::default()
            },
        )
        .unwrap();
    assert!(matches!(
        receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    drop(handle);

    assert!(!manager.inner.read().sink_schedules.contains_key(&sink_id));
}

#[tokio::test]
async fn test_clear_iceberg_maintenance_cancels_in_flight_task() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(467);
    let task_id = 88;
    let compactor_context_id = 1000.into();
    let mut receiver = manager
        .iceberg_compactor_manager
        .add_compactor(compactor_context_id);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    start_in_flight_on(&mut track, task_id, compactor_context_id, now);
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.snapshot_expiration_sink_ids.insert(sink_id);
        guard.manifest_rewrite_sink_ids.insert(sink_id);
    }

    manager.clear_iceberg_maintenance_by_sink_id(sink_id);

    {
        let guard = manager.inner.read();
        assert!(!guard.sink_schedules.contains_key(&sink_id));
        assert!(!guard.snapshot_expiration_sink_ids.contains(&sink_id));
        assert!(!guard.manifest_rewrite_sink_ids.contains(&sink_id));
    }

    let event = receiver.try_recv().unwrap().unwrap().event.unwrap();
    match event {
        IcebergResponseEvent::CancelCompactTask(cancel_task) => {
            assert_eq!(cancel_task.task_id, task_id);
        }
        other => panic!("unexpected iceberg compactor event: {other:?}"),
    }
}

#[tokio::test]
async fn test_pre_dispatch_failure_notifies_manual_waiter_and_removes_temporary_track() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(461);
    let now = Instant::now();
    let mut track = new_track(now, 120, 3, 1);
    track.finish_action = CompactionTrackFinishAction::RemoveTrack;
    let attempt = track.start_attempt();
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    drop(IcebergCompactionHandle::new(
        sink_id,
        attempt,
        manager.inner.clone(),
        manager.metadata_manager.clone(),
    ));

    let error = rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("failed before dispatch"));
    let guard = manager.inner.read();
    assert!(!guard.sink_schedules.contains_key(&sink_id));
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_handle_report_task_success_consumes_backlog_and_resets_to_idle() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(47);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    start_in_flight(&mut track, 9, now);
    record_commits(&mut track, 3);
    manager.inner.write().sink_schedules.insert(sink_id, track);

    manager.handle_report_task(IcebergReportTask {
        task_id: 9.into(),
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
        pk_index_result: None,
    });

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 3);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_handle_report_task_drained_finishes_active_round() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(470);
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 1);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 9, now);
    track.record_observed_snapshot(committed_snapshot(11, 2000));
    track.record_commit();
    manager.inner.write().sink_schedules.insert(sink_id, track);

    manager.handle_report_task(IcebergReportTask {
        task_id: 9.into(),
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Drained as i32,
        error_message: None,
        pk_index_result: None,
    });

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.round_max_file_sequence_number, None);
    assert_eq!(track.pending_commit_count, 1);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_handle_report_task_completes_manual_waiter_on_success() {
    let manager = build_test_manager().await;
    let task_id = 42;
    let sink_id = SinkId::new(47);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    start_in_flight(&mut track, task_id, now);
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.handle_report_task(IcebergReportTask {
        task_id: task_id.into(),
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
        pk_index_result: None,
    });

    assert_eq!(rx.await.unwrap().unwrap(), task_id);
    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 0);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_handle_report_task_completes_manual_waiter_on_failure() {
    let manager = build_test_manager().await;
    let task_id = 43;
    let sink_id = SinkId::new(48);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    start_in_flight(&mut track, task_id, now);
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.handle_report_task(IcebergReportTask {
        task_id: task_id.into(),
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Failed as i32,
        error_message: Some("boom".to_owned()),
        pk_index_result: None,
    });

    let error = rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("boom"));
    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 2);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_handle_report_task_treats_unbounded_drained_as_failure() {
    let manager = build_test_manager().await;
    let task_id = 431;
    let sink_id = SinkId::new(481);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    start_in_flight(&mut track, task_id, now);
    manager.inner.write().sink_schedules.insert(sink_id, track);

    manager.handle_report_task(IcebergReportTask {
        task_id: task_id.into(),
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Drained as i32,
        error_message: None,
        pk_index_result: None,
    });

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 2);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_handle_report_task_removes_temporary_manual_track_on_success() {
    let manager = build_test_manager().await;
    let task_id = 44;
    let sink_id = SinkId::new(49);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.finish_action = CompactionTrackFinishAction::RemoveTrack;
    start_in_flight(&mut track, task_id, now);
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.handle_report_task(IcebergReportTask {
        task_id: task_id.into(),
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
        pk_index_result: None,
    });

    assert_eq!(rx.await.unwrap().unwrap(), task_id);
    let guard = manager.inner.read();
    assert!(!guard.sink_schedules.contains_key(&sink_id));
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_handle_report_task_removes_temporary_manual_track_on_failure() {
    let manager = build_test_manager().await;
    let task_id = 45;
    let sink_id = SinkId::new(149);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.finish_action = CompactionTrackFinishAction::RemoveTrack;
    start_in_flight(&mut track, task_id, now);
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.handle_report_task(IcebergReportTask {
        task_id: task_id.into(),
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Failed as i32,
        error_message: Some("boom".to_owned()),
        pk_index_result: None,
    });

    let error = rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("boom"));
    let guard = manager.inner.read();
    assert!(!guard.sink_schedules.contains_key(&sink_id));
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_cancel_manual_compaction_waiter_keeps_pending_track() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(51);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    track.start_attempt();
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.cancel_manual_compaction_waiter(sink_id);

    assert!(rx.await.is_err());
    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 2);
    assert!(matches!(
        track.state,
        CompactionTrackState::PendingDispatch { .. }
    ));
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_trigger_manual_compaction_does_not_register_waiter_during_config_load() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(511);
    let _compactor_rx = manager.iceberg_compactor_manager.add_compactor(1.into());
    let catalog_write_guard = manager
        .metadata_manager
        .catalog_controller
        .get_inner_write_guard()
        .await;

    let join_handle = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.trigger_manual_compaction(sink_id).await })
    };
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    {
        let guard = manager.inner.read();
        assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
        assert!(!guard.sink_schedules.contains_key(&sink_id));
    }

    join_handle.abort();
    assert!(join_handle.await.unwrap_err().is_cancelled());
    drop(catalog_write_guard);

    let guard = manager.inner.read();
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
    assert!(!guard.sink_schedules.contains_key(&sink_id));
}

#[tokio::test]
async fn test_manual_compaction_waiter_is_not_stolen_during_config_load() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(512);
    let task_id = 48;
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 1);
    track.last_config_refresh_at = now - manager.config_refresh_interval();
    manager.inner.write().sink_schedules.insert(sink_id, track);
    let catalog_write_guard = manager
        .metadata_manager
        .catalog_controller
        .get_inner_write_guard()
        .await;

    let join_handle = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.start_manual_compaction(sink_id).await })
    };
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    {
        let mut guard = manager.inner.write();
        let track = guard.sink_schedules.get_mut(&sink_id).unwrap();
        start_in_flight(track, task_id, now);
    }
    manager.handle_report_task(IcebergReportTask {
        task_id: task_id.into(),
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
        pk_index_result: None,
    });
    assert!(
        !manager
            .inner
            .read()
            .manual_compaction_waiters
            .contains_key(&sink_id)
    );

    drop(catalog_write_guard);
    let waiter = join_handle.await.unwrap().unwrap();
    assert!(
        manager
            .inner
            .read()
            .manual_compaction_waiters
            .contains_key(&sink_id)
    );
    manager.cancel_manual_compaction_waiter(sink_id);
    assert!(waiter.await.is_err());
}

#[tokio::test]
async fn test_get_top_n_notifies_manual_waiter_on_report_timeout() {
    let manager = build_test_manager().await;
    let task_id = 46;
    let sink_id = SinkId::new(52);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    track.report_timeout = Duration::from_secs(1);
    start_in_flight(&mut track, task_id, now - Duration::from_secs(5));
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);

    assert_eq!(handles.len(), 0);
    let error = rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("timed out"));
    let guard = manager.inner.read();
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_get_top_n_removes_temporary_manual_track_on_report_timeout() {
    let manager = build_test_manager().await;
    let task_id = 47;
    let sink_id = SinkId::new(53);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 0);
    track.finish_action = CompactionTrackFinishAction::RemoveTrack;
    track.report_timeout = Duration::from_secs(1);
    start_in_flight(&mut track, task_id, now - Duration::from_secs(5));
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);

    assert!(handles.is_empty());
    let error = rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("timed out"));
    let guard = manager.inner.read();
    assert!(!guard.sink_schedules.contains_key(&sink_id));
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_clear_iceberg_maintenance_notifies_manual_waiter() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(54);
    let now = Instant::now();
    let track = new_track(now, 120, 10, 1);
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.snapshot_expiration_sink_ids.insert(sink_id);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.clear_iceberg_maintenance_by_sink_id(sink_id);

    let error = rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("was cleared"));
    let guard = manager.inner.read();
    assert!(!guard.sink_schedules.contains_key(&sink_id));
    assert!(!guard.snapshot_expiration_sink_ids.contains(&sink_id));
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_trigger_manual_compaction_rejects_existing_task() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(50);
    let now = Instant::now();
    let _compactor_rx = manager.iceberg_compactor_manager.add_compactor(1.into());
    let mut track = new_track(now, 120, 10, 2);
    start_in_flight(&mut track, 9, now);
    manager.inner.write().sink_schedules.insert(sink_id, track);

    let error = manager
        .trigger_manual_compaction(sink_id)
        .await
        .unwrap_err();

    let error = error.to_string();
    assert!(error.contains("already running"));
    assert!(error.contains("state=in_flight"));
    assert!(error.contains("task_id=9"));
    let guard = manager.inner.read();
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
    assert!(matches!(
        guard.sink_schedules.get(&sink_id).unwrap().state,
        CompactionTrackState::InFlight { task_id, .. } if task_id.as_raw_id() == 9
    ));
}

#[tokio::test]
async fn test_trigger_manual_compaction_rejects_without_compactor() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(502);

    let error = manager
        .trigger_manual_compaction(sink_id)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("No iceberg compactor available"));
    let guard = manager.inner.read();
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
    assert!(!guard.sink_schedules.contains_key(&sink_id));
}

#[tokio::test]
async fn test_start_manual_compaction_rejects_existing_pending_task() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(501);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    track.start_attempt();
    manager.inner.write().sink_schedules.insert(sink_id, track);

    let error = manager.start_manual_compaction(sink_id).await.unwrap_err();

    let error = error.to_string();
    assert!(error.contains("already running"));
    assert!(error.contains("state=pending_dispatch"));
    let guard = manager.inner.read();
    assert!(!guard.manual_compaction_waiters.contains_key(&sink_id));
    assert!(matches!(
        guard.sink_schedules.get(&sink_id).unwrap().state,
        CompactionTrackState::PendingDispatch { .. }
    ));
}

#[tokio::test]
async fn test_handle_report_task_failure_preserves_backlog_and_resets_to_idle() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(471);
    let now = Instant::now();
    let mut track = new_track(now, 120, 10, 2);
    start_in_flight(&mut track, 9, now);
    record_commits(&mut track, 3);
    manager.inner.write().sink_schedules.insert(sink_id, track);

    manager.handle_report_task(IcebergReportTask {
        task_id: 9.into(),
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Failed as i32,
        error_message: Some("boom".to_owned()),
        pk_index_result: None,
    });

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 5);
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
}

#[tokio::test]
async fn test_handle_report_task_ignores_stale_task_id() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(48);
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 2);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 9, now);
    let (tx, _rx) = tokio::sync::oneshot::channel();
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(sink_id, track);
        guard.manual_compaction_waiters.insert(sink_id, tx);
    }

    manager.handle_report_task(IcebergReportTask {
        task_id: 10.into(),
        sink_id: sink_id.as_raw_id(),
        status: IcebergReportTaskStatus::Success as i32,
        error_message: None,
        pk_index_result: None,
    });

    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert_eq!(track.pending_commit_count, 0);
    assert_eq!(track.round_max_file_sequence_number, Some(10));
    assert!(matches!(
        track.state,
        CompactionTrackState::InFlight { task_id, .. } if task_id.as_raw_id() == 9
    ));
    assert!(guard.manual_compaction_waiters.contains_key(&sink_id));
}

#[tokio::test]
async fn test_get_top_n_backs_off_after_timed_out_in_flight_task() {
    let manager = build_test_manager().await;
    let sink_id = SinkId::new(49);
    let now = Instant::now();
    let mut track = new_round_track(now, 120, 10, 2);
    track.report_timeout = Duration::from_secs(1);
    track.record_observed_snapshot(committed_snapshot(10, 1000));
    start_in_flight(&mut track, 7, now - Duration::from_secs(5));
    manager.inner.write().sink_schedules.insert(sink_id, track);

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);

    assert_eq!(handles.len(), 0);
    let guard = manager.inner.read();
    let track = guard.sink_schedules.get(&sink_id).unwrap();
    assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
    assert_eq!(track.round_max_file_sequence_number, Some(10));
}

#[tokio::test]
async fn test_pre_dispatch_failure_requeues_track_behind_overdue_candidates() {
    let manager = build_test_manager().await;
    let failing = SinkId::new(480);
    let healthy = SinkId::new(481);
    let now = Instant::now();
    // The failing track is the most overdue, so it wins the only slot first.
    let mut failing_track = new_round_track(now, 120, 10, 1);
    failing_track.record_observed_snapshot(committed_snapshot(10, 1000));
    failing_track.state = CompactionTrackState::Idle {
        next_compaction_time: now - Duration::from_secs(100),
        manual_task_type: None,
    };
    let mut healthy_track = new_track(now, 120, 10, 1);
    healthy_track.state = CompactionTrackState::Idle {
        next_compaction_time: now - Duration::from_secs(50),
        manual_task_type: None,
    };
    {
        let mut guard = manager.inner.write();
        guard.sink_schedules.insert(failing, failing_track);
        guard.sink_schedules.insert(healthy, healthy_track);
    }

    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);
    assert_eq!(handles.len(), 1);
    assert_eq!(handles[0].sink_id, failing);
    // Dropping the handle simulates a pre-dispatch failure.
    drop(handles);

    let guard = manager.inner.read();
    assert_eq!(
        guard
            .sink_schedules
            .get(&failing)
            .and_then(|track| track.round_max_file_sequence_number),
        Some(10)
    );
    drop(guard);

    // The failing track is re-queued at the revert time, so the still-overdue
    // healthy track wins the next slot instead of being starved.
    let handles = manager.get_top_n_iceberg_commit_sink_ids(1);
    assert_eq!(handles.len(), 1);
    assert_eq!(handles[0].sink_id, healthy);
}
