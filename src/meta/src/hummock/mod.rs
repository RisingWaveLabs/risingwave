// Copyright 2022 RisingWave Labs
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

pub mod compaction;
pub mod compactor_manager;
pub mod error;
mod manager;
pub(crate) use manager::checkpoint::{compress_payload, xxhash64_checksum};
pub use manager::*;
use thiserror_ext::AsReport;

mod level_handler;
mod metrics_utils;
#[cfg(any(test, feature = "test"))]
pub mod mock_hummock_meta_client;
pub mod model;
pub mod test_utils;
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use std::time::Duration;

pub use compactor_manager::*;
use futures::future::BoxFuture;
#[cfg(any(test, feature = "test"))]
pub use mock_hummock_meta_client::MockHummockMetaClient;
use risingwave_common::catalog::TableId;
use tokio::runtime::Runtime;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use crate::MetaOpts;
use crate::backup_restore::BackupManagerRef;
use crate::controller::streaming_job::TableChangeLogTruncateInfo;

type PinnedSnapshotEpochsFetcher =
    Box<dyn Fn() -> BoxFuture<'static, Option<HashMap<TableId, HashSet<u64>>>> + Send>;
type TableChangeLogTruncateInfoFetcher =
    Box<dyn Fn() -> BoxFuture<'static, Option<TableChangeLogTruncateInfo>> + Send>;

// Building and compressing a large checkpoint can spend a long time without yielding. Keep that
// work off the main meta runtime so it cannot monopolize one of its worker threads.
static VERSION_CHECKPOINT_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("rw-version-checkpoint")
        .enable_all()
        .build()
        .expect("failed to build hummock version checkpoint runtime")
});

/// Start hummock's asynchronous tasks.
pub fn start_hummock_workers(
    hummock_manager: HummockManagerRef,
    backup_manager: BackupManagerRef,
    meta_opts: &MetaOpts,
    get_table_change_log_truncate_info: TableChangeLogTruncateInfoFetcher,
    get_pinned_snapshot_epochs: PinnedSnapshotEpochsFetcher,
) -> Vec<(JoinHandle<()>, Sender<()>)> {
    // These critical tasks are put in their own timer loop deliberately, to avoid long-running ones
    // from blocking others.
    let workers = vec![
        start_checkpoint_loop(
            hummock_manager.clone(),
            backup_manager,
            Duration::from_secs(meta_opts.hummock_version_checkpoint_interval_sec),
            meta_opts.min_delta_log_num_for_hummock_version_checkpoint,
        ),
        start_vacuum_metadata_loop(
            hummock_manager.clone(),
            Duration::from_secs(meta_opts.vacuum_interval_sec),
        ),
        start_truncate_table_change_log_loop(
            hummock_manager.clone(),
            Duration::from_secs(meta_opts.table_change_log_truncate_interval_sec),
            get_table_change_log_truncate_info,
        ),
        start_vacuum_time_travel_metadata_loop(
            hummock_manager,
            Duration::from_secs(meta_opts.time_travel_vacuum_interval_sec),
            get_pinned_snapshot_epochs,
        ),
    ];
    workers
}

pub fn start_truncate_table_change_log_loop(
    hummock_manager: HummockManagerRef,
    interval: Duration,
    get_truncate_info: TableChangeLogTruncateInfoFetcher,
) -> (JoinHandle<()>, Sender<()>) {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let mut min_trigger_interval = tokio::time::interval(interval);
        min_trigger_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = min_trigger_interval.tick() => {},
                _ = &mut shutdown_rx => {
                    tracing::info!("Table change log truncate loop is stopped");
                    return;
                }
            }
            let Some(truncate_info) = get_truncate_info().await else {
                tracing::warn!(
                    "table change log truncation paused because retention metadata is unavailable"
                );
                continue;
            };
            if let Err(err) = hummock_manager
                .truncate_table_change_log(truncate_info)
                .await
            {
                tracing::warn!(error = %err.as_report(), "Table change log truncation error");
            }
        }
    });
    (join_handle, shutdown_tx)
}

/// Starts a task to periodically vacuum stale metadata.
pub fn start_vacuum_metadata_loop(
    hummock_manager: HummockManagerRef,
    interval: Duration,
) -> (JoinHandle<()>, Sender<()>) {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let mut min_trigger_interval = tokio::time::interval(interval);
        min_trigger_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // Wait for interval
                _ = min_trigger_interval.tick() => {},
                // Shutdown vacuum
                _ = &mut shutdown_rx => {
                    tracing::info!("Vacuum metadata loop is stopped");
                    return;
                }
            }
            if let Err(err) = hummock_manager.delete_version_deltas().await {
                tracing::warn!(error = %err.as_report(), "Vacuum metadata error");
            }
        }
    });
    (join_handle, shutdown_tx)
}

pub fn start_vacuum_time_travel_metadata_loop(
    hummock_manager: HummockManagerRef,
    interval: Duration,
    get_pinned_snapshot_epochs: PinnedSnapshotEpochsFetcher,
) -> (JoinHandle<()>, Sender<()>) {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let mut min_trigger_interval = tokio::time::interval(interval);
        min_trigger_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // Wait for interval
                _ = min_trigger_interval.tick() => {},
                // Shutdown vacuum
                _ = &mut shutdown_rx => {
                    tracing::info!("Vacuum time travel metadata loop is stopped");
                    return;
                }
            }
            let Some(pinned_snapshot_epochs) = get_pinned_snapshot_epochs().await else {
                tracing::warn!(
                    "time travel vacuum paused because pinned snapshots are unavailable"
                );
                continue;
            };
            if let Err(err) = hummock_manager
                .delete_time_travel_metadata(pinned_snapshot_epochs)
                .await
            {
                tracing::warn!(error = %err.as_report(), "Vacuum time travel metadata error");
            }
        }
    });
    (join_handle, shutdown_tx)
}

pub fn start_checkpoint_loop(
    hummock_manager: HummockManagerRef,
    backup_manager: BackupManagerRef,
    interval: Duration,
    min_delta_log_num: u64,
) -> (JoinHandle<()>, Sender<()>) {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let mut min_trigger_interval = tokio::time::interval(interval);
        min_trigger_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // Wait for interval
                _ = min_trigger_interval.tick() => {},
                // Shutdown checkpoint
                _ = &mut shutdown_rx => {
                    tracing::info!("Hummock version checkpoint is stopped");
                    return;
                }
            }
            if hummock_manager.is_version_checkpoint_paused()
                || hummock_manager.env.opts.compaction_deterministic_test
            {
                continue;
            }
            let checkpoint_manager = hummock_manager.clone();
            match VERSION_CHECKPOINT_RUNTIME
                .spawn(async move {
                    checkpoint_manager
                        .create_version_checkpoint(min_delta_log_num)
                        .await
                })
                .await
            {
                Ok(Err(err)) => {
                    tracing::warn!(error = %err.as_report(), "Hummock version checkpoint error.");
                }
                Err(err) => {
                    tracing::warn!(error = %err.as_report(), "Hummock version checkpoint task failed.");
                }
                Ok(Ok(_)) => {
                    let backup_manager_2 = backup_manager.clone();
                    let hummock_manager_2 = hummock_manager.clone();
                    tokio::task::spawn(async move {
                        let _ = hummock_manager_2
                            .try_start_minor_gc(backup_manager_2)
                            .await
                            .inspect_err(|err| {
                                tracing::warn!(error = %err.as_report(), "Hummock minor GC error.");
                            });
                    });
                }
            }
        }
    });
    (join_handle, shutdown_tx)
}
