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

use std::collections::BTreeMap;
use std::iter::empty;
use std::ops::Deref;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use auto_enums::auto_enum;
use risingwave_common::catalog::TableId;
use risingwave_common::log::LogSuppressor;
use risingwave_common::util::retry::exponential_backoff;
use risingwave_hummock_sdk::level::{Level, Levels};
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{CompactionGroupId, HummockVersionId, INVALID_VERSION_ID};
use risingwave_rpc_client::HummockMetaClient;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_retry::strategy::jitter;

#[derive(Debug, Clone)]
pub enum PinVersionAction {
    Pin(HummockVersionId),
    Unpin(HummockVersionId),
}

struct PinnedVersionGuard {
    version_id: HummockVersionId,
    pinned_version_manager_tx: UnboundedSender<PinVersionAction>,
}

impl PinnedVersionGuard {
    /// Creates a new `PinnedVersionGuard` and send a pin request to `pinned_version_worker`.
    fn new(
        version_id: HummockVersionId,
        pinned_version_manager_tx: UnboundedSender<PinVersionAction>,
    ) -> Self {
        if pinned_version_manager_tx
            .send(PinVersionAction::Pin(version_id))
            .is_err()
        {
            tracing::warn!("failed to send req pin version id{}", version_id);
        }

        Self {
            version_id,
            pinned_version_manager_tx,
        }
    }
}

impl Drop for PinnedVersionGuard {
    fn drop(&mut self) {
        if self
            .pinned_version_manager_tx
            .send(PinVersionAction::Unpin(self.version_id))
            .is_err()
        {
            static LOG_SUPPRESSOR: LazyLock<LogSuppressor> =
                LazyLock::new(|| LogSuppressor::per_second(1));
            if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                tracing::warn!(
                    suppressed_count,
                    version_id = %self.version_id,
                    "failed to send req unpin"
                );
            }
        }
    }
}

#[derive(Clone)]
pub struct PinnedVersion {
    version: Arc<HummockVersion>,
    guard: Arc<PinnedVersionGuard>,
}

impl Deref for PinnedVersion {
    type Target = HummockVersion;

    fn deref(&self) -> &Self::Target {
        &self.version
    }
}

impl PinnedVersion {
    pub fn new(
        version: HummockVersion,
        pinned_version_manager_tx: UnboundedSender<PinVersionAction>,
    ) -> Self {
        let version_id = version.id;
        PinnedVersion {
            guard: Arc::new(PinnedVersionGuard::new(
                version_id,
                pinned_version_manager_tx,
            )),
            version: Arc::new(version),
        }
    }

    pub fn new_pin_version(&self, version: HummockVersion) -> Option<Self> {
        assert!(
            version.id >= self.version.id,
            "pinning a older version {}. Current is {}",
            version.id,
            self.version.id
        );
        if version.id == self.version.id {
            return None;
        }
        let version_id = version.id;
        Some(PinnedVersion {
            guard: Arc::new(PinnedVersionGuard::new(
                version_id,
                self.guard.pinned_version_manager_tx.clone(),
            )),
            version: Arc::new(version),
        })
    }

    /// Create a new `PinnedVersion` with the given version. Referring to the usage in the
    /// `hummock_event_handler`.
    pub fn new_with_local_version(&self, version: HummockVersion) -> Option<Self> {
        assert!(
            version.id >= self.version.id,
            "pinning a older version {}. Current is {}",
            version.id,
            self.version.id
        );
        if version.id == self.version.id {
            return None;
        }

        let version_id = version.id;

        Some(PinnedVersion {
            guard: Arc::new(PinnedVersionGuard::new(
                version_id,
                self.guard.pinned_version_manager_tx.clone(),
            )),
            version: Arc::new(version),
        })
    }

    pub fn id(&self) -> HummockVersionId {
        self.version.id
    }

    pub fn is_valid(&self) -> bool {
        self.version.id != INVALID_VERSION_ID
    }

    fn levels_by_compaction_groups_id(&self, compaction_group_id: CompactionGroupId) -> &Levels {
        self.version
            .levels
            .get(&compaction_group_id)
            .unwrap_or_else(|| {
                panic!(
                    "levels for compaction group {} not found in version {}",
                    compaction_group_id,
                    self.id()
                )
            })
    }

    pub fn levels(&self, table_id: TableId) -> impl Iterator<Item = &Level> {
        #[auto_enum(Iterator)]
        match self.version.state_table_info.info().get(&table_id) {
            Some(info) => {
                let compaction_group_id = info.compaction_group_id;
                let levels = self.levels_by_compaction_groups_id(compaction_group_id);
                levels
                    .l0
                    .sub_levels
                    .iter()
                    .rev()
                    .chain(levels.levels.iter())
            }
            None => empty(),
        }
    }
}

pub(crate) async fn start_pinned_version_worker(
    mut rx: UnboundedReceiver<PinVersionAction>,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    max_version_pinning_duration_sec: u64,
) {
    let min_execute_interval = Duration::from_millis(1000);
    let max_retry_interval = Duration::from_secs(10);
    let get_backoff_strategy =
        || exponential_backoff(Duration::from_millis(10), 10, max_retry_interval).map(jitter);
    let mut retry_backoff = get_backoff_strategy();
    let mut min_execute_interval_tick = tokio::time::interval(min_execute_interval);
    min_execute_interval_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut need_unpin = false;

    let mut version_ids_in_use: BTreeMap<HummockVersionId, (usize, Instant)> = BTreeMap::new();
    let max_version_pinning_duration_sec = Duration::from_secs(max_version_pinning_duration_sec);
    // For each run in the loop, accumulate versions to unpin and call unpin RPC once.
    loop {
        min_execute_interval_tick.tick().await;
        // 0. Expire versions.
        while version_ids_in_use.len() > 1
            && let Some(e) = version_ids_in_use.first_entry()
        {
            if e.get().1.elapsed() < max_version_pinning_duration_sec {
                break;
            }
            need_unpin = true;
            e.remove();
        }

        // 1. Collect new versions to unpin.
        let mut versions_to_unpin = vec![];
        let inst = Instant::now();
        'collect: loop {
            match rx.try_recv() {
                Ok(version_action) => match version_action {
                    PinVersionAction::Pin(version_id) => {
                        version_ids_in_use
                            .entry(version_id)
                            .and_modify(|e| {
                                e.0 += 1;
                                e.1 = inst;
                            })
                            .or_insert((1, inst));
                    }
                    PinVersionAction::Unpin(version_id) => {
                        versions_to_unpin.push(version_id);
                    }
                },
                Err(err) => match err {
                    TryRecvError::Empty => {
                        break 'collect;
                    }
                    TryRecvError::Disconnected => {
                        tracing::info!("Shutdown hummock unpin worker");
                        return;
                    }
                },
            }
        }
        if !versions_to_unpin.is_empty() {
            need_unpin = true;
        }
        if !need_unpin {
            continue;
        }

        for version in &versions_to_unpin {
            match version_ids_in_use.get_mut(version) {
                Some((counter, _)) => {
                    *counter -= 1;
                    if *counter == 0 {
                        version_ids_in_use.remove(version);
                    }
                }
                None => tracing::warn!(
                    "version {} to unpin does not exist, may already be unpinned due to expiration",
                    version
                ),
            }
        }

        match version_ids_in_use.first_entry() {
            Some(unpin_before) => {
                // 2. Call unpin RPC, including versions failed to unpin in previous RPC calls.
                match hummock_meta_client
                    .unpin_version_before(*unpin_before.key())
                    .await
                {
                    Ok(_) => {
                        versions_to_unpin.clear();
                        need_unpin = false;
                        retry_backoff = get_backoff_strategy();
                    }
                    Err(err) => {
                        let retry_after = retry_backoff.next().unwrap_or(max_retry_interval);
                        tracing::warn!(
                            error = %err.as_report(),
                            "Failed to unpin version. Will retry after about {} milliseconds",
                            retry_after.as_millis()
                        );
                        tokio::time::sleep(retry_after).await;
                    }
                }
            }
            None => tracing::warn!("version_ids_in_use is empty!"),
        }
    }
}
