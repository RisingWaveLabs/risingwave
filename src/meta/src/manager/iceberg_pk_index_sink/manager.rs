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

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use parking_lot::RwLock;
use risingwave_common::id::PartialGraphId;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_pb::stream_service::barrier_complete_response::IcebergPkIndexSinkMetadata as PbIcebergPkIndexSinkMetadata;
use sea_orm::DatabaseConnection;
use tokio::sync::Mutex;
use tracing::warn;

use super::committed_epoch::PartialGraphCommittedEpochs;
use super::coordinator::IcebergPkIndexSinkCoordinator;

type CoordinatorRef = Arc<Mutex<IcebergPkIndexSinkCoordinator>>;

/// Manager for the Iceberg pk-index sink path, cheap to clone.
#[derive(Clone)]
pub struct IcebergPkIndexSinkManager {
    inner: Arc<ManagerInner>,
}

struct ManagerInner {
    db: DatabaseConnection,
    /// `sink_id -> (partial_graph_id, coordinator)`. Read on every pre-commit/commit/wait (only to
    /// clone out the ref / read the partial graph id, never held across an await); written only by
    /// register/unregister/reset, which are rare control-plane events. The `partial_graph_id` is the
    /// graph whose committed epoch the sink's merger waits on (its database main graph today; an
    /// independent job's graph in the future — see `committed_epoch`).
    coordinators: RwLock<HashMap<SinkId, (PartialGraphId, CoordinatorRef)>>,
    /// Per-partial-graph committed-epoch cursor. A cursor entry exists exactly while a partial graph has
    /// a registered pk-index sink: created by `ensure` in `register_sink` and dropped by `remove` in
    /// `unregister_sinks` (and `clear` in `reset`), all under the `coordinators` write lock. Advanced on
    /// every checkpoint completion via `advance_committed_epochs` (a no-op for partial graphs with no
    /// registered sink).
    committed_epochs: PartialGraphCommittedEpochs,
}

impl IcebergPkIndexSinkManager {
    pub fn new(db: DatabaseConnection) -> Self {
        IcebergPkIndexSinkManager {
            inner: Arc::new(ManagerInner {
                db,
                coordinators: RwLock::new(HashMap::new()),
                committed_epochs: PartialGraphCommittedEpochs::default(),
            }),
        }
    }

    /// Register an Iceberg pk-index sink so its commit coordinator is ready to receive epoch reports. Builds and
    /// fully initializes the coordinator (loading the iceberg table and draining any recovered pending
    /// commits) BEFORE inserting it, so a successful return means the sink is ready to serve. Idempotent:
    /// registering the same `sink_id` replaces the existing coordinator.
    pub async fn register_sink(
        &self,
        sink_id: SinkId,
        partial_graph_id: PartialGraphId,
        iceberg_config: IcebergConfig,
    ) -> anyhow::Result<()> {
        // Initialize (load + recover + drain) outside the map lock; this is the slow, fallible part.
        let coordinator =
            IcebergPkIndexSinkCoordinator::init(sink_id, iceberg_config, self.inner.db.clone())
                .await?;

        let prev = {
            let mut coordinators = self.inner.coordinators.write();
            let prev = coordinators.insert(
                sink_id,
                (partial_graph_id, Arc::new(Mutex::new(coordinator))),
            );
            // Start tracking this partial graph's committed epoch under the same write lock as the
            // coordinator insert, so a cursor entry exists exactly while a sink is registered (and a
            // `wait` racing an unregister can never observe a resurrected, never-advanced entry).
            self.inner.committed_epochs.ensure(partial_graph_id);
            prev
        };
        if prev.is_some() {
            // Replacing an existing coordinator. Any in-flight commit on the old one keeps it alive via its
            // own `Arc` until it finishes; the snapshot_id idempotency check guards against double-commit.
            warn!(%sink_id, "iceberg pk-index sink coordinator re-registered; replacing previous instance");
        }
        Ok(())
    }

    /// Pre-commit phase for one epoch: persist the merged report under `pending_sink_state` (no iceberg IO).
    /// The barrier-complete path awaits this BEFORE issuing hummock `commit_epoch`.
    pub async fn pre_commit_epoch(
        &self,
        sink_id: SinkId,
        prev_epoch: u64,
        reports: Vec<PbIcebergPkIndexSinkMetadata>,
    ) -> anyhow::Result<()> {
        let coordinator = self.coordinator(sink_id)?;
        coordinator
            .lock()
            .await
            .pre_commit(prev_epoch, reports)
            .await
    }

    /// Commit phase for one epoch: run an iceberg `overwrite_files` transaction and mark its pending row
    /// committed. The barrier-complete path awaits this AFTER hummock `commit_epoch`.
    pub async fn commit_epoch(&self, sink_id: SinkId) -> anyhow::Result<()> {
        let coordinator = self.coordinator(sink_id)?;
        coordinator.lock().await.commit().await
    }

    /// Advance the per-partial-graph committed epoch after a checkpoint completion.
    pub fn advance_committed_epochs(
        &self,
        epochs: impl IntoIterator<Item = (PartialGraphId, u64)>,
    ) {
        self.inner.committed_epochs.advance_all(epochs);
    }

    /// Block until `sink_id`'s partial graph has committed through `target_epoch`, then return the
    /// coordinator's committed iceberg snapshot id (a lower bound the caller must observe when it
    /// reloads the table). Does NOT hold the coordinator lock while waiting.
    pub async fn wait_epoch(
        &self,
        sink_id: SinkId,
        target_epoch: u64,
    ) -> anyhow::Result<Option<i64>> {
        let partial_graph_id = self.partial_graph_of(sink_id)?;
        self.inner
            .committed_epochs
            .wait(partial_graph_id, target_epoch)
            .await;
        let coordinator = self.coordinator(sink_id)?;
        let snapshot_id = coordinator.lock().await.current_snapshot_id();
        Ok(snapshot_id)
    }

    /// Unregister the given `sink_id`(s)' coordinator(s) (e.g. at DROP SINK time). Unregistering an unknown
    /// `sink_id` is a no-op.
    pub fn unregister_sinks(&self, sink_ids: Vec<SinkId>) {
        let mut coordinators = self.inner.coordinators.write();
        let mut touched_graphs = Vec::new();
        for sink_id in sink_ids {
            if let Some((partial_graph_id, _coord)) = coordinators.remove(&sink_id) {
                touched_graphs.push(partial_graph_id);
            }
        }
        // Drop a partial graph's committed-epoch cursor only once none of its sinks remain registered.
        for partial_graph_id in touched_graphs {
            if !coordinators.values().any(|(pg, _)| *pg == partial_graph_id) {
                self.inner.committed_epochs.remove(partial_graph_id);
            }
        }
    }

    /// Drop every coordinator. Used at recovery time.
    pub fn reset(&self) {
        let mut coordinators = self.inner.coordinators.write();
        coordinators.clear();
        self.inner.committed_epochs.clear();
    }

    fn coordinator(&self, sink_id: SinkId) -> anyhow::Result<CoordinatorRef> {
        self.inner
            .coordinators
            .read()
            .get(&sink_id)
            .map(|(_pg, coord)| coord.clone())
            .ok_or_else(|| {
                anyhow!(
                    "iceberg pk-index sink coordinator for sink {} is not registered",
                    sink_id
                )
            })
    }

    fn partial_graph_of(&self, sink_id: SinkId) -> anyhow::Result<PartialGraphId> {
        self.inner
            .coordinators
            .read()
            .get(&sink_id)
            .map(|(pg, _coord)| *pg)
            .ok_or_else(|| {
                anyhow!(
                    "iceberg pk-index sink coordinator for sink {} is not registered",
                    sink_id
                )
            })
    }
}
