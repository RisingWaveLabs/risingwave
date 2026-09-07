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

//! Per-`PartialGraphId` committed-epoch tracking for the iceberg pk-index sink.
//!
//! Tracks, per partial graph, the latest checkpoint epoch whose iceberg commit has completed. A
//! cursor entry exists exactly while a partial graph has a registered pk-index sink:
//! [`PartialGraphCommittedEpochs::ensure`] is called from `register_sink` and
//! [`PartialGraphCommittedEpochs::remove`] from unregister. Both [`PartialGraphCommittedEpochs::advance_all`]
//! and [`PartialGraphCommittedEpochs::wait`] are non-creating, so a partial graph without a sink never
//! accumulates an entry, and a `wait` that races an unregister resolves immediately (the caller then
//! observes the coordinator is gone and errors out) rather than blocking on a resurrected entry that
//! would never advance.
//!
//! The key is a `PartialGraphId` rather than a `DatabaseId` so that a sink running in an independent
//! partial graph (e.g. a batch-refresh job, `to_partial_graph_id(database_id, Some(job_id))`) is tracked
//! by the very same mechanism: the barrier-completion path advances every completed partial graph, so no
//! change here is needed to support such jobs. For a normal sink the key is its database's main graph
//! (`to_partial_graph_id(database_id, None)`).
//!
//! The barrier-completion path advances the cursor on **every** checkpoint completion (even epochs where
//! a sink reported nothing), so a merger's seed wait converges even on idle tables. Waiters block on a
//! `tokio::sync::watch` receiver and never hold any other lock.
//!
//! Note: this is unrelated to RisingWave's stream watermark abstraction; it is purely a per-partial-graph
//! "iceberg commit has progressed to epoch N" cursor.

use std::collections::HashMap;
use std::future::Future;

use parking_lot::Mutex;
use risingwave_common::id::PartialGraphId;
use tokio::sync::watch;

/// Per-`PartialGraphId` monotonic committed-epoch cursor, backed by one `watch` channel per tracked
/// partial graph.
#[derive(Default)]
pub struct PartialGraphCommittedEpochs {
    inner: Mutex<HashMap<PartialGraphId, watch::Sender<u64>>>,
}

impl PartialGraphCommittedEpochs {
    /// Start tracking `partial_graph_id` (initialized to 0) if not already tracked.
    pub fn ensure(&self, partial_graph_id: PartialGraphId) {
        self.inner
            .lock()
            .entry(partial_graph_id)
            .or_insert_with(|| watch::channel(0).0);
    }

    /// Advance each `(partial_graph_id, epoch)`'s committed epoch to `max(current, epoch)`
    pub fn advance_all(&self, epochs: impl IntoIterator<Item = (PartialGraphId, u64)>) {
        let map = self.inner.lock();
        for (partial_graph_id, epoch) in epochs {
            if let Some(sender) = map.get(&partial_graph_id) {
                sender.send_if_modified(|cur| {
                    if epoch > *cur {
                        *cur = epoch;
                        true
                    } else {
                        false
                    }
                });
            }
        }
    }

    /// Advance a single partial graph's committed epoch (test-only convenience over `advance_all`).
    #[cfg(test)]
    fn advance(&self, partial_graph_id: PartialGraphId, epoch: u64) {
        self.advance_all([(partial_graph_id, epoch)]);
    }

    /// Resolve once `partial_graph_id`'s committed epoch is `>= target`. If the partial graph is not
    /// tracked (never registered, or unregistered while this call raced), resolve immediately — the caller
    /// then observes the coordinator is gone and errors out, instead of blocking forever on an entry that
    /// nothing would advance.
    ///
    /// Returns an owned (`'static`) future rather than an `async fn` borrowing `&self`, so callers can
    /// `tokio::spawn` the wait without holding a borrow of `PartialGraphCommittedEpochs` for the lifetime
    /// of the wait. The subscription is taken eagerly (before returning); the wait holds no lock.
    pub fn wait(
        &self,
        partial_graph_id: PartialGraphId,
        target: u64,
    ) -> impl Future<Output = ()> + 'static {
        let rx = self
            .inner
            .lock()
            .get(&partial_graph_id)
            .map(|s| s.subscribe());
        async move {
            let Some(mut rx) = rx else {
                // Not tracked (unregistered / never registered): do not block.
                return;
            };
            while *rx.borrow_and_update() < target {
                if rx.changed().await.is_err() {
                    // Sender dropped (partial graph removed). Nothing more will advance it; stop waiting.
                    return;
                }
            }
        }
    }

    /// Stop tracking one partial graph (unregister of its last sink).
    pub fn remove(&self, partial_graph_id: PartialGraphId) {
        self.inner.lock().remove(&partial_graph_id);
    }

    /// Stop tracking every partial graph (global recovery reset).
    pub fn clear(&self) {
        self.inner.lock().clear();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn pg(id: u64) -> PartialGraphId {
        PartialGraphId::new(id)
    }

    #[tokio::test]
    async fn wait_returns_immediately_when_already_reached() {
        let epochs = PartialGraphCommittedEpochs::default();
        epochs.ensure(pg(1));
        epochs.advance(pg(1), 100);
        // Already >= target: must resolve without an advance.
        tokio::time::timeout(Duration::from_secs(1), epochs.wait(pg(1), 50))
            .await
            .expect("wait should resolve immediately");
    }

    #[tokio::test]
    async fn wait_unblocks_on_later_advance() {
        let epochs = PartialGraphCommittedEpochs::default();
        epochs.ensure(pg(1));
        let handle = tokio::spawn(epochs.wait(pg(1), 100));
        // Not yet reached.
        assert!(!handle.is_finished());
        epochs.advance(pg(1), 100);
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("wait should unblock after advance")
            .expect("task ok");
    }

    #[tokio::test]
    async fn advance_is_monotonic_max() {
        let epochs = PartialGraphCommittedEpochs::default();
        epochs.ensure(pg(1));
        epochs.advance(pg(1), 100);
        epochs.advance(pg(1), 50); // must not regress
        tokio::time::timeout(Duration::from_secs(1), epochs.wait(pg(1), 100))
            .await
            .expect("committed epoch must not regress below 100");
    }

    #[tokio::test]
    async fn partial_graphs_are_independent() {
        let epochs = PartialGraphCommittedEpochs::default();
        epochs.ensure(pg(1));
        epochs.ensure(pg(2));
        epochs.advance(pg(1), 100);
        // pg(2) is tracked but has not advanced; a target on pg(2) must still be pending.
        let pending =
            tokio::time::timeout(Duration::from_millis(200), epochs.wait(pg(2), 10)).await;
        assert!(
            pending.is_err(),
            "pg(2) committed epoch should not be satisfied by pg(1)"
        );
    }

    #[tokio::test]
    async fn wait_on_untracked_graph_resolves_immediately() {
        let epochs = PartialGraphCommittedEpochs::default();
        // No `ensure`: a wait racing an unregister (or on a never-registered graph) must not block.
        tokio::time::timeout(Duration::from_secs(1), epochs.wait(pg(9), 100))
            .await
            .expect("wait on an untracked partial graph must not block");
    }

    #[tokio::test]
    async fn advance_before_ensure_is_noop() {
        let epochs = PartialGraphCommittedEpochs::default();
        epochs.advance(pg(9), 100); // untracked -> no-op, must not create an entry at 100
        epochs.ensure(pg(9)); // now tracked, freshly at 0
        let pending =
            tokio::time::timeout(Duration::from_millis(200), epochs.wait(pg(9), 50)).await;
        assert!(
            pending.is_err(),
            "advance on an untracked partial graph must not create/set an entry"
        );
    }
}
