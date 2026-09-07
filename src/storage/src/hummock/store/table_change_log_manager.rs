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

use std::future::Future;
use std::iter;
use std::pin::Pin;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::Shared;
use moka::sync::Cache;
use risingwave_common::id::TableId;
use risingwave_hummock_sdk::change_log::TableChangeLogs;
use risingwave_rpc_client::HummockMetaClient;

use crate::hummock::{HummockError, HummockResult};
use crate::monitor::HummockStateStoreMetrics;

type InflightResult = Shared<Pin<Box<dyn Future<Output = HummockResult<TableChangeLogs>> + Send>>>;

#[derive(Eq, Hash, PartialEq)]
struct CacheKey {
    table_id: TableId,
    epoch_range: (u64, u64),
    include_epoch_only: bool,
    limit: Option<u32>,
}

/// A naive cache to reduce number of RPC sent to meta node.
pub struct TableChangeLogManager {
    cache: Cache<CacheKey, InflightResult>,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    metrics: Arc<HummockStateStoreMetrics>,
}

impl TableChangeLogManager {
    pub fn new(
        capacity: u64,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        metrics: Arc<HummockStateStoreMetrics>,
    ) -> Self {
        let cache = Cache::builder().max_capacity(capacity).build();
        Self {
            cache,
            hummock_meta_client,
            metrics,
        }
    }

    async fn get_or_insert(
        &self,
        table_id: TableId,
        epoch_range: (u64, u64),
        include_epoch_only: bool,
        limit: Option<u32>,
        fetch: impl Future<Output = HummockResult<TableChangeLogs>> + Send + 'static,
    ) -> HummockResult<TableChangeLogs> {
        let entry = self
            .cache
            .entry(CacheKey {
                table_id,
                epoch_range,
                include_epoch_only,
                limit,
            })
            .or_insert_with_if(
                || fetch.boxed().shared(),
                |inflight| {
                    if let Some(result) = inflight.peek() {
                        return result.is_err();
                    }
                    false
                },
            );
        if entry.is_fresh() {
            self.metrics.table_change_log_cache_miss.inc();
        } else {
            self.metrics.table_change_log_cache_hit.inc();
        }
        entry.value().clone().await
    }

    /// Fetches table change logs for the given `table_id` and `epoch_range`.
    ///
    /// - If the end value of `epoch_range` is not `u64::MAX`, attempts to retrieve logs from the cache; if not cached, fetches via an RPC to the meta node and stores the result in the cache.
    /// - If the end value of `epoch_range` is `u64::MAX`, always fetches table change logs directly from the meta node (bypassing the cache).
    ///
    /// Both the start and end values of `epoch_range` are inclusive.
    ///
    /// IMPORTANT: The caller must guarantee that the current max committed epoch is at least as large as the end of the provided `epoch_range`, if it's not `u64::MAX`.
    /// Otherwise, the cache may serve outdated results: as new epochs are committed beyond the current maximum, subsequent RPC calls for the same `epoch_range`
    /// could retrieve different or additional change logs that were not present in the previously cached result. For example, if you request logs for
    /// `epoch_range = (1, N)` when the current max committed epoch is `M < N`, committing epochs `M+1` through `N` would make the cache inconsistent with reality;
    /// future fetches for `(1, N)` could return new or updated information absent from the previous cache entry.
    pub async fn fetch_table_change_logs(
        &self,
        table_id: TableId,
        epoch_range: (u64, u64),
        include_epoch_only: bool,
        limit: Option<u32>,
    ) -> HummockResult<TableChangeLogs> {
        let _timer = self.metrics.table_change_log_fetch_latency.start_timer();
        let hummock_meta_client = self.hummock_meta_client.clone();
        let fetch = async move {
            hummock_meta_client
                .get_table_change_logs(
                    include_epoch_only,
                    Some(epoch_range.0),
                    Some(epoch_range.1),
                    Some(iter::once(table_id).collect()),
                    false,
                    limit,
                )
                .await
                .map_err(HummockError::meta_error)
        };
        if epoch_range.1 == u64::MAX {
            return fetch.await;
        }
        self.get_or_insert(table_id, epoch_range, include_epoch_only, limit, fetch)
            .await
    }
}
