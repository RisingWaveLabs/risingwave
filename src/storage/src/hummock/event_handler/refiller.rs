// Copyright 2023 RisingWave Labs
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

use std::collections::hash_map::HashMap;
use std::collections::{HashSet, VecDeque};
use std::future::poll_fn;
use std::hash::Hash;
use std::ops::{Bound, Range};
use std::sync::{Arc, LazyLock};
use std::task::Poll;
use std::time::{Duration, Instant};

use foyer::RangeBoundsExt;
use futures::future::{join_all, try_join_all};
use futures::{Future, FutureExt};
use itertools::Itertools;
use prometheus::core::{AtomicU64, GenericCounter, GenericCounterVec};
use prometheus::{
    Histogram, HistogramVec, IntGauge, Registry, register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry, register_int_gauge_with_registry,
};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::Role;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_common::hash::VirtualNode;
use risingwave_common::license::Feature;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::key::{FullKey, vnode_range};
use risingwave_hummock_sdk::{HummockSstableObjectId, KeyComparator};
use risingwave_pb::id::TableId;
use thiserror_ext::AsReport;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::sstable::SstableMetaHandle;
use crate::hummock::{
    Block, HummockError, HummockResult, RecentFilterTrait, Sstable, SstableBlockIndex,
    SstableStoreRef, TableHolder,
};
use crate::monitor::StoreLocalStatistic;
use crate::opts::StorageOpts;

pub static GLOBAL_CACHE_REFILL_METRICS: LazyLock<CacheRefillMetrics> =
    LazyLock::new(|| CacheRefillMetrics::new(&GLOBAL_METRICS_REGISTRY));

pub struct CacheRefillMetrics {
    pub refill_duration: HistogramVec,
    pub refill_total: GenericCounterVec<AtomicU64>,
    pub refill_bytes: GenericCounterVec<AtomicU64>,

    pub data_refill_success_duration: Histogram,
    pub meta_refill_success_duration: Histogram,

    pub data_refill_filtered_total: GenericCounter<AtomicU64>,
    pub data_refill_attempts_total: GenericCounter<AtomicU64>,
    pub data_refill_started_total: GenericCounter<AtomicU64>,
    pub meta_refill_attempts_total: GenericCounter<AtomicU64>,

    pub data_refill_parent_meta_lookup_hit_total: GenericCounter<AtomicU64>,
    pub data_refill_parent_meta_lookup_miss_total: GenericCounter<AtomicU64>,
    pub data_refill_unit_inheritance_hit_total: GenericCounter<AtomicU64>,
    pub data_refill_unit_inheritance_miss_total: GenericCounter<AtomicU64>,

    pub data_refill_block_unfiltered_total: GenericCounter<AtomicU64>,
    pub data_refill_block_success_total: GenericCounter<AtomicU64>,

    pub data_refill_ideal_bytes: GenericCounter<AtomicU64>,
    pub data_refill_success_bytes: GenericCounter<AtomicU64>,

    pub refill_queue_total: IntGauge,
}

impl CacheRefillMetrics {
    pub fn new(registry: &Registry) -> Self {
        let refill_duration = register_histogram_vec_with_registry!(
            "refill_duration",
            "refill duration",
            &["type", "op"],
            registry,
        )
        .unwrap();
        let refill_total = register_int_counter_vec_with_registry!(
            "refill_total",
            "refill total",
            &["type", "op"],
            registry,
        )
        .unwrap();
        let refill_bytes = register_int_counter_vec_with_registry!(
            "refill_bytes",
            "refill bytes",
            &["type", "op"],
            registry,
        )
        .unwrap();

        let data_refill_success_duration = refill_duration
            .get_metric_with_label_values(&["data", "success"])
            .unwrap();
        let meta_refill_success_duration = refill_duration
            .get_metric_with_label_values(&["meta", "success"])
            .unwrap();

        let data_refill_filtered_total = refill_total
            .get_metric_with_label_values(&["data", "filtered"])
            .unwrap();
        let data_refill_attempts_total = refill_total
            .get_metric_with_label_values(&["data", "attempts"])
            .unwrap();
        let data_refill_started_total = refill_total
            .get_metric_with_label_values(&["data", "started"])
            .unwrap();
        let meta_refill_attempts_total = refill_total
            .get_metric_with_label_values(&["meta", "attempts"])
            .unwrap();

        let data_refill_parent_meta_lookup_hit_total = refill_total
            .get_metric_with_label_values(&["parent_meta", "hit"])
            .unwrap();
        let data_refill_parent_meta_lookup_miss_total = refill_total
            .get_metric_with_label_values(&["parent_meta", "miss"])
            .unwrap();
        let data_refill_unit_inheritance_hit_total = refill_total
            .get_metric_with_label_values(&["unit_inheritance", "hit"])
            .unwrap();
        let data_refill_unit_inheritance_miss_total = refill_total
            .get_metric_with_label_values(&["unit_inheritance", "miss"])
            .unwrap();

        let data_refill_block_unfiltered_total = refill_total
            .get_metric_with_label_values(&["block", "unfiltered"])
            .unwrap();
        let data_refill_block_success_total = refill_total
            .get_metric_with_label_values(&["block", "success"])
            .unwrap();

        let data_refill_ideal_bytes = refill_bytes
            .get_metric_with_label_values(&["data", "ideal"])
            .unwrap();
        let data_refill_success_bytes = refill_bytes
            .get_metric_with_label_values(&["data", "success"])
            .unwrap();

        let refill_queue_total = register_int_gauge_with_registry!(
            "refill_queue_total",
            "refill queue total",
            registry,
        )
        .unwrap();

        Self {
            refill_duration,
            refill_total,
            refill_bytes,

            data_refill_success_duration,
            meta_refill_success_duration,
            data_refill_filtered_total,
            data_refill_attempts_total,
            data_refill_started_total,
            meta_refill_attempts_total,

            data_refill_parent_meta_lookup_hit_total,
            data_refill_parent_meta_lookup_miss_total,
            data_refill_unit_inheritance_hit_total,
            data_refill_unit_inheritance_miss_total,

            data_refill_block_unfiltered_total,
            data_refill_block_success_total,

            data_refill_ideal_bytes,
            data_refill_success_bytes,

            refill_queue_total,
        }
    }
}

#[derive(Debug)]
pub struct CacheRefillConfig {
    /// Cache refill timeout.
    pub timeout: Duration,

    /// Data file cache refill levels.
    pub data_refill_levels: HashSet<u32>,

    /// Meta file cache refill concurrency.
    pub meta_refill_concurrency: usize,

    /// Data file cache refill concurrency.
    pub concurrency: usize,

    /// Data file cache refill unit (blocks).
    pub unit: usize,

    /// Data file cache reill unit threshold.
    ///
    /// Only units whose admit rate > threshold will be refilled.
    pub threshold: f64,

    /// Skip recent filter.
    pub skip_recent_filter: bool,

    /// Skip inheritance filter.
    pub skip_inheritance_filter: bool,

    /// Default table cache refill policy.
    pub table_cache_refill_default_policy: CacheRefillPolicy,
}

impl CacheRefillConfig {
    pub fn from_storage_opts(options: &StorageOpts) -> Self {
        let data_refill_levels = match Feature::ElasticDiskCache.check_available() {
            Ok(_) => options
                .cache_refill_data_refill_levels
                .iter()
                .copied()
                .collect(),
            Err(e) => {
                tracing::warn!(error = %e.as_report(), "ElasticDiskCache is not available.");
                HashSet::new()
            }
        };

        Self {
            timeout: Duration::from_millis(options.cache_refill_timeout_ms),
            data_refill_levels,
            concurrency: options.cache_refill_concurrency,
            meta_refill_concurrency: options.cache_refill_meta_refill_concurrency,
            unit: options.cache_refill_unit,
            threshold: options.cache_refill_threshold,
            skip_recent_filter: options.cache_refill_skip_recent_filter,
            skip_inheritance_filter: options.cache_refill_skip_inheritance_filter,
            table_cache_refill_default_policy: options
                .cache_refill_table_cache_refill_default_policy,
        }
    }
}

struct Item {
    handle: JoinHandle<()>,
    event: CacheRefillerEvent,
}

pub(crate) type SpawnRefillTask = Arc<
    // first current version, second new version
    dyn Fn(Vec<SstDeltaInfo>, CacheRefillContext, PinnedVersion, PinnedVersion) -> JoinHandle<()>
        + Send
        + Sync
        + 'static,
>;

pub type TableCacheRefillContextMap = HashMap<TableId, TableCacheRefillContext>;

/// Per-table metadata captured for a refill task to decide whether an sstable block should be
/// refilled. Mutable runtime state used to build this snapshot is owned by `CacheRefiller`.
#[derive(Clone)]
pub struct TableCacheRefillContext {
    /// Vnodes covered by local streaming read versions on this compute node.
    pub streaming_vnode_bitmap: Option<Bitmap>,
    /// Vnodes served by this compute node according to the serving vnode mapping.
    pub serving_vnode_bitmap: Option<Bitmap>,
    /// Effective refill policy after applying the default policy and per-table overrides.
    pub policy: CacheRefillPolicy,
}

/// Read-only data cloned from `CacheRefiller` for monitor/debugging APIs.
///
/// Streaming vnode mapping is the table-level union maintained by the refiller.
#[derive(Clone)]
pub struct TableCacheRefillMonitorSnapshot {
    pub contexts: TableCacheRefillContextMap,
    pub policies: HashMap<TableId, CacheRefillPolicy>,
    pub default_policy: CacheRefillPolicy,
    pub streaming_table_vnode_mapping: HashMap<TableId, Bitmap>,
    pub serving_table_vnode_mapping: HashMap<TableId, Bitmap>,
}

fn vnode_range_overlaps_bitmap(vnode_range: (usize, usize), bitmap: &Bitmap) -> bool {
    assert!(vnode_range.0 <= vnode_range.1);
    let start = vnode_range.0.min(bitmap.len());
    let end = vnode_range.1.min(bitmap.len());
    if start == end || !bitmap.any() {
        return false;
    }
    if bitmap.all() {
        return true;
    }
    (start..end).any(|vnode| bitmap.is_set(vnode))
}

impl TableCacheRefillContext {
    fn allows_normal_data_refill_block(&self, sstable: &Sstable, block_index: usize) -> bool {
        if self.policy.is_unscoped_enabled() {
            return true;
        }

        (self.policy.is_streaming_scoped()
            && self.check_table_refill_streaming_vnodes(sstable, block_index))
            || (self.policy.is_serving_scoped()
                && self.check_table_refill_serving_vnodes(sstable, block_index))
    }

    fn allows_insert_only_data_refill_block(&self, sstable: &Sstable, block_index: usize) -> bool {
        // Insert-only deltas have no delete-side evidence for recent/inheritance filters.
        // Only serving-owned blocks need refill, because streaming writers already populated
        // their local cache.
        (self.policy.is_unscoped_enabled() || self.policy.is_serving_scoped())
            && self.check_table_refill_serving_vnodes(sstable, block_index)
    }

    fn check_table_refill_streaming_vnodes(&self, sstable: &Sstable, block_index: usize) -> bool {
        self.streaming_vnode_bitmap.as_ref().is_some_and(|bitmap| {
            let vnode_range = block_vnode_range(sstable, block_index);
            vnode_range_overlaps_bitmap(vnode_range, bitmap)
        })
    }

    fn check_table_refill_serving_vnodes(&self, sstable: &Sstable, block_index: usize) -> bool {
        self.serving_vnode_bitmap.as_ref().is_some_and(|bitmap| {
            let vnode_range = block_vnode_range(sstable, block_index);
            vnode_range_overlaps_bitmap(vnode_range, bitmap)
        })
    }
}

fn block_vnode_range(sstable: &Sstable, block_index: usize) -> (usize, usize) {
    let meta_handle = SstableMetaHandle::v2(sstable);
    let block_meta = meta_handle.block_meta(block_index);
    let block_smallest_key = FullKey::decode(&block_meta.smallest_key);
    let table_key_end = match (block_index + 1 < meta_handle.block_count())
        .then(|| meta_handle.block_meta(block_index + 1))
    {
        // A table switch always starts a new block. The next table's smallest key has an
        // unrelated vnode, so use the current table's terminal range instead.
        Some(next_block_meta) if next_block_meta.table_id() != block_meta.table_id() => {
            Bound::Unbounded
        }
        // Full-key versions of the same table key may span adjacent blocks. After projecting
        // away the epoch, the boundary vnode therefore remains part of the current block.
        Some(next_block_meta) => Bound::Included(
            FullKey::decode(&next_block_meta.smallest_key)
                .user_key
                .table_key,
        ),
        // `SstableMeta::largest_key` is the actual last key, unlike the next block's smallest
        // key above. Keep it inclusive, especially for singleton tables whose key contains only
        // the vnode prefix.
        None => Bound::Included(
            FullKey::decode(meta_handle.largest_key())
                .user_key
                .table_key,
        ),
    };

    let table_key_range = (
        Bound::Included(block_smallest_key.user_key.table_key),
        table_key_end,
    );
    // Block-meta separators may shorten the table key below the vnode prefix. They are valid
    // full-key search boundaries but cannot identify a vnode, so fail open instead of panicking
    // or dropping a block that may belong to this worker.
    if match &table_key_range.0 {
        Bound::Included(key) | Bound::Excluded(key) => key.as_ref().len() < VirtualNode::SIZE,
        Bound::Unbounded => false,
    } || match &table_key_range.1 {
        Bound::Included(key) | Bound::Excluded(key) => key.as_ref().len() < VirtualNode::SIZE,
        Bound::Unbounded => false,
    } {
        return (0, VirtualNode::MAX_REPRESENTABLE.to_index() + 1);
    }
    vnode_range(&table_key_range)
}

/// A cache refiller for hummock data.
pub(crate) struct CacheRefiller {
    /// order: old => new
    queue: VecDeque<Item>,

    spawn_refill_task: SpawnRefillTask,

    config: Arc<CacheRefillConfig>,
    meta_refill_concurrency: Option<Arc<Semaphore>>,
    concurrency: Arc<Semaphore>,
    sstable_store: SstableStoreRef,

    role: Role,
    default_policy: CacheRefillPolicy,
    table_cache_refill_policies: HashMap<TableId, CacheRefillPolicy>,
    streaming_table_vnode_mapping: HashMap<TableId, Bitmap>,
    serving_table_vnode_mapping: HashMap<TableId, Bitmap>,
}

impl CacheRefiller {
    pub(crate) fn new(
        role: Role,
        config: CacheRefillConfig,
        sstable_store: SstableStoreRef,
        spawn_refill_task: SpawnRefillTask,
    ) -> Self {
        let config = Arc::new(config);
        let concurrency = Arc::new(Semaphore::new(config.concurrency));
        let default_policy = config.table_cache_refill_default_policy;
        let meta_refill_concurrency = if config.meta_refill_concurrency == 0 {
            None
        } else {
            Some(Arc::new(Semaphore::new(config.meta_refill_concurrency)))
        };
        Self {
            queue: VecDeque::new(),
            spawn_refill_task,
            config,
            meta_refill_concurrency,
            concurrency,
            sstable_store,
            role,
            default_policy,
            table_cache_refill_policies: HashMap::new(),
            streaming_table_vnode_mapping: HashMap::new(),
            serving_table_vnode_mapping: HashMap::new(),
        }
    }

    pub(crate) fn default_spawn_refill_task() -> SpawnRefillTask {
        Arc::new(|deltas, context, _, _| {
            let task = CacheRefillTask { deltas, context };
            tokio::spawn(task.run())
        })
    }

    pub(crate) fn start_cache_refill(
        &mut self,
        mut deltas: Vec<SstDeltaInfo>,
        pinned_version: PinnedVersion,
        new_pinned_version: PinnedVersion,
    ) {
        for delta in &mut deltas {
            let for_serving = self.role.for_serving();
            // Writer-appended L0 SSTs are already warm on the streaming side. Their data refill
            // may therefore only be needed by serving workers.
            let for_streaming =
                self.role.for_streaming() && !delta.delete_sst_object_ids.is_empty();

            if !for_serving && !for_streaming {
                delta.insert_sst_infos.clear();
                continue;
            }

            // This is deliberately a whole-SST admission check before Meta load. The serving
            // mapping key set identifies result tables, but bitmap contents remain for the exact
            // block/vnode decision in DataCacheRefillTaskGenerator after Meta load. An SST stays
            // when any contained table matches either the serving or streaming refill lane.
            delta.insert_sst_infos.retain(|sst| {
                sst.table_ids.iter().any(|table_id| {
                    // A missing entry means there is no table override, not that the table is
                    // absent. Preserve the configured legacy/default policy in that case.
                    let policy = self
                        .table_cache_refill_policies
                        .get(table_id)
                        .copied()
                        .unwrap_or(self.default_policy);

                    // Enabled preserves legacy full refill. For scoped policies, mapping keys are
                    // only a whole-SST coarse gate; bitmap bits still filter blocks post-Meta.
                    match policy {
                        CacheRefillPolicy::Enabled => for_streaming || for_serving,
                        CacheRefillPolicy::Disabled => false,
                        CacheRefillPolicy::Streaming => {
                            for_streaming
                                && self.streaming_table_vnode_mapping.contains_key(table_id)
                        }
                        CacheRefillPolicy::Serving => {
                            for_serving && self.serving_table_vnode_mapping.contains_key(table_id)
                        }
                        CacheRefillPolicy::Both => {
                            (for_streaming
                                && self.streaming_table_vnode_mapping.contains_key(table_id))
                                || (for_serving
                                    && self.serving_table_vnode_mapping.contains_key(table_id))
                        }
                    }
                })
            });
        }
        let context = self.new_cache_refill_context(&deltas);
        let handle = (self.spawn_refill_task)(
            deltas,
            context,
            pinned_version.clone(),
            new_pinned_version.clone(),
        );
        let event = CacheRefillerEvent {
            pinned_version,
            new_pinned_version,
        };
        let item = Item { handle, event };
        self.queue.push_back(item);
        GLOBAL_CACHE_REFILL_METRICS.refill_queue_total.add(1);
    }

    fn new_cache_refill_context(&self, deltas: &[SstDeltaInfo]) -> CacheRefillContext {
        let table_ids = deltas.iter().flat_map(|delta| {
            delta
                .insert_sst_infos
                .iter()
                .flat_map(|sst| sst.table_ids.iter().copied())
        });
        CacheRefillContext {
            config: self.config.clone(),
            meta_refill_concurrency: self.meta_refill_concurrency.clone(),
            concurrency: self.concurrency.clone(),
            sstable_store: self.sstable_store.clone(),
            table_cache_refill_context_map: Arc::new(self.table_cache_refill_contexts(table_ids)),
        }
    }

    pub(crate) fn last_new_pinned_version(&self) -> Option<&PinnedVersion> {
        self.queue.back().map(|item| &item.event.new_pinned_version)
    }

    /// Replaces the complete policy snapshot applicable to this worker.
    pub(crate) fn replace_table_cache_refill_policies(
        &mut self,
        policies: HashMap<TableId, CacheRefillPolicy>,
    ) {
        self.table_cache_refill_policies = policies;
    }

    /// Replaces the complete serving vnode mapping snapshot.
    pub(crate) fn replace_serving_table_vnode_mapping(
        &mut self,
        mapping: HashMap<TableId, Bitmap>,
    ) {
        self.serving_table_vnode_mapping = mapping;
    }

    pub(crate) fn update_streaming_table_vnodes(
        &mut self,
        table_id: TableId,
        streaming_vnodes: Option<Bitmap>,
    ) {
        if let Some(streaming_vnodes) = streaming_vnodes {
            self.streaming_table_vnode_mapping
                .insert(table_id, streaming_vnodes);
        } else {
            self.streaming_table_vnode_mapping.remove(&table_id);
        }
    }

    fn table_cache_refill_contexts(
        &self,
        table_ids: impl IntoIterator<Item = TableId>,
    ) -> TableCacheRefillContextMap {
        let for_streaming = self.role.for_streaming();
        let for_serving = self.role.for_serving();
        table_ids
            .into_iter()
            .filter_map(|table_id| {
                if for_serving
                    && !for_streaming
                    && !self.serving_table_vnode_mapping.contains_key(&table_id)
                {
                    return None;
                }
                let policy = self
                    .table_cache_refill_policies
                    .get(&table_id)
                    .copied()
                    .unwrap_or(self.default_policy);
                let streaming_vnode_bitmap = (for_streaming && policy.is_streaming_scoped())
                    .then(|| self.streaming_table_vnode_mapping.get(&table_id).cloned())
                    .flatten();
                // `Enabled` normally does not use bitmap filtering. The only exception is L0
                // insert-only refill, where serving workers still need serving-locality evidence.
                let serving_vnode_bitmap = (for_serving
                    && (policy.is_serving_scoped() || policy.is_unscoped_enabled()))
                .then(|| self.serving_table_vnode_mapping.get(&table_id).cloned())
                .flatten();
                Some((
                    table_id,
                    TableCacheRefillContext {
                        streaming_vnode_bitmap,
                        serving_vnode_bitmap,
                        policy,
                    },
                ))
            })
            .collect()
    }

    pub(crate) fn table_cache_refill_monitor_snapshot(&self) -> TableCacheRefillMonitorSnapshot {
        let table_ids = self
            .table_cache_refill_policies
            .keys()
            .chain(self.streaming_table_vnode_mapping.keys())
            .chain(self.serving_table_vnode_mapping.keys())
            .copied();
        TableCacheRefillMonitorSnapshot {
            contexts: self.table_cache_refill_contexts(table_ids),
            policies: self.table_cache_refill_policies.clone(),
            default_policy: self.default_policy,
            streaming_table_vnode_mapping: self.streaming_table_vnode_mapping.clone(),
            serving_table_vnode_mapping: self.serving_table_vnode_mapping.clone(),
        }
    }
}

impl CacheRefiller {
    pub(crate) fn next_events(&mut self) -> impl Future<Output = Vec<CacheRefillerEvent>> + '_ {
        poll_fn(|cx| {
            const MAX_BATCH_SIZE: usize = 16;
            let mut events = None;
            while let Some(item) = self.queue.front_mut()
                && let Poll::Ready(result) = item.handle.poll_unpin(cx)
            {
                result.unwrap();
                let item = self.queue.pop_front().unwrap();
                GLOBAL_CACHE_REFILL_METRICS.refill_queue_total.sub(1);
                let events = events.get_or_insert_with(|| Vec::with_capacity(MAX_BATCH_SIZE));
                events.push(item.event);
                if events.len() >= MAX_BATCH_SIZE {
                    break;
                }
            }
            if let Some(events) = events {
                Poll::Ready(events)
            } else {
                Poll::Pending
            }
        })
    }
}

pub struct CacheRefillerEvent {
    pub pinned_version: PinnedVersion,
    pub new_pinned_version: PinnedVersion,
}

#[derive(Clone)]
pub(crate) struct CacheRefillContext {
    config: Arc<CacheRefillConfig>,
    meta_refill_concurrency: Option<Arc<Semaphore>>,
    concurrency: Arc<Semaphore>,
    sstable_store: SstableStoreRef,
    table_cache_refill_context_map: Arc<TableCacheRefillContextMap>,
}

struct DataCacheRefillTaskGenerator<'a> {
    context: &'a CacheRefillContext,
    delta: &'a SstDeltaInfo,
    ssts: &'a [TableHolder],
}

impl DataCacheRefillTaskGenerator<'_> {
    fn generate_unfiltered_tasks(&self) -> Vec<DataCacheRefillTask> {
        let mut tasks = Vec::new();

        // Skip data cache refill if data disk cache is not enabled.
        if !self.context.sstable_store.block_cache().is_hybrid() {
            return tasks;
        }

        if self.delta.insert_sst_infos.is_empty() {
            return tasks;
        }

        let has_parent_ssts = !self.delta.delete_sst_object_ids.is_empty();
        // CN-written SSTs are appended to L0 without replacing parent SSTs. Other inserted SSTs
        // need delete-side evidence for recent and inheritance filtering.
        debug_assert!(has_parent_ssts || self.delta.insert_sst_level == 0);

        // Return if the target level is not in the refill levels
        if !self
            .context
            .config
            .data_refill_levels
            .contains(&self.delta.insert_sst_level)
        {
            return tasks;
        }

        // Cache refill units must not cross a table boundary. A logical SST projection still
        // decides whether to admit each single-table unit.
        let unit = self.context.config.unit;
        assert!(unit > 0, "cache refill unit must be positive");
        let table_cache_refill_context_map = &self.context.table_cache_refill_context_map;
        for (sst_info, sst) in self.delta.insert_sst_infos.iter().zip_eq_fast(self.ssts) {
            debug_assert_eq!(sst_info.object_id, sst.id);
            debug_assert!(sst_info.table_ids.is_sorted());
            let meta_handle = SstableMetaHandle::v2(sst);
            let mut blk_start = 0;
            while blk_start < meta_handle.block_count() {
                // SstableBuilder ends a block before the table ID changes, so block metadata
                // defines the exact physical boundary. `table_ids` below only admits logical
                // projections and must not make a unit span another table.
                let table_id = meta_handle.block_meta(blk_start).table_id();
                let mut blk_end = std::cmp::min(meta_handle.block_count(), blk_start + unit);
                if let Some(table_boundary) = (blk_start + 1..blk_end)
                    .find(|&block_index| meta_handle.block_meta(block_index).table_id() != table_id)
                {
                    blk_end = table_boundary;
                }

                let should_refill = sst_info.table_ids.binary_search(&table_id).is_ok()
                    && (blk_start..blk_end).any(|block_index| {
                        table_cache_refill_context_map
                            .get(&table_id)
                            .is_some_and(|context| {
                                if has_parent_ssts {
                                    context.allows_normal_data_refill_block(sst, block_index)
                                } else {
                                    context.allows_insert_only_data_refill_block(sst, block_index)
                                }
                            })
                    });
                if should_refill {
                    tasks.push(DataCacheRefillTask {
                        sst: sst.clone(),
                        blks: blk_start..blk_end,
                    });
                }
                blk_start = blk_end;
            }
        }

        if tasks.is_empty() {
            return tasks;
        }

        // Policy/vnode ownership defines refill responsibility first, but it does not bypass
        // recent admission for normal insert+delete refill.
        if has_parent_ssts
            && !self.context.config.skip_recent_filter
            && !self.filter_by_recent_filter()
        {
            GLOBAL_CACHE_REFILL_METRICS
                .data_refill_filtered_total
                .inc_by(self.delta.delete_sst_object_ids.len() as u64);
            return vec![];
        }

        tasks
    }

    async fn filter_by_inheritance_if_needed(
        &self,
        tasks: Vec<DataCacheRefillTask>,
    ) -> Vec<DataCacheRefillTask> {
        // Skipping the recent filter selects full refill. Inheritance filtering only applies to
        // non-L0 normal refill after real recent-filter admission.
        let should_filter_by_inheritance = !tasks.is_empty()
            && !self.delta.delete_sst_object_ids.is_empty()
            && self.delta.insert_sst_level != 0
            && !self.context.config.skip_recent_filter
            && !self.context.config.skip_inheritance_filter;
        if should_filter_by_inheritance {
            self.filter_by_inheritance_filter(tasks).await
        } else {
            tasks
        }
    }

    // Return if recent filter is required and no deleted sst ids are in the recent filter.
    fn filter_by_recent_filter(&self) -> bool {
        let recent_filter = self.context.sstable_store.recent_filter();
        let targets = self
            .delta
            .delete_sst_object_ids
            .iter()
            .map(|id| (*id, usize::MAX))
            .collect_vec();
        recent_filter.contains_any(targets.iter())
    }

    async fn filter_by_inheritance_filter(
        &self,
        originals: Vec<DataCacheRefillTask>,
    ) -> Vec<DataCacheRefillTask> {
        // Get parent sst metas from cache.
        let sstable_store = self.context.sstable_store.clone();
        let futures = self.delta.delete_sst_object_ids.iter().map(|sst_obj_id| {
            let store = &sstable_store;
            async move {
                let res = store.sstable_cached(*sst_obj_id).await;
                match res {
                    Ok(Some(_)) => GLOBAL_CACHE_REFILL_METRICS
                        .data_refill_parent_meta_lookup_hit_total
                        .inc(),
                    Ok(None) => GLOBAL_CACHE_REFILL_METRICS
                        .data_refill_parent_meta_lookup_miss_total
                        .inc(),
                    _ => {}
                }
                res
            }
        });
        let parent_ssts = match try_join_all(futures).await {
            Ok(parent_ssts) => parent_ssts.into_iter().flatten(),
            Err(e) => {
                tracing::error!(error = %e.as_report(), "get old meta from cache error");
                return vec![];
            }
        };

        // assert units in asc order
        if cfg!(debug_assertions) {
            originals.iter().tuple_windows().for_each(|(a, b)| {
                debug_assert_ne!(
                    KeyComparator::compare_encoded_full_key(a.largest_key(), b.smallest_key()),
                    std::cmp::Ordering::Greater
                )
            });
        }

        let mut filtered: HashSet<SstableUnit> = HashSet::default();
        let recent_filter = self.context.sstable_store.recent_filter();
        for psst in parent_ssts {
            let meta_handle = SstableMetaHandle::v2(&psst);
            for pblk in 0..meta_handle.block_count() {
                let pleft = &meta_handle.block_meta(pblk).smallest_key;
                // `largest_key` can be included or excluded, both are treated as included here.
                let pright = meta_handle.block_upper_bound_key(pblk + 1);

                // partition point: unit.right < pblk.left
                let uleft = originals.partition_point(|task| {
                    KeyComparator::compare_encoded_full_key(task.largest_key(), pleft)
                        == std::cmp::Ordering::Less
                });
                // partition point: unit.left <= pblk.right
                let uright = originals.partition_point(|task| {
                    KeyComparator::compare_encoded_full_key(task.smallest_key(), pright)
                        != std::cmp::Ordering::Greater
                });

                // overlapping: uleft..uright
                for task in originals.iter().take(uright).skip(uleft) {
                    let unit = task.unit();
                    if filtered.contains(&unit) {
                        continue;
                    }
                    if recent_filter.contains(&(psst.id, pblk)) {
                        filtered.insert(unit);
                    }
                }
            }
        }

        let hit = filtered.len();
        let miss = originals.len() - hit;
        GLOBAL_CACHE_REFILL_METRICS
            .data_refill_unit_inheritance_hit_total
            .inc_by(hit as u64);
        GLOBAL_CACHE_REFILL_METRICS
            .data_refill_unit_inheritance_miss_total
            .inc_by(miss as u64);

        originals
            .into_iter()
            .filter(|task| filtered.contains(&task.unit()))
            .collect()
    }
}

#[derive(Debug)]
struct DataCacheRefillTask {
    sst: TableHolder,
    blks: Range<usize>,
}

impl DataCacheRefillTask {
    fn unit(&self) -> SstableUnit {
        SstableUnit {
            sst_obj_id: self.sst.id,
            blks: self.blks.clone(),
        }
    }

    fn smallest_key(&self) -> &[u8] {
        &SstableMetaHandle::v2(&self.sst)
            .block_meta(self.blks.start)
            .smallest_key
    }

    fn largest_key(&self) -> &[u8] {
        SstableMetaHandle::v2(&self.sst).block_upper_bound_key(self.blks.end)
    }
}

struct CacheRefillTask {
    deltas: Vec<SstDeltaInfo>,
    context: CacheRefillContext,
}

impl CacheRefillTask {
    async fn run(self) {
        let tasks = self
            .deltas
            .iter()
            .map(|delta| {
                let context = self.context.clone();
                async move {
                    let holders = match Self::meta_cache_refill(&context, delta).await {
                        Ok(holders) => holders,
                        Err(e) => {
                            tracing::warn!(error = %e.as_report(), "meta cache refill error");
                            return;
                        }
                    };
                    let generator = DataCacheRefillTaskGenerator {
                        context: &context,
                        delta,
                        ssts: &holders,
                    };
                    let tasks = generator.generate_unfiltered_tasks();

                    // Main counts after recent admission but before inheritance.
                    let unfiltered_block_count =
                        tasks.iter().map(|task| task.blks.len() as u64).sum();
                    GLOBAL_CACHE_REFILL_METRICS
                        .data_refill_block_unfiltered_total
                        .inc_by(unfiltered_block_count);

                    let tasks = generator.filter_by_inheritance_if_needed(tasks).await;
                    Self::data_cache_refill(&context, tasks).await;
                }
            })
            .collect_vec();
        let future = join_all(tasks);

        let _ = tokio::time::timeout(self.context.config.timeout, future).await;
    }

    async fn meta_cache_refill(
        context: &CacheRefillContext,
        delta: &SstDeltaInfo,
    ) -> HummockResult<Vec<TableHolder>> {
        let tasks = delta
            .insert_sst_infos
            .iter()
            .map(|info| async {
                let mut stats = StoreLocalStatistic::default();
                GLOBAL_CACHE_REFILL_METRICS.meta_refill_attempts_total.inc();

                let permit = if let Some(c) = &context.meta_refill_concurrency {
                    Some(c.acquire().await.unwrap())
                } else {
                    None
                };

                let now = Instant::now();
                let res = context.sstable_store.sstable(info, &mut stats).await;
                stats.discard();
                if res.is_ok() {
                    GLOBAL_CACHE_REFILL_METRICS
                        .meta_refill_success_duration
                        .observe(now.elapsed().as_secs_f64());
                }
                drop(permit);

                res
            })
            .collect_vec();
        let holders = try_join_all(tasks).await?;
        Ok(holders)
    }

    async fn data_cache_refill(context: &CacheRefillContext, tasks: Vec<DataCacheRefillTask>) {
        let mut futures = Vec::with_capacity(tasks.len());
        for task in tasks {
            // update filter for sst id only
            context
                .sstable_store
                .recent_filter()
                .insert((task.sst.id, usize::MAX));

            let blocks = task.blks.len();
            let mut contexts = Vec::with_capacity(blocks);
            let mut admits = 0;

            let meta_handle = SstableMetaHandle::v2(&task.sst);
            let (range_first, _) = meta_handle.block_range(task.blks.start);
            let (range_last, _) = meta_handle.block_range(task.blks.end - 1);
            let range = range_first.start..range_last.end;

            let size = range.size().unwrap();

            GLOBAL_CACHE_REFILL_METRICS
                .data_refill_ideal_bytes
                .inc_by(size as _);

            for blk in task.blks {
                let (range, uncompressed_capacity) = meta_handle.block_range(blk);
                let key = SstableBlockIndex {
                    sst_id: task.sst.id,
                    block_idx: blk as u64,
                };

                let mut writer = context.sstable_store.block_cache().storage_writer(key);

                if writer.filter(size).is_admitted() {
                    admits += 1;
                }

                contexts.push((writer, range, uncompressed_capacity))
            }

            if admits as f64 / contexts.len() as f64 >= context.config.threshold {
                let sstable_store = context.sstable_store.clone();
                let context = context.clone();
                let future = async move {
                    GLOBAL_CACHE_REFILL_METRICS.data_refill_attempts_total.inc();

                    let permit = context.concurrency.acquire().await.unwrap();

                    GLOBAL_CACHE_REFILL_METRICS.data_refill_started_total.inc();

                    let now = Instant::now();

                    let data = sstable_store
                        .store()
                        .read(&sstable_store.get_sst_data_path(task.sst.id), range.clone())
                        .await?;
                    let mut apply_disk_cache_futures = vec![];
                    for (w, r, uc) in contexts {
                        let offset = r.start - range.start;
                        let len = r.end - r.start;
                        let bytes = data.slice(offset..offset + len);
                        let future = async move {
                            let value = Box::new(Block::decode(bytes, uc)?);
                            // The entry should always be `Some(..)`, use if here for compatible.
                            if let Some(_entry) = w.force().insert(value) {
                                GLOBAL_CACHE_REFILL_METRICS
                                    .data_refill_success_bytes
                                    .inc_by(len as u64);
                                GLOBAL_CACHE_REFILL_METRICS
                                    .data_refill_block_success_total
                                    .inc();
                            }
                            Ok::<_, HummockError>(())
                        };
                        apply_disk_cache_futures.push(future);
                    }
                    try_join_all(apply_disk_cache_futures)
                        .await
                        .map_err(HummockError::file_cache)?;

                    GLOBAL_CACHE_REFILL_METRICS
                        .data_refill_success_duration
                        .observe(now.elapsed().as_secs_f64());
                    drop(permit);

                    Ok::<_, HummockError>(())
                };
                futures.push(future);
            }
        }

        let futures = futures.into_iter().map(|future| async move {
            if let Err(e) = future.await {
                tracing::error!(error = %e.as_report(), "data cache refill task error");
            }
        });

        join_all(futures).await;
    }
}

#[derive(Debug)]
pub struct SstableBlock {
    pub sst_obj_id: HummockSstableObjectId,
    pub blk_idx: usize,
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct SstableUnit {
    pub sst_obj_id: HummockSstableObjectId,
    pub blks: Range<usize>,
}

impl Ord for SstableUnit {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.sst_obj_id.cmp(&other.sst_obj_id) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        match self.blks.start.cmp(&other.blks.start) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        self.blks.end.cmp(&other.blks.end)
    }
}

impl PartialOrd for SstableUnit {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use parking_lot::Mutex;
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::config::Role;
    use risingwave_common::config::streaming::CacheRefillPolicy;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::compaction_group::group_split::split_sst_with_table_ids;
    use risingwave_hummock_sdk::key::{FullKey, UserKey, prefix_slice_with_vnode};
    use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
    use risingwave_hummock_sdk::version::HummockVersion;
    use risingwave_hummock_sdk::{EpochWithGap, HummockSstableObjectId};
    use risingwave_pb::hummock::PbHummockVersion;
    use risingwave_pb::id::TableId;
    use tokio::sync::mpsc::unbounded_channel;

    use super::{
        CacheRefillConfig, CacheRefillContext, CacheRefiller, DataCacheRefillTaskGenerator,
        SpawnRefillTask, SstDeltaInfo, block_vnode_range, vnode_range_overlaps_bitmap,
    };
    use crate::hummock::iterator::test_utils::{
        iterator_test_table_key_of, mock_sstable_store, mock_sstable_store_with_recent_filter,
    };
    use crate::hummock::local_version::pinned_version::PinnedVersion;
    use crate::hummock::recent_filter::simple::SimpleRecentFilter;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_test_sstable_with_table_ids,
    };
    use crate::hummock::value::HummockValue;
    use crate::hummock::{RecentFilter, RecentFilterTrait, SstableStoreRef, TableHolder};

    fn test_refill_config(default_policy: CacheRefillPolicy) -> CacheRefillConfig {
        CacheRefillConfig {
            timeout: Duration::from_secs(1),
            data_refill_levels: HashSet::new(),
            meta_refill_concurrency: 1,
            concurrency: 1,
            unit: 1,
            threshold: 0.0,
            skip_recent_filter: true,
            skip_inheritance_filter: true,
            table_cache_refill_default_policy: default_policy,
        }
    }

    fn pinned_version_for_test() -> PinnedVersion {
        PinnedVersion::new(
            HummockVersion::from(PbHummockVersion::default()),
            unbounded_channel().0,
        )
    }

    async fn gen_test_sst_with_object_id(
        table_id: TableId,
        sstable_store: SstableStoreRef,
        object_id: u64,
    ) -> (TableHolder, SstableInfo) {
        gen_test_sstable_with_table_ids(
            default_builder_opt_for_test(),
            object_id,
            (0..2).map(|idx| {
                (
                    FullKey {
                        user_key: risingwave_hummock_sdk::key::UserKey::for_test(
                            table_id,
                            iterator_test_table_key_of(idx),
                        ),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                    },
                    HummockValue::put(vec![idx as u8]),
                )
            }),
            sstable_store,
            vec![table_id.as_raw_id()],
        )
        .await
    }

    struct DataRefillGeneratorTestFixture {
        table_id: TableId,
        sstable_store: SstableStoreRef,
        sst: TableHolder,
        sst_info: SstableInfo,
        deleted_sst_object_id: HummockSstableObjectId,
    }

    impl DataRefillGeneratorTestFixture {
        async fn new(
            recent_filter: Option<Arc<RecentFilter<(HummockSstableObjectId, usize)>>>,
        ) -> Self {
            let table_id = TableId::from(233);
            let sstable_store = match recent_filter {
                Some(recent_filter) => mock_sstable_store_with_recent_filter(recent_filter).await,
                None => mock_sstable_store().await,
            };
            let (sst, sst_info) =
                gen_test_sst_with_object_id(table_id, sstable_store.clone(), 1).await;
            Self {
                table_id,
                sstable_store,
                sst,
                sst_info,
                deleted_sst_object_id: 2330.into(),
            }
        }

        fn context(
            &self,
            policy: CacheRefillPolicy,
            streaming_vnode_bitmap: Option<Bitmap>,
            serving_vnode_bitmap: Option<Bitmap>,
            configure: impl FnOnce(&mut CacheRefillConfig),
        ) -> CacheRefillContext {
            let mut config = test_refill_config(CacheRefillPolicy::Enabled);
            config.data_refill_levels.insert(0);
            configure(&mut config);
            CacheRefillContext {
                config: Arc::new(config),
                meta_refill_concurrency: None,
                concurrency: Arc::new(tokio::sync::Semaphore::new(1)),
                sstable_store: self.sstable_store.clone(),
                table_cache_refill_context_map: Arc::new(HashMap::from([(
                    self.table_id,
                    super::TableCacheRefillContext {
                        streaming_vnode_bitmap,
                        serving_vnode_bitmap,
                        policy,
                    },
                )])),
            }
        }

        fn normal_delta(
            &self,
            insert_sst_level: u32,
            deleted_sst_object_id: HummockSstableObjectId,
        ) -> SstDeltaInfo {
            SstDeltaInfo {
                insert_sst_infos: vec![self.sst_info.clone()],
                delete_sst_object_ids: vec![deleted_sst_object_id],
                insert_sst_level,
            }
        }

        fn normal_l0_delta(&self) -> SstDeltaInfo {
            self.normal_delta(0, self.deleted_sst_object_id)
        }

        fn l0_insert_only_delta(&self) -> SstDeltaInfo {
            SstDeltaInfo {
                insert_sst_infos: vec![self.sst_info.clone()],
                delete_sst_object_ids: vec![],
                insert_sst_level: 0,
            }
        }

        async fn generate(
            &self,
            context: &CacheRefillContext,
            delta: &SstDeltaInfo,
        ) -> Vec<super::DataCacheRefillTask> {
            let generator = DataCacheRefillTaskGenerator {
                context,
                delta,
                ssts: std::slice::from_ref(&self.sst),
            };
            let tasks = generator.generate_unfiltered_tasks();
            generator.filter_by_inheritance_if_needed(tasks).await
        }
    }

    #[tokio::test]
    async fn test_table_cache_refill_contexts_by_role_and_policy() {
        struct Case {
            name: &'static str,
            role: Role,
            default_policy: CacheRefillPolicy,
            policy: Option<CacheRefillPolicy>,
            has_streaming_vnodes: bool,
            has_serving_vnodes: bool,
            expected: Option<(CacheRefillPolicy, bool, bool)>,
        }

        let cases = [
            Case {
                name: "streaming role uses streaming side of Both",
                role: Role::Streaming,
                default_policy: CacheRefillPolicy::Disabled,
                policy: Some(CacheRefillPolicy::Both),
                has_streaming_vnodes: true,
                has_serving_vnodes: true,
                expected: Some((CacheRefillPolicy::Both, true, false)),
            },
            Case {
                name: "serving role uses serving side of Both",
                role: Role::Serving,
                default_policy: CacheRefillPolicy::Disabled,
                policy: Some(CacheRefillPolicy::Both),
                has_streaming_vnodes: true,
                has_serving_vnodes: true,
                expected: Some((CacheRefillPolicy::Both, false, true)),
            },
            Case {
                name: "both role keeps both sides",
                role: Role::Both,
                default_policy: CacheRefillPolicy::Disabled,
                policy: Some(CacheRefillPolicy::Both),
                has_streaming_vnodes: true,
                has_serving_vnodes: true,
                expected: Some((CacheRefillPolicy::Both, true, true)),
            },
            Case {
                name: "both role keeps streaming-only ownership",
                role: Role::Both,
                default_policy: CacheRefillPolicy::Disabled,
                policy: Some(CacheRefillPolicy::Both),
                has_streaming_vnodes: true,
                has_serving_vnodes: false,
                expected: Some((CacheRefillPolicy::Both, true, false)),
            },
            Case {
                name: "streaming scope without ownership has no usable bitmap",
                role: Role::Streaming,
                default_policy: CacheRefillPolicy::Disabled,
                policy: Some(CacheRefillPolicy::Streaming),
                has_streaming_vnodes: false,
                has_serving_vnodes: false,
                expected: Some((CacheRefillPolicy::Streaming, false, false)),
            },
            Case {
                name: "pure serving worker excludes unmapped table",
                role: Role::Serving,
                default_policy: CacheRefillPolicy::Disabled,
                policy: Some(CacheRefillPolicy::Serving),
                has_streaming_vnodes: true,
                has_serving_vnodes: false,
                expected: None,
            },
            Case {
                name: "default Enabled retains serving ownership",
                role: Role::Serving,
                default_policy: CacheRefillPolicy::Enabled,
                policy: None,
                has_streaming_vnodes: false,
                has_serving_vnodes: true,
                expected: Some((CacheRefillPolicy::Enabled, false, true)),
            },
            Case {
                name: "explicit policy overrides default",
                role: Role::Serving,
                default_policy: CacheRefillPolicy::Enabled,
                policy: Some(CacheRefillPolicy::Disabled),
                has_streaming_vnodes: false,
                has_serving_vnodes: true,
                expected: Some((CacheRefillPolicy::Disabled, false, false)),
            },
        ];

        let table_id = TableId::from(233);
        let streaming_vnodes = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [1, 3]);
        let serving_vnodes = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [2, 4]);
        let sstable_store = mock_sstable_store().await;
        for case in cases {
            let mut refiller = CacheRefiller::new(
                case.role,
                test_refill_config(case.default_policy),
                sstable_store.clone(),
                CacheRefiller::default_spawn_refill_task(),
            );
            if let Some(policy) = case.policy {
                refiller.replace_table_cache_refill_policies(HashMap::from([(table_id, policy)]));
            }
            if case.has_streaming_vnodes {
                refiller.update_streaming_table_vnodes(table_id, Some(streaming_vnodes.clone()));
            }
            if case.has_serving_vnodes {
                refiller.replace_serving_table_vnode_mapping(HashMap::from([(
                    table_id,
                    serving_vnodes.clone(),
                )]));
            }

            let contexts = refiller.table_cache_refill_contexts([table_id]);
            let actual = contexts.get(&table_id).map(|context| {
                (
                    context.policy,
                    context.streaming_vnode_bitmap.as_ref(),
                    context.serving_vnode_bitmap.as_ref(),
                )
            });
            let expected = case.expected.map(|(policy, streaming, serving)| {
                (
                    policy,
                    streaming.then_some(&streaming_vnodes),
                    serving.then_some(&serving_vnodes),
                )
            });
            assert_eq!(actual, expected, "{}", case.name);
        }
    }

    #[tokio::test]
    async fn test_refill_task_captures_runtime_context_snapshot() {
        let table_id = TableId::from(233);
        let old_vnodes = Bitmap::ones(VirtualNode::COUNT_FOR_TEST);
        let new_vnodes = Bitmap::from_range(VirtualNode::COUNT_FOR_TEST, 0..8);
        let captured_context = Arc::new(Mutex::new(None::<CacheRefillContext>));
        let captured_context_clone = captured_context.clone();
        let spawn_refill_task: SpawnRefillTask = Arc::new(move |_, context, _, _| {
            *captured_context_clone.lock() = Some(context);
            tokio::spawn(async {})
        });
        let mut refiller = CacheRefiller::new(
            Role::Serving,
            test_refill_config(CacheRefillPolicy::Enabled),
            mock_sstable_store().await,
            spawn_refill_task,
        );

        refiller.replace_table_cache_refill_policies(HashMap::from([(
            table_id,
            CacheRefillPolicy::Serving,
        )]));
        refiller
            .replace_serving_table_vnode_mapping(HashMap::from([(table_id, old_vnodes.clone())]));

        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                insert_sst_infos: vec![SstableInfo::from(SstableInfoInner {
                    table_ids: vec![table_id],
                    ..Default::default()
                })],
                ..Default::default()
            }],
            pinned_version_for_test(),
            pinned_version_for_test(),
        );
        refiller.replace_table_cache_refill_policies(HashMap::from([(
            table_id,
            CacheRefillPolicy::Disabled,
        )]));
        refiller.replace_serving_table_vnode_mapping(HashMap::from([(table_id, new_vnodes)]));

        let captured_context = captured_context.lock();
        let context = captured_context
            .as_ref()
            .unwrap()
            .table_cache_refill_context_map
            .get(&table_id)
            .unwrap();
        assert_eq!(context.policy, CacheRefillPolicy::Serving);
        assert_eq!(context.serving_vnode_bitmap.as_ref(), Some(&old_vnodes));
    }

    #[tokio::test]
    async fn test_cache_refill_prunes_whole_ssts_before_meta_load() {
        let streaming_table = TableId::from(1);
        let serving_table = TableId::from(2);
        let disabled_table = TableId::from(3);
        let internal_table = TableId::from(4);
        let fallback_table = TableId::from(5);
        let serving_vnodes = Bitmap::ones(VirtualNode::COUNT_FOR_TEST);
        let sstable_store = mock_sstable_store().await;
        let sst_info = |table_ids: Vec<TableId>| {
            SstableInfo::from(SstableInfoInner {
                table_ids,
                ..Default::default()
            })
        };
        let capture_pruned_table_ids = |role,
                                        default_policy,
                                        policies: HashMap<TableId, CacheRefillPolicy>,
                                        streaming_table_vnodes: HashMap<TableId, Bitmap>,
                                        serving_table_vnodes: HashMap<TableId, Bitmap>,
                                        delta| {
            let captured_deltas = Arc::new(Mutex::new(None::<Vec<SstDeltaInfo>>));
            let captured_deltas_clone = captured_deltas.clone();
            let spawn_refill_task: SpawnRefillTask = Arc::new(move |deltas, _, _, _| {
                *captured_deltas_clone.lock() = Some(deltas);
                tokio::spawn(async {})
            });
            let mut refiller = CacheRefiller::new(
                role,
                test_refill_config(default_policy),
                sstable_store.clone(),
                spawn_refill_task,
            );
            refiller.replace_table_cache_refill_policies(policies);
            for (table_id, vnodes) in streaming_table_vnodes {
                refiller.update_streaming_table_vnodes(table_id, Some(vnodes));
            }
            refiller.replace_serving_table_vnode_mapping(serving_table_vnodes);
            refiller.start_cache_refill(
                vec![delta],
                pinned_version_for_test(),
                pinned_version_for_test(),
            );
            captured_deltas
                .lock()
                .take()
                .unwrap()
                .pop()
                .unwrap()
                .insert_sst_infos
                .into_iter()
                .map(|sst| sst.table_ids.clone())
                .collect::<Vec<_>>()
        };

        let normal_delta = |insert_sst_infos| SstDeltaInfo {
            insert_sst_infos,
            delete_sst_object_ids: vec![1.into()],
            insert_sst_level: 1,
        };
        let insert_only_delta = |insert_sst_infos| SstDeltaInfo {
            insert_sst_infos,
            delete_sst_object_ids: vec![],
            insert_sst_level: 0,
        };

        assert_eq!(
            capture_pruned_table_ids(
                Role::Both,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (disabled_table, CacheRefillPolicy::Disabled),
                    (streaming_table, CacheRefillPolicy::Enabled),
                ]),
                HashMap::new(),
                HashMap::new(),
                normal_delta(vec![
                    sst_info(vec![disabled_table]),
                    sst_info(vec![streaming_table]),
                ]),
            ),
            vec![vec![streaming_table]],
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Both,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Streaming),
                    (internal_table, CacheRefillPolicy::Serving),
                ]),
                HashMap::from([(streaming_table, serving_vnodes.clone())]),
                HashMap::new(),
                normal_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![internal_table]),
                ]),
            ),
            vec![vec![streaming_table]],
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Both,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Streaming),
                    (serving_table, CacheRefillPolicy::Serving),
                    (internal_table, CacheRefillPolicy::Serving),
                ]),
                HashMap::from([(streaming_table, serving_vnodes.clone())]),
                HashMap::from([(serving_table, serving_vnodes.clone())]),
                insert_only_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![serving_table]),
                    sst_info(vec![internal_table]),
                ]),
            ),
            vec![vec![serving_table]],
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Serving,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Streaming),
                    (serving_table, CacheRefillPolicy::Serving),
                ]),
                HashMap::new(),
                HashMap::from([(serving_table, serving_vnodes.clone())]),
                normal_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![serving_table]),
                ]),
            ),
            vec![vec![serving_table]],
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Serving,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Enabled),
                    (serving_table, CacheRefillPolicy::Enabled),
                ]),
                HashMap::new(),
                HashMap::from([(serving_table, serving_vnodes.clone())]),
                normal_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![serving_table]),
                    sst_info(vec![streaming_table, serving_table]),
                ]),
            ),
            vec![
                vec![streaming_table],
                vec![serving_table],
                vec![streaming_table, serving_table],
            ],
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Both,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Both),
                    (serving_table, CacheRefillPolicy::Both),
                ]),
                HashMap::new(),
                HashMap::new(),
                normal_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![serving_table]),
                ]),
            ),
            Vec::<Vec<TableId>>::new(),
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Both,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Both),
                    (serving_table, CacheRefillPolicy::Both),
                ]),
                HashMap::from([(streaming_table, serving_vnodes.clone())]),
                HashMap::new(),
                normal_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![serving_table]),
                ]),
            ),
            vec![vec![streaming_table]],
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Both,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Both),
                    (serving_table, CacheRefillPolicy::Both),
                ]),
                HashMap::new(),
                HashMap::from([(serving_table, serving_vnodes.clone())]),
                normal_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![serving_table]),
                ]),
            ),
            vec![vec![serving_table]],
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Streaming,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Streaming),
                    (serving_table, CacheRefillPolicy::Serving),
                ]),
                HashMap::new(),
                HashMap::new(),
                normal_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![serving_table]),
                ]),
            ),
            Vec::<Vec<TableId>>::new(),
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Streaming,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Streaming),
                    (serving_table, CacheRefillPolicy::Serving),
                ]),
                HashMap::new(),
                HashMap::new(),
                insert_only_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![serving_table]),
                ]),
            ),
            Vec::<Vec<TableId>>::new(),
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Both,
                CacheRefillPolicy::Enabled,
                HashMap::from([(disabled_table, CacheRefillPolicy::Disabled)]),
                HashMap::new(),
                HashMap::new(),
                normal_delta(vec![
                    sst_info(vec![disabled_table]),
                    sst_info(vec![fallback_table]),
                    sst_info(vec![disabled_table, fallback_table]),
                ]),
            ),
            vec![vec![fallback_table], vec![disabled_table, fallback_table]],
        );
    }

    #[tokio::test]
    async fn test_normal_refill_applies_policy_and_vnode_ownership() {
        let fixture = DataRefillGeneratorTestFixture::new(None).await;
        let delta = fixture.normal_l0_delta();
        let owned = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [0]);
        let unowned = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [1]);
        let cases = vec![
            ("Enabled", CacheRefillPolicy::Enabled, None, None, true),
            (
                "Disabled",
                CacheRefillPolicy::Disabled,
                Some(owned.clone()),
                Some(owned.clone()),
                false,
            ),
            (
                "Streaming match",
                CacheRefillPolicy::Streaming,
                Some(owned.clone()),
                None,
                true,
            ),
            (
                "Streaming miss",
                CacheRefillPolicy::Streaming,
                Some(unowned.clone()),
                None,
                false,
            ),
            (
                "Streaming ownership missing",
                CacheRefillPolicy::Streaming,
                None,
                None,
                false,
            ),
            (
                "Serving match",
                CacheRefillPolicy::Serving,
                None,
                Some(owned.clone()),
                true,
            ),
            (
                "Serving miss",
                CacheRefillPolicy::Serving,
                None,
                Some(unowned.clone()),
                false,
            ),
            (
                "Serving ownership missing",
                CacheRefillPolicy::Serving,
                None,
                None,
                false,
            ),
            (
                "Both streaming match",
                CacheRefillPolicy::Both,
                Some(owned.clone()),
                Some(unowned.clone()),
                true,
            ),
            (
                "Both serving match",
                CacheRefillPolicy::Both,
                Some(unowned.clone()),
                Some(owned),
                true,
            ),
            (
                "Both misses",
                CacheRefillPolicy::Both,
                Some(unowned.clone()),
                Some(unowned),
                false,
            ),
        ];

        for (name, policy, streaming_vnodes, serving_vnodes, should_refill) in cases {
            let context = fixture.context(policy, streaming_vnodes, serving_vnodes, |_| {});
            assert_eq!(
                !fixture.generate(&context, &delta).await.is_empty(),
                should_refill,
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn test_normal_refill_applies_recent_and_inheritance_filters() {
        let recent_filter = SimpleRecentFilter::new(3, Duration::from_secs(60));
        let fixture =
            DataRefillGeneratorTestFixture::new(Some(Arc::new(recent_filter.clone().into()))).await;
        let delta = fixture.normal_l0_delta();

        let serving_context = fixture.context(
            CacheRefillPolicy::Serving,
            None,
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            |config| {
                config.skip_recent_filter = false;
            },
        );
        assert!(
            fixture.generate(&serving_context, &delta).await.is_empty(),
            "explicit Serving policy is not an implicit skip_recent_filter"
        );

        recent_filter.insert((fixture.deleted_sst_object_id, usize::MAX));
        assert!(
            !fixture.generate(&serving_context, &delta).await.is_empty(),
            "explicit Serving policy should produce tasks after recent admission hits"
        );

        let (_, parent_sst_info) =
            gen_test_sst_with_object_id(fixture.table_id, fixture.sstable_store.clone(), 2).await;
        let non_l0_delta = fixture.normal_delta(1, parent_sst_info.object_id);
        let non_l0_context = fixture.context(CacheRefillPolicy::Enabled, None, None, |config| {
            config.data_refill_levels.insert(1);
            config.skip_recent_filter = false;
            config.skip_inheritance_filter = false;
        });

        recent_filter.insert((parent_sst_info.object_id, usize::MAX));
        let generator = DataCacheRefillTaskGenerator {
            context: &non_l0_context,
            delta: &non_l0_delta,
            ssts: std::slice::from_ref(&fixture.sst),
        };
        let unfiltered_tasks = generator.generate_unfiltered_tasks();
        assert_eq!(
            unfiltered_tasks
                .iter()
                .map(|task| task.blks.len())
                .sum::<usize>(),
            fixture.sst.block_count(),
            "recent-admitted blocks should reach the inheritance stage"
        );
        assert!(
            generator
                .filter_by_inheritance_if_needed(unfiltered_tasks)
                .await
                .is_empty(),
            "after recent admission, parent block recent miss should filter non-L0 normal refill"
        );

        recent_filter.insert((parent_sst_info.object_id, 0));
        let tasks = fixture.generate(&non_l0_context, &non_l0_delta).await;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].sst.id, fixture.sst.id);
        assert_eq!(tasks[0].blks, 0..1);
    }

    #[tokio::test]
    async fn test_l0_insert_only_refill_policy_uses_serving_ownership() {
        let fixture = DataRefillGeneratorTestFixture::new(None).await;
        let delta = fixture.l0_insert_only_delta();
        let cases = [
            (
                "Enabled + serving overlap",
                CacheRefillPolicy::Enabled,
                None,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                true,
            ),
            (
                "Enabled without serving ownership",
                CacheRefillPolicy::Enabled,
                None,
                None,
                false,
            ),
            (
                "Serving + serving overlap",
                CacheRefillPolicy::Serving,
                None,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                true,
            ),
            (
                "Streaming + streaming overlap",
                CacheRefillPolicy::Streaming,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                None,
                false,
            ),
            (
                "Both + streaming overlap + serving non-overlap",
                CacheRefillPolicy::Both,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                Some(Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [1])),
                false,
            ),
            (
                "Both + serving overlap",
                CacheRefillPolicy::Both,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                true,
            ),
            (
                "Disabled + serving overlap",
                CacheRefillPolicy::Disabled,
                None,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                false,
            ),
        ];

        for (name, policy, streaming_vnodes, serving_vnodes, should_refill) in cases {
            let context = fixture.context(policy, streaming_vnodes, serving_vnodes, |config| {
                config.skip_recent_filter = false;
                config.skip_inheritance_filter = false;
            });
            assert_eq!(
                !fixture.generate(&context, &delta).await.is_empty(),
                should_refill,
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn test_refill_units_do_not_cross_table_projection_boundaries() {
        let table_a = TableId::from(233);
        let table_b = TableId::from(234);
        let sstable_store = mock_sstable_store().await;
        let (sst, sst_info) = gen_test_sstable_with_table_ids(
            default_builder_opt_for_test(),
            1,
            [table_a, table_b].into_iter().map(|table_id| {
                (
                    FullKey {
                        user_key: UserKey::for_test(table_id, iterator_test_table_key_of(0)),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                    },
                    HummockValue::put(b"value".to_vec()),
                )
            }),
            sstable_store.clone(),
            vec![table_a.as_raw_id(), table_b.as_raw_id()],
        )
        .await;
        assert_eq!(sst.block_count(), 2, "table switch must form a new block");

        let mut next_sst_id = 100.into();
        let (table_a_projection, table_b_projection) =
            split_sst_with_table_ids(&sst_info, &mut next_sst_id, 1, 1, vec![table_b]);
        assert_eq!(table_a_projection.object_id, sst_info.object_id);
        assert_eq!(table_b_projection.object_id, sst_info.object_id);
        assert_ne!(table_a_projection.sst_id, table_b_projection.sst_id);
        assert_eq!(table_a_projection.table_ids, vec![table_a]);
        assert_eq!(table_b_projection.table_ids, vec![table_b]);

        let deltas = [table_a_projection, table_b_projection].map(|projection| SstDeltaInfo {
            insert_sst_infos: vec![projection],
            delete_sst_object_ids: vec![],
            insert_sst_level: 0,
        });
        let normal_deltas = deltas.clone().map(|mut delta| {
            // A synthetic delete marks this as a normal delta; recent and inheritance filters
            // are disabled below, so the test does not rely on a matching parent SST.
            delta.delete_sst_object_ids = vec![999.into()];
            delta
        });
        let serving_vnodes = Bitmap::ones(VirtualNode::COUNT_FOR_TEST);
        let table_cache_refill_context_map = Arc::new(
            [table_a, table_b]
                .into_iter()
                .map(|table_id| {
                    (
                        table_id,
                        super::TableCacheRefillContext {
                            streaming_vnode_bitmap: None,
                            serving_vnode_bitmap: Some(serving_vnodes.clone()),
                            policy: CacheRefillPolicy::Serving,
                        },
                    )
                })
                .collect::<super::TableCacheRefillContextMap>(),
        );
        let make_context = |unit| {
            let mut config = test_refill_config(CacheRefillPolicy::Disabled);
            config.data_refill_levels.insert(0);
            config.unit = unit;
            CacheRefillContext {
                config: Arc::new(config),
                meta_refill_concurrency: None,
                concurrency: Arc::new(tokio::sync::Semaphore::new(1)),
                sstable_store: sstable_store.clone(),
                table_cache_refill_context_map: table_cache_refill_context_map.clone(),
            }
        };
        let generated_tasks = |context: &CacheRefillContext| {
            deltas
                .iter()
                .map(|delta| {
                    DataCacheRefillTaskGenerator {
                        context,
                        delta,
                        ssts: std::slice::from_ref(&sst),
                    }
                    .generate_unfiltered_tasks()
                })
                .collect::<Vec<_>>()
        };
        let generated_ranges = |context: &CacheRefillContext| {
            generated_tasks(context)
                .into_iter()
                .map(|tasks| tasks.into_iter().map(|task| task.blks).collect::<Vec<_>>())
                .collect::<Vec<_>>()
        };

        assert_eq!(
            generated_ranges(&make_context(1)),
            vec![vec![0..1], vec![1..2]],
            "each logical projection must select only its own block"
        );

        let wide_unit_context = make_context(2);
        assert_eq!(
            generated_ranges(&wide_unit_context),
            vec![vec![0..1], vec![1..2]],
            "units are clipped at table boundaries even when unit is larger than a table run"
        );

        let normal_ranges = normal_deltas
            .iter()
            .map(|delta| {
                DataCacheRefillTaskGenerator {
                    context: &wide_unit_context,
                    delta,
                    ssts: std::slice::from_ref(&sst),
                }
                .generate_unfiltered_tasks()
                .into_iter()
                .map(|task| task.blks)
                .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            normal_ranges,
            vec![vec![0..1], vec![1..2]],
            "normal refill uses the same table-boundary geometry"
        );

        for task in generated_tasks(&wide_unit_context).into_iter().flatten() {
            assert!(task.blks.len() <= wide_unit_context.config.unit);
            assert_eq!(
                task.sst.meta.block_metas[task.blks.start].table_id(),
                task.sst.meta.block_metas[task.blks.end - 1].table_id(),
                "a refill unit must not cross a table boundary"
            );
        }
    }

    #[tokio::test]
    async fn test_scoped_refill_handles_multi_table_vnode_boundary() {
        let table_a = TableId::from(233);
        let table_b = TableId::from(234);
        let vnode_a = VirtualNode::COUNT_FOR_TEST - 1;
        let sstable_store = mock_sstable_store().await;
        let (sst, sst_info) = gen_test_sstable_with_table_ids(
            default_builder_opt_for_test(),
            1,
            [
                (
                    FullKey {
                        user_key: UserKey::for_test(
                            table_a,
                            prefix_slice_with_vnode(VirtualNode::from_index(vnode_a), b"table_a"),
                        ),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                    },
                    HummockValue::put(Bytes::from_static(b"a")),
                ),
                (
                    FullKey {
                        user_key: UserKey::for_test(
                            table_b,
                            prefix_slice_with_vnode(VirtualNode::ZERO, b"table_b"),
                        ),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                    },
                    HummockValue::put(Bytes::from_static(b"b")),
                ),
            ]
            .into_iter(),
            sstable_store.clone(),
            vec![table_a.as_raw_id(), table_b.as_raw_id()],
        )
        .await;
        assert_eq!(sst.block_count(), 2, "table switch must form a new block");

        let generate = |streaming_vnodes| {
            let sstable_store = sstable_store.clone();
            let sst = sst.clone();
            let sst_info = sst_info.clone();
            let mut config = test_refill_config(CacheRefillPolicy::Streaming);
            config.data_refill_levels.insert(0);
            let context = CacheRefillContext {
                config: Arc::new(config),
                meta_refill_concurrency: None,
                concurrency: Arc::new(tokio::sync::Semaphore::new(1)),
                sstable_store,
                table_cache_refill_context_map: Arc::new(HashMap::from([(
                    table_a,
                    super::TableCacheRefillContext {
                        streaming_vnode_bitmap: Some(streaming_vnodes),
                        serving_vnode_bitmap: None,
                        policy: CacheRefillPolicy::Streaming,
                    },
                )])),
            };
            async move {
                let generator = DataCacheRefillTaskGenerator {
                    context: &context,
                    delta: &SstDeltaInfo {
                        insert_sst_infos: vec![sst_info.clone()],
                        delete_sst_object_ids: vec![2330.into()],
                        insert_sst_level: 0,
                    },
                    ssts: std::slice::from_ref(&sst),
                };
                let tasks = generator.generate_unfiltered_tasks();
                generator.filter_by_inheritance_if_needed(tasks).await
            }
        };

        let matching_tasks =
            generate(Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [vnode_a])).await;
        assert_eq!(matching_tasks.len(), 1);
        assert_eq!(matching_tasks[0].blks, 0..1);

        let non_matching_tasks = generate(Bitmap::from_indices(
            VirtualNode::COUNT_FOR_TEST,
            [VirtualNode::ZERO.to_index()],
        ))
        .await;
        assert!(non_matching_tasks.is_empty());
    }

    #[tokio::test]
    async fn test_block_vnode_range_handles_vnode_only_block_boundaries() {
        let table_id = TableId::from(233);
        let vnode = VirtualNode::ZERO;
        let sstable_store = mock_sstable_store().await;
        let mut builder_options = default_builder_opt_for_test();
        builder_options.block_capacity = 1;
        let (sst, _) = gen_test_sstable_with_table_ids(
            builder_options,
            1,
            [234, 233].into_iter().map(|epoch| {
                (
                    FullKey {
                        user_key: UserKey::for_test(table_id, prefix_slice_with_vnode(vnode, b"")),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(epoch)),
                    },
                    HummockValue::put(Bytes::from_static(b"value")),
                )
            }),
            sstable_store.clone(),
            vec![table_id.as_raw_id()],
        )
        .await;
        assert_eq!(sst.block_count(), 2);
        let expected = (vnode.to_index(), vnode.to_index() + 1);
        assert_eq!(block_vnode_range(&sst, 0), expected);
        assert_eq!(block_vnode_range(&sst, 1), expected);
    }

    #[tokio::test]
    async fn test_block_vnode_range_fails_open_for_shortened_meta_keys() {
        let table_id = TableId::from(233);
        let sstable_store = mock_sstable_store().await;
        let mut builder_options = default_builder_opt_for_test();
        builder_options.block_capacity = 1;
        builder_options.shorten_block_meta_key_threshold = Some(0);
        let (sst, _) = gen_test_sstable_with_table_ids(
            builder_options,
            1,
            [255, 256].into_iter().map(|vnode| {
                (
                    FullKey {
                        user_key: UserKey::for_test(
                            table_id,
                            prefix_slice_with_vnode(VirtualNode::from_index(vnode), b"long-key"),
                        ),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                    },
                    HummockValue::put(Bytes::from_static(b"value")),
                )
            }),
            sstable_store,
            vec![table_id.as_raw_id()],
        )
        .await;
        assert_eq!(sst.block_count(), 2);
        assert!(
            FullKey::decode(&sst.meta.block_metas[1].smallest_key)
                .user_key
                .table_key
                .as_ref()
                .len()
                < VirtualNode::SIZE
        );
        let full_range = (0, VirtualNode::MAX_REPRESENTABLE.to_index() + 1);
        assert_eq!(block_vnode_range(&sst, 0), full_range);
        assert_eq!(block_vnode_range(&sst, 1), full_range);
    }

    #[test]
    fn test_vnode_range_overlaps_bitmap_uses_right_exclusive_end() {
        let right_exclusive = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [12]);
        assert!(!vnode_range_overlaps_bitmap((10, 12), &right_exclusive));

        let inside_range = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [11]);
        assert!(vnode_range_overlaps_bitmap((10, 12), &inside_range));

        let last_vnode = Bitmap::from_indices(
            VirtualNode::COUNT_FOR_TEST,
            [VirtualNode::COUNT_FOR_TEST - 1],
        );
        assert!(vnode_range_overlaps_bitmap(
            (VirtualNode::COUNT_FOR_TEST - 1, VirtualNode::COUNT_FOR_TEST),
            &last_vnode
        ));
        assert!(!vnode_range_overlaps_bitmap(
            (VirtualNode::COUNT_FOR_TEST, VirtualNode::COUNT_FOR_TEST + 1),
            &last_vnode
        ));
    }
}
