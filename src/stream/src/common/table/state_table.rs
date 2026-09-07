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

use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::ops::Bound;
use std::ops::Bound::*;
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use educe::Educe;
use either::Either;
use foyer::Hint;
use futures::future::{ready, try_join_all};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt, pin_mut};
use itertools::Itertools;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{ArrayImplBuilder, ArrayRef, DataChunk, Op, StreamChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{
    ColumnDesc, ColumnId, TableId, TableOption, get_dist_key_in_pk_indices,
};
use risingwave_common::config::StreamingConfig;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt, VnodeCountCompat};
use risingwave_common::id::FragmentId;
use risingwave_common::row::{self, OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::{OrderType, cmp_datum};
use risingwave_common::util::value_encoding::BasicSerde;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_hummock_sdk::key::{
    CopyFromSlice, TableKey, end_bound_of_prefix, next_key, prefix_slice_with_vnode,
    prefixed_range_with_vnode, start_bound_of_excluded_prefix,
};
use risingwave_hummock_sdk::table_watermark::{
    VnodeWatermark, WatermarkDirection, WatermarkSerdeType,
};
use risingwave_pb::catalog::Table;
use risingwave_pb::plan_common::StorageTableDesc;
use risingwave_storage::StateStore;
use risingwave_storage::error::{ErrorKind, StorageError, StorageResult};
use risingwave_storage::hummock::CachePolicy;
use risingwave_storage::mem_table::MemTableError;
use risingwave_storage::row_serde::find_columns_by_ids;
use risingwave_storage::row_serde::row_serde_util::{
    deserialize_pk_with_vnode, serialize_pk, serialize_pk_with_vnode, serialize_row,
};
use risingwave_storage::row_serde::value_serde::ValueRowSerde;
use risingwave_storage::store::*;
use risingwave_storage::table::{KeyedRow, TableDistribution, should_calculate_prefix_hint};
use thiserror_ext::AsReport;
use tracing::{Instrument, trace};

use crate::cache::keyed_cache_may_stale;
use crate::executor::monitor::streaming_stats::StateTableMetrics;
use crate::executor::{StreamExecutorError, StreamExecutorResult};

/// This macro is used to mark a point where we want to randomly discard the operation and early
/// return, only in insane mode.
macro_rules! insane_mode_discard_point {
    () => {{
        use rand::Rng;
        if crate::consistency::insane() && rand::rng().random_bool(0.3) {
            return;
        }
    }};
}

/// Per-vnode statistics for pruning. None means this stat is not maintained.
/// For each vnode, we maintain the min and max storage table key (excluding the vnode part) observed in the vnode.
/// The stat won't differentiate between tombstone and normal keys.
struct VnodeStatistics {
    min_key: Option<Bytes>,
    max_key: Option<Bytes>,
}

impl VnodeStatistics {
    fn new() -> Self {
        Self {
            min_key: None,
            max_key: None,
        }
    }

    fn update_with_key(&mut self, key: &Bytes) {
        if let Some(min) = &self.min_key {
            if key < min {
                self.min_key = Some(key.clone());
            }
        } else {
            self.min_key = Some(key.clone());
        }

        if let Some(max) = &self.max_key {
            if key > max {
                self.max_key = Some(key.clone());
            }
        } else {
            self.max_key = Some(key.clone());
        }
    }

    fn can_prune(&self, key: &Bytes) -> bool {
        if let Some(min) = &self.min_key
            && key < min
        {
            return true;
        }
        if let Some(max) = &self.max_key
            && key > max
        {
            return true;
        }
        false
    }

    fn can_prune_range(&self, start: &Bound<Bytes>, end: &Bound<Bytes>) -> bool {
        // Check if the range is completely outside vnode bounds
        if let Some(max) = &self.max_key {
            match start {
                Included(s) if s > max => return true,
                Excluded(s) if s >= max => return true,
                _ => {}
            }
        }
        if let Some(min) = &self.min_key {
            match end {
                Included(e) if e < min => return true,
                Excluded(e) if e <= min => return true,
                _ => {}
            }
        }
        false
    }

    fn pruned_key_range(
        &self,
        start: &Bound<Bytes>,
        end: &Bound<Bytes>,
    ) -> Option<(Bound<Bytes>, Bound<Bytes>)> {
        if self.can_prune_range(start, end) {
            return None;
        }
        let new_start = if let Some(min) = &self.min_key {
            match start {
                Included(s) if s <= min => Included(min.clone()),
                Excluded(s) if s < min => Included(min.clone()),
                _ => start.clone(),
            }
        } else {
            start.clone()
        };

        let new_end = if let Some(max) = &self.max_key {
            match end {
                Included(e) if e >= max => Included(max.clone()),
                Excluded(e) if e > max => Included(max.clone()),
                _ => end.clone(),
            }
        } else {
            end.clone()
        };

        Some((new_start, new_end))
    }
}

/// `StateTableInner` is the interface accessing relational data in KV(`StateStore`) with
/// row-based encoding.
pub struct StateTableInner<S, SD = BasicSerde, const IS_REPLICATED: bool = false>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    /// Id for this table.
    table_id: TableId,

    /// State store backend.
    row_store: StateTableRowStore<S::Local, SD>,

    /// State store for accessing snapshot data
    store: S,

    /// Current epoch
    epoch: Option<EpochPair>,

    /// Used for serializing and deserializing the primary key.
    pk_serde: OrderedRowSerde,

    /// Indices of primary key.
    /// Note that the index is based on the all columns of the table, instead of the output ones.
    // FIXME: revisit constructions and usages.
    pk_indices: Vec<usize>,

    /// Distribution of the state table.
    ///
    /// It holds vnode bitmap. Only the rows whose vnode of the primary key is in this set will be visible to the
    /// executor. The table will also check whether the written rows
    /// conform to this partition.
    distribution: TableDistribution,

    prefix_hint_len: usize,

    value_indices: Option<Vec<usize>>,

    /// The index of the watermark column used for state cleaning in all columns.
    pub clean_watermark_index: Option<usize>,
    /// Pending watermark for state cleaning. Old states below this watermark will be cleaned when committing.
    pending_watermark: Option<ScalarImpl>,
    /// Last committed watermark for state cleaning. Will be restored on state table recovery.
    committed_watermark: Option<ScalarImpl>,
    /// Serializer and serde type for the watermark column.
    watermark_serde: Option<(OrderedRowSerde, WatermarkSerdeType)>,

    /// Data Types
    /// We will need to use to build data chunks from state table rows.
    data_types: Vec<DataType>,

    /// "i" here refers to the base `state_table`'s actual schema.
    /// "o" here refers to the replicated state table's output schema.
    /// This mapping is used to reconstruct a row being written from replicated state table.
    /// Such that the schema of this row will match the full schema of the base state table.
    /// It is only applicable for replication.
    i2o_mapping: ColIndexMapping,

    /// Output indices
    /// Used for:
    /// 1. Computing `output_value_indices` to ser/de replicated rows.
    /// 2. Computing output pk indices to used them for backfill state.
    pub output_indices: Vec<usize>,

    op_consistency_level: StateTableOpConsistencyLevel,

    /// Flag to indicate whether the state table has called `commit`, but has not called
    /// `post_yield_barrier` on the `StateTablePostCommit` callback yet.
    on_post_commit: bool,
}

/// `StateTable` will use `BasicSerde` as default
pub type StateTable<S> = StateTableInner<S, BasicSerde>;
/// `ReplicatedStateTable` is meant to replicate upstream shared buffer.
/// Used for `ArrangementBackfill` executor.
pub type ReplicatedStateTable<S, SD> = StateTableInner<S, SD, true>;

pub type FlushedStateTableReader<S, SD = BasicSerde> = StateTableFlushedSnapshotReader<
    <<S as StateStore>::Local as LocalStateStore>::FlushedSnapshotReader,
    SD,
>;

#[derive(Educe)]
#[educe(Clone)]
pub struct StateTableFlushedSnapshotReader<R, SD = BasicSerde>
where
    R: StateStoreRead,
    SD: ValueRowSerde,
{
    reader: Arc<R>,
    pk_serde: OrderedRowSerde,
    vnodes: Arc<Bitmap>,
    row_serde: Arc<SD>,
    metrics: Option<StateTableMetrics>,
}

// initialize
impl<S, SD, const IS_REPLICATED: bool> StateTableInner<S, SD, IS_REPLICATED>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    /// In streaming executors, this methods must be called **after** receiving and yielding the first barrier,
    /// and otherwise, deadlock can be likely to happen.
    pub async fn init_epoch(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.row_store
            .init(epoch, self.distribution.vnodes())
            .await?;
        assert_eq!(None, self.epoch.replace(epoch), "should not init for twice");
        Ok(())
    }

    pub async fn try_wait_committed_epoch(&self, prev_epoch: u64) -> StorageResult<()> {
        self.store
            .try_wait_epoch(
                HummockReadEpoch::Committed(prev_epoch),
                TryWaitEpochOptions {
                    table_id: self.table_id,
                },
            )
            .await
    }

    pub fn state_store(&self) -> &S {
        &self.store
    }
}

fn consistent_old_value_op(
    row_serde: Arc<impl ValueRowSerde>,
    is_log_store: bool,
) -> OpConsistencyLevel {
    OpConsistencyLevel::ConsistentOldValue {
        check_old_value: Arc::new(move |first: &Bytes, second: &Bytes| {
            if first == second {
                return true;
            }
            let first = match row_serde.deserialize(first) {
                Ok(rows) => rows,
                Err(e) => {
                    error!(error = %e.as_report(), value = ?first, "failed to deserialize the serialized value");
                    return false;
                }
            };
            let second = match row_serde.deserialize(second) {
                Ok(rows) => rows,
                Err(e) => {
                    error!(error = %e.as_report(), value = ?second, "failed to deserialize the serialized value");
                    return false;
                }
            };
            if first != second {
                error!(first = ?first, second = ?second, "sanity check fail");
                false
            } else {
                true
            }
        }),
        is_log_store,
    }
}

macro_rules! dispatch_value_indices {
    ($value_indices:expr, [$($row_var_name:ident),+], $body:expr) => {
        if let Some(value_indices) = $value_indices {
            $(
                let $row_var_name = $row_var_name.project(value_indices);
            )+
            $body
        } else {
            $body
        }
    };
}

/// Extract the logic of `StateTableRowStore` from `StateTable`, which serves
/// a similar functionality as a `BTreeMap<TableKey<Bytes>, OwnedRow>`, and
/// provides method to read (`get` and `iter`) over serialized key (or key range)
/// and returns `OwnedRow`, and write (`insert`, `delete`, `update`) on `(TableKey<Bytes>, OwnedRow)`.
struct StateTableRowStore<LS: LocalStateStore, SD: ValueRowSerde> {
    state_store: LS,
    all_rows: Option<HashMap<VirtualNode, BTreeMap<Bytes, OwnedRow>>>,

    table_id: TableId,
    row_serde: Arc<SD>,
    // should be only used for debugging in panic message of handle_mem_table_error
    pk_serde: OrderedRowSerde,

    // Per-vnode min/max key statistics for pruning
    vnode_stats: Option<HashMap<VirtualNode, VnodeStatistics>>,
    /// When false, vnode stats pruning is in dry-run mode:
    /// we maintain stats and verify pruning correctness but don't actually apply pruning.
    /// Reads still go to cache/storage even when pruning would indicate no results.
    enable_state_table_vnode_stats_pruning: bool,
    // Optional metrics for state table operations
    pub metrics: Option<StateTableMetrics>,
}

impl<LS: LocalStateStore, SD: ValueRowSerde> StateTableRowStore<LS, SD> {
    async fn may_load_vnode_stats(&mut self, vnode_bitmap: &Bitmap) -> StreamExecutorResult<()> {
        if self.vnode_stats.is_none() {
            return Ok(());
        }

        // vnode stats must be disabled when all rows are preloaded
        assert!(self.all_rows.is_none());

        let start_time = Instant::now();
        let mut stats_map = HashMap::new();

        // Scan each vnode to get min/max keys
        for vnode in vnode_bitmap.iter_vnodes() {
            let mut stats = VnodeStatistics::new();

            // Get min key via forward iteration
            let memcomparable_range_with_vnode = prefixed_range_with_vnode::<Bytes>(.., vnode);
            let read_options = ReadOptions {
                cache_policy: CachePolicy::Fill(Hint::Low),
                ..Default::default()
            };

            let mut iter = self
                .state_store
                .iter(memcomparable_range_with_vnode.clone(), read_options.clone())
                .await?;
            if let Some(item) = iter.try_next().await? {
                let (key_vnode, key_without_vnode) = item.0.user_key.table_key.split_vnode();
                assert_eq!(vnode, key_vnode);
                stats.min_key = Some(Bytes::copy_from_slice(key_without_vnode));
            }

            // Get max key via reverse iteration
            let mut rev_iter = self
                .state_store
                .rev_iter(memcomparable_range_with_vnode, read_options)
                .await?;
            if let Some(item) = rev_iter.try_next().await? {
                let (key_vnode, key_without_vnode) = item.0.user_key.table_key.split_vnode();
                assert_eq!(vnode, key_vnode);
                stats.max_key = Some(Bytes::copy_from_slice(key_without_vnode));
            }

            stats_map.insert(vnode, stats);
        }

        self.vnode_stats = Some(stats_map);

        // avoid flooding e2e-test log
        if !cfg!(debug_assertions) {
            info!(
                table_id = %self.table_id,
                vnode_count = vnode_bitmap.count_ones(),
                duration = ?start_time.elapsed(),
                "finished initializing vnode statistics"
            );
        }

        Ok(())
    }

    async fn may_reload_all_rows(&mut self, vnode_bitmap: &Bitmap) -> StreamExecutorResult<()> {
        if let Some(rows) = &mut self.all_rows {
            rows.clear();
            let start_time = Instant::now();
            *rows = try_join_all(vnode_bitmap.iter_vnodes().map(|vnode| {
                let state_store = &self.state_store;
                let row_serde = &self.row_serde;
                async move {
                    let mut rows = BTreeMap::new();
                    let memcomparable_range_with_vnode =
                        prefixed_range_with_vnode::<Bytes>(.., vnode);
                    // TODO: set read options
                    let stream = deserialize_keyed_row_stream::<Bytes>(
                        state_store
                            .iter(
                                memcomparable_range_with_vnode,
                                ReadOptions {
                                    prefix_hint: None,
                                    prefetch_options: Default::default(),
                                    cache_policy: Default::default(),
                                },
                            )
                            .await?,
                        &**row_serde,
                    );
                    pin_mut!(stream);
                    while let Some((encoded_key, row)) = stream.try_next().await? {
                        let key = TableKey(encoded_key);
                        let (iter_vnode, key) = key.split_vnode_bytes();
                        assert_eq!(vnode, iter_vnode);
                        rows.try_insert(key, row).expect("non-duplicated");
                    }
                    Ok((vnode, rows)) as StreamExecutorResult<_>
                }
            }))
            .await?
            .into_iter()
            .collect();
            // avoid flooding e2e-test log
            if !cfg!(debug_assertions) {
                info!(table_id = %self.table_id, vnode_count = vnode_bitmap.count_ones(), duration = ?start_time.elapsed(),"finished reloading all rows");
            }
        }
        Ok(())
    }

    async fn init(&mut self, epoch: EpochPair, vnode_bitmap: &Bitmap) -> StreamExecutorResult<()> {
        self.state_store.init(InitOptions::new(epoch)).await?;
        self.may_reload_all_rows(vnode_bitmap).await?;
        self.may_load_vnode_stats(vnode_bitmap).await
    }

    async fn update_vnode_bitmap(
        &mut self,
        vnodes: Arc<Bitmap>,
    ) -> StreamExecutorResult<Arc<Bitmap>> {
        let prev_vnodes = self.state_store.update_vnode_bitmap(vnodes.clone()).await?;
        self.may_reload_all_rows(&vnodes).await?;
        self.may_load_vnode_stats(&vnodes).await?;

        Ok(prev_vnodes)
    }

    async fn try_flush(&mut self) -> StreamExecutorResult<()> {
        self.state_store.try_flush().await?;
        Ok(())
    }

    async fn seal_current_epoch(
        &mut self,
        next_epoch: u64,
        table_watermarks: Option<(WatermarkDirection, Vec<VnodeWatermark>, WatermarkSerdeType)>,
        switch_consistent_op: Option<StateTableOpConsistencyLevel>,
    ) -> StreamExecutorResult<()> {
        if let Some((direction, watermarks, serde_type)) = &table_watermarks
            && let Some(rows) = &mut self.all_rows
        {
            match serde_type {
                WatermarkSerdeType::PkPrefix => {
                    for vnode_watermark in watermarks {
                        match direction {
                            WatermarkDirection::Ascending => {
                                for vnode in vnode_watermark.vnode_bitmap().iter_vnodes() {
                                    let rows = rows.get_mut(&vnode).expect("covered vnode");
                                    // split_off returns everything after the given key, including the key.
                                    *rows = rows.split_off(vnode_watermark.watermark());
                                }
                            }
                            WatermarkDirection::Descending => {
                                // Turn Exclude(vnode_watermark.watermark()) into Include(next_key(vnode_watermark.watermark())).
                                let split_off_key = next_key(vnode_watermark.watermark());
                                for vnode in vnode_watermark.vnode_bitmap().iter_vnodes() {
                                    let rows = rows.get_mut(&vnode).expect("covered vnode");
                                    // split_off away (Exclude(vnode_watermark.watermark())..) and keep
                                    // (..Include(vnode_watermark.watermark()))
                                    rows.split_off(split_off_key.as_slice());
                                }
                            }
                        }
                    }
                }
                WatermarkSerdeType::NonPkPrefix => {
                    warn!(table_id = %self.table_id, "table enabled preloading rows got disabled by written non pk prefix watermark");
                    self.all_rows = None;
                }
                WatermarkSerdeType::Value => {
                    warn!(table_id = %self.table_id, "table enabled preloading rows got disabled by written value watermark");
                    self.all_rows = None;
                }
            }
        }
        self.state_store
            .flush()
            .instrument(tracing::info_span!("state_table_flush"))
            .await?;
        let switch_op_consistency_level =
            switch_consistent_op.map(|new_consistency_level| match new_consistency_level {
                StateTableOpConsistencyLevel::Inconsistent => OpConsistencyLevel::Inconsistent,
                StateTableOpConsistencyLevel::ConsistentOldValue => {
                    consistent_old_value_op(self.row_serde.clone(), false)
                }
                StateTableOpConsistencyLevel::LogStoreEnabled => {
                    consistent_old_value_op(self.row_serde.clone(), true)
                }
            });
        self.state_store.seal_current_epoch(
            next_epoch,
            SealCurrentEpochOptions {
                table_watermarks,
                switch_op_consistency_level,
            },
        );
        Ok(())
    }
}

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum StateTableOpConsistencyLevel {
    /// Op is inconsistent
    Inconsistent,
    /// Op is consistent.
    /// - Insert op should ensure that the key does not exist previously
    /// - Delete and Update op should ensure that the key exists and the previous value matches the passed old value
    ConsistentOldValue,
    /// The requirement on operation consistency is the same as `ConsistentOldValue`.
    /// The difference is that in the `LogStoreEnabled`, the state table should also flush and store and old value.
    LogStoreEnabled,
}

pub struct StateTableBuilder<S, SD, const IS_REPLICATED: bool, PreloadAllRow> {
    // Extracted intermediate fields (source-agnostic)
    table_id: TableId,
    table_name_for_debug: String,
    table_columns: Vec<ColumnDesc>,
    order_types: Vec<OrderType>,
    pk_indices: Vec<usize>,
    dist_key_in_pk_indices: Vec<usize>,
    vnode_col_idx_in_pk: Option<usize>,
    expected_vnode_count: usize,
    value_indices: Vec<usize>,
    prefix_hint_len: usize,
    retention_seconds: Option<u32>,
    versioned: bool,
    fragment_id: FragmentId,
    clean_watermark_index: Option<usize>,

    // Builder configuration fields
    store: S,
    vnodes: Option<Arc<Bitmap>>,
    op_consistency_level: Option<StateTableOpConsistencyLevel>,
    output_column_ids: Option<Vec<ColumnId>>,
    preload_all_rows: PreloadAllRow,
    enable_vnode_key_stats: Option<bool>,
    /// When false, vnode stats pruning is in dry-run mode:
    /// we maintain stats and verify pruning correctness but don't actually apply pruning.
    enable_state_table_vnode_stats_pruning: bool,
    metrics: Option<StateTableMetrics>,

    _serde: PhantomData<SD>,
}

impl<S: StateStore, SD: ValueRowSerde, const IS_REPLICATED: bool>
    StateTableBuilder<S, SD, IS_REPLICATED, ()>
{
    fn with_preload_all_rows(
        self,
        preload_all_rows: bool,
    ) -> StateTableBuilder<S, SD, IS_REPLICATED, bool> {
        StateTableBuilder {
            table_id: self.table_id,
            table_name_for_debug: self.table_name_for_debug,
            table_columns: self.table_columns,
            order_types: self.order_types,
            pk_indices: self.pk_indices,
            dist_key_in_pk_indices: self.dist_key_in_pk_indices,
            vnode_col_idx_in_pk: self.vnode_col_idx_in_pk,
            expected_vnode_count: self.expected_vnode_count,
            value_indices: self.value_indices,
            prefix_hint_len: self.prefix_hint_len,
            retention_seconds: self.retention_seconds,
            versioned: self.versioned,
            fragment_id: self.fragment_id,
            clean_watermark_index: self.clean_watermark_index,
            store: self.store,
            vnodes: self.vnodes,
            op_consistency_level: self.op_consistency_level,
            output_column_ids: self.output_column_ids,
            preload_all_rows,
            enable_vnode_key_stats: self.enable_vnode_key_stats,
            enable_state_table_vnode_stats_pruning: self.enable_state_table_vnode_stats_pruning,
            metrics: self.metrics,
            _serde: Default::default(),
        }
    }

    pub fn enable_preload_all_rows_by_config(
        self,
        config: &StreamingConfig,
    ) -> StateTableBuilder<S, SD, IS_REPLICATED, bool> {
        let developer = &config.developer;
        let preload_all_rows = if developer.default_enable_mem_preload_state_table {
            !developer
                .mem_preload_state_table_ids_blacklist
                .contains(&self.table_id.as_raw_id())
        } else {
            developer
                .mem_preload_state_table_ids_whitelist
                .contains(&self.table_id.as_raw_id())
        };
        self.with_preload_all_rows(preload_all_rows)
    }

    pub fn forbid_preload_all_rows(self) -> StateTableBuilder<S, SD, IS_REPLICATED, bool> {
        self.with_preload_all_rows(false)
    }
}

impl<S: StateStore, SD: ValueRowSerde, const IS_REPLICATED: bool, PreloadAllRow>
    StateTableBuilder<S, SD, IS_REPLICATED, PreloadAllRow>
{
    pub fn with_op_consistency_level(
        mut self,
        op_consistency_level: StateTableOpConsistencyLevel,
    ) -> Self {
        self.op_consistency_level = Some(op_consistency_level);
        self
    }

    pub fn enable_vnode_key_stats(mut self, enable: bool, config: &StreamingConfig) -> Self {
        self.enable_vnode_key_stats = Some(enable);
        self.enable_state_table_vnode_stats_pruning =
            enable && config.developer.enable_state_table_vnode_stats_pruning;
        self
    }

    pub fn with_metrics(mut self, metrics: StateTableMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }
}

impl<S: StateStore, SD: ValueRowSerde, PreloadAllRow>
    StateTableBuilder<S, SD, true, PreloadAllRow>
{
    pub fn with_output_column_ids(mut self, output_column_ids: Vec<ColumnId>) -> Self {
        self.output_column_ids = Some(output_column_ids);
        self
    }
}

impl<S: StateStore, SD: ValueRowSerde, const IS_REPLICATED: bool>
    StateTableBuilder<S, SD, IS_REPLICATED, bool>
{
    pub async fn build(self) -> StateTableInner<S, SD, IS_REPLICATED> {
        let mut preload_all_rows = self.preload_all_rows;
        if preload_all_rows
            && let Err(e) =
                risingwave_common::license::Feature::StateTableMemoryPreload.check_available()
        {
            warn!(table_id=%self.table_id, e=%e.as_report(), "table configured to preload rows to memory but disabled by license");
            preload_all_rows = false;
        }

        let should_enable_vnode_key_stats = if preload_all_rows
            && let Some(enable_vnode_key_stats) = self.enable_vnode_key_stats
            && enable_vnode_key_stats
        {
            false
        } else {
            self.enable_vnode_key_stats.unwrap_or(false)
        };
        self.build_inner(preload_all_rows, should_enable_vnode_key_stats)
            .await
    }
}

// initialize
// FIXME(kwannoel): Enforce that none of the constructors here
// should be used by replicated state table.
// Apart from from_table_catalog_inner.
impl<S, SD, const IS_REPLICATED: bool> StateTableInner<S, SD, IS_REPLICATED>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    /// Create state table from table catalog and store.
    ///
    /// If `vnodes` is `None`, [`TableDistribution::singleton()`] will be used.
    #[cfg(any(test, feature = "test"))]
    pub async fn from_table_catalog(
        table_catalog: &Table,
        store: S,
        vnodes: Option<Arc<Bitmap>>,
    ) -> Self {
        StateTableBuilder::new(table_catalog, store, vnodes)
            .forbid_preload_all_rows()
            .build()
            .await
    }

    /// Create state table from table catalog and store with sanity check disabled.
    pub async fn from_table_catalog_inconsistent_op(
        table_catalog: &Table,
        store: S,
        vnodes: Option<Arc<Bitmap>>,
    ) -> Self {
        StateTableBuilder::new(table_catalog, store, vnodes)
            .with_op_consistency_level(StateTableOpConsistencyLevel::Inconsistent)
            .forbid_preload_all_rows()
            .build()
            .await
    }
}

impl<S: StateStore, SD: ValueRowSerde, const IS_REPLICATED: bool>
    StateTableBuilder<S, SD, IS_REPLICATED, ()>
{
    pub fn new(table_catalog: &Table, store: S, vnodes: Option<Arc<Bitmap>>) -> Self {
        let table_id = table_catalog.id;
        let table_columns: Vec<ColumnDesc> = table_catalog
            .columns
            .iter()
            .map(|col| col.column_desc.as_ref().unwrap().into())
            .collect();
        let order_types: Vec<OrderType> = table_catalog
            .pk
            .iter()
            .map(|col_order| OrderType::from_protobuf(col_order.get_order_type().unwrap()))
            .collect();
        let dist_key_indices: Vec<usize> = table_catalog
            .distribution_key
            .iter()
            .map(|dist_index| *dist_index as usize)
            .collect();

        let pk_indices = table_catalog
            .pk
            .iter()
            .map(|col_order| col_order.column_index as usize)
            .collect_vec();

        // FIXME(yuhao): only use `dist_key_in_pk` in the proto
        let dist_key_in_pk_indices = if table_catalog.get_dist_key_in_pk().is_empty() {
            get_dist_key_in_pk_indices(&dist_key_indices, &pk_indices).unwrap()
        } else {
            table_catalog
                .get_dist_key_in_pk()
                .iter()
                .map(|idx| *idx as usize)
                .collect()
        };

        let vnode_col_idx_in_pk = table_catalog.vnode_col_index.as_ref().and_then(|idx| {
            let vnode_col_idx = *idx as usize;
            pk_indices.iter().position(|&i| vnode_col_idx == i)
        });
        let value_indices = table_catalog
            .value_indices
            .iter()
            .map(|val| *val as usize)
            .collect_vec();
        let clean_watermark_indices = table_catalog.get_clean_watermark_column_indices();
        if clean_watermark_indices.len() > 1 {
            unimplemented!("multiple clean watermark columns are not supported yet")
        }
        let clean_watermark_index = clean_watermark_indices.first().map(|&i| i as usize);

        Self {
            table_id,
            table_name_for_debug: table_catalog.name.clone(),
            table_columns,
            order_types,
            pk_indices,
            dist_key_in_pk_indices,
            vnode_col_idx_in_pk,
            expected_vnode_count: table_catalog.vnode_count(),
            value_indices,
            prefix_hint_len: table_catalog.read_prefix_len_hint as usize,
            retention_seconds: table_catalog.retention_seconds,
            versioned: table_catalog.version.is_some(),
            fragment_id: table_catalog.fragment_id,
            clean_watermark_index,
            store,
            vnodes,
            op_consistency_level: None,
            output_column_ids: None,
            preload_all_rows: (),
            enable_vnode_key_stats: None,
            enable_state_table_vnode_stats_pruning: false,
            metrics: None,
            _serde: Default::default(),
        }
    }
}

impl<S: StateStore, SD: ValueRowSerde, const IS_REPLICATED: bool>
    StateTableBuilder<S, SD, IS_REPLICATED, bool>
{
    async fn build_inner(
        self,
        preload_all_rows: bool,
        should_enable_vnode_key_stats: bool,
    ) -> StateTableInner<S, SD, IS_REPLICATED> {
        let table_id = self.table_id;
        let table_columns = self.table_columns;
        let order_types = self.order_types;
        let pk_indices = self.pk_indices;
        let dist_key_in_pk_indices = self.dist_key_in_pk_indices;
        let vnode_col_idx_in_pk = self.vnode_col_idx_in_pk;
        let prefix_hint_len = self.prefix_hint_len;
        let metrics = self.metrics;

        let op_consistency_level = self
            .op_consistency_level
            .unwrap_or(StateTableOpConsistencyLevel::ConsistentOldValue);

        let output_column_ids = self.output_column_ids.unwrap_or_default();

        let data_types: Vec<DataType> = table_columns
            .iter()
            .map(|col| col.data_type.clone())
            .collect();

        // For replicated state tables (used in hash temporal join), the join key (pk_prefix) is
        // guaranteed by the optimizer to cover the distribution key, which is required by
        // `compute_prefix_vnode`. Assert this invariant at build time.
        if IS_REPLICATED && prefix_hint_len > 0 {
            assert!(
                dist_key_in_pk_indices.iter().all(|&d| d < prefix_hint_len),
                "replicated state table: distribution key indices {:?} must all be covered by \
                 prefix_hint_len {}",
                dist_key_in_pk_indices,
                prefix_hint_len,
            );
        }

        let distribution =
            TableDistribution::new(self.vnodes, dist_key_in_pk_indices, vnode_col_idx_in_pk);
        assert_eq!(
            distribution.vnode_count(),
            self.expected_vnode_count,
            "vnode count mismatch, scanning table {} under wrong distribution?",
            self.table_name_for_debug,
        );

        let pk_data_types = pk_indices
            .iter()
            .map(|i| table_columns[*i].data_type.clone())
            .collect();
        let pk_serde = OrderedRowSerde::new(pk_data_types, order_types);

        let input_value_indices = self.value_indices;

        let no_shuffle_value_indices = (0..table_columns.len()).collect_vec();

        // if value_indices is the no shuffle full columns.
        let value_indices = match input_value_indices.len() == table_columns.len()
            && input_value_indices == no_shuffle_value_indices
        {
            true => None,
            false => Some(input_value_indices.clone()),
        };

        let row_serde = Arc::new(SD::new(
            Arc::from_iter(input_value_indices.iter().copied()),
            Arc::from(table_columns.clone().into_boxed_slice()),
        ));

        let state_table_op_consistency_level = op_consistency_level;
        let op_consistency_level = match state_table_op_consistency_level {
            StateTableOpConsistencyLevel::Inconsistent => OpConsistencyLevel::Inconsistent,
            StateTableOpConsistencyLevel::ConsistentOldValue => {
                consistent_old_value_op(row_serde.clone(), false)
            }
            StateTableOpConsistencyLevel::LogStoreEnabled => {
                consistent_old_value_op(row_serde.clone(), true)
            }
        };

        let table_option = TableOption::new(self.retention_seconds);
        let new_local_options = if IS_REPLICATED {
            NewLocalOptions::new_replicated(
                table_id,
                self.fragment_id,
                op_consistency_level,
                table_option,
                distribution.vnodes().clone(),
            )
        } else {
            NewLocalOptions::new(
                table_id,
                self.fragment_id,
                op_consistency_level,
                table_option,
                distribution.vnodes().clone(),
                true,
            )
        };
        let local_state_store = self.store.new_local(new_local_options).await;

        // If state table has versioning, that means it supports
        // Schema change. In that case, the row encoding should be column aware as well.
        // Otherwise both will be false.
        // NOTE(kwannoel): Replicated table will follow upstream table's versioning. I'm not sure
        // If ALTER TABLE will propagate to this replicated table as well. Ideally it won't
        assert_eq!(self.versioned, row_serde.kind().is_column_aware());

        // Get info for replicated state table.
        let output_column_ids_to_input_idx = output_column_ids
            .iter()
            .enumerate()
            .map(|(pos, id)| (*id, pos))
            .collect::<HashMap<_, _>>();

        let columns = table_columns;

        // Compute i2o mapping
        // Note that this can be a partial mapping, since we use the i2o mapping to get
        // any 1 of the output columns, and use that to fill the input column.
        let mut i2o_mapping = vec![None; columns.len()];
        for (i, column) in columns.iter().enumerate() {
            if let Some(pos) = output_column_ids_to_input_idx.get(&column.column_id) {
                i2o_mapping[i] = Some(*pos);
            }
        }
        // We can prune any duplicate column indices
        let i2o_mapping = ColIndexMapping::new(i2o_mapping, output_column_ids.len());

        // Compute output indices
        let (_, output_indices) = find_columns_by_ids(&columns[..], &output_column_ids);

        // For replicated state tables, pk columns must be explicitly provided by the write caller
        // rather than being filled with None internally via i2o_mapping.
        if IS_REPLICATED {
            assert!(
                pk_indices
                    .iter()
                    .all(|&pk_idx| output_indices.contains(&pk_idx)),
                "all pk columns must be included in output_column_ids for replicated state table"
            );
        }

        let clean_watermark_index = self.clean_watermark_index;
        let watermark_serde = clean_watermark_index.map(|idx| {
            let pk_idx = pk_indices.iter().position(|&i| i == idx);
            let (watermark_serde, watermark_serde_type) = match pk_idx {
                Some(0) => (pk_serde.index(0).into_owned(), WatermarkSerdeType::PkPrefix),
                Some(pk_idx) => (
                    pk_serde.index(pk_idx).into_owned(),
                    WatermarkSerdeType::NonPkPrefix,
                ),
                None => (
                    OrderedRowSerde::new(
                        vec![data_types[idx].clone()],
                        vec![OrderType::ascending()],
                    ),
                    WatermarkSerdeType::Value,
                ),
            };
            (watermark_serde, watermark_serde_type)
        });

        // Restore persisted table watermark.
        let committed_watermark = if let Some((deser, _)) = watermark_serde.as_ref() {
            distribution
                .vnodes()
                .iter_vnodes()
                .filter_map(|vnode| {
                    let bytes = local_state_store.get_table_watermark(vnode)?;
                    let datum = deser.deserialize(&bytes).ok().and_then(|row| {
                        assert!(row.len() == 1);
                        row[0].clone()
                    });
                    if datum.is_none() {
                        tracing::error!(
                            ?vnode,
                            watermark = ?bytes,
                            "Failed to deserialize persisted watermark from state store.",
                        );
                    }
                    datum
                })
                .max_by(|a, b| cmp_datum(Some(a), Some(b), OrderType::ascending()))
        } else {
            None
        };

        StateTableInner {
            table_id,
            row_store: StateTableRowStore {
                all_rows: preload_all_rows.then(HashMap::new),
                state_store: local_state_store,
                row_serde,
                pk_serde: pk_serde.clone(),
                table_id,
                // Need to maintain vnode min/max key stats when vnode key pruning is enabled
                vnode_stats: should_enable_vnode_key_stats.then(HashMap::new),
                enable_state_table_vnode_stats_pruning: self.enable_state_table_vnode_stats_pruning,
                metrics,
            },
            store: self.store,
            epoch: None,
            pk_serde,
            pk_indices,
            distribution,
            prefix_hint_len,
            value_indices,
            pending_watermark: None,
            committed_watermark,
            watermark_serde,
            data_types,
            output_indices,
            i2o_mapping,
            op_consistency_level: state_table_op_consistency_level,
            clean_watermark_index,
            on_post_commit: false,
        }
    }
}

impl<S: StateStore, SD: ValueRowSerde, const IS_REPLICATED: bool>
    StateTableBuilder<S, SD, IS_REPLICATED, ()>
{
    pub fn new_from_storage_table_desc(
        table_desc: &StorageTableDesc,
        store: S,
        vnodes: Option<Arc<Bitmap>>,
        fragment_id: FragmentId,
    ) -> Self {
        let table_id = table_desc.table_id;
        let table_columns: Vec<ColumnDesc> =
            table_desc.columns.iter().map(ColumnDesc::from).collect();
        let order_types: Vec<OrderType> = table_desc
            .pk
            .iter()
            .map(|col_order| OrderType::from_protobuf(col_order.get_order_type().unwrap()))
            .collect();
        let pk_indices = table_desc
            .pk
            .iter()
            .map(|col_order| col_order.column_index as usize)
            .collect_vec();
        let dist_key_in_pk_indices = table_desc
            .dist_key_in_pk_indices
            .iter()
            .map(|&idx| idx as usize)
            .collect();
        // StorageTableDesc provides vnode_col_idx_in_pk directly (already pk-relative),
        // unlike Table which has an absolute column index that needs conversion.
        let vnode_col_idx_in_pk = table_desc.vnode_col_idx_in_pk.map(|k| k as usize);
        let raw_value_indices = table_desc
            .value_indices
            .iter()
            .map(|val| *val as usize)
            .collect_vec();

        Self {
            table_id,
            table_name_for_debug: table_id.to_string(),
            table_columns,
            order_types,
            pk_indices,
            dist_key_in_pk_indices,
            vnode_col_idx_in_pk,
            expected_vnode_count: table_desc.vnode_count(),
            value_indices: raw_value_indices,
            prefix_hint_len: table_desc.read_prefix_len_hint as usize,
            retention_seconds: table_desc.retention_seconds,
            versioned: table_desc.versioned,
            fragment_id,
            clean_watermark_index: None,
            store,
            vnodes,
            op_consistency_level: None,
            output_column_ids: None,
            preload_all_rows: (),
            enable_vnode_key_stats: None,
            enable_state_table_vnode_stats_pruning: false,
            metrics: None,
            _serde: Default::default(),
        }
    }
}

impl<S, SD, const IS_REPLICATED: bool> StateTableInner<S, SD, IS_REPLICATED>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    pub fn get_data_types(&self) -> &[DataType] {
        &self.data_types
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Get the vnode value with given (prefix of) primary key
    fn compute_prefix_vnode(&self, pk_prefix: &impl Row) -> VirtualNode {
        self.distribution
            .try_compute_vnode_by_pk_prefix(pk_prefix)
            .expect("For streaming, the given prefix must be enough to calculate the vnode")
    }

    /// Get the vnode value of the given primary key
    pub fn compute_vnode_by_pk(&self, pk: impl Row) -> VirtualNode {
        self.distribution.compute_vnode_by_pk(pk)
    }

    /// NOTE(kwannoel): This is used by backfill.
    /// We want to check pk indices of upstream table.
    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }

    /// Get the indices of the primary key columns in the output columns.
    ///
    /// Returns `None` if any of the primary key columns is not in the output columns.
    pub fn pk_in_output_indices(&self) -> Option<Vec<usize>> {
        assert!(IS_REPLICATED);
        self.pk_indices
            .iter()
            .map(|&i| self.output_indices.iter().position(|&j| i == j))
            .collect()
    }

    pub fn pk_serde(&self) -> &OrderedRowSerde {
        &self.pk_serde
    }

    pub fn vnodes(&self) -> &Arc<Bitmap> {
        self.distribution.vnodes()
    }

    pub fn flushed_snapshot_reader(&self) -> FlushedStateTableReader<S, SD> {
        StateTableFlushedSnapshotReader {
            reader: Arc::new(self.row_store.state_store.new_flushed_snapshot_reader()),
            pk_serde: self.pk_serde.clone(),
            vnodes: self.distribution.vnodes().clone(),
            row_serde: self.row_store.row_serde.clone(),
            metrics: self.row_store.metrics.clone(),
        }
    }

    pub fn value_indices(&self) -> &Option<Vec<usize>> {
        &self.value_indices
    }

    pub fn is_consistent_op(&self) -> bool {
        matches!(
            self.op_consistency_level,
            StateTableOpConsistencyLevel::ConsistentOldValue
                | StateTableOpConsistencyLevel::LogStoreEnabled
        )
    }

    pub fn metrics(&self) -> Option<&StateTableMetrics> {
        self.row_store.metrics.as_ref()
    }
}

impl<S, SD> StateTableInner<S, SD, true>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    /// Create replicated state table from table catalog with output indices
    pub async fn new_replicated(
        table_catalog: &Table,
        store: S,
        vnodes: Option<Arc<Bitmap>>,
        output_column_ids: Vec<ColumnId>,
    ) -> Self {
        // TODO: can it be ConsistentOldValue?
        // TODO: may enable preload_all_rows
        StateTableBuilder::new(table_catalog, store, vnodes)
            .with_op_consistency_level(StateTableOpConsistencyLevel::Inconsistent)
            .with_output_column_ids(output_column_ids)
            .forbid_preload_all_rows()
            .build()
            .await
    }
}

// point get
impl<S, SD, const IS_REPLICATED: bool> StateTableInner<S, SD, IS_REPLICATED>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    /// Get a single row from state table.
    pub async fn get_row(&self, pk: impl Row) -> StreamExecutorResult<Option<OwnedRow>> {
        let (serialized_pk, prefix_hint) = self.serialize_pk_and_get_prefix_hint(&pk);
        let row = self.row_store.get(serialized_pk, prefix_hint).await?;
        match row {
            Some(row) => {
                if IS_REPLICATED {
                    // If the table is replicated, we need to deserialize the row with the output
                    // indices.
                    let row = row.project(&self.output_indices);
                    Ok(Some(row.into_owned_row()))
                } else {
                    Ok(Some(row))
                }
            }
            None => Ok(None),
        }
    }

    /// Get a raw encoded row from state table.
    pub async fn exists(&self, pk: impl Row) -> StreamExecutorResult<bool> {
        let (serialized_pk, prefix_hint) = self.serialize_pk_and_get_prefix_hint(&pk);
        self.row_store.exists(serialized_pk, prefix_hint).await
    }

    fn serialize_pk(&self, pk: &impl Row) -> TableKey<Bytes> {
        assert!(pk.len() <= self.pk_indices.len());
        serialize_pk_with_vnode(pk, &self.pk_serde, self.compute_vnode_by_pk(pk))
    }

    fn serialize_pk_and_get_prefix_hint(&self, pk: &impl Row) -> (TableKey<Bytes>, Option<Bytes>) {
        let serialized_pk = self.serialize_pk(&pk);
        let prefix_hint = if should_calculate_prefix_hint(self.prefix_hint_len, pk.len(), false) {
            Some(serialized_pk.slice(VirtualNode::SIZE..))
        } else {
            #[cfg(debug_assertions)]
            if self.prefix_hint_len != 0 {
                warn!(
                    "prefix_hint_len is not equal to pk.len(), may not be able to utilize bloom filter"
                );
            }
            None
        };
        (serialized_pk, prefix_hint)
    }
}

impl<LS: LocalStateStore, SD: ValueRowSerde> StateTableRowStore<LS, SD> {
    async fn get(
        &self,
        key_bytes: TableKey<Bytes>,
        prefix_hint: Option<Bytes>,
    ) -> StreamExecutorResult<Option<OwnedRow>> {
        if let Some(m) = &self.metrics {
            m.get_count.inc();
        }
        if let Some(rows) = &self.all_rows {
            let (vnode, key) = key_bytes.split_vnode_bytes();
            return Ok(rows.get(&vnode).expect("covered vnode").get(&key).cloned());
        }

        // Try to prune using vnode statistics
        let should_prune = if let Some(stats) = &self.vnode_stats
            && let (vnode, key) = key_bytes.split_vnode_bytes()
            && let Some(vnode_stat) = stats.get(&vnode)
            && vnode_stat.can_prune(&key)
        {
            if let Some(m) = &self.metrics {
                m.get_vnode_pruned_count.inc();
            }
            true
        } else {
            false
        };

        if should_prune && self.enable_state_table_vnode_stats_pruning {
            return Ok(None);
        }

        let read_options = ReadOptions {
            prefix_hint,
            cache_policy: CachePolicy::Fill(Hint::Normal),
            ..Default::default()
        };

        let result = self
            .state_store
            .on_key_value(key_bytes, read_options, move |_, value| {
                let row = self.row_serde.deserialize(value)?;
                Ok(OwnedRow::new(row))
            })
            .await
            .map_err(Into::<StreamExecutorError>::into)?;

        // In dry-run mode, verify that pruning would have been correct
        if should_prune && result.is_some() {
            tracing::warn!(
                table_id = %self.table_id,
                "vnode stats pruning dry run fails for get. This will not affect correctness."
            );
        }

        Ok(result)
    }

    async fn exists(
        &self,
        key_bytes: TableKey<Bytes>,
        prefix_hint: Option<Bytes>,
    ) -> StreamExecutorResult<bool> {
        if let Some(m) = &self.metrics {
            m.get_count.inc();
        }
        if let Some(rows) = &self.all_rows {
            let (vnode, key) = key_bytes.split_vnode_bytes();
            return Ok(rows.get(&vnode).expect("covered vnode").contains_key(&key));
        }

        // Try to prune using vnode statistics
        let should_prune = if let Some(stats) = &self.vnode_stats
            && let (vnode, key) = key_bytes.split_vnode_bytes()
            && let Some(vnode_stat) = stats.get(&vnode)
            && vnode_stat.can_prune(&key)
        {
            if let Some(m) = &self.metrics {
                m.get_vnode_pruned_count.inc();
            }
            true
        } else {
            false
        };

        if should_prune && self.enable_state_table_vnode_stats_pruning {
            return Ok(false);
        }

        let read_options = ReadOptions {
            prefix_hint,
            cache_policy: CachePolicy::Fill(Hint::Normal),
            ..Default::default()
        };
        let result = self
            .state_store
            .on_key_value(key_bytes, read_options, move |_, _| Ok(()))
            .await?;
        let exists = result.is_some();

        // In dry-run mode, verify that pruning would have been correct
        if should_prune && exists {
            tracing::warn!(
                table_id = %self.table_id,
                "vnode stats pruning dry run fails for exists. This will not affect correctness."
            );
        }

        Ok(exists)
    }
}

/// A callback struct returned from [`StateTableInner::commit`].
///
/// Introduced to support single barrier configuration change proposed in <https://github.com/risingwavelabs/risingwave/issues/18312>.
/// In brief, to correctly handle the configuration change, when each stateful executor receives an upstream barrier, it should handle
/// the barrier in the order of `state_table.commit()` -> `yield barrier` -> `update_vnode_bitmap`.
///
/// The `StateTablePostCommit` captures the mutable reference of `state_table` when calling `state_table.commit()`, and after the executor
/// runs `yield barrier`, it should call `StateTablePostCommit::post_yield_barrier` to apply the vnode bitmap update if there is any.
/// The `StateTablePostCommit` is marked with `must_use`. The method name `post_yield_barrier` indicates that it should be called after
/// we have yielded the barrier. In `StateTable`, we add a flag `on_post_commit`, to indicate that whether the `StateTablePostCommit` is handled
/// properly. On `state_table.commit()`, we will mark the `on_post_commit` as true, and in `StateTablePostCommit::post_yield_barrier`, we will
/// remark the flag as false, and on `state_table.commit()`, we will assert that the `on_post_commit` must be false. Note that, the `post_yield_barrier`
/// should be called for all barriers rather than only for the barrier with update vnode bitmap. In this way, though we don't have scale test for all
/// streaming executor, we can ensure that all executor covered by normal e2e test have properly handled the `StateTablePostCommit`.
#[must_use]
pub struct StateTablePostCommit<'a, S, SD = BasicSerde, const IS_REPLICATED: bool = false>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    inner: &'a mut StateTableInner<S, SD, IS_REPLICATED>,
}

impl<'a, S, SD, const IS_REPLICATED: bool> StateTablePostCommit<'a, S, SD, IS_REPLICATED>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    /// Returns `Some((new_vnodes, old_vnodes, state_table), keyed_cache_may_stale)` if the vnode bitmap is updated.
    ///
    /// Note the `keyed_cache_may_stale` only applies to keyed cache. If the executor's cache is not keyed, but will
    /// be consumed with all vnodes it owns, the executor may need to ALWAYS clear the cache regardless of this flag.
    pub async fn post_yield_barrier(
        mut self,
        new_vnodes: Option<Arc<Bitmap>>,
    ) -> StreamExecutorResult<
        Option<(
            (
                Arc<Bitmap>,
                Arc<Bitmap>,
                &'a mut StateTableInner<S, SD, IS_REPLICATED>,
            ),
            bool,
        )>,
    > {
        self.inner.on_post_commit = false;
        Ok(if let Some(new_vnodes) = new_vnodes {
            let (old_vnodes, keyed_cache_may_stale) =
                self.update_vnode_bitmap(new_vnodes.clone()).await?;
            Some(((new_vnodes, old_vnodes, self.inner), keyed_cache_may_stale))
        } else {
            None
        })
    }

    pub fn inner(&self) -> &StateTableInner<S, SD, IS_REPLICATED> {
        &*self.inner
    }

    /// Update the vnode bitmap of the state table, returns the previous vnode bitmap.
    async fn update_vnode_bitmap(
        &mut self,
        new_vnodes: Arc<Bitmap>,
    ) -> StreamExecutorResult<(Arc<Bitmap>, bool)> {
        let prev_vnodes = self
            .inner
            .row_store
            .update_vnode_bitmap(new_vnodes.clone())
            .await?;
        assert_eq!(
            &prev_vnodes,
            self.inner.vnodes(),
            "state table and state store vnode bitmap mismatches"
        );

        if self.inner.distribution.is_singleton() {
            assert_eq!(
                &new_vnodes,
                self.inner.vnodes(),
                "should not update vnode bitmap for singleton table"
            );
        }
        assert_eq!(self.inner.vnodes().len(), new_vnodes.len());

        let keyed_cache_may_stale = keyed_cache_may_stale(self.inner.vnodes(), &new_vnodes);

        if keyed_cache_may_stale {
            self.inner.pending_watermark = None;
        }

        Ok((
            self.inner.distribution.update_vnode_bitmap(new_vnodes),
            keyed_cache_may_stale,
        ))
    }
}

// write
impl<LS: LocalStateStore, SD: ValueRowSerde> StateTableRowStore<LS, SD> {
    fn handle_mem_table_error(&self, e: StorageError) {
        let e = match e.into_inner() {
            ErrorKind::MemTable(e) => e,
            _ => unreachable!("should only get memtable error"),
        };
        match *e {
            MemTableError::InconsistentOperation { key, prev, new, .. } => {
                let (vnode, key) = deserialize_pk_with_vnode(&key, &self.pk_serde).unwrap();
                panic!(
                    "mem-table operation inconsistent! table_id: {}, vnode: {}, key: {:?}, prev: {}, new: {}",
                    self.table_id,
                    vnode,
                    key,
                    prev.debug_fmt(&*self.row_serde),
                    new.debug_fmt(&*self.row_serde),
                )
            }
        }
    }

    fn insert(&mut self, key: TableKey<Bytes>, value: impl Row) {
        insane_mode_discard_point!();
        let value_bytes = self.row_serde.serialize(&value).into();

        let (vnode, key_without_vnode) = key.split_vnode_bytes();

        // Update vnode statistics (skip if all_rows is present)
        if self.all_rows.is_none()
            && let Some(stats) = &mut self.vnode_stats
            && let Some(vnode_stat) = stats.get_mut(&vnode)
        {
            vnode_stat.update_with_key(&key_without_vnode);
        }

        if let Some(rows) = &mut self.all_rows {
            rows.get_mut(&vnode)
                .expect("covered vnode")
                .insert(key_without_vnode, value.into_owned_row());
        }
        self.state_store
            .insert(key, value_bytes, None)
            .unwrap_or_else(|e| self.handle_mem_table_error(e));
    }

    fn delete(&mut self, key: TableKey<Bytes>, value: impl Row) {
        insane_mode_discard_point!();
        let value_bytes = self.row_serde.serialize(value).into();

        let (vnode, key_without_vnode) = key.split_vnode_bytes();

        if self.all_rows.is_none()
            && let Some(stats) = &mut self.vnode_stats
            && let Some(vnode_stat) = stats.get_mut(&vnode)
        {
            vnode_stat.update_with_key(&key_without_vnode);
        }

        if let Some(rows) = &mut self.all_rows {
            rows.get_mut(&vnode)
                .expect("covered vnode")
                .remove(&key_without_vnode);
        }
        self.state_store
            .delete(key, value_bytes)
            .unwrap_or_else(|e| self.handle_mem_table_error(e));
    }

    fn update(&mut self, key_bytes: TableKey<Bytes>, old_value: impl Row, new_value: impl Row) {
        insane_mode_discard_point!();
        let new_value_bytes = self.row_serde.serialize(&new_value).into();
        let old_value_bytes = self.row_serde.serialize(old_value).into();

        let (vnode, key_without_vnode) = key_bytes.split_vnode_bytes();

        // Update does not change the key, so statistics remain valid (skip if all_rows is present)
        // But we update to ensure consistency
        if self.all_rows.is_none()
            && let Some(stats) = &mut self.vnode_stats
            && let Some(vnode_stat) = stats.get_mut(&vnode)
        {
            vnode_stat.update_with_key(&key_without_vnode);
        }

        if let Some(rows) = &mut self.all_rows {
            rows.get_mut(&vnode)
                .expect("covered vnode")
                .insert(key_without_vnode, new_value.into_owned_row());
        }
        self.state_store
            .insert(key_bytes, new_value_bytes, Some(old_value_bytes))
            .unwrap_or_else(|e| self.handle_mem_table_error(e));
    }
}

impl<S, SD, const IS_REPLICATED: bool> StateTableInner<S, SD, IS_REPLICATED>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    /// Insert a row into state table. Must provide a full row corresponding to the column desc of
    /// the table.
    pub fn insert(&mut self, value: impl Row) {
        let pk_indices = &self.pk_indices;
        let pk = (&value).project(pk_indices);

        let key_bytes = self.serialize_pk(&pk);
        dispatch_value_indices!(&self.value_indices, [value], {
            self.row_store.insert(key_bytes, value)
        })
    }

    /// Delete a row from state table. Must provide a full row of old value corresponding to the
    /// column desc of the table.
    pub fn delete(&mut self, old_value: impl Row) {
        let pk_indices = &self.pk_indices;
        let pk = (&old_value).project(pk_indices);

        let key_bytes = self.serialize_pk(&pk);
        dispatch_value_indices!(&self.value_indices, [old_value], {
            self.row_store.delete(key_bytes, old_value)
        })
    }

    /// Update a row. The old and new value should have the same pk.
    pub fn update(&mut self, old_value: impl Row, new_value: impl Row) {
        let old_pk = (&old_value).project(self.pk_indices());
        let new_pk = (&new_value).project(self.pk_indices());
        debug_assert!(
            Row::eq(&old_pk, new_pk),
            "pk should not change: {old_pk:?} vs {new_pk:?}. {}",
            self.table_id
        );

        let key_bytes = self.serialize_pk(&new_pk);
        dispatch_value_indices!(&self.value_indices, [old_value, new_value], {
            self.row_store.update(key_bytes, old_value, new_value)
        })
    }

    /// Write a record into state table. Must have the same schema with the table.
    pub fn write_record(&mut self, record: Record<impl Row>) {
        match record {
            Record::Insert { new_row } => self.insert(new_row),
            Record::Delete { old_row } => self.delete(old_row),
            Record::Update { old_row, new_row } => self.update(old_row, new_row),
        }
    }

    fn fill_non_output_indices(&self, chunk: StreamChunk) -> StreamChunk {
        fill_non_output_indices(&self.i2o_mapping, &self.data_types, chunk)
    }

    /// Write batch with a `StreamChunk` which should have the same schema with the table.
    // allow(izip, which use zip instead of zip_eq)
    #[allow(clippy::disallowed_methods)]
    pub fn write_chunk(&mut self, chunk: StreamChunk) {
        let chunk = if IS_REPLICATED {
            self.fill_non_output_indices(chunk)
        } else {
            chunk
        };

        let vnodes = self
            .distribution
            .compute_chunk_vnode(&chunk, &self.pk_indices);

        for (idx, optional_row) in chunk.rows_with_holes().enumerate() {
            let Some((op, row)) = optional_row else {
                continue;
            };
            let pk = row.project(&self.pk_indices);
            let vnode = vnodes[idx];
            let key_bytes = serialize_pk_with_vnode(pk, &self.pk_serde, vnode);
            match op {
                Op::Insert | Op::UpdateInsert => {
                    dispatch_value_indices!(&self.value_indices, [row], {
                        self.row_store.insert(key_bytes, row);
                    });
                }
                Op::Delete | Op::UpdateDelete => {
                    dispatch_value_indices!(&self.value_indices, [row], {
                        self.row_store.delete(key_bytes, row);
                    });
                }
            }
        }
    }

    /// Update watermark for state cleaning.
    ///
    /// # Arguments
    ///
    /// * `watermark` - Latest watermark received.
    pub fn update_watermark(&mut self, watermark: ScalarImpl) {
        trace!(table_id = %self.table_id, watermark = ?watermark, "update watermark");
        self.pending_watermark = Some(watermark);
    }

    /// Get the committed watermark of the state table. Watermarks should be fed into the state
    /// table through `update_watermark` method.
    pub fn get_committed_watermark(&self) -> Option<&ScalarImpl> {
        self.committed_watermark.as_ref()
    }

    pub async fn commit(
        &mut self,
        new_epoch: EpochPair,
    ) -> StreamExecutorResult<StateTablePostCommit<'_, S, SD, IS_REPLICATED>> {
        self.commit_inner(new_epoch, None).await
    }

    #[cfg(test)]
    pub async fn commit_for_test(&mut self, new_epoch: EpochPair) -> StreamExecutorResult<()> {
        self.commit_assert_no_update_vnode_bitmap(new_epoch).await
    }

    pub async fn commit_assert_no_update_vnode_bitmap(
        &mut self,
        new_epoch: EpochPair,
    ) -> StreamExecutorResult<()> {
        let post_commit = self.commit_inner(new_epoch, None).await?;
        post_commit.post_yield_barrier(None).await?;
        Ok(())
    }

    pub async fn commit_may_switch_consistent_op(
        &mut self,
        new_epoch: EpochPair,
        op_consistency_level: StateTableOpConsistencyLevel,
    ) -> StreamExecutorResult<StateTablePostCommit<'_, S, SD, IS_REPLICATED>> {
        if self.op_consistency_level != op_consistency_level {
            // avoid flooding e2e-test log
            if !cfg!(debug_assertions) {
                info!(
                    ?new_epoch,
                    prev_op_consistency_level = ?self.op_consistency_level,
                    ?op_consistency_level,
                    table_id = %self.table_id,
                    "switch to new op consistency level"
                );
            }
            self.commit_inner(new_epoch, Some(op_consistency_level))
                .await
        } else {
            self.commit_inner(new_epoch, None).await
        }
    }

    async fn commit_inner(
        &mut self,
        new_epoch: EpochPair,
        switch_consistent_op: Option<StateTableOpConsistencyLevel>,
    ) -> StreamExecutorResult<StateTablePostCommit<'_, S, SD, IS_REPLICATED>> {
        assert!(!self.on_post_commit);
        assert_eq!(
            self.epoch.expect("should only be called after init").curr,
            new_epoch.prev
        );
        if let Some(new_consistency_level) = switch_consistent_op {
            assert_ne!(self.op_consistency_level, new_consistency_level);
            self.op_consistency_level = new_consistency_level;
        }
        trace!(
            table_id = %self.table_id,
            epoch = ?self.epoch,
            "commit state table"
        );

        let table_watermarks = self.commit_pending_watermark();
        self.row_store
            .seal_current_epoch(new_epoch.curr, table_watermarks, switch_consistent_op)
            .instrument_await(await_tree::span!(
                "state_table_commit table_id={} epoch={}",
                self.table_id,
                new_epoch.curr
            ))
            .await?;
        self.epoch = Some(new_epoch);

        self.on_post_commit = true;
        Ok(StateTablePostCommit { inner: self })
    }

    /// Commit pending watermark and return vnode bitmap-watermark pairs to seal.
    fn commit_pending_watermark(
        &mut self,
    ) -> Option<(WatermarkDirection, Vec<VnodeWatermark>, WatermarkSerdeType)> {
        let watermark = self.pending_watermark.take()?;
        trace!(table_id = %self.table_id, watermark = ?watermark, "state cleaning");

        assert!(
            !self.pk_indices().is_empty(),
            "see pending watermark on empty pk"
        );
        let (watermark_serializer, watermark_type) = self
            .watermark_serde
            .as_ref()
            .expect("watermark serde should be initialized to commit watermark");
        let watermark_suffix =
            serialize_row(row::once(Some(watermark.clone())), watermark_serializer);
        let vnode_watermark = VnodeWatermark::new(
            self.vnodes().clone(),
            Bytes::copy_from_slice(watermark_suffix.as_ref()),
        );
        trace!(table_id = %self.table_id, ?vnode_watermark, "table watermark");

        let order_type = watermark_serializer.get_order_types().get(0).unwrap();
        let direction = if order_type.is_ascending() {
            WatermarkDirection::Ascending
        } else {
            WatermarkDirection::Descending
        };

        self.committed_watermark = Some(watermark);
        Some((direction, vec![vnode_watermark], *watermark_type))
    }

    pub async fn try_flush(&mut self) -> StreamExecutorResult<()> {
        self.row_store.try_flush().await?;
        Ok(())
    }
}

// Manually expand trait alias for better IDE experience.
pub trait RowStream<'a>: Stream<Item = StreamExecutorResult<OwnedRow>> + 'a {}
impl<'a, S: Stream<Item = StreamExecutorResult<OwnedRow>> + 'a> RowStream<'a> for S {}

pub trait KeyedRowStream<'a>: Stream<Item = StreamExecutorResult<KeyedRow<Bytes>>> + 'a {}
impl<'a, S: Stream<Item = StreamExecutorResult<KeyedRow<Bytes>>> + 'a> KeyedRowStream<'a> for S {}

pub trait PkRowStream<'a, K>: Stream<Item = StreamExecutorResult<(K, OwnedRow)>> + 'a {}
impl<'a, K, S: Stream<Item = StreamExecutorResult<(K, OwnedRow)>> + 'a> PkRowStream<'a, K> for S {}

pub type BoxedRowStream<'a> = BoxStream<'a, StreamExecutorResult<OwnedRow>>;

pub trait FromVnodeBytes {
    fn from_vnode_bytes(vnode: VirtualNode, bytes: &Bytes) -> Self;
}

impl FromVnodeBytes for Bytes {
    fn from_vnode_bytes(vnode: VirtualNode, bytes: &Bytes) -> Self {
        prefix_slice_with_vnode(vnode, bytes)
    }
}

impl FromVnodeBytes for () {
    fn from_vnode_bytes(_vnode: VirtualNode, _bytes: &Bytes) -> Self {}
}

impl<R, SD> StateTableFlushedSnapshotReader<R, SD>
where
    R: StateStoreRead,
    SD: ValueRowSerde,
{
    pub fn vnodes(&self) -> &Arc<Bitmap> {
        &self.vnodes
    }

    /// Scans flushed local state without reading uncommitted mem-table data.
    pub async fn iter_with_vnode(
        &self,
        vnode: VirtualNode,
        pk_range: &(Bound<impl Row>, Bound<impl Row>),
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl RowStream<'static>> {
        if let Some(m) = &self.metrics {
            m.iter_count.inc();
        }

        let memcomparable_range = prefix_range_to_memcomparable(&self.pk_serde, pk_range);
        let iter = self
            .reader
            .iter(
                prefixed_range_with_vnode(memcomparable_range, vnode),
                ReadOptions {
                    prefix_hint: None,
                    prefetch_options,
                    cache_policy: CachePolicy::Fill(Hint::Normal),
                },
            )
            .await?;
        let row_serde = self.row_serde.clone();
        Ok(iter
            .into_stream(move |(_key, value)| Ok(OwnedRow::new(row_serde.deserialize(value)?)))
            .map_err(Into::into))
    }
}

// Iterator functions
impl<S, SD, const IS_REPLICATED: bool> StateTableInner<S, SD, IS_REPLICATED>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    /// This function scans rows from the relational table with specific `pk_range` under the same
    /// `vnode`.
    pub async fn iter_with_vnode(
        &self,

        // Optional vnode that returns an iterator only over the given range under that vnode.
        // For now, we require this parameter, and will panic. In the future, when `None`, we can
        // iterate over each vnode that the `StateTableInner` owns.
        vnode: VirtualNode,
        pk_range: &(Bound<impl Row>, Bound<impl Row>),
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl RowStream<'_>> {
        Ok(self
            .iter_kv_with_pk_range::<()>(pk_range, vnode, prefetch_options)
            .await?
            .map_ok(|(_, row)| {
                if IS_REPLICATED {
                    row.project(&self.output_indices).into_owned_row()
                } else {
                    row
                }
            }))
    }

    pub async fn iter_keyed_row_with_vnode(
        &self,
        vnode: VirtualNode,
        pk_range: &(Bound<impl Row>, Bound<impl Row>),
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl KeyedRowStream<'_>> {
        Ok(self
            .iter_kv_with_pk_range(pk_range, vnode, prefetch_options)
            .await?
            .map_ok(|(key, row)| KeyedRow::new(TableKey(key), row)))
    }
}

impl<LS: LocalStateStore, SD: ValueRowSerde> StateTableRowStore<LS, SD> {
    // The lowest-level API.
    /// Middle-level APIs:
    /// - [`StateTableInner::iter_with_prefix_inner`]
    /// - [`StateTableInner::iter_kv_with_pk_range`]
    async fn iter_kv<K: CopyFromSlice + FromVnodeBytes>(
        &self,
        vnode: VirtualNode,
        (start, end): (Bound<Bytes>, Bound<Bytes>),
        prefix_hint: Option<Bytes>,
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl PkRowStream<'_, K>> {
        if let Some(m) = &self.metrics {
            m.iter_count.inc();
        }
        // Check if we can prune the entire range using vnode statistics
        let (pruned_start, pruned_end, should_prune_entirely) = if let Some(stats) =
            &self.vnode_stats
            && let Some(vnode_stat) = stats.get(&vnode)
        {
            match vnode_stat.pruned_key_range(&start, &end) {
                Some((new_start, new_end)) => {
                    if self.enable_state_table_vnode_stats_pruning {
                        (new_start, new_end, false)
                    } else {
                        // In dry-run mode, we don't apply pruning but verify correctness
                        (start, end, false)
                    }
                }
                None => {
                    if let Some(m) = &self.metrics {
                        m.iter_vnode_pruned_count.inc();
                    }
                    // Mark that we should prune entirely, but handle dry-run below
                    (start.clone(), end.clone(), true)
                }
            }
        } else {
            (start, end, false)
        };

        if should_prune_entirely && self.enable_state_table_vnode_stats_pruning {
            return Ok(futures::future::Either::Left(futures::stream::empty()));
        }

        let table_id = self.table_id;
        let inspect_fn = move |result: &StreamExecutorResult<(K, OwnedRow)>| {
            // Only log when in dry-run mode and we would have pruned but got results
            if should_prune_entirely && result.is_ok() {
                tracing::warn!(
                    table_id = %table_id,
                    "vnode stats pruning dry run fails for iter. This will not affect correctness."
                );
            }
        };

        if let Some(rows) = &self.all_rows {
            return Ok(futures::future::Either::Right(
                futures::future::Either::Left(
                    futures::stream::iter(
                        rows.get(&vnode)
                            .expect("covered vnode")
                            .range((pruned_start, pruned_end))
                            .map(move |(key, value)| {
                                Ok((K::from_vnode_bytes(vnode, key), value.clone()))
                            }),
                    )
                    .inspect(inspect_fn),
                ),
            ));
        }
        let read_options = ReadOptions {
            prefix_hint,
            prefetch_options,
            cache_policy: CachePolicy::Fill(Hint::Normal),
        };

        Ok(futures::future::Either::Right(
            futures::future::Either::Right(
                deserialize_keyed_row_stream(
                    self.state_store
                        .iter(
                            prefixed_range_with_vnode((pruned_start, pruned_end), vnode),
                            read_options,
                        )
                        .await?,
                    &*self.row_serde,
                )
                .inspect(inspect_fn),
            ),
        ))
    }

    async fn rev_iter_kv<K: CopyFromSlice + FromVnodeBytes>(
        &self,
        vnode: VirtualNode,
        (start, end): (Bound<Bytes>, Bound<Bytes>),
        prefix_hint: Option<Bytes>,
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl PkRowStream<'_, K>> {
        if let Some(m) = &self.metrics {
            m.iter_count.inc();
        }
        // Check if we can prune the entire range using vnode statistics
        let (pruned_start, pruned_end, should_prune_entirely) = if let Some(stats) =
            &self.vnode_stats
            && let Some(vnode_stat) = stats.get(&vnode)
        {
            match vnode_stat.pruned_key_range(&start, &end) {
                Some((new_start, new_end)) => {
                    if self.enable_state_table_vnode_stats_pruning {
                        (new_start, new_end, false)
                    } else {
                        // In dry-run mode, we don't apply pruning but verify correctness
                        (start, end, false)
                    }
                }
                None => {
                    if let Some(m) = &self.metrics {
                        m.iter_vnode_pruned_count.inc();
                    }
                    // Mark that we should prune entirely, but handle dry-run below
                    (start, end, true)
                }
            }
        } else {
            (start, end, false)
        };

        if should_prune_entirely && self.enable_state_table_vnode_stats_pruning {
            return Ok(futures::future::Either::Left(futures::stream::empty()));
        }

        let table_id = self.table_id;
        let inspect_fn = move |result: &StreamExecutorResult<(K, OwnedRow)>| {
            // Only log when in dry-run mode and we would have pruned but got results
            if should_prune_entirely && result.is_ok() {
                tracing::warn!(
                    table_id = %table_id,
                    "vnode stats pruning dry run fails for rev_iter. This will not affect correctness."
                );
            }
        };

        if let Some(rows) = &self.all_rows {
            return Ok(futures::future::Either::Right(
                futures::future::Either::Left(
                    futures::stream::iter(
                        rows.get(&vnode)
                            .expect("covered vnode")
                            .range((pruned_start, pruned_end))
                            .rev()
                            .map(move |(key, value)| {
                                Ok((K::from_vnode_bytes(vnode, key), value.clone()))
                            }),
                    )
                    .inspect(inspect_fn),
                ),
            ));
        }
        let read_options = ReadOptions {
            prefix_hint,
            prefetch_options,
            cache_policy: CachePolicy::Fill(Hint::Normal),
        };

        Ok(futures::future::Either::Right(
            futures::future::Either::Right(
                deserialize_keyed_row_stream(
                    self.state_store
                        .rev_iter(
                            prefixed_range_with_vnode((pruned_start, pruned_end), vnode),
                            read_options,
                        )
                        .await?,
                    &*self.row_serde,
                )
                .inspect(inspect_fn),
            ),
        ))
    }
}

impl<S, SD, const IS_REPLICATED: bool> StateTableInner<S, SD, IS_REPLICATED>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    /// This function scans rows from the relational table with specific `prefix` and `sub_range` under the same
    /// `vnode`. If `sub_range` is (Unbounded, Unbounded), it scans rows from the relational table with specific `pk_prefix`.
    /// `pk_prefix` is used to identify the exact vnode the scan should perform on.
    pub async fn iter_with_prefix(
        &self,
        pk_prefix: impl Row,
        sub_range: &(Bound<impl Row>, Bound<impl Row>),
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl RowStream<'_>> {
        let stream = self.iter_with_prefix_inner::</* REVERSE */ false, ()>(pk_prefix, sub_range, prefetch_options)
            .await?;
        Ok(stream.map_ok(|(_, row)| {
            if IS_REPLICATED {
                row.project(&self.output_indices).into_owned_row()
            } else {
                row
            }
        }))
    }

    /// This function scans rows from the relational table with specific `prefix` and `sub_range` under the same
    /// `vnode`, and filters out rows based on watermarks. It calls `iter_with_prefix` and further filters rows
    /// based on the table watermark retrieved from the state store.
    ///
    /// The caller must ensure that `clean_watermark_index` is set before calling this method, otherwise it will return all rows without filtering.
    pub async fn iter_with_prefix_respecting_watermark(
        &self,
        pk_prefix: impl Row,
        sub_range: &(Bound<impl Row>, Bound<impl Row>),
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<BoxedRowStream<'_>> {
        let vnode = self.compute_prefix_vnode(&pk_prefix);
        let Some(clean_watermark_index) = self.clean_watermark_index else {
            return self
                .iter_with_prefix(pk_prefix, sub_range, prefetch_options)
                .await
                .map(|s| s.boxed());
        };
        let Some((watermark_serde, watermark_type)) = &self.watermark_serde else {
            return Err(StreamExecutorError::from(anyhow!(
                "Missing watermark serde"
            )));
        };
        // Fast path. TableWatermarksIndex::rewrite_range_with_table_watermark has already filtered the rows.
        if matches!(watermark_type, WatermarkSerdeType::PkPrefix) {
            return self
                .iter_with_prefix(pk_prefix, sub_range, prefetch_options)
                .await
                .map(|s| s.boxed());
        }

        let watermark_bytes = self.row_store.state_store.get_table_watermark(vnode);
        let Some(watermark_bytes) = watermark_bytes else {
            return self
                .iter_with_prefix(pk_prefix, sub_range, prefetch_options)
                .await
                .map(|s| s.boxed());
        };
        let watermark_row = watermark_serde.deserialize(&watermark_bytes)?;
        if watermark_row.len() != 1 {
            return Err(StreamExecutorError::from(format!(
                "Watermark row should have exactly 1 column, got {}",
                watermark_row.len()
            )));
        }
        let watermark_value = watermark_row[0].clone();
        // StateTableInner::update_watermark should ensure that the watermark is not NULL
        if watermark_value.is_none() {
            return Err(StreamExecutorError::from(anyhow!(
                "Watermark cannot be NULL"
            )));
        }
        let order_type = watermark_serde.get_order_types().get(0).ok_or_else(|| {
            StreamExecutorError::from(anyhow!(
                "Watermark serde should have at least one order type"
            ))
        })?;

        let direction = if order_type.is_ascending() {
            WatermarkDirection::Ascending
        } else {
            WatermarkDirection::Descending
        };
        let clean_watermark_index_in_pk = self
            .pk_indices
            .iter()
            .position(|&i| i == clean_watermark_index);
        let clean_watermark_index_in_value = match &self.value_indices {
            Some(value_indices) => value_indices
                .iter()
                .position(|idx| *idx == clean_watermark_index)
                .ok_or_else(|| {
                    StreamExecutorError::from(anyhow!(
                        "clean watermark column index {} is not included in table value indices {:?}",
                        clean_watermark_index,
                        value_indices
                    ))
                })?,
            None => clean_watermark_index,
        };

        let stream = self
            .iter_with_prefix_inner::</* REVERSE */ false, Bytes>(pk_prefix, sub_range, prefetch_options)
            .await?
            .try_filter_map(move |(pk, row)| {
                let should_filter =  match watermark_type {
                    WatermarkSerdeType::PkPrefix => unreachable!(),
                    WatermarkSerdeType::NonPkPrefix => {
                        let table_key = TableKey(pk);
                        let (vnode, key) = table_key.split_vnode();
                        let pk_cols = self.pk_serde
                        .deserialize(key)
                        .unwrap_or_else(|e| {
                            panic!("Failed to deserialize table {} vnode {:?} key {:?} error: {:?}", self.table_id(), vnode, key, e.as_report());
                        });
                        direction.datum_filter_by_watermark(
                            pk_cols.datum_at(clean_watermark_index_in_pk.unwrap()),
                            &watermark_value,
                            *order_type,
                        )
                    },
                    WatermarkSerdeType::Value => {
                        direction.datum_filter_by_watermark(
                            row.datum_at(clean_watermark_index_in_value),
                            &watermark_value,
                            *order_type,
                        )
                    }
                };
                if should_filter {
                    ready(Ok(None))
                } else {
                    ready(Ok(Some(row)))
                }
            });
        Ok(stream.boxed())
    }

    /// Get the row from a state table with only 1 row.
    pub async fn get_from_one_row_table(&self) -> StreamExecutorResult<Option<OwnedRow>> {
        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Unbounded, Unbounded);
        let stream = self
            .iter_with_prefix(row::empty(), sub_range, Default::default())
            .await?;
        pin_mut!(stream);

        if let Some(res) = stream.next().await {
            let value = res?.into_owned_row();
            assert!(stream.next().await.is_none());
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    /// Get the row from a state table with only 1 row, and the row has only 1 col.
    ///
    /// `None` can mean either the row is never persisted, or is a persisted `NULL`,
    /// which does not matter in the use case.
    pub async fn get_from_one_value_table(&self) -> StreamExecutorResult<Option<ScalarImpl>> {
        Ok(self
            .get_from_one_row_table()
            .await?
            .and_then(|row| row[0].clone()))
    }

    pub async fn iter_keyed_row_with_prefix(
        &self,
        pk_prefix: impl Row,
        sub_range: &(Bound<impl Row>, Bound<impl Row>),
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl KeyedRowStream<'_>> {
        Ok(
            self.iter_with_prefix_inner::</* REVERSE */ false, Bytes>(pk_prefix, sub_range, prefetch_options)
                .await?.map_ok(|(key, row)| KeyedRow::new(TableKey(key), row)),
        )
    }

    pub async fn rev_iter_keyed_row_with_prefix(
        &self,
        pk_prefix: impl Row,
        sub_range: &(Bound<impl Row>, Bound<impl Row>),
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl KeyedRowStream<'_>> {
        Ok(
            self.iter_with_prefix_inner::</* REVERSE */ true, Bytes>(pk_prefix, sub_range, prefetch_options)
            .await?.map_ok(|(key, row)| KeyedRow::new(TableKey(key), row)),
        )
    }

    /// This function scans the table just like `iter_with_prefix`, but in reverse order.
    pub async fn rev_iter_with_prefix(
        &self,
        pk_prefix: impl Row,
        sub_range: &(Bound<impl Row>, Bound<impl Row>),
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl RowStream<'_>> {
        Ok(
            self.iter_with_prefix_inner::</* REVERSE */ true, ()>(pk_prefix, sub_range, prefetch_options)
                .await?.map_ok(|(_, row)| row),
        )
    }

    async fn iter_with_prefix_inner<const REVERSE: bool, K: CopyFromSlice + FromVnodeBytes>(
        &self,
        pk_prefix: impl Row,
        sub_range: &(Bound<impl Row>, Bound<impl Row>),
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl PkRowStream<'_, K>> {
        let prefix_serializer = self.pk_serde.prefix(pk_prefix.len());
        let encoded_prefix = serialize_pk(&pk_prefix, &prefix_serializer);

        // We assume that all usages of iterating the state table only access a single vnode.
        // If this assertion fails, then something must be wrong with the operator implementation or
        // the distribution derivation from the optimizer.
        let vnode = self.compute_prefix_vnode(&pk_prefix);

        // Construct prefix hint for prefix bloom filter.
        let pk_prefix_indices = &self.pk_indices[..pk_prefix.len()];
        if self.prefix_hint_len != 0 && !IS_REPLICATED {
            debug_assert_eq!(self.prefix_hint_len, pk_prefix.len());
        }
        let prefix_hint = {
            if should_calculate_prefix_hint(self.prefix_hint_len, pk_prefix.len(), true) {
                let encoded_prefix_len = self
                    .pk_serde
                    .deserialize_prefix_len(&encoded_prefix, self.prefix_hint_len)?;

                Some(Bytes::copy_from_slice(
                    &encoded_prefix[..encoded_prefix_len],
                ))
            } else {
                None
            }
        };

        trace!(
            table_id = %self.table_id(),
            ?prefix_hint, ?pk_prefix,
            ?pk_prefix_indices,
            iter_direction = if REVERSE { "reverse" } else { "forward" },
            "storage_iter_with_prefix"
        );

        let memcomparable_range =
            prefix_and_sub_range_to_memcomparable(&self.pk_serde, sub_range, pk_prefix);

        Ok(if REVERSE {
            futures::future::Either::Left(
                self.row_store
                    .rev_iter_kv(vnode, memcomparable_range, prefix_hint, prefetch_options)
                    .await?,
            )
        } else {
            futures::future::Either::Right(
                self.row_store
                    .iter_kv(vnode, memcomparable_range, prefix_hint, prefetch_options)
                    .await?,
            )
        })
    }

    /// This function scans raw key-values from the relational table with specific `pk_range` under
    /// the same `vnode`.
    async fn iter_kv_with_pk_range<'a, K: CopyFromSlice + FromVnodeBytes>(
        &'a self,
        pk_range: &(Bound<impl Row>, Bound<impl Row>),
        // Optional vnode that returns an iterator only over the given range under that vnode.
        // For now, we require this parameter, and will panic. In the future, when `None`, we can
        // iterate over each vnode that the `StateTableInner` owns.
        vnode: VirtualNode,
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl PkRowStream<'a, K>> {
        let memcomparable_range = prefix_range_to_memcomparable(&self.pk_serde, pk_range);

        // TODO: provide a trace of useful params.
        self.row_store
            .iter_kv(vnode, memcomparable_range, None, prefetch_options)
            .await
    }
}

fn deserialize_keyed_row_stream<'a, K: CopyFromSlice>(
    iter: impl StateStoreIter + 'a,
    deserializer: &'a impl ValueRowSerde,
) -> impl PkRowStream<'a, K> {
    iter.into_stream(move |(key, value)| {
        Ok((
            K::copy_from_slice(key.user_key.table_key.as_ref()),
            deserializer.deserialize(value).map(OwnedRow::new)?,
        ))
    })
    .map_err(Into::into)
}

pub fn prefix_range_to_memcomparable(
    pk_serde: &OrderedRowSerde,
    range: &(Bound<impl Row>, Bound<impl Row>),
) -> (Bound<Bytes>, Bound<Bytes>) {
    (
        start_range_to_memcomparable(pk_serde, &range.0),
        end_range_to_memcomparable(pk_serde, &range.1, None),
    )
}

fn prefix_and_sub_range_to_memcomparable(
    pk_serde: &OrderedRowSerde,
    sub_range: &(Bound<impl Row>, Bound<impl Row>),
    pk_prefix: impl Row,
) -> (Bound<Bytes>, Bound<Bytes>) {
    let (range_start, range_end) = sub_range;
    let prefix_serializer = pk_serde.prefix(pk_prefix.len());
    let serialized_pk_prefix = serialize_pk(&pk_prefix, &prefix_serializer);
    let start_range = match range_start {
        Included(start_range) => Bound::Included(Either::Left((&pk_prefix).chain(start_range))),
        Excluded(start_range) => Bound::Excluded(Either::Left((&pk_prefix).chain(start_range))),
        Unbounded => Bound::Included(Either::Right(&pk_prefix)),
    };
    let end_range = match range_end {
        Included(end_range) => Bound::Included((&pk_prefix).chain(end_range)),
        Excluded(end_range) => Bound::Excluded((&pk_prefix).chain(end_range)),
        Unbounded => Unbounded,
    };
    (
        start_range_to_memcomparable(pk_serde, &start_range),
        end_range_to_memcomparable(pk_serde, &end_range, Some(serialized_pk_prefix)),
    )
}

fn start_range_to_memcomparable<R: Row>(
    pk_serde: &OrderedRowSerde,
    bound: &Bound<R>,
) -> Bound<Bytes> {
    let serialize_pk_prefix = |pk_prefix: &R| {
        let prefix_serializer = pk_serde.prefix(pk_prefix.len());
        serialize_pk(pk_prefix, &prefix_serializer)
    };
    match bound {
        Unbounded => Unbounded,
        Included(r) => {
            let serialized = serialize_pk_prefix(r);

            Included(serialized)
        }
        Excluded(r) => {
            let serialized = serialize_pk_prefix(r);

            start_bound_of_excluded_prefix(&serialized)
        }
    }
}

fn end_range_to_memcomparable<R: Row>(
    pk_serde: &OrderedRowSerde,
    bound: &Bound<R>,
    serialized_pk_prefix: Option<Bytes>,
) -> Bound<Bytes> {
    let serialize_pk_prefix = |pk_prefix: &R| {
        let prefix_serializer = pk_serde.prefix(pk_prefix.len());
        serialize_pk(pk_prefix, &prefix_serializer)
    };
    match bound {
        Unbounded => match serialized_pk_prefix {
            Some(serialized_pk_prefix) => end_bound_of_prefix(&serialized_pk_prefix),
            None => Unbounded,
        },
        Included(r) => {
            let serialized = serialize_pk_prefix(r);
            // TODO: may use Included(serialized)?
            end_bound_of_prefix(&serialized)
        }
        Excluded(r) => {
            let serialized = serialize_pk_prefix(r);
            Excluded(serialized)
        }
    }
}

fn fill_non_output_indices(
    i2o_mapping: &ColIndexMapping,
    data_types: &[DataType],
    chunk: StreamChunk,
) -> StreamChunk {
    let (ops, columns, vis) = chunk.into_inner();
    let capacity = vis.len();
    let mut full_columns = Vec::with_capacity(data_types.len());
    for (i, data_type) in data_types.iter().enumerate() {
        if let Some(j) = i2o_mapping.try_map(i) {
            full_columns.push(columns[j].clone());
        } else {
            let mut column_builder = ArrayImplBuilder::with_type(capacity, data_type.clone());
            column_builder.append_n_null(capacity);
            let column: ArrayRef = column_builder.finish().into();
            full_columns.push(column)
        }
    }
    let data_chunk = DataChunk::new(full_columns, vis);
    StreamChunk::from_parts(ops, data_chunk)
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use expect_test::{Expect, expect};

    use super::*;

    fn check(actual: impl Debug, expect: Expect) {
        let actual = format!("{:#?}", actual);
        expect.assert_eq(&actual);
    }

    #[test]
    fn test_fill_non_output_indices() {
        let data_types = vec![DataType::Int32, DataType::Int32, DataType::Int32];
        let replicated_chunk = [OwnedRow::new(vec![
            Some(222_i32.into()),
            Some(2_i32.into()),
        ])];
        let replicated_chunk = StreamChunk::from_parts(
            vec![Op::Insert],
            DataChunk::from_rows(&replicated_chunk, &[DataType::Int32, DataType::Int32]),
        );
        let i2o_mapping = ColIndexMapping::new(vec![Some(1), None, Some(0)], 2);
        let filled_chunk = fill_non_output_indices(&i2o_mapping, &data_types, replicated_chunk);
        check(
            filled_chunk,
            expect![[r#"
            StreamChunk { cardinality: 1, capacity: 1, data:
            +---+---+---+-----+
            | + | 2 |   | 222 |
            +---+---+---+-----+
             }"#]],
        );
    }

    #[test]
    fn test_fill_non_output_indices_with_invisible_rows() {
        let data_types = vec![DataType::Int32, DataType::Int32, DataType::Int32];
        let replicated_chunk = [
            OwnedRow::new(vec![Some(222_i32.into()), Some(2_i32.into())]),
            OwnedRow::new(vec![Some(333_i32.into()), Some(3_i32.into())]),
        ];
        let (columns, _) =
            DataChunk::from_rows(&replicated_chunk, &[DataType::Int32, DataType::Int32])
                .into_parts();
        let replicated_chunk = StreamChunk::with_visibility(
            vec![Op::Insert, Op::Insert],
            columns,
            Bitmap::from_iter([false, false]),
        );
        let i2o_mapping = ColIndexMapping::new(vec![Some(1), None, Some(0)], 2);
        let filled_chunk = fill_non_output_indices(&i2o_mapping, &data_types, replicated_chunk);
        check(
            filled_chunk,
            expect![[r#"
            StreamChunk { cardinality: 0, capacity: 2, data:
            (empty)
             }"#]],
        );
    }
}
