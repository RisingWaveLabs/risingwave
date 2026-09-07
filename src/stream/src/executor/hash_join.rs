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

use std::assert_matches;
use std::collections::{BTreeMap, HashSet};
use std::marker::PhantomData;
use std::time::Duration;

use anyhow::Context;
use either::Either;
use itertools::Itertools;
use multimap::MultiMap;
use risingwave_common::array::Op;
use risingwave_common::hash::{HashKey, NullBitmap};
use risingwave_common::metrics::LabelGuardedHistogram;
use risingwave_common::row::RowExt;
use risingwave_common::types::{DefaultOrd, ToOwnedDatum};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_expr::expr::NonStrictExpression;
use risingwave_pb::stream_plan::InequalityType;
use tokio::time::Instant;

use self::builder::JoinChunkBuilder;
use super::barrier_align::*;
use super::join::hash_join::*;
use super::join::row::{JoinEncoding, JoinRow};
use super::join::*;
use super::watermark::*;
use crate::executor::CachedJoinRow;
use crate::executor::join::builder::JoinStreamChunkBuilder;
use crate::executor::join::hash_join::CacheResult;
use crate::executor::prelude::*;

fn is_subset(vec1: Vec<usize>, vec2: Vec<usize>) -> bool {
    HashSet::<usize>::from_iter(vec1).is_subset(&vec2.into_iter().collect())
}

/// Information about an inequality condition in the join.
/// Represents: `left_col <op> right_col` where op is one of `<`, `<=`, `>`, `>=`.
#[derive(Debug, Clone)]
pub struct InequalityPairInfo {
    /// Index of the left side column (from left input).
    pub left_idx: usize,
    /// Index of the right side column (from right input).
    pub right_idx: usize,
    /// Whether this condition is used to clean left state table.
    pub clean_left_state: bool,
    /// Whether this condition is used to clean right state table.
    pub clean_right_state: bool,
    /// Comparison operator.
    pub op: InequalityType,
}

impl InequalityPairInfo {
    /// Returns true if left side has larger values (`>` or `>=`).
    pub fn left_side_is_larger(&self) -> bool {
        matches!(
            self.op,
            InequalityType::GreaterThan | InequalityType::GreaterThanOrEqual
        )
    }
}

pub struct JoinParams {
    /// Indices of the join keys
    pub join_key_indices: Vec<usize>,
    /// Indices of the input pk after dedup
    pub deduped_pk_indices: Vec<usize>,
}

impl JoinParams {
    pub fn new(join_key_indices: Vec<usize>, deduped_pk_indices: Vec<usize>) -> Self {
        Self {
            join_key_indices,
            deduped_pk_indices,
        }
    }
}

struct JoinSide<K: HashKey, S: StateStore, E: JoinEncoding> {
    /// Store all data from a one side stream
    ht: JoinHashMap<K, S, E>,
    /// Indices of the join key columns
    join_key_indices: Vec<usize>,
    /// The data type of all columns without degree.
    all_data_types: Vec<DataType>,
    /// The start position for the side in output new columns
    start_pos: usize,
    /// The mapping from input indices of a side to output columns.
    i2o_mapping: Vec<(usize, usize)>,
    i2o_mapping_indexed: MultiMap<usize, usize>,
    /// The first field of the ith element indicates that when a watermark at the ith column of
    /// this side comes, what band join conditions should be updated in order to possibly
    /// generate a new watermark at that column or the corresponding column in the counterpart
    /// join side.
    ///
    /// The second field indicates that whether the column is required less than the
    /// the corresponding column in the counterpart join side in the band join condition.
    input2inequality_index: Vec<Vec<(usize, bool)>>,
    /// Some fields which are required non null to match due to inequalities.
    non_null_fields: Vec<usize>,
    /// (i, j) in this `Vec` means that state data in this join side can be cleaned if the value of
    /// its ith column is less than the synthetic watermark of the jth band join condition.
    state_clean_columns: Vec<(usize, usize)>,
    /// Whether degree table is needed for this side.
    need_degree_table: bool,
    _marker: std::marker::PhantomData<E>,
}

impl<K: HashKey, S: StateStore, E: JoinEncoding> std::fmt::Debug for JoinSide<K, S, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinSide")
            .field("join_key_indices", &self.join_key_indices)
            .field("col_types", &self.all_data_types)
            .field("start_pos", &self.start_pos)
            .field("i2o_mapping", &self.i2o_mapping)
            .field("need_degree_table", &self.need_degree_table)
            .finish()
    }
}

impl<K: HashKey, S: StateStore, E: JoinEncoding> JoinSide<K, S, E> {
    // WARNING: Please do not call this until we implement it.
    fn is_dirty(&self) -> bool {
        unimplemented!()
    }

    #[expect(dead_code)]
    fn clear_cache(&mut self) {
        assert!(
            !self.is_dirty(),
            "cannot clear cache while states of hash join are dirty"
        );

        // TODO: not working with rearranged chain
        // self.ht.clear();
    }

    pub async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.ht.init(epoch).await
    }
}

/// `HashJoinExecutor` takes two input streams and runs equal hash join on them.
/// The output columns are the concatenation of left and right columns.
pub struct HashJoinExecutor<K: HashKey, S: StateStore, const T: JoinTypePrimitive, E: JoinEncoding>
{
    ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Left input executor
    input_l: Option<Executor>,
    /// Right input executor
    input_r: Option<Executor>,
    /// The data types of the formed new columns
    actual_output_data_types: Vec<DataType>,
    /// The parameters of the left join executor
    side_l: JoinSide<K, S, E>,
    /// The parameters of the right join executor
    side_r: JoinSide<K, S, E>,
    /// Optional non-equi join conditions
    cond: Option<NonStrictExpression>,
    /// Inequality pairs with output column indices.
    /// Each entry contains: (`output_indices`, `InequalityPairInfo`).
    inequality_pairs: Vec<(Vec<usize>, InequalityPairInfo)>,
    /// The output watermark of each inequality condition and its value is the minimum of the
    /// watermarks from both sides. It will be used to generate watermark into downstream
    /// and do state cleaning based on `clean_left_state`/`clean_right_state` fields.
    inequality_watermarks: Vec<Option<Watermark>>,
    /// Join-key positions and cleaning flags for watermark handling.
    /// Each entry: (join-key position, `do_state_cleaning`).
    watermark_indices_in_jk: Vec<(usize, bool)>,

    /// Whether the logic can be optimized for append-only stream
    append_only_optimize: bool,

    metrics: Arc<StreamingMetrics>,
    /// The maximum size of the chunk produced by executor at a time
    chunk_size: usize,
    /// Count the messages received, clear to 0 when counted to `EVICT_EVERY_N_MESSAGES`
    cnt_rows_received: u32,

    /// watermark column index -> `BufferedWatermarks`
    watermark_buffers: BTreeMap<usize, BufferedWatermarks<SideTypePrimitive>>,

    /// When to alert high join amplification
    high_join_amplification_threshold: usize,

    /// Max number of rows that will be cached in the entry state.
    entry_state_max_rows: usize,
    /// Number of processed rows between periodic manual evictions of the join cache.
    join_cache_evict_interval_rows: u32,
}

impl<K: HashKey, S: StateStore, const T: JoinTypePrimitive, E: JoinEncoding> std::fmt::Debug
    for HashJoinExecutor<K, S, T, E>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashJoinExecutor")
            .field("join_type", &T)
            .field("input_left", &self.input_l.as_ref().unwrap().identity())
            .field("input_right", &self.input_r.as_ref().unwrap().identity())
            .field("side_l", &self.side_l)
            .field("side_r", &self.side_r)
            .field("stream_key", &self.info.stream_key)
            .field("schema", &self.info.schema)
            .field("actual_output_data_types", &self.actual_output_data_types)
            .field(
                "join_cache_evict_interval_rows",
                &self.join_cache_evict_interval_rows,
            )
            .finish()
    }
}

impl<K: HashKey, S: StateStore, const T: JoinTypePrimitive, E: JoinEncoding> Execute
    for HashJoinExecutor<K, S, T, E>
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

struct EqJoinArgs<'a, K: HashKey, S: StateStore, E: JoinEncoding> {
    ctx: &'a ActorContextRef,
    side_l: &'a mut JoinSide<K, S, E>,
    side_r: &'a mut JoinSide<K, S, E>,
    actual_output_data_types: &'a [DataType],
    cond: &'a mut Option<NonStrictExpression>,
    inequality_watermarks: &'a [Option<Watermark>],
    chunk: StreamChunk,
    append_only_optimize: bool,
    chunk_size: usize,
    cnt_rows_received: &'a mut u32,
    high_join_amplification_threshold: usize,
    entry_state_max_rows: usize,
    join_cache_evict_interval_rows: u32,
    join_matched_join_keys: &'a LabelGuardedHistogram,
}

impl<K: HashKey, S: StateStore, const T: JoinTypePrimitive, E: JoinEncoding>
    HashJoinExecutor<K, S, T, E>
{
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        info: ExecutorInfo,
        input_l: Executor,
        input_r: Executor,
        params_l: JoinParams,
        params_r: JoinParams,
        null_safe: Vec<bool>,
        output_indices: Vec<usize>,
        cond: Option<NonStrictExpression>,
        inequality_pairs: Vec<InequalityPairInfo>,
        state_table_l: StateTable<S>,
        degree_state_table_l: StateTable<S>,
        state_table_r: StateTable<S>,
        degree_state_table_r: StateTable<S>,
        watermark_epoch: AtomicU64Ref,
        is_append_only: bool,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
        high_join_amplification_threshold: usize,
        watermark_indices_in_jk: Vec<(usize, bool)>,
    ) -> Self {
        Self::new_with_cache_size(
            ctx,
            info,
            input_l,
            input_r,
            params_l,
            params_r,
            null_safe,
            output_indices,
            cond,
            inequality_pairs,
            state_table_l,
            degree_state_table_l,
            state_table_r,
            degree_state_table_r,
            watermark_epoch,
            is_append_only,
            metrics,
            chunk_size,
            high_join_amplification_threshold,
            None,
            watermark_indices_in_jk,
        )
    }

    #[expect(clippy::too_many_arguments)]
    pub fn new_with_cache_size(
        ctx: ActorContextRef,
        info: ExecutorInfo,
        input_l: Executor,
        input_r: Executor,
        params_l: JoinParams,
        params_r: JoinParams,
        null_safe: Vec<bool>,
        output_indices: Vec<usize>,
        cond: Option<NonStrictExpression>,
        inequality_pairs: Vec<InequalityPairInfo>,
        state_table_l: StateTable<S>,
        degree_state_table_l: StateTable<S>,
        state_table_r: StateTable<S>,
        degree_state_table_r: StateTable<S>,
        watermark_epoch: AtomicU64Ref,
        is_append_only: bool,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
        high_join_amplification_threshold: usize,
        entry_state_max_rows: Option<usize>,
        watermark_indices_in_jk: Vec<(usize, bool)>,
    ) -> Self {
        let entry_state_max_rows = match entry_state_max_rows {
            None => ctx.config.developer.hash_join_entry_state_max_rows,
            Some(entry_state_max_rows) => entry_state_max_rows,
        };
        let join_cache_evict_interval_rows = ctx
            .config
            .developer
            .join_hash_map_evict_interval_rows
            .max(1);
        let side_l_column_n = input_l.schema().len();

        let schema_fields = match T {
            JoinType::LeftSemi | JoinType::LeftAnti => input_l.schema().fields.clone(),
            JoinType::RightSemi | JoinType::RightAnti => input_r.schema().fields.clone(),
            _ => [
                input_l.schema().fields.clone(),
                input_r.schema().fields.clone(),
            ]
            .concat(),
        };

        let original_output_data_types = schema_fields
            .iter()
            .map(|field| field.data_type())
            .collect_vec();
        let actual_output_data_types = output_indices
            .iter()
            .map(|&idx| original_output_data_types[idx].clone())
            .collect_vec();

        // Data types of of hash join state.
        let state_all_data_types_l = input_l.schema().data_types();
        let state_all_data_types_r = input_r.schema().data_types();

        let state_pk_indices_l = input_l.stream_key().to_vec();
        let state_pk_indices_r = input_r.stream_key().to_vec();

        let state_join_key_indices_l = params_l.join_key_indices;
        let state_join_key_indices_r = params_r.join_key_indices;

        let degree_join_key_indices_l = (0..state_join_key_indices_l.len()).collect_vec();
        let degree_join_key_indices_r = (0..state_join_key_indices_r.len()).collect_vec();

        let degree_pk_indices_l = (state_join_key_indices_l.len()
            ..state_join_key_indices_l.len() + params_l.deduped_pk_indices.len())
            .collect_vec();
        let degree_pk_indices_r = (state_join_key_indices_r.len()
            ..state_join_key_indices_r.len() + params_r.deduped_pk_indices.len())
            .collect_vec();

        // If pk is contained in join key.
        let pk_contained_in_jk_l = is_subset(state_pk_indices_l, state_join_key_indices_l.clone());
        let pk_contained_in_jk_r = is_subset(state_pk_indices_r, state_join_key_indices_r.clone());

        // check whether join key contains pk in both side
        let append_only_optimize = is_append_only && pk_contained_in_jk_l && pk_contained_in_jk_r;

        let join_key_data_types_l = state_join_key_indices_l
            .iter()
            .map(|idx| state_all_data_types_l[*idx].clone())
            .collect_vec();

        let join_key_data_types_r = state_join_key_indices_r
            .iter()
            .map(|idx| state_all_data_types_r[*idx].clone())
            .collect_vec();

        assert_eq!(join_key_data_types_l, join_key_data_types_r);

        let null_matched = K::Bitmap::from_bool_vec(null_safe);

        let need_degree_table_l = need_left_degree(T) && !pk_contained_in_jk_r;
        let need_degree_table_r = need_right_degree(T) && !pk_contained_in_jk_l;

        let (left_to_output, right_to_output) = {
            let (left_len, right_len) = if is_left_semi_or_anti(T) {
                (state_all_data_types_l.len(), 0usize)
            } else if is_right_semi_or_anti(T) {
                (0usize, state_all_data_types_r.len())
            } else {
                (state_all_data_types_l.len(), state_all_data_types_r.len())
            };
            JoinStreamChunkBuilder::get_i2o_mapping(&output_indices, left_len, right_len)
        };

        let l2o_indexed = MultiMap::from_iter(left_to_output.iter().copied());
        let r2o_indexed = MultiMap::from_iter(right_to_output.iter().copied());

        let left_input_len = input_l.schema().len();
        let right_input_len = input_r.schema().len();
        let mut l2inequality_index = vec![vec![]; left_input_len];
        let mut r2inequality_index = vec![vec![]; right_input_len];
        let mut l_inequal_state_clean_columns = vec![];
        let mut r_inequal_state_clean_columns = vec![];
        let inequality_pairs = inequality_pairs
            .into_iter()
            .enumerate()
            .map(|(index, pair)| {
                // Map input columns to inequality index
                // The second field indicates whether this column is required to be less than
                // the corresponding column on the other side.
                // For `left >= right`: left is NOT less than right, right IS less than left
                // For `left <= right`: left IS less than right, right is NOT less than left
                let left_is_larger = pair.left_side_is_larger();
                l2inequality_index[pair.left_idx].push((index, !left_is_larger));
                r2inequality_index[pair.right_idx].push((index, left_is_larger));

                // Determine state cleanup columns
                if pair.clean_left_state {
                    l_inequal_state_clean_columns.push((pair.left_idx, index));
                }
                if pair.clean_right_state {
                    r_inequal_state_clean_columns.push((pair.right_idx, index));
                }

                // Get output indices for watermark emission
                // We only emit watermarks for the LARGER side's output columns
                let output_indices = if pair.left_side_is_larger() {
                    // Left is larger, emit for left output columns
                    l2o_indexed
                        .get_vec(&pair.left_idx)
                        .cloned()
                        .unwrap_or_default()
                } else {
                    // Right is larger, emit for right output columns
                    r2o_indexed
                        .get_vec(&pair.right_idx)
                        .cloned()
                        .unwrap_or_default()
                };

                (output_indices, pair)
            })
            .collect_vec();

        let mut l_non_null_fields = l2inequality_index
            .iter()
            .positions(|inequalities| !inequalities.is_empty())
            .collect_vec();
        let mut r_non_null_fields = r2inequality_index
            .iter()
            .positions(|inequalities| !inequalities.is_empty())
            .collect_vec();

        if append_only_optimize {
            l_inequal_state_clean_columns.clear();
            r_inequal_state_clean_columns.clear();
            l_non_null_fields.clear();
            r_non_null_fields.clear();
        }

        // Create degree tables with inequality column index for watermark-based cleaning.
        // The degree_inequality_idx is the column index in the input row that should be
        // stored in the degree table for inequality-based watermark cleaning.
        let l_inequality_idx = l_inequal_state_clean_columns
            .first()
            .map(|(col_idx, _)| *col_idx);
        let r_inequality_idx = r_inequal_state_clean_columns
            .first()
            .map(|(col_idx, _)| *col_idx);

        let degree_state_l = need_degree_table_l.then(|| {
            TableInner::new(
                degree_pk_indices_l,
                degree_join_key_indices_l,
                degree_state_table_l,
                l_inequality_idx,
            )
        });
        let degree_state_r = need_degree_table_r.then(|| {
            TableInner::new(
                degree_pk_indices_r,
                degree_join_key_indices_r,
                degree_state_table_r,
                r_inequality_idx,
            )
        });

        let inequality_watermarks = vec![None; inequality_pairs.len()];
        let watermark_buffers = BTreeMap::new();
        Self {
            ctx: ctx.clone(),
            info,
            input_l: Some(input_l),
            input_r: Some(input_r),
            actual_output_data_types,
            side_l: JoinSide {
                ht: JoinHashMap::new(
                    watermark_epoch.clone(),
                    join_key_data_types_l,
                    state_join_key_indices_l.clone(),
                    state_all_data_types_l.clone(),
                    state_table_l,
                    params_l.deduped_pk_indices,
                    degree_state_l,
                    null_matched.clone(),
                    pk_contained_in_jk_l,
                    metrics.clone(),
                    ctx.id,
                    ctx.fragment_id,
                    "left",
                ),
                join_key_indices: state_join_key_indices_l,
                all_data_types: state_all_data_types_l,
                i2o_mapping: left_to_output,
                i2o_mapping_indexed: l2o_indexed,
                input2inequality_index: l2inequality_index,
                non_null_fields: l_non_null_fields,
                state_clean_columns: l_inequal_state_clean_columns,
                start_pos: 0,
                need_degree_table: need_degree_table_l,
                _marker: PhantomData,
            },
            side_r: JoinSide {
                ht: JoinHashMap::new(
                    watermark_epoch,
                    join_key_data_types_r,
                    state_join_key_indices_r.clone(),
                    state_all_data_types_r.clone(),
                    state_table_r,
                    params_r.deduped_pk_indices,
                    degree_state_r,
                    null_matched,
                    pk_contained_in_jk_r,
                    metrics.clone(),
                    ctx.id,
                    ctx.fragment_id,
                    "right",
                ),
                join_key_indices: state_join_key_indices_r,
                all_data_types: state_all_data_types_r,
                start_pos: side_l_column_n,
                i2o_mapping: right_to_output,
                i2o_mapping_indexed: r2o_indexed,
                input2inequality_index: r2inequality_index,
                non_null_fields: r_non_null_fields,
                state_clean_columns: r_inequal_state_clean_columns,
                need_degree_table: need_degree_table_r,
                _marker: PhantomData,
            },
            cond,
            inequality_pairs,
            inequality_watermarks,
            watermark_indices_in_jk,
            append_only_optimize,
            metrics,
            chunk_size,
            cnt_rows_received: 0,
            watermark_buffers,
            high_join_amplification_threshold,
            entry_state_max_rows,
            join_cache_evict_interval_rows,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let input_l = self.input_l.take().unwrap();
        let input_r = self.input_r.take().unwrap();
        let aligned_stream = barrier_align(
            input_l.execute(),
            input_r.execute(),
            self.ctx.id,
            self.ctx.fragment_id,
            self.metrics.clone(),
            "Join",
        );
        pin_mut!(aligned_stream);

        let actor_id = self.ctx.id;

        let barrier = expect_first_barrier_from_aligned_stream(&mut aligned_stream).await?;
        let first_epoch = barrier.epoch;
        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);
        self.side_l.init(first_epoch).await?;
        self.side_r.init(first_epoch).await?;

        let actor_id_str = self.ctx.id.to_string();
        let fragment_id_str = self.ctx.fragment_id.to_string();

        // initialized some metrics
        let join_actor_input_waiting_duration_ns = self
            .metrics
            .join_actor_input_waiting_duration_ns
            .with_guarded_label_values(&[&actor_id_str, &fragment_id_str]);
        let left_join_match_duration_ns = self
            .metrics
            .join_match_duration_ns
            .with_guarded_label_values(&[actor_id_str.as_str(), fragment_id_str.as_str(), "left"]);
        let right_join_match_duration_ns = self
            .metrics
            .join_match_duration_ns
            .with_guarded_label_values(&[actor_id_str.as_str(), fragment_id_str.as_str(), "right"]);

        let barrier_join_match_duration_ns = self
            .metrics
            .join_match_duration_ns
            .with_guarded_label_values(&[
                actor_id_str.as_str(),
                fragment_id_str.as_str(),
                "barrier",
            ]);

        let left_join_cached_entry_count = self
            .metrics
            .join_cached_entry_count
            .with_guarded_label_values(&[actor_id_str.as_str(), fragment_id_str.as_str(), "left"]);

        let right_join_cached_entry_count = self
            .metrics
            .join_cached_entry_count
            .with_guarded_label_values(&[actor_id_str.as_str(), fragment_id_str.as_str(), "right"]);

        // Bind at executor scope: a per-chunk guard would be dropped between chunks and reset the series.
        let left_table_id_str = self.side_l.ht.table_id().to_string();
        let right_table_id_str = self.side_r.ht.table_id().to_string();
        let left_join_matched_join_keys = self
            .metrics
            .join_matched_join_keys
            .with_guarded_label_values(&[
                actor_id_str.as_str(),
                fragment_id_str.as_str(),
                left_table_id_str.as_str(),
            ]);
        let right_join_matched_join_keys = self
            .metrics
            .join_matched_join_keys
            .with_guarded_label_values(&[
                actor_id_str.as_str(),
                fragment_id_str.as_str(),
                right_table_id_str.as_str(),
            ]);

        let mut start_time = Instant::now();

        while let Some(msg) = aligned_stream
            .next()
            .instrument_await("hash_join_barrier_align")
            .await
        {
            join_actor_input_waiting_duration_ns.inc_by(start_time.elapsed().as_nanos() as u64);
            match msg? {
                AlignedMessage::WatermarkLeft(watermark) => {
                    for watermark_to_emit in self.handle_watermark(SideType::Left, watermark)? {
                        yield Message::Watermark(watermark_to_emit);
                    }
                }
                AlignedMessage::WatermarkRight(watermark) => {
                    for watermark_to_emit in self.handle_watermark(SideType::Right, watermark)? {
                        yield Message::Watermark(watermark_to_emit);
                    }
                }
                AlignedMessage::Left(chunk) => {
                    let mut left_time = Duration::from_nanos(0);
                    let mut left_start_time = Instant::now();
                    #[for_await]
                    for chunk in Self::eq_join_left(EqJoinArgs {
                        ctx: &self.ctx,
                        side_l: &mut self.side_l,
                        side_r: &mut self.side_r,
                        actual_output_data_types: &self.actual_output_data_types,
                        cond: &mut self.cond,
                        inequality_watermarks: &self.inequality_watermarks,
                        chunk,
                        append_only_optimize: self.append_only_optimize,
                        chunk_size: self.chunk_size,
                        cnt_rows_received: &mut self.cnt_rows_received,
                        high_join_amplification_threshold: self.high_join_amplification_threshold,
                        entry_state_max_rows: self.entry_state_max_rows,
                        join_cache_evict_interval_rows: self.join_cache_evict_interval_rows,
                        join_matched_join_keys: &left_join_matched_join_keys,
                    }) {
                        left_time += left_start_time.elapsed();
                        yield Message::Chunk(chunk?);
                        left_start_time = Instant::now();
                    }
                    left_time += left_start_time.elapsed();
                    left_join_match_duration_ns.inc_by(left_time.as_nanos() as u64);
                    self.try_flush_data().await?;
                }
                AlignedMessage::Right(chunk) => {
                    let mut right_time = Duration::from_nanos(0);
                    let mut right_start_time = Instant::now();
                    #[for_await]
                    for chunk in Self::eq_join_right(EqJoinArgs {
                        ctx: &self.ctx,
                        side_l: &mut self.side_l,
                        side_r: &mut self.side_r,
                        actual_output_data_types: &self.actual_output_data_types,
                        cond: &mut self.cond,
                        inequality_watermarks: &self.inequality_watermarks,
                        chunk,
                        append_only_optimize: self.append_only_optimize,
                        chunk_size: self.chunk_size,
                        cnt_rows_received: &mut self.cnt_rows_received,
                        high_join_amplification_threshold: self.high_join_amplification_threshold,
                        entry_state_max_rows: self.entry_state_max_rows,
                        join_cache_evict_interval_rows: self.join_cache_evict_interval_rows,
                        join_matched_join_keys: &right_join_matched_join_keys,
                    }) {
                        right_time += right_start_time.elapsed();
                        yield Message::Chunk(chunk?);
                        right_start_time = Instant::now();
                    }
                    right_time += right_start_time.elapsed();
                    right_join_match_duration_ns.inc_by(right_time.as_nanos() as u64);
                    self.try_flush_data().await?;
                }
                AlignedMessage::Barrier(barrier) => {
                    let barrier_start_time = Instant::now();
                    let (left_post_commit, right_post_commit) =
                        self.flush_data(barrier.epoch).await?;

                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(actor_id);

                    // We don't include the time post yielding barrier because the vnode update
                    // is a one-off and rare operation.
                    barrier_join_match_duration_ns
                        .inc_by(barrier_start_time.elapsed().as_nanos() as u64);
                    yield Message::Barrier(barrier);

                    // Update the vnode bitmap for state tables of both sides if asked.
                    right_post_commit
                        .post_yield_barrier(update_vnode_bitmap.clone())
                        .await?;
                    if left_post_commit
                        .post_yield_barrier(update_vnode_bitmap)
                        .await?
                        .unwrap_or(false)
                    {
                        self.watermark_buffers
                            .values_mut()
                            .for_each(|buffers| buffers.clear());
                        self.inequality_watermarks.fill(None);
                    }

                    // Report metrics of cached join rows/entries
                    for (join_cached_entry_count, ht) in [
                        (&left_join_cached_entry_count, &self.side_l.ht),
                        (&right_join_cached_entry_count, &self.side_r.ht),
                    ] {
                        join_cached_entry_count.set(ht.entry_count() as i64);
                    }
                }
            }
            start_time = Instant::now();
        }
    }

    async fn flush_data(
        &mut self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<(
        JoinHashMapPostCommit<'_, K, S, E>,
        JoinHashMapPostCommit<'_, K, S, E>,
    )> {
        // All changes to the state has been buffered in the mem-table of the state table. Just
        // `commit` them here.
        let left = self.side_l.ht.flush(epoch).await?;
        let right = self.side_r.ht.flush(epoch).await?;
        Ok((left, right))
    }

    async fn try_flush_data(&mut self) -> StreamExecutorResult<()> {
        // All changes to the state has been buffered in the mem-table of the state table. Just
        // `commit` them here.
        self.side_l.ht.try_flush().await?;
        self.side_r.ht.try_flush().await?;
        Ok(())
    }

    // We need to manually evict the cache.
    fn evict_cache(
        side_update: &mut JoinSide<K, S, E>,
        side_match: &mut JoinSide<K, S, E>,
        cnt_rows_received: &mut u32,
        join_cache_evict_interval_rows: u32,
    ) {
        *cnt_rows_received += 1;
        if *cnt_rows_received >= join_cache_evict_interval_rows {
            side_update.ht.evict();
            side_match.ht.evict();
            *cnt_rows_received = 0;
        }
    }

    fn handle_watermark(
        &mut self,
        side: SideTypePrimitive,
        watermark: Watermark,
    ) -> StreamExecutorResult<Vec<Watermark>> {
        let (side_update, side_match) = if side == SideType::Left {
            (&mut self.side_l, &mut self.side_r)
        } else {
            (&mut self.side_r, &mut self.side_l)
        };

        // Process join-key watermarks
        let wm_in_jk = side_update
            .join_key_indices
            .iter()
            .positions(|idx| *idx == watermark.col_idx);
        let mut watermarks_to_emit = vec![];
        for idx in wm_in_jk {
            let buffers = self
                .watermark_buffers
                .entry(idx)
                .or_insert_with(|| BufferedWatermarks::with_ids([SideType::Left, SideType::Right]));
            if let Some(selected_watermark) = buffers.handle_watermark(side, watermark.clone()) {
                if self
                    .watermark_indices_in_jk
                    .iter()
                    .any(|(jk_pos, do_clean)| *jk_pos == idx && *do_clean)
                {
                    side_match
                        .ht
                        .update_watermark(selected_watermark.val.clone());
                    side_update
                        .ht
                        .update_watermark(selected_watermark.val.clone());
                }

                let empty_indices = vec![];
                let output_indices = side_update
                    .i2o_mapping_indexed
                    .get_vec(&side_update.join_key_indices[idx])
                    .unwrap_or(&empty_indices)
                    .iter()
                    .chain(
                        side_match
                            .i2o_mapping_indexed
                            .get_vec(&side_match.join_key_indices[idx])
                            .unwrap_or(&empty_indices),
                    );
                for output_idx in output_indices {
                    watermarks_to_emit.push(selected_watermark.clone().with_idx(*output_idx));
                }
            };
        }

        // Process inequality watermarks
        // We can only yield the LARGER side's watermark downstream.
        // For `left >= right`: left is larger, emit for left output columns
        // For `left <= right`: right is larger, emit for right output columns
        let mut update_left_watermark = None;
        let mut update_right_watermark = None;
        if let Some(watermark_indices) = side_update.input2inequality_index.get(watermark.col_idx) {
            for (inequality_index, _) in watermark_indices {
                let buffers = self
                    .watermark_buffers
                    .entry(side_update.join_key_indices.len() + inequality_index)
                    .or_insert_with(|| {
                        BufferedWatermarks::with_ids([SideType::Left, SideType::Right])
                    });
                if let Some(selected_watermark) = buffers.handle_watermark(side, watermark.clone())
                {
                    let (output_indices, pair_info) = &self.inequality_pairs[*inequality_index];
                    let left_is_larger = pair_info.left_side_is_larger();

                    // Emit watermark for the larger side's output columns only
                    for output_idx in output_indices {
                        watermarks_to_emit.push(selected_watermark.clone().with_idx(*output_idx));
                    }
                    // Store watermark for state cleaning (via useful_state_clean_columns)
                    self.inequality_watermarks[*inequality_index] =
                        Some(selected_watermark.clone());

                    // Mark which side needs state cleaning (only if the clean flag is set)
                    if left_is_larger && pair_info.clean_left_state {
                        update_left_watermark = Some(selected_watermark.val.clone());
                    } else if !left_is_larger && pair_info.clean_right_state {
                        update_right_watermark = Some(selected_watermark.val.clone());
                    }
                }
            }
            // Do state cleaning on the larger side.
            // Now that degree tables include the inequality column, we can clean both
            // state and degree tables using `update_watermark`.
            if let Some(val) = update_left_watermark {
                self.side_l.ht.update_watermark(val);
            }
            if let Some(val) = update_right_watermark {
                self.side_r.ht.update_watermark(val);
            }
        }
        Ok(watermarks_to_emit)
    }

    fn row_concat(
        row_update: impl Row,
        update_start_pos: usize,
        row_matched: impl Row,
        matched_start_pos: usize,
    ) -> OwnedRow {
        let mut new_row = vec![None; row_update.len() + row_matched.len()];

        for (i, datum_ref) in row_update.iter().enumerate() {
            new_row[i + update_start_pos] = datum_ref.to_owned_datum();
        }
        for (i, datum_ref) in row_matched.iter().enumerate() {
            new_row[i + matched_start_pos] = datum_ref.to_owned_datum();
        }
        OwnedRow::new(new_row)
    }

    /// Used to forward `eq_join_oneside` to show join side in stack.
    fn eq_join_left(
        args: EqJoinArgs<'_, K, S, E>,
    ) -> impl Stream<Item = Result<StreamChunk, StreamExecutorError>> + '_ {
        Self::eq_join_oneside::<{ SideType::Left }>(args)
    }

    /// Used to forward `eq_join_oneside` to show join side in stack.
    fn eq_join_right(
        args: EqJoinArgs<'_, K, S, E>,
    ) -> impl Stream<Item = Result<StreamChunk, StreamExecutorError>> + '_ {
        Self::eq_join_oneside::<{ SideType::Right }>(args)
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn eq_join_oneside<const SIDE: SideTypePrimitive>(args: EqJoinArgs<'_, K, S, E>) {
        let EqJoinArgs {
            ctx,
            side_l,
            side_r,
            actual_output_data_types,
            cond,
            inequality_watermarks,
            chunk,
            append_only_optimize,
            chunk_size,
            cnt_rows_received,
            high_join_amplification_threshold,
            entry_state_max_rows,
            join_cache_evict_interval_rows,
            join_matched_join_keys,
            ..
        } = args;

        let (side_update, side_match) = if SIDE == SideType::Left {
            (side_l, side_r)
        } else {
            (side_r, side_l)
        };

        let useful_state_clean_columns = side_match
            .state_clean_columns
            .iter()
            .filter_map(|(column_idx, inequality_index)| {
                inequality_watermarks[*inequality_index]
                    .as_ref()
                    .map(|watermark| (*column_idx, watermark))
            })
            .collect_vec();

        let mut hashjoin_chunk_builder =
            JoinChunkBuilder::<T, SIDE>::new(JoinStreamChunkBuilder::new(
                chunk_size,
                actual_output_data_types.to_vec(),
                side_update.i2o_mapping.clone(),
                side_match.i2o_mapping.clone(),
            ));

        let keys = K::build_many(&side_update.join_key_indices, chunk.data_chunk());
        for (r, key) in chunk.rows_with_holes().zip_eq_debug(keys.iter()) {
            let Some((op, row)) = r else {
                continue;
            };
            Self::evict_cache(
                side_update,
                side_match,
                cnt_rows_received,
                join_cache_evict_interval_rows,
            );

            let cache_lookup_result = {
                let probe_non_null_requirement_satisfied = side_update
                    .non_null_fields
                    .iter()
                    .all(|column_idx| unsafe { row.datum_at_unchecked(*column_idx).is_some() });
                let build_non_null_requirement_satisfied =
                    key.null_bitmap().is_subset(side_match.ht.null_matched());
                if probe_non_null_requirement_satisfied && build_non_null_requirement_satisfied {
                    side_match.ht.take_state_opt(key)
                } else {
                    CacheResult::NeverMatch
                }
            };
            let mut total_matches = 0;

            macro_rules! match_rows {
                ($op:ident) => {
                    Self::handle_match_rows::<SIDE, { JoinOp::$op }>(
                        cache_lookup_result,
                        row,
                        key,
                        &mut hashjoin_chunk_builder,
                        side_match,
                        side_update,
                        &useful_state_clean_columns,
                        cond,
                        &mut total_matches,
                        append_only_optimize,
                        entry_state_max_rows,
                    )
                };
            }

            match op {
                Op::Insert | Op::UpdateInsert =>
                {
                    #[for_await]
                    for chunk in match_rows!(Insert) {
                        let chunk = chunk?;
                        yield chunk;
                    }
                }
                Op::Delete | Op::UpdateDelete =>
                {
                    #[for_await]
                    for chunk in match_rows!(Delete) {
                        let chunk = chunk?;
                        yield chunk;
                    }
                }
            };

            join_matched_join_keys.observe(total_matches as _);
            if total_matches > high_join_amplification_threshold {
                let join_key_data_types = side_update.ht.join_key_data_types();
                let key = key.deserialize(join_key_data_types)?;
                tracing::warn!(target: "high_join_amplification",
                    matched_rows_len = total_matches,
                    update_table_id = %side_update.ht.table_id(),
                    match_table_id = %side_match.ht.table_id(),
                    join_key = ?key,
                    actor_id = %ctx.id,
                    fragment_id = %ctx.fragment_id,
                    "large rows matched for join key"
                );
            }
        }
        // NOTE(kwannoel): We don't track metrics for this last chunk.
        if let Some(chunk) = hashjoin_chunk_builder.take() {
            yield chunk;
        }
    }

    /// For the probe-side row, we need to check if it has values in cache, if not, we need to
    /// fetch the matched rows from the state table.
    ///
    /// Every matched build-side row being processed needs to go through the following phases:
    /// 1. Handle join condition evaluation.
    /// 2. Always do cache refill, if the state count is good.
    /// 3. Handle state cleaning.
    /// 4. Handle degree table update.
    #[expect(clippy::too_many_arguments)]
    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn handle_match_rows<
        'a,
        const SIDE: SideTypePrimitive,
        const JOIN_OP: JoinOpPrimitive,
    >(
        cached_lookup_result: CacheResult<E>,
        row: RowRef<'a>,
        key: &'a K,
        hashjoin_chunk_builder: &'a mut JoinChunkBuilder<T, SIDE>,
        side_match: &'a mut JoinSide<K, S, E>,
        side_update: &'a mut JoinSide<K, S, E>,
        useful_state_clean_columns: &'a [(usize, &'a Watermark)],
        cond: &'a mut Option<NonStrictExpression>,
        total_matches: &'a mut usize,
        append_only_optimize: bool,
        entry_state_max_rows: usize,
    ) {
        let cache_hit = matches!(cached_lookup_result, CacheResult::Hit(_));
        let mut entry_state: JoinEntryState<E> = JoinEntryState::default();
        let mut entry_state_count = 0;

        let mut degree = 0;
        let mut append_only_matched_row = None;
        let mut matched_rows_to_clean = vec![];

        macro_rules! match_row {
            (
                $match_order_key_indices:expr,
                $degree_table:expr,
                $matched_row:expr,
                $matched_row_ref:expr,
                $from_cache:literal,
                $map_output:expr,
            ) => {
                Self::handle_match_row::<_, _, SIDE, { JOIN_OP }, { $from_cache }>(
                    row,
                    $matched_row,
                    $matched_row_ref,
                    hashjoin_chunk_builder,
                    $match_order_key_indices,
                    $degree_table,
                    side_update.start_pos,
                    side_match.start_pos,
                    cond,
                    &mut degree,
                    useful_state_clean_columns,
                    total_matches,
                    append_only_optimize,
                    &mut append_only_matched_row,
                    &mut matched_rows_to_clean,
                    $map_output,
                )
            };
        }

        let entry_state = match cached_lookup_result {
            CacheResult::NeverMatch => {
                let op = match JOIN_OP {
                    JoinOp::Insert => Op::Insert,
                    JoinOp::Delete => Op::Delete,
                };
                if let Some(chunk) = hashjoin_chunk_builder.forward_if_not_matched(op, row) {
                    yield chunk;
                }
                return Ok(());
            }
            CacheResult::Hit(mut cached_rows) => {
                let (match_order_key_indices, match_degree_state) =
                    side_match.ht.get_degree_state_mut_ref();
                // Handle cached rows which match the probe-side row.
                for (matched_row_ref, matched_row) in
                    cached_rows.values_mut(&side_match.all_data_types)
                {
                    let matched_row = matched_row?;
                    if let Some(chunk) = match_row!(
                        match_order_key_indices,
                        match_degree_state,
                        matched_row,
                        Some(matched_row_ref),
                        true,
                        Either::Left,
                    )
                    .await
                    {
                        yield chunk;
                    }
                }

                cached_rows
            }
            CacheResult::Miss => {
                // Handle rows which are not in cache.
                let (matched_rows, match_order_key_indices, degree_table) = side_match
                    .ht
                    .fetch_matched_rows_and_get_degree_table_ref(key)
                    .await?;

                #[for_await]
                for matched_row in matched_rows {
                    let (encoded_pk, matched_row) = matched_row?;

                    let mut matched_row_ref = None;

                    // cache refill
                    if entry_state_count <= entry_state_max_rows {
                        let row_ref = entry_state
                            .insert(encoded_pk, E::encode(&matched_row))
                            .with_context(|| format!("row: {}", row.display(),))?;
                        matched_row_ref = Some(row_ref);
                        entry_state_count += 1;
                    }
                    if let Some(chunk) = match_row!(
                        match_order_key_indices,
                        degree_table,
                        matched_row,
                        matched_row_ref,
                        false,
                        Either::Right,
                    )
                    .await
                    {
                        yield chunk;
                    }
                }
                Box::new(entry_state)
            }
        };

        // forward rows depending on join types
        let op = match JOIN_OP {
            JoinOp::Insert => Op::Insert,
            JoinOp::Delete => Op::Delete,
        };
        if degree == 0 {
            if let Some(chunk) = hashjoin_chunk_builder.forward_if_not_matched(op, row) {
                yield chunk;
            }
        } else if let Some(chunk) = hashjoin_chunk_builder.forward_exactly_once_if_matched(op, row)
        {
            yield chunk;
        }

        // cache refill
        if cache_hit || entry_state_count <= entry_state_max_rows {
            side_match.ht.update_state(key, entry_state);
        }

        // watermark state cleaning
        for matched_row in matched_rows_to_clean {
            // The committed table watermark is responsible for reclaiming rows in the state table.
            side_match.ht.delete_row_in_mem(key, &matched_row.row)?;
        }

        // apply append_only optimization to clean matched_rows which have been persisted
        if append_only_optimize && let Some(row) = append_only_matched_row {
            assert_matches!(JOIN_OP, JoinOp::Insert);
            side_match.ht.delete_handle_degree(key, row)?;
            return Ok(());
        }

        // no append_only optimization, update state table(s).
        match JOIN_OP {
            JoinOp::Insert => {
                side_update
                    .ht
                    .insert_handle_degree(key, JoinRow::new(row, degree))?;
            }
            JoinOp::Delete => {
                side_update
                    .ht
                    .delete_handle_degree(key, JoinRow::new(row, degree))?;
            }
        }
    }

    #[expect(clippy::too_many_arguments)]
    #[inline]
    async fn handle_match_row<
        'a,
        R: Row,  // input row type
        RO: Row, // output row type
        const SIDE: SideTypePrimitive,
        const JOIN_OP: JoinOpPrimitive,
        const MATCHED_ROWS_FROM_CACHE: bool,
    >(
        update_row: RowRef<'a>,
        mut matched_row: JoinRow<R>,
        mut matched_row_cache_ref: Option<&mut E::EncodedRow>,
        hashjoin_chunk_builder: &'a mut JoinChunkBuilder<T, SIDE>,
        match_order_key_indices: &[usize],
        match_degree_table: &mut Option<TableInner<S>>,
        side_update_start_pos: usize,
        side_match_start_pos: usize,
        cond: &Option<NonStrictExpression>,
        update_row_degree: &mut u64,
        useful_state_clean_columns: &[(usize, &'a Watermark)],
        total_matches: &mut usize,
        append_only_optimize: bool,
        append_only_matched_row: &mut Option<JoinRow<RO>>,
        matched_rows_to_clean: &mut Vec<JoinRow<RO>>,
        map_output: impl Fn(R) -> RO,
    ) -> Option<StreamChunk> {
        let mut need_state_clean = false;
        let mut chunk_opt = None;
        // TODO(kwannoel): Instead of evaluating this every loop,
        // we can call this only if there's a non-equi expression.
        // check join cond
        let join_condition_satisfied = Self::check_join_condition(
            update_row,
            side_update_start_pos,
            &matched_row.row,
            side_match_start_pos,
            cond,
        )
        .await;

        if join_condition_satisfied {
            *total_matches += 1;
            // update degree
            *update_row_degree += 1;
            // send matched row downstream

            // Inserts must happen before degrees are updated,
            // FIXME(kwannoel): We should let deletes and inserts happen BEFORE degree updates.
            if matches!(JOIN_OP, JoinOp::Insert)
                && !forward_exactly_once(T, SIDE)
                && let Some(chunk) =
                    hashjoin_chunk_builder.with_match::<JOIN_OP>(&update_row, &matched_row)
            {
                chunk_opt = Some(chunk);
            }
            // TODO(kwannoel): We can actually statically decide this, using join side + join type.
            if let Some(degree_table) = match_degree_table {
                update_degree::<S, { JOIN_OP }>(
                    match_order_key_indices,
                    degree_table,
                    &mut matched_row,
                );
                if MATCHED_ROWS_FROM_CACHE || matched_row_cache_ref.is_some() {
                    // update matched row in cache
                    match JOIN_OP {
                        JoinOp::Insert => matched_row_cache_ref.as_mut().unwrap().increase_degree(),
                        JoinOp::Delete => matched_row_cache_ref.as_mut().unwrap().decrease_degree(),
                    }
                }
            }

            // Deletes must happen after degree table is updated.
            // FIXME(kwannoel): We should let deletes and inserts happen BEFORE degree updates.
            if matches!(JOIN_OP, JoinOp::Delete)
                && !forward_exactly_once(T, SIDE)
                && let Some(chunk) =
                    hashjoin_chunk_builder.with_match::<JOIN_OP>(&update_row, &matched_row)
            {
                chunk_opt = Some(chunk);
            }
        } else {
            // check if need state cleaning
            for (column_idx, watermark) in useful_state_clean_columns {
                if matched_row.row.datum_at(*column_idx).is_some_and(|scalar| {
                    scalar
                        .default_cmp(&watermark.val.as_scalar_ref_impl())
                        .is_lt()
                }) {
                    need_state_clean = true;
                    break;
                }
            }
        }
        // If the stream is append-only and the join key covers pk in both side,
        // then we can remove matched rows since pk is unique and will not be
        // inserted again
        if append_only_optimize {
            assert_matches!(JOIN_OP, JoinOp::Insert);
            // Since join key contains pk and pk is unique, there should be only
            // one row if matched.
            assert!(append_only_matched_row.is_none());
            *append_only_matched_row = Some(matched_row.map(map_output));
        } else if need_state_clean {
            debug_assert!(
                !append_only_optimize,
                "`append_only_optimize` and `need_state_clean` must not both be true"
            );
            matched_rows_to_clean.push(matched_row.map(map_output));
        }

        chunk_opt
    }

    // TODO(yuhao-su): We should find a better way to eval the expression
    // without concat two rows.
    // if there are non-equi expressions
    // NOTE(kwannoel): We can probably let `eval` use `impl Row` instead of `OwnedRow`.
    #[inline]
    async fn check_join_condition(
        row: impl Row,
        side_update_start_pos: usize,
        matched_row: impl Row,
        side_match_start_pos: usize,
        join_condition: &Option<NonStrictExpression>,
    ) -> bool {
        if let Some(join_condition) = join_condition {
            let new_row = Self::row_concat(
                row,
                side_update_start_pos,
                matched_row,
                side_match_start_pos,
            );
            join_condition
                .eval_row_infallible(&new_row)
                .await
                .map(|s| *s.as_bool())
                .unwrap_or(false)
        } else {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use pretty_assertions::assert_eq;
    use risingwave_common::array::*;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, TableId};
    use risingwave_common::config::StreamingConfig;
    use risingwave_common::hash::{Key64, Key128};
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::MemoryEncoding;
    use crate::executor::test_utils::expr::build_from_pretty;
    use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};

    async fn create_in_memory_state_table(
        mem_state: MemoryStateStore,
        data_types: &[DataType],
        order_types: &[OrderType],
        pk_indices: &[usize],
        table_id: u32,
    ) -> (StateTable<MemoryStateStore>, StateTable<MemoryStateStore>) {
        create_in_memory_state_table_with_inequality(
            mem_state,
            data_types,
            order_types,
            pk_indices,
            table_id,
            None,
        )
        .await
    }

    async fn create_in_memory_state_table_with_inequality(
        mem_state: MemoryStateStore,
        data_types: &[DataType],
        order_types: &[OrderType],
        pk_indices: &[usize],
        table_id: u32,
        degree_inequality_type: Option<DataType>,
    ) -> (StateTable<MemoryStateStore>, StateTable<MemoryStateStore>) {
        create_in_memory_state_table_with_watermark(
            mem_state,
            data_types,
            order_types,
            pk_indices,
            table_id,
            degree_inequality_type,
            vec![],
            vec![],
        )
        .await
    }

    #[expect(clippy::too_many_arguments)]
    async fn create_in_memory_state_table_with_watermark(
        mem_state: MemoryStateStore,
        data_types: &[DataType],
        order_types: &[OrderType],
        pk_indices: &[usize],
        table_id: u32,
        degree_inequality_type: Option<DataType>,
        state_clean_watermark_indices: Vec<usize>,
        degree_clean_watermark_indices: Vec<usize>,
    ) -> (StateTable<MemoryStateStore>, StateTable<MemoryStateStore>) {
        let column_descs = data_types
            .iter()
            .enumerate()
            .map(|(id, data_type)| ColumnDesc::unnamed(ColumnId::new(id as i32), data_type.clone()))
            .collect_vec();
        let mut state_table_catalog = gen_pbtable(
            TableId::new(table_id),
            column_descs,
            order_types.to_vec(),
            pk_indices.to_vec(),
            0,
        );
        state_table_catalog.clean_watermark_indices = state_clean_watermark_indices
            .into_iter()
            .map(|idx| idx as u32)
            .collect();
        let state_table =
            StateTable::from_table_catalog(&state_table_catalog, mem_state.clone(), None).await;

        // Create degree table with schema: [pk..., _degree, inequality?]
        let mut degree_table_column_descs = vec![];
        pk_indices.iter().enumerate().for_each(|(pk_id, idx)| {
            degree_table_column_descs.push(ColumnDesc::unnamed(
                ColumnId::new(pk_id as i32),
                data_types[*idx].clone(),
            ))
        });
        // Add _degree column
        degree_table_column_descs.push(ColumnDesc::unnamed(
            ColumnId::new(pk_indices.len() as i32),
            DataType::Int64,
        ));
        // Add inequality column if present
        if let Some(ineq_type) = degree_inequality_type {
            degree_table_column_descs.push(ColumnDesc::unnamed(
                ColumnId::new((pk_indices.len() + 1) as i32),
                ineq_type,
            ));
        }
        let mut degree_table_catalog = gen_pbtable(
            TableId::new(table_id + 1),
            degree_table_column_descs,
            order_types.to_vec(),
            pk_indices.to_vec(),
            0,
        );
        degree_table_catalog.clean_watermark_indices = degree_clean_watermark_indices
            .into_iter()
            .map(|idx| idx as u32)
            .collect();
        let degree_state_table =
            StateTable::from_table_catalog(&degree_table_catalog, mem_state, None).await;
        (state_table, degree_state_table)
    }

    fn create_cond(condition_text: Option<String>) -> NonStrictExpression {
        build_from_pretty(
            condition_text
                .as_deref()
                .unwrap_or("(less_than:boolean $1:int8 $3:int8)"),
        )
    }

    async fn create_executor<const T: JoinTypePrimitive>(
        with_condition: bool,
        null_safe: bool,
        condition_text: Option<String>,
        inequality_pairs: Vec<InequalityPairInfo>,
    ) -> (MessageSender, MessageSender, BoxedMessageStream) {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64), // join key
                Field::unnamed(DataType::Int64),
            ],
        };
        let (tx_l, source_l) = MockSource::channel();
        let source_l = source_l.into_executor(schema.clone(), vec![1]);
        let (tx_r, source_r) = MockSource::channel();
        let source_r = source_r.into_executor(schema, vec![1]);
        let params_l = JoinParams::new(vec![0], vec![1]);
        let params_r = JoinParams::new(vec![0], vec![1]);
        let cond = with_condition.then(|| create_cond(condition_text));

        let mem_state = MemoryStateStore::new();

        // Determine inequality column types for degree tables based on inequality_pairs
        let l_degree_ineq_type = inequality_pairs
            .iter()
            .find(|pair| pair.clean_left_state)
            .map(|_| DataType::Int64); // Column 1 is Int64
        let r_degree_ineq_type = inequality_pairs
            .iter()
            .find(|pair| pair.clean_right_state)
            .map(|_| DataType::Int64); // Column 1 is Int64
        let l_clean_watermark_indices = inequality_pairs
            .iter()
            .find(|pair| pair.clean_left_state)
            .map(|pair| vec![pair.left_idx])
            .unwrap_or_default();
        let r_clean_watermark_indices = inequality_pairs
            .iter()
            .find(|pair| pair.clean_right_state)
            .map(|pair| vec![pair.right_idx])
            .unwrap_or_default();
        let degree_inequality_column_idx = 3;
        let l_degree_clean_watermark_indices = l_degree_ineq_type
            .as_ref()
            .map(|_| vec![degree_inequality_column_idx])
            .unwrap_or_default();
        let r_degree_clean_watermark_indices = r_degree_ineq_type
            .as_ref()
            .map(|_| vec![degree_inequality_column_idx])
            .unwrap_or_default();

        let (state_l, degree_state_l) = create_in_memory_state_table_with_watermark(
            mem_state.clone(),
            &[DataType::Int64, DataType::Int64],
            &[OrderType::ascending(), OrderType::ascending()],
            &[0, 1],
            0,
            l_degree_ineq_type,
            l_clean_watermark_indices,
            l_degree_clean_watermark_indices,
        )
        .await;

        let (state_r, degree_state_r) = create_in_memory_state_table_with_watermark(
            mem_state,
            &[DataType::Int64, DataType::Int64],
            &[OrderType::ascending(), OrderType::ascending()],
            &[0, 1],
            2,
            r_degree_ineq_type,
            r_clean_watermark_indices,
            r_degree_clean_watermark_indices,
        )
        .await;

        let schema = match T {
            JoinType::LeftSemi | JoinType::LeftAnti => source_l.schema().clone(),
            JoinType::RightSemi | JoinType::RightAnti => source_r.schema().clone(),
            _ => [source_l.schema().fields(), source_r.schema().fields()]
                .concat()
                .into_iter()
                .collect(),
        };
        let schema_len = schema.len();
        let info = ExecutorInfo::for_test(schema, vec![1], "HashJoinExecutor".to_owned(), 0);

        let executor = HashJoinExecutor::<Key64, MemoryStateStore, T, MemoryEncoding>::new(
            ActorContext::for_test(123),
            info,
            source_l,
            source_r,
            params_l,
            params_r,
            vec![null_safe],
            (0..schema_len).collect_vec(),
            cond,
            inequality_pairs,
            state_l,
            degree_state_l,
            state_r,
            degree_state_r,
            Arc::new(AtomicU64::new(0)),
            false,
            Arc::new(StreamingMetrics::unused()),
            1024,
            2048,
            vec![(0, true)],
        );
        (tx_l, tx_r, executor.boxed().execute())
    }

    async fn create_classical_executor<const T: JoinTypePrimitive>(
        with_condition: bool,
        null_safe: bool,
        condition_text: Option<String>,
    ) -> (MessageSender, MessageSender, BoxedMessageStream) {
        create_executor::<T>(with_condition, null_safe, condition_text, vec![]).await
    }

    async fn create_append_only_executor<const T: JoinTypePrimitive>(
        with_condition: bool,
    ) -> (MessageSender, MessageSender, BoxedMessageStream) {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };
        let (tx_l, source_l) = MockSource::channel();
        let source_l = source_l.into_executor(schema.clone(), vec![0]);
        let (tx_r, source_r) = MockSource::channel();
        let source_r = source_r.into_executor(schema, vec![0]);
        let params_l = JoinParams::new(vec![0, 1], vec![]);
        let params_r = JoinParams::new(vec![0, 1], vec![]);
        let cond = with_condition.then(|| create_cond(None));

        let mem_state = MemoryStateStore::new();

        let (state_l, degree_state_l) = create_in_memory_state_table(
            mem_state.clone(),
            &[DataType::Int64, DataType::Int64, DataType::Int64],
            &[
                OrderType::ascending(),
                OrderType::ascending(),
                OrderType::ascending(),
            ],
            &[0, 1, 0],
            0,
        )
        .await;

        let (state_r, degree_state_r) = create_in_memory_state_table(
            mem_state,
            &[DataType::Int64, DataType::Int64, DataType::Int64],
            &[
                OrderType::ascending(),
                OrderType::ascending(),
                OrderType::ascending(),
            ],
            &[0, 1, 1],
            1,
        )
        .await;

        let schema = match T {
            JoinType::LeftSemi | JoinType::LeftAnti => source_l.schema().clone(),
            JoinType::RightSemi | JoinType::RightAnti => source_r.schema().clone(),
            _ => [source_l.schema().fields(), source_r.schema().fields()]
                .concat()
                .into_iter()
                .collect(),
        };
        let schema_len = schema.len();
        let info = ExecutorInfo::for_test(schema, vec![1], "HashJoinExecutor".to_owned(), 0);

        let executor = HashJoinExecutor::<Key128, MemoryStateStore, T, MemoryEncoding>::new(
            ActorContext::for_test(123),
            info,
            source_l,
            source_r,
            params_l,
            params_r,
            vec![false],
            (0..schema_len).collect_vec(),
            cond,
            vec![],
            state_l,
            degree_state_l,
            state_r,
            degree_state_r,
            Arc::new(AtomicU64::new(0)),
            true,
            Arc::new(StreamingMetrics::unused()),
            1024,
            2048,
            vec![(0, true)],
        );
        (tx_l, tx_r, executor.boxed().execute())
    }

    #[tokio::test]
    async fn test_inequality_join_watermark() -> StreamExecutorResult<()> {
        // Test inequality join with watermark-based state cleanup
        // Condition: left.col1 >= right.col1 (left side is larger, clean left state)
        // Left state rows with col1 < watermark will be cleaned
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 2 4
             + 2 7
             + 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 6",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I
             + 2 3",
        );
        // Test with condition: left.col1 >= right.col1
        let (mut tx_l, mut tx_r, mut hash_join) = create_executor::<{ JoinType::Inner }>(
            true,
            false,
            Some(String::from(
                "(greater_than_or_equal:boolean $1:int8 $3:int8)",
            )),
            vec![InequalityPairInfo {
                left_idx: 1,
                right_idx: 1,
                clean_left_state: true, // left >= right, left is larger
                clean_right_state: false,
                op: InequalityType::GreaterThanOrEqual,
            }],
        )
        .await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the left chunk: (2, 4), (2, 7), (3, 8)
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push watermarks: left=10, right=6
        // Output watermark is min(10, 6) = 6 for both output columns
        tx_l.push_watermark(1, DataType::Int64, ScalarImpl::Int64(10));
        hash_join.next_unwrap_pending();

        tx_r.push_watermark(1, DataType::Int64, ScalarImpl::Int64(6));
        let output_watermark = hash_join.next_unwrap_ready_watermark()?;
        assert_eq!(
            output_watermark,
            Watermark::new(1, DataType::Int64, ScalarImpl::Int64(6))
        );

        // After watermark, left state rows with col1 < 6 are cleaned
        // Row (2, 4) should be cleaned (4 < 6)
        // Row (2, 7) should remain (7 >= 6)
        // Row (3, 8) should remain (8 >= 6)

        // push right chunk (2, 6)
        // Only (2, 7) can match: 7 >= 6 is TRUE
        // (2, 4) was cleaned, so it won't match
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 2 7 2 6"
            )
        );

        // push right chunk (2, 3)
        // (2, 7) can match: 7 >= 3 is TRUE
        // (2, 4) was cleaned, won't match
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 2 7 2 3"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_inner_join() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 3 10
             + 6 11",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::Inner }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        hash_join.next_unwrap_pending();

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 2 5 2 7"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 3 6 3 10"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_null_safe_hash_inner_join() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + . 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + . 8
             - . 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + . 10
             + 6 11",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::Inner }>(false, true, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        hash_join.next_unwrap_pending();

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 2 5 2 7"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + . 6 . 10"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_left_semi_join() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 3 10
             + 6 11",
        );
        let chunk_l3 = StreamChunk::from_pretty(
            "  I I
             + 6 10",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I  I
             - 6 11",
        );
        let chunk_r4 = StreamChunk::from_pretty(
            "  I  I
             - 6 9",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::LeftSemi }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        hash_join.next_unwrap_pending();

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 2 5"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 3 6"
            )
        );

        // push the 3rd left chunk (tests forward_exactly_once)
        tx_l.push_chunk(chunk_l3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 6 10"
            )
        );

        // push the 3rd right chunk
        // (tests that no change if there are still matches)
        tx_r.push_chunk(chunk_r3);
        hash_join.next_unwrap_pending();

        // push the 3rd left chunk
        // (tests that deletion occurs when there are no more matches)
        tx_r.push_chunk(chunk_r4);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                - 6 10"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_null_safe_hash_left_semi_join() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + . 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + . 8
             - . 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + . 10
             + 6 11",
        );
        let chunk_l3 = StreamChunk::from_pretty(
            "  I I
             + 6 10",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I  I
             - 6 11",
        );
        let chunk_r4 = StreamChunk::from_pretty(
            "  I  I
             - 6 9",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::LeftSemi }>(false, true, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        hash_join.next_unwrap_pending();

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 2 5"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + . 6"
            )
        );

        // push the 3rd left chunk (tests forward_exactly_once)
        tx_l.push_chunk(chunk_l3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 6 10"
            )
        );

        // push the 3rd right chunk
        // (tests that no change if there are still matches)
        tx_r.push_chunk(chunk_r3);
        hash_join.next_unwrap_pending();

        // push the 3rd left chunk
        // (tests that deletion occurs when there are no more matches)
        tx_r.push_chunk(chunk_r4);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                - 6 10"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_inner_join_append_only() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 1
             + 2 5 2
             + 3 6 3",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I I
             + 4 9 4
             + 5 10 5",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I I
             + 2 5 1
             + 4 9 2
             + 6 9 3",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 4
             + 3 6 5",
        );

        let (mut tx_l, mut tx_r, mut hash_join) =
            create_append_only_executor::<{ JoinType::Inner }>(false).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        hash_join.next_unwrap_pending();

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                + 2 5 2 2 5 1
                + 4 9 4 4 9 2"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                + 1 4 1 1 4 4
                + 3 6 3 3 6 5"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_left_semi_join_append_only() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 1
             + 2 5 2
             + 3 6 3",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I I
             + 4 9 4
             + 5 10 5",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I I
             + 2 5 1
             + 4 9 2
             + 6 9 3",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 4
             + 3 6 5",
        );

        let (mut tx_l, mut tx_r, mut hash_join) =
            create_append_only_executor::<{ JoinType::LeftSemi }>(false).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        hash_join.next_unwrap_pending();

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I
                + 2 5 2
                + 4 9 4"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I
                + 1 4 1
                + 3 6 3"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_right_semi_join_append_only() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 1
             + 2 5 2
             + 3 6 3",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I I
             + 4 9 4
             + 5 10 5",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I I
             + 2 5 1
             + 4 9 2
             + 6 9 3",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 4
             + 3 6 5",
        );

        let (mut tx_l, mut tx_r, mut hash_join) =
            create_append_only_executor::<{ JoinType::RightSemi }>(false).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        hash_join.next_unwrap_pending();

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I
                + 2 5 1
                + 4 9 2"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I
                + 1 4 4
                + 3 6 5"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_right_semi_join() -> StreamExecutorResult<()> {
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I  I
             + 3 10
             + 6 11",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I I
             + 6 10",
        );
        let chunk_l3 = StreamChunk::from_pretty(
            "  I  I
             - 6 11",
        );
        let chunk_l4 = StreamChunk::from_pretty(
            "  I  I
             - 6 9",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::RightSemi }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        hash_join.next_unwrap_pending();

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        hash_join.next_unwrap_pending();

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 2 5"
            )
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 3 6"
            )
        );

        // push the 3rd right chunk (tests forward_exactly_once)
        tx_r.push_chunk(chunk_r3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 6 10"
            )
        );

        // push the 3rd left chunk
        // (tests that no change if there are still matches)
        tx_l.push_chunk(chunk_l3);
        hash_join.next_unwrap_pending();

        // push the 3rd right chunk
        // (tests that deletion occurs when there are no more matches)
        tx_l.push_chunk(chunk_l4);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                - 6 10"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_left_anti_join() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 3 10
             + 6 11
             + 1 2
             + 1 3",
        );
        let chunk_l3 = StreamChunk::from_pretty(
            "  I I
             + 9 10",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I I
             - 1 2",
        );
        let chunk_r4 = StreamChunk::from_pretty(
            "  I I
             - 1 3",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::LeftAnti }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 1 4
                + 2 5
                + 3 6",
            )
        );

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I
                 + 3 8 D
                 - 3 8 D",
            )
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                - 2 5"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                - 3 6
                - 1 4"
            )
        );

        // push the 3rd left chunk (tests forward_exactly_once)
        tx_l.push_chunk(chunk_l3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 9 10"
            )
        );

        // push the 3rd right chunk
        // (tests that no change if there are still matches)
        tx_r.push_chunk(chunk_r3);
        hash_join.next_unwrap_pending();

        // push the 4th right chunk
        // (tests that insertion occurs when there are no more matches)
        tx_r.push_chunk(chunk_r4);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 1 4"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_right_anti_join() -> StreamExecutorResult<()> {
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I  I
             + 3 10
             + 6 11
             + 1 2
             + 1 3",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I I
             + 9 10",
        );
        let chunk_l3 = StreamChunk::from_pretty(
            "  I I
             - 1 2",
        );
        let chunk_l4 = StreamChunk::from_pretty(
            "  I I
             - 1 3",
        );
        let (mut tx_r, mut tx_l, mut hash_join) =
            create_classical_executor::<{ JoinType::LeftAnti }>(false, false, None).await;

        // push the init barrier for left and right
        tx_r.push_barrier(test_epoch(1), false);
        tx_l.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 1 4
                + 2 5
                + 3 6",
            )
        );

        // push the init barrier for left and right
        tx_r.push_barrier(test_epoch(2), false);
        tx_l.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I
                 + 3 8 D
                 - 3 8 D",
            )
        );

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                - 2 5"
            )
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                - 3 6
                - 1 4"
            )
        );

        // push the 3rd right chunk (tests forward_exactly_once)
        tx_r.push_chunk(chunk_r3);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 9 10"
            )
        );

        // push the 3rd left chunk
        // (tests that no change if there are still matches)
        tx_l.push_chunk(chunk_l3);
        hash_join.next_unwrap_pending();

        // push the 4th left chunk
        // (tests that insertion occurs when there are no more matches)
        tx_l.push_chunk(chunk_l4);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 1 4"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_inner_join_with_barrier() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 6 8
             + 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 3 10
             + 6 11",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::Inner }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push a barrier to left side
        tx_l.push_barrier(test_epoch(2), false);

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);

        // join the first right chunk
        tx_r.push_chunk(chunk_r1);

        // Consume stream chunk
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 2 5 2 7"
            )
        );

        // push a barrier to right side
        tx_r.push_barrier(test_epoch(2), false);

        // get the aligned barrier here
        let expected_epoch = EpochPair::new_test_epoch(test_epoch(2));
        assert!(matches!(
            hash_join.next_unwrap_ready_barrier()?,
            Barrier {
                epoch,
                mutation: None,
                ..
            } if epoch == expected_epoch
        ));

        // join the 2nd left chunk
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 6 8 6 9"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 3 6 3 10
                + 3 8 3 10
                + 6 8 6 11"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_inner_join_with_null_and_barrier() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 .
             + 3 .",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 6 .
             + 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 3 10
             + 6 11",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::Inner }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push a barrier to left side
        tx_l.push_barrier(test_epoch(2), false);

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);

        // join the first right chunk
        tx_r.push_chunk(chunk_r1);

        // Consume stream chunk
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 2 . 2 7"
            )
        );

        // push a barrier to right side
        tx_r.push_barrier(test_epoch(2), false);

        // get the aligned barrier here
        let expected_epoch = EpochPair::new_test_epoch(test_epoch(2));
        assert!(matches!(
            hash_join.next_unwrap_ready_barrier()?,
            Barrier {
                epoch,
                mutation: None,
                ..
            } if epoch == expected_epoch
        ));

        // join the 2nd left chunk
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 6 . 6 9"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 3 8 3 10
                + 3 . 3 10
                + 6 . 6 11"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_left_join() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 3 10
             + 6 11",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::LeftOuter }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 1 4 . .
                + 2 5 . .
                + 3 6 . ."
            )
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 3 8 . . D
                - 3 8 . . D"
            )
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                - 2 5 . .
                + 2 5 2 7"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                - 3 6 . .
                + 3 6 3 10"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_null_safe_hash_left_join() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + . 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + . 8
             - . 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + . 10
             + 6 11",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::LeftOuter }>(false, true, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 1 4 . .
                + 2 5 . .
                + . 6 . ."
            )
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + . 8 . . D
                - . 8 . . D"
            )
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                - 2 5 . .
                + 2 5 2 7"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                - . 6 . .
                + . 6 . 10"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_right_join() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 5 10
             - 5 10",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::RightOuter }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        hash_join.next_unwrap_pending();

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 2 5 2 7
                + . . 4 8
                + . . 6 9"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + . . 5 10 D
                - . . 5 10 D"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_left_join_append_only() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 1
             + 2 5 2
             + 3 6 3",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I I
             + 4 9 4
             + 5 10 5",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I I
             + 2 5 1
             + 4 9 2
             + 6 9 3",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 4
             + 3 6 5",
        );

        let (mut tx_l, mut tx_r, mut hash_join) =
            create_append_only_executor::<{ JoinType::LeftOuter }>(false).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                + 1 4 1 . . .
                + 2 5 2 . . .
                + 3 6 3 . . ."
            )
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                + 4 9 4 . . .
                + 5 10 5 . . ."
            )
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                - 2 5 2 . . .
                + 2 5 2 2 5 1
                - 4 9 4 . . .
                + 4 9 4 4 9 2"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I
                - 1 4 1 . . .
                + 1 4 1 1 4 4
                - 3 6 3 . . .
                + 3 6 3 3 6 5"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_right_join_append_only() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 1
             + 2 5 2
             + 3 6 3",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I I
             + 4 9 4
             + 5 10 5",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I I
             + 2 5 1
             + 4 9 2
             + 6 9 3",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I I I
             + 1 4 4
             + 3 6 5
             + 7 7 6",
        );

        let (mut tx_l, mut tx_r, mut hash_join) =
            create_append_only_executor::<{ JoinType::RightOuter }>(false).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        hash_join.next_unwrap_pending();

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I I I I I
                + 2 5 2 2 5 1
                + 4 9 4 4 9 2
                + . . . 6 9 3"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I I I I I
                + 1 4 1 1 4 4
                + 3 6 3 3 6 5
                + . . . 7 7 6"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_full_outer_join() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 5 10
             - 5 10",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::FullOuter }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 1 4 . .
                + 2 5 . .
                + 3 6 . ."
            )
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 3 8 . . D
                - 3 8 . . D"
            )
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                - 2 5 . .
                + 2 5 2 7
                + . . 4 8
                + . . 6 9"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + . . 5 10 D
                - . . 5 10 D"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_full_outer_join_update() -> StreamExecutorResult<()> {
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::FullOuter }>(false, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        tx_l.push_chunk(StreamChunk::from_pretty(
            "  I I
             + 1 1
            ",
        ));
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 1 1 . ."
            )
        );

        tx_r.push_chunk(StreamChunk::from_pretty(
            "  I I
             + 1 1
            ",
        ));
        let chunk = hash_join.next_unwrap_ready_chunk()?;

        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                - 1 1 . .
                + 1 1 1 1"
            )
        );

        tx_l.push_chunk(StreamChunk::from_pretty(
            "   I I
              - 1 1
              + 1 2
            ",
        ));
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        let chunk = chunk.compact_vis();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                - 1 1 1 1
                + 1 2 1 1
                "
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_full_outer_join_with_nonequi_condition() -> StreamExecutorResult<()>
    {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6
             + 3 7",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8
             - 1 4", // delete row to cause an empty JoinHashEntry
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 6
             + 4 8
             + 3 4",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 5 10
             - 5 10
             + 1 2",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::FullOuter }>(true, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 1 4 . .
                + 2 5 . .
                + 3 6 . .
                + 3 7 . ."
            )
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 3 8 . . D
                - 3 8 . . D
                - 1 4 . ."
            )
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                - 2 5 . .
                + 2 5 2 6
                + . . 4 8
                + . . 3 4" /* regression test (#2420): 3 4 should be forwarded only once
                            * despite matching on eq join on 2
                            * entries */
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + . . 5 10 D
                - . . 5 10 D
                + . . 1 2" /* regression test (#2420): 1 2 forwarded even if matches on an empty
                            * join entry */
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_inner_join_with_nonequi_condition() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 10
             + 3 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 3 10
             + 6 11",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::Inner }>(true, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        hash_join.next_unwrap_pending();

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        hash_join.next_unwrap_pending();

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        hash_join.next_unwrap_pending();

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 3 6 3 10"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_hash_join_watermark() -> StreamExecutorResult<()> {
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_classical_executor::<{ JoinType::Inner }>(true, false, None).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        tx_l.push_int64_watermark(0, 100);

        tx_l.push_int64_watermark(0, 200);

        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
        hash_join.next_unwrap_ready_barrier()?;

        tx_r.push_int64_watermark(0, 50);

        let w1 = hash_join.next().await.unwrap().unwrap();
        let w1 = w1.as_watermark().unwrap();

        let w2 = hash_join.next().await.unwrap().unwrap();
        let w2 = w2.as_watermark().unwrap();

        tx_r.push_int64_watermark(0, 100);

        let w3 = hash_join.next().await.unwrap().unwrap();
        let w3 = w3.as_watermark().unwrap();

        let w4 = hash_join.next().await.unwrap().unwrap();
        let w4 = w4.as_watermark().unwrap();

        assert_eq!(
            w1,
            &Watermark {
                col_idx: 2,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(50)
            }
        );

        assert_eq!(
            w2,
            &Watermark {
                col_idx: 0,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(50)
            }
        );

        assert_eq!(
            w3,
            &Watermark {
                col_idx: 2,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );

        assert_eq!(
            w4,
            &Watermark {
                col_idx: 0,
                data_type: DataType::Int64,
                val: ScalarImpl::Int64(100)
            }
        );

        Ok(())
    }

    async fn create_executor_with_evict_interval<const T: JoinTypePrimitive>(
        evict_interval: u32,
    ) -> (MessageSender, MessageSender, BoxedMessageStream) {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64), // join key
                Field::unnamed(DataType::Int64),
            ],
        };
        let (tx_l, source_l) = MockSource::channel();
        let source_l = source_l.into_executor(schema.clone(), vec![1]);
        let (tx_r, source_r) = MockSource::channel();
        let source_r = source_r.into_executor(schema, vec![1]);
        let params_l = JoinParams::new(vec![0], vec![1]);
        let params_r = JoinParams::new(vec![0], vec![1]);

        let mem_state = MemoryStateStore::new();

        let (state_l, degree_state_l) = create_in_memory_state_table(
            mem_state.clone(),
            &[DataType::Int64, DataType::Int64],
            &[OrderType::ascending(), OrderType::ascending()],
            &[0, 1],
            0,
        )
        .await;

        let (state_r, degree_state_r) = create_in_memory_state_table(
            mem_state,
            &[DataType::Int64, DataType::Int64],
            &[OrderType::ascending(), OrderType::ascending()],
            &[0, 1],
            2,
        )
        .await;

        let schema = match T {
            JoinType::LeftSemi | JoinType::LeftAnti => source_l.schema().clone(),
            JoinType::RightSemi | JoinType::RightAnti => source_r.schema().clone(),
            _ => [source_l.schema().fields(), source_r.schema().fields()]
                .concat()
                .into_iter()
                .collect(),
        };
        let schema_len = schema.len();
        let info = ExecutorInfo::for_test(schema, vec![1], "HashJoinExecutor".to_owned(), 0);

        let mut streaming_config = StreamingConfig::default();
        streaming_config.developer.join_hash_map_evict_interval_rows = evict_interval;

        let executor = HashJoinExecutor::<Key64, MemoryStateStore, T, MemoryEncoding>::new(
            ActorContext::for_test_with_config(123, streaming_config),
            info,
            source_l,
            source_r,
            params_l,
            params_r,
            vec![false],
            (0..schema_len).collect_vec(),
            None,
            vec![],
            state_l,
            degree_state_l,
            state_r,
            degree_state_r,
            Arc::new(AtomicU64::new(0)),
            false,
            Arc::new(StreamingMetrics::unused()),
            1024,
            2048,
            vec![(0, true)],
        );
        (tx_l, tx_r, executor.boxed().execute())
    }

    /// Tests that a hash join executor with `join_hash_map_evict_interval_rows = 0`
    /// (periodic eviction disabled) produces correct results.
    #[tokio::test]
    async fn test_hash_join_evict_interval_disabled() -> StreamExecutorResult<()> {
        let chunk_l = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_r = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 3 8",
        );

        // 0 disables periodic eviction
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_executor_with_evict_interval::<{ JoinType::Inner }>(0).await;

        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        tx_l.push_chunk(chunk_l);
        hash_join.next_unwrap_pending();

        tx_r.push_chunk(chunk_r);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 2 5 2 7
                + 3 6 3 8"
            )
        );

        Ok(())
    }

    /// Tests that a hash join executor with `join_hash_map_evict_interval_rows = 1`
    /// (evict after every row) produces correct results.
    #[tokio::test]
    async fn test_hash_join_evict_interval_one() -> StreamExecutorResult<()> {
        let chunk_l = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_r = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 3 8",
        );

        // evict after every row
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_executor_with_evict_interval::<{ JoinType::Inner }>(1).await;

        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        tx_l.push_chunk(chunk_l);
        hash_join.next_unwrap_pending();

        tx_r.push_chunk(chunk_r);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 2 5 2 7
                + 3 6 3 8"
            )
        );

        Ok(())
    }

    /// Tests that a hash join executor with a custom `join_hash_map_evict_interval_rows`
    /// value produces correct results.
    #[tokio::test]
    async fn test_hash_join_evict_interval_custom() -> StreamExecutorResult<()> {
        let chunk_l = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6
             + 4 7
             + 5 8",
        );
        let chunk_r = StreamChunk::from_pretty(
            "  I I
             + 1 9
             + 3 10
             + 5 11",
        );

        // evict every 2 rows
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_executor_with_evict_interval::<{ JoinType::Inner }>(2).await;

        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        hash_join.next_unwrap_ready_barrier()?;

        tx_l.push_chunk(chunk_l);
        hash_join.next_unwrap_pending();

        tx_r.push_chunk(chunk_r);
        let chunk = hash_join.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I
                + 1 4 1 9
                + 3 6 3 10
                + 5 8 5 11"
            )
        );

        Ok(())
    }
}
