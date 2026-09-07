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

use std::borrow::Cow;
use std::cmp::{Ordering, max, min};
use std::collections::HashMap;
use std::ops::Bound;

use await_tree::InstrumentAwait;
use futures::Stream;
use futures::future::try_join_all;
use futures_async_stream::try_stream;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{DataChunk, Op, StreamChunk};
use risingwave_common::bail;
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, Datum, DatumRef, ScalarRefImpl};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common::util::sort_util::{OrderType, cmp_datum, cmp_datum_iter};
use risingwave_common::util::value_encoding::BasicSerde;
use risingwave_common_rate_limit::RateLimit;
use risingwave_connector::error::ConnectorError;
use risingwave_connector::source::cdc::external::{CdcOffset, CdcOffsetParseFunc};
use risingwave_storage::StateStore;
use risingwave_storage::row_serde::value_serde::ValueRowSerde;
use risingwave_storage::table::collect_data_chunk_with_builder;

use crate::common::table::state_table::{ReplicatedStateTable, StateTableInner};
use crate::executor::{Message, StreamExecutorError, StreamExecutorResult, Watermark};

/// `vnode`, `is_finished`, `row_count`, all occupy 1 column each.
pub const METADATA_STATE_LEN: usize = 3;

#[derive(Clone, Debug)]
pub struct BackfillState {
    /// Used to track backfill progress.
    // TODO: Instead of using hashmap, perhaps we can just use static array.
    inner: HashMap<VirtualNode, BackfillStatePerVnode>,
}

impl BackfillState {
    pub(crate) fn has_progress(&self) -> bool {
        self.inner.values().any(|p| {
            matches!(
                p.current_state(),
                &BackfillProgressPerVnode::InProgress { .. }
            )
        })
    }

    pub(crate) fn get_current_state(
        &mut self,
        vnode: &VirtualNode,
    ) -> &mut BackfillProgressPerVnode {
        &mut self.inner.get_mut(vnode).unwrap().current_state
    }

    // Expects the vnode to always have progress, otherwise it will return an error.
    pub(crate) fn get_progress(
        &self,
        vnode: &VirtualNode,
    ) -> StreamExecutorResult<&BackfillProgressPerVnode> {
        match self.inner.get(vnode) {
            Some(p) => Ok(p.current_state()),
            None => bail!(
                "Backfill progress for vnode {:#?} not found, backfill_state not initialized properly",
                vnode,
            ),
        }
    }

    pub(crate) fn update_progress(
        &mut self,
        vnode: VirtualNode,
        new_pos: OwnedRow,
        snapshot_row_count_delta: u64,
    ) -> StreamExecutorResult<()> {
        let state = self.get_current_state(&vnode);
        match state {
            BackfillProgressPerVnode::NotStarted => {
                *state = BackfillProgressPerVnode::InProgress {
                    current_pos: new_pos,
                    snapshot_row_count: snapshot_row_count_delta,
                };
            }
            BackfillProgressPerVnode::InProgress {
                snapshot_row_count, ..
            } => {
                *state = BackfillProgressPerVnode::InProgress {
                    current_pos: new_pos,
                    snapshot_row_count: *snapshot_row_count + snapshot_row_count_delta,
                };
            }
            BackfillProgressPerVnode::Completed { .. } => unreachable!(),
        }
        Ok(())
    }

    pub(crate) fn finish_progress(&mut self, vnode: VirtualNode, pos_len: usize) {
        let finished_placeholder_position = construct_initial_finished_state(pos_len);
        let current_state = self.get_current_state(&vnode);
        let (new_pos, snapshot_row_count) = match current_state {
            BackfillProgressPerVnode::NotStarted => (finished_placeholder_position, 0),
            BackfillProgressPerVnode::InProgress {
                current_pos,
                snapshot_row_count,
            } => (current_pos.clone(), *snapshot_row_count),
            BackfillProgressPerVnode::Completed { .. } => {
                return;
            }
        };
        *current_state = BackfillProgressPerVnode::Completed {
            current_pos: new_pos,
            snapshot_row_count,
        };
    }

    /// Return state to be committed.
    fn get_commit_state(&self, vnode: &VirtualNode) -> Option<(Option<Vec<Datum>>, Vec<Datum>)> {
        let new_state = self.inner.get(vnode).unwrap().current_state().clone();
        let new_encoded_state = match new_state {
            BackfillProgressPerVnode::NotStarted => unreachable!(),
            BackfillProgressPerVnode::InProgress {
                current_pos,
                snapshot_row_count,
            } => {
                let mut encoded_state = vec![None; current_pos.len() + METADATA_STATE_LEN];
                encoded_state[0] = Some(vnode.to_scalar().into());
                encoded_state[1..current_pos.len() + 1].clone_from_slice(current_pos.as_inner());
                encoded_state[current_pos.len() + 1] = Some(false.into());
                encoded_state[current_pos.len() + 2] = Some((snapshot_row_count as i64).into());
                encoded_state
            }
            BackfillProgressPerVnode::Completed {
                current_pos,
                snapshot_row_count,
            } => {
                let mut encoded_state = vec![None; current_pos.len() + METADATA_STATE_LEN];
                encoded_state[0] = Some(vnode.to_scalar().into());
                encoded_state[1..current_pos.len() + 1].clone_from_slice(current_pos.as_inner());
                encoded_state[current_pos.len() + 1] = Some(true.into());
                encoded_state[current_pos.len() + 2] = Some((snapshot_row_count as i64).into());
                encoded_state
            }
        };
        let old_state = self.inner.get(vnode).unwrap().committed_state().clone();
        let old_encoded_state = match old_state {
            BackfillProgressPerVnode::NotStarted => None,
            BackfillProgressPerVnode::InProgress {
                current_pos,
                snapshot_row_count,
            } => {
                let committed_pos = current_pos;
                let mut encoded_state = vec![None; committed_pos.len() + METADATA_STATE_LEN];
                encoded_state[0] = Some(vnode.to_scalar().into());
                encoded_state[1..committed_pos.len() + 1]
                    .clone_from_slice(committed_pos.as_inner());
                encoded_state[committed_pos.len() + 1] = Some(false.into());
                encoded_state[committed_pos.len() + 2] = Some((snapshot_row_count as i64).into());
                Some(encoded_state)
            }
            BackfillProgressPerVnode::Completed {
                current_pos,
                snapshot_row_count,
            } => {
                let committed_pos = current_pos;
                let mut encoded_state = vec![None; committed_pos.len() + METADATA_STATE_LEN];
                encoded_state[0] = Some(vnode.to_scalar().into());
                encoded_state[1..committed_pos.len() + 1]
                    .clone_from_slice(committed_pos.as_inner());
                encoded_state[committed_pos.len() + 1] = Some(true.into());
                encoded_state[committed_pos.len() + 2] = Some((snapshot_row_count as i64).into());
                Some(encoded_state)
            }
        };
        Some((old_encoded_state, new_encoded_state))
    }

    // TODO: We can add a committed flag to speed up this check.
    /// Checks if the state needs to be committed.
    fn need_commit(&self, vnode: &VirtualNode) -> bool {
        let state = self.inner.get(vnode).unwrap();
        match state.current_state() {
            // If current state and committed state are the same, we don't need to commit.
            s @ BackfillProgressPerVnode::InProgress { .. }
            | s @ BackfillProgressPerVnode::Completed { .. } => s != state.committed_state(),
            BackfillProgressPerVnode::NotStarted => false,
        }
    }

    fn mark_committed(&mut self, vnode: VirtualNode) {
        let BackfillStatePerVnode {
            committed_state,
            current_state,
        } = self.inner.get_mut(&vnode).unwrap();

        assert!(matches!(
            current_state,
            BackfillProgressPerVnode::InProgress { .. }
                | BackfillProgressPerVnode::Completed { .. }
        ));
        *committed_state = current_state.clone();
    }

    pub(crate) fn get_snapshot_row_count(&self) -> u64 {
        self.inner
            .values()
            .map(|p| p.get_snapshot_row_count())
            .sum()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackfillStatePerVnode {
    committed_state: BackfillProgressPerVnode,
    current_state: BackfillProgressPerVnode,
}

impl BackfillStatePerVnode {
    pub(crate) fn new(
        committed_state: BackfillProgressPerVnode,
        current_state: BackfillProgressPerVnode,
    ) -> Self {
        Self {
            committed_state,
            current_state,
        }
    }

    pub(crate) fn committed_state(&self) -> &BackfillProgressPerVnode {
        &self.committed_state
    }

    pub(crate) fn current_state(&self) -> &BackfillProgressPerVnode {
        &self.current_state
    }

    pub(crate) fn get_snapshot_row_count(&self) -> u64 {
        self.current_state().get_snapshot_row_count()
    }
}

impl From<Vec<(VirtualNode, BackfillStatePerVnode)>> for BackfillState {
    fn from(v: Vec<(VirtualNode, BackfillStatePerVnode)>) -> Self {
        Self {
            inner: v.into_iter().collect(),
        }
    }
}

/// Used for tracking backfill state per vnode
/// The `OwnedRow` only contains the pk of upstream, to track `current_pos`.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum BackfillProgressPerVnode {
    /// no entry exists for a vnode, or on initialization of the executor.
    NotStarted,
    InProgress {
        /// The current snapshot offset
        current_pos: OwnedRow,
        /// Number of snapshot records read for this vnode.
        snapshot_row_count: u64,
    },
    Completed {
        /// The current snapshot offset
        current_pos: OwnedRow,
        /// Number of snapshot records read for this vnode.
        snapshot_row_count: u64,
    },
}

impl BackfillProgressPerVnode {
    fn get_snapshot_row_count(&self) -> u64 {
        match self {
            BackfillProgressPerVnode::NotStarted => 0,
            BackfillProgressPerVnode::InProgress {
                snapshot_row_count, ..
            }
            | BackfillProgressPerVnode::Completed {
                snapshot_row_count, ..
            } => *snapshot_row_count,
        }
    }
}

pub(crate) fn mark_cdc_chunk(
    offset_parse_func: &CdcOffsetParseFunc,
    chunk: StreamChunk,
    current_pos: &OwnedRow,
    pk_in_output_indices: &[usize],
    pk_order: &[OrderType],
    pk_needs_unsigned_i64_compare: &[bool],
    last_cdc_offset: Option<CdcOffset>,
) -> StreamExecutorResult<StreamChunk> {
    let chunk = chunk.compact_vis();
    mark_cdc_chunk_inner(
        offset_parse_func,
        chunk,
        current_pos,
        last_cdc_offset,
        pk_in_output_indices,
        pk_order,
        pk_needs_unsigned_i64_compare,
    )
}

/// Compare two primary-key rows column by column, recovering the upstream unsigned order
/// for `BIGINT UNSIGNED` pk columns.
///
/// `pk_needs_unsigned_i64_compare[i]` is true only for upstream `BIGINT UNSIGNED`. Frontend
/// up-casts narrower unsigned integers so they stay non-negative in RisingWave, and unsigned
/// float/double/decimal types must keep their native comparison semantics. Only `BIGINT UNSIGNED`
/// can overflow into a negative `i64`, so we reinterpret both sides as `u64` to restore upstream
/// MySQL ordering. The `ScalarRefImpl::Int64` match below is a defensive guard for that contract.
pub(crate) fn cmp_pk_unsigned_aware<'a>(
    lhs: impl Iterator<Item = DatumRef<'a>>,
    rhs: impl Iterator<Item = DatumRef<'a>>,
    pk_order: &[OrderType],
    pk_needs_unsigned_i64_compare: &[bool],
) -> Ordering {
    for (((l, r), order), &needs_unsigned_i64_compare) in lhs
        .zip_eq_debug(rhs)
        .zip_eq_debug(pk_order.iter())
        .zip_eq_debug(pk_needs_unsigned_i64_compare.iter())
    {
        let ord = match (needs_unsigned_i64_compare, l, r) {
            (true, Some(ScalarRefImpl::Int64(a)), Some(ScalarRefImpl::Int64(b))) => {
                let ord = (a as u64).cmp(&(b as u64));
                // Apply the column's sort direction, matching `cmp_datum`. CDC backfill always
                // reads the snapshot ascending, so the descending branch is only for parity.
                if order.is_descending() {
                    ord.reverse()
                } else {
                    ord
                }
            }
            // Signed column, NULL, up-cast unsigned integer, or non-integer unsigned column:
            // the original comparison is already correct.
            _ => cmp_datum(l, r, *order),
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Mark chunk:
/// For each row of the chunk, forward it to downstream if its pk <= `current_pos` for the
/// corresponding `vnode`, otherwise ignore it.
/// We implement it by changing the visibility bitmap.
pub(crate) fn mark_chunk_ref_by_vnode<S: StateStore, SD: ValueRowSerde>(
    chunk: &StreamChunk,
    backfill_state: &BackfillState,
    pk_in_output_indices: &[usize],
    upstream_table: &ReplicatedStateTable<S, SD>,
    pk_order: &[OrderType],
) -> StreamExecutorResult<StreamChunk> {
    let chunk = chunk.clone();
    let (data, ops) = chunk.into_parts();
    let mut new_visibility = BitmapBuilder::with_capacity(ops.len());

    let mut new_ops: Cow<'_, [Op]> = Cow::Borrowed(ops.as_ref());
    let mut unmatched_update_delete = false;
    let mut visible_update_delete = false;
    for (i, (op, row)) in ops.iter().zip_eq_debug(data.rows()).enumerate() {
        let pk = row.project(pk_in_output_indices);
        let vnode = upstream_table.compute_vnode_by_pk(pk);
        let visible = match backfill_state.get_progress(&vnode)? {
            // We want to just forward the row, if the vnode has finished backfill.
            BackfillProgressPerVnode::Completed { .. } => true,
            // If not started, no need to forward.
            BackfillProgressPerVnode::NotStarted => false,
            // If in progress, we need to check row <= current_pos.
            BackfillProgressPerVnode::InProgress { current_pos, .. } => {
                cmp_datum_iter(pk.iter(), current_pos.iter(), pk_order.iter().copied()).is_le()
            }
        };
        if !visible {
            tracing::trace!(
                source = "upstream",
                state = "process_barrier",
                action = "mark_chunk",
                ?vnode,
                ?op,
                ?pk,
                ?row,
                "update_filtered",
            );
        }
        new_visibility.append(visible);

        normalize_unmatched_updates(
            &mut new_ops,
            &mut unmatched_update_delete,
            &mut visible_update_delete,
            visible,
            i,
            op,
        );
    }
    let (columns, _) = data.into_parts();
    let chunk = StreamChunk::with_visibility(new_ops, columns, new_visibility.finish());
    Ok(chunk)
}

/// We will rewrite unmatched U-/U+ into +/- ops.
/// They can be unmatched because while they will always have the same stream key,
/// their storage pk might be different. Here we use storage pk (`current_pos`) to filter them,
/// as such, a U+ might be filtered out, but their corresponding U- could be kept, and vice versa.
///
/// This hanging U-/U+ can lead to issues downstream, since we work with an assumption in the
/// system that there's never hanging U-/U+.
fn normalize_unmatched_updates(
    normalized_ops: &mut Cow<'_, [Op]>,
    unmatched_update_delete: &mut bool,
    visible_update_delete: &mut bool,
    current_visibility: bool,
    current_op_index: usize,
    current_op: &Op,
) {
    if *unmatched_update_delete {
        assert_eq!(*current_op, Op::UpdateInsert);
        let visible_update_insert = current_visibility;
        match (visible_update_delete, visible_update_insert) {
            (true, false) => {
                // Lazily clone the ops here.
                let ops = normalized_ops.to_mut();
                ops[current_op_index - 1] = Op::Delete;
            }
            (false, true) => {
                // Lazily clone the ops here.
                let ops = normalized_ops.to_mut();
                ops[current_op_index] = Op::Insert;
            }
            (true, true) | (false, false) => {}
        }
        *unmatched_update_delete = false;
    } else {
        match current_op {
            Op::UpdateDelete => {
                *unmatched_update_delete = true;
                *visible_update_delete = current_visibility;
            }
            Op::UpdateInsert => {
                unreachable!("UpdateInsert should not be present without UpdateDelete")
            }
            _ => {}
        }
    }
}

fn mark_cdc_chunk_inner(
    offset_parse_func: &CdcOffsetParseFunc,
    chunk: StreamChunk,
    current_pos: &OwnedRow,
    last_cdc_offset: Option<CdcOffset>,
    pk_in_output_indices: &[usize],
    pk_order: &[OrderType],
    pk_needs_unsigned_i64_compare: &[bool],
) -> StreamExecutorResult<StreamChunk> {
    let (data, ops) = chunk.into_parts();
    let mut new_visibility = BitmapBuilder::with_capacity(ops.len());

    // `_rw_offset` must be placed at the last column right now
    let offset_col_idx = data.dimension() - 1;
    for v in data.rows().map(|row| {
        let offset_datum = row.datum_at(offset_col_idx).unwrap();
        let event_offset = (*offset_parse_func)(offset_datum.into_utf8())?;
        let visible = {
            // filter changelog events with binlog range
            let in_binlog_range = if let Some(binlog_low) = &last_cdc_offset {
                binlog_low <= &event_offset
            } else {
                true
            };

            if in_binlog_range {
                let lhs = row.project(pk_in_output_indices);
                let rhs = current_pos;
                cmp_pk_unsigned_aware(
                    lhs.iter(),
                    rhs.iter(),
                    pk_order,
                    pk_needs_unsigned_i64_compare,
                )
                .is_le()
            } else {
                false
            }
        };
        Ok::<_, ConnectorError>(visible)
    }) {
        new_visibility.append(v?);
    }

    let (columns, _) = data.into_parts();
    Ok(StreamChunk::with_visibility(
        ops,
        columns,
        new_visibility.finish(),
    ))
}

/// Builds a new stream chunk with `output_indices`.
pub(crate) fn mapping_chunk(chunk: StreamChunk, output_indices: &[usize]) -> StreamChunk {
    let (ops, columns, visibility) = chunk.into_inner();
    let mapped_columns = output_indices.iter().map(|&i| columns[i].clone()).collect();
    StreamChunk::with_visibility(ops, mapped_columns, visibility)
}

fn mapping_watermark(watermark: Watermark, upstream_indices: &[usize]) -> Option<Watermark> {
    watermark.transform_with_indices(upstream_indices)
}

pub(crate) fn mapping_message(msg: Message, upstream_indices: &[usize]) -> Option<Message> {
    match msg {
        Message::Barrier(_) => Some(msg),
        Message::Watermark(watermark) => {
            mapping_watermark(watermark, upstream_indices).map(Message::Watermark)
        }
        Message::Chunk(chunk) => Some(Message::Chunk(mapping_chunk(chunk, upstream_indices))),
    }
}

fn same_key_columns(lhs: &[usize], rhs: &[usize]) -> bool {
    lhs.len() == rhs.len() && lhs.iter().all(|idx| rhs.contains(idx))
}

/// Rewrites upstream updates when the input stream key is not the same column
/// set as the current executor stream key.
///
/// If an upstream update keeps the input stream key unchanged but changes the
/// current executor stream key, downstream state keyed by the current stream key
/// must see it as a delete followed by an insert.
pub(super) struct UpstreamStreamKeyUpdateNormalizer {
    current_stream_key_indices: Option<Vec<usize>>,
}

impl UpstreamStreamKeyUpdateNormalizer {
    /// Creates a normalizer for chunks with the given schema.
    ///
    /// `input_stream_key_indices` are the stream-key column indices of the input
    /// executor in the incoming chunk schema.
    ///
    /// `current_stream_key_indices` are the stream-key column indices of the
    /// current executor in the same incoming chunk schema.
    ///
    /// For example, if an upstream MV has input stream key `[k]` but the current
    /// executor stream key is `[k, ts]`, then an update from
    /// `(k = 1, ts = 10)` to `(k = 1, ts = 20)` is rewritten as
    /// `Delete(k = 1, ts = 10)` plus `Insert(k = 1, ts = 20)`. If the two stream
    /// keys are the same column set, or if an update does not change current
    /// stream-key values, it is left unchanged.
    pub(super) fn new(
        input_stream_key_indices: &[usize],
        current_stream_key_indices: Vec<usize>,
    ) -> Self {
        let current_stream_key_indices =
            (!same_key_columns(input_stream_key_indices, &current_stream_key_indices))
                .then_some(current_stream_key_indices);
        Self {
            current_stream_key_indices,
        }
    }

    pub(super) fn normalize_chunk(&self, chunk: StreamChunk) -> Option<StreamChunk> {
        if let Some(current_stream_key_indices) = &self.current_stream_key_indices {
            normalize_update_chunk_by_key(chunk, current_stream_key_indices)
        } else {
            Some(chunk)
        }
    }

    pub(super) fn normalize_message(&self, msg: Message) -> Option<Message> {
        match msg {
            Message::Chunk(chunk) => self.normalize_chunk(chunk).map(Message::Chunk),
            msg => Some(msg),
        }
    }
}

fn normalize_update_chunk_by_key(chunk: StreamChunk, key_indices: &[usize]) -> Option<StreamChunk> {
    let (data_chunk, ops) = chunk.into_parts();
    let mut update_indices = vec![];
    let mut row_idx = data_chunk.next_visible_row_idx(0);
    while let Some(idx) = row_idx {
        let row = data_chunk.row_at_unchecked_vis(idx);
        match ops[idx] {
            Op::UpdateDelete => {
                let next_idx = data_chunk
                    .next_visible_row_idx(idx + 1)
                    .unwrap_or_else(|| panic!("expect a U+ after U-\nU- row: {}", row.display()));
                let next_row = data_chunk.row_at_unchecked_vis(next_idx);
                debug_assert_eq!(
                    ops[next_idx],
                    Op::UpdateInsert,
                    "expect a U+ after U-\nU- row: {}\nrow after U-: {}",
                    row.display(),
                    next_row.display()
                );
                if row.project(key_indices) != next_row.project(key_indices) {
                    update_indices.push((idx, next_idx));
                }
                row_idx = data_chunk.next_visible_row_idx(next_idx + 1);
            }
            Op::UpdateInsert => panic!("expect a U- before U+\nU+ row: {}", row.display()),
            Op::Insert | Op::Delete => {
                row_idx = data_chunk.next_visible_row_idx(idx + 1);
            }
        }
    }

    if update_indices.is_empty() {
        return Some(StreamChunk::from_parts(ops, data_chunk));
    }

    let (columns, visibility) = data_chunk.into_parts();
    let mut ops = ops.to_vec();
    for (delete_idx, insert_idx) in update_indices {
        ops[delete_idx] = Op::Delete;
        ops[insert_idx] = Op::Insert;
    }
    Some(StreamChunk::from_parts(
        ops,
        DataChunk::new(columns, visibility),
    ))
}

/// Recovers progress per vnode, so we know which to backfill.
/// See how it decodes the state with the inline comments.
pub(crate) async fn get_progress_per_vnode<S: StateStore, const IS_REPLICATED: bool>(
    state_table: &StateTableInner<S, BasicSerde, IS_REPLICATED>,
) -> StreamExecutorResult<Vec<(VirtualNode, BackfillStatePerVnode)>> {
    debug_assert!(!state_table.vnodes().is_empty());
    let vnodes = state_table.vnodes().iter_vnodes();
    let mut result = Vec::with_capacity(state_table.vnodes().len());
    // 1. Get the vnode keys, so we can get the state per vnode.
    let vnode_keys = vnodes.map(|vnode| {
        let datum: [Datum; 1] = [Some(vnode.to_scalar().into())];
        datum
    });
    let tasks = vnode_keys.map(|vnode_key| state_table.get_row(vnode_key));
    // 2. Fetch the state for each vnode.
    //    It should have the following schema, it should not contain vnode:
    //    | pk | `backfill_finished` | `row_count` |
    let state_for_vnodes = try_join_all(tasks).await?;
    for (vnode, state_for_vnode) in state_table
        .vnodes()
        .iter_vnodes()
        .zip_eq_debug(state_for_vnodes)
    {
        let backfill_progress = match state_for_vnode {
            // There's some state, means there was progress made. It's either finished / in progress.
            Some(row) => {
                // 3. Decode the `snapshot_row_count`. Decode from the back, since
                //    pk is variable length.
                let snapshot_row_count = row.as_inner().get(row.len() - 1).unwrap();
                let snapshot_row_count = (*snapshot_row_count.as_ref().unwrap().as_int64()) as u64;

                // 4. Decode the `is_finished` flag (whether backfill has finished).
                //    Decode from the back, since pk is variable length.
                let vnode_is_finished = row.as_inner().get(row.len() - 2).unwrap();
                let vnode_is_finished = vnode_is_finished.as_ref().unwrap();

                // 5. Decode the `current_pos`.
                let current_pos = row.as_inner().get(..row.len() - 2).unwrap();
                let current_pos = current_pos.into_owned_row();

                // 6. Construct the in-memory state per vnode, based on the decoded state.
                if *vnode_is_finished.as_bool() {
                    BackfillStatePerVnode::new(
                        BackfillProgressPerVnode::Completed {
                            current_pos: current_pos.clone(),
                            snapshot_row_count,
                        },
                        BackfillProgressPerVnode::Completed {
                            current_pos,
                            snapshot_row_count,
                        },
                    )
                } else {
                    BackfillStatePerVnode::new(
                        BackfillProgressPerVnode::InProgress {
                            current_pos: current_pos.clone(),
                            snapshot_row_count,
                        },
                        BackfillProgressPerVnode::InProgress {
                            current_pos,
                            snapshot_row_count,
                        },
                    )
                }
            }
            // No state, means no progress made.
            None => BackfillStatePerVnode::new(
                BackfillProgressPerVnode::NotStarted,
                BackfillProgressPerVnode::NotStarted,
            ),
        };
        result.push((vnode, backfill_progress));
    }
    assert_eq!(result.len(), state_table.vnodes().count_ones());
    Ok(result)
}

/// Update backfill pos by vnode.
pub(crate) fn update_pos_by_vnode(
    vnode: VirtualNode,
    chunk: &StreamChunk,
    pk_in_output_indices: &[usize],
    backfill_state: &mut BackfillState,
    snapshot_row_count_delta: u64,
) -> StreamExecutorResult<()> {
    let new_pos = get_new_pos(chunk, pk_in_output_indices);
    assert_eq!(new_pos.len(), pk_in_output_indices.len());
    backfill_state.update_progress(vnode, new_pos, snapshot_row_count_delta)?;
    Ok(())
}

/// Get new backfill pos from the chunk. Since chunk should have ordered rows, we can just take the
/// last row.
pub(crate) fn get_new_pos(chunk: &StreamChunk, pk_in_output_indices: &[usize]) -> OwnedRow {
    chunk
        .rows()
        .last()
        .unwrap()
        .1
        .project(pk_in_output_indices)
        .into_owned_row()
}

pub(crate) fn get_cdc_chunk_last_offset(
    offset_parse_func: &CdcOffsetParseFunc,
    chunk: &StreamChunk,
) -> StreamExecutorResult<Option<CdcOffset>> {
    let row = chunk.rows().last().unwrap().1;
    let offset_col = row.iter().last().unwrap();
    let output =
        offset_col.map(|scalar| Ok::<_, ConnectorError>((*offset_parse_func)(scalar.into_utf8()))?);
    output.transpose().map_err(|e| e.into())
}

// NOTE(kwannoel): ["None" ..] encoding should be appropriate to mark
// the case where upstream snapshot is empty.
// This is so we can persist backfill state as "finished".
// It won't be confused with another case where pk position comprised of nulls,
// because they both record that backfill is finished.
pub(crate) fn construct_initial_finished_state(pos_len: usize) -> OwnedRow {
    OwnedRow::new(vec![None; pos_len])
}

pub(crate) fn compute_bounds(
    pk_indices: &[usize],
    current_pos: Option<OwnedRow>,
) -> Option<(Bound<OwnedRow>, Bound<OwnedRow>)> {
    // `current_pos` is None means it needs to scan from the beginning, so we use Unbounded to
    // scan. Otherwise, use Excluded.
    if let Some(current_pos) = current_pos {
        // If `current_pos` is an empty row which means upstream mv contains only one row and it
        // has been consumed. The iter interface doesn't support
        // `Excluded(empty_row)` range bound, so we can simply return `None`.
        if current_pos.is_empty() {
            assert!(pk_indices.is_empty());
            return None;
        }

        Some((Bound::Excluded(current_pos), Bound::Unbounded))
    } else {
        Some((Bound::Unbounded, Bound::Unbounded))
    }
}

#[try_stream(ok = StreamChunk, error = StreamExecutorError)]
pub(crate) async fn iter_chunks<'a, S, E, R>(mut iter: S, builder: &'a mut DataChunkBuilder)
where
    StreamExecutorError: From<E>,
    R: Row,
    S: Stream<Item = Result<R, E>> + Unpin + 'a,
{
    while let Some(data_chunk) = collect_data_chunk_with_builder(&mut iter, builder)
        .instrument_await("backfill_snapshot_read")
        .await?
    {
        debug_assert!(data_chunk.cardinality() > 0);
        let ops = vec![Op::Insert; data_chunk.capacity()];
        let stream_chunk = StreamChunk::from_parts(ops, data_chunk);
        yield stream_chunk;
    }
}

/// Schema
/// | vnode | pk | `backfill_finished` | `row_count` |
/// Persists the state per vnode based on `BackfillState`.
/// We track the current committed state via `committed_progress`
/// so we know whether we need to persist the state or not.
///
/// The state is encoded as follows:
/// `NotStarted`:
/// - Not persist to store at all.
///
/// `InProgress`:
/// - Format: | vnode | pk | false | `row_count` |
/// - If change in current pos: Persist.
/// - No change in current pos: Do not persist.
///
/// Completed
/// - Format: | vnode | pk | true | `row_count` |
/// - If previous state is `InProgress` / `NotStarted`: Persist.
/// - If previous state is Completed: Do not persist.
///
/// TODO(kwannoel): we should check committed state to be all `finished` in the tests.
/// TODO(kwannoel): Instead of persisting state per vnode each time,
/// we can optimize by persisting state for a subset of vnodes which were updated.
pub(crate) async fn persist_state_per_vnode<S: StateStore, const IS_REPLICATED: bool>(
    epoch: EpochPair,
    table: &mut StateTableInner<S, BasicSerde, IS_REPLICATED>,
    backfill_state: &mut BackfillState,
    #[cfg(debug_assertions)] state_len: usize,
    vnodes: impl Iterator<Item = VirtualNode>,
) -> StreamExecutorResult<()> {
    for vnode in vnodes {
        if !backfill_state.need_commit(&vnode) {
            continue;
        }
        let (encoded_prev_state, encoded_current_state) =
            match backfill_state.get_commit_state(&vnode) {
                Some((old_state, new_state)) => (old_state, new_state),
                None => continue,
            };
        if let Some(encoded_prev_state) = encoded_prev_state {
            // There's some progress, update the state.
            #[cfg(debug_assertions)]
            {
                let pk: &[Datum; 1] = &[Some(vnode.to_scalar().into())];
                // old_row only contains the value segment.
                let old_row = table.get_row(pk).await?;
                match old_row {
                    Some(old_row) => {
                        let inner = old_row.as_inner();
                        // value segment (without vnode) should be used for comparison
                        assert_eq!(inner, &encoded_prev_state[1..]);
                        assert_ne!(inner, &encoded_current_state[1..]);
                        assert_eq!(old_row.len(), state_len - 1);
                        assert_eq!(encoded_current_state.len(), state_len);
                    }
                    None => {
                        bail!("row {:#?} not found", pk);
                    }
                }
            }
            table.write_record(Record::Update {
                old_row: &encoded_prev_state[..],
                new_row: &encoded_current_state[..],
            });
        } else {
            // No existing state, create a new entry.
            #[cfg(debug_assertions)]
            {
                let pk: &[Datum; 1] = &[Some(vnode.to_scalar().into())];
                let row = table.get_row(pk).await?;
                assert!(row.is_none(), "row {:#?}", row);
                assert_eq!(encoded_current_state.len(), state_len);
            }
            table.write_record(Record::Insert {
                new_row: &encoded_current_state[..],
            });
        }
        backfill_state.mark_committed(vnode);
    }

    table.commit_assert_no_update_vnode_bitmap(epoch).await?;
    Ok(())
}

/// Creates a data chunk builder for snapshot read.
/// If the `rate_limit` is smaller than `chunk_size`, it will take precedence.
/// This is so we can partition snapshot read into smaller chunks than chunk size.
pub fn create_builder(
    rate_limit: RateLimit,
    chunk_size: usize,
    data_types: Vec<DataType>,
) -> DataChunkBuilder {
    let batch_size = match rate_limit {
        RateLimit::Disabled | RateLimit::Pause => chunk_size,
        RateLimit::Fixed(limit) => min(limit.get() as usize, chunk_size),
    };
    // Ensure that the batch size is at least 2, to have enough space for two rows in a single update.
    let batch_size = max(2, batch_size);
    DataChunkBuilder::new(data_types, batch_size)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_normalizing_unmatched_updates() {
        let ops = vec![
            Op::UpdateDelete,
            Op::UpdateInsert,
            Op::UpdateDelete,
            Op::UpdateInsert,
        ];
        let ops: Arc<[Op]> = ops.into();

        {
            let mut new_ops: Cow<'_, [Op]> = Cow::Borrowed(ops.as_ref());
            let mut unmatched_update_delete = true;
            let mut visible_update_delete = true;
            let current_visibility = true;
            normalize_unmatched_updates(
                &mut new_ops,
                &mut unmatched_update_delete,
                &mut visible_update_delete,
                current_visibility,
                1,
                &Op::UpdateInsert,
            );
            assert_eq!(
                &new_ops[..],
                vec![
                    Op::UpdateDelete,
                    Op::UpdateInsert,
                    Op::UpdateDelete,
                    Op::UpdateInsert
                ]
            );
        }
        {
            let mut new_ops: Cow<'_, [Op]> = Cow::Borrowed(ops.as_ref());
            let mut unmatched_update_delete = true;
            let mut visible_update_delete = false;
            let current_visibility = false;
            normalize_unmatched_updates(
                &mut new_ops,
                &mut unmatched_update_delete,
                &mut visible_update_delete,
                current_visibility,
                1,
                &Op::UpdateInsert,
            );
            assert_eq!(
                &new_ops[..],
                vec![
                    Op::UpdateDelete,
                    Op::UpdateInsert,
                    Op::UpdateDelete,
                    Op::UpdateInsert
                ]
            );
        }
        {
            let mut new_ops: Cow<'_, [Op]> = Cow::Borrowed(ops.as_ref());
            let mut unmatched_update_delete = true;
            let mut visible_update_delete = true;
            let current_visibility = false;
            normalize_unmatched_updates(
                &mut new_ops,
                &mut unmatched_update_delete,
                &mut visible_update_delete,
                current_visibility,
                1,
                &Op::UpdateInsert,
            );
            assert_eq!(
                &new_ops[..],
                vec![
                    Op::Delete,
                    Op::UpdateInsert,
                    Op::UpdateDelete,
                    Op::UpdateInsert
                ]
            );
        }
        {
            let mut new_ops: Cow<'_, [Op]> = Cow::Borrowed(ops.as_ref());
            let mut unmatched_update_delete = true;
            let mut visible_update_delete = false;
            let current_visibility = true;
            normalize_unmatched_updates(
                &mut new_ops,
                &mut unmatched_update_delete,
                &mut visible_update_delete,
                current_visibility,
                1,
                &Op::UpdateInsert,
            );
            assert_eq!(
                &new_ops[..],
                vec![
                    Op::UpdateDelete,
                    Op::Insert,
                    Op::UpdateDelete,
                    Op::UpdateInsert
                ]
            );
        }
    }
}
