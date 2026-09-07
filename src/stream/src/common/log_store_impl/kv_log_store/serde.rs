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

use std::collections::HashMap;
use std::mem::{replace, take};
use std::pin::Pin;
use std::sync::Arc;

use anyhow::anyhow;
use bytes::Bytes;
use futures::stream::{FuturesUnordered, Peekable, StreamFuture};
use futures::{Stream, StreamExt, TryStreamExt, pin_mut};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::value_encoding;
use risingwave_common::util::value_encoding::{
    BasicSerde, ValueRowDeserializer, ValueRowSerializer,
};
use risingwave_connector::sink::log_store::LogStoreResult;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_hummock_sdk::key::{TableKey, next_key};
use risingwave_pb::catalog::Table;
use risingwave_storage::error::StorageResult;
use risingwave_storage::row_serde::row_serde_util::{serialize_pk, serialize_pk_with_vnode};
use risingwave_storage::row_serde::value_serde::ValueRowSerdeNew;
use risingwave_storage::store::{StateStoreIterExt, StateStoreReadIter};
use risingwave_storage::table::{SINGLETON_VNODE, compute_vnode};
use rw_futures_util::select_all;

use crate::common::log_store_impl::kv_log_store::{
    Epoch, KvLogStorePkInfo, KvLogStoreReadMetrics, ReaderTruncationOffsetType, RowOpCodeType,
    SeqId,
};

const INSERT_OP_CODE: RowOpCodeType = risingwave_common::constants::log_store::INSERT_OP_CODE;
const DELETE_OP_CODE: RowOpCodeType = 2;
const UPDATE_INSERT_OP_CODE: RowOpCodeType = 3;
const UPDATE_DELETE_OP_CODE: RowOpCodeType = 4;
const BARRIER_OP_CODE: RowOpCodeType = 5;
const CHECKPOINT_BARRIER_OP_CODE: RowOpCodeType = 6;

struct ReadInfo {
    read_size: usize,
    read_count: usize,
}

impl ReadInfo {
    fn new() -> Self {
        Self {
            read_count: 0,
            read_size: 0,
        }
    }

    fn read_one_row(&mut self, size: usize) {
        self.read_count += 1;
        self.read_size += size;
    }

    fn read_update(&mut self, size: usize) {
        self.read_count += 2;
        self.read_size += size;
    }

    fn report(&mut self, metrics: &KvLogStoreReadMetrics) {
        metrics.storage_read_size.inc_by(self.read_size as _);
        metrics.storage_read_count.inc_by(self.read_count as _);
        self.read_size = 0;
        self.read_count = 0;
    }
}

#[derive(Debug)]
enum LogStoreRowOp {
    Row {
        seq_id: SeqId,
        op: Op,
        row: OwnedRow,
    },
    Barrier {
        is_checkpoint: bool,
    },
}

#[derive(Debug, PartialEq)]
enum LogStoreOp {
    Row {
        seq_id: SeqId,
        op: Op,
        row: OwnedRow,
    },
    Update {
        seq_id: SeqId,
        old_value: OwnedRow,
        new_value: OwnedRow,
    },
    Barrier {
        is_checkpoint: bool,
    },
}

#[derive(Debug, PartialEq)]
enum AlignedLogStoreOp {
    Row {
        vnode: VirtualNode,
        seq_id: SeqId,
        op: Op,
        row: OwnedRow,
    },
    Update {
        vnode: VirtualNode,
        seq_id: SeqId,
        old_value: OwnedRow,
        new_value: OwnedRow,
    },
    Barrier {
        vnodes: Arc<Bitmap>,
        is_checkpoint: bool,
    },
}

impl From<LogStoreRowOp> for LogStoreOp {
    fn from(value: LogStoreRowOp) -> Self {
        match value {
            LogStoreRowOp::Row { seq_id, op, row } => {
                if cfg!(debug_assertions) {
                    assert_ne!(op, Op::UpdateDelete);
                    assert_ne!(op, Op::UpdateInsert);
                } else if op == Op::UpdateDelete || op == Op::UpdateInsert {
                    warn!(?op, "create LogStoreOp from single update op");
                }
                Self::Row { seq_id, op, row }
            }
            LogStoreRowOp::Barrier { is_checkpoint } => Self::Barrier { is_checkpoint },
        }
    }
}

#[derive(Clone)]
pub(crate) struct LogStoreRowSerde {
    /// Used for serializing and deserializing the primary key.
    pk_serde: OrderedRowSerde,

    /// Row deserializer with value encoding
    row_serde: BasicSerde,

    /// Serde of epoch
    epoch_serde: OrderedRowSerde,

    /// Indices of distribution key for computing vnode.
    /// Note that the index is based on the all columns of the table, instead of the output ones.
    // FIXME: revisit constructions and usages.
    dist_key_indices: Option<Vec<usize>>,

    /// Virtual nodes that the table is partitioned into.
    ///
    /// Only the rows whose vnode of the primary key is in this set will be visible to the
    /// executor. The table will also check whether the written rows
    /// conform to this partition.
    vnodes: Arc<Bitmap>,

    /// The schema of payload
    payload_schema: Vec<DataType>,

    pk_info: &'static KvLogStorePkInfo,
}

impl LogStoreRowSerde {
    pub(crate) fn new(
        table_catalog: &Table,
        vnodes: Option<Arc<Bitmap>>,
        pk_info: &'static KvLogStorePkInfo,
    ) -> Self {
        let table_columns: Vec<ColumnDesc> = table_catalog
            .columns
            .iter()
            .map(|col| col.column_desc.as_ref().unwrap().into())
            .collect();
        let predefined_column_len: usize = pk_info.predefined_column_len();
        let dist_key_indices: Vec<usize> = table_catalog
            .distribution_key
            .iter()
            .map(|dist_index| {
                let index = *dist_index as usize;
                assert!(index >= predefined_column_len);
                index
            })
            .collect();

        let input_value_indices = table_catalog
            .value_indices
            .iter()
            .map(|val| *val as usize)
            .collect_vec();

        let data_types = input_value_indices
            .iter()
            .map(|idx| table_columns[*idx].data_type.clone())
            .collect_vec();

        for (schema_data_type, (_, log_store_data_type)) in data_types
            .iter()
            .take(predefined_column_len)
            .zip_eq(pk_info.predefined_columns.iter())
        {
            assert_eq!(schema_data_type, log_store_data_type);
        }

        let payload_schema = data_types[predefined_column_len..].to_vec();

        let row_serde = BasicSerde::new(input_value_indices.into(), table_columns.into());

        let vnodes = match vnodes {
            Some(vnodes) => vnodes,
            None => Bitmap::singleton_arc().clone(),
        };

        // epoch and seq_id. The seq_id of barrier is set null, and therefore the second order type
        // is nulls last
        let pk_serde = OrderedRowSerde::new(pk_info.pk_types(), Vec::from(pk_info.pk_orderings));

        let epoch_col_idx = pk_info.epoch_column_index;
        let epoch_serde = OrderedRowSerde::new(
            vec![pk_info.predefined_columns[epoch_col_idx].1.clone()],
            vec![pk_info.pk_orderings[epoch_col_idx]],
        );

        let dist_key_indices = if dist_key_indices.is_empty() {
            if !vnodes.is_singleton() {
                warn!(
                    ?vnodes,
                    "singleton log store gets non-singleton vnode bitmap"
                );
            }
            None
        } else {
            Some(dist_key_indices)
        };

        Self {
            pk_serde,
            row_serde,
            epoch_serde,
            dist_key_indices,
            vnodes,
            payload_schema,
            pk_info,
        }
    }

    pub(crate) fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) {
        self.vnodes = vnodes;
    }

    pub(crate) fn vnodes(&self) -> &Arc<Bitmap> {
        &self.vnodes
    }

    pub(crate) fn encode_epoch(epoch: u64) -> i64 {
        risingwave_common::constants::log_store::encode_epoch(epoch)
    }

    pub(crate) fn decode_epoch(encoded_epoch: i64) -> u64 {
        risingwave_common::constants::log_store::decode_epoch(encoded_epoch)
    }
}

impl LogStoreRowSerde {
    fn compute_vnode(&self, row: impl Row) -> VirtualNode {
        if let Some(dist_key_indices) = &self.dist_key_indices {
            compute_vnode(row, dist_key_indices, &self.vnodes)
        } else {
            SINGLETON_VNODE
        }
    }

    pub(crate) fn serialize_data_row(
        &self,
        epoch: u64,
        seq_id: SeqId,
        op: Op,
        row: impl Row,
    ) -> (VirtualNode, TableKey<Bytes>, Bytes) {
        let encoded_epoch = Self::encode_epoch(epoch);
        let pk = (self.pk_info.compute_pk)(VirtualNode::ZERO, encoded_epoch, Some(seq_id));
        let op_code = match op {
            Op::Insert => INSERT_OP_CODE,
            Op::Delete => DELETE_OP_CODE,
            Op::UpdateDelete => UPDATE_DELETE_OP_CODE,
            Op::UpdateInsert => UPDATE_INSERT_OP_CODE,
        };
        let extended_row_for_vnode = (&pk).chain([Some(ScalarImpl::Int16(op_code))]).chain(&row);
        let vnode = self.compute_vnode(&extended_row_for_vnode);
        let pk = (self.pk_info.compute_pk)(vnode, encoded_epoch, Some(seq_id));
        let extended_row = (&pk).chain([Some(ScalarImpl::Int16(op_code))]).chain(&row);
        let key_bytes = serialize_pk_with_vnode(&pk, &self.pk_serde, vnode);
        let value_bytes = self.row_serde.serialize(extended_row).into();
        (vnode, key_bytes, value_bytes)
    }

    pub(crate) fn serialize_barrier(
        &self,
        epoch: u64,
        vnode: VirtualNode,
        is_checkpoint: bool,
    ) -> (TableKey<Bytes>, Bytes) {
        let pk = (self.pk_info.compute_pk)(vnode, Self::encode_epoch(epoch), None);

        let op_code = if is_checkpoint {
            CHECKPOINT_BARRIER_OP_CODE
        } else {
            BARRIER_OP_CODE
        };

        let extended_row = (&pk)
            .chain([Some(ScalarImpl::Int16(op_code))])
            .chain(OwnedRow::new(vec![None; self.payload_schema.len()]));
        let key_bytes = serialize_pk_with_vnode(&pk, &self.pk_serde, vnode);
        let value_bytes = self.row_serde.serialize(extended_row).into();
        (key_bytes, value_bytes)
    }

    pub(crate) fn serialize_pk_epoch_prefix(&self, epoch: u64) -> Bytes {
        serialize_pk(
            [Some(ScalarImpl::Int64(Self::encode_epoch(epoch)))],
            &self.epoch_serde,
        )
    }

    pub(crate) fn serialize_log_store_pk(
        &self,
        vnode: VirtualNode,
        epoch: u64,
        seq_id: Option<SeqId>,
    ) -> TableKey<Bytes> {
        serialize_pk_with_vnode(
            (self.pk_info.compute_pk)(vnode, Self::encode_epoch(epoch), seq_id),
            &self.pk_serde,
            vnode,
        )
    }

    pub(crate) fn serialize_truncation_offset_watermark(
        &self,
        offset: ReaderTruncationOffsetType,
    ) -> Bytes {
        let (epoch, seq_id) = offset;
        Bytes::from(next_key(&serialize_pk(
            (self.pk_info.compute_pk)(
                VirtualNode::MAX_REPRESENTABLE,
                Self::encode_epoch(epoch),
                seq_id,
            ),
            &self.pk_serde,
        )))
    }
}

impl LogStoreRowSerde {
    fn deserialize(&self, value_bytes: &[u8]) -> value_encoding::Result<(Epoch, LogStoreRowOp)> {
        let row_data = self.row_serde.deserialize(value_bytes)?;

        let payload_row = OwnedRow::new(row_data[self.pk_info.predefined_column_len()..].to_vec());
        let epoch = Self::decode_epoch(
            *row_data[self.pk_info.epoch_column_index]
                .as_ref()
                .unwrap()
                .as_int64(),
        );

        let seq_id = row_data[self.pk_info.seq_id_column_index]
            .as_ref()
            .map(|i| *i.as_int32());

        let row_op_code = *row_data[self.pk_info.row_op_column_index]
            .as_ref()
            .unwrap()
            .as_int16();

        let op = match row_op_code {
            INSERT_OP_CODE => LogStoreRowOp::Row {
                seq_id: seq_id.expect("seq_id should be present for all rows"),
                op: Op::Insert,
                row: payload_row,
            },
            DELETE_OP_CODE => LogStoreRowOp::Row {
                seq_id: seq_id.expect("seq_id should be present for all rows"),
                op: Op::Delete,
                row: payload_row,
            },
            UPDATE_INSERT_OP_CODE => LogStoreRowOp::Row {
                seq_id: seq_id.expect("seq_id should be present for all rows"),
                op: Op::UpdateInsert,
                row: payload_row,
            },
            UPDATE_DELETE_OP_CODE => LogStoreRowOp::Row {
                seq_id: seq_id.expect("seq_id should be present for all rows"),
                op: Op::UpdateDelete,
                row: payload_row,
            },
            BARRIER_OP_CODE => {
                assert!(row_data[self.pk_info.seq_id_column_index].is_none());
                LogStoreRowOp::Barrier {
                    is_checkpoint: false,
                }
            }
            CHECKPOINT_BARRIER_OP_CODE => {
                assert!(row_data[self.pk_info.seq_id_column_index].is_none());
                LogStoreRowOp::Barrier {
                    is_checkpoint: true,
                }
            }
            _ => unreachable!("invalid row op code: {}", row_op_code),
        };
        Ok((epoch, op))
    }

    pub(crate) async fn deserialize_stream_chunk<I: StateStoreReadIter>(
        &self,
        iters: impl IntoIterator<Item = (VirtualNode, I)>,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        expected_epoch: u64,
        metrics: &KvLogStoreReadMetrics,
    ) -> LogStoreResult<StreamChunk> {
        let size_bound = (end_seq_id - start_seq_id + 1) as usize;
        let mut data_chunk_builder =
            DataChunkBuilder::new(self.payload_schema.clone(), size_bound + 1);
        let mut ops = Vec::with_capacity(size_bound);
        let mut read_info = ReadInfo::new();
        let stream = select_all(
            iters
                .into_iter()
                .map(|(vnode, iter)| deserialize_stream(vnode, iter, self.clone())),
        );
        pin_mut!(stream);
        while let Some(row) = stream.try_next().await? {
            let epoch = row.row_meta.epoch;
            let op = row.op;
            let row_size = row.row_meta.size;
            match (epoch, op) {
                (epoch, LogStoreOp::Row { op, row, .. }) => {
                    if epoch != expected_epoch {
                        return Err(anyhow!(
                            "decoded epoch {} not match expected epoch {}",
                            epoch,
                            expected_epoch
                        ));
                    }
                    read_info.read_one_row(row_size);
                    ops.push(op);
                    if ops.len() > size_bound {
                        return Err(anyhow!(
                            "row count {} exceed size bound {}",
                            ops.len(),
                            size_bound
                        ));
                    }
                    assert!(data_chunk_builder.append_one_row(row).is_none());
                }
                (
                    epoch,
                    LogStoreOp::Update {
                        new_value,
                        old_value,
                        ..
                    },
                ) => {
                    if epoch != expected_epoch {
                        return Err(anyhow!(
                            "decoded epoch {} not match expected epoch {}",
                            epoch,
                            expected_epoch
                        ));
                    }
                    read_info.read_update(row_size);
                    ops.push(Op::UpdateDelete);
                    ops.push(Op::UpdateInsert);
                    if ops.len() > size_bound {
                        return Err(anyhow!(
                            "row count {} exceed size bound {}",
                            ops.len(),
                            size_bound
                        ));
                    }
                    assert!(data_chunk_builder.append_one_row(old_value).is_none());
                    assert!(data_chunk_builder.append_one_row(new_value).is_none());
                }
                (_, LogStoreOp::Barrier { .. }) => {
                    return Err(anyhow!("should not get barrier when decoding stream chunk"));
                }
            }
        }
        if ops.is_empty() {
            return Err(anyhow!(
                "should not get empty row when decoding stream chunk. start seq id: {}, end seq id {}",
                start_seq_id,
                end_seq_id
            ));
        }
        read_info.report(metrics);
        Ok(StreamChunk::from_parts(
            ops,
            data_chunk_builder
                .consume_all()
                .expect("should not be empty"),
        ))
    }
}

#[derive(Debug)]
enum StreamState {
    /// The stream has not emitted any row op yet.
    Uninitialized,
    /// All parallelism of stream are consuming row.
    AllConsumingRow { curr_epoch: u64 },
    /// Some parallelism has reached the barrier, and is waiting for other parallelism to reach the
    /// barrier.
    BarrierAligning {
        aligned_vnodes: BitmapBuilder,
        read_size: usize,
        curr_epoch: u64,
        is_checkpoint: bool,
    },
    /// All parallelism has reached the barrier, and the barrier is emitted.
    BarrierEmitted { prev_epoch: u64 },
}

pub(crate) enum KvLogStoreItem {
    StreamChunk {
        chunk: StreamChunk,
        progress: HashMap<VirtualNode, SeqId>,
    },
    Barrier {
        vnodes: Arc<Bitmap>,
        is_checkpoint: bool,
    },
}

type PeekableLogStoreItemStream<S> = Peekable<LogStoreItemStream<S>>;

struct LogStoreRowOpStream<S: StateStoreReadIter> {
    serde: LogStoreRowSerde,

    /// Streams that have not reached a barrier
    row_streams: FuturesUnordered<StreamFuture<PeekableLogStoreItemStream<S>>>,

    /// Streams that have reached a barrier
    barrier_streams: Vec<PeekableLogStoreItemStream<S>>,

    not_started_streams: Vec<(u64, PeekableLogStoreItemStream<S>)>,

    stream_state: StreamState,

    metrics: KvLogStoreReadMetrics,
}

impl<S: StateStoreReadIter> LogStoreRowOpStream<S> {
    pub(crate) fn new(
        iters: Vec<(VirtualNode, S)>,
        serde: LogStoreRowSerde,
        metrics: KvLogStoreReadMetrics,
    ) -> Self {
        assert!(!iters.is_empty());
        Self {
            serde: serde.clone(),
            barrier_streams: iters
                .into_iter()
                .map(|(vnode, s)| deserialize_stream(vnode, s, serde.clone()).peekable())
                .collect(),
            row_streams: FuturesUnordered::new(),
            not_started_streams: Vec::new(),
            stream_state: StreamState::Uninitialized,
            metrics,
        }
    }

    fn check_is_checkpoint(&self, is_checkpoint: bool) -> LogStoreResult<()> {
        if let StreamState::BarrierAligning {
            is_checkpoint: curr_is_checkpoint,
            ..
        } = &self.stream_state
        {
            if is_checkpoint == *curr_is_checkpoint {
                Ok(())
            } else {
                Err(anyhow!(
                    "current aligning barrier is_checkpoint: {}, current barrier is_checkpoint {}",
                    curr_is_checkpoint,
                    is_checkpoint
                ))
            }
        } else {
            Ok(())
        }
    }

    #[try_stream(ok = (Epoch, KvLogStoreItem), error = anyhow::Error)]
    async fn into_vnode_log_store_item_stream(mut self, chunk_size: usize) {
        assert!(chunk_size >= 2, "too small chunk_size: {}", chunk_size);
        let mut ops = Vec::with_capacity(chunk_size);
        let mut data_chunk_builder =
            DataChunkBuilder::new(self.serde.payload_schema.clone(), chunk_size);

        let mut progress = HashMap::new();

        if !self.init().await? {
            // no data in all stream
            return Ok(());
        }

        let this = self;
        pin_mut!(this);

        let mut read_info = ReadInfo::new();
        while let Some(row) = this.next_op().await? {
            let epoch = row.epoch;
            let row_op = row.op;
            let row_read_size = row.size;
            match row_op {
                AlignedLogStoreOp::Row { vnode, seq_id, .. }
                | AlignedLogStoreOp::Update { vnode, seq_id, .. } => {
                    if let Some(previous_seq_id) = progress.insert(vnode, seq_id) {
                        assert!(previous_seq_id < seq_id);
                    }
                }
                _ => {}
            }

            match row_op {
                AlignedLogStoreOp::Row { op, row, .. } => {
                    read_info.read_one_row(row_read_size);
                    ops.push(op);
                    if let Some(chunk) = data_chunk_builder.append_one_row(row) {
                        let ops = replace(&mut ops, Vec::with_capacity(chunk_size));
                        read_info.report(&this.metrics);
                        yield (
                            epoch,
                            KvLogStoreItem::StreamChunk {
                                chunk: StreamChunk::from_parts(ops, chunk),
                                progress: take(&mut progress),
                            },
                        );
                    }
                }
                AlignedLogStoreOp::Update {
                    new_value,
                    old_value,
                    ..
                } => {
                    read_info.read_update(row_read_size);
                    if !data_chunk_builder.can_append_update() {
                        let ops = replace(&mut ops, Vec::with_capacity(chunk_size));
                        let chunk = data_chunk_builder.consume_all().expect("must not be empty");
                        yield (
                            epoch,
                            KvLogStoreItem::StreamChunk {
                                chunk: StreamChunk::from_parts(ops, chunk),
                                progress: take(&mut progress),
                            },
                        );
                    }
                    ops.extend([Op::UpdateDelete, Op::UpdateInsert]);
                    assert!(data_chunk_builder.append_one_row(old_value).is_none());
                    if let Some(chunk) = data_chunk_builder.append_one_row(new_value) {
                        let ops = replace(&mut ops, Vec::with_capacity(chunk_size));
                        read_info.report(&this.metrics);
                        yield (
                            epoch,
                            KvLogStoreItem::StreamChunk {
                                chunk: StreamChunk::from_parts(ops, chunk),
                                progress: take(&mut progress),
                            },
                        );
                    }
                }
                AlignedLogStoreOp::Barrier {
                    vnodes,
                    is_checkpoint,
                } => {
                    read_info.read_one_row(row_read_size);
                    read_info.report(&this.metrics);
                    if let Some(chunk) = data_chunk_builder.consume_all() {
                        let ops = replace(&mut ops, Vec::with_capacity(chunk_size));
                        yield (
                            epoch,
                            KvLogStoreItem::StreamChunk {
                                chunk: StreamChunk::from_parts(ops, chunk),
                                progress: take(&mut progress),
                            },
                        );
                    }
                    yield (
                        epoch,
                        KvLogStoreItem::Barrier {
                            vnodes,
                            is_checkpoint,
                        },
                    )
                }
            }
        }
    }
}

pub(crate) type LogStoreItemMergeStream<S: StateStoreReadIter> =
    impl Stream<Item = LogStoreResult<(Epoch, KvLogStoreItem)>>;

#[define_opaque(LogStoreItemMergeStream)]
pub(crate) fn merge_log_store_item_stream<S: StateStoreReadIter>(
    iters: Vec<(VirtualNode, S)>,
    serde: LogStoreRowSerde,
    chunk_size: usize,
    metrics: KvLogStoreReadMetrics,
) -> LogStoreItemMergeStream<S> {
    LogStoreRowOpStream::new(iters, serde, metrics).into_vnode_log_store_item_stream(chunk_size)
}

mod stream_de {
    use super::*;

    #[derive(Clone, Copy, Debug)]
    pub(super) struct RowMeta {
        pub vnode: VirtualNode,
        pub epoch: u64,
        pub size: usize,
    }

    #[derive(Debug)]
    pub(super) struct AlignedLogStoreRow {
        pub op: AlignedLogStoreOp,
        pub epoch: Epoch,
        pub size: usize,
    }

    #[derive(Debug)]
    pub(super) struct LogStoreRow {
        pub op: LogStoreOp,
        pub row_meta: RowMeta,
    }

    #[derive(Debug)]
    pub(super) struct RawLogStoreRow {
        pub op: LogStoreRowOp,
        pub row_meta: RowMeta,
    }

    pub(super) type LogStoreItemStream<S: StateStoreReadIter> =
        impl Stream<Item = LogStoreResult<LogStoreRow>> + Send + Unpin;

    #[define_opaque(LogStoreItemStream)]
    pub(super) fn deserialize_stream<S: StateStoreReadIter>(
        vnode: VirtualNode,
        iter: S,
        serde: LogStoreRowSerde,
    ) -> LogStoreItemStream<S> {
        may_merge_update(
            iter.into_stream(move |(key, value)| -> StorageResult<RawLogStoreRow> {
                let size = key.user_key.table_key.len() + value.len();
                let (epoch, op) = serde.deserialize(value)?;
                let row_meta = RowMeta { vnode, epoch, size };
                tracing::trace!(?row_meta, ?op, "read_row");
                Ok(RawLogStoreRow { op, row_meta })
            })
            .map_err(Into::into),
        )
        .boxed()
        // The `boxed` call was unnecessary in usual build. But when doing cargo doc,
        // rustc will panic in auto_trait.rs. May remove it when using future version of tool chain.
    }

    #[try_stream(ok = LogStoreRow, error = anyhow::Error)]
    async fn may_merge_update(stream: impl Stream<Item = LogStoreResult<RawLogStoreRow>> + Send) {
        pin_mut!(stream);
        while let Some(RawLogStoreRow { row_meta, op }) = stream.try_next().await? {
            if let LogStoreRowOp::Row {
                seq_id,
                op: Op::UpdateDelete,
                row: old_value,
            } = op
            {
                let next_item = stream.try_next().await?;
                if let Some(row) = next_item {
                    let next_op = row.op;
                    let next_row_meta = row.row_meta;
                    if let LogStoreRowOp::Row {
                        seq_id: next_seq_id,
                        op: Op::UpdateInsert,
                        row: new_value,
                    } = next_op
                    {
                        if row_meta.epoch != next_row_meta.epoch {
                            return Err(anyhow!(
                                "UpdateDelete epoch {} different UpdateInsert epoch {}",
                                row_meta.epoch,
                                next_row_meta.epoch
                            ));
                        }
                        let merged_row_meta = RowMeta {
                            vnode: row_meta.vnode,
                            epoch: row_meta.epoch,
                            size: (row_meta.size + next_row_meta.size),
                        };
                        yield LogStoreRow {
                            op: LogStoreOp::Update {
                                seq_id: next_seq_id,
                                new_value,
                                old_value,
                            },
                            row_meta: merged_row_meta,
                        };
                    } else {
                        if cfg!(debug_assertions) {
                            unreachable!(
                                "should get UpdateInsert after UpdateDelete but get {:?}",
                                next_op
                            );
                        } else {
                            // in release mode just warn
                            warn!("do not get UpdateInsert after UpdateDelete");
                        }
                        yield LogStoreRow {
                            op: LogStoreOp::Row {
                                seq_id,
                                op: Op::UpdateDelete,
                                row: old_value,
                            },
                            row_meta,
                        };
                        yield LogStoreRow {
                            op: LogStoreOp::from(next_op),
                            row_meta: next_row_meta,
                        };
                    }
                } else {
                    if cfg!(debug_assertions) {
                        unreachable!("should be end of stream after UpdateDelete");
                    } else {
                        // in release mode just warn
                        warn!("reach end of stream after UpdateDelete");
                    }
                    yield LogStoreRow {
                        op: LogStoreOp::Row {
                            seq_id,
                            op: Op::UpdateDelete,
                            row: old_value,
                        },
                        row_meta,
                    };
                }
            } else {
                yield LogStoreRow {
                    op: LogStoreOp::from(op),
                    row_meta,
                };
            }
        }
    }
}

use stream_de::*;

impl<S: StateStoreReadIter> LogStoreRowOpStream<S> {
    // Return Ok(false) means all streams have reach the end.
    async fn init(&mut self) -> LogStoreResult<bool> {
        match &self.stream_state {
            StreamState::Uninitialized => {}
            _ => unreachable!("cannot call init for twice"),
        };

        // before init, all streams are in `barrier_streams`
        assert!(
            self.row_streams.is_empty(),
            "when uninitialized, row_streams should be empty"
        );
        assert!(self.not_started_streams.is_empty());
        assert!(!self.barrier_streams.is_empty());

        for mut stream in self.barrier_streams.drain(..) {
            match Pin::new(&mut stream).peek().await {
                Some(Ok(row)) => {
                    self.not_started_streams.push((row.row_meta.epoch, stream));
                }
                Some(Err(_)) => match Pin::new(&mut stream).next().await {
                    Some(Err(e)) => {
                        return Err(e);
                    }
                    _ => unreachable!("on peek we have checked it's Some(Err(_))"),
                },
                None => {
                    continue;
                }
            }
        }

        if self.not_started_streams.is_empty() {
            // No stream has data
            return Ok(false);
        }

        // sorted by epoch descending. Earlier epoch at the end
        self.not_started_streams
            .sort_by_key(|(epoch, _)| HummockEpoch::MAX - *epoch);

        let (epoch, stream) = self
            .not_started_streams
            .pop()
            .expect("have check non-empty");
        self.row_streams.push(stream.into_future());
        while let Some((stream_epoch, _)) = self.not_started_streams.last()
            && *stream_epoch == epoch
        {
            let (_, stream) = self.not_started_streams.pop().expect("should not be empty");
            self.row_streams.push(stream.into_future());
        }
        self.stream_state = StreamState::AllConsumingRow { curr_epoch: epoch };
        Ok(true)
    }

    fn may_init_epoch(&mut self, epoch: u64) -> LogStoreResult<()> {
        let prev_epoch = match &self.stream_state {
            StreamState::Uninitialized => unreachable!("should have init"),
            StreamState::BarrierEmitted { prev_epoch } => *prev_epoch,
            StreamState::AllConsumingRow { curr_epoch }
            | StreamState::BarrierAligning { curr_epoch, .. } => {
                return if *curr_epoch != epoch {
                    Err(anyhow!(
                        "epoch {} does not match with current epoch {}",
                        epoch,
                        curr_epoch
                    ))
                } else {
                    Ok(())
                };
            }
        };

        if prev_epoch >= epoch {
            return Err(anyhow!(
                "epoch {} should be greater than prev epoch {}",
                epoch,
                prev_epoch
            ));
        }

        while let Some((stream_epoch, _)) = self.not_started_streams.last() {
            if *stream_epoch > epoch {
                // Current epoch has not reached the first epoch of
                // the stream. Later streams must also have greater epoch, so break here.
                break;
            }
            if *stream_epoch < epoch {
                return Err(anyhow!(
                    "current epoch {} has exceeded the epoch {} of the stream that has not started",
                    epoch,
                    stream_epoch
                ));
            }
            let (_, stream) = self.not_started_streams.pop().expect("should not be empty");
            self.row_streams.push(stream.into_future());
        }

        self.stream_state = StreamState::AllConsumingRow { curr_epoch: epoch };
        Ok(())
    }

    async fn next_op(&mut self) -> LogStoreResult<Option<AlignedLogStoreRow>> {
        while let (Some(result), stream) = self
            .row_streams
            .next()
            .await
            .expect("row stream should not be empty when polled")
        {
            let row = result?;
            let row_meta = row.row_meta;
            let vnode = row_meta.vnode;
            let decoded_epoch = row_meta.epoch;
            let size = row_meta.size;
            self.may_init_epoch(decoded_epoch)?;
            match row.op {
                LogStoreOp::Row { seq_id, op, row } => {
                    self.row_streams.push(stream.into_future());
                    let op = AlignedLogStoreOp::Row {
                        vnode,
                        seq_id,
                        op,
                        row,
                    };
                    let row = AlignedLogStoreRow {
                        op,
                        size,
                        epoch: decoded_epoch,
                    };
                    return Ok(Some(row));
                }
                LogStoreOp::Update {
                    seq_id,
                    old_value,
                    new_value,
                } => {
                    self.row_streams.push(stream.into_future());
                    let op = AlignedLogStoreOp::Update {
                        vnode,
                        seq_id,
                        old_value,
                        new_value,
                    };
                    let row = AlignedLogStoreRow {
                        op,
                        size,
                        epoch: decoded_epoch,
                    };
                    return Ok(Some(row));
                }
                LogStoreOp::Barrier { is_checkpoint } => {
                    self.check_is_checkpoint(is_checkpoint)?;
                    // Put the current stream to the barrier streams
                    self.barrier_streams.push(stream);

                    if self.row_streams.is_empty() {
                        let old_state = replace(
                            &mut self.stream_state,
                            StreamState::BarrierEmitted {
                                prev_epoch: decoded_epoch,
                            },
                        );

                        let (mut aligned_vnodes, mut read_size) = match old_state {
                            StreamState::BarrierAligning {
                                aligned_vnodes,
                                read_size,
                                ..
                            } => (aligned_vnodes, read_size),
                            _ => (BitmapBuilder::zeroed(self.serde.vnodes().len()), 0),
                        };
                        aligned_vnodes.set(vnode.to_index(), true);
                        read_size += size;

                        while let Some(stream) = self.barrier_streams.pop() {
                            self.row_streams.push(stream.into_future());
                        }
                        return Ok(Some(AlignedLogStoreRow {
                            epoch: decoded_epoch,
                            size: read_size,
                            op: AlignedLogStoreOp::Barrier {
                                vnodes: Arc::new(aligned_vnodes.finish()),
                                is_checkpoint,
                            },
                        }));
                    } else {
                        match &mut self.stream_state {
                            StreamState::BarrierAligning {
                                aligned_vnodes,
                                read_size,
                                curr_epoch,
                                is_checkpoint: current_is_checkpoint,
                            } => {
                                aligned_vnodes.set(vnode.to_index(), true);
                                *read_size += size;
                                if curr_epoch != &decoded_epoch {
                                    return Err(anyhow!(
                                        "current epoch {} does not match with decoded epoch {}",
                                        curr_epoch,
                                        decoded_epoch
                                    ));
                                }
                                if current_is_checkpoint != &is_checkpoint {
                                    return Err(anyhow!(
                                        "current is_checkpoint {} does not match with decoded is_checkpoint {}",
                                        current_is_checkpoint,
                                        is_checkpoint
                                    ));
                                }
                            }
                            other => {
                                let mut aligned_vnodes =
                                    BitmapBuilder::zeroed(self.serde.vnodes().len());
                                aligned_vnodes.set(vnode.to_index(), true);
                                *other = StreamState::BarrierAligning {
                                    aligned_vnodes,
                                    read_size: size,
                                    curr_epoch: decoded_epoch,
                                    is_checkpoint,
                                };
                            }
                        }
                        continue;
                    }
                }
            }
        }
        // End of stream
        match &self.stream_state {
            StreamState::BarrierEmitted { .. } => {}
            s => {
                return Err(anyhow!(
                    "when any of the stream reaches the end, it should be right after emitting an barrier. Current state: {:?}",
                    s
                ));
            }
        }
        assert!(
            self.barrier_streams.is_empty(),
            "should not have any pending barrier received stream after barrier emit"
        );
        if !self.not_started_streams.is_empty() {
            return Err(anyhow!(
                "a stream has reached the end but some other stream has not started yet"
            ));
        }
        if cfg!(debug_assertions) {
            while let Some((opt, _stream)) = self.row_streams.next().await {
                if let Some(result) = opt {
                    return Err(anyhow!(
                        "when any of the stream reaches the end, other stream should also reaches the end, but poll result: {:?}",
                        result
                    ));
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::iter::once;
    use std::sync::Arc;
    use std::task::Poll;

    use bytes::Bytes;
    use futures::stream::empty;
    use futures::{Stream, StreamExt, TryStreamExt, pin_mut, stream};
    use itertools::Itertools;
    use rand::prelude::SliceRandom;
    use rand::rng as thread_rng;
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::row::{OwnedRow, Row};
    use risingwave_common::types::DataType;
    use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
    use risingwave_common::util::epoch::{EpochExt, test_epoch};
    use risingwave_hummock_sdk::key::FullKey;
    use risingwave_storage::error::StorageResult;
    use risingwave_storage::store::{
        FromStreamStateStoreIter, StateStoreKeyedRow, StateStoreReadIter,
    };
    use risingwave_storage::table::SINGLETON_VNODE;
    use tokio::sync::oneshot;
    use tokio::sync::oneshot::Sender;

    use crate::common::log_store_impl::kv_log_store::serde::{
        AlignedLogStoreOp, KvLogStoreItem, LogStoreRowOp, LogStoreRowOpStream, LogStoreRowSerde,
        merge_log_store_item_stream,
    };
    use crate::common::log_store_impl::kv_log_store::test_utils::{
        TEST_TABLE_ID, check_rows_eq, gen_test_data, gen_test_log_store_table,
    };
    use crate::common::log_store_impl::kv_log_store::{
        KV_LOG_STORE_V2_INFO, KvLogStorePkInfo, KvLogStoreReadMetrics, SeqId,
    };

    const EPOCH0: u64 = test_epoch(1);
    const EPOCH1: u64 = test_epoch(2);
    const EPOCH2: u64 = test_epoch(3);

    fn assert_value_eq(expected: AlignedLogStoreOp, actual: AlignedLogStoreOp) {
        match (expected, actual) {
            (
                AlignedLogStoreOp::Barrier {
                    is_checkpoint: expected_is_checkpoint,
                    ..
                },
                AlignedLogStoreOp::Barrier {
                    is_checkpoint: actual_is_checkpoint,
                    ..
                },
            ) => {
                assert_eq!(expected_is_checkpoint, actual_is_checkpoint);
            }
            (
                AlignedLogStoreOp::Row {
                    op: expected_op,
                    row: expected_row,
                    ..
                },
                AlignedLogStoreOp::Row {
                    op: actual_op,
                    row: actual_row,
                    ..
                },
            ) => {
                assert_eq!(expected_op, actual_op);
                assert_eq!(expected_row, actual_row);
            }
            (
                AlignedLogStoreOp::Update {
                    old_value: expected_old_value,
                    new_value: expected_new_value,
                    ..
                },
                AlignedLogStoreOp::Update {
                    old_value: actual_old_value,
                    new_value: actual_new_value,
                    ..
                },
            ) => {
                assert_eq!(expected_old_value, actual_old_value);
                assert_eq!(expected_new_value, actual_new_value);
            }
            (e, a) => panic!("expected: {:?}, got: {:?}", e, a),
        }
    }

    #[test]
    fn test_serde_v1() {
        #[expect(deprecated)]
        test_serde_inner(&crate::common::log_store_impl::kv_log_store::v1::KV_LOG_STORE_V1_INFO);
    }

    #[test]
    fn test_serde_v2() {
        test_serde_inner(&KV_LOG_STORE_V2_INFO);
    }

    fn test_serde_inner(pk_info: &'static KvLogStorePkInfo) {
        let table = gen_test_log_store_table(pk_info);

        let serde = LogStoreRowSerde::new(
            &table,
            Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST))),
            pk_info,
        );

        let (ops, rows) = gen_test_data(0);

        let mut builder = DataChunkBuilder::new(vec![DataType::Int64, DataType::Varchar], 1000000);
        for row in &rows {
            assert!(builder.append_one_row(row).is_none());
        }
        let data_chunk = builder.consume_all().unwrap();
        let stream_chunk = StreamChunk::from_parts(ops, data_chunk);

        let mut epoch = test_epoch(233);

        let mut serialized_keys = vec![];
        let mut seq_id = 1;

        fn remove_vnode_prefix(key: &Bytes) -> Bytes {
            key.slice(VirtualNode::SIZE..)
        }
        let delete_range_right1 = serde.serialize_truncation_offset_watermark((epoch, None));

        for (op, row) in stream_chunk.rows() {
            let (_, key, value) = serde.serialize_data_row(epoch, seq_id, op, row);
            let key = remove_vnode_prefix(&key.0);
            assert!(key < delete_range_right1);
            serialized_keys.push(key);
            let (decoded_epoch, row_op) = serde.deserialize(&value).unwrap();
            assert_eq!(decoded_epoch, epoch);
            match row_op {
                LogStoreRowOp::Row {
                    op: deserialized_op,
                    row: deserialized_row,
                    ..
                } => {
                    assert_eq!(&op, &deserialized_op);
                    assert_eq!(row.to_owned_row(), deserialized_row);
                }
                LogStoreRowOp::Barrier { .. } => unreachable!(),
            }
            seq_id += 1;
        }

        let (key, encoded_barrier) = serde.serialize_barrier(epoch, SINGLETON_VNODE, false);
        let key = remove_vnode_prefix(&key.0);
        match serde.deserialize(&encoded_barrier).unwrap() {
            (decoded_epoch, LogStoreRowOp::Barrier { is_checkpoint }) => {
                assert!(!is_checkpoint);
                assert_eq!(decoded_epoch, epoch);
            }
            _ => unreachable!(),
        }
        assert!(key.as_ref() < delete_range_right1);
        serialized_keys.push(key);

        seq_id = 1;
        epoch.inc_epoch();

        let delete_range_right2 = serde.serialize_truncation_offset_watermark((epoch, None));

        for (op, row) in stream_chunk.rows() {
            let (_, key, value) = serde.serialize_data_row(epoch, seq_id, op, row);
            let key = remove_vnode_prefix(&key.0);
            assert!(key >= delete_range_right1);
            assert!(key < delete_range_right2);
            serialized_keys.push(key);
            let (decoded_epoch, row_op) = serde.deserialize(&value).unwrap();
            assert_eq!(decoded_epoch, epoch);
            match row_op {
                LogStoreRowOp::Row {
                    op: deserialized_op,
                    row: deserialized_row,
                    ..
                } => {
                    assert_eq!(&op, &deserialized_op);
                    assert_eq!(row.to_owned_row(), deserialized_row);
                }
                LogStoreRowOp::Barrier { .. } => unreachable!(),
            }
            seq_id += 1;
        }

        let (key, encoded_checkpoint_barrier) =
            serde.serialize_barrier(epoch, SINGLETON_VNODE, true);
        let key = remove_vnode_prefix(&key.0);
        match serde.deserialize(&encoded_checkpoint_barrier).unwrap() {
            (decoded_epoch, LogStoreRowOp::Barrier { is_checkpoint }) => {
                assert_eq!(decoded_epoch, epoch);
                assert!(is_checkpoint);
            }
            _ => unreachable!(),
        }
        assert!(key.as_ref() >= delete_range_right1);
        assert!(key.as_ref() < delete_range_right2);
        serialized_keys.push(key);

        assert_eq!(serialized_keys.len(), 2 * rows.len() + 2);
        assert!(serialized_keys.is_sorted());
    }

    #[test]
    fn test_encode_epoch() {
        let epochs = vec![
            u64::MIN,
            1u64,
            2,
            3,
            1 + (1 << 63),
            2 + (1 << 63),
            3 + (1 << 63),
            u64::MAX,
        ];
        assert!(epochs.is_sorted());
        let encoded_epochs = epochs
            .iter()
            .map(|epoch| LogStoreRowSerde::encode_epoch(*epoch))
            .collect_vec();
        assert!(encoded_epochs.is_sorted());
        assert_eq!(
            epochs,
            encoded_epochs
                .into_iter()
                .map(LogStoreRowSerde::decode_epoch)
                .collect_vec()
        );
    }

    #[tokio::test]
    async fn test_deserialize_stream_chunk_v1() {
        #[expect(deprecated)]
        test_deserialize_stream_chunk_inner(
            &crate::common::log_store_impl::kv_log_store::v1::KV_LOG_STORE_V1_INFO,
        )
        .await
    }

    #[tokio::test]
    async fn test_deserialize_stream_chunk_v2() {
        test_deserialize_stream_chunk_inner(&KV_LOG_STORE_V2_INFO).await
    }

    async fn test_deserialize_stream_chunk_inner(pk_info: &'static KvLogStorePkInfo) {
        let table = gen_test_log_store_table(pk_info);
        let serde = LogStoreRowSerde::new(
            &table,
            Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST))),
            pk_info,
        );
        let (ops, rows) = gen_test_data(0);

        let mut seq_id = 1;
        let start_seq_id = seq_id;

        let (stream, tx) = gen_row_stream(
            serde.clone(),
            ops.clone(),
            rows.clone(),
            EPOCH1,
            &mut seq_id,
        );
        let end_seq_id = seq_id - 1;
        tx.send(()).unwrap();
        let chunk = serde
            .deserialize_stream_chunk(
                once((
                    VirtualNode::ZERO,
                    FromStreamStateStoreIter::new(stream.boxed()),
                )),
                start_seq_id,
                end_seq_id,
                EPOCH1,
                &KvLogStoreReadMetrics::for_test(),
            )
            .await
            .unwrap();
        for (i, (op, row)) in chunk.rows().enumerate() {
            assert_eq!(ops[i], op);
            assert_eq!(rows[i], row.to_owned_row());
        }
    }

    fn gen_row_stream(
        serde: LogStoreRowSerde,
        ops: Vec<Op>,
        rows: Vec<OwnedRow>,
        epoch: u64,
        seq_id: &mut SeqId,
    ) -> (
        impl Stream<Item = StorageResult<StateStoreKeyedRow>> + use<>,
        Sender<()>,
    ) {
        let (tx, rx) = oneshot::channel();
        let row_data = ops
            .into_iter()
            .zip_eq(rows)
            .map(|(op, row)| {
                let (_, key, value) = serde.serialize_data_row(epoch, *seq_id, op, row);
                *seq_id += 1;
                Ok((FullKey::new(TEST_TABLE_ID, key, epoch), value))
            })
            .collect_vec();
        (
            stream::once(async move {
                rx.await.unwrap();
                stream::iter(row_data)
            })
            .flatten(),
            tx,
        )
    }

    #[expect(clippy::type_complexity)]
    fn gen_single_test_stream(
        serde: LogStoreRowSerde,
        seq_id: &mut SeqId,
        base: i64,
    ) -> (
        impl Stream<Item = StorageResult<StateStoreKeyedRow>> + use<>,
        oneshot::Sender<()>,
        oneshot::Sender<()>,
        Vec<Op>,
        Vec<OwnedRow>,
    ) {
        let (ops, rows) = gen_test_data(base);
        let first_barrier = {
            let (key, value) = serde.serialize_barrier(EPOCH0, SINGLETON_VNODE, true);
            Ok((FullKey::new(TEST_TABLE_ID, key, EPOCH0), value))
        };
        let stream = stream::once(async move { first_barrier });
        let (row_stream, tx1) =
            gen_row_stream(serde.clone(), ops.clone(), rows.clone(), EPOCH1, seq_id);
        let stream = stream.chain(row_stream);
        let stream = stream.chain(stream::once({
            let serde = serde.clone();
            async move {
                let (key, value) = serde.serialize_barrier(EPOCH1, SINGLETON_VNODE, false);
                Ok((FullKey::new(TEST_TABLE_ID, key, EPOCH1), value))
            }
        }));
        let (row_stream, tx2) =
            gen_row_stream(serde.clone(), ops.clone(), rows.clone(), EPOCH2, seq_id);
        let stream = stream.chain(row_stream).chain(stream::once({
            async move {
                let (key, value) = serde.serialize_barrier(EPOCH2, SINGLETON_VNODE, true);
                Ok((FullKey::new(TEST_TABLE_ID, key, EPOCH2), value))
            }
        }));
        (stream, tx1, tx2, ops, rows)
    }

    #[expect(clippy::type_complexity)]
    fn gen_multi_test_stream(
        serde: LogStoreRowSerde,
        size: usize,
    ) -> (
        LogStoreRowOpStream<impl StateStoreReadIter>,
        Vec<Option<Sender<()>>>,
        Vec<Option<Sender<()>>>,
        Vec<Vec<Op>>,
        Vec<Vec<OwnedRow>>,
    ) {
        let mut seq_id = 1;
        let mut streams = Vec::new();
        let mut tx1 = Vec::new();
        let mut tx2 = Vec::new();
        let mut ops = Vec::new();
        let mut rows = Vec::new();
        for i in 0..size {
            let (s, t1, t2, op_list, row_list) =
                gen_single_test_stream(serde.clone(), &mut seq_id, (100 * i) as _);
            let s = FromStreamStateStoreIter::new(s.boxed());
            streams.push((VirtualNode::ZERO, s));
            tx1.push(Some(t1));
            tx2.push(Some(t2));
            ops.push(op_list);
            rows.push(row_list);
        }

        let stream = LogStoreRowOpStream::new(streams, serde, KvLogStoreReadMetrics::for_test());

        for i in 0..size {
            let (o, r) = gen_test_data((100 * i) as _);
            ops.push(o);
            rows.push(r);
        }

        (stream, tx1, tx2, ops, rows)
    }

    #[tokio::test]
    async fn test_row_stream_basic_v1() {
        #[expect(deprecated)]
        test_row_stream_basic_inner(
            &crate::common::log_store_impl::kv_log_store::v1::KV_LOG_STORE_V1_INFO,
        )
        .await
    }

    #[tokio::test]
    async fn test_row_stream_basic_v2() {
        test_row_stream_basic_inner(&KV_LOG_STORE_V2_INFO).await
    }

    async fn test_row_stream_basic_inner(pk_info: &'static KvLogStorePkInfo) {
        let table = gen_test_log_store_table(pk_info);

        let serde = LogStoreRowSerde::new(
            &table,
            Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST))),
            pk_info,
        );

        const MERGE_SIZE: usize = 10;

        let (mut stream, mut tx1, mut tx2, ops, rows) = gen_multi_test_stream(serde, MERGE_SIZE);

        stream.init().await.unwrap();

        pin_mut!(stream);

        let row = stream.next_op().await.unwrap().unwrap();
        let epoch = row.epoch;
        let op = row.op;

        assert_eq!(EPOCH0, epoch);
        let actual = AlignedLogStoreOp::Barrier {
            is_checkpoint: true,
            vnodes: Arc::new(Bitmap::ones(0)),
        };
        assert_value_eq(actual, op);

        let mut index = (0..MERGE_SIZE).collect_vec();
        index.shuffle(&mut thread_rng());

        for i in index {
            tx1[i].take().unwrap().send(()).unwrap();
            let mut j = 0;
            while j < ops[i].len() {
                let row = stream.next_op().await.unwrap().unwrap();
                let epoch = row.epoch;
                let op = row.op;
                assert_eq!(EPOCH1, epoch);
                if let Op::UpdateDelete = ops[i][j] {
                    assert_eq!(Op::UpdateInsert, ops[i][j + 1]);
                    let expected_op = AlignedLogStoreOp::Update {
                        vnode: VirtualNode::ZERO,
                        seq_id: 0,
                        old_value: rows[i][j].clone(),
                        new_value: rows[i][j + 1].clone(),
                    };
                    assert_value_eq(expected_op, op);
                    j += 2;
                } else {
                    let expected_op = AlignedLogStoreOp::Row {
                        vnode: VirtualNode::ZERO,
                        seq_id: 0,
                        op: ops[i][j],
                        row: rows[i][j].clone(),
                    };
                    assert_value_eq(expected_op, op);
                    j += 1;
                }
            }
            assert_eq!(j, ops[i].len());
        }

        let row = stream.next_op().await.unwrap().unwrap();
        let epoch = row.epoch;
        let op = row.op;

        assert_eq!(EPOCH1, epoch);
        let actual = AlignedLogStoreOp::Barrier {
            is_checkpoint: false,
            vnodes: Arc::new(Bitmap::ones(0)),
        };
        assert_value_eq(actual, op);

        let mut index = (0..MERGE_SIZE).collect_vec();
        index.shuffle(&mut thread_rng());

        for i in index {
            tx2[i].take().unwrap().send(()).unwrap();
            let mut j = 0;
            while j < ops[i].len() {
                let row = stream.next_op().await.unwrap().unwrap();
                let epoch = row.epoch;
                let op = row.op;
                assert_eq!(EPOCH2, epoch);
                if let Op::UpdateDelete = ops[i][j] {
                    assert_eq!(Op::UpdateInsert, ops[i][j + 1]);
                    let expected_op = AlignedLogStoreOp::Update {
                        vnode: VirtualNode::ZERO,
                        seq_id: 0,
                        old_value: rows[i][j].clone(),
                        new_value: rows[i][j + 1].clone(),
                    };
                    assert_value_eq(expected_op, op);
                    j += 2;
                } else {
                    let expected_op = AlignedLogStoreOp::Row {
                        vnode: VirtualNode::ZERO,
                        seq_id: 0,
                        op: ops[i][j],
                        row: rows[i][j].clone(),
                    };
                    assert_value_eq(expected_op, op);
                    j += 1;
                }
            }
            assert_eq!(j, ops[i].len());
        }

        let row = stream.next_op().await.unwrap().unwrap();
        let epoch = row.epoch;
        let op = row.op;
        assert_eq!(EPOCH2, epoch);
        let actual = AlignedLogStoreOp::Barrier {
            is_checkpoint: true,
            vnodes: Arc::new(Bitmap::ones(0)),
        };
        assert_value_eq(actual, op);

        assert!(stream.next_op().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_log_store_stream_basic_v1() {
        #[expect(deprecated)]
        test_log_store_stream_basic_inner(
            &crate::common::log_store_impl::kv_log_store::v1::KV_LOG_STORE_V1_INFO,
        )
        .await
    }

    #[tokio::test]
    async fn test_log_store_stream_basic_v2() {
        test_log_store_stream_basic_inner(&KV_LOG_STORE_V2_INFO).await
    }

    async fn test_log_store_stream_basic_inner(pk_info: &'static KvLogStorePkInfo) {
        let table = gen_test_log_store_table(pk_info);

        let serde = LogStoreRowSerde::new(
            &table,
            Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST))),
            pk_info,
        );

        let mut seq_id = 1;
        let (stream, tx1, tx2, ops, rows) = gen_single_test_stream(serde.clone(), &mut seq_id, 0);
        let stream = FromStreamStateStoreIter::new(stream.boxed());

        const CHUNK_SIZE: usize = 3;

        let stream = merge_log_store_item_stream(
            vec![(VirtualNode::ZERO, stream)],
            serde,
            CHUNK_SIZE,
            KvLogStoreReadMetrics::for_test(),
        );

        pin_mut!(stream);

        let (epoch, item): (_, KvLogStoreItem) = stream.try_next().await.unwrap().unwrap();
        assert_eq!(EPOCH0, epoch);
        match item {
            KvLogStoreItem::StreamChunk { .. } => unreachable!(),
            KvLogStoreItem::Barrier { is_checkpoint, .. } => {
                assert!(is_checkpoint);
            }
        }

        assert!(
            poll_fn(|cx| Poll::Ready(stream.poll_next_unpin(cx)))
                .await
                .is_pending()
        );

        tx1.send(()).unwrap();

        {
            let mut remain = ops.len();
            while remain > 0 {
                let start_index = ops.len() - remain;
                let (epoch, item): (_, KvLogStoreItem) = stream.try_next().await.unwrap().unwrap();
                assert_eq!(EPOCH1, epoch);
                match item {
                    KvLogStoreItem::StreamChunk { chunk, .. } => {
                        let size = chunk.cardinality();
                        assert!(size <= CHUNK_SIZE);
                        remain -= size;
                        assert!(check_rows_eq(
                            chunk.rows(),
                            (start_index..(start_index + size)).map(|i| (ops[i], &rows[i]))
                        ));
                    }
                    _ => unreachable!(),
                }
            }
        }

        let (epoch, item): (_, KvLogStoreItem) = stream.try_next().await.unwrap().unwrap();
        assert_eq!(EPOCH1, epoch);
        match item {
            KvLogStoreItem::StreamChunk { .. } => unreachable!(),
            KvLogStoreItem::Barrier { is_checkpoint, .. } => {
                assert!(!is_checkpoint);
            }
        }

        assert!(
            poll_fn(|cx| Poll::Ready(stream.poll_next_unpin(cx)))
                .await
                .is_pending()
        );

        tx2.send(()).unwrap();

        {
            let mut remain = ops.len();
            while remain > 0 {
                let start_index = ops.len() - remain;
                let (epoch, item): (_, KvLogStoreItem) = stream.try_next().await.unwrap().unwrap();
                assert_eq!(EPOCH2, epoch);
                match item {
                    KvLogStoreItem::StreamChunk { chunk, .. } => {
                        let size = chunk.cardinality();
                        assert!(size <= CHUNK_SIZE);
                        remain -= size;
                        assert!(check_rows_eq(
                            chunk.rows(),
                            (start_index..(start_index + size)).map(|i| (ops[i], &rows[i]))
                        ));
                    }
                    _ => unreachable!(),
                }
            }
        }

        let (epoch, item): (_, KvLogStoreItem) = stream.try_next().await.unwrap().unwrap();
        assert_eq!(EPOCH2, epoch);
        match item {
            KvLogStoreItem::StreamChunk { .. } => unreachable!(),
            KvLogStoreItem::Barrier { is_checkpoint, .. } => {
                assert!(is_checkpoint);
            }
        }

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_empty_stream() {
        let pk_info: &'static KvLogStorePkInfo = &KV_LOG_STORE_V2_INFO;
        let table = gen_test_log_store_table(pk_info);

        let serde = LogStoreRowSerde::new(
            &table,
            Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST))),
            pk_info,
        );

        const CHUNK_SIZE: usize = 3;

        let stream = merge_log_store_item_stream(
            vec![
                (VirtualNode::ZERO, FromStreamStateStoreIter::new(empty())),
                (VirtualNode::ZERO, FromStreamStateStoreIter::new(empty())),
            ],
            serde,
            CHUNK_SIZE,
            KvLogStoreReadMetrics::for_test(),
        );

        pin_mut!(stream);

        assert!(stream.next().await.is_none());
    }
}
