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
use std::collections::hash_map::Entry;
use std::mem::replace;
use std::sync::Arc;

use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::TableOption;
use risingwave_common::metrics::{
    LabelGuardedHistogram, LabelGuardedIntCounter, LabelGuardedIntGauge,
};
use risingwave_connector::sink::SinkParam;
use risingwave_connector::sink::log_store::{LogStoreFactory, TruncateBarrierLogReader};
use risingwave_pb::catalog::Table;
use risingwave_storage::StateStore;
use risingwave_storage::store::{LocalStateStore, NewLocalOptions, OpConsistencyLevel};
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::{oneshot, watch};

use crate::common::log_store_impl::kv_log_store::buffer::new_log_store_buffer;
use crate::common::log_store_impl::kv_log_store::reader::KvLogStoreReader;
use crate::common::log_store_impl::kv_log_store::serde::LogStoreRowSerde;
use crate::common::log_store_impl::kv_log_store::writer::KvLogStoreWriter;
use crate::executor::monitor::StreamingMetrics;

pub(crate) mod buffer;
pub mod reader;
pub(crate) mod serde;
pub(crate) mod state;
#[cfg(test)]
pub mod test_utils;
mod writer;

pub(crate) use reader::{REWIND_BACKOFF_MULTIPLIER, REWIND_INITIAL_DELAY, REWIND_MAX_DELAY};
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::row::ArrayVec;
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::sort_util::OrderType;

#[derive(Debug)]
pub(crate) enum LogStoreVnodeProgress {
    None,
    PerVnode(HashMap<VirtualNode, (Epoch, Option<SeqId>)>),
    Aligned(Arc<Bitmap>, Epoch, Option<SeqId>),
}

impl LogStoreVnodeProgress {
    pub(crate) fn take(&mut self) -> Self {
        replace(self, Self::None)
    }

    fn assert_progress_increase(
        prev_epoch: Epoch,
        prev_progress: Option<SeqId>,
        epoch: Epoch,
        progress: Option<SeqId>,
    ) {
        match prev_progress {
            None => {
                assert!(
                    prev_epoch <= epoch,
                    "barrier epoch {} decrease to {}",
                    prev_epoch,
                    epoch
                );
            }
            Some(prev_progress) => {
                assert_eq!(prev_epoch, epoch);
                if let Some(progress) = progress {
                    assert!(
                        progress >= prev_progress,
                        "seq_id decreased: prev_epoch={}, epoch={}, prev_progress={}, progress={}",
                        prev_epoch,
                        epoch,
                        prev_progress,
                        progress
                    );
                    if progress == prev_progress {
                        tracing::warn!(
                            prev_epoch,
                            epoch,
                            prev_progress,
                            progress,
                            "progress did not strictly increase: this may indicate \
                             empty chunks (cardinality=0) were not filtered out"
                        );
                    }
                }
            }
        }
    }

    pub(crate) fn apply_per_vnode(&mut self, epoch: Epoch, progress: HashMap<VirtualNode, SeqId>) {
        fn apply_to_progress_map(
            prev_progress: &mut HashMap<VirtualNode, (Epoch, Option<SeqId>)>,
            epoch: Epoch,
            progress: HashMap<VirtualNode, SeqId>,
        ) {
            for (vnode, seq_id) in progress {
                match prev_progress.entry(vnode) {
                    Entry::Occupied(entry) => {
                        let (prev_epoch, prev_seq_id) = entry.into_mut();
                        LogStoreVnodeProgress::assert_progress_increase(
                            *prev_epoch,
                            *prev_seq_id,
                            epoch,
                            Some(seq_id),
                        );
                        *prev_epoch = epoch;
                        *prev_seq_id = Some(seq_id);
                    }
                    Entry::Vacant(entry) => {
                        entry.insert((epoch, Some(seq_id)));
                    }
                }
            }
        }
        match self {
            LogStoreVnodeProgress::None => {
                *self = LogStoreVnodeProgress::PerVnode(
                    progress
                        .into_iter()
                        .map(|(vnode, seq_id)| (vnode, (epoch, Some(seq_id))))
                        .collect(),
                )
            }
            LogStoreVnodeProgress::PerVnode(prev_progress) => {
                apply_to_progress_map(prev_progress, epoch, progress);
            }
            LogStoreVnodeProgress::Aligned(vnodes, prev_epoch, prev_progress) => {
                let mut progress_map = vnodes
                    .iter_vnodes()
                    .map(|vnode| (vnode, (*prev_epoch, *prev_progress)))
                    .collect();
                apply_to_progress_map(&mut progress_map, epoch, progress);
                *self = LogStoreVnodeProgress::PerVnode(progress_map);
            }
        }
    }

    pub(crate) fn apply_aligned(
        &mut self,
        vnodes: Arc<Bitmap>,
        epoch: Epoch,
        progress: Option<SeqId>,
    ) {
        match self {
            LogStoreVnodeProgress::None => {}
            LogStoreVnodeProgress::PerVnode(prev_progress) => {
                for (vnode, (prev_epoch, prev_progress)) in prev_progress.drain() {
                    assert!(
                        vnodes.is_set(vnode.to_index()),
                        "aligned progress not covered existing vnode {}",
                        vnode
                    );
                    Self::assert_progress_increase(prev_epoch, prev_progress, epoch, progress);
                }
            }
            LogStoreVnodeProgress::Aligned(prev_vnodes, prev_epoch, prev_progress) => {
                assert_eq!(vnodes.len(), prev_vnodes.len());
                assert!(prev_vnodes.iter_ones().all(|vnode| vnodes.is_set(vnode)));
                Self::assert_progress_increase(*prev_epoch, *prev_progress, epoch, progress);
            }
        }
        *self = LogStoreVnodeProgress::Aligned(vnodes, epoch, progress);
    }
}

// TODO: unify with `risingwave_common::Epoch`
pub(crate) type Epoch = u64;

pub(crate) type SeqId = i32;
type RowOpCodeType = i16;

pub(crate) const FIRST_SEQ_ID: SeqId = 0;

/// Readers truncate the offset at the granularity of seq id.
/// None `SeqIdType` means that the whole epoch is truncated.
pub(crate) type ReaderTruncationOffsetType = (u64, Option<SeqId>);

#[derive(Clone)]
pub struct KvLogStoreReadMetrics {
    pub storage_read_count: LabelGuardedIntCounter,
    pub storage_read_size: LabelGuardedIntCounter,
}

impl KvLogStoreReadMetrics {
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self {
            storage_read_count: LabelGuardedIntCounter::test_int_counter::<5>(),
            storage_read_size: LabelGuardedIntCounter::test_int_counter::<5>(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct KvLogStoreMetrics {
    pub storage_write_count: LabelGuardedIntCounter,
    pub storage_write_size: LabelGuardedIntCounter,
    pub rewind_count: LabelGuardedIntCounter,
    pub rewind_delay: LabelGuardedHistogram,
    pub buffer_unconsumed_item_count: LabelGuardedIntGauge,
    pub buffer_unconsumed_row_count: LabelGuardedIntGauge,
    pub buffer_unconsumed_epoch_count: LabelGuardedIntGauge,
    pub buffer_unconsumed_min_epoch: LabelGuardedIntGauge,
    pub buffer_memory_bytes: LabelGuardedIntGauge,
    pub persistent_log_read_metrics: KvLogStoreReadMetrics,
    pub flushed_buffer_read_metrics: KvLogStoreReadMetrics,
}

impl KvLogStoreMetrics {
    pub(crate) fn new(
        metrics: &StreamingMetrics,
        actor_id: ActorId,
        sink_param: &SinkParam,
        connector: &'static str,
    ) -> Self {
        let id = sink_param.sink_id.as_raw_id();
        let name = &sink_param.sink_name;
        Self::new_inner(metrics, actor_id, id, name, connector)
    }

    /// `id`: refers to a unique way to identify the logstore. This can be the sink id,
    ///       or for joins, it can be the `fragment_id`.
    /// `name`: refers to the MV / Sink that the log store is associated with.
    /// `target`: refers to the target of the log store,
    ///           for instance `MySql` Sink, PG sink, etc...
    ///           or unaligned join.
    pub(crate) fn new_inner(
        metrics: &StreamingMetrics,
        actor_id: ActorId,
        id: u32,
        name: &str,
        target: &'static str,
    ) -> Self {
        let actor_id_str = actor_id.to_string();
        let id_str = id.to_string();

        let labels = &[&actor_id_str, target, &id_str, name];
        let storage_write_size = metrics
            .kv_log_store_storage_write_size
            .with_guarded_label_values(labels);
        let storage_write_count = metrics
            .kv_log_store_storage_write_count
            .with_guarded_label_values(labels);

        const READ_PERSISTENT_LOG: &str = "persistent_log";
        const READ_FLUSHED_BUFFER: &str = "flushed_buffer";

        let persistent_log_read_size = metrics
            .kv_log_store_storage_read_size
            .with_guarded_label_values(&[
                actor_id_str.as_str(),
                target,
                id_str.as_str(),
                name,
                READ_PERSISTENT_LOG,
            ]);
        let persistent_log_read_count = metrics
            .kv_log_store_storage_read_count
            .with_guarded_label_values(&[
                actor_id_str.as_str(),
                target,
                id_str.as_str(),
                name,
                READ_PERSISTENT_LOG,
            ]);

        let flushed_buffer_read_size = metrics
            .kv_log_store_storage_read_size
            .with_guarded_label_values(&[
                actor_id_str.as_str(),
                target,
                id_str.as_str(),
                name,
                READ_FLUSHED_BUFFER,
            ]);
        let flushed_buffer_read_count = metrics
            .kv_log_store_storage_read_count
            .with_guarded_label_values(&[
                actor_id_str.as_str(),
                target,
                id_str.as_str(),
                name,
                READ_FLUSHED_BUFFER,
            ]);

        let rewind_count = metrics
            .kv_log_store_rewind_count
            .with_guarded_label_values(labels);

        let rewind_delay = metrics
            .kv_log_store_rewind_delay
            .with_guarded_label_values(labels);

        let buffer_unconsumed_item_count = metrics
            .kv_log_store_buffer_unconsumed_item_count
            .with_guarded_label_values(labels);
        let buffer_unconsumed_row_count = metrics
            .kv_log_store_buffer_unconsumed_row_count
            .with_guarded_label_values(labels);
        let buffer_unconsumed_epoch_count = metrics
            .kv_log_store_buffer_unconsumed_epoch_count
            .with_guarded_label_values(labels);
        let buffer_unconsumed_min_epoch = metrics
            .kv_log_store_buffer_unconsumed_min_epoch
            .with_guarded_label_values(labels);
        let buffer_memory_bytes = metrics
            .kv_log_store_buffer_memory_bytes
            .with_guarded_label_values(labels);

        Self {
            storage_write_size,
            rewind_count,
            storage_write_count,
            buffer_unconsumed_item_count,
            buffer_unconsumed_row_count,
            buffer_unconsumed_epoch_count,
            buffer_unconsumed_min_epoch,
            buffer_memory_bytes,
            persistent_log_read_metrics: KvLogStoreReadMetrics {
                storage_read_size: persistent_log_read_size,
                storage_read_count: persistent_log_read_count,
            },
            flushed_buffer_read_metrics: KvLogStoreReadMetrics {
                storage_read_count: flushed_buffer_read_count,
                storage_read_size: flushed_buffer_read_size,
            },
            rewind_delay,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        KvLogStoreMetrics {
            storage_write_count: LabelGuardedIntCounter::test_int_counter::<4>(),
            storage_write_size: LabelGuardedIntCounter::test_int_counter::<4>(),
            buffer_unconsumed_item_count: LabelGuardedIntGauge::test_int_gauge::<4>(),
            buffer_unconsumed_row_count: LabelGuardedIntGauge::test_int_gauge::<4>(),
            buffer_unconsumed_epoch_count: LabelGuardedIntGauge::test_int_gauge::<4>(),
            buffer_unconsumed_min_epoch: LabelGuardedIntGauge::test_int_gauge::<4>(),
            buffer_memory_bytes: LabelGuardedIntGauge::test_int_gauge::<4>(),
            rewind_count: LabelGuardedIntCounter::test_int_counter::<4>(),
            rewind_delay: LabelGuardedHistogram::test_histogram::<4>(),
            persistent_log_read_metrics: KvLogStoreReadMetrics::for_test(),
            flushed_buffer_read_metrics: KvLogStoreReadMetrics::for_test(),
        }
    }
}

pub(crate) struct FlushInfo {
    pub(crate) flush_size: usize,
    pub(crate) flush_count: usize,
}

impl FlushInfo {
    pub(crate) fn new() -> Self {
        FlushInfo {
            flush_count: 0,
            flush_size: 0,
        }
    }

    pub(crate) fn flush_one(&mut self, size: usize) {
        self.flush_size += size;
        self.flush_count += 1;
    }

    pub(crate) fn report(self, metrics: &KvLogStoreMetrics) {
        metrics.storage_write_count.inc_by(self.flush_count as _);
        metrics.storage_write_size.inc_by(self.flush_size as _);
    }
}

type KvLogStorePkRow = ArrayVec<[Datum; 3]>;

pub(crate) struct KvLogStorePkInfo {
    pub epoch_column_index: usize,
    pub row_op_column_index: usize,
    pub seq_id_column_index: usize,
    pub predefined_columns: &'static [(&'static str, DataType)],
    pub pk_orderings: &'static [OrderType],
    pub compute_pk:
        fn(vnode: VirtualNode, encoded_epoch: i64, seq_id: Option<SeqId>) -> KvLogStorePkRow,
}

impl KvLogStorePkInfo {
    pub fn pk_len(&self) -> usize {
        self.pk_orderings.len()
    }

    pub fn predefined_column_len(&self) -> usize {
        self.predefined_columns.len()
    }

    pub fn pk_types(&self) -> Vec<DataType> {
        (0..self.pk_len())
            .map(|i| self.predefined_columns[i].1.clone())
            .collect()
    }
}

#[expect(deprecated)]
pub(crate) use v1::KV_LOG_STORE_V1_INFO;

mod v1 {
    use std::sync::LazyLock;

    use risingwave_common::constants::log_store::v1::{
        EPOCH_COLUMN_INDEX, KV_LOG_STORE_PREDEFINED_COLUMNS, PK_ORDERING, ROW_OP_COLUMN_INDEX,
        SEQ_ID_COLUMN_INDEX,
    };
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::types::ScalarImpl;

    use super::{KvLogStorePkInfo, KvLogStorePkRow};
    use crate::common::log_store_impl::kv_log_store::SeqId;

    #[deprecated]
    pub(crate) static KV_LOG_STORE_V1_INFO: LazyLock<KvLogStorePkInfo> = LazyLock::new(|| {
        fn compute_pk(
            _vnode: VirtualNode,
            encoded_epoch: i64,
            seq_id: Option<SeqId>,
        ) -> KvLogStorePkRow {
            KvLogStorePkRow::from_array_len(
                [
                    Some(ScalarImpl::Int64(encoded_epoch)),
                    seq_id.map(ScalarImpl::Int32),
                    None,
                ],
                2,
            )
        }
        KvLogStorePkInfo {
            epoch_column_index: EPOCH_COLUMN_INDEX,
            row_op_column_index: ROW_OP_COLUMN_INDEX,
            seq_id_column_index: SEQ_ID_COLUMN_INDEX,
            predefined_columns: &KV_LOG_STORE_PREDEFINED_COLUMNS[..],
            pk_orderings: &PK_ORDERING[..],
            compute_pk,
        }
    });
}

pub(crate) use v2::KV_LOG_STORE_V2_INFO;

use crate::common::log_store_impl::kv_log_store::state::new_log_store_state;
use crate::task::ActorId;

/// A new version of log store schema. Compared to v1, the v2 added a new vnode column to the log store pk,
/// becomes `epoch`, `seq_id` and `vnode`. In this way, providing a log store pk, we can get exactly one single row.
///
/// In v1, dist key is not in pk, and we will get an error in batch query when we try to compute dist key in pk indices.
/// Now in v2, since we add a vnode column in pk, we can set the vnode index in pk correctly, and the batch query can be
/// correctly executed. See <https://github.com/risingwavelabs/risingwave/issues/14503> for details.
mod v2 {
    use std::sync::LazyLock;

    use risingwave_common::constants::log_store::v2::{
        EPOCH_COLUMN_INDEX, KV_LOG_STORE_PREDEFINED_COLUMNS, PK_ORDERING, ROW_OP_COLUMN_INDEX,
        SEQ_ID_COLUMN_INDEX,
    };
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::types::ScalarImpl;

    use super::{KvLogStorePkInfo, KvLogStorePkRow};
    use crate::common::log_store_impl::kv_log_store::SeqId;

    pub(crate) static KV_LOG_STORE_V2_INFO: LazyLock<KvLogStorePkInfo> = LazyLock::new(|| {
        fn compute_pk(
            vnode: VirtualNode,
            encoded_epoch: i64,
            seq_id: Option<SeqId>,
        ) -> KvLogStorePkRow {
            KvLogStorePkRow::from([
                Some(ScalarImpl::Int64(encoded_epoch)),
                seq_id.map(ScalarImpl::Int32),
                vnode.to_datum(),
            ])
        }
        KvLogStorePkInfo {
            epoch_column_index: EPOCH_COLUMN_INDEX,
            row_op_column_index: ROW_OP_COLUMN_INDEX,
            seq_id_column_index: SEQ_ID_COLUMN_INDEX,
            predefined_columns: &KV_LOG_STORE_PREDEFINED_COLUMNS[..],
            pk_orderings: &PK_ORDERING[..],
            compute_pk,
        }
    });
}

pub struct KvLogStoreFactory<S: StateStore> {
    state_store: S,

    table_catalog: Table,

    vnodes: Option<Arc<Bitmap>>,

    max_buffer_row_count: usize,
    chunk_size: usize,

    metrics: KvLogStoreMetrics,

    identity: String,

    pk_info: &'static KvLogStorePkInfo,

    /// Semaphore to limit the number of concurrent historical reads.
    /// `None` means unlimited.
    historical_read_semaphore: Option<Arc<tokio::sync::Semaphore>>,
}

impl<S: StateStore> KvLogStoreFactory<S> {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        state_store: S,
        table_catalog: Table,
        vnodes: Option<Arc<Bitmap>>,
        max_buffer_row_count: usize,
        chunk_size: usize,
        metrics: KvLogStoreMetrics,
        identity: impl Into<String>,
        historical_read_semaphore: Option<Arc<tokio::sync::Semaphore>>,
        pk_info: &'static KvLogStorePkInfo,
    ) -> Self {
        Self {
            state_store,
            table_catalog,
            vnodes,
            max_buffer_row_count,
            chunk_size,
            metrics,
            identity: identity.into(),
            pk_info,
            historical_read_semaphore,
        }
    }
}

impl<S: StateStore> LogStoreFactory for KvLogStoreFactory<S> {
    type Reader = TruncateBarrierLogReader<
        KvLogStoreReader<<S::Local as LocalStateStore>::FlushedSnapshotReader>,
    >;
    type Writer = KvLogStoreWriter<S::Local>;

    const ALLOW_REWIND: bool = true;
    const REBUILD_SINK_ON_UPDATE_VNODE_BITMAP: bool = true;

    async fn build(self) -> (Self::Reader, Self::Writer) {
        let table_id = self.table_catalog.id;
        let (pause_tx, pause_rx) = watch::channel(false);
        let serde = LogStoreRowSerde::new(&self.table_catalog, self.vnodes, self.pk_info);
        let local_state_store = self
            .state_store
            .new_local(NewLocalOptions {
                table_id,
                fragment_id: self.table_catalog.fragment_id,
                op_consistency_level: OpConsistencyLevel::Inconsistent,
                table_option: TableOption {
                    retention_seconds: None,
                },
                is_replicated: false,
                vnodes: serde.vnodes().clone(),
                upload_on_flush: false,
            })
            .await;

        let (tx, rx) = new_log_store_buffer(
            self.max_buffer_row_count,
            self.chunk_size,
            self.metrics.clone(),
        );

        let (read_state, write_state) =
            new_log_store_state(table_id, local_state_store, serde, self.chunk_size);

        let (init_epoch_tx, init_epoch_rx) = oneshot::channel();
        let (update_vnode_bitmap_tx, update_vnode_bitmap_rx) = unbounded_channel();

        let reader = KvLogStoreReader::new(
            read_state,
            rx,
            init_epoch_rx,
            update_vnode_bitmap_rx,
            self.metrics.clone(),
            pause_rx,
            self.identity.clone(),
            self.historical_read_semaphore,
        );

        let writer = KvLogStoreWriter::new(
            write_state,
            tx,
            init_epoch_tx,
            update_vnode_bitmap_tx,
            self.metrics,
            pause_tx,
            self.identity,
        );

        (TruncateBarrierLogReader::new(reader), writer)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::future::{Future, poll_fn};
    use std::iter::empty;
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::Duration;

    use futures::StreamExt;
    use itertools::Itertools;
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
    use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::row::{OwnedRow, Row};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
    use risingwave_common::util::epoch::{EpochExt, EpochPair};
    use risingwave_connector::sink::log_store::{
        ChunkId, FlushCurrentEpochOptions, LogReader, LogStoreFactory, LogStoreReadItem, LogWriter,
        TruncateOffset,
    };
    use risingwave_hummock_sdk::HummockReadEpoch;
    use risingwave_hummock_test::test_utils::prepare_hummock_test_env;
    use risingwave_pb::plan_common::PbField;
    use risingwave_pb::stream_plan::{PbSinkAddColumnsOp, PbSinkSchemaChange};
    use risingwave_storage::table::batch_table::BatchTable;

    use crate::common::log_store_impl::kv_log_store::test_utils::{
        LogWriterTestExt, TEST_DATA_SIZE, TEST_TABLE_ID, calculate_vnode_bitmap, check_rows_eq,
        check_stream_chunk_eq, gen_multi_vnode_stream_chunks, gen_stream_chunk_with_info,
        gen_test_log_store_table,
    };
    use crate::common::log_store_impl::kv_log_store::{
        KV_LOG_STORE_V2_INFO, KvLogStoreFactory, KvLogStoreMetrics, KvLogStorePkInfo,
    };

    #[tokio::test]
    async fn test_basic() {
        for count in (0..20).step_by(5) {
            #[expect(deprecated)]
            Box::pin(test_basic_inner(
                count * TEST_DATA_SIZE,
                &crate::common::log_store_impl::kv_log_store::v1::KV_LOG_STORE_V1_INFO,
            ))
            .await;
            Box::pin(test_basic_inner(
                count * TEST_DATA_SIZE,
                &KV_LOG_STORE_V2_INFO,
            ))
            .await;
        }
    }

    async fn test_basic_inner(max_row_count: usize, pk_info: &'static KvLogStorePkInfo) {
        let gen_stream_chunk = |base| gen_stream_chunk_with_info(base, pk_info);
        let test_env = prepare_hummock_test_env().await;

        let table = gen_test_log_store_table(pk_info);

        test_env.register_table(table.clone()).await;

        let stream_chunk1 = gen_stream_chunk(0);
        let stream_chunk2 = gen_stream_chunk(10);
        let bitmap = calculate_vnode_bitmap(stream_chunk1.rows().chain(stream_chunk2.rows()));

        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(Arc::new(bitmap)),
            max_row_count,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (mut reader, mut writer) = factory.build().await;

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .table_committed_epoch(table.id)
            .unwrap()
            .next_epoch();
        test_env
            .storage
            .start_epoch(epoch1, HashSet::from_iter([table.id]));
        writer
            .init(EpochPair::new_test_epoch(epoch1), false)
            .await
            .unwrap();
        writer.write_chunk(stream_chunk1.clone()).await.unwrap();
        let epoch2 = epoch1.next_epoch();
        test_env
            .storage
            .start_epoch(epoch2, HashSet::from_iter([table.id]));
        writer
            .flush_current_epoch_for_test(epoch2, false)
            .await
            .unwrap();
        writer.write_chunk(stream_chunk2.clone()).await.unwrap();
        let epoch3 = epoch2.next_epoch();
        writer
            .flush_current_epoch_for_test(epoch3, true)
            .await
            .unwrap();

        let sync_result = test_env
            .storage
            .seal_and_sync_epoch(epoch2, HashSet::from_iter([table.id]))
            .await
            .unwrap();
        assert!(!sync_result.uncommitted_ssts.is_empty());

        reader.init().await.unwrap();
        reader.start_from(None).await.unwrap();
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&stream_chunk1, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert_eq!(epoch, epoch1);
                assert!(!is_checkpoint)
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch2);
                assert!(check_stream_chunk_eq(&stream_chunk2, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_recovery() {
        for count in (0..20).step_by(5) {
            #[expect(deprecated)]
            Box::pin(test_recovery_inner(
                count * TEST_DATA_SIZE,
                &crate::common::log_store_impl::kv_log_store::v1::KV_LOG_STORE_V1_INFO,
            ))
            .await;
            Box::pin(test_recovery_inner(
                count * TEST_DATA_SIZE,
                &KV_LOG_STORE_V2_INFO,
            ))
            .await;
        }
    }

    async fn test_recovery_inner(max_row_count: usize, pk_info: &'static KvLogStorePkInfo) {
        let gen_stream_chunk = |base| gen_stream_chunk_with_info(base, pk_info);
        let test_env = prepare_hummock_test_env().await;

        let table = gen_test_log_store_table(pk_info);

        test_env.register_table(table.clone()).await;

        let stream_chunk1 = gen_stream_chunk(0);
        let stream_chunk2 = gen_stream_chunk(10);
        let bitmap = calculate_vnode_bitmap(stream_chunk1.rows().chain(stream_chunk2.rows()));
        let bitmap = Arc::new(bitmap);

        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(bitmap.clone()),
            max_row_count,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (mut reader, mut writer) = factory.build().await;

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .table_committed_epoch(table.id)
            .unwrap()
            .next_epoch();
        test_env
            .storage
            .start_epoch(epoch1, HashSet::from_iter([table.id]));
        writer
            .init(EpochPair::new_test_epoch(epoch1), false)
            .await
            .unwrap();
        writer.write_chunk(stream_chunk1.clone()).await.unwrap();
        let epoch2 = epoch1.next_epoch();
        test_env
            .storage
            .start_epoch(epoch2, HashSet::from_iter([table.id]));
        writer
            .flush_current_epoch_for_test(epoch2, false)
            .await
            .unwrap();
        writer.write_chunk(stream_chunk2.clone()).await.unwrap();
        let epoch3 = epoch2.next_epoch();
        writer
            .flush_current_epoch_for_test(epoch3, true)
            .await
            .unwrap();

        reader.init().await.unwrap();
        reader.start_from(None).await.unwrap();
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&stream_chunk1, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert_eq!(epoch, epoch1);
                assert!(!is_checkpoint)
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch2);
                assert!(check_stream_chunk_eq(&stream_chunk2, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }

        test_env.commit_epoch(epoch2).await;
        // The truncate does not work because it is after the sync
        reader
            .truncate(TruncateOffset::Barrier { epoch: epoch2 })
            .unwrap();

        drop(writer);

        // Recovery
        test_env.storage.clear_shared_buffer().await;

        // Rebuild log reader and writer in recovery
        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(bitmap),
            max_row_count,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (mut reader, mut writer) = factory.build().await;
        test_env
            .storage
            .start_epoch(epoch3, HashSet::from_iter([table.id]));
        writer
            .init(EpochPair::new_test_epoch(epoch3), false)
            .await
            .unwrap();
        reader.init().await.unwrap();
        reader.start_from(None).await.unwrap();
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&stream_chunk1, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert_eq!(epoch, epoch1);
                assert!(!is_checkpoint)
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch2);
                assert!(check_stream_chunk_eq(&stream_chunk2, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_truncate() {
        for count in (2..10).step_by(3) {
            #[expect(deprecated)]
            Box::pin(test_truncate_inner(
                count,
                &crate::common::log_store_impl::kv_log_store::v1::KV_LOG_STORE_V1_INFO,
            ))
            .await;
            Box::pin(test_truncate_inner(count, &KV_LOG_STORE_V2_INFO)).await;
        }
    }

    async fn test_truncate_inner(max_row_count: usize, pk_info: &'static KvLogStorePkInfo) {
        let gen_stream_chunk = |base| gen_stream_chunk_with_info(base, pk_info);
        let test_env = prepare_hummock_test_env().await;

        let table = gen_test_log_store_table(pk_info);

        test_env.register_table(table.clone()).await;

        let stream_chunk1_1 = gen_stream_chunk(0);
        let stream_chunk1_2 = gen_stream_chunk(10);
        let stream_chunk2 = gen_stream_chunk(20);
        let stream_chunk3 = gen_stream_chunk(20);
        let bitmap = calculate_vnode_bitmap(
            stream_chunk1_1
                .rows()
                .chain(stream_chunk1_2.rows())
                .chain(stream_chunk2.rows())
                .chain(stream_chunk3.rows()),
        );
        let bitmap = Arc::new(bitmap);

        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(bitmap.clone()),
            max_row_count,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (mut reader, mut writer) = factory.build().await;

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .table_committed_epoch(table.id)
            .unwrap()
            .next_epoch();
        test_env
            .storage
            .start_epoch(epoch1, HashSet::from_iter([table.id]));
        writer
            .init(EpochPair::new_test_epoch(epoch1), false)
            .await
            .unwrap();
        writer.write_chunk(stream_chunk1_1.clone()).await.unwrap();
        writer.write_chunk(stream_chunk1_2.clone()).await.unwrap();
        let epoch2 = epoch1.next_epoch();
        test_env
            .storage
            .start_epoch(epoch2, HashSet::from_iter([table.id]));
        writer
            .flush_current_epoch_for_test(epoch2, true)
            .await
            .unwrap();
        writer.write_chunk(stream_chunk2.clone()).await.unwrap();

        test_env.commit_epoch(epoch1).await;

        reader.init().await.unwrap();
        reader.start_from(None).await.unwrap();
        let chunk_id1 = match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    chunk_id,
                },
            ) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&stream_chunk1_1, &read_stream_chunk));
                chunk_id
            }
            _ => unreachable!(),
        };
        let chunk_id2 = match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    chunk_id,
                },
            ) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&stream_chunk1_2, &read_stream_chunk));
                chunk_id
            }
            _ => unreachable!(),
        };
        assert!(chunk_id2 > chunk_id1);
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert_eq!(epoch, epoch1);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }

        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch2);
                assert!(check_stream_chunk_eq(&stream_chunk2, &read_stream_chunk));
            }
            _ => unreachable!(),
        }

        // The truncate should work because it is before the flush
        reader
            .truncate(TruncateOffset::Chunk {
                epoch: epoch1,
                chunk_id: chunk_id1,
            })
            .unwrap();
        let epoch3 = epoch2.next_epoch();
        writer
            .flush_current_epoch_for_test(epoch3, true)
            .await
            .unwrap();

        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }

        // Truncation on epoch1 should work because it is before this sync
        test_env.commit_epoch(epoch2).await;

        drop(writer);

        // Recovery
        test_env.storage.clear_shared_buffer().await;

        // Rebuild log reader and writer in recovery
        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(bitmap),
            max_row_count,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (mut reader, mut writer) = factory.build().await;

        test_env
            .storage
            .start_epoch(epoch3, HashSet::from_iter([table.id]));
        writer
            .init(EpochPair::new_test_epoch(epoch3), false)
            .await
            .unwrap();

        writer.write_chunk(stream_chunk3.clone()).await.unwrap();

        reader.init().await.unwrap();
        reader.start_from(None).await.unwrap();
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&stream_chunk1_2, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert_eq!(epoch, epoch1);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch2);
                assert!(check_stream_chunk_eq(&stream_chunk2, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch3);
                assert!(check_stream_chunk_eq(&stream_chunk3, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_update_vnode_recover() {
        let pk_info: &'static KvLogStorePkInfo = &KV_LOG_STORE_V2_INFO;
        let test_env = prepare_hummock_test_env().await;

        let table = gen_test_log_store_table(pk_info);

        test_env.register_table(table.clone()).await;

        fn build_bitmap(indexes: impl Iterator<Item = usize>) -> Arc<Bitmap> {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
            for i in indexes {
                builder.set(i, true);
            }
            Arc::new(builder.finish())
        }

        let vnodes1 = build_bitmap((0..VirtualNode::COUNT_FOR_TEST).filter(|i| i % 2 == 0));
        let vnodes2 = build_bitmap((0..VirtualNode::COUNT_FOR_TEST).filter(|i| i % 2 == 1));

        let factory1 = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(vnodes1),
            10 * TEST_DATA_SIZE,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let factory2 = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(vnodes2),
            10 * TEST_DATA_SIZE,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (mut reader1, mut writer1) = factory1.build().await;
        let (mut reader2, mut writer2) = factory2.build().await;

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .table_committed_epoch(table.id)
            .unwrap()
            .next_epoch();
        test_env
            .storage
            .start_epoch(epoch1, HashSet::from_iter([table.id]));
        writer1
            .init(EpochPair::new_test_epoch(epoch1), false)
            .await
            .unwrap();
        writer2
            .init(EpochPair::new_test_epoch(epoch1), false)
            .await
            .unwrap();
        reader1.init().await.unwrap();
        reader1.start_from(None).await.unwrap();
        reader2.init().await.unwrap();
        reader2.start_from(None).await.unwrap();
        let [chunk1_1, chunk1_2] = gen_multi_vnode_stream_chunks::<2>(0, 100, pk_info);
        writer1.write_chunk(chunk1_1.clone()).await.unwrap();
        writer2.write_chunk(chunk1_2.clone()).await.unwrap();
        let epoch2 = epoch1.next_epoch();
        test_env
            .storage
            .start_epoch(epoch2, HashSet::from_iter([table.id]));
        writer1
            .flush_current_epoch_for_test(epoch2, false)
            .await
            .unwrap();
        writer2
            .flush_current_epoch_for_test(epoch2, false)
            .await
            .unwrap();
        let [chunk2_1, chunk2_2] = gen_multi_vnode_stream_chunks::<2>(200, 100, pk_info);
        writer1.write_chunk(chunk2_1.clone()).await.unwrap();
        writer2.write_chunk(chunk2_2.clone()).await.unwrap();

        match reader1.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk { chunk, .. }) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&chunk1_1, &chunk));
            }
            _ => unreachable!(),
        };
        match reader1.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert_eq!(epoch, epoch1);
                assert!(!is_checkpoint);
            }
            _ => unreachable!(),
        }

        match reader2.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk { chunk, .. }) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&chunk1_2, &chunk));
            }
            _ => unreachable!(),
        }
        match reader2.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert_eq!(epoch, epoch1);
                assert!(!is_checkpoint);
            }
            _ => unreachable!(),
        }

        // Only reader1 will truncate
        reader1
            .truncate(TruncateOffset::Barrier { epoch: epoch1 })
            .unwrap();

        match reader1.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk { chunk, .. }) => {
                assert_eq!(epoch, epoch2);
                assert!(check_stream_chunk_eq(&chunk2_1, &chunk));
            }
            _ => unreachable!(),
        }
        match reader2.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk { chunk, .. }) => {
                assert_eq!(epoch, epoch2);
                assert!(check_stream_chunk_eq(&chunk2_2, &chunk));
            }
            _ => unreachable!(),
        }

        let epoch3 = epoch2.next_epoch();
        writer1
            .flush_current_epoch_for_test(epoch3, true)
            .await
            .unwrap();
        writer2
            .flush_current_epoch_for_test(epoch3, true)
            .await
            .unwrap();

        match reader1.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint);
            }
            _ => unreachable!(),
        }
        match reader2.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint);
            }
            _ => unreachable!(),
        }

        // Truncation of reader1 on epoch1 should work because it is before this sync
        test_env.commit_epoch(epoch2).await;

        drop(writer1);
        drop(writer2);

        // Recovery
        test_env.storage.clear_shared_buffer().await;

        let vnodes = build_bitmap(0..VirtualNode::COUNT_FOR_TEST);
        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(vnodes),
            10 * TEST_DATA_SIZE,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (mut reader, mut writer) = factory.build().await;
        test_env
            .storage
            .start_epoch(epoch3, HashSet::from_iter([table.id]));
        writer
            .init(EpochPair::new(epoch3, epoch2), false)
            .await
            .unwrap();
        reader.init().await.unwrap();
        reader.start_from(None).await.unwrap();
        {
            // Though we don't truncate reader2 with epoch1, we have truncated reader1 with epoch1, and with align_init_epoch
            // set to true, we won't receive the following commented items.
            match reader.next_item().await.unwrap() {
                (epoch, LogStoreReadItem::StreamChunk { chunk, .. }) => {
                    assert_eq!(epoch, epoch1);
                    assert!(check_stream_chunk_eq(&chunk1_2, &chunk));
                }

                _ => unreachable!(),
            }
            match reader.next_item().await.unwrap() {
                (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                    assert_eq!(epoch, epoch1);
                    assert!(!is_checkpoint);
                }
                _ => unreachable!(),
            }
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk { chunk, .. }) => {
                assert_eq!(epoch, epoch2);
                assert!(check_rows_eq(
                    chunk2_1.rows().chain(chunk2_2.rows()),
                    chunk.rows()
                ));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint);
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_cancellation_safe() {
        let pk_info: &'static KvLogStorePkInfo = &KV_LOG_STORE_V2_INFO;
        let gen_stream_chunk = |base| gen_stream_chunk_with_info(base, pk_info);
        let test_env = prepare_hummock_test_env().await;

        let table = gen_test_log_store_table(pk_info);

        test_env.register_table(table.clone()).await;

        let stream_chunk1 = gen_stream_chunk(0);
        let stream_chunk2 = gen_stream_chunk(10);
        let bitmap = calculate_vnode_bitmap(stream_chunk1.rows().chain(stream_chunk2.rows()));

        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(Arc::new(bitmap)),
            0,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (mut reader, mut writer) = factory.build().await;

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .table_committed_epoch(table.id)
            .unwrap()
            .next_epoch();
        test_env
            .storage
            .start_epoch(epoch1, HashSet::from_iter([table.id]));
        writer
            .init(EpochPair::new_test_epoch(epoch1), false)
            .await
            .unwrap();
        writer.write_chunk(stream_chunk1.clone()).await.unwrap();
        let epoch2 = epoch1.next_epoch();
        writer
            .flush_current_epoch_for_test(epoch2, true)
            .await
            .unwrap();

        reader.init().await.unwrap();
        reader.start_from(None).await.unwrap();

        {
            let mut future = pin!(reader.next_item());
            assert!(
                poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
        }

        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&stream_chunk1, &read_stream_chunk));
            }
            item => unreachable!("{:?}", item),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                assert_eq!(epoch, epoch1);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_rewind_on_consuming_persisted_log() {
        let pk_info: &'static KvLogStorePkInfo = &KV_LOG_STORE_V2_INFO;
        let gen_stream_chunk = |base| gen_stream_chunk_with_info(base, pk_info);
        let test_env = prepare_hummock_test_env().await;

        let table = gen_test_log_store_table(pk_info);

        test_env.register_table(table.clone()).await;

        let stream_chunk1 = gen_stream_chunk(0);
        let stream_chunk2 = gen_stream_chunk(10);
        let stream_chunk3 = gen_stream_chunk(20);
        let stream_chunk4 = gen_stream_chunk(30);
        let stream_chunk5 = gen_stream_chunk(40);
        let bitmap = calculate_vnode_bitmap(
            stream_chunk1
                .rows()
                .chain(stream_chunk2.rows())
                .chain(stream_chunk3.rows())
                .chain(stream_chunk4.rows())
                .chain(stream_chunk5.rows()),
        );
        let bitmap = Arc::new(bitmap);

        async fn check_reader<'a, 'b>(
            reader: &'a mut impl LogReader,
            data: impl Iterator<Item = &'b (u64, Option<&'b StreamChunk>)>,
        ) -> Vec<ChunkId> {
            check_reader_inner(reader, data, true).await
        }

        async fn check_reader_last_unsealed<'a, 'b>(
            reader: &'a mut impl LogReader,
            data: impl Iterator<Item = &'b (u64, Option<&'b StreamChunk>)>,
        ) -> Vec<ChunkId> {
            check_reader_inner(reader, data, false).await
        }

        async fn check_reader_inner<'a, 'b>(
            reader: &'a mut impl LogReader,
            data: impl Iterator<Item = &'b (u64, Option<&'b StreamChunk>)>,
            last_sealed: bool,
        ) -> Vec<ChunkId> {
            let mut chunk_ids = Vec::new();
            let data = data.collect_vec();
            let size = data.len();
            for (i, (epoch, stream_chunk)) in data.into_iter().enumerate() {
                if let Some(stream_chunk) = stream_chunk {
                    match reader.next_item().await.unwrap() {
                        (
                            item_epoch,
                            LogStoreReadItem::StreamChunk {
                                chunk: read_stream_chunk,
                                chunk_id,
                            },
                        ) => {
                            assert_eq!(item_epoch, *epoch);
                            assert!(check_stream_chunk_eq(stream_chunk, &read_stream_chunk));
                            chunk_ids.push(chunk_id);
                        }
                        _ => unreachable!(),
                    };
                }
                if last_sealed || i != size - 1 {
                    match reader.next_item().await.unwrap() {
                        (item_epoch, LogStoreReadItem::Barrier { is_checkpoint, .. }) => {
                            assert_eq!(item_epoch, *epoch);
                            assert!(is_checkpoint)
                        }
                        _ => unreachable!(),
                    }
                }
            }
            let mut future = pin!(reader.next_item());
            assert!(
                poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            chunk_ids
        }

        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(bitmap.clone()),
            1024,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (mut reader, mut writer) = factory.build().await;

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .table_committed_epoch(table.id)
            .unwrap()
            .next_epoch();
        test_env
            .storage
            .start_epoch(epoch1, HashSet::from_iter([table.id]));
        writer
            .init(EpochPair::new_test_epoch(epoch1), false)
            .await
            .unwrap();
        writer.write_chunk(stream_chunk1.clone()).await.unwrap();
        let epoch2 = epoch1.next_epoch();
        test_env
            .storage
            .start_epoch(epoch2, HashSet::from_iter([table.id]));
        writer
            .flush_current_epoch_for_test(epoch2, true)
            .await
            .unwrap();
        writer.write_chunk(stream_chunk2.clone()).await.unwrap();
        let epoch3 = epoch2.next_epoch();
        test_env
            .storage
            .start_epoch(epoch3, HashSet::from_iter([table.id]));
        writer
            .flush_current_epoch_for_test(epoch3, true)
            .await
            .unwrap();
        writer.write_chunk(stream_chunk3.clone()).await.unwrap();
        writer
            .flush_current_epoch_for_test(u64::MAX, true)
            .await
            .unwrap();

        test_env.commit_epoch(epoch1).await;
        test_env.commit_epoch(epoch2).await;
        test_env.commit_epoch(epoch3).await;

        reader.init().await.unwrap();
        reader.start_from(None).await.unwrap();

        let chunk_ids = check_reader(
            &mut reader,
            [
                (epoch1, Some(&stream_chunk1)),
                (epoch2, Some(&stream_chunk2)),
                (epoch3, Some(&stream_chunk3)),
            ]
            .iter(),
        )
        .await;
        assert_eq!(3, chunk_ids.len());

        reader
            .truncate(TruncateOffset::Chunk {
                epoch: epoch1,
                chunk_id: chunk_ids[0],
            })
            .unwrap();
        reader.rewind().await.unwrap();
        reader.start_from(None).await.unwrap();
        let chunk_ids = check_reader(
            &mut reader,
            [
                (epoch1, None),
                (epoch2, Some(&stream_chunk2)),
                (epoch3, Some(&stream_chunk3)),
            ]
            .iter(),
        )
        .await;
        assert_eq!(2, chunk_ids.len());

        reader
            .truncate(TruncateOffset::Barrier { epoch: epoch1 })
            .unwrap();
        reader.rewind().await.unwrap();
        reader.start_from(None).await.unwrap();
        let chunk_ids = check_reader(
            &mut reader,
            [
                (epoch2, Some(&stream_chunk2)),
                (epoch3, Some(&stream_chunk3)),
            ]
            .iter(),
        )
        .await;
        assert_eq!(2, chunk_ids.len());

        reader
            .truncate(TruncateOffset::Chunk {
                epoch: epoch3,
                chunk_id: chunk_ids[1],
            })
            .unwrap();
        reader.rewind().await.unwrap();
        reader.start_from(None).await.unwrap();
        let chunk_ids = check_reader(&mut reader, [(epoch3, None)].iter()).await;
        assert_eq!(0, chunk_ids.len());

        // Recovery happens. Test rewind while consuming persisted log. No new data written.
        // Writer must be dropped first to ensure vnode assignment is exclusive.
        drop(reader);
        drop(writer);
        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(bitmap.clone()),
            1024,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (mut reader, mut writer) = factory.build().await;

        let epoch4 = epoch3.next_epoch();
        test_env
            .storage
            .start_epoch(epoch4, HashSet::from_iter([table.id]));
        writer
            .init(EpochPair::new(epoch4, epoch3), false)
            .await
            .unwrap();

        reader.init().await.unwrap();
        reader.start_from(None).await.unwrap();

        let data = [
            (epoch1, Some(&stream_chunk1)),
            (epoch2, Some(&stream_chunk2)),
            (epoch3, Some(&stream_chunk3)),
        ];

        assert_eq!(3, check_reader(&mut reader, data.iter()).await.len());

        reader
            .truncate(TruncateOffset::Barrier { epoch: epoch1 })
            .unwrap();
        reader.rewind().await.unwrap();
        reader.start_from(None).await.unwrap();
        let chunk_ids = check_reader(&mut reader, data[1..].iter()).await;
        assert_eq!(2, chunk_ids.len());

        reader
            .truncate(TruncateOffset::Chunk {
                epoch: epoch2,
                chunk_id: chunk_ids[0],
            })
            .unwrap();
        reader.rewind().await.unwrap();
        reader.start_from(None).await.unwrap();
        let chunk_ids = check_reader(&mut reader, data[1..].iter()).await;
        assert_eq!(2, chunk_ids.len());

        reader
            .truncate(TruncateOffset::Chunk {
                epoch: epoch2,
                chunk_id: chunk_ids[0],
            })
            .unwrap();

        reader
            .truncate(TruncateOffset::Barrier { epoch: epoch2 })
            .unwrap();
        reader.rewind().await.unwrap();
        reader.start_from(None).await.unwrap();
        let chunk_ids = check_reader(&mut reader, data[2..].iter()).await;
        assert_eq!(1, chunk_ids.len());

        // Recovery happens again. Test rewind with some new data written and flushed.
        // Writer must be dropped first to ensure vnode assignment is exclusive.
        drop(reader);
        drop(writer);
        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(bitmap.clone()),
            1024,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (mut reader, mut writer) = factory.build().await;

        writer
            .init(EpochPair::new(epoch4, epoch3), false)
            .await
            .unwrap();
        writer.write_chunk(stream_chunk4.clone()).await.unwrap();
        let epoch5 = epoch4 + 1;
        test_env
            .storage
            .start_epoch(epoch5, HashSet::from_iter([table.id]));
        writer
            .flush_current_epoch_for_test(epoch5, true)
            .await
            .unwrap();
        writer.write_chunk(stream_chunk5.clone()).await.unwrap();

        reader.init().await.unwrap();
        reader.start_from(None).await.unwrap();
        let data = [
            (epoch1, Some(&stream_chunk1)),
            (epoch2, Some(&stream_chunk2)),
            (epoch3, Some(&stream_chunk3)),
            (epoch4, Some(&stream_chunk4)),
            (epoch5, Some(&stream_chunk5)),
        ];

        assert_eq!(
            5,
            check_reader_last_unsealed(&mut reader, data.iter())
                .await
                .len()
        );

        reader
            .truncate(TruncateOffset::Barrier { epoch: epoch1 })
            .unwrap();
        reader.rewind().await.unwrap();
        reader.start_from(None).await.unwrap();
        let chunk_ids = check_reader_last_unsealed(&mut reader, data[1..].iter()).await;
        assert_eq!(4, chunk_ids.len());

        reader
            .truncate(TruncateOffset::Chunk {
                epoch: epoch2,
                chunk_id: chunk_ids[0],
            })
            .unwrap();
        reader.rewind().await.unwrap();
        reader.start_from(None).await.unwrap();
        let chunk_ids = check_reader_last_unsealed(&mut reader, data[1..].iter()).await;
        assert_eq!(4, chunk_ids.len());

        reader
            .truncate(TruncateOffset::Barrier { epoch: epoch2 })
            .unwrap();
        reader.rewind().await.unwrap();
        reader.start_from(None).await.unwrap();
        let chunk_ids = check_reader_last_unsealed(&mut reader, data[2..].iter()).await;
        assert_eq!(3, chunk_ids.len());

        reader
            .truncate(TruncateOffset::Barrier { epoch: epoch3 })
            .unwrap();
        reader.rewind().await.unwrap();
        reader.start_from(None).await.unwrap();
        let chunk_ids = check_reader_last_unsealed(&mut reader, data[3..].iter()).await;
        assert_eq!(2, chunk_ids.len());

        reader
            .truncate(TruncateOffset::Chunk {
                epoch: epoch4,
                chunk_id: chunk_ids[0],
            })
            .unwrap();
        reader.rewind().await.unwrap();
        reader.start_from(None).await.unwrap();
        let chunk_ids = check_reader_last_unsealed(
            &mut reader,
            [(epoch4, None), (epoch5, Some(&stream_chunk5))].iter(),
        )
        .await;
        assert_eq!(1, chunk_ids.len());

        reader
            .truncate(TruncateOffset::Barrier { epoch: epoch4 })
            .unwrap();
        reader.rewind().await.unwrap();
        reader.start_from(None).await.unwrap();
        let chunk_ids =
            check_reader_last_unsealed(&mut reader, [(epoch5, Some(&stream_chunk5))].iter()).await;
        assert_eq!(1, chunk_ids.len());

        reader
            .truncate(TruncateOffset::Chunk {
                epoch: epoch5,
                chunk_id: chunk_ids[0],
            })
            .unwrap();
        reader.rewind().await.unwrap();
        reader.start_from(None).await.unwrap();
        let chunk_ids = check_reader_last_unsealed(&mut reader, empty()).await;
        assert!(chunk_ids.is_empty());
    }

    async fn validate_reader(
        reader: &mut impl LogReader,
        expected: impl IntoIterator<Item = (u64, LogStoreReadItem)>,
    ) {
        for (expected_epoch, expected_item) in expected {
            let (epoch, item) = reader.next_item().await.unwrap();
            assert_eq!(expected_epoch, epoch);
            match (expected_item, item) {
                (
                    LogStoreReadItem::StreamChunk {
                        chunk: expected_chunk,
                        ..
                    },
                    LogStoreReadItem::StreamChunk { chunk, .. },
                ) => {
                    check_stream_chunk_eq(&expected_chunk, &chunk);
                }
                (
                    LogStoreReadItem::Barrier {
                        is_checkpoint: expected_is_checkpoint,
                        ..
                    },
                    LogStoreReadItem::Barrier { is_checkpoint, .. },
                ) => {
                    assert_eq!(expected_is_checkpoint, is_checkpoint);
                }
                _ => unreachable!(),
            }
        }
    }

    #[tokio::test]
    async fn test_truncate_historical() {
        #[expect(deprecated)]
        Box::pin(test_truncate_historical_inner(
            10,
            &crate::common::log_store_impl::kv_log_store::v1::KV_LOG_STORE_V1_INFO,
        ))
        .await;
        Box::pin(test_truncate_historical_inner(10, &KV_LOG_STORE_V2_INFO)).await;
    }

    async fn test_truncate_historical_inner(
        max_row_count: usize,
        pk_info: &'static KvLogStorePkInfo,
    ) {
        let gen_stream_chunk = |base| gen_stream_chunk_with_info(base, pk_info);
        let test_env = prepare_hummock_test_env().await;

        let table = gen_test_log_store_table(pk_info);

        test_env.register_table(table.clone()).await;

        let stream_chunk1 = gen_stream_chunk(0);
        let stream_chunk2 = gen_stream_chunk(10);
        let bitmap = calculate_vnode_bitmap(stream_chunk1.rows().chain(stream_chunk2.rows()));
        let bitmap = Arc::new(bitmap);

        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(bitmap.clone()),
            max_row_count,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (mut reader, mut writer) = factory.build().await;

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .table_committed_epoch(table.id)
            .unwrap()
            .next_epoch();
        test_env
            .storage
            .start_epoch(epoch1, HashSet::from_iter([table.id]));
        writer
            .init(EpochPair::new_test_epoch(epoch1), false)
            .await
            .unwrap();
        writer.write_chunk(stream_chunk1.clone()).await.unwrap();
        let epoch2 = epoch1.next_epoch();
        test_env
            .storage
            .start_epoch(epoch2, HashSet::from_iter([table.id]));
        writer
            .flush_current_epoch_for_test(epoch2, false)
            .await
            .unwrap();
        writer.write_chunk(stream_chunk2.clone()).await.unwrap();
        let epoch3 = epoch2.next_epoch();
        writer
            .flush_current_epoch_for_test(epoch3, true)
            .await
            .unwrap();

        test_env.commit_epoch(epoch2).await;

        reader.init().await.unwrap();
        reader.start_from(None).await.unwrap();
        validate_reader(
            &mut reader,
            [
                (
                    epoch1,
                    LogStoreReadItem::StreamChunk {
                        chunk: stream_chunk1.clone(),
                        chunk_id: 0,
                    },
                ),
                (
                    epoch1,
                    LogStoreReadItem::Barrier {
                        is_checkpoint: false,
                        new_vnode_bitmap: None,
                        is_stop: false,
                        schema_change: None,
                    },
                ),
                (
                    epoch2,
                    LogStoreReadItem::StreamChunk {
                        chunk: stream_chunk2.clone(),
                        chunk_id: 0,
                    },
                ),
                (
                    epoch2,
                    LogStoreReadItem::Barrier {
                        is_checkpoint: true,
                        new_vnode_bitmap: None,
                        is_stop: false,
                        schema_change: None,
                    },
                ),
            ],
        )
        .await;

        drop(writer);

        // Recovery
        test_env.storage.clear_shared_buffer().await;

        // Rebuild log reader and writer in recovery
        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(bitmap.clone()),
            max_row_count,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (mut reader, mut writer) = factory.build().await;
        test_env
            .storage
            .start_epoch(epoch3, HashSet::from_iter([table.id]));
        writer
            .init(EpochPair::new_test_epoch(epoch3), false)
            .await
            .unwrap();
        reader.init().await.unwrap();
        reader.start_from(None).await.unwrap();
        validate_reader(
            &mut reader,
            [
                (
                    epoch1,
                    LogStoreReadItem::StreamChunk {
                        chunk: stream_chunk1.clone(),
                        chunk_id: 0,
                    },
                ),
                (
                    epoch1,
                    LogStoreReadItem::Barrier {
                        is_checkpoint: false,
                        new_vnode_bitmap: None,
                        is_stop: false,
                        schema_change: None,
                    },
                ),
                (
                    epoch2,
                    LogStoreReadItem::StreamChunk {
                        chunk: stream_chunk2.clone(),
                        chunk_id: 0,
                    },
                ),
                (
                    epoch2,
                    LogStoreReadItem::Barrier {
                        is_checkpoint: true,
                        new_vnode_bitmap: None,
                        is_stop: false,
                        schema_change: None,
                    },
                ),
            ],
        )
        .await;
        // The truncate should take effect
        reader
            .truncate(TruncateOffset::Barrier { epoch: epoch1 })
            .unwrap();
        let epoch4 = epoch3.next_epoch();
        writer
            .flush_current_epoch_for_test(epoch4, true)
            .await
            .unwrap();
        test_env.commit_epoch(epoch3).await;

        drop(writer);

        // Recovery
        test_env.storage.clear_shared_buffer().await;

        // Rebuild log reader and writer in recovery
        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(bitmap),
            max_row_count,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (mut reader, mut writer) = factory.build().await;
        test_env
            .storage
            .start_epoch(epoch4, HashSet::from_iter([table.id]));
        writer
            .init(EpochPair::new_test_epoch(epoch4), false)
            .await
            .unwrap();
        reader.init().await.unwrap();
        reader.start_from(None).await.unwrap();
        validate_reader(
            &mut reader,
            [
                (
                    epoch2,
                    LogStoreReadItem::StreamChunk {
                        chunk: stream_chunk2.clone(),
                        chunk_id: 0,
                    },
                ),
                (
                    epoch2,
                    LogStoreReadItem::Barrier {
                        is_checkpoint: true,
                        new_vnode_bitmap: None,
                        is_stop: false,
                        schema_change: None,
                    },
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_schema_change() {
        let pk_info: &'static KvLogStorePkInfo = &KV_LOG_STORE_V2_INFO;
        let gen_stream_chunk = |base| gen_stream_chunk_with_info(base, pk_info);
        let test_env = prepare_hummock_test_env().await;

        let table = gen_test_log_store_table(pk_info);

        test_env.register_table(table.clone()).await;

        let stream_chunk1 = gen_stream_chunk(0);
        let bitmap = calculate_vnode_bitmap(stream_chunk1.rows());
        let bitmap = Arc::new(bitmap);

        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(bitmap),
            1024,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (mut reader, mut writer) = factory.build().await;

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .table_committed_epoch(table.id)
            .unwrap()
            .next_epoch();
        test_env
            .storage
            .start_epoch(epoch1, HashSet::from_iter([table.id]));
        writer
            .init(EpochPair::new_test_epoch(epoch1), false)
            .await
            .unwrap();

        writer.write_chunk(stream_chunk1.clone()).await.unwrap();

        let epoch2 = epoch1.next_epoch();
        test_env
            .storage
            .start_epoch(epoch2, HashSet::from_iter([table.id]));

        use risingwave_pb::stream_plan::sink_schema_change::PbOp as PbSchemaChangeOp;
        let schema_change = PbSinkSchemaChange {
            original_schema: table
                .columns
                .iter()
                .map(|col| PbField {
                    data_type: Some(
                        col.column_desc
                            .as_ref()
                            .unwrap()
                            .column_type
                            .as_ref()
                            .unwrap()
                            .clone(),
                    ),
                    name: col.column_desc.as_ref().unwrap().name.clone(),
                })
                .collect(),
            op: Some(PbSchemaChangeOp::AddColumns(PbSinkAddColumnsOp {
                fields: vec![PbField {
                    data_type: Some(DataType::Int32.to_protobuf()),
                    name: "new_col".to_owned(),
                }],
            })),
        };

        // Flush with schema change should wait until the reader truncates the barrier.
        let mut flush_future = Box::pin(writer.flush_current_epoch(
            epoch2,
            FlushCurrentEpochOptions {
                is_checkpoint: true,
                new_vnode_bitmap: None,
                is_stop: false,
                schema_change: Some(schema_change.clone()),
                wait_log_store_flush: false,
            },
        ));

        assert!(
            tokio::time::timeout(Duration::from_millis(50), flush_future.as_mut())
                .await
                .is_err()
        );

        reader.init().await.unwrap();
        reader.start_from(None).await.unwrap();

        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_chunk, ..
                },
            ) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&stream_chunk1, &read_chunk));
            }
            item => unreachable!("{:?}", item),
        }

        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::Barrier {
                    is_checkpoint,
                    schema_change: read_schema_change,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch1);
                assert!(is_checkpoint);
                assert_eq!(read_schema_change, Some(schema_change.clone()));
                reader
                    .truncate(TruncateOffset::Barrier { epoch: epoch1 })
                    .unwrap();
            }
            item => unreachable!("{:?}", item),
        }

        let post_flush = flush_future.await.unwrap();
        post_flush.post_yield_barrier().await.unwrap();

        // Writer should be able to continue with the next epoch after reader truncation.
        writer.write_chunk(gen_stream_chunk(10)).await.unwrap();
    }

    #[tokio::test]
    async fn test_wait_log_store_flush_without_schema_change() {
        let pk_info: &'static KvLogStorePkInfo = &KV_LOG_STORE_V2_INFO;
        let test_env = prepare_hummock_test_env().await;

        let table = gen_test_log_store_table(pk_info);

        test_env.register_table(table.clone()).await;

        let bitmap = Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST));
        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(bitmap),
            1024,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (mut reader, mut writer) = factory.build().await;

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .table_committed_epoch(table.id)
            .unwrap()
            .next_epoch();
        test_env
            .storage
            .start_epoch(epoch1, HashSet::from_iter([table.id]));
        writer
            .init(EpochPair::new_test_epoch(epoch1), false)
            .await
            .unwrap();

        let epoch2 = epoch1.next_epoch();
        test_env
            .storage
            .start_epoch(epoch2, HashSet::from_iter([table.id]));

        let mut flush_future = Box::pin(writer.flush_current_epoch(
            epoch2,
            FlushCurrentEpochOptions {
                is_checkpoint: true,
                new_vnode_bitmap: None,
                is_stop: true,
                schema_change: None,
                wait_log_store_flush: true,
            },
        ));

        assert!(
            tokio::time::timeout(Duration::from_millis(50), flush_future.as_mut())
                .await
                .is_err()
        );

        reader.init().await.unwrap();
        reader.start_from(None).await.unwrap();

        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::Barrier {
                    is_checkpoint,
                    is_stop,
                    schema_change,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch1);
                assert!(is_checkpoint);
                assert!(is_stop);
                assert_eq!(schema_change, None);
                reader
                    .truncate(TruncateOffset::Barrier { epoch: epoch1 })
                    .unwrap();
            }
            item => unreachable!("{:?}", item),
        }

        let post_flush = flush_future.await.unwrap();
        post_flush.post_yield_barrier().await.unwrap();
    }

    #[tokio::test]
    async fn test_schema_change_batch_scan_skips_old_schema_rows() {
        let pk_info: &'static KvLogStorePkInfo = &KV_LOG_STORE_V2_INFO;
        let test_env = prepare_hummock_test_env().await;

        let old_table = gen_test_log_store_table(pk_info);
        test_env.register_table(old_table.clone()).await;

        let old_chunk = gen_stream_chunk_with_info(0, pk_info);
        let old_bitmap = Arc::new(calculate_vnode_bitmap(old_chunk.rows()));
        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            old_table.clone(),
            Some(old_bitmap),
            1024,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (mut reader, mut writer) = factory.build().await;

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .table_committed_epoch(old_table.id)
            .unwrap()
            .next_epoch();
        test_env
            .storage
            .start_epoch(epoch1, HashSet::from_iter([TEST_TABLE_ID]));
        writer
            .init(EpochPair::new_test_epoch(epoch1), false)
            .await
            .unwrap();
        writer.write_chunk(old_chunk).await.unwrap();

        let epoch2 = epoch1.next_epoch();
        test_env
            .storage
            .start_epoch(epoch2, HashSet::from_iter([TEST_TABLE_ID]));
        let schema_change = PbSinkSchemaChange {
            original_schema: old_table
                .columns
                .iter()
                .map(|col| PbField {
                    data_type: Some(
                        col.column_desc
                            .as_ref()
                            .unwrap()
                            .column_type
                            .as_ref()
                            .unwrap()
                            .clone(),
                    ),
                    name: col.column_desc.as_ref().unwrap().name.clone(),
                })
                .collect(),
            op: Some(
                risingwave_pb::stream_plan::sink_schema_change::PbOp::AddColumns(
                    PbSinkAddColumnsOp {
                        fields: vec![PbField {
                            data_type: Some(DataType::Int32.to_protobuf()),
                            name: "age".to_owned(),
                        }],
                    },
                ),
            ),
        };
        let flush_future = writer.flush_current_epoch(
            epoch2,
            FlushCurrentEpochOptions {
                is_checkpoint: true,
                new_vnode_bitmap: None,
                is_stop: false,
                schema_change: Some(schema_change),
                wait_log_store_flush: false,
            },
        );
        let mut flush_future = Box::pin(flush_future);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), flush_future.as_mut())
                .await
                .is_err()
        );

        reader.init().await.unwrap();
        reader.start_from(None).await.unwrap();
        let _ = reader.next_item().await.unwrap();
        let _ = reader.next_item().await.unwrap();
        reader
            .truncate(TruncateOffset::Barrier { epoch: epoch1 })
            .unwrap();
        {
            let post_flush = tokio::time::timeout(Duration::from_secs(10), flush_future)
                .await
                .expect("schema-change flush should finish after reader truncates barrier")
                .unwrap();
            post_flush.post_yield_barrier().await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(10), test_env.commit_epoch(epoch1))
            .await
            .expect("committing the schema-change epoch should not hang");

        drop(reader);
        drop(writer);
        test_env.storage.clear_shared_buffer().await;

        let mut new_table = old_table.clone();
        new_table.columns.push(
            ColumnCatalog::visible(ColumnDesc::named(
                "age",
                ColumnId::new(new_table.columns.len() as i32),
                DataType::Int32,
            ))
            .to_protobuf(),
        );
        new_table.value_indices = (0..new_table.columns.len() as i32).collect();
        let mut builder = DataChunkBuilder::new(
            vec![DataType::Int64, DataType::Varchar, DataType::Int32],
            1024,
        );
        let vnode_zero_ids = (0_i64..)
            .filter(|id| {
                let row = OwnedRow::new(vec![
                    Some(ScalarImpl::Int64(*id)),
                    Some(ScalarImpl::Utf8("probe".into())),
                    Some(ScalarImpl::Int32(0)),
                ]);
                VirtualNode::compute_row_for_test(&row, &[0]).to_index() == 0
            })
            .take(2)
            .collect_vec();
        for row in [
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(vnode_zero_ids[0])),
                Some(ScalarImpl::Utf8("alice".into())),
                Some(ScalarImpl::Int32(40)),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(vnode_zero_ids[1])),
                Some(ScalarImpl::Utf8("bob".into())),
                Some(ScalarImpl::Int32(50)),
            ]),
        ] {
            assert!(builder.append_one_row(row).is_none());
        }
        let new_chunk =
            StreamChunk::from_parts(vec![Op::Insert, Op::Insert], builder.consume_all().unwrap());

        let new_bitmap = Arc::new(calculate_vnode_bitmap(new_chunk.rows()));
        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            new_table.clone(),
            Some(new_bitmap.clone()),
            1024,
            1024,
            KvLogStoreMetrics::for_test(),
            "test",
            None,
            pk_info,
        );
        let (_, mut writer) = factory.build().await;
        test_env
            .storage
            .start_epoch(epoch2, HashSet::from_iter([TEST_TABLE_ID]));
        writer
            .init(EpochPair::new_test_epoch(epoch2), false)
            .await
            .unwrap();
        writer.write_chunk(new_chunk).await.unwrap();
        let epoch3 = epoch2.next_epoch();
        tokio::time::timeout(
            Duration::from_secs(10),
            writer.flush_current_epoch_for_test(epoch3, true),
        )
        .await
        .expect("flushing the rebuilt writer should not hang")
        .unwrap();
        tokio::time::timeout(Duration::from_secs(10), test_env.commit_epoch(epoch2))
            .await
            .expect("committing the rebuilt-writer epoch should not hang");

        let batch_table = BatchTable::for_test(
            test_env.storage.clone(),
            TEST_TABLE_ID,
            new_table
                .columns
                .iter()
                .map(|col| col.column_desc.as_ref().unwrap().into())
                .collect_vec(),
            pk_info.pk_orderings.to_vec(),
            (0..pk_info.pk_len()).collect_vec(),
            new_table
                .value_indices
                .iter()
                .map(|&idx| idx as usize)
                .collect_vec(),
        );
        let age_column_idx = pk_info.predefined_column_len() + 2;
        let mut iter = pin!(
            batch_table
                .batch_iter(
                    HummockReadEpoch::Committed(epoch2),
                    false,
                    Default::default(),
                )
                .await
                .unwrap()
        );

        let mut ages = tokio::time::timeout(Duration::from_secs(10), async {
            let mut ages = vec![];
            while let Some(row) = iter.next().await {
                let row = row.unwrap();
                if let Some(age) = row.datum_at(age_column_idx) {
                    ages.push(age.into_int32());
                }
            }
            ages
        })
        .await
        .expect("batch scan after schema change should not hang");
        ages.sort_unstable();
        assert_eq!(ages, vec![40, 50]);
    }
}
